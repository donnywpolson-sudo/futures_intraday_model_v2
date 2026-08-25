"""Small, fail-closed runtime for one explicitly reviewed live model adapter.

The module deliberately does not load arbitrary model files.  The parent passes one
reviewed, pickleable adapter object to one spawned worker process.
"""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import multiprocessing as mp
from pathlib import Path
import queue
import sqlite3
import threading
import time
from typing import Any, Mapping, Protocol, Sequence


SUPPORTED_MODEL_SCHEMAS = frozenset({"ohlcv-1s", "ohlcv-1m", "ohlcv-1h", "ohlcv-1d"})
SCHEMA_SECONDS = {"ohlcv-1s": 1, "ohlcv-1m": 60, "ohlcv-1h": 3600, "ohlcv-1d": 86400}
DECISIONS = frozenset({"LONG", "SHORT", "FLAT", "ABSTAIN"})
MAX_LOOKBACK_BARS = 1_000_000


def _identifier(value: str, name: str) -> str:
    text = str(value).strip()
    if not text or len(text) > 80 or not all(c.isalnum() or c in "-_." for c in text):
        raise ValueError(f"{name} must be a bounded safe identifier")
    return text


@dataclass(frozen=True)
class ModelSpec:
    model_id: str
    version: str
    artifact_sha256: str
    strategy: str
    markets: tuple[str, ...]
    schema: str
    lookback_bars: int
    dependencies: tuple[str, ...] = ()
    accepts_contract_roll: bool = False
    inference_timeout_seconds: float = 2.0

    def __post_init__(self) -> None:
        for field in ("model_id", "version", "strategy"):
            _identifier(getattr(self, field), field)
        if len(self.artifact_sha256) != 64 or any(c not in "0123456789abcdef" for c in self.artifact_sha256):
            raise ValueError("artifact_sha256 must be a lowercase SHA-256 digest")
        normalized = tuple(str(m).strip().upper() for m in self.markets)
        if not normalized or len(set(normalized)) != len(normalized):
            raise ValueError("markets must be non-empty and unique")
        if any(not market.isalnum() or len(market) > 12 for market in normalized):
            raise ValueError("markets must use bounded provider root symbols")
        object.__setattr__(self, "markets", normalized)
        if self.schema not in SUPPORTED_MODEL_SCHEMAS:
            raise ValueError("unsupported model OHLCV schema")
        if isinstance(self.lookback_bars, bool) or not 1 <= self.lookback_bars <= MAX_LOOKBACK_BARS:
            raise ValueError("lookback_bars is outside the bounded runtime limit")
        if not 0.01 <= float(self.inference_timeout_seconds) <= 60.0:
            raise ValueError("inference_timeout_seconds must be within [0.01, 60]")
        for dependency in self.dependencies:
            _identifier(dependency, "dependency")

    @property
    def timeframe(self) -> str:
        return self.schema.removeprefix("ohlcv-")


@dataclass(frozen=True)
class ModelBar:
    market: str
    contract: str
    instrument_id: int
    schema: str
    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def validated(cls, value: Mapping[str, Any], spec: ModelSpec) -> "ModelBar":
        try:
            bar = cls(
                market=str(value["market"]).upper(),
                contract=str(value["contract"]).strip().upper(),
                instrument_id=int(value["instrument_id"]),
                schema=str(value["schema"]),
                time=int(value["time"]),
                open=float(value["open"]), high=float(value["high"]),
                low=float(value["low"]), close=float(value["close"]),
                volume=float(value["volume"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("model bar is missing or has invalid fields") from exc
        if bar.market not in spec.markets or bar.schema != spec.schema:
            raise ValueError("model bar is outside the adapter scope")
        if not bar.contract or bar.instrument_id < 0 or bar.time < 0:
            raise ValueError("model bar identity is invalid")
        prices = (bar.open, bar.high, bar.low, bar.close, bar.volume)
        if not all(math.isfinite(v) for v in prices) or bar.volume < 0:
            raise ValueError("model bar contains nonfinite or negative values")
        if bar.high < max(bar.open, bar.low, bar.close) or bar.low > min(bar.open, bar.high, bar.close):
            raise ValueError("model bar violates OHLC bounds")
        now = int(datetime.now(timezone.utc).timestamp())
        if bar.time > now:
            raise ValueError("model bar timestamp is in the future")
        return bar


@dataclass(frozen=True)
class ModelDecision:
    market: str
    decision: str
    confidence: float
    horizon_seconds: int
    reason: str
    probabilities: Mapping[str, float] | None = None
    expected_return: float = 0.0


class TrustedModelAdapter(Protocol):
    spec: ModelSpec

    def self_check(self) -> bool: ...

    def evaluate(self, bars: Mapping[str, Sequence[Mapping[str, Any]]]) -> Sequence[ModelDecision | Mapping[str, Any]]: ...


def verify_artifact_hash(path: Path, expected_sha256: str) -> None:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != expected_sha256:
        raise RuntimeError("model artifact hash mismatch")


def _validate_decision(value: ModelDecision | Mapping[str, Any], spec: ModelSpec, input_time: int) -> dict[str, Any]:
    raw = asdict(value) if isinstance(value, ModelDecision) else dict(value)
    try:
        market = str(raw["market"]).upper()
        direction = str(raw["decision"]).upper()
        confidence = float(raw["confidence"])
        horizon = int(raw["horizon_seconds"])
        reason = str(raw.get("reason", ""))[:240]
        expected_return = float(raw.get("expected_return", 0.0))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("model output is malformed") from exc
    if market not in spec.markets or direction not in DECISIONS:
        raise ValueError("model output is outside the adapter scope")
    if not math.isfinite(confidence) or not 0.0 <= confidence <= 1.0:
        raise ValueError("model confidence must be finite and within [0, 1]")
    if horizon <= 0 or not math.isfinite(expected_return):
        raise ValueError("model horizon or expected return is invalid")
    probabilities = raw.get("probabilities")
    if direction != "ABSTAIN":
        if probabilities is None:
            remainder = (1.0 - confidence) / 2.0
            probabilities = {"long": remainder, "flat": remainder, "short": remainder}
            probabilities[direction.lower()] = confidence
        if not isinstance(probabilities, Mapping) or set(probabilities) != {"long", "flat", "short"}:
            raise ValueError("model probabilities must define long, flat, and short")
        probabilities = {key: float(probabilities[key]) for key in ("long", "flat", "short")}
        if not all(math.isfinite(v) and 0 <= v <= 1 for v in probabilities.values()):
            raise ValueError("model probabilities are nonfinite or outside [0, 1]")
        if not math.isclose(sum(probabilities.values()), 1.0, abs_tol=1e-6):
            raise ValueError("model probabilities must sum to 1")
    else:
        probabilities = None
    return {
        "market": market, "decision": direction, "confidence": confidence,
        "horizon_seconds": horizon, "reason": reason or ("MODEL_ABSTAINED" if direction == "ABSTAIN" else "MODEL_DECISION"),
        "probabilities": probabilities, "expected_return": expected_return,
        "input_bar_time": input_time,
    }


def _worker_main(adapter: TrustedModelAdapter, input_queue: Any, output_queue: Any) -> None:
    spec = adapter.spec
    buffers = {market: deque(maxlen=spec.lookback_bars) for market in spec.markets}
    identities: dict[str, tuple[str, int]] = {}
    last_times: dict[str, int] = {}
    try:
        output_queue.put({"kind": "state", "state": "WARMING_UP", "reason": "FEATURE_WARMUP_INCOMPLETE"})
        while True:
            item = input_queue.get()
            if item is None:
                return
            bar = ModelBar.validated(item, spec)
            prior_time = last_times.get(bar.market)
            if prior_time is not None and bar.time <= prior_time:
                raise ValueError("duplicate or out-of-order model bar")
            identity = (bar.contract, bar.instrument_id)
            prior_identity = identities.get(bar.market)
            if prior_identity is not None and prior_identity != identity and not spec.accepts_contract_roll:
                buffers[bar.market].clear()
                output_queue.put({"kind": "state", "state": "WARMING_UP", "reason": "CONTRACT_ROLL_REWARM"})
            identities[bar.market] = identity
            last_times[bar.market] = bar.time
            buffers[bar.market].append(asdict(bar))
            output_queue.put({"kind": "accepted"})
            if any(len(buffer) < spec.lookback_bars for buffer in buffers.values()):
                continue
            batch = {market: tuple(buffers[market]) for market in spec.markets}
            input_time = max(int(batch[market][-1]["time"]) for market in spec.markets)
            output_queue.put({"kind": "inference_start"})
            decisions = adapter.evaluate(batch)
            if isinstance(decisions, (str, bytes)) or not isinstance(decisions, Sequence):
                raise ValueError("model output must be a sequence")
            validated = [_validate_decision(value, spec, input_time) for value in decisions]
            output_queue.put({"kind": "result", "input_time": input_time, "decisions": validated})
    except BaseException as exc:
        output_queue.put({"kind": "error", "reason": f"{type(exc).__name__}: {exc}"})


class ModelRuntime:
    """Parent-side handle.  Any runtime fault is terminal until explicit restart."""

    def __init__(self, adapter: TrustedModelAdapter, *, queue_size: int = 4096, context: str = "spawn") -> None:
        self.adapter = adapter
        self.spec = adapter.spec
        self._ctx = mp.get_context(context)
        self._input = self._ctx.Queue(maxsize=queue_size)
        self._output = self._ctx.Queue(maxsize=max(16, queue_size))
        self._process: mp.Process | None = None
        self._oldest_pending: float | None = None
        self._queued_bars = 0
        self._error: str | None = None

    @property
    def error(self) -> str | None:
        return self._error

    def start(self) -> None:
        if self._process is not None:
            raise RuntimeError("model worker already started")
        self._process = self._ctx.Process(target=_worker_main, args=(self.adapter, self._input, self._output), daemon=True)
        self._process.start()

    def submit(self, bar: Mapping[str, Any]) -> None:
        if self._error is not None:
            raise RuntimeError(self._error)
        if self._process is None or not self._process.is_alive():
            self._fail("MODEL WORKER CRASHED")
            raise RuntimeError(self._error)
        try:
            self._input.put_nowait(dict(bar))
        except queue.Full as exc:
            self._fail("MODEL INPUT OVERFLOW")
            raise RuntimeError(self._error) from exc
        self._queued_bars += 1

    def poll(self) -> list[dict[str, Any]]:
        if self._error is not None:
            return []
        messages: list[dict[str, Any]] = []
        while True:
            try:
                message = self._output.get_nowait()
            except queue.Empty:
                break
            messages.append(dict(message))
            if message.get("kind") == "accepted":
                self._queued_bars = max(0, self._queued_bars - 1)
            if message.get("kind") == "inference_start":
                self._oldest_pending = time.monotonic()
            if message.get("kind") == "error":
                self._fail(str(message.get("reason") or "MODEL WORKER ERROR"))
                break
            if message.get("kind") == "result":
                self._oldest_pending = None
        if self._process is not None and not self._process.is_alive() and self._error is None:
            self._fail("MODEL WORKER CRASHED")
        if self._oldest_pending is not None and time.monotonic() - self._oldest_pending > self.spec.inference_timeout_seconds:
            self._fail("MODEL INFERENCE TIMEOUT")
            messages.append({"kind": "error", "reason": self._error})
        return messages

    @property
    def queued_bars(self) -> int:
        return self._queued_bars

    def _fail(self, reason: str) -> None:
        self._error = reason
        if self._process is not None and self._process.is_alive():
            self._process.terminate()
            self._process.join(timeout=1.0)

    def stop(self) -> None:
        if self._process is None:
            return
        if self._process.is_alive():
            try:
                self._input.put_nowait(None)
            except queue.Full:
                self._process.terminate()
            self._process.join(timeout=1.0)
            if self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=1.0)


class DecisionStore:
    """Minimal append-only decision and model-state log."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS decisions (
              prediction_id TEXT PRIMARY KEY, canonical_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS model_states (
              sequence INTEGER PRIMARY KEY AUTOINCREMENT, changed_at INTEGER NOT NULL,
              state TEXT NOT NULL, reason TEXT NOT NULL
            );
            """
        )
        self._connection.commit()

    def record_decision(self, prediction_id: str, payload: Mapping[str, Any]) -> None:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        with self._lock:
            row = self._connection.execute("SELECT canonical_json FROM decisions WHERE prediction_id = ?", (prediction_id,)).fetchone()
            if row is not None and row[0] != canonical:
                raise RuntimeError("prediction identity collision")
            self._connection.execute("INSERT OR IGNORE INTO decisions VALUES (?, ?)", (prediction_id, canonical))
            self._connection.commit()

    def record_state(self, state: str, reason: str) -> None:
        with self._lock:
            row = self._connection.execute("SELECT state, reason FROM model_states ORDER BY sequence DESC LIMIT 1").fetchone()
            if row == (state, reason):
                return
            self._connection.execute(
                "INSERT INTO model_states(changed_at, state, reason) VALUES (?, ?, ?)",
                (int(time.time()), state, reason),
            )
            self._connection.commit()

    def decisions(self) -> list[str]:
        with self._lock:
            return [row[0] for row in self._connection.execute("SELECT canonical_json FROM decisions ORDER BY prediction_id")]

    def close(self) -> None:
        with self._lock:
            self._connection.close()


class _FixtureAdapter:
    spec = ModelSpec(
        model_id="packaged-worker-fixture", version="1", artifact_sha256="0" * 64,
        strategy="fixture", markets=("ES",), schema="ohlcv-1s", lookback_bars=2,
        inference_timeout_seconds=2.0,
    )

    def self_check(self) -> bool:
        return True

    def evaluate(self, bars: Mapping[str, Sequence[Mapping[str, Any]]]) -> Sequence[ModelDecision]:
        latest = bars["ES"][-1]
        direction = "LONG" if float(latest["close"]) >= float(latest["open"]) else "SHORT"
        return [ModelDecision("ES", direction, 0.6, 60, "FIXTURE")]


def packaged_worker_self_check(timeout_seconds: float = 10.0) -> dict[str, Any]:
    runtime = ModelRuntime(_FixtureAdapter())
    runtime.start()
    base = int(time.time()) - 10
    for offset in (0, 1):
        runtime.submit({
            "market": "ES", "contract": "ESZ6", "instrument_id": 1,
            "schema": "ohlcv-1s", "time": base + offset, "open": 100.0,
            "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1.0,
        })
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline:
            messages = runtime.poll()
            result = next((message for message in messages if message.get("kind") == "result"), None)
            if result is not None:
                return {"ok": True, "decision": result["decisions"][0]["decision"]}
            if runtime.error is not None:
                raise RuntimeError(runtime.error)
            time.sleep(0.01)
        raise RuntimeError("packaged model worker self-check timed out")
    finally:
        runtime.stop()
