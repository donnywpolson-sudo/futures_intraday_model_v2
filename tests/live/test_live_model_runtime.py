from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import time
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

import pytest
import futures_rebuild.live_cockpit.engine as cockpit_engine
from futures_rebuild.live_cockpit.engine import LiveCockpitEngine
from futures_rebuild.live_cockpit.history import HistoryBinding
from futures_rebuild.live_cockpit.approval import build_live_smoke_plan, validate_live_smoke_plan

from futures_rebuild.live_cockpit.model_runtime import (
    DecisionStore,
    MAX_LOOKBACK_BARS,
    ModelDecision,
    ModelRuntime,
    ModelSpec,
    packaged_worker_self_check,
)


BASE_SPEC = ModelSpec(
    model_id="reviewed-fixture", version="1", artifact_sha256="0" * 64,
    strategy="deterministic-test", markets=("ES",), schema="ohlcv-1s",
    lookback_bars=2, inference_timeout_seconds=1.0,
)


class DeterministicAdapter:
    def __init__(self, spec: ModelSpec = BASE_SPEC) -> None:
        self.spec = spec

    def self_check(self) -> bool:
        return True

    def evaluate(self, bars: Mapping[str, Sequence[Mapping[str, Any]]]):
        market = self.spec.markets[0]
        latest = bars[market][-1]
        direction = "LONG" if latest["close"] >= latest["open"] else "SHORT"
        return [ModelDecision(market, direction, 0.7, 60, "deterministic")]


class CrashAdapter(DeterministicAdapter):
    def evaluate(self, bars):
        raise RuntimeError("synthetic crash")


class HangAdapter(DeterministicAdapter):
    def evaluate(self, bars):
        time.sleep(5)
        return []


class MalformedAdapter(DeterministicAdapter):
    def evaluate(self, bars):
        return [{"market": "ES", "decision": "LONG", "confidence": float("nan"), "horizon_seconds": 1}]


class FailedIdentityAdapter(DeterministicAdapter):
    def self_check(self) -> bool:
        return False


def _bar(second: int, *, market: str = "ES", contract: str = "ESZ6", instrument_id: int = 1) -> dict[str, Any]:
    return {
        "market": market, "contract": contract, "instrument_id": instrument_id,
        "schema": "ohlcv-1s", "time": int(time.time()) - 100 + second,
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 3.0,
    }


def _wait_messages(runtime: ModelRuntime, kind: str, timeout: float = 5.0) -> list[dict[str, Any]]:
    seen: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        seen.extend(runtime.poll())
        if any(message.get("kind") == kind for message in seen):
            return seen
        if runtime.error is not None and kind != "error":
            raise AssertionError(runtime.error)
        time.sleep(0.01)
    raise AssertionError(f"worker did not emit {kind}: {seen!r}, error={runtime.error!r}")


def _run(adapter: DeterministicAdapter, bars: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    runtime = ModelRuntime(adapter)
    runtime.start()
    try:
        for bar in bars:
            runtime.submit(bar)
        messages = _wait_messages(runtime, "result")
        return next(message["decisions"] for message in messages if message.get("kind") == "result")
    finally:
        runtime.stop()


def test_adapter_manifest_rejects_unknown_schema_bad_hash_and_oversized_warmup() -> None:
    with pytest.raises(ValueError, match="schema"):
        replace(BASE_SPEC, schema="trades")
    with pytest.raises(ValueError, match="SHA-256"):
        replace(BASE_SPEC, artifact_sha256="not-a-hash")
    with pytest.raises(ValueError, match="lookback"):
        replace(BASE_SPEC, lookback_bars=MAX_LOOKBACK_BARS + 1)


def test_replay_is_byte_equivalent_across_worker_restart() -> None:
    bars = [_bar(0), _bar(7)]  # sparse one-second OHLCV is valid and is not filled
    first = json.dumps(_run(DeterministicAdapter(), bars), sort_keys=True, separators=(",", ":"))
    second = json.dumps(_run(DeterministicAdapter(), bars), sort_keys=True, separators=(",", ":"))
    assert first == second


def test_cross_market_waits_for_every_market_and_keeps_bounded_warmup() -> None:
    spec = replace(BASE_SPEC, markets=("ES", "NQ"), lookback_bars=2)
    runtime = ModelRuntime(DeterministicAdapter(spec))
    runtime.start()
    try:
        runtime.submit(_bar(0, market="ES"))
        runtime.submit(_bar(1, market="ES"))
        runtime.submit(_bar(0, market="NQ", contract="NQZ6", instrument_id=2))
        time.sleep(0.1)
        assert not any(message.get("kind") == "result" for message in runtime.poll())
        runtime.submit(_bar(1, market="NQ", contract="NQZ6", instrument_id=2))
        assert any(message.get("kind") == "result" for message in _wait_messages(runtime, "result"))
        assert runtime.queued_bars == 0
    finally:
        runtime.stop()


def test_contract_roll_rewarms_before_next_decision() -> None:
    runtime = ModelRuntime(DeterministicAdapter())
    runtime.start()
    try:
        runtime.submit(_bar(0)); runtime.submit(_bar(1))
        _wait_messages(runtime, "result")
        runtime.submit(_bar(2, contract="ESH7", instrument_id=2))
        time.sleep(0.1)
        messages = runtime.poll()
        assert any(message.get("reason") == "CONTRACT_ROLL_REWARM" for message in messages)
        assert not any(message.get("kind") == "result" for message in messages)
        runtime.submit(_bar(3, contract="ESH7", instrument_id=2))
        _wait_messages(runtime, "result")
    finally:
        runtime.stop()


@pytest.mark.parametrize("adapter", [CrashAdapter(), MalformedAdapter()])
def test_worker_crash_and_malformed_nonfinite_output_are_terminal(adapter) -> None:
    runtime = ModelRuntime(adapter)
    runtime.start()
    try:
        runtime.submit(_bar(0)); runtime.submit(_bar(1))
        _wait_messages(runtime, "error")
        assert runtime.error
        with pytest.raises(RuntimeError):
            runtime.submit(_bar(2))
    finally:
        runtime.stop()


def test_hung_inference_times_out_without_retry() -> None:
    runtime = ModelRuntime(HangAdapter(replace(BASE_SPEC, inference_timeout_seconds=0.1)))
    runtime.start()
    try:
        runtime.submit(_bar(0)); runtime.submit(_bar(1))
        messages = _wait_messages(runtime, "error", timeout=3.0)
        assert any("TIMEOUT" in str(message.get("reason")) for message in messages)
        assert runtime.error == "MODEL INFERENCE TIMEOUT"
    finally:
        runtime.stop()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda bar: {**bar, "time": int(time.time()) + 100},
        lambda bar: {**bar, "close": float("inf")},
        lambda bar: {**bar, "market": "CL"},
        lambda bar: {**bar, "contract": ""},
    ],
)
def test_future_nonfinite_wrong_market_and_missing_contract_fail_closed(mutation) -> None:
    runtime = ModelRuntime(DeterministicAdapter())
    runtime.start()
    try:
        runtime.submit(mutation(_bar(0)))
        _wait_messages(runtime, "error")
        assert runtime.error
    finally:
        runtime.stop()


def test_duplicate_and_out_of_order_bars_fail_closed() -> None:
    for second in (0, -1):
        runtime = ModelRuntime(DeterministicAdapter())
        runtime.start()
        try:
            runtime.submit(_bar(0)); runtime.submit(_bar(second))
            _wait_messages(runtime, "error")
            assert runtime.error
        finally:
            runtime.stop()


def test_decision_store_is_idempotent_and_detects_identity_collision(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "model.sqlite3"
    store = DecisionStore(path)
    try:
        store.record_state("WARMING_UP", "FEATURE_WARMUP_INCOMPLETE")
        store.record_state("WARMING_UP", "FEATURE_WARMUP_INCOMPLETE")
        store.record_decision("one", {"decision": "LONG", "time": 1})
        store.record_decision("one", {"time": 1, "decision": "LONG"})
        assert len(store.decisions()) == 1
        with pytest.raises(RuntimeError, match="collision"):
            store.record_decision("one", {"decision": "SHORT", "time": 1})
    finally:
        store.close()


def test_actual_two_market_one_second_cadence_drains_without_drops_or_backlog() -> None:
    spec = replace(BASE_SPEC, markets=("ES", "NQ"), lookback_bars=3, inference_timeout_seconds=2.0)
    runtime = ModelRuntime(DeterministicAdapter(spec), queue_size=512)
    runtime.start()
    try:
        for second in range(100):
            runtime.submit(_bar(second, market="ES"))
            runtime.submit(_bar(second, market="NQ", contract="NQZ6", instrument_id=2))
            runtime.poll()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and runtime.queued_bars:
            runtime.poll()
            time.sleep(0.005)
        assert runtime.error is None
        assert runtime.queued_bars == 0
        # The parent-side queue counter is the authoritative no-drop accounting invariant.
    finally:
        runtime.stop()


def test_packaged_worker_fixture_spawns_and_decides() -> None:
    assert packaged_worker_self_check() == {"ok": True, "decision": "LONG"}


class _TopologyLive:
    instances: list["_TopologyLive"] = []
    active = 0
    max_active = 0

    def __init__(self, **_kwargs) -> None:
        self.subscriptions: list[dict[str, Any]] = []
        self.started = False
        self.stopped = False
        self.callback = None
        self.__class__.instances.append(self)

    def subscribe(self, **kwargs) -> None:
        self.subscriptions.append(dict(kwargs))

    def add_callback(self, callback) -> None:
        self.callback = callback

    def start(self) -> None:
        self.started = True
        self.__class__.active += 1
        self.__class__.max_active = max(self.__class__.max_active, self.__class__.active)

    def stop(self) -> None:
        if self.started and not self.stopped:
            self.__class__.active -= 1
        self.stopped = True

    def block_for_close(self, **_kwargs) -> None:
        return None


class _TopologyHistorical:
    def __init__(self, **_kwargs) -> None:
        self.symbology = SimpleNamespace(TIMEOUT=1)
        self.timeseries = SimpleNamespace(TIMEOUT=1)
        self.metadata = SimpleNamespace(TIMEOUT=1)


class _TopologyDb:
    Live = _TopologyLive
    Historical = _TopologyHistorical


def test_engine_uses_minute_overview_plus_model_session_and_no_focus_trade_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _TopologyLive.instances = []
    _TopologyLive.active = 0
    _TopologyLive.max_active = 0
    monkeypatch.setattr(
        cockpit_engine,
        "resolve_cockpit_api_key_source",
        lambda _env: SimpleNamespace(key="provider-free-fake", source="test"),
    )
    monkeypatch.setattr(cockpit_engine, "FOCUS_MAPPING_WAIT_SECONDS", 0.0)
    adapter = DeterministicAdapter(replace(BASE_SPEC, lookback_bars=1))
    engine = LiveCockpitEngine(
        cache_path=None,
        cache_enabled=False,
        history_enabled=False,
        reconnect_enabled=False,
        db_module=_TopologyDb,
        model_adapter=adapter,
        model_store_path=tmp_path / "decisions.sqlite3",
    )
    engine._market_bindings["ES"] = HistoryBinding("ES", "ESZ6", 1)
    events: list[dict[str, Any]] = []
    try:
        engine.start(events.append)
        deadline = time.monotonic() + 3
        while len(_TopologyLive.instances) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        schemas = [
            subscription["schema"]
            for client in _TopologyLive.instances
            for subscription in client.subscriptions
        ]
        assert schemas == ["ohlcv-1m", "ohlcv-1s"]
        assert "trades" not in schemas
        assert engine.runtime_metrics()["max_live_sessions"] == 2
        assert _TopologyLive.max_active == 2
    finally:
        engine.stop()


def test_one_minute_model_reuses_overview_and_stays_at_one_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _TopologyLive.instances = []
    _TopologyLive.active = 0
    _TopologyLive.max_active = 0
    monkeypatch.setattr(
        cockpit_engine,
        "resolve_cockpit_api_key_source",
        lambda _env: SimpleNamespace(key="provider-free-fake", source="test"),
    )
    monkeypatch.setattr(cockpit_engine, "FOCUS_MAPPING_WAIT_SECONDS", 0.0)
    adapter = DeterministicAdapter(
        replace(BASE_SPEC, schema="ohlcv-1m", lookback_bars=1, inference_timeout_seconds=2.0)
    )
    engine = LiveCockpitEngine(
        cache_path=None,
        cache_enabled=False,
        history_enabled=False,
        reconnect_enabled=False,
        db_module=_TopologyDb,
        model_adapter=adapter,
        model_store_path=tmp_path / "decisions.sqlite3",
    )
    engine._market_bindings["ES"] = HistoryBinding("ES", "ESZ6", 1)
    try:
        engine.start(lambda _event: None)
        time.sleep(0.1)
        assert [
            subscription["schema"]
            for client in _TopologyLive.instances
            for subscription in client.subscriptions
        ] == ["ohlcv-1m"]
        assert engine.runtime_metrics()["max_live_sessions"] == 1
    finally:
        engine.stop()


def test_model_identity_failure_blocks_before_any_live_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _TopologyLive.instances = []
    monkeypatch.setattr(
        cockpit_engine,
        "resolve_cockpit_api_key_source",
        lambda _env: SimpleNamespace(key="must-not-be-used", source="test"),
    )
    engine = LiveCockpitEngine(
        cache_path=None, cache_enabled=False, history_enabled=False,
        reconnect_enabled=False, db_module=_TopologyDb,
        model_adapter=FailedIdentityAdapter(),
        model_store_path=tmp_path / "decisions.sqlite3",
    )
    events: list[dict[str, Any]] = []
    try:
        engine.start(events.append)
        assert _TopologyLive.instances == []
        assert any("before live connection" in event["payload"].get("message", "") for event in events)
    finally:
        engine.stop()


def test_approved_smoke_plan_binds_exact_model_identity_schema_and_markets() -> None:
    binding = {
        "model_id": "reviewed-fixture", "version": "1", "artifact_sha256": "0" * 64,
        "schema": "ohlcv-1s", "markets": ["ES", "NQ"],
    }
    plan = build_live_smoke_plan(
        "a" * 64,
        source_revision="b" * 40,
        package_inputs=[{"path": "model.bin", "bytes": 1, "sha256": "c" * 64}],
        model_binding=binding,
    )
    assert validate_live_smoke_plan(plan) == plan
    assert plan["scope"]["model"] == binding
    assert plan["scope"]["maximum_live_sessions"] == 2
    changed = json.loads(json.dumps(plan))
    changed["scope"]["model"]["schema"] = "ohlcv-1m"
    with pytest.raises(Exception, match="identity"):
        validate_live_smoke_plan(changed)
