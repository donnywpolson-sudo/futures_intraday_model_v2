"""Deterministic demo and bounded two-session Databento cockpit engines."""

from __future__ import annotations

import math
import queue
import threading
import time
import uuid
from bisect import bisect_left
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from .feed import (
    DEFAULT_CONTINUOUS_SUFFIX,
    DEFAULT_DATASET,
    DEFAULT_HISTORICAL_SCHEMA,
    DEFAULT_SCHEMA,
    DEFAULT_STYPE_IN,
    EXCHANGE_TZ_NAME,
    GLOBEX_OPEN_HOUR,
    GLOBEX_OPEN_MINUTE,
    RTH_OPEN_HOUR,
    RTH_OPEN_MINUTE,
    ROOT,
    SUPPORTED_CHART_TIMEFRAMES,
    TradeCandleAggregator,
    aggregate_candles,
    candle_bucket_time,
    chart_market_universe,
    floor_timeframe,
    historical_store_to_candles,
    import_databento,
    is_databento_auth_error,
    normalize_timeframe,
    normalize_ts_event,
    ohlcv_record_to_candle,
    record_error_text,
    resolve_single_instrument,
    timeframe_seconds,
    trading_day_start,
)

from .cache import BarCache
from .credentials import CredentialLocatorError, resolve_cockpit_api_key_source
from .history import (
    PLAN_EXPIRY_MINUTES,
    HistoryBinding,
    HistoryChunk,
    HistoryPlan,
    group_history_chunks,
    missing_intervals,
    promote_market,
)
from .market_groups import load_alpha_tier_grouping
from .predictions import (
    NullPredictionSource,
    PredictionContext,
    SyntheticPredictionSource,
    now_utc,
)
from .protocol import event, serialize_bar, timestamp_seconds


Publish = Callable[[dict[str, Any]], None]
VISUAL_UPDATE_HZ = {
    "efficient": 5.0,
    "smooth": 10.0,
    "high": 15.0,
}
DEFAULT_VISUAL_UPDATE_MODE = "smooth"
MIN_RENDER_HZ = VISUAL_UPDATE_HZ["efficient"]
MAX_RENDER_HZ = VISUAL_UPDATE_HZ["high"]
RENDER_INTERVAL_SECONDS_OVERRIDE: float | None = None
FOCUS_SWITCH_DEBOUNCE_SECONDS = 0.15
FOCUS_MAPPING_WAIT_SECONDS = 3.0
DEFAULT_HISTORY_HOURS = 168
OVERVIEW_STALE_SECONDS = 150.0
FOCUS_LIVE_TAIL_MAX_AGE_SECONDS = 150.0
SYMBOL_REQUEST_TIMEOUT_SECONDS = 30
SYMBOL_RESOLUTION_ATTEMPTS = 2
SYMBOL_RESOLUTION_RETRY_DELAY_SECONDS = 1.0
HISTORY_REQUEST_TIMEOUT_SECONDS = 30
MAX_HISTORY_COST_ESTIMATE_REQUESTS = 8
LIVE_REPLAY_MAX_HOURS = 24
LIVE_REPLAY_SAFETY_MINUTES = 2
HISTORY_MAPPING_WAIT_SECONDS = 5.0

ERROR_CODE_NAMES = {
    1: "AUTH_FAILED",
    2: "API_KEY_DEACTIVATED",
    3: "CONNECTION_LIMIT_EXCEEDED",
    4: "SYMBOL_RESOLUTION_FAILED",
    5: "INVALID_SUBSCRIPTION",
    6: "INTERNAL_ERROR",
    7: "SKIPPED_RECORDS_AFTER_SLOW_READING",
}
SYSTEM_CODE_NAMES = {
    0: "HEARTBEAT",
    1: "SUBSCRIPTION_ACK",
    2: "SLOW_READER_WARNING",
    3: "REPLAY_COMPLETED",
    4: "END_OF_INTERVAL",
}


class _HistoryAvailabilityBoundaryError(ValueError):
    """A dataset-range response cannot authorize a historical request window."""


class CockpitEngine(Protocol):
    def bootstrap_event(self) -> dict[str, Any]: ...

    def start(self, publish: Publish) -> None: ...

    def select_market(self, market: str) -> bool: ...

    def select_timeframe(self, timeframe: str) -> bool: ...

    def retry_history(self) -> bool: ...

    def confirm_history_cache(self, plan_id: str) -> bool: ...

    def set_history_cache_paused(self, paused: bool) -> bool: ...

    def retry_history_cache_estimate(self) -> bool: ...

    def set_visual_update_mode(self, mode: str) -> bool: ...

    def set_visual_update_active(self, active: bool) -> float: ...

    def stop(self) -> None: ...


class VisualUpdateState:
    """Thread-safe desired/effective chart-render rate state."""

    def __init__(self, mode: str = DEFAULT_VISUAL_UPDATE_MODE) -> None:
        self._lock = threading.RLock()
        self._mode = mode if mode in VISUAL_UPDATE_HZ else DEFAULT_VISUAL_UPDATE_MODE
        self._active = True

    @property
    def mode(self) -> str:
        with self._lock:
            return self._mode

    def set_mode(self, mode: str) -> bool:
        normalized = str(mode).strip().lower()
        if normalized not in VISUAL_UPDATE_HZ:
            return False
        with self._lock:
            self._mode = normalized
        return True

    def set_active(self, active: bool) -> float:
        with self._lock:
            self._active = bool(active)
            return self._effective_hz_locked()

    def effective_hz(self) -> float:
        with self._lock:
            return self._effective_hz_locked()

    def interval_seconds(self) -> float:
        override = RENDER_INTERVAL_SECONDS_OVERRIDE
        if override is not None:
            return max(0.001, float(override))
        return 1.0 / self.effective_hz()

    def _effective_hz_locked(self) -> float:
        return VISUAL_UPDATE_HZ[self._mode] if self._active else MIN_RENDER_HZ


def _wait_for_visual_tick(
    stop_event: threading.Event,
    visual_updates: VisualUpdateState,
    previous_deadline: float | None,
) -> tuple[bool, float]:
    """Wait until the next monotonic deadline without issuing catch-up bursts."""

    interval = visual_updates.interval_seconds()
    now = time.monotonic()
    if previous_deadline is None or now > previous_deadline + interval:
        deadline = now + interval
    else:
        deadline = previous_deadline + interval
    return stop_event.wait(max(0.0, deadline - now)), deadline


def _record_field(record: object, name: str) -> object | None:
    if not hasattr(record, name):
        return None
    value = getattr(record, name)
    try:
        return value() if callable(value) else value
    except Exception:
        return None


def _resolution_timed_out(exc: Exception) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        name = type(current).__name__.lower()
        message = str(current).lower()
        if "timeout" in name or "timed out" in message or "read timeout" in message:
            return True
        current = current.__cause__ or current.__context__
    return False


def _connection_failed(exc: Exception) -> bool:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ConnectionError) or "connection" in type(current).__name__.lower():
            return True
        current = current.__cause__ or current.__context__
    return False


def _history_failure_details(exc: Exception) -> dict[str, bool | str]:
    timed_out = _resolution_timed_out(exc)
    if timed_out:
        category = "TIMEOUT"
    elif is_databento_auth_error(exc):
        category = "AUTHORIZATION"
    elif _connection_failed(exc):
        category = "CONNECTION"
    else:
        category = "UNAVAILABLE"
    return {"failure_category": category, "timed_out": timed_out}


def _history_diagnostic(
    *,
    phase: str,
    chunk_number: int | None = None,
    requested_start: datetime | None = None,
    requested_end: datetime | None = None,
    download_began: bool = False,
) -> dict[str, Any]:
    return {
        "phase": phase,
        "chunk_number": chunk_number,
        "requested_start": (
            timestamp_seconds(requested_start) if requested_start is not None else None
        ),
        "requested_end": (
            timestamp_seconds(requested_end) if requested_end is not None else None
        ),
        "download_began": download_began,
    }


def _merge_completed_history(
    current: Sequence[Mapping[str, Any]],
    fetched: Sequence[Mapping[str, Any]],
    *,
    completed_before: datetime,
) -> list[dict[str, Any]]:
    by_time = {normalize_ts_event(bar["time"]): dict(bar) for bar in current}
    for bar in fetched:
        timestamp = normalize_ts_event(bar["time"])
        if timestamp < completed_before:
            by_time[timestamp] = dict(bar)
    return [by_time[key] for key in sorted(by_time)]


def _recent_gap_replay_start(
    bars: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    lower_bound: datetime | None = None,
) -> datetime | None:
    """Return the first missing recent minute eligible for live replay."""

    end = floor_timeframe(now, 60)
    earliest = floor_timeframe(
        end
        - timedelta(hours=LIVE_REPLAY_MAX_HOURS)
        + timedelta(minutes=LIVE_REPLAY_SAFETY_MINUTES),
        60,
    )
    start = earliest
    if lower_bound is not None:
        start = max(start, floor_timeframe(lower_bound, 60))
    if start >= end:
        return None

    timestamps = sorted(
        {
            timestamp
            for bar in bars
            if start <= (timestamp := normalize_ts_event(bar["time"])) < end
        }
    )
    expected = start
    for timestamp in timestamps:
        if timestamp > expected:
            return expected
        expected = max(expected, timestamp + timedelta(minutes=1))
    return expected if expected < end else None


def _upsert_sorted_bar(
    bars: list[dict[str, Any]], bar: Mapping[str, Any]
) -> bool:
    """Insert or authoritatively replace a bar while preserving time order."""

    timestamp = normalize_ts_event(bar["time"])
    index = bisect_left(
        bars,
        timestamp,
        key=lambda item: normalize_ts_event(item["time"]),
    )
    value = dict(bar)
    if index < len(bars) and normalize_ts_event(bars[index]["time"]) == timestamp:
        bars[index] = value
        return False
    bars.insert(index, value)
    return True


def provider_control_message(record: object) -> dict[str, Any] | None:
    """Return normalized Databento error/slow-reader control metadata."""

    record_type = type(record).__name__
    lowered_type = record_type.lower()
    is_error = "errormsg" in lowered_type or hasattr(record, "err")
    is_system = "systemmsg" in lowered_type
    if not is_error and not is_system:
        return None

    raw_code = _record_field(record, "code")
    try:
        code = int(raw_code) if raw_code is not None else None
    except (TypeError, ValueError):
        code = None
    message = record_error_text(record).strip()

    if is_error:
        return {
            "provider_kind": "error",
            "provider_code": code,
            "provider_name": ERROR_CODE_NAMES.get(code, "UNKNOWN_ERROR"),
            "message": message or "Databento reported a live-stream error",
        }
    if code == 2:
        return {
            "provider_kind": "system",
            "provider_code": code,
            "provider_name": SYSTEM_CODE_NAMES[2],
            "message": message or "Databento reported a slow-reader warning",
        }
    return None


def _market_payload(info: object, *, alpha_tier_group: str | None = None) -> dict[str, Any]:
    return {
        "symbol": str(getattr(info, "symbol")),
        "name": str(
            getattr(info, "display_name", None) or getattr(info, "symbol")
        ),
        "family": str(getattr(info, "family", None) or "Other"),
        "description": str(getattr(info, "description", None) or ""),
        "alpha_tier_group": alpha_tier_group,
        "status": "WAITING",
        "last": None,
        "change_1m": None,
    }


def _bootstrap_payload(
    *,
    markets: Sequence[object],
    selected_market: str,
    timeframe: str,
    mode: str,
    history_hours: int = DEFAULT_HISTORY_HOURS,
) -> dict[str, Any]:
    demo_mode = mode == "demo"
    symbols = [str(getattr(info, "symbol")) for info in markets]
    alpha_tiers = load_alpha_tier_grouping(
        ROOT / "configs" / "alpha_tiered.yaml", symbols
    )
    return {
        "markets": [
            _market_payload(
                info,
                alpha_tier_group=alpha_tiers.market_groups.get(
                    str(getattr(info, "symbol"))
                ),
            )
            for info in markets
        ],
        "selected_market": selected_market,
        "timeframe": timeframe,
        "timeframes": list(SUPPORTED_CHART_TIMEFRAMES),
        "display_tz": "local",
        "mode": mode,
        "observation_only": True,
        "prediction_capability": {
            "mode": "synthetic_demo" if demo_mode else "offline",
            "synthetic": demo_mode,
            "observation_only": True,
        },
        "history_cache_capability": {
            "enabled": not demo_mode,
            "cost_confirmation_required": not demo_mode,
            "history_hours": history_hours,
            "market_count": len(markets),
        },
        "market_grouping_capability": alpha_tiers.capability_payload(),
        "visual_update_capability": {
            "default_mode": DEFAULT_VISUAL_UPDATE_MODE,
            "modes": dict(VISUAL_UPDATE_HZ),
            "adaptive_floor_hz": MIN_RENDER_HZ,
        },
        "max_provider_sessions": 0 if demo_mode else 2,
        "max_render_hz": MAX_RENDER_HZ,
    }


def _aggregate(bars: Sequence[Mapping[str, Any]], timeframe: str) -> list[dict[str, Any]]:
    if not bars:
        return []
    return aggregate_candles(
        bars,
        seconds=timeframe_seconds(timeframe),
        timeframe=timeframe,
    )


def _session_markers(
    bars: Sequence[Mapping[str, Any]], timeframe: str
) -> list[dict[str, Any]]:
    if not bars or timeframe == "1d":
        return []
    exchange_tz = ZoneInfo(EXCHANGE_TZ_NAME)
    first = normalize_ts_event(bars[0]["time"])
    last = normalize_ts_event(bars[-1]["time"])
    day = first.astimezone(exchange_tz).date()
    end_day = last.astimezone(exchange_tz).date()
    markers: list[dict[str, Any]] = []
    while day <= end_day:
        anchors = (
            (RTH_OPEN_HOUR, RTH_OPEN_MINUTE, "RTH", "#6ea8fe"),
            (GLOBEX_OPEN_HOUR, GLOBEX_OPEN_MINUTE, "Globex", "#7f8ba3"),
        )
        for hour, minute, label, color in anchors:
            exchange_time = datetime(
                day.year, day.month, day.day, hour, minute, tzinfo=exchange_tz
            )
            bucket = candle_bucket_time(exchange_time.astimezone(timezone.utc), timeframe)
            if first <= bucket <= last:
                markers.append(
                    {
                        "time": timestamp_seconds(bucket),
                        "position": "aboveBar",
                        "color": color,
                        "shape": "circle",
                        "text": label,
                    }
                )
        day += timedelta(days=1)
    unique: dict[tuple[int, str], dict[str, Any]] = {}
    for marker in markers:
        unique[(int(marker["time"]), str(marker["text"]))] = marker
    return list(unique.values())


def _snapshot_event(
    *,
    market: str,
    contract: str,
    timeframe: str,
    bars: Sequence[Mapping[str, Any]],
    source: str,
    generation: int,
) -> dict[str, Any]:
    aggregated = _aggregate(bars, timeframe)
    return event(
        "chart_snapshot",
        {
            "market": market,
            "contract": contract,
            "timeframe": timeframe,
            "bars": [serialize_bar(bar) for bar in aggregated],
            "markers": _session_markers(aggregated, timeframe),
            "source": source,
            "generation": generation,
        },
    )


def _data_health_payload(
    *,
    market: str,
    contract: str,
    instrument_id: int | None,
    timeframe: str,
    generation: int,
    bars: Sequence[Mapping[str, Any]],
    state: str,
    history_state: str,
    requested_hours: float,
    continuity_state: str,
    unexpected_gap_count: int | None,
    largest_gap_seconds: int | None,
    reason_codes: Sequence[str] = (),
    evaluated_at: datetime | None = None,
) -> dict[str, Any]:
    current = evaluated_at or now_utc()
    serialized_times = [timestamp_seconds(bar["time"]) for bar in bars]
    coverage_hours = (
        max(0.0, (serialized_times[-1] - serialized_times[0]) / 3600.0)
        if len(serialized_times) >= 2
        else 0.0
    )
    return {
        "market": market,
        "contract": contract,
        "instrument_id": instrument_id,
        "timeframe": timeframe,
        "generation": generation,
        "evaluated_at": timestamp_seconds(current),
        "last_bar_time": serialized_times[-1] if serialized_times else None,
        "state": state,
        "history": {
            "state": history_state,
            "requested_hours": float(requested_hours),
            "coverage_hours": coverage_hours,
            "bar_count": len(bars),
        },
        "continuity": {
            "state": continuity_state,
            "unexpected_gap_count": unexpected_gap_count,
            "largest_gap_seconds": largest_gap_seconds,
        },
        "reason_codes": list(reason_codes),
    }


class DemoCockpitEngine:
    """Network-free deterministic engine used for visual and integration checks."""

    def __init__(self, *, market: str = "ES", timeframe: str = "1m") -> None:
        self.markets = chart_market_universe()
        symbols = {info.symbol for info in self.markets}
        self.market = market if market in symbols else "ES"
        self.timeframe = normalize_timeframe(timeframe)
        self.generation = 1
        self._publish: Publish | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._bars: dict[str, list[dict[str, Any]]] = {}
        self._ticks = 0
        self._prediction_source = SyntheticPredictionSource()
        self._visual_updates = VisualUpdateState()

    def bootstrap_event(self) -> dict[str, Any]:
        return event(
            "bootstrap",
            _bootstrap_payload(
                markets=self.markets,
                selected_market=self.market,
                timeframe=self.timeframe,
                mode="demo",
            ),
        )

    def _base_price(self, market: str) -> float:
        index = next(
            (idx for idx, info in enumerate(self.markets) if info.symbol == market), 0
        )
        anchors = {"ES": 6378.25, "NQ": 23214.5, "CL": 68.42, "GC": 3368.2}
        return anchors.get(market, 85.0 + index * 17.25)

    def _market_bars(self, market: str) -> list[dict[str, Any]]:
        cached = self._bars.get(market)
        if cached is not None:
            return cached
        end = floor_timeframe(datetime.now(timezone.utc), 60)
        start = end - timedelta(days=3)
        base = self._base_price(market)
        seed = sum(ord(character) for character in market)
        bars: list[dict[str, Any]] = []
        previous = base
        for index in range(3 * 24 * 60):
            timestamp = start + timedelta(minutes=index)
            wave = math.sin((index + seed) / 31.0) * base * 0.00055
            drift = math.sin((index + seed) / 257.0) * base * 0.00008
            close = previous + wave * 0.09 + drift
            spread = max(base * 0.00008, abs(wave) * 0.22)
            bars.append(
                {
                    "time": timestamp,
                    "open": previous,
                    "high": max(previous, close) + spread,
                    "low": min(previous, close) - spread,
                    "close": close,
                    "volume": 80 + ((index * 37 + seed) % 1_750),
                }
            )
            previous = close
        self._bars[market] = bars
        return bars

    def start(self, publish: Publish) -> None:
        self._publish = publish
        publish(
            event(
                "history_cache_status",
                {
                    "state": "COMPLETE",
                    "ready_markets": len(self.markets),
                    "total_markets": len(self.markets),
                    "queued_markets": 0,
                    "active_market": None,
                    "estimated_cost_usd": None,
                    "estimate_expires_at": None,
                    "plan_id": None,
                    "paused": False,
                    "message": "Demo cache ready",
                },
            )
        )
        for index, info in enumerate(self.markets):
            bars = self._market_bars(info.symbol)
            latest = bars[-1]
            previous = bars[-2]
            state = "LIVE"
            if index == 10:
                state = "STALE"
            elif index == 20:
                state = "WAITING"
            elif index == 30:
                state = "ERROR"
            publish(
                event(
                    "market_status",
                    {
                        "market": info.symbol,
                        "last": float(latest["close"]),
                        "change_1m": (
                            (float(latest["close"]) / float(previous["close"]) - 1.0)
                            * 100.0
                        ),
                        "state": state,
                        "age_seconds": 0 if state == "LIVE" else 180,
                    },
                )
            )
        self._publish_focus_snapshot(source="demo")
        publish(
            event(
                "feed_status",
                {
                    "scope": "overview",
                    "state": "LIVE",
                    "message": "Deterministic 41-market demo",
                },
            )
        )
        self._thread = threading.Thread(
            target=self._run_updates, name="cockpit-demo", daemon=True
        )
        self._thread.start()

    def _publish_focus_snapshot(self, *, source: str) -> None:
        if self._publish is None:
            return
        raw_bars = self._market_bars(self.market)
        bars = _aggregate(raw_bars, self.timeframe)
        contract = f"{self.market}M6"
        instrument_id = 100_000 + next(
            (
                index
                for index, info in enumerate(self.markets)
                if info.symbol == self.market
            ),
            0,
        )
        self._publish(
            _snapshot_event(
                market=self.market,
                contract=contract,
                timeframe=self.timeframe,
                bars=raw_bars,
                source=source,
                generation=self.generation,
            )
        )
        health_state = "CURRENT"
        history_state = "COMPLETE"
        continuity_state = "PASS"
        unexpected_gap_count: int | None = 0
        largest_gap_seconds: int | None = 0
        health_reasons: list[str] = []
        if self.market == "RTY":
            health_state = "DEGRADED"
            history_state = "PARTIAL"
            health_reasons = ["HISTORY_PARTIAL"]
        elif self.market == "YM":
            health_state = "DEGRADED"
            continuity_state = "WARN"
            unexpected_gap_count = 2
            largest_gap_seconds = 180
            health_reasons = ["CONTINUITY_WARNING"]
        elif self.market == "CL":
            health_state = "STALE"
            health_reasons = ["DATA_STALE"]
        elif self.market == "NG":
            health_state = "UNKNOWN"
            history_state = "UNAVAILABLE"
            continuity_state = "NOT_EVALUATED"
            unexpected_gap_count = None
            largest_gap_seconds = None
            health_reasons = ["HISTORY_UNAVAILABLE", "CONTINUITY_NOT_EVALUATED"]
        evaluated_at = now_utc()
        self._publish(
            event(
                "data_health",
                _data_health_payload(
                    market=self.market,
                    contract=contract,
                    instrument_id=instrument_id,
                    timeframe=self.timeframe,
                    generation=self.generation,
                    bars=bars,
                    state=health_state,
                    history_state=history_state,
                    requested_hours=72.0,
                    continuity_state=continuity_state,
                    unexpected_gap_count=unexpected_gap_count,
                    largest_gap_seconds=largest_gap_seconds,
                    reason_codes=health_reasons,
                    evaluated_at=evaluated_at,
                ),
            )
        )
        self._publish(
            event(
                "prediction_update",
                self._prediction_source.build(
                    PredictionContext(
                        market=self.market,
                        contract=contract,
                        instrument_id=instrument_id,
                        timeframe=self.timeframe,
                        generation=self.generation,
                        bars=bars,
                        prediction_time=evaluated_at,
                    )
                ),
            )
        )
        self._publish(
            event(
                "feed_status",
                {
                    "scope": "focus",
                    "market": self.market,
                    "state": "LIVE",
                    "message": "Demo stream active",
                },
            )
        )

    def _run_updates(self) -> None:
        deadline: float | None = None
        while True:
            stopped, deadline = _wait_for_visual_tick(
                self._stop_event, self._visual_updates, deadline
            )
            if stopped:
                return
            with self._lock:
                bars = self._market_bars(self.market)
                latest = dict(bars[-1])
                self._ticks += 1
                movement = math.sin(self._ticks / 4.0) * float(latest["close"]) * 0.000015
                latest["close"] = float(latest["close"]) + movement
                latest["high"] = max(float(latest["high"]), float(latest["close"]))
                latest["low"] = min(float(latest["low"]), float(latest["close"]))
                latest["volume"] = int(latest["volume"]) + 3 + self._ticks % 9
                bars[-1] = latest
                recent_count = max(4, timeframe_seconds(self.timeframe) // 60 + 2)
                aggregated = _aggregate(bars[-recent_count:], self.timeframe)
                if not aggregated or self._publish is None:
                    continue
                self._publish(
                    event(
                        "bar_update",
                        {
                            "market": self.market,
                            "timeframe": self.timeframe,
                            "bar": serialize_bar(aggregated[-1]),
                            "generation": self.generation,
                        },
                    )
                )

    def select_market(self, market: str) -> bool:
        if market not in {info.symbol for info in self.markets}:
            return False
        with self._lock:
            if market == self.market:
                return True
            self.market = market
            self.generation += 1
            self._publish_focus_snapshot(source="demo")
        return True

    def select_timeframe(self, timeframe: str) -> bool:
        normalized = normalize_timeframe(timeframe)
        if normalized not in SUPPORTED_CHART_TIMEFRAMES:
            return False
        with self._lock:
            if normalized == self.timeframe:
                return True
            self.timeframe = normalized
            self.generation += 1
            self._publish_focus_snapshot(source="timeframe-cache")
        return True

    def retry_history(self) -> bool:
        return False

    def confirm_history_cache(self, plan_id: str) -> bool:
        return False

    def set_history_cache_paused(self, paused: bool) -> bool:
        return False

    def retry_history_cache_estimate(self) -> bool:
        return False

    def set_visual_update_mode(self, mode: str) -> bool:
        return self._visual_updates.set_mode(mode)

    def set_visual_update_active(self, active: bool) -> float:
        return self._visual_updates.set_active(active)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=1.0)


class LiveCockpitEngine:
    """Two-session live engine: all-market minute overview plus one trade focus."""

    def __init__(
        self,
        *,
        cache_path: Path | None,
        market: str = "ES",
        timeframe: str = "1m",
        env: Mapping[str, str] | None = None,
        db_module: ModuleType | None = None,
        history_hours: int = DEFAULT_HISTORY_HOURS,
        history_enabled: bool = True,
        cache_enabled: bool = True,
        reconnect_enabled: bool = True,
        fail_fast_provider_errors: bool = False,
    ) -> None:
        self.markets = chart_market_universe()
        self._symbols = {info.symbol for info in self.markets}
        self.market = market if market in self._symbols else "ES"
        self.timeframe = normalize_timeframe(timeframe)
        # Preserve None so the shared resolver can apply its normal runtime
        # precedence, including repo/frozen api.env files. An explicit mapping
        # remains useful for isolated tests and callers that intentionally want
        # environment-only resolution.
        self.env = None if env is None else dict(env)
        self.db_module = db_module
        self.history_hours = history_hours
        self.history_enabled = history_enabled
        self.cache_enabled = cache_enabled
        self.reconnect_enabled = reconnect_enabled
        self.fail_fast_provider_errors = fail_fast_provider_errors
        if cache_enabled and cache_path is None:
            raise ValueError("cache_path is required when cache_enabled is true")
        self.cache = BarCache(cache_path) if cache_enabled and cache_path is not None else None
        self.generation = 0
        self._publish: Publish | None = None
        self._api_key: str | None = None
        self._historical: object | None = None
        self._overview_client: object | None = None
        self._focus_client: object | None = None
        self._overview_instruments: dict[int, str] = {}
        self._overview_latest: dict[str, tuple[float, datetime]] = {}
        self._overview_latest_bars: dict[str, tuple[int, dict[str, Any]]] = {}
        self._overview_pending_cache: dict[
            int, tuple[str, str, list[dict[str, Any]]]
        ] = {}
        self._market_bindings: dict[str, HistoryBinding] = {}
        self._raw_bars: list[dict[str, Any]] = []
        self._contract = self.market
        self._resolved_instrument_id: int | None = None
        self._switch_queue: queue.Queue[tuple[str, int]] = queue.Queue()
        self._history_thread: threading.Thread | None = None
        self._history_wakeup = threading.Event()
        self._history_lock = threading.RLock()
        self._history_plan: HistoryPlan | None = None
        self._history_reestimate_requested = False
        self._history_active_chunk: HistoryChunk | None = None
        self._history_active_chunk_number: int | None = None
        self._history_plan_chunk_number = 0
        self._history_ready_markets = 0
        self._history_dataset_range_requests = 0
        self._history_cost_estimate_requests = 0
        self._history_plan_confirmations = 0
        self._history_available_end: datetime | None = None
        self._pending_update: tuple[int, dict[str, Any]] | None = None
        self._pending_snapshot_generation: int | None = None
        self._pending_cache: list[dict[str, Any]] = []
        self._pending_cache_generation: int | None = None
        self._lock = threading.RLock()
        self._overview_lock = threading.RLock()
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._trading_day = trading_day_start(datetime.now(timezone.utc))
        if self.cache is not None:
            try:
                stored_bindings = self.cache.get_market_bindings(
                    dataset=DEFAULT_DATASET,
                    session_start=self._trading_day,
                )
            except Exception:
                stored_bindings = {}
            for stored_market, (stored_contract, stored_instrument_id) in stored_bindings.items():
                if stored_market in self._symbols:
                    self._market_bindings[stored_market] = HistoryBinding(
                        market=stored_market,
                        contract=stored_contract,
                        instrument_id=stored_instrument_id,
                    )
        self._focus_live_announced_generation: int | None = None
        self._provider_failure: dict[str, Any] | None = None
        self._active_live_clients: dict[int, str] = {}
        self._live_sessions_started = 0
        self._max_live_sessions = 0
        self._history_requests = 0
        self._replay_subscriptions = 0
        self._last_replay_start: datetime | None = None
        self._replay_watermarks: dict[int, datetime] = {}
        self._cache_reads = 0
        self._cache_writes = 0
        self._history_retry_active = False
        self._history_failure: dict[str, Any] | None = None
        self._history_complete_generation: int | None = None
        self._focus_last_live_event: tuple[int, str, datetime] | None = None
        self._shutdown_errors: list[str] = []
        self._metrics_lock = threading.RLock()
        self._prediction_source = NullPredictionSource()
        self._visual_updates = VisualUpdateState()

    def bootstrap_event(self) -> dict[str, Any]:
        return event(
            "bootstrap",
            _bootstrap_payload(
                markets=self.markets,
                selected_market=self.market,
                timeframe=self.timeframe,
                mode="live",
                history_hours=self.history_hours,
            ),
        )

    def _emit(self, event_type: str, payload: Mapping[str, Any]) -> None:
        if self._publish is not None:
            self._publish(event(event_type, payload))

    def _history_health_state(self, *, generation: int, has_bars: bool) -> str:
        with self._lock:
            if self._history_complete_generation == generation:
                return "COMPLETE"
        with self._metrics_lock:
            failure_generation = (
                int(self._history_failure.get("generation", -1))
                if self._history_failure is not None
                else -1
            )
        if failure_generation == generation:
            return "UNAVAILABLE"
        return "PARTIAL" if has_bars else "LOADING"

    def _live_tail_is_fresh(
        self,
        *,
        generation: int,
        contract: str,
        evaluated_at: datetime,
    ) -> bool:
        with self._lock:
            live_event = self._focus_last_live_event
        if live_event is None:
            return False
        live_generation, live_contract, live_time = live_event
        age_seconds = (evaluated_at - live_time).total_seconds()
        return (
            live_generation == generation
            and live_contract == contract
            and 0.0 <= age_seconds <= FOCUS_LIVE_TAIL_MAX_AGE_SECONDS
        )

    def _focus_health_payload(
        self,
        *,
        market: str,
        contract: str,
        instrument_id: int | None,
        timeframe: str,
        bars: Sequence[Mapping[str, Any]],
        generation: int,
        history_state: str | None = None,
        evaluated_at: datetime | None = None,
    ) -> dict[str, Any]:
        aggregated = _aggregate(bars, timeframe)
        resolved_history_state = history_state or self._history_health_state(
            generation=generation, has_bars=bool(aggregated)
        )
        evaluated = evaluated_at or now_utc()
        if resolved_history_state != "COMPLETE":
            health_state = "DEGRADED"
            health_reasons = [
                {
                    "LOADING": "HISTORY_LOADING",
                    "PARTIAL": "HISTORY_PARTIAL",
                    "UNAVAILABLE": "HISTORY_UNAVAILABLE",
                }.get(resolved_history_state, "HISTORY_UNAVAILABLE")
            ]
        elif not aggregated:
            health_state = "UNKNOWN"
            health_reasons = ["NO_BAR_DATA"]
        elif not self._live_tail_is_fresh(
            generation=generation,
            contract=contract,
            evaluated_at=evaluated,
        ):
            health_state = "DEGRADED"
            health_reasons = ["DATA_STALE"]
        else:
            health_state = "CURRENT"
            health_reasons = []
        health_reasons.append("CONTINUITY_NOT_EVALUATED")
        return _data_health_payload(
            market=market,
            contract=contract,
            instrument_id=instrument_id,
            timeframe=timeframe,
            generation=generation,
            bars=aggregated,
            state=health_state,
            history_state=resolved_history_state,
            requested_hours=float(self.history_hours),
            continuity_state="NOT_EVALUATED",
            unexpected_gap_count=None,
            largest_gap_seconds=None,
            reason_codes=health_reasons,
            evaluated_at=evaluated,
        )

    def _publish_current_focus_health(self, *, evaluated_at: datetime | None = None) -> None:
        with self._lock:
            market = self.market
            contract = self._contract
            instrument_id = self._resolved_instrument_id
            timeframe = self.timeframe
            generation = self.generation
            bars = list(self._raw_bars)
        if self._publish is None:
            return
        self._emit(
            "data_health",
            self._focus_health_payload(
                market=market,
                contract=contract,
                instrument_id=instrument_id,
                timeframe=timeframe,
                bars=bars,
                generation=generation,
                evaluated_at=evaluated_at,
            ),
        )

    def _publish_focus_bundle(
        self,
        *,
        market: str,
        contract: str,
        instrument_id: int | None,
        timeframe: str,
        bars: Sequence[Mapping[str, Any]],
        source: str,
        generation: int,
        history_state: str | None = None,
    ) -> None:
        if self._publish is None:
            return
        aggregated = _aggregate(bars, timeframe)
        evaluated_at = now_utc()
        self._publish(
            _snapshot_event(
                market=market,
                contract=contract,
                timeframe=timeframe,
                bars=bars,
                source=source,
                generation=generation,
            )
        )
        self._publish(
            event(
                "data_health",
                self._focus_health_payload(
                    market=market,
                    contract=contract,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
                    bars=bars,
                    generation=generation,
                    history_state=history_state,
                    evaluated_at=evaluated_at,
                ),
            )
        )
        self._publish(
            event(
                "prediction_update",
                self._prediction_source.build(
                    PredictionContext(
                        market=market,
                        contract=contract,
                        instrument_id=instrument_id,
                        timeframe=timeframe,
                        generation=generation,
                        bars=aggregated,
                        prediction_time=evaluated_at,
                    )
                ),
            )
        )

    def start(self, publish: Publish) -> None:
        self._publish = publish
        try:
            key_resolution = resolve_cockpit_api_key_source(self.env)
        except CredentialLocatorError as exc:
            self._emit(
                "feed_status",
                {
                    "scope": "all",
                    "state": "ERROR",
                    "message": f"Credential configuration error: {exc}",
                },
            )
            return
        if key_resolution is None:
            self._emit(
                "feed_status",
                {
                    "scope": "all",
                    "state": "ERROR",
                    "message": "Databento API key is not configured",
                },
            )
            return
        self._api_key = key_resolution.key
        try:
            self.db_module = self.db_module or import_databento()
            historical_cls = getattr(self.db_module, "Historical")
            self._historical = historical_cls(key=self._api_key)
            for api_name, timeout_seconds in (
                ("symbology", SYMBOL_REQUEST_TIMEOUT_SECONDS),
                ("timeseries", HISTORY_REQUEST_TIMEOUT_SECONDS),
                ("metadata", HISTORY_REQUEST_TIMEOUT_SECONDS),
            ):
                api = getattr(self._historical, api_name, None)
                if api is not None:
                    setattr(api, "TIMEOUT", timeout_seconds)
        except Exception as exc:
            self._emit(
                "feed_status",
                {"scope": "all", "state": "ERROR", "message": str(exc)},
            )
            return

        self._start_overview()
        if self._stop_event.is_set():
            return
        workers = [
            (self._switch_worker, "cockpit-focus-switch"),
            (self._render_worker, "cockpit-render"),
        ]
        if self.reconnect_enabled:
            workers.append((self._maintenance_worker, "cockpit-maintenance"))
        for target, name in workers:
            thread = threading.Thread(target=target, name=name, daemon=True)
            self._threads.append(thread)
            thread.start()
        self._ensure_history_worker()
        self.select_market(self.market, force=True)

    def _new_live_client(self) -> object:
        if self.db_module is None or self._api_key is None:
            raise RuntimeError("Databento live client is unavailable")
        live_cls = getattr(self.db_module, "Live")
        kwargs: dict[str, object] = {"key": self._api_key}
        if not self.reconnect_enabled:
            kwargs["reconnect_policy"] = "none"
        return live_cls(**kwargs)

    def _add_live_callback(
        self,
        client: object,
        callback: Callable[[object], None],
        *,
        scope: str,
    ) -> None:
        add_callback = getattr(client, "add_callback")

        def exception_callback(exc: Exception) -> None:
            self._mark_provider_failure(
                scope=scope,
                message=f"Live client exception: {type(exc).__name__}: {exc}",
                provider_kind="exception",
                provider_code=None,
                provider_name="CLIENT_EXCEPTION",
            )

        try:
            add_callback(callback, exception_callback)
        except TypeError:
            add_callback(callback)

    def _register_started_client(self, client: object, *, scope: str) -> None:
        with self._metrics_lock:
            self._active_live_clients[id(client)] = scope
            self._live_sessions_started += 1
            self._max_live_sessions = max(
                self._max_live_sessions, len(self._active_live_clients)
            )

    def _stop_client(self, client: object | None) -> None:
        if client is None:
            return
        error: Exception | None = None
        try:
            stop = getattr(client, "stop", None)
            if callable(stop):
                stop()
            wait_for_close = getattr(client, "block_for_close", None)
            if callable(wait_for_close):
                try:
                    wait_for_close(timeout=1.0)
                except TypeError:
                    wait_for_close()
        except Exception as exc:
            error = exc
        finally:
            with self._metrics_lock:
                self._active_live_clients.pop(id(client), None)
                if error is not None:
                    self._shutdown_errors.append(
                        f"{type(error).__name__}: {error}"
                    )

    def _mark_provider_failure(
        self,
        *,
        scope: str,
        message: str,
        provider_kind: str,
        provider_code: int | None,
        provider_name: str,
    ) -> None:
        payload = {
            "scope": scope,
            "state": "ERROR",
            "message": message,
            "provider_kind": provider_kind,
            "provider_code": provider_code,
            "provider_name": provider_name,
        }
        with self._metrics_lock:
            if self._provider_failure is None:
                self._provider_failure = dict(payload)
        self._emit("feed_status", payload)
        if self.fail_fast_provider_errors:
            self._stop_event.set()

    def _handle_provider_control(self, record: object, *, scope: str) -> bool:
        control = provider_control_message(record)
        if control is None:
            return False
        self._mark_provider_failure(scope=scope, **control)
        return True

    def runtime_metrics(self) -> dict[str, Any]:
        with self._metrics_lock:
            return {
                "live_sessions_started": self._live_sessions_started,
                "active_live_sessions": len(self._active_live_clients),
                "max_live_sessions": self._max_live_sessions,
                "history_requests": self._history_requests,
                "history_dataset_range_requests": self._history_dataset_range_requests,
                "history_cost_estimate_requests": self._history_cost_estimate_requests,
                "history_plan_confirmations": self._history_plan_confirmations,
                "history_ready_markets": self._history_ready_markets,
                "replay_subscriptions": self._replay_subscriptions,
                "last_replay_start_utc": (
                    self._last_replay_start.isoformat()
                    if self._last_replay_start is not None
                    else None
                ),
                "cache_reads": self._cache_reads,
                "cache_writes": self._cache_writes,
                "history_retry_active": self._history_retry_active,
                "history_failure": (
                    dict(self._history_failure)
                    if self._history_failure is not None
                    else None
                ),
                "provider_failure": (
                    dict(self._provider_failure)
                    if self._provider_failure is not None
                    else None
                ),
                "shutdown_errors": list(self._shutdown_errors),
                "contract": self._contract,
                "instrument_id": self._resolved_instrument_id,
                "history_enabled": self.history_enabled,
                "cache_enabled": self.cache_enabled,
                "reconnect_enabled": self.reconnect_enabled,
                "history_available_end_utc": (
                    self._history_available_end.isoformat()
                    if self._history_available_end is not None
                    else None
                ),
            }

    def _history_target(
        self,
        *,
        now: datetime | None = None,
        available_end: datetime | None = None,
    ) -> tuple[datetime, datetime]:
        completed_minute = floor_timeframe(now or datetime.now(timezone.utc), 60)
        requested_start = completed_minute - timedelta(hours=self.history_hours)
        resolved_available_end = (
            available_end
            if available_end is not None
            else self._history_available_end
        )
        historical_end = (
            min(completed_minute, resolved_available_end)
            if resolved_available_end is not None
            else completed_minute
        )
        return requested_start, historical_end

    def _lookup_history_available_end(
        self,
        *,
        completed_minute: datetime,
        requested_start: datetime,
    ) -> datetime:
        if self._historical is None:
            raise RuntimeError("historical client is unavailable")
        metadata = getattr(self._historical, "metadata", None)
        get_dataset_range = getattr(metadata, "get_dataset_range", None)
        if not callable(get_dataset_range):
            raise _HistoryAvailabilityBoundaryError("dataset range is unavailable")
        with self._metrics_lock:
            self._history_dataset_range_requests += 1
        value = get_dataset_range(dataset=DEFAULT_DATASET)
        if not isinstance(value, Mapping):
            raise _HistoryAvailabilityBoundaryError("dataset range is malformed")
        schema_ranges = value.get("schema")
        if not isinstance(schema_ranges, Mapping):
            raise _HistoryAvailabilityBoundaryError("dataset schema range is absent")
        schema_range = schema_ranges.get(DEFAULT_HISTORICAL_SCHEMA)
        if not isinstance(schema_range, Mapping):
            raise _HistoryAvailabilityBoundaryError("historical schema range is absent")
        raw_end = schema_range.get("end")
        if not isinstance(raw_end, str) or not raw_end.strip():
            raise _HistoryAvailabilityBoundaryError("historical schema end is absent")
        try:
            parsed_end = datetime.fromisoformat(raw_end.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise _HistoryAvailabilityBoundaryError(
                "historical schema end is malformed"
            ) from exc
        if parsed_end.tzinfo is None or parsed_end.utcoffset() is None:
            raise _HistoryAvailabilityBoundaryError(
                "historical schema end is timezone-less"
            )
        available_end = floor_timeframe(parsed_end.astimezone(timezone.utc), 60)
        historical_end = min(completed_minute, available_end)
        if historical_end <= requested_start:
            raise _HistoryAvailabilityBoundaryError(
                "historical schema end is outside the requested window"
            )
        return available_end

    def _missing_history(
        self,
        *,
        binding: HistoryBinding,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, datetime]]:
        if self.cache is None:
            return [(start, end)]
        coverage = self.cache.get_coverage(
            dataset=DEFAULT_DATASET,
            instrument_id=binding.instrument_id,
            start=start,
            end=end,
        )
        return missing_intervals(start=start, end=end, coverage=coverage)

    def _ready_market_count(
        self,
        *,
        bindings: Sequence[HistoryBinding],
        start: datetime,
        end: datetime,
    ) -> int:
        return sum(
            not self._missing_history(binding=binding, start=start, end=end)
            for binding in bindings
        )

    def _emit_history_cache_status(
        self,
        state: str,
        message: str,
        *,
        plan: HistoryPlan | None = None,
        failure_category: str | None = None,
        diagnostic: Mapping[str, Any] | None = None,
        active_market: str | None = None,
    ) -> None:
        with self._history_lock:
            current_plan = plan if plan is not None else self._history_plan
            ready_markets = self._history_ready_markets
            queued_markets = (
                len(
                    {
                        binding.market
                        for chunk in current_plan.chunks
                        for binding in chunk.bindings
                    }
                )
                if current_plan is not None
                else 0
            )
            paused = bool(current_plan.paused) if current_plan is not None else False
        payload: dict[str, Any] = {
            "state": state,
            "ready_markets": ready_markets,
            "total_markets": len(self.markets),
            "queued_markets": queued_markets,
            "active_market": active_market,
            "estimated_cost_usd": (
                current_plan.estimated_cost_usd if current_plan is not None else None
            ),
            "estimate_expires_at": (
                timestamp_seconds(current_plan.expires_at)
                if current_plan is not None
                else None
            ),
            "plan_id": current_plan.plan_id if current_plan is not None else None,
            "paused": paused,
            "message": message,
        }
        if failure_category is not None:
            payload["failure_category"] = failure_category
        if diagnostic is not None:
            payload["diagnostic"] = dict(diagnostic)
        self._emit("history_cache_status", payload)

    def _remember_binding(self, binding: HistoryBinding) -> None:
        reestimate = False
        changed = False
        with self._history_lock:
            previous = self._market_bindings.get(binding.market)
            self._market_bindings[binding.market] = binding
            changed = previous != binding
            if (
                previous is not None
                and previous != binding
                and self._history_plan is not None
                and not self._history_plan.confirmed
            ):
                self._history_plan = None
                self._history_reestimate_requested = True
                reestimate = True
        if reestimate:
            self._history_wakeup.set()
        if changed and self.cache is not None:
            try:
                self.cache.put_market_binding(
                    dataset=DEFAULT_DATASET,
                    market=binding.market,
                    raw_symbol=binding.contract,
                    instrument_id=binding.instrument_id,
                    session_start=self._trading_day,
                )
            except Exception:
                pass

    def _record_history_failure(
        self,
        *,
        failure_category: str,
        diagnostic: Mapping[str, Any],
    ) -> None:
        with self._lock:
            generation = self.generation
            self._history_complete_generation = None
        with self._metrics_lock:
            self._history_failure = {
                "failure_category": failure_category,
                "generation": generation,
                "diagnostic": dict(diagnostic),
            }

    def _set_selected_history_coverage(
        self,
        *,
        missing_by_instrument: Mapping[int, Sequence[tuple[datetime, datetime]]],
    ) -> None:
        with self._lock:
            instrument_id = self._resolved_instrument_id
            generation = self.generation
            if (
                instrument_id is not None
                and instrument_id in missing_by_instrument
                and not missing_by_instrument[instrument_id]
            ):
                self._history_complete_generation = generation
            else:
                self._history_complete_generation = None

    @staticmethod
    def _symbology_result(value: object) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            return {}
        result = value.get("result")
        return result if isinstance(result, Mapping) else {}

    @staticmethod
    def _active_symbology_value(entries: object, *, end_date: str) -> str | None:
        if not isinstance(entries, list):
            return None
        active: list[str] = []
        fallback: list[str] = []
        requested_end = datetime.fromisoformat(end_date).date()
        for entry in entries:
            if not isinstance(entry, Mapping) or entry.get("s") in {None, ""}:
                continue
            symbol = str(entry["s"])
            fallback.append(symbol)
            try:
                start = datetime.fromisoformat(str(entry.get("d0") or "0001-01-01")).date()
                end = datetime.fromisoformat(str(entry.get("d1") or "9999-12-31")).date()
            except ValueError:
                continue
            if start < requested_end <= end:
                active.append(symbol)
        candidates = tuple(dict.fromkeys(active or fallback))
        return candidates[0] if len(candidates) == 1 else None

    def _resolve_history_bindings(
        self,
        *,
        target_start: datetime,
        target_end: datetime,
    ) -> list[HistoryBinding]:
        with self._history_lock:
            known = dict(self._market_bindings)
        missing = sorted(self._symbols - set(known))
        if not missing or self._historical is None:
            return sorted(known.values(), key=lambda item: item.market)
        symbology = getattr(self._historical, "symbology", None)
        resolve = getattr(symbology, "resolve", None)
        if not callable(resolve):
            return sorted(known.values(), key=lambda item: item.market)
        start_date = target_start.date().isoformat()
        end_date = (target_end.date() + timedelta(days=1)).isoformat()
        queries = [f"{market}{DEFAULT_CONTINUOUS_SUFFIX}" for market in missing]
        mapping = resolve(
            dataset=DEFAULT_DATASET,
            symbols=queries,
            stype_in="continuous",
            stype_out="instrument_id",
            start_date=start_date,
            end_date=end_date,
        )
        result = self._symbology_result(mapping)
        instrument_by_market: dict[str, int] = {}
        for market, query in zip(missing, queries, strict=True):
            value = self._active_symbology_value(
                result.get(query) or result.get(query.upper()), end_date=end_date
            )
            if value is not None:
                try:
                    instrument_by_market[market] = int(value)
                except ValueError:
                    continue
        raw_by_instrument: dict[int, str] = {}
        if instrument_by_market:
            raw_mapping = resolve(
                dataset=DEFAULT_DATASET,
                symbols=list(instrument_by_market.values()),
                stype_in=DEFAULT_STYPE_IN,
                stype_out="raw_symbol",
                start_date=start_date,
                end_date=end_date,
            )
            raw_result = self._symbology_result(raw_mapping)
            for instrument_id in instrument_by_market.values():
                value = self._active_symbology_value(
                    raw_result.get(str(instrument_id)) or raw_result.get(instrument_id),
                    end_date=end_date,
                )
                if value:
                    raw_by_instrument[instrument_id] = value
        for market, instrument_id in instrument_by_market.items():
            binding = HistoryBinding(
                market=market,
                contract=raw_by_instrument.get(instrument_id, market),
                instrument_id=instrument_id,
            )
            known[market] = binding
            self._remember_binding(binding)
        return sorted(known.values(), key=lambda item: item.market)

    def _estimate_history_cost(self, chunks: Sequence[HistoryChunk]) -> float:
        if self._historical is None:
            raise RuntimeError("historical client is unavailable")
        metadata = getattr(self._historical, "metadata", None)
        get_cost = getattr(metadata, "get_cost", None)
        if not callable(get_cost):
            raise RuntimeError("historical cost estimator is unavailable")
        total = 0.0
        for chunk in chunks:
            with self._metrics_lock:
                self._history_cost_estimate_requests += 1
            total += float(
                get_cost(
                    dataset=DEFAULT_DATASET,
                    schema=DEFAULT_HISTORICAL_SCHEMA,
                    symbols=[binding.instrument_id for binding in chunk.bindings],
                    stype_in=DEFAULT_STYPE_IN,
                    start=chunk.start,
                    end=chunk.end,
                )
            )
        if not math.isfinite(total) or total < 0.0:
            raise ValueError("invalid historical cost estimate")
        return total

    def _prepare_history_plan(self) -> None:
        if (
            self._stop_event.is_set()
            or not self.history_enabled
            or self.cache is None
            or self._historical is None
        ):
            return
        self._emit_history_cache_status("CHECKING", "Checking one-week cache coverage")
        with self._lock:
            self._history_complete_generation = None
        completed_minute = floor_timeframe(datetime.now(timezone.utc), 60)
        requested_start = completed_minute - timedelta(hours=self.history_hours)
        range_diagnostic = _history_diagnostic(
            phase="DATASET_RANGE",
            requested_start=requested_start,
            requested_end=completed_minute,
        )
        try:
            available_end = self._lookup_history_available_end(
                completed_minute=completed_minute,
                requested_start=requested_start,
            )
        except Exception as exc:
            failure_category = (
                "DATA_AVAILABILITY"
                if isinstance(exc, _HistoryAvailabilityBoundaryError)
                else str(_history_failure_details(exc)["failure_category"])
            )
            with self._history_lock:
                self._history_plan = None
                self._history_retry_active = False
                self._history_available_end = None
            self._record_history_failure(
                failure_category=failure_category,
                diagnostic=range_diagnostic,
            )
            self._emit_history_cache_status(
                "ERROR",
                "Historical availability could not be verified; no history was downloaded.",
                failure_category=failure_category,
                diagnostic=range_diagnostic,
            )
            self._publish_current_focus_health(evaluated_at=completed_minute)
            return
        with self._history_lock:
            self._history_available_end = available_end
        self._publish_current_focus_health(evaluated_at=completed_minute)
        target_start, target_end = self._history_target(
            now=completed_minute,
            available_end=available_end,
        )
        binding_diagnostic = _history_diagnostic(
            phase="BINDING_RESOLUTION",
            requested_start=target_start,
            requested_end=target_end,
        )
        try:
            bindings = self._resolve_history_bindings(
                target_start=target_start,
                target_end=target_end,
            )
        except Exception as exc:
            failure_category = str(_history_failure_details(exc)["failure_category"])
            with self._history_lock:
                self._history_plan = None
                self._history_retry_active = False
            self._record_history_failure(
                failure_category=failure_category,
                diagnostic=binding_diagnostic,
            )
            self._emit_history_cache_status(
                "ERROR",
                "Some market contracts could not be resolved; live data remains available.",
                failure_category=failure_category,
                diagnostic=binding_diagnostic,
            )
            self._publish_current_focus_health(evaluated_at=completed_minute)
            return
        cache_diagnostic = _history_diagnostic(
            phase="CACHE_COVERAGE",
            requested_start=target_start,
            requested_end=target_end,
        )
        try:
            missing_by_instrument = {
                binding.instrument_id: self._missing_history(
                    binding=binding, start=target_start, end=target_end
                )
                for binding in bindings
            }
        except Exception:
            with self._history_lock:
                self._history_plan = None
                self._history_retry_active = False
            self._record_history_failure(
                failure_category="UNAVAILABLE",
                diagnostic=cache_diagnostic,
            )
            self._emit_history_cache_status(
                "ERROR",
                "The local history cache is unavailable; live data remains available.",
                failure_category="UNAVAILABLE",
                diagnostic=cache_diagnostic,
            )
            self._publish_current_focus_health(evaluated_at=completed_minute)
            return
        ready = sum(not intervals for intervals in missing_by_instrument.values())
        with self._history_lock:
            self._history_ready_markets = ready
        self._set_selected_history_coverage(
            missing_by_instrument=missing_by_instrument,
        )
        with self._metrics_lock:
            self._history_failure = None
        self._publish_current_focus_health(evaluated_at=completed_minute)
        chunks = group_history_chunks(bindings, missing_by_instrument)
        if not chunks:
            with self._history_lock:
                self._history_plan = None
                self._history_retry_active = False
            if ready == len(self.markets):
                self._emit_history_cache_status(
                    "COMPLETE", "One-week history is cached for all markets"
                )
            else:
                self._record_history_failure(
                    failure_category="UNAVAILABLE",
                    diagnostic=binding_diagnostic,
                )
                self._emit_history_cache_status(
                    "ERROR",
                    "Some market contracts could not be resolved; live data remains available.",
                    failure_category="UNAVAILABLE",
                    diagnostic=binding_diagnostic,
                )
            return
        cost_diagnostic = _history_diagnostic(
            phase="COST_ESTIMATE",
            requested_start=target_start,
            requested_end=target_end,
        )
        if len(chunks) > MAX_HISTORY_COST_ESTIMATE_REQUESTS:
            with self._history_lock:
                self._history_plan = None
                self._history_retry_active = False
            self._record_history_failure(
                failure_category="UNAVAILABLE",
                diagnostic=cost_diagnostic,
            )
            self._emit_history_cache_status(
                "ERROR",
                "History cost could not be estimated; no history was downloaded.",
                failure_category="UNAVAILABLE",
                diagnostic=cost_diagnostic,
            )
            return
        try:
            estimated_cost = self._estimate_history_cost(chunks)
        except Exception as exc:
            failure_category = str(_history_failure_details(exc)["failure_category"])
            with self._history_lock:
                self._history_plan = None
                self._history_retry_active = False
            self._record_history_failure(
                failure_category=failure_category,
                diagnostic=cost_diagnostic,
            )
            self._emit_history_cache_status(
                "ERROR",
                "History cost could not be estimated; no history was downloaded.",
                failure_category=failure_category,
                diagnostic=cost_diagnostic,
            )
            return
        created_at = datetime.now(timezone.utc)
        plan = HistoryPlan(
            plan_id=uuid.uuid4().hex,
            created_at=created_at,
            expires_at=created_at + timedelta(minutes=PLAN_EXPIRY_MINUTES),
            target_start=target_start,
            target_end=target_end,
            estimated_cost_usd=estimated_cost,
            chunks=list(chunks),
        )
        with self._history_lock:
            if self._stop_event.is_set():
                return
            self._history_plan = plan
            self._history_plan_chunk_number = 0
            self._history_retry_active = False
        self._emit_history_cache_status(
            "CONFIRMATION_REQUIRED",
            "Confirm the estimated cost to update missing one-week history.",
            plan=plan,
        )

    def _group_history_store(
        self,
        store: object,
        *,
        instrument_ids: Sequence[int],
    ) -> dict[int, list[dict[str, Any]]]:
        grouped = {int(instrument_id): [] for instrument_id in instrument_ids}
        try:
            records = iter(store)
        except TypeError:
            if len(instrument_ids) != 1:
                raise ValueError("multi-market history response is not iterable")
            grouped[int(instrument_ids[0])] = [
                dict(bar) for bar in historical_store_to_candles(store)
            ]
            return grouped
        for record in records:
            instrument_value = _record_field(record, "instrument_id")
            if instrument_value is None and len(instrument_ids) == 1:
                instrument_value = instrument_ids[0]
            try:
                instrument_id = int(instrument_value)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                continue
            if instrument_id not in grouped:
                continue
            grouped[instrument_id].append(dict(ohlcv_record_to_candle(record)))
        return grouped

    def _fetch_history_chunk(
        self, chunk: HistoryChunk
    ) -> tuple[dict[int, list[dict[str, Any]]], datetime]:
        if self._historical is None:
            raise RuntimeError("historical client is unavailable")
        instrument_ids = [binding.instrument_id for binding in chunk.bindings]
        request: dict[str, Any] = {
            "dataset": DEFAULT_DATASET,
            "schema": DEFAULT_HISTORICAL_SCHEMA,
            "symbols": instrument_ids,
            "stype_in": DEFAULT_STYPE_IN,
            "start": chunk.start,
            "end": chunk.end,
        }
        with self._metrics_lock:
            self._history_requests += 1
        store = self._historical.timeseries.get_range(**request)
        return (
            self._group_history_store(store, instrument_ids=instrument_ids),
            chunk.end,
        )

    @staticmethod
    def _mapping_symbol(record: object) -> str | None:
        if not hasattr(record, "stype_in_symbol") or not hasattr(record, "instrument_id"):
            return None
        value = getattr(record, "stype_in_symbol")
        value = value() if callable(value) else value
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        return str(value).strip().upper().removesuffix(DEFAULT_CONTINUOUS_SUFFIX.upper())

    @staticmethod
    def _mapping_raw_symbol(record: object) -> str | None:
        value = _record_field(record, "stype_out_symbol")
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        text = str(value or "").strip().upper()
        return text or None

    def _on_overview_record(self, record: object) -> None:
        if self._handle_provider_control(record, scope="overview"):
            return
        mapped_market = self._mapping_symbol(record)
        if mapped_market in self._symbols:
            instrument_id = getattr(record, "instrument_id")
            instrument_id = instrument_id() if callable(instrument_id) else instrument_id
            instrument_id = int(instrument_id)
            self._overview_instruments[instrument_id] = mapped_market
            raw_symbol = self._mapping_raw_symbol(record)
            if raw_symbol is not None:
                binding = HistoryBinding(
                    market=mapped_market,
                    contract=raw_symbol,
                    instrument_id=instrument_id,
                )
                with self._history_lock:
                    previous_binding = self._market_bindings.get(mapped_market)
                self._remember_binding(binding)
                with self._lock:
                    selected_identity = (
                        self.market,
                        self._contract,
                        self._resolved_instrument_id,
                    )
                if (
                    previous_binding is not None
                    and previous_binding != binding
                    and selected_identity[0] == mapped_market
                    and selected_identity[2] is not None
                    and selected_identity[1:] != (raw_symbol, instrument_id)
                ):
                    self.select_market(mapped_market, force=True)
            return
        if all(hasattr(record, field) for field in ("open", "high", "low", "close", "volume")):
            try:
                instrument_id_value = getattr(record, "instrument_id")
                instrument_id_value = (
                    instrument_id_value()
                    if callable(instrument_id_value)
                    else instrument_id_value
                )
                market = self._overview_instruments.get(int(instrument_id_value))
                if market is None:
                    return
                candle = ohlcv_record_to_candle(record)
                close = float(candle["close"])
                timestamp = normalize_ts_event(candle["time"])
                candle_value = dict(candle)
                binding = self._market_bindings.get(market)
                raw_symbol = binding.contract if binding is not None else market
                with self._overview_lock:
                    previous = self._overview_latest.get(market)
                    self._overview_latest[market] = (close, timestamp)
                    self._overview_latest_bars[market] = (
                        int(instrument_id_value),
                        candle_value,
                    )
                    if self.cache is not None:
                        pending = self._overview_pending_cache.get(int(instrument_id_value))
                        if pending is None or pending[1] != raw_symbol:
                            self._overview_pending_cache[int(instrument_id_value)] = (
                                market,
                                raw_symbol,
                                [candle_value],
                            )
                        else:
                            pending[2].append(candle_value)
                change = None
                if previous is not None and previous[0] != 0:
                    change = (close / previous[0] - 1.0) * 100.0
                self._emit(
                    "market_status",
                    {
                        "market": market,
                        "last": close,
                        "change_1m": change,
                        "state": "LIVE",
                        "age_seconds": max(
                            0.0,
                            (datetime.now(timezone.utc) - timestamp).total_seconds(),
                        ),
                    },
                )
            except Exception:
                return
            return
    def _refresh_overview_staleness(self, *, now: datetime | None = None) -> None:
        current = now or datetime.now(timezone.utc)
        for market, (last, timestamp) in list(self._overview_latest.items()):
            age_seconds = max(0.0, (current - timestamp).total_seconds())
            if age_seconds <= OVERVIEW_STALE_SECONDS:
                continue
            self._emit(
                "market_status",
                {
                    "market": market,
                    "last": last,
                    "change_1m": None,
                    "state": "STALE",
                    "age_seconds": age_seconds,
                },
            )

    def _start_overview(self) -> None:
        if self.db_module is None or self._api_key is None or self._stop_event.is_set():
            return
        with self._overview_lock:
            client: object | None = None
            try:
                client = self._new_live_client()
                client.subscribe(
                    dataset=DEFAULT_DATASET,
                    schema=DEFAULT_HISTORICAL_SCHEMA,
                    symbols=[f"{symbol}{DEFAULT_CONTINUOUS_SUFFIX}" for symbol in self._symbols],
                    stype_in="continuous",
                )
                self._add_live_callback(
                    client, self._on_overview_record, scope="overview"
                )
                client.start()
                self._register_started_client(client, scope="overview")
                if self._stop_event.is_set():
                    self._stop_client(client)
                    return
                self._overview_client = client
                self._emit(
                    "feed_status",
                    {
                        "scope": "overview",
                        "state": "LIVE",
                        "message": "41-market minute overview connected",
                    },
                )
            except Exception as exc:
                self._stop_client(client)
                self._overview_client = None
                self._emit(
                    "feed_status",
                    {
                        "scope": "overview",
                        "state": "ERROR",
                        "message": f"Overview unavailable: {exc}",
                    },
                )

    def _stop_overview(self) -> None:
        with self._overview_lock:
            self._stop_client(self._overview_client)
            self._overview_client = None
            self._overview_instruments.clear()

    def _publish_cached_selection(
        self,
        *,
        binding: HistoryBinding,
        generation: int,
    ) -> bool:
        now = datetime.now(timezone.utc)
        lookback_start = now - timedelta(hours=self.history_hours)
        completed_end = floor_timeframe(now, 60)
        cached: list[dict[str, Any]] = []
        if self.cache is not None:
            cached = self.cache.get_bars(
                dataset=DEFAULT_DATASET,
                instrument_id=binding.instrument_id,
                start=lookback_start,
                end=completed_end,
            )
            with self._metrics_lock:
                self._cache_reads += 1
        bars_by_time = {normalize_ts_event(bar["time"]): dict(bar) for bar in cached}
        overview_bar = self._overview_latest_bars.get(binding.market)
        if overview_bar is not None and overview_bar[0] == binding.instrument_id:
            overview_candle = overview_bar[1]
            overview_time = normalize_ts_event(overview_candle["time"])
            if overview_time <= now:
                bars_by_time[overview_time] = dict(overview_candle)
        if not bars_by_time:
            return False
        visible_bars = [bars_by_time[key] for key in sorted(bars_by_time)]
        target_start, target_end = self._history_target(now=now)
        availability_known = self._history_available_end is not None
        history_missing = (
            self._missing_history(
                binding=binding,
                start=target_start,
                end=target_end,
            )
            if availability_known
            else [(target_start, target_end)]
        )
        history_state = (
            "COMPLETE"
            if availability_known and not history_missing
            else "PARTIAL"
            if availability_known
            else "LOADING"
        )
        with self._lock:
            if generation != self.generation or self._stop_event.is_set():
                return False
            self._raw_bars = visible_bars
            self._contract = binding.contract
            self._resolved_instrument_id = binding.instrument_id
            timeframe = self.timeframe
            if history_state == "COMPLETE":
                self._history_complete_generation = generation
        self._publish_focus_bundle(
            market=binding.market,
            contract=binding.contract,
            instrument_id=binding.instrument_id,
            timeframe=timeframe,
            bars=visible_bars,
            source="selection-cache",
            generation=generation,
            history_state=history_state,
        )
        return True

    def select_market(self, market: str, *, force: bool = False) -> bool:
        normalized = market.strip().upper()
        if normalized not in self._symbols:
            return False
        with self._lock:
            if normalized == self.market and not force and self.generation > 0:
                if self._resolved_instrument_id is not None:
                    return True
            self.market = normalized
            self.generation += 1
            generation = self.generation
            self._contract = normalized
            self._resolved_instrument_id = None
            self._history_complete_generation = None
            self._focus_last_live_event = None
        with self._metrics_lock:
            self._history_failure = None
        self._emit(
            "feed_status",
            {
                "scope": "focus",
                "market": normalized,
                "state": "RESOLVING",
                "message": f"Resolving {normalized}",
            },
        )
        with self._history_lock:
            known_binding = self._market_bindings.get(normalized)
            if self._history_plan is not None and self._history_plan.confirmed:
                self._history_plan.chunks = promote_market(
                    self._history_plan.chunks, normalized
                )
                self._history_wakeup.set()
        if known_binding is not None and self._publish_cached_selection(
            binding=known_binding,
            generation=generation,
        ):
            self._emit(
                "feed_status",
                {
                    "scope": "focus",
                    "market": normalized,
                    "state": "CONNECTING",
                    "message": f"Cached {known_binding.contract} chart ready; connecting live stream",
                },
            )
        self._switch_queue.put((normalized, generation))
        return True

    def select_timeframe(self, timeframe: str) -> bool:
        try:
            normalized = normalize_timeframe(timeframe)
        except Exception:
            return False
        if normalized not in SUPPORTED_CHART_TIMEFRAMES:
            return False
        with self._lock:
            self.timeframe = normalized
            market = self.market
            contract = self._contract
            instrument_id = self._resolved_instrument_id
            generation = self.generation
            bars = list(self._raw_bars)
        self._publish_focus_bundle(
            market=market,
            contract=contract,
            instrument_id=instrument_id,
            timeframe=normalized,
            bars=bars,
            source="timeframe-cache",
            generation=generation,
        )
        return True

    def _ensure_history_worker(self) -> None:
        with self._lock:
            if self._stop_event.is_set():
                return
            if self._history_thread is not None and self._history_thread.is_alive():
                return
            thread = threading.Thread(
                target=self._history_worker,
                name="cockpit-history-refresh",
                daemon=True,
            )
            self._history_thread = thread
            self._threads.append(thread)
        thread.start()

    def _history_worker(self) -> None:
        if self._stop_event.wait(HISTORY_MAPPING_WAIT_SECONDS):
            return
        with self._history_lock:
            self._history_reestimate_requested = False
        self._prepare_history_plan()
        while not self._stop_event.is_set():
            with self._history_lock:
                reestimate = self._history_reestimate_requested
                self._history_reestimate_requested = False
                plan = self._history_plan
                chunk = None
                if (
                    not reestimate
                    and plan is not None
                    and plan.confirmed
                    and not plan.paused
                    and plan.chunks
                ):
                    chunk = plan.chunks.pop(0)
                    self._history_plan_chunk_number += 1
                    self._history_active_chunk = chunk
                    self._history_active_chunk_number = self._history_plan_chunk_number
            if reestimate:
                self._prepare_history_plan()
                continue
            if chunk is None or plan is None:
                self._history_wakeup.wait(0.2)
                self._history_wakeup.clear()
                continue
            active_market = chunk.bindings[0].market if len(chunk.bindings) == 1 else None
            chunk_number = self._history_active_chunk_number
            self._emit_history_cache_status(
                "WARMING",
                "Updating missing one-week history in the background",
                plan=plan,
                active_market=active_market,
            )
            try:
                grouped, covered_end = self._fetch_history_chunk(chunk)
                if self._stop_event.is_set():
                    return
                if self.cache is None:
                    raise RuntimeError("history cache is unavailable")
                batches = [
                    (
                        DEFAULT_DATASET,
                        binding.instrument_id,
                        binding.contract,
                        grouped.get(binding.instrument_id, []),
                    )
                    for binding in chunk.bindings
                    if grouped.get(binding.instrument_id)
                ]
                if batches:
                    self.cache.put_bar_batches(batches)
                    with self._metrics_lock:
                        self._cache_writes += 1
                for binding in chunk.bindings:
                    self.cache.record_coverage(
                        dataset=DEFAULT_DATASET,
                        instrument_id=binding.instrument_id,
                        raw_symbol=binding.contract,
                        start=chunk.start,
                        end=covered_end,
                    )
                self._refresh_selected_from_history_chunk(
                    chunk=chunk,
                    grouped=grouped,
                    covered_end=covered_end,
                    plan=plan,
                )
            except Exception as exc:
                details = _history_failure_details(exc)
                failure_category = str(details["failure_category"])
                diagnostic = _history_diagnostic(
                    phase="DOWNLOAD",
                    chunk_number=chunk_number,
                    requested_start=chunk.start,
                    requested_end=chunk.end,
                    download_began=True,
                )
                with self._history_lock:
                    self._history_active_chunk = None
                    self._history_active_chunk_number = None
                    if self._history_plan is plan:
                        self._history_plan = None
                    self._history_retry_active = False
                self._record_history_failure(
                    failure_category=failure_category,
                    diagnostic=diagnostic,
                )
                message = (
                    "History update timed out; cached and live data remain available."
                    if details["timed_out"]
                    else "History update failed; cached and live data remain available."
                )
                self._emit_history_cache_status(
                    "ERROR",
                    message,
                    failure_category=failure_category,
                    diagnostic=diagnostic,
                )
                self._publish_current_focus_health()
                continue
            with self._history_lock:
                self._history_active_chunk = None
                self._history_active_chunk_number = None
                bindings = list(self._market_bindings.values())
            missing_by_instrument = {
                binding.instrument_id: self._missing_history(
                    binding=binding,
                    start=plan.target_start,
                    end=plan.target_end,
                )
                for binding in bindings
            }
            ready = sum(not intervals for intervals in missing_by_instrument.values())
            self._set_selected_history_coverage(
                missing_by_instrument=missing_by_instrument,
            )
            with self._history_lock:
                self._history_ready_markets = ready
                remaining = bool(plan.chunks)
                if not remaining and self._history_plan is plan:
                    self._history_plan = None
                    self._history_retry_active = False
            with self._metrics_lock:
                self._history_failure = None
            if remaining:
                self._emit_history_cache_status(
                    "WARMING",
                    "Updating missing one-week history in the background",
                    plan=plan,
                )
            elif ready == len(self.markets):
                self._emit_history_cache_status(
                    "COMPLETE", "One-week history is cached for all markets"
                )
            else:
                self._emit_history_cache_status(
                    "PARTIAL",
                    "Some market history remains incomplete; refresh the estimate to retry.",
                )
            self._publish_current_focus_health()

    def retry_history(self) -> bool:
        return self.retry_history_cache_estimate()

    def retry_history_cache_estimate(self) -> bool:
        if self._stop_event.is_set() or not self.history_enabled or self.cache is None:
            return False
        with self._history_lock:
            if self._history_active_chunk is not None or self._history_retry_active:
                return False
            if self._history_plan is not None and self._history_plan.confirmed:
                return False
            self._history_plan = None
            self._history_retry_active = True
            self._history_reestimate_requested = True
        self._ensure_history_worker()
        self._emit_history_cache_status("CHECKING", "Refreshing history cost estimate")
        self._history_wakeup.set()
        return True

    def confirm_history_cache(self, plan_id: str) -> bool:
        expired = False
        with self._history_lock:
            plan = self._history_plan
            if (
                plan is None
                or plan.confirmed
                or plan.plan_id != str(plan_id)
                or self._history_active_chunk is not None
            ):
                return False
            if datetime.now(timezone.utc) >= plan.expires_at:
                self._history_plan = None
                expired = True
            else:
                plan.confirmed = True
                plan.paused = False
                self._history_plan_confirmations += 1
        if expired:
            diagnostic = _history_diagnostic(
                phase="COST_ESTIMATE",
                requested_start=plan.target_start if plan is not None else None,
                requested_end=plan.target_end if plan is not None else None,
            )
            self._record_history_failure(
                failure_category="UNAVAILABLE",
                diagnostic=diagnostic,
            )
            self._emit_history_cache_status(
                "ERROR",
                "The history estimate expired; refresh it before downloading.",
                failure_category="UNAVAILABLE",
                diagnostic=diagnostic,
            )
            return False
        self._emit_history_cache_status(
            "WARMING",
            "Updating missing one-week history in the background",
            plan=plan,
        )
        self._history_wakeup.set()
        return True

    def set_history_cache_paused(self, paused: bool) -> bool:
        if not isinstance(paused, bool):
            return False
        with self._history_lock:
            plan = self._history_plan
            if plan is None or not plan.confirmed:
                return False
            plan.paused = paused
        self._emit_history_cache_status(
            "PAUSED" if paused else "WARMING",
            (
                "History update will pause after the current request"
                if paused and self._history_active_chunk is not None
                else "History cache update paused"
                if paused
                else "History cache update resumed"
            ),
            plan=plan,
        )
        self._history_wakeup.set()
        return True

    def set_visual_update_mode(self, mode: str) -> bool:
        return self._visual_updates.set_mode(mode)

    def set_visual_update_active(self, active: bool) -> float:
        return self._visual_updates.set_active(active)

    def _refresh_selected_from_history_chunk(
        self,
        *,
        chunk: HistoryChunk,
        grouped: Mapping[int, Sequence[Mapping[str, Any]]],
        covered_end: datetime,
        plan: HistoryPlan,
    ) -> None:
        with self._lock:
            instrument_id = self._resolved_instrument_id
            generation = self.generation
            market = self.market
            contract = self._contract
            timeframe = self.timeframe
            if instrument_id is None or not any(
                binding.instrument_id == instrument_id for binding in chunk.bindings
            ):
                return
            fetched = grouped.get(instrument_id, ())
            merged = _merge_completed_history(
                self._raw_bars,
                fetched,
                completed_before=covered_end,
            )
            self._raw_bars = merged
        binding = next(
            binding for binding in chunk.bindings if binding.instrument_id == instrument_id
        )
        history_state = (
            "COMPLETE"
            if not self._missing_history(
                binding=binding, start=plan.target_start, end=plan.target_end
            )
            else "PARTIAL"
        )
        with self._lock:
            if history_state == "COMPLETE" and generation == self.generation:
                self._history_complete_generation = generation
        self._publish_focus_bundle(
            market=market,
            contract=contract,
            instrument_id=instrument_id,
            timeframe=timeframe,
            bars=merged,
            source="historical",
            generation=generation,
            history_state=history_state,
        )

    def _switch_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                selected = self._switch_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            if self._stop_event.wait(FOCUS_SWITCH_DEBOUNCE_SECONDS):
                return
            while True:
                try:
                    selected = self._switch_queue.get_nowait()
                except queue.Empty:
                    break
            market, generation = selected
            self._activate_focus(market, generation)

    def _wait_for_live_binding(
        self, market: str, generation: int
    ) -> HistoryBinding | None:
        deadline = time.monotonic() + FOCUS_MAPPING_WAIT_SECONDS
        while not self._stop_event.is_set() and generation == self.generation:
            with self._history_lock:
                binding = self._market_bindings.get(market)
            if binding is not None:
                return binding
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            if self._stop_event.wait(min(0.05, remaining)):
                return None
        return None

    def _activate_focus(self, market: str, generation: int) -> None:
        if self._historical is None or self.db_module is None or self._api_key is None:
            return
        now = datetime.now(timezone.utc)
        lookback_start = now - timedelta(hours=self.history_hours)
        end = floor_timeframe(now, 60)
        known_binding = self._wait_for_live_binding(market, generation)
        resolved = (
            SimpleNamespace(
                market=known_binding.market,
                raw_symbol=known_binding.contract,
                instrument_id=known_binding.instrument_id,
            )
            if known_binding is not None
            else None
        )
        if resolved is None:
            for attempt in range(1, SYMBOL_RESOLUTION_ATTEMPTS + 1):
                try:
                    resolved = resolve_single_instrument(
                        self._historical,
                        dataset=DEFAULT_DATASET,
                        market=market,
                        query_symbol=f"{market}{DEFAULT_CONTINUOUS_SUFFIX}",
                        start_date=lookback_start.date().isoformat(),
                        end_date=(now.date() + timedelta(days=1)).isoformat(),
                    )
                    break
                except Exception as exc:
                    timed_out = _resolution_timed_out(exc)
                    if timed_out and attempt < SYMBOL_RESOLUTION_ATTEMPTS:
                        self._emit(
                            "feed_status",
                            {
                                "scope": "focus",
                                "market": market,
                                "state": "RESOLVING",
                                "message": (
                                    "Contract service timed out; retrying "
                                    f"{market} ({attempt + 1}/{SYMBOL_RESOLUTION_ATTEMPTS})"
                                ),
                            },
                        )
                        if self._stop_event.wait(SYMBOL_RESOLUTION_RETRY_DELAY_SECONDS):
                            return
                        if generation != self.generation:
                            return
                        continue
                    message = (
                        "Contract lookup timed out twice. The 41-market overview is still "
                        f"live; select Retry to try {market} again."
                        if timed_out
                        else "Instrument resolution failed; the overview remains available."
                    )
                    self._emit(
                        "feed_status",
                        {
                            "scope": "focus",
                            "market": market,
                            "state": "ERROR",
                            "message": message,
                        },
                    )
                    return
        if resolved is None:
            return
        if generation != self.generation or self._stop_event.is_set():
            return

        contract = resolved.raw_symbol or resolved.market
        binding = HistoryBinding(
            market=market,
            contract=contract,
            instrument_id=resolved.instrument_id,
        )
        self._remember_binding(binding)
        cached: list[dict[str, Any]] = []
        if self.cache is not None:
            cached = self.cache.get_bars(
                dataset=DEFAULT_DATASET,
                instrument_id=resolved.instrument_id,
                start=lookback_start,
                end=end,
            )
            with self._metrics_lock:
                self._cache_reads += 1
        bars_by_time = {normalize_ts_event(bar["time"]): dict(bar) for bar in cached}
        overview_bar = self._overview_latest_bars.get(market)
        if overview_bar is not None and overview_bar[0] == resolved.instrument_id:
            overview_candle = overview_bar[1]
            overview_time = normalize_ts_event(overview_candle["time"])
            if overview_time <= now:
                bars_by_time[overview_time] = dict(overview_candle)
        with self._lock:
            replay_lower_bound = self._replay_watermarks.get(resolved.instrument_id)
        replay_start = (
            _recent_gap_replay_start(
                cached,
                now=now,
                lower_bound=replay_lower_bound,
            )
            if self.cache is not None
            else None
        )
        with self._lock:
            self._raw_bars = [bars_by_time[key] for key in sorted(bars_by_time)]
            self._contract = contract
            self._resolved_instrument_id = resolved.instrument_id
            timeframe = self.timeframe
        target_start, target_end = self._history_target(now=now)
        availability_known = self._history_available_end is not None
        history_missing = (
            self._missing_history(
                binding=binding,
                start=target_start,
                end=target_end,
            )
            if availability_known
            else [(target_start, target_end)]
        )
        history_state = (
            "COMPLETE"
            if availability_known and not history_missing
            else "PARTIAL"
            if availability_known and cached
            else "LOADING"
        )
        if history_state == "COMPLETE":
            with self._lock:
                self._history_complete_generation = generation
        visible_bars = [bars_by_time[key] for key in sorted(bars_by_time)]
        self._publish_focus_bundle(
            market=market,
            contract=contract,
            instrument_id=resolved.instrument_id,
            timeframe=timeframe,
            bars=visible_bars,
            source="cached" if visible_bars else "contract-resolved",
            generation=generation,
            history_state=history_state,
        )
        if generation != self.generation or self._stop_event.is_set():
            return
        bars = visible_bars

        self._emit(
            "feed_status",
            {
                "scope": "focus",
                "market": market,
                "state": "CONNECTING",
                "message": (
                    f"Connecting {contract} with recent gap recovery"
                    if replay_start is not None
                    else f"Connecting {contract} trade stream"
                ),
            },
        )
        seed = bars[-1] if bars else None
        current_bucket = None
        current_candle = None
        if seed is not None and normalize_ts_event(seed["time"]) == floor_timeframe(now, 60):
            current_bucket = normalize_ts_event(seed["time"])
            current_candle = dict(seed)
        aggregator = TradeCandleAggregator(
            timeframe_seconds=60,
            timeframe="1m",
            current_bucket=current_bucket,
            current_candle=current_candle,
        )

        def focus_callback(record: object) -> None:
            self._on_focus_record(record, generation=generation, aggregator=aggregator)

        try:
            client = self._new_live_client()
            if replay_start is not None:
                client.subscribe(
                    dataset=DEFAULT_DATASET,
                    schema=DEFAULT_HISTORICAL_SCHEMA,
                    symbols=resolved.instrument_id,
                    stype_in=DEFAULT_STYPE_IN,
                    start=replay_start,
                )
            client.subscribe(
                dataset=DEFAULT_DATASET,
                schema=DEFAULT_SCHEMA,
                symbols=resolved.instrument_id,
                stype_in=DEFAULT_STYPE_IN,
            )
            self._add_live_callback(client, focus_callback, scope="focus")
            if generation != self.generation or self._stop_event.is_set():
                self._stop_client(client)
                return
            previous_focus_client = self._focus_client
            self._focus_client = None
            self._focus_live_announced_generation = None
            self._stop_client(previous_focus_client)
            client.start()
            self._register_started_client(client, scope="focus")
            if generation != self.generation or self._stop_event.is_set():
                self._stop_client(client)
                return
            self._focus_client = client
            if replay_start is not None:
                with self._lock:
                    self._replay_watermarks[resolved.instrument_id] = end
                with self._metrics_lock:
                    self._replay_subscriptions += 1
                    self._last_replay_start = replay_start
        except Exception as exc:
            self._stop_client(locals().get("client"))
            self._emit(
                "feed_status",
                {
                    "scope": "focus",
                    "market": market,
                    "state": "HISTORICAL_ONLY" if bars else "ERROR",
                    "message": f"Live trade stream unavailable: {exc}",
                },
            )

    def _on_focus_record(
        self,
        record: object,
        *,
        generation: int,
        aggregator: TradeCandleAggregator,
    ) -> None:
        if generation != self.generation or self._stop_event.is_set():
            return
        if self._handle_provider_control(record, scope="focus"):
            return
        if all(
            hasattr(record, field)
            for field in ("ts_event", "open", "high", "low", "close", "volume")
        ):
            try:
                candle = ohlcv_record_to_candle(record)
                event_time = normalize_ts_event(_record_field(record, "ts_event"))
            except Exception:
                return
            with self._lock:
                if generation != self.generation or self._stop_event.is_set():
                    return
                _upsert_sorted_bar(self._raw_bars, candle)
                self._focus_last_live_event = (
                    generation,
                    self._contract,
                    event_time,
                )
                if self._pending_cache_generation != generation:
                    self._pending_cache = []
                    self._pending_cache_generation = generation
                self._pending_cache.append(dict(candle))
                self._pending_snapshot_generation = generation
            self._publish_current_focus_health(evaluated_at=event_time)
            return
        if hasattr(record, "price") and hasattr(record, "size"):
            try:
                candle = aggregator.apply_trade(record)
                event_time = normalize_ts_event(_record_field(record, "ts_event"))
            except Exception:
                return
            if candle is None:
                return
            with self._lock:
                candle_time = normalize_ts_event(candle["time"])
                if self._raw_bars:
                    last_time = normalize_ts_event(self._raw_bars[-1]["time"])
                    if candle_time < last_time:
                        return
                    if candle_time == last_time:
                        self._raw_bars[-1] = dict(candle)
                    else:
                        if self._pending_cache_generation != generation:
                            self._pending_cache = []
                            self._pending_cache_generation = generation
                        self._pending_cache.append(dict(self._raw_bars[-1]))
                        self._raw_bars.append(dict(candle))
                else:
                    self._raw_bars.append(dict(candle))
                self._pending_update = (generation, dict(candle))
                self._focus_last_live_event = (
                    generation,
                    self._contract,
                    event_time,
                )
            self._publish_current_focus_health(evaluated_at=event_time)
            if self._focus_live_announced_generation != generation:
                self._focus_live_announced_generation = generation
                self._emit(
                    "feed_status",
                    {
                        "scope": "focus",
                        "market": self.market,
                        "state": "LIVE",
                        "message": f"{self._contract} live",
                    },
                )
            return
    def _render_worker(self) -> None:
        deadline: float | None = None
        while True:
            stopped, deadline = _wait_for_visual_tick(
                self._stop_event, self._visual_updates, deadline
            )
            if stopped:
                return
            with self._overview_lock:
                overview_cache = self._overview_pending_cache
                self._overview_pending_cache = {}
            with self._lock:
                pending = self._pending_update
                self._pending_update = None
                snapshot_generation = self._pending_snapshot_generation
                self._pending_snapshot_generation = None
                cache_rows = self._pending_cache
                self._pending_cache = []
                cache_generation = self._pending_cache_generation
                self._pending_cache_generation = None
                timeframe = self.timeframe
                market = self.market
                contract = self._contract
                instrument_id = self._resolved_instrument_id
                generation = self.generation
                recent_count = max(4, timeframe_seconds(timeframe) // 60 + 2)
                recent = list(self._raw_bars[-recent_count:])
                snapshot_bars = (
                    list(self._raw_bars)
                    if snapshot_generation == generation
                    else []
                )
            if self.cache is not None:
                batches: list[
                    tuple[str, int, str, Sequence[Mapping[str, Any]]]
                ] = [
                    (DEFAULT_DATASET, overview_instrument_id, raw_symbol, rows)
                    for overview_instrument_id, (_market, raw_symbol, rows) in overview_cache.items()
                    if rows
                ]
                if (
                    cache_rows
                    and cache_generation == generation
                    and instrument_id is not None
                ):
                    batches.append(
                        (DEFAULT_DATASET, instrument_id, contract, cache_rows)
                    )
                try:
                    if batches:
                        self.cache.put_bar_batches(batches)
                        with self._metrics_lock:
                            self._cache_writes += 1
                except Exception:
                    pass
            if snapshot_generation == generation:
                self._publish_focus_bundle(
                    market=market,
                    contract=contract,
                    instrument_id=instrument_id,
                    timeframe=timeframe,
                    bars=snapshot_bars,
                    source="recent-replay",
                    generation=generation,
                )
                continue
            if pending is None or pending[0] != generation:
                continue
            aggregated = _aggregate(recent, timeframe)
            if not aggregated:
                continue
            self._emit(
                "bar_update",
                {
                    "market": market,
                    "timeframe": timeframe,
                    "bar": serialize_bar(aggregated[-1]),
                    "generation": generation,
                },
            )

    def _maintenance_worker(self) -> None:
        while not self._stop_event.wait(30.0):
            self._refresh_overview_staleness()
            self._publish_current_focus_health()
            current_day = trading_day_start(datetime.now(timezone.utc))
            if current_day == self._trading_day:
                continue
            self._trading_day = current_day
            with self._history_lock:
                self._market_bindings.clear()
                self._history_available_end = None
                if self._history_plan is not None:
                    self._history_plan.chunks.clear()
                self._history_plan = None
                self._history_reestimate_requested = True
                self._history_retry_active = True
            with self._lock:
                self._history_complete_generation = None
            self._emit(
                "feed_status",
                {
                    "scope": "all",
                    "state": "RECONNECTING",
                    "message": "Refreshing contracts for the new Globex session",
                },
            )
            self._stop_overview()
            self._start_overview()
            self.select_market(self.market, force=True)
            self._history_wakeup.set()

    def stop(self) -> None:
        self._stop_event.set()
        self._history_wakeup.set()
        self._stop_client(self._focus_client)
        self._focus_client = None
        self._stop_overview()
        for thread in self._threads:
            if thread is not threading.current_thread():
                thread.join(timeout=1.0)
        if self.cache is not None:
            self.cache.close()
