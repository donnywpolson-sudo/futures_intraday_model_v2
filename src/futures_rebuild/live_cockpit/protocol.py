"""Versioned messages exchanged between the Python engine and the web UI."""

from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Any, Mapping


PROTOCOL_VERSION = 1
EVENT_TYPES = frozenset(
    {
        "bootstrap",
        "chart_snapshot",
        "bar_update",
        "market_status",
        "feed_status",
        "data_health",
        "prediction_update",
        "history_cache_status",
    }
)
MARKET_STATES = frozenset({"LIVE", "WAITING", "STALE", "ERROR"})
FEED_STATES = frozenset(
    {
        "RESOLVING",
        "BACKFILLING",
        "CONNECTING",
        "LIVE",
        "RECONNECTING",
        "HISTORICAL_ONLY",
        "STALE",
        "ERROR",
        "STOPPED",
    }
)
PREDICTION_STATES = frozenset(
    {"OFFLINE", "WARMING_UP", "READY", "ABSTAIN", "STALE", "ERROR"}
)
PREDICTION_DIRECTIONS = frozenset({"LONG", "SHORT", "FLAT"})
PREDICTION_SOURCES = frozenset({"NONE", "SYNTHETIC_DEMO"})
PREDICTION_TIMEFRAME_SECONDS = {
    "1m": 60,
    "5m": 5 * 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1d": 24 * 60 * 60,
}
PREDICTION_REASON_CODES = frozenset(
    {
        "MODEL_NOT_AUTHORIZED",
        "FEATURE_WARMUP_INCOMPLETE",
        "DATA_INCOMPLETE",
        "DATA_STALE",
        "OUTSIDE_VALIDATED_SCOPE",
        "OUTSIDE_DEMO_SCENARIO",
        "MODEL_ABSTAINED",
        "SYNTHETIC_DEMO_ERROR",
    }
)
DATA_HEALTH_STATES = frozenset({"CURRENT", "DEGRADED", "STALE", "UNKNOWN"})
HISTORY_HEALTH_STATES = frozenset(
    {"LOADING", "COMPLETE", "PARTIAL", "UNAVAILABLE"}
)
CONTINUITY_STATES = frozenset({"PASS", "WARN", "NOT_EVALUATED"})
HISTORY_CACHE_STATES = frozenset(
    {
        "CHECKING",
        "CONFIRMATION_REQUIRED",
        "WARMING",
        "PAUSED",
        "COMPLETE",
        "PARTIAL",
        "ERROR",
    }
)
HISTORY_CACHE_FAILURE_CATEGORIES = frozenset(
    {
        "ESTIMATE_UNAVAILABLE",
        "SYMBOL_RESOLUTION",
        "AUTHORIZATION",
        "TIMEOUT",
        "CONNECTION",
        "DATA_AVAILABILITY",
        "UNAVAILABLE",
        "CACHE_UNAVAILABLE",
    }
)
DATA_HEALTH_REASON_CODES = frozenset(
    {
        "HISTORY_LOADING",
        "HISTORY_PARTIAL",
        "HISTORY_UNAVAILABLE",
        "DATA_STALE",
        "CONTINUITY_WARNING",
        "CONTINUITY_NOT_EVALUATED",
        "NO_BAR_DATA",
    }
)


def _finite_number(value: object, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be a finite number")
    return number


def _nonnegative_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def _identity_payload(payload: Mapping[str, Any]) -> None:
    for name in ("market", "contract", "timeframe"):
        value = payload.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a nonempty string")
    _nonnegative_int(payload.get("generation"), name="generation")
    instrument_id = payload.get("instrument_id")
    if instrument_id is not None:
        _nonnegative_int(instrument_id, name="instrument_id")


def _reason_codes(
    payload: Mapping[str, Any], *, allowed: frozenset[str], required: bool
) -> list[str]:
    values = payload.get("reason_codes")
    if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
        raise ValueError("reason_codes must be a list of safe reason identifiers")
    if required and not values:
        raise ValueError("at least one reason code is required")
    if any(value not in allowed for value in values):
        raise ValueError("unsupported reason code")
    return values


def direction_entropy(probabilities: Mapping[str, Any]) -> float:
    """Return normalized three-way entropy for direction probabilities."""

    values = [
        _finite_number(probabilities.get(name), name=f"probabilities.{name}")
        for name in ("long", "flat", "short")
    ]
    if any(value < 0.0 or value > 1.0 for value in values):
        raise ValueError("direction probabilities must be within [0, 1]")
    if not math.isclose(sum(values), 1.0, rel_tol=0.0, abs_tol=1e-6):
        raise ValueError("direction probabilities must sum to 1")
    entropy = -sum(value * math.log(value) for value in values if value > 0.0)
    return entropy / math.log(3.0)


def validate_prediction_payload(payload: Mapping[str, Any]) -> None:
    _identity_payload(payload)
    state = payload.get("state")
    if state not in PREDICTION_STATES:
        raise ValueError("unsupported prediction state")
    source = payload.get("source")
    if source not in PREDICTION_SOURCES:
        raise ValueError("unsupported prediction source")
    if payload.get("observation_only") is not True:
        raise ValueError("prediction events must be observation-only")
    synthetic = payload.get("synthetic")
    if not isinstance(synthetic, bool):
        raise ValueError("synthetic must be a boolean")
    if synthetic != (source == "SYNTHETIC_DEMO"):
        raise ValueError("prediction source and synthetic flag disagree")
    prediction_id = payload.get("prediction_id")
    if not isinstance(prediction_id, str) or not prediction_id or len(prediction_id) > 160:
        raise ValueError("prediction_id must be a bounded nonempty string")
    prediction_time = _nonnegative_int(
        payload.get("prediction_time"), name="prediction_time"
    )
    input_bar_time = payload.get("input_bar_time")
    if input_bar_time is not None:
        input_bar_time = _nonnegative_int(input_bar_time, name="input_bar_time")
        timeframe_seconds = PREDICTION_TIMEFRAME_SECONDS.get(str(payload["timeframe"]))
        if timeframe_seconds is None:
            raise ValueError("unsupported prediction timeframe")
        if input_bar_time + timeframe_seconds > prediction_time:
            raise ValueError("prediction input bar must be completed")
    model = payload.get("model")
    if model is not None:
        if not isinstance(model, Mapping):
            raise ValueError("model metadata must be a mapping")
        if set(model) != {"id", "version", "strategy"}:
            raise ValueError("model metadata has unsupported fields")
        for name in ("id", "version", "strategy"):
            value = model.get(name)
            if not isinstance(value, str) or not value or len(value) > 80:
                raise ValueError("model metadata must use bounded identifiers")

    forecast = payload.get("forecast")
    if state != "READY":
        if forecast is not None:
            raise ValueError("non-ready prediction states cannot carry a forecast")
        _reason_codes(payload, allowed=PREDICTION_REASON_CODES, required=True)
        return

    if input_bar_time is None:
        raise ValueError("ready predictions require a completed input bar")
    if not isinstance(forecast, Mapping):
        raise ValueError("ready predictions require a forecast mapping")
    if forecast.get("direction") not in PREDICTION_DIRECTIONS:
        raise ValueError("unsupported prediction direction")
    horizon_seconds = forecast.get("horizon_seconds")
    if (
        isinstance(horizon_seconds, bool)
        or not isinstance(horizon_seconds, int)
        or horizon_seconds <= 0
    ):
        raise ValueError("forecast horizon must be a positive integer")
    probabilities = forecast.get("probabilities")
    if not isinstance(probabilities, Mapping) or set(probabilities) != {
        "long",
        "flat",
        "short",
    }:
        raise ValueError("forecast probabilities must define long, flat, and short")
    entropy = direction_entropy(probabilities)
    _finite_number(forecast.get("expected_return"), name="expected_return")
    observed_entropy = _finite_number(
        forecast.get("direction_entropy"), name="direction_entropy"
    )
    if not 0.0 <= observed_entropy <= 1.0:
        raise ValueError("direction entropy must be within [0, 1]")
    if not math.isclose(observed_entropy, entropy, rel_tol=0.0, abs_tol=1e-6):
        raise ValueError("direction entropy does not match the probabilities")
    _reason_codes(payload, allowed=PREDICTION_REASON_CODES, required=False)


def validate_data_health_payload(payload: Mapping[str, Any]) -> None:
    _identity_payload(payload)
    if payload.get("state") not in DATA_HEALTH_STATES:
        raise ValueError("unsupported data-health state")
    evaluated_at = _nonnegative_int(payload.get("evaluated_at"), name="evaluated_at")
    last_bar_time = payload.get("last_bar_time")
    if last_bar_time is not None:
        last_bar_time = _nonnegative_int(last_bar_time, name="last_bar_time")
        if last_bar_time > evaluated_at:
            raise ValueError("last bar cannot be later than data-health evaluation")

    history = payload.get("history")
    if not isinstance(history, Mapping):
        raise ValueError("data health requires history details")
    if history.get("state") not in HISTORY_HEALTH_STATES:
        raise ValueError("unsupported history-health state")
    requested_hours = _finite_number(
        history.get("requested_hours"), name="history.requested_hours"
    )
    coverage_hours = _finite_number(
        history.get("coverage_hours"), name="history.coverage_hours"
    )
    if requested_hours <= 0.0 or coverage_hours < 0.0:
        raise ValueError("history hours must be nonnegative with a positive request")
    _nonnegative_int(history.get("bar_count"), name="history.bar_count")

    continuity = payload.get("continuity")
    if not isinstance(continuity, Mapping):
        raise ValueError("data health requires continuity details")
    continuity_state = continuity.get("state")
    if continuity_state not in CONTINUITY_STATES:
        raise ValueError("unsupported continuity state")
    gap_count = continuity.get("unexpected_gap_count")
    largest_gap = continuity.get("largest_gap_seconds")
    if continuity_state == "NOT_EVALUATED":
        if gap_count is not None or largest_gap is not None:
            raise ValueError("unevaluated continuity cannot claim numeric gaps")
    else:
        _nonnegative_int(gap_count, name="unexpected_gap_count")
        _nonnegative_int(largest_gap, name="largest_gap_seconds")
    _reason_codes(payload, allowed=DATA_HEALTH_REASON_CODES, required=False)


def validate_history_cache_payload(payload: Mapping[str, Any]) -> None:
    state = payload.get("state")
    if state not in HISTORY_CACHE_STATES:
        raise ValueError("unsupported history-cache state")
    ready_markets = _nonnegative_int(payload.get("ready_markets"), name="ready_markets")
    total_markets = _nonnegative_int(payload.get("total_markets"), name="total_markets")
    queued_markets = _nonnegative_int(payload.get("queued_markets"), name="queued_markets")
    if total_markets <= 0 or ready_markets > total_markets or queued_markets > total_markets:
        raise ValueError("invalid history-cache market counts")
    if not isinstance(payload.get("paused"), bool):
        raise ValueError("history-cache paused must be a boolean")
    message = payload.get("message")
    if not isinstance(message, str) or not message or len(message) > 240:
        raise ValueError("history-cache message must be bounded")
    active_market = payload.get("active_market")
    if active_market is not None and (
        not isinstance(active_market, str) or not active_market or len(active_market) > 12
    ):
        raise ValueError("invalid active history-cache market")
    plan_id = payload.get("plan_id")
    if plan_id is not None and (
        not isinstance(plan_id, str) or not plan_id or len(plan_id) > 80
    ):
        raise ValueError("invalid history-cache plan id")
    estimated_cost = payload.get("estimated_cost_usd")
    if estimated_cost is not None and _finite_number(
        estimated_cost, name="estimated_cost_usd"
    ) < 0.0:
        raise ValueError("history-cache estimate cannot be negative")
    expires_at = payload.get("estimate_expires_at")
    if expires_at is not None:
        _nonnegative_int(expires_at, name="estimate_expires_at")
    if state == "CONFIRMATION_REQUIRED" and (
        plan_id is None or estimated_cost is None or expires_at is None
    ):
        raise ValueError("confirmation-required cache status needs an estimate plan")
    failure_category = payload.get("failure_category")
    if state == "ERROR":
        if failure_category not in HISTORY_CACHE_FAILURE_CATEGORIES:
            raise ValueError("unsupported history-cache failure category")
    elif failure_category is not None:
        raise ValueError("non-error history-cache status cannot carry a failure category")
    diagnostic = payload.get("diagnostic")
    if diagnostic is not None:
        fields = {
            "phase",
            "chunk_number",
            "requested_start",
            "requested_end",
            "download_began",
        }
        if not isinstance(diagnostic, Mapping) or set(diagnostic) != fields:
            raise ValueError("history-cache diagnostic fields are not exact")
        phase = diagnostic.get("phase")
        if not isinstance(phase, str) or not phase or len(phase) > 32:
            raise ValueError("invalid history-cache diagnostic phase")
        chunk_number = diagnostic.get("chunk_number")
        if chunk_number is not None and _nonnegative_int(
            chunk_number, name="diagnostic.chunk_number"
        ) < 1:
            raise ValueError("history-cache diagnostic chunk number must be one-based")
        for field in ("requested_start", "requested_end"):
            value = diagnostic.get(field)
            if value is not None:
                _nonnegative_int(value, name=f"diagnostic.{field}")
        if not isinstance(diagnostic.get("download_began"), bool):
            raise ValueError("history-cache diagnostic download_began must be a boolean")


def event(event_type: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return a JSON-serializable cockpit event."""

    if event_type not in EVENT_TYPES:
        raise ValueError(f"unsupported cockpit event type: {event_type!r}")
    if event_type == "prediction_update":
        validate_prediction_payload(payload)
    elif event_type == "data_health":
        validate_data_health_payload(payload)
    elif event_type == "history_cache_status":
        validate_history_cache_payload(payload)
    return {
        "v": PROTOCOL_VERSION,
        "type": event_type,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "payload": dict(payload),
    }


def validate_event(message: Mapping[str, Any]) -> None:
    if message.get("v") != PROTOCOL_VERSION:
        raise ValueError("unsupported cockpit protocol version")
    if message.get("type") not in EVENT_TYPES:
        raise ValueError("unsupported cockpit event type")
    if not isinstance(message.get("payload"), Mapping):
        raise ValueError("cockpit event payload must be a mapping")
    payload = message["payload"]
    if message.get("type") == "prediction_update":
        validate_prediction_payload(payload)
    elif message.get("type") == "data_health":
        validate_data_health_payload(payload)
    elif message.get("type") == "history_cache_status":
        validate_history_cache_payload(payload)


def timestamp_seconds(value: datetime | str | int | float) -> int:
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return int(normalized.timestamp())
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        normalized = parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
        return int(normalized.timestamp())
    numeric = float(value)
    if abs(numeric) > 10_000_000_000:
        numeric /= 1_000_000_000
    return int(numeric)


def serialize_bar(bar: Mapping[str, Any]) -> dict[str, int | float]:
    return {
        "time": timestamp_seconds(bar["time"]),
        "open": float(bar["open"]),
        "high": float(bar["high"]),
        "low": float(bar["low"]),
        "close": float(bar["close"]),
        "volume": int(bar["volume"]),
    }
