"""Versioned messages exchanged between the Python engine and the web UI."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from typing import Any, Mapping


PROTOCOL_VERSION = 3
# One-week minute snapshots are intentionally large; execution/control payloads
# remain far smaller and entity collections have their own strict cap.
MAX_MESSAGE_BYTES = 4_194_304
MAX_EXECUTION_ENTITIES = 500
SECRET_FIELD_SUFFIXES = frozenset(
    {"token", "password", "secret", "authorization", "apikey"}
)
MANUAL_FORBIDDEN_FIELD_SUFFIXES = SECRET_FIELD_SUFFIXES | frozenset(
    {"accountid", "credential", "privatekey", "clientid", "filepath", "directorypath"}
)
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
        "execution_capability",
        "execution_readiness",
        "account_snapshot",
        "position_snapshot",
        "order_snapshot",
        "fill_event",
        "order_intent_preview",
        "order_submission_result",
        "reconciliation_status",
        "execution_health",
        "arm_state",
        "compliance_decision",
        "manual_capability",
        "manual_readiness",
        "operator_account_snapshot",
        "manual_ticket_preview",
        "manual_ticket_state",
        "operator_fill_report",
        "operator_protection_report",
        "operator_exit_report",
        "planned_actual_comparison",
        "manual_reconciliation_status",
    }
)
COMMAND_TYPES = frozenset(
    {
        "PREVIEW_ORDER_INTENT",
        "ARM_EXECUTION",
        "DISARM_EXECUTION",
        "SUBMIT_ORDER_INTENT",
        "CANCEL_ORDER",
        "CANCEL_ALL_ENTRIES",
        "FLATTEN_POSITION",
        "FLATTEN_ALL",
        "RECORD_OPERATOR_ACCOUNT_SNAPSHOT",
        "PREPARE_MANUAL_TICKET",
        "TRANSITION_MANUAL_TICKET",
        "COMPARE_MANUAL_TICKET",
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
MANUAL_EVENT_FIELDS = {
    "manual_capability": {"provider_id", "profile_id", "account_stage", "capability", "authority", "origin", "synthetic", "observed_at", "provider_api_readiness", "automatic_execution_authorized", "account_binding"},
    "manual_readiness": {"provider_id", "profile_id", "account_stage", "capability", "authority", "origin", "synthetic", "observed_at", "manual_ticket_preview_available", "manual_assistant_readiness", "blockers"},
    "operator_account_snapshot": {"provider_id", "profile_id", "account_stage", "capability", "authority", "origin", "synthetic", "observed_at", "snapshot"},
    "manual_ticket_preview": {"provider_id", "profile_id", "account_stage", "capability", "authority", "origin", "synthetic", "observed_at", "ticket"},
    "manual_ticket_state": {"provider_id", "profile_id", "account_stage", "capability", "authority", "origin", "synthetic", "observed_at", "ticket_id", "state"},
    "operator_fill_report": {"provider_id", "profile_id", "account_stage", "capability", "authority", "origin", "synthetic", "observed_at", "ticket_id", "report"},
    "operator_protection_report": {"provider_id", "profile_id", "account_stage", "capability", "authority", "origin", "synthetic", "observed_at", "ticket_id", "report"},
    "operator_exit_report": {"provider_id", "profile_id", "account_stage", "capability", "authority", "origin", "synthetic", "observed_at", "ticket_id", "report"},
    "planned_actual_comparison": {"provider_id", "profile_id", "account_stage", "capability", "authority", "origin", "synthetic", "observed_at", "ticket_id", "comparison"},
    "manual_reconciliation_status": {"provider_id", "profile_id", "account_stage", "capability", "authority", "origin", "synthetic", "observed_at", "status", "blockers"},
}


def _secret_field_name(value: object) -> bool:
    normalized = "".join(character for character in str(value).lower() if character.isalnum())
    return any(normalized.endswith(marker) for marker in SECRET_FIELD_SUFFIXES)


def _contains_secret_field(value: object) -> bool:
    if isinstance(value, Mapping):
        return any(
            _secret_field_name(key) or _contains_secret_field(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_secret_field(item) for item in value)
    return False


def _contains_manual_forbidden(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = "".join(character for character in str(key).lower() if character.isalnum())
            if any(normalized.endswith(marker) for marker in MANUAL_FORBIDDEN_FIELD_SUFFIXES) or normalized.endswith("path"):
                return True
            if _contains_manual_forbidden(item):
                return True
        return False
    if isinstance(value, (list, tuple)):
        return any(_contains_manual_forbidden(item) for item in value)
    if isinstance(value, str):
        normalized = value.replace("\\", "/").lower()
        return bool(
            (len(value) >= 3 and value[1:3] in {":\\", ":/"})
            or value.startswith("\\\\")
            or "/users/" in normalized
            or normalized.startswith("/home/")
        )
    return False


def _bounded_message(value: Mapping[str, Any]) -> None:
    if _contains_secret_field(value):
        raise ValueError("cockpit payload contains a forbidden secret field")
    try:
        size = len(json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
    except (TypeError, ValueError) as exc:
        raise ValueError("cockpit payload is not JSON serializable") from exc
    if size > MAX_MESSAGE_BYTES:
        raise ValueError("cockpit payload is oversized")


def _exact_fields(payload: Mapping[str, Any], fields: set[str], *, name: str) -> None:
    if set(payload) != fields:
        raise ValueError(f"{name} fields are not exact")


def _bounded_string(value: object, *, name: str, maximum: int = 160, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"{name} must be a bounded string")
    return value


def _aware_timestamp(value: object, *, name: str) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError(f"{name} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return parsed


def validate_execution_capability_payload(payload: Mapping[str, Any]) -> None:
    _exact_fields(payload, {
        "mode", "origin", "simulated", "provider_id", "platform_id", "profile_id",
        "account_stage", "connection_id", "connection_hash", "entitlement_status",
        "account_binding_present", "account_binding_id", "cost_profile_id",
        "exact_costs_verified", "production_readiness", "execution_authorized",
        "order_paths_reachable", "provider_connection_opened", "armed", "arm_expires_at",
        "blockers", "verified_micro_mappings", "disabled_signal_roots",
        "execution_capability", "direct_api_read_access", "direct_api_order_access",
        "manual_ticket_preview_available", "manual_assistant_readiness",
        "provider_api_readiness", "automatic_execution_authorized",
        "operator_reported_state",
    }, name="execution capability")
    modes = {
        "OBSERVATION_ONLY", "MFF_MANUAL_ASSISTANT", "TRADOVATE_READ_ONLY", "LOCAL_EXECUTION_SIMULATOR",
        "MFF_TRADOVATE_SIM_FUNDED", "MFF_TRADOVATE_LIVE",
    }
    if payload.get("mode") not in modes:
        raise ValueError("execution mode is invalid")
    if payload.get("origin") not in {"LOCAL_CONFIGURATION", "LOCAL_SIMULATOR", "PROVIDER_BACKED"}:
        raise ValueError("execution origin is invalid")
    for name in ("simulated", "account_binding_present", "exact_costs_verified", "production_readiness", "execution_authorized", "order_paths_reachable", "provider_connection_opened", "armed", "direct_api_read_access", "direct_api_order_access", "manual_ticket_preview_available", "manual_assistant_readiness", "provider_api_readiness", "automatic_execution_authorized", "operator_reported_state"):
        if not isinstance(payload.get(name), bool):
            raise ValueError(f"{name} must be a boolean")
    for name in ("provider_id", "platform_id", "profile_id", "account_stage", "connection_id", "cost_profile_id", "entitlement_status"):
        _bounded_string(payload.get(name), name=name)
    connection_hash = payload.get("connection_hash")
    if not isinstance(connection_hash, str) or len(connection_hash) != 64:
        raise ValueError("connection_hash is invalid")
    if payload.get("account_binding_id") is not None:
        _bounded_string(payload.get("account_binding_id"), name="account_binding_id")
    if payload.get("arm_expires_at") is not None:
        _aware_timestamp(payload.get("arm_expires_at"), name="arm_expires_at")
    for name in ("blockers", "verified_micro_mappings", "disabled_signal_roots"):
        values = payload.get(name)
        if not isinstance(values, list) or len(values) > 64 or any(not isinstance(item, str) or not item or len(item) > 160 for item in values):
            raise ValueError(f"{name} must be a bounded string list")
    if payload.get("production_readiness") is False and payload.get("order_paths_reachable") is not False:
        raise ValueError("order paths cannot be reachable while production readiness is false")
    if payload.get("execution_capability") not in {"MANUAL_ONLY", "READ_ONLY_API", "ORDER_API", "UNCONFIRMED"}:
        raise ValueError("execution capability is invalid")
    if payload.get("execution_capability") == "MANUAL_ONLY" and any(
        payload.get(name) is not False
        for name in ("direct_api_read_access", "direct_api_order_access", "provider_api_readiness", "automatic_execution_authorized", "order_paths_reachable")
    ):
        raise ValueError("manual-only capability cannot expose provider methods")


def validate_execution_entity_payload(payload: Mapping[str, Any], *, event_type: str) -> None:
    required = {"provider_id", "account_id", "profile_id", "account_stage", "origin", "simulated", "observed_at", "entities"}
    _exact_fields(payload, required, name=event_type)
    for name in ("provider_id", "profile_id", "account_stage", "origin"):
        _bounded_string(payload.get(name), name=name)
    if not isinstance(payload.get("account_id"), int) or payload["account_id"] <= 0:
        raise ValueError("account_id must be positive")
    if not isinstance(payload.get("simulated"), bool):
        raise ValueError("simulated must be a boolean")
    _aware_timestamp(payload.get("observed_at"), name="observed_at")
    entities = payload.get("entities")
    if not isinstance(entities, list) or len(entities) > MAX_EXECUTION_ENTITIES or any(not isinstance(item, Mapping) for item in entities):
        raise ValueError("execution entities are invalid")


def validate_execution_status_payload(payload: Mapping[str, Any], *, event_type: str) -> None:
    common = {"provider_id", "account_id", "profile_id", "account_stage", "origin", "simulated", "observed_at"}
    specific = {
        "order_intent_preview": {"intent_id", "allowed", "authoritative_quantity", "blockers"},
        "order_submission_result": {"intent_id", "accepted", "provider_order_id", "status", "reason_codes"},
        "reconciliation_status": {"status", "blockers", "orphan_order_ids", "sequence"},
        "execution_health": {"state", "connected", "last_sync_at", "blockers"},
        "arm_state": {"armed", "expires_at", "reason", "binding_id", "mode"},
        "compliance_decision": {"decision_id", "intent_id", "allowed", "actions", "reasons"},
    }[event_type]
    _exact_fields(payload, common | specific, name=event_type)
    for name in ("provider_id", "profile_id", "account_stage", "origin"):
        _bounded_string(payload.get(name), name=name)
    account_id = payload.get("account_id")
    if account_id is not None and (not isinstance(account_id, int) or account_id <= 0):
        raise ValueError("account_id must be null or positive")
    if not isinstance(payload.get("simulated"), bool):
        raise ValueError("simulated must be a boolean")
    _aware_timestamp(payload.get("observed_at"), name="observed_at")
    for name in ("allowed", "accepted", "connected", "armed"):
        if name in payload and not isinstance(payload.get(name), bool):
            raise ValueError(f"{name} must be a boolean")
    for name in ("blockers", "reason_codes", "actions", "reasons"):
        if name in payload:
            values = payload.get(name)
            if not isinstance(values, list) or len(values) > 64 or any(not isinstance(item, str) or not item or len(item) > 160 for item in values):
                raise ValueError(f"{name} must be a bounded string list")
    for name in ("intent_id", "status", "state", "reason", "binding_id", "mode", "decision_id"):
        if name in payload and payload.get(name) is not None:
            _bounded_string(payload.get(name), name=name)
    for name in ("last_sync_at", "expires_at"):
        if name in payload and payload.get(name) is not None:
            _aware_timestamp(payload.get(name), name=name)
    for name in ("authoritative_quantity", "sequence"):
        if name in payload:
            _nonnegative_int(payload.get(name), name=name)
    provider_order_id = payload.get("provider_order_id")
    if "provider_order_id" in payload and provider_order_id is not None and (not isinstance(provider_order_id, int) or provider_order_id <= 0):
        raise ValueError("provider_order_id must be null or positive")
    orphan_ids = payload.get("orphan_order_ids")
    if orphan_ids is not None and (not isinstance(orphan_ids, list) or len(orphan_ids) > MAX_EXECUTION_ENTITIES or any(not isinstance(item, int) or item <= 0 for item in orphan_ids)):
        raise ValueError("orphan_order_ids are invalid")


def validate_manual_event_payload(payload: Mapping[str, Any], *, event_type: str) -> None:
    if _contains_manual_forbidden(payload):
        raise ValueError("manual event contains a forbidden account, secret, or private path field")
    _exact_fields(payload, MANUAL_EVENT_FIELDS[event_type], name=event_type)
    for name in ("provider_id", "profile_id", "account_stage", "capability", "authority", "origin"):
        _bounded_string(payload.get(name), name=name)
    if payload.get("capability") != "MANUAL_ONLY":
        raise ValueError("manual event capability must be MANUAL_ONLY")
    if payload.get("authority") not in {"MODEL_CALCULATED", "MFF_RULE_VALIDATED", "OPERATOR_REPORTED", "OPERATOR_CONFIRMED"}:
        raise ValueError("manual event authority is invalid")
    if payload.get("origin") not in {"LOCAL_CONFIGURATION", "LOCAL_SIMULATOR", "OPERATOR_REPORTED"}:
        raise ValueError("manual event origin is invalid")
    if not isinstance(payload.get("synthetic"), bool):
        raise ValueError("manual event synthetic must be boolean")
    _aware_timestamp(payload.get("observed_at"), name="observed_at")
    for name in ("provider_api_readiness", "automatic_execution_authorized", "manual_ticket_preview_available", "manual_assistant_readiness"):
        if name in payload and not isinstance(payload.get(name), bool):
            raise ValueError(f"{name} must be boolean")
    for name in ("blockers",):
        if name in payload:
            values = payload.get(name)
            if not isinstance(values, list) or len(values) > 64 or any(not isinstance(item, str) or not item or len(item) > 160 for item in values):
                raise ValueError("manual blockers must be bounded")
    for name in ("snapshot", "ticket", "report", "comparison"):
        if name in payload and not isinstance(payload.get(name), Mapping):
            raise ValueError(f"{name} must be a mapping")
    for name in ("ticket_id", "state", "status", "account_binding"):
        if name in payload:
            _bounded_string(payload.get(name), name=name)


def validate_command(command: Mapping[str, Any]) -> None:
    _bounded_message(command)
    _exact_fields(command, {"v", "type", "payload"}, name="cockpit command")
    if command.get("v") != PROTOCOL_VERSION or command.get("type") not in COMMAND_TYPES or not isinstance(command.get("payload"), Mapping):
        raise ValueError("cockpit command envelope is invalid")
    payload = command["payload"]
    command_type = command["type"]
    if str(command_type).endswith("MANUAL_TICKET") or command_type == "RECORD_OPERATOR_ACCOUNT_SNAPSHOT":
        if _contains_manual_forbidden(payload):
            raise ValueError("manual cockpit payload contains a forbidden account, secret, or private path field")
    fields = {
        "PREVIEW_ORDER_INTENT": {"execution_symbol", "side", "order_type", "price", "stop_price", "target_price", "quantity"},
        "ARM_EXECUTION": {"binding_id", "confirmation", "duration_seconds"},
        "DISARM_EXECUTION": {"reason"},
        "SUBMIT_ORDER_INTENT": {"intent_id"},
        "CANCEL_ORDER": {"provider_order_id"},
        "CANCEL_ALL_ENTRIES": set(),
        "FLATTEN_POSITION": {"contract_id"},
        "FLATTEN_ALL": set(),
        "RECORD_OPERATOR_ACCOUNT_SNAPSHOT": {
            "profile_id", "stage", "account_alias", "nominal_plan_size_usd",
            "realized_balance_usd", "active_eod_floor_usd", "floor_lock_status",
            "current_session_realized_pnl_usd", "open_positions",
            "working_entry_orders", "protective_orders", "payout_state",
            "reconciliation_notes", "confirmation",
        },
        "PREPARE_MANUAL_TICKET": {
            "profile_id", "stage", "account_alias", "signal_instrument",
            "execution_contract", "side", "order_type", "entry_price",
            "stop_price", "target_price", "quantity", "strategy_candidate_id",
        },
        "TRANSITION_MANUAL_TICKET": {"ticket_id", "target", "report"},
        "COMPARE_MANUAL_TICKET": {"ticket_id", "report"},
    }[command_type]
    _exact_fields(payload, fields, name=str(command_type))
    if command_type == "PREVIEW_ORDER_INTENT":
        if payload.get("execution_symbol") not in {"MES", "MCL", "M6E"}:
            raise ValueError("execution symbol is not a verified micro")
        if payload.get("side") not in {"BUY", "SELL"} or payload.get("order_type") not in {"MARKET", "LIMIT", "STOP", "STOP_LIMIT"}:
            raise ValueError("order side or type is invalid")
        _nonnegative_int(payload.get("quantity"), name="quantity")
        if payload.get("quantity") == 0:
            raise ValueError("quantity must be positive")
        for name in ("price", "stop_price", "target_price"):
            if payload.get(name) is not None:
                _finite_number(payload.get(name), name=name)
        if payload.get("stop_price") is None:
            raise ValueError("protective stop is mandatory")
    elif command_type == "ARM_EXECUTION":
        _bounded_string(payload.get("binding_id"), name="binding_id")
        _bounded_string(payload.get("confirmation"), name="confirmation", maximum=240)
        duration = payload.get("duration_seconds")
        if not isinstance(duration, int) or not 30 <= duration <= 900:
            raise ValueError("arm duration is invalid")
    elif command_type in {"SUBMIT_ORDER_INTENT"}:
        _bounded_string(payload.get("intent_id"), name="intent_id")
    elif command_type in {"CANCEL_ORDER", "FLATTEN_POSITION"}:
        field = "provider_order_id" if command_type == "CANCEL_ORDER" else "contract_id"
        if not isinstance(payload.get(field), int) or payload[field] <= 0:
            raise ValueError(f"{field} must be positive")
    elif command_type == "DISARM_EXECUTION":
        _bounded_string(payload.get("reason"), name="reason", maximum=120)
    elif command_type == "RECORD_OPERATOR_ACCOUNT_SNAPSHOT":
        for name in ("profile_id", "stage", "account_alias", "floor_lock_status", "payout_state", "confirmation"):
            _bounded_string(payload.get(name), name=name, maximum=160)
        _bounded_string(str(payload.get("reconciliation_notes", "")) or "NONE", name="reconciliation_notes", maximum=500)
        for name in ("nominal_plan_size_usd", "realized_balance_usd", "active_eod_floor_usd", "current_session_realized_pnl_usd"):
            _finite_number(payload.get(name), name=name)
        for name in ("open_positions", "working_entry_orders", "protective_orders"):
            values = payload.get(name)
            if not isinstance(values, list) or len(values) > 100 or any(not isinstance(item, Mapping) for item in values):
                raise ValueError(f"{name} must be a bounded mapping list")
        snapshot_schemas = {
            "open_positions": {"signal_root", "execution_symbol", "execution_contract", "side", "quantity", "stop_ticks", "protection_status"},
            "working_entry_orders": {"signal_root", "execution_symbol", "execution_contract", "side", "quantity", "requested_quantity", "stop_ticks", "fill_status", "order_type", "entry_price"},
            "protective_orders": {"signal_root", "execution_symbol", "execution_contract", "side", "quantity", "stop_price", "status"},
        }
        for name, fields in snapshot_schemas.items():
            for item in payload[name]:
                _exact_fields(item, fields, name=f"{name} item")
                for field in fields & {"signal_root", "execution_symbol", "execution_contract", "side", "protection_status", "fill_status", "order_type", "status"}:
                    _bounded_string(item.get(field), name=f"{name}.{field}", maximum=40)
                for field in fields & {"quantity", "requested_quantity"}:
                    _nonnegative_int(item.get(field), name=f"{name}.{field}")
                for field in fields & {"stop_ticks", "entry_price", "stop_price"}:
                    if _finite_number(item.get(field), name=f"{name}.{field}") <= 0:
                        raise ValueError(f"{name}.{field} must be positive")
    elif command_type == "PREPARE_MANUAL_TICKET":
        for name in ("profile_id", "stage", "account_alias", "signal_instrument", "execution_contract", "side", "order_type", "strategy_candidate_id"):
            _bounded_string(payload.get(name), name=name, maximum=160)
        for name in ("entry_price", "stop_price", "target_price"):
            _finite_number(payload.get(name), name=name)
        quantity = _finite_number(payload.get("quantity"), name="quantity")
        if quantity <= 0:
            raise ValueError("quantity must be positive")
    elif command_type == "TRANSITION_MANUAL_TICKET":
        _bounded_string(payload.get("ticket_id"), name="ticket_id")
        target = _bounded_string(payload.get("target"), name="target")
        if not isinstance(payload.get("report"), Mapping):
            raise ValueError("operator report must be a mapping")
        _validate_manual_transition_report(str(target), payload["report"])
    elif command_type == "COMPARE_MANUAL_TICKET":
        _bounded_string(payload.get("ticket_id"), name="ticket_id")
        _validate_manual_comparison_report(payload.get("report"))


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


def _positive_int(value: object, *, name: str) -> int:
    number = _nonnegative_int(value, name=name)
    if number == 0:
        raise ValueError(f"{name} must be positive")
    return number


def _validate_manual_transition_report(target: str, report: Mapping[str, Any]) -> None:
    schemas = {
        "OPERATOR_REPORTED_SUBMITTED": {"actual_submission_time"},
        "OPERATOR_REPORTED_PARTIALLY_FILLED": {"partial_fills"},
        "OPERATOR_REPORTED_FILLED": {
            "actual_contract", "actual_side", "actual_quantity", "actual_fill_price",
            "actual_stop", "actual_target", "actual_fill_time", "actual_fees",
        },
        "OPERATOR_CONFIRMED_PROTECTED": {"actual_stop", "confirmed_at"},
        "OPERATOR_REPORTED_REJECTED": {"actual_rejection_reason"},
        "OPERATOR_REPORTED_CANCELLED": {"cancelled_at"},
        "OPERATOR_REPORTED_CLOSED": {"actual_exit_price", "actual_exit_time", "actual_fees"},
        "OPERATOR_RECONCILED": {"reconciliation_notes"},
        "STATE_UNCERTAIN": {"operator_notes"},
        "ABANDONED": {"operator_notes"},
    }
    if target not in schemas:
        raise ValueError("manual transition target is invalid")
    _exact_fields(report, schemas[target], name=f"{target} report")
    for name in ("actual_submission_time", "actual_fill_time", "confirmed_at", "cancelled_at", "actual_exit_time"):
        if name in report:
            _aware_timestamp(report[name], name=name)
    for name in ("actual_contract", "actual_side", "actual_rejection_reason", "reconciliation_notes", "operator_notes"):
        if name in report:
            _bounded_string(report[name], name=name, maximum=500 if name.endswith("notes") or name.endswith("reason") else 40)
    if "actual_side" in report and report["actual_side"] not in {"BUY", "SELL"}:
        raise ValueError("actual_side is invalid")
    if "actual_quantity" in report:
        _positive_int(report["actual_quantity"], name="actual_quantity")
    for name in ("actual_fill_price", "actual_stop", "actual_target", "actual_exit_price"):
        if name in report and _finite_number(report[name], name=name) <= 0:
            raise ValueError(f"{name} must be positive")
    if "actual_fees" in report and _finite_number(report["actual_fees"], name="actual_fees") < 0:
        raise ValueError("actual_fees must be nonnegative")
    if "partial_fills" in report:
        fills = report["partial_fills"]
        if not isinstance(fills, list) or not 1 <= len(fills) <= 100:
            raise ValueError("partial_fills must be a bounded nonempty list")
        for fill in fills:
            if not isinstance(fill, Mapping):
                raise ValueError("partial fill must be a mapping")
            _exact_fields(fill, {"quantity", "price", "time"}, name="partial fill")
            _positive_int(fill.get("quantity"), name="partial fill quantity")
            if _finite_number(fill.get("price"), name="partial fill price") <= 0:
                raise ValueError("partial fill price must be positive")
            _aware_timestamp(fill.get("time"), name="partial fill time")


def _validate_manual_comparison_report(value: object) -> None:
    if not isinstance(value, Mapping):
        raise ValueError("planned-versus-actual report must be a mapping")
    _exact_fields(
        value,
        {"actual_contract", "actual_side", "actual_quantity", "actual_fill_price", "actual_stop", "actual_target", "actual_fees"},
        name="planned-versus-actual report",
    )
    _bounded_string(value.get("actual_contract"), name="actual_contract", maximum=32)
    if value.get("actual_side") not in {"BUY", "SELL"}:
        raise ValueError("actual_side is invalid")
    _positive_int(value.get("actual_quantity"), name="actual_quantity")
    for name in ("actual_fill_price", "actual_stop", "actual_target"):
        if _finite_number(value.get(name), name=name) <= 0:
            raise ValueError(f"{name} must be positive")
    if _finite_number(value.get("actual_fees"), name="actual_fees") < 0:
        raise ValueError("actual_fees must be nonnegative")


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
    affected_markets = payload.get("affected_markets")
    if affected_markets is not None and (
        not isinstance(affected_markets, list)
        or len(affected_markets) > total_markets
        or any(
            not isinstance(market, str) or not market or len(market) > 12
            for market in affected_markets
        )
        or len(set(affected_markets)) != len(affected_markets)
    ):
        raise ValueError("invalid affected history-cache markets")
    missing_start = payload.get("missing_start")
    missing_end = payload.get("missing_end")
    if (missing_start is None) != (missing_end is None):
        raise ValueError("history-cache missing interval must be complete")
    if missing_start is not None and (
        _nonnegative_int(missing_start, name="missing_start")
        >= _nonnegative_int(missing_end, name="missing_end")
    ):
        raise ValueError("history-cache missing interval must increase")
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
    plan_fingerprint = payload.get("plan_fingerprint")
    if plan_fingerprint is not None and (
        not isinstance(plan_fingerprint, str)
        or len(plan_fingerprint) != 64
        or any(character not in "0123456789abcdef" for character in plan_fingerprint)
    ):
        raise ValueError("invalid history-cache plan fingerprint")
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
    policy_mode = payload.get("policy_mode")
    if policy_mode is not None and policy_mode not in {"UNDECIDED", "MANUAL", "AUTO"}:
        raise ValueError("unsupported history-update policy mode")
    for field in ("automatic_eligible", "automatic_blocked"):
        if field in payload and not isinstance(payload.get(field), bool):
            raise ValueError(f"history-cache {field} must be a boolean")
    automatic_reason = payload.get("automatic_reason")
    if automatic_reason is not None and (
        not isinstance(automatic_reason, str)
        or not automatic_reason
        or len(automatic_reason) > 64
    ):
        raise ValueError("invalid automatic-history reason")
    update_origin = payload.get("update_origin")
    if update_origin is not None and update_origin not in {"AUTO", "MANUAL"}:
        raise ValueError("invalid history-update origin")
    automatic_limit = payload.get("automatic_limit_usd")
    if automatic_limit is not None and _finite_number(
        automatic_limit, name="automatic_limit_usd"
    ) < 0.0:
        raise ValueError("automatic-history limit cannot be negative")
    automatic_interval = payload.get("automatic_interval_hours")
    if automatic_interval is not None and _nonnegative_int(
        automatic_interval, name="automatic_interval_hours"
    ) <= 0:
        raise ValueError("automatic-history interval must be positive")
    last_attempt = payload.get("last_auto_attempt_at")
    if last_attempt is not None:
        _nonnegative_int(last_attempt, name="last_auto_attempt_at")
    last_estimate = payload.get("last_auto_estimate_usd")
    if last_estimate is not None and _finite_number(
        last_estimate, name="last_auto_estimate_usd"
    ) < 0.0:
        raise ValueError("last automatic estimate cannot be negative")
    last_outcome = payload.get("last_auto_outcome")
    if last_outcome is not None and last_outcome not in {
        "STARTED",
        "COMPLETE",
        "ERROR",
        "PARTIAL",
        "REJECTED",
        "INTERRUPTED",
    }:
        raise ValueError("invalid automatic-history outcome")
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
    elif event_type in {"execution_capability", "execution_readiness"}:
        validate_execution_capability_payload(payload)
    elif event_type in {"account_snapshot", "position_snapshot", "order_snapshot", "fill_event"}:
        validate_execution_entity_payload(payload, event_type=event_type)
    elif event_type in {"order_intent_preview", "order_submission_result", "reconciliation_status", "execution_health", "arm_state", "compliance_decision"}:
        validate_execution_status_payload(payload, event_type=event_type)
    elif event_type in MANUAL_EVENT_FIELDS:
        validate_manual_event_payload(payload, event_type=event_type)
    message = {
        "v": PROTOCOL_VERSION,
        "type": event_type,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "payload": dict(payload),
    }
    _bounded_message(message)
    return message


def validate_event(message: Mapping[str, Any]) -> None:
    _bounded_message(message)
    _exact_fields(message, {"v", "type", "sent_at", "payload"}, name="cockpit event")
    if message.get("v") != PROTOCOL_VERSION:
        raise ValueError("unsupported cockpit protocol version")
    if message.get("type") not in EVENT_TYPES:
        raise ValueError("unsupported cockpit event type")
    if not isinstance(message.get("payload"), Mapping):
        raise ValueError("cockpit event payload must be a mapping")
    _aware_timestamp(message.get("sent_at"), name="sent_at")
    payload = message["payload"]
    if message.get("type") == "prediction_update":
        validate_prediction_payload(payload)
    elif message.get("type") == "data_health":
        validate_data_health_payload(payload)
    elif message.get("type") == "history_cache_status":
        validate_history_cache_payload(payload)
    elif message.get("type") in {"execution_capability", "execution_readiness"}:
        validate_execution_capability_payload(payload)
    elif message.get("type") in {"account_snapshot", "position_snapshot", "order_snapshot", "fill_event"}:
        validate_execution_entity_payload(payload, event_type=str(message.get("type")))
    elif message.get("type") in {"order_intent_preview", "order_submission_result", "reconciliation_status", "execution_health", "arm_state", "compliance_decision"}:
        validate_execution_status_payload(payload, event_type=str(message.get("type")))
    elif message.get("type") in MANUAL_EVENT_FIELDS:
        validate_manual_event_payload(payload, event_type=str(message.get("type")))


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
