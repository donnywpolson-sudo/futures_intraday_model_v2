"""Fail-closed, provider-free manual execution assistant for simulated accounts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from enum import Enum
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

from futures_rebuild.errors import ContractError
from futures_rebuild.prop_firm_account_runtime import (
    PortfolioRiskState,
    StopDefinedExposure,
    assert_no_same_underlying_hedge,
    build_runtime_identity,
    order_conduct_guard,
    size_runtime_order,
)

from .config import canonical_json


SCHEMA_VERSION = "manual_execution_assistant/1.1.0"
JOURNAL_SCHEMA_VERSION = "manual_execution_journal/1.1.0"
SNAPSHOT_MAX_AGE = timedelta(minutes=15)
MAX_COLLECTION = 100
MAX_NOTE = 500
HASH_RE = re.compile(r"^[0-9a-f]{64}$")
ALIAS_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,47}$")
PRIVATE_PATH_RE = re.compile(r"(?:^[A-Za-z]:[\\/]|^\\\\|/Users/|/home/)", re.IGNORECASE)
FORBIDDEN_FIELD_MARKERS = (
    "token", "password", "secret", "authorization", "apikey", "accountid",
    "credential", "privatekey", "clientid", "filepath", "directorypath",
)
CONTRACTS = {
    "ES": ("MES", Decimal("0.25"), Decimal("1.25"), "HMUZ"),
    "CL": ("MCL", Decimal("0.01"), Decimal("1.00"), "FGHJKMNQUVXZ"),
    "6E": ("M6E", Decimal("0.0001"), Decimal("1.25"), "HMUZ"),
}
BLOCKER_EXPLANATIONS = {
    "OPERATOR_SNAPSHOT_MISSING": "Reconcile a current operator-reported account snapshot.",
    "OPERATOR_SNAPSHOT_STALE": "Review and deliberately reconcile the account snapshot again.",
    "MARKET_DATA_STALE": "Wait for current market data before manual entry.",
    "MODEL_SETUP_STALE": "Refresh the model/setup record before manual entry.",
    "STRATEGY_POLICY_NOT_PROMOTED": "Bind an OOS-promoted strategy policy before manual entry.",
    "NEWS_RECORD_STALE_OR_BLOCKED": "Bind a current news record that permits entry.",
    "SESSION_RECORD_STALE_OR_BLOCKED": "Bind a current session record that permits entry.",
    "PRICE_LIMIT_RECORD_STALE_OR_BLOCKED": "Bind a current price-limit record that permits entry.",
    "STATE_UNCERTAIN": "Resolve the uncertain order or position state before preparing another entry.",
    "UNPROTECTED_POSITION": "Confirm a working protective stop for every reported open position.",
    "PROVISIONAL_COSTS_NOT_SELECTED": "Select the conservative provisional stress-cost policy.",
    "UNRESOLVED_MANUAL_ORDER_OR_POSITION": "Reconcile the reported order or position before preparing another entry.",
    "EQUIVALENT_PENDING_TICKET": "Resolve the equivalent pending manual ticket before preparing a duplicate.",
    "SAME_UNDERLYING_HEDGE_PROHIBITED": "The proposed direction would create a prohibited same-underlying hedge.",
    "OPERATOR_SNAPSHOT_PROFILE_HASH_MISMATCH": "Reconcile again after the selected profile changed.",
}


class ExecutionCapability(str, Enum):
    MANUAL_ONLY = "MANUAL_ONLY"
    READ_ONLY_API = "READ_ONLY_API"
    ORDER_API = "ORDER_API"


class ManualAuthority(str, Enum):
    MODEL_CALCULATED = "MODEL_CALCULATED"
    MFF_RULE_VALIDATED = "MFF_RULE_VALIDATED"
    OPERATOR_REPORTED = "OPERATOR_REPORTED"
    OPERATOR_CONFIRMED = "OPERATOR_CONFIRMED"
    BROKER_CONFIRMED = "BROKER_CONFIRMED"


class ManualTradeState(str, Enum):
    DRAFT = "DRAFT"
    BLOCKED = "BLOCKED"
    VALIDATED = "VALIDATED"
    READY_FOR_MANUAL_ENTRY = "READY_FOR_MANUAL_ENTRY"
    OPERATOR_REPORTED_SUBMITTED = "OPERATOR_REPORTED_SUBMITTED"
    OPERATOR_REPORTED_PARTIALLY_FILLED = "OPERATOR_REPORTED_PARTIALLY_FILLED"
    OPERATOR_REPORTED_FILLED = "OPERATOR_REPORTED_FILLED"
    OPERATOR_CONFIRMED_PROTECTED = "OPERATOR_CONFIRMED_PROTECTED"
    OPERATOR_REPORTED_REJECTED = "OPERATOR_REPORTED_REJECTED"
    OPERATOR_REPORTED_CANCELLED = "OPERATOR_REPORTED_CANCELLED"
    OPERATOR_REPORTED_CLOSED = "OPERATOR_REPORTED_CLOSED"
    OPERATOR_RECONCILED = "OPERATOR_RECONCILED"
    ABANDONED = "ABANDONED"
    STATE_UNCERTAIN = "STATE_UNCERTAIN"


TRANSITIONS = {
    ManualTradeState.DRAFT: {ManualTradeState.BLOCKED, ManualTradeState.VALIDATED, ManualTradeState.ABANDONED, ManualTradeState.STATE_UNCERTAIN},
    ManualTradeState.BLOCKED: {ManualTradeState.VALIDATED, ManualTradeState.ABANDONED, ManualTradeState.STATE_UNCERTAIN},
    ManualTradeState.VALIDATED: {ManualTradeState.READY_FOR_MANUAL_ENTRY, ManualTradeState.BLOCKED, ManualTradeState.ABANDONED, ManualTradeState.STATE_UNCERTAIN},
    ManualTradeState.READY_FOR_MANUAL_ENTRY: {ManualTradeState.OPERATOR_REPORTED_SUBMITTED, ManualTradeState.ABANDONED, ManualTradeState.STATE_UNCERTAIN},
    ManualTradeState.OPERATOR_REPORTED_SUBMITTED: {ManualTradeState.OPERATOR_REPORTED_PARTIALLY_FILLED, ManualTradeState.OPERATOR_REPORTED_FILLED, ManualTradeState.OPERATOR_REPORTED_REJECTED, ManualTradeState.OPERATOR_REPORTED_CANCELLED, ManualTradeState.STATE_UNCERTAIN},
    ManualTradeState.OPERATOR_REPORTED_PARTIALLY_FILLED: {ManualTradeState.OPERATOR_REPORTED_FILLED, ManualTradeState.OPERATOR_REPORTED_CANCELLED, ManualTradeState.STATE_UNCERTAIN},
    ManualTradeState.OPERATOR_REPORTED_FILLED: {ManualTradeState.OPERATOR_CONFIRMED_PROTECTED, ManualTradeState.STATE_UNCERTAIN},
    ManualTradeState.OPERATOR_CONFIRMED_PROTECTED: {ManualTradeState.OPERATOR_REPORTED_CLOSED, ManualTradeState.STATE_UNCERTAIN},
    ManualTradeState.OPERATOR_REPORTED_CLOSED: {ManualTradeState.OPERATOR_RECONCILED, ManualTradeState.STATE_UNCERTAIN},
    ManualTradeState.OPERATOR_REPORTED_REJECTED: {ManualTradeState.OPERATOR_RECONCILED, ManualTradeState.STATE_UNCERTAIN},
    ManualTradeState.OPERATOR_REPORTED_CANCELLED: {ManualTradeState.OPERATOR_RECONCILED, ManualTradeState.STATE_UNCERTAIN},
    ManualTradeState.STATE_UNCERTAIN: {ManualTradeState.OPERATOR_RECONCILED},
    ManualTradeState.OPERATOR_RECONCILED: set(),
    ManualTradeState.ABANDONED: set(),
}

REPORT_FIELDS: dict[ManualTradeState, tuple[set[str], set[str]]] = {
    ManualTradeState.OPERATOR_REPORTED_SUBMITTED: ({"actual_submission_time"}, {"actual_submission_time"}),
    ManualTradeState.OPERATOR_REPORTED_PARTIALLY_FILLED: ({"partial_fills"}, {"partial_fills"}),
    ManualTradeState.OPERATOR_REPORTED_FILLED: (
        {"actual_contract", "actual_side", "actual_quantity", "actual_fill_price", "actual_stop", "actual_target", "actual_fill_time", "actual_fees"},
        {"actual_contract", "actual_side", "actual_quantity", "actual_fill_price", "actual_stop", "actual_target", "actual_fill_time", "actual_fees"},
    ),
    ManualTradeState.OPERATOR_CONFIRMED_PROTECTED: ({"actual_stop", "confirmed_at"}, {"actual_stop", "confirmed_at"}),
    ManualTradeState.OPERATOR_REPORTED_REJECTED: ({"actual_rejection_reason"}, {"actual_rejection_reason"}),
    ManualTradeState.OPERATOR_REPORTED_CANCELLED: ({"cancelled_at"}, {"cancelled_at"}),
    ManualTradeState.OPERATOR_REPORTED_CLOSED: ({"actual_exit_price", "actual_exit_time", "actual_fees"}, {"actual_exit_price", "actual_exit_time", "actual_fees"}),
    ManualTradeState.OPERATOR_RECONCILED: ({"reconciliation_notes"}, {"reconciliation_notes"}),
    ManualTradeState.STATE_UNCERTAIN: ({"operator_notes"}, {"operator_notes"}),
    ManualTradeState.ABANDONED: ({"operator_notes"}, {"operator_notes"}),
}

ACTIVE_TICKET_STATES = frozenset(
    {
        ManualTradeState.READY_FOR_MANUAL_ENTRY,
        ManualTradeState.OPERATOR_REPORTED_SUBMITTED,
        ManualTradeState.OPERATOR_REPORTED_PARTIALLY_FILLED,
        ManualTradeState.OPERATOR_REPORTED_FILLED,
        ManualTradeState.OPERATOR_CONFIRMED_PROTECTED,
        ManualTradeState.OPERATOR_REPORTED_CLOSED,
        ManualTradeState.STATE_UNCERTAIN,
    }
)
UNRESOLVED_TICKET_STATES = frozenset(
    {
        ManualTradeState.OPERATOR_REPORTED_SUBMITTED,
        ManualTradeState.OPERATOR_REPORTED_PARTIALLY_FILLED,
        ManualTradeState.OPERATOR_REPORTED_FILLED,
        ManualTradeState.OPERATOR_REPORTED_CLOSED,
        ManualTradeState.STATE_UNCERTAIN,
    }
)
RISK_GROUPS = {"ES": "SP500_EQUITY_INDEX", "CL": "WTI_CRUDE_OIL", "6E": "EURUSD_FX"}


def _aware(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


def _decimal(value: object, name: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not number.is_finite():
        raise ValueError(f"{name} must be finite")
    return number


def _bounded(value: object, name: str, maximum: int = MAX_NOTE) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be a bounded string")
    return value.strip()


def _normalized_field(value: object) -> str:
    return "".join(character for character in str(value).lower() if character.isalnum())


def _reject_forbidden_data(value: object, *, name: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = _normalized_field(key)
            if any(normalized.endswith(marker) for marker in FORBIDDEN_FIELD_MARKERS):
                raise ValueError(f"{name} contains a forbidden field")
            if normalized.endswith("path"):
                raise ValueError(f"{name} contains a private path field")
            _reject_forbidden_data(item, name=name)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_forbidden_data(item, name=name)
    elif isinstance(value, str) and PRIVATE_PATH_RE.search(value):
        raise ValueError(f"{name} contains a private path")


def _safe_collection(value: object, name: str) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{name} must be a bounded mapping list")
    if len(value) > MAX_COLLECTION:
        raise ValueError(f"{name} is oversized")
    result: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise ValueError(f"{name} must contain mappings")
        _reject_forbidden_data(item, name=name)
        encoded = json.dumps(item, ensure_ascii=True)
        if len(encoded) > 2_000:
            raise ValueError(f"{name} contains oversized data")
        result.append(dict(item))
    return tuple(result)


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be positive")
    number = _decimal(value, name)
    integer = int(number.to_integral_value(rounding=ROUND_FLOOR))
    if integer <= 0:
        raise ValueError(f"{name} must be positive")
    return integer


def _reported_int(value: object, name: str, *, positive: bool = True) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or (value <= 0 if positive else value < 0):
        raise ValueError(f"{name} must be {'positive' if positive else 'nonnegative'}")
    return value


def _reported_collection(value: object, name: str, kind: str) -> tuple[dict[str, Any], ...]:
    collections = _safe_collection(value, name)
    schemas = {
        "open": {"signal_root", "execution_symbol", "execution_contract", "side", "quantity", "stop_ticks", "protection_status"},
        "working": {"signal_root", "execution_symbol", "execution_contract", "side", "quantity", "requested_quantity", "stop_ticks", "fill_status", "order_type", "entry_price"},
        "protective": {"signal_root", "execution_symbol", "execution_contract", "side", "quantity", "stop_price", "status"},
    }
    normalized: list[dict[str, Any]] = []
    for item in collections:
        if set(item) != schemas[kind]:
            raise ValueError(f"{name} fields are not exact")
        root = _bounded(item["signal_root"], f"{name} signal root", 8).upper()
        execution = _bounded(item["execution_symbol"], f"{name} execution symbol", 8).upper()
        if root not in CONTRACTS or execution != CONTRACTS[root][0]:
            raise ValueError(f"{name} contains an unsupported execution mapping")
        contract = _bounded(item["execution_contract"], f"{name} execution contract", 16).upper()
        if re.fullmatch(rf"{execution}[{CONTRACTS[root][3]}]\d", contract) is None:
            raise ValueError(f"{name} contains an unsupported execution contract")
        side = _bounded(item["side"], f"{name} side", 8).upper()
        if side not in {"BUY", "SELL", "LONG", "SHORT"}:
            raise ValueError(f"{name} side is invalid")
        result = dict(item)
        result.update(signal_root=root, execution_symbol=execution, execution_contract=contract, side=side)
        if kind == "open":
            result["quantity"] = _reported_int(item["quantity"], f"{name} quantity")
            result["stop_ticks"] = str(_decimal(item["stop_ticks"], f"{name} stop ticks"))
            if _decimal(result["stop_ticks"], f"{name} stop ticks") <= 0:
                raise ValueError(f"{name} stop ticks must be positive")
            result["protection_status"] = _bounded(item["protection_status"], f"{name} protection status", 40).upper()
        elif kind == "working":
            result["quantity"] = _reported_int(item["quantity"], f"{name} quantity", positive=False)
            result["requested_quantity"] = _reported_int(item["requested_quantity"], f"{name} requested quantity")
            result["stop_ticks"] = str(_decimal(item["stop_ticks"], f"{name} stop ticks"))
            if _decimal(result["stop_ticks"], f"{name} stop ticks") <= 0:
                raise ValueError(f"{name} stop ticks must be positive")
            result["fill_status"] = _bounded(item["fill_status"], f"{name} fill status", 24).upper()
            if result["fill_status"] not in {"UNKNOWN", "UNFILLED", "PARTIAL"}:
                raise ValueError(f"{name} fill status is invalid")
            result["order_type"] = _bounded(item["order_type"], f"{name} order type", 16).upper()
            if result["order_type"] not in {"LIMIT", "MARKET", "STOP", "STOP_LIMIT"}:
                raise ValueError(f"{name} order type is invalid")
            entry = _decimal(item["entry_price"], f"{name} entry price")
            if entry <= 0:
                raise ValueError(f"{name} entry price must be positive")
            result["entry_price"] = str(entry)
        else:
            result["quantity"] = _reported_int(item["quantity"], f"{name} quantity")
            stop_price = _decimal(item["stop_price"], f"{name} stop price")
            if stop_price <= 0:
                raise ValueError(f"{name} stop price must be positive")
            result["stop_price"] = str(stop_price)
            result["status"] = _bounded(item["status"], f"{name} status", 40).upper()
        normalized.append(result)
    return tuple(normalized)


def _timestamp(value: object, name: str) -> str:
    text = _bounded(value, name, 64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    _aware(parsed, name)
    return parsed.isoformat()


def _hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def _snapshot_core(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": str(value["timestamp"]),
        "provider": str(value["provider"]),
        "profile_id": str(value["profile_id"]),
        "profile_hash": str(value["profile_hash"]),
        "stage": str(value["stage"]),
        "account_alias": str(value["account_alias"]),
        "nominal_plan_size_usd": str(value["nominal_plan_size_usd"]),
        "realized_balance_usd": str(value["realized_balance_usd"]),
        "active_eod_floor_usd": str(value["active_eod_floor_usd"]),
        "floor_lock_status": str(value["floor_lock_status"]),
        "current_session_realized_pnl_usd": str(value["current_session_realized_pnl_usd"]),
        "open_positions": list(value["open_positions"]),
        "working_entry_orders": list(value["working_entry_orders"]),
        "protective_orders": list(value["protective_orders"]),
        "payout_state": str(value["payout_state"]),
        "source_authority": str(value["source_authority"]),
        "reconciliation_notes": str(value["reconciliation_notes"]),
        "schema_version": str(value["schema_version"]),
        "confirmed": bool(value["confirmed"]),
    }


def _contract_maturity(contract: str, *, symbol: str, months: str, now: datetime) -> tuple[str, int]:
    match = re.fullmatch(rf"{symbol}([{months}])(\d)", contract)
    if match is None:
        raise ValueError("execution contract must be an exact supported micro contract month")
    month_code, year_digit = match.groups()
    month_numbers = {code: index + 1 for index, code in enumerate("FGHJKMNQUVXZ")}
    month = month_numbers[month_code]
    candidates = [year for year in range(now.year, now.year + 10) if year % 10 == int(year_digit)]
    if not candidates:
        raise ValueError("execution contract maturity cannot be resolved")
    year = candidates[0]
    if (year, month) < (now.year, now.month) or (year * 12 + month) > (now.year * 12 + now.month + 24):
        raise ValueError("execution contract maturity must be current and within 24 months")
    return month_code, year


@dataclass(frozen=True)
class OperatorAccountSnapshot:
    snapshot_id: str
    timestamp: datetime
    provider: str
    profile_id: str
    profile_hash: str
    stage: str
    account_alias: str
    nominal_plan_size_usd: Decimal
    realized_balance_usd: Decimal
    active_eod_floor_usd: Decimal
    floor_lock_status: str
    current_session_realized_pnl_usd: Decimal
    open_positions: tuple[dict[str, Any], ...]
    working_entry_orders: tuple[dict[str, Any], ...]
    protective_orders: tuple[dict[str, Any], ...]
    payout_state: str
    source_authority: ManualAuthority
    reconciliation_notes: str
    schema_version: str
    snapshot_hash: str
    confirmed: bool

    @classmethod
    def create(cls, value: Mapping[str, Any], *, profile_hash: str, now: datetime) -> "OperatorAccountSnapshot":
        _aware(now, "snapshot timestamp")
        alias = _bounded(value.get("account_alias"), "account alias", 48)
        if not ALIAS_RE.fullmatch(alias):
            raise ValueError("account alias contains unsupported characters")
        stage = str(value.get("stage"))
        if stage not in {"evaluation", "sim_funded"}:
            raise ValueError("manual snapshot stage must be an MFF simulated stage")
        core = {
            "timestamp": now.isoformat(),
            "provider": "my_funded_futures",
            "profile_id": _bounded(value.get("profile_id"), "profile id", 128),
            "profile_hash": profile_hash,
            "stage": stage,
            "account_alias": alias,
            "nominal_plan_size_usd": str(_decimal(value.get("nominal_plan_size_usd"), "nominal plan size")),
            "realized_balance_usd": str(_decimal(value.get("realized_balance_usd"), "realized balance")),
            "active_eod_floor_usd": str(_decimal(value.get("active_eod_floor_usd"), "active EOD floor")),
            "floor_lock_status": _bounded(value.get("floor_lock_status"), "floor lock status", 80),
            "current_session_realized_pnl_usd": str(_decimal(value.get("current_session_realized_pnl_usd"), "session P&L")),
            "open_positions": list(_reported_collection(value.get("open_positions", ()), "open positions", "open")),
            "working_entry_orders": list(_reported_collection(value.get("working_entry_orders", ()), "working orders", "working")),
            "protective_orders": list(_reported_collection(value.get("protective_orders", ()), "protective orders", "protective")),
            "payout_state": _bounded(value.get("payout_state", "NOT_APPLICABLE"), "payout state", 80),
            "source_authority": ManualAuthority.OPERATOR_REPORTED.value,
            "reconciliation_notes": str(value.get("reconciliation_notes", ""))[:MAX_NOTE],
            "schema_version": SCHEMA_VERSION,
            "confirmed": True,
        }
        _reject_forbidden_data(core, name="operator snapshot")
        digest = _hash(core)
        return cls(
            snapshot_id=f"snapshot-{digest[:24]}", timestamp=now,
            provider="my_funded_futures", profile_id=core["profile_id"],
            profile_hash=profile_hash, stage=stage, account_alias=alias,
            nominal_plan_size_usd=Decimal(core["nominal_plan_size_usd"]),
            realized_balance_usd=Decimal(core["realized_balance_usd"]),
            active_eod_floor_usd=Decimal(core["active_eod_floor_usd"]),
            floor_lock_status=core["floor_lock_status"],
            current_session_realized_pnl_usd=Decimal(core["current_session_realized_pnl_usd"]),
            open_positions=tuple(core["open_positions"]),
            working_entry_orders=tuple(core["working_entry_orders"]),
            protective_orders=tuple(core["protective_orders"]),
            payout_state=core["payout_state"], source_authority=ManualAuthority.OPERATOR_REPORTED,
            reconciliation_notes=core["reconciliation_notes"], schema_version=SCHEMA_VERSION,
            snapshot_hash=digest, confirmed=True,
        )

    def current(self, *, now: datetime, profile_id: str, stage: str, alias: str, restart_stale: bool) -> bool:
        return bool(
            self.confirmed and not restart_stale and self.timestamp <= now
            and now - self.timestamp <= SNAPSHOT_MAX_AGE
            and self.profile_id == profile_id and self.stage == stage and self.account_alias == alias
        )


@dataclass(frozen=True)
class ManualReadinessInputs:
    now: datetime
    market_data_at: datetime
    model_setup_at: datetime
    strategy_policy_status: str
    session_status: str
    session_record_at: datetime
    news_status: str
    news_record_at: datetime
    price_limit_status: str
    price_limit_record_at: datetime
    maximum_age_seconds: int = 120

    def blockers(self) -> list[str]:
        _aware(self.now, "readiness time")
        if not isinstance(self.maximum_age_seconds, int) or not 1 <= self.maximum_age_seconds <= 3600:
            raise ValueError("maximum readiness age must be bounded")
        values: list[str] = []
        for timestamp, code in (
            (self.market_data_at, "MARKET_DATA_STALE"),
            (self.model_setup_at, "MODEL_SETUP_STALE"),
            (self.session_record_at, "SESSION_RECORD_STALE_OR_BLOCKED"),
            (self.news_record_at, "NEWS_RECORD_STALE_OR_BLOCKED"),
            (self.price_limit_record_at, "PRICE_LIMIT_RECORD_STALE_OR_BLOCKED"),
        ):
            _aware(timestamp, code)
            if timestamp > self.now or self.now - timestamp > timedelta(seconds=self.maximum_age_seconds):
                values.append(code)
        if self.strategy_policy_status not in {"OOS_PROMOTED", "SYNTHETIC_DEMO_PROMOTED"}:
            values.append("STRATEGY_POLICY_NOT_PROMOTED")
        if self.session_status != "OPEN_ENTRY_PERMITTED":
            values.append("SESSION_RECORD_STALE_OR_BLOCKED")
        if self.news_status != "ENTRY_PERMITTED":
            values.append("NEWS_RECORD_STALE_OR_BLOCKED")
        if self.price_limit_status != "ENTRY_PERMITTED":
            values.append("PRICE_LIMIT_RECORD_STALE_OR_BLOCKED")
        return list(dict.fromkeys(values))


@dataclass(frozen=True)
class ManualTicket:
    ticket_id: str
    prepared_at: datetime
    provider: str
    profile_id: str
    stage: str
    capability: ExecutionCapability
    account_alias: str
    signal_instrument: str
    execution_contract: str
    contract_month_code: str
    contract_year: int
    side: str
    order_type: str
    requested_entry: Decimal
    tick_aligned_entry: Decimal
    protective_stop: Decimal
    target: Decimal
    requested_quantity: int
    authoritative_maximum_quantity: int
    approved_quantity: int
    tick_size: Decimal
    tick_value_usd: Decimal
    risk_per_micro_usd: Decimal
    planned_stop_risk_usd: Decimal
    provisional_fees_usd: Decimal
    expected_slippage_usd: Decimal
    projected_micro_equivalents: int
    projected_concurrent_stop_risk_usd: Decimal
    current_balance_usd: Decimal
    active_floor_usd: Decimal
    distance_to_floor_usd: Decimal
    internal_reserve_usd: Decimal
    cost_policy_id: str
    cost_status: str
    manual_ticket_preview_available: bool
    manual_assistant_readiness: bool
    provider_api_readiness: bool
    automatic_execution_authorized: bool
    risk_decision: str
    blocker_reason_codes: tuple[str, ...]
    blocker_explanations: tuple[str, ...]
    state: ManualTradeState
    authority: ManualAuthority
    actual: dict[str, Any] = field(default_factory=dict)


def _tick_aligned(value: Decimal, tick: Decimal) -> bool:
    return value % tick == 0


def _exposure(item: Mapping[str, Any], *, worst_case: bool = False) -> StopDefinedExposure:
    quantity = int(item.get("quantity", 0))
    if worst_case and str(item.get("fill_status", "UNKNOWN")) == "UNKNOWN":
        quantity = max(quantity, int(item.get("requested_quantity", quantity)))
    return StopDefinedExposure(
        signal_root=str(item.get("signal_root", "")),
        execution_symbol=str(item.get("execution_symbol", "")),
        quantity=quantity,
        stop_ticks=_decimal(item.get("stop_ticks"), "reported stop ticks"),
    )


class ManualStateStore:
    """Atomic snapshot plus deterministic hash-chained operator event journal."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.state_path = root / "manual_execution_state.json"
        self.journal_path = root / "manual_execution_events.jsonl"
        self.corrupt = False
        self.persisted: dict[str, Any] = {}
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
            if raw.get("schema_version") != SCHEMA_VERSION or raw.get("state_hash") != _hash({k: v for k, v in raw.items() if k != "state_hash"}):
                raise ValueError("manual state hash or schema is invalid")
            _reject_forbidden_data(raw, name="manual state")
            self.persisted = raw
        except FileNotFoundError:
            pass
        except (OSError, ValueError, json.JSONDecodeError):
            self.corrupt = True

        try:
            journal_head, journal_sequence = self._validated_journal_tail()
            if self.persisted and (
                self.persisted.get("journal_head_hash") != journal_head
                or self.persisted.get("journal_sequence") != journal_sequence
            ):
                raise ValueError("manual state and journal are not bound")
            if not self.persisted and journal_sequence:
                raise ValueError("manual journal exists without bound state")
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            self.corrupt = True

    def _validated_journal_tail(self) -> tuple[str, int]:
        previous = "0" * 64
        sequence = 0
        if not self.journal_path.exists():
            return previous, sequence
        for line in self.journal_path.read_text(encoding="utf-8").splitlines():
            event = json.loads(line)
            sequence += 1
            if (
                event.get("schema_version") != JOURNAL_SCHEMA_VERSION
                or event.get("sequence") != sequence
                or event.get("previous_event_hash") != previous
            ):
                raise ValueError("manual journal chain is invalid")
            core = {
                key: event[key]
                for key in (
                    "schema_version", "sequence", "event_type", "at", "authority",
                    "previous_event_hash", "payload",
                )
            }
            if event.get("event_hash") != _hash(core) or event.get("event_id") != f"event-{_hash(core)[:24]}":
                raise ValueError("manual journal hash is invalid")
            _timestamp(event["at"], "journal event time")
            _reject_forbidden_data(event.get("payload"), name="journal payload")
            previous = str(event["event_hash"])
        return previous, sequence

    def recover_corrupt_files(self) -> tuple[Path, ...]:
        """Preserve corrupt bytes under hash-bound names before explicit reconciliation."""

        if not self.corrupt:
            return ()
        self.root.mkdir(parents=True, exist_ok=True)
        preserved: list[Path] = []
        for path in (self.state_path, self.journal_path):
            if not path.exists():
                continue
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            archive = path.with_name(f"{path.stem}.corrupt-{digest}{path.suffix}")
            os.replace(path, archive)
            preserved.append(archive)
        self.persisted = {}
        self.corrupt = False
        return tuple(preserved)

    def write_state(self, value: Mapping[str, Any]) -> None:
        if self.corrupt:
            raise ValueError("corrupt manual state requires explicit reconciliation")
        _reject_forbidden_data(value, name="manual state")
        core = {"schema_version": SCHEMA_VERSION, **_jsonable(dict(value))}
        payload = {**core, "state_hash": _hash(core)}
        self.root.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(".tmp")
        try:
            with temporary.open("wb") as stream:
                stream.write(canonical_json(payload))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.state_path)
        except OSError:
            self.corrupt = True
            raise
        self.persisted = payload

    def append(self, event_type: str, payload: Mapping[str, Any], *, at: datetime) -> dict[str, Any]:
        if self.corrupt:
            raise ValueError("corrupt manual journal requires explicit reconciliation")
        _reject_forbidden_data(payload, name="manual event")
        try:
            previous, prior_sequence = self._validated_journal_tail()
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            self.corrupt = True
            raise ValueError("manual journal chain is corrupt") from exc
        sequence = prior_sequence + 1
        core = {
            "schema_version": JOURNAL_SCHEMA_VERSION,
            "sequence": sequence,
            "event_type": _bounded(event_type, "event type", 80),
            "at": _aware(at, "event time").isoformat(),
            "authority": ManualAuthority.OPERATOR_REPORTED.value,
            "previous_event_hash": previous,
            "payload": _jsonable(dict(payload)),
        }
        event = {**core, "event_id": f"event-{_hash(core)[:24]}", "event_hash": _hash(core)}
        self.root.mkdir(parents=True, exist_ok=True)
        with self.journal_path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        return event


class ManualExecutionAssistant:
    """Manual ticket, operator reporting, persistence, and comparison boundary."""

    def __init__(self, *, repository_root: Path, state_root: Path) -> None:
        self.repository_root = repository_root
        self.store = ManualStateStore(state_root)
        self.snapshot: OperatorAccountSnapshot | None = None
        self.tickets: dict[str, ManualTicket] = {}
        self.restart_stale = True
        if self.store.corrupt:
            self.uncertain = True
        else:
            self.uncertain = bool(self.store.persisted.get("uncertain", False))
            try:
                raw_snapshot = self.store.persisted.get("snapshot")
                if isinstance(raw_snapshot, Mapping):
                    timestamp = datetime.fromisoformat(str(raw_snapshot["timestamp"]))
                    _aware(timestamp, "persisted snapshot timestamp")
                    if (
                        raw_snapshot.get("source_authority") != ManualAuthority.OPERATOR_REPORTED.value
                        or raw_snapshot.get("snapshot_hash") != _hash(_snapshot_core(raw_snapshot))
                    ):
                        raise ValueError("persisted operator snapshot binding is invalid")
                    self.snapshot = OperatorAccountSnapshot(
                        snapshot_id=str(raw_snapshot["snapshot_id"]),
                        timestamp=timestamp,
                        provider=str(raw_snapshot["provider"]),
                        profile_id=str(raw_snapshot["profile_id"]),
                        profile_hash=str(raw_snapshot["profile_hash"]),
                        stage=str(raw_snapshot["stage"]),
                        account_alias=str(raw_snapshot["account_alias"]),
                        nominal_plan_size_usd=Decimal(str(raw_snapshot["nominal_plan_size_usd"])),
                        realized_balance_usd=Decimal(str(raw_snapshot["realized_balance_usd"])),
                        active_eod_floor_usd=Decimal(str(raw_snapshot["active_eod_floor_usd"])),
                        floor_lock_status=str(raw_snapshot["floor_lock_status"]),
                        current_session_realized_pnl_usd=Decimal(str(raw_snapshot["current_session_realized_pnl_usd"])),
                        open_positions=tuple(dict(item) for item in raw_snapshot["open_positions"]),
                        working_entry_orders=tuple(dict(item) for item in raw_snapshot["working_entry_orders"]),
                        protective_orders=tuple(dict(item) for item in raw_snapshot["protective_orders"]),
                        payout_state=str(raw_snapshot["payout_state"]),
                        source_authority=ManualAuthority(str(raw_snapshot["source_authority"])),
                        reconciliation_notes=str(raw_snapshot["reconciliation_notes"]),
                        schema_version=str(raw_snapshot["schema_version"]),
                        snapshot_hash=str(raw_snapshot["snapshot_hash"]),
                        confirmed=bool(raw_snapshot["confirmed"]),
                    )
                raw_tickets = self.store.persisted.get("tickets", {})
                if isinstance(raw_tickets, Mapping):
                    decimal_fields = {
                        "requested_entry", "tick_aligned_entry", "protective_stop", "target",
                        "tick_size", "tick_value_usd", "risk_per_micro_usd",
                        "planned_stop_risk_usd", "provisional_fees_usd", "expected_slippage_usd",
                        "projected_concurrent_stop_risk_usd", "current_balance_usd",
                        "active_floor_usd", "distance_to_floor_usd", "internal_reserve_usd",
                    }
                    for ticket_id, raw in raw_tickets.items():
                        if not isinstance(raw, Mapping):
                            raise ValueError("persisted manual ticket must be a mapping")
                        values = dict(raw)
                        for name in decimal_fields:
                            values[name] = Decimal(str(values[name]))
                        values["prepared_at"] = datetime.fromisoformat(str(values["prepared_at"]))
                        _aware(values["prepared_at"], "persisted ticket timestamp")
                        values["capability"] = ExecutionCapability(str(values["capability"]))
                        values["state"] = ManualTradeState(str(values["state"]))
                        values["authority"] = ManualAuthority(str(values["authority"]))
                        values["blocker_reason_codes"] = tuple(values["blocker_reason_codes"])
                        values["blocker_explanations"] = tuple(values["blocker_explanations"])
                        loaded = ManualTicket(**values)
                        stale_reasons = tuple(dict.fromkeys((*loaded.blocker_reason_codes, "OPERATOR_SNAPSHOT_STALE")))
                        loaded = replace(
                            loaded,
                            manual_assistant_readiness=False,
                            provider_api_readiness=False,
                            automatic_execution_authorized=False,
                            blocker_reason_codes=stale_reasons,
                            blocker_explanations=tuple(
                                BLOCKER_EXPLANATIONS.get(code, code.replace("_", " ").title())
                                for code in stale_reasons
                            ),
                        )
                        if loaded.state is ManualTradeState.READY_FOR_MANUAL_ENTRY:
                            loaded = replace(
                                loaded,
                                state=ManualTradeState.BLOCKED,
                                authority=ManualAuthority.MODEL_CALCULATED,
                                approved_quantity=0,
                                risk_decision="BLOCK",
                            )
                        self.tickets[str(ticket_id)] = loaded
            except (KeyError, TypeError, ValueError, InvalidOperation):
                self.snapshot = None
                self.tickets = {}
                self.uncertain = True

    def record_snapshot(self, value: Mapping[str, Any], *, confirmation: str, now: datetime | None = None) -> OperatorAccountSnapshot:
        observed = now or datetime.now(timezone.utc)
        profile_id = str(value.get("profile_id", ""))
        profiles = json.loads((self.repository_root / "configs/prop_firm_profiles.json").read_text(encoding="utf-8"))
        profile = profiles["profiles"][profile_id]
        profile_hash = _hash(profile)
        alias = str(value.get("account_alias", ""))
        if confirmation != f"RECONCILE {alias}":
            raise ValueError("deliberate reconciliation confirmation does not match account alias")
        snapshot = OperatorAccountSnapshot.create(value, profile_hash=profile_hash, now=observed)
        self.store.recover_corrupt_files()
        self.snapshot = snapshot
        self.restart_stale = False
        self.uncertain = any(ticket.state is ManualTradeState.STATE_UNCERTAIN for ticket in self.tickets.values())
        self.store.append("OPERATOR_ACCOUNT_SNAPSHOT", {"snapshot_id": snapshot.snapshot_id, "snapshot_hash": snapshot.snapshot_hash, "profile_id": profile_id, "stage": snapshot.stage, "account_alias": alias}, at=observed)
        self._persist()
        return snapshot

    def _persist(self) -> None:
        journal_head, journal_sequence = self.store._validated_journal_tail()
        self.store.write_state({
            "uncertain": self.uncertain,
            "snapshot": _jsonable(asdict(self.snapshot)) if self.snapshot else None,
            "tickets": {key: _jsonable(asdict(value)) for key, value in self.tickets.items()},
            "restart_policy": "STALE_RECONCILIATION_REQUIRED",
            "provider_permission_restored": False,
            "pending_automatic_action": False,
            "journal_head_hash": journal_head,
            "journal_sequence": journal_sequence,
        })

    def _manual_blockers(self, *, profile_id: str, stage: str, alias: str, inputs: ManualReadinessInputs) -> list[str]:
        blockers = inputs.blockers()
        if self.store.corrupt or self.uncertain:
            blockers.append("STATE_UNCERTAIN")
        if self.snapshot is None:
            blockers.append("OPERATOR_SNAPSHOT_MISSING")
        elif not self.snapshot.current(now=inputs.now, profile_id=profile_id, stage=stage, alias=alias, restart_stale=self.restart_stale):
            blockers.append("OPERATOR_SNAPSHOT_STALE")
        if any(ticket.state is ManualTradeState.OPERATOR_REPORTED_FILLED for ticket in self.tickets.values()):
            blockers.append("UNPROTECTED_POSITION")
        if self.snapshot is not None and any(
            str(item.get("protection_status", "")).upper() != "CONFIRMED_WORKING"
            for item in self.snapshot.open_positions
        ):
            blockers.append("UNPROTECTED_POSITION")
        if any(ticket.state in UNRESOLVED_TICKET_STATES for ticket in self.tickets.values()):
            blockers.append("UNRESOLVED_MANUAL_ORDER_OR_POSITION")
        return list(dict.fromkeys(blockers))

    def _pending_exposures(self) -> tuple[StopDefinedExposure, ...]:
        values: list[StopDefinedExposure] = []
        for ticket in self.tickets.values():
            if ticket.state not in UNRESOLVED_TICKET_STATES:
                continue
            reported = ticket.actual.get("actual_quantity")
            if reported is None:
                quantity = ticket.approved_quantity or ticket.requested_quantity
            else:
                quantity = max(int(reported), 0)
            if quantity:
                values.append(
                    StopDefinedExposure(
                        ticket.signal_instrument,
                        ticket.execution_contract[:-2],
                        quantity,
                        abs(ticket.tick_aligned_entry - ticket.protective_stop) / ticket.tick_size,
                    )
                )
        return tuple(values)

    def prepare_ticket(self, value: Mapping[str, Any], *, inputs: ManualReadinessInputs) -> ManualTicket:
        signal = str(value.get("signal_instrument", "")).upper()
        if signal == "ZN" or signal not in CONTRACTS:
            raise ValueError("signal instrument has no verified micro mapping")
        symbol, tick, tick_value, months = CONTRACTS[signal]
        contract = str(value.get("execution_contract", "")).upper()
        month_code, contract_year = _contract_maturity(contract, symbol=symbol, months=months, now=inputs.now)
        if contract.startswith(("ES", "CL", "6E")):
            raise ValueError("mini or standard contracts are not permitted")
        side = str(value.get("side", "")).upper()
        order_type = str(value.get("order_type", "")).upper()
        if side not in {"BUY", "SELL"} or order_type not in {"LIMIT", "MARKET", "STOP", "STOP_LIMIT"}:
            raise ValueError("side or order type is invalid")
        entry = _decimal(value.get("entry_price"), "entry price")
        stop = _decimal(value.get("stop_price"), "protective stop")
        target = _decimal(value.get("target_price"), "target price")
        if not all(_tick_aligned(price, tick) for price in (entry, stop, target)):
            raise ValueError("entry, stop, and target must be tick aligned")
        stop_ticks = abs(entry - stop) / tick
        if stop_ticks <= 0:
            raise ValueError("protective stop must define positive risk")
        if (side == "BUY" and not stop < entry < target) or (side == "SELL" and not target < entry < stop):
            raise ValueError("stop and target must be directionally valid")
        quantity = _positive_int(value.get("quantity"), "quantity")
        profile_id = _bounded(value.get("profile_id"), "profile id", 128)
        stage = str(value.get("stage"))
        alias = _bounded(value.get("account_alias"), "account alias", 48)
        blockers = self._manual_blockers(profile_id=profile_id, stage=stage, alias=alias, inputs=inputs)
        try:
            profiles = json.loads((self.repository_root / "configs/prop_firm_profiles.json").read_text(encoding="utf-8"))
            if self.snapshot is not None and self.snapshot.profile_hash != _hash(profiles["profiles"][profile_id]):
                blockers.append("OPERATOR_SNAPSHOT_PROFILE_HASH_MISMATCH")
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            blockers.append("OPERATOR_SNAPSHOT_PROFILE_HASH_MISMATCH")
        if any(
            ticket.state in ACTIVE_TICKET_STATES
            and ticket.profile_id == profile_id
            and ticket.stage == stage
            and ticket.signal_instrument == signal
            and ticket.execution_contract == contract
            and ticket.side == side
            and ticket.order_type == order_type
            and ticket.tick_aligned_entry == entry
            and ticket.protective_stop == stop
            for ticket in self.tickets.values()
        ):
            blockers.append("EQUIVALENT_PENDING_TICKET")
        if stage not in {"evaluation", "sim_funded"}:
            blockers.append("STAGE_NOT_MANUAL_ONLY")
        provisional_fee = Decimal("0")
        provisional_slippage = Decimal("0")
        try:
            costs = json.loads((self.repository_root / "configs/prop_firm_execution_costs.json").read_text(encoding="utf-8"))
            cost = costs["cost_profiles"]["mff_micro_provisional_stress_v1"]
            if (
                cost.get("provisional") is not True
                or cost.get("production_readiness") is not False
                or cost.get("exact_provider_account_costs_verified") is not False
            ):
                raise ValueError("provisional cost policy status is invalid")
            provisional_fee = _decimal(cost["round_turn_commission_usd"][symbol], "provisional fee")
            provisional_slippage = _decimal(cost["expected_slippage_usd"][symbol], "provisional slippage")
            if provisional_fee <= 0 or provisional_slippage <= 0:
                raise ValueError("provisional costs must be positive")
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            blockers.append("PROVISIONAL_COSTS_NOT_SELECTED")
        authoritative = 0
        projected = 0
        existing_risk = Decimal("0")
        per_contract = stop_ticks * tick_value + Decimal("5.00")
        snapshot = self.snapshot
        if snapshot is not None and stage == "sim_funded":
            try:
                portfolio = PortfolioRiskState(
                    open_positions=tuple(_exposure(item) for item in snapshot.open_positions),
                    working_entries=tuple(_exposure(item, worst_case=True) for item in snapshot.working_entry_orders) + self._pending_exposures(),
                    realized_session_loss_usd=max(Decimal("0"), -snapshot.current_session_realized_pnl_usd),
                    current_equity_usd=snapshot.realized_balance_usd,
                    active_floor_usd=snapshot.active_eod_floor_usd,
                )
                identity = build_runtime_identity(root=self.repository_root, account_stage=stage, research_cost_profile_id="mff_micro_provisional_stress_v1")
                existing_directional = []
                for item in (*snapshot.open_positions, *snapshot.working_entry_orders):
                    root = str(item.get("signal_root", "")).upper()
                    existing_directional.append(
                        {
                            "underlying_risk_group": RISK_GROUPS.get(root, root),
                            "side": "LONG" if str(item.get("side", "")).upper() in {"BUY", "LONG"} else "SHORT",
                            "quantity": int(item.get("quantity", item.get("requested_quantity", 0))),
                        }
                    )
                assert_no_same_underlying_hedge(
                    existing=existing_directional,
                    proposed={"underlying_risk_group": RISK_GROUPS[signal], "side": "LONG" if side == "BUY" else "SHORT"},
                )
                conduct = order_conduct_guard(
                    recent_order_timestamps=(), now=inputs.now, rate_limit_per_minute=12,
                    existing_working_orders=[
                        {
                            "symbol": item["execution_contract"],
                            "side": "BUY" if str(item["side"]).upper() in {"BUY", "LONG"} else "SELL",
                            "order_type": item["order_type"],
                            "limit_price": str(item["entry_price"]),
                            "quantity": int(item["requested_quantity"]),
                        }
                        for item in snapshot.working_entry_orders
                    ],
                    proposed_order={
                        "symbol": contract, "side": side, "order_type": order_type,
                        "limit_price": str(entry), "quantity": quantity,
                    },
                )
                if not conduct.allowed:
                    blockers.extend(conduct.reasons)
                sizing = size_runtime_order(
                    root=self.repository_root, observed_runtime_identity=identity,
                    account_stage=stage, mode="PROVISIONAL_RESEARCH",
                    research_cost_profile_id="mff_micro_provisional_stress_v1",
                    strategy_candidate_id=str(value.get("strategy_candidate_id", "coarse-3")),
                    signal_root=signal, requested_execution_symbol=symbol,
                    stop_ticks=stop_ticks, portfolio_state=portfolio,
                )
                authoritative = sizing.quantity
                existing_quantity = sizing.projected_micro_equivalent - sizing.quantity
                projected = existing_quantity + quantity
                existing_risk = sizing.existing_stop_defined_risk_usd
                per_contract = sizing.risk_per_contract_usd
            except ContractError as exc:
                blockers.append(
                    "SAME_UNDERLYING_HEDGE_PROHIBITED"
                    if str(exc) == "order would create prohibited same-underlying hedge"
                    else "MFF_RUNTIME_SIZING_REJECTED"
                )
            except ValueError:
                blockers.append("MFF_RUNTIME_SIZING_REJECTED")
        else:
            blockers.append("MFF_RUNTIME_STAGE_OR_SNAPSHOT_UNAVAILABLE")
        if quantity > authoritative:
            blockers.append("REQUESTED_QUANTITY_EXCEEDS_AUTHORITATIVE_MAXIMUM")
        approved = quantity if not blockers and quantity <= authoritative else 0
        core = {"profile_id": profile_id, "stage": stage, "alias": alias, "signal": signal, "contract": contract, "side": side, "order_type": order_type, "entry": str(entry), "stop": str(stop), "target": str(target), "quantity": quantity, "prepared_at": inputs.now.isoformat(), "ticket_sequence": len(self.tickets) + 1}
        ticket_id = f"manual-{_hash(core)[:24]}"
        state = ManualTradeState.READY_FOR_MANUAL_ENTRY if not blockers else ManualTradeState.BLOCKED
        authority = ManualAuthority.MFF_RULE_VALIDATED if not blockers else ManualAuthority.MODEL_CALCULATED
        balance = snapshot.realized_balance_usd if snapshot else Decimal("0")
        floor = snapshot.active_eod_floor_usd if snapshot else Decimal("0")
        ticket = ManualTicket(
            ticket_id=ticket_id, prepared_at=inputs.now, provider="my_funded_futures",
            profile_id=profile_id, stage=stage, capability=ExecutionCapability.MANUAL_ONLY,
            account_alias=alias, signal_instrument=signal, execution_contract=contract,
            contract_month_code=month_code, contract_year=contract_year,
            side=side, order_type=order_type, requested_entry=entry, tick_aligned_entry=entry,
            protective_stop=stop, target=target, requested_quantity=quantity,
            authoritative_maximum_quantity=authoritative, approved_quantity=approved,
            tick_size=tick, tick_value_usd=tick_value, risk_per_micro_usd=per_contract,
            planned_stop_risk_usd=per_contract * (approved or quantity),
            provisional_fees_usd=provisional_fee * (approved or quantity),
            expected_slippage_usd=provisional_slippage * (approved or quantity),
            projected_micro_equivalents=projected,
            projected_concurrent_stop_risk_usd=existing_risk + per_contract * (approved or quantity),
            current_balance_usd=balance, active_floor_usd=floor, distance_to_floor_usd=balance - floor,
            internal_reserve_usd=Decimal("500"), cost_policy_id="mff_micro_provisional_stress_v1",
            cost_status="PROVISIONAL_NOT_OFFICIAL", manual_ticket_preview_available=True,
            manual_assistant_readiness=not blockers, provider_api_readiness=False,
            automatic_execution_authorized=False, risk_decision="PASS" if not blockers else "BLOCK",
            blocker_reason_codes=tuple(dict.fromkeys(blockers)),
            blocker_explanations=tuple(BLOCKER_EXPLANATIONS.get(code, code.replace("_", " ").title()) for code in dict.fromkeys(blockers)),
            state=state, authority=authority,
        )
        self.store.append("MANUAL_TICKET_PREPARED", {"ticket_id": ticket_id, "state": state.value, "authority": authority.value, "blockers": list(ticket.blocker_reason_codes)}, at=inputs.now)
        self.tickets[ticket_id] = ticket
        self._persist()
        return ticket

    def transition(self, ticket_id: str, target: str, report: Mapping[str, Any] | None = None, *, now: datetime | None = None) -> ManualTicket:
        observed = now or datetime.now(timezone.utc)
        _aware(observed, "transition time")
        ticket = self.tickets[ticket_id]
        destination = ManualTradeState(target)
        if destination not in TRANSITIONS[ticket.state]:
            raise ValueError(f"invalid manual transition {ticket.state.value} -> {destination.value}")
        details = dict(report or {})
        _reject_forbidden_data(details, name="operator report")
        if len(json.dumps(details, ensure_ascii=True)) > 8_000:
            raise ValueError("operator report is oversized")
        allowed, required = REPORT_FIELDS.get(destination, (set(), set()))
        if not required.issubset(details) or not set(details).issubset(allowed):
            raise ValueError("operator report fields are not exact for the requested transition")
        for name in ("actual_submission_time", "actual_fill_time", "confirmed_at", "cancelled_at", "actual_exit_time"):
            if name in details:
                details[name] = _timestamp(details[name], name)
        for name in ("actual_contract", "actual_side"):
            if name in details:
                details[name] = _bounded(details[name], name, 32).upper()
        if "actual_side" in details and details["actual_side"] not in {"BUY", "SELL"}:
            raise ValueError("actual side must be BUY or SELL")
        for name in ("actual_fill_price", "actual_stop", "actual_target", "actual_exit_price", "actual_fees"):
            if name in details:
                number = _decimal(details[name], name)
                if (name != "actual_fees" and number <= 0) or (name == "actual_fees" and number < 0):
                    raise ValueError(f"{name} is invalid")
                details[name] = str(number)
        if "actual_quantity" in details:
            quantity = _positive_int(details["actual_quantity"], "actual quantity")
            details["actual_quantity"] = quantity
            if quantity > ticket.approved_quantity:
                destination = ManualTradeState.STATE_UNCERTAIN
        partials = details.get("partial_fills")
        if partials is not None:
            if not isinstance(partials, list) or not 1 <= len(partials) <= MAX_COLLECTION:
                raise ValueError("partial fills must be a bounded nonempty list")
            normalized_partials: list[dict[str, Any]] = []
            total = 0
            weighted = Decimal("0")
            last_time = ""
            for item in partials:
                if not isinstance(item, Mapping) or set(item) != {"quantity", "price", "time"}:
                    raise ValueError("partial fill fields are not exact")
                fill_quantity = _positive_int(item["quantity"], "partial fill quantity")
                fill_price = _decimal(item["price"], "partial fill price")
                if fill_price <= 0:
                    raise ValueError("partial fill price must be positive")
                fill_time = _timestamp(item["time"], "partial fill time")
                total += fill_quantity
                weighted += fill_price * fill_quantity
                last_time = fill_time
                normalized_partials.append({"quantity": fill_quantity, "price": str(fill_price), "time": fill_time})
            details["partial_fills"] = normalized_partials
            details["actual_quantity"] = total
            details["actual_fill_price"] = str(weighted / total)
            details["actual_fill_time"] = last_time
            if total >= ticket.approved_quantity:
                destination = ManualTradeState.STATE_UNCERTAIN
        if destination is ManualTradeState.OPERATOR_REPORTED_CANCELLED and ticket.actual.get("actual_quantity", 0):
            destination = ManualTradeState.STATE_UNCERTAIN
        if destination is ManualTradeState.OPERATOR_CONFIRMED_PROTECTED:
            fill_price = _decimal(ticket.actual.get("actual_fill_price"), "actual fill price")
            fill_side = str(ticket.actual.get("actual_side", ticket.side)).upper()
            confirmed_stop = _decimal(details["actual_stop"], "actual stop")
            if not _tick_aligned(confirmed_stop, ticket.tick_size) or (
                fill_side == "BUY" and confirmed_stop >= fill_price
            ) or (fill_side == "SELL" and confirmed_stop <= fill_price):
                destination = ManualTradeState.STATE_UNCERTAIN
        authority = ManualAuthority.OPERATOR_CONFIRMED if destination in {ManualTradeState.OPERATOR_CONFIRMED_PROTECTED, ManualTradeState.OPERATOR_RECONCILED} else ManualAuthority.OPERATOR_REPORTED
        if authority is ManualAuthority.BROKER_CONFIRMED:
            raise ValueError("broker-confirmed authority is unavailable in manual-only mode")
        updated = replace(ticket, state=destination, authority=authority, actual={**ticket.actual, **details})
        self.restart_stale = True
        if destination is ManualTradeState.STATE_UNCERTAIN:
            self.uncertain = True
        elif destination is ManualTradeState.OPERATOR_RECONCILED and ticket.state is ManualTradeState.STATE_UNCERTAIN:
            self.uncertain = any(
                other.ticket_id != ticket_id and other.state is ManualTradeState.STATE_UNCERTAIN
                for other in self.tickets.values()
            )
        self.store.append("MANUAL_TICKET_STATE", {"ticket_id": ticket_id, "from": ticket.state.value, "to": destination.value, "authority": authority.value, "report": details}, at=observed)
        self.tickets[ticket_id] = updated
        self._persist()
        return updated

    def comparison(self, ticket_id: str, report: Mapping[str, Any] | None = None) -> dict[str, Any]:
        ticket = self.tickets[ticket_id]
        if ticket.state not in {
            ManualTradeState.OPERATOR_REPORTED_PARTIALLY_FILLED,
            ManualTradeState.OPERATOR_REPORTED_FILLED,
            ManualTradeState.OPERATOR_CONFIRMED_PROTECTED,
            ManualTradeState.OPERATOR_REPORTED_CLOSED,
            ManualTradeState.STATE_UNCERTAIN,
        }:
            raise ValueError("planned-versus-actual comparison requires an operator-reported fill state")
        actual = dict(ticket.actual)
        if report is not None:
            expected = {
                "actual_contract", "actual_side", "actual_quantity", "actual_fill_price",
                "actual_stop", "actual_target", "actual_fees",
            }
            if set(report) != expected:
                raise ValueError("planned-versus-actual report fields are not exact")
            _reject_forbidden_data(report, name="planned-versus-actual report")
            actual_side_value = _bounded(report["actual_side"], "actual side", 8).upper()
            if actual_side_value not in {"BUY", "SELL"}:
                raise ValueError("actual side must be BUY or SELL")
            normalized: dict[str, Any] = {
                "actual_contract": _bounded(report["actual_contract"], "actual contract", 32).upper(),
                "actual_side": actual_side_value,
                "actual_quantity": _positive_int(report["actual_quantity"], "actual quantity"),
            }
            for name in ("actual_fill_price", "actual_stop", "actual_target", "actual_fees"):
                number = _decimal(report[name], name)
                if (name != "actual_fees" and number <= 0) or (name == "actual_fees" and number < 0):
                    raise ValueError(f"{name} is invalid")
                normalized[name] = str(number)
            actual.update(normalized)
            ticket = replace(ticket, actual=actual)
            self.tickets[ticket_id] = ticket
        alerts: list[str] = []
        actual_contract = str(actual.get("actual_contract", ticket.execution_contract)).upper()
        actual_side = str(actual.get("actual_side", ticket.side)).upper()
        actual_quantity = int(actual.get("actual_quantity", 0) or 0)
        fill_price = _decimal(actual.get("actual_fill_price", ticket.tick_aligned_entry), "actual fill price")
        actual_stop = _decimal(actual.get("actual_stop", ticket.protective_stop), "actual stop")
        actual_target = _decimal(actual.get("actual_target", ticket.target), "actual target")
        if actual_contract != ticket.execution_contract:
            alerts.append("WRONG_CONTRACT_MONTH_OR_SYMBOL")
        if actual_side != ticket.side:
            alerts.append("WRONG_SIDE")
        if actual_quantity > ticket.approved_quantity:
            alerts.append("QUANTITY_ABOVE_AUTHORIZED")
        if actual_quantity <= 0 and ticket.state not in {ManualTradeState.OPERATOR_REPORTED_REJECTED, ManualTradeState.OPERATOR_REPORTED_CANCELLED}:
            alerts.append("DELAYED_OR_UNKNOWN_FILL")
        if actual_stop != ticket.protective_stop:
            alerts.append("STOP_PRICE_MISMATCH")
        if actual_target != ticket.target:
            alerts.append("TARGET_MISMATCH")
        if (actual_side == "BUY" and not actual_stop < fill_price < actual_target) or (
            actual_side == "SELL" and not actual_target < fill_price < actual_stop
        ):
            alerts.append("ACTUAL_STOP_OR_TARGET_DIRECTION_INVALID")
        signed_ticks = (fill_price - ticket.tick_aligned_entry) / ticket.tick_size
        if ticket.side == "SELL":
            signed_ticks = -signed_ticks
        stop_distance = abs(fill_price - actual_stop)
        actual_risk = (stop_distance / ticket.tick_size * ticket.tick_value_usd + Decimal("5.00")) * actual_quantity
        if actual_risk > ticket.planned_stop_risk_usd:
            alerts.append("ACTUAL_RISK_ABOVE_AUTHORIZED_RISK")
        if ticket.projected_micro_equivalents - ticket.approved_quantity + actual_quantity > 30:
            alerts.append("AGGREGATE_EXPOSURE_VIOLATION")
        if ticket.state in {ManualTradeState.OPERATOR_REPORTED_FILLED, ManualTradeState.STATE_UNCERTAIN}:
            alerts.append("MISSING_OR_UNCONFIRMED_PROTECTIVE_STOP")
        actual_fees = _decimal(actual.get("actual_fees", 0), "actual fees")
        if actual_fees != ticket.provisional_fees_usd:
            alerts.append("FEE_MISMATCH_FROM_PROVISIONAL_ESTIMATE")
        if alerts:
            self.uncertain = self.uncertain or any(code in alerts for code in ("DELAYED_OR_UNKNOWN_FILL", "MISSING_OR_UNCONFIRMED_PROTECTIVE_STOP"))
        submission_delay_seconds: float | None = None
        submission_time = actual.get("actual_submission_time")
        fill_time = actual.get("actual_fill_time")
        if submission_time and fill_time:
            try:
                submitted_at = datetime.fromisoformat(str(submission_time).replace("Z", "+00:00"))
                filled_at = datetime.fromisoformat(str(fill_time).replace("Z", "+00:00"))
                _aware(submitted_at, "submission time")
                _aware(filled_at, "fill time")
                submission_delay_seconds = (filled_at - submitted_at).total_seconds()
                if submission_delay_seconds < 0:
                    alerts.append("CONTRADICTORY_OPERATOR_EVENTS")
                    self.uncertain = True
            except ValueError:
                alerts.append("CONTRADICTORY_OPERATOR_EVENTS")
                self.uncertain = True
        planned_reward = abs(ticket.target - ticket.tick_aligned_entry) / ticket.tick_size * ticket.tick_value_usd
        actual_reward = abs(actual_target - fill_price) / ticket.tick_size * ticket.tick_value_usd
        actual_stop_per_contract = (stop_distance / ticket.tick_size * ticket.tick_value_usd + Decimal("5.00"))
        exit_price = actual.get("actual_exit_price")
        realized_result: str | None = None
        if exit_price is not None and actual_quantity > 0:
            direction = Decimal("1") if actual_side == "BUY" else Decimal("-1")
            realized_result = str((_decimal(exit_price, "actual exit price") - fill_price) / ticket.tick_size * ticket.tick_value_usd * actual_quantity * direction - actual_fees)
        material_alerts = {
            "WRONG_CONTRACT_MONTH_OR_SYMBOL", "WRONG_SIDE", "QUANTITY_ABOVE_AUTHORIZED",
            "DELAYED_OR_UNKNOWN_FILL", "ACTUAL_RISK_ABOVE_AUTHORIZED_RISK",
            "AGGREGATE_EXPOSURE_VIOLATION", "MISSING_OR_UNCONFIRMED_PROTECTIVE_STOP",
            "CONTRADICTORY_OPERATOR_EVENTS", "ACTUAL_STOP_OR_TARGET_DIRECTION_INVALID",
        }
        if material_alerts.intersection(alerts):
            ticket = replace(ticket, state=ManualTradeState.STATE_UNCERTAIN, authority=ManualAuthority.OPERATOR_REPORTED, actual=actual)
            self.tickets[ticket_id] = ticket
            self.uncertain = True
            self.restart_stale = True
        comparison = {
            "ticket_id": ticket.ticket_id,
            "authority": ManualAuthority.OPERATOR_REPORTED.value,
            "requested_contract": ticket.execution_contract,
            "actual_contract": actual_contract,
            "requested_side": ticket.side,
            "actual_side": actual_side,
            "requested_quantity": ticket.approved_quantity,
            "actual_quantity": actual_quantity,
            "entry_slippage_ticks": str(signed_ticks),
            "entry_slippage_usd": str(signed_ticks * ticket.tick_value_usd * actual_quantity),
            "submission_to_fill_delay_seconds": submission_delay_seconds,
            "planned_stop_distance": str(abs(ticket.tick_aligned_entry - ticket.protective_stop)),
            "actual_stop_distance": str(stop_distance),
            "planned_stop_risk_usd": str(ticket.planned_stop_risk_usd),
            "actual_stop_risk_usd": str(actual_risk),
            "planned_target_distance": str(abs(ticket.target - ticket.tick_aligned_entry)),
            "actual_target_distance": str(abs(actual_target - fill_price)),
            "planned_reward_risk": str(planned_reward / ticket.risk_per_micro_usd) if ticket.risk_per_micro_usd else None,
            "actual_reward_risk": str(actual_reward / actual_stop_per_contract) if actual_stop_per_contract else None,
            "estimated_fees_usd": str(ticket.provisional_fees_usd),
            "actual_fees_usd": str(actual_fees),
            "projected_micro_equivalents": ticket.projected_micro_equivalents,
            "actual_micro_equivalents": actual_quantity,
            "realized_result_usd": realized_result,
            "alerts": list(dict.fromkeys(alerts)),
            "operator_reported": True,
            "resulting_state": ticket.state.value,
        }
        self.store.append("PLANNED_ACTUAL_COMPARISON", {"ticket_id": ticket_id, "alerts": comparison["alerts"]}, at=datetime.now(timezone.utc))
        self._persist()
        return comparison

    def copy_summary(self, ticket_id: str) -> str:
        ticket = self.tickets[ticket_id]
        direction = "LONG" if ticket.side == "BUY" else "SHORT"
        ready = ticket.state is ManualTradeState.READY_FOR_MANUAL_ENTRY and ticket.manual_assistant_readiness
        title = "MFF EVALUATION" if ticket.stage == "evaluation" else "MFF RAPID EOD SIM FUNDED"
        lines = [
            f"{title} - MANUAL ORDER PREPARATION", "",
            f"Authority: {ticket.authority.value} / OPERATOR ENTRY REQUIRED",
            f"Provider/profile/stage: {ticket.provider} / {ticket.profile_id} / {ticket.stage}",
            f"Execution capability: {ticket.capability.value}",
            f"Contract: {ticket.execution_contract}",
            f"Contract maturity: {ticket.contract_month_code} {ticket.contract_year}",
            f"Direction: {direction}",
            f"Quantity: {ticket.approved_quantity if ready else 0} micros",
            f"Order: {ticket.side} {ticket.order_type}", f"Entry: {ticket.tick_aligned_entry}",
            f"Protective stop: {ticket.protective_stop}", f"Profit target: {ticket.target}",
            f"Planned stop risk: ${ticket.planned_stop_risk_usd:.2f}",
            f"Provisional costs/slippage: ${(ticket.provisional_fees_usd + ticket.expected_slippage_usd):.2f}",
            f"Cost status/policy: {ticket.cost_status} / {ticket.cost_policy_id}",
            "Time in force: DAY", f"Ticket ID: {ticket.ticket_id}",
            f"Prepared at: {ticket.prepared_at.isoformat()} (timezone-aware)", "",
            "NO ORDER HAS BEEN TRANSMITTED BY FUTURESLIVECOCKPIT.",
            "Enter and verify this order manually in Tradovate." if ready else "BLOCKED PREVIEW - DO NOT ENTER OR TRANSMIT THIS ORDER.",
        ]
        if not ready:
            lines.extend(("Blockers:", *[f"- {code}" for code in ticket.blocker_reason_codes]))
        return "\n".join(lines)

    def status_payload(self) -> dict[str, Any]:
        snapshot = self.snapshot
        return {
            "capability": ExecutionCapability.MANUAL_ONLY.value,
            "manual_ticket_preview_available": True,
            "manual_assistant_readiness": False,
            "provider_api_readiness": False,
            "automatic_execution_authorized": False,
            "account_binding": "UNSET",
            "operator_snapshot_present": snapshot is not None,
            "operator_snapshot_stale": True if snapshot is None else not snapshot.current(
                now=datetime.now(timezone.utc), profile_id=snapshot.profile_id, stage=snapshot.stage,
                alias=snapshot.account_alias, restart_stale=self.restart_stale,
            ),
            "state_uncertain": self.uncertain or self.store.corrupt or any(
                ticket.state is ManualTradeState.STATE_UNCERTAIN for ticket in self.tickets.values()
            ),
            "authority": ManualAuthority.OPERATOR_REPORTED.value,
            "tickets": [_jsonable(asdict(ticket)) for ticket in self.tickets.values()],
        }
