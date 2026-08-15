"""Typed account, order, fill, position, and execution-state models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
import math
import re
from typing import Any, Mapping


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$")
SYMBOL = re.compile(r"^[A-Z0-9]{1,16}$")


class ExecutionMode(str, Enum):
    OBSERVATION_ONLY = "OBSERVATION_ONLY"
    MFF_MANUAL_ASSISTANT = "MFF_MANUAL_ASSISTANT"
    TRADOVATE_READ_ONLY = "TRADOVATE_READ_ONLY"
    LOCAL_EXECUTION_SIMULATOR = "LOCAL_EXECUTION_SIMULATOR"
    MFF_TRADOVATE_SIM_FUNDED = "MFF_TRADOVATE_SIM_FUNDED"
    MFF_TRADOVATE_LIVE = "MFF_TRADOVATE_LIVE"


class EventOrigin(str, Enum):
    LOCAL_SIMULATOR = "LOCAL_SIMULATOR"
    PROVIDER_BACKED = "PROVIDER_BACKED"
    LOCAL_CONFIGURATION = "LOCAL_CONFIGURATION"


class IntentSource(str, Enum):
    MANUAL = "MANUAL"
    MODEL_PROPOSAL = "MODEL_PROPOSAL"
    EMERGENCY_ACTION = "EMERGENCY_ACTION"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class OrderStatus(str, Enum):
    PENDING_LOCAL = "PENDING_LOCAL"
    PENDING_PROVIDER = "PENDING_PROVIDER"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


def aware(value: datetime, *, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


def identifier(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be a bounded identifier")
    return value


def symbol(value: object, *, name: str = "symbol") -> str:
    if not isinstance(value, str) or not SYMBOL.fullmatch(value):
        raise ValueError(f"{name} must be a bounded uppercase symbol")
    return value


def positive_int(value: object, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def finite(value: object, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be finite")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number


def optional_finite(value: object, *, name: str) -> float | None:
    return None if value is None else finite(value, name=name)


@dataclass(frozen=True)
class AccountBinding:
    binding_id: str
    provider_id: str
    platform_id: str
    account_stage: str
    environment: str
    account_id: int
    account_spec: str
    user_id: int
    profile_id: str
    profile_hash: str
    connection_id: str
    connection_hash: str
    instrument_mapping_id: str
    instrument_mapping_hash: str
    cost_profile_id: str
    cost_profile_hash: str
    created_at: datetime
    evidence_reference: str
    binding_hash: str

    def __post_init__(self) -> None:
        for name in (
            "binding_id", "provider_id", "platform_id", "profile_id", "connection_id",
            "instrument_mapping_id", "cost_profile_id",
        ):
            identifier(getattr(self, name), name=name)
        if self.account_stage not in {"evaluation", "sim_funded", "live"}:
            raise ValueError("account_stage is invalid")
        if self.environment not in {"demo", "live"}:
            raise ValueError("environment must be demo or live")
        positive_int(self.account_id, name="account_id")
        positive_int(self.user_id, name="user_id")
        if not self.account_spec or len(self.account_spec) > 64:
            raise ValueError("account_spec must be bounded")
        for name in ("profile_hash", "connection_hash", "instrument_mapping_hash", "cost_profile_hash", "binding_hash"):
            value = getattr(self, name)
            if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
                raise ValueError(f"{name} must be a sha256")
        aware(self.created_at, name="created_at")
        if not self.evidence_reference or len(self.evidence_reference) > 240:
            raise ValueError("evidence_reference must be bounded")


@dataclass(frozen=True)
class AccountSnapshot:
    account_id: int
    account_spec: str
    user_id: int
    observed_at: datetime
    cash_balance: float | None
    equity: float | None
    active: bool
    read_only: bool
    origin: EventOrigin

    def __post_init__(self) -> None:
        positive_int(self.account_id, name="account_id")
        positive_int(self.user_id, name="user_id")
        aware(self.observed_at, name="observed_at")
        optional_finite(self.cash_balance, name="cash_balance")
        optional_finite(self.equity, name="equity")
        if not self.account_spec or len(self.account_spec) > 64:
            raise ValueError("account_spec must be bounded")


@dataclass(frozen=True)
class PositionSnapshot:
    provider_position_id: int
    account_id: int
    contract_id: int
    symbol: str
    net_quantity: int
    net_price: float | None
    observed_at: datetime
    origin: EventOrigin

    def __post_init__(self) -> None:
        positive_int(self.provider_position_id, name="provider_position_id")
        positive_int(self.account_id, name="account_id")
        positive_int(self.contract_id, name="contract_id")
        symbol(self.symbol)
        if isinstance(self.net_quantity, bool) or not isinstance(self.net_quantity, int):
            raise ValueError("net_quantity must be an integer")
        optional_finite(self.net_price, name="net_price")
        aware(self.observed_at, name="observed_at")


@dataclass(frozen=True)
class OrderSnapshot:
    provider_order_id: int
    account_id: int
    contract_id: int
    client_order_id: str | None
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: int
    filled_quantity: int
    status: OrderStatus
    price: float | None
    stop_price: float | None
    observed_at: datetime
    rejection_message: str | None
    parent_order_id: int | None = None
    oco_id: int | None = None
    origin: EventOrigin = EventOrigin.LOCAL_SIMULATOR

    def __post_init__(self) -> None:
        positive_int(self.provider_order_id, name="provider_order_id")
        positive_int(self.account_id, name="account_id")
        positive_int(self.contract_id, name="contract_id")
        symbol(self.symbol)
        positive_int(self.quantity, name="quantity")
        if isinstance(self.filled_quantity, bool) or not isinstance(self.filled_quantity, int) or not 0 <= self.filled_quantity <= self.quantity:
            raise ValueError("filled_quantity is invalid")
        optional_finite(self.price, name="price")
        optional_finite(self.stop_price, name="stop_price")
        aware(self.observed_at, name="observed_at")
        if self.client_order_id is not None:
            identifier(self.client_order_id, name="client_order_id")
        if self.rejection_message is not None and len(self.rejection_message) > 240:
            raise ValueError("rejection_message must be bounded")


@dataclass(frozen=True)
class FillSnapshot:
    provider_fill_id: int
    provider_order_id: int
    account_id: int
    contract_id: int
    symbol: str
    side: OrderSide
    quantity: int
    price: float
    observed_at: datetime
    origin: EventOrigin

    def __post_init__(self) -> None:
        for name in ("provider_fill_id", "provider_order_id", "account_id", "contract_id", "quantity"):
            positive_int(getattr(self, name), name=name)
        symbol(self.symbol)
        finite(self.price, name="price")
        aware(self.observed_at, name="observed_at")


@dataclass(frozen=True)
class OrderIntent:
    intent_id: str
    created_at: datetime
    source: IntentSource
    account_binding_id: str
    account_binding_hash: str
    profile_id: str
    profile_hash: str
    account_stage: str
    signal_instrument: str
    execution_symbol: str
    contract_id: int
    underlying_risk_group: str
    side: OrderSide
    order_type: OrderType
    requested_quantity: int
    entry_price: float | None
    stop_price: float
    target_price: float | None
    time_in_force: str
    market_data_at: datetime
    broker_state_at: datetime
    calendar_hash: str
    news_hash: str
    price_limit_hash: str
    strategy_policy_id: str
    strategy_policy_hash: str
    cost_profile_id: str
    cost_profile_hash: str
    model_identity: str | None = None
    audit_reasons: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        for name in ("intent_id", "account_binding_id", "profile_id", "underlying_risk_group", "strategy_policy_id", "cost_profile_id"):
            identifier(getattr(self, name), name=name)
        if self.account_stage not in {"evaluation", "sim_funded", "live"}:
            raise ValueError("account_stage is invalid")
        symbol(self.signal_instrument, name="signal_instrument")
        symbol(self.execution_symbol, name="execution_symbol")
        positive_int(self.contract_id, name="contract_id")
        positive_int(self.requested_quantity, name="requested_quantity")
        optional_finite(self.entry_price, name="entry_price")
        finite(self.stop_price, name="stop_price")
        optional_finite(self.target_price, name="target_price")
        if self.time_in_force not in {"DAY", "GTC", "GTD", "IOC", "FOK"}:
            raise ValueError("time_in_force is invalid")
        for name in ("created_at", "market_data_at", "broker_state_at"):
            aware(getattr(self, name), name=name)
        for name in ("account_binding_hash", "profile_hash", "calendar_hash", "news_hash", "price_limit_hash", "strategy_policy_hash", "cost_profile_hash"):
            if not re.fullmatch(r"[0-9a-f]{64}", str(getattr(self, name))):
                raise ValueError(f"{name} must be a sha256")
        if len(self.audit_reasons) > 64 or any(not value or len(value) > 120 for value in self.audit_reasons):
            raise ValueError("audit_reasons are invalid")


@dataclass(frozen=True)
class BrokerSnapshot:
    account: AccountSnapshot | None
    positions: tuple[PositionSnapshot, ...]
    orders: tuple[OrderSnapshot, ...]
    fills: tuple[FillSnapshot, ...]
    observed_at: datetime
    sequence: int
    synchronized: bool
    origin: EventOrigin

    def __post_init__(self) -> None:
        aware(self.observed_at, name="observed_at")
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 0:
            raise ValueError("sequence must be a nonnegative integer")


def public_payload(value: object) -> dict[str, Any]:
    """Serialize a domain value for local protocol payloads without secrets."""

    if not hasattr(value, "__dataclass_fields__"):
        raise TypeError("public_payload requires a dataclass")

    def convert(item: object) -> Any:
        if isinstance(item, datetime):
            return item.isoformat()
        if isinstance(item, Enum):
            return item.value
        if isinstance(item, Mapping):
            return {str(key): convert(nested) for key, nested in item.items()}
        if isinstance(item, (list, tuple)):
            return [convert(nested) for nested in item]
        return item

    return convert(asdict(value))


def exact_keys(value: Mapping[str, Any], expected: set[str], *, name: str) -> None:
    if set(value) != expected:
        raise ValueError(f"{name} fields are not exact")
