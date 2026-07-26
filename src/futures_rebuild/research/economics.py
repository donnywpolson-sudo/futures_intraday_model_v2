"""Exact actual-contract tick economics and synthetic P&L rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import json
import re

import numpy as np

from .contracts import ResearchContractError, explicit_int, require_sha256
from .interval_identity import IntervalRole, VerifiedIntervalIdentity


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class FuturesReturnBasis(str, Enum):
    """Denominator convention; this is not a margin or capital return."""

    ENTRY_PRICE_NOTIONAL = "ENTRY_PRICE_NOTIONAL"


def _decimal(value: object, *, name: str, positive: bool = False) -> Decimal:
    if not isinstance(value, Decimal):
        raise ResearchContractError(f"{name} must be an explicit Decimal")
    if not value.is_finite() or (positive and value <= 0):
        raise ResearchContractError(f"{name} is non-finite or non-positive")
    return value


def _market_id(value: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[A-Z0-9][A-Z0-9._-]*", value) is None:
        raise ResearchContractError("market_id must be canonical uppercase ASCII")
    return value


@dataclass(frozen=True)
class EconomicsBinding:
    actual_contract_id: str
    economics_record_id: str
    tick_size: Decimal
    tick_value: Decimal
    point_value: Decimal
    currency: str

    def validate(self) -> None:
        require_sha256(self.actual_contract_id, name="actual_contract_id")
        require_sha256(self.economics_record_id, name="economics_record_id")
        tick_size = _decimal(self.tick_size, name="tick_size", positive=True)
        tick_value = _decimal(self.tick_value, name="tick_value", positive=True)
        point_value = _decimal(self.point_value, name="point_value", positive=True)
        if tick_size * point_value != tick_value:
            raise ResearchContractError("tick_size * point_value must equal tick_value")
        if not isinstance(self.currency, str) or re.fullmatch(r"[A-Z]{3}", self.currency) is None:
            raise ResearchContractError("currency must be three uppercase letters")


@dataclass(frozen=True)
class FuturesPnLRow:
    """One entry-to-exit row with per-contract fees and summed two-side slippage."""

    market_id: str
    direction: Direction
    session_ordinal: int
    actual_contract_id: str
    economics_record_id: str
    interval_identity_id: str
    execution_start_at: datetime
    execution_end_at: datetime
    entry_price: Decimal
    exit_price: Decimal
    entry_price_ticks: int
    exit_price_ticks: int
    quantity: int
    gross_pnl: Decimal
    commission_per_contract: Decimal
    exchange_fees_per_contract: Decimal
    total_commission: Decimal
    total_exchange_fees: Decimal
    round_trip_slippage_ticks_per_contract: int
    total_round_trip_slippage_cost: Decimal
    net_pnl: Decimal
    entry_price_notional: Decimal
    return_basis: FuturesReturnBasis
    net_return_on_entry_price_notional: Decimal
    row_id: str

    def validate(
        self,
        economics: EconomicsBinding,
        interval_identity: VerifiedIntervalIdentity,
    ) -> None:
        rebuilt = build_pnl_row(
            market_id=self.market_id,
            direction=self.direction,
            session_ordinal=self.session_ordinal,
            economics=economics,
            interval_identity=interval_identity,
            execution_start_at=self.execution_start_at,
            execution_end_at=self.execution_end_at,
            entry_price=self.entry_price,
            exit_price=self.exit_price,
            quantity=self.quantity,
            commission_per_contract=self.commission_per_contract,
            exchange_fees_per_contract=self.exchange_fees_per_contract,
            round_trip_slippage_ticks_per_contract=(
                self.round_trip_slippage_ticks_per_contract
            ),
        )
        if rebuilt != self:
            raise ResearchContractError("P&L row does not recompute exactly")


def _price_ticks(price: Decimal, tick_size: Decimal, *, name: str) -> int:
    checked = _decimal(price, name=name)
    try:
        ticks = checked / tick_size
        integral = ticks.to_integral_value()
    except (InvalidOperation, ZeroDivisionError) as error:
        raise ResearchContractError(f"{name} cannot be expressed in ticks") from error
    if ticks != integral:
        raise ResearchContractError(f"{name} is off the verified tick grid")
    return int(integral)


def _utc(value: datetime, *, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ResearchContractError(f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ResearchContractError(f"{name} must be UTC")
    return value.astimezone(timezone.utc)


def build_pnl_row(
    *,
    market_id: str,
    direction: Direction,
    session_ordinal: int,
    economics: EconomicsBinding,
    interval_identity: VerifiedIntervalIdentity,
    execution_start_at: datetime,
    execution_end_at: datetime,
    entry_price: Decimal,
    exit_price: Decimal,
    quantity: int,
    commission_per_contract: Decimal,
    exchange_fees_per_contract: Decimal,
    round_trip_slippage_ticks_per_contract: int,
) -> FuturesPnLRow:
    """Build exact P&L; slippage is entry-plus-exit ticks per contract."""

    economics.validate()
    if not isinstance(interval_identity, VerifiedIntervalIdentity):
        raise ResearchContractError("verified interval identity is required")
    interval_identity.validate()
    pnl_identity = interval_identity.identity_for(IntervalRole.PNL)
    if (
        pnl_identity.actual_contract_id != economics.actual_contract_id
        or pnl_identity.economics_record_id != economics.economics_record_id
    ):
        raise ResearchContractError("interval identity and economics are mismatched")
    execution_start = _utc(execution_start_at, name="execution_start_at")
    execution_end = _utc(execution_end_at, name="execution_end_at")
    pnl_window = interval_identity.window_for(IntervalRole.PNL)
    if (
        execution_start != pnl_window.start_at
        or execution_end != pnl_window.end_at
    ):
        raise ResearchContractError(
            "execution interval does not match verified P&L identity window"
        )
    market = _market_id(market_id)
    if not isinstance(direction, Direction):
        raise ResearchContractError("direction must be LONG or SHORT")
    session = explicit_int(session_ordinal, name="session_ordinal")
    size = explicit_int(quantity, name="quantity")
    round_trip_slippage = explicit_int(
        round_trip_slippage_ticks_per_contract,
        name="round_trip_slippage_ticks_per_contract",
    )
    if size <= 0 or round_trip_slippage < 0:
        raise ResearchContractError("quantity must be positive and slippage non-negative")
    checked_commission_per_contract = _decimal(
        commission_per_contract, name="commission_per_contract"
    )
    checked_fees_per_contract = _decimal(
        exchange_fees_per_contract, name="exchange_fees_per_contract"
    )
    if checked_commission_per_contract < 0 or checked_fees_per_contract < 0:
        raise ResearchContractError("costs cannot be negative")
    entry_ticks = _price_ticks(entry_price, economics.tick_size, name="entry_price")
    exit_ticks = _price_ticks(exit_price, economics.tick_size, name="exit_price")
    signed_move = exit_ticks - entry_ticks
    sign = 1 if direction is Direction.LONG else -1
    gross = Decimal(sign * signed_move * size) * economics.tick_value
    total_commission = checked_commission_per_contract * Decimal(size)
    total_exchange_fees = checked_fees_per_contract * Decimal(size)
    total_round_trip_slippage_cost = (
        Decimal(round_trip_slippage * size) * economics.tick_value
    )
    net = (
        gross
        - total_commission
        - total_exchange_fees
        - total_round_trip_slippage_cost
    )
    entry_price_notional = abs(entry_price * economics.point_value * Decimal(size))
    if entry_price_notional == 0:
        raise ResearchContractError("zero entry notional cannot define return")
    net_return_on_entry_price_notional = net / entry_price_notional
    core = {
        "actual_contract_id": economics.actual_contract_id,
        "commission_per_contract": str(checked_commission_per_contract),
        "direction": direction.value,
        "economics_record_id": economics.economics_record_id,
        "entry_price": str(entry_price),
        "exchange_fees_per_contract": str(checked_fees_per_contract),
        "execution_end_at": execution_end.isoformat(),
        "execution_start_at": execution_start.isoformat(),
        "exit_price": str(exit_price),
        "market_id": market,
        "interval_identity_id": interval_identity.binding_id,
        "quantity": size,
        "return_basis": FuturesReturnBasis.ENTRY_PRICE_NOTIONAL.value,
        "session_ordinal": session,
        "round_trip_slippage_ticks_per_contract": round_trip_slippage,
    }
    row_id = hashlib.sha256(
        json.dumps(core, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    return FuturesPnLRow(
        market,
        direction,
        session,
        economics.actual_contract_id,
        economics.economics_record_id,
        interval_identity.binding_id,
        execution_start,
        execution_end,
        entry_price,
        exit_price,
        entry_ticks,
        exit_ticks,
        size,
        gross,
        checked_commission_per_contract,
        checked_fees_per_contract,
        total_commission,
        total_exchange_fees,
        round_trip_slippage,
        total_round_trip_slippage_cost,
        net,
        entry_price_notional,
        FuturesReturnBasis.ENTRY_PRICE_NOTIONAL,
        net_return_on_entry_price_notional,
        row_id,
    )


def pnl_rows_to_float64(
    rows: tuple[FuturesPnLRow, ...],
    *,
    economics_by_id: dict[str, EconomicsBinding],
    interval_identity_by_id: dict[str, VerifiedIntervalIdentity],
    market_id: str,
    direction: Direction,
) -> np.ndarray:
    if not rows:
        raise ResearchContractError("at least one P&L row is required")
    market = _market_id(market_id)
    if not isinstance(direction, Direction):
        raise ResearchContractError("direction must be LONG or SHORT")
    result = np.empty(len(rows), dtype=np.float64)
    prior_session: int | None = None
    for index, row in enumerate(rows):
        if row.market_id != market or row.direction is not direction:
            raise ResearchContractError("market/direction sleeves cannot be pooled")
        try:
            economics = economics_by_id[row.economics_record_id]
        except KeyError as error:
            raise ResearchContractError("P&L row lacks verified economics binding") from error
        try:
            interval_identity = interval_identity_by_id[row.interval_identity_id]
        except KeyError as error:
            raise ResearchContractError("P&L row lacks interval identity binding") from error
        row.validate(economics, interval_identity)
        if prior_session is not None and row.session_ordinal <= prior_session:
            raise ResearchContractError("P&L rows must have strictly increasing sessions")
        prior_session = row.session_ordinal
        result[index] = float(row.net_return_on_entry_price_notional)
    if not bool(np.all(np.isfinite(result))):
        raise ResearchContractError("P&L return conversion is non-finite")
    return result
