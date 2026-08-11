"""Broker-authoritative reconstruction and duplicate/out-of-order handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping, Sequence

from .domain import (
    AccountBinding,
    AccountSnapshot,
    BrokerSnapshot,
    EventOrigin,
    FillSnapshot,
    OrderSide,
    OrderSnapshot,
    OrderStatus,
    OrderType,
    PositionSnapshot,
)
from .errors import UnknownBrokerState


STATUS_MAP = {
    "PendingSubmit": OrderStatus.PENDING_PROVIDER,
    "Working": OrderStatus.WORKING,
    "PartiallyFilled": OrderStatus.PARTIALLY_FILLED,
    "Filled": OrderStatus.FILLED,
    "Canceled": OrderStatus.CANCELED,
    "Rejected": OrderStatus.REJECTED,
}
ORDER_TYPE_MAP = {
    "Market": OrderType.MARKET,
    "Limit": OrderType.LIMIT,
    "Stop": OrderType.STOP,
    "StopLimit": OrderType.STOP_LIMIT,
}


def _integer(
    value: object, *, name: str, positive: bool = True, allow_signed: bool = False
) -> int:
    if isinstance(value, bool):
        raise UnknownBrokerState(f"{name} is invalid")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise UnknownBrokerState(f"{name} is invalid") from exc
    if not allow_signed and ((positive and result <= 0) or (not positive and result < 0)):
        raise UnknownBrokerState(f"{name} is invalid")
    return result


def _timestamp(value: object, *, fallback: datetime) -> datetime:
    if value is None:
        return fallback
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise UnknownBrokerState("broker timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise UnknownBrokerState("broker timestamp is not timezone-aware")
    return parsed


def _exact_account(binding: AccountBinding, values: Sequence[Mapping[str, object]]) -> Mapping[str, object]:
    matches = [
        value for value in values
        if value.get("id") == binding.account_id
        and value.get("name") == binding.account_spec
        and value.get("userId") == binding.user_id
    ]
    if len(matches) != 1:
        raise UnknownBrokerState("exact bound Tradovate account was not found uniquely")
    return matches[0]


def broker_snapshot_from_entities(
    *,
    binding: AccountBinding,
    accounts: Sequence[Mapping[str, object]],
    positions: Sequence[Mapping[str, object]],
    orders: Sequence[Mapping[str, object]],
    fills: Sequence[Mapping[str, object]],
    contract_symbols: Mapping[int, str],
    observed_at: datetime | None = None,
    sequence: int = 0,
    origin: EventOrigin = EventOrigin.PROVIDER_BACKED,
) -> BrokerSnapshot:
    now = datetime.now(timezone.utc) if observed_at is None else observed_at
    account = _exact_account(binding, accounts)
    account_snapshot = AccountSnapshot(
        account_id=binding.account_id,
        account_spec=binding.account_spec,
        user_id=binding.user_id,
        observed_at=_timestamp(account.get("timestamp"), fallback=now),
        cash_balance=float(account["cashBalance"]) if account.get("cashBalance") is not None else None,
        equity=float(account["equity"]) if account.get("equity") is not None else None,
        active=account.get("active") is True,
        read_only=account.get("readonly") is True,
        origin=origin,
    )

    def contract_symbol(contract_id: int) -> str:
        value = contract_symbols.get(contract_id)
        if value is None:
            raise UnknownBrokerState("broker entity has an unresolved contract")
        return value

    position_values: list[PositionSnapshot] = []
    seen_positions: set[int] = set()
    for value in positions:
        if value.get("accountId") != binding.account_id:
            continue
        entity_id = _integer(value.get("id"), name="position id")
        if entity_id in seen_positions:
            raise UnknownBrokerState("duplicate provider position identifier")
        seen_positions.add(entity_id)
        contract_id = _integer(value.get("contractId"), name="position contract id")
        position_values.append(PositionSnapshot(
            provider_position_id=entity_id,
            account_id=binding.account_id,
            contract_id=contract_id,
            symbol=contract_symbol(contract_id),
            net_quantity=_integer(
                value.get("netPos", 0),
                name="net position",
                positive=False,
                allow_signed=True,
            ),
            net_price=float(value["netPrice"]) if value.get("netPrice") is not None else None,
            observed_at=_timestamp(value.get("timestamp"), fallback=now),
            origin=origin,
        ))

    order_values: list[OrderSnapshot] = []
    seen_orders: set[int] = set()
    for value in orders:
        if value.get("accountId") != binding.account_id:
            continue
        entity_id = _integer(value.get("id"), name="order id")
        if entity_id in seen_orders:
            raise UnknownBrokerState("duplicate provider order identifier")
        seen_orders.add(entity_id)
        contract_id = _integer(value.get("contractId"), name="order contract id")
        quantity = _integer(value.get("orderQty"), name="order quantity")
        filled = _integer(value.get("filledQty", 0), name="filled quantity", positive=False)
        status = STATUS_MAP.get(str(value.get("ordStatus")), OrderStatus.UNKNOWN)
        order_type = ORDER_TYPE_MAP.get(str(value.get("orderType")))
        action = str(value.get("action"))
        if order_type is None or action not in {"Buy", "Sell"}:
            raise UnknownBrokerState("provider order type or side is unknown")
        rejection = value.get("failureText")
        order_values.append(OrderSnapshot(
            provider_order_id=entity_id,
            account_id=binding.account_id,
            contract_id=contract_id,
            client_order_id=str(value["clOrdId"]) if value.get("clOrdId") else None,
            symbol=contract_symbol(contract_id),
            side=OrderSide.BUY if action == "Buy" else OrderSide.SELL,
            order_type=order_type,
            quantity=quantity,
            filled_quantity=filled,
            status=status,
            price=float(value["price"]) if value.get("price") is not None else None,
            stop_price=float(value["stopPrice"]) if value.get("stopPrice") is not None else None,
            observed_at=_timestamp(value.get("timestamp"), fallback=now),
            rejection_message=str(rejection)[:240] if rejection else None,
            parent_order_id=_integer(value["parentId"], name="parent order id") if value.get("parentId") else None,
            oco_id=_integer(value["ocoId"], name="oco id") if value.get("ocoId") else None,
            origin=origin,
        ))

    fill_values: list[FillSnapshot] = []
    seen_fills: set[int] = set()
    for value in fills:
        order_id = _integer(value.get("orderId"), name="fill order id")
        if order_id not in seen_orders:
            continue
        entity_id = _integer(value.get("id"), name="fill id")
        if entity_id in seen_fills:
            raise UnknownBrokerState("duplicate provider fill identifier")
        seen_fills.add(entity_id)
        contract_id = _integer(value.get("contractId"), name="fill contract id")
        action = str(value.get("action"))
        if action not in {"Buy", "Sell"}:
            raise UnknownBrokerState("provider fill side is unknown")
        fill_values.append(FillSnapshot(
            provider_fill_id=entity_id,
            provider_order_id=order_id,
            account_id=binding.account_id,
            contract_id=contract_id,
            symbol=contract_symbol(contract_id),
            side=OrderSide.BUY if action == "Buy" else OrderSide.SELL,
            quantity=_integer(value.get("qty"), name="fill quantity"),
            price=float(value["price"]),
            observed_at=_timestamp(value.get("timestamp"), fallback=now),
            origin=origin,
        ))
    return BrokerSnapshot(
        account=account_snapshot,
        positions=tuple(sorted(position_values, key=lambda item: item.provider_position_id)),
        orders=tuple(sorted(order_values, key=lambda item: item.provider_order_id)),
        fills=tuple(sorted(fill_values, key=lambda item: item.provider_fill_id)),
        observed_at=now,
        sequence=sequence,
        synchronized=True,
        origin=origin,
    )


@dataclass(frozen=True)
class ReconciliationResult:
    status: str
    snapshot: BrokerSnapshot
    blockers: tuple[str, ...]
    orphan_order_ids: tuple[int, ...]


class Reconciler:
    def __init__(self, *, binding: AccountBinding) -> None:
        self.binding = binding
        self._snapshot: BrokerSnapshot | None = None

    def apply(self, snapshot: BrokerSnapshot, *, known_provider_order_ids: set[int] | None = None) -> ReconciliationResult:
        if snapshot.account is None or snapshot.account.account_id != self.binding.account_id:
            raise UnknownBrokerState("broker snapshot does not match the exact account binding")
        if self._snapshot is not None:
            if snapshot.sequence < self._snapshot.sequence or snapshot.observed_at < self._snapshot.observed_at:
                return ReconciliationResult("IGNORED_OUT_OF_ORDER", self._snapshot, (), ())
            if snapshot.sequence == self._snapshot.sequence:
                if snapshot == self._snapshot:
                    return ReconciliationResult("IGNORED_DUPLICATE", self._snapshot, (), ())
                raise UnknownBrokerState("same broker sequence contained conflicting state")
        blockers: list[str] = []
        if not snapshot.synchronized:
            blockers.append("BROKER_SYNCHRONIZATION_INCOMPLETE")
        if any(order.status is OrderStatus.UNKNOWN for order in snapshot.orders):
            blockers.append("UNKNOWN_PROVIDER_ORDER_STATE")
        known = set() if known_provider_order_ids is None else known_provider_order_ids
        orphan_ids = tuple(
            order.provider_order_id for order in snapshot.orders
            if order.status in {OrderStatus.PENDING_PROVIDER, OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED}
            and order.provider_order_id not in known
        )
        if orphan_ids:
            blockers.append("ORPHAN_PROVIDER_ORDER")
        self._snapshot = snapshot
        return ReconciliationResult("RECONCILED" if not blockers else "BLOCKED", snapshot, tuple(blockers), orphan_ids)
