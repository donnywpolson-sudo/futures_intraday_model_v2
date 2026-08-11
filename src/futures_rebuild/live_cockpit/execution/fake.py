"""Deterministic local Tradovate transport and lifecycle simulator."""

from __future__ import annotations

from collections import deque
from dataclasses import replace
from datetime import datetime, timezone
from typing import Mapping

from .adapter import ExecutionAdapter
from .domain import (
    AccountSnapshot,
    BrokerSnapshot,
    EventOrigin,
    FillSnapshot,
    OrderIntent,
    OrderSide,
    OrderSnapshot,
    OrderStatus,
    OrderType,
    PositionSnapshot,
)
from .errors import ExecutionBlocked, UnknownBrokerState
from .tradovate_rest import TransportResponse


class FakeHttpTransport:
    def __init__(self, responses: list[TransportResponse | BaseException] | None = None) -> None:
        self.responses = deque(responses or [])
        self.requests: list[dict[str, object]] = []

    def request(self, *, method: str, url: str, headers: Mapping[str, str], json_body: Mapping[str, object] | None, timeout_seconds: float) -> TransportResponse:
        self.requests.append({
            "method": method,
            "url": url,
            "headers": {key: "<redacted>" if key.lower() == "authorization" else value for key, value in headers.items()},
            "json_body_keys": sorted(json_body) if json_body else [],
            "timeout_seconds": timeout_seconds,
        })
        if not self.responses:
            raise TimeoutError("fake response queue is empty")
        value = self.responses.popleft()
        if isinstance(value, BaseException):
            raise value
        return value


class FakeWebSocketTransport:
    def __init__(self, frames: list[str] | None = None) -> None:
        self.frames = deque(frames or [])
        self.sent: list[str] = []
        self.opened_url: str | None = None
        self.closed = False

    def open(self, *, url: str, timeout_seconds: float) -> None:
        del timeout_seconds
        self.opened_url = url
        self.closed = False

    def send(self, value: str) -> None:
        if self.opened_url is None or self.closed:
            raise RuntimeError("fake socket is closed")
        self.sent.append(value)

    def receive(self, *, timeout_seconds: float) -> str:
        del timeout_seconds
        if not self.frames:
            raise TimeoutError("fake socket frame queue is empty")
        return self.frames.popleft()

    def close(self) -> None:
        self.closed = True


class LocalExecutionSimulator(ExecutionAdapter):
    """No-network broker simulator with explicit synthetic origin."""

    provider_id = "LOCAL_EXECUTION_SIMULATOR"

    def __init__(self, *, account_id: int = 1, account_spec: str = "LOCAL-SIM", user_id: int = 1) -> None:
        self._connected = True
        self._account_id = account_id
        self._account_spec = account_spec
        self._user_id = user_id
        self._sequence = 0
        self._next_order_id = 1000
        self._next_fill_id = 5000
        self._orders: dict[int, OrderSnapshot] = {}
        self._fills: dict[int, FillSnapshot] = {}
        self._positions: dict[int, PositionSnapshot] = {}
        self._intent_orders: dict[str, int] = {}
        self._intents_by_order: dict[int, OrderIntent] = {}
        self._protective_orders: dict[int, tuple[int, ...]] = {}

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @property
    def connected(self) -> bool:
        return self._connected

    def reconcile(self) -> BrokerSnapshot:
        self._sequence += 1
        return BrokerSnapshot(
            account=AccountSnapshot(self._account_id, self._account_spec, self._user_id, self._now(), 0.0, 0.0, True, False, EventOrigin.LOCAL_SIMULATOR),
            positions=tuple(sorted(self._positions.values(), key=lambda value: value.provider_position_id)),
            orders=tuple(sorted(self._orders.values(), key=lambda value: value.provider_order_id)),
            fills=tuple(sorted(self._fills.values(), key=lambda value: value.provider_fill_id)),
            observed_at=self._now(), sequence=self._sequence, synchronized=True, origin=EventOrigin.LOCAL_SIMULATOR,
        )

    def submit(self, intent: OrderIntent) -> OrderSnapshot:
        if intent.intent_id in self._intent_orders:
            return self._orders[self._intent_orders[intent.intent_id]]
        if intent.stop_price is None:
            raise ExecutionBlocked("local simulator requires a protective stop")
        order_id = self._next_order_id
        self._next_order_id += 1
        order = OrderSnapshot(
            provider_order_id=order_id, account_id=self._account_id, contract_id=intent.contract_id,
            client_order_id=intent.intent_id, symbol=intent.execution_symbol, side=intent.side,
            order_type=intent.order_type, quantity=intent.requested_quantity, filled_quantity=0,
            status=OrderStatus.WORKING, price=intent.entry_price, stop_price=intent.stop_price,
            observed_at=self._now(), rejection_message=None, origin=EventOrigin.LOCAL_SIMULATOR,
        )
        self._orders[order_id] = order
        self._intent_orders[intent.intent_id] = order_id
        self._intents_by_order[order_id] = intent
        return order

    def _synchronize_protection(self, *, entry: OrderSnapshot, protected_quantity: int) -> None:
        intent = self._intents_by_order[entry.provider_order_id]
        child_ids = self._protective_orders.get(entry.provider_order_id, ())
        if child_ids:
            for child_id in child_ids:
                child = self._orders[child_id]
                self._orders[child_id] = replace(child, quantity=protected_quantity, observed_at=self._now())
            return
        protective_side = OrderSide.SELL if entry.side is OrderSide.BUY else OrderSide.BUY
        oco_id = self._next_order_id + 10_000 if intent.target_price is not None else None
        stop_id = self._next_order_id
        self._next_order_id += 1
        self._orders[stop_id] = OrderSnapshot(
            provider_order_id=stop_id, account_id=self._account_id, contract_id=entry.contract_id,
            client_order_id=f"{intent.intent_id}.stop", symbol=entry.symbol, side=protective_side,
            order_type=OrderType.STOP, quantity=protected_quantity, filled_quantity=0,
            status=OrderStatus.WORKING, price=None, stop_price=intent.stop_price,
            observed_at=self._now(), rejection_message=None, parent_order_id=entry.provider_order_id,
            oco_id=oco_id, origin=EventOrigin.LOCAL_SIMULATOR,
        )
        children = [stop_id]
        if intent.target_price is not None:
            target_id = self._next_order_id
            self._next_order_id += 1
            self._orders[target_id] = OrderSnapshot(
                provider_order_id=target_id, account_id=self._account_id, contract_id=entry.contract_id,
                client_order_id=f"{intent.intent_id}.target", symbol=entry.symbol, side=protective_side,
                order_type=OrderType.LIMIT, quantity=protected_quantity, filled_quantity=0,
                status=OrderStatus.WORKING, price=intent.target_price, stop_price=None,
                observed_at=self._now(), rejection_message=None, parent_order_id=entry.provider_order_id,
                oco_id=oco_id, origin=EventOrigin.LOCAL_SIMULATOR,
            )
            children.append(target_id)
        self._protective_orders[entry.provider_order_id] = tuple(children)

    def fill(self, *, provider_order_id: int, quantity: int, price: float) -> FillSnapshot:
        order = self._orders.get(provider_order_id)
        if order is None or order.status not in {OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED}:
            raise UnknownBrokerState("fake order cannot be filled")
        remaining = order.quantity - order.filled_quantity
        if quantity <= 0 or quantity > remaining:
            raise ValueError("fake fill quantity exceeds remaining order quantity")
        fill_id = self._next_fill_id
        self._next_fill_id += 1
        fill = FillSnapshot(fill_id, provider_order_id, self._account_id, order.contract_id, order.symbol, order.side, quantity, price, self._now(), EventOrigin.LOCAL_SIMULATOR)
        self._fills[fill_id] = fill
        new_filled = order.filled_quantity + quantity
        self._orders[provider_order_id] = replace(order, filled_quantity=new_filled, status=OrderStatus.FILLED if new_filled == order.quantity else OrderStatus.PARTIALLY_FILLED, observed_at=self._now())
        self._synchronize_protection(entry=order, protected_quantity=new_filled)
        signed = quantity if order.side is OrderSide.BUY else -quantity
        existing = self._positions.get(order.contract_id)
        net = signed + (existing.net_quantity if existing else 0)
        self._positions[order.contract_id] = PositionSnapshot(order.contract_id, self._account_id, order.contract_id, order.symbol, net, price, self._now(), EventOrigin.LOCAL_SIMULATOR)
        return fill

    def reject(self, *, provider_order_id: int, reason: str = "SYNTHETIC_REJECTION") -> OrderSnapshot:
        order = self._orders[provider_order_id]
        rejected = replace(order, status=OrderStatus.REJECTED, rejection_message=reason[:240], observed_at=self._now())
        self._orders[provider_order_id] = rejected
        return rejected

    def cancel_order(self, provider_order_id: int) -> OrderSnapshot:
        order = self._orders.get(provider_order_id)
        if order is None or order.status not in {OrderStatus.WORKING, OrderStatus.PARTIALLY_FILLED}:
            raise UnknownBrokerState("fake order cannot be canceled")
        canceled = replace(order, status=OrderStatus.CANCELED, observed_at=self._now())
        self._orders[provider_order_id] = canceled
        return canceled

    def cancel_all_entries(self) -> tuple[OrderSnapshot, ...]:
        return tuple(self.cancel_order(order_id) for order_id, order in tuple(self._orders.items()) if order.parent_order_id is None and order.status is OrderStatus.WORKING)

    def flatten_position(self, contract_id: int) -> PositionSnapshot:
        position = self._positions.get(contract_id)
        if position is None:
            raise UnknownBrokerState("fake position does not exist")
        flat = replace(position, net_quantity=0, observed_at=self._now())
        self._positions[contract_id] = flat
        for order_id, order in tuple(self._orders.items()):
            if order.contract_id == contract_id and order.parent_order_id is not None and order.status is OrderStatus.WORKING:
                self._orders[order_id] = replace(order, status=OrderStatus.CANCELED, observed_at=self._now())
        return flat

    def flatten_all(self) -> tuple[PositionSnapshot, ...]:
        return tuple(self.flatten_position(contract_id) for contract_id, position in tuple(self._positions.items()) if position.net_quantity)

    def close(self) -> None:
        self._connected = False
