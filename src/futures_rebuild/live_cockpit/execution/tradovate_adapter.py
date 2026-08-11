"""Composed Tradovate adapter; never constructed by ordinary cockpit startup."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Mapping

from .adapter import ExecutionAdapter
from .domain import AccountBinding, BrokerSnapshot, IntentSource, OrderIntent, OrderSide, OrderType
from .errors import ExecutionBlocked
from .reconciliation import Reconciler, broker_snapshot_from_entities
from .tradovate_auth import AccessToken, TokenManager
from .tradovate_rest import TradovateRestClient
from .tradovate_websocket import TradovateUserSync


class TradovateAdapter(ExecutionAdapter):
    provider_id = "tradovate"

    def __init__(
        self,
        *,
        binding: AccountBinding,
        rest: TradovateRestClient,
        user_sync: TradovateUserSync,
        token_manager: TokenManager,
        contract_symbols: Mapping[int, str],
    ) -> None:
        self.binding = binding
        self.rest = rest
        self.user_sync = user_sync
        self.token_manager = token_manager
        self.contract_symbols = dict(contract_symbols)
        self.reconciler = Reconciler(binding=binding)
        self._connected = False
        self._last_snapshot: BrokerSnapshot | None = None

    @property
    def connected(self) -> bool:
        return self._connected

    def connect(self, token: AccessToken) -> None:
        if not self.rest.authority.read_only:
            raise ExecutionBlocked("Tradovate read-only connection is not authorized")
        self.token_manager.set(token)
        self.user_sync.open(token=token, user_id=self.binding.user_id, account_id=self.binding.account_id)
        self._connected = True

    def reconnect(self, token: AccessToken) -> BrokerSnapshot:
        """Reconnect user sync, then rebuild broker state before any action."""

        self.user_sync.close()
        self._connected = False
        self._last_snapshot = None
        self.connect(token)
        return self.reconcile()

    def reconcile(self) -> BrokerSnapshot:
        token = self.token_manager.current()
        accounts = self.rest.accounts(token)
        positions = self.rest.positions(token, account_id=self.binding.account_id)
        orders = self.rest.orders(token)
        fills = self.rest.fills(token)
        snapshot = broker_snapshot_from_entities(
            binding=self.binding,
            accounts=accounts,
            positions=positions,
            orders=orders,
            fills=fills,
            contract_symbols=self.contract_symbols,
            observed_at=datetime.now(timezone.utc),
            sequence=(self._last_snapshot.sequence + 1 if self._last_snapshot else 1),
        )
        self.reconciler.apply(snapshot)
        self._last_snapshot = snapshot
        return snapshot

    @staticmethod
    def _action(side: OrderSide) -> str:
        return "Buy" if side is OrderSide.BUY else "Sell"

    @staticmethod
    def _order_type(value: OrderType) -> str:
        return {
            OrderType.MARKET: "Market",
            OrderType.LIMIT: "Limit",
            OrderType.STOP: "Stop",
            OrderType.STOP_LIMIT: "StopLimit",
        }[value]

    @staticmethod
    def _time_in_force(value: str) -> str:
        return {"DAY": "Day", "GTC": "GTC", "GTD": "GTD", "IOC": "IOC", "FOK": "FOK"}.get(
            value.upper(), value
        )

    def _entry_body(self, intent: OrderIntent) -> dict[str, object]:
        if intent.account_binding_id != self.binding.binding_id or intent.account_binding_hash != self.binding.binding_hash:
            raise ExecutionBlocked("order intent does not match the exact account binding")
        if intent.source is IntentSource.MANUAL:
            raise ExecutionBlocked("manual custom-app isAutomated treatment is unconfirmed")
        body: dict[str, object] = {
            "accountSpec": self.binding.account_spec,
            "accountId": self.binding.account_id,
            "clOrdId": intent.intent_id,
            "action": self._action(intent.side),
            "symbol": intent.execution_symbol,
            "orderQty": intent.requested_quantity,
            "orderType": self._order_type(intent.order_type),
            "timeInForce": self._time_in_force(intent.time_in_force),
            "isAutomated": True,
        }
        if intent.entry_price is not None:
            body["price"] = intent.entry_price
        return body

    def submit(self, intent: OrderIntent) -> Mapping[str, object]:
        token = self.token_manager.current()
        body = self._entry_body(intent)
        protective_action = "Sell" if intent.side is OrderSide.BUY else "Buy"
        body["bracket1"] = {"action": protective_action, "orderType": "Stop", "stopPrice": intent.stop_price, "timeInForce": "GTC"}
        if intent.target_price is not None:
            body["bracket2"] = {"action": protective_action, "orderType": "Limit", "price": intent.target_price, "timeInForce": "GTC"}
        return self.rest.place_oso(token, body)

    def cancel_order(self, provider_order_id: int) -> Mapping[str, object]:
        return self.rest.cancel_order(self.token_manager.current(), order_id=provider_order_id)

    def cancel_all_entries(self) -> tuple[Mapping[str, object], ...]:
        if self._last_snapshot is None:
            raise ExecutionBlocked("broker state is not reconciled")
        results = []
        for order in self._last_snapshot.orders:
            if order.parent_order_id is None and order.status.value in {"PENDING_PROVIDER", "WORKING", "PARTIALLY_FILLED"}:
                results.append(self.cancel_order(order.provider_order_id))
        return tuple(results)

    def flatten_position(self, contract_id: int) -> Mapping[str, object]:
        return self.rest.liquidate_position(self.token_manager.current(), account_id=self.binding.account_id, contract_id=contract_id)

    def flatten_all(self) -> tuple[Mapping[str, object], ...]:
        if self._last_snapshot is None:
            raise ExecutionBlocked("broker state is not reconciled")
        return tuple(self.flatten_position(position.contract_id) for position in self._last_snapshot.positions if position.net_quantity)

    def close(self) -> None:
        self.user_sync.close()
        self.token_manager.clear()
        self._connected = False
