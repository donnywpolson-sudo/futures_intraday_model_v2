"""Provider-neutral execution-adapter protocol and hard-disabled adapter."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .domain import BrokerSnapshot, OrderIntent
from .errors import ExecutionBlocked


@runtime_checkable
class ExecutionAdapter(Protocol):
    @property
    def provider_id(self) -> str: ...

    @property
    def connected(self) -> bool: ...

    def reconcile(self) -> BrokerSnapshot: ...

    def submit(self, intent: OrderIntent) -> object: ...

    def cancel_order(self, provider_order_id: int) -> object: ...

    def cancel_all_entries(self) -> object: ...

    def flatten_position(self, contract_id: int) -> object: ...

    def flatten_all(self) -> object: ...

    def close(self) -> None: ...


class DisabledExecutionAdapter:
    """Default adapter: no provider client exists and every operation is blocked."""

    provider_id = "NONE"
    connected = False

    @staticmethod
    def _blocked() -> None:
        raise ExecutionBlocked("execution adapter is disabled")

    def reconcile(self) -> BrokerSnapshot:
        self._blocked()

    def submit(self, intent: OrderIntent) -> object:
        del intent
        self._blocked()

    def cancel_order(self, provider_order_id: int) -> object:
        del provider_order_id
        self._blocked()

    def cancel_all_entries(self) -> object:
        self._blocked()

    def flatten_position(self, contract_id: int) -> object:
        del contract_id
        self._blocked()

    def flatten_all(self) -> object:
        self._blocked()

    def close(self) -> None:
        return None
