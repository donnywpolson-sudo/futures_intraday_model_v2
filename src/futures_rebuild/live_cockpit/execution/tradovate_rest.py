"""Bounded, transport-injected Tradovate REST operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol
from urllib.parse import urlencode

from .errors import ExecutionBlocked, TransportError, UnknownBrokerState
from .tradovate_auth import AccessToken, TradovateAuthMaterial, token_from_response


@dataclass(frozen=True)
class ServiceRoots:
    environment: str
    rest: str
    websocket: str

    def __post_init__(self) -> None:
        exact = {
            "demo": ("https://demo.tradovateapi.com/v1", "wss://demo.tradovateapi.com/v1/websocket"),
            "live": ("https://live.tradovateapi.com/v1", "wss://live.tradovateapi.com/v1/websocket"),
        }
        if self.environment not in exact or (self.rest, self.websocket) != exact[self.environment]:
            raise ValueError("Tradovate REST and WebSocket roots must be an exact matching environment")


@dataclass(frozen=True)
class TransportResponse:
    status: int
    body: object


class HttpTransport(Protocol):
    def request(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        json_body: Mapping[str, object] | None,
        timeout_seconds: float,
    ) -> TransportResponse: ...


@dataclass(frozen=True)
class OperationAuthority:
    read_only: bool
    order_change: bool
    local_simulator: bool
    receipt_id: str | None

    def __post_init__(self) -> None:
        if self.order_change and not (self.read_only or self.local_simulator):
            raise ValueError("order authority requires read or local-simulator authority")
        if not self.local_simulator and (self.read_only or self.order_change) and not self.receipt_id:
            raise ValueError("provider authority requires a receipt ID")


class TradovateRestClient:
    SAFE_RETRY_STATUSES = frozenset({423, 429, 500, 502, 503, 504})

    def __init__(
        self,
        *,
        roots: ServiceRoots,
        transport: HttpTransport,
        authority: OperationAuthority,
        timeout_seconds: float = 10.0,
        safe_read_attempts: int = 2,
    ) -> None:
        if timeout_seconds <= 0 or timeout_seconds > 30:
            raise ValueError("REST timeout must be within 30 seconds")
        if safe_read_attempts not in {1, 2}:
            raise ValueError("safe read attempts must be one or two")
        self.roots = roots
        self.transport = transport
        self.authority = authority
        self.timeout_seconds = timeout_seconds
        self.safe_read_attempts = safe_read_attempts

    @staticmethod
    def _headers(token: AccessToken | None) -> dict[str, str]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if token is not None:
            headers["Authorization"] = f"Bearer {token.value}"
        return headers

    @staticmethod
    def _body(response: TransportResponse) -> Any:
        if response.status in {401, 403}:
            raise ExecutionBlocked("Tradovate authentication or permission was denied")
        if response.status in {423, 429}:
            raise TransportError("Tradovate request was rate-limited")
        if response.status >= 500:
            raise TransportError("Tradovate service returned a server error")
        if response.status < 200 or response.status >= 300:
            raise TransportError(f"Tradovate request failed with HTTP {response.status}")
        body = response.body
        if isinstance(body, Mapping) and (body.get("errorText") or body.get("failureText") or body.get("failureReason")):
            raise ExecutionBlocked("Tradovate rejected the requested operation")
        return body

    def _request(
        self,
        *,
        method: str,
        path: str,
        token: AccessToken | None,
        body: Mapping[str, object] | None = None,
        query: Mapping[str, object] | None = None,
        safe_retry: bool,
    ) -> Any:
        suffix = f"?{urlencode(query)}" if query else ""
        url = f"{self.roots.rest}{path}{suffix}"
        attempts = self.safe_read_attempts if safe_retry else 1
        last: TransportResponse | None = None
        for _ in range(attempts):
            last = self.transport.request(
                method=method,
                url=url,
                headers=self._headers(token),
                json_body=body,
                timeout_seconds=self.timeout_seconds,
            )
            if last.status not in self.SAFE_RETRY_STATUSES:
                break
        assert last is not None
        return self._body(last)

    def acquire_token(self, material: TradovateAuthMaterial) -> AccessToken:
        if not self.authority.read_only and not self.authority.local_simulator:
            raise ExecutionBlocked("provider authentication is not authorized")
        value = self._request(method="POST", path="/auth/accesstokenrequest", token=None, body=material.request_body(), safe_retry=False)
        if not isinstance(value, Mapping):
            raise TransportError("Tradovate token response was invalid")
        return token_from_response(value)

    def renew_token(self, token: AccessToken) -> AccessToken:
        if not self.authority.read_only and not self.authority.local_simulator:
            raise ExecutionBlocked("provider authentication is not authorized")
        value = self._request(method="GET", path="/auth/renewaccesstoken", token=token, safe_retry=True)
        if not isinstance(value, Mapping):
            raise TransportError("Tradovate token renewal response was invalid")
        return token_from_response(value)

    def _read_list(self, path: str, token: AccessToken, *, query: Mapping[str, object] | None = None) -> list[Mapping[str, object]]:
        if not self.authority.read_only and not self.authority.local_simulator:
            raise ExecutionBlocked("provider read access is not authorized")
        value = self._request(method="GET", path=path, token=token, query=query, safe_retry=True)
        if not isinstance(value, list) or any(not isinstance(item, Mapping) for item in value):
            raise UnknownBrokerState("Tradovate list response was invalid")
        return list(value)

    def accounts(self, token: AccessToken) -> list[Mapping[str, object]]:
        return self._read_list("/account/list", token)

    def positions(self, token: AccessToken, *, account_id: int) -> list[Mapping[str, object]]:
        return self._read_list("/position/deps", token, query={"masterid": account_id})

    def orders(self, token: AccessToken) -> list[Mapping[str, object]]:
        return self._read_list("/order/list", token)

    def fills(self, token: AccessToken) -> list[Mapping[str, object]]:
        return self._read_list("/fill/list", token)

    def contract_find(self, token: AccessToken, *, name: str) -> Mapping[str, object]:
        values = self._read_list("/contract/suggest", token, query={"t": name, "l": 20})
        exact = [value for value in values if value.get("name") == name]
        if len(exact) != 1:
            raise UnknownBrokerState("exact Tradovate contract could not be resolved uniquely")
        return exact[0]

    def _change(self, path: str, token: AccessToken, body: Mapping[str, object]) -> Mapping[str, object]:
        if not self.authority.order_change and not self.authority.local_simulator:
            raise ExecutionBlocked("order-changing Tradovate access is not authorized")
        try:
            value = self._request(method="POST", path=path, token=token, body=body, safe_retry=False)
        except TimeoutError as exc:
            raise UnknownBrokerState("order outcome is unknown; reconcile without retry") from exc
        if not isinstance(value, Mapping):
            raise UnknownBrokerState("order-changing response was invalid; reconcile without retry")
        return value

    def place_order(self, token: AccessToken, body: Mapping[str, object]) -> Mapping[str, object]:
        return self._change("/order/placeorder", token, body)

    def place_oso(self, token: AccessToken, body: Mapping[str, object]) -> Mapping[str, object]:
        return self._change("/order/placeoso", token, body)

    def modify_order(self, token: AccessToken, body: Mapping[str, object]) -> Mapping[str, object]:
        return self._change("/order/modifyorder", token, body)

    def cancel_order(self, token: AccessToken, *, order_id: int) -> Mapping[str, object]:
        return self._change("/order/cancelorder", token, {"orderId": order_id})

    def liquidate_position(self, token: AccessToken, *, account_id: int, contract_id: int) -> Mapping[str, object]:
        return self._change("/order/liquidateposition", token, {"accountId": account_id, "contractId": contract_id, "admin": False})
