"""Transport-injected Tradovate WebSocket framing and user synchronization."""

from __future__ import annotations

import json
from typing import Any, Mapping, Protocol

from .errors import TransportError
from .tradovate_auth import AccessToken
from .tradovate_rest import ServiceRoots


class WebSocketTransport(Protocol):
    def open(self, *, url: str, timeout_seconds: float) -> None: ...
    def send(self, value: str) -> None: ...
    def receive(self, *, timeout_seconds: float) -> str: ...
    def close(self) -> None: ...


def request_frame(endpoint: str, request_id: int, payload: Mapping[str, object] | str | None = None) -> str:
    if not endpoint or any(character in endpoint for character in "\r\n"):
        raise ValueError("WebSocket endpoint is invalid")
    if request_id < 0:
        raise ValueError("request_id must be nonnegative")
    if payload is None:
        body = ""
    elif isinstance(payload, str):
        body = payload
    else:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return f"{endpoint}\n{request_id}\n\n{body}"


def parse_server_frame(value: str) -> list[Mapping[str, Any]]:
    if value in {"o", "h"}:
        return []
    if not isinstance(value, str) or len(value) > 2_000_000 or not value.startswith("a"):
        raise TransportError("Tradovate WebSocket frame was invalid")
    try:
        outer = json.loads(value[1:])
    except json.JSONDecodeError as exc:
        raise TransportError("Tradovate WebSocket frame was invalid") from exc
    if not isinstance(outer, list) or len(outer) > 5000:
        raise TransportError("Tradovate WebSocket frame was oversized")
    result: list[Mapping[str, Any]] = []
    for item in outer:
        decoded = item
        if isinstance(item, str):
            try:
                decoded = json.loads(item)
            except json.JSONDecodeError as exc:
                raise TransportError("Tradovate WebSocket message was invalid") from exc
        if not isinstance(decoded, Mapping):
            raise TransportError("Tradovate WebSocket message was invalid")
        result.append(decoded)
    return result


class TradovateUserSync:
    def __init__(self, *, roots: ServiceRoots, transport: WebSocketTransport, timeout_seconds: float = 10.0) -> None:
        if timeout_seconds <= 0 or timeout_seconds > 30:
            raise ValueError("WebSocket timeout must be within 30 seconds")
        self.roots = roots
        self.transport = transport
        self.timeout_seconds = timeout_seconds
        self._opened = False

    def open(self, *, token: AccessToken, user_id: int, account_id: int) -> None:
        self.transport.open(url=self.roots.websocket, timeout_seconds=self.timeout_seconds)
        try:
            self.transport.send(request_frame("authorize", 0, token.value))
            self.transport.send(request_frame("user/syncrequest", 1, {"users": [user_id], "accounts": [account_id], "splitResponses": True}))
        except Exception:
            self.transport.close()
            raise
        self._opened = True

    def receive(self) -> list[Mapping[str, Any]]:
        if not self._opened:
            raise TransportError("Tradovate user synchronization is not open")
        return parse_server_frame(self.transport.receive(timeout_seconds=self.timeout_seconds))

    def close(self) -> None:
        self.transport.close()
        self._opened = False
