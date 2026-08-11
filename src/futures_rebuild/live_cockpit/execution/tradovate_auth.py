"""Memory-only Tradovate token lifecycle using an injected REST client."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Mapping

from .credential_store import CredentialReference, CredentialStore
from .domain import aware
from .errors import ExecutionBlocked


@dataclass(frozen=True)
class TradovateAuthMaterial:
    name: str
    password: str
    app_id: str
    app_version: str
    cid: str
    secret: str
    device_id: str

    @classmethod
    def from_reference(cls, *, store: CredentialStore, reference: CredentialReference) -> "TradovateAuthMaterial":
        raw = store.read(reference.target)
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ExecutionBlocked("Tradovate credential reference is invalid") from exc
        expected = {"name", "password", "app_id", "app_version", "cid", "secret", "device_id"}
        if not isinstance(value, Mapping) or set(value) != expected or any(not isinstance(value[key], str) or not value[key] for key in expected):
            raise ExecutionBlocked("Tradovate credential reference is invalid")
        return cls(**value)

    def request_body(self) -> dict[str, str]:
        return {
            "name": self.name,
            "password": self.password,
            "appId": self.app_id,
            "appVersion": self.app_version,
            "cid": self.cid,
            "sec": self.secret,
            "deviceId": self.device_id,
        }


@dataclass(frozen=True)
class AccessToken:
    value: str
    expiration_time: datetime
    user_id: int
    user_status: str

    def __post_init__(self) -> None:
        if not self.value or len(self.value) > 8192:
            raise ValueError("access token is invalid")
        aware(self.expiration_time, name="expiration_time")
        if self.user_id <= 0:
            raise ValueError("user_id is invalid")

    def renewal_due(self, *, now: datetime) -> bool:
        return self.expiration_time - aware(now, name="now") <= timedelta(minutes=15)

    def expired(self, *, now: datetime) -> bool:
        return aware(now, name="now") >= self.expiration_time


def token_from_response(value: Mapping[str, object]) -> AccessToken:
    if value.get("errorText"):
        raise ExecutionBlocked("Tradovate authentication was rejected")
    try:
        expiration = datetime.fromisoformat(str(value["expirationTime"]).replace("Z", "+00:00"))
        token = str(value["accessToken"])
        user_id = int(value["userId"])
        status = str(value["userStatus"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ExecutionBlocked("Tradovate authentication response was invalid") from exc
    return AccessToken(token, expiration, user_id, status)


class TokenManager:
    """One in-memory token per application instance; never serializes tokens."""

    def __init__(self) -> None:
        self._token: AccessToken | None = None

    def clear(self) -> None:
        self._token = None

    def set(self, token: AccessToken) -> None:
        self._token = token

    def current(self, *, now: datetime | None = None) -> AccessToken:
        instant = datetime.now(timezone.utc) if now is None else aware(now, name="now")
        if self._token is None or self._token.expired(now=instant):
            self.clear()
            raise ExecutionBlocked("Tradovate access token is absent or expired")
        return self._token
