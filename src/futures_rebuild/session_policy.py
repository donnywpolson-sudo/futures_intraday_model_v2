"""Verified, versioned exchange-session date derivation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .boundary import RepoBoundary
from .canonical import sha256_json
from .errors import ContractError, IntegrityError
from .release import VerifiedReleaseReceipt
from .time_contracts import require_utc


_VERIFIED_SESSION_POLICY_FACTORY = object()


@dataclass(frozen=True)
class SessionDateRule:
    exchange: str
    timezone_name: str
    session_roll_local: time
    post_roll_day_offset: int

    def __post_init__(self) -> None:
        if not self.exchange or not self.timezone_name:
            raise ContractError("session rule exchange and timezone are required")
        if not 0 <= self.post_roll_day_offset <= 2:
            raise ContractError("session-date offset is outside the permitted range")
        try:
            ZoneInfo(self.timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ContractError("session rule timezone is unknown") from exc

    def as_dict(self) -> dict[str, object]:
        return {
            "exchange": self.exchange,
            "post_roll_day_offset": self.post_roll_day_offset,
            "session_roll_local": self.session_roll_local.isoformat(),
            "timezone": self.timezone_name,
        }


@dataclass(frozen=True)
class VerifiedSessionPolicy:
    receipt: VerifiedReleaseReceipt
    rules: Mapping[str, SessionDateRule]
    policy_hash: str
    boundary: RepoBoundary
    _factory_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._factory_token is not _VERIFIED_SESSION_POLICY_FACTORY:
            raise ContractError(
                "verified session policy can only be created from a verified release"
            )

    @classmethod
    def from_release(
        cls, receipt: VerifiedReleaseReceipt, boundary: RepoBoundary
    ) -> "VerifiedSessionPolicy":
        manifest = receipt.verify(boundary)
        if manifest.release_kind != "versioned_session_policy":
            raise IntegrityError("session policy receipt has the wrong release kind")
        paths = {entry.path for entry in manifest.files}
        if paths != {"session_policy.json"}:
            raise IntegrityError("session policy release must contain exactly one policy file")
        policy_path = boundary.active_root / receipt.relative_root / "session_policy.json"
        try:
            payload = json.loads(policy_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("session policy JSON is invalid") from exc
        if (
            not isinstance(payload, dict)
            or set(payload) != {"policy_version", "rules"}
            or payload.get("policy_version") != "1.0.0"
            or not isinstance(payload.get("rules"), list)
            or not payload["rules"]
        ):
            raise IntegrityError("session policy schema/version is invalid")
        rules: dict[str, SessionDateRule] = {}
        for raw in payload["rules"]:
            if not isinstance(raw, dict) or set(raw) != {
                "exchange",
                "post_roll_day_offset",
                "session_roll_local",
                "timezone",
            }:
                raise IntegrityError("session policy rule schema is invalid")
            if (
                any(
                    type(raw[name]) is not str
                    for name in ("exchange", "session_roll_local", "timezone")
                )
                or type(raw["post_roll_day_offset"]) is not int
            ):
                raise IntegrityError("session policy rule field types are not exact")
            try:
                rule = SessionDateRule(
                    exchange=raw["exchange"],
                    timezone_name=raw["timezone"],
                    session_roll_local=time.fromisoformat(raw["session_roll_local"]),
                    post_roll_day_offset=raw["post_roll_day_offset"],
                )
            except (TypeError, ValueError, ContractError) as exc:
                raise IntegrityError("session policy rule is invalid") from exc
            if rule.exchange in rules or rule.as_dict() != raw:
                raise IntegrityError("session policy rules are duplicate or noncanonical")
            rules[rule.exchange] = rule
        core = {
            "policy": payload,
            "release_receipt_id": receipt.receipt_id,
        }
        return cls(
            receipt,
            MappingProxyType(rules),
            sha256_json(core),
            boundary,
            _VERIFIED_SESSION_POLICY_FACTORY,
        )

    def verify(self) -> None:
        rebuilt = type(self).from_release(self.receipt, self.boundary)
        if rebuilt.policy_hash != self.policy_hash or dict(rebuilt.rules) != dict(self.rules):
            raise IntegrityError("session policy changed after verification")

    def exchange_session_date(self, exchange: str, event_at: datetime) -> date:
        self.verify()
        event = require_utc(event_at, "bar_event_at")
        try:
            rule = self.rules[exchange]
        except KeyError as exc:
            raise ContractError(f"session policy has no rule for exchange {exchange}") from exc
        local = event.astimezone(ZoneInfo(rule.timezone_name))
        result = local.date()
        if local.timetz().replace(tzinfo=None) >= rule.session_roll_local:
            result += timedelta(days=rule.post_roll_day_offset)
        return result
