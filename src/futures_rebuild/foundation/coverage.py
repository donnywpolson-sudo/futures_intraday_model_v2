"""Fail-closed research-scope policy for source-capability epochs."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ..canonical import assert_plain_file, canonical_bytes, sha256_json
from ..errors import ContractError, IntegrityError


_LAUNCH_MONTH = re.compile(r"[0-9]{4}-(0[1-9]|1[0-2])")
_ALIGNMENT = "FIRST_COMPLETE_UTC_YEAR_AFTER_PROVIDER_STATUS_LAUNCH_MONTH"
_PRE_SCOPE = "ABSTAIN_PRE_STATUS_CAPABILITY_EPOCH"
_ELIGIBLE = "ELIGIBLE"
_FAILED = "FAIL_STATUS_COVERAGE"


def _read_canonical_object(path: Path) -> dict[str, object]:
    try:
        assert_plain_file(path)
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("status research scope policy JSON is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("status research scope policy is not canonical JSON")
    return payload


@dataclass(frozen=True)
class StatusResearchScopePolicy:
    """Select a conservative, source-defined interval suffix without alpha data."""

    provider_status_launch_month: str
    research_interval_start: str
    source_urls: tuple[str, ...]
    alignment: str = _ALIGNMENT
    pre_scope_disposition: str = _PRE_SCOPE
    require_all_selected_intervals_at_or_after_start: bool = True

    @classmethod
    def from_file(cls, path: Path) -> "StatusResearchScopePolicy":
        return cls.from_dict(_read_canonical_object(path))

    @classmethod
    def from_dict(cls, payload: object) -> "StatusResearchScopePolicy":
        expected = {
            "alignment",
            "policy_version",
            "pre_scope_disposition",
            "provider_status_launch_month",
            "require_all_selected_intervals_at_or_after_start",
            "research_interval_start",
            "source_urls",
        }
        if (
            not isinstance(payload, dict)
            or set(payload) != expected
            or payload.get("policy_version") != "1.0.0"
            or payload.get("alignment") != _ALIGNMENT
            or payload.get("pre_scope_disposition") != _PRE_SCOPE
            or payload.get("require_all_selected_intervals_at_or_after_start")
            is not True
            or not isinstance(payload.get("source_urls"), list)
        ):
            raise ContractError("status research scope policy schema is invalid")
        try:
            policy = cls(
                provider_status_launch_month=payload[
                    "provider_status_launch_month"
                ],
                research_interval_start=payload["research_interval_start"],
                source_urls=tuple(payload["source_urls"]),
            )
        except TypeError as exc:
            raise ContractError("status research scope policy fields are invalid") from exc
        policy._validate()
        if policy.as_dict() != payload:
            raise ContractError("status research scope policy is not canonical")
        return policy

    def _validate(self) -> None:
        if (
            type(self.provider_status_launch_month) is not str
            or _LAUNCH_MONTH.fullmatch(self.provider_status_launch_month) is None
            or type(self.research_interval_start) is not str
            or type(self.source_urls) is not tuple
            or len(self.source_urls) < 2
            or self.source_urls != tuple(dict.fromkeys(self.source_urls))
            or any(
                type(url) is not str
                or not url.startswith("https://databento.com/")
                for url in self.source_urls
            )
        ):
            raise ContractError("status research scope policy values are invalid")
        launch_year = int(self.provider_status_launch_month[:4])
        try:
            start = date.fromisoformat(self.research_interval_start)
        except ValueError as exc:
            raise ContractError("status research interval start is invalid") from exc
        if start != date(launch_year + 1, 1, 1):
            raise ContractError(
                "status research interval start is not the first complete UTC year"
            )

    @property
    def start_date(self) -> date:
        self._validate()
        return date.fromisoformat(self.research_interval_start)

    def includes_interval(self, *, start: str, end: str) -> bool:
        try:
            interval_start = date.fromisoformat(start)
            interval_end = date.fromisoformat(end)
        except (TypeError, ValueError) as exc:
            raise IntegrityError("research interval selector dates are invalid") from exc
        if interval_end <= interval_start:
            raise IntegrityError("research interval selector is empty or reversed")
        return interval_start >= self.start_date

    def disposition(
        self, *, start: str, end: str, coverage_passed: bool
    ) -> str:
        if type(coverage_passed) is not bool:
            raise IntegrityError("research interval coverage state is invalid")
        if not self.includes_interval(start=start, end=end):
            return self.pre_scope_disposition
        return _ELIGIBLE if coverage_passed else _FAILED

    def as_dict(self) -> dict[str, object]:
        self._validate()
        return {
            "alignment": self.alignment,
            "policy_version": "1.0.0",
            "pre_scope_disposition": self.pre_scope_disposition,
            "provider_status_launch_month": self.provider_status_launch_month,
            "require_all_selected_intervals_at_or_after_start": (
                self.require_all_selected_intervals_at_or_after_start
            ),
            "research_interval_start": self.research_interval_start,
            "source_urls": list(self.source_urls),
        }

    @property
    def policy_hash(self) -> str:
        return sha256_json(self.as_dict())


def canonical_scope_policy_bytes(policy: StatusResearchScopePolicy) -> bytes:
    """Return the exact tracked representation used by closure tests."""

    return canonical_bytes(policy.as_dict()) + b"\n"


__all__ = ["StatusResearchScopePolicy", "canonical_scope_policy_bytes"]
