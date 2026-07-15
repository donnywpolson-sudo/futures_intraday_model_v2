"""Pinned causal-availability and normalization policy."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from ..canonical import sha256_file, sha256_json
from ..errors import ContractError, IntegrityError
from ..time_contracts import require_utc


@dataclass(frozen=True)
class FoundationPolicy:
    dataset: str
    interval: timedelta
    pinned_publication_latency: timedelta
    availability_basis: str
    policy_hash: str
    known_anomalies_sha256: str
    learned_normalization_allowed: bool = False

    @property
    def interval_ns(self) -> int:
        return (
            self.interval.days * 86_400_000_000_000
            + self.interval.seconds * 1_000_000_000
            + self.interval.microseconds * 1_000
        )

    @property
    def pinned_publication_latency_ns(self) -> int:
        return (
            self.pinned_publication_latency.days * 86_400_000_000_000
            + self.pinned_publication_latency.seconds * 1_000_000_000
            + self.pinned_publication_latency.microseconds * 1_000
        )

    @classmethod
    def from_file(cls, path: Path) -> "FoundationPolicy":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("foundation policy JSON is invalid") from exc
        expected = {
            "canonical_research_schemas",
            "continuous_contract",
            "dataset",
            "definition",
            "known_anomalies_sha256",
            "normalization",
            "ohlcv_1m",
            "policy_version",
            "unknown_or_ambiguous_disposition",
        }
        if not isinstance(payload, dict) or set(payload) != expected:
            raise IntegrityError("foundation policy schema is not exact")
        bar = payload.get("ohlcv_1m")
        normalization = payload.get("normalization")
        if (
            payload.get("policy_version") != "1.0.0"
            or payload.get("dataset") != "GLBX.MDP3"
            or not isinstance(bar, dict)
            or not isinstance(normalization, dict)
            or bar.get("ts_event_semantics") != "INTERVAL_START_UTC"
            or bar.get("availability_basis")
            != "MODELED_INTERVAL_END_PLUS_PINNED_LATENCY"
            or bar.get("provider_ts_recv_may_be_invented") is not False
            or normalization.get("learned_transform_location")
            != "INSIDE_EACH_WFA_TRAINING_FOLD_ONLY"
        ):
            raise IntegrityError("foundation policy weakens a pinned causal boundary")
        interval_ns = bar.get("interval_nanoseconds")
        latency_ns = bar.get("pinned_publication_latency_nanoseconds")
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in (interval_ns, latency_ns)
        ) or interval_ns % 1000 or latency_ns % 1000:
            raise IntegrityError("foundation availability durations are invalid")
        forbidden = set(normalization.get("foundation_forbidden", []))
        required_forbidden = {
            "GLOBAL_ZSCORE",
            "GLOBAL_WINSORIZATION",
            "GLOBAL_IMPUTATION",
            "GLOBAL_VOLATILITY_SCALING",
            "OUTCOME_CONDITIONED_FILTER",
        }
        if forbidden != required_forbidden:
            raise IntegrityError("foundation learned-normalization denylist is incomplete")
        known_hash = payload.get("known_anomalies_sha256")
        if not isinstance(known_hash, str) or len(known_hash) != 64:
            raise IntegrityError("known-anomaly policy hash is invalid")
        return cls(
            dataset="GLBX.MDP3",
            interval=timedelta(microseconds=interval_ns // 1000),
            pinned_publication_latency=timedelta(microseconds=latency_ns // 1000),
            availability_basis=bar["availability_basis"],
            policy_hash=sha256_json(payload),
            known_anomalies_sha256=known_hash,
        )

    def bar_available_at(self, event_at: datetime) -> datetime:
        event = require_utc(event_at, "bar.event_at")
        return event + self.interval + self.pinned_publication_latency

    def bar_available_at_ns(self, event_at_ns: int) -> int:
        if isinstance(event_at_ns, bool) or not isinstance(event_at_ns, int) or event_at_ns < 0:
            raise ContractError("bar.event_at_ns must be an exact nonnegative integer")
        return event_at_ns + self.interval_ns + self.pinned_publication_latency_ns

    def assert_decision_can_read_bar(
        self, *, event_at: datetime, decision_at: datetime
    ) -> datetime:
        decision = require_utc(decision_at, "decision_at")
        available = self.bar_available_at(event_at)
        if decision < available:
            raise ContractError("bar is not modeled available at the decision time")
        return available

    def assert_foundation_transform(self, transform_id: str) -> None:
        allowed = {
            "NANOUNIT_TO_EXACT_DECIMAL",
            "UTC_TIMESTAMP",
            "EXPLICIT_UNIT_AND_SCHEMA_CANONICALIZATION",
        }
        if transform_id not in allowed:
            raise ContractError("learned or outcome-informed transforms are WFA-fold-only")


@dataclass(frozen=True)
class KnownAnomalyPolicy:
    families: frozenset[tuple[str, int]]
    policy_hash: str

    @classmethod
    def from_file(
        cls, path: Path, *, expected_sha256: str
    ) -> "KnownAnomalyPolicy":
        if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
            raise IntegrityError("expected known-anomaly hash is invalid")
        observed_hash = sha256_file(path)
        if observed_hash != expected_sha256:
            raise IntegrityError("known-anomaly file differs from the foundation policy")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("known-anomaly policy JSON is invalid") from exc
        expected = {
            "contract_version",
            "default_disposition",
            "families",
            "promotion_requirement",
            "waivers_allowed",
        }
        if (
            not isinstance(payload, dict)
            or set(payload) != expected
            or payload.get("contract_version") != "1.0.0"
            or payload.get("default_disposition") != "QUARANTINE_FAIL_CLOSED"
            or payload.get("waivers_allowed") is not False
            or payload.get("promotion_requirement")
            != "anomaly_specific_source_alignment_and_causal_tests_pass"
            or not isinstance(payload.get("families"), list)
        ):
            raise IntegrityError("known-anomaly policy schema is not exact")
        families: set[tuple[str, int]] = set()
        normalized: list[dict[str, object]] = []
        for raw in payload["families"]:
            if not isinstance(raw, dict) or set(raw) != {"market", "year"}:
                raise IntegrityError("known-anomaly family schema is not exact")
            market = raw["market"]
            year = raw["year"]
            if (
                not isinstance(market, str)
                or re.fullmatch(r"[0-9A-Z]{2,3}", market) is None
                or isinstance(year, bool)
                or not isinstance(year, int)
                or not 1900 <= year <= 2200
                or (market, year) in families
            ):
                raise IntegrityError("known-anomaly family is invalid or duplicated")
            families.add((market, year))
            normalized.append({"market": market, "year": year})
        if normalized != sorted(normalized, key=lambda item: (str(item["market"]), int(item["year"]))):
            raise IntegrityError("known-anomaly families are not canonical")
        return cls(frozenset(families), observed_hash)

    def is_quarantined(self, market: str, year: int) -> bool:
        return (market, year) in self.families
