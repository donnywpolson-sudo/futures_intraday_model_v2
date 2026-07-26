"""Pinned causal-availability and normalization policy."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ..canonical import sha256_file, sha256_json
from ..errors import ContractError, IntegrityError
from ..time_contracts import require_utc


PROVIDER_TIMESTAMP_CLOCK_POLICY = {
    "causal_visibility": "TS_RECV_MUST_NOT_EXCEED_DECISION_TS_EVENT_AUDIT_ONLY",
    "cross_clock_order_assumption": "NONE",
    "definition_lifecycle_authority": (
        "ACTIVATION_EXPIRATION_SECURITY_UPDATE_ACTION"
    ),
    "definition_selection": (
        "SAME_INDEX_UTC_DATE_REPLAY_BY_TS_RECV_SOURCE_ORDER"
    ),
    "historical_archive_latency_claim": (
        "SOURCE_EPOCH_DEPENDENT_NEVER_INFERRED_FROM_CROSS_CLOCK_DELTA"
    ),
    "index_timestamp_when_present": "ts_recv",
    "instrument_id_date_utc_basis_when_ts_recv_present": "ts_recv",
    "negative_receive_event_delta": (
        "VALID_RECORDED_CROSS_CLOCK_OBSERVATION_NOT_AN_ELIGIBILITY_SIGNAL"
    ),
    "source_order": (
        "NONDECREASING_TS_RECV_THEN_SOURCE_FILE_AND_ROW_ORDINAL_"
        "EQUAL_TIME_CONFLICT_FAILS_CLOSED"
    ),
    "ts_event_authority": "PUBLISHER_OR_MARKET_ORIGINAL_UNADJUSTED",
    "ts_recv_authority": "DATABENTO_INDEX_TIMESTAMP_SOURCE_EPOCH_DEPENDENT",
}

DEFINITION_CAUSAL_POLICY = {
    "bar_start_baseline_required": True,
    "causal_knowledge_time_field": "ts_recv",
    "index_date_basis": "ts_recv_utc_date",
    "intrabar_critical_change_disposition": "FAIL_CLOSED",
    "lifecycle_authority": [
        "activation",
        "expiration",
        "security_update_action",
    ],
    "provider_event_time_role": "AUDIT_ONLY",
    "selection": "SAME_INDEX_UTC_DATE_REPLAY_BY_TS_RECV_SOURCE_ORDER",
}

PROVIDER_DATA_EPOCH_CONTRACT = {
    "contract_version": "1.0.0",
    "dataset": "GLBX.MDP3",
    "index_timestamp_when_present": "ts_recv",
    "epochs": [
        {
            "epoch_id": "GLBX_MDP2_PROVIDER_TIME",
            "start": "2010-06-06",
            "end_exclusive": "2017-05-21",
            "ts_recv_semantics": "PUBLISHER_TIME_NOT_CAPTURE_TIME",
            "physical_receive_latency_claim": False,
            "research_role": "HISTORICAL_DISCOVERY_CAVEATED",
        },
        {
            "epoch_id": "GLBX_MDP3_CAPTURE_TIME",
            "start": "2017-05-21",
            "end_exclusive": None,
            "ts_recv_semantics": "DATABENTO_INDEX_TIME_CAPTURE_OR_SNAPSHOT_DEPENDENT",
            "physical_receive_latency_claim": False,
            "research_role": "HISTORICAL_AND_PROSPECTIVE_CAUSAL",
        },
    ],
    "definition_lifecycle_quarantines": [
        {
            "start": "2015-11-22",
            "end_exclusive": "2017-05-21",
            "reason": "DATABENTO_REPORTED_INCORRECT_ACTIVATION_EXPIRATION",
            "disposition": "FAIL_CLOSED_IN_COVERAGE_DENOMINATOR",
            "source_url": (
                "https://issues.databento.com/roadmap/"
                "glbxmdp3-incorrect-activationexpiration-from-2015-11-22-to-2017-05-21"
            ),
        }
    ],
    "source_urls": [
        "https://databento.com/docs/knowledge-base/datasets/glbx-mdp3",
        "https://databento.com/docs/schemas-and-data-formats/instrument-definitions",
        (
            "https://databento.com/docs/standards-and-conventions/"
            "common-fields-enums-types"
        ),
    ],
}


def _utc_date_ns(value: str) -> int:
    parsed = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = parsed - epoch
    return delta.days * 86_400_000_000_000


_DATA_START_NS = _utc_date_ns("2010-06-06")
_MDP3_START_NS = _utc_date_ns("2017-05-21")
_LIFECYCLE_QUARANTINE_START_NS = _utc_date_ns("2015-11-22")
_LIFECYCLE_QUARANTINE_END_NS = _MDP3_START_NS


def _validate_provider_data_epochs(path: Path, expected_sha256: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is None:
        raise IntegrityError("provider data-epoch hash is invalid")
    if sha256_file(path) != expected_sha256:
        raise IntegrityError("provider data-epoch file differs from foundation policy")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError("provider data-epoch JSON is invalid") from exc
    if payload != PROVIDER_DATA_EPOCH_CONTRACT:
        raise IntegrityError("provider data-epoch contract is not exact")


@dataclass(frozen=True)
class FoundationPolicy:
    dataset: str
    interval: timedelta
    pinned_publication_latency: timedelta
    availability_basis: str
    policy_hash: str
    known_anomalies_sha256: str
    provider_data_epochs_sha256: str
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
            "provider_timestamp_clock_policy",
            "provider_data_epochs_sha256",
            "unknown_or_ambiguous_disposition",
        }
        if not isinstance(payload, dict) or set(payload) != expected:
            raise IntegrityError("foundation policy schema is not exact")
        bar = payload.get("ohlcv_1m")
        definition = payload.get("definition")
        normalization = payload.get("normalization")
        if (
            payload.get("policy_version") != "2.0.0"
            or payload.get("dataset") != "GLBX.MDP3"
            or payload.get("provider_timestamp_clock_policy")
            != PROVIDER_TIMESTAMP_CLOCK_POLICY
            or definition != DEFINITION_CAUSAL_POLICY
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
        epoch_hash = payload.get("provider_data_epochs_sha256")
        if (
            not isinstance(known_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", known_hash) is None
            or not isinstance(epoch_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", epoch_hash) is None
        ):
            raise IntegrityError("known-anomaly policy hash is invalid")
        _validate_provider_data_epochs(path.parent / "provider_data_epochs.json", epoch_hash)
        return cls(
            dataset="GLBX.MDP3",
            interval=timedelta(microseconds=interval_ns // 1000),
            pinned_publication_latency=timedelta(microseconds=latency_ns // 1000),
            availability_basis=bar["availability_basis"],
            policy_hash=sha256_json(payload),
            known_anomalies_sha256=known_hash,
            provider_data_epochs_sha256=epoch_hash,
        )

    def provider_timestamp_epoch_id(self, event_at_ns: int) -> str:
        if (
            isinstance(event_at_ns, bool)
            or not isinstance(event_at_ns, int)
            or event_at_ns < _DATA_START_NS
        ):
            raise ContractError("bar timestamp is outside the contracted provider epochs")
        return (
            "GLBX_MDP2_PROVIDER_TIME"
            if event_at_ns < _MDP3_START_NS
            else "GLBX_MDP3_CAPTURE_TIME"
        )

    def assert_definition_lifecycle_trusted(self, event_at_ns: int) -> None:
        self.provider_timestamp_epoch_id(event_at_ns)
        if _LIFECYCLE_QUARANTINE_START_NS <= event_at_ns < _LIFECYCLE_QUARANTINE_END_NS:
            raise ContractError(
                "definition lifecycle source epoch is quarantined fail closed"
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
