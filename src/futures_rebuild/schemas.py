"""Separate feature, outcome, and prediction schemas."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from .boundary import RepoBoundary
from .canonical import sha256_json
from .errors import ContractError
from .identity import ActualContractIdentity
from .release import VerifiedReleaseReceipt
from .time_contracts import AvailabilityBasis, require_utc


FORBIDDEN_FEATURE_NAMES = {
    "target",
    "label",
    "outcome",
    "future_return",
    "realized_outcome",
    "exit_price",
    "label_end_at",
    "matured_status",
}
FORBIDDEN_FEATURE_PREFIXES = ("target_", "label_", "outcome_", "future_")
FORBIDDEN_ROLL_FEATURE_NAMES = {
    "bars_until_roll",
    "bars_to_roll",
    "next_roll_at",
    "realized_roll_at",
    "realized_roll_boundary",
    "roll_end_at",
    "roll_interval_end",
}
CAUSAL_FEATURE_UPSTREAM_RELEASE_KIND = "futures_phase2_causal_interval"


class OutcomeStatus(str, Enum):
    MATURED = "MATURED"
    HALTED = "HALTED"
    EXPIRED = "EXPIRED"
    ROLL_UNRESOLVED = "ROLL_UNRESOLVED"
    MISSING_SOURCE = "MISSING_SOURCE"


@dataclass(frozen=True)
class FeatureLineage:
    source_release_id: str
    available_at: datetime
    transform_hash: str
    availability_basis: AvailabilityBasis
    availability_policy_hash: str
    source_release_retrieved_at: datetime
    contract_segment_hash: str
    uses_retrospective_roll_mapping: bool = False
    uses_future_outcome: bool = False

    def __post_init__(self) -> None:
        available = require_utc(self.available_at, "feature_lineage.available_at")
        retrieved = require_utc(
            self.source_release_retrieved_at,
            "feature_lineage.source_release_retrieved_at",
        )
        if any(
            type(value) is not str
            or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in (
                self.source_release_id,
                self.transform_hash,
                self.availability_policy_hash,
                self.contract_segment_hash,
            )
        ):
            raise ContractError("feature lineage release and policy identities must be SHA-256")
        if not isinstance(self.availability_basis, AvailabilityBasis):
            raise ContractError("feature availability basis and policy hash are required")
        if retrieved < available:
            raise ContractError("source retrieval cannot precede modeled/source availability")
        if (
            type(self.uses_retrospective_roll_mapping) is not bool
            or type(self.uses_future_outcome) is not bool
        ):
            raise ContractError("feature lineage information-use flags must be exact booleans")
        if self.uses_retrospective_roll_mapping or self.uses_future_outcome:
            raise ContractError("feature lineage contains a forbidden future-information dependency")

    def as_dict(self) -> dict[str, object]:
        return {
            "available_at": self.available_at.isoformat(),
            "availability_basis": self.availability_basis.value,
            "availability_policy_hash": self.availability_policy_hash,
            "contract_segment_hash": self.contract_segment_hash,
            "source_release_id": self.source_release_id,
            "source_release_retrieved_at": self.source_release_retrieved_at.isoformat(),
            "transform_hash": self.transform_hash,
            "uses_future_outcome": self.uses_future_outcome,
            "uses_retrospective_roll_mapping": self.uses_retrospective_roll_mapping,
        }


@dataclass(frozen=True)
class FeatureRow:
    actual: ActualContractIdentity
    bar_event_at: datetime
    decision_at: datetime
    available_at_max: datetime
    source_release_id: str
    allowed_upstream_release_ids: tuple[str, ...]
    verified_release_receipts: tuple[VerifiedReleaseReceipt, ...]
    boundary: RepoBoundary
    values: Mapping[str, float | int | bool | None]
    lineage: Mapping[str, FeatureLineage]
    inputs_complete: bool
    planned_entry_at: datetime
    label_unlock_at: datetime

    def __post_init__(self) -> None:
        bar_event = require_utc(self.bar_event_at, "bar_event_at")
        decision = require_utc(self.decision_at, "decision_at")
        available = require_utc(self.available_at_max, "available_at_max")
        entry = require_utc(self.planned_entry_at, "planned_entry_at")
        label_unlock = require_utc(self.label_unlock_at, "label_unlock_at")
        if bar_event > available or available > decision:
            raise ContractError("features include information unavailable at decision time")
        if entry <= decision or label_unlock < entry:
            raise ContractError("entry and label-unlock times must follow the decision")
        if re.fullmatch(r"[0-9a-f]{64}", self.source_release_id) is None or not self.values:
            raise ContractError("feature source release and nonempty values are required")
        if (
            not self.allowed_upstream_release_ids
            or self.allowed_upstream_release_ids
            != tuple(sorted(set(self.allowed_upstream_release_ids)))
            or any(
                re.fullmatch(r"[0-9a-f]{64}", item) is None
                for item in self.allowed_upstream_release_ids
            )
        ):
            raise ContractError("allowed upstream releases must be explicit, unique, and sorted")
        if set(self.lineage) != set(self.values):
            raise ContractError("every feature requires exactly one causal lineage record")
        for name, lineage in self.lineage.items():
            if lineage.source_release_id not in self.allowed_upstream_release_ids:
                raise ContractError(f"feature lineage uses a non-allowlisted release: {name}")
            if lineage.contract_segment_hash != self.actual.contract_segment_hash:
                raise ContractError(
                    f"feature lineage crosses an actual instrument_id boundary: {name}"
                )
        receipts_by_release: dict[str, VerifiedReleaseReceipt] = {}
        release_kinds: dict[str, str] = {}
        for receipt in self.verified_release_receipts:
            manifest = receipt.verify(self.boundary)
            if receipt.release_id in receipts_by_release:
                raise ContractError("feature release receipts are duplicate")
            receipts_by_release[receipt.release_id] = receipt
            release_kinds[receipt.release_id] = manifest.release_kind
        required_releases = {self.source_release_id, *self.allowed_upstream_release_ids}
        if set(receipts_by_release) != required_releases:
            raise ContractError("feature row release IDs do not exactly match verified receipts")
        if release_kinds[self.source_release_id] != "feature_release":
            raise ContractError("feature source must have the verified feature-release role")
        if any(
            release_kinds[release_id] != CAUSAL_FEATURE_UPSTREAM_RELEASE_KIND
            for release_id in self.allowed_upstream_release_ids
        ):
            raise ContractError(
                "feature upstream must have the verified causal-bar release role"
            )
        if type(self.inputs_complete) is not bool:
            raise ContractError("inputs_complete must be an explicit boolean")
        for name, value in self.values.items():
            normalized = name.casefold()
            if (
                normalized in FORBIDDEN_FEATURE_NAMES
                or normalized in FORBIDDEN_ROLL_FEATURE_NAMES
                or normalized.startswith(FORBIDDEN_FEATURE_PREFIXES)
            ):
                raise ContractError(f"outcome-like field is forbidden in features: {name}")
            if not name or not name.isidentifier():
                raise ContractError(f"invalid feature name: {name!r}")
            if value is not None and not isinstance(value, (bool, int, float)):
                raise ContractError(
                    f"feature values must be numeric, boolean, or null: {name}"
                )
            if isinstance(value, float) and not math.isfinite(value):
                raise ContractError(f"non-finite feature value: {name}")
            lineage = self.lineage[name]
            if lineage.available_at > decision:
                raise ContractError(f"feature lineage is not available by decision time: {name}")
        computed_available = max(item.available_at for item in self.lineage.values())
        if available != computed_available:
            raise ContractError(
                "available_at_max must equal the computed maximum lineage availability"
            )
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))
        object.__setattr__(self, "lineage", MappingProxyType(dict(self.lineage)))

    @property
    def row_id(self) -> str:
        return sha256_json(
            {
                "actual_contract": self.actual.as_dict(),
                "bar_event_at": self.bar_event_at.isoformat(),
                "available_at_max": self.available_at_max.isoformat(),
                "decision_at": self.decision_at.isoformat(),
                "inputs_complete": self.inputs_complete,
                "label_unlock_at": self.label_unlock_at.isoformat(),
                "lineage": {
                    name: self.lineage[name].as_dict() for name in sorted(self.lineage)
                },
                "planned_entry_at": self.planned_entry_at.isoformat(),
                "source_release_id": self.source_release_id,
                "allowed_upstream_release_ids": list(self.allowed_upstream_release_ids),
                "verified_release_receipts": [
                    receipt.as_dict()
                    for receipt in sorted(
                        self.verified_release_receipts, key=lambda item: item.release_id
                    )
                ],
                "values": dict(self.values),
            }
        )

    @property
    def source_release_receipt(self) -> VerifiedReleaseReceipt:
        return next(
            receipt
            for receipt in self.verified_release_receipts
            if receipt.release_id == self.source_release_id
        )


@dataclass(frozen=True)
class OutcomeRow:
    prediction_id: str
    actual: ActualContractIdentity
    decision_at: datetime
    label_end_at: datetime
    matured_at: datetime
    source_release_id: str
    interval_contract_segment_hashes: tuple[str, ...]
    included_in_coverage_denominator: bool
    status: OutcomeStatus
    price_return: float | None

    def __post_init__(self) -> None:
        decision = require_utc(self.decision_at, "decision_at")
        label_end = require_utc(self.label_end_at, "label_end_at")
        matured = require_utc(self.matured_at, "matured_at")
        if label_end <= decision or matured < label_end:
            raise ContractError("outcome interval must follow the decision")
        if (
            re.fullmatch(r"[0-9a-f]{64}", self.prediction_id) is None
            or re.fullmatch(r"[0-9a-f]{64}", self.source_release_id) is None
        ):
            raise ContractError("outcome prediction and source release identities are required")
        if (
            not self.interval_contract_segment_hashes
            or any(
                re.fullmatch(r"[0-9a-f]{64}", item) is None
                for item in self.interval_contract_segment_hashes
            )
            or self.interval_contract_segment_hashes[0]
            != self.actual.contract_segment_hash
        ):
            raise ContractError("outcome contract-segment evidence is invalid")
        if self.included_in_coverage_denominator is not True:
            raise ContractError("every predicted outcome must remain in the coverage denominator")
        if not isinstance(self.status, OutcomeStatus):
            raise ContractError("outcome status must use the declared enum")
        crossed_contract = len(set(self.interval_contract_segment_hashes)) != 1
        if crossed_contract and self.status is not OutcomeStatus.ROLL_UNRESOLVED:
            raise ContractError(
                "cross-contract outcome must remain unresolved without explicit multi-leg P&L"
            )
        if self.status is OutcomeStatus.MATURED:
            if (
                self.price_return is None
                or isinstance(self.price_return, bool)
                or not isinstance(self.price_return, (int, float))
                or not math.isfinite(self.price_return)
            ):
                raise ContractError("matured outcomes require a finite price return")
        elif self.price_return is not None:
            raise ContractError("unresolved outcomes cannot contain a return")


@dataclass(frozen=True)
class OutcomeCoverageReport:
    """Exact prediction/outcome join that makes unresolved rows non-droppable."""

    prediction_census: object
    outcomes: tuple[OutcomeRow, ...]
    prediction_ledger: object

    def __post_init__(self) -> None:
        from .ledger import PredictionCensusReceipt, PredictionLedger

        if (
            type(self.prediction_census) is not PredictionCensusReceipt
            or type(self.prediction_ledger) is not PredictionLedger
        ):
            raise ContractError(
                "coverage requires a census issued by the exact prediction ledger"
            )
        self.prediction_census.verify(self.prediction_ledger)
        prediction_ids = self.prediction_census.prediction_ids
        if (
            not prediction_ids
            or len(set(prediction_ids)) != len(prediction_ids)
            or any(re.fullmatch(r"[0-9a-f]{64}", item) is None for item in prediction_ids)
        ):
            raise ContractError("coverage census prediction IDs are invalid")
        outcome_ids = tuple(item.prediction_id for item in self.outcomes)
        if len(set(outcome_ids)) != len(outcome_ids) or set(outcome_ids) != set(
            prediction_ids
        ):
            raise ContractError(
                "outcome coverage must contain exactly one row for every prediction"
            )

    def _verify_census(self) -> None:
        self.prediction_census.verify(self.prediction_ledger)  # type: ignore[attr-defined]

    @property
    def prediction_ids(self) -> tuple[str, ...]:
        self._verify_census()
        return self.prediction_census.prediction_ids  # type: ignore[attr-defined]

    @property
    def denominator_count(self) -> int:
        self._verify_census()
        return len(self.prediction_ids)

    @property
    def resolved_count(self) -> int:
        self._verify_census()
        return sum(item.status is OutcomeStatus.MATURED for item in self.outcomes)

    @property
    def unresolved_count(self) -> int:
        return self.denominator_count - self.resolved_count


@dataclass(frozen=True)
class PredictionRow:
    prediction_id: str
    bundle_id: str
    actual: ActualContractIdentity
    decision_at: datetime
    recorded_at: datetime
    source_release_id: str
    source_release_receipt_id: str
    economics_record_id: str
    feature_row_id: str
    planned_entry_at: datetime
    label_unlock_at: datetime
    abstained: bool
    abstention_reasons: tuple[str, ...]
    expected_return: float | None
    probability_up: float | None
    probability_down: float | None
    probability_neutral: float | None
    uncertainty: float | None
    bundle_classification: str = "SYNTHETIC_MECHANICS_ONLY"
    candidate_provenance_id: str | None = None
    production_eligible: bool = False

    def __post_init__(self) -> None:
        require_utc(self.decision_at, "decision_at")
        recorded = require_utc(self.recorded_at, "recorded_at")
        entry = require_utc(self.planned_entry_at, "planned_entry_at")
        label_unlock = require_utc(self.label_unlock_at, "label_unlock_at")
        if entry <= self.decision_at or label_unlock < entry or recorded < self.decision_at:
            raise ContractError("prediction entry and label-unlock chronology is invalid")
        if type(self.actual) is not ActualContractIdentity or any(
            type(value) is not str
            or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in (
                self.prediction_id,
                self.bundle_id,
                self.source_release_id,
                self.source_release_receipt_id,
                self.economics_record_id,
                self.feature_row_id,
            )
        ):
            raise ContractError("prediction, bundle, feature-row, and source release IDs are required")
        if type(self.production_eligible) is not bool or type(
            self.bundle_classification
        ) is not str:
            raise ContractError("prediction production provenance types are invalid")
        if self.bundle_classification == "SYNTHETIC_MECHANICS_ONLY":
            if self.candidate_provenance_id is not None or self.production_eligible:
                raise ContractError("synthetic predictions can never be production eligible")
        elif self.bundle_classification == "CANDIDATE":
            if (
                re.fullmatch(r"[0-9a-f]{64}", self.candidate_provenance_id or "")
                is None
                or not self.production_eligible
            ):
                raise ContractError("candidate prediction lacks exact readiness provenance")
        else:
            raise ContractError("prediction bundle classification is invalid")
        if type(self.abstention_reasons) is not tuple:
            raise ContractError("abstention reasons must be an immutable tuple")
        if type(self.abstained) is not bool or any(
            type(reason) is not str or not reason for reason in self.abstention_reasons
        ):
            raise ContractError("abstention state and reasons are invalid")
        forecasts = (
            self.expected_return,
            self.probability_up,
            self.probability_down,
            self.probability_neutral,
            self.uncertainty,
        )
        if self.abstained:
            if not self.abstention_reasons or any(value is not None for value in forecasts):
                raise ContractError("abstention requires reasons and no forecast values")
            return
        if recorded >= entry:
            raise ContractError("active prediction must be recorded before planned entry")
        if self.abstention_reasons or any(value is None for value in forecasts):
            raise ContractError("active prediction requires all forecasts and no reasons")
        if any(
            isinstance(value, bool) or not isinstance(value, (int, float))
            for value in forecasts
        ):
            raise ContractError("active forecasts must be numeric scalars")
        assert self.expected_return is not None
        assert self.uncertainty is not None
        probabilities = (
            self.probability_up,
            self.probability_down,
            self.probability_neutral,
        )
        assert all(value is not None for value in probabilities)
        if (
            not math.isfinite(self.expected_return)
            or not math.isfinite(self.uncertainty)
            or self.uncertainty < 0
        ):
            raise ContractError("invalid expected return or uncertainty")
        if any(not 0 <= float(value) <= 1 for value in probabilities):
            raise ContractError("probabilities must be between zero and one")
        if abs(sum(float(value) for value in probabilities) - 1.0) > 1e-9:
            raise ContractError("probabilities must sum to one")


def prediction_id_for(
    *,
    bundle_id: str,
    actual: ActualContractIdentity,
    decision_at: datetime,
    recorded_at: datetime,
    source_release_id: str,
    source_release_receipt_id: str,
    economics_record_id: str,
    feature_row_id: str,
    planned_entry_at: datetime,
    label_unlock_at: datetime,
    bundle_classification: str = "SYNTHETIC_MECHANICS_ONLY",
    candidate_provenance_id: str | None = None,
    production_eligible: bool = False,
) -> str:
    return sha256_json(
        {
            "actual_identity_hash": actual.identity_hash,
            "bundle_id": bundle_id,
            "bundle_classification": bundle_classification,
            "candidate_provenance_id": candidate_provenance_id,
            "decision_at": decision_at.isoformat(),
            "economics_record_id": economics_record_id,
            "feature_row_id": feature_row_id,
            "label_unlock_at": label_unlock_at.isoformat(),
            "planned_entry_at": planned_entry_at.isoformat(),
            "production_eligible": production_eligible,
            "recorded_at": recorded_at.isoformat(),
            "source_release_id": source_release_id,
            "source_release_receipt_id": source_release_receipt_id,
        }
    )
