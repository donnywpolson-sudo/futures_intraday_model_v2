"""Causal, role-specific futures interval identity and coverage contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
import hashlib
import json
from typing import Mapping

from .contracts import ResearchContractError, require_sha256


class IntervalRole(str, Enum):
    FEATURE = "FEATURE"
    LABEL = "LABEL"
    RETURN = "RETURN"
    PNL = "PNL"


class IntervalResolutionStatus(str, Enum):
    PRE_DECISION_INELIGIBLE = "PRE_DECISION_INELIGIBLE"
    POST_DECISION_UNRESOLVED = "POST_DECISION_UNRESOLVED"
    RESOLVED = "RESOLVED"


_REQUIRED_ROLES = tuple(IntervalRole)
_POST_DECISION_ROLES = (
    IntervalRole.LABEL,
    IntervalRole.RETURN,
    IntervalRole.PNL,
)


def _utc(value: datetime, *, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ResearchContractError(f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ResearchContractError(f"{name} must be UTC")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class IntervalIdentitySegment:
    start_at: datetime
    end_at: datetime
    instrument_id_date_utc: date
    actual_contract_id: str
    economics_record_id: str

    def validate(self) -> None:
        start = _utc(self.start_at, name="segment.start_at")
        end = _utc(self.end_at, name="segment.end_at")
        if start >= end:
            raise ResearchContractError("identity segments are half-open with start < end")
        if not isinstance(self.instrument_id_date_utc, date) or isinstance(
            self.instrument_id_date_utc, datetime
        ):
            raise ResearchContractError("instrument_id_date_utc must be a date")
        if start.date() != self.instrument_id_date_utc:
            raise ResearchContractError("segment starts on the wrong instrument-ID UTC date")
        if (end - timedelta(microseconds=1)).date() != self.instrument_id_date_utc:
            raise ResearchContractError(
                "segment crosses an unresolved UTC-date mapping boundary"
            )
        require_sha256(self.actual_contract_id, name="segment.actual_contract_id")
        require_sha256(self.economics_record_id, name="segment.economics_record_id")

    def as_dict(self) -> dict[str, str]:
        return {
            "actual_contract_id": self.actual_contract_id,
            "economics_record_id": self.economics_record_id,
            "end_at": self.end_at.isoformat(),
            "instrument_id_date_utc": self.instrument_id_date_utc.isoformat(),
            "start_at": self.start_at.isoformat(),
        }


@dataclass(frozen=True)
class RoleIntervalWindow:
    role: IntervalRole
    start_at: datetime
    end_at: datetime

    def validate(self) -> None:
        if not isinstance(self.role, IntervalRole):
            raise ResearchContractError("role interval has an invalid role")
        start = _utc(self.start_at, name=f"{self.role.value}.start_at")
        end = _utc(self.end_at, name=f"{self.role.value}.end_at")
        if start >= end:
            raise ResearchContractError("role intervals are half-open with start < end")

    def as_dict(self) -> dict[str, str]:
        return {
            "end_at": self.end_at.isoformat(),
            "role": self.role.value,
            "start_at": self.start_at.isoformat(),
        }


def _causal_role_reasons(
    windows: Mapping[IntervalRole, RoleIntervalWindow],
    *,
    decision_at: datetime,
    planned_entry_at: datetime,
) -> tuple[str, ...]:
    decision = _utc(decision_at, name="decision_at")
    entry = _utc(planned_entry_at, name="planned_entry_at")
    reasons: list[str] = []
    if decision >= entry:
        reasons.append("ENTRY_NOT_STRICTLY_AFTER_DECISION")
    feature = windows[IntervalRole.FEATURE]
    label = windows[IntervalRole.LABEL]
    returns = windows[IntervalRole.RETURN]
    pnl = windows[IntervalRole.PNL]
    if feature.end_at > decision:
        reasons.append("FEATURE_USES_POST_DECISION_DATA")
    if label.start_at != entry:
        reasons.append("LABEL_START_DOES_NOT_MATCH_ENTRY")
    if returns.start_at != entry or pnl.start_at != entry:
        reasons.append("EXECUTION_START_DOES_NOT_MATCH_ENTRY")
    if returns.end_at != label.end_at or pnl.end_at != label.end_at:
        reasons.append("EXECUTION_END_DOES_NOT_MATCH_LABEL")
    if returns.start_at != pnl.start_at or returns.end_at != pnl.end_at:
        reasons.append("EXECUTION_INTERVAL_MISMATCH")
    return tuple(sorted(set(reasons)))


@dataclass(frozen=True)
class DecisionTimeIdentityCharter:
    """Roll-safe execution identity declared using information known by decision."""

    declared_at: datetime
    decision_at: datetime
    planned_entry_at: datetime
    role_windows: tuple[RoleIntervalWindow, ...]
    declared_execution_actual_contract_id: str
    declared_execution_economics_record_id: str
    roll_safety_policy_receipt_sha256: str
    horizon_declared_roll_safe: bool

    def validate(self) -> None:
        _utc(self.declared_at, name="charter.declared_at")
        _utc(self.decision_at, name="charter.decision_at")
        _utc(self.planned_entry_at, name="charter.planned_entry_at")
        if tuple(window.role for window in self.role_windows) != _REQUIRED_ROLES:
            raise ResearchContractError("charter interval roles are invalid")
        for window in self.role_windows:
            window.validate()
        require_sha256(
            self.declared_execution_actual_contract_id,
            name="charter.declared_execution_actual_contract_id",
        )
        require_sha256(
            self.declared_execution_economics_record_id,
            name="charter.declared_execution_economics_record_id",
        )
        require_sha256(
            self.roll_safety_policy_receipt_sha256,
            name="charter.roll_safety_policy_receipt_sha256",
        )
        if type(self.horizon_declared_roll_safe) is not bool:
            raise ResearchContractError("horizon_declared_roll_safe must be exact bool")

    def pre_decision_reasons(self) -> tuple[str, ...]:
        self.validate()
        reasons = list(
            _causal_role_reasons(
                {window.role: window for window in self.role_windows},
                decision_at=self.decision_at,
                planned_entry_at=self.planned_entry_at,
            )
        )
        if self.declared_at > self.decision_at:
            reasons.append("IDENTITY_CHARTER_DECLARED_AFTER_DECISION")
        if not self.horizon_declared_roll_safe:
            reasons.append("HORIZON_NOT_DECLARED_ROLL_SAFE")
        return tuple(sorted(set(reasons)))

    def window_for(self, role: IntervalRole) -> RoleIntervalWindow:
        self.validate()
        for window in self.role_windows:
            if window.role is role:
                return window
        raise ResearchContractError("identity charter lacks requested role")

    def as_dict(self) -> dict[str, object]:
        return {
            "declared_at": self.declared_at.isoformat(),
            "declared_execution_actual_contract_id": (
                self.declared_execution_actual_contract_id
            ),
            "declared_execution_economics_record_id": (
                self.declared_execution_economics_record_id
            ),
            "decision_at": self.decision_at.isoformat(),
            "horizon_declared_roll_safe": self.horizon_declared_roll_safe,
            "planned_entry_at": self.planned_entry_at.isoformat(),
            "role_windows": [window.as_dict() for window in self.role_windows],
            "roll_safety_policy_receipt_sha256": (
                self.roll_safety_policy_receipt_sha256
            ),
        }


@dataclass(frozen=True)
class VerifiedRoleIntervalIdentity:
    role: IntervalRole
    window: RoleIntervalWindow
    actual_contract_id: str
    economics_record_id: str

    def validate(self) -> None:
        if not isinstance(self.role, IntervalRole) or self.window.role is not self.role:
            raise ResearchContractError("verified role identity is mismatched")
        self.window.validate()
        require_sha256(self.actual_contract_id, name="role.actual_contract_id")
        require_sha256(self.economics_record_id, name="role.economics_record_id")

    def as_dict(self) -> dict[str, object]:
        return {
            "actual_contract_id": self.actual_contract_id,
            "economics_record_id": self.economics_record_id,
            "window": self.window.as_dict(),
        }


@dataclass(frozen=True)
class VerifiedIntervalIdentity:
    charter: DecisionTimeIdentityCharter
    role_identities: tuple[VerifiedRoleIntervalIdentity, ...]
    evidence_sha256: str
    binding_id: str

    def validate(self) -> None:
        if not isinstance(self.charter, DecisionTimeIdentityCharter):
            raise ResearchContractError("interval binding lacks a decision-time charter")
        self.charter.validate()
        if self.charter.pre_decision_reasons():
            raise ResearchContractError("interval binding charter is not decision-eligible")
        if tuple(identity.role for identity in self.role_identities) != _REQUIRED_ROLES:
            raise ResearchContractError("interval binding roles are invalid")
        for identity in self.role_identities:
            identity.validate()
            if identity.window != self.charter.window_for(identity.role):
                raise ResearchContractError("role identity window differs from charter")
        for role in _POST_DECISION_ROLES:
            identity = self._identity_for_unchecked(role)
            if (
                identity.actual_contract_id
                != self.charter.declared_execution_actual_contract_id
                or identity.economics_record_id
                != self.charter.declared_execution_economics_record_id
            ):
                raise ResearchContractError(
                    "execution identity differs from the decision-time declaration"
                )
        require_sha256(self.evidence_sha256, name="binding.evidence_sha256")
        core = {
            "charter": self.charter.as_dict(),
            "evidence_sha256": self.evidence_sha256,
            "role_identities": [
                identity.as_dict() for identity in self.role_identities
            ],
        }
        expected = hashlib.sha256(
            json.dumps(core, sort_keys=True, separators=(",", ":")).encode("ascii")
        ).hexdigest()
        if expected != self.binding_id:
            raise ResearchContractError("interval identity binding hash is invalid")

    @property
    def decision_at(self) -> datetime:
        return self.charter.decision_at

    @property
    def planned_entry_at(self) -> datetime:
        return self.charter.planned_entry_at

    @property
    def role_windows(self) -> tuple[RoleIntervalWindow, ...]:
        return self.charter.role_windows

    def window_for(self, role: IntervalRole) -> RoleIntervalWindow:
        self.validate()
        return self.charter.window_for(role)

    def _identity_for_unchecked(
        self, role: IntervalRole
    ) -> VerifiedRoleIntervalIdentity:
        for identity in self.role_identities:
            if identity.role is role:
                return identity
        raise ResearchContractError("interval identity binding lacks requested role")

    def identity_for(self, role: IntervalRole) -> VerifiedRoleIntervalIdentity:
        self.validate()
        return self._identity_for_unchecked(role)


@dataclass(frozen=True)
class IntervalIdentityDecision:
    status: IntervalResolutionStatus
    failure_reasons: tuple[str, ...]
    binding: VerifiedIntervalIdentity | None

    def __post_init__(self) -> None:
        if not isinstance(self.status, IntervalResolutionStatus):
            raise ResearchContractError("interval resolution status is invalid")
        if self.failure_reasons != tuple(sorted(set(self.failure_reasons))):
            raise ResearchContractError("interval failure reasons must be unique and sorted")
        if self.status is IntervalResolutionStatus.RESOLVED:
            if self.failure_reasons or self.binding is None:
                raise ResearchContractError("resolved interval decision is inconsistent")
            self.binding.validate()
        elif not self.failure_reasons or self.binding is not None:
            raise ResearchContractError("unresolved interval decision is inconsistent")

    @property
    def eligible(self) -> bool:
        """Whether identity is fully resolved; never use this as coverage denominator."""

        return self.status is IntervalResolutionStatus.RESOLVED

    @property
    def decision_eligible(self) -> bool:
        return self.status is not IntervalResolutionStatus.PRE_DECISION_INELIGIBLE

    @property
    def prediction_in_coverage_denominator(self) -> bool:
        return self.decision_eligible

    @property
    def abstention_reasons(self) -> tuple[str, ...]:
        """Compatibility alias; post-decision failures are unresolved, not abstentions."""

        return self.failure_reasons


def coverage_denominator_indices(
    decisions: tuple[IntervalIdentityDecision, ...],
) -> tuple[int, ...]:
    """Include every issued prediction, including unresolved future identity paths."""

    result: list[int] = []
    for index, decision in enumerate(decisions):
        if not isinstance(decision, IntervalIdentityDecision):
            raise ResearchContractError("coverage inputs must be interval decisions")
        if decision.prediction_in_coverage_denominator:
            result.append(index)
    return tuple(result)


def _assess_role_segments(
    role: IntervalRole,
    window: RoleIntervalWindow,
    segments: tuple[IntervalIdentitySegment, ...],
    evidence_payload: dict[str, dict[str, object]],
) -> tuple[VerifiedRoleIntervalIdentity | None, tuple[str, ...]]:
    prefix = role.value
    evidence_payload[prefix] = {"segments": [], "window": window.as_dict()}
    reasons: list[str] = []
    if not segments:
        return None, (f"{prefix}_INCOMPLETE_INTERVAL_IDENTITY_COVERAGE",)
    cursor = window.start_at
    actual_ids: set[str] = set()
    economics_ids: set[str] = set()
    for segment in segments:
        if not isinstance(segment, IntervalIdentitySegment):
            return None, (f"{prefix}_INVALID_INTERVAL_IDENTITY_EVIDENCE",)
        try:
            segment.validate()
        except ResearchContractError:
            return None, (f"{prefix}_INVALID_INTERVAL_IDENTITY_EVIDENCE",)
        payload = evidence_payload[prefix]["segments"]
        assert isinstance(payload, list)
        payload.append(segment.as_dict())
        if segment.start_at != cursor or segment.end_at > window.end_at:
            reasons.append(f"{prefix}_INCOMPLETE_INTERVAL_IDENTITY_COVERAGE")
        cursor = segment.end_at
        actual_ids.add(segment.actual_contract_id)
        economics_ids.add(segment.economics_record_id)
    if cursor != window.end_at:
        reasons.append(f"{prefix}_INCOMPLETE_INTERVAL_IDENTITY_COVERAGE")
    if len(actual_ids) != 1:
        reasons.append(f"{prefix}_ACTUAL_CONTRACT_CHANGED_WITHIN_INTERVAL")
    if len(economics_ids) != 1:
        reasons.append(f"{prefix}_ECONOMICS_CHANGED_WITHIN_INTERVAL")
    unique_reasons = tuple(sorted(set(reasons)))
    if unique_reasons:
        return None, unique_reasons
    identity = VerifiedRoleIntervalIdentity(
        role,
        window,
        next(iter(actual_ids)),
        next(iter(economics_ids)),
    )
    identity.validate()
    return identity, ()


def assess_interval_identity_bundle(
    segments_by_role: Mapping[IntervalRole, tuple[IntervalIdentitySegment, ...]],
    *,
    charter: DecisionTimeIdentityCharter,
) -> IntervalIdentityDecision:
    """Resolve role windows without allowing future identity failures to drop samples."""

    if not isinstance(segments_by_role, Mapping):
        raise ResearchContractError("segments_by_role must be a mapping")
    if not isinstance(charter, DecisionTimeIdentityCharter):
        raise ResearchContractError("a decision-time identity charter is required")
    charter.validate()
    unknown = set(segments_by_role).difference(_REQUIRED_ROLES)
    if unknown:
        raise ResearchContractError("interval evidence contains an unknown role")
    charter_reasons = charter.pre_decision_reasons()
    if charter_reasons:
        return IntervalIdentityDecision(
            IntervalResolutionStatus.PRE_DECISION_INELIGIBLE,
            charter_reasons,
            None,
        )

    evidence_payload: dict[str, dict[str, object]] = {}
    feature_identity, feature_reasons = _assess_role_segments(
        IntervalRole.FEATURE,
        charter.window_for(IntervalRole.FEATURE),
        segments_by_role.get(IntervalRole.FEATURE, ()),
        evidence_payload,
    )
    if feature_reasons:
        return IntervalIdentityDecision(
            IntervalResolutionStatus.PRE_DECISION_INELIGIBLE,
            feature_reasons,
            None,
        )
    assert feature_identity is not None

    role_identities: dict[IntervalRole, VerifiedRoleIntervalIdentity] = {
        IntervalRole.FEATURE: feature_identity
    }
    post_reasons: list[str] = []
    for role in _POST_DECISION_ROLES:
        identity, role_reasons = _assess_role_segments(
            role,
            charter.window_for(role),
            segments_by_role.get(role, ()),
            evidence_payload,
        )
        post_reasons.extend(role_reasons)
        if identity is None:
            continue
        role_identities[role] = identity
        if (
            identity.actual_contract_id
            != charter.declared_execution_actual_contract_id
        ):
            post_reasons.append(
                f"{role.value}_ACTUAL_CONTRACT_MISMATCH_WITH_DECLARATION"
            )
        if (
            identity.economics_record_id
            != charter.declared_execution_economics_record_id
        ):
            post_reasons.append(
                f"{role.value}_ECONOMICS_MISMATCH_WITH_DECLARATION"
            )
    unique_post_reasons = tuple(sorted(set(post_reasons)))
    if unique_post_reasons:
        return IntervalIdentityDecision(
            IntervalResolutionStatus.POST_DECISION_UNRESOLVED,
            unique_post_reasons,
            None,
        )

    ordered_identities = tuple(role_identities[role] for role in _REQUIRED_ROLES)
    evidence_sha = hashlib.sha256(
        json.dumps(evidence_payload, sort_keys=True, separators=(",", ":")).encode(
            "ascii"
        )
    ).hexdigest()
    core = {
        "charter": charter.as_dict(),
        "evidence_sha256": evidence_sha,
        "role_identities": [identity.as_dict() for identity in ordered_identities],
    }
    binding = VerifiedIntervalIdentity(
        charter,
        ordered_identities,
        evidence_sha,
        hashlib.sha256(
            json.dumps(core, sort_keys=True, separators=(",", ":")).encode("ascii")
        ).hexdigest(),
    )
    binding.validate()
    return IntervalIdentityDecision(IntervalResolutionStatus.RESOLVED, (), binding)
