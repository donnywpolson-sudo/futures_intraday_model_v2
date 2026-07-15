"""Global counted-trial governance and verified evaluation firewall."""

from __future__ import annotations

import json
import hashlib
import hmac
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path

from .boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from .canonical import (
    assert_plain_file,
    canonical_bytes,
    contained_path,
    fsync_directory,
    is_linklike,
    sha256_file,
    sha256_json,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .clock import ProductionClock, TrustedClock, require_trusted_clock
from .locking import FileLease
from .release import VerifiedReleaseReceipt
from .time_contracts import require_utc


EVENT_NAME = re.compile(r"^(?P<sequence>\d{20})_(?P<hash>[0-9a-f]{64})\.json$")
_TRIAL_EVENT_ROOT = Path("state/trial_events")
_TRIAL_LOCK_PATH = Path("state/locks/trial_events.lock")
_TRIAL_HEAD_PATH = Path("state/trial_heads/head.json")
_TRIAL_REGISTRY_ROOT = Path("state/trial_registry")
_EVALUATION_ROOT = Path("data/vault/releases")


def _canonical_active_path(
    boundary: RepoBoundary, path: Path, relative: Path, *, purpose: str
) -> Path:
    candidate = boundary.assert_active_path(path, purpose=purpose)
    expected = (boundary.active_root / relative).resolve(strict=False)
    if candidate != expected:
        raise UnauthorizedOperation(f"{purpose} must use its canonical repository path")
    return candidate


class EvaluationClassification(str, Enum):
    SYNTHETIC_MECHANICS_ONLY = "SYNTHETIC_MECHANICS_ONLY"
    REAL_HISTORY_DISCOVERY = "REAL_HISTORY_DISCOVERY"


class EvaluationReleaseRole(str, Enum):
    SYNTHETIC_MECHANICS = "SYNTHETIC_MECHANICS"
    TRAINING = "TRAINING"
    INNER_VALIDATION = "INNER_VALIDATION"
    OUTER_SCREEN = "OUTER_SCREEN"
    FINAL_HOLDOUT = "FINAL_HOLDOUT"
    ECONOMICS = "ECONOMICS"


_HOLDOUT_ROLE = EvaluationReleaseRole.FINAL_HOLDOUT
_REAL_HISTORY_DATA_ROLES = {
    EvaluationReleaseRole.TRAINING,
    EvaluationReleaseRole.INNER_VALIDATION,
    EvaluationReleaseRole.OUTER_SCREEN,
    EvaluationReleaseRole.FINAL_HOLDOUT,
}


@dataclass(frozen=True)
class LegacyCensusReceipt:
    release_receipt: VerifiedReleaseReceipt
    status: str
    observed_attempt_floor: int
    preregistered_penalty_count: int
    census_sha256: str
    rationale_sha256: str
    source_evidence_sha256: str
    receipt_id: str
    boundary: RepoBoundary
    exact_count_state: str | None = None
    trusted_gate: bool | None = None
    source_snapshot_id: str | None = None
    unresolved_reference_count: int | None = None

    @property
    def counting_attempt_count(self) -> int:
        """Conservative count used mechanically; never represented as exact history."""

        return self.preregistered_penalty_count

    @classmethod
    def from_release(
        cls, release_receipt: VerifiedReleaseReceipt, boundary: RepoBoundary
    ) -> "LegacyCensusReceipt":
        manifest = release_receipt.verify(boundary)
        if manifest.release_kind != "legacy_trial_census" or {
            entry.path for entry in manifest.files
        } != {"legacy_census.json"}:
            raise IntegrityError("legacy census release content is invalid")
        path = boundary.active_root / release_receipt.relative_root / "legacy_census.json"
        try:
            raw = path.read_bytes()
            payload = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError("legacy census JSON is invalid") from exc
        if manifest.schema_version == "2.0.0":
            from .legacy_trial_census import (
                INDETERMINATE_COUNT_STATE,
                LEGACY_CENSUS_SCHEMA_VERSION,
                UNRESOLVED_STATUS,
                validate_legacy_trial_census_payload,
            )

            if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
                raise IntegrityError("canonical legacy census JSON is not canonical")
            canonical = validate_legacy_trial_census_payload(payload)
            expected_metadata = {
                "census_sha256",
                "exact_count_state",
                "source_evidence_sha256",
                "source_snapshot_id",
                "status",
                "trusted_gate",
            }
            source_snapshot_id = str(canonical["source_snapshot_id"])
            if (
                manifest.schema_version != LEGACY_CENSUS_SCHEMA_VERSION
                or set(manifest.metadata) != expected_metadata
                or manifest.source_release_ids != (source_snapshot_id,)
                or manifest.metadata["census_sha256"]
                != canonical["census_sha256"]
                or manifest.metadata["exact_count_state"]
                != INDETERMINATE_COUNT_STATE
                or manifest.metadata["source_evidence_sha256"]
                != canonical["source_evidence_sha256"]
                or manifest.metadata["source_snapshot_id"] != source_snapshot_id
                or manifest.metadata["status"] != UNRESOLVED_STATUS
                or manifest.metadata["trusted_gate"] is not False
            ):
                raise IntegrityError("canonical legacy census release is invalid")
            status = str(canonical["status"])
            observed_floor = canonical["observed_attempt_floor"]
            penalty_count = canonical["preregistered_penalty_count"]
            census_hash = str(canonical["census_sha256"])
            rationale_hash = str(canonical["rationale_sha256"])
            source_hash = str(canonical["source_evidence_sha256"])
            exact_count_state = str(canonical["exact_count_state"])
            trusted_gate = canonical["trusted_gate"]
            unresolved = canonical["unresolved_references"]
            if (
                type(observed_floor) is not int
                or type(penalty_count) is not int
                or type(trusted_gate) is not bool
                or not isinstance(unresolved, list)
            ):
                raise IntegrityError("canonical legacy census fields are invalid")
            core = {
                "census_sha256": census_hash,
                "exact_count_state": exact_count_state,
                "observed_attempt_floor": observed_floor,
                "preregistered_penalty_count": penalty_count,
                "rationale_sha256": rationale_hash,
                "release_receipt_id": release_receipt.receipt_id,
                "source_evidence_sha256": source_hash,
                "source_snapshot_id": source_snapshot_id,
                "status": status,
                "trusted_gate": trusted_gate,
                "unresolved_reference_count": len(unresolved),
            }
            return cls(
                release_receipt=release_receipt,
                status=status,
                observed_attempt_floor=observed_floor,
                preregistered_penalty_count=penalty_count,
                census_sha256=census_hash,
                rationale_sha256=rationale_hash,
                source_evidence_sha256=source_hash,
                receipt_id=sha256_json(core),
                boundary=boundary,
                exact_count_state=exact_count_state,
                trusted_gate=trusted_gate,
                source_snapshot_id=source_snapshot_id,
                unresolved_reference_count=len(unresolved),
            )
        if not isinstance(payload, dict) or set(payload) != {
            "census_sha256",
            "observed_attempt_floor",
            "preregistered_penalty_count",
            "rationale_sha256",
            "source_evidence_sha256",
            "status",
        }:
            raise IntegrityError("legacy census schema is invalid")
        status = str(payload["status"])
        observed_floor = payload["observed_attempt_floor"]
        penalty_count = payload["preregistered_penalty_count"]
        census_hash = str(payload["census_sha256"])
        rationale_hash = str(payload["rationale_sha256"])
        source_hash = str(payload["source_evidence_sha256"])
        if (
            status not in {
                "CONSERVATIVE_PENALTY_PREREGISTERED",
                "INVALID_TRIAL_CENSUS_UNRESOLVED",
            }
            or isinstance(observed_floor, bool)
            or not isinstance(observed_floor, int)
            or observed_floor < 0
            or isinstance(penalty_count, bool)
            or not isinstance(penalty_count, int)
            or penalty_count < 0
            or any(
                re.fullmatch(r"[0-9a-f]{64}", value) is None
                for value in (census_hash, rationale_hash, source_hash)
            )
            or (
                status == "CONSERVATIVE_PENALTY_PREREGISTERED"
                and penalty_count <= observed_floor
            )
            or (
                status == "INVALID_TRIAL_CENSUS_UNRESOLVED"
                and penalty_count != 0
            )
        ):
            raise IntegrityError("legacy census fields are invalid")
        core = {
            "census_sha256": census_hash,
            "observed_attempt_floor": observed_floor,
            "preregistered_penalty_count": penalty_count,
            "rationale_sha256": rationale_hash,
            "release_receipt_id": release_receipt.receipt_id,
            "source_evidence_sha256": source_hash,
            "status": status,
        }
        return cls(
            release_receipt=release_receipt,
            status=status,
            observed_attempt_floor=observed_floor,
            preregistered_penalty_count=penalty_count,
            census_sha256=census_hash,
            rationale_sha256=rationale_hash,
            source_evidence_sha256=source_hash,
            receipt_id=sha256_json(core),
            boundary=boundary,
        )

    def verify(self) -> None:
        rebuilt = type(self).from_release(self.release_receipt, self.boundary)
        if rebuilt != self:
            raise IntegrityError("legacy census receipt changed after verification")


@dataclass(frozen=True)
class ExperimentCharter:
    hypothesis_id: str
    data_release_receipts: tuple[VerifiedReleaseReceipt, ...]
    release_role_bindings: tuple[tuple[str, EvaluationReleaseRole], ...]
    economics_release_receipt_ids: tuple[str, ...]
    feature_policy_hash: str
    target_policy_hash: str
    decision_rule_hash: str
    fold_policy_hash: str
    cost_policy_hash: str
    primary_metric: str
    benchmark_id: str
    minimum_effect: float
    minimum_effect_unit: str
    multiplicity_family_id: str
    multiplicity_family_rule_hash: str
    holdout_policy_hash: str
    outcome_unlock_at: datetime
    evaluation_classification: EvaluationClassification

    def __post_init__(self) -> None:
        require_utc(self.outcome_unlock_at, "outcome_unlock_at")
        required = (
            self.hypothesis_id,
            self.feature_policy_hash,
            self.target_policy_hash,
            self.decision_rule_hash,
            self.fold_policy_hash,
            self.cost_policy_hash,
            self.primary_metric,
            self.benchmark_id,
            self.minimum_effect_unit,
            self.multiplicity_family_id,
        )
        if (
            any(type(item) is not str or not item for item in required)
            or type(self.data_release_receipts) is not tuple
            or not self.data_release_receipts
            or any(
                type(item) is not VerifiedReleaseReceipt
                for item in self.data_release_receipts
            )
        ):
            raise ContractError("charter fields and verified releases must be explicit")
        release_ids = tuple(item.release_id for item in self.data_release_receipts)
        if release_ids != tuple(sorted(set(release_ids))):
            raise ContractError("charter release receipts must be unique and sorted")
        if (
            not isinstance(self.release_role_bindings, tuple)
            or tuple(item[0] for item in self.release_role_bindings) != release_ids
            or any(
                not isinstance(item, tuple)
                or len(item) != 2
                or type(item[0]) is not str
                or not isinstance(item[1], EvaluationReleaseRole)
                for item in self.release_role_bindings
            )
        ):
            raise ContractError(
                "every charter release requires one exact immutable evaluation role"
            )
        if (
            type(self.economics_release_receipt_ids) is not tuple
            or any(
                type(item) is not str
                for item in self.economics_release_receipt_ids
            )
            or self.economics_release_receipt_ids
            != tuple(sorted(set(self.economics_release_receipt_ids)))
            or not set(self.economics_release_receipt_ids).issubset(
                {item.receipt_id for item in self.data_release_receipts}
            )
        ):
            raise ContractError("economics receipt bindings are not verified charter releases")
        hashes = (
            self.feature_policy_hash,
            self.target_policy_hash,
            self.decision_rule_hash,
            self.fold_policy_hash,
            self.cost_policy_hash,
            self.multiplicity_family_rule_hash,
            self.holdout_policy_hash,
        )
        if any(
            type(value) is not str
            or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in hashes
        ):
            raise ContractError("charter policies must be exact SHA-256 values")
        if (
            type(self.minimum_effect) is not float
            or not math.isfinite(self.minimum_effect)
            or self.minimum_effect <= 0
        ):
            raise ContractError("minimum effect must be finite and strictly positive")
        if (
            not self.minimum_effect_unit.isidentifier()
            or self.minimum_effect_unit != self.minimum_effect_unit.upper()
        ):
            raise ContractError("minimum effect unit must be an uppercase identifier")
        if type(self.evaluation_classification) is not EvaluationClassification:
            raise ContractError("evaluation classification must use the declared enum")
        if (
            self.evaluation_classification
            is EvaluationClassification.REAL_HISTORY_DISCOVERY
            and not self.economics_release_receipt_ids
        ):
            raise ContractError("P&L-capable real-history trials require verified economics")
        if (
            self.evaluation_classification
            is EvaluationClassification.SYNTHETIC_MECHANICS_ONLY
            and self.economics_release_receipt_ids
        ):
            raise ContractError("synthetic mechanics cannot claim verified P&L economics")
        roles = dict(self.release_role_bindings)
        for receipt in self.data_release_receipts:
            role = roles[receipt.release_id]
            if receipt.receipt_id in self.economics_release_receipt_ids:
                if role is not EvaluationReleaseRole.ECONOMICS:
                    raise ContractError("economics releases require the ECONOMICS role")
            elif self.evaluation_classification is EvaluationClassification.SYNTHETIC_MECHANICS_ONLY:
                if role is not EvaluationReleaseRole.SYNTHETIC_MECHANICS:
                    raise ContractError("synthetic releases require the synthetic role")
            elif role not in _REAL_HISTORY_DATA_ROLES:
                raise ContractError("real-history data has an invalid evaluation role")

    def core(self) -> dict[str, object]:
        return {
            "benchmark_id": self.benchmark_id,
            "cost_policy_hash": self.cost_policy_hash,
            "data_release_receipts": [
                receipt.as_dict() for receipt in self.data_release_receipts
            ],
            "release_role_bindings": [
                [release_id, role.value]
                for release_id, role in self.release_role_bindings
            ],
            "decision_rule_hash": self.decision_rule_hash,
            "economics_release_receipt_ids": list(
                self.economics_release_receipt_ids
            ),
            "evaluation_classification": self.evaluation_classification.value,
            "feature_policy_hash": self.feature_policy_hash,
            "fold_policy_hash": self.fold_policy_hash,
            "holdout_policy_hash": self.holdout_policy_hash,
            "hypothesis_id": self.hypothesis_id,
            "minimum_effect": self.minimum_effect,
            "minimum_effect_unit": self.minimum_effect_unit,
            "multiplicity_family_id": self.multiplicity_family_id,
            "multiplicity_family_rule_hash": self.multiplicity_family_rule_hash,
            "outcome_unlock_at": self.outcome_unlock_at.isoformat(),
            "primary_metric": self.primary_metric,
            "target_policy_hash": self.target_policy_hash,
        }

    @property
    def charter_id(self) -> str:
        return sha256_json(self.core())

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "ExperimentCharter":
        expected = {
            "benchmark_id",
            "cost_policy_hash",
            "data_release_receipts",
            "decision_rule_hash",
            "economics_release_receipt_ids",
            "evaluation_classification",
            "feature_policy_hash",
            "fold_policy_hash",
            "holdout_policy_hash",
            "hypothesis_id",
            "minimum_effect",
            "minimum_effect_unit",
            "multiplicity_family_id",
            "multiplicity_family_rule_hash",
            "outcome_unlock_at",
            "primary_metric",
            "release_role_bindings",
            "target_policy_hash",
        }
        if (
            set(payload) != expected
            or not isinstance(payload.get("data_release_receipts"), list)
            or not isinstance(payload.get("release_role_bindings"), list)
        ):
            raise IntegrityError("registered charter schema is invalid")
        string_fields = expected.difference(
            {
                "data_release_receipts",
                "economics_release_receipt_ids",
                "minimum_effect",
                "release_role_bindings",
            }
        )
        if (
            any(type(payload[name]) is not str for name in string_fields)
            or not isinstance(payload["economics_release_receipt_ids"], list)
            or any(
                type(item) is not str
                for item in payload["economics_release_receipt_ids"]
            )
            or isinstance(payload["minimum_effect"], bool)
            or not isinstance(payload["minimum_effect"], (int, float))
            or any(
                not isinstance(item, list)
                or len(item) != 2
                or type(item[0]) is not str
                or type(item[1]) is not str
                for item in payload["release_role_bindings"]
            )
        ):
            raise IntegrityError("registered charter field types are invalid")
        try:
            result = cls(
                hypothesis_id=payload["hypothesis_id"],
                data_release_receipts=tuple(
                    VerifiedReleaseReceipt.from_dict(item)
                    for item in payload["data_release_receipts"]
                ),
                release_role_bindings=tuple(
                    (item[0], EvaluationReleaseRole(item[1]))
                    for item in payload["release_role_bindings"]
                    if isinstance(item, list) and len(item) == 2
                ),
                economics_release_receipt_ids=tuple(
                    payload["economics_release_receipt_ids"]  # type: ignore[arg-type]
                ),
                feature_policy_hash=payload["feature_policy_hash"],
                target_policy_hash=payload["target_policy_hash"],
                decision_rule_hash=payload["decision_rule_hash"],
                fold_policy_hash=payload["fold_policy_hash"],
                cost_policy_hash=payload["cost_policy_hash"],
                primary_metric=payload["primary_metric"],
                benchmark_id=payload["benchmark_id"],
                minimum_effect=payload["minimum_effect"],
                minimum_effect_unit=payload["minimum_effect_unit"],
                multiplicity_family_id=payload["multiplicity_family_id"],
                multiplicity_family_rule_hash=payload[
                    "multiplicity_family_rule_hash"
                ],
                holdout_policy_hash=payload["holdout_policy_hash"],
                outcome_unlock_at=datetime.fromisoformat(payload["outcome_unlock_at"]),
                evaluation_classification=EvaluationClassification(
                    payload["evaluation_classification"]
                ),
            )
        except (TypeError, ValueError, ContractError, IntegrityError) as exc:
            raise IntegrityError("registered charter is invalid") from exc
        if result.core() != payload:
            raise IntegrityError("registered charter is not canonical")
        return result


class TrialEventLedger:
    def __init__(
        self,
        root: Path,
        lock_path: Path,
        *,
        boundary: RepoBoundary,
        operation_receipt: OperationReceipt,
        clock: TrustedClock,
    ) -> None:
        operation_receipt.verify(boundary, operation="REGISTER_TRIAL")
        self.boundary = boundary
        self.operation_receipt = operation_receipt
        self.clock = require_trusted_clock(
            clock,
            boundary=boundary,
            operation_receipt=operation_receipt,
            allow_synthetic=False,
        )
        if type(self.clock) is not ProductionClock:
            raise ContractError("trial governance requires a production wall clock")
        self.root = _canonical_active_path(
            boundary, root, _TRIAL_EVENT_ROOT, purpose="global trial ledger"
        )
        self.lock_path = _canonical_active_path(
            boundary, lock_path, _TRIAL_LOCK_PATH, purpose="trial ledger lock"
        )
        self.head_path = _canonical_active_path(
            boundary,
            boundary.active_root / _TRIAL_HEAD_PATH,
            _TRIAL_HEAD_PATH,
            purpose="trial ledger head",
        )
        self.ledger_id = sha256_json(
            {
                "event_root": _TRIAL_EVENT_ROOT.as_posix(),
                "head_path": _TRIAL_HEAD_PATH.as_posix(),
                "repository_id": boundary.repository_id,
            }
        )

    def _head_payload(self, sequence: int, event_hash: str) -> dict[str, object]:
        core = {
            "event_hash": event_hash,
            "ledger_id": self.ledger_id,
            "repository_id": self.boundary.repository_id,
            "sequence": sequence,
        }
        return {**core, "head_id": sha256_json(core)}

    def _verify_persistent_head(self, events: list[dict[str, object]]) -> None:
        expected_sequence = len(events)
        expected_hash = str(events[-1]["event_hash"]) if events else "GENESIS"
        if not self.head_path.exists():
            if events:
                raise IntegrityError("trial event ledger lacks its persistent head")
            return
        try:
            assert_plain_file(self.head_path)
            payload = json.loads(self.head_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("trial event persistent head is invalid") from exc
        expected = self._head_payload(expected_sequence, expected_hash)
        if payload != expected:
            raise IntegrityError("trial event ledger differs from its persistent head")

    def _write_persistent_head(self, sequence: int, event_hash: str) -> None:
        payload = self._head_payload(sequence, event_hash)
        self.head_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.head_path.with_name(f".head-{os.urandom(16).hex()}.tmp")
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(descriptor, canonical_bytes(payload) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, self.head_path)
        fsync_directory(self.head_path.parent)

    def _load(self) -> list[dict[str, object]]:
        files: list[Path] = []
        if self.root.exists():
            self.boundary.assert_active_path(self.root, purpose="global trial ledger")
            if not self.root.is_dir() or is_linklike(self.root):
                raise IntegrityError("global trial ledger root is invalid")
            files = sorted(self.root.iterdir())
        previous = "GENESIS"
        events: list[dict[str, object]] = []
        for sequence, path in enumerate(files, start=1):
            if not path.is_file() or is_linklike(path) or EVENT_NAME.fullmatch(path.name) is None:
                raise IntegrityError("global trial ledger contains an unexpected path")
            assert_plain_file(path)
            try:
                event = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise IntegrityError("global trial event JSON is invalid") from exc
            expected = {
                "authorization_receipt_id",
                "charter_id",
                "counted_trial_number",
                "event_hash",
                "event_type",
                "event_time_utc",
                "legacy_census_receipt_id",
                "multiplicity_family_id",
                "multiplicity_family_rule_hash",
                "previous_event_hash",
                "sequence",
            }
            if not isinstance(event, dict) or set(event) != expected:
                raise IntegrityError("global trial event schema is invalid")
            if (
                type(event["sequence"]) is not int
                or type(event["counted_trial_number"]) is not int
                or any(
                    type(event[name]) is not str
                    for name in expected
                    if name not in {"sequence", "counted_trial_number"}
                )
                or event["event_type"]
                not in {"DECLARED", "PRE_OUTCOME_ANCHORED", "FINAL_HOLDOUT_UNLOCKED"}
            ):
                raise IntegrityError("global trial event field types are invalid")
            body = {key: event[key] for key in expected if key != "event_hash"}
            event_time = require_utc(
                datetime.fromisoformat(event["event_time_utc"]),
                "trial_event.event_time_utc",
            )
            if events and event_time < datetime.fromisoformat(
                str(events[-1]["event_time_utc"])
            ):
                raise IntegrityError("global trial event time moved backwards")
            if (
                event["sequence"] != sequence
                or event["previous_event_hash"] != previous
                or sha256_json(body) != event["event_hash"]
                or path.name != f"{sequence:020d}_{event['event_hash']}.json"
            ):
                raise IntegrityError("global trial event chain is invalid")
            previous = event["event_hash"]
            events.append(event)
        self._verify_persistent_head(events)
        return events

    def _append(self, body: dict[str, object]) -> dict[str, object]:
        events = self._load()
        sequence = len(events) + 1
        complete = {
            **body,
            "previous_event_hash": (
                str(events[-1]["event_hash"]) if events else "GENESIS"
            ),
            "sequence": sequence,
        }
        event = {**complete, "event_hash": sha256_json(complete)}
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / f"{sequence:020d}_{event['event_hash']}.json"
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(descriptor, canonical_bytes(event) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        fsync_directory(self.root)
        self._write_persistent_head(sequence, str(event["event_hash"]))
        return event

    def declare(
        self,
        charter: ExperimentCharter,
        census: LegacyCensusReceipt,
        authorization_receipt_id: str,
    ) -> dict[str, object]:
        self.operation_receipt.verify(self.boundary, operation="REGISTER_TRIAL")
        census.verify()
        if re.fullmatch(r"[0-9a-f]{64}", authorization_receipt_id) is None:
            raise ContractError("trial declaration authorization receipt ID is invalid")
        with FileLease(self.lock_path):
            events = self._load()
            declarations = [event for event in events if event["event_type"] == "DECLARED"]
            for event in declarations:
                if event["charter_id"] == charter.charter_id:
                    return event
            event_time = self.clock.now()
            if (
                charter.evaluation_classification
                is EvaluationClassification.REAL_HISTORY_DISCOVERY
                and event_time >= charter.outcome_unlock_at
            ):
                raise UnauthorizedOperation(
                    "real-history trial declaration missed its pre-outcome window"
                )
            counted = 0
            if charter.evaluation_classification is EvaluationClassification.REAL_HISTORY_DISCOVERY:
                counted = max(
                    (int(event["counted_trial_number"]) for event in declarations),
                    default=census.counting_attempt_count,
                ) + 1
            return self._append(
                {
                    "authorization_receipt_id": authorization_receipt_id,
                    "charter_id": charter.charter_id,
                    "counted_trial_number": counted,
                    "event_type": "DECLARED",
                    "event_time_utc": event_time.isoformat(),
                    "legacy_census_receipt_id": census.receipt_id,
                    "multiplicity_family_id": charter.multiplicity_family_id,
                    "multiplicity_family_rule_hash": charter.multiplicity_family_rule_hash,
                }
            )

    def anchor_pre_outcome(
        self,
        charter: ExperimentCharter,
        census: LegacyCensusReceipt,
        authorization: OperationReceipt,
    ) -> dict[str, object]:
        census.verify()
        if census.status != "CONSERVATIVE_PENALTY_PREREGISTERED":
            raise UnauthorizedOperation(
                "real-history anchoring requires a preregistered conservative census penalty"
            )
        with FileLease(self.lock_path):
            events = self._load()
            pre_anchor_head = str(events[-1]["event_hash"]) if events else "GENESIS"
            required_scope = {
                "charter_id": charter.charter_id,
                "legacy_census_receipt_id": census.receipt_id,
                "multiplicity_family_id": charter.multiplicity_family_id,
                "multiplicity_family_rule_hash": (
                    charter.multiplicity_family_rule_hash
                ),
                "trial_event_head": pre_anchor_head,
            }
            declared = [
                event
                for event in events
                if event["event_type"] == "DECLARED"
                and event["charter_id"] == charter.charter_id
            ]
            if len(declared) != 1:
                raise UnauthorizedOperation("trial must be declared before outcome anchoring")
            if any(
                event["charter_id"] == charter.charter_id
                and event["event_type"] == "PRE_OUTCOME_ANCHORED"
                for event in events
            ):
                raise IntegrityError("trial already has a pre-outcome anchor")
            event_time = self.clock.now()
            if event_time >= charter.outcome_unlock_at:
                raise UnauthorizedOperation(
                    "pre-outcome authorization anchor was attempted after outcome unlock"
                )
            authorization.consume(
                self.boundary,
                operation="REAL_HISTORY_EVALUATION",
                classification=(
                    OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
                ),
                required_scope=required_scope,
            )
            return self._append(
                {
                    "authorization_receipt_id": authorization.receipt_id,
                    "charter_id": charter.charter_id,
                    "counted_trial_number": declared[0]["counted_trial_number"],
                    "event_type": "PRE_OUTCOME_ANCHORED",
                    "event_time_utc": event_time.isoformat(),
                    "legacy_census_receipt_id": census.receipt_id,
                    "multiplicity_family_id": charter.multiplicity_family_id,
                    "multiplicity_family_rule_hash": charter.multiplicity_family_rule_hash,
                }
            )

    def events(self) -> tuple[dict[str, object], ...]:
        return tuple(self._load())


@dataclass(frozen=True)
class EvaluationPermit:
    charter_id: str
    counted_trial_number: int
    classification: EvaluationClassification
    release_receipts: tuple[VerifiedReleaseReceipt, ...]
    release_role_bindings: tuple[tuple[str, EvaluationReleaseRole], ...]
    authorized_roles: tuple[EvaluationReleaseRole, ...]
    evaluation_root: Path
    repository_id: str
    registry_id: str
    legacy_census_receipt_id: str
    declaration_event_hash: str
    authorization_anchor_event_hash: str
    trial_event_head: str
    permit_hash: str
    issuer_mac: str


class TrialRegistry:
    def __init__(
        self,
        root: Path,
        evaluation_root: Path,
        *,
        event_ledger: TrialEventLedger,
        census: LegacyCensusReceipt,
        boundary: RepoBoundary,
        operation_receipt: OperationReceipt,
    ) -> None:
        operation_receipt.verify(boundary, operation="REGISTER_TRIAL")
        self.root = _canonical_active_path(
            boundary, root, _TRIAL_REGISTRY_ROOT, purpose="trial registry"
        )
        self.evaluation_root = _canonical_active_path(
            boundary,
            evaluation_root,
            _EVALUATION_ROOT,
            purpose="evaluation release root",
        )
        self.event_ledger = event_ledger
        self.census = census
        self.boundary = boundary
        self.operation_receipt = operation_receipt
        self.registry_id = sha256_json(
            {
                "census_receipt_id": census.receipt_id,
                "evaluation_root": _EVALUATION_ROOT.as_posix(),
                "event_ledger_id": event_ledger.ledger_id,
                "registry_root": _TRIAL_REGISTRY_ROOT.as_posix(),
                "repository_id": boundary.repository_id,
            }
        )
        self._permit_secret = os.urandom(32)

    def register(self, charter: ExperimentCharter) -> Path:
        self.operation_receipt.verify(self.boundary, operation="REGISTER_TRIAL")
        for receipt in charter.data_release_receipts:
            receipt.verify(self.boundary)
            if receipt.receipt_id in charter.economics_release_receipt_ids:
                from .economics import VerifiedEconomicsRegistry

                VerifiedEconomicsRegistry.from_release(receipt, self.boundary)
        event = self.event_ledger.declare(
            charter,
            self.census,
            self.operation_receipt.receipt_id,
        )
        with FileLease(self.event_ledger.lock_path):
            self.root.mkdir(parents=True, exist_ok=True)
            target = self.root / f"{charter.charter_id}.json"
            payload = {
                "authorization_receipt_id": event["authorization_receipt_id"],
                "charter": charter.core(),
                "charter_id": charter.charter_id,
                "counted_trial_number": event["counted_trial_number"],
                "declaration_event_hash": event["event_hash"],
                "legacy_census_receipt_id": self.census.receipt_id,
                "repository_id": self.boundary.repository_id,
            }
            if target.exists():
                assert_plain_file(target)
                if json.loads(target.read_text(encoding="utf-8")) != payload:
                    raise IntegrityError("registered charter ID collision")
                return target
            descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(descriptor, canonical_bytes(payload) + b"\n")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            fsync_directory(self.root)
            return target

    def _load_registered(self, charter_id: str) -> tuple[ExperimentCharter, dict[str, object]]:
        path = self.root / f"{charter_id}.json"
        if not path.exists():
            raise UnauthorizedOperation("trial is not registered")
        try:
            assert_plain_file(path)
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise UnauthorizedOperation("trial is not registered") from exc
        if not isinstance(payload, dict) or set(payload) != {
            "authorization_receipt_id",
            "charter",
            "charter_id",
            "counted_trial_number",
            "declaration_event_hash",
            "legacy_census_receipt_id",
            "repository_id",
        } or payload["charter_id"] != charter_id or not isinstance(
            payload["charter"], dict
        ):
            raise IntegrityError("registered charter record is invalid")
        if (
            payload["repository_id"] != self.boundary.repository_id
            or payload["legacy_census_receipt_id"] != self.census.receipt_id
            or type(payload["counted_trial_number"]) is not int
            or type(payload["authorization_receipt_id"]) is not str
            or type(payload["declaration_event_hash"]) is not str
        ):
            raise IntegrityError("registered charter governance bindings are invalid")
        charter = ExperimentCharter.from_dict(payload["charter"])
        if charter.charter_id != charter_id:
            raise IntegrityError("registered charter hash is invalid")
        return charter, payload

    def unlock_final_holdout(
        self, charter_id: str, authorization: OperationReceipt
    ) -> dict[str, object]:
        """One-time holdout transition bound to the frozen charter and current head."""

        charter, record = self._load_registered(charter_id)
        if (
            charter.evaluation_classification
            is not EvaluationClassification.REAL_HISTORY_DISCOVERY
        ):
            raise UnauthorizedOperation("synthetic trials have no final holdout unlock")
        holdout_release_ids = tuple(
            release_id
            for release_id, role in charter.release_role_bindings
            if role is EvaluationReleaseRole.FINAL_HOLDOUT
        )
        if not holdout_release_ids:
            raise UnauthorizedOperation("charter has no separately bound final holdout")
        with FileLease(self.event_ledger.lock_path):
            events = list(self.event_ledger.events())
            if any(
                event["event_type"] == "FINAL_HOLDOUT_UNLOCKED"
                and event["charter_id"] == charter_id
                for event in events
            ):
                raise IntegrityError("final holdout was already unlocked")
            anchored = [
                event
                for event in events
                if event["event_type"] == "PRE_OUTCOME_ANCHORED"
                and event["charter_id"] == charter_id
            ]
            if len(anchored) != 1:
                raise UnauthorizedOperation("holdout unlock lacks the pre-outcome anchor")
            pre_unlock_head = str(events[-1]["event_hash"]) if events else "GENESIS"
            event_time = self.event_ledger.clock.now()
            if event_time < charter.outcome_unlock_at:
                raise UnauthorizedOperation("final holdout unlock was attempted too early")
            authorization.consume(
                self.boundary,
                operation="UNLOCK_FINAL_HOLDOUT",
                classification=(
                    OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
                ),
                required_scope={
                    "charter_id": charter_id,
                    "frozen_charter_hash": charter.charter_id,
                    "holdout_release_ids_hash": sha256_json(list(holdout_release_ids)),
                    "pre_unlock_trial_event_head": pre_unlock_head,
                },
            )
            return self.event_ledger._append(
                {
                    "authorization_receipt_id": authorization.receipt_id,
                    "charter_id": charter_id,
                    "counted_trial_number": record["counted_trial_number"],
                    "event_type": "FINAL_HOLDOUT_UNLOCKED",
                    "event_time_utc": event_time.isoformat(),
                    "legacy_census_receipt_id": self.census.receipt_id,
                    "multiplicity_family_id": charter.multiplicity_family_id,
                    "multiplicity_family_rule_hash": (
                        charter.multiplicity_family_rule_hash
                    ),
                }
            )

    def permit(self, charter_id: str) -> EvaluationPermit:
        charter, record = self._load_registered(charter_id)
        self.census.verify()
        events = self.event_ledger.events()
        declaration = [
            event
            for event in events
            if event["event_type"] == "DECLARED"
            and event["charter_id"] == charter_id
        ]
        if len(declaration) != 1 or declaration[0]["event_hash"] != record[
            "declaration_event_hash"
        ]:
            raise IntegrityError("registered trial is not bound to its global declaration")
        declared = declaration[0]
        if (
            declared["legacy_census_receipt_id"] != self.census.receipt_id
            or record["legacy_census_receipt_id"] != self.census.receipt_id
            or declared["counted_trial_number"] != record["counted_trial_number"]
            or declared["authorization_receipt_id"]
            != record["authorization_receipt_id"]
            or record["authorization_receipt_id"]
            != self.operation_receipt.receipt_id
        ):
            raise IntegrityError("trial declaration, census, count, and registry diverged")
        if (
            declaration[0]["multiplicity_family_id"]
            != charter.multiplicity_family_id
            or declaration[0]["multiplicity_family_rule_hash"]
            != charter.multiplicity_family_rule_hash
        ):
            raise IntegrityError("trial multiplicity family changed after declaration")
        anchor_hash = "NOT_REQUIRED"
        authorized_roles: tuple[EvaluationReleaseRole, ...]
        if charter.evaluation_classification is EvaluationClassification.REAL_HISTORY_DISCOVERY:
            if self.census.status != "CONSERVATIVE_PENALTY_PREREGISTERED":
                raise UnauthorizedOperation("INVALID_TRIAL_CENSUS_UNRESOLVED")
            observed_data_roles = {
                role
                for release_id, role in charter.release_role_bindings
                if release_id
                not in {
                    receipt.release_id
                    for receipt in charter.data_release_receipts
                    if receipt.receipt_id in charter.economics_release_receipt_ids
                }
            }
            if observed_data_roles != _REAL_HISTORY_DATA_ROLES:
                raise UnauthorizedOperation(
                    "real-history access requires disjoint training, inner, outer, and holdout releases"
                )
            anchored = [
                event
                for event in events
                if event["event_type"] == "PRE_OUTCOME_ANCHORED"
                and event["charter_id"] == charter_id
            ]
            if len(anchored) != 1:
                raise UnauthorizedOperation(
                    "real-history access lacks one external pre-outcome authorization anchor"
                )
            anchor = anchored[0]
            if (
                anchor["legacy_census_receipt_id"] != self.census.receipt_id
                or anchor["counted_trial_number"] != declared["counted_trial_number"]
                or anchor["multiplicity_family_id"] != charter.multiplicity_family_id
                or anchor["multiplicity_family_rule_hash"]
                != charter.multiplicity_family_rule_hash
            ):
                raise IntegrityError("pre-outcome anchor diverges from the declared trial")
            anchor_hash = str(anchor["event_hash"])
            holdout_unlocked = [
                event
                for event in events
                if event["event_type"] == "FINAL_HOLDOUT_UNLOCKED"
                and event["charter_id"] == charter_id
            ]
            if len(holdout_unlocked) > 1:
                raise IntegrityError("final holdout has multiple unlock events")
            roles = {
                EvaluationReleaseRole.TRAINING,
                EvaluationReleaseRole.INNER_VALIDATION,
                EvaluationReleaseRole.OUTER_SCREEN,
                EvaluationReleaseRole.ECONOMICS,
            }
            if holdout_unlocked:
                roles.add(EvaluationReleaseRole.FINAL_HOLDOUT)
            authorized_roles = tuple(sorted(roles, key=lambda item: item.value))
        else:
            authorized_roles = (EvaluationReleaseRole.SYNTHETIC_MECHANICS,)
        for receipt in charter.data_release_receipts:
            receipt.verify(self.boundary)
            if receipt.receipt_id in charter.economics_release_receipt_ids:
                from .economics import VerifiedEconomicsRegistry

                VerifiedEconomicsRegistry.from_release(receipt, self.boundary)
        head = str(events[-1]["event_hash"]) if events else "GENESIS"
        core = {
            "authorization_anchor_event_hash": anchor_hash,
            "charter_id": charter_id,
            "classification": charter.evaluation_classification.value,
            "counted_trial_number": record["counted_trial_number"],
            "declaration_event_hash": record["declaration_event_hash"],
            "evaluation_root": str(self.evaluation_root.resolve(strict=False)),
            "legacy_census_receipt_id": self.census.receipt_id,
            "release_receipts": [
                receipt.as_dict() for receipt in charter.data_release_receipts
            ],
            "release_role_bindings": [
                [release_id, role.value]
                for release_id, role in charter.release_role_bindings
            ],
            "repository_id": self.boundary.repository_id,
            "registry_id": self.registry_id,
            "authorized_roles": [role.value for role in authorized_roles],
            "trial_event_head": head,
        }
        permit_hash = sha256_json(core)
        issuer_mac = hmac.new(
            self._permit_secret,
            canonical_bytes({**core, "permit_hash": permit_hash}),
            hashlib.sha256,
        ).hexdigest()
        return EvaluationPermit(
            charter_id,
            record["counted_trial_number"],
            charter.evaluation_classification,
            charter.data_release_receipts,
            charter.release_role_bindings,
            authorized_roles,
            self.evaluation_root,
            self.boundary.repository_id,
            self.registry_id,
            self.census.receipt_id,
            str(record["declaration_event_hash"]),
            anchor_hash,
            head,
            permit_hash,
            issuer_mac,
        )

    def verify_permit(self, permit: EvaluationPermit) -> None:
        if type(permit) is not EvaluationPermit:
            raise IntegrityError("evaluation permit was not issued by the registry")
        expected = self.permit(permit.charter_id)
        if not hmac.compare_digest(expected.issuer_mac, permit.issuer_mac) or permit != expected:
            raise IntegrityError(
                "evaluation permit is forged, stale, or from a different registry"
            )

    def verify_candidate_provenance(self, provenance: object) -> None:
        required = (
            "charter_id",
            "counted_trial_number",
            "legacy_census_receipt_id",
            "declaration_event_hash",
            "trial_event_head",
            "trial_registry_id",
        )
        if any(not hasattr(provenance, name) for name in required):
            raise IntegrityError("candidate provenance fields are incomplete")
        charter, record = self._load_registered(provenance.charter_id)
        events = self.event_ledger.events()
        head = str(events[-1]["event_hash"]) if events else "GENESIS"
        unlocks = [
            event
            for event in events
            if event["event_type"] == "FINAL_HOLDOUT_UNLOCKED"
            and event["charter_id"] == charter.charter_id
        ]
        if (
            provenance.trial_registry_id != self.registry_id
            or provenance.legacy_census_receipt_id != self.census.receipt_id
            or provenance.counted_trial_number != record["counted_trial_number"]
            or provenance.declaration_event_hash != record["declaration_event_hash"]
            or provenance.trial_event_head != head
            or len(unlocks) != 1
        ):
            raise IntegrityError(
                "candidate provenance is stale or differs from the passed trial chain"
            )


class EvaluationFirewall:
    @staticmethod
    def assert_input(
        permit: EvaluationPermit,
        release_id: str,
        relative_path: str,
        *,
        boundary: RepoBoundary,
        registry: TrialRegistry,
        required_role: EvaluationReleaseRole,
    ) -> Path:
        if not isinstance(registry, TrialRegistry) or registry.boundary != boundary:
            raise IntegrityError("evaluation firewall requires the issuing trial registry")
        registry.verify_permit(permit)
        if not isinstance(required_role, EvaluationReleaseRole):
            raise ContractError("evaluation input role must use the declared enum")
        if required_role not in permit.authorized_roles:
            raise UnauthorizedOperation("evaluation permit does not authorize this release role")
        role_by_release = dict(permit.release_role_bindings)
        if role_by_release.get(release_id) is not required_role:
            raise UnauthorizedOperation("evaluation release role differs from the charter")
        matches = [item for item in permit.release_receipts if item.release_id == release_id]
        if len(matches) != 1:
            raise UnauthorizedOperation("release is not listed in the registered charter")
        manifest = matches[0].verify(boundary)
        release_root = boundary.active_root / matches[0].relative_root
        if release_root.parent.resolve(strict=False) != permit.evaluation_root.resolve(
            strict=False
        ):
            raise UnauthorizedOperation("release is outside the chartered evaluation root")
        candidate = contained_path(release_root, relative_path)
        if relative_path not in {entry.path for entry in manifest.files}:
            raise UnauthorizedOperation("path is not a payload in the verified release")
        assert_plain_file(candidate)
        return candidate
