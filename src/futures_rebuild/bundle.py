"""Verified-release model bundles with synthetic/candidate authorization separation."""

from __future__ import annotations

import json
import os
import re
import shutil
import uuid
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
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    is_linklike,
    sha256_file,
    sha256_json,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .locking import FileLease
from .release import VerifiedReleaseReceipt
from .time_contracts import require_utc


class BundleClassification(str, Enum):
    SYNTHETIC_MECHANICS_ONLY = "SYNTHETIC_MECHANICS_ONLY"
    CANDIDATE = "CANDIDATE"


@dataclass(frozen=True)
class CandidateProvenance:
    charter_id: str
    counted_trial_number: int
    legacy_census_receipt_id: str
    declaration_event_hash: str
    trial_event_head: str
    trial_registry_id: str
    outer_result_receipt_id: str
    final_holdout_result_receipt_id: str
    prediction_census_receipt_id: str
    multiplicity_gate_receipt_id: str
    negative_control_gate_receipt_id: str
    readiness_receipt: OperationReceipt
    provenance_id: str

    def core(self) -> dict[str, object]:
        return {
            "charter_id": self.charter_id,
            "counted_trial_number": self.counted_trial_number,
            "declaration_event_hash": self.declaration_event_hash,
            "final_holdout_result_receipt_id": self.final_holdout_result_receipt_id,
            "legacy_census_receipt_id": self.legacy_census_receipt_id,
            "multiplicity_gate_receipt_id": self.multiplicity_gate_receipt_id,
            "negative_control_gate_receipt_id": self.negative_control_gate_receipt_id,
            "outer_result_receipt_id": self.outer_result_receipt_id,
            "prediction_census_receipt_id": self.prediction_census_receipt_id,
            "readiness_receipt": self.readiness_receipt.as_dict(),
            "trial_event_head": self.trial_event_head,
            "trial_registry_id": self.trial_registry_id,
        }

    def validate(self) -> None:
        if type(self.counted_trial_number) is not int or self.counted_trial_number <= 0:
            raise ContractError("candidate counted-trial identity is invalid")
        hashes = (
            self.charter_id,
            self.legacy_census_receipt_id,
            self.declaration_event_hash,
            self.trial_event_head,
            self.trial_registry_id,
            self.outer_result_receipt_id,
            self.final_holdout_result_receipt_id,
            self.prediction_census_receipt_id,
            self.multiplicity_gate_receipt_id,
            self.negative_control_gate_receipt_id,
            self.provenance_id,
        )
        if (
            type(self.readiness_receipt) is not OperationReceipt
            or any(
                type(value) is not str
                or re.fullmatch(r"[0-9a-f]{64}", value) is None
                for value in hashes
            )
        ):
            raise ContractError("candidate provenance receipts must be exact SHA-256 IDs")
        if sha256_json(self.core()) != self.provenance_id:
            raise IntegrityError("candidate provenance hash is invalid")

    def verify(self, boundary: RepoBoundary, trial_registry: object | None = None) -> None:
        self.validate()
        self.readiness_receipt.verify(
            boundary,
            operation="CANDIDATE_READINESS",
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope={
                "candidate_provenance_core_hash": sha256_json(
                    {
                        key: value
                        for key, value in self.core().items()
                        if key != "readiness_receipt"
                    }
                ),
                "charter_id": self.charter_id,
                "trial_event_head": self.trial_event_head,
            },
        )
        if self.readiness_receipt.externally_authorized is not True:
            raise UnauthorizedOperation("candidate readiness lacks external authority")
        if trial_registry is not None:
            verifier = getattr(trial_registry, "verify_candidate_provenance", None)
            if verifier is None:
                raise IntegrityError("candidate provenance lacks its issuing trial registry")
            verifier(self)

    def as_dict(self) -> dict[str, object]:
        return {**self.core(), "provenance_id": self.provenance_id}

    @classmethod
    def from_dict(cls, payload: object) -> "CandidateProvenance":
        if not isinstance(payload, dict):
            raise IntegrityError("candidate provenance must be an object")
        expected = {
            "charter_id",
            "counted_trial_number",
            "declaration_event_hash",
            "final_holdout_result_receipt_id",
            "legacy_census_receipt_id",
            "multiplicity_gate_receipt_id",
            "negative_control_gate_receipt_id",
            "outer_result_receipt_id",
            "prediction_census_receipt_id",
            "provenance_id",
            "readiness_receipt",
            "trial_event_head",
            "trial_registry_id",
        }
        if set(payload) != expected or type(payload["counted_trial_number"]) is not int:
            raise IntegrityError("candidate provenance schema is invalid")
        string_keys = expected.difference({"counted_trial_number", "readiness_receipt"})
        if any(type(payload[key]) is not str for key in string_keys) or not isinstance(
            payload["readiness_receipt"], dict
        ):
            raise IntegrityError("candidate provenance field types are invalid")
        result = cls(
            charter_id=payload["charter_id"],
            counted_trial_number=payload["counted_trial_number"],
            legacy_census_receipt_id=payload["legacy_census_receipt_id"],
            declaration_event_hash=payload["declaration_event_hash"],
            trial_event_head=payload["trial_event_head"],
            trial_registry_id=payload["trial_registry_id"],
            outer_result_receipt_id=payload["outer_result_receipt_id"],
            final_holdout_result_receipt_id=payload[
                "final_holdout_result_receipt_id"
            ],
            prediction_census_receipt_id=payload["prediction_census_receipt_id"],
            multiplicity_gate_receipt_id=payload["multiplicity_gate_receipt_id"],
            negative_control_gate_receipt_id=payload[
                "negative_control_gate_receipt_id"
            ],
            readiness_receipt=OperationReceipt.from_dict(payload["readiness_receipt"]),
            provenance_id=payload["provenance_id"],
        )
        result.validate()
        return result


def _receipt_tuple(payload: object) -> tuple[VerifiedReleaseReceipt, ...]:
    if not isinstance(payload, list):
        raise IntegrityError("bundle release receipts must be a list")
    return tuple(VerifiedReleaseReceipt.from_dict(item) for item in payload)


@dataclass(frozen=True)
class BundleMetadata:
    bundle_classification: BundleClassification
    feature_names: tuple[str, ...]
    feature_schema_hash: str
    preprocessing_hash: str
    calibration_hash: str
    decision_policy_hash: str
    training_release_receipts: tuple[VerifiedReleaseReceipt, ...]
    inference_source_release_receipts: tuple[VerifiedReleaseReceipt, ...]
    definition_release_receipts: tuple[VerifiedReleaseReceipt, ...]
    economics_release_receipts: tuple[VerifiedReleaseReceipt, ...]
    training_cutoff: datetime
    loader_code_hash: str
    code_hash: str
    config_hash: str
    environment_hash: str
    dependency_lock_hash: str
    candidate_provenance: CandidateProvenance | None = None

    def __post_init__(self) -> None:
        require_utc(self.training_cutoff, "training_cutoff")
        if type(self.bundle_classification) is not BundleClassification:
            raise ContractError("bundle classification must use the declared enum")
        if (
            type(self.feature_names) is not tuple
            or not self.feature_names
            or any(
                type(name) is not str or not name.isidentifier()
                for name in self.feature_names
            )
            or len(set(self.feature_names)) != len(self.feature_names)
        ):
            raise ContractError("ordered, unique feature names are required")
        hash_fields = (
            self.feature_schema_hash,
            self.preprocessing_hash,
            self.calibration_hash,
            self.decision_policy_hash,
            self.loader_code_hash,
            self.code_hash,
            self.config_hash,
            self.environment_hash,
            self.dependency_lock_hash,
        )
        if any(
            type(value) is not str
            or re.fullmatch(r"[0-9a-f]{64}", value) is None
            for value in hash_fields
        ):
            raise ContractError("bundle provenance fields must be exact SHA-256 values")
        for name, receipts in (
            ("training_release_receipts", self.training_release_receipts),
            ("inference_source_release_receipts", self.inference_source_release_receipts),
            ("definition_release_receipts", self.definition_release_receipts),
            ("economics_release_receipts", self.economics_release_receipts),
        ):
            if type(receipts) is not tuple or any(
                type(receipt) is not VerifiedReleaseReceipt for receipt in receipts
            ):
                raise ContractError(f"{name} must contain exact verified-release receipts")
            ids = tuple(item.release_id for item in receipts)
            if not ids or ids != tuple(sorted(set(ids))):
                raise ContractError(f"{name} must be nonempty, unique, and sorted")
        expected_schema_hash = sha256_json({"feature_names": list(self.feature_names)})
        if self.feature_schema_hash != expected_schema_hash:
            raise ContractError("feature schema hash does not bind ordered feature names")
        if (
            self.bundle_classification is BundleClassification.SYNTHETIC_MECHANICS_ONLY
            and self.candidate_provenance is not None
        ):
            raise ContractError("synthetic bundles cannot carry candidate provenance")
        if self.candidate_provenance is not None and type(
            self.candidate_provenance
        ) is not CandidateProvenance:
            raise ContractError("candidate provenance must use the exact declared contract")

    @property
    def all_release_receipts(self) -> tuple[VerifiedReleaseReceipt, ...]:
        by_id: dict[str, VerifiedReleaseReceipt] = {}
        for receipt in (
            *self.training_release_receipts,
            *self.inference_source_release_receipts,
            *self.definition_release_receipts,
            *self.economics_release_receipts,
        ):
            prior = by_id.get(receipt.release_id)
            if prior is not None and prior != receipt:
                raise ContractError("one release ID has conflicting verified receipts")
            by_id[receipt.release_id] = receipt
        return tuple(by_id[key] for key in sorted(by_id))

    def verify_releases(self, boundary: RepoBoundary) -> None:
        for receipt in self.all_release_receipts:
            receipt.verify(boundary)

    def as_dict(self) -> dict[str, object]:
        return {
            "bundle_classification": self.bundle_classification.value,
            "calibration_hash": self.calibration_hash,
            "candidate_provenance": (
                None
                if self.candidate_provenance is None
                else self.candidate_provenance.as_dict()
            ),
            "code_hash": self.code_hash,
            "config_hash": self.config_hash,
            "decision_policy_hash": self.decision_policy_hash,
            "definition_release_receipts": [
                item.as_dict() for item in self.definition_release_receipts
            ],
            "dependency_lock_hash": self.dependency_lock_hash,
            "economics_release_receipts": [
                item.as_dict() for item in self.economics_release_receipts
            ],
            "environment_hash": self.environment_hash,
            "feature_names": list(self.feature_names),
            "feature_schema_hash": self.feature_schema_hash,
            "inference_source_release_receipts": [
                item.as_dict() for item in self.inference_source_release_receipts
            ],
            "loader_code_hash": self.loader_code_hash,
            "preprocessing_hash": self.preprocessing_hash,
            "training_cutoff": self.training_cutoff.isoformat(),
            "training_release_receipts": [
                item.as_dict() for item in self.training_release_receipts
            ],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "BundleMetadata":
        expected = {
            "bundle_classification",
            "calibration_hash",
            "candidate_provenance",
            "code_hash",
            "config_hash",
            "decision_policy_hash",
            "definition_release_receipts",
            "dependency_lock_hash",
            "economics_release_receipts",
            "environment_hash",
            "feature_names",
            "feature_schema_hash",
            "inference_source_release_receipts",
            "loader_code_hash",
            "preprocessing_hash",
            "training_cutoff",
            "training_release_receipts",
        }
        if set(payload) != expected:
            raise IntegrityError("bundle metadata contains missing or unexpected fields")
        try:
            result = cls(
                bundle_classification=BundleClassification(
                    str(payload["bundle_classification"])
                ),
                feature_names=tuple(payload["feature_names"]),  # type: ignore[arg-type]
                feature_schema_hash=str(payload["feature_schema_hash"]),
                preprocessing_hash=str(payload["preprocessing_hash"]),
                calibration_hash=str(payload["calibration_hash"]),
                decision_policy_hash=str(payload["decision_policy_hash"]),
                training_release_receipts=_receipt_tuple(
                    payload["training_release_receipts"]
                ),
                inference_source_release_receipts=_receipt_tuple(
                    payload["inference_source_release_receipts"]
                ),
                definition_release_receipts=_receipt_tuple(
                    payload["definition_release_receipts"]
                ),
                economics_release_receipts=_receipt_tuple(
                    payload["economics_release_receipts"]
                ),
                training_cutoff=datetime.fromisoformat(str(payload["training_cutoff"])),
                loader_code_hash=str(payload["loader_code_hash"]),
                code_hash=str(payload["code_hash"]),
                config_hash=str(payload["config_hash"]),
                environment_hash=str(payload["environment_hash"]),
                dependency_lock_hash=str(payload["dependency_lock_hash"]),
                candidate_provenance=(
                    None
                    if payload["candidate_provenance"] is None
                    else CandidateProvenance.from_dict(payload["candidate_provenance"])
                ),
            )
        except (KeyError, TypeError, ValueError, ContractError, IntegrityError) as exc:
            raise IntegrityError("bundle metadata is invalid") from exc
        if result.as_dict() != payload:
            raise IntegrityError("bundle metadata is not canonical")
        return result


def _authorization_scope(
    artifact_hash: str, metadata: BundleMetadata
) -> dict[str, str]:
    return {
        "artifact_sha256": artifact_hash,
        "code_hash": metadata.code_hash,
        "config_hash": metadata.config_hash,
        "environment_hash": metadata.environment_hash,
        "metadata_hash": sha256_json(metadata.as_dict()),
        "release_receipts_hash": sha256_json(
            [item.as_dict() for item in metadata.all_release_receipts]
        ),
    }


def seal_bundle(
    artifact: Path,
    bundle_root: Path,
    lock_path: Path,
    metadata: BundleMetadata,
    *,
    boundary: RepoBoundary,
    operation_receipt: OperationReceipt,
    trial_registry: object | None = None,
) -> Path:
    artifact = boundary.assert_active_path(artifact, purpose="bundle artifact")
    bundle_root = boundary.assert_active_path(
        bundle_root, purpose="bundle root", subtree="bundles"
    )
    lock_path = boundary.assert_active_path(lock_path, purpose="bundle lock")
    assert_no_linklike_ancestors(artifact)
    metadata.verify_releases(boundary)
    artifact_hash = sha256_file(artifact)
    scope = _authorization_scope(artifact_hash, metadata)
    if metadata.bundle_classification is BundleClassification.SYNTHETIC_MECHANICS_ONLY:
        operation_receipt.verify(
            boundary,
            operation="SEAL_SYNTHETIC_BUNDLE",
            classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
            required_scope=scope,
        )
    else:
        if metadata.candidate_provenance is None:
            raise UnauthorizedOperation(
                "candidate sealing requires exact passed-trial and readiness provenance"
            )
        metadata.candidate_provenance.verify(boundary, trial_registry)
        if operation_receipt.externally_authorized is not True:
            raise UnauthorizedOperation("candidate sealing lacks external authorization")
    manifest_core = {
        "artifact_sha256": artifact_hash,
        "authorization_receipt": operation_receipt.as_dict(),
        "metadata": metadata.as_dict(),
    }
    bundle_id = sha256_json(manifest_core)
    target = bundle_root / bundle_id
    bundle_root.mkdir(parents=True, exist_ok=True)
    boundary.assert_active_path(bundle_root, purpose="bundle root", subtree="bundles")
    with FileLease(lock_path):
        if target.exists():
            verify_bundle(target, boundary=boundary)
            return target
        if metadata.bundle_classification is BundleClassification.CANDIDATE:
            operation_receipt.consume(
                boundary,
                operation="SEAL_CANDIDATE_BUNDLE",
                classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
                required_scope=scope,
            )
        stage = bundle_root / f".tmp-{uuid.uuid4().hex}"
        stage.mkdir()
        copied = stage / "model.artifact"
        shutil.copyfile(artifact, copied)
        if sha256_file(copied) != artifact_hash:
            raise IntegrityError("bundle artifact changed during sealing")
        manifest = {**manifest_core, "bundle_id": bundle_id}
        descriptor = os.open(
            stage / "bundle_manifest.json",
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        )
        try:
            os.write(descriptor, canonical_bytes(manifest) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(stage, target)
        fsync_directory(bundle_root)
    verify_bundle(target, boundary=boundary)
    return target


def verify_bundle(path: Path, *, boundary: RepoBoundary) -> dict[str, object]:
    path = boundary.assert_active_path(path, purpose="bundle", subtree="bundles")
    assert_no_linklike_ancestors(path)
    if not path.exists() or not path.is_dir() or is_linklike(path):
        raise IntegrityError("bundle root must be a plain directory")
    manifest_path = path / "bundle_manifest.json"
    artifact_path = path / "model.artifact"
    try:
        assert_plain_file(manifest_path)
        assert_plain_file(artifact_path)
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, ContractError, IntegrityError) as exc:
        raise IntegrityError("bundle manifest is missing or invalid") from exc
    if set(manifest) != {
        "artifact_sha256",
        "authorization_receipt",
        "bundle_id",
        "metadata",
    } or not isinstance(manifest.get("metadata"), dict) or not isinstance(
        manifest.get("authorization_receipt"), dict
    ):
        raise IntegrityError("bundle manifest schema is invalid")
    metadata = BundleMetadata.from_dict(manifest["metadata"])
    metadata.verify_releases(boundary)
    receipt = OperationReceipt.from_dict(manifest["authorization_receipt"])
    scope = _authorization_scope(str(manifest["artifact_sha256"]), metadata)
    if metadata.bundle_classification is BundleClassification.SYNTHETIC_MECHANICS_ONLY:
        receipt.verify(
            boundary,
            operation="SEAL_SYNTHETIC_BUNDLE",
            classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
            required_scope=scope,
        )
    else:
        if metadata.candidate_provenance is None:
            raise IntegrityError("candidate bundle lacks passed-trial provenance")
        metadata.candidate_provenance.verify(boundary)
        receipt.verify(
            boundary,
            operation="SEAL_CANDIDATE_BUNDLE",
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope=scope,
        )
        if receipt.externally_authorized is not True:
            raise IntegrityError("candidate bundle lacks external authorization evidence")
        receipt.assert_consumed(
            boundary,
            operation="SEAL_CANDIDATE_BUNDLE",
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope=scope,
        )
    core = {
        "artifact_sha256": manifest["artifact_sha256"],
        "authorization_receipt": manifest["authorization_receipt"],
        "metadata": manifest["metadata"],
    }
    if sha256_json(core) != manifest.get("bundle_id") or path.name != manifest.get(
        "bundle_id"
    ):
        raise IntegrityError("bundle identity does not match its content")
    if sha256_file(artifact_path) != manifest["artifact_sha256"]:
        raise IntegrityError("sealed artifact bytes changed")
    if {item.name for item in path.iterdir()} != {
        "model.artifact",
        "bundle_manifest.json",
    }:
        raise IntegrityError("bundle contains unexpected files")
    return manifest
