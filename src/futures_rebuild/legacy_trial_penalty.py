"""Conservative executable successor to an unresolved legacy-trial census.

The successor preserves the indeterminate historical count and never claims an
exact census.  It converts a verified unresolved census into a preregistered
multiplicity penalty by counting every unresolved reference and a positive
safety margin in addition to the observed attempt floor.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Mapping

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_json
from .data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from .errors import ContractError, IntegrityError
from .legacy_trial_census import (
    INDETERMINATE_COUNT_STATE,
    LEGACY_CENSUS_FILENAME,
    LEGACY_CENSUS_RELEASE_KIND,
    LEGACY_CENSUS_SCHEMA_VERSION,
    UNRESOLVED_STATUS,
    validate_legacy_trial_census_payload,
)


CONSERVATIVE_CENSUS_SCHEMA_VERSION = "4.0.0"
CONSERVATIVE_CENSUS_STATUS = "CONSERVATIVE_PENALTY_PREREGISTERED"
CONSERVATIVE_CENSUS_FILENAME = "legacy_census_penalty.json"
SOURCE_CENSUS_RECEIPT_FILENAME = "source_unresolved_census_receipt.json"
COUNTING_RULE_ID = "OBSERVED_FLOOR_PLUS_EACH_UNRESOLVED_REFERENCE_PLUS_SAFETY_MARGIN"
DECISION_SCHEMA_VERSION = "legacy_trial_penalty_decision/1.0.0"


def _source_census(
    receipt: VerifiedReleaseReceipt, boundary: RepoBoundary
) -> tuple[dict[str, object], object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "evidence"
        or manifest.release_kind != LEGACY_CENSUS_RELEASE_KIND
        or manifest.schema_version != LEGACY_CENSUS_SCHEMA_VERSION
        or manifest.files
        or set(manifest.embedded_documents)
        != {LEGACY_CENSUS_FILENAME, "source_archive_receipt.json"}
    ):
        raise IntegrityError("conservative penalty source is not the unresolved census")
    payload = receipt.embedded_document(LEGACY_CENSUS_FILENAME, boundary)
    if not isinstance(payload, dict):
        raise IntegrityError("unresolved census payload is invalid")
    validated = validate_legacy_trial_census_payload(payload)
    unresolved = validated["unresolved_references"]
    if (
        validated["status"] != UNRESOLVED_STATUS
        or validated["exact_count_state"] != INDETERMINATE_COUNT_STATE
        or validated["preregistered_penalty_count"] != 0
        or validated["trusted_gate"] is not False
        or not isinstance(unresolved, list)
    ):
        raise IntegrityError("source census does not preserve unresolved trust closure")
    return validated, manifest


def build_conservative_penalty_payload(
    source_receipt: VerifiedReleaseReceipt,
    *,
    boundary: RepoBoundary,
    preregistered_penalty_count: int,
    safety_margin: int,
) -> dict[str, object]:
    """Build the exact conservative successor without publishing it."""

    source, _ = _source_census(source_receipt, boundary)
    unresolved = source["unresolved_references"]
    assert isinstance(unresolved, list)
    observed_floor = source["observed_attempt_floor"]
    if (
        type(observed_floor) is not int
        or type(safety_margin) is not int
        or safety_margin <= 0
        or type(preregistered_penalty_count) is not int
        or preregistered_penalty_count
        != observed_floor + len(unresolved) + safety_margin
    ):
        raise ContractError(
            "penalty must equal observed floor plus every unresolved reference "
            "plus a positive safety margin"
        )
    rationale = {
        "counting_rule_id": COUNTING_RULE_ID,
        "observed_attempt_floor": observed_floor,
        "preregistered_penalty_count": preregistered_penalty_count,
        "safety_margin": safety_margin,
        "unresolved_reference_count": len(unresolved),
    }
    core = {
        "exact_count_state": INDETERMINATE_COUNT_STATE,
        "observed_attempt_floor": observed_floor,
        "preregistered_penalty_count": preregistered_penalty_count,
        "rationale": rationale,
        "rationale_sha256": sha256_json(rationale),
        "source_census_release_id": source_receipt.release_id,
        "source_census_receipt_id": source_receipt.receipt_id,
        "source_census_sha256": source["census_sha256"],
        "source_evidence_sha256": source["source_evidence_sha256"],
        "source_snapshot_id": source["source_snapshot_id"],
        "status": CONSERVATIVE_CENSUS_STATUS,
        "trusted_gate": True,
        "unresolved_reference_count": len(unresolved),
    }
    return {**core, "census_sha256": sha256_json(core)}


def validate_conservative_penalty_payload(
    payload: Mapping[str, object],
) -> dict[str, object]:
    expected = {
        "census_sha256",
        "exact_count_state",
        "observed_attempt_floor",
        "preregistered_penalty_count",
        "rationale",
        "rationale_sha256",
        "source_census_release_id",
        "source_census_receipt_id",
        "source_census_sha256",
        "source_evidence_sha256",
        "source_snapshot_id",
        "status",
        "trusted_gate",
        "unresolved_reference_count",
    }
    if not isinstance(payload, Mapping) or set(payload) != expected:
        raise IntegrityError("conservative legacy census schema is invalid")
    rationale = payload["rationale"]
    rationale_keys = {
        "counting_rule_id",
        "observed_attempt_floor",
        "preregistered_penalty_count",
        "safety_margin",
        "unresolved_reference_count",
    }
    if not isinstance(rationale, Mapping) or set(rationale) != rationale_keys:
        raise IntegrityError("conservative legacy census rationale is invalid")
    integer_fields = (
        "observed_attempt_floor",
        "preregistered_penalty_count",
        "unresolved_reference_count",
    )
    if (
        any(type(payload[name]) is not int for name in integer_fields)
        or type(rationale["safety_margin"]) is not int
        or rationale["safety_margin"] <= 0
        or payload["status"] != CONSERVATIVE_CENSUS_STATUS
        or payload["exact_count_state"] != INDETERMINATE_COUNT_STATE
        or payload["trusted_gate"] is not True
        or rationale["counting_rule_id"] != COUNTING_RULE_ID
        or rationale["observed_attempt_floor"] != payload["observed_attempt_floor"]
        or rationale["preregistered_penalty_count"]
        != payload["preregistered_penalty_count"]
        or rationale["unresolved_reference_count"]
        != payload["unresolved_reference_count"]
        or payload["preregistered_penalty_count"]
        != payload["observed_attempt_floor"]
        + payload["unresolved_reference_count"]
        + rationale["safety_margin"]
        or payload["rationale_sha256"] != sha256_json(dict(rationale))
    ):
        raise IntegrityError("conservative legacy census counting rule is invalid")
    hash_fields = (
        "census_sha256",
        "rationale_sha256",
        "source_census_release_id",
        "source_census_receipt_id",
        "source_census_sha256",
        "source_evidence_sha256",
        "source_snapshot_id",
    )
    if any(
        type(payload[name]) is not str
        or len(payload[name]) != 64
        or any(character not in "0123456789abcdef" for character in payload[name])
        for name in hash_fields
    ):
        raise IntegrityError("conservative legacy census hash field is invalid")
    core = {key: payload[key] for key in payload if key != "census_sha256"}
    if payload["census_sha256"] != sha256_json(core):
        raise IntegrityError("conservative legacy census content address is invalid")
    return dict(payload)


def load_penalty_decision(
    path: Path,
    *,
    source_receipt: VerifiedReleaseReceipt,
    boundary: RepoBoundary,
) -> dict[str, object]:
    boundary.assert_active_path(path, purpose="legacy trial penalty decision", subtree="configs")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("legacy trial penalty decision JSON is invalid") from exc
    expected = {
        "counting_rule_id",
        "decision_id",
        "historical_execution_authorized",
        "observed_attempt_floor",
        "preregistered_penalty_count",
        "publication_authorized",
        "safety_margin",
        "schema_version",
        "selected_by_user",
        "source_census_sha256",
        "source_evidence_sha256",
        "source_snapshot_id",
        "unresolved_reference_count",
    }
    if not isinstance(raw, dict) or set(raw) != expected:
        raise IntegrityError("legacy trial penalty decision schema is invalid")
    core = {key: raw[key] for key in raw if key != "decision_id"}
    payload = build_conservative_penalty_payload(
        source_receipt,
        boundary=boundary,
        preregistered_penalty_count=raw["preregistered_penalty_count"],
        safety_margin=raw["safety_margin"],
    )
    if (
        raw["schema_version"] != DECISION_SCHEMA_VERSION
        or raw["counting_rule_id"] != COUNTING_RULE_ID
        or raw["selected_by_user"]
        != (
            "SELECT CONSERVATIVE LEGACY PENALTY "
            f"{raw['preregistered_penalty_count']}"
        )
        or raw["historical_execution_authorized"] is not False
        or raw["publication_authorized"] is not False
        or raw["decision_id"] != sha256_json(core)
        or raw["observed_attempt_floor"] != payload["observed_attempt_floor"]
        or raw["preregistered_penalty_count"]
        != payload["preregistered_penalty_count"]
        or raw["source_census_sha256"] != payload["source_census_sha256"]
        or raw["source_evidence_sha256"] != payload["source_evidence_sha256"]
        or raw["source_snapshot_id"] != payload["source_snapshot_id"]
        or raw["unresolved_reference_count"]
        != payload["unresolved_reference_count"]
    ):
        raise IntegrityError("legacy trial penalty decision differs from verified census")
    return raw


def publish_conservative_penalty_census(
    *,
    source_receipt: VerifiedReleaseReceipt,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
    preregistered_penalty_count: int,
    safety_margin: int,
) -> VerifiedReleaseReceipt:
    if publisher.boundary.repository_id != boundary.repository_id:
        raise IntegrityError("conservative census publisher belongs to another repository")
    payload = build_conservative_penalty_payload(
        source_receipt,
        boundary=boundary,
        preregistered_penalty_count=preregistered_penalty_count,
        safety_margin=safety_margin,
    )
    stage = publisher.create_stage("legacy_trial_census_penalty")
    manifest = ReleaseManifest.build(
        stage,
        phase="evidence",
        release_kind=LEGACY_CENSUS_RELEASE_KIND,
        schema_version=CONSERVATIVE_CENSUS_SCHEMA_VERSION,
        source_release_ids=(source_receipt.release_id,),
        embedded_documents={
            CONSERVATIVE_CENSUS_FILENAME: payload,
            SOURCE_CENSUS_RECEIPT_FILENAME: source_receipt.as_dict(),
        },
        metadata={
            "census_sha256": payload["census_sha256"],
            "exact_count_state": payload["exact_count_state"],
            "preregistered_penalty_count": payload[
                "preregistered_penalty_count"
            ],
            "source_census_release_id": source_receipt.release_id,
            "source_evidence_sha256": payload["source_evidence_sha256"],
            "source_snapshot_id": payload["source_snapshot_id"],
            "status": payload["status"],
            "trusted_gate": payload["trusted_gate"],
        },
    )
    manifest_path = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, boundary)
    load_conservative_penalty_census(receipt, boundary=boundary)
    return receipt


def load_conservative_penalty_census(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    expected_metadata = {
        "census_sha256",
        "exact_count_state",
        "preregistered_penalty_count",
        "source_census_release_id",
        "source_evidence_sha256",
        "source_snapshot_id",
        "status",
        "trusted_gate",
    }
    if (
        receipt.phase != "evidence"
        or manifest.release_kind != LEGACY_CENSUS_RELEASE_KIND
        or manifest.schema_version != CONSERVATIVE_CENSUS_SCHEMA_VERSION
        or manifest.files
        or set(manifest.embedded_documents)
        != {CONSERVATIVE_CENSUS_FILENAME, SOURCE_CENSUS_RECEIPT_FILENAME}
        or set(manifest.metadata) != expected_metadata
        or len(manifest.source_release_ids) != 1
    ):
        raise IntegrityError("conservative legacy census release contract is invalid")
    source_raw = receipt.embedded_document(SOURCE_CENSUS_RECEIPT_FILENAME, boundary)
    if not isinstance(source_raw, dict):
        raise IntegrityError("conservative census source receipt is invalid")
    source_receipt = VerifiedReleaseReceipt.from_dict(source_raw)
    source, _ = _source_census(source_receipt, boundary)
    payload_raw = receipt.embedded_document(CONSERVATIVE_CENSUS_FILENAME, boundary)
    if not isinstance(payload_raw, dict):
        raise IntegrityError("conservative census payload is invalid")
    payload = validate_conservative_penalty_payload(payload_raw)
    rebuilt = build_conservative_penalty_payload(
        source_receipt,
        boundary=boundary,
        preregistered_penalty_count=payload["preregistered_penalty_count"],
        safety_margin=payload["rationale"]["safety_margin"],
    )
    if (
        payload != rebuilt
        or manifest.source_release_ids != (source_receipt.release_id,)
        or payload["source_census_release_id"] != source_receipt.release_id
        or payload["source_census_receipt_id"] != source_receipt.receipt_id
        or payload["source_census_sha256"] != source["census_sha256"]
        or any(manifest.metadata[key] != payload[key] for key in expected_metadata)
    ):
        raise IntegrityError("conservative census differs from its source evidence")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Assess or publish an immutable conservative legacy-trial penalty"
    )
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-census-manifest", type=Path, required=True)
    parser.add_argument("--decision-config", type=Path, required=True)
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args(argv)
    root = args.repository_root.resolve(strict=False)
    boundary = RepoBoundary(root)
    boundary.assert_active_root(root)
    source_receipt = VerifiedReleaseReceipt.from_manifest(
        args.source_census_manifest, boundary
    )
    decision = load_penalty_decision(
        args.decision_config,
        source_receipt=source_receipt,
        boundary=boundary,
    )
    payload = build_conservative_penalty_payload(
        source_receipt,
        boundary=boundary,
        preregistered_penalty_count=decision["preregistered_penalty_count"],
        safety_margin=decision["safety_margin"],
    )
    receipt = None
    if args.publish:
        operation = OperationReceipt.issue_local(
            boundary,
            operation="PUBLISH_RELEASE",
            classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
            scope={
                "census_sha256": str(payload["census_sha256"]),
                "historical_execution_authorized": "false",
                "decision_id": str(decision["decision_id"]),
                "preregistered_penalty_count": str(
                    decision["preregistered_penalty_count"]
                ),
                "source_census_release_id": source_receipt.release_id,
                "trusted_gate": "multiplicity_only",
            },
        )
        publisher = AtomicPublisher(
            boundary=boundary,
            operation_receipt=operation,
            lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
        )
        receipt = publish_conservative_penalty_census(
            source_receipt=source_receipt,
            boundary=boundary,
            publisher=publisher,
            preregistered_penalty_count=decision["preregistered_penalty_count"],
            safety_margin=decision["safety_margin"],
        )
    summary = {
        "census_sha256": payload["census_sha256"],
        "exact_count_state": payload["exact_count_state"],
        "historical_execution_authorized": False,
        "preregistered_penalty_count": payload["preregistered_penalty_count"],
        "published": receipt is not None,
        "release_receipt": receipt.as_dict() if receipt is not None else None,
        "source_census_release_id": source_receipt.release_id,
        "status": payload["status"],
        "unresolved_reference_count": payload["unresolved_reference_count"],
    }
    print(canonical_bytes(summary).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CONSERVATIVE_CENSUS_FILENAME",
    "CONSERVATIVE_CENSUS_SCHEMA_VERSION",
    "CONSERVATIVE_CENSUS_STATUS",
    "DECISION_SCHEMA_VERSION",
    "build_conservative_penalty_payload",
    "load_conservative_penalty_census",
    "load_penalty_decision",
    "publish_conservative_penalty_census",
    "validate_conservative_penalty_payload",
]
