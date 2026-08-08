import os
import json
from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    PERSONAL_APPROVAL_ALGORITHM,
    PERSONAL_APPROVAL_AUTHORITY_ID,
)
from futures_rebuild.errors import ContractError, UnauthorizedOperation
from futures_rebuild.release import AtomicPublisher
from futures_rebuild.research_gateway_policy import (
    CERTIFIED_GATEWAY_SCHEMA,
    CERTIFIED_TRIAL_EXECUTION_OPERATION,
)


@pytest.mark.parametrize("component", ["staging", "publication", "lock"])
@pytest.mark.parametrize("root_kind", ["legacy", "stock", "outside"])
def test_release_writer_rejects_every_nonactive_root_before_creating_paths(
    boundary, operation_factory, tmp_path, component, root_kind
) -> None:
    roots = {
        "legacy": boundary.legacy_roots[0],
        "stock": boundary.foreign_roots[0],
        "outside": tmp_path / "unscoped",
    }
    forbidden = roots[root_kind] / "must-not-exist" / component
    arguments = {
        "staging": boundary.active_root / "data" / "vault" / ".staging" / "releases" / "test",
        "publication": boundary.active_root / "data" / "vault" / "releases",
        "lock": boundary.active_root / "state" / "locks" / "publish.lock",
    }
    arguments[component] = forbidden
    with pytest.raises(UnauthorizedOperation):
        AtomicPublisher(
            arguments["staging"],
            arguments["publication"],
            arguments["lock"],
            boundary=boundary,
            operation_receipt=operation_factory("PUBLISH_RELEASE"),
        )
    assert not forbidden.exists()


def test_boundary_requires_exact_content_addressed_snapshot_location(boundary) -> None:
    valid = boundary.active_root / "data" / "vault" / "source_snapshots" / ("a" * 64)
    assert boundary.assert_snapshot_path(valid) == valid.resolve(strict=False)
    for invalid in (
        boundary.active_root / "data" / "vault" / "source_snapshots" / "not-a-hash",
        valid / "nested",
        boundary.active_root / "data" / "vault" / "other" / ("a" * 64),
    ):
        with pytest.raises(UnauthorizedOperation):
            boundary.assert_snapshot_path(invalid)


def test_local_code_cannot_issue_candidate_or_real_history_authority(boundary) -> None:
    for classification in (
        OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    ):
        with pytest.raises(UnauthorizedOperation):
            OperationReceipt.issue_local(
                boundary,
                operation="FORBIDDEN",
                classification=classification,
            )


def test_exact_personal_approval_is_hash_bound_and_single_use(boundary) -> None:
    plan_id = "a" * 64
    plan_sha256 = "b" * 64
    command = CERTIFIED_TRIAL_EXECUTION_OPERATION
    approval = f"APPROVE {command} PLAN {plan_id} SHA256 {plan_sha256}"
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={
            "gateway_schema": CERTIFIED_GATEWAY_SCHEMA,
            "operation_kind": "TRIAL_HISTORICAL_EXECUTION",
            "trial_id": "1" * 64,
            "trial_family": "test_family",
            "protocol_id": "test_protocol",
            "registration_path": "state/trial_registry/test/" + "1" * 64 + ".json",
            "registration_sha256": "2" * 64,
            "readiness_certificate_id": "3" * 64,
            "readiness_evidence_sha256": "4" * 64,
            "alpha_ladder_contract_id": "5" * 64,
            "alpha_ladder_profile_id": "6" * 64,
            "alpha_ladder_stage": "tier_1",
            "mechanism_sha256": "7" * 64,
            "predecessor_decision_sha256": "8" * 64,
            "session_manifest_sha256": "9" * 64,
            "pilot_evaluation_session_ids_sha256": "a" * 64,
        },
        approval_command=command,
        approval_plan_id=plan_id,
        approval_plan_sha256=plan_sha256,
        approval_line=approval,
    )
    assert receipt.authority_key_id == PERSONAL_APPROVAL_AUTHORITY_ID
    assert receipt.signature_algorithm == PERSONAL_APPROVAL_ALGORITHM
    assert receipt.signature_hex == __import__("hashlib").sha256(
        approval.encode("utf-8")
    ).hexdigest()
    required_scope = dict(receipt.scope)
    receipt.consume(
        boundary,
        operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope,
    )
    with pytest.raises(UnauthorizedOperation, match="already used"):
        receipt.consume(
            boundary,
            operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=required_scope,
        )


def test_personal_approval_requires_the_literal_plan_bound_line(boundary) -> None:
    with pytest.raises(UnauthorizedOperation, match="exact personal-project approval"):
        OperationReceipt.issue_user_approved(
            boundary,
            operation="RUN_TRIAL_106",
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            scope={"trial_id": "106"},
            approval_command="RUN_BOUNDED_REAL_HISTORY_TRIAL",
            approval_plan_id="a" * 64,
            approval_plan_sha256="b" * 64,
            approval_line="APPROVE SOMETHING ELSE",
        )


def test_unknown_real_history_operation_fails_before_claim(boundary) -> None:
    command = "RUN_BOUNDED_REAL_HISTORY_TRIAL"
    plan_id = "a" * 64
    plan_sha256 = "b" * 64
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation="RUN_TRIAL_106",
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={"trial_id": "1" * 64},
        approval_command=command,
        approval_plan_id=plan_id,
        approval_plan_sha256=plan_sha256,
        approval_line=f"APPROVE {command} PLAN {plan_id} SHA256 {plan_sha256}",
    )
    with pytest.raises(UnauthorizedOperation, match="retired outside"):
        receipt.consume(
            boundary,
            operation="RUN_TRIAL_106",
            classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
            required_scope=dict(receipt.scope),
        )
    assert not (
        boundary.active_root / "state" / "authorization_uses"
        / f"{receipt.receipt_id}.json"
    ).exists()


def test_forged_personal_approval_digest_is_rejected(boundary) -> None:
    plan_id = "a" * 64
    plan_sha256 = "b" * 64
    command = "SEAL_PERSONAL_CANDIDATE"
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation="SEAL_CANDIDATE_BUNDLE",
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        scope={"candidate_id": "c" * 64},
        approval_command=command,
        approval_plan_id=plan_id,
        approval_plan_sha256=plan_sha256,
        approval_line=f"APPROVE {command} PLAN {plan_id} SHA256 {plan_sha256}",
    )
    payload = {**receipt.as_dict(), "signature_hex": "0" * 64}
    path = boundary.active_root / "state" / "authorizations" / "forged.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(UnauthorizedOperation, match="valid exact user approval"):
        OperationReceipt.load_external(path, boundary)


def test_verified_release_receipt_requires_exact_active_release_tree(
    boundary, operation_factory
) -> None:
    with pytest.raises(UnauthorizedOperation):
        AtomicPublisher(
            boundary.active_root / "data" / "vault" / ".staging" / "releases" / "other",
            boundary.active_root / "other-releases",
            boundary.active_root / "state" / "locks" / "other.lock",
            boundary=boundary,
            operation_receipt=operation_factory("PUBLISH_RELEASE"),
        )


def test_release_verification_rejects_hardlinked_payload(
    boundary, release_factory
) -> None:
    release, receipt = release_factory(
        release_kind="synthetic_hardlink_test",
        filename="rows.bin",
        content=b"synthetic",
    )
    payload = release / "rows.bin"
    alias = boundary.active_root / "state" / "hardlink-alias.bin"
    alias.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(payload, alias)
    except OSError as exc:
        pytest.skip(f"hard links unavailable on this filesystem: {exc}")
    with pytest.raises(ContractError, match="Hard-linked|hard-linked"):
        receipt.verify(boundary)
