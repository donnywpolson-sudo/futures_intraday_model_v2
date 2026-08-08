from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification, OperationReceipt, RepoBoundary,
)
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_authoritative_execution import (
    OPERATION, OUTPUT_ROOT, _required_scope, claim_authoritative_execution,
    load_authoritative_execution_plan, load_authoritative_registered_context,
    validate_registered_context_documents,
)
from futures_rebuild.tier1_authoritative_lifecycle import (
    EXPECTED_PREPUBLICATION_POINTER_SHA256,
    prepare_authoritative_lifecycle, published_documents,
    replace_pointer_with_rollback,
)
from futures_rebuild.tier1_authoritative_protocol import (
    load_authoritative_effective_contract, load_authoritative_protocol,
    load_failed_final_closure_preparation,
)


ROOT = Path(__file__).resolve().parents[1]


def _prepared_documents() -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    plan = load_authoritative_execution_plan(root=ROOT)
    prepared = prepare_authoritative_lifecycle(root=ROOT)
    return plan, published_documents(prepared)


def test_failed_final_trial_is_preserved_and_classified_pre_data() -> None:
    closure = load_failed_final_closure_preparation(root=ROOT)
    assert closure["disposition"] == "INVALID_PRE_DATA_LIFECYCLE_POINTER_BINDING_DEFECT"
    assert closure["execution_status"]["historical_rows_opened"] is False
    assert closure["execution_status"]["authorization_claim_created"] is False
    assert "configs/active_tier1_trial.json" not in closure["preserved_immutable_bindings"]


def test_authoritative_protocol_has_no_mutable_pointer_binding_or_parameter_change() -> None:
    protocol = load_authoritative_protocol(root=ROOT)
    effective = load_authoritative_effective_contract(root=ROOT)
    assert protocol["inherited_research_specification"]["parameter_changes"] == []
    assert protocol["lifecycle_validity"]["mutable_active_pointer_is_protocol_binding"] is False
    assert "configs/active_tier1_trial.json" not in protocol["bindings"]
    assert effective["protocol_id"] == protocol["lineage"]["research_predecessor_protocol_id"]


def test_authoritative_plan_requires_post_activation_validation_and_sealed_evidence() -> None:
    plan = load_authoritative_execution_plan(root=ROOT)
    assert plan["success_requires_post_activation_registered_context_validation"] is True
    assert plan["success_requires_verified_unpublished_bundle"] is True
    assert plan["maximum_host_runtime_seconds"] == 900


def test_simulated_postpublication_documents_are_execution_ready() -> None:
    plan, documents = _prepared_documents()
    trial_id = validate_registered_context_documents(
        plan=plan, pointer=documents["pointer"], registry=documents["registry"],
        certificate=documents["certificate"],
        failed_retirement=documents["failed_retirement"],
    )
    assert trial_id == documents["registry"]["trial_id"]


def test_registered_context_rejects_pointer_or_retirement_drift() -> None:
    plan, documents = _prepared_documents()
    bad_pointer = deepcopy(documents["pointer"])
    bad_pointer["protocol_id"] = "0" * 64
    with pytest.raises(UnauthorizedOperation, match="registered context"):
        validate_registered_context_documents(
            plan=plan, pointer=bad_pointer, registry=documents["registry"],
            certificate=documents["certificate"],
            failed_retirement=documents["failed_retirement"],
        )
    bad_retirement = deepcopy(documents["failed_retirement"])
    bad_retirement["state"] = "RETIRED_AS_VALID"
    with pytest.raises(UnauthorizedOperation, match="registered context"):
        validate_registered_context_documents(
            plan=plan, pointer=documents["pointer"], registry=documents["registry"],
            certificate=documents["certificate"],
            failed_retirement=bad_retirement,
        )


def test_pointer_compare_and_swap_restores_exact_bytes_on_failed_postcheck(
    tmp_path: Path,
) -> None:
    pointer = tmp_path / "active.json"
    original = b'{"trial":"preserved"}\n'
    pointer.write_bytes(original)

    def fail() -> None:
        raise IntegrityError("synthetic postcheck failure")

    with pytest.raises(IntegrityError, match="synthetic postcheck"):
        replace_pointer_with_rollback(
            pointer_path=pointer, new_bytes=b'{"trial":"new"}\n',
            expected_old_sha256=sha256_file(pointer), postcheck=fail,
        )
    assert pointer.read_bytes() == original


def test_authoritative_execution_claim_is_exact_and_single_use(tmp_path: Path) -> None:
    boundary = RepoBoundary(tmp_path)
    trial_id = "a" * 64
    plan = {
        "plan_id": "b" * 64, "plan_sha256": "c" * 64,
        "selected_sources_id": "d" * 64,
    }
    output_root = tmp_path / OUTPUT_ROOT
    required = _required_scope(
        trial_id=trial_id, plan=plan, output_root=output_root,
    )
    receipt = OperationReceipt.issue_user_approved(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={
            key: value for key, value in required.items()
            if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
        },
        approval_command=OPERATION, approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=str(plan["plan_sha256"]),
        approval_line=(
            f"APPROVE {OPERATION} PLAN {plan['plan_id']} "
            f"SHA256 {plan['plan_sha256']}"
        ),
    )
    claim_authoritative_execution(
        root=tmp_path, boundary=boundary, receipt=receipt,
        trial_id=trial_id, plan=plan, output_root=output_root,
    )
    with pytest.raises(UnauthorizedOperation, match="already consumed"):
        claim_authoritative_execution(
            root=tmp_path, boundary=boundary, receipt=receipt,
            trial_id=trial_id, plan=plan, output_root=output_root,
        )


def test_transition_stable_test_uses_simulation_before_publication_or_loader_after() -> None:
    plan = load_authoritative_execution_plan(root=ROOT)
    authoritative_root = ROOT / "state/trial_registry/tier1_authoritative_trial"
    if authoritative_root.exists() and any(authoritative_root.glob("*.json")):
        trial_id, _, _, _ = load_authoritative_registered_context(root=ROOT, plan=plan)
        assert len(trial_id) == 64
    else:
        _, documents = _prepared_documents()
        assert len(validate_registered_context_documents(
            plan=plan, pointer=documents["pointer"], registry=documents["registry"],
            certificate=documents["certificate"],
            failed_retirement=documents["failed_retirement"],
        )) == 64


def test_current_pointer_is_only_a_prepublication_compare_and_swap_condition() -> None:
    protocol = load_authoritative_protocol(root=ROOT)
    assert protocol["lifecycle_validity"][
        "prepublication_expected_pointer_is_compare_and_swap_condition_only"
    ] is True
    authoritative_root = ROOT / "state/trial_registry/tier1_authoritative_trial"
    if authoritative_root.exists() and any(authoritative_root.glob("*.json")):
        plan = load_authoritative_execution_plan(root=ROOT)
        load_authoritative_registered_context(root=ROOT, plan=plan)
    else:
        assert sha256_file(ROOT / "configs/active_tier1_trial.json") == (
            EXPECTED_PREPUBLICATION_POINTER_SHA256
        )
