from __future__ import annotations

import ast
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
    load_authoritative_execution_plan, validate_registered_context_documents,
)
from futures_rebuild.tier1_authoritative_lifecycle import (
    EXPECTED_PREPUBLICATION_POINTER_SHA256, replace_pointer_with_rollback,
)
from futures_rebuild.tier1_authoritative_protocol import (
    load_authoritative_effective_contract, load_authoritative_protocol,
)
from futures_rebuild.tier1_authoritative_stable_lifecycle import (
    load_invalid_unpublished_preparation, transition_documents,
)


ROOT = Path(__file__).resolve().parents[1]


def test_prior_unpublished_preparation_is_preserved_and_invalid() -> None:
    invalid = load_invalid_unpublished_preparation(root=ROOT)
    assert invalid["prepared_trial_id"] == (
        "461cffa140e546b57c91609a8cea18e4b7506b517c75010b320aa59295264167"
    )
    assert invalid["finding"]["research_parameter_defect"] is False
    assert invalid["lifecycle_status"]["published"] is False


def test_stable_protocol_keeps_parameters_and_pointer_out_of_bindings() -> None:
    protocol = load_authoritative_protocol(root=ROOT)
    effective = load_authoritative_effective_contract(root=ROOT)
    assert protocol["inherited_research_specification"]["parameter_changes"] == []
    assert "configs/active_tier1_trial.json" not in protocol["bindings"]
    assert effective["protocol_id"] == protocol["lineage"]["research_predecessor_protocol_id"]


def test_stable_plan_requires_postactivation_validation_and_sealed_evidence() -> None:
    plan = load_authoritative_execution_plan(root=ROOT)
    assert plan["success_requires_post_activation_registered_context_validation"] is True
    assert plan["success_requires_verified_unpublished_bundle"] is True
    assert plan["maximum_host_runtime_seconds"] == 900


def test_transition_documents_validate_in_current_lifecycle_state() -> None:
    plan, documents = transition_documents(root=ROOT)
    trial_id = validate_registered_context_documents(plan=plan, **documents)
    assert trial_id == documents["registry"]["trial_id"]


def test_transition_context_rejects_pointer_and_retirement_drift() -> None:
    plan, documents = transition_documents(root=ROOT)
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


def test_pointer_rollback_restores_exact_bytes_after_failed_postcheck(
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


def test_stable_execution_claim_is_exact_and_single_use(tmp_path: Path) -> None:
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


def test_every_selected_test_uses_transition_loader_not_preparation_loader() -> None:
    source = Path(__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    called_names = {
        node.func.id for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "prepare_stable_authoritative_lifecycle" not in called_names
    assert source.count("transition_documents(root=ROOT)") >= 2


def test_current_pointer_state_is_valid_before_or_after_publication() -> None:
    _, documents = transition_documents(root=ROOT)
    active_trial = documents["pointer"]["trial_id"]
    registry_trial = documents["registry"]["trial_id"]
    assert active_trial == registry_trial
    authoritative_root = ROOT / "state/trial_registry/tier1_authoritative_trial"
    if not authoritative_root.exists() or not any(authoritative_root.glob("*.json")):
        assert sha256_file(ROOT / "configs/active_tier1_trial.json") == (
            EXPECTED_PREPUBLICATION_POINTER_SHA256
        )
