from __future__ import annotations

import ast
from copy import deepcopy
from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_authoritative_certified_execution import (
    INVALID_TRIAL_ID,
    validate_certified_registered_context_documents,
)
from futures_rebuild.tier1_authoritative_certified_lifecycle import (
    EXPECTED_PREPUBLICATION_POINTER_SHA256,
    INVALID_TEST_FILES,
    SUPERSEDED_NODE_IDS,
    load_692a_invalid_retirement_preparation,
    load_certified_synthetic_verification,
    prepare_certified_authoritative_lifecycle,
    transition_documents,
)
from futures_rebuild.tier1_authoritative_execution import (
    OPERATION,
    OUTPUT_ROOT,
    _required_scope,
    claim_authoritative_execution,
    load_authoritative_execution_plan,
)
from futures_rebuild.tier1_authoritative_lifecycle import replace_pointer_with_rollback
from futures_rebuild.tier1_authoritative_protocol import (
    load_authoritative_effective_contract,
    load_authoritative_protocol,
)
from scripts.publish_tier1_authoritative_certified_lifecycle import (
    certification_pytest_arguments,
)


ROOT = Path(__file__).resolve().parents[1]


def test_692a_is_preserved_and_classified_invalid_without_research_change() -> None:
    invalid = load_692a_invalid_retirement_preparation(root=ROOT)
    assert invalid["trial_id"] == INVALID_TRIAL_ID
    assert invalid["research_parameter_defect"] is False
    assert invalid["historical_rows_opened"] is False
    for path, digest in invalid["preserved_bindings"].items():
        assert sha256_file(ROOT / path) == digest


def test_certified_protocol_keeps_parameters_and_pointer_out_of_bindings() -> None:
    protocol = load_authoritative_protocol(root=ROOT)
    effective = load_authoritative_effective_contract(root=ROOT)
    assert protocol["inherited_research_specification"]["parameter_changes"] == []
    assert "configs/active_tier1_trial.json" not in protocol["bindings"]
    assert effective["protocol_id"] == protocol["lineage"]["research_predecessor_protocol_id"]


def test_certified_plan_keeps_execution_separately_authorized_and_sealed() -> None:
    plan = load_authoritative_execution_plan(root=ROOT)
    assert plan["success_requires_post_activation_registered_context_validation"] is True
    assert plan["success_requires_verified_unpublished_bundle"] is True
    assert plan["maximum_host_runtime_seconds"] == 900
    assert plan["estimated_external_cost_usd"] == "0"


def test_suite_selection_is_explicit_and_identical_by_construction() -> None:
    verification = load_certified_synthetic_verification(root=ROOT)
    selection = verification["selection"]
    assert selection["ignored_invalid_test_files"] == list(INVALID_TEST_FILES)
    assert selection["deselected_historical_assertions"] == list(SUPERSEDED_NODE_IDS)
    first = certification_pytest_arguments(root=ROOT)
    second = certification_pytest_arguments(root=ROOT)
    assert first == second
    assert all(path not in first for path in INVALID_TEST_FILES)
    for node_id in SUPERSEDED_NODE_IDS:
        assert f"--deselect={node_id}" in first


def test_transition_documents_validate_in_rollback_or_active_state() -> None:
    plan, documents = transition_documents(root=ROOT)
    trial_id = validate_certified_registered_context_documents(
        plan=plan,
        pointer=documents["pointer"],
        registry=documents["registry"],
        certificate=documents["certificate"],
        invalid_retirement=documents["failed_retirement"],
    )
    assert trial_id == documents["registry"]["trial_id"]


def test_transition_validator_rejects_pointer_retirement_and_gate_drift() -> None:
    plan, documents = transition_documents(root=ROOT)
    bad_pointer = deepcopy(documents["pointer"])
    bad_pointer["protocol_id"] = "0" * 64
    with pytest.raises(UnauthorizedOperation, match="certified authoritative context"):
        validate_certified_registered_context_documents(
            plan=plan,
            pointer=bad_pointer,
            registry=documents["registry"],
            certificate=documents["certificate"],
            invalid_retirement=documents["failed_retirement"],
        )
    bad_retirement = deepcopy(documents["failed_retirement"])
    bad_retirement["research_parameter_defect"] = True
    with pytest.raises(UnauthorizedOperation, match="certified authoritative context"):
        validate_certified_registered_context_documents(
            plan=plan,
            pointer=documents["pointer"],
            registry=documents["registry"],
            certificate=documents["certificate"],
            invalid_retirement=bad_retirement,
        )
    bad_certificate = deepcopy(documents["certificate"])
    bad_certificate["gates"][0]["status"] = "FAIL"
    with pytest.raises(UnauthorizedOperation, match="certified authoritative context"):
        validate_certified_registered_context_documents(
            plan=plan,
            pointer=documents["pointer"],
            registry=documents["registry"],
            certificate=bad_certificate,
            invalid_retirement=documents["failed_retirement"],
        )


def test_failed_activation_restores_pointer_bytes_exactly(tmp_path: Path) -> None:
    pointer = tmp_path / "active.json"
    original = b'{"trial":"preserved"}\n'
    pointer.write_bytes(original)

    def fail() -> None:
        raise IntegrityError("synthetic post-activation suite failure")

    with pytest.raises(IntegrityError, match="post-activation"):
        replace_pointer_with_rollback(
            pointer_path=pointer,
            new_bytes=b'{"trial":"new"}\n',
            expected_old_sha256=sha256_file(pointer),
            postcheck=fail,
        )
    assert pointer.read_bytes() == original


def test_certified_execution_claim_remains_exact_and_single_use(tmp_path: Path) -> None:
    boundary = RepoBoundary(tmp_path)
    plan = {"plan_id": "b" * 64, "plan_sha256": "c" * 64, "selected_sources_id": "d" * 64}
    trial_id = "a" * 64
    output_root = tmp_path / OUTPUT_ROOT
    required = _required_scope(trial_id=trial_id, plan=plan, output_root=output_root)
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={
            key: value
            for key, value in required.items()
            if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
        },
        approval_command=OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=str(plan["plan_sha256"]),
        approval_line=(
            f"APPROVE {OPERATION} PLAN {plan['plan_id']} SHA256 {plan['plan_sha256']}"
        ),
    )
    claim_authoritative_execution(
        root=tmp_path,
        boundary=boundary,
        receipt=receipt,
        trial_id=trial_id,
        plan=plan,
        output_root=output_root,
    )
    with pytest.raises(UnauthorizedOperation, match="already consumed"):
        claim_authoritative_execution(
            root=tmp_path,
            boundary=boundary,
            receipt=receipt,
            trial_id=trial_id,
            plan=plan,
            output_root=output_root,
        )


def test_certified_entrypoints_do_not_import_provider_holdout_or_trading_modules() -> None:
    paths = (
        ROOT / "src/futures_rebuild/tier1_authoritative_certified_execution.py",
        ROOT / "src/futures_rebuild/tier1_authoritative_certified_lifecycle.py",
        ROOT / "scripts/run_tier1_authoritative_certified_historical_execution.py",
        ROOT / "scripts/publish_tier1_authoritative_certified_lifecycle.py",
    )
    forbidden = ("databento", "broker", "order", "holdout", "2025")
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports = [
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        ] + [
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        ]
        assert not any(term in name.lower() for name in imports for term in forbidden)


def test_transition_selection_uses_pointer_identity_not_registry_presence() -> None:
    source = (ROOT / "src/futures_rebuild/tier1_authoritative_certified_lifecycle.py").read_text(
        encoding="utf-8"
    )
    function = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name == "transition_documents"
    )
    rendered = ast.unparse(function)
    assert "pointer.get('trial_id')" in rendered
    assert ".glob(" not in rendered
    assert ".exists(" not in rendered


def test_prepared_trial_binds_published_692a_and_omits_mutable_pointer() -> None:
    prepared = prepare_certified_authoritative_lifecycle(root=ROOT)
    bindings = prepared.trial["bindings"]
    assert "configs/active_tier1_trial.json" not in bindings
    assert (
        f"state/trial_registry/tier1_authoritative_trial/{INVALID_TRIAL_ID}.json"
        in bindings
    )
    assert prepared.trial["supersedes_invalid_trial_id"] == INVALID_TRIAL_ID
    assert len(prepared.certificate["gates"]) == 14
    assert all(gate["status"] == "PASS" for gate in prepared.certificate["gates"])


def test_current_pointer_is_fail_closed_until_separate_publication() -> None:
    pointer_hash = sha256_file(ROOT / "configs/active_tier1_trial.json")
    if pointer_hash == EXPECTED_PREPUBLICATION_POINTER_SHA256:
        prepared = prepare_certified_authoritative_lifecycle(root=ROOT)
        assert prepared.pointer["state"] == "PREPARED_REQUIRES_PUBLICATION_APPROVAL"
    else:
        _, documents = transition_documents(root=ROOT)
        assert documents["pointer"]["state"] == "ACTIVE_REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
