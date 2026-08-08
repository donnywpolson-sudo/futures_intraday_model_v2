from __future__ import annotations

import ast
from copy import deepcopy
from pathlib import Path
from shutil import copyfile

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_authoritative_execution import (
    ACTIVE_POINTER_PATH,
    CERTIFICATE_ROOT,
    OPERATION,
    OUTPUT_ROOT,
    REGISTRY_ROOT,
    _required_scope,
    claim_authoritative_execution,
    load_authoritative_execution_plan,
)
from futures_rebuild.tier1_authoritative_lifecycle import replace_pointer_with_rollback
from futures_rebuild.tier1_authoritative_protocol import (
    load_authoritative_effective_contract,
    load_authoritative_protocol,
)
from futures_rebuild.tier1_authoritative_terminal_execution import (
    INVALID_RETIREMENT_ROOT,
    INVALID_TRIAL_ID,
    load_terminal_registered_context,
    validate_terminal_registered_context_documents,
)
from futures_rebuild.tier1_authoritative_terminal_lifecycle import (
    INVALID_TEST_FILES,
    SUPERSEDED_NODE_IDS,
    load_140f_invalid_retirement_preparation,
    load_terminal_synthetic_verification,
    synthetic_active_documents,
    transition_documents,
)
from scripts.publish_tier1_authoritative_terminal_lifecycle import (
    certification_pytest_arguments,
)


ROOT = Path(__file__).resolve().parents[1]


def test_140f_is_preserved_and_invalid_without_research_change() -> None:
    invalid = load_140f_invalid_retirement_preparation(root=ROOT)
    assert invalid["trial_id"] == INVALID_TRIAL_ID
    assert invalid["research_parameter_defect"] is False
    assert invalid["historical_rows_opened"] is False
    for path, digest in invalid["preserved_bindings"].items():
        assert sha256_file(ROOT / path) == digest


def test_terminal_protocol_keeps_parameters_and_pointer_out_of_bindings() -> None:
    protocol = load_authoritative_protocol(root=ROOT)
    effective = load_authoritative_effective_contract(root=ROOT)
    assert protocol["inherited_research_specification"]["parameter_changes"] == []
    assert "configs/active_tier1_trial.json" not in protocol["bindings"]
    assert effective["protocol_id"] == protocol["lineage"]["research_predecessor_protocol_id"]


def test_terminal_plan_remains_separately_authorized_and_sealed() -> None:
    plan = load_authoritative_execution_plan(root=ROOT)
    assert plan["success_requires_post_activation_registered_context_validation"] is True
    assert plan["success_requires_verified_unpublished_bundle"] is True
    assert plan["maximum_host_runtime_seconds"] == 900
    assert plan["estimated_external_cost_usd"] == "0"


def test_terminal_suite_selection_is_exact_and_state_neutral() -> None:
    verification = load_terminal_synthetic_verification(root=ROOT)
    selection = verification["selection"]
    assert selection["ignored_invalid_test_files"] == list(INVALID_TEST_FILES)
    assert selection["deselected_historical_assertions"] == list(SUPERSEDED_NODE_IDS)
    assert selection["selected_tests_may_call_prepublication_builder"] is False
    assert certification_pytest_arguments(root=ROOT) == certification_pytest_arguments(root=ROOT)


def test_synthetic_active_documents_validate_without_pointer_dependency() -> None:
    plan, documents = synthetic_active_documents(root=ROOT)
    trial_id = validate_terminal_registered_context_documents(
        plan=plan,
        pointer=documents["pointer"],
        registry=documents["registry"],
        certificate=documents["certificate"],
        invalid_retirement=documents["failed_retirement"],
    )
    assert trial_id == documents["registry"]["trial_id"]


def test_live_transition_documents_validate_before_or_after_activation() -> None:
    plan, documents = transition_documents(root=ROOT)
    assert validate_terminal_registered_context_documents(
        plan=plan,
        pointer=documents["pointer"],
        registry=documents["registry"],
        certificate=documents["certificate"],
        invalid_retirement=documents["failed_retirement"],
    ) == documents["registry"]["trial_id"]


def test_real_loader_accepts_complete_shadow_active_repository(tmp_path: Path) -> None:
    plan, documents = synthetic_active_documents(root=ROOT)
    registry = documents["registry"]
    trial_id = registry["trial_id"]
    retirement_id = registry["invalid_retirement_id"]
    for relative in registry["bindings"]:
        source = ROOT / relative
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        copyfile(source, destination)
    payloads = {
        ACTIVE_POINTER_PATH: documents["pointer"],
        REGISTRY_ROOT / f"{trial_id}.json": registry,
        CERTIFICATE_ROOT / f"{trial_id}.json": documents["certificate"],
        INVALID_RETIREMENT_ROOT / f"{retirement_id}.json": documents["failed_retirement"],
    }
    for relative, payload in payloads.items():
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(canonical_bytes(payload) + b"\n")
    loaded_id, _, loaded_registry, _ = load_terminal_registered_context(
        root=tmp_path, plan=plan
    )
    assert loaded_id == trial_id
    assert loaded_registry == registry


def test_terminal_validator_rejects_pointer_retirement_and_gate_drift() -> None:
    plan, documents = synthetic_active_documents(root=ROOT)
    bad_pointer = deepcopy(documents["pointer"])
    bad_pointer["protocol_id"] = "0" * 64
    with pytest.raises(UnauthorizedOperation, match="terminal authoritative context"):
        validate_terminal_registered_context_documents(
            plan=plan,
            pointer=bad_pointer,
            registry=documents["registry"],
            certificate=documents["certificate"],
            invalid_retirement=documents["failed_retirement"],
        )
    bad_retirement = deepcopy(documents["failed_retirement"])
    bad_retirement["research_parameter_defect"] = True
    with pytest.raises(UnauthorizedOperation, match="terminal authoritative context"):
        validate_terminal_registered_context_documents(
            plan=plan,
            pointer=documents["pointer"],
            registry=documents["registry"],
            certificate=documents["certificate"],
            invalid_retirement=bad_retirement,
        )
    bad_certificate = deepcopy(documents["certificate"])
    bad_certificate["gates"][0]["status"] = "FAIL"
    with pytest.raises(UnauthorizedOperation, match="terminal authoritative context"):
        validate_terminal_registered_context_documents(
            plan=plan,
            pointer=documents["pointer"],
            registry=documents["registry"],
            certificate=bad_certificate,
            invalid_retirement=documents["failed_retirement"],
        )


def test_failed_terminal_activation_restores_exact_pointer_bytes(tmp_path: Path) -> None:
    pointer = tmp_path / "active.json"
    original = b'{"trial":"preserved"}\n'
    pointer.write_bytes(original)

    def fail() -> None:
        raise IntegrityError("synthetic terminal postcheck failure")

    with pytest.raises(IntegrityError, match="terminal postcheck"):
        replace_pointer_with_rollback(
            pointer_path=pointer,
            new_bytes=b'{"trial":"new"}\n',
            expected_old_sha256=sha256_file(pointer),
            postcheck=fail,
        )
    assert pointer.read_bytes() == original


def test_terminal_execution_claim_is_exact_and_single_use(tmp_path: Path) -> None:
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


def test_terminal_selected_tests_cannot_call_prepublication_builder() -> None:
    path = ROOT / "tests/test_tier1_authoritative_terminal_lifecycle.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    calls = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "prepare_terminal_authoritative_lifecycle" not in calls


def test_terminal_entrypoints_import_no_provider_holdout_or_trading_modules() -> None:
    paths = (
        ROOT / "src/futures_rebuild/tier1_authoritative_terminal_execution.py",
        ROOT / "src/futures_rebuild/tier1_authoritative_terminal_lifecycle.py",
        ROOT / "scripts/run_tier1_authoritative_terminal_historical_execution.py",
        ROOT / "scripts/publish_tier1_authoritative_terminal_lifecycle.py",
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


def test_terminal_trial_binds_140f_and_omits_mutable_pointer_in_all_states() -> None:
    _, documents = transition_documents(root=ROOT)
    registry = documents["registry"]
    assert "configs/active_tier1_trial.json" not in registry["bindings"]
    assert (
        f"state/trial_registry/tier1_authoritative_trial/{INVALID_TRIAL_ID}.json"
        in registry["bindings"]
    )
    assert registry["supersedes_invalid_trial_id"] == INVALID_TRIAL_ID
    assert len(documents["certificate"]["gates"]) == 14
