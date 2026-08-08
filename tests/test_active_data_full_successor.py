from pathlib import Path

import pytest

from futures_rebuild.active_data_full_successor import (
    ACTIVE_DATA_PLAN_IMPLEMENTATION,
    ACTIVE_DATA_VIEW_IMPLEMENTATION,
    ALLOWED_REASONS,
    RECONCILIATION_REFRESH_PATHS,
    SUCCESSOR_GENERATOR,
    _verify_reconciliation_plan_bindings,
    build_successor_plan,
)
from futures_rebuild.active_data_full_plan import (
    FULL_CERTIFICATION_EXECUTOR,
    FULL_PLAN_GENERATOR,
)
from futures_rebuild.active_data_full_supervisor import (
    SUPERVISOR_LAUNCHER_PATH,
    SUPERVISOR_PATH,
)
from futures_rebuild.active_data_plan import IMPLEMENTATION_PATHS
from futures_rebuild.active_data_view import sha256_json
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError


def test_full_certification_fail_closed_reasons_are_exact() -> None:
    assert ALLOWED_REASONS == frozenset(
        {
            "PINNED_ENVIRONMENT_MISMATCH",
            "UNEXPECTED_CONCURRENT_PYTEST",
            "UNEXPLAINED_PROCESS_TERMINATION",
        }
    )
    assert RECONCILIATION_REFRESH_PATHS == frozenset(
        {
            ACTIVE_DATA_PLAN_IMPLEMENTATION,
            ACTIVE_DATA_VIEW_IMPLEMENTATION,
            FULL_CERTIFICATION_EXECUTOR,
            FULL_PLAN_GENERATOR,
            SUPERVISOR_LAUNCHER_PATH,
            SUPERVISOR_PATH,
            SUCCESSOR_GENERATOR,
        }
    )


def _reconciliation_binding_plan(tmp_path: Path) -> tuple[dict[str, object], Path]:
    source_root = Path(__file__).resolve().parents[1]
    for relative in RECONCILIATION_REFRESH_PATHS:
        copied = tmp_path / relative
        copied.parent.mkdir(parents=True, exist_ok=True)
        copied.write_bytes((source_root / relative).read_bytes())
    other_relative = "src/futures_rebuild/other_binding.py"
    other_copy = tmp_path / other_relative
    other_copy.write_bytes(b"preserved binding\n")
    plan: dict[str, object] = {
        "environment_bindings": {"environment": "b" * 64},
        "implementation_bindings": {
            **{relative: "a" * 64 for relative in RECONCILIATION_REFRESH_PATHS},
            other_relative: sha256_file(other_copy),
        },
        "semantic_bindings": {"policy": "c" * 64},
    }
    plan["plan_id"] = sha256_json(plan)
    return plan, other_copy


def test_reconciliation_accepts_only_declared_remediation_drift(
    tmp_path: Path,
) -> None:
    plan, _ = _reconciliation_binding_plan(tmp_path)

    _verify_reconciliation_plan_bindings(tmp_path, plan)

    assert all(
        plan["implementation_bindings"][relative] == "a" * 64
        for relative in RECONCILIATION_REFRESH_PATHS
    )
    assert plan["plan_id"] == sha256_json(
        {key: value for key, value in plan.items() if key != "plan_id"}
    )


def test_reconciliation_rejects_other_implementation_binding_drift(
    tmp_path: Path,
) -> None:
    plan, other_copy = _reconciliation_binding_plan(tmp_path)
    other_copy.write_bytes(b"drifted binding\n")

    with pytest.raises(
        IntegrityError,
        match=r"active-view binding changed: src/futures_rebuild/other_binding.py",
    ):
        _verify_reconciliation_plan_bindings(tmp_path, plan)


def test_interrupted_full_successor_uses_fresh_contained_scope(
    tmp_path: Path,
) -> None:
    source_root = Path(__file__).resolve().parents[1]
    refreshed_paths = {
        *IMPLEMENTATION_PATHS,
        FULL_PLAN_GENERATOR,
        FULL_CERTIFICATION_EXECUTOR,
        SUPERVISOR_LAUNCHER_PATH,
        SUPERVISOR_PATH,
        SUCCESSOR_GENERATOR,
    }
    for relative in refreshed_paths:
        copied = tmp_path / relative
        copied.parent.mkdir(parents=True, exist_ok=True)
        copied.write_bytes((source_root / relative).read_bytes())
    record_core = {
        "predecessor_plan_id": "a" * 64,
        "predecessor_scope_id": "b" * 64,
        "preservation_rule": "LEAVE_INTERRUPTED_WORKSPACE_UNCHANGED",
        "schema_version": "causal_active_full_interruption/1.0.0",
        "status": "INTERRUPTED_FAIL_CLOSED",
    }
    record = {**record_core, "interruption_id": sha256_json(record_core)}
    record_path = tmp_path / "manifests/interruptions/v5.json"
    record_path.parent.mkdir(parents=True)
    record_path.write_bytes(canonical_bytes(record) + b"\n")
    predecessor = {
        "certification_scope_id": "b" * 64,
        "entries": [],
        "environment_bindings": {"environment": "c" * 64},
        "foundation_release_id": "d" * 64,
        "implementation_bindings": {"executor": "e" * 64},
        "measured_projection": {},
        "limits": {"maximum_duration_seconds": 72_000},
        "operation": "CERTIFY_CAUSAL_ACTIVE_VIEW",
        "outputs": [
            f"reports/active_data_view/full/{'b' * 64}",
            f"state/active_data_view_certification/full/{'b' * 64}",
        ],
        "plan_id": "a" * 64,
        "semantic_bindings": {"policy": "f" * 64},
        "source_objects": [],
    }

    successor, approval = build_successor_plan(
        repository_root=tmp_path,
        predecessor_plan=predecessor,
        interruption_record_path=record_path,
        interruption_record=record,
    )

    scope_id = successor["certification_scope_id"]
    assert scope_id != predecessor["certification_scope_id"]
    assert successor["outputs"] == [
        f"reports/active_data_view/full/{scope_id}",
        f"state/active_data_view_certification/full/{scope_id}",
    ]
    assert successor["execution_attempt"]["attempt_number"] == 2
    assert (
        successor["implementation_bindings"][SUCCESSOR_GENERATOR]
        == sha256_file(tmp_path / SUCCESSOR_GENERATOR)
    )
    assert set(refreshed_paths) <= set(successor["implementation_bindings"])
    assert successor["plan_id"] == sha256_json(
        {key: value for key, value in successor.items() if key != "plan_id"}
    )
    assert approval["status"] == "PENDING"
    assert approval["plan_id"] == successor["plan_id"]
    assert successor["supervision"]["transport"] == (
        "WINDOWS_TASK_SCHEDULER_MANUAL_ONE_SHOT"
    )
