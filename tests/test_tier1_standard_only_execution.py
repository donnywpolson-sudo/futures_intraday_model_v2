from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification, OperationReceipt, RepoBoundary,
)
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_standard_only_execution import (
    OPERATION, OUTPUT_ROOT, PLAN_PATH, _required_scope,
    claim_execution_authorization, load_execution_plan,
    load_registered_execution_context, resolve_authorized_source_streams,
)


ROOT = Path(__file__).resolve().parents[1]


def _receipt(
    *, boundary: RepoBoundary, trial_id: str, plan: dict[str, object],
    output_root: Path,
) -> OperationReceipt:
    required = _required_scope(
        trial_id=trial_id, plan=plan, output_root=output_root,
    )
    return OperationReceipt.issue_user_approved(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={
            key: value for key, value in required.items()
            if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
        },
        approval_command=OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=str(plan["plan_sha256"]),
        approval_line=(
            f"APPROVE {OPERATION} PLAN {plan['plan_id']} "
            f"SHA256 {plan['plan_sha256']}"
        ),
    )


def test_standard_only_execution_plan_is_exact_and_fail_closed() -> None:
    plan = load_execution_plan(root=ROOT)
    assert plan["execution_mode"] == "IN_MEMORY_UNPUBLISHED_RESULT"
    assert plan["maximum_host_runtime_seconds"] == 900
    assert plan["selected_missing_execution_path_result"] == (
        "INCONCLUSIVE_DATA_OR_COVERAGE"
    )
    assert plan["runner_up_substitution"] is False
    assert plan["zero_return_imputation"] is False
    assert all(plan["forbidden_actions"].values())


def test_standard_only_execution_requires_published_active_context(tmp_path: Path) -> None:
    plan = load_execution_plan(root=ROOT)
    with pytest.raises(IntegrityError, match="execution artifact"):
        load_registered_execution_context(root=tmp_path, plan=plan)


def test_standard_only_authorization_is_exact_durable_and_single_use(
    tmp_path: Path,
) -> None:
    boundary = RepoBoundary(tmp_path)
    plan = {
        "plan_id": "b" * 64,
        "plan_sha256": "c" * 64,
        "selected_sources_id": "d" * 64,
    }
    trial_id = "a" * 64
    output_root = tmp_path / OUTPUT_ROOT
    receipt = _receipt(
        boundary=boundary, trial_id=trial_id, plan=plan,
        output_root=output_root,
    )
    claim = claim_execution_authorization(
        root=tmp_path, boundary=boundary, receipt=receipt,
        trial_id=trial_id, plan=plan, output_root=output_root,
    )
    assert claim.exists()
    with pytest.raises(UnauthorizedOperation, match="already consumed"):
        claim_execution_authorization(
            root=tmp_path, boundary=boundary, receipt=receipt,
            trial_id=trial_id, plan=plan, output_root=output_root,
        )


def test_standard_only_source_resolution_rejects_2025_before_catalog_or_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    boundary = RepoBoundary(tmp_path)
    selected = {
        **{
            f"{market}/{year}": {
                "release_id": f"{market}-{year}", "payload_sha256": "a" * 64,
            }
            for market in ("6E", "CL", "ES", "ZN")
            for year in range(2018, 2023)
        },
        "ES/2025": {"release_id": "forbidden", "payload_sha256": "b" * 64},
    }
    monkeypatch.setattr(
        "futures_rebuild.tier1_standard_only_execution._load_selected_sources",
        lambda root: selected,
    )
    monkeypatch.setattr(
        "futures_rebuild.tier1_standard_only_execution.sha256_json",
        lambda value: "c" * 64,
    )
    monkeypatch.setattr(
        "futures_rebuild.tier1_standard_only_execution._catalog",
        lambda: (_ for _ in ()).throw(AssertionError("catalog must not be read")),
    )
    with pytest.raises(UnauthorizedOperation, match="holdout or forward"):
        resolve_authorized_source_streams(
            root=tmp_path, boundary=boundary, selected_sources_id="c" * 64,
        )


def test_execution_plan_is_bound_before_registration() -> None:
    plan = load_execution_plan(root=ROOT)
    assert sha256_file(ROOT / PLAN_PATH)
    assert plan["estimated_external_cost_usd"] == "0"
