from __future__ import annotations

from pathlib import Path

from futures_rebuild.active_data_plan import (
    POLICY_APPROVAL_SCHEMA,
    build_policy_pending_approval,
    build_policy_successor_plan,
    build_pilot_plan,
    derive_inventory,
    validate_policy_approval,
    verify_policy_acceptance,
)
from futures_rebuild.active_data_view import _resolve_certification_workspace
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
import pytest


FOUNDATION_RELEASE_ID = (
    "637f16b3c23c9f2215858f49754965738fe9c00095661d7a29d6877d566ae5e3"
)
FOUNDATION_MANIFEST_SHA256 = (
    "969079b6576417658ede21e63b00b9d2211856157d01ef10c3ebb0d77cca2ad9"
)
POLICY_RELEASE_ID = (
    "cb3e9ad469301debdb1550efdce3df06b0b1abb61906ead2d949d04ac53a77a2"
)
POLICY_ACCEPTANCE_RECEIPT_ID = (
    "f0e72fb839da051d92d5711754bccb95f16484bc5e1b1e1fcf0c7ff1042abdcd"
)


def _root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_live_manifest_only_inventory_reconciles_reviewed_snapshot() -> None:
    inventory = derive_inventory(
        repository_root=_root(), foundation_release_id=FOUNDATION_RELEASE_ID
    )

    assert inventory["foundation_manifest_sha256"] == FOUNDATION_MANIFEST_SHA256
    assert inventory["counts"] == {
        "certification_candidates": 562,
        "discovery_selection_eligible": 198,
        "forward_only": 41,
        "holdout": 41,
        "market_count": 41,
        "market_year_count": 650,
        "quarantined": 6,
        "selected_interval_count": 562,
        "selected_row_count": 133_590_158,
        "split_market_years": 0,
    }
    quarantine = sorted(
        f"{item['market']}/{item['year']}"
        for item in inventory["entries"]
        if item["disposition"] == "QUARANTINED_NOT_MATERIALIZED"
    )
    assert quarantine == [
        "KE/2019",
        "KE/2021",
        "KE/2023",
        "KE/2024",
        "SR1/2020",
        "SR3/2020",
    ]
    assert all(
        item["year"] < 2025
        for item in inventory["entries"]
        if item["disposition"] == "RESEARCH_READY_CAUSAL_PRICE"
    )
    forbidden = {
        "feature_input_release_receipt",
        "outcome_source_input_release_receipt",
        "status_eligibility_release_receipt",
    }
    assert all(
        forbidden.isdisjoint(interval)
        for item in inventory["entries"]
        for interval in item["intervals"]
    )


def test_price_policy_successor_plan_is_non_authorizing_and_self_hashed() -> None:
    plan = build_policy_successor_plan(
        repository_root=_root(), foundation_release_id=FOUNDATION_RELEASE_ID
    )
    approval = build_policy_pending_approval(plan)

    assert plan["plan_id"] == sha256_json(
        {key: value for key, value in plan.items() if key != "plan_id"}
    )
    assert plan["policy"]["pre_2025_status_dependent_use"] == "FORBIDDEN"
    assert plan["policy"]["selection_rule"] == (
        "DISCOVERY_SELECTION_AND_CERTIFIED_ONLY"
    )
    assert "MODEL_FIT" in plan["policy"]["does_not_authorize"]
    assert "ACTIVE_VIEW_PUBLICATION" in plan["policy"]["does_not_authorize"]
    assert approval["status"] == "PENDING"
    assert approval["approval_receipt_id"] is None

    with pytest.raises(UnauthorizedOperation, match="exact hash-bound"):
        validate_policy_approval(approval, plan)
    core = {
        "approved_at": "2026-07-27T20:00:00Z",
        "operation": plan["operation"],
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_json(plan),
        "schema_version": POLICY_APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "a" * 64,
    }
    accepted = {**core, "approval_receipt_id": sha256_json(core)}
    assert validate_policy_approval(accepted, plan) == accepted["approval_receipt_id"]


def test_published_price_policy_acceptance_is_immutable_and_non_authorizing() -> None:
    receipt = verify_policy_acceptance(
        repository_root=_root(),
        policy_release_id=POLICY_RELEASE_ID,
        policy_acceptance_receipt_id=POLICY_ACCEPTANCE_RECEIPT_ID,
    )

    assert receipt["state"] == "ACCEPTED_NON_AUTHORIZING"
    assert receipt["policy_release_id"] == POLICY_RELEASE_ID


def test_pilot_plan_is_exactly_two_market_years_and_non_authorizing() -> None:
    plan, approval = build_pilot_plan(
        repository_root=_root(),
        foundation_release_id=FOUNDATION_RELEASE_ID,
        accepted_policy_release_id=POLICY_RELEASE_ID,
        policy_acceptance_receipt_id=POLICY_ACCEPTANCE_RECEIPT_ID,
    )

    assert [(entry["market"], entry["year"]) for entry in plan["entries"]] == [
        ("6A", 2010),
        ("ES", 2022),
    ]
    assert [entry["coverage_kind"] for entry in plan["entries"]] == [
        "PARTIAL_YEAR",
        "FULL_YEAR",
    ]
    assert all(
        len(interval["aggregation_sources"]) == 2
        for entry in plan["entries"]
        for interval in entry["intervals"]
    )
    assert len(plan["pilot_run_ids"]) == 2
    assert len(set(plan["pilot_run_ids"])) == 2
    assert plan["pilot_scope_id"] == sha256_json(
        {
            "entries": plan["entries"],
            "environment_bindings": plan["environment_bindings"],
            "foundation_release_id": plan["foundation_release_id"],
            "implementation_bindings": plan["implementation_bindings"],
            "semantic_bindings": plan["semantic_bindings"],
            "source_objects": plan["source_objects"],
        }
    )
    assert plan["pilot_run_ids"] == [
        sha256_json(
            {
                "pilot_scope_id": plan["pilot_scope_id"],
                "run_number": run_number,
            }
        )
        for run_number in (1, 2)
    ]
    assert plan["limits"]["maximum_candidates"] == 2
    assert plan["limits"]["maximum_workers"] == 1
    assert "PROVIDER_CALL_OR_DOWNLOAD" in plan["forbidden_actions"]
    assert "ACTIVE_ROOT_MUTATION" in plan["forbidden_actions"]
    assert approval["status"] == "PENDING"


def test_pilot_certification_workspace_is_confined_to_declared_state_root() -> None:
    plan, _ = build_pilot_plan(
        repository_root=_root(),
        foundation_release_id=FOUNDATION_RELEASE_ID,
        accepted_policy_release_id=POLICY_RELEASE_ID,
        policy_acceptance_receipt_id=POLICY_ACCEPTANCE_RECEIPT_ID,
    )

    state_root, first_workspace = _resolve_certification_workspace(
        repository_root=_root(),
        plan=plan,
        run_id=plan["pilot_run_ids"][0],
        market="6A",
        year=2010,
    )
    _, second_workspace = _resolve_certification_workspace(
        repository_root=_root(),
        plan=plan,
        run_id=plan["pilot_run_ids"][1],
        market="ES",
        year=2022,
    )

    assert state_root == (
        _root()
        / "state"
        / "active_data_view_certification"
        / "pilot"
        / plan["pilot_scope_id"]
    )
    assert first_workspace.relative_to(state_root) == Path("run-1/6A/2010")
    assert second_workspace.relative_to(state_root) == Path("run-2/ES/2022")

    escaped = dict(plan)
    escaped["outputs"] = [
        *plan["outputs"][:2],
        "state/active_data_view_certification/pilot/../escape",
    ]
    with pytest.raises(IntegrityError, match="not canonical"):
        _resolve_certification_workspace(
            repository_root=_root(),
            plan=escaped,
            run_id=plan["pilot_run_ids"][0],
            market="6A",
            year=2010,
        )
