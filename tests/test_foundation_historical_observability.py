from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.foundation.historical_observability import (
    CALENDAR_CLAIM,
    PRE_STATUS_CAPABILITY,
    STATUS_CAPABILITY,
    UNCERTAINTY_RULE,
    build_historical_observability_coverage,
    build_foundation_observability_successor_payload,
    validate_publication_approval,
    load_historical_observability_policy,
    validate_historical_observability_coverage,
    validate_historical_observability_policy,
)


ROOT = Path(__file__).resolve().parents[1]
FOUNDATION_ID = (
    "78806ef01714c72f6da537c1b6e6f8b2e903b14728822b0daa31b4c6c75a8909"
)


def _predecessor() -> dict[str, object]:
    manifest = json.loads(
        (
            ROOT
            / "manifests"
            / "data_releases"
            / "foundation"
            / f"{FOUNDATION_ID}.json"
        ).read_text(encoding="utf-8")
    )
    return manifest["embedded_documents"]["foundation_set.json"]


def test_active_policy_binds_exact_approval_and_semantics() -> None:
    policy = load_historical_observability_policy(
        ROOT / "configs" / "historical_observability_policy.json"
    )
    approval = policy["approval"]
    approval_core = {
        key: value
        for key, value in approval.items()
        if key != "approval_receipt_id"
    }
    assert approval["approval_receipt_id"] == sha256_json(approval_core)
    assert policy["calendar_claim"] == CALENDAR_CLAIM
    assert policy["uncertainty_rule"] == UNCERTAINTY_RULE
    assert policy["interval_count"] == 683
    assert policy["research_scope_market_year_count"] == 82


def test_coverage_uses_manifest_observability_without_calendar_claims() -> None:
    policy = load_historical_observability_policy(
        ROOT / "configs" / "historical_observability_policy.json"
    )
    predecessor = _predecessor()
    coverage = build_historical_observability_coverage(
        predecessor,
        predecessor_release_id=FOUNDATION_ID,
        policy=policy,
    )
    assert coverage["interval_count"] == 683
    assert coverage["market_count"] == 41
    assert coverage["research_scope_market_year_count"] == 82
    assert coverage["research_scope_interval_count"] == 115
    assert coverage["pre_status_interval_count"] == 568
    assert coverage["quarantined_interval_count"] == 6
    capabilities = {item["capability"] for item in coverage["intervals"]}
    assert capabilities == {PRE_STATUS_CAPABILITY, STATUS_CAPABILITY}
    assert all(item["observed_bar_rows"] > 0 for item in coverage["intervals"])
    assert all(item["calendar_claim"] == CALENDAR_CLAIM for item in coverage["intervals"])
    assert all(item["uncertainty_rule"] == UNCERTAINTY_RULE for item in coverage["intervals"])
    forbidden = {"open", "close", "closed", "holiday", "halt", "pause"}
    assert not any(forbidden.intersection(item) for item in coverage["intervals"])
    assert (
        validate_historical_observability_coverage(
            coverage,
            predecessor=predecessor,
            predecessor_release_id=FOUNDATION_ID,
            policy=policy,
        )
        == coverage
    )


def test_policy_and_coverage_tampering_fail_closed() -> None:
    policy = load_historical_observability_policy(
        ROOT / "configs" / "historical_observability_policy.json"
    )
    tampered_policy = deepcopy(policy)
    tampered_policy["uncertainty_rule"] = "UNOBSERVED_TIME_IS_CLOSED"
    with pytest.raises(IntegrityError, match="semantics"):
        validate_historical_observability_policy(tampered_policy)

    predecessor = _predecessor()
    tampered_predecessor = deepcopy(predecessor)
    tampered_predecessor["intervals"][0]["status_epoch_gate"]["bar_rows"] = 0
    with pytest.raises(IntegrityError, match="observability"):
        build_historical_observability_coverage(
            tampered_predecessor,
            predecessor_release_id=FOUNDATION_ID,
            policy=policy,
        )


def test_builder_does_not_open_historical_source_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = load_historical_observability_policy(
        ROOT / "configs" / "historical_observability_policy.json"
    )
    predecessor = _predecessor()

    def forbidden_open(*args: object, **kwargs: object) -> None:
        raise AssertionError("historical payload access is forbidden")

    monkeypatch.setattr(Path, "open", forbidden_open)
    coverage = build_historical_observability_coverage(
        predecessor,
        predecessor_release_id=FOUNDATION_ID,
        policy=policy,
    )
    assert coverage["interval_count"] == 683


def test_schema7_successor_builder_is_metadata_only_and_content_addressed() -> None:
    policy_path = ROOT / "configs" / "historical_observability_policy.json"
    policy = load_historical_observability_policy(policy_path)
    predecessor = _predecessor()
    from futures_rebuild.canonical import sha256_file

    successor = build_foundation_observability_successor_payload(
        predecessor,
        predecessor_release_id=FOUNDATION_ID,
        policy=policy,
        policy_sha256=sha256_file(policy_path),
    )
    core = {
        key: value
        for key, value in successor.items()
        if key != "foundation_set_id"
    }
    assert successor["schema_version"] == "7.0.0"
    assert successor["foundation_set_id"] == sha256_json(core)
    assert successor["intervals"] == predecessor["intervals"]
    assert successor["provider_call_count"] == 0
    assert successor["historical_outcome_or_label_execution"] is False


def test_publication_requires_exact_plan_bound_approval() -> None:
    plan_path = (
        ROOT
        / "reports"
        / "foundation"
        / "dbn_empirical_observability_foundation_publication_plan.json"
    )
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    from futures_rebuild.canonical import sha256_file

    core = {
        "approved_at": "2026-07-28T01:00:00Z",
        "expected_foundation_release_id": (
            plan["scope"]["authority"]["expected_foundation_release_id"]
        ),
        "operation": "PUBLISH_DBN_EMPIRICAL_OBSERVABILITY_FOUNDATION_SUCCESSOR",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "schema_version": "foundation_observability_successor_approval/1.0.0",
        "status": "APPROVED",
        "user_authorization_id": "a" * 64,
    }
    approval = {**core, "approval_receipt_id": sha256_json(core)}
    assert (
        validate_publication_approval(
            approval,
            plan=plan,
            plan_sha256=sha256_file(plan_path),
        )
        == approval["approval_receipt_id"]
    )
    tampered = deepcopy(approval)
    tampered["expected_foundation_release_id"] = "b" * 64
    with pytest.raises(IntegrityError, match="approval"):
        validate_publication_approval(
            tampered,
            plan=plan,
            plan_sha256=sha256_file(plan_path),
        )
