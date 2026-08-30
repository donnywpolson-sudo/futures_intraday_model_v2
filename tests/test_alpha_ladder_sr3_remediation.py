from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_macro_successor import load_successor_contract
from futures_rebuild.alpha_ladder_sr3_remediation import (
    DIAGNOSTIC_PLAN_PATH, HISTORY_ROOT, POLICY_PATH, REMEDIABLE_CAUSE,
    SYNTHETIC_CERTIFICATE_PATH, _synthetic_bundle, aggregate_sr3_conservative,
    build_consumed_closure, build_diagnostic_plan, build_policy,
    build_synthetic_certificate, diagnose_sr3,
)
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.research_gateway_policy import (
    ALPHA_LADDER_SR3_REMEDIATION_OPERATION, require_current_real_history_operation,
)


ROOT = Path(__file__).resolve().parents[1]
ECONOMICS = {"point_value": Decimal("2500"), "expected_unit_qty": Decimal("2500")}
pytestmark = pytest.mark.current


def test_multiple_point_in_time_ticks_use_maximum_friction() -> None:
    dates = ("2020-01-02", "2020-01-03")
    bundles = (_synthetic_bundle(dates[0], 2_500_000), _synthetic_bundle(dates[1], 5_000_000))
    diagnostic = diagnose_sr3(dates, bundles, ECONOMICS)
    result = aggregate_sr3_conservative(load_successor_contract(ROOT), dates, bundles, ECONOMICS)
    assert diagnostic["causes"] == [REMEDIABLE_CAUSE]
    assert diagnostic["remediation_allowed"] is True
    assert result["target_horizon_movement_ticks"] == "30"
    assert result["conservative_round_trip_friction_ticks"] == {
        "value": "5.6", "tick_value_usd": "6.2500",
    }
    assert result["movement_to_cost_ratio"] == str(Decimal("30") / Decimal("5.6"))


def test_unit_quantity_drift_is_not_remediable() -> None:
    date = "2020-01-02"
    diagnostic = diagnose_sr3(
        (date,), (_synthetic_bundle(date, 2_500_000),),
        {"point_value": Decimal("2500"), "expected_unit_qty": Decimal("1")},
    )
    result = aggregate_sr3_conservative(
        load_successor_contract(ROOT), (date,), (_synthetic_bundle(date, 2_500_000),),
        {"point_value": Decimal("2500"), "expected_unit_qty": Decimal("1")},
    )
    assert diagnostic["primary_cause"] == "UNIT_QUANTITY_MISMATCH"
    assert diagnostic["remediation_allowed"] is False
    assert set(result.values()) == {None}


def test_single_tick_behavior_remains_unchanged() -> None:
    date = "2020-01-02"
    result = aggregate_sr3_conservative(
        load_successor_contract(ROOT), (date,), (_synthetic_bundle(date, 2_500_000),), ECONOMICS,
    )
    assert result["target_horizon_movement_ticks"] == "30"
    assert result["conservative_round_trip_friction_ticks"] == {
        "value": "5.6", "tick_value_usd": "6.2500",
    }


def test_within_pair_tick_change_is_not_remediable() -> None:
    date = "2020-01-02"
    bundle = _synthetic_bundle(date, 2_500_000)
    bundle["observations"][31]["min_price_increment_nano"] = 5_000_000
    diagnostic = diagnose_sr3((date,), (bundle,), ECONOMICS)
    assert diagnostic["primary_cause"] == "PAIR_TICK_CHANGED"
    assert diagnostic["remediation_allowed"] is False


def test_prepared_package_rebuilds_exactly_without_row_authority() -> None:
    closure = build_consumed_closure(ROOT)
    policy = build_policy(ROOT, closure)
    certificate = build_synthetic_certificate(ROOT, policy)
    plan = build_diagnostic_plan(ROOT, closure, policy, certificate)
    assert (ROOT / HISTORY_ROOT / str(closure["closure_id"]) / "closure.json").is_file()
    assert (ROOT / POLICY_PATH).is_file()
    assert (ROOT / SYNTHETIC_CERTIFICATE_PATH).is_file()
    assert (ROOT / DIAGNOSTIC_PLAN_PATH).is_file()
    assert plan["authority"]["market_row_read"] is False
    assert plan["authority"]["macro_selection"] is False
    assert plan["outputs"]["result_metrics_forbidden"] is True
    assert plan["limits"]["by_market"]["SR3"]["files"] == len(plan["bindings"]["files"])


def test_remediation_gateway_requires_exact_sr3_scope() -> None:
    required = {
        "operation_kind": "POST_DIAGNOSTIC_SR3_REMEDIATION_AND_SEAL",
        "market": "SR3", "years": "2018,2019,2020,2021,2022",
        "consumed_closure_id": "a" * 64, "policy_id": "a" * 64,
        "diagnostic_evidence_id": "a" * 64, "carried_evidence_id": "a" * 64,
        "catalog_sha256": "a" * 64, "limits_sha256": "a" * 64,
        "implementation_sha256": "a" * 64, "plan_id": "a" * 64,
        "plan_sha256": "a" * 64,
    }
    require_current_real_history_operation(ALPHA_LADDER_SR3_REMEDIATION_OPERATION, required)
    required["market"] = "ZB"
    with pytest.raises(UnauthorizedOperation, match="SR3 remediation gateway scope is invalid"):
        require_current_real_history_operation(ALPHA_LADDER_SR3_REMEDIATION_OPERATION, required)
