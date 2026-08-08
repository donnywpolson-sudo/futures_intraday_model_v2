from __future__ import annotations

import copy

import pytest

from futures_rebuild.alpha_ladder_frozen_mechanism import (
    MANDATORY_BASELINES,
    build_frozen_mechanism,
    build_tier0_certificate,
    build_tier0_decision,
    validate_frozen_mechanism,
    validate_promotion_evidence,
    validate_tier0_certificate,
)
from futures_rebuild.alpha_research_ladder import ALL_APPROVED, BALANCED, CORE, TRADITIONAL
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _mechanism() -> dict[str, object]:
    return build_frozen_mechanism(
        contract_id="1" * 64, profile_id="2" * 64,
        source_protocol_id="3" * 64, source_protocol_sha256="4" * 64,
        all_markets=ALL_APPROVED,
    )


def _evidence(stage: str, markets: tuple[str, ...]) -> dict[str, object]:
    result: dict[str, object] = {
        "stress_net_pnl_usd": "100",
        "baseline_stress_net_pnl_usd": {
            name: "0" if name == "flat_no_trade" else "50"
            for name in MANDATORY_BASELINES
        },
        "trade_count": 8 if stage == "pilot" else 40,
        "maximum_continuous_drawdown_usd": "1500",
        "complete_coverage": True,
        "complete_metrics": True,
        "risk_rules_compliant": True,
        "live_readiness_claim": False,
    }
    if stage == "pilot":
        result["formal_significance_claim"] = False
        return result
    result.update({
        "primary_bootstrap_lower_bound_above_zero": True,
        "all_paired_baseline_lower_bounds_above_zero": True,
        "positive_portfolio_years": [True, True, True, False, False],
        "positive_folds": [True, True, True, True, True, False, False, False],
    })
    if stage == "tier_1":
        result["positive_markets"] = list(markets[:3])
        result["positive_market_year_cells"] = [True] * 8 + [False] * 12
    elif stage == "tier_2":
        additions = [market for market in markets if market not in CORE]
        result["positive_markets"] = [*CORE[:3], *additions[:8]]
        result["positive_market_year_cells"] = [True] * 32 + [False] * 48
        result["subgroup_decisions"] = {
            "core": "PASS", "additions": "PASS", "combined": "PASS",
        }
        result["subgroup_stress_net_pnl_usd"] = {
            "core": "25", "additions": "75", "combined": "100",
        }
    else:
        result["positive_markets"] = list(markets[:26])
        result["positive_market_year_cells"] = [True] * 76 + [False] * 114
        result["subgroup_decisions"] = {
            "traditional": "PASS", "satellite": "REPORTED", "combined": "PASS",
            "satellite_can_rescue_traditional_failure": False,
        }
        result["traditional_gate_results"] = {
            "stress_net_pnl_positive": True,
            "beat_zero_and_all_baselines": True,
            "formal_tests_passed": True,
            "complete_coverage_and_metrics": True,
            "drawdown_within_1500": True,
        }
    return result


def test_frozen_mechanism_is_ladder_wide_and_non_authorizing() -> None:
    mechanism = validate_frozen_mechanism(_mechanism())
    assert mechanism["ranking"]["canonical_market_order"] == list(ALL_APPROVED)
    assert mechanism["source_design_binding"]["six_market_scope_reused"] is False
    assert mechanism["model_parameters"]["ridge_penalty"] == "1.0"
    assert mechanism["model_parameters"]["entry_hurdle_predicted_stress_net_r"] == "0.25"
    assert mechanism["features"]["definitions"]["log_return_10"] == (
        "LN(CLOSE_T_DIV_CLOSE_T_MINUS_10)"
    )
    assert mechanism["baselines"]["candidate_schedule_reuse"] is False
    assert mechanism["promotion_gates"]["pilot"]["minimum_trades"] == 8
    assert mechanism["promotion_gates"]["tier_1"]["positive_markets_required"] == 3
    assert mechanism["promotion_gates"]["tier_2"]["positive_markets_required"] == 11
    assert mechanism["promotion_gates"]["tier_3"]["positive_traditional_markets_required"] == 26
    assert set(mechanism["costs"]["round_trip_adverse_ticks"]["stress"]) == set(ALL_APPROVED)
    assert all(value is False for value in mechanism["authority"].values())


def test_mechanism_identity_and_market_order_fail_closed() -> None:
    mechanism = _mechanism()
    mechanism["ranking"]["canonical_market_order"] = list(reversed(ALL_APPROVED))
    with pytest.raises(IntegrityError, match="mechanism_id"):
        validate_frozen_mechanism(mechanism)
    with pytest.raises(IntegrityError, match="41-market"):
        build_frozen_mechanism(
            contract_id="1" * 64, profile_id="2" * 64,
            source_protocol_id="3" * 64, source_protocol_sha256="4" * 64,
            all_markets=CORE,
        )


def test_tier0_certificate_is_synthetic_only() -> None:
    certificate = build_tier0_certificate(
        contract_id="1" * 64, profile_id="2" * 64,
        mechanism_id="3" * 64, mechanism_sha256="4" * 64,
        test_node_ids=("a", "b"), passed_test_count=2,
    )
    validate_tier0_certificate(
        certificate, contract_id="1" * 64, mechanism_sha256="4" * 64,
    )
    changed = copy.deepcopy(certificate)
    changed["alpha_evidence"] = True
    with pytest.raises(IntegrityError, match="certificate_id"):
        validate_tier0_certificate(
            changed, contract_id="1" * 64, mechanism_sha256="4" * 64,
        )
    decision = build_tier0_decision(
        contract_id="1" * 64, mechanism_sha256="4" * 64,
        synthetic_certificate_path="state/tier0.json",
        synthetic_certificate_sha256="5" * 64,
    )
    assert decision["stage"] == "tier_0"
    assert decision["decision"] == "PASS"


@pytest.mark.parametrize(
    ("stage", "markets"),
    (("pilot", ("ES",)), ("tier_1", CORE), ("tier_2", BALANCED), ("tier_3", TRADITIONAL)),
)
def test_each_locked_promotion_gate_accepts_exact_pass(
    stage: str, markets: tuple[str, ...],
) -> None:
    validate_promotion_evidence(_evidence(stage, markets), stage=stage, markets=markets)


def test_pilot_rejects_sparse_or_formal_claim() -> None:
    evidence = _evidence("pilot", ("ES",))
    evidence["trade_count"] = 7
    with pytest.raises(UnauthorizedOperation, match="trade-count"):
        validate_promotion_evidence(evidence, stage="pilot", markets=("ES",))
    evidence = _evidence("pilot", ("ES",))
    evidence["formal_significance_claim"] = True
    with pytest.raises(UnauthorizedOperation, match="formal"):
        validate_promotion_evidence(evidence, stage="pilot", markets=("ES",))


def test_baseline_drawdown_and_coverage_fail_closed() -> None:
    evidence = _evidence("tier_1", CORE)
    evidence["baseline_stress_net_pnl_usd"]["risk_matched_always_long"] = "100"
    with pytest.raises(UnauthorizedOperation, match="every baseline"):
        validate_promotion_evidence(evidence, stage="tier_1", markets=CORE)
    evidence = _evidence("tier_1", CORE)
    evidence["maximum_continuous_drawdown_usd"] = "1500.01"
    with pytest.raises(UnauthorizedOperation, match="common"):
        validate_promotion_evidence(evidence, stage="tier_1", markets=CORE)
    evidence = _evidence("tier_1", CORE)
    evidence["complete_coverage"] = False
    with pytest.raises(UnauthorizedOperation, match="common"):
        validate_promotion_evidence(evidence, stage="tier_1", markets=CORE)


def test_breadth_and_satellite_rescue_fail_closed() -> None:
    tier2 = _evidence("tier_2", BALANCED)
    tier2["subgroup_decisions"]["additions"] = "FAIL"
    with pytest.raises(UnauthorizedOperation, match="Tier 2"):
        validate_promotion_evidence(tier2, stage="tier_2", markets=BALANCED)
    tier3 = _evidence("tier_3", TRADITIONAL)
    tier3["subgroup_decisions"]["traditional"] = "FAIL"
    tier3["subgroup_decisions"]["satellite"] = "PASS"
    with pytest.raises(UnauthorizedOperation, match="Tier 3"):
        validate_promotion_evidence(tier3, stage="tier_3", markets=TRADITIONAL)
