import json
from pathlib import Path

import pytest

from futures_rebuild.bounded_alpha_discovery import (
    CONTRACT_PATH, build_contract, build_synthetic_certificate,
    enumerate_candidates, select_candidate, validate_contract,
)
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.current


def test_bounded_discovery_freeze_is_exact_and_fail_closed() -> None:
    contract = build_contract(ROOT)
    assert validate_contract(ROOT, contract) == contract
    assert contract["market_progression"]["tier_0_discovery_market"] == "ES"
    scope = contract["market_progression"]["scope_declaration"]
    assert scope == {
        "frozen_before_outcome_access": True,
        "claim": "CROSS_FAMILY_ES_NQ_CL_GC_ONLY",
        "predicted_effect_markets": ["ES", "NQ", "CL", "GC"],
        "broad_41_market_generality_claim": False,
        "corroboration_rule": "ONLY_PREREGISTERED_PREDICTED_SCOPE",
        "evidence_label": "CROSS_DOMAIN_CORROBORATION_NOT_INDEPENDENT_REPLICATION",
    }
    assert contract["market_progression"]["es_pilot_role"].endswith(
        "BECAUSE_ES_EFFECT_IS_PREDICTED"
    )
    assert contract["market_progression"]["es_alpha_failure_rejects_hypothesis"] is True
    assert contract["market_progression"]["required_family_anchors"] == ["ES", "CL", "GC"]
    assert contract["market_progression"]["tier_1_corroboration_markets"] == [
        "ES", "NQ", "CL", "GC",
    ]
    assert "tier_1_replication_markets" not in contract["market_progression"]
    assert "required_independent_anchors" not in contract["market_progression"]
    assert contract["market_progression"]["macro_diversification_claim"] is False
    claims = contract["claim_policy"]
    assert claims["default_claim_ceiling"] == "ROBUST_PREDICTIVE_ASSOCIATION"
    assert claims["requested_claim_level"] == "ROBUST_PREDICTIVE_ASSOCIATION"
    assert claims["point_in_time_causal_ordering_scope"].endswith(
        "NOT_ECONOMIC_CAUSAL_IDENTIFICATION"
    )
    stronger = claims["stronger_causal_status"]
    assert stronger["requested"] is False
    assert stronger["must_be_frozen_before_outcome_access"] is True
    assert set(stronger["required_if_requested"]) == {
        "MECHANISM_SPECIFIC_COMPETING_EXPLANATIONS",
        "MECHANISM_SPECIFIC_NEGATIVE_CONTROLS",
        "BOUNDARY_PREDICTIONS",
        "IDENTIFICATION_ARGUMENT",
    }
    assert stronger["missing_requirement_action"] == (
        "RETAIN_ROBUST_PREDICTIVE_ASSOCIATION_CEILING"
    )
    card = contract["mechanism_card"]
    assert card["frozen_before"] == "HISTORICAL_OUTCOME_OR_OUTER_EVALUATION_ACCESS"
    assert len(card["competing_explanations"]) == 3
    assert all(
        explanation["expected_boundary"] and explanation["discriminating_test"]
        for explanation in card["competing_explanations"]
    )
    regime_rule = card["competing_explanations"][2]["retention_rule"]
    assert regime_rule["minimum_retention_fraction"] == "0.50"
    assert regime_rule["every_leave_one_cluster_out_effect_strictly_positive"] is True
    accounting = contract["evidence_accounting"]
    assert accounting["market_families"] == {
        "EQUITY_INDEX": ["ES", "NQ"], "ENERGY": ["CL"], "METALS": ["GC"],
    }
    assert accounting["regime_clusters"]["cutpoints_fit_on"] == (
        "EACH_MARKET_OUTER_TRAINING_ONLY"
    )
    assert accounting["market_count_is_independence_count"] is False
    assert accounting["fold_count_is_independence_count"] is False
    assert accounting["cross_market_inference"]["resampling_unit"] == (
        "COMMON_COMPLETE_EXCHANGE_SESSION"
    )
    assert accounting["cross_market_inference"]["same_date_dependence_preserved"] is True
    assert set(accounting["required_reports"]) == {
        "PER_MARKET", "PER_MARKET_FAMILY", "PER_REGIME_CLUSTER",
        "MARKET_FAMILY_BY_REGIME_CLUSTER",
    }
    assert contract["search"]["candidate_budget"] == 27
    governance = contract["bindings"]["scientific_governance"]
    assert governance["legacy_attempts"]["conservative_count"] == 105
    assert len(governance["global_trial_ledger"]["head"]) == 64
    assert len(governance["scientific_policy"]["sha256"]) == 64
    assert enumerate_candidates() == contract["search"]["candidates"]
    conformance = contract["scientific_policy_conformance"]
    assert conformance["decision"] == "ACTIVE_POLICY_IS_SOLE_PROMOTION_FRAMEWORK"
    assert conformance["unresolved_conflict_action"] == "BLOCK_REGISTRATION"
    assert set(conformance["policy_sections"].values()) == {"INHERITED_WITHOUT_REPLACEMENT"}
    assert "explicit_rules" not in conformance
    assert contract["multiplicity"]["framework"] == "ACTIVE_SCIENTIFIC_POLICY_ONLY"
    assert "method" not in contract["multiplicity"]
    assert contract["multiplicity"]["information_family"] == {
        "legacy_conservative_attempts": 105,
        "current_candidate_alternatives": 27,
        "dsr_trial_count": 132,
        "unledgered_outcome_informed_choice_action": "INVALID_CONFIRMATORY_INFERENCE",
    }
    assert contract["multiplicity"]["pbo"]["legacy_matrix_status"] == "UNAVAILABLE"
    assert len(contract["construction_sensitivity"]["variants"]) == 4
    assert contract["construction_sensitivity"]["post_result_variant_selection_allowed"] is False
    assert contract["event_concentration"]["maximum_share_before_instability"] == "0.50"
    assert "CONTIGUOUS_5_COMPLETE_EXCHANGE_SESSION" in (
        contract["event_concentration"]["reports"]["largest_contiguous_event_block_share"]
    )
    assert contract["folds"]["outer_results_may_select_candidate"] is False
    economics = contract["economic_significance"]
    assert economics["research_screen"] == {
        "claim": "HISTORICAL_RESEARCH_SCREEN_ONLY",
        "primary_metric": (
            "MEAN_STRESS_COST_NET_PNL_USD_PER_COMPLETE_ELIGIBLE_SESSION_"
            "INCLUDING_ZERO_NO_TRADE"
        ),
        "mees_usd_per_session": "10.00",
        "confidence_requirement": (
            "MULTIPLICITY_ADJUSTED_ONE_SIDED_95PCT_LOWER_BOUND_"
            "STRICTLY_GREATER_THAN_MEES"
        ),
        "cost_basis": "FROZEN_PROVISIONAL_STRESS_RESEARCH_COSTS",
        "frozen_before": "HISTORICAL_OUTCOME_OR_OUTER_EVALUATION_ACCESS",
    }
    deployment = economics["deployment"]
    assert deployment["mees_usd_per_session"] == "20.00"
    assert deployment["cost_basis"] == (
        "EXACT_CURRENT_PROVIDER_ACCOUNT_AND_EXECUTION_EVIDENCE"
    )
    assert deployment["frozen_before"] == "EXECUTION_OR_FORWARD_EVIDENCE_ACCESS"
    assert deployment["exact_costs_required_at_gate"] is True
    assert economics["research_pass_satisfies_deployment_gate"] is False
    assert economics["missing_or_unfrozen_value_action"] == "FAIL_CLOSED"
    assert "RESEARCH_SCREEN_MEES_CONFIDENCE_BOUND_PASS" in contract["promotion"]["common"]
    assert "FROZEN_COMPETING_EXPLANATION_TESTS_PASS" in contract["promotion"]["common"]
    assert "FAMILY_AND_REGIME_EVIDENCE_REPORT_COMPLETE" in contract["promotion"]["common"]
    assert set(contract["promotion"]["common"]) >= {
        "SCIENTIFIC_POLICY_CONFORMANCE_PASS",
        "COMPLETE_INFORMATION_FAMILY_DSR_PASS",
        "CONSTRUCTION_SENSITIVITY_COMPLETE",
        "COMMON_SESSION_CROSS_MARKET_INFERENCE_COMPLETE",
        "EVENT_CONCENTRATION_INTERPRETATION_COMPLETE",
    }
    assert contract["recent_regime_challenge"]["maximum_attempts"] == 1
    assert contract["final_252"] == {
        "status_source": {
            "path": "configs/active_alpha_research_ladder.json",
            "stage": "final_historical_evaluation",
        },
    }
    assert not any(contract["authority"].values())

    ids = [candidate["candidate_id"] for candidate in contract["search"]["candidates"]]
    scores = {candidate_id: (0.0, 0.0, 0.0, 0.0) for candidate_id in ids}
    assert select_candidate(contract, scores) == ids[0]
    with pytest.raises(IntegrityError, match="cover all 27"):
        select_candidate(contract, dict(list(scores.items())[:-1]))
    with pytest.raises(IntegrityError, match="configured inner folds"):
        select_candidate(contract, {candidate_id: values[:3] for candidate_id, values in scores.items()})


def test_bounded_discovery_inherits_active_scientific_schedule() -> None:
    policy = json.loads((ROOT / "configs/research_readiness_contract.json").read_bytes())
    contract = build_contract(ROOT)

    assert contract["folds"]["outer"] == {
        "count": policy["nested_wfa"]["outer"]["minimum_folds"],
        "initial_training_sessions": policy["nested_wfa"]["outer"]["initial_train_sessions"],
        "evaluation_sessions": policy["nested_wfa"]["outer"]["test_sessions"],
    }
    assert contract["folds"]["inner_per_outer_training"] == {
        "count": policy["nested_wfa"]["inner"]["folds"],
        "minimum_training_sessions": 252,
        "evaluation_sessions": policy["nested_wfa"]["inner"]["validation_sessions"],
    }


def test_published_contract_and_synthetic_certificate_are_reproducible() -> None:
    published = __import__("json").loads((ROOT / CONTRACT_PATH).read_bytes())
    assert validate_contract(ROOT, published) == published
    certificate = build_synthetic_certificate(ROOT, published)
    assert certificate["state"] == "PASS_SYNTHETIC_MECHANICS_ONLY"
    assert certificate["authority"] == {
        "historical_row_read": False, "trial_execution": False,
    }
