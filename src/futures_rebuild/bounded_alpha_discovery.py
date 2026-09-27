"""Pre-result freeze for one bounded, automated Alpha discovery mechanism."""

from __future__ import annotations

import json
import math
import os
from itertools import product
from pathlib import Path
from statistics import median
from typing import Mapping, Sequence

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from .research_gateway_policy import scientific_governance_binding


CONTRACT_PATH = Path("configs/bounded_alpha_discovery_contract_v1.json")
CERTIFICATE_ROOT = Path("state/preexecution_certificates/bounded_alpha_discovery")
SCIENTIFIC_POLICY_PATH = Path("configs/research_readiness_contract.json")
CATALOG_PATH = Path("data/active/catalog.json")
HISTORICAL_DESIGN_PATH = Path("configs/active_alpha_historical_research_design.json")
PREDECESSOR_LADDER_PATH = Path("configs/active_alpha_research_ladder_macro_successor.json")
MACRO_CLOSURE_PATH = Path(
    "state/alpha_ladder_macro_successor_terminal_closure/"
    "7ff6ded9e3cbb45b17cb127783f3065f4e9f6f80ed38c3c623b1c15319c35cab/closure.json"
)
ACTIVE_MACRO_PATH = Path("data/active/alpha_ladder_macro_selection.json")
MARKETS = ("ES", "NQ", "CL", "GC")
FEATURE_PACKS = ("TREND", "TREND_VOLATILITY", "TREND_VOLATILITY_VOLUME")
HORIZONS = (30, 60, 120)
RIDGE_PENALTIES = ("0.1", "1.0", "10.0")


def _read(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"bounded discovery input is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"bounded discovery input must be an object: {path}")
    return value


def enumerate_candidates() -> list[dict[str, object]]:
    """Return the complete deterministic 27-candidate search space."""

    candidates = []
    for ordinal, (feature_pack, horizon, penalty) in enumerate(
        product(FEATURE_PACKS, HORIZONS, RIDGE_PENALTIES)
    ):
        core = {
            "ordinal": ordinal,
            "feature_pack": feature_pack,
            "holding_horizon_minutes": horizon,
            "model_family": "MARKET_SPECIFIC_TWO_TARGET_RIDGE",
            "ridge_penalty": penalty,
        }
        candidates.append({**core, "candidate_id": sha256_json(core)})
    return candidates


def build_contract(root: Path) -> dict[str, object]:
    """Build the exact preparation-only contract without opening market rows."""

    root = root.resolve(strict=False)
    catalog = _read(root / CATALOG_PATH)
    design = _read(root / HISTORICAL_DESIGN_PATH)
    ladder = _read(root / PREDECESSOR_LADDER_PATH)
    closure = _read(root / MACRO_CLOSURE_PATH)
    policy = _read(root / SCIENTIFIC_POLICY_PATH)
    nested_wfa = policy.get("nested_wfa")
    if not isinstance(nested_wfa, dict):
        raise IntegrityError("active scientific policy lacks nested WFA")
    outer_wfa = nested_wfa.get("outer")
    inner_wfa = nested_wfa.get("inner")
    if not isinstance(outer_wfa, dict) or not isinstance(inner_wfa, dict):
        raise IntegrityError("active scientific policy WFA schedule is invalid")
    schedule_values = (
        outer_wfa.get("minimum_folds"), outer_wfa.get("initial_train_sessions"),
        outer_wfa.get("test_sessions"), inner_wfa.get("folds"),
        inner_wfa.get("validation_sessions"),
    )
    if any(type(value) is not int or value <= 0 for value in schedule_values):
        raise IntegrityError("active scientific policy WFA schedule is invalid")
    entries = catalog.get("entries")
    if not isinstance(entries, list) or len(entries) != 144:
        raise IntegrityError("bounded discovery requires the certified 144-entry catalog")
    eligible = {
        (entry.get("market"), entry.get("year"))
        for entry in entries
        if isinstance(entry, dict) and entry.get("selection_eligible") is True
    }
    required = {(market, year) for market in MARKETS for year in range(2018, 2023)}
    if not required.issubset(eligible):
        raise IntegrityError("bounded discovery selection rows are not all catalog-eligible")
    if (
        closure.get("outcome") != "NO_ELIGIBLE_MACRO_DIVERSIFIER"
        or closure.get("retry_allowed") is not False
        or closure.get("selected_market") is not None
        or (root / ACTIVE_MACRO_PATH).exists()
    ):
        raise IntegrityError("bounded discovery requires the terminal no-macro closure")
    candidates = enumerate_candidates()
    core: dict[str, object] = {
        "schema_version": "bounded_alpha_discovery_contract/1.4.0",
        "state": "PRE_RESULT_PREPARATION_ONLY_NOT_REGISTERED",
        "classification": "NEW_COUNTED_MECHANISM_RESTARTING_AT_SYNTHETIC_ENGINEERING",
        "hypothesis": (
            "A bounded search over point-in-time-legal intraday trend, volatility, and volume state "
            "can select on ES one stable after-cost predictor that corroborates unchanged "
            "to NQ, CL, and GC."
        ),
        "claim_policy": {
            "default_claim_ceiling": "ROBUST_PREDICTIVE_ASSOCIATION",
            "point_in_time_causal_ordering_scope": (
                "INFORMATION_AVAILABILITY_ONLY_NOT_ECONOMIC_CAUSAL_IDENTIFICATION"
            ),
            "requested_claim_level": "ROBUST_PREDICTIVE_ASSOCIATION",
            "stronger_causal_status": {
                "requested": False,
                "promotion_label": "CAUSALLY_SUPPORTED_MECHANISM",
                "must_be_frozen_before_outcome_access": True,
                "required_if_requested": [
                    "MECHANISM_SPECIFIC_COMPETING_EXPLANATIONS",
                    "MECHANISM_SPECIFIC_NEGATIVE_CONTROLS",
                    "BOUNDARY_PREDICTIONS",
                    "IDENTIFICATION_ARGUMENT",
                ],
                "missing_requirement_action": (
                    "RETAIN_ROBUST_PREDICTIVE_ASSOCIATION_CEILING"
                ),
            },
        },
        "mechanism_card": {
            "frozen_before": "HISTORICAL_OUTCOME_OR_OUTER_EVALUATION_ACCESS",
            "proposed_incremental_claim": (
                "POINT_IN_TIME_INTRADAY_STATE_ADDS_PREDICTIVE_INFORMATION_BEYOND_"
                "FROZEN_COMMON_EXPOSURES"
            ),
            "competing_explanations": [
                {
                    "name": "UNCONDITIONAL_TIME_OF_DAY_DIRECTION",
                    "expected_boundary": "CANDIDATE_MUST_BEAT_FOLD_LOCAL_UNCONDITIONAL_DIRECTION",
                    "discriminating_test": "FROZEN_BASELINE_FOLD_LOCAL_UNCONDITIONAL_DIRECTION",
                },
                {
                    "name": "SIMPLE_SHORT_HORIZON_MOMENTUM_OR_REVERSAL",
                    "expected_boundary": "CANDIDATE_MUST_BEAT_BOTH_PREVIOUS_BAR_SIGN_RULES",
                    "discriminating_test": (
                        "FROZEN_BASELINES_PREVIOUS_BAR_SIGN_MOMENTUM_AND_REVERSAL"
                    ),
                },
                {
                    "name": "SHARED_TREND_VOLATILITY_OR_RISK_ON_OFF_REGIME",
                    "expected_boundary": (
                        "EVERY_PREREGISTERED_MARKET_FAMILY_LEAVE_ONE_REGIME_"
                        "CLUSTER_OUT_INCREMENTAL_EFFECT_MUST_RETAIN_AT_LEAST_"
                        "50_PERCENT_AND_REMAIN_STRICTLY_POSITIVE"
                    ),
                    "discriminating_test": (
                        "PREREGISTERED_FAMILY_LEAVE_ONE_REGIME_CLUSTER_OUT_RETENTION"
                    ),
                    "retention_rule": {
                        "effect_metric": (
                            "CANDIDATE_MINUS_STRONGEST_FROZEN_COMPETING_BASELINE_"
                            "STRESS_NET_PNL_USD_PER_SESSION"
                        ),
                        "comparison_unit": "PREREGISTERED_MARKET_FAMILY",
                        "exclusion_unit": (
                            "TRAINING_ONLY_TREND_VOLATILITY_REGIME_CLUSTER"
                        ),
                        "retention_fraction_formula": (
                            "LEAVE_ONE_CLUSTER_OUT_INCREMENTAL_EFFECT_DIVIDED_BY_"
                            "FULL_SAMPLE_INCREMENTAL_EFFECT"
                        ),
                        "minimum_retention_fraction": "0.50",
                        "every_leave_one_cluster_out_effect_strictly_positive": True,
                    },
                },
            ],
            "missing_or_changed_after_freeze_action": "FAIL_PROMOTION_CLOSE_MECHANISM",
        },
        "bindings": {
            "scientific_governance": scientific_governance_binding(root),
            "catalog": {
                "path": CATALOG_PATH.as_posix(),
                "sha256": sha256_file(root / CATALOG_PATH),
                "plan_id": catalog.get("plan_id"),
                "entry_count": len(entries),
            },
            "historical_design": {
                "path": HISTORICAL_DESIGN_PATH.as_posix(),
                "sha256": sha256_file(root / HISTORICAL_DESIGN_PATH),
                "pointer_id": design.get("pointer_id"),
            },
            "terminal_macro_closure": {
                "path": MACRO_CLOSURE_PATH.as_posix(),
                "sha256": sha256_file(root / MACRO_CLOSURE_PATH),
                "closure_id": closure.get("closure_id"),
                "outcome": closure.get("outcome"),
            },
            "predecessor_ladder": {
                "path": PREDECESSOR_LADDER_PATH.as_posix(),
                "sha256": sha256_file(root / PREDECESSOR_LADDER_PATH),
                "pointer_id": ladder.get("pointer_id"),
                "role": "IMMUTABLE_PREDECESSOR_NOT_EXECUTION_AUTHORITY",
            },
        },
        "scientific_policy_conformance": {
            "decision": "ACTIVE_POLICY_IS_SOLE_PROMOTION_FRAMEWORK",
            "unresolved_conflict_action": "BLOCK_REGISTRATION",
            "policy_sections": {
                name: "INHERITED_WITHOUT_REPLACEMENT"
                for name in (
                    "independence", "authorization", "readiness", "source_roles",
                    "trial_ledger", "sample_contract", "purge_embargo", "nested_wfa",
                    "fold_local_fit_audit", "uncertainty", "multiple_testing", "costs",
                    "baselines", "negative_controls", "sleeves", "power", "robustness",
                    "prospective_monitoring", "binding_gate", "role_isolation",
                    "research_basis",
                )
            },
        },
        "market_progression": {
            "scope_declaration": {
                "frozen_before_outcome_access": True,
                "claim": "CROSS_FAMILY_ES_NQ_CL_GC_ONLY",
                "predicted_effect_markets": list(MARKETS),
                "broad_41_market_generality_claim": False,
                "corroboration_rule": "ONLY_PREREGISTERED_PREDICTED_SCOPE",
                "evidence_label": "CROSS_DOMAIN_CORROBORATION_NOT_INDEPENDENT_REPLICATION",
            },
            "tier_0_discovery_market": "ES",
            "es_pilot_role": (
                "ENGINEERING_AND_SCIENTIFIC_GATE_BECAUSE_ES_EFFECT_IS_PREDICTED"
            ),
            "es_alpha_failure_rejects_hypothesis": True,
            "tier_1_corroboration_markets": list(MARKETS),
            "tier_1_incremental_markets": ["NQ", "CL", "GC"],
            "required_family_anchors": ["ES", "CL", "GC"],
            "nq_is_correlated_extension": True,
            "nq_cannot_rescue_anchor_failure": True,
            "macro_diversification_claim": False,
            "macro_candidates_excluded_because": "TERMINAL_EXECUTION_INELIGIBILITY_NOT_ALPHA_RESULTS",
        },
        "evidence_accounting": {
            "market_families": {
                "EQUITY_INDEX": ["ES", "NQ"],
                "ENERGY": ["CL"],
                "METALS": ["GC"],
            },
            "regime_clusters": {
                "method": "OUTER_TRAINING_TERCILES_CARTESIAN_PRODUCT",
                "cutpoints_fit_on": "EACH_MARKET_OUTER_TRAINING_ONLY",
                "volatility_input": "realized_volatility_10",
                "volatility_labels": ["LOW", "MIDDLE", "HIGH"],
                "trend_input": "log_return_10",
                "trend_labels": ["NEGATIVE", "MIDDLE", "POSITIVE"],
            },
            "required_reports": [
                "PER_MARKET", "PER_MARKET_FAMILY", "PER_REGIME_CLUSTER",
                "MARKET_FAMILY_BY_REGIME_CLUSTER",
            ],
            "market_count_is_independence_count": False,
            "fold_count_is_independence_count": False,
            "cross_market_inference": {
                "input": "SYNCHRONIZED_COMPLETE_EXCHANGE_SESSION_INCREMENTAL_RETURN_VECTORS",
                "family_aggregation": "EQUAL_WEIGHT_WITHIN_PREREGISTERED_MARKET_FAMILY",
                "resampling_unit": "COMMON_COMPLETE_EXCHANGE_SESSION",
                "same_date_dependence_preserved": True,
                "evidence_label": "CROSS_DOMAIN_CORROBORATION_NOT_INDEPENDENT_REPLICATION",
            },
            "missing_report_action": "FAIL_PROMOTION",
        },
        "construction_sensitivity": {
            "frozen_before": "HISTORICAL_OUTCOME_OR_OUTER_EVALUATION_ACCESS",
            "variants": [
                {"variant_id": "CANONICAL_DECLARED_CONSTRUCTION", "interpretation": "PRIMARY_PROMOTION_CONSTRUCTION"},
                {"variant_id": "EXCLUDE_ACTUAL_CONTRACT_ROLL_TRANSITION_SESSIONS", "interpretation": "ROLL_CONVENTION_DIAGNOSTIC"},
                {"variant_id": "AVAILABLE_TIME_STRICTLY_BEFORE_DECISION_CHECKPOINT", "interpretation": "BAR_BOUNDARY_DIAGNOSTIC"},
                {"variant_id": "EXCLUDE_SESSIONS_WITH_ELIGIBILITY_WINDOW_MISSINGNESS", "interpretation": "MISSINGNESS_DIAGNOSTIC"},
            ],
            "unsupported_variant_action": "REPORT_UNSUPPORTED_WITH_REASON_NEVER_IMPUTE",
            "missing_report_action": "FAIL_PROMOTION",
            "instability_action": "NARROW_CLAIM_OR_INCONCLUSIVE_ROBUSTNESS",
            "post_result_variant_selection_allowed": False,
        },
        "event_concentration": {
            "effect_metric": "CANDIDATE_MINUS_STRONGEST_FROZEN_COMPETING_BASELINE_STRESS_NET_PNL_USD",
            "reports": {
                "largest_session_share": "MAX_SESSION_EFFECT_DIVIDED_BY_TOTAL_INCREMENTAL_EFFECT",
                "largest_contiguous_event_block_share": (
                    "MAX_CONTIGUOUS_5_COMPLETE_EXCHANGE_SESSION_EFFECT_DIVIDED_BY_"
                    "TOTAL_INCREMENTAL_EFFECT"
                ),
            },
            "maximum_share_before_instability": "0.50",
            "applies_to": "EACH_REQUIRED_MARKET_AND_PREREGISTERED_MARKET_FAMILY",
            "instability_action": "NARROW_TO_EVENT_DEPENDENT_CLAIM_OR_INCONCLUSIVE_ROBUSTNESS",
            "missing_report_action": "FAIL_PROMOTION",
        },
        "decision_and_execution": {
            "checkpoint": "10:00:05_AMERICA_CHICAGO",
            "one_ranked_intent_per_session": True,
            "entry": "FIRST_LATER_CAUSAL_REPORTED_BAR_OPEN_WITHIN_120_SECONDS",
            "exit": "FIRST_CAUSAL_REPORTED_BAR_AT_OR_AFTER_ENTRY_PLUS_FROZEN_HORIZON_WITHIN_120_SECONDS",
            "same_actual_contract_identity_required": True,
            "missing_stale_ambiguous_or_nonfinite": "EXPLICIT_ABSTENTION",
        },
        "features": {
            "availability": "EVENT_AND_AVAILABLE_TIMES_NOT_LATER_THAN_DECISION_TIME",
            "training_only_standardization": "PER_MARKET_PER_OUTER_FOLD_MEAN_AND_POPULATION_STD",
            "definitions": {
                "log_return_1": "LN(CLOSE_T/CLOSE_T_MINUS_1)",
                "log_return_5": "LN(CLOSE_T/CLOSE_T_MINUS_5)",
                "log_return_10": "LN(CLOSE_T/CLOSE_T_MINUS_10)",
                "intrabar_range_fraction": "(HIGH_T-LOW_T)/CLOSE_T_MINUS_1",
                "atr_10_fraction": "MEAN_LAST_10_TRUE_RANGES/CLOSE_T_MINUS_1",
                "realized_volatility_10": "POPULATION_STD_LAST_10_LOG_RETURN_1",
                "log1p_volume": "LN(1+NONNEGATIVE_VOLUME_T)",
                "volume_zscore_20": "(VOLUME_T-MEAN_LAST_20)/POPULATION_STD_LAST_20",
            },
            "packs": {
                "TREND": ["log_return_1", "log_return_5", "log_return_10"],
                "TREND_VOLATILITY": [
                    "log_return_1", "log_return_5", "log_return_10",
                    "intrabar_range_fraction", "atr_10_fraction", "realized_volatility_10",
                ],
                "TREND_VOLATILITY_VOLUME": [
                    "log_return_1", "log_return_5", "log_return_10",
                    "intrabar_range_fraction", "atr_10_fraction", "realized_volatility_10",
                    "log1p_volume", "volume_zscore_20",
                ],
            },
        },
        "search": {
            "candidate_budget": len(candidates),
            "candidates": candidates,
            "adaptive_candidate_creation": False,
            "candidate_retry": False,
            "selection_market": "ES_ONLY",
            "selection_rule": "HIGHEST_MEDIAN_INNER_FOLD_STRESS_NET_DAILY_SHARPE_THEN_CANDIDATE_ORDER",
            "winner_count_maximum": 1,
        },
        "folds": {
            "selection_start_inclusive": "2018-01-01T00:00:00Z",
            "selection_end_exclusive": "2023-01-01T00:00:00Z",
            "outer": {
                "count": outer_wfa["minimum_folds"],
                "initial_training_sessions": outer_wfa["initial_train_sessions"],
                "evaluation_sessions": outer_wfa["test_sessions"],
            },
            "inner_per_outer_training": {
                "count": inner_wfa["folds"],
                "minimum_training_sessions": 252,
                "evaluation_sessions": inner_wfa["validation_sessions"],
            },
            "purge_minutes": 120,
            "embargo_sessions": 1,
            "chronological_expanding_only": True,
            "outer_results_may_select_candidate": False,
        },
        "costs": {
            "round_trip_fee_usd": "10.00",
            "promotion_scenario": "stress",
            "round_trip_adverse_ticks": {
                "base": {"ES": 2, "NQ": 2, "CL": 4, "GC": 4},
                "stress": {"ES": 4, "NQ": 4, "CL": 8, "GC": 8},
                "extreme": {"ES": 8, "NQ": 8, "CL": 16, "GC": 16},
            },
            "may_be_reduced_after_results": False,
        },
        "economic_significance": {
            "research_screen": {
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
            },
            "deployment": {
                "claim": "DEPLOYMENT_ELIGIBILITY_ONLY",
                "primary_metric": (
                    "MEAN_EXACT_EXECUTION_COST_NET_PNL_USD_PER_COMPLETE_ELIGIBLE_"
                    "SESSION_INCLUDING_ZERO_NO_TRADE"
                ),
                "mees_usd_per_session": "20.00",
                "confidence_requirement": (
                    "ONE_SIDED_95PCT_LOWER_BOUND_STRICTLY_GREATER_THAN_MEES"
                ),
                "cost_basis": "EXACT_CURRENT_PROVIDER_ACCOUNT_AND_EXECUTION_EVIDENCE",
                "frozen_before": "EXECUTION_OR_FORWARD_EVIDENCE_ACCESS",
                "exact_costs_required_at_gate": True,
            },
            "research_pass_satisfies_deployment_gate": False,
            "missing_or_unfrozen_value_action": "FAIL_CLOSED",
        },
        "baselines": [
            "flat_no_trade", "fold_local_unconditional_direction",
            "previous_reported_bar_sign_momentum", "previous_reported_bar_sign_reversal",
            "risk_matched_always_long", "risk_matched_always_short",
        ],
        "multiplicity": {
            "framework": "ACTIVE_SCIENTIFIC_POLICY_ONLY",
            "information_family": {
                "legacy_conservative_attempts": 105,
                "current_candidate_alternatives": len(candidates),
                "dsr_trial_count": 105 + len(candidates),
                "unledgered_outcome_informed_choice_action": "INVALID_CONFIRMATORY_INFERENCE",
            },
            "pbo": {
                "current_candidate_count": len(candidates),
                "required_matrix": "COMPLETE_COMMON_SESSION_STRESS_COST_RETURNS",
                "legacy_matrix_status": "UNAVAILABLE",
                "missing_or_incomparable_matrix_status": "UNAVAILABLE_NO_POSITIVE_CREDIT",
            },
        },
        "promotion": {
            "common": [
                "STRESS_NET_PNL_POSITIVE", "BEAT_ZERO_AND_EVERY_MANDATORY_BASELINE",
                "COMPLETE_COVERAGE_LINEAGE_AND_AUTHORITY", "MAXIMUM_DRAWDOWN_USD_NOT_OVER_1500",
                "RESEARCH_SCREEN_MEES_CONFIDENCE_BOUND_PASS",
                "FROZEN_COMPETING_EXPLANATION_TESTS_PASS",
                "FAMILY_AND_REGIME_EVIDENCE_REPORT_COMPLETE",
                "SCIENTIFIC_POLICY_CONFORMANCE_PASS",
                "COMPLETE_INFORMATION_FAMILY_DSR_PASS",
                "CONSTRUCTION_SENSITIVITY_COMPLETE",
                "COMMON_SESSION_CROSS_MARKET_INFERENCE_COMPLETE",
                "EVENT_CONCENTRATION_INTERPRETATION_COMPLETE",
            ],
            "tier_0_es": {
                "minimum_trades": 40, "positive_outer_folds_required": 5,
                "outer_fold_count": 8, "multiplicity_adjusted": True,
                "failure_action": "CLOSE_MECHANISM_NO_RETRY",
            },
            "tier_1": {
                "minimum_trades": 120, "positive_markets_required": 3,
                "market_count": 4, "required_positive_markets": ["ES", "CL", "GC"],
                "positive_years_required": 3, "year_count": 5,
                "positive_outer_folds_required": 5, "outer_fold_count": 8,
                "same_candidate_and_parameters_all_markets": True,
                "failure_action": "CLOSE_MECHANISM_NO_RETRY",
            },
        },
        "recent_regime_challenge": {
            "start_inclusive": "2023-01-01T00:00:00Z",
            "end_exclusive": "2025-07-13T22:00:00Z",
            "maximum_attempts": 1,
            "tuning_design_change_or_retry_after_result": False,
            "same_frozen_candidate_required": True,
            "required_positive_markets": ["ES", "CL", "GC"],
        },
        "final_252": {
            "status_source": {
                "path": "configs/active_alpha_research_ladder.json",
                "stage": "final_historical_evaluation",
            },
        },
        "gateway": {
            "registration_and_claim": "CertifiedResearchGateway",
            "historical_operation": "EXECUTE_CERTIFIED_TRIAL_HISTORICAL_SCREEN",
            "readiness_certificate_required": True,
            "single_use_receipt_required": True,
        },
        "terminal_outcomes": ["ONE_WINNER_FROZEN", "NO_ELIGIBLE_ALPHA_CANDIDATE"],
        "authority": {
            "market_row_read": False, "feature_or_outcome_construction": False,
            "model_fit_or_prediction": False, "wfa_or_economics": False,
            "trial_registration": False, "recent_challenge": False,
            "final_252_or_forward": False, "provider_or_network": False,
            "trading": False, "push": False,
        },
    }
    return {**core, "contract_id": sha256_json(core)}


def validate_contract(root: Path, payload: Mapping[str, object]) -> dict[str, object]:
    expected = build_contract(root)
    if dict(payload) != expected:
        raise IntegrityError("bounded discovery contract drifted from the frozen design")
    return expected


def select_candidate(
    contract: Mapping[str, object], scores: Mapping[str, Sequence[float]],
) -> str:
    """Apply the frozen complete-family selection rule to synthetic or future scores."""

    candidates = contract.get("search", {}).get("candidates")  # type: ignore[union-attr]
    if not isinstance(candidates, list):
        raise IntegrityError("bounded discovery candidates are missing")
    folds = contract.get("folds")
    inner_folds = folds.get("inner_per_outer_training") if isinstance(folds, Mapping) else None
    fold_count = inner_folds.get("count") if isinstance(inner_folds, Mapping) else None
    if type(fold_count) is not int or fold_count <= 0:
        raise IntegrityError("bounded discovery inner-fold schedule is missing")
    ordered = [str(candidate["candidate_id"]) for candidate in candidates]
    if set(scores) != set(ordered) or any(len(values) != fold_count for values in scores.values()):
        raise IntegrityError("candidate scores must cover all 27 candidates and configured inner folds")
    if any(
        type(value) not in {int, float} or not math.isfinite(value)
        for values in scores.values() for value in values
    ):
        raise IntegrityError("candidate scores must be finite numbers")
    ranked = sorted(ordered, key=lambda item: (-median(scores[item]), ordered.index(item)))
    return ranked[0]


def build_synthetic_certificate(root: Path, contract: Mapping[str, object]) -> dict[str, object]:
    frozen = validate_contract(root, contract)
    ordered = [item["candidate_id"] for item in frozen["search"]["candidates"]]  # type: ignore[index]
    fold_count = int(frozen["folds"]["inner_per_outer_training"]["count"])  # type: ignore[index]
    zeros = {str(candidate_id): (0.0,) * fold_count for candidate_id in ordered}
    winner = dict(zeros)
    winner[str(ordered[-1])] = (1.0,) * fold_count
    checks = {
        "candidate_count_exact": len(ordered) == 27 and len(set(ordered)) == 27,
        "deterministic_tie_break": select_candidate(frozen, zeros) == ordered[0],
        "best_median_selected": select_candidate(frozen, winner) == ordered[-1],
        "no_real_rows_opened": True,
        "historical_execution_blocked": True,
    }
    if not all(checks.values()):
        raise IntegrityError("bounded discovery synthetic mechanics failed")
    core = {
        "schema_version": "bounded_alpha_discovery_synthetic_certificate/1.0.0",
        "state": "PASS_SYNTHETIC_MECHANICS_ONLY",
        "contract_id": frozen["contract_id"],
        "contract_path": CONTRACT_PATH.as_posix(),
        "contract_sha256": sha256_file(root / CONTRACT_PATH),
        "implementation": {
            "path": "src/futures_rebuild/bounded_alpha_discovery.py",
            "sha256": sha256_file(root / "src/futures_rebuild/bounded_alpha_discovery.py"),
        },
        "checks": checks,
        "does_not_certify": [
            "SOURCE_OR_ROW_READINESS", "ALPHA", "WFA", "ECONOMICS",
            "RECENT_REGIME_CHALLENGE", "FINAL_252", "LIVE_EXECUTION",
        ],
        "authority": {"historical_row_read": False, "trial_execution": False},
    }
    return {**core, "certificate_id": sha256_json(core)}


def _write_new_or_exact(path: Path, payload: Mapping[str, object]) -> None:
    rendered = canonical_bytes(dict(payload)) + b"\n"
    if path.exists():
        if path.read_bytes() != rendered:
            raise IntegrityError(f"bounded discovery artifact drifted: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
    try:
        os.write(descriptor, rendered)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def prepare(root: Path) -> dict[str, str]:
    """Freeze the contract and synthetic certificate; grant no row authority."""

    contract = build_contract(root)
    _write_new_or_exact(root / CONTRACT_PATH, contract)
    certificate = build_synthetic_certificate(root, contract)
    certificate_path = CERTIFICATE_ROOT / str(certificate["certificate_id"]) / "certificate.json"
    _write_new_or_exact(root / certificate_path, certificate)
    return {
        "contract_id": str(contract["contract_id"]),
        "contract_path": CONTRACT_PATH.as_posix(),
        "certificate_id": str(certificate["certificate_id"]),
        "certificate_path": certificate_path.as_posix(),
        "state": "PREPARED_SYNTHETIC_ONLY_NO_ROW_AUTHORITY",
    }
