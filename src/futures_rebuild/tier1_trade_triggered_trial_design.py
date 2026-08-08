"""Prepare the source-selected trade-triggered Tier 1 declaration without registration."""

from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path

from .canonical import sha256_file, sha256_json
from .cash_open_source_compatibility_census import _read_canonical
from .errors import IntegrityError


SOURCE_REPORT_ID = "676807426738f24b000a5ec51c7df7906425b4427406afece1f4ffb6b1eae418"
SOURCE_REPORT_SHA256 = "b9c99d5e52e4edd42a3559f87149c5a3dc6f380d123d5455a8b9b2a53ef096a0"
SOURCE_REPORT_PATH = Path(
    "state/unpublished_evidence/reported_bar_trade_triggered_source_census/"
    f"{SOURCE_REPORT_ID}/source_census.json"
)
SOURCE_PLAN_PATH = Path("configs/reported_bar_trade_triggered_source_census_plan.json")
SOURCE_PLAN_SHA256 = "24646020776db92b2da5f7881e7f73bb210d6fba702d999e5df0f2313bed6c86"
DISCOVERY_PROTOCOL_PATH = Path(
    "configs/reported_bar_trade_triggered_source_discovery_protocol_v2.json"
)
DISCOVERY_PROTOCOL_ID = "5acd510305cbb4c0de6b813ff92e15b77151ae7cc0f9a2a2c53192cb9967d8d3"
DISCOVERY_PROTOCOL_SHA256 = "8eefcb8051de4eb94a975688db33c3022dc654b06ce5be35de7b3275c61ef7f6"
DECLARATION_PATH = Path("configs/tier1_trade_triggered_trial_protocol.json")
SELECTED_CHECKPOINT = "10:00"
SELECTED_MARKETS = ("CL", "ES", "NG", "NQ", "RTY", "YM")
TICK_SPEC = {
    "CL": {"tick_size": "0.01", "tick_value_usd": "10", "base": 4, "stress": 8, "extreme": 16},
    "ES": {"tick_size": "0.25", "tick_value_usd": "12.50", "base": 2, "stress": 4, "extreme": 8},
    "NG": {"tick_size": "0.001", "tick_value_usd": "10", "base": 4, "stress": 8, "extreme": 16},
    "NQ": {"tick_size": "0.25", "tick_value_usd": "5", "base": 2, "stress": 4, "extreme": 8},
    "RTY": {"tick_size": "0.10", "tick_value_usd": "5", "base": 2, "stress": 4, "extreme": 8},
    "YM": {"tick_size": "1", "tick_value_usd": "5", "base": 2, "stress": 4, "extreme": 8},
}


def _verified_source_report(root: Path) -> dict[str, object]:
    path = root / SOURCE_REPORT_PATH
    if sha256_file(path) != SOURCE_REPORT_SHA256:
        raise IntegrityError("trade-triggered source report drifted")
    report = _read_canonical(path, name="trade-triggered source report")
    core = {key: value for key, value in report.items() if key != "report_id"}
    if report.get("report_id") != SOURCE_REPORT_ID or sha256_json(core) != SOURCE_REPORT_ID:
        raise IntegrityError("trade-triggered source report identity is invalid")
    selection = report.get("selection")
    if not isinstance(selection, Mapping) or (
        selection.get("decision") != "PASS_SOURCE_COMPATIBILITY_DISCOVERY"
        or selection.get("selected_checkpoint") != SELECTED_CHECKPOINT
        or tuple(selection.get("selected_markets", ())) != SELECTED_MARKETS
    ):
        raise IntegrityError("trade-triggered source selection differs from the frozen result")
    selected = {
        str(item["market"]): item
        for item in report.get("market_checkpoint_results", [])
        if isinstance(item, dict)
        and item.get("checkpoint") == SELECTED_CHECKPOINT
        and item.get("market") in SELECTED_MARKETS
    }
    if len(selected) != len(SELECTED_MARKETS):
        raise IntegrityError("selected source cells are incomplete")
    for market in SELECTED_MARKETS:
        item = selected[market]
        overall = item.get("overall")
        if (
            item.get("status") != "PASS"
            or item.get("failed_gates") != []
            or not isinstance(overall, Mapping)
            or int(overall.get("candidate_mandatory_failures", -1)) != 0
            or any(
                int(value) != 0
                for value in overall.get("active_baseline_mandatory_failures", {}).values()
            )
        ):
            raise IntegrityError(f"selected source cell does not pass for {market}")
    return report


def planned_initial_loss_usd(
    *, stop_ticks: int, tick_value_usd: Decimal, stress_cost_usd: Decimal
) -> Decimal:
    if stop_ticks <= 0 or tick_value_usd <= 0 or stress_cost_usd < 0:
        raise IntegrityError("planned-loss inputs must be positive")
    return Decimal(stop_ticks) * tick_value_usd + stress_cost_usd


def risk_eligible(
    *, stop_ticks: int, tick_value_usd: Decimal, stress_cost_usd: Decimal
) -> bool:
    return planned_initial_loss_usd(
        stop_ticks=stop_ticks,
        tick_value_usd=tick_value_usd,
        stress_cost_usd=stress_cost_usd,
    ) <= Decimal("250")


def select_ranked_intent(intents: list[Mapping[str, object]]) -> Mapping[str, object] | None:
    eligible = [
        item for item in intents
        if item.get("market") in SELECTED_MARKETS
        and item.get("risk_eligible") is True
        and Decimal(str(item.get("predicted_net_r", "-Infinity"))) >= Decimal("0.25")
    ]
    if not eligible:
        return None
    order = {market: index for index, market in enumerate(SELECTED_MARKETS)}
    return min(
        eligible,
        key=lambda item: (-Decimal(str(item["predicted_net_r"])), order[str(item["market"])]),
    )


def build_declaration(*, root: Path) -> dict[str, object]:
    _verified_source_report(root)
    bindings = {
        SOURCE_REPORT_PATH.as_posix(): SOURCE_REPORT_SHA256,
        SOURCE_PLAN_PATH.as_posix(): SOURCE_PLAN_SHA256,
        DISCOVERY_PROTOCOL_PATH.as_posix(): DISCOVERY_PROTOCOL_SHA256,
        "configs/active_cash_open_impulse_historical_calendar.json": sha256_file(
            root / "configs/active_cash_open_impulse_historical_calendar.json"
        ),
        "data/active/catalog.json": sha256_file(root / "data/active/catalog.json"),
        "configs/contract_economics_rules.json": sha256_file(
            root / "configs/contract_economics_rules.json"
        ),
        "configs/prop_firm_risk_profile.json": sha256_file(
            root / "configs/prop_firm_risk_profile.json"
        ),
        "src/futures_rebuild/tier1_trade_triggered_trial_design.py": sha256_file(Path(__file__)),
    }
    core: dict[str, object] = {
        "schema_version": "tier1_trade_triggered_trial_protocol/1.0.0",
        "state": "PREPARED_NOT_REGISTERABLE_ROW_CERTIFICATE_REQUIRED",
        "classification": "ONE_PREREGISTERED_TIER1_HISTORICAL_SCREEN_PREPARATION",
        "research_only": True,
        "live_readiness": False,
        "lineage": {
            "source_discovery_protocol_id": DISCOVERY_PROTOCOL_ID,
            "source_compatibility_report_id": SOURCE_REPORT_ID,
            "market_and_checkpoint_selection_used_returns": False,
            "selected_checkpoint_chicago": SELECTED_CHECKPOINT,
            "selected_markets": list(SELECTED_MARKETS),
            "prior_failed_trial_outcomes_used_for_parameter_selection": False,
            "prior_failures_used_for_decision_validity_only": True,
        },
        "period": {
            "training_and_evaluation_years": [2018, 2019, 2020, 2021, 2022],
            "locked_untouched_holdout_year": 2025,
            "holdout_access": False,
        },
        "opportunity_universe": {
            "one_checkpoint_per_eligible_session": SELECTED_CHECKPOINT,
            "markets": list(SELECTED_MARKETS),
            "checkpoint_accounting_percent": 100,
            "feature_gap": "EXPLICIT_CAUSAL_ABSTENTION",
            "no_reported_trigger_by_decision_plus_120_seconds": "EXPLICIT_NO_TRADE_TIMEOUT",
            "future_outcome_availability_never_filters_prediction_eligibility": True,
        },
        "decision_and_execution": {
            "decision_time": "10:00:05_AMERICA_CHICAGO",
            "scores_frozen_at_decision": True,
            "cross_market_ranking": "PREDICTED_STRESS_NET_R_DESC_THEN_CL_ES_NG_NQ_RTY_YM",
            "select_exactly_one_intent_before_trigger_monitoring": True,
            "runner_up_substitution_after_no_trigger_or_missing_path": False,
            "trigger": "FIRST_REPORTED_BAR_AVAILABLE_AFTER_DECISION_WITHIN_120_SECONDS",
            "trigger_bar_as_fill": False,
            "order_time": "TRIGGER_AVAILABLE_AT",
            "entry_fill_proxy": "FIRST_LATER_REPORTED_BAR_OPEN_WITH_EVENT_AND_AVAILABLE_AT_WITHIN_120_SECONDS_OF_ORDER",
            "entry_cost_treatment": "LOCKED_SCENARIO_ADVERSE_TICKS_PLUS_FEES",
            "protective_stop": "DECISION_FROZEN_1.5_ATR20_ROUNDED_UP_TO_MARKET_TICK",
            "stop_fill_long": "BAR_OPEN_IF_BELOW_STOP_ELSE_STOP_WHEN_REPORTED_LOW_CROSSES",
            "stop_fill_short": "BAR_OPEN_IF_ABOVE_STOP_ELSE_STOP_WHEN_REPORTED_HIGH_CROSSES",
            "time_exit": "FIRST_CAUSAL_REPORTED_BAR_AT_OR_AFTER_ENTRY_EVENT_PLUS_30_MINUTES_WITHIN_120_SECONDS",
            "profit_target": None,
            "same_actual_contract_identity_required": True,
            "ambiguous_or_missing_triggered_path": "INCONCLUSIVE_DATA_OR_COVERAGE",
        },
        "features": {
            "causal_window": "REPORTED_BARS_EVENT_AT_09_30_THROUGH_BEFORE_10_00_AVAILABLE_BY_10_00_05",
            "minimum_distinct_reported_minutes": 15,
            "names": [
                "log_return_1",
                "log_return_5",
                "log_return_10",
                "intrabar_range_fraction",
                "atr_10_fraction",
                "range_to_atr_10",
                "realized_volatility_10",
                "log1p_volume",
                "volume_zscore_10",
            ],
            "definitions": {
                "log_return_1": "LN(CLOSE_T_DIV_CLOSE_T_MINUS_1)",
                "log_return_5": "LN(CLOSE_T_DIV_CLOSE_T_MINUS_5)",
                "log_return_10": "LN(CLOSE_T_DIV_CLOSE_T_MINUS_10)",
                "true_range": "MAX(HIGH_T_MINUS_LOW_T_ABS_HIGH_T_MINUS_CLOSE_T_MINUS_1_ABS_LOW_T_MINUS_CLOSE_T_MINUS_1)",
                "atr_10": "SIMPLE_MEAN_OF_LAST_10_TRUE_RANGES",
                "intrabar_range_fraction": "HIGH_T_MINUS_LOW_T_DIV_CLOSE_T_MINUS_1",
                "atr_10_fraction": "ATR_10_DIV_CLOSE_T_MINUS_1",
                "range_to_atr_10": "HIGH_T_MINUS_LOW_T_DIV_ATR_10",
                "realized_volatility_10": "POPULATION_STD_OF_LAST_10_ONE_BAR_LOG_RETURNS",
                "log1p_volume": "LN_1_PLUS_NONNEGATIVE_VOLUME_T",
                "volume_zscore_10": "VOLUME_T_MINUS_MEAN_LAST_10_VOLUMES_DIV_POPULATION_STD_LAST_10_VOLUMES",
            },
            "formula_guards": {
                "nonpositive_price_or_negative_volume": "EXPLICIT_CAUSAL_FEATURE_ABSTENTION",
                "zero_atr_denominator": "EXPLICIT_CAUSAL_FEATURE_ABSTENTION",
                "zero_volume_std": "VOLUME_ZSCORE_10_EQUALS_ZERO",
            },
            "trend_context": ["log_return_5", "log_return_10"],
            "volatility_regime": [
                "atr_10_fraction", "range_to_atr_10", "realized_volatility_10"
            ],
            "time_of_session_context": "EXPLICITLY_FIXED_BY_THE_SINGLE_10_00_CHECKPOINT_NOT_DUPLICATED_AS_CONSTANT_MODEL_COLUMNS",
            "numeric_nonfinite_or_undefined": "EXPLICIT_CAUSAL_FEATURE_ABSTENTION",
            "training_only_standardization": "PER_MARKET_PER_OUTER_FOLD_MEAN_AND_POPULATION_STD",
            "zero_training_std": "STANDARDIZED_VALUE_ZERO_NO_DIVISION",
        },
        "model": {
            "family": "MARKET_SPECIFIC_TWO_TARGET_RIDGE",
            "targets": ["LONG_STRESS_NET_R", "SHORT_STRESS_NET_R"],
            "target_definition": "DIRECTIONAL_STRESS_NET_PNL_USD_DIV_PLANNED_INITIAL_LOSS_USD_USING_THE_LOCKED_STOP_AND_TIME_EXIT",
            "ridge_penalty": "1.0",
            "intercept": True,
            "hyperparameter_search": False,
            "market_pooling": False,
            "direction": "ARGMAX_LONG_SHORT_PREDICTED_STRESS_NET_R",
            "entry_hurdle_predicted_stress_net_r": "0.25",
            "hurdle_comparison": "GREATER_THAN_OR_EQUAL",
            "training_row_eligibility": "FEATURE_COMPLETE_STRESS_RISK_ELIGIBLE_TRIGGER_OBSERVED_AND_COMPLETE_SAME_IDENTITY_TARGET_PATH",
            "no_trigger_or_incomplete_training_target": "NO_LABEL_NO_IMPUTATION_NO_ZERO_TARGET",
            "evaluation_checkpoint_retention": "EVERY_CALENDAR_ELIGIBLE_CHECKPOINT_IS_PREDICTION_OR_EXPLICIT_FEATURE_RISK_OR_TRIGGER_ABSTENTION",
        },
        "splits": {
            "construction": "CHECKPOINT_ELIGIBLE_CALENDAR_SESSIONS_BEFORE_SOURCE_COMPLETENESS",
            "outer_folds": 8,
            "initial_training_sessions": 504,
            "evaluation_sessions": 63,
            "embargo_sessions": 1,
            "purge_minutes": 40,
            "minimum_complete_training_sessions_per_market_fold": 252,
            "minimum_complete_evaluation_sessions_per_market_fold": 30,
            "transform_and_model_fit_scope": "TRAINING_ONLY",
            "fold_shortening_or_reassignment": False,
        },
        "position_and_risk": {
            "instrument": "ONE_BOUND_STANDARD_CONTRACT",
            "fractional_or_micro_proxy": False,
            "stop_atr_multiple": "1.5",
            "stop_tick_rounding": "AWAY_FROM_ZERO_TO_NEXT_FULL_TICK",
            "planned_initial_loss_formula": "STOP_TICKS_X_TICK_VALUE_PLUS_STRESS_ROUND_TRIP_COST",
            "maximum_planned_initial_loss_usd": "250",
            "risk_cap_failure": "EXPLICIT_POLICY_ABSTENTION_ZERO_POLICY_RETURN",
            "maximum_open_risk_usd": "250",
            "maximum_open_positions": 1,
            "maximum_entries_per_session": 1,
            "daily_loss_threshold_usd": "500",
            "continuous_drawdown_threshold_usd": "1500",
            "drawdown_breach": "NO_FURTHER_ENTRIES_IN_CONTINUOUS_ACCOUNT_PATH",
            "continuous_account_and_independent_fold_year_views_reported_separately": True,
            "gap_through_stop": "CONSERVATIVE_REPORTED_BAR_OPEN_FILL_AND_ACTUAL_LOSS_RECORDED",
        },
        "costs": {
            "label": "PROVIDER_NEUTRAL_CONSERVATIVE_PROVISIONAL_RESEARCH_COSTS",
            "fee_per_side_usd": "5.00",
            "round_trip_fee_usd": "10.00",
            "round_trip_adverse_execution_ticks_total": {
                scenario: {market: int(spec[scenario]) for market, spec in TICK_SPEC.items()}
                for scenario in ("base", "stress", "extreme")
            },
            "tick_allocation": "CEILING_HALF_ADVERSE_TICKS_AT_ENTRY_REMAINDER_AT_EXIT_DIRECTIONALLY_ADVERSE",
            "tick_size": {market: spec["tick_size"] for market, spec in TICK_SPEC.items()},
            "tick_value_usd": {
                market: spec["tick_value_usd"] for market, spec in TICK_SPEC.items()
            },
            "promotion_scenario": "stress",
            "may_be_reduced_after_outcomes": False,
            "exact_live_cost_claim": False,
        },
        "baselines": {
            "mandatory": [
                "flat_no_trade",
                "fold_local_unconditional_direction",
                "previous_reported_bar_sign_momentum",
                "previous_reported_bar_sign_reversal",
                "risk_matched_always_long",
                "risk_matched_always_short",
            ],
            "equivalence_ablation": "candidate_signal_without_cross_market_ranking",
            "flat_no_trade": "EXACT_ZERO_NO_FEATURE_ENTRY_EXIT_OR_COST",
            "fold_local_unconditional_direction": "PER_MARKET_LONG_IF_TRAINING_MEAN_LONG_STRESS_NET_R_GTE_SHORT_ELSE_SHORT_THEN_RANK_TRAINING_MEAN_SELECTED_DIRECTION_DESC_FIXED_MARKET_TIE",
            "previous_reported_bar_sign_momentum": "LONG_IF_LOG_RETURN_1_POSITIVE_SHORT_IF_NEGATIVE_ABSTAIN_IF_ZERO_RANK_ABS_LOG_RETURN_1_DESC_FIXED_MARKET_TIE",
            "previous_reported_bar_sign_reversal": "SHORT_IF_LOG_RETURN_1_POSITIVE_LONG_IF_NEGATIVE_ABSTAIN_IF_ZERO_RANK_ABS_LOG_RETURN_1_DESC_FIXED_MARKET_TIE",
            "risk_matched_always_long": "LONG_FIRST_RISK_ELIGIBLE_MARKET_IN_CL_ES_NG_NQ_RTY_YM_ORDER",
            "risk_matched_always_short": "SHORT_FIRST_RISK_ELIGIBLE_MARKET_IN_CL_ES_NG_NQ_RTY_YM_ORDER",
            "baseline_model_hurdle": "NONE",
            "baseline_selected_intent_trigger_rule": "SAME_TRIGGER_TIMEOUT_WITH_NO_RUNNER_UP_SUBSTITUTION",
            "active_baseline_independence": "OWN_CAUSAL_UNIVERSE_DIRECTION_RANKING_TRIGGER_ORDER_FILL_STOP_TIME_EXIT_COSTS_OVERLAP_ENTRY_LIMIT_DAILY_LOSS_EQUITY_AND_DRAWDOWN",
            "candidate_schedule_reuse": False,
            "same_cost_and_risk_scenarios_as_candidate": True,
        },
        "metrics": {
            "primary_series": "ONE_PORTFOLIO_STRESS_NET_PNL_USD_PER_ELIGIBLE_SESSION_INCLUDING_ZERO_NO_TRADE",
            "required": [
                "gross_pnl_usd", "fees_usd", "slippage_usd", "net_pnl_usd",
                "daily_annualized_sharpe_252", "daily_annualized_sortino_252",
                "maximum_continuous_drawdown_usd", "turnover_contract_round_trips",
                "trade_count", "hit_rate", "gross_exposure", "net_exposure",
                "portfolio_year_results", "market_year_results", "fold_results",
                "exit_dispositions", "abstention_dispositions", "coverage",
            ],
            "per_trade_sharpe_or_sortino_label_forbidden": True,
            "report_cost_scenarios": ["base", "stress", "extreme"],
            "missing_metric_or_coverage": "FAIL_CLOSED",
        },
        "statistics": {
            "primary_test": "ONE_SIDED_STATIONARY_BLOCK_BOOTSTRAP_MEAN_DAILY_STRESS_NET_PNL",
            "replications": 10000,
            "mean_block_length_sessions": 5,
            "fixed_seed": 20260807,
            "alpha": "0.05",
            "primary_lower_bound": "FIFTH_PERCENTILE_OF_10000_BOOTSTRAP_MEANS_MUST_EXCEED_ZERO",
            "baseline_comparisons": "PAIRED_DAILY_CANDIDATE_MINUS_BASELINE",
            "paired_resampling": "SAME_STATIONARY_BOOTSTRAP_SESSION_INDICES_FOR_CANDIDATE_AND_BASELINE",
            "multiplicity": "BONFERRONI_SIX_COMPARISONS_ONE_SIDED_ALPHA_PER_COMPARISON_0.008333333333333333",
            "paired_lower_bound": "PERCENTILE_0.8333333333333333_OF_EACH_10000_BOOTSTRAP_PAIRED_MEAN_DIFFERENCE_MUST_EXCEED_ZERO",
            "positive_point_estimate_alone_is_sufficient": False,
        },
        "promotion": {
            "stress_net_pnl_positive": True,
            "primary_bootstrap_lower_bound_above_zero": True,
            "beats_true_zero_and_each_mandatory_baseline_total_stress_net_pnl": True,
            "each_bonferroni_adjusted_paired_baseline_lower_bound_above_zero": True,
            "minimum_total_trades": 40,
            "minimum_portfolio_years_with_five_trades": 3,
            "positive_portfolio_years_required_of_five": 3,
            "positive_evaluation_folds_required_of_eight": 5,
            "positive_market_year_cells_required_of_thirty": 12,
            "markets_positive_overall_required_of_six": 4,
            "maximum_single_market_share_of_positive_stress_pnl": "0.50",
            "maximum_continuous_drawdown_usd": "1500",
            "complete_metrics_coverage_lineage_runtime_and_authorization_required": True,
            "promotion_label": "PASS_HISTORICAL_SCREEN_ONLY",
            "live_readiness_claim": False,
        },
        "preexecution_readiness_required_before_registration": {
            "source_report_alone_is_sufficient": False,
            "required_row_certification": [
                "FINITE_CAUSAL_OHLCV_FEATURE_INPUTS_BY_MARKET_FOLD_ROLE",
                "TRAINING_ONLY_TRANSFORMATION_AVAILABILITY",
                "FORTY_MINUTE_PURGE_AND_ONE_SESSION_EMBARGO",
                "EXACT_TRIGGER_ORDER_ENTRY_AND_SAME_IDENTITY_STOP_OR_TIME_EXIT_LEDGER",
                "ALL_REPORTED_BARS_BETWEEN_ENTRY_AND_EXIT_CLASSIFIED",
                "SCENARIO_SPECIFIC_STANDARD_CONTRACT_RISK_DISPOSITIONS",
                "CANDIDATE_AND_EACH_BASELINE_INDEPENDENT_UNIVERSE_COVERAGE",
                "EVERY_REQUIRED_METRIC_AND_PROMOTION_PATH_COMPUTABLE",
            ],
            "checkpoint_accounting_percent": 100,
            "triggered_path_coverage_percent": 100,
            "failed_or_unverifiable_gate": "BLOCK_REGISTRATION_AND_EXECUTION",
            "synthetic_tests_are_real_source_proof": False,
        },
        "execution_authority": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_host_runtime_seconds": 3600,
            "estimated_external_cost_usd": "0",
            "historical_rows_authorized": False,
            "model_fit_prediction_evaluation_authorized": False,
            "registration_authorized": False,
            "publication_authorized": False,
            "provider_network_credentials": False,
            "holdout_2025": False,
            "staging_commit_push": False,
            "trading": False,
        },
        "bindings": dict(sorted(bindings.items())),
    }
    return {**core, "protocol_id": sha256_json(core)}
