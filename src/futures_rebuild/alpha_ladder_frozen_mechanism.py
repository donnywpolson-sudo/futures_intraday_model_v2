"""Frozen mechanism and promotion rules for the Alpha research ladder.

This module is preparation and validation code only.  It cannot read market
rows, register a trial, execute research, or activate the ladder.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .canonical import sha256_json
from .errors import IntegrityError, UnauthorizedOperation


MECHANISM_SCHEMA = "alpha_ladder_frozen_mechanism/1.0.0"
TIER0_CERTIFICATE_SCHEMA = "alpha_ladder_tier0_synthetic_certificate/1.0.0"
TIER0_DECISION_SCHEMA = "alpha_ladder_stage_decision/1.0.0"
MANDATORY_BASELINES = (
    "flat_no_trade",
    "fold_local_unconditional_direction",
    "previous_reported_bar_sign_momentum",
    "previous_reported_bar_sign_reversal",
    "risk_matched_always_long",
    "risk_matched_always_short",
)
PHYSICAL_OR_SATELLITE = frozenset({
    "CL", "NG", "RB", "HO", "GC", "SI", "HG", "PL", "PA", "BTC", "ETH",
    "ZC", "ZS", "ZL", "ZM", "ZW", "KE", "LE", "HE", "GF",
})


def _identity(payload: Mapping[str, object], key: str, schema: str) -> None:
    core = dict(payload)
    identity = core.pop(key, None)
    if core.get("schema_version") != schema or identity != sha256_json(core):
        raise IntegrityError(f"{key} is invalid")


def _decimal(value: object, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError(f"{name} is not decimal") from exc
    if not result.is_finite():
        raise IntegrityError(f"{name} is not finite")
    return result


def _bool_sequence(value: object, *, length: int, name: str) -> tuple[bool, ...]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or len(value) != length
        or any(type(item) is not bool for item in value)
    ):
        raise IntegrityError(f"{name} must contain exactly {length} booleans")
    return tuple(value)


def build_frozen_mechanism(
    *, contract_id: str, profile_id: str, source_protocol_id: str,
    source_protocol_sha256: str, all_markets: Sequence[str],
) -> dict[str, object]:
    """Build the one mechanism shared by every Alpha ladder stage."""

    markets = tuple(all_markets)
    if len(markets) != 41 or len(set(markets)) != 41:
        raise IntegrityError("frozen mechanism requires the canonical 41-market order")
    for name, digest in {
        "contract_id": contract_id,
        "profile_id": profile_id,
        "source_protocol_id": source_protocol_id,
        "source_protocol_sha256": source_protocol_sha256,
    }.items():
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise IntegrityError(f"{name} must be a SHA-256 digest")

    base_ticks = {market: (4 if market in PHYSICAL_OR_SATELLITE else 2) for market in markets}
    core: dict[str, object] = {
        "schema_version": MECHANISM_SCHEMA,
        "state": "PREPARED_UNPUBLISHED_NOT_REGISTERED",
        "classification": "ONE_FROZEN_ALPHA_LADDER_MECHANISM",
        "research_only": True,
        "live_readiness": False,
        "ladder_binding": {"contract_id": contract_id, "profile_id": profile_id},
        "source_design_binding": {
            "protocol_id": source_protocol_id,
            "sha256": source_protocol_sha256,
            "reused_semantics_only": True,
            "six_market_scope_reused": False,
        },
        "features": {
            "names": [
                "log_return_1", "log_return_5", "log_return_10",
                "intrabar_range_fraction", "atr_10_fraction", "range_to_atr_10",
                "realized_volatility_10", "log1p_volume", "volume_zscore_10",
            ],
            "causal_window": "REPORTED_BARS_EVENT_AT_09_30_THROUGH_BEFORE_10_00_AVAILABLE_BY_10_00_05",
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
                "atr_10_fraction", "range_to_atr_10", "realized_volatility_10",
            ],
            "time_of_session": "FIXED_10_00_CHECKPOINT_NOT_A_MODEL_COLUMN",
            "missing_nonfinite_or_future_known": "EXPLICIT_CAUSAL_ABSTENTION",
        },
        "transformations": {
            "standardization": "PER_MARKET_PER_FOLD_TRAINING_ONLY_MEAN_AND_POPULATION_STD",
            "zero_training_std": "STANDARDIZED_VALUE_ZERO",
        },
        "model_family": "MARKET_SPECIFIC_TWO_TARGET_RIDGE",
        "model_parameters": {
            "targets": ["LONG_STRESS_NET_R", "SHORT_STRESS_NET_R"],
            "ridge_penalty": "1.0", "intercept": True,
            "hyperparameter_search": False, "market_pooling": False,
            "entry_hurdle_predicted_stress_net_r": "0.25",
            "hurdle_comparison": "GREATER_THAN_OR_EQUAL",
            "target_definition": "DIRECTIONAL_STRESS_NET_PNL_USD_DIV_PLANNED_INITIAL_LOSS_USD_USING_LOCKED_STOP_AND_TIME_EXIT",
            "training_row_eligibility": "FEATURE_COMPLETE_STRESS_RISK_ELIGIBLE_TRIGGER_OBSERVED_COMPLETE_SAME_IDENTITY_PATH",
            "no_trigger_or_incomplete_target": "NO_LABEL_NO_IMPUTATION_NO_ZERO_TARGET",
        },
        "checkpoint": {
            "chicago_time": "10:00", "decision_time": "10:00:05_AMERICA_CHICAGO",
            "one_checkpoint_per_eligible_session": True,
        },
        "entry_rules": {
            "trigger": "FIRST_REPORTED_BAR_AVAILABLE_AFTER_DECISION_WITHIN_120_SECONDS",
            "order_time": "TRIGGER_AVAILABLE_AT",
            "fill": "FIRST_LATER_REPORTED_BAR_OPEN_AVAILABLE_WITHIN_120_SECONDS_OF_ORDER",
            "trigger_bar_as_fill": False,
            "no_trigger": "EXPLICIT_NO_TRADE_TIMEOUT",
            "runner_up_substitution": False,
            "same_actual_contract_identity_required": True,
            "time_exit": "FIRST_CAUSAL_REPORTED_BAR_AT_OR_AFTER_ENTRY_PLUS_30_MINUTES_WITHIN_120_SECONDS",
        },
        "ranking": {
            "rule": "PREDICTED_STRESS_NET_R_DESC_THEN_CANONICAL_41_MARKET_ORDER",
            "canonical_market_order": list(markets),
            "restricted_to_current_stage_markets": True,
            "one_ranked_intent_per_session": True,
        },
        "costs": {
            "label": "PROVIDER_NEUTRAL_CONSERVATIVE_PROVISIONAL_RESEARCH_COSTS",
            "fee_per_side_usd": "5.00", "round_trip_fee_usd": "10.00",
            "promotion_scenario": "stress",
            "tick_size_and_value": "SOURCE_BOUND_ACTUAL_CONTRACT_IDENTITY_FROM_LOCKED_ECONOMICS",
            "round_trip_adverse_ticks": {
                "base": base_ticks,
                "stress": {market: ticks * 2 for market, ticks in base_ticks.items()},
                "extreme": {market: ticks * 4 for market, ticks in base_ticks.items()},
            },
            "tick_allocation": "CEILING_HALF_AT_ENTRY_REMAINDER_AT_EXIT_DIRECTIONALLY_ADVERSE",
            "may_be_reduced_after_outcomes": False,
            "exact_live_cost_claim": False,
        },
        "stop": {
            "distance": "1.5_ATR20_ROUNDED_AWAY_FROM_ZERO_TO_FULL_TICK",
            "gap_fill": "CONSERVATIVE_REPORTED_BAR_OPEN",
            "profit_target": None,
        },
        "sizing": {
            "instrument": "ONE_BOUND_STANDARD_CONTRACT", "micro_proxy": False,
            "planned_loss": "STOP_TICKS_X_TICK_VALUE_PLUS_STRESS_ROUND_TRIP_COST",
            "maximum_planned_loss_usd": "250", "maximum_open_positions": 1,
            "maximum_entries_per_session": 1, "daily_loss_limit_usd": "500",
            "continuous_drawdown_limit_usd": "1500",
            "risk_failure": "EXPLICIT_POLICY_ABSTENTION_ZERO_POLICY_RETURN",
        },
        "baselines": {
            "mandatory": list(MANDATORY_BASELINES),
            "flat_no_trade": "EXACT_ZERO_NO_FEATURE_ENTRY_EXIT_OR_COST",
            "fold_local_unconditional_direction": "PER_MARKET_TRAINING_MEAN_BEST_STRESS_NET_DIRECTION_THEN_RANK_DESC_CANONICAL_TIE",
            "previous_reported_bar_sign_momentum": "LONG_POSITIVE_SHORT_NEGATIVE_ABSTAIN_ZERO_RANK_ABS_RETURN_DESC_CANONICAL_TIE",
            "previous_reported_bar_sign_reversal": "SHORT_POSITIVE_LONG_NEGATIVE_ABSTAIN_ZERO_RANK_ABS_RETURN_DESC_CANONICAL_TIE",
            "risk_matched_always_long": "LONG_FIRST_RISK_ELIGIBLE_MARKET_IN_STAGE_CANONICAL_ORDER",
            "risk_matched_always_short": "SHORT_FIRST_RISK_ELIGIBLE_MARKET_IN_STAGE_CANONICAL_ORDER",
            "active_baselines": "OWN_CAUSAL_UNIVERSE_DIRECTION_RANKING_TRIGGER_FILL_EXIT_COSTS_SCHEDULE_RISK_AND_EQUITY",
            "candidate_schedule_reuse": False,
            "same_stage_market_scope_and_cost_risk_scenarios": True,
        },
        "fold_construction": {
            "calendar_basis": "CHECKPOINT_ELIGIBLE_SESSIONS_BEFORE_SOURCE_COMPLETENESS",
            "initial_training_sessions": 504, "evaluation_sessions": 63,
            "outer_folds": 8, "purge_minutes": 40, "embargo_sessions": 1,
            "fold_shortening_or_reassignment": False,
            "pilot_evaluation_sessions_excluded_FROM_ALL_LATER_MARKETS": True,
        },
        "metrics": {
            "primary_series": "DAILY_PORTFOLIO_STRESS_NET_PNL_INCLUDING_ZERO_NO_TRADE",
            "continuous_and_independent_fold_year_views_separate": True,
            "daily_annualized_statistics_only": True,
            "required": [
                "gross_pnl_usd", "fees_usd", "slippage_usd", "net_pnl_usd",
                "daily_annualized_sharpe_252", "daily_annualized_sortino_252",
                "maximum_continuous_drawdown_usd", "turnover_contract_round_trips",
                "trade_count", "hit_rate", "gross_exposure", "net_exposure",
                "portfolio_year_results", "market_year_results", "fold_results",
                "exit_dispositions", "abstention_dispositions", "coverage",
            ],
            "missing_metric_or_coverage": "FAIL_CLOSED",
        },
        "promotion_gates": {
            "common": {
                "stress_net_pnl_positive": True,
                "beat_true_zero_and_every_mandatory_baseline": True,
                "complete_coverage_metrics_lineage_and_authorization": True,
                "maximum_continuous_drawdown_usd": "1500",
                "live_readiness_claim": False,
            },
            "pilot": {
                "minimum_trades": 8, "formal_significance_required": False,
                "role": "GO_NO_GO_SCREEN_ONLY",
            },
            "tier_1": {
                "minimum_trades": 40, "positive_markets_required": 3,
                "market_count": 4, "positive_market_year_cells_required": 8,
                "market_year_cell_count": 20, "positive_years_required": 3,
                "year_count": 5, "positive_folds_required": 5, "fold_count": 8,
                "formal_multiplicity_adjusted_bootstrap_required": True,
            },
            "tier_2": {
                "minimum_trades": 40, "positive_markets_required": 11,
                "market_count": 16, "positive_market_year_cells_required": 32,
                "market_year_cell_count": 80, "positive_years_required": 3,
                "year_count": 5, "positive_folds_required": 5, "fold_count": 8,
                "core_and_additions_must_pass_separately": True,
                "positive_added_markets_required": 8, "added_market_count": 12,
                "formal_multiplicity_adjusted_bootstrap_required": True,
            },
            "tier_3": {
                "minimum_trades": 40, "positive_traditional_markets_required": 26,
                "traditional_market_count": 38,
                "positive_traditional_market_year_cells_required": 76,
                "traditional_market_year_cell_count": 190,
                "positive_years_required": 3, "year_count": 5,
                "positive_folds_required": 5, "fold_count": 8,
                "traditional_and_combined_must_pass": True,
                "satellites_can_rescue_traditional_failure": False,
                "formal_multiplicity_adjusted_bootstrap_required": True,
            },
        },
        "statistics": {
            "replications": 10000, "stationary_mean_block_sessions": 5,
            "seed": 20260807, "primary_one_sided_alpha": "0.05",
            "baseline_comparisons": "PAIRED_DAILY_SAME_BOOTSTRAP_INDICES",
            "multiplicity": "BONFERRONI_SIX_ONE_SIDED_COMPARISONS",
        },
        "authority": {
            "historical_rows": False, "registration": False, "execution": False,
            "holdout_2025": False, "provider_network_credentials": False,
            "publication": False, "trading": False,
        },
    }
    return {**core, "mechanism_id": sha256_json(core)}


def validate_frozen_mechanism(payload: Mapping[str, object]) -> dict[str, object]:
    _identity(payload, "mechanism_id", MECHANISM_SCHEMA)
    authority = payload.get("authority")
    if not isinstance(authority, Mapping) or any(value is not False for value in authority.values()):
        raise IntegrityError("frozen mechanism cannot grant authority")
    ranking = payload.get("ranking")
    gates = payload.get("promotion_gates")
    if not isinstance(ranking, Mapping) or len(tuple(ranking.get("canonical_market_order", ()))) != 41:
        raise IntegrityError("frozen mechanism lacks the canonical market order")
    if not isinstance(gates, Mapping) or set(gates) != {"common", "pilot", "tier_1", "tier_2", "tier_3"}:
        raise IntegrityError("frozen mechanism promotion topology is incomplete")
    return dict(payload)


def build_tier0_certificate(
    *, contract_id: str, profile_id: str, mechanism_id: str,
    mechanism_sha256: str, test_node_ids: Sequence[str], passed_test_count: int,
) -> dict[str, object]:
    nodes = tuple(test_node_ids)
    if not nodes or len(set(nodes)) != len(nodes) or any(not item for item in nodes):
        raise IntegrityError("Tier 0 certificate requires unique synthetic test nodes")
    if passed_test_count != len(nodes):
        raise UnauthorizedOperation("Tier 0 cannot pass with an incomplete synthetic suite")
    core = {
        "schema_version": TIER0_CERTIFICATE_SCHEMA,
        "stage": "tier_0", "decision": "PASS",
        "contract_id": contract_id, "profile_id": profile_id,
        "mechanism_id": mechanism_id, "mechanism_sha256": mechanism_sha256,
        "evidence_class": "SYNTHETIC_ENGINEERING_ONLY",
        "historical_rows_opened": False, "alpha_evidence": False,
        "profitability_claim": False, "test_node_ids": list(nodes),
        "passed_test_count": passed_test_count,
    }
    return {**core, "certificate_id": sha256_json(core)}


def validate_tier0_certificate(
    payload: Mapping[str, object], *, contract_id: str, mechanism_sha256: str,
) -> dict[str, object]:
    _identity(payload, "certificate_id", TIER0_CERTIFICATE_SCHEMA)
    nodes = payload.get("test_node_ids")
    if (
        payload.get("stage") != "tier_0" or payload.get("decision") != "PASS"
        or payload.get("contract_id") != contract_id
        or payload.get("mechanism_sha256") != mechanism_sha256
        or payload.get("evidence_class") != "SYNTHETIC_ENGINEERING_ONLY"
        or payload.get("historical_rows_opened") is not False
        or payload.get("alpha_evidence") is not False
        or payload.get("profitability_claim") is not False
        or not isinstance(nodes, Sequence) or isinstance(nodes, (str, bytes))
        or payload.get("passed_test_count") != len(nodes)
    ):
        raise UnauthorizedOperation("Tier 0 synthetic certificate is incomplete or mismatched")
    return dict(payload)


def build_tier0_decision(
    *, contract_id: str, mechanism_sha256: str,
    synthetic_certificate_path: str, synthetic_certificate_sha256: str,
) -> dict[str, object]:
    if not synthetic_certificate_path:
        raise IntegrityError("Tier 0 decision requires its synthetic certificate path")
    core = {
        "schema_version": TIER0_DECISION_SCHEMA,
        "contract_id": contract_id,
        "mechanism_sha256": mechanism_sha256,
        "stage": "tier_0",
        "decision": "PASS",
        "synthetic_certificate_path": synthetic_certificate_path,
        "synthetic_certificate_sha256": synthetic_certificate_sha256,
    }
    return {**core, "decision_id": sha256_json(core)}


def validate_promotion_evidence(
    evidence: Mapping[str, object], *, stage: str, markets: Sequence[str],
) -> None:
    """Reject a PASS decision unless every predeclared stage gate passes."""

    if stage not in {"pilot", "tier_1", "tier_2", "tier_3"}:
        return
    stress = _decimal(evidence.get("stress_net_pnl_usd"), "stress net P&L")
    baselines = evidence.get("baseline_stress_net_pnl_usd")
    if not isinstance(baselines, Mapping) or set(baselines) != set(MANDATORY_BASELINES):
        raise IntegrityError("promotion evidence lacks every mandatory baseline")
    baseline_values = {name: _decimal(value, name) for name, value in baselines.items()}
    if stress <= 0 or baseline_values["flat_no_trade"] != 0 or any(
        stress <= value for value in baseline_values.values()
    ):
        raise UnauthorizedOperation("stress result did not beat zero and every baseline")
    trade_count = evidence.get("trade_count")
    if type(trade_count) is not int or trade_count < (8 if stage == "pilot" else 40):
        raise UnauthorizedOperation("stage trade-count gate did not pass")
    if (
        not Decimal("0") <= _decimal(
            evidence.get("maximum_continuous_drawdown_usd"), "drawdown"
        ) <= Decimal("1500")
        or evidence.get("complete_coverage") is not True
        or evidence.get("complete_metrics") is not True
        or evidence.get("risk_rules_compliant") is not True
        or evidence.get("live_readiness_claim") is not False
    ):
        raise UnauthorizedOperation("common promotion gate did not pass")
    if stage == "pilot":
        if evidence.get("formal_significance_claim") is not False:
            raise UnauthorizedOperation("pilot cannot claim formal confirmation")
        return
    if (
        evidence.get("primary_bootstrap_lower_bound_above_zero") is not True
        or evidence.get("all_paired_baseline_lower_bounds_above_zero") is not True
    ):
        raise UnauthorizedOperation("formal multiplicity-adjusted tests did not pass")
    positive_markets = evidence.get("positive_markets")
    if (
        not isinstance(positive_markets, Sequence)
        or isinstance(positive_markets, (str, bytes))
        or any(not isinstance(market, str) for market in positive_markets)
    ):
        raise IntegrityError("positive-market evidence is malformed")
    if len(set(positive_markets)) != len(positive_markets) or not set(positive_markets) <= set(markets):
        raise IntegrityError("positive-market evidence is outside the stage universe")
    years = _bool_sequence(evidence.get("positive_portfolio_years"), length=5, name="year evidence")
    folds = _bool_sequence(evidence.get("positive_folds"), length=8, name="fold evidence")
    if sum(years) < 3 or sum(folds) < 5:
        raise UnauthorizedOperation("year or fold breadth gate did not pass")
    required_market_years = {"tier_1": (20, 8), "tier_2": (80, 32), "tier_3": (190, 76)}[stage]
    cells = _bool_sequence(
        evidence.get("positive_market_year_cells"), length=required_market_years[0],
        name="market-year evidence",
    )
    if sum(cells) < required_market_years[1]:
        raise UnauthorizedOperation("market-year breadth gate did not pass")
    if stage == "tier_1" and len(positive_markets) < 3:
        raise UnauthorizedOperation("Tier 1 market breadth gate did not pass")
    if stage == "tier_2":
        subgroup = evidence.get("subgroup_decisions")
        subgroup_pnl = evidence.get("subgroup_stress_net_pnl_usd")
        additions = [market for market in markets if market not in {"ES", "CL", "ZN", "6E"}]
        if (
            len(positive_markets) < 11 or not isinstance(subgroup, Mapping)
            or subgroup.get("core") != "PASS" or subgroup.get("additions") != "PASS"
            or subgroup.get("combined") != "PASS"
            or not isinstance(subgroup_pnl, Mapping)
            or set(subgroup_pnl) != {"core", "additions", "combined"}
            or any(_decimal(value, "Tier 2 subgroup stress P&L") <= 0
                   for value in subgroup_pnl.values())
            or len(set(positive_markets) & set(additions)) < 8
        ):
            raise UnauthorizedOperation("Tier 2 replication subgroup gate did not pass")
    if stage == "tier_3":
        subgroup = evidence.get("subgroup_decisions")
        traditional = evidence.get("traditional_gate_results")
        if (
            len(positive_markets) < 26 or not isinstance(subgroup, Mapping)
            or subgroup.get("traditional") != "PASS" or subgroup.get("combined") != "PASS"
            or subgroup.get("satellite_can_rescue_traditional_failure") is not False
            or not isinstance(traditional, Mapping)
            or traditional != {
                "stress_net_pnl_positive": True,
                "beat_zero_and_all_baselines": True,
                "formal_tests_passed": True,
                "complete_coverage_and_metrics": True,
                "drawdown_within_1500": True,
            }
        ):
            raise UnauthorizedOperation("Tier 3 traditional subgroup gate did not pass")
