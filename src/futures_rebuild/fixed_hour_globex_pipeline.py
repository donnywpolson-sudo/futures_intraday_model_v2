"""Inactive contracts and synthetic check for the all-Globex hourly pipeline."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from .exchange_calendar import CalendarSession, TradingStateInterval
from .fixed_hour_feature_expansion import (
    build_bound_feature_record,
    cost_adjusted_directions,
    frozen_feature_foundation_contract,
)
from .fixed_hour_outcome_foundation import (
    build_directional_path_outcome,
    frozen_outcome_foundation_contract,
    synthetic_globex_hourly_opportunity_grid,
    SYNTHETIC_SOURCE_CONTRACT_ID,
    SYNTHETIC_SOURCE_RELEASE_ID,
)
from .fixed_hour_ridge_discovery import NANO
from .research.splits import TemporalSamples


STAGE_2_5_SCHEMA = "causal_search_access_contract/2.5.0"
STAGE_5_SCHEMA = "fixed_hour_globex_mechanism_evaluation/1.0.0"
STAGE_5_V2_SCHEMA = "fixed_hour_globex_mechanism_evaluation/1.1.0"
STAGE_5_V3_SCHEMA = "fixed_hour_globex_mechanism_evaluation/1.2.0"
STAGE_5_V4_SCHEMA = "fixed_hour_globex_mechanism_evaluation/1.3.0"
STAGE_6_SCHEMA = "fixed_hour_globex_synthetic_certificate/1.0.0"
STAGE_6_V2_SCHEMA = "fixed_hour_globex_synthetic_certificate/1.1.0"
STAGE_6_V3_SCHEMA = "fixed_hour_globex_synthetic_certificate/1.2.0"
STAGE_6_V4_SCHEMA = "fixed_hour_globex_synthetic_certificate/1.3.0"
MARKETS = ("ES", "NQ", "CL", "GC")
PREDECESSOR_ID = "05462266b28b9e8df3cc23f78644b9b1b3f8e8554363e9460cc4f3a25bced974"
PREDECESSOR_PATH = Path(
    "state/causal_search_access_contract_registry"
    f"/{PREDECESSOR_ID}/contract.json"
)
STAGE_5_V1_PATH = Path("configs/fixed_hour_globex_mechanism_evaluation_v1.json")
STAGE_5_V2_PATH = Path("configs/fixed_hour_globex_mechanism_evaluation_v2.json")
STAGE_5_V3_PATH = Path("configs/fixed_hour_globex_mechanism_evaluation_v3.json")
STAGE_6_V1_PATH = Path("state/unpublished_evidence/fixed_hour_globex_synthetic/91b150836a9f932291f42ad5855ee44a62b4ac5cc4e6160b1bfdf38b796faccf/certificate.json")
STAGE_6_V2_PATH = Path("state/unpublished_evidence/fixed_hour_globex_synthetic/1c8af9be96d6c057b2c07f1307e0eacd9ee15613b91f44e1371953c2495f9598/certificate.json")
STAGE_6_V3_PATH = Path("state/unpublished_evidence/fixed_hour_globex_synthetic/4593e97b8e35fba92af8234d79ce755ab1338361735e69ae4973f2e06bca8f8f/certificate.json")


def _binding(root: Path, path: Path, identity: str) -> dict[str, str]:
    return {
        "contract_id": identity,
        "path": path.as_posix(),
        "sha256": sha256_file(root / path),
    }


def _published(root: Path, path: Path) -> dict[str, object]:
    payload = json.loads((root / path).read_bytes())
    if not isinstance(payload, dict) or (root / path).read_bytes() != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"published fixed-hour artifact is invalid: {path.as_posix()}")
    return payload


def frozen_stage_2_5_contract(root: Path) -> dict[str, object]:
    """Predeclare the exact new experiment without authorizing row access."""

    outcome = frozen_outcome_foundation_contract()
    features = frozen_feature_foundation_contract()
    core: dict[str, object] = {
        "schema_version": STAGE_2_5_SCHEMA,
        "state": "PREPARED_NOT_ACTIVE",
        "authority": {
            "development_rows": False,
            "challenge_rows": False,
            "final_252_or_forward": False,
            "registration_or_execution": False,
            "provider_or_network": False,
            "trading": False,
        },
        "predecessor": _binding(root, PREDECESSOR_PATH, PREDECESSOR_ID),
        "supersession": {
            "reason": "BIND_CORRECTED_PHASE_4_PROVENANCE_WITHOUT_CHANGING_THE_RESEARCH_QUESTION",
            "history_preserved": True,
            "scientific_reinterpretation": False,
        },
        "question": {
            "claim_ceiling": "ROBUST_PREDICTIVE_ASSOCIATION",
            "causal_question": "Do point-in-time one-minute OHLCV states predict after-cost long, short, or abstain decisions over the next fixed 60 minutes throughout verified Globex opens?",
            "estimand": "SESSION_AND_MARKET_BALANCED_STRESS_NET_FIXED_EXIT_TICKS",
            "mechanism_family": "ALL_GLOBEX_HOURLY_THREE_TARGET_PER_MARKET_RIDGE",
        },
        "source_scope": {
            "foundation": "ACTIVE_CAUSALLY_GATED_NORMALIZED_ONE_MINUTE_OHLCV_ONLY",
            "development_period": ["2018-01-01T00:00:00Z", "2023-01-01T00:00:00Z"],
            "development_markets": list(MARKETS),
            "tier_0": ["ES"],
            "tier_1_incremental": ["NQ", "CL", "GC"],
            "broad_generality_claim": False,
            "forbidden": ["RAW_DBN_ROWS", "2023_PLUS", "FINAL_252", "FORWARD_DATA"],
        },
        "tier_0_access_request_template": {
            "status": "NOT_ISSUED",
            "market": "ES",
            "period": ["2018-01-01T00:00:00Z", "2023-01-01T00:00:00Z"],
            "maximum_market_year_partitions": 5,
            "maximum_rows": 2_500_000,
            "maximum_bytes": 2_147_483_648,
            "maximum_uses": 1,
            "read_only": True,
            "provider_or_network": False,
            "allowed_fields": [
                "row_id", "root", "source_contract_id", "source_release_id",
                "source_file_sha256", "source_row_sha256", "definition_source_file_sha256",
                "definition_row_sha256", "event_at", "available_at", "open", "high", "low",
                "close", "volume", "actual_tradable_contract", "tick_size",
                "contract_multiplier", "tick_value_usd", "roll_flag",
                "missingness_state", "verified_open_interval", "trade_date",
            ],
            "output_root": "state/unpublished_evidence/fixed_hour_globex_tier0_es",
        },
        "timing": {
            "display_timezone": "America/Los_Angeles",
            "schedule": "EVERY_ELIGIBLE_PLUS_22_MINUTE_ALIGNED_NONOVERLAPPING_WINDOW_IN_EACH_VERIFIED_GLOBEX_OPEN_INTERVAL",
            "feature_history": "21_CONSECUTIVE_OPEN_BARS_PRODUCING_20_RETURNS",
            "decision_delay_seconds": 5,
            "entry": "NEXT_MINUTE_OPEN_PRINT_PROXY",
            "fixed_exit_minutes": 60,
            "break_roll_or_sparse_path": "INDETERMINATE_NO_TRADE_NO_LABEL",
        },
        "foundation_bindings": {
            "outcome_contract_id": outcome["contract_id"],
            "feature_contract_id": features["contract_id"],
        },
        "search": {
            "trial_family": "all_globex_hourly_three_target_per_market_ridge",
            "predefined_expansion_formula_count": 13,
            "candidate_budget": 9,
            "legacy_attempts_charged": 172,
            "combined_information_family_burden": 181,
            "adaptive_candidate_creation": False,
            "candidate_retry": False,
            "outer_result_reuse_for_design": False,
        },
        "evidence_rules": {
            "synthetic": "ENGINEERING_ONLY_NO_ALPHA_CLAIM",
            "development_wfa": "SELECTION_AND_ESTIMATION_NOT_CONFIRMATION",
            "tier_1": "UNCHANGED_MECHANISM_REPLICATION",
            "recent_challenge": "ONE_SHOT_CLOSE_ON_FAILURE",
            "final_252": "SEALED_UNREAD_SEPARATE_APPROVAL",
        },
        "stage_order": ["2.5", "3", "4", "5", "6", "7", "8", "10.5", "10.6", "11", "12"],
    }
    return {**core, "contract_id": sha256_json(core)}


def validate_stage_2_5_contract(payload: Mapping[str, object], *, root: Path) -> dict[str, object]:
    expected = frozen_stage_2_5_contract(root)
    if dict(payload) != expected:
        raise IntegrityError("all-Globex Stage 2.5 contract drifted")
    return dict(payload)


def frozen_stage_5_contract(root: Path) -> dict[str, object]:
    """Freeze the complete mechanism and evaluation procedure before WFA."""

    return _published(root, STAGE_5_V1_PATH)


def _obsolete_frozen_stage_5_contract_builder(root: Path) -> dict[str, object]:
    """Retained source of the historical v1 payload; use its published artifact."""

    stage_2_5 = frozen_stage_2_5_contract(root)
    outcome = frozen_outcome_foundation_contract()
    features = frozen_feature_foundation_contract()
    core: dict[str, object] = {
        "schema_version": STAGE_5_SCHEMA,
        "state": "FROZEN_PREPARED_NOT_EXECUTED",
        "authority": {
            "real_history": False,
            "holdout": False,
            "provider": False,
            "activation": False,
            "trading": False,
        },
        "bindings": {
            "stage_2_5_contract_id": stage_2_5["contract_id"],
            "outcome_contract_id": outcome["contract_id"],
            "feature_contract_id": features["contract_id"],
        },
        "scope": {
            "markets": list(MARKETS),
            "tier_0": ["ES"],
            "tier_1_incremental": ["NQ", "CL", "GC"],
            "opportunities": "ALL_FROZEN_SCHEDULE_OPPORTUNITIES_NO_SUBSAMPLING",
            "portfolio_or_capacity_claim": False,
        },
        "model": {
            "family": "THREE_INDEPENDENT_RIDGE_HEADS",
            "parameters": "PER_MARKET_NOT_POOLED",
            "targets": [
                "terminal_change_ticks",
                "maximum_upside_excursion_ticks",
                "maximum_downside_excursion_ticks",
            ],
            "feature_standardization": "FIT_TRAIN_ONLY_PER_MARKET",
            "target_standardization": "FIT_TRAIN_ONLY_PER_MARKET",
            "selected_hyperparameters": "CHOSEN_ON_ES_INNER_FOLDS_THEN_UNCHANGED_ACROSS_REPLICATIONS",
        },
        "selection": {
            "candidate_count": 9,
            "primary": "MEDIAN_INNER_FOLD_SESSION_AND_MARKET_BALANCED_STRESS_NET_TICKS",
            "secondary": "MEDIAN_NEGATIVE_STANDARDIZED_THREE_TARGET_MSE",
            "tie_break": "CANDIDATE_ID_ASCENDING",
            "feature_ranking": "13_PREDEFINED_FORMULAS_RANKED_INSIDE_EACH_INNER_TRAIN_SPLIT",
            "outer_results_may_change_selection": False,
        },
        "decision": {
            "long": "PREDICTED_TERMINAL_GT_STRESS_COST_AND_COST_ADJUSTED_LONG_MFE_MAE_GT_1",
            "short": "PREDICTED_NEGATIVE_TERMINAL_GT_STRESS_COST_AND_COST_ADJUSTED_SHORT_MFE_MAE_GT_1",
            "otherwise": "ABSTAIN",
            "position": "AT_MOST_ONE_CONTRACT_PER_MARKET_PER_OPPORTUNITY",
            "simultaneous_markets": "EVALUATED_SEPARATELY_NO_PORTFOLIO_CAPITAL_CLAIM",
            "exit": "FIXED_OPEN_PRINT_PROXY_EXACTLY_60_MINUTES_AFTER_ENTRY",
            "adjacent_windows": "CLOSE_THEN_INDEPENDENTLY_REOPEN_AT_SHARED_BOUNDARY_AND_CHARGE_BOTH_ROUND_TRIPS",
        },
        "costs": {
            "label": "PROVISIONAL_PROVIDER_NEUTRAL_CONSERVATIVE_RESEARCH_COSTS",
            "fee_round_trip_usd": "10.00",
            "adverse_ticks_round_trip": {
                "base": {"ES": 2, "NQ": 2, "CL": 4, "GC": 4},
                "stress": {"ES": 4, "NQ": 4, "CL": 8, "GC": 8},
                "extreme": {"ES": 8, "NQ": 8, "CL": 16, "GC": 16},
            },
            "total_cost_ticks": "ADVERSE_TICKS_PLUS_10_USD_DIVIDED_BY_POINT_IN_TIME_TICK_VALUE_USD",
            "selection_and_primary_gate_scenario": "stress",
            "may_be_reduced_after_result_access": False,
            "globex_segment_treatment": "SAME_CONSERVATIVE_COST_WITH_SEPARATE_PACIFIC_SEGMENT_REPORTING",
        },
        "chronology": {
            "split_unit": "COMPLETE_MARKET_TRADE_DATE",
            "same_trade_date_across_markets": "ONE_PARTITION_ONLY",
            "outer_folds": 8,
            "minimum_initial_fit_trade_dates": 504,
            "outer_audit_trade_dates": 63,
            "inner_folds": 4,
            "inner_audit_trade_dates": 63,
            "session_embargo": 1,
            "transforms_and_feature_selection": "TRAIN_ONLY",
            "label_interval_purge": "WHOLE_TRADE_DATE_CONSERVATIVE_PURGE",
        },
        "aggregation": {
            "row_independence_claim": False,
            "first": "MEAN_WITHIN_MARKET_TRADE_DATE",
            "second": "EQUAL_WEIGHT_MARKETS_WITHIN_TRADE_DATE",
            "inference_unit": "COMMON_COMPLETE_TRADE_DATE",
            "pacific_segment_reporting": "FOUR_EQUAL_DURATION_QUARTILES_WITHIN_EACH_VERIFIED_OPEN_INTERVAL",
        },
        "baselines": [
            "flat_no_trade",
            "fold_local_unconditional_direction",
            "previous_bar_sign_momentum",
            "previous_bar_sign_reversal",
            "always_long",
            "always_short",
        ],
        "multiplicity_and_inference": {
            "combined_trial_burden": 181,
            "deflated_sharpe_required": True,
            "one_sided_hac_lower_confidence_bound": "STRICTLY_ABOVE_ZERO_AT_95_PERCENT",
            "romano_wolf_family": "FOUR_MARKETS_AND_ALL_ACTIVE_BASELINE_DIFFERENTIALS",
            "romano_wolf_alpha": 0.05,
            "synchronized_common_trade_date_resampling": True,
            "individual_feature_p_values_or_fdr_claim": False,
        },
        "pass_fail": {
            "minimum_complete_trade_dates": 252,
            "minimum_executed_trades": 500,
            "minimum_schedule_coverage": 0.98,
            "quality_error_count_maximum": 0,
            "tier_0": "ES_STRESS_NET_POSITIVE_HAC_LOWER_BOUND_POSITIVE_DSR_AND_ALL_BASELINES_BEAT",
            "tier_1": "EACH_NQ_CL_GC_POSITIVE_AND_JOINT_ROMANO_WOLF_PASS_NO_ES_RESCUE",
            "segment_gate": "EACH_OPEN_INTERVAL_QUARTILE_WITH_63_DATES_MUST_HAVE_NONNEGATIVE_STRESS_MEAN",
            "extreme_cost": "REPORTED_NOT_PROMOTION_GATE",
            "failure": "CLOSE_MECHANISM_NO_TUNING_OR_RETRY",
        },
        "post_development": {
            "recent_challenge": "ONE_SHOT_2023_TO_2025_07_13_UNCHANGED_MECHANISM",
            "refit": "PARAMETERS_ONLY_NO_EVALUATION",
            "final_252": "SEALED_SEPARATE_APPROVAL",
            "forward": "MONITORING_ONLY_AFTER_ALL_GATES",
        },
        "execution_budget": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": 7200,
            "maximum_workers": 4,
            "maximum_external_cost_usd": "0",
        },
    }
    return {**core, "contract_id": sha256_json(core)}


def validate_stage_5_contract(payload: Mapping[str, object], *, root: Path) -> dict[str, object]:
    expected = frozen_stage_5_contract(root)
    if dict(payload) != expected:
        raise IntegrityError("all-Globex Stage 5 contract drifted")
    return dict(payload)


def frozen_stage_5_contract_v2(root: Path) -> dict[str, object]:
    """Preserve v1 and freeze diagnostic-only recent-regime reporting."""

    return _published(root, STAGE_5_V2_PATH)


def _obsolete_frozen_stage_5_contract_v2_builder(root: Path) -> dict[str, object]:
    """Retained source of the historical v2 payload; use its published artifact."""

    predecessor = frozen_stage_5_contract(root)
    core = deepcopy(predecessor)
    core.pop("contract_id")
    core.update({
        "schema_version": STAGE_5_V2_SCHEMA,
        "predecessor": _binding(root, STAGE_5_V1_PATH, str(predecessor["contract_id"])),
        "supersession": {
            "reason": "FREEZE_DEVELOPMENT_REGIME_DIAGNOSTIC_SLICES_BEFORE_ROW_ACCESS",
            "history_preserved": True,
            "wfa_period_or_fold_change": False,
            "scientific_reinterpretation": False,
        },
        "post_development": {
            "development_regime_stress_test": {
                "execution": "ONE_SHOT_UNCHANGED_MECHANISM",
                "official_trade_date_period": ["2023-01-01", "2025-07-14"],
                "period_boundary": "MARKET_TRADE_DATE_START_INCLUSIVE_END_EXCLUSIVE",
                "canonical_event_cutoff_exclusive": "2025-07-13T22:00:00Z",
                "official_pass_fail_scope": "ENTIRE_PERIOD_ONLY",
                "diagnostic_slices": [
                    {"label": "2023", "trade_date_period": ["2023-01-01", "2024-01-01"]},
                    {"label": "2024", "trade_date_period": ["2024-01-01", "2025-01-01"]},
                    {"label": "2025_TO_CUTOFF", "trade_date_period": ["2025-01-01", "2025-07-14"]},
                ],
                "diagnostic_slice_role": "REPORT_ONLY_NOT_GATES_NOT_SELECTION",
                "single_execution_produces_all_reports": True,
                "failure": "CLOSE_MECHANISM_NO_TUNING_OR_RETRY",
            },
            "refit": "PARAMETERS_ONLY_2018_TO_CUTOFF_NO_EVALUATION",
            "final_252": "SEALED_SEPARATE_APPROVAL",
            "forward": "MONITORING_ONLY_AFTER_ALL_GATES",
        },
    })
    return {**core, "contract_id": sha256_json(core)}


def frozen_stage_5_contract_v3(root: Path) -> dict[str, object]:
    """Return the immutable historical v3 mechanism."""

    return _published(root, STAGE_5_V3_PATH)


def _obsolete_frozen_stage_5_contract_v3_builder(root: Path) -> dict[str, object]:
    """Retained source of the historical v3 payload; use its published artifact."""

    predecessor = frozen_stage_5_contract_v2(root)
    core = deepcopy(predecessor)
    core.pop("contract_id")
    core.update({
        "schema_version": STAGE_5_V3_SCHEMA,
        "predecessor": _binding(root, STAGE_5_V2_PATH, str(predecessor["contract_id"])),
        "supersession": {
            "reason": "BIND_VERIFIED_PHASE_4_PROVENANCE_AND_CORRECTED_EXIT_OPEN_EXCURSIONS",
            "history_preserved": True,
            "target_family_changed": False,
            "sampling_alignment_changed": False,
            "scientific_reinterpretation": False,
        },
        "bindings": {
            "stage_2_5_contract_id": frozen_stage_2_5_contract(root)["contract_id"],
            "outcome_contract_id": frozen_outcome_foundation_contract()["contract_id"],
            "feature_contract_id": frozen_feature_foundation_contract()["contract_id"],
        },
    })
    core["scope"] = dict(core["scope"])
    core["scope"]["opportunities"] = "ALL_PREDECLARED_PLUS_22_MINUTE_ALIGNED_OPPORTUNITIES_NO_SUBSAMPLING"
    return {**core, "contract_id": sha256_json(core)}


def validate_stage_5_contract_v3(
    payload: Mapping[str, object], *, root: Path,
) -> dict[str, object]:
    expected = frozen_stage_5_contract_v3(root)
    if dict(payload) != expected:
        raise IntegrityError("all-Globex Stage 5 v3 contract drifted")
    return dict(payload)


def frozen_stage_5_contract_v4(root: Path) -> dict[str, object]:
    """Rebind the mechanism to signed-price foundations and feasible folds."""

    predecessor = frozen_stage_5_contract_v3(root)
    core = deepcopy(predecessor)
    core.pop("contract_id")
    core.update({
        "schema_version": STAGE_5_V4_SCHEMA,
        "predecessor": _binding(root, STAGE_5_V3_PATH, str(predecessor["contract_id"])),
        "supersession": {
            "reason": "SIGNED_PRICE_FEATURES_AND_ENFORCED_NESTED_DATE_MANIFEST",
            "history_preserved": True,
            "target_family_changed": False,
            "sampling_alignment_changed": False,
            "scientific_reinterpretation": False,
        },
        "bindings": {
            "stage_2_5_contract_id": frozen_stage_2_5_contract(root)["contract_id"],
            "outcome_contract_id": frozen_outcome_foundation_contract()["contract_id"],
            "feature_contract_id": frozen_feature_foundation_contract()["contract_id"],
        },
    })
    core["chronology"] = dict(core["chronology"])
    core["chronology"].update({
        "minimum_initial_fit_trade_dates": 505,
        "minimum_initial_inner_fit_trade_dates": 252,
        "fold_manifest_required": True,
    })
    return {**core, "contract_id": sha256_json(core)}


def validate_stage_5_contract_v4(payload: Mapping[str, object], *, root: Path) -> dict[str, object]:
    expected = frozen_stage_5_contract_v4(root)
    if dict(payload) != expected:
        raise IntegrityError("all-Globex Stage 5 v4 contract drifted")
    return dict(payload)


def validate_stage_5_contract_v2(
    payload: Mapping[str, object], *, root: Path,
) -> dict[str, object]:
    expected = frozen_stage_5_contract_v2(root)
    if dict(payload) != expected:
        raise IntegrityError("all-Globex Stage 5 v2 contract drifted")
    return dict(payload)


def global_trade_date_samples(
    rows: Sequence[Mapping[str, object]], *, required_markets: tuple[str, ...] = MARKETS,
) -> tuple[TemporalSamples, tuple[int, ...], tuple[str, ...]]:
    """Map complete common trade dates onto the existing chronological splitter."""

    if not rows or not required_markets or len(set(required_markets)) != len(required_markets):
        raise IntegrityError("global trade-date split scope is empty or duplicated")
    parsed: list[tuple[str, str, str, int]] = []
    for index, row in enumerate(rows):
        market = row.get("market")
        trade_date = row.get("trade_date")
        opportunity_id = row.get("opportunity_id")
        if (
            type(market) is not str or market not in required_markets
            or type(trade_date) is not str
            or type(opportunity_id) is not str or not opportunity_id
        ):
            raise IntegrityError("global trade-date row identity is invalid")
        try:
            date.fromisoformat(trade_date)
        except ValueError as exc:
            raise IntegrityError("global trade-date row date is invalid") from exc
        parsed.append((trade_date, market, opportunity_id, index))
    ordered = sorted(parsed)
    dates = tuple(sorted({item[0] for item in ordered}))
    for trade_date in dates:
        if {item[1] for item in ordered if item[0] == trade_date} != set(required_markets):
            raise IntegrityError("global split requires every market on every trade date")
    ordinals = {trade_date: index for index, trade_date in enumerate(dates)}
    sessions = np.asarray([ordinals[item[0]] for item in ordered], dtype=np.int64)
    samples = TemporalSamples(sessions, sessions, sessions + 1, sessions + 1)
    samples.validate()
    return samples, tuple(item[3] for item in ordered), dates


def _synthetic_row(start_ns: int, minute: int, market: str) -> dict[str, object]:
    tick = int(0.25 * NANO)
    base = (100 + MARKETS.index(market) * 10) * NANO
    open_nano = base + (minute // 10) * tick
    close_nano = open_nano + (tick if minute % 3 else -tick)
    row_id = sha256_json({"market": market, "bar_start_ns": start_ns})
    return {
        "row_id": row_id,
        "market": market,
        "source_contract_id": SYNTHETIC_SOURCE_CONTRACT_ID,
        "source_release_id": SYNTHETIC_SOURCE_RELEASE_ID,
        "source_file_sha256": sha256_json({"synthetic_file": market}),
        "source_row_sha256": sha256_json({"synthetic_row": row_id}),
        "definition_source_file_sha256": sha256_json({"synthetic_definition_file": market}),
        "definition_row_sha256": sha256_json({"synthetic_definition_row": market}),
        "source_cadence": "1m",
        "bar_start_ns": start_ns,
        "bar_end_ns": start_ns + 60 * NANO,
        "source_timestamp_ns": start_ns,
        "available_at_ns": start_ns + 65 * NANO,
        "decision_eligible_at_ns": start_ns + 65 * NANO,
        "open_nano": open_nano,
        "high_nano": max(open_nano, close_nano) + tick,
        "low_nano": min(open_nano, close_nano) - tick,
        "close_nano": close_nano,
        "volume": 100 + minute % 19,
        "actual_contract": f"{market}H2",
        "min_price_increment_nano": tick,
        "multiplier_nano": 50 * NANO,
        "project_trade_date": "2022-01-10",
        "official_schedule_state": "AUTHORITATIVE_APPLICABLE",
    }


def run_synthetic_stage_6(root: Path) -> dict[str, object]:
    """Exercise schedule, features, outcomes, and decision mechanics without history."""
    return _published(root, STAGE_6_V1_PATH)


def run_synthetic_stage_6_v2(root: Path) -> dict[str, object]:
    """Rebind the unchanged synthetic check to the Stage 5 v2 successor."""
    return _published(root, STAGE_6_V2_PATH)


def run_synthetic_stage_6_v3(root: Path) -> dict[str, object]:
    """Return the immutable historical v3 synthetic certificate."""

    return _published(root, STAGE_6_V3_PATH)


def run_synthetic_stage_6_v4(root: Path) -> dict[str, object]:
    """Exercise the signed-price successor without real history."""

    return _run_synthetic_stage_6(
        root, stage_5=frozen_stage_5_contract_v4(root), schema_version=STAGE_6_V4_SCHEMA,
    )


def _run_synthetic_stage_6(
    root: Path, *, stage_5: Mapping[str, object], schema_version: str,
) -> dict[str, object]:
    stage_2_5 = frozen_stage_2_5_contract(root)
    calendar_release_id = sha256_json({"source": "SYNTHETIC", "seed": 604})
    start = datetime(2022, 1, 9, 23, tzinfo=timezone.utc)
    session = CalendarSession(
        "ES", date(2022, 1, 10),
        (TradingStateInterval("OPEN", start, start + timedelta(hours=4)),),
    )
    records = []
    for market in MARKETS:
        rows = [
            _synthetic_row(int(start.timestamp() * NANO) + minute * 60 * NANO, minute, market)
            for minute in range(240)
        ]
        market_session = CalendarSession(market, session.trade_date, session.intervals)
        opportunities = synthetic_globex_hourly_opportunity_grid(
            market_session, calendar_release_id=calendar_release_id,
        )
        for opportunity in opportunities:
            feature_record = build_bound_feature_record(rows, opportunity)
            outcome = build_directional_path_outcome(
                rows, opportunity, expected_contract=f"{market}H2",
                as_of_ns=int(session.intervals[0].ends_at_utc.timestamp() * NANO),
            )
            if feature_record["audit"]["bar_count"] != 21 or outcome["status"] != "MATURED":
                raise IntegrityError("synthetic all-Globex chain did not mature")
            targets = outcome["targets"]
            assert isinstance(targets, Mapping)
            prediction = np.asarray([[
                targets["terminal_change_ticks"],
                targets["maximum_upside_excursion_ticks"],
                targets["maximum_downside_excursion_ticks"],
            ]], dtype=np.float64)
            tick_value_usd = (
                int(rows[0]["min_price_increment_nano"]) / NANO
                * int(rows[0]["multiplier_nano"]) / NANO
            )
            stress_cost = (4.0 if market in {"ES", "NQ"} else 8.0) + 10.0 / tick_value_usd
            direction = int(cost_adjusted_directions(
                prediction, np.asarray([stress_cost]),
            )[0])
            records.append({
                "market": market,
                "trade_date": opportunity["trade_date"],
                "opportunity_id": opportunity["opportunity_id"],
                "feature_record_id": feature_record["feature_record_id"],
                "outcome_id": outcome["outcome_id"],
                "synthetic_stress_cost_ticks": stress_cost,
                "synthetic_direction": direction,
            })
    global_trade_date_samples(records)
    core: dict[str, object] = {
        "schema_version": schema_version,
        "state": "PASS_SYNTHETIC_ENGINEERING_ONLY",
        "stage_2_5_contract_id": stage_2_5["contract_id"],
        "stage_5_contract_id": stage_5["contract_id"],
        "synthetic_calendar_release_id": calendar_release_id,
        "markets": list(MARKETS),
        "opportunities_checked": len(records),
        "first_and_last_opportunity_per_market_checked": True,
        "records_sha256": sha256_json(records),
        "authority": {
            "alpha_claim": False,
            "real_history": False,
            "holdout": False,
            "provider": False,
            "activation": False,
            "trading": False,
        },
    }
    return {**core, "certificate_id": sha256_json(core)}


def validate_published_contract(path: Path, payload: Mapping[str, object]) -> None:
    if path.read_bytes() != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"published contract is stale: {path.as_posix()}")
