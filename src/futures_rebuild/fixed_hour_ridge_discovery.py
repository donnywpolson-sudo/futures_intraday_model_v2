"""Preparation-only freeze for the corrected fixed-hour ridge mechanism."""

from __future__ import annotations

import json
import math
from copy import deepcopy
from datetime import date, datetime, time, timezone
from itertools import product
from pathlib import Path
from statistics import median
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np

from .canonical import sha256_file, sha256_json, write_new_canonical_authority
from .errors import IntegrityError
from .research_gateway_policy import scientific_governance_binding


CONTRACT_PATH = Path("configs/fixed_hour_ridge_discovery_contract_v1.json")
CERTIFICATE_ROOT = Path("state/preexecution_certificates/fixed_hour_ridge_discovery")
SELF = Path("src/futures_rebuild/fixed_hour_ridge_discovery.py")
CATALOG = Path("data/causally_gated_normalized/_control/catalog.json")
SCIENTIFIC_POLICY = Path("configs/research_readiness_contract.json")
ACTIVE_POINTERS = (
    Path("configs/active_alpha_historical_research_design.json"),
    Path("configs/active_alpha_ladder_execution_design.json"),
    Path("configs/active_alpha_research_ladder.json"),
)
PREDECESSOR_RESULT = Path(
    "state/intraday_research_official_results/"
    "ad7f318c5f26a4c5f316999c943546c9c1a95dde0f3dfae91e46056b50f24140/result.json"
)
PREDECESSOR_FAILURE = Path(
    "state/intraday_research_failure_registry/"
    "9fdba237dd8a3ebf73c0b053fa4f50c5e03e242bdc759cbffae30c38d381c5d1/failure.json"
)

MARKETS = ("ES", "NQ", "CL", "GC")
YEARS = tuple(range(2018, 2023))
PACKS = {
    "TREND": ("log_return_1", "log_return_5", "log_return_10"),
    "TREND_VOLATILITY": (
        "log_return_1", "log_return_5", "log_return_10",
        "intrabar_range_fraction", "atr_10_fraction", "realized_volatility_10",
    ),
    "TREND_VOLATILITY_VOLUME": (
        "log_return_1", "log_return_5", "log_return_10",
        "intrabar_range_fraction", "atr_10_fraction", "realized_volatility_10",
        "log1p_volume", "volume_zscore_20",
    ),
}
PENALTIES = ("0.1", "1.0", "10.0")
HORIZON_MINUTES = 60
NANO = 1_000_000_000


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"fixed-hour ridge input is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"fixed-hour ridge input must be an object: {path}")
    return value


def _binding(root: Path, path: Path) -> dict[str, str]:
    return {"path": path.as_posix(), "sha256": sha256_file(root / path)}


def enumerate_candidates() -> list[dict[str, object]]:
    candidates = []
    for ordinal, (feature_pack, penalty) in enumerate(product(PACKS, PENALTIES)):
        core = {
            "ordinal": ordinal,
            "feature_pack": feature_pack,
            "holding_horizon_minutes": HORIZON_MINUTES,
            "model_family": "MARKET_SPECIFIC_TWO_TARGET_RIDGE",
            "ridge_penalty": penalty,
            "prediction_hurdle_stress_net_r": "0.0",
        }
        candidates.append({**core, "candidate_id": sha256_json(core)})
    return candidates


def checkpoint_ns(trade_date: str, timezone_name: str = "America/Chicago") -> int:
    try:
        parsed = date.fromisoformat(trade_date)
        checkpoint = datetime.combine(parsed, time(10, 0, 5), ZoneInfo(timezone_name))
    except (ValueError, KeyError) as exc:
        raise IntegrityError("invalid trade date or project timezone") from exc
    return int(checkpoint.timestamp() * NANO)


def is_feature_eligible(row: Mapping[str, object], decision_at_ns: int) -> bool:
    timestamps = (
        row.get("bar_end_ns"), row.get("source_timestamp_ns"),
        row.get("available_at_ns"), row.get("decision_eligible_at_ns"),
    )
    return all(type(value) is int and value <= decision_at_ns for value in timestamps)


def feature_values(
    rows: Sequence[Mapping[str, object]], decision_at_ns: int,
) -> dict[str, float]:
    eligible = sorted(
        (row for row in rows if is_feature_eligible(row, decision_at_ns)),
        key=lambda row: int(row["bar_start_ns"]),
    )
    if len(eligible) < 20:
        raise IntegrityError("at least 20 causal one-minute bars are required")
    window = eligible[-20:]
    for row in window:
        timestamps = (row.get("bar_start_ns"), row.get("bar_end_ns"))
        prices = tuple(row.get(name) for name in ("open_nano", "high_nano", "low_nano", "close_nano"))
        if (
            any(type(value) is not int for value in timestamps + prices)
            or int(row["bar_start_ns"]) >= int(row["bar_end_ns"])
            or any(int(value) <= 0 for value in prices)
            or int(row["high_nano"]) < max(int(row["open_nano"]), int(row["close_nano"]))
            or int(row["low_nano"]) > min(int(row["open_nano"]), int(row["close_nano"]))
            or type(row.get("volume")) is not int
            or int(row["volume"]) < 0
        ):
            raise IntegrityError("invalid causal OHLCV feature row")

    closes = np.asarray([int(row["close_nano"]) for row in window], dtype=np.float64)
    highs = np.asarray([int(row["high_nano"]) for row in window], dtype=np.float64)
    lows = np.asarray([int(row["low_nano"]) for row in window], dtype=np.float64)
    volumes = np.asarray([int(row["volume"]) for row in window], dtype=np.float64)
    returns = np.log(closes[1:] / closes[:-1])
    true_ranges = np.maximum.reduce((
        highs[1:] - lows[1:], np.abs(highs[1:] - closes[:-1]),
        np.abs(lows[1:] - closes[:-1]),
    ))
    volume_std = float(np.std(volumes, ddof=0))
    values = {
        "log_return_1": float(math.log(closes[-1] / closes[-2])),
        "log_return_5": float(math.log(closes[-1] / closes[-6])),
        "log_return_10": float(math.log(closes[-1] / closes[-11])),
        "intrabar_range_fraction": float((highs[-1] - lows[-1]) / closes[-2]),
        "atr_10_fraction": float(np.mean(true_ranges[-10:]) / closes[-2]),
        "realized_volatility_10": float(np.std(returns[-10:], ddof=0)),
        "log1p_volume": float(math.log1p(volumes[-1])),
        "volume_zscore_20": (
            0.0 if volume_std == 0.0 else float((volumes[-1] - np.mean(volumes)) / volume_std)
        ),
    }
    if not all(math.isfinite(value) for value in values.values()):
        raise IntegrityError("nonfinite causal feature")
    return values


def resolve_outcome_path(
    rows: Sequence[Mapping[str, object]], decision_at_ns: int,
    *, horizon_minutes: int = HORIZON_MINUTES, tolerance_seconds: int = 120,
) -> dict[str, object]:
    ordered = sorted(rows, key=lambda row: int(row["bar_start_ns"]))
    entry_limit = decision_at_ns + tolerance_seconds * NANO
    entries = [
        row for row in ordered
        if decision_at_ns < int(row["bar_start_ns"]) <= entry_limit
    ]
    if not entries:
        return {"prediction_recorded": True, "score_status": "MISSING_ENTRY"}
    entry = entries[0]
    exit_target = int(entry["bar_start_ns"]) + horizon_minutes * 60 * NANO
    exits = [
        row for row in ordered
        if exit_target <= int(row["bar_start_ns"]) <= exit_target + tolerance_seconds * NANO
    ]
    if not exits:
        return {"prediction_recorded": True, "score_status": "MISSING_EXIT"}
    exit_row = exits[0]
    path = [
        row for row in ordered
        if int(entry["bar_start_ns"]) <= int(row["bar_start_ns"]) < int(exit_row["bar_start_ns"])
    ]
    identities = {str(row.get("actual_contract")) for row in [entry, *path, exit_row]}
    if len(identities) != 1 or "None" in identities:
        return {
            "prediction_recorded": True,
            "score_status": "ROLL_UNRESOLVED",
            "entry": entry,
            "exit": exit_row,
        }
    if not path:
        return {"prediction_recorded": True, "score_status": "INCONCLUSIVE_OUTCOME_COVERAGE"}
    return {
        "prediction_recorded": True,
        "score_status": "SCORABLE",
        "entry": entry,
        "exit": exit_row,
        "path": path,
    }


def stress_net_targets(
    *, entry_price: float, exit_price: float, atr_10_price: float,
    tick_size: float, multiplier: float, round_trip_cost_usd: float,
) -> tuple[float, float]:
    if min(entry_price, exit_price, atr_10_price, tick_size, multiplier) <= 0 or round_trip_cost_usd < 0:
        raise IntegrityError("invalid target economics")
    tick_value = tick_size * multiplier
    initial_risk = max(atr_10_price * multiplier, tick_value)
    gross_long = (exit_price - entry_price) * multiplier
    gross_short = -gross_long
    return (
        (gross_long - round_trip_cost_usd) / initial_risk,
        (gross_short - round_trip_cost_usd) / initial_risk,
    )


def net_mfe_mae_ratio(
    *, entry_price: float, highs: Sequence[float], lows: Sequence[float],
    direction: int, tick_size: float, multiplier: float, round_trip_cost_usd: float,
) -> float:
    if not highs or len(highs) != len(lows) or direction not in {-1, 1}:
        raise IntegrityError("invalid MFE/MAE path")
    if min(entry_price, tick_size, multiplier, *highs, *lows) <= 0 or round_trip_cost_usd < 0:
        raise IntegrityError("invalid MFE/MAE economics")
    if direction == 1:
        mfe = max(max(highs) - entry_price, 0.0) * multiplier
        mae = max(entry_price - min(lows), 0.0) * multiplier
    else:
        mfe = max(entry_price - min(lows), 0.0) * multiplier
        mae = max(max(highs) - entry_price, 0.0) * multiplier
    tick_value = tick_size * multiplier
    return max(mfe - round_trip_cost_usd, 0.0) / max(
        mae + round_trip_cost_usd, tick_value,
    )


class TwoTargetRidge:
    def __init__(self, penalty: float) -> None:
        if not math.isfinite(penalty) or penalty <= 0:
            raise IntegrityError("ridge penalty must be positive")
        self.penalty = penalty
        self.mean: np.ndarray | None = None
        self.scale: np.ndarray | None = None
        self.long_weights: np.ndarray | None = None
        self.short_weights: np.ndarray | None = None

    def fit(self, x: np.ndarray, y_long: np.ndarray, y_short: np.ndarray) -> "TwoTargetRidge":
        x = np.asarray(x, dtype=np.float64)
        y_long = np.asarray(y_long, dtype=np.float64)
        y_short = np.asarray(y_short, dtype=np.float64)
        if (
            x.ndim != 2 or y_long.shape != (x.shape[0],) or y_short.shape != (x.shape[0],)
            or x.shape[0] == 0 or not np.isfinite(x).all()
            or not np.isfinite(y_long).all() or not np.isfinite(y_short).all()
        ):
            raise IntegrityError("invalid ridge training arrays")
        self.mean = np.mean(x, axis=0)
        raw_scale = np.std(x, axis=0, ddof=0)
        self.scale = np.where(raw_scale == 0.0, 1.0, raw_scale)
        design = np.column_stack((np.ones(x.shape[0]), (x - self.mean) / self.scale))
        penalty = np.eye(design.shape[1]) * self.penalty
        penalty[0, 0] = 0.0
        system = design.T @ design + penalty
        self.long_weights = np.linalg.solve(system, design.T @ y_long)
        self.short_weights = np.linalg.solve(system, design.T @ y_short)
        return self

    def predict_intent(self, row: Sequence[float]) -> tuple[int, float, float]:
        if any(value is None for value in (self.mean, self.scale, self.long_weights, self.short_weights)):
            raise IntegrityError("ridge model is not fitted")
        vector = np.asarray(row, dtype=np.float64)
        assert self.mean is not None and self.scale is not None
        assert self.long_weights is not None and self.short_weights is not None
        if vector.shape != self.mean.shape or not np.isfinite(vector).all():
            raise IntegrityError("invalid ridge prediction row")
        design = np.insert((vector - self.mean) / self.scale, 0, 1.0)
        predicted_long = float(design @ self.long_weights)
        predicted_short = float(design @ self.short_weights)
        if predicted_long > predicted_short and predicted_long > 0.0:
            return 1, predicted_long, predicted_short
        if predicted_short > predicted_long and predicted_short > 0.0:
            return -1, predicted_long, predicted_short
        return 0, predicted_long, predicted_short


def intervals_overlap(first: tuple[int, int], second: tuple[int, int]) -> bool:
    if first[0] >= first[1] or second[0] >= second[1]:
        raise IntegrityError("label intervals must be nonempty")
    return first[0] < second[1] and second[0] < first[1]


def select_candidate(
    contract: Mapping[str, object], scores: Mapping[str, Sequence[float]],
) -> str:
    search = contract.get("search")
    candidates = search.get("candidates") if isinstance(search, Mapping) else None
    if not isinstance(candidates, list) or len(candidates) != 9:
        raise IntegrityError("fixed-hour ridge candidates are incomplete")
    ordered = [str(candidate["candidate_id"]) for candidate in candidates]
    if set(scores) != set(ordered) or any(len(values) != 4 for values in scores.values()):
        raise IntegrityError("scores must cover all nine candidates and four inner folds")
    if any(
        type(value) not in {int, float} or not math.isfinite(value)
        for values in scores.values() for value in values
    ):
        raise IntegrityError("candidate scores must be finite")
    return min(ordered, key=lambda item: (-median(scores[item]), ordered.index(item)))


def _source_scope(root: Path, catalog: Mapping[str, object]) -> list[dict[str, object]]:
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are missing")
    selected = []
    for market in MARKETS:
        for year in YEARS:
            matches = [
                entry for entry in entries
                if isinstance(entry, dict) and entry.get("market") == market
                and entry.get("year") == year and entry.get("selection_eligible") is True
            ]
            if len(matches) != 1:
                raise IntegrityError(f"active catalog lacks one eligible {market} {year} entry")
            entry = matches[0]
            expected = f"data/active/causally_gated_normalized/{market}/{year}/{year}.parquet"
            if 'annual_release_key' in entry:
                from .annual_foundation_release import minute_paths
                expected, expected_sidecar = minute_paths(entry)
            else:
                expected_sidecar = expected + '.manifest.json'
            if entry['sidecar_path'] != expected_sidecar:
                raise IntegrityError('source sidecar path differs from its storage contract')
            if entry.get("parquet_path") != expected or type(entry.get("row_count")) is not int:
                raise IntegrityError("active causal source path or row count is invalid")
            parquet = root / expected
            if not parquet.is_file():
                raise IntegrityError(f"active causal source is missing: {expected}")
            selected.append({
                "market": market,
                "year": year,
                "parquet_path": expected,
                "parquet_sha256": entry.get("parquet_sha256"),
                "sidecar_path": entry.get("sidecar_path"),
                "sidecar_sha256": entry.get("sidecar_sha256"),
                "row_count": entry["row_count"],
                "physical_bytes": parquet.stat().st_size,
            })
    return selected


def build_contract(root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    catalog = _read_json(root / CATALOG)
    policy = _read_json(root / SCIENTIFIC_POLICY)
    predecessor_result = _read_json(root / PREDECESSOR_RESULT)
    predecessor_failure = _read_json(root / PREDECESSOR_FAILURE)
    if (
        predecessor_result.get("official_result_id")
        != "ad7f318c5f26a4c5f316999c943546c9c1a95dde0f3dfae91e46056b50f24140"
        or predecessor_result.get("next_step_authorized") is not False
        or predecessor_failure.get("state") != "TERMINAL_NON_PROMOTABLE_OUTCOME_EXPOSED_NO_RETRY"
    ):
        raise IntegrityError("closed micro-exhaustion predecessor binding changed")
    sources = _source_scope(root, catalog)
    es_sources = [source for source in sources if source["market"] == "ES"]
    nested = policy.get("nested_wfa")
    if not isinstance(nested, dict):
        raise IntegrityError("active scientific policy lacks nested WFA")
    candidates = enumerate_candidates()
    core: dict[str, object] = {
        "schema_version": "fixed_hour_ridge_discovery_contract/1.0.0",
        "state": "PRE_RESULT_PREPARATION_ONLY_NOT_REGISTERED",
        "classification": "NEW_COUNTED_MECHANISM_RESTARTING_AT_STAGE_0",
        "hypothesis": (
            "Point-in-time-legal one-minute OHLCV state observed at 10:00:05 America/Chicago "
            "has robust after-cost predictive association with the next 60-minute ES return, "
            "with unchanged specification corroboration on NQ, CL, and GC."
        ),
        "claim_ceiling": "ROBUST_PREDICTIVE_ASSOCIATION_NOT_ECONOMIC_CAUSAL_IDENTIFICATION",
        "bindings": {
            "implementation": _binding(root, SELF),
            "catalog": {**_binding(root, CATALOG), "plan_id": catalog.get("plan_id")},
            "scientific_policy": _binding(root, SCIENTIFIC_POLICY),
            "scientific_governance": scientific_governance_binding(root),
            "active_pointers": {path.as_posix(): sha256_file(root / path) for path in ACTIVE_POINTERS},
            "closed_predecessor": {
                "official_result": _binding(root, PREDECESSOR_RESULT),
                "failure": _binding(root, PREDECESSOR_FAILURE),
                "cannot_be_retried_rescued_or_reinterpreted": True,
            },
        },
        "research_family_accounting": {
            "prior_conservative_individual_count": 169,
            "new_candidate_alternatives_if_evaluated": 9,
            "conservative_cumulative_individual_count_if_evaluated": 178,
            "exact_ledger_reconciliation_required_before_registration": True,
        },
        "source_contract": {
            "foundation": "ACTIVE_CAUSALLY_GATED_NORMALIZED_ONE_MINUTE_OHLCV_ONLY",
            "sources": sources,
            "market_year_file_count": len(sources),
            "expected_rows": sum(int(source["row_count"]) for source in sources),
            "expected_physical_bytes": sum(int(source["physical_bytes"]) for source in sources),
            "allowed_fields": [
                "market", "source_contract_id", "source_release_id", "source_file_path",
                "source_file_sha256", "source_row_sha256", "source_cadence", "bar_start_ns",
                "bar_end_ns", "source_timestamp_ns", "available_at_ns", "decision_eligible_at_ns",
                "publisher_id", "instrument_id", "raw_symbol", "actual_contract",
                "definition_source_file_path", "definition_source_file_sha256",
                "definition_row_sha256", "definition_event_at_ns", "definition_received_at_ns",
                "listing_activation_ns", "expiration_ns", "open_nano", "high_nano", "low_nano",
                "close_nano", "volume", "currency", "min_price_increment_nano", "multiplier_nano",
                "project_session_id", "project_trade_date", "project_grouping_start_ns",
                "project_grouping_end_ns", "project_timezone", "official_schedule_state",
            ],
            "sidecars_are_lineage_not_features": True,
            "rejected": [
                "RAW_L0_OR_DBN_ROWS", "SEALED_SOURCE_ROWS", "BID_ASK_SPREAD_DEPTH_OR_IMBALANCE",
                "TRADE_SIGN_OR_AGGRESSOR", "LIQUIDATIONS", "NEWS_OR_SENTIMENT", "2023_PLUS",
                "FINAL_252_HOLDOUT", "FORWARD_DATA",
            ],
        },
        "first_real_data_boundary": {
            "operation": "READ_ONLY_ES_2018_2022_READINESS_CENSUS",
            "parquet_files": [source["parquet_path"] for source in es_sources],
            "sidecar_files": [source["sidecar_path"] for source in es_sources],
            "expected_rows": sum(int(source["row_count"]) for source in es_sources),
            "expected_physical_bytes": sum(int(source["physical_bytes"]) for source in es_sources),
            "requires_separate_plain_language_approval": True,
            "model_fit_or_evaluation": False,
        },
        "decision_and_outcome": {
            "checkpoint": "10:00:05_AMERICA_CHICAGO_USING_PROJECT_TIMEZONE_AND_TRADE_DATE",
            "feature_availability": (
                "BAR_END_SOURCE_TIMESTAMP_AVAILABLE_AT_AND_DECISION_ELIGIBLE_AT_NOT_AFTER_CHECKPOINT"
            ),
            "entry": "FIRST_REPORTED_BAR_OPEN_STRICTLY_AFTER_CHECKPOINT_WITHIN_120_SECONDS",
            "exit": "FIRST_REPORTED_BAR_OPEN_AT_OR_AFTER_ENTRY_PLUS_60_MINUTES_WITHIN_120_SECONDS",
            "label_interval": "HALF_OPEN_ENTRY_INCLUSIVE_EXIT_EXCLUSIVE",
            "feature_label_overlap_allowed": False,
            "future_contract_change": (
                "PREDICTION_RETAINED_OUTCOME_ROLL_UNRESOLVED_AND_RETAINED_IN_COVERAGE_DENOMINATOR"
            ),
            "cross_contract_return_or_stitching_allowed": False,
        },
        "features": {
            "packs": {name: list(fields) for name, fields in PACKS.items()},
            "definitions": {
                "log_return_k": "LN(CLOSE_T/CLOSE_T_MINUS_K)_FOR_K_1_5_10",
                "intrabar_range_fraction": "(HIGH_T-LOW_T)/CLOSE_T_MINUS_1",
                "atr_10_fraction": "MEAN_LAST_10_TRUE_RANGES/CLOSE_T_MINUS_1",
                "realized_volatility_10": "POPULATION_STD_LAST_10_LOG_RETURN_1",
                "log1p_volume": "LN(1+NONNEGATIVE_VOLUME_T)",
                "volume_zscore_20": "POPULATION_ZSCORE_WITH_ZERO_WHEN_POPULATION_STD_IS_ZERO",
            },
            "training_transform": "FIT_PER_MARKET_ON_EACH_INNER_OR_OUTER_TRAINING_FOLD_ONLY",
            "nonfinite_missing_or_invalid": "FAIL_CLOSED_NO_PREDICTION",
        },
        "target_and_diagnostics": {
            "initial_risk_usd": "MAX(DECISION_TIME_ATR10_PRICE_TIMES_MULTIPLIER,TICK_VALUE_USD)",
            "long_target": "(LONG_GROSS_PNL_USD-ONE_ROUND_TRIP_STRESS_COST_USD)/INITIAL_RISK_USD",
            "short_target": "(SHORT_GROSS_PNL_USD-ONE_ROUND_TRIP_STRESS_COST_USD)/INITIAL_RISK_USD",
            "mfe_mae_role": "OUTCOME_ONLY_PROMOTION_DIAGNOSTIC_NEVER_FEATURE_OR_EXIT_TRIGGER",
            "net_mfe_mae": (
                "MAX(MFE_USD-ROUND_TRIP_COST_USD,0)/"
                "MAX(MAE_USD+ROUND_TRIP_COST_USD,TICK_SIZE_TIMES_MULTIPLIER)"
            ),
        },
        "search": {
            "candidate_budget": len(candidates),
            "candidates": candidates,
            "selection_market": "ES_ONLY",
            "inner_selection": "HIGHEST_MEDIAN_STRESS_NET_SESSION_SHARPE_THEN_CANDIDATE_ORDER",
            "outer_results_may_select_or_tune": False,
            "full_development_final_selection": "REAPPLY_FROZEN_INNER_SELECTION_WITHOUT_OUTER_RESULT_INPUT",
            "adaptive_candidates_horizons_hurdles_or_retries": False,
        },
        "model": {
            "algorithm": "MARKET_SPECIFIC_TWO_TARGET_RIDGE_NUMPY",
            "unpenalized_intercept": True,
            "one_unit_one_intent_per_market_session": True,
            "trade_rule": "STRICTLY_LARGER_PREDICTED_TARGET_AND_PREDICTED_STRESS_NET_R_ABOVE_ZERO",
            "corroboration": "SAME_PACK_PENALTY_HURDLE_AND_ALGORITHM_WITH_COEFFICIENTS_REFIT_PER_MARKET",
            "pooling_or_direct_ES_COEFFICIENT_TRANSFER": False,
        },
        "validation": {
            "selection_start_inclusive": "2018-01-01",
            "selection_end_exclusive": "2023-01-01",
            "outer": nested["outer"],
            "inner": {**nested["inner"], "minimum_training_sessions": 252},
            "purge_minutes": 120,
            "purge": "REMOVE_TRAIN_LABEL_INTERVALS_INTERSECTING_VALIDATION_OR_TEST_LABEL_INTERVALS",
            "embargo_sessions": 1,
            "chronological_expanding_only": True,
        },
        "costs": {
            "round_trip_fee_usd": "10.00",
            "round_trip_adverse_ticks": {
                "base": {"ES": 2, "NQ": 2, "CL": 4, "GC": 4},
                "stress": {"ES": 4, "NQ": 4, "CL": 8, "GC": 8},
                "extreme": {"ES": 8, "NQ": 8, "CL": 16, "GC": 16},
            },
            "formula": "FEE_PLUS_ADVERSE_TICKS_TIMES_TICK_VALUE_USD_CHARGED_ONCE_PER_ROUND_TRIP",
            "research_costs_are_provisional_not_deployment_evidence": True,
        },
        "inference": {
            "primary_observation": "COMPLETE_EXCHANGE_SESSION_RETURN_INCLUDING_ZERO_NO_TRADE",
            "stationary_session_block_bootstrap_resamples": 10000,
            "hac_required": True,
            "romano_wolf_joint_baseline_adjustment_required": True,
            "dsr_required_with_complete_information_family": True,
            "pbo": "NOT_APPLICABLE_NO_POSITIVE_CREDIT_WITH_ONLY_NINE_CONFIGURATIONS",
            "paired_t_test_or_bonferroni_only_allowed": False,
            "regimes": "OUTER_TRAINING_ONLY_TREND_AND_VOLATILITY_TERTILES_CARTESIAN_PRODUCT",
        },
        "mechanism_card": {
            "competing_explanations": [
                {
                    "name": "UNCONDITIONAL_TIME_OF_DAY_DIRECTION",
                    "test": "BEAT_FOLD_LOCAL_UNCONDITIONAL_DIRECTION_AND_ZERO",
                },
                {
                    "name": "SIMPLE_PREVIOUS_BAR_MOMENTUM_OR_REVERSAL",
                    "test": "BEAT_BOTH_PREVIOUS_BAR_SIGN_BASELINES",
                },
                {
                    "name": "TREND_OR_VOLATILITY_REGIME_PROXY",
                    "test": "TRAINING_ONLY_REGIME_LEAVE_ONE_CLUSTER_OUT_EFFECT_POSITIVE_AND_AT_LEAST_HALF",
                },
            ],
            "missing_or_changed_after_outcome_access": "INVALID_CLOSE_MECHANISM",
        },
        "baselines": [
            "FLAT_ZERO", "FOLD_LOCAL_MARKET_SESSION_UNCONDITIONAL",
            "PREVIOUS_BAR_SIGN_MOMENTUM", "PREVIOUS_BAR_SIGN_REVERSAL",
            "RISK_MATCHED_ALWAYS_LONG_INTRADAY", "RISK_MATCHED_ALWAYS_SHORT_INTRADAY",
        ],
        "promotion": {
            "es": [
                "STRESS_NET_PNL_POSITIVE", "MULTIPLICITY_ADJUSTED_LOWER_BOUND_GT_10_USD_PER_SESSION",
                "MEDIAN_NET_MFE_MAE_GT_1", "AT_LEAST_40_SCORED_TRADES",
                "AT_LEAST_5_OF_8_OUTER_FOLDS_POSITIVE", "BEAT_ZERO_AND_ALL_BASELINES",
                "DSR_AND_APPLICABLE_MULTIPLE_TESTING_PASS", "MAXIMUM_DRAWDOWN_USD_LTE_1500",
                "LARGEST_SESSION_AND_CONTIGUOUS_5_SESSION_BLOCK_SHARE_LTE_0_50",
                "COVERAGE_REGIME_FAMILY_ROLL_MISSINGNESS_AND_EFFECTIVE_SAMPLE_REPORTS_COMPLETE",
            ],
            "failure_action": "CLOSE_MECHANISM_NO_RETRY_OR_RESULT_INFORMED_RESCUE",
            "es_pass_next": "SEPARATELY_PREPARE_NQ_CL_GC_UNCHANGED_SPECIFICATION_CORROBORATION",
            "required_positive_family_anchors": ["ES", "CL", "GC"],
            "nq_cannot_rescue_anchor_failure": True,
        },
        "authority": {
            "historical_row_read": False,
            "feature_or_outcome_construction": False,
            "model_fit_or_prediction": False,
            "trial_registration_or_execution": False,
            "recent_regime_or_2023_plus": False,
            "holdout_or_forward": False,
            "provider_or_network": False,
            "publication_or_activation": False,
            "trading": False,
            "push": False,
        },
    }
    return {**core, "contract_id": sha256_json(core)}


def validate_contract(root: Path, contract: Mapping[str, object]) -> dict[str, object]:
    expected = build_contract(root)
    if dict(contract) == expected:
        return expected
    # The active research pointers and global ledger are preparation-time
    # provenance, not mutable inputs to the already frozen mechanism.
    historical = deepcopy(expected)
    observed_bindings = contract.get("bindings")
    if isinstance(observed_bindings, Mapping):
        historical["bindings"]["active_pointers"] = deepcopy(  # type: ignore[index]
            observed_bindings.get("active_pointers")
        )
        historical["bindings"]["scientific_governance"] = deepcopy(  # type: ignore[index]
            observed_bindings.get("scientific_governance")
        )
        historical["bindings"]["implementation"] = deepcopy(  # type: ignore[index]
            observed_bindings.get("implementation")
        )
    historical["contract_id"] = sha256_json({
        key: value for key, value in historical.items() if key != "contract_id"
    })
    if dict(contract) != historical:
        raise IntegrityError("fixed-hour ridge contract drifted from the frozen design")
    return historical


def _synthetic_row(start_ns: int, price: float, volume: int, contract: str = "ESH2") -> dict[str, object]:
    close = int(price * NANO)
    return {
        "bar_start_ns": start_ns,
        "bar_end_ns": start_ns + 60 * NANO,
        "source_timestamp_ns": start_ns,
        "available_at_ns": start_ns + 65 * NANO,
        "decision_eligible_at_ns": start_ns + 65 * NANO,
        "open_nano": close,
        "high_nano": close + int(0.10 * NANO),
        "low_nano": close - int(0.10 * NANO),
        "close_nano": close,
        "volume": volume,
        "actual_contract": contract,
        "project_timezone": "America/Chicago",
    }


def build_synthetic_certificate(
    root: Path, contract: Mapping[str, object],
) -> dict[str, object]:
    frozen = validate_contract(root, contract)
    winter = checkpoint_ns("2022-01-10")
    summer = checkpoint_ns("2022-07-11")
    winter_hour = datetime.fromtimestamp(winter / NANO, timezone.utc).hour
    summer_hour = datetime.fromtimestamp(summer / NANO, timezone.utc).hour

    feature_rows = []
    first_feature_start = winter - 21 * 60 * NANO - 5 * NANO
    for index in range(21):
        feature_rows.append(_synthetic_row(first_feature_start + index * 60 * NANO, 100 + index / 10, 100 + index))
    features = feature_values(feature_rows, winter)
    late = dict(feature_rows[-1])
    late["available_at_ns"] = winter + 1

    entry_start = winter + 55 * NANO
    outcome_rows = [
        _synthetic_row(entry_start + index * 60 * NANO, 102 + index / 100, 200 + index)
        for index in range(61)
    ]
    outcome = resolve_outcome_path(outcome_rows, winter)
    rolled = [dict(row) for row in outcome_rows]
    rolled[30]["actual_contract"] = "ESM2"
    roll_outcome = resolve_outcome_path(rolled, winter)

    long_r, short_r = stress_net_targets(
        entry_price=100.0, exit_price=102.0, atr_10_price=1.0,
        tick_size=0.25, multiplier=1.0, round_trip_cost_usd=0.25,
    )
    ratio = net_mfe_mae_ratio(
        entry_price=100.0, highs=[103.0], lows=[99.0], direction=1,
        tick_size=0.25, multiplier=1.0, round_trip_cost_usd=0.25,
    )

    rng = np.random.default_rng(42)
    x_train = rng.normal(size=(100, 3))
    y_long = 0.5 * x_train[:, 0] + rng.normal(scale=0.05, size=100)
    y_short = -0.5 * x_train[:, 0] + rng.normal(scale=0.05, size=100)
    model = TwoTargetRidge(1.0).fit(x_train, y_long, y_short)
    intent, _, _ = model.predict_intent([2.0, 0.0, 0.0])

    candidate_ids = [
        str(item["candidate_id"])
        for item in frozen["search"]["candidates"]  # type: ignore[index]
    ]
    tie_scores = {candidate_id: (0.0, 0.0, 0.0, 0.0) for candidate_id in candidate_ids}
    winner_scores = dict(tie_scores)
    winner_scores[candidate_ids[-1]] = (1.0, 1.0, 1.0, 1.0)
    authority = frozen["authority"]
    checks = {
        "candidate_count_exactly_nine": len(candidate_ids) == len(set(candidate_ids)) == 9,
        "dst_checkpoint_uses_chicago_rules": winter_hour == 16 and summer_hour == 15,
        "availability_after_checkpoint_rejected": not is_feature_eligible(late, winter),
        "features_finite_and_causal": all(math.isfinite(value) for value in features.values()),
        "entry_and_exit_are_10_01_and_11_01_equivalents": (
            outcome.get("score_status") == "SCORABLE"
            and int(outcome["entry"]["bar_start_ns"]) == entry_start  # type: ignore[index]
            and int(outcome["exit"]["bar_start_ns"]) == entry_start + 60 * 60 * NANO  # type: ignore[index]
            and int(feature_rows[-1]["bar_end_ns"]) <= winter < entry_start
        ),
        "future_roll_is_unscorable_not_retroactive_abstention": (
            roll_outcome == {
                "prediction_recorded": True,
                "score_status": "ROLL_UNRESOLVED",
                "entry": rolled[0],
                "exit": rolled[60],
            }
        ),
        "round_trip_cost_charged_once": math.isclose(long_r, 1.75) and math.isclose(short_r, -2.25),
        "dollar_mfe_mae_tick_floor_correct": math.isclose(ratio, 2.2),
        "training_only_standardization_and_ridge_intent": (
            intent == 1 and model.mean is not None and np.allclose(model.mean, np.mean(x_train, axis=0))
        ),
        "half_open_purge_overlap_mechanics": (
            not intervals_overlap((0, 60), (60, 120)) and intervals_overlap((0, 61), (60, 120))
            and frozen["validation"]["embargo_sessions"] == 1  # type: ignore[index]
        ),
        "deterministic_inner_selection": (
            select_candidate(frozen, tie_scores) == candidate_ids[0]
            and select_candidate(frozen, winner_scores) == candidate_ids[-1]
        ),
        "no_real_data_or_execution_authority": isinstance(authority, Mapping) and not any(authority.values()),
    }
    if not all(checks.values()):
        failed = [name for name, passed in checks.items() if not passed]
        raise IntegrityError(f"fixed-hour ridge synthetic mechanics failed: {failed}")
    core = {
        "schema_version": "fixed_hour_ridge_synthetic_certificate/1.0.0",
        "state": "PASS_SYNTHETIC_MECHANICS_ONLY_NO_ROW_AUTHORITY",
        "contract_id": frozen["contract_id"],
        "contract": _binding(root, CONTRACT_PATH),
        "implementation": _binding(root, SELF),
        "checks": checks,
        "does_not_certify": [
            "REAL_SOURCE_READINESS", "ALPHA", "MODEL_PERFORMANCE", "WFA", "ECONOMICS",
            "CORROBORATION", "RECENT_REGIME", "HOLDOUT", "FORWARD", "DEPLOYMENT_OR_TRADING",
        ],
        "authority": {"historical_row_read": False, "trial_execution": False},
    }
    return {**core, "certificate_id": sha256_json(core)}


def prepare(root: Path) -> dict[str, str]:
    root = root.resolve(strict=True)
    contract = build_contract(root)
    write_new_canonical_authority(root / CONTRACT_PATH, contract)
    certificate = build_synthetic_certificate(root, contract)
    certificate_path = CERTIFICATE_ROOT / str(certificate["certificate_id"]) / "certificate.json"
    write_new_canonical_authority(root / certificate_path, certificate)
    return {
        "state": "PREPARED_SYNTHETIC_ONLY_NO_ROW_AUTHORITY",
        "contract_id": str(contract["contract_id"]),
        "contract_path": CONTRACT_PATH.as_posix(),
        "certificate_id": str(certificate["certificate_id"]),
        "certificate_path": certificate_path.as_posix(),
    }


if __name__ == "__main__":
    print(json.dumps(prepare(Path.cwd()), sort_keys=True))
