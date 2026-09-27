"""Synthetic-only preparation for a bounded multi-family OHLCV mechanism."""

from __future__ import annotations

import math
from statistics import median
from typing import Mapping, Sequence

import numpy as np

from .canonical import sha256_json
from .errors import IntegrityError
from .fixed_hour_ridge_audit_successor import strict_outcome_path, validate_fixed_hour_rows
from .fixed_hour_ridge_discovery import NANO, is_feature_eligible, net_mfe_mae_ratio


MINUTE = 60 * NANO
FEATURE_BARS = 21
PENALTIES = (0.1, 1.0, 10.0)
INNER_FOLDS = 4
INNER_MINIMUM_TRADES = 12
PILOT_MINIMUM_TRADES = 20

FEATURE_FAMILIES = {
    "PRICE_PATH": (
        "close_change_ticks_1", "close_change_ticks_5", "close_change_ticks_10",
        "close_change_ticks_20",
        "signed_trend_efficiency_20",
    ),
    "VOLATILITY": (
        "close_change_volatility_ticks_5", "close_change_volatility_ticks_20",
        "atr_10_ticks",
        "volatility_ratio_5_20", "range_compression_5_20",
    ),
    "BAR_GEOMETRY": (
        "signed_body_fraction", "close_location_in_bar", "close_location_in_range_20",
    ),
    "VOLUME": (
        "log1p_volume", "volume_ratio_prior20_median", "recent_volume_share_5_20",
    ),
    "PRICE_VOLUME_INTERACTION": (
        "price_direction_weighted_volume_20", "return_log_volume_correlation_20",
    ),
}
FEATURE_NAMES = tuple(name for names in FEATURE_FAMILIES.values() for name in names)
FEATURE_FORMULAS = {
    "close_change_ticks_1": "(CLOSE_T-CLOSE_T_MINUS_1)/TICK_SIZE",
    "close_change_ticks_5": "(CLOSE_T-CLOSE_T_MINUS_5)/TICK_SIZE",
    "close_change_ticks_10": "(CLOSE_T-CLOSE_T_MINUS_10)/TICK_SIZE",
    "close_change_ticks_20": "(CLOSE_T-CLOSE_T_MINUS_20)/TICK_SIZE",
    "signed_trend_efficiency_20": "(CLOSE_T-CLOSE_T_MINUS_20)/SUM_20_ABS_CLOSE_CHANGES; ZERO_IF_DENOMINATOR_ZERO",
    "close_change_volatility_ticks_5": "POPULATION_STD_LAST_5_ONE_MINUTE_CLOSE_CHANGES_IN_TICKS",
    "close_change_volatility_ticks_20": "POPULATION_STD_LAST_20_ONE_MINUTE_CLOSE_CHANGES_IN_TICKS",
    "atr_10_ticks": "MEAN_LAST_10_TRUE_RANGES/TICK_SIZE",
    "volatility_ratio_5_20": "RV_TICKS_5/RV_TICKS_20; ZERO_IF_RV20_ZERO",
    "range_compression_5_20": "MEAN_LAST_5_TRUE_RANGES/MAX(MEAN_20_TRUE_RANGES,TICK_SIZE)",
    "signed_body_fraction": "(CLOSE_T-OPEN_T)/MAX(HIGH_T-LOW_T,TICK_SIZE)",
    "close_location_in_bar": "2*(CLOSE_T-LOW_T)/(HIGH_T-LOW_T)-1; ZERO_IF_RANGE_ZERO",
    "close_location_in_range_20": "2*(CLOSE_T-MIN_LOW_21)/(MAX_HIGH_21-MIN_LOW_21)-1; ZERO_IF_RANGE_ZERO",
    "log1p_volume": "LN(1+VOLUME_T)",
    "volume_ratio_prior20_median": "(1+VOLUME_T)/(1+MEDIAN_PRIOR_20_VOLUME)-1",
    "recent_volume_share_5_20": "SUM_LAST_5_VOLUME/SUM_LAST_20_VOLUME; ZERO_IF_DENOMINATOR_ZERO",
    "price_direction_weighted_volume_20": "SUM(SIGN(ONE_MINUTE_CLOSE_CHANGE_TICKS)*VOLUME)/SUM(VOLUME); PRICE_DIRECTION_PROXY_NOT_AGGRESSOR_FLOW",
    "return_log_volume_correlation_20": "POPULATION_CORRELATION(20_ONE_MINUTE_CLOSE_CHANGES_IN_TICKS,LOG1P_MATCHED_VOLUME); ZERO_IF_INVARIANT",
}


def _strict_window(
    rows: Sequence[Mapping[str, object]], decision_at_ns: int,
    *,
    expected_market: str | None = None,
    expected_trade_date: str | None = None,
    expected_source_contract_id: str | None = None,
    expected_source_release_id: str | None = None,
) -> tuple[list[Mapping[str, object]], dict[str, object]]:
    eligible = sorted(
        (row for row in rows if is_feature_eligible(row, decision_at_ns)),
        key=lambda row: int(row["bar_start_ns"]),
    )
    if len(eligible) < FEATURE_BARS:
        raise IntegrityError("multi-family feature window needs 21 eligible bars")
    window = eligible[-FEATURE_BARS:]
    starts = [int(row["bar_start_ns"]) for row in window]
    ends = [int(row["bar_end_ns"]) for row in window]
    if (
        any(end - start != MINUTE for start, end in zip(starts, ends, strict=True))
        or any(right != left + MINUTE for left, right in zip(starts, starts[1:]))
        or ends[-1] != decision_at_ns - 5 * NANO
    ):
        raise IntegrityError("multi-family feature window is not the exact consecutive predecision interval")

    required_ints = (
        "open_nano", "high_nano", "low_nano", "close_nano", "volume",
        "min_price_increment_nano", "multiplier_nano",
    )
    for row in window:
        if any(type(row.get(name)) is not int for name in required_ints):
            raise IntegrityError("multi-family feature row has invalid types")
        start = int(row["bar_start_ns"])
        end = int(row["bar_end_ns"])
        open_, high, low, close = (int(row[name]) for name in required_ints[:4])
        if (
            high < max(open_, close)
            or low > min(open_, close) or high < low or int(row["volume"]) < 0
            or int(row["min_price_increment_nano"]) <= 0
            or int(row["multiplier_nano"]) <= 0
            or any(value % int(row["min_price_increment_nano"]) for value in (open_, high, low, close))
            or int(row["source_timestamp_ns"]) != start
            or not end <= int(row["available_at_ns"]) <= int(row["decision_eligible_at_ns"]) <= decision_at_ns
        ):
            raise IntegrityError("multi-family feature row has invalid OHLCV, timing, or economics")

    contracts = {row.get("actual_contract") for row in window}
    ticks = {int(row["min_price_increment_nano"]) for row in window}
    multipliers = {int(row["multiplier_nano"]) for row in window}
    if (
        len(contracts) != 1 or not isinstance(next(iter(contracts)), str)
        or not next(iter(contracts)) or len(ticks) != 1 or len(multipliers) != 1
    ):
        raise IntegrityError("multi-family feature window crosses contract or economics identity")
    if expected_market is not None:
        validate_fixed_hour_rows(
            window,
            expected_market=expected_market,
            expected_trade_date=expected_trade_date,
            expected_source_contract_id=expected_source_contract_id,
            expected_source_release_id=expected_source_release_id,
            expected_contract=str(next(iter(contracts))),
        )
    audit: dict[str, object] = {
        "actual_contract": next(iter(contracts)),
        "first_bar_start_ns": starts[0],
        "last_bar_end_ns": ends[-1],
        "bar_count": FEATURE_BARS,
        "min_price_increment_nano": next(iter(ticks)),
        "multiplier_nano": next(iter(multipliers)),
    }
    if expected_market is not None:
        audit["source_row_ids"] = [str(row["row_id"]) for row in window]
        audit["source_row_sha256s"] = [str(row["source_row_sha256"]) for row in window]
    return window, audit


def multifamily_feature_values(
    rows: Sequence[Mapping[str, object]], decision_at_ns: int,
    *,
    expected_market: str | None = None,
    expected_trade_date: str | None = None,
    expected_source_contract_id: str | None = None,
    expected_source_release_id: str | None = None,
) -> tuple[dict[str, float], dict[str, object]]:
    """Compute the complete frozen library; partial packs are not candidates."""

    window, audit = _strict_window(
        rows, decision_at_ns,
        expected_market=expected_market,
        expected_trade_date=expected_trade_date,
        expected_source_contract_id=expected_source_contract_id,
        expected_source_release_id=expected_source_release_id,
    )
    opens = np.asarray([int(row["open_nano"]) for row in window], dtype=np.float64)
    highs = np.asarray([int(row["high_nano"]) for row in window], dtype=np.float64)
    lows = np.asarray([int(row["low_nano"]) for row in window], dtype=np.float64)
    closes = np.asarray([int(row["close_nano"]) for row in window], dtype=np.float64)
    volumes = np.asarray([int(row["volume"]) for row in window], dtype=np.float64)
    tick = float(audit["min_price_increment_nano"])
    changes = np.diff(closes)
    tick_changes = changes / tick
    true_ranges = np.maximum.reduce((
        highs[1:] - lows[1:], np.abs(highs[1:] - closes[:-1]),
        np.abs(lows[1:] - closes[:-1]),
    ))
    rv5 = float(np.std(tick_changes[-5:], ddof=0))
    rv20 = float(np.std(tick_changes, ddof=0))
    bar_range = highs[-1] - lows[-1]
    window_range = max(highs) - min(lows)
    prior_volume_median = float(np.median(volumes[:-1]))
    recent_volume = float(np.sum(volumes[-20:]))
    matched_volume = volumes[1:]
    matched_total = float(np.sum(matched_volume))
    log_volume = np.log1p(matched_volume)
    correlation = 0.0
    if rv20 > 0 and np.std(log_volume, ddof=0) > 0:
        correlation = float(np.corrcoef(tick_changes, log_volume)[0, 1])

    values = {
        "close_change_ticks_1": float((closes[-1] - closes[-2]) / tick),
        "close_change_ticks_5": float((closes[-1] - closes[-6]) / tick),
        "close_change_ticks_10": float((closes[-1] - closes[-11]) / tick),
        "close_change_ticks_20": float((closes[-1] - closes[0]) / tick),
        "signed_trend_efficiency_20": 0.0 if np.sum(np.abs(changes)) == 0 else float(np.sum(changes) / np.sum(np.abs(changes))),
        "close_change_volatility_ticks_5": rv5,
        "close_change_volatility_ticks_20": rv20,
        "atr_10_ticks": float(np.mean(true_ranges[-10:]) / tick),
        "volatility_ratio_5_20": 0.0 if rv20 == 0 else rv5 / rv20,
        "range_compression_5_20": float(np.mean(true_ranges[-5:]) / max(float(np.mean(true_ranges)), tick)),
        "signed_body_fraction": float((closes[-1] - opens[-1]) / max(bar_range, tick)),
        "close_location_in_bar": 0.0 if bar_range == 0 else float(2 * (closes[-1] - lows[-1]) / bar_range - 1),
        "close_location_in_range_20": 0.0 if window_range == 0 else float(2 * (closes[-1] - min(lows)) / window_range - 1),
        "log1p_volume": float(math.log1p(volumes[-1])),
        "volume_ratio_prior20_median": float((1 + volumes[-1]) / (1 + prior_volume_median) - 1),
        "recent_volume_share_5_20": 0.0 if recent_volume == 0 else float(np.sum(volumes[-5:]) / recent_volume),
        "price_direction_weighted_volume_20": 0.0 if matched_total == 0 else float(np.sum(np.sign(tick_changes) * matched_volume) / matched_total),
        "return_log_volume_correlation_20": correlation,
    }
    if tuple(values) != FEATURE_NAMES or not all(math.isfinite(value) for value in values.values()):
        raise IntegrityError("multi-family feature library is incomplete or nonfinite")
    return values, {**audit, "feature_names": list(FEATURE_NAMES), "atr_10_price_nano": float(np.mean(true_ranges[-10:]))}


def prepare_session(
    rows: Sequence[Mapping[str, object]], decision_at_ns: int, *, stress_cost_usd: float,
) -> dict[str, object]:
    """Join causal features to the strict fixed-hour outcome without leakage."""

    if not math.isfinite(stress_cost_usd) or stress_cost_usd < 0:
        raise IntegrityError("stress cost must be finite and nonnegative")
    features, feature_audit = multifamily_feature_values(rows, decision_at_ns)
    outcome = strict_outcome_path(
        rows, decision_at_ns, expected_contract=str(feature_audit["actual_contract"]),
    )
    if outcome.get("score_status") != "SCORABLE":
        return {"status": outcome["score_status"], "features": features, "feature_audit": feature_audit}
    entry = outcome["entry"]
    exit_row = outcome["exit"]
    path = outcome["path"]
    assert isinstance(entry, Mapping) and isinstance(exit_row, Mapping) and isinstance(path, list)
    expected_economics = (
        int(feature_audit["min_price_increment_nano"]),
        int(feature_audit["multiplier_nano"]),
    )
    if any(
        (row.get("min_price_increment_nano"), row.get("multiplier_nano")) != expected_economics
        for row in [entry, *path, exit_row]
    ):
        raise IntegrityError("outcome path economics differ from the feature-time contract")
    entry_price = int(entry["open_nano"]) / NANO
    exit_price = int(exit_row["open_nano"]) / NANO
    tick_size = int(feature_audit["min_price_increment_nano"]) / NANO
    multiplier = int(feature_audit["multiplier_nano"]) / NANO
    atr_10_price = float(feature_audit["atr_10_price_nano"]) / NANO
    initial_risk_usd = max(atr_10_price * multiplier, tick_size * multiplier)
    signed_gross_r = (exit_price - entry_price) * multiplier / initial_risk_usd
    stress_cost_r = stress_cost_usd / initial_risk_usd
    highs = [int(row["high_nano"]) / NANO for row in path]
    lows = [int(row["low_nano"]) / NANO for row in path]
    return {
        "status": "SCORABLE",
        "features": features,
        "feature_audit": feature_audit,
        "outcome": outcome,
        "signed_gross_r": signed_gross_r,
        "stress_cost_r": stress_cost_r,
        "stress_net_mfe_mae": {
            "long": net_mfe_mae_ratio(
                entry_price=entry_price, highs=highs, lows=lows, direction=1,
                tick_size=tick_size, multiplier=multiplier,
                round_trip_cost_usd=stress_cost_usd,
            ),
            "short": net_mfe_mae_ratio(
                entry_price=entry_price, highs=highs, lows=lows, direction=-1,
                tick_size=tick_size, multiplier=multiplier,
                round_trip_cost_usd=stress_cost_usd,
            ),
        },
    }


class SignedTargetRidge:
    """One signed gross-return target; costs are applied once by the trade rule."""

    def __init__(self, penalty: float) -> None:
        if penalty not in PENALTIES:
            raise IntegrityError("ridge penalty is outside the frozen search")
        self.penalty = penalty
        self.mean: np.ndarray | None = None
        self.scale: np.ndarray | None = None
        self.coefficients: np.ndarray | None = None
        self.intercept: float | None = None

    def fit(self, features: np.ndarray, signed_gross_r: np.ndarray) -> "SignedTargetRidge":
        features = np.asarray(features, dtype=np.float64)
        target = np.asarray(signed_gross_r, dtype=np.float64)
        if (
            features.ndim != 2 or features.shape[1] != len(FEATURE_NAMES)
            or target.shape != (features.shape[0],) or features.shape[0] < len(FEATURE_NAMES) + 1
            or not np.isfinite(features).all() or not np.isfinite(target).all()
        ):
            raise IntegrityError("invalid signed-target ridge training arrays")
        self.mean = np.mean(features, axis=0)
        raw_scale = np.std(features, axis=0, ddof=0)
        self.scale = np.where(raw_scale == 0.0, 1.0, raw_scale)
        design = np.column_stack((np.ones(len(features)), (features - self.mean) / self.scale))
        regularizer = np.eye(design.shape[1]) * self.penalty
        regularizer[0, 0] = 0.0
        weights = np.linalg.solve(design.T @ design + regularizer, design.T @ target)
        self.intercept = float(weights[0])
        self.coefficients = weights[1:]
        return self

    def predict(self, features: np.ndarray) -> np.ndarray:
        if any(value is None for value in (self.mean, self.scale, self.coefficients, self.intercept)):
            raise IntegrityError("signed-target ridge is not fitted")
        features = np.asarray(features, dtype=np.float64)
        assert self.mean is not None and self.scale is not None and self.coefficients is not None
        if features.ndim != 2 or features.shape[1] != len(FEATURE_NAMES) or not np.isfinite(features).all():
            raise IntegrityError("invalid signed-target ridge prediction array")
        return float(self.intercept) + ((features - self.mean) / self.scale) @ self.coefficients


def score_predictions(
    predictions: np.ndarray, signed_gross_r: np.ndarray, stress_cost_r: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    predictions = np.asarray(predictions, dtype=np.float64)
    gross = np.asarray(signed_gross_r, dtype=np.float64)
    costs = np.asarray(stress_cost_r, dtype=np.float64)
    if predictions.shape != gross.shape or gross.shape != costs.shape or predictions.ndim != 1:
        raise IntegrityError("prediction scoring arrays have different shapes")
    if not np.isfinite(predictions).all() or not np.isfinite(gross).all() or not np.isfinite(costs).all() or np.any(costs < 0):
        raise IntegrityError("prediction scoring arrays are invalid")
    intents = np.where(np.abs(predictions) > costs, np.sign(predictions), 0.0).astype(np.int8)
    return intents, intents * gross - (intents != 0) * costs


def _stability(models: Sequence[SignedTargetRidge]) -> list[dict[str, object]]:
    coefficients = np.stack([model.coefficients for model in models])
    rows = []
    for index, name in enumerate(FEATURE_NAMES):
        values = coefficients[:, index]
        positive = int(np.sum(values > 0))
        negative = int(np.sum(values < 0))
        rows.append({
            "feature": name,
            "family": next(family for family, names in FEATURE_FAMILIES.items() if name in names),
            "sign_agreement": max(positive, negative) / len(models),
            "median_absolute_standardized_coefficient": float(np.median(np.abs(values))),
        })
    return sorted(rows, key=lambda row: (-float(row["sign_agreement"]), -float(row["median_absolute_standardized_coefficient"]), str(row["feature"])))


def select_penalty(
    folds: Sequence[tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]],
) -> dict[str, object]:
    """Select only from inner folds; no outer-fold argument exists."""

    if len(folds) != INNER_FOLDS:
        raise IntegrityError("the frozen search requires four inner folds")
    candidates = []
    for penalty in PENALTIES:
        scores, counts, models = [], [], []
        for train_x, train_gross_r, validation_x, validation_gross_r, validation_cost_r in folds:
            model = SignedTargetRidge(penalty).fit(train_x, train_gross_r)
            intents, net_r = score_predictions(model.predict(validation_x), validation_gross_r, validation_cost_r)
            models.append(model)
            scores.append(float(np.mean(net_r)))
            counts.append(int(np.count_nonzero(intents)))
        candidates.append({
            "candidate_id": sha256_json({"full_feature_library": list(FEATURE_NAMES), "ridge_penalty": penalty}),
            "ridge_penalty": penalty,
            "inner_mean_session_net_r": scores,
            "inner_trade_counts": counts,
            "eligible": all(count >= INNER_MINIMUM_TRADES for count in counts) and median(scores) > 0,
            "median_inner_mean_session_net_r": median(scores),
            "feature_stability": _stability(models),
        })
    eligible = [candidate for candidate in candidates if candidate["eligible"]]
    if not eligible:
        raise IntegrityError("no multi-family candidate passes inner activity and net-return gates")
    selected = max(eligible, key=lambda candidate: (float(candidate["median_inner_mean_session_net_r"]), -PENALTIES.index(float(candidate["ridge_penalty"]))))
    return {"selected_candidate_id": selected["candidate_id"], "selected_ridge_penalty": selected["ridge_penalty"], "candidates": candidates}


def frozen_design() -> dict[str, object]:
    core = {
        "schema_version": "fixed_hour_multifamily_discovery/1.0.0",
        "state": "PREPARATION_ONLY_UNREGISTERED_NO_ROW_AUTHORITY",
        "classification": "NEW_COUNTED_MECHANISM_IF_REGISTERED_OR_EXECUTED",
        "hypothesis": "A regularized combination of pre-10:00:05 causal OHLCV state families predicts the signed next-60-minute ES gross return strongly enough to clear stress costs.",
        "claim_ceiling": "ROBUST_PREDICTIVE_ASSOCIATION",
        "data": {
            "foundation": "ACTIVE_CAUSALLY_GATED_NORMALIZED_ONE_MINUTE_OHLCV_ONLY",
            "predictor_fields": ["open_nano", "high_nano", "low_nano", "close_nano", "volume"],
            "gate_only_fields": ["bar_start_ns", "bar_end_ns", "source_timestamp_ns", "available_at_ns", "decision_eligible_at_ns", "actual_contract", "min_price_increment_nano", "multiplier_nano"],
            "sidecars": "LINEAGE_AND_INTEGRITY_ONLY_NEVER_PREDICTORS",
            "rejected": ["RAW_DBN_L0", "BID_ASK", "DEPTH", "AGGRESSOR_SIDE", "SEALED_HOLDOUT", "FORWARD", "2023_PLUS"],
        },
        "timing": {
            "checkpoint": "10:00:05_AMERICA_CHICAGO",
            "feature_window": "EXACT_21_CONSECUTIVE_ONE_MINUTE_BARS_ENDING_10:00:00_SAME_ACTUAL_CONTRACT",
            "entry": "10:01:00_BAR_OPEN_PRINT_PROXY",
            "exit": "11:01:00_BAR_OPEN_PRINT_PROXY",
            "horizon_minutes": 60,
            "purge_minutes": 120,
            "embargo_sessions": 1,
            "corrected_mechanics": "STRICT_OUTCOME_PATH_AND_PURGED_EMBARGOED_FOLDS_FROM_FIXED_HOUR_AUDIT_SUCCESSOR",
        },
        "features": {
            "families": {family: list(names) for family, names in FEATURE_FAMILIES.items()},
            "formulas": FEATURE_FORMULAS,
            "all_features_enter_every_candidate": True,
            "subset_or_indicator_search": False,
            "standardization": "EACH_TRAINING_FOLD_ONLY",
            "correlation_screening": "NONE_RIDGE_HANDLES_COLLINEARITY",
            "stability_ranking": "REPORT_ONLY_NEVER_USED_TO_REDESIGN_OR_RESCUE_THIS_MECHANISM",
        },
        "target": {
            "model_target": "SIGNED_GROSS_60_MINUTE_R",
            "initial_risk": "MAX(DECISION_TIME_ATR10_PRICE_TIMES_MULTIPLIER,TICK_VALUE_USD)",
            "trade_rule": "SIGN(PREDICTED_GROSS_R)_ONLY_WHEN_ABS(PREDICTION)_GT_SESSION_STRESS_COST_R",
            "stress_net_r": "INTENT_TIMES_SIGNED_GROSS_R_MINUS_ONE_ROUND_TRIP_STRESS_COST_R",
            "mfe_mae": "MAX(MFE_USD-ROUND_TRIP_COST_USD,0)/MAX(MAE_USD+ROUND_TRIP_COST_USD,TICK_VALUE_USD)",
            "mfe_mae_role": "OUTCOME_ONLY_DIAGNOSTIC_AND_GATE_NEVER_FEATURE_TARGET_OR_EXIT_TRIGGER",
        },
        "search": {
            "model": "NUMPY_SIGNED_TARGET_RIDGE",
            "candidate_penalties": list(PENALTIES),
            "candidate_count": len(PENALTIES),
            "selection": "HIGHEST_MEDIAN_INNER_FOLD_MEAN_SESSION_STRESS_NET_R",
            "inner_folds": INNER_FOLDS,
            "minimum_trades_each_42_SESSION_INNER_FOLD": INNER_MINIMUM_TRADES,
            "outer_results_may_select_tune_or_drop_features": False,
        },
        "pilot_gates": {
            "minimum_trades_in_63_sessions": PILOT_MINIMUM_TRADES,
            "largest_positive_trade_contribution_lte": 0.20,
            "stress_net_pnl_positive": True,
            "median_net_mfe_mae_gt": 1.0,
            "beat_zero_momentum_reversal_always_long_and_always_short": True,
            "failure": "CLOSE_COMPLETE_MECHANISM_NO_FEATURE_ADDITION_DELETION_OR_RETRY",
        },
        "prior_attempt_accounting": {
            "closed_fixed_hour_result_id": "74b00a56c672c8c45ae0d53a351bd5392e23f46efbfa28c76847bcb48d9df239",
            "prior_related_candidates": 9,
            "new_candidates_if_executed": len(PENALTIES),
            "minimum_related_candidate_total_if_executed": 12,
        },
        "authority": {
            "historical_row_read": False,
            "feature_or_outcome_materialization": False,
            "model_fit_on_real_data": False,
            "trial_registration_or_execution": False,
            "holdout_forward_provider_publication_activation_trading_or_push": False,
        },
    }
    return {**core, "design_id": sha256_json(core)}
