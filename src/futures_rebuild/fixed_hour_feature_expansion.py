"""Synthetic-only ranking of 13 predefined expansions after a fixed baseline."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from statistics import median

import numpy as np

from .canonical import sha256_json
from .errors import IntegrityError
from .exchange_calendar import VerifiedExchangeCalendar
from .fixed_hour_multifamily_discovery import (
    FEATURE_FAMILIES,
    FEATURE_FORMULAS,
    FEATURE_NAMES,
    INNER_FOLDS,
    multifamily_feature_values,
)
from .fixed_hour_outcome_foundation import (
    frozen_outcome_foundation_contract,
    validate_globex_opportunity,
)
from .research.contracts import finite_float64, require_unique_ascii_ids
from .research.feature_foundation import validate_feature_foundation_contract


CORE_FEATURES = (
    "close_change_ticks_20",
    "close_change_volatility_ticks_20",
    "close_location_in_range_20",
    "volume_ratio_prior20_median",
    "price_direction_weighted_volume_20",
)
EXPANSION_CANDIDATES = tuple(name for name in FEATURE_NAMES if name not in CORE_FEATURES)
EXPANSION_COUNTS = (0, 4, 8)
RIDGE_PENALTIES = (0.1, 1.0, 10.0)
REDUNDANCY_CORRELATION_MAXIMUM = 0.90
STABILITY_THRESHOLD = 0.60
OUTER_FOLDS = 8
AUDIT_TRADE_DATES = 63
MINIMUM_OUTER_FIT_TRADE_DATES = 505
MINIMUM_INNER_FIT_TRADE_DATES = 252


def _candidate_definitions() -> list[dict[str, object]]:
    candidates = []
    for expanded_count in EXPANSION_COUNTS:
        for penalty in RIDGE_PENALTIES:
            hyperparameters = {
                "maximum_added_features": expanded_count,
                "ranking": "RMS_ABS_TRAIN_ONLY_CORRELATION_ACROSS_THREE_TARGETS",
                "redundancy_filter": "ABS_TRAIN_ONLY_PAIRWISE_CORRELATION_LT_0.90",
                "ridge_penalty": penalty,
            }
            candidates.append({
                "candidate_id": sha256_json(hyperparameters),
                "feature_names": "ALL_DECLARED",
                "hyperparameters": hyperparameters,
                "hyperparameters_sha256": sha256_json(hyperparameters),
            })
    return candidates


def frozen_feature_foundation_contract() -> dict[str, object]:
    """Return the exact inactive Stage 4 contract for the restarted mechanism."""

    core_library = {
        family: [name for name in names if name in CORE_FEATURES]
        for family, names in FEATURE_FAMILIES.items()
        if any(name in CORE_FEATURES for name in names)
    }
    core: dict[str, object] = {
        "schema_version": "feature_foundation_contract/1.1.0",
        "state": "PREPARED_NOT_ACTIVE",
        "authority": {
            "real_history": False,
            "holdout": False,
            "provider": False,
            "activation": False,
            "trading": False,
        },
        "outcome_foundation_contract_id": frozen_outcome_foundation_contract()["contract_id"],
        "stage_order": [
            "BASELINE_FEATURES",
            "BOUNDED_PREDEFINED_FEATURE_RANKING",
            "FEATURE_SELECTION",
            "FREEZE_FEATURE_PROCEDURE",
        ],
        "core_library": core_library,
        "expanded_feature_names": list(EXPANSION_CANDIDATES),
        "feature_definitions": dict(FEATURE_FORMULAS),
        "expansion_grammar": {
            "operators": [
                "PREDEFINED_CAUSAL_OHLCV_FORMULA_LIBRARY_NO_GENERATION",
            ],
            "lags": [1, 5, 10, 20],
            "windows": [5, 10, 20],
            "interactions": True,
            "maximum_expanded_features": len(EXPANSION_CANDIDATES),
        },
        "feature_discovery": {
            "fit_scope": "INNER_TRAIN_ONLY",
            "attempt_budget": len(EXPANSION_CANDIDATES),
            "ranking": "RMS_ABS_TRAIN_ONLY_CORRELATION_ACROSS_THREE_TARGETS",
            "output": "ORDERED_EXPANSION_CANDIDATES",
        },
        "candidate_budget": len(EXPANSION_COUNTS) * len(RIDGE_PENALTIES),
        "candidates": _candidate_definitions(),
        "selector": {
            "fit_scope": "INNER_TRAIN_ONLY",
            "score_direction": "MAXIMIZE",
            "objective": "SESSION_AND_MARKET_BALANCED_STRESS_NET_TICKS_THEN_TARGET_MSE",
            "tie_break": "CANDIDATE_ID_ASCENDING",
        },
        "stability": {
            "metric": "MEAN_PAIRWISE_JACCARD",
            "threshold": STABILITY_THRESHOLD,
            "feature_names": list(EXPANSION_CANDIDATES),
            "failure_action": "FAIL_PROMOTION",
        },
        "outer_results_may_change_features": False,
    }
    contract = {**core, "contract_id": sha256_json(core)}
    return validate_feature_foundation_contract(contract)


def build_bound_feature_record(
    rows: Sequence[Mapping[str, object]], opportunity: Mapping[str, object],
    *, calendar: VerifiedExchangeCalendar | None = None,
) -> dict[str, object]:
    """Build one causal feature row bound to its frozen schedule identity."""

    scheduled = validate_globex_opportunity(opportunity, calendar=calendar)
    values, audit = multifamily_feature_values(
        rows, int(scheduled["decision_at_ns"]),
        expected_market=str(scheduled["market"]),
        expected_trade_date=str(scheduled["trade_date"]),
        expected_source_contract_id=str(scheduled["source_contract_id"]),
        expected_source_release_id=str(scheduled["source_release_id"]),
    )
    core: dict[str, object] = {
        "schema_version": "fixed_hour_globex_feature_record/1.2.0",
        "feature_foundation_contract_id": frozen_feature_foundation_contract()["contract_id"],
        "opportunity_id": scheduled["opportunity_id"],
        "market": scheduled["market"],
        "trade_date": scheduled["trade_date"],
        "calendar_release_id": scheduled["calendar_release_id"],
        "calendar_authority": scheduled["calendar_authority"],
        "source_contract_id": scheduled["source_contract_id"],
        "source_release_id": scheduled["source_release_id"],
        "decision_at_ns": scheduled["decision_at_ns"],
        "feature_values": values,
        "audit": audit,
    }
    return {**core, "feature_record_id": sha256_json(core)}


@dataclass(frozen=True)
class ExpansionFold:
    fold_id: str
    fit_sample_ids: tuple[str, ...]
    audit_sample_ids: tuple[str, ...]
    fit_trade_dates: tuple[str, ...]
    audit_trade_dates: tuple[str, ...]
    fit_market_ids: tuple[str, ...]
    audit_market_ids: tuple[str, ...]
    audit_stress_cost_ticks: np.ndarray
    fit_features: np.ndarray
    fit_targets: np.ndarray
    audit_features: np.ndarray
    audit_targets: np.ndarray

    def __post_init__(self) -> None:
        if type(self.fold_id) is not str or not self.fold_id:
            raise IntegrityError("feature-expansion fold ID is required")
        fit_ids = require_unique_ascii_ids(self.fit_sample_ids, name="fit_sample_ids")
        audit_ids = require_unique_ascii_ids(self.audit_sample_ids, name="audit_sample_ids")
        if set(fit_ids) & set(audit_ids):
            raise IntegrityError("feature-expansion fit and audit samples overlap")
        metadata = (
            (self.fit_trade_dates, len(fit_ids), "fit trade dates"),
            (self.audit_trade_dates, len(audit_ids), "audit trade dates"),
            (self.fit_market_ids, len(fit_ids), "fit markets"),
            (self.audit_market_ids, len(audit_ids), "audit markets"),
        )
        checked: list[tuple[str, ...]] = []
        for values, expected, name in metadata:
            if (
                not isinstance(values, tuple) or len(values) != expected
                or any(type(value) is not str or not value or not value.isascii() for value in values)
            ):
                raise IntegrityError(f"feature-expansion {name} are invalid")
            checked.append(values)
        fit_dates, audit_dates, fit_markets, audit_markets = checked
        try:
            for value in (*fit_dates, *audit_dates):
                date.fromisoformat(value)
        except ValueError as exc:
            raise IntegrityError("feature-expansion trade date is invalid") from exc
        complete_dates = all(
            all(
                {market for day, market in zip(dates, markets, strict=True) if day == current}
                == set(markets)
                for current in set(dates)
            )
            for dates, markets in ((fit_dates, fit_markets), (audit_dates, audit_markets))
        )
        if (
            set(fit_dates) & set(audit_dates)
            or max(fit_dates) >= min(audit_dates)
            or set(fit_markets) != set(audit_markets)
            or len(set(fit_markets)) != 1
            or not complete_dates
        ):
            raise IntegrityError("feature-expansion folds require one market and complete chronological trade dates")
        costs = finite_float64(
            self.audit_stress_cost_ticks, name="audit_stress_cost_ticks", ndim=1,
        )
        if costs.shape != (len(audit_ids),) or bool(np.any(costs <= 0.0)):
            raise IntegrityError("feature-expansion stress costs are invalid")
        fit_x = finite_float64(self.fit_features, name="fit_features", ndim=2)
        fit_y = finite_float64(self.fit_targets, name="fit_targets", ndim=2)
        audit_x = finite_float64(self.audit_features, name="audit_features", ndim=2)
        audit_y = finite_float64(self.audit_targets, name="audit_targets", ndim=2)
        if (
            fit_x.shape != (len(fit_ids), len(FEATURE_NAMES))
            or fit_y.shape != (len(fit_ids), 3)
            or audit_x.shape != (len(audit_ids), len(FEATURE_NAMES))
            or audit_y.shape != (len(audit_ids), 3)
        ):
            raise IntegrityError("feature-expansion fold arrays are not aligned")
        frozen = []
        for value in (fit_x, fit_y, audit_x, audit_y):
            copied = np.ascontiguousarray(value, dtype=np.float64).copy()
            copied.flags.writeable = False
            frozen.append(copied)
        object.__setattr__(self, "fit_sample_ids", fit_ids)
        object.__setattr__(self, "audit_sample_ids", audit_ids)
        object.__setattr__(self, "fit_trade_dates", fit_dates)
        object.__setattr__(self, "audit_trade_dates", audit_dates)
        object.__setattr__(self, "fit_market_ids", fit_markets)
        object.__setattr__(self, "audit_market_ids", audit_markets)
        frozen_costs = np.ascontiguousarray(costs, dtype=np.float64).copy()
        frozen_costs.flags.writeable = False
        object.__setattr__(self, "audit_stress_cost_ticks", frozen_costs)
        object.__setattr__(self, "fit_features", frozen[0])
        object.__setattr__(self, "fit_targets", frozen[1])
        object.__setattr__(self, "audit_features", frozen[2])
        object.__setattr__(self, "audit_targets", frozen[3])


@dataclass(frozen=True)
class Tier0Dataset:
    """Identity-bound ES arrays for the provider-free WFA builder."""

    sample_ids: tuple[str, ...]
    trade_dates: tuple[str, ...]
    market_ids: tuple[str, ...]
    features: np.ndarray
    targets: np.ndarray
    stress_cost_ticks: np.ndarray

    def __post_init__(self) -> None:
        ids = require_unique_ascii_ids(self.sample_ids, name="tier0 sample_ids")
        n = len(ids)
        if (
            len(self.trade_dates) != n or len(self.market_ids) != n
            or set(self.market_ids) != {"ES"}
        ):
            raise IntegrityError("Tier 0 requires aligned ES-only rows")
        try:
            parsed = tuple(date.fromisoformat(value) for value in self.trade_dates)
        except (TypeError, ValueError) as exc:
            raise IntegrityError("Tier 0 trade date is invalid") from exc
        if tuple(sorted(parsed)) != parsed:
            raise IntegrityError("Tier 0 rows are not trade-date ordered")
        x = finite_float64(self.features, name="tier0 features", ndim=2)
        y = finite_float64(self.targets, name="tier0 targets", ndim=2)
        costs = finite_float64(self.stress_cost_ticks, name="tier0 stress costs", ndim=1)
        if x.shape != (n, len(FEATURE_NAMES)) or y.shape != (n, 3) or costs.shape != (n,) or np.any(costs <= 0):
            raise IntegrityError("Tier 0 arrays do not match the frozen feature/target design")
        for name, value in (("features", x), ("targets", y), ("stress_cost_ticks", costs)):
            copied = np.ascontiguousarray(value, dtype=np.float64).copy()
            copied.flags.writeable = False
            object.__setattr__(self, name, copied)


def _indices_for_dates(all_dates: tuple[str, ...], selected: tuple[str, ...]) -> np.ndarray:
    selected_set = set(selected)
    return np.asarray([index for index, value in enumerate(all_dates) if value in selected_set], dtype=np.int64)


def _expansion_fold(dataset: Tier0Dataset, fold_id: str, fit_dates: tuple[str, ...], audit_dates: tuple[str, ...]) -> ExpansionFold:
    fit = _indices_for_dates(dataset.trade_dates, fit_dates)
    audit = _indices_for_dates(dataset.trade_dates, audit_dates)
    return ExpansionFold(
        fold_id=fold_id,
        fit_sample_ids=tuple(dataset.sample_ids[index] for index in fit),
        audit_sample_ids=tuple(dataset.sample_ids[index] for index in audit),
        fit_trade_dates=tuple(dataset.trade_dates[index] for index in fit),
        audit_trade_dates=tuple(dataset.trade_dates[index] for index in audit),
        fit_market_ids=("ES",) * len(fit),
        audit_market_ids=("ES",) * len(audit),
        audit_stress_cost_ticks=dataset.stress_cost_ticks[audit],
        fit_features=dataset.features[fit],
        fit_targets=dataset.targets[fit],
        audit_features=dataset.features[audit],
        audit_targets=dataset.targets[audit],
    )


def run_tier0_wfa_builder(dataset: Tier0Dataset) -> dict[str, object]:
    """Select and predict eight ES outer folds without using outer outcomes."""

    if not isinstance(dataset, Tier0Dataset):
        raise IntegrityError("Tier 0 requires its validated dataset")
    unique_dates = tuple(sorted(set(dataset.trade_dates)))
    required = MINIMUM_OUTER_FIT_TRADE_DATES + 1 + OUTER_FOLDS * AUDIT_TRADE_DATES
    if len(unique_dates) < required:
        raise IntegrityError(f"Tier 0 requires at least {required} complete trade dates")
    first_audit = len(unique_dates) - OUTER_FOLDS * AUDIT_TRADE_DATES
    outer_rows = []
    outer_selected_sets = []
    for outer_index in range(OUTER_FOLDS):
        audit_start = first_audit + outer_index * AUDIT_TRADE_DATES
        fit_dates = unique_dates[:audit_start - 1]
        embargo_date = unique_dates[audit_start - 1]
        audit_dates = unique_dates[audit_start:audit_start + AUDIT_TRADE_DATES]
        if len(fit_dates) < MINIMUM_OUTER_FIT_TRADE_DATES:
            raise IntegrityError("Tier 0 outer fit misses the 505-date minimum")
        inner_audit_start = len(fit_dates) - 4 * AUDIT_TRADE_DATES
        inner_folds = []
        inner_manifests = []
        for inner_index in range(4):
            start = inner_audit_start + inner_index * AUDIT_TRADE_DATES
            inner_fit_dates = fit_dates[:start - 1]
            inner_audit_dates = fit_dates[start:start + AUDIT_TRADE_DATES]
            if len(inner_fit_dates) < MINIMUM_INNER_FIT_TRADE_DATES:
                raise IntegrityError("Tier 0 inner fit misses the 252-date minimum")
            inner_folds.append(_expansion_fold(
                dataset, f"outer-{outer_index}-inner-{inner_index}",
                inner_fit_dates, inner_audit_dates,
            ))
            inner_manifests.append({
                "inner_fold_id": f"outer-{outer_index}-inner-{inner_index}",
                "fit_trade_dates": list(inner_fit_dates),
                "embargo_trade_date": fit_dates[start - 1],
                "audit_trade_dates": list(inner_audit_dates),
            })
        selection = select_expansion_candidate(tuple(inner_folds))
        if selection["state"] != "PASS_SYNTHETIC_SELECTION_MECHANICS_ONLY":
            raise IntegrityError("Tier 0 optional-feature stability failed promotion")
        chosen = next(item for item in selection["candidates"] if item["candidate_id"] == selection["selected_candidate_id"])
        fit = _indices_for_dates(dataset.trade_dates, fit_dates)
        audit = _indices_for_dates(dataset.trade_dates, audit_dates)
        ranked = discover_fold_features(dataset.features[fit], dataset.targets[fit])
        selected = select_fold_features(
            dataset.features[fit], ranked,
            maximum_added_features=int(chosen["hyperparameters"]["maximum_added_features"]),
        )
        columns = [FEATURE_NAMES.index(name) for name in selected]
        predicted = _ridge_predict(
            dataset.features[fit][:, columns], dataset.targets[fit],
            dataset.features[audit][:, columns],
            penalty=float(chosen["hyperparameters"]["ridge_penalty"]),
        )
        outer_selected_sets.append(set(selected) - set(CORE_FEATURES))
        outer_rows.append({
            "outer_fold_id": f"outer-{outer_index}",
            "fit_trade_dates": list(fit_dates),
            "embargo_trade_date": embargo_date,
            "audit_trade_dates": list(audit_dates),
            "fit_sample_ids": [dataset.sample_ids[index] for index in fit],
            "audit_sample_ids": [dataset.sample_ids[index] for index in audit],
            "selected_candidate_id": selection["selected_candidate_id"],
            "selected_feature_names": list(selected),
            "inner_fold_manifests": inner_manifests,
            "predictions": predicted.tolist(),
        })
    baseline_only = all(not value for value in outer_selected_sets)
    stability, passed = optional_feature_stability(outer_selected_sets)
    core = {
        "schema_version": "fixed_hour_tier0_wfa_builder/1.0.0",
        "feature_foundation_contract_id": frozen_feature_foundation_contract()["contract_id"],
        "market": "ES",
        "outer_folds": outer_rows,
        "stability": {
            "metric": "MEAN_PAIRWISE_JACCARD",
            "score": stability,
            "threshold": STABILITY_THRESHOLD,
            "applicability": "NOT_APPLICABLE_BASELINE_ONLY" if baseline_only else "OPTIONAL_EXPANSIONS_ONLY",
            "passed": passed,
        },
        "state": "BUILDER_COMPLETE_OUTER_OUTCOMES_UNSEEN" if passed else "FAIL_PROMOTION",
        "authority": {"real_history": False, "holdout": False, "activation": False, "trading": False},
    }
    return {**core, "run_id": sha256_json(core)}


def run_synthetic_tier0_wfa_builder() -> dict[str, object]:
    """Exercise the complete eight-fold builder without real-history authority."""

    count = MINIMUM_OUTER_FIT_TRADE_DATES + 1 + OUTER_FOLDS * AUDIT_TRADE_DATES
    rng = np.random.default_rng(812)
    features = np.zeros((count, len(FEATURE_NAMES)), dtype=np.float64)
    features[:, FEATURE_NAMES.index(CORE_FEATURES[0])] = rng.normal(scale=0.05, size=count)
    features[:, FEATURE_NAMES.index(EXPANSION_CANDIDATES[0])] = rng.normal(size=count)
    signal = 8.0 * features[:, FEATURE_NAMES.index(EXPANSION_CANDIDATES[0])]
    targets = np.column_stack((signal, 12.0 + signal, 12.0 - signal))
    dates = tuple((date(2018, 1, 1) + timedelta(days=index)).isoformat() for index in range(count))
    return run_tier0_wfa_builder(Tier0Dataset(
        sample_ids=tuple(f"synthetic-es-{index:04d}" for index in range(count)),
        trade_dates=dates,
        market_ids=("ES",) * count,
        features=features,
        targets=targets,
        stress_cost_ticks=np.ones(count, dtype=np.float64),
    ))


def discover_fold_features(
    fit_features: np.ndarray, fit_targets: np.ndarray,
) -> tuple[str, ...]:
    """Rank the bounded expansion candidates using training rows only."""

    x = finite_float64(fit_features, name="fit_features", ndim=2)
    y = finite_float64(fit_targets, name="fit_targets", ndim=2)
    if x.shape != (len(y), len(FEATURE_NAMES)) or y.shape[1] != 3:
        raise IntegrityError("feature-discovery matrix does not match the frozen library")
    lookup = {name: index for index, name in enumerate(FEATURE_NAMES)}

    def multi_target_relevance(name: str) -> float:
        values = x[:, lookup[name]]
        if np.std(values) == 0.0:
            return 0.0
        correlations = []
        for column in y.T:
            result = 0.0 if np.std(column) == 0.0 else float(np.corrcoef(values, column)[0, 1])
            correlations.append(result if math.isfinite(result) else 0.0)
        return math.sqrt(sum(value * value for value in correlations) / len(correlations))

    return tuple(sorted(
        EXPANSION_CANDIDATES,
        key=lambda name: (-multi_target_relevance(name), name),
    ))


def select_fold_features(
    fit_features: np.ndarray,
    discovered_feature_names: tuple[str, ...],
    *,
    maximum_added_features: int,
) -> tuple[str, ...]:
    """Select nonredundant discoveries after fold-local expansion."""

    x = finite_float64(fit_features, name="fit_features", ndim=2)
    if x.shape[1] != len(FEATURE_NAMES):
        raise IntegrityError("feature-selection matrix does not match the frozen library")
    if tuple(sorted(discovered_feature_names)) != tuple(sorted(EXPANSION_CANDIDATES)):
        raise IntegrityError("feature selection did not receive the complete discovery output")
    if maximum_added_features not in EXPANSION_COUNTS:
        raise IntegrityError("feature-expansion count is outside the frozen search")
    lookup = {name: index for index, name in enumerate(FEATURE_NAMES)}
    selected = list(CORE_FEATURES)
    for name in discovered_feature_names:
        if len(selected) == len(CORE_FEATURES) + maximum_added_features:
            break
        candidate = x[:, lookup[name]]
        if np.std(candidate) == 0.0:
            continue
        redundant = False
        for kept_name in selected:
            kept = x[:, lookup[kept_name]]
            if np.std(kept) == 0.0:
                continue
            correlation = float(abs(np.corrcoef(candidate, kept)[0, 1]))
            if math.isfinite(correlation) and correlation >= REDUNDANCY_CORRELATION_MAXIMUM:
                redundant = True
                break
        if not redundant:
            selected.append(name)
    return tuple(name for name in FEATURE_NAMES if name in selected)


def optional_feature_stability(
    selected_feature_names: Sequence[Sequence[str]],
) -> tuple[float | None, bool]:
    """Apply the frozen optional-only Jaccard promotion gate."""

    sets = [set(values) - set(CORE_FEATURES) for values in selected_feature_names]
    if all(not value for value in sets):
        return None, True
    pairwise = [
        len(left & right) / len(left | right) if left | right else 1.0
        for index, left in enumerate(sets)
        for right in sets[index + 1:]
    ]
    score = float(sum(pairwise) / len(pairwise)) if pairwise else 1.0
    return score, score >= STABILITY_THRESHOLD


def _ridge_predict(
    fit_features: np.ndarray,
    fit_target: np.ndarray,
    audit_features: np.ndarray,
    *,
    penalty: float,
) -> np.ndarray:
    means = np.mean(fit_features, axis=0, dtype=np.float64)
    scales = np.std(fit_features, axis=0, dtype=np.float64)
    scales = np.where(scales > 0.0, scales, 1.0)
    standardized = (fit_features - means) / scales
    target_mean = np.mean(fit_target, axis=0, dtype=np.float64)
    centered_target = fit_target - target_mean
    regularized = standardized.T @ standardized + penalty * np.eye(
        fit_features.shape[1], dtype=np.float64
    )
    try:
        weights = np.linalg.solve(regularized, standardized.T @ centered_target)
    except np.linalg.LinAlgError as exc:
        raise IntegrityError("feature-expansion ridge fit is singular") from exc
    predictions = target_mean + (
        (audit_features - means) / scales
    ) @ weights
    if not np.isfinite(predictions).all():
        raise IntegrityError("feature-expansion ridge prediction is nonfinite")
    return np.ascontiguousarray(predictions, dtype=np.float64)


def cost_adjusted_directions(
    predicted_targets: np.ndarray, stress_cost_ticks: np.ndarray,
) -> np.ndarray:
    """Apply the frozen long, short, or abstain rule to three target forecasts."""

    predicted = finite_float64(predicted_targets, name="predicted_targets", ndim=2)
    costs = finite_float64(stress_cost_ticks, name="stress_cost_ticks", ndim=1)
    if predicted.shape != (len(costs), 3) or bool(np.any(costs <= 0.0)):
        raise IntegrityError("direction inputs do not match the frozen target and cost rule")
    terminal = predicted[:, 0]
    upside = np.maximum(predicted[:, 1], 0.0)
    downside = np.maximum(predicted[:, 2], 0.0)
    long_ratio = np.maximum(upside - costs, 0.0) / np.maximum(downside + costs, 1.0)
    short_ratio = np.maximum(downside - costs, 0.0) / np.maximum(upside + costs, 1.0)
    return np.where(
        (long_ratio > 1.0) & (terminal > costs),
        1.0,
        np.where((short_ratio > 1.0) & (-terminal > costs), -1.0, 0.0),
    )


def _session_balanced_stress_net_ticks(
    predicted: np.ndarray,
    actual: np.ndarray,
    costs: np.ndarray,
    trade_dates: tuple[str, ...],
    market_ids: tuple[str, ...],
) -> float:
    direction = cost_adjusted_directions(predicted, costs)
    net = np.where(direction == 0.0, 0.0, direction * actual[:, 0] - costs)
    date_scores = []
    for trade_date in sorted(set(trade_dates)):
        market_scores = [
            float(np.mean(net[[
                index for index, (day, candidate_market) in enumerate(
                    zip(trade_dates, market_ids, strict=True)
                ) if day == trade_date and candidate_market == market
            ]]))
            for market in sorted(set(market_ids))
        ]
        date_scores.append(float(np.mean(market_scores)))
    return float(np.mean(date_scores))


def select_expansion_candidate(folds: tuple[ExpansionFold, ...]) -> dict[str, object]:
    """Run the frozen nine-way search strictly inside four inner folds."""

    if len(folds) != INNER_FOLDS or any(not isinstance(fold, ExpansionFold) for fold in folds):
        raise IntegrityError("feature expansion requires the frozen four inner folds")
    if len({fold.fold_id for fold in folds}) != len(folds):
        raise IntegrityError("feature-expansion fold IDs repeat")
    if any(
        len(set(fold.fit_trade_dates)) < MINIMUM_INNER_FIT_TRADE_DATES
        or len(set(fold.audit_trade_dates)) != AUDIT_TRADE_DATES
        for fold in folds
    ):
        raise IntegrityError("feature expansion requires 252 fit and exactly 63 audit trade dates")
    lookup = {name: index for index, name in enumerate(FEATURE_NAMES)}
    candidates = []
    for definition in _candidate_definitions():
        parameters = definition["hyperparameters"]
        fold_rows = []
        economic_scores = []
        prediction_scores = []
        for fold in folds:
            discovered = discover_fold_features(fold.fit_features, fold.fit_targets)
            selected = select_fold_features(
                fold.fit_features,
                discovered,
                maximum_added_features=int(parameters["maximum_added_features"]),
            )
            columns = [lookup[name] for name in selected]
            predicted = _ridge_predict(
                fold.fit_features[:, columns],
                fold.fit_targets,
                fold.audit_features[:, columns],
                penalty=float(parameters["ridge_penalty"]),
            )
            target_scale = np.std(fold.fit_targets, axis=0, dtype=np.float64)
            target_scale = np.where(target_scale > 0.0, target_scale, 1.0)
            score = -float(np.mean(((predicted - fold.audit_targets) / target_scale) ** 2))
            economic_score = _session_balanced_stress_net_ticks(
                predicted, fold.audit_targets, fold.audit_stress_cost_ticks,
                fold.audit_trade_dates, fold.audit_market_ids,
            )
            economic_scores.append(economic_score)
            prediction_scores.append(score)
            fold_rows.append({
                "fold_id": fold.fold_id,
                "fit_sample_ids": list(fold.fit_sample_ids),
                "audit_sample_ids": list(fold.audit_sample_ids),
                "fit_trade_dates": list(fold.fit_trade_dates),
                "audit_trade_dates": list(fold.audit_trade_dates),
                "fit_market_ids": list(fold.fit_market_ids),
                "audit_market_ids": list(fold.audit_market_ids),
                "discovered_feature_names": list(discovered),
                "selected_feature_names": list(selected),
                "session_and_market_balanced_stress_net_ticks": economic_score,
                "negative_mean_standardized_target_mse": score,
            })
        candidates.append({
            **definition,
            "folds": fold_rows,
            "median_session_and_market_balanced_stress_net_ticks": median(economic_scores),
            "median_negative_mean_standardized_target_mse": median(prediction_scores),
        })
    selected = min(
        candidates,
        key=lambda candidate: (
            -float(candidate["median_session_and_market_balanced_stress_net_ticks"]),
            -float(candidate["median_negative_mean_standardized_target_mse"]),
            str(candidate["candidate_id"]),
        ),
    )
    baseline_only = int(selected["hyperparameters"]["maximum_added_features"]) == 0
    stability, stability_passed = optional_feature_stability(
        [row["selected_feature_names"] for row in selected["folds"]]
    )
    core: dict[str, object] = {
        "schema_version": "fixed_hour_feature_expansion_selection/1.0.0",
        "feature_foundation_contract_id": frozen_feature_foundation_contract()["contract_id"],
        "selected_candidate_id": selected["candidate_id"],
        "candidates": candidates,
        "stability": {
            "metric": "MEAN_PAIRWISE_JACCARD",
            "score": stability,
            "threshold": STABILITY_THRESHOLD,
            "applicability": "NOT_APPLICABLE_BASELINE_ONLY" if baseline_only else "OPTIONAL_EXPANSIONS_ONLY",
            "passed": stability_passed,
        },
        "state": (
            "PASS_SYNTHETIC_SELECTION_MECHANICS_ONLY"
            if stability_passed
            else "FAIL_PROMOTION"
        ),
        "authority": {
            "real_history": False,
            "holdout": False,
            "activation": False,
        },
    }
    return {**core, "selection_id": sha256_json(core)}
