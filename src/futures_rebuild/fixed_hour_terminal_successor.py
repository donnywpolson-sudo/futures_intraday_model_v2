"""Inactive terminal-only fixed-hour research: pure mechanics, no market-data IO.

Predecessor modules retain their original behavior. Arrays accepted here do not
grant authority to read, register, evaluate, or publish historical research.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from functools import lru_cache

import numpy as np

from .canonical import sha256_json
from .errors import IntegrityError
from .exchange_calendar import VerifiedExchangeCalendar
from .fixed_hour_feature_expansion import CORE_FEATURES, RIDGE_PENALTIES, _ridge_predict
from .fixed_hour_multifamily_discovery import FEATURE_NAMES as OLD_NAMES, FEATURE_FAMILIES, multifamily_feature_values
from .fixed_hour_outcome_foundation import MINUTE, build_directional_path_outcome, validate_globex_opportunity
from .fixed_hour_ridge_audit_successor import validate_fixed_hour_rows
from .fixed_hour_ridge_discovery import NANO
from .research.contracts import finite_float64, require_unique_ascii_ids
from .research.splits import SessionWindow, TemporalSamples, nested_chronological_splits

MARKETS = ("ES", "NQ", "CL", "GC")
AUTHORITY = {key: False for key in ("real_history", "activation", "provider", "holdout", "forward", "trading")}
LONG_NAMES = ("close_change_ticks_60", "close_change_volatility_ticks_60")
QUARTILE_NAMES = tuple(f"open_interval_quartile_{q}" for q in (2, 3, 4))
INTERACTION_NAMES = tuple(f"{name}_times_q{q}" for q in (2, 3, 4) for name in (CORE_FEATURES[0], CORE_FEATURES[4]))
FEATURE_NAMES = (*OLD_NAMES, *LONG_NAMES, *QUARTILE_NAMES, *INTERACTION_NAMES)
PACKS = {
    "baseline": CORE_FEATURES,
    **{f"without_{name}": tuple(item for item in CORE_FEATURES if item != name) for name in CORE_FEATURES},
    "existing_18": OLD_NAMES,
    "context": (*CORE_FEATURES, *LONG_NAMES, *QUARTILE_NAMES),
    "context_interactions": (*CORE_FEATURES, *LONG_NAMES, *QUARTILE_NAMES, *INTERACTION_NAMES),
}
FAMILIES = {name: family for family, names in FEATURE_FAMILIES.items() for name in names}
FAMILIES.update({LONG_NAMES[0]: "price_path", LONG_NAMES[1]: "volatility"})
FAMILIES.update({name: "verified_time_context" for name in QUARTILE_NAMES})
FAMILIES.update({name: "time_interaction" for name in INTERACTION_NAMES})
ADVERSE_TICKS = {"base": (2., 2., 4., 4.), "stress": (4., 4., 8., 8.), "extreme": (8., 8., 16., 16.)}


def candidate_definitions() -> tuple[dict, ...]:
    return tuple({"candidate_id": f"{pack}:ridge={penalty:g}", "pack": pack,
                  "feature_names": list(names), "penalty": penalty}
                 for pack, names in PACKS.items() for penalty in RIDGE_PENALTIES)


def design() -> dict:
    """One material identity binds phases 3/4/5/6 and the inactive ladder."""
    core = {
        "schema_version": "fixed_hour_terminal_successor/1.0.0",
        "state": "IMPLEMENTED_INACTIVE_SYNTHETIC_ONLY", "authority": AUTHORITY,
        "markets": list(MARKETS), "candidates": list(candidate_definitions()),
        "feature_names": list(FEATURE_NAMES), "configurations_per_market": 27,
        "total_market_configurations": 108,
        "mechanism_card": {
            "status": "UNPROVEN_PREDICTIVE_HYPOTHESIS",
            "hypothesis": "Recent direction and price-volume behavior conditioned on longer trend/volatility and verified interval context predict 60-minute terminal change sufficient to clear stress costs.",
            "boundary": "Only declared markets and 61-bar-eligible fixed-grid Globex opportunities; no transferability or economic-causality claim.",
            "competing_explanations": {
                "unconditional_exposure": "Compare all frozen directional baselines on identical opportunities.",
                "sample_noise": "Nested chronological evaluation with complete-trial multiplicity and no outer-result changes.",
                "coverage_change": "Identical 61-bar eligibility across every candidate; report both original and eligible schedules.",
            },
        },
        "phase_4": {"fit_target": "terminal_change_ticks", "diagnostics_only": ["maximum_upside_excursion_ticks", "maximum_downside_excursion_ticks"],
                    "execution_reference": "DESIGNATED_ONE_MINUTE_OPEN_NANO_PRINT_PROXY_NOT_FILL",
                    "entry_grid_minutes": [22, 60], "required_contiguous_bars": 61,
                    "earliest_eligible_entry_offset_minutes": 82, "fixed_exit_minutes": 60,
                    "label_consistency": "U>=max(T,0);D>=max(-T,0);finite_integral_ticks",
                    "timezone": "America/Los_Angeles"},
        "phase_5": {"new_formulas": {LONG_NAMES[0]: "(close[-1]-close[-61])/tick", LONG_NAMES[1]: "population_std(diff(close[-61:])/tick)",
                    "quartile": "min(4,1+floor(4*(decision-open_start)/(open_end-open_start)))",
                    "interactions": "quartiles 2,3,4 times close_change_ticks_20 and price_direction_weighted_volume_20"},
                    "selection": "27_FIXED_CANDIDATES_INNER_WFA_ONLY", "transform": "TRAIN_ONLY_CONSTANT_AND_EXACT_DUPLICATE_REMOVAL_THEN_STANDARDIZATION",
                    "comparable_tolerance_ticks_per_opportunity": 0.01,
                    "tie_break": ["mean_retained_column_count", "larger_penalty", "candidate_id"],
                    "correlations": "TRAIN_ONLY_DESCRIPTIVE_NOT_SELECTOR",
                    "stability": "DESCRIPTIVE_PACK_FEATURE_FAMILY_FREQUENCY_NO_JACCARD_GATE"},
        "phase_6": {"outer_folds": 8, "inner_folds": 4, "audit_dates": 63,
                    "minimum_outer_fit_dates": 505, "minimum_inner_fit_dates": 252,
                    "embargo_dates": 1, "purge": "WHOLE_DATE_LABEL_KNOWN_BEFORE_AUDIT",
                    "period": ["2018-01-01", "2023-01-01"],
                    "decision": "LONG_IF_T>C;SHORT_IF_T<-C;ELSE_ABSTAIN",
                    "primary_score": "MEDIAN_INNER_FOLD_SESSION_BALANCED_STRESS_NET_TICKS",
                    "risk_adjusted_objective": False, "fee_round_trip_usd": 10.0,
                    "costs_provisional": True, "adverse_ticks": {name: list(values) for name, values in ADVERSE_TICKS.items()},
                    "cost_floor_reduction_after_results": False,
                    "baselines": ["flat_no_trade", "fold_local_unconditional_direction", "previous_bar_sign_momentum", "previous_bar_sign_reversal", "always_long", "always_short"],
                    "minimum_trade_dates": 252, "minimum_executed_trades": 500,
                    "minimum_eligible_schedule_coverage": 0.98, "quality_errors_max": 0,
                    "segment_gate": "NONNEGATIVE_STRESS_MEAN_FOR_EACH_QUARTILE_WITH_AT_LEAST_63_DATES",
                    "hac_lag": 5, "hac_one_sided_z": 1.6448536269514722,
                    "romano_wolf": {"alpha": 0.05, "resamples": 999, "mean_block_length": 10.0, "seed": 62817},
                    "dsr_min_probability": 0.95, "dsr_requires_complete_trial_sharpe_census": True},
        "ladder": {"qualification": "INDEPENDENT_MARKETS_JOINT_INFERENCE",
                   "engineering_order": list(MARKETS), "es_economic_failure_veto": False,
                   "all_declared_results_required": True, "legacy_macro_qualifier": False,
                   "broad_generality_claim": False, "phase_10_11": "NOT_APPLICABLE",
                   "downstream": "QUALIFIED_MARKETS_ONLY_PHASE_12_THEN_REFIT_THEN_COMMON_FINAL_252",
                   "stress_trade_date_period": ["2023-01-01", "2025-07-14"],
                   "stress_event_cutoff_exclusive": "2025-07-13T22:00:00Z",
                   "stress_decision": "WHOLE_PERIOD_ONLY_ANNUAL_SLICES_REPORT_ONLY",
                   "stress_retry_or_tuning": False, "refit": "FIT_PARAMETERS_ONLY_NO_EVALUATION",
                   "final_252_trade_dates": ["2025-07-14", "2026-07-13"], "forward_access": False},
    }
    return {**core, "mechanism_id": sha256_json(core)}


@lru_cache(maxsize=1)
def mechanism_id() -> str:
    return design()["mechanism_id"]


def validate_labels(terminal: np.ndarray, upside: np.ndarray, downside: np.ndarray) -> None:
    arrays = [finite_float64(a, name="realized labels", ndim=1) for a in (terminal, upside, downside)]
    if len({a.shape for a in arrays}) != 1 or any(np.any(a != np.rint(a)) for a in arrays):
        raise IntegrityError("realized labels must be aligned integral ticks")
    if np.any(upside < np.maximum(terminal, 0)) or np.any(downside < np.maximum(-terminal, 0)):
        raise IntegrityError("realized excursion labels contradict terminal change")


def terminal_directions(predictions: np.ndarray, stress_costs: np.ndarray) -> np.ndarray:
    p = finite_float64(predictions, name="terminal predictions", ndim=1)
    c = finite_float64(stress_costs, name="stress costs", ndim=1)
    if p.shape != c.shape or np.any(c <= 0):
        raise IntegrityError("terminal decisions require aligned positive costs")
    return np.where(p > c, 1., np.where(p < -c, -1., 0.))


def provisional_costs(market: str, tick_values: np.ndarray) -> dict[str, np.ndarray]:
    v = finite_float64(tick_values, name="tick values USD", ndim=1)
    if market not in MARKETS or np.any(v <= 0):
        raise IntegrityError("invalid market or point-in-time tick economics")
    return {scenario: np.full(v.shape, ticks[MARKETS.index(market)]) + 10. / v for scenario, ticks in ADVERSE_TICKS.items()}


def build_feature_record(rows: Sequence[Mapping], opportunity: Mapping, *, calendar: VerifiedExchangeCalendar | None = None) -> dict:
    scheduled = validate_globex_opportunity(opportunity, calendar=calendar)
    decision = scheduled["decision_at_ns"]
    end = decision - 5 * NANO
    start = end - 61 * MINUTE
    base = {"mechanism_id": mechanism_id(), "opportunity_id": scheduled["opportunity_id"],
            "market": scheduled["market"], "trade_date": scheduled["trade_date"],
            "calendar_release_id": scheduled["calendar_release_id"], "decision_at_ns": decision}
    if start < scheduled["open_interval_start_ns"]:
        return {**base, "status": "INSUFFICIENT_HISTORY", "feature_values": None}
    window = [row for row in rows if type(row.get("bar_start_ns")) is int and start <= row["bar_start_ns"] < end]
    window.sort(key=lambda row: row["bar_start_ns"])
    if len(window) != 61 or [r["bar_start_ns"] for r in window] != list(range(start, end, MINUTE)):
        raise IntegrityError("successor requires exactly 61 consecutive bars without duplicates")
    validate_fixed_hour_rows(window, expected_market=scheduled["market"], expected_trade_date=scheduled["trade_date"],
                            expected_source_contract_id=scheduled["source_contract_id"], expected_source_release_id=scheduled["source_release_id"],
                            expected_contract=window[0].get("actual_contract"))
    if any(r["decision_eligible_at_ns"] > decision for r in window):
        raise IntegrityError("feature history is not available at decision")
    if len({(r["min_price_increment_nano"], r["multiplier_nano"]) for r in window}) != 1:
        raise IntegrityError("feature history changes contract economics")
    values, audit = multifamily_feature_values(window[-21:], decision,
        expected_market=scheduled["market"], expected_trade_date=scheduled["trade_date"],
        expected_source_contract_id=scheduled["source_contract_id"], expected_source_release_id=scheduled["source_release_id"])
    tick = window[0]["min_price_increment_nano"]
    changes = np.array([(right["close_nano"] - left["close_nano"]) / tick for left, right in zip(window, window[1:])])
    values[LONG_NAMES[0]] = float((window[-1]["close_nano"] - window[0]["close_nano"]) / tick)
    values[LONG_NAMES[1]] = float(np.std(changes, ddof=0))
    quartile = min(4, 1 + 4 * (decision - scheduled["open_interval_start_ns"]) // (scheduled["open_interval_end_ns"] - scheduled["open_interval_start_ns"]))
    values.update({name: float(quartile == q) for q, name in zip((2, 3, 4), QUARTILE_NAMES)})
    for q in (2, 3, 4):
        for name in (CORE_FEATURES[0], CORE_FEATURES[4]):
            values[f"{name}_times_q{q}"] = values[name] * float(quartile == q)
    if tuple(values) != FEATURE_NAMES or not np.isfinite(list(values.values())).all():
        raise IntegrityError("successor feature library is invalid")
    core = {**base, "status": "VALID_FEATURE", "quartile": quartile, "feature_values": values,
            "audit": {**audit, "first_bar_start_ns": start, "bar_count": 61,
                      "source_row_ids": [r["row_id"] for r in window], "source_row_sha256s": [r["source_row_sha256"] for r in window]},
            "source_contract_id": scheduled["source_contract_id"], "source_release_id": scheduled["source_release_id"]}
    return {**core, "feature_record_id": sha256_json(core)}


def build_outcome(rows: Sequence[Mapping], opportunity: Mapping, *, expected_contract: str, as_of_ns: int, calendar: VerifiedExchangeCalendar | None = None) -> dict:
    result = build_directional_path_outcome(rows, opportunity, expected_contract=expected_contract, as_of_ns=as_of_ns, calendar=calendar)
    result = {**result, "mechanism_id": design()["mechanism_id"], "fit_target": None, "diagnostics": None}
    if result["status"] == "MATURED":
        t = result["targets"]
        validate_labels(*(np.array([t[name]], dtype=np.float64) for name in ("terminal_change_ticks", "maximum_upside_excursion_ticks", "maximum_downside_excursion_ticks")))
        result["fit_target"] = t["terminal_change_ticks"]
        result["diagnostics"] = {name: t[name] for name in ("maximum_upside_excursion_ticks", "maximum_downside_excursion_ticks")}
    result["predecessor_outcome_id"] = result.pop("outcome_id")
    return {**result, "outcome_id": sha256_json(result)}


def assemble_opportunities(rows: Sequence[Mapping], opportunities: Sequence[Mapping], *, as_of_ns: int,
                           calendar: VerifiedExchangeCalendar | None = None) -> dict:
    """Retain feature/prediction eligibility even when later outcomes are invalid.

    Coverage is derived from the declared grid, never from a candidate's trades.
    This is a pure construction helper; incomplete labels cannot enter fitting.
    """
    from .fixed_hour_terminal_evaluation import coverage_report

    ids = [o.get("opportunity_id") for o in opportunities]
    if not ids or len(set(ids)) != len(ids):
        raise IntegrityError("opportunity batch is empty or duplicated")
    intervals = {}
    for opportunity in opportunities:
        scheduled = validate_globex_opportunity(opportunity, calendar=calendar)
        key = tuple(scheduled[k] for k in ("market", "trade_date", "calendar_release_id", "open_interval_start_ns", "open_interval_end_ns"))
        intervals.setdefault(key, []).append(scheduled["entry_bar_start_ns"])
    for key, entries in intervals.items():
        expected = list(range(key[-2] + 22 * MINUTE, key[-1] - 61 * MINUTE + 1, 60 * MINUTE))
        if sorted(entries) != expected:
            raise IntegrityError("declared interval grid is incomplete; coverage denominator cannot shrink")
    records, insufficient, valid_feature, invalid_outcome, errors = [], 0, 0, 0, 0
    for opportunity in opportunities:
        # A forged schedule is a structural error, not a silently excluded row.
        scheduled = validate_globex_opportunity(opportunity, calendar=calendar)
        try:
            feature = build_feature_record(rows, scheduled, calendar=calendar)
        except IntegrityError as exc:
            records.append({"opportunity_id": scheduled["opportunity_id"], "status": "INVALID_FEATURE", "reason": str(exc)})
            errors += 1
            continue
        if feature["status"] == "INSUFFICIENT_HISTORY":
            insufficient += 1
            records.append({"feature": feature, "outcome": None})
            continue
        valid_feature += 1
        try:
            outcome = build_outcome(rows, scheduled, expected_contract=feature["audit"]["actual_contract"], as_of_ns=as_of_ns, calendar=calendar)
        except IntegrityError as exc:
            outcome = {"status": "INVALID_OUTCOME", "reason": str(exc), "fit_target": None}
            errors += 1
        if outcome["status"] != "MATURED":
            invalid_outcome += 1
        records.append({"feature": feature, "outcome": outcome})
    return {"mechanism_id": mechanism_id(), "records": records,
            "coverage": coverage_report(scheduled=len(opportunities), insufficient_history=insufficient,
                valid_feature=valid_feature, invalid_outcome=invalid_outcome, quality_errors=errors), "authority": AUTHORITY}


@dataclass(frozen=True)
class TerminalDataset:
    """Fitting interface deliberately cannot carry excursion diagnostics."""
    market: str
    sample_ids: tuple[str, ...]
    trade_dates: tuple[str, ...]
    features: np.ndarray
    terminal_ticks: np.ndarray
    stress_cost_ticks: np.ndarray
    global_dates: tuple[str, ...]
    tick_values_usd: np.ndarray

    def __post_init__(self) -> None:
        ids = require_unique_ascii_ids(self.sample_ids, name="opportunity IDs")
        if self.market not in MARKETS or len(ids) != len(self.trade_dates):
            raise IntegrityError("terminal dataset identity mismatch")
        try:
            parsed = tuple(date.fromisoformat(d) for d in self.global_dates)
        except (ValueError, TypeError) as exc:
            raise IntegrityError("invalid global dates") from exc
        if (not parsed or tuple(sorted(set(parsed))) != parsed or self.trade_dates != tuple(sorted(self.trade_dates))
            or set(self.trade_dates) != set(self.global_dates) or any(not date(2018, 1, 1) <= d < date(2023, 1, 1) for d in parsed)):
            raise IntegrityError("require complete ordered common development dates")
        for name, shape in (("features", (len(ids), len(FEATURE_NAMES))), ("terminal_ticks", (len(ids),)), ("stress_cost_ticks", (len(ids),)), ("tick_values_usd", (len(ids),))):
            array = finite_float64(getattr(self, name), name=name, ndim=len(shape))
            if array.shape != shape or (name == "stress_cost_ticks" and np.any(array <= 0)):
                raise IntegrityError("terminal arrays are not aligned")
            copied = array.copy()
            copied.flags.writeable = False
            object.__setattr__(self, name, copied)
        if np.any(self.terminal_ticks != np.rint(self.terminal_ticks)):
            raise IntegrityError("terminal fitting labels must be integral ticks")
        if not np.allclose(self.stress_cost_ticks, provisional_costs(self.market, self.tick_values_usd)["stress"], rtol=0, atol=1e-12):
            raise IntegrityError("terminal dataset must use the frozen stress floor")


def retained_columns(fit: np.ndarray, names: Sequence[str]) -> list[int]:
    finite_float64(fit, name="fit features", ndim=2)
    columns: list[int] = []
    for name in names:
        col = FEATURE_NAMES.index(name)
        if np.all(fit[:, col] == fit[0, col]) or any(np.array_equal(fit[:, col], fit[:, other]) for other in columns):
            continue
        columns.append(col)
    return columns


def fitted_model(fit: np.ndarray, target: np.ndarray, candidate: Mapping) -> dict:
    columns = retained_columns(fit, candidate["feature_names"])
    means = np.mean(fit[:, columns], axis=0)
    scales = np.std(fit[:, columns], axis=0)
    centered = (fit[:, columns] - means) / scales
    intercept = float(np.mean(target))
    weights = np.linalg.solve(centered.T @ centered + candidate["penalty"] * np.eye(len(columns)), centered.T @ (target - intercept)) if columns else np.empty(0)
    if not np.isfinite(weights).all() or not np.isfinite(intercept):
        raise IntegrityError("nonfinite terminal fit")
    core = {"columns": columns, "feature_names": [FEATURE_NAMES[i] for i in columns],
            "means": means.tolist(), "scales": scales.tolist(), "weights": weights.tolist(),
            "intercept_ticks": intercept, "candidate_id": candidate["candidate_id"], "target_unit": "ticks"}
    return {**core, "fitted_model_id": sha256_json(core)}


def apply_fitted(model: Mapping, features: np.ndarray) -> np.ndarray:
    finite_float64(features, name="prediction features", ndim=2)
    if model.get("fitted_model_id") != sha256_json({k: v for k, v in model.items() if k != "fitted_model_id"}):
        raise IntegrityError("fitted model identity drifted")
    if not model["columns"]:
        return np.full(len(features), model["intercept_ticks"])
    result = model["intercept_ticks"] + ((features[:, model["columns"]] - np.array(model["means"])) / np.array(model["scales"])) @ np.array(model["weights"])
    if not np.isfinite(result).all():
        raise IntegrityError("nonfinite terminal prediction")
    return result


def fit_predict(fit: np.ndarray, target: np.ndarray, audit: np.ndarray, candidate: Mapping) -> tuple[np.ndarray, list[int]]:
    # Use the predecessor's numerical kernel for repeated inner fits. The final
    # outer refit additionally serializes the same transform and coefficients.
    columns = retained_columns(fit, candidate["feature_names"])
    if not columns:
        return np.full(len(audit), float(np.mean(target))), columns
    return _ridge_predict(fit[:, columns], target, audit[:, columns], penalty=candidate["penalty"]), columns


def daily_means(values: np.ndarray, dates: Sequence[str]) -> np.ndarray:
    if len(values) != len(dates):
        raise IntegrityError("scores and dates mismatch")
    return np.array([np.mean(values[np.array(dates) == d]) for d in sorted(set(dates))], dtype=np.float64)


def stress_score(predicted: np.ndarray, actual: np.ndarray, costs: np.ndarray, dates: Sequence[str]) -> float:
    d = terminal_directions(predicted, costs)
    return float(np.mean(daily_means(d * actual - np.abs(d) * costs, dates)))


def choose_candidate(scores: Sequence[Mapping]) -> Mapping:
    if not scores or any(not np.isfinite(s["score"]) for s in scores):
        raise IntegrityError("invalid candidate scores")
    best = max(s["score"] for s in scores)
    comparable = [s for s in scores if s["score"] >= best - 0.01]
    return min(comparable, key=lambda s: (s["retained_count"], -s["penalty"], s["candidate_id"]))


def fold_schedule(dataset: TerminalDataset):
    dates = dataset.global_dates
    if len(dates) < 1010:
        raise IntegrityError("eight-fold WFA needs 1010 complete dates")
    ordinal = {d: i for i, d in enumerate(dates)}
    sessions = np.array([ordinal[d] for d in dataset.trade_dates], dtype=np.int64)
    samples = TemporalSamples(sessions, sessions, sessions + 1, sessions + 1)
    outer = [SessionWindow(len(dates) - 504 + i * 63, len(dates) - 504 + (i + 1) * 63) for i in range(8)]
    inner = [[SessionWindow(w.start - 1 - 252 + i * 63, w.start - 1 - 252 + (i + 1) * 63) for i in range(4)] for w in outer]
    splits = nested_chronological_splits(samples, outer, inner, session_embargo=1, minimum_fit_samples=252, minimum_audit_samples=63)
    for split in splits:
        if len(set(sessions[split.fit_indices])) < 505 or len(set(sessions[split.audit_indices])) != 63:
            raise IntegrityError("outer complete-date minima failed")
        for f in split.inner_folds:
            if len(set(sessions[f.fit_indices])) < 252 or len(set(sessions[f.audit_indices])) != 63:
                raise IntegrityError("inner complete-date minima failed")
    return splits


def run_market_wfa(dataset: TerminalDataset) -> dict:
    """Pure nested selection/refit/prediction; outer outcomes never enter scoring."""
    if not isinstance(dataset, TerminalDataset):
        raise IntegrityError("validated terminal dataset required")
    output = []
    for index, split in enumerate(fold_schedule(dataset)):
        scores = []
        for candidate in candidate_definitions():
            folds = []
            for inner in split.inner_folds:
                f, a = inner.fit_indices, inner.audit_indices
                pred, columns = fit_predict(dataset.features[f], dataset.terminal_ticks[f], dataset.features[a], candidate)
                folds.append({"score": stress_score(pred, dataset.terminal_ticks[a], dataset.stress_cost_ticks[a], tuple(dataset.trade_dates[i] for i in a)),
                              "columns": columns, "retained_count": len(columns)})
            scores.append({**candidate, "folds": folds, "score": float(np.median([s["score"] for s in folds])),
                           "retained_count": float(np.mean([s["retained_count"] for s in folds]))})
        chosen = choose_candidate(scores)
        f, a = split.fit_indices, split.audit_indices
        model = fitted_model(dataset.features[f], dataset.terminal_ticks[f], chosen)
        prediction, columns = apply_fitted(model, dataset.features[a]), model["columns"]
        winners = [choose_candidate([{**s, **s["folds"][i]} for s in scores]) for i in range(4)]
        feature_counts = Counter(FEATURE_NAMES[col] for w in winners for col in w["columns"])
        family_counts = Counter(family for w in winners for family in set(FAMILIES[FEATURE_NAMES[col]] for col in w["columns"]))
        correlations = {name: (float(np.corrcoef(dataset.features[f, col], dataset.terminal_ticks[f])[0, 1])
                              if np.std(dataset.features[f, col]) > 0 and np.std(dataset.terminal_ticks[f]) > 0 else None)
                        for col, name in enumerate(FEATURE_NAMES)}
        audit_dates = sorted(set(dataset.trade_dates[i] for i in a))
        baseline_direction = float(np.sign(np.mean(dataset.terminal_ticks[f])))
        previous_direction = np.sign(dataset.features[a, FEATURE_NAMES.index("close_change_ticks_1")])
        baseline_signals = {"flat_no_trade": np.zeros(len(a)), "fold_local_unconditional_direction": np.full(len(a), baseline_direction),
                            "previous_bar_sign_momentum": previous_direction, "previous_bar_sign_reversal": -previous_direction,
                            "always_long": np.ones(len(a)), "always_short": -np.ones(len(a))}
        output.append({"outer_fold_id": f"outer-{index}", "audit_indices": a.tolist(),
            "fit_sample_ids": [dataset.sample_ids[i] for i in f], "audit_sample_ids": [dataset.sample_ids[i] for i in a],
            "fit_trade_dates": sorted(set(dataset.trade_dates[i] for i in f)), "audit_trade_dates": audit_dates,
            "embargo_trade_date": dataset.global_dates[dataset.global_dates.index(audit_dates[0]) - 1],
            "inner_manifests": [{"fit_sample_ids": [dataset.sample_ids[i] for i in item.fit_indices],
                "audit_sample_ids": [dataset.sample_ids[i] for i in item.audit_indices],
                "fit_trade_dates": sorted(set(dataset.trade_dates[i] for i in item.fit_indices)),
                "audit_trade_dates": sorted(set(dataset.trade_dates[i] for i in item.audit_indices)),
                "embargo_trade_date": dataset.global_dates[item.validation_window.start - 1]} for item in split.inner_folds],
            "candidate_id": chosen["candidate_id"], "retained_features": [FEATURE_NAMES[i] for i in columns], "fitted_model": model,
            "predictions": prediction.tolist(), "directions": terminal_directions(prediction, dataset.stress_cost_ticks[a]).tolist(),
            "baseline_directions": {name: v.tolist() for name, v in baseline_signals.items()},
            "candidate_scores": scores, "training_correlations_diagnostic_only": correlations,
            "stability": {"pack_frequency": dict(Counter(w["pack"] for w in winners)), "feature_frequency": dict(feature_counts),
                          "family_frequency": dict(family_counts), "inner_folds": 4, "score_std": float(np.std([s["score"] for s in chosen["folds"]])), "promotion_gate": False}})
    core = {"mechanism_id": design()["mechanism_id"], "market": dataset.market,
            "global_dates_id": sha256_json(list(dataset.global_dates)), "outer_folds": output,
            "state": "SYNTHETIC_MECHANICS_COMPLETE_NOT_RESEARCH_AUTHORITY", "authority": AUTHORITY}
    return {**core, "run_id": sha256_json(core)}
