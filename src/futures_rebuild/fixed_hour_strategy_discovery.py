"""Inactive strategy discovery; isolated from hash-bound predecessor experiments.

Unchanged splitting, costs, validation and numerical helpers are reused directly.
Width/identity-dependent fitting and selection follow the frozen comparison code.
No real-data reader or execution authority is provided.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import date
from math import ceil

import numpy as np

from . import fixed_hour_terminal_successor as terminal
from . import fixed_hour_model_comparison as predecessor
from .canonical import sha256_json
from .errors import IntegrityError
from .exchange_calendar import VerifiedExchangeCalendar
from .fixed_hour_outcome_foundation import MINUTE, validate_globex_opportunity
from .fixed_hour_feature_expansion import _ridge_predict
from .research.contracts import finite_float64, require_unique_ascii_ids
from .fixed_hour_terminal_successor import (
    AUTHORITY, MARKETS, provisional_costs, terminal_directions, validate_labels,
    daily_means, stress_score, fold_schedule, apply_fitted,
)
from .fixed_hour_model_comparison import (
    VERSIONS, _xgb, LeafSupportError, check_leaf_dates, choose_candidate,
)

NEW_FEATURES = (
    "normalized_momentum_5", "normalized_momentum_20", "normalized_momentum_60",
    "normalized_stretch_60", "signed_squared_change_asymmetry_60",
)
FEATURE_NAMES = (*terminal.FEATURE_NAMES, *NEW_FEATURES)
PACKS = {**terminal.PACKS, "context_discovery": (*terminal.PACKS["context"], *NEW_FEATURES)}
FAMILIES = {**terminal.FAMILIES, **dict.fromkeys(NEW_FEATURES[:3], "normalized_momentum"),
            NEW_FEATURES[3]: "normalized_stretch", NEW_FEATURES[4]: "squared_change_asymmetry"}


def candidate_definitions():
    return tuple({"candidate_id": f"{pack}:ridge={penalty:g}", "pack": pack,
                  "feature_names": list(names), "penalty": penalty, "family": "ridge"}
                 for pack, names in PACKS.items() for penalty in terminal.RIDGE_PENALTIES) + tuple(
        {"candidate_id": f"{pack}:xgboost:depth={depth}", "pack": pack,
         "feature_names": list(names), "family": "xgboost", "max_depth": depth}
        for pack, names in PACKS.items() for depth in (2, 3))


def design():
    core = predecessor.design()
    previous = core.pop("mechanism_id")
    core.update(schema_version="fixed_hour_strategy_discovery/1.0.0",
        predecessor_mechanism_id=previous, candidates=list(candidate_definitions()),
        feature_names=list(FEATURE_NAMES), configurations_per_market=50,
        total_market_configurations=200)
    core["mechanism_card"]["hypothesis"] += (
        " Normalized momentum, stretch and squared-change asymmetry may add predictive information."
        " This is an unproven bundle hypothesis, not individual attribution or a semivariance replication.")
    core["phase_5"].update(selection="50_FIXED_CANDIDATES_INNER_WFA_ONLY",
        new_pack="context_discovery", matrix_threads=1, additional_formulas={
            "d": "60_INTEGER_CLOSE_DIFFERENCES_DIV_TICK", "s": "MAX(POPULATION_STD(d),1)",
            "normalized_momentum_h": "(p[60]-p[60-h])/tick/s/sqrt(h);h=5,20,60",
            "normalized_stretch_60": "SUM(p[60]-p[i] for i=0..60)/61/tick/s",
            "signed_squared_change_asymmetry_60": "(SUM_POSITIVE_d_SQUARED-SUM_NEGATIVE_d_SQUARED)/SUM_d_SQUARED;ZERO_IF_FLAT"},
        hypothesis_status="UNPROVEN_BUNDLE_NO_INDIVIDUAL_FEATURE_ATTRIBUTION")
    core["phase_6"]["added_value"].update(promotion_gate=False,
        identical_stream="NO_ADDED_VALUE_P_EQUALS_ONE", malformed_evidence="FAIL_CLOSED")
    core["phase_6"].update(objective="ECONOMIC_QUALIFICATION_OF_INNER_SELECTED_PROCEDURE",
        ridge_winner_eligible=True, multiplicity_family="ALL_28_INCLUDING_DIAGNOSTIC_RIDGE",
        cost_diagnostics="POOLED_PER_TRADE_USD;FROZEN_TRADES;NO_SELECTION_OR_PROMOTION_USE")
    return {**core, "mechanism_id": sha256_json(core)}


def mechanism_id():
    return design()["mechanism_id"]


def discovery_features(closes: Sequence[int], tick_size: int) -> dict[str, float]:
    if (len(closes) != 61 or type(tick_size) is not int or tick_size <= 0
        or any(type(p) is not int or p % tick_size for p in closes)):
        raise IntegrityError("discovery requires 61 integer tick-aligned closes and positive tick")
    try:
        with np.errstate(over="raise", invalid="raise", divide="raise"):
            d = np.array([(b-a)/tick_size for a, b in zip(closes, closes[1:])], dtype=np.float64)
            s = max(float(np.std(d, ddof=0)), 1.)
            momentum = [((closes[-1]-closes[-1-h])/tick_size)/(s*np.sqrt(h)) for h in (5,20,60)]
            stretch = (sum(closes[-1]-p for p in closes)/tick_size)/61/s
            squared = d*d
            total = float(np.sum(squared))
            asymmetry = float((np.sum(squared[d>0])-np.sum(squared[d<0]))/total) if total else 0.
            values = [*momentum, stretch, asymmetry]
    except (OverflowError, FloatingPointError) as exc:
        raise IntegrityError("discovery feature arithmetic overflow") from exc
    if not np.isfinite(values).all() or not np.isfinite(d).all():
        raise IntegrityError("nonfinite discovery features")
    return dict(zip(NEW_FEATURES, values, strict=True))


def build_feature_record(rows, opportunity, *, calendar=None):
    record = terminal.build_feature_record(rows, opportunity, calendar=calendar)
    record = {**record, "mechanism_id": mechanism_id()}
    if record["status"] == "INSUFFICIENT_HISTORY":
        return record
    audit = record["audit"]
    window = sorted((r for r in rows if audit["first_bar_start_ns"] <= r["bar_start_ns"] < audit["last_bar_end_ns"]),
                    key=lambda r: r["bar_start_ns"])
    values = discovery_features([r["close_nano"] for r in window], audit["min_price_increment_nano"])
    record["feature_values"] = {**record["feature_values"], **values}
    record["audit"] = {**audit, "feature_names": list(FEATURE_NAMES)}
    record.pop("feature_record_id")
    return {**record, "feature_record_id": sha256_json(record)}


def build_outcome(rows, opportunity, *, expected_contract, as_of_ns, calendar=None):
    result = terminal.build_outcome(rows, opportunity, expected_contract=expected_contract, as_of_ns=as_of_ns, calendar=calendar)
    result = {**result, "mechanism_id": mechanism_id(), "terminal_outcome_id": result["outcome_id"]}
    result.pop("outcome_id")
    return {**result, "outcome_id": sha256_json(result)}


@dataclass(frozen=True)
class TerminalDataset(terminal.TerminalDataset):
    def __post_init__(self):
        x = finite_float64(self.features, name="discovery features", ndim=2)
        if x.shape != (len(self.sample_ids), len(FEATURE_NAMES)):
            raise IntegrityError("discovery feature width mismatch")
        # Delegate all unchanged identity, date, cost and target checks.
        checked = terminal.TerminalDataset(self.market, self.sample_ids, self.trade_dates,
            x[:, :len(terminal.FEATURE_NAMES)], self.terminal_ticks, self.stress_cost_ticks,
            self.global_dates, self.tick_values_usd)
        for field in fields(checked):
            if field.name != "features":
                object.__setattr__(self, field.name, getattr(checked, field.name))
        copied = x.copy(); copied.flags.writeable = False
        object.__setattr__(self, "features", copied)


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


def fit_predict(fit: np.ndarray, target: np.ndarray, audit: np.ndarray, candidate: Mapping) -> tuple[np.ndarray, list[int]]:
    # Use the predecessor's numerical kernel for repeated inner fits. The final
    # outer refit additionally serializes the same transform and coefficients.
    columns = retained_columns(fit, candidate["feature_names"])
    if not columns:
        return np.full(len(audit), float(np.mean(target))), columns
    return _ridge_predict(fit[:, columns], target, audit[:, columns], penalty=candidate["penalty"]), columns


def _candidate(candidate):
    match = next((c for c in candidate_definitions() if c["candidate_id"] == candidate.get("candidate_id")), None)
    if match is None or any(candidate.get(k) != v for k, v in match.items()):
        raise IntegrityError("candidate differs from frozen search")
    return match


def fit_model(features, target, sample_ids, trade_dates, candidate):
    """Accept training data only; no validation labels or excursions interface."""
    c = _candidate(candidate)
    x = finite_float64(features, name="training features", ndim=2)
    y = finite_float64(target, name="training terminal ticks", ndim=1)
    ids = require_unique_ascii_ids(sample_ids, name="training opportunity IDs")
    if x.shape != (len(y), len(FEATURE_NAMES)) or not len(y) or len(ids) != len(y) or len(trade_dates) != len(y):
        raise IntegrityError("unaligned training inputs")
    if any(not isinstance(d, str) for d in trade_dates) or tuple(trade_dates) != tuple(sorted(trade_dates)):
        raise IntegrityError("training dates must be ordered strings")
    try:
        if any(not date(2018, 1, 1) <= date.fromisoformat(d) < date(2023, 1, 1) for d in trade_dates):
            raise ValueError("outside development period")
    except ValueError as exc:
        raise IntegrityError("invalid training dates") from exc
    columns = retained_columns(x, c["feature_names"])
    core = {"mechanism_id": design()["mechanism_id"], "candidate": c,
        "fit_sample_ids": list(ids), "fit_trade_dates": list(trade_dates),
        "columns": columns, "feature_names": [FEATURE_NAMES[i] for i in columns],
        "training_mean_ticks": float(np.mean(y)), "dependency_versions": dict(VERSIONS)}
    if c["family"] == "ridge":
        core["ridge"] = fitted_model(x, y, c)
    elif columns:
        xgb = _xgb()
        params = dict(design()["phase_5"]["xgboost"])
        for name in ("rounds", "minimum_leaf_dates", "early_stopping", "seed_search"):
            params.pop(name)
        params.update(max_depth=c["max_depth"], base_score=float(np.mean(y)),
                      min_child_weight=max(50, ceil(.05 * len(y))))
        matrix = xgb.DMatrix(x[:, columns], label=y, nthread=1)
        booster = xgb.train(params, matrix, num_boost_round=100)
        core.update(parameters=params, minimum_observed_leaf_dates=check_leaf_dates(
            booster.predict(matrix, pred_leaf=True), trade_dates),
            model_json=bytes(booster.save_raw(raw_format="json")).decode("utf-8"))
    else:
        _xgb()
        core["intercept_only"] = True
    return {**core, "model_id": sha256_json(core)}


def predict(model, features):
    if model.get("model_id") != sha256_json({k: v for k, v in model.items() if k != "model_id"}):
        raise IntegrityError("frozen model identity drift")
    if model["mechanism_id"] != design()["mechanism_id"] or model["dependency_versions"] != VERSIONS:
        raise IntegrityError("frozen model mechanism/version mismatch")
    c = _candidate(model["candidate"])
    x = finite_float64(features, name="prediction features", ndim=2)
    if x.shape[1] != len(FEATURE_NAMES):
        raise IntegrityError("prediction feature width mismatch")
    if c["family"] == "ridge":
        result = apply_fitted(model["ridge"], x)
    elif not model["columns"]:
        _xgb()
        result = np.full(len(x), model["training_mean_ticks"])
    else:
        xgb = _xgb()
        booster = xgb.Booster(params={"nthread": 1, "device": "cpu"})
        booster.load_model(bytearray(model["model_json"], "utf-8"))
        result = booster.predict(xgb.DMatrix(x[:, model["columns"]], nthread=1)).astype(np.float64)
    return finite_float64(result, name="terminal predictions", ndim=1)


def run_market_wfa(dataset):
    if not isinstance(dataset, TerminalDataset):
        raise IntegrityError("validated terminal dataset required")
    _xgb()
    output = []
    for index, split in enumerate(fold_schedule(dataset)):
        scores = []
        for c in candidate_definitions():
            folds = []
            try:
                for inner in split.inner_folds:
                    f, a = inner.fit_indices, inner.audit_indices
                    if c["family"] == "ridge":
                        p, columns = fit_predict(dataset.features[f], dataset.terminal_ticks[f], dataset.features[a], c)
                    else:
                        model = fit_model(dataset.features[f], dataset.terminal_ticks[f],
                            tuple(dataset.sample_ids[i] for i in f), tuple(dataset.trade_dates[i] for i in f), c)
                        p, columns = predict(model, dataset.features[a]), model["columns"]
                    folds.append({"score": stress_score(p, dataset.terminal_ticks[a], dataset.stress_cost_ticks[a],
                        tuple(dataset.trade_dates[i] for i in a)), "columns": columns, "retained_count": len(columns)})
                scores.append({**c, "status": "VALID", "folds": folds,
                    "score": float(np.median([s["score"] for s in folds])),
                    "retained_count": float(np.mean([s["retained_count"] for s in folds]))})
            except LeafSupportError as exc:
                scores.append({**c, "status": "INVALID_LEAF_SUPPORT", "reason": str(exc), "folds": folds})
        chosen = choose_candidate(scores)
        ridge = choose_candidate([s for s in scores if s["family"] == "ridge"])
        f, a = split.fit_indices, split.audit_indices
        streams = {}
        models = {}
        for name, winner in (("combined", chosen), ("ridge_only", ridge)):
            cid = winner["candidate_id"]
            if cid not in models:
                models[cid] = fit_model(dataset.features[f], dataset.terminal_ticks[f],
                    tuple(dataset.sample_ids[i] for i in f), tuple(dataset.trade_dates[i] for i in f), winner)
            model = models[cid]
            p = predict(model, dataset.features[a])
            streams[name] = {"candidate_id": cid, "fitted_model": model, "predictions": p.tolist(),
                "directions": terminal_directions(p, dataset.stress_cost_ticks[a]).tolist()}
        valid = [s for s in scores if s["status"] == "VALID"]
        winners = [choose_candidate([{**s, **s["folds"][i]} for s in valid]) for i in range(4)]
        previous = np.sign(dataset.features[a, FEATURE_NAMES.index("close_change_ticks_1")])
        signals = {"flat_no_trade": np.zeros(len(a)), "fold_local_unconditional_direction": np.full(len(a), np.sign(np.mean(dataset.terminal_ticks[f]))),
            "previous_bar_sign_momentum": previous, "previous_bar_sign_reversal": -previous,
            "always_long": np.ones(len(a)), "always_short": -np.ones(len(a))}
        output.append({"outer_fold_id": f"outer-{index}", "audit_indices": a.tolist(),
            "fit_sample_ids": [dataset.sample_ids[i] for i in f], "audit_sample_ids": [dataset.sample_ids[i] for i in a],
            "fit_trade_dates": sorted(set(dataset.trade_dates[i] for i in f)),
            "audit_trade_dates": sorted(set(dataset.trade_dates[i] for i in a)),
            "inner_manifests": [{"fit_sample_ids": [dataset.sample_ids[i] for i in z.fit_indices],
                "audit_sample_ids": [dataset.sample_ids[i] for i in z.audit_indices],
                "embargo_trade_date": dataset.global_dates[z.validation_window.start - 1]} for z in split.inner_folds],
            "streams": streams, "candidate_scores": scores,
            "baseline_directions": {k: v.tolist() for k, v in signals.items()},
            "stability": {"model_frequency": dict(Counter(w["family"] for w in winners)),
                "pack_frequency": dict(Counter(w["pack"] for w in winners)),
                "feature_frequency": dict(Counter(FEATURE_NAMES[col] for w in winners for col in w["columns"])),
                "family_frequency": dict(Counter(fam for w in winners for fam in {FAMILIES[FEATURE_NAMES[col]] for col in w["columns"]})),
                "score_std": float(np.std([z["score"] for z in chosen["folds"]])), "promotion_gate": False}})
    core = {"mechanism_id": design()["mechanism_id"], "market": dataset.market,
        "global_dates_id": sha256_json(list(dataset.global_dates)), "outer_folds": output,
        "state": "SYNTHETIC_MECHANICS_COMPLETE_NOT_RESEARCH_AUTHORITY", "authority": AUTHORITY}
    return {**core, "run_id": sha256_json(core)}
