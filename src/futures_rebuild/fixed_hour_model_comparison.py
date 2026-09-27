"""Inactive, bounded model comparison. No data access or execution authority."""
from __future__ import annotations

from collections import Counter
from datetime import date
from importlib.metadata import version
from math import ceil

import numpy as np

from . import fixed_hour_terminal_successor as prior
from .canonical import sha256_json
from .errors import IntegrityError
from .research.contracts import finite_float64, require_unique_ascii_ids

VERSIONS = {"xgboost": "3.0.5", "scipy": "1.17.1", "numpy": "2.4.4"}


def candidate_definitions():
    return tuple({**c, "family": "ridge"} for c in prior.candidate_definitions()) + tuple(
        {"candidate_id": f"{pack}:xgboost:depth={depth}", "pack": pack,
         "feature_names": list(names), "family": "xgboost", "max_depth": depth}
        for pack, names in prior.PACKS.items() for depth in (2, 3))


def design():
    core = prior.design()
    core.pop("mechanism_id")
    core.update(schema_version="fixed_hour_model_comparison/1.0.0",
                predecessor_mechanism_id=prior.mechanism_id(),
                candidates=list(candidate_definitions()), configurations_per_market=45,
                total_market_configurations=180, dependency_versions=VERSIONS)
    core["phase_5"].update(selection="45_FIXED_CANDIDATES_INNER_WFA_ONLY",
        transform="TRAIN_ONLY_CONSTANT_DUPLICATE_REMOVAL;RIDGE_STANDARDIZED;XGB_UNSCALED",
        tie_break=["ridge_before_xgboost", "mean_retained_column_count", "larger_ridge_penalty_or_shallower_tree", "candidate_id"],
        xgboost={"rounds": 100, "eta": .05, "max_bin": 64, "lambda": 10,
                 "alpha": 0, "gamma": 0, "subsample": 1, "colsample_bytree": 1,
                 "colsample_bylevel": 1, "colsample_bynode": 1, "seed": 62817,
                 "nthread": 1, "tree_method": "hist", "device": "cpu",
                 "objective": "reg:squarederror", "base_score": "TRAIN_TARGET_MEAN",
                 "min_child_weight": "max(50,ceil(.05*n_train))", "minimum_leaf_dates": 20,
                 "early_stopping": False, "seed_search": False})
    core["phase_6"]["added_value"] = {"benchmark": "INDEPENDENT_INNER_SELECTED_RIDGE",
        "mean_min_exclusive": .01, "hac_lower_min_exclusive": 0., "joint_p_max": .05,
        "declared_hypotheses": 28, "post_result_ridge_fallback": False}
    return {**core, "mechanism_id": sha256_json(core)}


def _xgb():
    try:
        if any(version(name) != expected for name, expected in VERSIONS.items()):
            raise IntegrityError("research-ml dependency version mismatch")
        import xgboost
        return xgboost
    except ImportError as exc:
        raise IntegrityError("approved research-ml dependencies unavailable") from exc


def _candidate(candidate):
    match = next((c for c in candidate_definitions() if c["candidate_id"] == candidate.get("candidate_id")), None)
    if match is None or any(candidate.get(k) != v for k, v in match.items()):
        raise IntegrityError("candidate differs from frozen search")
    return match


class LeafSupportError(IntegrityError):
    """Declared candidate is invalid; never change samples or relax support."""


def check_leaf_dates(leaves, dates):
    leaves = np.asarray(leaves)
    if leaves.ndim == 1:
        leaves = leaves[:, None]
    if leaves.ndim != 2 or len(leaves) != len(dates) or not np.isfinite(leaves).all():
        raise IntegrityError("invalid leaf assignments")
    dates = np.asarray(dates)
    minimum = min(len(set(dates[leaves[:, col] == leaf]))
                  for col in range(leaves.shape[1]) for leaf in np.unique(leaves[:, col]))
    if minimum < 20:
        raise LeafSupportError("fitted leaf has fewer than 20 distinct training dates")
    return minimum


def fit_model(features, target, sample_ids, trade_dates, candidate):
    """Accept training data only; no validation labels or excursions interface."""
    c = _candidate(candidate)
    x = finite_float64(features, name="training features", ndim=2)
    y = finite_float64(target, name="training terminal ticks", ndim=1)
    ids = require_unique_ascii_ids(sample_ids, name="training opportunity IDs")
    if x.shape != (len(y), len(prior.FEATURE_NAMES)) or not len(y) or len(ids) != len(y) or len(trade_dates) != len(y):
        raise IntegrityError("unaligned training inputs")
    if any(not isinstance(d, str) for d in trade_dates) or tuple(trade_dates) != tuple(sorted(trade_dates)):
        raise IntegrityError("training dates must be ordered strings")
    try:
        if any(not date(2018, 1, 1) <= date.fromisoformat(d) < date(2023, 1, 1) for d in trade_dates):
            raise ValueError("outside development period")
    except ValueError as exc:
        raise IntegrityError("invalid training dates") from exc
    columns = prior.retained_columns(x, c["feature_names"])
    core = {"mechanism_id": design()["mechanism_id"], "candidate": c,
        "fit_sample_ids": list(ids), "fit_trade_dates": list(trade_dates),
        "columns": columns, "feature_names": [prior.FEATURE_NAMES[i] for i in columns],
        "training_mean_ticks": float(np.mean(y)), "dependency_versions": dict(VERSIONS)}
    if c["family"] == "ridge":
        core["ridge"] = prior.fitted_model(x, y, c)
    elif columns:
        xgb = _xgb()
        params = dict(design()["phase_5"]["xgboost"])
        for name in ("rounds", "minimum_leaf_dates", "early_stopping", "seed_search"):
            params.pop(name)
        params.update(max_depth=c["max_depth"], base_score=float(np.mean(y)),
                      min_child_weight=max(50, ceil(.05 * len(y))))
        matrix = xgb.DMatrix(x[:, columns], label=y)
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
    if x.shape[1] != len(prior.FEATURE_NAMES):
        raise IntegrityError("prediction feature width mismatch")
    if c["family"] == "ridge":
        result = prior.apply_fitted(model["ridge"], x)
    elif not model["columns"]:
        _xgb()
        result = np.full(len(x), model["training_mean_ticks"])
    else:
        xgb = _xgb()
        booster = xgb.Booster(params={"nthread": 1, "device": "cpu"})
        booster.load_model(bytearray(model["model_json"], "utf-8"))
        result = booster.predict(xgb.DMatrix(x[:, model["columns"]])).astype(np.float64)
    return finite_float64(result, name="terminal predictions", ndim=1)


def choose_candidate(scores):
    valid = [s for s in scores if s.get("status", "VALID") == "VALID"]
    if not valid or any(not np.isfinite(s["score"]) for s in valid):
        raise IntegrityError("no valid finite candidates")
    best = max(s["score"] for s in valid)
    return min((s for s in valid if s["score"] >= best - .01), key=lambda s: (
        s["family"] != "ridge", s["retained_count"],
        -s["penalty"] if s["family"] == "ridge" else s["max_depth"], s["candidate_id"]))


def run_market_wfa(dataset):
    if not isinstance(dataset, prior.TerminalDataset):
        raise IntegrityError("validated terminal dataset required")
    _xgb()
    output = []
    for index, split in enumerate(prior.fold_schedule(dataset)):
        scores = []
        for c in candidate_definitions():
            folds = []
            try:
                for inner in split.inner_folds:
                    f, a = inner.fit_indices, inner.audit_indices
                    if c["family"] == "ridge":
                        p, columns = prior.fit_predict(dataset.features[f], dataset.terminal_ticks[f], dataset.features[a], c)
                    else:
                        model = fit_model(dataset.features[f], dataset.terminal_ticks[f],
                            tuple(dataset.sample_ids[i] for i in f), tuple(dataset.trade_dates[i] for i in f), c)
                        p, columns = predict(model, dataset.features[a]), model["columns"]
                    folds.append({"score": prior.stress_score(p, dataset.terminal_ticks[a], dataset.stress_cost_ticks[a],
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
                "directions": prior.terminal_directions(p, dataset.stress_cost_ticks[a]).tolist()}
        valid = [s for s in scores if s["status"] == "VALID"]
        winners = [choose_candidate([{**s, **s["folds"][i]} for s in valid]) for i in range(4)]
        previous = np.sign(dataset.features[a, prior.FEATURE_NAMES.index("close_change_ticks_1")])
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
                "feature_frequency": dict(Counter(prior.FEATURE_NAMES[col] for w in winners for col in w["columns"])),
                "family_frequency": dict(Counter(fam for w in winners for fam in {prior.FAMILIES[prior.FEATURE_NAMES[col]] for col in w["columns"]})),
                "score_std": float(np.std([z["score"] for z in chosen["folds"]])), "promotion_gate": False}})
    core = {"mechanism_id": design()["mechanism_id"], "market": dataset.market,
        "global_dates_id": sha256_json(list(dataset.global_dates)), "outer_folds": output,
        "state": "SYNTHETIC_MECHANICS_COMPLETE_NOT_RESEARCH_AUTHORITY", "authority": prior.AUTHORITY}
    return {**core, "run_id": sha256_json(core)}
