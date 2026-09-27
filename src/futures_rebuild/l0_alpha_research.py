"""Nested trading-result selection and deterministic development model custody."""
import numpy as np

from . import l0_alpha_candidate as c
from .l0_alpha_replay import replay, fixed_trade_sensitivity
from .canonical import sha256_json
from .errors import IntegrityError


class MarketRunFailure(IntegrityError):
    def __init__(self, message, partial_run):
        super().__init__(message)
        self.partial_run = partial_run


def fit_model(dataset, indices, candidate, *, fit_kind="inner"):
    declared = next((v for v in c.candidate_definitions() if v["candidate_id"] == candidate.get("candidate_id")), None)
    if declared is None or any(candidate.get(k) != v for k, v in declared.items()):
        raise IntegrityError("candidate differs from the ten frozen profiles")
    dates = c.old.training_support(dataset, indices, fit_kind=fit_kind)
    d = dataset.terminal_data
    x = c.old.finite_float64(d.features[indices], name="training features", ndim=2)
    y = c.old.finite_float64(d.terminal_ticks[indices], name="training labels", ndim=1)
    w = c.old.training_weights(dates)
    columns = []
    for name in declared["feature_names"]:
        i = c.FEATURES.index(name)
        if np.ptp(x[:, i]) and not any(np.array_equal(x[:, i], x[:, j]) for j in columns):
            columns.append(i)
    mean = float(w @ y)
    model = {"mechanism_id": c.mechanism_id(), "candidate": declared, "columns": columns,
        "feature_names": list(c.FEATURES), "fit_kind": fit_kind, "training_mean_ticks": mean,
        "observed_training_dates": len(set(dates)), "training_rows": len(indices),
        "training_id": sha256_json([x.tolist(), y.tolist(), dates]),
        "fit_ids_sha256": sha256_json([d.sample_ids[i] for i in indices]),
        "latest_label_ns": max(dataset.outcomes[dataset.fit_positions[i]]["known_at_ns"] for i in indices),
        "evidence_id": dataset._evidence_id, "dependency_versions": c.old.comparison.VERSIONS}
    if declared["family"] == "ridge":
        means = w @ x[:, columns]
        scales = np.sqrt(w @ ((x[:, columns] - means) ** 2))
        z = (x[:, columns] - means) / scales
        weights = np.linalg.solve(z.T @ (w[:, None] * z) + declared["penalty"] * np.eye(len(columns)),
                                  z.T @ (w * (y - mean))) if columns else np.empty(0)
        model.update(means=means.tolist(), scales=scales.tolist(), weights=weights.tolist())
    elif columns:
        xgb = c.old.comparison._xgb()
        params = dict(c.design()["xgboost"])
        for key in ("rounds", "minimum_leaf_dates", "early_stopping", "seed_search"):
            params.pop(key)
        params.update(max_depth=declared["max_depth"], base_score=mean)
        matrix = xgb.DMatrix(x[:, columns], label=y, weight=w)
        booster = xgb.train(params, matrix, num_boost_round=100)
        model.update(parameters=params, model_json=bytes(booster.save_raw(raw_format="json")).decode())
        # Retain the fitted artifact even if the declared support check rejects it.
        try:
            model["minimum_leaf_dates"] = c.old.comparison.check_leaf_dates(booster.predict(matrix, pred_leaf=True), dates)
        except c.old.comparison.LeafSupportError as exc:
            exc.fitted_model = c.bind(model, "model_id")
            raise
    else:
        c.old.comparison._xgb()
    return c.bind(model, "model_id")


def predict(model, features):
    c.identity(model, "model_id")
    if (model["mechanism_id"] != c.mechanism_id() or model["feature_names"] != list(c.FEATURES)
            or model["dependency_versions"] != c.old.comparison.VERSIONS):
        raise IntegrityError("model mechanism, feature order or dependency versions differ")
    x = c.old.finite_float64(np.asarray(features, dtype=float), name="prediction features", ndim=2)
    if x.shape[1] != len(c.FEATURES):
        raise IntegrityError("prediction feature width differs")
    columns = model["columns"]
    if not len(x):
        return np.empty(0)
    if model["candidate"]["family"] == "ridge":
        values = (x[:, columns] - np.array(model["means"])) / np.array(model["scales"])
        result = values @ np.array(model["weights"]) + model["training_mean_ticks"]
    elif not columns:
        result = np.full(len(x), model["training_mean_ticks"])
    else:
        xgb = c.old.comparison._xgb()
        booster = xgb.Booster(params={"nthread": 1, "device": "cpu"})
        booster.load_model(bytearray(model["model_json"], "utf-8"))
        result = booster.predict(xgb.DMatrix(x[:, columns])).astype(np.float64)
    return c.old.finite_float64(result, name="predictions", ndim=1)


def choose_candidate(scores):
    valid = [s for s in scores if s["status"] == "VALID"]
    if not valid or any(not np.isfinite(s["score"]) for s in valid):
        raise IntegrityError("no complete finite model candidates")
    winner = min(valid, key=lambda s: (-s["score"], s["family"] != "ridge", len(s["feature_names"]),
        -s["penalty"] if s["family"] == "ridge" else s["max_depth"], s["candidate_id"]))
    return {"candidate": {k: winner[k] for k in next(cand for cand in c.candidate_definitions()
                if cand["candidate_id"] == winner["candidate_id"])},
            "policy": "model" if winner["score"] > 0 else "no_trade", "score": max(0., winner["score"]),
            "model_score": winner["score"], "no_trade_score": 0.}


def forecast_batch(dataset, model, window):
    a, b = window.start * 330, window.stop * 330
    records = dataset.decisions[a:b]
    eligible = [i for i, r in enumerate(records) if r["status"] == "ELIGIBLE"]
    values = predict(model, dataset.features[a:b][eligible])
    predictions = [None] * len(records)
    for i, value in zip(eligible, values.tolist(), strict=True):
        predictions[i] = value
    return {"model_id": model["model_id"], "first_date": dataset.calendar_dates[window.start],
            "last_date": dataset.calendar_dates[window.stop - 1], "forecast_ids": [records[i]["forecast_id"] for i in eligible],
            "predictions": predictions, "evidence_id": dataset._evidence_id}


def replay_window(dataset, window, predictions, *, scenario="adverse", policy="model", **replay_options):
    a, b = window.start * 330, window.stop * 330
    return replay(dataset.decisions[a:b], dataset.outcomes[a:b], predictions,
                  dataset.calendar_dates[window.start:window.stop], scenario=scenario, policy=policy, **replay_options)


def compact_replay(result):
    """The saved full forecast stream and source ledger reconstruct decisions."""
    return c.bind({**{k: v for k, v in result.items() if k not in ("decisions", "replay_id")},
        "decision_count": len(result["decisions"]), "decision_ledger_sha256": sha256_json(result["decisions"])}, "replay_id")


def require_complete(result):
    if not result["complete"]:
        raise MarketRunFailure("scheduled execution has an unresolved reference; market economics incomplete", result)


def prediction_accuracy(dataset, window, predictions, training_mean):
    a, b = window.start * 330, window.stop * 330
    daily = {}
    for pos, prediction in zip(range(a, b), predictions, strict=True):
        outcome, record = dataset.outcomes[pos], dataset.decisions[pos]
        if outcome["status"] != "MATURED":
            continue
        if prediction is None:
            raise IntegrityError("observed target lacks its saved prediction")
        errors = {name: (value - outcome["target_ticks"]) ** 2 for name, value in
                  (("model", prediction), ("zero_change", 0.), ("training_mean", training_mean))}
        if not np.isfinite(list(errors.values())).all():
            raise IntegrityError("nonfinite prediction error")
        row = daily.setdefault(record["trade_date"], {"count": 0, "model": 0., "zero_change": 0., "training_mean": 0.})
        row["count"] += 1
        for name, value in errors.items():
            row[name] += value
    return {"daily_mse": {day: {name: r[name] / r["count"] for name in ("model", "zero_change", "training_mean")}
                          for day, r in daily.items()}, "observed_targets": sum(r["count"] for r in daily.values()),
            "forecast_count": sum(p is not None for p in predictions),
            "unscorable_dates": [d for d in dataset.calendar_dates[window.start:window.stop] if d not in daily],
            "claim": "DATE_BALANCED_OBSERVED_TARGET_ERROR;MISSINGNESS_DISCLOSED;NOT_ALPHA"}


def run_market_wfa(dataset, *, checkpoint=None, save=None, progress=None, calendar=None, calendar_view=None, legacy=False):
    """Complete inner search, outer policies and comparisons without reselection.

    Checkpoints may only reuse exact completed operations. The disk runner saves
    models and forecasts before replay and seals interrupted attempts.
    """
    from .l0_alpha_calendar import compile_calendar
    if legacy and any(f["source_contract_id"] != c.old.SYNTHETIC_SOURCE_CONTRACT_ID or
                      f["source_release_id"] != c.old.SYNTHETIC_SOURCE_RELEASE_ID for f in dataset.source_facts):
        raise IntegrityError("calendar-free legacy path is restricted to generated source identities")
    if calendar is None and not legacy:
        raise IntegrityError("source-bound calendar required before fitting")
    if calendar is not None:
        compile_calendar(calendar, calendar_view)
    replay_options = dict(calendar=calendar, calendar_view=calendar_view, legacy=legacy)
    dataset.verify()
    if dataset.calendar_dates != c.weekday_dates():
        raise IntegrityError("research must use the entire frozen 2018-2022 weekday calendar")
    checkpoint = checkpoint or (lambda name, produce: produce())
    save = save or (lambda name, payload: None)
    progress = progress or (lambda message: None)
    support = c.source_completeness(dataset)
    save("source_completeness", support)
    if support["support_status"] != "SUPPORTED":
        raise MarketRunFailure(support["support_failure"], {"source_completeness": support})
    outputs, scores = [], []
    try:
        for number, split in enumerate(c.fold_schedule(dataset)):
            scores = []
            progress(f"{dataset.market}: outer fold {number + 1}/8")
            for candidate in c.candidate_definitions():
                daily, manifests = [], []
                candidate_path = f"outer_{number}/{candidate['candidate_id'].replace(':', '_').replace('=', '_')}"
                try:
                    for j, inner in enumerate(split["inner"]):
                        path = f"{candidate_path}/inner_{j}"
                        def perform_inner():
                            try:
                                model = fit_model(dataset, inner["fit"], candidate)
                            except c.old.comparison.LeafSupportError as exc:
                                save(path + "/rejected_model", exc.fitted_model)
                                return {"status": "INVALID_LEAF_SUPPORT", "reason": str(exc)}
                            forecast = forecast_batch(dataset, model, inner["window"])
                            save(path + "/model", model)
                            save(path + "/forecasts", forecast)
                            result = compact_replay(replay_window(dataset, inner["window"], forecast["predictions"], **replay_options))
                            save(path + "/replay", result)
                            require_complete(result)
                            return {"status": "VALID", "model_id": model["model_id"], "replay_id": result["replay_id"],
                                    "daily_net_usd": {d: r["net_usd"] for d, r in result["daily"].items()},
                                    "trade_count": result["trade_count"]}
                        manifest = checkpoint(path, perform_inner)
                        manifests.append(manifest)
                        if manifest["status"] != "VALID":
                            raise c.old.comparison.LeafSupportError(manifest["reason"])
                        daily.extend(manifest["daily_net_usd"].values())
                    scores.append({**candidate, "status": "VALID", "score": float(np.mean(daily)), "inner": manifests})
                except c.old.comparison.LeafSupportError as exc:
                    scores.append({**candidate, "status": "INVALID_LEAF_SUPPORT", "reason": str(exc), "inner": manifests})
            chosen = choose_candidate(scores)
            window = split["outer_window"]
            path = f"outer_{number}/selected"
            def perform_outer():
                try:
                    model = fit_model(dataset, split["fit"], chosen["candidate"], fit_kind="outer")
                except c.old.comparison.LeafSupportError as exc:
                    save(path + "/rejected_model", exc.fitted_model)
                    raise
                save(path + "/model", model)
                forecast = forecast_batch(dataset, model, window)
                save(path + "/forecasts", forecast)
                predictions = forecast["predictions"]
                reports = {}
                for scenario in c.SCENARIOS:
                    result = compact_replay(replay_window(dataset, window, predictions, scenario=scenario, policy=chosen["policy"], **replay_options))
                    save(path + "/" + scenario, result)
                    require_complete(result)
                    reports[scenario] = result
                comparisons = {}
                for name in ("no_trade", "date_balanced_training_mean", "always_long", "always_short"):
                    comparison_forecasts = [model["training_mean_ticks"] if p is not None else None for p in predictions]
                    result = compact_replay(replay_window(dataset, window, comparison_forecasts, policy=name, **replay_options))
                    save(path + "/comparison_" + name, result)
                    require_complete(result)
                    comparisons[name] = result
                return {"model": model, "forecast": forecast, "reports": reports, "comparisons": comparisons,
                        "fixed_trade_sensitivity": fixed_trade_sensitivity(reports["adverse"]),
                        "prediction_accuracy": prediction_accuracy(dataset, window, predictions, model["training_mean_ticks"])}
            result = checkpoint(path, perform_outer)
            outputs.append({"outer_fold": number, "audit_dates": list(dataset.calendar_dates[window.start:window.stop]),
                            "candidate_scores": scores, "selected": chosen, **result})
    except Exception as exc:
        partial = {"market": dataset.market, "completed_outer_folds": outputs, "candidate_scores": scores,
                   "failure_evidence": getattr(exc, "partial_run", None), "source_completeness": support,
                   "status": "INCOMPLETE_ECONOMICS_OR_INTEGRITY_FAILURE", "replacement_allowed": False}
        raise MarketRunFailure(str(exc), partial) from exc
    return c.bind({"mechanism_id": c.mechanism_id(), "evidence_id": dataset._evidence_id, "market": dataset.market,
                   "outer_folds": outputs, "source_completeness": support,
                   "status": "COMPLETE_DEVELOPMENT_EXPERIMENT", "confirmation": c.design()["confirmation"]}, "run_id")


def validate_run(run):
    c.identity(run, "run_id")
    if (run["mechanism_id"] != c.mechanism_id() or len(run["outer_folds"]) != 8
            or run["status"] != "COMPLETE_DEVELOPMENT_EXPERIMENT"):
        raise IntegrityError("complete matching eight-fold run required")
    ranges = c.frozen_ranges(c.weekday_dates())
    for number, (fold, frozen) in enumerate(zip(run["outer_folds"], ranges, strict=True)):
        if fold["outer_fold"] != number or fold["audit_dates"] != frozen["outer"]["audit_dates"]:
            raise IntegrityError("saved outer dates differ from frozen dates")
        if [s["candidate_id"] for s in fold["candidate_scores"]] != [v["candidate_id"] for v in c.candidate_definitions()]:
            raise IntegrityError("saved candidate family is incomplete")
        for score in fold["candidate_scores"]:
            if score["status"] != "VALID":
                if score["status"] != "INVALID_LEAF_SUPPORT":
                    raise IntegrityError("integrity failure cannot invalidate only one candidate")
                continue
            if len(score["inner"]) != 4:
                raise IntegrityError("candidate lacks complete inner results")
            daily = []
            for inner, declared in zip(score["inner"], frozen["inner"], strict=True):
                if inner["status"] != "VALID" or sorted(inner["daily_net_usd"]) != declared["audit_dates"]:
                    raise IntegrityError("inner score denominator differs")
                daily.extend(inner["daily_net_usd"][day] for day in declared["audit_dates"])
            if score["score"] != float(np.mean(daily)):
                raise IntegrityError("candidate score differs from complete daily ledger")
        chosen = choose_candidate(fold["candidate_scores"])
        c.identity(fold["model"], "model_id")
        if (chosen != fold["selected"] or chosen["candidate"] != fold["model"]["candidate"]
                or fold["forecast"]["model_id"] != fold["model"]["model_id"]
                or fold["model"]["latest_label_ns"] >= c.decision_times(fold["audit_dates"][0])[0]):
            raise IntegrityError("outer policy or model differs from inner selection and maturity")


def evaluate_wfa(run):
    validate_run(run)
    aggregate = {}
    for group, names in (("reports", c.SCENARIOS), ("comparisons", c.design()["comparisons"])):
        for name in names:
            reports = [f[group][name] for f in run["outer_folds"]]
            for r in reports:
                c.identity(r, "replay_id")
                require_complete(r)
            daily = {d: row for r in reports for d, row in r["daily"].items()}
            if len(daily) != 504:
                raise IntegrityError("outer daily denominator differs or overlaps")
            trades = [t for r in reports for t in r["trades"]]
            net = sum(t["net_nano_usd"] for t in trades) / 1e9
            if not np.isclose(net, sum(d["net_usd"] for d in daily.values()), rtol=0, atol=1e-7):
                raise IntegrityError("trade ledger and daily dollars do not reconcile")
            curve = np.r_[0., np.cumsum([d["net_usd"] for d in daily.values()])]
            ranked = sorted(((row["net_usd"], day) for day, row in daily.items()), reverse=True)
            positive = sum(max(v, 0) for v, _ in ranked)
            aggregate[group + "/" + name] = {"net_usd": net, "mean_daily_net_usd": net / 504,
                "gross_usd": sum(t["gross_nano_usd"] for t in trades) / 1e9,
                "cost_usd": sum(t["cost_nano_usd"] for t in trades) / 1e9,
                "trade_count": len(trades), "daily": daily,
                "fold_net_usd": [r["net_usd"] for r in reports], "unresolved_coverage": 0,
                "trading_dates": sum(d["trades"] > 0 for d in daily.values()),
                "zero_trade_dates": sum(d["trades"] == 0 for d in daily.values()),
                "drawdown_usd": float(np.max(np.maximum.accumulate(curve) - curve)),
                "drawdown_basis": reports[0]["drawdown_basis"],
                "long_minutes": sum(r["long_minutes"] for r in reports), "short_minutes": sum(r["short_minutes"] for r in reports),
                "largest_winning_days": [{"net_usd": v, "date": d} for v, d in ranked[:5] if v > 0],
                "largest_losing_days": [{"net_usd": v, "date": d} for v, d in reversed(ranked[-5:]) if v < 0],
                "top_five_share_of_positive_day_profits": sum(max(v, 0) for v, _ in ranked[:5]) / positive if positive else None,
                "top_five_share_of_total_net_profit": sum(max(v, 0) for v, _ in ranked[:5]) / net if net > 0 else None,
                "fixed_trade_cost_headroom_usd": sum(t["gross_nano_usd"] for t in trades) / 1e9 / len(trades) - c.FEE_USD if trades else None,
                "remaining_adverse_headroom_usd": net / len(trades) if trades else None}
    accuracy = {day: values for f in run["outer_folds"] for day, values in f["prediction_accuracy"]["daily_mse"].items()}
    mse = {name: float(np.mean([v[name] for v in accuracy.values()])) if accuracy else None
           for name in ("model", "zero_change", "training_mean")}
    return {"market": run["market"], "run_id": run["run_id"], "economics": aggregate,
            "date_balanced_mse_ticks_squared": mse, "scorable_dates": len(accuracy),
            "unscorable_dates": [d for f in run["outer_folds"] for d in f["prediction_accuracy"]["unscorable_dates"]],
            "fixed_trade_sensitivity_by_fold": [f["fixed_trade_sensitivity"] for f in run["outer_folds"]],
            "outcome": "CANDIDATE_UNDER_ASSUMPTIONS" if aggregate["reports/adverse"]["net_usd"] > 0 else "NO_PROMISING_STRATEGY",
            "confirmation": c.design()["confirmation"], "alpha_established": False, "deployment_ready": False}


def final_refit(dataset, run):
    dataset.verify()
    validate_run(run)
    if (run["evidence_id"] != dataset._evidence_id or run["market"] != dataset.market
            or run["mechanism_id"] != c.mechanism_id() or len(run["outer_folds"]) != 8):
        raise IntegrityError("complete matching run required before final refit")
    chosen = choose_candidate(run["outer_folds"][-1]["candidate_scores"])
    if chosen != run["outer_folds"][-1]["selected"]:
        raise IntegrityError("final policy differs from last inner selection")
    indices = np.array([i for i, p in enumerate(dataset.fit_positions) if dataset.outcomes[p]["known_at_ns"] <= dataset.as_of_ns], dtype=np.int64)
    model = fit_model(dataset, indices, chosen["candidate"], fit_kind="final")
    return c.bind({"mechanism_id": c.mechanism_id(), "market": dataset.market, "model": model, "policy": chosen["policy"],
        "run_id": run["run_id"], "feature_order": list(c.FEATURES), "costs": c.design()["costs"],
        "training_cutoff_ns": dataset.as_of_ns, "latest_label_ns": model["latest_label_ns"],
        "timing": {k: c.design()[k] for k in ("decision_window", "entry", "holding_minutes", "availability", "refresh")},
        "contract_units": [list(v) for v in sorted({(r["tick_nano"], r["tick_value_nano_usd"]) for r in dataset.decisions if r["status"] == "ELIGIBLE"})],
        "active": False, "confirmation": c.design()["confirmation"]}, "final_id")
