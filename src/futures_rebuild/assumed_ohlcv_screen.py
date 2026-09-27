"""Bounded retrospective OHLCV screen. Observed coverage is an assumption, not a calendar."""

from __future__ import annotations

import json
from pathlib import Path
from time import monotonic
from types import SimpleNamespace

import numpy as np
import pyarrow.parquet as pq

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes
from .canonical import sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .fixed_hour_terminal_successor import provisional_costs
from .rolling_hour_price_census import _working_set
from . import rolling_hour_research as minute

OPERATION = "EXPLORE_ASSUMED_OHLCV_MINUTE_ONCE"
FAILED_PREDECESSOR_PLAN_ID = "a6aca8a8addd3f10b5d9164ef212fec35d383f64a5b8a25219809b2a30e74dca"
MARKETS = ("ES", "NQ", "CL", "GC")
HORIZONS = (15, 30, 60)
FEATURES = ("return_1_ticks", "return_5_ticks", "return_15_ticks",
            "return_60_ticks", "last_range_ticks", "mean_range_15_ticks",
            "return_volatility_15_ticks", "volume_z_15", "utc_minute_fraction")
PACKS = {"baseline": (0, 1, 4, 7), "context": (0, 1, 2, 4, 5, 6, 7),
         "expanded": tuple(range(len(FEATURES)))}
PROFILES = (("ridge", 1.), ("ridge", 10.), ("xgboost", 2))
COLUMNS = ("market", "bar_start_ns", "bar_end_ns", "available_at_ns",
           "decision_eligible_at_ns", "definition_received_at_ns", "actual_contract",
           "project_trade_date", "open_nano", "high_nano", "low_nano",
           "close_nano", "volume", "min_price_increment_nano", "multiplier_nano")
LIMITS = {"seconds": 43200, "memory_bytes": 16 * 2**30,
          "new_output_bytes": 2**30, "additional_cost_usd": 0}


def candidates():
    return tuple({"horizon": h, "pack": p, "family": f, "parameter": v,
                  "candidate_id": sha256_json([h, p, f, v])}
                 for h in HORIZONS for p in PACKS for f, v in PROFILES
                 if f != "xgboost" or p == "expanded")


def plan(root: Path):
    catalog_path = root / "data/causally_gated_normalized/_control/catalog.json"
    catalog = json.loads(catalog_path.read_bytes())
    sources = sorted(({"market": e["market"], "year": int(e["coverage_start"][:4]),
                       "path": e["parquet_path"], "sha256": e["parquet_sha256"],
                       "sidecar": e["sidecar_path"], "sidecar_sha256": e["sidecar_sha256"],
                       "rows": e["row_count"]}
                      for e in catalog["entries"] if e.get("market") in MARKETS
                      and e.get("selection_eligible") is True
                      and "2018-01-01" <= e.get("coverage_start", "") < "2023-01-01"),
                     key=lambda e: (e["market"], e["year"]))
    if (len(sources) != 20 or sum(e["rows"] for e in sources) != 7_010_381
            or {(e["market"], e["year"]) for e in sources}
               != {(m, y) for m in MARKETS for y in range(2018, 2023)}):
        raise IntegrityError("the approved twenty-source catalog scope changed")
    core = {"schema_version": "assumed_ohlcv_screen/1", "operation": OPERATION,
            "implementation_sha256": sha256_file(Path(__file__)),
            "failed_predecessor_plan_id": FAILED_PREDECESSOR_PLAN_ID,
            "claim": "RETROSPECTIVE_DESCRIPTIVE_ONLY_NO_CAUSAL_CALENDAR_OR_FILL_CLAIM",
            "source_root": "data/causally_gated_normalized/ohlcv_1m",
            "catalog_sha256": sha256_file(catalog_path), "sources": sources,
            "markets": list(MARKETS), "horizons": list(HORIZONS),
            "features": list(FEATURES), "packs": {k: list(v) for k, v in PACKS.items()},
            "expanded_selection": "FIT_FOLD_ONLY_WEIGHTED_ABS_PEARSON_UP_TO_FOUR_ADDITIONS;REJECT_ABS_CORRELATION_AT_LEAST_0.90",
            "candidates": list(candidates()), "folds": "8_OUTER_4_INNER_CHRONOLOGICAL",
            "maximum_fits_reserved": len(candidates()) * 8 * 4 * len(MARKETS) + 8 * len(MARKETS),
            "selector": "INNER_MEAN_DAILY_STRESS_PRICE_PROXY;ONE_FLAT_POSITION",
            "calendar_assumption": "OBSERVED_CONSECUTIVE_SAME_PROJECT_DATE_CONTRACT_BARS",
            "calendar_denominator": "OBSERVED_DATES_ONLY;UNOBSERVED_SESSIONS_NOT_ZERO_IMPUTED",
            "availability_assumption": "STORED_BAR_END_PLUS_FIVE_SECONDS_NOT_MEASURED_DELIVERY",
            "cost_assumption": "EXISTING_STRESS_TICKS_PLUS_TEN_USD_NO_FILL_VALIDATION",
            "prior_inspection": "DEVELOPMENT_HISTORY_PREVIOUSLY_INSPECTED;PREDECESSOR_ES_RESULT_INVALID_AND_VIEWED",
            "limits": LIMITS, "holdout": False, "forward": False, "trading": False}
    return {**core, "plan_id": sha256_json(core)}


def require_scope(root: Path, scope: dict[str, str]):
    expected = plan(root)
    request = {k: v for k, v in scope.items()
               if k not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}}
    if request != {"plan_id": expected["plan_id"],
                   "catalog_sha256": expected["catalog_sha256"],
                   "implementation_sha256": sha256_file(Path(__file__))}:
        raise UnauthorizedOperation("assumption screen scope differs from the frozen plan")


def _columns(path: Path, source: dict):
    with pq.ParquetFile(path) as parquet:
        if parquet.metadata.num_rows != source["rows"] or not set(COLUMNS) <= set(parquet.schema_arrow.names):
            raise IntegrityError("source row count or schema changed")
        table = parquet.read(columns=list(COLUMNS))
    result = {name: np.asarray(table[name].to_pylist()) for name in COLUMNS}
    if (len(result["bar_start_ns"]) != source["rows"]
            or not np.all(result["market"] == source["market"])
            or np.any(result["bar_end_ns"] - result["bar_start_ns"] != minute.MINUTE)
            or np.any(np.diff(result["bar_start_ns"]) <= 0)
            or np.any(result["available_at_ns"] < result["bar_end_ns"])
            or np.any(result["decision_eligible_at_ns"] < result["available_at_ns"])
            or np.any(result["open_nano"] <= 0)
            or np.any(result["low_nano"] <= 0)
            or np.any(result["low_nano"] > result["open_nano"])
            or np.any(result["open_nano"] > result["high_nano"])
            or np.any(result["low_nano"] > result["close_nano"])
            or np.any(result["close_nano"] > result["high_nano"])):
        raise IntegrityError("source prices or timestamps are not structurally valid")
    return result


def _samples(raw: dict, market: str):
    """Retain complete observed runs; eligibility is retrospective and disclosed."""
    n = len(raw["bar_start_ns"])
    t = raw["bar_start_ns"].astype(np.int64)
    day = raw["project_trade_date"]
    contract = raw["actual_contract"]
    tick = raw["min_price_increment_nano"].astype(np.int64)
    multiplier = raw["multiplier_nano"].astype(np.int64)
    split = np.flatnonzero((np.diff(t) != minute.MINUTE) | (day[1:] != day[:-1])
        | (contract[1:] != contract[:-1]) | (tick[1:] != tick[:-1])
        | (multiplier[1:] != multiplier[:-1])) + 1
    starts = np.r_[0, split]
    ends = np.r_[split, n]
    x_parts, y_parts, date_parts, time_parts, value_parts = [], [], [], [], []
    opens = raw["open_nano"].astype(np.float64)
    closes = raw["close_nano"].astype(np.float64)
    ranges = (raw["high_nano"].astype(np.float64) - raw["low_nano"].astype(np.float64))
    volume = raw["volume"].astype(np.float64)
    eligible = raw["decision_eligible_at_ns"].astype(np.int64)
    definition = raw["definition_received_at_ns"].astype(np.int64)
    for start, end in zip(starts, ends, strict=True):
        if (end - start < 123 or tick[start] <= 0 or multiplier[start] <= 0
                or not "2018-01-01" <= day[start] < "2023-01-01"):
            continue
        anchor = np.arange(start + 61, end - 61)
        recent = anchor - 1
        decision = t[anchor] + minute.DELAY
        if np.any(raw["project_trade_date"][anchor] != day[start]):
            raise IntegrityError("observed segment crosses project date")
        historical_known = np.lib.stride_tricks.sliding_window_view(
            np.maximum(eligible[start:end], definition[start:end]), 61)
        historical_known = historical_known[:len(anchor)].max(axis=1)
        valid = historical_known <= decision
        if not np.any(valid):
            continue
        anchor, recent = anchor[valid], recent[valid]
        q = float(tick[start])
        change = np.diff(closes[start:end]) / q
        local = recent - start
        vol = np.lib.stride_tricks.sliding_window_view(change, 15)
        rng = np.lib.stride_tricks.sliding_window_view(ranges[start:end] / q, 15)
        qty = np.lib.stride_tricks.sliding_window_view(volume[start:end], 15)
        returns_vol = vol[local - 15].std(axis=1)
        mean_range = rng[local - 14].mean(axis=1)
        qty_window = qty[local - 14]
        qty_sd = qty_window.std(axis=1)
        qty_z = np.divide(volume[recent] - qty_window.mean(axis=1), qty_sd,
                          out=np.zeros(len(anchor)), where=qty_sd > 0)
        features = np.column_stack(((closes[recent] - closes[recent - 1]) / q,
            (closes[recent] - closes[recent - 5]) / q,
            (closes[recent] - closes[recent - 15]) / q,
            (closes[recent] - closes[recent - 60]) / q,
            ranges[recent] / q, mean_range, returns_vol, qty_z,
            (t[anchor] // minute.MINUTE % 1440) / 1440.))
        targets = np.column_stack(tuple((opens[anchor + 1 + h] - opens[anchor + 1]) / q
                                          for h in HORIZONS))
        if not np.isfinite(features).all() or not np.isfinite(targets).all():
            raise IntegrityError("retrospective price sample has nonfinite values")
        x_parts.append(features)
        y_parts.append(targets)
        date_parts.append(np.full(len(anchor), day[start]))
        time_parts.append(t[anchor])
        tick_value_usd = q * float(multiplier[start]) / 1e18
        if not 0 < tick_value_usd < 1000:
            raise IntegrityError("assumed screen tick value is outside the declared market scale")
        value_parts.append(np.full(len(anchor), tick_value_usd))
    if not x_parts:
        raise IntegrityError("no complete observed price windows")
    return {"market": market, "x": np.vstack(x_parts), "y": np.vstack(y_parts),
            "dates": np.concatenate(date_parts), "decision_ns": np.concatenate(time_parts),
            "tick_value": np.concatenate(value_parts),
            "observed_run_count": len(starts), "sample_count": sum(len(x) for x in x_parts)}


def _expanded_columns(x, y, weights):
    baseline = list(PACKS["baseline"])

    def correlation(left, right):
        a = left - weights @ left
        b = right - weights @ right
        variance = (weights @ (a * a)) * (weights @ (b * b))
        return 0. if variance <= 0 else float((weights @ (a * b)) / np.sqrt(variance))

    extras = sorted((i for i in PACKS["expanded"] if i not in baseline),
                    key=lambda i: (-abs(correlation(x[:, i], y)), i))
    chosen = list(baseline)
    for i in extras:
        if len(chosen) == len(baseline) + 4:
            break
        if np.std(x[:, i]) == 0 or any(abs(correlation(x[:, i], x[:, j])) >= .90 for j in chosen):
            continue
        chosen.append(i)
    return tuple(chosen)


def _model(x, y, dates, candidate):
    unique, inverse, counts = np.unique(dates, return_inverse=True, return_counts=True)
    if len(unique) < 252:
        raise IntegrityError("insufficient observed fit dates")
    weights = 1. / (len(unique) * counts[inverse])
    columns = (_expanded_columns(x, y, weights) if candidate["pack"] == "expanded"
               else PACKS[candidate["pack"]])
    z = x[:, columns]
    intercept = float(weights @ y)
    if candidate["family"] == "ridge":
        center = weights @ z
        scale = np.sqrt(weights @ ((z - center) ** 2))
        retained = scale > 0
        scaled = (z[:, retained] - center[retained]) / scale[retained]
        coef = np.linalg.solve(scaled.T @ (weights[:, None] * scaled)
            + candidate["parameter"] * np.eye(retained.sum()),
            scaled.T @ (weights * (y - intercept))) if retained.any() else np.empty(0)
        return ("ridge", columns, retained, center, scale, coef, intercept)
    from .fixed_hour_model_comparison import _xgb, design
    xgb = _xgb()
    params = dict(design()["phase_5"]["xgboost"])
    for key in ("rounds", "minimum_leaf_dates", "early_stopping", "seed_search"):
        params.pop(key)
    params.update(max_depth=int(candidate["parameter"]), base_score=intercept,
                  min_child_weight=.05, **{"lambda": .01, "alpha": 0., "gamma": 0.})
    booster = xgb.train(params, xgb.DMatrix(z, label=y, weight=weights), num_boost_round=100)
    return ("xgboost", columns, booster)


def _predict(model, x):
    if model[0] == "ridge":
        _, columns, retained, center, scale, coef, intercept = model
        return intercept + ((x[:, columns][:, retained] - center[retained]) / scale[retained]) @ coef
    from .fixed_hour_model_comparison import _xgb
    return model[2].predict(_xgb().DMatrix(x[:, model[1]]))


def _replay(predictions, actual, dates, anchor_ns, tick_values, market, calendar_dates, horizon):
    if (len(predictions) != len(actual) or len(predictions) != len(dates)
            or len(predictions) != len(anchor_ns) or len(predictions) != len(tick_values)
            or not np.isfinite(predictions).all() or not np.isfinite(actual).all()):
        raise IntegrityError("proxy replay arrays are unaligned")
    costs = provisional_costs(market, tick_values)["stress"]
    directions = np.where(predictions > costs, 1, np.where(predictions < -costs, -1, 0))
    daily = {day: 0. for day in calendar_dates}
    trades = {day: 0 for day in calendar_dates}
    occupied_until = -1
    for i, direction in enumerate(directions):
        if not direction or anchor_ns[i] < occupied_until:
            continue
        day = str(dates[i])
        if day not in daily:
            raise IntegrityError("proxy replay date outside declared calendar")
        occupied_until = anchor_ns[i] + (1 + horizon) * minute.MINUTE
        daily[day] += float((direction * actual[i] - costs[i]) * tick_values[i])
        trades[day] += 1
    return daily, trades


def _combine(parts, market):
    return {"market": market, "x": np.vstack([p["x"] for p in parts]),
            "y": np.vstack([p["y"] for p in parts]),
            "dates": np.concatenate([p["dates"] for p in parts]),
            "decision_ns": np.concatenate([p["decision_ns"] for p in parts]),
            "tick_value": np.concatenate([p["tick_value"] for p in parts]),
            "observed_run_count": sum(p["observed_run_count"] for p in parts),
            "sample_count": sum(p["sample_count"] for p in parts)}


def _search(data, started, check_limit):
    market = data["market"]
    x, y, row_dates = data["x"], data["y"], data["dates"]
    calendar = tuple(str(day) for day in np.unique(row_dates))
    if len(calendar) < 1010 or x.shape != (len(row_dates), len(FEATURES)):
        raise IntegrityError("observed-date search support is insufficient")
    folds = minute.fold_schedule(SimpleNamespace(calendar_dates=calendar,
        terminal_data=SimpleNamespace(trade_dates=tuple(row_dates))))
    results, fits = [], 0
    for fold_number, fold in enumerate(folds):
        scores = []
        for candidate in candidates():
            horizon = candidate["horizon"]
            target = y[:, HORIZONS.index(horizon)]
            values, feature_sets, error = [], [], None
            for inner in fold.inner_folds:
                check_limit()
                fits += 1
                try:
                    fit = _model(x[inner.fit_indices], target[inner.fit_indices],
                                 row_dates[inner.fit_indices], candidate)
                    a = inner.audit_indices
                    forecast = _predict(fit, x[a])
                    window = calendar[inner.validation_window.start:inner.validation_window.stop]
                    daily, _ = _replay(forecast, target[a], row_dates[a],
                        data["decision_ns"][a], data["tick_value"][a], market, window, horizon)
                    values.extend(daily.values())
                    feature_sets.append([FEATURES[i] for i in fit[1]])
                except Exception as exc:
                    error = f"{type(exc).__name__}:{exc}"
                    break
            scores.append({"candidate_id": candidate["candidate_id"],
                           "status": "VALID" if error is None else "FAILED",
                           "reason": error, "mean_daily_proxy_usd":
                           float(np.mean(values)) if error is None and values else None,
                           "completed_inner_folds": len(feature_sets),
                           "inner_feature_sets": feature_sets})
        valid = [s for s in scores if s["status"] == "VALID"]
        if not valid:
            raise IntegrityError("every candidate failed inner evaluation")
        winner = max(valid, key=lambda s: (s["mean_daily_proxy_usd"], -int(s["candidate_id"], 16)))
        candidate = next(c for c in candidates() if c["candidate_id"] == winner["candidate_id"])
        check_limit()
        fits += 1
        target = y[:, HORIZONS.index(candidate["horizon"])]
        fit = _model(x[fold.fit_indices], target[fold.fit_indices],
                     row_dates[fold.fit_indices], candidate)
        a = fold.audit_indices
        window = calendar[fold.test_window.start:fold.test_window.stop]
        forecast = _predict(fit, x[a])
        daily, trades = _replay(forecast, target[a], row_dates[a],
            data["decision_ns"][a], data["tick_value"][a], market, window, candidate["horizon"])
        baseline_forecasts = {"flat": np.zeros(len(a)),
            "always_long": np.full(len(a), 1e9), "always_short": np.full(len(a), -1e9),
            "previous_bar_momentum": np.sign(x[a, 0]) * 1e9,
            "previous_bar_reversal": -np.sign(x[a, 0]) * 1e9,
            "training_mean_direction": np.full(len(a),
                np.sign(np.mean(target[fold.fit_indices])) * 1e9)}
        baselines = {name: float(np.mean(list(_replay(values, target[a], row_dates[a],
            data["decision_ns"][a], data["tick_value"][a], market, window,
            candidate["horizon"])[0].values()))) for name, values in baseline_forecasts.items()}
        results.append({"outer_fold": fold_number, "candidate_scores": scores,
                        "selected_candidate_id": winner["candidate_id"],
                        "selected_feature_names": [FEATURES[i] for i in fit[1]],
                        "audit_dates": list(window), "daily_stress_price_proxy_usd": daily,
                        "trades": sum(trades.values()),
                        "mean_daily_stress_price_proxy_usd": float(np.mean(list(daily.values()))),
                        "baseline_mean_daily_stress_price_proxy_usd": baselines})
    return {"market": market, "state": "DESCRIPTIVE_RETROSPECTIVE_ONLY",
            "observed_dates": len(calendar), "samples": len(x), "fits": fits,
            "outer_folds": results, "elapsed_seconds": monotonic() - started}


def execute_once(root: Path, receipt: OperationReceipt):
    """Run only with a consumed, plan-bound real-history receipt."""
    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    frozen = plan(root)
    scope = {"plan_id": frozen["plan_id"], "catalog_sha256": frozen["catalog_sha256"],
             "implementation_sha256": sha256_file(Path(__file__)),
             "approval_command": OPERATION, "approval_plan_id": frozen["plan_id"],
             "approval_plan_sha256": sha256_json(frozen)}
    classification = OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
    receipt.verify(boundary, operation=OPERATION, classification=classification,
                   required_scope=scope)
    output = boundary.assert_active_path(root / "state/unpublished_evidence/assumed_ohlcv_screen_v1"
        / frozen["plan_id"], purpose="assumption screen evidence")
    output.mkdir(parents=True, exist_ok=False)
    (output / "plan.json").write_bytes(canonical_bytes(frozen) + b"\n")
    (output / "approval.json").write_bytes(canonical_bytes({"user_approval": "okay do that",
        "approved_scope": "20 existing 2018-2022 price files; 12h; 16GiB; 1GiB outputs; zero purchase",
        "receipt": receipt.as_dict()}) + b"\n")
    receipt.consume(boundary, operation=OPERATION, classification=classification,
                    required_scope=scope)
    started, written, total_fits, results = monotonic(), 0, 0, []

    def check_limit():
        if monotonic() - started >= LIMITS["seconds"]:
            raise IntegrityError("assumption screen time cap reached")
        if _working_set() >= LIMITS["memory_bytes"]:
            raise IntegrityError("assumption screen working memory cap reached")

    def save(name, payload):
        nonlocal written
        raw = canonical_bytes(payload) + b"\n"
        if written + len(raw) > LIMITS["new_output_bytes"]:
            raise IntegrityError("assumption screen output cap reached")
        (output / name).write_bytes(raw)
        written += len(raw)

    try:
        for market in MARKETS:
            parts = []
            for source in (s for s in frozen["sources"] if s["market"] == market):
                check_limit()
                path = boundary.assert_active_path(root / source["path"], purpose="frozen OHLCV source")
                sidecar = boundary.assert_active_path(root / source["sidecar"], purpose="frozen OHLCV sidecar")
                if sha256_file(path) != source["sha256"] or sha256_file(sidecar) != source["sidecar_sha256"]:
                    raise IntegrityError("frozen price source or sidecar changed before read")
                raw = _columns(path, source)
                parts.append(_samples(raw, market))
                del raw
                if sha256_file(path) != source["sha256"] or sha256_file(sidecar) != source["sidecar_sha256"]:
                    raise IntegrityError("frozen price source or sidecar changed during read")
                check_limit()
            data = _combine(parts, market)
            del parts
            result = _search(data, started, check_limit)
            total_fits += result["fits"]
            save(market + ".json", result)
            results.append({"market": market, "samples": result["samples"],
                            "observed_dates": result["observed_dates"], "fits": result["fits"]})
            del data
        report = {"state": "COMPLETE_DESCRIPTIVE_ONLY", "plan_id": frozen["plan_id"],
                  "markets": results, "fits": total_fits,
                  "maximum_fits_reserved": frozen["maximum_fits_reserved"],
                  "elapsed_seconds": monotonic() - started,
                  "peak_working_set_bytes": _working_set(), "outputs_bytes": written,
                  "provider_cost_usd": 0, "holdout_read": False, "forward_read": False,
                  "profitability_or_tradability_claim": False}
        save("report.json", report)
        return report
    except Exception as exc:
        save("failure.json", {"state": "STOPPED_NO_RETRY", "reason": str(exc),
            "completed_markets": results, "fits": total_fits,
            "elapsed_seconds": monotonic() - started})
        raise
