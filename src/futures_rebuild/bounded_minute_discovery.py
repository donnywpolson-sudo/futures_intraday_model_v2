"""Inactive OHLCV-only minute-entry discovery mechanics.

No market-data reader, historical access grant, or promotion.
"""

from __future__ import annotations

from itertools import product
from math import isfinite
from decimal import Decimal
from types import SimpleNamespace
from time import monotonic

import numpy as np

from .canonical import sha256_json
from .errors import IntegrityError
from .fixed_hour_feature_expansion import CORE_FEATURES
from . import fixed_hour_terminal_successor as terminal
from .fixed_hour_terminal_successor import FEATURE_NAMES, PACKS
from .fixed_hour_ridge_audit_successor import validate_fixed_hour_rows
from . import rolling_hour_research as minute
from . import fixed_hour_model_comparison as comparison
from . import profit_aligned_research as profit

MARKETS = ("ES", "NQ", "CL", "GC")
HORIZONS = (15, 30, 60)
FIXED_PACKS = ("baseline", "existing_18", "context", "context_interactions")
EXPANSION_COUNTS = (4, 8)
PROFILES = (("ridge", .1), ("ridge", 1.), ("ridge", 10.),
            ("xgboost", 2), ("xgboost", 3))
OUTER_FOLDS = 8
INNER_FOLDS = 4
AUTHORITY = {key: False for key in ("historical_rows", "provider", "holdout", "forward",
                                    "registration", "activation", "publication", "trading")}


def candidates():
    """Enumerate every intended configuration before viewing outcomes."""
    definitions = []
    for horizon, pack, (family, parameter) in product(
            HORIZONS, (*FIXED_PACKS, *(f"expansion_{n}" for n in EXPANSION_COUNTS)), PROFILES):
        core = {"horizon_minutes": horizon, "feature_procedure": pack,
                "model_family": family,
                "model_parameter": parameter,
                "feature_names": list(PACKS[pack]) if pack in FIXED_PACKS else None}
        definitions.append({**core, "candidate_id": sha256_json(core)})
    if len(definitions) != 90 or len({c["candidate_id"] for c in definitions}) != 90:
        raise IntegrityError("bounded candidate enumeration changed")
    return tuple(definitions)


def specification():
    search = candidates()
    core = {"schema_version": "bounded_minute_discovery/1.0.0",
            "state": "INACTIVE_PREPARATION_ONLY", "authority": AUTHORITY,
            "markets": list(MARKETS), "horizons_minutes": list(HORIZONS),
            "common_eligibility_horizon_minutes": 60,
            "feature_library": list(FEATURE_NAMES), "core_features": list(CORE_FEATURES),
            "expansion_counts": list(EXPANSION_COUNTS),
            "expansion_ranking": "ABS_TRAINING_PEARSON_WITH_TERMINAL_TARGET;NAME_TIE_BREAK",
            "expansion_redundancy_abs_correlation_max": .90,
            "candidates": list(search), "configurations_per_market": len(search),
            "configuration_charges": len(search) * len(MARKETS),
            "minimum_reserved_fits": len(search) * len(MARKETS) * OUTER_FOLDS * INNER_FOLDS
                                     + len(MARKETS) * OUTER_FOLDS,
            "model_profiles": {"ridge_penalties": [.1, 1., 10.],
                               "xgboost_depths": [2, 3], "other_parameters": "REUSE_ROLLING_HOUR_V7"},
            "selection": "NESTED_CHRONOLOGICAL_INNER_DAILY_STRESS_NET;OUTER_RESULTS_NEVER_SELECT",
            "finalists": "AT_MOST_ONE_CANDIDATE_ID_PER_MARKET;FINAL_REFIT_NOT_PERFORMED",
            "cost_rule": "OHLCV_OPEN_PRINT_PROXY_PLUS_FROZEN_STRESS_COSTS;NO_FILL_CLAIM",
            "claim_ceiling": "DESCRIPTIVE_EXPLORATION_ONLY;NO_TRADABLE_PROFIT_CLAIM",
            "qualification": "BLOCK_UNTIL_QUOTE_EXECUTION_AND_RAW_PNL_INFERENCE_VALIDATED",
            "predecessor": "ROLLING_HOUR_V7_NOT_MUTATED;BOUND_27_CANDIDATE_FIXED_CHECKPOINT_INACTIVE"}
    return {**core, "specification_id": sha256_json(core)}


def selected_features(training_features, training_target, *, added_count):
    """Select additions using only the fit-fold arrays supplied by the caller."""
    x = np.asarray(training_features, dtype=np.float64)
    y = np.asarray(training_target, dtype=np.float64)
    if (added_count not in EXPANSION_COUNTS or x.ndim != 2 or
            x.shape[1] != len(FEATURE_NAMES) or y.shape != (len(x),) or
            len(x) < 2 or not np.isfinite(x).all() or not np.isfinite(y).all()):
        raise IntegrityError("invalid training-only feature expansion input")
    positions = {name: i for i, name in enumerate(FEATURE_NAMES)}
    target_sd = float(np.std(y))

    def correlation(a, b):
        a_sd, b_sd = float(np.std(a)), float(np.std(b))
        return 0. if a_sd == 0. or b_sd == 0. else float(np.corrcoef(a, b)[0, 1])

    ranked = sorted((name for name in FEATURE_NAMES if name not in CORE_FEATURES),
                    key=lambda name: (-abs(correlation(x[:, positions[name]], y))
                                      if target_sd else 0., name))
    selected = list(CORE_FEATURES)
    for name in ranked:
        if len(selected) == len(CORE_FEATURES) + added_count:
            break
        column = x[:, positions[name]]
        if np.std(column) == 0.:
            continue
        if any(abs(correlation(column, x[:, positions[kept]])) >= .90 for kept in selected):
            continue
        selected.append(name)
    return tuple(name for name in FEATURE_NAMES if name in selected)


def fixed_horizon_targets(base_schedule, path_rows, *, actual_contract, as_of_ns):
    """Build three terminal targets from one complete, matured 60-minute path."""
    minute.validate_schedule(base_schedule)
    if as_of_ns < base_schedule["exit_ns"] + minute.MINUTE + minute.DELAY:
        return None
    expected = list(range(base_schedule["entry_ns"],
                          base_schedule["exit_ns"] + minute.MINUTE, minute.MINUTE))
    if [row.get("bar_start_ns") for row in path_rows] != expected:
        raise IntegrityError("all horizons require the same complete 60-minute path")
    validate_fixed_hour_rows(path_rows, expected_market=base_schedule["market"],
        expected_trade_date=base_schedule["trade_date"],
        expected_source_contract_id=base_schedule["source_contract_id"],
        expected_source_release_id=base_schedule["source_release_id"],
        expected_contract=actual_contract)
    if (len({(row["min_price_increment_nano"], row["multiplier_nano"]) for row in path_rows}) != 1
            or any(row["decision_eligible_at_ns"] > as_of_ns for row in path_rows)):
        raise IntegrityError("outcome economics or maturity differs across horizons")
    tick = path_rows[0]["min_price_increment_nano"]
    entry = path_rows[0]["open_nano"]
    return {horizon: {"terminal_change_ticks":
            (path_rows[horizon]["open_nano"] - entry) / tick,
            "exit_ns": base_schedule["entry_ns"] + horizon * minute.MINUTE,
            "target_available_at_ns": path_rows[horizon]["decision_eligible_at_ns"],
            "source_row_ids": [row["row_id"] for row in path_rows[:horizon + 1]]}
            for horizon in HORIZONS}


def assemble_price_session(session, rows, *, as_of_ns, calendar=None,
                           source_contract_id=minute.SYNTHETIC_SOURCE_CONTRACT_ID,
                           source_release_id=minute.SYNTHETIC_SOURCE_RELEASE_ID):
    """Adapt supplied complete minute rows; the caller must certify real custody."""
    batch = minute.assemble_session(session, rows, as_of_ns=as_of_ns, quotes={},
        calendar=calendar, source_contract_id=source_contract_id,
        source_release_id=source_release_id)
    indexed = {row["bar_start_ns"]: row for row in rows}
    output = {"market": session.market, "trade_date": session.trade_date.isoformat(),
              "sample_ids": [], "features": [],
              "targets": {h: [] for h in HORIZONS},
              "opportunities": {h: [] for h in HORIZONS},
              "state": "SOURCE_ROWS_NOT_CERTIFIED", "authority": AUTHORITY}
    for record in batch["records"]:
        if record["status"] != "MATURED":
            raise IntegrityError("incomplete minute opportunity cannot enter price search")
        scheduled, feature = record["schedule"], record["feature"]
        path = [indexed[t] for t in range(scheduled["entry_ns"],
            scheduled["exit_ns"] + minute.MINUTE, minute.MINUTE)]
        targets = fixed_horizon_targets(scheduled, path,
            actual_contract=feature["actual_contract"], as_of_ns=as_of_ns)
        if targets is None or targets[60]["terminal_change_ticks"] != record["outcome"]["fit_target"]:
            raise IntegrityError("short and long horizon targets disagree")
        output["sample_ids"].append(scheduled["opportunity_id"])
        output["features"].append([feature["feature_values"][name] for name in FEATURE_NAMES])
        for horizon in HORIZONS:
            target = targets[horizon]
            output["targets"][horizon].append(target["terminal_change_ticks"])
            output["opportunities"][horizon].append({**{key: scheduled[key] for key in
                ("market", "trade_date", "opportunity_id", "decision_ns", "entry_ns",
                 "open_interval_start_ns", "open_interval_end_ns")},
                **target, "horizon_minutes": horizon,
                "actual_contract": feature["actual_contract"],
                "tick_value_usd": feature["tick_value_nano_usd"] / 1e9})
    return output


def combine_price_sessions(sessions, calendar_dates):
    """Require one supplied product for every declared date, including empty days."""
    dates = tuple(calendar_dates)
    batches = tuple(sessions)
    if (not dates or dates != tuple(sorted(set(dates))) or len(batches) != len(dates)
            or [batch.get("trade_date") for batch in batches] != list(dates)
            or len({batch.get("market") for batch in batches}) != 1
            or any(batch.get("state") != "SOURCE_ROWS_NOT_CERTIFIED" for batch in batches)):
        raise IntegrityError("complete ordered price-session products required")
    return {"market": batches[0]["market"], "calendar_dates": dates,
            "trade_dates": tuple(day for batch in batches for day in
                                 [batch["trade_date"]] * len(batch["sample_ids"])),
            "sample_ids": tuple(item for batch in batches for item in batch["sample_ids"]),
            "features": np.array([item for batch in batches for item in batch["features"]],
                                 dtype=np.float64).reshape(-1, len(FEATURE_NAMES)),
            "targets": {h: np.array([item for batch in batches for item in batch["targets"][h]],
                                     dtype=np.float64) for h in HORIZONS},
            "opportunities": {h: tuple(item for batch in batches
                                        for item in batch["opportunities"][h]) for h in HORIZONS},
            "state": "SOURCE_ROWS_NOT_CERTIFIED", "authority": AUTHORITY}


def adapt_supplied_price_sessions(*, sessions, rows_by_date, calendar, bindings,
                                  source_contract_id, source_release_id, as_of_ns):
    """Validate supplied certified-price products without opening source files."""
    if (source_contract_id == minute.SYNTHETIC_SOURCE_CONTRACT_ID
            or source_release_id == minute.SYNTHETIC_SOURCE_RELEASE_ID
            or not isinstance(calendar, minute.VerifiedExchangeCalendar)):
        raise IntegrityError("real price adapter needs non-synthetic source and verified calendar")
    calendar.verify()
    sessions = tuple(sessions)
    dates = profit.ordered_dates([session.trade_date.isoformat() for session in sessions])
    if (not dates or any(not "2018-01-01" <= day < "2023-01-01" for day in dates)
            or set(rows_by_date) != set(dates) or set(bindings) != set(dates)
            or len({session.market for session in sessions}) != 1
            or sessions[0].market not in MARKETS):
        raise IntegrityError("exact 2018-2022 one-market price scope required")
    expected = sorted(session.trade_date.isoformat() for (market, _), session in calendar.sessions.items()
        if market == sessions[0].market and dates[0] <= session.trade_date.isoformat() <= dates[-1])
    if list(dates) != expected:
        raise IntegrityError("declared dates omit verified calendar sessions")
    products = []
    for session in sessions:
        day = session.trade_date.isoformat()
        if calendar.sessions.get((session.market, session.trade_date)) != session:
            raise IntegrityError("session differs from verified calendar")
        binding, rows = bindings[day], rows_by_date[day]
        profit.identity(binding, "binding_id")
        for key in ("calendar_evidence_id", "contract_selection_evidence_id", "source_certificate_id"):
            profit.digest(binding.get(key))
        if (binding.get("market") != session.market or binding.get("trade_date") != day
                or binding.get("calendar_release_id") != calendar.receipt.release_id
                or binding.get("source_contract_id") != source_contract_id
                or binding.get("source_release_id") != source_release_id
                or binding.get("point_in_time_evidence") != "CERTIFIED_ORIGINAL_PUBLICATION"):
            raise IntegrityError("price binding lacks exact point-in-time provenance")
        opens = [int(interval.starts_at_utc.timestamp()) * 10**9
                 for interval in session.intervals if interval.state == "OPEN"]
        if any(type(binding.get(key)) is not int or binding[key] < 0
               or (opens and binding[key] > min(opens))
               for key in ("calendar_known_at_ns", "contract_selection_known_at_ns")):
            raise IntegrityError("calendar or contract selection was not known before open")
        expected_minutes = [t for interval in session.intervals if interval.state == "OPEN"
            for t in range(int(interval.starts_at_utc.timestamp()) * 10**9,
                           int(interval.ends_at_utc.timestamp()) * 10**9, minute.MINUTE)]
        if [row.get("bar_start_ns") for row in rows] != expected_minutes:
            raise IntegrityError("complete open-interval minute coverage required")
        contract = binding.get("actual_contract")
        if not isinstance(contract, str) or not contract:
            raise IntegrityError("point-in-time actual contract required")
        validate_fixed_hour_rows(rows, expected_market=session.market, expected_trade_date=day,
            expected_contract=contract, expected_source_contract_id=source_contract_id,
            expected_source_release_id=source_release_id)
        allowed = binding.get("source_file_sha256s")
        if not isinstance(allowed, list) or not allowed:
            raise IntegrityError("certified source file identities required")
        for digest in allowed:
            profit.digest(digest)
        if any(row["source_file_sha256"] not in allowed for row in rows):
            raise IntegrityError("price row lies outside certified source files")
        products.append(assemble_price_session(session, rows, as_of_ns=as_of_ns,
            calendar=calendar, source_contract_id=source_contract_id,
            source_release_id=source_release_id))
    return combine_price_sessions(products, dates)


def proxy_replay(predictions, opportunities, calendar_dates, *, forced_directions=False):
    """Score flat-only fixed exits at bar-open proxies with frozen adverse costs."""
    predictions = np.asarray(predictions, dtype=np.float64)
    if (predictions.ndim != 1 or predictions.shape != (len(opportunities),)
            or not np.isfinite(predictions).all() or not opportunities
            or (forced_directions and not np.isin(predictions, [-1, 0, 1]).all())):
        raise IntegrityError("proxy replay needs complete finite predictions")
    dates = tuple(calendar_dates)
    if not dates or dates != tuple(sorted(set(dates))):
        raise IntegrityError("complete ordered calendar dates required")
    market = opportunities[0].get("market")
    if market not in MARKETS:
        raise IntegrityError("unsupported proxy market")
    daily = dict.fromkeys(dates, Decimal(0))
    counts = dict.fromkeys(dates, 0)
    occupied_until = -1
    previous_decision = -1
    seen = set()
    decisions = []
    for prediction, row in zip(predictions, opportunities, strict=True):
        day = row.get("trade_date")
        decision, entry, exit_at = (row.get(key) for key in ("decision_ns", "entry_ns", "exit_ns"))
        horizon = row.get("horizon_minutes")
        tick_value = row.get("tick_value_usd")
        actual = row.get("terminal_change_ticks")
        opportunity_id = row.get("opportunity_id")
        interval_start = row.get("open_interval_start_ns")
        interval_end = row.get("open_interval_end_ns")
        if (row.get("market") != market or day not in daily or not isinstance(opportunity_id, str)
                or not opportunity_id or opportunity_id in seen
                or not isinstance(row.get("actual_contract"), str) or not row["actual_contract"]
                or any(type(v) is not int for v in (decision, entry, exit_at, interval_start, interval_end))
                or decision <= previous_decision or entry != (decision // minute.MINUTE + 1) * minute.MINUTE
                or horizon not in HORIZONS or exit_at != entry + horizon * minute.MINUTE
                or not interval_start <= decision < entry < exit_at
                or exit_at + minute.MINUTE > interval_end
                or type(row.get("target_available_at_ns")) is not int
                or row["target_available_at_ns"] < exit_at + minute.MINUTE
                or not isinstance(tick_value, (float, int)) or not isfinite(tick_value) or tick_value <= 0
                or not isinstance(actual, (float, int)) or not isfinite(actual)):
            raise IntegrityError("incomplete or noncausal proxy opportunity")
        seen.add(opportunity_id)
        previous_decision = decision
        stress_ticks = float(terminal.provisional_costs(market, np.array([tick_value]))["stress"][0])
        direction = (int(prediction) if forced_directions else
                     1 if prediction > stress_ticks else -1 if prediction < -stress_ticks else 0)
        if decision < occupied_until:
            direction = 0
        if direction:
            occupied_until = exit_at
            daily[day] += Decimal(str((direction * actual - stress_ticks) * tick_value))
            counts[day] += 1
        decisions.append({"opportunity_id": opportunity_id, "direction": direction,
                          "horizon_minutes": horizon})
    return {"state": "DESCRIPTIVE_PRICE_PROXY_ONLY", "market": market,
            "daily_stress_proxy_usd": {day: str(value) for day, value in daily.items()},
            "mean_daily_stress_proxy_usd": float(sum(daily.values()) / len(daily)),
            "daily_trade_counts": counts, "decisions": decisions,
            "authority": AUTHORITY}


def fit_candidate(features, target, trade_dates, candidate):
    """Fit one declared profile; the caller passes fit-fold rows only."""
    match = next((item for item in candidates()
                  if item["candidate_id"] == candidate.get("candidate_id")), None)
    if match is None or any(candidate.get(key) != value for key, value in match.items()):
        raise IntegrityError("candidate differs from frozen search")
    candidate = match
    x = np.asarray(features, dtype=np.float64)
    y = np.asarray(target, dtype=np.float64)
    dates = tuple(trade_dates)
    if (x.ndim != 2 or x.shape[1] != len(FEATURE_NAMES) or y.shape != (len(x),)
            or len(dates) != len(x) or not len(x) or dates != tuple(sorted(dates))
            or not np.isfinite(x).all() or not np.isfinite(y).all()):
        raise IntegrityError("candidate fit requires aligned finite chronological training rows")
    procedure = candidate["feature_procedure"]
    names = (candidate["feature_names"] if procedure in FIXED_PACKS else
             selected_features(x, y, added_count=int(procedure.removeprefix("expansion_"))))
    columns = terminal.retained_columns(x, names)
    weights = minute.training_weights(dates)
    mean = float(weights @ y)
    model = {"candidate_id": candidate["candidate_id"], "family": candidate["model_family"],
             "columns": columns, "feature_names": [FEATURE_NAMES[i] for i in columns],
             "training_dates": len(set(dates)), "intercept": mean}
    if candidate["model_family"] == "ridge":
        if columns:
            center = weights @ x[:, columns]
            scale = np.sqrt(weights @ ((x[:, columns] - center) ** 2))
            z = (x[:, columns] - center) / scale
            coefficient = np.linalg.solve(z.T @ (weights[:, None] * z)
                + candidate["model_parameter"] * np.eye(len(columns)),
                z.T @ (weights * (y - mean)))
        else:
            center, scale, coefficient = np.empty(0), np.empty(0), np.empty(0)
        model.update(center=center, scale=scale, coefficient=coefficient)
    elif columns:
        xgb = comparison._xgb()
        params = dict(comparison.design()["phase_5"]["xgboost"])
        for key in ("rounds", "minimum_leaf_dates", "early_stopping", "seed_search"):
            params.pop(key)
        params.update(max_depth=candidate["model_parameter"], base_score=mean,
                      min_child_weight=.05, **{"lambda": .01, "alpha": 0., "gamma": 0.})
        matrix = xgb.DMatrix(x[:, columns], label=y, weight=weights)
        booster = xgb.train(params, matrix, num_boost_round=100)
        comparison.check_leaf_dates(booster.predict(matrix, pred_leaf=True), dates)
        model["booster"] = booster
    return model


def predict_candidate(model, features):
    x = np.asarray(features, dtype=np.float64)
    if x.ndim != 2 or x.shape[1] != len(FEATURE_NAMES) or not np.isfinite(x).all():
        raise IntegrityError("candidate prediction features are invalid")
    columns = model["columns"]
    if not columns:
        result = np.full(len(x), model["intercept"])
    elif model["family"] == "ridge":
        result = model["intercept"] + ((x[:, columns] - model["center"]) / model["scale"]) @ model["coefficient"]
    else:
        result = model["booster"].predict(comparison._xgb().DMatrix(x[:, columns]))
    if not np.isfinite(result).all():
        raise IntegrityError("candidate prediction is nonfinite")
    return result


def choose_candidate(scores):
    valid = [row for row in scores if row["status"] == "VALID"]
    if not valid:
        return None
    best = max(row["score"] for row in valid)
    close = [row for row in valid if row["score"] >= best - .01]
    return min(close, key=lambda row: (
        row["model_family"] != "ridge", row["retained_count"],
        -row["model_parameter"] if row["model_family"] == "ridge" else row["model_parameter"],
        row["candidate_id"]))


def run_market_search(data, *, maximum_fits, maximum_seconds):
    """Run a complete in-memory exploratory search; no source or authority access."""
    market = data.get("market")
    dates = tuple(data.get("calendar_dates", ()))
    trade_dates = tuple(data.get("trade_dates", ()))
    ids = tuple(data.get("sample_ids", ()))
    x = np.asarray(data.get("features"), dtype=np.float64)
    labels = data.get("targets")
    opportunities = data.get("opportunities")
    if (market not in MARKETS or len(dates) < 1010 or dates != tuple(sorted(set(dates)))
            or len(ids) != len(trade_dates) or len(set(ids)) != len(ids)
            or trade_dates != tuple(sorted(trade_dates)) or any(day not in dates for day in trade_dates)
            or x.shape != (len(ids), len(FEATURE_NAMES)) or not np.isfinite(x).all()
            or not isinstance(labels, dict) or set(labels) != set(HORIZONS)
            or not isinstance(opportunities, dict) or set(opportunities) != set(HORIZONS)
            or type(maximum_fits) is not int or maximum_fits < 1
            or not isinstance(maximum_seconds, (int, float)) or maximum_seconds <= 0):
        raise IntegrityError("complete bounded common-calendar search data required")
    y = {}
    for horizon in HORIZONS:
        y[horizon] = np.asarray(labels[horizon], dtype=np.float64)
        rows = opportunities[horizon]
        if (y[horizon].shape != (len(ids),) or not np.isfinite(y[horizon]).all()
                or len(rows) != len(ids)
                or any(row.get("opportunity_id") != ids[i]
                       or row.get("trade_date") != trade_dates[i]
                       or row.get("horizon_minutes") != horizon
                       or row.get("terminal_change_ticks") != y[horizon][i]
                       for i, row in enumerate(rows))):
            raise IntegrityError("horizon labels or opportunities are not aligned")
    folds = minute.fold_schedule(SimpleNamespace(calendar_dates=dates,
        terminal_data=SimpleNamespace(trade_dates=trade_dates)))
    declared = candidates()
    if maximum_fits < len(declared) * len(folds) * INNER_FOLDS + len(folds):
        raise IntegrityError("fit budget is below complete market search")
    start, fits, output = monotonic(), 0, []

    def fit(indices, candidate):
        nonlocal fits
        if fits >= maximum_fits or monotonic() - start >= maximum_seconds:
            raise IntegrityError("search resource budget exhausted before fit")
        fits += 1  # Charge before the fitter; failures are not refunded.
        h = candidate["horizon_minutes"]
        return fit_candidate(x[indices], y[h][indices], [trade_dates[i] for i in indices], candidate)

    for fold_number, split in enumerate(folds):
        scores = []
        for candidate in declared:
            h, daily_scores, retained = candidate["horizon_minutes"], [], []
            status, reason = "VALID", None
            for inner in split.inner_folds:
                try:
                    model = fit(inner.fit_indices, candidate)
                    a = inner.audit_indices
                    prediction = predict_candidate(model, x[a])
                    audit_dates = dates[inner.validation_window.start:inner.validation_window.stop]
                    replay = proxy_replay(prediction, [opportunities[h][i] for i in a], audit_dates)
                    daily_scores.extend(float(value) for value in replay["daily_stress_proxy_usd"].values())
                    retained.append(len(model["columns"]))
                except comparison.LeafSupportError as exc:
                    status, reason = "INVALID_LEAF_SUPPORT", str(exc)
                    break
            scores.append({**candidate, "status": status, "reason": reason,
                           "score": float(np.mean(daily_scores)) if status == "VALID" else None,
                           "retained_count": float(np.mean(retained)) if status == "VALID" else None,
                           "completed_inner_folds": len(retained)})
        selected = choose_candidate(scores)
        if selected is None:
            return {"state": "NO_VALID_CANDIDATE", "market": market,
                    "completed_outer_folds": output, "candidate_scores": scores,
                    "fits_charged": fits, "authority": AUTHORITY}
        h = selected["horizon_minutes"]
        try:
            model = fit(split.fit_indices, selected)
        except comparison.LeafSupportError as exc:
            return {"state": "SELECTED_OUTER_REFIT_FAILED", "market": market,
                    "failed_outer_fold": fold_number, "reason": str(exc),
                    "completed_outer_folds": output, "candidate_scores": scores,
                    "fits_charged": fits, "authority": AUTHORITY}
        a = split.audit_indices
        audit_dates = dates[split.test_window.start:split.test_window.stop]
        rows = [opportunities[h][i] for i in a]
        prediction = predict_candidate(model, x[a])
        replay = proxy_replay(prediction, rows, audit_dates)
        prior_sign = np.sign(x[a, FEATURE_NAMES.index("close_change_ticks_1")])
        unconditional = float(np.sign(np.mean(y[h][split.fit_indices])))
        baselines = {"flat_no_trade": np.zeros(len(a)),
                     "fold_local_unconditional_direction": np.full(len(a), unconditional),
                     "previous_bar_sign_momentum": prior_sign,
                     "previous_bar_sign_reversal": -prior_sign,
                     "always_long": np.ones(len(a)), "always_short": -np.ones(len(a))}
        baseline_scores = {name: proxy_replay(directions, rows, audit_dates,
            forced_directions=True)["mean_daily_stress_proxy_usd"]
            for name, directions in baselines.items()}
        output.append({"outer_fold": fold_number, "candidate_scores": scores,
                       "selected_candidate_id": selected["candidate_id"],
                       "selected_horizon_minutes": h,
                       "selected_feature_names": model["feature_names"],
                       "audit_dates": list(audit_dates),
                       "daily_stress_proxy_usd": replay["daily_stress_proxy_usd"],
                       "baseline_mean_daily_stress_proxy_usd": baseline_scores,
                       "executed_trades": sum(replay["daily_trade_counts"].values())})
    return {"state": "COMPLETE_DESCRIPTIVE_ONLY", "market": market,
            "outer_folds": output,
            "final_candidate_id": output[-1]["selected_candidate_id"],
            "final_candidate_rule": "LAST_OUTER_INNER_WINNER",
            "fits_charged": fits, "authority": AUTHORITY}


def preflight(owner_inputs, evidence):
    """Report supplied prerequisites; this is never execution authority."""
    required_numbers = ("minimum_daily_net_usd", "maximum_drawdown_usd",
                        "maximum_attempts", "maximum_fits", "maximum_seconds",
                        "maximum_memory_bytes", "maximum_output_bytes")
    if not isinstance(owner_inputs, dict) or not isinstance(evidence, dict):
        raise IntegrityError("owner inputs and evidence statuses must be mappings")
    missing = [name for name in (*required_numbers, "maximum_additional_cost_usd")
               if name not in owner_inputs]
    if missing:
        return {"reported_prerequisites_complete": False,
                "blocks": [f"MISSING_OWNER_INPUT:{name}" for name in missing],
                "authority": AUTHORITY}
    for name in required_numbers:
        value = owner_inputs[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value <= 0:
            raise IntegrityError(f"{name} must be a finite positive number")
    cost = owner_inputs["maximum_additional_cost_usd"]
    if isinstance(cost, bool) or cost != 0:
        raise IntegrityError("zero-purchase route requires zero additional cost")
    blocks = [name for name in ("certified_development_ohlcv", "source_bound_readiness",
                              "declared_exploratory_scope", "bounded_resource_pilot")
              if evidence.get(name) is not True]
    if owner_inputs["maximum_fits"] < specification()["minimum_reserved_fits"]:
        blocks.append("FIT_BUDGET_BELOW_DECLARED_SEARCH")
    return {"reported_prerequisites_complete": not blocks, "blocks": blocks,
            "configuration_charges": specification()["configuration_charges"],
            "minimum_reserved_fits": specification()["minimum_reserved_fits"],
            "claim_ceiling": "DESCRIPTIVE_EXPLORATION_ONLY",
            "authority": AUTHORITY}
