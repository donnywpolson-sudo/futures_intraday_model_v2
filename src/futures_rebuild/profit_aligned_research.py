"""Inactive fixed-size research successor. Pure mechanics; no source-data reader."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from functools import lru_cache
from math import ceil, isfinite
import os
from pathlib import Path

import numpy as np

from . import fixed_hour_model_comparison as predecessor
from . import fixed_hour_terminal_successor as terminal
from .canonical import canonical_bytes, sha256_json, write_new_canonical_authority
from .fixed_hour_terminal_evaluation import coverage_report
from .errors import IntegrityError
from .research.contracts import finite_float64, require_unique_ascii_ids

MARKETS = terminal.MARKETS
AUTHORITY = dict(terminal.AUTHORITY)
MINUTE = 60_000_000_000
DEVELOPMENT_CUTOFF_NS = 1752444000000000000  # 2025-07-13T22:00:00Z, exclusive.


def bind(core, key):
    return {**core, key: sha256_json(core)}


def identity(value, key):
    if value.get(key) != sha256_json({k: v for k, v in value.items() if k != key}):
        raise IntegrityError(f"invalid {key}")


def digest(value):
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise IntegrityError("expected SHA-256 identity")
    return value


def integer(value, *, minimum=0):
    if type(value) is not int or value < minimum:
        raise IntegrityError("invalid integer")
    return value


def ordered_dates(values):
    if not isinstance(values, (tuple, list)) or not values:
        raise IntegrityError("nonempty date calendar required")
    try:
        if any(type(d) is not str or date.fromisoformat(d).isoformat() != d for d in values):
            raise ValueError("noncanonical date")
    except (ValueError, TypeError) as exc:
        raise IntegrityError("invalid date calendar") from exc
    if list(values) != sorted(set(values)):
        raise IntegrityError("date calendar repeats or reverses")
    return tuple(values)


def design():
    return bind({
        "schema_version": "profit_aligned_research/1.0.0", "state": "IMPLEMENTED_INACTIVE",
        "predecessor_mechanism_id": predecessor.design()["mechanism_id"],
        "markets": list(MARKETS), "candidates": list(predecessor.candidate_definitions()),
        "new_configuration_charges": 180, "primary_claims": [f"{m}:net_usd_vs_zero" for m in MARKETS],
        "position": "ONE_STANDARD_CONTRACT_PER_SIGNAL_NO_COMPOUNDING",
        "primary_score": "MEAN_OF_DAILY_SUMMED_STRESS_NET_USD_OVER_ALL_INNER_AUDIT_DATES",
        "tie_tolerance_usd_per_day": .01, "tie_order": predecessor.design()["phase_5"]["tie_break"],
        "fit_target": "TERMINAL_OPEN_PRINT_CHANGE_TICKS", "feature_names": list(terminal.FEATURE_NAMES),
        "selection_period": ["2018-01-01", "2023-01-01"],
        "development_cutoff_exclusive_ns": DEVELOPMENT_CUTOFF_NS,
        "outer_folds": 8, "inner_folds": 4, "audit_dates": 63, "embargo_dates": 1,
        "minimum_outer_fit_dates": 505, "minimum_inner_fit_dates": 252,
        "execution": "BUY_ASK_SELL_BID_AT_SCHEDULED_BOUNDARIES_PLUS_FEES_AND_ADVERSE_ALLOWANCE",
        "threshold": "PREDICTED_TICKS_VS_DECISION_KNOWN_FULL_SPREAD_PLUS_FEE_AND_STRESS_ALLOWANCE",
        "fee_floor_usd_round_trip": 10., "additional_adverse_ticks": {s: list(v) for s, v in terminal.ADVERSE_TICKS.items()},
        "clock_and_fee_evidence_required": True, "missing_exit": "INVALID_INCOMPLETE_NOT_ZERO",
        "benchmark_and_quartile_reports": "DESCRIPTIVE_NO_SUPERIORITY_OR_SEGMENT_PROMOTION_GATES",
        "historical_result": "DEVELOPMENT_ONLY_NOT_QUALIFICATION",
        "historical_stress": "ONE_SHOT_2023_TO_CUTOFF_WHOLE_PERIOD_POSITIVE_NET_USD",
        "final_refit": "SAME_SELECTED_CANDIDATE_2018_TO_CUTOFF_NO_EVALUATION",
        "final_candidate": "LAST_OUTER_FOLD_INNER_SELECTION_WINNER_NOT_OUTER_SCORE_WINNER",
        "confirmation": {"sessions": 252, "minimum_executed_trades": 500,
            "test": "ONE_SIDED_NULL_CENTERED_STUDENTIZED_STATIONARY_BOOTSTRAP",
            "hac_lag": 5, "mean_block_length": 10., "resamples": 9999, "seed": 62817,
            "multiplicity": "BONFERRONI_FIXED_FOUR_CLAIMS", "program_alpha": .05,
            "campaign_alpha": "0.05/2**k;k>=1;NO_RESET_OR_REALLOCATION",
            "early_success_or_extension": False, "retraining": False,
            "assumptions": "STATIONARY_WEAKLY_DEPENDENT_PNL_WITH_REQUIRED_FINITE_MOMENTS;APPROXIMATE_INFERENCE",
            "historical_holdout_or_old_forward_is_fresh": False},
        "authority": AUTHORITY,
    }, "mechanism_id")


@lru_cache(maxsize=1)
def mechanism_id():
    return design()["mechanism_id"]


def cost_policy(*, source_release_id, clock_policy_id, fee_evidence_id,
                maximum_quote_age_ns, verified_round_trip_fee_usd):
    for value in (source_release_id, clock_policy_id, fee_evidence_id):
        digest(value)
    integer(maximum_quote_age_ns, minimum=1)
    if (type(verified_round_trip_fee_usd) not in (float, int)
            or not isfinite(verified_round_trip_fee_usd) or verified_round_trip_fee_usd < 0):
        raise IntegrityError("verified fee must be finite and nonnegative")
    return bind({"schema_version": "profit_quote_policy/1.0.0", "mechanism_id": mechanism_id(),
        "source_release_id": source_release_id, "clock_policy_id": clock_policy_id,
        "fee_evidence_id": fee_evidence_id, "maximum_quote_age_ns": maximum_quote_age_ns,
        "verified_round_trip_fee_usd": float(verified_round_trip_fee_usd),
        "round_trip_fee_usd": max(10., float(verified_round_trip_fee_usd)),
        "claim": "QUOTE_BASED_RESEARCH_NOT_ACHIEVABLE_FILL_EVIDENCE", "authority": AUTHORITY}, "policy_id")


def validate_policy(policy):
    identity(policy, "policy_id")
    expected = cost_policy(**{k: policy[k] for k in ("source_release_id", "clock_policy_id",
        "fee_evidence_id", "maximum_quote_age_ns", "verified_round_trip_fee_usd")})
    if expected != policy:
        raise IntegrityError("quote policy changed")


def usable_quote(quote, *, at_ns, market, contract, tick_nano, policy):
    """Availability includes interval completion and the certified clock policy.

    Missing/bad books are execution abstentions, not repaired quotes. Zero and
    negative prices are valid. Structural identity errors always stop processing.
    """
    if quote is None:
        return False
    if not isinstance(quote, dict):
        raise IntegrityError("quote must be a record or absent")
    for field in ("interval_end_ns", "available_at_ns", "bid_nano", "ask_nano", "bid_size", "ask_size"):
        if type(quote.get(field)) is not int:
            raise IntegrityError("quote fields require exact integers")
    if (quote.get("market") != market or quote.get("actual_contract") != contract
            or quote.get("source_release_id") != policy["source_release_id"]
            or quote.get("clock_policy_id") != policy["clock_policy_id"]):
        raise IntegrityError("quote source/contract identity mismatch")
    if quote["available_at_ns"] < quote["interval_end_ns"]:
        raise IntegrityError("interval summary cannot be available before completion")
    if type(quote.get("quality_valid")) is not bool:
        raise IntegrityError("quote quality disposition is missing")
    if quote["bid_nano"] % tick_nano or quote["ask_nano"] % tick_nano:
        raise IntegrityError("quote price is off tick")
    return (quote["quality_valid"] and quote["bid_size"] >= 1 and quote["ask_size"] >= 1
        and quote["bid_nano"] <= quote["ask_nano"]
        and quote["interval_end_ns"] <= quote["available_at_ns"] <= at_ns
        and at_ns - quote["interval_end_ns"] <= policy["maximum_quote_age_ns"])


def validate_opportunity(row):
    for name in ("decision_ns", "entry_ns", "exit_ns", "tick_nano", "tick_value_nano_usd"):
        integer(row.get(name), minimum=1)
    if (row.get("market") not in MARKETS or not isinstance(row.get("actual_contract"), str)
            or not row["actual_contract"] or not isinstance(row.get("opportunity_id"), str)
            or not row["opportunity_id"]):
        raise IntegrityError("invalid opportunity identity")
    ordered_dates([row.get("trade_date")])
    integer(row.get("quartile"), minimum=1)
    if row["quartile"] > 4:
        raise IntegrityError("invalid open-interval quartile")
    if row["entry_ns"] != (row["decision_ns"] // MINUTE + 1) * MINUTE or row["exit_ns"] != row["entry_ns"] + 60 * MINUTE:
        raise IntegrityError("entry/exit differs from fixed-hour schedule")


def decision(prediction, row, policy):
    validate_policy(policy)
    validate_opportunity(row)
    if type(prediction) not in (float, int) or not isfinite(prediction):
        raise IntegrityError("nonfinite terminal prediction")
    quote = row.get("decision_quote")
    if not usable_quote(quote, at_ns=row["decision_ns"], market=row["market"],
            contract=row["actual_contract"], tick_nano=row["tick_nano"], policy=policy):
        return 0, "DECISION_QUOTE_ABSTENTION"
    spread = (quote["ask_nano"] - quote["bid_nano"]) / row["tick_nano"]
    cost = spread + policy["round_trip_fee_usd"] / (row["tick_value_nano_usd"] / 1e9)
    cost += terminal.ADVERSE_TICKS["stress"][MARKETS.index(row["market"])]
    return (1 if prediction > cost else -1 if prediction < -cost else 0), "DECISION_RECORDED"


def trade_record(prediction, row, policy):
    direction, reason = decision(prediction, row, policy)
    return _settle(direction, reason, prediction, row, policy)


def _settle(direction, reason, prediction, row, policy):
    net = {s: "0" for s in terminal.ADVERSE_TICKS}
    core = {"schema_version": "profit_trade/1.0.0", "mechanism_id": mechanism_id(),
        "opportunity_id": row["opportunity_id"], "trade_date": row["trade_date"], "market": row["market"],
        "quartile": row["quartile"],
        "input_id": sha256_json(row), "policy_id": policy["policy_id"], "prediction_ticks": prediction,
        "intended_direction": direction, "executed_direction": 0, "status": reason, "net_usd": net}
    if not direction:
        return bind(core, "trade_id")
    args = dict(market=row["market"], contract=row["actual_contract"], tick_nano=row["tick_nano"], policy=policy)
    if not usable_quote(row.get("entry_quote"), at_ns=row["entry_ns"], **args):
        return bind({**core, "status": "ENTRY_QUOTE_ABSTENTION"}, "trade_id")
    if not usable_quote(row.get("exit_quote"), at_ns=row["exit_ns"], **args):
        return bind({**core, "executed_direction": direction, "status": "INVALID_MISSING_EXIT", "net_usd": None}, "trade_id")
    entry = row["entry_quote"]["ask_nano" if direction > 0 else "bid_nano"]
    exit_ = row["exit_quote"]["bid_nano" if direction > 0 else "ask_nano"]
    ticks = direction * (exit_ - entry) // row["tick_nano"]
    tick_value = Decimal(row["tick_value_nano_usd"]) / Decimal(10**9)
    fee = Decimal(str(policy["round_trip_fee_usd"]))
    for scenario, values in terminal.ADVERSE_TICKS.items():
        net[scenario] = str((Decimal(ticks) - Decimal(str(values[MARKETS.index(row["market"])]))) * tick_value - fee)
    return bind({**core, "status": "EXECUTED_QUOTE_PROXY", "executed_direction": direction,
        "entry_price_nano": entry, "exit_price_nano": exit_, "quoted_gross_ticks": ticks}, "trade_id")


def daily_ledger(records, *, expected_ids, dates):
    dates = ordered_dates(dates)
    ids = require_unique_ascii_ids(tuple(expected_ids), name="scheduled opportunity IDs") if len(expected_ids) else ()
    if [r["opportunity_id"] for r in records] != list(ids):
        raise IntegrityError("trade ledger is incomplete, repeated or reordered")
    totals = {d: {s: Decimal(0) for s in terminal.ADVERSE_TICKS} for d in dates}
    counts = dict.fromkeys(dates, 0)
    quartiles = {q: {"executed_trades": 0, "net_usd": {s: Decimal(0) for s in terminal.ADVERSE_TICKS}} for q in range(1, 5)}
    for row in records:
        identity(row, "trade_id")
        if row["mechanism_id"] != mechanism_id() or row["trade_date"] not in totals:
            raise IntegrityError("trade ledger date/mechanism mismatch")
        if row["net_usd"] is None:
            raise IntegrityError("entered trade lacks exit evidence; P&L is incomplete")
        if set(row["net_usd"]) != set(terminal.ADVERSE_TICKS):
            raise IntegrityError("trade scenarios are incomplete")
        counts[row["trade_date"]] += int(row["executed_direction"] != 0)
        integer(row["quartile"], minimum=1)
        if row["quartile"] not in quartiles:
            raise IntegrityError("invalid ledger quartile")
        quartiles[row["quartile"]]["executed_trades"] += int(row["executed_direction"] != 0)
        for s in terminal.ADVERSE_TICKS:
            value = Decimal(row["net_usd"][s])
            if not value.is_finite():
                raise IntegrityError("nonfinite ledger amount")
            totals[row["trade_date"]][s] += value
            quartiles[row["quartile"]]["net_usd"][s] += value
    result = {s: str(sum((t[s] for t in totals.values()), Decimal(0))) for s in terminal.ADVERSE_TICKS}
    risk = {}
    for scenario in terminal.ADVERSE_TICKS:
        daily = np.array([float(t[scenario]) for t in totals.values()])
        curve = np.r_[0., np.cumsum([float(r["net_usd"][scenario]) for r in records])]
        risk[scenario] = {"daily_pnl_variance_usd_squared": float(np.var(daily)),
            "maximum_fixed_exit_drawdown_usd": float(np.max(np.maximum.accumulate(curve) - curve))}
    return bind({"schema_version": "profit_daily_ledger/1.0.0", "mechanism_id": mechanism_id(),
        "trade_ids": [r["trade_id"] for r in records], "dates": list(dates),
        "daily_usd": {d: {s: str(v) for s, v in t.items()} for d, t in totals.items()},
        "daily_trade_counts": counts, "total_usd": result,
        "mean_daily_stress_usd": float(Decimal(result["stress"]) / len(dates)),
        "descriptive_quartiles": {str(q): {"executed_trades": t["executed_trades"],
            "net_usd": {s: str(v) for s, v in t["net_usd"].items()}} for q, t in quartiles.items()},
        "descriptive_risk": risk, "intrabar_or_capital_return_claim": False,
        "executed_trades": sum(counts.values()), "authority": AUTHORITY}, "ledger_id")


def score_predictions(predictions, rows, policy, dates):
    if len(predictions) != len(rows):
        raise IntegrityError("predictions and opportunities differ")
    trades = [trade_record(float(p), row, policy) for p, row in zip(predictions, rows, strict=True)]
    return daily_ledger(trades, expected_ids=[r["opportunity_id"] for r in rows], dates=dates)


@dataclass(frozen=True)
class ProfitDataset:
    """Arrays plus complete opportunity identities; no file or provider access."""
    terminal_data: terminal.TerminalDataset
    opportunities: tuple[dict, ...]
    policy: dict
    coverage: dict

    def __post_init__(self):
        validate_policy(self.policy)
        d = self.terminal_data
        validate_coverage(self.coverage, len(self.opportunities))
        if len(self.opportunities) != len(d.sample_ids):
            raise IntegrityError("incomplete opportunity evidence")
        last = -1
        for i, row in enumerate(self.opportunities):
            validate_opportunity(row)
            integer(row.get("feature_available_at_ns"), minimum=1)
            integer(row.get("target_available_at_ns"), minimum=1)
            if row["feature_available_at_ns"] > row["decision_ns"] or row["target_available_at_ns"] < row["exit_ns"]:
                raise IntegrityError("feature or target availability violates chronology")
            if (row["opportunity_id"] != d.sample_ids[i] or row["trade_date"] != d.trade_dates[i]
                    or row["market"] != d.market or row["decision_ns"] <= last
                    or row["tick_value_nano_usd"] / 1e9 != d.tick_values_usd[i]):
                raise IntegrityError("opportunity/feature/economics alignment changed")
            last = row["decision_ns"]
        object.__setattr__(self, "_evidence_id", sha256_json([list(self.opportunities), self.policy, self.coverage]))

    def verify(self):
        if self._evidence_id != sha256_json([list(self.opportunities), self.policy, self.coverage]):
            raise IntegrityError("dataset evidence mutated")


def validate_coverage(coverage, observations):
    expected = coverage_report(**{k: coverage[k] for k in
        ("scheduled", "insufficient_history", "valid_feature", "invalid_outcome", "quality_errors")})
    if (coverage != expected or coverage["valid_outcome"] != observations
            or coverage["eligible_schedule_coverage"] < .98 or coverage["quality_errors"]
            or coverage["invalid_outcome"] or coverage["invalid_feature"]):
        raise IntegrityError("incomplete or invalid development evidence")


def fit_model(features, target, ids, dates, candidate, *, market, fit_kind="selection"):
    """Reuse predecessor numerical settings, with successor-specific fit bounds."""
    c = predecessor._candidate(candidate)
    x = finite_float64(features, name="fit features", ndim=2)
    y = finite_float64(target, name="fit target", ndim=1)
    ids = require_unique_ascii_ids(tuple(ids), name="fit IDs")
    if market not in MARKETS or fit_kind not in {"selection", "development_refit"}:
        raise IntegrityError("unsupported fitting operation")
    stop = "2023-01-01" if fit_kind == "selection" else "2025-07-14"
    ordered_dates(sorted(set(dates)))
    if (len(ids) != len(y) or x.shape != (len(y), len(terminal.FEATURE_NAMES))
            or len(dates) != len(y) or list(dates) != sorted(dates)
            or any(not "2018-01-01" <= d < stop for d in dates)):
        raise IntegrityError("fit inputs or development bounds differ")
    columns = terminal.retained_columns(x, c["feature_names"])
    core = {"mechanism_id": mechanism_id(), "market": market, "candidate": c, "fit_kind": fit_kind,
        "fit_sample_ids": list(ids), "fit_trade_dates": list(dates), "columns": columns,
        "training_id": sha256_json([x.tolist(), y.tolist()]), "dependency_versions": predecessor.VERSIONS,
        "training_mean_ticks": float(np.mean(y))}
    if c["family"] == "ridge":
        core["ridge"] = terminal.fitted_model(x, y, c)
    elif columns:
        xgb = predecessor._xgb()
        params = dict(predecessor.design()["phase_5"]["xgboost"])
        for k in ("rounds", "minimum_leaf_dates", "early_stopping", "seed_search"):
            params.pop(k)
        params.update(max_depth=c["max_depth"], base_score=float(np.mean(y)), min_child_weight=max(50, ceil(.05 * len(y))))
        matrix = xgb.DMatrix(x[:, columns], label=y)
        booster = xgb.train(params, matrix, num_boost_round=100)
        core.update(parameters=params, minimum_leaf_dates=predecessor.check_leaf_dates(
            booster.predict(matrix, pred_leaf=True), dates), model_json=bytes(booster.save_raw(raw_format="json")).decode())
    else:
        predecessor._xgb()
    return bind(core, "model_id")


def predict(model, features):
    identity(model, "model_id")
    if model["mechanism_id"] != mechanism_id() or model["dependency_versions"] != predecessor.VERSIONS:
        raise IntegrityError("model mechanism/version differs")
    c = predecessor._candidate(model["candidate"])
    x = finite_float64(features, name="prediction features", ndim=2)
    if x.shape[1] != len(terminal.FEATURE_NAMES):
        raise IntegrityError("prediction width differs")
    if c["family"] == "ridge":
        result = terminal.apply_fitted(model["ridge"], x)
    elif model["columns"]:
        xgb = predecessor._xgb()
        booster = xgb.Booster(params={"nthread": 1, "device": "cpu"})
        booster.load_model(bytearray(model["model_json"], "utf-8"))
        result = booster.predict(xgb.DMatrix(x[:, model["columns"]])).astype(np.float64)
    else:
        result = np.full(len(x), model["training_mean_ticks"])
    return finite_float64(result, name="terminal predictions", ndim=1)


def check_split_availability(dataset, fit_indices, audit_indices):
    if (not len(fit_indices) or not len(audit_indices)
            or max(dataset.opportunities[i]["target_available_at_ns"] for i in fit_indices)
            >= min(dataset.opportunities[i]["decision_ns"] for i in audit_indices)):
        raise IntegrityError("fit labels are not known before audit decisions")


def select_candidate(dataset, inner_folds):
    dataset.verify()
    d = dataset.terminal_data
    scores = []
    for c in predecessor.candidate_definitions():
        daily, retained, manifests = [], [], []
        try:
            for fold in inner_folds:
                f, a = fold.fit_indices, fold.audit_indices
                check_split_availability(dataset, f, a)
                if c["family"] == "ridge":
                    p, columns = terminal.fit_predict(d.features[f], d.terminal_ticks[f], d.features[a], c)
                else:
                    model = fit_model(d.features[f], d.terminal_ticks[f], [d.sample_ids[i] for i in f], [d.trade_dates[i] for i in f], c, market=d.market)
                    p, columns = predict(model, d.features[a]), model["columns"]
                dates = sorted(set(d.trade_dates[i] for i in a))
                report = score_predictions(p, [dataset.opportunities[i] for i in a], dataset.policy, dates)
                daily.extend(float(report["daily_usd"][day]["stress"]) for day in dates)
                retained.append(len(columns))
                manifests.append({"fit_ids": [d.sample_ids[i] for i in f], "audit_ids": [d.sample_ids[i] for i in a],
                    "daily_ledger_id": report["ledger_id"]})
            scores.append({**c, "status": "VALID", "score": float(np.mean(daily)),
                "retained_count": float(np.mean(retained)), "inner_manifests": manifests})
        except predecessor.LeafSupportError as exc:
            scores.append({**c, "status": "INVALID_LEAF_SUPPORT", "reason": str(exc)})
    return predecessor.choose_candidate(scores), scores


def run_market_wfa(dataset):
    dataset.verify()
    d = dataset.terminal_data
    predecessor._xgb()
    outputs = []
    for number, split in enumerate(terminal.fold_schedule(d)):
        winner, scores = select_candidate(dataset, split.inner_folds)
        ridge = predecessor.choose_candidate([s for s in scores if s["family"] == "ridge"])
        f, a = split.fit_indices, split.audit_indices
        check_split_availability(dataset, f, a)
        streams = {}
        for name, chosen in (("selected", winner), ("ridge_descriptive", ridge)):
            model = fit_model(d.features[f], d.terminal_ticks[f], [d.sample_ids[i] for i in f], [d.trade_dates[i] for i in f], chosen, market=d.market)
            p = predict(model, d.features[a])
            streams[name] = {"model": model, "predictions": p.tolist(), "candidate_id": chosen["candidate_id"]}
        outputs.append({"outer_fold": number, "fit_ids": [d.sample_ids[i] for i in f],
            "audit_indices": a.tolist(), "audit_ids": [d.sample_ids[i] for i in a],
            "candidate_scores": scores, "streams": streams})
    return bind({"mechanism_id": mechanism_id(), "market": d.market, "evidence_id": dataset._evidence_id,
        "outer_folds": outputs, "state": "FROZEN_PREDICTIONS_MECHANICS_ONLY", "authority": AUTHORITY}, "run_id")


def evaluate_wfa(dataset, run):
    identity(run, "run_id")
    if run != run_market_wfa(dataset):
        raise IntegrityError("run differs from complete deterministic selection/fitting replay")
    d = dataset.terminal_data
    indices = [i for f in run["outer_folds"] for i in f["audit_indices"]]
    rows = [dataset.opportunities[i] for i in indices]
    dates = sorted(set(d.trade_dates[i] for i in indices))
    reports = {}
    for name in ("selected", "ridge_descriptive"):
        predictions = [p for f in run["outer_folds"] for p in f["streams"][name]["predictions"]]
        reports[name] = score_predictions(predictions, rows, dataset.policy, dates)
    # Baselines use the same quote accounting and opportunity set. Forced signs
    # express their declared exposure; they remain subject to quote abstentions.
    for name in predecessor.design()["phase_6"]["baselines"]:
        signals = []
        for fold, split in zip(run["outer_folds"], terminal.fold_schedule(d), strict=True):
            a = split.audit_indices
            previous = np.sign(d.features[a, terminal.FEATURE_NAMES.index("close_change_ticks_1")])
            signs = {"flat_no_trade": np.zeros(len(a)), "always_long": np.ones(len(a)),
                "always_short": -np.ones(len(a)), "previous_bar_sign_momentum": previous,
                "previous_bar_sign_reversal": -previous,
                "fold_local_unconditional_direction": np.full(len(a), np.sign(np.mean(d.terminal_ticks[split.fit_indices])))}[name]
            signals.extend(signs.astype(int).tolist())
        trades = []
        for sign, row in zip(signals, rows, strict=True):
            _, reason = decision(0., row, dataset.policy)
            direction = 0 if reason == "DECISION_QUOTE_ABSTENTION" else sign
            trades.append(_settle(direction, reason, None, row, dataset.policy))
        reports[name] = daily_ledger(trades, expected_ids=[r["opportunity_id"] for r in rows], dates=dates)
    return bind({"mechanism_id": mechanism_id(), "run_id": run["run_id"], "market": d.market,
        "reports": reports, "state": "DEVELOPMENT_EVIDENCE_NOT_QUALIFICATION",
        "qualification": False, "benchmark_superiority_claim": False, "authority": AUTHORITY}, "result_id")


def freeze_development_model(dataset, run, *, frozen_at_ns):
    """One final candidate from inner selection, never the best outer result."""
    integer(frozen_at_ns, minimum=1)
    known = max(r["target_available_at_ns"] for r in dataset.opportunities)
    if known >= frozen_at_ns:
        raise IntegrityError("development freeze precedes complete training-label availability")
    if run != run_market_wfa(dataset):
        raise IntegrityError("selection run does not replay")
    d = dataset.terminal_data
    chosen = run["outer_folds"][-1]["streams"]["selected"]["model"]["candidate"]
    model = fit_model(d.features, d.terminal_ticks, d.sample_ids, d.trade_dates, chosen, market=d.market)
    return bind({k: v for k, v in {**model, "frozen_at_ns": frozen_at_ns,
        "maximum_target_available_at_ns": known}.items() if k != "model_id"}, "model_id")


def historical_stress_once(directory, *, model, features, rows, policy, dates, expected_ids, coverage):
    """In-memory synthetic mechanics only; consumes the attempt before scoring.

    This has no source reader. A real-data adapter and its authorization remain
    absent. Failed or interrupted attempts cannot be rerun in this store.
    """
    identity(model, "model_id")
    integer(model.get("frozen_at_ns"), minimum=1)
    integer(model.get("maximum_target_available_at_ns"), minimum=1)
    dates = ordered_dates(dates)
    validate_coverage(coverage, len(rows))
    if (model["fit_kind"] != "selection" or dates[0] < "2023-01-01" or dates[-1] >= "2025-07-14"
            or [r["opportunity_id"] for r in rows] != list(expected_ids)
            or not model["maximum_target_available_at_ns"] < model["frozen_at_ns"]
            < min(r["decision_ns"] for r in rows)):
        raise IntegrityError("historical stress scope differs")
    for row in rows:
        validate_opportunity(row)
        if (row["market"] != model["market"] or row["trade_date"] not in dates
                or row["exit_ns"] >= DEVELOPMENT_CUTOFF_NS
                or row["feature_available_at_ns"] > row["decision_ns"]):
            raise IntegrityError("stress row crosses market, availability or protected cutoff")
    x = finite_float64(features, name="stress features", ndim=2)
    declaration = bind({"model_id": model["model_id"], "policy_id": policy["policy_id"],
        "input_id": sha256_json([x.tolist(), rows, list(dates), coverage]), "authority": AUTHORITY}, "attempt_id")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    try:
        with (directory / f"stress-{model['market']}-attempt.json").open("xb") as stream:
            stream.write(canonical_bytes(declaration) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise IntegrityError("one-shot historical stress attempt already consumed") from exc
    ledger = score_predictions(predict(model, x), rows, policy, dates)
    result = bind({"attempt_id": declaration["attempt_id"], "model": model, "ledger": ledger,
        "stress_sample_ids": list(expected_ids), "stress_trade_dates": [r["trade_date"] for r in rows],
        "stress_feature_id": sha256_json(x.tolist()),
        "passed": Decimal(ledger["total_usd"]["stress"]) > 0,
        "state": "SYNTHETIC_DEVELOPMENT_STRESS_ONLY", "authority": AUTHORITY}, "stress_id")
    write_new_canonical_authority(directory / f"stress-{model['market']}-result.json", result)
    return result


def refit_for_freeze(stress, *, features, target, ids, dates, target_available_at_ns, frozen_at_ns):
    """Parameter-only refit of the fixed candidate; returns no evaluation score."""
    identity(stress, "stress_id")
    if not stress["passed"] or Decimal(stress["ledger"]["total_usd"]["stress"]) <= 0:
        raise IntegrityError("failed development stress cannot advance")
    integer(frozen_at_ns, minimum=1)
    if len(target_available_at_ns) != len(ids) or not len(ids):
        raise IntegrityError("refit label evidence is incomplete")
    for t in target_available_at_ns:
        integer(t, minimum=1)
        if t >= min(DEVELOPMENT_CUTOFF_NS, frozen_at_ns):
            raise IntegrityError("refit label is future-known or protected")
    old = stress["model"]
    expected_ids = old["fit_sample_ids"] + stress["stress_sample_ids"]
    expected_dates = old["fit_trade_dates"] + stress["stress_trade_dates"]
    if list(ids) != expected_ids or list(dates) != expected_dates:
        raise IntegrityError("refit must preserve every original and stress sample in order")
    x = finite_float64(features, name="refit features", ndim=2)
    y = finite_float64(target, name="refit targets", ndim=1)
    n = len(old["fit_sample_ids"])
    if (sha256_json([x[:n].tolist(), y[:n].tolist()]) != old["training_id"]
            or sha256_json(x[n:].tolist()) != stress["stress_feature_id"]):
        raise IntegrityError("refit changed original fit data or stress features")
    model = fit_model(x, y, ids, dates, old["candidate"], market=old["market"], fit_kind="development_refit")
    return bind({k: v for k, v in {**model, "frozen_at_ns": frozen_at_ns,
        "maximum_target_available_at_ns": max(target_available_at_ns),
        "development_stress_id": stress["stress_id"]}.items() if k != "model_id"}, "model_id")
