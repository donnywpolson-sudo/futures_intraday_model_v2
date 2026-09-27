"""Versioned, descriptive views of L0 replay results. No research or source reader.

The unversioned replay/evaluation artifacts remain evidence. This derived report
adds availability, scope and lineage without changing selection or qualification.
See docs/L0_REPORTING.md for units, accounting and inference prerequisites.
"""
from __future__ import annotations

import math
from copy import deepcopy
from datetime import date, datetime

import numpy as np

from .canonical import sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .research.bootstrap import stationary_bootstrap_index_rows

SCHEMA = "l0_alpha_report/2"
PNL_BASIS = "CLOSED_TRADE_DAILY_REFERENCE_PNL;NOT_INTRABAR_OR_ACCOUNT_EQUITY"
STAGES = {
    "INNER_DEVELOPMENT": "Evidence used for selection and redesign.",
    "OUTER_CHRONOLOGICAL": "Evidence about the procedure frozen before this evaluation; result-informed redesign makes reuse exploratory.",
    "FINAL_SEALED": "Governed confirmation under a previously frozen decision rule; observations excluded from ordinary reports.",
    "FORWARD": "Evidence for the version frozen before these observations arrived; observations excluded from ordinary L0 reports.",
    "UNKNOWN": "Evaluation stage and independence are unknown.",
}
DEVELOPMENT_STAGES = ("INNER_DEVELOPMENT", "OUTER_CHRONOLOGICAL")
SCOPE_FIELDS = (
    "dates", "instruments", "opportunity_set", "calendar_id", "session_boundaries",
    "timezone", "cutoff_convention", "evaluation_stage", "pnl_basis",
    "observation_cutoff_utc", "execution_assumptions", "cost_assumptions",
    "sizing_basis", "fold_by_date", "segment_by_date", "data_kind",
)
POLICY_FIELDS = ("model", "features", "selection", "refitting", "entry", "exit", "sizing",
                 "opportunity_set", "execution", "costs")
IDENTITY_FIELDS = ("candidate_id", "experiment_id", "run_id", "attempt_id", "parent_candidate_id",
                   "hypothesis", "intended_change", "started_at_utc", "finished_at_utc",
                   "source_of_return", "benchmark_exposure", "stress_liquidation_assumptions",
                   "benchmark_id", "portfolio_experiment", "operation_id")
CAPITAL_METRICS = ("cagr", "percentage_drawdown", "account_sharpe", "account_sortino", "ulcer_index")


def unavailable(reason, unit=None):
    return {"value": None, "unit": unit, "availability": "UNAVAILABLE", "reason": reason}


def measured(value, unit):
    if isinstance(value, float) and not math.isfinite(value):
        return unavailable("NONFINITE_RESULT", unit)
    return {"value": value, "unit": unit, "availability": "AVAILABLE", "reason": None}


def _number(value):
    return type(value) in (int, float) and math.isfinite(value)


def _time(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.utcoffset() is not None else None
    except (AttributeError, ValueError, TypeError):
        return None


def _known(value):
    if value is None or value == "UNKNOWN" or value == "":
        return False
    if isinstance(value, dict):
        return bool(value) and all(_known(v) for v in value.values())
    if isinstance(value, list):
        return bool(value) and all(_known(v) for v in value)
    return True


def check_scope(scope, *, observations=False):
    """Check metadata BEFORE looking at supplied results or opening a report file."""
    stage = scope.get("evaluation_stage", "UNKNOWN")
    dates = scope.get("dates")
    if stage not in STAGES:
        raise IntegrityError("unknown evaluation stage")
    if observations and stage not in DEVELOPMENT_STAGES:
        raise UnauthorizedOperation("ordinary L0 reports exclude sealed, forward and unknown-stage observations")
    if dates is not None:
        if not isinstance(dates, list) or dates != sorted(set(dates)):
            raise IntegrityError("declared report dates must be ordered and unique")
        for day in dates:
            try:
                parsed = date.fromisoformat(day)
            except (ValueError, TypeError) as exc:
                raise IntegrityError("invalid report date") from exc
            if day != parsed.isoformat():
                raise IntegrityError("report date must be YYYY-MM-DD")
            if observations and not "2018-01-01" <= day < "2023-01-01":
                raise UnauthorizedOperation("ordinary L0 observations are limited to 2018-2022 development dates")
    if observations and not dates:
        raise IntegrityError("declared evaluation calendar required before observations")
    if observations and scope.get("pnl_basis") != PNL_BASIS:
        raise IntegrityError("unsupported L0 P&L accounting basis")
    cutoff = scope.get("observation_cutoff_utc")
    if cutoff is not None and _time(cutoff) is None:
        raise IntegrityError("observation availability cutoff must be an offset-aware timestamp or null")


def lineage_interpretation(lineage, stage):
    informed = lineage.get("informed_by_results")
    freeze, exposed = _time(lineage.get("frozen_at_utc")), _time(lineage.get("first_result_exposure_utc"))
    if informed:
        independence = "RESULT_INFORMED_REVISION_REUSE_IS_EXPLORATORY"
    elif freeze is None or exposed is None or lineage.get("exposure_history_complete") is not True:
        independence = "UNKNOWN"
    elif freeze >= exposed:
        independence = "RESULTS_EXPOSED_BEFORE_FREEZE"
    elif stage == "INNER_DEVELOPMENT":
        independence = "DEVELOPMENT_SELECTION_NOT_CONFIRMATION"
    else:
        independence = "FREEZE_PRECEDES_RECORDED_EXPOSURE_NOT_PROOF_OF_INDEPENDENCE"
    return {"independence": independence, "stage_interpretation": STAGES[stage],
            "selection_bias_removed": False}


def drawdown(dates, values):
    """Positive dollar loss from initial zero; durations count evaluation dates."""
    peak = level = maximum = 0.0
    peak_date = None
    episode = None
    episodes, underwater = [], 0
    for day, value in zip(dates, values, strict=True):
        level += value
        depth = peak - level
        maximum = max(maximum, depth)
        if depth > 0:
            underwater += 1
            if episode is None:
                episode = {"peak_date": peak_date, "start_date": day, "trough_date": day,
                           "depth_usd": depth, "underwater_dates": 0, "recovery_date": None,
                           "status": "UNRECOVERED"}
            episode["underwater_dates"] += 1
            if depth > episode["depth_usd"]:
                episode.update(depth_usd=depth, trough_date=day)
        else:
            if episode is not None:
                episode.update(status="RECOVERED", recovery_date=day)
                episodes.append(episode)
                episode = None
            peak, peak_date = level, day
    if episode is not None:
        episodes.append(episode)
    return {"basis": PNL_BASIS, "initial_baseline_usd": 0.0, "maximum_depth_usd": maximum,
            "time_underwater_dates": underwater, "evaluated_dates": len(dates),
            "longest_underwater_dates": max((e["underwater_dates"] for e in episodes), default=0),
            "current_episode": episode, "completed_recoveries": sum(e["status"] == "RECOVERED" for e in episodes),
            "episodes": episodes, "duration_unit": "DECLARED_EVALUATION_DATES_BELOW_PEAK",
            "limitation": "Closed-trade reference series omits intratrade and open-position losses; not account-equity risk."}


def _daily_rows(scope, replay):
    dates = scope.get("dates") or []
    if replay is None:
        source = {}
    else:
        source = replay.get("daily", {})
        if not isinstance(source, dict):
            raise IntegrityError("daily replay must be a date-keyed mapping")
    # Check every key before inspecting its values. Extra dates are never silently joined away.
    check_scope({**scope, "dates": sorted(source)}, observations=bool(source))
    extra = [{"date": day, "reason": "OUTSIDE_DECLARED_EVALUATION_CALENDAR"} for day in sorted(set(source) - set(dates))]
    unresolved = (replay or {}).get("unresolved_position")
    if unresolved and unresolved.get("trade_date") not in dates:
        unresolved_day = dates[0] if dates else None
    else:
        unresolved_day = unresolved.get("trade_date") if unresolved else None
    rows = []
    for day in dates:
        raw = source.get(day)
        reason = None
        if raw is None:
            reason = "MISSING_DAILY_RECORD_NO_ZERO_IMPUTATION"
        elif not isinstance(raw, dict) or raw.get("complete") is not True:
            reason = "DAILY_ACCOUNTING_NOT_EXPLICITLY_COMPLETE"
        elif unresolved_day is not None and day >= unresolved_day:
            reason = "UNRESOLVED_EXECUTION_OR_SUBSEQUENT_ACCOUNTING"
        elif not all(_number(raw.get(k)) for k in ("gross_usd", "cost_usd", "net_usd")):
            reason = "MISSING_OR_NONFINITE_DAILY_ACCOUNTING"
        elif (raw["cost_usd"] < 0 or not math.isclose(raw["gross_usd"] - raw["cost_usd"], raw["net_usd"], rel_tol=0, abs_tol=1e-7)):
            reason = "GROSS_COST_NET_DO_NOT_RECONCILE"
        elif type(raw.get("trades")) is not int or raw["trades"] < 0:
            reason = "TRADE_COUNT_UNKNOWN_OR_INVALID"
        rows.append({"date": day, "fold_id": (scope.get("fold_by_date") or {}).get(day),
                     "segment_id": (scope.get("segment_by_date") or {}).get(day),
                     "availability": "AVAILABLE" if reason is None else "UNAVAILABLE", "reason": reason,
                     **{k: raw[k] if reason is None else None for k in ("gross_usd", "cost_usd", "net_usd", "trades")}})
    return rows, extra


def _metrics(rows, complete):
    reason = "RUN_OR_ACCOUNTING_INCOMPLETE"
    metrics = {k: unavailable(reason, unit) for k, unit in (
        ("gross_usd", "USD"), ("cost_usd", "USD"), ("net_usd", "USD"),
        ("mean_daily_net_usd", "USD/evaluation date"), ("daily_sample_variance", "USD^2"),
        ("trade_count", "closed round trips"), ("trading_dates", "evaluation dates"),
        ("zero_trade_dates", "evaluation dates"), ("mean_net_per_trade", "USD/closed round trip"))}
    if complete:
        values = [r["net_usd"] for r in rows]
        count = sum(r["trades"] for r in rows)
        for key in ("gross_usd", "cost_usd", "net_usd"):
            metrics[key] = measured(math.fsum(r[key] for r in rows), "USD")
        metrics.update(mean_daily_net_usd=measured(math.fsum(values) / len(rows), "USD/evaluation date"),
                       trade_count=measured(count, "closed round trips"),
                       trading_dates=measured(sum(r["trades"] > 0 for r in rows), "evaluation dates"),
                       zero_trade_dates=measured(sum(r["trades"] == 0 for r in rows), "evaluation dates"),
                       daily_sample_variance=measured(float(np.var(values, ddof=1)), "USD^2") if len(rows) > 1 else unavailable("FEWER_THAN_TWO_DATES", "USD^2"),
                       mean_net_per_trade=measured(math.fsum(values) / count, "USD/closed round trip") if count else unavailable("ZERO_TRADES", "USD/closed round trip"))
    for key in CAPITAL_METRICS:
        metrics[key] = unavailable("L0_REFERENCE_PNL_IS_NOT_A_CAPITAL_RETURN_SERIES;REQUIRES_CAPITAL_FINANCING_SIZING_CASHFLOWS_VALUATION_AND_RETURN_DEFINITION")
    metrics["deflated_sharpe"] = unavailable("VALID_RETURN_SERIES_AND_DEFENSIBLE_SEARCH_DISPERSION_AND_DEPENDENCE_EVIDENCE_REQUIRED;NO_ALPHA_CERTIFICATION")
    for key in ("var", "cvar"):
        metrics[key] = unavailable("UNITS_HORIZON_CONFIDENCE_ESTIMATION_METHOD_AND_VALID_INPUTS_NOT_DECLARED")
    return metrics


def _concentration(rows, complete):
    if not complete:
        return unavailable("RUN_OR_ACCOUNTING_INCOMPLETE")
    ranked = sorted(rows, key=lambda r: (-r["net_usd"], r["date"]))
    positive = math.fsum(max(r["net_usd"], 0) for r in rows)
    net = math.fsum(r["net_usd"] for r in rows)
    top = math.fsum(max(r["net_usd"], 0) for r in ranked[:5])
    return {"largest_winning_days": [{"date": r["date"], "net_usd": r["net_usd"]} for r in ranked[:5] if r["net_usd"] > 0],
            "largest_losing_days": [{"date": r["date"], "net_usd": r["net_usd"]} for r in reversed(ranked[-5:]) if r["net_usd"] < 0],
            "top_five_share_of_positive_day_profits": measured(top / positive, "fraction of positive-day profits") if positive else unavailable("NO_POSITIVE_DAY_PROFITS"),
            "top_five_share_of_total_net_profit": measured(top / net, "fraction of total net profit; may exceed one") if net > 0 else unavailable("NONPOSITIVE_TOTAL_NET_PROFIT")}


def _trade_diagnostics(scope, replay, metrics, complete):
    empty = {name: unavailable("COMPLETE_TRADE_LEDGER_REQUIRED") for name in
             ("itemized_costs", "exposure", "turnover", "exceptional_trades", "fixed_trade_cost_sensitivity")}
    if not complete or not isinstance((replay or {}).get("trades"), list):
        return empty
    trades = replay["trades"]
    if len(trades) != metrics["trade_count"]["value"]:
        return empty
    keys = ("gross_nano_usd", "cost_nano_usd", "net_nano_usd", "entry_ns", "exit_ns")
    if any(any(type(t.get(k)) is not int for k in keys) or t.get("direction") not in (-1, 1)
           or t["cost_nano_usd"] < 0 or t["exit_ns"] < t["entry_ns"] or t["gross_nano_usd"] - t["cost_nano_usd"] != t["net_nano_usd"] for t in trades):
        return empty
    if any(not math.isclose(sum(t[k.replace("_usd", "_nano_usd")] for t in trades) / 1e9,
                            metrics[k]["value"], rel_tol=0, abs_tol=1e-7) for k in ("gross_usd", "cost_usd", "net_usd")):
        return empty
    sizing = scope.get("sizing_basis")
    if sizing == "ONE_STANDARD_CONTRACT":
        empty["exposure"] = {"long_contract_minutes": sum((t["exit_ns"] - t["entry_ns"]) / 60e9 for t in trades if t["direction"] == 1),
                             "short_contract_minutes": sum((t["exit_ns"] - t["entry_ns"]) / 60e9 for t in trades if t["direction"] == -1),
                             "denominator_evaluation_dates": len(scope["dates"]), "basis": "RESOLVED_CLOSED_TRADES_ONLY"}
        empty["turnover"] = measured(2 * len(trades), "one-way standard-contract units")
    ranked = sorted(trades, key=lambda t: t["net_nano_usd"], reverse=True)
    empty["exceptional_trades"] = {"largest_winners_usd": [t["net_nano_usd"] / 1e9 for t in ranked[:5] if t["net_nano_usd"] > 0],
                                   "largest_losers_usd": [t["net_nano_usd"] / 1e9 for t in reversed(ranked[-5:]) if t["net_nano_usd"] < 0]}
    costs = scope.get("cost_assumptions") or {}
    fee = costs.get("round_trip_fee_usd")
    allowances = costs.get("round_trip_allowances_ticks")
    if costs.get("basis") != "ASSUMED_REFERENCE_ALLOWANCES" or not _number(fee) or fee < 0:
        return empty
    total = metrics["cost_usd"]["value"]
    if total < fee * len(trades):
        return empty
    empty["itemized_costs"] = {"basis": "ASSUMED_NOT_OBSERVED", "fees_usd": fee * len(trades),
                               "price_allowance_usd": total - fee * len(trades), "total_usd": total,
                               "already_deducted_in_net_pnl": True}
    if isinstance(allowances, dict) and all(_number(v) and v >= 0 for v in allowances.values()) and all(
            type(t.get("tick_value_nano_usd")) is int and t["tick_value_nano_usd"] > 0 for t in trades):
        empty["fixed_trade_cost_sensitivity"] = {
            "basis": "FIXED_SELECTION_TRADE_LIST_REPRICED_NOT_POLICY_REPLAY",
            "scenarios": {name: {"net_usd": sum(t["gross_nano_usd"] / 1e9 - ticks * t["tick_value_nano_usd"] / 1e9 - fee for t in trades),
                                  "trade_count": len(trades)} for name, ticks in sorted(allowances.items())},
            "remaining_cost_headroom_usd_per_round_trip": measured(metrics["net_usd"]["value"] / len(trades), "USD/round trip") if trades else unavailable("ZERO_TRADES")}
    return empty


def execution_cost_drift(orders):
    """Decision-to-fill implementation shortfall; diagnostic, never a P&L debit."""
    if not orders:
        return unavailable("NO_EXECUTION_OBSERVATIONS;REFERENCE_PRICES_ARE_NOT_FILLS")
    details = []
    for order in orders:
        decision = _time(order.get("decision_time_utc"))
        quantity, multiplier = order.get("requested_quantity"), order.get("multiplier_usd_per_price_unit")
        if (decision is None or order.get("side") not in ("BUY", "SELL") or
                not all(_number(v) for v in (order.get("decision_price"), quantity, multiplier)) or quantity <= 0 or multiplier <= 0):
            return unavailable("INVALID_DECISION_PRICE_TIME_SIDE_SIZE_OR_MULTIPLIER")
        fills = order.get("fills")
        if not isinstance(fills, list):
            return unavailable("FILL_LEDGER_MISSING")
        filled = cost = fees = 0.0
        for fill in fills:
            at = _time(fill.get("fill_time_utc"))
            if (at is None or at < decision or not all(_number(fill.get(k)) for k in ("price", "quantity", "fee_usd"))
                    or fill["quantity"] <= 0 or fill["fee_usd"] < 0):
                return unavailable("INVALID_FILL_OR_TIMESTAMP")
            filled += fill["quantity"]
            fees += fill["fee_usd"]
            cost += (1 if order["side"] == "BUY" else -1) * (fill["price"] - order["decision_price"]) * fill["quantity"] * multiplier
        if filled > quantity:
            return unavailable("FILLED_SIZE_EXCEEDS_REQUEST")
        status = "FILLED" if filled == quantity else "PARTIAL" if filled else "MISSED"
        if order.get("execution_status") != status:
            return unavailable("EXECUTION_STATUS_AND_FILL_SIZE_DISAGREE")
        assumed = order.get("assumed_cost_for_filled_quantity_usd")
        details.append({"order_id": order.get("order_id"), "status": status, "requested_quantity": quantity,
                        "filled_quantity": filled, "unfilled_quantity": quantity - filled,
                        "price_shortfall_usd": cost if filled else None, "fees_usd": fees,
                        "observed_cost_usd": cost + fees if filled else None,
                        "observed_minus_assumed_usd": cost + fees - assumed if filled and _number(assumed) else None})
    return {"orders": details, "basis": "SIGNED_DECISION_PRICE_TO_VOLUME_WEIGHTED_FILLS_PLUS_FEES",
            "cost_per_filled_unit": measured(sum((d["observed_cost_usd"] or 0) for d in details) / sum(d["filled_quantity"] for d in details), "USD/filled unit") if sum(d["filled_quantity"] for d in details) else unavailable("NO_FILLED_UNITS"),
            "missed_opportunity_cost": unavailable("UNFILLED_COUNTERFACTUAL_AND_HORIZON_NOT_DECLARED"),
            "additional_pnl_deduction_usd": 0,
            "limitation": "Price movement is diagnostic attribution, not causal market impact; do not deduct it again from fill-based P&L."}


def candidate_report(context, replay=None, *, execution_status=None, failure_reason=None, execution_observations=None):
    scope = {k: deepcopy(context.get("scope", {}).get(k)) for k in SCOPE_FIELDS}
    scope["evaluation_stage"] = scope["evaluation_stage"] or "UNKNOWN"
    check_scope(scope, observations=replay is not None or execution_observations is not None)
    if replay is not None and replay.get("scenario") is not None and replay["scenario"] != (scope.get("cost_assumptions") or {}).get("scenario"):
        raise IntegrityError("replay cost scenario and declaration differ")
    rows, extra = _daily_rows(scope, replay) if scope["evaluation_stage"] in DEVELOPMENT_STAGES else ([], [])
    lineage = {k: deepcopy(context.get("lineage", {}).get(k)) for k in (
        "frozen_at_utc", "first_result_exposure_utc", "exposure_history_complete", "informed_by_results",
        "declared_family", "search_space", "attempted_settings", "selection_metric", "decision_rule",
        "human_changes", "historical_search", "effective_independent_trials", "effective_trial_method", "selection_inputs")}
    supplied_status = execution_status or ("COMPLETE" if (replay or {}).get("complete") is True else "INCOMPLETE")
    if supplied_status not in ("COMPLETE", "FAILED", "INCOMPLETE", "ABANDONED"):
        raise IntegrityError("execution status must be separate from stage and metrics")
    good = [r for r in rows if r["availability"] == "AVAILABLE"]
    complete = supplied_status == "COMPLETE" and bool(rows) and len(good) == len(rows) and not extra and not (replay or {}).get("unresolved_position")
    metrics = _metrics(rows, complete)
    no_trade = complete and all(r["trades"] == 0 and r["gross_usd"] == r["cost_usd"] == r["net_usd"] == 0 for r in rows)
    missing_scope = [k for k, v in scope.items() if not _known(v)]
    policy = {k: deepcopy(context.get("policy", {}).get(k)) for k in POLICY_FIELDS}
    comparison_reasons = (["RUN_OR_ACCOUNTING_INCOMPLETE"] if not complete else []) + ["UNKNOWN_SCOPE:" + k for k in missing_scope]
    comparison_reasons += ["UNKNOWN_POLICY:" + k for k, v in policy.items() if not _known(v)]
    folds = []
    for fold_id in dict.fromkeys(r["fold_id"] for r in rows):
        subset = [r for r in rows if r["fold_id"] == fold_id]
        available = [r for r in subset if r["availability"] == "AVAILABLE"]
        folds.append({"fold_id": fold_id, "expected_dates": len(subset), "available_dates": len(available),
                      "net_usd": math.fsum(r["net_usd"] for r in available) if complete else None,
                      "mean_daily_net_usd": math.fsum(r["net_usd"] for r in available) / len(subset) if complete else None})
    report = {"schema_version": SCHEMA, "legacy_contract": "L0_UNVERSIONED_REPLAY_AND_EVALUATION",
              "identity": {k: deepcopy(context.get(k)) for k in IDENTITY_FIELDS}, "scope": scope, "policy": policy,
              "provenance": deepcopy(context.get("provenance", {})), "lineage": lineage,
              "record_kind": context.get("record_kind", "ATTEMPT"),
              "information_use": lineage_interpretation(lineage, scope["evaluation_stage"]),
              "declared_benchmarks": deepcopy(context.get("declared_benchmarks")),
              "intended_policy_changes": deepcopy(context.get("intended_policy_changes")),
              "execution": {"status": supplied_status, "reason": failure_reason, "valid_no_trade": no_trade,
                            "accounting_status": "COMPLETE" if complete else "INCOMPLETE_OR_UNAVAILABLE"},
              "metric_eligibility": {"complete_reference_economics": complete, "capital_return_metrics": False},
              "comparison_eligibility": {"descriptive": not comparison_reasons, "reasons": comparison_reasons,
                                         "confirmatory": False, "winner_selection": False,
                                         "decision_dependency": "NO_L0_PAIRED_CONFIRMATION_RULE;PREDECESSOR_STATISTICAL_FAILURE_PRESERVED"},
              "coverage": {"expected_dates": len(scope["dates"] or []), "available_dates": len(good),
                           "missing_or_unresolved_dates": [{"date": r["date"], "reason": r["reason"]} for r in rows if r["availability"] != "AVAILABLE"],
                           "excluded_dates": extra, "unresolved_execution": deepcopy((replay or {}).get("unresolved_position")),
                           "source_and_opportunity_coverage": deepcopy(context.get("source_and_opportunity_coverage"))},
              "daily": rows, "metrics": metrics, "folds": folds,
              "drawdown": drawdown([r["date"] for r in rows], [r["net_usd"] for r in rows]) if complete else unavailable("INCOMPLETE_REFERENCE_SERIES"),
              "profit_concentration": _concentration(rows, complete),
              "partial_diagnostic": {"label": "AVAILABLE_DATES_ONLY_NOT_FULL_RUN_OR_WINNER_EVIDENCE", "dates": len(good),
                                     "net_usd": math.fsum(r["net_usd"] for r in good) if good else None},
              "trade_diagnostics": _trade_diagnostics(scope, replay, metrics, complete),
              "execution_cost_drift": execution_cost_drift(execution_observations),
              "alpha_established": False, "research_gate_passed": False, "deployment_ready": False}
    report["report_id"] = sha256_json(report)
    return report


def validate_report(report):
    scope = report.get("scope", {})
    check_scope(scope, observations=bool(report.get("daily")))
    if report.get("schema_version") != SCHEMA or report.get("report_id") != sha256_json({k: v for k, v in report.items() if k != "report_id"}):
        raise IntegrityError("candidate report schema or identity differs")
    if any(report.get(k) is not False for k in ("alpha_established", "research_gate_passed", "deployment_ready")):
        raise IntegrityError("reporting cannot establish alpha, pass a gate or deploy")
    if report["comparison_eligibility"]["confirmatory"] or report["comparison_eligibility"]["winner_selection"]:
        raise IntegrityError("L0 report cannot select or promote a winner")
    if set(scope) != set(SCOPE_FIELDS) or set(report["policy"]) != set(POLICY_FIELDS):
        raise IntegrityError("report scope or policy fields are incomplete")
    if scope["evaluation_stage"] not in DEVELOPMENT_STAGES and report["daily"]:
        raise UnauthorizedOperation("metadata-only stages cannot contain observations")
    if scope["evaluation_stage"] in DEVELOPMENT_STAGES and [r["date"] for r in report["daily"]] != (scope["dates"] or []):
        raise IntegrityError("report daily alignment changed")
    complete = report["metric_eligibility"]["complete_reference_economics"]
    rows = report["daily"]
    for row in rows:
        if row["availability"] not in ("AVAILABLE", "UNAVAILABLE") or any(
                row[field] != (scope[mapping] or {}).get(row["date"])
                for field, mapping in (("fold_id", "fold_by_date"), ("segment_id", "segment_by_date"))):
            raise IntegrityError("daily availability or fold mapping differs")
        if row["availability"] == "AVAILABLE" and (not all(_number(row[k]) for k in ("gross_usd", "cost_usd", "net_usd"))
                or row["cost_usd"] < 0 or type(row["trades"]) is not int or row["trades"] < 0
                or not math.isclose(row["gross_usd"] - row["cost_usd"], row["net_usd"], rel_tol=0, abs_tol=1e-7)):
            raise IntegrityError("invalid available daily accounting")
    expected_complete = (report["execution"]["status"] == "COMPLETE" and bool(rows)
                         and all(r["availability"] == "AVAILABLE" for r in rows)
                         and not report["coverage"]["excluded_dates"] and not report["coverage"]["unresolved_execution"])
    if type(complete) is not bool or complete != expected_complete:
        raise IntegrityError("execution, coverage and metric eligibility disagree")
    no_trade = complete and all(r["trades"] == 0 and r["gross_usd"] == r["cost_usd"] == r["net_usd"] == 0 for r in rows)
    if (report["execution"]["status"] not in ("COMPLETE", "INCOMPLETE", "FAILED", "ABANDONED")
            or report["execution"]["valid_no_trade"] != no_trade
            or report["execution"]["accounting_status"] != ("COMPLETE" if complete else "INCOMPLETE_OR_UNAVAILABLE")
            or report["coverage"]["expected_dates"] != len(scope["dates"] or [])
            or report["coverage"]["available_dates"] != sum(r["availability"] == "AVAILABLE" for r in rows)):
        raise IntegrityError("execution or reported coverage differs from accounting")
    if report["metrics"] != _metrics(report["daily"], complete):
        raise IntegrityError("report metrics do not match the shared calculation")
    reasons = (["RUN_OR_ACCOUNTING_INCOMPLETE"] if not complete else [])
    reasons += ["UNKNOWN_SCOPE:" + k for k in SCOPE_FIELDS if not _known(scope[k])]
    reasons += ["UNKNOWN_POLICY:" + k for k in POLICY_FIELDS if not _known(report["policy"][k])]
    if report["comparison_eligibility"]["reasons"] != reasons or report["comparison_eligibility"]["descriptive"] != (not reasons):
        raise IntegrityError("comparison readiness does not match known accounting and policy")
    expected_drawdown = drawdown([r["date"] for r in rows], [r["net_usd"] for r in rows]) if complete else unavailable("INCOMPLETE_REFERENCE_SERIES")
    if report["drawdown"] != expected_drawdown or report["profit_concentration"] != _concentration(rows, complete):
        raise IntegrityError("report diagnostics differ from shared calculations")
    if report["information_use"] != lineage_interpretation(report["lineage"], scope["evaluation_stage"]):
        raise IntegrityError("information-use interpretation differs")


def combine_folds(context, reports):
    """Concatenate declared disjoint folds; never choose folds or rerun a policy."""
    check_scope(context["scope"], observations=True)
    ctx = deepcopy(context)
    daily, seen = {}, set()
    complete = bool(reports)
    statuses = []
    policies = {name: {} for name in ("model", "features", "entry")}
    for report in reports:
        validate_report(report)
        for key in SCOPE_FIELDS:
            if key not in ("dates", "fold_by_date", "segment_by_date") and report["scope"][key] != ctx["scope"].get(key):
                raise IntegrityError("fold aggregation scope mismatch: " + key)
        for row in report["daily"]:
            day = row["date"]
            if day in seen or day not in ctx["scope"]["dates"]:
                raise IntegrityError("duplicate or undeclared aggregate date")
            if any(row[field] != ctx["scope"][mapping].get(day) for field, mapping in (("fold_id", "fold_by_date"), ("segment_id", "segment_by_date"))):
                raise IntegrityError("aggregate fold or regime mapping differs")
            seen.add(day)
            daily[day] = {**row, "complete": row["availability"] == "AVAILABLE"}
            for name in policies:
                policies[name][str(row["fold_id"])] = report["policy"][name]
        complete = complete and report["metric_eligibility"]["complete_reference_economics"]
        statuses.append(report["execution"]["status"])
    ctx.update(record_kind="FOLD_AGGREGATE", attempt_id=None)
    ctx["provenance"]["source_reports"] = [{"report_id": r["report_id"], "execution": r["execution"], "coverage": r["coverage"]} for r in reports]
    for name, by_fold in policies.items():
        ctx["policy"][name] = {"declared_fold_policies": by_fold}
    status = "FAILED" if "FAILED" in statuses else "COMPLETE" if complete and seen == set(ctx["scope"]["dates"]) else "INCOMPLETE"
    result = candidate_report(ctx, {"daily": daily, "complete": complete}, execution_status=status,
                              failure_reason=None if complete and seen == set(ctx["scope"]["dates"]) else "DECLARED_FOLD_RESULTS_INCOMPLETE;SOURCE_REPORTS_RETAIN_REASONS")
    if result["metric_eligibility"]["complete_reference_economics"]:
        target = result["trade_diagnostics"]
        for name in target:
            parts = [r["trade_diagnostics"][name] for r in reports]
            if any(p.get("availability") == "UNAVAILABLE" for p in parts):
                continue
            if name == "itemized_costs":
                target[name] = {**parts[0], **{k: math.fsum(p[k] for p in parts) for k in ("fees_usd", "price_allowance_usd", "total_usd")}}
            elif name == "exposure":
                target[name] = {**parts[0], "denominator_evaluation_dates": len(ctx["scope"]["dates"]),
                                **{k: math.fsum(p[k] for p in parts) for k in ("long_contract_minutes", "short_contract_minutes")}}
            elif name == "turnover":
                target[name] = measured(sum(p["value"] for p in parts), parts[0]["unit"])
            elif name == "exceptional_trades":
                target[name] = {k: sorted([v for p in parts for v in p[k]], reverse=k == "largest_winners_usd")[:5]
                                for k in ("largest_winners_usd", "largest_losers_usd")}
            elif name == "fixed_trade_cost_sensitivity" and all(p["scenarios"].keys() == parts[0]["scenarios"].keys() for p in parts):
                count = result["metrics"]["trade_count"]["value"]
                target[name] = {"basis": parts[0]["basis"],
                    "scenarios": {s: {"net_usd": math.fsum(p["scenarios"][s]["net_usd"] for p in parts), "trade_count": count} for s in parts[0]["scenarios"]},
                    "remaining_cost_headroom_usd_per_round_trip": measured(result["metrics"]["net_usd"]["value"] / count, "USD/round trip") if count else unavailable("ZERO_TRADES")}
    result["report_id"] = sha256_json({k: v for k, v in result.items() if k != "report_id"})
    return result


def _policy_changes(a, b, prefix=""):
    result = []
    for key in sorted(set(a) | set(b)):
        name = prefix + key
        if isinstance(a.get(key), dict) and isinstance(b.get(key), dict):
            result.extend(_policy_changes(a[key], b[key], name + "."))
        elif a.get(key) != b.get(key):
            result.append(name)
    return result


def _paired_statistics(rows):
    if not rows:
        return {k: None for k in ("incumbent_mean_daily_usd", "candidate_mean_daily_usd", "mean_daily_difference_usd", "cumulative_difference_usd")} | {"observation_count": 0}
    return {"incumbent_mean_daily_usd": math.fsum(r["incumbent_net_usd"] for r in rows) / len(rows),
            "candidate_mean_daily_usd": math.fsum(r["candidate_net_usd"] for r in rows) / len(rows),
            "mean_daily_difference_usd": math.fsum(r["difference_usd"] for r in rows) / len(rows),
            "cumulative_difference_usd": math.fsum(r["difference_usd"] for r in rows), "observation_count": len(rows)}


def compare_candidates(incumbent, candidate, *, role="incumbent", inference=None):
    """Shared report/dashboard comparison; no inner joins, reselection or gates."""
    for report in (incumbent, candidate):
        validate_report(report)
    if role not in ("incumbent", "benchmark"):
        raise IntegrityError("comparison role must name incumbent or declared benchmark")
    a, b = incumbent["scope"], candidate["scope"]
    mismatch = [k for k in SCOPE_FIELDS if a[k] != b[k] or not _known(a[k]) or not _known(b[k])]
    reasons = ["INCOMPATIBLE_OR_UNKNOWN:" + k for k in mismatch]
    for name, report in (("INCUMBENT", incumbent), ("CANDIDATE", candidate)):
        reasons += [name + ":" + r for r in report["comparison_eligibility"]["reasons"]]
    if role == "benchmark" and (incumbent["identity"]["benchmark_id"] not in (candidate.get("declared_benchmarks") or [])):
        reasons.append("BENCHMARK_NOT_DECLARED_BY_CANDIDATE")
    amap, bmap = ({r["date"]: r for r in report["daily"]} for report in (incumbent, candidate))
    dates = sorted(set(a["dates"] or []) | set(b["dates"] or []))
    rows, missing = [], []
    for day in dates:
        left, right = amap.get(day), bmap.get(day)
        unavailable_sides = [name + ":" + (row["reason"] or "UNAVAILABLE") if row else name + ":MISSING_DECLARED_DATE"
                             for name, row in (("incumbent", left), ("candidate", right)) if row is None or row["availability"] != "AVAILABLE"]
        if unavailable_sides:
            missing.append({"date": day, "reasons": unavailable_sides})
        else:
            rows.append({"date": day, "fold_id": right["fold_id"], "segment_id": right["segment_id"],
                         "incumbent_net_usd": left["net_usd"], "candidate_net_usd": right["net_usd"],
                         "difference_usd": right["net_usd"] - left["net_usd"]})
    if missing:
        reasons.append("MISSING_OR_UNRESOLVED_PAIRED_DATES")
    changes = _policy_changes(incumbent["policy"], candidate["policy"])
    intended = candidate.get("intended_policy_changes") or []
    confounds = [c for c in changes if not any(c == field or c.startswith(field + ".") for field in intended)]
    eligible = not reasons
    stats = _paired_statistics(rows) if eligible else _paired_statistics([])
    result = {"incumbent_report_id": incumbent["report_id"], "candidate_report_id": candidate["report_id"],
              "role": role, "status": "DESCRIPTIVE_COMPLETE" if eligible else "INELIGIBLE",
              "reasons": sorted(set(reasons)), "confirmatory_eligible": False, "winner_selection_eligible": False,
              "attribution": "MODEL_ONLY" if role == "incumbent" and changes and all(c == "model" or c.startswith("model.") for c in changes) and not confounds else "FULL_POLICY",
              "changed_policy_fields": changes, "undeclared_or_confounding_changes": confounds,
              "coverage": {"expected_dates": len(dates), "matched_dates": len(rows), "missing_dates": missing,
                           "excluded_dates": {"incumbent": incumbent["coverage"]["excluded_dates"], "candidate": candidate["coverage"]["excluded_dates"]}},
              "statistics": stats, "daily_differences": rows if not mismatch else [],
              "partial_diagnostic": {"label": "MATCHED_AVAILABLE_DATES_ONLY_NOT_FULL_RUN_OR_WINNER_EVIDENCE", **_paired_statistics(rows)} if not mismatch and not eligible else None,
              "fold_results": [{"fold_id": f, **_paired_statistics([r for r in rows if r["fold_id"] == f])} for f in dict.fromkeys(r["fold_id"] for r in rows)] if eligible else [],
              "claims": {"profitable_under_stated_assumptions_on_this_sample": candidate["metrics"]["net_usd"]["value"] > 0 if candidate["metric_eligibility"]["complete_reference_economics"] else None,
                         "better_than_incumbent_on_this_sample": stats["mean_daily_difference_usd"] > 0 if eligible and role == "incumbent" else None,
                         "above_declared_benchmark_on_this_sample": stats["mean_daily_difference_usd"] > 0 if eligible and role == "benchmark" else None,
                         "independently_supported_improvement": unavailable("NO_VALIDATED_L0_PAIRED_DECISION_RULE_OR_INDEPENDENT_CONFIRMATION")},
              "uncertainty": _paired_uncertainty(rows, incumbent, candidate, inference) if eligible else unavailable("INCOMPLETE_OR_INCOMPATIBLE_COMPARISON"),
              "research_gate_passed": False}
    return result


def _paired_uncertainty(rows, incumbent, candidate, declaration):
    """Declared percentile intervals; shared stationary blocks within each segment.

    Segments are contiguous declared calendar positions AND fold/regime identities.
    Circular wrapping is only within a segment, never across a missing observation.
    This is descriptive conditional uncertainty, not selection-adjusted inference.
    """
    if not declaration:
        return unavailable("PAIRED_INFERENCE_NOT_PREREGISTERED;NO_DEFAULT_CONFIDENCE_OR_DECISION_THRESHOLD")
    required = ("declaration_id", "declared_at_utc", "estimand", "eligible_stages", "minimum_dates",
                "minimum_segment_dates", "mean_block_length", "sensitivity_block_lengths", "replicates", "seed",
                "confidence_level", "interval_method", "block_method", "gap_rule", "stationarity_assumption")
    if any(declaration.get(k) is None for k in required):
        return unavailable("INFERENCE_DECLARATION_INCOMPLETE")
    cfg = declaration
    at = _time(cfg["declared_at_utc"])
    exposures = [_time(r["lineage"].get("first_result_exposure_utc")) for r in (incumbent, candidate)]
    if at is None or any(e is None or at >= e for e in exposures):
        return unavailable("INFERENCE_PREDECLARATION_BEFORE_EXPOSURE_NOT_ESTABLISHED")
    if not isinstance(cfg["eligible_stages"], list) or any(r["scope"]["evaluation_stage"] not in cfg["eligible_stages"] for r in (incumbent, candidate)):
        return unavailable("INFERENCE_STAGE_NOT_DECLARED")
    if (cfg["estimand"] != "MEAN_DAILY_NET_DIFFERENCE_USD" or cfg["interval_method"] != "PERCENTILE"
            or cfg["block_method"] != "STATIONARY_WITHIN_FOLD_REGIME" or cfg["gap_rule"] != "NO_MISSING_DATES_NO_BOUNDARY_CROSSING"
            or not isinstance(cfg["stationarity_assumption"], str) or not cfg["stationarity_assumption"]):
        return unavailable("UNSUPPORTED_INFERENCE_METHOD_OR_ASSUMPTIONS")
    if (any(type(cfg[k]) is not int or cfg[k] < 2 for k in ("minimum_dates", "minimum_segment_dates", "replicates"))
            or type(cfg["seed"]) is not int or not 0 <= cfg["seed"] < 2**64
            or not _number(cfg["confidence_level"]) or not 0 < cfg["confidence_level"] < 1
            or not isinstance(cfg["sensitivity_block_lengths"], list)):
        return unavailable("INVALID_INFERENCE_SETTINGS")
    segments = []
    for i, row in enumerate(rows):
        if row["fold_id"] is None or row["segment_id"] is None:
            return unavailable("FOLD_OR_REGIME_SEGMENT_UNKNOWN")
        key = (row["fold_id"], row["segment_id"])
        if not segments or segments[-1][0] != key:
            segments.append((key, []))
        segments[-1][1].append(i)
    blocks = list(dict.fromkeys([cfg["mean_block_length"], *cfg["sensitivity_block_lengths"]]))
    if len(rows) < cfg["minimum_dates"] or any(len(s) < cfg["minimum_segment_dates"] for _, s in segments):
        return unavailable("DECLARED_MINIMUM_DATE_OR_SEGMENT_SUPPORT_NOT_MET")
    if any(not _number(block) or block <= 1 or block > min(len(s) for _, s in segments) for block in blocks):
        return unavailable("BLOCK_LENGTH_UNSUPPORTED;IID_TRADE_SHUFFLING_NOT_ALLOWED")
    # Bound display work, not the statistical rule. Larger declarations need a task-owned offline implementation.
    if cfg["replicates"] * len(rows) * len(blocks) > 5_000_000:
        return unavailable("DECLARED_RESAMPLING_EXCEEDS_BOUNDED_REPORT_WORK_LIMIT")
    matrix = np.array([[r["incumbent_net_usd"], r["candidate_net_usd"]] for r in rows], dtype=float)
    if not np.isfinite(matrix).all() or np.ptp(matrix[:, 1] - matrix[:, 0]) == 0:
        return unavailable("NONFINITE_OR_ZERO_VARIANCE_PAIRED_DIFFERENCE")
    alpha = (1 - cfg["confidence_level"]) / 2
    sensitivity = []
    horizon = cfg.get("drawdown_horizon_dates")
    for block in blocks:
        means, depths = [], []
        generators = [stationary_bootstrap_index_rows(n_observations=len(s), n_resamples=cfg["replicates"],
                      mean_block_length=block, seed=(cfg["seed"] + i) % 2**64) for i, (_, s) in enumerate(segments)]
        for index_rows in zip(*generators, strict=True):
            indices = np.concatenate([np.asarray(s)[index] for (_, s), index in zip(segments, index_rows, strict=True)])
            sample = matrix[indices]  # Both strategies always use the same dates.
            means.append(float(np.mean(sample[:, 1] - sample[:, 0])))
            if horizon == len(rows):
                curve = np.vstack([np.zeros(2), np.cumsum(sample, axis=0)])
                depths.append(np.max(np.maximum.accumulate(curve, axis=0) - curve, axis=0))
        scenario = unavailable("DRAWDOWN_HORIZON_MUST_EXPLICITLY_EQUAL_DECLARED_CALENDAR_LENGTH")
        if depths:
            scenario = {"horizon_evaluation_dates": horizon, "quantile_levels": [alpha, .5, 1 - alpha],
                        "incumbent_candidate_depth_usd": np.quantile(depths, [alpha, .5, 1 - alpha], axis=0).tolist(),
                        "claim": "CONDITIONAL_REFERENCE_PNL_SCENARIOS;NOT_LOSS_BOUNDS;NO_UNOBSERVED_CRISIS_MODEL"}
        sensitivity.append({"mean_block_length": block, "interval_usd_per_date": np.quantile(means, [alpha, 1 - alpha]).tolist(),
                            "drawdown_scenarios": scenario})
    return {"availability": "AVAILABLE_DESCRIPTIVE_ONLY", "declaration": deepcopy(cfg), "sensitivity": sensitivity,
            "segment_lengths": [len(s) for _, s in segments], "joint_alignment": "IDENTICAL_INDICES_FOR_ALL_STRATEGY_COLUMNS",
            "limitations": "Conditional within-segment stationarity; fixed fold weights; percentile interval is not adaptive-selection correction, calibrated qualification, or a promotion rule."}


def joint_loss_diagnostics(reports, portfolio_metadata=None):
    """Aligned vectors only; no implicit portfolio weights or optimizer."""
    if len(reports) < 2:
        return unavailable("AT_LEAST_TWO_COMPLETE_STRATEGIES_REQUIRED")
    for r in reports:
        validate_report(r)
    same_fields = ("dates", "calendar_id", "session_boundaries", "timezone", "pnl_basis", "evaluation_stage", "data_kind", "observation_cutoff_utc")
    if any(not r["metric_eligibility"]["complete_reference_economics"] or any(
            not _known(r["scope"][k]) or r["scope"][k] != reports[0]["scope"][k] for k in same_fields) for r in reports):
        return unavailable("JOINT_CALENDAR_STAGE_BASIS_OR_COVERAGE_INCOMPATIBLE")
    matrix = np.array([[row["net_usd"] for row in r["daily"]] for r in reports]).T
    both = (matrix < 0).all(axis=1)
    return {"report_ids": [r["report_id"] for r in reports], "aligned_dates": reports[0]["scope"]["dates"],
            "joint_loss_date_count": int(both.sum()), "denominator_dates": len(matrix),
            "joint_loss_vectors_usd": [{"date": day, "values": values.tolist()} for day, values, loss in zip(reports[0]["scope"]["dates"], matrix, both, strict=True) if loss],
            "correlation": measured(np.corrcoef(matrix, rowvar=False).tolist(), "Pearson daily reference-P&L correlation") if np.all(np.ptp(matrix, axis=0) > 0) else unavailable("ZERO_VARIANCE_STRATEGY"),
            "portfolio_metadata": deepcopy(portfolio_metadata),
            "portfolio_performance": unavailable("COMBINATIONS_AND_WEIGHTS_REQUIRE_SEPARATE_GOVERNED_EVALUATION;NO_IMPLICIT_WEIGHTS")}


def experiment_history(reports):
    attempts = {r["identity"]["attempt_id"] for r in reports if r["record_kind"] == "ATTEMPT" and r["identity"]["attempt_id"] is not None and r["identity"]["benchmark_id"] is None}
    return {"observed_attempt_count": len(attempts), "count_basis": "DISTINCT_RECORDED_ATTEMPT_IDS;NOT_CONFIGURATION_CHARGES_OR_INDEPENDENT_TRIALS",
            "unknown_attempt_identity_records": sum(r["record_kind"] == "ATTEMPT" and r["identity"]["attempt_id"] is None for r in reports),
            "historical_search": "UNKNOWN_UNLESS_SEPARATELY_EVIDENCED;SUPPLIED_REPORTS_ARE_NOT_A_COMPLETE_CENSUS",
            "effective_independent_trials": None, "selection_bias_removed": False,
            "entries": [{"report_id": r["report_id"], **r["identity"], "execution": r["execution"],
                         "lineage": r["lineage"], "information_use": r["information_use"]} for r in reports]}


def report_view(reports, comparisons=()):
    """One calculation path for JSON, Markdown and cockpit rendering."""
    by_id = {}
    for r in reports:
        validate_report(r)
        if r["report_id"] in by_id:
            raise IntegrityError("duplicate report identity")
        by_id[r["report_id"]] = r
    paired = []
    for request in comparisons:
        a, b = (by_id.get(request.get(k)) for k in ("incumbent_report_id", "candidate_report_id"))
        if a is None or b is None:
            paired.append({"status": "UNAVAILABLE", "reason": "DECLARED_INCUMBENT_OR_BENCHMARK_REPORT_MISSING", "request": request})
        else:
            paired.append(compare_candidates(a, b, role=request.get("role", "incumbent"), inference=request.get("inference")))
    return {"schema_version": "l0_alpha_comparison_view/2", "reports": deepcopy(reports),
            "history": experiment_history(reports), "comparisons": paired,
            "stage_definitions": STAGES, "alpha_established": False, "research_gate_passed": False}


def markdown_view(view, *, detailed=True):
    def value(metric):
        v = metric["value"]
        return f"{v:,.6f}" if isinstance(v, float) else str(v) if v is not None else "Unavailable: " + metric["reason"]
    def escape(v):
        return str(v).replace("|", "\\|").replace("\n", " ")
    lines = ["# L0 candidate reports", "", "Descriptive reference P&L. Software output is not alpha or a research-gate pass.", "",
             "| Candidate | Stage | Execution | Available / expected dates | Gross USD | Costs USD | Net USD | Mean USD/date |",
             "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    for r in view["reports"]:
        columns = [r["identity"]["candidate_id"], r["scope"]["evaluation_stage"], r["execution"]["status"] + (" / VALID NO TRADE" if r["execution"]["valid_no_trade"] else "") + (" / ACCOUNTING INCOMPLETE" if r["execution"]["status"] == "COMPLETE" and not r["metric_eligibility"]["complete_reference_economics"] else ""),
                   f"{r['coverage']['available_dates']} / {r['coverage']['expected_dates']}",
                   *[value(r["metrics"][k]) for k in ("gross_usd", "cost_usd", "net_usd", "mean_daily_net_usd")]]
        lines.append("| " + " | ".join(escape(c) for c in columns) + " |")
    lines.extend(["", f"Observed attempts: {view['history']['observed_attempt_count']}. Historical search and effective independent trial count remain unknown.", ""])
    # Typed fields retain every limitation without a second set of formulas.
    import json
    if detailed:
        lines.extend(["## Definitions, lineage and diagnostics", "", "```json", json.dumps(view, indent=2, sort_keys=True, allow_nan=False), "```", ""])
    else:
        lines.extend(["Individual reports retain definitions, lineage, availability and diagnostics:", ""])
        lines.extend(f"- [{escape(r['identity']['candidate_id'])}: {r['report_id'][:12]}]({r['report_id']}.md)" for r in view["reports"])
        lines.extend(["", "## Paired comparisons", "", "```json", json.dumps(view["comparisons"], indent=2, sort_keys=True, allow_nan=False), "```", ""])
    return "\n".join(lines)


def write_catalog(directory, reports, comparisons=()):
    """Create-only derived reports in the existing run's candidate_reports folder."""
    from pathlib import Path
    from .canonical import write_new_canonical_authority, sha256_file, assert_no_linklike_ancestors
    directory = Path(directory)
    assert_no_linklike_ancestors(directory)
    directory.mkdir(parents=True, exist_ok=True)
    entries = []
    for report in reports:
        validate_report(report)
        path = directory / (report["report_id"] + ".json")
        write_new_canonical_authority(path, report)
        human_report = path.with_suffix(".md")
        rendered_report = markdown_view(report_view([report])).encode("utf-8")
        if human_report.exists():
            if human_report.read_bytes() != rendered_report:
                raise IntegrityError("individual human report already exists with different bytes")
        else:
            with human_report.open("xb") as stream:
                stream.write(rendered_report)
        entries.append({"report_id": report["report_id"], "scope": report["scope"],
                        "payload_kind": "OBSERVATIONS" if report["daily"] else "METADATA_ONLY",
                        "filename": path.name, "sha256": sha256_file(path)})
    manifest = {"schema_version": "l0_alpha_report_catalog/2", "entries": entries,
                "comparisons": list(comparisons), "authority": "DERIVED_REPORTS_ONLY_NO_RESEARCH_AUTHORITY"}
    manifest["catalog_id"] = sha256_json(manifest)
    path = directory / ("catalog-" + manifest["catalog_id"] + ".json")
    write_new_canonical_authority(path, manifest)
    rendered = markdown_view(report_view(reports, comparisons), detailed=False).encode("utf-8")
    human = path.with_suffix(".md")
    if human.exists():
        if human.read_bytes() != rendered:
            raise IntegrityError("derived human report already exists with different bytes")
    else:
        with human.open("xb") as stream:
            stream.write(rendered)
    return path


def load_catalog(path):
    """Read only an explicit catalog and its same-directory report IDs.

    No glob, source resolver, arbitrary payload path, automatic selection, or
    research runner. ALL manifest scopes are checked before ANY report is read.
    """
    import re
    from pathlib import Path
    from .canonical import read_canonical_authority, assert_no_linklike_ancestors, assert_plain_file
    path = Path(path)
    if not re.fullmatch(r"catalog-[0-9a-f]{64}\.json", path.name):
        raise IntegrityError("explicit content-addressed L0 report catalog required")
    assert_no_linklike_ancestors(path)
    if assert_plain_file(path).st_size > 8 * 2**20:
        raise IntegrityError("report catalog exceeds display bound")
    manifest = read_canonical_authority(path, identity_field="catalog_id", expected_schema="l0_alpha_report_catalog/2")
    if path.name != "catalog-" + manifest["catalog_id"] + ".json":
        raise IntegrityError("catalog filename and identity disagree")
    for entry in manifest["entries"]:
        if entry.get("payload_kind", "OBSERVATIONS") not in ("OBSERVATIONS", "METADATA_ONLY"):
            raise IntegrityError("unknown report payload kind")
        check_scope(entry["scope"], observations=entry.get("payload_kind") != "METADATA_ONLY")
        if not re.fullmatch(r"[0-9a-f]{64}", entry["report_id"]) or entry["filename"] != entry["report_id"] + ".json":
            raise IntegrityError("catalog can reference only same-directory report IDs")
    reports = []
    for entry in manifest["entries"]:
        report_path = path.parent / entry["filename"]
        assert_no_linklike_ancestors(report_path)
        if assert_plain_file(report_path).st_size > 32 * 2**20:
            raise IntegrityError("candidate report exceeds display bound")
        report = read_canonical_authority(report_path, identity_field="report_id", expected_schema=SCHEMA,
                                          expected_sha256=entry["sha256"], expected_identity=entry["report_id"])
        if report["scope"] != entry["scope"]:
            raise IntegrityError("catalog and report scope disagree")
        if entry.get("payload_kind") == "METADATA_ONLY" and report["daily"]:
            raise IntegrityError("metadata-only report contains daily observations")
        reports.append(report)
    return report_view(reports, manifest["comparisons"])
