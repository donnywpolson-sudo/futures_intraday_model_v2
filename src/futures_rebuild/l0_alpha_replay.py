"""Chronological reference-price policy replay, with unresolved positions retained."""
import math

import numpy as np

from . import l0_alpha_candidate as c
from .errors import IntegrityError


def cost_nano_usd(record, scenario):
    if scenario not in c.SCENARIOS or record["market"] not in c.MARKETS:
        raise IntegrityError("undeclared cost scenario or market")
    units = record.get("tick_value_nano_usd")
    if type(units) is not int or units <= 0:
        raise IntegrityError("positive exact nano USD tick value required")
    return c.FEE_USD * 10**9 + c.ALLOWANCES[record["market"]][scenario] * units


def replay(decisions, outcomes, predictions, dates, *, scenario="adverse", policy="model",
           calendar=None, calendar_view=None, timing_only=False, legacy=False):
    """Entry/exit evidence is accessed only after a signal reserves the market.

    Completed exits are processed before the next decision. Missing entry or
    exit references stop the replay without inventing a fill or clearing risk.
    """
    if policy not in ("model", "no_trade", "date_balanced_training_mean", "always_long", "always_short"):
        raise IntegrityError("undeclared policy")
    from .l0_alpha_calendar import compile_calendar, entry_permission
    mean_policy = policy == "date_balanced_training_mean"
    if timing_only:
        if mean_policy:
            if predictions is None:
                raise IntegrityError("training-mean comparison requires bound training-label forecasts")
        elif policy not in ("no_trade", "always_long", "always_short") or predictions is not None:
            raise IntegrityError("timing-only replay requires a declared benchmark")
    if calendar is None and not legacy:
        raise IntegrityError("explicit source-bound calendar required")
    context = compile_calendar(calendar, calendar_view) if calendar is not None else None
    if timing_only and not mean_policy:
        predictions = [None] * len(decisions)
    dates = c.old.profit.ordered_dates(dates)
    if len(outcomes) != len(decisions) or len(predictions) != len(decisions) or not dates:
        raise IntegrityError("aligned complete replay inputs required")
    if len(decisions) != 330 * len(dates):
        raise IntegrityError("replay requires every scheduled decision, including abstentions")
    market = decisions[0]["market"]
    for i, (record, prediction) in enumerate(zip(decisions, predictions, strict=True)):
        if (record["market"] != market or record["trade_date"] != dates[i // 330]
                or record["decision_ns"] != c.decision_times(dates[i // 330])[i % 330]):
            raise IntegrityError("replay clock or market was changed")
        if (not timing_only or mean_policy) and record["status"] == "ELIGIBLE" and (prediction is None or not math.isfinite(prediction)):
            raise IntegrityError("every eligible decision needs a finite saved forecast")
    if timing_only and mean_policy:
        means = {value for row, value in zip(decisions, predictions, strict=True) if row["status"] == "ELIGIBLE"}
        if len(means) > 1:
            raise IntegrityError("training mean must remain fixed within the audit window")
    trades, events, ledger = [], [], []
    position, unresolved = None, None
    daily = {day: {"gross_nano_usd": 0, "cost_nano_usd": 0, "net_nano_usd": 0, "trades": 0} for day in dates}

    def advance(at):
        nonlocal position, unresolved
        if position is None or unresolved is not None:
            return
        row, outcome = position["record"], position["outcome"]
        for side in ("entry", "exit"):
            state = "PENDING_ENTRY" if side == "entry" else "OPEN"
            if position["state"] != state or row[side + "_ns"] > at:
                continue
            if outcome.get(side + "_status") != "RESOLVED":
                unresolved = {"forecast_id": row["forecast_id"], "trade_date": row["trade_date"],
                    "at_ns": row[side + "_ns"], "side": side, "direction": position["direction"],
                    "prior_state": position["state"], "actual_contract": row["actual_contract"],
                    "reason": outcome.get(side + "_status", "MISSING_REFERENCE")}
                position["state"] = "UNRESOLVED"
                events.append({**unresolved, "state": "UNRESOLVED"})
                return
            price = outcome.get(side + "_open_nano")
            if type(price) is not int or price % row["tick_nano"]:
                raise IntegrityError("invalid scheduled reference price")
            if side == "entry":
                position.update(state="OPEN", entry_open_nano=price)
                events.append({"forecast_id": row["forecast_id"], "at_ns": row["entry_ns"], "state": "OPEN"})
            else:
                events.append({"forecast_id": row["forecast_id"], "at_ns": row["exit_ns"], "state": "SCHEDULED_EXIT"})
                if timing_only:
                    trades.append({"forecast_id": row["forecast_id"], "entry_ns": row["entry_ns"], "exit_ns": row["exit_ns"]})
                    events.append({"forecast_id": row["forecast_id"], "at_ns": row["exit_ns"], "state": "FLAT"})
                    position = None
                    continue
                gross = position["direction"] * (price - position["entry_open_nano"]) // row["tick_nano"] * row["tick_value_nano_usd"]
                cost = cost_nano_usd(row, scenario)
                trade = {**{k: row[k] for k in ("forecast_id", "market", "trade_date", "actual_contract", "entry_ns", "exit_ns",
                                               "tick_nano", "tick_value_nano_usd")},
                    "direction": position["direction"], "entry_open_nano": position["entry_open_nano"], "exit_open_nano": price,
                    "gross_nano_usd": gross, "cost_nano_usd": cost, "net_nano_usd": gross - cost,
                    "price_interpretation": "HISTORICAL_OPENING_PRINT_REFERENCE_NOT_FILL"}
                trades.append(trade)
                day = daily[row["trade_date"]]
                for key in ("gross_nano_usd", "cost_nano_usd", "net_nano_usd"):
                    day[key] += trade[key]
                day["trades"] += 1
                events.append({"forecast_id": row["forecast_id"], "at_ns": row["exit_ns"], "state": "FLAT"})
                position = None

    for row, outcome, prediction in zip(decisions, outcomes, predictions, strict=True):
        advance(row["decision_ns"])
        direction = 0
        permission = None
        if unresolved:
            status = "STOPPED_UNRESOLVED_EXECUTION"
        elif position:
            status = "OCCUPIED_FORECAST_ONLY"
        elif row["status"] != "ELIGIBLE":
            status = row["status"]
        else:
            if policy == "no_trade":
                direction = 0
            elif policy in ("always_long", "always_short"):
                direction = 1 if policy == "always_long" else -1
            else:
                threshold = cost_nano_usd(row, scenario) / row["tick_value_nano_usd"]
                direction = int(prediction > threshold) - int(prediction < -threshold)
            status = "PENDING_ENTRY" if direction else "ABSTAIN_THRESHOLD_OR_NO_TRADE"
            if context is not None:
                permission = entry_permission(row, context)
                if direction and permission["state"] != "ALLOWED":
                    direction, status = 0, "CALENDAR_" + permission["state"]
            if direction:
                position = {"record": row, "outcome": outcome, "state": "PENDING_ENTRY", "direction": direction}
                events.append({"forecast_id": row["forecast_id"], "at_ns": row["decision_ns"], "state": "PENDING_ENTRY"})
        ledger.append({"forecast_id": row["forecast_id"], "status": status, "scheduled_direction": direction,
                       "calendar": permission, **({} if timing_only else {"prediction_ticks": prediction})})
    advance(decisions[-1]["exit_ns"])
    complete = unresolved is None
    if timing_only:
        return c.bind({"mode": "TIMING_ONLY_NO_PROFITABILITY", "scenario": scenario, "policy": policy, "complete": complete,
            "calendar_id": context["calendar_id"] if context else None, "calendar_view": calendar_view,
            "decisions": ledger, "events": events, "resolved_round_trips": len(trades),
            "attempted_entries": sum(e["state"] == "PENDING_ENTRY" for e in events),
            "unresolved_position": unresolved, "claims": "BENCHMARK_REFERENCE_COVERAGE_ONLY_NOT_MODEL_OR_FILL_COMPLETENESS"}, "replay_id")
    prefix = {key: sum(d[key] for d in daily.values()) / 1e9 for key in ("gross_nano_usd", "cost_nano_usd", "net_nano_usd")}
    daily_usd = {day: {"gross_usd": d["gross_nano_usd"] / 1e9, "cost_usd": d["cost_nano_usd"] / 1e9,
                       "net_usd": d["net_nano_usd"] / 1e9, "trades": d["trades"],
                       "complete": complete or day < unresolved["trade_date"]} for day, d in daily.items()}
    curve = np.r_[0., np.cumsum([d["net_usd"] for d in daily_usd.values()])]
    values = sorted(((d["net_usd"], day) for day, d in daily_usd.items()), reverse=True)
    total_net = prefix["net_nano_usd"]
    positive = sum(max(d["net_usd"], 0.) for d in daily_usd.values())
    result = {"scenario": scenario, "policy": policy, "complete": complete, "decisions": ledger,
        "events": events, "trades": trades, "unresolved_position": unresolved, "daily": daily_usd,
        "mean_daily_net_usd": total_net / len(dates) if complete else None,
        "gross_usd": prefix["gross_nano_usd"] if complete else None,
        "cost_usd": prefix["cost_nano_usd"] if complete else None, "net_usd": total_net if complete else None,
        "realized_prefix_only": prefix if not complete else None,
        "trade_count": len(trades), "trading_dates": sum(d["trades"] > 0 for d in daily.values()),
        "zero_trade_dates": sum(d["trades"] == 0 for d in daily.values()) if complete else None,
        "drawdown_usd": float(np.max(np.maximum.accumulate(curve) - curve)) if complete else None,
        "drawdown_basis": "CLOSED_TRADE_DAILY_REFERENCE_PNL;NOT_INTRABAR_OR_ACCOUNT_EQUITY",
        "long_minutes": sum(t["direction"] == 1 for t in trades) * 60,
        "short_minutes": sum(t["direction"] == -1 for t in trades) * 60,
        "largest_winning_days": [{"date": day, "net_usd": value} for value, day in values[:5] if value > 0] if complete else None,
        "largest_losing_days": [{"date": day, "net_usd": value} for value, day in reversed(values[-5:]) if value < 0] if complete else None,
        "top_five_share_of_positive_day_profits": sum(max(v, 0) for v, _ in values[:5]) / positive if complete and positive else None,
        "top_five_share_of_total_net_profit": sum(max(v, 0) for v, _ in values[:5]) / total_net if complete and total_net > 0 else None,
        "fixed_trade_cost_headroom_usd": (prefix["gross_nano_usd"] / len(trades) - c.FEE_USD) if complete and trades else None,
        "remaining_adverse_headroom_usd": total_net / len(trades) if complete and trades else None,
        "claims": "DEVELOPMENT_REFERENCE_ECONOMICS_ONLY"}
    return c.bind(result, "replay_id")


def fixed_trade_sensitivity(selection_replay):
    c.identity(selection_replay, "replay_id")
    if not selection_replay["complete"]:
        return {"status": "INCOMPLETE", "scenarios": None}
    trades = selection_replay["trades"]
    return {"status": "FIXED_SELECTION_TRADE_LIST_REPRICED_NOT_POLICY_REPLAY", "scenarios": {
        scenario: {"net_usd": sum(t["gross_nano_usd"] - cost_nano_usd(t, scenario) for t in trades) / 1e9,
                   "trade_count": len(trades)} for scenario in c.SCENARIOS}}
