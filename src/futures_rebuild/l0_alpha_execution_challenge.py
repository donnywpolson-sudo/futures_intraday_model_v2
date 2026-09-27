"""Frozen L1 reference-price diagnostics; no quote reader or confirmation test."""
from collections import Counter

from . import l0_alpha_candidate as c
from .canonical import sha256_json
from .errors import IntegrityError

SECOND = 10**9


def freeze_challenge(final, trades, *, dates, source_release_id, schema,
                     forecast_ledger_sha256, frozen_at_ns):
    c.identity(final, "final_id")
    if final["mechanism_id"] != c.mechanism_id() or schema not in ("BBO-1s", "TBBO"):
        raise IntegrityError("matching frozen strategy and declared L1 schema required")
    dates = c.old.profit.ordered_dates(dates)
    if not dates:
        raise IntegrityError("exact permitted dates required, including zero-trade dates")
    if final["policy"] == "no_trade" and trades:
        raise IntegrityError("frozen no-trade policy cannot supply execution trades")
    c.old.profit.digest(source_release_id)
    c.old.profit.digest(forecast_ledger_sha256)
    c.old.profit.integer(frozen_at_ns, minimum=1)
    previous = -1
    market = final["market"]
    ids = set()
    for trade in trades:
        if (trade["market"] not in c.MARKETS or market is not None and trade["market"] != market
                or trade["trade_date"] not in dates or trade["entry_ns"] < previous
                or trade["exit_ns"] != trade["entry_ns"] + 60 * c.MINUTE
                or trade["direction"] not in (-1, 1) or trade["forecast_id"] in ids
                or trade["entry_ns"] <= final["latest_label_ns"]):
            raise IntegrityError("challenge trades differ from bounded frozen strategy scope")
        market, previous = trade["market"], trade["exit_ns"]
        ids.add(trade["forecast_id"])
    return c.bind({"schema_version": "l0_execution_challenge/1", "mechanism_id": c.mechanism_id(),
        "final_id": final["final_id"], "forecast_ledger_sha256": forecast_ledger_sha256,
        "trades_sha256": sha256_json(trades), "dates": list(dates), "market": market,
        "source_release_id": source_release_id, "schema": schema, "frozen_at_ns": frozen_at_ns,
        "offset_seconds": [0, 1, 5], "maximum_observed_quote_age_ns": 2 * SECOND,
        "additional_round_trip_slippage_ticks": [0, 1, 5], "round_trip_fee_usd": c.FEE_USD,
        "matching": "EXACT_TIMESTAMP_SAME_ACTUAL_CONTRACT;NO_NEAREST_RECORD_SEARCH",
        "interpretation": "INTERVAL_END_REFERENCE" if schema == "BBO-1s" else "TRADE_CONDITIONED_REFERENCE_ONLY",
        "freshness": "FORWARD_FILLED_FIELDS_POSSIBLE;UNKNOWN_WITHOUT_OBSERVED_LAST_UPDATE",
        "delivery": "INTERVAL_BOUNDARY_IS_NOT_APPLICATION_RECEIPT_OR_A_FILL",
        "protected_data": "INFORMATION_CONSUMED_EVEN_IF_EXECUTION_ONLY;SEPARATE_ACCESS_REQUIRED",
        "confirmation": c.design()["confirmation"], "authority": c.old.AUTHORITY}, "challenge_id")


def match_quote(quote, trade, at, plan):
    if quote is None:
        return "MISSING_QUOTE"
    time_key = "interval_end_ns" if plan["schema"] == "BBO-1s" else "ts_recv"
    if (quote.get(time_key) != at or quote.get("ts_recv") != at
            or quote.get("market") != trade["market"] or quote.get("actual_contract") != trade["actual_contract"]
            or quote.get("source_release_id") != plan["source_release_id"] or quote.get("schema") != plan["schema"]):
        return "QUOTE_IDENTITY_OR_TIMESTAMP_MISMATCH"
    if (quote.get("quality_valid") is not True or any(type(quote.get(k)) is not int for k in
            ("bid_nano", "ask_nano", "bid_size", "ask_size")) or quote["bid_nano"] > quote["ask_nano"]
            or min(quote["bid_size"], quote["ask_size"]) <= 0
            or any(quote[k] % trade["tick_nano"] for k in ("bid_nano", "ask_nano"))):
        return "INVALID_OR_CROSSED_QUOTE"
    last_update = quote.get("last_quote_update_ns")
    if last_update is None:
        return "RESOLVED_FRESHNESS_UNKNOWN"
    if type(last_update) is not int or last_update > at:
        return "INVALID_QUOTE_UPDATE_TIME"
    if at - last_update > plan["maximum_observed_quote_age_ns"]:
        return "STALE_QUOTE"
    return "RESOLVED_OBSERVED_FRESHNESS"


def evaluate_challenge(plan, final, trades, quotes):
    """Only exact frozen endpoint snapshots; unresolved quotes remain visible."""
    c.identity(plan, "challenge_id")
    expected = freeze_challenge(final, trades, dates=plan["dates"], source_release_id=plan["source_release_id"],
        schema=plan["schema"], forecast_ledger_sha256=plan["forecast_ledger_sha256"], frozen_at_ns=plan["frozen_at_ns"])
    if plan != expected:
        raise IntegrityError("challenge protocol or strategy changed after freezing")
    requested = {(t["market"], t["actual_contract"], t[side + "_ns"] + offset * SECOND)
                 for t in trades for side in ("entry", "exit") for offset in plan["offset_seconds"]}
    indexed = {}
    for q in quotes:
        key = (q.get("market"), q.get("actual_contract"), q.get("ts_recv"))
        if key not in requested:
            raise IntegrityError("quote values outside the exact frozen diagnostic timestamps")
        if key in indexed:
            raise IntegrityError("ambiguous duplicate quote snapshot")
        indexed[key] = q
    results = {}
    for offset in plan["offset_seconds"]:
        records, counts, totals = [], Counter(), []
        daily = {d: 0. for d in plan["dates"]}
        for t in trades:
            matched = []
            for side in ("entry", "exit"):
                at = t[side + "_ns"] + offset * SECOND
                q = indexed.get((t["market"], t["actual_contract"], at))
                status = match_quote(q, t, at, plan)
                counts[status] += 1
                buy = t["direction"] == (1 if side == "entry" else -1)
                matched.append({"side": side, "at_ns": at, "status": status,
                    "reference_nano": q["ask_nano" if buy else "bid_nano"] if status.startswith("RESOLVED") else None,
                    "bid_size": q.get("bid_size") if q else None, "ask_size": q.get("ask_size") if q else None,
                    "quote_age_ns": at - q["last_quote_update_ns"] if q and type(q.get("last_quote_update_ns")) is int else None,
                    "application_received_at_ns": q.get("application_received_at_ns") if q else None})
            complete = all(r["reference_nano"] is not None for r in matched)
            net = None
            if complete:
                gross = t["direction"] * (matched[1]["reference_nano"] - matched[0]["reference_nano"]) / t["tick_nano"] * t["tick_value_nano_usd"] / 1e9
                net = gross - plan["round_trip_fee_usd"]
                totals.append((net, t["tick_value_nano_usd"] / 1e9))
                daily[t["trade_date"]] += net
            records.append({"forecast_id": t["forecast_id"], "complete": complete, "quotes": matched,
                            "quote_side_net_usd": net, "displayed_size_guarantees_fill": False})
        complete = len(totals) == len(trades)
        results[str(offset)] = {"complete": complete, "trades": records, "quote_status_counts": dict(counts),
            "resolved_trades": len(totals), "unresolved_trades": len(trades) - len(totals),
            "quote_side_net_usd": sum(v for v, _ in totals) if complete else None,
            "daily_quote_side_net_usd": daily if complete else None,
            "additional_slippage_sensitivity_usd": {str(ticks): sum(v - ticks * unit for v, unit in totals)
                if complete else None for ticks in plan["additional_round_trip_slippage_ticks"]}}
    return c.bind({"challenge_id": plan["challenge_id"], "timing_offsets_seconds": results,
        "interpretation": plan["interpretation"], "delivery": plan["delivery"],
        "qualification_ready": False, "deployment_ready": False,
        "revision_rule": "ANY_VALIDATION_INFORMED_CHANGE_REQUIRES_NEW_INDEPENDENT_EVIDENCE"}, "challenge_result_id")
