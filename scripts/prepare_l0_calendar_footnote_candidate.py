"""Inactive calendar evidence candidate. Reads reference workbooks, never market rows.

Run from the repository root. --check verifies saved outputs without rewriting.
The frozen builder and its bound runtime remain untouched pending promotion.
"""
from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from zipfile import ZipFile
import argparse
import io
import json
import re

from futures_rebuild import l0_alpha_calendar as cal, l0_alpha_candidate as c
from futures_rebuild import l0_alpha_preparation as prep, l0_order_readiness as order
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError

OUTPUT = Path(cal.OUTPUT) / "calendar_remediation_v1"
FOOTNOTE = re.compile(r"(\w+),\s+(\w+)\s+(\d+)\s*:\s*Regular CME Group Globex close for all products", re.I)


def terminal_claim(cells, item):
    """Read a dated regular-close footnote; reject ambiguity rather than repair it."""
    notes = [(x, FOOTNOTE.fullmatch(str(x.value).strip())) for x in cells if x.sheet == item["sheet"]]
    notes = [(x, m) for x, m in notes if m]
    if not notes:
        return None, "NO_EXPLICIT_REGULAR_CLOSE_FOOTNOTE"
    if len(notes) != 1:
        return None, "AMBIGUOUS_FOOTNOTES"
    cell, match = notes[0]
    last = item["events"][-1]
    opened = datetime.fromisoformat(last["at"])
    if last["event"] != "OPEN":
        return None, "TERMINAL_EVENT_NOT_OPEN"
    day = None
    for fmt in ("%Y %B %d", "%Y %b %d"):
        try:
            day = datetime.strptime(f"{opened.year} {match[2]} {match[3]}", fmt).date()
            break
        except ValueError:
            pass
    if day is None or day.strftime("%A").lower() != match[1].lower() or day != opened.date() + timedelta(days=1):
        return None, "CONTRADICTORY_FOOTNOTE_DATE"
    # The workbook's own first regular close supplies the clock, not a current calendar.
    row_cells = {x.column: x for x in cells if x.sheet == item["sheet"] and x.row == item["row_zero_based"]}
    close_cell = row_cells.get(1)
    headers = [x for x in cells if x.sheet == item["sheet"] and x.column == 1
               and x.row < item["row_zero_based"] and str(x.value) == "Regular Fri. Close"]
    regular_text = close_cell is not None and str(close_cell.value).startswith("Regular @ ")
    closes = [e for e in item["events"] if e["column"] == 1 and e["event"] == "CLOSE"]
    if close_cell is None or not (headers or regular_text) or len(closes) != 1:
        return None, "REGULAR_CLOSE_CLOCK_NOT_EXPLICIT"
    close = datetime.combine(day, datetime.fromisoformat(closes[0]["at"]).time(), cal.CHICAGO)
    decisions = c.decision_times(day.isoformat())
    final_exit = (decisions[-1] // c.MINUTE + 61) * c.MINUTE
    close_ns = int(close.timestamp()) * 10**9
    if int(opened.replace(tzinfo=cal.CHICAGO).timestamp()) * 10**9 >= decisions[0] or close_ns <= final_exit:
        return None, "REGULAR_SESSION_DOES_NOT_COVER_FULL_FROZEN_WINDOW"
    return {"date": day.isoformat(), "footnote_cell": [cell.row, cell.column], "footnote": cell.value,
            "regular_close_cell": [close_cell.row, close_cell.column], "regular_close_value": close_cell.value,
            "regular_close_at": close.isoformat(), "terminal_open_at": last["at"],
            "claim": "DATED_WORKBOOK_REGULAR_SESSION_AFTER_EXPLICIT_REOPEN;FROZEN_WINDOW_ONLY",
            "limitation": "Family schedule only; not exact-contract termination or evidence of no conditional halt",
            # This is an evidence coverage boundary, NOT a newly invented exchange close.
            "segment": {"start_ns": decisions[0], "end_ns": final_exit + 1, "state": "OPEN"}}, None


def candidate(root, baseline):
    result = deepcopy(baseline)
    result.pop("calendar_id")
    inventory = []
    cache = {}
    for item in result["schedules"]:
        if "member" not in item:
            continue
        key = (item["source"], item["member"])
        if key not in cache:
            raw = Path(item["source"]).read_bytes()
            if sha256(raw).hexdigest() != item["source_sha256"]:
                raise IntegrityError("calendar source changed")
            if Path(item["source"]).suffix == ".zip":
                with ZipFile(io.BytesIO(raw)) as z:
                    raw = z.read(item["member"])
            if sha256(raw).hexdigest() != item["member_sha256"]:
                raise IntegrityError("workbook member changed")
            cache[key] = cal.read_xls_bytes(raw)
        claim, reason = terminal_claim(cache[key], item)
        inventory.append({"source": item["source"], "member": item["member"], "family": item["family"],
                          "source_sha256": item["source_sha256"], "member_sha256": item["member_sha256"],
                          "available_at_ns": item["available_at_ns"], "claim": claim, "unresolved_reason": reason})
        if claim:
            item["segments"].append(claim["segment"])
            item["dates"] = sorted(set(item["dates"] + [claim["date"]]))
            item["terminal_footnote_evidence"] = claim
    result["parent_calendar_id"] = baseline["calendar_id"]
    result["candidate_scope"] = "UNPROMOTED_REFERENCE_CALENDAR_COMPONENT;NO_ADMISSION_AUTHORITY"
    result["builder_sha256"] = sha256_file(Path(__file__))
    return c.bind(result, "calendar_id"), inventory


def coverage(before, after):
    audit = {day for w in order.windows() for day in w["dates"]}
    output = {"dates": len(c.weekday_dates()), "audit_union_dates": len(audit), "views": {}, "folds": []}
    for view in cal.VIEWS:
        contexts = [cal.compile_calendar(x, view) for x in (before, after)]
        markets = {}
        for market in c.MARKETS:
            daily = []
            for day in c.weekday_dates():
                runs, counts = [], [Counter(), Counter()]
                for i, at in enumerate(c.decision_times(day)):
                    entry = (at // c.MINUTE + 1) * c.MINUTE
                    row = {"market": market, "trade_date": day, "decision_ns": at,
                           "entry_ns": entry, "exit_ns": entry + 60 * c.MINUTE}
                    checks = [cal.entry_permission(row, ctx) for ctx in contexts]
                    states = [check["state"] for check in checks]
                    if states[0] != "UNKNOWN" and states[0] != states[1]:
                        raise IntegrityError("candidate regressed an existing supported classification")
                    for count, state in zip(counts, states):
                        count[state] += 1
                    reason = checks[1]["reason"]
                    if states[1] == "UNKNOWN" and reason != "CONFLICTING_SCHEDULES":
                        items = contexts[1]["days"].get((cal.FAMILIES[market], day), [])
                        reason = "NO_DATE_SPECIFIC_SCHEDULE" if not items else "PUBLICATION_UNSUPPORTED_OR_LATE" if checks[1]["unavailable_sources"] == len(items) else "INTERVAL_NOT_FULLY_COVERED"
                    signature = [*states, reason]
                    if runs and runs[-1][2:] == signature:
                        runs[-1][1] = i + 1
                    else:
                        runs.append([i, i + 1, *signature])
                daily.append({"date": day, "in_audit_union": day in audit,
                              "before": dict(counts[0]), "after": dict(counts[1]), "runs": runs})
            summary = {}
            for scope in ("development", "audit_union"):
                summary[scope] = {version: dict(sum((Counter(d[version]) for d in daily
                    if scope == "development" or d["in_audit_union"]), Counter())) for version in ("before", "after")}
            markets[market] = {"summary": summary, "daily": daily}
            if view == "evidence_only":
                by_day = {d["date"]: d for d in daily}
                for w in order.windows():
                    counts = sum((Counter(by_day[d]["after"]) for d in w["dates"]), Counter())
                    output["folds"].append({"market": market, "slots": w["slots"], "counts": dict(counts),
                        "calendar_unknown_rejection_applies": counts["UNKNOWN"] > 0})
        output["views"][view] = markets
    return output


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--check", action="store_true")
    check = args.parse_args().check
    root = Path.cwd()
    prep.validate_bundle(root)
    baseline = json.loads((root / cal.ARTIFACT).read_text())
    if cal.build_calendar(root, cal.calendar_inputs(root)) != baseline:
        raise IntegrityError("baseline does not reproduce")
    after, claims = candidate(root, baseline)
    outputs = {"calendar_candidate.json": after, "footnote_inventory.json": claims,
               "coverage.json": coverage(baseline, after),
               "bindings.json": {"parent": sha256_file(root / cal.ARTIFACT),
                    "bundle": sha256_file(root / prep.CONFIG), "builder": sha256_file(Path(__file__)),
                    "mechanism_id": c.mechanism_id(), "active": False}}
    destination = root / OUTPUT
    if not check:
        destination.mkdir(parents=True, exist_ok=True)
    for name, value in outputs.items():
        path = destination / name
        if check or path.exists():
            if not path.exists() or json.loads(path.read_text()) != value:
                raise IntegrityError(f"saved candidate differs: {name}")
        else:
            with path.open("x", encoding="utf-8", newline="\n") as stream:
                json.dump(value, stream, sort_keys=True, separators=(",", ":"))
                stream.write("\n")
    print(json.dumps({"calendar_id": after["calendar_id"], "saved_outputs_match": True,
                      "summary": {m: v["summary"] for m, v in outputs["coverage.json"]["views"]["evidence_only"].items()}}))


if __name__ == "__main__":
    main()
