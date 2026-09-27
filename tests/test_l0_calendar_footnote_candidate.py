"""Reference-only checks; no prices, orders, models, or profitability."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import json

from futures_rebuild import l0_alpha_calendar as cal, l0_alpha_candidate as c

ROOT = Path(__file__).resolve().parents[1]
SPEC = spec_from_file_location("footnote_candidate", ROOT / "scripts/prepare_l0_calendar_footnote_candidate.py")
mod = module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


def test_owned_footnote_date_clock_publication_and_scope():
    baseline = json.loads((ROOT / cal.ARTIFACT).read_text())
    item = next(x for x in baseline["schedules"] if x.get("member") == "2022_labor.xls" and x["family"] == "EQUITY_INDEX")
    cells = cal.read_xls_bytes(Path(item["source"]).read_bytes())
    claim, reason = mod.terminal_claim(cells, item)
    assert reason is None and claim["date"] == "2022-09-06"
    assert claim["regular_close_at"] == "2022-09-06T16:00:00-05:00"
    assert datetime.fromtimestamp(claim["segment"]["end_ns"] // 10**9, c.ZONE).isoformat() == "2022-09-06T13:00:00-07:00"
    changed = [replace(x, value="Tuesday, September 7: Regular CME Group Globex close for all products")
               if [x.row, x.column] == claim["footnote_cell"] and x.sheet == item["sheet"] else x for x in cells]
    assert mod.terminal_claim(changed, item)[1] == "CONTRADICTORY_FOOTNOTE_DATE"
    candidate, inventory = mod.candidate(ROOT, baseline)
    assert all(x["unresolved_reason"] == "CONTRADICTORY_FOOTNOTE_DATE" for x in inventory
               if x["member"] in ("2022_juneteenth.xls", "2022_memorial.xls"))
    context = cal.compile_calendar(candidate, "evidence_only")
    day = claim["date"]
    at = c.decision_times(day)[-1]
    entry = (at // c.MINUTE + 1) * c.MINUTE
    row = {"market": "ES", "trade_date": day, "decision_ns": at, "entry_ns": entry, "exit_ns": entry + 60*c.MINUTE}
    assert cal.entry_permission(row, context)["state"] == "ALLOWED"
    assert cal.entry_permission(row, cal.compile_calendar(baseline, "evidence_only"))["state"] == "UNKNOWN"
    assert cal.entry_permission({**row, "exit_ns": claim["segment"]["end_ns"]}, context)["state"] == "UNKNOWN"
    late = deepcopy(candidate)
    for source in late["schedules"]:
        if day in source["dates"]:
            source["available_at_ns"] = row["decision_ns"] + 1
    late.pop("calendar_id")
    assert cal.entry_permission(row, cal.compile_calendar(c.bind(late, "calendar_id"), "evidence_only"))["state"] == "UNKNOWN"
    # Winter Central/Pacific conversions remain date-aware as well.
    winter = next(x["claim"] for x in inventory if x["member"] == "2018_mlk.xls" and x["claim"])
    assert winter["regular_close_at"] == "2018-01-16T16:00:00-06:00"
    assert datetime.fromtimestamp(winter["segment"]["end_ns"] // 10**9, c.ZONE).isoformat() == "2018-01-16T13:00:00-08:00"
