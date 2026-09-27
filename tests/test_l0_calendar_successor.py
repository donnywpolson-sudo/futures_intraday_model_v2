"""Reference-only regression checks for recovered custody and temporal admission."""
from copy import deepcopy
from pathlib import Path
import json
import sys

import pytest

pytestmark = pytest.mark.current

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import prepare_l0_calendar_successor as successor
sys.path.pop(0)

from futures_rebuild import l0_alpha_calendar as cal, l0_alpha_candidate as c
from futures_rebuild.errors import IntegrityError


def inputs():
    return json.loads((ROOT / successor.OUTPUT / "input_manifest.json").read_text())


def row(market, day, index):
    at = c.decision_times(day)[index]
    entry = (at // c.MINUTE + 1) * c.MINUTE
    return {"market": market, "trade_date": day, "decision_ns": at,
            "entry_ns": entry, "exit_ns": entry + 60 * c.MINUTE}


def test_recovered_original_long_path_and_stale_identity():
    manifest = inputs()
    recovery = json.loads((ROOT / successor.OUTPUT / "january_recovery.json").read_text())
    for name in ("january_original.xls", "january_original.sidecar.json"):
        original = recovery["copies"][name]
        binding = {"path": original["original_path"], "sha256": original["sha256"]}
        assert successor.read_bound(ROOT, binding) == (ROOT / successor.OUTPUT / name).read_bytes()
        with pytest.raises(IntegrityError, match="binding changed"):
            successor.read_bound(ROOT, {**binding, "sha256": "0" * 64})
    stale = deepcopy(manifest)
    stale["prior_calendar_id"] = "0" * 64
    with pytest.raises(IntegrityError, match="prior calendar identity"):
        successor.build_successor(ROOT, stale)


def test_exact_closures_full_exit_and_publication_boundary():
    before, after = successor.build_successor(ROOT, inputs())
    old, new = (cal.compile_calendar(x, "evidence_only") for x in (before, after))
    for market in c.MARKETS:
        for index in (0, 329):
            decision = row(market, "2019-01-01", index)
            assert cal.entry_permission(decision, old)["state"] == "UNKNOWN"
            assert cal.entry_permission(decision, new)["state"] == "BLOCKED"
    for market in ("ES", "NQ"):
        assert cal.entry_permission(row(market, "2018-12-05", 0), new)["state"] == "BLOCKED"
    assert cal.entry_permission(row("CL", "2018-12-05", 0), new)["state"] == "UNKNOWN"
    for market in c.MARKETS:
        assert cal.entry_permission(row(market, "2019-01-02", 329), new)["state"] == "ALLOWED"
        assert cal.entry_permission(row(market, "2018-12-31", 0), new)["state"] == "UNKNOWN"
    decision = row("ES", "2018-12-05", 0)
    decision.update(decision_ns=decision["decision_ns"]-30*c.MINUTE, entry_ns=decision["entry_ns"]-30*c.MINUTE,
                    exit_ns=decision["exit_ns"]-30*c.MINUTE)
    assert cal.entry_permission(decision, new)["state"] == "BLOCKED"  # exit crosses known close
    changed = deepcopy(after)
    for s in changed["schedules"]:
        if s["dates"] == ["2018-12-05"]:
            s["available_at_ns"] = decision["decision_ns"] + 1
    changed.pop("calendar_id")
    assert cal.entry_permission(decision, cal.compile_calendar(c.bind(changed, "calendar_id"), "evidence_only"))["state"] == "UNKNOWN"


def test_contract_projection_and_exact_slots_remain_blocked():
    manifest = inputs()
    bad = deepcopy(manifest)
    bad["roll_metadata"] = [{**bad["roll_metadata"][0], "market": "YM"}]
    with pytest.raises(IntegrityError, match="undeclared metadata read"):
        successor.selected_contract_metadata(ROOT, bad)
    assert set(successor.ROLL_COLUMNS) == {"observation_ordinal", "actual_contract_before", "actual_contract_after", "effective_time_ns"}
    partial = json.loads((ROOT / successor.OUTPUT / "admission_partial_inputs.json").read_text())
    rows = partial["fold_evidence_partial"]
    assert len({(r["market"], r["outer_fold"], r["role"]) for r in rows}) == 160
    assert all(r["calendar"]["unknown"] > 0 and sum(r["calendar"].values()) == 63 * 330 for r in rows)
    assert not partial["candidate_matches_contract_calendar"] and not partial["full_admission_run"]
