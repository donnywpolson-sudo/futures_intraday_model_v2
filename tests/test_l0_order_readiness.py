from copy import deepcopy
import struct

import pytest

from futures_rebuild import l0_alpha_candidate as c, l0_alpha_calendar as cal
from futures_rebuild import l0_alpha_preparation as p, l0_alpha_replay as r, l0_order_readiness as order
from futures_rebuild.cme_calendar_source_adequacy import parse_biff8_cells
from futures_rebuild.errors import IntegrityError

pytestmark = pytest.mark.current


def numeric_stream(kind, data):
    def record(k, v):
        return struct.pack("<HH", k, len(v)) + v
    sst = record(0xFC, struct.pack("<II", 0, 0))
    bound = struct.pack("<IBBBB", 17 + len(sst), 0, 0, 5, 0) + b"Hours"
    return record(0x85, bound) + sst + record(kind, data) + record(0xA, b"")


def test_numeric_cells_independent_bytes_and_corruption():
    # Actual MLK 2018 family-row MULRK: two noon fractions, columns L/M.
    raw = bytes.fromhex("04000b0051000000e03f51000000e03f0c00")
    cells = parse_biff8_cells(numeric_stream(0xBD, raw))
    assert [(v.row, v.column, v.value) for v in cells] == [(4, 11, .5), (4, 12, .5)]
    for encoded, value in ((0xFFFFFFE6, -7.), (49383, 123.45), (0x3FE00000, .5), (0x3FE00001, .005)):
        single = struct.pack("<HHHI", 2, 3, 0, encoded)
        multi = struct.pack("<HHHIHIH", 2, 3, 0, encoded, 0, encoded, 4)
        assert parse_biff8_cells(numeric_stream(0x27E, single))[0].value == value
        assert [v.value for v in parse_biff8_cells(numeric_stream(0xBD, multi))] == [value, value]
    for invalid in (raw[:-1], raw + b"x", raw[:-2] + struct.pack("<H", 10), raw[:-2] + struct.pack("<H", 256)):
        with pytest.raises(IntegrityError):
            parse_biff8_cells(numeric_stream(0xBD, invalid))
    with pytest.raises(IntegrityError):
        parse_biff8_cells(numeric_stream(0x27E, b"bad"))


def sample(day="2020-03-09", market="ES"):
    return c.assemble_dataset(market, [p.synthetic_day(market, day, dense=True)], dates=(day,), as_of_ns=p.AS_OF_NS)


def schedule(d, close=None, available=0):
    first, last = d.decisions[0], d.decisions[-1]
    close = close if close is not None else last["exit_ns"] + c.MINUTE
    return c.bind({"schedules": [{"family": cal.FAMILIES[d.market], "dates": list(d.calendar_dates),
        "available_at_ns": available, "source_sha256": "a" * 64,
        "segments": [{"start_ns": first["decision_ns"] - 100 * c.MINUTE, "end_ns": close, "state": "OPEN"},
                     {"start_ns": close, "end_ns": close + 1000 * c.MINUTE, "state": "CLOSED"}]}]}, "calendar_id")


def timing(d, calendar, **kw):
    return r.replay(d.decisions, d.outcomes, None, d.calendar_dates, timing_only=True,
                    calendar=calendar, calendar_view="evidence_only", policy="always_long", **kw)


def test_strict_close_availability_unknown_conflict_and_future_invariance():
    d = sample()
    first = d.decisions[0]
    for close, expected in ((first["exit_ns"], "BLOCKED"), (first["exit_ns"] + 1, "ALLOWED")):
        assert cal.entry_permission(first, cal.compile_calendar(schedule(d, close), "evidence_only"))["state"] == expected
    late = schedule(d, available=first["decision_ns"] + 1)
    assert cal.entry_permission(first, cal.compile_calendar(late, "evidence_only"))["state"] == "UNKNOWN"
    assert cal.entry_permission(first, cal.compile_calendar(late, "assumed_availability"))["state"] == "ALLOWED"
    empty = c.bind({"schedules": []}, "calendar_id")
    assert timing(d, empty)["attempted_entries"] == 0
    assert {v["status"] for v in timing(d, empty)["decisions"]} == {"CALENDAR_UNKNOWN"}
    conflict = deepcopy(schedule(d))
    conflict["schedules"].append(deepcopy(conflict["schedules"][0]))
    conflict["schedules"][-1]["segments"][0]["state"] = "CLOSED"
    conflict.pop("calendar_id")
    assert cal.entry_permission(first, cal.compile_calendar(c.bind(conflict, "calendar_id"), "evidence_only"))["state"] == "UNKNOWN"
    before = deepcopy(d.decisions)
    timing(d, schedule(d, first["exit_ns"]))
    assert d.decisions == before


def test_shared_timing_economic_parity_without_financial_calls(monkeypatch):
    d = sample()
    calendar = schedule(d)
    economics = r.replay(d.decisions, d.outcomes, [100.] * 330, d.calendar_dates,
                         calendar=calendar, calendar_view="evidence_only", policy="always_long")
    def forbidden(*args, **kwargs):
        raise AssertionError("financial routine called during timing diagnostic")
    monkeypatch.setattr(r, "cost_nano_usd", forbidden)
    monkeypatch.setattr(r.np, "cumsum", forbidden)
    result = timing(d, calendar)
    assert result["events"] == economics["events"]
    assert result["resolved_round_trips"] == economics["trade_count"] == 6
    assert "net_usd" not in result and "prediction_ticks" not in result["decisions"][0]
    with pytest.raises(IntegrityError, match="calendar"):
        r.replay(d.decisions, d.outcomes, None, d.calendar_dates, timing_only=True, policy="always_long")


def test_missing_exit_preserves_open_position_and_stops_future_assessment():
    d = sample(market="CL")
    outcomes = deepcopy(d.outcomes)
    outcomes[0]["exit_status"] = "MISSING_REFERENCE"
    result = r.replay(d.decisions, outcomes, None, d.calendar_dates, calendar=schedule(d),
                      calendar_view="evidence_only", timing_only=True, policy="always_long")
    assert not result["complete"] and result["unresolved_position"]["prior_state"] == "OPEN"
    assert result["attempted_entries"] == 1
    assert result["decisions"][1]["status"] == "OCCUPIED_FORECAST_ONLY"
    assert result["decisions"][-1]["status"] == "STOPPED_UNRESOLVED_EXECUTION"
    outcomes[0]["entry_status"] = "MISSING_REFERENCE"
    result = r.replay(d.decisions, outcomes, None, d.calendar_dates, calendar=schedule(d),
                      calendar_view="evidence_only", timing_only=True, policy="always_long")
    assert result["unresolved_position"]["prior_state"] == "PENDING_ENTRY"


def test_frozen_windows_preserve_duplicate_slots_and_original_clock():
    windows = order.windows()
    assert len(windows) == 19 and sum(len(w["slots"]) for w in windows) == 40
    assert windows[0]["dates"][0] == "2020-02-06"
    assert len({day for w in windows for day in w["dates"]}) == 757
    assert all(w["dates"] == list(c.weekday_dates()[w["start"]:w["stop"]]) for w in windows)


def test_known_recovered_holiday_times_and_future_capture_stays_unknown():
    from pathlib import Path
    from futures_rebuild.canonical import read_canonical_authority
    calendar = read_canonical_authority(Path(__file__).resolve().parents[1] / cal.ARTIFACT, identity_field="calendar_id")
    expected = (("2018-01-15", "EQUITY_INDEX", "12:00:00"), ("2021-11-26", "EQUITY_INDEX", "12:15:00"),
                ("2021-11-26", "ENERGY", "12:45:00"), ("2022-07-04", "ENERGY", "13:30:00"))
    for day, family, clock in expected:
        matches = [e for s in calendar["schedules"] if s["family"] == family for e in s.get("events", [])
                   if e["event"] in ("HALT", "CLOSE") and e["at"] == day + "T" + clock]
        assert matches
    d = sample(day="2020-05-25")
    assert cal.entry_permission(d.decisions[0], cal.compile_calendar(calendar, "evidence_only"))["state"] == "UNKNOWN"
    assert cal.entry_permission(d.decisions[0], cal.compile_calendar(calendar, "assumed_availability"))["state"] == "ALLOWED"
