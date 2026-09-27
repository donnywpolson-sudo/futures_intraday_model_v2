from copy import deepcopy
from datetime import datetime

import numpy as np
import pytest

from futures_rebuild import l0_alpha_candidate as c
from futures_rebuild import l0_alpha_preparation as prep
from futures_rebuild.l0_alpha_replay import replay, cost_nano_usd, fixed_trade_sensitivity
from futures_rebuild.errors import IntegrityError

pytestmark = pytest.mark.current


def dataset(market="ES", day="2018-01-02", *, dense=True, rows=None, events=()):
    daily = prep.synthetic_day(market, day, 1, dense=dense)
    return c.assemble_dataset(market, [(day, daily[1] if rows is None else rows, events)],
                              dates=(day,), as_of_ns=prep.AS_OF_NS)


def forecasts(d, value=100.):
    return [value if r["status"] == "ELIGIBLE" else None for r in d.decisions]


def run(d, value=100., **kwargs):
    return replay(d.decisions, d.outcomes, forecasts(d, value), d.calendar_dates, legacy=True, **kwargs)


def test_frozen_design_calendar_dst_holidays_and_final_ranges():
    spec = c.design()
    assert len(c.weekday_dates()) == 1305
    assert "2018-12-25" in c.weekday_dates() and "2018-01-06" not in c.weekday_dates()
    assert len(c.candidate_definitions()) == 10 and spec["configuration_charges"] == 40
    assert len(c.FEATURES) == 7 and not any("quartile" in n for n in c.FEATURES)
    for day, utc_hour in (("2018-03-09", 14), ("2018-03-12", 13), ("2018-11-02", 13), ("2018-11-05", 14)):
        times = c.decision_times(day)
        assert datetime.fromtimestamp(times[0] / 1e9, c.ZONE).strftime("%H:%M:%S") == "06:30:05"
        assert datetime.utcfromtimestamp(times[0] / 1e9).hour == utc_hour
        last_exit = times[-1] - c.DELAY + 61 * c.MINUTE
        assert datetime.fromtimestamp(last_exit / 1e9, c.ZONE).strftime("%H:%M") == "13:00"
        assert len(times) == 330
    assert len(spec["folds"]) == 8 and all(len(f["inner"]) == 4 for f in spec["folds"])
    assert sum(len(f["outer"]["audit_dates"]) for f in spec["folds"]) == 504


def test_features_match_existing_formulas_and_use_history_before_window():
    day, rows, _ = prep.synthetic_day("ES", "2018-01-02", 1, dense=True)
    d = dataset(rows=rows)
    assert all(r["status"] == "ELIGIBLE" for r in d.decisions)
    original, _ = c.old.multifamily_feature_values(rows[40:61], d.decisions[0]["decision_ns"])
    np.testing.assert_allclose(d.features[0, :5], [original[n] for n in c.FEATURES[:5]], rtol=0, atol=1e-12)
    assert d.decisions[0]["entry_ns"] - d.decisions[0]["decision_ns"] == c.MINUTE - c.DELAY


def test_future_prices_and_missing_target_do_not_filter_forecast():
    d = dataset()
    _, rows, _ = prep.synthetic_day("ES", "2018-01-02", 1, dense=True)
    first = d.decisions[0]
    changed = deepcopy(rows)
    for row in changed:
        if row["bar_start_ns"] >= first["entry_ns"]:
            for key in ("open_nano", "close_nano", "high_nano", "low_nano"):
                row[key] -= 1000 * row["min_price_increment_nano"]
    other = dataset(rows=changed)
    assert other.decisions[0] == first
    np.testing.assert_array_equal(d.features[0], other.features[0])
    missing = dataset(rows=[r for r in rows if r["bar_start_ns"] != first["exit_ns"]])
    assert missing.decisions[0] == first and missing.outcomes[0]["status"] == "UNAVAILABLE_TARGET"
    assert len(missing.decisions) == 330
    assert missing.outcomes[0]["exit_status"] == "MISSING_REFERENCE"


def test_contract_roll_resets_history_and_never_stitches_target():
    d = dataset()
    _, rows, _ = prep.synthetic_day("ES", "2018-01-02", 1, dense=True)
    for row in rows:
        if row["bar_start_ns"] >= d.decisions[0]["entry_ns"]:
            row["actual_contract"] = "ESM8"
    after = dataset(rows=rows)
    assert after.decisions[0] == d.decisions[0]
    assert after.outcomes[0]["entry_status"] == "CONTRACT_MISMATCH"
    assert any(r["status"] == "CONTRACT_HISTORY_RESET" for r in after.decisions)
    assert after.decisions[-1]["status"] == "ELIGIBLE"


def test_missing_bars_never_prove_closure_and_retrospective_status_does_not_abstain():
    d = dataset(rows=[])
    assert len(d.decisions) == 330 and all(r["status"] == "UNAVAILABLE_HISTORY" for r in d.decisions)
    assert d.decisions[0]["coverage"] == ["UNRESOLVED_COVERAGE"]
    at = d.decisions[0]["decision_ns"]
    event = {"start_ns": at, "end_ns": at + 24 * 60 * c.MINUTE, "known_at_ns": at + 1,
             "state": "CLOSED", "evidence_sha256": "a" * 64}
    late = dataset(events=[event])
    assert late.decisions[0]["status"] == "ELIGIBLE"
    event["known_at_ns"] = at
    closed = dataset(events=[event])
    assert all(r["status"] == "VERIFIED_CLOSED" for r in closed.decisions)
    event.update(state="NO_TRADES", known_at_ns=at + 100 * c.MINUTE, start_ns=at - 61 * c.MINUTE)
    no_trades = dataset(rows=[], events=[event])
    assert no_trades.decisions[0]["status"] == "UNAVAILABLE_HISTORY"
    assert no_trades.decisions[0]["coverage"] == ["NO_TRADES"]


def test_metadata_availability_maturity_and_mutation_fail_closed():
    _, rows, _ = prep.synthetic_day("ES", "2018-01-02", 1, dense=True)
    at = c.decision_times("2018-01-02")[0]
    rows[0]["definition_received_at_ns"] = at + 1
    d = dataset(rows=rows)
    assert d.decisions[0]["status"] == "UNAVAILABLE_HISTORY_OR_METADATA"
    good = dataset()
    outcome = c.attach_outcomes(good.decisions[:1], rows, as_of_ns=at)[0]
    assert outcome["target_ticks"] is None and outcome["entry_status"] == "UNMATURED"
    good.decisions[0]["status"] = "FORGED"
    with pytest.raises(IntegrityError, match="mutated"):
        good.verify()
    with pytest.raises(IntegrityError, match="duplicated"):
        dataset(rows=rows + rows[:1])


@pytest.mark.parametrize("market", c.MARKETS)
def test_cost_units_negative_prices_and_reference_pnl(market):
    _, rows, _ = prep.synthetic_day(market, "2018-01-02", 1, dense=True)
    offset = rows[0]["open_nano"] + 100 * rows[0]["min_price_increment_nano"]
    for row in rows:
        for key in ("open_nano", "high_nano", "low_nano", "close_nano"):
            row[key] -= offset
    d = dataset(market, rows=rows)
    result = run(d)
    assert result["complete"] and result["trade_count"] == 6
    trade = result["trades"][0]
    assert trade["gross_nano_usd"] == ((trade["exit_open_nano"] - trade["entry_open_nano"]) // trade["tick_nano"]) * trade["tick_value_nano_usd"]
    assert trade["cost_nano_usd"] == 10 * 10**9 + (6 if market in ("ES", "NQ") else 10) * trade["tick_value_nano_usd"]
    assert result["net_usd"] == sum(t["net_nano_usd"] for t in result["trades"]) / 1e9


def test_entry_and_exit_missing_preserve_occupancy_and_incomplete_economics():
    original = dataset()
    _, rows, _ = prep.synthetic_day("ES", "2018-01-02", 1, dense=True)
    for side in ("entry", "exit"):
        d = dataset(rows=[r for r in rows if r["bar_start_ns"] != original.decisions[0][side + "_ns"]])
        result = run(d)
        assert not result["complete"] and result["mean_daily_net_usd"] is None
        assert result["unresolved_position"]["side"] == side
        assert result["unresolved_position"]["direction"] == 1
        assert result["decisions"][0]["status"] == "PENDING_ENTRY"
        assert result["events"][-1]["state"] == "UNRESOLVED"
        assert result["decisions"][-1]["status"] == "STOPPED_UNRESOLVED_EXECUTION"


def test_occupied_signals_ignored_exit_precedes_later_decision_and_ties_abstain():
    d = dataset()
    result = run(d)
    assert result["trade_count"] == 6
    assert result["decisions"][1]["status"] == "OCCUPIED_FORECAST_ONLY"
    assert result["decisions"][60]["status"] == "OCCUPIED_FORECAST_ONLY"
    assert result["decisions"][61]["status"] == "PENDING_ENTRY"
    assert result["events"][:4] == [
        {"forecast_id": d.decisions[0]["forecast_id"], "at_ns": d.decisions[0]["decision_ns"], "state": "PENDING_ENTRY"},
        {"forecast_id": d.decisions[0]["forecast_id"], "at_ns": d.decisions[0]["entry_ns"], "state": "OPEN"},
        {"forecast_id": d.decisions[0]["forecast_id"], "at_ns": d.decisions[0]["exit_ns"], "state": "SCHEDULED_EXIT"},
        {"forecast_id": d.decisions[0]["forecast_id"], "at_ns": d.decisions[0]["exit_ns"], "state": "FLAT"}]
    threshold = cost_nano_usd(d.decisions[0], "adverse") / d.decisions[0]["tick_value_nano_usd"]
    assert run(d, threshold)["trade_count"] == 0 and run(d, -threshold)["trade_count"] == 0


def test_whole_policy_cost_sensitivity_differs_from_fixed_trade_repricing():
    d = dataset()
    selection = run(d, 5.)
    base = run(d, 5., scenario="base")
    assert selection["trade_count"] == 0 and base["trade_count"] == 6
    assert fixed_trade_sensitivity(selection)["scenarios"]["base"]["trade_count"] == 0
    assert run(d, policy="always_long")["trade_count"] == run(d, policy="always_short")["trade_count"] == 6
    assert run(d, policy="no_trade")["mean_daily_net_usd"] == 0


def test_empty_dates_keep_common_denominator_and_missing_dates_fail():
    daily = [prep.synthetic_day("ES", "2018-01-02", 1, dense=True), ("2018-01-03", [], [])]
    d = c.assemble_dataset("ES", daily, dates=("2018-01-02", "2018-01-03"), as_of_ns=prep.AS_OF_NS)
    result = run(d)
    assert result["mean_daily_net_usd"] == result["net_usd"] / 2 and result["zero_trade_dates"] == 1
    with pytest.raises(IntegrityError, match="missing"):
        c.assemble_dataset("ES", daily[:1], dates=d.calendar_dates, as_of_ns=prep.AS_OF_NS)


def test_dataset_rejects_forged_maturity_and_target_and_feature_abstention():
    d = dataset()
    for change in ({"target_ticks": 12345.}, {"known_at_ns": d.decisions[0]["decision_ns"]}):
        outcomes = [dict(o) for o in d.outcomes]
        outcomes[0].update(change)
        with pytest.raises(IntegrityError, match="mature label"):
            c.Dataset(d.market, d.calendar_dates, d.decisions, d.features, tuple(outcomes), d.source_facts, d.as_of_ns)
    decisions = deepcopy(d.decisions)
    decisions[0]["status"] = "UNAVAILABLE_HISTORY"
    with pytest.raises(IntegrityError, match="abstention"):
        c.Dataset(d.market, d.calendar_dates, decisions, d.features, d.outcomes, d.source_facts, d.as_of_ns)
