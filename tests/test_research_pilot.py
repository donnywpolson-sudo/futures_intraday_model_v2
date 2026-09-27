"""Generated fixtures only: no market readers, stored predictions, or historical fixtures.

Calendar: Mondays open, other days closed; not an exchange calendar.
Prices, signal, clocks, costs, support, priority and risk are SYNTHETIC STUDY VALUES.
Only the calendar/source provenance and ready-at/reference-fill interfaces are doubles.
All fitting, prediction, selection, aggregation and accounting use research_pilot.
"""
from dataclasses import replace
from datetime import date, datetime, timedelta
from functools import lru_cache
from math import sqrt

import numpy as np
import pytest

from futures_rebuild import research_pilot as p
from futures_rebuild.canonical import sha256_json
from futures_rebuild.discovery import bind
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.research.contracts import SyntheticOnlyPermit, ResearchContractError

pytestmark = pytest.mark.current
BILLION = 10**9
ES = p.Instrument("SYNTHETIC_ES", 250_000_000, 1250, 50)
NQ = p.Instrument("SYNTHETIC_NQ", 250_000_000, 500, 20)


def rebound(data, **changes):
    data = replace(data, **changes)
    return replace(data, permit=replace(data.permit, dataset_sha256=data.digest()))


def study(markets=("ES", "NQ"), **changes):
    history = bind({
        "schema_version": "discovery_study/1", "scientific_id": "SYNTHETIC_M1_FIXTURE_NOT_RESEARCH",
        "question": "Can the actual path learn generated y=4x?",
        "kind": "PREDICTIVE", "parents": [], "parentage": "NO_PARENT_KNOWN",
        "intended_change": "GENERATED_MECHANICS_ONLY", "informed_by": [], "exposure": [],
        "history_complete": False,
        "declaration": {"path": "SYNTHETIC_IN_MEMORY_DECLARATION",
                        "sha256": sha256_json("SYNTHETIC_M1_FIXTURE_NOT_RESEARCH")},
    }, "study_summary_id")
    value = {
        "protocol_id": p.protocol()["protocol_id"], "history": history, "kind": "PREDICTIVE",
        "markets": list(markets), "fixture_only": True, "ridge_penalty": 1.,
        "minimum_train_sessions": 2, "clock_offsets_seconds": [0, 300, 600],
        "route": {"tiers": [0, 1, 2, 3], "anchor": markets[0], "required_anchor": True,
                  "allow_inconclusive_expansion": False,
                  "macro_disposition": "NO_ELIGIBLE_MACRO_DIVERSIFIER"},
        "risk": {"market_priority": list(markets), "max_quantity": 1, "per_market_quantity": 1,
                 "risk_per_contract_cents": 10_000, "max_reserved_risk_cents": 10_000,
                 "round_trip_fee_cents": 25, "round_trip_slippage_ticks": 1},
    }
    return {**value, **changes}


@lru_cache
def generated():
    day, end = date(2015, 1, 1), date(2023, 4, 1)
    sessions, decisions, outcomes = [], [], []
    while day < end:
        first = p.ns(datetime(day.year, day.month, day.day, 6, 30, 5, tzinfo=p.PACIFIC))
        opened = day.weekday() == 0
        sessions.append(p.Session(day.isoformat(), p.local_midnight(day),
                                  p.local_midnight(day + timedelta(days=1)), first,
                                  "OPEN" if opened else "CLOSED"))
        if opened:
            for market, instrument in (("ES", ES), ("NQ", NQ)):
                for i, x in enumerate((-1., 0., 1.)):
                    at = first + i * 300 * BILLION
                    sample = f"SYNTHETIC:{market}:{day}:{i}"
                    d = p.Decision(sample, market, instrument.contract, day.isoformat(),
                                   at, at, (x,), at + 60*BILLION, at + 180*BILLION)
                    decisions.append(d)
                    outcomes.append(p.Outcome(sample, d.contract, d.contract, 100*BILLION,
                                              100*BILLION + int(4*x)*instrument.tick_nano,
                                              d.exit_ns + 60*BILLION))
        day += timedelta(days=1)
    data = p.Inputs(tuple(decisions), tuple(outcomes), tuple(sessions), (ES, NQ),
                    p.local_midnight(date(2015, 1, 1)), p.local_midnight(end),
                    SyntheticOnlyPermit("MECHANICS_ONLY", "SYNTHETIC", "m1-generated-v1", 0, "0"*64))
    return rebound(data)


def small_fit_data():
    data = generated()
    dates = {"2020-01-06", "2021-01-04"}
    decisions = tuple(d for d in data.decisions if d.market == "ES" and d.session in dates)
    # y=3+2x, with a duplicate and a constant column: independent closed-form oracle below.
    decisions = tuple(replace(d, features=(d.features[0], 7., d.features[0])) for d in decisions)
    outcomes = tuple(p.Outcome(d.sample_id, d.contract, d.contract, 100*BILLION,
                              100*BILLION + int(3+2*d.features[0])*ES.tick_nano,
                              d.exit_ns) for d in decisions)
    return rebound(data, decisions=decisions, outcomes=outcomes)


def outer_window():
    return p.schedule(generated().sessions, "2022-10-01", "2023-01-01")[0]["outer"]


@pytest.mark.parametrize("policy", p.POLICIES)
def test_actual_fit_prediction_independent_arithmetic(policy):
    data, window, spec = small_fit_data(), outer_window(), study(("ES",))
    model = p.fit(data, "ES", window, policy, spec, ready_at=window["activation_ns"])
    # Each date has x=(-1,0,1), y=(1,3,5). E[x]=0; Var[x]=2/3.
    # With lambda=1 and standardized x, beta=sqrt(2/3), intercept=3, raw slope=1.
    assert model["columns"] == [0]
    assert model["means"] == pytest.approx([0.], abs=1e-14)
    assert model["scales"] == pytest.approx([sqrt(2/3)])
    assert model["beta"] == pytest.approx([sqrt(2/3)])
    assert model["intercept"] == pytest.approx(3.)
    assert model["equal_session_training_mean"] == pytest.approx(3.)
    future = [replace(d, decision_ns=window["activation_ns"] + i*BILLION,
                      feature_available_ns=window["activation_ns"])
              for i, d in enumerate(data.decisions[:3])]
    forecasts = p.predict(model, future)
    assert [r["prediction_ticks"] for r in forecasts] == pytest.approx([2., 3., 4.])
    # Independent objective at the actual returned solution: mean squared residual 2/3 + beta^2 2/3.
    assert sum((a-b)**2 for a, b in zip((1, 3, 5), [r["prediction_ticks"] for r in forecasts]))/3 \
        + model["beta"][0]**2 == pytest.approx(4/3)
    # Cost/risk changes cannot silently change this predictive fit or forecasts.
    changed = {**spec, "risk": {**spec["risk"], "round_trip_fee_cents": 777}}
    again = p.fit(data, "ES", window, policy, changed, ready_at=window["activation_ns"])
    assert again == model
    assert model["latest_label_ns"] < model["cutoff_ns"]


def test_both_weights_concentration_and_independent_baseline():
    data, window, spec = small_fit_data(), outer_window(), study(("ES",))
    # Older date: y=2x. Newer date: y=10+4x. Distinct intercept reveals benchmark independence.
    decisions = tuple(replace(d, features=(d.features[0],)) for d in data.decisions)
    outcomes = tuple(replace(o, exit_nano=100*BILLION
                            + int((0 if d.session == "2020-01-06" else 10)
                                  + (2 if d.session == "2020-01-06" else 4)*d.features[0])*ES.tick_nano)
                     for d, o in zip(decisions, data.outcomes))
    data = rebound(data, decisions=decisions, outcomes=outcomes)
    models = [p.fit(data, "ES", window, q, spec, ready_at=window["activation_ns"]) for q in p.POLICIES]
    age_gap = (date(2021, 1, 4) - date(2020, 1, 6)).days
    recent_mass = 1/(1 + 2**(-age_gap/365.25))
    for m, r in zip(models, (0.5, recent_mass)):
        assert m["intercept"] == pytest.approx(10*r)
        assert m["beta"][0]/m["scales"][0] == pytest.approx(1+r)
        assert m["equal_session_training_mean"] == pytest.approx(5.)
        assert m["session_weight_max"] == pytest.approx(max(r, 1-r))
        assert m["session_weight_inverse_hhi"] == pytest.approx(1/(r*r+(1-r)**2))
    # Unequal row counts receive equal total session mass.
    dates = ["2020-01-06"]*3 + ["2021-01-04"]
    weights = p.row_weights(dates, window["cutoff_ns"], p.POLICIES[0])
    assert weights == pytest.approx([1/6, 1/6, 1/6, 1/2])
    assert sum(weights) == pytest.approx(1.)


def test_evaluation_equal_dates_independent_of_fitting_and_quarter_size():
    # Bound scoring records are test inputs to the real aggregator, not a substitute fitter.
    ds, os, rs = [], [], []
    for number, (day, n, y) in enumerate((("2022-12-26", 1, 3), ("2023-01-02", 3, 1),
                                        ("2023-01-09", 2, 1), ("2023-01-16", 1, 1))):
        for i in range(n):
            at = number*100+i
            d = p.Decision(str(at), "ES", ES.contract, day, at, at, (0.,), at+1, at+2)
            ds.append(d)
            os.append(p.Outcome(d.sample_id, d.contract, d.contract, 0, y*ES.tick_nano, at+2))
            rs.append(bind(dict(sample_id=d.sample_id, session=day, decision_ns=at, model_id="SYNTHETIC",
                                status="FORECAST", prediction_ticks=0., zero_ticks=0., mean_ticks=1.),
                           "forecast_id"))
    score = p.evaluate(rs, ds, os, (ES,), as_of=10000, aggregation=p.AGGREGATION)
    # Per-date loss [9,1,1,1] => 3; neither row-weighted 15/7 nor equal-quarter 5.
    assert score["mse"] == pytest.approx([3., 3., 1.])
    assert score["scored_targets"] == 7
    with pytest.raises(IntegrityError, match="aggregation"):
        p.evaluate(rs, ds, os, (ES,), as_of=10000, aggregation=p.POLICIES[1])


def test_known_learnable_actual_end_to_end_two_market_path():
    data = generated()
    result = p.run_pilot(data, study(), "2022-10-01", "2023-04-01")
    assert result["status"] == "COMPLETE_SYNTHETIC_MECHANICS"
    assert result["fit_count"] == 36  # 2 markets * 2 outer quarters * (2 policies * 4 inner + 1 final)
    expected = [d for d in data.decisions if "2022-10-01" <= d.session < "2023-04-01"]
    assert {r["sample_id"] for r in result["forecasts"]} == {d.sample_id for d in expected}
    for score in result["outer_metrics"].values():
        assert score["mse"] == pytest.approx([8/3, 32/3, 32/3])
        assert score["missing_targets"] == 0
    assert {v["policy"] for v in result["fits"]} == set(p.POLICIES)
    events = result["portfolio"]["events"]
    assert any(v["event"] == "ABSTAIN_SHARED_RISK" and v["market"] == "NQ" for v in events)
    assert not any(v["event"] == "ENTRY_REFERENCE" and v["market"] == "NQ" for v in events)
    assert result["qualifications"]["predictive"] == "NOT_QUALIFIED"
    assert result["qualifications"]["economic"] == "UNAVAILABLE_NO_ADOPTED_METHOD"
    assert result["recent_development_claim"].startswith("DEVELOPMENT_EVIDENCE_ONLY")


def test_no_signal_control_uses_actual_fitter():
    data, window = small_fit_data(), outer_window()
    outcomes = tuple(replace(o, exit_nano=o.entry_nano) for o in data.outcomes)
    data = rebound(data, outcomes=outcomes)
    model = p.fit(data, "ES", window, p.POLICIES[0], study(("ES",)), ready_at=window["activation_ns"])
    assert model["beta"] == [0.]
    assert model["intercept"] == 0.  # Deterministic null; no claimed stochastic false-positive calibration.


def test_causal_sentinels_and_identity_order():
    data, window, spec = small_fit_data(), outer_window(), study(("ES",))
    original = p.fit(data, "ES", window, p.POLICIES[0], spec, ready_at=window["activation_ns"])
    shuffled = rebound(data, decisions=tuple(reversed(data.decisions)), outcomes=tuple(reversed(data.outcomes)))
    reordered = p.fit(shuffled, "ES", window, p.POLICIES[0], spec, ready_at=window["activation_ns"])
    for field in ("beta", "intercept", "means", "scales", "fit_ids"):
        assert reordered[field] == original[field]
    # Deliberate future-information sentinel: outcome is known exactly at cutoff, so cannot fit.
    o = replace(data.outcomes[0], known_ns=window["cutoff_ns"], exit_nano=10_000*BILLION)
    late = rebound(data, outcomes=(o, *data.outcomes[1:]))
    model = p.fit(late, "ES", window, p.POLICIES[0], spec, ready_at=window["activation_ns"])
    assert o.sample_id not in model["fit_ids"]
    first = replace(data.decisions[0], decision_ns=window["activation_ns"],
                    feature_available_ns=window["activation_ns"]+1)
    assert p.predict(original, [first])[0]["status"] == "UNAVAILABLE_CAUSAL_INPUT"
    assert p.predict(original, [replace(first, features=None)])[0]["prediction_ticks"] is None
    late_model = p.fit(data, "ES", window, p.POLICIES[0], spec, ready_at=window["activation_ns"]+1)
    assert p.predict(late_model, [first])[0]["status"] == "ABSTAIN_FAILED_UPDATE"
    with pytest.raises(IntegrityError, match="identity"):
        p.predict({**original, "intercept": 99.}, [first])
    with pytest.raises(IntegrityError, match="permit"):
        replace(data, decisions=data.decisions[:-1]).validate()


def test_exact_chronology_dst_partial_cutoff_and_inner_history():
    data = generated()
    plans = p.schedule(data.sessions, "2022-10-01", "2023-02-01")
    outer, inner = plans[0]["outer"], plans[0]["inner"]
    assert outer["embargo_session"] == "2022-09-26"
    assert outer["cutoff_pacific"] == "2022-09-26T00:00:00-07:00"
    assert outer["activation_pacific"] == "2022-10-03T06:30:05-07:00"
    assert inner[-1]["stop_ns"] == outer["cutoff_ns"] and inner[-1]["partial"]
    assert inner[0]["lower_ns"] < outer["lower_ns"]
    assert len(inner) == 4 and plans[-1]["outer"]["partial"]
    assert p.five_years_before(p.ns(datetime(2020, 2, 29, tzinfo=p.PACIFIC))) \
        == p.ns(datetime(2015, 2, 28, tzinfo=p.PACIFIC))
    sessions = {s.day: s for s in data.sessions}
    assert (sessions["2022-03-13"].end_ns-sessions["2022-03-13"].start_ns)/BILLION == 23*3600
    assert (sessions["2022-11-06"].end_ns-sessions["2022-11-06"].start_ns)/BILLION == 25*3600
    # Exact final development timestamp, not a silently substituted midnight or July 14.
    extended = list(data.sessions)
    day = date(2023, 4, 1)
    while day <= date(2025, 7, 13):
        first = p.ns(datetime(day.year, day.month, day.day, 6, 30, 5, tzinfo=p.PACIFIC))
        extended.append(p.Session(str(day), p.local_midnight(day), p.local_midnight(day+timedelta(days=1)),
                                  first, "OPEN" if day.weekday() == 0 else "CLOSED"))
        day += timedelta(days=1)
    final = p.schedule(extended, "2025-07-01", "2025-07-13T22:00:00+00:00")[-1]["outer"]
    assert final["stop_ns"] == p.DEVELOPMENT_END and final["partial"]
    with pytest.raises(UnauthorizedOperation):
        p.schedule(extended, "2025-07-01", "2025-07-14")
    unknown = (replace(data.sessions[0], state="UNKNOWN"), *data.sessions[1:])
    with pytest.raises(IntegrityError, match="UNKNOWN"):
        p.schedule(unknown, "2022-10-01", "2023-01-01")


def test_missing_inputs_support_opportunities_and_qualification():
    assert p.prepare(None)["status"] == "MISSING_INPUTS"
    assert "risk" in p.prepare({})["missing"]
    with pytest.raises(UnauthorizedOperation):
        p.prepare(study(fixture_only=False))
    data, window = small_fit_data(), outer_window()
    too_little = p.fit(data, "ES", window, p.POLICIES[0], study(("ES",), minimum_train_sessions=3),
                      ready_at=window["activation_ns"])
    assert too_little["status"] == "INSUFFICIENT_SUPPORT"
    # Exact source envelope shortfall, independent of number of observed sessions.
    short = rebound(data, source_start_ns=p.local_midnight(date(2019, 1, 1)))
    assert p.fit(short, "ES", window, p.POLICIES[0], study(("ES",)),
                 ready_at=window["activation_ns"])["status"] == "INSUFFICIENT_HISTORY"
    full = generated()
    removed = next(d for d in full.decisions if d.session == "2022-10-03")
    damaged = rebound(full, decisions=tuple(d for d in full.decisions if d.sample_id != removed.sample_id),
                       outcomes=tuple(o for o in full.outcomes if o.sample_id != removed.sample_id))
    assert p.run_pilot(damaged, study(), "2022-10-01", "2023-01-01")["status"] \
        == "MISSING_OR_DUPLICATE_OPPORTUNITIES"
    assert p.prepare(study())["qualification"] == "UNAVAILABLE_NO_ADOPTED_METHOD"
    assert not any(p.protocol()["authority"].values())


def test_specialist_and_broad_route_interfaces():
    broad = study()["route"]
    assert p.tier_disposition(broad, 0, "REQUIRED_FAILURE") == "STOP_REQUIRED_CLAIM"
    assert p.tier_disposition(broad, 0, "INCONCLUSIVE", expansion_authorized=True) == "STOP_INSUFFICIENT_EVIDENCE"
    allowed = {**broad, "allow_inconclusive_expansion": True}
    assert p.tier_disposition(allowed, 0, "INCONCLUSIVE") == "AWAIT_EXACT_EXPANSION_AUTHORITY"
    assert p.tier_disposition(allowed, 0, "INCONCLUSIVE", expansion_authorized=True) \
        == "EXPLORATORY_CONTINUATION_ONLY"
    specialist = {**broad, "tiers": [1], "anchor": "CL"}
    p.validate_route(specialist, ["CL"])
    assert p.tier_disposition(specialist, 1, "DESCRIPTIVE_COMPLETE") == "TERMINAL_DECLARED_SCOPE_UNQUALIFIED"
    assert p.tier_disposition(broad, 0, "UNAVAILABLE_QUALIFICATION") == "UNAVAILABLE_QUALIFICATION"
    with pytest.raises(IntegrityError, match="qualification"):
        p.tier_disposition(broad, 0, "QUALIFIED")
    # This is a later-stage route interface test, not a 4/16/41-market campaign double.


def risk():
    return p.Risk(["ES", "NQ"], 1, 1, 10_000, 10_000, 100, 1)


def ref(at, price, contract=ES.contract):
    return {contract: p.Reference(contract, price, at, at)}


def sig(key, at, entry, exit_, prediction=4., market="ES", model="old"):
    return p.Signal(key, model, market, ES.contract if market == "ES" else NQ.contract,
                    at, entry, exit_, prediction)


def test_shared_risk_persistent_state_and_independent_money():
    book = p.Portfolio((ES, NQ), risk())
    t = p.ns(datetime(2022, 12, 31, 23, 59, 55, tzinfo=p.PACIFIC))
    book.step(t, {}, updates={"ES": "old", "NQ": "old"},
              signals=[sig("nq", t, t+1*BILLION, t+10*BILLION, market="NQ"), sig("es", t, t+1*BILLION, t+10*BILLION)])
    assert set(book.positions) == {"ES"}  # Reversed input order cannot defeat declared priority.
    assert book.events[-1]["event"] == "ABSTAIN_SHARED_RISK"
    book.step(t+1*BILLION, ref(t+1*BILLION, 100*BILLION))
    book.step(t+2*BILLION, ref(t+2*BILLION, 99_750_000_000), mark=True)
    assert book.equity_cents == -2600  # -1 tick*$12.50 - ($1 fee + 1 tick slippage).
    assert book.drawdown_cents == 2600
    book.step(t+6*BILLION, {}, updates={"ES": "new"},
              signals=[sig("occupied", t+6*BILLION, t+7*BILLION, t+20*BILLION, model="new")])
    assert book.positions["ES"]["signal"].model_id == "old"
    assert book.events[-1]["event"] == "OCCUPIED_FORECAST_ONLY"
    book.step(t+10*BILLION, ref(t+10*BILLION, 101*BILLION),
              signals=[sig("short", t+10*BILLION, t+11*BILLION, t+20*BILLION, prediction=-4., model="new")])
    assert book.cash_cents == 3650  # +4 ticks*1250 -1350, then new short reserves shared risk.
    book.step(t+11*BILLION, ref(t+11*BILLION, 100*BILLION))
    book.step(t+20*BILLION, ref(t+20*BILLION, 99_500_000_000))
    result = book.result()
    assert book.last_ns > p.ns(datetime(2023, 1, 1, tzinfo=p.PACIFIC))
    assert result["complete"]
    assert result["gross_cents"] == 7500 and result["cost_cents"] == 2700
    assert result["net_cents"] == 4800  # (5000-1350) + (2500-1350)
    exits = [v for v in result["events"] if v["event"] == "EXIT_REFERENCE"]
    assert [v["originating_model_id"] for v in exits] == ["old", "new"]
    assert result["drawdown_at_supplied_marks_cents"] == 2600


@pytest.mark.parametrize("missing", ["exit", "mark", "wrong_contract", "late_reference"])
def test_missing_execution_retains_position_and_blocks_new_entries(missing):
    book = p.Portfolio((ES, NQ), risk())
    book.step(1, {}, updates={"ES": "old"}, signals=[sig("first", 1, 2, 10)])
    book.step(2, ref(2, 100*BILLION))
    at = 3 if missing == "mark" else 10
    references = {} if missing in {"exit", "mark"} else (
        ref(at, 100*BILLION, NQ.contract) if missing == "wrong_contract"
        else {ES.contract: p.Reference(ES.contract, 100*BILLION, at, at+1)})
    book.step(at, references, mark=missing == "mark",
              signals=[sig("must-not-enter", at, at+1, at+2, market="NQ")])
    result = book.result()
    assert not result["complete"] and result["net_cents"] is None
    assert result["open_or_pending"] == 1 and book.positions["ES"]["signal"].forecast_id == "first"
    assert not any(v.get("forecast_id") == "must-not-enter" for v in book.events)
    assert result["unresolved"]


def test_failed_update_negative_prices_roll_units_and_threshold():
    book = p.Portfolio((ES, NQ), risk())
    book.step(1, {}, updates={"ES": "old"}, signals=[sig("first", 1, 2, 5)])
    book.step(2, ref(2, -BILLION))
    book.step(3, {}, updates={"ES": None}, signals=[sig("stale", 3, 4, 6)])
    assert book.events[-1]["event"] == "ABSTAIN_FAILED_OR_STALE_UPDATE"
    book.step(5, ref(5, 0))
    assert book.result()["net_cents"] == 3650
    assert ES.ticks(0) == 0 and ES.ticks(-BILLION) == -4
    with pytest.raises((IntegrityError, ResearchContractError, ValueError)):
        ES.ticks(1)
    with pytest.raises(IntegrityError, match="disagree"):
        replace(ES, tick_value_cents=500).validate()
    d = generated().decisions[0]
    o = p.Outcome(d.sample_id, d.contract, "NEXT_CONTRACT", 100*BILLION, 200*BILLION, d.exit_ns)
    assert p.target(d, o, ES) is None  # No invented roll-jump return.
    book.step(7, {}, updates={"ES": "new"},
              signals=[sig("flat", 7, 8, 9, prediction=1.08, model="new")])
    assert book.events[-1]["event"] == "ABSTAIN_COST_THRESHOLD"


def test_validation_maturity_missing_scores_and_expired_model():
    data, window, spec = small_fit_data(), outer_window(), study(("ES",))
    model = p.fit(data, "ES", window, p.POLICIES[0], spec, ready_at=window["activation_ns"])
    d = replace(data.decisions[0], decision_ns=window["stop_ns"], feature_available_ns=0)
    with pytest.raises(IntegrityError, match="scheduled development interval"):
        p.predict(model, [d])
    # Invalid protected-role declaration is rejected with synthetic data only.
    with pytest.raises(ResearchContractError, match="cannot authorize"):
        replace(data, permit=replace(data.permit, real_history_authorized=True)).validate()
    future = replace(d, decision_ns=window["activation_ns"])
    record = p.predict(model, [future])
    o = replace(data.outcomes[0], known_ns=window["stop_ns"])
    score = p.evaluate(record, [future], [o], (ES,), as_of=window["stop_ns"]-1,
                       aggregation=p.AGGREGATION)
    assert score["status"] == "INCOMPLETE_OUTCOMES" and score["missing_targets"] == 1
    assert score["scored_targets"] == 0
    # One inner outcome unavailable at selection makes selection explicitly incomplete.
    full = generated()
    plans = p.schedule(full.sessions, "2022-10-01", "2023-01-01")
    selected = next(d for d in full.decisions if d.market == "ES" and d.session == "2021-10-04")
    outcomes = tuple(replace(o, known_ns=plans[0]["outer"]["cutoff_ns"])
                     if o.sample_id == selected.sample_id else o for o in full.outcomes)
    result = p.run_pilot(rebound(full, outcomes=outcomes), study(), "2022-10-01", "2023-01-01")
    assert result["status"] == "INNER_EVIDENCE_INCOMPLETE"
    assert result["score"]["missing_targets"] == 1


def test_future_outcomes_cannot_change_actual_fit_or_forecast_values():
    data, window, spec = generated(), outer_window(), study()
    model = p.fit(data, "ES", window, p.POLICIES[0], spec, ready_at=window["activation_ns"])
    decisions = {d.sample_id: d for d in data.decisions}
    changed = tuple(replace(o, exit_nano=9999*BILLION)
                    if decisions[o.sample_id].decision_ns >= window["activation_ns"] else o
                    for o in data.outcomes)
    future = rebound(data, outcomes=changed)
    other = p.fit(future, "ES", window, p.POLICIES[0], spec, ready_at=window["activation_ns"])
    for field in ("beta", "intercept", "means", "scales", "fit_ids", "equal_session_training_mean"):
        assert model[field] == other[field]
    row = next(d for d in data.decisions if d.market == "ES" and d.decision_ns == window["activation_ns"])
    assert p.predict(model, [row])[0]["prediction_ticks"] == p.predict(other, [row])[0]["prediction_ticks"]
    # Exact source-release identities change; unchanged numeric forecasts are the causal claim.
    assert model["input_id"] != other["input_id"]


def test_same_timestamp_ordering_and_actual_contract_units():
    book = p.Portfolio((ES, NQ), risk())
    with pytest.raises(IntegrityError, match="clock"):
        book.step(1, {}, updates={"ES": "old"}, signals=[sig("ambiguous", 1, 2, 2)])
    # A different contract with different valid units is a different binding, never a roll jump.
    revised = p.Instrument("SYNTHETIC_NEXT_ES", 500_000_000, 2500, 50)
    assert revised.ticks(BILLION) == 2
    assert ES.ticks(BILLION) == 4
    with pytest.raises(IntegrityError, match="unique"):
        p.Portfolio((ES, ES), risk())


# ES close integration fixtures below are generated study values, never research settings.
# M1 Portfolio tests above remain later-stage accounting doubles; no close policy is implemented.
import builtins
import io
import json
from pathlib import Path


@pytest.fixture(autouse=True)
def no_market_or_history_payload_reads(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    def guarded(original):
        def checked(file, *args, **kwargs):
            if isinstance(file, (str, bytes, Path)):
                path = Path(file.decode() if isinstance(file, bytes) else file).resolve()
                if path.is_relative_to(root / "data") or (
                        path.is_relative_to(root / "state")
                        and not path.is_relative_to(root / "state/unpublished_evidence/es_close_anchored_continuation_integration_v1")):
                    raise PermissionError("SYNTHETIC_TEST_FORBIDS_SOURCE_OR_HISTORY_READ")
            return original(file, *args, **kwargs)
        return checked
    monkeypatch.setattr(builtins, "open", guarded(builtins.open))
    monkeypatch.setattr(io, "open", guarded(io.open))


def close_study(**changes):
    value = study(("ES",))
    value.pop("ridge_penalty")
    value.pop("clock_offsets_seconds")
    value.pop("risk")
    history = dict(value["history"])
    history.pop("study_summary_id")
    history.update(scientific_id="SYNTHETIC_ES_CLOSE_FIXTURE_NOT_RESEARCH",
                   question="Generated ES close integration mechanics",
                   declaration={"path": "SYNTHETIC_IN_MEMORY_CLOSE_DECLARATION",
                                "sha256": p.CLOSE_DECLARATION})
    value.update(history=bind(history, "study_summary_id"), estimator=p.CLOSE_ESTIMATOR,
                 protocol_id=p.close_protocol()["protocol_id"], support_scope="SYNTHETIC_ONLY",
                 route={**value["route"], "tiers": [0]})
    return {**value, **changes}


@lru_cache
def close_raw():
    cash, choices, bars = [], [], []
    day, end = date(2015, 1, 1), date(2023, 4, 1)
    known = p.local_midnight(date(2014, 1, 1))
    sequence = 0
    while day < end:
        full = day.weekday() < 5
        cash.append(p.CashDay(str(day), "FULL" if full else "CLOSED", known, "SYNTHETIC_CALENDAR"))
        if full:
            choices.append(p.CloseContract(str(day), ES.contract, 1, 1, "SYNTHETIC_SPEC",
                                           known, known, "SYNTHETIC_SELECTION",
                                           known, p.local_midnight(date(2030, 1, 1))))
            x = sequence % 3 - 1
            y = 4*x
            for hour, minute, opening, closing in (
                    (15, 28, 100*BILLION+99*ES.tick_nano, 100*BILLION+x*ES.tick_nano),
                    (15, 30, 100*BILLION, 100*BILLION+99*ES.tick_nano),
                    (15, 59, 100*BILLION+99*ES.tick_nano, 100*BILLION),
                    (16, 0, 100*BILLION+y*ES.tick_nano, 100*BILLION+99*ES.tick_nano)):
                at = p.close_time(str(day), hour, minute)
                bars.append(p.CloseBar(ES.contract, 1, 1, "SYNTHETIC_SPEC", at, at+60*BILLION, at,
                                       at+65*BILLION, at+65*BILLION, known, opening, closing,
                                       "SYNTHETIC_SOURCE", f"SYNTHETIC:{day}:{hour}:{minute}"))
            sequence += 1
        day += timedelta(days=1)
    scope = dict(source_start_ns=p.local_midnight(date(2015, 1, 1)), source_end_ns=p.local_midnight(end),
                 source_identity="SYNTHETIC_SOURCE",
                 vintage_assumption="SYNTHETIC_FIXED_VINTAGE_NO_HISTORICAL_DELIVERY_CLAIM")
    return tuple(cash), tuple(choices), tuple(bars), (ES,), scope


def close_data(cash=None, choices=None, bars=None, instruments=None, scope=None):
    original = close_raw()
    values = [a if a is not None else b for a, b in zip((cash, choices, bars, instruments, scope), original)]
    digest = p.close_source_digest(*values[:4], **values[4])
    permit = SyntheticOnlyPermit("MECHANICS_ONLY", "SYNTHETIC", "es-close-generated-v1", 0, digest)
    return p.close_inputs(*values[:4], **values[4], permit=permit)


@lru_cache
def close_generated():
    return close_data()


def close_window():
    return p.schedule(close_generated().sessions, "2022-10-01", "2023-01-01")[0]["outer"]


def close_fit_data(y_values=(1, 3, 5)):
    # Three distinct daily observations; all other dates remain explicit missing-input dispositions.
    data = close_generated()
    chosen = [d for d in data.decisions if "2021-01-04" <= d.session <= "2021-01-06"]
    ds = tuple(replace(d, features=(float(x),)) for d, x in zip(chosen[:len(y_values)], (-1,0,1)))
    os = tuple(p.Outcome(d.sample_id, d.contract, d.contract, 100*BILLION,
                        100*BILLION+y*ES.tick_nano, d.exit_ns+65*BILLION)
               for d, y in zip(ds, y_values))
    ids = {d.sample_id for d in ds}
    ops = tuple({**o, "has_decision": o["sample_id"] in ids,
                 "contract": ES.contract if o["sample_id"] in ids else None,
                 "input_status": "ELIGIBLE_INPUT" if o["sample_id"] in ids else "SYNTHETIC_MISSING_INPUT"}
                for o in data.opportunities)
    return rebound(data, decisions=ds, outcomes=os, opportunities=ops)


def test_close_source_read_sentinel_and_versioned_preparation():
    with pytest.raises(PermissionError, match="FORBIDS_SOURCE"):
        (Path(__file__).resolve().parents[1] / "data/SENTINEL_NO_READ.parquet").read_bytes()
    spec = close_study()
    ready = p.prepare(spec)
    assert ready["status"] == "READY_SYNTHETIC_ONLY"
    assert "risk" not in ready["missing"] and ready["economic"] == "UNAVAILABLE_UNBOUND_POLICY"
    assert p.protocol()["protocol_id"] == "2ada0af2267143107ec5734076614c2ba19896a0011a5a6ce6c373c437e017ed"
    assert p.close_prepare({})["status"] == "MISSING_INPUTS"
    assert not any(p.close_protocol()["authority"].values())
    for changed in (close_study(fixture_only=False), close_study(support_scope="REAL_POLICY")):
        with pytest.raises(UnauthorizedOperation):
            p.prepare(changed)
    with pytest.raises(IntegrityError):
        p.prepare(close_study(ridge_penalty=0))
    with pytest.raises(IntegrityError):
        p.prepare(close_study(markets=["ES", "NQ"]))
    raw = close_raw()
    forged = SyntheticOnlyPermit("MECHANICS_ONLY", "SYNTHETIC", "bad", 0, "0"*64)
    with pytest.raises(IntegrityError, match="exact supplied"):
        p.close_inputs(*raw[:4], **raw[4], permit=forged)
    with pytest.raises(ResearchContractError, match="cannot authorize"):
        p.close_inputs(*raw[:4], **raw[4], permit=replace(forged, real_history_authorized=True))


def close_future(d, day="2022-10-03"):
    at=p.close_time(day,15,29,5)
    return replace(d,session=day,decision_ns=at,feature_available_ns=at,
                   entry_ns=p.close_time(day,15,30),exit_ns=p.close_time(day,16,0))


def test_close_actual_wls_kernel_and_dispatch_independent_arithmetic():
    data, window = close_fit_data(), close_window()
    model = p.fit(data, "ES", window, p.POLICIES[0], close_study(), ready_at=window["activation_ns"])
    assert model["intercept"] == pytest.approx(3)
    assert model["means"] == pytest.approx([0])
    assert model["scales"] == pytest.approx([sqrt(2/3)])
    assert model["beta"] == pytest.approx([2*sqrt(2/3)])
    assert model["raw_slope"] == pytest.approx(2)
    ds = [close_future(d, day) for d,day in zip(data.decisions, ("2022-10-03","2022-10-04","2022-10-05"))]
    records = p.predict(model, ds)
    assert [r["prediction_ticks"] for r in records] == pytest.approx([1,3,5])
    assert sum((y-r["prediction_ticks"])**2 for y,r in zip((1,3,5),records))/3 == pytest.approx(0, abs=1e-24)
    # Same actual kernel called by close_fit; this is not a separate demonstration engine.
    weighted = p._close_wls([0,1,2], [1,2,5], [1/2,1/3,1/6])
    assert weighted["means"] == pytest.approx([2/3])
    assert weighted["scales"] == pytest.approx([sqrt(5)/3])
    assert weighted["intercept"] == pytest.approx(2)
    assert weighted["beta"] == pytest.approx([3/sqrt(5)])
    fitted = [(x-weighted["means"][0])/weighted["scales"][0]*weighted["beta"][0]+weighted["intercept"]
              for x in (0,1,2)]
    assert fitted == pytest.approx([4/5,13/5,22/5])
    assert sum(w*(y-f)**2 for w,y,f in zip((1/2,1/3,1/6),(1,2,5),fitted)) == pytest.approx(1/5)
    with pytest.raises(IntegrityError):
        p._close_wls([0,1], [0,1], [1,1])


def test_close_both_training_weights_and_independent_baseline():
    data, window = close_fit_data((1,2,5)), close_window()
    dates = [d.session for d in data.decisions]
    cutoff_day = date(2022, 9, 30)  # Friday before Monday activation; excluded full session.
    for policy in p.POLICIES:
        model = p.fit(data, "ES", window, policy, close_study(), ready_at=window["activation_ns"])
        raw = [1 if policy == p.POLICIES[0] else 2**(-(cutoff_day-date.fromisoformat(d)).days/365.25) for d in dates]
        w = [a/sum(raw) for a in raw]
        x, y = (-1,0,1), (1,2,5)
        mx, my = sum(a*b for a,b in zip(w,x)), sum(a*b for a,b in zip(w,y))
        slope = sum(a*(b-mx)*(c-my) for a,b,c in zip(w,x,y))/sum(a*(b-mx)**2 for a,b in zip(w,x))
        assert model["raw_slope"] == pytest.approx(slope)
        assert model["intercept"] == pytest.approx(my)
        assert model["equal_session_training_mean"] == pytest.approx(8/3)
        assert model["session_weight_max"] == pytest.approx(max(w))
        assert model["session_weight_inverse_hhi"] == pytest.approx(1/sum(a*a for a in w))
        assert p.row_weights(dates, window["cutoff_ns"], policy) == pytest.approx(w)


def test_close_learnable_actual_nested_path_and_persistent_roundtrip():
    result = p.run_pilot(close_generated(), close_study(), "2022-10-01", "2023-04-01")
    assert result["status"] == "COMPLETE_SYNTHETIC_MECHANICS"
    assert result["fit_count"] == 18
    assert sum(f["role"] == "OUTER" for f in result["fits"]) == 2
    assert {f["policy"] for f in result["fits"]} == set(p.POLICIES)
    score = result["outer_metrics"]
    assert score["mean_losses"]["prediction_ticks"] < 1e-24
    assert score["mean_delta_mse"] > 0
    assert score["directional"]["recall"]["1"] == 1 and score["directional"]["recall"]["-1"] == 1
    # Exact sign of a near-zero floating forecast is retained; no invented zero deadband.
    assert score["directional"]["balanced_accuracy"] is not None
    assert score["missing_outcomes"] == 0
    assert score["expected_opportunities"] == 182  # Civil-date ledger includes closed days.
    assert score["forecasts"] < score["expected_opportunities"]
    assert result["recent_metrics"]["scorable_sessions"] > 0
    assert result["recent_development_claim"].startswith("DEVELOPMENT_EVIDENCE_ONLY")
    assert result["qualifications"]["positive_continuation"] == "UNAVAILABLE_NO_ADOPTED_METHOD"
    saved = json.loads(json.dumps(result["persistent_state"]))
    state = p.CloseState.restore(saved, close_study())
    assert state.snapshot() == saved
    op = next(o for o in close_generated().opportunities if o["session"] == "2023-03-31")
    assert state.forecasts([op], close_generated().decisions)[0]["status"] == "FORECAST"
    changed = {**saved, "study_id": "bad"}
    with pytest.raises(IntegrityError):
        p.CloseState.restore(changed, close_study())


def test_close_contrary_and_no_information_cannot_qualify_or_rescue():
    window = close_window()
    for y, expected_slope in (((5,3,1), -2), ((3,3,3), 0), ((0,0,0), 0)):
        data = close_fit_data(y)
        model = p.fit(data, "ES", window, p.POLICIES[0], close_study(), ready_at=window["activation_ns"])
        assert model["raw_slope"] == pytest.approx(expected_slope)
        d = close_future(data.decisions[0])
        r = p.predict(model, [d])[0]
        assert r["continuation"] == "NONPOSITIVE_NO_REVERSAL_RESCUE"
    same = close_fit_data()
    ds = tuple(replace(d, features=(0.,)) for d in same.decisions)
    invalid = p.fit(rebound(same, decisions=ds), "ES", window, p.POLICIES[0], close_study(),
                    ready_at=window["activation_ns"])
    assert invalid["status"] == "UNAVAILABLE_ZERO_VARIANCE"


def test_close_exact_bars_previous_short_and_timezone():
    data = close_generated()
    d = next(d for d in data.decisions if d.session == "2022-10-03")
    op = next(o for o in data.opportunities if o["session"] == d.session)
    o = next(o for o in data.outcomes if o.sample_id == d.sample_id)
    assert d.decision_ns == p.ns(datetime(2022,10,3,19,29,5,tzinfo=p.UTC))
    assert d.entry_ns == p.ns(datetime(2022,10,3,19,30,tzinfo=p.UTC))
    assert d.exit_ns == p.ns(datetime(2022,10,3,20,0,tzinfo=p.UTC))
    assert o.known_ns == d.exit_ns+65*BILLION
    assert p.target(d,o,ES) == 4*d.features[0]  # Exact opens, not the deliberately different closes.
    assert op["previous_cash_session"] == "2022-09-30"
    assert p.close_time("2022-03-11",15,29,5) == p.ns(datetime(2022,3,11,20,29,5,tzinfo=p.UTC))
    assert p.close_time("2022-03-14",15,29,5) == p.ns(datetime(2022,3,14,19,29,5,tzinfo=p.UTC))
    cash = tuple(replace(c,state="SHORT") if c.day=="2022-09-30" else c for c in close_raw()[0])
    short = close_data(cash=cash)
    op = next(o for o in short.opportunities if o["session"]=="2022-10-03")
    assert op["input_status"]=="PREVIOUS_CASH_SESSION_SHORT"
    assert op["previous_cash_session"]=="2022-09-30"
    cash = tuple(replace(c,state="SHORT") if c.day=="2022-10-03" else c for c in close_raw()[0])
    short = close_data(cash=cash)
    plan = p.schedule(short.sessions,"2022-10-01","2023-01-01")[0]
    assert plan["outer"]["activation_ns"]==d.decision_ns  # No delay to a favorable eligible date.
    assert next(o for o in short.opportunities if o["session"]=="2022-10-03")["input_status"]=="CALENDAR_SHORT"


@pytest.mark.parametrize("change", ["late_input","late_definition","late_selection","wrong_contract",
                                   "wrong_minute","duplicate_exit","missing_exit","invalid_exit","negative_prices"])
def test_close_endpoint_failures_do_not_select_on_future_exit(change):
    cash, choices, original, units, scope = close_raw()
    signal = p.close_time("2022-10-03",15,28)
    exit_ = p.close_time("2022-10-03",16,0)
    bars = list(original)
    if change=="late_selection":
        choices=tuple(replace(c,selected_available_ns=signal+65*BILLION+1) if c.day=="2022-10-03" else c for c in choices)
    else:
        index=next(i for i,b in enumerate(bars) if b.start_ns==(exit_ if "exit" in change else signal))
        b=bars[index]
        if change=="late_input": bars[index]=replace(b,available_ns=b.available_ns+1,eligible_ns=b.eligible_ns+1)
        elif change=="late_definition": bars[index]=replace(b,definition_available_ns=signal+65*BILLION+1)
        elif change=="wrong_contract": bars[index]=replace(b,contract="SYNTHETIC_OTHER")
        elif change=="wrong_minute": bars[index]=replace(b,start_ns=b.start_ns+BILLION)
        elif change=="duplicate_exit": bars.append(b)
        elif change=="missing_exit": bars.pop(index)
        elif change=="invalid_exit": bars[index]=replace(b,open_nano=1)
        elif change=="negative_prices":
            bars=[replace(b,open_nano=b.open_nano-101*BILLION,close_nano=b.close_nano-101*BILLION) for b in bars]
    data=close_data(choices=choices,bars=tuple(bars))
    op=next(o for o in data.opportunities if o["session"]=="2022-10-03")
    if change in {"duplicate_exit","missing_exit","invalid_exit","negative_prices"}:
        assert op["input_status"]=="ELIGIBLE_INPUT"
        d=next(d for d in data.decisions if d.session=="2022-10-03")
        assert d.features==next(v.features for v in close_generated().decisions if v.session=="2022-10-03")
        outcome=next(o for o in data.outcomes if o.sample_id==d.sample_id)
        if change=="negative_prices": assert p.target(d,outcome,ES)==4*d.features[0]
        else: assert p.target(d,outcome,ES) is None
    else: assert op["input_status"]!="ELIGIBLE_INPUT"


def test_close_calendar_unknown_holiday_missing_choice_and_units():
    cash,choices,bars,units,scope=close_raw()
    changed=tuple(replace(c,state="UNKNOWN",known_ns=None) if c.day=="2022-09-30" else c for c in cash)
    data=close_data(cash=changed)
    assert p.run_pilot(data,close_study(),"2022-10-01","2023-01-01")["status"]=="UNAVAILABLE_CALENDAR"
    holiday=close_data(cash=tuple(replace(c,state="CLOSED") if c.day=="2022-09-30" else c for c in cash))
    assert next(o for o in holiday.opportunities if o["session"]=="2022-10-03")["previous_cash_session"]=="2022-09-29"
    missing=close_data(choices=tuple(c for c in choices if c.day!="2022-10-03"))
    assert next(o for o in missing.opportunities if o["session"]=="2022-10-03")["input_status"]=="UNAVAILABLE_CAUSAL_CONTRACT"
    for units in ((replace(ES,tick_nano=0),), (replace(ES,multiplier=0),)):
        with pytest.raises(IntegrityError): close_data(instruments=units)
    changed=tuple(replace(c,known_ns=p.close_time(c.day,15,29,5)+1) if c.day=="2022-10-03" else c for c in cash)
    assert next(o for o in close_data(cash=changed).opportunities if o["session"]=="2022-10-03")["input_status"]=="UNKNOWN_CALENDAR"


def test_close_training_maturity_order_and_future_sentinel():
    data,window=close_fit_data(),close_window()
    original=p.fit(data,"ES",window,p.POLICIES[0],close_study(),ready_at=window["activation_ns"])
    other=p.fit(rebound(data,decisions=tuple(reversed(data.decisions)),outcomes=tuple(reversed(data.outcomes))),
                "ES",window,p.POLICIES[0],close_study(),ready_at=window["activation_ns"])
    assert other==original
    late=rebound(data,outcomes=(replace(data.outcomes[0],known_ns=window["cutoff_ns"],exit_nano=9999*BILLION),*data.outcomes[1:]))
    model=p.fit(late,"ES",window,p.POLICIES[0],close_study(),ready_at=window["activation_ns"])
    assert data.outcomes[0].sample_id not in model["fit_ids"]
    assert model["excluded"]["IMMATURE_OR_UNKNOWN_LABEL"]==1
    assert model["equal_session_training_mean"]==4
    full=close_generated()
    model=p.fit(full,"ES",window,p.POLICIES[0],close_study(),ready_at=window["activation_ns"])
    changed=tuple(replace(o,exit_nano=9999*BILLION) if o.known_ns>=window["activation_ns"] else o for o in full.outcomes)
    after=p.fit(rebound(full,outcomes=changed),"ES",window,p.POLICIES[0],close_study(),ready_at=window["activation_ns"])
    assert after==model  # Training identity excludes future outcomes, as well as numerical fits.
    row=next(d for d in full.decisions if d.session=="2022-10-03")
    assert p.predict(model,[replace(row,feature_available_ns=row.decision_ns+1)])[0]["status"]=="UNAVAILABLE_CAUSAL_INPUT"
    assert model["latest_label_ns"]<model["cutoff_ns"]


def test_close_chronology_support_and_failed_update_persistence():
    data,window=close_generated(),close_window()
    plan=p.schedule(data.sessions,"2022-10-01","2023-02-01")
    assert window["cutoff_ns"]==p.local_midnight(date(2022,9,30))
    assert window["lower_ns"]==p.local_midnight(date(2017,9,30))
    assert plan[0]["inner"][0]["lower_ns"]==p.local_midnight(date(2016,9,30))
    assert plan[0]["inner"][-1]["stop_ns"]==window["cutoff_ns"]
    assert plan[0]["inner"][-1]["partial"] and plan[-1]["outer"]["partial"]
    model=p.fit(data,"ES",window,p.POLICIES[0],close_study(),ready_at=window["activation_ns"])
    state=p.CloseState(close_study());state.update(window,model,{"policy":p.POLICIES[0]})
    snapshot=json.loads(json.dumps(state.snapshot()))
    state=p.CloseState.restore(snapshot,close_study())
    second=plan[1]["outer"]
    state.update(second,None,{"failure":"MISSING_UPDATE"})
    op=next(o for o in data.opportunities if o["session"]=="2023-01-02")
    assert state.forecasts([op],data.decisions)[0]["status"]=="ABSTAIN_FAILED_UPDATE"
    first=p.CloseState.restore(snapshot,close_study())
    assert first.forecasts([op],data.decisions)[0]["status"]=="ABSTAIN_EXPIRED_OR_INACTIVE_MODEL"
    late=p.run_close_pilot(data,close_study(),"2022-10-01","2023-01-01",
                          ready_at={window["activation_ns"]:window["activation_ns"]+1})
    assert late["status"]=="INCOMPLETE_EVIDENCE"
    assert late["outer_metrics"]["forecasts"]==0
    assert late["persistent_state"]["updates"][-1]["status"]=="FAILED_UPDATE_NO_NEW_ENTRIES"
    assert p.fit(close_fit_data(),"ES",window,p.POLICIES[0],close_study(minimum_train_sessions=4),
                 ready_at=window["activation_ns"])["status"]=="INSUFFICIENT_SUPPORT"


def test_close_equal_session_scoring_baselines_ties_and_missing_outcomes():
    data=close_generated()
    days=["2022-12-30","2023-01-02","2023-01-03","2023-01-04"]
    ds=[d for day in days for d in data.decisions if d.session==day]
    replacements={d.sample_id:p.Outcome(d.sample_id,d.contract,d.contract,0,y*ES.tick_nano,d.exit_ns+65*BILLION)
                  for d,y in zip(ds,(3,1,1,1))}
    data=rebound(data,outcomes=tuple(replacements.get(o.sample_id,o) for o in data.outcomes))
    # Bound forecast inputs isolate the actual scoring owner; no alternative fitted engine.
    rs=[bind({"sample_id":d.sample_id,"session":d.session,"decision_ns":d.decision_ns,"model_id":"SYNTHETIC",
              "status":"FORECAST","prediction_ticks":0.,"mean_ticks":1.,"zero_ticks":0.,
              "weighted_intercept_ticks":2.,"modal_sign":1,"decision_id":sha256_json(p.asdict(d))},"forecast_id") for d in ds]
    score=p.close_score(rs,data,as_of=data.source_end_ns-1,aggregation=p.AGGREGATION)
    assert score["mean_losses"]["prediction_ticks"]==3  # Not equal-quarter 5.
    assert score["mean_losses"]["mean_ticks"]==1
    assert score["mean_delta_mse"]==-2 and score["relative_improvement"]==-2
    assert score["directional"]["balanced_accuracy"] is None
    assert score["directional"]["raw_accuracy"]==0 and score["modal_baseline"]["raw_accuracy"]==1
    assert score["paired_accuracy_benefit"]==-1
    candidates=[{"policy":q,"score":score,"failed_fits":[]} for q in p.POLICIES]
    assert p.close_select(candidates)==p.POLICIES[0]
    mismatched=[candidates[0],{**candidates[1],"score":{**score,"scorable_ids":[]}}]
    assert p.close_select(mismatched) is None
    assert p.close_select([candidates[0],{**candidates[1],"failed_fits":[{"status":"UNAVAILABLE"}]}]) is None
    with pytest.raises(IntegrityError): p.close_score(rs,data,as_of=data.source_end_ns,aggregation=p.POLICIES[1])
    with pytest.raises(IntegrityError): p.close_score(rs+rs[:1],data,as_of=data.source_end_ns,aggregation=p.AGGREGATION)
    missing=rebound(data,outcomes=tuple(replace(o,exit_nano=None) if o.sample_id==ds[0].sample_id else o for o in data.outcomes))
    partial=p.close_score(rs,missing,as_of=data.source_end_ns,aggregation=p.AGGREGATION)
    assert partial["expected_opportunities"]==4 and partial["forecasts"]==4
    assert partial["missing_outcomes"]==1 and partial["scorable_sessions"]==3
    model=p.fit(close_fit_data((-1,0,1)),"ES",close_window(),p.POLICIES[1],close_study(),ready_at=close_window()["activation_ns"])
    assert model["modal_sign"]==0
    model=p.fit(close_fit_data((-1,1,1)),"ES",close_window(),p.POLICIES[0],close_study(),ready_at=close_window()["activation_ns"])
    assert model["modal_sign"]==1



def test_close_missing_disposition_roundtrip_and_actual_failed_inner_update():
    data, spec = close_generated(), close_study()
    plan = p.schedule(data.sessions, "2022-10-01", "2023-01-01")[0]
    early = plan["inner"][0]["activation_ns"]
    result = p.run_close_pilot(data, spec, "2022-10-01", "2023-01-01",
                              ready_at={early: early+1})
    assert result["fit_count"] == 8  # Both candidate failures retained; no outer survivor selection.
    assert result["selections"][0]["winner"] is None
    assert all(c["failed_fits"][0]["status"] == "LATE_UPDATE" for c in result["selections"][0]["candidates"])
    assert result["outer_metrics"]["forecasts"] == 0
    state = p.CloseState.restore(json.loads(json.dumps(result["persistent_state"])), spec)
    assert any(d["status"] == "ABSTAIN_FAILED_UPDATE" for d in state.dispositions.values())
    op = next(o for o in data.opportunities if o["session"] == "2022-10-03")
    unavailable = {**op, "input_status": "UNAVAILABLE_SIGNAL:MISSING_EXACT_BAR"}
    empty = p.CloseState(spec)
    assert empty.forecasts([unavailable], data.decisions)[0]["prediction_ticks"] is None
    saved = json.loads(json.dumps(empty.snapshot()))
    restored = p.CloseState.restore(saved, spec)
    assert restored.dispositions[op["sample_id"]]["status"] == unavailable["input_status"]
    assert restored.snapshot() == saved
    with pytest.raises(IntegrityError, match="rewritten"):
        restored.forecasts([{**unavailable, "input_status": "UNKNOWN_CALENDAR"}], data.decisions)


def test_close_zero_loss_denominator_modal_ties_and_exact_history_floor():
    window = close_window()
    for y, modal in (((0,1), 0), ((-1,1), 1)):
        model = p.fit(close_fit_data(y), "ES", window, p.POLICIES[0], close_study(),
                      ready_at=window["activation_ns"])
        assert model["modal_sign"] == modal
    tiny = close_fit_data()
    short = rebound(tiny, source_start_ns=p.local_midnight(date(2019,1,1)))
    assert p.fit(short, "ES", window, p.POLICIES[0], close_study(),
                 ready_at=window["activation_ns"])["status"] == "INSUFFICIENT_HISTORY"
    data = close_generated()
    d = next(d for d in data.decisions if d.session == "2022-10-03")
    zero = rebound(data, outcomes=tuple(replace(o,entry_nano=0,exit_nano=0) if o.sample_id==d.sample_id else o
                                       for o in data.outcomes))
    r = bind({"sample_id":d.sample_id,"session":d.session,"decision_ns":d.decision_ns,"model_id":"SYNTHETIC",
              "status":"FORECAST","prediction_ticks":0.,"mean_ticks":0.,"zero_ticks":0.,
              "weighted_intercept_ticks":0.,"modal_sign":0,"decision_id":sha256_json(p.asdict(d))},"forecast_id")
    score = p.close_score([r],zero,as_of=data.source_end_ns-1,aggregation=p.AGGREGATION)
    assert score["mean_delta_mse"] == 0 and score["relative_improvement"] is None
    assert score["directional"]["raw_accuracy"] == 1
    assert score["directional"]["balanced_accuracy"] is None
    # A different earlier close is not substituted for the missing exact prior reference.
    stamp = p.close_time("2022-09-30",15,59)
    bars = tuple(replace(b,contract="SYNTHETIC_OTHER") if b.start_ns==stamp else b for b in close_raw()[2])
    missing = close_data(bars=bars)
    op = next(o for o in missing.opportunities if o["session"]=="2022-10-03")
    assert op["input_status"].startswith("UNAVAILABLE_PREVIOUS_REFERENCE")
    assert next(d for d in missing.decisions if d.session=="2022-10-03").features is None



def test_close_run_scope_and_bar_identity_fail_before_forecasting():
    data = close_generated()
    assert p.run_close_pilot(data, close_study(), "2022-10-01", "2023-04-02")["status"] == "INSUFFICIENT_SOURCE_ENVELOPE"
    stamp = p.close_time("2022-10-03",15,28)
    bars = tuple(replace(b,instrument_id=True) if b.start_ns==stamp else b for b in close_raw()[2])
    invalid = close_data(bars=bars)
    op = next(o for o in invalid.opportunities if o["session"]=="2022-10-03")
    assert op["input_status"] == "UNAVAILABLE_SIGNAL:INVALID_BAR_IDENTITY_OR_INTERVAL"
