from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json

import numpy as np
import pytest

from futures_rebuild import profit_aligned_research as research
from futures_rebuild import profit_aligned_confirmation as confirmation
from futures_rebuild import profit_aligned_validation as validation
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.fixed_hour_terminal_evaluation import coverage_report
from tests.test_fixed_hour_terminal_successor import synthetic_dataset, scheduled_fixture

pytestmark = pytest.mark.current
MINUTE = research.MINUTE


def policy():
    return research.cost_policy(source_release_id="a" * 64, clock_policy_id="b" * 64,
        fee_evidence_id="c" * 64, maximum_quote_age_ns=2_000_000_000, verified_round_trip_fee_usd=5.)


def ns(day):
    return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp()) * 10**9


def opportunity(day="2018-01-01", number=0, market="ES", change=10):
    decision = ns(day) + (60 + number * 120) * MINUTE + 5_000_000_000
    entry = (decision // MINUTE + 1) * MINUTE
    row = dict(opportunity_id=f"{market}-{day}-{number}", market=market, trade_date=day,
        quartile=1 + number % 4,
        actual_contract=f"{market}-SYNTHETIC", decision_ns=decision, entry_ns=entry,
        exit_ns=entry + 60 * MINUTE, tick_nano=10**9, tick_value_nano_usd=12_500_000_000,
        feature_available_at_ns=decision, target_available_at_ns=entry + 61 * MINUTE)
    for key, time, bid in (("decision_quote", decision, 100), ("entry_quote", entry, 100),
                            ("exit_quote", row["exit_ns"], 100 + change)):
        row[key] = dict(interval_end_ns=time - 10**9, available_at_ns=time - 10**9,
            bid_nano=bid * 10**9, ask_nano=(bid + 1) * 10**9, bid_size=1, ask_size=1,
            market=market, actual_contract=row["actual_contract"], source_release_id="a" * 64,
            clock_policy_id="b" * 64, quality_valid=True)
    return row


def coverage(n):
    return coverage_report(scheduled=n, insufficient_history=0, valid_feature=n, invalid_outcome=0, quality_errors=0)


def profit_dataset(market="ES", n=1010):
    d = synthetic_dataset(market, n=n)
    rows = []
    for i, day in enumerate(d.trade_dates):
        row = opportunity(day, i % 2, market, int(d.terminal_ticks[i]))
        row["opportunity_id"] = d.sample_ids[i]
        rows.append(row)
    return research.ProfitDataset(d, tuple(rows), policy(), coverage(len(rows)))


def test_profit_and_quote_accounting_regression():
    p = policy()
    assert p["round_trip_fee_usd"] == 10
    row = opportunity()
    long = research.trade_record(100., row, p)
    assert long["quoted_gross_ticks"] == 9
    assert Decimal(long["net_usd"]["stress"]) == Decimal("52.5")
    short = research.trade_record(-100., opportunity(change=-10), p)
    assert short["net_usd"] == long["net_usd"]
    # Same economics at zero and negative prices, with no duplicate spread cost.
    for quote in (row["decision_quote"], row["entry_quote"], row["exit_quote"]):
        quote["bid_nano"] -= 110 * 10**9
        quote["ask_nano"] -= 110 * 10**9
    assert research.trade_record(100., row, p)["net_usd"] == long["net_usd"]
    rows = [opportunity(change=14)] + [opportunity("2018-01-02", i, change=4) for i in range(10)]
    trades = [research.trade_record(100., r, p) for r in rows]
    old_score = (float(trades[0]["net_usd"]["stress"]) + np.mean([float(t["net_usd"]["stress"]) for t in trades[1:]])) / 2
    ledger = research.daily_ledger(trades, expected_ids=[r["opportunity_id"] for r in rows], dates=["2018-01-01", "2018-01-02", "2018-01-03"])
    assert old_score > 0 and Decimal(ledger["total_usd"]["stress"]) < 0 and ledger["mean_daily_stress_usd"] < 0
    assert ledger["daily_usd"]["2018-01-03"]["stress"] == "0"
    assert sum(Decimal(q["net_usd"]["stress"]) for q in ledger["descriptive_quartiles"].values()) == Decimal(ledger["total_usd"]["stress"])
    assert ledger["descriptive_risk"]["stress"]["maximum_fixed_exit_drawdown_usd"] > 0
    assert Decimal(ledger["total_usd"]["stress"]) == sum(Decimal(t["net_usd"]["stress"]) for t in trades)
    assert research.daily_ledger([], expected_ids=[], dates=["2018-01-01"])["executed_trades"] == 0
    with pytest.raises(IntegrityError):
        research.daily_ledger(trades[:-1], expected_ids=[r["opportunity_id"] for r in rows], dates=["2018-01-01", "2018-01-02"])


def test_causal_books_fees_and_missing_exit():
    row, p = opportunity(), policy()
    assert research.decision(5.8, row, p)[0] == 0
    row["decision_quote"]["available_at_ns"] = row["decision_ns"] + 1
    assert research.trade_record(100., row, p)["executed_direction"] == 0
    row = opportunity()
    row["entry_quote"] = None
    assert research.trade_record(100., row, p)["net_usd"]["stress"] == "0"
    row = opportunity()
    row["exit_quote"] = None
    trade = research.trade_record(100., row, p)
    assert trade["executed_direction"] == 1 and trade["net_usd"] is None
    with pytest.raises(IntegrityError):
        research.daily_ledger([trade], expected_ids=[row["opportunity_id"]], dates=[row["trade_date"]])
    for field, value in (("actual_contract", "WRONG"), ("source_release_id", "d" * 64), ("clock_policy_id", "d" * 64)):
        row = opportunity(); row["entry_quote"][field] = value
        with pytest.raises(IntegrityError): research.trade_record(100., row, p)
    row = opportunity(); row["exit_quote"]["interval_end_ns"] -= 10**10
    assert research.trade_record(100., row, p)["net_usd"] is None
    row = opportunity(); row["entry_quote"]["ask_size"] = 0
    assert research.trade_record(100., row, p)["executed_direction"] == 0


@pytest.mark.parametrize("candidate_index", [0, 27])
def test_models_and_information_cutoff(candidate_index):
    dataset = profit_dataset(n=300)
    d = dataset.terminal_data
    c = research.predecessor.candidate_definitions()[candidate_index]
    model = research.fit_model(d.features, d.terminal_ticks, d.sample_ids, d.trade_dates, c, market=d.market)
    np.testing.assert_array_equal(research.predict(model, d.features), research.predict(json.loads(json.dumps(model)), d.features))
    assert model["market"] == "ES"
    changed = deepcopy(model); changed["market"] = "GC"
    with pytest.raises(IntegrityError): research.predict(changed, d.features)
    research.check_split_availability(dataset, np.arange(100), np.arange(102, 120))
    dataset.opportunities[99]["target_available_at_ns"] = dataset.opportunities[102]["decision_ns"]
    with pytest.raises(IntegrityError): research.check_split_availability(dataset, np.arange(100), np.arange(102, 120))
    with pytest.raises(IntegrityError): dataset.verify()


def test_bootstrap_futility_is_conservative_and_preserves_all_campaign_decisions():
    for scenario in range(len(validation.SCENARIOS)):
        values = validation.generated_pnl(scenario, 8)
        full = confirmation.raw_p_values(values, full_resampling=True)
        fast = confirmation.raw_p_values(values)
        assert np.all(fast >= full)
        for index in range(1, 8):
            np.testing.assert_array_equal(full * 4 <= confirmation.campaign_alpha(index), fast * 4 <= confirmation.campaign_alpha(index))
    assert np.all(confirmation.raw_p_values(np.ones((252, 4))) == 1)
    assert np.all(confirmation.raw_p_values(-np.ones((252, 4))) == 1)
    values = validation.generated_pnl(11, 0)
    result = confirmation.decision_from_daily(values, invalid_markets=("ES",))
    assert result["declared_claim_count"] == 4 and result["market_decisions"]["ES"]["adjusted_p"] == 1
    assert not confirmation.decision_from_daily(values, index=7)["bootstrap_resolution_sufficient"]
    assert confirmation.campaign_alpha(1) == .025
    assert sum(confirmation.campaign_alpha(k) for k in range(1, 100)) <= .05


def campaign_inputs():
    days = [(date(2027, 1, 1) + timedelta(days=i)).isoformat() for i in range(252)]
    models, schedule = {}, {}
    c = research.predecessor.candidate_definitions()[0]
    for market in research.MARKETS:
        models[market] = research.fit_model(np.ones((2, 29)), np.array([100., 100.]),
            ["fit1", "fit2"], ["2018-01-01", "2023-01-01"], c, market=market, fit_kind="development_refit")
        models[market] = research.bind({k: v for k, v in {**models[market],
            "frozen_at_ns": ns("2026-12-01"), "maximum_target_available_at_ns": ns("2023-01-02"),
            "development_stress_id": "e" * 64}.items() if k != "model_id"}, "model_id")
        schedule[market] = {day: [{k: opportunity(day, i, market)[k] for k in confirmation.SCHEDULE_FIELDS} for i in range(2)] for day in days}
    return dict(frozen_at_ns=ns("2026-12-01"), sessions=days, models=models, policy=policy(),
        calendar_id="d" * 64, schedule=schedule,
        session_bounds={day: {"start_ns": ns(day), "end_ns": ns(day) + 1440 * MINUTE} for day in days})


def test_frozen_campaign_complete_replay_and_custody(tmp_path):
    inputs = campaign_inputs()
    store = confirmation.SyntheticCampaignStore(tmp_path)
    campaign = store.reserve(**inputs)
    assert campaign["campaign_index"] == 1 and not any(campaign["authority"].values())
    with pytest.raises(IntegrityError): store.reserve(**inputs)
    with pytest.raises(IntegrityError): store.evaluate(campaign, as_of_ns=ns(inputs["sessions"][-1]))
    for day in inputs["sessions"]:
        for market in research.MARKETS:
            outcomes = []
            for i in range(2):
                row = opportunity(day, i, market, change=20 + int(day[-2:]) % 4)
                outcome = {k: row.pop(k) for k in ("entry_quote", "exit_quote")}
                outcome["opportunity_id"] = row["opportunity_id"]
                store.record_prediction(campaign, market=market, day=day, row=row,
                    features=np.ones(29), feature_available_at_ns=row["decision_ns"], recorded_at_ns=row["decision_ns"])
                outcomes.append(outcome)
            store.record_day(campaign, market=market, day=day, outcomes=outcomes, recorded_at_ns=inputs["session_bounds"][day]["end_ns"])
    result = store.evaluate(campaign, as_of_ns=inputs["session_bounds"][inputs["sessions"][-1]]["end_ns"])
    assert all(d["qualified"] and d["executed_trades"] == 504 for d in result["market_decisions"].values())
    assert result["qualification_ready"] is False and result["activation_ready"] is False
    path = tmp_path / "day-1-ES-2027-01-01.json"
    altered = json.loads(path.read_bytes()); altered["trades"][0]["net_usd"]["stress"] = "999999"
    altered["trades"][0] = research.bind({k: v for k, v in altered["trades"][0].items() if k != "trade_id"}, "trade_id")
    altered = research.bind({k: v for k, v in altered.items() if k != "day_id"}, "day_id")
    path.write_text(json.dumps(altered))
    with pytest.raises(IntegrityError, match="raw quotes"):
        store.evaluate(campaign, as_of_ns=inputs["session_bounds"][inputs["sessions"][-1]]["end_ns"])


def test_campaign_rejects_future_features_and_missing_outcomes(tmp_path):
    inputs = campaign_inputs()
    store = confirmation.SyntheticCampaignStore(tmp_path)
    campaign = store.reserve(**inputs)
    day = inputs["sessions"][0]
    row = opportunity(day)
    with pytest.raises(IntegrityError, match="outcomes"):
        store.record_prediction(campaign, market="ES", day=day, row=row, features=np.ones(29),
            feature_available_at_ns=row["decision_ns"], recorded_at_ns=row["decision_ns"])
    row.pop("entry_quote"); row.pop("exit_quote")
    with pytest.raises(IntegrityError):
        store.record_prediction(campaign, market="ES", day=day, row=row, features=np.ones(29),
            feature_available_at_ns=row["decision_ns"] + 1, recorded_at_ns=row["decision_ns"])
    result = store.evaluate(campaign, as_of_ns=inputs["session_bounds"][inputs["sessions"][-1]]["end_ns"])
    assert all(v["status"] == "INVALID_INCOMPLETE_EVIDENCE" and v["adjusted_p"] == 1 for v in result["market_decisions"].values())
    wrong = deepcopy(inputs); wrong["models"]["ES"] = wrong["models"]["GC"]
    with pytest.raises(IntegrityError): confirmation.freeze_campaign(**wrong, prior_campaigns=[])
    wrong = deepcopy(inputs)
    wrong["models"]["ES"]["maximum_target_available_at_ns"] = ns("2027-01-01")
    wrong["models"]["ES"] = research.bind({k: v for k, v in wrong["models"]["ES"].items() if k != "model_id"}, "model_id")
    with pytest.raises(IntegrityError, match="after labels"): confirmation.freeze_campaign(**wrong, prior_campaigns=[])


def test_stress_once_and_parameter_only_refit(tmp_path):
    c = research.predecessor.candidate_definitions()[0]
    model = research.fit_model(np.ones((2, 29)), np.full(2, 100.), ["a", "b"],
        ["2018-01-01", "2022-12-01"], c, market="ES")
    model = research.bind({k: v for k, v in {**model, "frozen_at_ns": ns("2022-12-03"),
        "maximum_target_available_at_ns": ns("2022-12-02")}.items() if k != "model_id"}, "model_id")
    row = opportunity("2023-01-01", change=20)
    args = dict(model=model, features=np.ones((1, 29)), rows=[row], policy=policy(),
        dates=[row["trade_date"]], expected_ids=[row["opportunity_id"]], coverage=coverage(1))
    stress = research.historical_stress_once(tmp_path, **args)
    assert stress["passed"]
    with pytest.raises(IntegrityError, match="already consumed"): research.historical_stress_once(tmp_path, **args)
    final = research.refit_for_freeze(stress, features=np.ones((3, 29)), target=np.full(3, 100.),
        ids=["a", "b", row["opportunity_id"]], dates=["2018-01-01", "2022-12-01", "2023-01-01"],
        target_available_at_ns=[ns("2018-01-02"), ns("2022-12-02"), row["target_available_at_ns"]], frozen_at_ns=ns("2026-12-01"))
    assert final["candidate"] == model["candidate"] and final["fit_kind"] == "development_refit"
    assert "score" not in final
    assert final["maximum_target_available_at_ns"] < final["frozen_at_ns"]
    dataset = profit_dataset(n=300)
    with pytest.raises(IntegrityError, match="training-label availability"):
        research.freeze_development_model(dataset, {}, frozen_at_ns=ns("2018-01-01"))
    bad = deepcopy(args)
    bad["model"]["frozen_at_ns"] = ns("2024-01-01")
    bad["model"] = research.bind({k: v for k, v in bad["model"].items() if k != "model_id"}, "model_id")
    with pytest.raises(IntegrityError, match="scope"):
        research.historical_stress_once(tmp_path / "late", **bad)


def test_validation_requires_complete_counts_and_simultaneous_bounds():
    with pytest.raises(IntegrityError): validation.summarize([[1999, 0, 0]] * len(validation.SCENARIOS))
    with pytest.raises(IntegrityError): validation.summarize([])
    with pytest.raises(IntegrityError): validation.summarize([[2000, 0, 1900]] * (len(validation.SCENARIOS) - 1))
    counts = [[2000, 0, 0] for _ in validation.SCENARIOS]
    counts[11][2] = 1900
    reports, passed = validation.summarize(counts)
    assert passed and reports[11]["strong_power_gate_passed"]
    counts[6][1] = 100
    assert validation.summarize(counts)[1] is False
    assert validation.interval(0, 2000)["upper"] > .001
    assert sha256_json(validation.specification()) == sha256_json(validation.specification())


@pytest.mark.parametrize("market", research.MARKETS)
def test_complete_market_selection_replay_and_source_feature_boundary(market):
    dataset = profit_dataset(market)
    run = research.run_market_wfa(dataset)
    assert len(run["outer_folds"]) == 8
    assert all(len(f["candidate_scores"]) == 45 for f in run["outer_folds"])
    report = research.evaluate_wfa(dataset, run)
    assert report["qualification"] is False and len(report["reports"]) == 8
    assert report["reports"]["selected"]["mean_daily_stress_usd"] > 0
    assert report["reports"]["flat_no_trade"]["executed_trades"] == 0
    selected = run["outer_folds"][-1]["streams"]["selected"]["model"]
    grid, bars = scheduled_fixture(market)
    batch = research.terminal.assemble_opportunities(bars, grid, as_of_ns=bars[-1]["decision_eligible_at_ns"])
    record = next(r for r in batch["records"] if r["feature"]["status"] != "INSUFFICIENT_HISTORY")
    feature = record["feature"]
    prediction = research.predict(selected, np.array([list(feature["feature_values"].values())]))
    assert prediction.shape == (1,) and np.isfinite(prediction).all()
    assert record["outcome"]["status"] == "MATURED"
