from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from futures_rebuild import rolling_hour_research as r
from futures_rebuild import rolling_hour_preparation as prep
from futures_rebuild import fixed_hour_terminal_successor as terminal
from futures_rebuild import profit_aligned_research as profit
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.exchange_calendar import CalendarSession, TradingStateInterval
from futures_rebuild.fixed_hour_outcome_foundation import synthetic_globex_hourly_opportunity_grid
from futures_rebuild.research.contracts import ResearchContractError

pytestmark = pytest.mark.current
ROOT = Path(__file__).resolve().parents[1]


def session_batch(minutes=190):
    session, rows, quotes = prep.synthetic_session(minutes=minutes)
    return session, rows, quotes, r.assemble_session(session, rows,
        as_of_ns=rows[-1]["decision_eligible_at_ns"], quotes=quotes)


def test_design_is_distinct_bounded_and_non_authorizing():
    spec = r.design()
    assert spec["mechanism_id"] != profit.mechanism_id()
    assert len(r.candidate_definitions()) * len(r.MARKETS) == 60
    assert {c["pack"] for c in r.candidate_definitions()} == set(r.PACKS)
    assert {c["family"] for c in r.candidate_definitions()} == {"ridge", "xgboost"}
    assert not any(spec["authority"].values())
    assert spec["phase_4"]["input_cadence"] == "1m"
    assert spec["phase_5"]["new_formulas"] == []
    assert spec["ladder"]["route"] == [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15]


def test_every_minute_grid_and_no_crossing_breaks():
    session, rows, _, batch = session_batch()
    grid = r.opportunity_grid(session)
    assert len(grid) == 68
    assert grid[0]["entry_ns"] == rows[0]["bar_start_ns"] + 62 * r.MINUTE
    assert all(b["decision_ns"] - a["decision_ns"] == r.MINUTE for a, b in zip(grid, grid[1:]))
    assert grid[-1]["exit_ns"] + r.MINUTE == rows[-1]["bar_end_ns"]
    assert all(rec["status"] == "MATURED" for rec in batch["records"])
    start = session.intervals[0].starts_at_utc
    split = CalendarSession(session.market, session.trade_date, (
        TradingStateInterval("OPEN", start, start + timedelta(minutes=125)),
        TradingStateInterval("PAUSED", start + timedelta(minutes=125), start + timedelta(minutes=130)),
        TradingStateInterval("OPEN", start + timedelta(minutes=130), start + timedelta(minutes=255))))
    assert len(r.opportunity_grid(split)) == 6
    assert r.opportunity_grid(CalendarSession(session.market, session.trade_date,
        (TradingStateInterval("OPEN", start, start + timedelta(minutes=122)),))) == []


def test_features_reuse_exact_predecessor_formulas_on_matching_timestamp():
    session, rows, _, batch = session_batch()
    old_grid = synthetic_globex_hourly_opportunity_grid(session, calendar_release_id="a" * 64)
    old = terminal.build_feature_record(rows, old_grid[1])
    new = next(rec["feature"] for rec in batch["records"] if rec["schedule"]["decision_ns"] == old["decision_at_ns"])
    assert old["feature_values"] == new["feature_values"]
    # Outcomes and later rows cannot influence the feature function's exact input window.
    scheduled = batch["records"][0]["schedule"]
    original = r.build_feature_record(rows[:61], scheduled)
    altered = deepcopy(rows); altered[100]["close_nano"] += 1000000000
    assert r.build_feature_record(altered[:61], scheduled) == original
    for field, value in (("source_cadence", "1s"), ("actual_contract", "ROLLED"),
                         ("decision_eligible_at_ns", scheduled["decision_ns"] + 1)):
        bad = deepcopy(rows[:61]); bad[-1][field] = value
        with pytest.raises(IntegrityError): r.build_feature_record(bad, scheduled)
    with pytest.raises(IntegrityError): r.build_feature_record(rows[:60], scheduled)


def test_outcome_maturity_price_signs_and_exit_bar_high_exclusion():
    _, rows, _, batch = session_batch()
    scheduled = batch["records"][0]["schedule"]
    path = rows[62:123]
    args = dict(expected_contract=rows[0]["actual_contract"])
    immature = r.build_outcome([], scheduled, as_of_ns=scheduled["decision_ns"], **args)
    assert immature["status"] == "UNMATURED" and immature["fit_target"] is None
    result = r.build_outcome(path, scheduled, as_of_ns=rows[-1]["decision_eligible_at_ns"], **args)
    changed = deepcopy(path); changed[-1]["high_nano"] += 1000 * changed[-1]["min_price_increment_nano"]
    other = r.build_outcome(changed, scheduled, as_of_ns=rows[-1]["decision_eligible_at_ns"], **args)
    assert other["fit_target"] == result["fit_target"] and other["diagnostics"] == result["diagnostics"]
    for row in changed:
        for field in ("open_nano", "high_nano", "low_nano", "close_nano"):
            row[field] -= 1000 * row["min_price_increment_nano"]
    signed = r.build_outcome(changed, scheduled, as_of_ns=rows[-1]["decision_eligible_at_ns"], **args)
    assert signed["fit_target"] == result["fit_target"]
    missing = r.build_outcome(path[:-1], scheduled, as_of_ns=rows[-1]["decision_eligible_at_ns"], **args)
    assert missing["fit_target"] is None and missing["status"] != "MATURED"


def test_position_policy_keeps_forecasts_without_stacking_or_moving_exit():
    _, _, _, batch = session_batch()
    rows = [rec["execution"] for rec in batch["records"]]
    predictions = np.full(len(rows), 1000.)
    result = r.score_predictions(predictions, rows, prep.synthetic_policy(), [batch["trade_date"]])
    assert r.score_predictions(predictions.tolist(), rows, prep.synthetic_policy(), [batch["trade_date"]]) == result
    executed = [d for d in result["decisions"] if d["executed_direction"]]
    assert [d["opportunity_id"] for d in executed] == [rows[0]["opportunity_id"], rows[61]["opportunity_id"]]
    assert all(d["exit_ns"] - d["entry_ns"] == 60 * r.MINUTE for d in executed)
    assert len(result["decisions"]) == len(rows)
    assert all(d["status"] == "POSITION_HELD_FORECAST_ONLY" for d in result["decisions"][1:61])
    # Contradictory forecasts while held do not reverse, stack, or extend a trade.
    predictions[1:61] = -1000.
    changed = r.score_predictions(predictions, rows, prep.synthetic_policy(), [batch["trade_date"]])
    assert [d["executed_direction"] for d in changed["decisions"]] == [d["executed_direction"] for d in result["decisions"]]
    assert changed["numerical_kernel_ledger"]["total_usd"] == result["numerical_kernel_ledger"]["total_usd"]
    with pytest.raises(IntegrityError): r.score_predictions(predictions[::-1], rows[::-1], prep.synthetic_policy(), [batch["trade_date"]])


def test_missing_books_abstain_missing_entered_exit_invalidates_all_evidence():
    _, _, _, batch = session_batch()
    rows = deepcopy([rec["execution"] for rec in batch["records"]])
    rows[0]["entry_quote"] = None
    replay = r.score_predictions(np.full(len(rows), 1000.), rows, prep.synthetic_policy(), [batch["trade_date"]])
    assert replay["decisions"][0]["executed_direction"] == 0
    assert replay["decisions"][1]["executed_direction"] == 1
    rows[1]["exit_quote"] = None
    with pytest.raises(IntegrityError, match="exit evidence"):
        r.score_predictions(np.full(len(rows), 1000.), rows, prep.synthetic_policy(), [batch["trade_date"]])


def test_cost_threshold_and_baselines_share_occupancy_and_accounting():
    _, _, _, batch = session_batch()
    rows = [rec["execution"] for rec in batch["records"]]
    policy = prep.synthetic_policy()
    threshold = 1 + policy["round_trip_fee_usd"] / (rows[0]["tick_value_nano_usd"] / 1e9) + 4
    flat = r.score_predictions(np.full(len(rows), threshold), rows, policy, [batch["trade_date"]])
    assert flat["numerical_kernel_ledger"]["executed_trades"] == 0
    baseline = r.score_predictions(np.ones(len(rows)), rows, policy, [batch["trade_date"]], forced_directions=True)
    assert baseline["numerical_kernel_ledger"]["executed_trades"] == 2
    assert r.score_predictions(np.zeros(len(rows)), rows, policy, [batch["trade_date"]], forced_directions=True)["numerical_kernel_ledger"]["total_usd"]["stress"] == "0"
    assert Decimal(baseline["numerical_kernel_ledger"]["total_usd"]["stress"]) == sum(
        Decimal(profit.trade_record(1000., rows[i], policy)["net_usd"]["stress"]) for i in (0, 61))


def test_session_completeness_label_alignment_and_mutation():
    session, rows, quotes, batch = session_batch(125)
    dataset = r.RollingDataset((batch,), prep.synthetic_policy(), (session,))
    assert len(dataset.opportunities) == 3
    altered = deepcopy(batch); altered["records"].pop(); altered["schedule_ids"].pop()
    altered = profit.bind({k: v for k, v in altered.items() if k != "session_id"}, "session_id")
    with pytest.raises(IntegrityError, match="complete declared"):
        r.RollingDataset((altered,), prep.synthetic_policy(), (session,))
    missing = r.assemble_session(session, rows[:80] + rows[81:], as_of_ns=rows[-1]["decision_eligible_at_ns"], quotes=quotes)
    assert len(missing["records"]) == 3
    with pytest.raises(IntegrityError, match="incomplete"):
        r.RollingDataset((missing,), prep.synthetic_policy(), (session,))
    dataset.batches[0]["records"][0]["execution"]["entry_quote"] = None
    with pytest.raises(IntegrityError, match="mutated"): dataset.verify()


def test_purge_rejects_overlapping_label_information():
    dataset = prep.synthetic_dataset(n=3)
    with pytest.raises(IntegrityError, match="not known"):
        profit.check_split_availability(dataset, np.array([0]), np.array([1]))
    profit.check_split_availability(dataset, np.array([0, 1, 2]), np.array([3, 4, 5]))


@pytest.mark.parametrize("family", ["ridge", "xgboost"])
def test_reused_fit_serialization_and_successor_identity(family):
    dataset = prep.synthetic_dataset(n=252)
    candidate = next(c for c in r.candidate_definitions() if c["family"] == family)
    model = r.fit_model(dataset, np.arange(len(dataset.opportunities)), candidate)
    prediction = r.predict(model, dataset.terminal_data.features)
    assert len(prediction) == len(dataset.opportunities) and np.isfinite(prediction).all()
    wrong = deepcopy(model); wrong["mechanism_id"] = profit.mechanism_id()
    wrong = profit.bind({k: v for k, v in wrong.items() if k != "model_id"}, "model_id")
    with pytest.raises(IntegrityError): r.predict(wrong, dataset.terminal_data.features)


def test_failed_calibration_is_recomputed_and_cannot_be_promoted():
    block = prep.statistical_block(ROOT)
    assert len(block["failed_null_scenarios"]) == 10
    assert "rare_large_loss" in block["failed_null_scenarios"]
    assert len(block["required_regression_design"]["scenarios"]) == 14
    assert block["replacement_method"] is None and not block["qualification_ready"]
    assert not block["activation_ready"] and block["phase_12_eligible_markets"] == []
    # A bounded-score success cannot establish the intended raw-dollar mean.
    raw = np.array([20.] * 99 + [-10000.])
    assert np.mean(np.clip(raw / 100, -1, 1)) > 0 and np.mean(raw) < 0
    assert r.qualification_status()["bounded_score_substitution"] == "FORBIDDEN_DIFFERENT_ESTIMAND"
    # A finite sample can omit all rare losses despite negative population mean.
    assert .99 * 1 + .01 * -200 < 0 and .99**252 > .07


def test_preparation_binds_phases_accounting_and_preserves_blocks():
    bundle = prep.build_bundle(ROOT)
    prep.validate_bundle(bundle, ROOT)
    assert set(bundle["phase_contracts"]) == {"3", "4", "5", "6", "ladder"}
    assert len(bundle["accounting"]["new_configuration_ids"]) == 60
    assert "configs/rolling_hour_research_v4.json" in bundle["preserved_files"]
    diagnostic = bundle["phase_contracts"]["6"]["rules"]["diagnostics"]
    assert diagnostic["selection_effect"] == "NONE"
    assert diagnostic["forecast_comparison"]["primary_benchmark"] == "zero_change"
    assert diagnostic["forecast_comparison"]["secondary_benchmark"] == "training_mean"
    assert bundle["specification"]["phase_6"]["selection_score"] == "MEAN_DAILY_SUMMED_STRESS_NET_USD_INCLUDING_ZERO_TRADE_DATES"
    bad = deepcopy(bundle); bad["statistical_block"]["qualification_ready"] = True
    bad = profit.bind({k: v for k, v in bad.items() if k != "bundle_id"}, "bundle_id")
    with pytest.raises(IntegrityError): prep.validate_bundle(bad, ROOT)


def test_pipeline_interface_is_read_only_and_keeps_real_history_blocked(capsys):
    from futures_rebuild.pipeline import main
    assert main(["rolling-hour-research"]) == 0
    assert "BLOCKED_RAW_PNL_INFERENCE_NOT_VALIDATED" in capsys.readouterr().out
    with pytest.raises(SystemExit): main(["--real-history", "rolling-hour-research"])


def test_declared_calendar_rejects_missing_dates_and_intervals_and_retains_empty_dates():
    dataset = prep.synthetic_dataset(n=3)
    with pytest.raises(IntegrityError, match="declared calendar dates"):
        r.RollingDataset(dataset.batches[:1] + dataset.batches[2:], dataset.policy, dataset.declared_sessions)
    changed = deepcopy(dataset.batches[0])
    changed["open_intervals"][0][1] += r.MINUTE
    changed = profit.bind({k: v for k, v in changed.items() if k != "session_id"}, "session_id")
    with pytest.raises(IntegrityError, match="intervals differ"):
        r.RollingDataset((changed, *dataset.batches[1:]), dataset.policy, dataset.declared_sessions)
    empty_session, rows, quotes = prep.synthetic_session(day="2018-01-02", minutes=120)
    empty = r.assemble_session(empty_session, rows, as_of_ns=rows[-1]["decision_eligible_at_ns"], quotes=quotes)
    complete = r.RollingDataset((dataset.batches[0], empty, dataset.batches[2]), dataset.policy,
        (dataset.declared_sessions[0], empty_session, dataset.declared_sessions[2]))
    replay = r.score_predictions(np.ones(6), complete.opportunities, complete.policy,
        complete.calendar_dates, forced_directions=True)
    ledger = replay["numerical_kernel_ledger"]
    assert ledger["daily_usd"]["2018-01-02"]["stress"] == "0"
    assert len(ledger["dates"]) == 3 and ledger["executed_trades"] == 2
    assert ledger["mean_daily_stress_usd"] == float(Decimal(ledger["total_usd"]["stress"]) / 3)


def test_complete_calendar_controls_fold_boundaries_even_when_a_date_has_no_rows():
    dataset = prep.synthetic_dataset()
    old = r.fold_schedule(dataset)
    day = dataset.calendar_dates[600]
    session, rows, quotes = prep.synthetic_session(day=day, minutes=120)
    batch = r.assemble_session(session, rows, as_of_ns=rows[-1]["decision_eligible_at_ns"], quotes=quotes)
    batches, sessions = list(dataset.batches), list(dataset.declared_sessions)
    batches[600], sessions[600] = batch, session
    empty = r.RollingDataset(tuple(batches), dataset.policy, tuple(sessions))
    new = r.fold_schedule(empty)
    assert [(f.test_window, [i.validation_window for i in f.inner_folds]) for f in new] == [
        (f.test_window, [i.validation_window for i in f.inner_folds]) for f in old]
    second = new[1]
    assert day in empty.calendar_dates[second.test_window.start:second.test_window.stop]
    assert day not in {empty.terminal_data.trade_dates[i] for i in second.audit_indices}
    for split in new:
        profit.check_split_availability(empty, split.fit_indices, split.audit_indices)


def test_outer_labels_cannot_change_that_folds_selection_or_predictions(monkeypatch):
    dataset = prep.synthetic_dataset()
    split = r.fold_schedule(dataset)[0]
    # Exercise the actual first-fold selector twice; the complete eight-fold
    # run remains a separate acceptance test, not simulated by this unit test.
    monkeypatch.setattr(r, "fold_schedule", lambda _: (split,))
    before = r.run_market_wfa(dataset)["outer_folds"][0]
    batches = deepcopy(dataset.batches)
    for batch in batches[split.test_window.start:split.test_window.stop]:
        for rec in batch["records"]:
            outcome = {k: v for k, v in rec["outcome"].items() if k != "outcome_id"}
            outcome["fit_target"] = -outcome["fit_target"]
            outcome["diagnostics"] = {
                "maximum_upside_excursion_ticks": abs(outcome["fit_target"]) + 1,
                "maximum_downside_excursion_ticks": abs(outcome["fit_target"]) + 1}
            rec["outcome"] = profit.bind(outcome, "outcome_id")
            rec["execution"]["outcome_id"] = rec["outcome"]["outcome_id"]
        batch["session_id"] = sha256_json({k: v for k, v in batch.items() if k != "session_id"})
    changed = r.RollingDataset(tuple(batches), dataset.policy, dataset.declared_sessions)
    after = r.run_market_wfa(changed)["outer_folds"][0]
    assert before["candidate_scores"] == after["candidate_scores"]
    assert before["forecast_benchmarks"] == after["forecast_benchmarks"] == {
        "zero_change": 0., "training_mean": float(np.mean(dataset.terminal_data.terminal_ticks[split.fit_indices]))}
    for name in before["streams"]:
        assert before["streams"][name]["predictions"] == after["streams"][name]["predictions"]
        assert before["streams"][name]["model"]["kernel_model"] == after["streams"][name]["model"]["kernel_model"]


def test_forecast_comparison_matches_dates_handles_zero_and_rejects_nonfinite_errors():
    source = prep.synthetic_dataset(n=3)
    rows = [source.opportunities[i] for i in (0, 1, 3)]
    dataset = SimpleNamespace(opportunities=rows, policy=source.policy,
        terminal_data=SimpleNamespace(market="ES", terminal_ticks=np.array([2., 4., 8.])))
    run = {"outer_folds": [
        {"audit_indices": [0, 1], "audit_dates": ["2018-01-01"],
         "streams": {"selected": {"predictions": [1., 3.]}},
         "forecast_benchmarks": {"zero_change": 0., "training_mean": 3.}},
        {"audit_indices": [2], "audit_dates": ["2018-01-02", "2018-01-03"],
         "streams": {"selected": {"predictions": [6.]}},
         "forecast_benchmarks": {"zero_change": 0., "training_mean": 6.}}]}
    replay = r.score_predictions(np.ones(3), rows, source.policy, source.calendar_dates, forced_directions=True)
    assert replay["numerical_kernel_ledger"]["executed_trades"] == 2
    result = r.prediction_diagnostics(dataset, run, replay)["forecast_comparison"]
    assert result["forecast_count"] == 3  # Includes the held-position forecast.
    assert result["audit_date_count"] == 3 and result["forecast_date_count"] == 2
    assert result["empty_audit_dates"] == ["2018-01-03"]
    assert result["market"] == "ES" and not result["qualification_ready"]
    scores = result["streams"]
    assert scores["model"]["daily_mse_ticks_squared"] == {"2018-01-01": 1., "2018-01-02": 4.}
    assert scores["model"]["date_balanced_mse_ticks_squared"] == 2.5  # Row-weighted MSE would be 2.
    assert scores["zero_change"]["date_balanced_mse_ticks_squared"] == 37.
    assert scores["zero_change"]["mse_improvement_ticks_squared"] == 34.5
    assert scores["zero_change"]["skill"] == pytest.approx(1 - 2.5 / 37)
    assert scores["training_mean"]["date_balanced_mse_ticks_squared"] == 2.5
    assert scores["training_mean"]["skill"] == 0.
    dataset.terminal_data.terminal_ticks[:] = 0.
    zero = r.prediction_diagnostics(dataset, run, replay)["forecast_comparison"]["streams"]
    assert zero["zero_change"]["skill"] is None
    assert zero["zero_change"]["skill_unavailable_reason"] == "ZERO_BENCHMARK_MSE"
    for value in (np.nan, np.inf, 1e308):
        run["outer_folds"][0]["streams"]["selected"]["predictions"][0] = value
        with pytest.raises(ResearchContractError, match="squared errors"):
            r.prediction_diagnostics(dataset, run, replay)


def test_rich_fixture_covers_time_context_and_multiple_entries_with_independent_money_check():
    session, rows, quotes = prep.rich_synthetic_session("ES", "2018-03-05", 63)
    batch = r.assemble_session(session, rows, as_of_ns=rows[-1]["decision_eligible_at_ns"], quotes=quotes)
    dataset = r.RollingDataset((batch,), prep.synthetic_policy(), (session,))
    assert {rec["feature"]["quartile"] for rec in batch["records"]} == {1, 2, 3, 4}
    replay = r.score_predictions(np.ones(len(dataset.opportunities)), dataset.opportunities,
        dataset.policy, dataset.calendar_dates, forced_directions=True)
    assert replay["decisions"][0]["status"] == "ENTRY_QUOTE_ABSTENTION"
    assert replay["decisions"][1]["status"] == "DECISION_QUOTE_ABSTENTION"
    executed = [i for i, d in enumerate(replay["decisions"]) if d["executed_direction"]]
    assert len(executed) >= 2
    expected = Decimal(0)
    for i in executed:
        row = dataset.opportunities[i]
        ticks = Decimal(row["exit_quote"]["bid_nano"] - row["entry_quote"]["ask_nano"]) / row["tick_nano"]
        expected += (ticks - 4) * Decimal(row["tick_value_nano_usd"]) / 10**9 - 10
    assert Decimal(replay["numerical_kernel_ledger"]["total_usd"]["stress"]) == expected
    column = terminal.FEATURE_NAMES.index("close_change_volatility_ticks_60")
    assert np.ptp(dataset.terminal_data.features[:, column]) > 0


def test_future_quotes_do_not_change_decision_and_future_outcome_does_not_enter_prediction():
    train = prep.synthetic_dataset(n=252)
    model = r.fit_model(train, np.arange(len(train.opportunities)), r.candidate_definitions()[0])
    session, rows, quotes = prep.synthetic_session(day="2018-04-01", minutes=190)
    batch = r.assemble_session(session, rows, as_of_ns=rows[-1]["decision_eligible_at_ns"], quotes=quotes)
    schedule = batch["records"][0]["schedule"]
    feature = r.build_feature_record(rows[:61], schedule)
    x = np.array([[feature["feature_values"][n] for n in terminal.FEATURE_NAMES]])
    prediction = float(r.predict(model, x)[0])
    row = deepcopy(batch["records"][0]["execution"])
    before = profit.decision(prediction, row, train.policy)
    row["entry_quote"] = None
    row["exit_quote"] = {"unavailable_future_event": True}
    row["target_available_at_ns"] = None
    assert profit.decision(prediction, row, train.policy) == before
    assert r.predict(model, x)[0] == prediction
    assert r.build_outcome([], schedule, expected_contract=feature["actual_contract"],
        as_of_ns=schedule["decision_ns"])["status"] == "UNMATURED"


def test_remediation_disposition_preparation_and_charges_cannot_open_a_gate():
    import json
    spec = r.design()
    assert spec["phase_5"]["pack_sizes"] == {"baseline": 5, "context": 10, "context_interactions": 16}
    assert spec["phase_5"]["candidate_feature_union_size"] == 16
    assert spec["statistical_qualification"]["replacement_method"] is None
    assert spec["statistical_qualification"]["moment_bound"] is None
    bundle = prep.build_bundle(ROOT)
    previous = json.loads((ROOT / "configs/rolling_hour_research_v2.json").read_bytes())
    assert bundle["accounting"]["preserved_prior_configuration_ids"] == previous["accounting"]["new_configuration_ids"]
    assert not set(bundle["accounting"]["new_configuration_ids"]) & set(previous["accounting"]["new_configuration_ids"])
    preparation = bundle["readiness_preparation"]
    assert preparation["search_upper_bound"] is None and preparation["quote_inputs"] == []
    assert not any(preparation["authority"].values()) and not preparation["price_bytes_verified"]
    assert not bundle["statistical_block"]["qualification_ready"]


def test_diagnostic_refit_failure_is_unavailable_while_selected_failure_still_stops(monkeypatch):
    dataset = prep.synthetic_dataset()
    split = r.fold_schedule(dataset)[0]
    monkeypatch.setattr(r, "fold_schedule", lambda _: (split,))
    original = r.fit_model

    def diagnostic_failure(dataset, indices, candidate, **kwargs):
        if candidate["pack"] == "context_interactions" and kwargs.get("fit_kind") == "outer":
            raise r.comparison.LeafSupportError("injected diagnostic support failure")
        return original(dataset, indices, candidate, **kwargs)

    monkeypatch.setattr(r, "fit_model", diagnostic_failure)
    run = r.run_market_wfa(dataset)
    assert run["outer_folds"][0]["streams"]["selected"]["status"] == "FITTED"
    assert run["outer_folds"][0]["streams"]["context_interactions"]["predictions"] is None
    result = r.evaluate_wfa(dataset, run)
    assert len(result["reports"]["selected"]["numerical_kernel_ledger"]["dates"]) == 63
    assert result["reports"]["context_interactions"]["mean_daily_stress_usd"] is None
    assert result["matched_comparisons"][1]["mean_daily_stress_usd_difference"] is None
    assert not result["matched_comparisons"][1]["complete"]
    assert not result["qualification"]["qualification_ready"]
    forecast = result["diagnostics"]["forecast_comparison"]
    assert forecast["forecast_count"] == len(split.audit_indices)
    assert forecast["forecast_date_count"] == 63
    assert set(forecast["streams"]) == {"model", "zero_change", "training_mean"}

    def unavailable_diagnostic(*args):
        raise ResearchContractError("injected unavailable forecast diagnostic")
    monkeypatch.setattr(r, "prediction_diagnostics", unavailable_diagnostic)
    unavailable = r.evaluate_wfa(dataset, run)
    assert unavailable["reports"]["selected"] == result["reports"]["selected"]
    assert unavailable["diagnostics"]["state"] == "INCOMPLETE_FORECAST_DIAGNOSTICS"

    def selected_failure(dataset, indices, candidate, **kwargs):
        if candidate["family"] == "ridge" and kwargs.get("fit_kind") == "outer":
            raise r.comparison.LeafSupportError("injected selected support failure")
        return original(dataset, indices, candidate, **kwargs)

    monkeypatch.setattr(r, "fit_model", selected_failure)
    with pytest.raises(r.comparison.LeafSupportError, match="selected outer refit failed"):
        r.run_market_wfa(dataset)


def test_1010_calendar_with_476_first_inner_rows_on_two_dates_fails():
    from datetime import date
    dates = tuple((date(2018, 1, 1) + timedelta(days=i)).isoformat() for i in range(1010))
    observed = (dates[0],) * 238 + (dates[1],) * 238 + dates[252:]
    dataset = SimpleNamespace(calendar_dates=dates, terminal_data=SimpleNamespace(trade_dates=observed))
    with pytest.raises(IntegrityError, match=r"inner training has 2 observed dates; requires 252 \(rows=476\)"):
        r.fold_schedule(dataset)


@pytest.mark.parametrize("fit_kind,minimum", [("inner", 252), ("outer", 505), ("final", 505)])
def test_all_fit_paths_enforce_observed_dates(fit_kind, minimum):
    dataset = SimpleNamespace(terminal_data=SimpleNamespace(trade_dates=tuple(str(i) for i in range(minimum))))
    with pytest.raises(IntegrityError, match="observed dates"):
        r.training_support(dataset, np.arange(minimum - 1), fit_kind=fit_kind)
    assert len(r.training_support(dataset, np.arange(minimum), fit_kind=fit_kind)) == minimum
    with pytest.raises(IntegrityError, match="unique"):
        r.training_support(dataset, np.array([0, 0]), fit_kind=fit_kind)


@pytest.mark.parametrize("family", ["ridge", "xgboost"])
def test_normalized_fitting_replication_math_and_train_only_transform(family):
    from datetime import date
    n = 505
    column = terminal.FEATURE_NAMES.index(terminal.CORE_FEATURES[0])
    x = np.zeros((n, len(terminal.FEATURE_NAMES)))
    x[:, column] = np.arange(n) % 5
    y = 3. * x[:, column] + 7.
    dates = tuple((date(2018, 1, 1) + timedelta(days=i)).isoformat() for i in range(n))
    def data(ix):
        return SimpleNamespace(_evidence_id="c" * 64, terminal_data=SimpleNamespace(features=x[ix],
            terminal_ticks=y[ix], trade_dates=tuple(dates[i] for i in ix), sample_ids=tuple(map(str, range(len(ix))))))
    base = data(np.arange(n))
    duplicate = data(np.r_[np.zeros(7, dtype=int), np.arange(n)])
    c = next(c for c in r.candidate_definitions() if c["family"] == family)
    first = r.fit_model(base, np.arange(n), c)
    second = r.fit_model(duplicate, np.arange(n + 7), c)
    np.testing.assert_allclose(r.predict(first, x), r.predict(second, x), atol=1e-5, rtol=1e-6)
    for kind in ("outer", "final"):
        refit = r.fit_model(base, np.arange(n), c, fit_kind=kind)
        np.testing.assert_array_equal(r.predict(first, x), r.predict(refit, x))
    weights = r.training_weights(duplicate.terminal_data.trade_dates)
    assert weights.sum() == pytest.approx(1.) and weights[:8].sum() == pytest.approx(1 / n)
    if family == "ridge":
        model = first["kernel_model"]["ridge"]
        assert model["means"][0] == pytest.approx(x[:, column].mean())
        expected = 3 * x[:, column].std() / (1 + c["penalty"])
        assert model["weights"][0] == pytest.approx(expected)
        before = deepcopy(model)
        r.predict(first, x * 10000)
        assert model == before


def test_source_adapter_complete_integration_and_rejections(monkeypatch):
    from futures_rebuild.rolling_hour_source import adapt_source_sessions
    session, rows, old_quotes = prep.synthetic_session(minutes=125)
    contract_id, release_id, quote_id = (sha256_json({"generated_adapter_test": s}) for s in ("contract", "price", "quote"))
    for row in rows:
        row.update(source_contract_id=contract_id, source_release_id=release_id)
    class GeneratedCalendar:
        receipt = SimpleNamespace(release_id=sha256_json({"generated_calendar": 1}))
        sessions = {(session.market, session.trade_date): session}
        def verify(self):
            pass
    monkeypatch.setattr(r, "VerifiedExchangeCalendar", GeneratedCalendar)
    calendar = GeneratedCalendar()
    grid = r.opportunity_grid(session, calendar=calendar, source_contract_id=contract_id, source_release_id=release_id)
    quotes = {s["opportunity_id"]: deepcopy(q) for s, q in zip(grid, old_quotes.values(), strict=True)}
    for fields in quotes.values():
        for quote in fields.values():
            quote["source_release_id"] = quote_id
    policy = prep.synthetic_policy()
    policy = profit.bind({**{k: v for k, v in policy.items() if k != "policy_id"}, "source_release_id": quote_id}, "policy_id")
    day = session.trade_date.isoformat()
    binding = profit.bind({"market": "ES", "trade_date": day, "source_contract_id": contract_id,
        "source_release_id": release_id, "calendar_release_id": calendar.receipt.release_id,
        "calendar_evidence_id": "e" * 64, "contract_selection_evidence_id": "f" * 64,
        "source_certificate_id": "c" * 64, "actual_contract": rows[0]["actual_contract"],
        "calendar_known_at_ns": rows[0]["bar_start_ns"] - 1,
        "contract_selection_known_at_ns": rows[0]["bar_start_ns"] - 1,
        "point_in_time_evidence": "CERTIFIED_ORIGINAL_PUBLICATION",
        "source_file_sha256s": sorted(set(row["source_file_sha256"] for row in rows))}, "binding_id")
    args = dict(sessions=(session,), rows_by_date={day: rows}, quotes_by_date={day: quotes},
        calendar=calendar, source_contract_id=contract_id, source_release_id=release_id, policy=policy,
        bindings={day: binding}, as_of_ns=rows[-1]["decision_eligible_at_ns"])
    dataset = adapt_source_sessions(**args)
    assert len(dataset.opportunities) == 3
    assert dataset.source_release_id == release_id
    assert all(p["calendar_authority"] == "VERIFIED_RELEASE" for p in dataset.opportunities)
    for key, value, message in [
        ("rows_by_date", {day: rows[:-1]}, "coverage"),
        ("quotes_by_date", {day: {}}, "quote join"),
        ("source_release_id", r.SYNTHETIC_SOURCE_RELEASE_ID, "synthetic"),
        ("as_of_ns", rows[70]["decision_eligible_at_ns"], "incomplete")]:
        with pytest.raises(IntegrityError, match=message):
            adapt_source_sessions(**{**args, key: value})
    late = dict(binding); late.pop("binding_id"); late["calendar_known_at_ns"] = rows[-1]["bar_start_ns"]
    with pytest.raises(IntegrityError, match="known before"):
        adapt_source_sessions(**{**args, "bindings": {day: profit.bind(late, "binding_id")}})
    altered = deepcopy(rows); altered[0]["actual_contract"] = "WRONG"
    with pytest.raises(IntegrityError, match="contract"):
        adapt_source_sessions(**{**args, "rows_by_date": {day: altered}})
