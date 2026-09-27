from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path

import numpy as np
import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.exchange_calendar import CalendarSession, TradingStateInterval
from futures_rebuild.fixed_hour_globex_pipeline import _synthetic_row
from futures_rebuild.fixed_hour_outcome_foundation import synthetic_globex_hourly_opportunity_grid
from futures_rebuild.fixed_hour_terminal_successor import (
    CORE_FEATURES, FEATURE_NAMES, LONG_NAMES, MARKETS, PACKS, QUARTILE_NAMES,
    TerminalDataset, apply_fitted, assemble_opportunities, build_feature_record, build_outcome, candidate_definitions,
    fitted_model, fit_predict,
    choose_candidate, design, fold_schedule, provisional_costs, retained_columns,
    run_market_wfa, stress_score, terminal_directions, validate_labels,
)
from futures_rebuild.fixed_hour_terminal_evaluation import (
    BASELINES, coverage_report, downstream_eligibility, evaluate_outer_predictions,
    expected_costs, qualify_markets, risk_diagnostics, terminal_failure, trial_burden,
)
from futures_rebuild.research.contracts import ResearchContractError

pytestmark = pytest.mark.current
NANO = 1_000_000_000
MINUTE = 60 * NANO


def scheduled_fixture(market="ES", start=None, hours=6):
    start = start or datetime(2022, 1, 9, 23, tzinfo=timezone.utc)
    session = CalendarSession(market, date(2022, 1, 10), (TradingStateInterval("OPEN", start, start + timedelta(hours=hours)),))
    grid = synthetic_globex_hourly_opportunity_grid(session, calendar_release_id="a" * 64)
    ns = int(start.timestamp()) * NANO
    rows = [_synthetic_row(ns + i * MINUTE, i, market) for i in range(hours * 60)]
    return grid, rows


def synthetic_dataset(market="ES", *, n=1010, per_day=2):
    dates = tuple((date(2018, 1, 1) + timedelta(days=i)).isoformat() for i in range(n))
    rng = np.random.default_rng(583)
    x = rng.normal(size=(n * per_day, len(FEATURE_NAMES)))
    # Observed labels are integral ticks, with a deliberately learnable synthetic
    # directional relationship; this is not evidence about market predictability.
    target = np.rint(80 * x[:, 0] + rng.normal(size=len(x)) * 2)
    for col in range(len(FEATURE_NAMES)):
        if FEATURE_NAMES[col] == CORE_FEATURES[0]: x[:, col] = x[:, 0]
    costs = provisional_costs(market, np.full(len(x), 12.5))["stress"]
    return TerminalDataset(market, tuple(f"{market}-{i}" for i in range(len(x))), tuple(d for d in dates for _ in range(per_day)),
                           x, target, costs, dates, np.full(len(x), 12.5))


@pytest.fixture(scope="module")
def fitted():
    dataset = synthetic_dataset()
    return dataset, run_market_wfa(dataset)


def evaluation(dataset, run, excursion_extra=10):
    n = len(dataset.sample_ids)
    outer_n = sum(len(f["audit_indices"]) for f in run["outer_folds"])
    cov = coverage_report(scheduled=outer_n + 504, insufficient_history=504, valid_feature=outer_n, invalid_outcome=0, quality_errors=0)
    return evaluate_outer_predictions(dataset, run,
        upside_ticks=np.maximum(dataset.terminal_ticks, 0) + excursion_extra,
        downside_ticks=np.maximum(-dataset.terminal_ticks, 0) + excursion_extra,
        tick_values_usd=np.full(n, 12.5), quartiles=np.full(n, 2),
        decision_ns=np.arange(n, dtype=np.int64) * MINUTE, coverage=cov)


def bind(payload, key):
    return {**{k: v for k, v in payload.items() if k != key}, key: sha256_json({k: v for k, v in payload.items() if k != key})}


def test_terminal_rule_equality_invalid_and_excursion_independence():
    assert json.loads(json.dumps(design())) == design()
    np.testing.assert_array_equal(terminal_directions(np.array([2., 1., -1., -2., 0.]), np.ones(5)), [1, 0, 0, -1, 0])
    for p, c in ((np.array([np.nan]), np.ones(1)), (np.ones(1), np.zeros(1)), (np.ones(2), np.ones(1))):
        with pytest.raises((IntegrityError, ResearchContractError)): terminal_directions(p, c)
    validate_labels(np.array([-10., 10.]), np.array([0., 10.]), np.array([10., 0.]))
    for t, u, d in ((10., 4., 0.), (-10., 0., 4.), (0., -1., 1.), (.5, 1., 0.), (0., np.inf, 0.)):
        with pytest.raises((IntegrityError, ResearchContractError)): validate_labels(*(np.array([v]) for v in (t, u, d)))
    assert "upside" not in TerminalDataset.__dataclass_fields__
    assert "diagnostics" not in TerminalDataset.__dataclass_fields__


def test_grid_eligibility_and_formulas():
    grid, rows = scheduled_fixture()
    assert build_feature_record(rows, grid[0])["status"] == "INSUFFICIENT_HISTORY"
    record = build_feature_record(rows, grid[1])
    assert (grid[1]["entry_bar_start_ns"] - grid[1]["open_interval_start_ns"]) // MINUTE == 82
    assert record["audit"]["bar_count"] == 61
    values = record["feature_values"]
    assert tuple(values) == FEATURE_NAMES and len(values) == 29
    closes = np.array([r["close_nano"] for r in rows[20:81]], dtype=np.float64)
    assert values[LONG_NAMES[0]] == (closes[-1] - closes[0]) / rows[0]["min_price_increment_nano"]
    assert values[LONG_NAMES[1]] == np.std(np.diff(closes) / rows[0]["min_price_increment_nano"])
    for opportunity in grid[1:]:
        record = build_feature_record(rows, opportunity)
        q = 1 + int(4 * (opportunity["decision_at_ns"] - opportunity["open_interval_start_ns"]) // (opportunity["open_interval_end_ns"] - opportunity["open_interval_start_ns"]))
        assert record["quartile"] == q
        for quartile in (2, 3, 4):
            for name in (CORE_FEATURES[0], CORE_FEATURES[4]):
                assert record["feature_values"][f"{name}_times_q{quartile}"] == record["feature_values"][name] * (q == quartile)


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "future", "roll", "tick", "economics", "cadence", "source", "stale"])
def test_history_rejections(mutation):
    grid, rows = scheduled_fixture()
    if mutation == "missing": rows.pop(30)
    elif mutation == "duplicate": rows.append(dict(rows[30]))
    elif mutation == "future": rows[30]["decision_eligible_at_ns"] = grid[1]["decision_at_ns"] + 1
    elif mutation == "roll": rows[30]["actual_contract"] = "ESM2"
    elif mutation == "tick": rows[30]["close_nano"] += 1
    elif mutation == "economics": rows[30]["multiplier_nano"] *= 2
    elif mutation == "cadence": rows[30]["source_cadence"] = "5m"
    elif mutation == "source": rows[30]["source_release_id"] = "b" * 64
    elif mutation == "stale": rows.pop(80)
    with pytest.raises(IntegrityError): build_feature_record(rows, grid[1])


@pytest.mark.parametrize("offset", [0, -110 * NANO, "zero_entry"])
def test_signed_price_outcome_provenance_and_maturity(offset):
    grid, rows = scheduled_fixture()
    if offset == "zero_entry": offset = -rows[82]["open_nano"]
    for row in rows:
        for name in ("open_nano", "high_nano", "low_nano", "close_nano"): row[name] += offset
    feature = build_feature_record(rows, grid[1])
    assert feature["status"] == "VALID_FEATURE"
    args = dict(expected_contract="ESH2", as_of_ns=rows[-1]["decision_eligible_at_ns"])
    outcome = build_outcome(rows, grid[1], **args)
    assert outcome["fit_target"] == (rows[142]["open_nano"] - rows[82]["open_nano"]) / rows[82]["min_price_increment_nano"]
    assert outcome["audit"]["entry_bar_start_ns"] == rows[82]["bar_start_ns"]
    assert build_outcome(rows, grid[1], expected_contract="ESH2", as_of_ns=grid[1]["decision_at_ns"])["fit_target"] is None
    altered = deepcopy(rows)
    altered[142]["high_nano"] += 100 * NANO
    after = build_outcome(altered, grid[1], **args)
    assert after["diagnostics"] == outcome["diagnostics"]  # Exit high is not in the path.
    altered[82]["source_row_sha256"] = "invalid"
    with pytest.raises(IntegrityError): build_outcome(altered, grid[1], **args)


def test_breaks_early_closes_and_dst_do_not_bridge_history():
    for start in (datetime(2022, 3, 13, 9, tzinfo=timezone.utc), datetime(2022, 11, 6, 8, tzinfo=timezone.utc)):
        grid, rows = scheduled_fixture(start=start, hours=3)
        assert [build_feature_record(rows, o)["status"] for o in grid] == ["INSUFFICIENT_HISTORY", "VALID_FEATURE"]
    start = datetime(2022, 1, 9, 23, tzinfo=timezone.utc)
    session = CalendarSession("ES", date(2022, 1, 10), (
        TradingStateInterval("OPEN", start, start + timedelta(hours=3)),
        TradingStateInterval("PAUSED", start + timedelta(hours=3), start + timedelta(hours=4)),
        TradingStateInterval("OPEN", start + timedelta(hours=4), start + timedelta(hours=7))))
    grid = synthetic_globex_hourly_opportunity_grid(session, calendar_release_id="a" * 64)
    rows = [_synthetic_row(int(start.timestamp()) * NANO + i * MINUTE, i, "ES") for i in range(420)]
    assert [build_feature_record(rows, o)["status"] for o in grid] == ["INSUFFICIENT_HISTORY", "VALID_FEATURE"] * 2


def test_fixed_packs_constants_duplicates_and_tie_break():
    assert len(PACKS) == 9 and len(candidate_definitions()) == 27 and len(MARKETS) * len(candidate_definitions()) == 108
    x = np.zeros((10, len(FEATURE_NAMES)))
    x[:, 0] = np.arange(10); x[:, 1] = x[:, 0]
    assert retained_columns(x, FEATURE_NAMES[:3]) == [0]
    rows = [dict(score=1., retained_count=5, penalty=.1, candidate_id="a"),
            dict(score=.995, retained_count=4, penalty=1., candidate_id="b"),
            dict(score=.995, retained_count=4, penalty=10., candidate_id="c"),
            dict(score=.98, retained_count=0, penalty=10., candidate_id="d")]
    assert choose_candidate(rows)["candidate_id"] == "c"


def test_exact_session_score_and_risk():
    predicted = np.array([2., 0., -2.]); actual = np.array([4., 100., -2.]); costs = np.ones(3)
    assert stress_score(predicted, actual, costs, ("a", "a", "b")) == 1.25
    r = risk_diagnostics(np.array([2., -3., 4.]), np.full(3, 12.5), ("a", "a", "b"), np.array([1., -1., 1.]), np.array([2., 3., 4.]), np.array([1., 2., 3.]))
    assert r["ticks"]["total_pnl"] == 3 and r["ticks"]["maximum_fixed_exit_drawdown"] == 3
    assert r["ticks"]["daily_pnl_variance"] == 6.25
    assert r["usd"]["maximum_fixed_exit_drawdown"] == 37.5
    c = coverage_report(scheduled=100, insufficient_history=10, valid_feature=89, invalid_outcome=1, quality_errors=0)
    assert c["eligible_schedule_coverage"] == 88 / 90 and c["original_schedule_coverage"] == .88
    with pytest.raises(IntegrityError): coverage_report(scheduled=2, insufficient_history=1, valid_feature=2, invalid_outcome=0, quality_errors=0)


def test_cost_evidence_is_not_midpoint_or_future_data():
    kwargs = dict(market="ES", decision_ns=np.array([100, 200], dtype=np.int64), tick_values=np.full(2, 12.5), trusted_certification_ids=frozenset({"a" * 64}), evaluation_frozen_at_ns=300)
    assert expected_costs(None, **kwargs)[1] == "EXPECTED_COST_UNCALIBRATED"
    evidence = bind(dict(market="ES", certification_id="a" * 64, source_release_id="b" * 64,
        independent_of_candidate_results=True, price_reference_basis="ONE_MINUTE_OPEN_PRINT_PROXY", observations_end_ns=80,
        certified_at_ns=250, evaluation_frozen_at_ns=300,
        effective_start_ns=100, effective_end_ns=201, round_trip_fee_usd=10.,
        adverse_ticks_round_trip=2., coverage_ratio=1., all_verified_globex_intervals=True), "evidence_id")
    np.testing.assert_allclose(expected_costs(evidence, **kwargs)[0], [2.8, 2.8])
    # Historical observations may be certified later, but before the bound freeze.
    for key, value in (("price_reference_basis", "MIDPOINT"), ("independent_of_candidate_results", False),
                       ("observations_end_ns", 101), ("certified_at_ns", 301), ("certified_at_ns", 79),
                       ("evaluation_frozen_at_ns", 301), ("evaluation_frozen_at_ns", True)):
        with pytest.raises(IntegrityError): expected_costs(bind({**evidence, key: value}, "evidence_id"), **kwargs)
    for key, value in (("effective_end_ns", 200), ("coverage_ratio", .97), ("certification_id", "c" * 64)):
        assert expected_costs(bind({**evidence, key: value}, "evidence_id"), **kwargs)[1] == "EXPECTED_COST_UNCALIBRATED"
    assert expected_costs(evidence, **{**kwargs, "evaluation_frozen_at_ns": None})[1] == "EXPECTED_COST_UNCALIBRATED"
    with pytest.raises(IntegrityError, match="explicit integer"):
        expected_costs(evidence, **{**kwargs, "evaluation_frozen_at_ns": True})
    np.testing.assert_allclose(expected_costs(bind({**evidence, "certified_at_ns": 300}, "evidence_id"), **kwargs)[0], [2.8, 2.8])
    assert provisional_costs("ES", np.array([12.5]))["base"][0] * 12.5 == 35.
    assert provisional_costs("ES", np.array([12.5]))["stress"][0] * 12.5 == 60.


def test_nested_manifests_and_outer_label_isolation(fitted):
    dataset, run = fitted
    assert len(run["outer_folds"]) == 8
    first = run["outer_folds"][0]
    assert len(first["fit_trade_dates"]) == 505 and len(first["audit_trade_dates"]) == 63
    assert len(first["inner_manifests"][0]["fit_trade_dates"]) == 252
    assert len(first["candidate_scores"]) == 27
    target = dataset.terminal_ticks.copy()
    target[first["audit_indices"]] *= -1
    # Reusing a prior outer period as later training is legal. Only its own
    # forecast must be invariant to its labels.
    changed = run_market_wfa(replace(dataset, terminal_ticks=target))
    assert first == changed["outer_folds"][0]
    with pytest.raises(IntegrityError): fold_schedule(synthetic_dataset(n=1009))
    with pytest.raises(IntegrityError): replace(dataset, market="ZN")
    with pytest.raises(IntegrityError): replace(dataset, global_dates=dataset.global_dates[:-1])


def test_diagnostic_changes_do_not_change_prediction_run(fitted):
    dataset, run = fitted
    report = evaluation(dataset, run)
    assert report["expected_cost_status"] == "EXPECTED_COST_UNCALIBRATED"
    assert set(report["scenarios"]) == {"gross", "base", "stress", "extreme"}
    assert report["run_id"] == run["run_id"]
    # Diagnostics are only consumed by a separate evaluator, after prediction.
    other = evaluation(dataset, run, excursion_extra=999)
    assert other["result_id"] != report["result_id"]
    assert other["scenarios"]["stress"]["adverse_excursion"] != report["scenarios"]["stress"]["adverse_excursion"]
    assert other["daily_stress_net_ticks"] == report["daily_stress_net_ticks"]
    assert other["run_id"] == report["run_id"]
    for scenario in report["scenarios"]:
        assert other["scenarios"][scenario]["ticks"] == report["scenarios"][scenario]["ticks"]
        assert other["scenarios"][scenario]["trades"] == report["scenarios"][scenario]["trades"]


def test_complete_grid_retains_invalid_outcomes_and_coverage():
    grid, rows = scheduled_fixture(hours=3)
    batch = assemble_opportunities(rows, grid, as_of_ns=grid[1]["decision_at_ns"])
    assert batch["coverage"]["scheduled"] == 2
    assert batch["coverage"]["insufficient_history"] == 1
    assert batch["coverage"]["valid_feature"] == 1
    assert batch["coverage"]["invalid_outcome"] == 1
    assert batch["records"][1]["feature"]["status"] == "VALID_FEATURE"
    assert batch["records"][1]["outcome"]["fit_target"] is None
    with pytest.raises(IntegrityError, match="grid is incomplete"):
        assemble_opportunities(rows, grid[1:], as_of_ns=rows[-1]["decision_eligible_at_ns"])


def test_verified_quartile_boundary_is_decision_time_not_entry_time():
    start = datetime(2022, 1, 9, 23, tzinfo=timezone.utc)
    _, rows = scheduled_fixture(start=start)
    # +82 entry has an +81m05s decision. At this interval length the decision
    # is exactly 25% elapsed; one second longer keeps it in quartile 1.
    duration = timedelta(minutes=81, seconds=5) * 4
    for extra, expected in ((timedelta(0), 2), (timedelta(seconds=1), 1), (-timedelta(seconds=1), 2)):
        session = CalendarSession("ES", date(2022, 1, 10), (TradingStateInterval("OPEN", start, start + duration + extra),))
        grid = synthetic_globex_hourly_opportunity_grid(session, calendar_release_id="a" * 64)
        assert build_feature_record(rows, grid[1])["quartile"] == expected


def test_serialized_refit_matches_inner_kernel_and_training_only_transforms():
    dataset = synthetic_dataset()
    x, y = dataset.features.copy(), dataset.terminal_ticks
    x[:500, 1] = 1  # Constant only on training; changing outer values cannot retain it.
    x[:500, 2] = x[:500, 0]
    candidate = next(c for c in candidate_definitions() if c["pack"] == "existing_18")
    predicted, columns = fit_predict(x[:500], y[:500], x[500:], candidate)
    model = fitted_model(x[:500], y[:500], candidate)
    assert 1 not in columns and 2 not in columns
    np.testing.assert_allclose(apply_fitted(model, x[500:]), predicted, rtol=1e-12, atol=1e-12)
    changed = x[500:].copy(); changed[:, 1:3] = 1000000
    np.testing.assert_array_equal(apply_fitted(model, changed), apply_fitted(model, x[500:]))
    with pytest.raises(IntegrityError): apply_fitted({**model, "intercept_ticks": 999}, x[500:])
    with pytest.raises(IntegrityError): replace(dataset, stress_cost_ticks=dataset.stress_cost_ticks * .5)


def test_trial_burden_preserves_prior_and_deduplicates():
    e = dict(trial_id="old", count=2, included_in_prior_floor=False, configuration_ids=[])
    assert trial_burden(181, [e, e])["combined_burden"] == 291
    new = dict(trial_id="successor", count=1, included_in_prior_floor=False, configuration_ids=["ES/" + candidate_definitions()[0]["candidate_id"]])
    assert trial_burden(181, [new])["combined_burden"] == 289
    with pytest.raises(IntegrityError): trial_burden(180, [])
    with pytest.raises(IntegrityError): trial_burden(181, [e, {**e, "count": 3}])


def test_joint_complete_scope_missing_census_and_no_macro(fitted):
    dataset, run = fitted
    report = evaluation(dataset, run)
    results = {m: terminal_failure(m, reason="synthetic economic failure") for m in MARKETS}
    results["ES"] = report
    burden = trial_burden(181, [])
    qualified = qualify_markets(results, burden=burden, dsr_censuses={})
    assert "COMPLETE_DSR_CENSUS_UNAVAILABLE" in qualified["market_decisions"]["ES"]["reasons"]
    assert len(qualified["declared_hypothesis_ids"]) == 24
    assert all(qualified["adjusted_p_values"][f"NQ:{b}"] == 1 for b in BASELINES)
    assert qualified["phase_12_eligible_markets"] == []
    with pytest.raises(IntegrityError): qualify_markets({k: v for k, v in results.items() if k != "NQ"}, burden=burden, dsr_censuses={})
    with pytest.raises(IntegrityError): qualify_markets({**results, "ZN": results["NQ"]}, burden=burden, dsr_censuses={})
    with pytest.raises(IntegrityError): downstream_eligibility(qualified, {"NQ": "PASS"})


def test_independent_market_pass_and_downstream_without_nq_veto(fitted):
    dataset, run = fitted
    report = evaluation(dataset, run)
    burden = trial_burden(181, [])
    y = np.array(report["daily_stress_net_ticks"])
    sharpe = float(np.mean(y) / np.std(y, ddof=1))
    census = {"burden_id": burden["census_id"], "result_id": report["result_id"],
              "trial_ids": [f"synthetic-trial-{i}" for i in range(289)],
              "trial_sharpes": [*np.linspace(-.1, .1, 288), sharpe], "selected_trial_index": 288}
    results = {m: terminal_failure(m, reason="synthetic failure") for m in MARKETS}
    results["ES"] = report
    q = qualify_markets(results, burden=burden, dsr_censuses={"ES": census})
    assert q["market_decisions"]["ES"]["qualified"], q["market_decisions"]
    assert not q["market_decisions"]["NQ"]["qualified"]
    assert downstream_eligibility(q, {"ES": "PASS"})["refit_without_evaluation"] == ["ES"]
    assert downstream_eligibility(q, {"ES": "FAIL"})["refit_without_evaluation"] == []
    # Each market fits on its own arrays; ES economic failure has no veto.
    nq_data = replace(dataset, market="NQ", sample_ids=tuple(s.replace("ES", "NQ") for s in dataset.sample_ids))
    nq_run = run_market_wfa(nq_data)
    nq_report = evaluation(nq_data, nq_run)
    results = {m: terminal_failure(m, reason="synthetic failure") for m in MARKETS}
    results["NQ"] = nq_report
    q = qualify_markets(results, burden=burden, dsr_censuses={"NQ": {**census, "result_id": nq_report["result_id"]}})
    assert q["phase_12_eligible_markets"] == ["NQ"]
    assert not q["authority"]["real_history"]


def test_prepared_contract_certificate_and_documentation_agree():
    from futures_rebuild.fixed_hour_terminal_preparation import CONFIG, validate_bundle

    root = Path(__file__).resolve().parents[1]
    bundle = json.loads((root / CONFIG).read_bytes())
    # Preserved predecessor evidence does not certify the corrected cost interface.
    with pytest.raises(IntegrityError, match="drifted"):
        validate_bundle(bundle, root)
    assert set(bundle["phase_contracts"]) == {"3", "4", "5", "6", "ladder"}
    for phase in bundle["phase_contracts"].values():
        assert phase["mechanism_id"] == bundle["mechanism_id"]
        assert not any(phase["authority"].values())
    evidence = json.loads((root / bundle["synthetic_certificate"]["path"]).read_bytes())
    assert evidence["state"] == "PASSED_ENGINEERING_ONLY"
    assert set(evidence["market_runs"]) == set(MARKETS)
    assert evidence["downstream"]["refit_without_evaluation"] == ["CL"]
    for counts in evidence["construction"].values():
        assert counts == dict(scheduled=3030, insufficient_history=1010, valid_feature=2020, invalid_outcome=0, quality_errors=0)
    for document in ("PROJECT_OUTLINE.md", "CURRENT_WORKFLOW.md"):
        assert CONFIG.as_posix() in (root / document).read_text(encoding="utf-8")
