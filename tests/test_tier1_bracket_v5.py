from __future__ import annotations

import inspect
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_bracket_post_audit import CausalBar
from futures_rebuild.tier1_bracket_v4 import BracketFill, MarketSpec
from futures_rebuild.tier1_bracket_v4 import (
    DirectionOutcomes, ExpectedCheckpoint, FEATURE_NAMES, FrozenPrediction,
)
from futures_rebuild.tier1_bracket_v5 import (
    CalendarSessionSpec,
    AccountPathV5,
    CrossfitEvidenceBundleV5,
    CrossfitStatisticSeriesV5,
    CoverageEvidence,
    EvidenceArtifactsV5,
    V4_BOUND_PATHS,
    V4_TRIAL_ID,
    apply_continuous_risk_v5,
    authorized_source_streams_v5,
    bootstrap_sensitivity_v5,
    block_sensitivity_plan_v5,
    build_evidence_manifest_v5,
    build_expected_census_from_calendar,
    classify_power_and_effect_v5,
    conventional_dsr_status,
    entries_overlap_v5,
    evaluate_coverage_gate,
    iter_source_records_from_parquet,
    load_v5_contract,
    materialize_v5_rows,
    MaterializedRowV5,
    prepare_runtime_receipt_v5,
    prepare_v4_retirement_v5,
    prepare_v5_registration,
    require_historical_calendar_index_span_v5,
    claim_historical_operation_receipt_v5,
    persist_evidence_bundle_v5,
    plan_strategy_v5,
    portfolio_training_power_v5,
    risk_matched_always_long_eligible,
    PlannedTradeV5,
    OpportunityRecordV5,
    segmented_account_views_v5,
    simulate_independent_strategy_paths_v5,
    build_v5_folds_from_census,
    CensusCheckpoint,
    evaluate_strategies_v5,
    derive_v5_decision,
    fit_predict_v5,
    finalize_candidate_ledger_v5,
    REQUIRED_ACTIVE_STRATEGIES_V5,
    source_record_from_mapping,
    training_bootstrap_power_v5,
    verify_historical_operation_receipt_v5,
)


ROOT = Path(__file__).parents[1]
D = Decimal
MINUTE = 60_000_000_000


def _bar(event: int, open_: str, high: str, low: str, close: str, executable: bool = True) -> CausalBar:
    return CausalBar(
        event, event + MINUTE, event + MINUTE + 5_000_000_000,
        D(open_), D(high), D(low), D(close), executable,
    )


def _source_row(*, disposition: object = "ELIGIBLE") -> dict[str, object]:
    return {
        "actual_identity_hash": "a" * 64,
        "close_nano": 100_000_000_000,
        "disposition": disposition,
        "event_at_ns": 1,
        "exchange_session_date": "2022-01-03",
        "high_nano": 101_000_000_000,
        "low_nano": 99_000_000_000,
        "open_nano": 100_000_000_000,
        "point_value": "50",
        "source_row_sha256": "b" * 64,
        "tick_size": "0.25",
        "tick_value": "12.5",
        "volume": 10,
    }


def test_v5_contract_restores_mandate_and_preparations_preserve_v4() -> None:
    contract = load_v5_contract(root=ROOT)
    assert contract["risk"]["continuous_drawdown_threshold_usd"] == "1500"
    assert contract["risk"]["maximum_planned_initial_loss_usd"] == "250"
    before = {path: (ROOT / path).read_bytes() for path in V4_BOUND_PATHS}
    retirement = prepare_v4_retirement_v5(root=ROOT)
    assert retirement.canonical_payload["research_evidence_contaminated"] is False
    pointer = ROOT / "configs/tier1_historical_checkpoint_calendar_v5.json"
    if pointer.exists():
        prepared = prepare_v5_registration(root=ROOT)
        assert prepared.canonical_payload["calendar_release_id"]
        bindings = prepared.canonical_payload["bindings"]
        assert isinstance(bindings, dict)
        assert {
            "src/futures_rebuild/data_layout.py",
            "src/futures_rebuild/errors.py",
            "src/futures_rebuild/locking.py",
            "src/futures_rebuild/research/contracts.py",
            "src/futures_rebuild/research/dsr.py",
            "src/futures_rebuild/research/power.py",
        } <= set(bindings)
    else:
        with pytest.raises(IntegrityError):
            prepare_v5_registration(root=ROOT)
    assert before == {path: (ROOT / path).read_bytes() for path in V4_BOUND_PATHS}


def test_v5_registration_calendar_span_requires_gapless_2018_2022() -> None:
    release = "c" * 64

    def manifest(segments: list[dict[str, str]]) -> dict[str, object]:
        return {
            "release_kind": "exchange_calendar_index",
            "release_id": release,
            "embedded_documents": {
                "exchange_calendar_index.json": {"segments": segments}
            },
        }

    require_historical_calendar_index_span_v5(
        manifest=manifest([
            {
                "effective_from_trade_date": "2017-12-31",
                "effective_through_trade_date": "2023-01-01",
            }
        ]),
        expected_release_id=release,
    )
    with pytest.raises(
        IntegrityError,
        match="QUALIFIED_HISTORICAL_CALENDAR_SOURCE_NOT_ESTABLISHED",
    ):
        require_historical_calendar_index_span_v5(
            manifest=manifest([
                {
                    "effective_from_trade_date": "2018-01-01",
                    "effective_through_trade_date": "2020-12-31",
                },
                {
                    "effective_from_trade_date": "2021-01-02",
                    "effective_through_trade_date": "2022-12-31",
                },
            ]),
            expected_release_id=release,
        )


@pytest.mark.parametrize("disposition", [None, "", "UNKNOWN", "QUARANTINED_PENDING_REVALIDATION"])
def test_missing_unknown_or_nontradable_disposition_never_executes(disposition: object) -> None:
    row = _source_row(disposition=disposition)
    record = source_record_from_mapping(market="ES", row=row)
    assert record.executable is False
    assert record.bar is not None and record.bar.executable is False


def test_calendar_census_retains_a_session_with_no_price_rows() -> None:
    release = "c" * 64
    sessions = (
        CalendarSessionSpec("ES", "2022-01-03", 0, 10**20, release),
        CalendarSessionSpec("ES", "2022-01-04", 0, 10**20, release),
    )
    census = build_expected_census_from_calendar(sessions=sessions)
    assert len(census) == 6
    assert {item.expected.exchange_session_date for item in census} == {
        "2022-01-03", "2022-01-04"
    }
    assert all(item.calendar_release_id == release for item in census)


def test_missing_entire_price_session_becomes_three_explicit_abstentions() -> None:
    release = "c" * 64
    session = CalendarSessionSpec(
        "ES", "2022-01-03", 0, 10**20, release,
        {checkpoint: True for checkpoint in ("08:30", "10:30", "13:30")},
    )
    census = build_expected_census_from_calendar(sessions=(session,))
    rows = materialize_v5_rows(
        source_rows=(), census=census,
        market_specs={"ES": MarketSpec(D("0.25"), D("12.5"), D("50"))},
        contract=load_v5_contract(root=ROOT),
        prediction_scope_sessions=("2022-01-03",),
    )
    assert len(rows) == 3
    assert {row.ledger.terminal_disposition for row in rows} == {"MISSING_SOURCE_SESSION"}
    assert all(not row.ledger.prediction_produced for row in rows)


def test_risk_ineligible_checkpoint_keeps_prediction_then_abstains_from_order() -> None:
    expected = ExpectedCheckpoint("risk", "ES", 2022, "2022-01-03", "08:30", 100)
    row = MaterializedRowV5(
        expected,
        OpportunityRecordV5(
            "risk", "ES", "2022-01-03", "08:30", 100,
            "PREDICTION_PRODUCED", True, 1, 2,
            outcome_coverage="NOT_APPLICABLE_RISK_INELIGIBLE",
        ),
        {name: 0.0 for name in FEATURE_NAMES}, D("10"), "a" * 64,
        None, (), MarketSpec(D("0.25"), D("12.5"), D("50")), False,
    )
    prediction = FrozenPrediction(
        "risk", "ES", 2022, "2022-01-03", "08:30", 0,
        0.5, -0.5, "long", 0.5, "long", 0.1, 0.01,
    )
    plan = plan_strategy_v5(
        strategy="candidate", predictions=(prediction,), rows=(row,),
        scenario="stress",
    )
    assert not plan.trades
    assert plan.preliminary_terminals == {"risk": "RISK_CAP_REJECTION"}


def test_continuous_risk_uses_favorable_peak_and_exit_bar_adverse_mark() -> None:
    fill = BracketFill(
        0, 0, D("100"), D("120"), D("90"), D("120"), "TARGET",
        D("2010"), D("10"), D("2000"), D("250"),
    )
    result = apply_continuous_risk_v5(
        fill=fill, direction="long",
        path=(
            _bar(0, "100", "120", "100", "119"),
            _bar(MINUTE, "99", "100", "98", "99"),
        ),
        spec=MarketSpec(D("1"), D("100"), D("100")),
        realized_equity=D("100000"), prior_peak_equity=D("100000"),
        session_start_equity=D("100000"), daily_limit=D("5000"),
        drawdown_limit=D("1500"),
    )
    assert result.complete
    assert result.risk_breach == "DRAWDOWN"
    assert result.fill is not None
    assert result.fill.reason == "RISK_LIQUIDATION_DRAWDOWN"
    assert result.fill.exit_at_ns == MINUTE
    assert result.fill.gross_pnl_usd - result.fill.costs_usd == result.fill.net_pnl_usd
    assert result.maximum_drawdown_usd >= D("2000")
    assert [kind for _, kind, _ in result.equity_marks[:2]] == [
        "FAVORABLE_EXTREME", "ADVERSE_EXTREME"
    ]


def test_risk_breach_without_next_causal_price_is_incomplete() -> None:
    fill = BracketFill(
        0, 0, D("100"), D("120"), D("90"), D("120"), "TARGET",
        D("2010"), D("10"), D("2000"), D("250"),
    )
    result = apply_continuous_risk_v5(
        fill=fill, direction="long", path=(_bar(0, "100", "120", "100", "119"),),
        spec=MarketSpec(D("1"), D("100"), D("100")),
        realized_equity=D("100000"), prior_peak_equity=D("100000"),
        session_start_equity=D("100000"), daily_limit=D("5000"),
        drawdown_limit=D("1500"),
    )
    assert not result.complete and result.fill is None
    assert result.risk_breach == "DRAWDOWN"


def test_equal_timestamp_overlap_fails_closed() -> None:
    assert entries_overlap_v5(candidate_entry_at_ns=100, prior_exit_at_ns=100)
    assert not entries_overlap_v5(
        candidate_entry_at_ns=100, prior_exit_at_ns=100,
        prior_exit_proven_at_bar_open=True,
    )


def test_admitted_ledger_requires_causal_order_and_fill_timestamps() -> None:
    with pytest.raises(Exception, match="order/fill"):
        OpportunityRecordV5(
            "x", "ES", "2022-01-03", "08:30", 100,
            "ADMITTED_TRADE", True, 1, 2,
            outcome_coverage="COMPLETE",
        ).validate()
    OpportunityRecordV5(
        "x", "ES", "2022-01-03", "08:30", 100,
        "ADMITTED_TRADE", True, 1, 2, 101, 160,
        "COMPLETE",
    ).validate()


def test_coverage_gate_requires_complete_ledger_and_minimum_rates() -> None:
    expected_by_market_year = {
        f"{market}/{year}": 100
        for market in ("ES", "CL", "ZN", "6E")
        for year in (2020, 2021, 2022)
    }
    eligible_by_market_year = {
        key: 95 for key in expected_by_market_year
    }
    passed = evaluate_coverage_gate(CoverageEvidence(
        expected=1300, terminal=1300, causal_feature_expected=1200,
        causal_feature_eligible=1140, predictions=1129,
        market_year_expected=expected_by_market_year,
        market_year_feature_eligible=eligible_by_market_year,
    ))
    assert passed["status"] == "PASS"
    failed = evaluate_coverage_gate(CoverageEvidence(
        expected=1300, terminal=1299, causal_feature_expected=1200,
        causal_feature_eligible=1140, predictions=1129,
        market_year_expected=expected_by_market_year,
        market_year_feature_eligible=eligible_by_market_year,
    ))
    assert failed["status"] == "INCONCLUSIVE_DATA_OR_COVERAGE"


def test_registered_power_resamples_and_effect_classification_are_separate() -> None:
    evidence = training_bootstrap_power_v5(
        [0.0001 * ((index % 7) - 3) for index in range(80)],
        planned_evaluation_observations=60, alternative_mean=0.0003,
        resamples=5000, mean_block_length=10, seed=7,
    )
    assert evidence.resamples == 5000
    assert block_sensitivity_plan_v5() == (5, 10, 20)
    assert classify_power_and_effect_v5(
        power_adequate=True, complete_clusters=60,
        effect_mean_usd=D("-1"), confidence_upper_usd=D("5"),
        confidence_lower_usd=D("-5"), mees_usd=D("20"),
    ) == "FAIL_NO_EDGE"
    assert classify_power_and_effect_v5(
        power_adequate=False, complete_clusters=12,
        effect_mean_usd=D("-1"), confidence_upper_usd=D("5"),
        confidence_lower_usd=D("-5"), mees_usd=D("20"),
    ) == "FAIL_NO_EDGE"
    assert classify_power_and_effect_v5(
        power_adequate=False, complete_clusters=12,
        effect_mean_usd=D("10"), confidence_upper_usd=D("15"),
        confidence_lower_usd=D("-5"), mees_usd=D("20"),
    ) == "FAIL_NOT_ECONOMIC"


def test_crossfit_power_uses_registered_30_dollar_alternative() -> None:
    series = CrossfitStatisticSeriesV5(
        "CANDIDATE_MINUS_REQUIRED_BASELINE_SESSION_RETURN",
        "NESTED_CHRONOLOGICAL_CROSSFIT_TRAIN",
        tuple(0.0001 * ((index % 5) - 2) for index in range(60)),
        tuple(index // 10 for index in range(60)),
    )
    power = portfolio_training_power_v5(
        series, planned_evaluation_observations=60, seed=9,
    )
    assert power.resamples == 5000
    assert power.alternative_mean == pytest.approx(30 / 100000)


def test_bootstrap_runs_all_registered_block_sensitivities() -> None:
    values = [0.001 * ((index % 9) - 4) for index in range(40)]
    results = bootstrap_sensitivity_v5(
        values,
        seed=11, resamples=10000,
    )
    assert [item.mean_block_length for item in results] == [5, 10, 20]
    assert all(item.resamples == 10000 for item in results)
    assert results == bootstrap_sensitivity_v5(values, seed=11, resamples=10000)


def test_degenerate_inference_returns_fail_closed_decision_instead_of_crashing() -> None:
    sessions = tuple(f"2020-01-{index + 1:02d}" for index in range(30))
    empty = AccountPathV5(
        "empty", (), {}, (), {}, D("100000"), D("0"), True,
    )
    scenario = {name: empty for name in REQUIRED_ACTIVE_STRATEGIES_V5}
    evaluation = {name: scenario for name in ("base", "stress", "extreme")}
    baseline_ids = REQUIRED_ACTIVE_STRATEGIES_V5[1:]
    sleeve_ids = tuple(
        f"{market}/{checkpoint}/{direction}"
        for market in ("ES", "CL", "ZN", "6E")
        for checkpoint in ("08:30", "10:30", "13:30")
        for direction in ("long", "short")
    )
    crossfit = CrossfitEvidenceBundleV5(
        tuple(f"2019-01-{index + 1:02d}" for index in range(30)),
        tuple(index // 5 for index in range(30)),
        {name: (0.0,) * 30 for name in baseline_ids},
        {name: (0.0,) * 30 for name in sleeve_ids},
    )
    required_coverage = {
        f"{market}/{year}": 3
        for market in ("ES", "CL", "ZN", "6E")
        for year in (2020, 2021, 2022)
    }
    coverage = CoverageEvidence(
        expected=36, terminal=36, causal_feature_expected=36,
        causal_feature_eligible=36, predictions=36,
        market_year_expected=required_coverage,
        market_year_feature_eligible=required_coverage,
    )
    decision = derive_v5_decision(
        evaluation=evaluation, evaluation_sessions=sessions,
        coverage=coverage, crossfit=crossfit, seed=17,
    )
    assert decision["classification"] == "FAIL_NO_EDGE"
    assert decision["baseline_romano_wolf_status"].endswith("FAIL_CLOSED")
    assert decision["sleeve_romano_wolf_status"].endswith("FAIL_CLOSED")


def test_dsr_proxy_is_not_claimed_and_baseline_is_really_risk_capped() -> None:
    assert conventional_dsr_status(observed_trial_sharpes=None).startswith("NOT_CLAIMED")
    assert conventional_dsr_status(observed_trial_sharpes=(0.1, 0.2)) == "ELIGIBLE_FOR_CONVENTIONAL_DSR"
    assert risk_matched_always_long_eligible(planned_loss_usd=D("250"))
    assert not risk_matched_always_long_eligible(planned_loss_usd=D("250.01"))


def test_parquet_reader_is_batch_streaming_not_whole_file_materialization() -> None:
    source = inspect.getsource(iter_source_records_from_parquet)
    assert ".iter_batches(" in source
    assert ".to_pylist(" not in source
    assert ".read(" not in source


def test_boolean_like_local_authority_cannot_open_historical_execution(tmp_path: Path) -> None:
    boundary = RepoBoundary(ROOT)
    local = OperationReceipt.issue_local(
        boundary, operation="EXECUTE_TIER1_BRACKET_SUCCESSOR_V5_HISTORICAL_SCREEN",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
        scope={"trial_id": "d" * 64},
    )
    with pytest.raises(UnauthorizedOperation):
        verify_historical_operation_receipt_v5(
            boundary=boundary, receipt=local, trial_id="d" * 64,
            source_binding_id="e" * 64, output_root=tmp_path,
        )


def test_2025_source_path_is_rejected_before_registry_or_file_open(tmp_path: Path) -> None:
    boundary = RepoBoundary(tmp_path)
    receipt = OperationReceipt.issue_local(
        boundary, operation="synthetic",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    with pytest.raises(UnauthorizedOperation, match="2025"):
        authorized_source_streams_v5(
            root=tmp_path, boundary=boundary, receipt=receipt,
            trial_id="f" * 64,
            source_paths={("ES", 2025): tmp_path / "must-not-open.parquet"},
            output_root=tmp_path / "output",
        )


def test_external_historical_receipt_is_consumed_create_only(tmp_path: Path) -> None:
    boundary = RepoBoundary(tmp_path)
    command = "EXECUTE_TIER1_V5"
    plan_id = "1" * 64
    plan_sha = "2" * 64
    output = tmp_path / "research-output"
    scope = {
        "trial_id": "d" * 64,
        "source_binding_id": "e" * 64,
        "output_root": output.as_posix(),
        "holdout_or_forward_access": "false",
        "provider_access": "false",
        "publication": "false",
    }
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation="EXECUTE_TIER1_BRACKET_SUCCESSOR_V5_HISTORICAL_SCREEN",
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope, approval_command=command, approval_plan_id=plan_id,
        approval_plan_sha256=plan_sha,
        approval_line=f"APPROVE {command} PLAN {plan_id} SHA256 {plan_sha}",
    )
    claim = claim_historical_operation_receipt_v5(
        root=tmp_path, boundary=boundary, receipt=receipt,
        trial_id="d" * 64, source_binding_id="e" * 64,
        output_root=output,
    )
    assert claim.exists()
    with pytest.raises(UnauthorizedOperation, match="already consumed"):
        claim_historical_operation_receipt_v5(
            root=tmp_path, boundary=boundary, receipt=receipt,
            trial_id="d" * 64, source_binding_id="e" * 64,
            output_root=output,
        )


def test_evidence_manifest_binds_predictions_fills_marks_and_runtime() -> None:
    trial_id = "f" * 64
    runtime = prepare_runtime_receipt_v5(root=ROOT, trial_id=trial_id)
    artifacts = EvidenceArtifactsV5(
        model={"id": "m"}, predictions=({"opportunity_id": "p"},),
        opportunity_ledger=({"opportunity_id": "p", "terminal": "ADMITTED"},),
        fills=({"opportunity_id": "p", "net": "1"},),
        continuous_equity_marks=({"at": 1, "equity": "100001"},),
        segmented_metrics={"folds": []}, inference={"status": "OK"},
        decision={"classification": "FAIL_NO_EDGE"}, runtime_receipt=runtime,
    )
    manifest = build_evidence_manifest_v5(trial_id=trial_id, artifacts=artifacts)
    assert set(manifest["files"]) == {
        "continuous_equity_marks.json", "decision.json", "fills.json",
        "inference.json", "model.json", "opportunity_ledger.json",
        "predictions.json", "runtime_receipt.json", "segmented_metrics.json",
    }
    assert len(manifest["manifest_id"]) == 64


def test_evidence_bundle_persists_every_hash_create_only(tmp_path: Path) -> None:
    trial_id = "f" * 64
    artifacts = EvidenceArtifactsV5(
        model={"id": "m"}, predictions=({"opportunity_id": "p"},),
        opportunity_ledger=({"opportunity_id": "p", "terminal": "ADMITTED"},),
        fills=({"opportunity_id": "p", "net": "1"},),
        continuous_equity_marks=({"at": 1, "equity": "100001"},),
        segmented_metrics={"folds": []}, inference={"status": "OK"},
        decision={"classification": "FAIL_NO_EDGE"},
        runtime_receipt={"dependency_lock_receipt_id": "a" * 64},
    )
    boundary = RepoBoundary(tmp_path)
    first = persist_evidence_bundle_v5(
        boundary=boundary, output_root=tmp_path / "evidence",
        trial_id=trial_id, artifacts=artifacts,
    )
    assert Path(first["manifest_path"]).exists()
    with pytest.raises(Exception, match="create-only"):
        persist_evidence_bundle_v5(
            boundary=boundary, output_root=tmp_path / "evidence",
            trial_id=trial_id, artifacts=artifacts,
        )


def _planned_trade(
    opportunity_id: str, *, market: str, year: int, session: str,
    entry_at: int, pnl: str,
) -> PlannedTradeV5:
    fill = BracketFill(
        entry_at, entry_at + MINUTE, D("100"), D("100"), D("98"), D("104"),
        "TIMEOUT", D(pnl) + D("10"), D("10"), D(pnl), D("250"),
    )
    return PlannedTradeV5(
        opportunity_id, market, year, session, "08:30", "long", D("1"),
        fill,
        (
            _bar(entry_at, "100", "100.1", "99.9", "100"),
            _bar(entry_at + MINUTE, "100", "100.1", "99.9", "100"),
        ),
        MarketSpec(D("0.1"), D("10"), D("1")),
    )


def test_candidate_and_baseline_have_independent_risk_paths_and_segments() -> None:
    candidate = (
        _planned_trade("c1", market="ES", year=2020, session="2020-01-02", entry_at=0, pnl="-10"),
        _planned_trade("c2", market="ES", year=2021, session="2021-01-04", entry_at=10 * MINUTE, pnl="5"),
    )
    baseline = (
        _planned_trade("b1", market="CL", year=2022, session="2022-01-03", entry_at=0, pnl="5"),
    )
    paths = simulate_independent_strategy_paths_v5(
        plans_by_strategy={"candidate": candidate, "always_long": baseline},
        opportunity_ids_by_strategy={"candidate": ("c1", "c2"), "always_long": ("b1",)},
    )
    assert paths["candidate"].terminal_dispositions["c1"] == "ADMITTED_TRADE"
    assert paths["always_long"].terminal_dispositions["b1"] == "ADMITTED_TRADE"
    assert paths["candidate"].equity_marks is not paths["always_long"].equity_marks
    segments = segmented_account_views_v5(
        strategy="candidate", planned_trades=candidate,
        opportunity_market_year={"c1": ("ES", 2020), "c2": ("ES", 2021)},
    )
    assert set(segments) == {"ES/2020", "ES/2021"}
    assert all(item.ending_equity_usd in {D("99990"), D("100005")} for item in segments.values())


def test_market_specific_model_and_all_strategy_evaluators_integrate_synthetically() -> None:
    sessions = (
        "2018-01-02", "2019-01-02",
        "2020-01-02", "2020-02-03", "2020-03-02",
        "2021-01-04", "2021-02-01", "2021-03-01",
        "2022-01-03", "2022-02-01",
    )
    census: list[CensusCheckpoint] = []
    rows: list[MaterializedRowV5] = []
    for session_index, session in enumerate(sessions):
        year = int(session[:4])
        predict = year >= 2020
        for market_index, market in enumerate(("ES", "CL", "ZN", "6E")):
            for checkpoint_index, checkpoint in enumerate(("08:30", "10:30", "13:30")):
                decision = (session_index + 1) * 10**15 + checkpoint_index * 10**13
                opportunity_id = f"{session}/{market}/{checkpoint}"
                expected = ExpectedCheckpoint(
                    opportunity_id, market, year, session, checkpoint, decision,
                )
                census.append(CensusCheckpoint(expected, True, "c" * 64))
                ledger = OpportunityRecordV5(
                    opportunity_id, market, session, checkpoint, decision,
                    "PREDICTION_PRODUCED" if predict else "TRAINING_OR_PREDICTION_INELIGIBLE",
                    predict, decision - 2 * MINUTE, decision - MINUTE,
                    outcome_coverage="COMPLETE",
                )
                base = 1.0 + session_index * 0.01 + market_index * 0.001 + checkpoint_index * 0.0001
                features = {
                    name: base * (feature_index + 1)
                    for feature_index, name in enumerate(FEATURE_NAMES)
                }
                entry = decision + MINUTE
                long_fill = BracketFill(
                    entry, entry + MINUTE, D("100"), D("101"), D("98"), D("104"),
                    "TARGET", D("100"), D("10"), D("90"), D("250"),
                )
                short_fill = BracketFill(
                    entry, entry + MINUTE, D("100"), D("101"), D("102"), D("96"),
                    "STOP", D("-100"), D("10"), D("-110"), D("250"),
                )
                outcome = DirectionOutcomes(long_fill, short_fill)
                rows.append(MaterializedRowV5(
                    expected, ledger, features, D("1"), "a" * 64,
                    {scenario: outcome for scenario in ("base", "stress", "extreme")},
                    (
                        _bar(entry, "100", "101", "100", "100"),
                        _bar(entry + MINUTE, "101", "101", "100", "101"),
                    ),
                    MarketSpec(D("1"), D("100"), D("100")),
                ))
    folds = build_v5_folds_from_census(census)
    model = fit_predict_v5(rows=rows, folds=folds)
    assert len(model.predictions) == 96
    evaluation = evaluate_strategies_v5(
        predictions=model.predictions, rows=rows,
        strategies=REQUIRED_ACTIVE_STRATEGIES_V5,
    )
    assert set(evaluation) == {"base", "stress", "extreme"}
    for paths in evaluation.values():
        assert set(paths) == set(REQUIRED_ACTIVE_STRATEGIES_V5)
        assert paths["flat_no_trade"].ending_equity_usd == D("100000")
        assert not paths["flat_no_trade"].admitted
        assert all(
            set(path.terminal_dispositions)
            == {prediction.opportunity_id for prediction in model.predictions}
            for path in paths.values()
        )
    final_ledger = finalize_candidate_ledger_v5(
        rows=rows, candidate_path=evaluation["stress"]["candidate"],
    )
    admitted = [record for record in final_ledger if record.terminal_disposition == "ADMITTED_TRADE"]
    assert admitted
    assert all(
        record.decision_at_ns < record.order_submitted_at_ns <= record.fill_at_ns
        for record in admitted
    )
