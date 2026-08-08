from decimal import Decimal
from datetime import date, datetime, time
import json
from pathlib import Path
import shutil
from zoneinfo import ZoneInfo

import pytest
import numpy as np

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_bracket_post_audit import (
    BracketFill,
    CausalBar,
    OpportunityRecord,
    load_post_audit_contract,
)
from futures_rebuild.tier1_bracket_v4 import (
    AuthorizedHistoricalRun,
    DirectionOutcomes,
    EvaluationBundle,
    ExpectedCheckpoint,
    FoldSpec,
    FrozenPrediction,
    MarketSpec,
    MaterializedRow,
    InferenceInputs,
    NEGATIVE_CONTROL_IDS,
    SCENARIOS,
    SourceMinute,
    StrategyPath,
    _risk_adjusted_fill,
    _strategy_signal,
    load_authorized_source_minutes,
    load_v3_retirement_preparation,
    load_v4_contract,
    derive_v4_decision,
    execute_authorized_v4,
    build_expected_census,
    build_v4_folds_from_census,
    materialize_v4_rows,
    prepare_v4_registration,
    prepare_v3_retirement,
    persist_v3_retirement,
    persist_v4_registration,
    run_v4_pipeline,
    run_v4_negative_controls,
    simulate_strategy_path,
    simulate_v4_bracket_fill,
    verify_v3_retirement,
    verify_v4_registration,
)


ROOT = Path(__file__).parents[1]
D = Decimal
MINUTE = 60_000_000_000


def _specs() -> dict[str, MarketSpec]:
    return {
        market: MarketSpec(D("0.25"), D("1"), D("4"))
        for market in ("ES", "CL", "ZN", "6E")
    }


def _source(event: int, price: Decimal, volume: float) -> SourceMinute:
    return SourceMinute(
        "ES",
        "2022-01-03",
        CausalBar(
            event,
            event + MINUTE,
            event + MINUTE + 5_000_000_000,
            price,
            price + D("0.25"),
            price - D("0.25"),
            price,
        ),
        volume,
        "a" * 64,
        sha256_json({"event": event}),
    )


def test_v4_governance_is_pre_data_and_registration_is_deterministic() -> None:
    retirement = load_v3_retirement_preparation(root=ROOT)
    contract = load_v4_contract(root=ROOT)
    first = prepare_v4_registration(root=ROOT)
    second = prepare_v4_registration(root=ROOT)
    retired = prepare_v3_retirement(root=ROOT)

    assert retirement["disposition"] == "INCOMPLETE_PRE_DATA_IMPLEMENTATION_BINDING"
    assert retirement["research_evidence_contaminated"] is False
    assert contract["authority"]["historical_source_row_access_authorized"] is False
    assert first.trial_id == second.trial_id == sha256_json(first.canonical_payload)
    assert canonical_bytes(first.canonical_payload) == canonical_bytes(second.canonical_payload)
    assert len(first.canonical_payload["source_bindings"]) == 20
    assert retired.record_id == sha256_json(retired.canonical_payload)
    assert retired.canonical_payload["research_evidence_contaminated"] is False


def test_retirement_and_registration_publish_create_only_exact_bytes(
    tmp_path: Path,
) -> None:
    retired = prepare_v3_retirement(root=ROOT)
    registered = prepare_v4_registration(root=ROOT)
    paths = set(registered.canonical_payload["bindings"])
    preserved = retired.canonical_payload["preserved_bindings"]
    paths.update(
        (preserved["registry_path"], preserved["event_path"],
         "configs/tier1_bracket_v3_retirement_preparation.json")
    )
    for relative in paths:
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative, destination)

    persist_v3_retirement(root=tmp_path, prepared=retired)
    persist_v4_registration(root=tmp_path, prepared=registered)

    assert verify_v3_retirement(
        root=tmp_path, prepared=retired
    )["record_id"] == retired.record_id
    assert verify_v4_registration(
        root=tmp_path, prepared=registered
    )["trial_id"] == registered.trial_id
    with pytest.raises(IntegrityError, match="create-only"):
        persist_v4_registration(root=tmp_path, prepared=registered)


def test_materializer_uses_two_minute_old_bar_and_keeps_missing_outcome() -> None:
    decision = 100 * MINUTE
    rows = [
        _source(event, D("100") + D(index) / D("100"), float(index + 1))
        for index, event in enumerate(range(decision - 62 * MINUTE, decision, MINUTE))
    ]
    rows.extend(
        _source(event, D("101"), 100.0 + index)
        for index, event in enumerate(
            range(decision + MINUTE, decision + 62 * MINUTE, MINUTE)
        )
    )
    expected = ExpectedCheckpoint(
        "opportunity", "ES", 2022, "2022-01-03", "08:30", decision
    )

    materialized = materialize_v4_rows(
        source_rows=rows,
        expected=(expected,),
        market_specs=_specs(),
        contract=load_post_audit_contract(root=ROOT),
        prediction_scope_sessions=("2022-01-03",),
    )[0]

    assert materialized.ledger.prediction_produced
    assert materialized.ledger.feature_available_at_ns <= decision
    assert materialized.ledger.feature_event_at_ns == decision - 2 * MINUTE
    assert materialized.outcomes is not None
    assert all(
        getattr(materialized.outcomes[name], direction).entry_at_ns > decision
        for name in SCENARIOS
        for direction in ("long", "short")
    )

    missing = materialize_v4_rows(
        source_rows=rows[:20],
        expected=(expected,),
        market_specs=_specs(),
        contract=load_post_audit_contract(root=ROOT),
        prediction_scope_sessions=("2022-01-03",),
    )[0]
    assert missing.ledger.terminal_disposition == "INSUFFICIENT_CAUSAL_HISTORY"
    assert not missing.ledger.prediction_produced

    future_missing = materialize_v4_rows(
        source_rows=rows[:62],
        expected=(expected,),
        market_specs=_specs(),
        contract=load_post_audit_contract(root=ROOT),
        prediction_scope_sessions=("2022-01-03",),
    )[0]
    assert future_missing.ledger.prediction_produced
    assert future_missing.outcomes is None
    assert future_missing.ledger.outcome_coverage == "MISSING"


def test_flat_baseline_is_exact_zero_and_has_no_entries() -> None:
    path = simulate_strategy_path(
        strategy="flat_no_trade",
        predictions=(),
        rows_by_id={},
        scenario="stress",
        complete_sessions=("2022-01-03", "2022-01-04"),
    )
    assert path.admitted == ()
    assert path.metrics["net_pnl_usd"] == "0"
    assert path.metrics["maximum_drawdown_usd"] == "0"
    assert path.metrics["turnover_contract_equivalents"] == "0"


def test_candidate_ranking_ablation_is_not_the_candidate_schedule() -> None:
    prediction = FrozenPrediction(
        "x", "ES", 2022, "2022-01-03", "08:30", 0,
        0.5, -0.2, "long", 0.5, "long", 0.1, 0.01,
    )
    assert _strategy_signal(prediction, "candidate") == ("long", 0.5)
    assert _strategy_signal(
        prediction, "candidate_signal_market_order_ranking_ablation"
    ) == ("long", 0.0)


def test_registered_fold_builder_covers_evaluation_once_with_embargo() -> None:
    sessions = ("2018-01-02", "2019-01-02") + tuple(
        f"2020-01-{day:02d}" for day in range(2, 10)
    )
    expected = build_expected_census(
        sessions=tuple(
            (
                int(session[:4]), session,
                {"08:30": index * 100 + 1, "10:30": index * 100 + 2,
                 "13:30": index * 100 + 3},
            )
            for index, session in enumerate(sessions)
        )
    )
    folds = build_v4_folds_from_census(expected)
    tests = [session for fold in folds for session in fold.test_sessions]
    assert len(folds) == 8
    assert tests == list(sessions[2:])
    assert len(tests) == len(set(tests))
    assert all(
        set(fold.training_sessions).isdisjoint(fold.test_sessions)
        and fold.training_sessions[-1] < fold.test_sessions[0]
        for fold in folds
    )


def test_intratrade_mark_forces_next_bar_risk_liquidation() -> None:
    expected = ExpectedCheckpoint("x", "ES", 2022, "2022-01-03", "08:30", 0)
    ledger = OpportunityRecord(
        "x", "ES", "2022-01-03", "08:30", 0, "PREDICTION_PRODUCED", True,
        feature_event_at_ns=-2, feature_available_at_ns=-1,
    )
    fill = BracketFill(
        MINUTE, 4 * MINUTE, D("100"), D("100"), D("80"), D("120"),
        "TIMEOUT", D("10"), D("10"), D("0"), D("500"),
    )
    path = (
        CausalBar(MINUTE, 2 * MINUTE, 2 * MINUTE + 5, D("100"), D("101"), D("85"), D("90")),
        CausalBar(2 * MINUTE, 3 * MINUTE, 3 * MINUTE + 5, D("84"), D("85"), D("83"), D("84")),
    )
    row = MaterializedRow(
        expected, ledger, {"bar_return_1": 0.0}, D("1"), "b" * 64, None,
        path, MarketSpec(D("1"), D("1"), D("100")),
    )

    adjusted = _risk_adjusted_fill(
        row=row,
        fill=fill,
        direction="long",
        realized_equity=D("100000"),
        peak_equity=D("100000"),
        session_start_equity=D("100000"),
    )
    assert adjusted.reason == "RISK_LIQUIDATION_DAILY"
    assert adjusted.exit_at_ns == 2 * MINUTE
    assert adjusted.net_pnl_usd == D("-1610")


def test_v4_target_is_two_r_net_after_fees_and_both_sides_slippage() -> None:
    entry = CausalBar(100, 160, 165, D("100"), D("100"), D("100"), D("100"))
    target = CausalBar(160, 220, 225, D("102"), D("146"), D("102"), D("146"))
    fill = simulate_v4_bracket_fill(
        direction="long", decision_at_ns=0, entry_bar=entry,
        path_bars=(target,), atr=D("1"), tick_size=D("1"),
        tick_value=D("1"), point_value=D("1"), fee_per_side_usd=D("5"),
        round_trip_cost_ticks=4,
    )
    assert fill.reason == "TARGET"
    assert fill.planned_initial_loss_usd == D("16")
    assert fill.net_pnl_usd == D("32")
    assert fill.gross_pnl_usd - fill.costs_usd == fill.net_pnl_usd


def test_negative_control_set_and_holdout_guard_fail_closed_before_open() -> None:
    assert len(NEGATIVE_CONTROL_IDS) == 6
    authorization = AuthorizedHistoricalRun("missing", True, True, True, True)
    with pytest.raises(UnauthorizedOperation, match="2025 holdout"):
        load_authorized_source_minutes(
            root=ROOT,
            authorization=authorization,
            source_paths={("ES", 2025): Path("must-not-be-opened.parquet")},
        )
    with pytest.raises(UnauthorizedOperation, match="not authorized"):
        load_authorized_source_minutes(
            root=ROOT,
            authorization=AuthorizedHistoricalRun("missing", False, False, False, False),
            source_paths={},
        )


def test_degenerate_no_trade_history_returns_one_fail_closed_decision() -> None:
    sessions = tuple(f"2022-02-{day:02d}" for day in range(1, 31))
    pnl = {session: D("0") for session in sessions}
    metrics = {
        "complete_sessions": 30,
        "incomplete_sessions": 0,
        "net_pnl_usd": "0",
        "annualized_daily_sharpe": None,
        "annualized_daily_sortino": None,
        "maximum_drawdown_usd": "0",
        "turnover_contract_equivalents": "0",
    }
    paths = {
        name: StrategyPath(name, (), pnl, metrics, {}, {})
        for name in (
            "candidate", "flat_no_trade",
            "fold_local_unconditional_return_by_market_session",
            "previous_bar_sign_momentum", "previous_bar_sign_reversal",
            "risk_matched_always_long_intraday",
            "candidate_signal_market_order_ranking_ablation",
        )
    }
    evaluation = EvaluationBundle({}, {name: paths for name in SCENARIOS}, sessions, ())
    sleeves = {
        f"{market}/{checkpoint}/{direction}": np.zeros(30, dtype=np.float64)
        for market in ("ES", "CL", "ZN", "6E")
        for checkpoint in ("08:30", "10:30", "13:30")
        for direction in ("long", "short")
    }
    inference = InferenceInputs(
        np.zeros(30, dtype=np.float64), run_v4_negative_controls(),
        sleeves, sleeves, 50, 11,
    )
    decision = derive_v4_decision(evaluation=evaluation, inference=inference)
    assert decision["classification"] == "INCONCLUSIVE_DATA_OR_POWER"
    assert decision["dsr_status"] == "DEGENERATE_CANDIDATE_SERIES_FAIL_CLOSED"


def test_full_statistical_branch_returns_an_ordered_terminal_classification() -> None:
    sessions = tuple(f"s-{index:02d}" for index in range(40))
    candidate_pnl = {
        session: D("150") + (D("50") if index % 2 else D("-50"))
        for index, session in enumerate(sessions)
    }
    zero_pnl = {session: D("0") for session in sessions}
    candidate_metrics = {
        "complete_sessions": 40, "incomplete_sessions": 0,
        "net_pnl_usd": str(sum(candidate_pnl.values(), D("0"))),
        "annualized_daily_sharpe": 1.0, "annualized_daily_sortino": 1.0,
        "maximum_drawdown_usd": "0", "turnover_contract_equivalents": "0",
    }
    zero_metrics = {**candidate_metrics, "net_pnl_usd": "0"}
    paths = {
        "candidate": StrategyPath(
            "candidate", (), candidate_pnl, candidate_metrics, {}, {}
        ),
        **{
            name: StrategyPath(name, (), zero_pnl, zero_metrics, {}, {})
            for name in (
                "flat_no_trade", "fold_local_unconditional_return_by_market_session",
                "previous_bar_sign_momentum", "previous_bar_sign_reversal",
                "risk_matched_always_long_intraday",
                "candidate_signal_market_order_ranking_ablation",
            )
        },
    }
    evaluation = EvaluationBundle({}, {name: paths for name in SCENARIOS}, sessions, ())
    varied = np.asarray(
        [0.0015 + (0.0005 if index % 2 else -0.0005) for index in range(40)],
        dtype=np.float64,
    )
    sleeves = {
        f"{market}/{checkpoint}/{direction}": varied
        for market in ("ES", "CL", "ZN", "6E")
        for checkpoint in ("08:30", "10:30", "13:30")
        for direction in ("long", "short")
    }
    inference = InferenceInputs(
        varied, run_v4_negative_controls(), sleeves, sleeves, 50, 19,
    )
    first = derive_v4_decision(evaluation=evaluation, inference=inference)
    second = derive_v4_decision(evaluation=evaluation, inference=inference)
    assert first["classification"] in {
        "INCONCLUSIVE_DATA_OR_POWER", "FAIL_NO_EDGE", "FAIL_NOT_ECONOMIC",
        "INCONCLUSIVE_EFFECT", "FAIL_MULTIPLICITY_OR_CONTROL",
        "PASS_HISTORICAL_SCREEN",
    }
    assert canonical_bytes(first) == canonical_bytes(second)


def test_synthetic_source_to_terminal_decision_is_deterministic() -> None:
    session_names = tuple(f"2022-01-{day:02d}" for day in range(3, 14))
    schedule = []
    sources: list[SourceMinute] = []
    for day_index, session in enumerate(session_names):
        base = (day_index + 1) * 1_000_000_000_000_000
        decisions = {
            "08:30": base + 150 * MINUTE,
            "10:30": base + 270 * MINUTE,
            "13:30": base + 450 * MINUTE,
        }
        schedule.append((2022, session, decisions))
        for market_index, market in enumerate(("ES", "CL", "ZN", "6E")):
            identity = sha256_json({"market": market, "session": session})
            for minute in range(512):
                event = base + minute * MINUTE
                price = (
                    D("100") + D(market_index * 10) + D(day_index) / D("10")
                    + D(minute) / D("100") + D((minute % 7) - 3) / D("1000")
                )
                sources.append(
                    SourceMinute(
                        market,
                        session,
                        CausalBar(
                            event, event + MINUTE,
                            event + MINUTE + 5_000_000_000,
                            price, price + D("0.25"), price - D("0.25"), price,
                        ),
                        float(100 + minute % 37 + day_index),
                        identity,
                        sha256_json({"market": market, "session": session, "minute": minute}),
                    )
                )
    expected = build_expected_census(sessions=schedule)
    folds = tuple(
        FoldSpec(index, session_names[: index + 3], (session_names[index + 3],))
        for index in range(8)
    )
    kwargs = {
        "source_rows": sources,
        "expected": expected,
        "market_specs": _specs(),
        "contract": load_post_audit_contract(root=ROOT),
        "folds": folds,
        "inference": None,
        "prediction_scope_sessions": session_names[3:],
        "trial_id": "c" * 64,
    }

    first = run_v4_pipeline(**kwargs)
    second = run_v4_pipeline(**kwargs)

    assert len(first.materialized_rows) == 132
    assert len(first.model_fit.predictions) == 96
    assert first.evaluation.complete_sessions == session_names[3:]
    assert first.evaluation.canonical_payload[
        "stress_candidate_opportunity_funnel"
    ]["predictions_awaiting_terminal_decision"] == 0
    assert len(first.evaluation.canonical_payload[
        "stress_candidate_terminal_dispositions"
    ]) == 132
    assert first.decision["classification"] == "INCONCLUSIVE_DATA_OR_POWER"
    assert first.decision["complete_clusters"] == 8
    assert canonical_bytes(first.model_fit.canonical_model_payload) == canonical_bytes(
        second.model_fit.canonical_model_payload
    )
    assert canonical_bytes(first.evaluation.canonical_payload) == canonical_bytes(
        second.evaluation.canonical_payload
    )
    assert canonical_bytes(first.decision) == canonical_bytes(second.decision)


def test_authorized_parquet_adapter_runs_the_same_v4_engine(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    sessions_by_year = {
        2018: ("2018-01-03", "2018-01-04"),
        2019: ("2019-01-03", "2019-01-04"),
        2020: ("2020-01-03", "2020-01-06", "2020-01-07"),
        2021: ("2021-01-04", "2021-01-05", "2021-01-06"),
        2022: ("2022-01-03", "2022-01-04"),
    }
    chicago = ZoneInfo("America/Chicago")
    source_paths: dict[tuple[str, int], Path] = {}
    bindings = []
    for market_index, market in enumerate(("ES", "CL", "ZN", "6E")):
        for year, sessions in sessions_by_year.items():
            records = []
            for session_index, session in enumerate(sessions):
                start = datetime.combine(
                    date.fromisoformat(session), time(6, 0), tzinfo=chicago
                )
                identity = sha256_json({"market": market, "session": session})
                for minute in range(512):
                    event = int(start.timestamp() * 1_000_000_000) + minute * MINUTE
                    price = 100 + market_index * 10 + session_index / 10 + minute / 100
                    records.append(
                        {
                            "exchange_session_date": session,
                            "event_at_ns": event,
                            "open_nano": round(price * 1_000_000_000),
                            "high_nano": round((price + 0.25) * 1_000_000_000),
                            "low_nano": round((price - 0.25) * 1_000_000_000),
                            "close_nano": round(price * 1_000_000_000),
                            "volume": float(100 + minute % 37),
                            "actual_identity_hash": identity,
                            "source_row_sha256": sha256_json(
                                {"market": market, "session": session, "minute": minute}
                            ),
                            "tick_size": 0.25,
                            "tick_value": 1.0,
                            "point_value": 4.0,
                        }
                    )
            path = tmp_path / "synthetic_sources" / market / f"{year}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(records), path)
            source_paths[(market, year)] = path
            bindings.append(
                {
                    "market": market,
                    "year": year,
                    "source_parquet_sha256": sha256_file(path),
                }
            )
    trial_id = "d" * 64
    registry = (
        tmp_path / "state/trial_registry/tier1_bracket_successor_v4"
        / f"{trial_id}.json"
    )
    registry.parent.mkdir(parents=True, exist_ok=True)
    registry.write_text(
        json.dumps(
            {
                "trial_id": trial_id,
                "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
                "source_bindings": bindings,
            }
        ),
        encoding="utf-8",
    )
    contract = tmp_path / "configs/tier1_bracket_post_audit_successor_v3.json"
    contract.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / "configs/tier1_bracket_post_audit_successor_v3.json", contract)

    result = execute_authorized_v4(
        root=tmp_path,
        authorization=AuthorizedHistoricalRun(trial_id, True, True, True, True),
        source_paths=source_paths,
    )

    assert len(result.materialized_rows) == 144
    assert len(result.model_fit.predictions) == 96
    assert result.decision["classification"] == "INCONCLUSIVE_DATA_OR_POWER"
