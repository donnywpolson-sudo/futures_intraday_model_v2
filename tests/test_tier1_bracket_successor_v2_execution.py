from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_materializer import IndexedBracketEconomics
from futures_rebuild.tier1_bracket_successor_v2_execution import (
    CHECKPOINTS,
    FEATURE_NAMES,
    MARKETS,
    _opportunities,
    build_successor_evaluation,
    build_successor_split_plan,
    fit_successor_models,
    materialize_successor_market_year,
)


ROOT = Path(__file__).parents[1]
CHICAGO = ZoneInfo("America/Chicago")
IDENTITY = "a" * 64


def _source_rows() -> list[dict[str, object]]:
    start = datetime(2020, 1, 2, 7, 0, tzinfo=CHICAGO)
    rows = []
    for index in range(470):
        stamp = start + timedelta(minutes=index)
        open_nano = 100_000_000_000 + index * 250_000_000
        rows.append({
            "actual_identity_hash": IDENTITY,
            "close_nano": open_nano + 125_000_000,
            "currency": "USD",
            "disposition": "ELIGIBLE",
            "event_at_ns": int(stamp.astimezone(timezone.utc).timestamp() * 1_000_000_000),
            "exchange_session_date": "2020-01-02",
            "high_nano": open_nano + 500_000_000,
            "low_nano": open_nano - 500_000_000,
            "open_nano": open_nano,
            "point_value": "50",
            "source_row_sha256": f"{index + 1:064x}",
            "tick_size": "0.25",
            "tick_value": "12.5",
            "volume": float(100 + index % 17),
        })
    return rows


def _economics() -> dict[str, IndexedBracketEconomics]:
    item = IndexedBracketEconomics(
        actual_identity_hash=IDENTITY, tick_size=Decimal("0.25"), tick_value=Decimal("12.5"),
        point_value=Decimal("50"), currency="USD", quote_convention_id="USD_PER_POINT",
        economics_release_receipt_id="b" * 64,
    )
    return {IDENTITY: item}


def test_checkpoint_materialization_is_causal_sparse_and_stress_net() -> None:
    features, outcomes = materialize_successor_market_year(
        rows=_source_rows(), market="ES", year=2020, economics=_economics(),
        stress_cost_usd=Decimal("53.10"),
    )
    assert {row["checkpoint"] for row in features} == set(CHECKPOINTS)
    assert len(features) == len(outcomes) == 3
    assert all(set(FEATURE_NAMES) <= set(row) for row in features)
    assert all(row["entry_at_ns"] > row["decision_at_ns"] for row in outcomes)
    assert all(row["stress_round_trip_cost_usd"] == "53.10" for row in outcomes)


def test_materialization_rejects_holdout_scope() -> None:
    with pytest.raises(IntegrityError, match="scope"):
        materialize_successor_market_year(
            rows=_source_rows(), market="ES", year=2025, economics=_economics(),
            stress_cost_usd=Decimal("53.10"),
        )


def _training_fixture() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    sessions = []
    current = date(2018, 1, 2)
    while current.year <= 2022:
        if current.weekday() < 5:
            sessions.append(current.isoformat())
        current += timedelta(days=1)
    features: list[dict[str, object]] = []
    outcomes: list[dict[str, object]] = []
    counter = 0
    for market_index, market in enumerate(MARKETS):
        for session_index, session in enumerate(sessions):
            for checkpoint_index, checkpoint in enumerate(CHECKPOINTS):
                counter += 1
                key = f"{counter:064x}"
                base = 1.0 + market_index * 0.1 + session_index * 0.001 + checkpoint_index * 0.01
                values = {name: base * (feature_index + 1) + ((session_index + feature_index) % 7) * 0.003 for feature_index, name in enumerate(FEATURE_NAMES)}
                year = int(session[:4])
                features.append({
                    "market": market, "year": year, "exchange_session_date": session,
                    "checkpoint": checkpoint, "checkpoint_at_ns": counter * 1_000,
                    "decision_at_ns": counter * 1_000, "actual_identity_hash": IDENTITY,
                    "upstream_source_row_sha256": key, **values,
                })
                long_r = 0.3 + 0.02 * market_index + 0.01 * checkpoint_index + (session_index % 5) * 0.002
                short_r = -0.1 + 0.01 * checkpoint_index - (session_index % 3) * 0.001
                outcomes.append({
                    "market": market, "year": year, "exchange_session_date": session,
                    "checkpoint": checkpoint, "upstream_source_row_sha256": key,
                    "long_realized_net_r": str(long_r), "short_realized_net_r": str(short_r),
                })
    return features, outcomes


def test_market_specific_training_only_ridge_freezes_complete_predictions() -> None:
    features, outcomes = _training_fixture()
    split = build_successor_split_plan(features)
    models, predictions = fit_successor_models(
        feature_rows=features, outcome_rows=outcomes, split_plan=split,
    )
    assert len(models["models"]) == 32
    assert {(row["market"], row["year"]) for row in predictions} == {
        (market, year) for market in MARKETS for year in (2020, 2021, 2022)
    }
    assert all(row["selected_direction"] in {"long", "short", "neutral"} for row in predictions)


def test_baseline_and_candidate_rank_opportunities_independently() -> None:
    common = {
        "year": 2020, "exchange_session_date": "2020-01-02", "checkpoint": "08:30",
        "outer_fold": 0, "selected_direction": "long", "bar_return_1": 0.01,
    }
    predictions = [
        {**common, "market": "ES", "upstream_source_row_sha256": "1" * 64,
         "selected_predicted_net_r": 0.8, "fold_local_direction": "long", "fold_local_ranking_score": 0.1},
        {**common, "market": "CL", "upstream_source_row_sha256": "2" * 64,
         "selected_predicted_net_r": 0.3, "fold_local_direction": "short", "fold_local_ranking_score": 1.0},
    ]
    outcomes = {
        key: {
            "entry_at_ns": 10, "tick_value_usd": "1",
            "long_exit_at_ns": 20, "short_exit_at_ns": 20,
            "long_planned_all_in_risk_usd": "100", "short_planned_all_in_risk_usd": "100",
            "long_realized_gross_pnl_usd": "1", "short_realized_gross_pnl_usd": "1",
        }
        for key in ("1" * 64, "2" * 64)
    }
    candidate = _opportunities(predictions=predictions, outcomes=outcomes, strategy="candidate")
    baseline = _opportunities(
        predictions=predictions, outcomes=outcomes,
        strategy="fold_local_unconditional_return_by_market_session",
    )
    assert candidate[0].market == "ES"
    assert baseline[0].market == "CL"


def test_evaluation_flat_is_true_zero_and_reports_independent_views() -> None:
    predictions = []
    outcomes = []
    counter = 0
    for year in (2020, 2021, 2022):
        for market in MARKETS:
            counter += 1
            key = f"{counter:064x}"
            predictions.append({
                "market": market, "year": year, "exchange_session_date": f"{year}-06-01",
                "checkpoint": "08:30", "outer_fold": year - 2020,
                "upstream_source_row_sha256": key, "selected_direction": "long",
                "selected_predicted_net_r": 0.5, "fold_local_direction": "short",
                "fold_local_ranking_score": 0.2, "bar_return_1": 0.01,
            })
            outcomes.append({
                "upstream_source_row_sha256": key, "entry_at_ns": counter * 100,
                "tick_value_usd": "1", "long_exit_at_ns": counter * 100 + 50,
                "short_exit_at_ns": counter * 100 + 50,
                "long_planned_all_in_risk_usd": "100", "short_planned_all_in_risk_usd": "100",
                "long_realized_gross_pnl_usd": "200", "short_realized_gross_pnl_usd": "-100",
            })
    config = __import__("json").loads((ROOT / "configs/tier1_phase8_evaluation.json").read_text())
    report, decision = build_successor_evaluation(
        predictions=predictions, outcome_rows=outcomes, config=config,
    )
    flat = report["cost_scenarios"]["stress"]["continuous_account"]["strategies"]["flat_no_trade"]
    assert flat["metrics"]["net_pnl_usd"] == "0"
    assert flat["scheduler"]["admitted_count"] == 0
    assert len(report["cost_scenarios"]["stress"]["independent_market_year"]) == 12
    assert decision["live_readiness"] is False
