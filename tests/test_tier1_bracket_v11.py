from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from futures_rebuild.tier1_bracket_post_audit import CausalBar
from futures_rebuild.tier1_bracket_v4 import BracketFill, ExpectedCheckpoint, FEATURE_NAMES, MarketSpec
from futures_rebuild.tier1_bracket_v5 import (
    MaterializedRowV5, OpportunityRecordV5, load_v5_contract,
)
from futures_rebuild.tier1_bracket_v8 import FrozenPredictionV8
from futures_rebuild.tier1_bracket_v9 import ModelFitResultV9
from futures_rebuild.tier1_bracket_v10_decision_validity import (
    DirectionalOutcomeResolutionV10, attach_causal_outcomes_v10,
)
from futures_rebuild.tier1_bracket_v11 import (
    build_strategy_prediction_universes_v11,
    evaluate_independent_strategies_v11, load_v11_contract,
    segmented_account_views_v11,
)
from futures_rebuild.tier1_bracket_v11_pipeline import build_nested_crossfit_evidence_v11
from tests.test_tier1_bracket_v10_pipeline import _rows_for_crossfit


ROOT = Path(__file__).resolve().parents[1]
D = Decimal


def _row(market: str, index: int) -> MaterializedRowV5:
    session = "2020-01-02"
    opportunity_id = f"{market}-{index}"
    expected = ExpectedCheckpoint(opportunity_id, market, 2020, session, "08:30", 100)
    ledger = OpportunityRecordV5(
        opportunity_id, market, session, "08:30", 100,
        "PREDICTION_PRODUCED", True, 1, 2, outcome_coverage="COMPLETE",
    )
    return MaterializedRowV5(
        expected, ledger, {name: 0.01 for name in FEATURE_NAMES}, D("1"),
        "a" * 64, None, (), MarketSpec(D("0.25"), D("12.5"), D("50")), True,
    )


def _model(candidate_market: str = "CL") -> ModelFitResultV9:
    prediction = FrozenPredictionV8(
        f"{candidate_market}-0", candidate_market, 2020, "2020-01-02", "08:30", 0,
        0.5, -0.5, "long", 0.5, "long", 0.1, 0.01,
    )
    models = []
    for market in ("ES", "CL", "ZN", "6E"):
        models.append({
            "outer_fold": 0, "market": market, "status": "FITTED",
            "fold_local_unconditional": {
                checkpoint: {
                    "status": "ESTIMATED", "direction": "long", "score": 0.1,
                    "training_rows": 10,
                }
                for checkpoint in ("08:30", "10:30", "13:30")
            },
        })
    return ModelFitResultV9({"models": models}, (prediction,), 0, ("ES-0",))


def _resolution() -> DirectionalOutcomeResolutionV10:
    fill = BracketFill(
        120, 180, D("100"), D("101"), D("99"), D("101"), "TARGET",
        D("60"), D("10"), D("50"), D("100"),
    )
    bar = CausalBar(120, 180, 185, D("100"), D("101"), D("99"), D("100"), True)
    return DirectionalOutcomeResolutionV10(
        {
            (scenario, direction): fill
            for scenario in ("base", "stress", "extreme")
            for direction in ("long", "short")
        },
        (bar,), None,
    )


def test_v11_contract_is_prepared_without_strategy_tuning() -> None:
    inherited, delta = load_v11_contract(root=ROOT)
    assert inherited["strategy"]["minimum_predicted_net_r_after_stress_costs"] == "0.25"
    assert delta["state"] == "PREPARED_NOT_REGISTERED"
    assert set(delta["anti_tuning"].values()) == {False}


def test_candidate_model_unavailability_cannot_censor_model_independent_baselines() -> None:
    rows = tuple(_row(market, 0) for market in ("ES", "CL", "ZN", "6E"))
    folds = (SimpleNamespace(outer_fold=0, test_sessions=("2020-01-02",)),)
    universes = build_strategy_prediction_universes_v11(
        model=_model(), rows=rows, folds=folds,
    )
    candidate_ids = {item.opportunity_id for item in universes.predictions["candidate"]}
    always_ids = {
        item.opportunity_id
        for item in universes.predictions["risk_matched_always_long_intraday"]
    }
    momentum_ids = {
        item.opportunity_id
        for item in universes.predictions["previous_bar_sign_momentum"]
    }
    assert "ES-0" not in candidate_ids
    assert "ES-0" in always_ids == momentum_ids

    evaluation, coverage = evaluate_independent_strategies_v11(
        universes=universes, rows=rows,
        resolutions={row.expected.opportunity_id: _resolution() for row in rows},
    )
    assert evaluation["stress"]["candidate"].admitted[0].market == "CL"
    assert evaluation["stress"]["risk_matched_always_long_intraday"].admitted[0].market == "ES"
    assert coverage["stress"]["status"] == "PASS"


def test_model_independent_baseline_retains_missing_feature_as_explicit_abstention() -> None:
    complete = _row("CL", 0)
    missing = replace(
        _row("ES", 0), features=None,
        ledger=replace(
            _row("ES", 0).ledger,
            terminal_disposition="INSUFFICIENT_CAUSAL_HISTORY",
            prediction_produced=False, feature_event_at_ns=None,
            feature_available_at_ns=None,
        ),
    )
    rows = (missing, complete, _row("ZN", 0), _row("6E", 0))
    folds = (SimpleNamespace(outer_fold=0, test_sessions=("2020-01-02",)),)
    universes = build_strategy_prediction_universes_v11(
        model=_model(), rows=rows, folds=folds,
    )
    evaluation, _ = evaluate_independent_strategies_v11(
        universes=universes, rows=rows,
        resolutions={row.expected.opportunity_id: _resolution() for row in rows},
    )
    path = evaluation["stress"]["risk_matched_always_long_intraday"]
    assert set(path.terminal_dispositions) == {
        row.expected.opportunity_id for row in rows
    }
    assert path.terminal_dispositions["ES-0"] == "BASELINE_INPUT_COVERAGE_ABSTENTION"


def test_v11_nested_crossfit_uses_independent_complete_baseline_universes() -> None:
    upgraded, resolutions = attach_causal_outcomes_v10(
        rows=_rows_for_crossfit(), contract=load_v5_contract(root=ROOT),
    )
    evidence = build_nested_crossfit_evidence_v11(
        rows=upgraded, resolutions=resolutions,
    )
    assert evidence.controls.decision_availability["status"] == "PASS"
    assert evidence.baseline_coverage["status"] == "PASS"
    assert evidence.controls.evaluation_completeness["status"] == "PASS"


def test_v11_segmented_views_include_every_declared_market_year() -> None:
    plan = SimpleNamespace(
        strategy="flat_no_trade", trades=(),
        preliminary_terminals={"a": "FLAT_NO_TRADE", "b": "FLAT_NO_TRADE"},
    )
    views = segmented_account_views_v11(
        plan=plan,  # type: ignore[arg-type]
        opportunity_market_year={"a": ("ES", 2018), "b": ("ES", 2022)},
    )
    assert set(views) == {"ES/2018", "ES/2022"}
    assert views["ES/2018"].terminal_dispositions == {"a": "FLAT_NO_TRADE"}
