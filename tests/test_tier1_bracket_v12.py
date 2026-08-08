from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

from futures_rebuild.tier1_bracket_v12 import (
    evaluate_required_baseline_coverage_v12, load_v12_contract,
)
from futures_rebuild.tier1_bracket_v12_pipeline import (
    build_nested_crossfit_evidence_v12,
)
from futures_rebuild.tier1_bracket_v10_decision_validity import attach_causal_outcomes_v10
from futures_rebuild.tier1_bracket_v5 import load_v5_contract
from tests.test_tier1_bracket_v10_pipeline import _rows_for_crossfit
from tests.test_tier1_bracket_v11 import _model, _row


ROOT = Path(__file__).resolve().parents[1]


def test_v12_contract_changes_only_flat_coverage_independence() -> None:
    inherited, delta = load_v12_contract(root=ROOT)
    assert inherited["risk"]["continuous_drawdown_threshold_usd"] == "1500"
    assert inherited["strategy"]["minimum_predicted_net_r_after_stress_costs"] == "0.25"
    assert set(delta["anti_tuning"].values()) == {False}


def test_flat_coverage_is_exact_despite_missing_features() -> None:
    from futures_rebuild.tier1_bracket_v11 import build_strategy_prediction_universes_v11

    missing = _row("ES", 0)
    missing = replace(
        missing, features=None,
        ledger=replace(
            missing.ledger, terminal_disposition="INSUFFICIENT_CAUSAL_HISTORY",
            prediction_produced=False, feature_event_at_ns=None,
            feature_available_at_ns=None,
        ),
    )
    rows = (missing, _row("CL", 0), _row("ZN", 0), _row("6E", 0))
    folds = (SimpleNamespace(outer_fold=0, test_sessions=("2020-01-02",)),)
    universes = build_strategy_prediction_universes_v11(
        model=_model(), rows=rows, folds=folds,
    )
    result = evaluate_required_baseline_coverage_v12(
        rows=rows, folds=folds, universes=universes,
    )
    flat = result["strategies"]["flat_no_trade"]
    assert flat["expected"] == flat["eligible"] == 4
    assert flat["overall_rate"] == 1.0
    assert flat["feature_or_model_dependency"] is False
    assert flat["outcome_dependency"] is False


def test_v12_nested_crossfit_flat_coverage_is_exact() -> None:
    rows, resolutions = attach_causal_outcomes_v10(
        rows=_rows_for_crossfit(), contract=load_v5_contract(root=ROOT),
    )
    evidence = build_nested_crossfit_evidence_v12(
        rows=rows, resolutions=resolutions,
    )
    flat = evidence.baseline_coverage["strategies"]["flat_no_trade"]
    assert flat["status"] == "PASS"
    assert flat["overall_rate"] == 1.0
    assert set(flat["market_year_rates"].values()) == {1.0}
