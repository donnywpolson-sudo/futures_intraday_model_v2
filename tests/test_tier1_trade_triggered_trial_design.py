from __future__ import annotations

import inspect
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_trade_triggered_trial_design import (
    DECLARATION_PATH,
    SELECTED_MARKETS,
    build_declaration,
    planned_initial_loss_usd,
    risk_eligible,
    select_ranked_intent,
)
import futures_rebuild.tier1_trade_triggered_trial_design as design_module


ROOT = Path(__file__).resolve().parents[1]


def _synthetic_declaration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> dict[str, object]:
    monkeypatch.setattr(design_module, "_verified_source_report", lambda _root: {})
    for relative in (
        "configs/active_cash_open_impulse_historical_calendar.json",
        "data/active/catalog.json",
        "configs/contract_economics_rules.json",
        "configs/prop_firm_risk_profile.json",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("synthetic\n", encoding="utf-8")
    return build_declaration(root=tmp_path)


def test_declaration_is_source_selected_and_nonregisterable(
    local_evidence_root: Path,
) -> None:
    declaration = build_declaration(root=local_evidence_root)
    assert declaration["state"] == "PREPARED_NOT_REGISTERABLE_ROW_CERTIFICATE_REQUIRED"
    assert declaration["lineage"]["selected_checkpoint_chicago"] == "10:00"
    assert tuple(declaration["lineage"]["selected_markets"]) == SELECTED_MARKETS
    assert declaration["lineage"]["market_and_checkpoint_selection_used_returns"] is False
    assert declaration["execution_authority"]["registration_authorized"] is False
    assert declaration["execution_authority"]["historical_rows_authorized"] is False


def test_standard_contract_risk_cap_is_hard_and_cost_inclusive() -> None:
    assert planned_initial_loss_usd(
        stop_ticks=19, tick_value_usd=Decimal("10"), stress_cost_usd=Decimal("60")
    ) == Decimal("250")
    assert risk_eligible(
        stop_ticks=19, tick_value_usd=Decimal("10"), stress_cost_usd=Decimal("60")
    )
    assert not risk_eligible(
        stop_ticks=20, tick_value_usd=Decimal("10"), stress_cost_usd=Decimal("60")
    )
    with pytest.raises(IntegrityError):
        planned_initial_loss_usd(
            stop_ticks=0, tick_value_usd=Decimal("10"), stress_cost_usd=Decimal("60")
        )


def test_ranking_is_deterministic_and_hurdle_is_locked() -> None:
    selected = select_ranked_intent([
        {"market": "ES", "predicted_net_r": "0.30", "risk_eligible": True},
        {"market": "CL", "predicted_net_r": "0.30", "risk_eligible": True},
        {"market": "NQ", "predicted_net_r": "0.90", "risk_eligible": False},
        {"market": "YM", "predicted_net_r": "0.24", "risk_eligible": True},
    ])
    assert selected is not None and selected["market"] == "CL"
    assert select_ranked_intent([
        {"market": "ES", "predicted_net_r": "0.249999", "risk_eligible": True}
    ]) is None


def test_features_models_splits_costs_and_promotion_are_exact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    declaration = _synthetic_declaration(tmp_path, monkeypatch)
    assert declaration["model"]["ridge_penalty"] == "1.0"
    assert declaration["model"]["hyperparameter_search"] is False
    assert "STRESS_RISK_ELIGIBLE" in declaration["model"]["training_row_eligibility"]
    assert declaration["model"]["no_trigger_or_incomplete_training_target"].startswith("NO_LABEL")
    assert declaration["features"]["training_only_standardization"].startswith("PER_MARKET")
    assert declaration["features"]["definitions"]["log_return_10"].startswith("LN(")
    assert "FIXED" in declaration["features"]["time_of_session_context"]
    assert declaration["splits"]["outer_folds"] == 8
    assert declaration["splits"]["purge_minutes"] == 40
    assert declaration["costs"]["promotion_scenario"] == "stress"
    assert declaration["costs"]["may_be_reduced_after_outcomes"] is False
    assert declaration["promotion"]["positive_portfolio_years_required_of_five"] == 3
    assert declaration["promotion"]["positive_evaluation_folds_required_of_eight"] == 5
    assert declaration["statistics"]["multiplicity"].startswith("BONFERRONI_SIX")
    assert declaration["promotion"]["maximum_continuous_drawdown_usd"] == "1500"


def test_baselines_have_independent_paths_and_daily_statistics_are_labeled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    declaration = _synthetic_declaration(tmp_path, monkeypatch)
    assert len(declaration["baselines"]["mandatory"]) == 6
    assert declaration["baselines"]["candidate_schedule_reuse"] is False
    assert "OWN_CAUSAL_UNIVERSE" in declaration["baselines"]["active_baseline_independence"]
    assert declaration["metrics"]["per_trade_sharpe_or_sortino_label_forbidden"] is True
    assert declaration["metrics"]["primary_series"].startswith("ONE_PORTFOLIO")


def test_source_compatibility_does_not_falsely_certify_stop_readiness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    declaration = _synthetic_declaration(tmp_path, monkeypatch)
    readiness = declaration["preexecution_readiness_required_before_registration"]
    assert readiness["source_report_alone_is_sufficient"] is False
    assert readiness["triggered_path_coverage_percent"] == 100
    assert readiness["failed_or_unverifiable_gate"] == "BLOCK_REGISTRATION_AND_EXECUTION"
    assert any("STOP_OR_TIME_EXIT" in item for item in readiness["required_row_certification"])


def test_preparer_is_create_only_and_has_no_registration_or_execution_surface() -> None:
    source = (ROOT / "scripts/prepare_tier1_trade_triggered_trial_protocol.py").read_text(
        encoding="utf-8"
    )
    assert 'open("xb")' in source
    for forbidden in ("register_trial", "execute_once", "issue_user_approved", "fit(", "predict("):
        assert forbidden not in source
    implementation = inspect.getsource(build_declaration).lower()
    for forbidden in (
        "pyarrow", "read_parquet", "iter_batches", "open_nano", "close_nano",
        ".fit(", ".predict(", "evaluate_performance",
    ):
        assert forbidden not in implementation
    if (ROOT / DECLARATION_PATH).exists():
        assert (ROOT / DECLARATION_PATH).read_bytes().endswith(b"\n")
