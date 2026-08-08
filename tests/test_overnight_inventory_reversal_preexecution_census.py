from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.overnight_inventory_reversal_execution import SessionObservation
from futures_rebuild.overnight_inventory_reversal_preexecution_census import (
    build_fold_evidence,
    load_census_plan,
)


MARKETS = ("ES", "CL", "ZN", "6E")


def _observation(market: str, session: str, *, complete: bool = True) -> SessionObservation:
    return SessionObservation(
        market=market,
        session=session,
        overnight_return=0.01 if complete else None,
        execution_path=(),
        prior_session_direction=1,
        complete=complete,
        failure=None if complete else "MISSING_EXACT_08_29_FEATURE_BAR",
    )


def test_census_reconstructs_first_market_fold_failure_without_returns() -> None:
    sessions = [f"2018-01-{day:02d}" for day in range(1, 29)]
    observations = [
        _observation(market, session, complete=not (market == "ES" and session.endswith("28")))
        for market in MARKETS for session in sessions
    ]
    evidence, audit = build_fold_evidence(
        observations=observations,
        outer_folds=[{
            "outer_fit_session_range": [sessions[0], sessions[-2]],
            "outer_test_session_dates": [sessions[-1], sessions[-1]],
        }],
        expected_open_sessions={market: sessions for market in MARKETS},
        ordered_schedule_sessions=sessions,
    )
    assert len(evidence) == 4
    assert audit["first_runtime_failure_reconstructed"] == {
        "scenario_attempted_first": "base",
        "fold_id": "fold-0",
        "market": "ES",
        "expected_training_sessions": 27,
        "complete_training_sessions": 27,
        "minimum_required": 252,
        "exclusion_reasons": {},
    }
    assert audit["evaluation_returns_computed"] is False


def test_census_preserves_exact_exclusion_reasons() -> None:
    sessions = [f"2018-01-{day:02d}" for day in range(1, 5)]
    observations = [
        _observation(market, session, complete=session != sessions[1])
        for market in MARKETS for session in sessions
    ]
    evidence, _ = build_fold_evidence(
        observations=observations,
        outer_folds=[{
            "outer_fit_session_range": [sessions[0], sessions[2]],
            "outer_test_session_dates": [sessions[3], sessions[3]],
        }],
        expected_open_sessions={market: sessions for market in MARKETS},
        ordered_schedule_sessions=sessions,
    )
    es = next(item for item in evidence if item["market"] == "ES")
    assert es["exclusion_reasons"] == {
        "TRAINING__MISSING_EXACT_08_29_FEATURE_BAR": 1,
    }
    assert es["counts"]["complete_training_sessions"] == 2


def test_consumed_serial_plan_is_preserved_and_not_reusable(
    local_evidence_root: Path,
) -> None:
    root = local_evidence_root
    plan_path = root / "configs/overnight_inventory_reversal_fold_census_plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    core = dict(plan)
    assert core.pop("plan_id") == sha256_json(core)
    assert plan["historical_economics_evaluation"] is False
    assert plan["model_fit"] is False
    assert plan["prediction_generation"] is False
    assert plan["holdout_2025_access"] is False
    assert plan["provider_or_network_access"] is False
    assert plan["limits"]["maximum_attempts"] == 1
    assert plan["limits"]["maximum_retries"] == 0
    assert {
        "configs/tier1_historical_checkpoint_calendar_v5.json",
        "manifests/data_releases/controls/038940d82031f31e2c66ed37186e98a6ee6cff3e7248f634f2c7a8e94ea6ecf3.json",
        "manifests/data_releases/reference/5e5f1333ef3bb3487909a038dc3415ff372fdf1a3d1e7ced57fa627b75467139.json",
        "manifests/data_releases/reference/9ec544308450f423fec9b0791df4331b2f7b14758d68c3876202a3c2321e1a5b.json",
    } <= set(plan["bindings"])
    assert (root / "state/authorization_uses/3b81b12f5288a7c6012827788a7574ee109f4f4d87a051c673aaad3b801e5312.json").is_file()
    assert not (root / str(plan["output_root"])).exists()
    with pytest.raises(IntegrityError, match="plan drifted"):
        load_census_plan(root=root)


def test_census_adapter_cannot_call_economic_evaluator() -> None:
    root = Path(__file__).resolve().parents[1]
    source = (
        root / "src/futures_rebuild/overnight_inventory_reversal_preexecution_census.py"
    ).read_text(encoding="utf-8")
    assert "evaluate_fixed_trial" not in source
    assert "_net_for_direction" not in source
    assert "portfolio_net_pnl" not in source
