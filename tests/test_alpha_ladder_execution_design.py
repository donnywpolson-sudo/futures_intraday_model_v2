from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_execution_design import (
    ELIGIBLE, INELIGIBLE, NO_ELIGIBLE, UNKNOWN,
    evaluate_execution_proxy, load_execution_design_contract,
    select_macro_execution_candidate, validate_execution_design_contract,
)
from futures_rebuild.alpha_ladder_macro_successor_identity import build_identities
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
pytestmark = pytest.mark.current
ROOT = Path(__file__).resolve().parents[1]
ACTIVE_DESIGN_PATH = Path("configs/active_alpha_historical_research_design.json")
FINAL_MANIFEST_PATH = Path(
    "state/final_evaluation_session_manifest_registry/"
    "0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1/"
    "final_252_session_manifest.json"
)


def _evidence(*, movement: str = "50", coverage: str = "0.99") -> dict[str, object]:
    return {
        "target_horizon_movement_ticks": movement,
        "conservative_round_trip_friction_ticks": {"value": "5.0", "tick_value_usd": "10.0"},
        "movement_to_cost_ratio": str(float(movement) / 5.0),
        "active_minute_coverage": {"ratio": coverage, "usable_sessions": 252, "usable_observations": 97020},
        "zero_volume_minute_fraction": "0.01",
        "missingness_and_continuity": {
            "unexpected_gap_or_stale_fraction": "0.005",
            "point_in_time_metadata_coverage_ratio": "1.0",
            "quality_error_count": 0,
        },
        "roll_behavior": {"identity_violation_count": 0, "excluded_session_count": 4},
    }


def test_contract_is_canonical_deterministic_and_bound() -> None:
    first = load_execution_design_contract(ROOT)
    assert first == load_execution_design_contract(ROOT)
    assert first["state"]["mechanism"] == "NOT_STARTED"
    assert first["state"]["macro_selection"] == "PENDING_PRE_RESULT_EXECUTION_GATE"
    assert tuple(first["macro_selector"]["candidates"]) == ("ZN", "6E")


def test_active_execution_identity_resolves_mutable_status_from_ladder() -> None:
    pointer = json.loads(
        (ROOT / "configs/active_alpha_ladder_execution_design.json").read_bytes()
    )
    core = {key: value for key, value in pointer.items() if key != "pointer_id"}
    assert pointer["pointer_id"] == sha256_json(core)
    assert pointer["alpha_ladder_status_source"] == {
        "path": "configs/active_alpha_research_ladder.json",
        "stage": "final_historical_evaluation",
    }
    assert "alpha_ladder_binding" not in pointer


def test_active_rank1_historical_design_is_exact_and_non_authorizing() -> None:
    pointer = json.loads((ROOT / ACTIVE_DESIGN_PATH).read_bytes())
    core = {key: value for key, value in pointer.items() if key != "pointer_id"}
    assert pointer["pointer_id"] == sha256_json(core)
    assert pointer["state"] == "HISTORICAL_RESEARCH_DESIGN_IDENTITY_ONLY"
    assert pointer["does_not_authorize"] == [
        "FEATURE_OR_OUTCOME_CONSTRUCTION", "FINAL_252_OR_FORWARD_ACCESS",
        "MACRO_SELECTION", "MARKET_ROW_READ", "MODEL_FIT_OR_PREDICTION",
        "PROVIDER_OR_NETWORK", "TRADING", "WFA_OR_ALPHA_EVALUATION",
    ]
    assert pointer["design"] == "RANK_1_RECENT_REGIME_CHALLENGE"
    assert pointer["selection_and_wfa"]["start_inclusive"] == "2018-01-01T00:00:00Z"
    assert pointer["selection_and_wfa"]["end_exclusive"] == "2023-01-01T00:00:00Z"
    challenge = pointer["recent_regime_challenge"]
    assert challenge["start_inclusive"] == "2023-01-01T00:00:00Z"
    assert challenge["end_exclusive"] == "2025-07-13T22:00:00Z"
    assert challenge["maximum_attempts_per_market"] == 1
    assert challenge["tuning_or_design_change_after_result"] is False
    refit = pointer["final_refit"]
    assert refit["allowed_change"] == "FIT_PARAMETERS_ONLY"
    assert refit["performance_evaluation_after_refit_before_final_252"] is False
    assert pointer["final_252"] == {
        "only_independent_historical_confirmation": True,
        "status_source": {
            "path": "configs/active_alpha_research_ladder.json",
            "stage": "final_historical_evaluation",
        },
    }
    active = json.loads((ROOT / "configs/active_alpha_research_ladder.json").read_bytes())
    ladder = json.loads((ROOT / active["contract_path"]).read_bytes())
    assert ladder["stages"]["final_historical_evaluation"]["status"] == (
        "SEALED_AUTHORITY_RESOLVED_ACCESS_FORBIDDEN"
    )


def test_rank1_rebuild_does_not_copy_mutable_final_252_status() -> None:
    rank1 = build_identities(ROOT)["rank1"]
    assert rank1["final_252"] == {
        "only_independent_historical_confirmation": True,
        "status_source": {
            "path": "configs/active_alpha_research_ladder.json",
            "stage": "final_historical_evaluation",
        },
    }


def test_missing_evidence_fails_closed() -> None:
    contract = load_execution_design_contract(ROOT)
    evidence = _evidence()
    del evidence["roll_behavior"]
    assert evaluate_execution_proxy(contract, "ZN", evidence)["status"] == UNKNOWN


def test_result_bearing_evidence_is_forbidden() -> None:
    contract = load_execution_design_contract(ROOT)
    evidence = _evidence()
    evidence["sharpe"] = 2.0
    with pytest.raises(UnauthorizedOperation):
        evaluate_execution_proxy(contract, "ZN", evidence)


def test_only_zn_and_6e_are_permitted() -> None:
    contract = load_execution_design_contract(ROOT)
    with pytest.raises(UnauthorizedOperation):
        evaluate_execution_proxy(contract, "ES", _evidence())
    with pytest.raises(IntegrityError):
        select_macro_execution_candidate(contract, {"ZN": _evidence(), "ES": _evidence()})


def test_neither_passes_returns_no_eligible() -> None:
    contract = load_execution_design_contract(ROOT)
    failed = _evidence(coverage="0.50")
    result = select_macro_execution_candidate(contract, {"ZN": failed, "6E": deepcopy(failed)})
    assert result["outcome"] == NO_ELIGIBLE
    assert result["selected_market"] is None
    assert {item["status"] for item in result["candidate_results"].values()} == {INELIGIBLE}


def test_only_passing_candidate_is_selected_by_algorithm() -> None:
    contract = load_execution_design_contract(ROOT)
    result = select_macro_execution_candidate(contract, {"ZN": _evidence(coverage="0.50"), "6E": _evidence()})
    assert result["selected_market"] == "6E"
    assert result["candidate_results"]["6E"]["status"] == ELIGIBLE


def test_both_pass_uses_frozen_one_dimensional_tie_breaker() -> None:
    contract = load_execution_design_contract(ROOT)
    higher_6e = select_macro_execution_candidate(contract, {"ZN": _evidence(movement="50"), "6E": _evidence(movement="60")})
    exact_tie = select_macro_execution_candidate(contract, {"ZN": _evidence(), "6E": _evidence()})
    assert higher_6e["selected_market"] == "6E"
    assert exact_tie["selected_market"] == "ZN"
    proxy = contract["execution_proxy_gates"]["movement_proxy_anchor_contract"]
    assert proxy["aggregation"].startswith("UNWEIGHTED_MEDIAN_")
    assert proxy["direction_or_strategy_signal"] == "NONE"
    assert proxy["result_metrics_used"] is False
    assert proxy["tie_break_dimension"] == "HIGHEST_MOVEMENT_TO_COST_RATIO"


def test_row_authorization_is_unissued_and_final_252_remains_absent() -> None:
    contract = load_execution_design_contract(ROOT)
    template = contract["future_row_read_authorization"]
    assert (template["status"], template["authorization_id"], template["uses_consumed"]) == ("NOT_ISSUED", None, 0)
    assert tuple(template["markets"]) == ("ZN", "6E")
    assert not (ROOT / FINAL_MANIFEST_PATH).exists()


def test_contract_identity_rejects_any_semantic_change() -> None:
    contract = load_execution_design_contract(ROOT)
    changed = deepcopy(contract)
    changed["horizon"]["holding_horizon_minutes"] = 31
    with pytest.raises(IntegrityError):
        validate_execution_design_contract(changed)
