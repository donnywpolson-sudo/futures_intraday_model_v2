from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_source_compatible_successor import (
    PREDECESSOR_PATH,
    RETAINED_FIELDS,
    build_rejection,
    build_successor,
    validate_successor,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def _artifacts():
    rejection = build_rejection(root=ROOT)
    successor = build_successor(root=ROOT, rejection=rejection)
    predecessor = json.loads((ROOT / PREDECESSOR_PATH).read_text(encoding="utf-8"))
    return predecessor, rejection, successor


def test_v3_rejection_is_source_only_not_strategy_failure() -> None:
    _predecessor, rejection, _successor = _artifacts()
    assert rejection["classification"] == "CONCLUSIVE_PRE_REGISTRATION_SOURCE_INCOMPATIBILITY"
    assert rejection["pilot_readiness"] == "PASS"
    assert rejection["tier1_readiness"] == "FAIL"
    assert rejection["total_triggered_execution_gaps"] == 7
    assert rejection["economic_result"] == "NOT_PRODUCED"
    assert rejection["strategy_failure"] is False
    assert rejection["registration_allowed"] is False


def test_successor_is_new_counted_tier0_restart_with_retained_controls() -> None:
    predecessor, rejection, successor = _artifacts()
    assert successor["mechanism_id"] != predecessor["mechanism_id"]
    assert successor["restart_stage"] == "tier_0"
    assert successor["source_compatibility_gate"]["status"] == (
        "UNPROVEN_REQUIRES_NEW_ROW_CERTIFIED_CENSUS"
    )
    for field in RETAINED_FIELDS:
        assert successor[field] == predecessor[field]
    validate_successor(successor, predecessor=predecessor, rejection=rejection)


def test_resting_limit_entry_never_invents_a_fill() -> None:
    _predecessor, _rejection, successor = _artifacts()
    entry = successor["entry_rules"]
    assert entry["order_type"] == "RESTING_LIMIT_ONE_STANDARD_CONTRACT"
    assert entry["limit_price"] == "TRIGGER_REPORTED_CLOSE"
    assert "PENETRATES_THE_RESTING_LIMIT_BY_AT_LEAST_ONE_TICK" in entry["verified_fill"]
    assert entry["unfilled_limit"] == "EXPLICIT_CANCELLED_NO_TRADE_TIMEOUT"
    assert successor["source_compatibility_gate"]["future_path_used_for_admission"] is False


def test_filled_entry_requires_a_verified_exit() -> None:
    _predecessor, _rejection, successor = _artifacts()
    exit_rules = successor["exit_rules"]
    assert exit_rules["exit_resolution_window_minutes"] == 15
    assert exit_rules["unresolved_exit_after_filled_entry"] == (
        "MANDATORY_EXECUTION_PATH_FAILURE"
    )
    assert successor["source_compatibility_gate"]["filled_entry_to_verified_exit_percent"] == 100


def test_retained_cost_or_risk_change_is_rejected() -> None:
    predecessor, rejection, successor = _artifacts()
    changed = copy.deepcopy(successor)
    changed["costs"]["round_trip_fee_usd"] = "0"
    with pytest.raises((IntegrityError, UnauthorizedOperation)):
        validate_successor(changed, predecessor=predecessor, rejection=rejection)


def test_weakened_coverage_gate_is_rejected_even_with_rehashed_identity() -> None:
    from futures_rebuild.canonical import sha256_json

    predecessor, rejection, successor = _artifacts()
    changed = copy.deepcopy(successor)
    changed["source_compatibility_gate"]["filled_entry_to_verified_exit_percent"] = 99
    core = {key: value for key, value in changed.items() if key != "mechanism_id"}
    changed["mechanism_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="fail-closed"):
        validate_successor(changed, predecessor=predecessor, rejection=rejection)


def test_bar_touch_without_one_tick_penetration_cannot_claim_a_fill() -> None:
    from futures_rebuild.canonical import sha256_json

    predecessor, rejection, successor = _artifacts()
    changed = copy.deepcopy(successor)
    changed["entry_rules"]["verified_fill"] = "LATER_BAR_TOUCHES_LIMIT"
    core = {key: value for key, value in changed.items() if key != "mechanism_id"}
    changed["mechanism_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="fail-closed"):
        validate_successor(changed, predecessor=predecessor, rejection=rejection)


def test_successor_has_no_research_or_external_authority() -> None:
    _predecessor, _rejection, successor = _artifacts()
    assert set(successor["authority"].values()) == {False}
    assert set(successor["outcome_access"].values()) == {False}
    assert successor["source_compatibility_gate"]["registration_before_pass"] is False
    assert successor["source_compatibility_gate"]["pilot_execution_before_pass"] is False
