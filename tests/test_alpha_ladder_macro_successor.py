from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_macro_successor import (
    CANDIDATES, INELIGIBLE, NEW_CANDIDATES, NO_ELIGIBLE,
    load_predecessor_closure, load_successor_contract, select_successor_macro,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.research_gateway_policy import (
    ALPHA_LADDER_MACRO_SUCCESSOR_OPERATION,
    ALPHA_LADDER_MACRO_SUCCESSOR_SCOPE_KEYS,
    require_current_real_history_operation,
)


pytestmark = pytest.mark.current
ROOT = Path(__file__).resolve().parents[1]


def _evidence(*, movement: str = "60", tick_value: str = "12.5") -> dict[str, object]:
    friction = 4 + 10 / float(tick_value)
    return {
        "target_horizon_movement_ticks": movement,
        "conservative_round_trip_friction_ticks": {
            "value": str(friction), "tick_value_usd": tick_value,
        },
        "movement_to_cost_ratio": str(float(movement) / friction),
        "active_minute_coverage": {
            "usable_sessions": 1251, "usable_observations": 400000, "ratio": "1",
        },
        "zero_volume_minute_fraction": "0",
        "missingness_and_continuity": {
            "unexpected_gap_or_stale_fraction": "0",
            "point_in_time_metadata_coverage_ratio": "1",
            "quality_error_count": 0,
        },
        "roll_behavior": {"identity_violation_count": 0},
    }


def test_predecessor_closure_and_successor_freeze_are_exact() -> None:
    closure = load_predecessor_closure(ROOT)
    contract = load_successor_contract(ROOT)
    assert closure["outcome"] == NO_ELIGIBLE
    assert contract["candidate_order"] == list(CANDIDATES)
    assert contract["new_candidates"] == list(NEW_CANDIDATES)
    assert contract["phase2_priority"] == ["SR3", "ZB", "6J"]
    assert contract["measurement_schedule"]["session_count"] == 1251
    assert contract["measurement_schedule"]["sr3_pre_effective_coverage_exemption"] is False
    assert set(contract["cost_model"]["adverse_ticks_round_trip"].values()) == {4}
    assert not any(contract["authority"].values())


def test_selector_carries_failures_and_uses_frozen_order() -> None:
    contract = load_successor_contract(ROOT)
    result = select_successor_macro(
        contract,
        {"SR3": _evidence(), "ZB": _evidence(), "6J": _evidence()},
    )
    assert result["selected_market"] == "SR3"
    assert result["candidate_results"]["ZN"]["status"] == INELIGIBLE
    assert result["candidate_results"]["6E"]["status"] == INELIGIBLE


def test_no_eligible_and_unknown_fail_closed() -> None:
    contract = load_successor_contract(ROOT)
    failed = _evidence(movement="1")
    result = select_successor_macro(
        contract, {"SR3": failed, "ZB": deepcopy(failed), "6J": deepcopy(failed)},
    )
    assert result["outcome"] == NO_ELIGIBLE
    assert result["selected_market"] is None
    malformed = deepcopy(failed)
    malformed["active_minute_coverage"] = None
    result = select_successor_macro(
        contract, {"SR3": malformed, "ZB": deepcopy(failed), "6J": deepcopy(failed)},
    )
    assert result["outcome"] == "UNKNOWN_FAIL_CLOSED"
    assert result["selected_market"] is None


def test_wrong_order_or_result_bearing_evidence_is_rejected() -> None:
    contract = load_successor_contract(ROOT)
    with pytest.raises(IntegrityError, match="ordered"):
        select_successor_macro(
            contract, {"ZB": _evidence(), "SR3": _evidence(), "6J": _evidence()},
        )
    tainted = _evidence()
    tainted["pnl"] = 1
    with pytest.raises(UnauthorizedOperation, match="result-bearing"):
        select_successor_macro(
            contract, {"SR3": tainted, "ZB": _evidence(), "6J": _evidence()},
        )


def test_successor_gateway_requires_the_exact_rank1_bound_scope() -> None:
    scope = {key: "a" * 64 for key in ALPHA_LADDER_MACRO_SUCCESSOR_SCOPE_KEYS}
    scope.update({
        "operation_kind": "PRE_RESULT_MACRO_SUCCESSOR_EXECUTION_PROXY_AND_SEAL",
        "markets": "SR3,ZB,6J", "years": "2018,2019,2020,2021,2022",
    })
    require_current_real_history_operation(ALPHA_LADDER_MACRO_SUCCESSOR_OPERATION, scope)
    missing = dict(scope)
    missing.pop("rank1_pointer_id")
    with pytest.raises(UnauthorizedOperation, match="gateway scope"):
        require_current_real_history_operation(ALPHA_LADDER_MACRO_SUCCESSOR_OPERATION, missing)
