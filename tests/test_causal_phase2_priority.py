from pathlib import Path

from futures_rebuild.alpha_ladder_macro_successor import load_successor_contract
from futures_rebuild.causal_phase2_priority import (
    EXPECTED_CONTRACT_ID,
    PHASE2_ORDER,
    PREDECESSORS,
)


def test_phase2_priority_freeze_is_exact() -> None:
    root = Path(__file__).resolve().parents[1]
    contract = load_successor_contract(root)
    assert contract["contract_id"] == EXPECTED_CONTRACT_ID
    assert tuple(contract["phase2_priority"]) == PHASE2_ORDER
    assert PREDECESSORS == {"SR3": "6B", "ZB": "SR3", "6J": "ZB"}
    assert contract["measurement_schedule"]["sr3_pre_effective_coverage_exemption"] is False
