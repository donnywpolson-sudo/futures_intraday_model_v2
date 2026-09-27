from __future__ import annotations

from pathlib import Path

from futures_rebuild.causal_observation_priority_successor import (
    CERTIFIED_PREFIX,
    CHECKPOINT_SET_ID,
    PRIORITY_ORDER,
    build_transition,
    load_contract,
    validate_transition,
)


ROOT = Path(__file__).resolve().parents[1]


def test_priority_contract_is_non_authorizing_and_preserves_checkpoint_set() -> None:
    contract = load_contract(ROOT)

    assert PRIORITY_ORDER == ("ES", "GC", "6E", "CL", "ZN", "NQ")
    assert CERTIFIED_PREFIX == ("ES", "GC", "6E", "CL")
    assert contract["checkpoint_set_id"] == CHECKPOINT_SET_ID
    assert contract["original_checkpoint_market_order_preserved"] is True
    assert contract["historical_row_read_authorized"] is False
    assert contract["publication_authorized"] is False
    assert contract["activation_authorized"] is False


def test_live_certified_prefix_revalidates_every_preserved_output() -> None:
    transition = build_transition(ROOT)

    validate_transition(ROOT, transition)
    assert transition["status"] == (
        "PASS_PRIORITY_PREFIX_REVALIDATED_NO_EXECUTION_AUTHORITY"
    )
    assert transition["transition_id"] == (
        "21bdfd92dfab24d99974dfe01acb692894ff610b1a4d52d22485c0f46452e01d"
    )
    assert transition["complete_output_file_count"] == 3640
    assert transition["market_rows_opened"] == 0
    assert transition["historical_row_read_authorized"] is False
    assert transition["certification_authorized"] is False
