from pathlib import Path

import pytest

from futures_rebuild import causal_observation_market_checkpoint as checkpoint
from futures_rebuild.causal_v10_policy_compatibility import (
    CANARY_PATH,
    CURRENT_CANARY_SHA256,
    CURRENT_POLICY_SHA256,
    CURRENT_SOURCE_CLOSURE_SHA256,
    FROZEN_CANARY_SHA256,
    FROZEN_POLICY_SHA256,
    FROZEN_SOURCE_CLOSURE_SHA256,
    MARKET_MAXIMUM_OUTPUT_BYTES,
    MARKET_MAXIMUM_PEAK_ADDITIONAL_BYTES,
    MARKET_MINIMUM_FREE_AFTER_PEAK_BYTES,
    POLICY_PATH,
    SOURCE_CLOSURE_PATH,
    _compatible_hashes,
    _gate,
    validate_market_campaign_storage,
)
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def test_policy_compatibility_is_exact_and_non_authorizing() -> None:
    gate = _gate(ROOT)

    assert FROZEN_POLICY_SHA256 != CURRENT_POLICY_SHA256
    assert sha256_file(ROOT / POLICY_PATH) == CURRENT_POLICY_SHA256
    assert FROZEN_CANARY_SHA256 != CURRENT_CANARY_SHA256
    assert sha256_file(ROOT / CANARY_PATH) == CURRENT_CANARY_SHA256
    assert FROZEN_SOURCE_CLOSURE_SHA256 != CURRENT_SOURCE_CLOSURE_SHA256
    assert sha256_file(ROOT / SOURCE_CLOSURE_PATH) == CURRENT_SOURCE_CLOSURE_SHA256
    assert gate["causal_observation_operation_changed"] is False
    assert gate["receipt_scope_changed"] is False
    assert gate["research_rules_changed"] is False
    assert gate["sidecar_identity_rules_changed"] is False
    assert gate["standard_query_epoch_rules_changed"] is False
    assert gate["metadata_shape_compatibility_added"] is True
    assert gate["diagnostic_parent_duplicate_exclusion_added"] is True
    assert gate["full_universe_storage_policy_changed"] is False
    assert gate["publication_authorized"] is False
    assert gate["activation_authorized"] is False


def test_policy_compatibility_scopes_smaller_storage_gate_to_market_checkpoint() -> None:
    original = checkpoint.validate_full_build_storage_floor
    plan = {
        "limits": {"maximum_output_bytes": MARKET_MAXIMUM_OUTPUT_BYTES},
        "v10_policy_compatibility_gate": _gate(ROOT),
    }
    required = (
        MARKET_MINIMUM_FREE_AFTER_PEAK_BYTES
        + MARKET_MAXIMUM_PEAK_ADDITIONAL_BYTES
    )

    with _compatible_hashes(ROOT, plan):
        assert checkpoint.validate_full_build_storage_floor(free_bytes=required) == (
            MARKET_MINIMUM_FREE_AFTER_PEAK_BYTES
        )
        with pytest.raises(UnauthorizedOperation, match="storage floor"):
            checkpoint.validate_full_build_storage_floor(free_bytes=required - 1)

    assert checkpoint.validate_full_build_storage_floor is original


def test_campaign_storage_reserves_every_remaining_market_at_the_output_cap() -> None:
    required = (
        MARKET_MINIMUM_FREE_AFTER_PEAK_BYTES
        + MARKET_MAXIMUM_PEAK_ADDITIONAL_BYTES
        + 30 * MARKET_MAXIMUM_OUTPUT_BYTES
    )

    assert validate_market_campaign_storage(
        free_bytes=required, remaining_market_count=30
    ) == 0
    with pytest.raises(UnauthorizedOperation, match="campaign storage floor"):
        validate_market_campaign_storage(
            free_bytes=required - 1, remaining_market_count=30
        )
