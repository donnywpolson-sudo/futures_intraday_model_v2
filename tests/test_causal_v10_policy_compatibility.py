import json
from pathlib import Path

import pytest

from futures_rebuild import causal_observation_market_checkpoint as checkpoint
from futures_rebuild import causal_observation_market_certification as certification
from futures_rebuild import canonical as canonical_module
from futures_rebuild import data_layout
from futures_rebuild import causal_observation_foundation as foundation
from futures_rebuild import research_gateway_policy as policy
from futures_rebuild.causal_v10_policy_compatibility import (
    ACTIVE_SOURCE_CONTRACT_PATH,
    CANARY_PATH,
    CURRENT_CANARY_SHA256,
    CURRENT_CANONICAL_SEMANTICS_ID,
    CURRENT_DATA_LAYOUT_SEMANTICS_ID,
    CURRENT_DATA_LAYOUT_SHA256,
    CURRENT_POLICY_SEMANTICS_ID,
    CURRENT_SOURCE_CLOSURE_SHA256,
    CURRENT_CERTIFIER_BINDINGS_SHA256,
    FROZEN_CANARY_SHA256,
    FROZEN_DATA_LAYOUT_SHA256,
    FROZEN_SOURCE_CONTRACT_PATH,
    FROZEN_SOURCE_CONTRACT_SHA256,
    FROZEN_POLICY_SHA256,
    FROZEN_SOURCE_CLOSURE_SHA256,
    EARLY_CERTIFIER_BINDINGS_SHA256,
    EARLY_CERTIFIER_MARKETS,
    MARKET_MAXIMUM_OUTPUT_BYTES,
    MARKET_MAXIMUM_PEAK_ADDITIONAL_BYTES,
    MARKET_MINIMUM_FREE_AFTER_PEAK_BYTES,
    POLICY_PATH,
    SOURCE_CLOSURE_PATH,
    _compatible_hashes,
    _canonical_semantics,
    _data_layout_semantics,
    _gate,
    _policy_semantics,
    certify_compatible_complete_market_certificate_set,
    validate_compatible_market_checkpoint_plan,
    validate_market_campaign_storage,
)
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def test_policy_compatibility_is_exact_and_non_authorizing() -> None:
    gate = _gate(ROOT)

    assert CURRENT_POLICY_SEMANTICS_ID == sha256_json(_policy_semantics())
    assert FROZEN_CANARY_SHA256 != CURRENT_CANARY_SHA256
    assert sha256_file(ROOT / CANARY_PATH) == CURRENT_CANARY_SHA256
    assert FROZEN_SOURCE_CLOSURE_SHA256 != CURRENT_SOURCE_CLOSURE_SHA256
    assert sha256_file(ROOT / SOURCE_CLOSURE_PATH) == CURRENT_SOURCE_CLOSURE_SHA256
    assert gate["causal_observation_operation_changed"] is False
    assert gate["receipt_scope_changed"] is False
    assert gate["research_rules_changed"] is False
    assert gate["scientific_governance_binding_added"] is True
    assert gate["historical_trial_gateway_strengthened"] is True
    assert gate["current_policy_semantics_id"] == CURRENT_POLICY_SEMANTICS_ID
    assert gate["current_canonical_semantics_id"] == CURRENT_CANONICAL_SEMANTICS_ID
    assert CURRENT_DATA_LAYOUT_SEMANTICS_ID == sha256_json(_data_layout_semantics())
    assert FROZEN_DATA_LAYOUT_SHA256 != CURRENT_DATA_LAYOUT_SHA256
    assert gate["sidecar_identity_rules_changed"] is False
    assert gate["standard_query_epoch_rules_changed"] is False
    assert gate["metadata_shape_compatibility_added"] is True
    assert gate["backfill_midnight_boundary_normalization_added"] is True
    assert gate["diagnostic_parent_duplicate_exclusion_added"] is True
    assert gate["full_universe_storage_policy_changed"] is False
    assert gate["publication_authorized"] is False
    assert gate["activation_authorized"] is False


def test_policy_compatibility_ignores_unrelated_gateway_growth_but_rejects_causal_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = _gate(ROOT)
    monkeypatch.setattr(policy, "BIAS_GATEWAY_SCHEMA", "unrelated-successor")
    assert _gate(ROOT) == expected

    monkeypatch.setattr(policy, "CAUSAL_OBSERVATION_FULL_BUILD_OPERATION", "changed")
    with pytest.raises(IntegrityError, match="causal-build gateway semantics drifted"):
        _gate(ROOT)


def test_canonical_compatibility_ignores_additive_helpers_but_rejects_primitive_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert CURRENT_CANONICAL_SEMANTICS_ID == sha256_json(_canonical_semantics())
    monkeypatch.setattr(canonical_module, "canonical_bytes", lambda value: b"changed")
    with pytest.raises(IntegrityError, match="canonical primitives drifted"):
        _gate(ROOT)


def test_data_layout_compatibility_ignores_noncausal_growth_but_rejects_causal_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = {
        "limits": {"maximum_output_bytes": MARKET_MAXIMUM_OUTPUT_BYTES},
        "v10_policy_compatibility_gate": _gate(ROOT),
    }
    monkeypatch.setitem(data_layout._LOGICAL_PATTERNS, "dbn", object())
    with _compatible_hashes(ROOT, plan):
        assert checkpoint.sha256_file(ROOT / "src/futures_rebuild/data_layout.py") == (
            FROZEN_DATA_LAYOUT_SHA256
        )
        assert certification.sha256_file(
            ROOT / "src/futures_rebuild/data_layout.py"
        ) == CURRENT_DATA_LAYOUT_SHA256

    monkeypatch.setitem(data_layout._LOGICAL_TEMPLATES, "causally_gated_normalized", "changed")
    with pytest.raises(IntegrityError, match="causal data-layout semantics drifted"):
        with _compatible_hashes(ROOT, plan):
            checkpoint.sha256_file(ROOT / "src/futures_rebuild/data_layout.py")


def test_complete_set_compatibility_preserves_original_certificate_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    certificates = []
    for market in checkpoint.MARKET_ORDER:
        core = {
            "market": market,
            "certifier_implementation_bindings_sha256": (
                EARLY_CERTIFIER_BINDINGS_SHA256
                if market in EARLY_CERTIFIER_MARKETS
                else CURRENT_CERTIFIER_BINDINGS_SHA256
            ),
        }
        certificates.append({**core, "certificate_id": sha256_json(core)})
    original_ids = [item["certificate_id"] for item in certificates]
    projected_id = "a" * 64
    monkeypatch.setattr(
        certification,
        "certify_complete_market_certificate_set",
        lambda **_: {
            "schema_version": "projected",
            "status": "PASS_41_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE",
            "market_certificate_ids": [],
            "market_certificate_ids_sha256": "",
            "publication_authorized": False,
            "activation_authorized": False,
            "certificate_id": projected_id,
        },
    )

    result = certify_compatible_complete_market_certificate_set(
        repository_root=ROOT,
        checkpoint_set={},
        market_certificates=certificates,
    )

    assert result["market_certificate_ids"] == original_ids
    assert result["validation_projection_certificate_id"] == projected_id
    assert result["publication_authorized"] is False
    assert result["activation_authorized"] is False


def test_frozen_checkpoint_set_uses_exact_archived_source_contract() -> None:
    plan = json.loads(
        (
            ROOT
            / "state/causal_observation_v10_campaigns/"
            "1809dbe19ead0d0008eb5e20698ddd6f14bf93e49dd61f3fff626f96cf816cfc/"
            "RTY/build_plan.json"
        ).read_text(encoding="utf-8")
    )
    active = json.loads((ROOT / ACTIVE_SOURCE_CONTRACT_PATH).read_text(encoding="utf-8"))

    assert active["contract_id"] != plan["checkpoint_set"]["source_contract_id"]
    assert sha256_file(ROOT / FROZEN_SOURCE_CONTRACT_PATH) == FROZEN_SOURCE_CONTRACT_SHA256
    validate_compatible_market_checkpoint_plan(ROOT, plan)
    with _compatible_hashes(ROOT, plan):
        assert foundation._active_source_identity(RepoBoundary(ROOT)) == (
            plan["source"]["source_contract_id"],
            plan["source"]["canonical_release_id"],
        )


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
