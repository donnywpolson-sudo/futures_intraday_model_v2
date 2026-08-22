from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.alpha_research_ladder import ALL_APPROVED, BALANCED, CORE, SATELLITE, TRADITIONAL
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError
from futures_rebuild.final_evaluation_recalibration import (
    validate_contract,
    validate_manifest,
    validate_profile,
)


ROOT = Path(__file__).resolve().parents[1]
PREP = ROOT / "state/data_publication_staging/final_evaluation_session_manifest/purpose_limited_final_252_v2/preparation"
pytestmark = pytest.mark.current


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_certified_manifest_is_exact_and_purpose_limited() -> None:
    manifest = validate_manifest(load(PREP / "final_252_session_manifest.json"))
    assert manifest["manifest_id"] == "0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1"
    assert manifest["ordered_session_sha256"] == "fa05d8b7df4df05c1eef6a3266df3d2cb2d70dfe21e1161c327188022a201171"
    assert manifest["development_end_exclusive"] == "2025-07-13T22:00:00Z"
    assert manifest["forward_start"] == "2026-07-14T00:00:00Z"
    assert manifest["sessions"][0]["trade_date"] == "2025-07-14"
    assert manifest["sessions"][-1]["trade_date"] == "2026-07-13"
    assert not any(manifest["authority"].values())


def test_pristine_classification_binds_machine_and_human_evidence() -> None:
    successor = ROOT / "state/final_evaluation_session_manifest_registry/0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1"
    classification = load(successor / "contamination_classification.json")
    attestation = load(successor / "human_use_attestation.json")
    assert classification["classification"] == "RESEARCH_SELECTION_PRISTINE"
    assert classification["nomenclature"] == "Final Sealed 252-Session Holdout"
    assert classification["machine_exact_session_overlap_count"] == 0
    assert classification["machine_metadata_parse_failure_count"] == 0
    assert classification["human_attestation_id"] == attestation["attestation_id"]
    assert attestation["selection_use"] is False
    assert not any(attestation["authority"].values())


def test_successor_ladder_has_exact_levels_sets_and_final_binding() -> None:
    packet = load(PREP / "pipeline_activation_packet_v2.json")
    contract_dir = ROOT / Path(packet["ladder_successor"]["contract_path"]).parent
    contract_path = contract_dir / "universe_contract.json"
    contract = validate_contract(load(contract_path))
    profile = validate_profile(load(contract_dir / "alpha_tiered.yaml"), contract=contract, contract_path=contract_path)
    assert contract["transition_order"] == ["tier_0", "tier_1", "tier_2", "tier_3", "final_evaluation", "forward"]
    assert contract["stages"]["tier_0"]["markets"] == ["ES"]
    assert contract["stages"]["tier_1"]["markets"] == list(CORE)
    assert contract["stages"]["tier_2"]["markets"] == list(BALANCED)
    assert contract["stages"]["tier_3"]["traditional_markets"] == list(TRADITIONAL)
    assert contract["stages"]["tier_3"]["satellite_markets"] == list(SATELLITE)
    assert contract["stages"]["tier_3"]["markets"] == list(ALL_APPROVED)
    assert contract["standard_market_count"] == 41
    assert contract["deferred_micro_market_count"] == 17
    assert contract["stages"]["final_evaluation"]["binding"]["manifest_id"] == "0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1"
    assert contract["stages"]["final_evaluation"]["binding"]["manifest_path"].startswith("state/final_evaluation_session_manifest_registry/")
    assert contract["failed_mechanism"]["status"] == "CLOSED_FAILED_AT_TIER_0_ES_QUALIFICATION"
    assert contract["new_counted_mechanism"] == "NOT_STARTED_RESTART_AT_TIER_0_SYNTHETIC_ENGINEERING"
    assert not any(contract["authority"].values())
    assert not any(profile["authority"].values())


def test_manifest_tamper_fails_closed() -> None:
    manifest = load(PREP / "final_252_session_manifest.json")
    manifest["sessions"] = manifest["sessions"][:-1]
    with pytest.raises(IntegrityError):
        validate_manifest(manifest)


def test_active_pointer_resolves_to_recalibrated_successor() -> None:
    pointer = load(ROOT / "configs/active_alpha_research_ladder.json")
    assert pointer["pointer_id"] == "04134e8fa678f7961f119d6ff860ed102d415792397bff681f5f9251d2e19084"
    assert pointer["contract_id"] == "d7e2b182f4c27fdc09876b8140673988648628c1065d89b768c9dd1954cd362f"
    assert pointer["profile_id"] == "17ef8fed6210552f7ebad59f0d05ceeb77f82291fe54e159fede2b70e2510344"
    assert pointer["contract_path"] == "state/alpha_ladder_registry/d7e2b182f4c27fdc09876b8140673988648628c1065d89b768c9dd1954cd362f/universe_contract.json"
    assert pointer["profile_path"] == "state/alpha_ladder_registry/d7e2b182f4c27fdc09876b8140673988648628c1065d89b768c9dd1954cd362f/alpha_tiered.yaml"
    contract_path = ROOT / pointer["contract_path"]
    profile_path = ROOT / pointer["profile_path"]
    assert pointer["contract_sha256"] == "ad2bd5c3de539ff1e34ff12d9890a7ed310a658f9d51b31b9aa9561d4de5c0e9"
    assert pointer["profile_sha256"] == "348f07a071e49d87dc6be159d934c907c6295b7b477cc1492a78ba7537f81ec3"
    assert sha256_file(contract_path) == pointer["contract_sha256"]
    assert sha256_file(profile_path) == pointer["profile_sha256"]
    contract = validate_contract(load(contract_path))
    validate_profile(load(profile_path), contract=contract, contract_path=contract_path)
    assert contract["predecessor"]["contract_id"] == "53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815"
    assert contract["predecessor"]["preserved_byte_for_byte"] is True
    assert pointer["pointer_id"] != "8767e0d03748451f456891016e97e5bbd19b5b47eabea65030f8e4f78a002772"
