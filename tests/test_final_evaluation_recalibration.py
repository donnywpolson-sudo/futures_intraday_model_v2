from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import date, timedelta
from pathlib import Path

import pytest

from futures_rebuild.alpha_research_ladder import (
    build_hypothesis_conditional_successor_contract,
    build_hypothesis_conditional_successor_profile,
    load_active_ladder,
    validate_contract,
    validate_profile,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError
from futures_rebuild.final_evaluation_recalibration import (
    DECLARED_ORDERED_SESSION_SHA256,
    build_bounded_official_facts,
    build_independent_certificate,
    build_manifest,
    restoration_disposition,
    trade_dates_from_bounded_facts,
    validate_bounded_official_facts,
    validate_independent_certificate,
    validate_manifest,
)
from futures_rebuild.repository_surface import validate_active_alpha_authority_closure


ROOT = Path(__file__).resolve().parents[1]
FINAL_MANIFEST = ROOT / "state/final_evaluation_session_manifest_registry/0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1/final_252_session_manifest.json"
pytestmark = pytest.mark.current


def _synthetic_trade_dates() -> list[str]:
    first = date(2025, 7, 14)
    return [
        *(first + timedelta(days=offset) for offset in range(251)),
        date(2026, 7, 13),
    ]


def _source_documents() -> list[dict[str, object]]:
    return [
        {"publisher": "CME_GROUP", "document_id": "MSN11-29-24", "url": "https://www.cmegroup.com/2025.pdf"},
        {"publisher": "CME_GROUP", "document_id": "MSN01-12-26", "url": "https://www.cmegroup.com/2026.pdf"},
        {"publisher": "CME_GROUP", "document_id": "2026_GOOD_FRIDAY_CLEARING_ADVISORY", "url": "https://www.cmegroup.com/good-friday.pdf"},
    ]


def test_qualification_successor_pointer_is_exact_and_non_authorizing() -> None:
    pointer = json.loads((ROOT / "configs/active_alpha_research_ladder.json").read_text(encoding="utf-8"))
    assert pointer == {
        "contract_id": "e0725390e137fd6d4670d29e181bbb36dc8a826835605bbb5fe2be1275ec86b8",
        "contract_path": "state/alpha_ladder_registry/e0725390e137fd6d4670d29e181bbb36dc8a826835605bbb5fe2be1275ec86b8/universe_contract.json",
        "contract_sha256": "163386ae639308737298eac173127a600cf3780212b1207e2f54e942dcdab567",
        "pointer_id": "1ef9908ad1b52f2fb9acfb0b7add3a6b7c4c3fe02c8fc20b20396298abcb35c4",
        "profile_id": "f50697bda61fc0e5cbc48943e12e19132ea35d88d44238f54c4b61c05259d4d3",
        "profile_path": "state/alpha_ladder_registry/e0725390e137fd6d4670d29e181bbb36dc8a826835605bbb5fe2be1275ec86b8/alpha_tiered.yaml",
        "profile_sha256": "381ddfe9c27bce91cd61651fcf25f665aeb013b7abac4325ac7b871b3daa8b38",
        "schema_version": "active_alpha_research_ladder/1.0.0",
    }


def test_qualification_successor_loads_without_reinterpreting_the_predecessor() -> None:
    contract, profile = load_active_ladder(ROOT)
    assert contract["contract_id"] == "e0725390e137fd6d4670d29e181bbb36dc8a826835605bbb5fe2be1275ec86b8"
    assert profile["profile_id"] == "f50697bda61fc0e5cbc48943e12e19132ea35d88d44238f54c4b61c05259d4d3"
    assert contract["predecessor"]["contract_id"] == "67e06d46d8e7a27b3d44ca421be5b5f0f7b14f1232ebda8536fbc77f3bbfea26"
    assert contract["stages"]["final_historical_evaluation"]["status"] == "SEALED_AUTHORITY_RESOLVED_ACCESS_FORBIDDEN"
    assert contract["stages"]["tier_2"]["evaluation_pack"] == ["ES", "NQ", "CL", "NG", "RB", "GC", "HG", "SR3", "ZN", "ZB", "6E", "6J", "ZC", "ZS", "LE", "HE"]
    assert contract["stages"]["tier_3"]["satellite_can_rescue_traditional_failure"] is False
    assert contract["stages"]["tier_0"]["scientific_gate_condition"].endswith("INCLUDES_ES")
    assert contract["stages"]["tier_2"]["applicability"].endswith("BROAD_GENERALITY")
    assert profile["profiles"]["tier_3"]["applicability"].endswith("BROAD_GENERALITY")


def test_final_252_successor_is_sealed_but_access_remains_forbidden() -> None:
    successor = ROOT / (
        "state/final_evaluation_session_manifest_registry/"
        "ad1baab30b52fdda95e72777a4930ea18bd78037925cc36a27594f484c876549/"
        "final_252_session_manifest.json"
    )
    assert not FINAL_MANIFEST.exists()
    assert validate_manifest(json.loads(successor.read_text(encoding="utf-8")))
    for name in ("CURRENT_WORKFLOW.md", "PROJECT_OUTLINE.md", "README.md"):
        text = (ROOT / name).read_text(encoding="utf-8")
        assert "authority" in text.lower() and "unread" in text.lower()


def test_recovery_does_not_fabricate_the_broken_successor_targets() -> None:
    broken_root = ROOT / "state/alpha_ladder_registry/d7e2b182f4c27fdc09876b8140673988648628c1065d89b768c9dd1954cd362f"
    assert not (broken_root / "universe_contract.json").exists()
    assert not (broken_root / "alpha_tiered.yaml").exists()
    assert not FINAL_MANIFEST.exists()


def test_successor_authority_closure_is_fresh_checkout_recoverable() -> None:
    report = validate_active_alpha_authority_closure(ROOT)
    assert report["valid"] is True
    assert report["contract_id"] == "e0725390e137fd6d4670d29e181bbb36dc8a826835605bbb5fe2be1275ec86b8"

def test_predecessor_pointer_remains_git_recoverable_historical_evidence() -> None:
    pointer_path = ROOT / "configs/active_alpha_research_ladder.json"
    materialized = subprocess.check_output(
        [
            "git",
            "show",
            "7d87f8d33edf349af3c771f67a2295faa253d8bc:configs/active_alpha_research_ladder.json",
        ],
        cwd=ROOT,
    )
    assert len(materialized) == 728
    assert hashlib.sha256(materialized).hexdigest() == "bd119473e0b2b60ffcfcf41923ff2e07dc2e8d3608fac9caaf6818336fb5e624"
    assert materialized != pointer_path.read_bytes()


def test_successor_manifest_is_canonical_non_authorizing_and_bounded() -> None:
    manifest = build_manifest(
        trade_dates=[value.isoformat() for value in _synthetic_trade_dates()],
        bindings={"bounded_source_facts_id": "a" * 64, "source_bundle_id": "b" * 64,
                  "semantic_contract_id": "c" * 64},
    )
    assert validate_manifest(manifest) == manifest
    assert manifest["sessions"][0]["trade_date"] == "2025-07-14"
    assert manifest["sessions"][-1]["trade_date"] == "2026-07-13"
    assert not any(manifest["authority"].values())
    assert restoration_disposition(manifest) == "NEW_SUCCESSOR_AUTHORITY_REQUIRED"


def test_official_facts_reproduce_declared_ordered_sequence_and_certificate() -> None:
    facts = build_bounded_official_facts(source_documents=_source_documents())
    trade_dates = trade_dates_from_bounded_facts(facts)
    assert len(trade_dates) == 252
    assert "2026-04-03" in trade_dates
    assert all(value not in trade_dates for value in facts["excluded_trade_dates"])
    manifest = build_manifest(
        trade_dates=trade_dates,
        bindings={
            "bounded_source_facts_id": facts["bounded_source_facts_id"],
            "source_bundle_id": facts["source_bundle_id"],
            "semantic_contract_id": facts["semantic_contract_id"],
        },
    )
    certificate = build_independent_certificate(manifest=manifest, facts=facts)
    assert validate_independent_certificate(
        certificate, manifest=manifest, facts=facts,
    ) == certificate


def test_inferred_or_fallback_dates_are_rejected() -> None:
    facts = build_bounded_official_facts(source_documents=_source_documents())
    facts["semantic_contract"]["inferred_dates"] = True
    with pytest.raises(IntegrityError, match="bounded_source_facts|bounded official facts"):
        validate_bounded_official_facts(facts)


def test_any_declared_identity_mismatch_forces_a_successor() -> None:
    manifest = build_manifest(
        trade_dates=[value.isoformat() for value in _synthetic_trade_dates()],
        bindings={"bounded_source_facts_id": "a" * 64, "source_bundle_id": "b" * 64,
                  "semantic_contract_id": "c" * 64},
    )
    manifest["ordered_session_sha256"] = DECLARED_ORDERED_SESSION_SHA256
    assert restoration_disposition(manifest) == "NEW_SUCCESSOR_AUTHORITY_REQUIRED"


def test_final_binding_successor_preserves_current_qualification_ladder(
    tmp_path: Path,
) -> None:
    pointer = json.loads(
        (ROOT / "configs/active_alpha_research_ladder.json").read_text(encoding="utf-8")
    )
    current, _profile = load_active_ladder(ROOT)
    binding = {
        "manifest_id": "a" * 64,
        "manifest_path": "state/final_evaluation_session_manifest_registry/a/final_252_session_manifest.json",
        "manifest_sha256": "b" * 64,
        "session_count": 252,
        "access_allowed": False,
    }
    failure = current["prior_failed_mechanism"]
    contract = build_hypothesis_conditional_successor_contract(
        predecessor_path=pointer["contract_path"],
        predecessor_sha256=pointer["contract_sha256"],
        predecessor_contract_id=pointer["contract_id"],
        predecessor_profile_id=pointer["profile_id"],
        predecessor_pointer_id=pointer["pointer_id"],
        prior_failure_closure_id=failure["closure_id"],
        prior_failed_mechanism_id=failure["mechanism_id"],
        final_252_binding=binding,
    )
    assert validate_contract(contract) == contract
    assert contract["stages"]["tier_2"] == current["stages"]["tier_2"]
    assert contract["stages"]["tier_3"] == current["stages"]["tier_3"]
    assert contract["stages"]["final_historical_evaluation"]["access_allowed"] is False

    contract_path = tmp_path / "universe_contract.json"
    contract_path.write_bytes(canonical_bytes(contract) + b"\n")
    profile = build_hypothesis_conditional_successor_profile(
        contract_path=contract_path.as_posix(),
        contract_sha256=sha256_file(contract_path),
        contract_id=contract["contract_id"],
        final_252_binding=binding,
    )
    assert validate_profile(
        profile, root=tmp_path, prepared_contract_path=contract_path,
    ) == profile
