from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest

from futures_rebuild.alpha_research_ladder import load_active_ladder
from futures_rebuild.repository_surface import validate_active_alpha_authority_closure


ROOT = Path(__file__).resolve().parents[1]
FINAL_MANIFEST = ROOT / "state/final_evaluation_session_manifest_registry/0ff48f99d8b6d3a262ddf0a060bea8e733fc95aa7c4b4d43f19a0f78b107d4d1/final_252_session_manifest.json"
pytestmark = pytest.mark.current


def test_rebound_pointer_is_the_exact_last_complete_authority() -> None:
    pointer = json.loads((ROOT / "configs/active_alpha_research_ladder.json").read_text(encoding="utf-8"))
    assert pointer == {
        "contract_id": "53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815",
        "contract_path": "state/alpha_ladder_registry/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/universe_contract.json",
        "contract_sha256": "599264b353c6f115712e8a0d56f7e23150482683bd40ac9937899862fc8ec026",
        "pointer_id": "8767e0d03748451f456891016e97e5bbd19b5b47eabea65030f8e4f78a002772",
        "profile_id": "18fbb7a3a405ee2bcaef5dd7d6e757cfb3a69ec8485afd34e5fcf1f627aaeca6",
        "profile_path": "state/alpha_ladder_registry/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/alpha_tiered.yaml",
        "profile_sha256": "8363dae6bafb1c6555963262b0383ff094053a8f64ab4f0aab54b62b68c87f42",
        "schema_version": "active_alpha_research_ladder/1.0.0",
    }


def test_rebound_ladder_loads_without_semantic_reinterpretation() -> None:
    contract, profile = load_active_ladder(ROOT)
    assert contract["contract_id"] == "53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815"
    assert profile["profile_id"] == "18fbb7a3a405ee2bcaef5dd7d6e757cfb3a69ec8485afd34e5fcf1f627aaeca6"
    assert contract["stages"]["tier_2"]["markets"] == ["ES", "NQ", "CL", "NG", "RB", "GC", "HG", "SR3", "ZN", "ZB", "6E", "6J", "ZC", "ZS", "LE", "HE"]
    assert contract["stages"]["tier_3"]["satellite_can_rescue_traditional_failure"] is False


def test_final_252_authority_is_unresolved_and_access_forbidden() -> None:
    assert not FINAL_MANIFEST.exists()
    for name in ("CURRENT_WORKFLOW.md", "PROJECT_OUTLINE.md", "README.md"):
        text = (ROOT / name).read_text(encoding="utf-8")
        assert "UNRESOLVED_AUTHORITY_HOLDOUT_ACCESS_FORBIDDEN" in text


def test_recovery_does_not_fabricate_the_broken_successor_targets() -> None:
    broken_root = ROOT / "state/alpha_ladder_registry/d7e2b182f4c27fdc09876b8140673988648628c1065d89b768c9dd1954cd362f"
    assert not (broken_root / "universe_contract.json").exists()
    assert not (broken_root / "alpha_tiered.yaml").exists()
    assert not FINAL_MANIFEST.exists()


def test_rebound_authority_closure_is_fresh_checkout_recoverable() -> None:
    report = validate_active_alpha_authority_closure(ROOT)
    assert report["valid"] is True
    assert report["contract_id"] == "53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815"

def test_rebound_pointer_materializes_from_exact_git_blob() -> None:
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
    assert materialized == pointer_path.read_bytes()
