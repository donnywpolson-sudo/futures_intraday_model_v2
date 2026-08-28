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


def test_qualification_successor_pointer_is_exact_and_non_authorizing() -> None:
    pointer = json.loads((ROOT / "configs/active_alpha_research_ladder.json").read_text(encoding="utf-8"))
    assert pointer == {
        "contract_id": "c053756ad32d722e290b2ab1d95c97f1ee070a29afba4b1daaeda819d751c18b",
        "contract_path": "state/alpha_ladder_registry/c053756ad32d722e290b2ab1d95c97f1ee070a29afba4b1daaeda819d751c18b/universe_contract.json",
        "contract_sha256": "8587086283e57030a878217a7502cac681a1a85079ec86cf0dbb7ae424d6b6bb",
        "pointer_id": "7013c6cb37e02674de7874996041fd1ec3b8233668bd370330c2934726a8de40",
        "profile_id": "afcb79549f4dd45e7853c54db35ead3ea224d48a2b79028a2b8dadd241969d44",
        "profile_path": "state/alpha_ladder_registry/c053756ad32d722e290b2ab1d95c97f1ee070a29afba4b1daaeda819d751c18b/alpha_tiered.yaml",
        "profile_sha256": "26ba99aefbbb73cd59d53f35f014ebc3b8695be650ca27cdb4d8b7a00c9ae685",
        "schema_version": "active_alpha_research_ladder/1.0.0",
    }


def test_qualification_successor_loads_without_reinterpreting_the_predecessor() -> None:
    contract, profile = load_active_ladder(ROOT)
    assert contract["contract_id"] == "c053756ad32d722e290b2ab1d95c97f1ee070a29afba4b1daaeda819d751c18b"
    assert profile["profile_id"] == "afcb79549f4dd45e7853c54db35ead3ea224d48a2b79028a2b8dadd241969d44"
    assert contract["predecessor"]["contract_id"] == "53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815"
    assert contract["stages"]["tier_2"]["evaluation_pack"] == ["ES", "NQ", "CL", "NG", "RB", "GC", "HG", "SR3", "ZN", "ZB", "6E", "6J", "ZC", "ZS", "LE", "HE"]
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


def test_successor_authority_closure_is_fresh_checkout_recoverable() -> None:
    report = validate_active_alpha_authority_closure(ROOT)
    assert report["valid"] is True
    assert report["contract_id"] == "c053756ad32d722e290b2ab1d95c97f1ee070a29afba4b1daaeda819d751c18b"

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
