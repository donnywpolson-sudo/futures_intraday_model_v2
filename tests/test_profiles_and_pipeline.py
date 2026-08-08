from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml

from futures_rebuild.pipeline import PHASES, main, run_synthetic_pipeline
from futures_rebuild.profiles import ProfileContractError, validate_profiles


ROOT = Path(__file__).resolve().parents[1]
PROFILE = ROOT / "configs" / "alpha_tiered.yaml"


def _write_profile(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def test_profile_view_is_exactly_41_markets_and_non_authorizing() -> None:
    result = validate_profiles(PROFILE, repository_root=ROOT)
    assert result["market_count"] == 41
    assert result["traditional_market_count"] == 38
    assert result["satellite_market_count"] == 3
    assert all(value is False for value in result["authority"].values())


def test_profile_cannot_expand_universe_or_unlock_holdout(tmp_path: Path) -> None:
    original = yaml.safe_load(PROFILE.read_text(encoding="utf-8"))
    expanded = copy.deepcopy(original)
    expanded["market_sets"]["core"].append("NOT_APPROVED")
    with pytest.raises(ProfileContractError, match="expands"):
        validate_profiles(
            _write_profile(tmp_path / "expanded.yaml", expanded),
            repository_root=ROOT,
        )

    unlocked = copy.deepcopy(original)
    unlocked["cohorts"]["holdout"]["locked"] = False
    with pytest.raises(ProfileContractError, match="locked"):
        validate_profiles(
            _write_profile(tmp_path / "unlocked.yaml", unlocked),
            repository_root=ROOT,
        )


def test_full_pipeline_proves_all_phases_without_granting_authority() -> None:
    result = run_synthetic_pipeline(profile_path=PROFILE, repository_root=ROOT)
    assert result["success"] is True
    assert tuple(item["phase"] for item in result["phases"]) == PHASES
    assert all(item["synthetic_only"] is True for item in result["phases"])
    assert all(item["alpha_evidence"] is False for item in result["phases"])
    assert all(value is False for value in result["authority"].values())
    assert result["phases"][-2]["state"] == "PASS_GUARD_CLOSED"
    assert result["phases"][-1]["state"] == "PASS_GUARD_CLOSED"


def test_cli_blocks_real_history_before_output(tmp_path: Path) -> None:
    output = tmp_path / "must-not-exist.json"
    with pytest.raises(SystemExit, match="BLOCKED"):
        main(["--real-history", "--output", str(output), "smoke"])
    assert not output.exists()


def test_cli_profile_validation_includes_the_active_alpha_ladder(capsys) -> None:
    assert main(["validate-profiles"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["active_alpha_ladder"] == {
        "contract_id": "53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815",
        "profile_id": "18fbb7a3a405ee2bcaef5dd7d6e757cfb3a69ec8485afd34e5fcf1f627aaeca6",
        "state": "ACTIVE_HASH_BOUND",
    }


def test_cli_output_is_content_addressed_and_no_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "smoke.json"
    assert main(["--output", str(output), "smoke"]) == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["mode"] == "SYNTHETIC_MECHANICS_ONLY"
    with pytest.raises(FileExistsError):
        main(["--output", str(output), "smoke"])
