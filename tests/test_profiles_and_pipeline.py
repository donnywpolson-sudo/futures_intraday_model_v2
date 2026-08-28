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
    phase8 = next(item for item in result["phases"] if item["phase"] == "8")
    assert phase8["evidence"]["prop_firm_profile_id"] == (
        "mff_rapid_eod_50k_2026_08_10"
    )
    assert phase8["evidence"]["account_stage"] == "sim_funded"
    assert phase8["evidence"]["exact_provider_account_costs_verified"] is False
    assert len(phase8["evidence"]["prop_firm_runtime_identity"]["cache_identity"]) == 64
    assert (
        phase8["evidence"]["evaluation_result_label"]
        == "UNRESOLVED_SELECTED_PROVIDER_ACCOUNT_COSTS"
    )
    assert phase8["evidence"]["evaluation_authorized"] is False


def test_cli_blocks_real_history_before_output(tmp_path: Path) -> None:
    output = tmp_path / "must-not-exist.json"
    with pytest.raises(SystemExit, match="BLOCKED"):
        main(["--real-history", "--output", str(output), "smoke"])
    assert not output.exists()


def test_cli_profile_validation_includes_the_active_alpha_ladder(capsys) -> None:
    assert main(["validate-profiles"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["active_alpha_ladder"] == {
        "contract_id": "c053756ad32d722e290b2ab1d95c97f1ee070a29afba4b1daaeda819d751c18b",
        "profile_id": "afcb79549f4dd45e7853c54db35ead3ea224d48a2b79028a2b8dadd241969d44",
        "state": "ACTIVE_HASH_BOUND",
    }
    assert payload["active_prop_firm_profile"]["profile_id"] == (
        "mff_rapid_eod_50k_2026_08_10"
    )
    assert payload["active_prop_firm_profile"]["account_stage"] == "sim_funded"
    assert payload["active_prop_firm_profile"]["production_readiness"] is False
    assert payload["active_prop_firm_profile"]["state"] == (
        "SELECTED_NON_AUTHORIZING_PRODUCTION_BLOCKED"
    )


def test_cli_exposes_generic_prop_firm_prepare_only_interfaces(capsys) -> None:
    assert main(["prop-firm-risk-policy"]) == 0
    policy = json.loads(capsys.readouterr().out)
    assert policy["schema_version"] == "prop_firm_eod_risk_policy/2.0.0"
    assert policy["profile_id"] == "mff_rapid_eod_50k_2026_08_10"
    assert policy["account_stage"] == "sim_funded"
    assert policy["production_readiness"] is False
    assert all(value is False for value in policy["authority"].values())

    assert main(["prop-firm-phase8"]) == 0
    phase8 = json.loads(capsys.readouterr().out)
    assert phase8["schema_version"] == "prop_firm_phase8_preparation/2.0.0"
    assert phase8["state"] == (
        "PREPARED_MODEL_EVALUATION_NOT_AUTHORIZED_PRODUCTION_BLOCKED"
    )
    assert "exact_apex_live_costs_verified" not in phase8
    assert all(value is False for value in phase8["authority"].values())


def test_cli_lists_generic_prop_firm_interfaces(capsys) -> None:
    assert main(["list"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["preparation_interfaces"] == [
        "prop-firm-risk-policy",
        "prop-firm-phase8",
    ]


def test_cli_output_is_content_addressed_and_no_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "smoke.json"
    assert main(["--output", str(output), "smoke"]) == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["mode"] == "SYNTHETIC_MECHANICS_ONLY"
    with pytest.raises(FileExistsError):
        main(["--output", str(output), "smoke"])
