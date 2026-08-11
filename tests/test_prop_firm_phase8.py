import shutil
from pathlib import Path

import pytest

from futures_rebuild.errors import ContractError
from futures_rebuild.prop_firm_phase8 import (
    build_phase8_preparation,
    validate_phase8_result_label,
)


ROOT = Path(__file__).parents[1]


def _copy_generic_configs(target: Path) -> None:
    config_dir = target / "configs"
    config_dir.mkdir(parents=True)
    for name in (
        "prop_firm_profiles.json",
        "prop_firm_phase8_evaluation.json",
        "prop_firm_strategy_risk_policies.json",
        "prop_firm_execution_instruments.json",
        "prop_firm_execution_costs.json",
        "prop_firm_payout_policies.json",
    ):
        shutil.copyfile(ROOT / "configs" / name, config_dir / name)


def test_phase8_resolves_stage_mapping_hashes_and_unset_cost_blocker() -> None:
    preparation = build_phase8_preparation(root=ROOT)
    assert preparation["schema_version"] == "prop_firm_phase8_preparation/2.0.0"
    assert preparation["profile_id"] == "mff_rapid_eod_50k_2026_08_10"
    assert preparation["account_stage"] == "sim_funded"
    assert preparation["enabled_signal_roots"] == ["ES", "CL", "6E"]
    assert preparation["execution_dispositions"]["ZN"]["enabled"] is False
    assert preparation["round_turn_commission_usd"] == {
        "MES": "UNSET",
        "MCL": "UNSET",
        "M6E": "UNSET",
    }
    assert preparation["exact_provider_account_costs_verified"] is False
    assert preparation["evaluation_result_label"] == (
        "UNRESOLVED_SELECTED_PROVIDER_ACCOUNT_COSTS"
    )
    assert preparation["production_readiness"] is False
    assert len(preparation["runtime_identity"]["cache_identity"]) == 64
    assert all(value is False for value in preparation["authority"].values())


def test_phase8_result_label_is_provider_neutral_and_fail_closed() -> None:
    preparation = build_phase8_preparation(root=ROOT)
    validate_phase8_result_label(
        preparation=preparation,
        observed_label="UNRESOLVED_SELECTED_PROVIDER_ACCOUNT_COSTS",
    )
    with pytest.raises(ContractError, match="does not match"):
        validate_phase8_result_label(
            preparation=preparation,
            observed_label="EXACT_SELECTED_PROVIDER_ACCOUNT_COSTS",
        )


def test_phase8_requires_explicit_sim_funded_binding(tmp_path: Path) -> None:
    _copy_generic_configs(tmp_path)
    phase8_path = tmp_path / "configs" / "prop_firm_phase8_evaluation.json"
    text = phase8_path.read_text(encoding="utf-8").replace(
        '"account_stage": "sim_funded"', '"account_stage": "live"'
    )
    phase8_path.write_text(text, encoding="utf-8")
    with pytest.raises(ContractError, match="strategy policy account stage"):
        build_phase8_preparation(root=tmp_path)


def test_generic_phase8_interfaces_contain_no_provider_brand() -> None:
    paths = (
        ROOT / "configs" / "prop_firm_phase8_evaluation.json",
        ROOT / "src" / "futures_rebuild" / "prop_firm_phase8.py",
    )
    assert all("apex" not in path.read_text(encoding="utf-8").lower() for path in paths)
