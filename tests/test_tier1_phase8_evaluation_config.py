import json
from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_phase8_evaluation_config import (
    load_tier1_phase8_evaluation_config,
)


ROOT = Path(__file__).parents[1]


def test_default_tier1_configuration_is_apex_bound_and_complete() -> None:
    config, config_hash = load_tier1_phase8_evaluation_config(root=ROOT)

    assert config["risk_profile_id"] == "apex_eod_performance_50k"
    assert set(config["costs"]["base"]) == {"ES", "CL", "ZN", "6E"}
    assert len(config_hash) == 64
    assert config["costs"]["execution_connection"] == "Tradovate"
    assert config["costs"]["evaluation_result_label"] == "PROVISIONAL_EXECUTION_COSTS"
    assert config["costs"]["base"]["ZN"]["all_in_fee_per_side_usd"] == "2.50"
    assert config["margin"]["treatment"] == "NOT_USED_FOR_FIXED_RISK_RESEARCH_EVALUATION"
    assert config["margin"]["live_buying_power_validation"] is False
    assert config["position_sizing"]["risk_per_new_position_usd"] == 250
    assert config["position_sizing"]["maximum_entries_per_session"] == 3
    assert config["concentration_limits"]["daily_stop_loss_usd"] == 500
    assert config["concentration_limits"]["maximum_total_drawdown_usd"] == 1500
    assert config["bracket_exit_policy"]["maximum_hold_minutes"] == 60


def test_configuration_rejects_lower_than_one_tick_base_slippage(tmp_path) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    for filename in ("prop_firm_risk_profile.json", "tier1_phase8_evaluation.json"):
        (config_dir / filename).write_text((ROOT / "configs" / filename).read_text(encoding="utf-8"), encoding="utf-8")
    config_path = config_dir / "tier1_phase8_evaluation.json"
    document = json.loads(config_path.read_text(encoding="utf-8"))
    document["costs"]["base"]["ES"]["round_trip_slippage_ticks_per_contract"] = 0
    config_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(IntegrityError, match="at least one round-trip tick"):
        load_tier1_phase8_evaluation_config(root=tmp_path)


def test_configuration_rejects_drawdown_without_external_reserve(tmp_path) -> None:
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    for filename in ("prop_firm_risk_profile.json", "tier1_phase8_evaluation.json"):
        (config_dir / filename).write_text((ROOT / "configs" / filename).read_text(encoding="utf-8"), encoding="utf-8")
    config_path = config_dir / "tier1_phase8_evaluation.json"
    profile_path = config_dir / "prop_firm_risk_profile.json"
    document = json.loads(config_path.read_text(encoding="utf-8"))
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    document["concentration_limits"]["maximum_total_drawdown_usd"] = 2000
    profile["profiles"]["apex_eod_performance_50k"]["project_limits"]["maximum_total_drawdown_usd"] = 2000
    config_path.write_text(json.dumps(document), encoding="utf-8")
    profile_path.write_text(json.dumps(profile), encoding="utf-8")

    with pytest.raises(IntegrityError, match="firm-risk reserve"):
        load_tier1_phase8_evaluation_config(root=tmp_path)
