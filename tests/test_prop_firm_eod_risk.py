from decimal import Decimal
from copy import deepcopy
from pathlib import Path

import pytest

from futures_rebuild.errors import ContractError
from futures_rebuild.prop_firm_account_runtime import build_runtime_identity, raw_profile
from futures_rebuild.prop_firm_eod_risk import (
    OWNER_ATTESTATION,
    admission_cap_usd,
    build_active_draft_policy,
    build_owner_limits,
    load_active_profile,
    planned_loss_usd,
    round_turn_commission,
    update_eod_threshold,
    validate_draft_policy,
    validate_profile,
)


ROOT = Path(__file__).parents[1]


def test_active_profile_validates_and_resolves_zero_based_sim_funded_stage() -> None:
    profile_id, profile = load_active_profile(root=ROOT)
    validate_profile(profile_id=profile_id, profile=profile)
    assert profile_id == "mff_rapid_eod_50k_2026_08_10"
    assert profile["account_stage"] == "sim_funded"
    assert profile["account"]["starting_balance_usd"] == "0"
    assert profile["external_limits"]["initial_eod_threshold_usd"] == "-2000"


def test_active_costs_are_unset_and_fail_closed() -> None:
    _, profile = load_active_profile(root=ROOT)
    with pytest.raises(ContractError, match="cost is unresolved"):
        round_turn_commission(profile, "ES")
    with pytest.raises(ContractError, match="cost is unresolved"):
        planned_loss_usd(
            profile,
            stop_risk_usd="50",
            market="ES",
            stress_slippage_usd="5",
        )


def test_completed_session_floor_owner_limits_and_admission_cap() -> None:
    _, profile = load_active_profile(root=ROOT)
    assert update_eod_threshold(
        profile,
        previous_threshold_usd="-2000",
        highest_eod_balance_usd="500",
    ) == Decimal("-1500")
    limits = build_owner_limits(
        profile,
        r_usd="100",
        emergency_reserve_usd="400",
        owner_attestation=OWNER_ATTESTATION,
    )
    assert limits["firm_initial_daily_loss_limit_usd"] == "NONE"
    assert admission_cap_usd(
        profile,
        r_usd="100",
        emergency_reserve_usd="400",
        current_equity_usd="-1400",
        current_firm_threshold_usd="-2000",
        realized_daily_loss_usd="0",
    ) == Decimal("100")


def test_active_draft_policy_is_hash_bound_generic_and_production_blocked() -> None:
    profile_id, profile = load_active_profile(root=ROOT)
    identity = build_runtime_identity(root=ROOT)
    policy = build_active_draft_policy(root=ROOT)
    validate_draft_policy(
        policy,
        profile_id=profile_id,
        profile=profile,
        runtime_identity=identity,
    )
    assert policy["schema_version"] == "prop_firm_eod_risk_policy/2.0.0"
    assert policy["account_stage"] == "sim_funded"
    assert policy["cost_policy"]["exact_provider_account_costs_verified"] is False
    assert policy["production_readiness"] is False
    assert policy["runtime_identity"]["cache_identity"] == identity["cache_identity"]
    assert all(value is False for value in policy["authority"].values())


def test_generic_stage_validator_accepts_a_consistent_non_mff_100k_profile() -> None:
    _, original = raw_profile(root=ROOT)
    profile = deepcopy(original)
    profile["profile_schema_id"] = "generic_stage_profile/1.0.0"
    profile["provider_id"] = "synthetic_provider"
    profile["firm"] = "Synthetic Provider"
    profile["program"] = "Synthetic EOD 100K"
    profile["plan"] = "100K EOD"
    profile["official_sources"] = [profile["official_sources"][0]]
    for stage_name, stage in profile["stages"].items():
        stage["nominal_plan_size_usd"] = "100000"
        if stage_name == "evaluation":
            stage["ledger_starting_balance_usd"] = "100000"
            stage["maximum_eod_loss_usd"] = "3000"
            stage["maximum_micros"] = 60
        else:
            stage["ledger_starting_balance_usd"] = "0"
            stage["maximum_eod_loss_usd"] = "3000"
            stage["initial_loss_floor_usd"] = "-3000"
            stage["loss_floor_lock_usd"] = "200" if stage_name == "sim_funded" else "0"
            stage["maximum_micros"] = 60 if stage_name == "sim_funded" else 80
    validate_profile(profile_id="synthetic_eod_100k", profile=profile)


def test_mff_discriminator_remains_exact_and_unknown_discriminators_fail_closed() -> None:
    _, original = raw_profile(root=ROOT)
    drifted = deepcopy(original)
    drifted["stages"]["sim_funded"]["maximum_micros"] = 31
    with pytest.raises(ContractError, match="contract or lock limit drifted"):
        validate_profile(profile_id="mff_rapid_eod_50k_2026_08_10", profile=drifted)
    unknown = deepcopy(original)
    unknown["profile_schema_id"] = "unknown/9.9.9"
    with pytest.raises(ContractError, match="schema discriminator"):
        validate_profile(profile_id="unknown", profile=unknown)
