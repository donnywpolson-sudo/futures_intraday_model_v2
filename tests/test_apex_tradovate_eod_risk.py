import json
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.apex_tradovate_eod_risk import (
    FALLBACK_COMMISSION_MARKETS,
    FULL_41,
    OFFICIAL_ROUND_TURN_USD,
    SUPPORTED_FULL_CONTRACTS,
    UNSUPPORTED_FULL_CONTRACTS,
    admission_cap_usd,
    build_draft_policy,
    build_owner_limits,
    planned_loss_usd,
    round_turn_commission,
    update_eod_threshold,
    validate_draft_policy,
)
from futures_rebuild.errors import ContractError


def test_exact_41_market_support_partition() -> None:
    assert len(FULL_41) == 41
    assert len(SUPPORTED_FULL_CONTRACTS) == 28
    assert len(UNSUPPORTED_FULL_CONTRACTS) == 13
    assert set(FULL_41) == set(SUPPORTED_FULL_CONTRACTS) | set(UNSUPPORTED_FULL_CONTRACTS)
    assert not set(SUPPORTED_FULL_CONTRACTS) & set(UNSUPPORTED_FULL_CONTRACTS)


def test_official_and_conservative_fallback_commissions() -> None:
    assert round_turn_commission("ES") == Decimal("3.10")
    assert round_turn_commission("CL") == Decimal("3.34")
    assert round_turn_commission("6E") == Decimal("3.54")
    assert round_turn_commission("HE") == Decimal("4.54")
    assert set(FALLBACK_COMMISSION_MARKETS) == {"HO", "RB"}
    assert round_turn_commission("HO") == Decimal("4.54")
    assert round_turn_commission("RB") == Decimal("4.54")
    assert Decimal("4.54") == max(map(Decimal, OFFICIAL_ROUND_TURN_USD.values()))


@pytest.mark.parametrize("market", UNSUPPORTED_FULL_CONTRACTS)
def test_fallback_never_makes_unsupported_market_tradable(market: str) -> None:
    with pytest.raises(ContractError, match="not a supported"):
        round_turn_commission(market)


def test_planned_loss_includes_commission_and_stress_slippage() -> None:
    assert planned_loss_usd(
        stop_risk_usd="250", market="ES", stress_slippage_usd="25"
    ) == Decimal("278.10")


def test_eod_threshold_trails_highest_close_and_locks() -> None:
    assert update_eod_threshold(
        previous_threshold_usd="48000", highest_eod_balance_usd="50800"
    ) == Decimal("48800")
    assert update_eod_threshold(
        previous_threshold_usd="48800", highest_eod_balance_usd="52100"
    ) == Decimal("50100")
    assert update_eod_threshold(
        previous_threshold_usd="50100", highest_eod_balance_usd="54000"
    ) == Decimal("50100")


def test_owner_limits_have_no_automatic_2r_or_6r_relationship() -> None:
    limits = build_owner_limits(
        r_usd="300",
        emergency_reserve_usd="400",
        owner_attestation="OWNER_SELECTED_FROM_ACCOUNT_TOLERANCE_NOT_CENSUS_PASSAGE",
    )
    assert limits["r_usd"] == "300"
    assert limits["usable_initial_drawdown_usd"] == "1600"
    assert limits["automatic_2r_daily_limit"] == "false"
    assert limits["automatic_6r_drawdown_limit"] == "false"


@pytest.mark.parametrize(
    ("risk", "reserve", "attestation"),
    [
        ("0", "500", "OWNER_SELECTED_FROM_ACCOUNT_TOLERANCE_NOT_CENSUS_PASSAGE"),
        ("300", "0", "OWNER_SELECTED_FROM_ACCOUNT_TOLERANCE_NOT_CENSUS_PASSAGE"),
        ("1800", "500", "OWNER_SELECTED_FROM_ACCOUNT_TOLERANCE_NOT_CENSUS_PASSAGE"),
        ("300", "500", "SELECTED_TO_PASS_CENSUS"),
    ],
)
def test_owner_limits_fail_closed(risk: str, reserve: str, attestation: str) -> None:
    with pytest.raises(ContractError):
        build_owner_limits(
            r_usd=risk, emergency_reserve_usd=reserve, owner_attestation=attestation
        )


def test_admission_cap_is_strictest_apex_native_constraint() -> None:
    assert admission_cap_usd(
        r_usd="300",
        emergency_reserve_usd="400",
        current_equity_usd="50000",
        current_apex_threshold_usd="48000",
        realized_daily_loss_usd="0",
    ) == Decimal("300")
    assert admission_cap_usd(
        r_usd="300",
        emergency_reserve_usd="400",
        current_equity_usd="48600",
        current_apex_threshold_usd="48000",
        realized_daily_loss_usd="0",
    ) == Decimal("200")
    assert admission_cap_usd(
        r_usd="300",
        emergency_reserve_usd="400",
        current_equity_usd="50000",
        current_apex_threshold_usd="48000",
        realized_daily_loss_usd="850",
    ) == Decimal("150")


def test_draft_policy_is_premechanism_and_fail_closed() -> None:
    policy = build_draft_policy()
    validate_draft_policy(policy)
    assert policy["state"] == "PREPARED_AWAITING_OWNER_R_AND_EMERGENCY_RESERVE"
    assert policy["internal_owner_limits"]["r_usd"] is None
    assert policy["internal_owner_limits"]["emergency_reserve_usd"] is None
    assert policy["account"]["maximum_strategy_contracts"] == 1
    assert policy["account"]["maximum_strategy_trades_per_session"] == 1
    assert policy["authority"]["historical_rows"] is False
    assert policy["authority"]["mechanism_creation"] is False


def test_prepared_policy_bytes_match_deterministic_builder() -> None:
    root = Path(__file__).resolve().parents[1]
    prepared = json.loads(
        (root / "configs/apex_tradovate_50k_eod_risk_policy.json").read_text(
            encoding="utf-8"
        )
    )
    assert prepared == build_draft_policy()
    validate_draft_policy(prepared)
