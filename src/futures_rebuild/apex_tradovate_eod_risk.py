"""Fail-closed Apex 50K EOD PA risk and commission policy.

This is a pre-mechanism policy.  It deliberately leaves the owner's fixed
per-trade ``R`` and emergency reserve unset.  It never turns an unsupported
instrument into a tradable one by inventing a commission.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Mapping

from .canonical import sha256_json
from .errors import ContractError


APEX_COMMISSION_SOURCE = (
    "https://apextraderfunding.com/help-center/tradovate/"
    "tradovate-commission-instruments/"
)
APEX_EOD_RULE_SOURCE = (
    "https://apextraderfunding.com/help-center/"
    "eod-trailing-drawdown-accounts/eod-performance-accounts-pa/"
)
SOURCE_VERIFIED_DATE = "2026-08-08"

FULL_41 = (
    "ES", "NQ", "RTY", "YM", "CL", "NG", "RB", "HO", "GC", "SI",
    "HG", "PL", "SR3", "SR1", "ZQ", "TN", "ZT", "ZF", "ZN", "ZB",
    "UB", "6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S", "ZC",
    "ZS", "ZL", "ZM", "ZW", "KE", "LE", "HE", "GF", "BTC", "ETH",
    "PA",
)

SUPPORTED_FULL_CONTRACTS = (
    "ES", "NQ", "RTY", "YM", "CL", "NG", "RB", "HO", "GC", "SI",
    "HG", "PL", "PA", "6A", "6B", "6C", "6E", "6J", "6N", "6S",
    "ZC", "ZS", "ZL", "ZM", "ZW", "LE", "HE", "GF",
)

UNSUPPORTED_FULL_CONTRACTS = (
    "6M", "BTC", "ETH", "KE", "SR1", "SR3", "TN", "UB", "ZB", "ZF",
    "ZN", "ZQ", "ZT",
)

# Official Apex-Tradovate PA round-turn commissions for supported full
# contracts in the project's universe.  RB and HO are listed as supported by
# Apex but have no amount in the published commission table, so they use the
# conservative fallback below.
OFFICIAL_ROUND_TURN_USD = {
    "ES": "3.10", "NQ": "3.10", "RTY": "3.10", "YM": "3.10",
    "CL": "3.34", "NG": "3.54",
    "GC": "3.54", "SI": "3.54", "HG": "3.54", "PL": "3.54", "PA": "3.54",
    "6A": "3.54", "6B": "3.54", "6C": "3.54", "6E": "3.54",
    "6J": "3.54", "6N": "3.54", "6S": "3.54",
    "ZC": "4.54", "ZS": "4.54", "ZL": "4.54", "ZM": "4.54",
    "ZW": "4.54", "LE": "4.54", "HE": "4.54", "GF": "4.54",
}
FALLBACK_ROUND_TURN_USD = "4.54"
FALLBACK_COMMISSION_MARKETS = ("HO", "RB")

STARTING_BALANCE_USD = Decimal("50000")
MAX_EOD_DRAWDOWN_USD = Decimal("2000")
LEVEL_1_DLL_USD = Decimal("1000")
INITIAL_EOD_THRESHOLD_USD = Decimal("48000")
LOCKED_EOD_THRESHOLD_USD = Decimal("50100")
LOCK_TRIGGER_EOD_BALANCE_USD = Decimal("52100")


def _money(value: object, *, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ContractError(f"{name} must be an exact decimal") from exc
    if not result.is_finite():
        raise ContractError(f"{name} must be finite")
    return result


def round_turn_commission(market: str) -> Decimal:
    """Return the frozen PA commission, failing closed if not tradable."""

    if market in UNSUPPORTED_FULL_CONTRACTS or market not in FULL_41:
        raise ContractError(f"{market} is not a supported Apex-Tradovate full contract")
    if market in OFFICIAL_ROUND_TURN_USD:
        return Decimal(OFFICIAL_ROUND_TURN_USD[market])
    if market in FALLBACK_COMMISSION_MARKETS:
        return Decimal(FALLBACK_ROUND_TURN_USD)
    raise ContractError(f"{market} has no fail-closed commission disposition")


def update_eod_threshold(*, previous_threshold_usd: object, highest_eod_balance_usd: object) -> Decimal:
    """Apply the Apex 50K PA EOD trail and permanent $50,100 lock."""

    previous = _money(previous_threshold_usd, name="previous EOD threshold")
    highest = _money(highest_eod_balance_usd, name="highest EOD balance")
    if previous < INITIAL_EOD_THRESHOLD_USD or previous > LOCKED_EOD_THRESHOLD_USD:
        raise ContractError("previous EOD threshold is outside the 50K PA bounds")
    if highest < STARTING_BALANCE_USD:
        raise ContractError("highest EOD balance cannot be below the starting balance")
    candidate = highest - MAX_EOD_DRAWDOWN_USD
    return min(LOCKED_EOD_THRESHOLD_USD, max(previous, candidate))


def build_owner_limits(*, r_usd: object, emergency_reserve_usd: object, owner_attestation: str) -> dict[str, str]:
    """Bind owner-selected limits without deriving them from census passage."""

    if owner_attestation != "OWNER_SELECTED_FROM_ACCOUNT_TOLERANCE_NOT_CENSUS_PASSAGE":
        raise ContractError("exact owner account-tolerance attestation is required")
    risk = _money(r_usd, name="R")
    reserve = _money(emergency_reserve_usd, name="emergency reserve")
    if risk <= 0:
        raise ContractError("R must be positive")
    if reserve <= 0 or reserve >= MAX_EOD_DRAWDOWN_USD:
        raise ContractError("emergency reserve must be positive and below $2,000")
    usable = MAX_EOD_DRAWDOWN_USD - reserve
    if risk > usable:
        raise ContractError("R exceeds the account drawdown after reserve")
    return {
        "r_usd": format(risk, "f"),
        "emergency_reserve_usd": format(reserve, "f"),
        "usable_initial_drawdown_usd": format(usable, "f"),
        "apex_level_1_daily_loss_limit_usd": format(LEVEL_1_DLL_USD, "f"),
        "automatic_2r_daily_limit": "false",
        "automatic_6r_drawdown_limit": "false",
    }


def admission_cap_usd(
    *, r_usd: object, emergency_reserve_usd: object, current_equity_usd: object,
    current_apex_threshold_usd: object, realized_daily_loss_usd: object,
) -> Decimal:
    """Return the strictest account-native loss capacity for one new trade."""

    risk = _money(r_usd, name="R")
    reserve = _money(emergency_reserve_usd, name="emergency reserve")
    equity = _money(current_equity_usd, name="current equity")
    threshold = _money(current_apex_threshold_usd, name="current Apex threshold")
    daily_loss = _money(realized_daily_loss_usd, name="realized daily loss")
    if risk <= 0 or reserve <= 0 or daily_loss < 0:
        raise ContractError("R and reserve must be positive; daily loss cannot be negative")
    drawdown_capacity = equity - threshold - reserve
    dll_capacity = LEVEL_1_DLL_USD - daily_loss
    return max(Decimal("0"), min(risk, drawdown_capacity, dll_capacity))


def planned_loss_usd(*, stop_risk_usd: object, market: str, stress_slippage_usd: object) -> Decimal:
    """Add stop risk, exact/fallback PA commission, and locked stress slippage."""

    stop = _money(stop_risk_usd, name="stop risk")
    slippage = _money(stress_slippage_usd, name="stress slippage")
    if stop < 0 or slippage < 0:
        raise ContractError("stop risk and stress slippage cannot be negative")
    return stop + round_turn_commission(market) + slippage


def build_draft_policy() -> dict[str, object]:
    commission_dispositions: dict[str, Mapping[str, str]] = {}
    for market in FULL_41:
        if market in UNSUPPORTED_FULL_CONTRACTS:
            commission_dispositions[market] = {
                "disposition": "UNSUPPORTED_APEX_TRADOVATE_FULL_CONTRACT",
            }
        elif market in FALLBACK_COMMISSION_MARKETS:
            commission_dispositions[market] = {
                "disposition": "SUPPORTED_CONSERVATIVE_FALLBACK",
                "round_turn_usd": FALLBACK_ROUND_TURN_USD,
            }
        else:
            commission_dispositions[market] = {
                "disposition": "SUPPORTED_OFFICIAL_SCHEDULE",
                "round_turn_usd": OFFICIAL_ROUND_TURN_USD[market],
            }
    core: dict[str, object] = {
        "schema_version": "apex_tradovate_50k_eod_risk_policy/1.0.0",
        "state": "PREPARED_AWAITING_OWNER_R_AND_EMERGENCY_RESERVE",
        "account": {
            "provider": "APEX_TRADER_FUNDING",
            "platform": "TRADOVATE",
            "account_type": "50K_EOD_PERFORMANCE_ACCOUNT",
            "starting_balance_usd": "50000",
            "initial_level": "LEVEL_1",
            "maximum_strategy_contracts": 1,
            "maximum_strategy_trades_per_session": 1,
            "full_contracts_only": True,
        },
        "external_hard_limits": {
            "maximum_eod_drawdown_usd": "2000",
            "initial_eod_threshold_usd": "48000",
            "locked_eod_threshold_usd": "50100",
            "lock_trigger_highest_eod_balance_usd": "52100",
            "level_1_daily_loss_limit_usd": "1000",
            "threshold_enforced_intraday": True,
        },
        "internal_owner_limits": {
            "r_usd": None,
            "emergency_reserve_usd": None,
            "automatic_2r_daily_limit": False,
            "automatic_6r_drawdown_limit": False,
            "admission_cap": "MIN(R, EQUITY_MINUS_APEX_THRESHOLD_MINUS_RESERVE, DLL_REMAINING)",
        },
        "cost_policy": {
            "planned_loss": "STOP_RISK_PLUS_ROUND_TURN_COMMISSION_PLUS_LOCKED_STRESS_SLIPPAGE",
            "fallback_round_turn_usd": FALLBACK_ROUND_TURN_USD,
            "fallback_basis": "MAXIMUM_PUBLISHED_ROUND_TURN_AMONG_SUPPORTED_FULL_CONTRACTS_IN_41_MARKET_UNIVERSE",
            "fallback_markets": list(FALLBACK_COMMISSION_MARKETS),
            "unsupported_markets_never_receive_fallback": True,
            "commission_dispositions": commission_dispositions,
            "evaluation_activation_and_data_fees": "SEPARATE_OWNER_ECONOMICS_NOT_PER_TRADE",
        },
        "sources": {
            "commission_schedule": APEX_COMMISSION_SOURCE,
            "eod_pa_rules": APEX_EOD_RULE_SOURCE,
            "verified_date": SOURCE_VERIFIED_DATE,
        },
        "authority": {
            "historical_rows": False,
            "returns": False,
            "mechanism_creation": False,
            "registration": False,
            "activation": False,
            "publication": False,
            "trading": False,
        },
    }
    return {**core, "policy_id": sha256_json(core)}


def validate_draft_policy(policy: Mapping[str, object]) -> None:
    if dict(policy) != build_draft_policy():
        raise ContractError("Apex-Tradovate EOD draft policy drifted")
    if set(FULL_41) != set(SUPPORTED_FULL_CONTRACTS) | set(UNSUPPORTED_FULL_CONTRACTS):
        raise ContractError("41-market support partition is incomplete")
    if set(SUPPORTED_FULL_CONTRACTS) & set(UNSUPPORTED_FULL_CONTRACTS):
        raise ContractError("supported and unsupported markets overlap")
    if Decimal(FALLBACK_ROUND_TURN_USD) != max(map(Decimal, OFFICIAL_ROUND_TURN_USD.values())):
        raise ContractError("fallback is not the most expensive published round turn")
