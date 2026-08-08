"""Local validation for the non-executing Tier 1 Phase 8 configuration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from .canonical import sha256_file
from .errors import IntegrityError


CONFIG_RELATIVE_PATH = Path("configs/tier1_phase8_evaluation.json")
RISK_PROFILE_RELATIVE_PATH = Path("configs/prop_firm_risk_profile.json")
CORE_MARKETS = ("ES", "CL", "ZN", "6E")
REQUIRED_BASELINES = {
    "flat_no_trade",
    "fold_local_unconditional_return_by_market_session",
    "previous_bar_sign_momentum",
    "previous_bar_sign_reversal",
    "risk_matched_always_long_intraday",
}
IDENTICAL_FIXED_RISK_COMPARATOR = "equal_risk_version_of_candidate_signal"


def _read_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"cannot read Phase 8 configuration: {path.name}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("Phase 8 configuration must be a JSON object")
    return value


def load_tier1_phase8_evaluation_config(*, root: Path) -> tuple[dict[str, object], str]:
    """Load and fail closed on a config that exceeds the active Apex profile."""

    config_path = root / CONFIG_RELATIVE_PATH
    profile_path = root / RISK_PROFILE_RELATIVE_PATH
    config = _read_object(config_path)
    profile_document = _read_object(profile_path)
    profile_id = profile_document.get("active_profile_id")
    profiles = profile_document.get("profiles")
    if not isinstance(profile_id, str) or not isinstance(profiles, dict):
        raise IntegrityError("active prop-firm risk profile is invalid")
    profile = profiles.get(profile_id)
    if not isinstance(profile, dict) or config.get("risk_profile_id") != profile_id:
        raise IntegrityError("Phase 8 configuration must use the active prop-firm risk profile")
    if tuple(config.get("markets", ())) != CORE_MARKETS or config.get("period") != "2018-2022":
        raise IntegrityError("Phase 8 configuration must cover the fixed Tier 1 scope")

    costs = config.get("costs")
    if not isinstance(costs, dict) or costs.get("assumption_status") not in {
        "PROVISIONAL_APEX_TRADOVATE_ZN_FEE",
        "EXACT_APEX_TRADOVATE_SCHEDULE",
    }:
        raise IntegrityError("Phase 8 costs must declare their Apex schedule status")
    base = costs.get("base")
    if not isinstance(base, dict) or set(base) != set(CORE_MARKETS):
        raise IntegrityError("Phase 8 costs must specify every Tier 1 market")
    for market in CORE_MARKETS:
        entry = base[market]
        if not isinstance(entry, dict) or not isinstance(entry.get("round_trip_slippage_ticks_per_contract"), int) or entry["round_trip_slippage_ticks_per_contract"] < 1:
            raise IntegrityError("Phase 8 base costs require at least one round-trip tick")
    if costs.get("execution_connection") != "Tradovate" or not isinstance(costs.get("official_source"), str):
        raise IntegrityError("Phase 8 exact costs require the selected Apex connection and source")
    if costs["assumption_status"] == "PROVISIONAL_APEX_TRADOVATE_ZN_FEE":
        zn = base["ZN"]
        if (
            costs.get("evaluation_result_label") != "PROVISIONAL_EXECUTION_COSTS"
            or costs.get("exact_apex_live_costs_verified") is not False
            or zn.get("fee_status") != "PROVISIONAL_CONSERVATIVE_PRE_ACCOUNT_ASSUMPTION"
            or zn.get("all_in_fee_per_side_usd") != "2.50"
        ):
            raise IntegrityError("Phase 8 provisional ZN costs require an explicit provisional label")

    sizing = config.get("position_sizing")
    concentration = config.get("concentration_limits")
    margin = config.get("margin")
    project_limits = profile.get("project_limits")
    if not isinstance(sizing, dict) or not isinstance(concentration, dict) or not isinstance(project_limits, dict):
        raise IntegrityError("Phase 8 sizing or risk limits are invalid")
    if margin != {
        "treatment": "NOT_USED_FOR_FIXED_RISK_RESEARCH_EVALUATION",
        "live_buying_power_validation": False,
        "source": "outside_this_historical_research_lane",
    }:
        raise IntegrityError("Phase 8 margin must remain outside the fixed-risk research lane")
    for name in ("maximum_standard_contract_equivalents",):
        if sizing.get(name) != project_limits.get(name):
            raise IntegrityError("Phase 8 position size must not exceed the active project limit")
    for name in ("daily_stop_loss_usd", "maximum_total_drawdown_usd"):
        if concentration.get(name) != project_limits.get(name):
            raise IntegrityError("Phase 8 loss limit must match the active project limit")
    if (
        sizing.get("method") != "atr_bracket_fixed_one_contract"
        or sizing.get("risk_per_new_position_usd") != project_limits.get("maximum_initial_risk_usd")
        or sizing.get("maximum_open_risk_usd") != project_limits.get("maximum_initial_risk_usd")
        or sizing.get("maximum_entries_per_session") != project_limits.get("maximum_entries_per_session")
        or sizing.get("admission_cost_scenario") != "stress"
    ):
        raise IntegrityError("Phase 8 sizing must use the locked ATR-bracket admission limits")
    firm_limits = profile.get("firm_limits")
    if not isinstance(firm_limits, dict) or not isinstance(firm_limits.get("drawdown"), dict):
        raise IntegrityError("active prop-firm drawdown limit is invalid")
    external_drawdown = firm_limits["drawdown"].get("maximum_usd")
    if (
        concentration.get("external_firm_drawdown_usd") != external_drawdown
        or concentration.get("external_drawdown_reserve_usd") != project_limits.get("external_drawdown_reserve_usd")
        or concentration.get("maximum_total_drawdown_usd") + concentration.get("external_drawdown_reserve_usd")
        != external_drawdown
    ):
        raise IntegrityError("Phase 8 internal drawdown must retain the locked firm-risk reserve")
    bracket = config.get("bracket_exit_policy")
    if bracket != {
        "atr_method": "wilder",
        "atr_lookback_completed_bars": 20,
        "atr_multiple": "1.5",
        "profit_target_net_r_multiple": "2",
        "maximum_hold_minutes": 60,
        "session_roll": "17:00 America/Chicago",
        "session_end_fill": "last_verified_eligible_bar_before_roll",
        "intrabar_stop_target_collision": "stop_first",
        "entry_blackout_minutes_before_session_roll": 60,
        "missing_or_roll_behavior": "abstain_or_flatten_never_invent_price",
    }:
        raise IntegrityError("Phase 8 bracket exit policy is incomplete or drifted")

    metrics = config.get("pass_fail_metrics")
    if not isinstance(metrics, dict) or metrics.get("required_cost_scenario_for_promotion") != "stress":
        raise IntegrityError("Phase 8 promotion must require stress costs")
    if set(metrics.get("must_beat_after_costs", ())) != REQUIRED_BASELINES:
        raise IntegrityError("Phase 8 must compare against its required baselines")
    if metrics.get("equivalence_checks") != [IDENTICAL_FIXED_RISK_COMPARATOR]:
        raise IntegrityError("Phase 8 must retain its identical fixed-risk comparator")
    return config, sha256_file(config_path)
