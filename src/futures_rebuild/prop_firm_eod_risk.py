"""Provider-neutral, fail-closed prop-firm EOD risk policy helpers.

The selected provider and account stage are data.  This module performs only
deterministic local preparation and grants no external or trading authority.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Mapping

from .canonical import sha256_json
from .errors import ContractError
from .prop_firm_account_runtime import (
    PROFILE_PATH,
    build_runtime_identity,
    load_runtime_bindings,
    money,
    mapping,
    nonempty_string,
    raw_profile,
    selected_stage,
    stage_rules,
)


PROFILE_RELATIVE_PATH = PROFILE_PATH
OWNER_ATTESTATION = "OWNER_SELECTED_FROM_ACCOUNT_TOLERANCE_NOT_CENSUS_PASSAGE"


def _money(value: object, *, name: str) -> Decimal:
    return money(value, name=name)


def _mapping(value: object, *, name: str) -> Mapping[str, object]:
    return mapping(value, name=name)


def _string(value: object, *, name: str) -> str:
    return nonempty_string(value, name=name)


def _validate_legacy_profile(*, profile: Mapping[str, object]) -> None:
    connection = _mapping(profile.get("execution_connection"), name="execution_connection")
    _string(connection.get("connection_id"), name="connection_id")
    account = _mapping(profile.get("account"), name="account")
    limits = _mapping(profile.get("external_limits"), name="external_limits")
    access = _mapping(profile.get("market_access"), name="market_access")
    costs = _mapping(profile.get("execution_costs"), name="execution_costs")
    sources = _mapping(profile.get("official_sources"), name="official_sources")
    if not sources:
        raise ContractError("official_sources cannot be empty")

    starting = _money(account.get("starting_balance_usd"), name="starting balance")
    drawdown = _money(limits.get("maximum_eod_drawdown_usd"), name="maximum EOD drawdown")
    initial = _money(limits.get("initial_eod_threshold_usd"), name="initial EOD threshold")
    locked = _money(limits.get("locked_eod_threshold_usd"), name="locked EOD threshold")
    trigger = _money(limits.get("lock_trigger_highest_eod_balance_usd"), name="lock trigger")
    daily = _money(limits.get("initial_daily_loss_limit_usd"), name="daily loss limit")
    if drawdown <= 0 or daily <= 0 or initial != starting - drawdown:
        raise ContractError("external EOD limits are inconsistent")
    if trigger - drawdown != locked or not initial <= locked < trigger:
        raise ContractError("EOD threshold lock is inconsistent")

    universe = access.get("full_contract_universe")
    supported = access.get("supported_full_contracts")
    unsupported = access.get("unsupported_full_contracts")
    if not all(
        isinstance(value, list) and all(isinstance(item, str) for item in value)
        for value in (universe, supported, unsupported)
    ):
        raise ContractError("market-access sets must be string lists")
    if set(universe) != set(supported) | set(unsupported):
        raise ContractError("market-access partition is incomplete")
    if set(supported) & set(unsupported):
        raise ContractError("supported and unsupported markets overlap")

    official = _mapping(costs.get("official_round_turn_usd"), name="official costs")
    fallback_markets = costs.get("fallback_markets")
    if not isinstance(fallback_markets, list):
        raise ContractError("fallback markets must be a list")
    if set(official) | set(fallback_markets) != set(supported):
        raise ContractError("every supported market needs an exact or fallback cost")
    official_values = [_money(value, name=f"{market} round turn") for market, value in official.items()]
    fallback = _money(costs.get("fallback_round_turn_usd"), name="fallback round turn")
    if not official_values or fallback != max(official_values):
        raise ContractError("fallback must equal the most expensive official round turn")
    if costs.get("unsupported_markets_never_receive_fallback") is not True:
        raise ContractError("unsupported markets must never receive fallback costs")


def _validate_generic_stage_profile(*, profile: Mapping[str, object]) -> None:
    if profile.get("rules_as_of") is None:
        raise ContractError("stage-aware profile requires rules_as_of")
    allowed = profile.get("allowed_account_stages")
    if allowed != ["evaluation", "sim_funded", "live"]:
        raise ContractError("stage-aware profile must declare the three supported stages")
    stages = _mapping(profile.get("stages"), name="stages")
    if set(stages) != {"evaluation", "sim_funded", "live"}:
        raise ContractError("stage-aware profile is missing a required stage")
    active = selected_stage(profile)
    if active != "sim_funded":
        raise ContractError("primary funded-strategy profile must select sim_funded")

    for stage_name in ("evaluation", "sim_funded", "live"):
        rules = stage_rules(profile, stage=stage_name)
        nominal = _money(rules.get("nominal_plan_size_usd"), name=f"{stage_name} nominal size")
        starting = _money(rules.get("ledger_starting_balance_usd"), name=f"{stage_name} ledger")
        drawdown = _money(rules.get("maximum_eod_loss_usd"), name=f"{stage_name} maximum loss")
        maximum_micros = rules.get("maximum_micros")
        if nominal <= 0 or drawdown <= 0 or not isinstance(maximum_micros, int) or maximum_micros <= 0:
            raise ContractError(f"{stage_name} generic account limits are invalid")
        if rules.get("micro_units_per_mini") != 10:
            raise ContractError("firm contract conversion must remain ten micros per mini")
        if stage_name != "evaluation":
            initial = _money(rules.get("initial_loss_floor_usd"), name=f"{stage_name} initial floor")
            lock = _money(rules.get("loss_floor_lock_usd"), name=f"{stage_name} floor lock")
            if initial != starting - drawdown or lock < initial:
                raise ContractError(f"{stage_name} generic drawdown limits are inconsistent")

    access = _mapping(profile.get("market_access"), name="market_access")
    universe = access.get("signal_root_universe")
    enabled = access.get("enabled_signal_roots")
    disabled = access.get("disabled_signal_roots")
    if not all(isinstance(value, list) for value in (universe, enabled, disabled)):
        raise ContractError("signal-root access sets must be lists")
    if set(universe) != set(enabled) | set(disabled) or set(enabled) & set(disabled):
        raise ContractError("signal-root access partition is inconsistent")
    sources = profile.get("official_sources")
    if not isinstance(sources, list) or not sources:
        raise ContractError("stage-aware profile requires official source records")
    for source in sources:
        record = _mapping(source, name="official source")
        for field in ("source_id", "title", "url", "accessed_on", "supports"):
            if field not in record:
                raise ContractError(f"official source is missing {field}")
    authority = _mapping(profile.get("authority"), name="authority")
    if any(value is not False for value in authority.values()):
        raise ContractError("profile selection must grant no authority")
    if profile.get("production_readiness") is not False:
        raise ContractError("unresolved stage-aware profile must not be production-ready")


def _validate_mff_stage_profile(*, profile: Mapping[str, object]) -> None:
    _validate_generic_stage_profile(profile=profile)
    if (
        profile.get("provider_id") != "my_funded_futures"
        or profile.get("plan") != "50K Rapid EOD"
        or profile.get("rules_as_of") != "2026-08-10"
    ):
        raise ContractError("MFF Rapid EOD profile discriminator does not match provider and plan")

    evaluation = stage_rules(profile, stage="evaluation")
    if _money(evaluation.get("nominal_plan_size_usd"), name="evaluation plan size") != Decimal("50000"):
        raise ContractError("evaluation nominal size drifted")
    if (
        _money(evaluation.get("ledger_starting_balance_usd"), name="evaluation ledger") != Decimal("50000")
        or _money(evaluation.get("profit_target_usd"), name="evaluation profit target") != Decimal("3000")
        or _money(evaluation.get("maximum_eod_loss_usd"), name="evaluation maximum loss") != Decimal("2000")
        or evaluation.get("evaluation_consistency_percent") != "30"
        or evaluation.get("minimum_trading_days") != 4
        or evaluation.get("maximum_minis") != 3
        or evaluation.get("maximum_micros") != 30
        or evaluation.get("t1_news_policy") != "ALLOWED"
    ):
        raise ContractError("MFF evaluation plan-specific rules drifted")

    for stage_name, expected_maximum, expected_lock in (
        ("sim_funded", 30, Decimal("100")),
        ("live", 40, Decimal("0")),
    ):
        rules = stage_rules(profile, stage=stage_name)
        starting = _money(rules.get("ledger_starting_balance_usd"), name=f"{stage_name} ledger")
        drawdown = _money(rules.get("maximum_eod_loss_usd"), name=f"{stage_name} maximum loss")
        initial = _money(rules.get("initial_loss_floor_usd"), name=f"{stage_name} initial floor")
        lock = _money(rules.get("loss_floor_lock_usd"), name=f"{stage_name} floor lock")
        if starting != 0 or drawdown != Decimal("2000") or initial != -drawdown:
            raise ContractError(f"{stage_name} zero-based drawdown limits are inconsistent")
        if lock != expected_lock or rules.get("maximum_micros") != expected_maximum:
            raise ContractError(f"{stage_name} contract or lock limit drifted")
        if rules.get("micro_units_per_mini") != 10:
            raise ContractError("firm contract conversion must remain ten micros per mini")
        if rules.get("firm_daily_loss_limit_usd") is not None:
            raise ContractError("stage-aware profile must not invent a firm daily loss limit")

    access = _mapping(profile.get("market_access"), name="market_access")
    if access.get("execution_contract_type") != "MICRO_ONLY":
        raise ContractError("active strategy must remain micro-only")
    funded = stage_rules(profile, stage="sim_funded")
    if (
        funded.get("maximum_minis") != 3
        or funded.get("t1_news_policy") != "RESTRICTED"
        or funded.get("inactivity_calendar_days") != 7
        or funded.get("funded_consistency_rule") is not None
        or funded.get("funded_scaling_rule") is not None
    ):
        raise ContractError("MFF Sim Funded plan-specific rules drifted")
    live = stage_rules(profile, stage="live")
    if (
        live.get("stage_active") is not False
        or live.get("maximum_minis") != 4
        or live.get("profit_split_trader_percent") != "90"
        or live.get("profit_split_firm_percent") != "10"
        or live.get("payout_frequency") != "DAILY"
    ):
        raise ContractError("MFF Live plan-specific rules drifted")
    compliance = _mapping(profile.get("compliance"), name="compliance")
    expected_compliance = {
        "session_timezone": "America/New_York",
        "provider_session_open_local": "18:00:00",
        "provider_session_close_local": "16:10:00",
        "news_restricted_seconds_before": 120,
        "news_restricted_seconds_after": 120,
        "same_underlying_hedging": "PROHIBITED",
        "provider_auto_liquidation_reliance": False,
    }
    if any(compliance.get(key) != value for key, value in expected_compliance.items()):
        raise ContractError("MFF compliance rules drifted")

    sources = profile.get("official_sources")
    if not isinstance(sources, list) or len(sources) < 9:
        raise ContractError("stage-aware profile requires all official source records")
    source_ids = {source.get("source_id") for source in sources if isinstance(source, dict)}
    required_sources = {
        "rapid_eod_50k",
        "news_policy",
        "permitted_times",
        "max_eod_trailing",
        "inactivity",
        "price_limit",
        "fair_play",
        "cross_instrument",
        "rapid_live",
    }
    if source_ids != required_sources:
        raise ContractError("official source set is incomplete or unexpected")


def validate_profile(*, profile_id: str, profile: Mapping[str, object]) -> None:
    """Reject incomplete or internally inconsistent provider profiles."""

    _string(profile_id, name="profile_id")
    _string(profile.get("provider_id"), name="provider_id")
    _string(profile.get("firm"), name="firm")
    _string(profile.get("program"), name="program")
    connection = _mapping(profile.get("execution_connection"), name="execution_connection")
    _string(connection.get("connection_id"), name="connection_id")
    if "stages" in profile:
        discriminator = profile.get("profile_schema_id")
        if discriminator == "generic_stage_profile/1.0.0":
            _validate_generic_stage_profile(profile=profile)
        elif discriminator == "mff_rapid_eod_50k/1.0.0":
            if profile_id != "mff_rapid_eod_50k_2026_08_10":
                raise ContractError("MFF profile discriminator requires its immutable profile ID")
            _validate_mff_stage_profile(profile=profile)
        else:
            raise ContractError("stage-aware profile schema discriminator is unsupported")
    else:
        _validate_legacy_profile(profile=profile)


def _resolve_profile(
    *, root: Path, profile: Mapping[str, object], account_stage: str | None
) -> Mapping[str, object]:
    if "stages" not in profile:
        return profile
    stage = selected_stage(profile, account_stage)
    rules = stage_rules(profile, stage=stage)
    access = _mapping(profile.get("market_access"), name="market_access")
    bindings = load_runtime_bindings(root=root, profile=profile)
    _, costs = bindings["cost"]
    initial = rules.get("initial_loss_floor_usd")
    if initial is None:
        initial = str(
            _money(rules["ledger_starting_balance_usd"], name="ledger")
            - _money(rules["maximum_eod_loss_usd"], name="maximum loss")
        )
    locked = rules.get("loss_floor_lock_usd", initial)
    resolved = dict(profile)
    resolved.update(
        {
            "account_stage": stage,
            "account": {
                "account_size_usd": rules["nominal_plan_size_usd"],
                "starting_balance_usd": rules["ledger_starting_balance_usd"],
                "maximum_strategy_contracts": rules.get("maximum_micros"),
                "full_contracts_only": False,
            },
            "external_limits": {
                "maximum_eod_drawdown_usd": rules["maximum_eod_loss_usd"],
                "initial_eod_threshold_usd": initial,
                "locked_eod_threshold_usd": locked,
                "initial_daily_loss_limit_usd": rules.get("firm_daily_loss_limit_usd"),
                "threshold_enforced_intraday": True,
            },
            "market_access": {
                **dict(access),
                "full_contract_universe": list(access["signal_root_universe"]),
                "supported_full_contracts": list(access["enabled_signal_roots"]),
                "unsupported_full_contracts": list(access["disabled_signal_roots"]),
            },
            "execution_costs": dict(costs),
        }
    )
    return resolved


def load_profile(
    *, root: Path, profile_id: str, account_stage: str | None = None
) -> tuple[str, Mapping[str, object]]:
    selected_id, profile = raw_profile(root=root, profile_id=profile_id)
    validate_profile(profile_id=selected_id, profile=profile)
    return selected_id, _resolve_profile(root=root, profile=profile, account_stage=account_stage)


def load_active_profile(
    *, root: Path, account_stage: str | None = None
) -> tuple[str, Mapping[str, object]]:
    profile_id, profile = raw_profile(root=root)
    validate_profile(profile_id=profile_id, profile=profile)
    return profile_id, _resolve_profile(root=root, profile=profile, account_stage=account_stage)


def round_turn_commission(profile: Mapping[str, object], market: str) -> Decimal:
    """Return selected official costs, failing closed when they are unresolved."""

    access = _mapping(profile.get("market_access"), name="market_access")
    costs = _mapping(profile.get("execution_costs"), name="execution_costs")
    supported = set(access.get("supported_full_contracts", ()))
    if market not in supported:
        raise ContractError(f"{market} is not a supported provider-account instrument")
    if "round_turn_commission_usd" in costs:
        official = _mapping(costs.get("round_turn_commission_usd"), name="round-turn costs")
        if market not in official:
            raise ContractError(f"{market} cost is unresolved for the selected platform")
        return _money(official[market], name=f"{market} round turn")
    official = _mapping(costs.get("official_round_turn_usd"), name="official costs")
    if market in official:
        return _money(official[market], name=f"{market} round turn")
    fallback_markets = costs.get("fallback_markets")
    if isinstance(fallback_markets, list) and market in fallback_markets:
        return _money(costs.get("fallback_round_turn_usd"), name="fallback round turn")
    raise ContractError(f"{market} has no fail-closed commission disposition")


def update_eod_threshold(
    profile: Mapping[str, object],
    *,
    previous_threshold_usd: object,
    highest_eod_balance_usd: object,
) -> Decimal:
    """Apply the selected stage's completed-session trailing floor rule."""

    limits = _mapping(profile.get("external_limits"), name="external_limits")
    drawdown = _money(limits.get("maximum_eod_drawdown_usd"), name="maximum EOD drawdown")
    initial = _money(limits.get("initial_eod_threshold_usd"), name="initial EOD threshold")
    locked = _money(limits.get("locked_eod_threshold_usd"), name="locked EOD threshold")
    previous = _money(previous_threshold_usd, name="previous EOD threshold")
    completed = _money(highest_eod_balance_usd, name="completed-session EOD balance")
    if previous < initial or previous > locked:
        raise ContractError("previous EOD threshold is outside selected-stage bounds")
    return min(locked, max(previous, completed - drawdown))


def build_owner_limits(
    profile: Mapping[str, object],
    *,
    r_usd: object,
    emergency_reserve_usd: object,
    owner_attestation: str,
) -> dict[str, str]:
    if owner_attestation != OWNER_ATTESTATION:
        raise ContractError("exact owner account-tolerance attestation is required")
    limits = _mapping(profile.get("external_limits"), name="external_limits")
    risk = _money(r_usd, name="R")
    reserve = _money(emergency_reserve_usd, name="emergency reserve")
    drawdown = _money(limits.get("maximum_eod_drawdown_usd"), name="maximum EOD drawdown")
    daily_raw = limits.get("initial_daily_loss_limit_usd")
    if risk <= 0 or reserve <= 0 or reserve >= drawdown or risk > drawdown - reserve:
        raise ContractError("owner limits exceed usable firm drawdown")
    return {
        "r_usd": format(risk, "f"),
        "emergency_reserve_usd": format(reserve, "f"),
        "usable_initial_drawdown_usd": format(drawdown - reserve, "f"),
        "firm_initial_daily_loss_limit_usd": "NONE" if daily_raw is None else format(_money(daily_raw, name="daily loss limit"), "f"),
        "automatic_2r_daily_limit": "false",
        "automatic_6r_drawdown_limit": "false",
    }


def admission_cap_usd(
    profile: Mapping[str, object],
    *,
    r_usd: object,
    emergency_reserve_usd: object,
    current_equity_usd: object,
    current_firm_threshold_usd: object,
    realized_daily_loss_usd: object,
) -> Decimal:
    limits = _mapping(profile.get("external_limits"), name="external_limits")
    risk = _money(r_usd, name="R")
    reserve = _money(emergency_reserve_usd, name="emergency reserve")
    equity = _money(current_equity_usd, name="current equity")
    threshold = _money(current_firm_threshold_usd, name="current firm threshold")
    daily_loss = _money(realized_daily_loss_usd, name="realized daily loss")
    if risk <= 0 or reserve <= 0 or daily_loss < 0:
        raise ContractError("R and reserve must be positive; daily loss cannot be negative")
    capacities = [risk, equity - threshold - reserve]
    if limits.get("initial_daily_loss_limit_usd") is not None:
        capacities.append(_money(limits["initial_daily_loss_limit_usd"], name="daily loss limit") - daily_loss)
    return max(Decimal("0"), min(capacities))


def planned_loss_usd(
    profile: Mapping[str, object],
    *, stop_risk_usd: object, market: str, stress_slippage_usd: object
) -> Decimal:
    stop = _money(stop_risk_usd, name="stop risk")
    slippage = _money(stress_slippage_usd, name="stress slippage")
    if stop < 0 or slippage < 0:
        raise ContractError("stop risk and stress slippage cannot be negative")
    return stop + round_turn_commission(profile, market) + slippage


def build_draft_policy(
    *,
    profile_id: str,
    profile: Mapping[str, object],
    runtime_identity: Mapping[str, object] | None = None,
) -> dict[str, object]:
    account = _mapping(profile.get("account"), name="account")
    connection = _mapping(profile.get("execution_connection"), name="execution_connection")
    limits = _mapping(profile.get("external_limits"), name="external_limits")
    costs = _mapping(profile.get("execution_costs"), name="execution_costs")
    exact = costs.get("exact_provider_account_costs_verified", costs.get("exact_live_costs_verified", False))
    blockers = list(profile.get("readiness_blockers", ()))
    if not exact and "OFFICIAL_PROVIDER_ACCOUNT_COSTS_UNRESOLVED" not in blockers:
        blockers.append("OFFICIAL_PROVIDER_ACCOUNT_COSTS_UNRESOLVED")
    core: dict[str, object] = {
        "schema_version": "prop_firm_eod_risk_policy/2.0.0",
        "state": "PREPARED_PRODUCTION_BLOCKED" if blockers else "PREPARED_NOT_AUTHORIZED",
        "profile_id": profile_id,
        "profile_hash": (runtime_identity or {}).get("profile_hash", sha256_json(profile)),
        "rules_as_of": profile.get("rules_as_of", profile.get("reviewed_on")),
        "account_stage": profile.get("account_stage", "legacy_funded"),
        "account": {
            "provider_id": profile["provider_id"],
            "firm": profile["firm"],
            "program": profile["program"],
            "execution_connection_id": connection["connection_id"],
            **dict(account),
        },
        "external_hard_limits": dict(limits),
        "internal_strategy_policy": {
            "source": "SEPARATE_HASH_BOUND_CONFIG",
            "strategy_policy_id": (runtime_identity or {}).get("strategy_policy_id"),
            "strategy_policy_hash": (runtime_identity or {}).get("strategy_policy_hash"),
            "production_readiness": False,
        },
        "cost_policy": {
            "schedule_status": costs.get("schedule_status"),
            "exact_provider_account_costs_verified": exact,
            "execution_cost_profile_id": (runtime_identity or {}).get("execution_cost_profile_id"),
            "execution_cost_profile_hash": (runtime_identity or {}).get("execution_cost_profile_hash"),
        },
        "runtime_identity": dict(runtime_identity or {}),
        "production_readiness": False,
        "readiness_blockers": blockers,
        "sources": profile.get("official_sources"),
        "authority": {
            "provider_access": False,
            "historical_rows": False,
            "evaluation": False,
            "activation": False,
            "publication": False,
            "deployment": False,
            "payout_submission": False,
            "trading": False,
        },
    }
    return {**core, "policy_id": sha256_json(core)}


def build_active_draft_policy(*, root: Path) -> dict[str, object]:
    profile_id, profile = load_active_profile(root=root)
    identity = build_runtime_identity(root=root, profile_id=profile_id, account_stage=str(profile["account_stage"]))
    return build_draft_policy(profile_id=profile_id, profile=profile, runtime_identity=identity)


def validate_draft_policy(
    policy: Mapping[str, object],
    *,
    profile_id: str,
    profile: Mapping[str, object],
    runtime_identity: Mapping[str, object] | None = None,
) -> None:
    if dict(policy) != build_draft_policy(
        profile_id=profile_id, profile=profile, runtime_identity=runtime_identity
    ):
        raise ContractError("prop-firm EOD draft policy drifted")


__all__ = [
    "OWNER_ATTESTATION",
    "PROFILE_RELATIVE_PATH",
    "admission_cap_usd",
    "build_active_draft_policy",
    "build_draft_policy",
    "build_owner_limits",
    "load_active_profile",
    "load_profile",
    "planned_loss_usd",
    "round_turn_commission",
    "update_eod_threshold",
    "validate_draft_policy",
    "validate_profile",
]
