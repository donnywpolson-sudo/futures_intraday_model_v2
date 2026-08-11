"""Stage-aware prop-firm account, execution, payout, and compliance mechanics.

All functions are local deterministic simulations.  They do not connect to a
provider, submit an order or payout, read market rows, or grant authority.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from .canonical import sha256_json
from .errors import ContractError


PROFILE_PATH = Path("configs/prop_firm_profiles.json")
STRATEGY_POLICY_PATH = Path("configs/prop_firm_strategy_risk_policies.json")
INSTRUMENT_MAPPING_PATH = Path("configs/prop_firm_execution_instruments.json")
EXECUTION_COST_PATH = Path("configs/prop_firm_execution_costs.json")
PAYOUT_POLICY_PATH = Path("configs/prop_firm_payout_policies.json")


def money(value: object, *, name: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ContractError(f"{name} must be an exact decimal") from exc
    if not result.is_finite():
        raise ContractError(f"{name} must be finite")
    return result


def mapping(value: object, *, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ContractError(f"{name} must be an object")
    return value


def nonempty_string(value: object, *, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ContractError(f"{name} must be a nonempty string")
    return value


def strict_bool(value: object, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise ContractError(f"{name} must be boolean")
    return value


def sha256_text(value: object, *, name: str) -> str:
    text = nonempty_string(value, name=name)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ContractError(f"{name} must be a lowercase SHA-256")
    return text


def _load_json(root: Path, relative: Path, *, name: str) -> Mapping[str, object]:
    try:
        value = json.loads((root / relative).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ContractError(f"cannot read {name}: {root / relative}") from exc
    return mapping(value, name=name)


def load_profile_document(*, root: Path) -> Mapping[str, object]:
    document = _load_json(root, PROFILE_PATH, name="prop-firm profile document")
    if document.get("schema_version") not in {
        "prop_firm_profiles/1.0.0",
        "prop_firm_profiles/2.0.0",
    }:
        raise ContractError("prop-firm profile schema is unsupported")
    return document


def raw_profile(
    *, root: Path, profile_id: str | None = None
) -> tuple[str, Mapping[str, object]]:
    document = load_profile_document(root=root)
    selected_id = profile_id or nonempty_string(
        document.get("active_profile_id"), name="active_profile_id"
    )
    profiles = mapping(document.get("profiles"), name="profiles")
    return selected_id, mapping(profiles.get(selected_id), name="selected profile")


def selected_stage(profile: Mapping[str, object], stage: str | None = None) -> str:
    stages = profile.get("stages")
    if stages is None:
        if stage not in {None, "legacy_funded"}:
            raise ContractError("legacy profile supports only legacy_funded stage")
        return "legacy_funded"
    stage_map = mapping(stages, name="stages")
    chosen = stage or nonempty_string(
        profile.get("active_account_stage"), name="active_account_stage"
    )
    if chosen not in {"evaluation", "sim_funded", "live"}:
        raise ContractError(f"unsupported account stage: {chosen}")
    if chosen not in stage_map:
        raise ContractError(f"account stage is missing: {chosen}")
    return chosen


def stage_rules(
    profile: Mapping[str, object], *, stage: str | None = None
) -> Mapping[str, object]:
    chosen = selected_stage(profile, stage)
    if chosen == "legacy_funded":
        account = mapping(profile.get("account"), name="legacy account")
        limits = mapping(profile.get("external_limits"), name="legacy limits")
        return {
            "stage_active": True,
            "nominal_plan_size_usd": account["account_size_usd"],
            "ledger_starting_balance_usd": account["starting_balance_usd"],
            "maximum_eod_loss_usd": limits["maximum_eod_drawdown_usd"],
            "initial_loss_floor_usd": limits["initial_eod_threshold_usd"],
            "loss_floor_lock_usd": limits["locked_eod_threshold_usd"],
            "firm_daily_loss_limit_usd": limits["initial_daily_loss_limit_usd"],
            "drawdown_state_machine": "LEGACY_PROFILE",
        }
    return mapping(mapping(profile.get("stages"), name="stages")[chosen], name=chosen)


def _selected_config(
    *,
    root: Path,
    relative: Path,
    schema: str,
    active_key: str,
    collection_key: str,
    expected_id: str,
) -> tuple[str, Mapping[str, object]]:
    document = _load_json(root, relative, name=relative.as_posix())
    if document.get("schema_version") != schema:
        raise ContractError(f"unsupported schema for {relative.as_posix()}")
    selected_id = nonempty_string(document.get(active_key), name=active_key)
    if selected_id != expected_id:
        raise ContractError(
            f"{active_key} does not match the selected profile binding: "
            f"{selected_id} != {expected_id}"
        )
    collection = mapping(document.get(collection_key), name=collection_key)
    return selected_id, mapping(collection.get(selected_id), name=selected_id)


def _named_config(
    *, root: Path, relative: Path, schema: str, collection_key: str, selected_id: str
) -> tuple[str, Mapping[str, object]]:
    document = _load_json(root, relative, name=relative.as_posix())
    if document.get("schema_version") != schema:
        raise ContractError(f"unsupported schema for {relative.as_posix()}")
    collection = mapping(document.get(collection_key), name=collection_key)
    return selected_id, mapping(collection.get(selected_id), name=selected_id)


def load_runtime_bindings(
    *, root: Path, profile: Mapping[str, object]
) -> dict[str, tuple[str, Mapping[str, object]]]:
    ids = mapping(profile.get("binding_ids"), name="binding_ids")
    bindings = {
        "strategy": _selected_config(
            root=root,
            relative=STRATEGY_POLICY_PATH,
            schema="prop_firm_strategy_risk_policies/1.0.0",
            active_key="active_strategy_policy_id",
            collection_key="policies",
            expected_id=nonempty_string(ids.get("strategy_policy_id"), name="strategy_policy_id"),
        ),
        "mapping": _selected_config(
            root=root,
            relative=INSTRUMENT_MAPPING_PATH,
            schema="prop_firm_execution_instruments/1.0.0",
            active_key="active_mapping_id",
            collection_key="mappings",
            expected_id=nonempty_string(ids.get("execution_instrument_mapping_id"), name="execution_instrument_mapping_id"),
        ),
        "cost": _selected_config(
            root=root,
            relative=EXECUTION_COST_PATH,
            schema="prop_firm_execution_costs/1.0.0",
            active_key="active_cost_profile_id",
            collection_key="cost_profiles",
            expected_id=nonempty_string(ids.get("execution_cost_profile_id"), name="execution_cost_profile_id"),
        ),
        "payout": _selected_config(
            root=root,
            relative=PAYOUT_POLICY_PATH,
            schema="prop_firm_payout_policies/1.0.0",
            active_key="active_payout_policy_id",
            collection_key="payout_policies",
            expected_id=nonempty_string(ids.get("payout_policy_id"), name="payout_policy_id"),
        ),
    }
    validate_strategy_policy(bindings["strategy"][1])
    validate_instrument_mapping(bindings["mapping"][1])
    validate_execution_cost_profile(bindings["cost"][1])
    validate_payout_policy(bindings["payout"][1])
    return bindings


def _decimal_sequence(value: object, *, name: str) -> tuple[Decimal, ...]:
    if not isinstance(value, list) or not value:
        raise ContractError(f"{name} must be a nonempty list")
    result = tuple(money(item, name=name) for item in value)
    if any(item <= 0 for item in result) or tuple(sorted(set(result))) != result:
        raise ContractError(f"{name} must be unique, positive, and increasing")
    return result


def _validate_generic_strategy_policy(policy: Mapping[str, object]) -> None:
    if policy.get("account_stage") != "sim_funded":
        raise ContractError("strategy policy must explicitly target sim_funded")
    if policy.get("execution_contract_type") != "MICRO_ONLY":
        raise ContractError("strategy policy must remain micro-only")
    if policy.get("production_readiness") is not False:
        raise ContractError("unpromoted strategy policy cannot be production-ready")
    design = mapping(policy.get("candidate_design"), name="candidate design")
    risk = _decimal_sequence(design.get("planned_risk_per_trade_usd"), name="planned risk")
    concurrent = _decimal_sequence(
        design.get("maximum_concurrent_open_risk_usd"), name="concurrent risk"
    )
    session = _decimal_sequence(design.get("internal_session_stop_usd"), name="session stop")
    reserve = _decimal_sequence(
        design.get("minimum_reserved_cushion_usd"), name="reserved cushion"
    )
    entries = design.get("maximum_entries_per_session")
    if not isinstance(entries, list) or not entries or any(not isinstance(value, int) or value <= 0 for value in entries):
        raise ContractError("entry-count candidates must be positive integers")
    if not isinstance(design.get("pyramiding"), list) or not design["pyramiding"]:
        raise ContractError("pyramiding candidates are missing")
    if max(risk) > max(concurrent) or max(concurrent) > max(session):
        raise ContractError("strategy candidate bounds violate risk hierarchy")
    limits = mapping(policy.get("runtime_limits"), name="runtime limits")
    if not isinstance(limits.get("firm_micro_equivalent_cap"), int) or limits["firm_micro_equivalent_cap"] <= 0:
        raise ContractError("runtime firm cap is invalid")
    for name in ("per_execution_symbol_micros", "liquidity_micro_caps"):
        caps = mapping(limits.get(name), name=name)
        if not caps or any(not isinstance(value, int) or value <= 0 for value in caps.values()):
            raise ContractError(f"{name} must contain positive integer caps")
    if not isinstance(limits.get("per_underlying_risk_group_micros"), int):
        raise ContractError("underlying concentration cap is invalid")
    if not isinstance(limits.get("platform_micro_cap"), int):
        raise ContractError("platform cap is invalid")
    constraints = design.get("constraints")
    if not isinstance(constraints, list) or not constraints:
        raise ContractError("logical strategy constraints are missing")


def _validate_mff_strategy_policy(policy: Mapping[str, object]) -> None:
    _validate_generic_strategy_policy(policy)
    design = mapping(policy.get("candidate_design"), name="candidate design")
    if design.get("method") != "SEQUENTIAL_COARSE_TO_FINE_WITH_FROZEN_FINAL_TEST":
        raise ContractError("strategy search must remain sequential and bounded")
    if design.get("maximum_entries_per_session") != [1, 2, 3, 4, 5]:
        raise ContractError("entry-count candidates drifted")
    if design.get("pyramiding") != [False, True]:
        raise ContractError("pyramiding candidates drifted")
    expected_sequences = {
        "planned_risk_per_trade_usd": ["50", "75", "100", "125", "150", "200"],
        "maximum_concurrent_open_risk_usd": ["100", "150", "200", "300", "400"],
        "internal_session_stop_usd": ["200", "300", "400", "500", "600"],
        "minimum_reserved_cushion_usd": ["400", "500", "600", "800"],
    }
    if any(design.get(name) != expected for name, expected in expected_sequences.items()):
        raise ContractError("MFF research candidate design drifted")
    reserve = _decimal_sequence(design.get("minimum_reserved_cushion_usd"), name="reserved cushion")
    if min(reserve) < Decimal("400"):
        raise ContractError("strategy reserve candidates are too small")
    limits = mapping(policy.get("runtime_limits"), name="runtime limits")
    if limits.get("firm_micro_equivalent_cap") != 30:
        raise ContractError("runtime firm cap drifted")
    if (
        limits.get("per_execution_symbol_micros") != {"MES": 10, "MCL": 10, "M6E": 10}
        or limits.get("per_underlying_risk_group_micros") != 15
        or limits.get("platform_micro_cap") != 30
        or limits.get("liquidity_micro_caps") != {"MES": 10, "MCL": 10, "M6E": 10}
    ):
        raise ContractError("MFF internal runtime limits drifted")
    constraints = design.get("constraints")
    if {
        "PLANNED_RISK_LE_CONCURRENT_RISK",
        "CONCURRENT_RISK_LE_SESSION_STOP",
        "ORDER_RISK_LE_FLOOR_CUSHION_AFTER_RESERVE",
        "FINAL_100_USD_FLOOR_NOT_NORMAL_OPERATING_CAPITAL",
        "REALISTIC_MICRO_COSTS_REQUIRED",
        "NO_FINAL_TEST_RETUNING",
    } != set(constraints):
        raise ContractError("logical strategy constraints are incomplete")


def validate_strategy_policy(policy: Mapping[str, object]) -> None:
    schema_id = policy.get("policy_schema_id")
    if schema_id == "generic_micro_risk_policy/1.0.0":
        _validate_generic_strategy_policy(policy)
    elif schema_id == "mff_micro_risk_policy/1.0.0":
        _validate_mff_strategy_policy(policy)
    else:
        raise ContractError("strategy policy schema discriminator is unsupported")


def build_coarse_strategy_candidates(policy: Mapping[str, object]) -> tuple[dict[str, object], ...]:
    """Return six ordered candidates, never the full Cartesian product."""

    validate_strategy_policy(policy)
    design = mapping(policy["candidate_design"], name="candidate design")
    risk = list(design["planned_risk_per_trade_usd"])
    concurrent = ["100", "150", "200", "300", "400", "400"]
    session = ["200", "300", "400", "500", "600", "600"]
    reserve = ["800", "800", "600", "600", "500", "400"]
    entries = [1, 2, 3, 4, 5, 5]
    candidates = tuple(
        {
            "candidate_id": f"coarse-{index + 1}",
            "planned_risk_per_trade_usd": risk[index],
            "maximum_concurrent_open_risk_usd": concurrent[index],
            "internal_session_stop_usd": session[index],
            "minimum_reserved_cushion_usd": reserve[index],
            "maximum_entries_per_session": entries[index],
            "pyramiding": False,
            "next_stage": "LOCAL_FINE_NEIGHBORS_SELECTED_FROM_TRAINING_FOLDS_ONLY",
        }
        for index in range(6)
    )
    for candidate in candidates:
        planned = money(candidate["planned_risk_per_trade_usd"], name="candidate risk")
        open_risk = money(candidate["maximum_concurrent_open_risk_usd"], name="candidate open risk")
        stop = money(candidate["internal_session_stop_usd"], name="candidate session stop")
        if not planned <= open_risk <= stop:
            raise ContractError("coarse candidate violates risk hierarchy")
    return candidates


def _validate_generic_instrument_mapping(instrument_mapping: Mapping[str, object]) -> None:
    if instrument_mapping.get("execution_contract_type") != "MICRO_ONLY":
        raise ContractError("execution mapping must remain micro-only")
    instruments = mapping(instrument_mapping.get("instruments"), name="instruments")
    if not instruments:
        raise ContractError("execution mapping cannot be empty")
    for root, raw in instruments.items():
        instrument = mapping(raw, name=f"{root} mapping")
        if instrument.get("enabled") is False:
            if instrument.get("execution_symbol") is not None:
                raise ContractError("disabled mapping cannot name an execution symbol")
            continue
        resolve_execution_instrument(instrument_mapping, str(root))
        source = mapping(instrument.get("source"), name=f"{root} source")
        for field in ("title", "url", "accessed_on"):
            nonempty_string(source.get(field), name=f"{root} source {field}")


def _validate_mff_instrument_mapping(instrument_mapping: Mapping[str, object]) -> None:
    _validate_generic_instrument_mapping(instrument_mapping)
    instruments = mapping(instrument_mapping.get("instruments"), name="instruments")
    if set(instruments) != {"ES", "CL", "ZN", "6E"}:
        raise ContractError("execution mapping signal-root universe drifted")
    for root in ("ES", "CL", "6E"):
        instrument = resolve_execution_instrument(instrument_mapping, root)
        source = mapping(instrument.get("source"), name=f"{root} source")
        for field in ("title", "url", "accessed_on"):
            nonempty_string(source.get(field), name=f"{root} source {field}")
        if not str(source["url"]).startswith("https://www.cmegroup.com/"):
            raise ContractError("execution mapping source must be an official CME URL")
    disabled = mapping(instruments["ZN"], name="ZN mapping")
    if disabled.get("enabled") is not False or disabled.get("execution_symbol") is not None:
        raise ContractError("unsupported micro mapping must remain explicitly disabled")
    expected = {
        "ES": ("MES", "SP500_EQUITY_INDEX", "5", "0.25", "1.25", "CME_EQUITY_INDEX_US"),
        "CL": ("MCL", "WTI_CRUDE_OIL", "100", "0.01", "1.00", "NYMEX_ENERGY_US"),
        "6E": ("M6E", "EUR_USD_FX", "12500", "0.0001", "1.25", "CME_FX_US"),
    }
    for root, values in expected.items():
        instrument = mapping(instruments[root], name=f"{root} mapping")
        observed = tuple(
            instrument.get(field)
            for field in (
                "execution_symbol", "underlying_risk_group", "contract_multiplier",
                "tick_size", "tick_value_usd", "session_calendar",
            )
        )
        if observed != values:
            raise ContractError(f"MFF {root} execution mapping drifted")


def validate_instrument_mapping(instrument_mapping: Mapping[str, object]) -> None:
    schema_id = instrument_mapping.get("mapping_schema_id")
    if schema_id == "generic_micro_execution_mapping/1.0.0":
        _validate_generic_instrument_mapping(instrument_mapping)
    elif schema_id == "mff_micro_execution_mapping/1.0.0":
        _validate_mff_instrument_mapping(instrument_mapping)
    else:
        raise ContractError("execution mapping schema discriminator is unsupported")


def validate_execution_cost_profile(cost: Mapping[str, object]) -> None:
    if cost.get("cost_profile_schema_id") not in {
        "generic_unresolved_execution_costs/1.0.0",
        "generic_provisional_research_costs/1.0.0",
        "generic_verified_execution_costs/1.0.0",
    }:
        raise ContractError("execution-cost schema discriminator is unsupported")
    exact = cost.get("exact_provider_account_costs_verified")
    fees = mapping(cost.get("round_turn_commission_usd"), name="round-turn costs")
    if not isinstance(exact, bool):
        raise ContractError("cost verification flag must be boolean")
    if exact and not fees:
        raise ContractError("verified costs cannot be empty")
    if not exact and cost.get("production_readiness") is not False:
        raise ContractError("unverified costs must block production readiness")
    if cost.get("platform_connection_id") == "UNSET" and fees:
        raise ContractError("an unset platform cannot publish official fees")
    if cost.get("provisional") is True:
        slippage = mapping(cost.get("expected_slippage_usd"), name="provisional slippage")
        if not fees or set(fees) != set(slippage):
            raise ContractError("provisional costs require matching micro fee and slippage maps")
        if any(money(value, name="provisional fee") <= 0 for value in fees.values()):
            raise ContractError("provisional fees must be nonzero")
        if any(money(value, name="provisional slippage") <= 0 for value in slippage.values()):
            raise ContractError("provisional slippage must be nonzero")


def _validate_generic_payout_policy(policy: Mapping[str, object]) -> None:
    if policy.get("account_stage") != "sim_funded":
        raise ContractError("payout policy must explicitly target sim_funded")
    if money(policy.get("trader_share"), name="trader share") + money(
        policy.get("firm_share"), name="firm share"
    ) != Decimal("1"):
        raise ContractError("payout shares must sum to one")
    if money(policy.get("first_buffer_usd"), name="first buffer") <= 0:
        raise ContractError("first payout buffer must be positive")
    if money(policy.get("minimum_request_usd"), name="minimum request") <= 0:
        raise ContractError("minimum payout request must be positive")
    if policy.get("provider_submission_supported") is not False:
        raise ContractError("provider payout submission must remain disabled")


def _validate_mff_payout_policy(policy: Mapping[str, object]) -> None:
    _validate_generic_payout_policy(policy)
    if money(policy.get("first_buffer_usd"), name="first buffer") != Decimal("2100"):
        raise ContractError("first payout buffer drifted")
    if money(policy.get("minimum_request_usd"), name="minimum request") != Decimal("500"):
        raise ContractError("minimum payout request drifted")
    if (
        money(policy.get("subsequent_net_profit_required_usd"), name="subsequent payout profit")
        != Decimal("500")
        or policy.get("frequency") != "DAILY"
        or money(policy.get("trader_share"), name="trader share") != Decimal("0.90")
        or money(policy.get("firm_share"), name="firm share") != Decimal("0.10")
        or policy.get("maximum_withdrawable_rule")
        != "UNRESOLVED_MANUAL_CONFIRMATION_REQUIRED"
    ):
        raise ContractError("MFF payout policy drifted")


def validate_payout_policy(policy: Mapping[str, object]) -> None:
    schema_id = policy.get("policy_schema_id")
    if schema_id == "generic_payout_policy/1.0.0":
        _validate_generic_payout_policy(policy)
    elif schema_id == "mff_rapid_eod_payout/1.0.0":
        _validate_mff_payout_policy(policy)
    else:
        raise ContractError("payout policy schema discriminator is unsupported")


def build_runtime_identity(
    *, root: Path, profile_id: str | None = None, account_stage: str | None = None,
    research_cost_profile_id: str | None = None,
) -> dict[str, object]:
    selected_id, profile = raw_profile(root=root, profile_id=profile_id)
    stage = selected_stage(profile, account_stage)
    if stage == "legacy_funded":
        core = {
            "provider_id": profile["provider_id"],
            "plan": profile.get("program"),
            "profile_id": selected_id,
            "profile_hash": sha256_json(profile),
            "rules_as_of": profile.get("reviewed_on"),
            "account_stage": stage,
            "strategy_policy_id": "LEGACY_EMBEDDED",
            "strategy_policy_hash": sha256_json(profile.get("project_limits", {})),
            "execution_instrument_mapping_id": "LEGACY_EMBEDDED",
            "execution_instrument_mapping_hash": sha256_json(profile.get("market_access", {})),
            "execution_cost_profile_id": "LEGACY_EMBEDDED",
            "execution_cost_profile_hash": sha256_json(profile.get("execution_costs", {})),
            "payout_policy_id": "LEGACY_UNMODELED",
            "payout_policy_hash": sha256_json({"state": "LEGACY_UNMODELED"}),
        }
    else:
        bindings = load_runtime_bindings(root=root, profile=profile)
        strategy_id, strategy = bindings["strategy"]
        mapping_id, instrument_mapping = bindings["mapping"]
        cost_id, cost = bindings["cost"]
        if research_cost_profile_id is not None:
            allowed = mapping(profile.get("binding_ids"), name="binding_ids").get(
                "provisional_research_cost_profile_ids"
            )
            if not isinstance(allowed, list) or research_cost_profile_id not in allowed:
                raise ContractError("research cost profile is not bound by the selected profile")
            cost_id, cost = _named_config(
                root=root,
                relative=EXECUTION_COST_PATH,
                schema="prop_firm_execution_costs/1.0.0",
                collection_key="cost_profiles",
                selected_id=research_cost_profile_id,
            )
            validate_execution_cost_profile(cost)
        payout_id, payout = bindings["payout"]
        if strategy.get("account_stage") != stage:
            raise ContractError("strategy policy account stage does not match runtime stage")
        if payout.get("account_stage") != stage:
            raise ContractError("payout policy account stage does not match runtime stage")
        core = {
            "provider_id": profile["provider_id"],
            "plan": profile["plan"],
            "profile_id": selected_id,
            "profile_hash": sha256_json(profile),
            "rules_as_of": profile["rules_as_of"],
            "account_stage": stage,
            "strategy_policy_id": strategy_id,
            "strategy_policy_hash": sha256_json(strategy),
            "execution_instrument_mapping_id": mapping_id,
            "execution_instrument_mapping_hash": sha256_json(instrument_mapping),
            "execution_cost_profile_id": cost_id,
            "execution_cost_profile_hash": sha256_json(cost),
            "payout_policy_id": payout_id,
            "payout_policy_hash": sha256_json(payout),
            "economics_classification": (
                "PROVISIONAL_RESEARCH_STRESS_NOT_PROVIDER_VERIFIED"
                if cost.get("provisional") is True
                else "SELECTED_ACCOUNT_COST_PROFILE"
            ),
            "production_readiness": bool(
                profile.get("production_readiness") is True
                and strategy.get("production_readiness") is True
                and cost.get("production_readiness") is True
                and instrument_mapping.get("live_readiness") is True
                and payout.get("production_readiness") is True
            ),
        }
    return {**core, "cache_identity": sha256_json(core)}


def assert_cache_identity(
    *, expected: Mapping[str, object], observed: Mapping[str, object]
) -> None:
    required = {
        "profile_id",
        "profile_hash",
        "account_stage",
        "strategy_policy_id",
        "strategy_policy_hash",
        "execution_instrument_mapping_id",
        "execution_instrument_mapping_hash",
        "execution_cost_profile_id",
        "execution_cost_profile_hash",
        "payout_policy_id",
        "payout_policy_hash",
        "cache_identity",
    }
    if not required.issubset(expected) or not required.issubset(observed):
        raise ContractError("cache identity is incomplete")
    if any(expected[key] != observed[key] for key in required):
        raise ContractError("cached result identity does not match the selected runtime")


@dataclass(frozen=True)
class EodDrawdownState:
    profile_id: str
    profile_hash: str
    account_stage: str
    active_floor_usd: Decimal
    floor_lock_usd: Decimal
    maximum_loss_usd: Decimal
    highest_completed_eod_balance_usd: Decimal
    realized_account_balance_usd: Decimal
    floor_locked: bool
    last_completed_session_id: str | None = None
    last_completed_session_hash: str | None = None
    last_calendar_provider_id: str | None = None
    last_calendar_sha256: str | None = None
    last_completed_session_close_at: datetime | None = None
    last_completed_session_balance_usd: Decimal | None = None
    processed_session_hashes: tuple[str, ...] = ()
    processed_session_count: int = 0
    breached: bool = False


@dataclass(frozen=True)
class VerifiedSessionRecord:
    profile_id: str
    profile_hash: str
    account_stage: str
    calendar_provider_id: str
    calendar_version: str
    calendar_sha256: str
    session_kind: str
    session_open_at: datetime
    provider_close_at: datetime
    source_as_of: datetime
    fresh_until: datetime
    session_id: str
    record_hash: str


@dataclass(frozen=True)
class CompletedSessionEvent:
    session: VerifiedSessionRecord
    completed_session_eod_balance_usd: Decimal
    observed_at: datetime
    event_hash: str


def _runtime_binding(identity: Mapping[str, object], *, profile: Mapping[str, object], stage: str) -> None:
    expected = {
        "profile_id": identity.get("profile_id"),
        "profile_hash": sha256_json(profile),
        "account_stage": stage,
    }
    if any(identity.get(key) != value for key, value in expected.items()):
        raise ContractError("runtime identity does not bind the selected profile and stage")
    cache_identity = identity.get("cache_identity")
    if not isinstance(cache_identity, str) or len(cache_identity) != 64:
        raise ContractError("runtime cache identity is missing")


def build_verified_session_record(
    *, profile: Mapping[str, object], runtime_identity: Mapping[str, object],
    account_stage: str, calendar_provider_id: str, calendar_version: str,
    calendar_sha256: str, session_open_at: datetime, provider_close_at: datetime,
    source_as_of: datetime, fresh_until: datetime, session_kind: str = "ORDINARY",
) -> VerifiedSessionRecord:
    stage = selected_stage(profile, account_stage)
    _runtime_binding(runtime_identity, profile=profile, stage=stage)
    opened = _aware(session_open_at, name="session open")
    closed = _aware(provider_close_at, name="provider close")
    source_at = _aware(source_as_of, name="calendar source as-of")
    fresh = _aware(fresh_until, name="calendar fresh-until")
    compliance = mapping(profile.get("compliance"), name="compliance")
    timezone_name = nonempty_string(compliance.get("session_timezone"), name="session timezone")
    if timezone_name != "America/New_York":
        raise ContractError("provider session timezone is unsupported")
    ny = ZoneInfo(timezone_name)
    if getattr(opened.tzinfo, "key", None) != ny.key or getattr(closed.tzinfo, "key", None) != ny.key:
        raise ContractError("provider session boundaries must use America/New_York")
    if closed <= opened or source_at > fresh:
        raise ContractError("verified session chronology is invalid")
    kind = nonempty_string(session_kind, name="session kind")
    expected_open = time.fromisoformat(nonempty_string(compliance.get("provider_session_open_local"), name="session open rule"))
    expected_close = time.fromisoformat(nonempty_string(compliance.get("provider_session_close_local"), name="session close rule"))
    if kind == "ORDINARY":
        if opened.timetz().replace(tzinfo=None) != expected_open or closed.timetz().replace(tzinfo=None) != expected_close:
            raise ContractError("ordinary session boundaries do not match the selected profile")
        if opened.date() + timedelta(days=1) != closed.date():
            raise ContractError("ordinary provider session must close on the following date")
    elif kind != "VERIFIED_SHORTENED":
        raise ContractError("unknown or unverified shortened session fails closed")
    core = {
        "profile_id": runtime_identity["profile_id"],
        "profile_hash": runtime_identity["profile_hash"],
        "account_stage": stage,
        "calendar_provider_id": nonempty_string(calendar_provider_id, name="calendar provider ID"),
        "calendar_version": nonempty_string(calendar_version, name="calendar version"),
        "calendar_sha256": nonempty_string(calendar_sha256, name="calendar SHA-256"),
        "session_kind": kind,
        "session_open_at": opened.isoformat(),
        "provider_close_at": closed.isoformat(),
        "source_as_of": source_at.isoformat(),
        "fresh_until": fresh.isoformat(),
    }
    sha256_text(core["calendar_sha256"], name="calendar SHA-256")
    session_id = sha256_json(core)
    record_core = {**core, "session_id": session_id}
    return VerifiedSessionRecord(
        profile_id=str(core["profile_id"]), profile_hash=str(core["profile_hash"]),
        account_stage=stage, calendar_provider_id=str(core["calendar_provider_id"]),
        calendar_version=str(core["calendar_version"]), calendar_sha256=str(core["calendar_sha256"]),
        session_kind=kind, session_open_at=opened, provider_close_at=closed,
        source_as_of=source_at, fresh_until=fresh, session_id=session_id,
        record_hash=sha256_json(record_core),
    )


def _verify_session_record(
    session: VerifiedSessionRecord, *, profile: Mapping[str, object],
    runtime_identity: Mapping[str, object], require_current_at: datetime | None = None,
) -> None:
    rebuilt = build_verified_session_record(
        profile=profile, runtime_identity=runtime_identity, account_stage=session.account_stage,
        calendar_provider_id=session.calendar_provider_id, calendar_version=session.calendar_version,
        calendar_sha256=session.calendar_sha256, session_open_at=session.session_open_at,
        provider_close_at=session.provider_close_at, source_as_of=session.source_as_of,
        fresh_until=session.fresh_until, session_kind=session.session_kind,
    )
    if rebuilt.session_id != session.session_id or rebuilt.record_hash != session.record_hash:
        raise ContractError("verified session identity or hash does not reconstruct")
    if require_current_at is not None:
        current = _aware(require_current_at, name="session verification time")
        if current < session.source_as_of or current > session.fresh_until:
            raise ContractError("verified session calendar is stale or not yet effective")


def build_completed_session_event(
    *, session: VerifiedSessionRecord, profile: Mapping[str, object],
    runtime_identity: Mapping[str, object], completed_session_eod_balance_usd: object,
    observed_at: datetime,
) -> CompletedSessionEvent:
    observed = _aware(observed_at, name="completed-session observation")
    _verify_session_record(session, profile=profile, runtime_identity=runtime_identity, require_current_at=observed)
    if observed < session.provider_close_at:
        raise ContractError("provider session is not completed")
    balance = money(completed_session_eod_balance_usd, name="completed-session EOD balance")
    core = {
        "session_id": session.session_id,
        "session_record_hash": session.record_hash,
        "completed_session_eod_balance_usd": str(balance),
        "observed_at": observed.isoformat(),
    }
    return CompletedSessionEvent(session, balance, observed, sha256_json(core))


def initial_eod_state(
    profile: Mapping[str, object], *, runtime_identity: Mapping[str, object],
    account_stage: str | None = None
) -> EodDrawdownState:
    stage = selected_stage(profile, account_stage)
    _runtime_binding(runtime_identity, profile=profile, stage=stage)
    rules = stage_rules(profile, stage=stage)
    if rules.get("drawdown_state_machine") != "ZERO_BASED_EOD_TRAILING_LOCKED":
        raise ContractError(f"stage {stage} has no supported zero-based EOD state machine")
    starting = money(rules.get("ledger_starting_balance_usd"), name="ledger starting balance")
    maximum_loss = money(rules.get("maximum_eod_loss_usd"), name="maximum EOD loss")
    initial_floor = money(rules.get("initial_loss_floor_usd"), name="initial loss floor")
    floor_lock = money(rules.get("loss_floor_lock_usd"), name="loss floor lock")
    if starting != 0 or maximum_loss <= 0 or initial_floor != starting - maximum_loss:
        raise ContractError("zero-based EOD stage limits are inconsistent")
    if floor_lock < initial_floor:
        raise ContractError("loss-floor lock cannot be below the initial floor")
    return EodDrawdownState(
        profile_id=str(runtime_identity["profile_id"]),
        profile_hash=str(runtime_identity["profile_hash"]),
        account_stage=stage,
        active_floor_usd=initial_floor,
        floor_lock_usd=floor_lock,
        maximum_loss_usd=maximum_loss,
        highest_completed_eod_balance_usd=starting,
        realized_account_balance_usd=starting,
        floor_locked=initial_floor == floor_lock,
    )


def apply_completed_session_eod(
    state: EodDrawdownState,
    *,
    event: CompletedSessionEvent,
    profile: Mapping[str, object],
    runtime_identity: Mapping[str, object],
) -> EodDrawdownState:
    _runtime_binding(runtime_identity, profile=profile, stage=state.account_stage)
    if state.profile_id != runtime_identity.get("profile_id") or state.profile_hash != runtime_identity.get("profile_hash"):
        raise ContractError("drawdown state belongs to another profile")
    _verify_session_record(event.session, profile=profile, runtime_identity=runtime_identity, require_current_at=event.observed_at)
    expected_event_hash = sha256_json({
        "session_id": event.session.session_id,
        "session_record_hash": event.session.record_hash,
        "completed_session_eod_balance_usd": str(event.completed_session_eod_balance_usd),
        "observed_at": event.observed_at.isoformat(),
    })
    if expected_event_hash != event.event_hash or event.observed_at < event.session.provider_close_at:
        raise ContractError("completed session event is invalid")
    balance = event.completed_session_eod_balance_usd
    if event.event_hash in state.processed_session_hashes:
        return state
    if state.last_completed_session_id == event.session.session_id:
        raise ContractError("duplicate session ID has different completed event")
    if state.last_completed_session_close_at is not None and event.session.provider_close_at <= state.last_completed_session_close_at:
        raise ContractError("completed sessions must be applied in provider-session order")
    candidate = balance - state.maximum_loss_usd
    next_floor = min(state.floor_lock_usd, max(state.active_floor_usd, candidate))
    return replace(
        state,
        active_floor_usd=next_floor,
        highest_completed_eod_balance_usd=max(
            state.highest_completed_eod_balance_usd, balance
        ),
        realized_account_balance_usd=balance,
        floor_locked=state.floor_locked or next_floor == state.floor_lock_usd,
        last_completed_session_id=event.session.session_id,
        last_completed_session_hash=event.event_hash,
        last_calendar_provider_id=event.session.calendar_provider_id,
        last_calendar_sha256=event.session.calendar_sha256,
        last_completed_session_close_at=event.session.provider_close_at,
        last_completed_session_balance_usd=balance,
        processed_session_hashes=(*state.processed_session_hashes, event.event_hash),
        processed_session_count=state.processed_session_count + 1,
    )


def enforce_intraday_equity(
    state: EodDrawdownState, *, current_equity_usd: object
) -> EodDrawdownState:
    equity = money(current_equity_usd, name="current intraday equity")
    if equity <= state.active_floor_usd:
        return replace(state, breached=True)
    return state


def apply_simulated_withdrawal_to_drawdown(
    state: EodDrawdownState, *, gross_withdrawal_usd: object
) -> EodDrawdownState:
    gross = money(gross_withdrawal_usd, name="gross withdrawal")
    if gross <= 0:
        raise ContractError("gross withdrawal must be positive")
    next_balance = state.realized_account_balance_usd - gross
    if next_balance <= state.active_floor_usd:
        raise ContractError("withdrawal would exhaust the conservatively usable account cushion")
    return replace(state, realized_account_balance_usd=next_balance)


def resolve_execution_instrument(
    instrument_mapping: Mapping[str, object], signal_root: str
) -> Mapping[str, object]:
    instruments = mapping(instrument_mapping.get("instruments"), name="instruments")
    instrument = mapping(instruments.get(signal_root), name=f"instrument {signal_root}")
    if instrument.get("enabled") is False or not instrument.get("execution_symbol"):
        raise ContractError(f"{signal_root} has no verified micro-only execution mapping")
    for field in (
        "underlying_risk_group",
        "contract_multiplier",
        "tick_size",
        "tick_value_usd",
        "session_calendar",
        "expiration_roll_policy",
        "mini_to_micro_economic_ratio",
    ):
        if field not in instrument:
            raise ContractError(f"{signal_root} mapping is missing {field}")
    if money(instrument["mini_to_micro_economic_ratio"], name="mini-to-micro ratio") != Decimal("0.1"):
        raise ContractError("verified micro mappings must preserve the one-tenth ratio")
    return instrument


def portfolio_micro_equivalent(
    *, open_minis: int, open_micros: int, working_minis: int, working_micros: int
) -> int:
    values = (open_minis, open_micros, working_minis, working_micros)
    if any(not isinstance(value, int) or value < 0 for value in values):
        raise ContractError("contract counts must be nonnegative integers")
    return 10 * (open_minis + working_minis) + open_micros + working_micros


def enforce_aggregate_position_limit(
    *,
    open_minis: int,
    open_micros: int,
    working_minis: int,
    working_micros: int,
    proposed_minis: int = 0,
    proposed_micros: int = 0,
    maximum_micro_equivalent: int = 30,
    micro_only: bool = True,
) -> int:
    if micro_only and any(value > 0 for value in (open_minis, working_minis, proposed_minis)):
        raise ContractError("micro-only strategy rejects mini/standard contract intent")
    projected = portfolio_micro_equivalent(
        open_minis=open_minis + proposed_minis,
        open_micros=open_micros + proposed_micros,
        working_minis=working_minis,
        working_micros=working_micros,
    )
    if projected > maximum_micro_equivalent:
        raise ContractError("projected portfolio exceeds aggregate micro-equivalent cap")
    return projected


def _size_micro_quantity(
    *,
    allowed_trade_risk_usd: object,
    stop_ticks: object,
    tick_value_usd: object,
    expected_slippage_usd: object,
    round_turn_fees_usd: object,
    firm_remaining_micros: int,
    per_instrument_cap: int,
    concurrent_risk_remaining_usd: object,
    session_loss_budget_remaining_usd: object,
    equity_to_floor_cushion_usd: object,
    internal_reserve_usd: object,
    platform_cap: int,
    liquidity_cap: int,
) -> tuple[int, Decimal]:
    stop = money(stop_ticks, name="stop ticks")
    tick_value = money(tick_value_usd, name="tick value")
    slippage = money(expected_slippage_usd, name="expected slippage")
    fees = money(round_turn_fees_usd, name="round-turn fees")
    if stop <= 0 or tick_value <= 0 or slippage < 0 or fees < 0:
        raise ContractError("risk inputs are inconsistent")
    risk_per_contract = stop * tick_value + slippage + fees
    usable_cushion = money(equity_to_floor_cushion_usd, name="equity-to-floor cushion") - money(
        internal_reserve_usd, name="internal reserve"
    )
    budget = min(
        money(allowed_trade_risk_usd, name="allowed trade risk"),
        money(concurrent_risk_remaining_usd, name="concurrent-risk remaining"),
        money(session_loss_budget_remaining_usd, name="session-loss budget remaining"),
        usable_cushion,
    )
    caps = (firm_remaining_micros, per_instrument_cap, platform_cap, liquidity_cap)
    if any(not isinstance(cap, int) or cap < 0 for cap in caps):
        raise ContractError("quantity caps must be nonnegative integers")
    desired = int((budget / risk_per_contract).to_integral_value(rounding=ROUND_FLOOR)) if budget > 0 else 0
    quantity = min(desired, *caps)
    if quantity <= 0:
        raise ContractError("risk-based micro quantity is zero")
    return quantity, risk_per_contract


@dataclass(frozen=True)
class StopDefinedExposure:
    signal_root: str
    execution_symbol: str
    quantity: int
    stop_ticks: Decimal


@dataclass(frozen=True)
class PortfolioRiskState:
    open_positions: tuple[StopDefinedExposure, ...]
    working_entries: tuple[StopDefinedExposure, ...]
    realized_session_loss_usd: Decimal
    current_equity_usd: Decimal
    active_floor_usd: Decimal


@dataclass(frozen=True)
class RuntimeSizingResult:
    quantity: int
    execution_symbol: str
    risk_per_contract_usd: Decimal
    existing_stop_defined_risk_usd: Decimal
    projected_micro_equivalent: int
    runtime_identity: Mapping[str, object]
    production_readiness: bool
    economics_classification: str


def _resolved_economics(
    *, instrument_mapping: Mapping[str, object], cost: Mapping[str, object],
    signal_root: str, requested_execution_symbol: str,
) -> tuple[Mapping[str, object], Decimal, Decimal]:
    instrument = resolve_execution_instrument(instrument_mapping, signal_root)
    execution_symbol = nonempty_string(instrument.get("execution_symbol"), name="execution symbol")
    if requested_execution_symbol != execution_symbol:
        raise ContractError("requested execution symbol does not match the selected mapping")
    fees = mapping(cost.get("round_turn_commission_usd"), name="round-turn costs")
    slippage = mapping(cost.get("expected_slippage_usd"), name="expected slippage")
    if execution_symbol not in fees or execution_symbol not in slippage:
        raise ContractError("selected execution costs do not cover the mapped micro contract")
    fee = money(fees[execution_symbol], name="round-turn fee")
    slip = money(slippage[execution_symbol], name="expected slippage")
    if fee <= 0 or slip <= 0:
        raise ContractError("runtime economics must be explicit and nonzero")
    return instrument, fee, slip


def size_runtime_order(
    *, root: Path, observed_runtime_identity: Mapping[str, object],
    account_stage: str, mode: str, research_cost_profile_id: str | None,
    strategy_candidate_id: str, signal_root: str, requested_execution_symbol: str,
    stop_ticks: object, portfolio_state: PortfolioRiskState,
) -> RuntimeSizingResult:
    """Authoritative local sizing boundary using only selected, hash-bound inputs."""

    if account_stage != "sim_funded":
        raise ContractError("funded strategy sizing requires explicit sim_funded stage")
    if mode not in {"PRODUCTION", "PROVISIONAL_RESEARCH"}:
        raise ContractError("unknown sizing mode fails closed")
    if mode == "PRODUCTION" and research_cost_profile_id is not None:
        raise ContractError("production sizing cannot select provisional research costs")
    if mode == "PROVISIONAL_RESEARCH" and research_cost_profile_id is None:
        raise ContractError("provisional research sizing requires a named cost profile")
    identity = build_runtime_identity(
        root=root, account_stage=account_stage,
        research_cost_profile_id=research_cost_profile_id,
    )
    assert_cache_identity(expected=identity, observed=observed_runtime_identity)
    _, profile = raw_profile(root=root, profile_id=str(identity["profile_id"]))
    bindings = load_runtime_bindings(root=root, profile=profile)
    _, strategy = bindings["strategy"]
    _, instrument_mapping = bindings["mapping"]
    if research_cost_profile_id is None:
        _, cost = bindings["cost"]
    else:
        _, cost = _named_config(
            root=root, relative=EXECUTION_COST_PATH,
            schema="prop_firm_execution_costs/1.0.0", collection_key="cost_profiles",
            selected_id=research_cost_profile_id,
        )
        validate_execution_cost_profile(cost)
    if mode == "PRODUCTION" and (
        cost.get("exact_provider_account_costs_verified") is not True
        or cost.get("production_readiness") is not True
        or identity.get("production_readiness") is not True
    ):
        raise ContractError("verified production fees and readiness are unresolved")
    if mode == "PROVISIONAL_RESEARCH" and (
        cost.get("provisional") is not True or cost.get("production_readiness") is not False
    ):
        raise ContractError("research economics are not an explicit provisional profile")
    instrument, fee, slippage = _resolved_economics(
        instrument_mapping=instrument_mapping, cost=cost, signal_root=signal_root,
        requested_execution_symbol=requested_execution_symbol,
    )
    candidates = {str(value["candidate_id"]): value for value in build_coarse_strategy_candidates(strategy)}
    if strategy_candidate_id not in candidates:
        raise ContractError("unknown strategy-risk candidate")
    candidate = candidates[strategy_candidate_id]
    limits = mapping(strategy.get("runtime_limits"), name="runtime limits")
    per_symbol_caps = mapping(limits.get("per_execution_symbol_micros"), name="symbol caps")
    liquidity_caps = mapping(limits.get("liquidity_micro_caps"), name="liquidity caps")
    all_exposures = (*portfolio_state.open_positions, *portfolio_state.working_entries)
    realized_session_loss = money(
        portfolio_state.realized_session_loss_usd, name="realized session loss"
    )
    if realized_session_loss < 0:
        raise ContractError("realized session loss must be nonnegative")
    maximum_entries = candidate.get("maximum_entries_per_session")
    if not isinstance(maximum_entries, int) or len(all_exposures) >= maximum_entries:
        raise ContractError("strategy entry-count limit leaves no new entry capacity")
    existing_risk = Decimal("0")
    symbol_quantity = 0
    group_quantity = 0
    group = nonempty_string(instrument.get("underlying_risk_group"), name="underlying risk group")
    for exposure in all_exposures:
        if not isinstance(exposure.quantity, int) or exposure.quantity < 0:
            raise ContractError("portfolio exposure quantity is invalid")
        bound, exposure_fee, exposure_slippage = _resolved_economics(
            instrument_mapping=instrument_mapping, cost=cost,
            signal_root=exposure.signal_root,
            requested_execution_symbol=exposure.execution_symbol,
        )
        exposure_stop = money(exposure.stop_ticks, name="exposure stop ticks")
        if exposure_stop <= 0:
            raise ContractError("portfolio exposure stop ticks must be positive")
        exposure_risk = (
            exposure_stop
            * money(bound["tick_value_usd"], name="exposure tick value")
            + exposure_fee + exposure_slippage
        ) * exposure.quantity
        if exposure_risk < 0:
            raise ContractError("portfolio stop-defined risk is invalid")
        existing_risk += exposure_risk
        if exposure.execution_symbol == requested_execution_symbol:
            symbol_quantity += exposure.quantity
        if bound.get("underlying_risk_group") == group:
            group_quantity += exposure.quantity
    if candidate.get("pyramiding") is False and symbol_quantity:
        raise ContractError("selected strategy candidate prohibits pyramiding")
    existing_quantity = sum(exposure.quantity for exposure in all_exposures)
    maximum_firm = stage_rules(profile, stage=account_stage).get("maximum_micros")
    if not isinstance(maximum_firm, int) or maximum_firm != limits.get("firm_micro_equivalent_cap"):
        raise ContractError("profile and strategy firm caps conflict")
    concentration_cap = limits.get("per_underlying_risk_group_micros")
    if not isinstance(concentration_cap, int):
        raise ContractError("underlying concentration cap is invalid")
    tick_value = money(instrument["tick_value_usd"], name="mapped tick value")
    quantity, per_contract = _size_micro_quantity(
        allowed_trade_risk_usd=candidate["planned_risk_per_trade_usd"],
        stop_ticks=stop_ticks, tick_value_usd=tick_value,
        expected_slippage_usd=slippage, round_turn_fees_usd=fee,
        firm_remaining_micros=max(0, maximum_firm - existing_quantity),
        per_instrument_cap=max(0, int(per_symbol_caps[requested_execution_symbol]) - symbol_quantity),
        concurrent_risk_remaining_usd=(
            money(candidate["maximum_concurrent_open_risk_usd"], name="concurrent risk") - existing_risk
        ),
        session_loss_budget_remaining_usd=(
            money(candidate["internal_session_stop_usd"], name="session stop")
            - realized_session_loss
            - existing_risk
        ),
        equity_to_floor_cushion_usd=(
            money(portfolio_state.current_equity_usd, name="current equity")
            - money(portfolio_state.active_floor_usd, name="active floor")
            - existing_risk
        ),
        internal_reserve_usd=candidate["minimum_reserved_cushion_usd"],
        platform_cap=max(0, int(limits["platform_micro_cap"]) - existing_quantity),
        liquidity_cap=min(
            max(0, int(liquidity_caps[requested_execution_symbol]) - symbol_quantity),
            max(0, concentration_cap - group_quantity),
        ),
    )
    return RuntimeSizingResult(
        quantity=quantity, execution_symbol=requested_execution_symbol,
        risk_per_contract_usd=per_contract, existing_stop_defined_risk_usd=existing_risk,
        projected_micro_equivalent=existing_quantity + quantity,
        runtime_identity=identity,
        production_readiness=bool(identity.get("production_readiness")),
        economics_classification=str(identity["economics_classification"]),
    )


def assert_no_same_underlying_hedge(
    *, existing: Iterable[Mapping[str, object]], proposed: Mapping[str, object]
) -> None:
    group = nonempty_string(proposed.get("underlying_risk_group"), name="proposed risk group")
    side = nonempty_string(proposed.get("side"), name="proposed side").upper()
    if side not in {"LONG", "SHORT"}:
        raise ContractError("side must be LONG or SHORT")
    for exposure in existing:
        if exposure.get("underlying_risk_group") != group:
            continue
        quantity = exposure.get("quantity")
        if not isinstance(quantity, int) or quantity < 0:
            raise ContractError("exposure quantity must be a nonnegative integer")
        existing_side = str(exposure.get("side", "")).upper()
        if quantity and existing_side in {"LONG", "SHORT"} and existing_side != side:
            raise ContractError("order would create prohibited same-underlying hedge")


@dataclass(frozen=True)
class ComplianceDecision:
    allowed: bool
    actions: tuple[str, ...]
    reasons: tuple[str, ...]


def _aware(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ContractError(f"{name} must be timezone-aware")
    return value


def _parse_aware_datetime(value: object, *, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(nonempty_string(value, name=name))
    except ValueError as exc:
        raise ContractError(f"{name} must be an ISO-8601 datetime") from exc
    return _aware(parsed, name=name)


def news_event_guard(
    *,
    now: datetime,
    account_stage: str,
    events: Sequence[Mapping[str, object]],
    restricted_categories: set[str],
    calendar_status: str,
    internal_safety_lead_seconds: int,
    live_enforcement: bool,
) -> ComplianceDecision:
    _aware(now, name="now")
    if account_stage == "evaluation":
        return ComplianceDecision(True, (), ())
    if account_stage not in {"sim_funded", "live"}:
        raise ContractError("news guard received an unknown account stage")
    if calendar_status != "CURRENT_VERIFIED":
        if live_enforcement:
            return ComplianceDecision(False, ("BLOCK_NEW_ORDERS",), ("NEWS_CALENDAR_NOT_CURRENT",))
        return ComplianceDecision(True, (), ("HISTORICAL_NEWS_COVERAGE_MISSING",))
    for event in events:
        category = nonempty_string(event.get("category"), name="event category")
        timestamp = event.get("timestamp")
        if not isinstance(timestamp, datetime):
            raise ContractError("event timestamp must be a datetime")
        _aware(timestamp, name="event timestamp")
        if category not in restricted_categories:
            continue
        start = timestamp - timedelta(seconds=120 + internal_safety_lead_seconds)
        end = timestamp + timedelta(seconds=120)
        if start <= now <= end:
            return ComplianceDecision(
                False,
                ("CANCEL_WORKING_ENTRIES", "FLATTEN_APPLICABLE_POSITIONS", "BLOCK_NEW_ORDERS"),
                (f"RESTRICTED_NEWS:{category}:{timestamp.isoformat()}",),
            )
    return ComplianceDecision(True, (), ())


def session_window_guard(
    *,
    now: datetime,
    session: VerifiedSessionRecord,
    profile: Mapping[str, object],
    runtime_identity: Mapping[str, object],
    internal_flatten_buffer_minutes: int,
) -> ComplianceDecision:
    now = _aware(now, name="now")
    try:
        _verify_session_record(
            session, profile=profile, runtime_identity=runtime_identity,
            require_current_at=now,
        )
    except ContractError:
        return ComplianceDecision(False, ("BLOCK_NEW_ORDERS",), ("SESSION_CALENDAR_NOT_CURRENT",))
    opened = session.session_open_at
    closed = session.provider_close_at
    if now < opened or now >= closed:
        return ComplianceDecision(False, ("CANCEL_WORKING_ORDERS", "BLOCK_NEW_ORDERS"), ("OUTSIDE_PROVIDER_SESSION",))
    internal_close = closed - timedelta(minutes=internal_flatten_buffer_minutes)
    if now >= internal_close:
        return ComplianceDecision(
            False,
            ("CANCEL_WORKING_ORDERS", "FLATTEN_ALL_POSITIONS", "RECONCILE", "BLOCK_NEW_ORDERS"),
            ("INTERNAL_FLATTEN_DEADLINE",),
        )
    return ComplianceDecision(True, (), ())


def inactivity_status(
    *, last_funded_trade_date: date | None, as_of_date: date, paused: bool = False
) -> ComplianceDecision:
    if paused:
        return ComplianceDecision(True, (), ("AUDITED_PROVIDER_NOTATED_PAUSE",))
    if last_funded_trade_date is None:
        return ComplianceDecision(False, ("OPERATOR_REVIEW",), ("LAST_FUNDED_TRADE_UNKNOWN",))
    elapsed = (as_of_date - last_funded_trade_date).days
    if elapsed < 0:
        raise ContractError("last funded trade cannot be in the future")
    if elapsed >= 7:
        return ComplianceDecision(False, ("HARD_COMPLIANCE_WARNING",), ("INACTIVITY_DAY_7_OR_LATER",))
    if elapsed == 6:
        return ComplianceDecision(True, ("ESCALATED_OPERATOR_WARNING",), ("INACTIVITY_DAY_6",))
    if elapsed == 5:
        return ComplianceDecision(True, ("OPERATOR_WARNING",), ("INACTIVITY_DAY_5",))
    return ComplianceDecision(True, (), ())


def price_limit_guard(
    *,
    current_price: object,
    reference_price: object,
    lower_limit: object | None,
    upper_limit: object | None,
    prohibited_distance_fraction: object,
    data_status: str,
    account_stage: str,
    live_enforcement: bool,
) -> ComplianceDecision:
    if account_stage == "evaluation":
        return ComplianceDecision(True, (), ())
    if account_stage not in {"sim_funded", "live"}:
        raise ContractError("price-limit guard received an unknown account stage")
    if data_status != "CURRENT_CONTRACT_SESSION_VERIFIED" or lower_limit is None or upper_limit is None:
        if live_enforcement:
            return ComplianceDecision(False, ("CANCEL_WORKING_ORDERS", "BLOCK_NEW_ORDERS"), ("PRICE_LIMIT_DATA_NOT_CURRENT",))
        return ComplianceDecision(True, (), ("HISTORICAL_PRICE_LIMIT_COVERAGE_MISSING",))
    price = money(current_price, name="current price")
    reference = abs(money(reference_price, name="reference price"))
    lower = money(lower_limit, name="lower limit")
    upper = money(upper_limit, name="upper limit")
    fraction = money(prohibited_distance_fraction, name="prohibited distance fraction")
    if reference <= 0 or fraction <= 0 or lower >= upper:
        raise ContractError("price-limit inputs are inconsistent")
    distance = reference * fraction
    if price <= lower + distance or price >= upper - distance:
        return ComplianceDecision(False, ("CANCEL_WORKING_ORDERS", "BLOCK_NEW_ORDERS"), ("WITHIN_PROVIDER_PRICE_LIMIT_ZONE",))
    return ComplianceDecision(True, (), ())


def order_conduct_guard(
    *,
    recent_order_timestamps: Sequence[datetime],
    now: datetime,
    rate_limit_per_minute: int,
    existing_working_orders: Sequence[Mapping[str, object]],
    proposed_order: Mapping[str, object],
) -> ComplianceDecision:
    now = _aware(now, name="now")
    recent = [timestamp for timestamp in recent_order_timestamps if now - _aware(timestamp, name="order timestamp") <= timedelta(minutes=1)]
    if len(recent) >= rate_limit_per_minute:
        return ComplianceDecision(False, ("BLOCK_NEW_ORDERS",), ("INTERNAL_ORDER_RATE_LIMIT",))
    duplicate_fields = ("symbol", "side", "order_type", "limit_price", "quantity")
    if any(all(order.get(field) == proposed_order.get(field) for field in duplicate_fields) for order in existing_working_orders):
        return ComplianceDecision(False, ("BLOCK_NEW_ORDERS",), ("DUPLICATE_WORKING_ORDER",))
    return ComplianceDecision(True, (), ())


def operational_state_guard(
    *,
    configured_account_stage: str,
    observed_account_stage: str,
    kill_switch_engaged: bool,
    reconciliation_status: str,
    external_state_status: str,
) -> ComplianceDecision:
    if configured_account_stage not in {"evaluation", "sim_funded", "live"}:
        raise ContractError("configured account stage is invalid")
    reasons: list[str] = []
    if observed_account_stage != configured_account_stage:
        reasons.append("UNEXPECTED_ACCOUNT_STAGE_TRANSITION")
    if kill_switch_engaged:
        reasons.append("KILL_SWITCH_ENGAGED")
    if reconciliation_status != "RECONCILED":
        reasons.append("ACCOUNT_STATE_NOT_RECONCILED")
    if external_state_status != "CURRENT_VERIFIED":
        reasons.append("EXTERNAL_STATE_UNCERTAIN")
    if reasons:
        return ComplianceDecision(False, ("BLOCK_NEW_ORDERS",), tuple(reasons))
    return ComplianceDecision(True, (), ())


def build_compliance_log_record(
    *,
    runtime_identity: Mapping[str, object],
    previous_record_hash: str,
    event_id: str,
    observed_at: datetime,
    guard_name: str,
    decision: ComplianceDecision,
    input_snapshot_hash: str,
) -> dict[str, object]:
    """Build one hash-chained record for an append-only decision sink."""

    if len(previous_record_hash) != 64 or any(
        character not in "0123456789abcdef" for character in previous_record_hash
    ):
        raise ContractError("previous compliance-record hash is invalid")
    if len(input_snapshot_hash) != 64 or any(
        character not in "0123456789abcdef" for character in input_snapshot_hash
    ):
        raise ContractError("compliance input-snapshot hash is invalid")
    cache_identity = runtime_identity.get("cache_identity")
    if not isinstance(cache_identity, str) or len(cache_identity) != 64:
        raise ContractError("runtime cache identity is missing")
    core: dict[str, object] = {
        "schema_version": "prop_firm_compliance_decision/1.0.0",
        "event_id": nonempty_string(event_id, name="compliance event ID"),
        "observed_at": _aware(observed_at, name="compliance observed_at").isoformat(),
        "guard_name": nonempty_string(guard_name, name="guard name"),
        "allowed": decision.allowed,
        "actions": list(decision.actions),
        "reasons": list(decision.reasons),
        "input_snapshot_hash": input_snapshot_hash,
        "runtime_cache_identity": cache_identity,
        "previous_record_hash": previous_record_hash,
        "append_only": True,
    }
    return {**core, "record_hash": sha256_json(core)}


@dataclass(frozen=True)
class PayoutRecord:
    request_id: str
    approved_at: datetime
    gross_account_withdrawal_usd: Decimal
    firm_share_usd: Decimal
    net_trader_cash_usd: Decimal
    balance_after_payout_usd: Decimal


@dataclass(frozen=True)
class PayoutState:
    first_funded_trade_at: datetime | None
    completed_funded_trading_days: tuple[str, ...]
    realized_account_balance_usd: Decimal
    active_loss_floor_usd: Decimal
    floor_locked: bool
    first_buffer_cleared: bool
    payouts: tuple[PayoutRecord, ...]
    cumulative_trader_cash_usd: Decimal


def _provider_session_day(value: datetime) -> date:
    local = _aware(value, name="provider-session timestamp").astimezone(
        ZoneInfo("America/New_York")
    )
    return local.date() + (timedelta(days=1) if local.time() >= time(18, 0) else timedelta())


def initial_payout_state(*, active_loss_floor_usd: object) -> PayoutState:
    return PayoutState(
        first_funded_trade_at=None,
        completed_funded_trading_days=(),
        realized_account_balance_usd=Decimal("0"),
        active_loss_floor_usd=money(active_loss_floor_usd, name="active loss floor"),
        floor_locked=False,
        first_buffer_cleared=False,
        payouts=(),
        cumulative_trader_cash_usd=Decimal("0"),
    )


def update_payout_account_state(
    state: PayoutState,
    *,
    realized_account_balance_usd: object,
    active_loss_floor_usd: object,
    floor_locked: bool,
    funded_trade_at: datetime | None = None,
    completed_trading_at: datetime | None = None,
    first_buffer_usd: object = "2100",
) -> PayoutState:
    balance = money(realized_account_balance_usd, name="realized account balance")
    first_trade = state.first_funded_trade_at
    if funded_trade_at is not None:
        funded_trade_at = _aware(funded_trade_at, name="funded trade timestamp")
        first_trade = min(filter(None, (first_trade, funded_trade_at)), default=funded_trade_at)
    days = state.completed_funded_trading_days
    if completed_trading_at is not None:
        completed_day = _provider_session_day(
            _aware(completed_trading_at, name="completed funded trading timestamp")
        ).isoformat()
        if completed_day not in days:
            days = tuple(sorted((*days, completed_day)))
    return replace(
        state,
        first_funded_trade_at=first_trade,
        completed_funded_trading_days=days,
        realized_account_balance_usd=balance,
        active_loss_floor_usd=money(active_loss_floor_usd, name="active loss floor"),
        floor_locked=floor_locked,
        first_buffer_cleared=state.first_buffer_cleared or balance >= money(first_buffer_usd, name="first buffer"),
    )


def payout_eligibility(
    state: PayoutState, *, policy: Mapping[str, object], as_of: datetime
) -> ComplianceDecision:
    as_of = _aware(as_of, name="payout as-of").astimezone(timezone.utc)
    if state.first_funded_trade_at is None:
        return ComplianceDecision(False, (), ("NO_FUNDED_TRADE",))
    first_trade_utc = _aware(
        state.first_funded_trade_at, name="first funded trade"
    ).astimezone(timezone.utc)
    if as_of < first_trade_utc:
        return ComplianceDecision(False, (), ("PAYOUT_CHRONOLOGY_PRECEDES_FIRST_TRADE",))
    minimum_days = policy.get("first_payout_min_completed_trading_days")
    if not isinstance(minimum_days, int) or minimum_days < 1:
        raise ContractError("payout minimum trading days are invalid")
    if not state.payouts:
        if len(state.completed_funded_trading_days) < minimum_days:
            return ComplianceDecision(False, (), ("FIRST_PAYOUT_TIMING_NOT_MET",))
        if state.realized_account_balance_usd < money(policy.get("first_buffer_usd"), name="first payout buffer"):
            return ComplianceDecision(False, (), ("FIRST_PAYOUT_BUFFER_NOT_CLEARED",))
        return ComplianceDecision(True, (), ("FIRST_PAYOUT_ELIGIBLE",))
    latest = state.payouts[-1]
    latest_utc = _aware(latest.approved_at, name="latest payout approval").astimezone(timezone.utc)
    if as_of <= latest_utc:
        return ComplianceDecision(False, (), ("PAYOUT_CHRONOLOGY_NOT_AFTER_PRIOR_APPROVAL",))
    if _provider_session_day(as_of) <= _provider_session_day(latest_utc):
        return ComplianceDecision(False, (), ("DAILY_FREQUENCY_NOT_MET",))
    profit_since = state.realized_account_balance_usd - latest.balance_after_payout_usd
    if profit_since < money(policy.get("subsequent_net_profit_required_usd"), name="subsequent profit requirement"):
        return ComplianceDecision(False, (), ("SUBSEQUENT_PROFIT_NOT_MET",))
    return ComplianceDecision(True, (), ("SUBSEQUENT_PAYOUT_ELIGIBLE",))


def approve_simulated_payout(
    state: PayoutState,
    *,
    policy: Mapping[str, object],
    request_id: str,
    approved_at: datetime,
    gross_request_usd: object,
    manual_amount_confirmed: bool,
) -> tuple[PayoutState, PayoutRecord]:
    request_id = nonempty_string(request_id, name="payout request ID")
    approved_utc = _aware(approved_at, name="approved_at").astimezone(timezone.utc)
    gross = money(gross_request_usd, name="gross payout request")
    for record in state.payouts:
        if record.request_id == request_id:
            if record.approved_at == approved_utc and record.gross_account_withdrawal_usd == gross:
                return state, record
            raise ContractError("duplicate payout request ID conflicts with prior approval")
    eligibility = payout_eligibility(state, policy=policy, as_of=approved_utc)
    if not eligibility.allowed:
        raise ContractError(eligibility.reasons[0])
    minimum = money(policy.get("minimum_request_usd"), name="minimum payout request")
    if gross < minimum:
        raise ContractError("payout request is below the minimum")
    if policy.get("maximum_withdrawable_rule") == "UNRESOLVED_MANUAL_CONFIRMATION_REQUIRED" and not manual_amount_confirmed:
        raise ContractError("maximum withdrawable amount requires manual confirmation")
    post_balance = state.realized_account_balance_usd - gross
    if gross <= 0 or post_balance <= state.active_loss_floor_usd:
        raise ContractError("payout would exhaust the conservatively usable account cushion")
    trader_share = money(policy.get("trader_share"), name="trader share")
    firm_share = money(policy.get("firm_share"), name="firm share")
    if trader_share + firm_share != Decimal("1"):
        raise ContractError("payout split must sum to one")
    record = PayoutRecord(
        request_id=request_id,
        approved_at=approved_utc,
        gross_account_withdrawal_usd=gross,
        firm_share_usd=gross * firm_share,
        net_trader_cash_usd=gross * trader_share,
        balance_after_payout_usd=post_balance,
    )
    return (
        replace(
            state,
            realized_account_balance_usd=post_balance,
            payouts=(*state.payouts, record),
            cumulative_trader_cash_usd=state.cumulative_trader_cash_usd + record.net_trader_cash_usd,
        ),
        record,
    )


@dataclass(frozen=True)
class FundedAccountState:
    runtime_identity: Mapping[str, object]
    drawdown: EodDrawdownState
    payout: PayoutState


def _payout_record_payload(record: PayoutRecord) -> dict[str, object]:
    return {
        "request_id": record.request_id,
        "approved_at": record.approved_at.astimezone(timezone.utc).isoformat(),
        "gross_account_withdrawal_usd": str(record.gross_account_withdrawal_usd),
        "firm_share_usd": str(record.firm_share_usd),
        "net_trader_cash_usd": str(record.net_trader_cash_usd),
        "balance_after_payout_usd": str(record.balance_after_payout_usd),
    }


def funded_account_state_payload(state: FundedAccountState) -> dict[str, object]:
    drawdown = state.drawdown
    payout = state.payout
    core: dict[str, object] = {
        "schema_version": "prop_firm_funded_account_state/1.0.0",
        "runtime_identity": dict(state.runtime_identity),
        "drawdown": {
            "profile_id": drawdown.profile_id,
            "profile_hash": drawdown.profile_hash,
            "account_stage": drawdown.account_stage,
            "active_floor_usd": str(drawdown.active_floor_usd),
            "floor_lock_usd": str(drawdown.floor_lock_usd),
            "maximum_loss_usd": str(drawdown.maximum_loss_usd),
            "highest_completed_eod_balance_usd": str(drawdown.highest_completed_eod_balance_usd),
            "realized_account_balance_usd": str(drawdown.realized_account_balance_usd),
            "floor_locked": drawdown.floor_locked,
            "last_completed_session_id": drawdown.last_completed_session_id,
            "last_completed_session_hash": drawdown.last_completed_session_hash,
            "last_calendar_provider_id": drawdown.last_calendar_provider_id,
            "last_calendar_sha256": drawdown.last_calendar_sha256,
            "last_completed_session_close_at": (
                drawdown.last_completed_session_close_at.isoformat()
                if drawdown.last_completed_session_close_at else None
            ),
            "last_completed_session_balance_usd": (
                str(drawdown.last_completed_session_balance_usd)
                if drawdown.last_completed_session_balance_usd is not None else None
            ),
            "processed_session_hashes": list(drawdown.processed_session_hashes),
            "processed_session_count": drawdown.processed_session_count,
            "breached": drawdown.breached,
        },
        "payout": {
            "first_funded_trade_at": (
                payout.first_funded_trade_at.astimezone(timezone.utc).isoformat()
                if payout.first_funded_trade_at else None
            ),
            "completed_funded_trading_days": list(payout.completed_funded_trading_days),
            "realized_account_balance_usd": str(payout.realized_account_balance_usd),
            "active_loss_floor_usd": str(payout.active_loss_floor_usd),
            "floor_locked": payout.floor_locked,
            "first_buffer_cleared": payout.first_buffer_cleared,
            "payouts": [_payout_record_payload(record) for record in payout.payouts],
            "cumulative_trader_cash_usd": str(payout.cumulative_trader_cash_usd),
        },
    }
    return {**core, "state_hash": sha256_json(core)}


def serialize_funded_account_state(state: FundedAccountState) -> str:
    return json.dumps(funded_account_state_payload(state), sort_keys=True, separators=(",", ":"))


def deserialize_funded_account_state(
    raw: str | bytes, *, expected_runtime_identity: Mapping[str, object]
) -> FundedAccountState:
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ContractError("funded account state is not canonical JSON") from exc
    document = mapping(payload, name="funded account state")
    state_hash = document.get("state_hash")
    core = {key: value for key, value in document.items() if key != "state_hash"}
    if state_hash != sha256_json(core):
        raise ContractError("funded account state hash does not reconstruct")
    if core.get("schema_version") != "prop_firm_funded_account_state/1.0.0":
        raise ContractError("funded account state schema is unsupported")
    identity = mapping(core.get("runtime_identity"), name="persisted runtime identity")
    assert_cache_identity(expected=expected_runtime_identity, observed=identity)
    d = mapping(core.get("drawdown"), name="persisted drawdown")
    if d.get("profile_id") != identity.get("profile_id") or d.get("profile_hash") != identity.get("profile_hash"):
        raise ContractError("persisted drawdown profile binding conflicts")
    if d.get("account_stage") != identity.get("account_stage"):
        raise ContractError("persisted drawdown stage binding conflicts")
    close_value = d.get("last_completed_session_close_at")
    close_at = _parse_aware_datetime(close_value, name="persisted completed-session close") if close_value is not None else None
    drawdown = EodDrawdownState(
        profile_id=nonempty_string(d.get("profile_id"), name="persisted profile ID"),
        profile_hash=sha256_text(d.get("profile_hash"), name="persisted profile hash"),
        account_stage=nonempty_string(d.get("account_stage"), name="persisted account stage"),
        active_floor_usd=money(d.get("active_floor_usd"), name="persisted active floor"),
        floor_lock_usd=money(d.get("floor_lock_usd"), name="persisted floor lock"),
        maximum_loss_usd=money(d.get("maximum_loss_usd"), name="persisted maximum loss"),
        highest_completed_eod_balance_usd=money(d.get("highest_completed_eod_balance_usd"), name="persisted highest EOD"),
        realized_account_balance_usd=money(d.get("realized_account_balance_usd"), name="persisted realized balance"),
        floor_locked=strict_bool(d.get("floor_locked"), name="persisted floor lock state"),
        last_completed_session_id=d.get("last_completed_session_id"),
        last_completed_session_hash=d.get("last_completed_session_hash"),
        last_calendar_provider_id=d.get("last_calendar_provider_id"),
        last_calendar_sha256=d.get("last_calendar_sha256"),
        last_completed_session_close_at=close_at,
        last_completed_session_balance_usd=(
            money(d.get("last_completed_session_balance_usd"), name="persisted last EOD balance")
            if d.get("last_completed_session_balance_usd") is not None else None
        ),
        processed_session_hashes=tuple(d.get("processed_session_hashes", ())),
        processed_session_count=int(d.get("processed_session_count", -1)),
        breached=strict_bool(d.get("breached"), name="persisted breach state"),
    )
    if (
        drawdown.processed_session_count < 0
        or drawdown.processed_session_count != len(drawdown.processed_session_hashes)
        or len(set(drawdown.processed_session_hashes)) != len(drawdown.processed_session_hashes)
        or any(
            sha256_text(value, name="processed session hash") != value
            for value in drawdown.processed_session_hashes
        )
    ):
        raise ContractError("persisted processed-session chronology is inconsistent")
    if drawdown.last_completed_session_hash is not None:
        sha256_text(drawdown.last_completed_session_hash, name="last completed session hash")
    if drawdown.last_calendar_sha256 is not None:
        sha256_text(drawdown.last_calendar_sha256, name="last calendar SHA-256")
    if close_at is not None:
        _aware(close_at, name="persisted completed-session close")
    terminal_fields = (
        drawdown.last_completed_session_id,
        drawdown.last_completed_session_hash,
        drawdown.last_calendar_provider_id,
        drawdown.last_calendar_sha256,
        drawdown.last_completed_session_close_at,
        drawdown.last_completed_session_balance_usd,
    )
    if drawdown.processed_session_count == 0:
        if any(value is not None for value in terminal_fields):
            raise ContractError("empty persisted session history has terminal fields")
    elif any(value is None for value in terminal_fields):
        raise ContractError("persisted terminal session binding is incomplete")
    else:
        sha256_text(drawdown.last_completed_session_id, name="last completed session ID")
        nonempty_string(drawdown.last_calendar_provider_id, name="last calendar provider ID")
    p = mapping(core.get("payout"), name="persisted payout")
    records: list[PayoutRecord] = []
    for item in p.get("payouts", ()):
        value = mapping(item, name="persisted payout record")
        records.append(PayoutRecord(
            request_id=nonempty_string(value.get("request_id"), name="payout request ID"),
            approved_at=_parse_aware_datetime(value.get("approved_at"), name="payout approval"),
            gross_account_withdrawal_usd=money(value.get("gross_account_withdrawal_usd"), name="gross payout"),
            firm_share_usd=money(value.get("firm_share_usd"), name="firm share"),
            net_trader_cash_usd=money(value.get("net_trader_cash_usd"), name="trader cash"),
            balance_after_payout_usd=money(value.get("balance_after_payout_usd"), name="post-payout balance"),
        ))
    if any(
        record.gross_account_withdrawal_usd <= 0
        or record.firm_share_usd < 0
        or record.net_trader_cash_usd < 0
        or record.firm_share_usd + record.net_trader_cash_usd
        != record.gross_account_withdrawal_usd
        for record in records
    ):
        raise ContractError("persisted payout record arithmetic is inconsistent")
    for record in records:
        _aware(record.approved_at, name="persisted payout approval")
    if (
        len({record.request_id for record in records}) != len(records)
        or any(
            records[index].approved_at >= records[index + 1].approved_at
            or _provider_session_day(records[index].approved_at)
            >= _provider_session_day(records[index + 1].approved_at)
            for index in range(len(records) - 1)
        )
    ):
        raise ContractError("persisted payout chronology is not strictly increasing")
    first_trade = p.get("first_funded_trade_at")
    completed_days = p.get("completed_funded_trading_days")
    if (
        not isinstance(completed_days, list)
        or any(not isinstance(value, str) for value in completed_days)
        or completed_days != sorted(set(completed_days))
    ):
        raise ContractError("persisted funded trading-day chronology is invalid")
    try:
        for value in completed_days:
            date.fromisoformat(value)
    except ValueError as exc:
        raise ContractError("persisted funded trading day is invalid") from exc
    payout = PayoutState(
        first_funded_trade_at=_parse_aware_datetime(first_trade, name="first funded trade") if first_trade else None,
        completed_funded_trading_days=tuple(completed_days),
        realized_account_balance_usd=money(p.get("realized_account_balance_usd"), name="payout realized balance"),
        active_loss_floor_usd=money(p.get("active_loss_floor_usd"), name="payout active floor"),
        floor_locked=strict_bool(p.get("floor_locked"), name="payout floor lock state"),
        first_buffer_cleared=strict_bool(p.get("first_buffer_cleared"), name="first buffer state"),
        payouts=tuple(records),
        cumulative_trader_cash_usd=money(p.get("cumulative_trader_cash_usd"), name="cumulative trader cash"),
    )
    if (
        payout.realized_account_balance_usd != drawdown.realized_account_balance_usd
        or payout.active_loss_floor_usd != drawdown.active_floor_usd
        or payout.floor_locked != drawdown.floor_locked
    ):
        raise ContractError("persisted drawdown and payout account state conflict")
    if payout.first_funded_trade_at is not None:
        _aware(payout.first_funded_trade_at, name="persisted first funded trade")
    if payout.cumulative_trader_cash_usd != sum(
        (record.net_trader_cash_usd for record in payout.payouts), Decimal("0")
    ):
        raise ContractError("persisted cumulative payout cash conflicts with chronology")
    return FundedAccountState(dict(identity), drawdown, payout)


__all__ = [
    "ComplianceDecision",
    "CompletedSessionEvent",
    "EXECUTION_COST_PATH",
    "EodDrawdownState",
    "FundedAccountState",
    "INSTRUMENT_MAPPING_PATH",
    "PAYOUT_POLICY_PATH",
    "PROFILE_PATH",
    "PayoutRecord",
    "PayoutState",
    "PortfolioRiskState",
    "RuntimeSizingResult",
    "STRATEGY_POLICY_PATH",
    "StopDefinedExposure",
    "VerifiedSessionRecord",
    "apply_completed_session_eod",
    "apply_simulated_withdrawal_to_drawdown",
    "approve_simulated_payout",
    "assert_cache_identity",
    "assert_no_same_underlying_hedge",
    "build_coarse_strategy_candidates",
    "build_completed_session_event",
    "build_compliance_log_record",
    "build_runtime_identity",
    "build_verified_session_record",
    "deserialize_funded_account_state",
    "enforce_aggregate_position_limit",
    "enforce_intraday_equity",
    "inactivity_status",
    "initial_eod_state",
    "initial_payout_state",
    "load_profile_document",
    "load_runtime_bindings",
    "news_event_guard",
    "order_conduct_guard",
    "operational_state_guard",
    "payout_eligibility",
    "portfolio_micro_equivalent",
    "price_limit_guard",
    "raw_profile",
    "resolve_execution_instrument",
    "serialize_funded_account_state",
    "selected_stage",
    "session_window_guard",
    "size_runtime_order",
    "stage_rules",
    "update_payout_account_state",
    "validate_execution_cost_profile",
    "validate_instrument_mapping",
    "validate_payout_policy",
    "validate_strategy_policy",
]
