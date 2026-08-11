"""Mandatory MFF runtime and execution-readiness pre-trade gate."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence

from futures_rebuild.errors import ContractError
from futures_rebuild.prop_firm_account_runtime import (
    PortfolioRiskState,
    RuntimeSizingResult,
    assert_no_same_underlying_hedge,
    build_runtime_identity,
    news_event_guard,
    operational_state_guard,
    order_conduct_guard,
    price_limit_guard,
    size_runtime_order,
)

from .arm_state import ArmState
from .domain import AccountBinding, OrderIntent
from .errors import ExecutionBlocked


VERIFIED_MICRO_MAPPINGS = {"ES": "MES", "CL": "MCL", "6E": "M6E"}


@dataclass(frozen=True)
class GateDecision:
    allowed: bool
    blockers: tuple[str, ...]
    authoritative_quantity: int
    sizing: RuntimeSizingResult | None


@dataclass(frozen=True)
class ProviderGateContext:
    root: Path
    binding: AccountBinding | None
    arm_state: ArmState
    now: datetime
    entitlement_confirmed: bool
    endpoint_confirmed: bool
    account_synchronized: bool
    reconciliation_status: str
    external_state_status: str
    production_readiness: bool
    exact_costs_verified: bool
    configured_stage: str
    observed_stage: str
    kill_switch_engaged: bool
    news_events: Sequence[Mapping[str, object]]
    news_status: str
    restricted_news_categories: set[str]
    current_price: float
    reference_price: float
    lower_price_limit: float | None
    upper_price_limit: float | None
    price_limit_status: str
    existing_exposures: Sequence[Mapping[str, object]]
    recent_order_timestamps: Sequence[datetime]
    working_orders: Sequence[Mapping[str, object]]
    portfolio_state: PortfolioRiskState
    runtime_identity: Mapping[str, object]
    strategy_candidate_id: str
    stop_ticks: float


def _decision_blockers(decision: object) -> list[str]:
    if getattr(decision, "allowed", False):
        return []
    return [str(value) for value in getattr(decision, "reasons", ())]


def evaluate_provider_intent(intent: OrderIntent, context: ProviderGateContext) -> GateDecision:
    """Evaluate every backend gate; provider submission is unreachable on failure."""

    blockers: list[str] = []
    binding = context.binding
    if binding is None:
        blockers.append("EXACT_ACCOUNT_BINDING_UNSET")
    else:
        if binding.provider_id != "my_funded_futures" or binding.platform_id != "tradovate":
            blockers.append("ACCOUNT_BINDING_PROVIDER_MISMATCH")
        if intent.account_binding_id != binding.binding_id or intent.account_binding_hash != binding.binding_hash:
            blockers.append("ACCOUNT_BINDING_MISMATCH")
        if intent.account_stage != binding.account_stage or context.configured_stage != binding.account_stage:
            blockers.append("ACCOUNT_STAGE_MISMATCH")
        if intent.profile_id != binding.profile_id or intent.profile_hash != binding.profile_hash:
            blockers.append("PROFILE_BINDING_MISMATCH")
        if intent.cost_profile_id != binding.cost_profile_id or intent.cost_profile_hash != binding.cost_profile_hash:
            blockers.append("COST_BINDING_MISMATCH")
        identity_bindings = {
            "profile_id": binding.profile_id,
            "profile_hash": binding.profile_hash,
            "account_stage": binding.account_stage,
            "execution_instrument_mapping_id": binding.instrument_mapping_id,
            "execution_instrument_mapping_hash": binding.instrument_mapping_hash,
            "execution_cost_profile_id": binding.cost_profile_id,
            "execution_cost_profile_hash": binding.cost_profile_hash,
        }
        if any(context.runtime_identity.get(name) != expected for name, expected in identity_bindings.items()):
            blockers.append("RUNTIME_IDENTITY_BINDING_MISMATCH")
        if (
            intent.strategy_policy_id != context.runtime_identity.get("strategy_policy_id")
            or intent.strategy_policy_hash != context.runtime_identity.get("strategy_policy_hash")
        ):
            blockers.append("STRATEGY_BINDING_MISMATCH")
        try:
            context.arm_state.require_armed(binding=binding, now=context.now)
        except ExecutionBlocked:
            blockers.append("EXECUTION_DISARMED")
    if not context.entitlement_confirmed:
        blockers.append("TRADOVATE_API_ENTITLEMENT_UNCONFIRMED")
    if not context.endpoint_confirmed:
        blockers.append("MFF_STAGE_ENDPOINT_UNCONFIRMED")
    if not context.account_synchronized:
        blockers.append("BROKER_SYNCHRONIZATION_STALE")
    if not context.production_readiness:
        blockers.append("PRODUCTION_READINESS_FALSE")
    if not context.exact_costs_verified:
        blockers.append("OFFICIAL_MFF_PLATFORM_FEES_UNSET")
    if VERIFIED_MICRO_MAPPINGS.get(intent.signal_instrument) != intent.execution_symbol:
        blockers.append("EXECUTION_INSTRUMENT_NOT_VERIFIED_MICRO")
    if intent.signal_instrument == "ZN":
        blockers.append("ZN_EXECUTION_MAPPING_DISABLED")

    try:
        operational = operational_state_guard(
            configured_account_stage=context.configured_stage,
            observed_account_stage=context.observed_stage,
            kill_switch_engaged=context.kill_switch_engaged,
            reconciliation_status=context.reconciliation_status,
            external_state_status=context.external_state_status,
        )
        blockers.extend(_decision_blockers(operational))
        news = news_event_guard(
            now=context.now,
            account_stage=context.configured_stage,
            events=context.news_events,
            restricted_categories=context.restricted_news_categories,
            calendar_status=context.news_status,
            internal_safety_lead_seconds=30,
            live_enforcement=True,
        )
        blockers.extend(_decision_blockers(news))
        limits = price_limit_guard(
            current_price=context.current_price,
            reference_price=context.reference_price,
            lower_limit=context.lower_price_limit,
            upper_limit=context.upper_price_limit,
            prohibited_distance_fraction="0.02",
            data_status=context.price_limit_status,
            account_stage=context.configured_stage,
            live_enforcement=True,
        )
        blockers.extend(_decision_blockers(limits))
        assert_no_same_underlying_hedge(
            existing=context.existing_exposures,
            proposed={"underlying_risk_group": intent.underlying_risk_group, "side": "LONG" if intent.side.value == "BUY" else "SHORT"},
        )
        conduct = order_conduct_guard(
            recent_order_timestamps=context.recent_order_timestamps,
            now=context.now,
            rate_limit_per_minute=6,
            existing_working_orders=context.working_orders,
            proposed_order={
                "symbol": intent.execution_symbol,
                "side": intent.side.value,
                "order_type": intent.order_type.value,
                "limit_price": intent.entry_price,
                "quantity": intent.requested_quantity,
            },
        )
        blockers.extend(_decision_blockers(conduct))
    except ContractError as exc:
        blockers.append(f"MFF_RUNTIME_REJECTED:{str(exc)[:120]}")

    if blockers:
        return GateDecision(False, tuple(dict.fromkeys(blockers)), 0, None)
    try:
        sizing = size_runtime_order(
            root=context.root,
            observed_runtime_identity=context.runtime_identity,
            account_stage=context.configured_stage,
            mode="PRODUCTION",
            research_cost_profile_id=None,
            strategy_candidate_id=context.strategy_candidate_id,
            signal_root=intent.signal_instrument,
            requested_execution_symbol=intent.execution_symbol,
            stop_ticks=context.stop_ticks,
            portfolio_state=context.portfolio_state,
        )
    except ContractError as exc:
        return GateDecision(False, (f"MFF_RUNTIME_SIZING_REJECTED:{str(exc)[:120]}",), 0, None)
    if sizing.quantity <= 0:
        return GateDecision(False, ("AUTHORITATIVE_QUANTITY_ZERO",), 0, sizing)
    if intent.requested_quantity > sizing.quantity:
        return GateDecision(False, ("REQUESTED_QUANTITY_EXCEEDS_AUTHORITATIVE_MAXIMUM",), sizing.quantity, sizing)
    return GateDecision(True, (), sizing.quantity, sizing)


def local_simulator_sizing(
    *,
    root: Path,
    signal_root: str,
    execution_symbol: str,
    stop_ticks: float,
    portfolio_state: PortfolioRiskState,
    strategy_candidate_id: str = "coarse-3",
) -> RuntimeSizingResult:
    """Exercise the existing runtime with explicit provisional economics only."""

    identity = build_runtime_identity(
        root=root,
        account_stage="sim_funded",
        research_cost_profile_id="mff_micro_provisional_stress_v1",
    )
    return size_runtime_order(
        root=root,
        observed_runtime_identity=identity,
        account_stage="sim_funded",
        mode="PROVISIONAL_RESEARCH",
        research_cost_profile_id="mff_micro_provisional_stress_v1",
        strategy_candidate_id=strategy_candidate_id,
        signal_root=signal_root,
        requested_execution_symbol=execution_symbol,
        stop_ticks=stop_ticks,
        portfolio_state=portfolio_state,
    )
