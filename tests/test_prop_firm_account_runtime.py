from copy import deepcopy
import inspect
import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import ContractError
from futures_rebuild.prop_firm_account_runtime import (
    apply_completed_session_eod,
    apply_simulated_withdrawal_to_drawdown,
    approve_simulated_payout,
    assert_cache_identity,
    assert_no_same_underlying_hedge,
    build_coarse_strategy_candidates,
    build_completed_session_event,
    build_compliance_log_record,
    build_runtime_identity,
    build_verified_session_record,
    deserialize_funded_account_state,
    FundedAccountState,
    enforce_aggregate_position_limit,
    enforce_intraday_equity,
    inactivity_status,
    initial_eod_state,
    initial_payout_state,
    load_runtime_bindings,
    news_event_guard,
    order_conduct_guard,
    operational_state_guard,
    payout_eligibility,
    PortfolioRiskState,
    price_limit_guard,
    raw_profile,
    resolve_execution_instrument,
    session_window_guard,
    serialize_funded_account_state,
    size_runtime_order,
    StopDefinedExposure,
    stage_rules,
    update_payout_account_state,
)
from futures_rebuild.prop_firm_eod_risk import load_active_profile, load_profile


ROOT = Path(__file__).parents[1]
NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
ACTIVE_ID = "mff_rapid_eod_50k_2026_08_10"


def _active_raw() -> dict[str, object]:
    profile_id, profile = raw_profile(root=ROOT)
    assert profile_id == ACTIVE_ID
    return dict(profile)


def _payout_policy(profile: dict[str, object] | None = None) -> dict[str, object]:
    _, selected = load_runtime_bindings(root=ROOT, profile=profile or _active_raw())["payout"]
    return dict(selected)


def _identity(*, provisional: bool = False) -> dict[str, object]:
    return build_runtime_identity(
        root=ROOT,
        research_cost_profile_id="mff_micro_provisional_stress_v1" if provisional else None,
    )


def _session(
    provider_day: date, *, fresh_until: datetime | None = None, session_kind: str = "ORDINARY"
):
    profile = _active_raw()
    identity = _identity()
    opened = datetime.combine(provider_day - timedelta(days=1), datetime.min.time(), tzinfo=NY).replace(hour=18)
    closed = datetime.combine(provider_day, datetime.min.time(), tzinfo=NY).replace(hour=16, minute=10)
    return build_verified_session_record(
        profile=profile,
        runtime_identity=identity,
        account_stage="sim_funded",
        calendar_provider_id="test-calendar",
        calendar_version="2026-08-test",
        calendar_sha256="a" * 64,
        session_open_at=opened,
        provider_close_at=closed,
        source_as_of=opened - timedelta(days=1),
        fresh_until=fresh_until or closed + timedelta(days=1),
        session_kind=session_kind,
    )


def _event(provider_day: date, balance: str):
    profile = _active_raw()
    identity = _identity()
    session = _session(provider_day)
    return build_completed_session_event(
        session=session,
        profile=profile,
        runtime_identity=identity,
        completed_session_eod_balance_usd=balance,
        observed_at=session.provider_close_at + timedelta(minutes=1),
    )


def _ready_first_payout(balance: str = "2100"):
    state = initial_payout_state(active_loss_floor_usd="100")
    state = update_payout_account_state(
        state,
        realized_account_balance_usd=balance,
        active_loss_floor_usd="100",
        floor_locked=True,
        funded_trade_at=datetime(2026, 8, 10, 10, tzinfo=NY),
        completed_trading_at=datetime(2026, 8, 10, 16, 10, tzinfo=NY),
    )
    return state


def test_active_profile_has_exact_stage_separation_and_primary_sim_funded_values() -> None:
    profile = _active_raw()
    assert profile["active_account_stage"] == "sim_funded"
    evaluation = stage_rules(profile, stage="evaluation")
    funded = stage_rules(profile, stage="sim_funded")
    live = stage_rules(profile, stage="live")
    assert evaluation["ledger_starting_balance_usd"] == "50000"
    assert evaluation["profit_target_usd"] == "3000"
    assert evaluation["evaluation_consistency_percent"] == "30"
    assert evaluation["minimum_trading_days"] == 4
    assert evaluation["t1_news_policy"] == "ALLOWED"
    assert funded["ledger_starting_balance_usd"] == "0"
    assert funded["initial_loss_floor_usd"] == "-2000"
    assert funded["loss_floor_lock_usd"] == "100"
    assert funded["firm_daily_loss_limit_usd"] is None
    assert funded["funded_consistency_rule"] is None
    assert funded["maximum_micros"] == 30
    assert funded["inactivity_calendar_days"] == 7
    assert live["stage_active"] is False
    assert live["loss_floor_lock_usd"] == "0"
    assert live["maximum_micros"] == 40
    assert live["automatic_transition_trigger_net_profit_one_session_usd"] == "10000"


def test_historical_profile_remains_loadable_with_frozen_object_hash() -> None:
    profile_id, raw = raw_profile(root=ROOT, profile_id="apex_eod_performance_50k")
    assert sha256_json(raw) == "4324fd0ca62986e92e4fb67af79e0062d25a0097285bc0686a12443174b49189"
    loaded_id, loaded = load_profile(root=ROOT, profile_id=profile_id)
    assert loaded_id == profile_id
    assert loaded["execution_connection"]["connection_id"] == "tradovate"


def test_unknown_stage_and_unexpected_live_transition_fail_closed() -> None:
    with pytest.raises(ContractError, match="unsupported account stage"):
        load_active_profile(root=ROOT, account_stage="funded")
    _, live = load_active_profile(root=ROOT, account_stage="live")
    assert live["account_stage"] == "live"
    assert live["stages"]["live"]["stage_active"] is False
    assert live["compliance"]["unexpected_stage_transition"] == (
        "BLOCK_NEW_ORDERS_UNTIL_RECONCILED"
    )


def test_runtime_identity_binds_all_selected_objects_and_rejects_cross_profile_cache() -> None:
    identity = build_runtime_identity(root=ROOT)
    for key in (
        "profile_hash",
        "strategy_policy_hash",
        "execution_instrument_mapping_hash",
        "execution_cost_profile_hash",
        "payout_policy_hash",
        "cache_identity",
    ):
        assert len(identity[key]) == 64
    assert identity["account_stage"] == "sim_funded"
    assert identity["strategy_policy_id"] == "mff_micro_risk_research_v1"
    assert_cache_identity(expected=identity, observed=identity)
    stale = deepcopy(identity)
    stale["profile_id"] = "historical_provider_profile"
    with pytest.raises(ContractError, match="does not match"):
        assert_cache_identity(expected=identity, observed=stale)


def test_strategy_search_is_bounded_sequential_and_logically_constrained() -> None:
    profile = _active_raw()
    _, policy = load_runtime_bindings(root=ROOT, profile=profile)["strategy"]
    candidates = build_coarse_strategy_candidates(policy)
    assert len(candidates) == 6
    assert all(candidate["pyramiding"] is False for candidate in candidates)
    assert all(
        Decimal(candidate["planned_risk_per_trade_usd"])
        <= Decimal(candidate["maximum_concurrent_open_risk_usd"])
        <= Decimal(candidate["internal_session_stop_usd"])
        for candidate in candidates
    )
    assert all(
        candidate["next_stage"]
        == "LOCAL_FINE_NEIGHBORS_SELECTED_FROM_TRAINING_FOLDS_ONLY"
        for candidate in candidates
    )


def test_eod_floor_uses_completed_balance_not_intraday_high_and_is_replay_safe() -> None:
    profile = _active_raw()
    identity = _identity()
    state = initial_eod_state(profile, runtime_identity=identity)
    assert state.active_floor_usd == Decimal("-2000")
    assert enforce_intraday_equity(state, current_equity_usd="1000") == state
    event = _event(date(2026, 8, 10), "500")
    closed = apply_completed_session_eod(state, event=event, profile=profile, runtime_identity=identity)
    assert closed.active_floor_usd == Decimal("-1500")
    replay = apply_completed_session_eod(closed, event=event, profile=profile, runtime_identity=identity)
    assert replay == closed
    conflicting = _event(date(2026, 8, 10), "501")
    with pytest.raises(ContractError, match="duplicate session ID"):
        apply_completed_session_eod(closed, event=conflicting, profile=profile, runtime_identity=identity)


def test_eod_floor_never_declines_locks_permanently_and_withdrawal_does_not_reset_it() -> None:
    profile = _active_raw()
    identity = _identity()
    state = initial_eod_state(profile, runtime_identity=identity)
    state = apply_completed_session_eod(state, event=_event(date(2026, 8, 10), "1000"), profile=profile, runtime_identity=identity)
    assert state.active_floor_usd == Decimal("-1000")
    state = apply_completed_session_eod(state, event=_event(date(2026, 8, 11), "600"), profile=profile, runtime_identity=identity)
    assert state.active_floor_usd == Decimal("-1000")
    state = apply_completed_session_eod(state, event=_event(date(2026, 8, 12), "2100"), profile=profile, runtime_identity=identity)
    assert state.active_floor_usd == Decimal("100")
    state = apply_completed_session_eod(state, event=_event(date(2026, 8, 13), "9000"), profile=profile, runtime_identity=identity)
    assert state.active_floor_usd == Decimal("100")
    assert apply_simulated_withdrawal_to_drawdown(
        state, gross_withdrawal_usd="500"
    ).active_floor_usd == Decimal("100")


def test_fixed_floor_blocks_on_touch_and_live_lock_is_separate() -> None:
    funded = initial_eod_state(_active_raw(), runtime_identity=_identity(), account_stage="sim_funded")
    assert enforce_intraday_equity(funded, current_equity_usd="-2000").breached is True
    with pytest.raises(ContractError, match="strategy policy account stage"):
        build_runtime_identity(root=ROOT, account_stage="live")


def test_eod_update_requires_verified_session_calendar_including_holidays() -> None:
    profile = _active_raw()
    identity = _identity()
    with pytest.raises(ContractError, match="ordinary session boundaries"):
        build_verified_session_record(
            profile=profile, runtime_identity=identity, account_stage="sim_funded",
            calendar_provider_id="calendar", calendar_version="v1", calendar_sha256="b" * 64,
            session_open_at=datetime(2026, 11, 26, 18, tzinfo=NY),
            provider_close_at=datetime(2026, 11, 27, 13, tzinfo=NY),
            source_as_of=datetime(2026, 11, 25, tzinfo=NY),
            fresh_until=datetime(2026, 11, 28, tzinfo=NY),
            session_kind="ORDINARY",
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(open_minis=0, open_micros=29, working_minis=0, working_micros=0, proposed_micros=2),
        dict(open_minis=0, open_micros=30, working_minis=0, working_micros=0, proposed_micros=1),
        dict(open_minis=2, open_micros=11, working_minis=0, working_micros=0, proposed_micros=0, micro_only=False),
        dict(open_minis=0, open_micros=20, working_minis=0, working_micros=11, proposed_micros=0),
    ],
)
def test_aggregate_cap_includes_all_instruments_and_working_orders(kwargs) -> None:
    with pytest.raises(ContractError, match="aggregate micro-equivalent cap"):
        enforce_aggregate_position_limit(**kwargs)


def test_micro_only_rejects_mini_intents_even_below_firm_mixed_cap() -> None:
    with pytest.raises(ContractError, match="micro-only"):
        enforce_aggregate_position_limit(
            open_minis=0,
            open_micros=0,
            working_minis=0,
            working_micros=0,
            proposed_minis=1,
        )
    assert enforce_aggregate_position_limit(
        open_minis=2,
        open_micros=0,
        working_minis=0,
        working_micros=9,
        micro_only=False,
    ) == 29


def test_runtime_sizing_resolves_mapping_costs_and_portfolio_risk_internally() -> None:
    identity = _identity(provisional=True)
    state = PortfolioRiskState(
        open_positions=(StopDefinedExposure("CL", "MCL", 2, Decimal("20")),),
        working_entries=(StopDefinedExposure("6E", "M6E", 1, Decimal("20")),),
        realized_session_loss_usd=Decimal("0"),
        current_equity_usd=Decimal("3000"),
        active_floor_usd=Decimal("-1000"),
    )
    result = size_runtime_order(
        root=ROOT, observed_runtime_identity=identity, account_stage="sim_funded",
        mode="PROVISIONAL_RESEARCH", research_cost_profile_id="mff_micro_provisional_stress_v1",
        strategy_candidate_id="coarse-3", signal_root="ES", requested_execution_symbol="MES",
        stop_ticks="20", portfolio_state=state,
    )
    assert result.risk_per_contract_usd == Decimal("30.00")
    assert result.existing_stop_defined_risk_usd == Decimal("80.00")
    assert result.quantity == 3
    assert result.production_readiness is False
    assert result.economics_classification == "PROVISIONAL_RESEARCH_STRESS_NOT_PROVIDER_VERIFIED"


def test_runtime_sizing_fails_closed_on_unresolved_or_caller_conflicting_economics() -> None:
    base = PortfolioRiskState((), (), Decimal("0"), Decimal("3000"), Decimal("-1000"))
    with pytest.raises(ContractError, match="verified production fees"):
        size_runtime_order(
            root=ROOT, observed_runtime_identity=_identity(), account_stage="sim_funded",
            mode="PRODUCTION", research_cost_profile_id=None, strategy_candidate_id="coarse-3",
            signal_root="ES", requested_execution_symbol="MES", stop_ticks="20",
            portfolio_state=base,
        )


def test_public_runtime_surfaces_do_not_accept_caller_supplied_cost_or_session_status() -> None:
    base = PortfolioRiskState((), (), Decimal("0"), Decimal("3000"), Decimal("-1000"))
    sizing = inspect.signature(size_runtime_order).parameters
    eod = inspect.signature(apply_completed_session_eod).parameters
    payout_update = inspect.signature(update_payout_account_state).parameters
    assert not {"tick_value_usd", "round_turn_fees_usd", "expected_slippage_usd"} & set(sizing)
    assert not {"completed_session_id", "session_calendar_status"} & set(eod)
    assert "completed_trading_day" not in payout_update
    with pytest.raises(ContractError, match="does not match the selected mapping"):
        size_runtime_order(
            root=ROOT, observed_runtime_identity=_identity(provisional=True), account_stage="sim_funded",
            mode="PROVISIONAL_RESEARCH", research_cost_profile_id="mff_micro_provisional_stress_v1",
            strategy_candidate_id="coarse-3", signal_root="ES", requested_execution_symbol="MCL",
            stop_ticks="20", portfolio_state=base,
        )


def test_runtime_sizing_rejects_zero_after_floor_reserve_and_open_risk_caps() -> None:
    identity = _identity(provisional=True)
    with pytest.raises(ContractError, match="quantity is zero"):
        size_runtime_order(
            root=ROOT, observed_runtime_identity=identity, account_stage="sim_funded",
            mode="PROVISIONAL_RESEARCH", research_cost_profile_id="mff_micro_provisional_stress_v1",
            strategy_candidate_id="coarse-1", signal_root="ES", requested_execution_symbol="MES",
            stop_ticks="20",
            portfolio_state=PortfolioRiskState((), (), Decimal("0"), Decimal("-1590"), Decimal("-2000")),
        )


def test_verified_micro_mappings_resolve_and_unsupported_root_fails_closed() -> None:
    profile = _active_raw()
    _, instrument_mapping = load_runtime_bindings(root=ROOT, profile=profile)["mapping"]
    assert resolve_execution_instrument(instrument_mapping, "ES")["execution_symbol"] == "MES"
    assert resolve_execution_instrument(instrument_mapping, "CL")["execution_symbol"] == "MCL"
    assert resolve_execution_instrument(instrument_mapping, "6E")["execution_symbol"] == "M6E"
    with pytest.raises(ContractError, match="no verified micro-only"):
        resolve_execution_instrument(instrument_mapping, "ZN")


def test_same_underlying_hedge_is_rejected_across_contract_sizes_and_months() -> None:
    existing = [
        {
            "symbol": "MESU26",
            "underlying_risk_group": "SP500_EQUITY_INDEX",
            "side": "LONG",
            "quantity": 2,
            "state": "OPEN",
        }
    ]
    with pytest.raises(ContractError, match="same-underlying hedge"):
        assert_no_same_underlying_hedge(
            existing=existing,
            proposed={
                "symbol": "ESZ26",
                "underlying_risk_group": "SP500_EQUITY_INDEX",
                "side": "SHORT",
                "quantity": 1,
            },
        )


def test_news_guard_blocks_at_exact_boundaries_and_stale_calendar_fails_closed() -> None:
    event_at = datetime(2026, 8, 12, 8, 30, tzinfo=NY)
    event = {"category": "CPI", "timestamp": event_at}
    for now in (event_at - timedelta(seconds=180), event_at + timedelta(seconds=120)):
        decision = news_event_guard(
            now=now,
            account_stage="sim_funded",
            events=[event],
            restricted_categories={"CPI", "FOMC", "EMPLOYMENT", "EIA"},
            calendar_status="CURRENT_VERIFIED",
            internal_safety_lead_seconds=60,
            live_enforcement=True,
        )
        assert decision.allowed is False
        assert "CANCEL_WORKING_ENTRIES" in decision.actions
        assert "FLATTEN_APPLICABLE_POSITIONS" in decision.actions
    stale = news_event_guard(
        now=event_at,
        account_stage="sim_funded",
        events=[],
        restricted_categories={"CPI"},
        calendar_status="STALE",
        internal_safety_lead_seconds=60,
        live_enforcement=True,
    )
    assert stale.allowed is False
    evaluation = news_event_guard(
        now=event_at,
        account_stage="evaluation",
        events=[event],
        restricted_categories={"CPI"},
        calendar_status="CURRENT_VERIFIED",
        internal_safety_lead_seconds=60,
        live_enforcement=True,
    )
    assert evaluation.allowed is True


def test_session_guard_uses_new_york_dst_and_fails_closed_on_unknown_holiday() -> None:
    profile = _active_raw()
    identity = _identity()
    session = _session(date(2026, 7, 13))
    assert session.session_open_at.utcoffset() == timedelta(hours=-4)
    normal = session_window_guard(
        now=datetime(2026, 7, 13, 15, 0, tzinfo=NY),
        session=session, profile=profile, runtime_identity=identity,
        internal_flatten_buffer_minutes=5,
    )
    assert normal.allowed is True
    flatten = session_window_guard(
        now=datetime(2026, 7, 13, 16, 5, tzinfo=NY),
        session=session, profile=profile, runtime_identity=identity,
        internal_flatten_buffer_minutes=5,
    )
    assert flatten.allowed is False
    assert "FLATTEN_ALL_POSITIONS" in flatten.actions
    stale = _session(date(2026, 7, 14), fresh_until=datetime(2026, 7, 14, 14, tzinfo=NY))
    holiday = session_window_guard(
        now=datetime(2026, 7, 14, 15, 0, tzinfo=NY),
        session=stale, profile=profile, runtime_identity=identity,
        internal_flatten_buffer_minutes=5,
    )
    assert holiday.allowed is False


def test_verified_shortened_session_is_explicit_and_tamper_fails_closed() -> None:
    profile = _active_raw()
    identity = _identity()
    shortened = build_verified_session_record(
        profile=profile, runtime_identity=identity, account_stage="sim_funded",
        calendar_provider_id="holiday-calendar", calendar_version="v1",
        calendar_sha256="c" * 64,
        session_open_at=datetime(2026, 11, 26, 18, tzinfo=NY),
        provider_close_at=datetime(2026, 11, 27, 13, tzinfo=NY),
        source_as_of=datetime(2026, 11, 25, tzinfo=NY),
        fresh_until=datetime(2026, 11, 28, tzinfo=NY),
        session_kind="VERIFIED_SHORTENED",
    )
    event = build_completed_session_event(
        session=shortened, profile=profile, runtime_identity=identity,
        completed_session_eod_balance_usd="500",
        observed_at=datetime(2026, 11, 27, 13, 1, tzinfo=NY),
    )
    updated = apply_completed_session_eod(
        initial_eod_state(profile, runtime_identity=identity), event=event,
        profile=profile, runtime_identity=identity,
    )
    assert updated.active_floor_usd == Decimal("-1500")
    forged = deepcopy(shortened)
    object.__setattr__(forged, "provider_close_at", datetime(2026, 11, 27, 17, 30, tzinfo=NY))
    assert session_window_guard(
        now=datetime(2026, 11, 27, 12, tzinfo=NY), session=forged,
        profile=profile, runtime_identity=identity, internal_flatten_buffer_minutes=5,
    ).allowed is False


def test_inactivity_monitor_escalates_without_generating_a_trade() -> None:
    last = date(2026, 8, 1)
    assert inactivity_status(last_funded_trade_date=last, as_of_date=date(2026, 8, 6)).reasons == ("INACTIVITY_DAY_5",)
    assert inactivity_status(last_funded_trade_date=last, as_of_date=date(2026, 8, 7)).reasons == ("INACTIVITY_DAY_6",)
    hard = inactivity_status(last_funded_trade_date=last, as_of_date=date(2026, 8, 8))
    assert hard.allowed is False
    assert hard.actions == ("HARD_COMPLIANCE_WARNING",)


def test_price_limit_guard_uses_current_contract_fixture_and_missing_data_blocks_live() -> None:
    near = price_limit_guard(
        current_price="109",
        reference_price="100",
        lower_limit="90",
        upper_limit="110",
        prohibited_distance_fraction="0.02",
        data_status="CURRENT_CONTRACT_SESSION_VERIFIED",
        account_stage="sim_funded",
        live_enforcement=True,
    )
    assert near.allowed is False
    missing = price_limit_guard(
        current_price="100",
        reference_price="100",
        lower_limit=None,
        upper_limit=None,
        prohibited_distance_fraction="0.02",
        data_status="MISSING",
        account_stage="live",
        live_enforcement=True,
    )
    assert missing.allowed is False


def test_order_conduct_guard_throttles_and_detects_duplicate_working_limit() -> None:
    now = datetime(2026, 8, 10, 14, tzinfo=UTC)
    order = {
        "symbol": "MESU26",
        "side": "BUY",
        "order_type": "LIMIT",
        "limit_price": "5500",
        "quantity": 1,
    }
    duplicate = order_conduct_guard(
        recent_order_timestamps=[],
        now=now,
        rate_limit_per_minute=12,
        existing_working_orders=[order],
        proposed_order=order,
    )
    assert duplicate.allowed is False
    throttled = order_conduct_guard(
        recent_order_timestamps=[now - timedelta(seconds=i) for i in range(12)],
        now=now,
        rate_limit_per_minute=12,
        existing_working_orders=[],
        proposed_order=order,
    )
    assert throttled.reasons == ("INTERNAL_ORDER_RATE_LIMIT",)


def test_uncertain_or_transitioned_state_blocks_and_decisions_are_hash_chained() -> None:
    blocked = operational_state_guard(
        configured_account_stage="sim_funded",
        observed_account_stage="live",
        kill_switch_engaged=False,
        reconciliation_status="UNKNOWN",
        external_state_status="STALE",
    )
    assert blocked.allowed is False
    assert "UNEXPECTED_ACCOUNT_STAGE_TRANSITION" in blocked.reasons
    identity = build_runtime_identity(root=ROOT)
    first = build_compliance_log_record(
        runtime_identity=identity,
        previous_record_hash="0" * 64,
        event_id="decision-1",
        observed_at=datetime(2026, 8, 10, 14, tzinfo=UTC),
        guard_name="operational_state",
        decision=blocked,
        input_snapshot_hash="1" * 64,
    )
    second = build_compliance_log_record(
        runtime_identity=identity,
        previous_record_hash=first["record_hash"],
        event_id="decision-2",
        observed_at=datetime(2026, 8, 10, 14, 1, tzinfo=UTC),
        guard_name="operational_state",
        decision=blocked,
        input_snapshot_hash="2" * 64,
    )
    assert second["previous_record_hash"] == first["record_hash"]
    assert second["record_hash"] != first["record_hash"]


def test_first_payout_buffer_timing_minimum_and_split() -> None:
    policy = _payout_policy()
    below = _ready_first_payout("2099")
    assert payout_eligibility(
        below, policy=policy, as_of=datetime(2026, 8, 11, 10, tzinfo=NY)
    ).allowed is False
    ready = _ready_first_payout("2100")
    assert payout_eligibility(
        ready, policy=policy, as_of=datetime(2026, 8, 11, 10, tzinfo=NY)
    ).allowed is True
    with pytest.raises(ContractError, match="below the minimum"):
        approve_simulated_payout(
            ready,
            policy=policy,
            request_id="payout-1",
            approved_at=datetime(2026, 8, 11, 10, tzinfo=NY),
            gross_request_usd="499",
            manual_amount_confirmed=True,
        )
    paid, record = approve_simulated_payout(
        ready,
        policy=policy,
        request_id="payout-1",
        approved_at=datetime(2026, 8, 11, 10, tzinfo=NY),
        gross_request_usd="500",
        manual_amount_confirmed=True,
    )
    assert record.firm_share_usd == Decimal("50.00")
    assert record.net_trader_cash_usd == Decimal("450.00")
    assert paid.realized_account_balance_usd == Decimal("1600")
    assert paid.active_loss_floor_usd == Decimal("100")
    replay, replay_record = approve_simulated_payout(
        paid,
        policy=policy,
        request_id="payout-1",
        approved_at=datetime(2026, 8, 11, 10, tzinfo=NY),
        gross_request_usd="500",
        manual_amount_confirmed=True,
    )
    assert replay == paid
    assert replay_record == record
    with pytest.raises(ContractError, match="conflicts"):
        approve_simulated_payout(
            paid, policy=policy, request_id="payout-1",
            approved_at=datetime(2026, 8, 11, 11, tzinfo=NY),
            gross_request_usd="500", manual_amount_confirmed=True,
        )


def test_subsequent_payout_requires_500_new_profit_and_daily_frequency() -> None:
    policy = _payout_policy()
    state, _ = approve_simulated_payout(
        _ready_first_payout("2600"),
        policy=policy,
        request_id="payout-1",
        approved_at=datetime(2026, 8, 11, 10, tzinfo=NY),
        gross_request_usd="500",
        manual_amount_confirmed=True,
    )
    same_day = payout_eligibility(
        state, policy=policy, as_of=datetime(2026, 8, 11, 15, tzinfo=NY)
    )
    assert same_day.reasons == ("DAILY_FREQUENCY_NOT_MET",)
    state = update_payout_account_state(
        state,
        realized_account_balance_usd="2599",
        active_loss_floor_usd="100",
        floor_locked=True,
    )
    assert payout_eligibility(
        state, policy=policy, as_of=datetime(2026, 8, 12, 10, tzinfo=NY)
    ).allowed is False
    state = update_payout_account_state(
        state,
        realized_account_balance_usd="2600",
        active_loss_floor_usd="100",
        floor_locked=True,
    )
    assert payout_eligibility(
        state, policy=policy, as_of=datetime(2026, 8, 12, 10, tzinfo=NY)
    ).allowed is True


def test_unknown_platform_costs_block_production_and_do_not_fabricate_fees() -> None:
    profile = _active_raw()
    _, cost = load_runtime_bindings(root=ROOT, profile=profile)["cost"]
    assert cost["platform_connection_id"] == "UNSET"
    assert cost["round_turn_commission_usd"] == {}
    assert cost["production_readiness"] is False


def test_payout_chronology_uses_absolute_time_and_provider_session_day() -> None:
    policy = _payout_policy()
    state, _ = approve_simulated_payout(
        _ready_first_payout("2600"), policy=policy, request_id="payout-time",
        approved_at=datetime(2026, 8, 11, 19, tzinfo=NY), gross_request_usd="500",
        manual_amount_confirmed=True,
    )
    state = update_payout_account_state(
        state, realized_account_balance_usd="2600", active_loss_floor_usd="100", floor_locked=True,
    )
    same_provider_day = payout_eligibility(
        state, policy=policy, as_of=datetime(2026, 8, 12, 10, tzinfo=NY)
    )
    assert same_provider_day.reasons == ("DAILY_FREQUENCY_NOT_MET",)
    inverted = payout_eligibility(
        state, policy=policy,
        as_of=datetime(2026, 8, 12, 0, 30, tzinfo=ZoneInfo("Pacific/Kiritimati")),
    )
    assert inverted.reasons == ("PAYOUT_CHRONOLOGY_NOT_AFTER_PRIOR_APPROVAL",)


def test_funded_state_round_trip_reconstructs_hash_and_replay_is_idempotent() -> None:
    profile = _active_raw()
    identity = _identity()
    event = _event(date(2026, 8, 10), "2600")
    drawdown = apply_completed_session_eod(
        initial_eod_state(profile, runtime_identity=identity), event=event,
        profile=profile, runtime_identity=identity,
    )
    payout = update_payout_account_state(
        initial_payout_state(active_loss_floor_usd=drawdown.active_floor_usd),
        realized_account_balance_usd=drawdown.realized_account_balance_usd,
        active_loss_floor_usd=drawdown.active_floor_usd, floor_locked=drawdown.floor_locked,
        funded_trade_at=datetime(2026, 8, 10, 10, tzinfo=NY),
        completed_trading_at=datetime(2026, 8, 10, 16, 10, tzinfo=NY),
    )
    payout, payout_record = approve_simulated_payout(
        payout, policy=_payout_policy(), request_id="restart-payout",
        approved_at=datetime(2026, 8, 11, 10, tzinfo=NY), gross_request_usd="500",
        manual_amount_confirmed=True,
    )
    drawdown = apply_simulated_withdrawal_to_drawdown(drawdown, gross_withdrawal_usd="500")
    serialized = serialize_funded_account_state(FundedAccountState(identity, drawdown, payout))
    restored = deserialize_funded_account_state(serialized, expected_runtime_identity=identity)
    assert restored is not None
    replayed = apply_completed_session_eod(
        restored.drawdown, event=event, profile=profile, runtime_identity=identity,
    )
    assert replayed == restored.drawdown
    payout_replay, replay_record = approve_simulated_payout(
        restored.payout, policy=_payout_policy(), request_id="restart-payout",
        approved_at=datetime(2026, 8, 11, 10, tzinfo=NY), gross_request_usd="500",
        manual_amount_confirmed=True,
    )
    assert payout_replay == restored.payout
    assert replay_record == payout_record
    tampered = serialized.replace('"active_floor_usd":"100"', '"active_floor_usd":"101"', 1)
    with pytest.raises(ContractError, match="state hash"):
        deserialize_funded_account_state(tampered, expected_runtime_identity=identity)
    structurally_invalid = json.loads(serialized)
    structurally_invalid["drawdown"]["floor_locked"] = "true"
    core = {key: value for key, value in structurally_invalid.items() if key != "state_hash"}
    structurally_invalid["state_hash"] = sha256_json(core)
    with pytest.raises(ContractError, match="must be boolean"):
        deserialize_funded_account_state(
            json.dumps(structurally_invalid), expected_runtime_identity=identity
        )
