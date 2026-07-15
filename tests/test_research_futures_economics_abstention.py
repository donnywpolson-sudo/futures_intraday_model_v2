from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from datetime import date, datetime, timezone

import numpy as np
import pytest

from futures_rebuild.research import (
    DecisionTimeIdentityCharter,
    Direction,
    EconomicsBinding,
    FuturesInferenceUnit,
    FuturesReturnBasis,
    IntervalIdentitySegment,
    IntervalResolutionStatus,
    IntervalRole,
    ResearchContractError,
    RoleIntervalWindow,
    assess_inference_unit,
    assess_interval_identity_bundle,
    build_pnl_row,
    coverage_denominator_indices,
    pnl_rows_to_float64,
)


def _economics(*, actual: str = "a" * 64, record: str = "b" * 64) -> EconomicsBinding:
    return EconomicsBinding(
        actual_contract_id=actual,
        economics_record_id=record,
        tick_size=Decimal("0.25"),
        tick_value=Decimal("12.50"),
        point_value=Decimal("50"),
        currency="USD",
    )


def _charter(
    windows: dict[IntervalRole, RoleIntervalWindow],
    *,
    decision_at: datetime,
    entry_at: datetime,
    declared_at: datetime | None = None,
    actual: str = "a" * 64,
    economics: str = "b" * 64,
    roll_safe: bool = True,
) -> DecisionTimeIdentityCharter:
    return DecisionTimeIdentityCharter(
        declared_at=decision_at if declared_at is None else declared_at,
        decision_at=decision_at,
        planned_entry_at=entry_at,
        role_windows=tuple(windows[role] for role in IntervalRole),
        declared_execution_actual_contract_id=actual,
        declared_execution_economics_record_id=economics,
        roll_safety_policy_receipt_sha256="d" * 64,
        horizon_declared_roll_safe=roll_safe,
    )


def _interval_binding():
    feature_start = datetime(2026, 7, 14, 19, 0, tzinfo=timezone.utc)
    decision_at = datetime(2026, 7, 14, 20, 0, tzinfo=timezone.utc)
    entry_at = datetime(2026, 7, 14, 20, 1, tzinfo=timezone.utc)
    exit_at = datetime(2026, 7, 14, 20, 31, tzinfo=timezone.utc)
    windows = {
        IntervalRole.FEATURE: RoleIntervalWindow(
            IntervalRole.FEATURE, feature_start, decision_at
        ),
        IntervalRole.LABEL: RoleIntervalWindow(
            IntervalRole.LABEL, entry_at, exit_at
        ),
        IntervalRole.RETURN: RoleIntervalWindow(
            IntervalRole.RETURN, entry_at, exit_at
        ),
        IntervalRole.PNL: RoleIntervalWindow(IntervalRole.PNL, entry_at, exit_at),
    }
    segments = {
        role: (
            IntervalIdentitySegment(
                window.start_at,
                window.end_at,
                date(2026, 7, 14),
                "a" * 64,
                "b" * 64,
            ),
        )
        for role, window in windows.items()
    }
    decision = assess_interval_identity_bundle(
        segments,
        charter=_charter(windows, decision_at=decision_at, entry_at=entry_at),
    )
    assert decision.eligible and decision.binding is not None
    return decision.binding


def _row(*, session: int = 10, direction: Direction = Direction.LONG):
    interval_binding = _interval_binding()
    execution = interval_binding.window_for(IntervalRole.PNL)
    return build_pnl_row(
        market_id="ES",
        direction=direction,
        session_ordinal=session,
        economics=_economics(),
        interval_identity=interval_binding,
        execution_start_at=execution.start_at,
        execution_end_at=execution.end_at,
        entry_price=Decimal("100.00"),
        exit_price=Decimal("100.50"),
        quantity=2,
        commission_per_contract=Decimal("1.00"),
        exchange_fees_per_contract=Decimal("0.50"),
        round_trip_slippage_ticks_per_contract=2,
    )


def test_tick_exact_actual_contract_pnl_oracle() -> None:
    row = _row()
    assert row.entry_price_ticks == 400
    assert row.exit_price_ticks == 402
    assert row.gross_pnl == Decimal("50.00")
    assert row.commission_per_contract == Decimal("1.00")
    assert row.exchange_fees_per_contract == Decimal("0.50")
    assert row.total_commission == Decimal("2.00")
    assert row.total_exchange_fees == Decimal("1.00")
    assert row.round_trip_slippage_ticks_per_contract == 2
    assert row.total_round_trip_slippage_cost == Decimal("50.00")
    assert row.total_round_trip_slippage_cost == (
        Decimal(1 + 1) * Decimal(row.quantity) * Decimal("12.50")
    )
    assert row.net_pnl == Decimal("-3.00")
    assert row.entry_price_notional == Decimal("10000.00")
    assert row.return_basis is FuturesReturnBasis.ENTRY_PRICE_NOTIONAL
    assert row.net_return_on_entry_price_notional == Decimal("-0.0003")
    assert row.actual_contract_id == "a" * 64
    assert row.economics_record_id == "b" * 64
    assert len(row.row_id) == 64


def test_short_direction_and_off_tick_or_mismatched_economics_fail_closed() -> None:
    assert _row(direction=Direction.SHORT).gross_pnl == Decimal("-50.00")
    interval_binding = _interval_binding()
    execution = interval_binding.window_for(IntervalRole.PNL)
    with pytest.raises(ResearchContractError, match="off the verified tick grid"):
        build_pnl_row(
            market_id="ES",
            direction=Direction.LONG,
            session_ordinal=10,
            economics=_economics(),
            interval_identity=interval_binding,
            execution_start_at=execution.start_at,
            execution_end_at=execution.end_at,
            entry_price=Decimal("100.10"),
            exit_price=Decimal("100.50"),
            quantity=1,
            commission_per_contract=Decimal("0"),
            exchange_fees_per_contract=Decimal("0"),
            round_trip_slippage_ticks_per_contract=0,
        )
    row = _row()
    with pytest.raises(ResearchContractError, match="mismatched"):
        row.validate(_economics(record="c" * 64), _interval_binding())


def test_pnl_rows_convert_only_with_verified_isolated_sleeve_economics() -> None:
    rows = (_row(session=10), _row(session=11))
    values = pnl_rows_to_float64(
        rows,
        economics_by_id={"b" * 64: _economics()},
        interval_identity_by_id={_interval_binding().binding_id: _interval_binding()},
        market_id="ES",
        direction=Direction.LONG,
    )
    assert values.dtype == np.float64
    np.testing.assert_allclose(values, np.asarray([-0.0003, -0.0003]))
    with pytest.raises(ResearchContractError, match="cannot be pooled"):
        pnl_rows_to_float64(
            rows,
            economics_by_id={"b" * 64: _economics()},
            interval_identity_by_id={_interval_binding().binding_id: _interval_binding()},
            market_id="NQ",
            direction=Direction.LONG,
        )
    with pytest.raises(ResearchContractError, match="lacks verified economics"):
        pnl_rows_to_float64(
            rows,
            economics_by_id={},
            interval_identity_by_id={_interval_binding().binding_id: _interval_binding()},
            market_id="ES",
            direction=Direction.LONG,
        )


def test_utc_midnight_mapping_is_resolved_and_contract_change_abstains() -> None:
    feature_start = datetime(2026, 7, 14, 23, 58, tzinfo=timezone.utc)
    decision_at = datetime(2026, 7, 14, 23, 58, 59, tzinfo=timezone.utc)
    entry_at = datetime(2026, 7, 14, 23, 59, tzinfo=timezone.utc)
    midnight = datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 7, 15, 0, 1, tzinfo=timezone.utc)
    windows = {
        IntervalRole.FEATURE: RoleIntervalWindow(
            IntervalRole.FEATURE, feature_start, decision_at
        ),
        IntervalRole.LABEL: RoleIntervalWindow(IntervalRole.LABEL, entry_at, end),
        IntervalRole.RETURN: RoleIntervalWindow(IntervalRole.RETURN, entry_at, end),
        IntervalRole.PNL: RoleIntervalWindow(IntervalRole.PNL, entry_at, end),
    }
    feature_segment = (
        IntervalIdentitySegment(
            feature_start, decision_at, date(2026, 7, 14), "a" * 64, "b" * 64
        ),
    )
    stable_execution = (
        IntervalIdentitySegment(
            entry_at, midnight, date(2026, 7, 14), "a" * 64, "b" * 64
        ),
        IntervalIdentitySegment(
            midnight, end, date(2026, 7, 15), "a" * 64, "b" * 64
        ),
    )
    stable_by_role = {IntervalRole.FEATURE: feature_segment}
    stable_by_role.update(
        {role: stable_execution for role in IntervalRole if role is not IntervalRole.FEATURE}
    )
    stable_decision = assess_interval_identity_bundle(
        stable_by_role,
        charter=_charter(windows, decision_at=decision_at, entry_at=entry_at),
    )
    assert stable_decision.eligible is True

    changed_execution = (
        stable_execution[0],
        IntervalIdentitySegment(
            midnight, end, date(2026, 7, 15), "c" * 64, "b" * 64
        ),
    )
    changed_by_role = {IntervalRole.FEATURE: feature_segment}
    changed_by_role.update(
        {role: changed_execution for role in IntervalRole if role is not IntervalRole.FEATURE}
    )
    changed_decision = assess_interval_identity_bundle(
        changed_by_role,
        charter=_charter(windows, decision_at=decision_at, entry_at=entry_at),
    )
    assert changed_decision.eligible is False
    assert changed_decision.decision_eligible is True
    assert changed_decision.prediction_in_coverage_denominator is True
    assert changed_decision.status is IntervalResolutionStatus.POST_DECISION_UNRESOLVED
    assert changed_decision.binding is None
    assert changed_decision.abstention_reasons == (
        "LABEL_ACTUAL_CONTRACT_CHANGED_WITHIN_INTERVAL",
        "PNL_ACTUAL_CONTRACT_CHANGED_WITHIN_INTERVAL",
        "RETURN_ACTUAL_CONTRACT_CHANGED_WITHIN_INTERVAL",
    )


def test_role_intervals_are_independent_but_causally_bound() -> None:
    binding = _interval_binding()
    feature = binding.window_for(IntervalRole.FEATURE)
    label = binding.window_for(IntervalRole.LABEL)
    assert feature.start_at != label.start_at
    assert feature.end_at == binding.decision_at
    assert label.start_at == binding.planned_entry_at


@pytest.mark.parametrize(
    ("role", "start_hour", "start_minute", "end_hour", "end_minute", "reason"),
    (
        (
            IntervalRole.FEATURE,
            19,
            0,
            20,
            2,
            "FEATURE_USES_POST_DECISION_DATA",
        ),
        (
            IntervalRole.LABEL,
            20,
            0,
            20,
            31,
            "LABEL_START_DOES_NOT_MATCH_ENTRY",
        ),
    ),
)
def test_causal_role_violations_abstain(
    role: IntervalRole,
    start_hour: int,
    start_minute: int,
    end_hour: int,
    end_minute: int,
    reason: str,
) -> None:
    feature_start = datetime(2026, 7, 14, 19, 0, tzinfo=timezone.utc)
    decision_at = datetime(2026, 7, 14, 20, 0, tzinfo=timezone.utc)
    entry_at = datetime(2026, 7, 14, 20, 1, tzinfo=timezone.utc)
    exit_at = datetime(2026, 7, 14, 20, 31, tzinfo=timezone.utc)
    windows = {
        IntervalRole.FEATURE: RoleIntervalWindow(
            IntervalRole.FEATURE, feature_start, decision_at
        ),
        IntervalRole.LABEL: RoleIntervalWindow(IntervalRole.LABEL, entry_at, exit_at),
        IntervalRole.RETURN: RoleIntervalWindow(IntervalRole.RETURN, entry_at, exit_at),
        IntervalRole.PNL: RoleIntervalWindow(IntervalRole.PNL, entry_at, exit_at),
    }
    windows[role] = RoleIntervalWindow(
        role,
        datetime(2026, 7, 14, start_hour, start_minute, tzinfo=timezone.utc),
        datetime(2026, 7, 14, end_hour, end_minute, tzinfo=timezone.utc),
    )
    segments = {
        current_role: (
            IntervalIdentitySegment(
                window.start_at,
                window.end_at,
                date(2026, 7, 14),
                "a" * 64,
                "b" * 64,
            ),
        )
        for current_role, window in windows.items()
    }
    result = assess_interval_identity_bundle(
        segments,
        charter=_charter(windows, decision_at=decision_at, entry_at=entry_at),
    )
    assert result.eligible is False
    assert reason in result.abstention_reasons


def test_return_and_pnl_windows_must_match_and_remain_inside_label() -> None:
    binding = _interval_binding()
    windows = {window.role: window for window in binding.role_windows}
    pnl = windows[IntervalRole.PNL]
    shortened_pnl = RoleIntervalWindow(
        IntervalRole.PNL,
        pnl.start_at,
        datetime(2026, 7, 14, 20, 30, tzinfo=timezone.utc),
    )
    windows[IntervalRole.PNL] = shortened_pnl
    segments = {
        role: (
            IntervalIdentitySegment(
                window.start_at,
                window.end_at,
                date(2026, 7, 14),
                "a" * 64,
                "b" * 64,
            ),
        )
        for role, window in windows.items()
    }
    result = assess_interval_identity_bundle(
        segments,
        charter=_charter(
            windows,
            decision_at=binding.decision_at,
            entry_at=binding.planned_entry_at,
        ),
    )
    assert result.abstention_reasons == (
        "EXECUTION_END_DOES_NOT_MATCH_LABEL",
        "EXECUTION_INTERVAL_MISMATCH",
    )


def test_role_identity_coverage_is_contiguous_and_feature_identity_is_independent() -> None:
    binding = _interval_binding()
    windows = {window.role: window for window in binding.role_windows}
    segments = {
        role: (
            IntervalIdentitySegment(
                window.start_at,
                window.end_at,
                date(2026, 7, 14),
                "a" * 64,
                "b" * 64,
            ),
        )
        for role, window in windows.items()
    }
    feature = windows[IntervalRole.FEATURE]
    segments[IntervalRole.FEATURE] = (
        IntervalIdentitySegment(
            feature.start_at,
            datetime(2026, 7, 14, 19, 30, tzinfo=timezone.utc),
            date(2026, 7, 14),
            "a" * 64,
            "b" * 64,
        ),
        IntervalIdentitySegment(
            datetime(2026, 7, 14, 19, 31, tzinfo=timezone.utc),
            feature.end_at,
            date(2026, 7, 14),
            "a" * 64,
            "b" * 64,
        ),
    )
    gap = assess_interval_identity_bundle(
        segments,
        charter=_charter(
            windows,
            decision_at=binding.decision_at,
            entry_at=binding.planned_entry_at,
        ),
    )
    assert gap.abstention_reasons == (
        "FEATURE_INCOMPLETE_INTERVAL_IDENTITY_COVERAGE",
    )
    assert gap.status is IntervalResolutionStatus.PRE_DECISION_INELIGIBLE
    assert gap.prediction_in_coverage_denominator is False

    segments[IntervalRole.FEATURE] = (
        IntervalIdentitySegment(
            feature.start_at,
            feature.end_at,
            date(2026, 7, 14),
            "c" * 64,
            "b" * 64,
        ),
    )
    mismatch = assess_interval_identity_bundle(
        segments,
        charter=_charter(
            windows,
            decision_at=binding.decision_at,
            entry_at=binding.planned_entry_at,
        ),
    )
    assert mismatch.eligible is True
    assert mismatch.binding is not None
    assert (
        mismatch.binding.identity_for(IntervalRole.FEATURE).actual_contract_id
        == "c" * 64
    )
    assert (
        mismatch.binding.identity_for(IntervalRole.PNL).actual_contract_id
        == "a" * 64
    )


def test_decision_time_roll_safety_declaration_is_mandatory_and_causal() -> None:
    binding = _interval_binding()
    windows = {window.role: window for window in binding.role_windows}
    not_roll_safe = assess_interval_identity_bundle(
        {},
        charter=_charter(
            windows,
            decision_at=binding.decision_at,
            entry_at=binding.planned_entry_at,
            roll_safe=False,
        ),
    )
    assert not_roll_safe.status is IntervalResolutionStatus.PRE_DECISION_INELIGIBLE
    assert not_roll_safe.failure_reasons == ("HORIZON_NOT_DECLARED_ROLL_SAFE",)
    assert not_roll_safe.prediction_in_coverage_denominator is False

    declared_late = assess_interval_identity_bundle(
        {},
        charter=_charter(
            windows,
            decision_at=binding.decision_at,
            entry_at=binding.planned_entry_at,
            declared_at=binding.planned_entry_at,
        ),
    )
    assert declared_late.failure_reasons == (
        "IDENTITY_CHARTER_DECLARED_AFTER_DECISION",
    )
    assert declared_late.prediction_in_coverage_denominator is False

    same_time_windows = dict(windows)
    for role in (IntervalRole.LABEL, IntervalRole.RETURN, IntervalRole.PNL):
        same_time_windows[role] = RoleIntervalWindow(
            role,
            binding.decision_at,
            windows[role].end_at,
        )
    same_time = assess_interval_identity_bundle(
        {},
        charter=_charter(
            same_time_windows,
            decision_at=binding.decision_at,
            entry_at=binding.decision_at,
        ),
    )
    assert same_time.failure_reasons == (
        "ENTRY_NOT_STRICTLY_AFTER_DECISION",
    )

    shifted_execution_windows = dict(windows)
    shifted_start = datetime(2026, 7, 14, 20, 2, tzinfo=timezone.utc)
    for role in (IntervalRole.RETURN, IntervalRole.PNL):
        shifted_execution_windows[role] = RoleIntervalWindow(
            role,
            shifted_start,
            windows[role].end_at,
        )
    shifted_execution = assess_interval_identity_bundle(
        {},
        charter=_charter(
            shifted_execution_windows,
            decision_at=binding.decision_at,
            entry_at=binding.planned_entry_at,
        ),
    )
    assert shifted_execution.failure_reasons == (
        "EXECUTION_START_DOES_NOT_MATCH_ENTRY",
    )

    with pytest.raises(ResearchContractError, match="receipt_sha256"):
        assess_interval_identity_bundle(
            {},
            charter=replace(
                binding.charter,
                roll_safety_policy_receipt_sha256="not-a-receipt",
            ),
        )


def test_post_decision_identity_failures_cannot_select_on_outcome_sign() -> None:
    binding = _interval_binding()
    windows = {window.role: window for window in binding.role_windows}
    segments = {
        role: (
            IntervalIdentitySegment(
                window.start_at,
                window.end_at,
                date(2026, 7, 14),
                "a" * 64,
                "b" * 64,
            ),
        )
        for role, window in windows.items()
    }
    pnl = windows[IntervalRole.PNL]
    split = datetime(2026, 7, 14, 20, 15, tzinfo=timezone.utc)
    changed_segments = dict(segments)
    changed_segments[IntervalRole.PNL] = (
        IntervalIdentitySegment(
            pnl.start_at, split, date(2026, 7, 14), "a" * 64, "b" * 64
        ),
        IntervalIdentitySegment(
            split, pnl.end_at, date(2026, 7, 14), "c" * 64, "b" * 64
        ),
    )
    charter = _charter(
        windows,
        decision_at=binding.decision_at,
        entry_at=binding.planned_entry_at,
    )
    changed = assess_interval_identity_bundle(changed_segments, charter=charter)
    missing_segments = dict(segments)
    missing_segments.pop(IntervalRole.RETURN)
    missing = assess_interval_identity_bundle(missing_segments, charter=charter)

    decisions = (changed, missing)
    hypothetical_signed_outcomes = (Decimal("100"), Decimal("-100"))
    denominator = coverage_denominator_indices(decisions)
    assert denominator == (0, 1)
    assert tuple(hypothetical_signed_outcomes[index] for index in denominator) == (
        Decimal("100"),
        Decimal("-100"),
    )
    assert all(
        decision.status is IntervalResolutionStatus.POST_DECISION_UNRESOLVED
        for decision in decisions
    )
    assert all(decision.binding is None for decision in decisions)
    assert changed.failure_reasons == (
        "PNL_ACTUAL_CONTRACT_CHANGED_WITHIN_INTERVAL",
    )
    assert missing.failure_reasons == (
        "RETURN_INCOMPLETE_INTERVAL_IDENTITY_COVERAGE",
    )


def test_pnl_execution_interval_must_match_identity_binding() -> None:
    binding = _interval_binding()
    execution = binding.window_for(IntervalRole.PNL)
    with pytest.raises(ResearchContractError, match="execution interval does not match"):
        build_pnl_row(
            market_id="ES",
            direction=Direction.LONG,
            session_ordinal=10,
            economics=_economics(),
            interval_identity=binding,
            execution_start_at=binding.planned_entry_at,
            execution_end_at=datetime(2026, 7, 14, 20, 30, tzinfo=timezone.utc),
            entry_price=Decimal("100.00"),
            exit_price=Decimal("100.50"),
            quantity=2,
            commission_per_contract=Decimal("1.00"),
            exchange_fees_per_contract=Decimal("0.50"),
            round_trip_slippage_ticks_per_contract=2,
        )
    assert execution.end_at == datetime(2026, 7, 14, 20, 31, tzinfo=timezone.utc)


def test_actual_contract_session_unit_eligible_or_abstains_atomically() -> None:
    unit = FuturesInferenceUnit(
        market_id="ES",
        direction=Direction.LONG,
        expected_session_ordinal=10,
        observed_session_ordinal=10,
        actual_contract_id="a" * 64,
        economics_record_id="b" * 64,
        session_complete=True,
        is_roll_session=False,
        sessions_to_expiry=5,
    )
    eligible = assess_inference_unit(
        unit, economics=_economics(), minimum_sessions_to_expiry=2
    )
    assert eligible.eligible is True
    assert eligible.abstention_reasons == ()

    unsafe = replace(
        unit,
        observed_session_ordinal=None,
        session_complete=False,
        is_roll_session=True,
        sessions_to_expiry=2,
    )
    abstained = assess_inference_unit(
        unsafe, economics=_economics(), minimum_sessions_to_expiry=2
    )
    assert abstained.eligible is False
    assert abstained.abstention_reasons == (
        "EXPIRY_GUARD",
        "MISSING_OR_INCOMPLETE_SESSION",
        "ROLL_TRANSITION",
    )


def test_inference_unit_economics_mismatch_and_bool_flags_fail_closed() -> None:
    unit = FuturesInferenceUnit(
        "ES", Direction.LONG, 10, 10, "a" * 64, "b" * 64, True, False, 5
    )
    mismatch = assess_inference_unit(
        unit, economics=_economics(actual="c" * 64), minimum_sessions_to_expiry=2
    )
    assert "ECONOMICS_ACTUAL_CONTRACT_MISMATCH" in mismatch.abstention_reasons
    with pytest.raises(ResearchContractError, match="exact bool"):
        assess_inference_unit(
            replace(unit, is_roll_session=np.bool_(False)),
            economics=_economics(),
            minimum_sessions_to_expiry=2,
        )
