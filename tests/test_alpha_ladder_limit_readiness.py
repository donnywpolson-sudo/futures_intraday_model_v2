from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pytest

from futures_rebuild.alpha_ladder_limit_readiness import (
    CT,
    LimitBar,
    _direction_path,
    _fold_evidence,
    classify_session,
)


COSTS = {"base": 2, "stress": 4, "extreme": 8}


def _bar(
    minute: int, *, low: str = "99", high: str = "101", close: str = "100",
    identity: str = "a" * 64,
) -> LimitBar:
    event = datetime.combine(date(2020, 1, 2), time(9, 30), CT) + timedelta(minutes=minute)
    return LimitBar(
        event_at=event, available_at=event + timedelta(seconds=65), identity=identity,
        open=Decimal(close), high=Decimal(high), low=Decimal(low), close=Decimal(close),
        volume=Decimal("100"), tick_size=Decimal("0.25"), tick_value=Decimal("12.5"),
    )


def _complete_bars() -> list[LimitBar]:
    return [_bar(minute) for minute in range(0, 81)]


def test_one_tick_penetration_is_required_not_touch() -> None:
    bars = _complete_bars()
    feature = tuple(bars[8:29])
    trigger = bars[30]
    touched = [
        replace(bar, low=Decimal("100"), high=Decimal("100"))
        if 32 <= index <= 36 else bar
        for index, bar in enumerate(bars)
    ]
    filled, complete, disposition = _direction_path(
        bars=touched, trigger=trigger, feature=feature, direction="LONG",
        scenario="stress", adverse_ticks=4,
    )
    assert (filled, complete) == (False, True)
    assert disposition.endswith("EXPLICIT_CANCELLED_NO_TRADE_TIMEOUT")
    penetrated = list(touched)
    penetrated[32] = replace(penetrated[32], low=Decimal("99.75"))
    filled, complete, disposition = _direction_path(
        bars=penetrated, trigger=trigger, feature=feature, direction="LONG",
        scenario="stress", adverse_ticks=4,
    )
    assert (filled, complete) == (True, True)
    assert disposition.endswith("VERIFIED_LIMIT_EXIT")


def test_short_fill_also_requires_one_tick_penetration() -> None:
    bars = _complete_bars()
    feature = tuple(bars[8:29])
    trigger = bars[30]
    touched = [
        replace(bar, low=Decimal("100"), high=Decimal("100"))
        if 32 <= index <= 36 else bar
        for index, bar in enumerate(bars)
    ]
    assert _direction_path(
        bars=touched, trigger=trigger, feature=feature, direction="SHORT",
        scenario="stress", adverse_ticks=4,
    )[:2] == (False, True)
    touched[32] = replace(touched[32], high=Decimal("100.25"))
    assert _direction_path(
        bars=touched, trigger=trigger, feature=feature, direction="SHORT",
        scenario="stress", adverse_ticks=4,
    )[:2] == (True, True)


def test_filled_entry_without_verified_exit_fails_closed() -> None:
    bars = _complete_bars()
    feature = tuple(bars[8:29])
    trigger = bars[30]
    constrained = [
        replace(bar, low=Decimal("98"), high=Decimal("99"), close=Decimal("98.5"))
        if index >= 64 else bar
        for index, bar in enumerate(bars)
    ]
    filled, complete, disposition = _direction_path(
        bars=constrained, trigger=trigger, feature=feature, direction="LONG",
        scenario="stress", adverse_ticks=4,
    )
    assert filled is True
    assert complete is False
    assert disposition.endswith("VERIFIED_EXIT_MISSING")


def test_identity_change_after_fill_fails_closed() -> None:
    bars = _complete_bars()
    bars[40] = replace(bars[40], identity="b" * 64)
    result = _direction_path(
        bars=bars, trigger=bars[30], feature=tuple(bars[8:29]), direction="LONG",
        scenario="stress", adverse_ticks=4,
    )
    assert result[0] is True and result[1] is False
    assert result[2].endswith("HOLD_IDENTITY_CHANGING")


def test_feature_gap_is_an_explicit_checkpoint_abstention() -> None:
    result = classify_session(session="2020-01-02", bars=_complete_bars()[20:], cost_ticks=COSTS)
    assert result.feature_complete is False
    assert result.selected is False
    assert result.path_complete is True
    assert result.dispositions == ("EXPLICIT_CAUSAL_FEATURE_ABSTENTION",)


def test_complete_session_checks_both_directions_and_all_scenarios() -> None:
    result = classify_session(session="2020-01-02", bars=_complete_bars(), cost_ticks=COSTS)
    assert result.feature_complete is True
    assert result.path_complete is True
    assert any(item.startswith("LONG__stress") for item in result.dispositions)
    assert any(item.startswith("SHORT__stress") for item in result.dispositions)
    assert result.scenario_risk == {
        "base": "FEASIBLE", "stress": "FEASIBLE", "extreme": "RISK_ABSTENTION",
    }


def test_fold_accounts_for_every_checkpoint_and_baselines_are_independent() -> None:
    fold = {
        "fold_id": "fold-0", "training_sessions": ["2020-01-02"],
        "evaluation_sessions": ["2020-01-02"], "purge_minutes": 40,
        "embargo_sessions": ["2020-01-01"],
    }
    evidence = _fold_evidence(
        market="ES", fold=fold,
        rows_by_session={"2020-01-02": tuple(_complete_bars()), "__cost_ticks__": COSTS},
        risk_by_session={},
    )
    assert evidence["counts"]["terminal_evaluation_sessions"] == 1
    assert evidence["counts"]["expected_evaluation_sessions"] == 1
    baselines = evidence["baseline_universe_readiness"]
    assert baselines["flat_no_trade"]["selected_sessions"] == 0
    for name, baseline in baselines.items():
        assert baseline["schedule_independently_derived"] is True
        assert baseline["candidate_schedule_reused"] is False
        if name != "flat_no_trade":
            assert baseline["readiness_universe"] == (
                "INDEPENDENT_BOTH_DIRECTION_ALL_SCENARIO_CHECKPOINT_SUPERSET"
            )


def test_same_bar_entry_and_stop_uses_conservative_stop_precedence() -> None:
    bars = _complete_bars()
    bars[32] = replace(bars[32], low=Decimal("96"))
    filled, complete, disposition = _direction_path(
        bars=bars, trigger=bars[30], feature=tuple(bars[8:29]), direction="LONG",
        scenario="stress", adverse_ticks=4,
    )
    assert (filled, complete) == (True, True)
    assert disposition.endswith("VERIFIED_PROTECTIVE_STOP")
