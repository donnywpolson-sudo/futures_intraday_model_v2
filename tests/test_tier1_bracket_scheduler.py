from decimal import Decimal

from futures_rebuild.tier1_bracket_scheduler import BracketScheduleCandidate, candidates_from_directional_rows, schedule_bracket_candidates


def _candidate(*, market: str, entry: int, exit_at: int, pnl: str, session: str = "2021-01-04", score: float = 0.8) -> BracketScheduleCandidate:
    return BracketScheduleCandidate(market, session, entry, exit_at, "long", score, Decimal("250"), Decimal(pnl))


def test_bracket_scheduler_keeps_one_position_and_selects_strongest_simultaneous_signal() -> None:
    result = schedule_bracket_candidates(candidates=(
        _candidate(market="ES", entry=10, exit_at=20, pnl="1", score=0.5),
        _candidate(market="CL", entry=10, exit_at=20, pnl="1", score=0.8),
        _candidate(market="ZN", entry=15, exit_at=25, pnl="1"),
    ))
    assert [item.market for item in result.admitted] == ["CL"]
    assert result.simultaneous_abstentions == 1
    assert result.overlap_abstentions == 1


def test_bracket_scheduler_locks_out_after_daily_stop_and_entry_cap() -> None:
    daily = schedule_bracket_candidates(candidates=(
        _candidate(market="ES", entry=10, exit_at=20, pnl="-510"),
        _candidate(market="CL", entry=21, exit_at=30, pnl="1"),
    ))
    assert len(daily.admitted) == 1
    assert daily.daily_stop_abstentions == 1

    cap = schedule_bracket_candidates(candidates=tuple(
        _candidate(market="ES", entry=index * 20, exit_at=index * 20 + 10, pnl="1")
        for index in range(4)
    ))
    assert len(cap.admitted) == 3
    assert cap.entry_cap_abstentions == 1


def test_bracket_scheduler_locks_out_after_internal_drawdown() -> None:
    result = schedule_bracket_candidates(candidates=(
        _candidate(market="ES", entry=10, exit_at=20, pnl="-1501"),
        _candidate(market="CL", entry=21, exit_at=30, pnl="1", session="2021-01-05"),
    ))
    assert len(result.admitted) == 1
    assert result.drawdown_stop_abstentions == 1


def test_frozen_direction_selects_only_its_matching_bracket_label() -> None:
    source = "a" * 64
    rows = candidates_from_directional_rows(
        prediction_rows=({"upstream_source_row_sha256": source, "selected_direction": "short", "bounded_signal": -0.8, "market": "ES", "exchange_session_date": "2021-01-04", "entry_at_ns": 10},),
        outcome_rows=({"upstream_source_row_sha256": source, "long_planned_all_in_risk_usd": "100", "long_realized_net_r": "2", "long_exit_at_ns": 20, "short_planned_all_in_risk_usd": "200", "short_realized_net_r": "-1", "short_exit_at_ns": 30},),
    )
    assert rows[0].direction == "short"
    assert rows[0].planned_all_in_risk_usd == Decimal("200")
    assert rows[0].realized_net_pnl_usd == Decimal("-200")
