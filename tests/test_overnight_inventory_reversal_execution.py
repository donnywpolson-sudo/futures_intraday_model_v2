from __future__ import annotations

import hashlib
import math
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.overnight_inventory_reversal_execution import (
    BASELINES,
    MARKETS,
    build_session_observations,
    evaluate_fixed_trial,
    iter_ordered_session_observations,
)
from futures_rebuild.tier1_bracket_post_audit import CausalBar
from futures_rebuild.tier1_bracket_v4 import MarketSpec
from futures_rebuild.tier1_bracket_v5 import V5SourceRecord


CHICAGO = ZoneInfo("America/Chicago")
SPEC = MarketSpec(Decimal("0.25"), Decimal("12.5"), Decimal("50"))


def _event_ns(day: date, clock: time) -> int:
    local = datetime.combine(day, clock, tzinfo=CHICAGO)
    return int(local.timestamp()) * 1_000_000_000


def _record(
    market: str, session: date, day: date, clock: time,
    open_price: Decimal, close_price: Decimal,
) -> V5SourceRecord:
    event = _event_ns(day, clock)
    low = min(open_price, close_price)
    high = max(open_price, close_price)
    identity = hashlib.sha256(f"{market}/{session}/identity".encode()).hexdigest()
    source = hashlib.sha256(f"{market}/{session}/{event}".encode()).hexdigest()
    return V5SourceRecord(
        market=market,
        exchange_session_date=session.isoformat(),
        disposition="ELIGIBLE",
        bar=CausalBar(
            event, event + 60_000_000_000, event + 65_000_000_000,
            open_price, high, low, close_price, True,
        ),
        volume=1.0,
        actual_identity_hash=identity,
        source_row_sha256=source,
        market_spec=SPEC,
    )


def _session_rows(
    market: str, session: date, *, overnight_return: float,
) -> list[V5SourceRecord]:
    prior = session - timedelta(days=1)
    first = Decimal("100")
    overnight_close = Decimal(str(100 * math.exp(overnight_return)))
    rows = [
        _record(market, session, prior, time(17), first, first),
        _record(market, session, session, time(8, 29), overnight_close, overnight_close),
    ]
    favorable_exit = Decimal("96") if overnight_return > 0 else Decimal("104")
    for minute in range(61):
        clock = (datetime.combine(session, time(8, 31)) + timedelta(minutes=minute)).time()
        price = favorable_exit if minute == 60 else Decimal("100")
        rows.append(_record(market, session, session, clock, price, price))
    return rows


def _synthetic_history() -> tuple[list[V5SourceRecord], list[str]]:
    rows: list[V5SourceRecord] = []
    sessions: list[str] = []
    cursor = date(2018, 1, 2)
    for index in range(270):
        while cursor.weekday() >= 5:
            cursor += timedelta(days=1)
        sessions.append(cursor.isoformat())
        training_return = 0.001 if index % 2 else -0.001
        value = training_return if index < 260 else (0.01 if index % 2 else -0.01)
        for market in MARKETS:
            rows.extend(_session_rows(market, cursor, overnight_return=value))
        cursor += timedelta(days=1)
    return rows, sessions


def test_fixed_fold_uses_training_scale_and_exact_stress_execution() -> None:
    rows, sessions = _synthetic_history()
    observations = build_session_observations(source_records=rows)
    result = evaluate_fixed_trial(
        observations=observations,
        outer_folds=[{
            "outer_fit_session_range": [sessions[0], sessions[259]],
            "outer_test_session_dates": [sessions[260], sessions[-1]],
        }],
    )

    assert len(result.fold_scales) == 4
    assert {item.training_sessions for item in result.fold_scales} == {260}
    assert result.incomplete_market_sessions == 0
    assert result.candidate_trade_count == 40
    assert result.complete_portfolio_sessions == tuple(sessions[260:])
    assert all(value > 0 for value in result.portfolio_net_pnl_by_session.values())
    assert set(next(iter(result.baseline_portfolio_net_pnl_by_session.values()))) == set(BASELINES)


def test_below_threshold_is_a_complete_zero_pnl_session() -> None:
    rows, sessions = _synthetic_history()
    observations = build_session_observations(source_records=rows)
    result = evaluate_fixed_trial(
        observations=observations,
        outer_folds=[{
            "outer_fit_session_range": [sessions[0], sessions[259]],
            "outer_test_session_dates": [sessions[250], sessions[259]],
        }],
    )
    assert result.candidate_trade_count == 0
    assert all(value == 0 for value in result.portfolio_net_pnl_by_session.values())


def test_2025_row_is_rejected_before_evaluation() -> None:
    row = _session_rows("ES", date(2025, 1, 2), overnight_return=0.01)[0]
    with pytest.raises(UnauthorizedOperation, match="2025 holdout row"):
        build_session_observations(source_records=[row])


def test_bounded_market_stream_matches_batch_materializer() -> None:
    rows, _ = _synthetic_history()
    market_rows = [item for item in rows if item.market == "ES"]
    expected = tuple(
        item for item in build_session_observations(source_records=market_rows)
        if item.market == "ES"
    )
    actual = tuple(
        iter_ordered_session_observations(
            market="ES", source_records=market_rows,
        )
    )
    assert actual == expected
