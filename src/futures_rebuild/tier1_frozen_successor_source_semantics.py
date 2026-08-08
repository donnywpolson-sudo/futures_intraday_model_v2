"""Causal reported-bar semantics for the unversioned frozen successor.

Databento OHLCV rows are reported trade bars, not a guaranteed wall-clock
grid.  This module permits sparse *reported* bars for causal features and
post-entry price paths without forward-filling any price or manufacturing any
fill.  Entry remains exact and observed; liquidation requires a later observed
executable bar.  Missing and non-qualified rows remain fail-closed.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
import math
from statistics import fmean, pstdev
from typing import Sequence

from .errors import IntegrityError
from .tier1_bracket_post_audit import CausalBar
from .tier1_bracket_v4 import CHICAGO, FEATURE_NAMES
from .tier1_bracket_v5 import NS_PER_MINUTE, V5SourceRecord


REQUIRED_FEATURE_BARS = 61
MAXIMUM_FEATURE_SPAN_MINUTES = 75
MAXIMUM_LATEST_BAR_STALENESS_MINUTES = 5
MAXIMUM_HOLD_MINUTES = 60
MAXIMUM_LIQUIDATION_DELAY_MINUTES = 5
ENTRY_DELAY_MINUTES = 1


@dataclass(frozen=True)
class ReportedFeatureWindow:
    rows: tuple[V5SourceRecord, ...]
    latest_bar_staleness_ns: int
    elapsed_span_ns: int
    observed_bar_density: float
    missing_clock_minutes: int


@dataclass(frozen=True)
class ReportedExecutionPath:
    entry_bar: CausalBar
    bars: tuple[CausalBar, ...]
    timeout_at_ns: int
    liquidation_at_ns: int
    missing_clock_minutes: int


@dataclass(frozen=True)
class ReportedFeatureValues:
    values: dict[str, float]
    atr: Decimal


def _validated_session_rows(
    *, source_rows: Sequence[V5SourceRecord], market: str,
    exchange_session_date: str,
) -> tuple[V5SourceRecord, ...]:
    rows = tuple(source_rows)
    for row in rows:
        row.validate()
        if row.market != market or row.exchange_session_date != exchange_session_date:
            raise IntegrityError("reported-bar selector received a foreign source row")
    event_rows = [row for row in rows if row.bar is not None]
    events = [row.bar.event_at_ns for row in event_rows if row.bar is not None]
    if len(events) != len(set(events)):
        raise IntegrityError("reported-bar source contains an ambiguous event timestamp")
    return tuple(sorted(
        event_rows, key=lambda row: row.bar.event_at_ns if row.bar is not None else -1,
    ))


def select_reported_feature_window(
    *, source_rows: Sequence[V5SourceRecord], market: str,
    exchange_session_date: str, decision_at_ns: int,
) -> ReportedFeatureWindow:
    """Select 61 causal reported bars without inventing missing clock bars."""

    if type(decision_at_ns) is not int:
        raise IntegrityError("reported feature decision time is invalid")
    rows = _validated_session_rows(
        source_rows=source_rows, market=market,
        exchange_session_date=exchange_session_date,
    )
    eligible = [
        row for row in rows
        if row.executable and row.bar is not None
        and row.bar.available_at_ns <= decision_at_ns
    ]
    if len(eligible) < REQUIRED_FEATURE_BARS:
        raise IntegrityError("insufficient causal reported feature bars")
    latest = eligible[-1]
    assert latest.bar is not None
    identity = latest.actual_identity_hash
    same_identity = [row for row in eligible if row.actual_identity_hash == identity]
    if len(same_identity) < REQUIRED_FEATURE_BARS:
        raise IntegrityError("reported feature history crosses an identity boundary")
    selected = tuple(same_identity[-REQUIRED_FEATURE_BARS:])
    assert selected[0].bar is not None and selected[-1].bar is not None
    first_event = selected[0].bar.event_at_ns
    last_event = selected[-1].bar.event_at_ns
    intervening = [
        row for row in rows
        if row.bar is not None and first_event <= row.bar.event_at_ns <= last_event
    ]
    if any(not row.executable or row.actual_identity_hash != identity for row in intervening):
        raise IntegrityError("reported feature span contains a non-qualified or foreign identity row")
    if any(
        selected[index].bar is None or selected[index - 1].bar is None
        or selected[index].bar.event_at_ns <= selected[index - 1].bar.event_at_ns
        for index in range(1, len(selected))
    ):
        raise IntegrityError("reported feature bars are not strictly ordered")
    span = last_event - first_event
    if span < (REQUIRED_FEATURE_BARS - 1) * NS_PER_MINUTE:
        raise IntegrityError("reported feature bars are more frequent than one-minute schema")
    if span > MAXIMUM_FEATURE_SPAN_MINUTES * NS_PER_MINUTE:
        raise IntegrityError("reported feature span is too sparse")
    latest_staleness = decision_at_ns - latest.bar.bar_end_at_ns
    if not 0 <= latest_staleness <= MAXIMUM_LATEST_BAR_STALENESS_MINUTES * NS_PER_MINUTE:
        raise IntegrityError("latest reported feature bar is stale")
    clock_slots = span // NS_PER_MINUTE + 1
    if span % NS_PER_MINUTE or clock_slots < REQUIRED_FEATURE_BARS:
        raise IntegrityError("reported feature timestamps are off the one-minute grid")
    return ReportedFeatureWindow(
        selected, latest_staleness, span,
        REQUIRED_FEATURE_BARS / int(clock_slots),
        int(clock_slots) - REQUIRED_FEATURE_BARS,
    )


def compute_reported_feature_values(
    *, window: ReportedFeatureWindow, decision_at_ns: int,
) -> ReportedFeatureValues:
    """Compute the frozen features by reported-bar ordinal, never by filling gaps."""

    rows = window.rows
    if len(rows) != REQUIRED_FEATURE_BARS or type(decision_at_ns) is not int:
        raise IntegrityError("reported feature window is invalid")
    if any(
        row.bar is None or not row.executable
        or row.bar.available_at_ns > decision_at_ns
        for row in rows
    ):
        raise IntegrityError("reported feature computation received an ineligible bar")
    bars = tuple(row.bar for row in rows if row.bar is not None)
    if len(bars) != REQUIRED_FEATURE_BARS:
        raise IntegrityError("reported feature computation lost a source bar")
    closes = [float(bar.close_price) for bar in bars]
    volumes = [float(row.volume) for row in rows[-60:] if row.volume is not None]
    if len(volumes) != 60:
        raise IntegrityError("reported feature volume history is incomplete")
    volume_std = pstdev(volumes)
    if volume_std == 0:
        raise IntegrityError("reported feature volume history has zero variance")
    true_ranges: list[Decimal] = []
    for index in range(1, len(bars)):
        current, prior = bars[index], bars[index - 1]
        true_ranges.append(max(
            current.high_price - current.low_price,
            abs(current.high_price - prior.close_price),
            abs(current.low_price - prior.close_price),
        ))
    atr = sum(true_ranges[:20], Decimal("0")) / Decimal("20")
    for value in true_ranges[20:]:
        atr = (Decimal("19") * atr + value) / Decimal("20")
    if atr <= 0:
        raise IntegrityError("reported feature ATR is non-positive")
    log_returns = [
        math.log(closes[index] / closes[index - 1])
        for index in range(len(closes) - 20, len(closes))
    ]
    current = bars[-1]
    local = datetime.fromtimestamp(
        current.event_at_ns / 1_000_000_000, tz=timezone.utc,
    ).astimezone(CHICAGO)
    roll_date = local.date() if local.time() >= time(17, 0) else local.date() - timedelta(days=1)
    session_minute = int((
        local - datetime.combine(roll_date, time(17, 0), tzinfo=CHICAGO)
    ).total_seconds() // 60)
    if session_minute not in range(1440):
        raise IntegrityError("reported feature time lies outside the exchange session")
    angle = 2.0 * math.pi * session_minute / 1440.0
    current_range = current.high_price - current.low_price
    values = {
        "bar_return_1": closes[-1] / closes[-2] - 1.0,
        "return_5": closes[-1] / closes[-6] - 1.0,
        "return_20": closes[-1] / closes[-21] - 1.0,
        "intrabar_range_fraction": float(current_range / current.open_price),
        "atr_20_fraction": float(atr / current.close_price),
        "range_to_atr_20": float(current_range / atr),
        "realized_volatility_20": pstdev(log_returns),
        "log1p_volume": math.log1p(float(rows[-1].volume)),
        "volume_zscore_60": (float(rows[-1].volume) - fmean(volumes)) / volume_std,
        "session_minute_sin": math.sin(angle),
        "session_minute_cos": math.cos(angle),
    }
    if tuple(values) != FEATURE_NAMES or any(not math.isfinite(value) for value in values.values()):
        raise IntegrityError("reported feature vector is invalid")
    return ReportedFeatureValues(values, atr)


def select_reported_execution_path(
    *, source_rows: Sequence[V5SourceRecord], market: str,
    exchange_session_date: str, decision_at_ns: int,
) -> ReportedExecutionPath:
    """Require an exact observed entry and a bounded observed liquidation."""

    if type(decision_at_ns) is not int:
        raise IntegrityError("reported execution decision time is invalid")
    rows = _validated_session_rows(
        source_rows=source_rows, market=market,
        exchange_session_date=exchange_session_date,
    )
    entry_at = decision_at_ns + ENTRY_DELAY_MINUTES * NS_PER_MINUTE
    timeout_at = entry_at + MAXIMUM_HOLD_MINUTES * NS_PER_MINUTE
    liquidation_deadline = timeout_at + MAXIMUM_LIQUIDATION_DELAY_MINUTES * NS_PER_MINUTE
    entry_matches = [
        row for row in rows
        if row.bar is not None and row.bar.event_at_ns == entry_at
    ]
    if len(entry_matches) != 1 or not entry_matches[0].executable:
        raise IntegrityError("exact reported entry bar is absent or non-executable")
    entry = entry_matches[0]
    assert entry.bar is not None
    identity = entry.actual_identity_hash
    scoped = [
        row for row in rows
        if row.bar is not None
        and entry_at <= row.bar.event_at_ns <= liquidation_deadline
    ]
    if any(not row.executable or row.actual_identity_hash != identity for row in scoped):
        raise IntegrityError("reported execution span contains a non-qualified or foreign identity row")
    executable = [row for row in scoped if row.executable and row.bar is not None]
    liquidations = [
        row for row in executable
        if row.bar is not None and row.bar.event_at_ns >= timeout_at
    ]
    if not liquidations:
        raise IntegrityError("no observed executable liquidation exists within the delay limit")
    liquidation = liquidations[0]
    assert liquidation.bar is not None
    selected = tuple(
        row.bar for row in executable
        if row.bar is not None and row.bar.event_at_ns <= liquidation.bar.event_at_ns
    )
    if not selected or selected[0] is not entry.bar or selected[-1] is not liquidation.bar:
        raise IntegrityError("reported execution path endpoints are inconsistent")
    clock_slots = (
        (liquidation.bar.event_at_ns - entry_at) // NS_PER_MINUTE + 1
    )
    if any(
        selected[index].event_at_ns <= selected[index - 1].event_at_ns
        or (selected[index].event_at_ns - selected[index - 1].event_at_ns) % NS_PER_MINUTE
        for index in range(1, len(selected))
    ):
        raise IntegrityError("reported execution timestamps are off the one-minute grid")
    return ReportedExecutionPath(
        entry.bar, selected, timeout_at, liquidation.bar.event_at_ns,
        int(clock_slots) - len(selected),
    )
