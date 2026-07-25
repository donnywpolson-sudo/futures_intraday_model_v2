"""Pure planning primitives for bounded all-market history cache warmup."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping, Sequence


MAX_HISTORY_CHUNK_HOURS = 24
PLAN_EXPIRY_MINUTES = 10


@dataclass(frozen=True, order=True)
class HistoryBinding:
    market: str
    contract: str
    instrument_id: int


@dataclass(frozen=True)
class HistoryChunk:
    start: datetime
    end: datetime
    bindings: tuple[HistoryBinding, ...]

    @property
    def markets(self) -> tuple[str, ...]:
        return tuple(binding.market for binding in self.bindings)


@dataclass
class HistoryPlan:
    plan_id: str
    created_at: datetime
    expires_at: datetime
    target_start: datetime
    target_end: datetime
    estimated_cost_usd: float
    chunks: list[HistoryChunk]
    confirmed: bool = False
    paused: bool = False


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def merge_intervals(
    intervals: Sequence[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    normalized = sorted(
        (_utc(start), _utc(end)) for start, end in intervals if _utc(start) < _utc(end)
    )
    merged: list[tuple[datetime, datetime]] = []
    for start, end in normalized:
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
            continue
        merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def missing_intervals(
    *,
    start: datetime,
    end: datetime,
    coverage: Sequence[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    target_start = _utc(start)
    target_end = _utc(end)
    if target_start >= target_end:
        return []
    clipped = merge_intervals(
        [
            (max(target_start, _utc(covered_start)), min(target_end, _utc(covered_end)))
            for covered_start, covered_end in coverage
            if _utc(covered_end) > target_start and _utc(covered_start) < target_end
        ]
    )
    missing: list[tuple[datetime, datetime]] = []
    cursor = target_start
    for covered_start, covered_end in clipped:
        if covered_start > cursor:
            missing.append((cursor, covered_start))
        cursor = max(cursor, covered_end)
    if cursor < target_end:
        missing.append((cursor, target_end))
    return missing


def split_newest_first(
    intervals: Sequence[tuple[datetime, datetime]],
    *,
    max_hours: int = MAX_HISTORY_CHUNK_HOURS,
) -> list[tuple[datetime, datetime]]:
    if max_hours <= 0:
        raise ValueError("max_hours must be positive")
    width = timedelta(hours=max_hours)
    chunks: list[tuple[datetime, datetime]] = []
    for interval_start, interval_end in intervals:
        start = _utc(interval_start)
        cursor = _utc(interval_end)
        while cursor > start:
            chunk_start = max(start, cursor - width)
            chunks.append((chunk_start, cursor))
            cursor = chunk_start
    return sorted(chunks, key=lambda item: (item[1], item[0]), reverse=True)


def group_history_chunks(
    bindings: Sequence[HistoryBinding],
    missing_by_instrument: Mapping[int, Sequence[tuple[datetime, datetime]]],
) -> list[HistoryChunk]:
    grouped: dict[tuple[datetime, datetime], list[HistoryBinding]] = {}
    for binding in bindings:
        for start, end in split_newest_first(
            missing_by_instrument.get(binding.instrument_id, ())
        ):
            grouped.setdefault((start, end), []).append(binding)
    return [
        HistoryChunk(
            start=start,
            end=end,
            bindings=tuple(sorted(group, key=lambda item: item.market)),
        )
        for (start, end), group in sorted(
            grouped.items(), key=lambda item: (item[0][1], item[0][0]), reverse=True
        )
    ]


def promote_market(
    chunks: Sequence[HistoryChunk], market: str
) -> list[HistoryChunk]:
    """Move one market ahead without changing the approved request scope."""

    promoted: list[HistoryChunk] = []
    remaining: list[HistoryChunk] = []
    for chunk in chunks:
        selected = tuple(binding for binding in chunk.bindings if binding.market == market)
        others = tuple(binding for binding in chunk.bindings if binding.market != market)
        if selected:
            promoted.append(HistoryChunk(chunk.start, chunk.end, selected))
        if others:
            remaining.append(HistoryChunk(chunk.start, chunk.end, others))
    return promoted + remaining
