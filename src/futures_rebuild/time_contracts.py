"""Causal timestamp contracts for completed-bar decisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import re

from .errors import ContractError


class AvailabilityBasis(str, Enum):
    PROVIDER_TS_RECV = "PROVIDER_TS_RECV"
    MODELED_INTERVAL_END_PLUS_PINNED_LATENCY = (
        "MODELED_INTERVAL_END_PLUS_PINNED_LATENCY"
    )
    DERIVED_FROM_VERIFIED_UPSTREAM = "DERIVED_FROM_VERIFIED_UPSTREAM"


def require_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ContractError(f"{name} must be timezone-aware UTC")
    normalized = value.astimezone(timezone.utc)
    if value.utcoffset() != timedelta(0):
        raise ContractError(f"{name} must be encoded in UTC, not only convertible to UTC")
    return normalized


@dataclass(frozen=True)
class CausalTimestamp:
    event_at: datetime
    available_at: datetime
    decision_at: datetime
    source_received_at: datetime | None = None

    def __post_init__(self) -> None:
        event = require_utc(self.event_at, "event_at")
        available = require_utc(self.available_at, "available_at")
        decision = require_utc(self.decision_at, "decision_at")
        if available < event:
            raise ContractError("available_at cannot precede event_at")
        if decision < available:
            raise ContractError("decision_at cannot precede available_at")
        if self.source_received_at is not None:
            received = require_utc(self.source_received_at, "source_received_at")
            if received < event or available < received:
                raise ContractError(
                    "source_received_at must be between event_at and available_at"
                )


@dataclass(frozen=True)
class BarObservation:
    bar_start: datetime
    interval: timedelta
    available_at: datetime
    decision_at: datetime
    planned_entry_at: datetime
    availability_basis: AvailabilityBasis
    availability_policy_hash: str
    source_release_retrieved_at: datetime
    source_received_at: datetime | None = None
    modeled_publication_latency: timedelta | None = None

    def __post_init__(self) -> None:
        start = require_utc(self.bar_start, "bar_start")
        available = require_utc(self.available_at, "available_at")
        decision = require_utc(self.decision_at, "decision_at")
        entry = require_utc(self.planned_entry_at, "planned_entry_at")
        retrieved = require_utc(
            self.source_release_retrieved_at, "source_release_retrieved_at"
        )
        if self.interval <= timedelta(0):
            raise ContractError("bar interval must be positive")
        if available < start + self.interval:
            raise ContractError("bar cannot be available before its interval completes")
        if decision < available:
            raise ContractError("decision cannot precede completed-bar availability")
        if entry <= decision:
            raise ContractError("entry must be strictly after the decision timestamp")
        if not isinstance(self.availability_basis, AvailabilityBasis) or re.fullmatch(
            r"[0-9a-f]{64}", self.availability_policy_hash
        ) is None:
            raise ContractError("bar availability basis and policy hash are required")
        if retrieved < available:
            raise ContractError("archive/source release cannot be retrieved before availability")
        if self.availability_basis is AvailabilityBasis.PROVIDER_TS_RECV:
            raise ContractError(
                "Databento OHLCV bars have no ts_recv; provider receipt time cannot be invented"
            )
        elif self.availability_basis is AvailabilityBasis.MODELED_INTERVAL_END_PLUS_PINNED_LATENCY:
            if self.source_received_at is not None or self.modeled_publication_latency is None:
                raise ContractError("modeled OHLCV availability cannot invent provider ts_recv")
            if self.modeled_publication_latency < timedelta(0):
                raise ContractError("modeled publication latency cannot be negative")
            if available != start + self.interval + self.modeled_publication_latency:
                raise ContractError("modeled availability differs from interval end plus latency")
        else:
            raise ContractError("raw bars require provider or modeled availability basis")

    @property
    def bar_end(self) -> datetime:
        return self.bar_start + self.interval
