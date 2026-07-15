"""Causal actual-contract resolution from the bar instrument ID."""

from __future__ import annotations

from datetime import date, datetime
from types import MappingProxyType
from typing import Mapping, Protocol, Sequence

from ..errors import ContractError
from ..identity import ActualContractIdentity, ContractDefinition
from ..time_contracts import require_utc
from .economics import EconomicsRuleBook, ResolvedEconomics
from .records import ProviderBar, ProviderDefinition, datetime_to_ns


class SessionDateResolver(Protocol):
    def exchange_session_date(self, exchange: str, event_at: datetime) -> date: ...


class DefinitionSource(Protocol):
    def resolve(self, bar: ProviderBar, *, decision_at: datetime) -> ProviderDefinition: ...


class DefinitionIndex:
    """Bounded lookup by the actual bar instrument, never a mapped symbol."""

    def __init__(self, definitions: Sequence[ProviderDefinition]) -> None:
        grouped: dict[tuple[str, str, str, int, int], list[ProviderDefinition]] = {}
        for item in definitions:
            key = (
                item.source_release_id,
                item.source_manifest_sha256,
                item.dataset,
                item.publisher_id,
                item.instrument_id,
            )
            grouped.setdefault(key, []).append(item)
        normalized: dict[
            tuple[str, str, str, int, int], tuple[ProviderDefinition, ...]
        ] = {}
        for key, values in grouped.items():
            ordered = tuple(sorted(values, key=lambda item: (item.ts_event_ns, item.ts_recv_ns)))
            if len({(item.ts_event_ns, item.ts_recv_ns, item.row_sha256) for item in ordered}) != len(ordered):
                raise ContractError("definition index contains duplicate rows")
            normalized[key] = ordered
        self._definitions: Mapping[
            tuple[str, str, str, int, int], tuple[ProviderDefinition, ...]
        ] = MappingProxyType(normalized)

    def resolve(
        self, bar: ProviderBar, *, decision_at: datetime
    ) -> ProviderDefinition:
        key = (
            bar.source_release_id,
            bar.source_manifest_sha256,
            bar.dataset,
            bar.publisher_id,
            bar.instrument_id,
        )
        return _select_definition(
            bar,
            self._definitions.get(key, ()),
            decision_at=decision_at,
        )


def _select_definition(
    bar: ProviderBar,
    definitions: Sequence[ProviderDefinition],
    *,
    decision_at: datetime,
) -> ProviderDefinition:
    decision = require_utc(decision_at, "decision_at")
    decision_ns = datetime_to_ns(decision, "decision_at")
    candidates = [
        item
        for item in definitions
        if item.dataset == bar.dataset
        and item.market == bar.market
        and item.source_release_id == bar.source_release_id
        and item.source_manifest_sha256 == bar.source_manifest_sha256
        and item.publisher_id == bar.publisher_id
        and item.instrument_id == bar.instrument_id
        and item.ts_event_ns <= bar.event_at_ns
        and item.ts_recv_ns <= decision_ns
    ]
    if not candidates:
        raise ContractError(
            "bar instrument has no definition both effective at event and known at decision"
        )
    selected_key = max((item.ts_event_ns, item.ts_recv_ns) for item in candidates)
    selected = [
        item
        for item in candidates
        if (item.ts_event_ns, item.ts_recv_ns) == selected_key
    ]
    if len(selected) != 1:
        raise ContractError("bar definition is ambiguous at the decision time")
    return selected[0]


def definition_as_of(
    bar: ProviderBar,
    definitions: Sequence[ProviderDefinition],
    *,
    decision_at: datetime,
) -> ProviderDefinition:
    return _select_definition(bar, definitions, decision_at=decision_at)


def actual_identity_as_of(
    bar: ProviderBar,
    definitions: Sequence[ProviderDefinition] | DefinitionSource,
    *,
    decision_at: datetime,
    session_policy: SessionDateResolver,
    economics_rules: EconomicsRuleBook,
) -> tuple[ActualContractIdentity, ProviderDefinition, ResolvedEconomics]:
    selected = (
        definitions.resolve(bar, decision_at=decision_at)
        if isinstance(definitions, DefinitionIndex)
        else definition_as_of(bar, definitions, decision_at=decision_at)
    )
    economics = economics_rules.resolve(bar.market, selected)
    definition = ContractDefinition(
        dataset=selected.dataset,
        publisher_id=selected.publisher_id,
        instrument_id=selected.instrument_id,
        raw_symbol=selected.raw_symbol,
        exchange=selected.exchange,
        definition_release_id=selected.source_release_id,
        definition_manifest_sha256=selected.source_manifest_sha256,
        definition_row_id=selected.row_sha256,
        currency=selected.currency,
        multiplier=economics.point_value,
        min_tick=economics.tick_size,
    )
    session_date = session_policy.exchange_session_date(
        selected.exchange, bar.event_at
    )
    if not isinstance(session_date, date) or isinstance(session_date, datetime):
        raise ContractError("verified session policy returned a non-date")
    actual = ActualContractIdentity.from_definition(
        definition,
        instrument_id_date_utc=bar.event_at.date(),
        exchange_session_date=session_date,
    )
    if actual.instrument_id != bar.instrument_id:
        raise ContractError("actual identity is not the bar instrument_id")
    return actual, selected, economics
