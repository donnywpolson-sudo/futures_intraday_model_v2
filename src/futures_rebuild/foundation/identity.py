"""Causal actual-contract resolution from the bar instrument ID."""

from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache
from itertools import groupby
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
    """Replay as-received definitions for one daily instrument identity.

    Databento instrument IDs are only unique within one UTC index date.  The
    provider index timestamp (``ts_recv``) is the knowledge/order clock;
    ``ts_event`` is retained for audit and is never used as a causal gate.
    """

    def __init__(self, definitions: Sequence[ProviderDefinition]) -> None:
        grouped: dict[
            tuple[str, str, str, str, int, int, str],
            list[ProviderDefinition],
        ] = {}
        for item in definitions:
            key = (
                item.source_release_id,
                item.source_manifest_sha256,
                item.dataset,
                item.market,
                item.publisher_id,
                item.instrument_id,
                item.instrument_id_date_utc,
            )
            grouped.setdefault(key, []).append(item)
        normalized: dict[
            tuple[str, str, str, str, int, int, str],
            tuple[ProviderDefinition, ...],
        ] = {}
        conflicts: dict[
            tuple[str, str, str, str, int, int, str], str
        ] = {}
        for key, values in grouped.items():
            try:
                normalized[key] = _normalize_source_order(values)
            except ContractError as exc:
                conflicts[key] = str(exc)
                normalized[key] = ()
        self._definitions: Mapping[
            tuple[str, str, str, str, int, int, str],
            tuple[ProviderDefinition, ...],
        ] = MappingProxyType(normalized)
        self._conflicts: Mapping[
            tuple[str, str, str, str, int, int, str], str
        ] = MappingProxyType(conflicts)

    def resolve(
        self, bar: ProviderBar, *, decision_at: datetime
    ) -> ProviderDefinition:
        key = (
            bar.source_release_id,
            bar.source_manifest_sha256,
            bar.dataset,
            bar.market,
            bar.publisher_id,
            bar.instrument_id,
            bar.event_at.date().isoformat(),
        )
        conflict = self._conflicts.get(key)
        if conflict is not None:
            raise ContractError(f"definition index conflict: {conflict}")
        return _replay_definition(
            bar,
            self._definitions.get(key, ()),
            decision_at=decision_at,
        )


def _semantic_signature(item: ProviderDefinition) -> tuple[object, ...]:
    """Fields that can change identity, lifecycle, or contract economics."""

    return (
        item.activation_ns,
        item.expiration_ns,
        item.security_update_action,
        item.instrument_class,
        item.security_type,
        item.raw_symbol,
        item.exchange,
        item.currency,
        item.min_price_increment_nano,
        item.unit_of_measure_qty_nano,
        item.unit_of_measure,
    )


def _normalize_source_order(
    definitions: Sequence[ProviderDefinition],
) -> tuple[ProviderDefinition, ...]:
    """Create an unambiguous total order without inventing cross-file order."""

    source_positions = [
        (item.source_file_path, item.row_ordinal) for item in definitions
    ]
    if len(set(source_positions)) != len(source_positions):
        raise ContractError("definition index contains duplicate source positions")

    ordered = sorted(
        definitions,
        key=lambda item: (
            item.ts_recv_ns,
            item.source_file_path,
            item.row_ordinal,
            item.row_sha256,
        ),
    )
    result: list[ProviderDefinition] = []
    for _, receive_group_iter in groupby(ordered, key=lambda item: item.ts_recv_ns):
        receive_group = tuple(receive_group_iter)
        by_file: dict[str, list[ProviderDefinition]] = {}
        for item in receive_group:
            by_file.setdefault(item.source_file_path, []).append(item)
        sequences: dict[str, tuple[tuple[object, ...], ...]] = {}
        for source_path, values in by_file.items():
            values.sort(key=lambda item: item.row_ordinal)
            sequences[source_path] = tuple(_semantic_signature(item) for item in values)
        if len(set(sequences.values())) != 1:
            raise ContractError(
                "definition index has a conflicting equal-receive cross-file order"
            )
        # Identical duplicate source sequences are semantically interchangeable.
        # Choose one path deterministically so replay never counts them twice.
        selected_path = min(sequences)
        result.extend(by_file[selected_path])
    return tuple(result)


def _validate_active_definition(
    selected: ProviderDefinition, *, event_at_ns: int
) -> None:
    if selected.security_update_action not in {"ADD", "MODIFY"}:
        raise ContractError("definition replay did not end in an active definition")
    if selected.instrument_class != "FUTURE" or selected.security_type != "FUT":
        raise ContractError("definition is not an outright futures instrument")
    activation = selected.activation
    expiration = selected.expiration
    if activation is None or expiration is None:
        raise ContractError("definition activation or expiration is undefined")
    if not selected.activation_ns < selected.expiration_ns:
        raise ContractError("definition activation/expiration interval is invalid")
    if not selected.activation_ns <= event_at_ns < selected.expiration_ns:
        raise ContractError("definition lifecycle does not cover the bar event")


def _replay_definition(
    bar: ProviderBar,
    definitions: Sequence[ProviderDefinition],
    *,
    decision_at: datetime,
) -> ProviderDefinition:
    decision = require_utc(decision_at, "decision_at")
    decision_ns = datetime_to_ns(decision, "decision_at")
    if decision_ns < bar.event_at_ns:
        raise ContractError("definition decision precedes the bar event")

    baseline = [item for item in definitions if item.ts_recv_ns <= bar.event_at_ns]
    if not baseline:
        raise ContractError(
            "bar instrument has no same-day definition received by bar start"
        )

    selected: ProviderDefinition | None = None
    for item in baseline:
        action = item.security_update_action
        if action in {"ADD", "MODIFY"}:
            selected = item
        elif action == "DELETE":
            selected = None
        else:
            raise ContractError("definition replay contains an unknown update action")

    if selected is None:
        raise ContractError("bar instrument definition is deleted at bar start")
    _validate_active_definition(selected, event_at_ns=bar.event_at_ns)

    intrabar = [
        item
        for item in definitions
        if bar.event_at_ns < item.ts_recv_ns <= decision_ns
    ]
    if intrabar:
        raise ContractError(
            "critical definition update became known after bar start and by decision"
        )
    return selected


def _select_definition(
    bar: ProviderBar,
    definitions: Sequence[ProviderDefinition],
    *,
    decision_at: datetime,
) -> ProviderDefinition:
    return DefinitionIndex(definitions).resolve(bar, decision_at=decision_at)


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
    definition = _contract_definition(selected, economics)
    session_date = session_policy.exchange_session_date(
        selected.exchange, bar.event_at
    )
    if not isinstance(session_date, date) or isinstance(session_date, datetime):
        raise ContractError("verified session policy returned a non-date")
    actual = _actual_contract_identity(
        definition,
        bar.event_at.date(),
        session_date,
    )
    if actual.instrument_id != bar.instrument_id:
        raise ContractError("actual identity is not the bar instrument_id")
    return actual, selected, economics


@lru_cache(maxsize=65_536)
def _contract_definition(
    selected: ProviderDefinition,
    economics: ResolvedEconomics,
) -> ContractDefinition:
    return ContractDefinition(
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


@lru_cache(maxsize=131_072)
def _actual_contract_identity(
    definition: ContractDefinition,
    instrument_id_date_utc: date,
    exchange_session_date: date,
) -> ActualContractIdentity:
    return ActualContractIdentity.from_definition(
        definition,
        instrument_id_date_utc=instrument_id_date_utc,
        exchange_session_date=exchange_session_date,
    )
