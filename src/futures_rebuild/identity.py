"""Actual-contract identity and causal continuous-selection ledger."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from functools import lru_cache
import re
from typing import Sequence

from .canonical import sha256_json
from .errors import ContractError
from .time_contracts import require_utc


@dataclass(frozen=True)
class ContractDefinition:
    """A bitemporal definition version, independent of any bar/session date."""

    dataset: str
    publisher_id: int
    instrument_id: int
    raw_symbol: str
    exchange: str
    definition_release_id: str
    definition_manifest_sha256: str
    definition_row_id: str
    currency: str
    multiplier: Decimal
    min_tick: Decimal

    def __post_init__(self) -> None:
        if not self.dataset or not self.raw_symbol or not self.exchange:
            raise ContractError("definition dataset, raw symbol, and exchange are required")
        if (
            isinstance(self.publisher_id, bool)
            or isinstance(self.instrument_id, bool)
            or not isinstance(self.publisher_id, int)
            or not isinstance(self.instrument_id, int)
            or self.publisher_id <= 0
            or self.instrument_id <= 0
        ):
            raise ContractError("definition publisher/instrument IDs must be positive integers")
        if not self.definition_release_id or re.fullmatch(
            r"[0-9a-f]{64}", self.definition_manifest_sha256
        ) is None or re.fullmatch(r"[0-9a-f]{64}", self.definition_row_id) is None:
            raise ContractError("verified definition release/manifest/row identity is required")
        if not self.currency or any(
            not value.is_finite() or value <= 0
            for value in (self.multiplier, self.min_tick)
        ):
            raise ContractError("definition currency/multiplier/minimum tick are invalid")

    @property
    def lookup_key(self) -> tuple[str, int, int]:
        return self.dataset, self.publisher_id, self.instrument_id

    def as_dict(self) -> dict[str, object]:
        return {
            "currency": self.currency,
            "dataset": self.dataset,
            "definition_manifest_sha256": self.definition_manifest_sha256,
            "definition_release_id": self.definition_release_id,
            "definition_row_id": self.definition_row_id,
            "exchange": self.exchange,
            "instrument_id": self.instrument_id,
            "min_tick": str(self.min_tick),
            "multiplier": str(self.multiplier),
            "publisher_id": self.publisher_id,
            "raw_symbol": self.raw_symbol,
        }


@dataclass(frozen=True)
class ActualContractIdentity:
    dataset: str
    publisher_id: int
    instrument_id: int
    instrument_id_date_utc: date
    exchange_session_date: date
    raw_symbol: str
    exchange: str
    definition_release_id: str
    definition_manifest_sha256: str
    definition_row_id: str
    currency: str
    multiplier: Decimal
    min_tick: Decimal

    def __post_init__(self) -> None:
        if not self.dataset or not self.raw_symbol or not self.exchange:
            raise ContractError("dataset, raw_symbol, and exchange are required")
        if (
            isinstance(self.publisher_id, bool)
            or isinstance(self.instrument_id, bool)
            or not isinstance(self.publisher_id, int)
            or not isinstance(self.instrument_id, int)
            or self.publisher_id <= 0
            or self.instrument_id <= 0
        ):
            raise ContractError("publisher_id and instrument_id must be positive")
        for name, value in (
            ("instrument_id_date_utc", self.instrument_id_date_utc),
            ("exchange_session_date", self.exchange_session_date),
        ):
            if not isinstance(value, date) or isinstance(value, datetime):
                raise ContractError(f"{name} must be a date")
        if not self.definition_release_id:
            raise ContractError("definition_release_id is required")
        if (
            re.fullmatch(r"[0-9a-f]{64}", self.definition_manifest_sha256) is None
            or re.fullmatch(r"[0-9a-f]{64}", self.definition_row_id) is None
        ):
            raise ContractError("verified definition manifest and row hashes are required")
        if not self.currency:
            raise ContractError("currency is required")
        if (
            not self.multiplier.is_finite()
            or not self.min_tick.is_finite()
            or self.multiplier <= 0
            or self.min_tick <= 0
        ):
            raise ContractError("multiplier and minimum tick must be positive")

    @property
    def composite_key(self) -> str:
        return (
            f"{self.dataset}|{self.publisher_id}|{self.instrument_id}|"
            f"{self.instrument_id_date_utc.isoformat()}|"
            f"{self.exchange_session_date.isoformat()}|{self.raw_symbol}|"
            f"{self.definition_release_id}"
        )

    @property
    def lookup_key(self) -> tuple[str, int, int, str]:
        return (
            self.dataset,
            self.publisher_id,
            self.instrument_id,
            self.instrument_id_date_utc.isoformat(),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "currency": self.currency,
            "dataset": self.dataset,
            "definition_manifest_sha256": self.definition_manifest_sha256,
            "definition_release_id": self.definition_release_id,
            "definition_row_id": self.definition_row_id,
            "exchange": self.exchange,
            "exchange_session_date": self.exchange_session_date.isoformat(),
            "instrument_id": self.instrument_id,
            "instrument_id_date_utc": self.instrument_id_date_utc.isoformat(),
            "min_tick": str(self.min_tick),
            "multiplier": str(self.multiplier),
            "publisher_id": self.publisher_id,
            "raw_symbol": self.raw_symbol,
        }

    @property
    def identity_hash(self) -> str:
        return _actual_contract_identity_hash(self)

    @property
    def contract_segment_key(self) -> tuple[str, int, int, str, str]:
        """Resolved actual contract, independent of UTC/session namespace dates."""

        return (
            self.dataset,
            self.publisher_id,
            self.instrument_id,
            self.raw_symbol,
            self.exchange,
        )

    @property
    def contract_segment_hash(self) -> str:
        return sha256_json(
            {
                "dataset": self.dataset,
                "exchange": self.exchange,
                "instrument_id": self.instrument_id,
                "publisher_id": self.publisher_id,
                "raw_symbol": self.raw_symbol,
            }
        )

    @classmethod
    def from_definition(
        cls,
        definition: ContractDefinition,
        *,
        instrument_id_date_utc: date,
        exchange_session_date: date,
    ) -> "ActualContractIdentity":
        return cls(
            dataset=definition.dataset,
            publisher_id=definition.publisher_id,
            instrument_id=definition.instrument_id,
            instrument_id_date_utc=instrument_id_date_utc,
            exchange_session_date=exchange_session_date,
            raw_symbol=definition.raw_symbol,
            exchange=definition.exchange,
            definition_release_id=definition.definition_release_id,
            definition_manifest_sha256=definition.definition_manifest_sha256,
            definition_row_id=definition.definition_row_id,
            currency=definition.currency,
            multiplier=definition.multiplier,
            min_tick=definition.min_tick,
        )


@lru_cache(maxsize=131_072)
def _actual_contract_identity_hash(identity: ActualContractIdentity) -> str:
    """Hash each complete immutable actual identity once per process."""

    return sha256_json(identity.as_dict())


@dataclass(frozen=True)
class DefinitionObservation:
    """One actual-contract definition version with independent provider clocks.

    ``effective_at`` is the provider's explicit instrument activation time, not
    ``ts_event``.  ``source_received_at`` is the provider index timestamp used
    for causal visibility.  The publisher event clock is retained separately
    as audit provenance and is never ordered against the receive clock.
    """

    definition: ContractDefinition
    effective_at: datetime | None
    available_at: datetime
    source_release_id: str
    source_received_at: datetime
    expires_at: datetime | None = None
    provider_event_at: datetime | None = None
    definition_index_date_utc: date | None = None
    security_update_action: str = "ADD"
    instrument_class: str = "FUTURE"
    security_type: str = "FUT"
    source_file_path: str = "LEGACY_UNSPECIFIED"
    source_row_ordinal: int = 0

    def __post_init__(self) -> None:
        available = require_utc(self.available_at, "available_at")
        if not self.source_release_id:
            raise ContractError("definition source release is required")
        if self.definition.definition_release_id != self.source_release_id:
            raise ContractError("actual identity and definition observation release disagree")
        received = require_utc(self.source_received_at, "source_received_at")
        effective = (
            None
            if self.effective_at is None
            else require_utc(self.effective_at, "effective_at")
        )
        expires = (
            None
            if self.expires_at is None
            else require_utc(self.expires_at, "expires_at")
        )
        if self.provider_event_at is not None:
            require_utc(self.provider_event_at, "provider_event_at")
        if received > available:
            raise ContractError(
                "definition received/available chronology is invalid"
            )
        if effective is not None and expires is not None and expires <= effective:
            raise ContractError("definition activation/expiration chronology is invalid")
        if self.definition_index_date_utc is not None and (
            not isinstance(self.definition_index_date_utc, date)
            or isinstance(self.definition_index_date_utc, datetime)
        ):
            raise ContractError("definition index UTC date is invalid")
        if self.security_update_action not in {"ADD", "MODIFY", "DELETE"}:
            raise ContractError("definition update action is not supported")
        if not self.instrument_class or not self.security_type:
            raise ContractError("definition instrument class/type is missing")
        if not self.source_file_path:
            raise ContractError("definition source file path is missing")
        if (
            isinstance(self.source_row_ordinal, bool)
            or not isinstance(self.source_row_ordinal, int)
            or self.source_row_ordinal < 0
        ):
            raise ContractError("definition source row ordinal is invalid")


@dataclass(frozen=True)
class RollSelectionObservation:
    continuous_symbol: str
    actual: ActualContractIdentity
    effective_at: datetime
    available_at: datetime
    selected_from_data_through: datetime
    source_release_id: str

    def __post_init__(self) -> None:
        effective = require_utc(self.effective_at, "effective_at")
        available = require_utc(self.available_at, "available_at")
        data_through = require_utc(
            self.selected_from_data_through, "selected_from_data_through"
        )
        if not self.continuous_symbol or not self.source_release_id:
            raise ContractError("continuous symbol and source release are required")
        if data_through > available:
            raise ContractError("selection cannot use data not yet available")
        # effective_at and available_at are intentionally independent: an announced
        # mapping may become known before it becomes effective.
        _ = effective


@dataclass(frozen=True)
class EligibilityObservation:
    actual: ActualContractIdentity
    effective_at: datetime
    available_at: datetime
    eligible: bool
    reasons: tuple[str, ...]
    source_release_id: str

    def __post_init__(self) -> None:
        require_utc(self.effective_at, "effective_at")
        require_utc(self.available_at, "available_at")
        if not self.source_release_id:
            raise ContractError("eligibility source release is required")
        if self.eligible and self.reasons:
            raise ContractError("eligible observations cannot contain exclusion reasons")
        if not self.eligible and not self.reasons:
            raise ContractError("ineligible observations require at least one reason")


class AsOfRollLedger:
    """Select actual contracts strictly from facts known and effective at decision time."""

    def __init__(
        self,
        selections: tuple[RollSelectionObservation, ...],
        eligibility: tuple[EligibilityObservation, ...] = (),
    ) -> None:
        seen: set[tuple[str, datetime, datetime]] = set()
        for item in selections:
            key = (item.continuous_symbol, item.effective_at, item.available_at)
            if key in seen:
                raise ContractError(f"duplicate roll observation key: {key}")
            seen.add(key)
        self._selections = tuple(selections)
        eligibility_seen: set[tuple[str, datetime, datetime]] = set()
        for item in eligibility:
            key = (item.actual.composite_key, item.effective_at, item.available_at)
            if key in eligibility_seen:
                raise ContractError("conflicting eligibility observations share one as-of key")
            eligibility_seen.add(key)
        self._eligibility = tuple(eligibility)

    def select(self, continuous_symbol: str, decision_at: datetime) -> ActualContractIdentity:
        decision = require_utc(decision_at, "decision_at")
        candidates = [
            item
            for item in self._selections
            if item.continuous_symbol == continuous_symbol
            and item.effective_at <= decision
            and item.available_at <= decision
        ]
        if not candidates:
            raise ContractError(
                f"no selection was both effective and known for {continuous_symbol}"
            )
        # Most recent effective fact wins; later receipt breaks revision ties.
        selected_key = max((item.effective_at, item.available_at) for item in candidates)
        selected = [
            item
            for item in candidates
            if (item.effective_at, item.available_at) == selected_key
        ]
        if len(selected) != 1:
            raise ContractError("roll selection is ambiguous at the decision time")
        return selected[0].actual

    def eligibility_at(
        self, actual: ActualContractIdentity, decision_at: datetime
    ) -> EligibilityObservation:
        decision = require_utc(decision_at, "decision_at")
        candidates = [
            item
            for item in self._eligibility
            if item.actual.composite_key == actual.composite_key
            and item.effective_at <= decision
            and item.available_at <= decision
        ]
        if not candidates:
            raise ContractError("no as-of eligibility evidence for selected contract")
        selected_key = max((item.effective_at, item.available_at) for item in candidates)
        selected = [
            item
            for item in candidates
            if (item.effective_at, item.available_at) == selected_key
        ]
        if len(selected) != 1:
            raise ContractError("eligibility is ambiguous at the decision time")
        return selected[0]


@dataclass(frozen=True)
class RetrospectiveMappingInterval:
    """Provider metadata mapping retained only for reconciliation evidence."""

    symbol: str
    instrument_id: int
    start_at: datetime
    end_at: datetime

    def __post_init__(self) -> None:
        start = require_utc(self.start_at, "start_at")
        end = require_utc(self.end_at, "end_at")
        if not self.symbol or self.instrument_id <= 0 or end <= start:
            raise ContractError("retrospective mapping interval is invalid")


def resolve_bar_identity(
    dataset: str,
    publisher_id: int,
    bar_instrument_id: int,
    bar_event_at: datetime,
    definitions: Sequence[DefinitionObservation],
    *,
    decision_at: datetime,
    session_policy: object,
    retrospective_mappings: Sequence[RetrospectiveMappingInterval] = (),
) -> ActualContractIdentity:
    """Resolve from the bar's instrument_id; mappings can never select eligibility.

    Metadata mapping intervals may contain future end dates and are therefore
    deliberately excluded from the causal selection computation. They remain an
    accepted argument only so callers cannot accidentally substitute them for the
    authoritative bar identity while performing later reconciliation.
    """

    _ = retrospective_mappings
    decision = require_utc(decision_at, "decision_at")
    event = require_utc(bar_event_at, "bar_event_at")
    if event > decision:
        raise ContractError("bar identity cannot be resolved before its event time")
    from .session_policy import VerifiedSessionPolicy

    if not isinstance(session_policy, VerifiedSessionPolicy):
        raise ContractError("a verified versioned session policy is required")
    instrument_id_date_utc = event.date()
    key = (
        dataset,
        publisher_id,
        bar_instrument_id,
    )
    visible = [
        observation
        for observation in definitions
        if observation.definition.lookup_key == key
        and observation.source_received_at <= decision
        and observation.available_at <= decision
        and (
            observation.definition_index_date_utc is None
            or observation.definition_index_date_utc == instrument_id_date_utc
        )
    ]
    baseline = [
        observation
        for observation in visible
        if observation.source_received_at <= event
        and observation.available_at <= event
    ]
    if not baseline:
        raise ContractError(
            f"bar instrument_id has no same-day definition available by bar start: {bar_instrument_id}"
        )

    def semantic_state(item: DefinitionObservation) -> tuple[object, ...]:
        definition = item.definition
        return (
            definition.dataset,
            definition.publisher_id,
            definition.instrument_id,
            definition.raw_symbol,
            definition.exchange,
            definition.currency,
            definition.multiplier,
            definition.min_tick,
            item.effective_at,
            item.expires_at,
            item.security_update_action,
            item.instrument_class,
            item.security_type,
        )

    latest_received = max(item.source_received_at for item in baseline)
    receive_ties = [
        item for item in baseline if item.source_received_at == latest_received
    ]
    by_file: dict[str, list[DefinitionObservation]] = {}
    for item in receive_ties:
        by_file.setdefault(item.source_file_path, []).append(item)
    sequences: dict[str, tuple[tuple[object, ...], ...]] = {}
    for source_path, items in by_file.items():
        if len({item.source_row_ordinal for item in items}) != len(items):
            raise ContractError("definition receive-time tie repeats a source row ordinal")
        items.sort(key=lambda item: item.source_row_ordinal)
        sequences[source_path] = tuple(semantic_state(item) for item in items)
    if len(set(sequences.values())) != 1:
        raise ContractError("definition receive-time tie conflicts across source files")
    selected_path = min(sequences)
    selected_observation = max(
        by_file[selected_path], key=lambda item: item.source_row_ordinal
    )

    selected_state = semantic_state(selected_observation)
    changes_during_observation = [
        item
        for item in visible
        if (
            item.source_received_at > event or item.available_at > event
        )
        and (
            item.definition_index_date_utc is not None
            or semantic_state(item) != selected_state
        )
    ]
    if changes_during_observation:
        raise ContractError("definition changed within the bar observation window")
    if (
        selected_observation.security_update_action == "DELETE"
        or selected_observation.effective_at is None
        or (
            selected_observation.expires_at is None
            and selected_observation.definition_index_date_utc is not None
        )
        or not (
            selected_observation.effective_at
            <= event
            and (
                selected_observation.expires_at is None
                or event < selected_observation.expires_at
            )
        )
        or selected_observation.instrument_class != "FUTURE"
        or selected_observation.security_type != "FUT"
    ):
        raise ContractError("definition is deleted, inactive, expired, or not an outright future")

    selected = [selected_observation]
    definition_versions = {
        (
            item.definition.definition_release_id,
            item.definition.definition_manifest_sha256,
            item.definition.definition_row_id,
            item.definition.raw_symbol,
            item.definition.exchange,
            item.definition.currency,
            item.definition.multiplier,
            item.definition.min_tick,
        )
        for item in selected
    }
    if len(definition_versions) != 1:
        raise ContractError("definition observations are ambiguous at the decision time")
    definition = selected_observation.definition
    return ActualContractIdentity.from_definition(
        definition,
        instrument_id_date_utc=instrument_id_date_utc,
        exchange_session_date=session_policy.exchange_session_date(
            definition.exchange, event
        ),
    )


def utc_iso(value: datetime) -> str:
    return require_utc(value, "timestamp").astimezone(timezone.utc).isoformat()


def assert_single_actual_contract_segment(
    identities: Sequence[ActualContractIdentity], *, purpose: str
) -> str:
    """Fail closed when a feature/label/return/P&L interval crosses a contract."""

    if not identities:
        raise ContractError(f"{purpose} interval has no resolved actual-contract identity")
    hashes = {item.contract_segment_hash for item in identities}
    if len(hashes) != 1:
        raise ContractError(f"{purpose} interval crosses an actual instrument_id boundary")
    return next(iter(hashes))
