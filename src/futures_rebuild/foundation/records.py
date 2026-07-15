"""Exact provider record contracts; no fitting or global transformation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import PurePosixPath
from typing import Mapping

from ..errors import ContractError
from ..time_contracts import require_utc


NANO = Decimal("1000000000")
INT64_NULL = 2**63 - 1
INT32_NULL = 2**31 - 1
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def exact_int(value: object, name: str, *, nonnegative: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(f"{name} must be an exact integer")
    if nonnegative and value < 0:
        raise ContractError(f"{name} cannot be negative")
    return value


def nanounits(value: int, name: str, *, positive: bool = False) -> Decimal:
    raw = exact_int(value, name)
    if raw in {INT64_NULL, INT32_NULL} or (positive and raw <= 0):
        raise ContractError(f"{name} is null, sentinel, or nonpositive")
    result = Decimal(raw) / NANO
    if not result.is_finite():
        raise ContractError(f"{name} is nonfinite")
    return result


def datetime_to_ns(value: datetime, name: str) -> int:
    """Convert an aware UTC datetime without a floating-point timestamp."""

    utc = require_utc(value, name)
    delta = utc - EPOCH_UTC
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def ns_to_datetime(value: int, name: str) -> datetime:
    """Expose a UTC datetime while retaining exact nanoseconds on the record."""

    raw = exact_int(value, name, nonnegative=True)
    seconds, nanoseconds = divmod(raw, 1_000_000_000)
    try:
        return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(
            microsecond=nanoseconds // 1_000
        )
    except (OverflowError, OSError, ValueError) as exc:
        raise ContractError(f"{name} is outside the supported UTC range") from exc


@dataclass(frozen=True)
class ProviderDefinition:
    dataset: str
    market: str
    publisher_id: int
    instrument_id: int
    ts_event_ns: int
    ts_recv_ns: int
    raw_symbol: str
    exchange: str
    currency: str
    min_price_increment_nano: int
    unit_of_measure_qty_nano: int
    unit_of_measure: str
    source_release_id: str
    source_manifest_sha256: str
    source_file_path: str
    source_file_sha256: str
    row_sha256: str

    def __post_init__(self) -> None:
        exact_int(self.ts_event_ns, "definition.ts_event_ns", nonnegative=True)
        exact_int(self.ts_recv_ns, "definition.ts_recv_ns", nonnegative=True)
        if self.ts_recv_ns < self.ts_event_ns:
            raise ContractError("definition receive time precedes effective time")
        for name, value in (
            ("publisher_id", self.publisher_id),
            ("instrument_id", self.instrument_id),
            ("min_price_increment_nano", self.min_price_increment_nano),
            ("unit_of_measure_qty_nano", self.unit_of_measure_qty_nano),
        ):
            exact_int(value, name)
        if self.publisher_id <= 0 or self.instrument_id <= 0:
            raise ContractError("definition IDs must be positive")
        if (
            self.dataset != "GLBX.MDP3"
            or not self.market
            or not self.raw_symbol
            or not self.exchange
            or self.currency != "USD"
            or not self.unit_of_measure
        ):
            raise ContractError("definition identity/unit fields are invalid")
        for digest in (
            self.source_release_id,
            self.source_manifest_sha256,
            self.source_file_sha256,
            self.row_sha256,
        ):
            if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise ContractError("definition release/row hashes are invalid")
        relative = PurePosixPath(self.source_file_path)
        if (
            not self.source_file_path
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != self.source_file_path
        ):
            raise ContractError("definition source file path is invalid")

    @property
    def ts_event(self) -> datetime:
        return ns_to_datetime(self.ts_event_ns, "definition.ts_event_ns")

    @property
    def ts_recv(self) -> datetime:
        return ns_to_datetime(self.ts_recv_ns, "definition.ts_recv_ns")

    @property
    def min_tick(self) -> Decimal:
        return nanounits(
            self.min_price_increment_nano,
            "definition.min_price_increment",
            positive=True,
        )

    @property
    def observed_unit_qty(self) -> Decimal | None:
        if self.unit_of_measure_qty_nano in {0, INT64_NULL, INT32_NULL}:
            return None
        return nanounits(
            self.unit_of_measure_qty_nano,
            "definition.unit_of_measure_qty",
            positive=True,
        )


@dataclass(frozen=True)
class ProviderBar:
    dataset: str
    market: str
    publisher_id: int
    instrument_id: int
    event_at_ns: int
    open_nano: int
    high_nano: int
    low_nano: int
    close_nano: int
    volume: int
    source_release_id: str
    source_manifest_sha256: str
    source_file_path: str
    source_file_sha256: str
    row_sha256: str

    def __post_init__(self) -> None:
        exact_int(self.event_at_ns, "bar.event_at_ns", nonnegative=True)
        if self.dataset != "GLBX.MDP3" or not self.market:
            raise ContractError("bar dataset/market is invalid")
        for name, value in (
            ("publisher_id", self.publisher_id),
            ("instrument_id", self.instrument_id),
            ("volume", self.volume),
        ):
            exact_int(value, name, nonnegative=name == "volume")
        if self.publisher_id <= 0 or self.instrument_id <= 0:
            raise ContractError("bar IDs must be positive")
        prices = tuple(
            nanounits(value, f"bar.{name}", positive=True)
            for name, value in (
                ("open", self.open_nano),
                ("high", self.high_nano),
                ("low", self.low_nano),
                ("close", self.close_nano),
            )
        )
        opening, high, low, closing = prices
        if high < max(opening, closing) or low > min(opening, closing) or high < low:
            raise ContractError("bar OHLC ordering is invalid")
        for digest in (
            self.source_release_id,
            self.source_manifest_sha256,
            self.source_file_sha256,
            self.row_sha256,
        ):
            if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise ContractError("bar release/row hashes are invalid")
        relative = PurePosixPath(self.source_file_path)
        if (
            not self.source_file_path
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != self.source_file_path
        ):
            raise ContractError("bar source file path is invalid")

    @property
    def event_at(self) -> datetime:
        return ns_to_datetime(self.event_at_ns, "bar.event_at_ns")

    @property
    def prices(self) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        return tuple(
            nanounits(value, f"bar.{name}", positive=True)
            for name, value in (
                ("open", self.open_nano),
                ("high", self.high_nano),
                ("low", self.low_nano),
                ("close", self.close_nano),
            )
        )  # type: ignore[return-value]


def _validate_market_state_lineage(
    *,
    dataset: str,
    market: str,
    publisher_id: int,
    instrument_id: int,
    instrument_id_date_utc: str,
    source_release_id: str,
    source_manifest_sha256: str,
    source_file_path: str,
    source_file_sha256: str,
    row_ordinal: int,
    row_sha256: str,
) -> None:
    if dataset != "GLBX.MDP3" or not market:
        raise ContractError("market-state dataset/market is invalid")
    for name, value in (
        ("publisher_id", publisher_id),
        ("instrument_id", instrument_id),
    ):
        exact_int(value, name)
        if value <= 0:
            raise ContractError("market-state identity IDs must be positive")
    try:
        parsed_date = date.fromisoformat(instrument_id_date_utc)
    except (TypeError, ValueError) as exc:
        raise ContractError("market-state UTC identity date is invalid") from exc
    if parsed_date.isoformat() != instrument_id_date_utc:
        raise ContractError("market-state UTC identity date is not canonical")
    exact_int(row_ordinal, "row_ordinal", nonnegative=True)
    for digest in (
        source_release_id,
        source_manifest_sha256,
        source_file_sha256,
        row_sha256,
    ):
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise ContractError("market-state lineage hash is invalid")
    relative = PurePosixPath(source_file_path)
    if (
        not source_file_path
        or relative.is_absolute()
        or ".." in relative.parts
        or relative.as_posix() != source_file_path
    ):
        raise ContractError("market-state source file path is invalid")


@dataclass(frozen=True)
class StatusRecordV1:
    """Exact as-received Databento instrument-status record.

    All provider state is preserved as a named value.  Eligibility is a
    separate deterministic decision and is never inferred while decoding.
    """

    dataset: str
    market: str
    publisher_id: int
    instrument_id: int
    instrument_id_date_utc: str
    ts_event_ns: int
    ts_recv_ns: int
    action: str
    reason: str
    trading_event: str
    is_trading: str
    is_quoting: str
    is_short_sell_restricted: str
    source_release_id: str
    source_manifest_sha256: str
    source_file_path: str
    source_file_sha256: str
    row_ordinal: int
    row_sha256: str

    def __post_init__(self) -> None:
        _validate_market_state_lineage(
            dataset=self.dataset,
            market=self.market,
            publisher_id=self.publisher_id,
            instrument_id=self.instrument_id,
            instrument_id_date_utc=self.instrument_id_date_utc,
            source_release_id=self.source_release_id,
            source_manifest_sha256=self.source_manifest_sha256,
            source_file_path=self.source_file_path,
            source_file_sha256=self.source_file_sha256,
            row_ordinal=self.row_ordinal,
            row_sha256=self.row_sha256,
        )
        exact_int(self.ts_event_ns, "status.ts_event_ns", nonnegative=True)
        exact_int(self.ts_recv_ns, "status.ts_recv_ns", nonnegative=True)
        if self.ts_recv_ns < self.ts_event_ns:
            raise ContractError("status receive time precedes event time")
        for name, value in (
            ("action", self.action),
            ("reason", self.reason),
            ("trading_event", self.trading_event),
            ("is_trading", self.is_trading),
            ("is_quoting", self.is_quoting),
            ("is_short_sell_restricted", self.is_short_sell_restricted),
        ):
            if type(value) is not str or not value:
                raise ContractError(f"status {name} is invalid")
        if self.instrument_id_date_utc != ns_to_datetime(
            self.ts_event_ns, "status.ts_event_ns"
        ).date().isoformat():
            raise ContractError("status identity date differs from ts_event UTC date")

    @property
    def record_id(self) -> str:
        return self.row_sha256

    def as_dict(self) -> dict[str, object]:
        return {
            "action": self.action,
            "dataset": self.dataset,
            "instrument_id": self.instrument_id,
            "instrument_id_date_utc": self.instrument_id_date_utc,
            "is_quoting": self.is_quoting,
            "is_short_sell_restricted": self.is_short_sell_restricted,
            "is_trading": self.is_trading,
            "market": self.market,
            "publisher_id": self.publisher_id,
            "reason": self.reason,
            "row_ordinal": self.row_ordinal,
            "row_sha256": self.row_sha256,
            "source_file_path": self.source_file_path,
            "source_file_sha256": self.source_file_sha256,
            "source_manifest_sha256": self.source_manifest_sha256,
            "source_release_id": self.source_release_id,
            "trading_event": self.trading_event,
            "ts_event_ns": self.ts_event_ns,
            "ts_recv_ns": self.ts_recv_ns,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "StatusRecordV1":
        expected = set(cls.__dataclass_fields__)
        if not isinstance(payload, Mapping) or set(payload) != expected:
            raise ContractError("status record JSON schema is invalid")
        try:
            record = cls(**dict(payload))  # type: ignore[arg-type]
        except TypeError as exc:
            raise ContractError("status record JSON types are invalid") from exc
        if record.as_dict() != dict(payload):
            raise ContractError("status record JSON is not canonical")
        return record


@dataclass(frozen=True)
class StatisticsRecordV1:
    """Exact as-received Databento statistics NEW/DELETE record."""

    dataset: str
    market: str
    publisher_id: int
    instrument_id: int
    instrument_id_date_utc: str
    ts_event_ns: int
    ts_recv_ns: int
    ts_ref_ns: int
    ts_in_delta: int
    stat_type: str
    update_action: str
    price_nano: int
    quantity: int
    sequence: int
    channel_id: int
    flags: int
    source_release_id: str
    source_manifest_sha256: str
    source_file_path: str
    source_file_sha256: str
    row_ordinal: int
    row_sha256: str

    def __post_init__(self) -> None:
        _validate_market_state_lineage(
            dataset=self.dataset,
            market=self.market,
            publisher_id=self.publisher_id,
            instrument_id=self.instrument_id,
            instrument_id_date_utc=self.instrument_id_date_utc,
            source_release_id=self.source_release_id,
            source_manifest_sha256=self.source_manifest_sha256,
            source_file_path=self.source_file_path,
            source_file_sha256=self.source_file_sha256,
            row_ordinal=self.row_ordinal,
            row_sha256=self.row_sha256,
        )
        for name, value, nonnegative in (
            ("ts_event_ns", self.ts_event_ns, True),
            ("ts_recv_ns", self.ts_recv_ns, True),
            ("ts_ref_ns", self.ts_ref_ns, True),
            ("ts_in_delta", self.ts_in_delta, False),
            ("price_nano", self.price_nano, False),
            ("quantity", self.quantity, True),
            ("sequence", self.sequence, True),
            ("channel_id", self.channel_id, True),
            ("flags", self.flags, True),
        ):
            exact_int(value, f"statistics.{name}", nonnegative=nonnegative)
        if self.ts_recv_ns < self.ts_event_ns:
            raise ContractError("statistics receive time precedes event time")
        if type(self.stat_type) is not str or not self.stat_type:
            raise ContractError("statistics type is invalid")
        if type(self.update_action) is not str or not self.update_action:
            raise ContractError("statistics update action is invalid")
        if self.instrument_id_date_utc != ns_to_datetime(
            self.ts_event_ns, "statistics.ts_event_ns"
        ).date().isoformat():
            raise ContractError("statistics identity date differs from ts_event UTC date")

    @property
    def record_id(self) -> str:
        return self.row_sha256

    def as_dict(self) -> dict[str, object]:
        return {
            "channel_id": self.channel_id,
            "dataset": self.dataset,
            "flags": self.flags,
            "instrument_id": self.instrument_id,
            "instrument_id_date_utc": self.instrument_id_date_utc,
            "market": self.market,
            "price_nano": self.price_nano,
            "publisher_id": self.publisher_id,
            "quantity": self.quantity,
            "row_ordinal": self.row_ordinal,
            "row_sha256": self.row_sha256,
            "sequence": self.sequence,
            "source_file_path": self.source_file_path,
            "source_file_sha256": self.source_file_sha256,
            "source_manifest_sha256": self.source_manifest_sha256,
            "source_release_id": self.source_release_id,
            "stat_type": self.stat_type,
            "ts_event_ns": self.ts_event_ns,
            "ts_in_delta": self.ts_in_delta,
            "ts_recv_ns": self.ts_recv_ns,
            "ts_ref_ns": self.ts_ref_ns,
            "update_action": self.update_action,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "StatisticsRecordV1":
        expected = set(cls.__dataclass_fields__)
        if not isinstance(payload, Mapping) or set(payload) != expected:
            raise ContractError("statistics record JSON schema is invalid")
        try:
            record = cls(**dict(payload))  # type: ignore[arg-type]
        except TypeError as exc:
            raise ContractError("statistics record JSON types are invalid") from exc
        if record.as_dict() != dict(payload):
            raise ContractError("statistics record JSON is not canonical")
        return record
