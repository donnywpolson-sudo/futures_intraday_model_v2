"""Offline, lane-scoped Apex micro DBN decoding into inactive Parquet staging.

This module is intentionally provider-free.  It accepts only an exact local
DBN/sidecar binding after the separately authorized executor has verified the
complete source set.  It never publishes, activates a catalog, evaluates a
strategy, or reads a holdout/forward file.
"""

from __future__ import annotations

import hashlib
import stat
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Final, Iterable, Mapping

import databento
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .canonical import sha256_file, sha256_json
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation.decoder import (
    DEFINITION_DTYPE_V1,
    DEFINITION_DTYPE_V3,
    OHLCV_DTYPE,
    STATISTICS_DTYPE,
    STATUS_DTYPE,
    SUPPORTED_DATABENTO_VERSION,
)
from .foundation.records import INT32_NULL, INT64_NULL, UINT64_NULL
from .micro_alpha_pipeline import DATASET, LANE_ID, SCHEMAS, TIER_1_MARKETS


MAX_BATCH_ROWS: Final = 100_000
PINNED_PUBLICATION_LATENCY_NS: Final = 5_000_000_000
INTERVAL_NS: Final = {"ohlcv-1m": 60_000_000_000, "ohlcv-1s": 1_000_000_000}
AVAILABILITY_BASIS: Final = "MODELED_INTERVAL_END_PLUS_PINNED_5_SECOND_LATENCY"

_COMMON_LINEAGE = [
    pa.field("source_file_sha256", pa.string(), nullable=False),
    pa.field("row_ordinal", pa.uint64(), nullable=False),
    pa.field("row_sha256", pa.string(), nullable=False),
]


class CreatedByteBudget:
    """Thread-safe, write-time ceiling shared by every inactive Parquet sink."""

    def __init__(self, maximum_bytes: int) -> None:
        if isinstance(maximum_bytes, bool) or not isinstance(maximum_bytes, int) or maximum_bytes <= 0:
            raise ContractError("created-byte ceiling is invalid")
        self.maximum_bytes = maximum_bytes
        self._used = 0
        self._lock = threading.Lock()

    @property
    def used(self) -> int:
        with self._lock:
            return self._used

    def reserve(self, count: int) -> None:
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ContractError("created-byte reservation is invalid")
        with self._lock:
            if self._used + count > self.maximum_bytes:
                raise UnauthorizedOperation("inactive Parquet byte ceiling reached")
            self._used += count


class _BoundedCreateOnlySink:
    def __init__(self, path: Path, budget: CreatedByteBudget) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._stream = path.open("xb")
        self._budget = budget

    @property
    def closed(self) -> bool:
        return self._stream.closed

    def writable(self) -> bool:
        return True

    def write(self, value: bytes) -> int:
        self._budget.reserve(len(value))
        return self._stream.write(value)

    def tell(self) -> int:
        return self._stream.tell()

    def flush(self) -> None:
        self._stream.flush()

    def close(self) -> None:
        self._stream.close()

DEFINITION_SCHEMA: Final = pa.schema(
    [
        pa.field("publisher_id", pa.uint16(), nullable=False),
        pa.field("instrument_id", pa.uint32(), nullable=False),
        pa.field("ts_event_ns", pa.uint64(), nullable=False),
        pa.field("ts_recv_ns", pa.uint64(), nullable=False),
        pa.field("activation_ns", pa.uint64()),
        pa.field("expiration_ns", pa.uint64()),
        pa.field("security_update_action_raw", pa.binary(), nullable=False),
        pa.field("instrument_class_raw", pa.binary(), nullable=False),
        pa.field("security_type", pa.string(), nullable=False),
        pa.field("raw_symbol", pa.string(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("min_price_increment_nano", pa.int64()),
        pa.field("unit_of_measure_qty_nano", pa.int64()),
        pa.field("unit_of_measure", pa.string(), nullable=False),
        *_COMMON_LINEAGE,
    ],
    metadata={b"schema_id": b"APEX_MICRO_PHASE1B_DEFINITION_V1"},
)

BAR_SCHEMA: Final = pa.schema(
    [
        pa.field("publisher_id", pa.uint16(), nullable=False),
        pa.field("instrument_id", pa.uint32(), nullable=False),
        pa.field("event_at_ns", pa.uint64(), nullable=False),
        pa.field("available_at_ns", pa.uint64(), nullable=False),
        pa.field("open_nano", pa.int64()),
        pa.field("high_nano", pa.int64()),
        pa.field("low_nano", pa.int64()),
        pa.field("close_nano", pa.int64()),
        pa.field("volume", pa.uint64()),
        pa.field("availability_basis", pa.string(), nullable=False),
        *_COMMON_LINEAGE,
    ],
    metadata={b"schema_id": b"APEX_MICRO_PHASE1B_REPORTED_BAR_V1"},
)

STATUS_SCHEMA: Final = pa.schema(
    [
        pa.field("publisher_id", pa.uint16(), nullable=False),
        pa.field("instrument_id", pa.uint32(), nullable=False),
        pa.field("ts_event_ns", pa.uint64(), nullable=False),
        pa.field("ts_recv_ns", pa.uint64(), nullable=False),
        pa.field("action_code", pa.uint16(), nullable=False),
        pa.field("reason_code", pa.uint16(), nullable=False),
        pa.field("trading_event_code", pa.uint16(), nullable=False),
        pa.field("is_trading_raw", pa.binary(), nullable=False),
        pa.field("is_quoting_raw", pa.binary(), nullable=False),
        pa.field("is_short_sell_restricted_raw", pa.binary(), nullable=False),
        *_COMMON_LINEAGE,
    ],
    metadata={b"schema_id": b"APEX_MICRO_PHASE1B_STATUS_V1"},
)

STATISTICS_SCHEMA: Final = pa.schema(
    [
        pa.field("publisher_id", pa.uint16(), nullable=False),
        pa.field("instrument_id", pa.uint32(), nullable=False),
        pa.field("ts_event_ns", pa.uint64(), nullable=False),
        pa.field("ts_recv_ns", pa.uint64(), nullable=False),
        pa.field("ts_ref_ns", pa.uint64()),
        pa.field("price_nano", pa.int64()),
        pa.field("quantity", pa.int64()),
        pa.field("sequence", pa.uint32(), nullable=False),
        pa.field("ts_in_delta", pa.int32(), nullable=False),
        pa.field("stat_type_code", pa.uint16(), nullable=False),
        pa.field("channel_id", pa.uint16(), nullable=False),
        pa.field("update_action_code", pa.uint16(), nullable=False),
        pa.field("flags", pa.uint16(), nullable=False),
        *_COMMON_LINEAGE,
    ],
    metadata={b"schema_id": b"APEX_MICRO_PHASE1B_STATISTICS_V1"},
)

CAUSAL_SCHEMA: Final = pa.schema(
    [
        *BAR_SCHEMA,
        pa.field("causal_disposition", pa.string(), nullable=False),
        pa.field("feature_eligible", pa.bool_(), nullable=False),
    ],
    metadata={b"schema_id": b"APEX_MICRO_PHASE2_CAUSAL_1M_V1"},
)


@dataclass(frozen=True)
class DecodeResult:
    """Internal decode result; raw identities are never serialized to reports."""

    schema: str
    row_count: int
    output_path: str | None
    output_sha256: str | None
    output_bytes: int
    duplicate_count: int
    ambiguous_identity_count: int
    null_field_count: int
    roll_transition_count: int
    non_contiguous_instrument_count: int
    roll_sequence: tuple[int, ...]
    instrument_ids: tuple[int, ...]
    economics: tuple[tuple[int, int | None, int | None, str], ...]

    def public_record(self) -> dict[str, object]:
        core = {
            "schema": self.schema,
            "row_count": self.row_count,
            "output_created": self.output_path is not None,
            "output_path": self.output_path,
            "output_sha256": self.output_sha256,
            "output_bytes": self.output_bytes,
            "duplicate_count": self.duplicate_count,
            "ambiguous_identity_count": self.ambiguous_identity_count,
            "null_field_count": self.null_field_count,
            "roll_transition_count": self.roll_transition_count,
            "non_contiguous_instrument_count": self.non_contiguous_instrument_count,
            "roll_sequence_length": len(self.roll_sequence),
            "roll_sequence_sha256": sha256_json(list(self.roll_sequence)),
            "instrument_identity_count": len(self.instrument_ids),
            "instrument_identity_set_sha256": sha256_json(list(self.instrument_ids)),
            "raw_values_reported": False,
        }
        return {**core, "decode_record_id": sha256_json(core)}


@dataclass(frozen=True)
class CausalResult:
    row_count: int
    feature_eligible_rows: int
    explicit_null_rows: int
    output_path: str
    output_sha256: str
    output_bytes: int

    def public_record(self) -> dict[str, object]:
        core = {
            "row_count": self.row_count,
            "feature_eligible_rows": self.feature_eligible_rows,
            "explicit_null_rows": self.explicit_null_rows,
            "output_path": self.output_path,
            "output_sha256": self.output_sha256,
            "output_bytes": self.output_bytes,
            "learned_or_outcome_informed_transform_count": 0,
            "raw_values_reported": False,
        }
        return {**core, "causal_record_id": sha256_json(core)}


def _int(value: object, name: str) -> int:
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, bool) or not isinstance(value, int):
        raise IntegrityError(f"{name} is not an exact integer")
    return value


def _positive_id(value: object, name: str) -> int:
    result = _int(value, name)
    if result <= 0:
        raise IntegrityError(f"{name} is not positive")
    return result


def _timestamp(value: object, name: str) -> int:
    result = _int(value, name)
    if result in {0, UINT64_NULL} or result < 0 or result >= 253_402_300_800_000_000_000:
        raise IntegrityError(f"{name} is undefined or outside UTC bounds")
    return result


def _nullable_i64(value: object) -> int | None:
    result = _int(value, "nullable int64")
    return None if result in {INT64_NULL, INT32_NULL} else result


def _nullable_u64(value: object) -> int | None:
    result = _int(value, "nullable uint64")
    return None if result == UINT64_NULL else result


def _bytes(value: object, name: str, *, exact_one: bool = False) -> bytes:
    if isinstance(value, np.bytes_):
        value = bytes(value)
    if not isinstance(value, bytes) or (exact_one and len(value) != 1):
        raise IntegrityError(f"{name} is not exact encoded bytes")
    return value


def _text(value: object, name: str) -> str:
    raw = _bytes(value, name).rstrip(b"\x00")
    try:
        result = raw.decode("ascii", errors="strict")
    except UnicodeDecodeError as exc:
        raise IntegrityError(f"{name} is not strict ASCII") from exc
    if not result:
        raise IntegrityError(f"{name} is empty")
    return result


def _date_boundary_ns(value: str) -> int:
    parsed = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = parsed - epoch
    return delta.days * 86_400_000_000_000


def _schema_for(source_schema: str) -> pa.Schema:
    return {
        "definition": DEFINITION_SCHEMA,
        "status": STATUS_SCHEMA,
        "statistics": STATISTICS_SCHEMA,
        "ohlcv-1m": BAR_SCHEMA,
        "ohlcv-1s": BAR_SCHEMA,
    }[source_schema]


def _dtype_for(source_schema: str) -> set[tuple[str, ...]]:
    return {
        "definition": {DEFINITION_DTYPE_V1, DEFINITION_DTYPE_V3},
        "status": {STATUS_DTYPE},
        "statistics": {STATISTICS_DTYPE},
        "ohlcv-1m": {OHLCV_DTYPE},
        "ohlcv-1s": {OHLCV_DTYPE},
    }[source_schema]


def _validate_query(
    *, metadata: object, query: Mapping[str, object], market: str, source_schema: str
) -> None:
    required = {
        "compression",
        "dataset",
        "encoding",
        "end",
        "schema",
        "start",
        "stype_in",
        "stype_out",
        "symbols",
    }
    expected_stype = "parent" if source_schema == "definition" else "continuous"
    expected_symbol = f"{market}.FUT" if source_schema == "definition" else f"{market}.v.0"
    if (
        set(query) != required
        or query.get("compression") != "zstd"
        or query.get("encoding") != "dbn"
        or query.get("dataset") != DATASET
        or query.get("schema") != source_schema
        or query.get("stype_in") != expected_stype
        or query.get("stype_out") != "instrument_id"
        or query.get("symbols") != [expected_symbol]
    ):
        raise IntegrityError("micro DBN query contract drifted")
    try:
        observed = {
            "dataset": getattr(metadata, "dataset"),
            "schema": str(getattr(metadata, "schema")),
            "stype_in": str(getattr(metadata, "stype_in")),
            "stype_out": str(getattr(metadata, "stype_out")),
            "symbols": list(getattr(metadata, "symbols")),
            "start": getattr(metadata, "start"),
            "end": getattr(metadata, "end"),
            "ts_out": getattr(metadata, "ts_out"),
            "limit": getattr(metadata, "limit"),
        }
    except (AttributeError, TypeError) as exc:
        raise IntegrityError("micro DBN metadata is incomplete") from exc
    if (
        observed["dataset"] != DATASET
        or observed["schema"] != source_schema
        or observed["stype_in"] != expected_stype
        or observed["stype_out"] != "instrument_id"
        or observed["symbols"] != [expected_symbol]
        or observed["start"] != _date_boundary_ns(str(query["start"]))
        or observed["end"] != _date_boundary_ns(str(query["end"]))
        or observed["ts_out"] is not False
        or observed["limit"] is not None
    ):
        raise IntegrityError("micro DBN metadata differs from its sidecar")


def _row_hash(source_sha256: str, ordinal: int, row: np.void) -> str:
    raw_sha = hashlib.sha256(row.tobytes()).hexdigest()
    return sha256_json(
        {"source_file_sha256": source_sha256, "row_ordinal": ordinal, "raw_record_sha256": raw_sha}
    )


def _definition_record(row: np.void, *, source_sha: str, ordinal: int) -> tuple[dict[str, object], tuple[int, int | None, int | None, str]]:
    instrument_id = _positive_id(row["instrument_id"], "definition.instrument_id")
    tick = _nullable_i64(row["min_price_increment"])
    quantity = _nullable_i64(row["unit_of_measure_qty"])
    currency = _text(row["currency"], "definition.currency")
    result = {
        "publisher_id": _positive_id(row["publisher_id"], "definition.publisher_id"),
        "instrument_id": instrument_id,
        "ts_event_ns": _timestamp(row["ts_event"], "definition.ts_event"),
        "ts_recv_ns": _timestamp(row["ts_recv"], "definition.ts_recv"),
        "activation_ns": _nullable_u64(row["activation"]),
        "expiration_ns": _nullable_u64(row["expiration"]),
        "security_update_action_raw": _bytes(row["security_update_action"], "security_update_action", exact_one=True),
        "instrument_class_raw": _bytes(row["instrument_class"], "instrument_class", exact_one=True),
        "security_type": _text(row["security_type"], "security_type"),
        "raw_symbol": _text(row["raw_symbol"], "raw_symbol"),
        "exchange": _text(row["exchange"], "exchange"),
        "currency": currency,
        "min_price_increment_nano": tick,
        "unit_of_measure_qty_nano": quantity,
        "unit_of_measure": _text(row["unit_of_measure"], "unit_of_measure"),
        "source_file_sha256": source_sha,
        "row_ordinal": ordinal,
        "row_sha256": _row_hash(source_sha, ordinal, row),
    }
    return result, (instrument_id, tick, quantity, currency)


def _bar_record(
    row: np.void, *, source_sha: str, ordinal: int, source_schema: str
) -> tuple[dict[str, object], int]:
    event = _timestamp(row["ts_event"], "bar.ts_event")
    prices = [_nullable_i64(row[name]) for name in ("open", "high", "low", "close")]
    volume = _nullable_u64(row["volume"])
    nonnull = [value for value in prices if value is not None]
    if len(nonnull) == 4:
        opening, high, low, closing = nonnull
        if high < max(opening, closing) or low > min(opening, closing) or high < low:
            raise IntegrityError("micro bar OHLC ordering is invalid")
    available = event + INTERVAL_NS[source_schema] + PINNED_PUBLICATION_LATENCY_NS
    result = {
        "publisher_id": _positive_id(row["publisher_id"], "bar.publisher_id"),
        "instrument_id": _positive_id(row["instrument_id"], "bar.instrument_id"),
        "event_at_ns": event,
        "available_at_ns": available,
        "open_nano": prices[0],
        "high_nano": prices[1],
        "low_nano": prices[2],
        "close_nano": prices[3],
        "volume": volume,
        "availability_basis": AVAILABILITY_BASIS,
        "source_file_sha256": source_sha,
        "row_ordinal": ordinal,
        "row_sha256": _row_hash(source_sha, ordinal, row),
    }
    return result, sum(value is None for value in (*prices, volume))


def _status_record(row: np.void, *, source_sha: str, ordinal: int) -> dict[str, object]:
    return {
        "publisher_id": _positive_id(row["publisher_id"], "status.publisher_id"),
        "instrument_id": _positive_id(row["instrument_id"], "status.instrument_id"),
        "ts_event_ns": _timestamp(row["ts_event"], "status.ts_event"),
        "ts_recv_ns": _timestamp(row["ts_recv"], "status.ts_recv"),
        "action_code": _int(row["action"], "status.action"),
        "reason_code": _int(row["reason"], "status.reason"),
        "trading_event_code": _int(row["trading_event"], "status.trading_event"),
        "is_trading_raw": _bytes(row["is_trading"], "status.is_trading"),
        "is_quoting_raw": _bytes(row["is_quoting"], "status.is_quoting"),
        "is_short_sell_restricted_raw": _bytes(row["is_short_sell_restricted"], "status.is_short_sell_restricted"),
        "source_file_sha256": source_sha,
        "row_ordinal": ordinal,
        "row_sha256": _row_hash(source_sha, ordinal, row),
    }


def _statistics_record(row: np.void, *, source_sha: str, ordinal: int) -> tuple[dict[str, object], int]:
    price = _nullable_i64(row["price"])
    quantity = _nullable_i64(row["quantity"])
    ts_ref = _nullable_u64(row["ts_ref"])
    return {
        "publisher_id": _positive_id(row["publisher_id"], "statistics.publisher_id"),
        "instrument_id": _positive_id(row["instrument_id"], "statistics.instrument_id"),
        "ts_event_ns": _timestamp(row["ts_event"], "statistics.ts_event"),
        "ts_recv_ns": _timestamp(row["ts_recv"], "statistics.ts_recv"),
        "ts_ref_ns": ts_ref,
        "price_nano": price,
        "quantity": quantity,
        "sequence": _int(row["sequence"], "statistics.sequence"),
        "ts_in_delta": _int(row["ts_in_delta"], "statistics.ts_in_delta"),
        "stat_type_code": _int(row["stat_type"], "statistics.stat_type"),
        "channel_id": _int(row["channel_id"], "statistics.channel_id"),
        "update_action_code": _int(row["update_action"], "statistics.update_action"),
        "flags": _int(row["stat_flags"], "statistics.flags"),
        "source_file_sha256": source_sha,
        "row_ordinal": ordinal,
        "row_sha256": _row_hash(source_sha, ordinal, row),
    }, sum(value is None for value in (price, quantity, ts_ref))


def _table(records: list[dict[str, object]], schema: pa.Schema, *, source_schema: str, source_sha: str) -> pa.Table:
    enriched = schema.with_metadata(
        {
            **(schema.metadata or {}),
            b"lane_id": LANE_ID.encode("ascii"),
            b"source_schema": source_schema.encode("ascii"),
            b"source_file_sha256": source_sha.encode("ascii"),
            b"availability_policy": AVAILABILITY_BASIS.encode("ascii"),
        }
    )
    return pa.Table.from_pylist(records, schema=enriched)


def decode_dbn_to_inactive_parquet(
    *,
    source_path: Path,
    output_path: Path,
    market: str,
    source_schema: str,
    exact_query: Mapping[str, object],
    expected_source_sha256: str,
    batch_rows: int = MAX_BATCH_ROWS,
    created_byte_budget: CreatedByteBudget | None = None,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> DecodeResult:
    """Decode one approved pre-2025 DBN into one inactive staged Parquet."""

    if market not in TIER_1_MARKETS or source_schema not in SCHEMAS:
        raise ContractError("micro decode selector is outside Tier 0/1")
    try:
        year = int(source_path.parent.name)
    except ValueError as exc:
        raise UnauthorizedOperation("micro source year is not explicit") from exc
    if year < 2018 or year > 2024:
        raise UnauthorizedOperation("sealed holdout or forward DBN cannot be opened")
    if batch_rows != MAX_BATCH_ROWS:
        raise ContractError("micro decode batch size is not the frozen bound")
    if output_path.exists() or output_path.with_suffix(output_path.suffix + ".partial").exists():
        raise IntegrityError("micro staged output already exists")
    if databento.__version__ != SUPPORTED_DATABENTO_VERSION:
        raise ContractError("micro decoding requires the pinned offline Databento SDK")
    if sha256_file(source_path) != expected_source_sha256:
        raise IntegrityError("micro DBN hash differs before decode")
    store = databento.DBNStore.from_file(source_path)
    _validate_query(metadata=store.metadata, query=exact_query, market=market, source_schema=source_schema)
    decoded = store.to_ndarray(count=batch_rows)
    chunks: Iterable[np.ndarray] = (decoded,) if isinstance(decoded, np.ndarray) else decoded
    expected_dtypes = _dtype_for(source_schema)
    writer: pq.ParquetWriter | None = None
    sink: _BoundedCreateOnlySink | None = None
    partial = output_path.with_suffix(output_path.suffix + ".partial")
    row_count = 0
    duplicate_count = 0
    ambiguous_identity_count = 0
    null_field_count = 0
    roll_transition_count = 0
    non_contiguous_instruments: set[int] = set()
    roll_sequence: list[int] = []
    instruments: set[int] = set()
    economics: set[tuple[int, int | None, int | None, str]] = set()
    prior_order: int | None = None
    prior_key: tuple[object, ...] | None = None
    prior_event: int | None = None
    prior_instrument: int | None = None
    retired_instruments: set[int] = set()
    try:
        for chunk in chunks:
            if deadline is not None and clock() > deadline:
                raise TimeoutError("micro historical decode deadline reached")
            if not isinstance(chunk, np.ndarray) or chunk.dtype.names not in expected_dtypes:
                raise IntegrityError("micro DBN dtype differs from the pinned schema")
            records: list[dict[str, object]] = []
            for row in chunk:
                if source_schema == "definition":
                    record, economic = _definition_record(row, source_sha=expected_source_sha256, ordinal=row_count)
                    economics.add(economic)
                    order = int(record["ts_recv_ns"])
                    key = (order, record["instrument_id"], record["raw_symbol"])
                elif source_schema in INTERVAL_NS:
                    record, nulls = _bar_record(
                        row, source_sha=expected_source_sha256, ordinal=row_count, source_schema=source_schema
                    )
                    null_field_count += nulls
                    order = int(record["event_at_ns"])
                    key = (order, record["instrument_id"])
                    current_instrument = int(record["instrument_id"])
                    if prior_event == order and prior_instrument != current_instrument:
                        ambiguous_identity_count += 1
                    if prior_event is not None and order > prior_event and prior_instrument != current_instrument:
                        if prior_instrument is not None:
                            retired_instruments.add(prior_instrument)
                        if current_instrument in retired_instruments:
                            non_contiguous_instruments.add(current_instrument)
                        roll_transition_count += 1
                    if not roll_sequence or roll_sequence[-1] != current_instrument:
                        roll_sequence.append(current_instrument)
                    prior_event, prior_instrument = order, current_instrument
                elif source_schema == "status":
                    record = _status_record(row, source_sha=expected_source_sha256, ordinal=row_count)
                    order = int(record["ts_recv_ns"])
                    key = (hashlib.sha256(row.tobytes()).digest(),)
                else:
                    record, nulls = _statistics_record(row, source_sha=expected_source_sha256, ordinal=row_count)
                    null_field_count += nulls
                    order = int(record["ts_recv_ns"])
                    key = (hashlib.sha256(row.tobytes()).digest(),)
                if prior_order is not None and order < prior_order:
                    raise IntegrityError("micro DBN records are not in source order")
                if key == prior_key:
                    duplicate_count += 1
                prior_order, prior_key = order, key
                instruments.add(int(record["instrument_id"]))
                records.append(record)
                row_count += 1
            if records:
                table = _table(records, _schema_for(source_schema), source_schema=source_schema, source_sha=expected_source_sha256)
                if writer is None:
                    budget = created_byte_budget or CreatedByteBudget(2**63 - 1)
                    sink = _BoundedCreateOnlySink(partial, budget)
                    writer = pq.ParquetWriter(sink, table.schema, compression="zstd", use_dictionary=True)
                writer.write_table(table)
        if writer is not None:
            writer.close()
            writer = None
            if sink is not None:
                sink.close()
                sink = None
            if output_path.exists():
                raise IntegrityError("micro staged final appeared during decode")
            partial.rename(output_path)
            output_path.chmod(stat.S_IREAD)
            output_sha = sha256_file(output_path)
            output_bytes = output_path.stat().st_size
            output_relative = output_path.as_posix()
        else:
            output_sha = None
            output_bytes = 0
            output_relative = None
        if sha256_file(source_path) != expected_source_sha256:
            raise IntegrityError("micro DBN hash differs after decode")
        return DecodeResult(
            schema=source_schema,
            row_count=row_count,
            output_path=output_relative,
            output_sha256=output_sha,
            output_bytes=output_bytes,
            duplicate_count=duplicate_count,
            ambiguous_identity_count=ambiguous_identity_count,
            null_field_count=null_field_count,
            roll_transition_count=roll_transition_count,
            non_contiguous_instrument_count=len(non_contiguous_instruments),
            roll_sequence=tuple(roll_sequence),
            instrument_ids=tuple(sorted(instruments)),
            economics=tuple(sorted(economics)),
        )
    finally:
        if writer is not None:
            writer.close()
        if sink is not None:
            sink.close()
        del decoded
        del store


def materialize_causal_1m_inactive(
    *,
    source_path: Path,
    output_path: Path,
    identity_certified: bool,
    created_byte_budget: CreatedByteBudget | None = None,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> CausalResult:
    """Create a causal one-minute view without fitting or dropping null rows."""

    if not identity_certified:
        raise UnauthorizedOperation("uncertified micro identity cannot enter Phase 2")
    if output_path.exists() or output_path.with_suffix(output_path.suffix + ".partial").exists():
        raise IntegrityError("micro Phase 2 output already exists")
    parquet = pq.ParquetFile(source_path)
    source_parquet_sha256 = sha256_file(source_path)
    metadata = parquet.schema_arrow.metadata or {}
    if metadata.get(b"schema_id") != b"APEX_MICRO_PHASE1B_REPORTED_BAR_V1" or metadata.get(b"source_schema") != b"ohlcv-1m":
        raise IntegrityError("micro Phase 2 input is not a certified one-minute source")
    partial = output_path.with_suffix(output_path.suffix + ".partial")
    writer: pq.ParquetWriter | None = None
    sink: _BoundedCreateOnlySink | None = None
    row_count = 0
    eligible = 0
    explicit_null_rows = 0
    try:
        for batch in parquet.iter_batches(batch_size=MAX_BATCH_ROWS):
            if deadline is not None and clock() > deadline:
                raise TimeoutError("micro causal materialization deadline reached")
            table = pa.Table.from_batches([batch])
            null_mask = None
            for name in ("open_nano", "high_nano", "low_nano", "close_nano", "volume"):
                current = table[name].combine_chunks().is_null()
                null_mask = current if null_mask is None else pc.or_(null_mask, current)
            assert null_mask is not None
            null_count = int(pc.sum(pc.cast(null_mask, pa.int64())).as_py() or 0)
            count = table.num_rows
            explicit_null_rows += null_count
            eligible += count - null_count
            dispositions = pa.array(
                [
                    "SOURCE_NULL_PRESERVED_NOT_FEATURE_ELIGIBLE" if bool(value.as_py()) else "OBSERVED_REPORTED_BAR"
                    for value in null_mask
                ],
                type=pa.string(),
            )
            feature = pc.invert(null_mask)
            columns = [table[field.name].combine_chunks() for field in BAR_SCHEMA]
            causal = pa.Table.from_arrays(
                [*columns, dispositions, feature],
                schema=CAUSAL_SCHEMA.with_metadata(
                    {
                        **(CAUSAL_SCHEMA.metadata or {}),
                        b"lane_id": LANE_ID.encode("ascii"),
                        b"source_parquet_sha256": source_parquet_sha256.encode("ascii"),
                        b"availability_policy": AVAILABILITY_BASIS.encode("ascii"),
                    }
                ),
            )
            if writer is None:
                budget = created_byte_budget or CreatedByteBudget(2**63 - 1)
                sink = _BoundedCreateOnlySink(partial, budget)
                writer = pq.ParquetWriter(sink, causal.schema, compression="zstd", use_dictionary=True)
            writer.write_table(causal)
            row_count += count
        if writer is None:
            raise IntegrityError("micro Phase 2 cannot fabricate an empty output")
        writer.close()
        writer = None
        if sink is not None:
            sink.close()
            sink = None
        partial.rename(output_path)
        output_path.chmod(stat.S_IREAD)
        return CausalResult(
            row_count=row_count,
            feature_eligible_rows=eligible,
            explicit_null_rows=explicit_null_rows,
            output_path=output_path.as_posix(),
            output_sha256=sha256_file(output_path),
            output_bytes=output_path.stat().st_size,
        )
    finally:
        if writer is not None:
            writer.close()
        if sink is not None:
            sink.close()


__all__ = [
    "AVAILABILITY_BASIS",
    "BAR_SCHEMA",
    "CAUSAL_SCHEMA",
    "CreatedByteBudget",
    "CausalResult",
    "DEFINITION_SCHEMA",
    "DecodeResult",
    "MAX_BATCH_ROWS",
    "PINNED_PUBLICATION_LATENCY_NS",
    "STATISTICS_SCHEMA",
    "STATUS_SCHEMA",
    "decode_dbn_to_inactive_parquet",
    "materialize_causal_1m_inactive",
]
