"""Deterministic Parquet encoding for verified Phase 1B provider records."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator, Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from ..errors import ContractError, IntegrityError
from ..canonical import sha256_json
from .decoder import iter_bars, iter_definitions
from .identity import DefinitionIndex
from .pipeline import build_causal_bar
from .records import ProviderBar, ProviderDefinition
from .records import ns_to_datetime
from .snapshot import SnapshotFile
from .support import VerifiedFoundationPolicies


RAW_BAR_SCHEMA = pa.schema(
    [
        ("dataset", pa.string()),
        ("market", pa.string()),
        ("publisher_id", pa.uint16()),
        ("instrument_id", pa.uint32()),
        ("event_at_ns", pa.int64()),
        ("open_nano", pa.int64()),
        ("high_nano", pa.int64()),
        ("low_nano", pa.int64()),
        ("close_nano", pa.int64()),
        ("volume", pa.uint64()),
        ("source_release_id", pa.string()),
        ("source_manifest_sha256", pa.string()),
        ("source_file_path", pa.string()),
        ("source_file_sha256", pa.string()),
        ("row_sha256", pa.string()),
    ],
    metadata={b"schema_id": b"FUTURES_PHASE1B_RAW_BARS_V1"},
)
DEFINITION_SCHEMA = pa.schema(
    [
        ("dataset", pa.string()),
        ("market", pa.string()),
        ("publisher_id", pa.uint16()),
        ("instrument_id", pa.uint32()),
        ("ts_event_ns", pa.int64()),
        ("ts_recv_ns", pa.int64()),
        ("raw_symbol", pa.string()),
        ("exchange", pa.string()),
        ("currency", pa.string()),
        ("min_price_increment_nano", pa.int64()),
        ("unit_of_measure_qty_nano", pa.int64()),
        ("unit_of_measure", pa.string()),
        ("source_release_id", pa.string()),
        ("source_manifest_sha256", pa.string()),
        ("source_file_path", pa.string()),
        ("source_file_sha256", pa.string()),
        ("row_sha256", pa.string()),
    ],
    metadata={b"schema_id": b"FUTURES_PHASE1B_DEFINITIONS_V1"},
)
CAUSAL_BAR_SCHEMA = pa.schema(
    [
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("market", pa.string(), nullable=False),
        pa.field("publisher_id", pa.uint16(), nullable=False),
        pa.field("instrument_id", pa.uint32(), nullable=False),
        pa.field("event_at_ns", pa.int64(), nullable=False),
        pa.field("available_at_ns", pa.int64(), nullable=False),
        pa.field("resolution_as_of_ns", pa.int64(), nullable=False),
        pa.field("open_nano", pa.int64(), nullable=False),
        pa.field("high_nano", pa.int64(), nullable=False),
        pa.field("low_nano", pa.int64(), nullable=False),
        pa.field("close_nano", pa.int64(), nullable=False),
        pa.field("volume", pa.uint64(), nullable=False),
        pa.field("availability_basis", pa.string(), nullable=False),
        pa.field("availability_policy_hash", pa.string(), nullable=False),
        pa.field("foundation_policy_set_id", pa.string(), nullable=False),
        pa.field("source_raw_release_id", pa.string(), nullable=False),
        pa.field("source_release_id", pa.string(), nullable=False),
        pa.field("source_manifest_sha256", pa.string(), nullable=False),
        pa.field("source_file_path", pa.string(), nullable=False),
        pa.field("source_file_sha256", pa.string(), nullable=False),
        pa.field("source_row_sha256", pa.string(), nullable=False),
        pa.field("disposition", pa.string(), nullable=False),
        pa.field("prediction_in_coverage_denominator", pa.bool_(), nullable=False),
        pa.field("failure_code", pa.string()),
        pa.field("failure_detail_sha256", pa.string()),
        pa.field("actual_identity_hash", pa.string()),
        pa.field("instrument_id_date_utc", pa.string()),
        pa.field("exchange_session_date", pa.string()),
        pa.field("raw_symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("definition_release_id", pa.string()),
        pa.field("definition_manifest_sha256", pa.string()),
        pa.field("definition_row_sha256", pa.string()),
        pa.field("definition_ts_event_ns", pa.int64()),
        pa.field("definition_ts_recv_ns", pa.int64()),
        pa.field("currency", pa.string()),
        pa.field("point_value", pa.string()),
        pa.field("tick_size", pa.string()),
        pa.field("tick_value", pa.string()),
        pa.field("quote_convention", pa.string()),
        pa.field("economics_rulebook_hash", pa.string()),
        pa.field("provider_unit_qty_state", pa.string()),
    ],
    metadata={b"schema_id": b"FUTURES_PHASE2_CAUSAL_BARS_V1"},
)


def _writer(path: Path, schema: pa.Schema) -> pq.ParquetWriter:
    if path.exists():
        raise IntegrityError("immutable Parquet output already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    return pq.ParquetWriter(
        path,
        schema,
        compression="zstd",
        compression_level=9,
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
        data_page_version="2.0",
        use_deprecated_int96_timestamps=False,
    )


def _bar_batch(rows: Sequence[ProviderBar]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(
        [
            {
                "dataset": row.dataset,
                "market": row.market,
                "publisher_id": row.publisher_id,
                "instrument_id": row.instrument_id,
                "event_at_ns": row.event_at_ns,
                "open_nano": row.open_nano,
                "high_nano": row.high_nano,
                "low_nano": row.low_nano,
                "close_nano": row.close_nano,
                "volume": row.volume,
                "source_release_id": row.source_release_id,
                "source_manifest_sha256": row.source_manifest_sha256,
                "source_file_path": row.source_file_path,
                "source_file_sha256": row.source_file_sha256,
                "row_sha256": row.row_sha256,
            }
            for row in rows
        ],
        schema=RAW_BAR_SCHEMA,
    )


def _definition_batch(rows: Sequence[ProviderDefinition]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(
        [
            {
                "dataset": row.dataset,
                "market": row.market,
                "publisher_id": row.publisher_id,
                "instrument_id": row.instrument_id,
                "ts_event_ns": row.ts_event_ns,
                "ts_recv_ns": row.ts_recv_ns,
                "raw_symbol": row.raw_symbol,
                "exchange": row.exchange,
                "currency": row.currency,
                "min_price_increment_nano": row.min_price_increment_nano,
                "unit_of_measure_qty_nano": row.unit_of_measure_qty_nano,
                "unit_of_measure": row.unit_of_measure,
                "source_release_id": row.source_release_id,
                "source_manifest_sha256": row.source_manifest_sha256,
                "source_file_path": row.source_file_path,
                "source_file_sha256": row.source_file_sha256,
                "row_sha256": row.row_sha256,
            }
            for row in rows
        ],
        schema=DEFINITION_SCHEMA,
    )


def write_raw_bars(
    binding: SnapshotFile,
    *,
    market: str,
    expected_query_contract: Mapping[str, object],
    output: Path,
    batch_rows: int = 100_000,
) -> tuple[int, frozenset[int]]:
    count = 0
    instruments: set[int] = set()
    buffer: list[ProviderBar] = []
    writer = _writer(output, RAW_BAR_SCHEMA)
    try:
        for row in iter_bars(
            binding,
            market=market,
            expected_query_contract=expected_query_contract,
            batch_rows=batch_rows,
        ):
            buffer.append(row)
            instruments.add(row.instrument_id)
            if len(buffer) >= batch_rows:
                writer.write_batch(_bar_batch(buffer), row_group_size=len(buffer))
                count += len(buffer)
                buffer.clear()
        if buffer:
            writer.write_batch(_bar_batch(buffer), row_group_size=len(buffer))
            count += len(buffer)
    finally:
        writer.close()
    if count == 0 or not instruments:
        raise IntegrityError("canonical one-minute interval contains no bars")
    with output.open("r+b") as handle:
        os.fsync(handle.fileno())
    return count, frozenset(instruments)


def write_relevant_definitions(
    binding: SnapshotFile,
    *,
    market: str,
    expected_query_contract: Mapping[str, object],
    required_instrument_ids: frozenset[int],
    output: Path,
    batch_rows: int = 100_000,
) -> tuple[int, int]:
    if not required_instrument_ids:
        raise ContractError("definition selection requires actual bar instrument IDs")
    scanned = 0
    selected = 0
    buffer: list[ProviderDefinition] = []
    writer = _writer(output, DEFINITION_SCHEMA)
    try:
        for row in iter_definitions(
            binding,
            market=market,
            expected_query_contract=expected_query_contract,
            batch_rows=batch_rows,
        ):
            scanned += 1
            if row.instrument_id not in required_instrument_ids:
                continue
            buffer.append(row)
            if len(buffer) >= batch_rows:
                writer.write_batch(_definition_batch(buffer), row_group_size=len(buffer))
                selected += len(buffer)
                buffer.clear()
        if buffer:
            writer.write_batch(_definition_batch(buffer), row_group_size=len(buffer))
            selected += len(buffer)
    finally:
        writer.close()
    if selected == 0:
        raise IntegrityError("no definitions match the actual bar instruments")
    with output.open("r+b") as handle:
        os.fsync(handle.fileno())
    return scanned, selected


def _assert_schema(path: Path, expected: pa.Schema) -> pq.ParquetFile:
    try:
        parquet = pq.ParquetFile(path)
    except (OSError, pa.ArrowException) as exc:
        raise IntegrityError("verified raw release contains invalid Parquet") from exc
    if not parquet.schema_arrow.equals(expected, check_metadata=True):
        raise IntegrityError("Parquet schema differs from the pinned foundation schema")
    return parquet


def iter_raw_bars(path: Path, *, batch_rows: int = 100_000) -> Iterator[ProviderBar]:
    parquet = _assert_schema(path, RAW_BAR_SCHEMA)
    for batch in parquet.iter_batches(batch_size=batch_rows):
        for raw in batch.to_pylist():
            yield ProviderBar(**raw)


def read_definitions(path: Path, *, batch_rows: int = 100_000) -> tuple[ProviderDefinition, ...]:
    parquet = _assert_schema(path, DEFINITION_SCHEMA)
    result: list[ProviderDefinition] = []
    for batch in parquet.iter_batches(batch_size=batch_rows):
        result.extend(ProviderDefinition(**raw) for raw in batch.to_pylist())
    if not result:
        raise IntegrityError("verified raw release has no definitions")
    return tuple(result)


def _failure_code(message: str) -> str:
    if "no definition" in message or "definition" in message and "ambiguous" in message:
        return "DEFINITION_UNRESOLVED"
    if "econom" in message or "unit quantity" in message or "tick" in message:
        return "ECONOMICS_UNRESOLVED"
    if "session" in message or "exchange" in message:
        return "SESSION_UNRESOLVED"
    return "FOUNDATION_CONTRACT_UNRESOLVED"


def _causal_row(
    bar: ProviderBar,
    *,
    definition_index: DefinitionIndex,
    policies: VerifiedFoundationPolicies,
    source_raw_release_id: str,
) -> dict[str, object]:
    available_ns = policies.foundation.bar_available_at_ns(bar.event_at_ns)
    base: dict[str, object] = {
        "dataset": bar.dataset,
        "market": bar.market,
        "publisher_id": bar.publisher_id,
        "instrument_id": bar.instrument_id,
        "event_at_ns": bar.event_at_ns,
        "available_at_ns": available_ns,
        "resolution_as_of_ns": available_ns,
        "open_nano": bar.open_nano,
        "high_nano": bar.high_nano,
        "low_nano": bar.low_nano,
        "close_nano": bar.close_nano,
        "volume": bar.volume,
        "availability_basis": policies.foundation.availability_basis,
        "availability_policy_hash": policies.foundation.policy_hash,
        "foundation_policy_set_id": policies.policy_set_id,
        "source_raw_release_id": source_raw_release_id,
        "source_release_id": bar.source_release_id,
        "source_manifest_sha256": bar.source_manifest_sha256,
        "source_file_path": bar.source_file_path,
        "source_file_sha256": bar.source_file_sha256,
        "source_row_sha256": bar.row_sha256,
        "prediction_in_coverage_denominator": True,
    }
    nullable = {
        "actual_identity_hash": None,
        "currency": None,
        "definition_manifest_sha256": None,
        "definition_release_id": None,
        "definition_row_sha256": None,
        "definition_ts_event_ns": None,
        "definition_ts_recv_ns": None,
        "economics_rulebook_hash": None,
        "exchange": None,
        "exchange_session_date": None,
        "failure_code": None,
        "failure_detail_sha256": None,
        "instrument_id_date_utc": None,
        "point_value": None,
        "provider_unit_qty_state": None,
        "quote_convention": None,
        "raw_symbol": None,
        "tick_size": None,
        "tick_value": None,
    }
    try:
        result = build_causal_bar(
            bar,
            definition_index,
            decision_at=ns_to_datetime(available_ns, "available_at_ns"),
            policy=policies.foundation,
            anomaly_policy=policies.anomalies,
            session_policy=policies,
            economics_rules=policies.economics,
        )
    except ContractError as exc:
        detail = str(exc)
        return {
            **base,
            **nullable,
            "disposition": "UNRESOLVED_FAIL_CLOSED",
            "failure_code": _failure_code(detail),
            "failure_detail_sha256": sha256_json(
                {"error_type": type(exc).__name__, "message": detail}
            ),
        }
    actual = result.actual
    economics = result.economics
    return {
        **base,
        **nullable,
        "actual_identity_hash": actual.identity_hash,
        "currency": economics.currency,
        "definition_manifest_sha256": actual.definition_manifest_sha256,
        "definition_release_id": actual.definition_release_id,
        "definition_row_sha256": result.definition_row_sha256,
        "definition_ts_event_ns": definition_index.resolve(
            bar, decision_at=result.decision_at
        ).ts_event_ns,
        "definition_ts_recv_ns": definition_index.resolve(
            bar, decision_at=result.decision_at
        ).ts_recv_ns,
        "disposition": result.disposition.value,
        "economics_rulebook_hash": economics.rulebook_hash,
        "exchange": actual.exchange,
        "exchange_session_date": actual.exchange_session_date.isoformat(),
        "instrument_id_date_utc": actual.instrument_id_date_utc.isoformat(),
        "point_value": str(economics.point_value),
        "provider_unit_qty_state": economics.provider_unit_qty_state,
        "quote_convention": economics.quote_convention,
        "raw_symbol": actual.raw_symbol,
        "tick_size": str(economics.tick_size),
        "tick_value": str(economics.tick_value),
    }


def write_causal_bars(
    *,
    raw_bars_path: Path,
    definitions_path: Path,
    policies: VerifiedFoundationPolicies,
    source_raw_release_id: str,
    output: Path,
    batch_rows: int = 100_000,
) -> tuple[int, dict[str, int]]:
    policies.verify()
    definitions = read_definitions(definitions_path, batch_rows=batch_rows)
    index = DefinitionIndex(definitions)
    counts: dict[str, int] = {}
    count = 0
    buffer: list[dict[str, object]] = []
    writer = _writer(output, CAUSAL_BAR_SCHEMA)
    try:
        for bar in iter_raw_bars(raw_bars_path, batch_rows=batch_rows):
            row = _causal_row(
                bar,
                definition_index=index,
                policies=policies,
                source_raw_release_id=source_raw_release_id,
            )
            disposition = str(row["disposition"])
            counts[disposition] = counts.get(disposition, 0) + 1
            buffer.append(row)
            if len(buffer) >= batch_rows:
                batch = pa.RecordBatch.from_pylist(buffer, schema=CAUSAL_BAR_SCHEMA)
                writer.write_batch(batch, row_group_size=len(buffer))
                count += len(buffer)
                buffer.clear()
        if buffer:
            batch = pa.RecordBatch.from_pylist(buffer, schema=CAUSAL_BAR_SCHEMA)
            writer.write_batch(batch, row_group_size=len(buffer))
            count += len(buffer)
    finally:
        writer.close()
    if count == 0 or sum(counts.values()) != count:
        raise IntegrityError("causal output count/disposition census is invalid")
    with output.open("r+b") as handle:
        os.fsync(handle.fileno())
    return count, dict(sorted(counts.items()))
