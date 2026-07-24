"""Deterministic Parquet encoding for verified Phase 1B provider records."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterator, Mapping, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..identity import ActualContractIdentity, ContractDefinition
from ..errors import ContractError, IntegrityError
from ..canonical import sha256_json
from .decoder import iter_bars, iter_definitions
from .identity import DefinitionIndex
from .pipeline import build_causal_bar
from .records import ProviderBar, ProviderDefinition, UINT64_NULL
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
        ("instrument_id_date_utc", pa.string()),
        ("ts_event_ns", pa.uint64()),
        ("ts_recv_ns", pa.uint64()),
        ("activation_ns", pa.uint64()),
        ("expiration_ns", pa.uint64()),
        ("security_update_action", pa.string()),
        ("instrument_class", pa.string()),
        ("security_type", pa.string()),
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
        ("row_ordinal", pa.uint64()),
        ("row_sha256", pa.string()),
    ],
    metadata={b"schema_id": b"FUTURES_PHASE1B_DEFINITIONS_V2"},
)
CAUSAL_BAR_SCHEMA = pa.schema(
    [
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("market", pa.string(), nullable=False),
        pa.field("publisher_id", pa.uint16(), nullable=False),
        pa.field("instrument_id", pa.uint32(), nullable=False),
        pa.field("instrument_id_date_utc", pa.string(), nullable=False),
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
        pa.field("provider_timestamp_epoch_id", pa.string(), nullable=False),
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
        pa.field("exchange_session_date", pa.string()),
        pa.field("raw_symbol", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("definition_release_id", pa.string()),
        pa.field("definition_manifest_sha256", pa.string()),
        pa.field("definition_row_sha256", pa.string()),
        pa.field("definition_ts_event_ns", pa.int64()),
        pa.field("definition_ts_recv_ns", pa.int64()),
        pa.field("definition_index_date_utc", pa.string()),
        pa.field("definition_activation_ns", pa.int64()),
        pa.field("definition_expiration_ns", pa.int64()),
        pa.field("definition_security_update_action", pa.string()),
        pa.field("definition_instrument_class", pa.string()),
        pa.field("definition_security_type", pa.string()),
        pa.field("definition_source_row_ordinal", pa.int64()),
        pa.field("currency", pa.string()),
        pa.field("point_value", pa.string()),
        pa.field("tick_size", pa.string()),
        pa.field("tick_value", pa.string()),
        pa.field("quote_convention", pa.string()),
        pa.field("economics_rulebook_hash", pa.string()),
        pa.field("provider_unit_qty_state", pa.string()),
    ],
    metadata={b"schema_id": b"FUTURES_PHASE2_CAUSAL_BARS_V2"},
)


@dataclass
class _DefinitionTimestampCensus:
    row_count: int = 0
    negative_delta_rows: int = 0
    cross_utc_date_rows: int = 0
    undefined_ts_event_rows: int = 0
    undefined_ts_recv_rows: int = 0
    receive_order_violation_rows: int = 0
    minimum_delta_ns: int | None = None
    maximum_delta_ns: int | None = None
    _prior_ts_recv_ns: int | None = None
    _identity_date_keys: set[tuple[int, int, str]] = field(default_factory=set)

    def observe(self, row: ProviderDefinition) -> None:
        self.row_count += 1
        self._identity_date_keys.add(
            (row.publisher_id, row.instrument_id, row.instrument_id_date_utc)
        )
        if row.ts_event_ns in {0, UINT64_NULL}:
            self.undefined_ts_event_rows += 1
        if row.ts_recv_ns in {0, UINT64_NULL}:
            self.undefined_ts_recv_rows += 1
        delta = row.ts_recv_ns - row.ts_event_ns
        self.negative_delta_rows += int(delta < 0)
        self.cross_utc_date_rows += int(
            row.ts_recv_ns // 86_400_000_000_000
            != row.ts_event_ns // 86_400_000_000_000
        )
        if self._prior_ts_recv_ns is not None and row.ts_recv_ns < self._prior_ts_recv_ns:
            self.receive_order_violation_rows += 1
        self._prior_ts_recv_ns = row.ts_recv_ns
        self.minimum_delta_ns = (
            delta if self.minimum_delta_ns is None else min(self.minimum_delta_ns, delta)
        )
        self.maximum_delta_ns = (
            delta if self.maximum_delta_ns is None else max(self.maximum_delta_ns, delta)
        )

    def as_dict(self) -> dict[str, object]:
        if self.row_count <= 0 or self.minimum_delta_ns is None or self.maximum_delta_ns is None:
            raise IntegrityError("definition timestamp census cannot describe an empty set")
        return {
            "clock_contract": "TS_RECV_INDEX_TS_EVENT_AUDIT_ONLY",
            "cross_utc_date_rows": self.cross_utc_date_rows,
            "identity_date_key_count": len(self._identity_date_keys),
            "identity_date_key_set_sha256": sha256_json(
                sorted(self._identity_date_keys)
            ),
            "maximum_ts_recv_minus_ts_event_ns": self.maximum_delta_ns,
            "minimum_ts_recv_minus_ts_event_ns": self.minimum_delta_ns,
            "negative_ts_recv_minus_ts_event_rows": self.negative_delta_rows,
            "row_count": self.row_count,
            "ts_recv_order_violation_rows": self.receive_order_violation_rows,
            "undefined_ts_event_rows": self.undefined_ts_event_rows,
            "undefined_ts_recv_rows": self.undefined_ts_recv_rows,
        }


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
                "instrument_id_date_utc": row.instrument_id_date_utc,
                "ts_event_ns": row.ts_event_ns,
                "ts_recv_ns": row.ts_recv_ns,
                "activation_ns": row.activation_ns,
                "expiration_ns": row.expiration_ns,
                "security_update_action": row.security_update_action,
                "instrument_class": row.instrument_class,
                "security_type": row.security_type,
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
                "row_ordinal": row.row_ordinal,
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
) -> tuple[int, frozenset[tuple[int, int, str]]]:
    count = 0
    instrument_dates: set[tuple[int, int, str]] = set()
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
            instrument_dates.add(
                (
                    row.publisher_id,
                    row.instrument_id,
                    row.event_at.date().isoformat(),
                )
            )
            if len(buffer) >= batch_rows:
                writer.write_batch(_bar_batch(buffer), row_group_size=len(buffer))
                count += len(buffer)
                buffer.clear()
        if buffer:
            writer.write_batch(_bar_batch(buffer), row_group_size=len(buffer))
            count += len(buffer)
    finally:
        writer.close()
    if count == 0 or not instrument_dates:
        raise IntegrityError("canonical one-minute interval contains no bars")
    with output.open("r+b") as handle:
        os.fsync(handle.fileno())
    return count, frozenset(instrument_dates)


def write_relevant_definitions(
    binding: SnapshotFile,
    *,
    market: str,
    expected_query_contract: Mapping[str, object],
    required_instrument_dates: frozenset[tuple[int, int, str]],
    output: Path,
    batch_rows: int = 100_000,
) -> tuple[
    int,
    int,
    dict[str, object],
    frozenset[tuple[int, int, str]],
]:
    if not required_instrument_dates:
        raise ContractError(
            "definition selection requires actual bar publisher/instrument/date keys"
        )
    scanned = 0
    selected = 0
    census = _DefinitionTimestampCensus()
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
            if (
                row.publisher_id,
                row.instrument_id,
                row.instrument_id_date_utc,
            ) not in required_instrument_dates:
                continue
            census.observe(row)
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
    return (
        scanned,
        selected,
        census.as_dict(),
        frozenset(census._identity_date_keys),
    )


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


def raw_bar_row_count(path: Path) -> int:
    parquet = _assert_schema(path, RAW_BAR_SCHEMA)
    count = parquet.metadata.num_rows
    if count <= 0:
        raise IntegrityError("verified raw release contains no bars")
    return count


def read_raw_bar_audit(
    path: Path, *, batch_rows: int = 100_000
) -> tuple[int, frozenset[tuple[int, int, str]]]:
    parquet = _assert_schema(path, RAW_BAR_SCHEMA)
    count = 0
    keys: set[tuple[int, int, str]] = set()
    for batch in parquet.iter_batches(
        batch_size=batch_rows,
        columns=["publisher_id", "instrument_id", "event_at_ns"],
    ):
        for row in batch.to_pylist():
            count += 1
            publisher_id = row["publisher_id"]
            instrument_id = row["instrument_id"]
            event_at_ns = row["event_at_ns"]
            if any(
                isinstance(value, bool) or not isinstance(value, int)
                for value in (publisher_id, instrument_id, event_at_ns)
            ):
                raise IntegrityError("raw bar identity/date columns are invalid")
            keys.add(
                (
                    publisher_id,
                    instrument_id,
                    ns_to_datetime(event_at_ns, "bar.event_at_ns").date().isoformat(),
                )
            )
    if count <= 0 or not keys:
        raise IntegrityError("verified raw release contains no bar identity/date keys")
    return count, frozenset(keys)


def read_definitions(path: Path, *, batch_rows: int = 100_000) -> tuple[ProviderDefinition, ...]:
    parquet = _assert_schema(path, DEFINITION_SCHEMA)
    result: list[ProviderDefinition] = []
    for batch in parquet.iter_batches(batch_size=batch_rows):
        result.extend(ProviderDefinition(**raw) for raw in batch.to_pylist())
    if not result:
        raise IntegrityError("verified raw release has no definitions")
    return tuple(result)


def read_definition_audit(
    path: Path, *, batch_rows: int = 100_000
) -> tuple[dict[str, object], frozenset[tuple[int, int, str]]]:
    parquet = _assert_schema(path, DEFINITION_SCHEMA)
    census = _DefinitionTimestampCensus()
    for batch in parquet.iter_batches(batch_size=batch_rows):
        for raw in batch.to_pylist():
            census.observe(ProviderDefinition(**raw))
    return census.as_dict(), frozenset(census._identity_date_keys)


def read_definition_timestamp_census(
    path: Path, *, batch_rows: int = 100_000
) -> dict[str, object]:
    return read_definition_audit(path, batch_rows=batch_rows)[0]


def _failure_code(message: str) -> str:
    if "lifecycle source epoch is quarantined" in message:
        return "DEFINITION_LIFECYCLE_EPOCH_QUARANTINED"
    if "after bar start" in message:
        return "DEFINITION_INTRABAR_CHANGE"
    if "activation" in message or "expiration" in message or "lifecycle" in message:
        return "DEFINITION_LIFECYCLE_UNRESOLVED"
    if "definition index conflict" in message or "definition replay" in message:
        return "DEFINITION_ORDER_UNRESOLVED"
    if "no definition" in message or "definition" in message and "ambiguous" in message:
        return "DEFINITION_UNRESOLVED"
    if "econom" in message or "unit quantity" in message or "tick" in message:
        return "ECONOMICS_UNRESOLVED"
    if "session" in message or "exchange" in message:
        return "SESSION_UNRESOLVED"
    if "definition" in message:
        return "DEFINITION_UNRESOLVED"
    return "FOUNDATION_CONTRACT_UNRESOLVED"


@dataclass(frozen=True)
class _FastDefinitionContext:
    definition: ProviderDefinition
    contract_definition: ContractDefinition
    economics: object
    instrument_date: date
    disposition: str


@dataclass
class _FastCausalState:
    event_contracts: dict[int, tuple[int, str]] = field(default_factory=dict)
    session_dates: dict[tuple[str, int], date] = field(default_factory=dict)
    actual_identities: dict[
        tuple[ProviderDefinition, date], tuple[str, str]
    ] = field(default_factory=dict)


def _fast_definition_contexts(
    definitions: Sequence[ProviderDefinition],
    policies: VerifiedFoundationPolicies,
) -> Mapping[
    tuple[str, str, str, str, int, int, str],
    _FastDefinitionContext,
]:
    """Return only definition groups whose causal replay has one static version.

    Any ambiguous, updated, deleted, non-future, or economics-unresolved group
    is deliberately omitted and therefore uses the reference row-by-row path.
    """

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
    result: dict[
        tuple[str, str, str, str, int, int, str],
        _FastDefinitionContext,
    ] = {}
    for key, values in grouped.items():
        if len(values) != 1:
            continue
        selected = values[0]
        if (
            selected.security_update_action not in {"ADD", "MODIFY"}
            or selected.instrument_class != "FUTURE"
            or selected.security_type != "FUT"
            or selected.activation_ns in {0, UINT64_NULL}
            or selected.expiration_ns in {0, UINT64_NULL}
            or selected.activation_ns >= selected.expiration_ns
        ):
            continue
        try:
            economics = policies.economics.resolve(selected.market, selected)
            instrument_date = date.fromisoformat(
                selected.instrument_id_date_utc
            )
            contract_definition = ContractDefinition(
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
        except (ContractError, ValueError):
            continue
        result[key] = _FastDefinitionContext(
            definition=selected,
            contract_definition=contract_definition,
            economics=economics,
            instrument_date=instrument_date,
            disposition=(
                "ANOMALY_QUARANTINED"
                if policies.anomalies.is_quarantined(
                    selected.market, instrument_date.year
                )
                else "ELIGIBLE"
            ),
        )
    return result


def _column(batch: pa.RecordBatch, name: str) -> pa.Array:
    return batch.column(RAW_BAR_SCHEMA.get_field_index(name))


def _uniform_string(batch: pa.RecordBatch, name: str) -> str | None:
    values = _column(batch, name)
    if batch.num_rows == 0 or values.null_count:
        return None
    first = values[0].as_py()
    if (
        not isinstance(first, str)
        or not first
        or pc.all(pc.equal(values, pa.scalar(first))).as_py() is not True
    ):
        return None
    return first


def _take_context(
    contexts: Sequence[_FastDefinitionContext],
    codes: pa.Int32Array,
    values,
    *,
    value_type: pa.DataType,
) -> pa.Array:
    return pc.take(
        pa.array([values(context) for context in contexts], type=value_type),
        codes,
    )


def _fast_causal_batch(
    batch: pa.RecordBatch,
    *,
    contexts_by_key: Mapping[
        tuple[str, str, str, str, int, int, str],
        _FastDefinitionContext,
    ],
    policies: VerifiedFoundationPolicies,
    source_raw_release_id: str,
    state: _FastCausalState,
) -> pa.RecordBatch | None:
    """Vectorize the common one-definition eligible case.

    Returning ``None`` invokes the exact reference implementation for the
    entire batch, so the optimization can never invent semantics for a
    lifecycle update, unresolved row, or non-uniform source binding.
    """

    if batch.num_rows <= 0:
        return None
    source_release_id = _uniform_string(batch, "source_release_id")
    source_manifest_sha256 = _uniform_string(
        batch, "source_manifest_sha256"
    )
    dataset = _uniform_string(batch, "dataset")
    market = _uniform_string(batch, "market")
    if None in (
        source_release_id,
        source_manifest_sha256,
        dataset,
        market,
    ):
        return None
    try:
        event_values = _column(batch, "event_at_ns").to_numpy(
            zero_copy_only=False
        )
        publisher_values = _column(batch, "publisher_id").to_numpy(
            zero_copy_only=False
        )
        instrument_values = _column(batch, "instrument_id").to_numpy(
            zero_copy_only=False
        )
    except (pa.ArrowException, ValueError):
        return None
    if any(
        _column(batch, name).null_count
        for name in ("event_at_ns", "publisher_id", "instrument_id")
    ):
        return None
    utc_days = np.floor_divide(event_values, 86_400_000_000_000)
    day_strings = {
        int(day): date.fromordinal(date(1970, 1, 1).toordinal() + int(day)).isoformat()
        for day in np.unique(utc_days)
    }
    contexts: list[_FastDefinitionContext] = []
    context_codes: dict[ProviderDefinition, int] = {}
    row_codes: list[int] = []
    for publisher_id, instrument_id, day in zip(
        publisher_values, instrument_values, utc_days, strict=True
    ):
        context = contexts_by_key.get(
            (
                source_release_id,
                source_manifest_sha256,
                dataset,
                market,
                int(publisher_id),
                int(instrument_id),
                day_strings[int(day)],
            )
        )
        if context is None:
            return None
        code = context_codes.get(context.definition)
        if code is None:
            code = len(contexts)
            context_codes[context.definition] = code
            contexts.append(context)
        row_codes.append(code)
    code_values = np.asarray(row_codes, dtype=np.int32)
    for code, context in enumerate(contexts):
        selected_events = event_values[code_values == code]
        definition = context.definition
        if (
            selected_events.size == 0
            or np.any(selected_events < definition.ts_recv_ns)
            or np.any(selected_events < definition.activation_ns)
            or np.any(selected_events >= definition.expiration_ns)
        ):
            return None

    event_contracts: list[tuple[int, str]] = []
    try:
        for event_at_ns in event_values:
            event = int(event_at_ns)
            contract = state.event_contracts.get(event)
            if contract is None:
                policies.foundation.assert_definition_lifecycle_trusted(event)
                contract = (
                    policies.foundation.bar_available_at_ns(event),
                    policies.foundation.provider_timestamp_epoch_id(event),
                )
                state.event_contracts[event] = contract
            event_contracts.append(contract)
    except ContractError:
        return None

    actual_identity_hashes: list[str] = []
    exchange_session_dates: list[str] = []
    try:
        for code, event_at_ns in zip(row_codes, event_values, strict=True):
            context = contexts[code]
            event = int(event_at_ns)
            session_key = (context.definition.exchange, event)
            session_date = state.session_dates.get(session_key)
            if session_date is None:
                session_date = policies.exchange_session_date(
                    context.definition.exchange,
                    ns_to_datetime(event, "bar.event_at_ns"),
                )
                state.session_dates[session_key] = session_date
            actual_key = (context.definition, session_date)
            actual_fields = state.actual_identities.get(actual_key)
            if actual_fields is None:
                actual = ActualContractIdentity.from_definition(
                    context.contract_definition,
                    instrument_id_date_utc=context.instrument_date,
                    exchange_session_date=session_date,
                )
                actual_fields = (
                    actual.identity_hash,
                    session_date.isoformat(),
                )
                state.actual_identities[actual_key] = actual_fields
            actual_identity_hashes.append(actual_fields[0])
            exchange_session_dates.append(actual_fields[1])
    except ContractError:
        return None

    count = batch.num_rows
    codes = pa.array(code_values, type=pa.int32())
    available = pa.array(
        [item[0] for item in event_contracts], type=pa.int64()
    )
    arrays: dict[str, pa.Array] = {
        name: _column(batch, name)
        for name in (
            "dataset",
            "market",
            "publisher_id",
            "instrument_id",
            "event_at_ns",
            "open_nano",
            "high_nano",
            "low_nano",
            "close_nano",
            "volume",
            "source_release_id",
            "source_manifest_sha256",
            "source_file_path",
            "source_file_sha256",
        )
    }
    arrays.update(
        {
            "instrument_id_date_utc": pa.array(
                [day_strings[int(day)] for day in utc_days],
                type=pa.string(),
            ),
            "available_at_ns": available,
            "resolution_as_of_ns": available,
            "availability_basis": pa.repeat(
                policies.foundation.availability_basis, count
            ),
            "availability_policy_hash": pa.repeat(
                policies.foundation.policy_hash, count
            ),
            "foundation_policy_set_id": pa.repeat(
                policies.policy_set_id, count
            ),
            "provider_timestamp_epoch_id": pa.array(
                [item[1] for item in event_contracts], type=pa.string()
            ),
            "source_raw_release_id": pa.repeat(
                source_raw_release_id, count
            ),
            "source_row_sha256": _column(batch, "row_sha256"),
            "disposition": _take_context(
                contexts,
                codes,
                lambda item: item.disposition,
                value_type=pa.string(),
            ),
            "prediction_in_coverage_denominator": pa.repeat(True, count),
            "failure_code": pa.nulls(count, type=pa.string()),
            "failure_detail_sha256": pa.nulls(count, type=pa.string()),
            "actual_identity_hash": pa.array(
                actual_identity_hashes, type=pa.string()
            ),
            "exchange_session_date": pa.array(
                exchange_session_dates, type=pa.string()
            ),
            "raw_symbol": _take_context(
                contexts,
                codes,
                lambda item: item.definition.raw_symbol,
                value_type=pa.string(),
            ),
            "exchange": _take_context(
                contexts,
                codes,
                lambda item: item.definition.exchange,
                value_type=pa.string(),
            ),
            "definition_release_id": _take_context(
                contexts,
                codes,
                lambda item: item.definition.source_release_id,
                value_type=pa.string(),
            ),
            "definition_manifest_sha256": _take_context(
                contexts,
                codes,
                lambda item: item.definition.source_manifest_sha256,
                value_type=pa.string(),
            ),
            "definition_row_sha256": _take_context(
                contexts,
                codes,
                lambda item: item.definition.row_sha256,
                value_type=pa.string(),
            ),
            "definition_ts_event_ns": _take_context(
                contexts,
                codes,
                lambda item: item.definition.ts_event_ns,
                value_type=pa.int64(),
            ),
            "definition_ts_recv_ns": _take_context(
                contexts,
                codes,
                lambda item: item.definition.ts_recv_ns,
                value_type=pa.int64(),
            ),
            "definition_index_date_utc": _take_context(
                contexts,
                codes,
                lambda item: item.definition.instrument_id_date_utc,
                value_type=pa.string(),
            ),
            "definition_activation_ns": _take_context(
                contexts,
                codes,
                lambda item: item.definition.activation_ns,
                value_type=pa.int64(),
            ),
            "definition_expiration_ns": _take_context(
                contexts,
                codes,
                lambda item: item.definition.expiration_ns,
                value_type=pa.int64(),
            ),
            "definition_security_update_action": _take_context(
                contexts,
                codes,
                lambda item: item.definition.security_update_action,
                value_type=pa.string(),
            ),
            "definition_instrument_class": _take_context(
                contexts,
                codes,
                lambda item: item.definition.instrument_class,
                value_type=pa.string(),
            ),
            "definition_security_type": _take_context(
                contexts,
                codes,
                lambda item: item.definition.security_type,
                value_type=pa.string(),
            ),
            "definition_source_row_ordinal": _take_context(
                contexts,
                codes,
                lambda item: item.definition.row_ordinal,
                value_type=pa.int64(),
            ),
            "currency": _take_context(
                contexts,
                codes,
                lambda item: item.economics.currency,
                value_type=pa.string(),
            ),
            "point_value": _take_context(
                contexts,
                codes,
                lambda item: str(item.economics.point_value),
                value_type=pa.string(),
            ),
            "tick_size": _take_context(
                contexts,
                codes,
                lambda item: str(item.economics.tick_size),
                value_type=pa.string(),
            ),
            "tick_value": _take_context(
                contexts,
                codes,
                lambda item: str(item.economics.tick_value),
                value_type=pa.string(),
            ),
            "quote_convention": _take_context(
                contexts,
                codes,
                lambda item: item.economics.quote_convention,
                value_type=pa.string(),
            ),
            "economics_rulebook_hash": _take_context(
                contexts,
                codes,
                lambda item: item.economics.rulebook_hash,
                value_type=pa.string(),
            ),
            "provider_unit_qty_state": _take_context(
                contexts,
                codes,
                lambda item: item.economics.provider_unit_qty_state,
                value_type=pa.string(),
            ),
        }
    )
    return pa.RecordBatch.from_arrays(
        [arrays[field.name] for field in CAUSAL_BAR_SCHEMA],
        schema=CAUSAL_BAR_SCHEMA,
    )


def _update_value_counts(
    values: pa.Array, counts: dict[str, int]
) -> None:
    for item in pc.value_counts(values).to_pylist():
        value = item["values"]
        count = item["counts"]
        if not isinstance(value, str) or not value or not isinstance(count, int):
            raise IntegrityError("causal vectorized census is invalid")
        counts[value] = counts.get(value, 0) + count


def _causal_row(
    bar: ProviderBar,
    *,
    definition_index: DefinitionIndex,
    policies: VerifiedFoundationPolicies,
    source_raw_release_id: str,
) -> dict[str, object]:
    available_ns = policies.foundation.bar_available_at_ns(bar.event_at_ns)
    instrument_id_date_utc = bar.event_at.date().isoformat()
    provider_timestamp_epoch_id = policies.foundation.provider_timestamp_epoch_id(
        bar.event_at_ns
    )
    base: dict[str, object] = {
        "dataset": bar.dataset,
        "market": bar.market,
        "publisher_id": bar.publisher_id,
        "instrument_id": bar.instrument_id,
        "instrument_id_date_utc": instrument_id_date_utc,
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
        "provider_timestamp_epoch_id": provider_timestamp_epoch_id,
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
        "definition_index_date_utc": None,
        "definition_activation_ns": None,
        "definition_expiration_ns": None,
        "definition_security_update_action": None,
        "definition_instrument_class": None,
        "definition_security_type": None,
        "definition_source_row_ordinal": None,
        "economics_rulebook_hash": None,
        "exchange": None,
        "exchange_session_date": None,
        "failure_code": None,
        "failure_detail_sha256": None,
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
    selected = result.definition
    if selected.row_sha256 != result.definition_row_sha256:
        raise IntegrityError("causal definition replay is not deterministic")
    if actual.instrument_id_date_utc.isoformat() != instrument_id_date_utc:
        raise IntegrityError("causal actual identity date differs from the bar index date")
    if result.provider_timestamp_epoch_id != provider_timestamp_epoch_id:
        raise IntegrityError("causal provider timestamp epoch is not deterministic")
    return {
        **base,
        **nullable,
        "actual_identity_hash": actual.identity_hash,
        "currency": economics.currency,
        "definition_manifest_sha256": actual.definition_manifest_sha256,
        "definition_release_id": actual.definition_release_id,
        "definition_row_sha256": result.definition_row_sha256,
        "definition_ts_event_ns": selected.ts_event_ns,
        "definition_ts_recv_ns": selected.ts_recv_ns,
        "definition_index_date_utc": selected.instrument_id_date_utc,
        "definition_activation_ns": selected.activation_ns,
        "definition_expiration_ns": selected.expiration_ns,
        "definition_security_update_action": selected.security_update_action,
        "definition_instrument_class": selected.instrument_class,
        "definition_security_type": selected.security_type,
        "definition_source_row_ordinal": selected.row_ordinal,
        "disposition": result.disposition.value,
        "economics_rulebook_hash": economics.rulebook_hash,
        "exchange": actual.exchange,
        "exchange_session_date": actual.exchange_session_date.isoformat(),
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
) -> tuple[int, dict[str, int], dict[str, int]]:
    policies.verify()
    definitions = read_definitions(definitions_path, batch_rows=batch_rows)
    index = DefinitionIndex(definitions)
    fast_contexts = _fast_definition_contexts(definitions, policies)
    fast_state = _FastCausalState()
    counts: dict[str, int] = {}
    epoch_counts: dict[str, int] = {}
    count = 0
    parquet = _assert_schema(raw_bars_path, RAW_BAR_SCHEMA)
    writer = _writer(output, CAUSAL_BAR_SCHEMA)
    try:
        for raw_batch in parquet.iter_batches(batch_size=batch_rows):
            batch = _fast_causal_batch(
                raw_batch,
                contexts_by_key=fast_contexts,
                policies=policies,
                source_raw_release_id=source_raw_release_id,
                state=fast_state,
            )
            if batch is None:
                rows = [
                    _causal_row(
                        ProviderBar(**raw),
                        definition_index=index,
                        policies=policies,
                        source_raw_release_id=source_raw_release_id,
                    )
                    for raw in raw_batch.to_pylist()
                ]
                batch = pa.RecordBatch.from_pylist(
                    rows, schema=CAUSAL_BAR_SCHEMA
                )
            _update_value_counts(
                batch.column(
                    CAUSAL_BAR_SCHEMA.get_field_index("disposition")
                ),
                counts,
            )
            _update_value_counts(
                batch.column(
                    CAUSAL_BAR_SCHEMA.get_field_index(
                        "provider_timestamp_epoch_id"
                    )
                ),
                epoch_counts,
            )
            writer.write_batch(batch, row_group_size=batch.num_rows)
            count += batch.num_rows
    finally:
        writer.close()
    if count == 0 or sum(counts.values()) != count:
        raise IntegrityError("causal output count/disposition census is invalid")
    with output.open("r+b") as handle:
        os.fsync(handle.fileno())
    return count, dict(sorted(counts.items())), dict(sorted(epoch_counts.items()))


def read_causal_bar_census(
    path: Path, *, batch_rows: int = 100_000
) -> dict[str, object]:
    parquet = _assert_schema(path, CAUSAL_BAR_SCHEMA)
    row_count = 0
    denominator_rows = 0
    dispositions: dict[str, int] = {}
    epoch_counts: dict[str, int] = {}
    policy_set_ids: set[str] = set()
    raw_release_ids: set[str] = set()
    for batch in parquet.iter_batches(
        batch_size=batch_rows,
        columns=[
            "disposition",
            "prediction_in_coverage_denominator",
            "provider_timestamp_epoch_id",
            "foundation_policy_set_id",
            "source_raw_release_id",
        ],
    ):
        for row in batch.to_pylist():
            row_count += 1
            disposition = row["disposition"]
            epoch_id = row["provider_timestamp_epoch_id"]
            policy_set_id = row["foundation_policy_set_id"]
            raw_release_id = row["source_raw_release_id"]
            if not all(
                isinstance(value, str) and value
                for value in (disposition, epoch_id, policy_set_id, raw_release_id)
            ):
                raise IntegrityError("causal Parquet census fields are invalid")
            dispositions[disposition] = dispositions.get(disposition, 0) + 1
            epoch_counts[epoch_id] = epoch_counts.get(epoch_id, 0) + 1
            policy_set_ids.add(policy_set_id)
            raw_release_ids.add(raw_release_id)
            denominator = row["prediction_in_coverage_denominator"]
            if type(denominator) is not bool:
                raise IntegrityError("causal coverage-denominator flag is invalid")
            denominator_rows += int(denominator)
    if row_count <= 0:
        raise IntegrityError("causal Parquet contains no rows")
    return {
        "disposition_counts": dict(sorted(dispositions.items())),
        "foundation_policy_set_ids": sorted(policy_set_ids),
        "prediction_in_coverage_denominator_rows": denominator_rows,
        "provider_timestamp_epoch_counts": dict(sorted(epoch_counts.items())),
        "row_count": row_count,
        "source_raw_release_ids": sorted(raw_release_ids),
    }
