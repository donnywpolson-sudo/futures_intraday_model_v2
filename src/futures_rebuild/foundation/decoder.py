"""Pinned, offline DBN decoding into exact provider foundation records."""

from __future__ import annotations

import gc
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import databento
import databento_dbn as dbn
import numpy as np

from ..canonical import sha256_json
from ..errors import ContractError, IntegrityError
from ..source_symbology import require_allowed_query_symbology
from .records import (
    ProviderBar,
    ProviderDefinition,
    StatisticsRecordV1,
    StatusRecordV1,
    exact_int,
    ns_to_datetime,
)
from .snapshot import DBN_NAME, SnapshotFile


SUPPORTED_DATABENTO_VERSION = "0.78.0"
DATASET = "GLBX.MDP3"
MAX_BATCH_ROWS = 1_000_000
DEFINITION_DTYPE_V1 = (
    "length", "rtype", "publisher_id", "instrument_id", "ts_event", "ts_recv",
    "min_price_increment", "display_factor", "expiration", "activation",
    "high_limit_price", "low_limit_price", "max_price_variation",
    "trading_reference_price", "unit_of_measure_qty",
    "min_price_increment_amount", "price_ratio", "inst_attrib_value",
    "underlying_id", "raw_instrument_id", "market_depth_implied", "market_depth",
    "market_segment_id", "max_trade_vol", "min_lot_size", "min_lot_size_block",
    "min_lot_size_round_lot", "min_trade_vol", "_reserved2",
    "contract_multiplier", "decay_quantity", "original_contract_size",
    "_reserved3", "trading_reference_date", "appl_id", "maturity_year",
    "decay_start_date", "channel_id", "currency", "settl_currency", "secsubtype",
    "raw_symbol", "group", "exchange", "asset", "cfi", "security_type",
    "unit_of_measure", "underlying", "strike_price_currency", "instrument_class",
    "_reserved4", "strike_price", "_reserved5", "match_algorithm",
    "md_security_trading_status", "main_fraction", "price_display_format",
    "settl_price_type", "sub_fraction", "underlying_product",
    "security_update_action", "maturity_month", "maturity_day", "maturity_week",
    "user_defined_instrument", "contract_multiplier_unit", "flow_schedule_type",
    "tick_rule", "_dummy",
)
DEFINITION_DTYPE_V3 = (
    "length", "rtype", "publisher_id", "instrument_id", "ts_event", "ts_recv",
    "min_price_increment", "display_factor", "expiration", "activation",
    "high_limit_price", "low_limit_price", "max_price_variation",
    "unit_of_measure_qty", "min_price_increment_amount", "price_ratio",
    "strike_price", "raw_instrument_id", "leg_price", "leg_delta",
    "inst_attrib_value", "underlying_id", "market_depth_implied", "market_depth",
    "market_segment_id", "max_trade_vol", "min_lot_size", "min_lot_size_block",
    "min_lot_size_round_lot", "min_trade_vol", "contract_multiplier",
    "decay_quantity", "original_contract_size", "leg_instrument_id",
    "leg_ratio_price_numerator", "leg_ratio_price_denominator",
    "leg_ratio_qty_numerator", "leg_ratio_qty_denominator", "leg_underlying_id",
    "appl_id", "maturity_year", "decay_start_date", "channel_id", "leg_count",
    "leg_index", "currency", "settl_currency", "secsubtype", "raw_symbol",
    "group", "exchange", "asset", "cfi", "security_type", "unit_of_measure",
    "underlying", "strike_price_currency", "leg_raw_symbol", "instrument_class",
    "match_algorithm", "main_fraction", "price_display_format", "sub_fraction",
    "underlying_product", "security_update_action", "maturity_month",
    "maturity_day", "maturity_week", "user_defined_instrument",
    "contract_multiplier_unit", "flow_schedule_type", "tick_rule",
    "leg_instrument_class", "leg_side", "_reserved",
)
OHLCV_DTYPE = (
    "length", "rtype", "publisher_id", "instrument_id", "ts_event",
    "open", "high", "low", "close", "volume",
)
STATUS_DTYPE = (
    "length", "rtype", "publisher_id", "instrument_id", "ts_event",
    "ts_recv", "action", "reason", "trading_event", "is_trading",
    "is_quoting", "is_short_sell_restricted", "_reserved",
)
STATISTICS_DTYPE = (
    "length", "rtype", "publisher_id", "instrument_id", "ts_event",
    "ts_recv", "ts_ref", "price", "quantity", "sequence", "ts_in_delta",
    "stat_type", "channel_id", "update_action", "stat_flags", "_reserved",
)


def _text(value: object, name: str) -> str:
    if isinstance(value, np.bytes_):
        value = bytes(value)
    if not isinstance(value, bytes):
        raise IntegrityError(f"{name} is not an encoded DBN string")
    try:
        decoded = value.rstrip(b"\x00").decode("ascii", errors="strict")
    except UnicodeDecodeError as exc:
        raise IntegrityError(f"{name} is not strict ASCII") from exc
    if not decoded:
        raise IntegrityError(f"{name} is empty")
    return decoded


def _integer(value: object, name: str, *, nonnegative: bool = False) -> int:
    if isinstance(value, np.generic):
        value = value.item()
    return exact_int(value, name, nonnegative=nonnegative)


def _enum_name(enum_type: object, value: object, name: str) -> str:
    raw = _integer(value, name, nonnegative=True)
    try:
        return str(enum_type(raw).name)  # type: ignore[operator]
    except (TypeError, ValueError):
        return f"UNKNOWN_{raw}"


def _tri_state(value: object, name: str) -> str:
    if isinstance(value, np.bytes_):
        value = bytes(value)
    if not isinstance(value, bytes) or len(value) > 1:
        raise IntegrityError(f"{name} is not one exact DBN tri-state byte")
    if value == b"":
        return "UNKNOWN_EMPTY"
    try:
        return str(dbn.TriState(value.decode("ascii")).name)
    except (UnicodeDecodeError, ValueError):
        return f"UNKNOWN_{value.hex()}"


def _date_boundary_ns(value: str) -> int:
    parsed = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = parsed - epoch
    return delta.days * 86_400_000_000_000


def _validate_metadata(
    store: databento.DBNStore,
    binding: SnapshotFile,
    *,
    schema: str,
    market: str,
) -> None:
    if databento.__version__ != SUPPORTED_DATABENTO_VERSION:
        raise ContractError("foundation decoding requires the pinned offline DBN decoder")
    name = Path(binding.relative_path).name
    match = DBN_NAME.fullmatch(name)
    if match is None:
        raise IntegrityError("snapshot DBN filename is invalid")
    metadata = store.metadata
    require_allowed_query_symbology(
        schema=schema,
        market=market,
        stype_in=str(metadata.stype_in),
        symbols=metadata.symbols,
    )
    if (
        metadata.dataset != DATASET
        or str(metadata.schema) != schema
        or str(metadata.stype_out) != "instrument_id"
        or metadata.ts_out is not False
        or metadata.limit is not None
        or metadata.start != _date_boundary_ns(match.group("start"))
        or metadata.end != _date_boundary_ns(match.group("end"))
    ):
        raise IntegrityError("DBN metadata differs from its exact foundation contract")


def _chunks(
    binding: SnapshotFile,
    *,
    schema: str,
    market: str,
    batch_rows: int,
) -> Iterator[np.ndarray]:
    if (
        isinstance(batch_rows, bool)
        or not isinstance(batch_rows, int)
        or not 1 <= batch_rows <= MAX_BATCH_ROWS
    ):
        raise ContractError("DBN decode batch size is outside its bounded range")
    path = binding.verify()
    store = databento.DBNStore.from_file(path)
    _validate_metadata(store, binding, schema=schema, market=market)
    decoded = store.to_ndarray(count=batch_rows)
    chunks = (decoded,) if isinstance(decoded, np.ndarray) else decoded
    expected_dtypes = {
        "definition": {DEFINITION_DTYPE_V1, DEFINITION_DTYPE_V3},
        "ohlcv-1m": {OHLCV_DTYPE},
        "status": {STATUS_DTYPE},
        "statistics": {STATISTICS_DTYPE},
    }.get(schema)
    if expected_dtypes is None:
        raise ContractError("foundation decoder schema is unsupported")
    try:
        for chunk in chunks:
            if not isinstance(chunk, np.ndarray) or chunk.dtype.names not in expected_dtypes:
                raise IntegrityError("decoded DBN dtype differs from the pinned schema")
            yield chunk
    except (NotImplementedError, TypeError, ValueError) as exc:
        raise IntegrityError("offline DBN decoding failed") from exc
    finally:
        del decoded
        del store
        gc.collect()
        binding.verify()


def _row_id(binding: SnapshotFile, ordinal: int, raw_bytes: bytes) -> str:
    return sha256_json(
        {
            "file_sha256": binding.sha256,
            "ordinal": ordinal,
            "raw_record_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        }
    )


def iter_definitions(
    binding: SnapshotFile,
    *,
    market: str,
    batch_rows: int = 100_000,
) -> Iterator[ProviderDefinition]:
    ordinal = 0
    for chunk in _chunks(
        binding, schema="definition", market=market, batch_rows=batch_rows
    ):
        for row in chunk:
            raw_bytes = row.tobytes()
            yield ProviderDefinition(
                dataset=DATASET,
                market=market,
                publisher_id=_integer(row["publisher_id"], "publisher_id"),
                instrument_id=_integer(row["instrument_id"], "instrument_id"),
                ts_event_ns=_integer(row["ts_event"], "ts_event", nonnegative=True),
                ts_recv_ns=_integer(row["ts_recv"], "ts_recv", nonnegative=True),
                raw_symbol=_text(row["raw_symbol"], "raw_symbol"),
                exchange=_text(row["exchange"], "exchange"),
                currency=_text(row["currency"], "currency"),
                min_price_increment_nano=_integer(
                    row["min_price_increment"], "min_price_increment"
                ),
                unit_of_measure_qty_nano=_integer(
                    row["unit_of_measure_qty"], "unit_of_measure_qty"
                ),
                unit_of_measure=_text(row["unit_of_measure"], "unit_of_measure"),
                source_release_id=binding.source_snapshot_id,
                source_manifest_sha256=binding.migration_manifest_sha256,
                source_file_path=binding.relative_path,
                source_file_sha256=binding.sha256,
                row_sha256=_row_id(binding, ordinal, raw_bytes),
            )
            ordinal += 1


def iter_bars(
    binding: SnapshotFile,
    *,
    market: str,
    schema: str = "ohlcv-1m",
    batch_rows: int = 100_000,
) -> Iterator[ProviderBar]:
    if schema != "ohlcv-1m":
        raise ContractError("only ohlcv-1m is canonical foundation research input")
    ordinal = 0
    for chunk in _chunks(binding, schema=schema, market=market, batch_rows=batch_rows):
        for row in chunk:
            raw_bytes = row.tobytes()
            yield ProviderBar(
                dataset=DATASET,
                market=market,
                publisher_id=_integer(row["publisher_id"], "publisher_id"),
                instrument_id=_integer(row["instrument_id"], "instrument_id"),
                event_at_ns=_integer(row["ts_event"], "ts_event", nonnegative=True),
                open_nano=_integer(row["open"], "open"),
                high_nano=_integer(row["high"], "high"),
                low_nano=_integer(row["low"], "low"),
                close_nano=_integer(row["close"], "close"),
                volume=_integer(row["volume"], "volume", nonnegative=True),
                source_release_id=binding.source_snapshot_id,
                source_manifest_sha256=binding.migration_manifest_sha256,
                source_file_path=binding.relative_path,
                source_file_sha256=binding.sha256,
                row_sha256=_row_id(binding, ordinal, raw_bytes),
            )
            ordinal += 1


def iter_statuses(
    binding: SnapshotFile,
    *,
    market: str,
    batch_rows: int = 100_000,
) -> Iterator[StatusRecordV1]:
    ordinal = 0
    for chunk in _chunks(
        binding, schema="status", market=market, batch_rows=batch_rows
    ):
        for row in chunk:
            raw_bytes = row.tobytes()
            ts_event_ns = _integer(
                row["ts_event"], "status.ts_event", nonnegative=True
            )
            yield StatusRecordV1(
                dataset=DATASET,
                market=market,
                publisher_id=_integer(row["publisher_id"], "publisher_id"),
                instrument_id=_integer(row["instrument_id"], "instrument_id"),
                instrument_id_date_utc=ns_to_datetime(
                    ts_event_ns, "status.ts_event"
                ).date().isoformat(),
                ts_event_ns=ts_event_ns,
                ts_recv_ns=_integer(
                    row["ts_recv"], "status.ts_recv", nonnegative=True
                ),
                action=_enum_name(dbn.StatusAction, row["action"], "status.action"),
                reason=_enum_name(dbn.StatusReason, row["reason"], "status.reason"),
                trading_event=_enum_name(
                    dbn.TradingEvent, row["trading_event"], "status.trading_event"
                ),
                is_trading=_tri_state(row["is_trading"], "status.is_trading"),
                is_quoting=_tri_state(row["is_quoting"], "status.is_quoting"),
                is_short_sell_restricted=_tri_state(
                    row["is_short_sell_restricted"],
                    "status.is_short_sell_restricted",
                ),
                source_release_id=binding.source_snapshot_id,
                source_manifest_sha256=binding.migration_manifest_sha256,
                source_file_path=binding.relative_path,
                source_file_sha256=binding.sha256,
                row_ordinal=ordinal,
                row_sha256=_row_id(binding, ordinal, raw_bytes),
            )
            ordinal += 1


def iter_statistics(
    binding: SnapshotFile,
    *,
    market: str,
    batch_rows: int = 100_000,
) -> Iterator[StatisticsRecordV1]:
    ordinal = 0
    for chunk in _chunks(
        binding, schema="statistics", market=market, batch_rows=batch_rows
    ):
        for row in chunk:
            raw_bytes = row.tobytes()
            ts_event_ns = _integer(
                row["ts_event"], "statistics.ts_event", nonnegative=True
            )
            yield StatisticsRecordV1(
                dataset=DATASET,
                market=market,
                publisher_id=_integer(row["publisher_id"], "publisher_id"),
                instrument_id=_integer(row["instrument_id"], "instrument_id"),
                instrument_id_date_utc=ns_to_datetime(
                    ts_event_ns, "statistics.ts_event"
                ).date().isoformat(),
                ts_event_ns=ts_event_ns,
                ts_recv_ns=_integer(
                    row["ts_recv"], "statistics.ts_recv", nonnegative=True
                ),
                ts_ref_ns=_integer(
                    row["ts_ref"], "statistics.ts_ref", nonnegative=True
                ),
                ts_in_delta=_integer(
                    row["ts_in_delta"], "statistics.ts_in_delta"
                ),
                stat_type=_enum_name(
                    dbn.StatType, row["stat_type"], "statistics.stat_type"
                ),
                update_action=_enum_name(
                    dbn.StatUpdateAction,
                    row["update_action"],
                    "statistics.update_action",
                ),
                price_nano=_integer(row["price"], "statistics.price"),
                quantity=_integer(
                    row["quantity"], "statistics.quantity", nonnegative=True
                ),
                sequence=_integer(
                    row["sequence"], "statistics.sequence", nonnegative=True
                ),
                channel_id=_integer(
                    row["channel_id"], "statistics.channel_id", nonnegative=True
                ),
                flags=_integer(
                    row["stat_flags"], "statistics.stat_flags", nonnegative=True
                ),
                source_release_id=binding.source_snapshot_id,
                source_manifest_sha256=binding.migration_manifest_sha256,
                source_file_path=binding.relative_path,
                source_file_sha256=binding.sha256,
                row_ordinal=ordinal,
                row_sha256=_row_id(binding, ordinal, raw_bytes),
            )
            ordinal += 1
