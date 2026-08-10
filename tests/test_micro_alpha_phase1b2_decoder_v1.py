from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow.parquet as pq
import pytest

from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.foundation.decoder import (
    DEFINITION_DTYPE_V1,
    OHLCV_DTYPE,
    STATISTICS_DTYPE,
    STATUS_DTYPE,
)
from futures_rebuild.foundation.records import INT64_NULL, UINT64_NULL
from futures_rebuild import micro_alpha_phase1b2_decoder as decoder


pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def _ns(value: str) -> int:
    return int(
        datetime.fromisoformat(value)
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1_000_000_000
    )


def _dtype(names: tuple[str, ...]) -> np.dtype:
    text = {
        "currency", "settl_currency", "secsubtype", "raw_symbol", "group",
        "exchange", "asset", "cfi", "security_type", "unit_of_measure",
        "underlying", "strike_price_currency",
    }
    one = {
        "security_update_action", "instrument_class", "is_trading",
        "is_quoting", "is_short_sell_restricted",
    }
    unsigned = {"ts_ref", "volume"}
    return np.dtype(
        [
            (
                name,
                "S32" if name in text else "S1" if name in one else "u8" if name in unsigned else "i8",
            )
            for name in names
        ]
    )


def _row(schema: str, *, null_bar: bool = False) -> np.ndarray:
    names = {
        "definition": DEFINITION_DTYPE_V1,
        "status": STATUS_DTYPE,
        "statistics": STATISTICS_DTYPE,
        "ohlcv-1m": OHLCV_DTYPE,
        "ohlcv-1s": OHLCV_DTYPE,
    }[schema]
    rows = np.zeros(1, dtype=_dtype(names))
    rows["publisher_id"] = 1
    rows["instrument_id"] = 11
    rows["ts_event"] = _ns("2024-01-02")
    if "ts_recv" in names:
        rows["ts_recv"] = _ns("2024-01-02") + 10
    if schema == "definition":
        rows["activation"] = _ns("2023-12-01")
        rows["expiration"] = _ns("2024-03-01")
        rows["security_update_action"] = b"A"
        rows["instrument_class"] = b"F"
        rows["security_type"] = b"FUT"
        rows["raw_symbol"] = b"MESH4"
        rows["exchange"] = b"XCME"
        rows["currency"] = b"USD"
        rows["min_price_increment"] = 250_000_000
        rows["unit_of_measure_qty"] = 5_000_000_000
        rows["unit_of_measure"] = b"IPNT"
    elif schema == "status":
        rows["action"] = 1
        rows["reason"] = 2
        rows["trading_event"] = 3
        rows["is_trading"] = b"Y"
        rows["is_quoting"] = b"Y"
        rows["is_short_sell_restricted"] = b"N"
    elif schema == "statistics":
        rows["ts_ref"] = UINT64_NULL
        rows["price"] = INT64_NULL
        rows["quantity"] = INT64_NULL
        rows["sequence"] = 1
        rows["stat_type"] = 1
    else:
        for name, value in zip(("open", "high", "low", "close"), (100, 110, 90, 105)):
            rows[name] = INT64_NULL if null_bar else value
        rows["volume"] = UINT64_NULL if null_bar else 7
    return rows


def _query(schema: str) -> dict[str, object]:
    definition = schema == "definition"
    return {
        "compression": "zstd",
        "dataset": "GLBX.MDP3",
        "encoding": "dbn",
        "end": "2025-01-01",
        "schema": schema,
        "start": "2024-01-01",
        "stype_in": "parent" if definition else "continuous",
        "stype_out": "instrument_id",
        "symbols": ["MES.FUT" if definition else "MES.v.0"],
    }


def _install_store(
    monkeypatch: pytest.MonkeyPatch, *, schema: str, rows: np.ndarray
) -> None:
    metadata = SimpleNamespace(
        dataset="GLBX.MDP3",
        schema=schema,
        stype_in="parent" if schema == "definition" else "continuous",
        stype_out="instrument_id",
        symbols=["MES.FUT" if schema == "definition" else "MES.v.0"],
        start=_ns("2024-01-01"),
        end=_ns("2025-01-01"),
        ts_out=False,
        limit=None,
    )
    store = SimpleNamespace(metadata=metadata, to_ndarray=lambda *, count: rows)
    monkeypatch.setattr(
        decoder.databento,
        "DBNStore",
        SimpleNamespace(from_file=lambda path: store),
    )


@pytest.mark.parametrize("schema", ("definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"))
def test_all_five_schemas_decode_offline_create_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema: str
) -> None:
    source = tmp_path / "2024" / "2024-01-01_2025-01-01.dbn.zst"
    source.parent.mkdir()
    source.write_bytes(b"synthetic-price-free-test-fixture")
    output = tmp_path / "inactive" / f"{schema}.parquet"
    _install_store(monkeypatch, schema=schema, rows=_row(schema))
    result = decoder.decode_dbn_to_inactive_parquet(
        source_path=source,
        output_path=output,
        market="MES",
        source_schema=schema,
        exact_query=_query(schema),
        expected_source_sha256=sha256_file(source),
    )
    assert result.row_count == 1
    assert result.output_sha256 == sha256_file(output)
    assert pq.ParquetFile(output).schema_arrow.metadata[b"source_schema"] == schema.encode()
    with pytest.raises(IntegrityError, match="already exists"):
        decoder.decode_dbn_to_inactive_parquet(
            source_path=source,
            output_path=output,
            market="MES",
            source_schema=schema,
            exact_query=_query(schema),
            expected_source_sha256=sha256_file(source),
        )


def test_nullability_and_causal_availability_are_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "2024" / "2024-01-01_2025-01-01.dbn.zst"
    source.parent.mkdir()
    source.write_bytes(b"synthetic-null-test-fixture")
    phase1b = tmp_path / "inactive" / "bars.parquet"
    phase2 = tmp_path / "inactive" / "causal.parquet"
    _install_store(monkeypatch, schema="ohlcv-1m", rows=_row("ohlcv-1m", null_bar=True))
    result = decoder.decode_dbn_to_inactive_parquet(
        source_path=source,
        output_path=phase1b,
        market="MES",
        source_schema="ohlcv-1m",
        exact_query=_query("ohlcv-1m"),
        expected_source_sha256=sha256_file(source),
    )
    table = pq.read_table(phase1b)
    assert result.null_field_count == 5
    assert table["open_nano"].null_count == 1
    assert table["available_at_ns"][0].as_py() == (
        _ns("2024-01-02") + 60_000_000_000 + 5_000_000_000
    )
    causal = decoder.materialize_causal_1m_inactive(
        source_path=phase1b, output_path=phase2, identity_certified=True
    )
    causal_table = pq.read_table(phase2)
    assert causal.explicit_null_rows == 1
    assert causal.feature_eligible_rows == 0
    assert causal_table["causal_disposition"][0].as_py() == (
        "SOURCE_NULL_PRESERVED_NOT_FEATURE_ELIGIBLE"
    )


def test_2025_and_2026_fail_before_dbn_or_hash_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened: list[Path] = []
    monkeypatch.setattr(
        decoder.databento,
        "DBNStore",
        SimpleNamespace(from_file=lambda path: opened.append(Path(path))),
    )
    monkeypatch.setattr(
        decoder, "sha256_file", lambda path: pytest.fail("sealed bytes were accessed")
    )
    for year in (2025, 2026):
        source = tmp_path / str(year) / f"{year}-01-01_{year + 1}-01-01.dbn.zst"
        with pytest.raises(UnauthorizedOperation, match="sealed"):
            decoder.decode_dbn_to_inactive_parquet(
                source_path=source,
                output_path=tmp_path / f"{year}.parquet",
                market="MES",
                source_schema="ohlcv-1m",
                exact_query=_query("ohlcv-1m"),
                expected_source_sha256="a" * 64,
            )
    assert opened == []


def test_wrong_symbology_and_metadata_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "2024" / "source.dbn.zst"
    source.parent.mkdir()
    source.write_bytes(b"synthetic-query-drift")
    _install_store(monkeypatch, schema="ohlcv-1m", rows=_row("ohlcv-1m"))
    query = _query("ohlcv-1m")
    query["symbols"] = ["MES.FUT"]
    with pytest.raises(IntegrityError, match="query contract"):
        decoder.decode_dbn_to_inactive_parquet(
            source_path=source,
            output_path=tmp_path / "output.parquet",
            market="MES",
            source_schema="ohlcv-1m",
            exact_query=query,
            expected_source_sha256=sha256_file(source),
        )


def test_source_hash_drift_fails_before_dbn_store_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "2024" / "source.dbn.zst"
    source.parent.mkdir()
    source.write_bytes(b"mutated-source")
    opened: list[Path] = []
    monkeypatch.setattr(
        decoder.databento,
        "DBNStore",
        SimpleNamespace(from_file=lambda path: opened.append(Path(path))),
    )
    with pytest.raises(IntegrityError, match="hash differs before"):
        decoder.decode_dbn_to_inactive_parquet(
            source_path=source,
            output_path=tmp_path / "output.parquet",
            market="MES",
            source_schema="ohlcv-1m",
            exact_query=_query("ohlcv-1m"),
            expected_source_sha256="0" * 64,
        )
    assert opened == []


def test_one_second_schema_makes_only_reported_bar_claims() -> None:
    fields = set(decoder.BAR_SCHEMA.names)
    forbidden = {"bid", "ask", "queue_position", "fill", "tick_order"}
    assert fields.isdisjoint(forbidden)
    assert decoder.AVAILABILITY_BASIS == (
        "MODELED_INTERVAL_END_PLUS_PINNED_5_SECOND_LATENCY"
    )


def test_shared_created_byte_budget_refuses_before_excess_write(tmp_path: Path) -> None:
    budget = decoder.CreatedByteBudget(3)
    sink = decoder._BoundedCreateOnlySink(tmp_path / "partial.parquet", budget)
    assert sink.write(b"abc") == 3
    with pytest.raises(UnauthorizedOperation, match="byte ceiling"):
        sink.write(b"d")
    sink.close()
    assert (tmp_path / "partial.parquet").read_bytes() == b"abc"
    assert budget.used == 3
