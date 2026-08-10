from __future__ import annotations

import inspect
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild import micro_alpha_phase1b2_group_diagnostic as diagnostic
from futures_rebuild import micro_alpha_phase1b2_decoder as decoder
from futures_rebuild.research_gateway_policy import (
    PREPARATORY_REAL_HISTORY_OPERATIONS,
    require_current_real_history_operation,
)


ROOT = Path(__file__).resolve().parents[1]


def _bar_row(*, ordinal: int, event: int, instrument: int) -> dict[str, object]:
    return {
        "publisher_id": 1,
        "instrument_id": instrument,
        "event_at_ns": event,
        "available_at_ns": event + 5_000_000_000,
        "open_nano": 1_000_000_000,
        "high_nano": 1_000_000_000,
        "low_nano": 1_000_000_000,
        "close_nano": 1_000_000_000,
        "volume": 1,
        "availability_basis": "INTERVAL_END_PLUS_5_SECONDS",
        "source_file_sha256": "a" * 64,
        "row_ordinal": ordinal,
        "row_sha256": "b" * 64,
    }


def _write_phase1b(
    path: Path, *, schema: pa.Schema, source_schema: str,
    rows: list[dict[str, object]],
) -> None:
    enriched = schema.with_metadata(
        {
            **(schema.metadata or {}),
            b"lane_id": b"apex_integer_micro_11",
            b"source_schema": source_schema.encode("ascii"),
            b"source_file_sha256": b"a" * 64,
            b"availability_policy": b"INTERVAL_END_PLUS_5_SECONDS",
        }
    )
    table = pa.Table.from_pylist(rows, schema=enriched)
    pq.write_table(table, path)


def _write_bars(path: Path, rows: list[dict[str, object]]) -> None:
    _write_phase1b(
        path, schema=decoder.BAR_SCHEMA, source_schema="ohlcv-1m", rows=rows
    )


def test_operation_is_exactly_allowlisted() -> None:
    assert diagnostic.OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS
    require_current_real_history_operation(diagnostic.OPERATION, {})


def test_prepare_surface_cannot_execute_rows() -> None:
    from scripts import prepare_apex_micro_phase1b2_group_diagnostic_v2 as prepare

    source = inspect.getsource(prepare)
    assert "execute_once" not in source
    assert '"execute"' not in source
    assert '"preview-plan"' in source
    assert '"write-audit"' in source


def test_plan_build_is_stat_only_for_phase1b_sources() -> None:
    source = inspect.getsource(diagnostic.build_plan)
    assert "pq.ParquetFile" not in source
    assert "iter_batches" not in source
    assert "sha256_file(source_path)" not in source
    plan = diagnostic.build_plan(
        root=ROOT,
        implementation_head=diagnostic._git_head(ROOT),
    )
    assert plan["source_count"] == 5
    assert {item["schema"] for item in plan["sources"]} == set(diagnostic.SCHEMAS)
    assert plan["pre_authority_payload_reads"] == 0
    assert plan["diagnostic_only"] is True


def test_reconstruct_bar_summary_preserves_roll_and_duplicate_semantics(tmp_path: Path) -> None:
    path = tmp_path / "bars.parquet"
    _write_bars(
        path,
        [
            _bar_row(ordinal=0, event=1, instrument=10),
            _bar_row(ordinal=1, event=1, instrument=10),
            _bar_row(ordinal=2, event=2, instrument=11),
        ],
    )
    source = {
        "schema": "ohlcv-1m",
        "year": 2018,
        "source_sha256": "a" * 64,
        "sha256": "c" * 64,
        "bytes": path.stat().st_size,
    }
    result = diagnostic.reconstruct_decode_result(
        source_path=path,
        source=source,
        deadline=time.monotonic() + 10,
    )
    assert result.row_count == 3
    assert result.duplicate_count == 1
    assert result.roll_sequence == (10, 11)
    assert result.roll_transition_count == 1
    assert result.non_contiguous_instrument_count == 0
    assert result.public_record()["raw_values_reported"] is False


def test_reconstruct_accepts_all_five_frozen_phase1b_schemas(tmp_path: Path) -> None:
    lineage = {
        "source_file_sha256": "a" * 64,
        "row_ordinal": 0,
        "row_sha256": "b" * 64,
    }
    rows = {
        "definition": {
            "publisher_id": 1, "instrument_id": 10, "ts_event_ns": 1,
            "ts_recv_ns": 2, "activation_ns": 1, "expiration_ns": 9,
            "security_update_action_raw": b"A", "instrument_class_raw": b"F",
            "security_type": "FUT", "raw_symbol": "M6EH8", "exchange": "XCME",
            "currency": "USD", "min_price_increment_nano": 5_000_000,
            "unit_of_measure_qty_nano": 12_500_000_000,
            "unit_of_measure": "EUR", **lineage,
        },
        "status": {
            "publisher_id": 1, "instrument_id": 10, "ts_event_ns": 1,
            "ts_recv_ns": 2, "action_code": 1, "reason_code": 0,
            "trading_event_code": 1, "is_trading_raw": b"Y",
            "is_quoting_raw": b"Y", "is_short_sell_restricted_raw": b"N",
            **lineage,
        },
        "statistics": {
            "publisher_id": 1, "instrument_id": 10, "ts_event_ns": 1,
            "ts_recv_ns": 2, "ts_ref_ns": None, "price_nano": None,
            "quantity": None, "sequence": 1, "ts_in_delta": 0,
            "stat_type_code": 1, "channel_id": 1, "update_action_code": 1,
            "flags": 0, **lineage,
        },
        "ohlcv-1m": _bar_row(ordinal=0, event=1, instrument=10),
        "ohlcv-1s": _bar_row(ordinal=0, event=1, instrument=10),
    }
    schema_by_name = {
        "definition": decoder.DEFINITION_SCHEMA,
        "status": decoder.STATUS_SCHEMA,
        "statistics": decoder.STATISTICS_SCHEMA,
        "ohlcv-1m": decoder.BAR_SCHEMA,
        "ohlcv-1s": decoder.BAR_SCHEMA,
    }
    for source_schema, row in rows.items():
        path = tmp_path / f"{source_schema}.parquet"
        _write_phase1b(
            path,
            schema=schema_by_name[source_schema],
            source_schema=source_schema,
            rows=[row],
        )
        result = diagnostic.reconstruct_decode_result(
            source_path=path,
            source={
                "schema": source_schema,
                "year": 2018,
                "source_sha256": "a" * 64,
                "sha256": "c" * 64,
                "bytes": path.stat().st_size,
            },
            deadline=time.monotonic() + 10,
        )
        assert result.schema == source_schema
        assert result.row_count == 1
        assert result.instrument_ids == (10,)
    assert diagnostic.reconstruct_decode_result(
        source_path=tmp_path / "statistics.parquet",
        source={
            "schema": "statistics", "year": 2018,
            "source_sha256": "a" * 64, "sha256": "c" * 64,
            "bytes": (tmp_path / "statistics.parquet").stat().st_size,
        },
        deadline=time.monotonic() + 10,
    ).null_field_count == 3


def test_2025_rejected_before_parquet_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    opened = False

    def forbidden_open(path: Path) -> object:
        nonlocal opened
        opened = True
        raise AssertionError(path)

    monkeypatch.setattr(diagnostic.pq, "ParquetFile", forbidden_open)
    with pytest.raises(UnauthorizedOperation, match="outside the frozen scope"):
        diagnostic.reconstruct_decode_result(
            source_path=tmp_path / "2025.parquet",
            source={"schema": "ohlcv-1m", "year": 2025},
            deadline=time.monotonic() + 10,
        )
    assert opened is False


def test_authorization_is_consumed_before_any_row_reconstruction() -> None:
    source = inspect.getsource(diagnostic.execute_once)
    assert source.index("authorization.verify(") < source.index("reconstruct_decode_result(")
    assert source.index("authorization.consume(") < source.index("reconstruct_decode_result(")
    assert source.index("sha256_file(source_path)") < source.index(
        "reconstruct_decode_result("
    )


def test_diagnostic_forbids_phase2_output_and_raw_value_reporting() -> None:
    source = inspect.getsource(diagnostic.execute_once)
    assert "materialize_causal_1m_inactive" not in source
    assert '"phase2_parquets_created": 0' in source
    assert '"raw_values_reported": False' in source
    assert '"year_2025_or_2026_payloads_opened": 0' in source
