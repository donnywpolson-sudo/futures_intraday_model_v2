from __future__ import annotations

import inspect
import time
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild import micro_alpha_phase1b2_decoder as decoder
from futures_rebuild import micro_alpha_phase1b2_phase2_successor as successor
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.micro_alpha_phase1b2_decoder import DecodeResult
from futures_rebuild.micro_alpha_phase1b2_execution import _expected_economics
from futures_rebuild.research_gateway_policy import (
    PREPARATORY_REAL_HISTORY_OPERATIONS,
    require_current_real_history_operation,
)


ROOT = Path(__file__).resolve().parents[1]


def _definition_row(
    *, ordinal: int, receive: int, tick: int = 5_000_000,
) -> dict[str, object]:
    return {
        "publisher_id": 1,
        "instrument_id": 10,
        "ts_event_ns": receive - 1,
        "ts_recv_ns": receive,
        "activation_ns": 1,
        "expiration_ns": 9,
        "security_update_action_raw": b"A",
        "instrument_class_raw": b"F",
        "security_type": "FUT",
        "raw_symbol": "M6EH8",
        "exchange": "XCME",
        "currency": "USD",
        "min_price_increment_nano": tick,
        "unit_of_measure_qty_nano": 12_500_000_000,
        "unit_of_measure": "EUR",
        "source_file_sha256": "a" * 64,
        "row_ordinal": ordinal,
        "row_sha256": f"{ordinal:064x}",
    }


def _write_definition(path: Path, rows: list[dict[str, object]]) -> dict[str, object]:
    schema = decoder.DEFINITION_SCHEMA.with_metadata(
        {
            **(decoder.DEFINITION_SCHEMA.metadata or {}),
            b"lane_id": b"apex_integer_micro_11",
            b"source_schema": b"definition",
            b"source_file_sha256": b"a" * 64,
        }
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return {
        "request_id": "c" * 64,
        "market": "M6E",
        "schema": "definition",
        "year": 2018,
        "source_sha256": "a" * 64,
        "sha256": "b" * 64,
        "bytes": path.stat().st_size,
    }


def _scan(path: Path, rows: list[dict[str, object]]) -> successor.Phase1BScan:
    return successor.reconstruct_phase1b_scan(
        source_path=path,
        source=_write_definition(path, rows),
        deadline=time.monotonic() + 10,
    )


def _result(
    schema: str, *, duplicates: int = 0, economics=(), rows: int = 10,
) -> DecodeResult:
    return DecodeResult(
        schema=schema,
        row_count=rows,
        output_path="inactive.parquet",
        output_sha256="a" * 64,
        output_bytes=10,
        duplicate_count=duplicates,
        ambiguous_identity_count=0,
        null_field_count=0,
        roll_transition_count=0,
        non_contiguous_instrument_count=0,
        roll_sequence=(10,) if schema in {"ohlcv-1m", "ohlcv-1s"} else (),
        instrument_ids=(10,),
        economics=tuple(economics),
    )


def _accepted_scans(classification: str) -> dict[str, successor.Phase1BScan]:
    tick, quantity, currency = _expected_economics("M6E")
    certificate = {
        "classification": classification,
        "legacy_repeat_count": 1 if classification != "NO_LEGACY_REPEATS" else 0,
    }
    return {
        "definition": successor.Phase1BScan(
            result=_result(
                "definition",
                duplicates=1 if classification != "NO_LEGACY_REPEATS" else 0,
                economics=((10, tick, quantity, currency),),
            ),
            definition_repeat_certificate=certificate,
        ),
        "status": successor.Phase1BScan(_result("status"), None),
        "statistics": successor.Phase1BScan(_result("statistics"), None),
        "ohlcv-1m": successor.Phase1BScan(_result("ohlcv-1m"), None),
        "ohlcv-1s": successor.Phase1BScan(_result("ohlcv-1s", rows=20), None),
    }


def test_operation_is_exactly_allowlisted() -> None:
    assert successor.OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS
    require_current_real_history_operation(successor.OPERATION, {})


def test_prepare_surface_cannot_execute_rows() -> None:
    from scripts import prepare_apex_micro_phase1b2_phase2_successor_v4 as prepare

    source = inspect.getsource(prepare)
    assert "execute_once" not in source
    assert '"execute"' not in source
    assert '"preview-plan"' in source
    assert '"write-audit"' in source


def test_plan_is_stat_only_and_binds_complete_successor_scope() -> None:
    source = inspect.getsource(successor.build_plan)
    assert "pq.ParquetFile" not in source
    assert "iter_batches" not in source
    assert "sha256_file(source_path)" not in source
    plan = successor.load_plan(root=ROOT)
    if (ROOT / str(plan["staging_root"])).exists():
        with pytest.raises(IntegrityError, match="create-only output collision"):
            successor.build_plan(
                root=ROOT, implementation_head=successor._git_head(ROOT)
            )
    else:
        assert plan == successor.build_plan(
            root=ROOT, implementation_head=successor._git_head(ROOT)
        )
    assert plan["source_count"] == 120
    assert plan["source_bytes"] == 6_627_486_838
    assert plan["coverage_cell_count"] == 140
    assert plan["prelaunch_cell_count"] == 20
    assert plan["interval_count"] == 24
    assert plan["limits"]["maximum_parquet_open_operations"] == 144
    assert plan["limits"]["maximum_parquet_outputs"] == 24
    assert plan["pre_authority_payload_reads"] == 0


def test_exact_semantic_definition_repeats_are_certified_and_preserved(tmp_path: Path) -> None:
    scan = _scan(
        tmp_path / "exact.parquet",
        [_definition_row(ordinal=0, receive=2), _definition_row(ordinal=1, receive=2)],
    )
    certificate = scan.definition_repeat_certificate
    assert certificate is not None
    assert scan.result.duplicate_count == 1
    assert certificate["classification"] == "EXACT_SEMANTIC_DUPLICATES_PRESERVED"
    assert certificate["exact_semantic_duplicate_count"] == 1
    assert certificate["distinct_same_key_update_count"] == 0
    assert certificate["phase1b_rows_preserved_without_deduplication"] is True


def test_distinct_same_key_definition_update_fails_certification(tmp_path: Path) -> None:
    scan = _scan(
        tmp_path / "distinct.parquet",
        [
            _definition_row(ordinal=0, receive=2),
            _definition_row(ordinal=1, receive=2, tick=10_000_000),
        ],
    )
    certificate = scan.definition_repeat_certificate
    assert certificate is not None
    assert certificate["classification"] == "DISTINCT_SAME_KEY_DEFINITION_UPDATES"
    scans = _accepted_scans("EXACT_SEMANTIC_DUPLICATES_PRESERVED")
    scans["definition"] = scan
    assert successor.successor_group_disposition(market="M6E", scans=scans) == (
        "AMBIGUOUS_IDENTITY", False,
    )


def test_group_accepts_only_explicit_exact_definition_repeats() -> None:
    assert successor.successor_group_disposition(
        market="M6E", scans=_accepted_scans("EXACT_SEMANTIC_DUPLICATES_PRESERVED")
    ) == ("ACCEPTED", True)
    scans = _accepted_scans("EXACT_SEMANTIC_DUPLICATES_PRESERVED")
    scans["statistics"] = successor.Phase1BScan(
        _result("statistics", duplicates=1), None
    )
    assert successor.successor_group_disposition(market="M6E", scans=scans) == (
        "DUPLICATE", False,
    )


def test_2025_is_rejected_before_parquet_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = False

    def forbidden_open(path: Path) -> object:
        nonlocal opened
        opened = True
        raise AssertionError(path)

    monkeypatch.setattr(successor.pq, "ParquetFile", forbidden_open)
    with pytest.raises(UnauthorizedOperation, match="outside frozen scope"):
        successor.reconstruct_phase1b_scan(
            source_path=tmp_path / "2025.parquet",
            source={"market": "M6E", "schema": "definition", "year": 2025},
            deadline=time.monotonic() + 10,
        )
    assert opened is False


def test_bounded_parallel_stops_submission_after_first_failure() -> None:
    started: list[int] = []
    preserved: dict[str, int] = {}

    def worker(item: int) -> int:
        started.append(item)
        if item == 0:
            raise RuntimeError("synthetic failure")
        time.sleep(0.05)
        return item

    with pytest.raises(RuntimeError, match="synthetic failure"):
        successor._bounded_parallel(
            items=list(range(8)), worker=worker, key=str, maximum_workers=2,
            result_sink=preserved,
        )
    assert set(started) == {0, 1}
    assert preserved == {"1": 1}


def test_cross_interval_roll_certificate_rejects_retired_identity_reappearance() -> None:
    first = _accepted_scans("NO_LEGACY_REPEATS")
    second = _accepted_scans("NO_LEGACY_REPEATS")
    third = _accepted_scans("NO_LEGACY_REPEATS")
    for scans, sequence in ((first, (10,)), (second, (11,)), (third, (10,))):
        scans["ohlcv-1m"] = successor.Phase1BScan(
            replace(scans["ohlcv-1m"].result, roll_sequence=sequence), None
        )
        scans["ohlcv-1s"] = successor.Phase1BScan(
            replace(scans["ohlcv-1s"].result, roll_sequence=sequence), None
        )
    groups = {
        ("M6E", 2018, "a"): first,
        ("M6E", 2019, "b"): second,
        ("M6E", 2020, "c"): third,
    }
    certificate = successor.cross_interval_roll_certificate(
        market="M6E", groups=groups
    )
    assert certificate["roll_continuity_certified"] is False
    assert certificate["non_contiguous_reappearance_count"] == 1
    assert certificate["raw_instrument_ids_reported"] is False


def test_execution_orders_authority_before_rows_and_has_no_dbn_surface() -> None:
    execution = inspect.getsource(successor.execute_once)
    assert execution.index("authorization.verify(") < execution.index("_scan_one(")
    assert execution.index("authorization.consume(") < execution.index("_scan_one(")
    module = inspect.getsource(successor)
    assert "DBNStore" not in module
    assert "decode_dbn_to_inactive_parquet" not in module
    assert '"dbn_payloads_opened": 0' in execution
    assert '"year_2025_or_2026_payloads_opened": 0' in execution
    assert '"raw_values_or_semantic_keys_reported": False' in execution
