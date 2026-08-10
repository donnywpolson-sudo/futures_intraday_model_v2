from __future__ import annotations

import inspect
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild import micro_alpha_phase1b2_decoder as decoder
from futures_rebuild import micro_alpha_phase1b2_definition_duplicate_diagnostic as diagnostic
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.research_gateway_policy import (
    PREPARATORY_REAL_HISTORY_OPERATIONS,
    require_current_real_history_operation,
)


ROOT = Path(__file__).resolve().parents[1]


def _row(*, ordinal: int, receive: int, tick: int = 5_000_000) -> dict[str, object]:
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
        "market": "M6E",
        "schema": "definition",
        "year": 2018,
        "source_sha256": "a" * 64,
        "sha256": "b" * 64,
        "bytes": path.stat().st_size,
    }


def _classify(path: Path, rows: list[dict[str, object]]) -> dict[str, int | str]:
    source = _write_definition(path, rows)
    return diagnostic.classify_definition_repeats(
        source_path=path,
        source=source,
        deadline=time.monotonic() + 10,
    )


def test_operation_is_exactly_allowlisted() -> None:
    assert diagnostic.OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS
    require_current_real_history_operation(diagnostic.OPERATION, {})


def test_prepare_surface_cannot_execute_rows() -> None:
    from scripts import prepare_apex_micro_phase1b2_definition_duplicate_diagnostic_v3 as prepare

    source = inspect.getsource(prepare)
    assert "execute_once" not in source
    assert '"execute"' not in source
    assert '"preview-plan"' in source
    assert '"write-audit"' in source


def test_plan_build_is_stat_only_for_definition_source() -> None:
    source = inspect.getsource(diagnostic.build_plan)
    assert "pq.ParquetFile" not in source
    assert "iter_batches" not in source
    assert "sha256_file(source_path)" not in source
    plan = diagnostic.build_plan(root=ROOT, implementation_head=diagnostic._git_head(ROOT))
    assert plan["source_count"] == 1
    assert plan["source_bytes"] == 68_274
    assert plan["pre_authority_payload_reads"] == 0
    assert plan["diagnostic_only"] is True


def test_exact_semantic_duplicate_is_distinguished_from_lineage(tmp_path: Path) -> None:
    result = _classify(
        tmp_path / "exact.parquet",
        [_row(ordinal=0, receive=2), _row(ordinal=1, receive=2)],
    )
    assert result == {
        "row_count": 2,
        "legacy_repeat_count": 1,
        "exact_semantic_duplicate_count": 1,
        "distinct_same_key_update_count": 0,
        "classification": "EXACT_SEMANTIC_DUPLICATES",
    }


def test_distinct_same_key_update_is_not_called_exact_duplicate(tmp_path: Path) -> None:
    result = _classify(
        tmp_path / "update.parquet",
        [_row(ordinal=0, receive=2), _row(ordinal=1, receive=2, tick=10_000_000)],
    )
    assert result["exact_semantic_duplicate_count"] == 0
    assert result["distinct_same_key_update_count"] == 1
    assert result["classification"] == "LEGACY_KEY_CONFLATES_DISTINCT_DEFINITION_UPDATES"


def test_mixed_repeat_classification_is_explicit(tmp_path: Path) -> None:
    result = _classify(
        tmp_path / "mixed.parquet",
        [
            _row(ordinal=0, receive=2),
            _row(ordinal=1, receive=2),
            _row(ordinal=2, receive=2, tick=10_000_000),
        ],
    )
    assert result["legacy_repeat_count"] == 2
    assert result["exact_semantic_duplicate_count"] == 1
    assert result["distinct_same_key_update_count"] == 1
    assert result["classification"] == "MIXED_EXACT_DUPLICATES_AND_DISTINCT_UPDATES"


def test_2025_is_rejected_before_parquet_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    opened = False

    def forbidden_open(path: Path) -> object:
        nonlocal opened
        opened = True
        raise AssertionError(path)

    monkeypatch.setattr(diagnostic.pq, "ParquetFile", forbidden_open)
    with pytest.raises(UnauthorizedOperation, match="outside frozen scope"):
        diagnostic.classify_definition_repeats(
            source_path=tmp_path / "2025.parquet",
            source={"market": "M6E", "schema": "definition", "year": 2025},
            deadline=time.monotonic() + 10,
        )
    assert opened is False


def test_authorization_is_consumed_before_classifier_and_output_is_price_free() -> None:
    source = inspect.getsource(diagnostic.execute_once)
    assert source.index("authorization.verify(") < source.index("classify_definition_repeats(")
    assert source.index("authorization.consume(") < source.index("classify_definition_repeats(")
    assert "pq.write_table" not in source
    assert '"parquets_created": 0' in source
    assert '"raw_values_or_semantic_keys_reported": False' in source
    assert '"year_2025_or_2026_payloads_opened": 0' in source
