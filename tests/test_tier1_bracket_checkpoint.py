from pathlib import Path
from decimal import Decimal

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_checkpoint import checkpoint_path, load_checkpoint
from futures_rebuild.tier1_bracket_materializer import IndexedBracketEconomics, write_streamed_bracket_chunks


def _rows() -> list[dict[str, object]]:
    return [{
        "event_at_ns": index * 60_000_000_000,
        "open_nano": 100_000_000_000, "high_nano": 105_000_000_000,
        "low_nano": 95_000_000_000, "close_nano": 100_000_000_000,
        "volume": 10, "exchange_session_date": "2021-01-04",
        "actual_identity_hash": "a" * 64, "source_row_sha256": f"{index:064x}",
        "tick_size": "1", "tick_value": "1", "disposition": "ELIGIBLE",
    } for index in range(22)]


def _economics() -> dict[str, IndexedBracketEconomics]:
    return {"a" * 64: IndexedBracketEconomics("a" * 64, Decimal("1"), Decimal("1"), Decimal("1"), "USD", "decimal", "e" * 64)}


def _many_rows(count: int = 82) -> list[dict[str, object]]:
    rows = _rows()
    for index in range(len(rows), count):
        row = dict(rows[0])
        row["event_at_ns"] = index * 60_000_000_000
        row["source_row_sha256"] = f"{index:064x}"
        rows.append(row)
    return rows


def _context() -> dict[str, str]:
    return {"source_parquet_sha256": "a" * 64, "signal_contract_id": "b" * 64}


def test_stream_writer_hashes_chunks_and_resumes_without_duplicate_rows(tmp_path: Path) -> None:
    rows, context = _many_rows(), _context()
    checkpoint = checkpoint_path(root=tmp_path, context=context, market="ES", year=2018)
    first = write_streamed_bracket_chunks(
        batches=(rows,), stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics(),
        stage=tmp_path / "stage", checkpoint=checkpoint, root=tmp_path, context=context,
        chunk_rows=20, stop_after_chunks=1,
    )
    assert first["complete"] is False
    assert first["output_rows"] == 20
    resumed = write_streamed_bracket_chunks(
        batches=(rows[:13], rows[13:]), stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics(),
        stage=tmp_path / "stage", checkpoint=checkpoint, root=tmp_path, context=context, chunk_rows=20,
    )
    assert resumed["complete"] is True
    assert resumed["input_rows"] == len(rows)
    assert resumed["output_rows"] == len(rows)
    assert resumed["carry_rows"] == []
    assert len(resumed["chunks"]) >= 2
    assert all(chunk["row_count"] <= 20 for chunk in resumed["chunks"])
    assert load_checkpoint(path=checkpoint, context=context, root=tmp_path) == resumed


def test_stream_writer_rejects_tampered_chunk_and_order_drift(tmp_path: Path) -> None:
    rows, context = _many_rows(), _context()
    checkpoint = checkpoint_path(root=tmp_path, context=context, market="ES", year=2018)
    write_streamed_bracket_chunks(
        batches=(rows,), stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics(),
        stage=tmp_path / "stage", checkpoint=checkpoint, root=tmp_path, context=context, chunk_rows=20,
    )
    payload = load_checkpoint(path=checkpoint, context=context, root=tmp_path)
    (tmp_path / payload["chunks"][0]["feature_payload"]).write_bytes(b"tampered")
    with pytest.raises(IntegrityError, match="hash"):
        load_checkpoint(path=checkpoint, context=context, root=tmp_path)
    backwards = _many_rows()
    backwards[21]["event_at_ns"] = backwards[20]["event_at_ns"]
    with pytest.raises(IntegrityError, match="order"):
        write_streamed_bracket_chunks(
            batches=(backwards,), stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics(),
            stage=tmp_path / "other", checkpoint=checkpoint_path(root=tmp_path, context={"source_parquet_sha256": "c" * 64}, market="ES", year=2018),
            root=tmp_path, context={"source_parquet_sha256": "c" * 64}, chunk_rows=20,
        )
