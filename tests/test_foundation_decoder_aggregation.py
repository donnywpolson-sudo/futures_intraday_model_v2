from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from futures_rebuild.errors import ContractError, IntegrityError
from futures_rebuild.foundation import decoder
from futures_rebuild.source_symbology import build_query_contract


def _boundary_ns(value: str) -> int:
    return int(
        datetime.fromisoformat(value)
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1_000_000_000
    )


def _decode_chunks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    requested_schema: str,
    metadata_schema: str | None = None,
    dtype_names: tuple[str, ...] = decoder.OHLCV_DTYPE,
) -> list[np.ndarray]:
    source = tmp_path / "2010-06-06_2011-01-01.dbn.zst"
    source.write_bytes(b"synthetic-offline-dbn")
    rows = np.zeros(1, dtype=[(name, np.int64) for name in dtype_names])
    metadata = SimpleNamespace(
        dataset=decoder.DATASET,
        schema=metadata_schema or requested_schema,
        stype_out="instrument_id",
        ts_out=False,
        limit=None,
        start=_boundary_ns("2010-06-06"),
        end=_boundary_ns("2011-01-01"),
        stype_in="continuous",
        symbols=["6A.v.0"],
    )
    store = SimpleNamespace(
        metadata=metadata,
        to_ndarray=lambda *, count: rows,
    )
    monkeypatch.setattr(
        decoder.databento,
        "DBNStore",
        SimpleNamespace(from_file=lambda path: store),
    )
    binding = SimpleNamespace(
        relative_path=(
            f"dbn/{requested_schema.replace('-', '_')}/6A/2010/"
            "2010-06-06_2011-01-01.dbn.zst"
        ),
        verify=lambda: source,
    )
    query = build_query_contract(
        schema=requested_schema,
        market="6A",
        start="2010-06-06",
        end="2011-01-01",
        stype_in="continuous",
        symbols=["6A.v.0"],
    )
    return list(
        decoder._chunks(
            binding,
            schema=requested_schema,
            market="6A",
            expected_query_contract=query,
            batch_rows=100,
        )
    )


@pytest.mark.parametrize("schema", ("ohlcv-1h", "ohlcv-1d"))
def test_pinned_aggregate_schemas_decode_offline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema: str
) -> None:
    chunks = _decode_chunks(
        tmp_path,
        monkeypatch,
        requested_schema=schema,
    )

    assert len(chunks) == 1
    assert chunks[0].dtype.names == decoder.OHLCV_DTYPE


@pytest.mark.parametrize("schema", ("ohlcv-1h", "ohlcv-1d"))
def test_pinned_aggregate_schemas_reject_dtype_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, schema: str
) -> None:
    with pytest.raises(IntegrityError, match="dtype"):
        _decode_chunks(
            tmp_path,
            monkeypatch,
            requested_schema=schema,
            dtype_names=decoder.OHLCV_DTYPE[:-1],
        )


def test_pinned_aggregate_schema_rejects_metadata_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(IntegrityError, match="metadata"):
        _decode_chunks(
            tmp_path,
            monkeypatch,
            requested_schema="ohlcv-1h",
            metadata_schema="ohlcv-1d",
        )


def test_unapproved_ohlcv_schema_remains_unsupported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(ContractError, match="unsupported"):
        _decode_chunks(
            tmp_path,
            monkeypatch,
            requested_schema="ohlcv-1s",
        )
