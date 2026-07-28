from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild.active_data_certification import (
    _process_memory_bytes,
    canonical_parquet_fingerprint,
    canonical_parquet_sequence_fingerprint,
    compare_parquet_canonical,
    validate_causal_invariants,
    validate_aggregation_crosschecks,
)
from futures_rebuild.active_data_view import materialize_parquet
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError
from futures_rebuild.foundation.parquet import CAUSAL_BAR_SCHEMA


@pytest.mark.skipif(os.name != "nt", reason="Windows process-memory regression")
def test_windows_process_memory_measurement_uses_pointer_safe_signatures() -> None:
    measurement = _process_memory_bytes()

    assert measurement["working_set_bytes"] > 0
    assert measurement["peak_working_set_bytes"] >= measurement["working_set_bytes"]


def _row(event_at_ns: int, *, disposition: str = "ELIGIBLE") -> dict[str, object]:
    failure = None if disposition == "ELIGIBLE" else "MISSING_DEFINITION"
    identity = "a" * 64 if disposition == "ELIGIBLE" else None
    session = "2022-01-03" if disposition == "ELIGIBLE" else None
    return {
        "dataset": "GLBX.MDP3",
        "market": "ES",
        "publisher_id": 1,
        "instrument_id": 10,
        "instrument_id_date_utc": "2022-01-03",
        "event_at_ns": event_at_ns,
        "available_at_ns": event_at_ns + 61_000_000_000,
        "resolution_as_of_ns": event_at_ns + 60_000_000_000,
        "open_nano": 100,
        "high_nano": 120,
        "low_nano": 90,
        "close_nano": 110,
        "volume": 4,
        "availability_basis": "MODELED_INTERVAL_END_PLUS_PINNED_LATENCY",
        "availability_policy_hash": "1" * 64,
        "foundation_policy_set_id": "2" * 64,
        "provider_timestamp_epoch_id": "GLBX_MDP3_CAPTURE_TIME",
        "source_raw_release_id": "3" * 64,
        "source_release_id": "4" * 64,
        "source_manifest_sha256": "5" * 64,
        "source_file_path": "dbn/ohlcv_1m/ES/2022/source.dbn.zst",
        "source_file_sha256": "6" * 64,
        "source_row_sha256": "7" * 64,
        "disposition": disposition,
        "prediction_in_coverage_denominator": True,
        "failure_code": failure,
        "failure_detail_sha256": "8" * 64 if failure else None,
        "actual_identity_hash": identity,
        "exchange_session_date": session,
        "raw_symbol": "ESH2" if identity else None,
        "exchange": "XCME" if identity else None,
        "definition_release_id": "9" * 64 if identity else None,
        "definition_manifest_sha256": "a" * 64 if identity else None,
        "definition_row_sha256": "b" * 64 if identity else None,
        "definition_ts_event_ns": event_at_ns - 1 if identity else None,
        "definition_ts_recv_ns": event_at_ns - 1 if identity else None,
        "definition_index_date_utc": "2022-01-03" if identity else None,
        "definition_activation_ns": event_at_ns - 2 if identity else None,
        "definition_expiration_ns": event_at_ns + 1_000 if identity else None,
        "definition_security_update_action": "ADD" if identity else None,
        "definition_instrument_class": "FUTURE" if identity else None,
        "definition_security_type": "FUT" if identity else None,
        "definition_source_row_ordinal": 1 if identity else None,
        "currency": "USD" if identity else None,
        "point_value": "50" if identity else None,
        "tick_size": "0.25" if identity else None,
        "tick_value": "12.5" if identity else None,
        "quote_convention": "PRICE" if identity else None,
        "economics_rulebook_hash": "c" * 64 if identity else None,
        "provider_unit_qty_state": "KNOWN" if identity else None,
    }


def _write(path: Path, rows: list[dict[str, object]], *, row_group_size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=CAUSAL_BAR_SCHEMA),
        path,
        compression="zstd",
        row_group_size=row_group_size,
    )


def test_canonical_fingerprint_is_independent_of_parquet_row_groups(
    tmp_path: Path,
) -> None:
    rows = [_row(1), _row(2), _row(3, disposition="UNRESOLVED_FAIL_CLOSED")]
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    _write(first, rows, row_group_size=1)
    _write(second, rows, row_group_size=3)

    left = canonical_parquet_fingerprint(first, batch_rows=2)
    right = canonical_parquet_fingerprint(second, batch_rows=2)

    assert left["canonical_row_hash"] == right["canonical_row_hash"]
    assert left["schema_fingerprint"] == right["schema_fingerprint"]
    assert left["row_count"] == right["row_count"] == 3
    compared = compare_parquet_canonical(
        first,
        second,
        expected_schema=CAUSAL_BAR_SCHEMA,
        batch_rows=2,
    )
    assert compared["row_count"] == 3
    assert compared["container_hash_equal"] is False


def test_sequence_fingerprint_matches_deterministic_split_materialization(
    tmp_path: Path,
) -> None:
    first = tmp_path / "source-a.parquet"
    second = tmp_path / "source-b.parquet"
    output = tmp_path / "out" / "2022.parquet"
    _write(first, [_row(1), _row(2)], row_group_size=1)
    _write(second, [_row(3), _row(4)], row_group_size=2)
    expected = canonical_parquet_sequence_fingerprint(
        (first, second), batch_rows=2
    )

    _, row_hash, row_count = materialize_parquet(
        sources=(first, second),
        source_sha256s=(sha256_file(first), sha256_file(second)),
        destination=output,
        expected_row_count=4,
        expected_schema_fingerprint=str(expected["schema_fingerprint"]),
        batch_rows=2,
    )

    assert row_count == 4
    assert row_hash == expected["canonical_row_hash"]


def test_causal_invariants_preserve_fail_closed_rows_in_denominator(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bars.parquet"
    _write(
        path,
        [_row(1), _row(2, disposition="UNRESOLVED_FAIL_CLOSED")],
        row_group_size=2,
    )

    result = validate_causal_invariants(path, batch_rows=2)

    assert result["row_count"] == 2
    assert result["prediction_in_coverage_denominator_rows"] == 2
    assert result["eligible_rows"] == 1
    assert result["failure_rows"] == 1


@pytest.mark.parametrize(
    ("change", "match"),
    [
        ({"high_nano": 80}, "OHLC"),
        ({"available_at_ns": 1}, "timestamps"),
    ],
)
def test_causal_invariants_reject_invalid_market_data(
    tmp_path: Path, change: dict[str, object], match: str
) -> None:
    row = _row(1)
    row.update(change)
    path = tmp_path / "bad.parquet"
    _write(path, [row], row_group_size=1)

    with pytest.raises(IntegrityError, match=match):
        validate_causal_invariants(path)


def test_split_materialization_rejects_overlap_and_source_tampering(
    tmp_path: Path,
) -> None:
    first = tmp_path / "source-a.parquet"
    second = tmp_path / "source-b.parquet"
    _write(first, [_row(1), _row(2)], row_group_size=2)
    _write(second, [_row(2), _row(3)], row_group_size=2)
    expected = canonical_parquet_fingerprint(first)
    with pytest.raises(IntegrityError, match="overlap"):
        materialize_parquet(
            sources=(first, second),
            source_sha256s=(sha256_file(first), sha256_file(second)),
            destination=tmp_path / "overlap.parquet",
            expected_row_count=4,
            expected_schema_fingerprint=str(expected["schema_fingerprint"]),
        )
    with pytest.raises(IntegrityError, match="source changed"):
        materialize_parquet(
            sources=(first,),
            source_sha256s=("0" * 64,),
            destination=tmp_path / "tampered.parquet",
            expected_row_count=2,
            expected_schema_fingerprint=str(expected["schema_fingerprint"]),
        )


def test_missing_predeclared_aggregation_source_is_not_an_automatic_pass(
    tmp_path: Path,
) -> None:
    path = tmp_path / "raw.parquet"
    # No payload is read when the exact aggregate source family is unavailable.
    result = validate_aggregation_crosschecks(
        raw_bars_path=path,
        dbn_release=object(),  # type: ignore[arg-type]
        market="ES",
        aggregation_sources=(),
        batch_rows=100,
    )

    assert result == {
        "reason": "NO_OVERLAPPING_PROVIDER_AGGREGATE_BOUND_BY_PLAN",
        "state": "NOT_AVAILABLE",
    }
