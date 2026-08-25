from __future__ import annotations

import os
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.causal_observation_parquet import (
    FILENAMES,
    _parquet_io_path,
    read_bundle,
    read_table,
    write_bundle,
    write_table,
)
from futures_rebuild.errors import IntegrityError


H = "a" * 64
pytestmark = pytest.mark.current


def _tables() -> dict[str, list[dict[str, object]]]:
    observation_core = {
        "market": "ES",
        "source_contract_id": H,
        "source_release_id": H,
        "source_file_path": "synthetic/ohlcv_1m.dbn.zst",
        "source_file_sha256": H,
        "source_row_sha256": H,
        "source_cadence": "1m",
        "bar_start_ns": 1_000_000_000,
        "bar_end_ns": 61_000_000_000,
        "source_timestamp_ns": 1_000_000_000,
        "available_at_ns": 66_000_000_000,
        "decision_eligible_at_ns": 66_000_000_000,
        "publisher_id": 1,
        "instrument_id": 2,
        "raw_symbol": "ESH5",
        "actual_contract": "ESH5",
        "definition_source_file_path": "synthetic/definition.dbn.zst",
        "definition_source_file_sha256": H,
        "definition_row_sha256": H,
        "definition_event_at_ns": 1,
        "definition_received_at_ns": 2,
        "listing_activation_ns": 0,
        "expiration_ns": 2**64 - 1,
        "open_nano": -100,
        "high_nano": 0,
        "low_nano": -200,
        "close_nano": -50,
        "volume": 5,
        "currency": "USD",
        "min_price_increment_nano": 25,
        "multiplier_nano": 50,
        "project_session_id": "ES:1970-01-01",
        "project_trade_date": "1970-01-01",
        "project_grouping_start_ns": 0,
        "project_grouping_end_ns": 86_400_000_000_000,
        "project_timezone": "America/Chicago",
        "official_schedule_state": "UNKNOWN_FAIL_CLOSED",
    }
    row_id = sha256_json(observation_core)
    observation = {"row_id": row_id, **observation_core}
    evidence_sha = sha256_json(
        {
            "market": "ES",
            "source_row_sha256": H,
            "interval_start_ns": 1_000_000_000,
            "interval_end_ns": 61_000_000_000,
            "authority": "DECODED_SOURCE_ROW",
        }
    )
    missing_core = {
        "observation_row_id": row_id,
        "market": "ES",
        "interval_start_ns": 1_000_000_000,
        "interval_end_ns": 61_000_000_000,
        "state": "OBSERVED_VALID",
        "authority": "DECODED_SOURCE_ROW",
        "evidence_sha256": evidence_sha,
    }
    roll_causal = sha256_json(
        {
            "definition_row_sha256": H,
            "definition_received_at_ns": 2,
            "prior_contract": "ESH5",
        }
    )
    cadence_core = {
        "row_id": row_id,
        "source_cadence": "1m",
        "comparison_cadence": "1s",
        "interval_boundary_compatible": True,
        "result": "SOURCE_MISSING",
        "exception_state": "REFERENCE_ABSENT",
    }
    return {
        "observations": [observation],
        "missingness": [{"evidence_id": sha256_json(missing_core), **missing_core}],
        "roll": [{
            "row_id": row_id,
            "actual_contract_before": "ESH5",
            "actual_contract_after": "ESH5",
            "effective_time_ns": None,
            "causal_selection_evidence_sha256": roll_causal,
            "roll_flag": False,
            "price_discontinuity_flag": False,
            "crossing_status": "NO_CROSSING",
        }],
        "quality": [{
            "row_id": row_id,
            "row_identity_sha256": row_id,
            "ohlc_valid": True,
            "volume_valid": True,
            "timestamp_order_valid": True,
            "duplicate_state": "UNIQUE",
            "source_contract_id": H,
            "source_release_id": H,
            "source_file_sha256": H,
            "quality_flags": ["PROVIDER_VALID_NEGATIVE_PRICE"],
        }],
        "cadence": [{"comparison_id": sha256_json(cadence_core), **cadence_core}],
    }


def test_five_table_parquet_round_trip_is_exact_and_deterministic(tmp_path: Path) -> None:
    tables = _tables()
    first = tmp_path / "first"
    second = tmp_path / "second"
    write_bundle(first, tables=tables)
    write_bundle(second, tables=tables)
    assert read_bundle(first) == tables
    for name, filename in FILENAMES.items():
        assert sha256_file(first / filename) == sha256_file(second / filename)
        assert pq.ParquetFile(first / filename).metadata.num_row_groups == 1


def test_monthly_row_groups_and_create_only_behavior(tmp_path: Path) -> None:
    row = _tables()["observations"][0]
    second = dict(row)
    second["bar_start_ns"] = int(row["bar_start_ns"]) + 60_000_000_000
    second["bar_end_ns"] = int(row["bar_end_ns"]) + 60_000_000_000
    second["source_timestamp_ns"] = second["bar_start_ns"]
    second["available_at_ns"] = int(second["bar_end_ns"]) + 5_000_000_000
    second["decision_eligible_at_ns"] = second["available_at_ns"]
    second_core = {key: value for key, value in second.items() if key != "row_id"}
    second["row_id"] = sha256_json(second_core)
    path = tmp_path / "observations.parquet"
    write_table(path, name="observations", row_groups=([row], [second]))
    assert pq.ParquetFile(path).metadata.num_row_groups == 2
    assert read_table(path, name="observations") == [row, second]
    with pytest.raises(IntegrityError, match="already exists"):
        write_table(path, name="observations", row_groups=([row],))


def test_truncated_parquet_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "observations.parquet"
    write_table(path, name="observations", row_groups=(_tables()["observations"],))
    raw = path.read_bytes()
    path.write_bytes(raw[:-8])
    with pytest.raises(IntegrityError, match="unreadable"):
        read_table(path, name="observations")


@pytest.mark.skipif(os.name != "nt", reason="Windows extended-length path contract")
def test_exact_bounded_2025_layout_supports_265_character_parquet_path(
    tmp_path: Path,
) -> None:
    suffix = Path(
        "2025/2025-07-01_2025-07-13T220000Z/candidate/observations.parquet"
    )
    prefix = tmp_path.resolve()
    padding_length = 265 - len(str(prefix / suffix)) - 1
    assert 0 < padding_length < 256
    directory = prefix / ("x" * padding_length) / suffix.parent
    path = directory / suffix.name
    assert len(str(path)) == 265

    write_bundle(directory, tables=_tables())

    assert read_bundle(directory) == _tables()
    assert Path(_parquet_io_path(path)).is_file()
