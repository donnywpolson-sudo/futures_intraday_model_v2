from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild.active_phase5_splits import ReleasePair, _schedule, _session_counts
from futures_rebuild.errors import IntegrityError


def test_schedule_freezes_the_required_eight_fold_windows() -> None:
    sessions = tuple(f"session-{index:04d}" for index in range(1_200))

    folds = _schedule(sessions)

    assert len(folds) == 8
    assert folds[0]["outer_test_session_dates"] == [sessions[674], sessions[736]]
    assert len(folds[0]["inner_validation_session_dates"]) == 4
    assert folds[-1]["outer_test_session_dates"] == [sessions[1115], sessions[1177]]


def test_schedule_rejects_insufficient_observed_sessions() -> None:
    with pytest.raises(IntegrityError, match="too short"):
        _schedule(tuple(f"session-{index:04d}" for index in range(1_177)))


def test_session_counts_uses_only_mature_feature_ready_rows(tmp_path) -> None:
    feature_path = tmp_path / "features.parquet"
    outcome_path = tmp_path / "outcomes.parquet"
    pq.write_table(
        pa.table(
            {
                "bar_event_at_ns": [100, 420, 780],
                "decision_at_ns": [120, 480, 840],
                "label_unlock_at_ns": [420, 780, 1140],
                "planned_entry_at_ns": [180, 540, 900],
                "status": ["FEATURE_READY", "FEATURE_READY", "UNAVAILABLE_OR_INELIGIBLE"],
                "actual_identity_hash": ["a" * 64, "a" * 64, None],
                "exchange_session_date": ["2020-01-02", "2020-01-02", None],
                "upstream_source_row_sha256": ["1" * 64, "2" * 64, "3" * 64],
            }
        ),
        feature_path,
    )
    pq.write_table(
        pa.table(
            {
                "source_bar_event_at_ns": [100, 420, 780],
                "decision_at_ns": [120, 480, 840],
                "entry_at_ns": [60_000_000_120, 60_000_000_480, 60_000_000_840],
                "label_unlock_at_ns": [420, 780, 1140],
                "status": ["MATURED", "MATURED", "MISSING_SOURCE"],
                "actual_identity_hash": ["a" * 64, "a" * 64, None],
                "exchange_session_date": ["2020-01-02", "2020-01-02", None],
                "upstream_source_row_sha256": ["1" * 64, "2" * 64, "3" * 64],
            }
        ),
        outcome_path,
    )
    pair = ReleasePair("ES", 2020, "b" * 64, "c" * 64, outcome_path, feature_path, "d" * 64)

    progress: list[str] = []

    assert _session_counts((pair,), progress=progress.append) == {"2020-01-02": 2}
    assert progress == ["verified ES-2020"]
