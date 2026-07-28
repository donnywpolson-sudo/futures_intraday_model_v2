from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from futures_rebuild.active_data_full_plan import (
    _correctness_projection,
    _planned_candidates,
)
from futures_rebuild.active_data_plan import derive_inventory
from futures_rebuild.active_data_view import CERTIFICATION_STATE


FOUNDATION_RELEASE_ID = (
    "637f16b3c23c9f2215858f49754965738fe9c00095661d7a29d6877d566ae5e3"
)


def _root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_correctness_projection_ignores_only_nondeterministic_measurements() -> None:
    first = {
        "certification_report_id": "a" * 64,
        "interval_reports": [
            {
                "canonical_causal": {"canonical_row_hash": "c" * 64},
                "interval_report_id": "b" * 64,
                "measurements": {"dbn_decode_seconds": "1.000000000"},
                "status": "PASS",
            }
        ],
        "measurements": {"certification_seconds": "2.000000000"},
        "status": "PASS",
    }
    second = deepcopy(first)
    second["certification_report_id"] = "d" * 64
    second["measurements"]["certification_seconds"] = "3.000000000"
    second["interval_reports"][0]["interval_report_id"] = "e" * 64
    second["interval_reports"][0]["measurements"]["dbn_decode_seconds"] = (
        "2.000000000"
    )
    assert _correctness_projection(first) == _correctness_projection(second)
    second["interval_reports"][0]["canonical_causal"]["canonical_row_hash"] = (
        "f" * 64
    )
    assert _correctness_projection(first) != _correctness_projection(second)


def test_full_planning_binds_available_aggregation_sources_explicitly() -> None:
    root = _root()
    inventory = derive_inventory(
        repository_root=root,
        foundation_release_id=FOUNDATION_RELEASE_ID,
    )
    entries = [
        entry
        for entry in inventory["entries"]
        if (entry["market"], entry["year"]) in {("6A", 2010), ("ES", 2022)}
    ]
    planned, source_objects = _planned_candidates(
        root=root,
        inventory_entries=entries,
    )
    assert len(planned) == 2
    assert all(entry["disposition"] == CERTIFICATION_STATE for entry in planned)
    for entry in planned:
        assert entry["source_ceiling"]["maximum_source_files"] > 0
        for interval in entry["intervals"]:
            assert interval["aggregation_expectations"] == {
                "ohlcv-1d": "REQUIRED",
                "ohlcv-1h": "REQUIRED",
            }
            assert {source["schema"] for source in interval["aggregation_sources"]} == {
                "ohlcv-1d",
                "ohlcv-1h",
            }
    paths = [item["path"] for item in source_objects]
    assert paths == sorted(set(paths))
