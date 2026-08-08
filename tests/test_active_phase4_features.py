from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild.active_phase3_input import ActivePhase3Input
from futures_rebuild.active_phase4_features import (
    ActivePhase4FeatureBinding,
    FEATURE_NAMES,
    build_active_phase4_features,
)
from futures_rebuild.errors import IntegrityError


def _binding(path: str, *, market: str = "ES", year: int = 2019) -> ActivePhase4FeatureBinding:
    active = ActivePhase3Input(market, year, path, "a" * 64, "b" * 64, "c" * 64, "d" * 64, "e" * 64)
    return ActivePhase4FeatureBinding(
        active_input=active,
        input_record_path="manifests/phase3_inputs/cf850301855f9763888ababd50a3400bd2e28e73be698ea3c16f06700717630a.json",
        input_record_sha256="f" * 64,
        feature_spec_path="configs/mechanical_feature_spec.json",
        feature_spec_sha256="0" * 64,
        feature_spec={"feature_names": list(FEATURE_NAMES)},
    )


def _write_rows(
    boundary, *, available_at_ns: list[int], market: str = "ES", year: int = 2019
) -> str:
    path = boundary.active_root / f"data/active/causally_gated_normalized/{market}/{year}/{year}.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "market": [market, market, market],
                "event_at_ns": [100, 200, 300],
                "available_at_ns": available_at_ns,
                "open_nano": [100, 100, 100],
                "high_nano": [110, 110, 110],
                "low_nano": [90, 90, 90],
                "close_nano": [105, 95, 100],
                "volume": [7, 8, 9],
                "disposition": ["ELIGIBLE", "ELIGIBLE", "ELIGIBLE"],
                "actual_identity_hash": ["a" * 64, "a" * 64, "a" * 64],
                "exchange_session_date": ["2019-01-01", "2019-01-01", "2019-01-01"],
                "source_row_sha256": ["b" * 64, "c" * 64, "d" * 64],
            }
        ),
        path,
    )
    return path.relative_to(boundary.active_root).as_posix()


def test_builds_causal_feature_rows(boundary) -> None:
    source = _write_rows(boundary, available_at_ns=[100, 201, 299])

    result = build_active_phase4_features(boundary=boundary, binding=_binding(source))

    report = json.loads((boundary.active_root / result["report_path"]).read_text(encoding="utf-8"))
    assert result["feature_count"] == 3
    assert report["feature_ready_count"] == 2
    assert report["unavailable_or_ineligible_count"] == 1
    assert report["feature_names"] == list(FEATURE_NAMES)
    assert report["decision_time_basis"] == "first_minute_boundary_at_or_after_available_at_ns"
    assert report["model_fitting"] is False
    rows = pq.read_table(
        boundary.active_root / "data/features/active_es_mechanical_v3/ES/2019/2019" / result["release_id"] / "features.parquet"
    ).to_pylist()
    assert rows[0]["status"] == "FEATURE_READY"
    assert rows[0]["bar_return"] == pytest.approx(0.05)
    assert rows[1]["status"] == "FEATURE_READY"
    assert rows[1]["decision_at_ns"] == 60_000_000_000
    assert rows[1]["planned_entry_at_ns"] == 120_000_000_000
    assert rows[2]["status"] == "UNAVAILABLE_OR_INELIGIBLE"
    assert rows[2]["volume"] is None


def test_builds_non_es_feature_rows_into_the_bound_market_year(boundary) -> None:
    source = _write_rows(
        boundary, available_at_ns=[100, 201, 299], market="CL", year=2020
    )

    result = build_active_phase4_features(
        boundary=boundary, binding=_binding(source, market="CL", year=2020)
    )

    target = (
        boundary.active_root
        / "data/features/active_es_mechanical_v3/CL/2020/2020"
        / result["release_id"]
        / "features.parquet"
    )
    assert target.is_file()


def test_rejects_missing_required_columns(boundary) -> None:
    path = boundary.active_root / "data/active/causally_gated_normalized/ES/2019/2019.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.table({"market": ["ES"]}), path)

    with pytest.raises(IntegrityError, match="missing required columns"):
        build_active_phase4_features(
            boundary=boundary,
            binding=_binding(path.relative_to(boundary.active_root).as_posix()),
        )
