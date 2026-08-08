from __future__ import annotations

import json

import pytest

from futures_rebuild.active_phase3_input import load_active_phase3_input
from futures_rebuild.active_phase3_validation import (
    ActivePhase3MechanicsValidation,
    prepare_active_phase3_mechanics_validation,
)
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import ContractError, IntegrityError



def _write_active_view(boundary) -> None:
    root = boundary.active_root / "data" / "active" / "causally_gated_normalized" / "ES" / "2019"
    root.mkdir(parents=True)
    parquet = root / "2019.parquet"
    parquet.write_bytes(b"opaque-parquet-test-bytes")
    payload = {
        "access_policy_binding": {
            "active_view_id": "a" * 64,
            "capability": "RESEARCH_READY_CAUSAL_PRICE",
            "market": "ES",
            "permitted_uses": ["DISCOVERY_RESEARCH"],
            "selection_eligible": True,
            "year": 2019,
        },
        "entry_binding": {
            "disposition": "RESEARCH_READY_CAUSAL_PRICE",
            "market": "ES",
            "parquet_path": "data/active/causally_gated_normalized/ES/2019/2019.parquet",
            "parquet_sha256": sha256_file(parquet),
            "source_bindings": [
                {"causal_release_id": "b" * 64, "raw_release_id": "c" * 64}
            ],
            "year": 2019,
        },
        "schema_version": "causal_active_market_year_manifest/1.0.0",
    }
    (root / "2019.parquet.manifest.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_input_record(boundary) -> str:
    active = load_active_phase3_input(boundary=boundary, market="ES", year=2019)
    relative = (
        "manifests/phase3_inputs/"
        "cf850301855f9763888ababd50a3400bd2e28e73be698ea3c16f06700717630a.json"
    )
    path = boundary.active_root / relative
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(
            {
                "active_view_id": active.active_view_id,
                "causal_release_id": active.causal_release_id,
                "input_id": active.input_id,
                "market": active.market,
                "parquet_path": active.parquet_path,
                "parquet_sha256": active.parquet_sha256,
                "schema_version": "phase3_active_input_record/1.0.0",
                "sidecar_sha256": active.sidecar_sha256,
                "source_raw_release_id": active.source_raw_release_id,
                "year": active.year,
            }
        ),
        encoding="utf-8",
    )
    return relative


def test_prepares_exact_metadata_only_mechanics_validation(boundary) -> None:
    _write_active_view(boundary)
    relative = _write_input_record(boundary)

    result = prepare_active_phase3_mechanics_validation(
        boundary=boundary, input_record_path=relative
    )

    assert result.maximum_row_reads == 0
    assert result.entry_delay_seconds == 60
    assert result.label_horizon_seconds == 300
    assert result.input_record_sha256 == sha256_file(boundary.active_root / relative)
    assert result.validation_id == result.validation_id


def test_rejects_input_record_that_does_not_match_verified_active_view(boundary) -> None:
    _write_active_view(boundary)
    relative = _write_input_record(boundary)
    path = boundary.active_root / relative
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["year"] = 2020
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(IntegrityError, match="differs"):
        prepare_active_phase3_mechanics_validation(
            boundary=boundary, input_record_path=relative
        )


def test_rejects_any_row_read_budget(boundary) -> None:
    _write_active_view(boundary)
    relative = _write_input_record(boundary)
    active = load_active_phase3_input(boundary=boundary, market="ES", year=2019)

    with pytest.raises(ContractError, match="metadata-only"):
        ActivePhase3MechanicsValidation(
            active_input=active,
            input_record_path=relative,
            input_record_sha256=sha256_file(boundary.active_root / relative),
            maximum_row_reads=1,
        )
