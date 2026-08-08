from __future__ import annotations

import json

import pytest

from futures_rebuild.active_phase3_input import load_active_phase3_input
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError


def _write_active_view(boundary, *, eligible: bool = True, altered: bool = False) -> None:
    root = boundary.active_root / "data" / "active" / "causally_gated_normalized" / "ES" / "2019"
    root.mkdir(parents=True)
    parquet = root / "2019.parquet"
    parquet.write_bytes(b"opaque-parquet-test-bytes")
    parquet_hash = sha256_file(parquet)
    if altered:
        parquet_hash = "0" * 64
    payload = {
        "access_policy_binding": {
            "active_view_id": "a" * 64,
            "capability": "RESEARCH_READY_CAUSAL_PRICE",
            "market": "ES",
            "permitted_uses": ["DISCOVERY_RESEARCH"],
            "selection_eligible": eligible,
            "year": 2019,
        },
        "entry_binding": {
            "disposition": "RESEARCH_READY_CAUSAL_PRICE",
            "market": "ES",
            "parquet_path": "data/active/causally_gated_normalized/ES/2019/2019.parquet",
            "parquet_sha256": parquet_hash,
            "source_bindings": [
                {"causal_release_id": "b" * 64, "raw_release_id": "c" * 64}
            ],
            "year": 2019,
        },
        "schema_version": "causal_active_market_year_manifest/1.0.0",
    }
    (root / "2019.parquet.manifest.json").write_text(json.dumps(payload), encoding="utf-8")


def test_loads_metadata_only_active_phase3_input(boundary) -> None:
    _write_active_view(boundary)

    result = load_active_phase3_input(boundary=boundary, market="ES", year=2019)

    assert result.market == "ES"
    assert result.year == 2019
    assert result.causal_release_id == "b" * 64
    assert result.source_raw_release_id == "c" * 64
    assert result.input_id == result.input_id


@pytest.mark.parametrize("eligible,altered", [(False, False), (True, True)])
def test_rejects_ineligible_or_altered_active_view(boundary, eligible, altered) -> None:
    _write_active_view(boundary, eligible=eligible, altered=altered)

    with pytest.raises(IntegrityError):
        load_active_phase3_input(boundary=boundary, market="ES", year=2019)


def test_rejects_different_market_or_year(boundary) -> None:
    _write_active_view(boundary)

    with pytest.raises(IntegrityError):
        load_active_phase3_input(boundary=boundary, market="ES", year=2020)
