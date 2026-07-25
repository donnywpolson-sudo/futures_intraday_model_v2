from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_file
from futures_rebuild.successor_inventory import (
    SuccessorInventoryError,
    build_inventory,
)


def _fixture(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    dbn = root / "data" / "dbn" / "ohlcv_1m" / "ES" / "2024" / "2024.dbn.zst"
    dbn.parent.mkdir(parents=True)
    dbn.write_bytes(b"synthetic-dbn")
    sidecar = Path(f"{dbn}.manifest.json")
    sidecar.write_text(
        json.dumps(
            {
                "dataset": "GLBX.MDP3",
                "vendor": "databento",
                "request_status": "ok",
                "schema": "ohlcv-1m",
                "market": "ES",
                "path": "data/dbn/ohlcv_1m/ES/2024/2024.dbn.zst",
                "file_sha256": sha256_file(dbn),
                "file_size_bytes": dbn.stat().st_size,
                "symbols_requested": ["ES.v.0"],
                "stype_in": "continuous",
                "start": "2024-01-01",
                "end": "2025-01-01",
                "job_id": "SYNTHETIC",
            }
        ),
        encoding="utf-8",
    )
    combined = dbn.stat().st_size + sidecar.stat().st_size
    contract = {
        "schema_version": "eight_market_successor_candidate/1.0.0",
        "classification": "NON_AUTHORIZING_READ_ONLY_SOURCE_INVENTORY",
        "source_root": str(root),
        "parent_release": {
            "release_id": "a" * 64,
            "dbn_files": 1,
            "sidecar_files": 1,
            "combined_files": 2,
            "combined_bytes": 10,
        },
        "markets": ["ES"],
        "families": {"ohlcv_1m": "ohlcv-1m"},
        "expected_candidate": {
            "dbn_files": 1,
            "sidecar_files": 1,
            "combined_files": 2,
            "combined_bytes": combined,
        },
        "expected_union": {
            "dbn_files": 2,
            "sidecar_files": 2,
            "combined_files": 4,
            "combined_bytes": combined + 10,
            "market_count": 1,
        },
        "excluded_relative_paths": ["data/dbn/ohlcv_1m/ES/ignored.tmp/x"],
        "authority": {
            "provider_calls_authorized": False,
            "copy_authorized": False,
            "destination_mutation_authorized": False,
            "legacy_mutation_authorized": False,
        },
    }
    excluded = root / "data" / "dbn" / "ohlcv_1m" / "ES" / "ignored.tmp" / "x"
    excluded.parent.mkdir()
    excluded.write_bytes(b"x")
    path = tmp_path / "candidate.json"
    path.write_text(json.dumps(contract), encoding="utf-8")
    return path


def test_exact_candidate_is_verified_without_granting_copy_authority(
    tmp_path: Path,
) -> None:
    inventory = build_inventory(_fixture(tmp_path))
    assert inventory["candidate_totals"]["dbn_files"] == 1
    assert inventory["authority"]["copy_authorized"] is False
    assert inventory["records"][0]["dbn_sha256"]


def test_tampered_dbn_fails_closed(tmp_path: Path) -> None:
    contract = _fixture(tmp_path)
    dbn = next((tmp_path / "source").rglob("*.dbn.zst"))
    dbn.write_bytes(b"tampered")
    with pytest.raises(SuccessorInventoryError, match="sidecar mismatch"):
        build_inventory(contract)


def test_undeclared_temporary_file_fails_closed(tmp_path: Path) -> None:
    contract = _fixture(tmp_path)
    extra = (
        tmp_path
        / "source"
        / "data"
        / "dbn"
        / "ohlcv_1m"
        / "ES"
        / "unexpected.tmp"
        / "extra"
    )
    extra.parent.mkdir()
    extra.write_bytes(b"x")
    with pytest.raises(SuccessorInventoryError, match="undeclared temporary"):
        build_inventory(contract)
