from __future__ import annotations

import json

import pytest

from futures_rebuild.annual_dbn_gate_b import (
    EXPECTED_OUTPUT_FILE_COUNT,
    EXPECTED_SUCCESSOR_FILES,
    _prefer_primary_ohlcv_1m_entries,
    _successor_entries,
)
from futures_rebuild.canonical import sha256_file
from futures_rebuild.causal_observation_full_build import (
    validate_source_query_contracts,
)
from futures_rebuild.errors import IntegrityError


pytestmark = pytest.mark.current


def test_successor_inventory_is_additive_unique_and_nonadmitting() -> None:
    prior = [
        {"path": f"data/dbn/old/{index:05d}", "admitted_standard_foundation": True}
        for index in range(EXPECTED_SUCCESSOR_FILES - EXPECTED_OUTPUT_FILE_COUNT)
    ]
    annual = [
        {
            "path": f"data/dbn/annual/{index:03d}",
            "kind": "DBN" if index % 2 == 0 else "SIDECAR",
            "admitted_standard_foundation": False,
        }
        for index in range(EXPECTED_OUTPUT_FILE_COUNT)
    ]

    result = _successor_entries(prior, annual)

    assert len(result) == EXPECTED_SUCCESSOR_FILES
    assert result == sorted(result, key=lambda item: item["path"])
    annual[0]["admitted_standard_foundation"] = True
    with pytest.raises(IntegrityError, match="not exact"):
        _successor_entries(prior, annual)


def test_gate_b_prefers_continuous_primary_before_query_preflight(
    tmp_path,
) -> None:
    entries = []
    for suffix, stype, symbols, digest in (
        ("", "continuous", ["6N.v.0"], "a" * 64),
        (".parent", "parent", ["6N.FUT"], "b" * 64),
    ):
        dbn = (
            "data/dbn/ohlcv_1m/6N/2010/"
            f"2010-06-06_2011-01-01{suffix}.dbn.zst"
        )
        sidecar = f"{dbn}.manifest.json"
        sidecar_path = tmp_path / sidecar
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        sidecar_path.write_text(
            json.dumps(
                {
                    "dataset": "GLBX.MDP3",
                    "schema": "ohlcv-1m",
                    "market": "6N",
                    "symbols_requested": symbols,
                    "start": "2010-06-06",
                    "end": "2011-01-01",
                    "stype_in": stype,
                    "path": dbn,
                    "file_sha256": digest,
                    "file_size_bytes": 10,
                }
            ),
            encoding="utf-8",
        )
        common = {
            "admitted_standard_foundation": True,
            "market": "6N",
            "family": "ohlcv_1m",
            "year": 2010,
            "interval_start_inclusive": "2010-06-06",
            "interval_end_exclusive": "2011-01-01",
        }
        entries.extend(
            (
                {
                    **common,
                    "kind": "DBN",
                    "path": dbn,
                    "sha256": digest,
                    "size_bytes": 10,
                },
                {
                    **common,
                    "kind": "SIDECAR",
                    "path": sidecar,
                    "sha256": sha256_file(sidecar_path),
                    "size_bytes": sidecar_path.stat().st_size,
                },
            )
        )
    entries.append(
        {
            "admitted_standard_foundation": True,
            "family": "statistics",
            "kind": "DBN",
            "path": "data/dbn/statistics/6N/2010/retained.parent.dbn.zst",
        }
    )

    result = _prefer_primary_ohlcv_1m_entries(tmp_path, entries)
    selected = [
        item
        for item in result
        if item.get("family") == "ohlcv_1m"
        and item["admitted_standard_foundation"] is True
    ]

    assert all(item["admitted_standard_foundation"] is True for item in entries)
    assert [item["path"] for item in selected] == [
        "data/dbn/ohlcv_1m/6N/2010/2010-06-06_2011-01-01.dbn.zst",
        "data/dbn/ohlcv_1m/6N/2010/2010-06-06_2011-01-01.dbn.zst.manifest.json",
    ]
    assert result[-1]["admitted_standard_foundation"] is True
    assert validate_source_query_contracts(
        tmp_path, selected, expected_count=1
    )["query_contract_count"] == 1
