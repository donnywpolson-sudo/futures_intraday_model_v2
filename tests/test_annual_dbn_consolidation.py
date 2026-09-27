from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild import annual_dbn_consolidation as annual
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.data_layout import DataReleaseManifest
from futures_rebuild.errors import ContractError, IntegrityError


pytestmark = pytest.mark.current


def _write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(payload) + b"\n")


def _manifest_only_release(root: Path, *, missing_dbn: bool) -> tuple[DataReleaseManifest, str]:
    directory = root / "data/dbn/bbo_1s/HE/2025"
    directory.mkdir(parents=True)
    logical_dbn = "data/dbn/bbo_1s/HE/2025/2025-09-01.dbn.zst"
    sidecar = directory / "2025-09-01.dbn.zst.manifest.json"
    payload = {
        "market": "HE",
        "trade_date": "2025-09-01",
        "provider_condition": (
            "NOT_REPORTED_DEGRADED" if missing_dbn else "ZERO_BILLABLE_NO_DBN"
        ),
        "source_dbn_sha256": "0" * 64 if missing_dbn else None,
        "source_dbn_size_bytes": 1 if missing_dbn else None,
        "sidecar_id": "1" * 64,
    }
    _write(sidecar, payload)
    manifest = DataReleaseManifest.build(
        directory,
        phase="dbn",
        release_kind="test",
        schema_version="1",
        logical_paths={sidecar.name: f"{logical_dbn}.manifest.json"},
    )
    return manifest, f"{logical_dbn}.manifest.json"


def test_manifest_only_bbo_disposition_is_retained_and_missing_dbn_fails(tmp_path: Path) -> None:
    manifest, logical = _manifest_only_release(tmp_path, missing_dbn=False)
    row = annual._source_row(tmp_path, manifest, logical, "bbo_1s")
    assert row["dbn"] is None
    assert row["source_disposition"]["provider_condition"] == "ZERO_BILLABLE_NO_DBN"

    broken = tmp_path / "broken"
    manifest, logical = _manifest_only_release(broken, missing_dbn=True)
    with pytest.raises(IntegrityError, match="entry is absent"):
        annual._source_row(broken, manifest, logical, "bbo_1s")


def test_bbo_annual_resolver_applies_exact_daily_boundaries(tmp_path: Path) -> None:
    payload_root = tmp_path / "payload"
    logical = "data/dbn/bbo_1s/HE/2026/2026-01-01_2026-09-01.dbn.zst"
    source = payload_root / Path(logical).relative_to("data/dbn")
    source.parent.mkdir(parents=True)
    source.write_bytes(b"annual")
    eligible = {
        "market": "HE",
        "trade_date": "2026-01-02",
        "annual_logical_path": logical,
        "annual_sha256": sha256_file(source),
        "annual_size_bytes": source.stat().st_size,
        "filter_start_inclusive_ns": annual._iso_ns("2026-01-02"),
        "filter_end_exclusive_ns": annual._iso_ns("2026-01-03"),
        "source_eligible": True,
        "disposition": "CERTIFIED_SOURCE_ELIGIBLE_NOT_ALPHA_ACTIVE",
    }
    excluded = {
        **eligible,
        "trade_date": "2026-01-03",
        "source_eligible": False,
        "disposition": "PROVIDER_DEGRADED_EXCLUDED_NO_AUTHORITATIVE_WAIVER",
    }
    catalog = tmp_path / "catalog.json"
    _write(catalog, {"entries": [eligible, excluded]})

    result = annual.resolve_bbo_1s_date(
        root=tmp_path,
        catalog_path=catalog,
        market="HE",
        trade_date="2026-01-02",
        candidate_payload_root=payload_root,
    )
    assert result["source"] == source
    assert result["start_inclusive_ns"] == annual._iso_ns("2026-01-02")
    assert result["end_exclusive_ns"] == annual._iso_ns("2026-01-03")
    assert annual.resolve_bbo_1s_date(
        root=tmp_path,
        catalog_path=catalog,
        market="HE",
        trade_date="2026-01-03",
        candidate_payload_root=payload_root,
    ) == {
        "source": None,
        "disposition": "PROVIDER_DEGRADED_EXCLUDED_NO_AUTHORITATIVE_WAIVER",
        "eligible": False,
    }

    with pytest.raises(IntegrityError, match="unique catalog disposition"):
        annual.resolve_bbo_1s_date(
            root=tmp_path,
            catalog_path=catalog,
            market="HE",
            trade_date="2026-01-04",
            candidate_payload_root=payload_root,
        )


def test_source_selection_rejects_duplicate_and_unknown_units() -> None:
    plan = {"units": [{"unit_key": "tbbo:HE:2026"}]}
    with pytest.raises(ContractError, match="duplicates"):
        annual._selected_units(plan, ["tbbo:HE:2026", "tbbo:HE:2026"])
    with pytest.raises(ContractError, match="unknown"):
        annual._selected_units(plan, ["bbo_1s:HE:2026"])
