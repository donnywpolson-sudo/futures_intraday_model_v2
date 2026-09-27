from pathlib import Path
from types import SimpleNamespace

import databento
import numpy as np
import pytest

from futures_rebuild import l1_execution_bbo_1s_certification as quality
from futures_rebuild import l1_execution_bbo_1s_source as source


ROOT = Path(__file__).resolve().parents[1]


def test_plan_row_gates_and_canonical_dispositions_are_frozen() -> None:
    plan = quality.load_plan(root=ROOT)
    assert plan["maximum_workers"] == 4
    rows = np.zeros(2, dtype=quality.BBO_DTYPE)
    rows["rtype"] = quality.BBO_RTYPE
    rows["publisher_id"] = 1
    rows["instrument_id"] = 2
    rows["ts_event"] = 3
    rows["ts_recv"] = 4
    rows["sequence"] = 5
    rows["bid_px_00"] = 101
    rows["ask_px_00"] = 100
    rows["bid_sz_00"] = rows["ask_sz_00"] = 1
    rows["bid_ct_00"] = rows["ask_ct_00"] = 1
    counts = quality._semantic_counts(rows)
    assert counts["duplicate_record_identities"] == 1
    assert counts["crossed_book_rows"] == 2
    rows["bid_px_00"] = databento.UNDEF_PRICE
    assert quality._semantic_counts(rows)["invalid_bid_sentinel_or_size_rows"] == 2

    base = {
        "market": "ES",
        "trade_date": "2025-09-17",
        "logical_path": "data/dbn/bbo_1s/ES/2025/2025-09-17.dbn.zst",
        "request_id": "r",
        "sidecar_id": "s",
        "plan_id": "p",
        "file_size_bytes": 1,
        "file_sha256": "h",
        "provider_condition": "DEGRADED",
    }
    sidecar = quality._canonical_sidecar(
        source_item=base,
        eligibility_entry={
            "source_eligible": False,
            "disposition": "PROVIDER_DEGRADED_EXCLUDED_NO_AUTHORITATIVE_WAIVER",
        },
        certificate_id="c",
        catalog_id="e",
    )
    assert sidecar["row_quality_certified"] is False
    assert sidecar["source_eligible"] is False
    assert sidecar["disposition"] == (
        "PROVIDER_DEGRADED_EXCLUDED_NO_AUTHORITATIVE_WAIVER"
    )


def test_consumer_selects_only_certified_non_alpha_partitions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    eligible = {
        "request_id": "r1",
        "market": "ES",
        "trade_date": "2025-09-02",
        "logical_path": "data/dbn/bbo_1s/ES/2025/2025-09-02.dbn.zst",
        "provider_condition": "NOT_REPORTED_DEGRADED",
        "disposition": source.ELIGIBLE_DISPOSITION,
        "structural_scan_completed": True,
        "row_quality_certified": True,
        "source_eligible": True,
        "alpha_source_authorized": False,
    }
    excluded = {
        **eligible,
        "request_id": "r2",
        "trade_date": "2025-09-03",
        "logical_path": "data/dbn/bbo_1s/ES/2025/2025-09-03.dbn.zst",
        "disposition": "PROVIDER_DEGRADED_EXCLUDED_NO_AUTHORITATIVE_WAIVER",
        "row_quality_certified": False,
        "source_eligible": False,
    }
    catalog = {
        "state": "SEALED_SOURCE_ELIGIBILITY_CATALOG_NOT_ALPHA_ACTIVE",
        "entry_count": 2,
        "eligible_count": 1,
        "excluded_count": 1,
        "alpha_source_authorized": False,
        "entries": [eligible, excluded],
    }
    released = {
        eligible["logical_path"]: SimpleNamespace(logical_path=eligible["logical_path"]),
        f'{eligible["logical_path"]}.manifest.json': SimpleNamespace(
            logical_path=f'{eligible["logical_path"]}.manifest.json'
        ),
    }
    manifest = SimpleNamespace(
        release_id=source.RELEASE_ID,
        release_kind="l1_bbo_1s_market_partition_canonical_custody",
        files=tuple(released.values()),
        metadata={"eligible_count": 1, "alpha_source_authorized": False},
        embedded_documents={
            "eligibility_catalog": {
                "id": source.CATALOG_ID,
                "sha256": source.CATALOG_SHA256,
            }
        },
        physical_relative_path=lambda entry: Path(entry.logical_path),
    )
    monkeypatch.setattr(source, "ENTRY_COUNT", 2)
    monkeypatch.setattr(source, "ELIGIBLE_COUNT", 1)
    monkeypatch.setattr(source, "EXCLUDED_COUNT", 1)
    monkeypatch.setattr(source, "RELEASE_FILE_COUNT", 2)
    monkeypatch.setattr(source, "read_canonical_authority", lambda *args, **kwargs: catalog)
    monkeypatch.setattr(source, "sha256_file", lambda path: source.RELEASE_MANIFEST_SHA256)
    monkeypatch.setattr(source, "verify_data_release_manifest", lambda *args: manifest)
    monkeypatch.setattr(source, "assert_plain_file", lambda path: None)

    assert source.eligible_bbo_1s_paths(root=tmp_path) == (
        tmp_path / eligible["logical_path"],
    )
    catalog["entries"][0]["alpha_source_authorized"] = True
    with pytest.raises(source.IntegrityError, match="entry binding"):
        source.eligible_bbo_1s_paths(root=tmp_path)
