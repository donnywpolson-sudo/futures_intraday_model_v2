from pathlib import Path

from futures_rebuild import tbbo_catalog_admission as admission


ROOT = Path(__file__).resolve().parents[1]


def test_tbbo_catalog_entry_and_sidecar_preserve_exclusions() -> None:
    plan = admission.load_plan(root=ROOT)
    assert plan["canonical_root"] == "data/dbn/tbbo"
    source = {
        "dbn": {
            "path": "dbn/tbbo/SR3/2026/2026-05-01_2026-06-01.dbn.zst",
            "sha256": "a" * 64,
            "size": 7,
        },
        "sidecar": {"sha256": "b" * 64},
        "query": {
            "request_id": "request",
            "market": "SR3",
            "query_contract": {
                "start": "2026-05-01T00:00:00Z",
                "end": "2026-06-01T00:00:00Z",
            },
        },
    }
    entry = admission._catalog_entry(
        source,
        {"monthly": [{"interval": "2026-05-01/2026-06-01", "row_count": 11}]},
        degraded_dates=("2026-05-24",),
        market_quarantined_dates=("2026-05-17",),
    )
    assert entry["mandatory_excluded_utc_dates"] == ["2026-05-17", "2026-05-24"]
    assert entry["row_count"] == 11
    assert entry["source_eligible"] is True
    assert entry["alpha_source_authorized"] is False
    sidecar = admission._canonical_sidecar(
        source=source,
        eligibility=entry,
        snapshot_id="c" * 64,
        certificate_id="d" * 64,
        catalog_id="e" * 64,
    )
    assert sidecar["mandatory_excluded_utc_dates"] == ["2026-05-17", "2026-05-24"]
    assert sidecar["alpha_source_authorized"] is False
