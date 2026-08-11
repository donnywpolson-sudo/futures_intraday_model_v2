from __future__ import annotations

import json

import pytest

from scripts.audit_standard_data_topology_source_safe import OUTPUT, ROOT


pytestmark = pytest.mark.legacy


def test_standard_topology_audit_is_preserved_source_safe_historical_evidence() -> None:
    report = json.loads((ROOT / OUTPUT).read_text(encoding="utf-8"))
    assert report["state"] == "PASS_SOURCE_SAFE_PROVENANCE_METADATA_ONLY"
    assert report["catalog"]["active_market_year_count"] == 562
    assert report["catalog"]["disposition_counts"] == {
        "FORWARD_ONLY_NOT_MATERIALIZED": 41,
        "LOCKED_HOLDOUT_NOT_MATERIALIZED": 41,
        "QUARANTINED_NOT_MATERIALIZED": 6,
        "RESEARCH_READY_CAUSAL_PRICE": 562,
    }
    assert report["folder_roles"] == {
        "immutable_phase1b_release_store": "data/raw",
        "immutable_phase2_release_store": "data/causally_gated_normalized",
        "authoritative_catalog_selected_view": "data/active/causally_gated_normalized",
        "active_resolution_rule": "DATA_ACTIVE_CATALOG_ONLY_NO_DIRECT_ARCHIVE_GLOB",
    }
    assert report["payload_safety"] == {
        "dbn_payloads_opened": 0,
        "parquet_payloads_opened": 0,
        "historical_rows_read": 0,
        "payload_sha256_recomputed": False,
        "year_2025_or_2026_payload_opened": False,
    }
    assert report["conclusion"]["duplicate_named_roots_are_conflicting_active_sources"] is False
    assert report["conclusion"]["row_level_recertification_performed"] is False
