from __future__ import annotations

import json

import pytest

from scripts.prepare_safe_cleanup_inventory_v4 import OUTPUT, ROOT, build_plan


pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_cleanup_preparation_is_deterministic_non_destructive_and_catalog_first() -> None:
    plan = build_plan(root=ROOT)
    persisted = json.loads((ROOT / OUTPUT).read_text(encoding="utf-8"))
    assert plan == persisted
    assert plan["state"] == "PREPARED_NO_MUTATION_EXACT_CLEANUP_APPROVAL_REQUIRED"
    assert plan["authoritative_resolution"] == {
        "standard_lane": "data/active/catalog.json",
        "standard_active_root": "data/active/causally_gated_normalized",
        "phase2_release_history_root": "data/causally_gated_normalized",
        "micro_lane": "NO_ACTIVE_POINTER_OR_CATALOG",
        "directory_presence_alone_grants_research_use": False,
    }
    assert plan["cleanup_execution"] == {
        "performed": False,
        "files_deleted": 0,
        "directories_deleted": 0,
        "files_moved": 0,
        "active_data_changed": False,
        "raw_data_changed": False,
        "manifests_changed": False,
    }
    assert all(
        item["proposed_action"] == "PRESERVE_NO_MOVE_DELETE_OR_RENAME"
        for item in plan["protected_paths"]
    )
    assert not any(
        item["path"].startswith("data/")
        for item in plan["regenerable_cache_candidates"]
    )
    assert plan["payload_safety"] == {
        "dbn_or_parquet_payload_opened": False,
        "historical_rows_read": False,
        "year_2025_or_2026_payload_opened": False,
    }
