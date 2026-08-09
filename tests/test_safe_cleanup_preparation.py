from __future__ import annotations

import json

import pytest

from futures_rebuild.canonical import sha256_file, sha256_json
from scripts.prepare_safe_cleanup_inventory_v4 import (
    OUTPUT as V4_OUTPUT,
    ROOT,
)
from scripts.prepare_safe_cleanup_inventory_v5 import OUTPUT, build_plan


pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_cleanup_preparation_is_deterministic_non_destructive_and_catalog_first() -> None:
    v4 = json.loads((ROOT / V4_OUTPUT).read_text(encoding="utf-8"))
    v4_core = dict(v4)
    v4_id = v4_core.pop("plan_id")
    assert v4_id == sha256_json(v4_core)
    assert v4_id == "811e3d6d9eb35cf24e49a9e215f6d4550cdfb69792a46bf3cf5cb52244c48b72"
    assert sha256_file(ROOT / V4_OUTPUT) == (
        "2e488ad54e5e9300918918cb83f99512e768d8537f8cb2beb7337e61c0f81b84"
    )
    assert v4["observed_head"] == "558ee0943a06a89699a888d35f329bbdc17099fc"

    plan = build_plan(root=ROOT)
    persisted = json.loads((ROOT / OUTPUT).read_text(encoding="utf-8"))
    assert plan == persisted
    assert plan["state"] == (
        "PREPARED_NO_MUTATION_EXACT_CLEANUP_CENSUS_AND_APPROVAL_REQUIRED"
    )
    assert plan["head_binding"] == {
        "prepared_head_recorded": False,
        "reason": (
            "ONGOING_AUTHORIZED_COMMITS_MUST_NOT_SELF_INVALIDATE_PREPARE_ONLY_POLICY"
        ),
        "exact_execution_head_required_after_candidate_census": True,
    }
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
    assert plan["candidate_policy"]["frozen_candidates"] == []
    assert plan["candidate_policy"]["data_path_candidates_allowed"] is False
    assert plan["payload_safety"] == {
        "dbn_or_parquet_payload_opened": False,
        "historical_rows_read": False,
        "year_2025_or_2026_payload_opened": False,
    }
