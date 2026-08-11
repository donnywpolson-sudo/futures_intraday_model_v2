from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REPORT = Path("state/unpublished_evidence/data_topology_source_safe_audit_v2/report.json")
pytestmark = pytest.mark.legacy


def _persisted() -> dict[str, object]:
    return json.loads((ROOT / REPORT).read_text(encoding="utf-8"))


def test_v2_report_is_preserved_source_safe_historical_evidence() -> None:
    persisted = _persisted()
    assert persisted["state"] == "PASS_SOURCE_SAFE_AUTHORITY_AND_FOLDER_ROLE_AUDIT"
    assert persisted["standard_lane"]["source_of_truth"] == "data/active/catalog.json"
    assert persisted["standard_lane"]["active_market_year_count"] == 562
    assert persisted["standard_lane"]["market_count"] == 41
    assert persisted["standard_lane"][
        "duplicate_named_phase2_roots_are_conflicting_active_sources"
    ] is False
    assert persisted["micro_lane"]["dbn_count"] == 160
    assert persisted["micro_lane"]["active_pointer"] == "ABSENT"
    assert persisted["micro_lane"]["active_catalog"] == "ABSENT"


def test_every_data_root_has_an_explicit_non_authorizing_role() -> None:
    report = _persisted()
    inventory = report["data_root_inventory"]
    assert inventory["dbn"]["role"] == "PHASE1A_SOURCE_CUSTODY_NOT_RESEARCH_EVIDENCE"
    assert inventory["causally_gated_normalized"]["role"] == (
        "CONTENT_ADDRESSED_IMMUTABLE_PHASE2_RELEASE_HISTORY"
    )
    assert inventory["active"]["resolution_rule"] == (
        "RESOLVE_ONLY_THROUGH_DATA_ACTIVE_CATALOG_JSON"
    )
    assert all(item["active_by_directory_presence"] is False for item in inventory.values())


def test_cleanup_conclusion_preserves_data_and_requires_separate_approval() -> None:
    report = _persisted()
    conclusion = report["cleanup_conclusion"]
    assert conclusion["data_root_cleanup_candidate_count"] == 0
    assert conclusion["standard_phase2_roots_require_merge"] is False
    assert report["authority_rules"][
        "actual_cleanup_requires_separate_exact_candidate_manifest_and_approval"
    ] is True


def test_persisted_report_records_no_payload_access_or_cleanup_authority() -> None:
    report = _persisted()
    assert report["payload_safety"]["historical_rows_read"] == 0
    assert report["payload_safety"]["dbn_payloads_opened"] == 0
    assert report["authority_rules"]["cleanup_may_not_merge_delete_move_or_relabel_data_roots"] is True
