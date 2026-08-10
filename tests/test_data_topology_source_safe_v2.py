from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import audit_data_topology_source_safe_v2 as audit


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_v2_report_is_current_deterministic_and_source_safe() -> None:
    persisted = json.loads((ROOT / audit.OUTPUT).read_text(encoding="utf-8"))
    rebuilt = audit.build_report(root=ROOT)
    assert persisted == rebuilt
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
    report = audit.build_report(root=ROOT)
    inventory = report["data_root_inventory"]
    assert set(inventory) == {
        item.name for item in (ROOT / "data").iterdir() if item.is_dir()
    }
    assert inventory["dbn"]["role"] == "PHASE1A_SOURCE_CUSTODY_NOT_RESEARCH_EVIDENCE"
    assert inventory["causally_gated_normalized"]["role"] == (
        "CONTENT_ADDRESSED_IMMUTABLE_PHASE2_RELEASE_HISTORY"
    )
    assert inventory["active"]["resolution_rule"] == (
        "RESOLVE_ONLY_THROUGH_DATA_ACTIVE_CATALOG_JSON"
    )
    assert all(item["active_by_directory_presence"] is False for item in inventory.values())


def test_cleanup_conclusion_preserves_data_and_requires_separate_approval() -> None:
    report = audit.build_report(root=ROOT)
    conclusion = report["cleanup_conclusion"]
    assert conclusion["data_root_cleanup_candidate_count"] == 0
    assert conclusion["standard_phase2_roots_require_merge"] is False
    assert report["authority_rules"][
        "actual_cleanup_requires_separate_exact_candidate_manifest_and_approval"
    ] is True


def test_auditor_has_no_payload_reader_or_cleanup_mutation_surface() -> None:
    source = inspect.getsource(audit)
    forbidden = (
        "read_parquet",
        "DBNStore",
        "read_dbn",
        "to_df(",
        "unlink(",
        "rmtree(",
        "Remove-Item",
        "shutil.move",
    )
    assert not any(token.lower() in source.lower() for token in forbidden)
