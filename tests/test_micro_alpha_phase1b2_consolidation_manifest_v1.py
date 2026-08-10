from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_consolidation_manifest_v1 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_manifest_reconstructs_and_preserves_notes_unstaged() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    rebuilt = manifest.build_manifest(root=ROOT)
    assert persisted == rebuilt
    assert persisted["recommended_exact_stage_path_count"] == 16
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert persisted["next_sequential_boundary"] == "EXACT_PATH_STAGING_APPROVAL"


def test_manifest_records_source_safe_effects_and_no_push() -> None:
    value = manifest.build_manifest(root=ROOT)
    assert value["authority_and_effects"] == {
        "dbn_rows_decoded": 0,
        "historical_rows_read": 0,
        "year_2025_or_2026_payloads_opened": 0,
        "provider_or_network_calls_during_prepare_only_work": 0,
        "catalog_or_pointer_activated": False,
        "registration_or_evaluation": False,
        "cleanup_mutation": False,
        "git_staging": False,
        "git_commit": False,
        "git_push": False,
    }
    assert value["after_staging"] == "SEPARATE_LOCAL_COMMIT_APPROVAL_NO_PUSH"


def test_manifest_has_no_mutating_git_or_cleanup_surface() -> None:
    source = inspect.getsource(manifest)
    assert "git\", \"add" not in source
    assert "git\", \"commit" not in source
    assert "git\", \"push" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
