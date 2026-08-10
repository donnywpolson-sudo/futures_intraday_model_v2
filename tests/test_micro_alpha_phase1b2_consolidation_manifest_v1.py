from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_consolidation_manifest_v1 as manifest
from scripts import (
    prepare_apex_micro_phase1b2_postcommit_reconstruction_manifest_v1 as remediation,
)


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
    consolidation_commit = manifest._git(
        ROOT, "log", "-1", "--format=%H", "--", manifest.OUTPUT.as_posix()
    )
    assert persisted["observed_head"] == manifest._git(
        ROOT, "rev-parse", f"{consolidation_commit}^"
    )
    assert set(
        manifest._git(
            ROOT, "diff-tree", "--no-commit-id", "--name-only", "-r",
            consolidation_commit,
        ).splitlines()
    ) == set(manifest.RECOMMENDED)


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


def test_postcommit_reconstruction_remediation_is_exact_and_deterministic() -> None:
    persisted = json.loads((ROOT / remediation.OUTPUT).read_text(encoding="utf-8"))
    assert persisted == remediation.build_manifest(root=ROOT)
    assert persisted["recommended_exact_stage_path_count"] == 4
    assert persisted["recommended_exact_stage_paths"] == list(
        remediation.RECOMMENDED
    )
    assert persisted["defect"] == {
        "state": "POST_COMMIT_RECONSTRUCTION_DEFECT_REMEDIATED",
        "cause": "BUILDER_REQUIRED_PRECOMMIT_WORKTREE_AFTER_COMMIT",
        "correction": "RECONSTRUCT_FROM_EXACT_HISTORICAL_CONSOLIDATION_COMMIT",
        "predecessor_manifest_overwritten": False,
    }
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]


def test_remediation_manifest_has_no_staging_commit_or_push_surface() -> None:
    source = inspect.getsource(remediation)
    assert "git\", \"add" not in source
    assert "git\", \"commit" not in source
    assert "git\", \"push" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
