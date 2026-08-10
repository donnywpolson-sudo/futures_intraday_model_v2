from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_json
from scripts import prepare_apex_micro_phase1b2_phase2_completion_manifest_v4 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_phase2_completion_v4_reconstructs_sealed_row_map_scope() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    core = dict(persisted)
    assert core.pop("manifest_id") == sha256_json(core)
    expected_worktree = set(manifest.RECOMMENDED) | set(manifest.PRESERVED_UNSTAGED)
    if manifest._tracked(manifest.OUTPUT) or manifest._worktree_paths() == expected_worktree:
        assert persisted == manifest.build_manifest()
    assert persisted["recommended_exact_stage_path_count"] == 21
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert persisted["predecessor_manifest_id"] == (
        "49fb68cbd9b91ac6bac2d1841866010ebfa1403a9d1da147b80984c9f0eec9c0"
    )
    assert persisted["predecessor_manifest_sha256"] == (
        "b7d484fe38f2f0939f5e46348366ec64c4285653121c553bc5606d3959668ffb"
    )
    assert persisted["predecessor_classification"] == (
        "SUPERSEDED_PREPARATION_MISSING_REQUIRED_MAP_STATUS"
    )
    assert persisted["predecessor_manifest_byte_for_byte_preserved"] is True
    boundary = persisted["remaining_historical_row_boundary"]
    assert boundary == {
        "classification": "HISTORICAL_ROW_APPROVAL_REQUIRED",
        "scope": "INACTIVE_2025_HOLDOUT_AND_2026_FORWARD_MICRO_ROWS",
        "phase2_success_scope": "2018_THROUGH_2024_ONLY",
        "separate_approval_required": True,
    }
    result = persisted["certified_inactive_result"]
    assert result["state"] == "SUCCESS_CERTIFIED_INACTIVE_PHASE2"
    assert result["phase2_output_count"] == 24
    assert result["phase2_output_bytes"] == 454_578_644
    assert result["raw_or_derived_parquet_recommended_for_git_stage"] is False
    assert result["catalog_or_pointer_activated"] is False
    assert not any(path.endswith(".parquet") for path in manifest.RECOMMENDED)


def test_completion_v4_has_no_decode_execution_cleanup_or_git_surface() -> None:
    source = inspect.getsource(manifest)
    assert "ParquetFile" not in source
    assert "DBNStore" not in source
    assert "iter_batches" not in source
    assert "execute_once" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
    assert '"git", "add"' not in source
    assert '"git", "commit"' not in source
    assert '"git", "push"' not in source
