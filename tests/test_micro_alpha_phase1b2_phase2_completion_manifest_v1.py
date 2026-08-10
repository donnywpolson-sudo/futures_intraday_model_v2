from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_phase2_completion_manifest_v1 as manifest
from futures_rebuild.canonical import sha256_json


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_phase2_completion_manifest_reconstructs_exact_inactive_result() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    core = dict(persisted)
    assert core.pop("manifest_id") == sha256_json(core)
    expected_worktree = set(manifest.RECOMMENDED) | set(manifest.PRESERVED_UNSTAGED)
    if manifest._tracked(manifest.OUTPUT) or manifest._worktree_paths() == expected_worktree:
        assert persisted == manifest.build_manifest()
    assert persisted["recommended_exact_stage_path_count"] == 11
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    result = persisted["certified_inactive_phase2"]
    assert result["state"] == "SUCCESS_CERTIFIED_INACTIVE_PHASE2"
    assert result["markets"] == ["MES", "MCL", "MGC", "M6E"]
    assert result["years"] == list(range(2018, 2025))
    assert result["source_count"] == 120
    assert result["source_bytes"] == 6_627_486_838
    assert result["coverage_cell_count"] == 140
    assert result["accepted_cell_count"] == 120
    assert result["prelaunch_cell_count"] == 20
    assert result["five_schema_interval_count"] == 24
    assert result["phase2_output_count"] == 24
    assert result["phase2_output_bytes"] == 454_578_644
    assert result["definition_repeat_classification_counts"] == {
        "EXACT_SEMANTIC_DUPLICATES_PRESERVED": 24,
    }
    assert result["definition_rows_deduplicated"] == 0
    custody = persisted["inactive_custody_not_for_git"]
    assert len(custody["phase2_outputs"]) == 24
    assert custody["raw_or_derived_parquet_recommended_for_git_stage"] is False
    assert custody["catalog_candidate_published"] is False
    assert custody["active_catalog_or_pointer_exists"] is False
    assert not any(path.endswith(".parquet") for path in manifest.RECOMMENDED)


def test_completion_manifest_has_no_row_decode_activation_cleanup_or_git_surface() -> None:
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
