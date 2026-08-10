from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_phase2_successor_manifest_v1 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_phase2_successor_manifest_reconstructs_exact_scope() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    assert persisted == manifest.build_manifest()
    assert persisted["recommended_exact_stage_path_count"] == 14
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    result = persisted["definition_result"]
    assert result["legacy_repeat_count"] == 308
    assert result["exact_semantic_duplicate_count"] == 308
    assert result["distinct_same_key_update_count"] == 0
    assert result["definition_rows_deduplicated"] == 0
    preview = persisted["phase2_successor_preview"]
    assert preview["source_count"] == 120
    assert preview["source_bytes"] == 6_627_486_838
    assert preview["coverage_cell_count"] == 140
    assert preview["interval_count"] == 24
    assert preview["maximum_parquet_open_operations"] == 144
    assert preview["maximum_parquet_outputs"] == 24
    assert preview["dbn_reachable"] is False


def test_manifest_has_no_row_execution_cleanup_or_git_mutation_surface() -> None:
    source = inspect.getsource(manifest)
    assert "execute_once" not in source
    assert "ParquetFile" not in source
    assert "DBNStore" not in source
    assert "iter_batches" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
    assert '"git", "add"' not in source
    assert '"git", "commit"' not in source
    assert '"git", "push"' not in source
