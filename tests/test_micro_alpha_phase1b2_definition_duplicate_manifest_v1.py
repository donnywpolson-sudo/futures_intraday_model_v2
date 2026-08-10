from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_definition_duplicate_manifest_v1 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_definition_duplicate_manifest_reconstructs_exact_scope() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    assert persisted == manifest.build_manifest()
    assert persisted["recommended_exact_stage_path_count"] == 14
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert persisted["group_result"]["group_disposition"] == "DUPLICATE"
    assert persisted["group_result"]["definition_legacy_repeat_count"] == 308
    successor = persisted["definition_diagnostic_successor"]
    assert successor["source_count"] == 1
    assert successor["source_bytes"] == 68_274
    assert successor["maximum_batch_rows"] == 100_000
    assert successor["dbn_reachable"] is False
    assert successor["second_parquet_source_reachable"] is False
    assert successor["parquet_creation_reachable"] is False


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
