from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_group_diagnostic_manifest_v4 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_group_diagnostic_manifest_v4_reconstructs_exact_scope() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    assert persisted == manifest.build_manifest()
    assert persisted["recommended_exact_stage_path_count"] == 21
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert persisted["supersession_reason"] == (
        "FINAL_LIFECYCLE_AND_ALL_SCHEMA_TEST_SCOPE"
    )
    inactive = persisted["preserved_inactive_not_for_git"]
    assert inactive["phase1b_parquet_count"] == 120
    assert inactive["phase1b_parquet_bytes"] == 6_627_486_838
    assert inactive["diagnostic_parquet_count"] == 1
    assert inactive["diagnostic_parquet_bytes"] == 17_093_314
    assert inactive["raw_or_derived_parquet_recommended_for_git_stage"] is False
    successor = persisted["group_diagnostic_successor"]
    assert successor["source_count"] == 5
    assert successor["source_bytes"] == 86_344_286
    assert successor["maximum_batch_rows"] == 100_000
    assert successor["dbn_reachable"] is False
    assert successor["sixth_parquet_source_reachable"] is False
    assert successor["phase2_parquet_creation_reachable"] is False


def test_group_manifest_v4_has_no_execution_cleanup_or_git_mutation_surface() -> None:
    source = inspect.getsource(manifest)
    assert "execute_once" not in source
    assert "DBNStore" not in source
    assert "iter_batches" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
    assert '"git", "add"' not in source
    assert '"git", "commit"' not in source
    assert '"git", "push"' not in source
