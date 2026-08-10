from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_json
from scripts import prepare_apex_micro_phase1b2_phase2_completion_manifest_v3 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_phase2_completion_v3_reconstructs_successor_aware_scope() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    core = dict(persisted)
    assert core.pop("manifest_id") == sha256_json(core)
    expected_worktree = set(manifest.RECOMMENDED) | set(manifest.PRESERVED_UNSTAGED)
    if manifest._tracked(manifest.OUTPUT) or manifest._worktree_paths() == expected_worktree:
        assert persisted == manifest.build_manifest()
    assert persisted["recommended_exact_stage_path_count"] == 18
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert [item["manifest_id"] for item in persisted["predecessor_manifests"]] == [
        "97cc8af5d2535c896b85d199bbb273899b798596841d8ec96a3faf6fcff55f62",
        "f2353539c9032ba2078af58c84d749309d632c9eea17048bffa8db5c39a9a327",
    ]
    transition = persisted["successor_aware_test_transition"]
    assert transition["remediated_test_count"] == 2
    assert transition["uncommitted_predecessor_manifest_identity_verified"] is True
    assert transition["committed_manifest_reconstruction_retained"] is True
    assert transition["historical_row_or_executor_rerun_authority"] is False
    assert transition["parquet_rows_decoded_by_remediation"] == 0
    result = persisted["certified_inactive_result"]
    assert result["state"] == "SUCCESS_CERTIFIED_INACTIVE_PHASE2"
    assert result["phase2_output_count"] == 24
    assert result["phase2_output_bytes"] == 454_578_644
    assert result["raw_or_derived_parquet_recommended_for_git_stage"] is False
    assert result["catalog_or_pointer_activated"] is False
    assert not any(path.endswith(".parquet") for path in manifest.RECOMMENDED)


def test_completion_v3_has_no_decode_execution_cleanup_or_git_surface() -> None:
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
