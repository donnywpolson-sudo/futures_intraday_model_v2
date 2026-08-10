from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_json
from scripts import prepare_apex_micro_phase1b2_phase2_completion_manifest_v5 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_phase2_completion_v5_reconstructs_postcommit_test_remediation() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    core = dict(persisted)
    assert core.pop("manifest_id") == sha256_json(core)
    expected_worktree = set(manifest.RECOMMENDED) | set(manifest.PRESERVED_UNSTAGED)
    if manifest._tracked(manifest.OUTPUT) or manifest._worktree_paths() == expected_worktree:
        assert persisted == manifest.build_manifest()
    assert persisted["recommended_exact_stage_path_count"] == 7
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert persisted["predecessor_manifest_id"] == (
        "c9293063c8210fad57a68b3d7aae5fb14817911e926d777fda47fb27a5a5840a"
    )
    assert persisted["predecessor_manifest_sha256"] == (
        "e82db00ef1e4a4c6643aaf1de583bf49b9443cadce72b375f712dd55b9c1f11e"
    )
    assert persisted["predecessor_commit"] == (
        "caa928439622df6a531f67ba75f446b7432168f7"
    )
    remediation = persisted["postcommit_test_remediation"]
    assert remediation["remediated_test_count"] == 4
    assert remediation["executed_plan_implementation_head"] == (
        "21069d7210afa967557480dcc1035cb61b869fa2"
    )
    assert remediation["executed_plan_authorization_consumed"] is True
    assert remediation["fresh_preview_scope_is_distinct"] is True
    assert remediation["fresh_plan_written_or_authorized"] is False
    assert remediation["historical_rows_decoded_by_remediation"] == 0
    result = persisted["certified_inactive_result"]
    assert result["state"] == "SUCCESS_CERTIFIED_INACTIVE_PHASE2"
    assert result["phase2_output_count"] == 24
    assert result["phase2_output_bytes"] == 454_578_644
    assert result["raw_or_derived_parquet_recommended_for_git_stage"] is False
    assert result["catalog_or_pointer_activated"] is False
    assert not any(path.endswith(".parquet") for path in manifest.RECOMMENDED)


def test_completion_v5_has_no_decode_execution_cleanup_or_git_surface() -> None:
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
