from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_phase2_completion_manifest_v2 as manifest
from futures_rebuild.canonical import sha256_json


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_phase2_completion_v2_reconstructs_post_success_transition() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    core = dict(persisted)
    assert core.pop("manifest_id") == sha256_json(core)
    expected_worktree = set(manifest.RECOMMENDED) | set(manifest.PRESERVED_UNSTAGED)
    if manifest._tracked(manifest.OUTPUT) or manifest._worktree_paths() == expected_worktree:
        assert persisted == manifest.build_manifest()
    assert persisted["recommended_exact_stage_path_count"] == 15
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert persisted["preserved_v1"] == {
        "manifest_id": "97cc8af5d2535c896b85d199bbb273899b798596841d8ec96a3faf6fcff55f62",
        "manifest_sha256": "1fa95a51211514245a46ed9f5f96311ad3f83f42fd0995ade8dafd0ee54cf644",
        "classification": "SUPERSEDED_PREPARATION_POST_SUCCESS_TEST_TRANSITION",
        "byte_for_byte_preserved": True,
    }
    transition = persisted["post_success_transition"]
    assert transition["terminal_state"] == "SUCCESS_CERTIFIED_INACTIVE_PHASE2"
    assert transition["phase2_output_count"] == 24
    assert transition["phase2_output_bytes"] == 454_578_644
    assert transition["persisted_plan_remains_immutable"] is True
    assert transition["fresh_plan_reconstruction_expected_result"] == (
        "FAIL_CLOSED_CREATE_ONLY_OUTPUT_COLLISION"
    )
    assert transition["test_reads_persisted_plan_before_asserting_collision"] is True
    assert transition["executor_rerun_authority"] is False
    assert transition["parquet_rows_decoded_by_remediation"] == 0
    assert not any(path.endswith(".parquet") for path in manifest.RECOMMENDED)


def test_completion_v2_has_no_decode_execution_cleanup_or_git_surface() -> None:
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
