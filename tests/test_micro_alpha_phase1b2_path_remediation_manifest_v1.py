from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_path_remediation_manifest_v1 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_path_remediation_manifest_reconstructs_exact_scope() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    assert persisted == manifest.build_manifest()
    assert persisted["recommended_exact_stage_path_count"] == 14
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert persisted["v2_failure"] == {
        "authorization_consumed": True,
        "source_hashes_verified": 120,
        "dbn_rows_decoded": 0,
        "created_output_bytes": 0,
        "first_partial_path_chars": 299,
        "automatic_retries": 0,
        "v2_reexecution_permitted": False,
    }
    assert persisted["v3_remediation"]["path_alias_collision_count"] == 0
    assert persisted["v3_remediation"]["maximum_staged_partial_path_chars"] == 240


def test_path_manifest_has_no_execution_cleanup_or_git_mutation_surface() -> None:
    source = inspect.getsource(manifest)
    assert "execute_authorized_phase1b2" not in source
    assert "DBNStore" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
    assert "git\", \"add" not in source
    assert "git\", \"commit" not in source
    assert "git\", \"push" not in source
