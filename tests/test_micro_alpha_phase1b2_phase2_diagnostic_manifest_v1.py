from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_phase2_diagnostic_manifest_v1 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_phase2_diagnostic_manifest_reconstructs_exact_scope() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    assert persisted == manifest.build_manifest()
    assert persisted["recommended_exact_stage_path_count"] == 17
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert persisted["preserved_inactive_not_for_git"] == {
        "phase1b_parquet_count": 120,
        "phase1b_parquet_bytes": 6_627_486_838,
        "phase1b_inventory_id": (
            "287030d829e4aa136d3cc5c499c2a23f2afcfc39525694ba21a735dc72fa5a75"
        ),
        "raw_or_derived_parquet_recommended_for_git_stage": False,
    }
    assert persisted["diagnostic"]["source_count"] == 1
    assert persisted["diagnostic"]["dbn_reachable"] is False
    assert persisted["diagnostic"]["second_parquet_source_reachable"] is False


def test_diagnostic_manifest_has_no_execution_cleanup_or_git_mutation_surface() -> None:
    source = inspect.getsource(manifest)
    assert "execute_once" not in source
    assert "DBNStore" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
    assert "git\", \"add" not in source
    assert "git\", \"commit" not in source
    assert "git\", \"push" not in source
