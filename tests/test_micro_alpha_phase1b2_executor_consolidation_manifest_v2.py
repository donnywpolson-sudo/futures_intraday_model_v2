from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_executor_consolidation_manifest_v2 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_successor_manifest_and_supersession_reconstruct_exactly() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    supersession = json.loads(
        (ROOT / manifest.SUPERSESSION).read_text(encoding="utf-8")
    )
    assert persisted == manifest.build_manifest()
    assert supersession == manifest.build_supersession()
    assert persisted["recommended_exact_stage_path_count"] == 16
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert persisted["scope"]["per_interval_receipt_exact_source_binding"] is True
    assert persisted["scope"]["row_payloads_opened"] == 0
    assert supersession["predecessor_overwritten_or_deleted"] is False


def test_successor_manifest_has_no_mutation_or_execution_surface() -> None:
    source = inspect.getsource(manifest)
    assert "git\", \"add" not in source
    assert "git\", \"commit" not in source
    assert "git\", \"push" not in source
    assert "execute_authorized_phase1b2" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
