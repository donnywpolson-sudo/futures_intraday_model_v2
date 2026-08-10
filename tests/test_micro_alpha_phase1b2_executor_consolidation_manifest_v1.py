from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_executor_consolidation_manifest_v1 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_executor_manifest_is_preserved_as_superseded_source_safe_scope() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    assert persisted["recommended_exact_stage_path_count"] == 12
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert persisted["scope"]["source_count"] == 120
    assert persisted["scope"]["source_bytes"] == 1_232_883_585
    assert persisted["scope"]["row_payloads_opened"] == 0
    assert persisted["manifest_id"] == (
        "1d7c1b0257408f9ca8c201cbe544ff5ac29e9264142579784dfdb0471f6750d5"
    )


def test_executor_manifest_has_no_mutating_or_execution_surface() -> None:
    source = inspect.getsource(manifest)
    assert "git\", \"add" not in source
    assert "git\", \"commit" not in source
    assert "git\", \"push" not in source
    assert "execute_authorized_phase1b2" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
