from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_gateway_remediation_manifest_v1 as manifest


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_gateway_remediation_manifest_reconstructs_exact_scope() -> None:
    persisted = json.loads((ROOT / manifest.OUTPUT).read_text(encoding="utf-8"))
    assert persisted == manifest.build_manifest()
    assert persisted["recommended_exact_stage_path_count"] == 14
    assert persisted["recommended_exact_stage_paths"] == list(manifest.RECOMMENDED)
    assert persisted["preserved_unstaged_paths"] == [
        "CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md",
    ]
    assert persisted["remediation"] == {
        "exact_operation_allowlisted": (
            "BUILD_APEX_MICRO_PHASE1B2_INACTIVE_FOUNDATION_V1_ONCE"
        ),
        "wildcard_or_alias_allowlisted": False,
        "v1_plan_preserved": True,
        "v1_audit_preserved": True,
        "v1_authorization_consumed": False,
        "v1_dbn_rows_decoded": 0,
        "successor_plan_version": 2,
    }


def test_gateway_manifest_has_no_execution_or_git_mutation_surface() -> None:
    source = inspect.getsource(manifest)
    assert "execute_authorized_phase1b2" not in source
    assert "git\", \"add" not in source
    assert "git\", \"commit" not in source
    assert "git\", \"push" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
