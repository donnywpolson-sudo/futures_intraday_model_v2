from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_preexecution_failure_v1 as failure


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_preexecution_failure_report_reconstructs_and_proves_zero_use() -> None:
    persisted = json.loads((ROOT / failure.OUTPUT).read_text(encoding="utf-8"))
    assert persisted == failure.build_report()
    assert persisted["state"] == (
        "SUPERSEDED_FAIL_CLOSED_BEFORE_AUTHORIZATION_CONSUMPTION"
    )
    assert persisted["authorization_receipt_consumed"] is False
    assert persisted["execution_attempt_started"] is False
    assert persisted["source_hashes_read"] == 0
    assert persisted["dbn_rows_decoded"] == 0
    assert persisted["year_2025_or_2026_payloads_opened"] == 0
    assert persisted["staging_or_evidence_root_created"] is False


def test_failure_report_builder_has_no_execution_or_mutation_surface() -> None:
    source = inspect.getsource(failure)
    assert "execute_authorized_phase1b2" not in source
    assert "DBNStore" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
    assert "git\", \"add" not in source
    assert "git\", \"commit" not in source
    assert "git\", \"push" not in source
