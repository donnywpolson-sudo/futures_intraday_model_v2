from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_execution_failure_v2 as failure


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_v2_failure_report_reconstructs_consumed_zero_decode_attempt() -> None:
    persisted = json.loads((ROOT / failure.OUTPUT).read_text(encoding="utf-8"))
    assert persisted == failure.build_report()
    assert persisted["state"] == (
        "SUPERSEDED_FAIL_CLOSED_AFTER_HASH_CENSUS_BEFORE_DECODE_COMPLETION"
    )
    assert persisted["authorization_receipt_consumed"] is True
    assert persisted["attempts"] == 1
    assert persisted["automatic_retries"] == 0
    assert persisted["source_hashes_verified"] == 120
    assert persisted["dbn_rows_decoded"] == 0
    assert persisted["completed_phase1b_outputs"] == 0
    assert persisted["completed_phase2_outputs"] == 0
    assert persisted["created_output_bytes"] == 0
    assert persisted["staging_file_count"] == 0
    assert persisted["year_2025_or_2026_payloads_opened"] == 0
    assert persisted["v2_reexecution_permitted"] is False


def test_v2_failure_diagnosis_is_price_free_and_path_specific() -> None:
    report = failure.build_report()
    basis = report["diagnosis_basis"]
    assert report["diagnosis"] == "WINDOWS_STAGED_OUTPUT_PATH_LENGTH_CONTRACT_DEFECT"
    assert basis["first_partial_path_chars"] == 299
    assert basis["windows_legacy_max_path_chars"] == 260
    assert basis["parent_directories_created_without_file"] is True
    assert basis["raw_values_reported"] is False


def test_v2_failure_builder_has_no_decode_retry_or_cleanup_surface() -> None:
    source = inspect.getsource(failure)
    assert "execute_authorized_phase1b2" not in source
    assert "DBNStore" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
    assert "git\", \"add" not in source
    assert "git\", \"commit" not in source
    assert "git\", \"push" not in source
