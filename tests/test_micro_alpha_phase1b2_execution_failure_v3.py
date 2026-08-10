from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts import prepare_apex_micro_phase1b2_execution_failure_v3 as failure


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_v3_failure_report_reconstructs_exact_phase1b_inventory() -> None:
    persisted = json.loads((ROOT / failure.OUTPUT).read_text(encoding="utf-8"))
    assert persisted == failure.build_report()
    assert persisted["state"] == (
        "SUPERSEDED_PHASE1B_COMPLETE_PHASE2_TRANSITION_FAILED_CLOSED"
    )
    assert persisted["authorization_receipt_consumed"] is True
    assert persisted["attempts"] == 1
    assert persisted["automatic_retries"] == 0
    assert persisted["source_hashes_verified"] == 120
    assert persisted["completed_phase1b_outputs"] == 120
    assert persisted["completed_phase2_outputs"] == 0
    assert persisted["created_output_bytes"] == 6_627_486_838
    assert len(persisted["phase1b_inventory"]) == 120
    assert persisted["dbn_rows_reopened_during_failure_seal"] == 0
    assert persisted["parquet_rows_opened_during_failure_seal"] == 0
    assert persisted["v3_reexecution_permitted"] is False


def test_v3_failure_inventory_is_price_free_and_exact() -> None:
    report = failure.build_report()
    records = report["phase1b_inventory"]
    assert len({item["request_id"] for item in records}) == 120
    assert len({item["relative_path"] for item in records}) == 120
    assert all(len(item["sha256"]) == 64 for item in records)
    assert all(len(item["source_sha256"]) == 64 for item in records)
    assert sum(item["bytes"] for item in records) == 6_627_486_838
    assert report["raw_values_reported"] is False


def test_v3_failure_builder_has_no_decode_retry_or_cleanup_surface() -> None:
    source = inspect.getsource(failure)
    assert "DBNStore" not in source
    assert "read_table" not in source
    assert "iter_batches" not in source
    assert "unlink(" not in source
    assert "rmtree(" not in source
    assert "git\", \"add" not in source
    assert "git\", \"commit" not in source
    assert "git\", \"push" not in source
