from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.reported_bar_fixed_horizon_protocol_correction import (
    CORRECTED_PROTOCOL_PATH,
    INVALID_PROTOCOL_PATH,
    INVALID_PROTOCOL_SHA256,
    build_corrected_protocol,
    build_invalidity,
    invalidity_path,
)


ROOT = Path(__file__).resolve().parents[1]


def test_first_protocol_is_preserved_and_invalid_pre_data() -> None:
    record = build_invalidity(root=ROOT)
    assert sha256_file(ROOT / INVALID_PROTOCOL_PATH) == INVALID_PROTOCOL_SHA256
    assert record["classification"] == "INVALID_PRE_DATA_MISSING_EXECUTION_BOUNDS"
    assert record["historical_rows_read"] is False
    assert record["economic_result"] == "NOT_PRODUCED"


def test_corrected_protocol_freezes_exact_host_limits_when_prepared() -> None:
    invalidity = build_invalidity(root=ROOT)
    path = ROOT / invalidity_path(invalidity)
    if not path.exists():
        pytest.skip("invalidity is created after pre-write validation")
    protocol = build_corrected_protocol(root=ROOT, invalidity=invalidity)
    assert protocol["execution_limits"] == {
        "maximum_attempts": 1,
        "maximum_retries": 0,
        "maximum_workers": 4,
        "worker_deadline_seconds": 3300,
        "maximum_runtime_seconds": 3600,
        "maximum_external_cost_usd": "0",
        "windows_host_required": True,
    }
    assert protocol["coverage_gates"]["feature_complete_candidate_execution_path_percent"] == 100
    assert protocol["source_only_selection"]["returns_costs_predictions_or_outcomes_used"] is False
    assert protocol["protocol_id"] == sha256_json(
        {key: value for key, value in protocol.items() if key != "protocol_id"}
    )
    if (ROOT / CORRECTED_PROTOCOL_PATH).exists():
        assert sha256_file(ROOT / CORRECTED_PROTOCOL_PATH)
