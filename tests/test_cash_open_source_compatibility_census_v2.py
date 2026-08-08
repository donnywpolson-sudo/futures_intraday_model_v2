from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.cash_open_source_compatibility_census_v2 import (
    FAILED_USE_PATH,
    FAILED_USE_SHA256,
    OUTPUT_ROOT,
    PLAN_PATH,
    PREDECESSOR_PLAN_PATH,
    PREDECESSOR_PLAN_SHA256,
    build_failure_record,
    build_plan_v2,
    execute_census_v2_once,
    failure_record_path,
    load_plan_v2,
)
from futures_rebuild.errors import UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def test_failure_record_preserves_consumed_attempt() -> None:
    record = build_failure_record(root=ROOT)
    assert record["classification"] == "INCONCLUSIVE_EXECUTION_HOST_PERMISSION"
    assert record["attempt_consumed"] is True
    assert record["workers_started"] is False
    assert record["historical_rows_decoded"] == 0
    assert record["census_output_created"] is False
    assert record["economic_result"] == "NOT_PRODUCED"
    assert sha256_file(ROOT / PREDECESSOR_PLAN_PATH) == PREDECESSOR_PLAN_SHA256
    assert sha256_file(ROOT / FAILED_USE_PATH) == FAILED_USE_SHA256


def test_successor_plan_changes_only_host_attempt_boundary() -> None:
    failure = build_failure_record(root=ROOT)
    path = ROOT / failure_record_path(failure)
    if not path.exists():
        pytest.skip("failure record is created after pre-write synthetic validation")
    plan = build_plan_v2(root=ROOT, failure=failure)
    predecessor = json.loads((ROOT / PREDECESSOR_PLAN_PATH).read_text(encoding="utf-8"))
    for key in ("spec_id", "markets", "years", "active_calendar_id", "limits", "authority"):
        assert plan[key] == predecessor[key]
    assert plan["host_execution"]["worker_count"] == 4
    assert plan["limits"]["maximum_attempts"] == 1
    assert plan["limits"]["maximum_retries"] == 0
    assert plan["output_root"] == OUTPUT_ROOT.as_posix()
    assert plan["plan_id"] == sha256_json({key: value for key, value in plan.items() if key != "plan_id"})


def test_executor_is_windows_main_process_spawn_guarded(monkeypatch: pytest.MonkeyPatch) -> None:
    source = inspect.getsource(execute_census_v2_once)
    assert 'get_context("spawn")' in source
    assert 'Pool(processes=int(limits["maximum_workers"]))' in source
    monkeypatch.setattr("futures_rebuild.cash_open_source_compatibility_census_v2.os.name", "posix")
    with pytest.raises((UnauthorizedOperation, FileNotFoundError)):
        execute_census_v2_once(root=ROOT, boundary=None, receipt=None)  # type: ignore[arg-type]


def test_repository_runner_is_prepare_only() -> None:
    source = (ROOT / "scripts/run_cash_open_41_market_source_compatibility_census_v2.py").read_text(
        encoding="utf-8"
    )
    assert "execute_census_v2_once" not in source
    assert "issue_user_approved" not in source
    assert "BLOCKED_SEPARATE_WINDOWS_HOST_APPROVAL_REQUIRED" in source


def test_persisted_successor_plan_is_hash_bound_when_present() -> None:
    if not (ROOT / PLAN_PATH).exists():
        pytest.skip("plan is created after pre-write synthetic validation")
    plan = load_plan_v2(root=ROOT)
    assert plan["state"] == "PREPARED_NOT_EXECUTED"
    assert plan["authority"]["performance_evaluation"] is False
    output_root = ROOT / OUTPUT_ROOT
    if not output_root.exists():
        return
    reports = tuple(output_root.rglob("source_compatibility.json"))
    assert len(reports) == 1
    report = json.loads(reports[0].read_text(encoding="utf-8"))
    assert report["report_id"] == sha256_json(
        {key: value for key, value in report.items() if key != "report_id"}
    )
    assert report["plan_id"] == plan["plan_id"]
    assert report["state"] == "SEALED_UNPUBLISHED_SOURCE_ONLY_EVIDENCE"
    assert report["authority"]["price_values_emitted"] is False
    assert report["authority"]["performance_evaluation"] is False
