from __future__ import annotations

import inspect
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild.cash_open_source_compatibility import SourceRow
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.reported_bar_fixed_horizon_census import (
    OUTPUT_ROOT,
    PLAN_PATH,
    REQUIRED_COLUMNS,
    build_plan,
    certify_market_checkpoint,
    execute_once,
    load_plan,
    select_configuration,
)


ROOT = Path(__file__).resolve().parents[1]
CT = ZoneInfo("America/Chicago")


def _row(session: str, local: datetime, identity: str = "contract") -> SourceRow:
    event_ns = int(local.astimezone(timezone.utc).timestamp() * 1_000_000_000)
    return SourceRow(
        "ES", session, event_ns, event_ns + 65_000_000_000,
        True, identity, "a" * 64,
    )


def _sessions(count: int = 1100) -> list[str]:
    start = datetime(2018, 1, 1, tzinfo=CT)
    return [(start + timedelta(days=value)).date().isoformat() for value in range(count)]


def _complete_rows(sessions: list[str], checkpoint: str = "09:00") -> dict[str, tuple[SourceRow, ...]]:
    clock = datetime.strptime(checkpoint, "%H:%M").time()
    result: dict[str, tuple[SourceRow, ...]] = {}
    for session in sessions:
        center = datetime.combine(datetime.fromisoformat(session).date(), clock, tzinfo=CT)
        rows = [_row(session, center - timedelta(minutes=value)) for value in range(30, 0, -2)]
        rows.extend([_row(session, center + timedelta(minutes=1)), _row(session, center + timedelta(minutes=31))])
        result[session] = tuple(rows)
    return result


def test_plan_is_hash_bound_and_prepare_only() -> None:
    if not (ROOT / PLAN_PATH).exists():
        plan = build_plan(root=ROOT)
    else:
        plan = load_plan(root=ROOT)
    assert len(plan["markets"]) == 41
    assert plan["checkpoint_grid"] == ["09:00", "09:30", "10:00", "10:30"]
    assert plan["execution_limits"]["maximum_attempts"] == 1
    source = (ROOT / "scripts/run_reported_bar_fixed_horizon_source_census.py").read_text(
        encoding="utf-8"
    )
    assert "execute_once" not in source and "issue_user_approved" not in source


def test_reader_schema_and_output_are_price_free() -> None:
    assert REQUIRED_COLUMNS == {
        "actual_identity_hash", "disposition", "event_at_ns",
        "exchange_session_date", "source_row_sha256",
    }
    source = inspect.getsource(execute_once)
    for forbidden in ("open_nano", "high_nano", "low_nano", "close_nano", "net_pnl", "sharpe", "sortino"):
        assert forbidden not in source
    assert ".glob(" not in source and ".rglob(" not in source


def test_complete_synthetic_market_checkpoint_passes_all_gates() -> None:
    sessions = _sessions()
    result = certify_market_checkpoint(
        market="ES", checkpoint="09:00", eligible_sessions=sessions,
        rows_by_session=_complete_rows(sessions), catalog_complete=True,
    )
    assert result["status"] == "PASS"
    assert result["failed_gates"] == []
    assert result["overall"]["candidate_path_percent"] == 100
    assert result["overall"]["always_direction_baseline_path_percent"] == 100


def test_decision_abstentions_can_pass_but_future_gap_fails() -> None:
    sessions = _sessions()
    rows = _complete_rows(sessions)
    for session in sessions[:40]:
        rows[session] = ()
    passing = certify_market_checkpoint(
        market="ES", checkpoint="09:00", eligible_sessions=sessions,
        rows_by_session=rows, catalog_complete=True,
    )
    assert passing["status"] == "FAIL"  # always-long/short cannot prove entry on abstention days
    assert "ALWAYS_DIRECTION_BASELINE_PATH_100_PERCENT" in passing["failed_gates"]
    rows = _complete_rows(sessions)
    rows[sessions[-1]] = rows[sessions[-1]][:-1]
    failed = certify_market_checkpoint(
        market="ES", checkpoint="09:00", eligible_sessions=sessions,
        rows_by_session=rows, catalog_complete=True,
    )
    assert "CANDIDATE_EXECUTION_PATH_100_PERCENT" in failed["failed_gates"]


def test_catalog_failure_and_one_fold_failure_fail_closed() -> None:
    sessions = _sessions()
    result = certify_market_checkpoint(
        market="ES", checkpoint="09:00", eligible_sessions=sessions,
        rows_by_session=_complete_rows(sessions), catalog_complete=False,
        catalog_failures=("2020:ABSENT",),
    )
    assert result["status"] == "FAIL"
    assert "ACTIVE_CATALOG_COMPLETE_2018_2022" in result["failed_gates"]


def test_selection_is_deterministic_and_never_uses_returns() -> None:
    results = [
        {"market": market, "checkpoint": checkpoint, "status": "PASS", "fold_results": [
            {"training": {"feature_complete_percent": completeness},
             "evaluation": {"feature_complete_percent": completeness}}
        ]}
        for checkpoint, markets, completeness in (
            ("09:00", ("ES", "NQ"), 99.0),
            ("09:30", ("ES", "NQ"), 99.5),
        )
        for market in markets
    ]
    selected = select_configuration(results)
    assert selected["selected_checkpoint"] == "09:30"
    assert selected["selected_markets"] == ["ES", "NQ"]
    source = inspect.getsource(select_configuration).lower()
    for forbidden in ("net_pnl", "gross_pnl", "sharpe", "sortino", "realized_return"):
        assert forbidden not in source


def test_executor_refuses_non_windows_host_before_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    if not (ROOT / PLAN_PATH).exists():
        pytest.skip("plan is created after pre-write validation")
    monkeypatch.setattr("futures_rebuild.reported_bar_fixed_horizon_census.os.name", "posix")
    with pytest.raises(UnauthorizedOperation):
        execute_once(root=ROOT, boundary=None, receipt=None)  # type: ignore[arg-type]
    output = ROOT / OUTPUT_ROOT
    if not output.exists():
        return
    reports = tuple(output.rglob("source_census.json"))
    assert len(reports) == 1
    report = json.loads(reports[0].read_text(encoding="utf-8"))
    assert report["report_id"] == sha256_json(
        {key: value for key, value in report.items() if key != "report_id"}
    )
    assert report["authority"]["price_values_emitted"] is False
    assert report["authority"]["performance_evaluation"] is False
