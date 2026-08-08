from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild.cash_open_source_compatibility import SourceRow
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.reported_bar_trade_triggered_census import (
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


def _row(
    session: str,
    local: datetime,
    *,
    available: datetime | None = None,
    identity: str = "contract",
) -> SourceRow:
    event_ns = int(local.astimezone(timezone.utc).timestamp() * 1_000_000_000)
    available_at = available or local + timedelta(seconds=65)
    available_ns = int(available_at.astimezone(timezone.utc).timestamp() * 1_000_000_000)
    return SourceRow("ES", session, event_ns, available_ns, True, identity, "a" * 64)


def _sessions(count: int = 1100) -> list[str]:
    start = datetime(2018, 1, 1, tzinfo=CT)
    return [(start + timedelta(days=value)).date().isoformat() for value in range(count)]


def _complete_rows(
    sessions: list[str], checkpoint: str = "09:00"
) -> dict[str, tuple[SourceRow, ...]]:
    clock = datetime.strptime(checkpoint, "%H:%M").time()
    result: dict[str, tuple[SourceRow, ...]] = {}
    for session in sessions:
        center = datetime.combine(datetime.fromisoformat(session).date(), clock, tzinfo=CT)
        rows = [_row(session, center - timedelta(minutes=value)) for value in range(30, 0, -2)]
        decision = center + timedelta(seconds=5)
        trigger = _row(session, center, available=decision + timedelta(seconds=60))
        entry = _row(session, center + timedelta(minutes=2))
        exit_bar = _row(session, center + timedelta(minutes=32))
        rows.extend((trigger, entry, exit_bar))
        result[session] = tuple(rows)
    return result


def test_plan_is_hash_bound_prepare_only_and_four_worker() -> None:
    plan = load_plan(root=ROOT) if (ROOT / PLAN_PATH).exists() else build_plan(root=ROOT)
    assert len(plan["markets"]) == 41
    assert plan["checkpoint_grid"] == ["09:00", "09:30", "10:00", "10:30"]
    assert plan["execution_limits"]["maximum_attempts"] == 1
    assert plan["execution_limits"]["maximum_retries"] == 0
    assert plan["execution_limits"]["maximum_workers"] == 4
    source = (ROOT / "scripts/run_reported_bar_trade_triggered_source_census.py").read_text(
        encoding="utf-8"
    )
    assert "execute_once" not in source and "issue_user_approved" not in source


def test_reader_and_output_are_price_free_and_catalog_only() -> None:
    assert REQUIRED_COLUMNS == {
        "actual_identity_hash", "disposition", "event_at_ns",
        "exchange_session_date", "source_row_sha256",
    }
    source = inspect.getsource(execute_once)
    for forbidden in (
        "open_nano", "high_nano", "low_nano", "close_nano", "net_pnl", "sharpe", "sortino"
    ):
        assert forbidden not in source
    assert ".glob(" not in source and ".rglob(" not in source
    assert "resolve(" in source


def test_complete_triggered_market_checkpoint_passes() -> None:
    sessions = _sessions()
    result = certify_market_checkpoint(
        market="ES",
        checkpoint="09:00",
        eligible_sessions=sessions,
        rows_by_session=_complete_rows(sessions),
        catalog_complete=True,
    )
    assert result["status"] == "PASS"
    assert result["failed_gates"] == []
    assert result["overall"]["candidate_triggered_path_percent"] == 100
    assert all(
        value == 100
        for value in result["overall"]["active_baseline_triggered_path_percent"].values()
    )


def test_no_reported_trigger_is_accounted_valid_no_trade() -> None:
    sessions = _sessions()
    rows = _complete_rows(sessions)
    for session in sessions:
        rows[session] = rows[session][:-3]
    result = certify_market_checkpoint(
        market="ES", checkpoint="09:00", eligible_sessions=sessions,
        rows_by_session=rows, catalog_complete=True,
    )
    assert result["status"] == "PASS"
    assert result["overall"]["candidate_triggered_path_expected"] == 0
    assert result["overall"]["candidate_dispositions"] == {
        "EXPLICIT_CAUSAL_NO_TRADE_TIMEOUT": len(sessions)
    }


def test_trigger_without_later_entry_fails_candidate_and_baselines() -> None:
    sessions = _sessions()
    rows = _complete_rows(sessions)
    broken = sessions[-1]
    rows[broken] = tuple(item for item in rows[broken] if item.event_at_ns != rows[broken][-2].event_at_ns)
    result = certify_market_checkpoint(
        market="ES", checkpoint="09:00", eligible_sessions=sessions,
        rows_by_session=rows, catalog_complete=True,
    )
    assert result["status"] == "FAIL"
    assert "CANDIDATE_TRIGGERED_PATH_100_PERCENT" in result["failed_gates"]
    assert "ALWAYS_LONG_TRIGGERED_PATH_100_PERCENT" in result["failed_gates"]


def test_late_exit_and_identity_change_fail_closed() -> None:
    sessions = _sessions()
    rows = _complete_rows(sessions)
    broken = sessions[-1]
    changed = list(rows[broken])
    exit_row = changed[-1]
    changed[-1] = SourceRow(
        exit_row.market, exit_row.session, exit_row.event_at_ns, exit_row.available_at_ns,
        True, "rolled", exit_row.source_row_sha256,
    )
    rows[broken] = tuple(changed)
    result = certify_market_checkpoint(
        market="ES", checkpoint="09:00", eligible_sessions=sessions,
        rows_by_session=rows, catalog_complete=True,
    )
    assert result["status"] == "FAIL"
    assert "CANDIDATE_TRIGGERED_PATH_100_PERCENT" in result["failed_gates"]


def test_aggregate_coverage_cannot_hide_fold_or_baseline_failure() -> None:
    sessions = _sessions()
    rows = _complete_rows(sessions)
    for session in sessions[:120]:
        rows[session] = ()
    result = certify_market_checkpoint(
        market="ES", checkpoint="09:00", eligible_sessions=sessions,
        rows_by_session=rows, catalog_complete=True,
    )
    assert result["status"] == "FAIL"
    assert any(
        "FEATURE_COMPLETE_MARKET_FOLD_90_PERCENT" in fold["failed_gates"]
        or "MINIMUM_252_COMPLETE_TRAINING_SESSIONS" in fold["failed_gates"]
        for fold in result["fold_results"]
    )


def test_selection_is_deterministic_and_return_free() -> None:
    results = [
        {
            "market": market,
            "checkpoint": checkpoint,
            "status": "PASS",
            "fold_results": [{
                "training": {"feature_complete_percent": completeness},
                "evaluation": {"feature_complete_percent": completeness},
            }],
        }
        for checkpoint, markets, completeness in (
            ("09:00", ("ES", "NQ"), 99.0),
            ("09:30", ("ES", "NQ"), 99.5),
        )
        for market in markets
    ]
    selected = select_configuration(results)
    assert selected["selected_checkpoint"] == "09:30"
    source = inspect.getsource(select_configuration).lower()
    for forbidden in ("net_pnl", "gross_pnl", "sharpe", "sortino", "realized_return"):
        assert forbidden not in source


def test_executor_refuses_non_windows_before_any_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    if not (ROOT / PLAN_PATH).exists():
        pytest.skip("plan follows pre-write validation")
    monkeypatch.setattr("futures_rebuild.reported_bar_trade_triggered_census.os.name", "posix")
    with pytest.raises(UnauthorizedOperation):
        execute_once(root=ROOT, boundary=None, receipt=None)  # type: ignore[arg-type]
