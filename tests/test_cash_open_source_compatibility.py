from __future__ import annotations

import inspect
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.cash_open_source_compatibility import (
    CHECKPOINT_GRID,
    REJECTED_PROTOCOL_ID,
    SourceRow,
    build_market_calendar_folds,
    build_rejected_protocol_closure,
    certify_market_configuration,
    classify_checkpoint,
    select_compatible_market_set,
    source_row_from_mapping,
)
from futures_rebuild.cash_open_source_compatibility_census import build_census_plan
from futures_rebuild.cash_open_source_compatibility_correction import build_correction
from futures_rebuild.certified_research_gateway import CertifiedResearchGateway
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]
GRID_PATH = ROOT / (
    "state/unpublished_evidence/cash_open_impulse_41_market_calendar_grid_successor/"
    "cd64f912cceec3ff613b0d28f3965804c25d36d9b940d622b062128cfca0843b/"
    "historical_calendar_successor.json"
)
BASE_PATH = ROOT / (
    "state/calendar_registry/cash_open_impulse_41_market/"
    "54bc5550a0ba28af2a509fb32c756b39041686ba10ffa6bd832e6d96469c0397/"
    "historical_calendar_successor.json"
)


def _event(session: str, clock: str) -> int:
    local = datetime.fromisoformat(f"{session}T{clock}:00").replace(
        tzinfo=ZoneInfo("America/Chicago")
    )
    return int(local.timestamp() * 1_000_000_000)


def _complete_rows(
    *, session: str = "2020-01-02", checkpoint: str = "09:00", identity: str = "a" * 64
) -> tuple[SourceRow, ...]:
    center = datetime.fromisoformat(f"{session}T{checkpoint}:00")
    clocks = [center - timedelta(minutes=value) for value in range(30, 0, -1)]
    clocks += [center + timedelta(minutes=value) for value in range(1, 32)]
    return tuple(
        SourceRow(
            market="ES", session=session,
            event_at_ns=_event(session, item.strftime("%H:%M")),
            available_at_ns=_event(session, item.strftime("%H:%M")) + 65_000_000_000,
            executable=True, actual_identity_hash=identity,
            source_row_sha256=f"{index + 1:064x}",
        )
        for index, item in enumerate(clocks)
    )


def test_grid_calendar_is_additive_exact_and_inactive() -> None:
    assert sha256_file(GRID_PATH) == "e76ec4310da674e1bbacf5356662d97d8a2c8b115c728fa9386b53f8d289be52"
    grid = json.loads(GRID_PATH.read_text(encoding="utf-8"))
    base = json.loads(BASE_PATH.read_text(encoding="utf-8"))
    assert grid["calendar_id"] == sha256_json(
        {key: value for key, value in grid.items() if key != "calendar_id"}
    )
    assert grid["decision"] == "PASS_EXACT_REFERENCE_COVERAGE"
    assert grid["status"] == "PREPARED_INACTIVE_UNPUBLISHED"
    assert grid["checkpoint_grid"] == list(CHECKPOINT_GRID)
    assert grid["unresolved_reference_count"] == 0
    assert len(grid["calendar_rows"]) == len(base["calendar_rows"]) == 74_866
    for before, after in zip(base["calendar_rows"], grid["calendar_rows"], strict=True):
        assert before["market"] == after["market"]
        assert before["trade_date"] == after["trade_date"]
        for checkpoint in ("09:00", "10:30"):
            assert before["checkpoint_open"][checkpoint] == after["checkpoint_open"][checkpoint]
            assert before["disposition"][checkpoint] == after["disposition"][checkpoint]
        assert set(after["checkpoint_open"]) == set(CHECKPOINT_GRID)
    assert grid["authority"]["year_2025_accessed"] is False
    assert {row["trade_date"][:4] for row in grid["calendar_rows"]} == {
        "2018", "2019", "2020", "2021", "2022"
    }


def test_feature_gap_has_decision_unavailable_semantics() -> None:
    rows = _complete_rows()[:-32]
    result = classify_checkpoint(
        market="ES", session="2020-01-02", checkpoint="09:00", rows=rows
    )
    assert result.failure == "DECISION_UNAVAILABLE_DUE_TO_FEATURE_GAP"
    assert not result.decision_available and not result.entry_after_decision


def test_future_execution_gap_is_explicit_and_not_a_timing_error() -> None:
    rows = _complete_rows()[:-1]
    result = classify_checkpoint(
        market="ES", session="2020-01-02", checkpoint="09:00", rows=rows
    )
    assert result.failure == "EXECUTION_PATH_INCOMPLETE_OR_IDENTITY_CHANGING"
    assert result.decision_available and result.feature_complete
    assert not result.execution_complete


def test_complete_checkpoint_passes_and_identity_change_fails() -> None:
    rows = _complete_rows()
    assert classify_checkpoint(
        market="ES", session="2020-01-02", checkpoint="09:00", rows=rows
    ).complete
    changed = list(rows)
    changed[-1] = SourceRow(**{**changed[-1].__dict__, "actual_identity_hash": "b" * 64})
    result = classify_checkpoint(
        market="ES", session="2020-01-02", checkpoint="09:00", rows=changed
    )
    assert result.failure == "EXECUTION_PATH_INCOMPLETE_OR_IDENTITY_CHANGING"


def test_source_mapping_rejects_2025_and_reads_no_price_field() -> None:
    mapping = {
        "exchange_session_date": "2020-01-02", "event_at_ns": _event("2020-01-02", "09:00"),
        "source_row_sha256": "1" * 64, "disposition": "ELIGIBLE",
        "actual_identity_hash": "2" * 64,
        "open_nano": object(),
    }
    assert source_row_from_mapping(market="ES", row=mapping).executable
    mapping["exchange_session_date"] = "2025-01-02"
    with pytest.raises(IntegrityError):
        source_row_from_mapping(market="ES", row=mapping)


def test_folds_are_calendar_ordinal_and_ignore_source_rows() -> None:
    start = datetime(2018, 1, 1)
    sessions = tuple((start + timedelta(days=index)).date().isoformat() for index in range(1100))
    folds = build_market_calendar_folds(sessions)
    assert len(folds) == 8
    assert len(folds[0]["training_sessions"]) == 504
    assert len(folds[0]["evaluation_sessions"]) == 63
    assert len(folds[-1]["training_sessions"]) == 945


def test_one_missing_path_forces_zero_tolerance_failure() -> None:
    start = datetime(2018, 1, 1)
    sessions = tuple((start + timedelta(days=index)).date().isoformat() for index in range(1100))
    rows = {session: _complete_rows(session=session) for session in sessions}
    rows[sessions[0]] = rows[sessions[0]][:-1]
    result = certify_market_configuration(
        market="ES", checkpoints=("09:00",), eligible_sessions=sessions,
        rows_by_session=rows, catalog_complete=True,
    )
    assert result["status"] == "FAIL"
    assert "ONE_HUNDRED_PERCENT_CAUSAL_PATH_COVERAGE" in result["failed_gates"]


def test_source_only_selection_rule_is_deterministic() -> None:
    primary = select_compatible_market_set({("09:00", "10:30"): ["ES", "CL"]})
    assert primary["selection_stage"] == "PRIMARY"
    fallback = select_compatible_market_set({
        ("09:00", "10:30"): ["ES"],
        ("09:00", "10:00"): ["ZN", "ES", "CL"],
        ("09:30", "10:30"): ["GC", "NQ", "RTY"],
    })
    assert fallback["selected_checkpoints"] == ["09:00", "10:00"]
    assert fallback["selected_markets"] == ["CL", "ES", "ZN"]
    single = select_compatible_market_set({("09:30",): ["ES", "CL"]})
    assert single["selection_stage"] == "FALLBACK_SINGLE"
    assert select_compatible_market_set({})["decision"].startswith("REJECTED")


def test_rejected_protocol_closure_is_hash_bound_and_gateway_denied(tmp_path: Path) -> None:
    closure = build_rejected_protocol_closure(root=ROOT)
    assert closure["record_id"] == sha256_json(
        {key: value for key, value in closure.items() if key != "record_id"}
    )
    assert closure["economic_result"] == "NOT_PRODUCED"
    gateway = object.__new__(CertifiedResearchGateway)
    with pytest.raises(UnauthorizedOperation, match="cannot be registered"):
        CertifiedResearchGateway.register_trial(
            gateway,
            registration_path=tmp_path / "trial.json",
            registration={"protocol_id": REJECTED_PROTOCOL_ID},
            readiness_evidence_path=tmp_path / "readiness.json",
        )


def test_mutable_pointer_preparation_is_preserved_invalid_and_corrected() -> None:
    invalidity, corrected = build_correction(root=ROOT)
    assert invalidity["classification"] == "INVALID_PRE_DATA_MUTABLE_POINTER_BINDING"
    assert invalidity["economic_result"] == "NOT_PRODUCED"
    assert corrected["spec_id"] == sha256_json(
        {key: value for key, value in corrected.items() if key != "spec_id"}
    )
    assert corrected["prepared_calendar"]["calendar_id"] == (
        "cd64f912cceec3ff613b0d28f3965804c25d36d9b940d622b062128cfca0843b"
    )
    assert corrected["catalog_inventory"]["market_count"] == 41
    assert corrected["catalog_inventory"]["expected_market_years"] == 205
    assert corrected["authority"]["historical_row_read"] is False


def test_current_census_source_has_no_archive_discovery_or_economics() -> None:
    import futures_rebuild.cash_open_source_compatibility as mechanics
    import futures_rebuild.cash_open_source_compatibility_census as census

    source = inspect.getsource(mechanics) + inspect.getsource(census)
    assert ".glob(" not in source and ".rglob(" not in source
    assert "open_nano" not in inspect.getsource(census)
    assert "net_pnl" not in source and "sharpe" not in source and "sortino" not in source
    spec = ROOT / "configs/cash_open_41_market_source_compatibility_spec_v2.json"
    if not spec.exists():
        with pytest.raises((IntegrityError, FileNotFoundError)):
            build_census_plan(root=ROOT)
        return

    active_pointer = json.loads(
        (ROOT / "configs/active_cash_open_impulse_historical_calendar.json").read_text(
            encoding="utf-8"
        )
    )
    if active_pointer.get("calendar_id") != json.loads(
        GRID_PATH.read_text(encoding="utf-8")
    )["calendar_id"]:
        with pytest.raises(UnauthorizedOperation):
            build_census_plan(root=ROOT)
        return

    plan = build_census_plan(root=ROOT)
    assert plan["state"] == "PREPARED_NOT_EXECUTED"
    assert plan["authority"]["performance_evaluation"] is False
    assert plan["authority"]["registration"] is False
    assert plan["authority"]["publication"] is False
