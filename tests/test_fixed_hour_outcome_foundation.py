from __future__ import annotations

import json
import hashlib
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import numpy as np

from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.exchange_calendar import CalendarSession, TradingStateInterval, VerifiedExchangeCalendar
from futures_rebuild.errors import IntegrityError
from futures_rebuild.fixed_hour_outcome_foundation import (
    SYNTHETIC_SOURCE_CONTRACT_ID,
    SYNTHETIC_SOURCE_RELEASE_ID,
    build_directional_path_outcome,
    frozen_outcome_foundation_contract,
    globex_hourly_opportunity_grid,
    synthetic_globex_hourly_opportunity_grid,
    validate_globex_opportunity,
)
from futures_rebuild.fixed_hour_feature_expansion import build_bound_feature_record
from futures_rebuild.fixed_hour_ridge_discovery import NANO


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.current


def _row(start: int, price: float) -> dict[str, object]:
    value = int(price * NANO)
    row_id = hashlib.sha256(f"row:{start}".encode()).hexdigest()
    return {
        "row_id": row_id,
        "market": "ES",
        "source_contract_id": SYNTHETIC_SOURCE_CONTRACT_ID,
        "source_release_id": SYNTHETIC_SOURCE_RELEASE_ID,
        "source_file_sha256": "1" * 64,
        "source_row_sha256": hashlib.sha256(f"source:{start}".encode()).hexdigest(),
        "definition_source_file_sha256": "2" * 64,
        "definition_row_sha256": "3" * 64,
        "source_cadence": "1m",
        "bar_start_ns": start,
        "bar_end_ns": start + 60 * NANO,
        "source_timestamp_ns": start,
        "available_at_ns": start + 65 * NANO,
        "decision_eligible_at_ns": start + 65 * NANO,
        "open_nano": value,
        "high_nano": value + int(0.25 * NANO),
        "low_nano": value - int(0.25 * NANO),
        "close_nano": value,
        "volume": 100,
        "actual_contract": "ESH2",
        "min_price_increment_nano": int(0.25 * NANO),
        "multiplier_nano": 50 * NANO,
        "project_trade_date": "2022-01-10",
        "official_schedule_state": "AUTHORITATIVE_APPLICABLE",
    }


def test_directional_path_targets_are_separate_cost_neutral_and_maturity_gated() -> None:
    contract = frozen_outcome_foundation_contract()
    assert contract["model_target_components"] == [
        "terminal_change_ticks",
        "maximum_upside_excursion_ticks",
        "maximum_downside_excursion_ticks",
    ]
    assert contract["separation"]["feature_columns_in_outcome"] is False
    assert contract["separation"]["costs_embedded_in_targets"] is False
    assert contract["timing"] == {
        "timezone": "America/Los_Angeles",
        "scope": "PREDECLARED_PLUS_22_MINUTE_ALIGNMENT_IN_EVERY_VERIFIED_GLOBEX_OPEN_INTERVAL",
        "first_decision": "AFTER_21_CONSECUTIVE_OPEN_BARS_PRODUCING_20_TICK_CHANGES",
        "cadence_minutes": 60,
        "decision_delay_seconds": 5,
        "entry": "NEXT_ONE_MINUTE_BAR_OPEN_PRINT_PROXY",
        "path_minutes": 60,
        "terminal_exit": "ENTRY_PLUS_60_MINUTES_BAR_OPEN_PRINT_PROXY",
        "interval_boundary": "FEATURE_ENTRY_PATH_AND_EXIT_BAR_MUST_FIT_ONE_OPEN_INTERVAL",
        "alignment_claim_ceiling": "ONLY_THIS_FIXED_ALIGNMENT_NOT_ALL_POSSIBLE_HOURLY_STARTS",
    }
    path = ROOT / "configs/fixed_hour_directional_outcome_foundation_v3.json"
    published = json.loads(path.read_text(encoding="utf-8"))
    assert published == contract
    assert path.read_bytes() == canonical_bytes(contract) + b"\n"

    interval_start = datetime(2022, 1, 9, 23, 0, tzinfo=timezone.utc)
    calendar_release_id = "a" * 64
    session = CalendarSession("ES", datetime(2022, 1, 10).date(), (TradingStateInterval(
        "OPEN", interval_start, interval_start + timedelta(hours=23),
    ),))
    opportunities = synthetic_globex_hourly_opportunity_grid(
        session, calendar_release_id=calendar_release_id,
    )
    assert len(opportunities) == 22
    assert opportunities[0]["decision_at_ns"] == int(
        (interval_start + timedelta(minutes=21, seconds=5)).timestamp() * NANO
    )
    assert opportunities[0]["entry_bar_start_ns"] == int(
        (interval_start + timedelta(minutes=22)).timestamp() * NANO
    )
    assert opportunities[0]["terminal_exit_bar_start_ns"] == int(
        (interval_start + timedelta(minutes=82)).timestamp() * NANO
    )
    assert opportunities[0]["market"] == "ES"
    assert opportunities[0]["trade_date"] == "2022-01-10"
    assert opportunities[0]["calendar_release_id"] == calendar_release_id
    assert all(
        left["terminal_exit_bar_start_ns"] == right["entry_bar_start_ns"]
        for left, right in zip(opportunities, opportunities[1:])
    )
    start_ns = int(interval_start.timestamp() * NANO)
    rows = [
        _row(start_ns + index * 60 * NANO, 100.0 + (max(index - 22, 0) // 10) * 0.25)
        for index in range(23 * 60)
    ]
    rows[42]["high_nano"] = int(103.0 * NANO)
    rows[32]["low_nano"] = int(99.5 * NANO)
    available_at = int(rows[82]["decision_eligible_at_ns"])
    feature_record = build_bound_feature_record(rows, opportunities[0])
    assert len(feature_record["feature_values"]) == 18
    assert feature_record["audit"]["bar_count"] == 21
    assert feature_record["opportunity_id"] == opportunities[0]["opportunity_id"]
    assert feature_record["source_release_id"] == SYNTHETIC_SOURCE_RELEASE_ID
    assert len(feature_record["audit"]["source_row_ids"]) == 21

    hidden = build_directional_path_outcome(
        rows, opportunities[0], expected_contract="ESH2", as_of_ns=available_at - 1,
    )
    assert hidden["status"] == "UNMATURED" and hidden["targets"] is None

    matured = build_directional_path_outcome(
        rows, opportunities[0], expected_contract="ESH2", as_of_ns=available_at,
    )
    assert matured["status"] == "MATURED"
    assert matured["targets"] == {
        "terminal_change_ticks": 6.0,
        "maximum_upside_excursion_ticks": 12.0,
        "maximum_downside_excursion_ticks": 2.0,
        "long_raw_mfe_mae": 6.0,
        "short_raw_mfe_mae": 1.0 / 6.0,
    }
    assert len(matured["audit"]["path_bar_starts_ns"]) == 60
    assert len(matured["audit"]["source_row_ids"]) == 61
    last_feature = build_bound_feature_record(rows, opportunities[-1])
    last = build_directional_path_outcome(
        rows, opportunities[-1], expected_contract="ESH2",
        as_of_ns=int(rows[-1]["decision_eligible_at_ns"]),
    )
    assert last_feature["audit"]["bar_count"] == 21 and last["status"] == "MATURED"


def test_directional_targets_accept_zero_and_negative_prices_but_reject_off_tick() -> None:
    start = datetime(2022, 1, 9, 23, tzinfo=timezone.utc)
    session = CalendarSession("ES", datetime(2022, 1, 10).date(), (
        TradingStateInterval("OPEN", start, start + timedelta(hours=4)),
    ))
    opportunity = synthetic_globex_hourly_opportunity_grid(
        session, calendar_release_id="a" * 64,
    )[0]
    start_ns = int(start.timestamp() * NANO)
    rows = [_row(start_ns + index * 60 * NANO, -1.0 + index * 0.25) for index in range(240)]
    available_at = int(rows[82]["decision_eligible_at_ns"])
    result = build_directional_path_outcome(
        rows, opportunity, expected_contract="ESH2", as_of_ns=available_at,
    )
    assert result["status"] == "MATURED"
    assert all(np.isfinite(value) for value in result["targets"].values())

    off_tick = [dict(row) for row in rows]
    off_tick[42]["close_nano"] += 1
    with pytest.raises(IntegrityError, match="one-minute OHLCV semantics"):
        build_directional_path_outcome(
            off_tick, opportunity, expected_contract="ESH2", as_of_ns=available_at,
        )


def _verified_calendar(monkeypatch: pytest.MonkeyPatch, session: CalendarSession, release_id: str) -> VerifiedExchangeCalendar:
    monkeypatch.setattr(VerifiedExchangeCalendar, "verify", lambda self: None)
    calendar = object.__new__(VerifiedExchangeCalendar)
    object.__setattr__(calendar, "sessions", {(session.market, session.trade_date): session})
    object.__setattr__(calendar, "receipt", SimpleNamespace(release_id=release_id))
    return calendar


def test_phase4_rejects_wrong_identity_calendar_and_source_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    start = datetime(2022, 1, 9, 23, tzinfo=timezone.utc)
    session = CalendarSession("ES", datetime(2022, 1, 10).date(), (
        TradingStateInterval("OPEN", start, start + timedelta(hours=4)),
    ))
    calendar = _verified_calendar(monkeypatch, session, "a" * 64)
    opportunities = globex_hourly_opportunity_grid(
        calendar, market="ES", trade_date="2022-01-10",
        source_contract_id="b" * 64, source_release_id="c" * 64,
    )
    opportunity = opportunities[0]
    rows = [_row(int(start.timestamp() * NANO) + index * 60 * NANO, 100.0) for index in range(240)]
    for row in rows:
        row["source_contract_id"] = "b" * 64
        row["source_release_id"] = "c" * 64

    for field, value in (
        ("market", "NQ"),
        ("project_trade_date", "2022-01-11"),
        ("source_contract_id", "d" * 64),
        ("source_release_id", "d" * 64),
    ):
        changed = [dict(row) for row in rows]
        changed[22][field] = value
        with pytest.raises(IntegrityError):
            build_directional_path_outcome(
                changed, opportunity, expected_contract="ESH2",
                as_of_ns=int(session.intervals[0].ends_at_utc.timestamp() * NANO), calendar=calendar,
            )
        changed_feature = [dict(row) for row in rows]
        changed_feature[10][field] = value
        with pytest.raises(IntegrityError):
            build_bound_feature_record(changed_feature, opportunity, calendar=calendar)

    other_session = CalendarSession("ES", datetime(2022, 1, 11).date(), session.intervals)
    other_calendar = _verified_calendar(monkeypatch, other_session, "d" * 64)
    with pytest.raises(IntegrityError):
        validate_globex_opportunity(opportunity, calendar=other_calendar)
    with pytest.raises(IntegrityError):
        globex_hourly_opportunity_grid(
            calendar, market="ES", trade_date="2022-01-11",
            source_contract_id="b" * 64, source_release_id="c" * 64,
        )


def test_phase4_rejects_malformed_filled_and_duplicate_rows_and_counts_exit_open() -> None:
    start = datetime(2022, 1, 9, 23, tzinfo=timezone.utc)
    session = CalendarSession("ES", datetime(2022, 1, 10).date(), (
        TradingStateInterval("OPEN", start, start + timedelta(hours=4)),
    ))
    opportunity = synthetic_globex_hourly_opportunity_grid(session, calendar_release_id="a" * 64)[0]
    rows = [_row(int(start.timestamp() * NANO) + index * 60 * NANO, 100.0) for index in range(240)]
    as_of = int(session.intervals[0].ends_at_utc.timestamp() * NANO)

    mutations = []
    long_bar = [dict(row) for row in rows]
    long_bar[30]["bar_end_ns"] += 30 * NANO
    mutations.append(long_bar)
    off_grid = [dict(row) for row in rows]
    off_grid[30]["high_nano"] += 1
    mutations.append(off_grid)
    filled = [dict(row) for row in rows]
    filled[30]["volume"] = 0
    mutations.append(filled)
    duplicate_start = [dict(row) for row in rows]
    duplicate_start[30]["bar_start_ns"] = duplicate_start[29]["bar_start_ns"]
    duplicate_start[30]["bar_end_ns"] = duplicate_start[29]["bar_end_ns"]
    duplicate_start[30]["source_timestamp_ns"] = duplicate_start[29]["source_timestamp_ns"]
    mutations.append(duplicate_start)
    duplicate_row_id = [dict(row) for row in rows]
    duplicate_row_id[30]["row_id"] = duplicate_row_id[29]["row_id"]
    mutations.append(duplicate_row_id)
    for changed in mutations:
        with pytest.raises(IntegrityError):
            build_directional_path_outcome(
                changed, opportunity, expected_contract="ESH2", as_of_ns=as_of,
            )

    jumped = [dict(row) for row in rows]
    jumped[82]["open_nano"] = jumped[82]["high_nano"] = jumped[82]["close_nano"] = int(125 * NANO)
    jumped[82]["low_nano"] = int(124.75 * NANO)
    matured = build_directional_path_outcome(
        jumped, opportunity, expected_contract="ESH2", as_of_ns=as_of,
    )
    assert matured["targets"]["terminal_change_ticks"] == 100.0
    assert matured["targets"]["maximum_upside_excursion_ticks"] == 100.0
    assert matured["audit"]["maximum_upside_price_source"] == "EXIT_OPEN"
