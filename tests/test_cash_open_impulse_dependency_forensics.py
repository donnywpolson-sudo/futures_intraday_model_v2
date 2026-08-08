from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from futures_rebuild.cash_open_impulse_dependency_forensics import (
    ForensicRow,
    classify_checkpoint_exact,
)
from futures_rebuild.cash_open_impulse_dependency_forensics_v2 import (
    iter_forensic_rows_v2,
)


CHICAGO = ZoneInfo("America/Chicago")


def _row(day: date, clock: time, *, identity: str = "a" * 64,
         disposition: str = "ELIGIBLE", spec=("0.25", "12.5", "50")) -> ForensicRow:
    event = int(datetime.combine(day, clock, CHICAGO).timestamp() * 1_000_000_000)
    return ForensicRow(day.isoformat(), event, disposition, identity, "b" * 64, spec)


def _complete(day: date) -> list[ForensicRow]:
    values = []
    for minute in range(8 * 60 + 30, 9 * 60):
        values.append(_row(day, time(*divmod(minute, 60))))
    for minute in range(9 * 60 + 1, 9 * 60 + 32):
        values.append(_row(day, time(*divmod(minute, 60))))
    return values


def _reasons(result: dict[str, object]) -> set[str]:
    return {str(item["reason"]) for item in result["failures"]}


def test_exact_classifier_accepts_complete_causal_path() -> None:
    day = date(2021, 2, 8)
    result = classify_checkpoint_exact(
        market="ES", session=day.isoformat(), checkpoint=time(9, 0), rows=_complete(day)
    )
    assert result["complete"] is True
    assert result["failures"] == []


def test_exact_classifier_separates_missing_nonexecuting_and_duplicate() -> None:
    day = date(2021, 2, 8)
    rows = _complete(day)
    rows = [item for item in rows if item.event_at_ns != _row(day, time(8, 45)).event_at_ns]
    rows = [
        _row(day, time(8, 46), disposition="MISSING_OR_AMBIGUOUS_MARKET_IDENTITY")
        if item.event_at_ns == _row(day, time(8, 46)).event_at_ns else item
        for item in rows
    ]
    rows.append(_row(day, time(9, 10)))
    result = classify_checkpoint_exact(
        market="ES", session=day.isoformat(), checkpoint=time(9, 0), rows=rows
    )
    assert _reasons(result) >= {
        "MISSING_MINUTE", "NON_EXECUTABLE_DISPOSITION", "DUPLICATE_EXECUTABLE_MINUTE"
    }


def test_exact_classifier_separates_identity_roll_and_spec_change() -> None:
    day = date(2021, 2, 8)
    rows = _complete(day)
    changed = []
    for item in rows:
        clock = datetime.fromtimestamp(item.event_at_ns / 1_000_000_000, CHICAGO).time()
        if clock >= time(9, 16):
            changed.append(ForensicRow(
                item.session, item.event_at_ns, item.disposition, "c" * 64,
                item.row_sha256, ("0.5", "25", "50"),
            ))
        else:
            changed.append(item)
    result = classify_checkpoint_exact(
        market="ES", session=day.isoformat(), checkpoint=time(9, 0), rows=changed
    )
    assert _reasons(result) >= {
        "IDENTITY_CHANGE", "MARKET_SPEC_CHANGE",
        "ROLL_OR_IDENTITY_CHANGE_BETWEEN_FEATURE_AND_EXECUTION",
    }


def test_exact_classifier_reports_missing_source_session_without_price_values() -> None:
    day = date(2021, 2, 8)
    result = classify_checkpoint_exact(
        market="ES", session=day.isoformat(), checkpoint=time(9, 0), rows=[]
    )
    assert result["complete"] is False
    assert _reasons(result) == {"MISSING_MINUTE"}
    assert "open" not in str(result).lower()
    assert "close" not in str(result).lower()


def test_v2_reader_causally_attaches_sessionless_nontradable_row(tmp_path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    day = date(2021, 2, 8)
    events = [
        int(datetime.combine(day, time(8, minute), CHICAGO).timestamp() * 1_000_000_000)
        for minute in (30, 31, 32)
    ]
    path = tmp_path / "sessionless.parquet"
    pq.write_table(pa.table({
        "actual_identity_hash": ["a" * 64, None, "a" * 64],
        "disposition": ["ELIGIBLE", "MISSING_OR_AMBIGUOUS_MARKET_IDENTITY", "ELIGIBLE"],
        "event_at_ns": events,
        "exchange_session_date": [day.isoformat(), None, day.isoformat()],
        "point_value": ["50", None, "50"],
        "source_row_sha256": ["b" * 64, "c" * 64, "d" * 64],
        "tick_size": ["0.25", None, "0.25"],
        "tick_value": ["12.5", None, "12.5"],
    }), path)
    rows = list(iter_forensic_rows_v2(path))
    assert [row.session for row in rows] == [day.isoformat()] * 3
    assert rows[1].disposition == "MISSING_OR_AMBIGUOUS_MARKET_IDENTITY"
    assert rows[1].executable is False
