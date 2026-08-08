from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild.alpha_ladder_missing_session_provenance import (
    CLASSIFICATIONS,
    FORBIDDEN_REPORT_FIELDS,
    TARGETS,
    _assert_price_free,
    _scan_parquet,
    _write_once,
    classify_provenance,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _evidence(**overrides):
    value = {
        "calendar_closed": False,
        "active_causal_count": 0,
        "alternate_causal_count": 0,
        "any_causal_count": 0,
        "causal_mislabeled_count": 0,
        "raw_count": 0,
        "dbn_1m_count": 0,
        "dbn_1s_count": 0,
        "independent_trade_stream_complete": False,
        "independent_trade_count": None,
    }
    value.update(overrides)
    return value


def test_exact_sealed_target_scope_is_eight_market_sessions():
    assert len(TARGETS) == 8
    assert set(TARGETS) == {
        ("ES", "2018-12-05"),
        ("ZN", "2018-12-05"),
        ("CL", "2020-02-28"),
        ("ZN", "2020-02-28"),
        ("6E", "2020-06-30"),
        ("CL", "2020-06-30"),
        ("ES", "2020-06-30"),
        ("ZN", "2020-06-30"),
    }


@pytest.mark.parametrize(
    ("evidence", "expected", "detail"),
    [
        (
            _evidence(calendar_closed=True),
            "CALENDAR_CLOSURE",
            "AUTHORITATIVE_CME_SCHEDULE_CORRECTION",
        ),
        (
            _evidence(causal_mislabeled_count=30, raw_count=30, dbn_1m_count=30),
            "NORMALIZATION_LOSS",
            "CAUSAL_SESSION_LABEL_LOSS",
        ),
        (
            _evidence(alternate_causal_count=30, any_causal_count=30, raw_count=30, dbn_1m_count=30),
            "NORMALIZATION_LOSS",
            "ACTIVE_RELEASE_SELECTION_LOSS",
        ),
        (
            _evidence(raw_count=30, dbn_1m_count=30),
            "NORMALIZATION_LOSS",
            "RAW_TO_CAUSAL_LOSS",
        ),
        (
            _evidence(dbn_1m_count=30, dbn_1s_count=1800),
            "NORMALIZATION_LOSS",
            "DBN_TO_RAW_LOSS",
        ),
        (
            _evidence(dbn_1s_count=1800),
            "RAW_SOURCE_ABSENCE",
            "OHLCV_1M_ABSENT_WHILE_OHLCV_1S_PRESENT",
        ),
        (
            _evidence(),
            "RAW_SOURCE_ABSENCE",
            "NO_PROVIDER_REPORTED_BARS_AND_NO_NO_TRADE_PROOF",
        ),
        (
            _evidence(
                independent_trade_stream_complete=True,
                independent_trade_count=0,
            ),
            "VERIFIED_NO_TRADE",
            "COMPLETE_INDEPENDENT_TRADE_STREAM_ZERO_EVENTS",
        ),
        (
            _evidence(active_causal_count=1),
            "UNRESOLVED_EVIDENCE_CONFLICT",
            "SEALED_ACTIVE_ABSENCE_CONTRADICTED",
        ),
    ],
)
def test_classifier_is_conservative_and_deterministic(evidence, expected, detail):
    assert classify_provenance(evidence) == (expected, detail)
    assert expected in CLASSIFICATIONS


def test_absence_cannot_be_relabelled_no_trade_without_independent_trade_stream():
    classification, _detail = classify_provenance(
        _evidence(
            independent_trade_stream_complete=False,
            independent_trade_count=0,
        )
    )
    assert classification == "RAW_SOURCE_ABSENCE"


@pytest.mark.parametrize("field", sorted(FORBIDDEN_REPORT_FIELDS))
def test_price_and_economic_fields_are_rejected(field):
    with pytest.raises(IntegrityError, match="field leaked"):
        _assert_price_free({field: 1})


def test_price_free_control_fields_remain_allowed():
    _assert_price_free(
        {
            "price_free_output": True,
            "classification": "RAW_SOURCE_ABSENCE",
            "row_identity_set_sha256": "a" * 64,
            "authority": {"returns": False},
        }
    )


def test_returns_payload_is_still_rejected():
    with pytest.raises(IntegrityError, match="field leaked"):
        _assert_price_free({"returns": [0.01]})


def test_report_writer_is_create_only(tmp_path: Path):
    path = tmp_path / "report.json"
    _write_once(path, {"state": "SEALED"})
    assert json.loads(path.read_text(encoding="utf-8")) == {"state": "SEALED"}
    with pytest.raises(UnauthorizedOperation, match="already exists"):
        _write_once(path, {"state": "CHANGED"})


def test_causal_scan_detects_event_date_rows_with_wrong_session_label(tmp_path: Path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    ct = ZoneInfo("America/Chicago")
    event_at_ns = int(datetime(2020, 6, 30, 9, 45, tzinfo=ct).timestamp() * 1_000_000_000)
    path = tmp_path / "causal.parquet"
    pq.write_table(
        pa.table(
            {
                "event_at_ns": [event_at_ns],
                "exchange_session_date": ["2020-06-29"],
                "source_row_sha256": ["a" * 64],
            }
        ),
        path,
    )
    result = _scan_parquet(
        path=path, kind="causal", sessions=("2020-06-30",),
    )["2020-06-30"]
    assert result["event_date_window_count"] == 1
    assert result["session_label_window_count"] == 0
    assert result["mislabeled_window_count"] == 1
    assert result["observed_session_label_counts"] == {"2020-06-29": 1}
    assert result["price_values_included"] is False


def test_raw_scan_uses_event_clock_not_a_session_label(tmp_path: Path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    ct = ZoneInfo("America/Chicago")
    inside = int(datetime(2020, 2, 28, 9, 59, tzinfo=ct).timestamp() * 1_000_000_000)
    outside = int(datetime(2020, 2, 28, 10, 0, tzinfo=ct).timestamp() * 1_000_000_000)
    path = tmp_path / "raw.parquet"
    pq.write_table(
        pa.table(
            {
                "event_at_ns": [inside, outside],
                "row_sha256": ["b" * 64, "c" * 64],
            }
        ),
        path,
    )
    result = _scan_parquet(
        path=path, kind="raw", sessions=("2020-02-28",),
    )["2020-02-28"]
    assert result["event_date_window_count"] == 1
    assert result["session_label_window_count"] == 0
    assert result["earliest_event_at"].startswith("2020-02-28T09:59:00")


def test_calendar_closure_has_precedence_over_source_observations():
    classification, detail = classify_provenance(
        _evidence(
            calendar_closed=True,
            active_causal_count=30,
            raw_count=30,
            dbn_1m_count=30,
            dbn_1s_count=1800,
        )
    )
    assert (classification, detail) == (
        "CALENDAR_CLOSURE",
        "AUTHORITATIVE_CME_SCHEDULE_CORRECTION",
    )
