from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.reported_bar_fixed_horizon_protocol import (
    PROTOCOL_PATH,
    ReportedBarEvidence,
    build_protocol,
    build_rejection_record,
    build_topology_audit,
    classify_reported_bar_checkpoint,
    rejection_path,
    topology_path,
)


ROOT = Path(__file__).resolve().parents[1]
CT = ZoneInfo("America/Chicago")


def _row(event: datetime, identity: str = "contract") -> ReportedBarEvidence:
    return ReportedBarEvidence(event, event + timedelta(seconds=65), identity)


def _complete_rows() -> tuple[datetime, list[ReportedBarEvidence]]:
    checkpoint = datetime(2022, 6, 1, 9, 0, tzinfo=CT)
    rows = [_row(checkpoint - timedelta(minutes=value)) for value in range(30, 0, -2)]
    rows.extend([_row(checkpoint + timedelta(minutes=1)), _row(checkpoint + timedelta(minutes=31))])
    return checkpoint, rows


def test_sealed_topology_is_conclusive_and_price_free() -> None:
    audit = build_topology_audit(root=ROOT)
    assert audit["market_configuration_results"] == 287
    assert audit["passing_market_configurations"] == 0
    assert audit["diagnosis"]["economic_outcome_examined"] is False
    assert audit["topology_id"] == sha256_json(
        {key: value for key, value in audit.items() if key != "topology_id"}
    )


def test_sparse_interior_reported_bars_can_be_causally_complete() -> None:
    checkpoint, rows = _complete_rows()
    result = classify_reported_bar_checkpoint(checkpoint=checkpoint, rows=rows)
    assert result.feature_complete is True
    assert result.execution_complete is True
    assert result.disposition == "COMPLETE"


def test_feature_gap_is_explicit_decision_time_abstention() -> None:
    checkpoint, rows = _complete_rows()
    result = classify_reported_bar_checkpoint(checkpoint=checkpoint, rows=rows[:5])
    assert result.disposition == "EXPLICIT_CAUSAL_FEATURE_ABSTENTION"
    assert result.feature_complete is False


def test_future_entry_or_exit_gap_fails_path() -> None:
    checkpoint, rows = _complete_rows()
    assert classify_reported_bar_checkpoint(checkpoint=checkpoint, rows=rows[:-2]).disposition == (
        "EXECUTION_ENTRY_PATH_INCOMPLETE"
    )
    assert classify_reported_bar_checkpoint(checkpoint=checkpoint, rows=rows[:-1]).disposition == (
        "EXECUTION_EXIT_PATH_INCOMPLETE"
    )


def test_identity_change_fails_execution() -> None:
    checkpoint, rows = _complete_rows()
    rows[-1] = _row(checkpoint + timedelta(minutes=31), identity="rolled")
    result = classify_reported_bar_checkpoint(checkpoint=checkpoint, rows=rows)
    assert result.disposition == "EXECUTION_IDENTITY_CHANGING"


def test_protocol_freezes_coverage_and_no_economics_when_prepared(
    tmp_path: Path,
) -> None:
    audit = {"topology_id": "a" * 64, "passing_market_configurations": 0}
    rejection = build_rejection_record(root=tmp_path, audit=audit)
    for relative, payload in (
        (topology_path(audit), audit),
        (rejection_path(rejection), rejection),
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(canonical_bytes(payload) + b"\n")
    for relative in (
        "configs/active_cash_open_impulse_historical_calendar.json",
        "data/active/catalog.json",
        "src/futures_rebuild/active_data_view.py",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("synthetic\n", encoding="utf-8")
    protocol = build_protocol(root=tmp_path, audit=audit, rejection=rejection)
    assert protocol["coverage_gates"]["checkpoint_accounting_percent"] == 100
    assert protocol["coverage_gates"]["feature_complete_candidate_execution_path_percent"] == 100
    assert protocol["source_only_selection"]["returns_costs_predictions_or_outcomes_used"] is False
    assert protocol["authority"]["historical_row_read"] is False
    assert protocol["protocol_id"] == sha256_json(
        {key: value for key, value in protocol.items() if key != "protocol_id"}
    )
    assert not (tmp_path / PROTOCOL_PATH).exists()
