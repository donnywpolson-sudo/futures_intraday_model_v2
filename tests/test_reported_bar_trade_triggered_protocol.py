from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.reported_bar_fixed_horizon_protocol import ReportedBarEvidence
from futures_rebuild.reported_bar_trade_triggered_protocol import (
    PROTOCOL_PATH,
    build_invalid_preparation,
    build_protocol,
    build_rejection,
    build_topology,
    classify_trade_triggered_checkpoint,
    invalid_preparation_path,
    rejection_path,
    topology_path,
)


ROOT = Path(__file__).resolve().parents[1]
CT = ZoneInfo("America/Chicago")


def _row(event: datetime, available: datetime | None = None, identity: str = "contract") -> ReportedBarEvidence:
    return ReportedBarEvidence(event, available or event + timedelta(seconds=65), identity)


def _feature(checkpoint: datetime) -> list[ReportedBarEvidence]:
    return [_row(checkpoint - timedelta(minutes=value)) for value in range(30, 0, -2)]


def test_sealed_topology_proves_baseline_semantics_rejection() -> None:
    topology = build_topology(root=ROOT)
    assert topology["market_checkpoint_results"] == 164
    assert topology["passing_market_checkpoints"] == 0
    assert topology["baseline_only_failed_cell_count"] == 28
    assert topology["diagnosis"]["economic_outcome_examined"] is False
    assert topology["topology_id"] == sha256_json(
        {key: value for key, value in topology.items() if key != "topology_id"}
    )


def test_no_trigger_is_explicit_no_trade_timeout() -> None:
    checkpoint = datetime(2022, 6, 1, 9, 0, tzinfo=CT)
    result = classify_trade_triggered_checkpoint(
        checkpoint=checkpoint, rows=_feature(checkpoint), feature_required=True
    )
    assert result.disposition == "EXPLICIT_CAUSAL_NO_TRADE_TIMEOUT"
    assert result.path_required is False
    assert result.order_placed is False


def test_trigger_cannot_be_reused_as_retroactive_fill() -> None:
    checkpoint = datetime(2022, 6, 1, 9, 0, tzinfo=CT)
    decision = checkpoint + timedelta(seconds=5)
    trigger = _row(checkpoint + timedelta(minutes=1), decision + timedelta(seconds=120))
    result = classify_trade_triggered_checkpoint(
        checkpoint=checkpoint, rows=[*_feature(checkpoint), trigger], feature_required=True
    )
    assert result.trigger_observed is True
    assert result.order_placed is True
    assert result.entry_fill_complete is False
    assert result.disposition == "TRIGGERED_ORDER_ENTRY_FILL_INCOMPLETE"


def test_entry_evidence_reported_after_timeout_fails_closed() -> None:
    checkpoint = datetime(2022, 6, 1, 9, 0, tzinfo=CT)
    decision = checkpoint + timedelta(seconds=5)
    trigger = _row(checkpoint + timedelta(minutes=1), decision + timedelta(seconds=120))
    late_entry = _row(
        checkpoint + timedelta(minutes=3),
        trigger.available_at + timedelta(minutes=2, seconds=1),
    )
    result = classify_trade_triggered_checkpoint(
        checkpoint=checkpoint,
        rows=[*_feature(checkpoint), trigger, late_entry],
        feature_required=True,
    )
    assert result.disposition == "TRIGGERED_ORDER_ENTRY_FILL_INCOMPLETE"


def test_exit_evidence_reported_after_timeout_fails_closed() -> None:
    checkpoint = datetime(2022, 6, 1, 9, 0, tzinfo=CT)
    decision = checkpoint + timedelta(seconds=5)
    trigger = _row(checkpoint + timedelta(minutes=1), decision + timedelta(seconds=120))
    entry = _row(checkpoint + timedelta(minutes=3))
    exit_order = entry.event_at + timedelta(minutes=30)
    late_exit = _row(
        exit_order,
        exit_order + timedelta(minutes=2, seconds=1),
    )
    result = classify_trade_triggered_checkpoint(
        checkpoint=checkpoint,
        rows=[*_feature(checkpoint), trigger, entry, late_exit],
        feature_required=True,
    )
    assert result.disposition == "TRIGGERED_ORDER_EXIT_FILL_INCOMPLETE"


def test_later_fill_and_exit_complete_causal_path() -> None:
    checkpoint = datetime(2022, 6, 1, 9, 0, tzinfo=CT)
    decision = checkpoint + timedelta(seconds=5)
    trigger = _row(checkpoint + timedelta(minutes=1), decision + timedelta(seconds=120))
    entry = _row(checkpoint + timedelta(minutes=3))
    exit_bar = _row(checkpoint + timedelta(minutes=33))
    result = classify_trade_triggered_checkpoint(
        checkpoint=checkpoint,
        rows=[*_feature(checkpoint), trigger, entry, exit_bar],
        feature_required=True,
    )
    assert trigger.available_at < entry.event_at
    assert result.disposition == "COMPLETE"
    assert result.path_required is True
    assert result.exit_fill_complete is True


def test_triggered_entry_missing_exit_or_identity_fails() -> None:
    checkpoint = datetime(2022, 6, 1, 9, 0, tzinfo=CT)
    decision = checkpoint + timedelta(seconds=5)
    trigger = _row(checkpoint + timedelta(minutes=1), decision + timedelta(seconds=120))
    entry = _row(checkpoint + timedelta(minutes=3))
    missing = classify_trade_triggered_checkpoint(
        checkpoint=checkpoint, rows=[*_feature(checkpoint), trigger, entry], feature_required=True
    )
    assert missing.disposition == "TRIGGERED_ORDER_EXIT_FILL_INCOMPLETE"
    changed = classify_trade_triggered_checkpoint(
        checkpoint=checkpoint,
        rows=[*_feature(checkpoint), trigger, entry, _row(checkpoint + timedelta(minutes=33), identity="rolled")],
        feature_required=True,
    )
    assert changed.disposition == "EXIT_IDENTITY_CHANGING"


def test_feature_required_and_always_direction_universes_are_independent() -> None:
    checkpoint = datetime(2022, 6, 1, 9, 0, tzinfo=CT)
    decision = checkpoint + timedelta(seconds=5)
    trigger = _row(checkpoint + timedelta(minutes=1), decision + timedelta(seconds=120))
    entry = _row(checkpoint + timedelta(minutes=3))
    exit_bar = _row(checkpoint + timedelta(minutes=33))
    candidate = classify_trade_triggered_checkpoint(
        checkpoint=checkpoint, rows=[trigger, entry, exit_bar], feature_required=True
    )
    baseline = classify_trade_triggered_checkpoint(
        checkpoint=checkpoint, rows=[trigger, entry, exit_bar], feature_required=False
    )
    assert candidate.disposition == "EXPLICIT_CAUSAL_FEATURE_ABSTENTION"
    assert baseline.disposition == "COMPLETE"


def test_prepared_protocol_freezes_limits_and_no_economics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import futures_rebuild.reported_bar_trade_triggered_protocol as module

    predecessor = {
        "protocol_id": "a" * 64,
        "years": [2018, 2019],
        "market_universe": "SYNTHETIC",
        "checkpoint_grid": ["10:00"],
        "checkpoint_configurations": [["10:00"]],
        "decision_rule": {"feature_rows_must_be_available_by_decision": True},
        "coverage_gates": {"checkpoint_accounting_percent": 100},
        "folds": {"outer_folds": 2},
        "baseline_requirements": {"active_baselines": ["ALWAYS_LONG"]},
        "source_resolution": {"active_catalog_only": True},
        "source_only_selection": {
            "returns_costs_predictions_or_outcomes_used": False
        },
        "execution_limits": {"maximum_attempts": 1},
        "authority": {"historical_row_read": False},
    }
    predecessor_path = tmp_path / "synthetic_predecessor.json"
    predecessor_path.write_bytes(canonical_bytes(predecessor) + b"\n")
    monkeypatch.setattr(module, "PREDECESSOR_PROTOCOL_PATH", Path("synthetic_predecessor.json"))
    monkeypatch.setattr(module, "PREDECESSOR_PROTOCOL_ID", predecessor["protocol_id"])
    monkeypatch.setattr(
        module, "PREDECESSOR_PROTOCOL_SHA256", sha256_file(predecessor_path)
    )
    topology = {"topology_id": "b" * 64}
    rejection = {"rejection_id": "c" * 64}
    invalid = {"invalid_preparation_id": "d" * 64}
    for relative, payload in (
        (topology_path(topology), topology),
        (rejection_path(rejection), rejection),
        (invalid_preparation_path(invalid), invalid),
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
    protocol = build_protocol(
        root=tmp_path,
        topology=topology,
        rejection=rejection,
        invalid_preparation=invalid,
    )
    assert protocol["entry_lifecycle"]["trigger_price_used_as_fill"] is False
    assert protocol["coverage_gates"]["triggered_order_entry_and_exit_path_percent"] == 100
    assert protocol["execution_limits"]["maximum_attempts"] == 1
    assert protocol["source_only_selection"]["returns_costs_predictions_or_outcomes_used"] is False
    assert protocol["authority"]["historical_row_read"] is False
    assert protocol["protocol_id"] == sha256_json(
        {key: value for key, value in protocol.items() if key != "protocol_id"}
    )
    assert not (tmp_path / PROTOCOL_PATH).exists()
