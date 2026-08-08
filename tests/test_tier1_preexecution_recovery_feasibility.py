from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import json

import pytest

from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.foundation.records import ProviderBar
from futures_rebuild.tier1_bracket_v5 import NS_PER_MINUTE
from futures_rebuild.tier1_preexecution_recovery_feasibility import (
    RecoveryTarget,
    SessionRecoveryTarget,
    build_recovery_targets,
    canonical_ohlcv_catalog,
    classify_session_checkpoint_presence,
    classify_target_presence,
    load_recovery_feasibility_plan,
)


ROOT = Path(__file__).resolve().parents[1]


def _ns(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)


def _checkpoint(
    *, checkpoint_id: str, decision: int, reasons: list[str],
    missing_execution: list[int] | None = None,
) -> dict[str, object]:
    return {
        "checkpoint_id": checkpoint_id,
        "market": "ES",
        "year": 2020,
        "exchange_session_date": "2020-01-02",
        "checkpoint": "08:30",
        "decision_at_ns": decision,
        "registered_session_close_at_ns": decision + 8 * 60 * NS_PER_MINUTE,
        "disposition": "MISSING_SOURCE_DEPENDENCIES",
        "reason_codes": reasons,
        "feature_anchor_at_ns": None,
        "missing_feature_timestamps_ns": [],
        "nonexecutable_feature_timestamps_ns": [],
        "causally_late_feature_timestamps_ns": [],
        "identity_mismatch_feature_timestamps_ns": [],
        "missing_execution_timestamps_ns": missing_execution or [],
        "nonexecutable_execution_timestamps_ns": [],
    }


def _provider_bar(*, event: int, ordinal: int = 1) -> ProviderBar:
    return ProviderBar(
        dataset="GLBX.MDP3", market="ES", publisher_id=1,
        instrument_id=2, event_at_ns=event,
        open_nano=100_000_000_000, high_nano=101_000_000_000,
        low_nano=99_000_000_000, close_nano=100_000_000_000,
        volume=10, source_release_id="a" * 64,
        source_manifest_sha256="b" * 64,
        source_file_path="dbn/ohlcv_1m/ES/2020/file.dbn.zst",
        source_file_sha256="c" * 64, row_sha256=f"{ordinal:064x}",
    )


def test_missing_session_is_retained_without_guessing_its_feature_anchor() -> None:
    decision = _ns("2020-01-02T14:30:00")
    overlap = decision + NS_PER_MINUTE
    record = {
        "checkpoint_count": 2,
        "checkpoint_inventory": [
            _checkpoint(
                checkpoint_id="session", decision=decision,
                reasons=["MISSING_SOURCE_SESSION"],
            ),
            _checkpoint(
                checkpoint_id="explicit", decision=decision,
                reasons=["MISSING_EXECUTION_TIMESTAMPS"],
                missing_execution=[overlap],
            ),
        ],
    }
    timestamps, sessions = build_recovery_targets(record)
    assert len(timestamps) == 1
    assert timestamps[0].event_at_ns == overlap
    assert timestamps[0].dependency_roles == ("MISSING_EXECUTION_DEPENDENCY",)
    assert timestamps[0].checkpoint_ids == ("explicit",)
    assert len(sessions) == 1
    assert sessions[0].checkpoint_id == "session"


def test_target_builder_rejects_holdout_timestamp() -> None:
    decision = _ns("2025-01-02T14:30:00")
    record = {
        "checkpoint_count": 1,
        "checkpoint_inventory": [
            _checkpoint(
                checkpoint_id="holdout", decision=decision,
                reasons=["MISSING_EXECUTION_TIMESTAMPS"],
                missing_execution=[decision + NS_PER_MINUTE],
            )
        ],
    }
    with pytest.raises(IntegrityError, match="target identity"):
        build_recovery_targets(record)


def test_missing_session_anchor_is_resolved_from_canonical_rows() -> None:
    decision = _ns("2020-01-02T14:30:00")
    target = SessionRecoveryTarget(
        "ES", "2020-01-02", "session", "08:30", decision,
    )
    # decision-2m is absent. A guessed window would fail, but the registered
    # causal rule correctly anchors at decision-3m when that is the latest
    # available canonical minute.
    events = [
        decision + offset * NS_PER_MINUTE
        for offset in [*range(-63, -2), *range(1, 62)]
    ]
    result = classify_session_checkpoint_presence(
        target=target,
        bars=tuple(_provider_bar(event=event, ordinal=index + 1)
                   for index, event in enumerate(events)),
    )
    assert result["disposition"] == "CANONICAL_SESSION_CHECKPOINT_DEPENDENCIES_PRESENT"
    assert result["feature_anchor_at_ns"] == decision - 3 * NS_PER_MINUTE
    assert result["missing_feature_timestamps_ns"] == []


def test_presence_classification_reports_identities_but_never_prices() -> None:
    present = _ns("2020-01-02T14:31:00")
    absent = present + NS_PER_MINUTE
    targets = (
        RecoveryTarget("ES", present, ("MISSING_EXECUTION_DEPENDENCY",), ("a",)),
        RecoveryTarget("ES", absent, ("MISSING_EXECUTION_DEPENDENCY",), ("b",)),
    )
    result = classify_target_presence(
        targets=targets, bars=(_provider_bar(event=present),),
    )
    assert [item["disposition"] for item in result] == [
        "CANONICAL_OHLCV_1M_PRESENT", "CANONICAL_OHLCV_1M_ABSENT",
    ]
    assert len(result[0]["canonical_row_identities"]) == 1
    assert not ({"open", "high", "low", "close", "volume"} & set(result[0]))


def test_duplicate_canonical_timestamp_is_ambiguous_and_fails_closed() -> None:
    event = _ns("2020-01-02T14:31:00")
    target = RecoveryTarget("ES", event, ("MISSING_EXECUTION_DEPENDENCY",), ("a",))
    result = classify_target_presence(
        targets=(target,),
        bars=(_provider_bar(event=event, ordinal=1), _provider_bar(event=event, ordinal=2)),
    )
    assert result[0]["disposition"] == "CANONICAL_OHLCV_1M_AMBIGUOUS"
    assert len(result[0]["canonical_row_identities"]) == 2


def test_real_gap_target_and_canonical_catalog_bindings_are_exact(
    local_evidence_root: Path,
) -> None:
    plan = load_recovery_feasibility_plan(root=local_evidence_root)
    assert plan["timestamp_target_count"] == 5_599
    assert plan["session_checkpoint_target_count"] == 9
    assert plan["canonical_file_count"] == 20
    assert plan["canonical_source_family"] == "dbn_ohlcv_1m"
    assert plan["diagnostic_source_families_excluded"] == [
        "dbn_ohlcv_1s", "dbn_trades",
    ]
    assert set(plan["forbidden_actions"].values()) == {True}
    _, catalog = canonical_ohlcv_catalog(
        root=local_evidence_root,
        boundary=RepoBoundary(local_evidence_root),
    )
    assert {(item["market"], item["year"]) for item in catalog} == {
        (market, year)
        for market in ("6E", "CL", "ES", "ZN")
        for year in range(2018, 2023)
    }


def test_published_recovery_map_is_canonical_bounded_and_research_free() -> None:
    path = ROOT / (
        "state/source_quality/tier1_preexecution_recovery_feasibility/"
        "c343a1ad972f51c9af774b9c7ffb163a3337b9cbf24c0cddeb0d17ca4a35c4eb.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    record_id = payload.pop("record_id")
    payload["state"] = "PREPARED_CREATE_ONLY"
    assert sha256_json(payload) == record_id == path.stem
    assert payload["timestamp_target_count"] == 5_599
    assert payload["session_checkpoint_target_count"] == 9
    assert payload["disposition_counts"] == {
        "CANONICAL_OHLCV_1M_ABSENT": 5_598,
        "CANONICAL_OHLCV_1M_PRESENT": 1,
        "CANONICAL_SESSION_CHECKPOINT_DEPENDENCIES_INCOMPLETE": 9,
    }
    assert {item["year"] for item in payload["source_audit"]} == {
        2018, 2019, 2020, 2021, 2022,
    }
    for field in (
        "prices_reported", "provider_access", "diagnostic_source_families_read",
        "successor_data_created", "active_data_mutation", "model_fit",
        "prediction_generation", "historical_evaluation",
        "trial_registration_or_retirement", "holdout_or_forward_access", "trading",
    ):
        assert payload[field] is False
