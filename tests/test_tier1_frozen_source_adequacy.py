from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from futures_rebuild.tier1_bracket_v4 import ExpectedCheckpoint, FoldSpec
from futures_rebuild.tier1_bracket_v5 import CensusCheckpoint, NS_PER_MINUTE
from futures_rebuild.tier1_bracket_v10 import (
    SourceIntegrityAuditV10,
    normalize_source_mappings_v10,
)
from futures_rebuild.tier1_frozen_source_adequacy_census import (
    CheckpointCoverage,
    adjudicate_source_adequacy,
    classify_session_checkpoints,
    load_source_adequacy_plan,
)


ROOT = Path(__file__).resolve().parents[1]
DECISION = 1_600_020_000_000_000_000


def _rows(offsets: list[int]):
    mappings = []
    for ordinal, offset in enumerate(offsets, start=1):
        mappings.append({
            "event_at_ns": DECISION + offset * NS_PER_MINUTE,
            "exchange_session_date": "2020-01-02",
            "source_row_sha256": f"{ordinal:064x}",
            "disposition": "ELIGIBLE",
            "prediction_in_coverage_denominator": True,
            "failure_code": "NONE",
            "failure_detail_sha256": "a" * 64,
            "actual_identity_hash": "b" * 64,
            "open_nano": 100_000_000_000,
            "high_nano": 101_000_000_000,
            "low_nano": 99_000_000_000,
            "close_nano": 100_000_000_000,
            "volume": 10 + ordinal,
            "tick_size": "0.25",
            "tick_value": "12.50",
            "point_value": "50",
        })
    return tuple(normalize_source_mappings_v10(
        market="ES", rows=iter(mappings), audit=SourceIntegrityAuditV10("ES"),
    ))


def _checkpoint(opportunity_id: str = "open") -> CensusCheckpoint:
    return CensusCheckpoint(
        ExpectedCheckpoint(
            opportunity_id, "ES", 2020, "2020-01-02", "08:30", DECISION,
        ),
        True,
        "c" * 64,
    )


def test_checkpoint_census_retains_complete_and_unavailable_opportunities() -> None:
    offsets = [*range(-62, -1), 1, 2, 20, 40, 61]
    complete = classify_session_checkpoints(
        source_rows=_rows(offsets), checkpoints=(_checkpoint(),),
    )
    assert len(complete) == 1
    assert complete[0].feature_status == "COMPLETE"
    assert complete[0].execution_status == "COMPLETE"

    unavailable = classify_session_checkpoints(
        source_rows=(), checkpoints=(_checkpoint("absent"),),
    )
    assert len(unavailable) == 1
    assert unavailable[0].feature_status == "CAUSAL_ABSTENTION"
    assert unavailable[0].execution_status == "EXPLICIT_UNAVAILABLE"
    assert unavailable[0].feature_reason
    assert unavailable[0].execution_reason


def _dates(start: date, count: int) -> list[str]:
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _passing_ledger() -> tuple[list[CheckpointCoverage], list[FoldSpec]]:
    training = [*_dates(date(2018, 1, 2), 40), *_dates(date(2019, 1, 2), 40)]
    evaluation = [
        *_dates(date(2020, 1, 2), 80),
        *_dates(date(2021, 1, 2), 80),
        *_dates(date(2022, 1, 2), 80),
    ]
    folds = [
        FoldSpec(index, tuple(training), tuple(evaluation[index * 30:(index + 1) * 30]))
        for index in range(8)
    ]
    records: list[CheckpointCoverage] = []
    for market in ("ES", "CL", "ZN", "6E"):
        for session in [*training, *evaluation]:
            year = int(session[:4])
            opportunity_id = f"{market}/{session}"
            records.append(CheckpointCoverage(
                opportunity_id, market, year, session, "08:30",
                "COMPLETE", None,
                "COMPLETE", None,
            ))
    return records, folds


def test_adequacy_passes_only_with_complete_ledger_and_frozen_coverage_gates() -> None:
    records, folds = _passing_ledger()
    expected_ids = [item.opportunity_id for item in records]
    result = adjudicate_source_adequacy(
        records=records, expected_ids=expected_ids, folds=folds,
    )
    assert result["decision"] == "PASS"
    assert result["checks"] == {
        "terminal_open_checkpoint_ledger_complete": True,
        "overall_feature_rate_at_least_95_percent": True,
        "every_market_year_feature_rate_at_least_90_percent": True,
        "every_market_fold_role_feature_rate_at_least_90_percent_and_30_sessions": True,
        "every_execution_path_has_terminal_source_status": True,
        "every_feature_complete_checkpoint_has_complete_execution_path": True,
        "incomplete_selected_execution_forces_trial_rejection": True,
    }
    assert result["overall"]["execution"]["explicit_unavailable"] == 0


def test_feature_complete_but_unavailable_execution_blocks_registration() -> None:
    records, folds = _passing_ledger()
    item = records[0]
    records[0] = CheckpointCoverage(
        item.opportunity_id, item.market, item.year,
        item.exchange_session_date, item.checkpoint,
        item.feature_status, item.feature_reason,
        "EXPLICIT_UNAVAILABLE", "exact entry unavailable",
    )
    result = adjudicate_source_adequacy(
        records=records,
        expected_ids=[record.opportunity_id for record in records],
        folds=folds,
    )
    assert result["decision"] == "FAIL"
    assert result["checks"][
        "every_feature_complete_checkpoint_has_complete_execution_path"
    ] is False


def test_adequacy_fails_when_one_market_year_drops_below_90_percent() -> None:
    records, folds = _passing_ledger()
    affected = [
        index for index, item in enumerate(records)
        if item.market == "ZN" and item.year == 2018
    ]
    for index in affected[:5]:
        item = records[index]
        records[index] = CheckpointCoverage(
            item.opportunity_id, item.market, item.year,
            item.exchange_session_date, item.checkpoint,
            "CAUSAL_ABSTENTION", "insufficient causal reported feature bars",
            item.execution_status, item.execution_reason,
        )
    result = adjudicate_source_adequacy(
        records=records,
        expected_ids=[item.opportunity_id for item in records],
        folds=folds,
    )
    assert result["decision"] == "FAIL"
    assert result["market_years"]["ZN/2018"]["feature_rate"] == 35 / 40
    assert result["market_years"]["ZN/2018"]["status"] == "FAIL"


def test_source_adequacy_plan_is_hash_bound_and_keeps_forbidden_actions_closed() -> None:
    plan = load_source_adequacy_plan(root=ROOT)
    assert plan["plan_id"] == (
        "4564533b69c4414503be80c6781f3c7dfd898b19f523a20467e0ac2695853516"
    )
    assert plan["selected_release_count"] == 20
    assert plan["reported_bar_semantics"]["incomplete_selected_execution_forces_trial_rejection"] is True
    assert set(plan["forbidden_actions"].values()) == {True}
