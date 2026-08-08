from __future__ import annotations

from pathlib import Path

import json

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.canonical import sha256_json
from futures_rebuild.tier1_bracket_v4 import ExpectedCheckpoint
from futures_rebuild.tier1_bracket_v5 import CensusCheckpoint, NS_PER_MINUTE
from futures_rebuild.tier1_bracket_v10 import (
    SourceIntegrityAuditV10,
    normalize_source_mappings_v10,
)
from futures_rebuild.tier1_preexecution_gap_inventory import (
    classify_checkpoint_dependencies,
    load_gap_inventory_plan,
)


ROOT = Path(__file__).resolve().parents[1]
DECISION = 1_600_020_000_000_000_000


def _mapping(
    event: int, *, disposition: str = "ELIGIBLE", identity: str = "b" * 64,
) -> dict[str, object]:
    return {
        "event_at_ns": event,
        "exchange_session_date": "2020-01-02",
        "source_row_sha256": f"{abs(event // NS_PER_MINUTE):064x}"[-64:],
        "disposition": disposition,
        "prediction_in_coverage_denominator": True,
        "failure_code": "NONE" if disposition == "ELIGIBLE" else "UNRESOLVED",
        "failure_detail_sha256": "a" * 64,
        "actual_identity_hash": identity if disposition == "ELIGIBLE" else None,
        "open_nano": 100_000_000_000,
        "high_nano": 101_000_000_000,
        "low_nano": 99_000_000_000,
        "close_nano": 100_000_000_000,
        "volume": 10,
        "tick_size": "0.25",
        "tick_value": "12.50",
        "point_value": "50",
    }


def _checkpoint(*, open_: bool = True) -> CensusCheckpoint:
    expected = ExpectedCheckpoint(
        "checkpoint", "ES", 2020, "2020-01-02", "08:30", DECISION,
    )
    return CensusCheckpoint(expected, open_, "c" * 64)


def _complete_rows(*, omit: int | None = None, nonexec: int | None = None):
    offsets = [*range(-62, -1), *range(1, 62)]
    mappings = []
    for offset in offsets:
        if offset == omit:
            continue
        mappings.append(_mapping(
            DECISION + offset * NS_PER_MINUTE,
            disposition="UNRESOLVED_FAIL_CLOSED" if offset == nonexec else "ELIGIBLE",
        ))
    audit = SourceIntegrityAuditV10("ES")
    return tuple(normalize_source_mappings_v10(
        market="ES", rows=iter(mappings), audit=audit,
    ))


def test_horizon_abstention_is_calendar_known_and_needs_no_source_rows() -> None:
    result = classify_checkpoint_dependencies(
        source_rows=(), checkpoint=_checkpoint(),
        registered_session_close_at_ns=DECISION + 30 * NS_PER_MINUTE,
    )
    assert result.disposition == "PRECAUSAL_SESSION_HORIZON_ABSTENTION"
    assert result.reason_codes == (
        "REGISTERED_SESSION_ENDS_BEFORE_REQUIRED_EXECUTION_HORIZON",
    )
    assert result.missing_execution_timestamps_ns == ()


def test_complete_exact_dependencies_are_not_a_gap() -> None:
    result = classify_checkpoint_dependencies(
        source_rows=_complete_rows(), checkpoint=_checkpoint(),
        registered_session_close_at_ns=DECISION + 120 * NS_PER_MINUTE,
    )
    assert result.disposition == "COMPLETE_DEPENDENCIES"
    assert result.reason_codes == ()
    assert result.feature_anchor_at_ns == DECISION - 2 * NS_PER_MINUTE


def test_missing_execution_minute_is_reported_exactly() -> None:
    missing = DECISION + 30 * NS_PER_MINUTE
    result = classify_checkpoint_dependencies(
        source_rows=_complete_rows(omit=30), checkpoint=_checkpoint(),
        registered_session_close_at_ns=DECISION + 120 * NS_PER_MINUTE,
    )
    assert result.disposition == "MISSING_SOURCE_DEPENDENCIES"
    assert result.missing_execution_timestamps_ns == (missing,)
    assert "MISSING_EXECUTION_TIMESTAMPS" in result.reason_codes


def test_nonexecutable_dependency_is_not_counted_as_missing_or_complete() -> None:
    event = DECISION - 20 * NS_PER_MINUTE
    result = classify_checkpoint_dependencies(
        source_rows=_complete_rows(nonexec=-20), checkpoint=_checkpoint(),
        registered_session_close_at_ns=DECISION + 120 * NS_PER_MINUTE,
    )
    assert result.nonexecutable_feature_timestamps_ns == (event,)
    assert result.missing_feature_timestamps_ns == ()
    assert result.disposition == "MISSING_SOURCE_DEPENDENCIES"


def test_missing_whole_session_is_distinct_from_horizon_abstention() -> None:
    result = classify_checkpoint_dependencies(
        source_rows=(), checkpoint=_checkpoint(),
        registered_session_close_at_ns=DECISION + 120 * NS_PER_MINUTE,
    )
    assert result.disposition == "MISSING_SOURCE_DEPENDENCIES"
    assert result.reason_codes == ("MISSING_SOURCE_SESSION",)


def test_duplicate_timestamp_fails_as_ambiguous() -> None:
    rows = list(_complete_rows())
    rows.append(rows[0])
    result = classify_checkpoint_dependencies(
        source_rows=tuple(rows), checkpoint=_checkpoint(),
        registered_session_close_at_ns=DECISION + 120 * NS_PER_MINUTE,
    )
    assert result.disposition == "AMBIGUOUS_SOURCE_DEPENDENCIES"
    assert result.reason_codes == ("DUPLICATE_EVENT_TIMESTAMP",)


def test_closed_checkpoint_cannot_enter_open_gap_inventory() -> None:
    with pytest.raises(IntegrityError, match="closed calendar checkpoint"):
        classify_checkpoint_dependencies(
            source_rows=(), checkpoint=_checkpoint(open_=False),
            registered_session_close_at_ns=DECISION + 120 * NS_PER_MINUTE,
        )


def test_plan_is_hash_bound_to_20_selected_releases_and_forbidden_actions() -> None:
    plan = load_gap_inventory_plan(root=ROOT)
    assert plan["selected_release_count"] == 20
    assert plan["maximum_host_runtime_seconds"] == 900
    assert plan["estimated_external_cost_usd"] == "0"
    assert set(plan["forbidden_actions"].values()) == {True}


def test_published_gap_inventory_is_canonical_complete_and_research_free() -> None:
    path = ROOT / (
        "state/source_quality/tier1_preexecution_gap_inventory/"
        "874c2f97b76e8bc19077bc209bae58a4b07e09a629c823af10948e6021772d61.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    record_id = payload.pop("record_id")
    payload["state"] = "PREPARED_CREATE_ONLY"
    assert sha256_json(payload) == record_id == path.stem
    assert payload["checkpoint_count"] == 15_343
    assert payload["disposition_counts"] == {
        "COMPLETE_DEPENDENCIES": 14_371,
        "MISSING_SOURCE_DEPENDENCIES": 972,
    }
    assert len(payload["checkpoint_inventory"]) == payload["checkpoint_count"]
    assert {item["year"] for item in payload["checkpoint_inventory"]} == {
        2018, 2019, 2020, 2021, 2022,
    }
    for field in (
        "prices_reported", "model_fit", "prediction_generation",
        "historical_evaluation", "trial_registration_or_retirement",
        "holdout_or_forward_access", "provider_access",
        "active_data_mutation", "trading",
    ):
        assert payload[field] is False
