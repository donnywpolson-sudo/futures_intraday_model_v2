from __future__ import annotations

from pathlib import Path
from decimal import Decimal

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_v4 import ExpectedCheckpoint, MarketSpec
from futures_rebuild.tier1_bracket_v5 import (
    CensusCheckpoint,
    NS_PER_MINUTE,
    load_v5_contract,
    materialize_v5_rows,
)
from futures_rebuild.tier1_bracket_v10 import (
    SourceIntegrityAuditV10,
    audit_checkpoint_dependencies_v10,
    load_v10_contract,
    normalize_source_mappings_v10,
)
from futures_rebuild.canonical import sha256_file, sha256_json


ROOT = Path(__file__).resolve().parents[1]


def _row(
    event: int, *, session: str | None = "2020-01-02",
    disposition: str = "ELIGIBLE", volume: int = 10,
) -> dict[str, object]:
    return {
        "event_at_ns": event,
        "exchange_session_date": session,
        "source_row_sha256": f"{event // NS_PER_MINUTE:064x}"[-64:],
        "disposition": disposition,
        "prediction_in_coverage_denominator": True,
        "failure_code": (
            "NONE" if disposition == "ELIGIBLE" else "DEFINITION_INTRABAR_CHANGE"
        ),
        "failure_detail_sha256": "a" * 64,
        "actual_identity_hash": "b" * 64 if disposition == "ELIGIBLE" else None,
        "open_nano": 100_000_000_000,
        "high_nano": 101_000_000_000,
        "low_nano": 99_000_000_000,
        "close_nano": 100_000_000_000,
        "volume": volume,
        "tick_size": "0.25",
        "tick_value": "12.50",
        "point_value": "50",
    }


def test_unproven_session_gap_is_diagnostic_and_does_not_duplicate_rows() -> None:
    start = 1_600_000_000_000_000_000
    audit = SourceIntegrityAuditV10("CL")
    output = list(normalize_source_mappings_v10(
        market="CL",
        rows=iter([_row(start), _row(start + 2 * NS_PER_MINUTE)]),
        audit=audit,
    ))
    assert [item.bar.event_at_ns for item in output if item.bar is not None] == [
        start, start + 2 * NS_PER_MINUTE,
    ]
    assert all(item.executable for item in output)
    assert audit.observed_adjacent_timestamp_discontinuities == 1
    assert audit.sessions_with_observed_discontinuities == {"2020-01-02"}


def _materialize_one_checkpoint(*, omit_feature_minute: int | None = None):
    decision = 1_600_020_000_000_000_000
    minute_events = [
        decision + offset * NS_PER_MINUTE
        for offset in range(-62, -1)
        if offset != omit_feature_minute
    ] + [
        decision + offset * NS_PER_MINUTE for offset in range(1, 62)
    ]
    # This earlier row creates an observed same-session discontinuity that is
    # outside both exact checkpoint dependency windows.
    events = [decision - 200 * NS_PER_MINUTE, *minute_events]
    audit = SourceIntegrityAuditV10("ES")
    normalized = tuple(normalize_source_mappings_v10(
        market="ES",
        rows=iter([
            _row(event, volume=10 + index % 7)
            for index, event in enumerate(events)
        ]),
        audit=audit,
    ))
    expected = ExpectedCheckpoint(
        "checkpoint", "ES", 2020, "2020-01-02", "08:30", decision,
    )
    materialized = materialize_v5_rows(
        source_rows=normalized,
        census=(CensusCheckpoint(expected, True, "c" * 64),),
        market_specs={
            "ES": MarketSpec(Decimal("0.25"), Decimal("12.5"), Decimal("50")),
        },
        contract=load_v5_contract(root=ROOT),
        prediction_scope_sessions=("2020-01-02",),
    )
    return audit, materialized[0]


def test_gap_outside_exact_checkpoint_dependencies_does_not_erase_checkpoint() -> None:
    audit, row = _materialize_one_checkpoint()
    # One discontinuity precedes the feature window; the other is the normal
    # causal boundary from the last available feature bar (decision - 2m) to
    # the registered entry bar (decision + 1m).
    assert audit.observed_adjacent_timestamp_discontinuities == 2
    assert row.ledger.prediction_produced
    assert row.features is not None
    assert row.ledger.outcome_coverage in {
        "COMPLETE", "STRESS_COMPLETE_PARTIAL_DIAGNOSTICS",
    }
    assert row.outcomes is not None and "stress" in row.outcomes


def test_gap_inside_feature_dependency_window_abstains_fail_closed() -> None:
    audit, row = _materialize_one_checkpoint(omit_feature_minute=-20)
    assert audit.observed_adjacent_timestamp_discontinuities == 3
    assert row.ledger.terminal_disposition == "INSUFFICIENT_CAUSAL_HISTORY"
    assert not row.ledger.prediction_produced


def test_dependency_census_counts_exact_windows_without_model_or_predictions() -> None:
    decision = 1_600_020_000_000_000_000
    events = [
        decision + offset * NS_PER_MINUTE
        for offset in [*range(-62, -1), *range(1, 62)]
    ]
    audit = SourceIntegrityAuditV10("ES")
    normalized = tuple(normalize_source_mappings_v10(
        market="ES",
        rows=iter([_row(event, volume=10 + index % 7) for index, event in enumerate(events)]),
        audit=audit,
    ))
    expected = ExpectedCheckpoint(
        "checkpoint", "ES", 2020, "2020-01-02", "08:30", decision,
    )
    result = audit_checkpoint_dependencies_v10(
        source_rows=normalized,
        census=(CensusCheckpoint(expected, True, "c" * 64),),
    )
    assert result.as_dict() == {
        "expected_open_checkpoints": 1,
        "missing_source_sessions": 0,
        "ambiguous_source_sessions": 0,
        "complete_feature_windows": 1,
        "incomplete_feature_windows": 0,
        "complete_execution_windows": 1,
        "incomplete_execution_windows": 0,
        "complete_both_windows": 1,
    }

    missing_path = tuple(
        item for item in normalized
        if item.bar is not None
        and item.bar.event_at_ns != decision + 30 * NS_PER_MINUTE
    )
    incomplete = audit_checkpoint_dependencies_v10(
        source_rows=missing_path,
        census=(CensusCheckpoint(expected, True, "c" * 64),),
    )
    assert incomplete.complete_feature_windows == 1
    assert incomplete.incomplete_execution_windows == 1
    assert incomplete.complete_both_windows == 0


def test_nontradable_row_is_retained_once_and_never_executes() -> None:
    start = 1_600_000_000_000_000_000
    audit = SourceIntegrityAuditV10("ZN")
    output = list(normalize_source_mappings_v10(
        market="ZN",
        rows=iter([_row(start, disposition="UNRESOLVED_FAIL_CLOSED")]),
        audit=audit,
    ))
    assert len(output) == 1
    assert output[0].bar is not None and not output[0].executable
    assert audit.nontradable_rows == 1


def test_sessionless_nontradable_row_is_recovered_once_but_not_executable() -> None:
    start = 1_600_000_000_000_000_000
    audit = SourceIntegrityAuditV10("ES")
    output = list(normalize_source_mappings_v10(
        market="ES",
        rows=iter([
            _row(start),
            _row(
                start + NS_PER_MINUTE, session=None,
                disposition="UNRESOLVED_FAIL_CLOSED",
            ),
            _row(start + 2 * NS_PER_MINUTE),
        ]),
        audit=audit,
    ))
    assert len(output) == 3
    assert output[1].exchange_session_date == "2020-01-02"
    assert not output[1].executable
    assert audit.sessionless_nontradable_rows == 1


def test_tradable_row_without_session_fails_closed() -> None:
    audit = SourceIntegrityAuditV10("6E")
    with pytest.raises(IntegrityError, match="tradable.*session"):
        list(normalize_source_mappings_v10(
            market="6E",
            rows=iter([_row(1_600_000_000_000_000_000, session=None)]),
            audit=audit,
        ))


def test_stream_and_audit_market_must_match() -> None:
    with pytest.raises(IntegrityError, match="market"):
        list(normalize_source_mappings_v10(
            market="CL", rows=iter(()), audit=SourceIntegrityAuditV10("ES"),
        ))


def test_v10_contract_is_prepared_only_and_forbids_strategy_tuning() -> None:
    inherited, contract = load_v10_contract(root=ROOT)
    assert contract["state"] == "PREPARED_NOT_REGISTERED"
    assert contract["inherited_v9_contract_sha256"] == sha256_file(
        ROOT / "configs/tier1_bracket_successor_v9.json"
    )
    assert contract["source_continuity_successor"]["feature_completeness"].startswith(
        "EXACT_61_BAR"
    )
    assert set(contract["anti_tuning"].values()) == {False}
    assert inherited["risk"]["continuous_drawdown_threshold_usd"] == "1500"
    assert inherited["strategy"]["minimum_predicted_net_r_after_stress_costs"] == "0.25"


def test_dependency_census_plan_is_hash_bound_and_read_only() -> None:
    import json

    plan = json.loads(
        (ROOT / "configs/tier1_bracket_v9_dependency_window_census_plan.json")
        .read_text(encoding="utf-8")
    )
    core = dict(plan)
    plan_id = core.pop("plan_id")
    assert plan_id == sha256_json(core)
    assert plan["execution_mode"] == "IN_MEMORY_UNPUBLISHED_COUNTS_ONLY"
    assert plan["maximum_host_runtime_seconds"] == 300
    assert set(plan["authorized_actions"]) == {
        "registered_source_row_read", "dependency_window_census",
    }
    assert all(plan["authorized_actions"].values())
    assert all(plan["forbidden_actions"].values())
    assert 2025 not in plan["source_scope"]["years"]
