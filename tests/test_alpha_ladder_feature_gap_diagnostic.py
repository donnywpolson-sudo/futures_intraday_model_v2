from __future__ import annotations

import copy
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_feature_gap_diagnostic import (
    CLASSIFICATIONS,
    EXPECTED_TARGETS,
    GENERIC_FEATURE_ABSTENTION,
    OUTPUT_ROOT,
    PLAN_PATH,
    FeatureRow,
    build_plan,
    diagnose_feature_session,
    load_plan,
    reconcile_diagnostics,
    required_scope,
    validate_plan,
)
from futures_rebuild.alpha_ladder_limit_readiness import CT
from futures_rebuild.errors import IntegrityError
from futures_rebuild.research_gateway_policy import (
    ALPHA_LADDER_READINESS_CENSUS_OPERATION,
    PREPARATORY_REAL_HISTORY_OPERATIONS,
)


ROOT = Path(__file__).resolve().parents[1]
COSTS = {"base": 2, "stress": 4, "extreme": 8}


def _rows(
    *, session: date = date(2020, 1, 2), count: int = 30,
    start: time = time(9, 30),
) -> tuple[FeatureRow, ...]:
    base = datetime.combine(session, start, CT)
    return tuple(FeatureRow(
        event_at=base + timedelta(minutes=index),
        available_at=base + timedelta(minutes=index, seconds=65),
        executable=True,
        identity="a" * 64,
        source_row_sha256=f"{index + 1:064x}",
        disposition="CAUSAL_EXECUTABLE",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("100"),
        tick_size=Decimal("0.25"),
        tick_value=Decimal("12.5"),
    ) for index in range(count))


def _diagnose(rows) -> dict[str, object]:
    return diagnose_feature_session(
        session="2020-01-02", rows=rows, cost_ticks=COSTS,
    )


def test_complete_feature_window_remains_price_free() -> None:
    result = _diagnose(_rows())
    assert result["classification"] == "FEATURE_COMPLETE"
    assert result["production_feature_complete"] is True
    assert result["price_values_included"] is False
    assert not any(key in result for key in ("open", "high", "low", "close", "price"))


@pytest.mark.parametrize(
    ("rows", "classification"),
    [
        ((), "SOURCE_SESSION_ABSENT"),
        (_rows(count=20), "INSUFFICIENT_REPORTED_BAR_HISTORY"),
        (_rows(count=21, start=time(9, 38)), "WINDOW_START_TOO_LATE"),
        (_rows(count=21), "WINDOW_END_TOO_EARLY"),
    ],
)
def test_missing_and_window_coverage_classes_are_exact(rows, classification) -> None:
    result = _diagnose(rows)
    assert result["classification"] == classification
    assert result["production_feature_complete"] is False
    assert result["production_dispositions"] == [GENERIC_FEATURE_ABSTENTION]


def test_late_availability_is_distinguished_from_missing_rows() -> None:
    rows = list(_rows())
    decision = datetime.combine(date(2020, 1, 2), time(10, 0), CT) + timedelta(seconds=5)
    for index in range(20, 30):
        rows[index] = replace(rows[index], available_at=decision + timedelta(seconds=index))
    result = _diagnose(rows)
    assert result["classification"] == "LATE_AVAILABILITY"
    assert result["late_availability_row_count"] == 10


def test_identity_economics_invalid_fields_and_geometry_are_separate() -> None:
    identity = list(_rows())
    identity[-1] = replace(identity[-1], identity="b" * 64)
    assert _diagnose(identity)["classification"] == "IDENTITY_ROLL_DISCONTINUITY"

    economics = list(_rows())
    economics[-1] = replace(economics[-1], tick_value=Decimal("25"))
    assert _diagnose(economics)["classification"] == "ECONOMICS_DRIFT"

    invalid_economics = list(_rows())
    invalid_economics[-1] = replace(invalid_economics[-1], tick_size=Decimal("0"))
    assert _diagnose(invalid_economics)["classification"] == "INVALID_ECONOMICS"

    invalid = list(_rows())
    invalid[-1] = replace(invalid[-1], high=Decimal("-1"))
    assert _diagnose(invalid)["classification"] == "INVALID_FIELDS"

    flat = tuple(replace(row, high=Decimal("100"), low=Decimal("100")) for row in _rows())
    assert _diagnose(flat)["classification"] == "FEATURE_STOP_GEOMETRY_INVALID"


def test_duplicate_timestamp_uses_production_classifier_disposition() -> None:
    rows = (*_rows(), _rows()[0])
    result = _diagnose(rows)
    assert result["classification"] == "DUPLICATE_EVENT_TIMESTAMP"
    assert result["production_dispositions"] == [
        "AMBIGUOUS_DUPLICATE_SOURCE_TIMESTAMP"
    ]


def test_exact_reconciliation_preserves_fold_roles_and_rejects_count_drift() -> None:
    target = {
        "market": "ES",
        "fold_id": "fold-0",
        "training_session_ids": ["2020-01-02", "2020-01-03"],
        "evaluation_session_ids": ["2020-01-06"],
        "expected_training_feature_gaps": 1,
        "expected_evaluation_feature_gaps": 1,
        "expected_feature_gaps_by_market_year": {
            "2020": {"training_feature_gaps": 1, "evaluation_feature_gaps": 1},
        },
    }
    complete = {
        "session": "2020-01-03", "classification": "FEATURE_COMPLETE",
        "production_feature_complete": True, "production_dispositions": [],
    }
    gap = {
        "classification": "INSUFFICIENT_REPORTED_BAR_HISTORY",
        "production_feature_complete": False,
        "production_dispositions": [GENERIC_FEATURE_ABSTENTION],
    }
    diagnostics = {"ES": {
        "2020-01-02": {**gap, "session": "2020-01-02"},
        "2020-01-03": complete,
        "2020-01-06": {**gap, "session": "2020-01-06"},
    }}
    result = reconcile_diagnostics(targets=(target,), diagnostics=diagnostics)
    assert result["status"] == "EXACT_RECONCILIATION"
    assert result["unique_feature_gap_session_count"] == 2
    assert result["feature_gap_records"][0]["fold_roles"] == ["fold-0:training"]

    changed = copy.deepcopy(target)
    changed["expected_training_feature_gaps"] = 0
    with pytest.raises(IntegrityError, match="reconcile"):
        reconcile_diagnostics(targets=(changed,), diagnostics=diagnostics)


def test_plan_reconstructs_exact_nine_targets_without_row_execution() -> None:
    plan = load_plan(root=ROOT) if (ROOT / PLAN_PATH).exists() else build_plan(root=ROOT)
    targets = plan["targets"]
    assert tuple((item["market"], item["fold_id"]) for item in targets) == EXPECTED_TARGETS
    assert all(len(item["evaluation_session_ids"]) == 63 for item in targets)
    assert [len(item["training_session_ids"]) for item in targets] == [
        504, 630, 504, 630, 504, 630, 693, 945, 630,
    ]
    assert not any(
        session.startswith("2025")
        for item in targets
        for key in ("training_session_ids", "evaluation_session_ids")
        for session in item[key]
    )
    assert plan["target_failed_fold_market_count"] == 9
    assert plan["classifications"] == list(CLASSIFICATIONS)
    assert plan["price_free_output"] is True
    assert plan["authority"]["returns"] is False
    assert plan["authority"]["historical_row_read"] is True
    assert plan["execution_limits"]["maximum_attempts"] == 1
    assert plan["execution_limits"]["maximum_retries"] == 0


def test_diagnostic_reuses_the_transition_stable_preparatory_operation() -> None:
    assert ALPHA_LADDER_READINESS_CENSUS_OPERATION \
        in PREPARATORY_REAL_HISTORY_OPERATIONS


def test_plan_rejects_rehashed_target_or_authority_drift() -> None:
    plan = build_plan(root=ROOT)
    changed = copy.deepcopy(plan)
    changed["targets"][0]["expected_training_feature_gaps"] += 1
    with pytest.raises(IntegrityError, match="plan drifted"):
        validate_plan(changed, root=ROOT)
    changed = copy.deepcopy(plan)
    changed["authority"]["returns"] = True
    with pytest.raises(IntegrityError, match="plan drifted"):
        validate_plan(changed, root=ROOT)


def test_future_scope_is_diagnostic_only_after_plan_is_created() -> None:
    if not (ROOT / PLAN_PATH).exists():
        pytest.skip("immutable plan is created after synthetic tests")
    plan = load_plan(root=ROOT)
    scope = required_scope(root=ROOT, plan=plan)
    assert scope["purpose"] == (
        "EXACT_PRICE_FREE_DIAGNOSIS_OF_NINE_SEALED_TIER1_FEATURE_GAPS"
    )
    assert scope["target_failed_fold_market_count"] == "9"
    assert scope["price_free_output"] == "true"
    assert scope["returns"] == "false"
    assert scope["registration"] == "false"
    assert scope["holdout_2025_access"] == "false"


def test_output_is_absent_before_execution_and_runner_is_describe_only() -> None:
    assert not (ROOT / OUTPUT_ROOT).exists()
    runner = (ROOT / "scripts/run_alpha_ladder_feature_gap_diagnostic.py").read_text(
        encoding="utf-8",
    )
    assert "execute_once" not in runner
    assert "BLOCKED_SEPARATE_WINDOWS_HOST_ROW_READ_APPROVAL_REQUIRED" in runner
