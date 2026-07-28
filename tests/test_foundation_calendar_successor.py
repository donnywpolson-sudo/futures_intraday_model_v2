from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.foundation.calendar_successor import (
    APPROVAL_SCHEMA,
    OPERATION,
    InvocationBudgetReached,
    InvocationLimiter,
    _assert_calendar_interval_coverage,
    _require_official_historical_calendar_route,
    validate_approval,
)
from futures_rebuild.errors import ContractError


def test_official_historical_successor_is_disabled_by_selected_policy() -> None:
    root = __import__("pathlib").Path(__file__).resolve().parents[1]
    boundary = SimpleNamespace(active_root=root)
    with pytest.raises(
        IntegrityError,
        match="selected route is DBN empirical observability",
    ):
        _require_official_historical_calendar_route(boundary)


def test_invocation_limiter_yields_only_after_persisted_checkpoint_bound() -> None:
    limiter = InvocationLimiter(
        maximum_checkpoints=3,
        maximum_completed_intervals=10,
        maximum_seconds=60,
    )

    limiter("calendar_coverage")
    limiter("ES:2025:raw")
    with pytest.raises(InvocationBudgetReached):
        limiter("ES:2025:causal")

    assert limiter.checkpoints == 3
    assert limiter.completed_intervals == 0


def test_invocation_limiter_counts_completed_intervals_and_allows_final() -> None:
    limiter = InvocationLimiter(
        maximum_checkpoints=100,
        maximum_completed_intervals=2,
        maximum_seconds=60,
    )

    limiter("ES:2024:outcome_source_input")
    with pytest.raises(InvocationBudgetReached):
        limiter("ES:2025:outcome_source_input")

    final_limiter = InvocationLimiter(
        maximum_checkpoints=1,
        maximum_completed_intervals=1,
        maximum_seconds=1,
    )
    final_limiter("foundation_set")


def test_exact_approval_validation_rejects_plan_drift() -> None:
    plan = {"plan_id": "a" * 64}
    approved_at = datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc)
    core = {
        "approved_at": approved_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": "b" * 64,
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "user_authorization_id": "c" * 64,
    }
    approval = {**core, "approval_receipt_id": sha256_json(core)}

    assert (
        validate_approval(
            approval,
            plan=plan,
            plan_sha256="b" * 64,
        )
        == approval["approval_receipt_id"]
    )

    with pytest.raises(IntegrityError):
        validate_approval(
            approval,
            plan=plan,
            plan_sha256="d" * 64,
        )


def test_calendar_successor_fails_before_plan_when_history_is_uncovered() -> None:
    class FutureOnlyIndex:
        @staticmethod
        def calendar_for(market, trade_date):
            if trade_date.year < 2026:
                raise ContractError(f"{market} is outside active coverage")
            return object()

    intervals = (
        SimpleNamespace(
            end="2011-01-01",
            market="ES",
            start="2010-01-01",
        ),
    )

    with pytest.raises(
        IntegrityError,
        match="cannot cover the exact historical foundation interval scope",
    ):
        _assert_calendar_interval_coverage(FutureOnlyIndex(), intervals)
