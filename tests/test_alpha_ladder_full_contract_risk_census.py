from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_full_contract_risk_census import (
    PLAN_PATH,
    RiskBar,
    _folds,
    _validate_plan_semantics,
    build_account_risk_policy,
    build_plan,
    classify_feature_risk,
    load_plan,
    required_scope,
    summarize_records,
)
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.research_gateway_policy import (
    ALPHA_LADDER_READINESS_CENSUS_OPERATION,
    require_current_real_history_operation,
)


ROOT = Path(__file__).resolve().parents[1]


def _bars(*, identity: str = "a" * 64, late_last: bool = False, tick_value: str = "12.50"):
    session = date(2020, 1, 2)
    result = []
    for index in range(21):
        event = datetime.combine(session, time(9, 39)) + timedelta(minutes=index)
        available = event + timedelta(seconds=65)
        if late_last and index == 20:
            available += timedelta(seconds=1)
        result.append(RiskBar(
            event_at=event.replace(tzinfo=__import__(
                "zoneinfo"
            ).ZoneInfo("America/Chicago")),
            available_at=available.replace(tzinfo=__import__(
                "zoneinfo"
            ).ZoneInfo("America/Chicago")),
            identity=identity,
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100"),
            volume=Decimal("10"),
            tick_size=Decimal("0.25"),
            tick_value=Decimal(tick_value),
        ))
    return result


def _classify(bars):
    return classify_feature_risk(
        session="2020-01-02",
        bars=bars,
        point_value=Decimal("50"),
        fee_usd=Decimal("10"),
        cost_ticks={"base": 2, "stress": 4, "extreme": 8},
        risk_levels=(Decimal("200"), Decimal("250")),
    )


def test_full_contract_stop_cost_and_risk_are_exact() -> None:
    result = _classify(_bars())
    assert result["disposition"] == "RISK_MEASURABLE"
    assert result["stop_ticks"] == 12
    assert result["planned_loss_usd"] == {
        "base": "185.00", "extreme": "260.00", "stress": "210.00",
    }
    assert result["risk_feasible_by_level"] == {"200": False, "250": True}


def test_future_available_bar_fails_closed() -> None:
    assert _classify(_bars(late_last=True))["disposition"] == (
        "FEATURE_INCOMPLETE_MISSING_OR_LATE_BARS"
    )


def test_identity_and_economics_drift_fail_closed() -> None:
    bars = _bars()
    bars[-1] = RiskBar(**{**bars[-1].__dict__, "identity": "b" * 64})
    assert _classify(bars)["disposition"] == "FEATURE_INCOMPLETE_IDENTITY_ECONOMICS_OR_TIME"
    assert _classify(_bars(tick_value="5"))["disposition"] == "ECONOMICS_OR_FEATURE_FIELDS_INVALID"


def test_account_policy_is_owner_backed_and_fixed_1r_2r_6r() -> None:
    policy = build_account_risk_policy(
        r_usd="500",
        owner_attestation="OWNER_ACCOUNT_TOLERANCE_NOT_SELECTED_TO_PASS_CENSUS",
    )
    assert policy["instrument"] == "ONE_FULL_CONTRACT"
    assert policy["maximum_planned_loss_usd"] == "500"
    assert policy["daily_loss_limit_usd"] == "1000"
    assert policy["continuous_drawdown_limit_usd"] == "3000"
    assert policy["fixed_across_alpha_ladder"] is True
    with pytest.raises(IntegrityError, match="attestation"):
        build_account_risk_policy(r_usd="500", owner_attestation="SELECTED_TO_PASS")
    with pytest.raises(IntegrityError, match="positive"):
        build_account_risk_policy(
            r_usd="0",
            owner_attestation="OWNER_ACCOUNT_TOLERANCE_NOT_SELECTED_TO_PASS_CENSUS",
        )


def test_summary_cannot_hide_incomplete_checkpoint() -> None:
    records = [
        {
            "disposition": "RISK_MEASURABLE",
            "calendar_eligible": True,
            "source_eligible": True,
            "planned_loss_usd": {"base": "100", "stress": "200", "extreme": "300"},
        },
        {
            "disposition": "FEATURE_INCOMPLETE_MISSING_OR_LATE_BARS",
            "calendar_eligible": True,
            "source_eligible": True,
        },
    ]
    summary = summarize_records(records, (Decimal("250"),))
    assert summary["checkpoint_count"] == 2
    assert summary["expected_risk_measurable_count"] == 2
    assert summary["risk_measurable_count"] == 1
    assert summary["stress_coverage_curve"]["250"]["percent_of_expected"] == "50"


def test_fold_construction_is_locked_and_fail_closed() -> None:
    sessions = [f"2020-01-{index + 1:04d}" for index in range(1009)]
    folds = _folds(sessions)
    assert len(folds) == 8
    assert len(folds[0]["training_sessions"]) == 504
    assert len(folds[0]["evaluation_sessions"]) == 63
    assert folds[0]["purge_minutes"] == 40
    assert _folds(sessions[:-1]) == ()


def test_completed_plan_is_preserved_and_fails_closed_after_ladder_change() -> None:
    plan = json.loads((ROOT / PLAN_PATH).read_text(encoding="utf-8"))
    _validate_plan_semantics(plan)
    assert len(plan["markets"]) == 41
    assert len(plan["source_bindings"]) == 198
    assert len(plan["catalog_inventory"]) == 205
    assert plan["stop_geometry"]["contracts"] == 1
    assert plan["stop_geometry"]["micros"] is False
    assert plan["account_risk_decision"]["selected_r_usd"] is None
    assert plan["authority"]["returns_or_trade_pnl"] is False
    assert sha256_file(ROOT / PLAN_PATH) == required_scope(root=ROOT, plan=plan)[
        "approval_plan_sha256"
    ]
    with pytest.raises(IntegrityError, match="non-price binding drifted"):
        load_plan(root=ROOT)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("stop_geometry", "contracts"), 0.5),
        (("stop_geometry", "micros"), True),
        (("stop_geometry", "proxy_fills"), True),
        (("account_risk_decision", "selected_r_usd"), "500"),
        (("account_risk_decision", "daily_loss_limit"), "3R"),
        (("feasibility_gate", "minimum_evaluation_risk_feasible_sessions_per_fold"), 8),
        (("authority", "model_fit"), True),
    ],
)
def test_plan_semantic_drift_is_rejected(path, value) -> None:
    plan = build_plan(root=ROOT)
    plan[path[0]][path[1]] = value
    with pytest.raises(IntegrityError, match="semantics drifted"):
        _validate_plan_semantics(plan)


def test_operation_is_preparatory_but_unknown_alias_is_rejected() -> None:
    plan = json.loads((ROOT / PLAN_PATH).read_text(encoding="utf-8"))
    scope = required_scope(root=ROOT, plan=plan)
    require_current_real_history_operation(
        ALPHA_LADDER_READINESS_CENSUS_OPERATION, scope
    )
    with pytest.raises(UnauthorizedOperation):
        require_current_real_history_operation(
            "CENSUS_ALPHA_LADDER_FULL_CONTRACT_RISK_ALIAS", scope
        )


def test_runner_is_prepare_only() -> None:
    source = (ROOT / "scripts/run_alpha_ladder_full_contract_risk_census.py").read_text(
        encoding="utf-8"
    )
    assert "execute_once" not in source
    assert "BLOCKED_SEPARATE_WINDOWS_HOST_HISTORICAL_ROW_APPROVAL_REQUIRED" in source
