from __future__ import annotations

from datetime import date, datetime, time, timedelta
from dataclasses import replace
from decimal import Decimal

from futures_rebuild.alpha_ladder_limit_readiness import CT, LimitBar
from futures_rebuild.alpha_ladder_limit_readiness_v2 import _fold_evidence
from futures_rebuild import alpha_ladder_limit_readiness as v1
from futures_rebuild import alpha_ladder_limit_readiness_v2 as v2
from futures_rebuild.alpha_ladder_frozen_mechanism import MANDATORY_BASELINES
from futures_rebuild.preexecution_fold_certification import (
    ROW_CERTIFIED,
    build_fold_readiness_certificate,
)


def _bars(session_date: date = date(2020, 1, 2)) -> tuple[LimitBar, ...]:
    start = datetime.combine(session_date, time(9, 30), CT)
    return tuple(LimitBar(
        event_at=start + timedelta(minutes=i),
        available_at=start + timedelta(minutes=i, seconds=65),
        identity="a" * 64, open=Decimal("100"), high=Decimal("101"),
        low=Decimal("99"), close=Decimal("100"), volume=Decimal("100"),
        tick_size=Decimal("0.25"), tick_value=Decimal("12.5"),
    ) for i in range(81))


def test_v2_evidence_exactly_matches_universal_baseline_schema() -> None:
    fold = {"fold_id": "fold-0", "training_sessions": ["2020-01-02"],
            "evaluation_sessions": ["2020-01-02"], "purge_minutes": 40,
            "embargo_sessions": ["2020-01-01"]}
    evidence = _fold_evidence(
        market="ES", fold=fold,
        rows_by_session={"2020-01-02": _bars(),
                         "__cost_ticks__": {"base": 2, "stress": 4, "extreme": 8}},
        risk_by_session={},
    )
    expected = {"expected_sessions", "terminal_sessions", "selected_sessions",
                "selected_path_complete_sessions", "scenario_risk_dispositions",
                "schedule_independently_derived", "flat_no_trade"}
    assert all(set(item) == expected
               for item in evidence["baseline_universe_readiness"].values())
    certificate = build_fold_readiness_certificate(
        trial_family="synthetic_v2", protocol_id="1" * 64,
        source_bindings={"synthetic.json": "2" * 64}, fold_evidence=(evidence,),
        required_markets=("ES",), required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=("base", "stress", "extreme"),
        required_outer_fold_ids=("fold-0",), required_nested_fold_ids=(),
        expected_outer_folds=1, expected_nested_folds=0,
        minimum_training_sessions=1, minimum_evaluation_sessions=1,
        minimum_purge_minutes=40, minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )
    assert certificate["fold_market_results"][0]["failed_gates"] == []


def test_scope_adapter_remains_stable_while_v1_gateway_is_patched(monkeypatch, tmp_path) -> None:
    plan = {
        "execution_limits": {"worker_deadline_seconds": 3300,
                             "maximum_runtime_seconds": 3600},
        "plan_id": "1" * 64,
    }
    monkeypatch.setattr(v2, "PLAN_PATH", tmp_path / "plan.json")
    monkeypatch.setattr(v1, "PLAN_PATH", tmp_path / "plan.json")
    monkeypatch.setattr(v1, "OUTPUT_ROOT", v2.OUTPUT_ROOT)
    (tmp_path / "plan.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(v1, "required_scope", v2.required_scope)
    scope = v2.required_scope(root=tmp_path, plan=plan)
    assert scope["output_root"] == v2.OUTPUT_ROOT.as_posix()
    assert scope["returns"] == "false"


def test_multiple_direction_scenario_failures_count_as_one_session_exclusion() -> None:
    bars = list(_bars())
    bars[40] = replace(bars[40], identity="b" * 64)
    fold = {"fold_id": "fold-0", "training_sessions": ["2020-01-02"],
            "evaluation_sessions": ["2020-01-02"], "purge_minutes": 40,
            "embargo_sessions": ["2020-01-01"]}
    evidence = _fold_evidence(
        market="ES", fold=fold,
        rows_by_session={"2020-01-02": tuple(bars),
                         "__cost_ticks__": {"base": 2, "stress": 4, "extreme": 8}},
        risk_by_session={},
    )
    assert sum(value for reason, value in evidence["exclusion_reasons"].items()
               if reason.startswith("TRAINING__")) == 1
    assert sum(value for reason, value in evidence["exclusion_reasons"].items()
               if reason.startswith("EVALUATION__")) == 1
    certificate = build_fold_readiness_certificate(
        trial_family="synthetic_incomplete_v2", protocol_id="1" * 64,
        source_bindings={"synthetic.json": "2" * 64}, fold_evidence=(evidence,),
        required_markets=("ES",), required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=("base", "stress", "extreme"),
        required_outer_fold_ids=("fold-0",), required_nested_fold_ids=(),
        expected_outer_folds=1, expected_nested_folds=0,
        minimum_training_sessions=1, minimum_evaluation_sessions=1,
        minimum_purge_minutes=40, minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )
    assert certificate["overall_decision"] == "FAIL"
