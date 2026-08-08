from __future__ import annotations

import copy
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_limit_readiness import CT, LimitBar
from futures_rebuild.alpha_ladder_reported_trade_exit_readiness import (
    PLAN_PATH,
    _direction_path,
    _fold_evidence,
    build_plan,
    classify_session,
    load_plan,
    required_scope,
    validate_plan,
)
from futures_rebuild.alpha_ladder_reported_trade_exit_tier0 import MECHANISM_ID
from futures_rebuild.errors import IntegrityError
from futures_rebuild.preexecution_fold_certification import (
    ROW_CERTIFIED,
    build_fold_readiness_certificate,
)


ROOT = Path(__file__).resolve().parents[1]


def _bars(session_date: date = date(2020, 1, 2)) -> tuple[LimitBar, ...]:
    start = datetime.combine(session_date, time(9, 30), CT)
    return tuple(LimitBar(
        event_at=start + timedelta(minutes=index),
        available_at=start + timedelta(minutes=index, seconds=65),
        identity="a" * 64,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("100"),
        tick_size=Decimal("0.25"),
        tick_value=Decimal("12.5"),
    ) for index in range(81))


COSTS = {"base": 2, "stress": 4, "extreme": 8}


def test_reported_trade_exit_does_not_require_price_to_return() -> None:
    bars = list(_bars())
    bars[64] = replace(
        bars[64], open=Decimal("98.5"), high=Decimal("99"),
        low=Decimal("98"), close=Decimal("98.5"),
    )
    feature = tuple(bars[9:30])
    filled, complete, disposition = _direction_path(
        bars=bars,
        trigger=bars[30],
        feature=feature,
        direction="LONG",
        scenario="base",
        adverse_ticks=2,
    )
    assert filled is True
    assert complete is True
    assert disposition.endswith("VERIFIED_REPORTED_TRADE_EXIT")


def test_protective_stop_remains_active_until_exit_proxy() -> None:
    bars = list(_bars())
    bars[63] = replace(bars[63], low=Decimal("96"))
    filled, complete, disposition = _direction_path(
        bars=bars,
        trigger=bars[30],
        feature=tuple(bars[9:30]),
        direction="LONG",
        scenario="stress",
        adverse_ticks=4,
    )
    assert filled is True and complete is True
    assert disposition.endswith("VERIFIED_PROTECTIVE_STOP")


def test_protective_stop_has_precedence_at_reported_exit_open() -> None:
    bars = list(_bars())
    bars[64] = replace(
        bars[64], open=Decimal("96"), high=Decimal("101"),
        low=Decimal("95"), close=Decimal("100"),
    )
    filled, complete, disposition = _direction_path(
        bars=bars,
        trigger=bars[30],
        feature=tuple(bars[9:30]),
        direction="LONG",
        scenario="stress",
        adverse_ticks=4,
    )
    assert filled is True and complete is True
    assert disposition.endswith("VERIFIED_PROTECTIVE_STOP")


def test_missing_reported_exit_after_fill_fails_closed() -> None:
    bars = _bars()[:64]
    filled, complete, disposition = _direction_path(
        bars=bars,
        trigger=bars[30],
        feature=tuple(bars[9:30]),
        direction="LONG",
        scenario="base",
        adverse_ticks=2,
    )
    assert filled is True and complete is False
    assert "REPORTED_TRADE_EXIT_EVIDENCE_MISSING" in disposition


def test_active_baselines_derive_their_own_directions(monkeypatch) -> None:
    from futures_rebuild import alpha_ladder_reported_trade_exit_readiness as module

    observed = []

    def fake_path(**kwargs):
        observed.append(kwargs["direction"])
        return True, True, "PASS"

    monkeypatch.setattr(module, "_direction_path", fake_path)
    bars = _bars()
    classify_session(
        session="2020-01-02", bars=bars, cost_ticks=COSTS,
        baseline="risk_matched_always_long",
    )
    assert set(observed) == {"LONG"}
    observed.clear()
    classify_session(
        session="2020-01-02", bars=bars, cost_ticks=COSTS,
        baseline="risk_matched_always_short",
    )
    assert set(observed) == {"SHORT"}
    observed.clear()
    classify_session(
        session="2020-01-02", bars=bars, cost_ticks=COSTS,
        baseline="fold_local_unconditional_direction",
    )
    assert set(observed) == {"LONG", "SHORT"}


def test_sign_baselines_use_their_own_causal_direction(monkeypatch) -> None:
    from futures_rebuild import alpha_ladder_reported_trade_exit_readiness as module

    observed = []

    def fake_path(**kwargs):
        observed.append(kwargs["direction"])
        return True, True, "PASS"

    monkeypatch.setattr(module, "_direction_path", fake_path)
    bars = list(_bars())
    bars[29] = replace(bars[29], close=Decimal("101"))
    classify_session(
        session="2020-01-02", bars=bars, cost_ticks=COSTS,
        baseline="previous_reported_bar_sign_momentum",
    )
    assert set(observed) == {"LONG"}
    observed.clear()
    classify_session(
        session="2020-01-02", bars=bars, cost_ticks=COSTS,
        baseline="previous_reported_bar_sign_reversal",
    )
    assert set(observed) == {"SHORT"}


def test_duplicate_source_timestamp_fails_candidate_and_baseline_closed() -> None:
    bars = (*_bars(), _bars()[30])
    candidate = classify_session(
        session="2020-01-02", bars=bars, cost_ticks=COSTS,
    )
    baseline = classify_session(
        session="2020-01-02", bars=bars, cost_ticks=COSTS,
        baseline="risk_matched_always_long",
    )
    assert candidate.selected is True and candidate.path_complete is False
    assert baseline.selected is True and baseline.path_complete is False
    assert candidate.dispositions == ("AMBIGUOUS_DUPLICATE_SOURCE_TIMESTAMP",)


def test_fold_evidence_has_independent_complete_baseline_universes() -> None:
    fold = {
        "fold_id": "fold-0",
        "training_sessions": ["2020-01-02"],
        "evaluation_sessions": ["2020-01-02"],
        "purge_minutes": 40,
        "embargo_sessions": ["2020-01-01"],
    }
    evidence = _fold_evidence(
        market="ES",
        fold=fold,
        rows_by_session={"2020-01-02": _bars(), "__cost_ticks__": COSTS},
        risk_by_session={},
    )
    baselines = evidence["baseline_universe_readiness"]
    assert baselines["flat_no_trade"]["selected_sessions"] == 0
    assert baselines["flat_no_trade"]["selected_path_complete_sessions"] == 0
    assert all(item["schedule_independently_derived"] is True for item in baselines.values())
    assert all(
        item["selected_sessions"] == item["selected_path_complete_sessions"]
        for name, item in baselines.items() if name != "flat_no_trade"
    )
    certificate = build_fold_readiness_certificate(
        trial_family="synthetic_reported_trade_exit",
        protocol_id="1" * 64,
        source_bindings={"synthetic.json": "2" * 64},
        fold_evidence=(evidence,),
        required_markets=("ES",),
        required_baselines=tuple(baselines),
        required_cost_scenarios=("base", "stress", "extreme"),
        required_outer_fold_ids=("fold-0",),
        required_nested_fold_ids=(),
        expected_outer_folds=1,
        expected_nested_folds=0,
        minimum_training_sessions=1,
        minimum_evaluation_sessions=1,
        minimum_purge_minutes=40,
        minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED,
        historical_rows_opened=True,
    )
    assert certificate["overall_decision"] == "PASS"


def test_one_missing_filled_exit_fails_candidate_and_mandatory_baseline_gates() -> None:
    fold = {
        "fold_id": "fold-0",
        "training_sessions": ["2020-01-02"],
        "evaluation_sessions": ["2020-01-02"],
        "purge_minutes": 40,
        "embargo_sessions": ["2020-01-01"],
    }
    evidence = _fold_evidence(
        market="ES",
        fold=fold,
        rows_by_session={"2020-01-02": _bars()[:64], "__cost_ticks__": COSTS},
        risk_by_session={},
    )
    certificate = build_fold_readiness_certificate(
        trial_family="synthetic_missing_exit",
        protocol_id="1" * 64,
        source_bindings={"synthetic.json": "2" * 64},
        fold_evidence=(evidence,),
        required_markets=("ES",),
        required_baselines=tuple(evidence["baseline_universe_readiness"]),
        required_cost_scenarios=("base", "stress", "extreme"),
        required_outer_fold_ids=("fold-0",),
        required_nested_fold_ids=(),
        expected_outer_folds=1,
        expected_nested_folds=0,
        minimum_training_sessions=1,
        minimum_evaluation_sessions=1,
        minimum_purge_minutes=40,
        minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED,
        historical_rows_opened=True,
    )
    result = certificate["fold_market_results"][0]
    assert certificate["overall_decision"] == "FAIL"
    assert "CANDIDATE_SELECTED_PATH_COVERAGE" in result["failed_gates"]
    assert "MANDATORY_BASELINE_SELECTED_PATH_COVERAGE" in result["failed_gates"]


def test_plan_is_exactly_bound_and_unexecuted() -> None:
    plan = load_plan(root=ROOT) if (ROOT / PLAN_PATH).exists() else build_plan(root=ROOT)
    assert plan["mechanism_id"] == MECHANISM_ID
    assert plan["markets"] == ["ES", "CL", "ZN", "6E"]
    assert plan["tier_1"]["initial_training_sessions"] == 504
    assert plan["tier_1"]["no_cross_market_calendar_intersection_drop"] is True
    assert plan["exit_price_return_condition"] is False
    assert plan["coverage"] == {
        "checkpoint_accounting_percent": 100,
        "active_baseline_checkpoint_accounting_percent": 100,
        "filled_entry_verified_exit_percent": 100,
        "future_complete_path_filtering": False,
    }
    assert plan["execution_limits"]["maximum_attempts"] == 1
    assert plan["execution_limits"]["maximum_retries"] == 0
    assert plan["authority"]["historical_row_read"] is True
    assert plan["authority"]["returns"] is False


def test_scope_grants_only_future_readiness_row_access() -> None:
    if not (ROOT / PLAN_PATH).exists():
        pytest.skip("immutable plan is created after focused preparation tests")
    plan = load_plan(root=ROOT)
    scope = required_scope(root=ROOT, plan=plan)
    assert scope["purpose"] == "ALPHA_REPORTED_TRADE_EXIT_PILOT_AND_TIER1_READINESS_ONLY"
    assert scope["returns"] == "false"
    assert scope["model_fit"] == "false"
    assert scope["registration"] == "false"
    assert scope["holdout_2025_access"] == "false"


def test_plan_rejects_weakened_coverage_even_if_identity_is_rehashed() -> None:
    from futures_rebuild.canonical import sha256_json

    plan = build_plan(root=ROOT)
    changed = copy.deepcopy(plan)
    changed["coverage"]["filled_entry_verified_exit_percent"] = 99
    core = {key: value for key, value in changed.items() if key != "plan_id"}
    changed["plan_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="plan drifted"):
        validate_plan(changed, root=ROOT)


def test_plan_rejects_old_252_session_tier1_minimum() -> None:
    from futures_rebuild.canonical import sha256_json

    plan = build_plan(root=ROOT)
    changed = copy.deepcopy(plan)
    changed["tier_1"]["initial_training_sessions"] = 252
    core = {key: value for key, value in changed.items() if key != "plan_id"}
    changed["plan_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="plan drifted"):
        validate_plan(changed, root=ROOT)


def test_plan_rejects_removed_dependency_binding() -> None:
    from futures_rebuild.canonical import sha256_json

    plan = build_plan(root=ROOT)
    changed = copy.deepcopy(plan)
    changed["bindings"].pop("src/futures_rebuild/active_data_view.py")
    core = {key: value for key, value in changed.items() if key != "plan_id"}
    changed["plan_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="plan drifted"):
        validate_plan(changed, root=ROOT)
