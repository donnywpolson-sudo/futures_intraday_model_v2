from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.intraday_alpha_discovery import (
    BASELINES,
    IntradayBar,
    build_portfolio_path,
    build_program,
    enumerate_programs,
    evaluate_program_decision,
    materialize_candidate_matrices,
    materialize_program_inputs,
    run_synthetic_acceptance,
    select_inner_wfa_winner,
    validate_program,
)
from futures_rebuild.research.dsr import deflated_sharpe_ratio


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.high_risk


def test_frozen_program_is_exact_and_bound_to_current_metadata() -> None:
    program = build_program(ROOT)
    assert validate_program(ROOT, program) == program
    assert [item["program_name"] for item in program["programs"]] == [
        "FLOW_CONTINUATION",
        "LIQUIDITY_SHOCK_REVERSAL",
        "CROSS_MARKET_INFORMATION_TRANSFER",
    ]
    assert [len(item["candidates"]) for item in program["programs"]] == [12, 12, 12]
    assert program["candidate_count"] == 36
    assert program["search_accounting"]["minimum_information_family"] == 168
    assert program["decision_rules"]["romano_wolf_adjusted_one_sided_p_maximum"] == pytest.approx(0.05 / 168)
    assert program["monitoring"]["state"] == "BLOCKED_MISSING_FINITE_SESSION_PNL_BOUND"
    assert set(program["bindings"]["implementation"]) >= {
        "src/futures_rebuild/intraday_alpha_discovery.py",
        "src/futures_rebuild/certified_research_gateway.py",
        "src/futures_rebuild/research/dsr.py",
    }
    assert program["bindings"]["abandoned_pre_result_design"]["status"] == (
        "ABANDONED_PRE_RESULT_NOT_EVALUATED"
    )
    assert program["source_rules"]["primary_signal_cadence"] == "ohlcv_1m"
    assert program["source_rules"]["ohlcv_1s_use"].endswith(
        "ADVERSE_PATH_DIAGNOSTICS_ONLY"
    )
    assert program["session_rules"]["latest_entry_by_horizon"] == {
        "15": "14:40:00",
        "30": "14:25:00",
        "60": "13:55:00",
    }
    assert program["portfolio"]["minimum_passing_programs"] == 2
    assert program["power"]["design_alternative_usd_per_session"] > program["power"]["mees_usd_per_session"]
    assert not any(program["authority"].values())


def test_inner_wfa_selection_is_deterministic_and_outer_max_is_not_required_for_dsr() -> None:
    program = enumerate_programs()[0]
    scores = np.zeros((12, 4), dtype=np.float64)
    scores[3, :] = 1.0
    assert select_inner_wfa_winner(program, scores) == (
        3,
        program["candidates"][3]["candidate_id"],
    )
    returns = np.array([-1.0, 0.5, 1.0, 2.0, 1.5, 0.25], dtype=np.float64)
    selected_sharpe = float(np.mean(returns) / np.std(returns, ddof=1))
    census = np.array([selected_sharpe, selected_sharpe + 0.5, -0.5], dtype=np.float64)
    result = deflated_sharpe_ratio(
        returns,
        census,
        raw_trial_count=3,
        selected_trial_index=0,
        selection_rule="PREREGISTERED_INNER_WFA_WINNER",
    )
    assert result.selection_rule == "PREREGISTERED_INNER_WFA_WINNER"


def _strong_inputs(specification: dict[str, object]) -> dict[str, object]:
    generator = np.random.Generator(np.random.PCG64(17))
    sessions = 160
    candidates = 12
    stress = generator.normal(6.0, 5.0, size=(sessions, candidates)).astype(np.float64)
    stress[:, 0] = generator.normal(32.0, 7.0, size=sessions)
    costs = {
        "gross": stress + 8.0,
        "base": stress + 4.0,
        "stress": stress,
        "extreme": stress - 5.0,
    }
    baselines = generator.normal(-4.0, 4.0, size=(sessions, len(BASELINES))).astype(np.float64)
    baselines[:, 0] = 0.0
    inner = np.zeros((candidates, 4), dtype=np.float64)
    inner[0, :] = 2.0
    census = np.linspace(-1.0, 0.5, 168, dtype=np.float64)
    selected_sharpe = float(np.mean(stress[:, 0]) / np.std(stress[:, 0], ddof=1))
    census[132] = selected_sharpe
    current_ids = [
        candidate["candidate_id"]
        for item in specification["programs"]
        for candidate in item["candidates"]
    ]
    trial_ids = [f"LEGACY_{index:03d}" for index in range(105)] + specification["bindings"]["abandoned_pre_result_design"]["candidate_ids"] + current_ids
    dates = tuple(f"2020-{1 + index // 28:02d}-{1 + index % 28:02d}" for index in range(sessions // 4) for _ in range(4))
    markets = tuple(market for _index in range(sessions // 4) for market in ("CL", "ES", "GC", "NQ"))
    return {
        "specification": specification,
        "program_name": "FLOW_CONTINUATION",
        "source_context": {"source_kind": "SYNTHETIC", "gateway_receipt": None, "exposure_fit_scope": "OUTER_TRAINING_ONLY"},
        "session_ids": tuple(f"S{index:03d}" for index in range(sessions)),
        "session_dates": dates,
        "market_ids": markets,
        "mechanism_pair_ids": None,
        "candidate_returns_by_cost": costs,
        "baseline_stress_returns": baselines,
        "inner_stress_sharpes": inner,
        "outer_fold_ids": np.repeat(np.arange(8, dtype=np.int8), sessions // 8),
        "trade_flags": np.ones((sessions, candidates), dtype=np.bool_),
        "event_times_by_candidate": tuple(
            tuple(f"2020-01-01T{12 + (index % 3):02d}:00:00+00:00" for _candidate in range(candidates))
            for index in range(sessions)
        ),
        "direction": np.where(np.arange(sessions) % 2, -1, 1).astype(np.int8),
        "regimes": {
            name: tuple(("LOW", "MIDDLE", "HIGH")[index % 3] for index in range(sessions))
            for name in ("volatility", "trend", "gap", "liquidity")
        },
        "adverse_path_usd": -np.abs(generator.normal(20.0, 5.0, size=sessions)).astype(np.float64),
        "control_session_returns": {
            "LOW_VOLUME_MOVE": generator.normal(-3.0, 4.0, size=sessions).astype(np.float64),
            "TEMPORALLY_REVERSED_CONTROL": generator.normal(-3.0, 4.0, size=sessions).astype(np.float64),
        },
        "residual_stress_returns": stress[:, 0] - 2.0,
        "exposure_coefficients_by_fold": {
            name: np.zeros(8, dtype=np.float64)
            for name in ("market_direction", "trend", "realized_volatility", "liquidity", "gap", "time_of_day")
        },
        "complete_trial_ids": tuple(trial_ids),
        "complete_trial_sharpes": census,
        "bootstrap_seed": 11,
        "romano_wolf_seed": 13,
        "resamples": 5000,
    }


def test_one_canonical_decision_path_passes_strong_complete_synthetic_evidence() -> None:
    decision = evaluate_program_decision(**_strong_inputs(build_program(ROOT)))
    assert decision["outcome"] == "PASS"
    assert decision["evaluation_classification"] == "SYNTHETIC_MECHANICS_ONLY"
    assert decision["evidence_label"] is None
    assert decision["executable_alpha"] is False
    assert len(decision["candidate_session_returns"]["stress"]) == 160
    assert len(decision["candidate_session_returns"]["stress"][0]) == 12
    assert decision["tail_report"]["one_second_adverse_path"]["complete"] is True


def test_missing_legacy_sharpe_census_is_diagnostic_only_never_fabricated() -> None:
    inputs = _strong_inputs(build_program(ROOT))
    inputs["complete_trial_ids"] = None
    inputs["complete_trial_sharpes"] = None
    decision = evaluate_program_decision(**inputs)
    assert decision["outcome"] == "PASS"
    assert decision["metrics"]["dsr_status"] == "NOT_AVAILABLE_INCOMPLETE_LEGACY_CENSUS"


def test_real_history_requires_gateway_binding_and_incomplete_gamma_report_blocks() -> None:
    inputs = _strong_inputs(build_program(ROOT))
    inputs["source_context"] = {"source_kind": "CERTIFIED_REAL_HISTORY", "exposure_fit_scope": "OUTER_TRAINING_ONLY"}
    with pytest.raises(IntegrityError, match="gateway"):
        evaluate_program_decision(**inputs)


def test_portfolio_uses_all_inverse_vol_sleeves_and_nets_opposing_intents() -> None:
    pnl = np.ones((4, 2, 2), dtype=np.float64)
    intent = np.ones((4, 2, 2), dtype=np.int8)
    intent[0, 1, 0] = -1
    result = build_portfolio_path(pnl, intent, np.array([1.0, 2.0], dtype=np.float64))
    assert result["program_count"] == 2
    assert result["weights"] == pytest.approx([2 / 3, 1 / 3])
    assert result["session_pnl"][0] == pytest.approx(1.0)
    assert max(result["outer_training_variance_share"]) <= 0.6


def test_synthetic_campaign_acceptance_is_deterministic_and_strict() -> None:
    first = run_synthetic_acceptance()
    assert first == run_synthetic_acceptance()
    assert first["status"] == "PASS_FULL_SHARED_PROMOTION_GATE_SYNTHETIC_CERTIFICATION"
    assert first["activation_gate_satisfied"] is True
    assert first["null"]["campaigns"] == 10_000
    assert first["null"]["one_sided_95_wilson_upper"] <= 0.05
    assert first["known_edge"]["campaigns"] == 5_000
    assert first["known_edge"]["power"] >= 0.8
    assert all(first["tail_traps"].values())


def _bar(market: str, date: str, minute: int, price: float, volume: float, *, available_offset: int = 0) -> IntradayBar:
    event = datetime(2020, 1, int(date[-2:]), 12, 0, tzinfo=timezone.utc) + timedelta(minutes=minute)
    return IntradayBar(
        market=market,
        session_date=date,
        event_at=event,
        available_at=event + timedelta(minutes=available_offset),
        actual_identity=f"{market}-H0",
        open=price,
        high=price + 0.25,
        low=price - 0.25,
        close=price,
        volume=volume,
        tick_size=0.25,
        tick_value=12.5,
    )


def test_bar_materializer_is_training_only_next_bar_and_first_event() -> None:
    bars = []
    for date_index, date in enumerate(("2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04")):
        for market_index, market in enumerate(("ES", "NQ", "CL", "GC")):
            for minute in range(100):
                price = 100.0 + market_index + minute * (0.01 + date_index * 0.002)
                volume = 100.0 + date_index * 10.0 + minute % 7
                if date == "2020-01-04" and minute == 20:
                    price += 5.0
                    volume = 1000.0
                bars.append(_bar(market, date, minute, price, volume))
    result = materialize_candidate_matrices(
        build_program(ROOT),
        program_name="FLOW_CONTINUATION",
        bars=bars,
        training_session_dates=("2020-01-01", "2020-01-02", "2020-01-03"),
        evaluation_session_dates=("2020-01-04",),
    )
    assert result["statistics_scope"] == "TRAINING_SESSION_DATES_ONLY"
    assert result["candidate_returns_by_cost"]["stress"].shape == (4, 12)
    assert np.all(result["candidate_returns_by_cost"]["gross"] >= result["candidate_returns_by_cost"]["base"])
    assert np.all(np.count_nonzero(result["trade_flags"], axis=0) <= 4)


def test_bar_materializer_rejects_duplicate_timestamp_and_future_availability_abstains() -> None:
    bar = _bar("ES", "2020-01-01", 0, 100.0, 100.0)
    with pytest.raises(IntegrityError, match="duplicate"):
        materialize_candidate_matrices(
            build_program(ROOT),
            program_name="FLOW_CONTINUATION",
            bars=(bar, bar),
            training_session_dates=("2019-12-31",),
            evaluation_session_dates=("2020-01-01",),
        )


def test_program_materializer_builds_inner_selection_baselines_and_outer_folds() -> None:
    dates = tuple(f"2020-01-{day:02d}" for day in range(1, 19))
    bars = []
    for date_index, date in enumerate(dates):
        for market_index, market in enumerate(("ES", "NQ", "CL", "GC")):
            for minute in range(85):
                price = 100.0 + market_index + minute * (0.01 + 0.001 * (date_index % 5))
                volume = 100.0 + 5.0 * (date_index % 5) + minute % 3
                if minute == 20 and date_index >= 2:
                    price += 4.0
                    volume += 800.0
                bars.append(_bar(market, date, minute, price, volume))
    result = materialize_program_inputs(
        build_program(ROOT),
        program_name="FLOW_CONTINUATION",
        bars=bars,
        training_session_dates=dates[:10],
        evaluation_session_dates=dates[10:],
    )
    assert result["inner_stress_sharpes"].shape == (12, 4)
    assert result["baseline_stress_returns"].shape == (32, len(BASELINES))
    assert set(result["outer_fold_ids"].tolist()) == set(range(8))
    assert result["selection_scope"].startswith("FOUR_CHRONOLOGICAL_INNER_FOLDS")


def test_measured_controls_residuals_and_replication_fail_closed() -> None:
    inputs = _strong_inputs(build_program(ROOT))
    inputs["control_session_returns"]["LOW_VOLUME_MOVE"] = inputs["candidate_returns_by_cost"]["stress"][:, 0].copy()
    assert "MECHANISM_NEGATIVE_CONTROLS" in evaluate_program_decision(**inputs)["failed_gates"]
    inputs = _strong_inputs(build_program(ROOT))
    inputs["residual_stress_returns"] = -np.ones(160, dtype=np.float64)
    assert "RESIDUAL_STRESS_EDGE" in evaluate_program_decision(**inputs)["failed_gates"]
    inputs = _strong_inputs(build_program(ROOT))
    inputs["candidate_returns_by_cost"]["stress"][np.asarray(inputs["market_ids"]) == "CL", 0] = -20.0
    inputs["candidate_returns_by_cost"]["extreme"][np.asarray(inputs["market_ids"]) == "CL", 0] = -25.0
    inputs["residual_stress_returns"] = inputs["candidate_returns_by_cost"]["stress"][:, 0] - 2.0
    assert "CROSS_MARKET_REPLICATION" in evaluate_program_decision(**inputs)["failed_gates"]
