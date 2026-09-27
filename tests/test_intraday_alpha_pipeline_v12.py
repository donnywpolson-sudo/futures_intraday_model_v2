from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.intraday_alpha_execution_v12 import _active_es_row_to_bar, audit_registered_source_rows, certify_registered_source_checkpoint, _load_registration_bound_inputs, execute_es_flow_screen_once, execute_intraday_research_once
from futures_rebuild.intraday_alpha_pipeline_v12 import (
    FLAWED_AUTHORITY_ID,
    PROGRAM_SCHEMA,
    CausalBar,
    MinuteBarCube,
    ES_FLOW_COSTS,
    ES_FLOW_SCREEN_STAGE,
    _schedule,
    _array_trade,
    _minute_cube,
    _tail_gates,
    _backward_asof_bar,
    _known_edge_path,
    _shared_date_lower,
    _split_records,
    _synthetic_event_rows,
    _es_flow_candidates,
    _choose_es_rescue_candidate,
    _causal_feature_arrays,
    _es_rescue_feature,
    _mes_proxy_outcome,
    _ridge_model,
    advance_forward_monitor,
    build_es_flow_economics,
    build_es_flow_registration,
    build_es_flow_screen_program,
    build_es_forward_confirmation_contract,
    evaluate_portfolio_decision,
    generate_synthetic_minute_cube,
    run_synthetic_campaign_chunk,
    run_synthetic_certification,
    synthetic_gamma_trap_outcomes,
    synthetic_certification_manifest,
    evaluate_program_decision,
    evaluate_es_flow_screen,
    evaluate_es_flow_wfa,
    evaluate_es_forward_confirmation,
    assert_authority_not_blocked,
    build_failure_record,
    build_program,
    build_blocked_authority,
    build_certified_source_registration,
    build_source_registration,
    materialize_program_inputs,
    validate_causal_bars,
)
from futures_rebuild.research.bootstrap import apply_shared_indices, apply_shared_indices_batch, stationary_bootstrap_indices, stationary_bootstrap_indices_batch
from futures_rebuild.research.cscv import exhaustive_cscv_pbo, exhaustive_cscv_pbo_batch
from futures_rebuild.research.hac import newey_west_mean, newey_west_mean_batch
from futures_rebuild.research.multiple_testing import romano_wolf_from_differentials, romano_wolf_from_differentials_batch


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.high_risk


@lru_cache(maxsize=1)
def _rescue_program() -> dict[str, object]:
    return build_es_flow_screen_program(ROOT)


def dates(count: int) -> tuple[str, ...]:
    return tuple(str(value) for value in np.arange(np.datetime64("2018-01-01"), np.datetime64("2018-01-01") + count))


def test_predecessor_is_hash_bound_and_permanently_rejected() -> None:
    failure = build_failure_record(ROOT)
    assert failure["state"] == "SUPERSEDED_NON_ACTIVATABLE_BLOCKED_REMEDIATION"
    assert failure["predecessor"]["authority"]["authority_id"] == FLAWED_AUTHORITY_ID
    with pytest.raises(IntegrityError, match="permanently blocked"):
        assert_authority_not_blocked(ROOT, FLAWED_AUTHORITY_ID)


def test_exact_schedule_preserves_embargo_and_fit_minima() -> None:
    with pytest.raises(IntegrityError, match="1,010"):
        _schedule(dates(1009))
    folds = _schedule(dates(1010))
    assert [len(fold.audit_indices) for fold in folds] == [63] * 8
    assert [len(fold.fit_indices) for fold in folds] == [505 + 63 * index for index in range(8)]
    assert all([len(inner.audit_indices) for inner in fold.inner_folds] == [63] * 4 for fold in folds)
    assert [[len(inner.fit_indices) for inner in fold.inner_folds] for fold in folds] == [
        [252 + 63 * (outer + inner) for inner in range(4)] for outer in range(8)
    ]
    assert [fold.audit_indices.tolist() for fold in folds] == [
        list(range(506 + 63 * index, 569 + 63 * index)) for index in range(8)
    ]
    records = _split_records(dates(1010), folds)
    assert len(records) == 8
    assert len({record["split_id"] for record in records}) == 8
    assert [len(record["inner"]) for record in records] == [4] * 8


def test_materializer_consumes_bars_and_owns_all_result_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    import futures_rebuild.intraday_alpha_pipeline_v12 as pipeline

    failure = build_failure_record(ROOT)
    program = build_program(ROOT, failure)
    assert program["schema_version"] == PROGRAM_SCHEMA
    assert program["schedule"]["required_complete_dates"] == 1010
    monkeypatch.setattr(pipeline, "_schedule", lambda _dates: ())
    chicago = ZoneInfo("America/Chicago")
    bars = []
    for day in range(2):
        date = f"2020-01-{day + 2:02d}"
        start = datetime(2020, 1, day + 2, 8, 30, tzinfo=chicago)
        for market in ("CL", "ES", "GC", "NQ"):
            for minute in range(391):
                event = start + timedelta(minutes=minute)
                bars.append(CausalBar(market, date, event, event + timedelta(seconds=60), f"{market}-H20", 100.0, 100.5, 99.5, 100.0, 10.0, 0.25, 12.5))
    result = materialize_program_inputs(
        program,
        "FLOW_CONTINUATION",
        bars=bars,
        one_second_bars=(),
        stress_cost_usd=5.0,
        extreme_cost_usd=10.0,
    )
    assert result["campaign_count"] == 1
    assert result["minute_cube"].open.shape == (1, 2, 4, 391)
    assert np.all(result["minute_cube"].observed)
    result["minute_cube"].validate()
    assert not {"gross", "stress", "controls", "folds", "residuals", "baselines", "regimes", "source_context"} & set(result)
    forged = {**result, "residuals": [[999.0]]}
    with pytest.raises(IntegrityError, match="caller-supplied scientific evidence"):
        evaluate_program_decision(program, forged)
    shifted = replace(result["minute_cube"], event_at_ns=result["minute_cube"].event_at_ns + 3_600_000_000_000, available_at_ns=result["minute_cube"].available_at_ns + 3_600_000_000_000)
    with pytest.raises(IntegrityError, match="Chicago session grid"):
        shifted.validate()
    with pytest.raises(IntegrityError, match="duplicate minute"):
        materialize_program_inputs(program, "FLOW_CONTINUATION", bars=[*bars, bars[0]], one_second_bars=(), stress_cost_usd=5.0, extreme_cost_usd=10.0)
    incomplete = [bar for bar in bars if not (bar.exchange_session_date == "2020-01-03" and bar.market == "NQ" and bar.event_at.minute == 10)]
    filtered = materialize_program_inputs(program, "FLOW_CONTINUATION", bars=incomplete, one_second_bars=(), stress_cost_usd=5.0, extreme_cost_usd=10.0)
    assert filtered["exchange_session_dates"] == ("2020-01-02", "2020-01-03")
    assert not np.all(filtered["minute_cube"].observed[0, 1, 3])
    with pytest.raises(IntegrityError, match="cost ladder"):
        materialize_program_inputs(program, "FLOW_CONTINUATION", bars=bars, one_second_bars=(), stress_cost_usd=4.0, extreme_cost_usd=10.0)


def test_authority_route_rejects_forged_mapping_before_any_gateway_use() -> None:
    with pytest.raises(IntegrityError, match="actual OperationReceipt"):
        execute_intraday_research_once(ROOT, object(), {"operation": "forged"}, ROOT / "missing.json", "0" * 64)  # type: ignore[arg-type]
    with pytest.raises(IntegrityError, match="actual OperationReceipt"):
        execute_es_flow_screen_once(ROOT, object(), {"operation": "forged"}, ROOT / "missing.json", "0" * 64)  # type: ignore[arg-type]


def test_es_flow_registration_binds_only_the_frozen_program_without_authority() -> None:
    program = _rescue_program()
    registration = build_es_flow_registration(ROOT, program)
    assert registration["protocol_id"] == program["program_id"]
    assert registration["candidate_ids"] == [candidate["candidate_id"] for candidate in program["candidates"]]
    assert not any(registration["authority"].values())
    assert registration["readiness"]["sources_sha256"] == sha256_json(program["sources"])


def test_active_es_parquet_row_maps_to_causal_bar_and_es_economics() -> None:
    event = datetime(2018, 1, 2, 14, 30, tzinfo=timezone.utc)
    row = {
        "market": "ES", "project_trade_date": "2018-01-02", "source_cadence": "1m",
        "bar_start_ns": int(event.timestamp() * 1_000_000_000),
        "available_at_ns": int((event + timedelta(seconds=65)).timestamp() * 1_000_000_000),
        "actual_contract": "ESH8", "open_nano": 2_700_000_000_000,
        "high_nano": 2_701_000_000_000, "low_nano": 2_699_000_000_000,
        "close_nano": 2_700_500_000_000, "volume": 100,
        "min_price_increment_nano": 250_000_000, "multiplier_nano": 50_000_000_000,
    }
    bar = _active_es_row_to_bar(row, 2018)
    assert bar is not None
    assert (bar.exchange_session_date, bar.actual_identity, bar.tick_size, bar.tick_value) == ("2018-01-02", "ESH8", 0.25, 12.5)


def test_screen_bar_validation_does_not_require_the_five_year_wfa_schedule() -> None:
    start = datetime(2018, 1, 2, 8, 30, tzinfo=ZoneInfo("America/Chicago"))
    bars = [
        CausalBar("ES", "2018-01-02", event, event + timedelta(seconds=65), "ESH8", 100.0, 100.25, 99.75, 100.0, 1.0, 0.25, 12.5)
        for event in (start, start + timedelta(minutes=390))
    ]
    assert validate_causal_bars(bars, ("ES",), require_wfa_schedule=False)[1] == ("2018-01-02",)
    with pytest.raises(IntegrityError, match="1,010"):
        validate_causal_bars(bars, ("ES",))


def _es_flow_screen_fixture() -> tuple[dict[str, object], MinuteBarCube]:
    raw_dates = [datetime(2018, 1, 1) + timedelta(days=index) for index in range(83)]
    raw_dates += [datetime(year, 1, 1) + timedelta(days=index) for year in range(2019, 2023) for index in range(20)]
    session_dates = tuple(value.date().isoformat() for value in raw_dates)
    shape = (1, len(session_dates), 1, 391)
    event = np.broadcast_to(_synthetic_event_rows(session_dates)[None, :, None, :], shape).copy()
    available = event + 60_000_000_000
    identity = np.zeros(shape, dtype=np.int32)
    observed = np.ones(shape, dtype=np.bool_)
    day = np.arange(len(session_dates), dtype=np.float64)[:, None]
    minute = np.arange(391, dtype=np.float64)[None, :]
    close_2d = 100.0 + 0.01 * np.sin(day + minute)
    volume_2d = 10.0 + np.mod(day + minute, 5.0)
    open_2d = close_2d.copy()
    for date_index in range(63, len(session_dates)):
        trigger = 15 + date_index - 63
        close_2d[date_index, trigger:trigger + 2] += 5.0
        volume_2d[date_index, trigger:trigger + 2] = 1_000.0
        for offset in range(63):
            open_2d[date_index, trigger + 2 + offset] = 100.0 + 0.2 * offset
            close_2d[date_index, trigger + 2 + offset] = 100.0 + 0.2 * offset
    open_values = open_2d[None, :, None, :]
    close_values = close_2d[None, :, None, :]
    high = np.maximum(open_values, close_values) + 0.25
    low = np.minimum(open_values, close_values) - 0.25
    volume = volume_2d[None, :, None, :]
    tick_size = np.full(shape, 0.25, dtype=np.float64)
    tick_value = np.full(shape, 12.5, dtype=np.float64)
    cube = MinuteBarCube(session_dates, ("ES",), ("ES-CONTINUOUS",), event, available, identity, observed, open_values, high, low, close_values, volume, tick_size, tick_value)
    return _rescue_program(), cube


def _development_cube(cube: MinuteBarCube) -> MinuteBarCube:
    count = sum(int(value[:4]) <= 2020 for value in cube.exchange_session_dates)
    return replace(
        cube,
        exchange_session_dates=cube.exchange_session_dates[:count],
        **{
            name: getattr(cube, name)[:, :count].copy()
            for name in ("event_at_ns", "available_at_ns", "identity_code", "observed", "open", "high", "low", "close", "volume", "tick_size", "tick_value")
        },
    )


def test_es_flow_screen_is_eight_candidates_shared_entry_costed_and_2018_2020_only() -> None:
    specification, cube = _es_flow_screen_fixture()
    development = _development_cube(cube)
    result = evaluate_es_flow_screen(specification, development)
    assert len(result["variants"]) == 8
    assert result["qualifying_candidate_ids"] == [candidate["candidate_id"] for candidate in _es_flow_candidates()]
    assert result["decision"] == "CONTINUE_TO_HISTORICAL_WFA"
    assert result["wfa_candidate_ids"] == [candidate["candidate_id"] for candidate in _es_flow_candidates()]
    assert result["post_2020_outcomes_computed_or_exposed"] is False
    assert result["next_step_authorized"] is False
    for variant in result["variants"]:
        assert variant["opportunity_count"] == 60
        assert variant["profile_stress_net_pnl_usd"] > 0
        assert variant["median_realized_net_mfe_mae"] > 1.0
        assert len(variant["opportunities"]) == 60
    paired = {}
    for variant in result["variants"]:
        for row in variant["opportunities"]:
            paired.setdefault((variant["return_z_threshold"], variant["volume_surprise_threshold"], row["session_date"]), []).append((row["shared_event_id"], row["entry_at_ns"]))
    assert all(len(set(values)) == 1 for values in paired.values())
    with pytest.raises(IntegrityError, match="only development dates"):
        evaluate_es_flow_screen(specification, cube)
    sparse = replace(development, observed=development.observed.copy())
    sparse.observed[0, 63, 0] = False
    audited = evaluate_es_flow_screen(specification, sparse)
    assert all(variant["screen_session_count"] == 60 for variant in audited["variants"])


def _flat_outcome_cube() -> MinuteBarCube:
    rows = [
        CausalBar(**{**row.__dict__, "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0})
        for row in _minute_path()
    ]
    return _minute_cube(rows, dates=("2020-01-02",), markets=("ES",), campaign_count=1)


def test_mes_proxy_mapping_sizing_excursions_stops_and_latest_entry() -> None:
    economics = build_es_flow_economics(ROOT)
    assert (economics["execution_symbol"], economics["contract_multiplier_usd_per_point"], economics["tick_size"], economics["tick_value_usd"]) == ("MES", 5.0, 0.25, 1.25)
    cube = _flat_outcome_cube()
    for name in ("open", "high", "low", "close"):
        getattr(cube, name)[0, 0, 0, 77] = 102.0
    time_exit = _mes_proxy_outcome(cube, date_index=0, event_minute=60, horizon=15, direction=1, atr20_points=1.0, economics=economics)
    assert time_exit["exit_reason"] == "TIME_EXIT"
    assert time_exit["mfe_usd"] == pytest.approx(100.0)
    assert time_exit["mae_usd"] == 0.0
    assert time_exit["raw_mfe_mae"] == pytest.approx(8.0)
    stop_cube = _flat_outcome_cube()
    stop_cube.open[0, 0, 0, 63] = 100.0
    stop_cube.high[0, 0, 0, 63] = 110.0
    stop_cube.low[0, 0, 0, 63] = 98.5
    stopped = _mes_proxy_outcome(stop_cube, date_index=0, event_minute=60, horizon=15, direction=1, atr20_points=1.0, economics=economics)
    assert stopped["exit_reason"] == "PROTECTIVE_STOP_PESSIMISTIC_BAR_ORDER"
    assert stopped["mfe_usd"] == 0.0
    assert stopped["mae_usd"] == pytest.approx(75.0)
    gap_cube = _flat_outcome_cube()
    gap_cube.open[0, 0, 0, 63] = 98.0
    gap_cube.high[0, 0, 0, 63] = 98.25
    gap_cube.low[0, 0, 0, 63] = 97.75
    gap_cube.close[0, 0, 0, 63] = 98.0
    gap = _mes_proxy_outcome(gap_cube, date_index=0, event_minute=60, horizon=15, direction=1, atr20_points=1.0, economics=economics)
    assert gap["exit_reason"] == "GAP_THROUGH_STOP"
    assert gap["exit_price"] == 98.0
    short_cube = _flat_outcome_cube()
    short_cube.open[0, 0, 0, 77] = 98.0
    short_cube.high[0, 0, 0, 77] = 100.0
    short_cube.low[0, 0, 0, 77] = 98.0
    short_cube.close[0, 0, 0, 77] = 98.0
    short = _mes_proxy_outcome(short_cube, date_index=0, event_minute=60, horizon=15, direction=-1, atr20_points=1.0, economics=economics)
    assert short["mfe_usd"] == pytest.approx(100.0)
    assert _mes_proxy_outcome(cube, date_index=0, event_minute=60, horizon=15, direction=1, atr20_points=100.0, economics=economics)["disposition"] == "RISK_ABSTENTION"
    latest = _mes_proxy_outcome(cube, date_index=0, event_minute=323, horizon=60, direction=1, atr20_points=1.0, economics=economics)
    latest_entry = datetime.fromtimestamp(latest["entry_at_ns"] / 1_000_000_000, tz=timezone.utc).astimezone(ZoneInfo("America/Chicago"))
    assert latest_entry.strftime("%H:%M") == "13:55"


def test_mes_economics_cannot_be_supplied_and_ridge_is_three_target_fit_only() -> None:
    specification, cube = _es_flow_screen_fixture()
    tampered = json.loads(json.dumps(specification))
    tampered["execution"]["economics"]["tick_value_usd"] = 99.0
    core = {key: value for key, value in tampered.items() if key != "program_id"}
    tampered["program_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="caller-supplied"):
        evaluate_es_flow_screen(tampered, _development_cube(cube))
    features = np.arange(60, dtype=np.float64).reshape(10, 6)
    features[:, 1] += np.arange(10) ** 2
    features[:, 2] += np.arange(10) ** 3
    features[:, 3] += np.sin(np.arange(10))
    features[:, 4] += np.cos(np.arange(10))
    targets = np.arange(30, dtype=np.float64).reshape(10, 3)
    model = _ridge_model(features, targets)
    assert model["ridge_penalty"] == 1.0
    assert np.asarray(model["coefficients"]).shape == (6, 3)
    invariant = features.copy()
    invariant[:, 5] = 0.0
    with pytest.raises(IntegrityError, match="minute_of_regular_session"):
        _ridge_model(invariant, targets)


def test_rescue_program_binds_active_multifile_sources_and_volume_reference_contract() -> None:
    program = build_es_flow_screen_program(ROOT)
    assert program["candidate_count"] == 8
    assert program["research_family_accounting"]["conservative_cumulative_trial_count"] == 168
    assert [source["source_binding_count"] for source in program["sources"]] == [12] * 5
    _specification, cube = _es_flow_screen_fixture()
    identity = cube.identity_code.copy()
    identity[0, :44, 0, :] = 1
    crossed = replace(cube, identity_labels=("ES-CURRENT", "ES-PRIOR"), identity_code=identity)
    returns, volume = _causal_feature_arrays(crossed, 15)
    assert _es_rescue_feature(crossed, returns, volume, date_index=63, event_minute=20) is None
    identity[0, 43, 0, :] = 0
    admitted = replace(crossed, identity_code=identity)
    returns, volume = _causal_feature_arrays(admitted, 15)
    assert _es_rescue_feature(admitted, returns, volume, date_index=63, event_minute=20) is not None


def test_horizon_tie_break_defaults_to_15_then_lower_thresholds() -> None:
    candidates = _es_flow_candidates()
    pair = [candidate for candidate in candidates if candidate["return_z_threshold"] == 1.5 and candidate["volume_surprise_threshold"] == 0.5]
    qualifying = [{**candidate, "stress_net_pnl_usd": 10.0} for candidate in pair]
    daily = {str(candidate["candidate_id"]): np.ones(8, dtype=np.float64) for candidate in pair}
    indices = np.tile(np.arange(8, dtype=np.int64), (20, 1))
    selected, comparisons = _choose_es_rescue_candidate(qualifying, daily, indices)
    assert next(candidate for candidate in pair if candidate["candidate_id"] == selected)["holding_horizon_minutes"] == 15
    assert comparisons[0]["reason"] == "INCONCLUSIVE_DEFAULT_TO_15"


def _es_wfa_fixture() -> tuple[dict[str, object], MinuteBarCube]:
    raw_dates = [datetime(year, 1, 1) + timedelta(days=index) for year, count in ((2018, 169), (2019, 169), (2020, 168), (2021, 252), (2022, 252)) for index in range(count)]
    session_dates = tuple(value.date().isoformat() for value in raw_dates)
    shape = (1, len(session_dates), 1, 391)
    event = np.broadcast_to(_synthetic_event_rows(session_dates)[None, :, None, :], shape).copy()
    available = event + 60_000_000_000
    identity = np.zeros(shape, dtype=np.int32)
    observed = np.ones(shape, dtype=np.bool_)
    day = np.arange(len(session_dates), dtype=np.float64)[:, None]
    minute = np.arange(391, dtype=np.float64)[None, :]
    close = 100.0 + 0.01 * np.sin(day * 0.7 + minute * 0.11)
    volume = 10.0 + np.mod(day + minute, 7.0)
    open_values = close.copy()
    for date_index in range(63, len(session_dates)):
        trigger = 20 + (date_index - 63) % 250
        close[date_index, trigger] += 5.0 + 0.01 * (date_index % 5)
        volume[date_index, trigger] = 1_000.0 + date_index % 11
        for offset in range(63):
            value = 100.0 + 0.2 * offset + 0.001 * (date_index % 17) * offset
            open_values[date_index, trigger + 2 + offset] = value
            close[date_index, trigger + 2 + offset] = value
    open_4d = open_values[None, :, None, :]
    close_4d = close[None, :, None, :]
    cube = MinuteBarCube(
        session_dates,
        ("ES",),
        ("ES-CONTINUOUS",),
        event,
        available,
        identity,
        observed,
        open_4d,
        np.maximum(open_4d, close_4d) + 0.25,
        np.minimum(open_4d, close_4d) - 0.25,
        close_4d,
        volume[None, :, None, :],
        np.full(shape, 0.25, dtype=np.float64),
        np.full(shape, 12.5, dtype=np.float64),
    )
    return _rescue_program(), cube


def test_wfa_is_eight_fold_training_only_three_target_and_complete_40_contrast_family() -> None:
    specification, cube = _es_wfa_fixture()
    screen = evaluate_es_flow_screen(specification, _development_cube(cube))
    result = evaluate_es_flow_wfa(specification, cube, screen)
    assert result["bootstrap"]["resamples"] == 10_000
    assert result["bootstrap"]["seed"] == 20260807
    assert result["bootstrap"]["mean_block_sessions"] == 5.0
    assert result["bootstrap"]["romano_wolf_hypotheses"] == 40
    assert result["bootstrap"]["degenerate_contrasts_retained_as_non_rejections"] > 0
    assert len(result["candidates"]) == 8
    for candidate in result["candidates"]:
        assert len(candidate["outer_models"]) == 8
        assert len(candidate["audit_sessions"]) == 504
        assert set(candidate["baseline_comparisons"]) == {"ZERO_NO_TRADE", "UNFILTERED_CONTINUATION", "MATCHED_REVERSAL", "RISK_MATCHED_ALWAYS_LONG", "RISK_MATCHED_ALWAYS_SHORT"}
        for fold in candidate["outer_models"]:
            assert fold["training_end"] < fold["audit_start"]
            assert np.asarray(fold["model"]["coefficients"]).shape == (6, 3)
        assert all(row["prediction_emitted_before_outcome_attachment"] is True for row in candidate["audit_sessions"])
    assert result["selected_candidate_id"] is None
    assert all(not candidate["gate_checks"]["ridge_improves_unfiltered_out_of_sample"] for candidate in result["candidates"])


def _forward_fixture(count: int) -> tuple[dict[str, object], list[dict[str, object]]]:
    candidate = _es_flow_candidates()[0]
    model = {
        "ridge_penalty": 1.0,
        "feature_names": ["signed_15_minute_return", "return_z_score", "volume_surprise_z_score", "atr20_divided_by_price", "realized_volatility_20_minute", "minute_of_regular_session"],
        "target_names": ["terminal_net_pnl_r", "mfe_r", "mae_r"],
        "feature_mean": [0.0] * 6,
        "feature_scale": [1.0] * 6,
        "intercept": [1.0, 2.0, 0.1],
        "coefficients": np.zeros((6, 3)).tolist(),
        "training_row_count": 100,
    }
    artifact_core = {
        "candidate": candidate,
        "model": model,
        "event_entry_stop_cost_risk_and_decision_rules": {"execution": {"economics": build_es_flow_economics(ROOT)}},
    }
    artifact = {**artifact_core, "frozen_artifact_id": sha256_json(artifact_core)}
    contract = build_es_forward_confirmation_contract(artifact, datetime(2026, 1, 1, tzinfo=timezone.utc))
    sessions = []
    start = datetime(2026, 1, 2)
    session_dates = [(start + timedelta(days=2 * index)).date().isoformat() for index in range(count)]
    month_members = {month: [value for value in session_dates if value[:7] == month] for month in {value[:7] for value in session_dates}}
    winning_dates = {
        value
        for values in month_members.values()
        for value in values[:int(np.ceil(0.65 * len(values)))]
    }
    for index, session_date in enumerate(session_dates):
        decision = 1_800_000_000_000_000_000 + index * 100_000_000_000_000
        win = session_date in winning_dates
        sessions.append({
            "session_date": session_date,
            "scheduled_session": True,
            "execution_data_kind": "MES_EXECUTABLE_BID_ASK",
            "feed_complete": True,
            "mes_actual_contract_identity": "MES-H26",
            "fee_schedule": {"independently_resolved": True, "fee_schedule_id": "TEST", "fee_schedule_sha256": "a" * 64, "observed_round_trip_cost_usd_per_contract": 1.0},
            "signal": {
                "frozen_artifact_id": artifact["frozen_artifact_id"],
                "causal_timing_valid": True,
                "actual_contract_identity_valid": True,
                "es_actual_contract_identity": "ES-H26",
                "decision_at_ns": decision,
                "atr20_points": 1.0,
                "features": dict(zip(model["feature_names"], (0.1, 2.1, 2.0, 0.01, 0.01, 100.0), strict=True)),
            },
            "quotes": [
                {"at_ns": decision + 1, "bid": 100.0, "ask": 100.25, "stale": False, "actual_contract_identity": "MES-H26"},
                {"at_ns": decision + 1 + int(candidate["holding_horizon_minutes"]) * 60_000_000_000, "bid": 101.25 if win else 99.0, "ask": 101.5 if win else 99.25, "stale": False, "actual_contract_identity": "MES-H26"},
            ],
        })
    return contract, sessions


def test_forward_contract_is_immutable_quote_only_and_obeys_minimum_and_inconclusive_limits() -> None:
    contract, sessions = _forward_fixture(200)
    with pytest.raises(IntegrityError, match="backfilled"):
        evaluate_es_forward_confirmation(contract, [{**sessions[0], "session_date": "2026-01-01"}])
    with pytest.raises(IntegrityError, match="OHLCV-only"):
        evaluate_es_forward_confirmation(contract, [{**sessions[0], "execution_data_kind": "OHLCV"}])
    changed = json.loads(json.dumps(sessions[0]))
    changed["signal"]["frozen_artifact_id"] = "changed"
    with pytest.raises(IntegrityError, match="frozen artifact"):
        evaluate_es_forward_confirmation(contract, [changed])
    assert evaluate_es_forward_confirmation(contract, sessions[:99])["status"] == "CONTINUE_COLLECTING_FORWARD_EVIDENCE"
    assert evaluate_es_forward_confirmation(contract, sessions[:100])["status"] == "CONTINUE_UNCHANGED_INCONCLUSIVE"
    assert evaluate_es_forward_confirmation(contract, sessions)["status"] == "INSUFFICIENT_FORWARD_EVIDENCE"
    passing_contract, passing = _forward_fixture(100)
    for row in passing:
        row["quotes"][1]["bid"] = 101.25
        row["quotes"][1]["ask"] = 101.5
    passed = evaluate_es_forward_confirmation(passing_contract, passing)
    assert passed["status"] == "FORWARD_PAPER_VALIDATED"
    assert passed["authoritative"] is False
    assert passed["orders_authorized"] is False


def test_naive_time_is_rejected() -> None:
    from futures_rebuild.intraday_alpha_pipeline_v12 import _chicago

    with pytest.raises(IntegrityError, match="timezone-aware"):
        _chicago(datetime(2020, 1, 1), "event_at")
    assert _chicago(datetime(2020, 1, 1, tzinfo=timezone.utc), "event_at").tzinfo is not None


def _minute_path(*, future_dependency: bool = False, roll_at: int | None = None) -> list[CausalBar]:
    chicago = ZoneInfo("America/Chicago")
    start = datetime(2020, 1, 2, 8, 30, tzinfo=chicago)
    result = []
    for minute in range(391):
        event = start + timedelta(minutes=minute)
        available = event + timedelta(seconds=60)
        if future_dependency and minute == 60:
            available = event + timedelta(seconds=66)
        identity = "ES-H20" if roll_at is None or minute < roll_at else "ES-M20"
        result.append(CausalBar("ES", "2020-01-02", event, available, identity, 100.0 + minute, 100.5 + minute, 99.5 + minute, 100.0 + minute, 10.0, 0.25, 12.5))
    return result


def test_decision_delay_strict_entry_future_dependency_and_roll_fail_closed() -> None:
    path = _minute_path()
    nonlinear = [CausalBar(**{**row.__dict__, "open": float(index * index), "close": float(index * index), "high": float(index * index + 1), "low": float(index * index - 1)}) for index, row in enumerate(path)]
    cube = _minute_cube(nonlinear, dates=("2020-01-02",), markets=("ES",), campaign_count=1)
    result = _array_trade(cube, {}, campaign=0, date=0, market_position=0, event_minute=60, lookback=15, horizon=15, direction=1, synthetic_campaign_id="TIMING_TEST")
    assert result is not None
    gross, adverse, _exposure = result
    assert gross == pytest.approx((77 * 77 - 62 * 62) / 0.25 * 12.5)
    assert np.isfinite(adverse)
    future_cube = _minute_cube(_minute_path(future_dependency=True), dates=("2020-01-02",), markets=("ES",), campaign_count=1)
    roll_cube = _minute_cube(_minute_path(roll_at=70), dates=("2020-01-02",), markets=("ES",), campaign_count=1)
    sparse_cube = _minute_cube([row for index, row in enumerate(path) if index != 70], dates=("2020-01-02",), markets=("ES",), campaign_count=1)
    assert _array_trade(future_cube, {}, campaign=0, date=0, market_position=0, event_minute=60, lookback=15, horizon=15, direction=1, synthetic_campaign_id="TIMING_TEST") is None
    assert _array_trade(roll_cube, {}, campaign=0, date=0, market_position=0, event_minute=60, lookback=15, horizon=15, direction=1, synthetic_campaign_id="TIMING_TEST") is None
    assert _array_trade(sparse_cube, {}, campaign=0, date=0, market_position=0, event_minute=60, lookback=15, horizon=15, direction=1, synthetic_campaign_id="TIMING_TEST") is None
    _report, failures = _tail_gates(np.ones((10, 1), dtype=np.float64), np.full((10, 1), np.nan, dtype=np.float64))
    assert "ONE_SECOND_ADVERSE_PATH" in failures


def test_cross_market_join_is_backward_asof_and_rejects_stale_or_future_known() -> None:
    path = _minute_path()
    leader_at = path[60].event_at
    decision = leader_at + timedelta(seconds=65)
    assert _backward_asof_bar(path, leader_at=leader_at, decision_at=decision) == (60, path[60])
    stale = [path[58]]
    assert _backward_asof_bar(stale, leader_at=leader_at, decision_at=decision) is None
    late = list(path)
    late[60] = CausalBar(**{**late[60].__dict__, "available_at": decision + timedelta(seconds=1)})
    assert _backward_asof_bar(late, leader_at=leader_at, decision_at=decision) is None


def test_scalar_and_campaign_kernels_are_identical() -> None:
    generator = np.random.Generator(np.random.PCG64(41))
    series = generator.normal(size=64).astype(np.float64)
    assert newey_west_mean(series, lag=4) == newey_west_mean_batch(series[None, :], lag=4)[0]
    indices = np.asarray([[0, 2, 1, 3], [3, 3, 2, 1]], dtype=np.int64)
    matrix = np.arange(12, dtype=np.float64).reshape(4, 3)
    assert np.array_equal(apply_shared_indices(matrix, indices), apply_shared_indices_batch(matrix[None, ...], indices)[0])
    scalar_indices = stationary_bootstrap_indices(n_observations=8, n_resamples=4, mean_block_length=2.0, seed=9)
    batch_indices = stationary_bootstrap_indices_batch(n_observations=8, n_resamples=4, mean_block_length=2.0, seeds=(9,))[0]
    assert np.array_equal(scalar_indices, batch_indices)
    candidates = generator.normal(size=(64, 3)).astype(np.float64)
    scalar_cscv = exhaustive_cscv_pbo(candidates, strategy_ids=("A", "B", "C"), blocks=8)
    batch_cscv = exhaustive_cscv_pbo_batch(candidates[None, ...], strategy_ids=("A", "B", "C"), blocks=8)[0]
    assert scalar_cscv.pbo_conservative == batch_cscv.pbo_conservative
    differentials = generator.normal(0.2, 1.0, size=(64, 2)).astype(np.float64)
    scalar_rw = romano_wolf_from_differentials(differentials, hypothesis_ids=("A", "B"), hac_lag=2, mean_block_length=8.0, n_resamples=30, seed=7, minimum_resamples=30)
    batch_rw = romano_wolf_from_differentials_batch(differentials[None, ...], hypothesis_ids=("A", "B"), hac_lag=2, mean_block_length=8.0, n_resamples=30, seeds=(7,), minimum_resamples=30)[0]
    assert np.array_equal(scalar_rw.adjusted_p_values, batch_rw.adjusted_p_values)
    synchronized = generator.normal(size=(504, 1)).astype(np.float64)
    assert _shared_date_lower(synchronized, seed=13, resamples=50) == _shared_date_lower(np.repeat(synchronized, 7, axis=1), seed=13, resamples=50)


def test_monitor_is_append_only_hash_chained_and_cannot_promote(tmp_path: Path) -> None:
    authority = "b" * 64
    first = advance_forward_monitor(tmp_path, authority, None, "2026-01-02", {"stress_pnl": 1.0})
    first_path = Path("state/intraday_research_monitor_registry") / authority / first["checkpoint_id"] / "checkpoint.json"
    second = advance_forward_monitor(tmp_path, authority, first_path, "2026-01-03", {"drawdown": 2.0}, action="DOWNGRADE")
    second_path = Path("state/intraday_research_monitor_registry") / authority / second["checkpoint_id"] / "checkpoint.json"
    third = advance_forward_monitor(tmp_path, authority, second_path, "2026-01-04", {"tail": {"es5": -1.0}})
    third_path = Path("state/intraday_research_monitor_registry") / authority / third["checkpoint_id"] / "checkpoint.json"
    assert second["prior"]["checkpoint_id"] == first["checkpoint_id"]
    assert second["inferential_state"] == "BLOCKED_MISSING_FINITE_SESSION_PNL_BOUND"
    with pytest.raises(IntegrityError, match="cannot promote"):
        advance_forward_monitor(tmp_path, authority, None, "2026-01-05", {"stress_pnl": 3.0}, action="PROMOTE")
    with pytest.raises(IntegrityError, match="non-descriptive"):
        advance_forward_monitor(tmp_path, authority, None, "2026-01-05", {"new_threshold": 1.0})
    with pytest.raises(IntegrityError, match="skip"):
        advance_forward_monitor(tmp_path, authority, first_path, "2026-01-05", {"stress_pnl": 3.0})
    (tmp_path / first_path).write_bytes(b"{}\n")
    with pytest.raises(IntegrityError):
        advance_forward_monitor(tmp_path, authority, third_path, "2026-01-05", {"stress_pnl": 3.0})


def test_portfolio_169_includes_all_passers_and_nets_opposing_intents() -> None:
    common_dates = list(dates(504))
    def decision(name: str, direction: int) -> dict[str, object]:
        values = np.full((504, 1), 20.0, dtype=np.float64)
        training = [np.resize(np.asarray((-2.0, 2.0)), 505 + 63 * fold) for fold in range(8)]
        variances = [float(np.var(path, ddof=1)) for path in training]
        return {"outcome": "PASS", "program_id": name, "exchange_session_dates": common_dates, "markets": ["ES"], "cost_paths": {"gross": (values + 5).tolist(), "base": (values + 2.5).tolist()}, "stress_date_market": values.tolist(), "extreme_date_market": (values - 1).tolist(), "adverse_date_market": np.full((504, 1), -2.0).tolist(), "residuals": values.tolist(), "direction": np.full((504, 1), direction, dtype=np.int8).tolist(), "trade_flags": np.ones((504, 1), dtype=np.bool_).tolist(), "outer_training_session_returns": [path.tolist() for path in training], "outer_training_variance": variances}
    result = evaluate_portfolio_decision({"schema_version": PROGRAM_SCHEMA}, [decision("A", 1), decision("B", -1)])
    assert result["included_program_ids"] == ["A", "B"]
    assert np.allclose(result["fold_weights"], 0.5)
    assert np.count_nonzero(result["stress_date_market"]) == 0
    assert result["outcome"] == "FAIL"


def test_synthetic_generator_is_campaign_id_deterministic_and_manifest_does_not_execute(monkeypatch: pytest.MonkeyPatch) -> None:
    import futures_rebuild.intraday_alpha_pipeline_v12 as pipeline

    monkeypatch.setattr(pipeline, "_synthetic_dates", lambda count=1010: ("2018-01-02", "2018-01-03"))
    first = generate_synthetic_minute_cube(program_name="FLOW_CONTINUATION", campaign_ids=("NULL_00000",), kind="NULL")
    second = generate_synthetic_minute_cube(program_name="FLOW_CONTINUATION", campaign_ids=("NULL_00000",), kind="NULL")
    assert np.array_equal(first.close, second.close)
    assert np.array_equal(first.available_at_ns, second.available_at_ns)
    manifest = synthetic_certification_manifest({"program_id": "p"}, chunk_size=1)
    assert manifest["campaigns"] == {"null": 10_000, "known_edge": 5_000, "known_edge_usd_per_complete_session": 30.0}
    assert manifest["romano_wolf_bootstrap_resamples"] == 10_000
    assert manifest["final_execution_performed"] is False
    with pytest.raises(IntegrityError, match="scientific-setting overrides"):
        run_synthetic_certification({"program_id": "p"}, null_campaigns=10_000, known_edge_campaigns=5_000, chunk_size=1, resamples=999, exact=True)
    assert all(not row["passed_promotion_tail_gates"] for row in synthetic_gamma_trap_outcomes())


def test_one_synthetic_campaign_uses_all_three_production_program_paths() -> None:
    program = build_program(ROOT, build_failure_record(ROOT))
    result = run_synthetic_campaign_chunk(program, campaign_ids=("SMOKE_00000",), kind="NULL", resamples=1)
    assert set(result["terminal_outcomes"][0]["program_outcomes"]) == {
        "FLOW_CONTINUATION",
        "LIQUIDITY_SHOCK_REVERSAL",
        "CROSS_MARKET_INFORMATION_TRANSFER",
    }
    assert result["production_materializer_and_evaluator"] is True


def test_full_terminal_bytes_are_scalar_chunk_invariant() -> None:
    program = build_program(ROOT, build_failure_record(ROOT))
    campaign_ids = ("SCALAR_EQ_00000", "SCALAR_EQ_00001")
    batch = run_synthetic_campaign_chunk(program, campaign_ids=campaign_ids, kind="NULL", resamples=1)
    scalar = [
        run_synthetic_campaign_chunk(program, campaign_ids=(campaign_id,), kind="NULL", resamples=1)["terminal_outcomes"][0]
        for campaign_id in campaign_ids
    ]
    assert canonical_bytes(batch["terminal_outcomes"]) == canonical_bytes(scalar)


def test_known_edge_is_observed_exactly_and_only_frozen_candidate_passes() -> None:
    program = build_program(ROOT, build_failure_record(ROOT))
    cube = generate_synthetic_minute_cube(program_name="FLOW_CONTINUATION", campaign_ids=("KNOWN_EDGE_TEST",), kind="KNOWN_EDGE")
    path = _known_edge_path(cube, 0)
    assert np.array_equal(path.sum(axis=1), np.full(1010, 30.0))
    assert np.array_equal(np.flatnonzero(path[0]), np.asarray([1, 3]))  # ES/NQ
    assert np.array_equal(np.flatnonzero(path[1]), np.asarray([0, 2]))  # CL/GC
    materialized = materialize_program_inputs(program, "FLOW_CONTINUATION", bars=cube, one_second_bars=(), stress_cost_usd=5.0, extreme_cost_usd=10.0, campaign_ids=("KNOWN_EDGE_TEST",), resamples=10_000)
    materialized.update({"synthetic": True, "synthetic_kind": "KNOWN_EDGE"})
    decision = evaluate_program_decision(program, materialized)
    assert decision["selected_candidate_indices"] == [0] * 8
    assert decision["outcome"] == "PASS"
    assert np.array_equal(np.asarray(decision["stress_date_market"]), path[-504:])


def test_metadata_source_registration_is_exact_and_loader_stops_before_dbn_scan() -> None:
    program = build_program(ROOT, build_failure_record(ROOT))
    registration = build_source_registration(ROOT, program)
    assert (registration["dbn_count"], registration["sidecar_count"]) == (144, 144)
    assert registration["dbn_size_bytes"] == 3_613_367_227
    assert registration["row_completeness"]["status"] == "BLOCKED_PENDING_SEPARATELY_APPROVED_SCAN"
    with pytest.raises(IntegrityError, match="certified one-minute"):
        _load_registration_bound_inputs(ROOT, registration)


def test_source_audit_checkpoints_each_file_and_resumes_without_reopening_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import futures_rebuild.intraday_alpha_execution_v12 as execution

    markets = ("CL", "ES", "GC", "NQ", "ZB", "ZN")
    pairs = [
        {"market": markets[index % len(markets)], "family": "definition", "year": 2000 + index, "dbn": {"sha256": f"{index:064x}", "size_bytes": index + 1}, "sidecar": {}}
        for index in range(144)
    ]
    core = {
        "schema_version": "intraday_research_source_registration/1.2.3",
        "state": "METADATA_BOUND_ROW_COMPLETENESS_AUDIT_REQUIRED",
        "source_release": {"release_id": "release", "release_manifest_sha256": "a" * 64, "inventory_sha256": "b" * 64},
        "historical_interval": {"start_inclusive": "2018-01-02T00:00:00Z", "end_exclusive": "2025-07-13T22:00:00Z"},
        "source_pairs": pairs,
        "markets": list(markets),
        "dbn_count": 144,
        "sidecar_count": 144,
        "dbn_size_bytes": sum(index + 1 for index in range(144)),
        "bindings": {},
        "requirements": {"incomplete_session_policy": "EXCLUDE_BEFORE_FOLDS_NEVER_ZERO_RETURN", "minimum_common_sessions": 1010},
    }
    registration = {**core, "registration_id": sha256_json(core)}
    monkeypatch.setattr(execution, "_dbn_binding", lambda **_kwargs: object())
    monkeypatch.setattr(execution, "_query_contract", lambda *_args: object())
    opened = 0

    def interrupted(*_args: object, **_kwargs: object):
        nonlocal opened
        opened += 1
        if opened == 3:
            raise RuntimeError("interrupted")
        return iter((object(),))

    monkeypatch.setattr(execution, "iter_definitions", interrupted)
    with pytest.raises(RuntimeError, match="interrupted"):
        audit_registered_source_rows(tmp_path, registration)
    assert opened == 3
    resumed = 0

    def finish(*_args: object, **_kwargs: object):
        nonlocal resumed
        resumed += 1
        return iter((object(),))

    monkeypatch.setattr(execution, "iter_definitions", finish)
    result = audit_registered_source_rows(tmp_path, registration)
    assert result["status"] == "BLOCKED_INSUFFICIENT_COMPLETE_SESSIONS_NO_CERTIFICATE"
    assert resumed == 142


@pytest.mark.parametrize("offsets", ((0, 0), (60, 0), (0, 90)))
def test_source_audit_rejects_duplicate_reversed_and_off_grid_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, offsets: tuple[int, int]) -> None:
    import futures_rebuild.intraday_alpha_execution_v12 as execution

    markets = ("CL", "ES", "GC", "NQ", "ZB", "ZN")
    pairs = [
        {"market": markets[index % 6], "family": "ohlcv_1m" if index == 0 else "definition", "year": 2000 + index, "dbn": {"sha256": f"{index:064x}", "size_bytes": index + 1}, "sidecar": {}}
        for index in range(144)
    ]
    core = {"schema_version": "intraday_research_source_registration/1.2.3", "state": "METADATA_BOUND_ROW_COMPLETENESS_AUDIT_REQUIRED", "source_release": {"release_id": "release", "release_manifest_sha256": "a" * 64, "inventory_sha256": "b" * 64}, "source_pairs": pairs, "markets": list(markets)}
    registration = {**core, "registration_id": sha256_json(core)}
    monkeypatch.setattr(execution, "_dbn_binding", lambda **_kwargs: object())
    monkeypatch.setattr(execution, "_query_contract", lambda *_args: object())
    start = datetime(2020, 1, 2, 8, 30, tzinfo=ZoneInfo("America/Chicago"))
    monkeypatch.setattr(execution, "iter_bars", lambda *_args, **_kwargs: iter(SimpleNamespace(event_at=start + timedelta(seconds=offset)) for offset in offsets))
    with pytest.raises(IntegrityError, match="duplicate, reversed, or off-grid"):
        audit_registered_source_rows(tmp_path, registration)


def test_source_successor_is_immutable_and_certification_gate_is_separate(tmp_path: Path) -> None:
    sessions = list(dates(1010))
    predecessor_core = {"schema_version": "intraday_research_source_registration/1.2.3", "state": "METADATA_BOUND_ROW_COMPLETENESS_AUDIT_REQUIRED", "row_completeness": {"status": "BLOCKED"}}
    predecessor = {**predecessor_core, "registration_id": sha256_json(predecessor_core)}
    calendar_core = {"common_exchange_session_dates": sessions}
    calendar = {**calendar_core, "calendar_id": sha256_json(calendar_core)}
    certificate_core = {"schema_version": "intraday_research_row_completeness/1.2.3", "status": "PASS_COMPLETE", "source_registration_id": predecessor["registration_id"], "common_exchange_session_dates_sha256": sha256_json(sessions)}
    certificate = {**certificate_core, "certificate_id": sha256_json(certificate_core)}
    calendar_path = Path("calendar.json")
    certificate_path = Path("certificate.json")
    (tmp_path / calendar_path).write_bytes(canonical_bytes(calendar) + b"\n")
    (tmp_path / certificate_path).write_bytes(canonical_bytes(certificate) + b"\n")
    successor = build_certified_source_registration(tmp_path, predecessor, certificate_path, calendar_path)
    assert successor["state"] == "ROW_COMPLETE_CERTIFIED_NOT_ACTIVE"
    assert successor["predecessor_registration_id"] == predecessor["registration_id"]
    assert successor["row_completeness"]["status"] == "COMPLETE"
    bad = {**certificate, "status": "BLOCKED"}
    (tmp_path / certificate_path).write_bytes(canonical_bytes(bad) + b"\n")
    with pytest.raises(IntegrityError, match="cannot certify"):
        build_certified_source_registration(tmp_path, predecessor, certificate_path, calendar_path)


def test_completed_checkpoint_excludes_partial_dates_without_payload_reread_and_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import futures_rebuild.intraday_alpha_execution_v12 as execution

    predecessor_path = ROOT / "state/intraday_research_source_registration_registry/17293a7b67fe665b4126de5a147a2c181b935ec75e26b19944f3fe56950e97f6/registration.json"
    checkpoint_path = Path("state/intraday_research_source_scan_checkpoints/17293a7b67fe665b4126de5a147a2c181b935ec75e26b19944f3fe56950e97f6/checkpoint.json")
    predecessor = json.loads(predecessor_path.read_bytes())
    # Reclassification must use a successor with the exact predecessor scope.
    # The live source inventory is allowed to drift, so rebuilding from ROOT
    # would correctly fail the scope-drift guard before this checkpoint test.
    successor_core = {
        **predecessor,
        "schema_version": "intraday_research_source_registration/1.2.3",
        "state": "METADATA_BOUND_ROW_COMPLETENESS_AUDIT_REQUIRED",
        "requirements": {
            **predecessor["requirements"],
            "incomplete_session_policy": "EXCLUDE_BEFORE_FOLDS_NEVER_ZERO_RETURN",
        },
    }
    successor = {**successor_core, "registration_id": sha256_json({key: value for key, value in successor_core.items() if key != "registration_id"})}
    target = tmp_path / checkpoint_path
    target.parent.mkdir(parents=True)
    original_checkpoint = (ROOT / checkpoint_path).read_bytes()
    target.write_bytes(original_checkpoint)
    calendar_pointer = tmp_path / "configs/active_cash_open_impulse_historical_calendar.json"
    calendar_pointer.parent.mkdir(parents=True)
    calendar_pointer.write_bytes((ROOT / "configs/active_cash_open_impulse_historical_calendar.json").read_bytes())
    monkeypatch.setattr(execution, "iter_bars", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("DBN payload reopened")))
    monkeypatch.setattr(execution, "iter_definitions", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("DBN payload reopened")))

    checkpoint_sha256 = sha256_file(target)
    result = certify_registered_source_checkpoint(tmp_path, predecessor, successor, checkpoint_path, checkpoint_sha256)
    assert result["status"] == "PASS_COMPLETE"
    assert (result["complete_session_count"], result["incomplete_session_count"], result["excluded_date_count"]) == (1819, 126, 931)
    calendar = json.loads((tmp_path / result["calendar_path"]).read_bytes())
    assert calendar["common_exchange_session_dates"] == sorted(set(calendar["common_exchange_session_dates"]))
    assert "2018-01-15" not in calendar["common_exchange_session_dates"]
    assert next(item for item in calendar["excluded_dates"] if item["exchange_session_date"] == "2018-01-15")["reason"] == "INCOMPLETE_REGISTERED_SESSION_BOUNDARY_COVERAGE"
    certificate = json.loads((tmp_path / result["certificate_path"]).read_bytes())
    assert certificate["source_scan_evidence"]["payload_reread"] is False
    assert certificate["dbn_count_scanned"] == 144
    with pytest.raises(IntegrityError, match="bytes changed"):
        certify_registered_source_checkpoint(tmp_path, predecessor, successor, checkpoint_path, "0" * 64)

    drifted_core = {key: value for key, value in successor.items() if key != "registration_id"}
    drifted_core["source_release"] = {**drifted_core["source_release"], "release_id": "drifted"}
    drifted = {**drifted_core, "registration_id": sha256_json(drifted_core)}
    with pytest.raises(IntegrityError, match="scope drifted"):
        certify_registered_source_checkpoint(tmp_path, predecessor, drifted, checkpoint_path, checkpoint_sha256)

    incomplete = json.loads(original_checkpoint)
    incomplete["completed"].pop(next(iter(incomplete["completed"])))
    incomplete_core = {key: value for key, value in incomplete.items() if key != "checkpoint_sha256"}
    incomplete["checkpoint_sha256"] = sha256_json(incomplete_core)
    target.write_bytes(canonical_bytes(incomplete) + b"\n")
    with pytest.raises(IntegrityError, match="checkpoint is invalid"):
        certify_registered_source_checkpoint(tmp_path, predecessor, successor, checkpoint_path, sha256_file(target))

    overlapping = json.loads(original_checkpoint)
    cl_minutes = [pair for pair in predecessor["source_pairs"] if pair["market"] == "CL" and pair["family"] == "ohlcv_1m"]
    first_id, second_id = (sha256_json(pair) for pair in cl_minutes[:2])
    overlap_date = next(iter(overlapping["completed"][first_id]["summary"]["counts"]))
    overlapping["completed"][second_id]["summary"]["counts"][overlap_date] = overlapping["completed"][first_id]["summary"]["counts"][overlap_date]
    overlapping["completed"][second_id]["summary"]["spans"][overlap_date] = overlapping["completed"][first_id]["summary"]["spans"][overlap_date]
    overlapping_core = {key: value for key, value in overlapping.items() if key != "checkpoint_sha256"}
    overlapping["checkpoint_sha256"] = sha256_json(overlapping_core)
    target.write_bytes(canonical_bytes(overlapping) + b"\n")
    with pytest.raises(IntegrityError, match="overlap an exchange session"):
        certify_registered_source_checkpoint(tmp_path, predecessor, successor, checkpoint_path, sha256_file(target))

    missing_definitions = json.loads(original_checkpoint)
    for pair in predecessor["source_pairs"]:
        if pair["market"] == "CL" and pair["family"] == "definition":
            missing_definitions["completed"][sha256_json(pair)]["summary"]["definition_count"] = 0
    missing_core = {key: value for key, value in missing_definitions.items() if key != "checkpoint_sha256"}
    missing_definitions["checkpoint_sha256"] = sha256_json(missing_core)
    target.write_bytes(canonical_bytes(missing_definitions) + b"\n")
    with pytest.raises(IntegrityError, match="definition coverage"):
        certify_registered_source_checkpoint(tmp_path, predecessor, successor, checkpoint_path, sha256_file(target))

    malformed = json.loads(original_checkpoint)
    malformed["completed"][first_id]["summary"]["spans"][overlap_date] = ["bad", "bad"]
    malformed_core = {key: value for key, value in malformed.items() if key != "checkpoint_sha256"}
    malformed["checkpoint_sha256"] = sha256_json(malformed_core)
    target.write_bytes(canonical_bytes(malformed) + b"\n")
    with pytest.raises(IntegrityError, match="span is malformed"):
        certify_registered_source_checkpoint(tmp_path, predecessor, successor, checkpoint_path, sha256_file(target))


def test_authority_keeps_activation_separate_from_certification() -> None:
    failure = build_failure_record(ROOT)
    program = build_program(ROOT, failure)
    source = build_source_registration(ROOT, program)
    manifest = synthetic_certification_manifest(program, chunk_size=1)
    authority = build_blocked_authority(ROOT, program, failure, manifest, source)
    assert authority["activation"]["unsatisfied_gates"] == ["SEPARATE_ACTIVATION_APPROVAL"]
    assert "CERTIFIED_ONE_SECOND_SOURCE_BINDING" in authority["certification"]["unsatisfied_gates"]


def test_preparation_rejects_performance_from_another_program() -> None:
    from futures_rebuild.intraday_alpha_pipeline_v12 import prepare_inactive_successor

    with pytest.raises(IntegrityError, match="current program"):
        prepare_inactive_successor(ROOT, {"qualifies": True, "program_id": "stale", "selected_chunk_size": 1, "selected_workers": 1})


def test_exact_accounting_resume_order_and_tamper_fail_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import futures_rebuild.intraday_alpha_pipeline_v12 as pipeline

    program = build_program(ROOT, build_failure_record(ROOT))
    performance = {"qualifies": True, "selected_chunk_size": 5000, "selected_workers": 1, "results": [{"chunk_size": 5000, "workers": 1, "projected_peak_ram_fraction": 0.01}]}
    manifest = synthetic_certification_manifest(program, chunk_size=5000, performance_evidence=performance)
    calls = []

    def fake_chunk(_specification: object, *, campaign_ids: tuple[str, ...], kind: str, resamples: int) -> dict[str, object]:
        calls.append((kind, campaign_ids[0], campaign_ids[-1]))
        terminals = []
        for campaign_id in campaign_ids:
            ordinal = int(campaign_id.rsplit("_", 1)[1])
            terminal = {"campaign_id": campaign_id, "kind": kind, "promoted": kind == "NULL" and ordinal == 0, "known_edge_detected": kind == "KNOWN_EDGE" and ordinal < 4000}
            terminals.append({**terminal, "terminal_id": sha256_json(terminal)})
        core = {"campaign_ids": list(campaign_ids), "terminal_outcomes": terminals}
        return {**core, "chunk_id": sha256_json(core)}

    monkeypatch.setattr(pipeline, "run_synthetic_campaign_chunk", fake_chunk)
    result = run_synthetic_certification(program, manifest=manifest, run_root=tmp_path, run_ordinal=1, exact=True)
    assert len(calls) == 3
    assert result["null"]["false_promotions"] == 1
    assert result["known_edge"]["power"] == 0.8
    assert result["acceptance_passed"] is True
    assert [result["terminal_outcomes"][index]["kind"] for index in (0, 9999, 10000, 14999)] == ["NULL", "NULL", "KNOWN_EDGE", "KNOWN_EDGE"]
    assert run_synthetic_certification(program, manifest=manifest, run_root=tmp_path, run_ordinal=1, exact=True)["merkle_digest"] == result["merkle_digest"]
    assert len(calls) == 3
    chunk_root = tmp_path / result["run_id"] / "chunks"
    first = sorted(chunk_root.glob("*.json"))[0]
    original = first.read_bytes()
    first.write_bytes(original[:-1])
    with pytest.raises(IntegrityError, match="canonical"):
        run_synthetic_certification(program, manifest=manifest, run_root=tmp_path, run_ordinal=1, exact=True)
    first.write_bytes(original)
    first.unlink()
    with pytest.raises(IntegrityError, match="chunk gap"):
        run_synthetic_certification(program, manifest=manifest, run_root=tmp_path, run_ordinal=1, exact=True)


def test_final_outer_audit_cannot_change_selection_or_earlier_oos_predictions() -> None:
    failure = build_failure_record(ROOT)
    program = build_program(ROOT, failure)
    cube = generate_synthetic_minute_cube(program_name="FLOW_CONTINUATION", campaign_ids=("PERTURBATION_0",), kind="NULL")
    first_inputs = materialize_program_inputs(program, "FLOW_CONTINUATION", bars=cube, one_second_bars=(), stress_cost_usd=5.0, extreme_cost_usd=10.0, campaign_ids=("PERTURBATION_0",), resamples=30)
    first_inputs.update({"synthetic": True, "synthetic_kind": "NULL"})
    first = evaluate_program_decision(program, first_inputs)
    factor = 1.0 + np.arange(391, dtype=np.float64) * 0.0001
    changed_open = cube.open.copy(); changed_high = cube.high.copy(); changed_low = cube.low.copy(); changed_close = cube.close.copy()
    for values in (changed_open, changed_high, changed_low, changed_close):
        values[:, -63:, :, :] *= factor
    changed = replace(cube, open=changed_open, high=changed_high, low=changed_low, close=changed_close)
    changed.validate()
    second_inputs = materialize_program_inputs(program, "FLOW_CONTINUATION", bars=changed, one_second_bars=(), stress_cost_usd=5.0, extreme_cost_usd=10.0, campaign_ids=("PERTURBATION_0",), resamples=30)
    second_inputs.update({"synthetic": True, "synthetic_kind": "NULL"})
    second = evaluate_program_decision(program, second_inputs)
    assert first["selected_candidate_indices"] == second["selected_candidate_indices"]
    assert first["split_records"] == second["split_records"]
    assert first["exposure_coefficients"] == second["exposure_coefficients"]
    assert np.array_equal(np.asarray(first["stress_date_market"])[:441], np.asarray(second["stress_date_market"])[:441])
    assert np.array_equal(np.asarray(first["exposure_predictions"])[:441], np.asarray(second["exposure_predictions"])[:441])
    assert np.array_equal(np.asarray(first["residuals"]), np.asarray(first["stress_date_market"]) - np.asarray(first["exposure_predictions"]))
    assert len(first["candidate_ids"]) == 12
    assert np.asarray(first["candidate_stress_date_market"]).shape == (504, 4, 12)
    assert first["negative_control_ids"] == ["LOW_VOLUME", "BLOCK_DERANGED_TIMING"]
    assert len(first["regime_definitions"]) == 8 * 4 * 4
