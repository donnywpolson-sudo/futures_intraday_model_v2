import json
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_phase8_real_adapter import (
    _approved_real_read_for_codex_task,
    convert_pinned_source_bars_to_execution_rows,
    derive_fold_local_directions,
    schedule_one_contract_execution_rows,
    normalize_phase8_execution_rows,
    pin_phase8_prediction_release,
    read_pinned_phase8_rows,
)
from futures_rebuild.tier1_phase8_evaluation_config import load_tier1_phase8_evaluation_config
from futures_rebuild.tier1_phase8_evaluator import Phase8SyntheticTrade, evaluate_tier1_phase8_synthetic
from futures_rebuild.tier1_phase8_runner import (
    build_phase8_evaluation_reports,
)


TRIAL = "a" * 64
RELEASE = "b" * 64
ROOT = Path(__file__).parents[1]


def _pinned_prediction(tmp_path: Path) -> object:
    payload = tmp_path / "data" / "predictions" / "frozen" / "predictions.parquet"
    payload.parent.mkdir(parents=True)
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pylist([{
        "market": "ES", "year": 2018, "exchange_session_date": "2018-01-02",
        "actual_identity_hash": "c" * 64, "decision_at_ns": 60, "outer_fold": 0,
        "upstream_source_row_sha256": "d" * 64, "prediction": 0.1,
    }]), payload)
    pairs = []
    for index in range(20):
        feature_id = f"{index:064x}"
        outcome_id = f"{index + 20:064x}"
        feature_logical = Path("data") / "features" / str(index) / "features.parquet"
        outcome_logical = Path("data") / "outcomes" / str(index) / "outcomes.parquet"
        feature = tmp_path / feature_logical.parent / feature_id / feature_logical.name
        outcome = tmp_path / outcome_logical.parent / outcome_id / outcome_logical.name
        feature.parent.mkdir(parents=True)
        outcome.parent.mkdir(parents=True)
        row = {"status": "FEATURE_READY", "actual_identity_hash": "c" * 64, "decision_at_ns": 60, "upstream_source_row_sha256": "d" * 64}
        pq.write_table(pa.Table.from_pylist([row]), feature)
        pq.write_table(pa.Table.from_pylist([{**row, "status": "MATURED"}]), outcome)
        for family, release_id, source, filename in (("features", feature_id, feature, "features.parquet"), ("outcomes", outcome_id, outcome, "outcomes.parquet")):
            path = tmp_path / "manifests" / "data_releases" / family / f"{release_id}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            logical = feature_logical if family == "features" else outcome_logical
            path.write_text(json.dumps({"files": [{"logical_path": logical.as_posix(), "sha256": sha256_file(source)}]}), encoding="utf-8")
        market, year = ("ES", 2018 + index)
        source_bar = tmp_path / "data" / "active" / "causally_gated_normalized" / market / str(year) / f"{year}.parquet"
        source_bar.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist([{"event_at_ns": 0, "open_nano": 100, "actual_identity_hash": "c" * 64}]), source_bar)
        source_hash = sha256_file(source_bar)
        sidecar = source_bar.with_suffix(".parquet.manifest.json")
        sidecar.write_text(json.dumps({"entry_binding": {"market": market, "year": year, "parquet_path": source_bar.relative_to(tmp_path).as_posix(), "parquet_sha256": source_hash}}), encoding="utf-8")
        pairs.append({"market": market, "year": year, "feature_release_id": feature_id, "outcome_release_id": outcome_id, "source_parquet_sha256": source_hash})
    manifest = {
        "schema_version": "tier1_phase6_prediction_release/1.0.0",
        "release_id": RELEASE,
        "trial_id": TRIAL,
        "prediction_only": True,
        "input_pairs": pairs,
        "payload": payload.relative_to(tmp_path).as_posix(),
        "payload_sha256": sha256_file(payload),
    }
    path = tmp_path / "manifests" / "data_releases" / "predictions" / f"{RELEASE}.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return pin_phase8_prediction_release(root=tmp_path, prediction_release_id=RELEASE, trial_id=TRIAL)


def test_adapter_pins_hashes_but_rejects_direct_real_row_access(tmp_path: Path) -> None:
    pinned = _pinned_prediction(tmp_path)

    with pytest.raises(UnauthorizedOperation, match="Codex confirmation required"):
        read_pinned_phase8_rows(pinned=pinned)

    with pytest.raises(UnauthorizedOperation, match="is retired"):
        _approved_real_read_for_codex_task()


def test_adapter_rejects_changed_prediction_payload(tmp_path: Path) -> None:
    pinned = _pinned_prediction(tmp_path)
    pinned.prediction_payload.write_bytes(b"changed")

    with pytest.raises(IntegrityError, match="hash"):
        pin_phase8_prediction_release(root=tmp_path, prediction_release_id=RELEASE, trial_id=TRIAL)


def test_adapter_rejects_missing_or_escaped_pair_payload(tmp_path: Path) -> None:
    _pinned_prediction(tmp_path)
    feature_id = f"{0:064x}"
    payload = tmp_path / "data" / "features" / "0" / feature_id / "features.parquet"
    payload.unlink()

    with pytest.raises(IntegrityError, match="payload hash"):
        pin_phase8_prediction_release(root=tmp_path, prediction_release_id=RELEASE, trial_id=TRIAL)

    escaped_root = tmp_path / "escaped"
    _pinned_prediction(escaped_root)
    manifest = escaped_root / "manifests" / "data_releases" / "features" / f"{feature_id}.json"
    content = json.loads(manifest.read_text(encoding="utf-8"))
    content["files"][0]["logical_path"] = "../outside/features.parquet"
    manifest.write_text(json.dumps(content), encoding="utf-8")

    with pytest.raises(IntegrityError, match="logical payload path"):
        pin_phase8_prediction_release(root=escaped_root, prediction_release_id=RELEASE, trial_id=TRIAL)


def test_normalization_excludes_rolls_and_checks_tick_math() -> None:
    prediction = {
        "upstream_source_row_sha256": "d" * 64,
        "actual_identity_hash": "c" * 64,
        "decision_at_ns": 60,
        "prediction": Decimal("1"),
    }
    row = {
        "upstream_source_row_sha256": "d" * 64,
        "actual_identity_hash": "c" * 64,
        "decision_at_ns": 60,
        "entry_actual_identity_hash": "c" * 64,
        "exit_actual_identity_hash": "c" * 64,
        "tick_size": "0.25", "tick_value_usd": "12.50", "point_value": "50",
        "entry_price": "100", "exit_price": "101", "quantity": 1,
        "risk_at_entry_usd": "125", "market": "ES", "market_year": 2018, "session": 1,
        "baseline_gross_pnl_usd": {
            "fold_local_unconditional_return_by_market_session": "1",
            "previous_bar_sign_momentum": "2", "previous_bar_sign_reversal": "3",
            "risk_matched_always_long_intraday": "4", "equal_risk_version_of_candidate_signal": "5",
        },
    }
    trades = normalize_phase8_execution_rows(prediction_rows=(prediction,), execution_rows=(row,))
    assert trades[0].gross_pnl_usd == Decimal("50")

    roll = {**row, "exit_actual_identity_hash": "e" * 64}
    with pytest.raises(IntegrityError, match="no non-roll"):
        normalize_phase8_execution_rows(prediction_rows=(prediction,), execution_rows=(roll,))

    with pytest.raises(IntegrityError, match="tick math"):
        normalize_phase8_execution_rows(
            prediction_rows=(prediction,), execution_rows=({**row, "tick_value_usd": "11"},)
        )


def test_source_bar_conversion_builds_pure_execution_rows_and_excludes_rolls() -> None:
    source_hash = "d" * 64
    identity = "c" * 64
    prediction = {
        "upstream_source_row_sha256": source_hash, "actual_identity_hash": identity,
        "decision_at_ns": 60, "prediction": Decimal("1"), "market": "ES", "year": 2018,
        "outer_fold": 1, "exchange_session_date": "2018-01-02",
    }
    feature = {
        "upstream_source_row_sha256": source_hash, "actual_identity_hash": identity,
        "decision_at_ns": 60, "planned_entry_at_ns": 120, "status": "FEATURE_READY",
        "bar_return": Decimal("0.01"),
    }
    outcome = {
        "upstream_source_row_sha256": source_hash, "actual_identity_hash": identity,
        "decision_at_ns": 60, "entry_at_ns": 120, "label_unlock_at_ns": 360,
        "status": "MATURED",
    }
    economics = {"tick_size": "0.25", "tick_value": "12.50", "point_value": "50", "currency": "USD"}
    source = {"upstream_source_row_sha256": source_hash, "market": "ES", "event_at_ns": 60, "actual_identity_hash": identity, **economics}
    entry = {"upstream_source_row_sha256": "e" * 64, "market": "ES", "event_at_ns": 120, "actual_identity_hash": identity, "disposition": "ELIGIBLE", "open_nano": 100_000_000_000, **economics}
    exit = {"upstream_source_row_sha256": "f" * 64, "market": "ES", "event_at_ns": 360, "actual_identity_hash": identity, "disposition": "ELIGIBLE", "open_nano": 101_000_000_000, **economics}

    converted = convert_pinned_source_bars_to_execution_rows(
        prediction_rows=(prediction,), feature_rows=(feature,), outcome_rows=(outcome,),
        source_bar_rows=(source, entry, exit), fold_local_directions={("ES", 1, 0): 1},
    )
    assert converted.excluded_roll_count == 0
    trades = normalize_phase8_execution_rows(prediction_rows=(prediction,), execution_rows=converted.execution_rows)
    assert trades[0].gross_pnl_usd == Decimal("50.00")
    assert trades[0].baseline_gross_pnl_usd["equal_risk_version_of_candidate_signal"] == Decimal("50.00")

    rolled_exit = {**exit, "actual_identity_hash": "g" * 64}
    rolled = convert_pinned_source_bars_to_execution_rows(
        prediction_rows=(prediction,), feature_rows=(feature,), outcome_rows=(outcome,),
        source_bar_rows=(source, entry, rolled_exit), fold_local_directions={("ES", 1, 0): 1},
    )
    assert rolled.execution_rows == ()
    assert rolled.excluded_roll_count == 1

    with pytest.raises(IntegrityError, match="fold-local"):
        convert_pinned_source_bars_to_execution_rows(
            prediction_rows=(prediction,), feature_rows=(feature,), outcome_rows=(outcome,),
            source_bar_rows=(source, entry, exit), fold_local_directions={},
        )


def test_fold_local_directions_use_only_declared_training_rows() -> None:
    prediction = {"market": "ES", "outer_fold": 0, "decision_at_ns": 60 * 1_000_000_000}
    training = {
        "market": "ES", "status": "MATURED", "exchange_session_date": "2018-01-02",
        "decision_at_ns": 60 * 1_000_000_000, "price_return": "0.01",
    }
    future_test = {**training, "exchange_session_date": "2018-02-01", "price_return": "-1.00"}
    directions = derive_fold_local_directions(
        prediction_rows=(prediction,), outcome_rows=(training, future_test),
        outer_folds=({"outer_fit_session_range": ["2018-01-01", "2018-01-31"]},),
    )
    assert directions.directions == {("ES", 0, 1): 1}
    assert directions.fallback_keys == frozenset()


def test_fold_local_direction_uses_documented_market_training_fallback() -> None:
    prediction = {"market": "ES", "outer_fold": 0, "decision_at_ns": 120 * 1_000_000_000}
    training = {
        "market": "ES", "status": "MATURED", "exchange_session_date": "2018-01-02",
        "decision_at_ns": 60 * 1_000_000_000, "price_return": "0.01",
    }
    directions = derive_fold_local_directions(
        prediction_rows=(prediction,), outcome_rows=(training,),
        outer_folds=({"outer_fit_session_range": ["2018-01-01", "2018-01-31"]},),
    )
    assert directions.directions == {("ES", 0, 2): 1}
    assert directions.fallback_keys == frozenset({("ES", 0, 2)})


def test_one_contract_scheduler_uses_score_ties_and_blocks_overlaps() -> None:
    predictions = (
        {"upstream_source_row_sha256": "a" * 64, "prediction": Decimal("0.02")},
        {"upstream_source_row_sha256": "b" * 64, "prediction": Decimal("0.03")},
        {"upstream_source_row_sha256": "c" * 64, "prediction": Decimal("0.04")},
    )
    rows = (
        {"upstream_source_row_sha256": "a" * 64, "market": "ES", "outer_fold": 0, "entry_at_ns": 10, "exit_at_ns": 20},
        {"upstream_source_row_sha256": "b" * 64, "market": "CL", "outer_fold": 0, "entry_at_ns": 10, "exit_at_ns": 20},
        {"upstream_source_row_sha256": "c" * 64, "market": "ZN", "outer_fold": 0, "entry_at_ns": 15, "exit_at_ns": 25},
    )
    schedule = schedule_one_contract_execution_rows(
        prediction_rows=predictions, execution_rows=rows,
        training_volatilities={("ES", 0): Decimal("0.01"), ("CL", 0): Decimal("0.01"), ("ZN", 0): Decimal("0.01")},
    )
    assert [row["market"] for row in schedule.execution_rows] == ["CL"]
    assert schedule.simultaneous_selection_abstentions == 1
    assert schedule.position_overlap_abstentions == 1


def test_report_payloads_are_provisional_and_create_once(tmp_path: Path) -> None:
    pinned = _pinned_prediction(tmp_path / "inputs")
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    baselines = {
        "fold_local_unconditional_return_by_market_session": Decimal("1"),
        "previous_bar_sign_momentum": Decimal("2"),
        "previous_bar_sign_reversal": Decimal("3"),
        "risk_matched_always_long_intraday": Decimal("4"),
        "equal_risk_version_of_candidate_signal": Decimal("100"),
    }
    result = evaluate_tier1_phase8_synthetic(
        trades=tuple(
            Phase8SyntheticTrade(market, year, session, 1, Decimal("125"), Decimal("100"), Decimal("1"), baselines)
            for session, (market, year) in enumerate(
                ((market, year) for market in ("ES", "CL", "ZN", "6E") for year in range(2018, 2023)), start=1
            )
        ),
        evaluation_config=config,
    )
    reports = build_phase8_evaluation_reports(pinned=pinned, evaluation=result, preparation_id="e" * 64)
    assert reports.model_selection["result_label"] == "PROVISIONAL_EXECUTION_COSTS"
    assert reports.model_selection["cost_scenarios"]["base"]["identical_fixed_risk_comparator_matches"] is True
    with pytest.raises(UnauthorizedOperation, match="is retired"):
        _approved_real_read_for_codex_task()
