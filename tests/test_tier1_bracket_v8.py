from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild.boundary import OperationClassification, OperationReceipt, RepoBoundary
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_bracket_v4 import (
    BracketFill,
    DirectionOutcomes,
    ExpectedCheckpoint,
    FEATURE_NAMES,
    FrozenPrediction,
    MarketSpec,
)
from futures_rebuild.tier1_bracket_v5 import (
    CensusCheckpoint,
    CoverageEvidence,
    CrossfitEvidenceBundleV5,
    EvidenceArtifactsV5,
    MaterializedRowV5,
    OpportunityRecordV5,
    build_v5_folds_from_census,
)
from futures_rebuild.tier1_bracket_v6 import V6PipelineResult
from futures_rebuild.tier1_bracket_v8 import (
    V7_EVENT,
    V7_REGISTRY,
    V7_TRIAL_ID,
    FrozenPredictionV8,
    authorized_source_streams_v8,
    build_evidence_manifest_v8,
    derive_v8_decision,
    evaluate_required_baseline_coverage_v8,
    fit_predict_v8,
    load_v8_contract,
    persist_evidence_bundle_v8,
    plan_strategy_v8,
    prepare_v7_retirement_v8,
    prepare_v8_registration,
)


ROOT = Path(__file__).resolve().parents[1]
D = Decimal


def _prediction(index: int, *, neutral: bool = False) -> FrozenPredictionV8:
    market = ("ES", "CL", "ZN", "6E")[(index // 30) % 4]
    year = 2020 + ((index // 10) % 3)
    return FrozenPredictionV8(
        f"p{index}", market, year, f"{year}-01-{index % 10 + 2:02d}",
        ("08:30", "10:30", "13:30")[index % 3], index % 8,
        0.4, -0.1, "long", 0.4,
        None if neutral else "long", None if neutral else 0.1, 0.01,
    )


def _coverage_predictions(*, neutral_indices: set[int]) -> tuple[FrozenPredictionV8, ...]:
    output: list[FrozenPredictionV8] = []
    index = 0
    for market in ("ES", "CL", "ZN", "6E"):
        for year in (2020, 2021, 2022):
            for slot in range(10):
                output.append(FrozenPredictionV8(
                    f"{market}-{year}-{slot}", market, year,
                    f"{year}-01-{slot + 2:02d}",
                    ("08:30", "10:30", "13:30")[slot % 3], slot % 8,
                    0.4, -0.1, "long", 0.4,
                    None if index in neutral_indices else "long",
                    None if index in neutral_indices else 0.1, 0.01,
                ))
                index += 1
    return tuple(output)


def _fit_fixture() -> tuple[list[MaterializedRowV5], tuple[CensusCheckpoint, ...]]:
    sessions = (
        "2018-01-02", "2019-01-02",
        "2020-01-02", "2020-02-03", "2020-03-02",
        "2021-01-04", "2021-02-01", "2021-03-01",
        "2022-01-03", "2022-02-01",
    )
    census: list[CensusCheckpoint] = []
    rows: list[MaterializedRowV5] = []
    for session_index, session in enumerate(sessions):
        year = int(session[:4])
        predict = year >= 2020
        for market_index, market in enumerate(("ES", "CL", "ZN", "6E")):
            for checkpoint_index, checkpoint in enumerate(("08:30", "10:30", "13:30")):
                decision = (session_index + 1) * 10**15 + checkpoint_index * 10**13
                opportunity_id = f"{session}/{market}/{checkpoint}"
                expected = ExpectedCheckpoint(
                    opportunity_id, market, year, session, checkpoint, decision,
                )
                census.append(CensusCheckpoint(expected, True, "c" * 64))
                ledger = OpportunityRecordV5(
                    opportunity_id, market, session, checkpoint, decision,
                    "PREDICTION_PRODUCED" if predict else "TRAINING_OR_PREDICTION_INELIGIBLE",
                    predict, decision - 120_000_000_000, decision - 60_000_000_000,
                    outcome_coverage="COMPLETE",
                )
                base = 1.0 + session_index * 0.01 + market_index * 0.001 + checkpoint_index * 0.0001
                features = {
                    name: base * (feature_index + 1)
                    for feature_index, name in enumerate(FEATURE_NAMES)
                }
                fill = BracketFill(
                    decision + 1, decision + 2, D("100"), D("101"), D("98"),
                    D("104"), "TARGET", D("100"), D("10"), D("90"), D("250"),
                )
                outcomes = None if market == "ES" and checkpoint == "13:30" else {
                    scenario: DirectionOutcomes(fill, fill)
                    for scenario in ("base", "stress", "extreme")
                }
                rows.append(MaterializedRowV5(
                    expected, ledger, features, D("1"), "a" * 64, outcomes, (),
                    MarketSpec(D("1"), D("100"), D("100")),
                ))
    return rows, tuple(census)


def test_v8_contract_is_narrow_and_registration_binds_runner() -> None:
    inherited, delta = load_v8_contract(root=ROOT)
    retirement = prepare_v7_retirement_v8(root=ROOT)
    registration = prepare_v8_registration(root=ROOT)
    assert inherited["risk"]["continuous_drawdown_threshold_usd"] == "1500"
    rule = delta["required_baseline_coverage_successor"]
    assert rule["minimum_overall_eligible_rate"] == "0.95"
    assert rule["minimum_each_market_year_eligible_rate"] == "0.90"
    assert registration.canonical_payload["change_scope"] == (
        "REQUIRED_BASELINE_ABSTENTION_AND_COVERAGE_GATE_ONLY"
    )
    assert "tests/conftest.py" in registration.canonical_payload["bindings"]
    assert registration.canonical_payload["v7_retirement_record_id"] == retirement.record_id
    assert len(registration.canonical_payload["source_bindings"]) == 20


def test_v8_fit_preserves_candidate_predictions_and_abstains_empty_baseline_cell() -> None:
    rows, census = _fit_fixture()
    model = fit_predict_v8(rows=rows, folds=build_v5_folds_from_census(census))
    es_late = [
        item for item in model.predictions
        if item.market == "ES" and item.checkpoint == "13:30"
    ]
    assert es_late
    assert all(item.fold_local_direction is None for item in es_late)
    assert all(item.fold_local_score is None for item in es_late)
    assert all(item.selected_direction in {"long", "short", "neutral"} for item in es_late)
    cells = [
        model_row["fold_local_unconditional"]["13:30"]
        for model_row in model.canonical_model_payload["models"]
        if model_row["market"] == "ES"
    ]
    assert all(cell == {
        "status": "NO_TRAINING_OUTCOME_ABSTAIN", "direction": None,
        "score": None, "training_rows": 0,
    } for cell in cells)


def test_v8_baseline_abstention_is_explicit_and_never_manufactures_a_trade() -> None:
    prediction = _prediction(0, neutral=True)
    plan = plan_strategy_v8(
        strategy="fold_local_unconditional_return_by_market_session",
        predictions=(prediction,), rows=(), scenario="stress",
    )
    assert plan.trades == ()
    assert plan.preliminary_terminals == {
        prediction.opportunity_id: "BASELINE_TRAINING_COVERAGE_ABSTENTION"
    }


def test_v8_baseline_coverage_gate_enforces_both_locked_thresholds() -> None:
    assert evaluate_required_baseline_coverage_v8(
        _coverage_predictions(neutral_indices={0})
    )["status"] == "PASS"
    failed = evaluate_required_baseline_coverage_v8(
        _coverage_predictions(neutral_indices={0, 1})
    )
    assert failed["status"] == "INCONCLUSIVE_DATA_OR_COVERAGE"
    assert failed["overall_eligible_rate"] > 0.95
    assert failed["market_year_eligible_rates"]["ES/2020"] == 0.8


def test_v8_decision_cannot_promote_when_required_baseline_coverage_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "futures_rebuild.tier1_bracket_v8.v5.derive_v5_decision",
        lambda **_: {
            "schema_version": "old", "classification": "PASS_HISTORICAL_SCREEN",
            "decision_id": "d" * 64,
        },
    )
    coverage = CoverageEvidence(1, 1, 1, 1, 1, {}, {})
    crossfit = CrossfitEvidenceBundleV5((), (), {}, {})
    decision = derive_v8_decision(
        evaluation={}, evaluation_sessions=(), coverage=coverage,
        baseline_coverage={"status": "INCONCLUSIVE_DATA_OR_COVERAGE", "passed": False},
        crossfit=crossfit, seed=1,
    )
    assert decision["classification"] == "INCONCLUSIVE_DATA_OR_COVERAGE"
    assert decision["schema_version"] == "tier1_bracket_successor_v8_decision/1.0.0"


def test_v8_lifecycle_is_valid_before_and_after_create_only_publication() -> None:
    retirement = prepare_v7_retirement_v8(root=ROOT)
    registration = prepare_v8_registration(root=ROOT)
    retirement_path = (
        ROOT / "state/trial_registry/tier1_bracket_v7_retirement" / f"{retirement.record_id}.json"
    )
    registration_path = (
        ROOT / "state/trial_registry/tier1_bracket_successor_v8" / f"{registration.trial_id}.json"
    )
    assert retirement_path.exists() == registration_path.exists()
    if retirement_path.exists():
        assert '"state":"RETIRED_INVALID_AFTER_SOURCE_ACCESS_BEFORE_PREDICTIONS"' in retirement_path.read_text()
        assert '"state":"REGISTERED_BEFORE_SOURCE_ROW_ACCESS"' in registration_path.read_text()


def test_v7_registered_and_execution_bytes_remain_preserved() -> None:
    retirement = prepare_v7_retirement_v8(root=ROOT)
    preserved = retirement.canonical_payload["preserved_v7_sha256"]
    assert V7_REGISTRY.as_posix() in preserved
    assert V7_EVENT.as_posix() in preserved
    assert retirement.canonical_payload["trial_id"] == V7_TRIAL_ID
    assert (
        "state/authorization_uses/98e688ef10d8e389b9abe49ebe01e1b6daa916bb348d5a435673efc2312ab552.json"
        in preserved
    )


def test_v8_rejects_2025_before_registry_or_file_open(tmp_path: Path) -> None:
    boundary = RepoBoundary(tmp_path)
    receipt = OperationReceipt.issue_local(
        boundary, operation="synthetic",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    with pytest.raises(UnauthorizedOperation, match="2025"):
        authorized_source_streams_v8(
            root=tmp_path, boundary=boundary, receipt=receipt,
            trial_id="f" * 64,
            source_paths={("ES", 2025): tmp_path / "must-not-open.parquet"},
            output_root=tmp_path / "output",
        )


def test_v8_evidence_manifest_is_versioned_complete_and_create_only(tmp_path: Path) -> None:
    evidence = EvidenceArtifactsV5(
        model={"coefficient": D("1.25")},
        predictions=({"opportunity_id": "p", "score": D("0.3")},),
        opportunity_ledger=({"opportunity_id": "p", "terminal": "ADMITTED_TRADE"},),
        fills=({"opportunity_id": "p", "net": D("2.50")},),
        continuous_equity_marks=({"equity": D("100002.50")},),
        segmented_metrics={"folds": []},
        inference={"required_baseline_coverage": {"status": "PASS"}},
        decision={"classification": "FAIL_NO_EDGE"},
        runtime_receipt={"runtime_receipt_id": "a" * 64},
    )
    result = V6PipelineResult(
        base=SimpleNamespace(evidence=evidence),
        source_integrity_audit={"ES/2020": {"nontradable_rows": 1}},
    )
    trial_id = "f" * 64
    manifest = build_evidence_manifest_v8(trial_id=trial_id, result=result)
    assert manifest["schema_version"] == "tier1_bracket_successor_v8_evidence_manifest/1.0.0"
    assert len(manifest["files"]) == 10
    output = tmp_path / "evidence"
    published = persist_evidence_bundle_v8(
        boundary=RepoBoundary(tmp_path), output_root=output,
        trial_id=trial_id, result=result,
    )
    assert Path(published["manifest_path"]).exists()
    with pytest.raises(IntegrityError, match="create-only"):
        persist_evidence_bundle_v8(
            boundary=RepoBoundary(tmp_path), output_root=output,
            trial_id=trial_id, result=result,
        )
