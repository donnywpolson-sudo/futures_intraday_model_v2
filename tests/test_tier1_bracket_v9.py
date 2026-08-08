from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild.boundary import OperationClassification, OperationReceipt, RepoBoundary
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_bracket_v4 import (
    BracketFill, DirectionOutcomes, ExpectedCheckpoint, FEATURE_NAMES, MarketSpec,
)
from futures_rebuild.tier1_bracket_v5 import (
    CensusCheckpoint, CoverageEvidence, CrossfitEvidenceBundleV5,
    EvidenceArtifactsV5, MaterializedRowV5, OpportunityRecordV5,
    build_v5_folds_from_census, reconcile_v5_opportunity_ledger,
)
from futures_rebuild.tier1_bracket_v6 import V6PipelineResult
from futures_rebuild.tier1_bracket_v9 import (
    CrossfitEvidenceV9, V8_EVENT, V8_REGISTRY, V8_TRIAL_ID,
    apply_model_unavailable_abstentions_v9, authorized_source_streams_v9,
    build_evidence_manifest_v9, derive_v9_decision,
    evaluate_crossfit_model_availability_v9, fit_predict_v9, load_v9_contract,
    persist_evidence_bundle_v9, prepare_v8_retirement_v9, prepare_v9_registration,
)


ROOT = Path(__file__).resolve().parents[1]
D = Decimal


def _fit_fixture(
    *, no_training_market: str | None = None,
    empty_test: tuple[str, str] | None = None,
) -> tuple[list[MaterializedRowV5], tuple[CensusCheckpoint, ...]]:
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
        default_predict = year >= 2020
        for market_index, market in enumerate(("ES", "CL", "ZN", "6E")):
            unavailable_test = empty_test == (market, session)
            for checkpoint_index, checkpoint in enumerate(("08:30", "10:30", "13:30")):
                decision = (session_index + 1) * 10**15 + checkpoint_index * 10**13
                opportunity_id = f"{session}/{market}/{checkpoint}"
                expected = ExpectedCheckpoint(
                    opportunity_id, market, year, session, checkpoint, decision,
                )
                census.append(CensusCheckpoint(expected, True, "c" * 64))
                predict = default_predict and not unavailable_test
                ledger = OpportunityRecordV5(
                    opportunity_id, market, session, checkpoint, decision,
                    "PREDICTION_PRODUCED" if predict else "TRAINING_OR_PREDICTION_INELIGIBLE",
                    predict, decision - 120_000_000_000, decision - 60_000_000_000,
                    outcome_coverage="COMPLETE",
                )
                base = 1.0 + session_index * 0.01 + market_index * 0.001 + checkpoint_index * 0.0001
                features = None if unavailable_test else {
                    name: base * (feature_index + 1)
                    for feature_index, name in enumerate(FEATURE_NAMES)
                }
                fill = BracketFill(
                    decision + 1, decision + 2, D("100"), D("101"), D("98"),
                    D("104"), "TARGET", D("100"), D("10"), D("90"), D("250"),
                )
                outcomes = None if market == no_training_market else {
                    scenario: DirectionOutcomes(fill, fill)
                    for scenario in ("base", "stress", "extreme")
                }
                rows.append(MaterializedRowV5(
                    expected, ledger, features, D("1"), "a" * 64, outcomes, (),
                    MarketSpec(D("1"), D("100"), D("100")),
                ))
    return rows, tuple(census)


def _availability_rows(per_market: int = 10) -> tuple[MaterializedRowV5, ...]:
    output: list[MaterializedRowV5] = []
    for market in ("ES", "CL", "ZN", "6E"):
        for index in range(per_market):
            opportunity_id = f"{market}-{index}"
            expected = ExpectedCheckpoint(
                opportunity_id, market, 2019, "2019-01-02", "08:30", index,
            )
            ledger = OpportunityRecordV5(
                opportunity_id, market, "2019-01-02", "08:30", index,
                "PREDICTION_PRODUCED", True, index - 2, index - 1,
            )
            output.append(MaterializedRowV5(
                expected, ledger, {name: 1.0 for name in FEATURE_NAMES}, D("1"),
                "a" * 64, None,
            ))
    return tuple(output)


def test_v9_contract_is_narrow_and_registration_binds_execution_failure() -> None:
    inherited, delta = load_v9_contract(root=ROOT)
    retirement = prepare_v8_retirement_v9(root=ROOT)
    registration = prepare_v9_registration(root=ROOT)
    assert inherited["risk"]["continuous_drawdown_threshold_usd"] == "1500"
    rule = delta["market_fold_coverage_successor"]
    assert rule["training_fallback_pooling_or_borrowing"] == "FORBIDDEN"
    assert rule["nested_crossfit_minimum_overall_statistic_eligibility"] == "0.95"
    assert rule["nested_crossfit_minimum_overall_model_availability"] == "0.99"
    assert registration.canonical_payload["change_scope"] == (
        "MARKET_FOLD_TRAINING_AND_TEST_COVERAGE_REPRESENTATION_ONLY"
    )
    assert "tests/conftest.py" in registration.canonical_payload["bindings"]
    assert registration.canonical_payload["v8_retirement_record_id"] == retirement.record_id
    assert len(registration.canonical_payload["source_bindings"]) == 20


def test_v9_empty_test_market_fold_is_recorded_without_fit_or_exception() -> None:
    rows, census = _fit_fixture(empty_test=("ES", "2020-01-02"))
    model = fit_predict_v9(rows=rows, folds=build_v5_folds_from_census(census))
    cells = [
        item for item in model.canonical_model_payload["models"]
        if item["market"] == "ES" and item["status"] == "NO_TEST_PREDICTION_ROWS_NO_FIT"
    ]
    assert cells
    assert all(item["testing_rows"] == 0 for item in cells)
    assert not model.model_unavailable_opportunity_ids


def test_v9_empty_training_market_fold_becomes_explicit_prediction_abstentions() -> None:
    rows, census = _fit_fixture(no_training_market="ES")
    model = fit_predict_v9(rows=rows, folds=build_v5_folds_from_census(census))
    assert model.model_unavailable_opportunity_ids
    assert all(item.startswith(("2020", "2021", "2022")) for item in model.model_unavailable_opportunity_ids)
    adjusted = apply_model_unavailable_abstentions_v9(
        rows=rows, opportunity_ids=model.model_unavailable_opportunity_ids,
    )
    affected = [
        row for row in adjusted
        if row.expected.opportunity_id in set(model.model_unavailable_opportunity_ids)
    ]
    assert affected
    assert all(not row.ledger.prediction_produced for row in affected)
    assert {row.ledger.terminal_disposition for row in affected} == {
        "MODEL_TRAINING_COVERAGE_ABSTENTION"
    }
    assert not any(item.market == "ES" for item in model.predictions)
    reconciled = reconcile_v5_opportunity_ledger(
        expected_ids=[row.expected.opportunity_id for row in adjusted],
        records=[row.ledger for row in adjusted],
    )
    assert reconciled["expected"] == len(adjusted)


def test_v9_crossfit_model_availability_gate_is_hard() -> None:
    rows = _availability_rows()
    assert evaluate_crossfit_model_availability_v9(
        rows=rows, unavailable_ids=(),
    )["status"] == "PASS"
    failed = evaluate_crossfit_model_availability_v9(
        rows=rows, unavailable_ids=("ES-0",),
    )
    assert failed["status"] == "INCONCLUSIVE_DATA_OR_POWER"
    assert failed["overall_statistic_eligibility_rate"] == 1.0
    assert failed["overall_model_availability_rate"] == 0.975
    assert failed["market_model_availability_rates"]["ES"] == 0.9

    ineligible = list(rows)
    ineligible[0] = replace(
        ineligible[0], ledger=replace(
            ineligible[0].ledger,
            terminal_disposition="TRAINING_OR_PREDICTION_INELIGIBLE",
            prediction_produced=False,
        ),
    )
    denominator = evaluate_crossfit_model_availability_v9(
        rows=tuple(ineligible), unavailable_ids=(),
    )
    assert denominator["calendar_open_expected_opportunities"] == 40
    assert denominator["statistic_eligible_opportunities"] == 39
    assert denominator["overall_statistic_eligibility_rate"] == 0.975


def test_v9_decision_cannot_promote_when_crossfit_model_coverage_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "futures_rebuild.tier1_bracket_v9.v8.derive_v8_decision",
        lambda **_: {
            "schema_version": "old", "classification": "PASS_HISTORICAL_SCREEN",
            "decision_id": "d" * 64,
        },
    )
    decision = derive_v9_decision(
        evaluation={}, evaluation_sessions=(),
        coverage=CoverageEvidence(1, 1, 1, 1, 1, {}, {}),
        baseline_coverage={"status": "PASS", "passed": True},
        crossfit=CrossfitEvidenceV9(
            CrossfitEvidenceBundleV5((), (), {}, {}),
            {"status": "INCONCLUSIVE_DATA_OR_POWER", "passed": False},
        ), seed=1,
    )
    assert decision["classification"] == "INCONCLUSIVE_DATA_OR_POWER"
    assert decision["schema_version"] == "tier1_bracket_successor_v9_decision/1.0.0"


def test_v9_lifecycle_is_valid_before_and_after_create_only_publication() -> None:
    retirement = prepare_v8_retirement_v9(root=ROOT)
    registration = prepare_v9_registration(root=ROOT)
    retirement_path = (
        ROOT / "state/trial_registry/tier1_bracket_v8_retirement" / f"{retirement.record_id}.json"
    )
    registration_path = (
        ROOT / "state/trial_registry/tier1_bracket_successor_v9" / f"{registration.trial_id}.json"
    )
    assert retirement_path.exists() == registration_path.exists()
    if retirement_path.exists():
        assert '"state":"RETIRED_INVALID_AFTER_SOURCE_ACCESS_BEFORE_PREDICTIONS"' in retirement_path.read_text()
        assert '"state":"REGISTERED_BEFORE_SOURCE_ROW_ACCESS"' in registration_path.read_text()


def test_v8_registered_and_execution_bytes_remain_preserved() -> None:
    retirement = prepare_v8_retirement_v9(root=ROOT)
    preserved = retirement.canonical_payload["preserved_v8_sha256"]
    assert V8_REGISTRY.as_posix() in preserved
    assert V8_EVENT.as_posix() in preserved
    assert retirement.canonical_payload["trial_id"] == V8_TRIAL_ID
    assert (
        "state/authorization_uses/8ddd5886bc02b93873a3b34811dced29f03123f6cf60927f775df66698e0c82f.json"
        in preserved
    )


def test_v9_rejects_2025_before_registry_or_file_open(tmp_path: Path) -> None:
    boundary = RepoBoundary(tmp_path)
    receipt = OperationReceipt.issue_local(
        boundary, operation="synthetic",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    with pytest.raises(UnauthorizedOperation, match="2025"):
        authorized_source_streams_v9(
            root=tmp_path, boundary=boundary, receipt=receipt,
            trial_id="f" * 64,
            source_paths={("ES", 2025): tmp_path / "must-not-open.parquet"},
            output_root=tmp_path / "output",
        )


def test_v9_evidence_manifest_is_versioned_complete_and_create_only(tmp_path: Path) -> None:
    evidence = EvidenceArtifactsV5(
        model={"coefficient": D("1.25")},
        predictions=({"opportunity_id": "p", "score": D("0.3")},),
        opportunity_ledger=({"opportunity_id": "p", "terminal": "ADMITTED_TRADE"},),
        fills=({"opportunity_id": "p", "net": D("2.50")},),
        continuous_equity_marks=({"equity": D("100002.50")},),
        segmented_metrics={"folds": []},
        inference={"nested_crossfit_model_availability": {"status": "PASS"}},
        decision={"classification": "FAIL_NO_EDGE"},
        runtime_receipt={"runtime_receipt_id": "a" * 64},
    )
    result = V6PipelineResult(
        base=SimpleNamespace(evidence=evidence),
        source_integrity_audit={"ES/2020": {"nontradable_rows": 1}},
    )
    trial_id = "f" * 64
    manifest = build_evidence_manifest_v9(trial_id=trial_id, result=result)
    assert manifest["schema_version"] == "tier1_bracket_successor_v9_evidence_manifest/1.0.0"
    assert len(manifest["files"]) == 10
    output = tmp_path / "evidence"
    published = persist_evidence_bundle_v9(
        boundary=RepoBoundary(tmp_path), output_root=output,
        trial_id=trial_id, result=result,
    )
    assert Path(published["manifest_path"]).exists()
    with pytest.raises(IntegrityError, match="create-only"):
        persist_evidence_bundle_v9(
            boundary=RepoBoundary(tmp_path), output_root=output,
            trial_id=trial_id, result=result,
        )
