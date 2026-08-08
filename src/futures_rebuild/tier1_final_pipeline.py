"""Complete synthetic-to-evidence pipeline for the final unversioned trial."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict

from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v9 as v9
from .errors import IntegrityError
from .tier1_bracket_v10_decision_validity import (
    DirectionalOutcomeResolutionV10, evaluate_crossfit_decision_availability_v10,
    evaluate_evaluation_completeness_v10, prepare_crossfit_prediction_rows_v10,
)
from .tier1_bracket_v10_pipeline import CrossfitEvidenceV10, _nested_folds_v10, derive_v10_decision
from .tier1_bracket_v11 import (
    build_strategy_prediction_universes_v11, segmented_account_views_v11,
)
from .tier1_bracket_v11_pipeline import _crossfit_statistic_v11
from .tier1_bracket_v12 import evaluate_required_baseline_coverage_v12
from .tier1_bracket_v12_pipeline import CrossfitEvidenceV12
from .tier1_final_decision_validity import (
    derive_final_decision, evaluate_final_strategies, plan_final_strategy,
)
from .tier1_frozen_trial_pipeline import materialize_reported_bar_streams


def build_final_nested_crossfit(
    *, rows: Sequence[v5.MaterializedRowV5],
    resolutions: Mapping[str, DirectionalOutcomeResolutionV10],
    contract: Mapping[str, object],
) -> CrossfitEvidenceV12:
    folds, sessions = _nested_folds_v10(rows)
    prepared = prepare_crossfit_prediction_rows_v10(
        rows=rows, owner_sessions=sessions,
    )
    model = v9.fit_predict_v9(rows=prepared, folds=folds)
    availability = evaluate_crossfit_decision_availability_v10(
        rows=prepared, owner_sessions=sessions,
        unavailable_ids=model.model_unavailable_opportunity_ids,
    )
    adjusted = v9.apply_model_unavailable_abstentions_v9(
        rows=prepared, opportunity_ids=model.model_unavailable_opportunity_ids,
    )
    universes = build_strategy_prediction_universes_v11(
        model=model, rows=adjusted, folds=folds,
    )
    baseline_coverage = evaluate_required_baseline_coverage_v12(
        rows=adjusted, folds=folds, universes=universes,
    )
    evaluation, selected = evaluate_final_strategies(
        universes=universes.predictions, rows=adjusted,
        resolutions=resolutions, contract=contract,
    )
    completeness = evaluate_evaluation_completeness_v10(
        evaluation=evaluation, selected_path_coverage=selected,
    )
    statistic = _crossfit_statistic_v11(
        evaluation=evaluation, sessions=sessions, folds=folds,
    )
    return CrossfitEvidenceV12(
        CrossfitEvidenceV10(statistic, availability, selected, completeness),
        baseline_coverage,
    )


def _path_payload(path: v5.AccountPathV5) -> dict[str, object]:
    return {
        "ending_equity_usd": str(path.ending_equity_usd),
        "maximum_continuous_drawdown_usd": str(path.maximum_continuous_drawdown_usd),
        "complete": path.complete,
        "admitted_trades": len(path.admitted),
        "terminal_dispositions": dict(path.terminal_dispositions),
        "session_net_pnl_usd": {
            session: str(value) for session, value in path.session_net_pnl_usd.items()
        },
    }


def run_final_trial_pipeline(
    *, streams: Mapping[tuple[str, int], Iterable[v5.V5SourceRecord]],
    census: Sequence[v5.CensusCheckpoint], contract: Mapping[str, object],
    trial_id: str, runtime_receipt: Mapping[str, object],
) -> v5.V5PipelineResult:
    """Run only after final registration and separate historical authorization."""

    if not v5._hex64(trial_id):
        raise IntegrityError("final pipeline requires a registered trial identity")
    folds = v5.build_v5_folds_from_census(census)
    prediction_sessions = tuple(
        session for fold in folds for session in fold.test_sessions
    )
    materialized, resolutions = materialize_reported_bar_streams(
        streams=streams, census=census, contract=contract,
        prediction_scope_sessions=prediction_sessions,
    )
    model = v9.fit_predict_v9(rows=materialized, folds=folds)
    rows = v9.apply_model_unavailable_abstentions_v9(
        rows=materialized,
        opportunity_ids=model.model_unavailable_opportunity_ids,
    )
    universes = build_strategy_prediction_universes_v11(
        model=model, rows=rows, folds=folds,
    )
    evaluation, selected = evaluate_final_strategies(
        universes=universes.predictions, rows=rows,
        resolutions=resolutions, contract=contract,
    )
    completeness = evaluate_evaluation_completeness_v10(
        evaluation=evaluation, selected_path_coverage=selected,
    )
    baseline_coverage = evaluate_required_baseline_coverage_v12(
        rows=rows, folds=folds, universes=universes,
    )
    segmented: dict[str, Mapping[str, v5.AccountPathV5]] = {}
    for strategy in v5.REQUIRED_ACTIVE_STRATEGIES_V5:
        predictions = universes.predictions[strategy]
        plan = plan_final_strategy(
            strategy=strategy, predictions=predictions, rows=rows,
            scenario="stress", resolutions=resolutions, contract=contract,
        )
        segmented[strategy] = segmented_account_views_v11(
            plan=plan,
            opportunity_market_year={
                item.opportunity_id: (item.market, item.year)
                for item in predictions
            },
        )
    prediction_scope = set(prediction_sessions)
    calendar_open_ids = {
        item.expected.opportunity_id for item in census if item.calendar_open
    }
    evaluation_rows = [
        row for row in rows
        if row.expected.exchange_session_date in prediction_scope
    ]
    expected_by: dict[str, int] = {}
    features_by: dict[str, int] = {}
    for row in evaluation_rows:
        if row.expected.opportunity_id not in calendar_open_ids:
            continue
        key = f"{row.expected.market}/{row.expected.year}"
        expected_by[key] = expected_by.get(key, 0) + 1
        features_by[key] = features_by.get(key, 0) + int(row.features is not None)
    coverage_evidence = v5.CoverageEvidence(
        expected=len(evaluation_rows), terminal=len(evaluation_rows),
        causal_feature_expected=sum(
            row.expected.opportunity_id in calendar_open_ids
            for row in evaluation_rows
        ),
        causal_feature_eligible=sum(row.features is not None for row in evaluation_rows),
        predictions=len(model.predictions), market_year_expected=expected_by,
        market_year_feature_eligible=features_by,
    )
    coverage_result = v5.evaluate_coverage_gate(coverage_evidence)
    crossfit = build_final_nested_crossfit(
        rows=rows, resolutions=resolutions, contract=contract,
    )
    combined_baseline_coverage = {
        **baseline_coverage, "nested_crossfit": crossfit.baseline_coverage,
    }
    if crossfit.baseline_coverage.get("status") != "PASS":
        combined_baseline_coverage["status"] = "INCONCLUSIVE_DATA_OR_COVERAGE"
        combined_baseline_coverage["passed"] = False
    complete_decision = None
    if (
        completeness.get("passed") is True
        and crossfit.controls.evaluation_completeness.get("passed") is True
    ):
        complete_decision = derive_v10_decision(
            evaluation=evaluation, evaluation_sessions=prediction_sessions,
            coverage=coverage_evidence,
            baseline_coverage=combined_baseline_coverage,
            crossfit=crossfit.controls,
            evaluation_completeness=completeness,
            seed=int(trial_id[:16], 16),
        )
    decision = derive_final_decision(
        evaluation=evaluation, selected_path_coverage=selected,
        complete_decision=complete_decision,
    )
    candidate_path = evaluation["stress"]["candidate"]
    opportunity_ledger = tuple(
        asdict(record) for record in v5.finalize_candidate_ledger_v5(
            rows=rows, candidate_path=candidate_path,
        )
    )
    fills: list[dict[str, object]] = []
    marks: list[dict[str, object]] = []
    for scenario, paths in evaluation.items():
        for strategy, path in paths.items():
            for trade in path.admitted:
                fills.append({
                    "scenario": scenario, "strategy": strategy,
                    "opportunity_id": trade.opportunity_id,
                    "market": trade.market, "year": trade.year,
                    "session": trade.session, "checkpoint": trade.checkpoint,
                    "direction": trade.direction, "fill": asdict(trade.fill),
                })
            for at_ns, opportunity_id, kind, equity in path.equity_marks:
                marks.append({
                    "scenario": scenario, "strategy": strategy,
                    "at_ns": at_ns, "opportunity_id": opportunity_id,
                    "kind": kind, "equity_usd": str(equity),
                })
    artifacts = v5.EvidenceArtifactsV5(
        model=model.canonical_model_payload,
        predictions=tuple(asdict(item) for item in model.predictions),
        opportunity_ledger=opportunity_ledger,
        fills=tuple(fills), continuous_equity_marks=tuple(marks),
        segmented_metrics={
            "continuous_account_paths": {
                scenario: {
                    strategy: _path_payload(path)
                    for strategy, path in paths.items()
                }
                for scenario, paths in evaluation.items()
            },
            "independent_market_years_stress": {
                strategy: {
                    key: _path_payload(path) for key, path in views.items()
                }
                for strategy, views in segmented.items()
            },
        },
        inference={
            "coverage": coverage_result,
            "independent_required_baseline_coverage": baseline_coverage,
            "nested_crossfit_required_baseline_coverage": crossfit.baseline_coverage,
            "outer_selected_path_coverage": selected,
            "outer_evaluation_completeness": completeness,
            "nested_crossfit_decision_availability": crossfit.controls.decision_availability,
            "nested_crossfit_selected_path_coverage": crossfit.controls.selected_path_coverage,
            "nested_crossfit_evaluation_completeness": crossfit.controls.evaluation_completeness,
            "outer_model_unavailable_opportunity_ids": list(
                model.model_unavailable_opportunity_ids
            ),
        },
        decision=decision, runtime_receipt=runtime_receipt,
    )
    v5.build_evidence_manifest_v5(trial_id=trial_id, artifacts=artifacts)
    return v5.V5PipelineResult(  # type: ignore[arg-type]
        rows, model, evaluation, segmented, crossfit.controls.base,
        coverage_result, decision, artifacts,
    )
