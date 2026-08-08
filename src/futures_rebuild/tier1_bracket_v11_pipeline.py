"""Prepared V11 pipeline with independently constructed baseline universes."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal

from .errors import IntegrityError
from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v8 as v8
from . import tier1_bracket_v9 as v9
from .tier1_bracket_v10_decision_validity import (
    DirectionalOutcomeResolutionV10, attach_causal_outcomes_v10,
    evaluate_crossfit_decision_availability_v10,
    evaluate_evaluation_completeness_v10,
    materialize_checkpoint_scoped_streams_v10,
    prepare_crossfit_prediction_rows_v10,
)
from .tier1_bracket_v10_pipeline import (
    CrossfitEvidenceV10, _nested_folds_v10, derive_v10_decision,
)
from .tier1_bracket_v11 import (
    StrategyPredictionUniversesV11, build_strategy_prediction_universes_v11,
    evaluate_independent_strategies_v11,
    evaluate_required_baseline_coverage_v11, plan_independent_strategy_v11,
    segmented_account_views_v11,
)


@dataclass(frozen=True)
class CrossfitEvidenceV11:
    controls: CrossfitEvidenceV10
    baseline_coverage: Mapping[str, object]


def _crossfit_statistic_v11(
    *, evaluation: Mapping[str, Mapping[str, v5.AccountPathV5]],
    sessions: Sequence[str], folds: Sequence[object],
) -> v5.CrossfitEvidenceBundleV5:
    stress = evaluation["stress"]
    candidate = stress["candidate"]
    differential = {
        baseline: tuple(
            float((
                candidate.session_net_pnl_usd.get(session, Decimal("0"))
                - stress[baseline].session_net_pnl_usd.get(session, Decimal("0"))
            ) / Decimal("100000"))
            for session in sessions
        )
        for baseline in v5.REQUIRED_ACTIVE_STRATEGIES_V5[1:]
    }
    sleeve_ids = tuple(
        f"{market}/{checkpoint}/{direction}"
        for market in v5.MARKETS for checkpoint in v5.CHECKPOINTS
        for direction in ("long", "short")
    )
    contributions = {
        sleeve: {session: Decimal("0") for session in sessions}
        for sleeve in sleeve_ids
    }
    for trade in candidate.admitted:
        contributions[f"{trade.market}/{trade.checkpoint}/{trade.direction}"][trade.session] += (
            trade.fill.net_pnl_usd
        )
    sleeve_returns = {
        sleeve: tuple(
            float(contributions[sleeve][session] / Decimal("100000"))
            for session in sessions
        )
        for sleeve in sleeve_ids
    }
    fold_ids = tuple(
        int(getattr(fold, "outer_fold"))
        for fold in folds for _ in getattr(fold, "test_sessions")
    )
    if len(fold_ids) != len(sessions):
        raise IntegrityError("V11 crossfit fold ownership does not reconcile")
    return v5.CrossfitEvidenceBundleV5(
        tuple(sessions), fold_ids, differential, sleeve_returns,
    )


def build_nested_crossfit_evidence_v11(
    *, rows: Sequence[v5.MaterializedRowV5],
    resolutions: Mapping[str, DirectionalOutcomeResolutionV10],
) -> CrossfitEvidenceV11:
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
    baseline_coverage = evaluate_required_baseline_coverage_v11(
        rows=adjusted, folds=folds, universes=universes,
    )
    evaluation, selected_coverage = evaluate_independent_strategies_v11(
        universes=universes, rows=adjusted, resolutions=resolutions,
    )
    completeness = evaluate_evaluation_completeness_v10(
        evaluation=evaluation, selected_path_coverage=selected_coverage,
    )
    base = _crossfit_statistic_v11(
        evaluation=evaluation, sessions=sessions, folds=folds,
    )
    return CrossfitEvidenceV11(
        CrossfitEvidenceV10(base, availability, selected_coverage, completeness),
        baseline_coverage,
    )


def run_v11_pipeline(
    *, streams: Mapping[tuple[str, int], Iterable[v5.V5SourceRecord]],
    census: Sequence[v5.CensusCheckpoint], contract: Mapping[str, object],
    trial_id: str, runtime_receipt: Mapping[str, object],
) -> v5.V5PipelineResult:
    if not v5._hex64(trial_id):
        raise IntegrityError("V11 pipeline requires a registered trial identity")
    folds = v5.build_v5_folds_from_census(census)
    prediction_sessions = tuple(session for fold in folds for session in fold.test_sessions)
    materialized = materialize_checkpoint_scoped_streams_v10(
        streams=streams, census=census, contract=contract,
        prediction_scope_sessions=prediction_sessions,
    )
    upgraded, resolutions = attach_causal_outcomes_v10(
        rows=materialized, contract=contract,
    )
    model = v9.fit_predict_v9(rows=upgraded, folds=folds)
    rows = v9.apply_model_unavailable_abstentions_v9(
        rows=upgraded, opportunity_ids=model.model_unavailable_opportunity_ids,
    )
    universes = build_strategy_prediction_universes_v11(
        model=model, rows=rows, folds=folds,
    )
    evaluation, selected_coverage = evaluate_independent_strategies_v11(
        universes=universes, rows=rows, resolutions=resolutions,
    )
    completeness = evaluate_evaluation_completeness_v10(
        evaluation=evaluation, selected_path_coverage=selected_coverage,
    )
    baseline_coverage = evaluate_required_baseline_coverage_v11(
        rows=rows, folds=folds, universes=universes,
    )
    segmented: dict[str, Mapping[str, v5.AccountPathV5]] = {}
    for strategy in v5.REQUIRED_ACTIVE_STRATEGIES_V5:
        predictions = universes.predictions[strategy]
        plan = plan_independent_strategy_v11(
            strategy=strategy, predictions=predictions, rows=rows,
            scenario="stress", resolutions=resolutions,
        )
        metadata = {
            item.opportunity_id: (item.market, item.year) for item in predictions
        }
        segmented[strategy] = segmented_account_views_v11(
            plan=plan,
            opportunity_market_year=metadata,
        )
    prediction_scope = set(prediction_sessions)
    calendar_open_ids = {
        item.expected.opportunity_id for item in census if item.calendar_open
    }
    evaluation_rows = [
        row for row in rows if row.expected.exchange_session_date in prediction_scope
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
            row.expected.opportunity_id in calendar_open_ids for row in evaluation_rows
        ),
        causal_feature_eligible=sum(row.features is not None for row in evaluation_rows),
        predictions=len(model.predictions), market_year_expected=expected_by,
        market_year_feature_eligible=features_by,
    )
    coverage_result = v5.evaluate_coverage_gate(coverage_evidence)
    crossfit = build_nested_crossfit_evidence_v11(rows=rows, resolutions=resolutions)
    combined_baseline_coverage = {
        **baseline_coverage,
        "nested_crossfit": crossfit.baseline_coverage,
    }
    if crossfit.baseline_coverage.get("status") != "PASS":
        combined_baseline_coverage["status"] = "INCONCLUSIVE_DATA_OR_COVERAGE"
        combined_baseline_coverage["passed"] = False
    decision = derive_v10_decision(
        evaluation=evaluation, evaluation_sessions=prediction_sessions,
        coverage=coverage_evidence,
        baseline_coverage=combined_baseline_coverage,
        crossfit=crossfit.controls, evaluation_completeness=completeness,
        seed=int(trial_id[:16], 16),
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
                    "opportunity_id": trade.opportunity_id, "market": trade.market,
                    "year": trade.year, "session": trade.session,
                    "checkpoint": trade.checkpoint, "direction": trade.direction,
                    "fill": asdict(trade.fill),
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
        opportunity_ledger=opportunity_ledger, fills=tuple(fills),
        continuous_equity_marks=tuple(marks),
        segmented_metrics={
            "continuous_account_paths": {
                scenario: {
                    strategy: {
                        "ending_equity_usd": str(path.ending_equity_usd),
                        "maximum_continuous_drawdown_usd": str(
                            path.maximum_continuous_drawdown_usd
                        ),
                        "complete": path.complete,
                        "admitted_trades": len(path.admitted),
                        "terminal_dispositions": dict(path.terminal_dispositions),
                        "session_net_pnl_usd": {
                            session: str(value)
                            for session, value in path.session_net_pnl_usd.items()
                        },
                    }
                    for strategy, path in paths.items()
                }
                for scenario, paths in evaluation.items()
            },
            "independent_market_years_stress": {
                strategy: {
                key: {
                    "ending_equity_usd": str(path.ending_equity_usd),
                    "maximum_continuous_drawdown_usd": str(path.maximum_continuous_drawdown_usd),
                    "complete": path.complete,
                    "terminal_dispositions": dict(path.terminal_dispositions),
                }
                for key, path in views.items()
            }
            for strategy, views in segmented.items()
            },
        },
        inference={
            "coverage": coverage_result,
            "independent_required_baseline_coverage": baseline_coverage,
            "nested_crossfit_required_baseline_coverage": crossfit.baseline_coverage,
            "outer_selected_path_coverage": selected_coverage,
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
