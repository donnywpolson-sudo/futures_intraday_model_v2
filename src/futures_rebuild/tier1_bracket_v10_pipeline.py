"""Prepared, non-publishing V10 evaluation pipeline.

V10 retains V9's frozen model math and promotion thresholds.  It changes only
source-window and decision-validity mechanics established before V10
registration.  This module performs no file or provider access by itself.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal

from .canonical import sha256_json
from .errors import IntegrityError
from . import tier1_bracket_v4 as v4
from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v8 as v8
from . import tier1_bracket_v9 as v9
from .tier1_bracket_v10_decision_validity import (
    DirectionalOutcomeResolutionV10,
    attach_causal_outcomes_v10,
    evaluate_crossfit_decision_availability_v10,
    evaluate_evaluation_completeness_v10,
    evaluate_strategies_v10,
    materialize_checkpoint_scoped_streams_v10,
    plan_strategy_rank_before_outcome_v10,
    prepare_crossfit_prediction_rows_v10,
)


@dataclass(frozen=True)
class CrossfitEvidenceV10:
    base: v5.CrossfitEvidenceBundleV5
    decision_availability: Mapping[str, object]
    selected_path_coverage: Mapping[str, Mapping[str, object]]
    evaluation_completeness: Mapping[str, object]


def _nested_folds_v10(
    rows: Sequence[v5.MaterializedRowV5],
) -> tuple[tuple[v4.FoldSpec, ...], tuple[str, ...]]:
    sessions = sorted({
        row.expected.exchange_session_date for row in rows
        if row.expected.year in {2018, 2019}
    })
    seed_size = max(30, math.ceil(len(sessions) * 0.40))
    evaluation_sessions = sessions[seed_size:]
    if seed_size >= len(sessions) or len(evaluation_sessions) < 8:
        raise IntegrityError("V10 training history cannot support nested crossfit")
    quotient, remainder = divmod(len(evaluation_sessions), 8)
    folds: list[v4.FoldSpec] = []
    start = 0
    for index in range(8):
        size = quotient + (1 if index < remainder else 0)
        test = evaluation_sessions[start:start + size]
        first = sessions.index(test[0])
        training = sessions[:first - 1]
        if not training:
            raise IntegrityError("V10 nested crossfit embargo leaves no training history")
        folds.append(v4.FoldSpec(index, tuple(training), tuple(test)))
        start += size
    return tuple(folds), tuple(evaluation_sessions)


def build_nested_crossfit_evidence_v10(
    *, rows: Sequence[v5.MaterializedRowV5],
    resolutions: Mapping[str, DirectionalOutcomeResolutionV10],
) -> CrossfitEvidenceV10:
    folds, evaluation_sessions = _nested_folds_v10(rows)
    prepared = prepare_crossfit_prediction_rows_v10(
        rows=rows, owner_sessions=evaluation_sessions,
    )
    model = v9.fit_predict_v9(rows=prepared, folds=folds)
    availability = evaluate_crossfit_decision_availability_v10(
        rows=prepared, owner_sessions=evaluation_sessions,
        unavailable_ids=model.model_unavailable_opportunity_ids,
    )
    adjusted = v9.apply_model_unavailable_abstentions_v9(
        rows=prepared, opportunity_ids=model.model_unavailable_opportunity_ids,
    )
    evaluation, path_coverage = evaluate_strategies_v10(
        predictions=model.predictions, rows=adjusted, resolutions=resolutions,
        strategies=v5.REQUIRED_ACTIVE_STRATEGIES_V5,
    )
    completeness = evaluate_evaluation_completeness_v10(
        evaluation=evaluation, selected_path_coverage=path_coverage,
    )
    ordered_sessions = tuple(evaluation_sessions)
    stress = evaluation["stress"]
    candidate = stress["candidate"]
    differential = {
        baseline: tuple(
            float((
                candidate.session_net_pnl_usd.get(session, Decimal("0"))
                - stress[baseline].session_net_pnl_usd.get(session, Decimal("0"))
            ) / Decimal("100000"))
            for session in ordered_sessions
        )
        for baseline in v5.REQUIRED_ACTIVE_STRATEGIES_V5[1:]
    }
    sleeve_ids = tuple(
        f"{market}/{checkpoint}/{direction}"
        for market in v5.MARKETS for checkpoint in v5.CHECKPOINTS
        for direction in ("long", "short")
    )
    contributions = {
        sleeve: {session: Decimal("0") for session in ordered_sessions}
        for sleeve in sleeve_ids
    }
    for trade in candidate.admitted:
        contributions[
            f"{trade.market}/{trade.checkpoint}/{trade.direction}"
        ][trade.session] += trade.fill.net_pnl_usd
    sleeve_returns = {
        sleeve: tuple(
            float(contributions[sleeve][session] / Decimal("100000"))
            for session in ordered_sessions
        )
        for sleeve in sleeve_ids
    }
    base = v5.CrossfitEvidenceBundleV5(
        ordered_sessions,
        tuple(fold.outer_fold for fold in folds for _ in fold.test_sessions),
        differential, sleeve_returns,
    )
    if len(base.fold_ids) != len(base.sessions):
        raise IntegrityError("V10 nested crossfit fold ownership does not reconcile")
    return CrossfitEvidenceV10(base, availability, path_coverage, completeness)


def derive_v10_decision(
    *, evaluation: Mapping[str, Mapping[str, v5.AccountPathV5]],
    evaluation_sessions: Sequence[str], coverage: v5.CoverageEvidence,
    baseline_coverage: Mapping[str, object], crossfit: CrossfitEvidenceV10,
    evaluation_completeness: Mapping[str, object], seed: int,
) -> dict[str, object]:
    coverage_result = v5.evaluate_coverage_gate(coverage)
    blocking_statuses = {
        "outer_coverage": coverage_result.get("status"),
        "required_baseline_coverage": baseline_coverage.get("status"),
        "outer_evaluation_completeness": evaluation_completeness.get("status"),
        "nested_crossfit_decision_availability": crossfit.decision_availability.get("status"),
        "nested_crossfit_evaluation_completeness": crossfit.evaluation_completeness.get("status"),
    }
    if any(status != "PASS" for status in blocking_statuses.values()):
        classification = (
            "INVALID" if "INVALID" in blocking_statuses.values()
            else "INCONCLUSIVE_DATA_OR_POWER"
            if crossfit.decision_availability.get("status") == "INCONCLUSIVE_DATA_OR_POWER"
            else "INCONCLUSIVE_DATA_OR_COVERAGE"
        )
        core = {
            "schema_version": "tier1_bracket_successor_v10_decision/1.0.0",
            "classification": classification,
            "inference_executed": False,
            "gate_order": "DATA_AND_POLICY_COMPLETENESS_BEFORE_PERFORMANCE_INFERENCE",
            "blocking_gate_statuses": blocking_statuses,
            "coverage": coverage_result,
            "required_baseline_coverage": dict(baseline_coverage),
            "nested_crossfit_decision_availability": dict(crossfit.decision_availability),
            "nested_crossfit_selected_path_coverage": dict(crossfit.selected_path_coverage),
            "nested_crossfit_evaluation_completeness": dict(crossfit.evaluation_completeness),
            "outer_evaluation_completeness": dict(evaluation_completeness),
        }
        return {**core, "decision_id": sha256_json(core)}
    inherited = v8.derive_v8_decision(
        evaluation=evaluation, evaluation_sessions=evaluation_sessions,
        coverage=coverage, baseline_coverage=baseline_coverage,
        crossfit=crossfit.base, seed=seed,
    )
    core = dict(inherited)
    core.pop("decision_id", None)
    core["schema_version"] = "tier1_bracket_successor_v10_decision/1.0.0"
    core["inference_executed"] = True
    core["gate_order"] = "DATA_AND_POLICY_COMPLETENESS_BEFORE_PERFORMANCE_INFERENCE"
    core["nested_crossfit_decision_availability"] = dict(crossfit.decision_availability)
    core["nested_crossfit_selected_path_coverage"] = dict(crossfit.selected_path_coverage)
    core["nested_crossfit_evaluation_completeness"] = dict(crossfit.evaluation_completeness)
    core["outer_evaluation_completeness"] = dict(evaluation_completeness)
    return {**core, "decision_id": sha256_json(core)}


def run_v10_pipeline(
    *, streams: Mapping[tuple[str, int], Iterable[v5.V5SourceRecord]],
    census: Sequence[v5.CensusCheckpoint], contract: Mapping[str, object],
    trial_id: str, runtime_receipt: Mapping[str, object],
) -> v5.V5PipelineResult:
    """Run only after a registered V10 trial and separate historical approval."""

    if not v5._hex64(trial_id):
        raise IntegrityError("V10 pipeline requires a registered trial identity")
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
    evaluation, selected_coverage = evaluate_strategies_v10(
        predictions=model.predictions, rows=rows, resolutions=resolutions,
        strategies=v5.REQUIRED_ACTIVE_STRATEGIES_V5,
    )
    evaluation_completeness = evaluate_evaluation_completeness_v10(
        evaluation=evaluation, selected_path_coverage=selected_coverage,
    )
    opportunity_metadata = {
        item.opportunity_id: (item.market, item.year) for item in model.predictions
    }
    segmented: dict[str, Mapping[str, v5.AccountPathV5]] = {}
    for strategy in v5.REQUIRED_ACTIVE_STRATEGIES_V5:
        plan = plan_strategy_rank_before_outcome_v10(
            strategy=strategy, predictions=model.predictions, rows=rows,
            scenario="stress", resolutions=resolutions,
        )
        segmented[strategy] = v5.segmented_account_views_v5(
            strategy=strategy, planned_trades=plan.trades,
            opportunity_market_year=opportunity_metadata,
        )
    prediction_scope = set(prediction_sessions)
    calendar_open_ids = {
        item.expected.opportunity_id for item in census if item.calendar_open
    }
    evaluation_rows = [
        row for row in rows if row.expected.exchange_session_date in prediction_scope
    ]
    market_year_expected: dict[str, int] = {}
    market_year_features: dict[str, int] = {}
    for row in evaluation_rows:
        if row.expected.opportunity_id not in calendar_open_ids:
            continue
        key = f"{row.expected.market}/{row.expected.year}"
        market_year_expected[key] = market_year_expected.get(key, 0) + 1
        market_year_features[key] = market_year_features.get(key, 0) + int(
            row.features is not None
        )
    coverage_evidence = v5.CoverageEvidence(
        expected=len(evaluation_rows), terminal=len(evaluation_rows),
        causal_feature_expected=sum(
            row.expected.opportunity_id in calendar_open_ids for row in evaluation_rows
        ),
        causal_feature_eligible=sum(row.features is not None for row in evaluation_rows),
        predictions=len(model.predictions),
        market_year_expected=market_year_expected,
        market_year_feature_eligible=market_year_features,
    )
    coverage_result = v5.evaluate_coverage_gate(coverage_evidence)
    baseline_coverage = v8.evaluate_required_baseline_coverage_v8(model.predictions)
    crossfit = build_nested_crossfit_evidence_v10(rows=rows, resolutions=resolutions)
    decision = derive_v10_decision(
        evaluation=evaluation, evaluation_sessions=prediction_sessions,
        coverage=coverage_evidence, baseline_coverage=baseline_coverage,
        crossfit=crossfit, evaluation_completeness=evaluation_completeness,
        seed=int(trial_id[:16], 16),
    )
    opportunity_ledger = tuple(
        asdict(record) for record in v5.finalize_candidate_ledger_v5(
            rows=rows, candidate_path=evaluation["stress"]["candidate"],
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
                    "scenario": scenario, "strategy": strategy, "at_ns": at_ns,
                    "opportunity_id": opportunity_id, "kind": kind,
                    "equity_usd": str(equity),
                })
    segmented_payload = {
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
    }
    artifacts = v5.EvidenceArtifactsV5(
        model=model.canonical_model_payload,
        predictions=tuple(asdict(item) for item in model.predictions),
        opportunity_ledger=opportunity_ledger, fills=tuple(fills),
        continuous_equity_marks=tuple(marks), segmented_metrics=segmented_payload,
        inference={
            "coverage": coverage_result,
            "required_baseline_coverage": baseline_coverage,
            "outer_selected_path_coverage": selected_coverage,
            "outer_evaluation_completeness": evaluation_completeness,
            "nested_crossfit_decision_availability": crossfit.decision_availability,
            "nested_crossfit_selected_path_coverage": crossfit.selected_path_coverage,
            "nested_crossfit_evaluation_completeness": crossfit.evaluation_completeness,
            "outer_model_unavailable_opportunity_ids": list(
                model.model_unavailable_opportunity_ids
            ),
        },
        decision=decision, runtime_receipt=runtime_receipt,
    )
    v5.build_evidence_manifest_v5(trial_id=trial_id, artifacts=artifacts)
    return v5.V5PipelineResult(  # type: ignore[arg-type]
        rows, model, evaluation, segmented, crossfit.base, coverage_result,
        decision, artifacts,
    )
