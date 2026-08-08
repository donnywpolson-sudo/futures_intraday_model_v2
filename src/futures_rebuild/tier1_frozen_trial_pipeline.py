"""Unversioned frozen Tier 1 source adapter and pipeline entrypoint.

This module replaces the invalid exact-clock-minute materialization rule with
the preregistered reported-trade-bar semantics.  It retains every calendar
checkpoint, computes features only from information available at the decision,
and attaches future execution paths only after prediction eligibility is fixed.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict
from decimal import Decimal

from .errors import IntegrityError
from .canonical import sha256_json
from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v9 as v9
from .tier1_bracket_post_audit import cost_ticks, planned_initial_loss_usd
from .tier1_bracket_v4 import DirectionOutcomes, simulate_v4_bracket_fill
from .tier1_bracket_v10_decision_validity import (
    DirectionalOutcomeResolutionV10,
    evaluate_evaluation_completeness_v10,
)
from .tier1_bracket_v10_pipeline import derive_v10_decision
from .tier1_bracket_v11 import (
    build_strategy_prediction_universes_v11,
    evaluate_independent_strategies_v11,
    plan_independent_strategy_v11,
    segmented_account_views_v11,
)
from .tier1_bracket_v12 import evaluate_required_baseline_coverage_v12
from .tier1_bracket_v12_pipeline import build_nested_crossfit_evidence_v12
from .tier1_frozen_successor_source_semantics import (
    compute_reported_feature_values,
    select_reported_execution_path,
    select_reported_feature_window,
)


def _empty_row(
    *, checkpoint: v5.CensusCheckpoint, disposition: str,
) -> v5.MaterializedRowV5:
    expected = checkpoint.expected
    return v5.MaterializedRowV5(
        expected,
        v5.OpportunityRecordV5(
            expected.opportunity_id, expected.market,
            expected.exchange_session_date, expected.checkpoint,
            expected.decision_at_ns, disposition, False,
        ),
        None, None, None, None,
    )


def materialize_reported_bar_rows(
    *, source_rows: Sequence[v5.V5SourceRecord],
    census: Sequence[v5.CensusCheckpoint],
    market_specs: Mapping[str, v5.MarketSpec], contract: Mapping[str, object],
    prediction_scope_sessions: Sequence[str],
) -> tuple[
    tuple[v5.MaterializedRowV5, ...],
    Mapping[str, DirectionalOutcomeResolutionV10],
]:
    """Materialize retained checkpoints without filling unreported minutes."""

    for row in source_rows:
        row.validate()
    if not census or not {item.expected.market for item in census} <= set(market_specs):
        raise IntegrityError("frozen source adapter lacks market economics")
    for spec in market_specs.values():
        spec.validate()
    prediction_scope = set(prediction_scope_sessions)
    fee = Decimal(str(contract["costs"]["fee_per_side_usd"]))  # type: ignore[index]
    output: list[v5.MaterializedRowV5] = []
    resolutions: dict[str, DirectionalOutcomeResolutionV10] = {}
    for checkpoint in census:
        expected = checkpoint.expected
        expected.validate()
        if not checkpoint.calendar_open:
            output.append(_empty_row(checkpoint=checkpoint, disposition="CALENDAR_CLOSED"))
            continue
        raw = tuple(
            row for row in source_rows
            if row.market == expected.market
            and row.exchange_session_date == expected.exchange_session_date
        )
        if not raw:
            output.append(_empty_row(
                checkpoint=checkpoint, disposition="MISSING_SOURCE_SESSION",
            ))
            continue
        try:
            selected = select_reported_feature_window(
                source_rows=raw, market=expected.market,
                exchange_session_date=expected.exchange_session_date,
                decision_at_ns=expected.decision_at_ns,
            )
            computed = compute_reported_feature_values(
                window=selected, decision_at_ns=expected.decision_at_ns,
            )
        except IntegrityError:
            output.append(_empty_row(
                checkpoint=checkpoint, disposition="INSUFFICIENT_CAUSAL_HISTORY",
            ))
            continue
        last = selected.rows[-1]
        assert last.bar is not None
        spec = market_specs[expected.market]
        planned = planned_initial_loss_usd(
            atr=computed.atr, tick_size=spec.tick_size,
            tick_value=spec.tick_value,
            round_trip_cost_ticks=cost_ticks(
                contract=contract, scenario="stress", market=expected.market,
            ),
            fee_per_side_usd=fee,
        )
        predict = expected.exchange_session_date in prediction_scope
        risk_eligible = planned <= Decimal("250")
        outcomes: dict[str, DirectionOutcomes] = {}
        execution_bars = ()
        resolution: DirectionalOutcomeResolutionV10 | None = None
        if risk_eligible:
            try:
                execution = select_reported_execution_path(
                    source_rows=raw, market=expected.market,
                    exchange_session_date=expected.exchange_session_date,
                    decision_at_ns=expected.decision_at_ns,
                )
                execution_bars = execution.bars
                fills = {}
                for scenario in ("base", "stress", "extreme"):
                    ticks = cost_ticks(
                        contract=contract, scenario=scenario, market=expected.market,
                    )
                    directional = {}
                    for direction in ("long", "short"):
                        try:
                            directional[direction] = simulate_v4_bracket_fill(
                                direction=direction,
                                decision_at_ns=expected.decision_at_ns,
                                entry_bar=execution.entry_bar,
                                path_bars=execution.bars,
                                atr=computed.atr,
                                tick_size=spec.tick_size,
                                tick_value=spec.tick_value,
                                point_value=spec.point_value,
                                fee_per_side_usd=fee,
                                round_trip_cost_ticks=ticks,
                                maximum_planned_loss_usd=Decimal("250"),
                            )
                        except IntegrityError:
                            continue
                        fills[(scenario, direction)] = directional[direction]
                    if set(directional) == {"long", "short"}:
                        outcomes[scenario] = DirectionOutcomes(
                            directional["long"], directional["short"],
                        )
                if fills:
                    resolution = DirectionalOutcomeResolutionV10(
                        dict(sorted(fills.items())), execution.bars, None,
                    )
            except IntegrityError:
                outcomes = {}
                execution_bars = ()
        coverage = (
            "NOT_APPLICABLE_RISK_INELIGIBLE" if not risk_eligible
            else "COMPLETE" if len(outcomes) == 3
            else "STRESS_COMPLETE_PARTIAL_DIAGNOSTICS" if "stress" in outcomes
            else "MISSING"
        )
        ledger = v5.OpportunityRecordV5(
            expected.opportunity_id, expected.market,
            expected.exchange_session_date, expected.checkpoint,
            expected.decision_at_ns,
            "PREDICTION_PRODUCED" if predict else "TRAINING_OR_PREDICTION_INELIGIBLE",
            predict, last.bar.event_at_ns, last.bar.available_at_ns,
            outcome_coverage=coverage,
        )
        ledger.validate()
        output.append(v5.MaterializedRowV5(
            expected, ledger, computed.values, computed.atr,
            last.source_row_sha256,
            outcomes if "stress" in outcomes else None,
            tuple(execution_bars), spec, risk_eligible,
        ))
        if resolution is not None:
            resolutions[expected.opportunity_id] = resolution
    v5.reconcile_v5_opportunity_ledger(
        expected_ids=[item.expected.opportunity_id for item in census],
        records=[item.ledger for item in output],
    )
    return tuple(output), dict(sorted(resolutions.items()))


def materialize_reported_bar_streams(
    *, streams: Mapping[tuple[str, int], Iterable[v5.V5SourceRecord]],
    census: Sequence[v5.CensusCheckpoint], contract: Mapping[str, object],
    prediction_scope_sessions: Sequence[str], maximum_session_rows: int = 2_000,
) -> tuple[
    tuple[v5.MaterializedRowV5, ...],
    Mapping[str, DirectionalOutcomeResolutionV10],
]:
    """Stream all 20 market-years with a one-session memory bound."""

    expected_keys = {
        (market, year) for market in v5.MARKETS for year in range(2018, 2023)
    }
    if set(streams) != expected_keys or maximum_session_rows != 2_000:
        raise IntegrityError("frozen stream source coverage or memory bound is invalid")
    census_by_key: dict[tuple[str, int], list[v5.CensusCheckpoint]] = {}
    for checkpoint in census:
        census_by_key.setdefault(
            (checkpoint.expected.market, checkpoint.expected.year), [],
        ).append(checkpoint)
    if set(census_by_key) != expected_keys:
        raise IntegrityError("frozen stream calendar lacks a market-year")
    all_rows: list[v5.MaterializedRowV5] = []
    all_resolutions: dict[str, DirectionalOutcomeResolutionV10] = {}
    for key in sorted(expected_keys):
        market, year = key
        by_session: dict[str, list[v5.CensusCheckpoint]] = {}
        for checkpoint in census_by_key[key]:
            by_session.setdefault(
                checkpoint.expected.exchange_session_date, [],
            ).append(checkpoint)
        current_session: str | None = None
        buffer: list[v5.V5SourceRecord] = []
        seen: set[str] = set()
        spec: v5.MarketSpec | None = None
        previous_event = -1

        def flush() -> None:
            nonlocal buffer
            if current_session is None:
                return
            if current_session in seen:
                raise IntegrityError("frozen source session is not contiguous")
            seen.add(current_session)
            checkpoints = by_session.get(current_session)
            if checkpoints:
                if spec is None:
                    raise IntegrityError("frozen source stream lacks market economics")
                rows, resolutions = materialize_reported_bar_rows(
                    source_rows=tuple(buffer), census=tuple(checkpoints),
                    market_specs={market: spec}, contract=contract,
                    prediction_scope_sessions=prediction_scope_sessions,
                )
                all_rows.extend(rows)
                overlap = set(all_resolutions).intersection(resolutions)
                if overlap:
                    raise IntegrityError("frozen source outcomes are duplicated")
                all_resolutions.update(resolutions)
            buffer = []

        for record in streams[key]:
            record.validate()
            if record.market != market or int(record.exchange_session_date[:4]) != year:
                raise IntegrityError("frozen source row is outside its binding")
            if record.bar is not None:
                if record.bar.event_at_ns < previous_event:
                    raise IntegrityError("frozen source stream is not chronological")
                previous_event = record.bar.event_at_ns
            if record.market_spec is not None:
                if spec is None:
                    spec = record.market_spec
                elif spec != record.market_spec:
                    raise IntegrityError("frozen market economics changed inside a source")
            if current_session is None:
                current_session = record.exchange_session_date
            elif record.exchange_session_date != current_session:
                if record.exchange_session_date < current_session:
                    raise IntegrityError("frozen source sessions are not chronological")
                flush()
                current_session = record.exchange_session_date
            buffer.append(record)
            if len(buffer) > maximum_session_rows:
                raise IntegrityError("frozen source session exceeds its memory bound")
        flush()
        if spec is None:
            raise IntegrityError("frozen market-year contains no valid economics")
        for session in sorted(set(by_session) - seen):
            rows, _ = materialize_reported_bar_rows(
                source_rows=(), census=tuple(by_session[session]),
                market_specs={market: spec}, contract=contract,
                prediction_scope_sessions=prediction_scope_sessions,
            )
            all_rows.extend(rows)
    all_rows.sort(key=lambda row: (
        row.expected.exchange_session_date, row.expected.checkpoint,
        v5.MARKETS.index(row.expected.market),
    ))
    v5.reconcile_v5_opportunity_ledger(
        expected_ids=[item.expected.opportunity_id for item in census],
        records=[item.ledger for item in all_rows],
    )
    return tuple(all_rows), dict(sorted(all_resolutions.items()))


def derive_frozen_trial_decision(
    *, evaluation: Mapping[str, Mapping[str, v5.AccountPathV5]],
    evaluation_sessions: Sequence[str], coverage: v5.CoverageEvidence,
    baseline_coverage: Mapping[str, object],
    crossfit: object, evaluation_completeness: Mapping[str, object], seed: int,
) -> dict[str, object]:
    """Promote on aggregate evidence; retain sparse sleeve tests as diagnostics."""

    inherited = derive_v10_decision(
        evaluation=evaluation, evaluation_sessions=evaluation_sessions,
        coverage=coverage, baseline_coverage=baseline_coverage,
        crossfit=crossfit,  # type: ignore[arg-type]
        evaluation_completeness=evaluation_completeness, seed=seed,
    )
    if inherited.get("inference_executed") is not True:
        return inherited
    core = dict(inherited)
    core.pop("decision_id", None)
    core["schema_version"] = "tier1_frozen_trial_decision/1.0.0"
    core["sleeve_tests_role"] = (
        "DIAGNOSTIC_ONLY_NOT_A_REQUIREMENT_THAT_EVERY_SPARSE_SLEEVE_BE_POSITIVE"
    )
    effect = core.get("candidate_effect_classification")
    if effect != "PASS_EFFECT_GATE":
        classification = str(effect)
    elif core.get("stress_and_baselines_passed") is not True:
        classification = "FAIL_MULTIPLICITY_OR_CONTROL"
    elif (
        core.get("distribution_passed") is not True
        or core.get("drawdown_passed") is not True
    ):
        classification = "FAIL_PROMOTION_GATE"
    else:
        classification = "PASS_HISTORICAL_SCREEN"
    core["classification"] = classification
    return {**core, "decision_id": sha256_json(core)}


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


def run_frozen_trial_pipeline(
    *, streams: Mapping[tuple[str, int], Iterable[v5.V5SourceRecord]],
    census: Sequence[v5.CensusCheckpoint], contract: Mapping[str, object],
    trial_id: str, runtime_receipt: Mapping[str, object],
) -> v5.V5PipelineResult:
    """Run the one frozen trial only after registration and separate approval."""

    if not v5._hex64(trial_id):
        raise IntegrityError("frozen pipeline requires a registered trial identity")
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
    evaluation, selected_coverage = evaluate_independent_strategies_v11(
        universes=universes, rows=rows, resolutions=resolutions,
    )
    completeness = evaluate_evaluation_completeness_v10(
        evaluation=evaluation, selected_path_coverage=selected_coverage,
    )
    baseline_coverage = evaluate_required_baseline_coverage_v12(
        rows=rows, folds=folds, universes=universes,
    )
    segmented: dict[str, Mapping[str, v5.AccountPathV5]] = {}
    for strategy in v5.REQUIRED_ACTIVE_STRATEGIES_V5:
        predictions = universes.predictions[strategy]
        plan = plan_independent_strategy_v11(
            strategy=strategy, predictions=predictions, rows=rows,
            scenario="stress", resolutions=resolutions,
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
    crossfit = build_nested_crossfit_evidence_v12(
        rows=rows, resolutions=resolutions,
    )
    combined_baseline_coverage = {
        **baseline_coverage, "nested_crossfit": crossfit.baseline_coverage,
    }
    if crossfit.baseline_coverage.get("status") != "PASS":
        combined_baseline_coverage["status"] = "INCONCLUSIVE_DATA_OR_COVERAGE"
        combined_baseline_coverage["passed"] = False
    decision = derive_frozen_trial_decision(
        evaluation=evaluation, evaluation_sessions=prediction_sessions,
        coverage=coverage_evidence,
        baseline_coverage=combined_baseline_coverage,
        crossfit=crossfit.controls,
        evaluation_completeness=completeness,
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
