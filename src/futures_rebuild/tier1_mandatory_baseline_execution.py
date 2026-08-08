"""Successor mechanics for complete scenario-specific baseline execution.

Historic Tier 1 trial modules are immutable registered evidence.  This module
builds on their frozen source semantics without changing those bytes.  It
materializes every scenario that is independently risk eligible, so a stress
risk abstention cannot erase a valid base execution path.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal

from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v9 as v9
from .errors import IntegrityError
from .tier1_bracket_post_audit import cost_ticks, planned_initial_loss_usd
from .tier1_bracket_v4 import DirectionOutcomes, simulate_v4_bracket_fill
from .tier1_bracket_v10_decision_validity import (
    DirectionalOutcomeResolutionV10,
    evaluate_evaluation_completeness_v10,
)
from .tier1_bracket_v10_pipeline import derive_v10_decision
from .tier1_bracket_v11 import build_strategy_prediction_universes_v11
from .tier1_bracket_v12 import evaluate_required_baseline_coverage_v12
from .tier1_final_decision_validity import (
    derive_final_decision,
    evaluate_final_strategies,
)
from .tier1_final_pipeline import build_final_nested_crossfit
from .tier1_frozen_successor_source_semantics import select_reported_execution_path
from .tier1_frozen_trial_pipeline import materialize_reported_bar_rows


SCENARIOS = ("base", "stress", "extreme")
MAXIMUM_PLANNED_LOSS_USD = Decimal("250")


def materialize_mandatory_baseline_rows(
    *, source_rows: Sequence[v5.V5SourceRecord],
    census: Sequence[v5.CensusCheckpoint],
    market_specs: Mapping[str, v5.MarketSpec],
    contract: Mapping[str, object],
    prediction_scope_sessions: Sequence[str],
) -> tuple[
    tuple[v5.MaterializedRowV5, ...],
    Mapping[str, DirectionalOutcomeResolutionV10],
]:
    """Add scenario-eligible fills omitted by the legacy stress-wide gate."""

    rows, inherited = materialize_reported_bar_rows(
        source_rows=source_rows,
        census=census,
        market_specs=market_specs,
        contract=contract,
        prediction_scope_sessions=prediction_scope_sessions,
    )
    resolutions = dict(inherited)
    output: list[v5.MaterializedRowV5] = []
    fee = Decimal(str(contract["costs"]["fee_per_side_usd"]))  # type: ignore[index]
    for row in rows:
        if (
            row.expected.opportunity_id in resolutions
            or row.features is None
            or row.atr is None
            or row.market_spec is None
        ):
            output.append(row)
            continue
        raw = tuple(
            item for item in source_rows
            if item.market == row.expected.market
            and item.exchange_session_date == row.expected.exchange_session_date
        )
        try:
            execution = select_reported_execution_path(
                source_rows=raw,
                market=row.expected.market,
                exchange_session_date=row.expected.exchange_session_date,
                decision_at_ns=row.expected.decision_at_ns,
            )
        except IntegrityError:
            output.append(row)
            continue
        fills = {}
        outcomes: dict[str, DirectionOutcomes] = {}
        for scenario in SCENARIOS:
            ticks = cost_ticks(
                contract=contract, scenario=scenario, market=row.expected.market,
            )
            if planned_initial_loss_usd(
                atr=row.atr,
                tick_size=row.market_spec.tick_size,
                tick_value=row.market_spec.tick_value,
                round_trip_cost_ticks=ticks,
                fee_per_side_usd=fee,
            ) > MAXIMUM_PLANNED_LOSS_USD:
                continue
            directional = {}
            for direction in ("long", "short"):
                try:
                    directional[direction] = simulate_v4_bracket_fill(
                        direction=direction,
                        decision_at_ns=row.expected.decision_at_ns,
                        entry_bar=execution.entry_bar,
                        path_bars=execution.bars,
                        atr=row.atr,
                        tick_size=row.market_spec.tick_size,
                        tick_value=row.market_spec.tick_value,
                        point_value=row.market_spec.point_value,
                        fee_per_side_usd=fee,
                        round_trip_cost_ticks=ticks,
                        maximum_planned_loss_usd=MAXIMUM_PLANNED_LOSS_USD,
                    )
                except IntegrityError:
                    continue
                fills[(scenario, direction)] = directional[direction]
            if set(directional) == {"long", "short"}:
                outcomes[scenario] = DirectionOutcomes(
                    directional["long"], directional["short"],
                )
        if not fills:
            output.append(row)
            continue
        resolutions[row.expected.opportunity_id] = DirectionalOutcomeResolutionV10(
            dict(sorted(fills.items())), execution.bars, None,
        )
        legacy_outcomes = row.outcomes
        if "stress" in outcomes:
            legacy_outcomes = outcomes
        output.append(replace(
            row, outcomes=legacy_outcomes, execution_path=execution.bars,
        ))
    return tuple(output), dict(sorted(resolutions.items()))


def materialize_mandatory_baseline_streams(
    *, streams: Mapping[tuple[str, int], Iterable[v5.V5SourceRecord]],
    census: Sequence[v5.CensusCheckpoint], contract: Mapping[str, object],
    prediction_scope_sessions: Sequence[str], maximum_session_rows: int = 2_000,
) -> tuple[
    tuple[v5.MaterializedRowV5, ...],
    Mapping[str, DirectionalOutcomeResolutionV10],
]:
    """Stream all market-years while retaining only one source session."""

    expected_keys = {
        (market, year) for market in v5.MARKETS for year in range(2018, 2023)
    }
    if set(streams) != expected_keys or maximum_session_rows != 2_000:
        raise IntegrityError("baseline-complete source coverage or memory bound is invalid")
    census_by_key: dict[tuple[str, int], list[v5.CensusCheckpoint]] = {}
    for checkpoint in census:
        census_by_key.setdefault(
            (checkpoint.expected.market, checkpoint.expected.year), [],
        ).append(checkpoint)
    if set(census_by_key) != expected_keys:
        raise IntegrityError("baseline-complete calendar lacks a market-year")

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
                raise IntegrityError("baseline-complete source session is not contiguous")
            seen.add(current_session)
            checkpoints = by_session.get(current_session)
            if checkpoints:
                if spec is None:
                    raise IntegrityError("baseline-complete source lacks market economics")
                rows, resolutions = materialize_mandatory_baseline_rows(
                    source_rows=tuple(buffer), census=tuple(checkpoints),
                    market_specs={market: spec}, contract=contract,
                    prediction_scope_sessions=prediction_scope_sessions,
                )
                all_rows.extend(rows)
                if set(all_resolutions).intersection(resolutions):
                    raise IntegrityError("baseline-complete outcomes are duplicated")
                all_resolutions.update(resolutions)
            buffer = []

        for record in streams[key]:
            record.validate()
            if record.market != market or int(record.exchange_session_date[:4]) != year:
                raise IntegrityError("baseline-complete source row is outside its binding")
            if record.bar is not None:
                if record.bar.event_at_ns < previous_event:
                    raise IntegrityError("baseline-complete source is not chronological")
                previous_event = record.bar.event_at_ns
            if record.market_spec is not None:
                if spec is None:
                    spec = record.market_spec
                elif spec != record.market_spec:
                    raise IntegrityError("baseline-complete market economics changed")
            if current_session is None:
                current_session = record.exchange_session_date
            elif record.exchange_session_date != current_session:
                if record.exchange_session_date < current_session:
                    raise IntegrityError("baseline-complete sessions are not chronological")
                flush()
                current_session = record.exchange_session_date
            buffer.append(record)
            if len(buffer) > maximum_session_rows:
                raise IntegrityError("baseline-complete session exceeds its memory bound")
        flush()
        if spec is None:
            raise IntegrityError("baseline-complete market-year lacks economics")
        for session in sorted(set(by_session) - seen):
            rows, _ = materialize_mandatory_baseline_rows(
                source_rows=(), census=tuple(by_session[session]),
                market_specs={market: spec}, contract=contract,
                prediction_scope_sessions=prediction_scope_sessions,
            )
            all_rows.extend(rows)
    all_rows.sort(key=lambda row: (
        row.expected.exchange_session_date,
        row.expected.checkpoint,
        v5.MARKETS.index(row.expected.market),
    ))
    v5.reconcile_v5_opportunity_ledger(
        expected_ids=[item.expected.opportunity_id for item in census],
        records=[item.ledger for item in all_rows],
    )
    return tuple(all_rows), dict(sorted(all_resolutions.items()))


@dataclass(frozen=True)
class MandatoryBaselineEvaluation:
    decision: Mapping[str, object]
    complete_decision_reached: bool
    selected_path_coverage: Mapping[str, Mapping[str, object]]
    outer_completeness: Mapping[str, object]
    nested_selected_path_coverage: Mapping[str, Mapping[str, object]]
    nested_completeness: Mapping[str, object]


def evaluate_mandatory_baseline_pipeline(
    *, streams: Mapping[tuple[str, int], Iterable[v5.V5SourceRecord]],
    census: Sequence[v5.CensusCheckpoint], contract: Mapping[str, object],
    trial_id: str,
) -> MandatoryBaselineEvaluation:
    """Run the future promotion lattice without provider or publication actions."""

    if not v5._hex64(trial_id):
        raise IntegrityError("baseline-complete evaluation requires a trial identity")
    folds = v5.build_v5_folds_from_census(census)
    prediction_sessions = tuple(
        session for fold in folds for session in fold.test_sessions
    )
    materialized, resolutions = materialize_mandatory_baseline_streams(
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
        prediction_scope = set(prediction_sessions)
        calendar_open_ids = {
            item.expected.opportunity_id for item in census if item.calendar_open
        }
        evaluation_rows = [
            item for item in rows
            if item.expected.exchange_session_date in prediction_scope
        ]
        expected_by: dict[str, int] = {}
        features_by: dict[str, int] = {}
        for item in evaluation_rows:
            if item.expected.opportunity_id not in calendar_open_ids:
                continue
            key = f"{item.expected.market}/{item.expected.year}"
            expected_by[key] = expected_by.get(key, 0) + 1
            features_by[key] = features_by.get(key, 0) + int(
                item.features is not None
            )
        coverage = v5.CoverageEvidence(
            expected=len(evaluation_rows), terminal=len(evaluation_rows),
            causal_feature_expected=sum(
                item.expected.opportunity_id in calendar_open_ids
                for item in evaluation_rows
            ),
            causal_feature_eligible=sum(
                item.features is not None for item in evaluation_rows
            ),
            predictions=len(model.predictions),
            market_year_expected=expected_by,
            market_year_feature_eligible=features_by,
        )
        complete_decision = derive_v10_decision(
            evaluation=evaluation,
            evaluation_sessions=prediction_sessions,
            coverage=coverage,
            baseline_coverage=combined_baseline_coverage,
            crossfit=crossfit.controls,
            evaluation_completeness=completeness,
            seed=int(trial_id[:16], 16),
        )
    decision = derive_final_decision(
        evaluation=evaluation,
        selected_path_coverage=selected,
        complete_decision=complete_decision,
    )
    return MandatoryBaselineEvaluation(
        decision, complete_decision is not None, selected, completeness,
        crossfit.controls.selected_path_coverage,
        crossfit.controls.evaluation_completeness,
    )
