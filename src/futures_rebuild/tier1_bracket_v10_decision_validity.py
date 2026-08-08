"""Decision-validity controls prepared for the unregistered V10 successor.

This module intentionally does not alter or import historical results.  It
separates information known at the decision from outcome observability, ranks
trade intents before looking up future paths, and accepts a path only through
the first missing/non-executable minute.  A gap after a causal exit is
irrelevant; a gap before the exit leaves that outcome unresolved.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
import json
from pathlib import Path

from .errors import IntegrityError
from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v10 as v10
from .tier1_bracket_post_audit import CausalBar, cost_ticks
from .tier1_bracket_v4 import (
    BracketFill, DirectionOutcomes, FrozenPrediction, _strategy_signal,
    simulate_v4_bracket_fill,
)


SCENARIOS = ("base", "stress", "extreme")
DIRECTIONS = ("long", "short")


def load_decision_validity_contract_v10(
    *, root: Path,
) -> tuple[dict[str, object], dict[str, object]]:
    """Validate every decision-validity rule added after the source census."""

    inherited, contract = v10.load_v10_contract(root=root)
    try:
        raw = json.loads((root / v10.V10_CONTRACT).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("invalid V10 decision-validity contract") from exc
    source = raw.get("source_continuity_successor")
    decision = raw.get("decision_validity_successor")
    required_decision = {
        "prediction_eligibility": "CALENDAR_OPEN_OWNED_FOLD_WITH_CAUSAL_FEATURES_INDEPENDENT_OF_FUTURE_OUTCOME_AVAILABILITY",
        "risk_cap_rejection": "DECISION_TIME_POLICY_ABSTENTION_INCLUDED_AS_ZERO_POLICY_RETURN_NOT_MISSING_STATISTICAL_DATA",
        "missing_features": "TRUE_DECISION_DATA_INELIGIBILITY_INCLUDED_IN_COVERAGE_DENOMINATOR",
        "model_unavailable": "EXPLICIT_PRE_PREDICTION_ABSTENTION_INCLUDED_IN_MODEL_AVAILABILITY_GATE",
        "signal_risk_and_cross_market_ranking": "FROZEN_BEFORE_ANY_OUTCOME_LOOKUP",
        "missing_selected_outcome": "NO_RUNNER_UP_SUBSTITUTION_AND_INCONCLUSIVE_DATA_OR_COVERAGE",
        "policy_statistic": "EVERY_DECLARED_EVALUATION_SESSION_INCLUDED_WITH_ZERO_RETURN_FOR_INTENTIONAL_NO_TRADE",
        "gate_order": "DATA_AND_POLICY_COMPLETENESS_BEFORE_ANY_PERFORMANCE_INFERENCE",
        "required_active_baselines": "EACH_USES_ITS_OWN_CAUSAL_SIGNAL_RANKING_SELECTED_PATH_COVERAGE_SCHEDULE_COSTS_AND_RISK_STATE",
    }
    if (
        not isinstance(source, dict)
        or source.get("entry_and_outcome_completeness")
        != "ENTRY_AT_DECISION_PLUS_ONE_MINUTE_AND_EXACT_MINUTE_CONTIGUOUS_CAUSAL_PREFIX_THROUGH_DIRECTION_AND_SCENARIO_SPECIFIC_EXIT"
        or source.get("gap_before_causal_exit") != "SELECTED_OUTCOME_UNRESOLVED_FAIL_CLOSED"
        or source.get("gap_after_causal_exit") != "IRRELEVANT_TO_THE_ALREADY_PROVEN_FILL"
        or source.get("duplicate_event_identity")
        != "ONLY_CHECKPOINTS_WHOSE_EXACT_FEATURE_OR_ENTRY_OUTCOME_DEPENDENCIES_CONTAIN_THE_DUPLICATE_ARE_AMBIGUOUS_FAIL_CLOSED"
        or decision != required_decision
    ):
        raise IntegrityError("V10 decision-validity rules are incomplete or drifted")
    return inherited, contract


@dataclass(frozen=True)
class DirectionalOutcomeResolutionV10:
    fills: Mapping[tuple[str, str], BracketFill]
    contiguous_path: tuple[CausalBar, ...]
    first_unresolved_event_at_ns: int | None

    def fill(self, *, scenario: str, direction: str) -> BracketFill | None:
        return self.fills.get((scenario, direction))


def resolve_directional_outcomes_v10(
    *, path_bars: Sequence[CausalBar], decision_at_ns: int, atr: Decimal,
    spec: v5.MarketSpec, contract: Mapping[str, object], market: str,
) -> DirectionalOutcomeResolutionV10:
    """Resolve each direction/scenario from the causal minute prefix only."""

    if market not in v5.MARKETS or atr <= 0:
        raise IntegrityError("V10 outcome request is invalid")
    spec.validate()
    by_event: dict[int, list[CausalBar]] = {}
    for bar in path_bars:
        bar.validate()
        by_event.setdefault(bar.event_at_ns, []).append(bar)
    prefix: list[CausalBar] = []
    first_unresolved: int | None = None
    for offset in range(1, 62):
        expected = decision_at_ns + offset * v5.NS_PER_MINUTE
        matches = by_event.get(expected, [])
        if len(matches) != 1 or not matches[0].executable:
            first_unresolved = expected
            break
        bar = matches[0]
        prefix.append(bar)
    if not prefix:
        return DirectionalOutcomeResolutionV10({}, (), first_unresolved)
    entry = prefix[0]
    fills: dict[tuple[str, str], BracketFill] = {}
    fee = Decimal(str(contract["costs"]["fee_per_side_usd"]))  # type: ignore[index]
    for scenario in SCENARIOS:
        ticks = cost_ticks(contract=contract, scenario=scenario, market=market)
        for direction in DIRECTIONS:
            try:
                fill = simulate_v4_bracket_fill(
                    direction=direction, decision_at_ns=decision_at_ns,
                    entry_bar=entry, path_bars=prefix, atr=atr,
                    tick_size=spec.tick_size, tick_value=spec.tick_value,
                    point_value=spec.point_value, fee_per_side_usd=fee,
                    round_trip_cost_ticks=ticks,
                    maximum_planned_loss_usd=Decimal("250"),
                )
            except IntegrityError:
                continue
            if first_unresolved is not None and fill.exit_at_ns >= first_unresolved:
                raise IntegrityError("V10 outcome crossed an unresolved minute")
            fills[(scenario, direction)] = fill
    return DirectionalOutcomeResolutionV10(
        dict(sorted(fills.items())), tuple(prefix), first_unresolved,
    )


def prepare_crossfit_prediction_rows_v10(
    *, rows: Sequence[v5.MaterializedRowV5], owner_sessions: Sequence[str],
) -> tuple[v5.MaterializedRowV5, ...]:
    """Make prediction eligibility depend on decision-time features, not outcomes."""

    owners = set(owner_sessions)
    output: list[v5.MaterializedRowV5] = []
    for row in rows:
        if row.expected.exchange_session_date not in owners or row.features is None:
            output.append(row)
            continue
        if (
            row.ledger.feature_event_at_ns is None
            or row.ledger.feature_available_at_ns is None
            or row.ledger.feature_available_at_ns > row.expected.decision_at_ns
        ):
            raise IntegrityError("V10 prediction row lacks causal feature lineage")
        ledger = v5.OpportunityRecordV5(
            row.expected.opportunity_id, row.expected.market,
            row.expected.exchange_session_date, row.expected.checkpoint,
            row.expected.decision_at_ns, "PREDICTION_PRODUCED", True,
            row.ledger.feature_event_at_ns, row.ledger.feature_available_at_ns,
            outcome_coverage=row.ledger.outcome_coverage,
        )
        ledger.validate()
        output.append(replace(row, ledger=ledger))
    return tuple(output)


def attach_causal_outcomes_v10(
    *, rows: Sequence[v5.MaterializedRowV5], contract: Mapping[str, object],
) -> tuple[tuple[v5.MaterializedRowV5, ...], Mapping[str, DirectionalOutcomeResolutionV10]]:
    """Replace V5's full-window outcome check with direction-specific prefixes."""

    output: list[v5.MaterializedRowV5] = []
    resolutions: dict[str, DirectionalOutcomeResolutionV10] = {}
    for row in rows:
        if (
            row.features is None or row.atr is None or row.market_spec is None
            or not row.risk_eligible
        ):
            output.append(row)
            continue
        resolution = resolve_directional_outcomes_v10(
            path_bars=row.execution_path,
            decision_at_ns=row.expected.decision_at_ns,
            atr=row.atr, spec=row.market_spec, contract=contract,
            market=row.expected.market,
        )
        resolutions[row.expected.opportunity_id] = resolution
        outcomes: dict[str, DirectionOutcomes] = {}
        for scenario in SCENARIOS:
            long_fill = resolution.fill(scenario=scenario, direction="long")
            short_fill = resolution.fill(scenario=scenario, direction="short")
            if long_fill is not None and short_fill is not None:
                outcomes[scenario] = DirectionOutcomes(long_fill, short_fill)
        coverage = (
            "COMPLETE" if len(outcomes) == 3
            else "STRESS_COMPLETE_PARTIAL_DIAGNOSTICS" if "stress" in outcomes
            else "MISSING"
        )
        output.append(replace(
            row,
            ledger=replace(row.ledger, outcome_coverage=coverage),
            outcomes=(outcomes if "stress" in outcomes else None),
            execution_path=resolution.contiguous_path,
        ))
    return tuple(output), dict(sorted(resolutions.items()))


def materialize_checkpoint_scoped_rows_v10(
    *, source_rows: Sequence[v5.V5SourceRecord],
    census: Sequence[v5.CensusCheckpoint],
    market_specs: Mapping[str, v5.MarketSpec], contract: Mapping[str, object],
    prediction_scope_sessions: Sequence[str],
) -> tuple[v5.MaterializedRowV5, ...]:
    """Let only a checkpoint's exact dependencies affect that checkpoint."""

    for row in source_rows:
        row.validate()
    output: list[v5.MaterializedRowV5] = []
    for checkpoint in census:
        expected = checkpoint.expected
        raw = [
            row for row in source_rows
            if row.market == expected.market
            and row.exchange_session_date == expected.exchange_session_date
        ]
        if not checkpoint.calendar_open or not raw:
            output.extend(v5.materialize_v5_rows(
                source_rows=(), census=(checkpoint,), market_specs=market_specs,
                contract=contract,
                prediction_scope_sessions=prediction_scope_sessions,
            ))
            continue
        causal = [
            row for row in raw
            if row.executable and row.bar is not None
            and row.bar.available_at_ns <= expected.decision_at_ns
        ]
        if not causal:
            output.append(v5.MaterializedRowV5(
                expected,
                v5.OpportunityRecordV5(
                    expected.opportunity_id, expected.market,
                    expected.exchange_session_date, expected.checkpoint,
                    expected.decision_at_ns, "INSUFFICIENT_CAUSAL_HISTORY", False,
                ),
                None, None, None, None,
            ))
            continue
        latest_event = max(row.bar.event_at_ns for row in causal if row.bar is not None)
        feature_events = {
            latest_event - offset * v5.NS_PER_MINUTE for offset in range(61)
        }
        future_events = {
            expected.decision_at_ns + offset * v5.NS_PER_MINUTE
            for offset in range(1, 62)
        }
        scoped = tuple(
            row for row in raw
            if row.bar is not None and row.bar.event_at_ns in feature_events
        )
        materialized = v5.materialize_v5_rows(
            source_rows=scoped, census=(checkpoint,), market_specs=market_specs,
            contract=contract,
            prediction_scope_sessions=prediction_scope_sessions,
        )
        row = materialized[0]
        if row.features is not None and row.risk_eligible:
            future_path = tuple(sorted(
                (
                    item.bar for item in raw
                    if item.bar is not None and item.bar.event_at_ns in future_events
                ),
                key=lambda bar: bar.event_at_ns,
            ))
            row = replace(
                row, execution_path=future_path,
                market_spec=market_specs[expected.market],
            )
        output.append(row)
    if len(output) != len(census):
        raise IntegrityError("V10 checkpoint-scoped materialization did not reconcile")
    return tuple(output)


def materialize_checkpoint_scoped_streams_v10(
    *, streams: Mapping[tuple[str, int], Iterable[v5.V5SourceRecord]],
    census: Sequence[v5.CensusCheckpoint], contract: Mapping[str, object],
    prediction_scope_sessions: Sequence[str], maximum_session_rows: int = 2_000,
) -> tuple[v5.MaterializedRowV5, ...]:
    """Stream one session at a time and apply checkpoint-scoped ambiguity."""

    expected_keys = {
        (market, year) for market in v5.MARKETS for year in range(2018, 2023)
    }
    if set(streams) != expected_keys or maximum_session_rows != 2_000:
        raise IntegrityError("V10 source stream coverage or memory bound is invalid")
    census_by_key: dict[tuple[str, int], list[v5.CensusCheckpoint]] = {}
    for item in census:
        census_by_key.setdefault((item.expected.market, item.expected.year), []).append(item)
    if set(census_by_key) != expected_keys:
        raise IntegrityError("V10 calendar census lacks a market-year")
    output: list[v5.MaterializedRowV5] = []
    for key in sorted(expected_keys):
        market, year = key
        expected_by_session: dict[str, list[v5.CensusCheckpoint]] = {}
        for item in census_by_key[key]:
            expected_by_session.setdefault(item.expected.exchange_session_date, []).append(item)
        seen: set[str] = set()
        current_session: str | None = None
        buffer: list[v5.V5SourceRecord] = []
        spec: v5.MarketSpec | None = None
        prior_event = -1

        def flush() -> None:
            nonlocal buffer, current_session
            if current_session is None:
                return
            seen.add(current_session)
            session_census = expected_by_session.get(current_session)
            if session_census is not None:
                if spec is None:
                    raise IntegrityError("V10 source stream lacks market economics")
                output.extend(materialize_checkpoint_scoped_rows_v10(
                    source_rows=tuple(buffer), census=tuple(session_census),
                    market_specs={market: spec}, contract=contract,
                    prediction_scope_sessions=prediction_scope_sessions,
                ))
            buffer = []

        for record in streams[key]:
            record.validate()
            if (
                record.market != market
                or int(record.exchange_session_date[:4]) != year
            ):
                raise IntegrityError("V10 source stream row is outside its market-year binding")
            if record.bar is not None:
                if record.bar.event_at_ns < prior_event:
                    raise IntegrityError("V10 source stream is not chronological")
                prior_event = record.bar.event_at_ns
            if record.market_spec is not None:
                if spec is None:
                    spec = record.market_spec
                elif spec != record.market_spec:
                    raise IntegrityError("V10 market economics vary inside a source stream")
            if current_session is None:
                current_session = record.exchange_session_date
            elif record.exchange_session_date != current_session:
                if record.exchange_session_date < current_session:
                    raise IntegrityError("V10 source session labels are not chronological")
                flush()
                current_session = record.exchange_session_date
            buffer.append(record)
            if len(buffer) > maximum_session_rows:
                raise IntegrityError("V10 source session exceeds the registered memory bound")
        flush()
        if spec is None:
            raise IntegrityError("V10 source market-year contains no valid economics")
        for session in sorted(set(expected_by_session) - seen):
            output.extend(materialize_checkpoint_scoped_rows_v10(
                source_rows=(), census=tuple(expected_by_session[session]),
                market_specs={market: spec}, contract=contract,
                prediction_scope_sessions=prediction_scope_sessions,
            ))
    output.sort(key=lambda row: (
        row.expected.exchange_session_date, row.expected.checkpoint,
        v5.MARKETS.index(row.expected.market),
    ))
    v5.reconcile_v5_opportunity_ledger(
        expected_ids=[item.expected.opportunity_id for item in census],
        records=[item.ledger for item in output],
    )
    return tuple(output)


def evaluate_crossfit_decision_availability_v10(
    *, rows: Sequence[v5.MaterializedRowV5], owner_sessions: Sequence[str],
    unavailable_ids: Sequence[str],
) -> dict[str, object]:
    """Audit feature/model availability without censoring policy abstentions."""

    owners = set(owner_sessions)
    unavailable = set(unavailable_ids)
    if len(unavailable) != len(unavailable_ids):
        return {"status": "INVALID", "passed": False}
    scoped = [
        row for row in rows
        if row.expected.exchange_session_date in owners
        and row.ledger.terminal_disposition != "CALENDAR_CLOSED"
    ]
    by_id = {row.expected.opportunity_id: row for row in scoped}
    if not scoped or len(by_id) != len(scoped):
        return {"status": "INVALID", "passed": False}
    feature_ids = {row.expected.opportunity_id for row in scoped if row.features is not None}
    predicted_ids = {row.expected.opportunity_id for row in scoped if row.ledger.prediction_produced}
    if predicted_ids != feature_ids or not unavailable <= feature_ids:
        return {"status": "INVALID", "passed": False}
    expected = {market: 0 for market in v5.MARKETS}
    feature = {market: 0 for market in v5.MARKETS}
    model = {market: 0 for market in v5.MARKETS}
    risk_abstentions = {market: 0 for market in v5.MARKETS}
    for row in scoped:
        market = row.expected.market
        expected[market] += 1
        eligible = row.expected.opportunity_id in feature_ids
        feature[market] += int(eligible)
        model[market] += int(eligible and row.expected.opportunity_id not in unavailable)
        risk_abstentions[market] += int(eligible and not row.risk_eligible)
    if any(expected[market] <= 0 for market in v5.MARKETS):
        return {"status": "INVALID", "passed": False}
    feature_rates = {market: feature[market] / expected[market] for market in v5.MARKETS}
    model_rates = {
        market: model[market] / feature[market] if feature[market] else 0.0
        for market in v5.MARKETS
    }
    feature_overall = sum(feature.values()) / sum(expected.values())
    model_overall = sum(model.values()) / sum(feature.values()) if sum(feature.values()) else 0.0
    passed = (
        feature_overall >= 0.95 and min(feature_rates.values()) >= 0.90
        and model_overall >= 0.99 and min(model_rates.values()) >= 0.90
    )
    return {
        "status": "PASS" if passed else "INCONCLUSIVE_DATA_OR_POWER",
        "passed": passed,
        "calendar_open_expected_opportunities": sum(expected.values()),
        "decision_feature_eligible_opportunities": sum(feature.values()),
        "model_available_opportunities": sum(model.values()),
        "risk_cap_policy_abstentions": sum(risk_abstentions.values()),
        "overall_decision_feature_eligibility_rate": feature_overall,
        "market_decision_feature_eligibility_rates": dict(sorted(feature_rates.items())),
        "overall_model_availability_rate": model_overall,
        "market_model_availability_rates": dict(sorted(model_rates.items())),
        "risk_cap_policy_abstentions_by_market": dict(sorted(risk_abstentions.items())),
    }


def plan_strategy_rank_before_outcome_v10(
    *, strategy: str, predictions: Sequence[FrozenPrediction],
    rows: Sequence[v5.MaterializedRowV5], scenario: str,
    resolutions: Mapping[str, DirectionalOutcomeResolutionV10],
) -> v5.PlannedStrategyV5:
    """Freeze signal/risk/ranking first; never replace a winner with observable data."""

    if scenario not in SCENARIOS:
        raise IntegrityError("V10 scenario is invalid")
    rows_by_id = {row.expected.opportunity_id: row for row in rows}
    if len(rows_by_id) != len(rows):
        raise IntegrityError("V10 materialized rows are duplicated")
    if strategy == "flat_no_trade":
        return v5.PlannedStrategyV5(
            strategy, (), {item.opportunity_id: "FLAT_NO_TRADE" for item in predictions},
        )
    terminals: dict[str, str] = {}
    intents: dict[tuple[str, str], list[tuple[FrozenPrediction, str, Decimal]]] = {}
    for prediction in predictions:
        row = rows_by_id.get(prediction.opportunity_id)
        if row is None:
            raise IntegrityError("V10 prediction lacks its materialized row")
        signal = _strategy_signal(prediction, strategy)
        if signal is None:
            terminals[prediction.opportunity_id] = "HURDLE_FAILURE"
            continue
        direction, score = signal
        if direction not in DIRECTIONS or score is None:
            terminals[prediction.opportunity_id] = "BASELINE_TRAINING_COVERAGE_ABSTENTION"
            continue
        if not row.risk_eligible:
            terminals[prediction.opportunity_id] = "RISK_CAP_REJECTION"
            continue
        intents.setdefault((prediction.session, prediction.checkpoint), []).append(
            (prediction, direction, Decimal(str(score)))
        )
    trades: list[v5.PlannedTradeV5] = []
    for key in sorted(intents):
        ranked = sorted(
            intents[key], key=lambda item: (-item[2], v5.MARKETS.index(item[0].market)),
        )
        winner, direction, score = ranked[0]
        for loser, _, _ in ranked[1:]:
            terminals[loser.opportunity_id] = "CROSS_MARKET_RANKING_LOSS"
        row = rows_by_id[winner.opportunity_id]
        resolution = resolutions.get(winner.opportunity_id)
        fill = resolution.fill(scenario=scenario, direction=direction) if resolution else None
        if fill is None or resolution is None or row.market_spec is None:
            terminals[winner.opportunity_id] = "MISSING_PRICE_PATH"
            continue
        trades.append(v5.PlannedTradeV5(
            winner.opportunity_id, winner.market, winner.year, winner.session,
            winner.checkpoint, direction, score, fill,
            resolution.contiguous_path, row.market_spec,
        ))
    return v5.PlannedStrategyV5(strategy, tuple(trades), dict(sorted(terminals.items())))


def evaluate_selected_path_coverage_v10(
    *, plans: Mapping[str, v5.PlannedStrategyV5],
) -> dict[str, object]:
    """Fail closed when any policy-selected intent lacks its causal outcome."""

    missing = {
        strategy: sum(value == "MISSING_PRICE_PATH" for value in plan.preliminary_terminals.values())
        for strategy, plan in plans.items() if strategy != "flat_no_trade"
    }
    passed = bool(missing) and sum(missing.values()) == 0
    return {
        "status": "PASS" if passed else "INCONCLUSIVE_DATA_OR_COVERAGE",
        "passed": passed,
        "selected_intents_missing_causal_outcome": sum(missing.values()),
        "by_strategy": dict(sorted(missing.items())),
    }


def evaluate_strategies_v10(
    *, predictions: Sequence[FrozenPrediction], rows: Sequence[v5.MaterializedRowV5],
    resolutions: Mapping[str, DirectionalOutcomeResolutionV10],
    strategies: Sequence[str],
) -> tuple[
    Mapping[str, Mapping[str, v5.AccountPathV5]],
    Mapping[str, Mapping[str, object]],
]:
    """Evaluate independent paths, with selected-path completeness as a hard gate."""

    prediction_ids = tuple(item.opportunity_id for item in predictions)
    if len(prediction_ids) != len(set(prediction_ids)):
        raise IntegrityError("V10 frozen predictions are duplicated")
    evaluations: dict[str, Mapping[str, v5.AccountPathV5]] = {}
    coverage: dict[str, Mapping[str, object]] = {}
    for scenario in SCENARIOS:
        plans = {
            strategy: plan_strategy_rank_before_outcome_v10(
                strategy=strategy, predictions=predictions, rows=rows,
                scenario=scenario, resolutions=resolutions,
            )
            for strategy in strategies
        }
        coverage[scenario] = evaluate_selected_path_coverage_v10(plans=plans)
        paths = v5.simulate_independent_strategy_paths_v5(
            plans_by_strategy={name: plan.trades for name, plan in plans.items()},
            opportunity_ids_by_strategy={name: prediction_ids for name in plans},
        )
        reconciled: dict[str, v5.AccountPathV5] = {}
        for name, path in paths.items():
            terminals = dict(path.terminal_dispositions)
            for opportunity_id, disposition in plans[name].preliminary_terminals.items():
                if terminals[opportunity_id] == "NO_SIGNAL":
                    terminals[opportunity_id] = disposition
            reconciled[name] = v5.AccountPathV5(
                path.strategy, path.admitted, dict(sorted(terminals.items())),
                path.equity_marks, path.session_net_pnl_usd,
                path.ending_equity_usd, path.maximum_continuous_drawdown_usd,
                path.complete,
            )
        evaluations[scenario] = reconciled
    return evaluations, coverage


def evaluate_evaluation_completeness_v10(
    *, evaluation: Mapping[str, Mapping[str, v5.AccountPathV5]],
    selected_path_coverage: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    expected_scenarios = set(SCENARIOS)
    if set(evaluation) != expected_scenarios or set(selected_path_coverage) != expected_scenarios:
        return {"status": "INVALID", "passed": False}
    incomplete_paths = {
        f"{scenario}/{strategy}": not path.complete
        for scenario, paths in evaluation.items() for strategy, path in paths.items()
    }
    selected_missing = sum(
        int(item.get("selected_intents_missing_causal_outcome", 0))
        for item in selected_path_coverage.values()
    )
    passed = not any(incomplete_paths.values()) and selected_missing == 0
    return {
        "status": "PASS" if passed else "INCONCLUSIVE_DATA_OR_COVERAGE",
        "passed": passed,
        "selected_intents_missing_causal_outcome": selected_missing,
        "incomplete_account_paths": sorted(
            name for name, incomplete in incomplete_paths.items() if incomplete
        ),
    }
