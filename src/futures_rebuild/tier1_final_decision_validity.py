"""Final unversioned Tier 1 decision-validity mechanics.

This module is intentionally separate from every registered implementation.
It fixes scenario-specific risk abstentions and uses a preregisterable
three-way decision lattice: promote, reject on a fully observed mandatory
failure, or remain inconclusive.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal

from . import tier1_bracket_v5 as v5
from .errors import IntegrityError
from .canonical import sha256_json
from .tier1_bracket_post_audit import cost_ticks
from .tier1_bracket_v4 import FrozenPrediction, _strategy_signal, planned_initial_loss_usd
from .tier1_bracket_v10_decision_validity import (
    DirectionalOutcomeResolutionV10, evaluate_selected_path_coverage_v10,
)


SCENARIOS = ("base", "stress", "extreme")
MAXIMUM_PLANNED_LOSS_USD = Decimal("250")
MAXIMUM_CONTINUOUS_DRAWDOWN_USD = Decimal("1500")
STARTING_EQUITY_USD = Decimal("100000")


def scenario_planned_loss_usd(
    *, row: v5.MaterializedRowV5, scenario: str,
    contract: Mapping[str, object],
) -> Decimal:
    """Compute the decision-known risk cap separately for each cost scenario."""

    if scenario not in SCENARIOS or row.atr is None or row.market_spec is None:
        raise IntegrityError("scenario risk request lacks causal economics")
    fee = Decimal(str(contract["costs"]["fee_per_side_usd"]))  # type: ignore[index]
    return planned_initial_loss_usd(
        atr=row.atr,
        tick_size=row.market_spec.tick_size,
        tick_value=row.market_spec.tick_value,
        round_trip_cost_ticks=cost_ticks(
            contract=contract, scenario=scenario, market=row.expected.market,
        ),
        fee_per_side_usd=fee,
    )


def plan_final_strategy(
    *, strategy: str, predictions: Sequence[FrozenPrediction],
    rows: Sequence[v5.MaterializedRowV5], scenario: str,
    resolutions: Mapping[str, DirectionalOutcomeResolutionV10],
    contract: Mapping[str, object],
) -> v5.PlannedStrategyV5:
    """Rank only scenario-eligible intents; never call policy abstention missing data."""

    if scenario not in SCENARIOS:
        raise IntegrityError("final scenario is invalid")
    rows_by_id = {row.expected.opportunity_id: row for row in rows}
    if len(rows_by_id) != len(rows):
        raise IntegrityError("final materialized rows are duplicated")
    if strategy == "flat_no_trade":
        return v5.PlannedStrategyV5(
            strategy, (), {
                item.opportunity_id: "FLAT_NO_TRADE" for item in predictions
            },
        )
    terminals: dict[str, str] = {}
    intents: dict[tuple[str, str], list[tuple[FrozenPrediction, str, Decimal]]] = {}
    for prediction in predictions:
        row = rows_by_id.get(prediction.opportunity_id)
        if row is None:
            raise IntegrityError("final prediction lacks its materialized row")
        if row.features is None:
            terminals[prediction.opportunity_id] = "INPUT_COVERAGE_ABSTENTION"
            continue
        signal = _strategy_signal(prediction, strategy)
        if signal is None:
            terminals[prediction.opportunity_id] = "HURDLE_FAILURE"
            continue
        direction, score = signal
        if direction not in {"long", "short"} or score is None:
            terminals[prediction.opportunity_id] = "BASELINE_TRAINING_COVERAGE_ABSTENTION"
            continue
        if scenario_planned_loss_usd(
            row=row, scenario=scenario, contract=contract,
        ) > MAXIMUM_PLANNED_LOSS_USD:
            terminals[prediction.opportunity_id] = "RISK_CAP_REJECTION"
            continue
        intents.setdefault((prediction.session, prediction.checkpoint), []).append(
            (prediction, direction, Decimal(str(score)))
        )
    trades: list[v5.PlannedTradeV5] = []
    for key in sorted(intents):
        ranked = sorted(
            intents[key],
            key=lambda item: (-item[2], v5.MARKETS.index(item[0].market)),
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
            winner.opportunity_id, winner.market, winner.year,
            winner.session, winner.checkpoint, direction, score, fill,
            resolution.contiguous_path, row.market_spec,
        ))
    return v5.PlannedStrategyV5(
        strategy, tuple(trades), dict(sorted(terminals.items())),
    )


def evaluate_final_strategies(
    *, universes: Mapping[str, Sequence[FrozenPrediction]],
    rows: Sequence[v5.MaterializedRowV5],
    resolutions: Mapping[str, DirectionalOutcomeResolutionV10],
    contract: Mapping[str, object],
) -> tuple[
    Mapping[str, Mapping[str, v5.AccountPathV5]],
    Mapping[str, Mapping[str, object]],
]:
    """Give every strategy and scenario its own schedule and account path."""

    required = set(v5.REQUIRED_ACTIVE_STRATEGIES_V5)
    if set(universes) != required:
        raise IntegrityError("final strategy universes are incomplete")
    evaluations: dict[str, Mapping[str, v5.AccountPathV5]] = {}
    coverage: dict[str, Mapping[str, object]] = {}
    for scenario in SCENARIOS:
        plans = {
            strategy: plan_final_strategy(
                strategy=strategy, predictions=universes[strategy], rows=rows,
                scenario=scenario, resolutions=resolutions, contract=contract,
            )
            for strategy in v5.REQUIRED_ACTIVE_STRATEGIES_V5
        }
        coverage[scenario] = evaluate_selected_path_coverage_v10(plans=plans)
        paths = v5.simulate_independent_strategy_paths_v5(
            plans_by_strategy={name: plan.trades for name, plan in plans.items()},
            opportunity_ids_by_strategy={
                name: tuple(item.opportunity_id for item in universes[name])
                for name in plans
            },
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


def derive_final_decision(
    *, evaluation: Mapping[str, Mapping[str, v5.AccountPathV5]],
    selected_path_coverage: Mapping[str, Mapping[str, object]],
    complete_decision: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Apply the frozen promote/reject/inconclusive lattice.

    A promotion still requires every registered gate. A rejection is conclusive
    when the fully observed stress candidate independently fails net P&L or the
    drawdown ceiling. Missing data can never create a pass or a rejection.
    """

    stress_paths = evaluation.get("stress")
    stress_coverage = selected_path_coverage.get("stress")
    if not isinstance(stress_paths, Mapping) or not isinstance(stress_coverage, Mapping):
        raise IntegrityError("final decision lacks stress evidence")
    candidate = stress_paths.get("candidate")
    by_strategy = stress_coverage.get("by_strategy")
    if not isinstance(candidate, v5.AccountPathV5) or not isinstance(by_strategy, Mapping):
        raise IntegrityError("final decision lacks candidate evidence")
    candidate_missing = by_strategy.get("candidate")
    candidate_observed = (
        candidate.complete and type(candidate_missing) is int and candidate_missing == 0
    )
    net_pnl = candidate.ending_equity_usd - STARTING_EQUITY_USD
    failed: list[str] = []
    if candidate_observed and net_pnl <= 0:
        failed.append("STRESS_NET_PNL_NOT_POSITIVE")
    if (
        candidate_observed
        and candidate.maximum_continuous_drawdown_usd
        > MAXIMUM_CONTINUOUS_DRAWDOWN_USD
    ):
        failed.append("CONTINUOUS_DRAWDOWN_EXCEEDS_1500_USD")
    if failed:
        core = {
            "schema_version": "tier1_final_decision/1.0.0",
            "classification": "REJECT_HISTORICAL_SCREEN_MANDATORY_GATE",
            "inference_executed": True,
            "candidate_selected_path_complete": True,
            "failed_mandatory_gates": failed,
            "stress_candidate_net_pnl_usd": str(net_pnl),
            "stress_candidate_maximum_continuous_drawdown_usd": str(
                candidate.maximum_continuous_drawdown_usd
            ),
            "missing_data_helped_decision": False,
            "promotion_possible": False,
        }
        return {**core, "decision_id": sha256_json(core)}
    all_coverage_pass = all(
        isinstance(item, Mapping) and item.get("passed") is True
        for item in selected_path_coverage.values()
    )
    if not candidate_observed or not all_coverage_pass or complete_decision is None:
        core = {
            "schema_version": "tier1_final_decision/1.0.0",
            "classification": "INCONCLUSIVE_DATA_OR_COVERAGE",
            "inference_executed": False,
            "candidate_selected_path_complete": candidate_observed,
            "failed_mandatory_gates": [],
            "missing_data_helped_decision": False,
            "promotion_possible": False,
        }
        return {**core, "decision_id": sha256_json(core)}
    if complete_decision.get("classification") not in {
        "PASS_HISTORICAL_SCREEN", "FAIL_PROMOTION_GATE",
        "FAIL_MULTIPLICITY_OR_CONTROL", "FAIL_NO_EDGE",
    }:
        raise IntegrityError("complete final decision has an invalid classification")
    core = {
        **complete_decision,
        "schema_version": "tier1_final_decision/1.0.0",
        "candidate_selected_path_complete": True,
        "missing_data_helped_decision": False,
    }
    core.pop("decision_id", None)
    return {**core, "decision_id": sha256_json(core)}
