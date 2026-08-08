"""Independent causal opportunity universes for every required baseline."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from .canonical import sha256_file
from .errors import IntegrityError
from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v8 as v8
from . import tier1_bracket_v9 as v9
from .tier1_bracket_v10_decision_validity import (
    DirectionalOutcomeResolutionV10, evaluate_selected_path_coverage_v10,
    plan_strategy_rank_before_outcome_v10,
)


V10_TRIAL_ID = "53c9aa144b38187ea93acbfbe2ea10f6c76fcb382edef01a5254b3ecda38ddc3"
V11_CONTRACT = Path("configs/tier1_bracket_successor_v11.json")


def load_v11_contract(*, root: Path) -> tuple[dict[str, object], dict[str, object]]:
    try:
        delta = json.loads((root / V11_CONTRACT).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("invalid V11 contract JSON") from exc
    rule = delta.get("independent_baseline_universe_successor")
    authority = delta.get("authority")
    if (
        not isinstance(delta, dict)
        or delta.get("schema_version") != "tier1_bracket_successor_v11_contract/1.0.0"
        or delta.get("state") != "PREPARED_NOT_REGISTERED"
        or delta.get("supersedes_v10_trial_id") != V10_TRIAL_ID
        or delta.get("inherited_v10_contract_path") != "configs/tier1_bracket_successor_v10.json"
        or sha256_file(root / "configs/tier1_bracket_successor_v10.json")
        != delta.get("inherited_v10_contract_sha256")
        or not isinstance(rule, dict)
        or rule.get("candidate_model_unavailable")
        != "CANNOT_CENSOR_ANY_MODEL_INDEPENDENT_BASELINE"
        or rule.get("coverage")
        != "EACH_REQUIRED_BASELINE_HAS_ITS_OWN_DECLARED_DENOMINATOR_EXPLICIT_TERMINAL_DISPOSITION_ELIGIBILITY_METRICS_AND_HARD_GATE"
        or not isinstance(authority, dict)
        or authority.get("publication_requires_separate_approval") is not True
        or authority.get("holdout_or_forward_access") is not False
    ):
        raise IntegrityError("V11 independent-baseline contract is incomplete or drifted")
    from .tier1_bracket_v10_decision_validity import load_decision_validity_contract_v10
    inherited, _ = load_decision_validity_contract_v10(root=root)
    return inherited, delta


@dataclass(frozen=True)
class StrategyPredictionUniversesV11:
    predictions: Mapping[str, tuple[v8.FrozenPredictionV8, ...]]


def _dummy_prediction(
    *, row: v5.MaterializedRowV5, fold: int,
    fold_local_direction: str | None = None,
    fold_local_score: float | None = None,
) -> v8.FrozenPredictionV8:
    bar_return = float(row.features["bar_return_1"]) if row.features is not None else 0.0
    return v8.FrozenPredictionV8(
        row.expected.opportunity_id, row.expected.market, row.expected.year,
        row.expected.exchange_session_date, row.expected.checkpoint, fold,
        0.0, 0.0, "neutral", 0.0,
        fold_local_direction, fold_local_score, bar_return,
    )


def build_strategy_prediction_universes_v11(
    *, model: v9.ModelFitResultV9, rows: Sequence[v5.MaterializedRowV5],
    folds: Sequence[object],
) -> StrategyPredictionUniversesV11:
    owners: dict[str, int] = {}
    for fold in folds:
        fold_id = int(getattr(fold, "outer_fold"))
        for session in getattr(fold, "test_sessions"):
            if session in owners:
                raise IntegrityError("V11 test session belongs to multiple folds")
            owners[str(session)] = fold_id
    candidate = tuple(model.predictions)
    candidate_ids = {item.opportunity_id for item in candidate}
    if len(candidate_ids) != len(candidate):
        raise IntegrityError("V11 candidate predictions are duplicated")
    metadata = model.canonical_model_payload.get("models")
    if not isinstance(metadata, list):
        raise IntegrityError("V11 model metadata is absent")
    fold_local: dict[tuple[int, str, str], tuple[str | None, float | None]] = {}
    for item in metadata:
        if not isinstance(item, dict):
            raise IntegrityError("V11 model cell metadata is invalid")
        fold_id, market = int(item["outer_fold"]), str(item["market"])
        values = item.get("fold_local_unconditional")
        for checkpoint in v5.CHECKPOINTS:
            direction: str | None = None
            score: float | None = None
            if isinstance(values, dict) and isinstance(values.get(checkpoint), dict):
                cell = values[checkpoint]
                raw_direction, raw_score = cell.get("direction"), cell.get("score")
                if raw_direction in {"long", "short"} and raw_score is not None:
                    direction, score = str(raw_direction), float(raw_score)
            fold_local[(fold_id, market, checkpoint)] = (direction, score)
    scoped = [
        row for row in rows if row.expected.exchange_session_date in owners
    ]
    flat: list[v8.FrozenPredictionV8] = []
    feature_based: list[v8.FrozenPredictionV8] = []
    unconditional: list[v8.FrozenPredictionV8] = []
    for row in scoped:
        fold = owners[row.expected.exchange_session_date]
        flat.append(_dummy_prediction(row=row, fold=fold))
        feature_based.append(_dummy_prediction(row=row, fold=fold))
        direction, score = fold_local.get(
            (fold, row.expected.market, row.expected.checkpoint), (None, None)
        )
        unconditional.append(_dummy_prediction(
            row=row, fold=fold, fold_local_direction=direction,
            fold_local_score=score,
        ))
    by_strategy = {
        "candidate": candidate,
        "flat_no_trade": tuple(flat),
        "fold_local_unconditional_return_by_market_session": tuple(unconditional),
        "previous_bar_sign_momentum": tuple(feature_based),
        "previous_bar_sign_reversal": tuple(feature_based),
        "risk_matched_always_long_intraday": tuple(feature_based),
        "candidate_signal_market_order_ranking_ablation": candidate,
    }
    if set(by_strategy) != set(v5.REQUIRED_ACTIVE_STRATEGIES_V5):
        raise IntegrityError("V11 strategy universe set is incomplete")
    return StrategyPredictionUniversesV11(by_strategy)


def evaluate_required_baseline_coverage_v11(
    *, rows: Sequence[v5.MaterializedRowV5], folds: Sequence[object],
    universes: StrategyPredictionUniversesV11,
) -> dict[str, object]:
    owners = {
        str(session)
        for fold in folds for session in getattr(fold, "test_sessions")
    }
    expected_rows = [
        row for row in rows
        if row.expected.exchange_session_date in owners
        and row.ledger.terminal_disposition != "CALENDAR_CLOSED"
    ]
    years = {row.expected.year for row in expected_rows}
    required_keys = {f"{market}/{year}" for market in v5.MARKETS for year in years}
    expected = {key: 0 for key in required_keys}
    for row in expected_rows:
        expected[f"{row.expected.market}/{row.expected.year}"] += 1
    if not expected_rows or any(value <= 0 for value in expected.values()):
        return {"status": "INVALID", "passed": False}
    results: dict[str, object] = {}
    passed = True
    rows_by_id = {row.expected.opportunity_id: row for row in expected_rows}
    candidate_ids = {
        item.opportunity_id for item in universes.predictions["candidate"]
    }
    for strategy in v5.REQUIRED_ACTIVE_STRATEGIES_V5[1:]:
        universe = universes.predictions[strategy]
        universe_ids = {item.opportunity_id for item in universe}
        denominator_ids = (
            candidate_ids
            if strategy == "candidate_signal_market_order_ranking_ablation"
            else set(rows_by_id)
        )
        ids = set()
        for item in universe:
            row = rows_by_id.get(item.opportunity_id)
            if row is None or row.features is None:
                continue
            if (
                strategy == "fold_local_unconditional_return_by_market_session"
                and not (
                    item.fold_local_direction in {"long", "short"}
                    and item.fold_local_score is not None
                )
            ):
                continue
            ids.add(item.opportunity_id)
        if not denominator_ids or not denominator_ids <= set(rows_by_id):
            return {"status": "INVALID", "passed": False}
        eligible = {key: 0 for key in required_keys}
        strategy_expected = {key: 0 for key in required_keys}
        for row in expected_rows:
            key = f"{row.expected.market}/{row.expected.year}"
            if row.expected.opportunity_id in denominator_ids:
                strategy_expected[key] += 1
            if row.expected.opportunity_id in ids:
                eligible[key] += 1
        populated = {key for key, value in strategy_expected.items() if value > 0}
        if not populated or not denominator_ids <= universe_ids:
            return {"status": "INVALID", "passed": False}
        rates = {
            key: eligible[key] / strategy_expected[key] for key in sorted(populated)
        }
        overall = sum(eligible.values()) / sum(strategy_expected.values())
        strategy_pass = overall >= 0.95 and min(rates.values()) >= 0.90
        passed &= strategy_pass
        results[strategy] = {
            "status": "PASS" if strategy_pass else "INCONCLUSIVE_DATA_OR_COVERAGE",
            "expected": sum(strategy_expected.values()), "eligible": sum(eligible.values()),
            "overall_rate": overall, "market_year_rates": rates,
        }
    return {
        "status": "PASS" if passed else "INCONCLUSIVE_DATA_OR_COVERAGE",
        "passed": passed, "strategies": results,
    }


def plan_independent_strategy_v11(
    *, strategy: str, predictions: Sequence[v8.FrozenPredictionV8],
    rows: Sequence[v5.MaterializedRowV5], scenario: str,
    resolutions: Mapping[str, DirectionalOutcomeResolutionV10],
) -> v5.PlannedStrategyV5:
    """Retain every declared opportunity while only signaling on causal inputs."""

    if strategy in {"candidate", "candidate_signal_market_order_ranking_ablation"}:
        return plan_strategy_rank_before_outcome_v10(
            strategy=strategy, predictions=predictions, rows=rows,
            scenario=scenario, resolutions=resolutions,
        )
    if strategy == "flat_no_trade":
        return plan_strategy_rank_before_outcome_v10(
            strategy=strategy, predictions=predictions, rows=rows,
            scenario=scenario, resolutions=resolutions,
        )
    rows_by_id = {row.expected.opportunity_id: row for row in rows}
    eligible: list[v8.FrozenPredictionV8] = []
    terminals: dict[str, str] = {}
    for prediction in predictions:
        row = rows_by_id.get(prediction.opportunity_id)
        if row is None:
            raise IntegrityError("V11 baseline opportunity lacks its materialized row")
        if row.ledger.terminal_disposition == "CALENDAR_CLOSED":
            terminals[prediction.opportunity_id] = "CALENDAR_CLOSED"
        elif row.features is None:
            terminals[prediction.opportunity_id] = "BASELINE_INPUT_COVERAGE_ABSTENTION"
        else:
            eligible.append(prediction)
    planned = plan_strategy_rank_before_outcome_v10(
        strategy=strategy, predictions=tuple(eligible), rows=rows,
        scenario=scenario, resolutions=resolutions,
    )
    overlap = set(terminals) & set(planned.preliminary_terminals)
    if overlap:
        raise IntegrityError("V11 baseline terminal dispositions overlap")
    terminals.update(planned.preliminary_terminals)
    return v5.PlannedStrategyV5(
        strategy, planned.trades, dict(sorted(terminals.items())),
    )


def segmented_account_views_v11(
    *, plan: v5.PlannedStrategyV5,
    opportunity_market_year: Mapping[str, tuple[str, int]],
) -> Mapping[str, v5.AccountPathV5]:
    """Reset every observed market-year and retain its full terminal ledger."""

    trade_by_id = {trade.opportunity_id: trade for trade in plan.trades}
    if not set(trade_by_id) <= set(opportunity_market_year):
        raise IntegrityError("V11 segmented plan is outside its declared universe")
    identities = sorted(
        set(opportunity_market_year.values()),
        key=lambda item: (v5.MARKETS.index(item[0]), item[1]),
    )
    result: dict[str, v5.AccountPathV5] = {}
    for market, year in identities:
        ids = tuple(
            opportunity_id
            for opportunity_id, identity in opportunity_market_year.items()
            if identity == (market, year)
        )
        path = v5.simulate_account_path_v5(
            strategy=f"{plan.strategy}:{market}/{year}",
            planned_trades=tuple(
                trade_by_id[opportunity_id]
                for opportunity_id in ids if opportunity_id in trade_by_id
            ),
            all_opportunity_ids=ids,
        )
        terminals = dict(path.terminal_dispositions)
        for opportunity_id in ids:
            disposition = plan.preliminary_terminals.get(opportunity_id)
            if disposition is not None and terminals[opportunity_id] == "NO_SIGNAL":
                terminals[opportunity_id] = disposition
        result[f"{market}/{year}"] = v5.AccountPathV5(
            path.strategy, path.admitted, dict(sorted(terminals.items())),
            path.equity_marks, path.session_net_pnl_usd,
            path.ending_equity_usd, path.maximum_continuous_drawdown_usd,
            path.complete,
        )
    return result


def evaluate_independent_strategies_v11(
    *, universes: StrategyPredictionUniversesV11,
    rows: Sequence[v5.MaterializedRowV5],
    resolutions: Mapping[str, DirectionalOutcomeResolutionV10],
) -> tuple[
    Mapping[str, Mapping[str, v5.AccountPathV5]],
    Mapping[str, Mapping[str, object]],
]:
    evaluations: dict[str, Mapping[str, v5.AccountPathV5]] = {}
    coverage: dict[str, Mapping[str, object]] = {}
    for scenario in ("base", "stress", "extreme"):
        plans = {
            strategy: plan_independent_strategy_v11(
                strategy=strategy, predictions=universes.predictions[strategy],
                rows=rows, scenario=scenario, resolutions=resolutions,
            )
            for strategy in v5.REQUIRED_ACTIVE_STRATEGIES_V5
        }
        coverage[scenario] = evaluate_selected_path_coverage_v10(plans=plans)
        paths = v5.simulate_independent_strategy_paths_v5(
            plans_by_strategy={name: plan.trades for name, plan in plans.items()},
            opportunity_ids_by_strategy={
                name: tuple(item.opportunity_id for item in universes.predictions[name])
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
