"""One-shot execution for the preregistered Tier 1 bracket successor v2.

The pure functions in this module are synthetic-testable.  Repository I/O is
isolated in ``execute_registered_successor_v2`` and fails closed unless the
registered declaration and a create-only implementation binding are intact.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from statistics import fmean, pstdev
from typing import Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import DataReleaseManifest, DataReleaseReceipt, PhasePublisher
from .errors import IntegrityError
from .foundation.materialize import load_causal_interval
from .tier1_bracket_evaluation import ALL_BASELINES, REQUIRED_BASELINES, ZERO_METRICS, _metric_payload
from .tier1_bracket_interval_resolver import BracketIntervalBinding, classify_source_disposition, load_verified_interval_economics
from .tier1_bracket_materializer import IndexedBracketEconomics, VerifiedSourceBar, indexed_bracket_economics_from_registry
from .tier1_bracket_scheduler import BracketScheduleCandidate, schedule_bracket_candidates
from .tier1_bracket_successor_v2 import verify_successor_v2_registration
from .tier1_bracket_trial import build_directional_bracket_outcome, wilder_atr_nano


TRIAL_ID = "aab8134537f5f6efa9d9ced5603adb89212d43e67282ab5a2ab7e3adb3fd011c"
CLOSURE_ID = "105016f9d7fa3c30545da3bc6df76552aa9c0d8914c230080c5e3a444b2dc834"
MARKETS = ("ES", "CL", "ZN", "6E")
YEARS = tuple(range(2018, 2023))
EVALUATION_YEARS = (2020, 2021, 2022)
CHECKPOINTS = ("08:30", "10:30", "13:30")
CHICAGO = ZoneInfo("America/Chicago")
NS_PER_MINUTE = 60_000_000_000
FEATURE_NAMES = (
    "bar_return_1", "return_5", "return_20", "intrabar_range_fraction",
    "atr_20_fraction", "range_to_atr_20", "realized_volatility_20",
    "log1p_volume", "volume_zscore_60", "session_minute_sin", "session_minute_cos",
)
STRESS_COST_USD = {
    "ES": Decimal("53.10"), "CL": Decimal("83.34"),
    "ZN": Decimal("67.50"), "6E": Decimal("28.54"),
}
IMPLEMENTATION_BINDING_ROOT = Path("state/trial_registry/tier1_bracket_successor_v2_execution")
DECISION_EVENT_ROOT = Path("state/trial_events/tier1_bracket_successor_v2_evaluation")


@dataclass(frozen=True)
class IntervalSource:
    market: str
    year: int
    causal_release_id: str
    economics_release_id: str
    interval_key: str
    payload: Path
    economics: Mapping[str, IndexedBracketEconomics]


@dataclass(frozen=True)
class StrategyOpportunity:
    market: str
    year: int
    session: str
    checkpoint: str
    entry_at_ns: int
    direction: str
    exit_at_ns: int
    ranking_score: float
    risk: Decimal
    gross_pnl: Decimal
    tick_value: Decimal


def _finite(value: float, *, name: str) -> float:
    if not isinstance(value, float) or not math.isfinite(value):
        raise IntegrityError(f"successor {name} is not finite")
    return value


def _checkpoint_ns(session: str, checkpoint: str) -> int:
    try:
        local_date = date.fromisoformat(session)
        hour, minute = (int(item) for item in checkpoint.split(":"))
        stamp = datetime.combine(local_date, time(hour, minute), tzinfo=CHICAGO)
    except (TypeError, ValueError) as exc:
        raise IntegrityError("successor checkpoint identity is invalid") from exc
    return int(stamp.astimezone(timezone.utc).timestamp() * 1_000_000_000)


def _session_minute(stamp_ns: int) -> int:
    local = datetime.fromtimestamp(stamp_ns / 1_000_000_000, tz=timezone.utc).astimezone(CHICAGO)
    roll_date = local.date() if local.time() >= time(17, 0) else local.date() - timedelta(days=1)
    roll = datetime.combine(roll_date, time(17, 0), tzinfo=CHICAGO)
    minute = int((local - roll).total_seconds() // 60)
    if minute < 0 or minute >= 1440:
        raise IntegrityError("successor session minute is outside one session")
    return minute


def _causal_features(sources: Sequence[VerifiedSourceBar], index: int) -> dict[str, float] | None:
    if index < 60:
        return None
    window = sources[index - 60 : index + 1]
    current = sources[index]
    if (
        not current.bar.eligible
        or any(
            not item.bar.eligible
            or item.bar.session != current.bar.session
            or item.bar.actual_identity_hash != current.bar.actual_identity_hash
            for item in window
        )
        or any(window[pos].bar.event_at_ns - window[pos - 1].bar.event_at_ns != NS_PER_MINUTE for pos in range(1, len(window)))
    ):
        return None
    closes = [float(item.bar.close_nano) for item in window]
    volumes = [float(item.volume) for item in window[-60:]]
    if any(not math.isfinite(value) or value < 0 for value in volumes):
        return None
    volume_std = pstdev(volumes)
    if volume_std == 0:
        return None
    atr_window = tuple(item.bar for item in window)
    atr = wilder_atr_nano(bars=atr_window, decision_index=len(atr_window) - 1)
    if atr is None or atr <= 0:
        return None
    log_returns = [math.log(closes[pos] / closes[pos - 1]) for pos in range(len(closes) - 20, len(closes))]
    minute = _session_minute(current.bar.event_at_ns)
    angle = 2.0 * math.pi * minute / 1440.0
    current_range = current.bar.high_nano - current.bar.low_nano
    values = {
        "bar_return_1": closes[-1] / closes[-2] - 1.0,
        "return_5": closes[-1] / closes[-6] - 1.0,
        "return_20": closes[-1] / closes[-21] - 1.0,
        "intrabar_range_fraction": current_range / float(current.bar.open_nano),
        "atr_20_fraction": float(atr) / closes[-1],
        "range_to_atr_20": current_range / float(atr),
        "realized_volatility_20": pstdev(log_returns),
        "log1p_volume": math.log1p(current.volume),
        "volume_zscore_60": (current.volume - fmean(volumes)) / volume_std,
        "session_minute_sin": math.sin(angle),
        "session_minute_cos": math.cos(angle),
    }
    if tuple(values) != FEATURE_NAMES or any(not math.isfinite(value) for value in values.values()):
        raise IntegrityError("successor causal feature vector is invalid")
    return values


def materialize_successor_market_year(
    *, rows: Sequence[Mapping[str, object]], market: str, year: int,
    economics: Mapping[str, IndexedBracketEconomics], stress_cost_usd: Decimal,
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    """Create checkpoint-only causal features and paired stress-net labels."""

    if market not in MARKETS or year not in YEARS or stress_cost_usd != STRESS_COST_USD[market]:
        raise IntegrityError("successor market-year scope or stress cost differs from registration")
    sources = tuple(VerifiedSourceBar.from_mapping(row, indexed_economics=economics) for row in rows)
    if not sources:
        raise IntegrityError("successor source interval is empty")
    if any(sources[pos].bar.event_at_ns <= sources[pos - 1].bar.event_at_ns for pos in range(1, len(sources))):
        raise IntegrityError("successor source timestamps are not strictly increasing")
    hashes = [item.source_row_sha256 for item in sources]
    if len(hashes) != len(set(hashes)):
        raise IntegrityError("successor source hashes are ambiguous")
    by_session: dict[str, list[int]] = defaultdict(list)
    for index, source in enumerate(sources):
        if source.bar.eligible and source.bar.session.startswith(str(year)):
            by_session[source.bar.session].append(index)
    feature_rows: list[dict[str, object]] = []
    outcome_rows: list[dict[str, object]] = []
    observed: set[tuple[str, str]] = set()
    for session, indices in sorted(by_session.items()):
        for checkpoint in CHECKPOINTS:
            target = _checkpoint_ns(session, checkpoint)
            eligible = [index for index in indices if sources[index].bar.event_at_ns <= target]
            if not eligible:
                continue
            index = eligible[-1]
            decision = sources[index]
            if target - decision.bar.event_at_ns > NS_PER_MINUTE:
                continue
            features = _causal_features(sources, index)
            if features is None or index + 1 >= len(sources):
                continue
            path = tuple(item.bar for item in sources[index - 60 : index + 61])
            if len(path) != 121:
                continue
            long = build_directional_bracket_outcome(
                bars=path, decision_index=60, direction="long",
                tick_size_nano=decision.tick_size_nano, tick_value_usd=decision.tick_value_usd,
                stress_round_trip_cost_usd=stress_cost_usd,
            )
            short = build_directional_bracket_outcome(
                bars=path, decision_index=60, direction="short",
                tick_size_nano=decision.tick_size_nano, tick_value_usd=decision.tick_value_usd,
                stress_round_trip_cost_usd=stress_cost_usd,
            )
            if long.status != "MATURED" or short.status != "MATURED":
                continue
            if long.entry_at_ns != short.entry_at_ns or type(long.entry_at_ns) is not int:
                raise IntegrityError("successor directional labels disagree on entry time")
            identity = (session, checkpoint)
            if identity in observed:
                raise IntegrityError("successor checkpoint opportunity is ambiguous")
            observed.add(identity)
            common = {
                "market": market, "year": year, "exchange_session_date": session,
                "checkpoint": checkpoint, "checkpoint_at_ns": target,
                "decision_at_ns": decision.bar.event_at_ns,
                "actual_identity_hash": decision.bar.actual_identity_hash,
                "upstream_source_row_sha256": decision.source_row_sha256,
            }
            feature_rows.append({**common, **features})
            outcome_rows.append({
                **common, "entry_at_ns": long.entry_at_ns, "tick_value_usd": str(decision.tick_value_usd),
                "long_realized_net_r": str(long.realized_net_r), "short_realized_net_r": str(short.realized_net_r),
                "long_planned_all_in_risk_usd": str(long.planned_all_in_risk_usd),
                "short_planned_all_in_risk_usd": str(short.planned_all_in_risk_usd),
                "long_realized_gross_pnl_usd": str(long.realized_gross_pnl_usd),
                "short_realized_gross_pnl_usd": str(short.realized_gross_pnl_usd),
                "long_exit_at_ns": long.exit_at_ns, "short_exit_at_ns": short.exit_at_ns,
                "long_exit_reason": long.exit_reason, "short_exit_reason": short.exit_reason,
                "stress_round_trip_cost_usd": str(stress_cost_usd),
            })
    if not feature_rows or len(feature_rows) != len(outcome_rows):
        raise IntegrityError("successor market-year has no aligned checkpoint rows")
    return tuple(feature_rows), tuple(outcome_rows)


def build_successor_split_plan(feature_rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    sessions = sorted({str(row["exchange_session_date"]) for row in feature_rows})
    first = 504 + 1 + 4 * 42 + 1
    folds: list[dict[str, object]] = []
    for number in range(8):
        start, stop = first + number * 63, first + (number + 1) * 63
        if stop > len(sessions):
            raise IntegrityError("successor history cannot support eight registered folds")
        folds.append({
            "outer_fold": number,
            "fit_session_dates": [sessions[0], sessions[start - 2]],
            "test_session_dates": [sessions[start], sessions[stop - 1]],
            "purge_horizon_minutes": 60, "embargo_sessions": 1,
        })
    core = {
        "schema_version": "tier1_bracket_successor_v2_splits/1.0.0", "trial_id": TRIAL_ID,
        "session_dates": sessions, "outer_folds": folds, "holdout_excluded": "2025",
    }
    return {**core, "split_plan_id": sha256_json(core)}


def fit_successor_models(
    *, feature_rows: Sequence[Mapping[str, object]], outcome_rows: Sequence[Mapping[str, object]],
    split_plan: Mapping[str, object],
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    """Fit market-specific, fold-local standardized Ridge models and freeze predictions."""

    import numpy as np

    outcomes = {str(row["upstream_source_row_sha256"]): row for row in outcome_rows}
    if len(outcomes) != len(outcome_rows):
        raise IntegrityError("successor labels are ambiguous")
    models: list[dict[str, object]] = []
    predictions: list[dict[str, object]] = []
    folds = split_plan.get("outer_folds")
    if not isinstance(folds, list) or len(folds) != 8:
        raise IntegrityError("successor split plan is incomplete")
    for fold in folds:
        if not isinstance(fold, Mapping):
            raise IntegrityError("successor fold is invalid")
        number = int(fold["outer_fold"])
        fit_start, fit_end = fold["fit_session_dates"]
        test_start, test_end = fold["test_session_dates"]
        for market in MARKETS:
            training = [row for row in feature_rows if row["market"] == market and fit_start <= row["exchange_session_date"] <= fit_end]
            testing = [row for row in feature_rows if row["market"] == market and test_start <= row["exchange_session_date"] <= test_end]
            if not training or not testing:
                raise IntegrityError("successor market-fold lacks training or test coverage")
            x = np.asarray([[float(row[name]) for name in FEATURE_NAMES] for row in training], dtype=float)
            mean, scale = x.mean(axis=0), x.std(axis=0, ddof=0)
            if not np.isfinite(x).all() or not np.isfinite(mean).all() or not np.isfinite(scale).all() or np.any(scale == 0):
                raise IntegrityError("successor training-only standardization is invalid")
            z = (x - mean) / scale
            design = np.column_stack((np.ones(len(z)), z))
            y_long = np.asarray([float(outcomes[str(row["upstream_source_row_sha256"])]["long_realized_net_r"]) for row in training])
            y_short = np.asarray([float(outcomes[str(row["upstream_source_row_sha256"])]["short_realized_net_r"]) for row in training])
            penalty = np.eye(design.shape[1]); penalty[0, 0] = 0.0
            try:
                long_coef = np.linalg.solve(design.T @ design + penalty, design.T @ y_long)
                short_coef = np.linalg.solve(design.T @ design + penalty, design.T @ y_short)
            except np.linalg.LinAlgError as exc:
                raise IntegrityError("successor Ridge system is singular") from exc
            checkpoint_means: dict[str, dict[str, float | str]] = {}
            for checkpoint in CHECKPOINTS:
                subset = [row for row in training if row["checkpoint"] == checkpoint]
                if not subset:
                    raise IntegrityError("successor baseline has no training-only checkpoint rows")
                long_mean = fmean(float(outcomes[str(row["upstream_source_row_sha256"])]["long_realized_net_r"]) for row in subset)
                short_mean = fmean(float(outcomes[str(row["upstream_source_row_sha256"])]["short_realized_net_r"]) for row in subset)
                direction = "long" if long_mean >= short_mean else "short"
                checkpoint_means[checkpoint] = {"direction": direction, "ranking_score": max(long_mean, short_mean)}
            models.append({
                "market": market, "outer_fold": number, "training_rows": len(training),
                "feature_mean": mean.tolist(), "feature_population_std": scale.tolist(),
                "long_coefficients": long_coef.tolist(), "short_coefficients": short_coef.tolist(),
                "ridge_penalty": 1.0, "intercept_penalized": False,
                "fold_local_unconditional": checkpoint_means,
            })
            for row in testing:
                vector = np.asarray([float(row[name]) for name in FEATURE_NAMES])
                design_row = np.concatenate(([1.0], (vector - mean) / scale))
                long_prediction, short_prediction = float(design_row @ long_coef), float(design_row @ short_coef)
                _finite(long_prediction, name="long prediction"); _finite(short_prediction, name="short prediction")
                if long_prediction == short_prediction:
                    selected_direction, selected = "neutral", long_prediction
                elif long_prediction > short_prediction:
                    selected_direction, selected = "long", long_prediction
                else:
                    selected_direction, selected = "short", short_prediction
                if selected < 0.25:
                    selected_direction = "neutral"
                baseline = checkpoint_means[str(row["checkpoint"])]
                predictions.append({
                    **{key: row[key] for key in (
                        "market", "year", "exchange_session_date", "checkpoint", "checkpoint_at_ns",
                        "decision_at_ns", "actual_identity_hash", "upstream_source_row_sha256",
                    )},
                    "outer_fold": number, "long_prediction_net_r": long_prediction,
                    "short_prediction_net_r": short_prediction, "selected_predicted_net_r": selected,
                    "selected_direction": selected_direction,
                    "bar_return_1": float(row["bar_return_1"]),
                    "fold_local_direction": baseline["direction"],
                    "fold_local_ranking_score": float(baseline["ranking_score"]),
                })
    expected = {(market, year) for market in MARKETS for year in EVALUATION_YEARS}
    observed = {(str(row["market"]), int(row["year"])) for row in predictions}
    if observed != expected:
        raise IntegrityError(f"successor frozen prediction coverage is incomplete: {sorted(expected - observed)}")
    core = {
        "schema_version": "tier1_bracket_successor_v2_models/1.0.0", "trial_id": TRIAL_ID,
        "feature_names": list(FEATURE_NAMES), "models": models,
        "standardization": "MARKET_AND_OUTER_FOLD_TRAINING_ONLY",
    }
    return {**core, "model_bundle_id": sha256_json(core)}, tuple(predictions)


def _strategy_direction(row: Mapping[str, object], strategy: str) -> tuple[str, float] | None:
    if strategy in {"candidate", "equal_risk_version_of_candidate_signal"}:
        direction = row["selected_direction"]
        if direction == "neutral":
            return None
        return str(direction), float(row["selected_predicted_net_r"])
    if strategy == "fold_local_unconditional_return_by_market_session":
        return str(row["fold_local_direction"]), float(row["fold_local_ranking_score"])
    momentum = "long" if float(row["bar_return_1"]) >= 0 else "short"
    if strategy == "previous_bar_sign_momentum":
        return momentum, abs(float(row["bar_return_1"]))
    if strategy == "previous_bar_sign_reversal":
        return ("short" if momentum == "long" else "long"), abs(float(row["bar_return_1"]))
    if strategy == "risk_matched_always_long_intraday":
        return "long", 0.0
    raise IntegrityError(f"successor strategy is unknown: {strategy}")


def _scenario_cost(config: Mapping[str, object], scenario: str, market: str, tick_value: Decimal) -> Decimal:
    try:
        base = config["costs"]["base"][market]  # type: ignore[index]
        if scenario == "base":
            fee = Decimal(str(base["all_in_fee_per_side_usd"]))
            ticks = int(base["round_trip_slippage_ticks_per_contract"])
        else:
            scenario_config = config["costs"][scenario]  # type: ignore[index]
            fee = Decimal(str(base["all_in_fee_per_side_usd"])) * Decimal(str(scenario_config["fee_multiplier"]))
            ticks = max(
                int(base["round_trip_slippage_ticks_per_contract"]) * int(scenario_config["slippage_multiplier"]),
                int(scenario_config["minimum_round_trip_slippage_ticks"]),
            )
    except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
        raise IntegrityError("successor evaluation cost configuration is invalid") from exc
    return fee * Decimal(2) + Decimal(ticks) * tick_value


def _opportunities(
    *, predictions: Sequence[Mapping[str, object]], outcomes: Mapping[str, Mapping[str, object]],
    strategy: str,
) -> tuple[StrategyOpportunity, ...]:
    market_order = {market: index for index, market in enumerate(MARKETS)}
    grouped: dict[tuple[str, str], list[StrategyOpportunity]] = defaultdict(list)
    for row in predictions:
        selected = _strategy_direction(row, strategy)
        if selected is None:
            continue
        direction, score = selected
        key = str(row["upstream_source_row_sha256"])
        outcome = outcomes.get(key)
        if outcome is None:
            raise IntegrityError("successor frozen prediction lacks its exact label")
        prefix = direction
        try:
            opportunity = StrategyOpportunity(
                market=str(row["market"]), year=int(row["year"]),
                session=str(row["exchange_session_date"]), checkpoint=str(row["checkpoint"]),
                entry_at_ns=int(outcome["entry_at_ns"]), direction=direction,
                exit_at_ns=int(outcome[f"{prefix}_exit_at_ns"]), ranking_score=float(score),
                risk=Decimal(str(outcome[f"{prefix}_planned_all_in_risk_usd"])),
                gross_pnl=Decimal(str(outcome[f"{prefix}_realized_gross_pnl_usd"])),
                tick_value=Decimal(str(outcome["tick_value_usd"])),
            )
        except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
            raise IntegrityError("successor opportunity is incomplete") from exc
        if (
            opportunity.market not in MARKETS or opportunity.year not in EVALUATION_YEARS
            or opportunity.direction not in {"long", "short"}
            or opportunity.exit_at_ns <= opportunity.entry_at_ns
            or not math.isfinite(opportunity.ranking_score)
            or opportunity.risk <= 0 or opportunity.risk > Decimal("250")
            or not opportunity.gross_pnl.is_finite() or opportunity.tick_value <= 0
        ):
            raise IntegrityError("successor opportunity is invalid")
        grouped[(opportunity.session, opportunity.checkpoint)].append(opportunity)
    selected_rows: list[StrategyOpportunity] = []
    for group in grouped.values():
        selected_rows.append(sorted(group, key=lambda item: (-item.ranking_score, market_order[item.market]))[0])
    return tuple(sorted(selected_rows, key=lambda item: (item.entry_at_ns, market_order[item.market])))


def _simulate_strategy(
    *, opportunities: Sequence[StrategyOpportunity], config: Mapping[str, object], scenario: str,
) -> dict[str, object]:
    admitted: list[tuple[StrategyOpportunity, Decimal]] = []
    session_entries: dict[str, int] = defaultdict(int)
    session_pnl: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    equity = Decimal("0"); peak = Decimal("0"); open_until = -1
    overlap = entry_cap = daily_stop = drawdown_stop = 0
    for item in opportunities:
        if item.entry_at_ns < open_until:
            overlap += 1
            continue
        if session_pnl[item.session] <= Decimal("-500"):
            daily_stop += 1
            continue
        if peak - equity >= Decimal("1500"):
            drawdown_stop += 1
            continue
        if session_entries[item.session] >= 3:
            entry_cap += 1
            continue
        net = item.gross_pnl - _scenario_cost(config, scenario, item.market, item.tick_value)
        admitted.append((item, net))
        session_entries[item.session] += 1
        session_pnl[item.session] += net
        equity += net; peak = max(peak, equity); open_until = item.exit_at_ns
    net_values = [net for _, net in admitted]
    quantities = [1 if item.direction == "long" else -1 for item, _ in admitted]
    by_market_year: dict[str, list[tuple[StrategyOpportunity, Decimal]]] = defaultdict(list)
    for item, net in admitted:
        by_market_year[f"{item.market}/{item.year}"].append((item, net))
    return {
        "metrics": _metric_payload(net_values, quantities),
        "scheduler": {
            "admitted_count": len(admitted), "neutral_abstentions": 0,
            "simultaneous_abstentions": 0, "overlap_abstentions": overlap,
            "entry_cap_abstentions": entry_cap, "daily_stop_abstentions": daily_stop,
            "drawdown_stop_abstentions": drawdown_stop,
        },
        "by_market_year": {
            key: _metric_payload(
                [net for _, net in values],
                [1 if item.direction == "long" else -1 for item, _ in values],
            )
            for key, values in sorted(by_market_year.items())
        },
    }


def _strategy_views(
    *, predictions: Sequence[Mapping[str, object]], outcomes: Mapping[str, Mapping[str, object]],
    config: Mapping[str, object], scenario: str,
) -> dict[str, object]:
    result: dict[str, object] = {
        "flat_no_trade": {
            "metrics": dict(ZERO_METRICS),
            "scheduler": {name: 0 for name in (
                "admitted_count", "neutral_abstentions", "simultaneous_abstentions",
                "overlap_abstentions", "entry_cap_abstentions", "daily_stop_abstentions",
                "drawdown_stop_abstentions",
            )},
            "by_market_year": {},
        }
    }
    for strategy in ("candidate", *ALL_BASELINES):
        if strategy == "flat_no_trade":
            continue
        result[strategy] = _simulate_strategy(
            opportunities=_opportunities(predictions=predictions, outcomes=outcomes, strategy=strategy),
            config=config, scenario=scenario,
        )
    return result


def build_successor_evaluation(
    *, predictions: Sequence[Mapping[str, object]], outcome_rows: Sequence[Mapping[str, object]],
    config: Mapping[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    outcomes = {str(row["upstream_source_row_sha256"]): row for row in outcome_rows}
    if len(outcomes) != len(outcome_rows):
        raise IntegrityError("successor evaluation labels are ambiguous")
    expected = {(market, year) for market in MARKETS for year in EVALUATION_YEARS}
    if {(str(row["market"]), int(row["year"])) for row in predictions} != expected:
        raise IntegrityError("successor evaluation prediction coverage is incomplete")
    scenarios: dict[str, object] = {}
    for scenario in ("base", "stress", "extreme"):
        continuous = _strategy_views(predictions=predictions, outcomes=outcomes, config=config, scenario=scenario)
        by_market_year: dict[str, object] = {}
        for market, year in sorted(expected):
            subset = [row for row in predictions if row["market"] == market and row["year"] == year]
            by_market_year[f"{market}/{year}"] = _strategy_views(
                predictions=subset, outcomes=outcomes, config=config, scenario=scenario,
            )
        by_fold: dict[str, object] = {}
        for fold in range(8):
            subset = [row for row in predictions if row["outer_fold"] == fold]
            if subset:
                by_fold[str(fold)] = _strategy_views(
                    predictions=subset, outcomes=outcomes, config=config, scenario=scenario,
                )
        candidate_total = Decimal(str(continuous["candidate"]["metrics"]["net_pnl_usd"]))  # type: ignore[index]
        baseline_totals = {
            name: continuous[name]["metrics"]["net_pnl_usd"]  # type: ignore[index]
            for name in ALL_BASELINES
        }
        scenarios[scenario] = {
            "continuous_account": {"strategies": continuous},
            "independent_market_year": by_market_year,
            "independent_outer_fold": by_fold,
            "aggregate": continuous["candidate"]["metrics"],  # type: ignore[index]
            "scheduler": continuous["candidate"]["scheduler"],  # type: ignore[index]
            "baseline_net_pnl_usd": baseline_totals,
            "beats_required_baselines": all(candidate_total > Decimal(str(baseline_totals[name])) for name in REQUIRED_BASELINES),
        }
    core = {
        "schema_version": "tier1_bracket_successor_v2_evaluation/1.0.0",
        "trial_id": TRIAL_ID, "result_label": "PROVISIONAL_EXECUTION_COSTS",
        "cost_scenarios": scenarios,
    }
    report = {**core, "run_id": sha256_json(core), "report_kind": "model_selection"}
    stress = scenarios["stress"]
    continuous = stress["continuous_account"]["strategies"]  # type: ignore[index]
    candidate_total = Decimal(str(continuous["candidate"]["metrics"]["net_pnl_usd"]))
    portfolio_years = []
    market_years = []
    for year in EVALUATION_YEARS:
        year_predictions = [row for row in predictions if row["year"] == year]
        view = _strategy_views(predictions=year_predictions, outcomes=outcomes, config=config, scenario="stress")
        portfolio_years.append((year, Decimal(str(view["candidate"]["metrics"]["net_pnl_usd"]))))
    for market, year in sorted(expected):
        value = stress["independent_market_year"][f"{market}/{year}"]["candidate"]["metrics"]["net_pnl_usd"]  # type: ignore[index]
        market_years.append((market, year, Decimal(str(value))))
    clauses = {
        "positive_stress_net_pnl": candidate_total > 0,
        "beats_true_zero_no_trade": candidate_total > Decimal(str(continuous["flat_no_trade"]["metrics"]["net_pnl_usd"])),
        "beats_all_required_independent_baselines": bool(stress["beats_required_baselines"]),
        "positive_portfolio_years_at_least_2_of_3": sum(value > 0 for _, value in portfolio_years) >= 2,
        "positive_market_years_at_least_6_of_12": sum(value > 0 for _, _, value in market_years) >= 6,
        "markets_with_positive_year_at_least_3_of_4": sum(any(item_market == market and value > 0 for item_market, _, value in market_years) for market in MARKETS) >= 3,
        "continuous_drawdown_within_1500": Decimal(str(continuous["candidate"]["metrics"]["maximum_drawdown_usd"])) <= Decimal("1500"),
        "complete_metrics_and_coverage": len(market_years) == 12 and len(portfolio_years) == 3,
        "provisional_costs_forbid_live_readiness": True,
    }
    decision_core = {
        "schema_version": "tier1_bracket_successor_v2_decision/1.0.0", "trial_id": TRIAL_ID,
        "evaluation_run_id": report["run_id"], "promotion_clauses": clauses,
        "portfolio_year_net_pnl_usd": {str(year): str(value) for year, value in portfolio_years},
        "market_year_net_pnl_usd": {f"{market}/{year}": str(value) for market, year, value in market_years},
        "live_readiness": False, "cost_status": "PROVISIONAL_EXECUTION_COSTS",
    }
    decision_value = "PROMOTE_RESEARCH_ONLY" if all(clauses.values()) else "REJECTED"
    decision = {**decision_core, "decision": decision_value}
    decision["decision_id"] = sha256_json(decision)
    return report, decision


def _load_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"successor cannot read {path.name}") from exc
    if not isinstance(payload, dict):
        raise IntegrityError("successor JSON input is not an object")
    return payload


def _receipt(root: Path, phase: str, release_id: str, boundary: RepoBoundary) -> DataReleaseReceipt:
    receipt = DataReleaseReceipt.from_manifest(
        root / "manifests" / "data_releases" / phase / f"{release_id}.json", boundary
    )
    receipt.verify(boundary)
    return receipt


def resolve_successor_sources(*, root: Path) -> tuple[IntervalSource, ...]:
    """Resolve and hash-check all 20 approved intervals without opening Parquet rows."""

    boundary = RepoBoundary(root)
    registry_path = root / "state/trial_registry/tier1_bracket_successor_v2" / f"{TRIAL_ID}.json"
    registry = _load_json(registry_path)
    index_id = str(registry.get("phase8_index_release_id"))
    audit_id = str(registry.get("phase8_audit_release_id"))
    index = _receipt(root, "reference", index_id, boundary)
    audit = _receipt(root, "reference", audit_id, boundary)
    if audit.release_id != audit_id:
        raise IntegrityError("successor audit binding is invalid")
    payload = _load_json(index.resolve_file(
        "data/reference/economics/phase8_actual_contract_economics_index.json", boundary
    ))
    entries = payload.get("economics_by_interval")
    if not isinstance(entries, list):
        raise IntegrityError("successor Phase 8 index has no interval entries")
    source_pairs = {
        (str(item["market"]), int(item["year"])): dict(item)
        for item in registry.get("source_pairs", []) if isinstance(item, Mapping)
    }
    if set(source_pairs) != {(market, year) for market in MARKETS for year in YEARS}:
        raise IntegrityError("successor registered source-pair coverage is incomplete")
    # These are predecessor feature/outcome provenance bindings, not hashes of
    # the later Phase 8 causal payload.  Verify each declaration in its own
    # schema before resolving the independently pinned causal index.
    for (market, year), pair in source_pairs.items():
        feature_id, outcome_id = pair.get("prior_feature_release_id"), pair.get("prior_outcome_release_id")
        source_hash = pair.get("source_parquet_sha256")
        if not all(isinstance(value, str) and len(value) == 64 for value in (feature_id, outcome_id, source_hash)):
            raise IntegrityError("successor predecessor source binding is invalid")
        feature_manifest = _load_json(root / "manifests/data_releases/features" / f"{feature_id}.json")
        outcome_manifest = _load_json(root / "manifests/data_releases/outcomes" / f"{outcome_id}.json")
        if (
            feature_manifest.get("release_id") != feature_id
            or outcome_manifest.get("release_id") != outcome_id
            or feature_manifest.get("source_parquet_sha256") != source_hash
            or outcome_manifest.get("source_parquet_sha256") != source_hash
            or feature_manifest.get("metadata", {}).get("market", market) != market
            or feature_manifest.get("metadata", {}).get("year", year) != year
        ):
            raise IntegrityError(f"successor predecessor provenance differs for {market}-{year}")
    result: list[IntervalSource] = []
    for item in entries:
        if not isinstance(item, Mapping):
            continue
        causal_id = item.get("causal_release_id")
        economics_id = item.get("economics_release_id")
        interval_key = item.get("interval_key")
        if not all(isinstance(value, str) and len(value) == 64 for value in (causal_id, economics_id)) or not isinstance(interval_key, str):
            continue
        interval_parts = interval_key.split("/")
        if len(interval_parts) != 3 or interval_parts[0] not in MARKETS:
            continue
        try:
            indexed_year = int(interval_parts[1])
        except ValueError:
            continue
        if indexed_year not in YEARS:
            continue
        causal = _receipt(root, "causally_gated_normalized", str(causal_id), boundary)
        source_path, report = load_causal_interval(causal, boundary=boundary)
        market, year = report.get("market"), report.get("year")
        if market != interval_parts[0] or year != indexed_year:
            raise IntegrityError("successor causal receipt disagrees with its Phase 8 interval key")
        key = (str(market), int(year))
        binding = BracketIntervalBinding(index_id, str(causal_id), str(economics_id), interval_key)
        economics = indexed_bracket_economics_from_registry(
            load_verified_interval_economics(boundary=boundary, binding=binding)
        )
        result.append(IntervalSource(str(market), int(year), str(causal_id), str(economics_id), interval_key, source_path, economics))
    observed = {(item.market, item.year) for item in result}
    expected = {(market, year) for market in MARKETS for year in YEARS}
    if observed != expected or len(result) != 20:
        raise IntegrityError(f"successor resolved interval coverage differs: missing={sorted(expected - observed)}")
    return tuple(sorted(result, key=lambda item: (MARKETS.index(item.market), item.year)))


def _publisher(root: Path, *, operation_name: str, lock_name: str) -> tuple[RepoBoundary, PhasePublisher]:
    boundary = RepoBoundary(root)
    operation = OperationReceipt.issue_local(
        boundary, operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={"operation": operation_name, "scope": "registered_successor_v2_ES_CL_ZN_6E_2018_2022_only"},
    )
    return boundary, PhasePublisher(
        boundary=boundary, operation_receipt=operation,
        lock_path=root / "state/locks" / lock_name,
    )


def _publish_parquet(
    *, root: Path, table: object, phase: str, kind: str, logical_path: str,
    sources: Iterable[str], metadata: Mapping[str, object], stage_name: str,
) -> DataReleaseReceipt:
    import pyarrow.parquet as pq

    boundary, publisher = _publisher(root, operation_name=kind, lock_name=f"{stage_name}.lock")
    stage = publisher.create_stage(stage_name)
    payload = stage / "payload.parquet"
    pq.write_table(table, payload, compression="zstd")
    manifest = DataReleaseManifest.build(
        stage, phase=phase, release_kind=kind, schema_version="1.0.0",
        logical_paths={"payload.parquet": logical_path}, source_release_ids=tuple(sorted(set(sources))),
        metadata=dict(metadata),
    )
    receipt = DataReleaseReceipt.from_manifest(publisher.publish(stage, manifest), boundary)
    receipt.verify(boundary)
    return receipt


def _publish_document(
    *, root: Path, kind: str, document_name: str, document: Mapping[str, object],
    sources: Iterable[str], metadata: Mapping[str, object],
) -> DataReleaseReceipt:
    boundary, publisher = _publisher(root, operation_name=kind, lock_name=f"{kind}.lock")
    stage = publisher.create_stage(kind)
    manifest = DataReleaseManifest.build(
        stage, phase="evidence", release_kind=kind, schema_version="1.0.0",
        source_release_ids=tuple(sorted(set(sources))), embedded_documents={document_name: dict(document)},
        metadata=dict(metadata),
    )
    receipt = DataReleaseReceipt.from_manifest(publisher.publish(stage, manifest), boundary)
    receipt.verify(boundary)
    return receipt


def _publish_json_files(
    *, root: Path, phase: str, kind: str, files: Mapping[str, Mapping[str, object]],
    logical_paths: Mapping[str, str], sources: Iterable[str], metadata: Mapping[str, object],
) -> DataReleaseReceipt:
    boundary, publisher = _publisher(root, operation_name=kind, lock_name=f"{kind}.lock")
    stage = publisher.create_stage(kind)
    for name, payload in files.items():
        (stage / name).write_bytes(canonical_bytes(dict(payload)) + b"\n")
    manifest = DataReleaseManifest.build(
        stage, phase=phase, release_kind=kind, schema_version="1.0.0",
        logical_paths=dict(logical_paths), source_release_ids=tuple(sorted(set(sources))),
        metadata=dict(metadata),
    )
    receipt = DataReleaseReceipt.from_manifest(publisher.publish(stage, manifest), boundary)
    receipt.verify(boundary)
    return receipt


def prepare_execution_binding(*, root: Path, intervals: Sequence[IntervalSource]) -> dict[str, object]:
    spec = {
        "schema_version": "tier1_bracket_successor_v2_execution_binding/1.0.0",
        "trial_id": TRIAL_ID, "failed_trial_closure_id": CLOSURE_ID,
        "execution_module_sha256": sha256_file(Path(__file__)),
        "registered_contract_sha256": sha256_file(root / "configs/tier1_bracket_successor_v2.json"),
        "registered_evaluator_sha256": sha256_file(root / "src/futures_rebuild/tier1_bracket_evaluation.py"),
        "evaluation_config_sha256": sha256_file(root / "configs/tier1_phase8_evaluation.json"),
        "source_bindings": [
            {"market": item.market, "year": item.year, "causal_release_id": item.causal_release_id,
             "economics_release_id": item.economics_release_id, "source_parquet_sha256": sha256_file(item.payload)}
            for item in intervals
        ],
        "feature_formulas": {
            "returns": "close_to_close_1_5_20_completed_same_contract_session_bars",
            "intrabar_range_fraction": "(high-low)/open",
            "atr_20_fraction": "wilder_atr20/close",
            "range_to_atr_20": "(high-low)/wilder_atr20",
            "realized_volatility_20": "population_std_20_completed_log_returns",
            "volume": "log1p_current_and_population_zscore_last_60_including_current",
            "session_time": "sin_cos_minutes_since_17:00_America/Chicago_over_1440",
        },
        "direction_tie_behavior": "ABSTAIN",
        "execution_scope": "PINNED_LOCAL_2018_2022_ONLY_2025_EXCLUDED",
        "provider_access": False, "trading": False, "live_readiness": False,
    }
    return {**spec, "implementation_binding_id": sha256_json(spec)}


def persist_execution_binding(*, root: Path, payload: Mapping[str, object]) -> Path:
    identifier = payload.get("implementation_binding_id")
    if not isinstance(identifier, str) or sha256_json({key: value for key, value in payload.items() if key != "implementation_binding_id"}) != identifier:
        raise IntegrityError("successor implementation binding identity is invalid")
    directory = root / IMPLEMENTATION_BINDING_ROOT
    existing = tuple(directory.glob("*.json")) if directory.is_dir() else ()
    if existing:
        raise IntegrityError("successor execution is create-only and an implementation binding already exists")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{identifier}.json"
    with path.open("xb") as stream:
        stream.write(canonical_bytes(dict(payload)) + b"\n")
    return path


def execute_registered_successor_v2(*, root: Path) -> dict[str, object]:
    """Execute and publish the approved successor exactly once."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    registration = verify_successor_v2_registration(root=root, trial_id=TRIAL_ID)
    if registration["trial_id"] != TRIAL_ID:
        raise IntegrityError("successor registration verification returned the wrong trial")
    if (root / DECISION_EVENT_ROOT).exists() and tuple((root / DECISION_EVENT_ROOT).glob("*.json")):
        raise IntegrityError("successor evaluation is create-only and already has a decision event")
    intervals = resolve_successor_sources(root=root)
    binding = prepare_execution_binding(root=root, intervals=intervals)
    binding_path = persist_execution_binding(root=root, payload=binding)
    registry = _load_json(root / "state/trial_registry/tier1_bracket_successor_v2" / f"{TRIAL_ID}.json")
    index_id = str(registry["phase8_index_release_id"])
    audit_id = str(registry["phase8_audit_release_id"])

    all_features: list[dict[str, object]] = []
    all_outcomes: list[dict[str, object]] = []
    feature_receipts: dict[tuple[str, int], DataReleaseReceipt] = {}
    outcome_receipts: dict[tuple[str, int], DataReleaseReceipt] = {}
    required_columns = (
        "actual_identity_hash", "close_nano", "currency", "disposition", "event_at_ns",
        "exchange_session_date", "high_nano", "low_nano", "open_nano", "point_value",
        "source_row_sha256", "tick_size", "tick_value", "volume",
    )
    for interval in intervals:
        parquet = pq.ParquetFile(interval.payload)
        missing = set(required_columns) - set(parquet.schema_arrow.names)
        if missing:
            raise IntegrityError(f"successor causal source lacks columns for {interval.market}-{interval.year}: {sorted(missing)}")
        table = pq.read_table(interval.payload, columns=list(required_columns))
        features, outcomes = materialize_successor_market_year(
            rows=table.to_pylist(), market=interval.market, year=interval.year,
            economics=interval.economics, stress_cost_usd=STRESS_COST_USD[interval.market],
        )
        feature_table = pa.Table.from_pylist(list(features))
        outcome_table = pa.Table.from_pylist(list(outcomes))
        source_ids = (index_id, audit_id, interval.causal_release_id, interval.economics_release_id)
        feature_receipts[(interval.market, interval.year)] = _publish_parquet(
            root=root, table=feature_table, phase="features", kind="tier1_bracket_successor_v2_features",
            logical_path=f"data/features/{TRIAL_ID}/{interval.market}/{interval.year}/checkpoint-v2/features.parquet",
            sources=source_ids,
            metadata={"trial_id": TRIAL_ID, "market": interval.market, "year": interval.year,
                      "row_count": len(features), "implementation_binding_id": binding["implementation_binding_id"]},
            stage_name=f"tier1_bracket_successor_v2_features_{interval.market}_{interval.year}",
        )
        outcome_receipts[(interval.market, interval.year)] = _publish_parquet(
            root=root, table=outcome_table, phase="outcomes", kind="tier1_bracket_successor_v2_stress_labels",
            logical_path=f"data/outcomes/{TRIAL_ID}/{interval.market}/{interval.year}/checkpoint-v2/outcomes.parquet",
            sources=source_ids,
            metadata={"trial_id": TRIAL_ID, "market": interval.market, "year": interval.year,
                      "row_count": len(outcomes), "target_cost_scenario": "stress",
                      "implementation_binding_id": binding["implementation_binding_id"]},
            stage_name=f"tier1_bracket_successor_v2_outcomes_{interval.market}_{interval.year}",
        )
        all_features.extend(features); all_outcomes.extend(outcomes)
        del table, feature_table, outcome_table

    split_plan = build_successor_split_plan(all_features)
    split_receipt = _publish_document(
        root=root, kind="tier1_bracket_successor_v2_chronological_splits",
        document_name="split_plan", document=split_plan,
        sources=(receipt.release_id for receipt in feature_receipts.values()),
        metadata={"trial_id": TRIAL_ID, "split_plan_id": split_plan["split_plan_id"],
                  "implementation_binding_id": binding["implementation_binding_id"]},
    )
    model_bundle, predictions = fit_successor_models(
        feature_rows=all_features, outcome_rows=all_outcomes, split_plan=split_plan,
    )
    model_receipt = _publish_document(
        root=root, kind="tier1_bracket_successor_v2_market_specific_ridge_models",
        document_name="model_bundle", document=model_bundle,
        sources=(split_receipt.release_id, *[item.release_id for item in feature_receipts.values()],
                 *[item.release_id for item in outcome_receipts.values()]),
        metadata={"trial_id": TRIAL_ID, "model_bundle_id": model_bundle["model_bundle_id"],
                  "model_count": len(model_bundle["models"]),
                  "implementation_binding_id": binding["implementation_binding_id"]},
    )
    prediction_receipts: dict[tuple[str, int], DataReleaseReceipt] = {}
    prediction_index_entries: list[dict[str, object]] = []
    for market in MARKETS:
        for year in EVALUATION_YEARS:
            rows = [row for row in predictions if row["market"] == market and row["year"] == year]
            if not rows:
                raise IntegrityError(f"successor prediction partition is empty for {market}-{year}")
            first_date = min(str(row["exchange_session_date"]) for row in rows)
            receipt = _publish_parquet(
                root=root, table=pa.Table.from_pylist(rows), phase="predictions",
                kind="tier1_bracket_successor_v2_frozen_predictions",
                logical_path=f"data/predictions/{TRIAL_ID}/{market}/{year}/{first_date}/frozen_predictions.parquet",
                sources=(model_receipt.release_id, split_receipt.release_id, feature_receipts[(market, year)].release_id),
                metadata={"trial_id": TRIAL_ID, "market": market, "year": year, "prediction_rows": len(rows),
                          "model_bundle_id": model_bundle["model_bundle_id"],
                          "implementation_binding_id": binding["implementation_binding_id"]},
                stage_name=f"tier1_bracket_successor_v2_predictions_{market}_{year}",
            )
            prediction_receipts[(market, year)] = receipt
            prediction_index_entries.append({
                "market": market, "year": year, "prediction_release_id": receipt.release_id,
                "prediction_receipt_id": receipt.receipt_id, "prediction_rows": len(rows),
            })
    prediction_index_core = {
        "schema_version": "tier1_bracket_successor_v2_prediction_index/1.0.0",
        "trial_id": TRIAL_ID, "split_plan_id": split_plan["split_plan_id"],
        "model_bundle_id": model_bundle["model_bundle_id"],
        "prediction_releases": prediction_index_entries,
        "implementation_binding_id": binding["implementation_binding_id"],
    }
    prediction_index_payload = {**prediction_index_core, "prediction_index_id": sha256_json(prediction_index_core)}
    prediction_index_receipt = _publish_json_files(
        root=root, phase="reference", kind="tier1_bracket_successor_v2_prediction_index",
        files={"prediction_index.json": prediction_index_payload},
        logical_paths={"prediction_index.json": "data/reference/economics/tier1_bracket_successor_v2_prediction_index.json"},
        sources=(item.release_id for item in prediction_receipts.values()),
        metadata={"trial_id": TRIAL_ID, "prediction_release_count": len(prediction_receipts),
                  "prediction_index_id": prediction_index_payload["prediction_index_id"]},
    )

    evaluation_config = _load_json(root / "configs/tier1_phase8_evaluation.json")
    report, decision = build_successor_evaluation(
        predictions=predictions, outcome_rows=all_outcomes, config=evaluation_config,
    )
    risk = {
        "schema_version": report["schema_version"], "trial_id": TRIAL_ID,
        "run_id": report["run_id"], "report_kind": "risk",
        "result_label": "PROVISIONAL_EXECUTION_COSTS",
        "cost_scenarios": {
            name: {
                "aggregate": value["aggregate"], "scheduler": value["scheduler"],
                "independent_market_year": {
                    key: segment["candidate"] for key, segment in value["independent_market_year"].items()
                },
                "independent_outer_fold": {
                    key: segment["candidate"] for key, segment in value["independent_outer_fold"].items()
                },
            }
            for name, value in report["cost_scenarios"].items()
        },
    }
    evaluation_receipt = _publish_json_files(
        root=root, phase="evaluations", kind="tier1_bracket_successor_v2_historical_evaluation",
        files={"model_selection.json": report, "risk.json": risk, "decision.json": decision},
        logical_paths={
            "model_selection.json": f"data/evaluations/tier1_bracket_successor_v2/{TRIAL_ID}/aggregate/model_selection.json",
            "risk.json": f"data/evaluations/tier1_bracket_successor_v2/{TRIAL_ID}/aggregate/risk.json",
            "decision.json": f"data/evaluations/tier1_bracket_successor_v2/{TRIAL_ID}/aggregate/decision.json",
        },
        sources=(prediction_index_receipt.release_id, *[item.release_id for item in outcome_receipts.values()]),
        metadata={"trial_id": TRIAL_ID, "run_id": report["run_id"], "decision": decision["decision"],
                  "decision_id": decision["decision_id"], "result_label": "PROVISIONAL_EXECUTION_COSTS",
                  "implementation_binding_id": binding["implementation_binding_id"]},
    )
    event_core = {
        "schema_version": "tier1_bracket_successor_v2_evaluation_event/1.0.0",
        "event_type": "EVALUATED_AND_DECIDED", "trial_id": TRIAL_ID,
        "implementation_binding_id": binding["implementation_binding_id"],
        "feature_release_ids": [item.release_id for item in feature_receipts.values()],
        "outcome_release_ids": [item.release_id for item in outcome_receipts.values()],
        "split_release_id": split_receipt.release_id, "model_release_id": model_receipt.release_id,
        "prediction_index_release_id": prediction_index_receipt.release_id,
        "evaluation_release_id": evaluation_receipt.release_id,
        "decision": decision["decision"], "decision_id": decision["decision_id"],
        "provider_access": False, "holdout_or_forward_access": False,
        "trading": False, "live_readiness": False, "git_actions": False,
    }
    event = {**event_core, "event_id": sha256_json(event_core)}
    event_dir = root / DECISION_EVENT_ROOT
    event_dir.mkdir(parents=True, exist_ok=True)
    event_path = event_dir / f"{event['event_id']}.json"
    with event_path.open("xb") as stream:
        stream.write(canonical_bytes(event) + b"\n")
    return {
        "trial_id": TRIAL_ID, "implementation_binding": binding_path.relative_to(root).as_posix(),
        "implementation_binding_id": binding["implementation_binding_id"],
        "feature_release_count": len(feature_receipts), "outcome_release_count": len(outcome_receipts),
        "prediction_release_count": len(prediction_receipts),
        "split_release_id": split_receipt.release_id, "model_release_id": model_receipt.release_id,
        "prediction_index_release_id": prediction_index_receipt.release_id,
        "evaluation_release_id": evaluation_receipt.release_id,
        "decision": decision["decision"], "decision_id": decision["decision_id"],
        "event_path": event_path.relative_to(root).as_posix(),
    }


def main() -> int:
    result = execute_registered_successor_v2(root=Path.cwd())
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
