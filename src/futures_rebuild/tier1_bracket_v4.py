"""Complete pure execution engine for the post-audit Tier 1 v4 trial.

The engine is source-to-decision for in-memory rows.  Real file opening and
publication remain separate authorization-gated orchestration.  Synthetic tests
exercise the same materializer, model, scheduler, evaluator, and decision code.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from statistics import NormalDist, fmean, pstdev
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .research.contracts import ResearchContractError
from .research.bootstrap import stationary_bootstrap_index_rows
from .research.dsr import deflated_sharpe_ratio
from .research.hac import newey_west_mean
from .research.multiple_testing import romano_wolf_from_differentials
from .research.power import training_only_mde
from .tier1_bracket_post_audit import (
    BracketFill,
    CausalBar,
    GateEvidence,
    OpportunityRecord,
    SessionObservation,
    account_metrics,
    causality_certificate,
    classify_historical_screen,
    cost_ticks,
    latest_causal_feature_bar,
    load_post_audit_contract,
    planned_initial_loss_usd,
    reconcile_opportunity_ledger,
    round_up_ticks,
)


V4_CONTRACT_PATH = Path("configs/tier1_bracket_successor_v4.json")
V3_RETIREMENT_PATH = Path("configs/tier1_bracket_v3_retirement_preparation.json")
V3_TRIAL_ID = "1763e644f5b3ebcd95a744971145f9a552c19d2db6b95226b540dcecef247f33"
V3_REGISTRY = Path("state/trial_registry/tier1_bracket_post_audit_v3") / f"{V3_TRIAL_ID}.json"
V3_EVENT = Path("state/trial_events/tier1_bracket_post_audit_v3") / f"{V3_TRIAL_ID}.json"
V2_EXECUTION_BINDING = Path(
    "state/trial_registry/tier1_bracket_successor_v2_execution/"
    "e1a1a1d9cd78426f60f09d0032be8b7409c54efc7114a601db2fef8ef9bdd719.json"
)
LEGACY_PENALTY_MANIFEST = Path(
    "manifests/data_releases/evidence/"
    "a3a24c59e592baa38444fbf7380ab41df666898a638eb4454683fbc4f393e359.json"
)
V4_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v4")
V4_EVENT_ROOT = Path("state/trial_events/tier1_bracket_successor_v4")
V3_RETIREMENT_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_v3_retirement")
V3_RETIREMENT_EVENT_ROOT = Path("state/trial_events/tier1_bracket_v3_retirement")
MARKETS = ("ES", "CL", "ZN", "6E")
CHECKPOINTS = ("08:30", "10:30", "13:30")
SCENARIOS = ("base", "stress", "extreme")
FEATURE_NAMES = (
    "bar_return_1",
    "return_5",
    "return_20",
    "intrabar_range_fraction",
    "atr_20_fraction",
    "range_to_atr_20",
    "realized_volatility_20",
    "log1p_volume",
    "volume_zscore_60",
    "session_minute_sin",
    "session_minute_cos",
)
ACTIVE_BASELINES = (
    "fold_local_unconditional_return_by_market_session",
    "previous_bar_sign_momentum",
    "previous_bar_sign_reversal",
    "risk_matched_always_long_intraday",
    "candidate_signal_market_order_ranking_ablation",
)
ALL_STRATEGIES = ("candidate", "flat_no_trade", *ACTIVE_BASELINES)
NS_PER_MINUTE = 60_000_000_000
CHICAGO = ZoneInfo("America/Chicago")
NEGATIVE_CONTROL_IDS = (
    "future_feature_timestamp_injection_rejected",
    "same_bar_entry_rejected",
    "missing_checkpoint_retained_as_abstention",
    "flat_baseline_zero",
    "baseline_scheduler_state_independent",
    "holdout_path_rejected_before_open",
)


def _load(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"cannot read {path.name}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{path.name} must contain one object")
    return value


def load_v3_retirement_preparation(*, root: Path) -> dict[str, object]:
    payload = _load(root / V3_RETIREMENT_PATH)
    bindings = payload.get("preserved_bindings")
    if (
        payload.get("schema_version")
        != "tier1_bracket_v3_retirement_preparation/1.0.0"
        or payload.get("state") != "PREPARED_NOT_PUBLISHED"
        or payload.get("trial_id") != V3_TRIAL_ID
        or payload.get("disposition")
        != "INCOMPLETE_PRE_DATA_IMPLEMENTATION_BINDING"
        or payload.get("research_evidence_contaminated") is not False
        or payload.get("source_row_access") is not False
        or payload.get("historical_evaluation") is not False
        or not isinstance(bindings, dict)
        or bindings.get("registry_sha256") != sha256_file(root / V3_REGISTRY)
        or bindings.get("event_sha256") != sha256_file(root / V3_EVENT)
    ):
        raise IntegrityError("v3 retirement preparation is invalid")
    return payload


def load_v4_contract(*, root: Path) -> dict[str, object]:
    payload = _load(root / V4_CONTRACT_PATH)
    inherited = load_post_audit_contract(root=root)
    authority = payload.get("authority")
    stages = payload.get("required_bound_execution_stages")
    split = payload.get("split_plan")
    multiplicity = payload.get("multiplicity")
    if (
        payload.get("schema_version") != "tier1_bracket_successor_v4_contract/1.0.0"
        or payload.get("state") != "PREPARED_NOT_REGISTERED"
        or payload.get("supersedes_incomplete_v3_trial_id") != V3_TRIAL_ID
        or payload.get("inherits_policy_sha256")
        != sha256_file(root / "configs/tier1_bracket_post_audit_successor_v3.json")
        or inherited.get("period", {}).get("locked_untouched_holdout") != "2025"  # type: ignore[union-attr]
        or not isinstance(authority, dict)
        or set(authority.values()) != {False}
        or not isinstance(stages, list)
        or len(stages) != 10
        or payload.get("baselines") != ["flat_no_trade", *ACTIVE_BASELINES]
        or not isinstance(split, dict)
        or split.get("purge_minutes") != 60
        or split.get("embargo_sessions") != 1
        or not isinstance(multiplicity, dict)
        or multiplicity.get("legacy_trial_penalty_count") != 105
        or multiplicity.get("legacy_penalty_manifest")
        != LEGACY_PENALTY_MANIFEST.as_posix()
    ):
        raise IntegrityError("v4 successor contract is invalid")
    return payload


@dataclass(frozen=True)
class MarketSpec:
    tick_size: Decimal
    tick_value: Decimal
    point_value: Decimal

    def validate(self) -> None:
        if any(
            not value.is_finite() or value <= 0
            for value in (self.tick_size, self.tick_value, self.point_value)
        ):
            raise IntegrityError("market specification is invalid")


@dataclass(frozen=True)
class SourceMinute:
    market: str
    exchange_session_date: str
    bar: CausalBar
    volume: float
    actual_identity_hash: str
    source_row_sha256: str

    def validate(self) -> None:
        self.bar.validate()
        hexadecimal = set("0123456789abcdef")
        if (
            self.market not in MARKETS
            or not self.exchange_session_date
            or not math.isfinite(self.volume)
            or self.volume < 0
            or len(self.actual_identity_hash) != 64
            or len(self.source_row_sha256) != 64
            or not set(self.actual_identity_hash).issubset(hexadecimal)
            or not set(self.source_row_sha256).issubset(hexadecimal)
        ):
            raise IntegrityError("source minute is invalid")


@dataclass(frozen=True)
class ExpectedCheckpoint:
    opportunity_id: str
    market: str
    year: int
    exchange_session_date: str
    checkpoint: str
    decision_at_ns: int

    def validate(self) -> None:
        if (
            not self.opportunity_id
            or self.market not in MARKETS
            or self.year not in range(2018, 2023)
            or self.checkpoint not in CHECKPOINTS
            or type(self.decision_at_ns) is not int
        ):
            raise IntegrityError("expected checkpoint is invalid")


@dataclass(frozen=True)
class DirectionOutcomes:
    long: BracketFill
    short: BracketFill


@dataclass(frozen=True)
class MaterializedRow:
    expected: ExpectedCheckpoint
    ledger: OpportunityRecord
    features: Mapping[str, float] | None
    atr: Decimal | None
    source_row_sha256: str | None
    outcomes: Mapping[str, DirectionOutcomes] | None
    execution_path: tuple[CausalBar, ...] = ()
    market_spec: MarketSpec | None = None


def build_expected_census(
    *, sessions: Sequence[tuple[int, str, Mapping[str, int]]],
) -> tuple[ExpectedCheckpoint, ...]:
    rows: list[ExpectedCheckpoint] = []
    for year, session, decisions in sessions:
        if set(decisions) != set(CHECKPOINTS):
            raise IntegrityError("session checkpoint schedule is incomplete")
        for market in MARKETS:
            for checkpoint in CHECKPOINTS:
                core = {
                    "market": market,
                    "year": year,
                    "session": session,
                    "checkpoint": checkpoint,
                    "decision_at_ns": decisions[checkpoint],
                }
                rows.append(
                    ExpectedCheckpoint(
                        sha256_json(core), market, year, session, checkpoint,
                        decisions[checkpoint],
                    )
                )
    return tuple(rows)


def build_expected_census_from_sources(
    *, source_rows: Sequence[SourceMinute],
) -> tuple[ExpectedCheckpoint, ...]:
    """Build the universe from session labels before features or outcomes."""

    sessions: set[str] = set()
    for item in source_rows:
        item.validate()
        try:
            session_date = date.fromisoformat(item.exchange_session_date)
        except ValueError as exc:
            raise IntegrityError("source session label is invalid") from exc
        if session_date.year not in range(2018, 2023):
            raise IntegrityError("source session lies outside discovery scope")
        sessions.add(item.exchange_session_date)
    if not sessions:
        raise IntegrityError("source session census is empty")
    schedule: list[tuple[int, str, Mapping[str, int]]] = []
    for session in sorted(sessions):
        local_date = date.fromisoformat(session)
        decisions: dict[str, int] = {}
        for checkpoint in CHECKPOINTS:
            hour, minute = (int(value) for value in checkpoint.split(":"))
            local = datetime.combine(
                local_date, time(hour, minute), tzinfo=CHICAGO
            )
            decisions[checkpoint] = int(
                local.astimezone(timezone.utc).timestamp() * 1_000_000_000
            )
        schedule.append((local_date.year, session, decisions))
    return build_expected_census(sessions=schedule)


def build_v4_folds_from_census(
    expected: Sequence[ExpectedCheckpoint],
) -> tuple[FoldSpec, ...]:
    sessions = sorted({item.exchange_session_date for item in expected})
    training = [item for item in sessions if item[:4] in {"2018", "2019"}]
    evaluation = [item for item in sessions if item[:4] in {"2020", "2021", "2022"}]
    if not training or len(evaluation) < 8:
        raise IntegrityError("session census cannot support eight folds")
    quotient, remainder = divmod(len(evaluation), 8)
    folds: list[FoldSpec] = []
    start = 0
    for index in range(8):
        size = quotient + (1 if index < remainder else 0)
        test = evaluation[start : start + size]
        first_test = sessions.index(test[0])
        fit = sessions[: max(0, first_test - 1)]
        if not fit or fit[-1] >= test[0]:
            raise IntegrityError("fold embargo cannot be applied")
        folds.append(FoldSpec(index, tuple(fit), tuple(test)))
        start += size
    if start != len(evaluation):
        raise IntegrityError("fold evaluation coverage does not reconcile")
    return tuple(folds)


def _wilder_atr(history: Sequence[SourceMinute]) -> Decimal:
    if len(history) < 21:
        raise IntegrityError("ATR history is incomplete")
    true_ranges: list[Decimal] = []
    for index in range(1, len(history)):
        current, prior = history[index].bar, history[index - 1].bar
        true_ranges.append(
            max(
                current.high_price - current.low_price,
                abs(current.high_price - prior.close_price),
                abs(current.low_price - prior.close_price),
            )
        )
    atr = sum(true_ranges[:20], Decimal("0")) / Decimal("20")
    for value in true_ranges[20:]:
        atr = (Decimal("19") * atr + value) / Decimal("20")
    if atr <= 0:
        raise IntegrityError("ATR is non-positive")
    return atr


def _features(history: Sequence[SourceMinute], *, decision_at_ns: int) -> tuple[dict[str, float], Decimal]:
    if len(history) != 61:
        raise IntegrityError("feature history must contain exactly 61 bars")
    if any(item.bar.available_at_ns > decision_at_ns for item in history):
        raise IntegrityError("feature history contains future-available information")
    if len({item.actual_identity_hash for item in history}) != 1 or len(
        {item.exchange_session_date for item in history}
    ) != 1:
        raise IntegrityError("feature history crosses an identity or session boundary")
    if any(
        history[index].bar.event_at_ns - history[index - 1].bar.event_at_ns
        != NS_PER_MINUTE
        for index in range(1, len(history))
    ):
        raise IntegrityError("feature history is not minute-contiguous")
    closes = [float(item.bar.close_price) for item in history]
    volumes = [item.volume for item in history[-60:]]
    volume_std = pstdev(volumes)
    if volume_std == 0:
        raise IntegrityError("feature volume history has zero variance")
    atr = _wilder_atr(history)
    log_returns = [
        math.log(closes[index] / closes[index - 1])
        for index in range(len(closes) - 20, len(closes))
    ]
    current = history[-1]
    local = datetime.fromtimestamp(
        current.bar.event_at_ns / 1_000_000_000, tz=timezone.utc
    ).astimezone(CHICAGO)
    roll_date = (
        local.date() if local.time() >= time(17, 0)
        else local.date() - timedelta(days=1)
    )
    roll = datetime.combine(roll_date, time(17, 0), tzinfo=CHICAGO)
    minute = int((local - roll).total_seconds() // 60)
    if minute not in range(1440):
        raise IntegrityError("feature time lies outside the exchange session")
    angle = 2.0 * math.pi * minute / 1440.0
    current_range = current.bar.high_price - current.bar.low_price
    values = {
        "bar_return_1": closes[-1] / closes[-2] - 1.0,
        "return_5": closes[-1] / closes[-6] - 1.0,
        "return_20": closes[-1] / closes[-21] - 1.0,
        "intrabar_range_fraction": float(current_range / current.bar.open_price),
        "atr_20_fraction": float(atr / current.bar.close_price),
        "range_to_atr_20": float(current_range / atr),
        "realized_volatility_20": pstdev(log_returns),
        "log1p_volume": math.log1p(current.volume),
        "volume_zscore_60": (current.volume - fmean(volumes)) / volume_std,
        "session_minute_sin": math.sin(angle),
        "session_minute_cos": math.cos(angle),
    }
    if tuple(values) != FEATURE_NAMES or any(not math.isfinite(item) for item in values.values()):
        raise IntegrityError("feature vector is invalid")
    return values, atr


def simulate_v4_bracket_fill(
    *, direction: str, decision_at_ns: int, entry_bar: CausalBar,
    path_bars: Sequence[CausalBar], atr: Decimal, tick_size: Decimal,
    tick_value: Decimal, point_value: Decimal, fee_per_side_usd: Decimal,
    round_trip_cost_ticks: int,
    maximum_planned_loss_usd: Decimal = Decimal("1000"),
    maximum_hold_ns: int = 3_600_000_000_000,
) -> BracketFill:
    if (
        direction not in {"long", "short"}
        or entry_bar.event_at_ns <= decision_at_ns
        or maximum_hold_ns <= 0
    ):
        raise IntegrityError("v4 bracket direction or entry timing is invalid")
    entry_bar.validate()
    for bar in path_bars:
        bar.validate()
    if any(bar.event_at_ns < entry_bar.event_at_ns for bar in path_bars):
        raise IntegrityError("v4 outcome path begins before entry")
    planned = planned_initial_loss_usd(
        atr=atr, tick_size=tick_size, tick_value=tick_value,
        round_trip_cost_ticks=round_trip_cost_ticks,
        fee_per_side_usd=fee_per_side_usd,
    )
    if planned > maximum_planned_loss_usd:
        raise IntegrityError("planned initial loss exceeds the research cap")
    half_ticks = Decimal(round_trip_cost_ticks) / Decimal("2")
    sign = Decimal("1") if direction == "long" else Decimal("-1")
    entry = entry_bar.open_price + sign * half_ticks * tick_size
    stop_ticks = round_up_ticks(
        distance=Decimal("1.5") * atr, tick_size=tick_size
    )
    stop = entry - sign * Decimal(stop_ticks) * tick_size
    fees = Decimal("2") * fee_per_side_usd
    required_fill_to_fill = (
        Decimal("2") * planned + half_ticks * tick_value + fees
    )
    target_ticks = int(
        (required_fill_to_fill / tick_value).to_integral_value(
            rounding=ROUND_CEILING
        )
    )
    target = entry + sign * Decimal(target_ticks) * tick_size
    timeout_at = entry_bar.event_at_ns + maximum_hold_ns
    exit_price: Decimal | None = None
    exit_at = 0
    reason = ""
    for bar in sorted(path_bars, key=lambda item: item.event_at_ns):
        if not bar.executable:
            continue
        if bar.event_at_ns >= timeout_at:
            exit_price = bar.open_price - sign * half_ticks * tick_size
            exit_at, reason = bar.event_at_ns, "TIMEOUT"
            break
        if direction == "long":
            if bar.open_price <= stop:
                exit_price, reason = bar.open_price - half_ticks * tick_size, "STOP_GAP"
            elif bar.low_price <= stop:
                exit_price, reason = stop - half_ticks * tick_size, "STOP"
            elif bar.high_price >= target:
                exit_price, reason = target - half_ticks * tick_size, "TARGET"
        else:
            if bar.open_price >= stop:
                exit_price, reason = bar.open_price + half_ticks * tick_size, "STOP_GAP"
            elif bar.high_price >= stop:
                exit_price, reason = stop + half_ticks * tick_size, "STOP"
            elif bar.low_price <= target:
                exit_price, reason = target + half_ticks * tick_size, "TARGET"
        if exit_price is not None:
            exit_at = bar.event_at_ns
            break
    if exit_price is None:
        raise IntegrityError("v4 outcome path ends before a causal executable exit")
    net = sign * (exit_price - entry) * point_value - fees
    total_costs = fees + Decimal(round_trip_cost_ticks) * tick_value
    gross = net + total_costs
    if reason == "TARGET" and net < Decimal("2") * planned:
        raise IntegrityError("v4 target fill did not clear its declared net 2R")
    return BracketFill(
        entry_bar.event_at_ns, exit_at, entry, exit_price, stop, target,
        reason, gross, total_costs, net, planned,
    )


def materialize_v4_rows(
    *, source_rows: Sequence[SourceMinute], expected: Sequence[ExpectedCheckpoint],
    market_specs: Mapping[str, MarketSpec], contract: Mapping[str, object],
    prediction_scope_sessions: Sequence[str] | None = None,
) -> tuple[MaterializedRow, ...]:
    for item in source_rows:
        item.validate()
    for item in expected:
        item.validate()
    source_hashes = [item.source_row_sha256 for item in source_rows]
    if len(source_hashes) != len(set(source_hashes)):
        raise IntegrityError("source rows contain duplicate identities")
    if len({item.opportunity_id for item in expected}) != len(expected):
        raise IntegrityError("expected opportunity census is ambiguous")
    if set(market_specs) != set(MARKETS):
        raise IntegrityError("market specifications are incomplete")
    for spec in market_specs.values():
        spec.validate()
    grouped: dict[tuple[str, str], list[SourceMinute]] = defaultdict(list)
    for item in source_rows:
        grouped[(item.market, item.exchange_session_date)].append(item)
    output: list[MaterializedRow] = []
    prediction_scope = (
        {item.exchange_session_date for item in expected}
        if prediction_scope_sessions is None
        else set(prediction_scope_sessions)
    )
    fee = Decimal(str(contract["costs"]["fee_per_side_usd"]))  # type: ignore[index]
    for checkpoint in expected:
        rows = sorted(
            grouped.get((checkpoint.market, checkpoint.exchange_session_date), ()),
            key=lambda item: item.bar.event_at_ns,
        )
        if len({item.bar.event_at_ns for item in rows}) != len(rows):
            ledger = OpportunityRecord(
                checkpoint.opportunity_id, checkpoint.market,
                checkpoint.exchange_session_date, checkpoint.checkpoint,
                checkpoint.decision_at_ns,
                "MISSING_OR_AMBIGUOUS_MARKET_IDENTITY", False,
            )
            output.append(MaterializedRow(checkpoint, ledger, None, None, None, None))
            continue
        try:
            current_bar = latest_causal_feature_bar(
                bars=[item.bar for item in rows],
                decision_at_ns=checkpoint.decision_at_ns,
            )
        except IntegrityError:
            ledger = OpportunityRecord(
                checkpoint.opportunity_id, checkpoint.market,
                checkpoint.exchange_session_date, checkpoint.checkpoint,
                checkpoint.decision_at_ns, "INSUFFICIENT_CAUSAL_HISTORY", False,
            )
            output.append(MaterializedRow(checkpoint, ledger, None, None, None, None))
            continue
        current_index = next(
            index for index, item in enumerate(rows) if item.bar is current_bar
        )
        try:
            feature_values, atr = _features(
                rows[current_index - 60 : current_index + 1],
                decision_at_ns=checkpoint.decision_at_ns,
            )
        except (IntegrityError, IndexError):
            ledger = OpportunityRecord(
                checkpoint.opportunity_id, checkpoint.market,
                checkpoint.exchange_session_date, checkpoint.checkpoint,
                checkpoint.decision_at_ns, "INSUFFICIENT_CAUSAL_HISTORY", False,
            )
            output.append(MaterializedRow(checkpoint, ledger, None, None, None, None))
            continue
        future = [
            item for item in rows
            if checkpoint.decision_at_ns + NS_PER_MINUTE
            <= item.bar.event_at_ns
            <= checkpoint.decision_at_ns + 61 * NS_PER_MINUTE
        ]
        future_contiguous = bool(future) and all(
            future[index].bar.event_at_ns - future[index - 1].bar.event_at_ns
            == NS_PER_MINUTE
            for index in range(1, len(future))
        )
        outcomes: dict[str, DirectionOutcomes] = {}
        spec = market_specs[checkpoint.market]
        entry_bar = (
            future[0].bar
            if future_contiguous
            and future[0].bar.event_at_ns
            == checkpoint.decision_at_ns + NS_PER_MINUTE
            and future[0].bar.executable
            else None
        )
        if entry_bar is not None:
            for scenario in SCENARIOS:
                ticks = cost_ticks(
                    contract=contract, scenario=scenario, market=checkpoint.market,
                )
                path = [item.bar for item in future]
                try:
                    outcomes[scenario] = DirectionOutcomes(
                        long=simulate_v4_bracket_fill(
                            direction="long", decision_at_ns=checkpoint.decision_at_ns,
                            entry_bar=entry_bar, path_bars=path, atr=atr,
                            tick_size=spec.tick_size, tick_value=spec.tick_value,
                            point_value=spec.point_value, fee_per_side_usd=fee,
                            round_trip_cost_ticks=ticks,
                        ),
                        short=simulate_v4_bracket_fill(
                            direction="short", decision_at_ns=checkpoint.decision_at_ns,
                            entry_bar=entry_bar, path_bars=path, atr=atr,
                            tick_size=spec.tick_size, tick_value=spec.tick_value,
                            point_value=spec.point_value, fee_per_side_usd=fee,
                            round_trip_cost_ticks=ticks,
                        ),
                    )
                except IntegrityError:
                    outcomes = {}
                    break
        in_prediction_scope = checkpoint.exchange_session_date in prediction_scope
        ledger = OpportunityRecord(
            checkpoint.opportunity_id, checkpoint.market,
            checkpoint.exchange_session_date, checkpoint.checkpoint,
            checkpoint.decision_at_ns,
            (
                "PREDICTION_PRODUCED"
                if in_prediction_scope
                else "TRAINING_OR_PREDICTION_INELIGIBLE"
            ),
            in_prediction_scope,
            feature_event_at_ns=(current_bar.event_at_ns if in_prediction_scope else None),
            feature_available_at_ns=(
                current_bar.available_at_ns if in_prediction_scope else None
            ),
            outcome_coverage="COMPLETE" if len(outcomes) == 3 else "MISSING",
        )
        output.append(
            MaterializedRow(
                checkpoint, ledger, feature_values, atr,
                rows[current_index].source_row_sha256,
                outcomes if len(outcomes) == 3 else None,
                tuple(item.bar for item in future),
                spec,
            )
        )
    reconcile_opportunity_ledger(
        expected_ids=[item.opportunity_id for item in expected],
        records=[item.ledger for item in output],
    )
    causality_certificate([item.ledger for item in output])
    return tuple(output)


@dataclass(frozen=True)
class FoldSpec:
    outer_fold: int
    training_sessions: tuple[str, ...]
    test_sessions: tuple[str, ...]


@dataclass(frozen=True)
class FrozenPrediction:
    opportunity_id: str
    market: str
    year: int
    session: str
    checkpoint: str
    outer_fold: int
    long_predicted_net_r: float
    short_predicted_net_r: float
    selected_direction: str
    selected_predicted_net_r: float
    fold_local_direction: str
    fold_local_score: float
    bar_return_1: float


@dataclass(frozen=True)
class ModelFitResult:
    canonical_model_payload: Mapping[str, object]
    predictions: tuple[FrozenPrediction, ...]
    training_outcome_exclusions: int


def _stress_r(row: MaterializedRow, direction: str) -> float | None:
    if row.outcomes is None:
        return None
    fill = getattr(row.outcomes["stress"], direction)
    if fill.planned_initial_loss_usd <= 0:
        raise IntegrityError("outcome has non-positive planned risk")
    return float(fill.net_pnl_usd / fill.planned_initial_loss_usd)


def fit_predict_v4(
    *, rows: Sequence[MaterializedRow], folds: Sequence[FoldSpec],
) -> ModelFitResult:
    if len(folds) != 8 or {fold.outer_fold for fold in folds} != set(range(8)):
        raise IntegrityError("v4 requires exactly eight outer folds")
    all_sessions = sorted({row.expected.exchange_session_date for row in rows})
    session_order = {session: index for index, session in enumerate(all_sessions)}
    test_owners: dict[str, int] = {}
    for fold in folds:
        if not fold.training_sessions or not fold.test_sessions:
            raise IntegrityError("fold training or test sessions are empty")
        if set(fold.training_sessions).intersection(fold.test_sessions):
            raise IntegrityError("fold training and test sessions overlap")
        if max(session_order[item] for item in fold.training_sessions) >= min(
            session_order[item] for item in fold.test_sessions
        ):
            raise IntegrityError("fold is not chronological")
        for session in fold.test_sessions:
            if session in test_owners:
                raise IntegrityError("test session belongs to multiple folds")
            test_owners[session] = fold.outer_fold
    models: list[dict[str, object]] = []
    predictions: list[FrozenPrediction] = []
    exclusions = 0
    for fold in sorted(folds, key=lambda item: item.outer_fold):
        for market in MARKETS:
            training_all = [
                row for row in rows
                if row.expected.market == market
                and row.expected.exchange_session_date in fold.training_sessions
                and row.features is not None
            ]
            training = [row for row in training_all if row.outcomes is not None]
            exclusions += len(training_all) - len(training)
            testing = [
                row for row in rows
                if row.expected.market == market
                and row.expected.exchange_session_date in fold.test_sessions
                and row.features is not None
                and row.ledger.prediction_produced
            ]
            if not training or not testing:
                raise IntegrityError("market-fold lacks training or prediction coverage")
            x = np.asarray(
                [[float(row.features[name]) for name in FEATURE_NAMES] for row in training],
                dtype=np.float64,
            )
            center = x.mean(axis=0)
            scale = x.std(axis=0, ddof=0)
            if not np.isfinite(x).all() or not np.isfinite(scale).all():
                raise IntegrityError("training-only standardization is non-finite")
            constant_features = [
                FEATURE_NAMES[index]
                for index, value in enumerate(scale)
                if value == 0
            ]
            scale = np.where(scale == 0, 1.0, scale)
            z = (x - center) / scale
            design = np.column_stack((np.ones(len(z)), z))
            y_long = np.asarray([_stress_r(row, "long") for row in training], dtype=np.float64)
            y_short = np.asarray([_stress_r(row, "short") for row in training], dtype=np.float64)
            penalty = np.eye(design.shape[1], dtype=np.float64)
            penalty[0, 0] = 0.0
            try:
                long_coef = np.linalg.solve(
                    design.T @ design + penalty, design.T @ y_long
                )
                short_coef = np.linalg.solve(
                    design.T @ design + penalty, design.T @ y_short
                )
            except np.linalg.LinAlgError as exc:
                raise IntegrityError("v4 Ridge fit is singular") from exc
            checkpoint_means: dict[str, dict[str, object]] = {}
            for checkpoint in CHECKPOINTS:
                subset = [row for row in training if row.expected.checkpoint == checkpoint]
                if not subset:
                    raise IntegrityError("fold-local checkpoint baseline is empty")
                long_mean = fmean(_stress_r(row, "long") for row in subset)  # type: ignore[arg-type]
                short_mean = fmean(_stress_r(row, "short") for row in subset)  # type: ignore[arg-type]
                direction = "long" if long_mean >= short_mean else "short"
                checkpoint_means[checkpoint] = {
                    "direction": direction,
                    "score": max(long_mean, short_mean),
                }
            models.append(
                {
                    "outer_fold": fold.outer_fold,
                    "market": market,
                    "training_opportunity_ids": sorted(
                        row.expected.opportunity_id for row in training
                    ),
                    "training_rows": len(training),
                    "training_outcome_exclusions": len(training_all) - len(training),
                    "feature_center": center.tolist(),
                    "feature_population_std": scale.tolist(),
                    "constant_training_features_scaled_to_one": constant_features,
                    "long_coefficients": long_coef.tolist(),
                    "short_coefficients": short_coef.tolist(),
                    "fold_local_unconditional": checkpoint_means,
                }
            )
            for row in testing:
                assert row.features is not None
                vector = np.asarray(
                    [float(row.features[name]) for name in FEATURE_NAMES],
                    dtype=np.float64,
                )
                design_row = np.concatenate(([1.0], (vector - center) / scale))
                long_value = float(design_row @ long_coef)
                short_value = float(design_row @ short_coef)
                if not math.isfinite(long_value) or not math.isfinite(short_value):
                    raise IntegrityError("frozen prediction is non-finite")
                if long_value == short_value:
                    direction, selected = "neutral", long_value
                elif long_value > short_value:
                    direction, selected = "long", long_value
                else:
                    direction, selected = "short", short_value
                if selected < 0.25:
                    direction = "neutral"
                baseline = checkpoint_means[row.expected.checkpoint]
                predictions.append(
                    FrozenPrediction(
                        row.expected.opportunity_id,
                        market,
                        row.expected.year,
                        row.expected.exchange_session_date,
                        row.expected.checkpoint,
                        fold.outer_fold,
                        long_value,
                        short_value,
                        direction,
                        selected,
                        str(baseline["direction"]),
                        float(baseline["score"]),
                        float(row.features["bar_return_1"]),
                    )
                )
    predicted_ids = [item.opportunity_id for item in predictions]
    if len(predicted_ids) != len(set(predicted_ids)):
        raise IntegrityError("frozen predictions are duplicated")
    expected_prediction_ids = {
        row.expected.opportunity_id
        for row in rows
        if row.ledger.prediction_produced
        and row.features is not None
        and row.expected.exchange_session_date in test_owners
    }
    if set(predicted_ids) != expected_prediction_ids:
        raise IntegrityError("frozen prediction coverage does not reconcile")
    core: dict[str, object] = {
        "schema_version": "tier1_bracket_successor_v4_models/1.0.0",
        "feature_names": list(FEATURE_NAMES),
        "model_family": "MARKET_SPECIFIC_TWO_TARGET_RIDGE",
        "ridge_penalty": 1.0,
        "training_only_standardization": True,
        "models": models,
    }
    return ModelFitResult(
        {**core, "model_bundle_id": sha256_json(core)},
        tuple(sorted(predictions, key=lambda item: (item.session, item.checkpoint, item.market))),
        exclusions,
    )


@dataclass(frozen=True)
class StrategyTrade:
    opportunity_id: str
    market: str
    year: int
    session: str
    checkpoint: str
    direction: str
    ranking_score: float
    fill: BracketFill


@dataclass(frozen=True)
class StrategyPath:
    strategy_id: str
    admitted: tuple[StrategyTrade, ...]
    session_net_pnl_usd: Mapping[str, Decimal]
    metrics: Mapping[str, object]
    abstentions: Mapping[str, int]
    terminal_dispositions: Mapping[str, str]


def _strategy_signal(
    prediction: FrozenPrediction, strategy: str,
) -> tuple[str, float] | None:
    if strategy in {
        "candidate", "candidate_signal_market_order_ranking_ablation",
    }:
        if prediction.selected_direction == "neutral":
            return None
        score = (
            prediction.selected_predicted_net_r
            if strategy == "candidate"
            else 0.0
        )
        return prediction.selected_direction, score
    if strategy == "fold_local_unconditional_return_by_market_session":
        return prediction.fold_local_direction, prediction.fold_local_score
    momentum = "long" if prediction.bar_return_1 >= 0 else "short"
    if strategy == "previous_bar_sign_momentum":
        return momentum, abs(prediction.bar_return_1)
    if strategy == "previous_bar_sign_reversal":
        return ("short" if momentum == "long" else "long"), abs(
            prediction.bar_return_1
        )
    if strategy == "risk_matched_always_long_intraday":
        return "long", 0.0
    raise IntegrityError("unknown strategy")


def _validate_v4_independent_baselines(
    *, expected_opportunity_ids: Sequence[str],
    paths: Mapping[str, StrategyPath], scenario: str,
) -> None:
    required = {"flat_no_trade", *ACTIVE_BASELINES}
    if set(paths) != {"candidate", *required}:
        raise IntegrityError("v4 required strategy paths are incomplete")
    expected = set(expected_opportunity_ids)
    for name in required:
        if any(item.opportunity_id not in expected for item in paths[name].admitted):
            raise IntegrityError("baseline traded outside its opportunity universe")
    flat = paths["flat_no_trade"]
    if (
        flat.admitted
        or flat.metrics.get("net_pnl_usd") != "0"
        or flat.metrics.get("turnover_contract_equivalents") != "0"
    ):
        raise IntegrityError("flat baseline is not exactly flat")
    state_ids = {
        (f"{scenario}:{name}:signal", f"{scenario}:{name}:scheduler")
        for name in required
    }
    if len(state_ids) != len(required):
        raise IntegrityError("baseline state identities are not independent")


def _risk_adjusted_fill(
    *, row: MaterializedRow, fill: BracketFill, direction: str,
    realized_equity: Decimal, peak_equity: Decimal,
    session_start_equity: Decimal,
) -> BracketFill:
    """Apply the locked daily/drawdown marks before the ordinary bracket exit."""

    if row.market_spec is None or not row.execution_path:
        return fill
    spec = row.market_spec
    sign = Decimal("1") if direction == "long" else Decimal("-1")
    fee = Decimal("10")
    slip_ticks = (fill.costs_usd - fee) / spec.tick_value
    half_slip = slip_ticks * spec.tick_size / Decimal("2")
    executable = sorted(
        (bar for bar in row.execution_path if bar.executable),
        key=lambda bar: bar.event_at_ns,
    )
    for index, bar in enumerate(executable):
        if bar.event_at_ns >= fill.exit_at_ns:
            break
        adverse_price = bar.low_price if direction == "long" else bar.high_price
        marked_exit = adverse_price - sign * half_slip
        marked_net = sign * (marked_exit - fill.entry_price) * spec.point_value - fee
        marked_equity = realized_equity + marked_net
        daily_breach = session_start_equity - marked_equity >= Decimal("1000")
        drawdown_breach = peak_equity - marked_equity >= Decimal("5000")
        if not (daily_breach or drawdown_breach):
            continue
        later = [
            candidate for candidate in executable[index + 1 :]
            if candidate.event_at_ns <= fill.exit_at_ns
        ]
        if not later:
            return fill
        liquidation = later[0]
        exit_price = liquidation.open_price - sign * half_slip
        net = sign * (exit_price - fill.entry_price) * spec.point_value - fee
        gross = net + fill.costs_usd
        return BracketFill(
            fill.entry_at_ns,
            liquidation.event_at_ns,
            fill.entry_price,
            exit_price,
            fill.stop_price,
            fill.target_price,
            "RISK_LIQUIDATION_DRAWDOWN" if drawdown_breach else "RISK_LIQUIDATION_DAILY",
            gross,
            fill.costs_usd,
            net,
            fill.planned_initial_loss_usd,
        )
    return fill


def simulate_strategy_path(
    *, strategy: str, predictions: Sequence[FrozenPrediction],
    rows_by_id: Mapping[str, MaterializedRow], scenario: str,
    complete_sessions: Sequence[str],
) -> StrategyPath:
    if strategy == "flat_no_trade":
        metrics = account_metrics(
            sessions=tuple(
                SessionObservation(session, Decimal("0"), True)
                for session in complete_sessions
            )
        )
        return StrategyPath(
            strategy, (), {item: Decimal("0") for item in complete_sessions},
            metrics, {}, {item.opportunity_id: "FLAT_NO_TRADE" for item in predictions},
        )
    market_order = {market: index for index, market in enumerate(MARKETS)}
    grouped: dict[tuple[str, str], list[StrategyTrade]] = defaultdict(list)
    missing_outcomes = 0
    risk_rejections = 0
    terminals: dict[str, str] = {}
    for prediction in predictions:
        signal = _strategy_signal(prediction, strategy)
        if signal is None:
            terminals[prediction.opportunity_id] = "HURDLE_FAILURE"
            continue
        direction, score = signal
        row = rows_by_id.get(prediction.opportunity_id)
        if row is None or row.outcomes is None:
            missing_outcomes += 1
            terminals[prediction.opportunity_id] = "MISSING_PRICE_PATH"
            continue
        fill = getattr(row.outcomes[scenario], direction)
        stress_risk = getattr(
            row.outcomes["stress"], direction
        ).planned_initial_loss_usd
        if stress_risk > Decimal("1000"):
            risk_rejections += 1
            terminals[prediction.opportunity_id] = "RISK_CAP_REJECTION"
            continue
        grouped[(prediction.session, prediction.checkpoint)].append(
            StrategyTrade(
                prediction.opportunity_id, prediction.market, prediction.year,
                prediction.session, prediction.checkpoint, direction, score, fill,
            )
        )
    ranked: list[StrategyTrade] = []
    for group in grouped.values():
        ordered = sorted(
            group,
            key=lambda item: (-item.ranking_score, market_order[item.market]),
        )
        ranked.append(ordered[0])
        for loser in ordered[1:]:
            terminals[loser.opportunity_id] = "CROSS_MARKET_RANKING_LOSS"
    ranked.sort(key=lambda item: (item.fill.entry_at_ns, market_order[item.market]))
    admitted: list[StrategyTrade] = []
    session_pnl = {session: Decimal("0") for session in complete_sessions}
    session_entries: dict[str, int] = defaultdict(int)
    equity = peak = Decimal("100000")
    session_start_equity: dict[str, Decimal] = {}
    open_until = -1
    overlap = entry_cap = daily_stop = drawdown_stop = 0
    for trade in ranked:
        if trade.session not in session_pnl:
            terminals[trade.opportunity_id] = "MISSING_PRICE_PATH"
            continue
        session_start_equity.setdefault(trade.session, equity)
        if trade.fill.entry_at_ns < open_until:
            overlap += 1
            terminals[trade.opportunity_id] = "OVERLAP_ABSTENTION"
            continue
        if session_pnl[trade.session] <= Decimal("-1000"):
            daily_stop += 1
            terminals[trade.opportunity_id] = "DAILY_STOP_ABSTENTION"
            continue
        if peak - equity >= Decimal("5000"):
            drawdown_stop += 1
            terminals[trade.opportunity_id] = "DRAWDOWN_ABSTENTION"
            continue
        if session_entries[trade.session] >= 3:
            entry_cap += 1
            terminals[trade.opportunity_id] = "ENTRY_CAP_ABSTENTION"
            continue
        row = rows_by_id[trade.opportunity_id]
        adjusted_fill = _risk_adjusted_fill(
            row=row,
            fill=trade.fill,
            direction=trade.direction,
            realized_equity=equity,
            peak_equity=peak,
            session_start_equity=session_start_equity[trade.session],
        )
        trade = StrategyTrade(
            trade.opportunity_id, trade.market, trade.year, trade.session,
            trade.checkpoint, trade.direction, trade.ranking_score, adjusted_fill,
        )
        admitted.append(trade)
        terminals[trade.opportunity_id] = "ADMITTED_TRADE"
        session_entries[trade.session] += 1
        session_pnl[trade.session] += trade.fill.net_pnl_usd
        equity += trade.fill.net_pnl_usd
        peak = max(peak, equity)
        open_until = trade.fill.exit_at_ns
    metrics = account_metrics(
        sessions=tuple(
            SessionObservation(
                session, session_pnl[session], True,
                Decimal("2")
                * sum(1 for trade in admitted if trade.session == session),
            )
            for session in complete_sessions
        )
    )
    if set(terminals) != {item.opportunity_id for item in predictions}:
        raise IntegrityError("strategy terminal opportunity ledger is incomplete")
    return StrategyPath(
        strategy,
        tuple(admitted),
        session_pnl,
        metrics,
        {
            "missing_outcome": missing_outcomes,
            "risk_cap": risk_rejections,
            "overlap": overlap,
            "entry_cap": entry_cap,
            "daily_stop": daily_stop,
            "drawdown_stop": drawdown_stop,
        },
        terminals,
    )


@dataclass(frozen=True)
class EvaluationBundle:
    canonical_payload: Mapping[str, object]
    paths_by_scenario: Mapping[str, Mapping[str, StrategyPath]]
    complete_sessions: tuple[str, ...]
    incomplete_sessions: tuple[str, ...]


def build_v4_evaluation(
    *, rows: Sequence[MaterializedRow], predictions: Sequence[FrozenPrediction],
) -> EvaluationBundle:
    rows_by_id = {row.expected.opportunity_id: row for row in rows}
    if len(rows_by_id) != len(rows):
        raise IntegrityError("materialized rows are duplicated")
    prediction_ids = {item.opportunity_id for item in predictions}
    if len(prediction_ids) != len(predictions):
        raise IntegrityError("predictions are duplicated")
    prediction_sessions = sorted({item.session for item in predictions})
    incomplete = {
        row.expected.exchange_session_date
        for row in rows
        if row.expected.opportunity_id in prediction_ids and row.outcomes is None
    }
    complete = tuple(session for session in prediction_sessions if session not in incomplete)
    if not complete:
        raise IntegrityError("evaluation has no complete sessions")
    paths_by_scenario: dict[str, dict[str, StrategyPath]] = {}
    scenario_payload: dict[str, object] = {}
    for scenario in SCENARIOS:
        paths = {
            strategy: simulate_strategy_path(
                strategy=strategy,
                predictions=predictions,
                rows_by_id=rows_by_id,
                scenario=scenario,
                complete_sessions=complete,
            )
            for strategy in ALL_STRATEGIES
        }
        paths_by_scenario[scenario] = paths
        expected_ids = tuple(sorted(rows_by_id))
        _validate_v4_independent_baselines(
            expected_opportunity_ids=expected_ids,
            paths=paths,
            scenario=scenario,
        )
        market_year: dict[str, object] = {}
        for market in MARKETS:
            for year in range(2020, 2023):
                subset = [
                    item for item in predictions
                    if item.market == market and item.year == year
                ]
                segment_sessions = tuple(
                    session
                    for session in complete
                    if any(item.session == session for item in subset)
                )
                if not segment_sessions:
                    continue
                market_year[f"{market}/{year}"] = {
                    strategy: simulate_strategy_path(
                        strategy=strategy,
                        predictions=subset,
                        rows_by_id=rows_by_id,
                        scenario=scenario,
                        complete_sessions=segment_sessions,
                    ).metrics
                    for strategy in ALL_STRATEGIES
                }
        folds: dict[str, object] = {}
        for outer_fold in range(8):
            subset = [item for item in predictions if item.outer_fold == outer_fold]
            segment_sessions = tuple(
                session
                for session in complete
                if any(item.session == session for item in subset)
            )
            if not segment_sessions:
                continue
            folds[str(outer_fold)] = {
                strategy: simulate_strategy_path(
                    strategy=strategy,
                    predictions=subset,
                    rows_by_id=rows_by_id,
                    scenario=scenario,
                    complete_sessions=segment_sessions,
                ).metrics
                for strategy in ALL_STRATEGIES
            }
        scenario_payload[scenario] = {
            "continuous": {
                strategy: {
                    "metrics": dict(path.metrics),
                    "abstentions": dict(path.abstentions),
                    "admitted_count": len(path.admitted),
                }
                for strategy, path in paths.items()
            },
            "independent_market_year": market_year,
            "independent_outer_fold": folds,
        }
    stress_candidate = paths_by_scenario["stress"]["candidate"]
    admitted_by_id = {
        item.opportunity_id: item for item in stress_candidate.admitted
    }
    final_records: list[OpportunityRecord] = []
    for row in rows:
        if not row.ledger.prediction_produced:
            final_records.append(row.ledger)
            continue
        disposition = stress_candidate.terminal_dispositions.get(
            row.expected.opportunity_id
        )
        if disposition is None:
            raise IntegrityError("candidate terminal ledger omitted a prediction")
        admitted = admitted_by_id.get(row.expected.opportunity_id)
        final_records.append(
            OpportunityRecord(
                row.expected.opportunity_id,
                row.expected.market,
                row.expected.exchange_session_date,
                row.expected.checkpoint,
                row.expected.decision_at_ns,
                disposition,
                True,
                feature_event_at_ns=row.ledger.feature_event_at_ns,
                feature_available_at_ns=row.ledger.feature_available_at_ns,
                order_submitted_at_ns=(
                    admitted.fill.entry_at_ns if admitted is not None else None
                ),
                fill_at_ns=(
                    admitted.fill.entry_at_ns if admitted is not None else None
                ),
                outcome_coverage=(
                    "COMPLETE"
                    if admitted is not None
                    else row.ledger.outcome_coverage
                ),
            )
        )
    funnel = reconcile_opportunity_ledger(
        expected_ids=tuple(rows_by_id), records=final_records,
    )
    certificate = causality_certificate(final_records)
    core: dict[str, object] = {
        "schema_version": "tier1_bracket_successor_v4_evaluation/1.0.0",
        "complete_sessions": list(complete),
        "incomplete_sessions": sorted(incomplete),
        "stress_candidate_opportunity_funnel": funnel,
        "stress_candidate_causality_certificate": certificate,
        "stress_candidate_terminal_dispositions": {
            item.opportunity_id: item.terminal_disposition
            for item in sorted(final_records, key=lambda value: value.opportunity_id)
        },
        "scenarios": scenario_payload,
    }
    return EvaluationBundle(
        {**core, "evaluation_id": sha256_json(core)},
        paths_by_scenario,
        complete,
        tuple(sorted(incomplete)),
    )


@dataclass(frozen=True)
class InferenceInputs:
    training_differential_returns: np.ndarray
    negative_controls: Mapping[str, bool]
    sleeve_evaluation_returns: Mapping[str, np.ndarray]
    sleeve_training_returns: Mapping[str, np.ndarray]
    bootstrap_resamples: int
    seed: int
    legacy_trial_penalty_count: int = 105


def _hac_lag(observations: int, *, overlap_lag: int = 1) -> int:
    return max(overlap_lag, math.floor(4 * (observations / 100) ** (2 / 9)))


def derive_v4_decision(
    *, evaluation: EvaluationBundle, inference: InferenceInputs,
) -> dict[str, object]:
    if set(inference.negative_controls) != set(NEGATIVE_CONTROL_IDS):
        raise IntegrityError("negative controls are incomplete or unexpected")
    controls_passed = all(inference.negative_controls.values())
    stress = evaluation.paths_by_scenario["stress"]
    sessions = evaluation.complete_sessions
    candidate = np.asarray(
        [float(stress["candidate"].session_net_pnl_usd[item] / Decimal("100000")) for item in sessions],
        dtype=np.float64,
    )
    if len(candidate) < 3:
        raise IntegrityError("too few complete sessions for inference")
    sleeve_ids = tuple(
        f"{market}/{checkpoint}/{direction}"
        for market in MARKETS
        for checkpoint in CHECKPOINTS
        for direction in ("long", "short")
    )
    if set(inference.sleeve_evaluation_returns) != set(sleeve_ids) or set(
        inference.sleeve_training_returns
    ) != set(sleeve_ids):
        raise IntegrityError("twenty-four sleeve evidence series are incomplete")
    lag = _hac_lag(len(candidate))
    hac = newey_west_mean(candidate, lag=lag)
    mean_block_length = min(10.0, float(len(candidate)))
    means = np.asarray(
        [
            float(np.mean(candidate[index], dtype=np.float64))
            for index in stationary_bootstrap_index_rows(
                n_observations=len(candidate),
                n_resamples=inference.bootstrap_resamples,
                mean_block_length=mean_block_length,
                seed=inference.seed,
            )
        ],
        dtype=np.float64,
    )
    lower_one_sided = float(np.quantile(means, 0.05))
    lower_two_sided = float(np.quantile(means, 0.025))
    upper_two_sided = float(np.quantile(means, 0.975))
    if len(candidate) < 30:
        core: dict[str, object] = {
            "schema_version": "tier1_bracket_successor_v4_decision/1.0.0",
            "classification": "INCONCLUSIVE_DATA_OR_POWER",
            "complete_clusters": len(candidate),
            "hac_mean_daily_return": hac.mean,
            "hac_standard_error": hac.standard_error,
            "bootstrap_one_sided_95_lower_daily_return": lower_one_sided,
            "bootstrap_two_sided_95_interval": [
                lower_two_sided, upper_two_sided,
            ],
            "power_adequate": False,
            "dsr_probability": None,
            "dsr_status": "NOT_RUN_INSUFFICIENT_CLUSTERS",
            "romano_wolf_adjusted_p": {
                name: None for name in ACTIVE_BASELINES
            },
            "pbo": "NOT_APPLICABLE_SINGLE_PREDECLARED_CONFIGURATION",
            "negative_controls": dict(sorted(inference.negative_controls.items())),
            "negative_controls_passed": controls_passed,
            "sleeves": {
                sleeve_id: {"status": "INCONCLUSIVE_CLUSTER_COUNT"}
                for sleeve_id in sleeve_ids
            },
            "distribution_gate_passed": False,
            "stress_and_baselines_passed": False,
            "drawdown_gate_passed": False,
        }
        return {**core, "decision_id": sha256_json(core)}
    if hac.status != "OK":
        core = {
            "schema_version": "tier1_bracket_successor_v4_decision/1.0.0",
            "classification": "INCONCLUSIVE_DATA_OR_POWER",
            "complete_clusters": len(candidate),
            "hac_mean_daily_return": hac.mean,
            "hac_standard_error": hac.standard_error,
            "bootstrap_one_sided_95_lower_daily_return": lower_one_sided,
            "bootstrap_two_sided_95_interval": [lower_two_sided, upper_two_sided],
            "power_adequate": False,
            "dsr_probability": 0.0,
            "dsr_status": "DEGENERATE_CANDIDATE_SERIES_FAIL_CLOSED",
            "romano_wolf_adjusted_p": {name: None for name in ACTIVE_BASELINES},
            "pbo": "NOT_APPLICABLE_SINGLE_PREDECLARED_CONFIGURATION",
            "negative_controls": dict(sorted(inference.negative_controls.items())),
            "negative_controls_passed": controls_passed,
            "sleeves": {
                sleeve_id: {"status": "INCONCLUSIVE_DEGENERATE_PORTFOLIO"}
                for sleeve_id in sleeve_ids
            },
            "distribution_gate_passed": False,
            "stress_and_baselines_passed": False,
            "drawdown_gate_passed": False,
        }
        return {**core, "decision_id": sha256_json(core)}
    differentials = np.column_stack(
        [
            candidate
            - np.asarray(
                [
                    float(stress[name].session_net_pnl_usd[item] / Decimal("100000"))
                    for item in sessions
                ],
                dtype=np.float64,
            )
            for name in ACTIVE_BASELINES
        ]
    )
    romano_status = "OK"
    try:
        romano = romano_wolf_from_differentials(
            differentials,
            hypothesis_ids=ACTIVE_BASELINES,
            hac_lag=lag,
            mean_block_length=mean_block_length,
            n_resamples=inference.bootstrap_resamples,
            seed=inference.seed,
            minimum_resamples=inference.bootstrap_resamples,
        )
        romano_adjusted = romano.adjusted_p_values
    except ResearchContractError:
        romano_status = "CONTRACT_FAILURE_FAIL_CLOSED"
        romano_adjusted = np.ones(len(ACTIVE_BASELINES), dtype=np.float64)
    if inference.legacy_trial_penalty_count != 105:
        raise IntegrityError("legacy trial penalty count differs from registration")
    candidate_std = float(np.std(candidate, ddof=1))
    candidate_sharpe = (
        float(np.mean(candidate, dtype=np.float64)) / candidate_std
        if candidate_std > 0
        else float("-inf")
    )
    null_scale = 1.0 / math.sqrt(len(candidate) - 1)
    normal = NormalDist()
    null_census = np.asarray(
        [
            normal.inv_cdf((index + 0.5) / inference.legacy_trial_penalty_count)
            * null_scale
            for index in range(inference.legacy_trial_penalty_count)
        ],
        dtype=np.float64,
    )
    dsr_probability = 0.0
    dsr_status = "CANDIDATE_NOT_UNIQUE_MAXIMUM_FAIL_CLOSED"
    if math.isfinite(candidate_sharpe) and candidate_sharpe > float(np.max(null_census)):
        trial_sharpes = np.concatenate((null_census, [candidate_sharpe]))
        try:
            dsr_result = deflated_sharpe_ratio(
                candidate,
                trial_sharpes,
                raw_trial_count=len(trial_sharpes),
                selected_trial_index=len(trial_sharpes) - 1,
            )
        except ResearchContractError:
            dsr_status = "DSR_CONTRACT_FAILURE_FAIL_CLOSED"
        else:
            dsr_probability = dsr_result.probability
            dsr_status = "OK"
    try:
        power = training_only_mde(
            inference.training_differential_returns,
            partition_role="TRAIN",
            hac_lag=min(lag, len(inference.training_differential_returns) - 1),
            planned_evaluation_observations=len(candidate),
            alpha=0.05,
            target_power=0.80,
            alternative="greater",
            economic_mean_hurdle=0.0002,
        )
        power_adequate = power.adequately_powered
    except ResearchContractError:
        power_adequate = False
    sleeve_evidence: dict[str, object] = {}
    sleeves_powered = True
    sleeve_mees = float(Decimal("0.8333333333333333333333333333") / Decimal("100000"))
    for sleeve_id in sleeve_ids:
        evaluation_values = np.asarray(
            inference.sleeve_evaluation_returns[sleeve_id], dtype=np.float64
        )
        training_values = np.asarray(
            inference.sleeve_training_returns[sleeve_id], dtype=np.float64
        )
        if len(evaluation_values) < 30:
            sleeves_powered = False
            sleeve_evidence[sleeve_id] = {"status": "INCONCLUSIVE_CLUSTER_COUNT"}
            continue
        try:
            sleeve_power = training_only_mde(
                training_values,
                partition_role="TRAIN",
                hac_lag=min(_hac_lag(len(evaluation_values)), len(training_values) - 1),
                planned_evaluation_observations=len(evaluation_values),
                alpha=0.05,
                target_power=0.80,
                alternative="greater",
                economic_mean_hurdle=sleeve_mees,
            )
            sleeve_hac = newey_west_mean(
                evaluation_values, lag=_hac_lag(len(evaluation_values))
            )
            passed = (
                sleeve_power.adequately_powered
                and sleeve_hac.status == "OK"
                and sleeve_hac.mean > sleeve_mees
            )
        except ResearchContractError:
            passed = False
            sleeve_power = None
            sleeve_hac = None
        sleeves_powered = sleeves_powered and passed
        sleeve_evidence[sleeve_id] = {
            "status": "PASS" if passed else "INCONCLUSIVE_OR_FAIL",
            "mean": sleeve_hac.mean if sleeve_hac is not None else None,
            "power_adequate": (
                sleeve_power.adequately_powered if sleeve_power is not None else False
            ),
        }
    stress_candidate = stress["candidate"]
    candidate_net = Decimal(str(stress_candidate.metrics["net_pnl_usd"]))
    baseline_nets = {
        name: Decimal(str(stress[name].metrics["net_pnl_usd"]))
        for name in ACTIVE_BASELINES
    }
    market_year_positive = 0
    years_positive = set()
    by_market_year: dict[tuple[str, int], Decimal] = defaultdict(lambda: Decimal("0"))
    by_year: dict[int, Decimal] = defaultdict(lambda: Decimal("0"))
    for trade in stress_candidate.admitted:
        by_market_year[(trade.market, trade.year)] += trade.fill.net_pnl_usd
        by_year[trade.year] += trade.fill.net_pnl_usd
    market_year_positive = sum(value > 0 for value in by_market_year.values())
    markets_positive = {
        market for (market, _), value in by_market_year.items() if value > 0
    }
    years_positive = {year for year, value in by_year.items() if value > 0}
    distribution_passed = (
        len(years_positive) >= 2
        and market_year_positive >= 6
        and markets_positive == set(MARKETS)
    )
    max_drawdown = Decimal(str(stress_candidate.metrics["maximum_drawdown_usd"]))
    stress_passed = (
        candidate_net > 0
        and all(candidate_net > value for value in baseline_nets.values())
        and bool(np.all(romano_adjusted <= 0.05))
    )
    evidence = GateEvidence(
        invalid=False,
        complete_clusters=len(candidate),
        power=Decimal(str(0.80 if power_adequate else 0)),
        every_required_sleeve_powered=sleeves_powered,
        confidence_lower_usd=Decimal(str(lower_one_sided * 100000)),
        confidence_upper_usd=Decimal(str(upper_two_sided * 100000)),
        mees_usd=Decimal("20"),
        dsr_probability=Decimal(str(dsr_probability)),
        romano_wolf_passed=bool(np.all(romano_adjusted <= 0.05)),
        controls_passed=controls_passed,
        stress_and_baselines_passed=stress_passed,
        distribution_gate_passed=distribution_passed,
        drawdown_gate_passed=max_drawdown <= Decimal("5000"),
    )
    core: dict[str, object] = {
        "schema_version": "tier1_bracket_successor_v4_decision/1.0.0",
        "classification": classify_historical_screen(evidence),
        "complete_clusters": len(candidate),
        "hac_mean_daily_return": hac.mean,
        "hac_standard_error": hac.standard_error,
        "bootstrap_one_sided_95_lower_daily_return": lower_one_sided,
        "bootstrap_two_sided_95_interval": [lower_two_sided, upper_two_sided],
        "power_adequate": power_adequate,
        "dsr_probability": dsr_probability,
        "dsr_status": dsr_status,
        "romano_wolf_adjusted_p": {
            name: float(value)
            for name, value in zip(ACTIVE_BASELINES, romano_adjusted)
        },
        "romano_wolf_status": romano_status,
        "pbo": "NOT_APPLICABLE_SINGLE_PREDECLARED_CONFIGURATION",
        "negative_controls": dict(sorted(inference.negative_controls.items())),
        "negative_controls_passed": controls_passed,
        "sleeves": sleeve_evidence,
        "distribution_gate_passed": distribution_passed,
        "stress_and_baselines_passed": stress_passed,
        "drawdown_gate_passed": max_drawdown <= Decimal("5000"),
    }
    return {**core, "decision_id": sha256_json(core)}


@dataclass(frozen=True)
class V4PipelineResult:
    materialized_rows: tuple[MaterializedRow, ...]
    model_fit: ModelFitResult
    evaluation: EvaluationBundle
    decision: Mapping[str, object]


def run_v4_negative_controls() -> dict[str, bool]:
    results = {name: False for name in NEGATIVE_CONTROL_IDS}
    future = OpportunityRecord(
        "negative-future", "ES", "2022-01-03", "08:30", 100,
        "PREDICTION_PRODUCED", True,
        feature_event_at_ns=99, feature_available_at_ns=101,
    )
    try:
        future.validate()
    except IntegrityError:
        results["future_feature_timestamp_injection_rejected"] = True
    bar = CausalBar(
        100, 160, 165, Decimal("100"), Decimal("100"),
        Decimal("100"), Decimal("100"),
    )
    try:
        simulate_v4_bracket_fill(
            direction="long", decision_at_ns=100, entry_bar=bar,
            path_bars=(bar,), atr=Decimal("1"), tick_size=Decimal("1"),
            tick_value=Decimal("1"), point_value=Decimal("1"),
            fee_per_side_usd=Decimal("5"), round_trip_cost_ticks=0,
        )
    except IntegrityError:
        results["same_bar_entry_rejected"] = True
    missing = OpportunityRecord(
        "negative-missing", "ES", "2022-01-03", "08:30", 100,
        "INSUFFICIENT_CAUSAL_HISTORY", False,
    )
    funnel = reconcile_opportunity_ledger(
        expected_ids=(missing.opportunity_id,), records=(missing,)
    )
    results["missing_checkpoint_retained_as_abstention"] = (
        funnel["pre_prediction_abstentions"] == 1
    )
    flat = simulate_strategy_path(
        strategy="flat_no_trade", predictions=(), rows_by_id={},
        scenario="stress", complete_sessions=("2022-01-03",),
    )
    results["flat_baseline_zero"] = (
        not flat.admitted and flat.metrics["net_pnl_usd"] == "0"
    )
    zero_paths = {
        name: StrategyPath(
            name, (), {"2022-01-03": Decimal("0")}, flat.metrics, {}, {}
        )
        for name in ALL_STRATEGIES
    }
    _validate_v4_independent_baselines(
        expected_opportunity_ids=("negative-missing",),
        paths=zero_paths,
        scenario="stress",
    )
    results["baseline_scheduler_state_independent"] = True
    try:
        load_authorized_source_minutes(
            root=Path("."),
            authorization=AuthorizedHistoricalRun("none", True, True, True, True),
            source_paths={("ES", 2025): Path("must-not-open")},
        )
    except UnauthorizedOperation:
        results["holdout_path_rejected_before_open"] = True
    if not all(results.values()):
        raise IntegrityError("one or more v4 negative controls failed")
    return results


def build_v4_inference_inputs(
    *, rows: Sequence[MaterializedRow], evaluation: EvaluationBundle,
    trial_id: str, bootstrap_resamples: int = 10000,
) -> InferenceInputs:
    if len(trial_id) != 64 or not set(trial_id).issubset(set("0123456789abcdef")):
        raise IntegrityError("inference seed requires a canonical trial ID")
    training_rows = [
        row for row in rows
        if not row.ledger.prediction_produced and row.outcomes is not None
    ]
    training_sessions = sorted(
        {row.expected.exchange_session_date for row in training_rows}
    )
    if len(training_sessions) < 3:
        raise IntegrityError("training-only inference history is insufficient")
    training_proxy: list[float] = []
    for session in training_sessions:
        session_rows = [
            row for row in training_rows
            if row.expected.exchange_session_date == session
        ]
        training_proxy.append(
            sum(
                (
                    float(row.outcomes["stress"].long.net_pnl_usd)
                    + float(row.outcomes["stress"].short.net_pnl_usd)
                ) / 2.0
                for row in session_rows
                if row.outcomes is not None
            ) / 100000.0
        )
    sleeve_ids = tuple(
        f"{market}/{checkpoint}/{direction}"
        for market in MARKETS for checkpoint in CHECKPOINTS
        for direction in ("long", "short")
    )
    sleeve_training: dict[str, np.ndarray] = {}
    for sleeve_id in sleeve_ids:
        market, checkpoint, direction = sleeve_id.split("/")
        values: list[float] = []
        for session in training_sessions:
            matches = [
                row for row in training_rows
                if row.expected.exchange_session_date == session
                and row.expected.market == market
                and row.expected.checkpoint == checkpoint
                and row.outcomes is not None
            ]
            if len(matches) != 1:
                continue
            fill = getattr(matches[0].outcomes["stress"], direction)  # type: ignore[index]
            values.append(float(fill.net_pnl_usd / Decimal("100000")))
        sleeve_training[sleeve_id] = np.asarray(values, dtype=np.float64)
    candidate = evaluation.paths_by_scenario["stress"]["candidate"]
    sleeve_evaluation: dict[str, np.ndarray] = {}
    for sleeve_id in sleeve_ids:
        market, checkpoint, direction = sleeve_id.split("/")
        values = []
        for session in evaluation.complete_sessions:
            pnl = sum(
                (
                    trade.fill.net_pnl_usd
                    for trade in candidate.admitted
                    if trade.session == session
                    and trade.market == market
                    and trade.checkpoint == checkpoint
                    and trade.direction == direction
                ),
                Decimal("0"),
            )
            values.append(float(pnl / Decimal("100000")))
        sleeve_evaluation[sleeve_id] = np.asarray(values, dtype=np.float64)
    return InferenceInputs(
        training_differential_returns=np.asarray(training_proxy, dtype=np.float64),
        negative_controls=run_v4_negative_controls(),
        sleeve_evaluation_returns=sleeve_evaluation,
        sleeve_training_returns=sleeve_training,
        bootstrap_resamples=bootstrap_resamples,
        seed=int(trial_id[:16], 16),
    )


def run_v4_pipeline(
    *, source_rows: Sequence[SourceMinute], expected: Sequence[ExpectedCheckpoint],
    market_specs: Mapping[str, MarketSpec], contract: Mapping[str, object],
    folds: Sequence[FoldSpec], inference: InferenceInputs | None,
    prediction_scope_sessions: Sequence[str],
    trial_id: str | None = None,
) -> V4PipelineResult:
    materialized = materialize_v4_rows(
        source_rows=source_rows,
        expected=expected,
        market_specs=market_specs,
        contract=contract,
        prediction_scope_sessions=prediction_scope_sessions,
    )
    model_fit = fit_predict_v4(rows=materialized, folds=folds)
    evaluation = build_v4_evaluation(
        rows=materialized,
        predictions=model_fit.predictions,
    )
    if inference is None:
        if trial_id is None:
            raise IntegrityError("automatic inference requires the registered trial ID")
        inference = build_v4_inference_inputs(
            rows=materialized, evaluation=evaluation, trial_id=trial_id,
        )
    controls = run_v4_negative_controls()
    if dict(inference.negative_controls) != controls:
        raise IntegrityError("inference negative controls were not engine-derived")
    decision = derive_v4_decision(evaluation=evaluation, inference=inference)
    return V4PipelineResult(materialized, model_fit, evaluation, decision)


@dataclass(frozen=True)
class PreparedV4Registration:
    trial_id: str
    canonical_payload: Mapping[str, object]


@dataclass(frozen=True)
class PreparedV3Retirement:
    record_id: str
    canonical_payload: Mapping[str, object]


def prepare_v3_retirement(*, root: Path) -> PreparedV3Retirement:
    preparation = load_v3_retirement_preparation(root=root)
    core: dict[str, object] = {
        "schema_version": "tier1_bracket_v3_retirement/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "trial_id": V3_TRIAL_ID,
        "disposition": preparation["disposition"],
        "reason": preparation["reason"],
        "research_evidence_contaminated": False,
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "holdout_or_forward_access": False,
        "preserved_bindings": preparation["preserved_bindings"],
        "preparation_sha256": sha256_file(root / V3_RETIREMENT_PATH),
    }
    return PreparedV3Retirement(sha256_json(core), core)


def persist_v3_retirement(
    *, root: Path, prepared: PreparedV3Retirement,
) -> dict[str, str]:
    if prepared.record_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("v3 retirement identity is invalid")
    bindings = prepared.canonical_payload.get("preserved_bindings")
    if (
        not isinstance(bindings, dict)
        or bindings.get("registry_sha256") != sha256_file(root / V3_REGISTRY)
        or bindings.get("event_sha256") != sha256_file(root / V3_EVENT)
        or prepared.canonical_payload.get("preparation_sha256")
        != sha256_file(root / V3_RETIREMENT_PATH)
    ):
        raise IntegrityError("v3 retirement preservation binding changed")
    registry = root / V3_RETIREMENT_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = root / V3_RETIREMENT_EVENT_ROOT / f"{prepared.record_id}.json"
    if registry.exists() or event.exists():
        raise IntegrityError("v3 retirement is create-only")
    registry.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    record = {
        **dict(prepared.canonical_payload),
        "state": "RETIRED_WITHOUT_DATA_ACCESS",
        "record_id": prepared.record_id,
    }
    notice = {
        "schema_version": "tier1_bracket_v3_retirement_event/1.0.0",
        "event_type": "RETIRED",
        "trial_id": V3_TRIAL_ID,
        "record_id": prepared.record_id,
        "disposition": "INCOMPLETE_PRE_DATA_IMPLEMENTATION_BINDING",
        "research_evidence_contaminated": False,
    }
    try:
        with registry.open("xb") as stream:
            stream.write(canonical_bytes(record) + b"\n")
        with event.open("xb") as stream:
            stream.write(canonical_bytes(notice) + b"\n")
    except FileExistsError as exc:
        raise IntegrityError("v3 retirement raced another writer") from exc
    return {
        "record_id": prepared.record_id,
        "registry_path": registry.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
    }


def verify_v3_retirement(
    *, root: Path, prepared: PreparedV3Retirement,
) -> dict[str, str]:
    registry = root / V3_RETIREMENT_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = root / V3_RETIREMENT_EVENT_ROOT / f"{prepared.record_id}.json"
    record = _load(registry)
    notice = _load(event)
    expected_record = {
        **dict(prepared.canonical_payload),
        "state": "RETIRED_WITHOUT_DATA_ACCESS",
        "record_id": prepared.record_id,
    }
    expected_notice = {
        "schema_version": "tier1_bracket_v3_retirement_event/1.0.0",
        "event_type": "RETIRED",
        "trial_id": V3_TRIAL_ID,
        "record_id": prepared.record_id,
        "disposition": "INCOMPLETE_PRE_DATA_IMPLEMENTATION_BINDING",
        "research_evidence_contaminated": False,
    }
    if record != expected_record or notice != expected_notice:
        raise IntegrityError("published v3 retirement differs from preparation")
    return {
        "record_id": prepared.record_id,
        "registry_sha256": sha256_file(registry),
        "event_sha256": sha256_file(event),
    }


def prepare_v4_registration(*, root: Path) -> PreparedV4Registration:
    """Bind the complete executor and metadata without opening market rows."""

    contract = load_v4_contract(root=root)
    retirement = load_v3_retirement_preparation(root=root)
    source_binding = _load(root / V2_EXECUTION_BINDING)
    sources = source_binding.get("source_bindings")
    if not isinstance(sources, list) or len(sources) != 20:
        raise IntegrityError("v4 requires exactly twenty source metadata bindings")
    pairs: set[tuple[str, int]] = set()
    for item in sources:
        if not isinstance(item, dict):
            raise IntegrityError("v4 source metadata binding is malformed")
        market, year, digest = item.get("market"), item.get("year"), item.get(
            "source_parquet_sha256"
        )
        if (
            market not in MARKETS
            or type(year) is not int
            or year not in range(2018, 2023)
            or not isinstance(digest, str)
            or len(digest) != 64
        ):
            raise IntegrityError("v4 source metadata binding is outside scope")
        pairs.add((str(market), year))
    if pairs != {(market, year) for market in MARKETS for year in range(2018, 2023)}:
        raise IntegrityError("v4 source metadata coverage is incomplete")
    bound_paths = (
        V4_CONTRACT_PATH,
        V3_RETIREMENT_PATH,
        V3_REGISTRY,
        V3_EVENT,
        V2_EXECUTION_BINDING,
        LEGACY_PENALTY_MANIFEST,
        Path("src/futures_rebuild/tier1_bracket_post_audit.py"),
        Path("src/futures_rebuild/tier1_bracket_v4.py"),
        Path("tests/test_tier1_bracket_post_audit.py"),
        Path("tests/test_tier1_bracket_v4.py"),
    )
    core: dict[str, object] = {
        "schema_version": "tier1_bracket_successor_v4_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": contract["classification"],
        "supersedes_v3_disposition": retirement["disposition"],
        "bindings": {
            path.as_posix(): sha256_file(root / path) for path in bound_paths
        },
        "source_bindings": sorted(
            (dict(item) for item in sources),
            key=lambda item: (str(item["market"]), int(item["year"])),
        ),
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "trading": False,
    }
    return PreparedV4Registration(sha256_json(core), core)


def persist_v4_registration(
    *, root: Path, prepared: PreparedV4Registration,
) -> dict[str, str]:
    """Create the declaration/event; the caller must hold publication approval."""

    bindings = prepared.canonical_payload.get("bindings")
    if prepared.trial_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("v4 prepared registration identity is invalid")
    if not isinstance(bindings, dict) or any(
        not isinstance(path, str)
        or not isinstance(digest, str)
        or sha256_file(root / path) != digest
        for path, digest in bindings.items()
    ):
        raise IntegrityError("v4 implementation changed after preparation")
    registry = root / V4_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = root / V4_EVENT_ROOT / f"{prepared.trial_id}.json"
    if registry.exists() or event.exists():
        raise IntegrityError("v4 registration is create-only")
    registry.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    registered = {
        **dict(prepared.canonical_payload),
        "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
        "trial_id": prepared.trial_id,
    }
    declared = {
        "schema_version": "tier1_bracket_successor_v4_event/1.0.0",
        "event_type": "DECLARED",
        "trial_id": prepared.trial_id,
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "holdout_or_forward_access": False,
    }
    try:
        with registry.open("xb") as stream:
            stream.write(canonical_bytes(registered) + b"\n")
        with event.open("xb") as stream:
            stream.write(canonical_bytes(declared) + b"\n")
    except FileExistsError as exc:
        raise IntegrityError("v4 registration raced another writer") from exc
    return {
        "trial_id": prepared.trial_id,
        "registry_path": registry.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
    }


def verify_v4_registration(
    *, root: Path, prepared: PreparedV4Registration,
) -> dict[str, str]:
    registry = root / V4_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = root / V4_EVENT_ROOT / f"{prepared.trial_id}.json"
    registered = _load(registry)
    declared = _load(event)
    expected_registered = {
        **dict(prepared.canonical_payload),
        "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
        "trial_id": prepared.trial_id,
    }
    expected_declared = {
        "schema_version": "tier1_bracket_successor_v4_event/1.0.0",
        "event_type": "DECLARED",
        "trial_id": prepared.trial_id,
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "holdout_or_forward_access": False,
    }
    if registered != expected_registered or declared != expected_declared:
        raise IntegrityError("published v4 declaration differs from preparation")
    return {
        "trial_id": prepared.trial_id,
        "registry_sha256": sha256_file(registry),
        "event_sha256": sha256_file(event),
    }


@dataclass(frozen=True)
class AuthorizedHistoricalRun:
    trial_id: str
    source_row_read: bool
    model_fit: bool
    prediction_generation: bool
    historical_evaluation: bool
    holdout_or_forward_access: bool = False


@dataclass(frozen=True)
class AuthorizedSourceBundle:
    rows: tuple[SourceMinute, ...]
    market_specs: Mapping[str, MarketSpec]


def load_authorized_source_minutes(
    *, root: Path, authorization: AuthorizedHistoricalRun,
    source_paths: Mapping[tuple[str, int], Path],
) -> AuthorizedSourceBundle:
    """Open only registered 2018-2022 sources after explicit run authorization."""

    for market, year in source_paths:
        if year == 2025:
            raise UnauthorizedOperation("2025 holdout access is forbidden")
        if market not in MARKETS or year not in range(2018, 2023):
            raise IntegrityError("source path is outside the registered period")
    if (
        not authorization.source_row_read
        or not authorization.model_fit
        or not authorization.prediction_generation
        or not authorization.historical_evaluation
        or authorization.holdout_or_forward_access
    ):
        raise UnauthorizedOperation("historical v4 execution is not authorized")
    registry = _load(root / V4_REGISTRY_ROOT / f"{authorization.trial_id}.json")
    if (
        registry.get("trial_id") != authorization.trial_id
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
    ):
        raise IntegrityError("v4 registered declaration is absent or inconsistent")
    bindings = registry.get("source_bindings")
    if not isinstance(bindings, list):
        raise IntegrityError("registered v4 source bindings are missing")
    expected = {
        (str(item["market"]), int(item["year"])): str(item["source_parquet_sha256"])
        for item in bindings if isinstance(item, dict)
    }
    if set(source_paths) != set(expected):
        raise IntegrityError("authorized source path map does not exactly match registration")
    import pyarrow.parquet as pq

    required = {
        "exchange_session_date", "event_at_ns", "open_nano", "high_nano",
        "low_nano", "close_nano", "volume", "actual_identity_hash",
        "source_row_sha256", "tick_size", "tick_value", "point_value",
    }
    output: list[SourceMinute] = []
    specifications: dict[str, MarketSpec] = {}
    for key in sorted(source_paths):
        market, _ = key
        path = source_paths[key]
        if sha256_file(path) != expected[key]:
            raise IntegrityError("historical source bytes do not match registration")
        parquet = pq.ParquetFile(path)
        if not required.issubset(parquet.schema_arrow.names):
            raise IntegrityError("historical source schema is incomplete")
        for item in parquet.read(columns=sorted(required)).to_pylist():
            event = int(item["event_at_ns"])
            session = str(item["exchange_session_date"])
            if not session.startswith(str(key[1])):
                raise IntegrityError("source row session year differs from its binding")
            spec = MarketSpec(
                Decimal(str(item["tick_size"])),
                Decimal(str(item["tick_value"])),
                Decimal(str(item["point_value"])),
            )
            spec.validate()
            if market in specifications and specifications[market] != spec:
                raise IntegrityError("market economics vary inside the registered source set")
            specifications[market] = spec
            minute = SourceMinute(
                    market=market,
                    exchange_session_date=session,
                    bar=CausalBar(
                        event,
                        event + NS_PER_MINUTE,
                        event + NS_PER_MINUTE + 5_000_000_000,
                        Decimal(int(item["open_nano"])) / Decimal("1000000000"),
                        Decimal(int(item["high_nano"])) / Decimal("1000000000"),
                        Decimal(int(item["low_nano"])) / Decimal("1000000000"),
                        Decimal(int(item["close_nano"])) / Decimal("1000000000"),
                        True,
                    ),
                    volume=float(item["volume"]),
                    actual_identity_hash=str(item["actual_identity_hash"]),
                    source_row_sha256=str(item["source_row_sha256"]),
                )
            minute.validate()
            output.append(minute)
    if set(specifications) != set(MARKETS):
        raise IntegrityError("authorized source market economics are incomplete")
    return AuthorizedSourceBundle(tuple(output), specifications)


def execute_authorized_v4(
    *, root: Path, authorization: AuthorizedHistoricalRun,
    source_paths: Mapping[tuple[str, int], Path],
) -> V4PipelineResult:
    """Reject the retired V4 real-history route before any source access."""

    raise UnauthorizedOperation(
        "V4 historical execution is retired; use CertifiedResearchGateway"
    )

    bundle = load_authorized_source_minutes(
        root=root, authorization=authorization, source_paths=source_paths,
    )
    expected = build_expected_census_from_sources(source_rows=bundle.rows)
    folds = build_v4_folds_from_census(expected)
    prediction_sessions = tuple(
        session for fold in folds for session in fold.test_sessions
    )
    return run_v4_pipeline(
        source_rows=bundle.rows,
        expected=expected,
        market_specs=bundle.market_specs,
        contract=load_post_audit_contract(root=root),
        folds=folds,
        inference=None,
        prediction_scope_sessions=prediction_sessions,
        trial_id=authorization.trial_id,
    )
