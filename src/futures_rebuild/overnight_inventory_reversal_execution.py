"""Pure mechanics for the fixed overnight-inventory-reversal trial.

This module accepts already-authorized in-memory source records.  It never
discovers files, opens Parquet, writes evidence, or grants historical access.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, time, timezone
from decimal import Decimal, ROUND_FLOOR
from statistics import median
from collections.abc import Iterable, Iterator
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

from .errors import IntegrityError, UnauthorizedOperation
from .tier1_bracket_v5 import NS_PER_MINUTE, V5SourceRecord


CHICAGO = ZoneInfo("America/Chicago")
MARKETS = ("ES", "CL", "ZN", "6E")
THRESHOLD = 1.5
MAD_SCALE = 1.4826
MINIMUM_SCALE_SESSIONS = 252
FEE_PER_SIDE_USD = Decimal("5")
MAXIMUM_PLANNED_INITIAL_LOSS_USD = Decimal("250")
COST_TICKS = {
    "base": {"ES": 2, "CL": 4, "ZN": 2, "6E": 2},
    "stress": {"ES": 4, "CL": 8, "ZN": 4, "6E": 4},
    "extreme": {"ES": 8, "CL": 16, "ZN": 8, "6E": 8},
}
BASELINES = (
    "flat_no_trade",
    "overnight_displacement_continuation",
    "fold_local_unconditional_direction_by_market",
    "previous_session_sign_momentum",
    "previous_session_sign_reversal",
    "risk_matched_always_long_at_candidate_eligible_events",
)


@dataclass(frozen=True)
class SessionObservation:
    market: str
    session: str
    overnight_return: float | None
    execution_path: tuple[V5SourceRecord, ...]
    prior_session_direction: int | None
    complete: bool
    failure: str | None


@dataclass(frozen=True)
class FoldScale:
    market: str
    fold: int
    fit_start: str
    fit_end: str
    location: float
    scale: float
    unconditional_direction: int
    training_sessions: int


@dataclass(frozen=True)
class SessionEvaluation:
    market: str
    session: str
    fold: int
    complete: bool
    candidate_eligible: bool
    standardized_displacement: float | None
    candidate_net_pnl_usd: Decimal | None
    baseline_net_pnl_usd: Mapping[str, Decimal | None]
    failure: str | None


@dataclass(frozen=True)
class TrialEvaluation:
    cost_scenario: str
    sessions: tuple[SessionEvaluation, ...]
    fold_scales: tuple[FoldScale, ...]
    complete_portfolio_sessions: tuple[str, ...]
    portfolio_net_pnl_by_session: Mapping[str, Decimal]
    baseline_portfolio_net_pnl_by_session: Mapping[str, Mapping[str, Decimal]]
    incomplete_market_sessions: int
    candidate_trade_count: int


def _local_time(event_at_ns: int) -> time:
    seconds, remainder = divmod(event_at_ns, 1_000_000_000)
    value = datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(CHICAGO)
    return value.time().replace(microsecond=remainder // 1_000)


def _sign(value: Decimal | float) -> int:
    return 1 if value > 0 else -1 if value < 0 else 0


def _session_observation(
    *, market: str, session: str, rows: Sequence[V5SourceRecord],
    prior_session_direction: int | None,
) -> SessionObservation:
    ordered = tuple(sorted(rows, key=lambda item: item.bar.event_at_ns if item.bar else -1))
    executable = tuple(item for item in ordered if item.executable and item.bar is not None)
    events = [item.bar.event_at_ns for item in executable if item.bar is not None]
    if len(events) != len(set(events)):
        return SessionObservation(
            market, session, None, (), prior_session_direction, False,
            "DUPLICATE_EXECUTABLE_EVENT",
        )
    overnight = tuple(
        item for item in executable
        if (_local_time(item.bar.event_at_ns) >= time(17, 0)
            or _local_time(item.bar.event_at_ns) <= time(8, 29))
    )
    entry = tuple(
        item for item in executable
        if _local_time(item.bar.event_at_ns) == time(8, 31)
    )
    scheduled_exit = tuple(
        item for item in executable
        if _local_time(item.bar.event_at_ns) == time(9, 31)
    )
    if not overnight or len(entry) != 1 or len(scheduled_exit) != 1:
        return SessionObservation(
            market, session, None, (), prior_session_direction, False,
            "MISSING_OVERNIGHT_ENTRY_OR_EXIT",
        )
    first = overnight[0]
    last = overnight[-1]
    assert first.bar is not None and last.bar is not None
    if _local_time(last.bar.event_at_ns) != time(8, 29):
        return SessionObservation(
            market, session, None, (), prior_session_direction, False,
            "MISSING_EXACT_08_29_FEATURE_BAR",
        )
    # The source contract makes an OHLCV minute available five seconds after
    # its interval end.  The corrected pre-outcome build therefore uses an
    # 08:30:05 decision, which still precedes the exact 08:31 entry.
    decision_at_ns = last.bar.event_at_ns + NS_PER_MINUTE + 5_000_000_000
    entry_event = entry[0].bar.event_at_ns  # type: ignore[union-attr]
    if last.bar.available_at_ns > decision_at_ns or decision_at_ns >= entry_event:
        raise IntegrityError("overnight feature is not causal at the fixed decision")
    exit_event = scheduled_exit[0].bar.event_at_ns  # type: ignore[union-attr]
    required = set(range(entry_event, exit_event + NS_PER_MINUTE, NS_PER_MINUTE))
    path = tuple(
        item for item in executable
        if item.bar is not None and item.bar.event_at_ns in required
    )
    identities = {
        item.actual_identity_hash for item in (*overnight, *path)
        if item.actual_identity_hash is not None
    }
    specs = {item.market_spec for item in (*overnight, *path) if item.market_spec is not None}
    if (
        {item.bar.event_at_ns for item in path if item.bar is not None} != required
        or len(path) != 61
        or len(identities) != 1
        or len(specs) != 1
    ):
        return SessionObservation(
            market, session, None, (), prior_session_direction, False,
            "INCOMPLETE_OR_IDENTITY_CHANGING_EXECUTION_PATH",
        )
    overnight_return = math.log(float(last.bar.close_price / first.bar.open_price))
    if not math.isfinite(overnight_return):
        raise IntegrityError("overnight return is non-finite")
    return SessionObservation(
        market, session, overnight_return, path, prior_session_direction, True, None,
    )


def build_session_observations(
    *, source_records: Sequence[V5SourceRecord],
) -> tuple[SessionObservation, ...]:
    """Materialize fixed causal sessions without fitting or evaluating them."""

    grouped: dict[tuple[str, str], list[V5SourceRecord]] = {}
    for row in source_records:
        row.validate()
        if row.exchange_session_date.startswith("2025"):
            raise UnauthorizedOperation("2025 holdout row is rejected before evaluation")
        try:
            year = int(row.exchange_session_date[:4])
        except ValueError as exc:
            raise IntegrityError("source session has no registered year") from exc
        if row.market not in MARKETS or year not in range(2018, 2023):
            raise IntegrityError("source row is outside the registered 2018-2022 scope")
        grouped.setdefault((row.market, row.exchange_session_date), []).append(row)
    output: list[SessionObservation] = []
    for market in MARKETS:
        sessions = sorted(session for item_market, session in grouped if item_market == market)
        prior_direction: int | None = None
        for session in sessions:
            rows = grouped[(market, session)]
            observation = _session_observation(
                market=market, session=session, rows=rows,
                prior_session_direction=prior_direction,
            )
            output.append(observation)
            executable = sorted(
                (item for item in rows if item.executable and item.bar is not None),
                key=lambda item: item.bar.event_at_ns,  # type: ignore[union-attr]
            )
            if executable:
                first_bar = executable[0].bar
                last_bar = executable[-1].bar
                assert first_bar is not None and last_bar is not None
                prior_direction = _sign(last_bar.close_price - first_bar.open_price)
    return tuple(output)


def iter_ordered_session_observations(
    *, market: str, source_records: Iterable[V5SourceRecord],
) -> Iterator[SessionObservation]:
    """Bounded-memory materializer for one market's ordered 2018-2022 stream."""

    if market not in MARKETS:
        raise IntegrityError("ordered observation stream has an invalid market")
    current_session: str | None = None
    current_rows: list[V5SourceRecord] = []
    prior_direction: int | None = None
    previous_event = -1

    def finish() -> tuple[SessionObservation, int | None]:
        assert current_session is not None and current_rows
        observation = _session_observation(
            market=market, session=current_session, rows=current_rows,
            prior_session_direction=prior_direction,
        )
        executable = [
            item for item in current_rows if item.executable and item.bar is not None
        ]
        direction = prior_direction
        if executable:
            first_bar = executable[0].bar
            last_bar = executable[-1].bar
            assert first_bar is not None and last_bar is not None
            direction = _sign(last_bar.close_price - first_bar.open_price)
        return observation, direction

    for row in source_records:
        row.validate()
        if row.market != market:
            raise IntegrityError("ordered source stream changes market")
        if row.exchange_session_date.startswith("2025"):
            raise UnauthorizedOperation("2025 holdout row is rejected before evaluation")
        year = int(row.exchange_session_date[:4])
        if year not in range(2018, 2023):
            raise IntegrityError("ordered source stream leaves 2018-2022")
        event = row.bar.event_at_ns if row.bar is not None else previous_event + 1
        if current_session is not None and row.exchange_session_date < current_session:
            raise IntegrityError("ordered source stream moves backwards by session")
        if current_session == row.exchange_session_date and event < previous_event:
            raise IntegrityError("ordered source stream moves backwards within a session")
        if current_session is not None and row.exchange_session_date != current_session:
            observation, prior_direction = finish()
            yield observation
            current_rows = []
            previous_event = -1
        current_session = row.exchange_session_date
        current_rows.append(row)
        previous_event = event
    if current_session is not None:
        observation, _ = finish()
        yield observation


def _scheduled_long_gross(observation: SessionObservation) -> Decimal:
    if not observation.complete or not observation.execution_path:
        raise IntegrityError("scheduled outcome requires a complete session")
    first, last = observation.execution_path[0], observation.execution_path[-1]
    assert first.bar is not None and last.bar is not None and first.market_spec is not None
    return (last.bar.open_price - first.bar.open_price) * first.market_spec.point_value


def fit_fold_scale(
    *, observations: Sequence[SessionObservation], market: str, fold: int,
    fit_start: str, fit_end: str,
) -> FoldScale:
    training = tuple(
        item for item in observations
        if item.market == market and fit_start <= item.session <= fit_end
        and item.complete and item.overnight_return is not None
    )
    if len(training) < MINIMUM_SCALE_SESSIONS:
        raise IntegrityError("fold has fewer than 252 complete training sessions")
    values = [item.overnight_return for item in training]
    location = median(values)
    scale = MAD_SCALE * median(abs(value - location) for value in values)
    if not math.isfinite(scale) or scale <= 0:
        raise IntegrityError("fold-local overnight scale is degenerate")
    mean_long = sum((_scheduled_long_gross(item) for item in training), Decimal())
    unconditional = 1 if mean_long >= 0 else -1
    return FoldScale(
        market, fold, fit_start, fit_end, location, scale,
        unconditional, len(training),
    )


def _net_for_direction(
    observation: SessionObservation, direction: int, *, cost_scenario: str,
) -> Decimal:
    if direction not in {-1, 1} or not observation.complete:
        raise IntegrityError("fixed execution requires a complete signed session")
    path = observation.execution_path
    first = path[0]
    assert first.bar is not None and first.market_spec is not None
    spec = first.market_spec
    if cost_scenario not in COST_TICKS:
        raise IntegrityError("unknown cost scenario")
    costs = (
        Decimal(2) * FEE_PER_SIDE_USD
        + Decimal(COST_TICKS[cost_scenario][observation.market]) * spec.tick_value
    )
    stop_ticks = int(
        ((MAXIMUM_PLANNED_INITIAL_LOSS_USD - costs) / spec.tick_value)
        .to_integral_value(rounding=ROUND_FLOOR)
    )
    stop_ticks = max(1, stop_ticks)
    entry = first.bar.open_price
    stop = entry - Decimal(direction) * Decimal(stop_ticks) * spec.tick_size
    exit_price: Decimal | None = None
    for index, item in enumerate(path[:-1]):
        assert item.bar is not None
        bar = item.bar
        if index > 0 and (
            (direction == 1 and bar.open_price <= stop)
            or (direction == -1 and bar.open_price >= stop)
        ):
            exit_price = bar.open_price
            break
        if (
            (direction == 1 and bar.low_price <= stop)
            or (direction == -1 and bar.high_price >= stop)
        ):
            exit_price = stop
            break
    if exit_price is None:
        final_bar = path[-1].bar
        assert final_bar is not None
        exit_price = final_bar.open_price
    gross = Decimal(direction) * (exit_price - entry) * spec.point_value
    return gross - costs


def evaluate_fixed_trial(
    *, observations: Sequence[SessionObservation],
    outer_folds: Sequence[Mapping[str, object]],
    cost_scenario: str = "stress",
) -> TrialEvaluation:
    """Execute every frozen outer fold once under stress costs."""

    by_key = {(item.market, item.session): item for item in observations}
    if len(by_key) != len(observations):
        raise IntegrityError("session observations are duplicated")
    results: list[SessionEvaluation] = []
    scales: list[FoldScale] = []
    seen_tests: set[str] = set()
    for fold_number, raw in enumerate(outer_folds):
        fit_range = raw.get("outer_fit_session_range")
        test_range = raw.get("outer_test_session_dates")
        if (
            not isinstance(fit_range, list) or len(fit_range) != 2
            or not isinstance(test_range, list) or len(test_range) != 2
            or not all(isinstance(value, str) for value in (*fit_range, *test_range))
        ):
            raise IntegrityError("outer fold is malformed")
        fit_start, fit_end = fit_range
        test_start, test_end = test_range
        for market in MARKETS:
            scale = fit_fold_scale(
                observations=observations, market=market, fold=fold_number,
                fit_start=fit_start, fit_end=fit_end,
            )
            scales.append(scale)
            test_sessions = sorted(
                session for item_market, session in by_key
                if item_market == market and test_start <= session <= test_end
            )
            for session in test_sessions:
                if f"{market}/{session}" in seen_tests:
                    raise IntegrityError("outer test session is reused")
                seen_tests.add(f"{market}/{session}")
                observation = by_key[(market, session)]
                empty = {name: None for name in BASELINES}
                if not observation.complete or observation.overnight_return is None:
                    results.append(SessionEvaluation(
                        market, session, fold_number, False, False, None, None,
                        empty, observation.failure,
                    ))
                    continue
                z_value = (observation.overnight_return - scale.location) / scale.scale
                eligible = abs(z_value) >= THRESHOLD
                if not eligible:
                    zeros = {name: Decimal() for name in BASELINES}
                    results.append(SessionEvaluation(
                        market, session, fold_number, True, False, z_value,
                        Decimal(), zeros, None,
                    ))
                    continue
                displacement = _sign(z_value)
                candidate = _net_for_direction(
                    observation, -displacement, cost_scenario=cost_scenario,
                )
                prior = observation.prior_session_direction
                baselines: dict[str, Decimal | None] = {
                    "flat_no_trade": Decimal(),
                    "overnight_displacement_continuation": _net_for_direction(
                        observation, displacement, cost_scenario=cost_scenario,
                    ),
                    "fold_local_unconditional_direction_by_market": _net_for_direction(
                        observation, scale.unconditional_direction,
                        cost_scenario=cost_scenario,
                    ),
                    "previous_session_sign_momentum": (
                        _net_for_direction(
                            observation, prior, cost_scenario=cost_scenario,
                        ) if prior in {-1, 1} else None
                    ),
                    "previous_session_sign_reversal": (
                        _net_for_direction(
                            observation, -prior, cost_scenario=cost_scenario,
                        ) if prior in {-1, 1} else None
                    ),
                    "risk_matched_always_long_at_candidate_eligible_events": (
                        _net_for_direction(
                            observation, 1, cost_scenario=cost_scenario,
                        )
                    ),
                }
                complete = all(value is not None for value in baselines.values())
                results.append(SessionEvaluation(
                    market, session, fold_number, complete, True, z_value,
                    candidate if complete else None, baselines,
                    None if complete else "MISSING_PRIOR_SESSION_BASELINE",
                ))

    grouped: dict[str, list[SessionEvaluation]] = {}
    for item in results:
        grouped.setdefault(item.session, []).append(item)
    complete_dates: list[str] = []
    portfolio: dict[str, Decimal] = {}
    baseline_portfolio: dict[str, dict[str, Decimal]] = {}
    for session, rows in sorted(grouped.items()):
        if {item.market for item in rows} != set(MARKETS) or not all(item.complete for item in rows):
            continue
        complete_dates.append(session)
        portfolio[session] = sum(
            (item.candidate_net_pnl_usd for item in rows if item.candidate_net_pnl_usd is not None),
            Decimal(),
        )
        baseline_portfolio[session] = {
            name: sum(
                (item.baseline_net_pnl_usd[name] for item in rows
                 if item.baseline_net_pnl_usd[name] is not None),
                Decimal(),
            )
            for name in BASELINES
        }
    return TrialEvaluation(
        cost_scenario, tuple(results), tuple(scales), tuple(complete_dates), portfolio,
        baseline_portfolio,
        sum(not item.complete for item in results),
        sum(item.candidate_eligible and item.complete for item in results),
    )
