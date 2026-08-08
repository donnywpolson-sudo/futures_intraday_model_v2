"""Portfolio-risk scheduling for the new directional bracket predictions.

This consumes already-created prediction/outcome rows.  It is intentionally
separate from the old Phase 6 scheduler because its score and targets differ.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Mapping, Sequence

from .errors import IntegrityError


Direction = Literal["long", "short"]
ZERO = Decimal("0")


@dataclass(frozen=True)
class BracketScheduleCandidate:
    market: str
    exchange_session_date: str
    entry_at_ns: int
    exit_at_ns: int
    direction: Direction
    bounded_signal: float
    planned_all_in_risk_usd: Decimal
    realized_net_pnl_usd: Decimal

    def validate(self) -> None:
        if (
            self.market not in {"ES", "CL", "ZN", "6E"}
            or not isinstance(self.exchange_session_date, str)
            or type(self.entry_at_ns) is not int
            or type(self.exit_at_ns) is not int
            or self.exit_at_ns <= self.entry_at_ns
            or self.direction not in {"long", "short"}
            or not isinstance(self.bounded_signal, float)
            or not self.planned_all_in_risk_usd.is_finite()
            or not self.realized_net_pnl_usd.is_finite()
            or self.planned_all_in_risk_usd <= ZERO
        ):
            raise IntegrityError("bracket scheduling candidate is invalid")


@dataclass(frozen=True)
class BracketSchedule:
    admitted: tuple[BracketScheduleCandidate, ...]
    neutral_abstentions: int
    simultaneous_abstentions: int
    overlap_abstentions: int
    entry_cap_abstentions: int
    daily_stop_abstentions: int
    drawdown_stop_abstentions: int


def candidates_from_directional_rows(
    *, prediction_rows: Sequence[Mapping[str, object]], outcome_rows: Sequence[Mapping[str, object]],
) -> tuple[BracketScheduleCandidate, ...]:
    """Join a frozen selected direction to its exact long or short label."""

    outcomes = {row.get("upstream_source_row_sha256"): row for row in outcome_rows}
    if len(outcomes) != len(outcome_rows) or None in outcomes:
        raise IntegrityError("bracket outcome rows have duplicate or invalid source hashes")
    candidates: list[BracketScheduleCandidate] = []
    for prediction in prediction_rows:
        source = prediction.get("upstream_source_row_sha256")
        outcome = outcomes.get(source)
        if not isinstance(source, str) or outcome is None:
            raise IntegrityError("bracket prediction lacks its exact outcome row")
        direction = prediction.get("selected_direction")
        if direction == "neutral":
            continue
        if direction not in {"long", "short"}:
            raise IntegrityError("bracket prediction direction is invalid")
        prefix = str(direction)
        try:
            risk = Decimal(str(outcome[f"{prefix}_planned_all_in_risk_usd"]))
            net_r = Decimal(str(outcome[f"{prefix}_realized_net_r"]))
        except (KeyError, ArithmeticError) as exc:
            raise IntegrityError("bracket prediction lacks a matured selected-direction label") from exc
        exit_at = outcome.get(f"{prefix}_exit_at_ns")
        if type(exit_at) is not int:
            raise IntegrityError("bracket selected-direction outcome lacks an exit time")
        signal = prediction.get("bounded_signal")
        if not isinstance(signal, float):
            raise IntegrityError("bracket prediction signal is invalid")
        market, session, entry = prediction.get("market"), prediction.get("exchange_session_date"), prediction.get("entry_at_ns")
        if not isinstance(market, str) or not isinstance(session, str) or type(entry) is not int:
            raise IntegrityError("bracket prediction scope is invalid")
        candidates.append(BracketScheduleCandidate(
            market, session, entry, exit_at, direction, signal, risk, net_r * risk,
        ))
    return tuple(candidates)


def schedule_bracket_candidates(
    *, candidates: Sequence[BracketScheduleCandidate], maximum_initial_risk_usd: Decimal = Decimal("250"),
    maximum_entries_per_session: int = 3, daily_stop_loss_usd: Decimal = Decimal("500"),
    maximum_total_drawdown_usd: Decimal = Decimal("1500"),
) -> BracketSchedule:
    """Apply the locked one-position, entry-cap, daily, and drawdown controls.

    A daily or drawdown stop can only block entries *after* a completed prior
    trade reveals the loss.  A gap through a bracket stop remains represented
    in that prior trade's net P&L; it is never made harmless retroactively.
    """

    if (
        maximum_initial_risk_usd != Decimal("250")
        or maximum_entries_per_session != 3
        or daily_stop_loss_usd != Decimal("500")
        or maximum_total_drawdown_usd != Decimal("1500")
    ):
        raise IntegrityError("bracket scheduler limits must match the locked Tier 1 policy")
    for candidate in candidates:
        candidate.validate()
    grouped: dict[int, list[BracketScheduleCandidate]] = {}
    for candidate in candidates:
        if candidate.planned_all_in_risk_usd > maximum_initial_risk_usd:
            continue
        grouped.setdefault(candidate.entry_at_ns, []).append(candidate)
    admitted: list[BracketScheduleCandidate] = []
    entries_by_session: dict[str, int] = {}
    realized_by_session: dict[str, Decimal] = {}
    equity = ZERO
    peak = ZERO
    open_until = -1
    simultaneous = overlap = entry_cap = daily_stop = drawdown_stop = 0
    market_order = {market: index for index, market in enumerate(("ES", "CL", "ZN", "6E"))}
    for entry_at in sorted(grouped):
        group = grouped[entry_at]
        if entry_at < open_until:
            overlap += len(group)
            continue
        # Earlier entries are closed by this point because there is one open
        # position maximum.  Their realized result is now actionable risk data.
        eligible: list[BracketScheduleCandidate] = []
        for candidate in group:
            session_pnl = realized_by_session.get(candidate.exchange_session_date, ZERO)
            if session_pnl <= -daily_stop_loss_usd:
                daily_stop += 1
            elif peak - equity >= maximum_total_drawdown_usd:
                drawdown_stop += 1
            elif entries_by_session.get(candidate.exchange_session_date, 0) >= maximum_entries_per_session:
                entry_cap += 1
            else:
                eligible.append(candidate)
        if not eligible:
            continue
        ranked = sorted(eligible, key=lambda item: (-abs(item.bounded_signal), market_order[item.market], item.direction))
        selected = ranked[0]
        simultaneous += len(ranked) - 1
        admitted.append(selected)
        entries_by_session[selected.exchange_session_date] = entries_by_session.get(selected.exchange_session_date, 0) + 1
        realized_by_session[selected.exchange_session_date] = realized_by_session.get(selected.exchange_session_date, ZERO) + selected.realized_net_pnl_usd
        equity += selected.realized_net_pnl_usd
        peak = max(peak, equity)
        open_until = selected.exit_at_ns
    return BracketSchedule(tuple(admitted), 0, simultaneous, overlap, entry_cap, daily_stop, drawdown_stop)
