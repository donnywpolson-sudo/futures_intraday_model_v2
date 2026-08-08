"""Pure rows and signal rules for the registered Tier 1 bracket successor.

This module deliberately performs no filesystem or provider I/O.  The later
high-risk materializer supplies verified source bars, persists rows, and fits
models only after conversational approval.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Sequence

from .errors import IntegrityError
from .tier1_bracket_trial import BracketBar, BracketOutcome, build_directional_bracket_outcome


DiagnosticClass = Literal["TARGET_FIRST", "STOP_FIRST", "VERTICAL_OR_SAFETY_EXIT", "UNAVAILABLE"]
SelectedDirection = Literal["long", "short", "neutral"]


@dataclass(frozen=True)
class DirectionalBracketRows:
    """A causal feature row and both directional labels for one decision bar."""

    decision_at_ns: int
    label_unlock_at_ns: int
    features: dict[str, float]
    long_net_r: Decimal | None
    short_net_r: Decimal | None
    long_planned_all_in_risk_usd: Decimal | None
    short_planned_all_in_risk_usd: Decimal | None
    long_realized_gross_pnl_usd: Decimal | None
    short_realized_gross_pnl_usd: Decimal | None
    long_exit_at_ns: int | None
    short_exit_at_ns: int | None
    long_diagnostic: DiagnosticClass
    short_diagnostic: DiagnosticClass
    long_exit_reason: str | None
    short_exit_reason: str | None


def diagnostic_class(outcome: BracketOutcome) -> DiagnosticClass:
    """Map observable bracket exits to a diagnostic triple-barrier class."""

    if outcome.status != "MATURED" or outcome.exit_reason is None:
        return "UNAVAILABLE"
    if outcome.exit_reason == "TARGET":
        return "TARGET_FIRST"
    if outcome.exit_reason in {"STOP", "STOP_GAP", "STOP_FIRST_COLLISION"}:
        return "STOP_FIRST"
    if outcome.exit_reason in {"MAX_HOLD", "SESSION_END", "ROLL_BOUNDARY"}:
        return "VERTICAL_OR_SAFETY_EXIT"
    raise IntegrityError("bracket outcome has an unknown exit reason")


def mechanical_features(*, decision: BracketBar) -> dict[str, float]:
    """Return only values known at the completed decision bar."""

    decision.validate()
    if decision.open_nano <= 0:
        raise IntegrityError("feature source open must be positive")
    open_price = float(decision.open_nano)
    return {
        "bar_body_fraction": (decision.close_nano - decision.open_nano) / open_price,
        "bar_return": decision.close_nano / open_price - 1.0,
        "intrabar_range_fraction": (decision.high_nano - decision.low_nano) / open_price,
    }


def materialize_directional_row(
    *, bars: Sequence[BracketBar], decision_index: int, tick_size_nano: int,
    tick_value_usd: Decimal, stress_round_trip_cost_usd: Decimal, volume: float,
) -> DirectionalBracketRows:
    """Generate fresh causal features and paired net-R labels from supplied bars."""

    if not math.isfinite(volume) or volume < 0:
        raise IntegrityError("feature source volume must be finite and non-negative")
    decision = bars[decision_index]
    features = mechanical_features(decision=decision)
    features["volume"] = volume
    long = build_directional_bracket_outcome(
        bars=bars, decision_index=decision_index, direction="long", tick_size_nano=tick_size_nano,
        tick_value_usd=tick_value_usd, stress_round_trip_cost_usd=stress_round_trip_cost_usd,
    )
    short = build_directional_bracket_outcome(
        bars=bars, decision_index=decision_index, direction="short", tick_size_nano=tick_size_nano,
        tick_value_usd=tick_value_usd, stress_round_trip_cost_usd=stress_round_trip_cost_usd,
    )
    return DirectionalBracketRows(
        decision_at_ns=decision.event_at_ns,
        label_unlock_at_ns=decision.event_at_ns + 60 * 60_000_000_000,
        features=features,
        long_net_r=long.realized_net_r if long.status == "MATURED" else None,
        short_net_r=short.realized_net_r if short.status == "MATURED" else None,
        long_planned_all_in_risk_usd=long.planned_all_in_risk_usd if long.status == "MATURED" else None,
        short_planned_all_in_risk_usd=short.planned_all_in_risk_usd if short.status == "MATURED" else None,
        long_realized_gross_pnl_usd=long.realized_gross_pnl_usd if long.status == "MATURED" else None,
        short_realized_gross_pnl_usd=short.realized_gross_pnl_usd if short.status == "MATURED" else None,
        long_exit_at_ns=long.exit_at_ns if long.status == "MATURED" else None,
        short_exit_at_ns=short.exit_at_ns if short.status == "MATURED" else None,
        long_diagnostic=diagnostic_class(long), short_diagnostic=diagnostic_class(short),
        long_exit_reason=long.exit_reason, short_exit_reason=short.exit_reason,
    )


def bounded_direction_score(*, long_prediction_net_r: float, short_prediction_net_r: float) -> float | None:
    """Return a bounded directional preference, or None for unusable estimates."""

    if not math.isfinite(long_prediction_net_r) or not math.isfinite(short_prediction_net_r):
        return None
    denominator = abs(long_prediction_net_r) + abs(short_prediction_net_r) + 1e-12
    return (long_prediction_net_r - short_prediction_net_r) / denominator


def training_only_neutral_threshold(*, training_scores: Sequence[float], quantile: float = 0.60) -> float:
    """Nearest-rank threshold of absolute *training* scores only."""

    if quantile != 0.60:
        raise IntegrityError("Tier 1 bracket neutral threshold is locked at the 60th percentile")
    values = sorted(abs(value) for value in training_scores if math.isfinite(value))
    if not values:
        raise IntegrityError("training-only neutral threshold requires finite training scores")
    index = math.ceil(quantile * len(values)) - 1
    return values[index]


def select_direction(*, score: float | None, threshold: float) -> SelectedDirection:
    """Select long/short only beyond the frozen training-derived threshold."""

    if not math.isfinite(threshold) or threshold < 0:
        raise IntegrityError("neutral threshold must be finite and non-negative")
    if score is None:
        return "neutral"
    if score >= threshold:
        return "long"
    if score <= -threshold:
        return "short"
    return "neutral"
