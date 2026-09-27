"""Causal L1 BBO execution-cost validation without an impact overclaim."""

from __future__ import annotations

import math
from collections.abc import Mapping

from .canonical import sha256_json
from .errors import IntegrityError


def _digest(value: object, *, name: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise IntegrityError(f"{name} must be a SHA-256 digest")
    return value


def causal_l1_round_trip_cost(
    *,
    source_certification_id: str,
    entry_decision_ns: int,
    entry_quote_available_ns: int,
    entry_bid: float,
    entry_ask: float,
    exit_decision_ns: int,
    exit_quote_available_ns: int,
    exit_bid: float,
    exit_ask: float,
    tick_size: float,
    tick_value_usd: float,
    round_trip_fee_usd: float,
    adverse_selection_ticks: float,
    maximum_quote_age_ns: int,
) -> dict[str, object]:
    """Estimate causal spread cost; actual slippage and impact remain unidentified."""

    _digest(source_certification_id, name="L1 source certification identity")
    integer_values = (
        entry_decision_ns,
        entry_quote_available_ns,
        exit_decision_ns,
        exit_quote_available_ns,
        maximum_quote_age_ns,
    )
    if any(type(value) is not int for value in integer_values) or maximum_quote_age_ns < 0:
        raise IntegrityError("L1 timestamps and maximum age must be integers")
    if (
        entry_quote_available_ns > entry_decision_ns
        or exit_quote_available_ns > exit_decision_ns
        or entry_decision_ns - entry_quote_available_ns > maximum_quote_age_ns
        or exit_decision_ns - exit_quote_available_ns > maximum_quote_age_ns
    ):
        raise IntegrityError("L1 quote is future-known or stale; execution must abstain")
    numeric = (
        entry_bid,
        entry_ask,
        exit_bid,
        exit_ask,
        tick_size,
        tick_value_usd,
        round_trip_fee_usd,
        adverse_selection_ticks,
    )
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        for value in numeric
    ):
        raise IntegrityError("L1 execution inputs must be finite numbers")
    if (
        min(entry_bid, entry_ask, exit_bid, exit_ask, tick_size, tick_value_usd) <= 0.0
        or entry_ask < entry_bid
        or exit_ask < exit_bid
        or round_trip_fee_usd < 0.0
        or adverse_selection_ticks < 0.0
    ):
        raise IntegrityError("L1 quote or cost assumptions are invalid")
    entry_spread_ticks = (float(entry_ask) - float(entry_bid)) / float(tick_size)
    exit_spread_ticks = (float(exit_ask) - float(exit_bid)) / float(tick_size)
    if not math.isclose(entry_spread_ticks, round(entry_spread_ticks), abs_tol=1e-9) or not math.isclose(
        exit_spread_ticks, round(exit_spread_ticks), abs_tol=1e-9
    ):
        raise IntegrityError("L1 spread is not aligned to the contract tick")
    quoted_spread_cost_usd = (
        0.5 * (entry_spread_ticks + exit_spread_ticks) * float(tick_value_usd)
    )
    adverse_selection_cost_usd = float(adverse_selection_ticks) * float(tick_value_usd)
    total = quoted_spread_cost_usd + adverse_selection_cost_usd + float(round_trip_fee_usd)
    core: dict[str, object] = {
        "schema_version": "causal_l1_round_trip_cost/1.0.0",
        "source_certification_id": source_certification_id,
        "entry": {
            "decision_ns": entry_decision_ns,
            "quote_available_ns": entry_quote_available_ns,
            "quote_age_ns": entry_decision_ns - entry_quote_available_ns,
            "bid": float(entry_bid),
            "ask": float(entry_ask),
            "spread_ticks": float(entry_spread_ticks),
        },
        "exit": {
            "decision_ns": exit_decision_ns,
            "quote_available_ns": exit_quote_available_ns,
            "quote_age_ns": exit_decision_ns - exit_quote_available_ns,
            "bid": float(exit_bid),
            "ask": float(exit_ask),
            "spread_ticks": float(exit_spread_ticks),
        },
        "tick_size": float(tick_size),
        "tick_value_usd": float(tick_value_usd),
        "adverse_selection_ticks": float(adverse_selection_ticks),
        "maximum_quote_age_ns": maximum_quote_age_ns,
        "quoted_spread_cost_usd": quoted_spread_cost_usd,
        "adverse_selection_proxy_cost_usd": adverse_selection_cost_usd,
        "round_trip_fee_usd": float(round_trip_fee_usd),
        "estimated_round_trip_cost_usd": total,
        "actual_slippage_status": "NOT_IDENTIFIED_FROM_L1_BBO",
        "market_impact_status": "NOT_IDENTIFIED_FROM_L1_BBO",
        "future_volume_used": False,
        "status": "CAUSAL_L1_SPREAD_COST_VALIDATED",
        "authority": {
            "provider": False,
            "trading": False,
            "holdout": False,
            "activation": False,
        },
    }
    return {**core, "cost_validation_id": sha256_json(core)}


def validate_causal_l1_cost(payload: Mapping[str, object]) -> dict[str, object]:
    try:
        entry = payload["entry"]
        exit_ = payload["exit"]
        if not isinstance(entry, Mapping) or not isinstance(exit_, Mapping):
            raise TypeError
        expected = causal_l1_round_trip_cost(
            source_certification_id=str(payload["source_certification_id"]),
            entry_decision_ns=entry["decision_ns"],
            entry_quote_available_ns=entry["quote_available_ns"],
            entry_bid=entry["bid"],
            entry_ask=entry["ask"],
            exit_decision_ns=exit_["decision_ns"],
            exit_quote_available_ns=exit_["quote_available_ns"],
            exit_bid=exit_["bid"],
            exit_ask=exit_["ask"],
            tick_size=payload["tick_size"],
            tick_value_usd=payload["tick_value_usd"],
            round_trip_fee_usd=payload["round_trip_fee_usd"],
            adverse_selection_ticks=payload["adverse_selection_ticks"],
            maximum_quote_age_ns=payload["maximum_quote_age_ns"],
        )
    except (KeyError, TypeError, ValueError, ZeroDivisionError, IntegrityError) as exc:
        raise IntegrityError("causal L1 cost validation is invalid") from exc
    if dict(payload) != expected:
        raise IntegrityError("causal L1 cost validation is invalid")
    return dict(payload)
