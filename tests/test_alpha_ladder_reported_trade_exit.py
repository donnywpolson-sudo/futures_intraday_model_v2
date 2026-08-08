from __future__ import annotations

import copy
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_limit_readiness import CT, LimitBar
from futures_rebuild.alpha_ladder_reported_trade_exit_successor import (
    build_closure,
    build_successor,
    classify_reported_trade_exit,
    validate_closure,
    validate_successor,
)
from futures_rebuild.cash_open_source_compatibility_census import _read_canonical
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def _bar(minute: int, *, identity: str = "a" * 64, close: str = "100") -> LimitBar:
    event = datetime.combine(date(2020, 1, 2), time(10, 30), CT) + timedelta(minutes=minute)
    return LimitBar(event, event + timedelta(seconds=65), identity, Decimal(close),
                    Decimal(close), Decimal(close), Decimal(close), Decimal("1"),
                    Decimal("0.25"), Decimal("12.5"))


def test_reported_trade_exit_takes_first_later_bar_regardless_of_price() -> None:
    scheduled = datetime.combine(date(2020, 1, 2), time(10, 30), CT)
    bars = (_bar(0, close="100"), _bar(1, close="80"), _bar(2, close="120"))
    result = classify_reported_trade_exit(
        bars=bars, scheduled_exit_intent=scheduled, identity="a" * 64)
    assert result.complete is True
    assert result.evidence_bar == bars[1]
    assert result.evidence_bar.open == Decimal("80")
    assert result.fill_price == Decimal("80")
    assert result.fill_time == bars[1].event_at
    assert result.disposition == "VERIFIED_CAUSAL_REPORTED_TRADE_EXIT_PROXY"


def test_exit_never_uses_a_bar_before_order_time() -> None:
    scheduled = datetime.combine(date(2020, 1, 2), time(10, 30), CT)
    result = classify_reported_trade_exit(
        bars=(_bar(0), _bar(1)), scheduled_exit_intent=scheduled, identity="a" * 64)
    assert result.evidence_bar == _bar(1)
    assert result.order_time == scheduled + timedelta(seconds=5)


def test_missing_or_changed_identity_exit_fails_closed() -> None:
    scheduled = datetime.combine(date(2020, 1, 2), time(10, 30), CT)
    missing = classify_reported_trade_exit(
        bars=(_bar(0),), scheduled_exit_intent=scheduled, identity="a" * 64)
    assert missing.complete is False
    assert missing.disposition == "REPORTED_TRADE_EXIT_EVIDENCE_MISSING"
    changed = classify_reported_trade_exit(
        bars=(_bar(1, identity="b" * 64),), scheduled_exit_intent=scheduled,
        identity="a" * 64)
    assert changed.complete is False
    assert changed.disposition == "REPORTED_TRADE_EXIT_IDENTITY_CHANGING"


def test_ambiguous_or_invalid_reported_trade_exit_fails_closed() -> None:
    scheduled = datetime.combine(date(2020, 1, 2), time(10, 30), CT)
    duplicate = classify_reported_trade_exit(
        bars=(_bar(1), _bar(1)), scheduled_exit_intent=scheduled,
        identity="a" * 64)
    assert duplicate.complete is False
    assert duplicate.disposition == "REPORTED_TRADE_EXIT_EVIDENCE_AMBIGUOUS"
    invalid_bar = replace(_bar(1), volume=Decimal("0"))
    invalid = classify_reported_trade_exit(
        bars=(invalid_bar,), scheduled_exit_intent=scheduled, identity="a" * 64)
    assert invalid.complete is False
    assert invalid.disposition == "REPORTED_TRADE_EXIT_EVIDENCE_INVALID"


def test_closure_is_not_strategy_failure_and_successor_changes_only_exit_semantics() -> None:
    closure = build_closure(root=ROOT)
    validate_closure(closure, root=ROOT)
    assert closure["classification"] == (
        "PRE_REGISTRATION_SOURCE_INCOMPATIBLE_UNRESOLVED_EXIT_PATHS")
    assert closure["display_classification"] == (
        "PRE_REGISTRATION_SOURCE_INCOMPATIBLE — UNRESOLVED EXIT PATHS")
    assert closure["strategy_failure"] is False
    predecessor = _read_canonical(
        ROOT / "state/unpublished_evidence/alpha_ladder_source_compatible_successor/"
        "767ecf3987d816c2f657fbf030da25bf72511275812d6664aa6bd56faf7f3660/"
        "mechanism.json", name="predecessor")
    successor = build_successor(root=ROOT, closure=closure)
    validate_successor(successor, predecessor=predecessor, closure=closure, root=ROOT)
    assert successor["mechanism_id"] != predecessor["mechanism_id"]
    assert successor["entry_rules"] == predecessor["entry_rules"]
    assert successor["costs"] == predecessor["costs"]
    assert successor["promotion_gates"] == predecessor["promotion_gates"]
    assert successor["exit_rules"]["price_return_condition"] is None
    assert successor["restart_stage"] == "tier_0"


def test_successor_rejects_economic_drift_or_weakened_coverage() -> None:
    closure = build_closure(root=ROOT)
    predecessor = _read_canonical(
        ROOT / "state/unpublished_evidence/alpha_ladder_source_compatible_successor/"
        "767ecf3987d816c2f657fbf030da25bf72511275812d6664aa6bd56faf7f3660/"
        "mechanism.json", name="predecessor")
    successor = build_successor(root=ROOT, closure=closure)
    drifted = copy.deepcopy(successor)
    drifted["costs"]["round_trip_fee_usd"] = "0"
    drifted["mechanism_id"] = sha256_json(
        {key: value for key, value in drifted.items() if key != "mechanism_id"})
    with pytest.raises(UnauthorizedOperation, match="retained field"):
        validate_successor(drifted, predecessor=predecessor, closure=closure, root=ROOT)
    weakened = copy.deepcopy(successor)
    weakened["source_compatibility_gate"]["filled_entry_to_verified_exit_percent"] = 99
    weakened["mechanism_id"] = sha256_json(
        {key: value for key, value in weakened.items() if key != "mechanism_id"})
    with pytest.raises(IntegrityError, match="fail closed"):
        validate_successor(weakened, predecessor=predecessor, closure=closure, root=ROOT)
    rebound = copy.deepcopy(successor)
    rebound["predecessor"]["closure_id"] = "0" * 64
    rebound["mechanism_id"] = sha256_json(
        {key: value for key, value in rebound.items() if key != "mechanism_id"})
    with pytest.raises(IntegrityError, match="fail closed"):
        validate_successor(rebound, predecessor=predecessor, closure=closure, root=ROOT)
