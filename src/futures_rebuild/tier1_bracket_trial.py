"""Pure, research-only mechanics for the versioned Tier 1 bracket trial.

The module accepts only caller-supplied bars.  It never opens releases, calls a
provider, produces predictions, or writes a research artifact.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Literal, Mapping, Sequence

from .canonical import sha256_json
from .errors import IntegrityError


Direction = Literal["long", "short"]
NS_PER_MINUTE = 60_000_000_000
ATR_PERIOD = 20
MAXIMUM_HOLD_MINUTES = 60
MAXIMUM_INITIAL_RISK_USD = Decimal("250")
TRIAL_CONFIG_RELATIVE_PATH = Path("configs/tier1_bracket_trial.json")
REGISTERED_TRIAL_ROOT = Path("state/trial_registry/tier1_bracket_prediction")


def _decimal(value: object, *, name: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise IntegrityError(f"{name} must be a finite Decimal")
    return value


def load_tier1_bracket_trial_contract(*, root: Path) -> dict[str, object]:
    """Load the fixed local-only bracket-trial contract without opening data."""

    try:
        payload = json.loads((root / TRIAL_CONFIG_RELATIVE_PATH).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("Tier 1 bracket trial contract is unreadable") from exc
    expected = {
        "schema_version": "tier1_bracket_trial/1.0.0",
        "trial_status": "LOCAL_IMPLEMENTATION_ONLY_NOT_REGISTERED",
        "research_only": True,
        "live_readiness": False,
        "markets": ["ES", "CL", "ZN", "6E"],
        "discovery_period": "2018-2022",
        "locked_untouched_holdout": "2025",
        "bar_resolution": "ohlcv_1m",
        "entry_delay_bars": 1,
        "directions": ["long", "short"],
        "label_unlock_rule": "entry_at_plus_60_minutes_even_when_exit_occurs_earlier",
        "split_purge_horizon_minutes": 60,
        "baseline_execution_parity_required": True,
        "forbidden_claims": [
            "live_execution_realism",
            "apex_readiness",
            "exact_zn_fees",
            "margin_or_buying_power_validation",
        ],
    }
    if payload != expected:
        raise IntegrityError("Tier 1 bracket trial contract is incomplete or drifted")
    return payload


def load_registered_tier1_bracket_trial(*, root: Path) -> dict[str, object] | None:
    """Read the one create-only trial record; the config is only its template."""

    registry_root = root / REGISTERED_TRIAL_ROOT
    records = tuple(sorted(registry_root.glob("*.json"))) if registry_root.is_dir() else ()
    if not records:
        return None
    # Historic declarations remain immutable.  A current successor may coexist
    # only when it explicitly supersedes the historic economics binding.
    current = tuple(path for path in records if 'current' in path.stem)
    if len(records) != 1 and len(current) != 1:
        raise IntegrityError("Tier 1 bracket trial registry has no unique current record")
    record_path = current[0] if current else records[0]
    try:
        record = json.loads(record_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("Tier 1 bracket trial registry is unreadable") from exc
    if not isinstance(record, dict):
        raise IntegrityError("Tier 1 bracket trial registry must be an object")
    trial_id = record.get("trial_id")
    core = {key: value for key, value in record.items() if key not in {"trial_id", "registered_at_utc"}}
    template = record.get("bracket_contract")
    if (
        not isinstance(trial_id, str)
        or sha256_json(core) != trial_id
        or record.get("state") not in {"REGISTERED_BEFORE_BRACKET_SOURCE_ROW_OPEN", "CURRENT_REGISTERED_BEFORE_BRACKET_SOURCE_ROW_OPEN"}
        or not isinstance(template, dict)
        or template.get("trial_status") != "LOCAL_IMPLEMENTATION_ONLY_NOT_REGISTERED"
    ):
        raise IntegrityError("Tier 1 bracket trial registry is inconsistent")
    return {
        "trial_id": trial_id,
        "registration_state": str(record["state"]),
        "template_state": str(template["trial_status"]),
        "registry_path": record_path.relative_to(root).as_posix(),
    }


@dataclass(frozen=True)
class BracketBar:
    event_at_ns: int
    open_nano: int
    high_nano: int
    low_nano: int
    close_nano: int
    session: str
    actual_identity_hash: str
    eligible: bool = True

    def validate(self) -> None:
        if (
            type(self.event_at_ns) is not int
            or not all(type(value) is int and value > 0 for value in (self.open_nano, self.high_nano, self.low_nano, self.close_nano))
            or self.high_nano < max(self.open_nano, self.low_nano, self.close_nano)
            or self.low_nano > min(self.open_nano, self.high_nano, self.close_nano)
            or not isinstance(self.session, str)
            or not isinstance(self.actual_identity_hash, str)
        ):
            raise IntegrityError("bracket bar is invalid")


@dataclass(frozen=True)
class BracketOutcome:
    direction: Direction
    status: str
    decision_at_ns: int
    entry_at_ns: int | None
    exit_at_ns: int | None
    label_unlock_at_ns: int
    entry_price_nano: int | None
    exit_price_nano: int | None
    stop_price_nano: int | None
    target_price_nano: int | None
    planned_all_in_risk_usd: Decimal | None
    realized_gross_pnl_usd: Decimal | None
    realized_net_r: Decimal | None
    exit_reason: str | None = None


@dataclass(frozen=True)
class BracketExecutionProtocol:
    """Fields every candidate and baseline must share for a valid comparison."""

    entry_delay_bars: int = 1
    maximum_standard_contract_equivalents: int = 1
    maximum_entries_per_session: int = 3
    maximum_hold_minutes: int = MAXIMUM_HOLD_MINUTES
    cost_scenario: str = "stress"
    roll_handling: str = "contract_boundary_not_return"


def require_baseline_execution_parity(
    *, candidate: BracketExecutionProtocol, baseline: BracketExecutionProtocol
) -> None:
    """Reject a baseline that receives easier fills or risk controls."""

    if candidate != baseline:
        raise IntegrityError("baseline execution protocol differs from the candidate")


def _round_tick(value: Decimal, tick_size_nano: int, rounding: str) -> int:
    if type(tick_size_nano) is not int or tick_size_nano <= 0:
        raise IntegrityError("tick size must be a positive integer nano price")
    ticks = (value / Decimal(tick_size_nano)).to_integral_value(rounding=rounding)
    return int(ticks) * tick_size_nano


def _validate_bars(bars: Sequence[BracketBar]) -> None:
    if len(bars) < ATR_PERIOD + 2:
        raise IntegrityError("bracket path requires enough bars for ATR and entry")
    for index, bar in enumerate(bars):
        bar.validate()
        if index and bars[index - 1].event_at_ns >= bar.event_at_ns:
            raise IntegrityError("bracket bars must be strictly ordered")


def wilder_atr_nano(*, bars: Sequence[BracketBar], decision_index: int) -> Decimal | None:
    """Return causal Wilder ATR(20), or None when the path is not contiguous."""

    _validate_bars(bars)
    if decision_index < ATR_PERIOD or decision_index >= len(bars):
        return None
    identity, session = bars[decision_index].actual_identity_hash, bars[decision_index].session
    start = decision_index
    while start > 0:
        prior, current = bars[start - 1], bars[start]
        if (
            not prior.eligible
            or not current.eligible
            or prior.actual_identity_hash != identity
            or current.actual_identity_hash != identity
            or prior.session != session
            or current.session != session
            or current.event_at_ns - prior.event_at_ns != NS_PER_MINUTE
        ):
            break
        start -= 1
    run = bars[start : decision_index + 1]
    if any(not bar.eligible or bar.actual_identity_hash != identity or bar.session != session for bar in run):
        return None
    if len(run) < ATR_PERIOD + 1:
        return None
    ranges: list[Decimal] = []
    previous_close = run[0].close_nano
    for bar in run[1:]:
        ranges.append(Decimal(max(bar.high_nano - bar.low_nano, abs(bar.high_nano - previous_close), abs(bar.low_nano - previous_close))))
        previous_close = bar.close_nano
    atr = sum(ranges[:ATR_PERIOD], Decimal("0")) / Decimal(ATR_PERIOD)
    for value in ranges[ATR_PERIOD:]:
        atr = (atr * Decimal(ATR_PERIOD - 1) + value) / Decimal(ATR_PERIOD)
    return atr


def build_directional_bracket_outcome(
    *,
    bars: Sequence[BracketBar],
    decision_index: int,
    direction: Direction,
    tick_size_nano: int,
    tick_value_usd: Decimal,
    stress_round_trip_cost_usd: Decimal,
) -> BracketOutcome:
    """Label one directional path with conservative OHLC-only fill assumptions."""

    _validate_bars(bars)
    if direction not in ("long", "short"):
        raise IntegrityError("bracket direction is invalid")
    tick_value = _decimal(tick_value_usd, name="tick_value_usd")
    costs = _decimal(stress_round_trip_cost_usd, name="stress_round_trip_cost_usd")
    if tick_value <= 0 or costs < 0:
        raise IntegrityError("bracket economics are invalid")
    decision = bars[decision_index]
    atr = wilder_atr_nano(bars=bars, decision_index=decision_index)
    unlock = decision.event_at_ns + MAXIMUM_HOLD_MINUTES * NS_PER_MINUTE
    if atr is None or decision_index + 1 >= len(bars):
        return BracketOutcome(direction, "ABSTAIN_INSUFFICIENT_CAUSAL_PATH", decision.event_at_ns, None, None, unlock, None, None, None, None, None, None, None)
    entry_bar = bars[decision_index + 1]
    if (
        not entry_bar.eligible
        or entry_bar.actual_identity_hash != decision.actual_identity_hash
        or entry_bar.session != decision.session
        or entry_bar.event_at_ns - decision.event_at_ns != NS_PER_MINUTE
    ):
        return BracketOutcome(direction, "ABSTAIN_MISSING_OR_ROLL_ENTRY", decision.event_at_ns, None, None, unlock, None, None, None, None, None, None, None)
    entry = entry_bar.open_nano
    stop_distance = atr * Decimal("1.5")
    if direction == "long":
        stop = _round_tick(Decimal(entry) - stop_distance, tick_size_nano, ROUND_FLOOR)
        gross_stop_loss = Decimal(entry - stop) / Decimal(tick_size_nano) * tick_value
    else:
        stop = _round_tick(Decimal(entry) + stop_distance, tick_size_nano, ROUND_CEILING)
        gross_stop_loss = Decimal(stop - entry) / Decimal(tick_size_nano) * tick_value
    planned_risk = gross_stop_loss + costs
    if stop <= 0 or planned_risk > MAXIMUM_INITIAL_RISK_USD:
        return BracketOutcome(direction, "ABSTAIN_INITIAL_RISK_CAP", decision.event_at_ns, entry_bar.event_at_ns, None, unlock, entry, None, stop, None, planned_risk, None, None)
    # Net target must equal 2R after the same all-in stress costs.
    gross_target_profit = Decimal("2") * planned_risk + costs
    target_ticks = (gross_target_profit / tick_value).to_integral_value(rounding=ROUND_CEILING)
    if direction == "long":
        target = entry + int(target_ticks) * tick_size_nano
    else:
        target = entry - int(target_ticks) * tick_size_nano

    maximum_exit_at = entry_bar.event_at_ns + MAXIMUM_HOLD_MINUTES * NS_PER_MINUTE
    prior = decision
    for index in range(decision_index + 1, len(bars)):
        bar = bars[index]
        if not bar.eligible:
            return BracketOutcome(direction, "ABSTAIN_MISSING_SOURCE", decision.event_at_ns, entry_bar.event_at_ns, None, unlock, entry, None, stop, target, planned_risk, None, None)
        if bar.actual_identity_hash != decision.actual_identity_hash:
            return _close(direction, "ROLL_BOUNDARY", decision, entry_bar, prior, unlock, entry, stop, target, planned_risk, tick_size_nano, tick_value, costs)
        if bar.session != decision.session:
            return _close(direction, "SESSION_END", decision, entry_bar, prior, unlock, entry, stop, target, planned_risk, tick_size_nano, tick_value, costs)
        if bar.event_at_ns - prior.event_at_ns != NS_PER_MINUTE:
            return BracketOutcome(direction, "ABSTAIN_MISSING_SOURCE", decision.event_at_ns, entry_bar.event_at_ns, None, unlock, entry, None, stop, target, planned_risk, None, None)
        if direction == "long":
            if bar.open_nano <= stop:
                return _close(direction, "STOP_GAP", decision, entry_bar, bar, unlock, entry, stop, target, planned_risk, tick_size_nano, tick_value, costs, price=bar.open_nano)
            if bar.open_nano >= target:
                return _close(direction, "TARGET", decision, entry_bar, bar, unlock, entry, stop, target, planned_risk, tick_size_nano, tick_value, costs, price=target)
            hit_stop, hit_target = bar.low_nano <= stop, bar.high_nano >= target
        else:
            if bar.open_nano >= stop:
                return _close(direction, "STOP_GAP", decision, entry_bar, bar, unlock, entry, stop, target, planned_risk, tick_size_nano, tick_value, costs, price=bar.open_nano)
            if bar.open_nano <= target:
                return _close(direction, "TARGET", decision, entry_bar, bar, unlock, entry, stop, target, planned_risk, tick_size_nano, tick_value, costs, price=target)
            hit_stop, hit_target = bar.high_nano >= stop, bar.low_nano <= target
        if hit_stop:  # Includes an unobservable same-bar collision: stop first.
            return _close(direction, "STOP" if not hit_target else "STOP_FIRST_COLLISION", decision, entry_bar, bar, unlock, entry, stop, target, planned_risk, tick_size_nano, tick_value, costs, price=stop)
        if hit_target:
            return _close(direction, "TARGET", decision, entry_bar, bar, unlock, entry, stop, target, planned_risk, tick_size_nano, tick_value, costs, price=target)
        if bar.event_at_ns + NS_PER_MINUTE >= maximum_exit_at:
            return _close(direction, "MAX_HOLD", decision, entry_bar, bar, unlock, entry, stop, target, planned_risk, tick_size_nano, tick_value, costs)
        prior = bar
    return BracketOutcome(direction, "ABSTAIN_MISSING_SOURCE", decision.event_at_ns, entry_bar.event_at_ns, None, unlock, entry, None, stop, target, planned_risk, None, None)


def _close(direction: Direction, reason: str, decision: BracketBar, entry_bar: BracketBar, exit_bar: BracketBar, unlock: int, entry: int, stop: int, target: int, planned_risk: Decimal, tick_size_nano: int, tick_value: Decimal, costs: Decimal, *, price: int | None = None) -> BracketOutcome:
    exit_price = exit_bar.close_nano if price is None else price
    ticks = Decimal((exit_price - entry) if direction == "long" else (entry - exit_price)) / Decimal(tick_size_nano)
    gross = ticks * tick_value
    return BracketOutcome(direction, "MATURED", decision.event_at_ns, entry_bar.event_at_ns, exit_bar.event_at_ns + NS_PER_MINUTE, unlock, entry, exit_price, stop, target, planned_risk, gross, (gross - costs) / planned_risk, reason)


def bracket_trial_metadata(*, protocol: BracketExecutionProtocol) -> dict[str, object]:
    """Return explicit non-authorizing metadata for a later registration."""

    return {
        "schema_version": "tier1_bracket_trial/1.0.0",
        "trial_status": "LOCAL_IMPLEMENTATION_ONLY_NOT_REGISTERED",
        "research_only": True,
        "live_readiness": False,
        "directions": ["long", "short"],
        "discovery_period": "2018-2022",
        "locked_untouched_holdout": "2025",
        "label_unlock_rule": "entry_at_plus_60_minutes_even_when_exit_occurs_earlier",
        "protocol": protocol.__dict__,
        "forbidden_claims": ["live_execution_realism", "apex_readiness", "exact_zn_fees", "margin_or_buying_power_validation"],
    }
