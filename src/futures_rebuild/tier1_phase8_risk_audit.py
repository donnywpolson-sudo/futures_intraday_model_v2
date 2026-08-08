"""Synthetic-only realism audit for the Tier 1 ATR-bracket policy.

This local tool intentionally has no release reader or provider dependency.  It
tests fixed adverse paths against the configured controls; it cannot validate
live fills, account rules, or profitability.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Mapping, Sequence

from .errors import IntegrityError
from .tier1_phase8_evaluation_config import load_tier1_phase8_evaluation_config


ZERO = Decimal("0")
MINUTE_NS = 60_000_000_000
DEFAULT_REPORT_PATH = Path("reports/audits/final/tier1_bracket_policy_risk_audit.json")
_EXIT_REASONS = {"stop", "target", "max_hold", "session_end", "roll_boundary"}
_SUPPORTED_CLAIMS = (
    "configured_base_stress_extreme_cost_math",
    "defined_path_risk_admission_and_circuit_breakers",
    "defined_missing_stale_roll_and_late_session_abstentions",
    "defined_one_position_and_three_entry_schedule",
)
_BLOCKED_LIVE_ASSUMPTIONS = (
    "actual_bid_ask_spread_and_queue_position",
    "partial_or_rejected_order_fills",
    "broker_stop_order_behavior",
    "exchange_halts_and_disconnects",
    "margin_and_buying_power",
    "current_apex_terms_and_exact_zn_fees",
)
_SYNTHETIC_TICK_VALUES = {
    "ES": Decimal("12.50"),
    "CL": Decimal("10"),
    "ZN": Decimal("15.625"),
    "6E": Decimal("6.25"),
}


def _finite(value: Decimal, *, name: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise IntegrityError(f"{name} must be a finite Decimal")
    return value


@dataclass(frozen=True)
class Phase8RiskAuditPath:
    """One candidate path before configured fees/slippage are applied."""

    scenario_id: str
    market: str
    session: int
    entry_at_ns: int
    exit_at_ns: int | None
    planned_initial_risk_usd: Decimal
    worst_open_gross_pnl_usd: Decimal
    realized_gross_pnl_usd: Decimal | None
    tick_value_usd: Decimal
    exit_reason: str | None
    extra_adverse_slippage_ticks: int = 0
    source_complete: bool = True
    source_fresh: bool = True
    minutes_to_session_roll: int = 120

    def validate(self, *, markets: set[str]) -> None:
        if (
            not isinstance(self.scenario_id, str)
            or not self.scenario_id
            or self.market not in markets
            or type(self.session) is not int
            or type(self.entry_at_ns) is not int
            or type(self.extra_adverse_slippage_ticks) is not int
            or not 0 <= self.extra_adverse_slippage_ticks <= 4
            or type(self.minutes_to_session_roll) is not int
        ):
            raise IntegrityError("risk-audit path is invalid")
        _finite(self.planned_initial_risk_usd, name="planned_initial_risk_usd")
        _finite(self.worst_open_gross_pnl_usd, name="worst_open_gross_pnl_usd")
        _finite(self.tick_value_usd, name="tick_value_usd")
        if self.tick_value_usd <= ZERO or self.planned_initial_risk_usd <= ZERO:
            raise IntegrityError("risk-audit path economics are invalid")
        if self.source_complete:
            if (
                type(self.exit_at_ns) is not int
                or self.exit_at_ns < self.entry_at_ns
                or not isinstance(self.realized_gross_pnl_usd, Decimal)
                or self.exit_reason not in _EXIT_REASONS
            ):
                raise IntegrityError("complete risk-audit path requires a recognized closed exit")
            _finite(self.realized_gross_pnl_usd, name="realized_gross_pnl_usd")
            if self.worst_open_gross_pnl_usd > self.realized_gross_pnl_usd:
                raise IntegrityError("worst open P&L cannot be better than realized P&L")
        elif any(value is not None for value in (self.exit_at_ns, self.realized_gross_pnl_usd, self.exit_reason)):
            raise IntegrityError("incomplete source path must abstain without an invented fill")


@dataclass(frozen=True)
class Tier1RiskAuditPathResult:
    scenario_id: str
    disposition: str
    configured_cost_usd: Decimal
    realized_net_pnl_usd: Decimal | None
    forced_flatten: bool

    def report(self) -> dict[str, object]:
        return {
            "scenario_id": self.scenario_id,
            "disposition": self.disposition,
            "configured_cost_usd": str(self.configured_cost_usd),
            "realized_net_pnl_usd": None if self.realized_net_pnl_usd is None else str(self.realized_net_pnl_usd),
            "forced_flatten": self.forced_flatten,
        }


@dataclass(frozen=True)
class Tier1RiskAuditScenarioResult:
    cost_scenario: str
    accepted_trade_count: int
    rejected_trade_count: int
    skipped_trade_count: int
    forced_flatten_count: int
    exit_counts: Mapping[str, int]
    hold_time_buckets: Mapping[str, int]
    rejected_by_market: Mapping[str, int]
    worst_realized_loss_usd: Decimal
    daily_stop_sessions: tuple[int, ...]
    drawdown_stop_triggered: bool
    policy_bypasses: tuple[str, ...]
    path_results: tuple[Tier1RiskAuditPathResult, ...]

    def report(self) -> dict[str, object]:
        return {
            "accepted_trade_count": self.accepted_trade_count,
            "rejected_trade_count": self.rejected_trade_count,
            "skipped_trade_count": self.skipped_trade_count,
            "forced_flatten_count": self.forced_flatten_count,
            "exit_counts": dict(self.exit_counts),
            "hold_time_buckets": dict(self.hold_time_buckets),
            "rejected_by_market": dict(self.rejected_by_market),
            "worst_realized_loss_usd": str(self.worst_realized_loss_usd),
            "daily_stop_sessions": list(self.daily_stop_sessions),
            "drawdown_stop_triggered": self.drawdown_stop_triggered,
            "policy_bypasses": list(self.policy_bypasses),
            "policy_controls_pass": not self.policy_bypasses,
            "path_results": [item.report() for item in self.path_results],
        }


@dataclass(frozen=True)
class Tier1RiskAuditResult:
    scenarios: Mapping[str, Tier1RiskAuditScenarioResult]
    supported_simulation_claims: tuple[str, ...]
    blocked_live_assumptions: tuple[str, ...]
    policy_bypasses: tuple[str, ...]

    def report(self) -> dict[str, object]:
        return {
            "schema_version": "tier1_phase8_synthetic_risk_audit/2.0.0",
            "kind": "LOCAL_SYNTHETIC_ONLY_NOT_LIVE_VALIDATION",
            "supported_simulation_claims": list(self.supported_simulation_claims),
            "blocked_live_assumptions": list(self.blocked_live_assumptions),
            "policy_bypasses": list(self.policy_bypasses),
            "policy_controls_pass": not self.policy_bypasses,
            "live_realism_claim_supported": False,
            "live_ready": False,
            "scenarios": {name: result.report() for name, result in self.scenarios.items()},
        }


def _cost(evaluation_config: Mapping[str, object], *, scenario: str, market: str, extra_ticks: int) -> Decimal:
    costs = evaluation_config.get("costs")
    if not isinstance(costs, dict) or not isinstance(costs.get("base"), dict):
        raise IntegrityError("risk audit cost configuration is invalid")
    entry = costs["base"].get(market)
    if not isinstance(entry, dict) or not isinstance(entry.get("all_in_fee_per_side_usd"), str) or not isinstance(entry.get("round_trip_slippage_ticks_per_contract"), int):
        raise IntegrityError("risk audit market costs are invalid")
    fee = Decimal(entry["all_in_fee_per_side_usd"])
    slippage = entry["round_trip_slippage_ticks_per_contract"]
    if scenario != "base":
        sensitivity = costs.get(scenario)
        if not isinstance(sensitivity, dict) or not isinstance(sensitivity.get("fee_multiplier"), str) or not isinstance(sensitivity.get("slippage_multiplier"), int) or not isinstance(sensitivity.get("minimum_round_trip_slippage_ticks"), int):
            raise IntegrityError("risk audit sensitivity costs are invalid")
        fee *= Decimal(sensitivity["fee_multiplier"])
        slippage = max(slippage * sensitivity["slippage_multiplier"], sensitivity["minimum_round_trip_slippage_ticks"])
    return fee * Decimal(2) + Decimal(slippage + extra_ticks) * _SYNTHETIC_TICK_VALUES[market]


def _hold_bucket(path: Phase8RiskAuditPath) -> str:
    if path.exit_reason in {"session_end", "roll_boundary"}:
        return "safety_exit"
    assert path.exit_at_ns is not None
    held = (path.exit_at_ns - path.entry_at_ns) // MINUTE_NS
    if held <= 5:
        return "0_5_minutes"
    if held <= 30:
        return "6_30_minutes"
    return "31_60_minutes"


def _audit_scenario(*, paths: Sequence[Phase8RiskAuditPath], evaluation_config: Mapping[str, object], cost_scenario: str) -> Tier1RiskAuditScenarioResult:
    sizing = evaluation_config.get("position_sizing")
    limits = evaluation_config.get("concentration_limits")
    bracket = evaluation_config.get("bracket_exit_policy")
    markets = evaluation_config.get("markets")
    if not all(isinstance(value, dict) for value in (sizing, limits, bracket)) or not isinstance(markets, list):
        raise IntegrityError("risk audit requires a complete Tier 1 configuration")
    max_risk = sizing.get("risk_per_new_position_usd")
    max_entries = sizing.get("maximum_entries_per_session")
    daily_stop = limits.get("daily_stop_loss_usd")
    total_stop = limits.get("maximum_total_drawdown_usd")
    max_hold = bracket.get("maximum_hold_minutes")
    late_cutoff = bracket.get("entry_blackout_minutes_before_session_roll")
    if not all(type(value) is int and value > 0 for value in (max_risk, max_entries, daily_stop, total_stop, max_hold, late_cutoff)):
        raise IntegrityError("risk audit numeric configuration is invalid")

    accepted = rejected = skipped = flattened = 0
    exit_counts: dict[str, int] = defaultdict(int)
    buckets: dict[str, int] = defaultdict(int)
    rejected_by_market: dict[str, int] = defaultdict(int)
    bypasses: list[str] = []
    daily_pnl: dict[int, Decimal] = defaultdict(lambda: ZERO)
    entries: dict[int, int] = defaultdict(int)
    daily_locked: set[int] = set()
    open_until = 0
    equity = peak = ZERO
    total_locked = False
    worst_loss = ZERO
    path_results: list[Tier1RiskAuditPathResult] = []

    for path in sorted(paths, key=lambda item: (item.entry_at_ns, item.market, item.scenario_id)):
        path.validate(markets=set(markets))
        costs = _cost(evaluation_config, scenario=cost_scenario, market=path.market, extra_ticks=path.extra_adverse_slippage_ticks)
        reject = (
            path.planned_initial_risk_usd > Decimal(max_risk)
            or not path.source_complete
            or not path.source_fresh
            or path.minutes_to_session_roll < late_cutoff
        )
        if reject:
            rejected += 1
            rejected_by_market[path.market] += 1
            path_results.append(Tier1RiskAuditPathResult(path.scenario_id, "REJECTED_ADMISSION_OR_SOURCE", costs, None, False))
            continue
        if path.entry_at_ns < open_until or entries[path.session] >= max_entries or path.session in daily_locked or total_locked:
            skipped += 1
            path_results.append(Tier1RiskAuditPathResult(path.scenario_id, "SKIPPED_CIRCUIT_OR_SCHEDULE", costs, None, False))
            continue
        assert path.exit_at_ns is not None and path.realized_gross_pnl_usd is not None and path.exit_reason is not None
        held = (path.exit_at_ns - path.entry_at_ns) // MINUTE_NS
        if held > max_hold and path.exit_reason not in {"session_end", "roll_boundary"}:
            bypasses.append(f"{path.scenario_id}:unbounded_hold")
            path_results.append(Tier1RiskAuditPathResult(path.scenario_id, "POLICY_BYPASS", costs, None, False))
            continue

        net = path.realized_gross_pnl_usd - costs
        worst_open = path.worst_open_gross_pnl_usd - costs
        prior_equity, prior_daily = equity, daily_pnl[path.session]
        forced = prior_daily + worst_open <= -Decimal(daily_stop) or peak - (prior_equity + min(worst_open, ZERO)) >= Decimal(total_stop)
        if forced:
            flattened += 1
            net = min(net, worst_open)  # Forced flatten at the supplied adverse prevailing-price outcome.
            if prior_daily + worst_open <= -Decimal(daily_stop):
                daily_locked.add(path.session)
            if peak - (prior_equity + min(worst_open, ZERO)) >= Decimal(total_stop):
                total_locked = True

        accepted += 1
        entries[path.session] += 1
        open_until = path.exit_at_ns
        daily_pnl[path.session] += net
        equity += net
        peak = max(peak, equity)
        worst_loss = min(worst_loss, net)
        exit_counts[path.exit_reason] += 1
        buckets[_hold_bucket(path)] += 1
        path_results.append(Tier1RiskAuditPathResult(path.scenario_id, "FORCED_FLATTEN" if forced else "ACCEPTED", costs, net, forced))

    return Tier1RiskAuditScenarioResult(
        cost_scenario=cost_scenario,
        accepted_trade_count=accepted,
        rejected_trade_count=rejected,
        skipped_trade_count=skipped,
        forced_flatten_count=flattened,
        exit_counts=dict(sorted(exit_counts.items())),
        hold_time_buckets=dict(sorted(buckets.items())),
        rejected_by_market=dict(sorted(rejected_by_market.items())),
        worst_realized_loss_usd=worst_loss,
        daily_stop_sessions=tuple(sorted(daily_locked)),
        drawdown_stop_triggered=total_locked,
        policy_bypasses=tuple(bypasses),
        path_results=tuple(path_results),
    )


def audit_tier1_phase8_risk_synthetic(*, paths: Sequence[Phase8RiskAuditPath], evaluation_config: Mapping[str, object]) -> Tier1RiskAuditResult:
    """Run every configured cost scenario without opening market data."""

    if not paths:
        raise IntegrityError("risk audit requires at least one synthetic path")
    if any(type(path) is not Phase8RiskAuditPath for path in paths):
        raise IntegrityError("risk audit requires intratrade path inputs, not pre-closed trades")
    scenarios = {
        name: _audit_scenario(paths=paths, evaluation_config=evaluation_config, cost_scenario=name)
        for name in ("base", "stress", "extreme")
    }
    bypasses = tuple(sorted({item for result in scenarios.values() for item in result.policy_bypasses}))
    return Tier1RiskAuditResult(scenarios, _SUPPORTED_CLAIMS, _BLOCKED_LIVE_ASSUMPTIONS, bypasses)


def default_tier1_risk_realism_paths() -> tuple[Phase8RiskAuditPath, ...]:
    """Deterministic synthetic paths; not historical performance observations."""

    m = MINUTE_NS
    paths = [
        Phase8RiskAuditPath("normal_target", "ES", 1, 0, 30*m, Decimal("250"), Decimal("-80"), Decimal("520"), _SYNTHETIC_TICK_VALUES["ES"], "target", 1),
        Phase8RiskAuditPath("stop_collision", "CL", 2, 31*m, 36*m, Decimal("250"), Decimal("-245"), Decimal("-235"), _SYNTHETIC_TICK_VALUES["CL"], "stop", 2),
        Phase8RiskAuditPath("gap_stop", "ZN", 3, 37*m, 38*m, Decimal("250"), Decimal("-300"), Decimal("-280"), _SYNTHETIC_TICK_VALUES["ZN"], "stop", 4),
        Phase8RiskAuditPath("two_loss_one", "6E", 4, 39*m, 44*m, Decimal("250"), Decimal("-255"), Decimal("-240"), _SYNTHETIC_TICK_VALUES["6E"], "stop", 1),
        Phase8RiskAuditPath("two_loss_two", "6E", 4, 45*m, 50*m, Decimal("250"), Decimal("-260"), Decimal("-245"), _SYNTHETIC_TICK_VALUES["6E"], "stop", 2),
        Phase8RiskAuditPath("third_loss_skipped", "6E", 4, 51*m, 56*m, Decimal("250"), Decimal("-260"), Decimal("-245"), _SYNTHETIC_TICK_VALUES["6E"], "stop", 3),
        Phase8RiskAuditPath("max_hold", "ES", 5, 57*m, 117*m, Decimal("250"), Decimal("-60"), Decimal("35"), _SYNTHETIC_TICK_VALUES["ES"], "max_hold", 1),
        Phase8RiskAuditPath("over_risk", "CL", 6, 118*m, 119*m, Decimal("251"), Decimal("-5"), Decimal("10"), _SYNTHETIC_TICK_VALUES["CL"], "target"),
        Phase8RiskAuditPath("missing", "ZN", 7, 120*m, None, Decimal("250"), Decimal("-20"), None, _SYNTHETIC_TICK_VALUES["ZN"], None, source_complete=False),
        Phase8RiskAuditPath("stale", "ES", 8, 121*m, 122*m, Decimal("250"), Decimal("-5"), Decimal("10"), _SYNTHETIC_TICK_VALUES["ES"], "target", source_fresh=False),
        Phase8RiskAuditPath("late", "CL", 9, 123*m, 124*m, Decimal("250"), Decimal("-5"), Decimal("10"), _SYNTHETIC_TICK_VALUES["CL"], "target", minutes_to_session_roll=59),
        Phase8RiskAuditPath("roll", "6E", 10, 125*m, 126*m, Decimal("250"), Decimal("-10"), Decimal("20"), _SYNTHETIC_TICK_VALUES["6E"], "roll_boundary"),
        Phase8RiskAuditPath("session_end", "ES", 11, 127*m, 128*m, Decimal("250"), Decimal("-10"), Decimal("20"), _SYNTHETIC_TICK_VALUES["ES"], "session_end"),
    ]
    for ordinal, market in enumerate(("ES", "CL", "ZN", "6E")):
        for extra_ticks in range(1, 5):
            start = (130 + (ordinal * 4 + extra_ticks) * 2) * m
            paths.append(Phase8RiskAuditPath(f"slippage_{market}_{extra_ticks}_ticks", market, 30 + ordinal * 4 + extra_ticks, start, start + m, Decimal("250"), Decimal("-15"), Decimal("40"), _SYNTHETIC_TICK_VALUES[market], "target", extra_ticks))
    # Same-session fourth entry is rejected; overlapping decisions are rejected too.
    for ordinal in range(4):
        start = (170 + ordinal * 2) * m
        paths.append(Phase8RiskAuditPath(f"entry_cap_{ordinal + 1}", "ES", 50, start, start + m, Decimal("250"), Decimal("-10"), Decimal("20"), _SYNTHETIC_TICK_VALUES["ES"], "target", 1))
    paths.append(Phase8RiskAuditPath("overlap", "CL", 60, 171*m, 173*m, Decimal("250"), Decimal("-10"), Decimal("20"), _SYNTHETIC_TICK_VALUES["CL"], "target", 1))
    # Six adverse sessions exercise the $1,500 internal drawdown circuit breaker.
    for index in range(6):
        paths.append(Phase8RiskAuditPath(f"drawdown_{index + 1}", "ZN", 70 + index, (300 + index * 2)*m, (301 + index * 2)*m, Decimal("250"), Decimal("-280"), Decimal("-250"), _SYNTHETIC_TICK_VALUES["ZN"], "stop", 4))
    return tuple(paths)


def run_default_tier1_risk_realism_audit(*, evaluation_config: Mapping[str, object]) -> Tier1RiskAuditResult:
    return audit_tier1_phase8_risk_synthetic(paths=default_tier1_risk_realism_paths(), evaluation_config=evaluation_config)


def write_local_risk_audit_report(*, root: Path, output: Path = DEFAULT_REPORT_PATH) -> Path:
    """Atomically write the deterministic local report; never create a release."""

    config, _ = load_tier1_phase8_evaluation_config(root=root)
    report = run_default_tier1_risk_realism_audit(evaluation_config=config).report()
    target = output if output.is_absolute() else root / output
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(target)
    return target


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write the local synthetic Tier 1 bracket-risk audit.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT_PATH)
    args = parser.parse_args(argv)
    print(write_local_risk_audit_report(root=args.root.resolve(), output=args.output).resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
