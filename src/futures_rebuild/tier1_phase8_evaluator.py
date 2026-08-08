"""Pure, non-publishing Tier 1 Phase 8 net-economics evaluator."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from math import sqrt
from typing import Mapping, Sequence

from .errors import IntegrityError


ZERO = Decimal("0")
IDENTICAL_FIXED_RISK_COMPARATOR = "equal_risk_version_of_candidate_signal"


def _decimal(value: object, *, name: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise IntegrityError(f"{name} must be a finite Decimal")
    return value


@dataclass(frozen=True)
class Phase8SyntheticTrade:
    """One closed synthetic trade; no price, provider, or release input exists here."""

    market: str
    market_year: int
    session: int
    signed_quantity: int
    risk_at_entry_usd: Decimal
    gross_pnl_usd: Decimal
    tick_value_usd: Decimal
    baseline_gross_pnl_usd: Mapping[str, Decimal]
    entry_at_ns: int = 0
    exit_at_ns: int = 0

    def validate(self, *, markets: set[str], required_baselines: set[str]) -> None:
        if self.market not in markets or not isinstance(self.market_year, int) or not isinstance(self.session, int):
            raise IntegrityError("synthetic trade is outside the Tier 1 scope")
        if not isinstance(self.signed_quantity, int) or self.signed_quantity == 0:
            raise IntegrityError("synthetic trade quantity must be nonzero")
        if type(self.entry_at_ns) is not int or type(self.exit_at_ns) is not int or self.exit_at_ns < self.entry_at_ns:
            raise IntegrityError("synthetic trade timing is invalid")
        if _decimal(self.risk_at_entry_usd, name="risk_at_entry_usd") <= ZERO:
            raise IntegrityError("synthetic trade risk must be positive")
        if _decimal(self.tick_value_usd, name="tick_value_usd") <= ZERO:
            raise IntegrityError("synthetic trade tick value must be positive")
        _decimal(self.gross_pnl_usd, name="gross_pnl_usd")
        missing = required_baselines - {"flat_no_trade"} - set(self.baseline_gross_pnl_usd)
        if missing:
            raise IntegrityError("synthetic trade omits required baseline outcomes")
        for value in self.baseline_gross_pnl_usd.values():
            _decimal(value, name="baseline_gross_pnl_usd")


@dataclass(frozen=True)
class Phase8Metrics:
    net_pnl_usd: Decimal
    sharpe: float
    sortino: float
    maximum_drawdown_usd: Decimal
    turnover_contract_equivalents: int
    hit_rate: float
    gross_exposure_contract_equivalents: int
    net_directional_contract_equivalents: int
    observation_count: int


@dataclass(frozen=True)
class Tier1Phase8Evaluation:
    result_label: str
    exact_apex_live_costs_verified: bool
    aggregate: Phase8Metrics
    by_market_year: Mapping[str, Phase8Metrics]
    baseline_net_pnl_usd: Mapping[str, Decimal]
    beats_required_baselines: bool
    market_year_coverage_complete: bool
    skipped_trade_count: int
    scenarios: Mapping[str, "Tier1Phase8ScenarioEvaluation"]


@dataclass(frozen=True)
class Tier1Phase8ScenarioEvaluation:
    """One immutable-cost sensitivity view of the same closed trade set."""

    cost_scenario: str
    aggregate: Phase8Metrics
    by_market_year: Mapping[str, Phase8Metrics]
    baseline_net_pnl_usd: Mapping[str, Decimal]
    beats_required_baselines: bool
    market_year_coverage_complete: bool
    skipped_trade_count: int
    identical_fixed_risk_comparator_matches: bool


def _metrics(net_pnls: Sequence[Decimal], quantities: Sequence[int]) -> Phase8Metrics:
    if not net_pnls or len(net_pnls) != len(quantities):
        raise IntegrityError("metrics require aligned nonempty outcomes")
    values = [float(value) for value in net_pnls]
    mean = sum(values) / len(values)
    deviation = sqrt(sum((value - mean) ** 2 for value in values) / len(values))
    downside = [min(value, 0.0) for value in values]
    downside_deviation = sqrt(sum(value**2 for value in downside) / len(values))
    equity = ZERO
    peak = ZERO
    drawdown = ZERO
    for pnl in net_pnls:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return Phase8Metrics(
        net_pnl_usd=sum(net_pnls, ZERO),
        sharpe=0.0 if deviation == 0.0 else mean / deviation,
        sortino=0.0 if downside_deviation == 0.0 else mean / downside_deviation,
        maximum_drawdown_usd=drawdown,
        turnover_contract_equivalents=sum(abs(value) for value in quantities),
        hit_rate=sum(value > ZERO for value in net_pnls) / len(net_pnls),
        gross_exposure_contract_equivalents=sum(abs(value) for value in quantities),
        net_directional_contract_equivalents=sum(quantities),
        observation_count=len(net_pnls),
    )


def _scenario_costs(
    costs: Mapping[str, object], scenario: str, market: str
) -> tuple[Decimal, int]:
    base = costs.get("base")
    if not isinstance(base, dict) or not isinstance(base.get(market), dict):
        raise IntegrityError("synthetic trade lacks a configured market cost")
    entry = base[market]
    fee = entry.get("all_in_fee_per_side_usd")
    slippage = entry.get("round_trip_slippage_ticks_per_contract")
    if not isinstance(fee, str) or not isinstance(slippage, int):
        raise IntegrityError("synthetic trade has an incomplete cost schedule")
    if scenario == "base":
        return Decimal(fee), slippage
    sensitivity = costs.get(scenario)
    if not isinstance(sensitivity, dict):
        raise IntegrityError("Tier 1 cost sensitivity is invalid")
    multiplier = sensitivity.get("fee_multiplier")
    slippage_multiplier = sensitivity.get("slippage_multiplier")
    minimum = sensitivity.get("minimum_round_trip_slippage_ticks")
    if not isinstance(multiplier, str) or not isinstance(slippage_multiplier, int) or not isinstance(minimum, int):
        raise IntegrityError("Tier 1 cost sensitivity is incomplete")
    return Decimal(fee) * Decimal(multiplier), max(slippage * slippage_multiplier, minimum)


def _evaluate_scenario(
    *,
    trades: Sequence[Phase8SyntheticTrade],
    evaluation_config: Mapping[str, object],
    scenario: str,
    expected_market_years: set[str] | None,
) -> Tier1Phase8ScenarioEvaluation:
    """Apply one locked cost scenario to pre-normalized closed trades."""

    costs = evaluation_config.get("costs")
    sizing = evaluation_config.get("position_sizing")
    limits = evaluation_config.get("concentration_limits")
    baselines = evaluation_config.get("baselines")
    metrics = evaluation_config.get("pass_fail_metrics")
    markets = evaluation_config.get("markets")
    if not all(isinstance(value, dict) for value in (costs, sizing, limits, metrics)) or not isinstance(baselines, list) or not isinstance(markets, list):
        raise IntegrityError("Tier 1 evaluation configuration is incomplete")
    if costs.get("evaluation_result_label") != "PROVISIONAL_EXECUTION_COSTS" or costs.get("exact_apex_live_costs_verified") is not False:
        raise IntegrityError("synthetic evaluation requires the provisional-cost boundary")
    required_baselines = set(baselines)
    market_set = set(markets)
    expected_scopes = expected_market_years or {f"{market}/{year}" for market in market_set for year in range(2018, 2023)}
    if not expected_scopes or any(scope.split("/", 1)[0] not in market_set for scope in expected_scopes):
        raise IntegrityError("Tier 1 expected market-year scope is invalid")
    max_quantity = sizing.get("maximum_standard_contract_equivalents")
    risk_per_trade = sizing.get("risk_per_new_position_usd")
    daily_stop = limits.get("daily_stop_loss_usd")
    total_drawdown_stop = limits.get("maximum_total_drawdown_usd")
    if not all(isinstance(value, int) for value in (max_quantity, risk_per_trade, daily_stop, total_drawdown_stop)):
        raise IntegrityError("Tier 1 sizing and loss limits must be integers")

    candidate_by_scope: dict[str, list[Decimal]] = defaultdict(list)
    quantity_by_scope: dict[str, list[int]] = defaultdict(list)
    baseline_totals: dict[str, Decimal] = {name: ZERO for name in required_baselines}
    daily_pnl: dict[int, Decimal] = defaultdict(lambda: ZERO)
    equity = ZERO
    peak = ZERO
    skipped = 0

    for trade in sorted(trades, key=lambda item: (item.entry_at_ns, item.market, item.market_year)):
        trade.validate(markets=market_set, required_baselines=required_baselines)
        if trade.baseline_gross_pnl_usd.get(IDENTICAL_FIXED_RISK_COMPARATOR) != trade.gross_pnl_usd:
            raise IntegrityError("identical fixed-risk comparator must match candidate gross P&L exactly")
        if abs(trade.signed_quantity) > max_quantity or trade.risk_at_entry_usd > Decimal(risk_per_trade):
            raise IntegrityError("synthetic trade exceeds the locked Apex position or risk limit")
        if daily_pnl[trade.session] <= -Decimal(daily_stop) or peak - equity >= Decimal(total_drawdown_stop):
            skipped += 1
            continue
        fee, slippage_ticks = _scenario_costs(costs, scenario, trade.market)
        quantity = abs(trade.signed_quantity)
        total_cost = (Decimal(fee) * Decimal(2) + Decimal(slippage_ticks) * trade.tick_value_usd) * quantity
        net = trade.gross_pnl_usd - total_cost
        scope = f"{trade.market}/{trade.market_year}"
        candidate_by_scope[scope].append(net)
        quantity_by_scope[scope].append(trade.signed_quantity)
        daily_pnl[trade.session] += net
        equity += net
        peak = max(peak, equity)
        for baseline in required_baselines:
            if baseline == "flat_no_trade":
                continue
            baseline_totals[baseline] += trade.baseline_gross_pnl_usd[baseline] - total_cost

    aggregate_net = [value for scoped in candidate_by_scope.values() for value in scoped]
    aggregate_quantity = [value for scoped in quantity_by_scope.values() for value in scoped]
    aggregate = _metrics(aggregate_net, aggregate_quantity)
    by_market_year = {
        scope: _metrics(candidate_by_scope[scope], quantity_by_scope[scope])
        for scope in sorted(candidate_by_scope)
    }
    coverage_complete = set(by_market_year) == expected_scopes
    required_to_beat = metrics.get("must_beat_after_costs")
    equivalence_checks = metrics.get("equivalence_checks")
    if (
        not isinstance(required_to_beat, list)
        or not set(required_to_beat).issubset(baseline_totals)
        or equivalence_checks != [IDENTICAL_FIXED_RISK_COMPARATOR]
    ):
        raise IntegrityError("Tier 1 baseline comparison is incomplete")
    return Tier1Phase8ScenarioEvaluation(
        cost_scenario=scenario,
        aggregate=aggregate,
        by_market_year=by_market_year,
        baseline_net_pnl_usd=baseline_totals,
        beats_required_baselines=coverage_complete and all(aggregate.net_pnl_usd > baseline_totals[name] for name in required_to_beat),
        market_year_coverage_complete=coverage_complete,
        skipped_trade_count=skipped,
        identical_fixed_risk_comparator_matches=(
            baseline_totals[IDENTICAL_FIXED_RISK_COMPARATOR] == aggregate.net_pnl_usd
        ),
    )


def evaluate_tier1_phase8_synthetic(
    *, trades: Sequence[Phase8SyntheticTrade], evaluation_config: Mapping[str, object],
    expected_market_years: set[str] | None = None,
) -> Tier1Phase8Evaluation:
    """Apply base, stress, and extreme locked costs; never read or publish data."""

    scenarios = {
        name: _evaluate_scenario(
            trades=trades,
            evaluation_config=evaluation_config,
            scenario=name,
            expected_market_years=expected_market_years,
        )
        for name in ("base", "stress", "extreme")
    }
    base = scenarios["base"]
    return Tier1Phase8Evaluation(
        result_label="PROVISIONAL_EXECUTION_COSTS",
        exact_apex_live_costs_verified=False,
        aggregate=base.aggregate,
        by_market_year=base.by_market_year,
        baseline_net_pnl_usd=base.baseline_net_pnl_usd,
        beats_required_baselines=base.beats_required_baselines,
        market_year_coverage_complete=base.market_year_coverage_complete,
        skipped_trade_count=base.skipped_trade_count,
        scenarios=scenarios,
    )
