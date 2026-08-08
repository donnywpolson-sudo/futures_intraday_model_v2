from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_phase8_evaluation_config import load_tier1_phase8_evaluation_config
from futures_rebuild.tier1_phase8_evaluator import (
    Phase8SyntheticTrade,
    evaluate_tier1_phase8_synthetic,
)


ROOT = Path(__file__).parents[1]


def _trade(market: str, year: int, session: int, gross: str) -> Phase8SyntheticTrade:
    baselines = {
        "fold_local_unconditional_return_by_market_session": Decimal("1"),
        "previous_bar_sign_momentum": Decimal("2"),
        "previous_bar_sign_reversal": Decimal("3"),
        "risk_matched_always_long_intraday": Decimal("4"),
        "equal_risk_version_of_candidate_signal": Decimal(gross),
    }
    return Phase8SyntheticTrade(market, year, session, 1, Decimal("125"), Decimal(gross), Decimal("1"), baselines)


def test_evaluator_applies_costs_metrics_and_provisional_label() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    result = evaluate_tier1_phase8_synthetic(
        trades=tuple(_trade(market, year, offset, "100") for offset, (market, year) in enumerate(((market, year) for market in ("ES", "CL", "ZN", "6E") for year in range(2018, 2023)), start=1)),
        evaluation_config=config,
    )

    assert result.result_label == "PROVISIONAL_EXECUTION_COSTS"
    assert result.exact_apex_live_costs_verified is False
    assert len(result.by_market_year) == 20
    assert result.market_year_coverage_complete
    assert result.aggregate.net_pnl_usd < Decimal("2000")
    assert result.aggregate.turnover_contract_equivalents == 20
    assert result.baseline_net_pnl_usd["flat_no_trade"] == Decimal("0")
    assert result.scenarios["base"].identical_fixed_risk_comparator_matches
    assert tuple(result.scenarios) == ("base", "stress", "extreme")
    assert result.scenarios["stress"].aggregate.net_pnl_usd < result.scenarios["base"].aggregate.net_pnl_usd
    assert result.scenarios["extreme"].aggregate.net_pnl_usd < result.scenarios["stress"].aggregate.net_pnl_usd


def test_evaluator_stops_after_daily_loss_limit() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    result = evaluate_tier1_phase8_synthetic(
        trades=(_trade("ES", 2018, 1, "-600"), _trade("ES", 2018, 1, "100")),
        evaluation_config=config,
    )

    assert result.skipped_trade_count == 1
    assert result.aggregate.turnover_contract_equivalents == 1
    assert not result.market_year_coverage_complete
    assert not result.beats_required_baselines


def test_evaluator_rejects_an_exact_live_cost_label() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    invalid = deepcopy(config)
    invalid["costs"]["evaluation_result_label"] = "EXACT_APEX_LIVE_COSTS"

    with pytest.raises(IntegrityError, match="provisional-cost boundary"):
        evaluate_tier1_phase8_synthetic(trades=(_trade("ES", 2018, 1, "100"),), evaluation_config=invalid)


def test_evaluator_rejects_a_nonidentical_fixed_risk_comparator() -> None:
    config, _ = load_tier1_phase8_evaluation_config(root=ROOT)
    trade = _trade("ES", 2018, 1, "100")
    invalid = Phase8SyntheticTrade(
        **{**trade.__dict__, "baseline_gross_pnl_usd": {**trade.baseline_gross_pnl_usd, "equal_risk_version_of_candidate_signal": Decimal("99")}}
    )

    with pytest.raises(IntegrityError, match="must match candidate gross"):
        evaluate_tier1_phase8_synthetic(trades=(invalid,), evaluation_config=config)
