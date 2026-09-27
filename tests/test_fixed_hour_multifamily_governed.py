import pytest

from futures_rebuild.fixed_hour_multifamily_governed import strategy_metrics


pytestmark = pytest.mark.current


def test_synthetic_pilot_metrics_charge_cost_once_and_count_zeros() -> None:
    metrics = strategy_metrics([0.0, 40.0, -20.0, 0.0], [0, 1, -1, 0])

    assert metrics["net_pnl_usd"] == 20.0
    assert metrics["trade_count"] == 2
    assert metrics["winning_trade_fraction"] == 0.5
    assert metrics["mean_session_pnl_usd"] == 5.0
    assert metrics["session_standard_deviation_usd"] == pytest.approx(21.7944947177)
    assert metrics["session_sharpe_252_including_abstention_zeros"] == pytest.approx(
        5.0 * 252 ** 0.5 / 21.7944947177
    )
    assert metrics["maximum_drawdown_usd"] == 20.0
