from pathlib import Path

from futures_rebuild.fixed_hour_ridge_discovery import build_contract
from futures_rebuild.fixed_hour_ridge_governed import nested_select


ROOT = Path(__file__).resolve().parents[1]


def test_nested_selection_is_training_only_and_deterministic() -> None:
    protocol = build_contract(ROOT)
    rows = {}
    for index in range(504):
        value = (index % 17 - 8) / 1000
        rows[f"2020-{index:04d}"] = {
            "features": {
                "log_return_1": value, "log_return_5": value * 2,
                "log_return_10": value * 3, "intrabar_range_fraction": abs(value),
                "atr_10_fraction": abs(value) + 0.01,
                "realized_volatility_10": abs(value) / 2,
                "log1p_volume": 5 + value, "volume_zscore_20": value * 4,
            },
            "targets": {"long": value * 10, "short": -value * 10},
            "net_pnl_usd": {"stress": {"long": value * 100, "short": -value * 100}},
        }
    ids = tuple(rows)
    first, scores = nested_select(protocol, rows, ids)
    second, repeated = nested_select(protocol, rows, ids)
    assert first == second
    assert scores == repeated
    assert len(scores) == 9 and all(len(value) == 4 for value in scores.values())
