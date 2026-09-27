from pathlib import Path
from datetime import date, timedelta
from time import monotonic

import numpy as np
import pytest

from futures_rebuild import assumed_ohlcv_screen as screen
from futures_rebuild.errors import UnauthorizedOperation

pytestmark = pytest.mark.current


def test_observed_price_runs_and_assumption_scope():
    n = 200
    minute = 60_000_000_000
    t = 1_600_000_000_000_000_000 + np.arange(n, dtype=np.int64) * minute
    price = 1_000_000_000_000 + np.arange(n, dtype=np.int64) * 250_000_000
    raw = {"bar_start_ns": t, "bar_end_ns": t + minute,
           "project_trade_date": np.full(n, "2020-09-13"),
           "actual_contract": np.full(n, "ESZ0"),
           "min_price_increment_nano": np.full(n, 250_000_000),
           "multiplier_nano": np.full(n, 50_000_000_000),
           "open_nano": price, "close_nano": price,
           "high_nano": price + 250_000_000, "low_nano": price - 250_000_000,
           "volume": np.arange(n, dtype=np.uint64) + 100,
           "decision_eligible_at_ns": t + minute + 5_000_000_000,
           "definition_received_at_ns": t - minute}
    samples = screen._samples(raw, "ES")
    assert samples["sample_count"] == 78
    assert samples["x"].shape == (78, len(screen.FEATURES))
    assert np.all(samples["y"][:, 0] == 15)
    assert np.all(samples["y"][:, 2] == 60)
    assert np.all(samples["tick_value"] == 12.5)
    daily, trades = screen._replay(np.array([1000.]), samples["y"][:1, 0],
        samples["dates"][:1], samples["decision_ns"][:1], samples["tick_value"][:1],
        "ES", ("2020-09-13",), 15)
    assert 0 < daily["2020-09-13"] < 250
    assert trades["2020-09-13"] == 1
    frozen = screen.plan(Path(__file__).resolve().parents[1])
    assert len(frozen["sources"]) == 20
    assert len(frozen["candidates"]) == 21
    assert frozen["failed_predecessor_plan_id"] == screen.FAILED_PREDECESSOR_PLAN_ID
    with pytest.raises(UnauthorizedOperation):
        screen.require_scope(Path(__file__).resolve().parents[1], {"plan_id": "wrong"})


def test_descriptive_nested_search_keeps_outer_scores_out_of_selection(monkeypatch):
    candidate = next(c for c in screen.candidates() if c["family"] == "ridge"
                     and c["pack"] == "baseline" and c["horizon"] == 15)
    monkeypatch.setattr(screen, "candidates", lambda: (candidate,))
    n = 1010
    days = np.array([(date(2018, 1, 1) + timedelta(days=i)).isoformat() for i in range(n)])
    x = np.zeros((n, len(screen.FEATURES)))
    x[:, 0] = np.sin(np.arange(n))
    data = {"market": "ES", "dates": days, "x": x,
            "y": np.column_stack([20*x[:, 0], 20*x[:, 0], 20*x[:, 0]]),
            "decision_ns": np.arange(n, dtype=np.int64) * 86400_000_000_000,
            "tick_value": np.full(n, 12.5)}
    result = screen._search(data, monotonic(), lambda: None)
    assert len(result["outer_folds"]) == 8
    assert result["fits"] == 40
    assert all(len(fold["candidate_scores"]) == 1 for fold in result["outer_folds"])


def test_expansion_uses_training_target_and_rejects_redundant_columns():
    rng = np.random.default_rng(724)
    x = rng.normal(size=(200, len(screen.FEATURES)))
    x[:, 3] = x[:, 2]
    y = x[:, 2].copy()
    weights = np.full(150, 1 / 150)
    chosen = screen._expanded_columns(x[:150], y[:150], weights)
    assert 2 in chosen and 3 not in chosen
    x[150:] *= 1000
    y[150:] *= -1000
    assert screen._expanded_columns(x[:150], y[:150], weights) == chosen
