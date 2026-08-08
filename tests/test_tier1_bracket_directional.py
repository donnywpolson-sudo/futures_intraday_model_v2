from decimal import Decimal

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_directional import (
    bounded_direction_score,
    materialize_directional_row,
    select_direction,
    training_only_neutral_threshold,
)
from futures_rebuild.tier1_bracket_trial import BracketBar


MINUTE = 60_000_000_000


def _bars(*, terminal_high: int = 130, terminal_low: int = 95) -> list[BracketBar]:
    rows = [BracketBar(index * MINUTE, 100, 105, 95, 100, "2021-01-04", "identity") for index in range(22)]
    rows[-1] = BracketBar(21 * MINUTE, 100, terminal_high, terminal_low, 100, "2021-01-04", "identity")
    return rows


def test_directional_materialization_keeps_net_r_primary_and_triple_barrier_diagnostic() -> None:
    row = materialize_directional_row(
        bars=_bars(), decision_index=20, tick_size_nano=1, tick_value_usd=Decimal("1"),
        stress_round_trip_cost_usd=Decimal("0"), volume=42.0,
    )

    assert row.long_net_r == Decimal("2")
    assert row.long_diagnostic == "TARGET_FIRST"
    assert row.short_diagnostic == "STOP_FIRST"
    assert row.features["bar_return"] == 0.0
    assert row.label_unlock_at_ns == 80 * MINUTE


def test_threshold_is_nearest_rank_training_only_and_neutralizes_small_scores() -> None:
    threshold = training_only_neutral_threshold(training_scores=[0.1, -0.2, 0.3, -0.4, 0.5])
    assert threshold == 0.3
    assert select_direction(score=0.29, threshold=threshold) == "neutral"
    assert select_direction(score=0.3, threshold=threshold) == "long"
    assert select_direction(score=-0.3, threshold=threshold) == "short"
    assert select_direction(score=None, threshold=threshold) == "neutral"


def test_score_is_bounded_and_rejects_nonfinite_training_inputs() -> None:
    score = bounded_direction_score(long_prediction_net_r=2.0, short_prediction_net_r=-1.0)
    assert score is not None and 0.0 < score < 1.0
    assert bounded_direction_score(long_prediction_net_r=float("nan"), short_prediction_net_r=1.0) is None
    with pytest.raises(IntegrityError, match="finite"):
        training_only_neutral_threshold(training_scores=[float("nan")])
