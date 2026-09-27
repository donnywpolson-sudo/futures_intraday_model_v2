import numpy as np
import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.fixed_hour_multifamily_discovery import (
    FEATURE_NAMES,
    INNER_MINIMUM_TRADES,
    SignedTargetRidge,
    frozen_design,
    multifamily_feature_values,
    prepare_session,
    score_predictions,
    select_penalty,
)
from futures_rebuild.fixed_hour_ridge_discovery import NANO, _synthetic_row, checkpoint_ns


pytestmark = pytest.mark.current


def test_multifamily_mechanism_is_causal_bounded_and_training_only() -> None:
    decision = checkpoint_ns("2022-01-10")
    start = decision - 21 * 60 * NANO - 5 * NANO
    rows = []
    for index in range(21):
        row = _synthetic_row(start + index * 60 * NANO, 100 + index * 0.25 + (index % 3) * 0.25, 100 + index * 7)
        row.update(min_price_increment_nano=int(0.25 * NANO), multiplier_nano=50 * NANO)
        row["high_nano"] = row["close_nano"] + int(0.25 * NANO)
        row["low_nano"] = row["close_nano"] - int(0.25 * NANO)
        rows.append(row)

    features, audit = multifamily_feature_values(rows, decision)
    assert tuple(features) == FEATURE_NAMES and len(features) == 18
    assert audit["bar_count"] == 21 and audit["actual_contract"] == "ESH2"

    future = dict(rows[-1])
    future.update(bar_start_ns=decision, bar_end_ns=decision + 60 * NANO, source_timestamp_ns=decision, available_at_ns=decision + 65 * NANO, decision_eligible_at_ns=decision + 65 * NANO, close_nano=200 * NANO)
    assert multifamily_feature_values([*rows, future], decision)[0] == features

    gapped = [dict(row) for row in rows]
    gapped[10]["bar_start_ns"] += 60 * NANO
    gapped[10]["bar_end_ns"] += 60 * NANO
    with pytest.raises(IntegrityError, match="consecutive"):
        multifamily_feature_values(gapped, decision)
    mixed = [dict(row) for row in rows]
    mixed[0]["actual_contract"] = "ESM2"
    with pytest.raises(IntegrityError, match="contract"):
        multifamily_feature_values(mixed, decision)

    entry_start = (decision // (60 * NANO) + 1) * 60 * NANO
    outcome_rows = []
    for index in range(61):
        row = _synthetic_row(entry_start + index * 60 * NANO, 102 + index * 0.02, 300 + index)
        row.update(min_price_increment_nano=int(0.25 * NANO), multiplier_nano=50 * NANO)
        outcome_rows.append(row)
    prepared = prepare_session([*rows, *outcome_rows], decision, stress_cost_usd=60.0)
    assert prepared["status"] == "SCORABLE" and prepared["signed_gross_r"] > 0
    assert prepared["stress_cost_r"] > 0 and set(prepared["stress_net_mfe_mae"]) == {"long", "short"}
    intents, net_r = score_predictions(np.array([0.5, -0.5, 0.01]), np.array([0.4, -0.4, 9.0]), np.full(3, 0.1))
    assert intents.tolist() == [1, -1, 0] and np.allclose(net_r, [0.3, 0.3, 0.0])

    rng = np.random.default_rng(42)
    folds = []
    for _ in range(4):
        train_x = rng.normal(size=(220, len(FEATURE_NAMES)))
        validation_x = rng.normal(size=(42, len(FEATURE_NAMES)))
        train_y = 0.5 * train_x[:, 0] - 0.25 * train_x[:, 12] + rng.normal(scale=0.05, size=220)
        validation_y = 0.5 * validation_x[:, 0] - 0.25 * validation_x[:, 12] + rng.normal(scale=0.05, size=42)
        folds.append((train_x, train_y, validation_x, validation_y, np.full(42, 0.03)))
    selection = select_penalty(folds)
    assert selection["selected_ridge_penalty"] in (0.1, 1.0, 10.0)
    assert all(min(candidate["inner_trade_counts"]) >= INNER_MINIMUM_TRADES for candidate in selection["candidates"] if candidate["eligible"])
    assert selection["candidates"][0]["feature_stability"][0]["sign_agreement"] >= 0.75

    model = SignedTargetRidge(1.0).fit(folds[0][0], folds[0][1])
    assert np.allclose(model.mean, np.mean(folds[0][0], axis=0))
    design = frozen_design()
    assert design["search"]["candidate_count"] == 3
    assert design["features"]["all_features_enter_every_candidate"] is True
    assert not any(design["authority"].values())


def test_all_eighteen_tick_features_support_signed_and_zero_prices() -> None:
    decision = checkpoint_ns("2022-01-10")
    start = decision - 21 * 60 * NANO - 5 * NANO
    tick = int(0.25 * NANO)
    close_ticks = np.asarray([-10, -8, -9, -5, -4, -6, -1, 0, 2, 1, 4, 8, 7, 9, 12, 11, 15, 14, 18, 17, 20], dtype=np.float64)
    volumes = np.asarray([0, 2, 4, 8, 16, 3, 6, 12, 24, 5, 10, 20, 7, 14, 28, 9, 18, 36, 11, 22, 44], dtype=np.float64)
    rows = []
    for index, (close_ticks_value, volume) in enumerate(zip(close_ticks, volumes, strict=True)):
        close = int(close_ticks_value) * tick
        row = _synthetic_row(start + index * 60 * NANO, 1.0, int(volume))
        row.update(
            open_nano=close, high_nano=close + tick, low_nano=close - tick,
            close_nano=close, min_price_increment_nano=tick,
            multiplier_nano=50 * NANO,
        )
        rows.append(row)

    actual, _ = multifamily_feature_values(rows, decision)
    changes = np.diff(close_ticks)
    true_ranges = np.maximum.reduce((
        np.full(20, 2.0),
        np.abs(close_ticks[1:] + 1 - close_ticks[:-1]),
        np.abs(close_ticks[1:] - 1 - close_ticks[:-1]),
    ))
    rv5 = float(np.std(changes[-5:], ddof=0))
    rv20 = float(np.std(changes, ddof=0))
    matched_volume = volumes[1:]
    expected = {
        "close_change_ticks_1": changes[-1],
        "close_change_ticks_5": close_ticks[-1] - close_ticks[-6],
        "close_change_ticks_10": close_ticks[-1] - close_ticks[-11],
        "close_change_ticks_20": close_ticks[-1] - close_ticks[0],
        "signed_trend_efficiency_20": float(np.sum(changes) / np.sum(np.abs(changes))),
        "close_change_volatility_ticks_5": rv5,
        "close_change_volatility_ticks_20": rv20,
        "atr_10_ticks": float(np.mean(true_ranges[-10:])),
        "volatility_ratio_5_20": rv5 / rv20,
        "range_compression_5_20": float(np.mean(true_ranges[-5:]) / np.mean(true_ranges)),
        "signed_body_fraction": 0.0,
        "close_location_in_bar": 0.0,
        "close_location_in_range_20": float(2 * (close_ticks[-1] - (min(close_ticks) - 1)) / ((max(close_ticks) + 1) - (min(close_ticks) - 1)) - 1),
        "log1p_volume": float(np.log1p(volumes[-1])),
        "volume_ratio_prior20_median": float((1 + volumes[-1]) / (1 + np.median(volumes[:-1])) - 1),
        "recent_volume_share_5_20": float(np.sum(volumes[-5:]) / np.sum(volumes[-20:])),
        "price_direction_weighted_volume_20": float(np.sum(np.sign(changes) * matched_volume) / np.sum(matched_volume)),
        "return_log_volume_correlation_20": float(np.corrcoef(changes, np.log1p(matched_volume))[0, 1]),
    }
    assert tuple(actual) == FEATURE_NAMES
    assert len(actual) == len(expected) == 18
    for name, value in expected.items():
        assert actual[name] == pytest.approx(value)

    off_tick = [dict(row) for row in rows]
    off_tick[-1]["close_nano"] += 1
    with pytest.raises(IntegrityError, match="invalid OHLCV"):
        multifamily_feature_values(off_tick, decision)

    flat = []
    for index, row in enumerate(rows):
        copy = dict(row)
        copy.update(open_nano=0, high_nano=0, low_nano=0, close_nano=0, volume=0)
        flat.append(copy)
    flat_values, _ = multifamily_feature_values(flat, decision)
    assert all(np.isfinite(value) for value in flat_values.values())
    assert flat_values["close_change_volatility_ticks_20"] == 0.0
    assert flat_values["volatility_ratio_5_20"] == 0.0
    assert flat_values["close_location_in_bar"] == 0.0
    assert flat_values["recent_volume_share_5_20"] == 0.0
