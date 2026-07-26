from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np

from futures_rebuild.research import (
    Direction,
    NegativeControlOutcome,
    NegativeControlState,
    ResearchBookCharter,
    ResearchBookState,
    SleeveKey,
    SleeveState,
    SleeveThresholds,
    SyntheticSleeveMetrics,
    apply_negative_control_indices,
    circular_block_derangement_indices,
    evaluate_negative_controls,
    evaluate_research_book,
    evaluate_synthetic_sleeve,
    make_synthetic_permit,
    synthetic_noise_control,
)


def test_block_derangement_noise_and_control_outcomes() -> None:
    indices = circular_block_derangement_indices(
        n_observations=8, block_size=2, seed=7
    )
    np.testing.assert_array_equal(indices, np.asarray([2, 3, 4, 5, 6, 7, 0, 1]))
    values = np.column_stack(
        (np.arange(8, dtype=np.float64), np.arange(8, dtype=np.float64) + 10.0)
    )
    controlled = apply_negative_control_indices(values, indices)
    np.testing.assert_array_equal(controlled[:, 1] - controlled[:, 0], 10.0)
    np.testing.assert_array_equal(
        synthetic_noise_control(shape=(4, 2), seed=91),
        synthetic_noise_control(shape=(4, 2), seed=91),
    )
    clear = evaluate_negative_controls(
        (
            NegativeControlOutcome("roll-map-shift", True, False),
            NegativeControlOutcome("economics-swap", True, False),
        )
    )
    assert clear.state is NegativeControlState.CLEAR
    suspicious = evaluate_negative_controls(
        (NegativeControlOutcome("future-roll", True, True),)
    )
    assert suspicious.state is NegativeControlState.LEAKAGE_SUSPECTED


def _metrics(*, p_value: float) -> SyntheticSleeveMetrics:
    return SyntheticSleeveMetrics(
        mean_after_costs=0.020,
        confidence_lower_bound=0.015,
        minimum_economically_effective_mean=0.010,
        romano_wolf_adjusted_p=p_value,
        dsr_probability=0.99,
        pbo_conservative=0.10,
        power_sufficient=True,
        negative_controls_clear=True,
        numerically_valid=True,
    )


def test_market_direction_sleeves_cannot_cross_subsidize() -> None:
    fixture = np.asarray([[0.0, 1.0], [1.0, 0.0]], dtype=np.float64)
    permit = make_synthetic_permit(fixture, generator_id="sleeve-oracle", seed=4)
    thresholds = SleeveThresholds(0.05, 0.95, 0.20)
    es_long_key = SleeveKey("ES", Direction.LONG)
    nq_short_key = SleeveKey("NQ", Direction.SHORT)
    es = evaluate_synthetic_sleeve(
        key=es_long_key,
        metrics=_metrics(p_value=0.01),
        thresholds=thresholds,
        permit=permit,
        fixture=fixture,
    )
    nq = evaluate_synthetic_sleeve(
        key=nq_short_key,
        metrics=_metrics(p_value=0.06),
        thresholds=thresholds,
        permit=permit,
        fixture=fixture,
    )
    assert es.state is SleeveState.MECHANICS_READY
    assert nq.state is SleeveState.MECHANICS_FAIL_CLOSED
    charter = ResearchBookCharter.create(
        registered_sleeves=(es_long_key, nq_short_key),
        included_sleeves=(es_long_key, nq_short_key),
    )
    assert evaluate_research_book(charter, (es, nq)) is ResearchBookState.MECHANICS_FAIL_CLOSED
    preexcluded = ResearchBookCharter.create(
        registered_sleeves=(es_long_key, nq_short_key),
        included_sleeves=(es_long_key,),
    )
    assert evaluate_research_book(preexcluded, (es, nq)) is ResearchBookState.MECHANICS_READY

    with np.testing.assert_raises_regex(ValueError, "explicit real float"):
        evaluate_synthetic_sleeve(
            key=es_long_key,
            metrics=replace(_metrics(p_value=0.01), mean_after_costs=True),
            thresholds=thresholds,
            permit=permit,
            fixture=fixture,
        )
    with np.testing.assert_raises_regex(ValueError, "exact bool"):
        evaluate_synthetic_sleeve(
            key=es_long_key,
            metrics=replace(_metrics(p_value=0.01), power_sufficient=np.bool_(True)),
            thresholds=thresholds,
            permit=permit,
            fixture=fixture,
        )


def test_research_package_has_no_stock_import_io_fit_or_alpha_state() -> None:
    package = Path(__file__).parents[1] / "src" / "futures_rebuild" / "research"
    source = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(package.glob("*.py"))
    ).lower()
    forbidden = (
        "us_stocks_swing_model",
        "requests.",
        "urllib",
        "http://",
        "https://",
        ".fit(",
        "read_csv(",
        "read_parquet(",
        "historical_pass",
        "alpha_pass",
    )
    for token in forbidden:
        assert token not in source
