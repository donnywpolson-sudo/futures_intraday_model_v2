from __future__ import annotations

import numpy as np
import pytest

from futures_rebuild.research import (
    ResearchContractError,
    apply_shared_indices,
    hac_t_statistic,
    newey_west_mean,
    romano_wolf_from_differentials,
    romano_wolf_stepdown,
    stationary_bootstrap_index_kernel,
    stationary_bootstrap_index_rows,
    stationary_bootstrap_indices,
)


def test_newey_west_hand_oracle_and_degenerate_guard() -> None:
    result = newey_west_mean(np.asarray([1.0, 2.0, 3.0, 4.0]), lag=1)
    assert result.mean == pytest.approx(2.5)
    assert result.long_run_variance == pytest.approx(1.5625)
    assert result.standard_error == pytest.approx(0.625)
    assert result.status == "OK"
    with pytest.raises(ResearchContractError, match="degenerate"):
        hac_t_statistic(np.ones(5, dtype=np.float64), lag=0)
    with pytest.raises(ResearchContractError, match="explicit real"):
        hac_t_statistic(np.arange(5, dtype=np.float64), lag=0, null_mean=True)


def test_stationary_bootstrap_uniform_oracle() -> None:
    restart = np.asarray([[0.0, 0.9, 0.1, 0.8, 0.2]], dtype=np.float64)
    starts = np.asarray([[0.65, 0.2, 0.99, 0.4, 0.0]], dtype=np.float64)
    actual = stationary_bootstrap_index_kernel(
        n_observations=5,
        mean_block_length=2.0,
        restart_uniforms=restart,
        start_uniforms=starts,
    )
    np.testing.assert_array_equal(actual, np.asarray([[3, 4, 4, 0, 0]], dtype=np.int64))


def test_streamed_and_materialized_indices_match_and_share_columns() -> None:
    materialized = stationary_bootstrap_indices(
        n_observations=6, n_resamples=4, mean_block_length=2.0, seed=41
    )
    streamed = np.stack(
        tuple(
            stationary_bootstrap_index_rows(
                n_observations=6,
                n_resamples=4,
                mean_block_length=2.0,
                seed=41,
            )
        )
    )
    np.testing.assert_array_equal(materialized, streamed)
    matrix = np.column_stack(
        (np.arange(6, dtype=np.float64), np.arange(6, dtype=np.float64) + 100.0)
    )
    resampled = apply_shared_indices(matrix, materialized)
    np.testing.assert_array_equal(resampled[:, :, 1] - resampled[:, :, 0], 100.0)
    with pytest.raises(ResearchContractError, match="memory cap"):
        stationary_bootstrap_indices(
            n_observations=100,
            n_resamples=100,
            mean_block_length=2.0,
            seed=1,
            maximum_materialized_bytes=100,
        )


def test_romano_wolf_plus_one_stepdown_oracle() -> None:
    result = romano_wolf_stepdown(
        np.asarray([3.0, 2.0, 1.0], dtype=np.float64),
        np.asarray(
            [
                [2.5, 1.5, 0.5],
                [3.5, 0.5, 1.2],
                [0.0, 2.5, 0.8],
                [1.0, 1.0, 1.5],
            ],
            dtype=np.float64,
        ),
        hypothesis_ids=("A", "B", "C"),
        tail="greater",
        minimum_resamples=4,
    )
    np.testing.assert_allclose(result.adjusted_p_values, np.asarray([0.4, 0.4, 0.6]))


def test_full_romano_wolf_stream_is_deterministic() -> None:
    time = np.arange(64, dtype=np.float64)
    values = np.column_stack(
        (0.02 + np.sin(time * 0.37), 0.01 + np.cos(time * 0.23))
    )
    first = romano_wolf_from_differentials(
        values,
        hypothesis_ids=("ES-LONG", "NQ-SHORT"),
        hac_lag=2,
        mean_block_length=4.0,
        n_resamples=31,
        seed=9,
        minimum_resamples=31,
    )
    second = romano_wolf_from_differentials(
        values,
        hypothesis_ids=("ES-LONG", "NQ-SHORT"),
        hac_lag=2,
        mean_block_length=4.0,
        n_resamples=31,
        seed=9,
        minimum_resamples=31,
    )
    np.testing.assert_array_equal(first.adjusted_p_values, second.adjusted_p_values)
    np.testing.assert_allclose(first.adjusted_p_values, np.asarray([0.5625, 0.5625]))
    assert first.null_centered is True
