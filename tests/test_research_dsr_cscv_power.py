from __future__ import annotations

import numpy as np
import pytest

from futures_rebuild.research import (
    ResearchContractError,
    deflated_sharpe_ratio,
    exhaustive_cscv_pbo,
    training_only_mde,
)


def test_dsr_census_mean_and_selected_trial_binding_oracle() -> None:
    returns = np.arange(5, dtype=np.float64)
    selected_sharpe = float(np.mean(returns) / np.std(returns, ddof=1))
    result = deflated_sharpe_ratio(
        returns,
        np.asarray([0.0, selected_sharpe], dtype=np.float64),
        raw_trial_count=2,
        selected_trial_index=1,
    )
    assert result.selected_sharpe_per_period == pytest.approx(1.2649110640673518)
    assert result.trial_sharpe_mean == pytest.approx(0.6324555320336759)
    assert result.trial_sharpe_std == pytest.approx(0.8944271909999159)
    assert result.expected_maximum_sharpe == pytest.approx(1.0973388446257617)
    assert result.probability == pytest.approx(0.6164722577174275)
    assert result.selection_rule == "MAX_SHARPE"


def test_dsr_rejects_unrelated_or_nonwinning_census_row() -> None:
    returns = np.arange(5, dtype=np.float64)
    with pytest.raises(ResearchContractError, match="do not match"):
        deflated_sharpe_ratio(
            returns,
            np.asarray([0.0, 1.0], dtype=np.float64),
            raw_trial_count=2,
            selected_trial_index=1,
        )
    selected_sharpe = float(np.mean(returns) / np.std(returns, ddof=1))
    with pytest.raises(ResearchContractError, match="unique deterministic"):
        deflated_sharpe_ratio(
            returns,
            np.asarray([selected_sharpe, selected_sharpe], dtype=np.float64),
            raw_trial_count=2,
            selected_trial_index=1,
        )


def test_exhaustive_cscv_pbo_oracle() -> None:
    returns = np.asarray(
        [[3.0, 0.0], [3.0, 0.0], [0.0, 2.0], [0.0, 2.0]],
        dtype=np.float64,
    )
    result = exhaustive_cscv_pbo(
        returns, strategy_ids=("A", "B"), blocks=4, metric="mean"
    )
    assert result.combinations == 6
    assert result.pbo_strict == pytest.approx(1.0 / 3.0)
    assert result.pbo_conservative == pytest.approx(1.0 / 3.0)


def test_cscv_ties_sampling_and_bool_tolerance_fail_closed() -> None:
    with pytest.raises(ResearchContractError, match="winner is tied"):
        exhaustive_cscv_pbo(
            np.ones((8, 2), dtype=np.float64),
            strategy_ids=("A", "B"),
            blocks=4,
        )
    values = np.column_stack((np.arange(8), -np.arange(8))).astype(np.float64)
    with pytest.raises(ResearchContractError, match="combination cap"):
        exhaustive_cscv_pbo(
            values,
            strategy_ids=("A", "B"),
            blocks=4,
            maximum_combinations=5,
        )
    with pytest.raises(ResearchContractError, match="explicit real"):
        exhaustive_cscv_pbo(
            values,
            strategy_ids=("A", "B"),
            blocks=4,
            tie_tolerance=True,
        )


def test_training_only_mde_oracle_and_partition_guard() -> None:
    training = np.asarray([-2.0, 2.0, -2.0, 2.0], dtype=np.float64)
    result = training_only_mde(
        training,
        partition_role="TRAIN",
        hac_lag=0,
        planned_evaluation_observations=100,
        alpha=0.05,
        target_power=0.8,
        alternative="greater",
        economic_mean_hurdle=0.5,
    )
    assert result.long_run_variance == pytest.approx(4.0)
    assert result.minimum_detectable_mean == pytest.approx(0.4972949721048773)
    assert result.required_evaluation_observations == 99
    assert result.adequately_powered is True
    with pytest.raises(ResearchContractError, match="TRAIN"):
        training_only_mde(
            training,
            partition_role="AUDIT",
            hac_lag=0,
            planned_evaluation_observations=100,
            alpha=0.05,
            target_power=0.8,
            alternative="greater",
            economic_mean_hurdle=0.5,
        )
    with pytest.raises(ResearchContractError, match="explicit real"):
        training_only_mde(
            training,
            partition_role="TRAIN",
            hac_lag=0,
            planned_evaluation_observations=100,
            alpha=True,
            target_power=0.8,
            alternative="greater",
            economic_mean_hurdle=0.5,
        )
