"""Null-centered Romano-Wolf max-T stepdown for aligned futures sleeves."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .bootstrap import stationary_bootstrap_index_rows
from .contracts import (
    ResearchContractError,
    explicit_int,
    finite_float64,
    require_unique_ascii_ids,
)
from .hac import hac_t_statistic


@dataclass(frozen=True)
class RomanoWolfResult:
    hypothesis_ids: tuple[str, ...]
    tail: str
    observed_statistics: np.ndarray
    stepdown_order: np.ndarray
    stage_p_values: np.ndarray
    adjusted_p_values: np.ndarray
    resamples: int
    null_centered: bool


def romano_wolf_stepdown(
    observed_statistics: np.ndarray,
    null_centered_bootstrap_statistics: np.ndarray,
    *,
    hypothesis_ids: tuple[str, ...],
    tail: str,
    minimum_resamples: int = 999,
    maximum_bootstrap_stat_bytes: int = 256 * 1024 * 1024,
) -> RomanoWolfResult:
    observed = finite_float64(observed_statistics, name="observed_statistics", ndim=1)
    boot = finite_float64(
        null_centered_bootstrap_statistics,
        name="null_centered_bootstrap_statistics",
        ndim=2,
    )
    ids = require_unique_ascii_ids(hypothesis_ids, name="hypothesis_ids")
    if len(observed) != len(ids) or boot.shape[1] != len(ids):
        raise ResearchContractError("statistics and hypothesis_ids must align")
    if tail not in {"greater", "two-sided"}:
        raise ResearchContractError("tail must be greater or two-sided")
    minimum = explicit_int(minimum_resamples, name="minimum_resamples")
    if minimum < 1 or boot.shape[0] < minimum:
        raise ResearchContractError("too few bootstrap resamples")
    direct_cap = explicit_int(
        maximum_bootstrap_stat_bytes, name="maximum_bootstrap_stat_bytes"
    )
    if direct_cap < 1 or boot.nbytes > direct_cap:
        raise ResearchContractError("bootstrap statistic matrix exceeds memory cap")
    observed_extreme = observed if tail == "greater" else np.abs(observed)
    boot_extreme = boot if tail == "greater" else np.abs(boot)
    ids_array = np.asarray(ids, dtype="U")
    order = np.lexsort((ids_array, -observed_extreme)).astype(np.int64)
    stages = np.empty(len(ids), dtype=np.float64)
    adjusted_ordered = np.empty(len(ids), dtype=np.float64)
    running = 0.0
    denominator = boot.shape[0] + 1.0
    for stage in range(len(ids)):
        remaining = order[stage:]
        maxima = np.max(boot_extreme[:, remaining], axis=1)
        exceedances = int(np.count_nonzero(maxima >= observed_extreme[order[stage]]))
        stage_p = (1.0 + exceedances) / denominator
        stages[stage] = stage_p
        running = max(running, stage_p)
        adjusted_ordered[stage] = min(1.0, running)
    adjusted = np.empty(len(ids), dtype=np.float64)
    adjusted[order] = adjusted_ordered
    return RomanoWolfResult(
        ids,
        tail,
        observed.copy(),
        order,
        stages,
        adjusted,
        boot.shape[0],
        True,
    )


def romano_wolf_from_differentials(
    differentials: np.ndarray,
    *,
    hypothesis_ids: tuple[str, ...],
    hac_lag: int,
    mean_block_length: float,
    n_resamples: int,
    seed: int,
    tail: str = "greater",
    minimum_resamples: int = 999,
    maximum_bootstrap_stat_bytes: int = 256 * 1024 * 1024,
) -> RomanoWolfResult:
    values = finite_float64(differentials, name="differentials", ndim=2)
    ids = require_unique_ascii_ids(hypothesis_ids, name="hypothesis_ids")
    if values.shape[1] != len(ids):
        raise ResearchContractError("differentials and hypothesis_ids must align")
    resamples = explicit_int(n_resamples, name="n_resamples")
    minimum = explicit_int(minimum_resamples, name="minimum_resamples")
    cap = explicit_int(
        maximum_bootstrap_stat_bytes, name="maximum_bootstrap_stat_bytes"
    )
    if resamples < minimum or minimum < 1:
        raise ResearchContractError("too few bootstrap resamples")
    if cap < 1 or resamples > cap // np.dtype(np.float64).itemsize // len(ids):
        raise ResearchContractError("bootstrap statistic matrix exceeds memory cap")
    observed = np.asarray(
        [hac_t_statistic(values[:, column], lag=hac_lag) for column in range(len(ids))],
        dtype=np.float64,
    )
    centered = values - np.mean(values, axis=0, dtype=np.float64)
    bootstrap_statistics = np.empty((resamples, len(ids)), dtype=np.float64)
    rows = stationary_bootstrap_index_rows(
        n_observations=values.shape[0],
        n_resamples=resamples,
        mean_block_length=mean_block_length,
        seed=seed,
    )
    for resample, indices in enumerate(rows):
        sample = centered[indices, :]
        for column in range(len(ids)):
            bootstrap_statistics[resample, column] = hac_t_statistic(
                sample[:, column], lag=hac_lag
            )
    return romano_wolf_stepdown(
        observed,
        bootstrap_statistics,
        hypothesis_ids=ids,
        tail=tail,
        minimum_resamples=minimum_resamples,
        maximum_bootstrap_stat_bytes=maximum_bootstrap_stat_bytes,
    )
