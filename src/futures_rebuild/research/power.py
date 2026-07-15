"""Training-only HAC-LRV minimum detectable effect planning."""

from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from statistics import NormalDist

import numpy as np

from .contracts import ResearchContractError, explicit_int, explicit_real, finite_float64
from .hac import newey_west_mean


_NORMAL = NormalDist()


@dataclass(frozen=True)
class PowerPlan:
    training_observations: int
    planned_evaluation_observations: int
    hac_lag: int
    long_run_variance: float
    variance_inflation: float
    alpha: float
    target_power: float
    alternative: str
    minimum_detectable_mean: float
    economic_mean_hurdle: float
    required_evaluation_observations: int
    adequately_powered: bool
    status: str


def training_only_mde(
    training_differentials: np.ndarray,
    *,
    partition_role: str,
    hac_lag: int,
    planned_evaluation_observations: int,
    alpha: float,
    target_power: float,
    alternative: str,
    economic_mean_hurdle: float,
    variance_inflation: float = 1.0,
) -> PowerPlan:
    values = finite_float64(training_differentials, name="training_differentials", ndim=1)
    if partition_role != "TRAIN":
        raise ResearchContractError("MDE inputs must be TRAIN partition data")
    planned = explicit_int(
        planned_evaluation_observations, name="planned_evaluation_observations"
    )
    checked_alpha = explicit_real(alpha, name="alpha")
    checked_power = explicit_real(target_power, name="target_power")
    hurdle = explicit_real(economic_mean_hurdle, name="economic_mean_hurdle")
    inflation = explicit_real(variance_inflation, name="variance_inflation")
    if planned < 1 or not (0.0 < checked_alpha < 1.0) or not (
        0.0 < checked_power < 1.0
    ):
        raise ResearchContractError("power-plan counts/probabilities are invalid")
    if alternative not in {"greater", "two-sided"}:
        raise ResearchContractError("alternative must be greater or two-sided")
    if hurdle <= 0.0 or inflation < 1.0:
        raise ResearchContractError("hurdle must be positive and inflation at least one")
    hac = newey_west_mean(values, lag=hac_lag)
    if hac.status != "OK" or hac.long_run_variance <= 0.0:
        raise ResearchContractError("training LRV is degenerate")
    probability = 1.0 - (
        checked_alpha if alternative == "greater" else checked_alpha / 2.0
    )
    critical = _NORMAL.inv_cdf(probability)
    power_quantile = _NORMAL.inv_cdf(checked_power)
    if critical <= 0.0 or power_quantile <= 0.0:
        raise ResearchContractError("requires alpha < .5 and target power > .5")
    z_sum = critical + power_quantile
    inflated_lrv = hac.long_run_variance * inflation
    mde = z_sum * np.sqrt(inflated_lrv / planned)
    required_float = inflated_lrv * (z_sum / hurdle) ** 2
    if not all(np.isfinite(item) for item in (inflated_lrv, mde, required_float)):
        raise ResearchContractError("power-plan arithmetic is non-finite")
    required = ceil(required_float)
    adequately_powered = bool(planned >= required and hurdle >= mde)
    return PowerPlan(
        len(values),
        planned,
        hac.lag,
        hac.long_run_variance,
        inflation,
        checked_alpha,
        checked_power,
        alternative,
        float(mde),
        hurdle,
        required,
        adequately_powered,
        "TRAINING_PLAN_ONLY",
    )
