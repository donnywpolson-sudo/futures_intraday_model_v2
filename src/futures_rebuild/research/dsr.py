"""Deflated Sharpe mechanics bound to a complete registered trial census."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import NormalDist

import numpy as np

from .contracts import ResearchContractError, explicit_int, finite_float64


_EULER_MASCHERONI = 0.5772156649015329
_NORMAL = NormalDist()


@dataclass(frozen=True)
class DeflatedSharpeResult:
    observations: int
    raw_trial_count: int
    selected_trial_index: int
    selection_rule: str
    selected_sharpe_per_period: float
    trial_sharpe_mean: float
    trial_sharpe_std: float
    expected_maximum_sharpe: float
    skewness: float
    kurtosis_non_excess: float
    test_statistic: float
    probability: float
    status: str


def deflated_sharpe_ratio(
    returns: np.ndarray,
    trial_sharpes: np.ndarray,
    *,
    raw_trial_count: int,
    selected_trial_index: int,
    selection_rule: str = "MAX_SHARPE",
) -> DeflatedSharpeResult:
    values = finite_float64(returns, name="returns", ndim=1)
    census = finite_float64(trial_sharpes, name="trial_sharpes", ndim=1)
    count = explicit_int(raw_trial_count, name="raw_trial_count")
    selected_index = explicit_int(selected_trial_index, name="selected_trial_index")
    if count != len(census) or count < 2:
        raise ResearchContractError("raw_trial_count must equal a census of at least two")
    if not (0 <= selected_index < count):
        raise ResearchContractError("selected_trial_index is out of bounds")
    if selection_rule != "MAX_SHARPE":
        raise ResearchContractError("only prebound MAX_SHARPE selection is supported")
    if len(values) < 3:
        raise ResearchContractError("DSR needs at least three returns")
    try:
        with np.errstate(over="raise", invalid="raise", divide="raise"):
            selected_mean = float(np.mean(values, dtype=np.float64))
            selected_std = float(np.std(values, ddof=1))
            centered = values - selected_mean
            second = float(np.mean(centered**2, dtype=np.float64))
            third = float(np.mean(centered**3, dtype=np.float64))
            fourth = float(np.mean(centered**4, dtype=np.float64))
    except FloatingPointError as error:
        raise ResearchContractError("DSR return moments overflowed") from error
    if not all(np.isfinite(item) for item in (selected_mean, selected_std, second, third, fourth)):
        raise ResearchContractError("DSR return moments are non-finite")
    if selected_std <= 0.0 or second <= 0.0:
        raise ResearchContractError("selected returns have degenerate variance")
    selected_sharpe = selected_mean / selected_std
    skewness = third / second**1.5
    kurtosis = fourth / second**2
    tolerance = 1e-12
    if not np.isclose(
        census[selected_index], selected_sharpe, rtol=tolerance, atol=tolerance
    ):
        raise ResearchContractError("selected returns do not match the bound trial census row")
    maximum = float(np.max(census))
    winners = np.flatnonzero(
        np.isclose(census, maximum, rtol=tolerance, atol=tolerance)
    )
    if len(winners) != 1 or int(winners[0]) != selected_index:
        raise ResearchContractError("selected trial is not the unique deterministic census winner")
    try:
        with np.errstate(over="raise", invalid="raise", divide="raise"):
            trial_mean = float(np.mean(census, dtype=np.float64))
            trial_std = float(np.std(census, ddof=1))
    except FloatingPointError as error:
        raise ResearchContractError("trial Sharpe moments overflowed") from error
    if not np.isfinite(trial_mean) or not np.isfinite(trial_std) or trial_std <= 0.0:
        raise ResearchContractError("trial Sharpe census variance is degenerate")
    n_trials = float(count)
    extreme_quantile = (
        (1.0 - _EULER_MASCHERONI) * _NORMAL.inv_cdf(1.0 - 1.0 / n_trials)
        + _EULER_MASCHERONI
        * _NORMAL.inv_cdf(1.0 - 1.0 / (n_trials * np.e))
    )
    expected_maximum = trial_mean + trial_std * extreme_quantile
    denominator_squared = (
        1.0
        - skewness * selected_sharpe
        + ((kurtosis - 1.0) / 4.0) * selected_sharpe**2
    )
    if not np.isfinite(expected_maximum) or not np.isfinite(denominator_squared):
        raise ResearchContractError("DSR arithmetic is non-finite")
    if denominator_squared <= 0.0:
        raise ResearchContractError("DSR moment correction is non-positive")
    statistic = (
        (selected_sharpe - expected_maximum)
        * np.sqrt(len(values) - 1.0)
        / np.sqrt(denominator_squared)
    )
    probability = _NORMAL.cdf(float(statistic))
    if not np.isfinite(probability):
        raise ResearchContractError("DSR probability is non-finite")
    return DeflatedSharpeResult(
        observations=len(values),
        raw_trial_count=count,
        selected_trial_index=selected_index,
        selection_rule=selection_rule,
        selected_sharpe_per_period=float(selected_sharpe),
        trial_sharpe_mean=trial_mean,
        trial_sharpe_std=trial_std,
        expected_maximum_sharpe=float(expected_maximum),
        skewness=float(skewness),
        kurtosis_non_excess=float(kurtosis),
        test_statistic=float(statistic),
        probability=float(probability),
        status="MECHANICS_ONLY",
    )
