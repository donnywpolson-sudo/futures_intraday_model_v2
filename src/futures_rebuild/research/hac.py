"""Explicit-lag Bartlett Newey-West inference for a mean."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .contracts import ResearchContractError, explicit_int, explicit_real, finite_float64


@dataclass(frozen=True)
class HACMeanResult:
    observations: int
    lag: int
    mean: float
    long_run_variance: float
    variance_of_mean: float
    standard_error: float
    status: str


def newey_west_mean(x: np.ndarray, *, lag: int) -> HACMeanResult:
    values = finite_float64(x, name="x", ndim=1)
    checked_lag = explicit_int(lag, name="lag")
    n = len(values)
    if checked_lag < 0 or checked_lag >= n:
        raise ResearchContractError("lag must satisfy 0 <= lag < n")
    if n < max(3, checked_lag + 2):
        raise ResearchContractError("too few observations for declared lag")
    try:
        with np.errstate(over="raise", invalid="raise", divide="raise"):
            mean = float(np.mean(values, dtype=np.float64))
            centered = values - mean
            gamma0 = float(np.dot(centered, centered) / n)
            long_run_variance = gamma0
            for offset in range(1, checked_lag + 1):
                covariance = float(np.dot(centered[offset:], centered[:-offset]) / n)
                weight = 1.0 - offset / (checked_lag + 1.0)
                long_run_variance += 2.0 * weight * covariance
    except FloatingPointError as error:
        raise ResearchContractError("HAC arithmetic overflowed or became invalid") from error
    if not all(np.isfinite(item) for item in (mean, gamma0, long_run_variance)):
        raise ResearchContractError("HAC arithmetic produced a non-finite value")
    tolerance = (
        np.finfo(np.float64).eps
        * max(1.0, abs(gamma0))
        * max(1, 2 * checked_lag + 1)
        * 64.0
    )
    if long_run_variance < -tolerance:
        raise ResearchContractError("Newey-West LRV is materially negative")
    if abs(long_run_variance) <= tolerance:
        long_run_variance = 0.0
    variance_of_mean = long_run_variance / n
    standard_error = float(np.sqrt(variance_of_mean))
    return HACMeanResult(
        observations=n,
        lag=checked_lag,
        mean=mean,
        long_run_variance=float(long_run_variance),
        variance_of_mean=float(variance_of_mean),
        standard_error=standard_error,
        status="OK" if standard_error > 0.0 else "DEGENERATE",
    )


def hac_t_statistic(x: np.ndarray, *, lag: int, null_mean: float = 0.0) -> float:
    checked_null = explicit_real(null_mean, name="null_mean")
    result = newey_west_mean(x, lag=lag)
    if result.status != "OK":
        raise ResearchContractError("a degenerate HAC estimate has no t statistic")
    return (result.mean - checked_null) / result.standard_error
