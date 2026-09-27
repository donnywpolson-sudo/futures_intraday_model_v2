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


def _newey_west_mean_one(x: np.ndarray, *, lag: int) -> HACMeanResult:
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


def _newey_west_arrays(x: np.ndarray, *, lag: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, int]:
    values = finite_float64(x, name="x", ndim=2)
    checked_lag = explicit_int(lag, name="lag")
    n = values.shape[1]
    if checked_lag < 0 or checked_lag >= n or n < max(3, checked_lag + 2):
        raise ResearchContractError("lag or observation count is invalid")
    try:
        with np.errstate(over="raise", invalid="raise", divide="raise"):
            means = np.mean(values, axis=1, dtype=np.float64)
            centered = values - means[:, None]
            gamma0 = np.einsum("ij,ij->i", centered, centered, dtype=np.float64) / n
            long_run = gamma0.copy()
            for offset in range(1, checked_lag + 1):
                covariance = np.einsum(
                    "ij,ij->i", centered[:, offset:], centered[:, :-offset], dtype=np.float64
                ) / n
                long_run += 2.0 * (1.0 - offset / (checked_lag + 1.0)) * covariance
    except FloatingPointError as error:
        raise ResearchContractError("HAC arithmetic overflowed or became invalid") from error
    tolerance = np.finfo(np.float64).eps * np.maximum(1.0, np.abs(gamma0)) * max(1, 2 * checked_lag + 1) * 64.0
    if bool(np.any(long_run < -tolerance)):
        raise ResearchContractError("Newey-West LRV is materially negative")
    long_run = np.where(np.abs(long_run) <= tolerance, 0.0, long_run)
    standard_errors = np.sqrt(long_run / n)
    return means, long_run, standard_errors, checked_lag


def newey_west_mean_batch(x: np.ndarray, *, lag: int) -> tuple[HACMeanResult, ...]:
    """Apply the canonical HAC kernel to a leading campaign dimension."""

    means, long_run, standard_errors, checked_lag = _newey_west_arrays(x, lag=lag)
    n = np.asarray(x).shape[1]
    return tuple(
        HACMeanResult(n, checked_lag, float(means[index]), float(long_run[index]), float(long_run[index] / n), float(standard_errors[index]), "OK" if standard_errors[index] > 0.0 else "DEGENERATE")
        for index in range(len(means))
    )


def newey_west_mean(x: np.ndarray, *, lag: int) -> HACMeanResult:
    values = finite_float64(x, name="x", ndim=1)
    return newey_west_mean_batch(values[None, :], lag=lag)[0]


def hac_t_statistic(x: np.ndarray, *, lag: int, null_mean: float = 0.0) -> float:
    checked_null = explicit_real(null_mean, name="null_mean")
    result = newey_west_mean(x, lag=lag)
    if result.status != "OK":
        raise ResearchContractError("a degenerate HAC estimate has no t statistic")
    return (result.mean - checked_null) / result.standard_error


def hac_t_statistic_batch(
    x: np.ndarray, *, lag: int, null_mean: float = 0.0
) -> np.ndarray:
    values = finite_float64(x, name="x", ndim=2)
    checked_null = explicit_real(null_mean, name="null_mean")
    means, _long_run, standard_errors, _checked_lag = _newey_west_arrays(values, lag=lag)
    if bool(np.any(standard_errors <= 0.0)):
        raise ResearchContractError("a degenerate HAC estimate has no t statistic")
    return np.asarray((means - checked_null) / standard_errors, dtype=np.float64)
