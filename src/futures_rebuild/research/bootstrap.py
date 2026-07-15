"""PCG64 stationary-bootstrap rows shared across all futures sleeves."""

from __future__ import annotations

from collections.abc import Iterator

import numpy as np

from .contracts import ResearchContractError, explicit_int, explicit_real, finite_float64


def stationary_bootstrap_index_kernel(
    *,
    n_observations: int,
    mean_block_length: float,
    restart_uniforms: np.ndarray,
    start_uniforms: np.ndarray,
) -> np.ndarray:
    n = explicit_int(n_observations, name="n_observations")
    block = explicit_real(mean_block_length, name="mean_block_length")
    if n < 2 or not (1.0 <= block <= n):
        raise ResearchContractError("bootstrap n/block-length settings are invalid")
    restart = finite_float64(restart_uniforms, name="restart_uniforms", ndim=2)
    starts = finite_float64(start_uniforms, name="start_uniforms", ndim=2)
    if restart.shape != starts.shape or restart.shape[1] != n:
        raise ResearchContractError("uniform arrays must share shape (B,n)")
    if bool(np.any((restart < 0.0) | (restart >= 1.0))):
        raise ResearchContractError("restart_uniforms must lie in [0,1)")
    if bool(np.any((starts < 0.0) | (starts >= 1.0))):
        raise ResearchContractError("start_uniforms must lie in [0,1)")
    probability = 1.0 / block
    indices = np.empty(restart.shape, dtype=np.int64)
    indices[:, 0] = np.floor(starts[:, 0] * n).astype(np.int64)
    for column in range(1, n):
        new_start = np.floor(starts[:, column] * n).astype(np.int64)
        continuation = (indices[:, column - 1] + 1) % n
        indices[:, column] = np.where(
            restart[:, column] < probability, new_start, continuation
        )
    return indices


def _validated_generation(
    *,
    n_observations: int,
    n_resamples: int,
    mean_block_length: float,
    seed: int,
) -> tuple[int, int, float, int]:
    n = explicit_int(n_observations, name="n_observations")
    resamples = explicit_int(n_resamples, name="n_resamples")
    block = explicit_real(mean_block_length, name="mean_block_length")
    checked_seed = explicit_int(seed, name="seed")
    if n < 2 or resamples < 1 or not (1.0 <= block <= n):
        raise ResearchContractError("bootstrap generation settings are invalid")
    if not (0 <= checked_seed < 2**64):
        raise ResearchContractError("seed must fit uint64")
    return n, resamples, block, checked_seed


def stationary_bootstrap_index_rows(
    *,
    n_observations: int,
    n_resamples: int,
    mean_block_length: float,
    seed: int,
) -> Iterator[np.ndarray]:
    """Yield one O(T)-memory row, using a fixed per-row RNG draw order."""

    n, resamples, block, checked_seed = _validated_generation(
        n_observations=n_observations,
        n_resamples=n_resamples,
        mean_block_length=mean_block_length,
        seed=seed,
    )
    generator = np.random.Generator(np.random.PCG64(checked_seed))
    for _ in range(resamples):
        restart = generator.random((1, n), dtype=np.float64)
        starts = generator.random((1, n), dtype=np.float64)
        yield stationary_bootstrap_index_kernel(
            n_observations=n,
            mean_block_length=block,
            restart_uniforms=restart,
            start_uniforms=starts,
        )[0]


def stationary_bootstrap_indices(
    *,
    n_observations: int,
    n_resamples: int,
    mean_block_length: float,
    seed: int,
    maximum_materialized_bytes: int = 256 * 1024 * 1024,
) -> np.ndarray:
    n, resamples, block, checked_seed = _validated_generation(
        n_observations=n_observations,
        n_resamples=n_resamples,
        mean_block_length=mean_block_length,
        seed=seed,
    )
    cap = explicit_int(maximum_materialized_bytes, name="maximum_materialized_bytes")
    if cap < 1 or resamples > cap // np.dtype(np.int64).itemsize // n:
        raise ResearchContractError("materialized index matrix exceeds memory cap")
    result = np.empty((resamples, n), dtype=np.int64)
    for row_number, row in enumerate(
        stationary_bootstrap_index_rows(
            n_observations=n,
            n_resamples=resamples,
            mean_block_length=block,
            seed=checked_seed,
        )
    ):
        result[row_number, :] = row
    return result


def apply_shared_indices(values: np.ndarray, indices: np.ndarray) -> np.ndarray:
    matrix = finite_float64(values, name="values", ndim=2)
    if not isinstance(indices, np.ndarray) or indices.dtype != np.dtype(np.int64):
        raise ResearchContractError("indices must be an int64 numpy array")
    if indices.ndim != 2 or indices.shape[1] != matrix.shape[0]:
        raise ResearchContractError("indices must have shape (B,T)")
    if indices.size == 0 or bool(np.any((indices < 0) | (indices >= len(matrix)))):
        raise ResearchContractError("indices contain an invalid observation")
    return matrix[indices, :]
