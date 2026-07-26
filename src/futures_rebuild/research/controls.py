"""Deterministic negative controls for futures leakage detection."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import numpy as np

from .contracts import (
    ResearchContractError,
    explicit_int,
    finite_float64,
    require_unique_ascii_ids,
)


class NegativeControlState(str, Enum):
    CLEAR = "CLEAR"
    LEAKAGE_SUSPECTED = "LEAKAGE_SUSPECTED"
    INVALID = "INVALID"


@dataclass(frozen=True)
class NegativeControlOutcome:
    control_id: str
    complete: bool
    candidate_gate_passed: bool


@dataclass(frozen=True)
class NegativeControlResult:
    state: NegativeControlState
    control_ids: tuple[str, ...]
    suspicious_controls: tuple[str, ...]
    incomplete_controls: tuple[str, ...]


def circular_block_derangement_indices(
    *,
    n_observations: int,
    block_size: int,
    seed: int,
) -> np.ndarray:
    n = explicit_int(n_observations, name="n_observations")
    size = explicit_int(block_size, name="block_size")
    checked_seed = explicit_int(seed, name="seed")
    if n < 2 or size < 1 or n % size:
        raise ResearchContractError("block_size must divide at least two observations")
    blocks = n // size
    if blocks < 2 or not (0 <= checked_seed < 2**64):
        raise ResearchContractError("negative-control block/seed settings are invalid")
    generator = np.random.Generator(np.random.PCG64(checked_seed))
    offset = int(generator.integers(1, blocks))
    matrix = np.arange(n, dtype=np.int64).reshape(blocks, size)
    return np.roll(matrix, shift=offset, axis=0).reshape(-1)


def apply_negative_control_indices(values: np.ndarray, indices: np.ndarray) -> np.ndarray:
    matrix = finite_float64(values, name="values")
    if matrix.ndim not in {1, 2}:
        raise ResearchContractError("control values must be 1D or 2D")
    if not isinstance(indices, np.ndarray) or indices.dtype != np.dtype(np.int64):
        raise ResearchContractError("indices must be int64")
    if indices.ndim != 1 or len(indices) != len(matrix):
        raise ResearchContractError("indices must map every observation")
    if set(indices.tolist()) != set(range(len(matrix))):
        raise ResearchContractError("indices must be a complete permutation")
    return matrix[indices, ...]


def synthetic_noise_control(*, shape: tuple[int, ...], seed: int) -> np.ndarray:
    if not shape or any(
        isinstance(size, (bool, np.bool_))
        or not isinstance(size, (int, np.integer))
        or int(size) < 1
        for size in shape
    ):
        raise ResearchContractError("shape must contain positive integers")
    checked_seed = explicit_int(seed, name="seed")
    if not (0 <= checked_seed < 2**64):
        raise ResearchContractError("seed must fit uint64")
    return np.random.Generator(np.random.PCG64(checked_seed)).standard_normal(
        tuple(int(size) for size in shape), dtype=np.float64
    )


def evaluate_negative_controls(
    outcomes: tuple[NegativeControlOutcome, ...],
) -> NegativeControlResult:
    if not outcomes:
        raise ResearchContractError("at least one negative control is required")
    ids = require_unique_ascii_ids(
        (outcome.control_id for outcome in outcomes), name="control_ids"
    )
    for outcome in outcomes:
        if type(outcome.complete) is not bool or type(
            outcome.candidate_gate_passed
        ) is not bool:
            raise ResearchContractError("control flags must be exact bool")
    incomplete = tuple(item.control_id for item in outcomes if not item.complete)
    suspicious = tuple(
        item.control_id
        for item in outcomes
        if item.complete and item.candidate_gate_passed
    )
    state = (
        NegativeControlState.INVALID
        if incomplete
        else NegativeControlState.LEAKAGE_SUSPECTED
        if suspicious
        else NegativeControlState.CLEAR
    )
    return NegativeControlResult(state, ids, suspicious, incomplete)
