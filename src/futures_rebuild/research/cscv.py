"""Exhaustive equal-block CSCV/PBO for causally produced futures paths."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from math import comb

import numpy as np

from .contracts import (
    ResearchContractError,
    explicit_int,
    explicit_real,
    finite_float64,
    require_unique_ascii_ids,
)


@dataclass(frozen=True)
class CSCVResult:
    strategy_ids: tuple[str, ...]
    blocks: int
    combinations: int
    selected_strategy_indices: np.ndarray
    oos_rank_logits: np.ndarray
    pbo_strict: float
    pbo_conservative: float
    metric: str
    status: str


def _score(values: np.ndarray, *, metric: str) -> np.ndarray:
    try:
        with np.errstate(over="raise", invalid="raise", divide="raise"):
            means = np.mean(values, axis=0, dtype=np.float64)
            deviations = None if metric == "mean" else np.std(values, axis=0, ddof=1)
    except FloatingPointError as error:
        raise ResearchContractError("CSCV score arithmetic overflowed") from error
    if bool(np.any(~np.isfinite(means))):
        raise ResearchContractError("CSCV mean is non-finite")
    if metric == "mean":
        return means
    assert deviations is not None
    if bool(np.any(~np.isfinite(deviations))) or bool(np.any(deviations <= 0.0)):
        raise ResearchContractError("CSCV Sharpe slice is degenerate")
    return means / deviations


def exhaustive_cscv_pbo(
    strategy_returns: np.ndarray,
    *,
    strategy_ids: tuple[str, ...],
    blocks: int,
    metric: str = "mean",
    maximum_combinations: int = 200_000,
    tie_tolerance: float = 0.0,
) -> CSCVResult:
    values = finite_float64(strategy_returns, name="strategy_returns", ndim=2)
    ids = require_unique_ascii_ids(strategy_ids, name="strategy_ids")
    block_count = explicit_int(blocks, name="blocks")
    maximum = explicit_int(maximum_combinations, name="maximum_combinations")
    tolerance = explicit_real(tie_tolerance, name="tie_tolerance")
    if values.shape[1] != len(ids) or len(ids) < 2:
        raise ResearchContractError("CSCV needs at least two aligned strategies")
    if block_count < 4 or block_count % 2:
        raise ResearchContractError("blocks must be even and at least four")
    if values.shape[0] % block_count:
        raise ResearchContractError("observations must divide into equal blocks")
    if metric not in {"mean", "sharpe"} or tolerance < 0.0:
        raise ResearchContractError("CSCV metric/tie settings are invalid")
    combination_count = comb(block_count, block_count // 2)
    if maximum < 1 or combination_count > maximum:
        raise ResearchContractError("exhaustive CSCV exceeds combination cap")
    block_indices = np.arange(values.shape[0], dtype=np.int64).reshape(block_count, -1)
    all_blocks = frozenset(range(block_count))
    winners: list[int] = []
    logits: list[float] = []
    for in_tuple in combinations(range(block_count), block_count // 2):
        out_blocks = sorted(all_blocks.difference(in_tuple))
        in_rows = block_indices[list(in_tuple), :].reshape(-1)
        out_rows = block_indices[out_blocks, :].reshape(-1)
        in_scores = _score(values[in_rows, :], metric=metric)
        tied = np.flatnonzero(np.abs(in_scores - np.max(in_scores)) <= tolerance)
        if len(tied) != 1:
            raise ResearchContractError("CSCV in-sample winner is tied")
        winner = int(tied[0])
        out_scores = _score(values[out_rows, :], metric=metric)
        selected = out_scores[winner]
        if len(np.flatnonzero(np.abs(out_scores - selected) <= tolerance)) != 1:
            raise ResearchContractError("CSCV selected OOS rank is tied")
        rank = 1 + int(np.count_nonzero(out_scores < selected))
        omega = rank / (len(ids) + 1.0)
        winners.append(winner)
        logits.append(float(np.log(omega / (1.0 - omega))))
    winner_array = np.asarray(winners, dtype=np.int64)
    logit_array = np.asarray(logits, dtype=np.float64)
    return CSCVResult(
        ids,
        block_count,
        combination_count,
        winner_array,
        logit_array,
        float(np.mean(logit_array < 0.0)),
        float(np.mean(logit_array <= 0.0)),
        metric,
        "MECHANICS_ONLY",
    )
