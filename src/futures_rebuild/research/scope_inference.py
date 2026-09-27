"""Joint common-session inference for a predeclared market scope."""

from __future__ import annotations

import numpy as np

from ..canonical import sha256_json
from .contracts import (
    ResearchContractError,
    array_sha256,
    explicit_real,
    finite_float64,
    require_unique_ascii_ids,
)
from .multiple_testing import romano_wolf_from_differentials


def joint_scope_test(
    differentials: np.ndarray,
    *,
    session_ids: tuple[str, ...],
    market_ids: tuple[str, ...],
    required_positive_markets: tuple[str, ...],
    hac_lag: int,
    mean_block_length: float,
    n_resamples: int,
    seed: int,
    alpha: float,
    minimum_effect: float,
) -> dict[str, object]:
    """Test all markets from one aligned matrix and one synchronized row resample."""

    values = finite_float64(differentials, name="scope_differentials", ndim=2)
    sessions = require_unique_ascii_ids(session_ids, name="common_session_ids")
    markets = require_unique_ascii_ids(market_ids, name="market_ids")
    required = require_unique_ascii_ids(
        required_positive_markets, name="required_positive_markets"
    )
    checked_alpha = explicit_real(alpha, name="alpha")
    hurdle = explicit_real(minimum_effect, name="minimum_effect")
    if values.shape != (len(sessions), len(markets)):
        raise ResearchContractError(
            "scope matrix must contain every market for every common session"
        )
    if tuple(sorted(sessions)) != sessions:
        raise ResearchContractError("common sessions must be chronological")
    if not set(required) <= set(markets):
        raise ResearchContractError("required-positive markets leave the tested scope")
    if not 0.0 < checked_alpha < 0.5 or hurdle < 0.0:
        raise ResearchContractError("scope alpha or minimum effect is invalid")
    result = romano_wolf_from_differentials(
        values,
        hypothesis_ids=markets,
        hac_lag=hac_lag,
        mean_block_length=mean_block_length,
        n_resamples=n_resamples,
        seed=seed,
        minimum_resamples=n_resamples,
    )
    means = np.mean(values, axis=0, dtype=np.float64)
    adjusted = {
        market: float(result.adjusted_p_values[index])
        for index, market in enumerate(markets)
    }
    market_means = {
        market: float(means[index]) for index, market in enumerate(markets)
    }
    passed = all(
        market_means[market] > hurdle and adjusted[market] <= checked_alpha
        for market in required
    )
    core: dict[str, object] = {
        "schema_version": "joint_scope_inference/1.0.0",
        "session_ids": list(sessions),
        "market_ids": list(markets),
        "required_positive_markets": list(required),
        "differentials_sha256": array_sha256(values),
        "resampling_unit": "COMMON_COMPLETE_EXCHANGE_SESSION",
        "same_date_dependence_preserved": True,
        "market_count_is_independence_count": False,
        "mean_differentials": market_means,
        "romano_wolf_adjusted_p": adjusted,
        "alpha": checked_alpha,
        "minimum_effect": hurdle,
        "passed": passed,
        "state": "PASS" if passed else "FAIL_PROMOTION",
        "authority": {
            "new_trial": False,
            "holdout": False,
            "activation": False,
        },
    }
    return {**core, "scope_test_id": sha256_json(core)}
