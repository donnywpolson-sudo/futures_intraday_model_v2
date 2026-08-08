"""Pure two-target Ridge fitting and fold-local signal selection.

The caller supplies an already chronological split.  In particular, the test
rows never influence model coefficients or the neutral threshold.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np

from .errors import IntegrityError
from .tier1_bracket_directional import bounded_direction_score, select_direction, training_only_neutral_threshold


FEATURE_NAMES = ("bar_body_fraction", "bar_return", "intrabar_range_fraction", "volume")


@dataclass(frozen=True)
class DirectionalTrainingRow:
    features: tuple[float, float, float, float]
    long_net_r: float
    short_net_r: float

    def validate(self) -> None:
        values = (*self.features, self.long_net_r, self.short_net_r)
        if not all(np.isfinite(value) for value in values):
            raise IntegrityError("directional Ridge rows must be finite")


@dataclass(frozen=True)
class DirectionalRidgeModel:
    long_coefficients: tuple[float, ...]
    short_coefficients: tuple[float, ...]
    ridge_penalty: float = 1.0


@dataclass(frozen=True)
class DirectionalPrediction:
    long_prediction_net_r: float
    short_prediction_net_r: float
    bounded_signal: float | None
    neutral_threshold: float
    selected_direction: str


def _matrix(rows: Sequence[DirectionalTrainingRow]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if not rows:
        raise IntegrityError("directional Ridge needs at least one training row")
    for row in rows:
        row.validate()
    x = np.asarray([[1.0, *row.features] for row in rows], dtype=np.float64)
    return x, np.asarray([row.long_net_r for row in rows]), np.asarray([row.short_net_r for row in rows])


def fit_directional_ridge(*, training_rows: Sequence[DirectionalTrainingRow]) -> DirectionalRidgeModel:
    """Fit exactly two deterministic Ridge models using training rows only."""

    x, long_y, short_y = _matrix(training_rows)
    penalty = np.eye(x.shape[1], dtype=np.float64)
    penalty[0, 0] = 0.0  # Do not penalize the intercept.
    system = x.T @ x + penalty
    try:
        long_coefficients = np.linalg.solve(system, x.T @ long_y)
        short_coefficients = np.linalg.solve(system, x.T @ short_y)
    except np.linalg.LinAlgError as exc:
        raise IntegrityError("directional Ridge system is singular") from exc
    return DirectionalRidgeModel(tuple(long_coefficients.tolist()), tuple(short_coefficients.tolist()))


def _predict(model: DirectionalRidgeModel, row: DirectionalTrainingRow) -> tuple[float, float]:
    row.validate()
    x = np.asarray([1.0, *row.features], dtype=np.float64)
    long = float(x @ np.asarray(model.long_coefficients))
    short = float(x @ np.asarray(model.short_coefficients))
    if not np.isfinite(long) or not np.isfinite(short):
        raise IntegrityError("directional Ridge produced non-finite prediction")
    return long, short


def predict_fold(
    *, model: DirectionalRidgeModel, training_rows: Sequence[DirectionalTrainingRow], test_rows: Sequence[DirectionalTrainingRow],
) -> tuple[DirectionalPrediction, ...]:
    """Apply a 60th-percentile threshold fitted exclusively from training scores."""

    training_scores = []
    for row in training_rows:
        long, short = _predict(model, row)
        score = bounded_direction_score(long_prediction_net_r=long, short_prediction_net_r=short)
        if score is None:
            raise IntegrityError("training score is unexpectedly invalid")
        training_scores.append(score)
    threshold = training_only_neutral_threshold(training_scores=training_scores)
    predictions = []
    for row in test_rows:
        long, short = _predict(model, row)
        score = bounded_direction_score(long_prediction_net_r=long, short_prediction_net_r=short)
        predictions.append(DirectionalPrediction(
            long_prediction_net_r=long,
            short_prediction_net_r=short,
            bounded_signal=score,
            neutral_threshold=threshold,
            selected_direction=select_direction(score=score, threshold=threshold),
        ))
    return tuple(predictions)
