"""Independent market/direction gates with no cross-market subsidy."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib

import numpy as np

from .contracts import (
    ResearchContractError,
    SyntheticOnlyPermit,
    explicit_real,
    require_synthetic_permit,
)
from .economics import Direction, _market_id


class SleeveState(str, Enum):
    MECHANICS_READY = "MECHANICS_READY"
    MECHANICS_FAIL_CLOSED = "MECHANICS_FAIL_CLOSED"


class ResearchBookState(str, Enum):
    MECHANICS_READY = "MECHANICS_READY"
    MECHANICS_FAIL_CLOSED = "MECHANICS_FAIL_CLOSED"


@dataclass(frozen=True, order=True)
class SleeveKey:
    market_id: str
    direction: Direction

    def validate(self) -> None:
        _market_id(self.market_id)
        if not isinstance(self.direction, Direction):
            raise ResearchContractError("sleeve direction must be LONG or SHORT")

    @property
    def sleeve_id(self) -> str:
        self.validate()
        return f"{self.market_id}|{self.direction.value}"


@dataclass(frozen=True)
class SleeveThresholds:
    alpha: float
    dsr_probability_minimum: float
    pbo_conservative_maximum: float

    def validate(self) -> None:
        alpha = explicit_real(self.alpha, name="alpha")
        dsr = explicit_real(
            self.dsr_probability_minimum, name="dsr_probability_minimum"
        )
        pbo = explicit_real(
            self.pbo_conservative_maximum, name="pbo_conservative_maximum"
        )
        if not (0.0 < alpha < 1.0 and 0.0 < dsr < 1.0 and 0.0 <= pbo < 1.0):
            raise ResearchContractError("sleeve thresholds are outside valid ranges")


@dataclass(frozen=True)
class SyntheticSleeveMetrics:
    mean_after_costs: float
    confidence_lower_bound: float
    minimum_economically_effective_mean: float
    romano_wolf_adjusted_p: float
    dsr_probability: float
    pbo_conservative: float
    power_sufficient: bool
    negative_controls_clear: bool
    numerically_valid: bool


@dataclass(frozen=True)
class SleeveGateResult:
    key: SleeveKey
    state: SleeveState
    failed_gates: tuple[str, ...]
    mechanics_only: bool = True


@dataclass(frozen=True)
class ResearchBookCharter:
    registered_sleeves: tuple[SleeveKey, ...]
    included_sleeves: tuple[SleeveKey, ...]
    charter_hash: str

    @classmethod
    def create(
        cls,
        *,
        registered_sleeves: tuple[SleeveKey, ...],
        included_sleeves: tuple[SleeveKey, ...],
    ) -> "ResearchBookCharter":
        registered_ids = _unique_sleeve_ids(registered_sleeves, name="registered")
        included_ids = _unique_sleeve_ids(included_sleeves, name="included")
        if not set(included_ids).issubset(set(registered_ids)):
            raise ResearchContractError("included sleeves must be preregistered")
        payload = ("\0".join(registered_ids) + "\1" + "\0".join(included_ids)).encode(
            "ascii"
        )
        return cls(
            registered_sleeves,
            included_sleeves,
            hashlib.sha256(payload).hexdigest(),
        )

    def validate(self) -> None:
        rebuilt = ResearchBookCharter.create(
            registered_sleeves=self.registered_sleeves,
            included_sleeves=self.included_sleeves,
        )
        if rebuilt.charter_hash != self.charter_hash:
            raise ResearchContractError("research-book charter hash is invalid")


def _unique_sleeve_ids(
    keys: tuple[SleeveKey, ...], *, name: str
) -> tuple[str, ...]:
    if not keys:
        raise ResearchContractError(f"{name} sleeves must be non-empty")
    ids = tuple(key.sleeve_id for key in keys)
    if len(set(ids)) != len(ids):
        raise ResearchContractError(f"{name} sleeves must be unique")
    return ids


def evaluate_synthetic_sleeve(
    *,
    key: SleeveKey,
    metrics: SyntheticSleeveMetrics,
    thresholds: SleeveThresholds,
    permit: SyntheticOnlyPermit,
    fixture: np.ndarray,
) -> SleeveGateResult:
    key.validate()
    require_synthetic_permit(permit, fixture)
    thresholds.validate()
    numeric_names = (
        "mean_after_costs",
        "confidence_lower_bound",
        "minimum_economically_effective_mean",
        "romano_wolf_adjusted_p",
        "dsr_probability",
        "pbo_conservative",
    )
    for name in numeric_names:
        value = getattr(metrics, name)
        if isinstance(value, (bool, np.bool_)) or not isinstance(
            value, (float, np.floating)
        ):
            raise ResearchContractError(f"{name} must be an explicit real float")
        if not np.isfinite(value):
            raise ResearchContractError(f"{name} must be finite")
    for name in ("power_sufficient", "negative_controls_clear", "numerically_valid"):
        if type(getattr(metrics, name)) is not bool:
            raise ResearchContractError(f"{name} must be an exact bool")
    failed: list[str] = []
    if not metrics.numerically_valid:
        failed.append("NUMERICAL_VALIDITY")
    if metrics.minimum_economically_effective_mean <= 0.0:
        failed.append("MEES_STRICTLY_POSITIVE")
    if metrics.mean_after_costs <= metrics.minimum_economically_effective_mean:
        failed.append("MEAN_AFTER_COSTS")
    if metrics.confidence_lower_bound <= metrics.minimum_economically_effective_mean:
        failed.append("CONFIDENCE_LOWER_BOUND")
    if not (0.0 <= metrics.romano_wolf_adjusted_p <= thresholds.alpha):
        failed.append("ROMANO_WOLF")
    if not (
        thresholds.dsr_probability_minimum <= metrics.dsr_probability <= 1.0
    ):
        failed.append("DEFLATED_SHARPE")
    if not (0.0 <= metrics.pbo_conservative <= thresholds.pbo_conservative_maximum):
        failed.append("PBO")
    if metrics.power_sufficient is not True:
        failed.append("POWER")
    if metrics.negative_controls_clear is not True:
        failed.append("NEGATIVE_CONTROLS")
    return SleeveGateResult(
        key,
        SleeveState.MECHANICS_READY if not failed else SleeveState.MECHANICS_FAIL_CLOSED,
        tuple(failed),
    )


def evaluate_research_book(
    charter: ResearchBookCharter,
    results: tuple[SleeveGateResult, ...],
) -> ResearchBookState:
    charter.validate()
    registered_ids = _unique_sleeve_ids(charter.registered_sleeves, name="registered")
    result_ids = tuple(result.key.sleeve_id for result in results)
    if len(set(result_ids)) != len(result_ids) or set(result_ids) != set(registered_ids):
        raise ResearchContractError("results must cover every registered market/direction")
    if any(result.mechanics_only is not True for result in results):
        raise ResearchContractError("book accepts mechanics-only sleeve results")
    by_id = {result.key.sleeve_id: result for result in results}
    included_ids = _unique_sleeve_ids(charter.included_sleeves, name="included")
    if all(by_id[sleeve_id].state is SleeveState.MECHANICS_READY for sleeve_id in included_ids):
        return ResearchBookState.MECHANICS_READY
    return ResearchBookState.MECHANICS_FAIL_CLOSED
