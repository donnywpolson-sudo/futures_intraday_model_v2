"""Pure prospective-monitoring decisions with no fitting or external I/O."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math


class MonitoringContractError(ValueError):
    pass


class MonitoringState(str, Enum):
    MONITORING_PENDING = "MONITORING_PENDING"
    MONITORING_OK = "MONITORING_OK"
    MONITORING_WARNING = "MONITORING_WARNING"
    MONITORING_PAUSED = "MONITORING_PAUSED"
    MONITORING_INVALID = "MONITORING_INVALID"


@dataclass(frozen=True)
class MonitoringPolicy:
    minimum_distinct_dates: int = 30
    minimum_predictions: int = 500
    psi_warning: float = 0.10
    psi_pause: float = 0.25
    missingness_warning_delta: float = 0.05
    missingness_pause_delta: float = 0.10
    coverage_warning_ratio: float = 0.95
    coverage_pause_ratio: float = 0.90
    matured_score_pause_degradation: float = 0.10

    def validate(self) -> None:
        if self.minimum_distinct_dates != 30 or self.minimum_predictions != 500:
            raise MonitoringContractError("global monitoring window must remain 30 dates and 500 predictions")
        expected = (0.10, 0.25, 0.05, 0.10, 0.95, 0.90, 0.10)
        actual = (
            self.psi_warning,
            self.psi_pause,
            self.missingness_warning_delta,
            self.missingness_pause_delta,
            self.coverage_warning_ratio,
            self.coverage_pause_ratio,
            self.matured_score_pause_degradation,
        )
        if any(type(value) is not float or not math.isfinite(value) for value in actual):
            raise MonitoringContractError("monitoring thresholds must be finite explicit floats")
        if actual != expected:
            raise MonitoringContractError("monitoring thresholds differ from the frozen balanced policy")


@dataclass(frozen=True)
class MonitoringObservation:
    distinct_dates: int
    eligible_predictions: int
    maximum_psi: float
    maximum_missingness_delta: float
    coverage: float
    sealed_coverage_floor: float
    matured_score_relative_degradation: float | None
    source_or_bundle_stale: bool = False


@dataclass(frozen=True)
class MonitoringDecision:
    state: MonitoringState
    reasons: tuple[str, ...]
    mechanics_only: bool = True

    @property
    def requires_abstention(self) -> bool:
        return self.state in {
            MonitoringState.MONITORING_PAUSED,
            MonitoringState.MONITORING_INVALID,
        }


def assess_monitoring(
    observation: MonitoringObservation, policy: MonitoringPolicy = MonitoringPolicy()
) -> MonitoringDecision:
    policy.validate()
    if type(observation.distinct_dates) is not int or observation.distinct_dates < 0:
        return MonitoringDecision(MonitoringState.MONITORING_INVALID, ("INVALID_DATE_COUNT",))
    if type(observation.eligible_predictions) is not int or observation.eligible_predictions < 0:
        return MonitoringDecision(MonitoringState.MONITORING_INVALID, ("INVALID_PREDICTION_COUNT",))
    numeric = (
        observation.maximum_psi,
        observation.maximum_missingness_delta,
        observation.coverage,
        observation.sealed_coverage_floor,
    )
    if any(type(value) is not float or not math.isfinite(value) for value in numeric):
        return MonitoringDecision(MonitoringState.MONITORING_INVALID, ("INVALID_NUMERIC_EVIDENCE",))
    if observation.matured_score_relative_degradation is not None and (
        type(observation.matured_score_relative_degradation) is not float
        or not math.isfinite(observation.matured_score_relative_degradation)
    ):
        return MonitoringDecision(MonitoringState.MONITORING_INVALID, ("INVALID_MATURED_SCORE",))
    if not (0.0 <= observation.coverage <= 1.0 and 0.0 < observation.sealed_coverage_floor <= 1.0):
        return MonitoringDecision(MonitoringState.MONITORING_INVALID, ("INVALID_COVERAGE",))
    if type(observation.source_or_bundle_stale) is not bool:
        return MonitoringDecision(MonitoringState.MONITORING_INVALID, ("INVALID_STALENESS_FLAG",))
    if (
        observation.distinct_dates < policy.minimum_distinct_dates
        or observation.eligible_predictions < policy.minimum_predictions
    ):
        return MonitoringDecision(MonitoringState.MONITORING_PENDING, ("MINIMUM_WINDOW_PENDING",))
    pause: list[str] = []
    warning: list[str] = []
    coverage_ratio = observation.coverage / observation.sealed_coverage_floor
    if observation.source_or_bundle_stale:
        pause.append("SOURCE_OR_BUNDLE_STALE")
    if observation.maximum_psi >= policy.psi_pause:
        pause.append("PSI_PAUSE")
    elif observation.maximum_psi >= policy.psi_warning:
        warning.append("PSI_WARNING")
    if observation.maximum_missingness_delta >= policy.missingness_pause_delta:
        pause.append("MISSINGNESS_PAUSE")
    elif observation.maximum_missingness_delta >= policy.missingness_warning_delta:
        warning.append("MISSINGNESS_WARNING")
    if coverage_ratio < policy.coverage_pause_ratio:
        pause.append("COVERAGE_PAUSE")
    elif coverage_ratio < policy.coverage_warning_ratio:
        warning.append("COVERAGE_WARNING")
    if (
        observation.matured_score_relative_degradation is not None
        and observation.matured_score_relative_degradation
        >= policy.matured_score_pause_degradation
    ):
        pause.append("MATURED_SCORE_PAUSE")
    if pause:
        return MonitoringDecision(MonitoringState.MONITORING_PAUSED, tuple(pause))
    if warning:
        return MonitoringDecision(MonitoringState.MONITORING_WARNING, tuple(warning))
    return MonitoringDecision(MonitoringState.MONITORING_OK, ())
