from __future__ import annotations

import numpy as np
import pytest

from futures_rebuild.monitoring import (
    MonitoringObservation,
    MonitoringState,
    assess_monitoring,
)
from futures_rebuild.research import (
    FoldEffect,
    RobustnessState,
    StabilityPolicy,
    TemporalConcentrationPolicy,
    VariantEffect,
    deterministic_stability_seeds,
    evaluate_temporal_concentration,
    evaluate_variant_stability,
    make_synthetic_permit,
    verify_deterministic_repeat,
)


@pytest.fixture
def mechanics() -> tuple[np.ndarray, object]:
    fixture = np.asarray([1.0, 2.0, 3.0], dtype=np.float64)
    return fixture, make_synthetic_permit(fixture, generator_id="robustness-fixture", seed=7)


def _folds(values: tuple[float, ...]) -> tuple[FoldEffect, ...]:
    return tuple(FoldEffect(f"fold-{index}", 63, float(value)) for index, value in enumerate(values))


def test_temporal_concentration_requires_broad_positive_support(mechanics) -> None:
    fixture, permit = mechanics
    passed = evaluate_temporal_concentration(
        folds=_folds((0.10, 0.09, 0.08, 0.07, 0.06, -0.01, -0.01, -0.01)),
        policy=TemporalConcentrationPolicy(),
        permit=permit,
        fixture=fixture,
    )
    assert passed.state is RobustnessState.MECHANICS_READY
    assert passed.positive_folds == passed.required_positive_folds == 5
    assert min(passed.leave_one_out_effects) > 0.0

    concentrated = evaluate_temporal_concentration(
        folds=_folds((0.50, 0.01, 0.01, 0.01, -0.10, -0.10, -0.10, -0.10)),
        policy=TemporalConcentrationPolicy(),
        permit=permit,
        fixture=fixture,
    )
    assert concentrated.state is RobustnessState.MECHANICS_INCONCLUSIVE
    assert "INSUFFICIENT_POSITIVE_FOLDS" in concentrated.reasons


def test_stability_seeds_variants_and_repeat_are_frozen(mechanics) -> None:
    fixture, permit = mechanics
    trial_id = "a" * 64
    assert deterministic_stability_seeds(trial_id) == deterministic_stability_seeds(trial_id)
    assert len(set(deterministic_stability_seeds(trial_id))) == 5
    assert verify_deterministic_repeat("1" * 64, "1" * 64)
    assert not verify_deterministic_repeat("1" * 64, "2" * 64)

    passed = evaluate_variant_stability(
        base_effect=1.0,
        variants=tuple(
            VariantEffect(f"variant-{index}", value)
            for index, value in enumerate((0.8, 0.7, 0.6, 0.5, -0.1))
        ),
        policy=StabilityPolicy(),
        permit=permit,
        fixture=fixture,
    )
    assert passed.state is RobustnessState.MECHANICS_READY
    assert passed.positive_variants == passed.required_positive_variants == 4
    assert passed.median_retention == pytest.approx(0.6)

    unstable = evaluate_variant_stability(
        base_effect=1.0,
        variants=tuple(
            VariantEffect(f"variant-{index}", value)
            for index, value in enumerate((0.6, 0.5, 0.4, -0.1, -0.2))
        ),
        policy=StabilityPolicy(),
        permit=permit,
        fixture=fixture,
    )
    assert unstable.state is RobustnessState.MECHANICS_INCONCLUSIVE
    assert "INSUFFICIENT_POSITIVE_VARIANTS" in unstable.reasons
    assert "MEDIAN_EFFECT_RETENTION" in unstable.reasons


def test_monitoring_precedence_and_threshold_boundaries() -> None:
    pending = assess_monitoring(MonitoringObservation(29, 500, 0.0, 0.0, 1.0, 1.0, None))
    assert pending.state is MonitoringState.MONITORING_PENDING

    warning = assess_monitoring(MonitoringObservation(30, 500, 0.10, 0.05, 0.94, 1.0, None))
    assert warning.state is MonitoringState.MONITORING_WARNING
    assert not warning.requires_abstention

    paused = assess_monitoring(MonitoringObservation(30, 500, 0.25, 0.10, 0.89, 1.0, 0.10))
    assert paused.state is MonitoringState.MONITORING_PAUSED
    assert paused.requires_abstention
    assert set(paused.reasons) == {"PSI_PAUSE", "MISSINGNESS_PAUSE", "COVERAGE_PAUSE", "MATURED_SCORE_PAUSE"}

    invalid = assess_monitoring(MonitoringObservation(30, 500, float("nan"), 0.0, 1.0, 1.0, None))
    assert invalid.state is MonitoringState.MONITORING_INVALID
    assert invalid.requires_abstention
