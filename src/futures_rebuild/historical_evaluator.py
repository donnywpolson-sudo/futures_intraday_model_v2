"""Fit-free evaluator for frozen synthetic outer-fold predictions."""

from __future__ import annotations

import math

import numpy as np

from .canonical import sha256_json
from .historical_engine_contracts import (
    BuilderFoldResult,
    EvaluatorFoldPacket,
    FrozenEvaluation,
)
from .research.contracts import ResearchContractError
from .schemas import OutcomeStatus


def _verified_packet(packet: EvaluatorFoldPacket) -> EvaluatorFoldPacket:
    if not isinstance(packet, EvaluatorFoldPacket):
        raise ResearchContractError("evaluator requires an exact splitter packet")
    return EvaluatorFoldPacket(
        fold_id=packet.fold_id,
        split_schedule_id=packet.split_schedule_id,
        audit_sample_ids=packet.audit_sample_ids,
        audit_labels=packet.audit_labels,
        outcome_statuses=packet.outcome_statuses,
        audit_decision_sessions=packet.audit_decision_sessions,
        audit_label_unlock_sessions=packet.audit_label_unlock_sessions,
        packet_id=packet.packet_id,
    )


def evaluate_frozen_outer_fold(
    packet: EvaluatorFoldPacket,
    built: BuilderFoldResult,
) -> FrozenEvaluation:
    """Score immutable predictions without training-module access or mutation."""

    packet = _verified_packet(packet)
    if not isinstance(built, BuilderFoldResult):
        raise ResearchContractError("evaluator requires a frozen builder result")
    built.artifact.validate()
    built.predictions.validate()
    predictions = built.predictions
    if (
        packet.fold_id != built.artifact.fold_id
        or predictions.fold_id != packet.fold_id
        or predictions.artifact_id != built.artifact.artifact_id
        or predictions.artifact_sha256 != built.artifact.artifact_sha256
        or predictions.audit_sample_ids != packet.audit_sample_ids
        or predictions.prediction_sessions
        != tuple(int(item) for item in packet.audit_decision_sessions)
        or predictions.label_unlock_sessions
        != tuple(int(item) for item in packet.audit_label_unlock_sessions)
    ):
        raise ResearchContractError("frozen prediction/evaluator bindings differ")
    resolved = np.asarray(
        [item == OutcomeStatus.MATURED.value for item in packet.outcome_statuses],
        dtype=np.bool_,
    )
    denominator_count = len(packet.audit_sample_ids)
    resolved_count = int(np.count_nonzero(resolved))
    unresolved_count = denominator_count - resolved_count
    mse: float | None = None
    baseline_mse: float | None = None
    correlation: float | None = None
    directional_accuracy: float | None = None
    if resolved_count:
        labels = packet.audit_labels[resolved]
        forecasts = np.asarray(predictions.expected_returns, dtype=np.float64)[resolved]
        baselines = np.asarray(
            predictions.baseline_expected_returns, dtype=np.float64
        )[resolved]
        errors = labels - forecasts
        baseline_errors = labels - baselines
        mse = float(np.mean(errors * errors, dtype=np.float64))
        baseline_mse = float(
            np.mean(baseline_errors * baseline_errors, dtype=np.float64)
        )
        directional_accuracy = float(np.mean(np.sign(labels) == np.sign(forecasts)))
        if (
            resolved_count >= 2
            and float(np.std(labels)) > 0.0
            and float(np.std(forecasts)) > 0.0
        ):
            correlation = float(np.corrcoef(labels, forecasts)[0, 1])
        if any(
            not math.isfinite(value)
            for value in (mse, baseline_mse, directional_accuracy)
        ) or (correlation is not None and not math.isfinite(correlation)):
            raise ResearchContractError("evaluator produced a nonfinite metric")
    if resolved_count == 0:
        state = "INCONCLUSIVE_SYNTHETIC_OUTCOME_COVERAGE"
    elif (
        mse is not None
        and baseline_mse is not None
        and mse < baseline_mse
        and correlation is not None
        and correlation > 0.0
    ):
        state = "SYNTHETIC_SIGNAL_RECOVERED"
    else:
        state = "SYNTHETIC_NOISE_OR_NO_EDGE"
    core = {
        "alpha_evidence": False,
        "artifact_id": built.artifact.artifact_id,
        "baseline_mean_squared_error": baseline_mse,
        "candidate_eligible": False,
        "correlation": correlation,
        "denominator_count": denominator_count,
        "directional_accuracy": directional_accuracy,
        "evaluator_packet_id": packet.packet_id,
        "fold_id": packet.fold_id,
        "mean_squared_error": mse,
        "mechanics_state": state,
        "prediction_manifest_id": predictions.manifest_id,
        "resolved_count": resolved_count,
        "unresolved_count": unresolved_count,
    }
    result = FrozenEvaluation(
        fold_id=packet.fold_id,
        evaluator_packet_id=packet.packet_id,
        artifact_id=built.artifact.artifact_id,
        prediction_manifest_id=predictions.manifest_id,
        denominator_count=denominator_count,
        resolved_count=resolved_count,
        unresolved_count=unresolved_count,
        mean_squared_error=mse,
        baseline_mean_squared_error=baseline_mse,
        correlation=correlation,
        directional_accuracy=directional_accuracy,
        mechanics_state=state,
        alpha_evidence=False,
        candidate_eligible=False,
        evaluation_id=sha256_json(core),
    )
    result.validate()
    return result


def evaluate_frozen_research_run(
    packets: tuple[EvaluatorFoldPacket, ...],
    built: tuple[BuilderFoldResult, ...],
) -> tuple[FrozenEvaluation, ...]:
    if (
        type(packets) is not tuple
        or type(built) is not tuple
        or not packets
        or len(packets) != len(built)
    ):
        raise ResearchContractError("evaluator run inputs are incomplete")
    return tuple(
        evaluate_frozen_outer_fold(packet, result)
        for packet, result in zip(packets, built, strict=True)
    )
