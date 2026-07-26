"""Synthetic-only nested-selection builder for a ridge-type linear forecast.

The builder can see matured training/inner labels and unlabeled outer features.
It never receives outer labels and emits only content-addressed frozen mechanics
artifacts and predictions.  Nothing here seals a candidate.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass

import numpy as np

from .canonical import canonical_bytes, sha256_json
from .historical_engine_contracts import (
    BuilderFoldPacket,
    BuilderFoldResult,
    FitAuditRecord,
    FrozenLinearArtifact,
    FrozenPredictions,
    LinearCandidate,
)
from .predictor import ARTIFACT_FORMAT
from .research.contracts import ResearchContractError, array_sha256


@dataclass(frozen=True)
class _LinearFit:
    raw_intercept: float
    raw_weights: np.ndarray
    uncertainty: float
    audit_predictions: np.ndarray
    audit: FitAuditRecord


def _verified_packet(packet: BuilderFoldPacket) -> BuilderFoldPacket:
    if not isinstance(packet, BuilderFoldPacket):
        raise ResearchContractError("builder requires an exact splitter packet")
    return BuilderFoldPacket(
        fold_id=packet.fold_id,
        split_schedule_id=packet.split_schedule_id,
        feature_names=packet.feature_names,
        fit_sample_ids=packet.fit_sample_ids,
        unresolved_fit_sample_ids=packet.unresolved_fit_sample_ids,
        audit_sample_ids=packet.audit_sample_ids,
        fit_features=packet.fit_features,
        fit_labels=packet.fit_labels,
        audit_features=packet.audit_features,
        audit_decision_sessions=packet.audit_decision_sessions,
        audit_label_unlock_sessions=packet.audit_label_unlock_sessions,
        inner_packets=packet.inner_packets,
        permit=packet.permit,
        packet_id=packet.packet_id,
    )


def _candidate_indices(
    candidate: LinearCandidate, available: tuple[str, ...]
) -> np.ndarray:
    candidate.validate(available)
    lookup = {name: index for index, name in enumerate(available)}
    return np.asarray([lookup[name] for name in candidate.feature_names], dtype=np.int64)


def _audit_record(
    *,
    fold_id: str,
    stage: str,
    candidate_id: str,
    fit_ids: tuple[str, ...],
    audit_ids: tuple[str, ...],
    fit_x: np.ndarray,
    fit_y: np.ndarray,
    audit_x: np.ndarray,
    audit_y: np.ndarray | None,
    scaler_fit_id: str,
    model_fit_id: str,
) -> FitAuditRecord:
    core = {
        "audit_features_sha256": array_sha256(audit_x),
        "audit_labels_sha256": None if audit_y is None else array_sha256(audit_y),
        "audit_sample_ids": list(audit_ids),
        "candidate_id": candidate_id,
        "fit_features_sha256": array_sha256(fit_x),
        "fit_labels_sha256": array_sha256(fit_y),
        "fit_sample_ids": list(fit_ids),
        "fold_id": fold_id,
        "model_fit_id": model_fit_id,
        "scaler_fit_id": scaler_fit_id,
        "stage": stage,
    }
    result = FitAuditRecord(
        fold_id=fold_id,
        stage=stage,
        candidate_id=candidate_id,
        fit_sample_ids=fit_ids,
        audit_sample_ids=audit_ids,
        fit_features_sha256=core["fit_features_sha256"],
        fit_labels_sha256=core["fit_labels_sha256"],
        audit_features_sha256=core["audit_features_sha256"],
        audit_labels_sha256=core["audit_labels_sha256"],
        scaler_fit_id=scaler_fit_id,
        model_fit_id=model_fit_id,
        record_id=sha256_json(core),
    )
    result.validate()
    return result


def _ridge_linear_fit(
    *,
    fold_id: str,
    stage: str,
    candidate: LinearCandidate,
    fit_ids: tuple[str, ...],
    audit_ids: tuple[str, ...],
    fit_x: np.ndarray,
    fit_y: np.ndarray,
    audit_x: np.ndarray,
    audit_y: np.ndarray | None,
) -> _LinearFit:
    if fit_x.ndim != 2 or audit_x.ndim != 2 or fit_x.shape[1] != audit_x.shape[1]:
        raise ResearchContractError("linear fit matrices are incompatible")
    means = np.mean(fit_x, axis=0, dtype=np.float64)
    scales = np.std(fit_x, axis=0, dtype=np.float64)
    scales = np.where(scales > 0.0, scales, 1.0).astype(np.float64)
    standardized = (fit_x - means) / scales
    centered_y = fit_y - np.mean(fit_y, dtype=np.float64)
    gram = standardized.T @ standardized
    regularized = gram + float(candidate.ridge_penalty) * np.eye(
        fit_x.shape[1], dtype=np.float64
    )
    try:
        scaled_weights = np.linalg.solve(regularized, standardized.T @ centered_y)
    except np.linalg.LinAlgError as exc:
        raise ResearchContractError("ridge linear system is not solvable") from exc
    raw_weights = np.ascontiguousarray(scaled_weights / scales, dtype=np.float64)
    raw_intercept = float(
        np.mean(fit_y, dtype=np.float64) - np.dot(raw_weights, means)
    )
    train_predictions = raw_intercept + fit_x @ raw_weights
    residuals = fit_y - train_predictions
    uncertainty = float(np.sqrt(np.mean(residuals * residuals, dtype=np.float64)))
    audit_predictions = np.ascontiguousarray(
        raw_intercept + audit_x @ raw_weights, dtype=np.float64
    )
    if not bool(
        np.all(
            np.isfinite(
                np.concatenate(
                    (
                        means,
                        scales,
                        raw_weights,
                        np.asarray(
                            [raw_intercept, uncertainty], dtype=np.float64
                        ),
                        audit_predictions,
                    )
                )
            )
        )
    ):
        raise ResearchContractError("linear fit produced a nonfinite artifact")
    scaler_fit_id = sha256_json(
        {
            "feature_names": list(candidate.feature_names),
            "fit_features_sha256": array_sha256(fit_x),
            "fit_sample_ids": list(fit_ids),
            "means": means.tolist(),
            "scales": scales.tolist(),
            "scaler": "FOLD_LOCAL_MEAN_STD_V1",
        }
    )
    model_fit_id = sha256_json(
        {
            "candidate_id": candidate.candidate_id,
            "fit_labels_sha256": array_sha256(fit_y),
            "raw_intercept": raw_intercept,
            "raw_weights": raw_weights.tolist(),
            "ridge_penalty": float(candidate.ridge_penalty),
            "scaler_fit_id": scaler_fit_id,
            "uncertainty": uncertainty,
        }
    )
    audit = _audit_record(
        fold_id=fold_id,
        stage=stage,
        candidate_id=candidate.candidate_id,
        fit_ids=fit_ids,
        audit_ids=audit_ids,
        fit_x=fit_x,
        fit_y=fit_y,
        audit_x=audit_x,
        audit_y=audit_y,
        scaler_fit_id=scaler_fit_id,
        model_fit_id=model_fit_id,
    )
    return _LinearFit(
        raw_intercept,
        raw_weights,
        uncertainty,
        audit_predictions,
        audit,
    )


def _forecast_payload(
    score: float, *, uncertainty: float
) -> dict[str, float]:
    probability_up = 1.0 / (
        1.0 + math.exp(-max(-40.0, min(40.0, float(score))))
    )
    return {
        "expected_return": float(score),
        "probability_down": 1.0 - probability_up,
        "probability_neutral": 0.0,
        "probability_up": probability_up,
        "uncertainty": uncertainty,
    }


def build_synthetic_outer_fold(
    packet: BuilderFoldPacket,
    candidates: tuple[LinearCandidate, ...],
) -> BuilderFoldResult:
    """Select only on inner labels, then freeze one outer artifact/prediction set."""

    packet = _verified_packet(packet)
    if (
        type(candidates) is not tuple
        or not candidates
        or any(not isinstance(item, LinearCandidate) for item in candidates)
        or len({item.candidate_id for item in candidates}) != len(candidates)
    ):
        raise ResearchContractError("builder requires unique linear candidates")
    ordered = tuple(sorted(candidates, key=lambda item: item.candidate_id))
    candidate_columns = {
        item.candidate_id: _candidate_indices(item, packet.feature_names)
        for item in ordered
    }
    inner_scores: dict[str, list[float]] = {item.candidate_id: [] for item in ordered}
    inner_audits: list[FitAuditRecord] = []
    for candidate in ordered:
        columns = candidate_columns[candidate.candidate_id]
        for inner in packet.inner_packets:
            result = _ridge_linear_fit(
                fold_id=inner.inner_fold_id,
                stage="INNER_SELECTION",
                candidate=candidate,
                fit_ids=inner.fit_sample_ids,
                audit_ids=inner.audit_sample_ids,
                fit_x=inner.fit_features[:, columns],
                fit_y=inner.fit_labels,
                audit_x=inner.audit_features[:, columns],
                audit_y=inner.audit_labels,
            )
            errors = inner.audit_labels - result.audit_predictions
            score = float(np.mean(errors * errors, dtype=np.float64))
            if not math.isfinite(score):
                raise ResearchContractError("inner selection score is nonfinite")
            inner_scores[candidate.candidate_id].append(score)
            inner_audits.append(result.audit)
    mean_scores = {
        candidate_id: float(np.mean(values, dtype=np.float64))
        for candidate_id, values in inner_scores.items()
    }
    selected_id = min(mean_scores, key=lambda key: (mean_scores[key], key))
    selected = next(item for item in ordered if item.candidate_id == selected_id)
    columns = candidate_columns[selected_id]
    outer = _ridge_linear_fit(
        fold_id=packet.fold_id,
        stage="OUTER_REFIT",
        candidate=selected,
        fit_ids=packet.fit_sample_ids,
        audit_ids=packet.audit_sample_ids,
        fit_x=packet.fit_features[:, columns],
        fit_y=packet.fit_labels,
        audit_x=packet.audit_features[:, columns],
        audit_y=None,
    )
    parity_input = np.mean(packet.fit_features[:, columns], axis=0).tolist()
    parity_score = outer.raw_intercept + float(
        np.dot(outer.raw_weights, np.asarray(parity_input, dtype=np.float64))
    )
    artifact_payload = {
        "artifact_format": ARTIFACT_FORMAT,
        "expected_return_scale": 1.0,
        "feature_names": list(selected.feature_names),
        "intercept": outer.raw_intercept,
        "parity_input": parity_input,
        "parity_output": _forecast_payload(
            parity_score, uncertainty=outer.uncertainty
        ),
        "uncertainty": outer.uncertainty,
        "weights": outer.raw_weights.tolist(),
    }
    artifact_bytes = canonical_bytes(artifact_payload) + b"\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    artifact_core = {
        "artifact_sha256": artifact_sha256,
        "builder_packet_id": packet.packet_id,
        "fold_id": packet.fold_id,
        "outer_fit_audit_id": outer.audit.record_id,
        "selected_candidate_id": selected_id,
    }
    artifact = FrozenLinearArtifact(
        fold_id=packet.fold_id,
        builder_packet_id=packet.packet_id,
        selected_candidate_id=selected_id,
        outer_fit_audit_id=outer.audit.record_id,
        artifact_bytes=artifact_bytes,
        artifact_sha256=artifact_sha256,
        artifact_id=sha256_json(artifact_core),
    )
    artifact.validate()
    baseline = tuple(
        float(np.mean(packet.fit_labels, dtype=np.float64))
        for _ in packet.audit_sample_ids
    )
    prediction_core = {
        "artifact_id": artifact.artifact_id,
        "artifact_sha256": artifact.artifact_sha256,
        "audit_sample_ids": list(packet.audit_sample_ids),
        "baseline_expected_returns": list(baseline),
        "expected_returns": outer.audit_predictions.tolist(),
        "fold_id": packet.fold_id,
        "label_unlock_sessions": packet.audit_label_unlock_sessions.tolist(),
        "prediction_sessions": packet.audit_decision_sessions.tolist(),
    }
    predictions = FrozenPredictions(
        fold_id=packet.fold_id,
        artifact_id=artifact.artifact_id,
        artifact_sha256=artifact.artifact_sha256,
        audit_sample_ids=packet.audit_sample_ids,
        prediction_sessions=tuple(
            int(item) for item in packet.audit_decision_sessions
        ),
        label_unlock_sessions=tuple(
            int(item) for item in packet.audit_label_unlock_sessions
        ),
        expected_returns=tuple(float(item) for item in outer.audit_predictions),
        baseline_expected_returns=baseline,
        manifest_id=sha256_json(prediction_core),
    )
    predictions.validate()
    return BuilderFoldResult(
        artifact=artifact,
        predictions=predictions,
        selected_candidate_id=selected_id,
        inner_selection_scores=mean_scores,
        inner_fit_audits=tuple(inner_audits),
        outer_fit_audit=outer.audit,
    )


def build_synthetic_research_run(
    packets: tuple[BuilderFoldPacket, ...],
    candidates: tuple[LinearCandidate, ...],
) -> tuple[BuilderFoldResult, ...]:
    if type(packets) is not tuple or not packets:
        raise ResearchContractError("builder run requires outer-fold packets")
    return tuple(build_synthetic_outer_fold(packet, candidates) for packet in packets)
