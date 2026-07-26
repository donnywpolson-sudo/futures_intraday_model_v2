"""Immutable synthetic research-engine packets shared across isolated roles.

This module contains no fitting or evaluation implementation.  It gives the
splitter, builder, and evaluator one strict content-addressed interchange
format while keeping outer labels physically absent from builder packets.
"""

from __future__ import annotations

import json
import hashlib
import math
import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

import numpy as np

from .canonical import canonical_bytes, sha256_json
from .predictor import ARTIFACT_FORMAT
from .research.contracts import (
    ResearchContractError,
    SyntheticOnlyPermit,
    array_sha256,
    finite_float64,
    require_synthetic_permit,
    require_unique_ascii_ids,
)
from .research.splits import TemporalSamples
from .schemas import (
    FORBIDDEN_FEATURE_NAMES,
    FORBIDDEN_FEATURE_PREFIXES,
    FORBIDDEN_ROLL_FEATURE_NAMES,
    OutcomeStatus,
)


def _frozen_float64(value: np.ndarray, *, name: str, ndim: int) -> np.ndarray:
    checked = finite_float64(value, name=name, ndim=ndim)
    result = np.ascontiguousarray(checked, dtype=np.float64).copy()
    result.flags.writeable = False
    return result


def _frozen_int64(value: np.ndarray, *, name: str) -> np.ndarray:
    if not isinstance(value, np.ndarray) or value.dtype != np.dtype(np.int64):
        raise ResearchContractError(f"{name} must be an int64 numpy.ndarray")
    if value.ndim != 1 or value.size == 0:
        raise ResearchContractError(f"{name} must be a nonempty vector")
    result = np.ascontiguousarray(value, dtype=np.int64).copy()
    result.flags.writeable = False
    return result


def _frozen_bool(value: np.ndarray, *, name: str) -> np.ndarray:
    if not isinstance(value, np.ndarray) or value.dtype != np.dtype(np.bool_):
        raise ResearchContractError(f"{name} must be a bool numpy.ndarray")
    if value.ndim != 1 or value.size == 0:
        raise ResearchContractError(f"{name} must be a nonempty vector")
    result = np.ascontiguousarray(value, dtype=np.bool_).copy()
    result.flags.writeable = False
    return result


def _feature_names(values: tuple[str, ...]) -> tuple[str, ...]:
    result = require_unique_ascii_ids(values, name="feature_names")
    for name in result:
        normalized = name.casefold()
        if (
            not name.isidentifier()
            or normalized in FORBIDDEN_FEATURE_NAMES
            or normalized in FORBIDDEN_ROLL_FEATURE_NAMES
            or normalized.startswith(FORBIDDEN_FEATURE_PREFIXES)
        ):
            raise ResearchContractError(
                f"future/outcome/roll feature canary rejected before fitting: {name}"
            )
    return result


def synthetic_research_fixture(
    features: np.ndarray,
    labels: np.ndarray,
    resolved: np.ndarray | None = None,
) -> np.ndarray:
    x = finite_float64(features, name="features", ndim=2)
    y = finite_float64(labels, name="labels", ndim=1)
    if len(x) != len(y):
        raise ResearchContractError("feature/label row counts differ")
    parts = (x.reshape(-1), y)
    if resolved is not None:
        if (
            not isinstance(resolved, np.ndarray)
            or resolved.dtype != np.dtype(np.bool_)
            or resolved.ndim != 1
            or len(resolved) != len(y)
        ):
            raise ResearchContractError("resolved mask must be a row-aligned bool vector")
        parts = (*parts, resolved.astype(np.float64))
    return np.ascontiguousarray(np.concatenate(parts), dtype=np.float64)


def _temporal_copy(value: TemporalSamples, *, n: int) -> TemporalSamples:
    if not isinstance(value, TemporalSamples) or value.validate() != n:
        raise ResearchContractError("temporal samples do not match the research rows")
    return TemporalSamples(
        _frozen_int64(value.decision_session, name="decision_session"),
        _frozen_int64(value.label_start, name="label_start"),
        _frozen_int64(value.label_end, name="label_end"),
        _frozen_int64(value.label_known_session, name="label_known_session"),
    )


@dataclass(frozen=True)
class HistoricalResearchDataset:
    sample_ids: tuple[str, ...]
    feature_names: tuple[str, ...]
    features: np.ndarray
    labels: np.ndarray
    outcome_statuses: tuple[str, ...]
    temporal: TemporalSamples
    permit: SyntheticOnlyPermit

    def __post_init__(self) -> None:
        sample_ids = require_unique_ascii_ids(self.sample_ids, name="sample_ids")
        names = _feature_names(self.feature_names)
        features = _frozen_float64(self.features, name="features", ndim=2)
        labels = _frozen_float64(self.labels, name="labels", ndim=1)
        if (
            features.shape != (len(sample_ids), len(names))
            or len(labels) != len(sample_ids)
            or len(self.outcome_statuses) != len(sample_ids)
        ):
            raise ResearchContractError("historical dataset shapes are not row aligned")
        allowed = {item.value for item in OutcomeStatus}
        if any(type(item) is not str or item not in allowed for item in self.outcome_statuses):
            raise ResearchContractError("historical dataset outcome status is invalid")
        resolved = np.asarray(
            [item == OutcomeStatus.MATURED.value for item in self.outcome_statuses],
            dtype=np.bool_,
        )
        if bool(np.any((~resolved) & (labels != 0.0))):
            raise ResearchContractError(
                "unresolved outcomes require an explicit unused zero sentinel"
            )
        require_synthetic_permit(
            self.permit, synthetic_research_fixture(features, labels, resolved)
        )
        object.__setattr__(self, "sample_ids", sample_ids)
        object.__setattr__(self, "feature_names", names)
        object.__setattr__(self, "features", features)
        object.__setattr__(self, "labels", labels)
        object.__setattr__(self, "temporal", _temporal_copy(self.temporal, n=len(labels)))

    @property
    def resolved_mask(self) -> np.ndarray:
        result = np.asarray(
            [item == OutcomeStatus.MATURED.value for item in self.outcome_statuses],
            dtype=np.bool_,
        )
        result.flags.writeable = False
        return result

    @property
    def dataset_id(self) -> str:
        return sha256_json(
            {
                "feature_names": list(self.feature_names),
                "features_sha256": array_sha256(self.features),
                "labels_sha256": array_sha256(self.labels),
                "outcome_statuses": list(self.outcome_statuses),
                "permit": {
                    "dataset_sha256": self.permit.dataset_sha256,
                    "generator_id": self.permit.generator_id,
                    "purpose": self.permit.purpose,
                    "seed": self.permit.seed,
                    "source_kind": self.permit.source_kind,
                },
                "sample_ids": list(self.sample_ids),
                "temporal": {
                    "decision_sha256": array_sha256(self.temporal.decision_session),
                    "known_sha256": array_sha256(self.temporal.label_known_session),
                    "label_end_sha256": array_sha256(self.temporal.label_end),
                    "label_start_sha256": array_sha256(self.temporal.label_start),
                },
            }
        )


def builder_packet_fixture(
    fit_features: np.ndarray,
    fit_labels: np.ndarray,
    audit_features: np.ndarray,
    inner_packets: tuple["BuilderInnerPacket", ...],
) -> np.ndarray:
    parts: list[np.ndarray] = [
        fit_features.reshape(-1),
        fit_labels,
        audit_features.reshape(-1),
    ]
    for packet in inner_packets:
        parts.extend(
            (
                packet.fit_features.reshape(-1),
                packet.fit_labels,
                packet.audit_features.reshape(-1),
                packet.audit_labels,
            )
        )
    return np.ascontiguousarray(np.concatenate(parts), dtype=np.float64)


def builder_packet_id_for(
    *,
    fold_id: str,
    split_schedule_id: str,
    feature_names: tuple[str, ...],
    fit_sample_ids: tuple[str, ...],
    unresolved_fit_sample_ids: tuple[str, ...],
    audit_sample_ids: tuple[str, ...],
    fit_features: np.ndarray,
    fit_labels: np.ndarray,
    audit_features: np.ndarray,
    audit_decision_sessions: np.ndarray,
    audit_label_unlock_sessions: np.ndarray,
    inner_packets: tuple["BuilderInnerPacket", ...],
    permit: SyntheticOnlyPermit,
) -> str:
    return sha256_json(
        {
            "audit_decisions_sha256": array_sha256(audit_decision_sessions),
            "audit_features_sha256": array_sha256(audit_features),
            "audit_label_unlocks_sha256": array_sha256(
                audit_label_unlock_sessions
            ),
            "audit_sample_ids": list(audit_sample_ids),
            "feature_names": list(feature_names),
            "fit_features_sha256": array_sha256(fit_features),
            "fit_labels_sha256": array_sha256(fit_labels),
            "fit_sample_ids": list(fit_sample_ids),
            "fold_id": fold_id,
            "inner_packet_ids": [item.packet_id for item in inner_packets],
            "permit_dataset_sha256": permit.dataset_sha256,
            "split_schedule_id": split_schedule_id,
            "unresolved_fit_sample_ids": list(unresolved_fit_sample_ids),
        }
    )


def evaluator_packet_id_for(
    *,
    fold_id: str,
    split_schedule_id: str,
    audit_sample_ids: tuple[str, ...],
    audit_labels: np.ndarray,
    outcome_statuses: tuple[str, ...],
    audit_decision_sessions: np.ndarray,
    audit_label_unlock_sessions: np.ndarray,
) -> str:
    return sha256_json(
        {
            "audit_decisions_sha256": array_sha256(audit_decision_sessions),
            "audit_labels_sha256": array_sha256(audit_labels),
            "audit_label_unlocks_sha256": array_sha256(
                audit_label_unlock_sessions
            ),
            "audit_sample_ids": list(audit_sample_ids),
            "fold_id": fold_id,
            "outcome_statuses": list(outcome_statuses),
            "split_schedule_id": split_schedule_id,
        }
    )


@dataclass(frozen=True)
class BuilderInnerPacket:
    inner_fold_id: str
    fit_sample_ids: tuple[str, ...]
    audit_sample_ids: tuple[str, ...]
    fit_features: np.ndarray
    fit_labels: np.ndarray
    audit_features: np.ndarray
    audit_labels: np.ndarray

    def __post_init__(self) -> None:
        if type(self.inner_fold_id) is not str or not self.inner_fold_id:
            raise ResearchContractError("inner fold ID is required")
        fit_ids = require_unique_ascii_ids(self.fit_sample_ids, name="inner_fit_ids")
        audit_ids = require_unique_ascii_ids(self.audit_sample_ids, name="inner_audit_ids")
        if set(fit_ids).intersection(audit_ids):
            raise ResearchContractError("inner fit/audit sample IDs overlap")
        fit_x = _frozen_float64(self.fit_features, name="inner_fit_features", ndim=2)
        fit_y = _frozen_float64(self.fit_labels, name="inner_fit_labels", ndim=1)
        audit_x = _frozen_float64(
            self.audit_features, name="inner_audit_features", ndim=2
        )
        audit_y = _frozen_float64(self.audit_labels, name="inner_audit_labels", ndim=1)
        if (
            len(fit_ids) != len(fit_x)
            or len(fit_y) != len(fit_x)
            or len(audit_ids) != len(audit_x)
            or len(audit_y) != len(audit_x)
            or fit_x.shape[1] != audit_x.shape[1]
        ):
            raise ResearchContractError("inner packet shapes are invalid")
        object.__setattr__(self, "fit_sample_ids", fit_ids)
        object.__setattr__(self, "audit_sample_ids", audit_ids)
        object.__setattr__(self, "fit_features", fit_x)
        object.__setattr__(self, "fit_labels", fit_y)
        object.__setattr__(self, "audit_features", audit_x)
        object.__setattr__(self, "audit_labels", audit_y)

    @property
    def packet_id(self) -> str:
        return sha256_json(
            {
                "audit_features_sha256": array_sha256(self.audit_features),
                "audit_labels_sha256": array_sha256(self.audit_labels),
                "audit_sample_ids": list(self.audit_sample_ids),
                "fit_features_sha256": array_sha256(self.fit_features),
                "fit_labels_sha256": array_sha256(self.fit_labels),
                "fit_sample_ids": list(self.fit_sample_ids),
                "inner_fold_id": self.inner_fold_id,
            }
        )


@dataclass(frozen=True)
class BuilderFoldPacket:
    fold_id: str
    split_schedule_id: str
    feature_names: tuple[str, ...]
    fit_sample_ids: tuple[str, ...]
    unresolved_fit_sample_ids: tuple[str, ...]
    audit_sample_ids: tuple[str, ...]
    fit_features: np.ndarray
    fit_labels: np.ndarray
    audit_features: np.ndarray
    audit_decision_sessions: np.ndarray
    audit_label_unlock_sessions: np.ndarray
    inner_packets: tuple[BuilderInnerPacket, ...]
    permit: SyntheticOnlyPermit
    packet_id: str

    def __post_init__(self) -> None:
        if type(self.fold_id) is not str or not self.fold_id:
            raise ResearchContractError("builder fold ID is required")
        if type(self.split_schedule_id) is not str or len(self.split_schedule_id) != 64:
            raise ResearchContractError("split schedule ID must be SHA-256")
        names = _feature_names(self.feature_names)
        fit_ids = require_unique_ascii_ids(self.fit_sample_ids, name="outer_fit_ids")
        unresolved = tuple(self.unresolved_fit_sample_ids)
        if any(type(item) is not str or not item or not item.isascii() for item in unresolved):
            raise ResearchContractError("unresolved fit IDs must be explicit ASCII IDs")
        if len(set(unresolved)) != len(unresolved):
            raise ResearchContractError("unresolved fit IDs contain duplicates")
        audit_ids = require_unique_ascii_ids(self.audit_sample_ids, name="outer_audit_ids")
        if set(fit_ids).intersection(audit_ids) or set(unresolved).intersection(audit_ids):
            raise ResearchContractError("outer fit/audit IDs overlap")
        fit_x = _frozen_float64(self.fit_features, name="outer_fit_features", ndim=2)
        fit_y = _frozen_float64(self.fit_labels, name="outer_fit_labels", ndim=1)
        audit_x = _frozen_float64(self.audit_features, name="outer_audit_features", ndim=2)
        decisions = _frozen_int64(
            self.audit_decision_sessions, name="outer_audit_decisions"
        )
        unlocks = _frozen_int64(
            self.audit_label_unlock_sessions, name="outer_audit_label_unlocks"
        )
        if (
            fit_x.shape != (len(fit_ids), len(names))
            or len(fit_y) != len(fit_ids)
            or audit_x.shape != (len(audit_ids), len(names))
            or len(decisions) != len(audit_ids)
            or len(unlocks) != len(audit_ids)
            or bool(np.any(decisions >= unlocks))
            or not self.inner_packets
        ):
            raise ResearchContractError("builder packet shapes/timing are invalid")
        outer_fit = set(fit_ids)
        for inner in self.inner_packets:
            if inner.fit_features.shape[1] != len(names) or not set(
                (*inner.fit_sample_ids, *inner.audit_sample_ids)
            ).issubset(outer_fit):
                raise ResearchContractError("inner packet escapes the outer fit partition")
        fixture = builder_packet_fixture(fit_x, fit_y, audit_x, self.inner_packets)
        require_synthetic_permit(self.permit, fixture)
        expected_packet_id = builder_packet_id_for(
            fold_id=self.fold_id,
            split_schedule_id=self.split_schedule_id,
            feature_names=names,
            fit_sample_ids=fit_ids,
            unresolved_fit_sample_ids=unresolved,
            audit_sample_ids=audit_ids,
            fit_features=fit_x,
            fit_labels=fit_y,
            audit_features=audit_x,
            audit_decision_sessions=decisions,
            audit_label_unlock_sessions=unlocks,
            inner_packets=self.inner_packets,
            permit=self.permit,
        )
        if self.packet_id != expected_packet_id:
            raise ResearchContractError("builder packet ID is invalid")
        object.__setattr__(self, "feature_names", names)
        object.__setattr__(self, "fit_sample_ids", fit_ids)
        object.__setattr__(self, "unresolved_fit_sample_ids", unresolved)
        object.__setattr__(self, "audit_sample_ids", audit_ids)
        object.__setattr__(self, "fit_features", fit_x)
        object.__setattr__(self, "fit_labels", fit_y)
        object.__setattr__(self, "audit_features", audit_x)
        object.__setattr__(self, "audit_decision_sessions", decisions)
        object.__setattr__(self, "audit_label_unlock_sessions", unlocks)


@dataclass(frozen=True)
class EvaluatorFoldPacket:
    fold_id: str
    split_schedule_id: str
    audit_sample_ids: tuple[str, ...]
    audit_labels: np.ndarray
    outcome_statuses: tuple[str, ...]
    audit_decision_sessions: np.ndarray
    audit_label_unlock_sessions: np.ndarray
    packet_id: str

    def __post_init__(self) -> None:
        ids = require_unique_ascii_ids(self.audit_sample_ids, name="evaluator_audit_ids")
        labels = _frozen_float64(self.audit_labels, name="evaluator_labels", ndim=1)
        decisions = _frozen_int64(self.audit_decision_sessions, name="evaluator_decisions")
        unlocks = _frozen_int64(
            self.audit_label_unlock_sessions, name="evaluator_label_unlocks"
        )
        allowed = {item.value for item in OutcomeStatus}
        if (
            len(labels) != len(ids)
            or len(self.outcome_statuses) != len(ids)
            or len(decisions) != len(ids)
            or len(unlocks) != len(ids)
            or bool(np.any(decisions >= unlocks))
            or any(item not in allowed for item in self.outcome_statuses)
        ):
            raise ResearchContractError("evaluator packet shapes/statuses are invalid")
        resolved = np.asarray(
            [item == OutcomeStatus.MATURED.value for item in self.outcome_statuses],
            dtype=np.bool_,
        )
        if bool(np.any((~resolved) & (labels != 0.0))):
            raise ResearchContractError("unresolved evaluator labels contain a return")
        expected = evaluator_packet_id_for(
            fold_id=self.fold_id,
            split_schedule_id=self.split_schedule_id,
            audit_sample_ids=ids,
            audit_labels=labels,
            outcome_statuses=self.outcome_statuses,
            audit_decision_sessions=decisions,
            audit_label_unlock_sessions=unlocks,
        )
        if self.packet_id != expected:
            raise ResearchContractError("evaluator packet ID is invalid")
        object.__setattr__(self, "audit_sample_ids", ids)
        object.__setattr__(self, "audit_labels", labels)
        object.__setattr__(self, "audit_decision_sessions", decisions)
        object.__setattr__(self, "audit_label_unlock_sessions", unlocks)


@dataclass(frozen=True)
class LinearCandidate:
    candidate_id: str
    feature_names: tuple[str, ...]
    ridge_penalty: float

    def validate(self, available_features: tuple[str, ...]) -> None:
        if type(self.candidate_id) is not str or not self.candidate_id.isascii() or not self.candidate_id:
            raise ResearchContractError("linear candidate ID is invalid")
        names = _feature_names(self.feature_names)
        if not set(names).issubset(available_features) or tuple(
            name for name in available_features if name in names
        ) != names:
            raise ResearchContractError("candidate features are not an ordered subset")
        if (
            isinstance(self.ridge_penalty, bool)
            or not isinstance(self.ridge_penalty, (int, float))
            or not math.isfinite(float(self.ridge_penalty))
            or float(self.ridge_penalty) <= 0
        ):
            raise ResearchContractError("ridge penalty must be a positive finite scalar")


@dataclass(frozen=True)
class FitAuditRecord:
    fold_id: str
    stage: str
    candidate_id: str
    fit_sample_ids: tuple[str, ...]
    audit_sample_ids: tuple[str, ...]
    fit_features_sha256: str
    fit_labels_sha256: str
    audit_features_sha256: str
    audit_labels_sha256: str | None
    scaler_fit_id: str
    model_fit_id: str
    record_id: str

    def validate(self) -> None:
        fit_ids = require_unique_ascii_ids(self.fit_sample_ids, name="fit_audit_fit_ids")
        audit_ids = require_unique_ascii_ids(
            self.audit_sample_ids, name="fit_audit_audit_ids"
        )
        if set(fit_ids).intersection(audit_ids):
            raise ResearchContractError("fit audit records overlapping sample IDs")
        if (
            type(self.fold_id) is not str
            or not self.fold_id
            or type(self.candidate_id) is not str
            or not self.candidate_id
            or self.stage not in {"INNER_SELECTION", "OUTER_REFIT"}
        ):
            raise ResearchContractError("fit audit role identities are invalid")
        hashes = (
            self.fit_features_sha256,
            self.fit_labels_sha256,
            self.audit_features_sha256,
            self.scaler_fit_id,
            self.model_fit_id,
        )
        if any(
            type(item) is not str or re.fullmatch(r"[0-9a-f]{64}", item) is None
            for item in hashes
        ) or (
            self.audit_labels_sha256 is not None
            and re.fullmatch(r"[0-9a-f]{64}", self.audit_labels_sha256) is None
        ):
            raise ResearchContractError("fit audit hashes are invalid")
        core = {
            "audit_features_sha256": self.audit_features_sha256,
            "audit_labels_sha256": self.audit_labels_sha256,
            "audit_sample_ids": list(audit_ids),
            "candidate_id": self.candidate_id,
            "fit_features_sha256": self.fit_features_sha256,
            "fit_labels_sha256": self.fit_labels_sha256,
            "fit_sample_ids": list(fit_ids),
            "fold_id": self.fold_id,
            "model_fit_id": self.model_fit_id,
            "scaler_fit_id": self.scaler_fit_id,
            "stage": self.stage,
        }
        if self.record_id != sha256_json(core):
            raise ResearchContractError("fit audit record ID is invalid")


def _artifact_payload(artifact_bytes: bytes) -> dict[str, object]:
    try:
        payload = json.loads(artifact_bytes.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ResearchContractError("linear artifact is invalid JSON") from exc
    if artifact_bytes != canonical_bytes(payload) + b"\n" or not isinstance(payload, dict):
        raise ResearchContractError("linear artifact is not canonical JSON")
    expected = {
        "artifact_format",
        "expected_return_scale",
        "feature_names",
        "intercept",
        "parity_input",
        "parity_output",
        "uncertainty",
        "weights",
    }
    if set(payload) != expected or payload.get("artifact_format") != ARTIFACT_FORMAT:
        raise ResearchContractError("linear artifact is not trusted-format compatible")
    return payload


@dataclass(frozen=True)
class FrozenLinearArtifact:
    fold_id: str
    builder_packet_id: str
    selected_candidate_id: str
    outer_fit_audit_id: str
    artifact_bytes: bytes
    artifact_sha256: str
    artifact_id: str

    def payload(self) -> Mapping[str, object]:
        return MappingProxyType(_artifact_payload(self.artifact_bytes))

    def validate(self) -> None:
        payload = _artifact_payload(self.artifact_bytes)
        if self.artifact_sha256 != hashlib.sha256(self.artifact_bytes).hexdigest():
            raise ResearchContractError("frozen artifact byte hash is invalid")
        try:
            if (
                not isinstance(payload["feature_names"], list)
                or not isinstance(payload["weights"], list)
                or not isinstance(payload["parity_input"], list)
            ):
                raise TypeError
            names = tuple(payload["feature_names"])
            weights = tuple(float(item) for item in payload["weights"])
            parity = tuple(float(item) for item in payload["parity_input"])
            scalar_values = (
                float(payload["intercept"]),
                float(payload["expected_return_scale"]),
                float(payload["uncertainty"]),
                *weights,
                *parity,
            )
        except (TypeError, ValueError) as exc:
            raise ResearchContractError("frozen artifact values are invalid") from exc
        if (
            names != _feature_names(names)
            or len(names) != len(weights)
            or len(names) != len(parity)
            or any(not math.isfinite(item) for item in scalar_values)
            or float(payload["expected_return_scale"]) <= 0
            or float(payload["uncertainty"]) < 0
        ):
            raise ResearchContractError("frozen artifact shape/value contract failed")
        parity_output = payload.get("parity_output")
        parity_fields = {
            "expected_return",
            "probability_down",
            "probability_neutral",
            "probability_up",
            "uncertainty",
        }
        if not isinstance(parity_output, dict) or set(parity_output) != parity_fields:
            raise ResearchContractError("frozen artifact parity schema is invalid")
        try:
            observed_score = float(payload["intercept"]) + sum(
                weight * value for weight, value in zip(weights, parity, strict=True)
            )
            observed_up = 1.0 / (
                1.0 + math.exp(-max(-40.0, min(40.0, observed_score)))
            )
            expected_parity = {
                "expected_return": observed_score
                * float(payload["expected_return_scale"]),
                "probability_down": 1.0 - observed_up,
                "probability_neutral": 0.0,
                "probability_up": observed_up,
                "uncertainty": float(payload["uncertainty"]),
            }
            parity_failed = any(
                not math.isfinite(float(parity_output[name]))
                or not math.isclose(
                    expected_parity[name],
                    float(parity_output[name]),
                    rel_tol=1e-14,
                    abs_tol=1e-14,
                )
                for name in parity_fields
            )
        except (TypeError, ValueError) as exc:
            raise ResearchContractError("frozen artifact parity values are invalid") from exc
        if parity_failed:
            raise ResearchContractError("frozen artifact reload parity failed")
        core = {
            "artifact_sha256": self.artifact_sha256,
            "builder_packet_id": self.builder_packet_id,
            "fold_id": self.fold_id,
            "outer_fit_audit_id": self.outer_fit_audit_id,
            "selected_candidate_id": self.selected_candidate_id,
        }
        if self.artifact_id != sha256_json(core):
            raise ResearchContractError("frozen artifact ID is invalid")


@dataclass(frozen=True)
class FrozenPredictions:
    fold_id: str
    artifact_id: str
    artifact_sha256: str
    audit_sample_ids: tuple[str, ...]
    prediction_sessions: tuple[int, ...]
    label_unlock_sessions: tuple[int, ...]
    expected_returns: tuple[float, ...]
    baseline_expected_returns: tuple[float, ...]
    manifest_id: str

    def validate(self) -> None:
        ids = require_unique_ascii_ids(self.audit_sample_ids, name="prediction_audit_ids")
        n = len(ids)
        if any(
            len(values) != n
            for values in (
                self.prediction_sessions,
                self.label_unlock_sessions,
                self.expected_returns,
                self.baseline_expected_returns,
            )
        ):
            raise ResearchContractError("prediction manifest arrays are not aligned")
        if any(
            isinstance(item, bool) or not isinstance(item, int)
            for item in (*self.prediction_sessions, *self.label_unlock_sessions)
        ) or any(
            prediction >= unlock
            for prediction, unlock in zip(
                self.prediction_sessions, self.label_unlock_sessions, strict=True
            )
        ):
            raise ResearchContractError("predictions do not predate label unlock")
        if any(
            isinstance(item, bool)
            or not isinstance(item, (int, float))
            or not math.isfinite(float(item))
            for item in (*self.expected_returns, *self.baseline_expected_returns)
        ):
            raise ResearchContractError("prediction manifest contains invalid forecasts")
        core = {
            "artifact_id": self.artifact_id,
            "artifact_sha256": self.artifact_sha256,
            "audit_sample_ids": list(ids),
            "baseline_expected_returns": list(self.baseline_expected_returns),
            "expected_returns": list(self.expected_returns),
            "fold_id": self.fold_id,
            "label_unlock_sessions": list(self.label_unlock_sessions),
            "prediction_sessions": list(self.prediction_sessions),
        }
        if self.manifest_id != sha256_json(core):
            raise ResearchContractError("prediction manifest ID is invalid")


@dataclass(frozen=True)
class BuilderFoldResult:
    artifact: FrozenLinearArtifact
    predictions: FrozenPredictions
    selected_candidate_id: str
    inner_selection_scores: Mapping[str, float]
    inner_fit_audits: tuple[FitAuditRecord, ...]
    outer_fit_audit: FitAuditRecord

    def __post_init__(self) -> None:
        self.artifact.validate()
        self.predictions.validate()
        self.outer_fit_audit.validate()
        for item in self.inner_fit_audits:
            item.validate()
        scores = dict(self.inner_selection_scores)
        if not scores or any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            for value in scores.values()
        ):
            raise ResearchContractError("builder selection scores are invalid")
        expected_selected = min(scores, key=lambda key: (float(scores[key]), key))
        inner_candidate_ids = {item.candidate_id for item in self.inner_fit_audits}
        if (
            self.selected_candidate_id not in scores
            or self.selected_candidate_id != expected_selected
            or set(scores) != inner_candidate_ids
            or not self.inner_fit_audits
            or any(item.stage != "INNER_SELECTION" for item in self.inner_fit_audits)
            or self.outer_fit_audit.stage != "OUTER_REFIT"
            or self.outer_fit_audit.candidate_id != self.selected_candidate_id
            or self.outer_fit_audit.fold_id != self.artifact.fold_id
            or self.outer_fit_audit.record_id != self.artifact.outer_fit_audit_id
            or self.artifact.selected_candidate_id != self.selected_candidate_id
            or self.predictions.artifact_id != self.artifact.artifact_id
            or self.predictions.fold_id != self.artifact.fold_id
            or self.predictions.audit_sample_ids
            != self.outer_fit_audit.audit_sample_ids
        ):
            raise ResearchContractError("builder result is internally inconsistent")
        object.__setattr__(self, "inner_selection_scores", MappingProxyType(scores))


@dataclass(frozen=True)
class FrozenEvaluation:
    fold_id: str
    evaluator_packet_id: str
    artifact_id: str
    prediction_manifest_id: str
    denominator_count: int
    resolved_count: int
    unresolved_count: int
    mean_squared_error: float | None
    baseline_mean_squared_error: float | None
    correlation: float | None
    directional_accuracy: float | None
    mechanics_state: str
    alpha_evidence: bool
    candidate_eligible: bool
    evaluation_id: str

    def validate(self) -> None:
        if (
            type(self.denominator_count) is not int
            or type(self.resolved_count) is not int
            or type(self.unresolved_count) is not int
            or self.denominator_count <= 0
            or self.resolved_count < 0
            or self.unresolved_count < 0
            or self.resolved_count + self.unresolved_count != self.denominator_count
            or self.alpha_evidence is not False
            or self.candidate_eligible is not False
        ):
            raise ResearchContractError("evaluation coverage/authority contract failed")
        metrics = (
            self.mean_squared_error,
            self.baseline_mean_squared_error,
            self.correlation,
            self.directional_accuracy,
        )
        if any(value is not None and not math.isfinite(value) for value in metrics):
            raise ResearchContractError("evaluation metric is nonfinite")
        core = {
            "alpha_evidence": self.alpha_evidence,
            "artifact_id": self.artifact_id,
            "baseline_mean_squared_error": self.baseline_mean_squared_error,
            "candidate_eligible": self.candidate_eligible,
            "correlation": self.correlation,
            "denominator_count": self.denominator_count,
            "directional_accuracy": self.directional_accuracy,
            "evaluator_packet_id": self.evaluator_packet_id,
            "fold_id": self.fold_id,
            "mean_squared_error": self.mean_squared_error,
            "mechanics_state": self.mechanics_state,
            "prediction_manifest_id": self.prediction_manifest_id,
            "resolved_count": self.resolved_count,
            "unresolved_count": self.unresolved_count,
        }
        if self.evaluation_id != sha256_json(core):
            raise ResearchContractError("evaluation ID is invalid")
