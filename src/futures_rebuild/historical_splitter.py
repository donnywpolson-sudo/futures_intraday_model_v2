"""Synthetic-only splitter that physically separates builder and evaluator inputs."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .canonical import sha256_json
from .historical_engine_contracts import (
    BuilderFoldPacket,
    BuilderInnerPacket,
    EvaluatorFoldPacket,
    HistoricalResearchDataset,
    builder_packet_fixture,
    builder_packet_id_for,
    evaluator_packet_id_for,
)
from .research.contracts import ResearchContractError, array_sha256, make_synthetic_permit
from .research.splits import NestedFold


def _indices(value: np.ndarray, *, n: int, name: str) -> np.ndarray:
    if (
        not isinstance(value, np.ndarray)
        or value.dtype != np.dtype(np.int64)
        or value.ndim != 1
        or value.size == 0
        or bool(np.any(value < 0))
        or bool(np.any(value >= n))
        or len(np.unique(value)) != len(value)
    ):
        raise ResearchContractError(f"{name} is not an exact in-range int64 partition")
    return value


def _ids(dataset: HistoricalResearchDataset, indices: np.ndarray) -> tuple[str, ...]:
    return tuple(dataset.sample_ids[int(index)] for index in indices)


def _schedule_id(
    dataset: HistoricalResearchDataset, folds: tuple[NestedFold, ...]
) -> str:
    return sha256_json(
        {
            "feature_names": list(dataset.feature_names),
            "features_sha256": array_sha256(dataset.features),
            "folds": [
                {
                    "audit_indices": fold.audit_indices.tolist(),
                    "fit_indices": fold.fit_indices.tolist(),
                    "inner": [
                        {
                            "audit_indices": inner.audit_indices.tolist(),
                            "fit_indices": inner.fit_indices.tolist(),
                            "window": [
                                inner.validation_window.start,
                                inner.validation_window.stop,
                            ],
                        }
                        for inner in fold.inner_folds
                    ],
                    "window": [fold.test_window.start, fold.test_window.stop],
                }
                for fold in folds
            ],
            "sample_ids": list(dataset.sample_ids),
            "temporal": {
                "decision_sha256": array_sha256(dataset.temporal.decision_session),
                "known_sha256": array_sha256(dataset.temporal.label_known_session),
                "label_end_sha256": array_sha256(dataset.temporal.label_end),
                "label_start_sha256": array_sha256(dataset.temporal.label_start),
            },
        }
    )


@dataclass(frozen=True)
class SplitResearchRun:
    dataset_id: str
    split_schedule_id: str
    builder_packets: tuple[BuilderFoldPacket, ...]
    evaluator_packets: tuple[EvaluatorFoldPacket, ...]
    split_run_id: str

    def __post_init__(self) -> None:
        if (
            not self.builder_packets
            or len(self.builder_packets) != len(self.evaluator_packets)
            or any(
                builder.fold_id != evaluator.fold_id
                or builder.split_schedule_id != self.split_schedule_id
                or evaluator.split_schedule_id != self.split_schedule_id
                or builder.audit_sample_ids != evaluator.audit_sample_ids
                for builder, evaluator in zip(
                    self.builder_packets, self.evaluator_packets, strict=True
                )
            )
        ):
            raise ResearchContractError("split run role packets are inconsistent")
        expected = sha256_json(
            {
                "builder_packet_ids": [item.packet_id for item in self.builder_packets],
                "dataset_id": self.dataset_id,
                "evaluator_packet_ids": [
                    item.packet_id for item in self.evaluator_packets
                ],
                "split_schedule_id": self.split_schedule_id,
            }
        )
        if self.split_run_id != expected:
            raise ResearchContractError("split run ID is invalid")


def split_synthetic_research_run(
    dataset: HistoricalResearchDataset,
    folds: tuple[NestedFold, ...],
) -> SplitResearchRun:
    """Return separate builder packets without outer labels and evaluator labels."""

    if not isinstance(dataset, HistoricalResearchDataset) or not folds:
        raise ResearchContractError("synthetic dataset and nested folds are required")
    n = len(dataset.sample_ids)
    schedule_id = _schedule_id(dataset, folds)
    builder_packets: list[BuilderFoldPacket] = []
    evaluator_packets: list[EvaluatorFoldPacket] = []
    resolved = dataset.resolved_mask

    previous_audit_ids: set[str] = set()
    for fold_number, fold in enumerate(folds):
        if not isinstance(fold, NestedFold) or not fold.inner_folds:
            raise ResearchContractError("splitter requires exact nested folds")
        fit_indices = _indices(fold.fit_indices, n=n, name="outer_fit_indices")
        audit_indices = _indices(fold.audit_indices, n=n, name="outer_audit_indices")
        if set(fit_indices.tolist()).intersection(audit_indices.tolist()):
            raise ResearchContractError("outer fit/audit partitions overlap")
        fold.test_window.validate()
        audit_ids = _ids(dataset, audit_indices)
        if previous_audit_ids.intersection(audit_ids):
            raise ResearchContractError("outer audit sample IDs repeat across folds")
        previous_audit_ids.update(audit_ids)
        fold_id = sha256_json(
            {
                "audit_sample_ids": list(audit_ids),
                "fold_number": fold_number,
                "split_schedule_id": schedule_id,
                "test_window": [fold.test_window.start, fold.test_window.stop],
            }
        )

        matured_fit = fit_indices[resolved[fit_indices]]
        unresolved_fit = fit_indices[~resolved[fit_indices]]
        if matured_fit.size == 0:
            raise ResearchContractError("outer fit has no matured outcomes")
        fit_ids = _ids(dataset, matured_fit)
        unresolved_fit_ids = _ids(dataset, unresolved_fit) if unresolved_fit.size else ()

        inner_packets: list[BuilderInnerPacket] = []
        for inner_number, inner in enumerate(fold.inner_folds):
            inner_fit = _indices(
                inner.fit_indices, n=n, name="inner_fit_indices"
            )
            inner_audit = _indices(
                inner.audit_indices, n=n, name="inner_audit_indices"
            )
            inner_fit = inner_fit[resolved[inner_fit]]
            inner_audit = inner_audit[resolved[inner_audit]]
            if inner_fit.size == 0 or inner_audit.size == 0:
                raise ResearchContractError(
                    "inner selection fold has no matured fit/audit outcomes"
                )
            inner_fit_ids = _ids(dataset, inner_fit)
            inner_audit_ids = _ids(dataset, inner_audit)
            inner_fold_id = sha256_json(
                {
                    "audit_sample_ids": list(inner_audit_ids),
                    "fit_sample_ids": list(inner_fit_ids),
                    "fold_id": fold_id,
                    "inner_number": inner_number,
                    "validation_window": [
                        inner.validation_window.start,
                        inner.validation_window.stop,
                    ],
                }
            )
            inner_packets.append(
                BuilderInnerPacket(
                    inner_fold_id=inner_fold_id,
                    fit_sample_ids=inner_fit_ids,
                    audit_sample_ids=inner_audit_ids,
                    fit_features=dataset.features[inner_fit],
                    fit_labels=dataset.labels[inner_fit],
                    audit_features=dataset.features[inner_audit],
                    audit_labels=dataset.labels[inner_audit],
                )
            )
        inner_tuple = tuple(inner_packets)
        fit_features = dataset.features[matured_fit]
        fit_labels = dataset.labels[matured_fit]
        audit_features = dataset.features[audit_indices]
        audit_decisions = dataset.temporal.decision_session[audit_indices]
        audit_unlocks = dataset.temporal.label_known_session[audit_indices]
        fixture = builder_packet_fixture(
            fit_features, fit_labels, audit_features, inner_tuple
        )
        permit = make_synthetic_permit(
            fixture,
            generator_id=f"historical-splitter-fold-{fold_number}",
            seed=int(fold_id[:16], 16),
        )
        builder_id = builder_packet_id_for(
            fold_id=fold_id,
            split_schedule_id=schedule_id,
            feature_names=dataset.feature_names,
            fit_sample_ids=fit_ids,
            unresolved_fit_sample_ids=unresolved_fit_ids,
            audit_sample_ids=audit_ids,
            fit_features=fit_features,
            fit_labels=fit_labels,
            audit_features=audit_features,
            audit_decision_sessions=audit_decisions,
            audit_label_unlock_sessions=audit_unlocks,
            inner_packets=inner_tuple,
            permit=permit,
        )
        builder_packets.append(
            BuilderFoldPacket(
                fold_id=fold_id,
                split_schedule_id=schedule_id,
                feature_names=dataset.feature_names,
                fit_sample_ids=fit_ids,
                unresolved_fit_sample_ids=unresolved_fit_ids,
                audit_sample_ids=audit_ids,
                fit_features=fit_features,
                fit_labels=fit_labels,
                audit_features=audit_features,
                audit_decision_sessions=audit_decisions,
                audit_label_unlock_sessions=audit_unlocks,
                inner_packets=inner_tuple,
                permit=permit,
                packet_id=builder_id,
            )
        )
        evaluator_labels = dataset.labels[audit_indices]
        evaluator_statuses = tuple(dataset.outcome_statuses[int(i)] for i in audit_indices)
        evaluator_id = evaluator_packet_id_for(
            fold_id=fold_id,
            split_schedule_id=schedule_id,
            audit_sample_ids=audit_ids,
            audit_labels=evaluator_labels,
            outcome_statuses=evaluator_statuses,
            audit_decision_sessions=audit_decisions,
            audit_label_unlock_sessions=audit_unlocks,
        )
        evaluator_packets.append(
            EvaluatorFoldPacket(
                fold_id=fold_id,
                split_schedule_id=schedule_id,
                audit_sample_ids=audit_ids,
                audit_labels=evaluator_labels,
                outcome_statuses=evaluator_statuses,
                audit_decision_sessions=audit_decisions,
                audit_label_unlock_sessions=audit_unlocks,
                packet_id=evaluator_id,
            )
        )

    builder_tuple = tuple(builder_packets)
    evaluator_tuple = tuple(evaluator_packets)
    split_run_id = sha256_json(
        {
            "builder_packet_ids": [item.packet_id for item in builder_tuple],
            "dataset_id": dataset.dataset_id,
            "evaluator_packet_ids": [item.packet_id for item in evaluator_tuple],
            "split_schedule_id": schedule_id,
        }
    )
    return SplitResearchRun(
        dataset.dataset_id,
        schedule_id,
        builder_tuple,
        evaluator_tuple,
        split_run_id,
    )
