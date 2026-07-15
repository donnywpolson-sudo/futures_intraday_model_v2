from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.historical_builder import build_synthetic_research_run
from futures_rebuild.historical_engine_contracts import (
    HistoricalResearchDataset,
    LinearCandidate,
    synthetic_research_fixture,
)
from futures_rebuild.historical_evaluator import evaluate_frozen_research_run
from futures_rebuild.historical_splitter import split_synthetic_research_run
from futures_rebuild.predictor import ARTIFACT_FORMAT
from futures_rebuild.producer_bridge import CAUSAL_OUTCOME_LABEL_METHOD_ID
from futures_rebuild.research import (
    ResearchContractError,
    SessionWindow,
    TemporalSamples,
    make_synthetic_permit,
    nested_chronological_splits,
)
from futures_rebuild.schemas import OutcomeStatus


def _temporal(n: int) -> TemporalSamples:
    decision = np.arange(n, dtype=np.int64)
    return TemporalSamples(
        decision,
        decision + np.int64(1),
        decision + np.int64(2),
        decision + np.int64(2),
    )


def _folds(temporal: TemporalSamples):
    return nested_chronological_splits(
        temporal,
        (SessionWindow(100, 120), SessionWindow(130, 150)),
        (
            (SessionWindow(50, 60), SessionWindow(75, 85)),
            (SessionWindow(70, 80), SessionWindow(105, 115)),
        ),
        session_embargo=1,
        minimum_fit_samples=20,
        minimum_audit_samples=8,
    )


def _dataset(
    *,
    signal: bool,
    label_override: np.ndarray | None = None,
    statuses: tuple[str, ...] | None = None,
    feature_names: tuple[str, ...] = ("signal", "noise"),
) -> HistoricalResearchDataset:
    n = 170
    time = np.arange(n, dtype=np.float64)
    x0 = np.sin(time * 0.173) + 0.2 * np.cos(time * 0.037)
    x1 = np.cos(time * 0.311) - 0.1 * np.sin(time * 0.071)
    features = np.column_stack((x0, x1)).astype(np.float64)
    if signal:
        labels = (0.8 * x0 + 0.03 * np.sin(time * 0.91)).astype(np.float64)
    else:
        labels = np.where((time.astype(np.int64) % 2) == 0, 0.5, -0.5).astype(
            np.float64
        )
    if label_override is not None:
        labels = np.ascontiguousarray(label_override, dtype=np.float64)
    if statuses is None:
        statuses = tuple(OutcomeStatus.MATURED.value for _ in range(n))
    resolved = np.asarray(
        [item == OutcomeStatus.MATURED.value for item in statuses], dtype=np.bool_
    )
    labels = labels.copy()
    labels[~resolved] = 0.0
    fixture = synthetic_research_fixture(features, labels, resolved)
    permit = make_synthetic_permit(
        fixture, generator_id="historical-engine-fixture", seed=404
    )
    return HistoricalResearchDataset(
        sample_ids=tuple(f"sample-{index:04d}" for index in range(n)),
        feature_names=feature_names,
        features=features,
        labels=labels,
        outcome_statuses=statuses,
        temporal=_temporal(n),
        permit=permit,
    )


def _candidates() -> tuple[LinearCandidate, ...]:
    return (
        LinearCandidate("all-ridge-1", ("signal", "noise"), 1.0),
        LinearCandidate("signal-ridge-small", ("signal",), 0.01),
        LinearCandidate("signal-ridge-large", ("signal",), 25.0),
    )


def test_signal_recovery_is_inner_selected_frozen_and_trusted_format_compatible() -> None:
    dataset = _dataset(signal=True)
    split = split_synthetic_research_run(dataset, _folds(dataset.temporal))
    built = build_synthetic_research_run(split.builder_packets, _candidates())
    evaluated = evaluate_frozen_research_run(split.evaluator_packets, built)

    assert all(item.mechanics_state == "SYNTHETIC_SIGNAL_RECOVERED" for item in evaluated)
    assert all(item.alpha_evidence is False for item in evaluated)
    assert all(item.candidate_eligible is False for item in evaluated)
    for packet, result in zip(split.builder_packets, built, strict=True):
        payload = result.artifact.payload()
        assert payload["artifact_format"] == ARTIFACT_FORMAT
        assert result.selected_candidate_id in result.inner_selection_scores
        assert not set(result.outer_fit_audit.fit_sample_ids).intersection(
            result.outer_fit_audit.audit_sample_ids
        )
        assert result.outer_fit_audit.fit_sample_ids == packet.fit_sample_ids
        assert result.predictions.audit_sample_ids == packet.audit_sample_ids
        assert all(
            prediction_session < label_unlock_session
            for prediction_session, label_unlock_session in zip(
                result.predictions.prediction_sessions,
                result.predictions.label_unlock_sessions,
                strict=True,
            )
        )


def test_outer_label_mutation_cannot_change_first_fold_builder_output() -> None:
    original = _dataset(signal=True)
    folds = _folds(original.temporal)
    changed_labels = original.labels.copy()
    changed_labels[folds[0].audit_indices] *= -100.0
    changed = _dataset(signal=True, label_override=changed_labels)

    first_split = split_synthetic_research_run(original, folds)
    second_split = split_synthetic_research_run(changed, _folds(changed.temporal))
    assert first_split.builder_packets[0].packet_id == second_split.builder_packets[0].packet_id
    first = build_synthetic_research_run((first_split.builder_packets[0],), _candidates())[0]
    second = build_synthetic_research_run((second_split.builder_packets[0],), _candidates())[0]
    assert first.artifact == second.artifact
    assert first.predictions == second.predictions
    assert first_split.evaluator_packets[0].packet_id != second_split.evaluator_packets[0].packet_id


def test_noise_and_unresolved_denominators_never_gain_authority() -> None:
    noise = _dataset(signal=False)
    noise_split = split_synthetic_research_run(noise, _folds(noise.temporal))
    noise_built = build_synthetic_research_run(
        noise_split.builder_packets, _candidates()
    )
    noise_evaluated = evaluate_frozen_research_run(
        noise_split.evaluator_packets, noise_built
    )
    assert all(
        item.mechanics_state == "SYNTHETIC_NOISE_OR_NO_EDGE"
        for item in noise_evaluated
    )

    statuses = [OutcomeStatus.MATURED.value for _ in range(170)]
    for index in (102, 107, 113):
        statuses[index] = OutcomeStatus.MISSING_SOURCE.value
    dataset = _dataset(signal=True, statuses=tuple(statuses))
    split = split_synthetic_research_run(dataset, _folds(dataset.temporal))
    built = build_synthetic_research_run(split.builder_packets, _candidates())
    evaluated = evaluate_frozen_research_run(split.evaluator_packets, built)

    assert evaluated[0].denominator_count == 20
    assert evaluated[0].resolved_count == 17
    assert evaluated[0].unresolved_count == 3
    assert all(item.alpha_evidence is False and item.candidate_eligible is False for item in evaluated)


def test_future_canary_is_rejected_before_linear_algebra(monkeypatch) -> None:
    calls = 0
    original = np.linalg.solve

    def spy(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(np.linalg, "solve", spy)
    with pytest.raises(ResearchContractError, match="canary rejected before fitting"):
        _dataset(signal=True, feature_names=("future_return", "noise"))
    assert calls == 0


def test_prediction_and_artifact_tamper_fail_closed() -> None:
    dataset = _dataset(signal=True)
    split = split_synthetic_research_run(dataset, _folds(dataset.temporal))
    built = build_synthetic_research_run((split.builder_packets[0],), _candidates())[0]
    with pytest.raises(ResearchContractError, match="manifest ID"):
        replace(
            built.predictions,
            expected_returns=tuple(-item for item in built.predictions.expected_returns),
        ).validate()
    with pytest.raises(
        ResearchContractError, match="invalid JSON|canonical JSON|byte hash"
    ):
        replace(
            built.artifact,
            artifact_bytes=built.artifact.artifact_bytes + b"x",
        ).validate()

    payload = dict(built.artifact.payload())
    parity = dict(payload["parity_output"])
    parity["expected_return"] = float(parity["expected_return"]) + 1.0
    payload["parity_output"] = parity
    artifact_bytes = canonical_bytes(payload) + b"\n"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    artifact_id = sha256_json(
        {
            "artifact_sha256": artifact_sha256,
            "builder_packet_id": built.artifact.builder_packet_id,
            "fold_id": built.artifact.fold_id,
            "outer_fit_audit_id": built.artifact.outer_fit_audit_id,
            "selected_candidate_id": built.artifact.selected_candidate_id,
        }
    )
    with pytest.raises(ResearchContractError, match="reload parity"):
        replace(
            built.artifact,
            artifact_bytes=artifact_bytes,
            artifact_sha256=artifact_sha256,
            artifact_id=artifact_id,
        ).validate()


def test_evaluator_source_has_no_builder_import_or_fit_call() -> None:
    source = (
        Path(__file__).parents[1]
        / "src"
        / "futures_rebuild"
        / "historical_evaluator.py"
    ).read_text(encoding="utf-8")
    assert "historical_builder" not in source
    assert ".fit(" not in source


def test_mechanics_config_matches_code_and_stays_non_authoritative() -> None:
    payload = json.loads(
        (
            Path(__file__).parents[1]
            / "configs"
            / "synthetic_research_engine.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["outcome_generation"]["label_method_id"] == (
        CAUSAL_OUTCOME_LABEL_METHOD_ID
    )
    assert payload["research_roles"]["artifact_format"] == ARTIFACT_FORMAT
    assert payload["authority"] == {
        "alpha_evidence": False,
        "candidate_eligible": False,
        "historical_research_ready": False,
        "real_history_execution_authorized": False,
        "candidate_sealing_authorized": False,
    }
