"""Half-open purging and nested expanding chronological futures splits."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np

from .contracts import (
    ResearchContractError,
    assert_disjoint_partitions,
    explicit_int,
    int64_vector,
)


@dataclass(frozen=True)
class TemporalSamples:
    decision_session: np.ndarray
    label_start: np.ndarray
    label_end: np.ndarray
    label_known_session: np.ndarray

    def validate(self) -> int:
        arrays = (
            int64_vector(self.decision_session, name="decision_session"),
            int64_vector(self.label_start, name="label_start"),
            int64_vector(self.label_end, name="label_end"),
            int64_vector(self.label_known_session, name="label_known_session"),
        )
        if len({len(value) for value in arrays}) != 1:
            raise ResearchContractError("temporal arrays must have equal length")
        if bool(np.any(self.label_end <= self.label_start)):
            raise ResearchContractError("label intervals require start < end")
        if bool(np.any(np.diff(self.decision_session) < 0)):
            raise ResearchContractError("samples must be decision-session ordered")
        if bool(np.any(self.label_known_session < self.decision_session)):
            raise ResearchContractError("labels cannot be known before decisions")
        if bool(np.any(self.label_known_session < self.label_end)):
            raise ResearchContractError("labels cannot be known before their intervals end")
        return len(self.decision_session)


@dataclass(frozen=True, order=True)
class SessionWindow:
    start: int
    stop: int

    def validate(self) -> None:
        start = explicit_int(self.start, name="window.start")
        stop = explicit_int(self.stop, name="window.stop")
        if start >= stop:
            raise ResearchContractError("session windows are half-open with start < stop")


@dataclass(frozen=True)
class InnerFold:
    validation_window: SessionWindow
    fit_indices: np.ndarray
    audit_indices: np.ndarray


@dataclass(frozen=True)
class NestedFold:
    test_window: SessionWindow
    fit_indices: np.ndarray
    audit_indices: np.ndarray
    inner_folds: tuple[InnerFold, ...]


def _checked_indices(indices: np.ndarray, *, n: int, name: str) -> np.ndarray:
    values = int64_vector(indices, name=name)
    if bool(np.any(values < 0)) or bool(np.any(values >= n)):
        raise ResearchContractError(f"{name} contains an out-of-range index")
    if len(np.unique(values)) != len(values):
        raise ResearchContractError(f"{name} contains duplicates")
    return values


def purge_and_post_embargo_indices(
    samples: TemporalSamples,
    candidate_indices: np.ndarray,
    heldout_indices: np.ndarray,
    *,
    post_embargo_sessions: int,
) -> np.ndarray:
    n = samples.validate()
    candidates = _checked_indices(candidate_indices, n=n, name="candidate_indices")
    heldout = _checked_indices(heldout_indices, n=n, name="heldout_indices")
    embargo = explicit_int(post_embargo_sessions, name="post_embargo_sessions")
    if embargo < 0:
        raise ResearchContractError("post_embargo_sessions cannot be negative")
    if embargo and bool(
        np.any(samples.label_end[heldout] > np.iinfo(np.int64).max - embargo)
    ):
        raise ResearchContractError("post-embargo interval overflows int64")
    keep = np.ones(len(candidates), dtype=np.bool_)
    for position, candidate in enumerate(candidates):
        start = samples.label_start[candidate]
        end = samples.label_end[candidate]
        decision = samples.decision_session[candidate]
        overlap = np.any(
            (start < samples.label_end[heldout])
            & (samples.label_start[heldout] < end)
        )
        embargoed = np.any(
            (samples.label_end[heldout] <= decision)
            & (decision < samples.label_end[heldout] + np.int64(embargo))
        )
        keep[position] = not bool(overlap or embargoed)
    return candidates[keep]


def _window_indices(samples: TemporalSamples, window: SessionWindow) -> np.ndarray:
    window.validate()
    return np.flatnonzero(
        (samples.decision_session >= window.start)
        & (samples.decision_session < window.stop)
    ).astype(np.int64)


def _strict_past_fit(
    samples: TemporalSamples,
    heldout: np.ndarray,
    window: SessionWindow,
    *,
    session_embargo: int,
) -> np.ndarray:
    cutoff = window.start - session_embargo
    candidates = np.flatnonzero(
        (samples.decision_session < cutoff)
        & (samples.label_known_session < window.start)
    ).astype(np.int64)
    return purge_and_post_embargo_indices(
        samples, candidates, heldout, post_embargo_sessions=0
    )


def nested_chronological_splits(
    samples: TemporalSamples,
    outer_test_windows: Sequence[SessionWindow],
    inner_validation_windows: Sequence[Sequence[SessionWindow]],
    *,
    session_embargo: int,
    minimum_fit_samples: int,
    minimum_audit_samples: int,
) -> tuple[NestedFold, ...]:
    samples.validate()
    embargo = explicit_int(session_embargo, name="session_embargo")
    minimum_fit = explicit_int(minimum_fit_samples, name="minimum_fit_samples")
    minimum_audit = explicit_int(minimum_audit_samples, name="minimum_audit_samples")
    if embargo < 0 or minimum_fit < 1 or minimum_audit < 1:
        raise ResearchContractError("embargo/minimum sample settings are invalid")
    if not outer_test_windows or len(outer_test_windows) != len(inner_validation_windows):
        raise ResearchContractError("each outer window needs an inner schedule")
    previous_stop: int | None = None
    result: list[NestedFold] = []
    for outer_window, inner_schedule in zip(
        outer_test_windows, inner_validation_windows, strict=True
    ):
        outer_window.validate()
        if previous_stop is not None and outer_window.start < previous_stop:
            raise ResearchContractError("outer audit windows overlap or reverse")
        previous_stop = outer_window.stop
        audit = _window_indices(samples, outer_window)
        fit = _strict_past_fit(samples, audit, outer_window, session_embargo=embargo)
        if len(fit) < minimum_fit or len(audit) < minimum_audit:
            raise ResearchContractError("outer fold misses declared sample minima")
        assert_disjoint_partitions(fit, audit)
        fit_set = set(int(value) for value in fit.tolist())
        inner_folds: list[InnerFold] = []
        prior_inner_stop: int | None = None
        for validation_window in inner_schedule:
            validation_window.validate()
            if validation_window.stop > outer_window.start - embargo:
                raise ResearchContractError("inner validation enters outer embargo/audit")
            if prior_inner_stop is not None and validation_window.start < prior_inner_stop:
                raise ResearchContractError("inner audit windows overlap or reverse")
            prior_inner_stop = validation_window.stop
            inner_audit = _window_indices(samples, validation_window)
            if not set(int(value) for value in inner_audit.tolist()).issubset(fit_set):
                raise ResearchContractError("inner audit escapes outer fit set")
            inner_fit = _strict_past_fit(
                samples, inner_audit, validation_window, session_embargo=embargo
            )
            if not set(int(value) for value in inner_fit.tolist()).issubset(fit_set):
                raise ResearchContractError("inner fit escapes purged outer fit set")
            if len(inner_fit) < minimum_fit or len(inner_audit) < minimum_audit:
                raise ResearchContractError("inner fold misses declared sample minima")
            assert_disjoint_partitions(inner_fit, inner_audit, audit)
            inner_folds.append(InnerFold(validation_window, inner_fit, inner_audit))
        if not inner_folds:
            raise ResearchContractError("at least one inner fold is required")
        result.append(NestedFold(outer_window, fit, audit, tuple(inner_folds)))
    return tuple(result)
