"""Freeze a bounded Tier 1 chronological Phase 5 split plan.

The builder reads only release identity, timing, session, and status columns.
It never reads feature values or outcome returns, and it writes a plan rather
than a model, prediction, or evaluation artifact.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping

import numpy as np

from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_json
from .errors import IntegrityError
from .research.splits import SessionWindow, TemporalSamples, nested_chronological_splits


MARKETS = ("ES", "CL", "ZN", "6E")
YEARS = tuple(range(2018, 2023))
OUTCOME_METHOD = "active_es_60s_300s_v2"
FEATURE_METHOD = "active_es_mechanical_v3"
SPLIT_SCHEMA_VERSION = "tier1_phase5_split_plan/1.0.0"
INITIAL_TRAIN_SESSIONS = 504
OUTER_TEST_SESSIONS = 63
OUTER_STEP_SESSIONS = 63
OUTER_FOLDS = 8
INNER_FOLDS = 4
INNER_VALIDATION_SESSIONS = 42
SESSION_EMBARGO = 1


@dataclass(frozen=True)
class ReleasePair:
    market: str
    year: int
    outcome_release_id: str
    feature_release_id: str
    outcome_path: Path
    feature_path: Path
    source_parquet_sha256: str


def _load_json(path: Path) -> Mapping[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"Phase 5 manifest is unreadable: {path}") from exc
    if not isinstance(payload, dict):
        raise IntegrityError("Phase 5 manifest must be an object")
    return payload


def discover_tier1_release_pairs(*, boundary: RepoBoundary) -> tuple[ReleasePair, ...]:
    """Bind exactly one Phase 3 and Phase 4 successor per Tier 1 market-year."""
    pairs: list[ReleasePair] = []
    for market in MARKETS:
        for year in YEARS:
            outcome_root = (
                boundary.active_root / "data" / "outcomes" / OUTCOME_METHOD / market / str(year) / str(year)
            )
            feature_root = (
                boundary.active_root / "data" / "features" / FEATURE_METHOD / market / str(year) / str(year)
            )
            outcomes = tuple(path for path in outcome_root.iterdir() if path.is_dir())
            features = tuple(path for path in feature_root.iterdir() if path.is_dir())
            if len(outcomes) != 1 or len(features) != 1:
                raise IntegrityError(f"Phase 5 requires one bound release pair for {market}-{year}")
            outcome_id, feature_id = outcomes[0].name, features[0].name
            outcome_path = outcomes[0] / "outcomes.parquet"
            feature_path = features[0] / "features.parquet"
            if not outcome_path.is_file() or not feature_path.is_file():
                raise IntegrityError(f"Phase 5 release payload is missing for {market}-{year}")
            outcome_manifest = _load_json(
                boundary.active_root / "manifests" / "data_releases" / "outcomes" / f"{outcome_id}.json"
            )
            feature_manifest = _load_json(
                boundary.active_root / "manifests" / "data_releases" / "features" / f"{feature_id}.json"
            )
            source = outcome_manifest.get("source_parquet_sha256")
            if source != feature_manifest.get("source_parquet_sha256") or not isinstance(source, str):
                raise IntegrityError(f"Phase 5 release pair source differs for {market}-{year}")
            pairs.append(ReleasePair(market, year, outcome_id, feature_id, outcome_path, feature_path, source))
    return tuple(pairs)


def _session_counts(
    pairs: Iterable[ReleasePair], *, progress: Callable[[str], None] | None = None
) -> dict[str, int]:
    """Verify pair identity while counting only mature, feature-ready samples."""
    import pyarrow.parquet as pq

    counts: dict[str, int] = {}
    for pair in pairs:
        feature_rows: dict[str, tuple[int, int, int, object, object, object]] = {}
        feature_events: dict[int, str] = {}
        reader = pq.ParquetFile(pair.feature_path)
        columns = [
            "bar_event_at_ns", "decision_at_ns", "label_unlock_at_ns",
            "status", "actual_identity_hash", "exchange_session_date", "upstream_source_row_sha256",
        ]
        if not set(columns).issubset(reader.schema_arrow.names):
            raise IntegrityError(f"Phase 5 feature schema is incomplete for {pair.market}-{pair.year}")
        for batch in reader.iter_batches(batch_size=65_536, columns=columns):
            for row in batch.to_pylist():
                key = row["upstream_source_row_sha256"]
                event = row["bar_event_at_ns"]
                if not isinstance(key, str) or not isinstance(event, int) or key in feature_rows or event in feature_events:
                    raise IntegrityError(f"Phase 5 feature identity is ambiguous for {pair.market}-{pair.year}")
                session = row["exchange_session_date"]
                identity = row["actual_identity_hash"]
                if row["status"] == "FEATURE_READY" and (
                    not isinstance(session, str) or not isinstance(identity, str)
                ):
                    raise IntegrityError(f"Phase 5 ready feature identity is invalid for {pair.market}-{pair.year}")
                feature_rows[key] = (event, row["decision_at_ns"], row["label_unlock_at_ns"], row["status"], session, identity)
                feature_events[event] = session
        matched = 0
        outcome_reader = pq.ParquetFile(pair.outcome_path)
        outcome_columns = [
            "source_bar_event_at_ns", "decision_at_ns", "entry_at_ns", "label_unlock_at_ns",
            "status", "actual_identity_hash", "exchange_session_date", "upstream_source_row_sha256",
        ]
        if not set(outcome_columns).issubset(outcome_reader.schema_arrow.names):
            raise IntegrityError(f"Phase 5 outcome schema is incomplete for {pair.market}-{pair.year}")
        for batch in outcome_reader.iter_batches(batch_size=65_536, columns=outcome_columns):
            for row in batch.to_pylist():
                key = row["upstream_source_row_sha256"]
                feature = feature_rows.get(key)
                if feature is None:
                    raise IntegrityError(f"Phase 5 outcome lacks exact feature match for {pair.market}-{pair.year}")
                event, decision, unlock, feature_status, session, identity = feature
                matched_sample = row["status"] == "MATURED" and feature_status == "FEATURE_READY"
                if (
                    row["source_bar_event_at_ns"] != event
                    or row["decision_at_ns"] != decision
                    or row["label_unlock_at_ns"] != unlock
                    or row["entry_at_ns"] != decision + 60 * 1_000_000_000
                ):
                    raise IntegrityError(f"Phase 5 pair timing or identity differs for {pair.market}-{pair.year}")
                matched += 1
                if matched_sample:
                    if (
                        not isinstance(session, str)
                        or not isinstance(identity, str)
                        or row["actual_identity_hash"] != identity
                        or row["exchange_session_date"] != session
                    ):
                        raise IntegrityError(f"Phase 5 mature sample identity differs for {pair.market}-{pair.year}")
                    if unlock not in feature_events:
                        raise IntegrityError(f"Phase 5 matured label endpoint is unobserved for {pair.market}-{pair.year}")
                    counts[session] = counts.get(session, 0) + 1
        if matched != len(feature_rows):
            raise IntegrityError(f"Phase 5 pair row counts differ for {pair.market}-{pair.year}")
        if progress is not None:
            progress(f"verified {pair.market}-{pair.year}")
    if not counts:
        raise IntegrityError("Phase 5 has no mature, feature-ready samples")
    return counts


def _schedule(session_dates: tuple[str, ...]) -> tuple[dict[str, object], ...]:
    first_outer_start = INITIAL_TRAIN_SESSIONS + SESSION_EMBARGO + INNER_FOLDS * INNER_VALIDATION_SESSIONS + 1
    windows = tuple(
        SessionWindow(first_outer_start + index * OUTER_STEP_SESSIONS, first_outer_start + index * OUTER_STEP_SESSIONS + OUTER_TEST_SESSIONS)
        for index in range(OUTER_FOLDS)
    )
    if windows[-1].stop > len(session_dates):
        raise IntegrityError("Tier 1 history is too short for the required eight-fold Phase 5 schedule")
    inner = tuple(
        tuple(
            SessionWindow(
                outer.start - SESSION_EMBARGO - INNER_FOLDS * INNER_VALIDATION_SESSIONS + number * INNER_VALIDATION_SESSIONS,
                outer.start - SESSION_EMBARGO - (INNER_FOLDS - number - 1) * INNER_VALIDATION_SESSIONS,
            )
            for number in range(INNER_FOLDS)
        )
        for outer in windows
    )
    temporal = TemporalSamples(
        np.arange(len(session_dates), dtype=np.int64),
        np.arange(len(session_dates), dtype=np.int64),
        np.arange(1, len(session_dates) + 1, dtype=np.int64),
        np.arange(1, len(session_dates) + 1, dtype=np.int64),
    )
    folds = nested_chronological_splits(
        temporal, windows, inner, session_embargo=SESSION_EMBARGO, minimum_fit_samples=1, minimum_audit_samples=1
    )
    return tuple(
        {
            "outer_test_session_dates": [session_dates[fold.test_window.start], session_dates[fold.test_window.stop - 1]],
            "outer_fit_session_range": [session_dates[0], session_dates[fold.test_window.start - SESSION_EMBARGO - 1]],
            "inner_validation_session_dates": [
                [session_dates[item.validation_window.start], session_dates[item.validation_window.stop - 1]]
                for item in fold.inner_folds
            ],
        }
        for fold in folds
    )


def build_tier1_phase5_split_plan(
    *, boundary: RepoBoundary, pairs: Iterable[ReleasePair] | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Create one immutable Phase 5 schedule for the exact Tier 1 release set."""
    bound_pairs = tuple(discover_tier1_release_pairs(boundary=boundary) if pairs is None else pairs)
    if not bound_pairs:
        raise IntegrityError("Phase 5 requires bound release pairs")
    counts = _session_counts(bound_pairs, progress=progress)
    session_dates = tuple(sorted(counts))
    folds = _schedule(session_dates)
    input_pairs = [
        {"market": pair.market, "year": pair.year, "outcome_release_id": pair.outcome_release_id, "feature_release_id": pair.feature_release_id, "source_parquet_sha256": pair.source_parquet_sha256}
        for pair in bound_pairs
    ]
    core = {
        "schema_version": SPLIT_SCHEMA_VERSION,
        "input_pairs": input_pairs,
        "session_dates": list(session_dates),
        "mature_feature_ready_sample_counts": {date: counts[date] for date in session_dates},
        "outer_folds": list(folds),
        "schedule": {
            "initial_train_sessions": INITIAL_TRAIN_SESSIONS,
            "outer_test_sessions": OUTER_TEST_SESSIONS,
            "outer_step_sessions": OUTER_STEP_SESSIONS,
            "outer_folds": OUTER_FOLDS,
            "inner_folds": INNER_FOLDS,
            "inner_validation_sessions": INNER_VALIDATION_SESSIONS,
            "session_embargo": SESSION_EMBARGO,
            "interval_convention": "half_open",
            "purge_rule": "remove_train_sample_when_train_label_interval_intersects_any_validation_or_test_label_interval",
            "holdout_and_forward_excluded": True,
        },
    }
    plan_id = sha256_json(core)
    manifest = boundary.active_root / "manifests" / "split_plans" / "tier1_core" / f"{plan_id}.json"
    report = boundary.active_root / "reports" / "phase5_splits" / "tier1_core" / plan_id / "report.json"
    if manifest.exists() or report.exists():
        raise IntegrityError("Phase 5 split-plan target already exists")
    manifest.parent.mkdir(parents=True, exist_ok=True)
    report.parent.mkdir(parents=True)
    payload = {**core, "plan_id": plan_id}
    manifest.write_bytes(canonical_bytes(payload) + b"\n")
    report.write_bytes(canonical_bytes({"plan_id": plan_id, "phase": 5, "pair_count": len(bound_pairs), "session_count": len(session_dates), "mature_feature_ready_sample_count": sum(counts.values()), "outer_fold_count": len(folds), "model_fitting": False, "prediction_generation": False, "economics_evaluation": False, "holdout_and_forward_excluded": True}) + b"\n")
    return {"plan_id": plan_id, "manifest_path": manifest.relative_to(boundary.active_root).as_posix(), "report_path": report.relative_to(boundary.active_root).as_posix(), "session_count": len(session_dates), "sample_count": sum(counts.values())}
