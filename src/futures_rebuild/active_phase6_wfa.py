"""Offline preparation for the Tier 1 Phase 6 walk-forward workflow.

This module intentionally binds releases and describes the future trial before
any historical feature/outcome rows, model code, or prediction outputs are
opened.  Calling :meth:`Tier1Phase6Runner.execute` always fails closed: actual
walk-forward fitting belongs to a separately approved real-history attempt.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

import numpy as np

from .active_phase5_splits import ReleasePair, discover_tier1_release_pairs
from .boundary import RepoBoundary
from .canonical import sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


PHASE6_SCHEMA_VERSION = "tier1_phase6_wfa_preflight/1.0.0"
RUNNER_FAMILY = "tier1_nested_chronological_wfa/1.0.0"
TRIAL_TEMPLATE_SCHEMA_VERSION = "tier1_phase6_trial_template/1.0.0"


def _load_object(path: Path) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"Phase 6 binding is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("Phase 6 binding must be a JSON object")
    return value


def _pair_record(pair: ReleasePair) -> dict[str, object]:
    return {
        "feature_release_id": pair.feature_release_id,
        "market": pair.market,
        "outcome_release_id": pair.outcome_release_id,
        "source_parquet_sha256": pair.source_parquet_sha256,
        "year": pair.year,
    }


@dataclass(frozen=True)
class Tier1Phase6Binding:
    """Exact inputs and safeguards needed to prepare, not execute, Phase 6."""

    plan_id: str
    phase5_manifest_path: Path
    phase5_manifest_sha256: str
    input_pairs: tuple[dict[str, object], ...]
    outer_fold_count: int

    def trial_declaration_template(self) -> dict[str, object]:
        """Return an unsigned, unregistered declaration for later approval."""
        core = {
            "schema_version": TRIAL_TEMPLATE_SCHEMA_VERSION,
            "phase": 6,
            "phase6_plan_id": self.plan_id,
            "runner_family": RUNNER_FAMILY,
            "phase5_manifest": self.phase5_manifest_path.as_posix(),
            "phase5_manifest_sha256": self.phase5_manifest_sha256,
            "input_pairs": list(self.input_pairs),
            "outer_fold_count": self.outer_fold_count,
            "model_family": "UNSPECIFIED_UNTIL_REAL_HISTORY_APPROVAL",
            "hyperparameter_budget": "UNSPECIFIED_UNTIL_REAL_HISTORY_APPROVAL",
            "prediction_release": "NOT_CREATED",
            "trial_registration": "NOT_REGISTERED",
            "forbidden_without_new_approval": [
                "provider_access",
                "historical_feature_or_outcome_row_read",
                "model_fit",
                "prediction_materialization",
                "economics_evaluation",
                "trial_registration",
                "installation",
                "trading",
                "push",
            ],
        }
        return {**core, "template_id": sha256_json(core)}


def prepare_tier1_phase6_binding(*, boundary: RepoBoundary) -> Tier1Phase6Binding:
    """Verify the committed Phase 5 plan and all 20 metadata-only release pairs."""
    manifest_root = boundary.active_root / "manifests" / "split_plans" / "tier1_core"
    manifests = tuple(sorted(path for path in manifest_root.glob("*.json") if path.is_file()))
    if len(manifests) != 1:
        raise IntegrityError("Phase 6 requires exactly one Tier 1 Phase 5 manifest")
    phase5_path = manifests[0]
    phase5 = _load_object(phase5_path)
    plan_pairs = phase5.get("input_pairs")
    folds = phase5.get("outer_folds")
    if not isinstance(plan_pairs, list) or not isinstance(folds, list) or len(folds) != 8:
        raise IntegrityError("Phase 6 requires the committed eight-fold Phase 5 plan")

    discovered = tuple(_pair_record(pair) for pair in discover_tier1_release_pairs(boundary=boundary))
    if len(discovered) != 20 or plan_pairs != list(discovered):
        raise IntegrityError("Phase 5 inputs do not exactly bind the current Tier 1 releases")

    manifest_rel = phase5_path.relative_to(boundary.active_root)
    core = {
        "schema_version": PHASE6_SCHEMA_VERSION,
        "runner_family": RUNNER_FAMILY,
        "phase5_manifest": manifest_rel.as_posix(),
        "phase5_manifest_sha256": sha256_file(phase5_path),
        "input_pairs": list(discovered),
        "outer_fold_count": len(folds),
    }
    return Tier1Phase6Binding(
        plan_id=sha256_json(core),
        phase5_manifest_path=manifest_rel,
        phase5_manifest_sha256=str(core["phase5_manifest_sha256"]),
        input_pairs=discovered,
        outer_fold_count=len(folds),
    )


@dataclass(frozen=True)
class Tier1Phase6Runner:
    """A deliberately non-executing gateway for a later authorized WFA run."""

    binding: Tier1Phase6Binding

    def execute(self) -> None:
        raise UnauthorizedOperation(
            "Tier 1 Phase 6 real-history fitting requires a registered, separately approved trial"
        )


@dataclass
class Phase6PredictionOnlyTrialContract:
    """A narrow pre-row-open declaration contract for Phase 6 predictions.

    This does not replace :class:`ExperimentCharter` or its economics and
    holdout controls.  It covers only an already-approved, prediction-only
    Phase 6 run and models the ordering rule required by that run: registration
    must happen before outcome rows can be opened.
    """

    binding: Tier1Phase6Binding
    model_family: str = "RIDGE_LINEAR_ALL_MECHANICAL_FEATURES"
    ridge_penalty: float = 1.0
    seed: int = 106
    registered: bool = False
    outcome_rows_opened: bool = False

    def __post_init__(self) -> None:
        if (
            self.model_family != "RIDGE_LINEAR_ALL_MECHANICAL_FEATURES"
            or type(self.ridge_penalty) is not float
            or self.ridge_penalty != 1.0
            or type(self.seed) is not int
            or self.seed != 106
            or self.registered
            or self.outcome_rows_opened
        ):
            raise IntegrityError("Phase 6 prediction-only contract must use the fixed conservative trial")

    def declaration(self) -> dict[str, object]:
        core = {
            "schema_version": "tier1_phase6_prediction_only_trial/1.0.0",
            "phase": 6,
            "phase6_plan_id": self.binding.plan_id,
            "phase5_manifest": self.binding.phase5_manifest_path.as_posix(),
            "phase5_manifest_sha256": self.binding.phase5_manifest_sha256,
            "input_pairs": list(self.binding.input_pairs),
            "outer_fold_count": self.binding.outer_fold_count,
            "model_family": self.model_family,
            "ridge_penalty": self.ridge_penalty,
            "seed": self.seed,
            "hyperparameter_search": False,
            "prediction_only": True,
            "economics_evaluation": False,
            "holdout_or_forward_access": False,
            "registration_must_precede_outcome_row_open": True,
        }
        return {**core, "trial_id": sha256_json(core)}

    def register_in_memory(self) -> None:
        """Advance only the local state machine; persistent registration is later."""
        if self.outcome_rows_opened:
            raise UnauthorizedOperation("cannot register after Phase 3 outcome rows were opened")
        self.registered = True

    def authorize_outcome_row_open(self) -> None:
        if not self.registered:
            raise UnauthorizedOperation("Phase 6 registration must precede Phase 3 outcome row access")
        self.outcome_rows_opened = True


def prepare_phase6_prediction_only_trial(*, binding: Tier1Phase6Binding) -> Phase6PredictionOnlyTrialContract:
    """Construct the fixed trial contract without persistence or data access."""
    return Phase6PredictionOnlyTrialContract(binding=binding)


def _fold_dates(phase5: Mapping[str, object]) -> tuple[tuple[str, str, tuple[str, ...]], ...]:
    folds = phase5.get("outer_folds")
    if not isinstance(folds, list) or len(folds) != 8:
        raise IntegrityError("Phase 6 requires exactly eight Phase 5 outer folds")
    result = []
    for fold in folds:
        if not isinstance(fold, dict):
            raise IntegrityError("Phase 5 fold is invalid")
        fit = fold.get("outer_fit_session_range")
        test = fold.get("outer_test_session_dates")
        if not (isinstance(fit, list) and len(fit) == 2 and isinstance(test, list) and len(test) == 2):
            raise IntegrityError("Phase 5 fold ranges are invalid")
        result.append((str(fit[0]), str(fit[1]), (str(test[0]), str(test[1]))))
    return tuple(result)


def _ridge_from_sufficient_statistics(xtx: np.ndarray, xty: np.ndarray) -> np.ndarray:
    penalty = np.eye(xtx.shape[0], dtype=np.float64)
    penalty[0, 0] = 0.0
    try:
        return np.linalg.solve(xtx + penalty, xty)
    except np.linalg.LinAlgError as exc:
        raise IntegrityError("Phase 6 ridge system is singular") from exc


def run_tier1_phase6_prediction_only_wfa(*, boundary: RepoBoundary, maximum_seconds: int = 1_200) -> dict[str, object]:
    """Run the one fixed, bounded prediction-only WFA after persistent registration.

    This reads the four fixed Phase 4 numeric features and the Phase 3 return
    only after the create-only registration document exists.  It uses sufficient
    statistics, so each bound pair is opened once rather than once per fold.
    """
    from .current_research_surface import reject_retired_real_history_surface

    del boundary, maximum_seconds
    reject_retired_real_history_surface("legacy Tier 1 Phase 6 WFA")
    raise AssertionError("unreachable")


def _retired_run_tier1_phase6_prediction_only_wfa(*, boundary: RepoBoundary, maximum_seconds: int = 1_200) -> dict[str, object]:
    """Preserved implementation body; unreachable from current research."""

    started = time.monotonic()
    binding = prepare_tier1_phase6_binding(boundary=boundary)
    contract = prepare_phase6_prediction_only_trial(binding=binding)
    trial = contract.declaration()
    trial_id = str(trial["trial_id"])
    registry = boundary.active_root / "state" / "trial_registry" / "phase6_prediction_only" / f"{trial_id}.json"
    event = boundary.active_root / "state" / "trial_events" / "phase6_prediction_only" / f"{trial_id}.json"
    if registry.exists() or event.exists():
        raise IntegrityError("Phase 6 prediction-only trial registration already exists")
    registration = {**trial, "registered_at_utc": datetime.now(timezone.utc).isoformat(), "state": "REGISTERED_BEFORE_OUTCOME_OPEN"}
    for path, payload in ((registry, registration), (event, {"event_type": "DECLARED", "trial_id": trial_id, "registered_at_utc": registration["registered_at_utc"]})):
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
        try:
            os.write(descriptor, json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    contract.register_in_memory()
    contract.authorize_outcome_row_open()

    phase5_path = boundary.active_root / binding.phase5_manifest_path
    phase5 = _load_object(phase5_path)
    folds = _fold_dates(phase5)
    temporary = boundary.active_root / ".pytest_tmp" / "workflow" / trial_id
    temporary.mkdir(parents=True, exist_ok=False)
    xtx = [np.zeros((5, 5), dtype=np.float64) for _ in folds]
    xty = [np.zeros(5, dtype=np.float64) for _ in folds]
    prediction_rows = 0
    matched_rows = 0
    import pyarrow as pa
    import pyarrow.parquet as pq

    for pair in discover_tier1_release_pairs(boundary=boundary):
        if time.monotonic() - started > maximum_seconds:
            raise TimeoutError("Phase 6 duration ceiling exceeded")
        outcomes: dict[str, tuple[float, str, str, int]] = {}
        outcome_reader = pq.ParquetFile(pair.outcome_path)
        for batch in outcome_reader.iter_batches(batch_size=65_536, columns=["status", "price_return", "exchange_session_date", "actual_identity_hash", "decision_at_ns", "upstream_source_row_sha256"]):
            for row in batch.to_pylist():
                if row["status"] == "MATURED" and row["price_return"] is not None:
                    outcomes[str(row["upstream_source_row_sha256"])] = (float(row["price_return"]), str(row["exchange_session_date"]), str(row["actual_identity_hash"]), int(row["decision_at_ns"]))
        feature_reader = pq.ParquetFile(pair.feature_path)
        for batch in feature_reader.iter_batches(batch_size=65_536, columns=["status", "exchange_session_date", "actual_identity_hash", "decision_at_ns", "upstream_source_row_sha256", "bar_body_fraction", "bar_return", "intrabar_range_fraction", "volume"]):
            rows = batch.to_pylist()
            joined = []
            for row in rows:
                matched = outcomes.get(str(row["upstream_source_row_sha256"]))
                if row["status"] != "FEATURE_READY" or matched is None:
                    continue
                if (
                    str(row["exchange_session_date"]) != matched[1]
                    or str(row["actual_identity_hash"]) != matched[2]
                    or int(row["decision_at_ns"]) != matched[3]
                ):
                    raise IntegrityError("Phase 6 feature/outcome identity or timing mismatch")
                joined.append(row)
            if not joined:
                continue
            matched_rows += len(joined)
            if matched_rows > 6_900_000:
                raise IntegrityError("Phase 6 row ceiling exceeded")
            x = np.asarray([[1.0, float(row["bar_body_fraction"]), float(row["bar_return"]), float(row["intrabar_range_fraction"]), float(row["volume"])] for row in joined], dtype=np.float64)
            y = np.asarray([outcomes[str(row["upstream_source_row_sha256"])][0] for row in joined], dtype=np.float64)
            dates = np.asarray([str(row["exchange_session_date"]) for row in joined], dtype=object)
            for index, (fit_start, fit_end, (test_start, test_end)) in enumerate(folds):
                train = (dates >= fit_start) & (dates <= fit_end)
                if train.any():
                    xtx[index] += x[train].T @ x[train]
                    xty[index] += x[train].T @ y[train]
                test = (dates >= test_start) & (dates <= test_end)
                if test.any():
                    count = int(test.sum())
                    prediction_rows += count
                    if prediction_rows > 3_000_000:
                        raise IntegrityError("Phase 6 prediction ceiling exceeded")
                    table = pa.table({"market": [pair.market] * count, "year": [pair.year] * count, "outer_fold": [index] * count, "exchange_session_date": dates[test].tolist(), "actual_identity_hash": [str(row["actual_identity_hash"]) for row, keep in zip(joined, test) if keep], "decision_at_ns": [int(row["decision_at_ns"]) for row, keep in zip(joined, test) if keep], "upstream_source_row_sha256": [str(row["upstream_source_row_sha256"]) for row, keep in zip(joined, test) if keep], "feature_0": x[test, 1], "feature_1": x[test, 2], "feature_2": x[test, 3], "feature_3": x[test, 4]})
                    chunk = temporary / f"fold-{index}-{pair.market}-{pair.year}-{time.monotonic_ns()}.parquet"
                    pq.write_table(table, chunk)
    models = [_ridge_from_sufficient_statistics(a, b) for a, b in zip(xtx, xty)]
    staged_prediction = temporary / "predictions.parquet"
    writer = None
    try:
        for chunk in sorted(temporary.glob("fold-*.parquet")):
            table = pq.read_table(chunk)
            fold = table.column("outer_fold").to_numpy()
            features = np.column_stack([np.ones(table.num_rows), table.column("feature_0").to_numpy(), table.column("feature_1").to_numpy(), table.column("feature_2").to_numpy(), table.column("feature_3").to_numpy()])
            prediction = np.empty(table.num_rows, dtype=np.float64)
            for index, model in enumerate(models):
                mask = fold == index
                prediction[mask] = features[mask] @ model
            table = table.drop(["feature_0", "feature_1", "feature_2", "feature_3"]).append_column("prediction", pa.array(prediction))
            if writer is None:
                writer = pq.ParquetWriter(staged_prediction, table.schema, compression="zstd")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    if prediction_rows == 0 or not staged_prediction.is_file() or time.monotonic() - started > maximum_seconds:
        raise IntegrityError("Phase 6 produced no valid prediction payload")
    payload_hash = sha256_file(staged_prediction)
    release_id = sha256_json({"trial_id": trial_id, "payload_sha256": payload_hash, "prediction_rows": prediction_rows})
    output = boundary.active_root / "data" / "predictions" / "tier1_phase6_conservative" / release_id / "predictions.parquet"
    manifest = boundary.active_root / "manifests" / "data_releases" / "predictions" / f"{release_id}.json"
    report = boundary.active_root / "reports" / "phase6_wfa" / "tier1_phase6_conservative" / release_id / "report.json"
    if output.exists() or manifest.exists() or report.exists():
        raise IntegrityError("Phase 6 immutable output collision")
    output.parent.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(staged_prediction, output)
    core = {"schema_version": "tier1_phase6_prediction_release/1.0.0", "release_id": release_id, "trial_id": trial_id, "phase5_manifest_sha256": binding.phase5_manifest_sha256, "input_pairs": list(binding.input_pairs), "payload": output.relative_to(boundary.active_root).as_posix(), "payload_sha256": sha256_file(output), "prediction_rows": prediction_rows, "outer_fold_count": 8, "model": {"family": contract.model_family, "ridge_penalty": 1.0, "seed": 106}, "prediction_only": True}
    for path, payload in ((manifest, core), (report, {"phase": 6, "release_id": release_id, "trial_id": trial_id, "prediction_rows": prediction_rows, "outer_fold_count": 8, "model_fitting": True, "economics_evaluation": False, "holdout_or_forward_access": False})):
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
        try:
            os.write(descriptor, json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return {"trial_id": trial_id, "release_id": release_id, "prediction_rows": prediction_rows, "manifest_path": manifest.relative_to(boundary.active_root).as_posix(), "report_path": report.relative_to(boundary.active_root).as_posix()}
