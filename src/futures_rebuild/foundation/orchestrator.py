"""Restart-safe, offline orchestration of the non-alpha futures foundation.

This module deliberately stops at mechanically labelable causal bars.  It does
not read a prediction ledger, create outcomes, fit a model, run WFA, contact a
provider, or make an alpha claim.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Mapping

from ..boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from ..canonical import (
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    sha256_file,
    sha256_json,
)
from ..errors import ContractError, IntegrityError
from ..locking import FileLease
from ..producer_bridge import (
    DEFINITION_RELEASE_KIND,
    ECONOMICS_RELEASE_KIND,
    FEATURE_RELEASE_KIND,
    SESSION_RELEASE_KIND,
    CausalFeatureSpec,
    load_actual_contract_definitions,
    load_actual_contract_economics,
    load_causal_feature_release,
    load_versioned_session_policy,
    publish_actual_contract_definitions,
    publish_actual_contract_economics,
    publish_causal_feature_release,
    publish_versioned_session_policy,
)
from ..release import AtomicPublisher, ReleaseManifest, VerifiedReleaseReceipt
from .materialize import (
    CAUSAL_RELEASE_KIND,
    RAW_RELEASE_KIND,
    load_causal_interval,
    load_raw_interval,
    materialize_causal_interval,
    materialize_raw_interval,
)
from .market_state import (
    MARKET_STATE_RELEASE_KIND,
    STATUS_ELIGIBILITY_RELEASE_KIND,
    FoundationCoveragePolicy,
    LoadedMarketStateFoundation,
    StatisticsRolePolicy,
    load_market_state_foundation,
    load_status_eligibility,
    publish_market_state_foundation,
    publish_status_eligibility,
    status_eligible_decision_keys,
)
from .records import datetime_to_ns
from .selection import (
    SELECTION_RELEASE_KIND,
    load_source_selection,
    resolve_foundation_selection,
)
from .snapshot import PublishedSourceSnapshot
from .support import (
    POLICY_RELEASE_KIND,
    VerifiedFoundationPolicies,
    publish_foundation_policies,
)


CHECKPOINT_VERSION = "2.0.0"
FOUNDATION_SET_RELEASE_KIND = "futures_mechanical_foundation_set"
FOUNDATION_SET_SCHEMA_VERSION = "2.0.0"
OUTCOME_SOURCE_RELEASE_KIND = "futures_outcome_source_input"
OUTCOME_SOURCE_SCHEMA_VERSION = "1.0.0"
OUTCOME_SOURCE_ROLE = "LABELABLE_VERIFIED_CAUSAL_BARS_ONLY"
OUTCOME_DEFERRED_UNTIL = (
    "SEPARATELY_AUTHORIZED_PREDECLARED_SAMPLE_OR_PREDICTION_CONTRACT"
)
_RECEIPT_PHASES = (
    "raw",
    "definitions",
    "causal",
    "status_eligibility",
    "economics",
    "feature_input",
)
_CLOSURE_MODULES = (
    "boundary.py",
    "canonical.py",
    "economics.py",
    "foundation/decoder.py",
    "foundation/economics.py",
    "foundation/identity.py",
    "foundation/materialize.py",
    "foundation/market_state.py",
    "foundation/orchestrator.py",
    "foundation/parquet.py",
    "foundation/pipeline.py",
    "foundation/policy.py",
    "foundation/records.py",
    "foundation/selection.py",
    "foundation/snapshot.py",
    "foundation/support.py",
    "identity.py",
    "locking.py",
    "producer_bridge.py",
    "release.py",
    "session_policy.py",
    "source_symbology.py",
)
_CONFIG_FILES = (
    "contract_economics_rules.json",
    "environment.lock.json",
    "foundation_policy.json",
    "foundation_coverage_policy.json",
    "known_anomalies.json",
    "mechanical_feature_spec.json",
    "session_policy.json",
    "statistics_foundation_roles.json",
)


@dataclass(frozen=True)
class FoundationRunResult:
    run_id: str
    checkpoint_path: Path
    completed_phase_count: int
    foundation_set_receipt: VerifiedReleaseReceipt

    def as_dict(self) -> dict[str, object]:
        return {
            "checkpoint_path": str(self.checkpoint_path),
            "completed_phase_count": self.completed_phase_count,
            "foundation_set_receipt": self.foundation_set_receipt.as_dict(),
            "run_id": self.run_id,
            "status": "COMPLETE_DEPENDENCY_CLOSED_NON_ALPHA",
        }


def _read_canonical_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        assert_plain_file(path)
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} JSON is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def _atomic_checkpoint(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_bytes(dict(payload)) + b"\n"
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    descriptor = None
    try:
        descriptor = os.open(
            temporary,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        )
        remaining = memoryview(encoded)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("checkpoint write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _receipt(payload: object, *, name: str) -> VerifiedReleaseReceipt:
    if not isinstance(payload, dict):
        raise IntegrityError(f"{name} checkpoint receipt is invalid")
    try:
        return VerifiedReleaseReceipt.from_dict(payload)
    except IntegrityError as exc:
        raise IntegrityError(f"{name} checkpoint receipt is invalid") from exc


def _interval_key(market: str, year: int, start: str, end: str) -> str:
    key = f"{market}/{year}/{start}_{end}"
    if (
        re.fullmatch(
            r"[0-9A-Z]{2,3}/\d{4}/\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}",
            key,
        )
        is None
    ):
        raise IntegrityError("selected interval key is invalid")
    return key


def _implementation_closure() -> dict[str, str]:
    source_root = Path(__file__).resolve().parents[1]
    return {relative: sha256_file(source_root / relative) for relative in _CLOSURE_MODULES}


def _config_closure(boundary: RepoBoundary) -> dict[str, str]:
    root = boundary.active_root / "configs"
    return {name: sha256_file(root / name) for name in _CONFIG_FILES}


def _verify_selection_index_bindings(
    selection: Mapping[str, object], snapshot: PublishedSourceSnapshot
) -> None:
    raw_files = selection.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        raise IntegrityError("source selection has no exact selected-file index")
    seen: set[str] = set()
    for raw in raw_files:
        if not isinstance(raw, dict):
            raise IntegrityError("source selection file binding is invalid")
        declared = raw.get("path")
        if type(declared) is not str or Path(declared).as_posix() != declared:
            raise IntegrityError("source selection path is not canonical")
        try:
            relative = Path(declared).relative_to(Path("data") / "dbn")
        except ValueError as exc:
            raise IntegrityError("source selection path is outside logical data/dbn") from exc
        snapshot_relative = (Path("dbn") / relative).as_posix()
        if snapshot_relative in seen:
            raise IntegrityError("source selection contains a duplicate file binding")
        seen.add(snapshot_relative)
        binding = snapshot.file(snapshot_relative)
        if raw.get("sha256") != binding.sha256 or raw.get("size") != binding.size:
            raise IntegrityError("source selection differs from the verified snapshot index")


def _feature_ready_join_keys(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> frozenset[tuple[str, int, int]]:
    manifest = receipt.verify(boundary)
    if manifest.release_kind != FEATURE_RELEASE_KIND:
        raise IntegrityError("feature join index requires an exact feature release")
    path = boundary.active_root / receipt.relative_root / "feature_rows.jsonl"
    keys: set[tuple[str, int, int]] = set()
    try:
        with path.open("rb") as handle:
            for line in handle:
                row = json.loads(line.decode("utf-8"))
                if (
                    not isinstance(row, dict)
                    or line != canonical_bytes(row) + b"\n"
                ):
                    raise IntegrityError("feature join source is not canonical JSONL")
                if row.get("status") != "FEATURE_READY":
                    continue
                identity_hash = row.get("upstream_foundation_actual_identity_hash")
                if re.fullmatch(r"[0-9a-f]{64}", str(identity_hash)) is None:
                    raise IntegrityError("feature join source identity is invalid")
                key = (
                    str(identity_hash),
                    datetime_to_ns(
                        datetime.fromisoformat(row["bar_event_at"]),
                        "feature.bar_event_at",
                    ),
                    datetime_to_ns(
                        datetime.fromisoformat(row["decision_at"]),
                        "feature.decision_at",
                    ),
                )
                if key in keys:
                    raise IntegrityError("feature join source contains a duplicate key")
                keys.add(key)
    except (OSError, UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
        raise IntegrityError("feature join source is invalid") from exc
    if len(keys) != manifest.metadata.get("feature_ready_rows"):
        raise IntegrityError("feature join source census is invalid")
    return frozenset(keys)


def _checkpoint_payload(core: Mapping[str, object]) -> dict[str, object]:
    return {**core, "checkpoint_id": sha256_json(dict(core))}


def _checkpoint_core(payload: Mapping[str, object]) -> dict[str, object]:
    if set(payload) != {
        "checkpoint_id",
        "checkpoint_version",
        "completed",
        "run_contract",
        "run_id",
        "status",
    }:
        raise IntegrityError("foundation checkpoint schema is invalid")
    core = {key: payload[key] for key in payload if key != "checkpoint_id"}
    if payload["checkpoint_id"] != sha256_json(core):
        raise IntegrityError("foundation checkpoint content address is invalid")
    return core


def _phase_count(completed: Mapping[str, object]) -> int:
    count = int("source_bound" in completed)
    count += int("foundation_policy" in completed)
    count += int("session_policy" in completed)
    count += int("market_state" in completed)
    intervals = completed.get("intervals", {})
    if isinstance(intervals, dict):
        for value in intervals.values():
            if isinstance(value, dict):
                count += sum(phase in value for phase in _RECEIPT_PHASES)
                count += int("outcome_source_input" in value)
    count += int("foundation_set" in completed)
    return count


class FoundationOrchestrator:
    """One-writer, resumable publication of dependency-closed foundation sets."""

    def __init__(
        self,
        *,
        boundary: RepoBoundary,
        operation_receipt: OperationReceipt,
        batch_rows: int = 100_000,
    ) -> None:
        if type(batch_rows) is not int or batch_rows <= 0:
            raise ContractError("foundation batch_rows must be a positive exact integer")
        operation_receipt.verify(boundary, operation="PUBLISH_RELEASE")
        if operation_receipt.classification not in {
            OperationClassification.SYNTHETIC_MECHANICS_ONLY,
            OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        }:
            raise ContractError("foundation orchestration requires a non-alpha receipt")
        self.boundary = boundary
        self.batch_rows = batch_rows
        self.publisher = AtomicPublisher(
            boundary.active_root
            / "data"
            / "vault"
            / ".staging"
            / "releases"
            / "foundation",
            boundary.active_root / "data" / "vault" / "releases",
            boundary.active_root / "state" / "locks" / "foundation-release.lock",
            boundary=boundary,
            operation_receipt=operation_receipt,
        )
        self.checkpoint_root = boundary.assert_active_path(
            boundary.active_root
            / "data"
            / "vault"
            / ".staging"
            / "foundation_runs"
            / "_boundary_probe",
            purpose="foundation checkpoint root",
            subtree="data/vault/.staging/foundation_runs",
        ).parent

    def _run_contract(
        self,
        *,
        snapshot: PublishedSourceSnapshot,
        selection_receipt: VerifiedReleaseReceipt,
        selection: Mapping[str, object],
        intervals: tuple[object, ...],
        resolved_selection: object,
        feature_spec: CausalFeatureSpec,
    ) -> dict[str, object]:
        selected: list[dict[str, object]] = []
        for interval in intervals:
            selected.append(
                {
                    "bar_path": interval.bars.binding.relative_path,
                    "bar_query_contract_id": interval.bars.query_contract_id,
                    "bar_query_mode_id": interval.bars.query_mode_id,
                    "bar_sha256": interval.bars.binding.sha256,
                    "coverage_disposition": interval.coverage_disposition,
                    "definition_path": interval.definition.binding.relative_path,
                    "definition_query_contract_id": interval.definition.query_contract_id,
                    "definition_query_mode_id": interval.definition.query_mode_id,
                    "definition_sha256": interval.definition.binding.sha256,
                    "end": interval.end,
                    "interval_key": _interval_key(
                        interval.market, interval.year, interval.start, interval.end
                    ),
                    "market": interval.market,
                    "statistics_source_files": [
                        {
                            "path": item.binding.relative_path,
                            "query_contract_id": item.query_contract_id,
                            "query_mode_id": item.query_mode_id,
                            "sha256": item.binding.sha256,
                        }
                        for item in interval.statistics
                    ],
                    "start": interval.start,
                    "status_source_files": [
                        {
                            "path": item.binding.relative_path,
                            "query_contract_id": item.query_contract_id,
                            "query_mode_id": item.query_mode_id,
                            "sha256": item.binding.sha256,
                        }
                        for item in interval.status
                    ],
                    "year": interval.year,
                }
            )
        receipt = snapshot.receipt
        required_snapshot_fields = (
            "files_index_sha256",
            "manifest_sha256",
            "source_snapshot_id",
        )
        if any(
            type(receipt.get(name)) is not str
            or re.fullmatch(r"[0-9a-f]{64}", str(receipt.get(name))) is None
            for name in required_snapshot_fields
        ):
            raise IntegrityError("verified source snapshot identity fields are invalid")
        contract = {
            "batch_rows": self.batch_rows,
            "config_closure": _config_closure(self.boundary),
            "feature_spec": feature_spec.as_dict(),
            "feature_spec_hash": feature_spec.spec_hash,
            "implementation_closure": _implementation_closure(),
            "coverage_matrix": list(resolved_selection.coverage_matrix),
            "coverage_matrix_id": resolved_selection.coverage_matrix_id,
            "query_manifest": list(resolved_selection.query_manifest),
            "query_manifest_id": resolved_selection.query_manifest_id,
            "query_mode_census": list(resolved_selection.query_mode_census),
            "interval_count": len(selected),
            "intervals": selected,
            "repository_id": self.boundary.repository_id,
            "selection_manifest_id": selection.get("selection_manifest_id"),
            "source_selection_receipt": selection_receipt.as_dict(),
            "source_snapshot_files_index_sha256": receipt["files_index_sha256"],
            "source_snapshot_manifest_sha256": receipt["manifest_sha256"],
            "source_snapshot_id": snapshot.source_snapshot_id,
        }
        if (
            not selected
            or type(contract["selection_manifest_id"]) is not str
            or re.fullmatch(
                r"[0-9a-f]{64}", str(contract["selection_manifest_id"])
            )
            is None
        ):
            raise IntegrityError("foundation run contract has no exact selected intervals")
        return contract

    def _load_or_initialize_checkpoint(
        self, *, run_id: str, run_contract: Mapping[str, object]
    ) -> tuple[Path, dict[str, object]]:
        run_root = self.boundary.assert_active_path(
            self.checkpoint_root / run_id,
            purpose="foundation run checkpoint",
            subtree="data/vault/.staging/foundation_runs",
        )
        checkpoint_path = run_root / "checkpoint.json"
        expected_core: dict[str, object] = {
            "checkpoint_version": CHECKPOINT_VERSION,
            "completed": {},
            "run_contract": dict(run_contract),
            "run_id": run_id,
            "status": "RUNNING",
        }
        if not checkpoint_path.exists():
            _atomic_checkpoint(checkpoint_path, _checkpoint_payload(expected_core))
            return checkpoint_path, expected_core
        payload = _read_canonical_object(
            checkpoint_path, description="foundation checkpoint"
        )
        core = _checkpoint_core(payload)
        if (
            core.get("checkpoint_version") != CHECKPOINT_VERSION
            or core.get("run_id") != run_id
            or core.get("run_contract") != dict(run_contract)
            or core.get("status") not in {"RUNNING", "COMPLETE"}
            or not isinstance(core.get("completed"), dict)
        ):
            raise IntegrityError("foundation checkpoint differs from the exact run contract")
        return checkpoint_path, core

    def _persist(
        self,
        checkpoint_path: Path,
        core: dict[str, object],
        *,
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ) -> None:
        run_contract = core.get("run_contract")
        if not isinstance(run_contract, dict):
            raise IntegrityError("foundation run contract checkpoint is invalid")
        if (
            _implementation_closure() != run_contract.get("implementation_closure")
            or _config_closure(self.boundary) != run_contract.get("config_closure")
        ):
            raise IntegrityError(
                "foundation implementation or config closure changed during the run"
            )
        _atomic_checkpoint(checkpoint_path, _checkpoint_payload(core))
        if after_checkpoint is not None:
            after_checkpoint(phase)

    def run(
        self,
        *,
        source_snapshot_root: Path,
        source_selection_receipt: VerifiedReleaseReceipt,
        feature_spec: CausalFeatureSpec,
        after_checkpoint: Callable[[str], None] | None = None,
    ) -> FoundationRunResult:
        snapshot = PublishedSourceSnapshot.open(
            source_snapshot_root, boundary=self.boundary
        )
        selection = load_source_selection(
            source_selection_receipt,
            snapshot=snapshot,
            boundary=self.boundary,
        )
        resolved_selection = resolve_foundation_selection(
            selection, snapshot=snapshot
        )
        intervals = resolved_selection.intervals
        _verify_selection_index_bindings(selection, snapshot)
        coverage_policy = FoundationCoveragePolicy.from_file(
            self.boundary.active_root / "configs" / "foundation_coverage_policy.json"
        )
        statistics_roles = StatisticsRolePolicy.from_file(
            self.boundary.active_root / "configs" / "statistics_foundation_roles.json"
        )
        tracked_feature_spec = _load_feature_spec(
            self.boundary.active_root / "configs" / "mechanical_feature_spec.json",
            boundary=self.boundary,
        )
        if feature_spec != tracked_feature_spec:
            raise IntegrityError(
                "foundation feature specification differs from its tracked mechanical contract"
            )
        run_contract = self._run_contract(
            snapshot=snapshot,
            selection_receipt=source_selection_receipt,
            selection=selection,
            intervals=intervals,
            resolved_selection=resolved_selection,
            feature_spec=feature_spec,
        )
        run_id = sha256_json(run_contract)
        run_lock = self.boundary.assert_active_path(
            self.boundary.active_root
            / "state"
            / "locks"
            / f"foundation-{run_id}.lock",
            purpose="foundation run lock",
            subtree="state/locks",
        )
        with FileLease(run_lock):
            checkpoint_path, core = self._load_or_initialize_checkpoint(
                run_id=run_id, run_contract=run_contract
            )
            return self._run_locked(
                snapshot=snapshot,
                selection=selection,
                intervals=intervals,
                resolved_selection=resolved_selection,
                source_selection_receipt=source_selection_receipt,
                feature_spec=feature_spec,
                coverage_policy=coverage_policy,
                statistics_roles=statistics_roles,
                checkpoint_path=checkpoint_path,
                core=core,
                after_checkpoint=after_checkpoint,
            )

    def _run_locked(
        self,
        *,
        snapshot: PublishedSourceSnapshot,
        selection: Mapping[str, object],
        intervals: tuple[object, ...],
        resolved_selection: object,
        source_selection_receipt: VerifiedReleaseReceipt,
        feature_spec: CausalFeatureSpec,
        coverage_policy: FoundationCoveragePolicy,
        statistics_roles: StatisticsRolePolicy,
        checkpoint_path: Path,
        core: dict[str, object],
        after_checkpoint: Callable[[str], None] | None,
    ) -> FoundationRunResult:
        completed = core["completed"]
        if not isinstance(completed, dict):
            raise IntegrityError("foundation completed-phase map is invalid")
        allowed_top = {
            "source_bound",
            "foundation_policy",
            "session_policy",
            "market_state",
            "intervals",
            "foundation_set",
        }
        if not set(completed).issubset(allowed_top):
            raise IntegrityError("foundation checkpoint has an unknown completed phase")

        source_bound = {
            "selection_manifest_id": selection["selection_manifest_id"],
            "source_selection_receipt_id": source_selection_receipt.receipt_id,
            "source_snapshot_id": snapshot.source_snapshot_id,
        }
        if "source_bound" in completed:
            if completed["source_bound"] != source_bound:
                raise IntegrityError("checkpoint source binding changed")
        else:
            if completed:
                raise IntegrityError("foundation checkpoint skips source-bound phase")
            completed["source_bound"] = source_bound
            self._persist(
                checkpoint_path,
                core,
                phase="source_bound",
                after_checkpoint=after_checkpoint,
            )

        policy_receipt: VerifiedReleaseReceipt
        if "foundation_policy" in completed:
            policy_receipt = _receipt(
                completed["foundation_policy"], name="foundation_policy"
            )
            policies = VerifiedFoundationPolicies.from_release(
                policy_receipt, boundary=self.boundary
            )
        else:
            if any(
                key in completed
                for key in ("session_policy", "market_state", "intervals", "foundation_set")
            ):
                raise IntegrityError("foundation checkpoint skips policy phase")
            policy_receipt = publish_foundation_policies(
                boundary=self.boundary,
                publisher=self.publisher,
                config_root=self.boundary.active_root / "configs",
            )
            policies = VerifiedFoundationPolicies.from_release(
                policy_receipt, boundary=self.boundary
            )
            completed["foundation_policy"] = policy_receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase="foundation_policy",
                after_checkpoint=after_checkpoint,
            )

        if "session_policy" in completed:
            session_receipt = _receipt(
                completed["session_policy"], name="session_policy"
            )
            session_policy = load_versioned_session_policy(
                session_receipt, policies=policies, boundary=self.boundary
            )
        else:
            if any(
                key in completed for key in ("market_state", "intervals", "foundation_set")
            ):
                raise IntegrityError("foundation checkpoint skips session-policy phase")
            session_receipt = publish_versioned_session_policy(
                policies=policies,
                boundary=self.boundary,
                publisher=self.publisher,
            )
            session_policy = load_versioned_session_policy(
                session_receipt, policies=policies, boundary=self.boundary
            )
            completed["session_policy"] = session_receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase="session_policy",
                after_checkpoint=after_checkpoint,
            )

        if "market_state" in completed:
            market_state_receipt = _receipt(
                completed["market_state"], name="market_state"
            )
            market_state = load_market_state_foundation(
                market_state_receipt,
                boundary=self.boundary,
                expected_selection=resolved_selection,
                expected_source_selection_receipt=source_selection_receipt,
                expected_coverage_policy=coverage_policy,
                expected_statistics_roles=statistics_roles,
            )
        else:
            if any(key in completed for key in ("intervals", "foundation_set")):
                raise IntegrityError("foundation checkpoint skips market-state phase")
            market_state_receipt = publish_market_state_foundation(
                selection=resolved_selection,
                source_selection_receipt=source_selection_receipt,
                coverage_policy=coverage_policy,
                statistics_roles=statistics_roles,
                publisher=self.publisher,
                batch_rows=self.batch_rows,
            )
            market_state = load_market_state_foundation(
                market_state_receipt,
                boundary=self.boundary,
                expected_selection=resolved_selection,
                expected_source_selection_receipt=source_selection_receipt,
                expected_coverage_policy=coverage_policy,
                expected_statistics_roles=statistics_roles,
            )
            completed["market_state"] = market_state_receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase="market_state",
                after_checkpoint=after_checkpoint,
            )

        interval_state = completed.setdefault("intervals", {})
        if not isinstance(interval_state, dict):
            raise IntegrityError("foundation interval checkpoint map is invalid")
        expected_keys = {
            _interval_key(item.market, item.year, item.start, item.end)
            for item in intervals
        }
        if not set(interval_state).issubset(expected_keys):
            raise IntegrityError("foundation checkpoint contains an unknown interval")

        verified_intervals: list[dict[str, object]] = []
        total_bar_rows = 0
        total_status_eligible_rows = 0
        total_status_resolved_rows = 0
        total_status_unresolved_rows = 0
        total_feature_ready_rows = 0
        total_status_gated_feature_ready_rows = 0
        status_epoch_gates: list[dict[str, object]] = []
        for interval in intervals:
            key = _interval_key(
                interval.market, interval.year, interval.start, interval.end
            )
            state = interval_state.setdefault(key, {})
            if not isinstance(state, dict) or not set(state).issubset(
                {*_RECEIPT_PHASES, "outcome_source_input"}
            ):
                raise IntegrityError("foundation interval phase map is invalid")

            raw_receipt = self._ensure_raw(
                state,
                interval=interval,
                selection_receipt=source_selection_receipt,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:raw",
                after_checkpoint=after_checkpoint,
            )
            definitions = self._ensure_definitions(
                state,
                raw_receipt=raw_receipt,
                policies=policies,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:definitions",
                after_checkpoint=after_checkpoint,
            )
            causal_receipt = self._ensure_causal(
                state,
                raw_receipt=raw_receipt,
                policies=policies,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:causal",
                after_checkpoint=after_checkpoint,
            )
            status_eligibility_receipt = self._ensure_status_eligibility(
                state,
                causal_receipt=causal_receipt,
                market_state=market_state,
                market=interval.market,
                year=interval.year,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:status_eligibility",
                after_checkpoint=after_checkpoint,
            )
            status_contract = load_status_eligibility(
                status_eligibility_receipt,
                causal_receipt=causal_receipt,
                market_state_receipt=market_state_receipt,
                boundary=self.boundary,
            )
            economics = self._ensure_economics(
                state,
                causal_receipt=causal_receipt,
                definitions=definitions,
                policies=policies,
                session_policy=session_policy,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:economics",
                after_checkpoint=after_checkpoint,
            )
            feature_receipt = self._ensure_feature_input(
                state,
                causal_receipt=causal_receipt,
                definitions=definitions,
                economics=economics,
                policies=policies,
                session_policy=session_policy,
                feature_spec=feature_spec,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:feature_input",
                after_checkpoint=after_checkpoint,
            )
            loaded_features = load_causal_feature_release(
                feature_receipt,
                causal_receipt=causal_receipt,
                definitions=definitions,
                economics_registry=economics,
                policies=policies,
                session_policy=session_policy,
                boundary=self.boundary,
            )
            eligible_decision_keys = status_eligible_decision_keys(
                status_eligibility_receipt,
                causal_receipt=causal_receipt,
                market_state_receipt=market_state_receipt,
                boundary=self.boundary,
            )
            feature_ready_keys = _feature_ready_join_keys(
                feature_receipt, boundary=self.boundary
            )
            status_gated_feature_ready_rows = len(
                feature_ready_keys & eligible_decision_keys
            )
            interval_bar_rows = int(status_contract["total_rows"])
            interval_feature_ready_rows = len(loaded_features.rows)
            interval_status_resolved_fraction = (
                Decimal(int(status_contract["resolved_status_rows"]))
                / Decimal(interval_bar_rows)
                if interval_bar_rows
                else Decimal(0)
            )
            interval_status_gated_feature_fraction = (
                Decimal(status_gated_feature_ready_rows)
                / Decimal(interval_feature_ready_rows)
                if interval_feature_ready_rows
                else Decimal(0)
            )
            research_eligible = (
                interval_status_resolved_fraction
                >= coverage_policy.minimum_status_resolved_decision_fraction
                and interval_status_gated_feature_fraction
                >= coverage_policy.minimum_status_gated_feature_ready_fraction
            )
            status_epoch_gate_core: dict[str, object] = {
                "bar_rows": interval_bar_rows,
                "feature_ready_rows": interval_feature_ready_rows,
                "interval_key": key,
                "research_disposition": (
                    "ELIGIBLE"
                    if research_eligible
                    else "ABSTAIN_STATUS_COVERAGE"
                ),
                "status_gated_feature_ready_fraction": str(
                    interval_status_gated_feature_fraction
                ),
                "status_gated_feature_ready_rows": status_gated_feature_ready_rows,
                "status_eligible_rows": int(status_contract["eligible_rows"]),
                "status_query_contract_ids": sorted(
                    item.query_contract_id for item in interval.status
                ),
                "status_query_mode_ids": sorted(
                    {item.query_mode_id for item in interval.status}
                ),
                "status_resolved_decision_fraction": str(
                    interval_status_resolved_fraction
                ),
                "status_resolved_rows": int(status_contract["resolved_status_rows"]),
                "status_source_present": bool(interval.status),
                "status_unresolved_rows": int(
                    status_contract["unresolved_status_rows"]
                ),
            }
            status_epoch_gate = {
                **status_epoch_gate_core,
                "status_epoch_gate_id": sha256_json(status_epoch_gate_core),
            }
            status_epoch_gates.append(status_epoch_gate)
            outcome_input_receipt = self._ensure_outcome_source_input(
                state,
                causal_receipt=causal_receipt,
                definitions=definitions,
                economics=economics,
                policies=policies,
                session_policy=session_policy,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:outcome_source_input",
                after_checkpoint=after_checkpoint,
            )
            # Rehash the exact consumed provider inputs immediately before the
            # aggregate can become accepted, catching mutation during a long run.
            for item in (
                interval.definition,
                interval.bars,
                *interval.status,
                *interval.statistics,
            ):
                binding = item.binding
                binding.verify()
                item.sidecar_binding.verify()

            total_bar_rows += int(status_contract["total_rows"])
            total_status_eligible_rows += int(status_contract["eligible_rows"])
            total_status_resolved_rows += int(status_contract["resolved_status_rows"])
            total_status_unresolved_rows += int(
                status_contract["unresolved_status_rows"]
            )
            total_feature_ready_rows += len(loaded_features.rows)
            total_status_gated_feature_ready_rows += (
                status_gated_feature_ready_rows
            )

            verified_intervals.append(
                {
                    "bar_query_contract_id": interval.bars.query_contract_id,
                    "bar_query_mode_id": interval.bars.query_mode_id,
                    "bar_source_path": interval.bars.binding.relative_path,
                    "bar_source_sha256": interval.bars.binding.sha256,
                    "causal_release_receipt": causal_receipt.as_dict(),
                    "coverage_disposition": interval.coverage_disposition,
                    "definition_release_receipt": definitions.receipt.as_dict(),
                    "definition_query_contract_id": interval.definition.query_contract_id,
                    "definition_query_mode_id": interval.definition.query_mode_id,
                    "definition_source_path": interval.definition.binding.relative_path,
                    "definition_source_sha256": interval.definition.binding.sha256,
                    "economics_release_receipt": economics.release_receipt.as_dict(),
                    "end": interval.end,
                    "feature_input_release_receipt": feature_receipt.as_dict(),
                    "feature_ready_rows": len(loaded_features.rows),
                    "interval_key": key,
                    "market": interval.market,
                    "outcome_source_input_release_receipt": (
                        outcome_input_receipt.as_dict()
                    ),
                    "raw_release_receipt": raw_receipt.as_dict(),
                    "status_eligibility_release_receipt": (
                        status_eligibility_receipt.as_dict()
                    ),
                    "status_epoch_gate": status_epoch_gate,
                    "status_eligible_rows": status_contract["eligible_rows"],
                    "status_gated_feature_ready_rows": (
                        status_gated_feature_ready_rows
                    ),
                    "status_resolved_rows": status_contract["resolved_status_rows"],
                    "status_unresolved_rows": status_contract[
                        "unresolved_status_rows"
                    ],
                    "start": interval.start,
                    "year": interval.year,
                }
            )

        if set(interval_state) != expected_keys:
            raise IntegrityError("foundation interval dependency closure is incomplete")

        status_resolved_fraction = (
            Decimal(total_status_resolved_rows) / Decimal(total_bar_rows)
            if total_bar_rows
            else Decimal(0)
        )
        status_gated_feature_ready_fraction = (
            Decimal(total_status_gated_feature_ready_rows)
            / Decimal(total_feature_ready_rows)
            if total_feature_ready_rows
            else Decimal(0)
        )
        if (
            total_bar_rows < coverage_policy.minimum_bar_rows
            or total_status_gated_feature_ready_rows
            < coverage_policy.minimum_status_gated_feature_ready_rows
            or total_status_eligible_rows
            < coverage_policy.minimum_status_eligible_rows
            or total_bar_rows
            != total_status_resolved_rows + total_status_unresolved_rows
            or status_resolved_fraction
            < coverage_policy.minimum_status_resolved_decision_fraction
            or status_gated_feature_ready_fraction
            < coverage_policy.minimum_status_gated_feature_ready_fraction
            or not any(
                gate["research_disposition"] == "ELIGIBLE"
                for gate in status_epoch_gates
            )
        ):
            raise IntegrityError(
                "foundation nonzero/status coverage gates are not satisfied"
            )
        coverage_gate = {
            "bar_rows": total_bar_rows,
            "coverage_matrix_id": resolved_selection.coverage_matrix_id,
            "coverage_policy": coverage_policy.as_dict(),
            "coverage_policy_hash": coverage_policy.policy_hash,
            "feature_ready_rows": total_feature_ready_rows,
            "missing_status_rows_remain_in_denominator": True,
            "statistics_feature_use": False,
            "statistics_source_market_year_fraction": market_state.contract[
                "statistics_source_market_year_fraction"
            ],
            "status_eligible_rows": total_status_eligible_rows,
            "status_epoch_gates": status_epoch_gates,
            "status_epoch_gates_id": sha256_json(status_epoch_gates),
            "research_eligible_interval_count": sum(
                gate["research_disposition"] == "ELIGIBLE"
                for gate in status_epoch_gates
            ),
            "research_abstained_interval_count": sum(
                gate["research_disposition"] == "ABSTAIN_STATUS_COVERAGE"
                for gate in status_epoch_gates
            ),
            "status_gated_feature_ready_rows": (
                total_status_gated_feature_ready_rows
            ),
            "status_gated_feature_ready_fraction": str(
                status_gated_feature_ready_fraction
            ),
            "status_resolved_rows": total_status_resolved_rows,
            "status_resolved_decision_fraction": str(status_resolved_fraction),
            "status_source_market_year_fraction": market_state.contract[
                "status_source_market_year_fraction"
            ],
            "status_unresolved_rows": total_status_unresolved_rows,
        }

        foundation_set_core = {
            "alpha_evidence": False,
            "candidate_eligible": False,
            "dependency_closure_complete": True,
            "coverage_gate": coverage_gate,
            "coverage_matrix": list(resolved_selection.coverage_matrix),
            "coverage_matrix_id": resolved_selection.coverage_matrix_id,
            "feature_spec": feature_spec.as_dict(),
            "feature_spec_hash": feature_spec.spec_hash,
            "foundation_policy_receipt": policy_receipt.as_dict(),
            "historical_outcome_or_label_execution": False,
            "interval_count": len(verified_intervals),
            "intervals": verified_intervals,
            "learned_or_outcome_informed_transform_count": 0,
            "model_fit_count": 0,
            "market_state_release_receipt": market_state_receipt.as_dict(),
            "query_manifest": list(resolved_selection.query_manifest),
            "query_manifest_id": resolved_selection.query_manifest_id,
            "query_mode_census": list(resolved_selection.query_mode_census),
            "feature_role_contract": {
                "eligibility_authority": "STATUS_AS_OF_ELIGIBILITY_RELEASE",
                "mechanical_feature_values_are_not_standalone_research_eligibility": True,
                "statistics_feature_use": False,
            },
            "outcome_contract": {
                "deferred_until": OUTCOME_DEFERRED_UNTIL,
                "labels_materialized": False,
                "prediction_ledger_read": False,
                "role": OUTCOME_SOURCE_ROLE,
            },
            "provider_call_count": 0,
            "run_contract": dict(core["run_contract"]),
            "run_id": core["run_id"],
            "schema_version": FOUNDATION_SET_SCHEMA_VERSION,
            "session_policy_receipt": session_receipt.as_dict(),
            "source_selection_receipt": source_selection_receipt.as_dict(),
            "source_snapshot_id": snapshot.source_snapshot_id,
            "wfa_execution_count": 0,
        }
        foundation_set = {
            **foundation_set_core,
            "foundation_set_id": sha256_json(foundation_set_core),
        }
        if "foundation_set" in completed:
            foundation_set_receipt = _receipt(
                completed["foundation_set"], name="foundation_set"
            )
            observed = load_foundation_set(
                foundation_set_receipt, boundary=self.boundary
            )
            if observed != foundation_set:
                raise IntegrityError("completed foundation-set payload changed")
        else:
            stage = self.publisher.create_stage("foundation_set")
            (stage / "foundation_set.json").write_bytes(
                canonical_bytes(foundation_set) + b"\n"
            )
            dependency_ids = {
                source_selection_receipt.release_id,
                policy_receipt.release_id,
                session_receipt.release_id,
                market_state_receipt.release_id,
            }
            for item in verified_intervals:
                for name in (
                    "raw_release_receipt",
                    "definition_release_receipt",
                    "causal_release_receipt",
                    "status_eligibility_release_receipt",
                    "economics_release_receipt",
                    "feature_input_release_receipt",
                    "outcome_source_input_release_receipt",
                ):
                    dependency_ids.add(str(item[name]["release_id"]))
            manifest = ReleaseManifest.build(
                stage,
                release_kind=FOUNDATION_SET_RELEASE_KIND,
                schema_version=FOUNDATION_SET_SCHEMA_VERSION,
                source_release_ids=tuple(sorted(dependency_ids)),
                metadata={
                    "coverage_matrix_id": resolved_selection.coverage_matrix_id,
                    "feature_spec_hash": feature_spec.spec_hash,
                    "foundation_set_id": foundation_set["foundation_set_id"],
                    "interval_count": len(verified_intervals),
                    "query_manifest_id": resolved_selection.query_manifest_id,
                    "run_id": core["run_id"],
                    "source_snapshot_id": snapshot.source_snapshot_id,
                },
            )
            release = self.publisher.publish(stage, manifest)
            foundation_set_receipt = VerifiedReleaseReceipt.from_release(
                release, self.boundary
            )
            if (
                load_foundation_set(foundation_set_receipt, boundary=self.boundary)
                != foundation_set
            ):
                raise IntegrityError("published foundation set failed exact readback")
            completed["foundation_set"] = foundation_set_receipt.as_dict()
            core["status"] = "COMPLETE"
            self._persist(
                checkpoint_path,
                core,
                phase="foundation_set",
                after_checkpoint=after_checkpoint,
            )

        if core["status"] != "COMPLETE":
            core["status"] = "COMPLETE"
            self._persist(
                checkpoint_path,
                core,
                phase="complete_repair",
                after_checkpoint=after_checkpoint,
            )
        return FoundationRunResult(
            run_id=str(core["run_id"]),
            checkpoint_path=checkpoint_path,
            completed_phase_count=_phase_count(completed),
            foundation_set_receipt=foundation_set_receipt,
        )

    def _ensure_raw(
        self,
        state: dict[str, object],
        *,
        interval: object,
        selection_receipt: VerifiedReleaseReceipt,
        checkpoint_path: Path,
        core: dict[str, object],
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ) -> VerifiedReleaseReceipt:
        if "raw" in state:
            receipt = _receipt(state["raw"], name=phase)
        else:
            if state:
                raise IntegrityError("foundation interval skips raw phase")
            receipt = materialize_raw_interval(
                definition_binding=interval.definition.binding,
                bar_binding=interval.bars.binding,
                definition_query_contract=interval.definition.query_contract,
                bar_query_contract=interval.bars.query_contract,
                market=interval.market,
                year=interval.year,
                filename=Path(interval.bars.binding.relative_path).name,
                source_selection_release_id=selection_receipt.release_id,
                publisher=self.publisher,
                batch_rows=self.batch_rows,
            )
            state["raw"] = receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase=phase,
                after_checkpoint=after_checkpoint,
            )
        loaded = load_raw_interval(receipt, boundary=self.boundary)
        report = loaded.interval_receipt
        if (
            report.get("market") != interval.market
            or report.get("year") != interval.year
            or report.get("source_snapshot_id")
            != interval.bars.binding.source_snapshot_id
            or report.get("source_selection_release_id") != selection_receipt.release_id
            or report.get("source_definition_file_sha256")
            != interval.definition.binding.sha256
            or report.get("source_bar_file_sha256") != interval.bars.binding.sha256
            or report.get("definition_query_contract_id")
            != interval.definition.query_contract_id
            or report.get("bar_query_contract_id") != interval.bars.query_contract_id
        ):
            raise IntegrityError("raw checkpoint release has wrong exact upstream IDs")
        return receipt

    def _ensure_definitions(
        self,
        state: dict[str, object],
        *,
        raw_receipt: VerifiedReleaseReceipt,
        policies: VerifiedFoundationPolicies,
        checkpoint_path: Path,
        core: dict[str, object],
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ):
        if "definitions" in state:
            receipt = _receipt(state["definitions"], name=phase)
        else:
            if set(state) != {"raw"}:
                raise IntegrityError("foundation interval skips definitions phase")
            receipt = publish_actual_contract_definitions(
                raw_receipt=raw_receipt,
                policies=policies,
                boundary=self.boundary,
                publisher=self.publisher,
            )
            state["definitions"] = receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase=phase,
                after_checkpoint=after_checkpoint,
            )
        return load_actual_contract_definitions(
            receipt,
            raw_receipt=raw_receipt,
            policies=policies,
            boundary=self.boundary,
        )

    def _ensure_causal(
        self,
        state: dict[str, object],
        *,
        raw_receipt: VerifiedReleaseReceipt,
        policies: VerifiedFoundationPolicies,
        checkpoint_path: Path,
        core: dict[str, object],
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ) -> VerifiedReleaseReceipt:
        if "causal" in state:
            receipt = _receipt(state["causal"], name=phase)
        else:
            if set(state) != {"raw", "definitions"}:
                raise IntegrityError("foundation interval skips causal phase")
            receipt = materialize_causal_interval(
                raw_receipt=raw_receipt,
                policies=policies,
                publisher=self.publisher,
                batch_rows=self.batch_rows,
            )
            state["causal"] = receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase=phase,
                after_checkpoint=after_checkpoint,
            )
        _, report = load_causal_interval(receipt, boundary=self.boundary)
        if (
            report.get("source_raw_release_id") != raw_receipt.release_id
            or report.get("foundation_policy_release_id") != policies.receipt.release_id
            or report.get("foundation_policy_set_id") != policies.policy_set_id
        ):
            raise IntegrityError("causal checkpoint release has wrong exact upstream IDs")
        return receipt

    def _ensure_status_eligibility(
        self,
        state: dict[str, object],
        *,
        causal_receipt: VerifiedReleaseReceipt,
        market_state: LoadedMarketStateFoundation,
        market: str,
        year: int,
        checkpoint_path: Path,
        core: dict[str, object],
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ) -> VerifiedReleaseReceipt:
        if "status_eligibility" in state:
            receipt = _receipt(state["status_eligibility"], name=phase)
        else:
            if set(state) != {"raw", "definitions", "causal"}:
                raise IntegrityError(
                    "foundation interval skips status-eligibility phase"
                )
            receipt = publish_status_eligibility(
                causal_receipt=causal_receipt,
                market_state=market_state,
                market=market,
                year=year,
                publisher=self.publisher,
                batch_rows=self.batch_rows,
            )
            state["status_eligibility"] = receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase=phase,
                after_checkpoint=after_checkpoint,
            )
        load_status_eligibility(
            receipt,
            causal_receipt=causal_receipt,
            market_state_receipt=market_state.receipt,
            boundary=self.boundary,
        )
        return receipt

    def _ensure_economics(
        self,
        state: dict[str, object],
        *,
        causal_receipt: VerifiedReleaseReceipt,
        definitions: object,
        policies: VerifiedFoundationPolicies,
        session_policy: object,
        checkpoint_path: Path,
        core: dict[str, object],
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ):
        if "economics" in state:
            receipt = _receipt(state["economics"], name=phase)
        else:
            if set(state) != {
                "raw",
                "definitions",
                "causal",
                "status_eligibility",
            }:
                raise IntegrityError("foundation interval skips economics phase")
            receipt = publish_actual_contract_economics(
                causal_receipt=causal_receipt,
                definitions=definitions,
                policies=policies,
                session_policy=session_policy,
                boundary=self.boundary,
                publisher=self.publisher,
            )
            state["economics"] = receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase=phase,
                after_checkpoint=after_checkpoint,
            )
        return load_actual_contract_economics(
            receipt,
            causal_receipt=causal_receipt,
            definitions=definitions,
            policies=policies,
            session_policy=session_policy,
            boundary=self.boundary,
        )

    def _ensure_feature_input(
        self,
        state: dict[str, object],
        *,
        causal_receipt: VerifiedReleaseReceipt,
        definitions: object,
        economics: object,
        policies: VerifiedFoundationPolicies,
        session_policy: object,
        feature_spec: CausalFeatureSpec,
        checkpoint_path: Path,
        core: dict[str, object],
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ) -> VerifiedReleaseReceipt:
        if "feature_input" in state:
            receipt = _receipt(state["feature_input"], name=phase)
        else:
            if set(state) != {
                "raw",
                "definitions",
                "causal",
                "status_eligibility",
                "economics",
            }:
                raise IntegrityError("foundation interval skips feature-input phase")
            receipt = publish_causal_feature_release(
                causal_receipt=causal_receipt,
                definitions=definitions,
                economics_registry=economics,
                policies=policies,
                session_policy=session_policy,
                feature_spec=feature_spec,
                boundary=self.boundary,
                publisher=self.publisher,
            )
            state["feature_input"] = receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase=phase,
                after_checkpoint=after_checkpoint,
            )
        load_causal_feature_release(
            receipt,
            causal_receipt=causal_receipt,
            definitions=definitions,
            economics_registry=economics,
            policies=policies,
            session_policy=session_policy,
            boundary=self.boundary,
        )
        return receipt

    def _ensure_outcome_source_input(
        self,
        state: dict[str, object],
        *,
        causal_receipt: VerifiedReleaseReceipt,
        definitions: object,
        economics: object,
        policies: VerifiedFoundationPolicies,
        session_policy: object,
        checkpoint_path: Path,
        core: dict[str, object],
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ) -> VerifiedReleaseReceipt:
        dependency_receipts = (
            causal_receipt,
            definitions.receipt,
            economics.release_receipt,
            policies.receipt,
            session_policy.receipt,
        )
        payload_core = {
            "causal_release_receipt": causal_receipt.as_dict(),
            "definition_release_receipt": definitions.receipt.as_dict(),
            "deferred_until": OUTCOME_DEFERRED_UNTIL,
            "economics_release_receipt": economics.release_receipt.as_dict(),
            "foundation_policy_receipt": policies.receipt.as_dict(),
            "labels_materialized": False,
            "outcomes_materialized": False,
            "prediction_ledger_read": False,
            "role": OUTCOME_SOURCE_ROLE,
            "schema_version": OUTCOME_SOURCE_SCHEMA_VERSION,
            "session_policy_receipt": session_policy.receipt.as_dict(),
        }
        payload = {
            **payload_core,
            "outcome_source_input_id": sha256_json(payload_core),
        }
        if "outcome_source_input" in state:
            receipt = _receipt(state["outcome_source_input"], name=phase)
        else:
            if set(state) != {
                "raw",
                "definitions",
                "causal",
                "status_eligibility",
                "economics",
                "feature_input",
            }:
                raise IntegrityError("foundation interval skips outcome-source phase")
            stage = self.publisher.create_stage("outcome_source_input")
            (stage / "outcome_source_input.json").write_bytes(
                canonical_bytes(payload) + b"\n"
            )
            manifest = ReleaseManifest.build(
                stage,
                release_kind=OUTCOME_SOURCE_RELEASE_KIND,
                schema_version=OUTCOME_SOURCE_SCHEMA_VERSION,
                source_release_ids=tuple(
                    receipt.release_id for receipt in dependency_receipts
                ),
                metadata={
                    "causal_release_id": causal_receipt.release_id,
                    "outcome_source_input_id": payload["outcome_source_input_id"],
                    "role": OUTCOME_SOURCE_ROLE,
                },
            )
            release = self.publisher.publish(stage, manifest)
            receipt = VerifiedReleaseReceipt.from_release(release, self.boundary)
            state["outcome_source_input"] = receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase=phase,
                after_checkpoint=after_checkpoint,
            )
        observed = load_outcome_source_input(receipt, boundary=self.boundary)
        if observed != payload:
            raise IntegrityError("outcome-source release differs from exact dependencies")
        return receipt


def load_outcome_source_input(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        manifest.release_kind != OUTCOME_SOURCE_RELEASE_KIND
        or manifest.schema_version != OUTCOME_SOURCE_SCHEMA_VERSION
        or {entry.path for entry in manifest.files} != {"outcome_source_input.json"}
        or set(manifest.metadata)
        != {"causal_release_id", "outcome_source_input_id", "role"}
        or manifest.metadata.get("role") != OUTCOME_SOURCE_ROLE
    ):
        raise IntegrityError("outcome-source release contract is invalid")
    path = boundary.active_root / receipt.relative_root / "outcome_source_input.json"
    payload = _read_canonical_object(path, description="outcome-source input")
    if set(payload) != {
        "causal_release_receipt",
        "definition_release_receipt",
        "deferred_until",
        "economics_release_receipt",
        "foundation_policy_receipt",
        "labels_materialized",
        "outcome_source_input_id",
        "outcomes_materialized",
        "prediction_ledger_read",
        "role",
        "schema_version",
        "session_policy_receipt",
    }:
        raise IntegrityError("outcome-source payload schema is invalid")
    outcome_source_id = payload.pop("outcome_source_input_id", None)
    if (
        outcome_source_id != sha256_json(payload)
        or outcome_source_id != manifest.metadata["outcome_source_input_id"]
        or payload.get("schema_version") != OUTCOME_SOURCE_SCHEMA_VERSION
        or payload.get("role") != OUTCOME_SOURCE_ROLE
        or payload.get("deferred_until") != OUTCOME_DEFERRED_UNTIL
        or payload.get("labels_materialized") is not False
        or payload.get("outcomes_materialized") is not False
        or payload.get("prediction_ledger_read") is not False
    ):
        raise IntegrityError("outcome-source identity or safety posture is invalid")
    receipt_fields = (
        ("causal_release_receipt", CAUSAL_RELEASE_KIND),
        ("definition_release_receipt", DEFINITION_RELEASE_KIND),
        ("economics_release_receipt", ECONOMICS_RELEASE_KIND),
        ("foundation_policy_receipt", POLICY_RELEASE_KIND),
        ("session_policy_receipt", SESSION_RELEASE_KIND),
    )
    dependencies: list[VerifiedReleaseReceipt] = []
    for name, expected_kind in receipt_fields:
        dependency = _receipt(payload.get(name), name=name)
        dependency.verify(boundary)
        if dependency.release_kind != expected_kind:
            raise IntegrityError("outcome-source dependency kind is invalid")
        dependencies.append(dependency)
    causal = dependencies[0]
    if (
        manifest.metadata["causal_release_id"] != causal.release_id
        or manifest.source_release_ids
        != tuple(sorted(dependency.release_id for dependency in dependencies))
    ):
        raise IntegrityError("outcome-source exact dependency closure is invalid")
    payload["outcome_source_input_id"] = outcome_source_id
    return payload


def load_foundation_set(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        manifest.release_kind != FOUNDATION_SET_RELEASE_KIND
        or manifest.schema_version != FOUNDATION_SET_SCHEMA_VERSION
        or {item.path for item in manifest.files} != {"foundation_set.json"}
        or set(manifest.metadata)
        != {
            "feature_spec_hash",
            "coverage_matrix_id",
            "foundation_set_id",
            "interval_count",
            "query_manifest_id",
            "run_id",
            "source_snapshot_id",
        }
    ):
        raise IntegrityError("foundation-set release contract is invalid")
    path = boundary.active_root / receipt.relative_root / "foundation_set.json"
    payload = _read_canonical_object(path, description="foundation set")
    if set(payload) != {
        "alpha_evidence",
        "candidate_eligible",
        "coverage_gate",
        "coverage_matrix",
        "coverage_matrix_id",
        "dependency_closure_complete",
        "feature_role_contract",
        "feature_spec",
        "feature_spec_hash",
        "foundation_policy_receipt",
        "foundation_set_id",
        "historical_outcome_or_label_execution",
        "interval_count",
        "intervals",
        "learned_or_outcome_informed_transform_count",
        "model_fit_count",
        "market_state_release_receipt",
        "query_manifest",
        "query_manifest_id",
        "query_mode_census",
        "outcome_contract",
        "provider_call_count",
        "run_contract",
        "run_id",
        "schema_version",
        "session_policy_receipt",
        "source_selection_receipt",
        "source_snapshot_id",
        "wfa_execution_count",
    }:
        raise IntegrityError("foundation-set payload schema is invalid")
    foundation_set_id = payload.pop("foundation_set_id", None)
    if (
        foundation_set_id != sha256_json(payload)
        or foundation_set_id != manifest.metadata["foundation_set_id"]
        or payload.get("schema_version") != FOUNDATION_SET_SCHEMA_VERSION
        or payload.get("run_id") != manifest.metadata["run_id"]
        or payload.get("feature_spec_hash") != manifest.metadata["feature_spec_hash"]
        or payload.get("coverage_matrix_id") != manifest.metadata["coverage_matrix_id"]
        or payload.get("query_manifest_id") != manifest.metadata["query_manifest_id"]
        or not isinstance(payload.get("query_manifest"), list)
        or payload.get("query_manifest_id")
        != sha256_json(payload.get("query_manifest"))
        or not isinstance(payload.get("query_mode_census"), list)
        or payload.get("source_snapshot_id")
        != manifest.metadata["source_snapshot_id"]
        or payload.get("interval_count") != manifest.metadata["interval_count"]
        or payload.get("dependency_closure_complete") is not True
        or payload.get("provider_call_count") != 0
        or payload.get("model_fit_count") != 0
        or payload.get("wfa_execution_count") != 0
        or payload.get("historical_outcome_or_label_execution") is not False
        or payload.get("learned_or_outcome_informed_transform_count") != 0
        or payload.get("alpha_evidence") is not False
        or payload.get("candidate_eligible") is not False
        or payload.get("feature_role_contract")
        != {
            "eligibility_authority": "STATUS_AS_OF_ELIGIBILITY_RELEASE",
            "mechanical_feature_values_are_not_standalone_research_eligibility": True,
            "statistics_feature_use": False,
        }
        or not isinstance(payload.get("coverage_matrix"), list)
        or payload.get("coverage_matrix_id")
        != sha256_json(payload.get("coverage_matrix"))
        or not isinstance(payload.get("coverage_gate"), dict)
        or payload["coverage_gate"].get("coverage_matrix_id")
        != payload.get("coverage_matrix_id")
        or payload["coverage_gate"].get("missing_status_rows_remain_in_denominator")
        is not True
        or payload["coverage_gate"].get("statistics_feature_use") is not False
        or payload.get("outcome_contract")
        != {
            "deferred_until": OUTCOME_DEFERRED_UNTIL,
            "labels_materialized": False,
            "prediction_ledger_read": False,
            "role": OUTCOME_SOURCE_ROLE,
        }
        or re.fullmatch(r"[0-9a-f]{64}", str(payload.get("run_id"))) is None
        or not isinstance(payload.get("run_contract"), dict)
        or sha256_json(payload.get("run_contract")) != payload.get("run_id")
        or payload["run_contract"].get("query_manifest")
        != payload.get("query_manifest")
        or payload["run_contract"].get("query_manifest_id")
        != payload.get("query_manifest_id")
        or payload["run_contract"].get("query_mode_census")
        != payload.get("query_mode_census")
        or re.fullmatch(
            r"[0-9a-f]{64}", str(payload.get("source_snapshot_id"))
        )
        is None
    ):
        raise IntegrityError("foundation-set content address or safety posture is invalid")
    feature_spec = CausalFeatureSpec.from_dict(payload.get("feature_spec"))
    if feature_spec.spec_hash != payload.get("feature_spec_hash"):
        raise IntegrityError("foundation-set feature specification hash is invalid")
    try:
        coverage_policy = FoundationCoveragePolicy.from_dict(
            payload["coverage_gate"].get("coverage_policy")
        )
    except ContractError as exc:
        raise IntegrityError("foundation-set coverage policy is invalid") from exc
    if payload["coverage_gate"].get("coverage_policy_hash") != coverage_policy.policy_hash:
        raise IntegrityError("foundation-set coverage policy hash is invalid")
    top_receipts = (
        _receipt(payload.get("source_selection_receipt"), name="source_selection"),
        _receipt(payload.get("foundation_policy_receipt"), name="foundation_policy"),
        _receipt(payload.get("session_policy_receipt"), name="session_policy"),
        _receipt(payload.get("market_state_release_receipt"), name="market_state"),
    )
    expected_top_kinds = (
        SELECTION_RELEASE_KIND,
        POLICY_RELEASE_KIND,
        SESSION_RELEASE_KIND,
        MARKET_STATE_RELEASE_KIND,
    )
    dependency_ids = set()
    for dependency, expected_kind in zip(top_receipts, expected_top_kinds, strict=True):
        dependency.verify(boundary)
        if dependency.release_kind != expected_kind:
            raise IntegrityError("foundation-set top-level dependency kind is invalid")
        dependency_ids.add(dependency.release_id)
    selection_manifest = top_receipts[0].verify(boundary)
    market_state_manifest = top_receipts[3].verify(boundary)
    market_state_contract = _read_canonical_object(
        boundary.active_root
        / top_receipts[3].relative_root
        / "market_state_contract.json",
        description="foundation-set market-state contract",
    )
    if (
        selection_manifest.metadata.get("query_manifest_id")
        != payload.get("query_manifest_id")
        or market_state_manifest.metadata.get("query_manifest_id")
        != payload.get("query_manifest_id")
        or market_state_contract.get("query_manifest_id")
        != payload.get("query_manifest_id")
        or market_state_contract.get("query_mode_census")
        != payload.get("query_mode_census")
    ):
        raise IntegrityError("foundation-set query dependency closure is invalid")
    raw_intervals = payload.get("intervals")
    if (
        not isinstance(raw_intervals, list)
        or not raw_intervals
        or len(raw_intervals) != payload.get("interval_count")
    ):
        raise IntegrityError("foundation-set interval collection is invalid")
    interval_keys: list[str] = []
    receipt_fields = (
        "raw_release_receipt",
        "definition_release_receipt",
        "causal_release_receipt",
        "status_eligibility_release_receipt",
        "economics_release_receipt",
        "feature_input_release_receipt",
        "outcome_source_input_release_receipt",
    )
    expected_interval_kinds = {
        "raw_release_receipt": RAW_RELEASE_KIND,
        "definition_release_receipt": DEFINITION_RELEASE_KIND,
        "causal_release_receipt": CAUSAL_RELEASE_KIND,
        "status_eligibility_release_receipt": STATUS_ELIGIBILITY_RELEASE_KIND,
        "economics_release_receipt": ECONOMICS_RELEASE_KIND,
        "feature_input_release_receipt": FEATURE_RELEASE_KIND,
        "outcome_source_input_release_receipt": OUTCOME_SOURCE_RELEASE_KIND,
    }
    aggregate_bar_rows = 0
    aggregate_feature_ready_rows = 0
    aggregate_status_eligible_rows = 0
    aggregate_status_gated_feature_ready_rows = 0
    aggregate_status_resolved_rows = 0
    aggregate_status_unresolved_rows = 0
    observed_status_epoch_gates: list[dict[str, object]] = []
    run_contract_intervals = payload["run_contract"].get("intervals")
    if not isinstance(run_contract_intervals, list):
        raise IntegrityError("foundation-set run query intervals are invalid")
    run_interval_by_key = {
        str(item.get("interval_key")): item
        for item in run_contract_intervals
        if isinstance(item, dict)
    }
    if len(run_interval_by_key) != len(run_contract_intervals):
        raise IntegrityError("foundation-set run query intervals are not unique")
    for raw_interval in raw_intervals:
        if not isinstance(raw_interval, dict) or set(raw_interval) != {
            "bar_source_path",
            "bar_source_sha256",
            "bar_query_contract_id",
            "bar_query_mode_id",
            "causal_release_receipt",
            "coverage_disposition",
            "definition_release_receipt",
            "definition_source_path",
            "definition_source_sha256",
            "definition_query_contract_id",
            "definition_query_mode_id",
            "economics_release_receipt",
            "end",
            "feature_input_release_receipt",
            "feature_ready_rows",
            "interval_key",
            "market",
            "outcome_source_input_release_receipt",
            "raw_release_receipt",
            "status_eligibility_release_receipt",
            "status_epoch_gate",
            "status_eligible_rows",
            "status_gated_feature_ready_rows",
            "status_resolved_rows",
            "status_unresolved_rows",
            "start",
            "year",
        }:
            raise IntegrityError("foundation-set interval schema is invalid")
        key = raw_interval.get("interval_key")
        market = raw_interval.get("market")
        year = raw_interval.get("year")
        start = raw_interval.get("start")
        end = raw_interval.get("end")
        if (
            type(key) is not str
            or type(market) is not str
            or type(year) is not int
            or type(start) is not str
            or type(end) is not str
            or key != _interval_key(market, year, start, end)
            or any(
                re.fullmatch(r"[0-9a-f]{64}", str(raw_interval.get(name))) is None
                for name in (
                    "bar_query_contract_id",
                    "bar_query_mode_id",
                    "bar_source_sha256",
                    "definition_query_contract_id",
                    "definition_query_mode_id",
                    "definition_source_sha256",
                )
            )
            or type(raw_interval.get("bar_source_path")) is not str
            or type(raw_interval.get("definition_source_path")) is not str
            or type(raw_interval.get("coverage_disposition")) is not str
            or any(
                type(raw_interval.get(name)) is not int
                or int(raw_interval.get(name)) < 0
                for name in (
                    "feature_ready_rows",
                    "status_eligible_rows",
                    "status_gated_feature_ready_rows",
                    "status_resolved_rows",
                    "status_unresolved_rows",
                )
            )
        ):
            raise IntegrityError("foundation-set interval key is invalid")
        status_epoch_gate = raw_interval.get("status_epoch_gate")
        expected_gate_keys = {
            "bar_rows",
            "feature_ready_rows",
            "interval_key",
            "research_disposition",
            "status_epoch_gate_id",
            "status_gated_feature_ready_fraction",
            "status_gated_feature_ready_rows",
            "status_eligible_rows",
            "status_query_contract_ids",
            "status_query_mode_ids",
            "status_resolved_decision_fraction",
            "status_resolved_rows",
            "status_source_present",
            "status_unresolved_rows",
        }
        if (
            not isinstance(status_epoch_gate, dict)
            or set(status_epoch_gate) != expected_gate_keys
        ):
            raise IntegrityError("foundation-set status epoch gate schema is invalid")
        gate_core = {
            name: value
            for name, value in status_epoch_gate.items()
            if name != "status_epoch_gate_id"
        }
        if (
            status_epoch_gate["status_epoch_gate_id"] != sha256_json(gate_core)
            or status_epoch_gate["interval_key"] != key
            or status_epoch_gate["research_disposition"]
            not in {"ELIGIBLE", "ABSTAIN_STATUS_COVERAGE"}
            or type(status_epoch_gate["status_source_present"]) is not bool
            or not isinstance(status_epoch_gate["status_query_contract_ids"], list)
            or not isinstance(status_epoch_gate["status_query_mode_ids"], list)
        ):
            raise IntegrityError("foundation-set status epoch gate is invalid")
        observed_status_epoch_gates.append(status_epoch_gate)
        interval_keys.append(key)
        parsed_receipts: dict[str, VerifiedReleaseReceipt] = {}
        for name in receipt_fields:
            parsed = _receipt(raw_interval.get(name), name=name)
            parsed.verify(boundary)
            if parsed.release_kind != expected_interval_kinds[name]:
                raise IntegrityError(
                    "foundation-set interval dependency kind is invalid"
                )
            parsed_receipts[name] = parsed
            dependency_ids.add(parsed.release_id)
        loaded_raw = load_raw_interval(
            parsed_receipts["raw_release_receipt"], boundary=boundary
        ).interval_receipt
        if (
            loaded_raw.get("bar_query_contract_id")
            != raw_interval["bar_query_contract_id"]
            or loaded_raw.get("definition_query_contract_id")
            != raw_interval["definition_query_contract_id"]
            or loaded_raw.get("source_bar_file_sha256")
            != raw_interval["bar_source_sha256"]
            or loaded_raw.get("source_definition_file_sha256")
            != raw_interval["definition_source_sha256"]
            or loaded_raw.get("source_bar_file_path")
            != raw_interval["bar_source_path"]
            or loaded_raw.get("source_definition_file_path")
            != raw_interval["definition_source_path"]
        ):
            raise IntegrityError("foundation-set raw query provenance is invalid")
        outcome_payload = load_outcome_source_input(
            parsed_receipts["outcome_source_input_release_receipt"],
            boundary=boundary,
        )
        expected_outcome_dependencies = {
            "causal_release_receipt": parsed_receipts[
                "causal_release_receipt"
            ].as_dict(),
            "definition_release_receipt": parsed_receipts[
                "definition_release_receipt"
            ].as_dict(),
            "economics_release_receipt": parsed_receipts[
                "economics_release_receipt"
            ].as_dict(),
            "foundation_policy_receipt": top_receipts[1].as_dict(),
            "session_policy_receipt": top_receipts[2].as_dict(),
        }
        if any(
            outcome_payload[name] != expected
            for name, expected in expected_outcome_dependencies.items()
        ):
            raise IntegrityError("foundation-set outcome-source binding is invalid")
        feature_manifest = parsed_receipts["feature_input_release_receipt"].verify(
            boundary
        )
        status_contract = load_status_eligibility(
            parsed_receipts["status_eligibility_release_receipt"],
            causal_receipt=parsed_receipts["causal_release_receipt"],
            market_state_receipt=top_receipts[3],
            boundary=boundary,
        )
        status_keys = status_eligible_decision_keys(
            parsed_receipts["status_eligibility_release_receipt"],
            causal_receipt=parsed_receipts["causal_release_receipt"],
            market_state_receipt=top_receipts[3],
            boundary=boundary,
        )
        feature_keys = _feature_ready_join_keys(
            parsed_receipts["feature_input_release_receipt"], boundary=boundary
        )
        observed_status_gated_features = len(feature_keys & status_keys)
        if (
            raw_interval["status_eligible_rows"] != status_contract["eligible_rows"]
            or raw_interval["status_resolved_rows"]
            != status_contract["resolved_status_rows"]
            or raw_interval["status_unresolved_rows"]
            != status_contract["unresolved_status_rows"]
            or raw_interval["status_gated_feature_ready_rows"]
            != observed_status_gated_features
        ):
            raise IntegrityError("foundation-set status census binding is invalid")
        if parsed_receipts[
            "outcome_source_input_release_receipt"
        ].release_id in feature_manifest.source_release_ids:
            raise IntegrityError("feature and outcome-source roles are not isolated")
        if raw_interval["feature_ready_rows"] != feature_manifest.metadata.get(
            "feature_ready_rows"
        ):
            raise IntegrityError("foundation-set feature-ready census is invalid")
        run_interval = run_interval_by_key.get(str(key))
        status_sources = (
            run_interval.get("status_source_files")
            if isinstance(run_interval, dict)
            else None
        )
        if not isinstance(status_sources, list):
            raise IntegrityError("foundation-set status query sources are invalid")
        expected_query_contract_ids = sorted(
            str(item.get("query_contract_id"))
            for item in status_sources
            if isinstance(item, dict)
        )
        expected_query_mode_ids = sorted(
            {
                str(item.get("query_mode_id"))
                for item in status_sources
                if isinstance(item, dict)
            }
        )
        interval_bar_rows = int(status_contract["total_rows"])
        interval_feature_rows = int(raw_interval["feature_ready_rows"])
        interval_resolved_fraction = (
            Decimal(int(status_contract["resolved_status_rows"]))
            / Decimal(interval_bar_rows)
            if interval_bar_rows
            else Decimal(0)
        )
        interval_gated_fraction = (
            Decimal(observed_status_gated_features) / Decimal(interval_feature_rows)
            if interval_feature_rows
            else Decimal(0)
        )
        expected_disposition = (
            "ELIGIBLE"
            if interval_resolved_fraction
            >= coverage_policy.minimum_status_resolved_decision_fraction
            and interval_gated_fraction
            >= coverage_policy.minimum_status_gated_feature_ready_fraction
            else "ABSTAIN_STATUS_COVERAGE"
        )
        if (
            status_epoch_gate["bar_rows"] != interval_bar_rows
            or status_epoch_gate["feature_ready_rows"] != interval_feature_rows
            or status_epoch_gate["status_gated_feature_ready_rows"]
            != observed_status_gated_features
            or status_epoch_gate["status_eligible_rows"]
            != status_contract["eligible_rows"]
            or status_epoch_gate["status_resolved_rows"]
            != status_contract["resolved_status_rows"]
            or status_epoch_gate["status_unresolved_rows"]
            != status_contract["unresolved_status_rows"]
            or status_epoch_gate["status_resolved_decision_fraction"]
            != str(interval_resolved_fraction)
            or status_epoch_gate["status_gated_feature_ready_fraction"]
            != str(interval_gated_fraction)
            or status_epoch_gate["research_disposition"] != expected_disposition
            or status_epoch_gate["status_source_present"] != bool(status_sources)
            or status_epoch_gate["status_query_contract_ids"]
            != expected_query_contract_ids
            or status_epoch_gate["status_query_mode_ids"] != expected_query_mode_ids
        ):
            raise IntegrityError("foundation-set per-epoch status gate is invalid")
        aggregate_bar_rows += int(status_contract["total_rows"])
        aggregate_feature_ready_rows += int(raw_interval["feature_ready_rows"])
        aggregate_status_eligible_rows += int(raw_interval["status_eligible_rows"])
        aggregate_status_gated_feature_ready_rows += int(
            raw_interval["status_gated_feature_ready_rows"]
        )
        aggregate_status_resolved_rows += int(raw_interval["status_resolved_rows"])
        aggregate_status_unresolved_rows += int(raw_interval["status_unresolved_rows"])
    if interval_keys != sorted(set(interval_keys)):
        raise IntegrityError("foundation-set intervals are not unique and sorted")
    coverage_gate = payload["coverage_gate"]
    expected_status_resolved_fraction = (
        Decimal(aggregate_status_resolved_rows) / Decimal(aggregate_bar_rows)
        if aggregate_bar_rows
        else Decimal(0)
    )
    expected_status_gated_feature_ready_fraction = (
        Decimal(aggregate_status_gated_feature_ready_rows)
        / Decimal(aggregate_feature_ready_rows)
        if aggregate_feature_ready_rows
        else Decimal(0)
    )
    if (
        coverage_gate.get("bar_rows") != aggregate_bar_rows
        or coverage_gate.get("feature_ready_rows") != aggregate_feature_ready_rows
        or coverage_gate.get("status_eligible_rows")
        != aggregate_status_eligible_rows
        or coverage_gate.get("status_epoch_gates") != observed_status_epoch_gates
        or coverage_gate.get("status_epoch_gates_id")
        != sha256_json(observed_status_epoch_gates)
        or coverage_gate.get("research_eligible_interval_count")
        != sum(
            gate["research_disposition"] == "ELIGIBLE"
            for gate in observed_status_epoch_gates
        )
        or coverage_gate.get("research_abstained_interval_count")
        != sum(
            gate["research_disposition"] == "ABSTAIN_STATUS_COVERAGE"
            for gate in observed_status_epoch_gates
        )
        or len(observed_status_epoch_gates) != len(raw_intervals)
        or not any(
            gate["research_disposition"] == "ELIGIBLE"
            for gate in observed_status_epoch_gates
        )
        or coverage_gate.get("status_gated_feature_ready_rows")
        != aggregate_status_gated_feature_ready_rows
        or coverage_gate.get("status_resolved_rows")
        != aggregate_status_resolved_rows
        or coverage_gate.get("status_unresolved_rows")
        != aggregate_status_unresolved_rows
        or aggregate_bar_rows
        != aggregate_status_resolved_rows + aggregate_status_unresolved_rows
        or aggregate_bar_rows < coverage_policy.minimum_bar_rows
        or aggregate_status_eligible_rows
        < coverage_policy.minimum_status_eligible_rows
        or aggregate_status_gated_feature_ready_rows
        < coverage_policy.minimum_status_gated_feature_ready_rows
        or coverage_gate.get("status_resolved_decision_fraction")
        != str(expected_status_resolved_fraction)
        or expected_status_resolved_fraction
        < coverage_policy.minimum_status_resolved_decision_fraction
        or coverage_gate.get("status_gated_feature_ready_fraction")
        != str(expected_status_gated_feature_ready_fraction)
        or expected_status_gated_feature_ready_fraction
        < coverage_policy.minimum_status_gated_feature_ready_fraction
        or Decimal(str(coverage_gate.get("status_source_market_year_fraction")))
        < coverage_policy.minimum_status_source_market_year_fraction
        or Decimal(
            str(coverage_gate.get("statistics_source_market_year_fraction"))
        )
        < coverage_policy.minimum_statistics_source_market_year_fraction
    ):
        raise IntegrityError("foundation-set aggregate coverage census is invalid")
    if tuple(sorted(dependency_ids)) != manifest.source_release_ids:
        raise IntegrityError("foundation-set manifest lacks exact dependency closure")
    payload["foundation_set_id"] = foundation_set_id
    return payload


def _load_feature_spec(path: Path, *, boundary: RepoBoundary) -> CausalFeatureSpec:
    boundary.assert_active_path(
        path, purpose="foundation feature specification", subtree="configs"
    )
    return CausalFeatureSpec.from_dict(
        _read_canonical_object(path, description="foundation feature specification")
    )


def _boundary_from_contract(repository_root: Path, source_contract: Path) -> RepoBoundary:
    payload = json.loads(source_contract.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ContractError("source contract must be a JSON object")
    boundary = RepoBoundary(
        Path(str(payload["active_repository"])),
        legacy_roots=(Path(str(payload["legacy_repository"])),),
        foreign_roots=(
            Path.home() / "Desktop" / "US_stocks_swing_model",
            Path.home() / "Desktop" / "US_stocks_swing_model_v2",
        ),
    )
    boundary.assert_active_root(repository_root)
    boundary.assert_active_path(
        source_contract, purpose="source contract", subtree="configs"
    )
    return boundary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    parser.add_argument("--source-snapshot-root", type=Path, required=True)
    parser.add_argument("--source-selection-release", type=Path, required=True)
    parser.add_argument("--feature-spec", type=Path, required=True)
    parser.add_argument("--batch-rows", type=int, default=100_000)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error("foundation publication requires explicit --execute")
    boundary = _boundary_from_contract(args.repository_root, args.source_contract)
    feature_spec = _load_feature_spec(args.feature_spec, boundary=boundary)
    snapshot = PublishedSourceSnapshot.open(
        args.source_snapshot_root, boundary=boundary
    )
    selection_receipt = VerifiedReleaseReceipt.from_release(
        args.source_selection_release, boundary
    )
    selection = load_source_selection(
        selection_receipt, snapshot=snapshot, boundary=boundary
    )
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={
            "feature_spec_hash": feature_spec.spec_hash,
            "selection_manifest_id": str(selection["selection_manifest_id"]),
            "source_snapshot_id": snapshot.source_snapshot_id,
        },
    )
    result = FoundationOrchestrator(
        boundary=boundary,
        operation_receipt=operation,
        batch_rows=args.batch_rows,
    ).run(
        source_snapshot_root=args.source_snapshot_root,
        source_selection_receipt=selection_receipt,
        feature_spec=feature_spec,
    )
    print(canonical_bytes(result.as_dict()).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
