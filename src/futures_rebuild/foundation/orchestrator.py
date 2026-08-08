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
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Callable, Mapping, Sequence

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
from ..exchange_calendar import (
    COVERAGE_RELEASE_KIND as CALENDAR_COVERAGE_RELEASE_KIND,
    ELIGIBILITY_RELEASE_KIND as CALENDAR_ELIGIBILITY_RELEASE_KIND,
    INDEX_RELEASE_KIND as CALENDAR_INDEX_RELEASE_KIND,
    LoadedFoundationCalendarCoverage,
    load_calendar_state_eligibility,
    load_active_calendar_index,
    load_exchange_calendar_policy,
    load_foundation_calendar_coverage,
    publish_calendar_state_eligibility,
    publish_foundation_calendar_coverage,
    verify_calendar_freshness,
)
from ..source_contract import legacy_roots_from_contract
from ..locking import FileLease
from ..producer_bridge import (
    DEFINITION_RELEASE_KIND,
    ECONOMICS_RELEASE_KIND,
    SESSION_RELEASE_KIND,
    CausalFeatureSpec,
    load_actual_contract_definitions,
    load_versioned_session_policy,
    publish_actual_contract_definitions,
    publish_actual_contract_economics,
    publish_versioned_session_policy,
    verify_actual_contract_economics_context,
)
from ..data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
    verify_data_release_manifest,
)
from .coverage import StatusResearchScopePolicy
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
)
from .historical_observability import (
    FOUNDATION_OBSERVABILITY_SCHEMA_VERSION,
    build_historical_observability_coverage,
    load_foundation_observability_successor,
    load_historical_observability_policy,
)
from .resources import (
    FoundationResourcePolicy,
    assert_capacity_admission,
    assert_runtime_capacity,
)
from .selection import (
    SELECTION_RELEASE_KIND,
    load_source_selection_with_resolution,
)
from .snapshot import PublishedDbnRelease as PublishedSourceSnapshot
from .support import (
    POLICY_RELEASE_KIND,
    VerifiedFoundationPolicies,
    publish_foundation_policies,
)


CHECKPOINT_VERSION = "5.0.0"
FOUNDATION_SET_RELEASE_KIND = "futures_mechanical_foundation_set"
FOUNDATION_SET_SCHEMA_VERSION = "4.0.0"
FOUNDATION_SUCCESSOR_SCHEMA_VERSION = "5.0.0"
FOUNDATION_CALENDAR_SCHEMA_VERSION = "6.0.0"
OUTCOME_SOURCE_RELEASE_KIND = "futures_outcome_source_input"
OUTCOME_SOURCE_SCHEMA_VERSION = "1.0.0"
OUTCOME_SOURCE_CALENDAR_SCHEMA_VERSION = "2.0.0"
OUTCOME_SOURCE_ROLE = "LABELABLE_VERIFIED_CAUSAL_BARS_ONLY"
FEATURE_SOURCE_INPUT_RELEASE_KIND = "futures_feature_source_input"
FEATURE_SOURCE_INPUT_SCHEMA_VERSION = "1.0.0"
FEATURE_SOURCE_INPUT_CALENDAR_SCHEMA_VERSION = "2.0.0"
FEATURE_SOURCE_INPUT_ROLE = "DEFERRED_DETERMINISTIC_BAR_LOCAL_FEATURE_INPUT"
FEATURE_MATERIALIZATION_DEFERRED_UNTIL = (
    "SEPARATELY_AUTHORIZED_PREDECLARED_SAMPLE_OR_HYPOTHESIS_CONTRACT"
)
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
    "data_layout.py",
    "economics.py",
    "exchange_calendar.py",
    "foundation/decoder.py",
    "foundation/coverage.py",
    "foundation/economics.py",
    "foundation/identity.py",
    "foundation/historical_observability.py",
    "foundation/materialize.py",
    "foundation/market_state.py",
    "foundation/calendar_successor.py",
    "foundation/orchestrator.py",
    "foundation/parquet.py",
    "foundation/pipeline.py",
    "foundation/policy.py",
    "foundation/records.py",
    "foundation/resources.py",
    "foundation/selection.py",
    "foundation/snapshot.py",
    "foundation/successor.py",
    "foundation/successor_contract.py",
    "foundation/support.py",
    "identity.py",
    "inference.py",
    "locking.py",
    "producer_bridge.py",
    "session_policy.py",
    "source_symbology.py",
    "time_contracts.py",
)
_CONFIG_FILES = (
    "contract_economics_rules.json",
    "environment.lock.json",
    "exchange_calendar_policy.json",
    "historical_observability_policy.json",
    "foundation_policy.json",
    "foundation_coverage_policy.json",
    "foundation_resource_policy.json",
    "known_anomalies.json",
    "mechanical_feature_spec.json",
    "provider_data_epochs.json",
    "session_policy.json",
    "statistics_foundation_roles.json",
    "status_research_scope_policy.json",
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


def _source_family_coverage_passes(
    contract: Mapping[str, object],
    *,
    coverage_policy: FoundationCoveragePolicy,
) -> bool:
    """Gate research scope while preserving the complete archive census."""

    try:
        archive_status_fraction = Decimal(
            str(contract["status_source_market_year_fraction"])
        )
        research_status_fraction = Decimal(
            str(contract["research_scope_status_source_market_year_fraction"])
        )
        statistics_fraction = Decimal(
            str(contract["statistics_source_market_year_fraction"])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise IntegrityError(
            "foundation source-family coverage contract is invalid"
        ) from exc
    fractions = (
        archive_status_fraction,
        research_status_fraction,
        statistics_fraction,
    )
    if any(
        not value.is_finite() or value < Decimal(0) or value > Decimal(1)
        for value in fractions
    ):
        raise IntegrityError(
            "foundation source-family coverage fraction is invalid"
        )
    return (
        research_status_fraction
        >= coverage_policy.minimum_status_source_market_year_fraction
        and statistics_fraction
        >= coverage_policy.minimum_statistics_source_market_year_fraction
    )


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


def _checkpoint_payload(core: Mapping[str, object]) -> dict[str, object]:
    return {**core, "checkpoint_id": sha256_json(dict(core))}


def _checkpoint_core(payload: Mapping[str, object]) -> dict[str, object]:
    if set(payload) != {
        "checkpoint_id",
        "checkpoint_version",
        "completed",
        "layout_version",
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
    count += int("calendar_coverage" in completed)
    count += int("market_state" in completed)
    intervals = completed.get("intervals", {})
    if isinstance(intervals, dict):
        for value in intervals.values():
            if isinstance(value, dict):
                count += sum(phase in value for phase in _RECEIPT_PHASES)
                count += int("calendar_eligibility" in value)
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
        allow_legacy_calendar_unbound: bool = False,
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
        if type(allow_legacy_calendar_unbound) is not bool:
            raise ContractError("legacy calendar compatibility flag must be exact")
        self.allow_legacy_calendar_unbound = allow_legacy_calendar_unbound
        self.resource_policy = FoundationResourcePolicy.from_file(
            boundary.active_root / "configs" / "foundation_resource_policy.json"
        )
        self.publisher = AtomicPublisher(
            boundary=boundary,
            operation_receipt=operation_receipt,
            lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
        )
        self.checkpoint_root = boundary.assert_active_path(
            boundary.active_root
            / "state"
            / "foundation_runs_v2"
            / "_boundary_probe",
            purpose="foundation checkpoint root",
            subtree="state/foundation_runs_v2",
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
        calendar_index_receipt: VerifiedReleaseReceipt | None,
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
        manifest = verify_data_release_manifest(
            snapshot.manifest_path,
            self.boundary,
            verify_files=False,
        )
        if (
            manifest.release_id != receipt.release_id
            or sha256_file(snapshot.manifest_path) != receipt.manifest_sha256
        ):
            raise IntegrityError("source DBN manifest changed after snapshot verification")
        contract = {
            "batch_rows": self.batch_rows,
            "calendar_index_receipt": (
                calendar_index_receipt.as_dict()
                if calendar_index_receipt is not None
                else None
            ),
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
            "source_dbn_files_index_sha256": snapshot.files_index_sha256,
            "source_dbn_manifest_sha256": receipt.manifest_sha256,
            "source_dbn_release_id": snapshot.source_release_id,
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
            subtree="state/foundation_runs_v2",
        )
        checkpoint_path = run_root / "checkpoint.json"
        expected_core: dict[str, object] = {
            "checkpoint_version": CHECKPOINT_VERSION,
            "completed": {},
            "layout_version": "2.0.0",
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
            or core.get("layout_version") != "2.0.0"
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
        assert_runtime_capacity(
            volume_path=self.boundary.active_root,
            policy=self.resource_policy,
        )
        _atomic_checkpoint(checkpoint_path, _checkpoint_payload(core))
        if after_checkpoint is not None:
            after_checkpoint(phase)

    def run(
        self,
        *,
        source_dbn_manifest: Path,
        source_selection_receipt: VerifiedReleaseReceipt,
        feature_spec: CausalFeatureSpec,
        calendar_index_receipt: VerifiedReleaseReceipt | None = None,
        after_checkpoint: Callable[[str], None] | None = None,
    ) -> FoundationRunResult:
        global_lock = self.boundary.assert_active_path(
            self.boundary.active_root
            / "state"
            / "locks"
            / "foundation-build.lock",
            purpose="global foundation build lock",
            subtree="state/locks",
        )
        with FileLease(global_lock):
            return self._run_exclusive(
                source_dbn_manifest=source_dbn_manifest,
                source_selection_receipt=source_selection_receipt,
                feature_spec=feature_spec,
                calendar_index_receipt=calendar_index_receipt,
                after_checkpoint=after_checkpoint,
            )

    def _run_exclusive(
        self,
        *,
        source_dbn_manifest: Path,
        source_selection_receipt: VerifiedReleaseReceipt,
        feature_spec: CausalFeatureSpec,
        calendar_index_receipt: VerifiedReleaseReceipt | None,
        after_checkpoint: Callable[[str], None] | None = None,
    ) -> FoundationRunResult:
        snapshot = PublishedSourceSnapshot.open(source_dbn_manifest, boundary=self.boundary)
        selection, resolved_selection = load_source_selection_with_resolution(
            source_selection_receipt,
            snapshot=snapshot,
            boundary=self.boundary,
        )
        intervals = resolved_selection.intervals
        if (
            calendar_index_receipt is None
            and not self.allow_legacy_calendar_unbound
        ):
            raise IntegrityError(
                "HISTORICAL_OBSERVABILITY_CONTRACT_NOT_BOUND"
            )
        if calendar_index_receipt is not None:
            load_exchange_calendar_policy(
                self.boundary.active_root
                / "configs"
                / "exchange_calendar_policy.json"
            )
            expected_calendar_markets = tuple(
                sorted({str(interval.market) for interval in intervals})
            )
            active_calendar_index = load_active_calendar_index(
                boundary=self.boundary,
                expected_markets=expected_calendar_markets,
            )
            if active_calendar_index.receipt != calendar_index_receipt:
                raise IntegrityError(
                    "foundation calendar index is not the active approved index"
                )
            verify_calendar_freshness(
                active_calendar_index,
                expected_markets=expected_calendar_markets,
                now=datetime.now(timezone.utc),
            )
        assert_capacity_admission(
            volume_path=self.boundary.active_root,
            selection=selection,
            policy=self.resource_policy,
        )
        coverage_policy = FoundationCoveragePolicy.from_file(
            self.boundary.active_root / "configs" / "foundation_coverage_policy.json"
        )
        scope_policy = StatusResearchScopePolicy.from_file(
            self.boundary.active_root
            / "configs"
            / "status_research_scope_policy.json"
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
            calendar_index_receipt=calendar_index_receipt,
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
                calendar_index_receipt=calendar_index_receipt,
                coverage_policy=coverage_policy,
                scope_policy=scope_policy,
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
        calendar_index_receipt: VerifiedReleaseReceipt | None,
        coverage_policy: FoundationCoveragePolicy,
        scope_policy: StatusResearchScopePolicy,
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
            "calendar_coverage",
            "market_state",
            "intervals",
            "foundation_set",
        }
        if not set(completed).issubset(allowed_top):
            raise IntegrityError("foundation checkpoint has an unknown completed phase")

        source_bound = {
            "selection_manifest_id": selection["selection_manifest_id"],
            "source_selection_receipt_id": source_selection_receipt.receipt_id,
            "source_dbn_release_id": snapshot.source_release_id,
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
                for key in (
                    "session_policy",
                    "calendar_coverage",
                    "market_state",
                    "intervals",
                    "foundation_set",
                )
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
                key in completed
                for key in (
                    "calendar_coverage",
                    "market_state",
                    "intervals",
                    "foundation_set",
                )
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

        calendar_coverage_receipt: VerifiedReleaseReceipt | None = None
        calendar_coverage: LoadedFoundationCalendarCoverage | None = None
        if calendar_index_receipt is not None:
            if "calendar_coverage" in completed:
                calendar_coverage_receipt = _receipt(
                    completed["calendar_coverage"], name="calendar_coverage"
                )
                calendar_coverage = load_foundation_calendar_coverage(
                    calendar_coverage_receipt,
                    boundary=self.boundary,
                    expected_intervals=intervals,
                )
                if (
                    calendar_coverage.index.receipt
                    != calendar_index_receipt
                ):
                    raise IntegrityError(
                        "foundation calendar coverage changed its active index"
                    )
            else:
                if any(
                    key in completed
                    for key in ("market_state", "intervals", "foundation_set")
                ):
                    raise IntegrityError(
                        "foundation checkpoint skips calendar-coverage phase"
                    )
                calendar_coverage_receipt = publish_foundation_calendar_coverage(
                    index_receipt=calendar_index_receipt,
                    intervals=intervals,
                    publisher=self.publisher,
                )
                calendar_coverage = load_foundation_calendar_coverage(
                    calendar_coverage_receipt,
                    boundary=self.boundary,
                    expected_intervals=intervals,
                )
                completed["calendar_coverage"] = (
                    calendar_coverage_receipt.as_dict()
                )
                self._persist(
                    checkpoint_path,
                    core,
                    phase="calendar_coverage",
                    after_checkpoint=after_checkpoint,
                )
        elif "calendar_coverage" in completed:
            raise IntegrityError(
                "legacy foundation checkpoint unexpectedly binds a calendar"
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
                expected_policies=policies,
                expected_coverage_policy=coverage_policy,
                expected_scope_policy=scope_policy,
                expected_statistics_roles=statistics_roles,
                trusted_checkpoint_mtime_ns=checkpoint_path.stat().st_mtime_ns,
            )
        else:
            if any(key in completed for key in ("intervals", "foundation_set")):
                raise IntegrityError("foundation checkpoint skips market-state phase")
            market_state_receipt = publish_market_state_foundation(
                selection=resolved_selection,
                source_selection_receipt=source_selection_receipt,
                policies=policies,
                coverage_policy=coverage_policy,
                scope_policy=scope_policy,
                statistics_roles=statistics_roles,
                publisher=self.publisher,
                batch_rows=self.batch_rows,
            )
            market_state = load_market_state_foundation(
                market_state_receipt,
                boundary=self.boundary,
                expected_selection=resolved_selection,
                expected_source_selection_receipt=source_selection_receipt,
                expected_policies=policies,
                expected_coverage_policy=coverage_policy,
                expected_scope_policy=scope_policy,
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
        research_bar_rows = 0
        research_status_eligible_rows = 0
        research_status_resolved_rows = 0
        research_status_unresolved_rows = 0
        research_feature_ready_rows = 0
        research_status_gated_feature_ready_rows = 0
        status_epoch_gates: list[dict[str, object]] = []
        for interval in intervals:
            key = _interval_key(
                interval.market, interval.year, interval.start, interval.end
            )
            state = interval_state.setdefault(key, {})
            if not isinstance(state, dict) or not set(state).issubset(
                {*_RECEIPT_PHASES, "calendar_eligibility", "outcome_source_input"}
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
            calendar_eligibility_receipt: VerifiedReleaseReceipt | None = None
            if calendar_coverage_receipt is not None:
                calendar_eligibility_receipt = self._ensure_calendar_eligibility(
                    state,
                    causal_receipt=causal_receipt,
                    coverage_receipt=calendar_coverage_receipt,
                    market=interval.market,
                    year=interval.year,
                    interval_key=key,
                    checkpoint_path=checkpoint_path,
                    core=core,
                    phase=f"{key}:calendar_eligibility",
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
                calendar_coverage_receipt=calendar_coverage_receipt,
                calendar_eligibility_receipt=calendar_eligibility_receipt,
                feature_spec=feature_spec,
                checkpoint_path=checkpoint_path,
                core=core,
                phase=f"{key}:feature_input",
                after_checkpoint=after_checkpoint,
            )
            feature_input = load_feature_source_input(
                feature_receipt, boundary=self.boundary
            )
            status_gated_feature_ready_rows = int(
                status_contract["eligible_rows"]
            )
            interval_bar_rows = int(status_contract["total_rows"])
            interval_feature_ready_rows = int(
                feature_input["feature_ready_rows"]
            )
            if (
                feature_input["causal_release_receipt"]
                != causal_receipt.as_dict()
                or status_gated_feature_ready_rows
                > interval_feature_ready_rows
            ):
                raise IntegrityError(
                    "compact status/feature causal census is invalid"
                )
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
            interval_coverage_passed = (
                interval_status_resolved_fraction
                >= coverage_policy.minimum_status_resolved_decision_fraction
                and interval_status_gated_feature_fraction
                >= coverage_policy.minimum_status_gated_feature_ready_fraction
            )
            in_research_scope = scope_policy.includes_interval(
                start=interval.start, end=interval.end
            )
            research_disposition = scope_policy.disposition(
                start=interval.start,
                end=interval.end,
                coverage_passed=interval_coverage_passed,
            )
            status_epoch_gate_core: dict[str, object] = {
                "bar_rows": interval_bar_rows,
                "feature_ready_rows": interval_feature_ready_rows,
                "in_research_scope": in_research_scope,
                "interval_key": key,
                "research_disposition": research_disposition,
                "research_scope_policy_hash": scope_policy.policy_hash,
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
                calendar_coverage_receipt=calendar_coverage_receipt,
                calendar_eligibility_receipt=calendar_eligibility_receipt,
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
            total_feature_ready_rows += interval_feature_ready_rows
            total_status_gated_feature_ready_rows += (
                status_gated_feature_ready_rows
            )
            if in_research_scope:
                research_bar_rows += interval_bar_rows
                research_status_eligible_rows += int(
                    status_contract["eligible_rows"]
                )
                research_status_resolved_rows += int(
                    status_contract["resolved_status_rows"]
                )
                research_status_unresolved_rows += int(
                    status_contract["unresolved_status_rows"]
                )
                research_feature_ready_rows += interval_feature_ready_rows
                research_status_gated_feature_ready_rows += (
                    status_gated_feature_ready_rows
                )

            verified_intervals.append(
                {
                    "bar_query_contract_id": interval.bars.query_contract_id,
                    "bar_query_mode_id": interval.bars.query_mode_id,
                    "bar_source_path": interval.bars.binding.relative_path,
                    "bar_source_sha256": interval.bars.binding.sha256,
                    "causal_release_receipt": causal_receipt.as_dict(),
                    **(
                        {
                            "calendar_eligibility_release_receipt": (
                                calendar_eligibility_receipt.as_dict()
                            )
                        }
                        if calendar_eligibility_receipt is not None
                        else {}
                    ),
                    "coverage_disposition": interval.coverage_disposition,
                    "definition_release_receipt": definitions.receipt.as_dict(),
                    "definition_query_contract_id": interval.definition.query_contract_id,
                    "definition_query_mode_id": interval.definition.query_mode_id,
                    "definition_source_path": interval.definition.binding.relative_path,
                    "definition_source_sha256": interval.definition.binding.sha256,
                    "economics_release_receipt": economics.release_receipt.as_dict(),
                    "end": interval.end,
                    "feature_input_release_receipt": feature_receipt.as_dict(),
                    "feature_ready_rows": interval_feature_ready_rows,
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

        archive_status_resolved_fraction = (
            Decimal(total_status_resolved_rows) / Decimal(total_bar_rows)
            if total_bar_rows
            else Decimal(0)
        )
        archive_status_gated_feature_ready_fraction = (
            Decimal(total_status_gated_feature_ready_rows)
            / Decimal(total_feature_ready_rows)
            if total_feature_ready_rows
            else Decimal(0)
        )
        status_resolved_fraction = (
            Decimal(research_status_resolved_rows) / Decimal(research_bar_rows)
            if research_bar_rows
            else Decimal(0)
        )
        status_gated_feature_ready_fraction = (
            Decimal(research_status_gated_feature_ready_rows)
            / Decimal(research_feature_ready_rows)
            if research_feature_ready_rows
            else Decimal(0)
        )
        in_scope_gates = [
            gate for gate in status_epoch_gates if gate["in_research_scope"] is True
        ]
        if (
            research_bar_rows < coverage_policy.minimum_bar_rows
            or research_status_gated_feature_ready_rows
            < coverage_policy.minimum_status_gated_feature_ready_rows
            or research_status_eligible_rows
            < coverage_policy.minimum_status_eligible_rows
            or total_bar_rows
            != total_status_resolved_rows + total_status_unresolved_rows
            or research_bar_rows
            != research_status_resolved_rows + research_status_unresolved_rows
            or status_resolved_fraction
            < coverage_policy.minimum_status_resolved_decision_fraction
            or status_gated_feature_ready_fraction
            < coverage_policy.minimum_status_gated_feature_ready_fraction
            or not in_scope_gates
            or any(
                gate["research_disposition"] != "ELIGIBLE"
                for gate in in_scope_gates
            )
            or not _source_family_coverage_passes(
                market_state.contract,
                coverage_policy=coverage_policy,
            )
        ):
            raise IntegrityError(
                "foundation nonzero/status coverage gates are not satisfied"
            )
        archive_census_core = {
            "bar_rows": total_bar_rows,
            "feature_ready_rows": total_feature_ready_rows,
            "missing_status_rows_remain_in_denominator": True,
            "status_eligible_rows": total_status_eligible_rows,
            "status_gated_feature_ready_fraction": str(
                archive_status_gated_feature_ready_fraction
            ),
            "status_gated_feature_ready_rows": (
                total_status_gated_feature_ready_rows
            ),
            "status_resolved_decision_fraction": str(
                archive_status_resolved_fraction
            ),
            "status_resolved_rows": total_status_resolved_rows,
            "status_unresolved_rows": total_status_unresolved_rows,
        }
        archive_census = {
            **archive_census_core,
            "archive_census_id": sha256_json(archive_census_core),
        }
        coverage_gate = {
            "archive_census": archive_census,
            "bar_rows": research_bar_rows,
            "coverage_matrix_id": resolved_selection.coverage_matrix_id,
            "coverage_policy": coverage_policy.as_dict(),
            "coverage_policy_hash": coverage_policy.policy_hash,
            "feature_ready_rows": research_feature_ready_rows,
            "missing_status_rows_remain_in_denominator": True,
            "research_abstained_interval_count": sum(
                gate["in_research_scope"] is False for gate in status_epoch_gates
            ),
            "research_eligible_interval_count": sum(
                gate["research_disposition"] == "ELIGIBLE"
                for gate in status_epoch_gates
            ),
            "research_failed_interval_count": sum(
                gate["research_disposition"] == "FAIL_STATUS_COVERAGE"
                for gate in status_epoch_gates
            ),
            "research_scope_interval_count": len(in_scope_gates),
            "research_scope_policy": scope_policy.as_dict(),
            "research_scope_policy_hash": scope_policy.policy_hash,
            "statistics_feature_use": False,
            "statistics_source_market_year_fraction": market_state.contract[
                "statistics_source_market_year_fraction"
            ],
            "research_scope_status_source_market_year_fraction": (
                market_state.contract[
                    "research_scope_status_source_market_year_fraction"
                ]
            ),
            "status_eligible_rows": research_status_eligible_rows,
            "status_epoch_gates": status_epoch_gates,
            "status_epoch_gates_id": sha256_json(status_epoch_gates),
            "status_gated_feature_ready_rows": (
                research_status_gated_feature_ready_rows
            ),
            "status_gated_feature_ready_fraction": str(
                status_gated_feature_ready_fraction
            ),
            "status_resolved_rows": research_status_resolved_rows,
            "status_resolved_decision_fraction": str(status_resolved_fraction),
            "status_source_market_year_fraction": market_state.contract[
                "status_source_market_year_fraction"
            ],
            "status_unresolved_rows": research_status_unresolved_rows,
            **(
                {
                    "calendar_contract_status": "BOUND_AND_ALL_ROWS_OPEN",
                    "calendar_coverage_receipt_id": (
                        calendar_coverage_receipt.receipt_id
                    ),
                }
                if calendar_coverage_receipt is not None
                else {}
            ),
        }

        foundation_schema_version = (
            FOUNDATION_CALENDAR_SCHEMA_VERSION
            if calendar_coverage_receipt is not None
            else FOUNDATION_SET_SCHEMA_VERSION
        )
        foundation_set_core = {
            "alpha_evidence": False,
            "candidate_eligible": False,
            "dependency_closure_complete": True,
            "coverage_gate": coverage_gate,
            "coverage_matrix": list(resolved_selection.coverage_matrix),
            "coverage_matrix_id": resolved_selection.coverage_matrix_id,
            **(
                {
                    "calendar_coverage_receipt": (
                        calendar_coverage_receipt.as_dict()
                    )
                }
                if calendar_coverage_receipt is not None
                else {}
            ),
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
            "schema_version": foundation_schema_version,
            "session_policy_receipt": session_receipt.as_dict(),
            "source_selection_receipt": source_selection_receipt.as_dict(),
            "source_dbn_release_id": snapshot.source_release_id,
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
            dependency_ids = {
                source_selection_receipt.release_id,
                policy_receipt.release_id,
                session_receipt.release_id,
                market_state_receipt.release_id,
            }
            if calendar_coverage_receipt is not None:
                dependency_ids.add(calendar_coverage_receipt.release_id)
            for item in verified_intervals:
                for name in (
                    "raw_release_receipt",
                    "definition_release_receipt",
                    "causal_release_receipt",
                    *(
                        ("calendar_eligibility_release_receipt",)
                        if calendar_coverage_receipt is not None
                        else ()
                    ),
                    "status_eligibility_release_receipt",
                    "economics_release_receipt",
                    "feature_input_release_receipt",
                    "outcome_source_input_release_receipt",
                ):
                    dependency_ids.add(str(item[name]["release_id"]))
            manifest = ReleaseManifest.build(
                stage,
                phase="foundation",
                release_kind=FOUNDATION_SET_RELEASE_KIND,
                schema_version=foundation_schema_version,
                logical_paths={},
                source_release_ids=tuple(sorted(dependency_ids)),
                embedded_documents={"foundation_set.json": foundation_set},
                metadata={
                    "coverage_matrix_id": resolved_selection.coverage_matrix_id,
                    "feature_spec_hash": feature_spec.spec_hash,
                    "foundation_set_id": foundation_set["foundation_set_id"],
                    "interval_count": len(verified_intervals),
                    "query_manifest_id": resolved_selection.query_manifest_id,
                    "run_id": core["run_id"],
                    "source_dbn_release_id": snapshot.source_release_id,
                },
            )
            manifest_path = self.publisher.publish(stage, manifest)
            foundation_set_receipt = VerifiedReleaseReceipt.from_manifest(
                manifest_path, self.boundary
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
            or report.get("source_dbn_release_id")
            != interval.bars.binding.source_release_id
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

    def _ensure_calendar_eligibility(
        self,
        state: dict[str, object],
        *,
        causal_receipt: VerifiedReleaseReceipt,
        coverage_receipt: VerifiedReleaseReceipt,
        market: str,
        year: int,
        interval_key: str,
        checkpoint_path: Path,
        core: dict[str, object],
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ) -> VerifiedReleaseReceipt:
        if "calendar_eligibility" in state:
            receipt = _receipt(state["calendar_eligibility"], name=phase)
        else:
            if set(state) != {"raw", "definitions", "causal"}:
                raise IntegrityError(
                    "foundation interval skips calendar-eligibility phase"
                )
            receipt = publish_calendar_state_eligibility(
                causal_receipt=causal_receipt,
                coverage_receipt=coverage_receipt,
                market=market,
                year=year,
                interval_key=interval_key,
                publisher=self.publisher,
            )
            state["calendar_eligibility"] = receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase=phase,
                after_checkpoint=after_checkpoint,
            )
        payload = load_calendar_state_eligibility(
            receipt,
            boundary=self.boundary,
            expected_causal_receipt=causal_receipt,
            expected_coverage_receipt=coverage_receipt,
        )
        if (
            payload["disposition"] != "ELIGIBLE"
            or payload["interval_key"] != interval_key
            or payload["market"] != market
            or payload["year"] != year
        ):
            raise IntegrityError("foundation calendar-state eligibility failed closed")
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
            if set(state) not in (
                {"raw", "definitions", "causal"},
                {"raw", "definitions", "causal", "calendar_eligibility"},
            ):
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
            return verify_actual_contract_economics_context(
                receipt,
                causal_receipt=causal_receipt,
                definitions=definitions,
                policies=policies,
                session_policy=session_policy,
                boundary=self.boundary,
            )
        else:
            expected = {"raw", "definitions", "causal", "status_eligibility"}
            if "calendar_eligibility" in state:
                expected.add("calendar_eligibility")
            if set(state) != expected:
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
        return verify_actual_contract_economics_context(
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
        calendar_coverage_receipt: VerifiedReleaseReceipt | None,
        calendar_eligibility_receipt: VerifiedReleaseReceipt | None,
        feature_spec: CausalFeatureSpec,
        checkpoint_path: Path,
        core: dict[str, object],
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ) -> VerifiedReleaseReceipt:
        calendar_bound = (
            calendar_coverage_receipt is not None
            and calendar_eligibility_receipt is not None
        )
        if (calendar_coverage_receipt is None) != (
            calendar_eligibility_receipt is None
        ):
            raise IntegrityError("feature input has partial calendar binding")
        dependency_receipts = (
            causal_receipt,
            definitions.receipt,
            economics.release_receipt,
            policies.receipt,
            session_policy.receipt,
            *(
                (
                    calendar_coverage_receipt,
                    calendar_eligibility_receipt,
                )
                if calendar_bound
                else ()
            ),
        )
        _, causal_report = load_causal_interval(
            causal_receipt, boundary=self.boundary
        )
        dispositions = causal_report.get("disposition_counts")
        total = causal_report.get("row_count")
        if (
            not isinstance(dispositions, dict)
            or type(total) is not int
            or total <= 0
            or any(type(value) is not int or value < 0 for value in dispositions.values())
            or sum(dispositions.values()) != total
        ):
            raise IntegrityError("causal feature-source census is invalid")
        ready = int(dispositions.get("ELIGIBLE", 0))
        unresolved = total - ready
        payload_core = {
            "bar_local_deterministic": True,
            "causal_release_receipt": causal_receipt.as_dict(),
            **(
                {
                    "calendar_coverage_receipt": (
                        calendar_coverage_receipt.as_dict()
                    ),
                    "calendar_state_eligibility_receipt": (
                        calendar_eligibility_receipt.as_dict()
                    ),
                }
                if calendar_bound
                else {}
            ),
            "definition_release_receipt": definitions.receipt.as_dict(),
            "economics_release_receipt": economics.release_receipt.as_dict(),
            "feature_ready_rows": ready,
            "feature_spec": feature_spec.as_dict(),
            "feature_spec_hash": feature_spec.spec_hash,
            "features_materialized": False,
            "fit_or_global_state": False,
            "foundation_policy_receipt": policies.receipt.as_dict(),
            "materialization_deferred_until": (
                FEATURE_MATERIALIZATION_DEFERRED_UNTIL
            ),
            "prediction_ledger_read": False,
            "role": FEATURE_SOURCE_INPUT_ROLE,
            "schema_version": (
                FEATURE_SOURCE_INPUT_CALENDAR_SCHEMA_VERSION
                if calendar_bound
                else FEATURE_SOURCE_INPUT_SCHEMA_VERSION
            ),
            "session_policy_receipt": session_policy.receipt.as_dict(),
            "total_upstream_rows": total,
            "unresolved_upstream_rows": unresolved,
            "uses_future_outcome": False,
        }
        payload = {
            **payload_core,
            "feature_source_input_id": sha256_json(payload_core),
        }
        if "feature_input" in state:
            receipt = _receipt(state["feature_input"], name=phase)
        else:
            expected_state = {
                "raw",
                "definitions",
                "causal",
                "status_eligibility",
                "economics",
            }
            if calendar_bound:
                expected_state.add("calendar_eligibility")
            if set(state) != expected_state:
                raise IntegrityError("foundation interval skips feature-input phase")
            causal_manifest = causal_receipt.verify(self.boundary)
            causal_root = str(causal_manifest.metadata.get("logical_root", ""))
            prefix = "data/causally_gated_normalized/"
            if not causal_root.startswith(prefix):
                raise IntegrityError(
                    "feature source input lacks a layout-v2 causal selector"
                )
            feature_root = (
                f"data/features/{feature_spec.spec_hash}/"
                f"{causal_root.removeprefix(prefix)}"
            )
            stage = self.publisher.create_stage("feature_source_input")
            staged_name = "feature_source_input.json"
            (stage / staged_name).write_bytes(canonical_bytes(payload) + b"\n")
            manifest = ReleaseManifest.build(
                stage,
                phase="features",
                release_kind=FEATURE_SOURCE_INPUT_RELEASE_KIND,
                schema_version=(
                    FEATURE_SOURCE_INPUT_CALENDAR_SCHEMA_VERSION
                    if calendar_bound
                    else FEATURE_SOURCE_INPUT_SCHEMA_VERSION
                ),
                logical_paths={
                    staged_name: f"{feature_root}/{staged_name}"
                },
                source_release_ids=tuple(
                    dependency.release_id for dependency in dependency_receipts
                ),
                metadata={
                    "causal_release_id": causal_receipt.release_id,
                    "feature_ready_rows": ready,
                    "feature_source_input_id": payload[
                        "feature_source_input_id"
                    ],
                    "feature_spec_hash": feature_spec.spec_hash,
                    "role": FEATURE_SOURCE_INPUT_ROLE,
                    "total_upstream_rows": total,
                    "unresolved_upstream_rows": unresolved,
                },
            )
            manifest_path = self.publisher.publish(stage, manifest)
            receipt = VerifiedReleaseReceipt.from_manifest(
                manifest_path, self.boundary
            )
            state["feature_input"] = receipt.as_dict()
            self._persist(
                checkpoint_path,
                core,
                phase=phase,
                after_checkpoint=after_checkpoint,
            )
        observed = load_feature_source_input(receipt, boundary=self.boundary)
        if observed != payload:
            raise IntegrityError(
                "feature source input differs from exact dependencies"
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
        calendar_coverage_receipt: VerifiedReleaseReceipt | None,
        calendar_eligibility_receipt: VerifiedReleaseReceipt | None,
        checkpoint_path: Path,
        core: dict[str, object],
        phase: str,
        after_checkpoint: Callable[[str], None] | None,
    ) -> VerifiedReleaseReceipt:
        calendar_bound = (
            calendar_coverage_receipt is not None
            and calendar_eligibility_receipt is not None
        )
        if (calendar_coverage_receipt is None) != (
            calendar_eligibility_receipt is None
        ):
            raise IntegrityError("outcome input has partial calendar binding")
        dependency_receipts = (
            causal_receipt,
            definitions.receipt,
            economics.release_receipt,
            policies.receipt,
            session_policy.receipt,
            *(
                (
                    calendar_coverage_receipt,
                    calendar_eligibility_receipt,
                )
                if calendar_bound
                else ()
            ),
        )
        payload_core = {
            "causal_release_receipt": causal_receipt.as_dict(),
            **(
                {
                    "calendar_coverage_receipt": (
                        calendar_coverage_receipt.as_dict()
                    ),
                    "calendar_state_eligibility_receipt": (
                        calendar_eligibility_receipt.as_dict()
                    ),
                }
                if calendar_bound
                else {}
            ),
            "definition_release_receipt": definitions.receipt.as_dict(),
            "deferred_until": OUTCOME_DEFERRED_UNTIL,
            "economics_release_receipt": economics.release_receipt.as_dict(),
            "foundation_policy_receipt": policies.receipt.as_dict(),
            "labels_materialized": False,
            "outcomes_materialized": False,
            "prediction_ledger_read": False,
            "role": OUTCOME_SOURCE_ROLE,
            "schema_version": (
                OUTCOME_SOURCE_CALENDAR_SCHEMA_VERSION
                if calendar_bound
                else OUTCOME_SOURCE_SCHEMA_VERSION
            ),
            "session_policy_receipt": session_policy.receipt.as_dict(),
        }
        payload = {
            **payload_core,
            "outcome_source_input_id": sha256_json(payload_core),
        }
        if "outcome_source_input" in state:
            receipt = _receipt(state["outcome_source_input"], name=phase)
        else:
            expected_state = {
                "raw",
                "definitions",
                "causal",
                "status_eligibility",
                "economics",
                "feature_input",
            }
            if calendar_bound:
                expected_state.add("calendar_eligibility")
            if set(state) != expected_state:
                raise IntegrityError("foundation interval skips outcome-source phase")
            causal_manifest = causal_receipt.verify(self.boundary)
            causal_root = str(causal_manifest.metadata.get("logical_root", ""))
            prefix = "data/causally_gated_normalized/"
            if not causal_root.startswith(prefix):
                raise IntegrityError("outcome source lacks a layout-v2 causal selector")
            outcome_root = f"data/outcome_sources/{causal_root.removeprefix(prefix)}"
            stage = self.publisher.create_stage("outcome_source_input")
            staged_name = "outcome_source_input.json"
            (stage / staged_name).write_bytes(canonical_bytes(payload) + b"\n")
            manifest = ReleaseManifest.build(
                stage,
                phase="outcome_sources",
                release_kind=OUTCOME_SOURCE_RELEASE_KIND,
                schema_version=(
                    OUTCOME_SOURCE_CALENDAR_SCHEMA_VERSION
                    if calendar_bound
                    else OUTCOME_SOURCE_SCHEMA_VERSION
                ),
                logical_paths={
                    staged_name: f"{outcome_root}/{staged_name}"
                },
                source_release_ids=tuple(
                    receipt.release_id for receipt in dependency_receipts
                ),
                metadata={
                    "causal_release_id": causal_receipt.release_id,
                    "outcome_source_input_id": payload["outcome_source_input_id"],
                    "role": OUTCOME_SOURCE_ROLE,
                },
            )
            manifest_path = self.publisher.publish(stage, manifest)
            receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, self.boundary)
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


def load_feature_source_input(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    calendar_bound = (
        manifest.schema_version == FEATURE_SOURCE_INPUT_CALENDAR_SCHEMA_VERSION
    )
    if (
        receipt.phase != "features"
        or manifest.release_kind != FEATURE_SOURCE_INPUT_RELEASE_KIND
        or manifest.schema_version
        not in {
            FEATURE_SOURCE_INPUT_SCHEMA_VERSION,
            FEATURE_SOURCE_INPUT_CALENDAR_SCHEMA_VERSION,
        }
        or len(manifest.files) != 1
        or Path(manifest.files[0].logical_path).name
        != "feature_source_input.json"
        or set(manifest.metadata)
        != {
            "causal_release_id",
            "feature_ready_rows",
            "feature_source_input_id",
            "feature_spec_hash",
            "role",
            "total_upstream_rows",
            "unresolved_upstream_rows",
        }
        or manifest.metadata.get("role") != FEATURE_SOURCE_INPUT_ROLE
    ):
        raise IntegrityError("feature-source release contract is invalid")
    path = receipt.resolve_unique_filename("feature_source_input.json", boundary)
    payload = _read_canonical_object(path, description="feature-source input")
    expected = {
        "bar_local_deterministic",
        "causal_release_receipt",
        "definition_release_receipt",
        "economics_release_receipt",
        "feature_ready_rows",
        "feature_source_input_id",
        "feature_spec",
        "feature_spec_hash",
        "features_materialized",
        "fit_or_global_state",
        "foundation_policy_receipt",
        "materialization_deferred_until",
        "prediction_ledger_read",
        "role",
        "schema_version",
        "session_policy_receipt",
        "total_upstream_rows",
        "unresolved_upstream_rows",
        "uses_future_outcome",
    }
    if calendar_bound:
        expected.update(
            {
                "calendar_coverage_receipt",
                "calendar_state_eligibility_receipt",
            }
        )
    if set(payload) != expected:
        raise IntegrityError("feature-source payload schema is invalid")
    feature_source_input_id = payload.pop("feature_source_input_id", None)
    spec = CausalFeatureSpec.from_dict(payload.get("feature_spec"))
    counts = (
        payload.get("total_upstream_rows"),
        payload.get("feature_ready_rows"),
        payload.get("unresolved_upstream_rows"),
    )
    if (
        feature_source_input_id != sha256_json(payload)
        or feature_source_input_id
        != manifest.metadata["feature_source_input_id"]
        or payload.get("schema_version") != manifest.schema_version
        or payload.get("role") != FEATURE_SOURCE_INPUT_ROLE
        or payload.get("feature_spec_hash") != spec.spec_hash
        or payload.get("feature_spec_hash")
        != manifest.metadata["feature_spec_hash"]
        or payload.get("materialization_deferred_until")
        != FEATURE_MATERIALIZATION_DEFERRED_UNTIL
        or payload.get("bar_local_deterministic") is not True
        or payload.get("features_materialized") is not False
        or payload.get("fit_or_global_state") is not False
        or payload.get("uses_future_outcome") is not False
        or payload.get("prediction_ledger_read") is not False
        or any(type(value) is not int or value < 0 for value in counts)
        or counts[0] <= 0
        or counts[1] + counts[2] != counts[0]
        or manifest.metadata["total_upstream_rows"] != counts[0]
        or manifest.metadata["feature_ready_rows"] != counts[1]
        or manifest.metadata["unresolved_upstream_rows"] != counts[2]
    ):
        raise IntegrityError("feature-source identity or safety posture is invalid")
    receipt_fields = (
        ("causal_release_receipt", CAUSAL_RELEASE_KIND),
        ("definition_release_receipt", DEFINITION_RELEASE_KIND),
        ("economics_release_receipt", ECONOMICS_RELEASE_KIND),
        ("foundation_policy_receipt", POLICY_RELEASE_KIND),
        ("session_policy_receipt", SESSION_RELEASE_KIND),
        *(
            (
                ("calendar_coverage_receipt", CALENDAR_COVERAGE_RELEASE_KIND),
                (
                    "calendar_state_eligibility_receipt",
                    CALENDAR_ELIGIBILITY_RELEASE_KIND,
                ),
            )
            if calendar_bound
            else ()
        ),
    )
    dependencies: list[VerifiedReleaseReceipt] = []
    for name, expected_kind in receipt_fields:
        dependency = _receipt(payload.get(name), name=name)
        dependency.verify(boundary)
        if dependency.release_kind != expected_kind:
            raise IntegrityError("feature-source dependency kind is invalid")
        dependencies.append(dependency)
    causal = dependencies[0]
    if calendar_bound:
        coverage = dependencies[-2]
        eligibility = dependencies[-1]
        load_foundation_calendar_coverage(coverage, boundary=boundary)
        load_calendar_state_eligibility(
            eligibility,
            boundary=boundary,
            expected_causal_receipt=causal,
            expected_coverage_receipt=coverage,
        )
    if (
        manifest.metadata["causal_release_id"] != causal.release_id
        or manifest.source_release_ids
        != tuple(sorted(dependency.release_id for dependency in dependencies))
    ):
        raise IntegrityError("feature-source exact dependency closure is invalid")
    payload["feature_source_input_id"] = feature_source_input_id
    return payload


def load_outcome_source_input(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    calendar_bound = (
        manifest.schema_version == OUTCOME_SOURCE_CALENDAR_SCHEMA_VERSION
    )
    if (
        receipt.phase != "outcome_sources"
        or manifest.release_kind != OUTCOME_SOURCE_RELEASE_KIND
        or manifest.schema_version
        not in {
            OUTCOME_SOURCE_SCHEMA_VERSION,
            OUTCOME_SOURCE_CALENDAR_SCHEMA_VERSION,
        }
        or len(manifest.files) != 1
        or Path(manifest.files[0].logical_path).name
        != "outcome_source_input.json"
        or set(manifest.metadata)
        != {"causal_release_id", "outcome_source_input_id", "role"}
        or manifest.metadata.get("role") != OUTCOME_SOURCE_ROLE
    ):
        raise IntegrityError("outcome-source release contract is invalid")
    path = receipt.resolve_unique_filename("outcome_source_input.json", boundary)
    payload = _read_canonical_object(path, description="outcome-source input")
    expected = {
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
    }
    if calendar_bound:
        expected.update(
            {
                "calendar_coverage_receipt",
                "calendar_state_eligibility_receipt",
            }
        )
    if set(payload) != expected:
        raise IntegrityError("outcome-source payload schema is invalid")
    outcome_source_id = payload.pop("outcome_source_input_id", None)
    if (
        outcome_source_id != sha256_json(payload)
        or outcome_source_id != manifest.metadata["outcome_source_input_id"]
        or payload.get("schema_version") != manifest.schema_version
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
        *(
            (
                ("calendar_coverage_receipt", CALENDAR_COVERAGE_RELEASE_KIND),
                (
                    "calendar_state_eligibility_receipt",
                    CALENDAR_ELIGIBILITY_RELEASE_KIND,
                ),
            )
            if calendar_bound
            else ()
        ),
    )
    dependencies: list[VerifiedReleaseReceipt] = []
    for name, expected_kind in receipt_fields:
        dependency = _receipt(payload.get(name), name=name)
        dependency.verify(boundary)
        if dependency.release_kind != expected_kind:
            raise IntegrityError("outcome-source dependency kind is invalid")
        dependencies.append(dependency)
    causal = dependencies[0]
    if calendar_bound:
        coverage = dependencies[-2]
        eligibility = dependencies[-1]
        load_foundation_calendar_coverage(coverage, boundary=boundary)
        load_calendar_state_eligibility(
            eligibility,
            boundary=boundary,
            expected_causal_receipt=causal,
            expected_coverage_receipt=coverage,
        )
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
    if manifest.schema_version == FOUNDATION_OBSERVABILITY_SCHEMA_VERSION:
        return load_foundation_observability_successor(
            receipt,
            boundary=boundary,
        )
    supported_schema_versions = {
        FOUNDATION_SET_SCHEMA_VERSION,
        FOUNDATION_SUCCESSOR_SCHEMA_VERSION,
        FOUNDATION_CALENDAR_SCHEMA_VERSION,
    }
    calendar_bound = manifest.schema_version == FOUNDATION_CALENDAR_SCHEMA_VERSION
    observability_bound = False
    successor_like = manifest.schema_version == FOUNDATION_SUCCESSOR_SCHEMA_VERSION
    expected_metadata = {
        "feature_spec_hash",
        "coverage_matrix_id",
        "foundation_set_id",
        "interval_count",
        "query_manifest_id",
        "run_id",
        "source_dbn_release_id",
    }
    if successor_like:
        expected_metadata.add("successor_provenance_id")
    if observability_bound:
        expected_metadata.update(
            {
                "historical_observability_coverage_id",
                "historical_observability_policy_sha256",
                "predecessor_foundation_release_id",
            }
        )
    if (
        manifest.release_kind != FOUNDATION_SET_RELEASE_KIND
        or manifest.schema_version not in supported_schema_versions
        or manifest.files
        or set(manifest.embedded_documents) != {"foundation_set.json"}
        or set(manifest.metadata) != expected_metadata
    ):
        raise IntegrityError("foundation-set release contract is invalid")
    raw_payload = receipt.embedded_document("foundation_set.json", boundary)
    if not isinstance(raw_payload, dict):
        raise IntegrityError("foundation-set embedded document is invalid")
    payload = dict(raw_payload)
    expected_payload_keys = {
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
        "source_dbn_release_id",
        "wfa_execution_count",
    }
    if successor_like:
        expected_payload_keys.add("successor_provenance")
    if calendar_bound:
        expected_payload_keys.add("calendar_coverage_receipt")
    if observability_bound:
        expected_payload_keys.update(
            {
                "historical_observability_coverage",
                "historical_observability_policy_sha256",
                "predecessor_foundation_release_id",
            }
        )
    if set(payload) != expected_payload_keys:
        raise IntegrityError("foundation-set payload schema is invalid")
    foundation_set_id = payload.pop("foundation_set_id", None)
    if (
        foundation_set_id != sha256_json(payload)
        or foundation_set_id != manifest.metadata["foundation_set_id"]
        or payload.get("schema_version") != manifest.schema_version
        or payload.get("run_id") != manifest.metadata["run_id"]
        or payload.get("feature_spec_hash") != manifest.metadata["feature_spec_hash"]
        or payload.get("coverage_matrix_id") != manifest.metadata["coverage_matrix_id"]
        or payload.get("query_manifest_id") != manifest.metadata["query_manifest_id"]
        or not isinstance(payload.get("query_manifest"), list)
        or payload.get("query_manifest_id")
        != sha256_json(payload.get("query_manifest"))
        or not isinstance(payload.get("query_mode_census"), list)
        or payload.get("source_dbn_release_id")
        != manifest.metadata["source_dbn_release_id"]
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
            r"[0-9a-f]{64}", str(payload.get("source_dbn_release_id"))
        )
        is None
        or (
            successor_like
            and (
                not isinstance(payload.get("successor_provenance"), dict)
                or payload["successor_provenance"].get("successor_provenance_id")
                != manifest.metadata.get("successor_provenance_id")
            )
        )
    ):
        raise IntegrityError("foundation-set content address or safety posture is invalid")
    feature_spec = CausalFeatureSpec.from_dict(payload.get("feature_spec"))
    if feature_spec.spec_hash != payload.get("feature_spec_hash"):
        raise IntegrityError("foundation-set feature specification hash is invalid")
    try:
        coverage_policy = FoundationCoveragePolicy.from_dict(
            payload["coverage_gate"].get("coverage_policy")
        )
        scope_policy = StatusResearchScopePolicy.from_dict(
            payload["coverage_gate"].get("research_scope_policy")
        )
    except ContractError as exc:
        raise IntegrityError("foundation-set coverage policy is invalid") from exc
    if (
        payload["coverage_gate"].get("coverage_policy_hash")
        != coverage_policy.policy_hash
        or payload["coverage_gate"].get("research_scope_policy_hash")
        != scope_policy.policy_hash
    ):
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
    calendar_coverage_receipt: VerifiedReleaseReceipt | None = None
    if calendar_bound:
        calendar_coverage_receipt = _receipt(
            payload.get("calendar_coverage_receipt"),
            name="calendar_coverage",
        )
        calendar_coverage_receipt.verify(boundary)
        if calendar_coverage_receipt.release_kind != CALENDAR_COVERAGE_RELEASE_KIND:
            raise IntegrityError(
                "foundation-set calendar coverage dependency kind is invalid"
            )
        dependency_ids.add(calendar_coverage_receipt.release_id)
    selection_manifest = top_receipts[0].verify(boundary)
    market_state_manifest = top_receipts[3].verify(boundary)
    market_state_contract = top_receipts[3].embedded_document(
        "market_state_contract.json", boundary
    )
    if not isinstance(market_state_contract, dict):
        raise IntegrityError("foundation-set market-state contract is invalid")
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
    interval_markets_by_key = {
        str(item.get("interval_key")): str(item.get("market"))
        for item in raw_intervals
        if isinstance(item, dict)
    }
    if len(interval_markets_by_key) != len(raw_intervals):
        raise IntegrityError("foundation-set interval identity collection is invalid")
    if calendar_coverage_receipt is not None:
        load_foundation_calendar_coverage(
            calendar_coverage_receipt,
            boundary=boundary,
            expected_intervals=raw_intervals,
        )
    interval_policy_pairs: dict[
        str, tuple[VerifiedReleaseReceipt, VerifiedReleaseReceipt]
    ] = {}
    if successor_like:
        from .successor_contract import (
            REBUILT_MARKETS,
            verify_foundation_successor_provenance,
        )

        successor_provenance = payload.get("successor_provenance")
        assert isinstance(successor_provenance, dict)
        interval_policy_pairs = verify_foundation_successor_provenance(
            successor_provenance,
            boundary=boundary,
            selection_receipt=top_receipts[0],
            interval_markets_by_key=interval_markets_by_key,
        )
        if any(
            interval_policy_pairs[market]
            != (top_receipts[1], top_receipts[2])
            for market in REBUILT_MARKETS
        ):
            raise IntegrityError(
                "foundation successor top-level policy is not the rebuilt component"
            )
        unique_component_pairs = {
            (policy.release_id, session.release_id): (policy, session)
            for policy, session in interval_policy_pairs.values()
        }
        for policy_receipt, session_receipt in unique_component_pairs.values():
            dependency_ids.add(policy_receipt.release_id)
            dependency_ids.add(session_receipt.release_id)
    if observability_bound:
        observability_policy_path = (
            boundary.active_root
            / "configs"
            / "historical_observability_policy.json"
        )
        observability_policy = load_historical_observability_policy(
            observability_policy_path
        )
        policy_sha256 = sha256_file(observability_policy_path)
        predecessor_release_id = payload.get("predecessor_foundation_release_id")
        if (
            payload.get("historical_observability_policy_sha256")
            != policy_sha256
            or manifest.metadata.get("historical_observability_policy_sha256")
            != policy_sha256
            or predecessor_release_id
            != observability_policy["predecessor_foundation_release_id"]
            or manifest.metadata.get("predecessor_foundation_release_id")
            != predecessor_release_id
        ):
            raise IntegrityError(
                "foundation historical-observability policy binding is invalid"
            )
        expected_observability_coverage = build_historical_observability_coverage(
            {
                "intervals": raw_intervals,
                "schema_version": FOUNDATION_SUCCESSOR_SCHEMA_VERSION,
                "source_dbn_release_id": payload.get("source_dbn_release_id"),
            },
            predecessor_release_id=str(predecessor_release_id),
            policy=observability_policy,
        )
        if (
            payload.get("historical_observability_coverage")
            != expected_observability_coverage
            or manifest.metadata.get("historical_observability_coverage_id")
            != expected_observability_coverage[
                "historical_observability_coverage_id"
            ]
        ):
            raise IntegrityError(
                "foundation historical-observability coverage is invalid"
            )
        dependency_ids.add(str(predecessor_release_id))
    interval_keys: list[str] = []
    receipt_fields = (
        "raw_release_receipt",
        "definition_release_receipt",
        "causal_release_receipt",
        *(
            ("calendar_eligibility_release_receipt",)
            if calendar_bound
            else ()
        ),
        "status_eligibility_release_receipt",
        "economics_release_receipt",
        "feature_input_release_receipt",
        "outcome_source_input_release_receipt",
    )
    expected_interval_kinds = {
        "raw_release_receipt": RAW_RELEASE_KIND,
        "definition_release_receipt": DEFINITION_RELEASE_KIND,
        "causal_release_receipt": CAUSAL_RELEASE_KIND,
        **(
            {
                "calendar_eligibility_release_receipt": (
                    CALENDAR_ELIGIBILITY_RELEASE_KIND
                )
            }
            if calendar_bound
            else {}
        ),
        "status_eligibility_release_receipt": STATUS_ELIGIBILITY_RELEASE_KIND,
        "economics_release_receipt": ECONOMICS_RELEASE_KIND,
        "feature_input_release_receipt": FEATURE_SOURCE_INPUT_RELEASE_KIND,
        "outcome_source_input_release_receipt": OUTCOME_SOURCE_RELEASE_KIND,
    }
    aggregate_bar_rows = 0
    aggregate_feature_ready_rows = 0
    aggregate_status_eligible_rows = 0
    aggregate_status_gated_feature_ready_rows = 0
    aggregate_status_resolved_rows = 0
    aggregate_status_unresolved_rows = 0
    research_bar_rows = 0
    research_feature_ready_rows = 0
    research_status_eligible_rows = 0
    research_status_gated_feature_ready_rows = 0
    research_status_resolved_rows = 0
    research_status_unresolved_rows = 0
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
        expected_interval_keys = {
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
        }
        if calendar_bound:
            expected_interval_keys.add("calendar_eligibility_release_receipt")
        if (
            not isinstance(raw_interval, dict)
            or set(raw_interval) != expected_interval_keys
        ):
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
            "in_research_scope",
            "interval_key",
            "research_disposition",
            "research_scope_policy_hash",
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
            not in {
                "ELIGIBLE",
                "ABSTAIN_PRE_STATUS_CAPABILITY_EPOCH",
                "FAIL_STATUS_COVERAGE",
            }
            or type(status_epoch_gate["in_research_scope"]) is not bool
            or status_epoch_gate["research_scope_policy_hash"]
            != scope_policy.policy_hash
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
        interval_policy_receipt, interval_session_receipt = (
            interval_policy_pairs.get(
                str(market),
                (top_receipts[1], top_receipts[2]),
            )
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
            "foundation_policy_receipt": interval_policy_receipt.as_dict(),
            "session_policy_receipt": interval_session_receipt.as_dict(),
        }
        if calendar_bound:
            assert calendar_coverage_receipt is not None
            expected_outcome_dependencies.update(
                {
                    "calendar_coverage_receipt": (
                        calendar_coverage_receipt.as_dict()
                    ),
                    "calendar_state_eligibility_receipt": parsed_receipts[
                        "calendar_eligibility_release_receipt"
                    ].as_dict(),
                }
            )
        if any(
            outcome_payload[name] != expected
            for name, expected in expected_outcome_dependencies.items()
        ):
            raise IntegrityError("foundation-set outcome-source binding is invalid")
        feature_manifest = parsed_receipts["feature_input_release_receipt"].verify(
            boundary
        )
        feature_payload = load_feature_source_input(
            parsed_receipts["feature_input_release_receipt"], boundary=boundary
        )
        expected_feature_dependencies = {
            "causal_release_receipt": parsed_receipts[
                "causal_release_receipt"
            ].as_dict(),
            "definition_release_receipt": parsed_receipts[
                "definition_release_receipt"
            ].as_dict(),
            "economics_release_receipt": parsed_receipts[
                "economics_release_receipt"
            ].as_dict(),
            "foundation_policy_receipt": interval_policy_receipt.as_dict(),
            "session_policy_receipt": interval_session_receipt.as_dict(),
        }
        if calendar_bound:
            assert calendar_coverage_receipt is not None
            expected_feature_dependencies.update(
                {
                    "calendar_coverage_receipt": (
                        calendar_coverage_receipt.as_dict()
                    ),
                    "calendar_state_eligibility_receipt": parsed_receipts[
                        "calendar_eligibility_release_receipt"
                    ].as_dict(),
                }
            )
        if (
            any(
                feature_payload[name] != expected
                for name, expected in expected_feature_dependencies.items()
            )
            or feature_payload["feature_spec_hash"]
            != payload["feature_spec_hash"]
        ):
            raise IntegrityError("foundation-set feature-source binding is invalid")
        if calendar_bound:
            assert calendar_coverage_receipt is not None
            eligibility_payload = load_calendar_state_eligibility(
                parsed_receipts["calendar_eligibility_release_receipt"],
                boundary=boundary,
                expected_causal_receipt=parsed_receipts[
                    "causal_release_receipt"
                ],
                expected_coverage_receipt=calendar_coverage_receipt,
            )
            if eligibility_payload["disposition"] != "ELIGIBLE":
                raise IntegrityError(
                    "foundation-set includes schedule-ineligible bars"
                )
        status_contract = load_status_eligibility(
            parsed_receipts["status_eligibility_release_receipt"],
            causal_receipt=parsed_receipts["causal_release_receipt"],
            market_state_receipt=top_receipts[3],
            boundary=boundary,
        )
        observed_status_gated_features = int(
            status_contract["eligible_rows"]
        )
        if (
            raw_interval["status_eligible_rows"] != status_contract["eligible_rows"]
            or raw_interval["status_resolved_rows"]
            != status_contract["resolved_status_rows"]
            or raw_interval["status_unresolved_rows"]
            != status_contract["unresolved_status_rows"]
            or raw_interval["status_gated_feature_ready_rows"]
            != observed_status_gated_features
            or observed_status_gated_features
            > int(feature_payload["feature_ready_rows"])
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
        interval_coverage_passed = (
            interval_resolved_fraction
            >= coverage_policy.minimum_status_resolved_decision_fraction
            and interval_gated_fraction
            >= coverage_policy.minimum_status_gated_feature_ready_fraction
        )
        expected_in_scope = scope_policy.includes_interval(start=start, end=end)
        expected_disposition = scope_policy.disposition(
            start=start,
            end=end,
            coverage_passed=interval_coverage_passed,
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
            or status_epoch_gate["in_research_scope"] is not expected_in_scope
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
        if expected_in_scope:
            research_bar_rows += interval_bar_rows
            research_feature_ready_rows += interval_feature_rows
            research_status_eligible_rows += int(
                raw_interval["status_eligible_rows"]
            )
            research_status_gated_feature_ready_rows += int(
                raw_interval["status_gated_feature_ready_rows"]
            )
            research_status_resolved_rows += int(
                raw_interval["status_resolved_rows"]
            )
            research_status_unresolved_rows += int(
                raw_interval["status_unresolved_rows"]
            )
    if interval_keys != sorted(set(interval_keys)):
        raise IntegrityError("foundation-set intervals are not unique and sorted")
    coverage_gate = payload["coverage_gate"]
    expected_status_resolved_fraction = (
        Decimal(research_status_resolved_rows) / Decimal(research_bar_rows)
        if research_bar_rows
        else Decimal(0)
    )
    expected_status_gated_feature_ready_fraction = (
        Decimal(research_status_gated_feature_ready_rows)
        / Decimal(research_feature_ready_rows)
        if research_feature_ready_rows
        else Decimal(0)
    )
    archive_status_resolved_fraction = (
        Decimal(aggregate_status_resolved_rows) / Decimal(aggregate_bar_rows)
        if aggregate_bar_rows
        else Decimal(0)
    )
    archive_status_gated_feature_ready_fraction = (
        Decimal(aggregate_status_gated_feature_ready_rows)
        / Decimal(aggregate_feature_ready_rows)
        if aggregate_feature_ready_rows
        else Decimal(0)
    )
    archive_census_core = {
        "bar_rows": aggregate_bar_rows,
        "feature_ready_rows": aggregate_feature_ready_rows,
        "missing_status_rows_remain_in_denominator": True,
        "status_eligible_rows": aggregate_status_eligible_rows,
        "status_gated_feature_ready_fraction": str(
            archive_status_gated_feature_ready_fraction
        ),
        "status_gated_feature_ready_rows": (
            aggregate_status_gated_feature_ready_rows
        ),
        "status_resolved_decision_fraction": str(
            archive_status_resolved_fraction
        ),
        "status_resolved_rows": aggregate_status_resolved_rows,
        "status_unresolved_rows": aggregate_status_unresolved_rows,
    }
    expected_archive_census = {
        **archive_census_core,
        "archive_census_id": sha256_json(archive_census_core),
    }
    in_scope_gates = [
        gate
        for gate in observed_status_epoch_gates
        if gate["in_research_scope"] is True
    ]
    coverage_gate_keys = {
        "archive_census",
        "bar_rows",
        "coverage_matrix_id",
        "coverage_policy",
        "coverage_policy_hash",
        "feature_ready_rows",
        "missing_status_rows_remain_in_denominator",
        "research_abstained_interval_count",
        "research_eligible_interval_count",
        "research_failed_interval_count",
        "research_scope_interval_count",
        "research_scope_policy",
        "research_scope_policy_hash",
        "research_scope_status_source_market_year_fraction",
        "statistics_feature_use",
        "statistics_source_market_year_fraction",
        "status_eligible_rows",
        "status_epoch_gates",
        "status_epoch_gates_id",
        "status_gated_feature_ready_fraction",
        "status_gated_feature_ready_rows",
        "status_resolved_decision_fraction",
        "status_resolved_rows",
        "status_source_market_year_fraction",
        "status_unresolved_rows",
    }
    if calendar_bound:
        coverage_gate_keys.update(
            {
                "calendar_contract_status",
                "calendar_coverage_receipt_id",
            }
        )
    if (
        set(coverage_gate) != coverage_gate_keys
        or coverage_gate.get("archive_census") != expected_archive_census
        or coverage_gate.get("coverage_matrix_id")
        != payload.get("coverage_matrix_id")
        or coverage_gate.get("missing_status_rows_remain_in_denominator")
        is not True
        or coverage_gate.get("statistics_feature_use") is not False
        or coverage_gate.get("status_source_market_year_fraction")
        != market_state_contract.get("status_source_market_year_fraction")
        or coverage_gate.get("statistics_source_market_year_fraction")
        != market_state_contract.get("statistics_source_market_year_fraction")
        or coverage_gate.get("research_scope_status_source_market_year_fraction")
        != market_state_contract.get(
            "research_scope_status_source_market_year_fraction"
        )
        or coverage_gate.get("bar_rows") != research_bar_rows
        or coverage_gate.get("feature_ready_rows") != research_feature_ready_rows
        or coverage_gate.get("status_eligible_rows")
        != research_status_eligible_rows
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
            gate["in_research_scope"] is False
            for gate in observed_status_epoch_gates
        )
        or coverage_gate.get("research_failed_interval_count")
        != sum(
            gate["research_disposition"] == "FAIL_STATUS_COVERAGE"
            for gate in observed_status_epoch_gates
        )
        or coverage_gate.get("research_scope_interval_count")
        != len(in_scope_gates)
        or len(observed_status_epoch_gates) != len(raw_intervals)
        or not in_scope_gates
        or any(gate["research_disposition"] != "ELIGIBLE" for gate in in_scope_gates)
        or coverage_gate.get("status_gated_feature_ready_rows")
        != research_status_gated_feature_ready_rows
        or coverage_gate.get("status_resolved_rows")
        != research_status_resolved_rows
        or coverage_gate.get("status_unresolved_rows")
        != research_status_unresolved_rows
        or aggregate_bar_rows
        != aggregate_status_resolved_rows + aggregate_status_unresolved_rows
        or research_bar_rows
        != research_status_resolved_rows + research_status_unresolved_rows
        or research_bar_rows < coverage_policy.minimum_bar_rows
        or research_status_eligible_rows
        < coverage_policy.minimum_status_eligible_rows
        or research_status_gated_feature_ready_rows
        < coverage_policy.minimum_status_gated_feature_ready_rows
        or coverage_gate.get("status_resolved_decision_fraction")
        != str(expected_status_resolved_fraction)
        or expected_status_resolved_fraction
        < coverage_policy.minimum_status_resolved_decision_fraction
        or coverage_gate.get("status_gated_feature_ready_fraction")
        != str(expected_status_gated_feature_ready_fraction)
        or expected_status_gated_feature_ready_fraction
        < coverage_policy.minimum_status_gated_feature_ready_fraction
        or not _source_family_coverage_passes(
            coverage_gate,
            coverage_policy=coverage_policy,
        )
        or (
            calendar_bound
            and (
                calendar_coverage_receipt is None
                or coverage_gate.get("calendar_contract_status")
                != "BOUND_AND_ALL_ROWS_OPEN"
                or coverage_gate.get("calendar_coverage_receipt_id")
                != calendar_coverage_receipt.receipt_id
            )
        )
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
        legacy_roots=legacy_roots_from_contract(payload),
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
    parser.add_argument("--source-dbn-manifest", type=Path, required=True)
    parser.add_argument("--source-selection-manifest", type=Path, required=True)
    parser.add_argument("--calendar-index-manifest", type=Path, required=True)
    parser.add_argument("--feature-spec", type=Path, required=True)
    parser.add_argument("--batch-rows", type=int, default=100_000)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error("foundation publication requires explicit --execute")
    from ..current_research_surface import reject_retired_project_execution

    reject_retired_project_execution(
        root=args.repository_root,
        surface="legacy direct foundation publication CLI",
    )
    boundary = _boundary_from_contract(args.repository_root, args.source_contract)
    feature_spec = _load_feature_spec(args.feature_spec, boundary=boundary)
    dbn_receipt = VerifiedReleaseReceipt.from_manifest(
        args.source_dbn_manifest,
        boundary,
        verify_files=False,
    )
    if (
        dbn_receipt.phase != "dbn"
        or dbn_receipt.release_kind != "futures_phase1a_verified_dbn"
        or dbn_receipt.schema_version != "1.0.0"
    ):
        raise IntegrityError("requested source is not a Phase 1A DBN release")
    source_dbn_release_id = dbn_receipt.release_id
    selection_receipt = VerifiedReleaseReceipt.from_manifest(
        args.source_selection_manifest, boundary
    )
    selection_manifest = selection_receipt.verify(boundary)
    selection_manifest_id = selection_manifest.metadata.get(
        "selection_manifest_id"
    )
    acceptance_release_ids = selection_manifest.metadata.get(
        "anomaly_acceptance_release_ids"
    )
    if (
        selection_manifest.release_kind != SELECTION_RELEASE_KIND
        or selection_manifest.metadata.get("source_dbn_release_id")
        != source_dbn_release_id
        or not isinstance(acceptance_release_ids, list)
        or any(type(item) is not str for item in acceptance_release_ids)
        or acceptance_release_ids != sorted(set(acceptance_release_ids))
        or selection_manifest.source_release_ids
        != tuple(sorted((source_dbn_release_id, *acceptance_release_ids)))
        or type(selection_manifest_id) is not str
        or re.fullmatch(r"[0-9a-f]{64}", selection_manifest_id) is None
    ):
        raise IntegrityError(
            "selection release is not bound to the requested DBN release"
        )
    calendar_index_receipt = VerifiedReleaseReceipt.from_manifest(
        args.calendar_index_manifest, boundary
    )
    calendar_index_manifest = calendar_index_receipt.verify(boundary)
    if calendar_index_manifest.release_kind != CALENDAR_INDEX_RELEASE_KIND:
        raise IntegrityError(
            "requested calendar index is not a verified exchange-calendar index"
        )
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={
            "feature_spec_hash": feature_spec.spec_hash,
            "calendar_index_release_id": calendar_index_receipt.release_id,
            "selection_manifest_id": selection_manifest_id,
            "source_dbn_release_id": source_dbn_release_id,
        },
    )
    result = FoundationOrchestrator(
        boundary=boundary,
        operation_receipt=operation,
        batch_rows=args.batch_rows,
    ).run(
        source_dbn_manifest=args.source_dbn_manifest,
        source_selection_receipt=selection_receipt,
        feature_spec=feature_spec,
        calendar_index_receipt=calendar_index_receipt,
    )
    print(canonical_bytes(result.as_dict()).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
