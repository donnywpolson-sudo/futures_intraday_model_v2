"""Plan and run a bounded, restart-safe calendar-bound foundation successor."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Mapping

from ..boundary import OperationClassification, OperationReceipt, RepoBoundary
from ..canonical import canonical_bytes, sha256_file, sha256_json
from ..data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    verify_data_release_manifest,
)
from ..errors import ContractError, IntegrityError
from ..exchange_calendar import (
    INDEX_RELEASE_KIND as CALENDAR_INDEX_RELEASE_KIND,
    approved_research_markets,
    load_calendar_state_eligibility,
    load_active_calendar_index,
    load_exchange_calendar_policy,
    load_foundation_calendar_coverage,
    publish_calendar_state_eligibility,
    publish_foundation_calendar_coverage,
    verify_calendar_freshness,
)
from ..locking import FileLease
from ..producer_bridge import (
    load_actual_contract_definitions,
    load_versioned_session_policy,
    verify_actual_contract_economics_context,
)
from .coverage import StatusResearchScopePolicy
from .market_state import (
    FoundationCoveragePolicy,
    StatisticsRolePolicy,
    load_market_state_foundation,
    load_status_eligibility,
)
from .materialize import load_causal_interval, load_raw_interval
from .orchestrator import (
    FOUNDATION_SET_RELEASE_KIND,
    FOUNDATION_CALENDAR_SCHEMA_VERSION,
    FOUNDATION_SUCCESSOR_SCHEMA_VERSION,
    FoundationOrchestrator,
    _atomic_checkpoint,
    _boundary_from_contract,
    _checkpoint_core,
    _checkpoint_payload as _orchestrator_checkpoint_payload,
    _config_closure,
    _implementation_closure,
    _interval_key,
    _load_feature_spec,
    _phase_count,
    _receipt,
    load_feature_source_input,
    load_outcome_source_input,
)
from .selection import load_source_selection_with_resolution
from .snapshot import PublishedDbnRelease as PublishedSourceSnapshot
from .successor import FoundationSuccessorFinalizer
from .support import VerifiedFoundationPolicies


PLAN_SCHEMA = "foundation_calendar_successor_plan/1.0.0"
APPROVAL_SCHEMA = "foundation_calendar_successor_approval/1.0.0"
OPERATION = "PUBLISH_BOUNDED_CALENDAR_BOUND_FOUNDATION_SUCCESSOR"
MAXIMUM_INTERVALS = 683
MAXIMUM_CHECKPOINTS_PER_INVOCATION = 32
MAXIMUM_COMPLETED_INTERVALS_PER_INVOCATION = 8
MAXIMUM_INVOCATION_SECONDS = 1_800
BATCH_ROWS = 100_000
MAXIMUM_TOTAL_INVOCATIONS = 86
MAXIMUM_REFERENCED_RELEASE_BYTES = 107_374_182_400
CHECKPOINT_SCHEMA = "foundation_calendar_successor_checkpoint/1.0.0"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_UTC_SECOND = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
_REUSABLE_PHASES = {
    "raw",
    "definitions",
    "causal",
    "status_eligibility",
    "economics",
    "feature_input",
    "outcome_source_input",
}
_SUCCESSOR_INTERVAL_PHASES = {
    "raw",
    "definitions",
    "causal",
    "calendar_eligibility",
    "status_eligibility",
    "economics",
    "feature_input",
    "outcome_source_input",
}


def _require_official_historical_calendar_route(boundary: RepoBoundary) -> None:
    policy = load_exchange_calendar_policy(
        boundary.active_root / "configs" / "exchange_calendar_policy.json"
    )
    if (
        policy.get("historical_backfill_policy")
        != "AUTHORITATIVE_CME_BYTES_REQUIRED_NO_TEMPLATE_OR_DATABENTO_STATUS_RECONSTRUCTION"
    ):
        raise IntegrityError(
            "official-calendar historical foundation successor is disabled; "
            "the selected route is DBN empirical observability"
        )


class InvocationBudgetReached(RuntimeError):
    """A normal restart-safe yield after a persisted checkpoint."""


class InvocationLimiter:
    """Yield after exact persisted checkpoint, interval, or elapsed bounds."""

    def __init__(
        self,
        *,
        maximum_checkpoints: int = MAXIMUM_CHECKPOINTS_PER_INVOCATION,
        maximum_completed_intervals: int = (
            MAXIMUM_COMPLETED_INTERVALS_PER_INVOCATION
        ),
        maximum_seconds: int = MAXIMUM_INVOCATION_SECONDS,
    ) -> None:
        if (
            type(maximum_checkpoints) is not int
            or maximum_checkpoints <= 0
            or type(maximum_completed_intervals) is not int
            or maximum_completed_intervals <= 0
            or type(maximum_seconds) is not int
            or maximum_seconds <= 0
        ):
            raise ContractError("foundation invocation bounds must be positive")
        self.maximum_checkpoints = maximum_checkpoints
        self.maximum_completed_intervals = maximum_completed_intervals
        self.maximum_seconds = maximum_seconds
        self.started = time.monotonic()
        self.checkpoints = 0
        self.completed_intervals = 0

    def __call__(self, phase: str) -> None:
        self.checkpoints += 1
        if phase.endswith(":outcome_source_input"):
            self.completed_intervals += 1
        if phase == "foundation_set":
            return
        if (
            self.checkpoints >= self.maximum_checkpoints
            or self.completed_intervals
            >= self.maximum_completed_intervals
            or time.monotonic() - self.started >= self.maximum_seconds
        ):
            raise InvocationBudgetReached(
                "foundation invocation reached its persisted restart boundary"
            )


def _read_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"{description} is unreadable") from exc
    if not isinstance(payload, dict):
        raise IntegrityError(f"{description} must be an object")
    return payload


def _write_create_only(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_bytes(dict(payload)) + b"\n"
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        remaining = memoryview(encoded)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("canonical plan write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _relative(path: Path, boundary: RepoBoundary) -> str:
    resolved = path.resolve(strict=True)
    boundary.assert_active_path(resolved, purpose="foundation successor input")
    return resolved.relative_to(boundary.active_root).as_posix()


def _checkpoint_payload(path: Path) -> dict[str, object]:
    return _read_object(path, description="foundation checkpoint")


def _checkpoint_summary(path: Path) -> dict[str, object]:
    payload = _checkpoint_payload(path)
    completed = payload.get("completed")
    status = payload.get("status")
    if (
        not isinstance(completed, dict)
        or status not in {"RUNNING", "COMPLETE"}
        or type(payload.get("checkpoint_id")) is not str
    ):
        raise IntegrityError("foundation checkpoint summary is invalid")
    return {
        "checkpoint_id": payload["checkpoint_id"],
        "checkpoint_sha256": sha256_file(path),
        "completed_phase_count": _phase_count(completed),
        "status": status,
    }


def _load_predecessor_foundation_shallow(
    receipt: VerifiedReleaseReceipt,
    *,
    boundary: RepoBoundary,
) -> tuple[object, dict[str, object]]:
    manifest = verify_data_release_manifest(
        boundary.active_root / receipt.manifest_path,
        boundary,
        verify_files=False,
    )
    payload = manifest.embedded_documents.get("foundation_set.json")
    if (
        manifest.release_kind != FOUNDATION_SET_RELEASE_KIND
        or manifest.schema_version != FOUNDATION_SUCCESSOR_SCHEMA_VERSION
        or manifest.files
        or not isinstance(payload, dict)
        or payload.get("schema_version") != FOUNDATION_SUCCESSOR_SCHEMA_VERSION
        or payload.get("foundation_set_id")
        != sha256_json(
            {
                key: value
                for key, value in payload.items()
                if key != "foundation_set_id"
            }
        )
        or payload.get("foundation_set_id")
        != manifest.metadata.get("foundation_set_id")
        or payload.get("run_id") != manifest.metadata.get("run_id")
        or payload.get("dependency_closure_complete") is not True
        or payload.get("provider_call_count") != 0
        or payload.get("model_fit_count") != 0
        or payload.get("wfa_execution_count") != 0
    ):
        raise IntegrityError(
            "calendar successor predecessor foundation is invalid"
        )
    return manifest, dict(payload)


def _assert_calendar_interval_coverage(
    index: object,
    intervals: object,
) -> None:
    for interval in intervals:
        start = date.fromisoformat(str(interval.start))
        end = date.fromisoformat(str(interval.end)) - timedelta(days=1)
        for trade_date in (start, end):
            try:
                index.calendar_for(interval.market, trade_date)
            except ContractError as exc:
                raise IntegrityError(
                    "active exchange calendar cannot cover the exact "
                    "historical foundation interval scope"
                ) from exc


def _reusable_checkpoint_authority(
    path: Path,
    *,
    boundary: RepoBoundary,
    expected_interval_keys: list[str],
    source_dbn_release_id: str,
    source_selection_receipt: VerifiedReleaseReceipt,
) -> dict[str, object]:
    resolved_path = boundary.assert_active_path(
        path,
        purpose="reusable pre-calendar foundation checkpoint",
        subtree="state/foundation_runs_v2",
    )
    payload = _checkpoint_payload(resolved_path)
    core = _checkpoint_core(payload)
    completed = core.get("completed")
    intervals = completed.get("intervals") if isinstance(completed, dict) else None
    source_bound = completed.get("source_bound") if isinstance(completed, dict) else None
    if (
        core.get("status") != "RUNNING"
        or type(core.get("run_id")) is not str
        or not isinstance(intervals, dict)
        or sorted(intervals) != expected_interval_keys
        or any(
            not isinstance(state, dict) or set(state) != _REUSABLE_PHASES
            for state in intervals.values()
        )
        or not isinstance(source_bound, dict)
        or source_bound.get("source_dbn_release_id") != source_dbn_release_id
        or source_bound.get("source_selection_receipt_id")
        != source_selection_receipt.receipt_id
        or any(
            name not in completed
            for name in ("foundation_policy", "session_policy", "market_state")
        )
    ):
        raise IntegrityError(
            "reusable pre-calendar checkpoint is not an exact 683-interval closure"
        )
    return {
        "checkpoint_id": payload["checkpoint_id"],
        "checkpoint_path": _relative(resolved_path, boundary),
        "checkpoint_sha256": sha256_file(resolved_path),
        "completed_interval_count": len(intervals),
        "completed_interval_phase_count": sum(
            len(state) for state in intervals.values()
        ),
        "foundation_policy_receipt": completed["foundation_policy"],
        "market_state_receipt": completed["market_state"],
        "run_id": core["run_id"],
        "session_policy_receipt": completed["session_policy"],
        "status": core["status"],
    }


def _build_authority(
    *,
    boundary: RepoBoundary,
    source_dbn_manifest_path: Path,
    source_selection_manifest_path: Path,
    predecessor_foundation_manifest_path: Path,
    reusable_checkpoint_path: Path,
    calendar_index_manifest_path: Path,
    feature_spec_path: Path,
    freshness_at: datetime | None = None,
) -> dict[str, object]:
    snapshot = PublishedSourceSnapshot.open(
        source_dbn_manifest_path,
        boundary=boundary,
        verify_files=False,
    )
    selection_receipt = VerifiedReleaseReceipt.from_manifest(
        source_selection_manifest_path, boundary
    )
    selection, resolved = load_source_selection_with_resolution(
        selection_receipt,
        snapshot=snapshot,
        boundary=boundary,
        verify_source_files=False,
    )
    feature_spec = _load_feature_spec(feature_spec_path, boundary=boundary)
    calendar_receipt = VerifiedReleaseReceipt.from_manifest(
        calendar_index_manifest_path, boundary
    )
    calendar_manifest = calendar_receipt.verify(boundary)
    if calendar_manifest.release_kind != CALENDAR_INDEX_RELEASE_KIND:
        raise IntegrityError("foundation plan calendar index kind is invalid")
    markets = approved_research_markets(
        boundary.active_root / "configs" / "research_universe_contract.json"
    )
    active_index = load_active_calendar_index(
        boundary=boundary,
        expected_markets=markets,
    )
    if active_index.receipt != calendar_receipt:
        raise IntegrityError("foundation plan calendar index is not active")
    freshness = verify_calendar_freshness(
        active_index,
        expected_markets=markets,
        now=freshness_at or datetime.now(timezone.utc),
    )
    predecessor_receipt = VerifiedReleaseReceipt.from_manifest(
        predecessor_foundation_manifest_path, boundary
    )
    predecessor_manifest, predecessor = _load_predecessor_foundation_shallow(
        predecessor_receipt,
        boundary=boundary,
    )
    if predecessor_manifest.schema_version != FOUNDATION_SUCCESSOR_SCHEMA_VERSION:
        raise IntegrityError(
            "calendar successor predecessor must be the accepted schema-v5 foundation"
        )
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={"purpose": "PLAN_CALENDAR_BOUND_FOUNDATION_SUCCESSOR"},
    )
    orchestrator = FoundationOrchestrator(
        boundary=boundary,
        operation_receipt=operation,
        batch_rows=BATCH_ROWS,
    )
    intervals = resolved.intervals
    run_contract = orchestrator._run_contract(
        snapshot=snapshot,
        selection_receipt=selection_receipt,
        selection=selection,
        intervals=intervals,
        resolved_selection=resolved,
        feature_spec=feature_spec,
        calendar_index_receipt=calendar_receipt,
    )
    run_id = sha256_json(run_contract)
    interval_count = len(intervals)
    predecessor_intervals = predecessor.get("intervals")
    if (
        interval_count != MAXIMUM_INTERVALS
        or predecessor.get("interval_count") != interval_count
        or predecessor.get("source_dbn_release_id")
        != snapshot.source_release_id
        or predecessor.get("source_selection_receipt")
        != selection_receipt.as_dict()
        or predecessor.get("feature_spec_hash") != feature_spec.spec_hash
        or predecessor.get("query_manifest_id") != resolved.query_manifest_id
        or predecessor.get("coverage_matrix_id") != resolved.coverage_matrix_id
        or not isinstance(predecessor_intervals, list)
        or len(predecessor_intervals) != interval_count
        or len(markets) != 41
    ):
        raise IntegrityError(
            "calendar successor predecessor differs from current foundation authority"
        )
    expected_interval_keys = sorted(
        str(item["interval_key"]) for item in run_contract["intervals"]
    )
    _assert_calendar_interval_coverage(active_index, intervals)
    predecessor_interval_keys = sorted(
        str(item.get("interval_key"))
        for item in predecessor_intervals
        if isinstance(item, dict)
    )
    if predecessor_interval_keys != expected_interval_keys:
        raise IntegrityError(
            "calendar successor predecessor interval identities differ"
        )
    reusable_checkpoint = _reusable_checkpoint_authority(
        reusable_checkpoint_path,
        boundary=boundary,
        expected_interval_keys=expected_interval_keys,
        source_dbn_release_id=snapshot.source_release_id,
        source_selection_receipt=selection_receipt,
    )
    active_pointer = boundary.active_root / "configs" / "active_exchange_calendar.json"
    return {
        "active_calendar_pointer_path": _relative(active_pointer, boundary),
        "active_calendar_pointer_sha256": sha256_file(active_pointer),
        "calendar_freshness_check": freshness,
        "calendar_index_manifest_path": _relative(
            calendar_index_manifest_path, boundary
        ),
        "calendar_index_manifest_sha256": sha256_file(
            calendar_index_manifest_path
        ),
        "calendar_index_receipt": calendar_receipt.as_dict(),
        "feature_spec_hash": feature_spec.spec_hash,
        "feature_spec_path": _relative(feature_spec_path, boundary),
        "feature_spec_sha256": sha256_file(feature_spec_path),
        "foundation_run_contract": run_contract,
        "foundation_run_id": run_id,
        "interval_count": interval_count,
        "market_count": len(markets),
        "markets": list(markets),
        "predecessor_foundation_manifest_path": _relative(
            predecessor_foundation_manifest_path, boundary
        ),
        "predecessor_foundation_manifest_sha256": sha256_file(
            predecessor_foundation_manifest_path
        ),
        "predecessor_foundation_receipt": predecessor_receipt.as_dict(),
        "reusable_pre_calendar_checkpoint": reusable_checkpoint,
        "source_dbn_manifest_path": _relative(
            source_dbn_manifest_path, boundary
        ),
        "source_dbn_manifest_sha256": sha256_file(source_dbn_manifest_path),
        "source_dbn_receipt": snapshot.receipt.as_dict(),
        "source_selection_manifest_path": _relative(
            source_selection_manifest_path, boundary
        ),
        "source_selection_manifest_sha256": sha256_file(
            source_selection_manifest_path
        ),
        "source_selection_receipt": selection_receipt.as_dict(),
    }


def build_plan(
    *,
    boundary: RepoBoundary,
    source_dbn_manifest_path: Path,
    source_selection_manifest_path: Path,
    predecessor_foundation_manifest_path: Path,
    reusable_checkpoint_path: Path,
    calendar_index_manifest_path: Path,
    feature_spec_path: Path,
    freshness_at: datetime | None = None,
) -> dict[str, object]:
    _require_official_historical_calendar_route(boundary)
    authority = _build_authority(
        boundary=boundary,
        source_dbn_manifest_path=source_dbn_manifest_path,
        source_selection_manifest_path=source_selection_manifest_path,
        predecessor_foundation_manifest_path=predecessor_foundation_manifest_path,
        reusable_checkpoint_path=reusable_checkpoint_path,
        calendar_index_manifest_path=calendar_index_manifest_path,
        feature_spec_path=feature_spec_path,
        freshness_at=freshness_at,
    )
    interval_count = int(authority["interval_count"])
    scope: dict[str, object] = {
        "authority": authority,
        "bounds": {
            "batch_rows": BATCH_ROWS,
            "maximum_checkpoints_per_invocation": (
                MAXIMUM_CHECKPOINTS_PER_INVOCATION
            ),
            "maximum_completed_intervals_per_invocation": (
                MAXIMUM_COMPLETED_INTERVALS_PER_INVOCATION
            ),
            "maximum_intervals": MAXIMUM_INTERVALS,
            "maximum_invocation_seconds": MAXIMUM_INVOCATION_SECONDS,
            "maximum_referenced_release_bytes": (
                MAXIMUM_REFERENCED_RELEASE_BYTES
            ),
            "maximum_total_invocations": MAXIMUM_TOTAL_INVOCATIONS,
            "provider_calls": 0,
        },
        "expected_publication": {
            "calendar_coverage_releases": 1,
            "calendar_eligibility_releases": interval_count,
            "feature_source_input_releases": interval_count,
            "foundation_set_releases": 1,
            "foundation_schema_version": FOUNDATION_CALENDAR_SCHEMA_VERSION,
            "maximum_completed_phase_count": 6 + interval_count * 4 + 1,
            "reused_pre_calendar_interval_count": interval_count,
            "outcome_source_input_releases": interval_count,
        },
        "forbidden_actions": [
            "ACCESS_HOLDOUT_OR_FORWARD",
            "CALL_ANY_PROVIDER",
            "DELETE_OR_OVERWRITE_ACCEPTED_RELEASE",
            "FIT_MODEL_OR_READ_OUTCOMES",
            "MATERIALIZE_LABELS_OR_PREDICTIONS",
            "MUTATE_ACTIVE_DATA_VIEW",
            "PUSH_REMOTE",
            "TRADE_OR_PLACE_ORDER",
        ],
        "implementation_closure": _implementation_closure(),
        "launcher": {
            "command": "futures-foundation-calendar-successor run",
            "pyproject_path": "pyproject.toml",
            "pyproject_sha256": sha256_file(
                boundary.active_root / "pyproject.toml"
            ),
        },
        "config_closure": _config_closure(boundary),
        "output_paths": {
            "checkpoint": (
                "state/foundation_calendar_successors/"
                f"{authority['foundation_run_id']}/checkpoint.json"
            ),
            "foundation_manifest_template": (
                "manifests/data_releases/foundation/{release_id}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
            "run_lock": (
                "state/locks/foundation-calendar-successor-"
                f"{authority['foundation_run_id']}.lock"
            ),
            "staging_root": "state/data_publication_staging",
        },
        "restart_policy": (
            "SAME_PLAN_AND_APPROVAL_MAY_RESUME_ONLY_THE_BOUND_RUN_UNTIL_COMPLETE"
        ),
        "stop_conditions": [
            "ACTIVE_CALENDAR_OR_FRESHNESS_DRIFT",
            "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
            "CHECKPOINT_OR_RELEASE_IDENTITY_DRIFT",
            "CONFIG_OR_IMPLEMENTATION_HASH_DRIFT",
            "INVOCATION_BOUND_REACHED",
            "PUBLICATION_CONFLICT_OR_UNDECLARED_OUTPUT",
            "RESOURCE_OR_STORAGE_BOUND_REACHED",
        ],
    }
    core: dict[str, object] = {
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "execution_authorized": False,
        "operation": OPERATION,
        "schema_version": PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_plan(
    payload: Mapping[str, object], *, boundary: RepoBoundary
) -> dict[str, object]:
    core = {key: payload[key] for key in payload if key != "plan_id"}
    scope = payload.get("scope")
    if (
        set(payload)
        != {
            "classification",
            "execution_authorized",
            "operation",
            "plan_id",
            "schema_version",
            "scope",
        }
        or payload.get("schema_version") != PLAN_SCHEMA
        or payload.get("operation") != OPERATION
        or payload.get("classification")
        != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload.get("execution_authorized") is not False
        or payload.get("plan_id") != sha256_json(core)
        or not isinstance(scope, dict)
        or not isinstance(scope.get("authority"), dict)
        or not isinstance(scope.get("launcher"), dict)
    ):
        raise IntegrityError("foundation calendar-successor plan is invalid")
    authority = scope["authority"]
    freshness = authority.get("calendar_freshness_check")
    if (
        not isinstance(freshness, dict)
        or type(freshness.get("checked_at_utc")) is not str
    ):
        raise IntegrityError("foundation successor freshness evidence is invalid")
    try:
        freshness_at = datetime.fromisoformat(
            str(freshness["checked_at_utc"]).replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise IntegrityError(
            "foundation successor freshness time is invalid"
        ) from exc
    expected = build_plan(
        boundary=boundary,
        source_dbn_manifest_path=(
            boundary.active_root / str(authority["source_dbn_manifest_path"])
        ),
        source_selection_manifest_path=(
            boundary.active_root
            / str(authority["source_selection_manifest_path"])
        ),
        predecessor_foundation_manifest_path=(
            boundary.active_root
            / str(authority["predecessor_foundation_manifest_path"])
        ),
        reusable_checkpoint_path=(
            boundary.active_root
            / str(
                authority["reusable_pre_calendar_checkpoint"][
                    "checkpoint_path"
                ]
            )
        ),
        calendar_index_manifest_path=(
            boundary.active_root
            / str(authority["calendar_index_manifest_path"])
        ),
        feature_spec_path=(
            boundary.active_root / str(authority["feature_spec_path"])
        ),
        freshness_at=freshness_at,
    )
    if dict(payload) != expected:
        raise IntegrityError(
            "foundation calendar-successor plan is not reproducible"
        )
    return dict(payload)


def validate_approval(
    approval: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> str:
    core_keys = {
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    core = {key: approval[key] for key in core_keys if key in approval}
    if (
        set(approval) != {*core_keys, "approval_receipt_id"}
        or approval.get("schema_version") != APPROVAL_SCHEMA
        or approval.get("operation") != OPERATION
        or approval.get("status") != "APPROVED"
        or approval.get("plan_id") != plan["plan_id"]
        or approval.get("plan_sha256") != plan_sha256
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _SHA256.fullmatch(str(approval["user_authorization_id"])) is None
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise IntegrityError(
            "foundation calendar-successor lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])


def _load_successor_checkpoint(
    *,
    path: Path,
    run_id: str,
    run_contract: Mapping[str, object],
) -> dict[str, object]:
    expected = {
        "checkpoint_version": CHECKPOINT_SCHEMA,
        "completed": {},
        "layout_version": "2.0.0",
        "run_contract": dict(run_contract),
        "run_id": run_id,
        "status": "RUNNING",
    }
    if not path.exists():
        _atomic_checkpoint(path, _orchestrator_checkpoint_payload(expected))
        return expected
    payload = _checkpoint_payload(path)
    core = _checkpoint_core(payload)
    if (
        core.get("checkpoint_version") != CHECKPOINT_SCHEMA
        or core.get("layout_version") != "2.0.0"
        or core.get("run_id") != run_id
        or core.get("run_contract") != dict(run_contract)
        or core.get("status") not in {"RUNNING", "COMPLETE"}
        or not isinstance(core.get("completed"), dict)
    ):
        raise IntegrityError(
            "calendar-bound foundation successor checkpoint drifted"
        )
    return core


def _persist_successor_checkpoint(
    path: Path,
    core: dict[str, object],
    *,
    phase: str,
    limiter: InvocationLimiter,
) -> None:
    _atomic_checkpoint(path, _orchestrator_checkpoint_payload(core))
    limiter(phase)


def _successor_context(
    *,
    authority: Mapping[str, object],
    boundary: RepoBoundary,
) -> tuple[
    PublishedSourceSnapshot,
    VerifiedReleaseReceipt,
    dict[str, object],
    object,
    object,
]:
    snapshot = PublishedSourceSnapshot.open(
        boundary.active_root / str(authority["source_dbn_manifest_path"]),
        boundary=boundary,
        verify_files=False,
    )
    selection_receipt = VerifiedReleaseReceipt.from_manifest(
        boundary.active_root
        / str(authority["source_selection_manifest_path"]),
        boundary,
    )
    selection, resolved = load_source_selection_with_resolution(
        selection_receipt,
        snapshot=snapshot,
        boundary=boundary,
        verify_source_files=False,
    )
    feature_spec = _load_feature_spec(
        boundary.active_root / str(authority["feature_spec_path"]),
        boundary=boundary,
    )
    return snapshot, selection_receipt, selection, resolved, feature_spec


def _run_calendar_successor(
    *,
    plan: Mapping[str, object],
    approval_id: str,
    boundary: RepoBoundary,
    limiter: InvocationLimiter,
) -> dict[str, object]:
    scope = plan["scope"]
    assert isinstance(scope, dict)
    authority = scope["authority"]
    assert isinstance(authority, dict)
    snapshot, selection_receipt, selection, resolved, feature_spec = (
        _successor_context(authority=authority, boundary=boundary)
    )
    intervals = resolved.intervals
    if len(intervals) != MAXIMUM_INTERVALS:
        raise IntegrityError("calendar successor interval scope drifted")

    reusable_authority = authority["reusable_pre_calendar_checkpoint"]
    if not isinstance(reusable_authority, dict):
        raise IntegrityError("calendar successor reusable authority is invalid")
    reusable_path = (
        boundary.active_root / str(reusable_authority["checkpoint_path"])
    )
    reusable_payload = _checkpoint_payload(reusable_path)
    if (
        reusable_payload.get("checkpoint_id")
        != reusable_authority["checkpoint_id"]
        or sha256_file(reusable_path)
        != reusable_authority["checkpoint_sha256"]
    ):
        raise IntegrityError("reusable pre-calendar checkpoint changed")
    reusable_core = _checkpoint_core(reusable_payload)
    reusable_completed = reusable_core["completed"]
    assert isinstance(reusable_completed, dict)
    reusable_intervals = reusable_completed["intervals"]
    assert isinstance(reusable_intervals, dict)

    calendar_receipt = VerifiedReleaseReceipt.from_manifest(
        boundary.active_root / str(authority["calendar_index_manifest_path"]),
        boundary,
    )
    markets = tuple(str(item) for item in authority["markets"])
    active_index = load_active_calendar_index(
        boundary=boundary,
        expected_markets=markets,
    )
    if active_index.receipt != calendar_receipt:
        raise IntegrityError("calendar successor index is no longer active")
    verify_calendar_freshness(
        active_index,
        expected_markets=markets,
        now=datetime.now(timezone.utc),
    )

    policies_receipt = _receipt(
        reusable_completed["foundation_policy"],
        name="reusable foundation policy",
    )
    policies = VerifiedFoundationPolicies.from_release(
        policies_receipt,
        boundary=boundary,
    )
    session_receipt = _receipt(
        reusable_completed["session_policy"],
        name="reusable session policy",
    )
    session_policy = load_versioned_session_policy(
        session_receipt,
        policies=policies,
        boundary=boundary,
    )
    market_state_receipt = _receipt(
        reusable_completed["market_state"],
        name="reusable market state",
    )
    coverage_policy = FoundationCoveragePolicy.from_file(
        boundary.active_root / "configs" / "foundation_coverage_policy.json"
    )
    scope_policy = StatusResearchScopePolicy.from_file(
        boundary.active_root
        / "configs"
        / "status_research_scope_policy.json"
    )
    statistics_roles = StatisticsRolePolicy.from_file(
        boundary.active_root
        / "configs"
        / "statistics_foundation_roles.json"
    )
    market_state = load_market_state_foundation(
        market_state_receipt,
        boundary=boundary,
        expected_selection=resolved,
        expected_source_selection_receipt=selection_receipt,
        expected_policies=policies,
        expected_coverage_policy=coverage_policy,
        expected_scope_policy=scope_policy,
        expected_statistics_roles=statistics_roles,
        trusted_checkpoint_mtime_ns=reusable_path.stat().st_mtime_ns,
    )

    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={
            "approval_receipt_id": approval_id,
            "foundation_run_id": str(authority["foundation_run_id"]),
            "plan_id": str(plan["plan_id"]),
        },
    )
    orchestrator = FoundationOrchestrator(
        boundary=boundary,
        operation_receipt=operation,
        batch_rows=BATCH_ROWS,
    )
    checkpoint_path = (
        boundary.active_root / str(scope["output_paths"]["checkpoint"])
    )
    run_id = str(authority["foundation_run_id"])
    run_contract = authority["foundation_run_contract"]
    if not isinstance(run_contract, dict) or sha256_json(run_contract) != run_id:
        raise IntegrityError("calendar successor run contract identity drifted")

    global_lock = boundary.assert_active_path(
        boundary.active_root / "state" / "locks" / "foundation-build.lock",
        purpose="global calendar foundation successor lock",
        subtree="state/locks",
    )
    run_lock = boundary.assert_active_path(
        boundary.active_root / str(scope["output_paths"]["run_lock"]),
        purpose="calendar foundation successor run lock",
        subtree="state/locks",
    )
    with FileLease(global_lock), FileLease(run_lock):
        core = _load_successor_checkpoint(
            path=checkpoint_path,
            run_id=run_id,
            run_contract=run_contract,
        )
        completed = core["completed"]
        assert isinstance(completed, dict)
        exact_top = {
            "source_bound": {
                "selection_manifest_id": selection["selection_manifest_id"],
                "source_dbn_release_id": snapshot.source_release_id,
                "source_selection_receipt_id": selection_receipt.receipt_id,
            },
            "pre_calendar_checkpoint": {
                "checkpoint_id": reusable_authority["checkpoint_id"],
                "run_id": reusable_authority["run_id"],
            },
            "foundation_policy": policies_receipt.as_dict(),
            "session_policy": session_receipt.as_dict(),
            "market_state": market_state_receipt.as_dict(),
        }
        for name, value in exact_top.items():
            if name in completed:
                if completed[name] != value:
                    raise IntegrityError(
                        f"calendar successor {name} checkpoint drifted"
                    )
            else:
                completed[name] = value
                _persist_successor_checkpoint(
                    checkpoint_path,
                    core,
                    phase=name,
                    limiter=limiter,
                )

        if "calendar_coverage" in completed:
            calendar_coverage_receipt = _receipt(
                completed["calendar_coverage"],
                name="calendar coverage",
            )
            calendar_coverage = load_foundation_calendar_coverage(
                calendar_coverage_receipt,
                boundary=boundary,
                expected_intervals=intervals,
            )
            if calendar_coverage.index.receipt != calendar_receipt:
                raise IntegrityError("calendar coverage index drifted")
        else:
            calendar_coverage_receipt = publish_foundation_calendar_coverage(
                index_receipt=calendar_receipt,
                intervals=intervals,
                publisher=orchestrator.publisher,
            )
            completed["calendar_coverage"] = (
                calendar_coverage_receipt.as_dict()
            )
            _persist_successor_checkpoint(
                checkpoint_path,
                core,
                phase="calendar_coverage",
                limiter=limiter,
            )

        interval_states = completed.setdefault("intervals", {})
        assembled = completed.setdefault("assembled_intervals", {})
        if not isinstance(interval_states, dict) or not isinstance(
            assembled, dict
        ):
            raise IntegrityError("calendar successor interval checkpoint is invalid")
        expected_keys = {
            _interval_key(
                interval.market,
                interval.year,
                interval.start,
                interval.end,
            )
            for interval in intervals
        }
        if (
            not set(interval_states).issubset(expected_keys)
            or not set(assembled).issubset(expected_keys)
        ):
            raise IntegrityError("calendar successor checkpoint has unknown interval")

        for interval in intervals:
            key = _interval_key(
                interval.market,
                interval.year,
                interval.start,
                interval.end,
            )
            if key in assembled:
                if not isinstance(assembled[key], dict):
                    raise IntegrityError(
                        "calendar successor assembled record is invalid"
                    )
                continue
            reusable_state = reusable_intervals.get(key)
            if (
                not isinstance(reusable_state, dict)
                or set(reusable_state) != _REUSABLE_PHASES
            ):
                raise IntegrityError(
                    "calendar successor reusable interval is incomplete"
                )
            state = interval_states.setdefault(
                key,
                {
                    name: reusable_state[name]
                    for name in (
                        "raw",
                        "definitions",
                        "causal",
                        "status_eligibility",
                        "economics",
                    )
                },
            )
            if (
                not isinstance(state, dict)
                or not set(state).issubset(_SUCCESSOR_INTERVAL_PHASES)
                or any(
                    state.get(name) != reusable_state[name]
                    for name in (
                        "raw",
                        "definitions",
                        "causal",
                        "status_eligibility",
                        "economics",
                    )
                )
            ):
                raise IntegrityError(
                    "calendar successor reusable interval receipt drifted"
                )
            raw_receipt = _receipt(state["raw"], name=f"{key}:raw")
            definition_receipt = _receipt(
                state["definitions"],
                name=f"{key}:definitions",
            )
            causal_receipt = _receipt(state["causal"], name=f"{key}:causal")
            status_receipt = _receipt(
                state["status_eligibility"],
                name=f"{key}:status",
            )
            economics_receipt = _receipt(
                state["economics"],
                name=f"{key}:economics",
            )
            raw = load_raw_interval(raw_receipt, boundary=boundary)
            definitions = load_actual_contract_definitions(
                definition_receipt,
                raw_receipt=raw_receipt,
                policies=policies,
                boundary=boundary,
            )
            _, causal_report = load_causal_interval(
                causal_receipt,
                boundary=boundary,
            )
            if (
                causal_report.get("source_raw_release_id")
                != raw_receipt.release_id
                or causal_report.get("foundation_policy_release_id")
                != policies_receipt.release_id
                or causal_report.get("foundation_policy_set_id")
                != policies.policy_set_id
            ):
                raise IntegrityError(
                    "calendar successor causal policy binding drifted"
                )

            if "calendar_eligibility" in state:
                calendar_eligibility_receipt = _receipt(
                    state["calendar_eligibility"],
                    name=f"{key}:calendar",
                )
            else:
                calendar_eligibility_receipt = (
                    publish_calendar_state_eligibility(
                        causal_receipt=causal_receipt,
                        coverage_receipt=calendar_coverage_receipt,
                        market=interval.market,
                        year=interval.year,
                        interval_key=key,
                        publisher=orchestrator.publisher,
                    )
                )
                state["calendar_eligibility"] = (
                    calendar_eligibility_receipt.as_dict()
                )
                _persist_successor_checkpoint(
                    checkpoint_path,
                    core,
                    phase=f"{key}:calendar_eligibility",
                    limiter=limiter,
                )
            calendar_eligibility = load_calendar_state_eligibility(
                calendar_eligibility_receipt,
                boundary=boundary,
                expected_causal_receipt=causal_receipt,
                expected_coverage_receipt=calendar_coverage_receipt,
            )
            if (
                calendar_eligibility["disposition"] != "ELIGIBLE"
                or calendar_eligibility["interval_key"] != key
            ):
                raise IntegrityError(
                    "calendar successor eligibility failed closed"
                )

            status = load_status_eligibility(
                status_receipt,
                causal_receipt=causal_receipt,
                market_state_receipt=market_state_receipt,
                boundary=boundary,
            )
            economics = verify_actual_contract_economics_context(
                economics_receipt,
                causal_receipt=causal_receipt,
                definitions=definitions,
                policies=policies,
                session_policy=session_policy,
                boundary=boundary,
            )
            feature_receipt = orchestrator._ensure_feature_input(
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
                after_checkpoint=limiter,
            )
            outcome_receipt = orchestrator._ensure_outcome_source_input(
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
                after_checkpoint=limiter,
            )
            feature = load_feature_source_input(
                feature_receipt,
                boundary=boundary,
            )
            outcome = load_outcome_source_input(
                outcome_receipt,
                boundary=boundary,
            )
            interval_bar_rows = int(status["total_rows"])
            feature_ready_rows = int(feature["feature_ready_rows"])
            eligible_rows = int(status["eligible_rows"])
            resolved_rows = int(status["resolved_status_rows"])
            unresolved_rows = int(status["unresolved_status_rows"])
            resolved_fraction = (
                Decimal(resolved_rows) / Decimal(interval_bar_rows)
                if interval_bar_rows
                else Decimal(0)
            )
            gated_fraction = (
                Decimal(eligible_rows) / Decimal(feature_ready_rows)
                if feature_ready_rows
                else Decimal(0)
            )
            coverage_passed = (
                resolved_fraction
                >= coverage_policy.minimum_status_resolved_decision_fraction
                and gated_fraction
                >= coverage_policy.minimum_status_gated_feature_ready_fraction
            )
            in_scope = scope_policy.includes_interval(
                start=interval.start,
                end=interval.end,
            )
            gate_core = {
                "bar_rows": interval_bar_rows,
                "feature_ready_rows": feature_ready_rows,
                "in_research_scope": in_scope,
                "interval_key": key,
                "research_disposition": scope_policy.disposition(
                    start=interval.start,
                    end=interval.end,
                    coverage_passed=coverage_passed,
                ),
                "research_scope_policy_hash": scope_policy.policy_hash,
                "status_gated_feature_ready_fraction": str(gated_fraction),
                "status_gated_feature_ready_rows": eligible_rows,
                "status_eligible_rows": eligible_rows,
                "status_query_contract_ids": sorted(
                    item.query_contract_id for item in interval.status
                ),
                "status_query_mode_ids": sorted(
                    {item.query_mode_id for item in interval.status}
                ),
                "status_resolved_decision_fraction": str(resolved_fraction),
                "status_resolved_rows": resolved_rows,
                "status_source_present": bool(interval.status),
                "status_unresolved_rows": unresolved_rows,
            }
            gate = {
                **gate_core,
                "status_epoch_gate_id": sha256_json(gate_core),
            }
            raw_contract = raw.interval_receipt
            expected_dependencies = {
                "calendar_coverage_receipt": (
                    calendar_coverage_receipt.as_dict()
                ),
                "calendar_state_eligibility_receipt": (
                    calendar_eligibility_receipt.as_dict()
                ),
            }
            if (
                raw_contract.get("market") != interval.market
                or raw_contract.get("year") != interval.year
                or feature_ready_rows <= 0
                or eligible_rows > feature_ready_rows
                or any(
                    feature[name] != value or outcome[name] != value
                    for name, value in expected_dependencies.items()
                )
            ):
                raise IntegrityError(
                    "calendar successor assembled interval binding is invalid"
                )
            record = {
                "bar_query_contract_id": interval.bars.query_contract_id,
                "bar_query_mode_id": interval.bars.query_mode_id,
                "bar_source_path": interval.bars.binding.relative_path,
                "bar_source_sha256": interval.bars.binding.sha256,
                "calendar_eligibility_release_receipt": (
                    calendar_eligibility_receipt.as_dict()
                ),
                "causal_release_receipt": causal_receipt.as_dict(),
                "coverage_disposition": interval.coverage_disposition,
                "definition_release_receipt": definition_receipt.as_dict(),
                "definition_query_contract_id": (
                    interval.definition.query_contract_id
                ),
                "definition_query_mode_id": (
                    interval.definition.query_mode_id
                ),
                "definition_source_path": (
                    interval.definition.binding.relative_path
                ),
                "definition_source_sha256": interval.definition.binding.sha256,
                "economics_release_receipt": economics_receipt.as_dict(),
                "end": interval.end,
                "feature_input_release_receipt": feature_receipt.as_dict(),
                "feature_ready_rows": feature_ready_rows,
                "interval_key": key,
                "market": interval.market,
                "outcome_source_input_release_receipt": (
                    outcome_receipt.as_dict()
                ),
                "raw_release_receipt": raw_receipt.as_dict(),
                "start": interval.start,
                "status_eligibility_release_receipt": status_receipt.as_dict(),
                "status_epoch_gate": gate,
                "status_eligible_rows": eligible_rows,
                "status_gated_feature_ready_rows": eligible_rows,
                "status_resolved_rows": resolved_rows,
                "status_unresolved_rows": unresolved_rows,
                "year": interval.year,
            }
            assembled[key] = record
            _persist_successor_checkpoint(
                checkpoint_path,
                core,
                phase=f"{key}:assembled",
                limiter=limiter,
            )

        if set(assembled) != expected_keys:
            raise IntegrityError(
                "calendar successor assembly is unexpectedly incomplete"
            )
        verified_intervals = [
            assembled[key] for key in sorted(assembled)
        ]
        if any(not isinstance(item, dict) for item in verified_intervals):
            raise IntegrityError("calendar successor assembled record is invalid")
        coverage_gate = FoundationSuccessorFinalizer._coverage_gate(
            intervals=verified_intervals,
            resolved=resolved,
            market_state=market_state,
            coverage_policy=coverage_policy,
            scope_policy=scope_policy,
        )
        coverage_gate.update(
            {
                "calendar_contract_status": "BOUND_AND_ALL_ROWS_OPEN",
                "calendar_coverage_receipt_id": (
                    calendar_coverage_receipt.receipt_id
                ),
            }
        )
        foundation_core = {
            "alpha_evidence": False,
            "calendar_coverage_receipt": calendar_coverage_receipt.as_dict(),
            "candidate_eligible": False,
            "coverage_gate": coverage_gate,
            "coverage_matrix": list(resolved.coverage_matrix),
            "coverage_matrix_id": resolved.coverage_matrix_id,
            "dependency_closure_complete": True,
            "feature_role_contract": {
                "eligibility_authority": "STATUS_AS_OF_ELIGIBILITY_RELEASE",
                "mechanical_feature_values_are_not_standalone_research_eligibility": True,
                "statistics_feature_use": False,
            },
            "feature_spec": feature_spec.as_dict(),
            "feature_spec_hash": feature_spec.spec_hash,
            "foundation_policy_receipt": policies_receipt.as_dict(),
            "historical_outcome_or_label_execution": False,
            "interval_count": len(verified_intervals),
            "intervals": verified_intervals,
            "learned_or_outcome_informed_transform_count": 0,
            "market_state_release_receipt": market_state_receipt.as_dict(),
            "model_fit_count": 0,
            "outcome_contract": {
                "deferred_until": (
                    "SEPARATELY_AUTHORIZED_PREDECLARED_SAMPLE_OR_PREDICTION_CONTRACT"
                ),
                "labels_materialized": False,
                "prediction_ledger_read": False,
                "role": "LABELABLE_VERIFIED_CAUSAL_BARS_ONLY",
            },
            "provider_call_count": 0,
            "query_manifest": list(resolved.query_manifest),
            "query_manifest_id": resolved.query_manifest_id,
            "query_mode_census": list(resolved.query_mode_census),
            "run_contract": run_contract,
            "run_id": run_id,
            "schema_version": FOUNDATION_CALENDAR_SCHEMA_VERSION,
            "session_policy_receipt": session_receipt.as_dict(),
            "source_dbn_release_id": snapshot.source_release_id,
            "source_selection_receipt": selection_receipt.as_dict(),
            "wfa_execution_count": 0,
        }
        foundation_set = {
            **foundation_core,
            "foundation_set_id": sha256_json(foundation_core),
        }
        if "foundation_set" in completed:
            foundation_receipt = _receipt(
                completed["foundation_set"],
                name="calendar foundation set",
            )
            foundation_manifest = verify_data_release_manifest(
                boundary.active_root / foundation_receipt.manifest_path,
                boundary,
                verify_files=False,
            )
            if (
                foundation_manifest.embedded_documents.get(
                    "foundation_set.json"
                )
                != foundation_set
            ):
                raise IntegrityError("completed calendar foundation changed")
        else:
            stage = orchestrator.publisher.create_stage(
                "calendar_foundation_successor"
            )
            dependency_ids = {
                calendar_coverage_receipt.release_id,
                market_state_receipt.release_id,
                policies_receipt.release_id,
                selection_receipt.release_id,
                session_receipt.release_id,
            }
            for item in verified_intervals:
                for name in (
                    "raw_release_receipt",
                    "definition_release_receipt",
                    "causal_release_receipt",
                    "calendar_eligibility_release_receipt",
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
                schema_version=FOUNDATION_CALENDAR_SCHEMA_VERSION,
                logical_paths={},
                source_release_ids=tuple(sorted(dependency_ids)),
                embedded_documents={"foundation_set.json": foundation_set},
                metadata={
                    "coverage_matrix_id": resolved.coverage_matrix_id,
                    "feature_spec_hash": feature_spec.spec_hash,
                    "foundation_set_id": foundation_set["foundation_set_id"],
                    "interval_count": len(verified_intervals),
                    "query_manifest_id": resolved.query_manifest_id,
                    "run_id": run_id,
                    "source_dbn_release_id": snapshot.source_release_id,
                },
            )
            manifest_path = orchestrator.publisher.publish(stage, manifest)
            foundation_receipt = VerifiedReleaseReceipt.from_manifest(
                manifest_path,
                boundary,
                verify_files=False,
            )
            readback = verify_data_release_manifest(
                manifest_path,
                boundary,
                verify_files=False,
            )
            if (
                readback.embedded_documents.get("foundation_set.json")
                != foundation_set
            ):
                raise IntegrityError(
                    "calendar foundation publication failed exact readback"
                )
            completed["foundation_set"] = foundation_receipt.as_dict()
            core["status"] = "COMPLETE"
            _persist_successor_checkpoint(
                checkpoint_path,
                core,
                phase="foundation_set",
                limiter=limiter,
            )
        return {
            "checkpoint_path": checkpoint_path,
            "foundation_receipt": foundation_receipt,
            "run_id": run_id,
        }


def execute_plan(
    *,
    plan_path: Path,
    approval_path: Path,
    boundary: RepoBoundary,
) -> dict[str, object]:
    _require_official_historical_calendar_route(boundary)
    plan = validate_plan(
        _read_object(plan_path, description="foundation successor plan"),
        boundary=boundary,
    )
    approval = _read_object(
        approval_path, description="foundation successor approval"
    )
    approval_id = validate_approval(
        approval,
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    limiter = InvocationLimiter()
    try:
        result = _run_calendar_successor(
            plan=plan,
            approval_id=approval_id,
            boundary=boundary,
            limiter=limiter,
        )
    except InvocationBudgetReached:
        scope = plan["scope"]
        assert isinstance(scope, dict)
        authority = scope["authority"]
        assert isinstance(authority, dict)
        checkpoint = (
            boundary.active_root / str(scope["output_paths"]["checkpoint"])
        )
        summary = _checkpoint_summary(checkpoint)
        return {
            "approval_receipt_id": approval_id,
            "foundation_run_id": authority["foundation_run_id"],
            "invocation": {
                "completed_intervals": limiter.completed_intervals,
                "persisted_checkpoints": limiter.checkpoints,
            },
            "plan_id": plan["plan_id"],
            "status": "INCOMPLETE_RESTART_SAFE",
            **summary,
        }
    foundation_receipt = result["foundation_receipt"]
    if (
        not isinstance(foundation_receipt, VerifiedReleaseReceipt)
        or foundation_receipt.schema_version
        != FOUNDATION_CALENDAR_SCHEMA_VERSION
    ):
        raise IntegrityError(
            "calendar-bound foundation successor failed exact readback"
        )
    return {
        "approval_receipt_id": approval_id,
        "foundation_run_id": result["run_id"],
        "foundation_set_receipt": foundation_receipt.as_dict(),
        "invocation": {
            "completed_intervals": limiter.completed_intervals,
            "persisted_checkpoints": limiter.checkpoints,
        },
        "plan_id": plan["plan_id"],
        "status": "COMPLETE",
        **_checkpoint_summary(result["checkpoint_path"]),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--source-dbn-manifest", type=Path, required=True)
    plan.add_argument("--source-selection-manifest", type=Path, required=True)
    plan.add_argument(
        "--predecessor-foundation-manifest", type=Path, required=True
    )
    plan.add_argument("--reusable-checkpoint", type=Path, required=True)
    plan.add_argument("--calendar-index-manifest", type=Path, required=True)
    plan.add_argument("--feature-spec", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    run = commands.add_parser("run")
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--approval", type=Path, required=True)
    run.add_argument("--execute", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.repository_root.resolve(strict=True)
    source_contract = args.source_contract.resolve(strict=True)
    boundary = _boundary_from_contract(root, source_contract)
    if args.command == "plan":
        payload = build_plan(
            boundary=boundary,
            source_dbn_manifest_path=args.source_dbn_manifest.resolve(
                strict=True
            ),
            source_selection_manifest_path=(
                args.source_selection_manifest.resolve(strict=True)
            ),
            predecessor_foundation_manifest_path=(
                args.predecessor_foundation_manifest.resolve(strict=True)
            ),
            reusable_checkpoint_path=(
                args.reusable_checkpoint.resolve(strict=True)
            ),
            calendar_index_manifest_path=(
                args.calendar_index_manifest.resolve(strict=True)
            ),
            feature_spec_path=args.feature_spec.resolve(strict=True),
        )
        output = (
            args.output
            if args.output.is_absolute()
            else boundary.active_root / args.output
        )
        boundary.assert_active_path(
            output,
            purpose="calendar-bound foundation successor plan",
            subtree="reports",
        )
        _write_create_only(output, payload)
        print(canonical_bytes(payload).decode("utf-8"))
        return 0
    if not args.execute:
        raise SystemExit(
            "foundation calendar-successor execution requires explicit --execute"
        )
    result = execute_plan(
        plan_path=args.plan.resolve(strict=True),
        approval_path=args.approval.resolve(strict=True),
        boundary=boundary,
    )
    print(canonical_bytes(result).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
