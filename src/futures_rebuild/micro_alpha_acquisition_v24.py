"""Reconstruction-stable successor for Apex micro Phase 1A inactive custody.

V21 remains immutable failure evidence.  V24 downloads every authorized annual
request again into a new staging root; it never resumes or promotes predecessor bytes.
Downloaded DBNs are hashed as opaque files and are never decoded.
"""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import threading
import time
import warnings
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .micro_alpha_acquisition import (
    DownloadProviderApis,
    build_file_download_provider_apis,
)
from .micro_alpha_databento_preflight import CREDENTIAL_SOURCE
from .micro_alpha_pipeline import CURRENT_ACQUISITION_MARKETS, DATASET, SCHEMAS
from .runtime_environment import require_locked_repository_environment


PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_phase1a_acquisition_plan_v24.json"
)
AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v24/audit.json"
)
OPERATION: Final = "ACQUIRE_APEX_MICRO_TIER01_RAW_DBN_INACTIVE_CUSTODY_V24_ONCE"
STAGING_ROOT: Final = Path(
    "state/provider_acquisition_staging/apex_micro_tier01_v24"
)
V21_PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_phase1a_acquisition_plan_v21.json"
)
V21_AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v21/audit.json"
)
V21_FAILURE_REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_v21_failure/report.json"
)
V21_AUTHORIZATION_PATH: Final = Path(
    "state/authorization_uses/"
    "5c04fecd51692b216f468ccf1eecbf72e918d06e675b2a4287a03e4c684ac282.json"
)
V22_PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_phase1a_acquisition_plan_v22.json"
)
V22_AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v22/audit.json"
)
V22_CENSUS_PATH: Final = Path(
    "state/unpublished_evidence/safe_cleanup_candidate_census_v7/census.json"
)
V22_SUPERSESSION_PATH: Final = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1a_acquisition_v22_supersession/report.json"
)
STANDARD_TOPOLOGY_PATH: Final = Path(
    "state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json"
)
CLEANUP_CENSUS_PATH: Final = Path(
    "state/unpublished_evidence/safe_cleanup_candidate_census_v9/census.json"
)
V23_PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_phase1a_acquisition_plan_v23.json"
)
V23_AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v23/audit.json"
)
V23_CENSUS_PATH: Final = Path(
    "state/unpublished_evidence/safe_cleanup_candidate_census_v8/census.json"
)
V23_SUPERSESSION_PATH: Final = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1a_acquisition_v23_supersession/report.json"
)
V21_PLAN_ID: Final = (
    "a21652882790dfe2a9d56ebce9edab7b223e5d29d49af7edcae2774e3517899b"
)
V21_PLAN_SHA256: Final = (
    "36e7c7a39474a1c0e8f5b424653961d09dd878a2879580f8aced8d1bd28455fc"
)
V21_AUDIT_ID: Final = (
    "33c4a63c5b4a6dc371ed5816be2c170b8c64a6eb4efe36c2b8a7d1ff2d846707"
)
V21_AUDIT_SHA256: Final = (
    "4151529f4d20f93f15ac8b22ffde290e033e81607febd9c5d765cd6d14ffb73f"
)
V21_FAILURE_REPORT_ID: Final = (
    "4d73fc36955be62df88b196094f272395779a25af9b053d69876752c9592b4d0"
)
V21_FAILURE_REPORT_SHA256: Final = (
    "ad1aa50c0f9dd2362479e049cbed0c69d116beebdd4a793153db534e82668b61"
)
V21_AUTHORIZATION_RECEIPT_ID: Final = (
    "5c04fecd51692b216f468ccf1eecbf72e918d06e675b2a4287a03e4c684ac282"
)
V21_AUTHORIZATION_SHA256: Final = (
    "6f1995eee7cb3a0c36fcfacc1a7feae1d588e3b1b7c484ea3b8508a5d32ffe5e"
)
V22_PLAN_ID: Final = (
    "d0fbcaa2e787910d7bf90d55a8ce623380bc5026386dd899042b918f85556e37"
)
V22_PLAN_SHA256: Final = (
    "829972937ad2baa35a06eeb131e000a5766f18447d0007aa22e11f1d4c0245de"
)
V22_AUDIT_ID: Final = (
    "d09bd0855a40c63de428fe8f25e9a3d00b3e85819e2bdbd73d570499d874eb13"
)
V22_AUDIT_SHA256: Final = (
    "561a196bc8059e0ac241e3f2f828f09df6489cfa7fd7b2488c19bc4111ab775f"
)
V22_CENSUS_ID: Final = (
    "4e065c60f31cc9f5c1e40155e01eb2ea21638827a05174252a4693d557ea8639"
)
V22_CENSUS_SHA256: Final = (
    "97d145ad6a3f5f25a6c2a27c665f649972573715cefd3e986476da77b4b5cf57"
)
V22_SUPERSESSION_ID: Final = (
    "1e0fa5e779465b0ac7840f53cc9615482c52412d5ce078204b09b85de116f2ad"
)
V22_SUPERSESSION_SHA256: Final = (
    "8c971ddd59f7a69320eae8ca3ee41ea0d46b2ce5196475d816699d679a8e86f0"
)
V23_PLAN_ID: Final = (
    "a1121d8aa7980cf757d4492f86c9e160effbd9c767a03666e05e5079c12ffb79"
)
V23_PLAN_SHA256: Final = (
    "90b5711d558f04c55bf0b88f5dce516a75fc86b19343c59b80f893d34342d299"
)
V23_AUDIT_ID: Final = (
    "978ba98c3fde9b1d57ea90b7d095be94695c6043b53744bb8907c7248c8edc94"
)
V23_AUDIT_SHA256: Final = (
    "8d31f40d5d8b130fbde311023b27723b004de89d201597d9b58f2944edc94054"
)
V23_CENSUS_ID: Final = (
    "74c1c04692e7237565c14e226005844614ff074b5f3952584a820c4c60d423f8"
)
V23_CENSUS_SHA256: Final = (
    "e727cd6bb2bd4562882ef9206a858de94ed0b2d6c37fda87cac08bf4284976ce"
)
V23_SUPERSESSION_ID: Final = (
    "5da8e19ef1d98e1bd1f3b8a98ec965fd5e50734176fd8261ab77271df22c4119"
)
V23_SUPERSESSION_SHA256: Final = (
    "9a7691575160b9f4e7f25130036ad6960830333b2846543930efb4abd92ce723"
)
MAXIMUM_RUNTIME_SECONDS: Final = 43_200
MAXIMUM_PER_DOWNLOAD_SECONDS: Final = 900
MAXIMUM_RETRIES: Final = 0
MAXIMUM_DBN_FILES: Final = 160
MAXIMUM_SIDECARS: Final = 160
MAXIMUM_PARALLEL_DOWNLOADS: Final = 2
MAXIMUM_PROVIDER_CLIENTS: Final = 3


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{description} is not an object")
    return value


def _git_head(root: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    head = completed.stdout.strip()
    if len(head) != 40:
        raise IntegrityError("committed implementation HEAD is invalid")
    return head


def _self_hashed(value: Mapping[str, object], key: str) -> bool:
    core = dict(value)
    observed = core.pop(key, None)
    return observed == sha256_json(core)


def _validate_predecessor_evidence(
    *, root: Path
) -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
]:
    plan = _object(root / V21_PLAN_PATH, "v21 acquisition plan")
    audit = _object(root / V21_AUDIT_PATH, "v21 acquisition audit")
    failure = _object(root / V21_FAILURE_REPORT_PATH, "v21 failure report")
    authorization = _object(root / V21_AUTHORIZATION_PATH, "v21 authorization use")
    if (
        plan.get("plan_id") != V21_PLAN_ID
        or not _self_hashed(plan, "plan_id")
        or sha256_file(root / V21_PLAN_PATH) != V21_PLAN_SHA256
        or audit.get("audit_id") != V21_AUDIT_ID
        or not _self_hashed(audit, "audit_id")
        or sha256_file(root / V21_AUDIT_PATH) != V21_AUDIT_SHA256
        or failure.get("report_id") != V21_FAILURE_REPORT_ID
        or not _self_hashed(failure, "report_id")
        or sha256_file(root / V21_FAILURE_REPORT_PATH)
        != V21_FAILURE_REPORT_SHA256
        or failure.get("state")
        != "SEALED_FAIL_CLOSED_RUNTIME_CEILING_NO_ACCEPTED_SOURCE"
        or failure.get("accepted_dbn_count") != 0
        or failure.get("accepted_sidecar_count") != 0
        or failure.get("final_destination_count") != 0
        or failure.get("automatic_retries") != 0
        or failure.get("predecessor_staging_reusable_by_successor") is not False
        or authorization.get("receipt_id") != V21_AUTHORIZATION_RECEIPT_ID
        or sha256_file(root / V21_AUTHORIZATION_PATH) != V21_AUTHORIZATION_SHA256
    ):
        raise IntegrityError("v21 acquisition failure evidence drifted")
    v22_plan = _object(root / V22_PLAN_PATH, "v22 acquisition plan")
    v22_audit = _object(root / V22_AUDIT_PATH, "v22 acquisition audit")
    v22_census = _object(root / V22_CENSUS_PATH, "v22 cleanup census")
    supersession = _object(root / V22_SUPERSESSION_PATH, "v22 supersession")
    if (
        v22_plan.get("plan_id") != V22_PLAN_ID
        or not _self_hashed(v22_plan, "plan_id")
        or sha256_file(root / V22_PLAN_PATH) != V22_PLAN_SHA256
        or v22_audit.get("audit_id") != V22_AUDIT_ID
        or not _self_hashed(v22_audit, "audit_id")
        or sha256_file(root / V22_AUDIT_PATH) != V22_AUDIT_SHA256
        or v22_census.get("census_id") != V22_CENSUS_ID
        or not _self_hashed(v22_census, "census_id")
        or sha256_file(root / V22_CENSUS_PATH) != V22_CENSUS_SHA256
        or supersession.get("report_id") != V22_SUPERSESSION_ID
        or not _self_hashed(supersession, "report_id")
        or sha256_file(root / V22_SUPERSESSION_PATH)
        != V22_SUPERSESSION_SHA256
        or supersession.get("state")
        != "SUPERSEDED_PREPARATION_SELF_REFERENTIAL_CENSUS"
        or supersession.get("disposition", {}).get("execute_v22_plan") is not False
    ):
        raise IntegrityError("v22 superseded preparation evidence drifted")
    v23_plan = _object(root / V23_PLAN_PATH, "v23 acquisition plan")
    v23_audit = _object(root / V23_AUDIT_PATH, "v23 acquisition audit")
    v23_census = _object(root / V23_CENSUS_PATH, "v23 cleanup census")
    v23_supersession = _object(
        root / V23_SUPERSESSION_PATH, "v23 supersession"
    )
    if (
        v23_plan.get("plan_id") != V23_PLAN_ID
        or not _self_hashed(v23_plan, "plan_id")
        or sha256_file(root / V23_PLAN_PATH) != V23_PLAN_SHA256
        or v23_audit.get("audit_id") != V23_AUDIT_ID
        or not _self_hashed(v23_audit, "audit_id")
        or sha256_file(root / V23_AUDIT_PATH) != V23_AUDIT_SHA256
        or v23_census.get("census_id") != V23_CENSUS_ID
        or not _self_hashed(v23_census, "census_id")
        or sha256_file(root / V23_CENSUS_PATH) != V23_CENSUS_SHA256
        or v23_supersession.get("report_id") != V23_SUPERSESSION_ID
        or not _self_hashed(v23_supersession, "report_id")
        or sha256_file(root / V23_SUPERSESSION_PATH)
        != V23_SUPERSESSION_SHA256
        or v23_supersession.get("state")
        != "SUPERSEDED_PREPARATION_VOLATILE_CAPACITY_SNAPSHOT"
        or v23_supersession.get("disposition", {}).get("execute_v23_plan")
        is not False
    ):
        raise IntegrityError("v23 superseded preparation evidence drifted")
    return plan, failure, supersession, v23_supersession


def build_acquisition_plan(
    *, root: Path, committed_head: str, require_destination_absence: bool = True
) -> dict[str, object]:
    """Freeze a reconstruction-stable successor for all 160 annual requests."""

    root = root.resolve(strict=True)
    if committed_head != _git_head(root):
        raise IntegrityError("v24 plan must bind the live committed HEAD")
    predecessor, failure, v22_supersession, v23_supersession = (
        _validate_predecessor_evidence(root=root)
    )
    requests = json.loads(json.dumps(predecessor["requests"]))
    if not isinstance(requests, list) or len(requests) != MAXIMUM_DBN_FILES:
        raise IntegrityError("v21 request set is invalid for v24")
    destinations = [
        str(item[key])
        for item in requests
        for key in ("dbn_destination", "sidecar_destination")
    ]
    if len(set(destinations)) != 320:
        raise IntegrityError("v24 destinations collide")
    if require_destination_absence and any((root / path).exists() for path in destinations):
        raise IntegrityError("v24 destination already exists")
    implementation_paths = (
        "configs/dependency_lock_receipt.json",
        "scripts/prepare_apex_micro_phase1a_acquisition_v24.py",
        "scripts/prepare_apex_micro_phase1a_acquisition_v22_supersession.py",
        "scripts/prepare_apex_micro_phase1a_acquisition_v23_supersession.py",
        "scripts/audit_standard_data_topology_source_safe.py",
        "scripts/prepare_safe_cleanup_candidate_census_v6.py",
        "scripts/prepare_safe_cleanup_candidate_census_v7.py",
        "scripts/prepare_safe_cleanup_candidate_census_v8.py",
        "scripts/prepare_safe_cleanup_candidate_census_v9.py",
        "src/futures_rebuild/boundary.py",
        "src/futures_rebuild/canonical.py",
        "src/futures_rebuild/live_cockpit/databento_auth.py",
        "src/futures_rebuild/micro_alpha_acquisition.py",
        "src/futures_rebuild/micro_alpha_acquisition_v21.py",
        "src/futures_rebuild/micro_alpha_acquisition_v24.py",
        "src/futures_rebuild/micro_alpha_pipeline.py",
        "src/futures_rebuild/research_gateway_policy.py",
        "src/futures_rebuild/runtime_environment.py",
    )
    core = dict(predecessor)
    core.pop("plan_id", None)
    core.update(
        {
            "schema_version": "apex_micro_phase1a_acquisition_plan/24.0.0",
            "state": "PREPARED_REQUIRES_SEPARATE_EXACT_DOWNLOAD_APPROVAL",
            "operation": OPERATION,
            "committed_implementation_head": committed_head,
            "lane_id": "apex_integer_micro_24",
            "requests": requests,
            "implementation_hashes": {
                path: sha256_file(root / path) for path in implementation_paths
            },
            "predecessor_failure_evidence": {
                "plan_path": V21_PLAN_PATH.as_posix(),
                "plan_id": V21_PLAN_ID,
                "plan_sha256": V21_PLAN_SHA256,
                "audit_path": V21_AUDIT_PATH.as_posix(),
                "audit_id": V21_AUDIT_ID,
                "audit_sha256": V21_AUDIT_SHA256,
                "failure_report_path": V21_FAILURE_REPORT_PATH.as_posix(),
                "failure_report_id": V21_FAILURE_REPORT_ID,
                "failure_report_sha256": V21_FAILURE_REPORT_SHA256,
                "authorization_receipt_id": V21_AUTHORIZATION_RECEIPT_ID,
                "authorization_sha256": V21_AUTHORIZATION_SHA256,
                "verified_complete_staging_pairs": failure[
                    "verified_complete_staging_pairs"
                ],
                "accepted_source_pairs": 0,
                "staging_reuse": False,
                "automatic_retry": False,
            },
            "superseded_v22_preparation": {
                "plan_path": V22_PLAN_PATH.as_posix(),
                "plan_id": V22_PLAN_ID,
                "plan_sha256": V22_PLAN_SHA256,
                "audit_path": V22_AUDIT_PATH.as_posix(),
                "audit_id": V22_AUDIT_ID,
                "audit_sha256": V22_AUDIT_SHA256,
                "cleanup_census_path": V22_CENSUS_PATH.as_posix(),
                "cleanup_census_id": V22_CENSUS_ID,
                "cleanup_census_sha256": V22_CENSUS_SHA256,
                "supersession_report_path": V22_SUPERSESSION_PATH.as_posix(),
                "supersession_report_id": V22_SUPERSESSION_ID,
                "supersession_report_sha256": V22_SUPERSESSION_SHA256,
                "state": v22_supersession["state"],
                "provider_execution_performed": False,
                "authorization_consumed": False,
                "execute_as_current": False,
            },
            "superseded_v23_preparation": {
                "plan_path": V23_PLAN_PATH.as_posix(),
                "plan_id": V23_PLAN_ID,
                "plan_sha256": V23_PLAN_SHA256,
                "audit_path": V23_AUDIT_PATH.as_posix(),
                "audit_id": V23_AUDIT_ID,
                "audit_sha256": V23_AUDIT_SHA256,
                "cleanup_census_path": V23_CENSUS_PATH.as_posix(),
                "cleanup_census_id": V23_CENSUS_ID,
                "cleanup_census_sha256": V23_CENSUS_SHA256,
                "supersession_report_path": V23_SUPERSESSION_PATH.as_posix(),
                "supersession_report_id": V23_SUPERSESSION_ID,
                "supersession_report_sha256": V23_SUPERSESSION_SHA256,
                "state": v23_supersession["state"],
                "provider_execution_performed": False,
                "authorization_consumed": False,
                "execute_as_current": False,
            },
            "limits": {
                **dict(predecessor["limits"]),
                "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
                "maximum_per_download_seconds": MAXIMUM_PER_DOWNLOAD_SECONDS,
                "maximum_attempts": 1,
                "maximum_retries": 0,
                "maximum_parallel_downloads": MAXIMUM_PARALLEL_DOWNLOADS,
                "maximum_provider_clients": MAXIMUM_PROVIDER_CLIENTS,
                "maximum_provider_calls": 320,
            },
            "custody": {
                **dict(predecessor["custody"]),
                "predecessor_staging_reuse": False,
                "successor_redownloads_every_request": True,
                "failure_evidence_marked_read_only": True,
                "partial_finalization_rolled_back": True,
                "staging_sources_retained_until_pair_set_verified": True,
            },
            "provider_warning_policy": {
                "capture_categories_and_counts": True,
                "capture_message_contents": False,
                "warning_does_not_activate_or_certify_source": True,
                "source_certification_required_after_row_read_approval": True,
            },
        }
    )
    return {**core, "plan_id": sha256_json(core)}


def write_acquisition_plan_create_only(
    *, root: Path, committed_head: str
) -> dict[str, object]:
    plan = build_acquisition_plan(root=root, committed_head=committed_head)
    path = root / PLAN_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(plan) + b"\n")
    return plan


def load_acquisition_plan(
    *, root: Path, require_destination_absence: bool = True
) -> dict[str, object]:
    root = root.resolve(strict=True)
    plan = _object(root / PLAN_PATH, "v24 acquisition plan")
    if (
        not _self_hashed(plan, "plan_id")
        or plan.get("state")
        != "PREPARED_REQUIRES_SEPARATE_EXACT_DOWNLOAD_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("committed_implementation_head") != _git_head(root)
        or plan.get("markets") != list(CURRENT_ACQUISITION_MARKETS)
        or plan.get("schemas") != list(SCHEMAS)
    ):
        raise UnauthorizedOperation("v24 acquisition plan is absent or drifted")
    expected = build_acquisition_plan(
        root=root,
        committed_head=str(plan["committed_implementation_head"]),
        require_destination_absence=require_destination_absence,
    )
    if plan != expected:
        raise IntegrityError("v24 acquisition plan does not reconstruct exactly")
    limits = plan.get("limits")
    if (
        not isinstance(limits, Mapping)
        or limits.get("exact_request_count") != 160
        or limits.get("maximum_dbn_files") != 160
        or limits.get("maximum_sidecars") != 160
        or limits.get("maximum_provider_calls") != 320
        or limits.get("maximum_runtime_seconds") != MAXIMUM_RUNTIME_SECONDS
        or limits.get("maximum_per_download_seconds")
        != MAXIMUM_PER_DOWNLOAD_SECONDS
        or limits.get("maximum_parallel_downloads") != 2
        or limits.get("maximum_provider_clients") != 3
        or limits.get("maximum_external_cost_usd") != "0"
        or limits.get("maximum_attempts") != 1
        or limits.get("maximum_retries") != 0
        or plan.get("predecessor_failure_evidence", {}).get("staging_reuse")
        is not False
    ):
        raise IntegrityError("v24 acquisition limits drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["limits"]
    return {
        "plan_id": str(plan["plan_id"]),
        "committed_implementation_head": str(plan["committed_implementation_head"]),
        "predecessor_failure_report_id": V21_FAILURE_REPORT_ID,
        "superseded_v22_preparation_report_id": V22_SUPERSESSION_ID,
        "superseded_v23_preparation_report_id": V23_SUPERSESSION_ID,
        "markets": ",".join(CURRENT_ACQUISITION_MARKETS),
        "schemas": ",".join(SCHEMAS),
        "request_count": str(limits["exact_request_count"]),
        "maximum_dbn_files": str(limits["maximum_dbn_files"]),
        "maximum_sidecars": str(limits["maximum_sidecars"]),
        "maximum_provider_calls": str(limits["maximum_provider_calls"]),
        "maximum_total_bytes": str(limits["maximum_total_bytes"]),
        "required_free_disk_bytes": str(limits["required_free_disk_bytes"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_per_download_seconds": str(limits["maximum_per_download_seconds"]),
        "maximum_parallel_downloads": str(limits["maximum_parallel_downloads"]),
        "maximum_provider_clients": str(limits["maximum_provider_clients"]),
        "maximum_external_cost_usd": "0",
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "credential_source": CREDENTIAL_SOURCE,
        "destination_root": "data/dbn",
        "inactive_custody": "true",
        "predecessor_staging_reuse": "false",
        "dbn_row_decode": "false",
        "publication": "false",
        "catalog_activation": "false",
        "registration": "false",
        "trading": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _zero_cost(value: object) -> None:
    if isinstance(value, bool):
        raise IntegrityError("fresh acquisition cost is invalid")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError("fresh acquisition cost is invalid") from exc
    if not amount.is_finite() or amount != 0:
        raise UnauthorizedOperation("fresh acquisition cost is unexpectedly nonzero")


def _metadata_query(query: Mapping[str, object]) -> dict[str, object]:
    return {
        key: query[key]
        for key in ("dataset", "schema", "stype_in", "symbols", "start", "end")
    }


def _set_timeout(function: Callable[..., object], seconds: float) -> None:
    owner = getattr(function, "__self__", None)
    if owner is not None and hasattr(owner, "TIMEOUT"):
        setattr(owner, "TIMEOUT", max(1.0, seconds))


def _mark_read_only(path: Path) -> None:
    path.chmod(stat.S_IREAD)


def _write_terminal(path: Path, core: Mapping[str, object]) -> dict[str, object]:
    terminal = {**core, "terminal_id": sha256_json(core)}
    with path.open("xb") as stream:
        stream.write(canonical_bytes(terminal) + b"\n")
    _mark_read_only(path)
    return terminal


@dataclass(frozen=True)
class _DownloadWorkerResult:
    records: tuple[dict[str, object], ...]
    get_range_calls: int
    provider_client_created: bool
    failure_type: str | None
    failed_request_id: str | None


class _ConcurrentWarningCollector:
    """Capture warning categories by worker without retaining message text."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._categories: dict[int, list[str]] = {}
        self._original_showwarning: Callable[..., object] | None = None
        self._original_filters: list[object] = []

    def __enter__(self) -> _ConcurrentWarningCollector:
        self._original_showwarning = warnings.showwarning
        self._original_filters = list(warnings.filters)
        warnings.simplefilter("always")

        def showwarning(
            _message: object,
            category: type[Warning],
            _filename: str,
            _lineno: int,
            _file: object = None,
            _line: str | None = None,
        ) -> None:
            with self._lock:
                self._categories.setdefault(threading.get_ident(), []).append(
                    category.__name__
                )

        warnings.showwarning = showwarning
        return self

    def __exit__(self, *_args: object) -> None:
        if self._original_showwarning is not None:
            warnings.showwarning = self._original_showwarning
        warnings.filters[:] = self._original_filters

    def start_call(self) -> None:
        with self._lock:
            self._categories[threading.get_ident()] = []

    def finish_call(self) -> tuple[int, list[str]]:
        with self._lock:
            captured = self._categories.pop(threading.get_ident(), [])
        return len(captured), sorted(set(captured))


def _download_worker(
    *,
    root: Path,
    downloads: Path,
    plan_id: str,
    items: tuple[Mapping[str, object], ...],
    provider_factory: Callable[[], DownloadProviderApis],
    stop_event: threading.Event,
    total_state: dict[str, int],
    total_lock: threading.Lock,
    maximum_total_bytes: int,
    started: float,
    clock: Callable[[], float],
    warning_collector: _ConcurrentWarningCollector,
) -> _DownloadWorkerResult:
    records: list[dict[str, object]] = []
    calls = 0
    failed_request_id: str | None = None
    try:
        apis = provider_factory()
    except Exception as exc:
        stop_event.set()
        return _DownloadWorkerResult((), 0, False, type(exc).__name__, None)
    try:
        for item in items:
            if stop_event.is_set():
                break
            remaining = MAXIMUM_RUNTIME_SECONDS - (clock() - started)
            if remaining <= 0:
                raise UnauthorizedOperation("v24 acquisition runtime ceiling reached")
            request_id = str(item["request_id"])
            failed_request_id = request_id
            partial = downloads / f"{request_id[:16]}.dbn.zst.partial"
            if partial.exists():
                raise IntegrityError("v24 staging destination already exists")
            _set_timeout(
                apis.get_range,
                min(float(MAXIMUM_PER_DOWNLOAD_SECONDS), float(remaining)),
            )
            calls += 1
            warning_collector.start_call()
            apis.get_range(**item["query"], path=str(partial))
            warning_count, warning_categories = warning_collector.finish_call()
            if not partial.is_file():
                raise IntegrityError("provider did not create the v24 staging file")
            size = partial.stat().st_size
            if size <= 0 or size > item["request_byte_ceiling"]:
                raise UnauthorizedOperation("v24 file is empty or exceeds its ceiling")
            digest = sha256_file(partial)
            with total_lock:
                proposed = total_state["bytes"] + size
                if proposed > maximum_total_bytes:
                    raise UnauthorizedOperation("v24 total byte ceiling exceeded")
                total_state["bytes"] = proposed
            sidecar_path = downloads / f"{request_id[:16]}.manifest.json.partial"
            sidecar_core = {
                "schema_version": "apex_micro_inactive_dbn_manifest/24.0.0",
                "state": "INACTIVE_CUSTODY_NOT_A_RESEARCH_SOURCE",
                "plan_id": plan_id,
                "request_id": request_id,
                "exact_authorized_query": {
                    **dict(item["query"]),
                    "encoding": item["wire_format"]["encoding"],
                    "compression": item["wire_format"]["compression"],
                },
                "wire_format_contract": item["wire_format"]["contract"],
                "metadata_estimated_cost_usd": item[
                    "metadata_estimated_cost_usd"
                ],
                "fresh_exact_cost_requote_usd": "0",
                "external_cost_incurred_usd": "0",
                "byte_count": size,
                "sha256": digest,
                "provider_warning_count": warning_count,
                "provider_warning_categories": warning_categories,
                "provider_warning_messages_recorded": False,
                "source_certification_required": True,
                "dbn_rows_decoded": 0,
                "payload_opened_for_row_access": False,
                "catalog_activation": False,
            }
            with sidecar_path.open("xb") as stream:
                stream.write(
                    canonical_bytes(
                        {**sidecar_core, "manifest_id": sha256_json(sidecar_core)}
                    )
                    + b"\n"
                )
            records.append(
                {
                    "request_id": request_id,
                    "staging_dbn": partial.relative_to(root).as_posix(),
                    "staging_sidecar": sidecar_path.relative_to(root).as_posix(),
                    "dbn_destination": item["dbn_destination"],
                    "sidecar_destination": item["sidecar_destination"],
                    "byte_count": size,
                    "sha256": digest,
                    "provider_warning_count": warning_count,
                    "provider_warning_categories": warning_categories,
                }
            )
            failed_request_id = None
    except Exception as exc:
        stop_event.set()
        return _DownloadWorkerResult(
            tuple(records), calls, True, type(exc).__name__, failed_request_id
        )
    return _DownloadWorkerResult(tuple(records), calls, True, None, None)


def execute_authorized_acquisition(
    *,
    root: Path,
    authorization: OperationReceipt,
    provider_factory: Callable[[], DownloadProviderApis],
    credential_source: str,
    clock: Callable[[], float] = time.monotonic,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
    mark_immutable: Callable[[Path], None] = _mark_read_only,
    link_file: Callable[[Path, Path], None] = os.link,
) -> dict[str, object]:
    """Execute v24 once without reusing any predecessor staging bytes."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_acquisition_plan(root=root)
    if credential_source != CREDENTIAL_SOURCE:
        raise UnauthorizedOperation("v24 credential source is not bound")
    environment_check(root)
    destinations = [
        root / str(item[key])
        for item in plan["requests"]
        for key in ("dbn_destination", "sidecar_destination")
    ]
    if any(path.exists() for path in destinations):
        raise IntegrityError("v24 create-only destination already exists")
    free = getattr(disk_usage(root), "free", None)
    if type(free) is not int or free < plan["limits"]["required_free_disk_bytes"]:
        raise UnauthorizedOperation("insufficient disk capacity for v24")
    claim = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    attempt = root / STAGING_ROOT / authorization.receipt_id[:16]
    boundary.assert_active_path(
        attempt.absolute(),
        purpose="Apex micro v24 acquisition staging",
        subtree=STAGING_ROOT.as_posix(),
    )
    attempt.mkdir(parents=True, exist_ok=False)
    terminal_path = attempt / "terminal.json"
    started = clock()
    exact_count = len(plan["requests"])
    provider_calls = {"get_cost": 0, "get_range": 0}
    provider_client_count = 0
    staged: list[dict[str, object]] = []
    finalized: list[dict[str, object]] = []
    finalization_attempts: list[dict[str, object]] = []
    finalization_rollback_failures: list[dict[str, object]] = []
    staging_cleanup_failures: list[dict[str, object]] = []
    worker_failures: list[dict[str, object]] = []
    downloads: Path | None = None
    failure_stage = "PROVIDER_FACTORY"
    failure_substage: str | None = None
    base: dict[str, object] = {
        "schema_version": "apex_micro_phase1a_acquisition_terminal/24.0.0",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "predecessor_failure_report_id": V21_FAILURE_REPORT_ID,
        "superseded_v22_preparation_report_id": V22_SUPERSESSION_ID,
        "predecessor_staging_reused": False,
        "credential_source": CREDENTIAL_SOURCE,
        "credential_content_recorded": False,
        "maximum_external_cost_usd": "0",
        "external_cost_incurred_usd": "0",
        "automatic_retries": 0,
        "maximum_parallel_downloads": 2,
        "maximum_provider_clients": 3,
        "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
        "maximum_per_download_seconds": MAXIMUM_PER_DOWNLOAD_SECONDS,
        "dbn_rows_decoded": 0,
        "payloads_opened_for_row_access": 0,
        "year_2025_or_2026_payloads_opened": 0,
        "raw_values_reported": False,
        "catalog_or_pointer_activated": False,
        "published": False,
        "registered": False,
        "model_fit_prediction_or_evaluation": False,
        "trading": False,
    }
    try:
        apis = provider_factory()
        provider_client_count = 1
        failure_stage = "FRESH_EXACT_ZERO_COST_CENSUS"
        for item in plan["requests"]:
            remaining = MAXIMUM_RUNTIME_SECONDS - (clock() - started)
            if remaining <= 0:
                raise UnauthorizedOperation("v24 runtime exhausted before download")
            _set_timeout(apis.get_cost, min(90.0, float(remaining)))
            provider_calls["get_cost"] += 1
            _zero_cost(apis.get_cost(**_metadata_query(item["query"])))
        downloads = attempt / "downloads"
        downloads.mkdir()
        failure_stage = "DOWNLOAD_TO_INACTIVE_STAGING"
        worker_count = min(MAXIMUM_PARALLEL_DOWNLOADS, exact_count)
        queues = tuple(
            tuple(plan["requests"][index::worker_count])
            for index in range(worker_count)
        )
        stop_event = threading.Event()
        total_state = {"bytes": 0}
        total_lock = threading.Lock()
        with _ConcurrentWarningCollector() as warning_collector:
            with ThreadPoolExecutor(
                max_workers=worker_count,
                thread_name_prefix="apex-micro-v24-dbn",
            ) as executor:
                futures = [
                    executor.submit(
                        _download_worker,
                        root=root,
                        downloads=downloads,
                        plan_id=str(plan["plan_id"]),
                        items=queue,
                        provider_factory=provider_factory,
                        stop_event=stop_event,
                        total_state=total_state,
                        total_lock=total_lock,
                        maximum_total_bytes=int(
                            plan["limits"]["maximum_total_bytes"]
                        ),
                        started=started,
                        clock=clock,
                        warning_collector=warning_collector,
                    )
                    for queue in queues
                ]
                results = [future.result() for future in futures]
        provider_calls["get_range"] = sum(result.get_range_calls for result in results)
        provider_client_count += sum(
            int(result.provider_client_created) for result in results
        )
        staged = [record for result in results for record in result.records]
        request_order = {
            str(item["request_id"]): index
            for index, item in enumerate(plan["requests"])
        }
        staged.sort(key=lambda item: request_order[str(item["request_id"])])
        worker_failures = [
            {
                "worker_index": index,
                "exception_type": result.failure_type,
                "failed_request_id": result.failed_request_id,
            }
            for index, result in enumerate(results)
            if result.failure_type is not None
        ]
        if worker_failures:
            raise IntegrityError("v24 bounded download worker failed")
        if (
            provider_calls != {"get_cost": exact_count, "get_range": exact_count}
            or len(staged) != exact_count
            or provider_client_count != 3
        ):
            raise IntegrityError("v24 successful call or staging count drifted")
        failure_stage = "FINAL_DESTINATION_RECHECK"
        if any(path.exists() for path in destinations):
            raise IntegrityError("v24 destination appeared before finalization")
        failure_stage = "CREATE_ONLY_FINALIZATION"
        for item in staged:
            failure_substage = "CREATE_FINAL_PARENT"
            source_dbn = root / str(item["staging_dbn"])
            source_sidecar = root / str(item["staging_sidecar"])
            final_dbn = root / str(item["dbn_destination"])
            final_sidecar = root / str(item["sidecar_destination"])
            final_dbn.parent.mkdir(parents=True, exist_ok=True)
            record = {
                "request_id": item["request_id"],
                "dbn_destination": item["dbn_destination"],
                "sidecar_destination": item["sidecar_destination"],
                "dbn_link_created": False,
                "sidecar_link_created": False,
                "staging_sources_removed": False,
                "hash_reverified": False,
                "marked_immutable": False,
                "rollback_dbn_removed": False,
                "rollback_sidecar_removed": False,
            }
            finalization_attempts.append(record)
            failure_substage = "CREATE_DBN_LINK"
            link_file(source_dbn, final_dbn)
            record["dbn_link_created"] = True
            failure_substage = "CREATE_SIDECAR_LINK"
            link_file(source_sidecar, final_sidecar)
            record["sidecar_link_created"] = True
            failure_substage = "REVERIFY_FINAL_DBN"
            if (
                final_dbn.stat().st_size != item["byte_count"]
                or sha256_file(final_dbn, reject_hardlinks=False) != item["sha256"]
            ):
                raise IntegrityError("v24 finalized DBN hash differs")
            failure_substage = "REVERIFY_FINAL_SIDECAR"
            sidecar = _object(final_sidecar, "v24 final sidecar")
            if not _self_hashed(sidecar, "manifest_id"):
                raise IntegrityError("v24 finalized sidecar identity differs")
            record["hash_reverified"] = True
            failure_substage = "MARK_FINAL_PAIR_IMMUTABLE"
            mark_immutable(final_dbn)
            mark_immutable(final_sidecar)
            record["marked_immutable"] = True
            finalized.append(
                {
                    "request_id": item["request_id"],
                    "dbn_destination": item["dbn_destination"],
                    "sidecar_destination": item["sidecar_destination"],
                    "byte_count": item["byte_count"],
                    "sha256": item["sha256"],
                    "provider_warning_count": item["provider_warning_count"],
                    "provider_warning_categories": item[
                        "provider_warning_categories"
                    ],
                }
            )
        for item, record in zip(staged, finalization_attempts, strict=True):
            failure_substage = "REMOVE_VERIFIED_STAGING_LINKS"
            source_dbn = root / str(item["staging_dbn"])
            source_sidecar = root / str(item["staging_sidecar"])
            for kind, source in (("dbn", source_dbn), ("sidecar", source_sidecar)):
                try:
                    source.unlink()
                except OSError as cleanup_exc:
                    staging_cleanup_failures.append(
                        {
                            "request_id": record["request_id"],
                            "kind": kind,
                            "exception_type": type(cleanup_exc).__name__,
                        }
                    )
            record["staging_sources_removed"] = not (
                source_dbn.exists() or source_sidecar.exists()
            )
        if staging_cleanup_failures:
            raise IntegrityError("v24 staging hard-link cleanup failed")
        core = {
            **base,
            "state": "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY",
            "provider_call_counts": provider_calls,
            "provider_client_count": provider_client_count,
            "download_worker_count": worker_count,
            "accepted_dbn_count": exact_count,
            "accepted_sidecar_count": exact_count,
            "total_bytes": sum(int(item["byte_count"]) for item in staged),
            "provider_warning_count": sum(
                int(item["provider_warning_count"]) for item in staged
            ),
            "staging_cleanup_failures": staging_cleanup_failures,
            "accepted_files": finalized,
            "prelaunch_coverage": plan["prelaunch_coverage"],
            "terminal_written_last": True,
        }
    except Exception as exc:
        for record in reversed(finalization_attempts):
            for kind in ("sidecar", "dbn"):
                created_key = f"{kind}_link_created"
                removed_key = f"rollback_{kind}_removed"
                if not record[created_key] or record["staging_sources_removed"]:
                    continue
                destination = root / str(record[f"{kind}_destination"])
                try:
                    if destination.exists():
                        destination.chmod(stat.S_IREAD | stat.S_IWRITE)
                        destination.unlink()
                    record[removed_key] = True
                except OSError as rollback_exc:
                    finalization_rollback_failures.append(
                        {
                            "request_id": record["request_id"],
                            "kind": kind,
                            "exception_type": type(rollback_exc).__name__,
                        }
                    )
        if downloads is not None and downloads.exists():
            for path in downloads.iterdir():
                if path.is_file():
                    _mark_read_only(path)
        core = {
            **base,
            "state": "FAILURE_INACTIVE_EVIDENCE_PRESERVED",
            "failure_code": "ACQUISITION_FAIL_CLOSED",
            "failure_stage": failure_stage,
            "failure_substage": failure_substage,
            "exception_type": type(exc).__name__,
            "provider_call_counts": provider_calls,
            "provider_client_count": provider_client_count,
            "download_worker_failures": worker_failures,
            "accepted_dbn_count": 0,
            "accepted_sidecar_count": 0,
            "staged_complete_pairs": staged,
            "completed_finalized_pairs": finalized,
            "finalization_attempts": finalization_attempts,
            "finalization_rollback_failures": finalization_rollback_failures,
            "staging_cleanup_failures": staging_cleanup_failures,
            "partial_final_destinations_preserved": bool(
                finalization_rollback_failures
            ),
            "staging_file_census": (
                sorted(
                    path.relative_to(root).as_posix()
                    for path in downloads.iterdir()
                    if path.is_file()
                )
                if downloads is not None and downloads.exists()
                else []
            ),
            "terminal_written_last": True,
        }
    return _write_terminal(terminal_path, core)


def verify_completed_acquisition(
    *, root: Path, terminal_path: Path
) -> dict[str, object]:
    root = root.resolve(strict=True)
    terminal_abs = terminal_path if terminal_path.is_absolute() else root / terminal_path
    terminal = _object(terminal_abs, "v24 acquisition terminal")
    plan = load_acquisition_plan(root=root, require_destination_absence=False)
    exact_count = len(plan["requests"])
    if (
        not _self_hashed(terminal, "terminal_id")
        or terminal.get("state") != "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY"
        or terminal.get("plan_id") != plan["plan_id"]
        or terminal.get("plan_sha256") != sha256_file(root / PLAN_PATH)
        or terminal.get("provider_call_counts")
        != {"get_cost": exact_count, "get_range": exact_count}
        or terminal.get("provider_client_count") != 3
        or terminal.get("download_worker_count") != 2
        or terminal.get("accepted_dbn_count") != exact_count
        or terminal.get("accepted_sidecar_count") != exact_count
        or terminal.get("external_cost_incurred_usd") != "0"
        or terminal.get("automatic_retries") != 0
        or terminal.get("credential_content_recorded") is not False
        or terminal.get("dbn_rows_decoded") != 0
        or terminal.get("payloads_opened_for_row_access") != 0
        or terminal.get("year_2025_or_2026_payloads_opened") != 0
        or terminal.get("catalog_or_pointer_activated") is not False
    ):
        raise IntegrityError("v24 terminal safety or count drifted")
    records = terminal.get("accepted_files")
    if not isinstance(records, list) or len(records) != exact_count:
        raise IntegrityError("v24 accepted-file census drifted")
    total = 0
    for record in records:
        dbn = root / str(record["dbn_destination"])
        sidecar_path = root / str(record["sidecar_destination"])
        if (
            not dbn.is_file()
            or not sidecar_path.is_file()
            or dbn.stat().st_size != record["byte_count"]
            or sha256_file(dbn) != record["sha256"]
        ):
            raise IntegrityError("v24 accepted DBN differs")
        sidecar = _object(sidecar_path, "v24 accepted sidecar")
        if (
            not _self_hashed(sidecar, "manifest_id")
            or sidecar.get("state") != "INACTIVE_CUSTODY_NOT_A_RESEARCH_SOURCE"
            or sidecar.get("plan_id") != plan["plan_id"]
            or sidecar.get("request_id") != record["request_id"]
            or sidecar.get("byte_count") != record["byte_count"]
            or sidecar.get("sha256") != record["sha256"]
            or sidecar.get("provider_warning_messages_recorded") is not False
            or sidecar.get("source_certification_required") is not True
            or sidecar.get("dbn_rows_decoded") != 0
            or sidecar.get("payload_opened_for_row_access") is not False
        ):
            raise IntegrityError("v24 accepted sidecar differs")
        total += int(record["byte_count"])
    if (
        total != terminal.get("total_bytes")
        or total > plan["limits"]["maximum_total_bytes"]
        or (root / "configs/active_micro_alpha_research_ladder.json").exists()
        or (root / "data/active/catalogs/apex_micro.json").exists()
    ):
        raise IntegrityError("v24 inactive custody reconciliation is incomplete")
    return {
        "status": "PASS_INACTIVE_CUSTODY_NO_ROW_DECODE",
        "terminal_id": terminal["terminal_id"],
        "dbn_count": exact_count,
        "sidecar_count": exact_count,
        "total_bytes": total,
        "provider_warning_count": terminal["provider_warning_count"],
    }


def build_plan_audit(
    *,
    root: Path,
    fresh_standard_topology_report: Mapping[str, object],
    fresh_cleanup_census: Mapping[str, object],
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
) -> dict[str, object]:
    root = root.resolve(strict=True)
    plan = load_acquisition_plan(root=root)
    topology = _object(root / STANDARD_TOPOLOGY_PATH, "standard topology report")
    cleanup = _object(root / CLEANUP_CENSUS_PATH, "v9 cleanup census")
    if dict(fresh_standard_topology_report) != topology:
        raise IntegrityError("fresh standard topology reconstruction drifted")
    if dict(fresh_cleanup_census) != cleanup:
        raise IntegrityError("fresh v9 cleanup census drifted")
    if (
        not _self_hashed(topology, "report_id")
        or topology.get("state") != "PASS_SOURCE_SAFE_PROVENANCE_METADATA_ONLY"
        or topology.get("payload_safety", {}).get("historical_rows_read") != 0
        or not _self_hashed(cleanup, "census_id")
        or cleanup.get("state")
        != "PREPARED_NO_MUTATION_SEPARATE_EXACT_CLEANUP_APPROVAL_REQUIRED"
        or cleanup.get("committed_head") != _git_head(root)
        or cleanup.get("cleanup_execution", {}).get("performed") is not False
        or cleanup.get("payload_safety", {}).get("historical_rows_read") is not False
    ):
        raise IntegrityError("v24 topology or cleanup evidence drifted")
    free = getattr(disk_usage(root), "free", None)
    required = plan["limits"]["required_free_disk_bytes"]
    if type(free) is not int or free < required:
        raise UnauthorizedOperation("insufficient disk for v24 plan")
    destinations = [
        str(item[key])
        for item in plan["requests"]
        for key in ("dbn_destination", "sidecar_destination")
    ]
    if any((root / path).exists() for path in destinations):
        raise UnauthorizedOperation("v24 destination conflict appeared")
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1a_acquisition_plan_audit/24.0.0",
        "state": "PASS_EXACT_DOWNLOAD_APPROVAL_PREPARATION_ONLY",
        "observed_head": _git_head(root),
        "plan_path": PLAN_PATH.as_posix(),
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "predecessor_failure_report_id": V21_FAILURE_REPORT_ID,
        "predecessor_failure_report_sha256": V21_FAILURE_REPORT_SHA256,
        "superseded_v22_preparation_report_id": V22_SUPERSESSION_ID,
        "superseded_v22_preparation_report_sha256": V22_SUPERSESSION_SHA256,
        "superseded_v23_preparation_report_id": V23_SUPERSESSION_ID,
        "superseded_v23_preparation_report_sha256": V23_SUPERSESSION_SHA256,
        "predecessor_staging_reuse": False,
        "standard_topology": {
            "path": STANDARD_TOPOLOGY_PATH.as_posix(),
            "report_id": topology["report_id"],
            "sha256": sha256_file(root / STANDARD_TOPOLOGY_PATH),
            "fresh_reconstruction_match": True,
            "payload_rows_read": 0,
        },
        "cleanup_governance": {
            "path": CLEANUP_CENSUS_PATH.as_posix(),
            "census_id": cleanup["census_id"],
            "sha256": sha256_file(root / CLEANUP_CENSUS_PATH),
            "candidate_count": cleanup["candidate_count"],
            "cleanup_performed": False,
            "separate_exact_cleanup_approval_required": True,
        },
        "scope": {
            "markets": list(CURRENT_ACQUISITION_MARKETS),
            "schemas": list(SCHEMAS),
            "exact_requests": 160,
            "dbn_files": 160,
            "adjacent_sidecars": 160,
            "destination_paths": len(destinations),
            "destination_conflicts": 0,
            "higher_tier_markets": [],
            "forbidden_schemas": [],
        },
        "capacity": {
            "maximum_total_bytes": plan["limits"]["maximum_total_bytes"],
            "required_free_disk_bytes": required,
            "observed_free_disk_bytes_recorded": False,
            "live_capacity_checked_at_creation": True,
            "live_capacity_recheck_required_immediately_before_execution": True,
            "fits_disk": True,
        },
        "execution": {
            "maximum_provider_calls": 320,
            "maximum_parallel_downloads": 2,
            "maximum_provider_clients": 3,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_per_download_seconds": MAXIMUM_PER_DOWNLOAD_SECONDS,
            "maximum_external_cost_usd": "0",
            "maximum_attempts": 1,
            "maximum_retries": 0,
        },
        "safety": {
            "dbn_download_performed": False,
            "historical_rows_read": False,
            "year_2025_or_2026_payload_opened": False,
            "catalog_or_pointer_activated": False,
            "predecessor_bytes_reused": False,
            "provider_warning_messages_recorded": False,
            "publication_registration_evaluation_or_trading": False,
            "cleanup_mutation_performed": False,
            "deterministic_reconstruction": True,
        },
    }
    return {**core, "audit_id": sha256_json(core)}


def write_plan_audit_create_only(
    *,
    root: Path,
    fresh_standard_topology_report: Mapping[str, object],
    fresh_cleanup_census: Mapping[str, object],
) -> dict[str, object]:
    audit = build_plan_audit(
        root=root,
        fresh_standard_topology_report=fresh_standard_topology_report,
        fresh_cleanup_census=fresh_cleanup_census,
    )
    path = root / AUDIT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(audit) + b"\n")
    return audit


__all__ = [
    "AUDIT_PATH",
    "CLEANUP_CENSUS_PATH",
    "CREDENTIAL_SOURCE",
    "DownloadProviderApis",
    "MAXIMUM_RUNTIME_SECONDS",
    "OPERATION",
    "PLAN_PATH",
    "STAGING_ROOT",
    "build_acquisition_plan",
    "build_file_download_provider_apis",
    "build_plan_audit",
    "execute_authorized_acquisition",
    "load_acquisition_plan",
    "required_scope",
    "verify_completed_acquisition",
    "write_acquisition_plan_create_only",
    "write_plan_audit_create_only",
]
