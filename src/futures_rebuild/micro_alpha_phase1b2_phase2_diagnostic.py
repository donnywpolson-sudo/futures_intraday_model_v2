"""Bounded one-interval diagnostic for the v3 Phase 2 transition failure."""

from __future__ import annotations

import json
import shutil
import stat
import subprocess
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Final

import pyarrow.parquet as pq

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, contained_path, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .micro_alpha_phase1b2_decoder import (
    BAR_SCHEMA,
    CreatedByteBudget,
    materialize_causal_1m_inactive,
)


OPERATION: Final = "DIAGNOSE_APEX_MICRO_PHASE2_FIRST_INTERVAL_V1_ONCE"
PLAN_PATH: Final = Path("configs/apex_micro_phase1b2_phase2_diagnostic_plan_v1.json")
AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_diagnostic_plan_v1/audit.json"
)
FAILURE_REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v3_supersession/report.json"
)
STAGING_ROOT: Final = Path("state/data_publication_staging/apex_micro_phase2_diagnostic_v1")
EVIDENCE_ROOT: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_diagnostic_v1"
)
PLAN_SCHEMA: Final = "apex_micro_phase1b2_phase2_diagnostic_plan/1.0.0"
AUDIT_SCHEMA: Final = "apex_micro_phase1b2_phase2_diagnostic_audit/1.0.0"
REPORT_SCHEMA: Final = "apex_micro_phase1b2_phase2_diagnostic_report/1.0.0"
TERMINAL_SCHEMA: Final = "apex_micro_phase1b2_phase2_diagnostic_terminal/1.0.0"
MAXIMUM_SOURCE_COUNT: Final = 1
MAXIMUM_WORKERS: Final = 1
MAXIMUM_RUNTIME_SECONDS: Final = 300
MAXIMUM_OUTPUT_BYTES: Final = 1024**3
REQUIRED_FREE_DISK_BYTES: Final = 2 * 1024**3
MAXIMUM_ATTEMPTS: Final = 1
MAXIMUM_RETRIES: Final = 0

IMPLEMENTATION_PATHS: Final = (
    Path("src/futures_rebuild/micro_alpha_phase1b2_phase2_diagnostic.py"),
    Path("src/futures_rebuild/micro_alpha_phase1b2_decoder.py"),
    Path("src/futures_rebuild/boundary.py"),
    Path("src/futures_rebuild/research_gateway_policy.py"),
    Path("src/futures_rebuild/canonical.py"),
    Path("configs/dependency_lock_receipt.json"),
)


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if type(value) is not dict:
        raise IntegrityError(f"{description} is not an object")
    return value


def _self_hash(value: Mapping[str, object], key: str, description: str) -> None:
    core = dict(value)
    observed = core.pop(key, None)
    if type(observed) is not str or observed != sha256_json(core):
        raise IntegrityError(f"{description} identity drifted")


def _git_head(root: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
    ).strip()


def _write_create_only(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(value) + b"\n")
    path.chmod(stat.S_IREAD)


def _first_source(failure: Mapping[str, object]) -> Mapping[str, object]:
    inventory = failure.get("phase1b_inventory")
    if not isinstance(inventory, list) or len(inventory) != 120:
        raise IntegrityError("v3 Phase 1B inventory is absent")
    matches = [
        item
        for item in inventory
        if isinstance(item, Mapping)
        and item.get("market") == "M6E"
        and item.get("schema") == "ohlcv-1m"
        and item.get("year") == 2018
    ]
    if len(matches) != 1:
        raise IntegrityError("diagnostic source selector is not exact")
    return matches[0]


def build_plan(*, root: Path, implementation_head: str) -> dict[str, object]:
    """Build a footer-only plan without opening a Parquet row batch."""

    root = root.resolve(strict=True)
    if implementation_head != _git_head(root):
        raise IntegrityError("diagnostic plan must bind the live committed HEAD")
    failure = _object(root / FAILURE_REPORT_PATH, "v3 failure report")
    _self_hash(failure, "report_id", "v3 failure report")
    if failure.get("state") != "SUPERSEDED_PHASE1B_COMPLETE_PHASE2_TRANSITION_FAILED_CLOSED":
        raise IntegrityError("v3 failure state is not diagnostic-eligible")
    source = dict(_first_source(failure))
    source_path = contained_path(root, str(source["relative_path"]))
    if (
        source.get("market") != "M6E"
        or source.get("schema") != "ohlcv-1m"
        or source.get("year") != 2018
        or sha256_file(source_path) != source.get("sha256")
        or source_path.stat().st_size != source.get("bytes")
    ):
        raise IntegrityError("diagnostic source binding drifted")
    parquet = pq.ParquetFile(source_path)
    metadata = parquet.schema_arrow.metadata or {}
    if (
        parquet.schema_arrow.names != BAR_SCHEMA.names
        or metadata.get(b"schema_id") != b"APEX_MICRO_PHASE1B_REPORTED_BAR_V1"
        or metadata.get(b"source_schema") != b"ohlcv-1m"
        or metadata.get(b"source_file_sha256") != str(source["source_sha256"]).encode("ascii")
    ):
        raise IntegrityError("diagnostic Parquet footer binding drifted")
    implementation_hashes = {
        path.as_posix(): sha256_file(root / path) for path in IMPLEMENTATION_PATHS
    }
    scope_id = sha256_json(
        {
            "implementation_head": implementation_head,
            "source_sha256": source["sha256"],
            "source_request_id": source["request_id"],
            "failure_report_id": failure["report_id"],
        }
    )
    scope_path_id = scope_id[:24]
    staging_root = (STAGING_ROOT / scope_path_id).as_posix()
    evidence_root = (EVIDENCE_ROOT / scope_path_id).as_posix()
    core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "state": "PREPARED_REQUIRES_SEPARATE_DERIVED_ROW_DIAGNOSTIC_APPROVAL",
        "operation": OPERATION,
        "implementation_head": implementation_head,
        "implementation_hashes": implementation_hashes,
        "failure_report_id": failure["report_id"],
        "failure_report_sha256": sha256_file(root / FAILURE_REPORT_PATH),
        "scope_id": scope_id,
        "scope_path_id": scope_path_id,
        "source": source,
        "source_footer": {
            "row_count": parquet.metadata.num_rows,
            "row_group_count": parquet.num_row_groups,
            "schema_names": parquet.schema_arrow.names,
            "schema_id": "APEX_MICRO_PHASE1B_REPORTED_BAR_V1",
            "source_schema": "ohlcv-1m",
            "footer_only_plan_read": True,
            "row_batches_opened": 0,
        },
        "limits": {
            "maximum_source_count": MAXIMUM_SOURCE_COUNT,
            "maximum_workers": MAXIMUM_WORKERS,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
            "required_free_disk_bytes": REQUIRED_FREE_DISK_BYTES,
            "maximum_attempts": MAXIMUM_ATTEMPTS,
            "maximum_retries": MAXIMUM_RETRIES,
            "provider_calls": 0,
            "external_cost_usd": "0",
        },
        "staging_root": staging_root,
        "evidence_root": evidence_root,
        "diagnostic_output": f"{staging_root}/causal_first_interval.parquet",
        "report_path": f"{evidence_root}/report.json",
        "terminal_path": f"{evidence_root}/terminal.json",
        "forbidden": {
            "dbn_open": True,
            "second_parquet_source_open": True,
            "year_2025_or_2026_payload_open": True,
            "provider_or_network_access": True,
            "credential_access": True,
            "raw_values_in_report": True,
            "publication_activation_registration_evaluation_or_trading": True,
            "git_staging_commit_or_push": True,
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def write_plan_create_only(*, root: Path, implementation_head: str) -> dict[str, object]:
    plan = build_plan(root=root, implementation_head=implementation_head)
    _write_create_only(root / PLAN_PATH, plan)
    return plan


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH, "Phase 2 diagnostic plan")
    _self_hash(plan, "plan_id", "Phase 2 diagnostic plan")
    if plan.get("state") != "PREPARED_REQUIRES_SEPARATE_DERIVED_ROW_DIAGNOSTIC_APPROVAL":
        raise IntegrityError("Phase 2 diagnostic plan state is invalid")
    return plan


def build_audit(*, root: Path) -> dict[str, object]:
    plan = load_plan(root=root)
    if build_plan(root=root, implementation_head=str(plan["implementation_head"])) != plan:
        raise IntegrityError("Phase 2 diagnostic plan reconstruction differs")
    if (root / str(plan["staging_root"])).exists() or (root / str(plan["evidence_root"])).exists():
        raise IntegrityError("Phase 2 diagnostic create-only output exists")
    core = {
        "schema_version": AUDIT_SCHEMA,
        "state": "PASS_SOURCE_SAFE_PHASE2_DIAGNOSTIC_AUDIT",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "source_count": 1,
        "source_market": "M6E",
        "source_schema": "ohlcv-1m",
        "source_year": 2018,
        "parquet_row_batches_opened": 0,
        "dbn_payloads_opened": 0,
        "provider_calls": 0,
        "external_cost_usd": "0",
        "deterministic_reconstruction": True,
    }
    return {**core, "audit_id": sha256_json(core)}


def write_audit_create_only(*, root: Path) -> dict[str, object]:
    audit = build_audit(root=root)
    _write_create_only(root / AUDIT_PATH, audit)
    return audit


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    source = plan["source"]
    limits = plan["limits"]
    return {
        "source_market": str(source["market"]),
        "source_schema": str(source["schema"]),
        "source_year": str(source["year"]),
        "source_sha256": str(source["sha256"]),
        "maximum_source_count": str(MAXIMUM_SOURCE_COUNT),
        "maximum_workers": str(MAXIMUM_WORKERS),
        "maximum_runtime_seconds": str(MAXIMUM_RUNTIME_SECONDS),
        "maximum_output_bytes": str(MAXIMUM_OUTPUT_BYTES),
        "required_free_disk_bytes": str(REQUIRED_FREE_DISK_BYTES),
        "maximum_attempts": str(MAXIMUM_ATTEMPTS),
        "maximum_retries": str(MAXIMUM_RETRIES),
        "provider_calls": "0",
        "external_cost_usd": "0",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _seal_tree(root: Path) -> None:
    if not root.exists():
        return
    for path in root.rglob("*"):
        if path.is_file():
            path.chmod(stat.S_IREAD)


def execute_once(
    *, root: Path, authorization: OperationReceipt, clock=time.monotonic,
    disk_usage=shutil.disk_usage,
) -> dict[str, object]:
    """Read one exact derived Parquet and report only price-free diagnostics."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    if _git_head(root) != plan["implementation_head"]:
        raise IntegrityError("Phase 2 diagnostic HEAD drifted")
    if build_plan(root=root, implementation_head=str(plan["implementation_head"])) != plan:
        raise IntegrityError("Phase 2 diagnostic plan drifted")
    if build_audit(root=root) != _object(root / AUDIT_PATH, "Phase 2 diagnostic audit"):
        raise IntegrityError("Phase 2 diagnostic audit drifted")
    staging = contained_path(root, str(plan["staging_root"]))
    evidence = contained_path(root, str(plan["evidence_root"]))
    if staging.exists() or evidence.exists():
        raise IntegrityError("Phase 2 diagnostic create-only output exists")
    free = getattr(disk_usage(root), "free", None)
    if type(free) is not int or free < REQUIRED_FREE_DISK_BYTES:
        raise UnauthorizedOperation("insufficient disk for Phase 2 diagnostic")
    scope = required_scope(root=root, plan=plan)
    authorization.verify(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    staging.mkdir(parents=True, exist_ok=False)
    evidence.mkdir(parents=True, exist_ok=False)
    use_path = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    started = clock()
    source = contained_path(root, str(plan["source"]["relative_path"]))
    output = contained_path(root, str(plan["diagnostic_output"]))
    failure_type: str | None = None
    result_record: dict[str, object] | None = None
    try:
        result = materialize_causal_1m_inactive(
            source_path=source,
            output_path=output,
            identity_certified=True,
            created_byte_budget=CreatedByteBudget(MAXIMUM_OUTPUT_BYTES),
            deadline=started + MAXIMUM_RUNTIME_SECONDS,
            clock=clock,
        )
        result_record = result.public_record()
        result_record["output_path"] = output.relative_to(root).as_posix()
        state = "PASS_FIRST_INTERVAL_PHASE2_MATERIALIZATION"
    except Exception as exc:
        failure_type = type(exc).__name__
        state = "FAIL_CLOSED_FIRST_INTERVAL_PHASE2_MATERIALIZATION"
    report_core = {
        "schema_version": REPORT_SCHEMA,
        "state": state,
        "plan_id": plan["plan_id"],
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "diagnostic_stage": "MATERIALIZE_CAUSAL_1M_INACTIVE",
        "source_market": "M6E",
        "source_schema": "ohlcv-1m",
        "source_year": 2018,
        "source_sha256": plan["source"]["sha256"],
        "result": result_record,
        "failure_type": failure_type,
        "attempts": 1,
        "automatic_retries": 0,
        "provider_calls": 0,
        "external_cost_usd": "0",
        "dbn_payloads_opened": 0,
        "year_2025_or_2026_payloads_opened": 0,
        "raw_values_reported": False,
        "published_activated_registered_evaluated_or_traded": False,
    }
    report = {**report_core, "report_id": sha256_json(report_core)}
    report_path = contained_path(root, str(plan["report_path"]))
    _write_create_only(report_path, report)
    terminal_core = {
        "schema_version": TERMINAL_SCHEMA,
        "state": state,
        "plan_id": plan["plan_id"],
        "report_id": report["report_id"],
        "report_sha256": sha256_file(report_path),
        "authorization_receipt_id": authorization.receipt_id,
        "attempts": 1,
        "automatic_retries": 0,
        "provider_calls": 0,
        "external_cost_usd": "0",
        "terminal_written_last": True,
    }
    terminal = {**terminal_core, "terminal_id": sha256_json(terminal_core)}
    _seal_tree(staging)
    terminal_path = contained_path(root, str(plan["terminal_path"]))
    _write_create_only(terminal_path, terminal)
    return terminal


__all__ = [
    "AUDIT_PATH",
    "OPERATION",
    "PLAN_PATH",
    "build_audit",
    "build_plan",
    "execute_once",
    "load_plan",
    "required_scope",
    "write_audit_create_only",
    "write_plan_create_only",
]
