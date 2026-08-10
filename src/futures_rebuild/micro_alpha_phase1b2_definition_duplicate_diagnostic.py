"""Bounded classifier for legacy definition-key repeats in the first micro group."""

from __future__ import annotations

import json
import shutil
import stat
import subprocess
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Final

import pyarrow as pa
import pyarrow.parquet as pq

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, contained_path, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .micro_alpha_phase1b2_decoder import DEFINITION_SCHEMA


OPERATION: Final = "DIAGNOSE_APEX_MICRO_DEFINITION_DUPLICATE_SEMANTICS_V3_ONCE"
PLAN_PATH: Final = Path(
    "configs/apex_micro_phase1b2_definition_duplicate_diagnostic_plan_v3.json"
)
AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_definition_duplicate_diagnostic_plan_v3/audit.json"
)
GROUP_PLAN_PATH: Final = Path(
    "configs/apex_micro_phase1b2_phase2_group_diagnostic_plan_v2.json"
)
GROUP_AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_phase2_group_diagnostic_plan_v2/audit.json"
)
GROUP_REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_group_diagnostic_v2/"
    "57515d8f88bca13ec9c9cab3/report.json"
)
GROUP_TERMINAL_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_group_diagnostic_v2/"
    "57515d8f88bca13ec9c9cab3/terminal.json"
)
EVIDENCE_ROOT: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_definition_duplicate_diagnostic_v3"
)
PLAN_SCHEMA: Final = "apex_micro_phase1b2_definition_duplicate_diagnostic_plan/3.0.0"
AUDIT_SCHEMA: Final = "apex_micro_phase1b2_definition_duplicate_diagnostic_audit/3.0.0"
REPORT_SCHEMA: Final = "apex_micro_phase1b2_definition_duplicate_diagnostic_report/3.0.0"
TERMINAL_SCHEMA: Final = "apex_micro_phase1b2_definition_duplicate_diagnostic_terminal/3.0.0"
MAXIMUM_SOURCE_COUNT: Final = 1
MAXIMUM_WORKERS: Final = 1
MAXIMUM_BATCH_ROWS: Final = 100_000
MAXIMUM_RUNTIME_SECONDS: Final = 300
MAXIMUM_OUTPUT_BYTES: Final = 4 * 1024**2
REQUIRED_FREE_DISK_BYTES: Final = 512 * 1024**2
MAXIMUM_ATTEMPTS: Final = 1
MAXIMUM_RETRIES: Final = 0
MARKET: Final = "M6E"
YEAR: Final = 2018
SCHEMA: Final = "definition"
LEGACY_REPEAT_COUNT: Final = 308
_LINEAGE_COLUMNS: Final = {"source_file_sha256", "row_ordinal", "row_sha256"}

IMPLEMENTATION_PATHS: Final = (
    Path("src/futures_rebuild/micro_alpha_phase1b2_definition_duplicate_diagnostic.py"),
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
    if path.stat().st_size > MAXIMUM_OUTPUT_BYTES:
        raise UnauthorizedOperation("definition diagnostic evidence byte ceiling exceeded")
    path.chmod(stat.S_IREAD)


def _definition_source(group_plan: Mapping[str, object]) -> dict[str, object]:
    sources = group_plan.get("sources")
    if not isinstance(sources, list) or len(sources) != 5:
        raise IntegrityError("five-schema predecessor source set is absent")
    selected = [
        item
        for item in sources
        if isinstance(item, Mapping)
        and item.get("market") == MARKET
        and item.get("year") == YEAR
        and item.get("schema") == SCHEMA
    ]
    if len(selected) != 1:
        raise IntegrityError("definition duplicate source selector is not exact")
    return dict(selected[0])


def build_plan(*, root: Path, implementation_head: str) -> dict[str, object]:
    """Build a stat-only plan without hashing or opening the definition Parquet."""

    root = root.resolve(strict=True)
    if implementation_head != _git_head(root):
        raise IntegrityError("definition diagnostic plan must bind the committed HEAD")
    group_plan = _object(root / GROUP_PLAN_PATH, "group diagnostic plan")
    _self_hash(group_plan, "plan_id", "group diagnostic plan")
    group_audit = _object(root / GROUP_AUDIT_PATH, "group diagnostic audit")
    _self_hash(group_audit, "audit_id", "group diagnostic audit")
    group_report = _object(root / GROUP_REPORT_PATH, "group diagnostic report")
    _self_hash(group_report, "report_id", "group diagnostic report")
    group_terminal = _object(root / GROUP_TERMINAL_PATH, "group diagnostic terminal")
    _self_hash(group_terminal, "terminal_id", "group diagnostic terminal")
    if (
        group_report.get("state") != "PASS_FIRST_GROUP_TRANSITION_DIAGNOSTIC"
        or group_terminal.get("state") != "PASS_FIRST_GROUP_TRANSITION_DIAGNOSTIC"
        or group_terminal.get("report_id") != group_report.get("report_id")
        or group_audit.get("plan_id") != group_plan.get("plan_id")
        or group_report.get("group_disposition") != "DUPLICATE"
        or group_report.get("identity_and_roll_certified") is not False
    ):
        raise IntegrityError("group diagnostic result is not duplicate-classifier eligible")
    summaries = group_report.get("public_decode_summaries")
    if not isinstance(summaries, list) or len(summaries) != 5:
        raise IntegrityError("group diagnostic price-free summaries are absent")
    repeats = {
        str(item.get("schema")): item.get("duplicate_count")
        for item in summaries
        if isinstance(item, Mapping)
    }
    if repeats.get(SCHEMA) != LEGACY_REPEAT_COUNT or any(
        repeats.get(name) != 0 for name in ("status", "statistics", "ohlcv-1m", "ohlcv-1s")
    ):
        raise IntegrityError("group diagnostic duplicate isolation drifted")
    source = _definition_source(group_plan)
    source_path = contained_path(root, str(source.get("relative_path", "")))
    if not source_path.is_file() or source_path.stat().st_size != source.get("bytes"):
        raise IntegrityError("definition diagnostic source size binding drifted")
    implementation_hashes = {
        path.as_posix(): sha256_file(root / path) for path in IMPLEMENTATION_PATHS
    }
    scope_id = sha256_json(
        {
            "implementation_head": implementation_head,
            "source_sha256": source["sha256"],
            "request_id": source["request_id"],
            "group_report_id": group_report["report_id"],
            "legacy_repeat_count": LEGACY_REPEAT_COUNT,
        }
    )
    scope_path_id = scope_id[:24]
    evidence_root = (EVIDENCE_ROOT / scope_path_id).as_posix()
    core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "state": "PREPARED_REQUIRES_SEPARATE_DEFINITION_DUPLICATE_DIAGNOSTIC_APPROVAL",
        "operation": OPERATION,
        "implementation_head": implementation_head,
        "implementation_hashes": implementation_hashes,
        "group_plan_id": group_plan["plan_id"],
        "group_plan_sha256": sha256_file(root / GROUP_PLAN_PATH),
        "group_audit_id": group_audit["audit_id"],
        "group_audit_sha256": sha256_file(root / GROUP_AUDIT_PATH),
        "group_report_id": group_report["report_id"],
        "group_report_sha256": sha256_file(root / GROUP_REPORT_PATH),
        "group_terminal_id": group_terminal["terminal_id"],
        "group_terminal_sha256": sha256_file(root / GROUP_TERMINAL_PATH),
        "scope_id": scope_id,
        "scope_path_id": scope_path_id,
        "source": source,
        "source_count": 1,
        "source_bytes": source["bytes"],
        "market": MARKET,
        "schema": SCHEMA,
        "year": YEAR,
        "legacy_repeat_count": LEGACY_REPEAT_COUNT,
        "limits": {
            "maximum_source_count": MAXIMUM_SOURCE_COUNT,
            "maximum_workers": MAXIMUM_WORKERS,
            "maximum_batch_rows": MAXIMUM_BATCH_ROWS,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
            "required_free_disk_bytes": REQUIRED_FREE_DISK_BYTES,
            "maximum_attempts": MAXIMUM_ATTEMPTS,
            "maximum_retries": MAXIMUM_RETRIES,
            "provider_calls": 0,
            "external_cost_usd": "0",
        },
        "evidence_root": evidence_root,
        "report_path": f"{evidence_root}/report.json",
        "terminal_path": f"{evidence_root}/terminal.json",
        "pre_authority_payload_reads": 0,
        "diagnostic_only": True,
        "forbidden": {
            "dbn_open": True,
            "second_parquet_open": True,
            "parquet_creation": True,
            "year_2025_or_2026_payload_open": True,
            "provider_or_network_access": True,
            "credential_access": True,
            "raw_values_or_semantic_keys_in_report": True,
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
    plan = _object(root / PLAN_PATH, "definition duplicate diagnostic plan")
    _self_hash(plan, "plan_id", "definition duplicate diagnostic plan")
    if plan.get("state") != "PREPARED_REQUIRES_SEPARATE_DEFINITION_DUPLICATE_DIAGNOSTIC_APPROVAL":
        raise IntegrityError("definition duplicate diagnostic plan state is invalid")
    return plan


def build_audit(*, root: Path) -> dict[str, object]:
    plan = load_plan(root=root)
    if build_plan(root=root, implementation_head=str(plan["implementation_head"])) != plan:
        raise IntegrityError("definition duplicate diagnostic plan reconstruction differs")
    if (root / str(plan["evidence_root"])).exists():
        raise IntegrityError("definition duplicate diagnostic create-only output exists")
    core = {
        "schema_version": AUDIT_SCHEMA,
        "state": "PASS_SOURCE_SAFE_DEFINITION_DUPLICATE_DIAGNOSTIC_AUDIT",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "source_count": 1,
        "source_bytes": plan["source_bytes"],
        "market": MARKET,
        "schema": SCHEMA,
        "year": YEAR,
        "legacy_repeat_count": LEGACY_REPEAT_COUNT,
        "parquet_payloads_opened": 0,
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
    if not isinstance(source, Mapping):
        raise IntegrityError("definition diagnostic source binding is invalid")
    return {
        "source_market": MARKET,
        "source_schema": SCHEMA,
        "source_year": str(YEAR),
        "source_count": "1",
        "source_bytes": str(plan["source_bytes"]),
        "source_sha256": str(source["sha256"]),
        "legacy_repeat_count": str(LEGACY_REPEAT_COUNT),
        "maximum_workers": str(MAXIMUM_WORKERS),
        "maximum_batch_rows": str(MAXIMUM_BATCH_ROWS),
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


def classify_definition_repeats(
    *, source_path: Path, source: Mapping[str, object], deadline: float,
    clock=time.monotonic,
) -> dict[str, int | str]:
    """Count legacy-key repeats without reporting any definition value or key."""

    if source.get("market") != MARKET or source.get("schema") != SCHEMA or source.get("year") != YEAR:
        raise UnauthorizedOperation("definition duplicate source is outside frozen scope")
    parquet = pq.ParquetFile(source_path)
    metadata = parquet.schema_arrow.metadata or {}
    if (
        parquet.schema_arrow.names != DEFINITION_SCHEMA.names
        or metadata.get(b"source_schema") != b"definition"
        or metadata.get(b"source_file_sha256")
        != str(source["source_sha256"]).encode("ascii")
    ):
        raise IntegrityError("definition duplicate Parquet binding drifted")
    names = parquet.schema_arrow.names
    semantic_names = [name for name in names if name not in _LINEAGE_COLUMNS]
    row_count = 0
    legacy_repeats = 0
    exact_semantic_duplicates = 0
    conflicting_same_key_updates = 0
    prior_legacy: tuple[object, ...] | None = None
    prior_semantic: tuple[object, ...] | None = None
    prior_order: int | None = None
    for batch in parquet.iter_batches(batch_size=MAXIMUM_BATCH_ROWS):
        if clock() > deadline:
            raise TimeoutError("definition duplicate diagnostic deadline reached")
        if not isinstance(batch, pa.RecordBatch) or batch.schema.names != names:
            raise IntegrityError("definition duplicate row batch schema drifted")
        for row in batch.to_pylist():
            if row.get("row_ordinal") != row_count:
                raise IntegrityError("definition duplicate row identity is not contiguous")
            if row.get("source_file_sha256") != source["source_sha256"]:
                raise IntegrityError("definition duplicate row lineage drifted")
            order = int(row["ts_recv_ns"])
            if prior_order is not None and order < prior_order:
                raise IntegrityError("definition duplicate rows are not in source order")
            legacy = (order, int(row["instrument_id"]), row["raw_symbol"])
            semantic = tuple(row[name] for name in semantic_names)
            if legacy == prior_legacy:
                legacy_repeats += 1
                if semantic == prior_semantic:
                    exact_semantic_duplicates += 1
                else:
                    conflicting_same_key_updates += 1
            prior_order, prior_legacy, prior_semantic = order, legacy, semantic
            row_count += 1
    if legacy_repeats != exact_semantic_duplicates + conflicting_same_key_updates:
        raise IntegrityError("definition repeat classification is incomplete")
    if exact_semantic_duplicates == legacy_repeats:
        classification = "EXACT_SEMANTIC_DUPLICATES"
    elif conflicting_same_key_updates == legacy_repeats:
        classification = "LEGACY_KEY_CONFLATES_DISTINCT_DEFINITION_UPDATES"
    else:
        classification = "MIXED_EXACT_DUPLICATES_AND_DISTINCT_UPDATES"
    return {
        "row_count": row_count,
        "legacy_repeat_count": legacy_repeats,
        "exact_semantic_duplicate_count": exact_semantic_duplicates,
        "distinct_same_key_update_count": conflicting_same_key_updates,
        "classification": classification,
    }


def _seal_tree(root: Path) -> None:
    if root.exists():
        for path in root.rglob("*"):
            if path.is_file():
                path.chmod(stat.S_IREAD)


def execute_once(
    *, root: Path, authorization: OperationReceipt, clock=time.monotonic,
    disk_usage=shutil.disk_usage,
) -> dict[str, object]:
    """Read one definition Parquet and classify repeats without source values."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    if _git_head(root) != plan["implementation_head"]:
        raise IntegrityError("definition duplicate diagnostic HEAD drifted")
    if build_plan(root=root, implementation_head=str(plan["implementation_head"])) != plan:
        raise IntegrityError("definition duplicate diagnostic plan drifted")
    if build_audit(root=root) != _object(root / AUDIT_PATH, "definition diagnostic audit"):
        raise IntegrityError("definition duplicate diagnostic audit drifted")
    evidence = contained_path(root, str(plan["evidence_root"]))
    if evidence.exists():
        raise IntegrityError("definition duplicate diagnostic create-only output exists")
    free = getattr(disk_usage(root), "free", None)
    if type(free) is not int or free < REQUIRED_FREE_DISK_BYTES:
        raise UnauthorizedOperation("insufficient disk for definition duplicate diagnostic")
    scope = required_scope(root=root, plan=plan)
    authorization.verify(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    evidence.mkdir(parents=True, exist_ok=False)
    use_path = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    source = plan["source"]
    source_path = contained_path(root, str(source["relative_path"]))
    before = sha256_file(source_path)
    failure_type: str | None = None
    parquet_payloads_opened = 0
    result: dict[str, int | str] | None = None
    try:
        if before != source["sha256"]:
            raise IntegrityError("definition duplicate source hash drifted before scan")
        parquet_payloads_opened = 1
        result = classify_definition_repeats(
            source_path=source_path,
            source=source,
            deadline=clock() + MAXIMUM_RUNTIME_SECONDS,
            clock=clock,
        )
        if result["legacy_repeat_count"] != LEGACY_REPEAT_COUNT:
            raise IntegrityError("definition duplicate repeat count differs from predecessor")
        if sha256_file(source_path) != source["sha256"]:
            raise IntegrityError("definition duplicate source hash drifted after scan")
        state = "PASS_DEFINITION_DUPLICATE_SEMANTICS_DIAGNOSTIC"
    except Exception as exc:
        failure_type = type(exc).__name__
        state = "FAIL_CLOSED_DEFINITION_DUPLICATE_SEMANTICS_DIAGNOSTIC"
    report_core = {
        "schema_version": REPORT_SCHEMA,
        "state": state,
        "plan_id": plan["plan_id"],
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "market": MARKET,
        "schema": SCHEMA,
        "year": YEAR,
        "source_sha256": source["sha256"],
        "source_bytes": source["bytes"],
        "result": result,
        "failure_type": failure_type,
        "attempts": 1,
        "automatic_retries": 0,
        "provider_calls": 0,
        "external_cost_usd": "0",
        "dbn_payloads_opened": 0,
        "parquet_payloads_opened": parquet_payloads_opened,
        "parquets_created": 0,
        "year_2025_or_2026_payloads_opened": 0,
        "raw_values_or_semantic_keys_reported": False,
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
    _seal_tree(evidence)
    terminal_path = contained_path(root, str(plan["terminal_path"]))
    _write_create_only(terminal_path, terminal)
    return terminal


__all__ = [
    "AUDIT_PATH",
    "OPERATION",
    "PLAN_PATH",
    "build_audit",
    "build_plan",
    "classify_definition_repeats",
    "execute_once",
    "load_plan",
    "required_scope",
    "write_audit_create_only",
    "write_plan_create_only",
]
