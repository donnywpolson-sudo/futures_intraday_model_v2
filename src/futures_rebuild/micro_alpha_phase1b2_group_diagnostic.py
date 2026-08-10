"""Bounded five-schema diagnostic for the v3 Phase 2 transition failure."""

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
from .micro_alpha_phase1b2_decoder import (
    BAR_SCHEMA,
    DEFINITION_SCHEMA,
    STATISTICS_SCHEMA,
    STATUS_SCHEMA,
    DecodeResult,
)
from .micro_alpha_phase1b2_execution import _group_disposition, _serialize_result
from .micro_alpha_pipeline import SCHEMAS


OPERATION: Final = "DIAGNOSE_APEX_MICRO_PHASE2_FIRST_GROUP_V2_ONCE"
PLAN_PATH: Final = Path("configs/apex_micro_phase1b2_phase2_group_diagnostic_plan_v2.json")
AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_group_diagnostic_plan_v2/audit.json"
)
V3_PLAN_PATH: Final = Path("configs/apex_micro_phase1b2_historical_execution_plan_v3.json")
FAILURE_REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v3_supersession/report.json"
)
PREDECESSOR_PLAN_PATH: Final = Path(
    "configs/apex_micro_phase1b2_phase2_diagnostic_plan_v1.json"
)
PREDECESSOR_AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_diagnostic_plan_v1/audit.json"
)
PREDECESSOR_REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_diagnostic_v1/"
    "9456f21bd11c75fa6710e1ad/report.json"
)
PREDECESSOR_TERMINAL_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_diagnostic_v1/"
    "9456f21bd11c75fa6710e1ad/terminal.json"
)
EVIDENCE_ROOT: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_group_diagnostic_v2"
)
PLAN_SCHEMA: Final = "apex_micro_phase1b2_phase2_group_diagnostic_plan/2.0.0"
AUDIT_SCHEMA: Final = "apex_micro_phase1b2_phase2_group_diagnostic_audit/2.0.0"
REPORT_SCHEMA: Final = "apex_micro_phase1b2_phase2_group_diagnostic_report/2.0.0"
TERMINAL_SCHEMA: Final = "apex_micro_phase1b2_phase2_group_diagnostic_terminal/2.0.0"
MAXIMUM_SOURCE_COUNT: Final = 5
MAXIMUM_WORKERS: Final = 1
MAXIMUM_BATCH_ROWS: Final = 100_000
MAXIMUM_RUNTIME_SECONDS: Final = 900
MAXIMUM_OUTPUT_BYTES: Final = 16 * 1024**2
REQUIRED_FREE_DISK_BYTES: Final = 1024**3
MAXIMUM_ATTEMPTS: Final = 1
MAXIMUM_RETRIES: Final = 0
MARKET: Final = "M6E"
YEAR: Final = 2018
INTERVAL: Final = "2018-01-01_2019-01-01"

IMPLEMENTATION_PATHS: Final = (
    Path("src/futures_rebuild/micro_alpha_phase1b2_group_diagnostic.py"),
    Path("src/futures_rebuild/micro_alpha_phase1b2_decoder.py"),
    Path("src/futures_rebuild/micro_alpha_phase1b2_execution.py"),
    Path("src/futures_rebuild/boundary.py"),
    Path("src/futures_rebuild/research_gateway_policy.py"),
    Path("src/futures_rebuild/canonical.py"),
    Path("configs/dependency_lock_receipt.json"),
)

_SCHEMA_BY_SOURCE: Final = {
    "definition": DEFINITION_SCHEMA,
    "status": STATUS_SCHEMA,
    "statistics": STATISTICS_SCHEMA,
    "ohlcv-1m": BAR_SCHEMA,
    "ohlcv-1s": BAR_SCHEMA,
}
_LINEAGE_COLUMNS: Final = {"source_file_sha256", "row_ordinal", "row_sha256"}


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
        raise UnauthorizedOperation("group diagnostic evidence byte ceiling exceeded")
    path.chmod(stat.S_IREAD)


def _source_group(
    *, failure: Mapping[str, object], v3_plan: Mapping[str, object], root: Path
) -> list[dict[str, object]]:
    inventory = failure.get("phase1b_inventory")
    sources = v3_plan.get("sources")
    if not isinstance(inventory, list) or len(inventory) != 120:
        raise IntegrityError("v3 Phase 1B inventory is absent")
    if not isinstance(sources, list) or len(sources) != 120:
        raise IntegrityError("v3 execution source plan is absent")
    execution_by_request = {
        str(item.get("request_id")): item
        for item in sources
        if isinstance(item, Mapping)
    }
    selected: list[dict[str, object]] = []
    for inventory_item in inventory:
        if not isinstance(inventory_item, Mapping):
            raise IntegrityError("v3 Phase 1B inventory entry is invalid")
        if inventory_item.get("market") != MARKET or inventory_item.get("year") != YEAR:
            continue
        request_id = str(inventory_item.get("request_id"))
        execution_item = execution_by_request.get(request_id)
        if not isinstance(execution_item, Mapping):
            raise IntegrityError("group diagnostic execution binding is absent")
        if (
            execution_item.get("market") != MARKET
            or execution_item.get("year") != YEAR
            or execution_item.get("interval") != INTERVAL
            or execution_item.get("schema") != inventory_item.get("schema")
            or execution_item.get("phase1b_release_id")
            != inventory_item.get("phase1b_release_id")
        ):
            raise IntegrityError("group diagnostic execution binding drifted")
        source_path = contained_path(root, str(inventory_item.get("relative_path", "")))
        if not source_path.is_file() or source_path.stat().st_size != inventory_item.get("bytes"):
            raise IntegrityError("group diagnostic source size binding drifted")
        selected.append(
            {
                **dict(inventory_item),
                "execution_item": dict(execution_item),
                "execution_item_sha256": sha256_json(dict(execution_item)),
            }
        )
    if len(selected) != MAXIMUM_SOURCE_COUNT or {
        str(item["schema"]) for item in selected
    } != set(SCHEMAS):
        raise IntegrityError("group diagnostic source selector is not exact")
    return sorted(selected, key=lambda item: str(item["schema"]))


def build_plan(*, root: Path, implementation_head: str) -> dict[str, object]:
    """Build a stat-only plan without hashing or decoding a Phase 1B Parquet."""

    root = root.resolve(strict=True)
    if implementation_head != _git_head(root):
        raise IntegrityError("group diagnostic plan must bind the live committed HEAD")
    failure = _object(root / FAILURE_REPORT_PATH, "v3 failure report")
    _self_hash(failure, "report_id", "v3 failure report")
    if failure.get("state") != "SUPERSEDED_PHASE1B_COMPLETE_PHASE2_TRANSITION_FAILED_CLOSED":
        raise IntegrityError("v3 failure state is not group-diagnostic eligible")
    predecessor_report = _object(root / PREDECESSOR_REPORT_PATH, "predecessor diagnostic report")
    _self_hash(predecessor_report, "report_id", "predecessor diagnostic report")
    predecessor_terminal = _object(root / PREDECESSOR_TERMINAL_PATH, "predecessor diagnostic terminal")
    _self_hash(predecessor_terminal, "terminal_id", "predecessor diagnostic terminal")
    if (
        predecessor_report.get("state") != "PASS_FIRST_INTERVAL_PHASE2_MATERIALIZATION"
        or predecessor_terminal.get("state") != "PASS_FIRST_INTERVAL_PHASE2_MATERIALIZATION"
        or predecessor_terminal.get("report_id") != predecessor_report.get("report_id")
    ):
        raise IntegrityError("predecessor materialization diagnostic did not pass")
    v3_plan = _object(root / V3_PLAN_PATH, "v3 execution plan")
    _self_hash(v3_plan, "plan_id", "v3 execution plan")
    sources = _source_group(failure=failure, v3_plan=v3_plan, root=root)
    implementation_hashes = {
        path.as_posix(): sha256_file(root / path) for path in IMPLEMENTATION_PATHS
    }
    scope_id = sha256_json(
        {
            "implementation_head": implementation_head,
            "source_sha256s": [str(item["sha256"]) for item in sources],
            "request_ids": [str(item["request_id"]) for item in sources],
            "failure_report_id": failure["report_id"],
            "predecessor_report_id": predecessor_report["report_id"],
        }
    )
    scope_path_id = scope_id[:24]
    evidence_root = (EVIDENCE_ROOT / scope_path_id).as_posix()
    core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "state": "PREPARED_REQUIRES_SEPARATE_FIVE_SOURCE_GROUP_DIAGNOSTIC_APPROVAL",
        "operation": OPERATION,
        "implementation_head": implementation_head,
        "implementation_hashes": implementation_hashes,
        "failure_report_id": failure["report_id"],
        "failure_report_sha256": sha256_file(root / FAILURE_REPORT_PATH),
        "predecessor_plan_id": _object(root / PREDECESSOR_PLAN_PATH, "predecessor plan")["plan_id"],
        "predecessor_plan_sha256": sha256_file(root / PREDECESSOR_PLAN_PATH),
        "predecessor_audit_id": _object(root / PREDECESSOR_AUDIT_PATH, "predecessor audit")["audit_id"],
        "predecessor_audit_sha256": sha256_file(root / PREDECESSOR_AUDIT_PATH),
        "predecessor_report_id": predecessor_report["report_id"],
        "predecessor_report_sha256": sha256_file(root / PREDECESSOR_REPORT_PATH),
        "predecessor_terminal_id": predecessor_terminal["terminal_id"],
        "predecessor_terminal_sha256": sha256_file(root / PREDECESSOR_TERMINAL_PATH),
        "scope_id": scope_id,
        "scope_path_id": scope_path_id,
        "market": MARKET,
        "year": YEAR,
        "interval": INTERVAL,
        "sources": sources,
        "source_count": len(sources),
        "source_bytes": sum(int(item["bytes"]) for item in sources),
        "schemas": list(SCHEMAS),
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
            "sixth_parquet_source_open": True,
            "phase2_parquet_creation": True,
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
    plan = _object(root / PLAN_PATH, "group diagnostic plan")
    _self_hash(plan, "plan_id", "group diagnostic plan")
    if plan.get("state") != "PREPARED_REQUIRES_SEPARATE_FIVE_SOURCE_GROUP_DIAGNOSTIC_APPROVAL":
        raise IntegrityError("group diagnostic plan state is invalid")
    return plan


def build_audit(*, root: Path) -> dict[str, object]:
    plan = load_plan(root=root)
    if build_plan(root=root, implementation_head=str(plan["implementation_head"])) != plan:
        raise IntegrityError("group diagnostic plan reconstruction differs")
    if (root / str(plan["evidence_root"])).exists():
        raise IntegrityError("group diagnostic create-only output exists")
    core = {
        "schema_version": AUDIT_SCHEMA,
        "state": "PASS_SOURCE_SAFE_FIVE_SOURCE_GROUP_DIAGNOSTIC_AUDIT",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "source_count": MAXIMUM_SOURCE_COUNT,
        "source_bytes": plan["source_bytes"],
        "market": MARKET,
        "year": YEAR,
        "schemas": list(SCHEMAS),
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
    return {
        "source_market": MARKET,
        "source_year": str(YEAR),
        "source_schemas": ",".join(SCHEMAS),
        "source_count": str(MAXIMUM_SOURCE_COUNT),
        "source_bytes": str(plan["source_bytes"]),
        "source_set_sha256": sha256_json(
            [str(item["sha256"]) for item in plan["sources"]]
        ),
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


def _semantic_key(row: Mapping[str, object], names: list[str]) -> tuple[object, ...]:
    return tuple(row[name] for name in names if name not in _LINEAGE_COLUMNS)


def reconstruct_decode_result(
    *, source_path: Path, source: Mapping[str, object], deadline: float,
    clock=time.monotonic,
) -> DecodeResult:
    """Reconstruct only the original price-free certification summary."""

    schema_name = str(source["schema"])
    if schema_name not in SCHEMAS or int(source["year"]) != YEAR:
        raise UnauthorizedOperation("group diagnostic source is outside the frozen scope")
    parquet = pq.ParquetFile(source_path)
    expected_schema = _SCHEMA_BY_SOURCE[schema_name]
    metadata = parquet.schema_arrow.metadata or {}
    if (
        parquet.schema_arrow.names != expected_schema.names
        or metadata.get(b"source_schema") != schema_name.encode("ascii")
        or metadata.get(b"source_file_sha256")
        != str(source["source_sha256"]).encode("ascii")
    ):
        raise IntegrityError("group diagnostic Parquet schema binding drifted")
    row_count = 0
    duplicate_count = 0
    ambiguous_identity_count = 0
    null_field_count = 0
    roll_transition_count = 0
    non_contiguous_instruments: set[int] = set()
    retired_instruments: set[int] = set()
    roll_sequence: list[int] = []
    instruments: set[int] = set()
    economics: set[tuple[int, int | None, int | None, str]] = set()
    prior_key: tuple[object, ...] | None = None
    prior_order: int | None = None
    prior_event: int | None = None
    prior_instrument: int | None = None
    names = parquet.schema_arrow.names
    for batch in parquet.iter_batches(batch_size=MAXIMUM_BATCH_ROWS):
        if clock() > deadline:
            raise TimeoutError("group diagnostic row scan deadline reached")
        if not isinstance(batch, pa.RecordBatch) or batch.schema.names != names:
            raise IntegrityError("group diagnostic row batch schema drifted")
        for row in batch.to_pylist():
            if row.get("row_ordinal") != row_count:
                raise IntegrityError("group diagnostic row identity is not contiguous")
            if row.get("source_file_sha256") != source["source_sha256"]:
                raise IntegrityError("group diagnostic row lineage drifted")
            instrument = int(row["instrument_id"])
            instruments.add(instrument)
            if schema_name == "definition":
                order = int(row["ts_recv_ns"])
                key = (order, instrument, row["raw_symbol"])
                economics.add(
                    (
                        instrument,
                        row["min_price_increment_nano"],
                        row["unit_of_measure_qty_nano"],
                        str(row["currency"]),
                    )
                )
            elif schema_name in {"ohlcv-1m", "ohlcv-1s"}:
                order = int(row["event_at_ns"])
                key = (order, instrument)
                null_field_count += sum(
                    row[name] is None
                    for name in ("open_nano", "high_nano", "low_nano", "close_nano", "volume")
                )
                if prior_event == order and prior_instrument != instrument:
                    ambiguous_identity_count += 1
                if prior_event is not None and order > prior_event and prior_instrument != instrument:
                    if prior_instrument is not None:
                        retired_instruments.add(prior_instrument)
                    if instrument in retired_instruments:
                        non_contiguous_instruments.add(instrument)
                    roll_transition_count += 1
                if not roll_sequence or roll_sequence[-1] != instrument:
                    roll_sequence.append(instrument)
                prior_event, prior_instrument = order, instrument
            else:
                order = int(row["ts_recv_ns"])
                key = _semantic_key(row, names)
                if schema_name == "statistics":
                    null_field_count += sum(
                        row[name] is None for name in ("ts_ref_ns", "price_nano", "quantity")
                    )
            if prior_order is not None and order < prior_order:
                raise IntegrityError("group diagnostic rows are not in source order")
            if key == prior_key:
                duplicate_count += 1
            prior_order, prior_key = order, key
            row_count += 1
    return DecodeResult(
        schema=schema_name,
        row_count=row_count,
        output_path=source_path.as_posix(),
        output_sha256=str(source["sha256"]),
        output_bytes=int(source["bytes"]),
        duplicate_count=duplicate_count,
        ambiguous_identity_count=ambiguous_identity_count,
        null_field_count=null_field_count,
        roll_transition_count=roll_transition_count,
        non_contiguous_instrument_count=len(non_contiguous_instruments),
        roll_sequence=tuple(roll_sequence),
        instrument_ids=tuple(sorted(instruments)),
        economics=tuple(sorted(economics)),
    )


def _seal_tree(root: Path) -> None:
    if root.exists():
        for path in root.rglob("*"):
            if path.is_file():
                path.chmod(stat.S_IREAD)


def execute_once(
    *, root: Path, authorization: OperationReceipt, clock=time.monotonic,
    disk_usage=shutil.disk_usage,
) -> dict[str, object]:
    """Read exactly five Phase 1B Parquets and diagnose the first group transition."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    if _git_head(root) != plan["implementation_head"]:
        raise IntegrityError("group diagnostic HEAD drifted")
    if build_plan(root=root, implementation_head=str(plan["implementation_head"])) != plan:
        raise IntegrityError("group diagnostic plan drifted")
    if build_audit(root=root) != _object(root / AUDIT_PATH, "group diagnostic audit"):
        raise IntegrityError("group diagnostic audit drifted")
    evidence = contained_path(root, str(plan["evidence_root"]))
    if evidence.exists():
        raise IntegrityError("group diagnostic create-only output exists")
    free = getattr(disk_usage(root), "free", None)
    if type(free) is not int or free < REQUIRED_FREE_DISK_BYTES:
        raise UnauthorizedOperation("insufficient disk for group diagnostic")
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
    started = clock()
    results: dict[str, DecodeResult] = {}
    serialized: list[dict[str, object]] = []
    failure_type: str | None = None
    failure_stage: str | None = None
    group_disposition: str | None = None
    identity_certified = False
    source_hashes_verified_before = 0
    source_hashes_verified_after = 0
    try:
        for source in plan["sources"]:
            source_path = contained_path(root, str(source["relative_path"]))
            failure_stage = f"PRE_SCAN_HASH_{source['schema']}"
            if sha256_file(source_path) != source["sha256"]:
                raise IntegrityError("group diagnostic source hash drifted before scan")
            source_hashes_verified_before += 1
        for source in plan["sources"]:
            failure_stage = f"RECONSTRUCT_{source['schema']}"
            source_path = contained_path(root, str(source["relative_path"]))
            result = reconstruct_decode_result(
                source_path=source_path,
                source=source,
                deadline=started + MAXIMUM_RUNTIME_SECONDS,
                clock=clock,
            )
            if sha256_file(source_path) != source["sha256"]:
                raise IntegrityError("group diagnostic source hash drifted after scan")
            source_hashes_verified_after += 1
            results[str(source["schema"])] = result
        failure_stage = "GROUP_DISPOSITION"
        group_disposition, identity_certified = _group_disposition(
            market=MARKET, results=results
        )
        failure_stage = "INTERVAL_RECEIPT_SERIALIZATION"
        for source in plan["sources"]:
            serialized.append(
                _serialize_result(
                    root,
                    item=source["execution_item"],
                    result=results[str(source["schema"])],
                )
            )
        state = "PASS_FIRST_GROUP_TRANSITION_DIAGNOSTIC"
        failure_stage = None
    except Exception as exc:
        failure_type = type(exc).__name__
        state = "FAIL_CLOSED_FIRST_GROUP_TRANSITION_DIAGNOSTIC"
    public_summaries: list[dict[str, object]] = []
    for schema in sorted(results):
        public = results[schema].public_record()
        public["output_path"] = Path(str(public["output_path"])).relative_to(root).as_posix()
        public_summaries.append({"schema": schema, **public})
    report_core = {
        "schema_version": REPORT_SCHEMA,
        "state": state,
        "plan_id": plan["plan_id"],
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "diagnostic_stage": "FIRST_FIVE_SCHEMA_GROUP_TRANSITION",
        "market": MARKET,
        "year": YEAR,
        "interval": INTERVAL,
        "source_count": MAXIMUM_SOURCE_COUNT,
        "source_bytes": plan["source_bytes"],
        "source_hashes_verified_before_scan": source_hashes_verified_before,
        "source_hashes_verified_after_scan": source_hashes_verified_after,
        "group_disposition": group_disposition,
        "identity_and_roll_certified": identity_certified,
        "public_decode_summaries": public_summaries,
        "serialized_interval_receipt_count": len(serialized),
        "serialized_interval_receipt_ids": [
            str(item["decode_record_id"]) for item in serialized
        ],
        "failure_stage": failure_stage,
        "failure_type": failure_type,
        "attempts": 1,
        "automatic_retries": 0,
        "provider_calls": 0,
        "external_cost_usd": "0",
        "dbn_payloads_opened": 0,
        "parquet_payloads_opened": len(results),
        "phase2_parquets_created": 0,
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
    "execute_once",
    "load_plan",
    "reconstruct_decode_result",
    "required_scope",
    "write_audit_create_only",
    "write_plan_create_only",
]
