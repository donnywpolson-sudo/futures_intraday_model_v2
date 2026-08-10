"""Exact-duplicate-safe Phase 2 successor over preserved micro Phase 1B files.

Planning is stat-only. Execution is a separate one-use derived-row boundary that
can open only the 120 frozen 2018-2024 Phase 1B Parquets, scan them in bounded
batches, and create 24 inactive causal one-minute Parquets plus price-free
certification evidence. Exact retained-semantics definition repeats remain in
their immutable Phase 1B source and receive an explicit certificate; distinct
same-key updates fail closed.
"""

from __future__ import annotations

import json
import shutil
import stat
import subprocess
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Final, TypeVar

import pyarrow as pa
import pyarrow.parquet as pq

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    contained_path,
    sha256_file,
    sha256_json,
)
from .errors import IntegrityError, UnauthorizedOperation
from .micro_alpha_phase1b2_decoder import (
    BAR_SCHEMA,
    DEFINITION_SCHEMA,
    STATISTICS_SCHEMA,
    STATUS_SCHEMA,
    CausalResult,
    CreatedByteBudget,
    DecodeResult,
    materialize_causal_1m_inactive,
)
from .micro_alpha_phase1b2_execution import _group_disposition, _serialize_result
from .micro_alpha_phase1b2_preparation import (
    ACTIVE_MICRO_CATALOG_PATH,
    ACTIVE_MICRO_POINTER_PATH,
    require_row_certified_catalog_candidate,
)
from .micro_alpha_pipeline import LANE_ID, SCHEMAS, TIER_1_MARKETS


OPERATION: Final = "BUILD_APEX_MICRO_PHASE2_CERTIFIED_INACTIVE_SUCCESSOR_V4_ONCE"
PLAN_PATH: Final = Path("configs/apex_micro_phase1b2_phase2_successor_plan_v4.json")
AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_successor_plan_v4/audit.json"
)
V3_PLAN_PATH: Final = Path("configs/apex_micro_phase1b2_historical_execution_plan_v3.json")
V3_TERMINAL_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_execution_v3/"
    "f28cb40f23574e6905a10ff2/terminal.json"
)
V3_FAILURE_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v3_supersession/"
    "report.json"
)
GROUP_REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_group_diagnostic_v2/"
    "57515d8f88bca13ec9c9cab3/report.json"
)
GROUP_TERMINAL_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_group_diagnostic_v2/"
    "57515d8f88bca13ec9c9cab3/terminal.json"
)
DUPLICATE_PLAN_PATH: Final = Path(
    "configs/apex_micro_phase1b2_definition_duplicate_diagnostic_plan_v3.json"
)
DUPLICATE_AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_definition_duplicate_diagnostic_plan_v3/audit.json"
)
DUPLICATE_REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_definition_duplicate_diagnostic_v3/"
    "f0a577957d40262b1faca744/report.json"
)
DUPLICATE_TERMINAL_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_definition_duplicate_diagnostic_v3/"
    "f0a577957d40262b1faca744/terminal.json"
)
STAGING_ROOT: Final = Path("state/data_publication_staging/apex_integer_micro_11")
EVIDENCE_ROOT: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_successor_v4"
)
PLAN_SCHEMA: Final = "apex_micro_phase1b2_phase2_successor_plan/4.0.0"
AUDIT_SCHEMA: Final = "apex_micro_phase1b2_phase2_successor_audit/4.0.0"
REPORT_SCHEMA: Final = "apex_micro_phase1b2_source_certification/2.0.0"
CATALOG_SCHEMA: Final = "apex_micro_inactive_catalog_candidate/2.0.0"
TERMINAL_SCHEMA: Final = "apex_micro_phase1b2_phase2_successor_terminal/4.0.0"
FAILURE_SCHEMA: Final = "apex_micro_phase1b2_phase2_successor_failure/4.0.0"
DEFINITION_REPEAT_SCHEMA: Final = "apex_micro_definition_repeat_certificate/1.0.0"
ELIGIBLE_YEARS: Final = tuple(range(2018, 2025))
EXPECTED_SOURCE_COUNT: Final = 120
EXPECTED_SOURCE_BYTES: Final = 6_627_486_838
EXPECTED_INTERVAL_COUNT: Final = 24
EXPECTED_COVERAGE_CELL_COUNT: Final = 140
EXPECTED_PRELAUNCH_COUNT: Final = 20
MAXIMUM_PARQUET_OPEN_OPERATIONS: Final = 144
MAXIMUM_PARQUET_OUTPUTS: Final = 24
MAXIMUM_OUTPUT_BYTES: Final = 64 * 1024**3
MAXIMUM_EVIDENCE_FILE_BYTES: Final = 16 * 1024**2
REQUIRED_FREE_DISK_BYTES: Final = 80 * 1024**3
MAXIMUM_RUNTIME_SECONDS: Final = 43_200
MAXIMUM_WORKERS: Final = 2
MAXIMUM_BATCH_ROWS: Final = 100_000
MAXIMUM_ATTEMPTS: Final = 1
MAXIMUM_RETRIES: Final = 0
MAXIMUM_STAGED_PARTIAL_PATH_CHARS: Final = 240
_LINEAGE_COLUMNS: Final = {"source_file_sha256", "row_ordinal", "row_sha256"}
_SCHEMA_BY_SOURCE: Final = {
    "definition": DEFINITION_SCHEMA,
    "status": STATUS_SCHEMA,
    "statistics": STATISTICS_SCHEMA,
    "ohlcv-1m": BAR_SCHEMA,
    "ohlcv-1s": BAR_SCHEMA,
}

IMPLEMENTATION_PATHS: Final = (
    Path("src/futures_rebuild/micro_alpha_phase1b2_phase2_successor.py"),
    Path("src/futures_rebuild/micro_alpha_phase1b2_decoder.py"),
    Path("src/futures_rebuild/micro_alpha_phase1b2_execution.py"),
    Path("src/futures_rebuild/micro_alpha_phase1b2_preparation.py"),
    Path("src/futures_rebuild/research_gateway_policy.py"),
    Path("src/futures_rebuild/boundary.py"),
    Path("src/futures_rebuild/canonical.py"),
    Path("configs/dependency_lock_receipt.json"),
)


@dataclass(frozen=True)
class Phase1BScan:
    result: DecodeResult
    definition_repeat_certificate: dict[str, object] | None


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
    payload = canonical_bytes(value) + b"\n"
    if len(payload) > MAXIMUM_EVIDENCE_FILE_BYTES:
        raise UnauthorizedOperation("micro successor evidence file ceiling exceeded")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(payload)
    path.chmod(stat.S_IREAD)


def _phase1b_sources(
    *, root: Path, failure: Mapping[str, object], v3_plan: Mapping[str, object]
) -> list[dict[str, object]]:
    inventory = failure.get("phase1b_inventory")
    execution_sources = v3_plan.get("sources")
    if not isinstance(inventory, list) or len(inventory) != EXPECTED_SOURCE_COUNT:
        raise IntegrityError("micro successor Phase 1B inventory is absent")
    if not isinstance(execution_sources, list) or len(execution_sources) != EXPECTED_SOURCE_COUNT:
        raise IntegrityError("micro successor execution bindings are absent")
    by_request = {
        str(item.get("request_id")): item
        for item in execution_sources
        if isinstance(item, Mapping)
    }
    sources: list[dict[str, object]] = []
    for inventory_item in inventory:
        if not isinstance(inventory_item, Mapping):
            raise IntegrityError("micro successor inventory entry is invalid")
        request_id = str(inventory_item.get("request_id"))
        execution_item = by_request.get(request_id)
        if not isinstance(execution_item, Mapping):
            raise IntegrityError("micro successor request binding is absent")
        if (
            inventory_item.get("market") != execution_item.get("market")
            or inventory_item.get("schema") != execution_item.get("schema")
            or inventory_item.get("year") != execution_item.get("year")
            or inventory_item.get("phase1b_release_id")
            != execution_item.get("phase1b_release_id")
            or inventory_item.get("source_sha256") != execution_item.get("source_sha256")
        ):
            raise IntegrityError("micro successor Phase 1B binding drifted")
        year = inventory_item.get("year")
        if type(year) is not int or year not in ELIGIBLE_YEARS:
            raise UnauthorizedOperation("micro successor source year is not eligible")
        source_path = contained_path(root, str(inventory_item.get("relative_path", "")))
        if not source_path.is_file() or source_path.stat().st_size != inventory_item.get("bytes"):
            raise IntegrityError("micro successor Phase 1B size binding drifted")
        sources.append(
            {
                **dict(inventory_item),
                "execution_item": dict(execution_item),
                "execution_item_sha256": sha256_json(dict(execution_item)),
            }
        )
    sources.sort(key=lambda item: (str(item["market"]), int(item["year"]), str(item["schema"])))
    if (
        len(sources) != EXPECTED_SOURCE_COUNT
        or sum(int(item["bytes"]) for item in sources) != EXPECTED_SOURCE_BYTES
        or len({str(item["request_id"]) for item in sources}) != EXPECTED_SOURCE_COUNT
    ):
        raise IntegrityError("micro successor Phase 1B inventory totals drifted")
    grouped: dict[tuple[str, int], set[str]] = defaultdict(set)
    for item in sources:
        grouped[(str(item["market"]), int(item["year"]))].add(str(item["schema"]))
    if len(grouped) != EXPECTED_INTERVAL_COUNT or any(value != set(SCHEMAS) for value in grouped.values()):
        raise IntegrityError("micro successor five-schema groups drifted")
    return sources


def _predecessor_evidence(root: Path) -> dict[str, dict[str, object]]:
    paths = {
        "v3_plan": V3_PLAN_PATH,
        "v3_terminal": V3_TERMINAL_PATH,
        "v3_failure": V3_FAILURE_PATH,
        "group_report": GROUP_REPORT_PATH,
        "group_terminal": GROUP_TERMINAL_PATH,
        "duplicate_plan": DUPLICATE_PLAN_PATH,
        "duplicate_audit": DUPLICATE_AUDIT_PATH,
        "duplicate_report": DUPLICATE_REPORT_PATH,
        "duplicate_terminal": DUPLICATE_TERMINAL_PATH,
    }
    values = {name: _object(root / path, name.replace("_", " ")) for name, path in paths.items()}
    for name, key in (
        ("v3_plan", "plan_id"),
        ("v3_terminal", "terminal_id"),
        ("v3_failure", "report_id"),
        ("group_report", "report_id"),
        ("group_terminal", "terminal_id"),
        ("duplicate_plan", "plan_id"),
        ("duplicate_audit", "audit_id"),
        ("duplicate_report", "report_id"),
        ("duplicate_terminal", "terminal_id"),
    ):
        _self_hash(values[name], key, name.replace("_", " "))
    duplicate = values["duplicate_report"]
    result = duplicate.get("result")
    if (
        values["v3_terminal"].get("completed_decode_count") != EXPECTED_SOURCE_COUNT
        or values["v3_terminal"].get("completed_phase2_count") != 0
        or values["v3_failure"].get("state")
        != "SUPERSEDED_PHASE1B_COMPLETE_PHASE2_TRANSITION_FAILED_CLOSED"
        or values["group_report"].get("state") != "PASS_FIRST_GROUP_TRANSITION_DIAGNOSTIC"
        or values["group_report"].get("group_disposition") != "DUPLICATE"
        or values["group_terminal"].get("report_id") != values["group_report"].get("report_id")
        or duplicate.get("state") != "PASS_DEFINITION_DUPLICATE_SEMANTICS_DIAGNOSTIC"
        or not isinstance(result, Mapping)
        or result.get("classification") != "EXACT_SEMANTIC_DUPLICATES"
        or result.get("legacy_repeat_count") != 308
        or result.get("exact_semantic_duplicate_count") != 308
        or result.get("distinct_same_key_update_count") != 0
        or values["duplicate_terminal"].get("report_id") != duplicate.get("report_id")
        or values["duplicate_audit"].get("plan_id") != values["duplicate_plan"].get("plan_id")
    ):
        raise IntegrityError("micro successor predecessor evidence is not eligible")
    return values


def build_plan(*, root: Path, implementation_head: str) -> dict[str, object]:
    """Build the full successor plan without hashing or opening a Parquet payload."""

    root = root.resolve(strict=True)
    if implementation_head != _git_head(root):
        raise IntegrityError("micro successor plan must bind the committed HEAD")
    if (root / ACTIVE_MICRO_CATALOG_PATH).exists() or (root / ACTIVE_MICRO_POINTER_PATH).exists():
        raise UnauthorizedOperation("micro catalog or pointer became active")
    evidence = _predecessor_evidence(root)
    v3_plan = evidence["v3_plan"]
    sources = _phase1b_sources(root=root, failure=evidence["v3_failure"], v3_plan=v3_plan)
    coverage = v3_plan.get("coverage")
    phase2 = v3_plan.get("phase2")
    if not isinstance(coverage, list) or len(coverage) != EXPECTED_COVERAGE_CELL_COUNT:
        raise IntegrityError("micro successor coverage census drifted")
    if not isinstance(phase2, list) or len(phase2) != EXPECTED_INTERVAL_COUNT:
        raise IntegrityError("micro successor Phase 2 release plan drifted")
    if sum(item.get("planned_disposition") == "PRODUCT_NOT_YET_EFFECTIVE" for item in coverage if isinstance(item, Mapping)) != EXPECTED_PRELAUNCH_COUNT:
        raise IntegrityError("micro successor prelaunch census drifted")
    implementation_hashes = {
        path.as_posix(): sha256_file(root / path) for path in IMPLEMENTATION_PATHS
    }
    scope_id = sha256_json(
        {
            "implementation_head": implementation_head,
            "source_inventory_id": evidence["v3_failure"]["phase1b_inventory_id"],
            "source_hashes": [str(item["sha256"]) for item in sources],
            "phase2_release_ids": [str(item["phase2_release_id"]) for item in phase2],
            "definition_diagnostic_report_id": evidence["duplicate_report"]["report_id"],
        }
    )
    scope_path_id = scope_id[:24]
    staging_root = (STAGING_ROOT / scope_path_id).as_posix()
    evidence_root = (EVIDENCE_ROOT / scope_path_id).as_posix()
    output_paths = [str(item["phase2_output_path"]) for item in phase2]
    partial_lengths = [
        len(str((root / staging_root / relative).resolve(strict=False))) + len(".partial")
        for relative in output_paths
    ]
    if len(set(output_paths)) != EXPECTED_INTERVAL_COUNT or max(partial_lengths) > MAXIMUM_STAGED_PARTIAL_PATH_CHARS:
        raise IntegrityError("micro successor output path bound drifted")
    for relative in output_paths:
        output = contained_path(root / staging_root, relative)
        if output.exists() or output.with_suffix(output.suffix + ".partial").exists():
            raise IntegrityError("micro successor create-only output collision")
    predecessor_bindings = {
        name: {
            "artifact_id": value.get(
                "plan_id",
                value.get("audit_id", value.get("report_id", value.get("terminal_id"))),
            ),
            "path": {
                "v3_plan": V3_PLAN_PATH,
                "v3_terminal": V3_TERMINAL_PATH,
                "v3_failure": V3_FAILURE_PATH,
                "group_report": GROUP_REPORT_PATH,
                "group_terminal": GROUP_TERMINAL_PATH,
                "duplicate_plan": DUPLICATE_PLAN_PATH,
                "duplicate_audit": DUPLICATE_AUDIT_PATH,
                "duplicate_report": DUPLICATE_REPORT_PATH,
                "duplicate_terminal": DUPLICATE_TERMINAL_PATH,
            }[name].as_posix(),
            "sha256": sha256_file(
                root
                / {
                    "v3_plan": V3_PLAN_PATH,
                    "v3_terminal": V3_TERMINAL_PATH,
                    "v3_failure": V3_FAILURE_PATH,
                    "group_report": GROUP_REPORT_PATH,
                    "group_terminal": GROUP_TERMINAL_PATH,
                    "duplicate_plan": DUPLICATE_PLAN_PATH,
                    "duplicate_audit": DUPLICATE_AUDIT_PATH,
                    "duplicate_report": DUPLICATE_REPORT_PATH,
                    "duplicate_terminal": DUPLICATE_TERMINAL_PATH,
                }[name]
            ),
        }
        for name, value in evidence.items()
    }
    core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "state": "PREPARED_REQUIRES_SEPARATE_FULL_PHASE2_DERIVED_ROW_APPROVAL",
        "operation": OPERATION,
        "implementation_head": implementation_head,
        "implementation_hashes": implementation_hashes,
        "predecessor_bindings": predecessor_bindings,
        "scope_id": scope_id,
        "scope_path_id": scope_path_id,
        "lane_id": LANE_ID,
        "markets": list(TIER_1_MARKETS),
        "years": list(ELIGIBLE_YEARS),
        "schemas": list(SCHEMAS),
        "sources": sources,
        "source_count": len(sources),
        "source_bytes": sum(int(item["bytes"]) for item in sources),
        "source_set_sha256": sha256_json([str(item["sha256"]) for item in sources]),
        "coverage": coverage,
        "coverage_cell_count": len(coverage),
        "prelaunch_cell_count": EXPECTED_PRELAUNCH_COUNT,
        "phase2": phase2,
        "interval_count": len(phase2),
        "staging_root": staging_root,
        "evidence_root": evidence_root,
        "outputs": {
            "source_certification_report": f"{evidence_root}/source_certification_report.json",
            "inactive_catalog_candidate": f"{evidence_root}/inactive_catalog_candidate.json",
            "failure_report": f"{evidence_root}/failure_report.json",
            "terminal": f"{evidence_root}/terminal.json",
            "phase2_parquet_paths": output_paths,
        },
        "limits": {
            "maximum_workers": MAXIMUM_WORKERS,
            "maximum_batch_rows": MAXIMUM_BATCH_ROWS,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_source_count": EXPECTED_SOURCE_COUNT,
            "maximum_source_bytes": EXPECTED_SOURCE_BYTES,
            "maximum_parquet_open_operations": MAXIMUM_PARQUET_OPEN_OPERATIONS,
            "maximum_parquet_outputs": MAXIMUM_PARQUET_OUTPUTS,
            "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
            "maximum_evidence_file_bytes": MAXIMUM_EVIDENCE_FILE_BYTES,
            "required_free_disk_bytes": REQUIRED_FREE_DISK_BYTES,
            "maximum_staged_partial_path_chars": MAXIMUM_STAGED_PARTIAL_PATH_CHARS,
            "observed_max_staged_partial_path_chars": max(partial_lengths),
            "maximum_attempts": MAXIMUM_ATTEMPTS,
            "maximum_retries": MAXIMUM_RETRIES,
            "provider_calls": 0,
            "external_cost_usd": "0",
        },
        "definition_repeat_policy": {
            "preserve_all_phase1b_rows": True,
            "allow_only_exact_retained_semantics_repeats": True,
            "distinct_same_key_update_fails_closed": True,
            "silent_deduplication_forbidden": True,
            "raw_values_or_semantic_keys_in_evidence_forbidden": True,
        },
        "pre_authority_payload_reads": 0,
        "forbidden": {
            "dbn_open": True,
            "year_2025_or_2026_payload_open": True,
            "provider_or_network_access": True,
            "credential_access": True,
            "phase1b_mutation_or_deduplication": True,
            "status_or_statistics_feature_use": True,
            "one_second_bbo_queue_fill_or_ordering_claim": True,
            "raw_values_or_semantic_keys_in_report": True,
            "publication_activation_registration_evaluation_or_trading": True,
            "standard_lane_mutation": True,
            "git_staging_commit_or_push": True,
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def write_plan_create_only(*, root: Path, implementation_head: str) -> dict[str, object]:
    plan = build_plan(root=root, implementation_head=implementation_head)
    _write_create_only(root / PLAN_PATH, plan)
    return plan


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH, "micro successor plan")
    _self_hash(plan, "plan_id", "micro successor plan")
    if plan.get("state") != "PREPARED_REQUIRES_SEPARATE_FULL_PHASE2_DERIVED_ROW_APPROVAL":
        raise IntegrityError("micro successor plan state is invalid")
    return plan


def build_audit(*, root: Path) -> dict[str, object]:
    plan = load_plan(root=root)
    if build_plan(root=root, implementation_head=str(plan["implementation_head"])) != plan:
        raise IntegrityError("micro successor plan reconstruction differs")
    if (root / str(plan["staging_root"])).exists() or (root / str(plan["evidence_root"])).exists():
        raise IntegrityError("micro successor create-only root already exists")
    core = {
        "schema_version": AUDIT_SCHEMA,
        "state": "PASS_SOURCE_SAFE_FULL_PHASE2_SUCCESSOR_AUDIT",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "implementation_head": plan["implementation_head"],
        "source_count": plan["source_count"],
        "source_bytes": plan["source_bytes"],
        "coverage_cell_count": plan["coverage_cell_count"],
        "prelaunch_cell_count": plan["prelaunch_cell_count"],
        "interval_count": plan["interval_count"],
        "parquet_payloads_opened": 0,
        "parquet_row_batches_opened": 0,
        "dbn_payloads_opened": 0,
        "year_2025_or_2026_payloads_opened": 0,
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
    limits = plan.get("limits")
    if not isinstance(limits, Mapping):
        raise IntegrityError("micro successor limit binding is invalid")
    return {
        "implementation_head": str(plan["implementation_head"]),
        "source_count": str(plan["source_count"]),
        "source_bytes": str(plan["source_bytes"]),
        "source_set_sha256": str(plan["source_set_sha256"]),
        "coverage_cell_count": str(plan["coverage_cell_count"]),
        "prelaunch_cell_count": str(plan["prelaunch_cell_count"]),
        "interval_count": str(plan["interval_count"]),
        "maximum_workers": str(limits["maximum_workers"]),
        "maximum_batch_rows": str(limits["maximum_batch_rows"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_parquet_open_operations": str(limits["maximum_parquet_open_operations"]),
        "maximum_parquet_outputs": str(limits["maximum_parquet_outputs"]),
        "maximum_output_bytes": str(limits["maximum_output_bytes"]),
        "required_free_disk_bytes": str(limits["required_free_disk_bytes"]),
        "maximum_attempts": str(limits["maximum_attempts"]),
        "maximum_retries": str(limits["maximum_retries"]),
        "provider_calls": "0",
        "external_cost_usd": "0",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def reconstruct_phase1b_scan(
    *, source_path: Path, source: Mapping[str, object], deadline: float,
    clock: Callable[[], float] = time.monotonic,
) -> Phase1BScan:
    """Reconstruct price-free certification state for one frozen Phase 1B file."""

    schema_name = str(source.get("schema"))
    market = str(source.get("market"))
    year = source.get("year")
    if (
        schema_name not in SCHEMAS
        or market not in TIER_1_MARKETS
        or type(year) is not int
        or year not in ELIGIBLE_YEARS
    ):
        raise UnauthorizedOperation("micro successor source is outside frozen scope")
    assert_no_linklike_ancestors(source_path)
    source_info = assert_plain_file(source_path)
    if source_info.st_size != source.get("bytes"):
        raise IntegrityError("micro successor Phase 1B source size drifted")
    parquet = pq.ParquetFile(source_path)
    expected_schema = _SCHEMA_BY_SOURCE[schema_name]
    metadata = parquet.schema_arrow.metadata or {}
    if (
        parquet.schema_arrow.names != expected_schema.names
        or metadata.get(b"source_schema") != schema_name.encode("ascii")
        or metadata.get(b"source_file_sha256")
        != str(source["source_sha256"]).encode("ascii")
    ):
        raise IntegrityError("micro successor Phase 1B schema binding drifted")
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
    prior_semantic: tuple[object, ...] | None = None
    prior_order: int | None = None
    prior_event: int | None = None
    prior_instrument: int | None = None
    exact_semantic_duplicates = 0
    distinct_same_key_updates = 0
    names = parquet.schema_arrow.names
    semantic_names = [name for name in names if name not in _LINEAGE_COLUMNS]
    for batch in parquet.iter_batches(batch_size=MAXIMUM_BATCH_ROWS):
        if clock() > deadline:
            raise TimeoutError("micro successor Phase 1B scan deadline reached")
        if not isinstance(batch, pa.RecordBatch) or batch.schema.names != names:
            raise IntegrityError("micro successor Phase 1B row batch schema drifted")
        for row in batch.to_pylist():
            if row.get("row_ordinal") != row_count:
                raise IntegrityError("micro successor Phase 1B row identity is not contiguous")
            if row.get("source_file_sha256") != source["source_sha256"]:
                raise IntegrityError("micro successor Phase 1B row lineage drifted")
            instrument = int(row["instrument_id"])
            instruments.add(instrument)
            semantic: tuple[object, ...] | None = None
            if schema_name == "definition":
                order = int(row["ts_recv_ns"])
                key = (order, instrument, row["raw_symbol"])
                semantic = tuple(row[name] for name in semantic_names)
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
                key = tuple(row[name] for name in semantic_names)
                if schema_name == "statistics":
                    null_field_count += sum(
                        row[name] is None for name in ("ts_ref_ns", "price_nano", "quantity")
                    )
            if prior_order is not None and order < prior_order:
                raise IntegrityError("micro successor Phase 1B rows are not in source order")
            if key == prior_key:
                duplicate_count += 1
                if schema_name == "definition":
                    if semantic == prior_semantic:
                        exact_semantic_duplicates += 1
                    else:
                        distinct_same_key_updates += 1
            prior_order, prior_key = order, key
            if schema_name == "definition":
                prior_semantic = semantic
            row_count += 1
    result = DecodeResult(
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
    certificate: dict[str, object] | None = None
    if schema_name == "definition":
        if duplicate_count != exact_semantic_duplicates + distinct_same_key_updates:
            raise IntegrityError("micro successor definition repeat classification is incomplete")
        if duplicate_count == 0:
            classification = "NO_LEGACY_REPEATS"
        elif exact_semantic_duplicates == duplicate_count:
            classification = "EXACT_SEMANTIC_DUPLICATES_PRESERVED"
        elif distinct_same_key_updates == duplicate_count:
            classification = "DISTINCT_SAME_KEY_DEFINITION_UPDATES"
        else:
            classification = "MIXED_EXACT_DUPLICATES_AND_DISTINCT_UPDATES"
        certificate_core: dict[str, object] = {
            "schema_version": DEFINITION_REPEAT_SCHEMA,
            "market": market,
            "year": year,
            "request_id": source["request_id"],
            "phase1b_sha256": source["sha256"],
            "row_count": row_count,
            "legacy_repeat_count": duplicate_count,
            "exact_semantic_duplicate_count": exact_semantic_duplicates,
            "distinct_same_key_update_count": distinct_same_key_updates,
            "classification": classification,
            "phase1b_rows_preserved_without_deduplication": True,
            "raw_values_or_semantic_keys_reported": False,
        }
        certificate = {
            **certificate_core,
            "definition_repeat_certificate_id": sha256_json(certificate_core),
        }
    return Phase1BScan(result=result, definition_repeat_certificate=certificate)


def _scan_one(
    *, root: Path, source: Mapping[str, object], deadline: float,
    clock: Callable[[], float],
) -> Phase1BScan:
    path = contained_path(root, str(source["relative_path"]))
    if sha256_file(path) != source["sha256"]:
        raise IntegrityError("micro successor Phase 1B hash drifted before scan")
    scan = reconstruct_phase1b_scan(
        source_path=path, source=source, deadline=deadline, clock=clock
    )
    if sha256_file(path) != source["sha256"]:
        raise IntegrityError("micro successor Phase 1B hash drifted after scan")
    return scan


_Item = TypeVar("_Item")
_Result = TypeVar("_Result")


def _bounded_parallel(
    *, items: list[_Item], worker: Callable[[_Item], _Result],
    key: Callable[[_Item], str], maximum_workers: int = MAXIMUM_WORKERS,
    result_sink: dict[str, _Result] | None = None,
) -> dict[str, _Result]:
    """Keep at most two tasks live and stop submitting after the first failure."""

    iterator = iter(items)
    results: dict[str, _Result] = {} if result_sink is None else result_sink
    first_failure: Exception | None = None
    with ThreadPoolExecutor(max_workers=maximum_workers) as executor:
        pending: dict[Future[_Result], _Item] = {}
        for _ in range(maximum_workers):
            try:
                item = next(iterator)
            except StopIteration:
                break
            pending[executor.submit(worker, item)] = item
        while pending:
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            completed_without_failure: list[tuple[_Item, _Result]] = []
            for future in done:
                item = pending.pop(future)
                try:
                    completed_without_failure.append((item, future.result()))
                except Exception as exc:  # preserve first failure after the other live task settles
                    if first_failure is None:
                        first_failure = exc
            for item, result in completed_without_failure:
                results[key(item)] = result
                if first_failure is None:
                    try:
                        next_item = next(iterator)
                    except StopIteration:
                        continue
                    pending[executor.submit(worker, next_item)] = next_item
        if first_failure is not None:
            raise first_failure
    return results


def successor_group_disposition(
    *, market: str, scans: Mapping[str, Phase1BScan]
) -> tuple[str, bool]:
    if set(scans) != set(SCHEMAS):
        return "SOURCE_UNAVAILABLE", False
    certificate = scans["definition"].definition_repeat_certificate
    if not isinstance(certificate, Mapping):
        return "AMBIGUOUS_IDENTITY", False
    classification = certificate.get("classification")
    if classification not in {"NO_LEGACY_REPEATS", "EXACT_SEMANTIC_DUPLICATES_PRESERVED"}:
        return "AMBIGUOUS_IDENTITY", False
    if any(
        scan.result.duplicate_count
        for schema, scan in scans.items()
        if schema != "definition"
    ):
        return "DUPLICATE", False
    results = {schema: scan.result for schema, scan in scans.items()}
    results["definition"] = replace(results["definition"], duplicate_count=0)
    return _group_disposition(market=market, results=results)


def cross_interval_roll_certificate(
    *, market: str, groups: Mapping[tuple[str, int, str], Mapping[str, Phase1BScan]]
) -> dict[str, object]:
    """Certify that rank-zero identities never reappear after retirement across years."""

    market_groups = sorted(
        ((key, value) for key, value in groups.items() if key[0] == market),
        key=lambda pair: (pair[0][1], pair[0][2]),
    )
    if not market_groups:
        raise IntegrityError("micro successor market roll groups are absent")
    sequence: list[int] = []
    for _, scans in market_groups:
        minute = scans["ohlcv-1m"].result.roll_sequence
        second = scans["ohlcv-1s"].result.roll_sequence
        if minute != second or not minute:
            raise IntegrityError("micro successor interval roll evidence drifted")
        for instrument in minute:
            if not sequence or sequence[-1] != instrument:
                sequence.append(instrument)
    seen: set[int] = set()
    retired: set[int] = set()
    prior: int | None = None
    reappearing = 0
    for instrument in sequence:
        if prior is not None and instrument != prior:
            retired.add(prior)
        if instrument in retired:
            reappearing += 1
        seen.add(instrument)
        prior = instrument
    core: dict[str, object] = {
        "market": market,
        "interval_count": len(market_groups),
        "instrument_identity_count": len(seen),
        "roll_transition_count": max(0, len(sequence) - 1),
        "roll_sequence_sha256": sha256_json(sequence),
        "non_contiguous_reappearance_count": reappearing,
        "roll_continuity_certified": reappearing == 0,
        "raw_instrument_ids_reported": False,
    }
    return {**core, "cross_interval_roll_certificate_id": sha256_json(core)}


def _causal_public(root: Path, result: CausalResult) -> dict[str, object]:
    public = result.public_record()
    public.pop("causal_record_id")
    output = Path(str(public["output_path"])).resolve(strict=False)
    try:
        public["output_path"] = output.relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise IntegrityError("micro successor Phase 2 output escaped repository") from exc
    return public


def _seal_tree(path: Path) -> None:
    if not path.exists():
        return
    for item in path.rglob("*"):
        if item.is_file():
            item.chmod(stat.S_IREAD)


def execute_once(
    *, root: Path, authorization: OperationReceipt,
    clock: Callable[[], float] = time.monotonic,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
) -> dict[str, object]:
    """Execute the full inactive Phase 2 successor without DBN or sealed-year access."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    if _git_head(root) != plan["implementation_head"]:
        raise IntegrityError("micro successor committed HEAD drifted")
    if build_plan(root=root, implementation_head=str(plan["implementation_head"])) != plan:
        raise IntegrityError("micro successor plan drifted")
    audit = _object(root / AUDIT_PATH, "micro successor audit")
    _self_hash(audit, "audit_id", "micro successor audit")
    if build_audit(root=root) != audit:
        raise IntegrityError("micro successor audit drifted")
    staging = contained_path(root, str(plan["staging_root"]))
    evidence = contained_path(root, str(plan["evidence_root"]))
    if staging.exists() or evidence.exists():
        raise IntegrityError("micro successor create-only root already exists")
    free = getattr(disk_usage(root), "free", None)
    if type(free) is not int or free < REQUIRED_FREE_DISK_BYTES:
        raise UnauthorizedOperation("insufficient disk for micro Phase 2 successor")
    if (root / ACTIVE_MICRO_CATALOG_PATH).exists() or (root / ACTIVE_MICRO_POINTER_PATH).exists():
        raise UnauthorizedOperation("micro catalog or pointer became active")
    scope = required_scope(root=root, plan=plan)
    authorization.verify(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    use_path = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    staging.mkdir(parents=True, exist_ok=False)
    evidence.mkdir(parents=True, exist_ok=False)
    started = clock()
    deadline = started + MAXIMUM_RUNTIME_SECONDS
    scans: dict[str, Phase1BScan] = {}
    phase2_results: dict[str, CausalResult] = {}
    coverage: list[dict[str, object]] = []
    report_id: str | None = None
    report_sha256: str | None = None
    candidate_id: str | None = None
    source_hashes_verified_after = 0
    failure: Exception | None = None
    group_disposition_counts: dict[str, int] = {}
    definition_repeat_classification_counts: dict[str, int] = {}
    created_budget = CreatedByteBudget(MAXIMUM_OUTPUT_BYTES)
    try:
        source_items = [dict(item) for item in plan["sources"]]
        _bounded_parallel(
            items=source_items,
            worker=lambda item: _scan_one(
                root=root, source=item, deadline=deadline, clock=clock
            ),
            key=lambda item: str(item["request_id"]),
            result_sink=scans,
        )
        if len(scans) != EXPECTED_SOURCE_COUNT:
            raise IntegrityError("micro successor Phase 1B scan count drifted")
        source_by_request = {str(item["request_id"]): item for item in source_items}
        groups: dict[tuple[str, int, str], dict[str, Phase1BScan]] = defaultdict(dict)
        group_sources: dict[tuple[str, int, str], dict[str, dict[str, object]]] = defaultdict(dict)
        for request_id, scan in scans.items():
            item = source_by_request[request_id]
            execution_item = item["execution_item"]
            key = (
                str(item["market"]),
                int(item["year"]),
                str(execution_item["interval"]),
            )
            groups[key][str(item["schema"])] = scan
            group_sources[key][str(item["schema"])] = item
        dispositions: dict[tuple[str, int, str], str] = {}
        for key, group in groups.items():
            disposition, certified = successor_group_disposition(market=key[0], scans=group)
            dispositions[key] = disposition
        group_disposition_counts = dict(sorted(Counter(dispositions.values()).items()))
        definition_repeat_classification_counts = dict(
            sorted(
                Counter(
                    str(scan.definition_repeat_certificate["classification"])
                    for scan in scans.values()
                    if scan.definition_repeat_certificate is not None
                ).items()
            )
        )
        if any(disposition != "ACCEPTED" for disposition in dispositions.values()):
            raise IntegrityError("micro successor one or more groups failed certification")
        cross_roll_certificates = [
            cross_interval_roll_certificate(market=market, groups=groups)
            for market in TIER_1_MARKETS
        ]
        if any(
            certificate["roll_continuity_certified"] is not True
            for certificate in cross_roll_certificates
        ):
            raise IntegrityError("micro successor cross-interval roll continuity failed")
        m6e_certificate = groups[("M6E", 2018, "2018-01-01_2019-01-01")][
            "definition"
        ].definition_repeat_certificate
        predecessor_result = _object(root / DUPLICATE_REPORT_PATH, "definition diagnostic report")["result"]
        if (
            not isinstance(m6e_certificate, Mapping)
            or m6e_certificate.get("legacy_repeat_count") != predecessor_result.get("legacy_repeat_count")
            or m6e_certificate.get("exact_semantic_duplicate_count")
            != predecessor_result.get("exact_semantic_duplicate_count")
            or m6e_certificate.get("distinct_same_key_update_count")
            != predecessor_result.get("distinct_same_key_update_count")
        ):
            raise IntegrityError("micro successor M6E diagnostic reconstruction drifted")

        phase2_items = [dict(item) for item in plan["phase2"]]

        def materialize(item: dict[str, object]) -> CausalResult:
            key = (str(item["market"]), int(item["year"]), str(item["interval"]))
            source = group_sources[key]["ohlcv-1m"]
            source_path = contained_path(root, str(source["relative_path"]))
            if sha256_file(source_path) != source["sha256"]:
                raise IntegrityError("micro successor one-minute source drifted before materialization")
            output_path = contained_path(staging, str(item["phase2_output_path"]))
            result = materialize_causal_1m_inactive(
                source_path=source_path,
                output_path=output_path,
                identity_certified=True,
                created_byte_budget=created_budget,
                deadline=deadline,
                clock=clock,
            )
            if sha256_file(source_path) != source["sha256"]:
                raise IntegrityError("micro successor one-minute source drifted after materialization")
            return result

        _bounded_parallel(
            items=phase2_items,
            worker=materialize,
            key=lambda item: str(item["phase2_release_id"]),
            result_sink=phase2_results,
        )
        if len(phase2_results) != EXPECTED_INTERVAL_COUNT:
            raise IntegrityError("micro successor Phase 2 output count drifted")
        if created_budget.used > MAXIMUM_OUTPUT_BYTES:
            raise UnauthorizedOperation("micro successor created-byte ceiling exceeded")
        for source in source_items:
            if clock() > deadline:
                raise TimeoutError("micro successor final source verification deadline reached")
            path = contained_path(root, str(source["relative_path"]))
            if sha256_file(path) != source["sha256"]:
                raise IntegrityError("micro successor Phase 1B hash drifted after construction")
            source_hashes_verified_after += 1
        scan_receipts = {
            request_id: {
                **_serialize_result(
                    root,
                    item=source_by_request[request_id]["execution_item"],
                    result=scan.result,
                ),
                "definition_repeat_certificate_id": (
                    scan.definition_repeat_certificate or {}
                ).get("definition_repeat_certificate_id"),
            }
            for request_id, scan in scans.items()
        }
        for cell in plan["coverage"]:
            if cell["planned_disposition"] == "PRODUCT_NOT_YET_EFFECTIVE":
                record = {
                    **dict(cell),
                    "disposition": "PRODUCT_NOT_YET_EFFECTIVE",
                    "decode_record_id": None,
                    "definition_repeat_certificate_id": None,
                }
            else:
                request_id = str(cell["source_request_id"])
                receipt = scan_receipts[request_id]
                record = {
                    **dict(cell),
                    "disposition": "ACCEPTED",
                    "decode_record_id": receipt["decode_record_id"],
                    "definition_repeat_certificate_id": receipt[
                        "definition_repeat_certificate_id"
                    ],
                }
            coverage.append(record)
        disposition_counts = dict(sorted(Counter(item["disposition"] for item in coverage).items()))
        if (
            len(coverage) != EXPECTED_COVERAGE_CELL_COUNT
            or disposition_counts != {"ACCEPTED": 120, "PRODUCT_NOT_YET_EFFECTIVE": 20}
        ):
            raise IntegrityError("micro successor coverage disposition census drifted")
        coverage_core = {
            "schema_version": "apex_micro_phase1b2_coverage_census/2.0.0",
            "cell_count": len(coverage),
            "disposition_counts": disposition_counts,
            "cells": coverage,
        }
        coverage_census = {
            **coverage_core,
            "coverage_census_id": sha256_json(coverage_core),
        }
        definition_certificates = sorted(
            (
                scan.definition_repeat_certificate
                for scan in scans.values()
                if scan.definition_repeat_certificate is not None
            ),
            key=lambda item: (str(item["market"]), int(item["year"])),
        )
        if len(definition_certificates) != EXPECTED_INTERVAL_COUNT or any(
            item["classification"]
            not in {"NO_LEGACY_REPEATS", "EXACT_SEMANTIC_DUPLICATES_PRESERVED"}
            for item in definition_certificates
        ):
            raise IntegrityError("micro successor definition certificate census drifted")
        phase2_records: list[dict[str, object]] = []
        phase2_by_id = {str(item["phase2_release_id"]): item for item in phase2_items}
        for release_id, result in sorted(phase2_results.items()):
            item = phase2_by_id[release_id]
            core = {
                "market": item["market"],
                "year": item["year"],
                "interval": item["interval"],
                "phase2_release_id": release_id,
                "identity_and_economics_certified": True,
                "roll_continuity_certified": True,
                **_causal_public(root, result),
            }
            phase2_records.append({**core, "phase2_record_id": sha256_json(core)})
        decode_records = [scan_receipts[key] for key in sorted(scan_receipts)]
        report_core: dict[str, object] = {
            "schema_version": REPORT_SCHEMA,
            "state": "CERTIFIED_INACTIVE_NOT_PUBLISHED",
            "plan_id": plan["plan_id"],
            "authorization_receipt_id": authorization.receipt_id,
            "authorization_use_path": use_path.relative_to(root).as_posix(),
            "source_count": len(scans),
            "source_bytes": plan["source_bytes"],
            "source_hashes_verified_before_and_after": source_hashes_verified_after,
            "decode_records": decode_records,
            "definition_repeat_certificates": definition_certificates,
            "definition_repeat_classification_counts": definition_repeat_classification_counts,
            "definition_rows_deduplicated": 0,
            "group_disposition_counts": group_disposition_counts,
            "cross_interval_roll_certificates": cross_roll_certificates,
            "phase2_records": phase2_records,
            "coverage_census": coverage_census,
            "identity_and_roll_continuity_certified": True,
            "catalog_candidate_eligible": True,
            "status_and_statistics_used_as_features": False,
            "one_second_evidence_semantics": "REPORTED_TRADE_BARS_ONLY",
            "one_second_bbo_queue_guaranteed_fill_or_within_second_ordering_claimed": False,
            "year_2025_or_2026_materialized": False,
            "provider_calls": 0,
            "external_cost_usd": "0",
            "raw_values_or_semantic_keys_reported": False,
            "features_outcomes_predictions_returns_or_evaluation_created": False,
            "published": False,
            "catalog_or_pointer_activated": False,
        }
        report = {
            **report_core,
            "source_certification_id": sha256_json(report_core),
        }
        report_path = contained_path(root, str(plan["outputs"]["source_certification_report"]))
        _write_create_only(report_path, report)
        report_id = str(report["source_certification_id"])
        report_sha256 = sha256_file(report_path)
        aggregate_phase1b_sha = sha256_json(
            sorted(str(scan.result.output_sha256) for scan in scans.values())
        )
        aggregate_phase2_sha = sha256_json(
            sorted(str(result.output_sha256) for result in phase2_results.values())
        )
        candidate_base = {
            "lane_id": LANE_ID,
            "contract_scale": "MICRO_INTEGER_ONLY",
            "state": "CERTIFIED_INACTIVE_NOT_PUBLISHED",
            "source_certification_id": report_id,
            "source_certification_sha256": report_sha256,
            "coverage_census_id": coverage_census["coverage_census_id"],
            "coverage_cell_count": len(coverage),
            "phase1b_release_id": sha256_json(
                sorted(str(item["phase1b_release_id"]) for item in source_items)
            ),
            "phase1b_release_sha256": aggregate_phase1b_sha,
            "phase2_release_id": sha256_json(
                sorted(str(item["phase2_release_id"]) for item in phase2_items)
            ),
            "phase2_release_sha256": aggregate_phase2_sha,
            "markets": list(TIER_1_MARKETS),
            "years": list(ELIGIBLE_YEARS),
            "disposition_census_complete": True,
            "actual_identity_and_roll_continuity_certified": True,
            "holdout_2025_materialized": False,
            "forward_2026_materialized": False,
        }
        require_row_certified_catalog_candidate(candidate_base)
        candidate_core = {
            "schema_version": CATALOG_SCHEMA,
            "future_active_path": ACTIVE_MICRO_CATALOG_PATH.as_posix(),
            "published": False,
            "active_pointer_written": False,
            **candidate_base,
        }
        candidate = {
            **candidate_core,
            "catalog_candidate_id": sha256_json(candidate_core),
        }
        candidate_path = contained_path(root, str(plan["outputs"]["inactive_catalog_candidate"]))
        _write_create_only(candidate_path, candidate)
        candidate_id = str(candidate["catalog_candidate_id"])
        state = "SUCCESS_CERTIFIED_INACTIVE_PHASE2"
    except Exception as exc:
        failure = exc
        state = "FAILURE_INACTIVE_PARTIAL_EVIDENCE_PRESERVED"
        failure_core = {
            "schema_version": FAILURE_SCHEMA,
            "state": state,
            "plan_id": plan["plan_id"],
            "authorization_receipt_id": authorization.receipt_id,
            "authorization_use_path": use_path.relative_to(root).as_posix(),
            "failure_type": type(exc).__name__,
            "completed_source_scan_count": len(scans),
            "completed_phase2_count": len(phase2_results),
            "group_disposition_counts": group_disposition_counts,
            "definition_repeat_classification_counts": definition_repeat_classification_counts,
            "created_output_bytes": created_budget.used,
            "attempts": 1,
            "automatic_retries": 0,
            "provider_calls": 0,
            "external_cost_usd": "0",
            "dbn_payloads_opened": 0,
            "year_2025_or_2026_payloads_opened": 0,
            "raw_values_or_semantic_keys_reported": False,
            "published_activated_registered_evaluated_or_traded": False,
        }
        failure_report = {**failure_core, "failure_report_id": sha256_json(failure_core)}
        _write_create_only(
            contained_path(root, str(plan["outputs"]["failure_report"])),
            failure_report,
        )
    _seal_tree(staging)
    _seal_tree(evidence)
    terminal_core = {
        "schema_version": TERMINAL_SCHEMA,
        "state": state,
        "plan_id": plan["plan_id"],
        "audit_id": audit["audit_id"],
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "completed_source_scan_count": len(scans),
        "completed_phase2_count": len(phase2_results),
        "source_hashes_verified_after_construction": source_hashes_verified_after,
        "group_disposition_counts": group_disposition_counts,
        "definition_repeat_classification_counts": definition_repeat_classification_counts,
        "source_certification_id": report_id,
        "source_certification_sha256": report_sha256,
        "inactive_catalog_candidate_id": candidate_id,
        "created_output_bytes": created_budget.used,
        "failure_type": None if failure is None else type(failure).__name__,
        "attempts": 1,
        "automatic_retries": 0,
        "provider_calls": 0,
        "external_cost_usd": "0",
        "dbn_payloads_opened": 0,
        "year_2025_or_2026_payloads_opened": 0,
        "raw_values_or_semantic_keys_reported": False,
        "published_activated_registered_evaluated_or_traded": False,
        "catalog_or_pointer_activated": False,
        "terminal_written_last": True,
    }
    terminal = {**terminal_core, "terminal_id": sha256_json(terminal_core)}
    _write_create_only(contained_path(root, str(plan["outputs"]["terminal"])), terminal)
    return terminal


__all__ = [
    "AUDIT_PATH",
    "OPERATION",
    "PLAN_PATH",
    "Phase1BScan",
    "build_audit",
    "build_plan",
    "execute_once",
    "load_plan",
    "reconstruct_phase1b_scan",
    "required_scope",
    "successor_group_disposition",
    "write_audit_create_only",
    "write_plan_create_only",
]
