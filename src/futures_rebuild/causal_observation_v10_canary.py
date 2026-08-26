"""Bounded production-faithful ES-2025 gate for the V10 market campaign.

The canary performs exactly one producer decode and one separately implemented
raw-source replay.  Its inactive bytes are evidence only: they cannot complete,
seed, or be reused by the complete ES market checkpoint.
"""

from __future__ import annotations

import json
import shutil
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path

import databento

from .boundary import OperationReceipt, RepoBoundary
from .canonical import io_path, sha256_file, sha256_json
from .causal_full_build_durable_host import (
    expected_durable_host_plan,
    validate_active_durable_host_evidence,
    validate_durable_host_plan,
)
from .causal_observation_foundation import (
    CAUSAL_OBSERVATION_CONTRACT_ID,
    ECONOMICS_RULEBOOK_PATH,
    ECONOMICS_RULEBOOK_SHA256,
    authorize_v10_es_2025_canary_row_read,
)
from .causal_observation_full_build import (
    BOUNDARY_SOURCE_FAMILIES,
    ES_2025_CANARY_EXECUTION_ROLE,
    PINNED_PYTHON_EXECUTABLE,
    _contained,
    _execute,
    _json,
    _load_economics_rulebook,
    _market_windows,
    _write_create_only,
    validate_complete_development_boundary_metadata,
)
from .causal_observation_market_certification import _run_replay_in_fresh_process
from .causal_observation_market_checkpoint import (
    OUTPUT_ROOT,
    _validate_checkpoint_set,
)
from .causal_source_closure import select_exact_standard_source_entries
from .errors import IntegrityError, UnauthorizedOperation
from .foundation.decoder import SUPPORTED_DATABENTO_VERSION
from .foundation_operation_firewall import issue_current_source_closure_context
from .locking import FileLease
from .research_gateway_policy import CAUSAL_OBSERVATION_V10_CANARY_OPERATION


PLAN_SCHEMA = "development_causal_observation_v10_es_2025_canary_plan/1.0.0"
RESULT_SCHEMA = "development_causal_observation_v10_es_2025_canary_result/1.0.0"
FAILURE_SCHEMA = "development_causal_observation_v10_es_2025_canary_failure/1.0.0"
TERMINAL_STATUS = "PASS_V10_ES_2025_CANARY_VERIFIED_INACTIVE"
START = "2025-01-01T00:00:00Z"
END = "2025-07-13T22:00:00Z"
DBN_COUNT = 7
SIDECAR_COUNT = 7
SOURCE_BYTES = 69_984_372
PAYLOAD_BYTES_PER_DECODE = 69_971_994
TOTAL_DECODE_BYTE_CEILING = 139_943_988
MAXIMUM_PARTITIONS = 7
MAXIMUM_OUTPUT_BYTES = 800_000_000
MAXIMUM_RUNTIME_SECONDS = 21_600
MAXIMUM_DECODED_RECORDS = 100_000_000
REQUIRED_CANARY_BINDINGS = frozenset(
    {
        "scripts/start_causal_full_build_v10_worker.ps1",
        "src/futures_rebuild/causal_observation_full_build.py",
        "src/futures_rebuild/causal_observation_market_certification.py",
        "src/futures_rebuild/causal_observation_v10_canary.py",
        "src/futures_rebuild/causal_observation_verifier.py",
    }
)


def _validate_entry_set(
    entries: Sequence[Mapping[str, object]],
    *,
    source: Mapping[str, object] | None = None,
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    selected = tuple(sorted((dict(item) for item in entries), key=lambda item: str(item["path"])))
    dbns = tuple(item for item in selected if item.get("kind") == "DBN")
    sidecars = tuple(item for item in selected if item.get("kind") == "SIDECAR")
    expected_common = {
        "market": "ES",
        "year": 2025,
        "interval_start_inclusive": START,
        "interval_end_exclusive": END,
        "lane": "STANDARD_41",
        "admitted_standard_foundation": True,
    }
    if (
        len(selected) != DBN_COUNT + SIDECAR_COUNT
        or len(dbns) != DBN_COUNT
        or len(sidecars) != SIDECAR_COUNT
        or {str(item.get("family")) for item in dbns} != BOUNDARY_SOURCE_FAMILIES
        or {str(item.get("family")) for item in sidecars} != BOUNDARY_SOURCE_FAMILIES
        or any(any(item.get(key) != value for key, value in expected_common.items()) for item in selected)
        or sum(int(item["size_bytes"]) for item in selected) != SOURCE_BYTES
        or sum(int(item["size_bytes"]) for item in dbns) != PAYLOAD_BYTES_PER_DECODE
        or (
            source is not None
            and (
                sha256_json(selected) != source.get("exact_source_entries_sha256")
                or sha256_json(dbns) != source.get("exact_dbn_entries_sha256")
            )
        )
    ):
        raise IntegrityError("V10 canary source scope is not exact ES-2025")
    return selected, dbns


def v10_es_2025_inventory_document(repository_root: Path) -> dict[str, object]:
    """Select the seven registered DBN/sidecar pairs from active metadata only."""

    root = repository_root.resolve(strict=True)
    source_contract = _json(root / "configs/source_contract.json")
    inventory_path = _contained(root, source_contract["complete_inventory"]["path"])
    inventory = _json(inventory_path)
    entries = inventory.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active source inventory entries are absent")
    selected = tuple(
        dict(item)
        for item in entries
        if item.get("market") == "ES"
        and item.get("year") == 2025
        and item.get("admitted_standard_foundation") is True
    )
    selected, dbns = _validate_entry_set(selected)
    core = {
        "schema_version": "development_causal_observation_v10_es_2025_inventory/1.0.0",
        "source_contract_id": source_contract["contract_id"],
        "canonical_release_id": source_contract["active_canonical_source"]["release_id"],
        "entries": list(selected),
        "exact_source_entries_sha256": sha256_json(selected),
        "exact_dbn_entries_sha256": sha256_json(dbns),
        "source_entry_count": 14,
        "dbn_file_count": 7,
        "sidecar_file_count": 7,
        "total_source_bytes": SOURCE_BYTES,
        "payload_bytes_per_decode": PAYLOAD_BYTES_PER_DECODE,
        "payload_bytes_two_decodes_ceiling": TOTAL_DECODE_BYTE_CEILING,
        "payload_files_opened": 0,
        "rows_read": 0,
    }
    return {**core, "inventory_id": sha256_json(core)}


def build_v10_es_2025_canary_plan(
    *,
    repository_root: Path,
    inventory_path: Path,
    attempt_id: str,
    checkpoint_set: Mapping[str, object],
) -> dict[str, object]:
    """Build a nonauthorizing plan from one already-written 14-entry inventory."""

    root = repository_root.resolve(strict=True)
    inventory_path = inventory_path.resolve(strict=True)
    inventory_path.relative_to(root)
    inventory = _json(inventory_path)
    entries = inventory.get("entries")
    if not isinstance(entries, list) or any(not isinstance(item, Mapping) for item in entries):
        raise IntegrityError("V10 canary inventory entries are absent")
    selected, dbns = _validate_entry_set(entries)
    source_contract = _json(root / "configs/source_contract.json")
    checkpoint = dict(checkpoint_set)
    checkpoint_set_id = sha256_json(checkpoint)
    core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "operation": CAUSAL_OBSERVATION_V10_CANARY_OPERATION,
        "execution_role": ES_2025_CANARY_EXECUTION_ROLE,
        "target_market": "ES",
        "target_year": 2025,
        "attempt_id": attempt_id,
        "checkpoint_set": checkpoint,
        "checkpoint_set_id": checkpoint_set_id,
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source": {
            "source_contract_id": source_contract["contract_id"],
            "canonical_release_id": source_contract["active_canonical_source"]["release_id"],
            "inventory_path": inventory_path.relative_to(root).as_posix(),
            "inventory_sha256": sha256_file(inventory_path),
            "exact_source_entries_sha256": sha256_json(selected),
            "exact_dbn_entries_sha256": sha256_json(dbns),
            "exact_source_entry_count": 14,
            "exact_dbn_file_count": DBN_COUNT,
            "exact_sidecar_file_count": SIDECAR_COUNT,
            "total_source_bytes": SOURCE_BYTES,
            "maximum_payload_bytes": PAYLOAD_BYTES_PER_DECODE,
            "work_unit_count": 1,
        },
        "output_staging_path": f"{OUTPUT_ROOT}/_canary/ES/{attempt_id}",
        "development_start_inclusive": START,
        "development_end_exclusive": END,
        "holdout_allowed": False,
        "forward_allowed": False,
        "provider_calls": 0,
        "execution_authorized": False,
        "complete_market_checkpoint": False,
        "reusable_in_same_checkpoint_set": False,
        "can_seed_complete_market_checkpoint": False,
        "authority": {name: False for name in (
            "activation", "evaluation", "features", "fitting", "forward", "holdout",
            "mechanism", "outcomes", "prediction", "provider", "publication", "wfa",
        )},
        "limits": {
            "maximum_payload_bytes": PAYLOAD_BYTES_PER_DECODE,
            "maximum_payload_bytes_per_decode": PAYLOAD_BYTES_PER_DECODE,
            "maximum_payload_bytes_total": TOTAL_DECODE_BYTE_CEILING,
            "maximum_decoded_records": MAXIMUM_DECODED_RECORDS,
            "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
            "maximum_partition_count": MAXIMUM_PARTITIONS,
        },
        "execution": {
            "producer_decodes": 1,
            "independent_replay_decodes": 1,
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_workers": 1,
            "python_executable": PINNED_PYTHON_EXECUTABLE,
            "databento_version": SUPPORTED_DATABENTO_VERSION,
        },
        "durable_host": expected_durable_host_plan("ES", attempt_id),
        "task_cleanup": {
            "task_name": expected_durable_host_plan("ES", attempt_id)["task_name"],
            "unregister_after_terminal_evidence": True,
            "unregister_before_terminal_evidence": False,
        },
        "economics": {
            "rulebook_path": ECONOMICS_RULEBOOK_PATH,
            "rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        },
        "canary_implementation_bindings": {
            relative: sha256_file(root / relative)
            for relative in REQUIRED_CANARY_BINDINGS
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def _load_entries(root: Path, plan: Mapping[str, object]) -> tuple[dict[str, object], ...]:
    source = plan["source"]
    inventory_path = _contained(root, source["inventory_path"])
    if sha256_file(inventory_path) != source["inventory_sha256"]:
        raise IntegrityError("V10 canary inventory differs")
    inventory = _json(inventory_path)
    entries = inventory.get("entries")
    if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
        raise IntegrityError("V10 canary inventory entries are absent")
    selected, _ = _validate_entry_set(entries, source=source)
    for item in selected:
        path = _contained(root, item["path"])
        if io_path(path).stat().st_size != item["size_bytes"]:
            raise IntegrityError("V10 canary source file identity differs")
        if (
            item["kind"] == "SIDECAR"
            and sha256_file(path, reject_hardlinks=False) != item["sha256"]
        ):
            raise IntegrityError("V10 canary sidecar identity differs")
    return selected


def validate_v10_es_2025_canary_plan(root: Path, plan: Mapping[str, object]) -> None:
    source = plan.get("source")
    limits = plan.get("limits")
    execution = plan.get("execution")
    authority = plan.get("authority")
    bindings = plan.get("canary_implementation_bindings")
    attempt = plan.get("attempt_id")
    if (
        plan.get("schema_version") != PLAN_SCHEMA
        or plan.get("operation") != CAUSAL_OBSERVATION_V10_CANARY_OPERATION
        or plan.get("execution_role") != ES_2025_CANARY_EXECUTION_ROLE
        or plan.get("target_market") != "ES"
        or plan.get("target_year") != 2025
        or type(attempt) is not str
        or plan.get("causal_contract_id") != CAUSAL_OBSERVATION_CONTRACT_ID
        or not isinstance(source, Mapping)
        or not isinstance(limits, Mapping)
        or not isinstance(execution, Mapping)
        or not isinstance(authority, Mapping)
        or not isinstance(bindings, Mapping)
        or set(bindings) != REQUIRED_CANARY_BINDINGS
        or any(bool(value) for value in authority.values())
        or plan.get("output_staging_path") != f"{OUTPUT_ROOT}/_canary/ES/{attempt}"
        or plan.get("development_start_inclusive") != START
        or plan.get("development_end_exclusive") != END
        or plan.get("holdout_allowed") is not False
        or plan.get("forward_allowed") is not False
        or plan.get("provider_calls") != 0
        or plan.get("execution_authorized") is not False
        or plan.get("complete_market_checkpoint") is not False
        or plan.get("reusable_in_same_checkpoint_set") is not False
        or plan.get("can_seed_complete_market_checkpoint") is not False
        or source.get("exact_source_entry_count") != 14
        or source.get("exact_dbn_file_count") != DBN_COUNT
        or source.get("exact_sidecar_file_count") != SIDECAR_COUNT
        or source.get("total_source_bytes") != SOURCE_BYTES
        or source.get("maximum_payload_bytes") != PAYLOAD_BYTES_PER_DECODE
        or source.get("work_unit_count") != 1
        or limits
        != {
            "maximum_payload_bytes": PAYLOAD_BYTES_PER_DECODE,
            "maximum_payload_bytes_per_decode": PAYLOAD_BYTES_PER_DECODE,
            "maximum_payload_bytes_total": TOTAL_DECODE_BYTE_CEILING,
            "maximum_decoded_records": MAXIMUM_DECODED_RECORDS,
            "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
            "maximum_partition_count": MAXIMUM_PARTITIONS,
        }
        or execution
        != {
            "producer_decodes": 1,
            "independent_replay_decodes": 1,
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_workers": 1,
            "python_executable": PINNED_PYTHON_EXECUTABLE,
            "databento_version": SUPPORTED_DATABENTO_VERSION,
        }
        or plan.get("economics")
        != {
            "rulebook_path": ECONOMICS_RULEBOOK_PATH,
            "rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        }
        or plan.get("task_cleanup")
        != {
            "task_name": expected_durable_host_plan("ES", str(attempt))["task_name"],
            "unregister_after_terminal_evidence": True,
            "unregister_before_terminal_evidence": False,
        }
    ):
        raise UnauthorizedOperation("V10 ES-2025 canary plan is not exact")
    _validate_checkpoint_set(root, plan)
    validate_durable_host_plan(root, plan)
    for relative, expected in bindings.items():
        if sha256_file(_contained(root, relative)) != expected:
            raise IntegrityError("V10 canary implementation binding differs")
    if sha256_json({key: value for key, value in plan.items() if key != "plan_id"}) != plan.get("plan_id"):
        raise IntegrityError("V10 ES-2025 canary plan identity differs")


def validate_v10_es_2025_canary_execution_environment(
    root: Path, plan: Mapping[str, object]
) -> None:
    expected = (root / PINNED_PYTHON_EXECUTABLE).resolve(strict=True)
    if Path(sys.executable).resolve(strict=False) != expected or databento.__version__ != SUPPORTED_DATABENTO_VERSION:
        raise UnauthorizedOperation("V10 canary requires the pinned decoder runtime")
    validate_active_durable_host_evidence(root, plan)


def _replay_plan(plan_path: Path, root: Path, plan: Mapping[str, object]) -> dict[str, object]:
    return {
        "target_market": "ES",
        "attempt_id": plan["attempt_id"],
        "checkpoint_set_id": plan["checkpoint_set_id"],
        "causal_contract_id": plan["causal_contract_id"],
        "build_plan_path": plan_path.relative_to(root).as_posix(),
        "build_plan_sha256": sha256_file(plan_path),
        "source": dict(plan["source"]),
    }


def run_authorized_v10_es_2025_canary(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path
) -> dict[str, object]:
    """Consume one receipt, produce inactive bytes, and independently replay once."""

    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan_path = plan_path.resolve(strict=True)
    plan_path.relative_to(root)
    plan = _json(plan_path)
    validate_v10_es_2025_canary_plan(root, plan)
    selected = _load_entries(root, plan)
    validate_complete_development_boundary_metadata(root, selected, standard_roots=frozenset({"ES"}))
    selected = select_exact_standard_source_entries(
        root,
        operation_context=issue_current_source_closure_context(root),
        source_entries=selected,
        windows=_market_windows(selected),
    )
    output = _contained(root, plan["output_staging_path"])
    if output.exists():
        raise IntegrityError("V10 canary output already exists")
    if shutil.disk_usage(root).free < MAXIMUM_OUTPUT_BYTES + 100 * 1024**3:
        raise UnauthorizedOperation("V10 canary storage floor is not met")
    global_lock = root / "state/locks/foundation-build.lock"
    run_lock = root / f"state/locks/causal-observation-v10-canary-{plan['plan_id']}.lock"
    if global_lock.exists() or run_lock.exists():
        raise UnauthorizedOperation("V10 canary writer lock is active")
    progress: dict[str, object] = {
        "current_market": "ES",
        "current_year": 2025,
        "current_work_unit_dbn_count": 0,
        "current_work_unit_dbn_bytes": 0,
        "current_work_unit_decode_state": "NOT_STARTED",
        "current_work_unit_state": "NOT_STARTED",
        "dbn_files_opened": 0,
        "dbn_payload_bytes_opened": 0,
        "dbn_paths_opened": [],
        "decoded_record_count": 0,
        "complete_work_unit_count": 0,
        "complete_partition_count": 0,
        "output_bytes": 0,
    }
    with FileLease(global_lock), FileLease(run_lock):
        validate_v10_es_2025_canary_execution_environment(root, plan)
        context = authorize_v10_es_2025_canary_row_read(
            boundary=boundary,
            receipt=receipt,
            plan=plan,
            plan_sha256=sha256_file(plan_path),
        )
        started = time.monotonic()
        try:
            producer = _execute(
                root=root,
                boundary=boundary,
                plan=plan,
                plan_sha256=sha256_file(plan_path),
                context=context,
                selected=selected,
                economics_rulebook=_load_economics_rulebook(root),
                progress=progress,
                started=started,
            )
            if (
                producer.payload.get("status") != "PASS_V10_ES_2025_CANARY_PRODUCER_INACTIVE"
                or producer.payload.get("complete_market_checkpoint") is not False
                or producer.payload.get("reusable_in_same_checkpoint_set") is not False
                or producer.payload.get("partition_count", 0) > MAXIMUM_PARTITIONS
            ):
                raise IntegrityError("V10 canary producer terminal is invalid")
            remaining = MAXIMUM_RUNTIME_SECONDS - (time.monotonic() - started)
            if remaining <= 0:
                raise UnauthorizedOperation("V10 canary runtime ceiling exceeded before replay")
            replay = _run_replay_in_fresh_process(
                root,
                _replay_plan(plan_path, root, plan),
                producer.payload,
                timeout_seconds=remaining,
            )
            if time.monotonic() - started > MAXIMUM_RUNTIME_SECONDS:
                raise UnauthorizedOperation("V10 canary total runtime ceiling exceeded")
            core = {
                "schema_version": RESULT_SCHEMA,
                "status": TERMINAL_STATUS,
                "plan_id": plan["plan_id"],
                "attempt_id": plan["attempt_id"],
                "checkpoint_set_id": plan["checkpoint_set_id"],
                "target_market": "ES",
                "target_year": 2025,
                "producer_result_id": producer.payload["result_id"],
                "producer_result_sha256": sha256_file(producer.result_path),
                "independent_replay_evidence": replay.as_dict(),
                "independent_replay_evidence_id": replay.evidence_id,
                "authorized_decode_count": 2,
                "authorized_decode_byte_ceiling": TOTAL_DECODE_BYTE_CEILING,
                "complete_market_checkpoint": False,
                "reusable_in_same_checkpoint_set": False,
                "can_seed_complete_market_checkpoint": False,
                "campaign_advancement_eligible": True,
                "provider_calls": 0,
                "holdout_rows": 0,
                "forward_rows": 0,
                "outcomes": 0,
                "features": 0,
                "wfa": 0,
                "fitting": 0,
                "predictions": 0,
                "evaluations": 0,
                "publication_authorized": False,
                "activation_authorized": False,
            }
            result = {**core, "result_id": sha256_json(core)}
            _write_create_only(output / "canary_result.json", result)
            return result
        except (Exception, KeyboardInterrupt) as exc:
            io_path(output).mkdir(parents=True, exist_ok=True)
            failure = output / "failure.json"
            if not failure.exists():
                core = {
                    "schema_version": FAILURE_SCHEMA,
                    "status": "FAILED_V10_ES_2025_CANARY_TERMINAL",
                    "plan_id": plan["plan_id"],
                    "attempt_id": plan["attempt_id"],
                    "receipt_id": context.receipt_id,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "progress": progress,
                    "campaign_advancement_eligible": False,
                    "automatic_retry_authorized": False,
                    "partition_reuse_authorized": False,
                    "complete_market_checkpoint": False,
                    "publication_authorized": False,
                    "activation_authorized": False,
                }
                _write_create_only(failure, {**core, "failure_id": sha256_json(core)})
            raise
