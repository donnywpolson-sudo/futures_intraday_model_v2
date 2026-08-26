"""V10 complete-market checkpoints for the development causal foundation."""

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
    V10_CHECKPOINT_MARKETS,
    authorize_market_checkpoint_row_read,
)
from .causal_observation_full_build import (
    COMPLETE_MARKET_EXECUTION_ROLE,
    MAXIMUM_OUTPUT_BYTES,
    MAXIMUM_PARTITION_COUNT,
    MAXIMUM_RUNTIME_SECONDS,
    PINNED_PYTHON_EXECUTABLE,
    _contained,
    _execute,
    _json,
    _load_economics_rulebook,
    _market_windows,
    _validate_v10_reuse_binding_shape,
    _write_create_only,
    validate_complete_development_boundary_metadata,
    validate_full_build_storage_floor,
)
from .causal_source_closure import select_exact_standard_source_entries
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation.decoder import SUPPORTED_DATABENTO_VERSION
from .foundation_operation_firewall import issue_current_source_closure_context
from .locking import FileLease


PLAN_SCHEMA = "development_causal_observation_market_checkpoint_plan/1.1.0"
CHECKPOINT_SET_SCHEMA = "development_causal_observation_checkpoint_set/1.1.0"
CHECKPOINT_SET_CERTIFICATE_SCHEMA = (
    "development_causal_observation_checkpoint_set_certificate/1.0.0"
)
OUTPUT_ROOT = (
    "data/causally_gated_normalized/v10"
)
MARKET_ORDER = V10_CHECKPOINT_MARKETS
CHECKPOINT_SET_REQUIRED_BINDINGS = frozenset(
    {
        "scripts/start_causal_full_build_v10_worker.ps1",
        "src/futures_rebuild/canonical.py",
        "src/futures_rebuild/causal_full_build_durable_host.py",
        "src/futures_rebuild/causal_observation_foundation.py",
        "src/futures_rebuild/causal_observation_full_build.py",
        "src/futures_rebuild/causal_observation_market_checkpoint.py",
        "src/futures_rebuild/causal_observation_parquet.py",
        "src/futures_rebuild/causal_observation_verifier.py",
        "src/futures_rebuild/data_layout.py",
    }
)


def checkpoint_set_identity(value: Mapping[str, object]) -> str:
    if value.get("schema_version") != CHECKPOINT_SET_SCHEMA:
        raise ContractError("market checkpoint-set schema differs")
    return sha256_json(dict(value))


def _validate_checkpoint_set(root: Path, plan: Mapping[str, object]) -> dict[str, object]:
    value = plan.get("checkpoint_set")
    if not isinstance(value, Mapping):
        raise UnauthorizedOperation("market checkpoint set is absent")
    checkpoint_set = dict(value)
    active = _json(root / "configs/source_contract.json")
    bindings = checkpoint_set.get("implementation_bindings")
    if (
        checkpoint_set.get("schema_version") != CHECKPOINT_SET_SCHEMA
        or checkpoint_set.get("market_order") != list(MARKET_ORDER)
        or set(MARKET_ORDER) != set(active["universe"]["standard_roots"])
        or checkpoint_set.get("source_contract_id") != active.get("contract_id")
        or checkpoint_set.get("canonical_release_id")
        != active["active_canonical_source"]["release_id"]
        or checkpoint_set.get("causal_contract_id") != CAUSAL_OBSERVATION_CONTRACT_ID
        or checkpoint_set.get("development_end_exclusive")
        != "2025-07-13T22:00:00Z"
        or checkpoint_set.get("writer_configuration")
        != {
            "format": "PARQUET",
            "compression": "ZSTD",
            "compression_level": 9,
            "partitioning": "market/year/month",
        }
        or not isinstance(bindings, Mapping)
        or not CHECKPOINT_SET_REQUIRED_BINDINGS.issubset(bindings)
        or plan.get("checkpoint_set_id") != checkpoint_set_identity(checkpoint_set)
    ):
        raise UnauthorizedOperation("market checkpoint set is not exact")
    for relative, expected in bindings.items():
        path = _contained(root, relative)
        if sha256_file(path) != expected:
            raise IntegrityError("market checkpoint implementation binding differs")
    return checkpoint_set


def validate_market_checkpoint_plan(root: Path, plan: Mapping[str, object]) -> None:
    market = plan.get("target_market")
    attempt_id = plan.get("attempt_id")
    source = plan.get("source")
    limits = plan.get("limits")
    execution = plan.get("execution")
    authority = plan.get("authority")
    economics = plan.get("economics")
    if (
        plan.get("schema_version") != PLAN_SCHEMA
        or plan.get("execution_role") != COMPLETE_MARKET_EXECUTION_ROLE
        or plan.get("operation") != "BUILD_DEVELOPMENT_CAUSAL_OBSERVATION_FOUNDATION_ONCE"
        or plan.get("causal_contract_id") != CAUSAL_OBSERVATION_CONTRACT_ID
        or market not in MARKET_ORDER
        or type(attempt_id) is not str
        or not isinstance(source, Mapping)
        or not isinstance(limits, Mapping)
        or not isinstance(execution, Mapping)
        or not isinstance(authority, Mapping)
        or any(bool(value) for value in authority.values())
        or economics
        != {
            "rulebook_path": ECONOMICS_RULEBOOK_PATH,
            "rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        }
        or plan.get("output_staging_path")
        != f"{OUTPUT_ROOT}/{plan.get('checkpoint_set_id')}/{market}/{attempt_id}"
        or plan.get("development_end_exclusive") != "2025-07-13T22:00:00Z"
        or plan.get("holdout_allowed") is not False
        or plan.get("forward_allowed") is not False
        or plan.get("provider_calls") != 0
        or plan.get("execution_authorized") is not False
        or execution
        != {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_workers": 1,
            "python_executable": PINNED_PYTHON_EXECUTABLE,
            "databento_version": SUPPORTED_DATABENTO_VERSION,
        }
        or type(limits.get("maximum_payload_bytes")) is not int
        or limits["maximum_payload_bytes"] != source.get("maximum_payload_bytes")
        or type(limits.get("maximum_decoded_records")) is not int
        or limits["maximum_decoded_records"] <= 0
        or type(limits.get("maximum_output_bytes")) is not int
        or not 0 < limits["maximum_output_bytes"] <= MAXIMUM_OUTPUT_BYTES
        or type(limits.get("maximum_partition_count")) is not int
        or not 0 < limits["maximum_partition_count"] <= MAXIMUM_PARTITION_COUNT
        or plan.get("reuse_failed_market_partitions") is not False
        or any(
            type(source.get(name)) is not int or int(source[name]) <= 0
            for name in (
                "exact_source_entry_count",
                "exact_dbn_file_count",
                "exact_sidecar_file_count",
                "total_source_bytes",
                "maximum_payload_bytes",
                "work_unit_count",
            )
        )
        or source.get("exact_source_entry_count")
        != source.get("exact_dbn_file_count") + source.get("exact_sidecar_file_count")
    ):
        raise UnauthorizedOperation("market checkpoint plan is not exact")
    _validate_v10_reuse_binding_shape(plan)
    _validate_checkpoint_set(root, plan)
    validate_durable_host_plan(root, plan)
    plan_id = plan.get("plan_id")
    if type(plan_id) is not str or sha256_json(
        {key: value for key, value in plan.items() if key != "plan_id"}
    ) != plan_id:
        raise IntegrityError("market checkpoint plan identity differs")
    inventory = _contained(root, source.get("inventory_path"))
    if sha256_file(inventory) != source.get("inventory_sha256"):
        raise IntegrityError("market checkpoint inventory differs")


def _load_market_entries(root: Path, plan: Mapping[str, object]) -> tuple[dict[str, object], ...]:
    source = plan["source"]
    inventory = _json(_contained(root, source["inventory_path"]))
    entries = inventory.get("entries")
    if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
        raise IntegrityError("market checkpoint inventory entries are absent")
    market = plan["target_market"]
    selected = tuple(
        sorted(
            (dict(item) for item in entries if item.get("market") == market),
            key=lambda item: str(item["path"]),
        )
    )
    dbns = tuple(item for item in selected if item.get("kind") == "DBN")
    if (
        len(selected) != source.get("exact_source_entry_count")
        or len(dbns) != source.get("exact_dbn_file_count")
        or len(selected) - len(dbns) != source.get("exact_sidecar_file_count")
        or sha256_json(selected) != source.get("exact_source_entries_sha256")
        or sha256_json(dbns) != source.get("exact_dbn_entries_sha256")
        or sum(int(item["size_bytes"]) for item in selected)
        != source.get("total_source_bytes")
        or sum(int(item["size_bytes"]) for item in dbns)
        != source.get("maximum_payload_bytes")
    ):
        raise IntegrityError("market checkpoint source identity or counts differ")
    return selected


def validate_market_checkpoint_execution_environment(
    root: Path, plan: Mapping[str, object]
) -> None:
    expected = (root / PINNED_PYTHON_EXECUTABLE).resolve(strict=True)
    if (
        Path(sys.executable).resolve(strict=False) != expected
        or databento.__version__ != SUPPORTED_DATABENTO_VERSION
    ):
        raise UnauthorizedOperation("market checkpoint requires the pinned decoder runtime")
    validate_active_durable_host_evidence(root, plan)


def run_authorized_market_checkpoint(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path
):
    """Consume one market receipt; another market can never be affected by failure."""

    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan_path = plan_path.resolve(strict=True)
    try:
        plan_path.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation("market checkpoint plan is outside the repository") from exc
    plan = _json(plan_path)
    plan_sha = sha256_file(plan_path)
    validate_market_checkpoint_plan(root, plan)
    selected = _load_market_entries(root, plan)
    market = str(plan["target_market"])
    validate_complete_development_boundary_metadata(
        root, selected, standard_roots=frozenset({market})
    )
    selected = select_exact_standard_source_entries(
        root,
        operation_context=issue_current_source_closure_context(root),
        source_entries=selected,
        windows=_market_windows(selected),
    )
    output = _contained(root, plan["output_staging_path"])
    if output.exists():
        raise IntegrityError("market checkpoint output already exists")
    validate_full_build_storage_floor(free_bytes=shutil.disk_usage(root).free)
    global_lock = root / "state/locks/foundation-build.lock"
    run_lock = root / f"state/locks/causal-observation-market-{market}-{plan['plan_id']}.lock"
    if global_lock.exists() or run_lock.exists():
        raise UnauthorizedOperation("market checkpoint writer lock is active")
    progress: dict[str, object] = {
        "current_market": market,
        "current_year": None,
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
        if output.exists():
            raise IntegrityError("market checkpoint output appeared after lock acquisition")
        validate_market_checkpoint_execution_environment(root, plan)
        economics_rulebook = _load_economics_rulebook(root)
        context = authorize_market_checkpoint_row_read(
            boundary=boundary, receipt=receipt, plan=plan, plan_sha256=plan_sha
        )
        try:
            result = _execute(
                root=root,
                boundary=boundary,
                plan=plan,
                plan_sha256=plan_sha,
                context=context,
                selected=selected,
                economics_rulebook=economics_rulebook,
                progress=progress,
                started=time.monotonic(),
            )
            partitions = result.payload.get("partitions")
            if (
                result.payload.get("status") != "PASS_COMPLETE_MARKET_CHECKPOINT_INACTIVE"
                or result.payload.get("target_market") != market
                or result.payload.get("complete_work_unit_count")
                != plan["source"]["work_unit_count"]
                or result.payload.get("source_entry_count")
                != plan["source"]["exact_source_entry_count"]
                or result.payload.get("dbn_file_count")
                != plan["source"]["exact_dbn_file_count"]
                or not isinstance(partitions, list)
                or not partitions
                or any(item.get("market") != market for item in partitions)
            ):
                raise IntegrityError("market checkpoint completion is not exact")
            return result
        except (Exception, KeyboardInterrupt) as exc:
            io_path(output).mkdir(parents=True, exist_ok=True)
            failure_path = output / "failure.json"
            if not failure_path.exists():
                sealed_count = int(progress["complete_work_unit_count"])
                failure = {
                    "schema_version": "development_causal_observation_market_checkpoint_failure/1.1.0",
                    "status": "FAILED_MARKET_TERMINAL_OTHER_CHECKPOINTS_UNAFFECTED",
                    "target_market": market,
                    "checkpoint_set_id": plan["checkpoint_set_id"],
                    "plan_id": plan["plan_id"],
                    "receipt_id": context.receipt_id,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "progress": progress,
                    "sealed_work_unit_count": sealed_count,
                    "sealed_work_units_reusable_if_all_bindings_match": (
                        sealed_count > 0
                    ),
                    "unsealed_work_unit_reuse_authorized": False,
                    "completed_other_market_checkpoints_affected": False,
                    "required_retry": "FRESH_RECEIPT_REMAINING_UNSEALED_WORK_ONLY",
                    "publication_authorized": False,
                    "activation_authorized": False,
                }
                _write_create_only(failure_path, failure)
            raise


def certify_complete_checkpoint_set(
    *, checkpoint_set: Mapping[str, object], market_results: Sequence[Mapping[str, object]]
) -> dict[str, object]:
    """Certify structural readiness only when all 41 immutable markets pass."""

    checkpoint_set_id = checkpoint_set_identity(checkpoint_set)
    by_market: dict[str, Mapping[str, object]] = {}
    for result in market_results:
        core = {key: value for key, value in result.items() if key != "result_id"}
        market = result.get("target_market")
        partitions = result.get("partitions")
        if (
            market not in MARKET_ORDER
            or market in by_market
            or result.get("result_id") != sha256_json(core)
            or result.get("status") != "PASS_COMPLETE_MARKET_CHECKPOINT_INACTIVE"
            or result.get("checkpoint_set_id") != checkpoint_set_id
            or type(result.get("attempt_id")) is not str
            or result.get("source_contract_id") != checkpoint_set.get("source_contract_id")
            or result.get("source_release_id")
            != checkpoint_set.get("canonical_release_id")
            or result.get("causal_contract_id") != checkpoint_set.get("causal_contract_id")
            or result.get("complete_market_checkpoint") is not True
            or result.get("reusable_in_same_checkpoint_set") is not True
            or result.get("provider_calls") != 0
            or result.get("holdout_rows") != 0
            or result.get("forward_rows") != 0
            or result.get("publication_authorized") is not False
            or result.get("activation_authorized") is not False
            or not isinstance(partitions, list)
            or not partitions
            or any(item.get("market") != market for item in partitions)
            or len(
                {
                    (item.get("year"), item.get("interval"))
                    for item in partitions
                }
            )
            != len(partitions)
            or any(result.get(name) != 0 for name in (
                "outcomes", "features", "wfa", "fitting", "predictions", "evaluations",
                "mechanism_executions",
            ))
        ):
            raise IntegrityError("market checkpoint result is invalid or incompatible")
        by_market[str(market)] = result
    if tuple(by_market) != MARKET_ORDER and set(by_market) != set(MARKET_ORDER):
        raise UnauthorizedOperation("complete checkpoint set lacks exact 41-market coverage")
    ordered = [str(by_market[market]["result_id"]) for market in MARKET_ORDER]
    core = {
        "schema_version": CHECKPOINT_SET_CERTIFICATE_SCHEMA,
        "status": "PASS_41_MARKET_CHECKPOINT_SET_INACTIVE",
        "checkpoint_set_id": checkpoint_set_id,
        "market_count": len(MARKET_ORDER),
        "market_order": list(MARKET_ORDER),
        "market_result_ids": ordered,
        "market_result_ids_sha256": sha256_json(ordered),
        "publication_authorized": False,
        "activation_authorized": False,
    }
    return {**core, "certificate_id": sha256_json(core)}
