"""Run the Gate-B causal rebuild with the existing producer and certifier."""

from __future__ import annotations

import argparse
import math
import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

from .annual_dbn_gate_b import verify_publication
from .boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
    _personal_approval_line,
)
from .canonical import io_path, iter_plain_files, sha256_file, sha256_json
from .causal_full_build_durable_host import expected_durable_host_plan
from .causal_observation_foundation import (
    CAUSAL_OBSERVATION_CONTRACT_ID,
    ECONOMICS_RULEBOOK_PATH,
    ECONOMICS_RULEBOOK_SHA256,
    required_full_build_scope,
)
from .causal_observation_full_build import (
    COMPLETE_MARKET_EXECUTION_ROLE,
    EXPECTED_DBN_COUNT,
    EXPECTED_ENTRY_COUNT,
    EXPECTED_PAYLOAD_BYTES,
    EXPECTED_PRIMARY_1M_DBN_COUNT,
    EXPECTED_SIDECAR_COUNT,
    EXPECTED_SOURCE_BYTES,
    EXPECTED_WORK_UNIT_COUNT,
    MAXIMUM_OUTPUT_BYTES,
    MAXIMUM_PARTITION_COUNT,
    MAXIMUM_PEAK_ADDITIONAL_BYTES,
    MAXIMUM_PROJECTED_RUNTIME_HIGH_SECONDS,
    MAXIMUM_RUNTIME_SECONDS,
    MINIMUM_FREE_AFTER_PEAK_BYTES,
    PINNED_PYTHON_EXECUTABLE,
    PLAN_SCHEMA,
    REMAINING_WORK_UNIT_ORDER,
    RESULT_SCHEMA,
    RUNTIME_PROJECTION_SCHEMA,
    STANDARD_ROOT_COUNT,
    DEFERRED_MICRO_COUNT,
    WORK_UNIT_PRIORITY_MARKETS,
    _json,
    _validate_plan,
    _write_create_only,
    run_authorized_full_build,
    validate_source_query_contracts,
)
from .causal_observation_market_certification import (
    bind_predecessor_market_certificate,
    build_market_certification_plan,
    certify_complete_market_certificate_set,
    required_market_certification_scope,
    run_authorized_market_certification,
)
from .causal_observation_market_checkpoint import (
    CHECKPOINT_SET_REQUIRED_BINDINGS,
    CHECKPOINT_SET_SCHEMA,
    MARKET_ORDER,
    OUTPUT_ROOT,
    checkpoint_set_identity,
    validate_market_checkpoint_plan,
)
from .errors import IntegrityError, UnauthorizedOperation
from .foundation.decoder import SUPPORTED_DATABENTO_VERSION
from .research_gateway_policy import CAUSAL_OBSERVATION_FULL_BUILD_OPERATION


GATE_B_PLAN = Path(
    "state/annual_dbn_gate_b_v1/"
    "8e2451867159ade2e355f802bddc7cca3cc4633a6db1d3d2aa499d00571fe45d/plan.json"
)
GATE_B_PUBLICATION = GATE_B_PLAN.parent / "publication_receipt.json"
STATE_ROOT = Path("state/annual_dbn_gate_b_causal_v1")
FULL_STAGE_ROOT = Path("state/data_publication_staging/annual_dbn_gate_b_causal_v1")
OLD_CERTIFICATE_ROOT = Path("state/causal_observation_market_certificates")
OLD_HOST_ROOT = Path(
    "state/causal_full_build_host/"
    "causal_observation_full_development_bounded_2025_v10"
)
IMPLEMENTATION_PATH = Path("src/futures_rebuild/annual_dbn_causal_rebuild.py")
LEGACY_RUNTIME_RECEIPT_ID = (
    "708733f6638be78f266bf6615731f250e9a410335e67a91324b9ee6f46f60689"
)


def _write(path: Path, payload: Mapping[str, object]) -> None:
    if path.exists():
        if _json(path) != dict(payload):
            raise IntegrityError(f"existing Gate-B causal artifact differs: {path}")
        return
    _write_create_only(path, payload)


def _projection(
    evidence: Sequence[Mapping[str, object]], *, total_work_units: int
) -> dict[str, object]:
    observed = sum(int(item["elapsed_seconds"]) for item in evidence)
    high = math.ceil(observed * 1.2)
    if (
        len(evidence) != len(MARKET_ORDER)
        or total_work_units != EXPECTED_WORK_UNIT_COUNT
        or high > MAXIMUM_PROJECTED_RUNTIME_HIGH_SECONDS
    ):
        raise UnauthorizedOperation("measured causal runtime does not fit the fixed ceiling")
    core: dict[str, object] = {
        "schema_version": RUNTIME_PROJECTION_SCHEMA,
        "status": "PASS_RUNTIME_CEILING_SIZED_FROM_INTERRUPTED_V6",
        "source_receipt_id": LEGACY_RUNTIME_RECEIPT_ID,
        "total_work_unit_count": total_work_units,
        "completed_work_unit_count": total_work_units,
        "observed_elapsed_seconds": observed,
        "projected_runtime_seconds": observed,
        "projected_runtime_high_seconds": high,
        "successor_runtime_ceiling_seconds": MAXIMUM_RUNTIME_SECONDS,
        "partial_output_reuse_allowed": False,
        "measurement_basis": "COMPLETE_41_MARKET_DURABLE_HOST_EXIT_EVIDENCE",
        "market_evidence": list(evidence),
    }
    return {**core, "projection_id": sha256_json(core)}


def _old_checkpoint_set_id(root: Path, gate_plan: Mapping[str, object]) -> str:
    catalog = _json(root / str(gate_plan["predecessor"]["active_catalog_snapshot_path"]))
    ids = {
        str(binding["path"]).split("/")[3]
        for entry in catalog.get("entries", [])
        for binding in entry.get("source_bindings", [])
        if str(binding.get("path", "")).startswith(
            "data/causally_gated_normalized/v10/"
        )
    }
    if len(ids) != 1:
        raise IntegrityError("predecessor active catalog does not bind one checkpoint set")
    return ids.pop()


def _old_set_certificate_ids(root: Path, checkpoint_set_id: str) -> dict[str, str]:
    paths = tuple(
        path
        for path in iter_plain_files(
            root / OLD_CERTIFICATE_ROOT / checkpoint_set_id / "complete_set"
        )
        if path.suffix == ".json"
    )
    if len(paths) != 1:
        raise IntegrityError("predecessor complete-set certificate is not unique")
    certificate = _json(paths[0])
    ids = certificate.get("market_certificate_ids")
    if not isinstance(ids, list) or len(ids) != len(MARKET_ORDER):
        raise IntegrityError("predecessor complete-set certificate coverage differs")
    return dict(zip(MARKET_ORDER, map(str, ids), strict=True))


def _old_market(
    root: Path, checkpoint_set_id: str, market: str, certificate_id: str
) -> dict[str, object]:
    certificates = tuple(
        path
        for path in iter_plain_files(
            root / OLD_CERTIFICATE_ROOT / checkpoint_set_id / market
        )
        if path.suffix == ".json" and _json(path).get("certificate_id") == certificate_id
    )
    if len(certificates) != 1:
        raise IntegrityError(f"predecessor {market} certificate is not unique")
    certificate = _json(certificates[0])
    attempt = str(certificate["attempt_id"])
    checkpoint_path = (
        root / OUTPUT_ROOT / checkpoint_set_id / market / attempt / "market_checkpoint.json"
    )
    checkpoint = _json(checkpoint_path)
    if (
        certificate.get("market") != market
        or checkpoint.get("result_id")
        != sha256_json({k: v for k, v in checkpoint.items() if k != "result_id"})
    ):
        raise IntegrityError(f"predecessor {market} evidence differs")
    host = root / OLD_HOST_ROOT / market / attempt
    started_path, exit_path = host / "started.json", host / "exit.json"
    started, exited = _json(started_path), _json(exit_path)
    started_core = {k: v for k, v in started.items() if k != "started_id"}
    exit_core = {k: v for k, v in exited.items() if k != "exit_id"}
    if (
        started.get("started_id") != sha256_json(started_core)
        or exited.get("exit_id") != sha256_json(exit_core)
        or exited.get("status") != "PASS"
        or started.get("plan_id") != exited.get("plan_id")
    ):
        raise IntegrityError(f"predecessor {market} durable-host evidence differs")
    elapsed = math.ceil(
        (
            datetime.fromisoformat(str(exited["finished_at"]))
            - datetime.fromisoformat(str(started["started_at"]))
        ).total_seconds()
    )
    return {
        "attempt_id": attempt,
        "checkpoint_path": checkpoint_path.relative_to(root).as_posix(),
        "checkpoint_sha256": sha256_file(checkpoint_path),
        "decoded_record_count": int(checkpoint["decoded_record_count"]),
        "timing": {
            "market": market,
            "attempt_id": attempt,
            "started_path": started_path.relative_to(root).as_posix(),
            "started_sha256": sha256_file(started_path),
            "exit_path": exit_path.relative_to(root).as_posix(),
            "exit_sha256": sha256_file(exit_path),
            "elapsed_seconds": elapsed,
        },
    }


def _checkpoint_set(root: Path, source: Mapping[str, object]) -> dict[str, object]:
    bindings = {
        relative: sha256_file(root / relative)
        for relative in sorted(CHECKPOINT_SET_REQUIRED_BINDINGS)
    }
    return {
        "schema_version": CHECKPOINT_SET_SCHEMA,
        "market_order": list(MARKET_ORDER),
        "source_contract_id": source["source_contract_id"],
        "canonical_release_id": source["canonical_release_id"],
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "writer_configuration": {
            "format": "PARQUET",
            "compression": "ZSTD",
            "compression_level": 9,
            "partitioning": "market/year/month",
        },
        "implementation_bindings": bindings,
    }


def prepare(root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    gate_plan = _json(root / GATE_B_PLAN)
    verify_publication(root=root, plan_path=root / GATE_B_PLAN)
    inventory_path = root / str(gate_plan["causal_rebuild"]["source_inventory_path"])
    inventory = _json(inventory_path)
    entries = list(inventory["entries"])
    dbns = [item for item in entries if item["kind"] == "DBN"]
    if (
        len(entries) != EXPECTED_ENTRY_COUNT
        or len(dbns) != EXPECTED_DBN_COUNT
        or sum(item["kind"] == "SIDECAR" for item in entries) != EXPECTED_SIDECAR_COUNT
        or sum(int(item["size_bytes"]) for item in entries) != EXPECTED_SOURCE_BYTES
        or sum(int(item["size_bytes"]) for item in dbns) != EXPECTED_PAYLOAD_BYTES
    ):
        raise IntegrityError("Gate-B causal inventory differs from the full-build contract")
    query_contract_preflight = validate_source_query_contracts(root, entries)
    work_units = len({(str(item["market"]), int(item["year"])) for item in entries})
    old_set = _old_checkpoint_set_id(root, gate_plan)
    old_ids = _old_set_certificate_ids(root, old_set)
    old = {
        market: _old_market(root, old_set, market, old_ids[market])
        for market in MARKET_ORDER
    }
    source_contract = _json(root / "configs/source_contract.json")
    source = {
        "source_contract_id": source_contract["contract_id"],
        "canonical_release_id": source_contract["active_canonical_source"]["release_id"],
        "inventory_path": inventory_path.relative_to(root).as_posix(),
        "inventory_sha256": sha256_file(inventory_path),
        "exact_source_entries_sha256": sha256_json(entries),
        "exact_dbn_entries_sha256": sha256_json(dbns),
        "exact_source_entry_count": len(entries),
        "exact_dbn_file_count": len(dbns),
        "exact_sidecar_file_count": len(entries) - len(dbns),
        "total_source_bytes": sum(int(item["size_bytes"]) for item in entries),
        "maximum_payload_bytes": sum(int(item["size_bytes"]) for item in dbns),
        "primary_1m_dbn_count": sum(item["family"] == "ohlcv_1m" for item in dbns),
        "work_unit_count": work_units,
        "standard_root_count": STANDARD_ROOT_COUNT,
        "deferred_micro_count": DEFERRED_MICRO_COUNT,
        "minimum_year": min(int(item["year"]) for item in entries),
        "maximum_year": max(int(item["year"]) for item in entries),
    }
    run_id = sha256_json(
        {
            "gate_b_plan_id": gate_plan["plan_id"],
            "source_contract_id": source["source_contract_id"],
            "predecessor_checkpoint_set_id": old_set,
            "implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
        }
    )
    run_root = root / STATE_ROOT / run_id
    projection = _projection(
        [old[market]["timing"] for market in MARKET_ORDER],
        total_work_units=work_units,
    )
    projection_path = run_root / "runtime_projection.json"
    _write(projection_path, projection)
    plan_core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "operation": CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source": source,
        "output_staging_path": (FULL_STAGE_ROOT / run_id / "full_build").as_posix(),
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "holdout_allowed": False,
        "forward_allowed": False,
        "provider_calls": 0,
        "execution_authorized": False,
        "authority": {
            name: False
            for name in (
                "activation", "evaluation", "features", "fitting", "forward",
                "holdout", "mechanism", "outcomes", "prediction", "provider",
                "publication", "wfa",
            )
        },
        "limits": {
            "maximum_payload_bytes": EXPECTED_PAYLOAD_BYTES,
            "maximum_decoded_records": 1_000_000_000,
            "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
            "maximum_partition_count": MAXIMUM_PARTITION_COUNT,
            "maximum_peak_additional_bytes": MAXIMUM_PEAK_ADDITIONAL_BYTES,
        },
        "execution": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_workers": 1,
            "priority_markets": list(WORK_UNIT_PRIORITY_MARKETS),
            "remaining_order": REMAINING_WORK_UNIT_ORDER,
            "python_executable": PINNED_PYTHON_EXECUTABLE,
            "databento_version": SUPPORTED_DATABENTO_VERSION,
        },
        "runtime_projection": {
            "path": projection_path.relative_to(root).as_posix(),
            "sha256": sha256_file(projection_path),
            "projection_id": projection["projection_id"],
        },
        "storage": {
            "required_free_after_peak_bytes": MINIMUM_FREE_AFTER_PEAK_BYTES,
            "publication_authorized": False,
            "activation_authorized": False,
            "partitioning": "market/year/month",
            "empty_partitions": False,
            "full_1s_duplication": False,
        },
        "economics": {
            "rulebook_path": ECONOMICS_RULEBOOK_PATH,
            "rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        },
        "reuse_canary_candidates": False,
        "reuse_prior_partitions": False,
    }
    plan = {**plan_core, "plan_id": sha256_json(plan_core)}
    plan_path = run_root / "full_build_plan.json"
    _write(plan_path, plan)
    _validate_plan(root, plan)
    preparation_core = {
        "schema_version": "annual_dbn_gate_b_causal_preparation/1.0.0",
        "status": "PREPARED_MEASURED_FULL_CAUSAL_REBUILD_NOT_EXECUTED",
        "run_id": run_id,
        "gate_b_plan_id": gate_plan["plan_id"],
        "predecessor_checkpoint_set_id": old_set,
        "full_build_plan_path": plan_path.relative_to(root).as_posix(),
        "full_build_plan_sha256": sha256_file(plan_path),
        "runtime_projection_id": projection["projection_id"],
        "observed_runtime_seconds": projection["observed_elapsed_seconds"],
        "projected_runtime_high_seconds": projection["projected_runtime_high_seconds"],
        "query_contract_preflight": query_contract_preflight,
        "old_markets": old,
        "provider_calls": 0,
        "deletions": 0,
    }
    preparation = {**preparation_core, "preparation_id": sha256_json(preparation_core)}
    _write(run_root / "preparation.json", preparation)
    return preparation


def _recovery_predecessor(
    root: Path, predecessor_run_id: str,
) -> tuple[dict[str, object], dict[str, object], Path, str]:
    run_root = root / STATE_ROOT / predecessor_run_id
    preparation = _json(run_root / "preparation.json")
    plan_path = root / str(preparation["full_build_plan_path"])
    plan = _json(plan_path)
    failure_path = root / str(plan["output_staging_path"]) / "failure.json"
    failure_sha256 = sha256_file(failure_path)
    failure = _json(failure_path)
    if (
        preparation.get("run_id") != predecessor_run_id
        or plan.get("plan_id") != failure.get("plan_id")
        or sha256_file(plan_path) != failure.get("plan_sha256")
        or failure.get("status")
        != "FAILED_AUTHORIZATION_CONSUMED_NO_AUTOMATIC_RETRY"
        or failure.get("terminal") is not True
        or failure.get("receipt_reuse_authorized") is not False
        or failure.get("partial_partition_reuse_authorized") is not False
        or failure.get("automatic_retry_authorized") is not False
        or failure.get("required_successor") != "NEW_PLAN_NEW_RECEIPT_NEW_OUTPUT_ROOT"
        or (failure_path.parent / "full_build_result.json").exists()
    ):
        raise IntegrityError("interrupted Gate-B predecessor is not exact terminal evidence")
    return preparation, plan, failure_path, failure_sha256


def _validate_recovery_predecessor(root: Path, plan: Mapping[str, object]) -> None:
    binding = plan.get("predecessor_failure")
    if binding is None:
        return
    if not isinstance(binding, Mapping) or set(binding) != {
        "run_id",
        "plan_id",
        "plan_sha256",
        "failure_path",
        "failure_sha256",
        "partial_output_reuse",
    }:
        raise UnauthorizedOperation("Gate-B recovery predecessor binding is malformed")
    _, predecessor, failure_path, failure_sha256 = _recovery_predecessor(
        root, str(binding["run_id"])
    )
    if (
        binding.get("plan_id") != predecessor["plan_id"]
        or binding.get("plan_sha256")
        != sha256_file(root / STATE_ROOT / str(binding["run_id"]) / "full_build_plan.json")
        or binding.get("failure_path") != failure_path.relative_to(root).as_posix()
        or binding.get("failure_sha256") != failure_sha256
        or binding.get("partial_output_reuse") is not False
        or plan.get("output_staging_path") == predecessor.get("output_staging_path")
    ):
        raise IntegrityError("Gate-B recovery predecessor binding differs")


def prepare_recovery(root: Path, predecessor_run_id: str) -> dict[str, object]:
    """Prepare a fresh full retry after an interrupted, consumed attempt."""

    root = root.resolve(strict=True)
    gate_plan = _json(root / GATE_B_PLAN)
    publication = _json(root / GATE_B_PUBLICATION)
    if (
        publication.get("status") != "PASS_ANNUAL_CUSTODY_AND_SOURCE_CONTROLS_ACTIVE"
        or publication.get("plan_id") != gate_plan["plan_id"]
        or publication.get("deletions") != 0
        or sha256_file(root / "configs/source_contract.json")
        != gate_plan["successor"]["source_contract_candidate_sha256"]
        or sha256_file(root / "configs/active_dbn_congruence_release_v1.json")
        != gate_plan["successor"]["pointer_candidate_sha256"]
    ):
        raise IntegrityError("Gate-B annual source publication receipt or aliases differ")
    predecessor_preparation, predecessor, failure_path, failure_sha256 = (
        _recovery_predecessor(root, predecessor_run_id)
    )
    predecessor_plan_path = root / str(
        predecessor_preparation["full_build_plan_path"]
    )
    predecessor_binding = {
        "run_id": predecessor_run_id,
        "plan_id": predecessor["plan_id"],
        "plan_sha256": sha256_file(predecessor_plan_path),
        "failure_path": failure_path.relative_to(root).as_posix(),
        "failure_sha256": failure_sha256,
        "partial_output_reuse": False,
    }
    run_id = sha256_json(
        {
            "predecessor_failure": predecessor_binding,
            "implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
            "recovery_generation": 1,
        }
    )
    run_root = root / STATE_ROOT / run_id
    projection = _json(root / str(predecessor["runtime_projection"]["path"]))
    projection_path = run_root / "runtime_projection.json"
    _write(projection_path, projection)
    plan_core = {
        key: value
        for key, value in predecessor.items()
        if key not in {"plan_id", "output_staging_path", "runtime_projection"}
    }
    plan_core.update(
        {
            "output_staging_path": (FULL_STAGE_ROOT / run_id / "full_build").as_posix(),
            "runtime_projection": {
                "path": projection_path.relative_to(root).as_posix(),
                "sha256": sha256_file(projection_path),
                "projection_id": projection["projection_id"],
            },
            "predecessor_failure": predecessor_binding,
        }
    )
    plan = {**plan_core, "plan_id": sha256_json(plan_core)}
    plan_path = run_root / "full_build_plan.json"
    _write(plan_path, plan)
    _validate_plan(root, plan)
    _validate_recovery_predecessor(root, plan)
    preparation_core = {
        key: value
        for key, value in predecessor_preparation.items()
        if key
        not in {
            "preparation_id",
            "status",
            "run_id",
            "full_build_plan_path",
            "full_build_plan_sha256",
            "runtime_projection_id",
        }
    }
    preparation_core.update(
        {
            "status": "PREPARED_FRESH_FULL_RETRY_NO_PARTIAL_REUSE_NOT_EXECUTED",
            "run_id": run_id,
            "full_build_plan_path": plan_path.relative_to(root).as_posix(),
            "full_build_plan_sha256": sha256_file(plan_path),
            "runtime_projection_id": projection["projection_id"],
            "predecessor_failure": predecessor_binding,
        }
    )
    preparation = {
        **preparation_core,
        "preparation_id": sha256_json(preparation_core),
    }
    _write(run_root / "preparation.json", preparation)
    return preparation


def _approved_receipt(
    root: Path, *, plan: Mapping[str, object], plan_path: Path, scope: Mapping[str, str]
) -> OperationReceipt:
    bare_scope = {
        key: value
        for key, value in scope.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    return OperationReceipt.issue_user_approved(
        RepoBoundary(root),
        operation=CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=bare_scope,
        approval_command=CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=sha256_file(plan_path),
        approval_line=_personal_approval_line(
            CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
            str(plan["plan_id"]),
            sha256_file(plan_path),
        ),
    )


def build(root: Path, run_id: str) -> dict[str, object]:
    root = root.resolve(strict=True)
    plan_path = root / STATE_ROOT / run_id / "full_build_plan.json"
    plan = _json(plan_path)
    _validate_recovery_predecessor(root, plan)
    scope = required_full_build_scope(
        plan=plan,
        plan_sha256=sha256_file(plan_path),
        source_contract_id=str(plan["source"]["source_contract_id"]),
        canonical_release_id=str(plan["source"]["canonical_release_id"]),
    )
    receipt = _approved_receipt(root, plan=plan, plan_path=plan_path, scope=scope)
    _write(plan_path.parent / "full_build_receipt.json", receipt.as_dict())
    result = run_authorized_full_build(
        repository_root=root, receipt=receipt, plan_path=plan_path
    )
    return dict(result.payload)


def _move_tree(source: Path, destination: Path) -> int:
    if destination.exists():
        raise IntegrityError(f"causal market destination already exists: {destination}")
    expected = len(iter_plain_files(source))
    io_path(destination.parent).mkdir(parents=True, exist_ok=True)
    os.replace(io_path(source), io_path(destination))
    if source.exists() or len(iter_plain_files(destination)) != expected:
        raise IntegrityError("causal partition stage move differs")
    return expected


def split(root: Path, run_id: str) -> dict[str, object]:
    root = root.resolve(strict=True)
    run_root = root / STATE_ROOT / run_id
    preparation = _json(run_root / "preparation.json")
    full_plan = _json(root / str(preparation["full_build_plan_path"]))
    full_result_path = root / str(full_plan["output_staging_path"]) / "full_build_result.json"
    full = _json(full_result_path)
    if (
        full.get("schema_version") != RESULT_SCHEMA
        or full.get("status")
        != "PASS_AUTHORIZED_FULL_DEVELOPMENT_CANDIDATE_NOT_PUBLISHED_NOT_ACTIVE"
        or full.get("result_id")
        != sha256_json({k: v for k, v in full.items() if k != "result_id"})
        or full.get("complete_work_unit_count") != EXPECTED_WORK_UNIT_COUNT
    ):
        raise IntegrityError("authorized full causal result is not complete")
    checkpoint_set = _checkpoint_set(root, full_plan["source"])
    checkpoint_set_id = checkpoint_set_identity(checkpoint_set)
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for item in full["partitions"]:
        grouped[str(item["market"])].append(dict(item))
    source_inventory = _json(root / str(full_plan["source"]["inventory_path"]))
    old = preparation["old_markets"]
    markets = []
    for market in MARKET_ORDER:
        attempt_id = sha256_json(
            {
                "run_id": run_id,
                "full_result_id": full["result_id"],
                "checkpoint_set_id": checkpoint_set_id,
                "market": market,
            }
        )
        output = Path(OUTPUT_ROOT) / checkpoint_set_id / market / attempt_id
        source_market = root / str(full_plan["output_staging_path"]) / market
        moved_files = _move_tree(source_market, root / output / "sealed")
        entries = sorted(
            (item for item in source_inventory["entries"] if item["market"] == market),
            key=lambda item: str(item["path"]),
        )
        dbns = [item for item in entries if item["kind"] == "DBN"]
        inventory_core = {
            "schema_version": "annual_dbn_gate_b_market_source_inventory/1.0.0",
            "market": market,
            "entries": entries,
        }
        inventory = {**inventory_core, "inventory_id": sha256_json(inventory_core)}
        market_root = run_root / "markets" / market
        inventory_path = market_root / "source_inventory.json"
        _write(inventory_path, inventory)
        partitions = []
        source_prefix = f"{full_plan['output_staging_path']}/{market}/"
        target_prefix = f"{output.as_posix()}/sealed/"
        for item in grouped[market]:
            if not str(item["stage"]).startswith(source_prefix):
                raise IntegrityError(f"full causal {market} partition path differs")
            partitions.append(
                {**item, "stage": str(item["stage"]).replace(source_prefix, target_prefix, 1)}
            )
        record = {
            "market": market,
            "attempt_id": attempt_id,
            "output_staging_path": output.as_posix(),
            "source": {
                "source_contract_id": full_plan["source"]["source_contract_id"],
                "canonical_release_id": full_plan["source"]["canonical_release_id"],
                "inventory_path": inventory_path.relative_to(root).as_posix(),
                "inventory_sha256": sha256_file(inventory_path),
                "exact_source_entries_sha256": sha256_json(entries),
                "exact_dbn_entries_sha256": sha256_json(dbns),
                "exact_source_entry_count": len(entries),
                "exact_dbn_file_count": len(dbns),
                "exact_sidecar_file_count": len(entries) - len(dbns),
                "total_source_bytes": sum(int(item["size_bytes"]) for item in entries),
                "maximum_payload_bytes": sum(int(item["size_bytes"]) for item in dbns),
                "work_unit_count": len({int(item["year"]) for item in entries}),
            },
            "partitions": partitions,
            "partition_count": len(partitions),
            "partition_inventory_sha256": sha256_json(partitions),
            "output_bytes": sum(int(item["output_bytes"]) for item in partitions),
            "decoded_record_count": old[market]["decoded_record_count"],
            "decoded_record_count_basis": {
                "unchanged_source_entries_sha256": sha256_json(entries),
                "predecessor_checkpoint_path": old[market]["checkpoint_path"],
                "predecessor_checkpoint_sha256": old[market]["checkpoint_sha256"],
            },
            "moved_file_count": moved_files,
        }
        markets.append(record)
    core = {
        "schema_version": "annual_dbn_gate_b_causal_split/1.0.0",
        "status": "PASS_FULL_BUILD_PARTITIONS_MOVED_INACTIVE",
        "run_id": run_id,
        "full_result_path": full_result_path.relative_to(root).as_posix(),
        "full_result_sha256": sha256_file(full_result_path),
        "full_result_id": full["result_id"],
        "checkpoint_set": checkpoint_set,
        "checkpoint_set_id": checkpoint_set_id,
        "markets": markets,
        "provider_calls": 0,
        "deletions": 0,
    }
    result = {**core, "split_id": sha256_json(core)}
    _write(run_root / "split.json", result)
    return result


def _market_build_plan(
    split_result: Mapping[str, object], market_record: Mapping[str, object]
) -> dict[str, object]:
    source = market_record["source"]
    core: dict[str, object] = {
        "schema_version": "development_causal_observation_market_checkpoint_plan/1.1.0",
        "operation": CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        "execution_role": COMPLETE_MARKET_EXECUTION_ROLE,
        "target_market": market_record["market"],
        "attempt_id": market_record["attempt_id"],
        "checkpoint_set": split_result["checkpoint_set"],
        "checkpoint_set_id": split_result["checkpoint_set_id"],
        "source": source,
        "output_staging_path": market_record["output_staging_path"],
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "holdout_allowed": False,
        "forward_allowed": False,
        "provider_calls": 0,
        "execution_authorized": False,
        "authority": {"activation": False, "publication": False, "provider": False},
        "limits": {
            "maximum_payload_bytes": source["maximum_payload_bytes"],
            "maximum_decoded_records": 1_000_000_000,
            "maximum_output_bytes": 800_000_000,
            "maximum_partition_count": 192,
        },
        "execution": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_workers": 1,
            "python_executable": PINNED_PYTHON_EXECUTABLE,
            "databento_version": SUPPORTED_DATABENTO_VERSION,
        },
        "economics": {
            "rulebook_path": ECONOMICS_RULEBOOK_PATH,
            "rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        },
        "durable_host": expected_durable_host_plan(
            str(market_record["market"]), str(market_record["attempt_id"])
        ),
        "reuse_failed_market_partitions": False,
    }
    return {**core, "plan_id": sha256_json(core)}


def _checkpoint(
    full: Mapping[str, object], plan: Mapping[str, object], market: Mapping[str, object]
) -> dict[str, object]:
    core = {
        "schema_version": RESULT_SCHEMA,
        "status": "PASS_COMPLETE_MARKET_CHECKPOINT_INACTIVE",
        "plan_id": plan["plan_id"],
        "plan_sha256": "PENDING_WRITE",
        "receipt_id": full["receipt_id"],
        "source_contract_id": plan["source"]["source_contract_id"],
        "source_release_id": plan["source"]["canonical_release_id"],
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source_entry_count": plan["source"]["exact_source_entry_count"],
        "dbn_file_count": plan["source"]["exact_dbn_file_count"],
        "payload_bytes_opened_maximum": plan["source"]["maximum_payload_bytes"],
        "decoded_record_count": market["decoded_record_count"],
        "complete_work_unit_count": plan["source"]["work_unit_count"],
        "partition_count": market["partition_count"],
        "output_bytes": market["output_bytes"],
        "partition_inventory_sha256": market["partition_inventory_sha256"],
        "partitions": market["partitions"],
        "provider_calls": 0,
        "holdout_rows": 0,
        "forward_rows": 0,
        "outcomes": 0,
        "features": 0,
        "wfa": 0,
        "fitting": 0,
        "predictions": 0,
        "evaluations": 0,
        "mechanism_executions": 0,
        "publication_authorized": False,
        "activation_authorized": False,
        "target_market": market["market"],
        "attempt_id": market["attempt_id"],
        "checkpoint_set_id": plan["checkpoint_set_id"],
        "complete_market_checkpoint": True,
        "reusable_in_same_checkpoint_set": True,
        "execution_role": COMPLETE_MARKET_EXECUTION_ROLE,
        "can_seed_complete_market_checkpoint": False,
        "materialization": {
            "kind": "AUTHORIZED_FULL_BUILD_PARTITION_STAGE_MOVE",
            "full_result_id": full["result_id"],
            "decoded_record_count_basis": market["decoded_record_count_basis"],
        },
    }
    return core


def certify(root: Path, run_id: str) -> dict[str, object]:
    root = root.resolve(strict=True)
    run_root = root / STATE_ROOT / run_id
    split_result = _json(run_root / "split.json")
    full = _json(root / str(split_result["full_result_path"]))
    records = {str(item["market"]): item for item in split_result["markets"]}
    certificates = []
    predecessor_path: str | None = None
    for market in MARKET_ORDER:
        record = records[market]
        plan = _market_build_plan(split_result, record)
        if predecessor_path is not None:
            plan = bind_predecessor_market_certificate(
                repository_root=root,
                build_plan=plan,
                certificate_path=predecessor_path,
            )
        market_root = run_root / "markets" / market
        plan_path = market_root / "build_plan.json"
        _write(plan_path, plan)
        validate_market_checkpoint_plan(root, plan)
        checkpoint_core = _checkpoint(full, plan, record)
        checkpoint_core["plan_sha256"] = sha256_file(plan_path)
        checkpoint = {**checkpoint_core, "result_id": sha256_json(checkpoint_core)}
        checkpoint_path = root / str(plan["output_staging_path"]) / "market_checkpoint.json"
        _write(checkpoint_path, checkpoint)
        cert_plan = build_market_certification_plan(
            repository_root=root,
            build_plan_path=plan_path.relative_to(root).as_posix(),
            build_plan=plan,
            build_plan_sha256=sha256_file(plan_path),
        )
        cert_plan_path = market_root / "certification_plan.json"
        _write(cert_plan_path, cert_plan)
        scope = required_market_certification_scope(
            plan=cert_plan,
            plan_sha256=sha256_file(cert_plan_path),
            checkpoint_sha256=sha256_file(checkpoint_path),
        )
        receipt = _approved_receipt(
            root, plan=cert_plan, plan_path=cert_plan_path, scope=scope
        )
        _write(market_root / "certification_receipt.json", receipt.as_dict())
        certificate = run_authorized_market_certification(
            repository_root=root, receipt=receipt, plan_path=cert_plan_path
        )
        certificates.append(certificate)
        predecessor_path = str(cert_plan["certificate_path"])
    complete = certify_complete_market_certificate_set(
        repository_root=root,
        checkpoint_set=split_result["checkpoint_set"],
        market_certificates=certificates,
    )
    complete_path = (
        root
        / OLD_CERTIFICATE_ROOT
        / str(split_result["checkpoint_set_id"])
        / "complete_set"
        / f"{complete['certificate_id']}.json"
    )
    _write(complete_path, complete)
    result = {
        "status": "PASS_41_MARKET_CAUSAL_REBUILD_AND_CERTIFICATION_INACTIVE",
        "run_id": run_id,
        "checkpoint_set_id": split_result["checkpoint_set_id"],
        "complete_certificate_id": complete["certificate_id"],
        "complete_certificate_path": complete_path.relative_to(root).as_posix(),
        "complete_certificate_sha256": sha256_file(complete_path),
        "market_count": len(certificates),
        "provider_calls": 0,
        "deletions": 0,
    }
    _write(run_root / "certification_result.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", choices=("prepare", "recover", "build", "split", "certify")
    )
    parser.add_argument("--run-id")
    args = parser.parse_args()
    root = Path.cwd().resolve(strict=True)
    if args.command == "prepare":
        result = prepare(root)
    elif args.command == "recover":
        if not args.run_id:
            parser.error("--run-id is required")
        result = prepare_recovery(root, args.run_id)
    else:
        if not args.run_id:
            parser.error("--run-id is required")
        result = globals()[args.command](root, args.run_id)
    print(result)


if __name__ == "__main__":
    main()
