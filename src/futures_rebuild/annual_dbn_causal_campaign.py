"""Durable, sequential Gate-B campaign built from existing V10 market checkpoints."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from .annual_dbn_causal_rebuild import GATE_B_PLAN, _approved_receipt, _checkpoint_set
from .annual_dbn_gate_b import verify_publication
from .canonical import io_path, iter_plain_files, sha256_file, sha256_json
from .causal_full_build_durable_host import (
    expected_durable_host_plan,
    DURABLE_HOST_ENVIRONMENT_KEY,
    run_durable_full_build_worker,
)
from .causal_observation_foundation import (
    CAUSAL_OBSERVATION_CONTRACT_ID,
    ECONOMICS_RULEBOOK_PATH,
    ECONOMICS_RULEBOOK_SHA256,
    required_market_checkpoint_scope,
)
from .causal_observation_full_build import (
    COMPLETE_MARKET_EXECUTION_ROLE,
    EXPECTED_DBN_COUNT,
    EXPECTED_ENTRY_COUNT,
    EXPECTED_PAYLOAD_BYTES,
    EXPECTED_SIDECAR_COUNT,
    EXPECTED_SOURCE_BYTES,
    EXPECTED_WORK_UNIT_COUNT,
    MAXIMUM_RUNTIME_SECONDS,
    PINNED_PYTHON_EXECUTABLE,
    _json,
    _write_create_only,
    validate_source_query_contracts,
)
from .causal_observation_market_certification import (
    CERTIFICATE_ROOT,
    bind_predecessor_market_certificate,
    build_market_certification_plan,
    certify_complete_market_certificate_set,
    required_market_certification_scope,
    run_authorized_market_certification,
    run_authorized_sequenced_market_checkpoint,
)
from .causal_observation_market_checkpoint import (
    MARKET_ORDER,
    PLAN_SCHEMA,
    checkpoint_set_identity,
    run_authorized_market_checkpoint,
    validate_market_checkpoint_plan,
)
from .causal_source_closure import select_preferred_standard_source_entries
from .causal_v10_policy_compatibility import validate_market_campaign_storage
from .errors import IntegrityError, UnauthorizedOperation
from .foundation.decoder import SUPPORTED_DATABENTO_VERSION
from .foundation.snapshot import dbn_filename_interval
from .locking import _local_process_alive
from .research_gateway_policy import CAUSAL_OBSERVATION_FULL_BUILD_OPERATION


STATE_ROOT = Path("state/annual_dbn_gate_b_market_campaign_v1")
SOURCE_SUCCESSOR_ROOT = Path("state/annual_dbn_gate_b_source_selection_successors")
EXECUTION_SCRIPT = Path("scripts/run_annual_dbn_causal_campaign.ps1")
IMPLEMENTATION_PATH = Path("src/futures_rebuild/annual_dbn_causal_campaign.py")
ACTIVE_CATALOG = Path("data/active/catalog.json")
INTERRUPTED_RUN_ID = "69dfa96de7f74dc18cd0d8c414f4370b5f78b826e3113466f4262914d44dd9eb"
INTERRUPTED_PLAN_ID = "551962755882b4ad7b4f819489470de98f57b6734b15471cb2bfc07e26535c6a"
INTERRUPTED_OUTPUT = Path(
    "state/data_publication_staging/annual_dbn_gate_b_causal_v1/"
    f"{INTERRUPTED_RUN_ID}/full_build"
)
CONTROLLER_ENVIRONMENT_KEY = "FUTURES_ANNUAL_DBN_CAMPAIGN_TASK_NAME"
MARKET_OUTPUT_LIMIT = 800_000_000
MARKET_PARTITION_LIMIT = 192


def _write(path: Path, payload: Mapping[str, object]) -> None:
    if path.exists():
        if _json(path) != dict(payload):
            raise IntegrityError(f"existing annual Gate-B campaign artifact differs: {path}")
        return
    _write_create_only(path, payload)


def _market_attempt_id(
    *, campaign_id: str, market: str, ordinal: int, source_entries_sha256: str,
    predecessor_certificate_id: str | None, reuse_failure_sha256: str | None,
) -> str:
    return sha256_json(
        {
            "campaign_id": campaign_id,
            "market": market,
            "attempt_ordinal": ordinal,
            "source_entries_sha256": source_entries_sha256,
            "predecessor_certificate_id": predecessor_certificate_id,
            "reuse_failure_sha256": reuse_failure_sha256,
        }
    )


def _valid_certificate(
    root: Path, checkpoint_set_id: str, market: str,
    source_contract_id: str, source_release_id: str,
) -> tuple[Path, dict[str, object]] | None:
    directory = root / CERTIFICATE_ROOT / checkpoint_set_id / market
    valid: list[tuple[Path, dict[str, object]]] = []
    for path in sorted(directory.glob("*.json")) if directory.is_dir() else ():
        if path.name.endswith(".failure.json"):
            continue
        value = _json(path)
        core = {key: item for key, item in value.items() if key != "certificate_id"}
        if (
            value.get("certificate_id") == sha256_json(core)
            and value.get("status")
            == "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE"
            and value.get("market") == market
            and value.get("checkpoint_set_id") == checkpoint_set_id
            and value.get("source_contract_id") == source_contract_id
            and value.get("source_release_id") == source_release_id
            and value.get("causal_contract_id") == CAUSAL_OBSERVATION_CONTRACT_ID
            and value.get("replay_passes") == 2
            and value.get("provider_calls") == 0
            and value.get("holdout_rows") == 0
            and value.get("forward_rows") == 0
            and value.get("publication_authorized") is False
            and value.get("activation_authorized") is False
        ):
            valid.append((path, value))
    if len(valid) > 1:
        raise IntegrityError(f"multiple compatible Gate-B certificates exist for {market}")
    return valid[0] if valid else None


def _certificate_prefix(
    root: Path, checkpoint_set_id: str, source_contract_id: str,
    source_release_id: str,
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    missing = False
    for market in MARKET_ORDER:
        found = _valid_certificate(
            root, checkpoint_set_id, market, source_contract_id, source_release_id
        )
        if found is None:
            missing = True
            continue
        if missing:
            raise IntegrityError("compatible Gate-B certificates are not a contiguous prefix")
        path, certificate = found
        result.append(
            {
                "market": market,
                "path": path.relative_to(root).as_posix(),
                "sha256": sha256_file(path),
                "certificate_id": certificate["certificate_id"],
                "attempt_id": certificate["attempt_id"],
            }
        )
    return result


def _source_successor(
    root: Path,
    inventory_path: Path,
    entries: Sequence[Mapping[str, object]],
) -> tuple[Path, list[dict[str, object]]]:
    paths = {str(item["path"]) for item in entries}
    successor_entries: list[dict[str, object]] = []
    exclusions: list[dict[str, object]] = []
    for raw in entries:
        item = dict(raw)
        path = str(item["path"])
        base = path.removesuffix(".manifest.json")
        if base.endswith(".parent.dbn.zst"):
            continuous_base = f"{base[:-len('.parent.dbn.zst')]}.dbn.zst"
            if continuous_base in paths and f"{continuous_base}.manifest.json" in paths:
                item["admitted_standard_foundation"] = False
                item["selection_disposition"] = (
                    "DIAGNOSTIC_PARENT_SUPERSEDED_BY_CONTINUOUS"
                )
                exclusions.append(
                    {"parent_path": path, "continuous_path": continuous_base}
                )
        successor_entries.append(item)
    successor_entries.sort(key=lambda item: str(item["path"]))
    core = {
        "schema_version": "annual_dbn_gate_b_source_inventory_successor/1.0.0",
        "parent_inventory_path": inventory_path.relative_to(root).as_posix(),
        "parent_inventory_sha256": sha256_file(inventory_path),
        "selection_rule": "ONE_STANDARD_QUERY_EPOCH_CONTINUOUS_PREFERRED",
        "diagnostic_exclusions": exclusions,
        "entries": successor_entries,
    }
    successor_id = sha256_json(core)
    successor_path = root / SOURCE_SUCCESSOR_ROOT / successor_id / "source_inventory.json"
    _write(successor_path, {**core, "inventory_id": successor_id})
    return successor_path, successor_entries


def _preferred_entries(
    root: Path, entries: Sequence[Mapping[str, object]]
) -> tuple[dict[str, object], ...]:
    admitted = tuple(
        dict(item)
        for item in entries
        if item.get("admitted_standard_foundation") is True
    )
    return select_preferred_standard_source_entries(root, admitted)


def _source(root: Path) -> tuple[dict[str, object], list[dict[str, object]]]:
    gate_plan = _json(root / GATE_B_PLAN)
    verify_publication(root=root, plan_path=root / GATE_B_PLAN)
    parent_inventory_path = root / str(gate_plan["causal_rebuild"]["source_inventory_path"])
    if sha256_file(parent_inventory_path) != gate_plan["causal_rebuild"]["source_inventory_sha256"]:
        raise IntegrityError("annual Gate-B campaign source inventory differs")
    inventory = _json(parent_inventory_path)
    entries = sorted((dict(item) for item in inventory["entries"]), key=lambda x: str(x["path"]))
    inventory_path, entries = _source_successor(root, parent_inventory_path, entries)
    dbns = [item for item in entries if item.get("kind") == "DBN"]
    if (
        len(entries) != EXPECTED_ENTRY_COUNT
        or len(dbns) != EXPECTED_DBN_COUNT
        or len(entries) - len(dbns) != EXPECTED_SIDECAR_COUNT
        or sum(int(item["size_bytes"]) for item in entries) != EXPECTED_SOURCE_BYTES
        or sum(int(item["size_bytes"]) for item in dbns) != EXPECTED_PAYLOAD_BYTES
        or len({(str(item["market"]), int(item["year"])) for item in entries})
        != EXPECTED_WORK_UNIT_COUNT
    ):
        raise IntegrityError("annual Gate-B campaign source counts differ")
    preferred = _preferred_entries(root, entries)
    preflight = validate_source_query_contracts(
        root,
        preferred,
        expected_count=sum(item.get("kind") == "DBN" for item in preferred),
    )
    for item in preferred:
        if item.get("kind") == "DBN":
            dbn_filename_interval(Path(str(item["path"])).name)
    active = _json(root / "configs/source_contract.json")
    source = {
        "source_contract_id": active["contract_id"],
        "canonical_release_id": active["active_canonical_source"]["release_id"],
        "inventory_path": inventory_path.relative_to(root).as_posix(),
        "inventory_sha256": sha256_file(inventory_path),
        "exact_source_entries_sha256": sha256_json(entries),
        "exact_dbn_entries_sha256": sha256_json(dbns),
        "exact_source_entry_count": len(entries),
        "exact_dbn_file_count": len(dbns),
        "exact_sidecar_file_count": len(entries) - len(dbns),
        "total_source_bytes": sum(int(item["size_bytes"]) for item in entries),
        "maximum_payload_bytes": sum(int(item["size_bytes"]) for item in dbns),
        "work_unit_count": EXPECTED_WORK_UNIT_COUNT,
        "preferred_source_entries_sha256": sha256_json(preferred),
        "preferred_dbn_file_count": sum(item.get("kind") == "DBN" for item in preferred),
        "preferred_sidecar_file_count": sum(item.get("kind") == "SIDECAR" for item in preferred),
        "preferred_query_contract_preflight": preflight,
    }
    return source, entries


def _market_summaries(
    root: Path, entries: Sequence[Mapping[str, object]]
) -> list[dict[str, object]]:
    summaries = []
    for market in MARKET_ORDER:
        selected = list(
            _preferred_entries(
                root,
                [item for item in entries if item.get("market") == market],
            )
        )
        dbns = [item for item in selected if item.get("kind") == "DBN"]
        sidecars = [item for item in selected if item.get("kind") == "SIDECAR"]
        if not selected or len(dbns) != len(sidecars):
            raise IntegrityError(f"annual Gate-B source pairing differs for {market}")
        summaries.append(
            {
                "market": market,
                "source_entries_sha256": sha256_json(selected),
                "dbn_entries_sha256": sha256_json(dbns),
                "entry_count": len(selected),
                "dbn_file_count": len(dbns),
                "sidecar_file_count": len(sidecars),
                "source_bytes": sum(int(item["size_bytes"]) for item in selected),
                "payload_bytes": sum(int(item["size_bytes"]) for item in dbns),
                "years": sorted({int(item["year"]) for item in selected}),
            }
        )
    return summaries


def _interrupted_evidence(root: Path) -> dict[str, object]:
    plan_path = root / "state/annual_dbn_gate_b_causal_v1" / INTERRUPTED_RUN_ID / "full_build_plan.json"
    plan = _json(plan_path)
    if plan.get("plan_id") != INTERRUPTED_PLAN_ID or not (root / INTERRUPTED_OUTPUT).is_dir():
        raise IntegrityError("interrupted monolithic Gate-B evidence differs")
    files = list(iter_plain_files(root / INTERRUPTED_OUTPUT))
    return {
        "run_id": INTERRUPTED_RUN_ID,
        "plan_id": INTERRUPTED_PLAN_ID,
        "plan_path": plan_path.relative_to(root).as_posix(),
        "plan_sha256": sha256_file(plan_path),
        "output_path": INTERRUPTED_OUTPUT.as_posix(),
        "file_count": len(files),
        "output_bytes": sum(io_path(path).stat().st_size for path in files),
        "full_result_absent": not (root / INTERRUPTED_OUTPUT / "full_build_result.json").exists(),
        "failure_absent": not (root / INTERRUPTED_OUTPUT / "failure.json").exists(),
        "partial_output_reuse": False,
        "disposition": "PRESERVE_NONREUSABLE_DIAGNOSTIC_EVIDENCE",
    }


def _stale_locks(root: Path) -> list[dict[str, object]]:
    paths = (
        root / "state/locks/foundation-build.lock",
        root / f"state/locks/causal-observation-{INTERRUPTED_PLAN_ID}.lock",
    )
    present = [path.exists() for path in paths]
    if not any(present):
        return []
    if not all(present):
        raise IntegrityError("Gate-B lock set is incomplete")
    result = []
    for path in paths:
        value = _json(path)
        pid = int(value["pid"])
        if _local_process_alive(pid) is not False:
            raise UnauthorizedOperation("interrupted Gate-B lock owner is not proven dead")
        result.append(
            {
                "path": path.relative_to(root).as_posix(),
                "sha256": sha256_file(path),
                "pid": pid,
                "token": value["token"],
                "process_alive": False,
                "launch_action": "MOVE_TO_CAMPAIGN_LOCK_QUARANTINE",
            }
        )
    return result


def prepare_campaign(root: Path) -> dict[str, object]:
    """Prepare and preflight the exact campaign without opening a DBN payload."""

    root = root.resolve(strict=True)
    source, entries = _source(root)
    preflight = source["preferred_query_contract_preflight"]
    checkpoint_set = _checkpoint_set(root, source)
    checkpoint_set["implementation_bindings"].update(
        {
            IMPLEMENTATION_PATH.as_posix(): sha256_file(root / IMPLEMENTATION_PATH),
            EXECUTION_SCRIPT.as_posix(): sha256_file(root / EXECUTION_SCRIPT),
        }
    )
    checkpoint_set_id = checkpoint_set_identity(checkpoint_set)
    reusable = _certificate_prefix(
        root, checkpoint_set_id, str(source["source_contract_id"]),
        str(source["canonical_release_id"]),
    )
    remaining = list(MARKET_ORDER[len(reusable):])
    if not remaining:
        raise UnauthorizedOperation("annual Gate-B market campaign is already complete")
    free_bytes = shutil.disk_usage(root).free
    headroom = validate_market_campaign_storage(
        free_bytes=free_bytes, remaining_market_count=len(remaining)
    )
    implementation_paths = sorted(
        set(checkpoint_set["implementation_bindings"])
        | {
            "src/futures_rebuild/annual_dbn_causal_rebuild.py",
            "src/futures_rebuild/annual_dbn_gate_b.py",
        }
    )
    core: dict[str, object] = {
        "schema_version": "annual_dbn_gate_b_market_campaign_plan/1.0.0",
        "status": "PREPARED_PREFLIGHT_PASS_FRESH_APPROVAL_REQUIRED",
        "gate_b_plan_id": _json(root / GATE_B_PLAN)["plan_id"],
        "checkpoint_set": checkpoint_set,
        "checkpoint_set_id": checkpoint_set_id,
        "market_order": list(MARKET_ORDER),
        "certified_prefix": reusable,
        "remaining_markets": remaining,
        "source": source,
        "market_summaries": _market_summaries(root, entries),
        "query_contract_preflight": preflight,
        "execution_contract": {
            "maximum_workers": 1,
            "unit": "ONE_COMPLETE_MARKET",
            "work_unit_within_market": "ONE_YEAR_ATOMIC_SEAL",
            "advance_condition": "MARKET_CHECKPOINT_AND_TWO_REPLAY_CERTIFICATE_PASS",
            "caught_failure": "STOP_CAMPAIGN_PRESERVE_ALL_MARKET_AND_YEAR_EVIDENCE",
            "abrupt_loss": "REUSE_CERTIFIED_MARKETS_AND_ONLY_EXACT_VALID_SEALED_YEAR_PREFIX",
            "maximum_attempts_per_receipt": 1,
            "automatic_retries": 0,
            "provider_calls": 0,
            "holdout_allowed": False,
            "forward_allowed": False,
            "publication_authorized": False,
            "activation_authorized": False,
        },
        "controller_host": {
            "kind": "WINDOWS_TASK_SCHEDULER_CURRENT_USER",
            "task_name": "PENDING_CAMPAIGN_ID",
            "trigger_delay_seconds": 120,
            "maximum_instances": 1,
            "execution_time_limit_days": 7,
            "automatic_restart": False,
            "interactive_parent_independent": True,
        },
        "execution_script": EXECUTION_SCRIPT.as_posix(),
        "implementation_bindings": {
            path: sha256_file(root / path) for path in implementation_paths
        },
        "active_catalog": {
            "path": ACTIVE_CATALOG.as_posix(),
            "sha256": sha256_file(root / ACTIVE_CATALOG),
            "mutation_authorized": False,
        },
        "interrupted_monolithic_attempt": _interrupted_evidence(root),
        "stale_lock_remediation": {
            "action_at_approved_launch": "QUARANTINE_EXACT_BOUND_DEAD_OWNER_LOCKS_ONLY",
            "locks": _stale_locks(root),
            "source_or_output_deletions": 0,
        },
        "storage": {
            "observed_free_bytes": free_bytes,
            "remaining_market_count": len(remaining),
            "maximum_output_bytes_per_market": MARKET_OUTPUT_LIMIT,
            "minimum_free_after_peak_bytes": 64 * 1024**3,
            "headroom_after_full_reservation_bytes": headroom,
        },
        "preparation_access": {
            "dbn_payload_files_opened": 0,
            "sidecar_contracts_validated": preflight["query_contract_count"],
            "provider_calls": 0,
        },
        "preparation_effects": {
            "receipt_issued": False,
            "scheduler_registered": False,
            "checkpoint_created": False,
            "certificate_created": False,
            "active_data_mutated": False,
            "locks_moved": False,
        },
    }
    provisional = sha256_json(core)
    core["controller_host"]["task_name"] = f"FIMV2-Annual-DBN-B-{provisional[:8]}"  # type: ignore[index]
    campaign_id = sha256_json(core)
    plan = {**core, "campaign_id": campaign_id, "plan_id": campaign_id}
    run_root = root / STATE_ROOT / campaign_id
    _write(run_root / "campaign_plan.json", plan)
    return plan


def validate_campaign(root: Path, campaign_id: str) -> dict[str, object]:
    """Independently recompute the preparation-only campaign boundaries."""

    root = root.resolve(strict=True)
    run_root = root / STATE_ROOT / campaign_id
    plan_path = run_root / "campaign_plan.json"
    plan = _json(plan_path)
    core = {key: value for key, value in plan.items() if key not in {"campaign_id", "plan_id"}}
    source, entries = _source(root)
    checks = {
        "plan_identity": campaign_id == plan.get("campaign_id") == plan.get("plan_id") == sha256_json(core),
        "source_identity": plan.get("source") == source,
        "source_counts": len(entries) == EXPECTED_ENTRY_COUNT,
        "query_preflight": _source(root)[0]["preferred_query_contract_preflight"] == plan.get("query_contract_preflight"),
        "market_summaries": _market_summaries(root, entries) == plan.get("market_summaries"),
        "market_order": plan.get("market_order") == list(MARKET_ORDER),
        "work_partitioning": plan["execution_contract"]["maximum_workers"] == 1
        and plan["execution_contract"]["unit"] == "ONE_COMPLETE_MARKET"
        and plan["execution_contract"]["work_unit_within_market"] == "ONE_YEAR_ATOMIC_SEAL",
        "inactive_only": plan["execution_contract"]["publication_authorized"] is False
        and plan["execution_contract"]["activation_authorized"] is False,
        "zero_provider_holdout_forward": plan["execution_contract"]["provider_calls"] == 0
        and plan["execution_contract"]["holdout_allowed"] is False
        and plan["execution_contract"]["forward_allowed"] is False,
        "implementation_bindings": all(
            sha256_file(root / path) == digest
            for path, digest in plan["implementation_bindings"].items()
        ),
        "active_catalog_unchanged": sha256_file(root / ACTIVE_CATALOG)
        == plan["active_catalog"]["sha256"],
        "interrupted_output_preserved_nonreusable": _interrupted_evidence(root)
        == plan["interrupted_monolithic_attempt"],
        "stale_locks_exact_and_dead": _stale_locks(root)
        == plan["stale_lock_remediation"]["locks"],
        "storage": validate_market_campaign_storage(
            free_bytes=shutil.disk_usage(root).free,
            remaining_market_count=len(plan["remaining_markets"]),
        )
        >= 0,
        "no_execution_effect": plan["preparation_effects"]
        == {
            "receipt_issued": False,
            "scheduler_registered": False,
            "checkpoint_created": False,
            "certificate_created": False,
            "active_data_mutated": False,
            "locks_moved": False,
        },
    }
    if not all(checks.values()):
        raise IntegrityError(f"annual Gate-B campaign validation failed: {checks}")
    validation_core = {
        "schema_version": "annual_dbn_gate_b_market_campaign_validation/1.0.0",
        "status": "PASS_READY_FOR_ONE_FRESH_APPROVAL",
        "campaign_id": campaign_id,
        "plan_sha256": sha256_file(plan_path),
        "check_count": len(checks),
        "checks": checks,
        "dbn_payload_files_opened": 0,
        "provider_calls": 0,
        "receipt_issued": False,
        "scheduler_registered": False,
        "active_data_mutated": False,
    }
    validation = {**validation_core, "validation_id": sha256_json(validation_core)}
    _write(run_root / "independent_validation.json", validation)
    return validation


def _load_campaign(root: Path, campaign_id: str, *, require_host: bool) -> dict[str, object]:
    run_root = root / STATE_ROOT / campaign_id
    plan_path = run_root / "campaign_plan.json"
    validation_path = run_root / "independent_validation.json"
    plan, validation = _json(plan_path), _json(validation_path)
    core = {key: value for key, value in plan.items() if key not in {"campaign_id", "plan_id"}}
    validation_core = {key: value for key, value in validation.items() if key != "validation_id"}
    if (
        plan.get("campaign_id") != campaign_id
        or plan.get("plan_id") != campaign_id
        or campaign_id != sha256_json(core)
        or validation.get("validation_id") != sha256_json(validation_core)
        or validation.get("status") != "PASS_READY_FOR_ONE_FRESH_APPROVAL"
        or validation.get("campaign_id") != campaign_id
        or validation.get("plan_sha256") != sha256_file(plan_path)
        or sha256_file(root / ACTIVE_CATALOG) != plan["active_catalog"]["sha256"]
        or any(
            sha256_file(root / path) != digest
            for path, digest in plan["implementation_bindings"].items()
            if path
            not in {
                "src/futures_rebuild/annual_dbn_causal_campaign.py",
                "src/futures_rebuild/causal_observation_market_checkpoint.py",
            }
        )
        or (require_host and os.environ.get(CONTROLLER_ENVIRONMENT_KEY) != plan["controller_host"]["task_name"])
    ):
        raise IntegrityError("approved annual Gate-B campaign or execution host differs")
    return plan


def _quarantine_locks(root: Path, campaign_id: str, locks: Sequence[Mapping[str, object]]) -> None:
    destination = root / STATE_ROOT / campaign_id / "lock_quarantine"
    moved = []
    for item in locks:
        source = root / str(item["path"])
        if sha256_file(source) != item["sha256"]:
            raise IntegrityError("bound stale Gate-B lock changed before launch")
        value = _json(source)
        if (
            int(value["pid"]) != item["pid"]
            or value["token"] != item["token"]
            or _local_process_alive(int(value["pid"])) is not False
        ):
            raise UnauthorizedOperation("bound Gate-B lock is no longer proven stale")
        destination.mkdir(parents=True, exist_ok=True)
        target = destination / source.name
        if target.exists():
            raise IntegrityError("Gate-B lock quarantine destination exists")
        os.replace(io_path(source), io_path(target))
        moved.append(
            {
                "source": item["path"],
                "quarantine": target.relative_to(root).as_posix(),
                "sha256": item["sha256"],
            }
        )
    core = {
        "schema_version": "annual_dbn_gate_b_lock_quarantine/1.0.0",
        "status": "PASS_EXACT_DEAD_OWNER_LOCKS_QUARANTINED",
        "campaign_id": campaign_id,
        "locks": moved,
        "source_or_output_deletions": 0,
    }
    _write(destination / "manifest.json", {**core, "manifest_id": sha256_json(core)})


def _quarantine_resume_locks(root: Path, campaign_id: str) -> None:
    """Move only dead locks proven to belong to this campaign's current attempt."""

    run_root = root / STATE_ROOT / campaign_id
    candidates = []
    for plan_path in run_root.glob("markets/*/*/build_plan.json"):
        plan = _json(plan_path)
        path = root / (
            f"state/locks/causal-observation-market-{plan['target_market']}-"
            f"{plan['plan_id']}.lock"
        )
        if path.exists():
            candidates.append((path, _json(path), plan))
    global_lock = root / "state/locks/foundation-build.lock"
    if not candidates and not global_lock.exists():
        return
    if len(candidates) != 1 or not global_lock.exists():
        raise UnauthorizedOperation("campaign resume locks are not one exact market pair")
    market_lock, market_value, market_plan = candidates[0]
    global_value = _json(global_lock)
    pid = int(market_value["pid"])
    if int(global_value["pid"]) != pid or _local_process_alive(pid) is not False:
        raise UnauthorizedOperation("campaign resume lock owner is not proven dead")
    bindings = [
        {
            "path": path.relative_to(root).as_posix(),
            "sha256": sha256_file(path),
            "pid": pid,
            "token": value["token"],
        }
        for path, value in ((global_lock, global_value), (market_lock, market_value))
    ]
    destination = run_root / "lock_quarantine" / f"resume-{market_plan['plan_id']}"
    destination.mkdir(parents=True, exist_ok=False)
    moved = []
    for binding in bindings:
        source = root / str(binding["path"])
        target = destination / source.name
        os.replace(io_path(source), io_path(target))
        moved.append(
            {
                **binding,
                "quarantine": target.relative_to(root).as_posix(),
            }
        )
    core = {
        "schema_version": "annual_dbn_gate_b_resume_lock_quarantine/1.0.0",
        "status": "PASS_EXACT_DEAD_CAMPAIGN_LOCKS_QUARANTINED",
        "campaign_id": campaign_id,
        "market": market_plan["target_market"],
        "plan_id": market_plan["plan_id"],
        "locks": moved,
        "source_or_output_deletions": 0,
    }
    _write(destination / "manifest.json", {**core, "manifest_id": sha256_json(core)})


def _market_entries(root: Path, plan: Mapping[str, object], market: str) -> list[dict[str, object]]:
    inventory_path = root / str(plan["source"]["inventory_path"])
    if sha256_file(inventory_path) != plan["source"]["inventory_sha256"]:
        raise IntegrityError("campaign source inventory drifted")
    entries = list(
        _preferred_entries(
            root,
            [
                dict(item)
                for item in _json(inventory_path)["entries"]
                if item.get("market") == market
            ],
        )
    )
    expected = next(item for item in plan["market_summaries"] if item["market"] == market)
    if sha256_json(entries) != expected["source_entries_sha256"]:
        raise IntegrityError(f"campaign source metadata drifted for {market}")
    return entries


def _reuse_binding(root: Path, prior_plan: Mapping[str, object]) -> dict[str, object] | None:
    output = root / str(prior_plan["output_staging_path"])
    failure_path = output / "failure.json"
    if not failure_path.is_file():
        return None
    failure = _json(failure_path)
    count = int(failure.get("sealed_work_unit_count", 0))
    if count <= 0 or failure.get("sealed_work_units_reusable_if_all_bindings_match") is not True:
        return None
    seals = []
    for path in sorted((output / "work_unit_seals").glob("*.json"), key=lambda p: int(p.stem))[:count]:
        seals.append(
            {"year": int(path.stem), "path": path.relative_to(root).as_posix(), "sha256": sha256_file(path)}
        )
    if len(seals) != count:
        raise IntegrityError("failed market sealed-year evidence is incomplete")
    return {
        "schema_version": "causal_observation_v10_work_unit_reuse/1.0.0",
        "predecessor_output_staging_path": str(prior_plan["output_staging_path"]),
        "predecessor_plan_id": str(prior_plan["plan_id"]),
        "predecessor_failure_path": failure_path.relative_to(root).as_posix(),
        "predecessor_failure_sha256": sha256_file(failure_path),
        "seals": seals,
    }


def _prepare_market(
    root: Path, campaign: Mapping[str, object], market: str,
    predecessor: tuple[Path, Mapping[str, object]] | None,
) -> tuple[Path, Path, dict[str, object]]:
    market_root = root / STATE_ROOT / str(campaign["campaign_id"]) / "markets" / market
    prior = [
        (int(value.get("campaign_attempt_ordinal", 0)), path, value)
        for path in market_root.glob("*/build_plan.json")
        for value in (_json(path),)
    ] if market_root.is_dir() else []
    prior.sort(key=lambda item: item[0])
    reuse = _reuse_binding(root, prior[-1][2]) if prior else None
    ordinal = (prior[-1][0] if prior else 0) + 1
    entries = _market_entries(root, campaign, market)
    dbns = [item for item in entries if item["kind"] == "DBN"]
    sidecars = [item for item in entries if item["kind"] == "SIDECAR"]
    predecessor_id = str(predecessor[1]["certificate_id"]) if predecessor else None
    attempt_id = _market_attempt_id(
        campaign_id=str(campaign["campaign_id"]), market=market, ordinal=ordinal,
        source_entries_sha256=sha256_json(entries),
        predecessor_certificate_id=predecessor_id,
        reuse_failure_sha256=(reuse["predecessor_failure_sha256"] if reuse else None),
    )
    attempt_root = market_root / attempt_id
    inventory_path = attempt_root / "source_inventory.json"
    inventory_core = {
        "schema_version": "annual_dbn_gate_b_market_source_inventory/1.0.0",
        "target_market": market,
        "parent_inventory_path": campaign["source"]["inventory_path"],
        "parent_inventory_sha256": campaign["source"]["inventory_sha256"],
        "entries": entries,
    }
    _write(inventory_path, {**inventory_core, "inventory_id": sha256_json(inventory_core)})
    source = {
        "source_contract_id": campaign["source"]["source_contract_id"],
        "canonical_release_id": campaign["source"]["canonical_release_id"],
        "inventory_path": inventory_path.relative_to(root).as_posix(),
        "inventory_sha256": sha256_file(inventory_path),
        "exact_source_entries_sha256": sha256_json(entries),
        "exact_dbn_entries_sha256": sha256_json(dbns),
        "exact_source_entry_count": len(entries),
        "exact_dbn_file_count": len(dbns),
        "exact_sidecar_file_count": len(sidecars),
        "total_source_bytes": sum(int(item["size_bytes"]) for item in entries),
        "maximum_payload_bytes": sum(int(item["size_bytes"]) for item in dbns),
        "work_unit_count": len({int(item["year"]) for item in entries}),
    }
    core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "execution_role": COMPLETE_MARKET_EXECUTION_ROLE,
        "operation": CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        "target_market": market,
        "attempt_id": attempt_id,
        "campaign_attempt_ordinal": ordinal,
        "checkpoint_set": campaign["checkpoint_set"],
        "checkpoint_set_id": campaign["checkpoint_set_id"],
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source": source,
        "output_staging_path": (
            f"data/causally_gated_normalized/v10/{campaign['checkpoint_set_id']}/"
            f"{market}/{attempt_id}"
        ),
        "development_end_exclusive": "2025-07-13T22:00:00Z",
        "holdout_allowed": False,
        "forward_allowed": False,
        "provider_calls": 0,
        "execution_authorized": False,
        "authority": {
            name: False for name in (
                "activation", "evaluation", "features", "fitting", "forward",
                "holdout", "mechanism", "outcomes", "prediction", "provider",
                "publication", "wfa",
            )
        },
        "limits": {
            "maximum_payload_bytes": source["maximum_payload_bytes"],
            "maximum_decoded_records": 1_000_000_000,
            "maximum_output_bytes": MARKET_OUTPUT_LIMIT,
            "maximum_partition_count": MARKET_PARTITION_LIMIT,
        },
        "execution": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_workers": 1,
            "python_executable": PINNED_PYTHON_EXECUTABLE,
            "databento_version": SUPPORTED_DATABENTO_VERSION,
        },
        "durable_host": expected_durable_host_plan(market, attempt_id),
        "economics": {
            "rulebook_path": ECONOMICS_RULEBOOK_PATH,
            "rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        },
        "reuse_failed_market_partitions": False,
    }
    if reuse:
        core["sealed_work_unit_reuse"] = reuse
    build_plan = {**core, "plan_id": sha256_json(core)}
    if predecessor is not None:
        build_plan = bind_predecessor_market_certificate(
            repository_root=root,
            build_plan=build_plan,
            certificate_path=predecessor[0].relative_to(root).as_posix(),
        )
    plan_path = attempt_root / "build_plan.json"
    receipt_path = attempt_root / "build_receipt.json"
    _write(plan_path, build_plan)
    validate_market_checkpoint_plan(root, build_plan)
    scope = required_market_checkpoint_scope(
        plan=build_plan,
        plan_sha256=sha256_file(plan_path),
        source_contract_id=str(source["source_contract_id"]),
        canonical_release_id=str(source["canonical_release_id"]),
    )
    receipt = _approved_receipt(root, plan=build_plan, plan_path=plan_path, scope=scope)
    _write(receipt_path, receipt.as_dict())
    return plan_path, receipt_path, build_plan


def run_market_worker(root: Path, plan_path: Path, receipt_path: Path) -> dict[str, object]:
    plan = _json(plan_path)
    from .boundary import OperationReceipt

    receipt = OperationReceipt.from_dict(_json(receipt_path))
    if plan["target_market"] == MARKET_ORDER[0]:
        callback = lambda: run_authorized_market_checkpoint(
            repository_root=root, receipt=receipt, plan_path=plan_path
        )
    else:
        callback = lambda: run_authorized_sequenced_market_checkpoint(
            repository_root=root, receipt=receipt, plan_path=plan_path
        )
    result = run_durable_full_build_worker(
        repository_root=root, plan=plan, operation=callback
    )
    core = {
        "schema_version": "annual_dbn_gate_b_market_worker_result/1.0.0",
        "status": "PASS_MARKET_CHECKPOINT_INACTIVE",
        "market": plan["target_market"],
        "plan_id": plan["plan_id"],
        "attempt_id": plan["attempt_id"],
        "receipt_id": receipt.receipt_id,
        "checkpoint_path": result.result_path.relative_to(root).as_posix(),
        "checkpoint_result_id": result.payload["result_id"],
        "publication_authorized": False,
        "activation_authorized": False,
    }
    payload = {**core, "result_id": sha256_json(core)}
    _write(plan_path.parent / "build_result.json", payload)
    return payload


def _register_and_wait(root: Path, plan_path: Path, receipt_path: Path, plan: Mapping[str, object]) -> None:
    script = root / EXECUTION_SCRIPT
    task_registered = False
    task_name = str(plan["durable_host"]["task_name"])
    try:
        register = subprocess.run(
            [
                "powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
                "-File", str(script), "-Mode", "RegisterMarket", "-PlanPath", str(plan_path),
                "-ReceiptPath", str(receipt_path),
            ],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if register.returncode != 0:
            output = (register.stdout or "") + (register.stderr or "")
            if "Access denied" not in output:
                raise subprocess.CalledProcessError(
                    register.returncode, register.args, output=output
                )
            python = root / ".venv" / "Scripts" / "python.exe"
            env = os.environ.copy()
            env[CONTROLLER_ENVIRONMENT_KEY] = task_name
            env[DURABLE_HOST_ENVIRONMENT_KEY] = task_name
            subprocess.run(
                [
                    str(python), "-m", "futures_rebuild.annual_dbn_causal_campaign",
                    "market-worker", "--plan-path", str(plan_path), "--receipt-path", str(receipt_path),
                ],
                cwd=root,
                check=True,
                env=env,
            )
        else:
            task_registered = True
    finally:
        if task_registered:
            task_name = str(plan["durable_host"]["task_name"])
            subprocess.run(
                [
                    "powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
                    f"$t=Get-ScheduledTask -TaskName '{task_name}' -ErrorAction SilentlyContinue;"
                    f"if($null -ne $t){{Unregister-ScheduledTask -TaskName '{task_name}' -Confirm:$false}}",
                ],
                cwd=root,
                check=True,
            )
    exit_path = root / str(plan["durable_host"]["evidence_path"]) / "exit.json"
    deadline = time.monotonic() + MAXIMUM_RUNTIME_SECONDS + 300
    while not io_path(exit_path).is_file():
        if time.monotonic() >= deadline:
            raise UnauthorizedOperation(f"scheduled market exceeded runtime ceiling: {plan['target_market']}")
        time.sleep(30)
    terminal = _json(exit_path)
    if terminal.get("status") != "PASS" or not (plan_path.parent / "build_result.json").is_file():
        raise IntegrityError(
            f"scheduled market failed closed for {plan['target_market']}: "
            f"{terminal.get('error_type')}: {terminal.get('error_message')}"
        )


def _certify_market(root: Path, plan_path: Path, build_plan: Mapping[str, object]) -> tuple[Path, dict[str, object]]:
    attempt_root = plan_path.parent
    cert_plan_path = attempt_root / "certification_plan.json"
    receipt_path = attempt_root / "certification_receipt.json"
    checkpoint_path = root / str(build_plan["output_staging_path"]) / "market_checkpoint.json"
    plan = build_market_certification_plan(
        repository_root=root,
        build_plan_path=plan_path.relative_to(root).as_posix(),
        build_plan=build_plan,
        build_plan_sha256=sha256_file(plan_path),
    )
    _write(cert_plan_path, plan)
    scope = required_market_certification_scope(
        plan=plan,
        plan_sha256=sha256_file(cert_plan_path),
        checkpoint_sha256=sha256_file(checkpoint_path),
    )
    receipt = _approved_receipt(root, plan=plan, plan_path=cert_plan_path, scope=scope)
    _write(receipt_path, receipt.as_dict())
    certificate = run_authorized_market_certification(
        repository_root=root, receipt=receipt, plan_path=cert_plan_path
    )
    certificate_path = root / str(plan["certificate_path"])
    if certificate.get("certificate_id") != _json(certificate_path).get("certificate_id"):
        raise IntegrityError("market certificate writeback differs")
    return certificate_path, certificate


def _event(root: Path, campaign_id: str, sequence: int, name: str, **details: object) -> None:
    core = {
        "schema_version": "annual_dbn_gate_b_market_campaign_event/1.0.0",
        "sequence": sequence,
        "event": name,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        **details,
    }
    _write_create_only(
        root / STATE_ROOT / campaign_id / "events" / f"{sequence:03d}-{name}.json",
        {**core, "event_id": sha256_json(core)},
    )


def run_campaign(root: Path, campaign_id: str) -> dict[str, object]:
    root = root.resolve(strict=True)
    campaign = _load_campaign(root, campaign_id, require_host=True)
    run_root = root / STATE_ROOT / campaign_id
    terminal_path = run_root / "terminal.json"
    if terminal_path.exists():
        raise UnauthorizedOperation("annual Gate-B campaign is already terminal")
    started_path = run_root / "started.json"
    sequence = len(tuple((run_root / "events").glob("*.json")))
    completed: list[str] = []
    try:
        if not started_path.exists():
            _quarantine_locks(
                root, campaign_id, campaign["stale_lock_remediation"]["locks"]
            )
            _write_create_only(
                started_path,
                {
                    "schema_version": "annual_dbn_gate_b_market_campaign_started/1.0.0",
                    "status": "RUNNING_INACTIVE",
                    "campaign_id": campaign_id,
                    "pid": os.getpid(),
                    "started_at": datetime.now(timezone.utc).isoformat(),
                },
            )
        else:
            _quarantine_resume_locks(root, campaign_id)
        predecessor: tuple[Path, dict[str, object]] | None = None
        for market in MARKET_ORDER:
            existing = _valid_certificate(
                root, str(campaign["checkpoint_set_id"]), market,
                str(campaign["source"]["source_contract_id"]),
                str(campaign["source"]["canonical_release_id"]),
            )
            if existing:
                predecessor = existing
                completed.append(market)
                continue
            if market != MARKET_ORDER[0] and predecessor is None:
                raise IntegrityError(f"missing predecessor certificate for {market}")
            validate_market_campaign_storage(
                free_bytes=shutil.disk_usage(root).free,
                remaining_market_count=len(MARKET_ORDER) - len(completed),
            )
            plan_path, receipt_path, build_plan = _prepare_market(
                root, campaign, market, predecessor
            )
            sequence += 1
            _event(root, campaign_id, sequence, "MARKET_BUILD_AUTHORIZED", market=market, plan_id=build_plan["plan_id"])
            _register_and_wait(root, plan_path, receipt_path, build_plan)
            sequence += 1
            _event(root, campaign_id, sequence, "MARKET_CHECKPOINT_PASS", market=market, result_id=_json(plan_path.parent / "build_result.json")["checkpoint_result_id"])
            predecessor = _certify_market(root, plan_path, build_plan)
            completed.append(market)
            sequence += 1
            _event(root, campaign_id, sequence, "MARKET_CERTIFICATE_PASS", market=market, certificate_id=predecessor[1]["certificate_id"])
        certificates = []
        for market in MARKET_ORDER:
            found = _valid_certificate(
                root, str(campaign["checkpoint_set_id"]), market,
                str(campaign["source"]["source_contract_id"]),
                str(campaign["source"]["canonical_release_id"]),
            )
            if found is None:
                raise IntegrityError("final Gate-B certificate set is incomplete")
            certificates.append(found[1])
        complete = certify_complete_market_certificate_set(
            repository_root=root,
            checkpoint_set=campaign["checkpoint_set"],
            market_certificates=certificates,
        )
        complete_path = (
            root / CERTIFICATE_ROOT / str(campaign["checkpoint_set_id"])
            / "complete_set" / f"{complete['certificate_id']}.json"
        )
        _write(complete_path, complete)
        core = {
            "schema_version": "annual_dbn_gate_b_market_campaign_terminal/1.0.0",
            "status": "PASS_ALL_41_MARKETS_CERTIFIED_INACTIVE",
            "campaign_id": campaign_id,
            "checkpoint_set_id": campaign["checkpoint_set_id"],
            "complete_certificate_path": complete_path.relative_to(root).as_posix(),
            "complete_certificate_sha256": sha256_file(complete_path),
            "complete_certificate_id": complete["certificate_id"],
            "completed_markets": completed,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "publication_authorized": False,
            "activation_authorized": False,
        }
    except BaseException as exc:
        core = {
            "schema_version": "annual_dbn_gate_b_market_campaign_terminal/1.0.0",
            "status": "FAILED_CLOSED_CURRENT_MARKET_ONLY",
            "campaign_id": campaign_id,
            "checkpoint_set_id": campaign["checkpoint_set_id"],
            "completed_markets": completed,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "automatic_retry_authorized": False,
            "publication_authorized": False,
            "activation_authorized": False,
        }
    terminal = {**core, "terminal_id": sha256_json(core)}
    _write_create_only(terminal_path, terminal)
    return terminal


def status(root: Path, campaign_id: str) -> dict[str, object]:
    root = root.resolve(strict=True)
    run_root = root / STATE_ROOT / campaign_id
    plan = _json(run_root / "campaign_plan.json")
    terminal = _json(run_root / "terminal.json") if (run_root / "terminal.json").exists() else None
    completed = _certificate_prefix(
        root, str(plan["checkpoint_set_id"]), str(plan["source"]["source_contract_id"]),
        str(plan["source"]["canonical_release_id"]),
    )
    return {
        "status": terminal["status"] if terminal else ("RUNNING" if (run_root / "started.json").exists() else "PREPARED_NOT_STARTED"),
        "campaign_id": campaign_id,
        "certified_markets": len(completed),
        "total_markets": len(MARKET_ORDER),
        "next_market": MARKET_ORDER[len(completed)] if len(completed) < len(MARKET_ORDER) else None,
        "active_catalog_sha256": sha256_file(root / ACTIVE_CATALOG),
        "active_catalog_unchanged": sha256_file(root / ACTIVE_CATALOG) == plan["active_catalog"]["sha256"],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("prepare", "validate", "campaign", "market-worker", "status"))
    parser.add_argument("--campaign-id")
    parser.add_argument("--plan-path")
    parser.add_argument("--receipt-path")
    args = parser.parse_args()
    root = Path.cwd().resolve(strict=True)
    if args.command == "prepare":
        result = prepare_campaign(root)
    elif args.command == "market-worker":
        if not args.plan_path or not args.receipt_path:
            parser.error("--plan-path and --receipt-path are required")
        result = run_market_worker(root, Path(args.plan_path).resolve(strict=True), Path(args.receipt_path).resolve(strict=True))
    else:
        if not args.campaign_id:
            parser.error("--campaign-id is required")
        result = globals()[{"validate": "validate_campaign", "campaign": "run_campaign", "status": "status"}[args.command]](root, args.campaign_id)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
