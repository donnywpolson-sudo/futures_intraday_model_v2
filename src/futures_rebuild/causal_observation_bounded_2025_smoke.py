"""One-use 6A/2025 runtime smoke for the causal-observation full-build path.

The module reuses the active source selector, decoder, compact writer, and
independent verifier.  It is inert without an exact external receipt and can
create only inactive staging output.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from .boundary import OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .causal_observation_canary import (
    _CanaryStageCreator,
    _build_market_candidate_with_state,
    _decode_selected_sources,
    _load_economics_rulebook,
)
from .causal_observation_foundation import (
    CAUSAL_OBSERVATION_CONTRACT_ID,
    ECONOMICS_RULEBOOK_PATH,
    ECONOMICS_RULEBOOK_SHA256,
    CausalObservationOperationContext,
    authorize_bounded_2025_smoke_row_read,
    prepared_inventory,
)
from .causal_observation_full_build import (
    BOUNDARY_SOURCE_FAMILIES,
    BOUNDARY_START_INCLUSIVE,
    DEVELOPMENT_END_EXCLUSIVE,
    MINIMUM_FREE_AFTER_PEAK_BYTES,
    _market_windows,
    _month_windows,
    _slice_decoded,
    validate_complete_development_boundary_metadata,
)
from .causal_observation_verifier import verify_observation_candidate
from .causal_source_closure import select_exact_standard_source_entries
from .data_layout import STAGING_ROOT
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation_operation_firewall import issue_current_source_closure_context
from .locking import FileLease
from .research_gateway_policy import (
    CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
)


PLAN_SCHEMA = "bounded_2025_causal_observation_smoke_plan/1.0.0"
RESULT_SCHEMA = "bounded_2025_causal_observation_smoke_result/1.0.0"
FAILURE_SCHEMA = "bounded_2025_causal_observation_smoke_failure/1.0.0"
MARKET = "6A"
YEAR = 2025
EXPECTED_ENTRY_COUNT = 14
EXPECTED_DBN_COUNT = 7
EXPECTED_SIDECAR_COUNT = 7
MAXIMUM_RUNTIME_SECONDS = 7_200
MAXIMUM_PARTITION_COUNT = 7
MAXIMUM_OUTPUT_BYTES = 750_000_000
MAXIMUM_PEAK_ADDITIONAL_BYTES = 1_000_000_000


@dataclass(frozen=True, slots=True)
class Bounded2025SmokeResult:
    result_path: Path
    payload: Mapping[str, object]


def _json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"bounded-2025 smoke JSON is invalid: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"bounded-2025 smoke JSON is not an object: {path}")
    return value


def _contained(root: Path, relative: object) -> Path:
    if type(relative) is not str or not relative:
        raise ContractError("bounded-2025 smoke path is absent")
    value = Path(relative)
    if value.is_absolute() or ".." in value.parts or value.as_posix() != relative:
        raise ContractError("bounded-2025 smoke path is not canonical and relative")
    candidate = (root / value).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation("bounded-2025 smoke path escapes the repository") from exc
    return candidate


def _write_create_only(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(dict(payload)) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _active_source(root: Path) -> tuple[dict[str, object], str, str]:
    contract = _json(root / "configs/source_contract.json")
    contract_id = contract.get("contract_id")
    source = contract.get("active_canonical_source")
    core = {key: value for key, value in contract.items() if key != "contract_id"}
    if (
        type(contract_id) is not str
        or sha256_json(core) != contract_id
        or not isinstance(source, Mapping)
        or type(source.get("release_id")) is not str
    ):
        raise IntegrityError("active bounded-2025 smoke source identity differs")
    return contract, contract_id, str(source["release_id"])


def _validate_plan(root: Path, plan: Mapping[str, object]) -> None:
    source = plan.get("source")
    limits = plan.get("limits")
    execution = plan.get("execution")
    storage = plan.get("storage")
    authority = plan.get("authority")
    economics = plan.get("economics")
    window = plan.get("window")
    bindings = plan.get("implementation_bindings")
    contract, contract_id, release_id = _active_source(root)
    policy = contract.get("selection_policy")
    if (
        plan.get("schema_version") != PLAN_SCHEMA
        or plan.get("operation")
        != CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION
        or plan.get("causal_contract_id") != CAUSAL_OBSERVATION_CONTRACT_ID
        or plan.get("status") != "PREPARED_NOT_AUTHORIZED_NO_ROW_READ"
        or plan.get("development_end_exclusive") != DEVELOPMENT_END_EXCLUSIVE
        or plan.get("roots") != [MARKET]
        or plan.get("holdout_allowed") is not False
        or plan.get("forward_allowed") is not False
        or plan.get("provider_calls") != 0
        or plan.get("execution_authorized") is not False
        or not isinstance(source, Mapping)
        or not isinstance(limits, Mapping)
        or not isinstance(execution, Mapping)
        or not isinstance(storage, Mapping)
        or not isinstance(authority, Mapping)
        or not isinstance(window, Mapping)
        or not isinstance(bindings, Mapping)
        or any(bool(value) for value in authority.values())
        or source.get("source_contract_id") != contract_id
        or source.get("canonical_release_id") != release_id
        or source.get("exact_source_entry_count") != EXPECTED_ENTRY_COUNT
        or source.get("exact_dbn_file_count") != EXPECTED_DBN_COUNT
        or source.get("exact_sidecar_file_count") != EXPECTED_SIDECAR_COUNT
        or source.get("maximum_payload_bytes")
        != limits.get("maximum_payload_bytes")
        or not isinstance(policy, Mapping)
        or window
        != {
            "start": BOUNDARY_START_INCLUSIVE,
            "end": DEVELOPMENT_END_EXCLUSIVE,
        }
        or limits.get("maximum_output_bytes") != MAXIMUM_OUTPUT_BYTES
        or limits.get("maximum_partition_count") != MAXIMUM_PARTITION_COUNT
        or not isinstance(limits.get("maximum_payload_bytes"), int)
        or int(limits["maximum_payload_bytes"]) <= 0
        or not isinstance(limits.get("maximum_decoded_records"), int)
        or int(limits["maximum_decoded_records"]) <= 0
        or execution
        != {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_workers": 1,
        }
        or storage
        != {
            "activation_authorized": False,
            "maximum_peak_additional_bytes": MAXIMUM_PEAK_ADDITIONAL_BYTES,
            "publication_authorized": False,
            "required_free_after_peak_bytes": MINIMUM_FREE_AFTER_PEAK_BYTES,
        }
        or economics
        != {
            "rulebook_path": ECONOMICS_RULEBOOK_PATH,
            "rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        }
        or plan.get("reuse_prior_receipt") is not False
        or plan.get("reuse_prior_partitions") is not False
    ):
        raise UnauthorizedOperation("bounded-2025 smoke plan is not exact and nonauthorizing")
    plan_id = plan.get("plan_id")
    if type(plan_id) is not str or sha256_json(
        {key: value for key, value in plan.items() if key != "plan_id"}
    ) != plan_id:
        raise IntegrityError("bounded-2025 smoke plan identity differs")
    required_bindings = {
        "configs/causal_observation_contract_v1.json",
        "configs/contract_economics_rules.json",
        "configs/source_contract.json",
        "src/futures_rebuild/causal_observation_bounded_2025_smoke.py",
        "src/futures_rebuild/causal_observation_canary.py",
        "src/futures_rebuild/causal_observation_foundation.py",
        "src/futures_rebuild/causal_observation_full_build.py",
        "src/futures_rebuild/causal_observation_verifier.py",
        "src/futures_rebuild/causal_source_closure.py",
        "src/futures_rebuild/foundation/decoder.py",
        "src/futures_rebuild/research_gateway_policy.py",
    }
    if set(bindings) != required_bindings:
        raise IntegrityError("bounded-2025 smoke implementation binding set differs")
    for relative, expected in bindings.items():
        path = _contained(root, relative)
        if type(expected) is not str or sha256_file(path) != expected:
            raise IntegrityError(
                f"bounded-2025 smoke implementation binding differs: {relative}"
            )
    inventory = _contained(root, source.get("inventory_path"))
    if sha256_file(inventory) != source.get("inventory_sha256"):
        raise IntegrityError("bounded-2025 smoke inventory differs")
    output = _contained(root, plan.get("output_staging_path"))
    try:
        output.relative_to(root / STAGING_ROOT)
    except ValueError as exc:
        raise UnauthorizedOperation("bounded-2025 smoke output is outside staging") from exc


def _load_exact_source_entries(
    root: Path, plan: Mapping[str, object]
) -> tuple[dict[str, object], ...]:
    source = plan["source"]
    inventory = _json(_contained(root, source["inventory_path"]))
    entries = inventory.get("entries")
    if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
        raise IntegrityError("bounded-2025 smoke inventory entries are absent")
    selected = tuple(dict(item) for item in entries)
    dbns = tuple(item for item in selected if item.get("kind") == "DBN")
    sidecars = tuple(item for item in selected if item.get("kind") == "SIDECAR")
    expected_pairs = {
        (family, kind) for family in BOUNDARY_SOURCE_FAMILIES for kind in ("DBN", "SIDECAR")
    }
    if (
        len(selected) != EXPECTED_ENTRY_COUNT
        or len(dbns) != EXPECTED_DBN_COUNT
        or len(sidecars) != EXPECTED_SIDECAR_COUNT
        or {(item.get("family"), item.get("kind")) for item in selected}
        != expected_pairs
        or any(item.get("market") != MARKET or item.get("year") != YEAR for item in selected)
        or sha256_json(selected) != source.get("exact_source_entries_sha256")
        or sum(int(item["size_bytes"]) for item in selected)
        != source.get("total_source_bytes")
        or sum(int(item["size_bytes"]) for item in dbns)
        != source.get("maximum_payload_bytes")
    ):
        raise IntegrityError("bounded-2025 smoke source identity or counts differ")
    return selected


def _execute(
    *,
    root: Path,
    boundary: RepoBoundary,
    plan: Mapping[str, object],
    plan_sha256: str,
    context: CausalObservationOperationContext,
    selected: Sequence[Mapping[str, object]],
    progress: dict[str, object],
    started: float,
) -> Bounded2025SmokeResult:
    output = _contained(root, plan["output_staging_path"])
    relative_output = output.relative_to(root / STAGING_ROOT).as_posix()
    contract, _, _ = _active_source(root)
    decoded = _decode_selected_sources(
        root=root,
        selected=selected,
        windows={MARKET: dict(plan["window"])},
        source_contract=contract,
        maximum_decoded_records=int(plan["limits"]["maximum_decoded_records"]),
    )[MARKET]
    progress.update(
        {
            "dbn_files_opened": EXPECTED_DBN_COUNT,
            "dbn_payload_bytes_opened": int(plan["limits"]["maximum_payload_bytes"]),
            "decoded_record_count": decoded.decoded_record_count,
        }
    )
    if decoded.decoded_record_count > int(plan["limits"]["maximum_decoded_records"]):
        raise UnauthorizedOperation("bounded-2025 smoke decoded-record ceiling exceeded")
    economics = _load_economics_rulebook(root)
    prior_observation: Mapping[str, object] | None = None
    carried_support: tuple[tuple[int, str, str], ...] = ()
    partitions: list[dict[str, object]] = []
    output_bytes = 0
    for start_ns, end_ns, interval, month_window in _month_windows(
        BOUNDARY_START_INCLUSIVE, DEVELOPMENT_END_EXCLUSIVE
    ):
        if time.monotonic() - started > MAXIMUM_RUNTIME_SECONDS:
            raise UnauthorizedOperation("bounded-2025 smoke runtime ceiling exceeded")
        month = _slice_decoded(
            decoded,
            start_ns=start_ns,
            end_ns=end_ns,
            definitions=decoded.definitions,
            carried_support=carried_support,
        )
        if not month.primary_1m:
            continue
        creator = _CanaryStageCreator(
            boundary=boundary,
            relative=f"{relative_output}/{MARKET}/{YEAR}/{interval}",
        )
        built = _build_market_candidate_with_state(
            publisher=creator,
            context=context,
            market=MARKET,
            window=month_window,
            decoded=month,
            allowed_roots=frozenset({MARKET}),
            economics_rulebook=economics,
            prior_observation=prior_observation,
        )
        certificate = verify_observation_candidate(
            stage=built.prepared.stage,
            manifest=built.prepared.manifest,
            economics_rulebook=economics,
        )
        inventory = prepared_inventory(built.prepared)
        partition_bytes = sum(int(item["size"]) for item in inventory["files"])
        output_bytes += partition_bytes
        partitions.append(
            {
                "market": MARKET,
                "year": YEAR,
                "interval": interval,
                "release_id": built.prepared.manifest.release_id,
                "certificate_id": certificate["certificate_id"],
                "inventory_sha256": inventory["files_sha256"],
                "output_bytes": partition_bytes,
                "stage": built.prepared.stage.relative_to(root).as_posix(),
            }
        )
        prior_observation = built.last_observation
        last_bar_end = int(built.last_observation["bar_end_ns"])
        carried_support = tuple(
            row for row in decoded.support_rows if last_bar_end <= row[0] < end_ns
        )
        progress["complete_partition_count"] = len(partitions)
        progress["output_bytes"] = output_bytes
        if len(partitions) > MAXIMUM_PARTITION_COUNT:
            raise UnauthorizedOperation("bounded-2025 smoke partition ceiling exceeded")
        if output_bytes > MAXIMUM_OUTPUT_BYTES:
            raise UnauthorizedOperation("bounded-2025 smoke output byte ceiling exceeded")
    if not partitions:
        raise IntegrityError("bounded-2025 smoke produced no verified partitions")
    core: dict[str, object] = {
        "schema_version": RESULT_SCHEMA,
        "status": "PASS_INACTIVE_BOUNDED_2025_SMOKE_NOT_PUBLISHED_NOT_ACTIVE",
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "receipt_id": context.receipt_id,
        "source_contract_id": context.source_contract_id,
        "source_release_id": context.source_release_id,
        "causal_contract_id": context.causal_contract_id,
        "market": MARKET,
        "year": YEAR,
        "source_entry_count": len(selected),
        "dbn_file_count": EXPECTED_DBN_COUNT,
        "payload_bytes_opened": int(plan["limits"]["maximum_payload_bytes"]),
        "decoded_record_count": decoded.decoded_record_count,
        "partition_count": len(partitions),
        "output_bytes": output_bytes,
        "partition_inventory_sha256": sha256_json(partitions),
        "partitions": partitions,
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
    }
    payload = {**core, "result_id": sha256_json(core)}
    result_path = output / "smoke_result.json"
    _write_create_only(result_path, payload)
    return Bounded2025SmokeResult(result_path=result_path, payload=payload)


def run_authorized_bounded_2025_smoke(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path
) -> Bounded2025SmokeResult:
    """Consume one exact receipt and run the inactive 6A/2025 smoke once."""

    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(root)
    path = plan_path.resolve(strict=True)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation("bounded-2025 smoke plan is outside the repository") from exc
    plan = _json(path)
    plan_sha = sha256_file(path)
    _validate_plan(root, plan)
    _load_economics_rulebook(root)
    selected = _load_exact_source_entries(root, plan)
    validate_complete_development_boundary_metadata(
        root, selected, standard_roots=frozenset({MARKET})
    )
    closure_context = issue_current_source_closure_context(root)
    selected = select_exact_standard_source_entries(
        root,
        operation_context=closure_context,
        source_entries=selected,
        windows={MARKET: dict(plan["window"])},
    )
    output = _contained(root, plan["output_staging_path"])
    if output.exists():
        raise IntegrityError("bounded-2025 smoke output staging path already exists")
    free = shutil.disk_usage(root).free
    if free - MAXIMUM_PEAK_ADDITIONAL_BYTES < MINIMUM_FREE_AFTER_PEAK_BYTES:
        raise UnauthorizedOperation("bounded-2025 smoke storage floor would be breached")
    global_lock = root / "state/locks/foundation-build.lock"
    run_lock = root / f"state/locks/causal-observation-smoke-{plan['plan_id']}.lock"
    if global_lock.exists() or run_lock.exists():
        raise UnauthorizedOperation("bounded-2025 smoke build lock is already active")
    context = authorize_bounded_2025_smoke_row_read(
        boundary=boundary,
        receipt=receipt,
        plan=plan,
        plan_sha256=plan_sha,
    )
    progress: dict[str, object] = {
        "dbn_files_opened": 0,
        "dbn_payload_bytes_opened": 0,
        "decoded_record_count": 0,
        "complete_partition_count": 0,
        "output_bytes": 0,
    }
    try:
        with FileLease(global_lock), FileLease(run_lock):
            return _execute(
                root=root,
                boundary=boundary,
                plan=plan,
                plan_sha256=plan_sha,
                context=context,
                selected=selected,
                progress=progress,
                started=time.monotonic(),
            )
    except Exception as exc:
        failure = {
            "schema_version": FAILURE_SCHEMA,
            "status": "FAILED_AUTHORIZATION_CONSUMED_NO_AUTOMATIC_RETRY",
            "plan_id": plan["plan_id"],
            "plan_sha256": plan_sha,
            "receipt_id": context.receipt_id,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "progress": progress,
            "terminal": True,
            "receipt_reuse_authorized": False,
            "partial_partition_reuse_authorized": False,
            "automatic_retry_authorized": False,
            "required_successor": "NEW_PLAN_NEW_RECEIPT_NEW_OUTPUT_ROOT",
            "publication_authorized": False,
            "activation_authorized": False,
        }
        output.mkdir(parents=True, exist_ok=True)
        failure_path = output / "failure.json"
        if not failure_path.exists():
            _write_create_only(failure_path, failure)
        raise
