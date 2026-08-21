"""Prepare and certify the non-active MSF OHLCV-1D publication successor.

The module is deliberately provider-free.  Preparation creates a small,
plain-file candidate addition and an immutable complete virtual inventory;
activation is a separately authorized one-time transaction.  The scope is
exactly the nine retained MSF daily partitions and cannot be widened through
arguments.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from .boundary import OperationClassification, OperationReceipt, RepoBoundary


MANIFEST_REL = Path(
    "reports/ohlcv_58_completion/ohlcv58_20260821T0437427849423Z/"
    "execution_manifest.jsonl"
)
MANIFEST_SHA256 = "25df3d5fb71a0c0113c5f3214fde6d8d4f796b1f0337b387e2fafa330910d588"
LEDGER_REL = Path(
    "state/o58/e41c77a3/state/provider_acquisition_staging/"
    "ohlcv_1d_1h_historical_backfill/25df3d5fb71a0c01/job_ledger.jsonl"
)
LEDGER_SHA256 = "8e7655103cafec3780b4a32cd2b80a09a48ff3daf7b89d2b32fb7b84d47d5a3c"
STATE_ROOT_REL = Path("state/o58/e41c77a3")
CANARY_REL = STATE_ROOT_REL / "data/dbn/ohlcv_1d/MSF"
ACTIVE_POINTER_REL = Path("configs/active_dbn_congruence_release_v1.json")
EXPECTED_POINTER_SHA256 = "b35492071f932c5ad38ba861ba0afe8a3c5a15e60b0ed9407e7f6b70f98ef978"
EXPECTED_PRIOR_RELEASE_ID = "6fa4357699ac573a6b5405b736dbbcd4b46a33654be2c065a404cdb0ac3446b4"
CANONICAL_ROOT_REL = Path("data/dbn")
LOCK_REL = Path("state/locks/canonical_dbn_msf_ohlcv1d_publication_v1.lock")
REPORT_PARENT_REL = Path("reports/ohlcv_msf_1d_publication_successor")
SHADOW_PARENT_REL = Path("state/data_publication_staging")
REGISTRY_PARENT_REL = Path("state/dbn_congruence_release_registry")
ARCHIVE_PARENT_REL = Path("state/dbn_congruence_release_archive")
YEARS = tuple(range(2018, 2027))
EXPECTED_RECORDS = 2591
EXPECTED_DBN_BYTES = 57292
EXPECTED_CANARY_BYTES = 74635
EXPECTED_PRIOR_FILES = 9908
EXPECTED_PRIOR_BYTES = 23401842795
EXPECTED_SUCCESSOR_FILES = 9926
EXPECTED_SUCCESSOR_BYTES = 23401917430
EXPECTED_PRIOR_UNITS = 356
EXPECTED_SUCCESSOR_UNITS = 365
EXPECTED_SUCCESSOR_ARTIFACTS = 730
JOB_ID = "GLBX-20260821-YCFX9XW8SF"
PUBLICATION_OPERATION = "PUBLISH_MSF_OHLCV_1D_CANARY"
PUBLICATION_APPROVAL_COMMAND = "PUBLISH_MSF_OHLCV_1D_CANARY"


class SuccessorError(RuntimeError):
    """Raised when any immutable scope, integrity, or transaction gate fails."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def serialized_json(value: Any) -> bytes:
    return canonical_bytes(value) + b"\n"


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_json(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SuccessorError(f"unable to read {label}: {path}") from exc
    if not isinstance(value, dict):
        raise SuccessorError(f"{label} is not a JSON object")
    return value


def load_jsonl(path: Path, label: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            value = json.loads(line)
            if not isinstance(value, dict):
                raise SuccessorError(f"{label} line {number} is not an object")
            rows.append(value)
    except (OSError, json.JSONDecodeError) as exc:
        raise SuccessorError(f"unable to read {label}: {path}") from exc
    return rows


def contained(root: Path, relative: Path) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise SuccessorError(f"unsafe project-relative path: {relative}")
    candidate = (root / relative).resolve(strict=False)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise SuccessorError(f"path escapes repository: {relative}") from exc
    return candidate


def create_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise SuccessorError(f"create-only artifact already exists: {path}") from exc


def create_json(path: Path, value: Any) -> None:
    create_bytes(path, serialized_json(value))


def atomic_replace_bytes(path: Path, value: bytes, suffix: str) -> None:
    temporary = path.with_name(f".{path.name}.{suffix}.tmp")
    if temporary.exists():
        raise SuccessorError(f"pointer temporary path already exists: {temporary}")
    create_bytes(temporary, value)
    os.replace(temporary, path)


def _inventory(root: Path, *, include_links: bool = False) -> list[dict[str, Any]]:
    if not root.is_dir():
        raise SuccessorError(f"inventory root is absent: {root}")
    rows: list[dict[str, Any]] = []
    for directory, directories, filenames in os.walk(root):
        directories.sort()
        for filename in sorted(filenames):
            path = Path(directory) / filename
            stat = path.stat()
            row: dict[str, Any] = {
                "relative_path": path.relative_to(root).as_posix(),
                "size_bytes": stat.st_size,
                "sha256": sha256_file(path),
            }
            if include_links:
                row["link_count"] = int(stat.st_nlink)
            rows.append(row)
    rows.sort(key=lambda item: item["relative_path"])
    return rows


def _inventory_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    normalized = [
        {
            "relative_path": str(item["relative_path"]),
            "size_bytes": int(item["size_bytes"]),
            "sha256": str(item["sha256"]),
        }
        for item in rows
    ]
    return {
        "file_count": len(normalized),
        "total_bytes": sum(item["size_bytes"] for item in normalized),
        "inventory_sha256": sha256_json(normalized),
    }


def _expected_partition(year: int) -> tuple[str, str, str]:
    if year == 2026:
        end = "2026-07-14"
    else:
        end = f"{year + 1:04d}-01-01"
    stem = f"{year:04d}-01-01_{end}.dbn.zst"
    return stem, f"{year:04d}-01-01T00:00:00Z", f"{end}T00:00:00Z"


def validate_canary(root: Path) -> dict[str, Any]:
    manifest = contained(root, MANIFEST_REL)
    ledger = contained(root, LEDGER_REL)
    if sha256_file(manifest) != MANIFEST_SHA256:
        raise SuccessorError("execution manifest hash drifted")
    if sha256_file(ledger) != LEDGER_SHA256:
        raise SuccessorError("job ledger hash drifted")
    canary = contained(root, CANARY_REL)
    files = sorted(path for path in canary.rglob("*") if path.is_file())
    expected_paths: set[str] = set()
    partitions: list[dict[str, Any]] = []
    total_records = 0
    total_dbn_bytes = 0
    total_bytes = 0
    for year in YEARS:
        stem, start, end = _expected_partition(year)
        dbn = canary / str(year) / stem
        sidecar_path = dbn.with_name(dbn.name + ".manifest.json")
        expected_paths.update(
            {dbn.relative_to(canary).as_posix(), sidecar_path.relative_to(canary).as_posix()}
        )
        if not dbn.is_file() or not sidecar_path.is_file():
            raise SuccessorError(f"missing canary partition pair for {year}")
        sidecar = load_json(sidecar_path, f"MSF {year} sidecar")
        if sidecar.get("schema_version") != "ohlcv_historical_backfill_sidecar/1.0.0":
            raise SuccessorError(f"MSF {year} sidecar schema differs")
        if sidecar.get("job_id") != JOB_ID or sidecar.get("market") != "MSF":
            raise SuccessorError(f"MSF {year} provenance differs")
        if sidecar.get("dataset") != "GLBX.MDP3" or sidecar.get("databento_schema") != "ohlcv-1d":
            raise SuccessorError(f"MSF {year} provider contract differs")
        if sidecar.get("request_start_inclusive") != start or sidecar.get("request_end_exclusive") != end:
            raise SuccessorError(f"MSF {year} request interval differs")
        dbn_size = dbn.stat().st_size
        dbn_hash = sha256_file(dbn)
        if sidecar.get("sha256") != dbn_hash or int(sidecar.get("dbn_byte_size", -1)) != dbn_size:
            raise SuccessorError(f"MSF {year} sidecar byte binding differs")
        records = int(sidecar.get("record_count", -1))
        if records <= 0:
            raise SuccessorError(f"MSF {year} record count is invalid")
        total_records += records
        total_dbn_bytes += dbn_size
        total_bytes += dbn_size + sidecar_path.stat().st_size
        partitions.append(
            {
                "year": year,
                "dbn_source_path": dbn.relative_to(root).as_posix(),
                "sidecar_source_path": sidecar_path.relative_to(root).as_posix(),
                "future_dbn_path": f"data/dbn/ohlcv_1d/MSF/{year}/{stem}",
                "future_sidecar_path": f"data/dbn/ohlcv_1d/MSF/{year}/{stem}.manifest.json",
                "dbn_sha256": dbn_hash,
                "dbn_size_bytes": dbn_size,
                "sidecar_sha256": sha256_file(sidecar_path),
                "sidecar_size_bytes": sidecar_path.stat().st_size,
                "record_count": records,
                "start_inclusive": start,
                "end_exclusive": end,
                "request_fingerprint": sidecar.get("request_fingerprint"),
            }
        )
    actual_paths = {path.relative_to(canary).as_posix() for path in files}
    if actual_paths != expected_paths:
        raise SuccessorError(
            f"canary path set differs: missing={sorted(expected_paths-actual_paths)}, "
            f"unexpected={sorted(actual_paths-expected_paths)}"
        )
    if (total_records, total_dbn_bytes, total_bytes) != (
        EXPECTED_RECORDS,
        EXPECTED_DBN_BYTES,
        EXPECTED_CANARY_BYTES,
    ):
        raise SuccessorError("canary totals differ from the retained completion evidence")

    # This is the real resume implementation with a provider factory that can
    # only fail.  A valid completed canary must return before construction.
    from .ohlcv_historical_backfill import execute_manifest

    def forbidden_provider(_: Path) -> Any:
        raise AssertionError("provider factory touched during provider-free certification")

    resume = execute_manifest(
        root=contained(root, STATE_ROOT_REL),
        manifest_path=manifest,
        execute=True,
        manifest_sha256=MANIFEST_SHA256,
        maximum_authorized_cost_usd="0",
        markets=("MSF",),
        schemas=("ohlcv-1d",),
        resume=True,
        provider_factory=forbidden_provider,
    )
    if resume.get("actions") != 0 or resume.get("state_counts") != {"COMPLETE_VALID": 9}:
        raise SuccessorError("repeat canary resume is not an offline nine-partition no-op")
    return {
        "status": "PASS_NINE_COMPLETE_VALID_PROVIDER_FREE_NOOP",
        "job_id": JOB_ID,
        "execution_manifest": {"path": MANIFEST_REL.as_posix(), "sha256": MANIFEST_SHA256},
        "job_ledger": {"path": LEDGER_REL.as_posix(), "sha256": LEDGER_SHA256},
        "state_root": STATE_ROOT_REL.as_posix(),
        "partition_count": 9,
        "record_count": total_records,
        "dbn_bytes": total_dbn_bytes,
        "combined_bytes": total_bytes,
        "resume_result": resume,
        "partitions": partitions,
    }


def validate_active(root: Path) -> dict[str, Any]:
    pointer_path = contained(root, ACTIVE_POINTER_REL)
    pointer_hash = sha256_file(pointer_path)
    if pointer_hash != EXPECTED_POINTER_SHA256:
        raise SuccessorError("active pointer differs from the reviewed publication baseline")
    pointer = load_json(pointer_path, "active pointer")
    if pointer.get("status") != "ACTIVE" or pointer.get("release_id") != EXPECTED_PRIOR_RELEASE_ID:
        raise SuccessorError("active pointer does not resolve the expected prior release")
    manifest_rel = Path(str(pointer.get("release_manifest_path")))
    manifest_path = contained(root, manifest_rel)
    if sha256_file(manifest_path) != pointer.get("release_manifest_sha256"):
        raise SuccessorError("prior release manifest differs from pointer binding")
    release = load_json(manifest_path, "prior release manifest")
    if release.get("release_id") != EXPECTED_PRIOR_RELEASE_ID:
        raise SuccessorError("prior release ID differs")
    units = release.get("release_core", {}).get("normalized_unit_manifests")
    artifacts = release.get("canonical_artifact_index")
    if not isinstance(units, list) or len(units) != EXPECTED_PRIOR_UNITS:
        raise SuccessorError("prior release unit count differs")
    if not isinstance(artifacts, list) or len(artifacts) != EXPECTED_PRIOR_UNITS * 2:
        raise SuccessorError("prior release artifact count differs")
    if any(item.get("unit_id", "").startswith("GLBX.MDP3|ohlcv-1d|MSF|") for item in units):
        raise SuccessorError("MSF daily units already exist in the active release")
    contract_rel = Path(str(pointer.get("activation_contract_path")))
    contract_path = contained(root, contract_rel)
    if sha256_file(contract_path) != pointer.get("activation_contract_sha256"):
        raise SuccessorError("active activation contract differs from pointer binding")
    contract = load_json(contract_path, "active activation contract")
    shadow_binding = contract.get("bound_plans", {}).get("shadow_validation")
    if not isinstance(shadow_binding, dict):
        raise SuccessorError("active activation contract lacks its complete shadow binding")
    certified_shadow_path = contained(root, Path(str(shadow_binding.get("path"))))
    if sha256_file(certified_shadow_path) != shadow_binding.get("sha256"):
        raise SuccessorError("active release shadow validation differs from contract binding")
    certified_shadow = load_json(certified_shadow_path, "active release shadow validation")
    certified_rows = certified_shadow.get("shadow_inventory")
    if not isinstance(certified_rows, list) or len(certified_rows) != EXPECTED_PRIOR_FILES:
        raise SuccessorError("active release complete certified inventory is invalid")
    expected_inventory = [
        {
            "relative_path": Path(str(item["future_project_relative_path"]))
            .relative_to(CANONICAL_ROOT_REL)
            .as_posix(),
            "size_bytes": int(item["size_bytes"]),
            "sha256": str(item["sha256"]),
        }
        for item in certified_rows
    ]
    expected_inventory.sort(key=lambda item: item["relative_path"])
    canonical = contained(root, CANONICAL_ROOT_REL)
    if (canonical / "ohlcv_1d/MSF").exists():
        raise SuccessorError("MSF daily canonical destination already exists")
    inventory = _inventory(canonical, include_links=True)
    summary = _inventory_summary(inventory)
    observed_inventory = [
        {key: item[key] for key in ("relative_path", "size_bytes", "sha256")}
        for item in inventory
    ]
    if observed_inventory != expected_inventory:
        raise SuccessorError("complete canonical root differs from the active release")
    if summary["file_count"] != EXPECTED_PRIOR_FILES or summary["total_bytes"] != EXPECTED_PRIOR_BYTES:
        raise SuccessorError("complete canonical root count or bytes differ")
    return {
        "pointer": pointer,
        "pointer_sha256": pointer_hash,
        "release": release,
        "release_path": manifest_rel.as_posix(),
        "release_sha256": sha256_file(manifest_path),
        "inventory": inventory,
        "inventory_summary": summary,
    }


def _source_hash(root: Path) -> str:
    return sha256_file(Path(__file__).resolve())


def _candidate_basis(root: Path, canary: Mapping[str, Any], active: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "ohlcv_msf_1d_publication_candidate_basis/1.0.0",
        "scope": "EXACT_NINE_MSF_OHLCV_1D_PARTITIONS_ONLY",
        "implementation_sha256": _source_hash(root),
        "execution_manifest_sha256": MANIFEST_SHA256,
        "job_ledger_sha256": LEDGER_SHA256,
        "active_pointer_sha256": active["pointer_sha256"],
        "prior_release_id": EXPECTED_PRIOR_RELEASE_ID,
        "partitions": [
            {
                key: item[key]
                for key in (
                    "year",
                    "future_dbn_path",
                    "future_sidecar_path",
                    "dbn_sha256",
                    "dbn_size_bytes",
                    "sidecar_sha256",
                    "sidecar_size_bytes",
                    "record_count",
                )
            }
            for item in canary["partitions"]
        ],
    }


def _copy_tree_create_only(source: Path, destination: Path) -> None:
    if destination.exists():
        raise SuccessorError(f"shadow destination already exists: {destination}")
    destination.mkdir(parents=True, exist_ok=False)
    for directory, directories, filenames in os.walk(source):
        directories.sort()
        relative = Path(directory).relative_to(source)
        target_directory = destination / relative
        for name in directories:
            (target_directory / name).mkdir(exist_ok=False)
        for name in sorted(filenames):
            source_file = Path(directory) / name
            target_file = target_directory / name
            with source_file.open("rb") as reader, target_file.open("xb") as writer:
                shutil.copyfileobj(reader, writer, length=1024 * 1024)
                writer.flush()
                os.fsync(writer.fileno())


def _addition_units(canary: Mapping[str, Any], run_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    units: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    shadow_prefix = f"state/data_publication_staging/{run_id}/candidate_additions/data/dbn"
    for item in canary["partitions"]:
        unit_id = f"GLBX.MDP3|ohlcv-1d|MSF|{item['year']}"
        units.append(
            {
                "schema_version": "dbn_canonical_publication_unit_manifest/3.0.0",
                "unit_id": unit_id,
                "family": "ohlcv-1d",
                "market": "MSF",
                "year": item["year"],
                "coverage_interval": {
                    "start_inclusive_utc": item["start_inclusive"],
                    "end_exclusive_utc": item["end_exclusive"],
                },
                "canonical_dbn": {
                    "project_relative_path": item["future_dbn_path"],
                    "sha256": item["dbn_sha256"],
                    "size_bytes": item["dbn_size_bytes"],
                    "copy_semantics": "SAME_VOLUME_HARDLINK_EXACT_IMMUTABLE_BYTES",
                },
                "input_provenance": {
                    "job_id": JOB_ID,
                    "execution_manifest_sha256": MANIFEST_SHA256,
                    "job_ledger_sha256": LEDGER_SHA256,
                    "source_dbn_path": item["dbn_source_path"],
                    "source_sidecar_path": item["sidecar_source_path"],
                    "source_sidecar_sha256": item["sidecar_sha256"],
                    "record_count": item["record_count"],
                },
                "activation_status": "CERTIFIED_NON_ACTIVE_NOT_PUBLISHED",
            }
        )
        for kind, future_key, hash_key, size_key in (
            ("DBN", "future_dbn_path", "dbn_sha256", "dbn_size_bytes"),
            ("MANIFEST", "future_sidecar_path", "sidecar_sha256", "sidecar_size_bytes"),
        ):
            future = item[future_key]
            inside = Path(future).relative_to(CANONICAL_ROOT_REL).as_posix()
            artifacts.append(
                {
                    "unit_id": unit_id,
                    "kind": kind,
                    "future_project_relative_path": future,
                    "candidate_shadow_project_relative_path": f"{shadow_prefix}/{inside}",
                    "sha256": item[hash_key],
                    "size_bytes": item[size_key],
                }
            )
    return units, artifacts


def _candidate_paths(root: Path, run_id: str) -> dict[str, Path]:
    report = contained(root, REPORT_PARENT_REL / run_id)
    payload = contained(root, SHADOW_PARENT_REL / run_id / "candidate_additions/data/dbn")
    return {"report": report, "payload": payload}


def _artifact_ref(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _verify_existing_candidate(root: Path, run_id: str) -> dict[str, Any]:
    paths = _candidate_paths(root, run_id)
    receipt_path = paths["report"] / "independent_certification_receipt.json"
    if not receipt_path.is_file():
        raise SuccessorError("deterministic candidate path exists without a complete certification receipt")
    receipt = load_json(receipt_path, "independent certification receipt")
    if receipt.get("status") != "PASS_CERTIFIED_NON_ACTIVE_REQUIRES_PUBLICATION_APPROVAL":
        raise SuccessorError("existing candidate is not certified non-active")
    for reference in receipt.get("sealed_artifacts", []):
        path = contained(root, Path(str(reference["path"])))
        if sha256_file(path) != reference["sha256"] or path.stat().st_size != reference["size_bytes"]:
            raise SuccessorError(f"existing sealed candidate artifact drifted: {path}")
    validation = load_json(paths["report"] / "candidate_tree_validation.json", "candidate validation")
    observed = _inventory(paths["payload"], include_links=True)
    summary = _inventory_summary(observed)
    if summary != validation.get("candidate_summary"):
        raise SuccessorError("existing certified candidate payload drifted")
    if any(int(item.get("link_count", 0)) != 1 for item in observed):
        raise SuccessorError("existing certified candidate payload is not plain-file isolated")
    return {
        "status": "NO_ACTION_CERTIFIED_CANDIDATE_ALREADY_EXISTS",
        "run_id": run_id,
        "release_id": receipt["successor_release_id"],
        "report_root": paths["report"].relative_to(root).as_posix(),
        "candidate_payload_root": paths["payload"].relative_to(root).as_posix(),
        "approval_packet": receipt["approval_packet"],
    }


def prepare_candidate(root: Path) -> dict[str, Any]:
    root = root.resolve(strict=True)
    canary = validate_canary(root)
    active = validate_active(root)
    basis = _candidate_basis(root, canary, active)
    candidate_id = sha256_json(basis)
    run_id = f"msf1dpub_{candidate_id[:24]}"
    paths = _candidate_paths(root, run_id)
    if paths["report"].exists() or paths["payload"].exists():
        return _verify_existing_candidate(root, run_id)
    if contained(root, LOCK_REL).exists():
        raise SuccessorError("publication lease already exists")
    free = shutil.disk_usage(root).free
    if free < 1_000_000_000:
        raise SuccessorError("less than the one-gigabyte preparation safety floor is free")

    report = paths["report"]
    payload = paths["payload"]
    report.mkdir(parents=True, exist_ok=False)
    payload.parent.mkdir(parents=True, exist_ok=True)
    try:
        _copy_tree_create_only(contained(root, CANARY_REL), payload / "ohlcv_1d/MSF")
        candidate_inventory = _inventory(payload, include_links=True)
        candidate_summary = _inventory_summary(candidate_inventory)
        if candidate_summary["file_count"] != 18 or candidate_summary["total_bytes"] != EXPECTED_CANARY_BYTES:
            raise SuccessorError("candidate payload count or bytes differ")
        if any(int(item["link_count"]) != 1 for item in candidate_inventory):
            raise SuccessorError("candidate payload unexpectedly contains hardlinks")
        virtual_inventory = [
            {key: item[key] for key in ("relative_path", "size_bytes", "sha256")}
            for item in active["inventory"]
        ]
        virtual_inventory.extend(
            {
                "relative_path": item["relative_path"],
                "size_bytes": item["size_bytes"],
                "sha256": item["sha256"],
            }
            for item in candidate_inventory
        )
        virtual_inventory.sort(key=lambda item: item["relative_path"])
        successor_summary = _inventory_summary(virtual_inventory)
        if successor_summary["file_count"] != EXPECTED_SUCCESSOR_FILES:
            raise SuccessorError("virtual successor file count differs")
        if successor_summary["total_bytes"] != EXPECTED_SUCCESSOR_BYTES:
            raise SuccessorError("virtual successor byte total differs")

        prior_release = active["release"]
        additions, addition_artifacts = _addition_units(canary, run_id)
        prior_units = prior_release["release_core"]["normalized_unit_manifests"]
        prior_artifacts = prior_release["canonical_artifact_index"]
        transformed_prior_artifacts = []
        for item in prior_artifacts:
            copied = dict(item)
            copied["candidate_shadow_project_relative_path"] = copied["future_project_relative_path"]
            copied["candidate_source_class"] = "CURRENT_ACTIVE_PRESERVED_IN_PLACE"
            transformed_prior_artifacts.append(copied)
        artifacts = sorted(
            transformed_prior_artifacts + addition_artifacts,
            key=lambda item: (item["future_project_relative_path"], item["kind"]),
        )
        if len(artifacts) != EXPECTED_SUCCESSOR_ARTIFACTS:
            raise SuccessorError("successor canonical artifact count differs")

        source_receipt = {
            "schema_version": "ohlcv_msf_1d_source_immutability_receipt/1.0.0",
            "status": "PASS_EXACT_SOURCE_BYTES_BOUND_AND_UNCHANGED",
            "run_id": run_id,
            "candidate_id": candidate_id,
            "created_utc": utc_now(),
            "candidate_basis": basis,
            "canary": canary,
            "active_pointer_sha256": active["pointer_sha256"],
            "prior_release_id": EXPECTED_PRIOR_RELEASE_ID,
            "provider_calls": 0,
            "credential_reads": 0,
        }
        create_json(report / "source_immutability_receipt.json", source_receipt)

        candidate_validation = {
            "schema_version": "ohlcv_msf_1d_candidate_tree_validation/1.0.0",
            "status": "PASS_CERTIFIED_ISOLATED_ADDITION_AND_COMPLETE_VIRTUAL_SUCCESSOR",
            "run_id": run_id,
            "created_utc": utc_now(),
            "candidate_payload_root": payload.relative_to(root).as_posix(),
            "candidate_summary": candidate_summary,
            "virtual_successor_summary": successor_summary,
            "expected_delta": {"files": 18, "bytes": EXPECTED_CANARY_BYTES},
            "hardlink_count": 0,
            "candidate_inventory": candidate_inventory,
            "virtual_successor_inventory": virtual_inventory,
        }
        create_json(report / "candidate_tree_validation.json", candidate_validation)

        release_core = {
            "schema_version": "ohlcv_msf_1d_publication_successor_core/1.0.0",
            "scope": "EXACT_NINE_MSF_OHLCV_1D_PARTITIONS_ONLY",
            "physical_publication_successor_to": EXPECTED_PRIOR_RELEASE_ID,
            "canonical_artifact_root": CANONICAL_ROOT_REL.as_posix(),
            "candidate_additions_root": payload.relative_to(root).as_posix(),
            "source_bindings": {
                "candidate_id": candidate_id,
                "implementation_sha256": _source_hash(root),
                "execution_manifest_sha256": MANIFEST_SHA256,
                "job_ledger_sha256": LEDGER_SHA256,
                "active_pointer_sha256": active["pointer_sha256"],
                "prior_release_manifest_sha256": active["release_sha256"],
            },
            "unit_counts": {"prior": EXPECTED_PRIOR_UNITS, "added": 9, "successor": EXPECTED_SUCCESSOR_UNITS},
            "artifact_counts": {"dbn": EXPECTED_SUCCESSOR_UNITS, "manifest": EXPECTED_SUCCESSOR_UNITS, "total": EXPECTED_SUCCESSOR_ARTIFACTS},
            "normalized_unit_manifests": prior_units + additions,
            "complete_virtual_successor": successor_summary,
            "cadence_root_counts_after_publication": {"ohlcv_1m": 58, "ohlcv_1s": 58, "ohlcv_1h": 33, "ohlcv_1d": 34},
            "prohibited_scope": {"ohlcv_1h": True, "other_markets": True, "full_58_continuation": True},
        }
        release_id = sha256_json(release_core)
        release_manifest = {
            "schema_version": "ohlcv_msf_1d_publication_successor_release/1.0.0",
            "release_id": release_id,
            "release_status": "CERTIFIED_NON_ACTIVE_NOT_PUBLISHED",
            "publication_authorized": False,
            "activation_authorized": False,
            "created_utc": utc_now(),
            "release_core": release_core,
            "canonical_artifact_index": artifacts,
            "canonical_artifact_index_sha256": sha256_json(artifacts),
            "complete_shadow_tree": successor_summary,
        }
        release_path = report / "successor_release_manifest.json"
        create_json(release_path, release_manifest)

        rollback = {
            "schema_version": "ohlcv_msf_1d_publication_rollback_plan/1.0.0",
            "status": "PREPARED_NON_ACTIVE",
            "run_id": run_id,
            "prior_release_id": EXPECTED_PRIOR_RELEASE_ID,
            "successor_release_id": release_id,
            "prior_pointer_sha256": active["pointer_sha256"],
            "prior_root": active["inventory_summary"],
            "transaction": [
                "ACQUIRE_EXCLUSIVE_LEASE_AND_REVALIDATE",
                "PRESERVE_EXACT_PRIOR_POINTER_BYTES",
                "PRESERVE_ALL_9908_PRIOR_FILES_IN_PLACE_UNCHANGED",
                "ATOMIC_INSTALL_PREVIOUSLY_ABSENT_OHLCV_1D_MSF_DIRECTORY",
                "VERIFY_COMPLETE_SUCCESSOR_ROOT",
                "COMPARE_AND_SWAP_ACTIVE_POINTER",
                "CANONICAL_RESOLVER_AND_NINE_PARTITION_READBACK",
            ],
            "failure_policy": "RESTORE_EXACT_PRIOR_POINTER_AND_ATOMICALLY_REMOVE_ADDED_MSF_DIRECTORY_ON_ANY_FAILURE",
            "permanent_cleanup": False,
        }
        create_json(report / "rollback_plan.json", rollback)

        implementation_ref = {
            "path": Path(__file__).resolve().relative_to(root).as_posix(),
            "sha256": _source_hash(root),
        }
        contract_core = {
            "schema_version": "ohlcv_msf_1d_publication_contract/1.0.0",
            "status": "CERTIFIED_NON_ACTIVE_SEPARATE_APPROVAL_REQUIRED",
            "scope": "EXACT_NINE_MSF_OHLCV_1D_PARTITIONS_ONLY",
            "successor_release": _artifact_ref(release_path, root),
            "prior_active_pointer_sha256": active["pointer_sha256"],
            "prior_release_id": EXPECTED_PRIOR_RELEASE_ID,
            "implementation": implementation_ref,
            "exclusive_lease": LOCK_REL.as_posix(),
            "same_volume_atomic_absent_directory_install": True,
            "compare_and_swap_pointer": True,
            "added_directory_and_pointer_rollback": True,
            "preserve_all_prior_files_in_place": True,
            "provider_access": False,
            "credential_access": False,
            "publication_requires_separate_owner_approval": True,
        }
        contract = {**contract_core, "contract_id": sha256_json(contract_core)}
        contract_path = report / "publication_contract.json"
        create_json(contract_path, contract)

        registry_core = {
            "schema_version": "ohlcv_msf_1d_publication_registry/1.0.0",
            "status": "REGISTERED_NON_ACTIVE",
            "scope": contract["scope"],
            "contract": _artifact_ref(contract_path, root),
            "implementation": implementation_ref,
            "prepare_function": "prepare_candidate",
            "activate_function": "activate_authorized",
            "resolve_function": "resolve_active_successor",
            "one_time_authorization": True,
        }
        registry = {**registry_core, "registry_id": sha256_json(registry_core)}
        registry_path = report / "publication_registry.json"
        create_json(registry_path, registry)

        wrapper_core = {
            "schema_version": "canonical_data_dbn_active_wrapper/2.0.0",
            "status": "IMMUTABLE_SUCCESSOR_WRAPPER",
            "release_id": release_id,
            "physical_publication_successor_to": EXPECTED_PRIOR_RELEASE_ID,
            "release_manifest": _artifact_ref(release_path, root),
            "publication_contract": _artifact_ref(contract_path, root),
            "publication_registry": _artifact_ref(registry_path, root),
            "canonical_artifact_count": EXPECTED_SUCCESSOR_ARTIFACTS,
            "complete_root": successor_summary,
        }
        wrapper = {**wrapper_core, "wrapper_id": sha256_json(wrapper_core)}
        wrapper_path = report / "successor_wrapper.json"
        create_json(wrapper_path, wrapper)

        pointer_template_core = {
            "schema_version": "canonical_data_dbn_active_pointer/2.0.0",
            "status": "ACTIVE",
            "scope": "EXACT_NINE_MSF_OHLCV_1D_PARTITIONS_ONLY",
            "release_id": release_id,
            "physical_publication_successor_to": EXPECTED_PRIOR_RELEASE_ID,
            "release_manifest_path": release_path.relative_to(root).as_posix(),
            "release_manifest_sha256": sha256_file(release_path),
            "wrapper_path": f"{REGISTRY_PARENT_REL.as_posix()}/{release_id}/active_wrapper_v2.json",
            "wrapper_sha256": sha256_file(wrapper_path),
            "publication_contract_path": contract_path.relative_to(root).as_posix(),
            "publication_contract_sha256": sha256_file(contract_path),
            "publication_registry_path": registry_path.relative_to(root).as_posix(),
            "publication_registry_sha256": sha256_file(registry_path),
            "canonical_artifact_root": CANONICAL_ROOT_REL.as_posix(),
            "rollback_archive_root": f"{ARCHIVE_PARENT_REL.as_posix()}/{EXPECTED_PRIOR_RELEASE_ID}/{run_id}",
            "activated_at_utc_rule": "SET_ONCE_AT_AUTHORIZED_EXECUTION",
        }
        pointer_template = {**pointer_template_core, "pointer_template_id": sha256_json(pointer_template_core)}
        pointer_template_path = report / "active_pointer_template.json"
        create_json(pointer_template_path, pointer_template)

        packet_core = {
            "schema_version": "ohlcv_msf_1d_publication_approval_packet/1.0.0",
            "status": "CERTIFIED_NON_ACTIVE_REQUIRES_SEPARATE_PUBLICATION_APPROVAL",
            "scope": contract["scope"],
            "successor_release_id": release_id,
            "successor_release_manifest": _artifact_ref(release_path, root),
            "current_active_pointer": {"path": ACTIVE_POINTER_REL.as_posix(), "sha256": active["pointer_sha256"]},
            "publication_contract": _artifact_ref(contract_path, root),
            "publication_registry": _artifact_ref(registry_path, root),
            "successor_wrapper": _artifact_ref(wrapper_path, root),
            "active_pointer_template": _artifact_ref(pointer_template_path, root),
            "rollback_plan": _artifact_ref(report / "rollback_plan.json", root),
            "candidate_validation": _artifact_ref(report / "candidate_tree_validation.json", root),
            "outputs": {
                "added_market": "MSF",
                "added_schema": "ohlcv-1d",
                "added_partitions": 9,
                "added_files": 18,
                "added_logical_bytes": EXPECTED_CANARY_BYTES,
                "provider_cost_usd": "0",
            },
            "prohibitions": [
                "NO_PROVIDER_OR_CREDENTIAL_ACCESS",
                "NO_OHLCV_1H",
                "NO_OTHER_MARKETS",
                "NO_FULL_58_CONTINUATION",
                "NO_OVERWRITE_OR_CLEANUP",
            ],
            "approval_effect": "AUTHORIZE_ONE_ATOMIC_ABSENT_DIRECTORY_INSTALL_AND_POINTER_PUBLICATION_ONLY",
        }
        packet = {**packet_core, "packet_id": sha256_json(packet_core)}
        packet_path = report / "activation_authorization_packet.json"
        create_json(packet_path, packet)

        sealed_names = [
            "source_immutability_receipt.json",
            "candidate_tree_validation.json",
            "successor_release_manifest.json",
            "rollback_plan.json",
            "publication_contract.json",
            "publication_registry.json",
            "successor_wrapper.json",
            "active_pointer_template.json",
            "activation_authorization_packet.json",
        ]
        certification_core = {
            "schema_version": "ohlcv_msf_1d_independent_certification/1.0.0",
            "status": "PASS_CERTIFIED_NON_ACTIVE_REQUIRES_PUBLICATION_APPROVAL",
            "run_id": run_id,
            "certified_utc": utc_now(),
            "successor_release_id": release_id,
            "approval_packet": _artifact_ref(packet_path, root),
            "sealed_artifacts": [_artifact_ref(report / name, root) for name in sealed_names],
            "acceptance": {
                "nine_complete_valid_partitions": True,
                "record_count": EXPECTED_RECORDS,
                "provider_free_repeat_resume_noop": True,
                "complete_virtual_successor_hash_verified": True,
                "candidate_files_plain_and_isolated": True,
                "active_pointer_unchanged": sha256_file(contained(root, ACTIVE_POINTER_REL)) == active["pointer_sha256"],
                "canonical_root_unchanged": _inventory_summary(_inventory(contained(root, CANONICAL_ROOT_REL))) == active["inventory_summary"],
                "publication_performed": False,
                "ohlcv_1h_action_count": 0,
                "other_market_action_count": 0,
                "provider_calls": 0,
                "credential_reads": 0,
            },
        }
        certification = {**certification_core, "certificate_id": sha256_json(certification_core)}
        certification_path = report / "independent_certification_receipt.json"
        create_json(certification_path, certification)

        approval_text = (
            f"Approve canonical publication of certified successor release {release_id}, bound to approval "
            f"packet SHA-256 {sha256_file(packet_path)} and current active pointer SHA-256 "
            f"{active['pointer_sha256']}. Add only the nine retained MSF ohlcv-1d partitions from "
            "2018-01-01 through 2026-07-14 exclusive at zero provider cost, with no provider or credential "
            "access, no ohlcv-1h, no other markets, no full-58 continuation, no overwrites, and no cleanup. "
            "Use the certified same-volume atomic absent-directory installation and pointer compare-and-swap, "
            "preserve all prior canonical files in place and the exact prior pointer, and roll back the added "
            "directory and pointer on any validation failure.\n"
        )
        create_bytes(report / "PUBLICATION_APPROVAL_REQUEST.txt", approval_text.encode("utf-8"))
        return {
            "status": "PASS_CERTIFIED_NON_ACTIVE_REQUIRES_PUBLICATION_APPROVAL",
            "run_id": run_id,
            "release_id": release_id,
            "report_root": report.relative_to(root).as_posix(),
            "candidate_payload_root": payload.relative_to(root).as_posix(),
            "approval_packet": _artifact_ref(packet_path, root),
            "approval_request": (report / "PUBLICATION_APPROVAL_REQUEST.txt").relative_to(root).as_posix(),
        }
    except Exception:
        # Preserve any partial candidate as fail-closed evidence.  A later run
        # will refuse to overwrite it and must diagnose the exact residue.
        raise


def _required_authorization_scope(packet: Mapping[str, Any], packet_sha256: str) -> dict[str, str]:
    return {
        "approval_packet_sha256": packet_sha256,
        "successor_release_id": str(packet["successor_release_id"]),
        "prior_pointer_sha256": str(packet["current_active_pointer"]["sha256"]),
        "scope": "EXACT_NINE_MSF_OHLCV_1D_PARTITIONS_ONLY",
        "provider_cost_usd": "0",
        "provider_access": "false",
        "ohlcv_1h_actions": "0",
        "other_market_actions": "0",
        "full_58_continuation": "false",
        "approval_command": PUBLICATION_APPROVAL_COMMAND,
        "approval_plan_id": str(packet["packet_id"]),
        "approval_plan_sha256": packet_sha256,
    }


def _verify_authorization(
    root: Path,
    run_id: str,
    authorization: OperationReceipt,
) -> tuple[dict[str, Any], RepoBoundary, dict[str, str]]:
    report = _candidate_paths(root, run_id)["report"]
    packet_path = report / "activation_authorization_packet.json"
    packet = load_json(packet_path, "activation authorization packet")
    if not isinstance(authorization, OperationReceipt):
        raise SuccessorError("publication requires an OperationReceipt")
    boundary = RepoBoundary(root)
    required_scope = _required_authorization_scope(packet, sha256_file(packet_path))
    authorization.verify(
        boundary,
        operation=PUBLICATION_OPERATION,
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        required_scope=required_scope,
    )
    return packet, boundary, required_scope


@dataclass
class _TransactionState:
    addition_installed: bool = False
    pointer_replaced: bool = False


def _maybe_fail(failpoint: str | None, value: str) -> None:
    if failpoint == value:
        raise SuccessorError(f"injected transaction failure: {value}")


def resolve_active_successor(root: Path, run_id: str) -> dict[str, Any]:
    root = root.resolve(strict=True)
    report = _candidate_paths(root, run_id)["report"]
    release = load_json(report / "successor_release_manifest.json", "successor release")
    pointer = load_json(contained(root, ACTIVE_POINTER_REL), "active pointer")
    if pointer.get("status") != "ACTIVE" or pointer.get("release_id") != release["release_id"]:
        raise SuccessorError("active pointer does not resolve the successor")
    if pointer.get("release_manifest_sha256") != sha256_file(report / "successor_release_manifest.json"):
        raise SuccessorError("active pointer release binding differs")
    observed = _inventory(contained(root, CANONICAL_ROOT_REL))
    if _inventory_summary(observed) != release["complete_shadow_tree"]:
        raise SuccessorError("active successor root differs")
    for artifact in release["canonical_artifact_index"]:
        path = contained(root, Path(artifact["future_project_relative_path"]))
        if path.stat().st_size != artifact["size_bytes"] or sha256_file(path) != artifact["sha256"]:
            raise SuccessorError(f"canonical artifact readback differs: {path}")
    return {
        "status": "PASS_ACTIVE_SUCCESSOR_CANONICAL_READBACK",
        "release_id": release["release_id"],
        "canonical_artifacts_verified": len(release["canonical_artifact_index"]),
        "complete_root": release["complete_shadow_tree"],
    }


def activate_authorized(
    root: Path,
    run_id: str,
    authorization: OperationReceipt,
    *,
    failpoint: str | None = None,
    writer_scan: Callable[[], Sequence[str]] = lambda: (),
) -> dict[str, Any]:
    """Publish the certified successor after a separately issued receipt.

    This function is intentionally not exposed by the CLI.  It is callable
    only by an approved task that constructs the bound one-time receipt.
    """

    root = root.resolve(strict=True)
    packet, boundary, required_scope = _verify_authorization(root, run_id, authorization)
    paths = _candidate_paths(root, run_id)
    report = paths["report"]
    payload = paths["payload"]
    release = load_json(report / "successor_release_manifest.json", "successor release")
    release_id = release["release_id"]
    pointer_path = contained(root, ACTIVE_POINTER_REL)
    if load_json(pointer_path, "active pointer").get("release_id") == release_id:
        authorization.assert_consumed(
            boundary,
            operation=PUBLICATION_OPERATION,
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope=required_scope,
        )
        readback = resolve_active_successor(root, run_id)
        return {"status": "NO_ACTION_ALREADY_ACTIVE_SAME_RELEASE", "readback": readback}
    authorization_use_path = authorization.consume(
        boundary,
        operation=PUBLICATION_OPERATION,
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        required_scope=required_scope,
    )
    if sha256_file(pointer_path) != packet["current_active_pointer"]["sha256"]:
        raise SuccessorError("active pointer drifted before authorized publication")
    if writer_scan():
        raise SuccessorError("canonical writer detected before publication")
    lease = contained(root, LOCK_REL)
    lease.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(lease, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise SuccessorError("publication lease already exists") from exc
    os.write(descriptor, serialized_json({"run_id": run_id, "release_id": release_id, "created_utc": utc_now()}))
    os.fsync(descriptor)
    state = _TransactionState()
    canonical = contained(root, CANONICAL_ROOT_REL)
    archive_root = contained(root, ARCHIVE_PARENT_REL / EXPECTED_PRIOR_RELEASE_ID / run_id)
    prior_pointer_path = archive_root / "prior_active_pointer.json"
    wrapper_source = report / "successor_wrapper.json"
    wrapper_path = contained(root, REGISTRY_PARENT_REL / release_id / "active_wrapper_v2.json")
    pointer_template = load_json(report / "active_pointer_template.json", "active pointer template")
    prior_pointer_bytes = pointer_path.read_bytes()
    try:
        if writer_scan():
            raise SuccessorError("canonical writer detected after lease acquisition")
        if sha256_file(pointer_path) != packet["current_active_pointer"]["sha256"]:
            raise SuccessorError("active pointer drifted after lease acquisition")
        current_summary = _inventory_summary(_inventory(canonical))
        if current_summary != {
            "file_count": EXPECTED_PRIOR_FILES,
            "total_bytes": EXPECTED_PRIOR_BYTES,
            "inventory_sha256": validate_active(root)["inventory_summary"]["inventory_sha256"],
        }:
            raise SuccessorError("prior canonical root drifted after lease acquisition")
        candidate_msf = payload / "ohlcv_1d/MSF"
        candidate_summary = _inventory_summary(_inventory(payload))
        candidate_validation = load_json(report / "candidate_tree_validation.json", "candidate validation")
        if candidate_summary != candidate_validation["candidate_summary"]:
            raise SuccessorError("certified candidate payload drifted before transition")
        destination_msf = canonical / "ohlcv_1d/MSF"
        if destination_msf.exists():
            raise SuccessorError("MSF daily canonical destination appeared before publication")
        if candidate_msf.stat().st_dev != (canonical / "ohlcv_1d").stat().st_dev:
            raise SuccessorError("candidate payload and canonical destination are not on one volume")
        _maybe_fail(failpoint, "before_install")
        archive_root.mkdir(parents=True, exist_ok=False)
        create_bytes(prior_pointer_path, prior_pointer_bytes)
        wrapper_path.parent.mkdir(parents=True, exist_ok=False)
        create_bytes(wrapper_path, wrapper_source.read_bytes())
        os.replace(candidate_msf, destination_msf)
        state.addition_installed = True
        _maybe_fail(failpoint, "after_install")
        if _inventory_summary(_inventory(canonical)) != release["complete_shadow_tree"]:
            raise SuccessorError("successor root differs after atomic addition install")
        _maybe_fail(failpoint, "before_pointer_replace")
        activated_core = {
            key: value
            for key, value in pointer_template.items()
            if key not in {"pointer_template_id", "activated_at_utc_rule"}
        }
        activated_core["activated_at_utc"] = utc_now()
        activated_core["wrapper_path"] = wrapper_path.relative_to(root).as_posix()
        activated = {**activated_core, "pointer_id": sha256_json(activated_core)}
        atomic_replace_bytes(pointer_path, serialized_json(activated), run_id)
        state.pointer_replaced = True
        _maybe_fail(failpoint, "after_pointer_replace")
        readback = resolve_active_successor(root, run_id)
        _maybe_fail(failpoint, "after_readback")
        receipt = {
            "schema_version": "ohlcv_msf_1d_publication_receipt/1.0.0",
            "status": "ACTIVATED_AND_VERIFIED",
            "run_id": run_id,
            "activated_utc": activated["activated_at_utc"],
            "successor_release_id": release_id,
            "prior_release_id": EXPECTED_PRIOR_RELEASE_ID,
            "authorization_receipt_id": authorization.receipt_id,
            "authorization_use_path": authorization_use_path.relative_to(root).as_posix(),
            "active_pointer_sha256": sha256_file(pointer_path),
            "rollback_archive_root": archive_root.relative_to(root).as_posix(),
            "prior_files_preserved_in_place": EXPECTED_PRIOR_FILES,
            "readback": readback,
            "provider_calls": 0,
            "credential_reads": 0,
            "permanent_cleanup": False,
        }
        create_json(report / "publication_receipt.json", receipt)
        return receipt
    except Exception:
        if state.pointer_replaced:
            atomic_replace_bytes(pointer_path, prior_pointer_bytes, run_id + ".rollback")
        if state.addition_installed:
            restored_msf = payload / "ohlcv_1d/MSF"
            restored_msf.parent.mkdir(parents=True, exist_ok=True)
            live_msf = canonical / "ohlcv_1d/MSF"
            if live_msf.exists():
                os.replace(live_msf, restored_msf)
        if sha256_file(pointer_path) != packet["current_active_pointer"]["sha256"]:
            raise SuccessorError("rollback failed to restore the exact prior pointer")
        raise
    finally:
        os.close(descriptor)
        try:
            lease.unlink()
        except FileNotFoundError:
            pass


def cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare the non-active nine-partition MSF daily publication successor")
    parser.add_argument("--repository-root", default=".")
    args = parser.parse_args(argv)
    result = prepare_candidate(Path(args.repository_root))
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(cli())
