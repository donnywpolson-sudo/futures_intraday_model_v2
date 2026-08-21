"""Provider-free preparation and receipt-bound additive OHLCV publication."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .errors import ContractError, IntegrityError
from .ohlcv_completion_campaign import load_batch
from .ohlcv_historical_backfill import (
    _validated_no_data_evidence,
    classify_target,
    load_manifest,
)
from .ohlcv_historical_backfill_v3 import SCHEMA_DIRECTORIES
from .ohlcv_msf_1d_publication_successor import (
    _inventory,
    _inventory_summary,
    atomic_replace_bytes,
    contained,
    create_bytes,
    create_json,
    load_json,
    serialized_json,
    sha256_file,
    sha256_json,
    utc_now,
)


ACTIVE_POINTER_REL = Path("configs/active_dbn_congruence_release_v1.json")
CANONICAL_ROOT_REL = Path("data/dbn")
REPORT_PARENT_REL = Path("reports/ohlcv_58_completion_publication")
STAGING_PARENT_REL = Path("state/data_publication_staging")
REGISTRY_PARENT_REL = Path("state/dbn_congruence_release_registry")
ARCHIVE_PARENT_REL = Path("state/dbn_congruence_release_archive")
LOCK_REL = Path("state/locks/ohlcv_58_completion_publication.lock")
PUBLICATION_OPERATION = "PUBLISH_OHLCV_58_COMPLETION_BATCH"
NO_DATA_STATE = "NO_DATA_CONFIRMED"
COMPLETE_STATE = "COMPLETE_VALID"
INDEPENDENT_CERTIFICATE_NAME = "independent_certificate_v2.json"
APPROVAL_PACKET_NAME = "activation_authorization_packet_v2.json"


@dataclass
class _TransactionState:
    installed: list[tuple[Path, Path]]
    pointer_replaced: bool = False


def _load_pointer_and_release(root: Path) -> tuple[dict[str, Any], Path, dict[str, Any]]:
    pointer_path = contained(root, ACTIVE_POINTER_REL)
    pointer = load_json(pointer_path, "active DBN pointer")
    if pointer.get("status") != "ACTIVE":
        raise IntegrityError("active DBN pointer is not ACTIVE")
    relative = pointer.get("release_manifest_path")
    expected = pointer.get("release_manifest_sha256")
    if not isinstance(relative, str) or not isinstance(expected, str):
        raise IntegrityError("active DBN pointer lacks a release binding")
    release_path = contained(root, Path(relative))
    if sha256_file(release_path) != expected:
        raise IntegrityError("active release differs from pointer binding")
    release = load_json(release_path, "active DBN release")
    if release.get("release_id") != pointer.get("release_id"):
        raise IntegrityError("active pointer and release IDs differ")
    return pointer, release_path, release


def _copy_tree_plain(source: Path, destination: Path) -> None:
    if destination.exists():
        raise IntegrityError(f"publication candidate destination exists: {destination}")
    destination.mkdir(parents=True)
    for directory, directories, filenames in os.walk(source):
        directories.sort()
        relative = Path(directory).relative_to(source)
        target_directory = destination / relative
        target_directory.mkdir(parents=True, exist_ok=True)
        for filename in sorted(filenames):
            incoming = Path(directory) / filename
            outgoing = target_directory / filename
            with incoming.open("rb") as source_stream, outgoing.open("xb") as target_stream:
                shutil.copyfileobj(source_stream, target_stream, length=1_048_576)
                target_stream.flush()
                os.fsync(target_stream.fileno())
            if outgoing.stat().st_nlink != 1 or sha256_file(outgoing) != sha256_file(incoming):
                raise IntegrityError("publication candidate is not an exact plain-file copy")


def _selected_rows(batch: Mapping[str, Any], manifest_path: Path) -> list[dict[str, Any]]:
    selected = {
        (str(market), str(schema))
        for market, schemas in batch["selection"].items()
        for schema in schemas
    }
    rows = [
        row
        for row in load_manifest(manifest_path)
        if (str(row["market"]), str(row["schema"])) in selected
    ]
    if not rows or {(str(row["market"]), str(row["schema"])) for row in rows} != selected:
        raise IntegrityError("publication manifest does not cover the exact batch")
    return rows


def _bound_artifact(
    root: Path,
    path: Path,
    expected_sha256: str,
    label: str,
) -> tuple[Path, dict[str, Any]]:
    resolved = path.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ContractError(f"{label} is outside the repository") from exc
    if sha256_file(resolved) != expected_sha256:
        raise IntegrityError(f"{label} hash differs")
    return resolved, load_json(resolved, label)


def _validate_acquisition_result(
    payload: Mapping[str, Any],
    *,
    manifest_sha256: str,
    selected_target_count: int,
) -> None:
    repeat = payload.get("repeat")
    if not isinstance(repeat, Mapping) or repeat.get("status") != "NO_ACTION_BATCH_ALREADY_COMPLETE":
        raise IntegrityError("completion acquisition lacks a provider-free terminal no-op")
    resume = repeat.get("resume")
    if not isinstance(resume, Mapping):
        raise IntegrityError("completion acquisition no-op payload is absent")
    if (
        resume.get("manifest_sha256") != manifest_sha256
        or resume.get("actions") != 0
        or resume.get("paid_submissions") != 0
        or resume.get("selected_targets") != selected_target_count
        or resume.get("result") != "NO_ACTION_ALL_TARGETS_COMPLETE_OR_NO_DATA"
        or resume.get("state_counts") != {COMPLETE_STATE: 478, NO_DATA_STATE: 2}
        or payload.get("publication") is not False
    ):
        raise IntegrityError("completion acquisition terminal no-op differs from contract")


def _validate_no_data_certificate(
    certificate: Mapping[str, Any],
    *,
    manifest_sha256: str,
    evidence_sha256s: set[str],
    no_data_target_ids: set[str],
) -> None:
    successor = certificate.get("successor")
    evidence = certificate.get("evidence")
    jobs = evidence.get("jobs") if isinstance(evidence, Mapping) else None
    if (
        certificate.get("status") != "PASS_READY_TO_RESUME_EXISTING_LEDGER"
        or not isinstance(successor, Mapping)
        or successor.get("manifest_sha256") != manifest_sha256
        or not isinstance(evidence, Mapping)
        or evidence.get("evidence_sha256") not in evidence_sha256s
        or not isinstance(jobs, list)
        or {str(item.get("target_id")) for item in jobs if isinstance(item, Mapping)}
        != no_data_target_ids
    ):
        raise IntegrityError("completion no-data certificate differs from publication inputs")


def _classify_publication_rows(
    root: Path,
    acquisition_root: Path,
    rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    complete: list[dict[str, Any]] = []
    no_data: list[dict[str, Any]] = []
    evidence_payloads: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if row.get("current_state") == NO_DATA_STATE:
            evidence = _validated_no_data_evidence(row)
            evidence_path = contained(root, Path(str(evidence["evidence_path"])))
            evidence_sha = str(evidence["evidence_sha256"])
            key = (evidence_path.as_posix(), evidence_sha)
            if key not in evidence_payloads:
                if sha256_file(evidence_path) != evidence_sha:
                    raise IntegrityError("completion no-data evidence hash differs")
                evidence_payloads[key] = load_json(evidence_path, "completion no-data evidence")
            payload = evidence_payloads[key]
            targets = payload.get("targets")
            if not isinstance(targets, list):
                raise IntegrityError("completion no-data evidence set lacks targets")
            matches = [
                item
                for item in targets
                if isinstance(item, Mapping)
                and item.get("target", {}).get("target_id") == row.get("target_id")
                and item.get("job", {}).get("job_id") == evidence.get("job_id")
                and item.get("provider_file_manifest", {}).get("provider_manifest_hash")
                == evidence.get("provider_manifest_hash")
            ]
            if len(matches) != 1:
                raise IntegrityError("completion no-data evidence does not bind the exact target")
            observed = classify_target(acquisition_root, row, confirmed_record_count=0)
            if observed.get("current_state") != NO_DATA_STATE:
                raise IntegrityError(f"publication no-data target is not absent: {row['target_id']}")
            no_data.append(dict(row))
            continue
        observed = classify_target(acquisition_root, row)
        if observed.get("current_state") != COMPLETE_STATE:
            raise IntegrityError(f"publication source target is not complete: {row['target_id']}")
        complete.append(dict(row))
    return complete, no_data


def _addition_unit(
    root: Path,
    state_root: Path,
    row: Mapping[str, Any],
    *,
    manifest_sha256: str,
    candidate_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source_dbn = state_root / str(row["final_path"])
    source_sidecar = state_root / str(row["sidecar_path"])
    schema = str(row["schema"])
    market = str(row["market"])
    year = int(row["year"])
    unit_id = f"GLBX.MDP3|{schema}|{market}|{year}"
    if row.get("current_state") == NO_DATA_STATE:
        evidence = _validated_no_data_evidence(row)
        if source_dbn.exists() or source_sidecar.exists():
            raise IntegrityError("certified no-data publication unit unexpectedly has files")
        return (
            {
                "activation_status": "CERTIFIED_NO_DATA_EVIDENCE_ONLY",
                "canonical_dbn": None,
                "coverage_interval": {
                    "end_exclusive_utc": row["intended_end_exclusive"],
                    "start_inclusive_utc": row["intended_start_inclusive"],
                },
                "family": schema,
                "input_provenance": {
                    "evidence_path": evidence["evidence_path"],
                    "evidence_sha256": evidence["evidence_sha256"],
                    "execution_manifest_sha256": manifest_sha256,
                    "job_id": evidence["job_id"],
                    "provider_manifest_hash": evidence["provider_manifest_hash"],
                    "request_fingerprint": evidence["request_fingerprint"],
                },
                "market": market,
                "provider_record_count": 0,
                "publication_action": "NO_FILE_CREATE",
                "schema_version": "dbn_canonical_publication_no_data_unit_manifest/1.0.0",
                "unit_id": unit_id,
                "year": year,
            },
            [],
        )
    sidecar = load_json(source_sidecar, "completion sidecar")
    unit = {
        "activation_status": "CERTIFIED_NON_ACTIVE_NOT_PUBLISHED",
        "canonical_dbn": {
            "copy_semantics": "EXACT_PLAIN_FILE_COPY_FROM_HASH_BOUND_ACQUISITION_STATE",
            "project_relative_path": str(row["final_path"]),
            "sha256": sha256_file(source_dbn),
            "size_bytes": source_dbn.stat().st_size,
        },
        "coverage_interval": {
            "end_exclusive_utc": row["intended_end_exclusive"],
            "start_inclusive_utc": row["intended_start_inclusive"],
        },
        "family": schema,
        "input_provenance": {
            "execution_manifest_sha256": manifest_sha256,
            "job_id": sidecar["job_id"],
            "record_count": sidecar["record_count"],
            "source_dbn_path": source_dbn.relative_to(root).as_posix(),
            "source_sidecar_path": source_sidecar.relative_to(root).as_posix(),
            "source_sidecar_sha256": sha256_file(source_sidecar),
        },
        "market": market,
        "schema_version": "dbn_canonical_publication_unit_manifest/3.0.0",
        "unit_id": unit_id,
        "year": year,
    }
    artifacts: list[dict[str, Any]] = []
    for kind, source, future in (
        ("DBN", source_dbn, Path(str(row["final_path"]))),
        ("MANIFEST", source_sidecar, Path(str(row["sidecar_path"]))),
    ):
        candidate = candidate_root / future.relative_to(CANONICAL_ROOT_REL)
        artifacts.append(
            {
                "candidate_shadow_project_relative_path": candidate.relative_to(root).as_posix(),
                "future_project_relative_path": future.as_posix(),
                "kind": kind,
                "sha256": sha256_file(source),
                "size_bytes": source.stat().st_size,
                "unit_id": unit_id,
            }
        )
    return unit, artifacts


def prepare_candidate(
    root: Path,
    *,
    batch_path: Path,
    batch_sha256: str,
    manifest_path: Path,
    manifest_sha256: str,
    state_root: Path,
    acquisition_result_path: Path,
    acquisition_result_sha256: str,
    no_data_certificate_path: Path,
    no_data_certificate_sha256: str,
) -> dict[str, Any]:
    root = root.resolve(strict=True)
    batch = load_batch(root, batch_path, expected_sha256=batch_sha256)
    manifest_path = manifest_path.resolve(strict=True)
    if sha256_file(manifest_path) != manifest_sha256:
        raise IntegrityError("publication execution manifest hash differs")
    if state_root.is_absolute() or ".." in state_root.parts:
        raise ContractError("publication state root must be project-relative")
    acquisition_root = (root / state_root).resolve(strict=True)
    rows = _selected_rows(batch, manifest_path)
    complete_rows, no_data_rows = _classify_publication_rows(root, acquisition_root, rows)
    if len(rows) != 480 or len(complete_rows) != 478 or len(no_data_rows) != 2:
        raise IntegrityError("publication target state counts differ from the exact completion batch")
    acquisition_result_path, acquisition_result = _bound_artifact(
        root,
        acquisition_result_path,
        acquisition_result_sha256,
        "completion acquisition result",
    )
    _validate_acquisition_result(
        acquisition_result,
        manifest_sha256=manifest_sha256,
        selected_target_count=len(rows),
    )
    no_data_certificate_path, no_data_certificate = _bound_artifact(
        root,
        no_data_certificate_path,
        no_data_certificate_sha256,
        "completion no-data certificate",
    )
    evidence_sha256s = {
        str(_validated_no_data_evidence(row)["evidence_sha256"])
        for row in no_data_rows
    }
    _validate_no_data_certificate(
        no_data_certificate,
        manifest_sha256=manifest_sha256,
        evidence_sha256s=evidence_sha256s,
        no_data_target_ids={str(row["target_id"]) for row in no_data_rows},
    )
    pointer, prior_release_path, prior_release = _load_pointer_and_release(root)
    predecessors = {
        str(row["manifest_predecessor_sha256"])
        for row in rows
        if row.get("manifest_predecessor_sha256") is not None
    }
    if len(predecessors) > 1:
        raise IntegrityError("publication manifest has multiple acquisition predecessors")
    ledger_manifest_sha256 = next(iter(predecessors), manifest_sha256)
    ledger_path = (
        acquisition_root
        / "state/provider_acquisition_staging/ohlcv_1d_1h_historical_backfill"
        / ledger_manifest_sha256[:16]
        / "job_ledger.jsonl"
    )
    if not ledger_path.is_file():
        raise IntegrityError("publication acquisition ledger is absent")
    basis = {
        "active_pointer_sha256": sha256_file(root / ACTIVE_POINTER_REL),
        "batch_plan_id": batch["plan_id"],
        "batch_sha256": batch_sha256,
        "acquisition_result_sha256": acquisition_result_sha256,
        "manifest_sha256": manifest_sha256,
        "no_data_certificate_sha256": no_data_certificate_sha256,
        "state_root": state_root.as_posix(),
    }
    run_id = "ohlcvpub_" + sha256_json(basis)[:32]
    report = root / REPORT_PARENT_REL / run_id
    candidate_root = root / STAGING_PARENT_REL / run_id / "candidate_additions/data/dbn"
    if report.exists() or candidate_root.exists():
        build_receipt_path = report / "candidate_build_receipt.json"
        if not build_receipt_path.is_file():
            raise IntegrityError("partial publication candidate already exists")
        build_receipt = load_json(build_receipt_path, "existing candidate build receipt")
        release_path = report / "successor_release_manifest.json"
        if (
            build_receipt.get("status") != "PASS_CANDIDATE_PREPARED_REQUIRES_INDEPENDENT_CERTIFICATION"
            or build_receipt.get("run_id") != run_id
            or build_receipt.get("release_manifest_sha256") != sha256_file(release_path)
            or build_receipt.get("candidate_summary") != _inventory_summary(_inventory(candidate_root))
        ):
            raise IntegrityError("existing publication candidate differs from its build receipt")
        return {
            "release_id": load_json(release_path, "successor release")["release_id"],
            "run_id": run_id,
            "status": "NO_ACTION_CANDIDATE_ALREADY_PREPARED",
        }
    report.mkdir(parents=True)
    additions = sorted(
        {
            (SCHEMA_DIRECTORIES[str(row["schema"])], str(row["market"]))
            for row in rows
        }
    )
    canonical = root / CANONICAL_ROOT_REL
    for local_schema, market in additions:
        source = acquisition_root / "data/dbn" / local_schema / market
        destination = canonical / local_schema / market
        if destination.exists():
            raise IntegrityError(f"publication destination is not absent: {local_schema}/{market}")
        expected_files = {
            Path(str(row[key])).relative_to(Path("data/dbn") / local_schema / market).as_posix()
            for row in rows
            if SCHEMA_DIRECTORIES[str(row["schema"])] == local_schema and str(row["market"]) == market
            and row.get("current_state") != NO_DATA_STATE
            for key in ("final_path", "sidecar_path")
        }
        observed_files = {
            path.relative_to(source).as_posix() for path in source.rglob("*") if path.is_file()
        }
        if observed_files != expected_files:
            raise IntegrityError(f"publication source path set differs: {local_schema}/{market}")
        _copy_tree_plain(source, candidate_root / local_schema / market)
    prior_inventory = _inventory(canonical)
    addition_inventory = _inventory(candidate_root, include_links=True)
    if any(int(item["link_count"]) != 1 for item in addition_inventory):
        raise IntegrityError("publication candidate contains non-plain files")
    virtual_inventory = sorted(
        [*prior_inventory, *[{k: v for k, v in item.items() if k != "link_count"} for item in addition_inventory]],
        key=lambda item: item["relative_path"],
    )
    prior_units = prior_release.get("release_core", {}).get("normalized_unit_manifests")
    prior_artifacts = prior_release.get("canonical_artifact_index")
    if not isinstance(prior_units, list) or not isinstance(prior_artifacts, list):
        raise IntegrityError("prior active release lacks canonical indexes")
    added_units: list[dict[str, Any]] = []
    added_artifacts: list[dict[str, Any]] = []
    for row in rows:
        unit, artifacts = _addition_unit(
            root,
            acquisition_root,
            row,
            manifest_sha256=manifest_sha256,
            candidate_root=candidate_root,
        )
        added_units.append(unit)
        added_artifacts.extend(artifacts)
    unit_ids = {str(item.get("unit_id")) for item in prior_units if isinstance(item, Mapping)}
    if any(unit["unit_id"] in unit_ids for unit in added_units):
        raise IntegrityError("publication candidate would replace an existing canonical unit")
    all_units = sorted([*map(dict, prior_units), *added_units], key=lambda item: str(item["unit_id"]))
    all_artifacts = sorted(
        [*map(dict, prior_artifacts), *added_artifacts],
        key=lambda item: str(item["future_project_relative_path"]),
    )
    cadence_counts = {
        schema: len([item for item in (canonical / schema).iterdir() if item.is_dir()])
        + sum(1 for local_schema, _ in additions if local_schema == schema)
        for schema in ("ohlcv_1d", "ohlcv_1h", "ohlcv_1m", "ohlcv_1s")
    }
    release_core = {
        "added_directories": [f"data/dbn/{schema}/{market}" for schema, market in additions],
        "artifact_counts": {
            "dbn": sum(item["kind"] == "DBN" for item in all_artifacts),
            "manifest": sum(item["kind"] == "MANIFEST" for item in all_artifacts),
            "total": len(all_artifacts),
        },
        "batch_plan_id": batch["plan_id"],
        "batch_sha256": batch_sha256,
        "acquisition_result": {
            "path": acquisition_result_path.relative_to(root).as_posix(),
            "sha256": acquisition_result_sha256,
        },
        "cadence_root_counts_after_publication": cadence_counts,
        "canonical_artifact_root": CANONICAL_ROOT_REL.as_posix(),
        "complete_virtual_successor": _inventory_summary(virtual_inventory),
        "execution_manifest_sha256": manifest_sha256,
        "manifest_predecessor_sha256": ledger_manifest_sha256,
        "no_data_certificate": {
            "path": no_data_certificate_path.relative_to(root).as_posix(),
            "sha256": no_data_certificate_sha256,
        },
        "job_ledger": {
            "path": ledger_path.relative_to(root).as_posix(),
            "sha256": sha256_file(ledger_path),
        },
        "normalized_unit_manifests": all_units,
        "physical_publication_successor_to": pointer["release_id"],
        "schema_version": "ohlcv_58_completion_publication_successor_core/1.0.0",
        "scope": "EXACT_IMMUTABLE_COMPLETION_BATCH_ADDITIONS_ONLY",
        "source_state_root": state_root.as_posix(),
        "unit_counts": {
            "added": len(added_units),
            "added_byte_backed": len(complete_rows),
            "added_no_data_evidence_only": len(no_data_rows),
            "prior": len(prior_units),
            "successor": len(all_units),
        },
    }
    release_id = sha256_json(release_core)
    release = {
        "activation_authorized": False,
        "canonical_artifact_index": all_artifacts,
        "complete_shadow_tree": release_core["complete_virtual_successor"],
        "created_utc": utc_now(),
        "publication_authorized": False,
        "release_core": release_core,
        "release_id": release_id,
        "release_status": "PREPARED_NON_ACTIVE_REQUIRES_INDEPENDENT_CERTIFICATION",
        "schema_version": "ohlcv_58_completion_publication_successor_release/1.0.0",
    }
    release_path = report / "successor_release_manifest.json"
    create_json(release_path, release)
    wrapper = {
        "release_id": release_id,
        "release_manifest_path": release_path.relative_to(root).as_posix(),
        "release_manifest_sha256": sha256_file(release_path),
        "schema_version": "canonical_data_dbn_release_wrapper/2.0.0",
        "status": "IMMUTABLE_SUCCESSOR_WRAPPER",
    }
    wrapper_path = report / "successor_wrapper.json"
    create_json(wrapper_path, wrapper)
    pointer_template = {
        "activated_at_utc_rule": "SET_ONCE_AT_AUTHORIZED_EXECUTION",
        "canonical_artifact_root": CANONICAL_ROOT_REL.as_posix(),
        "physical_publication_successor_to": pointer["release_id"],
        "release_id": release_id,
        "release_manifest_path": release_path.relative_to(root).as_posix(),
        "release_manifest_sha256": sha256_file(release_path),
        "schema_version": "canonical_data_dbn_active_pointer/2.0.0",
        "scope": "EXACT_IMMUTABLE_COMPLETION_BATCH_ADDITIONS_ONLY",
        "status": "ACTIVE",
        "wrapper_sha256": sha256_file(wrapper_path),
    }
    pointer_template["pointer_template_id"] = sha256_json(pointer_template)
    create_json(report / "active_pointer_template.json", pointer_template)
    rollback = {
        "added_directories": release_core["added_directories"],
        "failure_policy": "RESTORE_EXACT_PRIOR_POINTER_AND_MOVE_EVERY_ADDITION_BACK_TO_STAGING",
        "prior_pointer_sha256": basis["active_pointer_sha256"],
        "prior_release_id": pointer["release_id"],
        "run_id": run_id,
        "schema_version": "ohlcv_58_completion_publication_rollback/1.0.0",
    }
    create_json(report / "rollback_plan.json", rollback)
    build_receipt = {
        "acquisition_result_sha256": acquisition_result_sha256,
        "candidate_summary": _inventory_summary(addition_inventory),
        "manifest_sha256": manifest_sha256,
        "no_data_certificate_sha256": no_data_certificate_sha256,
        "plain_file_count": len(addition_inventory),
        "release_id": release_id,
        "release_manifest_sha256": sha256_file(release_path),
        "run_id": run_id,
        "schema_version": "ohlcv_58_completion_publication_candidate_build/1.0.0",
        "status": "PASS_CANDIDATE_PREPARED_REQUIRES_INDEPENDENT_CERTIFICATION",
        "target_counts": {COMPLETE_STATE: len(complete_rows), NO_DATA_STATE: len(no_data_rows)},
        "virtual_successor": release_core["complete_virtual_successor"],
    }
    create_json(report / "candidate_build_receipt.json", build_receipt)
    return {
        "candidate_payload_root": candidate_root.relative_to(root).as_posix(),
        "release_id": release_id,
        "run_id": run_id,
        "status": build_receipt["status"],
    }


def required_publication_scope(packet: Mapping[str, Any], packet_sha256: str) -> dict[str, str]:
    return {
        "added_directory_count": str(len(packet["added_directories"])),
        "approval_command": PUBLICATION_OPERATION,
        "approval_packet_sha256": packet_sha256,
        "approval_plan_id": str(packet["packet_id"]),
        "approval_plan_sha256": packet_sha256,
        "candidate_inventory_sha256": str(packet["candidate_inventory_sha256"]),
        "independent_certificate_sha256": str(packet["independent_certificate_sha256"]),
        "prior_pointer_sha256": str(packet["current_active_pointer_sha256"]),
        "provider_access": "false",
        "release_manifest_sha256": str(packet["release_manifest_sha256"]),
        "run_id": str(packet["run_id"]),
        "successor_release_id": str(packet["successor_release_id"]),
    }


def _validate_publication_packet(
    root: Path,
    report: Path,
    packet: Mapping[str, Any],
    *,
    require_candidate: bool,
) -> dict[str, Any]:
    if (
        packet.get("schema_version") != "ohlcv_58_completion_publication_approval_packet/2.0.0"
        or packet.get("packet_id")
        != sha256_json({key: value for key, value in packet.items() if key != "packet_id"})
        or packet.get("provider_access") is not False
    ):
        raise IntegrityError("completion publication approval packet differs from contract")
    bound = {
        INDEPENDENT_CERTIFICATE_NAME: "independent_certificate_sha256",
        "successor_release_manifest.json": "release_manifest_sha256",
        "successor_wrapper.json": "successor_wrapper_sha256",
        "active_pointer_template.json": "pointer_template_sha256",
        "rollback_plan.json": "rollback_plan_sha256",
    }
    for filename, field in bound.items():
        if sha256_file(report / filename) != packet.get(field):
            raise IntegrityError(f"completion publication packet binding differs: {filename}")
    certificate = load_json(
        report / INDEPENDENT_CERTIFICATE_NAME,
        "independent publication certificate",
    )
    release = load_json(report / "successor_release_manifest.json", "completion successor release")
    if (
        certificate.get("status") != "PASS_CERTIFIED_NON_ACTIVE_REQUIRES_PUBLICATION_APPROVAL"
        or certificate.get("run_id") != packet.get("run_id")
        or certificate.get("release_id") != packet.get("successor_release_id")
        or release.get("release_id") != packet.get("successor_release_id")
    ):
        raise IntegrityError("completion publication certificate or release differs from packet")
    if require_candidate:
        candidate_root = root / STAGING_PARENT_REL / str(packet["run_id"]) / "candidate_additions/data/dbn"
        summary = _inventory_summary(_inventory(candidate_root, include_links=True))
        if (
            summary.get("inventory_sha256") != packet.get("candidate_inventory_sha256")
            or summary.get("file_count") != packet.get("added_files")
            or summary.get("total_bytes") != packet.get("added_logical_bytes")
            or any(int(item["link_count"]) != 1 for item in _inventory(candidate_root, include_links=True))
        ):
            raise IntegrityError("completion publication candidate differs from packet")
    return release


def resolve_active_successor(root: Path, run_id: str) -> dict[str, Any]:
    root = root.resolve(strict=True)
    report = root / REPORT_PARENT_REL / run_id
    release_path = report / "successor_release_manifest.json"
    release = load_json(release_path, "completion successor release")
    pointer = load_json(root / ACTIVE_POINTER_REL, "active pointer")
    if pointer.get("release_id") != release.get("release_id") or pointer.get("status") != "ACTIVE":
        raise IntegrityError("active pointer does not resolve completion successor")
    if pointer.get("release_manifest_sha256") != sha256_file(release_path):
        raise IntegrityError("completion successor pointer binding differs")
    observed = _inventory(root / CANONICAL_ROOT_REL)
    if _inventory_summary(observed) != release["complete_shadow_tree"]:
        raise IntegrityError("completion successor canonical root differs")
    for artifact in release["canonical_artifact_index"]:
        path = contained(root, Path(str(artifact["future_project_relative_path"])))
        if path.stat().st_size != artifact["size_bytes"] or sha256_file(path) != artifact["sha256"]:
            raise IntegrityError(f"completion successor artifact differs: {path}")
    return {
        "canonical_artifacts_verified": len(release["canonical_artifact_index"]),
        "complete_root": release["complete_shadow_tree"],
        "release_id": release["release_id"],
        "status": "PASS_ACTIVE_SUCCESSOR_CANONICAL_READBACK",
    }


def activate_authorized(
    root: Path,
    run_id: str,
    authorization: OperationReceipt,
    *,
    failpoint: str | None = None,
    writer_scan: Callable[[], Sequence[str]] = lambda: (),
) -> dict[str, Any]:
    root = root.resolve(strict=True)
    report = root / REPORT_PARENT_REL / run_id
    packet_path = report / APPROVAL_PACKET_NAME
    packet = load_json(packet_path, "completion publication packet")
    packet_sha = sha256_file(packet_path)
    scope = required_publication_scope(packet, packet_sha)
    boundary = RepoBoundary(root)
    pointer_path = root / ACTIVE_POINTER_REL
    pointer = load_json(pointer_path, "active pointer")
    already_active = pointer.get("release_id") == packet.get("successor_release_id")
    release = _validate_publication_packet(
        root,
        report,
        packet,
        require_candidate=not already_active,
    )
    authorization.verify(
        boundary,
        operation=PUBLICATION_OPERATION,
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        required_scope=scope,
    )
    if already_active:
        authorization.assert_consumed(
            boundary,
            operation=PUBLICATION_OPERATION,
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope=scope,
        )
        return {"status": "NO_ACTION_ALREADY_ACTIVE_SAME_RELEASE", "readback": resolve_active_successor(root, run_id)}
    if sha256_file(pointer_path) != packet["current_active_pointer_sha256"]:
        raise IntegrityError("active pointer drifted before completion publication")
    if writer_scan():
        raise IntegrityError("canonical writer detected before completion publication")
    candidate_root = root / STAGING_PARENT_REL / run_id / "candidate_additions/data/dbn"
    for relative in packet["added_directories"]:
        future = Path(str(relative))
        source = candidate_root / future.relative_to(CANONICAL_ROOT_REL)
        destination = root / future
        if not source.is_dir() or destination.exists() or source.stat().st_dev != destination.parent.stat().st_dev:
            raise IntegrityError("completion publication destination is not an absent same-volume directory")
    lease = root / LOCK_REL
    lease.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(lease, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise IntegrityError("completion publication lease exists") from exc
    os.write(descriptor, serialized_json({"run_id": run_id, "release_id": release["release_id"]}))
    os.fsync(descriptor)
    prior_pointer = pointer_path.read_bytes()
    state = _TransactionState(installed=[])
    archive = root / ARCHIVE_PARENT_REL / str(load_json(pointer_path, "active pointer")["release_id"]) / run_id
    wrapper_source = report / "successor_wrapper.json"
    wrapper = root / REGISTRY_PARENT_REL / str(release["release_id"]) / "active_wrapper_v2.json"
    try:
        if writer_scan():
            raise IntegrityError("canonical writer detected after completion publication lease")
        use_path = authorization.consume(
            boundary,
            operation=PUBLICATION_OPERATION,
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope=scope,
        )
        archive.mkdir(parents=True, exist_ok=False)
        create_bytes(archive / "prior_active_pointer.json", prior_pointer)
        wrapper.parent.mkdir(parents=True, exist_ok=False)
        create_bytes(wrapper, wrapper_source.read_bytes())
        if failpoint == "before_install":
            raise IntegrityError("injected completion publication failure: before_install")
        for relative in packet["added_directories"]:
            future = Path(str(relative))
            source = candidate_root / future.relative_to(CANONICAL_ROOT_REL)
            destination = root / future
            if destination.exists() or source.stat().st_dev != destination.parent.stat().st_dev:
                raise IntegrityError("completion publication destination is not an absent same-volume directory")
            os.replace(source, destination)
            state.installed.append((destination, source))
            if failpoint == f"after_install_{len(state.installed)}":
                raise IntegrityError(f"injected completion publication failure: {failpoint}")
        if _inventory_summary(_inventory(root / CANONICAL_ROOT_REL)) != release["complete_shadow_tree"]:
            raise IntegrityError("completion successor root differs after additions")
        if failpoint == "before_pointer_replace":
            raise IntegrityError("injected completion publication failure: before_pointer_replace")
        template = load_json(report / "active_pointer_template.json", "active pointer template")
        activated = {
            key: value
            for key, value in template.items()
            if key not in {"pointer_template_id", "activated_at_utc_rule"}
        }
        activated["activated_at_utc"] = utc_now()
        activated["wrapper_path"] = wrapper.relative_to(root).as_posix()
        activated["pointer_id"] = sha256_json(activated)
        atomic_replace_bytes(pointer_path, serialized_json(activated), run_id)
        state.pointer_replaced = True
        if failpoint == "after_pointer_replace":
            raise IntegrityError("injected completion publication failure: after_pointer_replace")
        readback = resolve_active_successor(root, run_id)
        if failpoint == "after_readback":
            raise IntegrityError("injected completion publication failure: after_readback")
        receipt = {
            "active_pointer_sha256": sha256_file(pointer_path),
            "authorization_receipt_id": authorization.receipt_id,
            "authorization_use_path": use_path.relative_to(root).as_posix(),
            "provider_calls": 0,
            "readback": readback,
            "rollback_archive_root": archive.relative_to(root).as_posix(),
            "run_id": run_id,
            "schema_version": "ohlcv_58_completion_publication_receipt/1.0.0",
            "status": "ACTIVATED_AND_VERIFIED",
            "successor_release_id": release["release_id"],
        }
        create_json(report / "publication_receipt.json", receipt)
        return receipt
    except Exception:
        if state.pointer_replaced:
            atomic_replace_bytes(pointer_path, prior_pointer, run_id + ".rollback")
        for destination, source in reversed(state.installed):
            source.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                os.replace(destination, source)
        if sha256_file(pointer_path) != packet["current_active_pointer_sha256"]:
            raise IntegrityError("completion publication rollback did not restore the prior pointer")
        raise
    finally:
        os.close(descriptor)
        lease.unlink(missing_ok=True)
