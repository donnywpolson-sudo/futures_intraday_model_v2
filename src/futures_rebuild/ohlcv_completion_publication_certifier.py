"""Independent, provider-free certification for one OHLCV-58 publication candidate."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .errors import IntegrityError
from .ohlcv_completion_campaign import load_batch
from .ohlcv_historical_backfill import (
    _validated_no_data_evidence,
    classify_target,
    load_manifest,
)
from .ohlcv_historical_backfill_v3 import SCHEMA_DIRECTORIES, authoritative_universe
from .ohlcv_msf_1d_publication_successor import (
    _inventory,
    _inventory_summary,
    contained,
    create_json,
    load_json,
    sha256_file,
    sha256_json,
    utc_now,
)
from . import ohlcv_completion_publication as publication


CERTIFICATE_NAME = "independent_certificate_v2.json"
PACKET_NAME = "activation_authorization_packet_v2.json"
EXPECTED_COMPLETE_TARGETS = 478
EXPECTED_NO_DATA_TARGETS = 2
EXPECTED_ADDED_DIRECTORIES = 48
EXPECTED_CANDIDATE_FILES = 956
EXPECTED_CANDIDATE_BYTES = 25_689_765


def _immediate_directories(path: Path) -> set[str]:
    return {item.name for item in path.iterdir() if item.is_dir()}


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
    if len(rows) != 480 or {
        (str(row["market"]), str(row["schema"])) for row in rows
    } != selected:
        raise IntegrityError("independent certification manifest scope differs")
    return rows


def _verify_source_and_candidate(
    root: Path,
    acquisition_root: Path,
    candidate_root: Path,
    rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    expected_files: dict[str, Path] = {}
    complete: list[dict[str, Any]] = []
    no_data: list[dict[str, Any]] = []
    for row in rows:
        if row.get("current_state") == publication.NO_DATA_STATE:
            evidence = _validated_no_data_evidence(row)
            evidence_path = contained(root, Path(str(evidence["evidence_path"])))
            if sha256_file(evidence_path) != evidence["evidence_sha256"]:
                raise IntegrityError("independent no-data evidence hash differs")
            observed = classify_target(acquisition_root, row, confirmed_record_count=0)
            if observed.get("current_state") != publication.NO_DATA_STATE:
                raise IntegrityError("independent no-data target is not absent")
            no_data.append(row)
            continue
        observed = classify_target(acquisition_root, row)
        if observed.get("current_state") != publication.COMPLETE_STATE:
            raise IntegrityError("independent byte-backed target is not complete")
        complete.append(row)
        for key in ("final_path", "sidecar_path"):
            relative = Path(str(row[key])).relative_to(publication.CANONICAL_ROOT_REL).as_posix()
            expected_files[relative] = acquisition_root / str(row[key])
    if len(complete) != EXPECTED_COMPLETE_TARGETS or len(no_data) != EXPECTED_NO_DATA_TARGETS:
        raise IntegrityError("independent target state counts differ")
    observed_files = {
        path.relative_to(candidate_root).as_posix(): path
        for path in candidate_root.rglob("*")
        if path.is_file()
    }
    if set(observed_files) != set(expected_files):
        raise IntegrityError("independent candidate path inventory differs")
    for relative, candidate in observed_files.items():
        source = expected_files[relative]
        if (
            candidate.stat().st_nlink != 1
            or candidate.stat().st_size != source.stat().st_size
            or sha256_file(candidate) != sha256_file(source)
        ):
            raise IntegrityError(f"independent candidate bytes differ: {relative}")
    inventory = _inventory(candidate_root, include_links=True)
    summary = _inventory_summary(inventory)
    if (
        summary["file_count"] != EXPECTED_CANDIDATE_FILES
        or summary["total_bytes"] != EXPECTED_CANDIDATE_BYTES
        or any(int(item["link_count"]) != 1 for item in inventory)
    ):
        raise IntegrityError("independent candidate size or plain-file count differs")
    return summary, complete, no_data


def _verify_release(
    root: Path,
    report: Path,
    candidate_root: Path,
    release: Mapping[str, Any],
    candidate_summary: Mapping[str, Any],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    pointer, _, prior = publication._load_pointer_and_release(root)
    core = release.get("release_core")
    if not isinstance(core, Mapping) or release.get("release_id") != sha256_json(core):
        raise IntegrityError("independent successor release identity differs")
    additions = core.get("added_directories")
    units = core.get("normalized_unit_manifests")
    artifacts = release.get("canonical_artifact_index")
    if (
        not isinstance(additions, list)
        or len(additions) != EXPECTED_ADDED_DIRECTORIES
        or not isinstance(units, list)
        or not isinstance(artifacts, list)
        or core.get("physical_publication_successor_to") != pointer.get("release_id")
        or core.get("unit_counts")
        != {
            "added": 480,
            "added_byte_backed": EXPECTED_COMPLETE_TARGETS,
            "added_no_data_evidence_only": EXPECTED_NO_DATA_TARGETS,
            "prior": 374,
            "successor": 854,
        }
    ):
        raise IntegrityError("independent successor release counts or predecessor differ")
    prior_artifacts = prior.get("canonical_artifact_index")
    if not isinstance(prior_artifacts, list):
        raise IntegrityError("independent prior release lacks artifact index")
    artifact_by_path = {
        str(item["future_project_relative_path"]): item
        for item in artifacts
        if isinstance(item, Mapping)
    }
    if len(artifact_by_path) != 1_704 or any(
        artifact_by_path.get(str(item["future_project_relative_path"])) != item
        for item in prior_artifacts
        if isinstance(item, Mapping)
    ):
        raise IntegrityError("independent successor artifact index differs from predecessor")
    for item in _inventory(candidate_root):
        future = f"data/dbn/{item['relative_path']}"
        artifact = artifact_by_path.get(future)
        if (
            not isinstance(artifact, Mapping)
            or artifact.get("sha256") != item["sha256"]
            or artifact.get("size_bytes") != item["size_bytes"]
        ):
            raise IntegrityError(f"independent successor artifact binding differs: {future}")
    no_data_ids = {
        str(row["target_id"])
        for row in rows
        if row.get("current_state") == publication.NO_DATA_STATE
    }
    no_data_units = [
        item
        for item in units
        if isinstance(item, Mapping)
        and item.get("schema_version")
        == "dbn_canonical_publication_no_data_unit_manifest/1.0.0"
    ]
    if (
        len(no_data_units) != EXPECTED_NO_DATA_TARGETS
        or any(item.get("canonical_dbn") is not None for item in no_data_units)
        or any(item.get("publication_action") != "NO_FILE_CREATE" for item in no_data_units)
    ):
        raise IntegrityError("independent no-data release units differ")
    expected = set(authoritative_universe(root)["roots"])
    projected: dict[str, set[str]] = {}
    for local_schema in ("ohlcv_1d", "ohlcv_1h"):
        projected[local_schema] = _immediate_directories(root / "data/dbn" / local_schema) | {
            Path(str(value)).name
            for value in additions
            if Path(str(value)).parent.name == local_schema
        }
    registered = {
        str(item["market"])
        for item in units
        if isinstance(item, Mapping)
        and item.get("family") in {"ohlcv-1d", "ohlcv-1h"}
    }
    if projected["ohlcv_1d"] != expected or projected["ohlcv_1h"] != expected or registered != expected:
        raise IntegrityError("independent projected 58/58 reconciliation differs")
    current_inventory = _inventory(root / publication.CANONICAL_ROOT_REL)
    candidate_inventory = [
        {key: value for key, value in item.items() if key != "link_count"}
        for item in _inventory(candidate_root, include_links=True)
    ]
    virtual = sorted([*current_inventory, *candidate_inventory], key=lambda item: item["relative_path"])
    if (
        _inventory_summary(virtual) != release.get("complete_shadow_tree")
        or _inventory_summary(candidate_inventory) != candidate_summary
    ):
        raise IntegrityError("independent complete successor inventory differs")
    return {
        "authoritative_registered_markets": 58,
        "conflict": False,
        "no_data_target_ids": sorted(no_data_ids),
        "ohlcv_1d_markets": 58,
        "ohlcv_1h_markets": 58,
        "verified_target_markets": 58,
    }


def certify_candidate(
    root: Path,
    run_id: str,
    *,
    batch_path: Path,
    batch_sha256: str,
    manifest_path: Path,
    manifest_sha256: str,
    state_root: Path,
) -> dict[str, Any]:
    root = root.resolve(strict=True)
    report = contained(root, publication.REPORT_PARENT_REL / run_id)
    candidate_root = contained(
        root,
        publication.STAGING_PARENT_REL / run_id / "candidate_additions/data/dbn",
    )
    certificate_path = report / CERTIFICATE_NAME
    packet_path = report / PACKET_NAME
    if certificate_path.exists() or packet_path.exists():
        if not certificate_path.is_file() or not packet_path.is_file():
            raise IntegrityError("partial independent publication certification exists")
        certificate = load_json(certificate_path, "existing independent certificate")
        packet = load_json(packet_path, "existing publication approval packet")
        bindings = certificate.get("bindings")
        candidate_inventory = _inventory(candidate_root, include_links=True)
        candidate_summary = _inventory_summary(candidate_inventory)
        if (
            certificate.get("status") != "PASS_CERTIFIED_NON_ACTIVE_REQUIRES_PUBLICATION_APPROVAL"
            or not isinstance(bindings, Mapping)
            or bindings.get("active_pointer_sha256")
            != sha256_file(root / publication.ACTIVE_POINTER_REL)
            or bindings.get("build_receipt_sha256")
            != sha256_file(report / "candidate_build_receipt.json")
            or bindings.get("successor_release_manifest_sha256")
            != sha256_file(report / "successor_release_manifest.json")
            or bindings.get("successor_wrapper_sha256")
            != sha256_file(report / "successor_wrapper.json")
            or bindings.get("pointer_template_sha256")
            != sha256_file(report / "active_pointer_template.json")
            or bindings.get("rollback_plan_sha256") != sha256_file(report / "rollback_plan.json")
            or certificate.get("candidate_summary") != candidate_summary
            or any(int(item["link_count"]) != 1 for item in candidate_inventory)
            or packet.get("independent_certificate_sha256") != sha256_file(certificate_path)
            or packet.get("candidate_inventory_sha256") != candidate_summary["inventory_sha256"]
            or packet.get("packet_id")
            != sha256_json({key: value for key, value in packet.items() if key != "packet_id"})
        ):
            raise IntegrityError("existing independent publication certification differs")
        return {
            "approval_packet": packet_path.relative_to(root).as_posix(),
            "approval_packet_sha256": sha256_file(packet_path),
            "release_id": certificate["release_id"],
            "run_id": run_id,
            "status": "NO_ACTION_CERTIFIED_CANDIDATE_ALREADY_EXISTS",
        }
    batch = load_batch(root, batch_path, expected_sha256=batch_sha256)
    manifest_path = manifest_path.resolve(strict=True)
    if sha256_file(manifest_path) != manifest_sha256:
        raise IntegrityError("independent execution manifest hash differs")
    rows = _selected_rows(batch, manifest_path)
    acquisition_root = contained(root, state_root)
    candidate_summary, complete, no_data = _verify_source_and_candidate(
        root,
        acquisition_root,
        candidate_root,
        rows,
    )
    build_receipt_path = report / "candidate_build_receipt.json"
    build_receipt = load_json(build_receipt_path, "candidate build receipt")
    release_path = report / "successor_release_manifest.json"
    release = load_json(release_path, "successor release")
    if (
        build_receipt.get("release_manifest_sha256") != sha256_file(release_path)
        or build_receipt.get("candidate_summary") != candidate_summary
        or build_receipt.get("manifest_sha256") != manifest_sha256
    ):
        raise IntegrityError("independent candidate differs from build receipt")
    reconciliation = _verify_release(
        root,
        report,
        candidate_root,
        release,
        candidate_summary,
        rows,
    )
    pointer_sha = sha256_file(root / publication.ACTIVE_POINTER_REL)
    rollback_path = report / "rollback_plan.json"
    rollback = load_json(rollback_path, "publication rollback plan")
    if (
        rollback.get("prior_pointer_sha256") != pointer_sha
        or rollback.get("prior_release_id") != release["release_core"]["physical_publication_successor_to"]
        or rollback.get("added_directories") != release["release_core"]["added_directories"]
    ):
        raise IntegrityError("independent rollback transaction differs")
    wrapper_path = report / "successor_wrapper.json"
    pointer_template_path = report / "active_pointer_template.json"
    certificate = {
        "bindings": {
            "active_pointer_sha256": pointer_sha,
            "batch_sha256": batch_sha256,
            "build_receipt_sha256": sha256_file(build_receipt_path),
            "candidate_inventory_sha256": candidate_summary["inventory_sha256"],
            "execution_manifest_sha256": manifest_sha256,
            "independent_certifier_sha256": sha256_file(Path(__file__)),
            "pointer_template_sha256": sha256_file(pointer_template_path),
            "publication_implementation_sha256": sha256_file(Path(publication.__file__)),
            "rollback_plan_sha256": sha256_file(rollback_path),
            "successor_release_manifest_sha256": sha256_file(release_path),
            "successor_wrapper_sha256": sha256_file(wrapper_path),
        },
        "candidate_summary": candidate_summary,
        "certified_at_utc": utc_now(),
        "provider_calls": 0,
        "reconciliation": reconciliation,
        "release_id": release["release_id"],
        "run_id": run_id,
        "schema_version": "ohlcv_58_completion_publication_independent_certificate/2.0.0",
        "status": "PASS_CERTIFIED_NON_ACTIVE_REQUIRES_PUBLICATION_APPROVAL",
        "target_counts": {
            publication.COMPLETE_STATE: len(complete),
            publication.NO_DATA_STATE: len(no_data),
        },
    }
    create_json(certificate_path, certificate)
    packet_core = {
        "added_directories": release["release_core"]["added_directories"],
        "added_files": candidate_summary["file_count"],
        "added_logical_bytes": candidate_summary["total_bytes"],
        "batch_plan_id": batch["plan_id"],
        "candidate_inventory_sha256": candidate_summary["inventory_sha256"],
        "current_active_pointer_sha256": pointer_sha,
        "independent_certificate_sha256": sha256_file(certificate_path),
        "pointer_template_sha256": sha256_file(pointer_template_path),
        "provider_access": False,
        "publication_effect": "INSTALL_EXACT_ABSENT_DIRECTORIES_AND_COMPARE_SWAP_POINTER",
        "release_manifest_sha256": sha256_file(release_path),
        "rollback_plan_sha256": sha256_file(rollback_path),
        "run_id": run_id,
        "schema_version": "ohlcv_58_completion_publication_approval_packet/2.0.0",
        "successor_release_id": release["release_id"],
        "successor_wrapper_sha256": sha256_file(wrapper_path),
    }
    packet = {**packet_core, "packet_id": sha256_json(packet_core)}
    create_json(packet_path, packet)
    return {
        "approval_packet": packet_path.relative_to(root).as_posix(),
        "approval_packet_sha256": sha256_file(packet_path),
        "release_id": release["release_id"],
        "run_id": run_id,
        "status": certificate["status"],
    }
