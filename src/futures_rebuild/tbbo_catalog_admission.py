"""Provider-free canonical publication of the certified 16-market TBBO snapshot."""

from __future__ import annotations

from .custody_successor import resolve_snapshot, binding_matches

import hashlib
import os
import shutil
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Final

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, io_path, sha256_file, sha256_json
from .data_layout import (
    DataFileEntry,
    DataReleaseManifest,
    PhasePublisher,
    verify_data_tree_closure,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .tbbo_16_market_acquisition import json_object, write_or_verify


PLAN_PATH: Final = Path("configs/tbbo_catalog_admission_plan_v1.json")
PUBLICATION_OPERATION: Final = "PUBLISH_RELEASE"
CATALOG_ROOT: Final = Path("state/tbbo_eligibility_catalog_registry")
PREPARATION_ROOT: Final = Path("state/tbbo_publication_preparations")
PUBLICATION_LOCK: Final = Path("state/locks/tbbo_canonical_publication_v1.lock")
MARKETS: Final = (
    "ES", "NQ", "CL", "GC", "NG", "RB", "HG", "SR3",
    "ZN", "ZB", "6E", "6J", "ZC", "ZS", "LE", "HE",
)
DBN_FILE_COUNT: Final = 192
SIDECAR_FILE_COUNT: Final = 192
MINIMUM_FREE_BYTES: Final = 12_000_000_000


def load_plan(*, root: Path) -> dict[str, object]:
    plan = json_object(root / PLAN_PATH, "TBBO catalog-admission plan")
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    implementation = plan.get("implementation")
    if (
        plan_id != sha256_json(core)
        or plan.get("operation") != PUBLICATION_OPERATION
        or tuple(plan.get("markets", ())) != MARKETS
        or plan.get("dbn_file_count") != DBN_FILE_COUNT
        or plan.get("sidecar_file_count") != SIDECAR_FILE_COUNT
        or plan.get("minimum_free_bytes") != MINIMUM_FREE_BYTES
        or plan.get("canonical_root") != "data/dbn/tbbo"
        or plan.get("vault_snapshot_preserved") is not True
        or plan.get("alpha_source_authorized") is not False
        or not isinstance(implementation, Mapping)
        or implementation.get("path") != Path(__file__).relative_to(root).as_posix()
        or not binding_matches(root, Path(__file__), implementation.get("sha256"))
    ):
        raise ContractError("TBBO catalog-admission plan drifted")
    return plan


def _bound_sources(
    *, root: Path, verify_payloads: bool = False
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    plan = load_plan(root=root)
    custody_path = root / str(plan["custody_registry_path"])
    certificate_path = root / str(plan["quality_certificate_path"])
    custody = json_object(custody_path, "TBBO custody registry")
    certificate = json_object(certificate_path, "TBBO quality certificate")
    entries = custody.get("entries")
    if (
        sha256_file(custody_path) != plan["custody_registry_sha256"]
        or custody.get("snapshot_id") != plan["snapshot_id"]
        or custody.get("dbn_file_count") != DBN_FILE_COUNT
        or custody.get("sidecar_file_count") != SIDECAR_FILE_COUNT
        or custody.get("total_payload_bytes") != plan["total_payload_bytes"]
        or not isinstance(entries, list)
        or len(entries) != DBN_FILE_COUNT
        or sha256_file(certificate_path) != plan["quality_certificate_sha256"]
        or certificate.get("certificate_id") != plan["quality_certificate_id"]
        or certificate.get("state")
        != "CERTIFIED_ROW_QUALITY_WITH_MANDATORY_EXCLUSIONS_NOT_CATALOG_ADMITTED"
        or certificate.get("research_catalog_admitted") is not False
        or certificate.get("alpha_research_authorized") is not False
        or tuple(certificate.get("markets", ())) != MARKETS
        or any(item.get("quality_passed") is not True for item in certificate.get("market_results", ()))
    ):
        raise IntegrityError("TBBO custody or quality-certificate binding differs")
    vault = resolve_snapshot(root, str(custody["vault_snapshot_path"]))
    seen: set[str] = set()
    for item in entries:
        if not isinstance(item, Mapping) or not isinstance(item.get("dbn"), Mapping):
            raise IntegrityError("TBBO custody entry schema is invalid")
        dbn = item["dbn"]
        relative = str(dbn["path"])
        source = vault / relative
        if relative in seen or not relative.startswith("dbn/tbbo/"):
            raise IntegrityError("TBBO custody paths are duplicate or invalid")
        seen.add(relative)
        if not source.is_file() or source.stat().st_size != int(dbn["size"]):
            raise IntegrityError("TBBO vault payload is missing or has wrong size")
        if verify_payloads and sha256_file(source) != dbn["sha256"]:
            raise IntegrityError("TBBO vault payload hash differs")
    return plan, custody, certificate


def _catalog_entry(
    source: Mapping[str, object],
    market_result: Mapping[str, object],
    *,
    degraded_dates: tuple[str, ...],
    market_quarantined_dates: tuple[str, ...],
) -> dict[str, object]:
    dbn = source["dbn"]
    query = source["query"]
    assert isinstance(dbn, Mapping) and isinstance(query, Mapping)
    contract = query["query_contract"]
    assert isinstance(contract, Mapping)
    start = str(contract["start"])[:10]
    end = str(contract["end"])[:10]
    interval = f"{start}/{end}"
    monthly = {
        str(item["interval"]): int(item["row_count"])
        for item in market_result["monthly"]
    }
    if interval not in monthly:
        raise IntegrityError("TBBO quality certificate lacks a monthly row count")
    start_date, end_date = date.fromisoformat(start), date.fromisoformat(end)
    exclusions = sorted(
        day
        for day in {*degraded_dates, *market_quarantined_dates}
        if start_date <= date.fromisoformat(day) < end_date
    )
    return {
        "request_id": query["request_id"],
        "market": query["market"],
        "coverage_start": start,
        "coverage_end": end,
        "logical_path": f"data/{dbn['path']}",
        "dbn_sha256": dbn["sha256"],
        "dbn_size_bytes": dbn["size"],
        "row_count": monthly[interval],
        "mandatory_excluded_utc_dates": exclusions,
        "disposition": (
            "CERTIFIED_SOURCE_ELIGIBLE_WITH_MANDATORY_ROW_EXCLUSIONS_"
            "NOT_ALPHA_ACTIVE"
        ),
        "row_quality_certified": True,
        "source_eligible": True,
        "alpha_source_authorized": False,
    }


def build_eligibility_catalog(*, root: Path) -> dict[str, object]:
    plan, custody, certificate = _bound_sources(root=root)
    by_market = {
        str(item["market"]): item for item in certificate["market_results"]
    }
    quarantines = certificate["market_quarantined_utc_dates"]
    entries = [
        _catalog_entry(
            item,
            by_market[str(item["query"]["market"])],
            degraded_dates=tuple(str(day) for day in certificate["degraded_utc_dates"]),
            market_quarantined_dates=tuple(
                str(day) for day in quarantines.get(str(item["query"]["market"]), ())
            ),
        )
        for item in custody["entries"]
    ]
    entries.sort(key=lambda item: (MARKETS.index(str(item["market"])), str(item["coverage_start"])))
    if len(entries) != DBN_FILE_COUNT or any(not item["source_eligible"] for item in entries):
        raise IntegrityError("TBBO eligibility catalog is incomplete")
    core = {
        "schema_version": "tbbo_eligibility_catalog/1.0.0",
        "state": "SEALED_SOURCE_ELIGIBILITY_CATALOG_NOT_ALPHA_ACTIVE",
        "plan_id": plan["plan_id"],
        "snapshot_id": custody["snapshot_id"],
        "quality_certificate_id": certificate["certificate_id"],
        "entries": entries,
        "entry_count": len(entries),
        "eligible_count": len(entries),
        "mandatory_exclusion_policy": {
            "degraded_utc_dates": certificate["degraded_utc_dates"],
            "boundary_sessions": certificate["boundary_sessions"],
            "market_quarantined_utc_dates": quarantines,
            "status_only_session_policy": certificate["status_only_session_policy"],
            "identical_tbbo_row_policy": certificate["identical_tbbo_row_policy"],
        },
        "alpha_source_authorized": False,
    }
    catalog_id = sha256_json(core)
    catalog = {**core, "catalog_id": catalog_id}
    write_or_verify(
        root / CATALOG_ROOT / catalog_id / "catalog.json",
        catalog,
        "TBBO eligibility catalog",
    )
    return catalog


def _canonical_sidecar(
    *, source: Mapping[str, object], eligibility: Mapping[str, object],
    snapshot_id: str, certificate_id: str, catalog_id: str,
) -> dict[str, object]:
    core = {
        "schema_version": "tbbo_canonical_sidecar/1.0.0",
        "state": "PUBLISHED_CANONICAL_CUSTODY",
        "market": eligibility["market"],
        "coverage_start": eligibility["coverage_start"],
        "coverage_end": eligibility["coverage_end"],
        "logical_path": eligibility["logical_path"],
        "request_id": eligibility["request_id"],
        "source_snapshot_id": snapshot_id,
        "source_dbn_sha256": source["dbn"]["sha256"],
        "source_dbn_size_bytes": source["dbn"]["size"],
        "source_sidecar_sha256": source["sidecar"]["sha256"],
        "quality_certificate_id": certificate_id,
        "eligibility_catalog_id": catalog_id,
        "mandatory_excluded_utc_dates": eligibility["mandatory_excluded_utc_dates"],
        "row_quality_certified": True,
        "source_eligible": True,
        "alpha_source_authorized": False,
    }
    return {**core, "sidecar_id": sha256_json(core)}


def _copy_verified(source: Path, destination: Path, *, size: int, sha256: str) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    written = 0
    with io_path(source).open("rb") as reader, io_path(destination).open("xb") as writer:
        while chunk := reader.read(1_048_576):
            digest.update(chunk)
            writer.write(chunk)
            written += len(chunk)
        writer.flush()
        os.fsync(writer.fileno())
    if written != size or digest.hexdigest() != sha256:
        raise IntegrityError("TBBO publication copy differs from sealed custody")


def prepare_publication(
    *, root: Path, catalog: Mapping[str, object], authorization: OperationReceipt
) -> dict[str, object]:
    plan, custody, certificate = _bound_sources(root=root)
    if shutil.disk_usage(root).free < MINIMUM_FREE_BYTES:
        raise UnauthorizedOperation("TBBO publication free-space floor failed")
    boundary = RepoBoundary(root)
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=authorization,
        lock_path=root / PUBLICATION_LOCK,
    )
    stage = publisher.create_stage("tbbo_canonical")
    vault = resolve_snapshot(root, str(custody["vault_snapshot_path"]))
    eligibility = {str(item["request_id"]): item for item in catalog["entries"]}
    logical_paths: dict[str, str] = {}
    for item in custody["entries"]:
        dbn = item["dbn"]
        source_dbn = vault / str(dbn["path"])
        logical_dbn = f"data/{dbn['path']}"
        staged_dbn = stage / str(dbn["path"]).removeprefix("dbn/")
        _copy_verified(
            source_dbn,
            staged_dbn,
            size=int(dbn["size"]),
            sha256=str(dbn["sha256"]),
        )
        logical_paths[staged_dbn.relative_to(stage).as_posix()] = logical_dbn
        request_id = str(item["query"]["request_id"])
        sidecar = _canonical_sidecar(
            source=item,
            eligibility=eligibility[request_id],
            snapshot_id=str(custody["snapshot_id"]),
            certificate_id=str(certificate["certificate_id"]),
            catalog_id=str(catalog["catalog_id"]),
        )
        staged_sidecar = Path(f"{staged_dbn}.manifest.json")
        with io_path(staged_sidecar).open("xb") as stream:
            stream.write(canonical_bytes(sidecar) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        logical_paths[staged_sidecar.relative_to(stage).as_posix()] = f"{logical_dbn}.manifest.json"
    manifest = DataReleaseManifest.build(
        stage,
        phase="dbn",
        release_kind="tbbo_16_market_certified_canonical_custody",
        schema_version="1.0.0",
        logical_paths=logical_paths,
        source_release_ids=tuple(sorted((
            str(custody["snapshot_id"]),
            str(certificate["certificate_id"]),
            str(catalog["catalog_id"]),
        ))),
        embedded_documents={
            "quality_certificate": {
                "id": certificate["certificate_id"],
                "sha256": plan["quality_certificate_sha256"],
            },
            "eligibility_catalog": {
                "id": catalog["catalog_id"],
                "sha256": sha256_file(
                    root / CATALOG_ROOT / str(catalog["catalog_id"]) / "catalog.json"
                ),
            },
        },
        metadata={
            "snapshot_id": custody["snapshot_id"],
            "dbn_file_count": DBN_FILE_COUNT,
            "sidecar_file_count": SIDECAR_FILE_COUNT,
            "eligible_count": DBN_FILE_COUNT,
            "vault_snapshot_preserved": True,
            "alpha_source_authorized": False,
        },
    )
    preparation = {
        "schema_version": "tbbo_publication_preparation/1.0.0",
        "state": "PREPARED_FOR_MANIFEST_LAST_PUBLICATION",
        "plan_id": plan["plan_id"],
        "release_id": manifest.release_id,
        "stage_path": stage.relative_to(root).as_posix(),
        "manifest": manifest.as_dict(),
        "logical_paths": logical_paths,
        "snapshot_id": custody["snapshot_id"],
        "quality_certificate_id": certificate["certificate_id"],
        "eligibility_catalog_id": catalog["catalog_id"],
        "file_count": len(manifest.files),
    }
    write_or_verify(
        root / PREPARATION_ROOT / manifest.release_id / "preparation.json",
        preparation,
        "TBBO publication preparation",
    )
    return preparation


def required_publication_scope(
    *, root: Path, preparation: Mapping[str, object]
) -> dict[str, str]:
    plan = load_plan(root=root)
    return {
        "snapshot_id": str(preparation["snapshot_id"]),
        "quality_certificate_id": str(preparation["quality_certificate_id"]),
        "eligibility_catalog_id": str(preparation["eligibility_catalog_id"]),
        "release_id": str(preparation["release_id"]),
        "dbn_file_count": str(DBN_FILE_COUNT),
        "sidecar_file_count": str(SIDECAR_FILE_COUNT),
        "canonical_root": "data/dbn/tbbo",
        "vault_snapshot_preserved": "true",
        "alpha_source_authorized": "false",
        "approval_command": PUBLICATION_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def publish_prepared(
    *, root: Path, preparation: Mapping[str, object], authorization: OperationReceipt
) -> dict[str, object]:
    boundary = RepoBoundary(root)
    authorization.consume(
        boundary,
        operation=PUBLICATION_OPERATION,
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        required_scope=required_publication_scope(root=root, preparation=preparation),
    )
    payload = preparation["manifest"]
    if not isinstance(payload, Mapping):
        raise IntegrityError("TBBO publication preparation lacks its manifest")
    manifest = DataReleaseManifest(
        release_id=str(payload["release_id"]),
        phase=str(payload["phase"]),
        release_kind=str(payload["release_kind"]),
        schema_version=str(payload["schema_version"]),
        source_release_ids=tuple(str(item) for item in payload["source_release_ids"]),
        files=tuple(
            DataFileEntry(str(item["logical_path"]), int(item["size"]), str(item["sha256"]))
            for item in payload["files"]
        ),
        embedded_documents=dict(payload["embedded_documents"]),
        metadata=dict(payload["metadata"]),
        layout_version=str(payload["layout_version"]),
        manifest_version=str(payload["manifest_version"]),
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=authorization,
        lock_path=root / PUBLICATION_LOCK,
    )
    stage = root / str(preparation["stage_path"])
    relative_to_logical = dict(preparation["logical_paths"])
    manifest_path = publisher.publish(
        stage,
        manifest,
        staged_paths={logical: relative for relative, logical in relative_to_logical.items()},
    )
    closure = verify_data_tree_closure(boundary)
    return {
        "release_id": manifest.release_id,
        "manifest_path": manifest_path.relative_to(root).as_posix(),
        "eligibility_catalog_id": preparation["eligibility_catalog_id"],
        "canonical_root": "data/dbn/tbbo",
        "dbn_file_count": DBN_FILE_COUNT,
        "sidecar_file_count": SIDECAR_FILE_COUNT,
        "vault_snapshot_preserved": True,
        "alpha_source_authorized": False,
        "data_tree_closure": closure,
    }


__all__ = [
    "PLAN_PATH",
    "PUBLICATION_OPERATION",
    "build_eligibility_catalog",
    "load_plan",
    "prepare_publication",
    "publish_prepared",
    "required_publication_scope",
]
