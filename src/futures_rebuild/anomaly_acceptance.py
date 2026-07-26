"""Offline, materialization-only acceptance for quarantined DBN source files."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Mapping, Sequence

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import DataReleaseManifest, DataReleaseReceipt, PhasePublisher
from .errors import ContractError, IntegrityError
from .source_contract import legacy_roots_from_contract

if TYPE_CHECKING:
    from .foundation.snapshot import PublishedDbnRelease


ACCEPTANCE_RELEASE_KIND = "futures_dbn_anomaly_materialization_acceptance"
ACCEPTANCE_SCHEMA_VERSION = "3.0.0"
ACCEPTANCE_STATUS = (
    "ACCEPTED_FOR_MATERIALIZATION_ONLY_CAUSAL_QUARANTINE_RETAINED"
)
ACCEPTANCE_DOCUMENT = "anomaly_materialization_acceptance.json"
QUARANTINE_PREFIX = "QUARANTINED_"


def _catalog_document_sha256(selection: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_bytes(dict(selection)) + b"\n").hexdigest()


def _selection_identity(selection: Mapping[str, object]) -> str:
    selection_id = selection.get("selection_manifest_id")
    core = {key: value for key, value in selection.items() if key != "selection_manifest_id"}
    if (
        type(selection_id) is not str
        or selection_id != sha256_json(core)
        or selection.get("catalog_contract_version") != "2.0.0"
        or selection.get("source_scope") != "VERIFIED_LAYOUT_V2_DBN_RELEASE"
    ):
        raise IntegrityError("anomaly acceptance catalog identity is invalid")
    return selection_id


def _quarantined_entries(
    selection: Mapping[str, object],
) -> tuple[Mapping[str, object], ...]:
    raw_files = selection.get("files")
    if not isinstance(raw_files, list):
        raise IntegrityError("anomaly acceptance catalog files are invalid")
    result: list[Mapping[str, object]] = []
    seen: set[str] = set()
    for raw in raw_files:
        if not isinstance(raw, dict):
            raise IntegrityError("anomaly acceptance catalog file entry is invalid")
        core = {key: value for key, value in raw.items() if key != "validation_sha256"}
        validation_sha256 = raw.get("validation_sha256")
        if type(validation_sha256) is not str or validation_sha256 != sha256_json(core):
            raise IntegrityError("anomaly acceptance catalog validation hash is invalid")
        disposition = raw.get("coverage_disposition")
        if type(disposition) is not str:
            raise IntegrityError("anomaly acceptance coverage disposition is invalid")
        if disposition.startswith(QUARANTINE_PREFIX):
            if validation_sha256 in seen:
                raise IntegrityError("anomaly acceptance catalog contains a duplicate")
            seen.add(validation_sha256)
            result.append(raw)
    return tuple(sorted(result, key=lambda item: str(item["path"])))


def _verify_source_binding(
    selection: Mapping[str, object], snapshot: "PublishedDbnRelease"
) -> None:
    if (
        selection.get("source_dbn_release_id") != snapshot.source_release_id
        or selection.get("source_dbn_manifest_sha256")
        != snapshot.source_manifest_sha256
    ):
        raise IntegrityError("anomaly acceptance is bound to another DBN release")


def _source_pair(
    snapshot: "PublishedDbnRelease", raw: Mapping[str, object]
) -> tuple[Path, Path]:
    path = str(raw.get("path", ""))
    sidecar_path = str(raw.get("sidecar_path", ""))
    if not path.startswith("data/dbn/") or sidecar_path != f"{path}.manifest.json":
        raise IntegrityError("quarantined DBN path binding is invalid")
    binding = snapshot.file(path.removeprefix("data/"))
    sidecar = snapshot.file(sidecar_path.removeprefix("data/"))
    if (
        raw.get("sha256") != binding.sha256
        or raw.get("size") != binding.size
        or raw.get("sidecar_sha256") != sidecar.sha256
        or raw.get("sidecar_size") != sidecar.size
    ):
        raise IntegrityError("quarantined DBN bytes differ from the catalog")
    return binding.verify(), sidecar.verify()


def publish_anomaly_materialization_acceptance(
    selection: Mapping[str, object],
    *,
    snapshot: "PublishedDbnRelease",
    publisher: PhasePublisher,
    maximum_total_bytes: int,
) -> DataReleaseReceipt:
    """Full-scan quarantined files and authorize only causal materialization."""

    selection_id = _selection_identity(selection)
    _verify_source_binding(selection, snapshot)
    if publisher.boundary.repository_id != snapshot.receipt.repository_id:
        raise IntegrityError("anomaly acceptance publisher belongs to another repository")
    anomaly_contract = publisher.boundary.active_root / "configs" / "known_anomalies.json"
    publisher.boundary.assert_active_path(
        anomaly_contract, purpose="known anomaly contract", subtree="configs"
    )
    if selection.get("known_anomalies_sha256") != sha256_file(anomaly_contract):
        raise IntegrityError("known anomaly contract changed after catalog creation")
    quarantined = _quarantined_entries(selection)
    if not quarantined:
        raise ContractError("anomaly acceptance is unnecessary without quarantined files")
    if type(maximum_total_bytes) is not int or maximum_total_bytes <= 0:
        raise ContractError("maximum_total_bytes must be a positive exact integer")
    observed_total_bytes = sum(int(item.get("size", -1)) for item in quarantined)
    if observed_total_bytes < 0 or observed_total_bytes > maximum_total_bytes:
        raise ContractError("quarantined DBN bytes exceed the explicit scan cap")

    # Import locally so dbn_catalog can retain a compatibility wrapper without a cycle.
    from .dbn_catalog import validate_dbn_pair

    validations: list[dict[str, object]] = []
    for raw in quarantined:
        dbn_path, sidecar_path = _source_pair(snapshot, raw)
        result = validate_dbn_pair(
            dbn_path,
            logical_path=str(raw["path"]),
            sidecar_path=sidecar_path,
            expected_schema=str(raw["schema"]),
            role="QUARANTINED_MATERIALIZATION_ONLY",
            sample_records=1,
            scan_to_end=True,
        )
        stable_fields = (
            "end",
            "market",
            "path",
            "query_contract_id",
            "query_mode_id",
            "schema",
            "sha256",
            "sidecar_path",
            "sidecar_sha256",
            "sidecar_size",
            "size",
            "start",
            "year",
        )
        if any(result.get(field) != raw.get(field) for field in stable_fields):
            raise IntegrityError("full-stream validation differs from the catalog binding")
        decode = result.get("decode")
        if (
            not isinstance(decode, dict)
            or decode.get("decode_status") != "FULL_SCAN"
            or type(decode.get("record_count")) is not int
            or int(decode["record_count"]) <= 0
        ):
            raise IntegrityError("quarantined DBN did not complete a full-stream scan")
        validations.append(
            {
                "catalog_validation_sha256": raw["validation_sha256"],
                "end": raw["end"],
                "full_scan_validation_sha256": result["validation_sha256"],
                "market": raw["market"],
                "path": raw["path"],
                "query_contract_id": raw["query_contract_id"],
                "query_mode_id": raw["query_mode_id"],
                "record_count": decode["record_count"],
                "schema": raw["schema"],
                "sha256": raw["sha256"],
                "sidecar_path": raw["sidecar_path"],
                "sidecar_sha256": raw["sidecar_sha256"],
                "size": raw["size"],
                "start": raw["start"],
                "year": raw["year"],
            }
        )
    payload = {
        "acceptance_contract_version": ACCEPTANCE_SCHEMA_VERSION,
        "accepted_catalog_validation_sha256s": sorted(
            str(item["validation_sha256"]) for item in quarantined
        ),
        "catalog_document_sha256": _catalog_document_sha256(selection),
        "catalog_selection_manifest_id": selection_id,
        "causal_quarantine_retained": True,
        "file_count": len(validations),
        "known_anomalies_sha256": selection["known_anomalies_sha256"],
        "maximum_total_bytes": maximum_total_bytes,
        "model_fit_count": 0,
        "provider_call_count": 0,
        "research_eligibility_granted": False,
        "scan_policy": "FULL_STREAM_BOUNDED_MEMORY_SINGLE_WORKER",
        "source_dbn_manifest_sha256": snapshot.source_manifest_sha256,
        "source_dbn_release_id": snapshot.source_release_id,
        "status": ACCEPTANCE_STATUS,
        "total_bytes": observed_total_bytes,
        "trading_action_count": 0,
        "validations": validations,
        "wfa_oos_run_count": 0,
    }
    stage = publisher.create_stage("anomaly_acceptance")
    manifest = DataReleaseManifest.build(
        stage,
        phase="evidence",
        release_kind=ACCEPTANCE_RELEASE_KIND,
        schema_version=ACCEPTANCE_SCHEMA_VERSION,
        source_release_ids=(snapshot.source_release_id,),
        embedded_documents={ACCEPTANCE_DOCUMENT: payload},
        metadata={
            "catalog_selection_manifest_id": selection_id,
            "causal_quarantine_retained": True,
            "research_eligibility_granted": False,
        },
    )
    path = publisher.publish(stage, manifest)
    return DataReleaseReceipt.from_manifest(path, publisher.boundary)


def assert_anomaly_materialization_eligible(
    selection: Mapping[str, object],
    *,
    acceptance_receipts: Sequence[DataReleaseReceipt],
    snapshot: "PublishedDbnRelease",
    boundary: RepoBoundary,
) -> None:
    """Require exactly one immutable aggregate acceptance when quarantine exists."""

    selection_id = _selection_identity(selection)
    _verify_source_binding(selection, snapshot)
    anomaly_contract = boundary.active_root / "configs" / "known_anomalies.json"
    boundary.assert_active_path(
        anomaly_contract, purpose="known anomaly contract", subtree="configs"
    )
    if selection.get("known_anomalies_sha256") != sha256_file(anomaly_contract):
        raise IntegrityError("known anomaly contract changed after catalog creation")
    quarantined = _quarantined_entries(selection)
    if not quarantined:
        if acceptance_receipts:
            raise IntegrityError("anomaly acceptance is forbidden without quarantine")
        return
    if len(acceptance_receipts) != 1:
        raise IntegrityError("quarantined source requires one exact aggregate acceptance")
    receipt = acceptance_receipts[0]
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "evidence"
        or manifest.release_kind != ACCEPTANCE_RELEASE_KIND
        or manifest.schema_version != ACCEPTANCE_SCHEMA_VERSION
        or manifest.files
        or manifest.source_release_ids != (snapshot.source_release_id,)
        or set(manifest.embedded_documents) != {ACCEPTANCE_DOCUMENT}
        or manifest.metadata
        != {
            "catalog_selection_manifest_id": selection_id,
            "causal_quarantine_retained": True,
            "research_eligibility_granted": False,
        }
    ):
        raise IntegrityError("anomaly materialization acceptance release is invalid")
    payload = receipt.embedded_document(ACCEPTANCE_DOCUMENT, boundary)
    expected_keys = {
        "acceptance_contract_version",
        "accepted_catalog_validation_sha256s",
        "catalog_document_sha256",
        "catalog_selection_manifest_id",
        "causal_quarantine_retained",
        "file_count",
        "known_anomalies_sha256",
        "maximum_total_bytes",
        "model_fit_count",
        "provider_call_count",
        "research_eligibility_granted",
        "scan_policy",
        "source_dbn_manifest_sha256",
        "source_dbn_release_id",
        "status",
        "total_bytes",
        "trading_action_count",
        "validations",
        "wfa_oos_run_count",
    }
    expected_ids = sorted(str(item["validation_sha256"]) for item in quarantined)
    if (
        not isinstance(payload, dict)
        or set(payload) != expected_keys
        or payload.get("acceptance_contract_version") != ACCEPTANCE_SCHEMA_VERSION
        or payload.get("status") != ACCEPTANCE_STATUS
        or payload.get("catalog_selection_manifest_id") != selection_id
        or payload.get("catalog_document_sha256")
        != _catalog_document_sha256(selection)
        or payload.get("known_anomalies_sha256")
        != selection.get("known_anomalies_sha256")
        or payload.get("source_dbn_release_id") != snapshot.source_release_id
        or payload.get("source_dbn_manifest_sha256")
        != snapshot.source_manifest_sha256
        or payload.get("causal_quarantine_retained") is not True
        or payload.get("research_eligibility_granted") is not False
        or any(payload.get(key) != 0 for key in (
            "model_fit_count", "provider_call_count", "trading_action_count", "wfa_oos_run_count"
        ))
        or payload.get("scan_policy") != "FULL_STREAM_BOUNDED_MEMORY_SINGLE_WORKER"
        or payload.get("accepted_catalog_validation_sha256s") != expected_ids
        or type(payload.get("file_count")) is not int
        or payload.get("file_count") != len(quarantined)
    ):
        raise IntegrityError("anomaly materialization acceptance evidence is not exact")
    validations = payload.get("validations")
    if not isinstance(validations, list) or len(validations) != len(quarantined):
        raise IntegrityError("anomaly materialization validation census is incomplete")
    by_id = {str(item["validation_sha256"]): item for item in quarantined}
    seen: set[str] = set()
    total_bytes = 0
    validation_keys = {
        "catalog_validation_sha256",
        "end",
        "full_scan_validation_sha256",
        "market",
        "path",
        "query_contract_id",
        "query_mode_id",
        "record_count",
        "schema",
        "sha256",
        "sidecar_path",
        "sidecar_sha256",
        "size",
        "start",
        "year",
    }
    for validation in validations:
        if not isinstance(validation, dict) or set(validation) != validation_keys:
            raise IntegrityError("anomaly materialization validation entry is invalid")
        catalog_id = validation.get("catalog_validation_sha256")
        if type(catalog_id) is not str or catalog_id in seen or catalog_id not in by_id:
            raise IntegrityError("anomaly materialization validation binding is invalid")
        raw = by_id[catalog_id]
        seen.add(catalog_id)
        exact_fields = (
            "end", "market", "path", "query_contract_id", "query_mode_id", "schema",
            "sha256", "sidecar_path", "sidecar_sha256", "size", "start", "year",
        )
        if (
            any(validation.get(field) != raw.get(field) for field in exact_fields)
            or type(validation.get("full_scan_validation_sha256")) is not str
            or re.fullmatch(
                r"[0-9a-f]{64}", str(validation["full_scan_validation_sha256"])
            )
            is None
            or type(validation.get("record_count")) is not int
            or int(validation["record_count"]) <= 0
        ):
            raise IntegrityError("anomaly materialization validation differs from catalog")
        _source_pair(snapshot, raw)
        total_bytes += int(raw["size"])
    if (
        seen != set(expected_ids)
        or type(payload.get("total_bytes")) is not int
        or payload.get("total_bytes") != total_bytes
        or type(payload.get("maximum_total_bytes")) is not int
        or int(payload["maximum_total_bytes"]) < total_bytes
    ):
        raise IntegrityError("anomaly materialization byte or coverage census is invalid")


def _boundary_from_contract(repository_root: Path, source_contract: Path) -> RepoBoundary:
    try:
        payload = json.loads(source_contract.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ContractError("source contract JSON is invalid") from exc
    if not isinstance(payload, dict):
        raise ContractError("source contract must be a JSON object")
    boundary = RepoBoundary(
        Path(str(payload["active_repository"])),
        legacy_roots=legacy_roots_from_contract(payload),
        foreign_roots=(
            Path.home() / "Desktop" / "US_stocks_swing_model",
            Path.home() / "Desktop" / "US_stocks_swing_model_v2",
        ),
    )
    boundary.assert_active_root(repository_root)
    boundary.assert_active_path(source_contract, purpose="source contract", subtree="configs")
    return boundary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    parser.add_argument("--source-dbn-manifest", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--maximum-total-bytes", type=int, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if not args.execute:
        parser.error("anomaly materialization acceptance requires explicit --execute")
    boundary = _boundary_from_contract(args.repository_root, args.source_contract)
    catalog = boundary.assert_active_path(
        args.catalog, purpose="verified DBN catalog", subtree="state/source_selection"
    )
    try:
        raw = catalog.read_bytes()
        selection = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("verified DBN catalog JSON is invalid") from exc
    if not isinstance(selection, dict) or raw != canonical_bytes(selection) + b"\n":
        raise IntegrityError("verified DBN catalog is not canonical JSON")
    from .foundation.snapshot import PublishedDbnRelease

    snapshot = PublishedDbnRelease.open(args.source_dbn_manifest, boundary=boundary)
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={
            "catalog_sha256": sha256_file(catalog),
            "maximum_total_bytes": str(args.maximum_total_bytes),
            "source_dbn_release_id": snapshot.source_release_id,
        },
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )
    receipt = publish_anomaly_materialization_acceptance(
        selection,
        snapshot=snapshot,
        publisher=publisher,
        maximum_total_bytes=args.maximum_total_bytes,
    )
    print(canonical_bytes(receipt.as_dict()).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
