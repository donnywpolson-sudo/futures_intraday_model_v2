"""Hash-bound, copy-only publication of the verified eight-market successor."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    assert_plain_file,
    canonical_bytes,
    is_linklike,
    sha256_file,
    sha256_json,
)
from .data_layout import (
    DataFileEntry,
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
    verify_data_release_manifest,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .successor_inventory import build_inventory


PLAN_SCHEMA = "eight_market_successor_migration_plan/1.0.0"
APPROVAL_SCHEMA = "eight_market_successor_migration_approval/1.0.0"
RECEIPT_SCHEMA = "eight_market_successor_migration_receipt/1.0.0"
OPERATION = "COPY_EIGHT_MARKET_DBN_SUCCESSOR_AND_ADMIT_41_MARKET_UNIVERSE"
IMPLEMENTATION_PATHS = (
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/data_layout.py",
    "src/futures_rebuild/successor_inventory.py",
    "src/futures_rebuild/successor_migration.py",
)
_HASH = re.compile(r"^[0-9a-f]{64}$")


class SuccessorMigrationError(ContractError):
    """The successor plan, approval, or publication failed closed."""


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return canonical_bytes(dict(payload)) + b"\n"


def _load_json(path: Path, *, canonical: bool = True) -> dict[str, Any]:
    assert_plain_file(path)
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise SuccessorMigrationError(f"invalid UTF-8 JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise SuccessorMigrationError(f"JSON root is not an object: {path}")
    if canonical and raw != _canonical_json(payload):
        raise SuccessorMigrationError(f"JSON is not canonical: {path}")
    return payload


def _write_new(path: Path, payload: Mapping[str, Any]) -> None:
    if path.exists():
        raise SuccessorMigrationError(f"refusing to overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _canonical_json(payload)
    descriptor, name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _replace_exact(
    path: Path, *, expected_sha256: str, payload: Mapping[str, Any]
) -> None:
    if sha256_file(path) != expected_sha256:
        raise IntegrityError(f"refusing to replace changed contract: {path}")
    encoded = _canonical_json(payload)
    descriptor, name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def implementation_manifest(root: Path) -> dict[str, str]:
    return {
        relative: sha256_file(root / relative)
        for relative in IMPLEMENTATION_PATHS
    }


def _inventory_entries(
    inventory: Mapping[str, Any],
) -> tuple[DataFileEntry, ...]:
    records = inventory.get("records")
    if not isinstance(records, list):
        raise SuccessorMigrationError("candidate inventory records are missing")
    entries: list[DataFileEntry] = []
    for record in records:
        if not isinstance(record, dict):
            raise SuccessorMigrationError("candidate inventory record is invalid")
        try:
            entries.extend(
                (
                    DataFileEntry(
                        str(record["destination_path"]),
                        int(record["dbn_bytes"]),
                        str(record["dbn_sha256"]),
                    ),
                    DataFileEntry(
                        str(record["sidecar_path"]),
                        int(record["sidecar_bytes"]),
                        str(record["sidecar_sha256"]),
                    ),
                )
            )
        except (KeyError, TypeError, ValueError, ContractError) as exc:
            raise SuccessorMigrationError(
                "candidate inventory record cannot become a release entry"
            ) from exc
    result = tuple(sorted(entries))
    if len(result) != 942 or len({item.logical_path for item in result}) != 942:
        raise SuccessorMigrationError("candidate entry closure is not exactly 942 files")
    return result


def _contract_templates(
    source_contract: Mapping[str, Any],
    universe_contract: Mapping[str, Any],
) -> dict[str, Any]:
    source_template = json.loads(json.dumps(source_contract))
    source_template["contract_version"] = "2.1.0"
    source_template["legacy_repository"] = None
    source_template["external_repository_access"] = "FORBIDDEN"
    source_template["canonical_dbn_release"] = {
        "phase": "dbn",
        "release_id": "$SUCCESSOR_RELEASE_ID",
        "release_kind": "futures_phase1a_verified_dbn",
        "schema_version": "1.0.0",
        "manifest_path": "$SUCCESSOR_MANIFEST_PATH",
        "manifest_sha256": "$SUCCESSOR_MANIFEST_SHA256",
        "dbn_files": 4491,
        "sidecar_files": 4491,
        "combined_files": 8982,
        "combined_bytes": 25592717852,
    }
    source_template["vault_expectations"] = {
        "dbn_files": 4491,
        "sidecar_files": 4491,
        "combined_files": 8982,
        "combined_bytes": 25592717852,
    }
    universe_template = json.loads(json.dumps(universe_contract))
    universe_template["status"] = "APPROVED"
    universe_template["approval_receipt_id"] = "$MIGRATION_APPROVAL_RECEIPT_ID"
    return {
        "source_contract_template_sha256": sha256_json(source_template),
        "research_universe_template_sha256": sha256_json(universe_template),
    }


def build_plan(
    *,
    repository_root: Path,
    candidate_contract_path: Path,
    inventory_path: Path,
    parent_manifest_path: Path,
    verify_parent_files: bool = True,
) -> dict[str, Any]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(active_root=root)
    candidate_contract_path = candidate_contract_path.resolve(strict=True)
    inventory_path = inventory_path.resolve(strict=True)
    parent_manifest_path = parent_manifest_path.resolve(strict=True)
    for path, subtree in (
        (candidate_contract_path, "configs"),
        (inventory_path, "manifests/migration_candidates"),
        (parent_manifest_path, "manifests/data_releases/dbn"),
    ):
        boundary.assert_active_path(path, purpose="successor plan input", subtree=subtree)

    stored_inventory = _load_json(inventory_path)
    contract_reference = stored_inventory.get("contract_path")
    if (
        not isinstance(contract_reference, str)
        or (root / contract_reference).resolve(strict=True) != candidate_contract_path
    ):
        raise IntegrityError("stored inventory points to another candidate contract")
    live_inventory = build_inventory(
        candidate_contract_path, contract_reference=contract_reference
    )
    if stored_inventory != live_inventory:
        raise IntegrityError("stored candidate inventory differs from live verified sources")
    candidate_entries = _inventory_entries(live_inventory)

    parent = verify_data_release_manifest(
        parent_manifest_path, boundary, verify_files=verify_parent_files
    )
    if (
        parent.release_id != live_inventory.get("parent_release_id")
        or parent.phase != "dbn"
        or len(parent.files) != 8040
        or sum(item.size for item in parent.files) != 25007876004
    ):
        raise IntegrityError("parent DBN release differs from the frozen baseline")
    parent_paths = {item.logical_path for item in parent.files}
    if parent_paths.intersection(item.logical_path for item in candidate_entries):
        raise IntegrityError("candidate files overlap the immutable parent release")

    mappings = [
        {
            "destination_path": entry.logical_path,
            "sha256": entry.sha256,
            "size": entry.size,
        }
        for entry in candidate_entries
    ]
    source_contract_path = root / "configs" / "source_contract.json"
    universe_contract_path = root / "configs" / "research_universe_contract.json"
    source_contract = _load_json(source_contract_path, canonical=False)
    universe_contract = _load_json(universe_contract_path, canonical=False)
    implementation = implementation_manifest(root)
    scope = {
        "candidate_contract_path": candidate_contract_path.relative_to(root).as_posix(),
        "candidate_contract_sha256": sha256_file(candidate_contract_path),
        "inventory_path": inventory_path.relative_to(root).as_posix(),
        "inventory_file_sha256": sha256_file(inventory_path),
        "inventory_id": live_inventory["inventory_id"],
        "parent_manifest_path": parent_manifest_path.relative_to(root).as_posix(),
        "parent_manifest_sha256": sha256_file(parent_manifest_path),
        "parent_release_id": parent.release_id,
        "candidate_mapping_sha256": sha256_json(mappings),
        "candidate_files": 942,
        "candidate_bytes": 584841848,
        "successor_files": 8982,
        "successor_bytes": 25592717852,
        "destination_root": "data/dbn",
        "staging_root": "state/data_publication_staging",
        "manifest_root": "manifests/data_releases/dbn",
        "copy_mode": "COPY_ONLY_NO_OVERWRITE_NO_LINKS",
        "rollback_boundary": "UNPUBLISHED_STAGING_ONLY",
        "excluded_paths": list(live_inventory["excluded_relative_paths"]),
        "source_contract_sha256": sha256_file(source_contract_path),
        "research_universe_contract_sha256": sha256_file(universe_contract_path),
        **_contract_templates(source_contract, universe_contract),
        "implementation_manifest": implementation,
        "implementation_sha256": sha256_json(implementation),
        "provider_calls_authorized": False,
        "legacy_mutation_authorized": False,
    }
    core = {
        "schema_version": PLAN_SCHEMA,
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "operation": OPERATION,
        "scope": scope,
        "execution_authorized": False,
    }
    return {**core, "plan_id": sha256_json(core)}


def approval_draft(plan: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": APPROVAL_SCHEMA,
        "status": "PENDING_APPROVAL",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": None,
        "approved_at": None,
        "user_authorization_id": None,
        "approval_receipt_id": None,
    }


def approval_payload(
    *,
    plan: Mapping[str, Any],
    plan_sha256: str,
    approved_at: str,
    user_authorization_id: str,
) -> dict[str, Any]:
    core = {
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "approved_at": approved_at,
        "user_authorization_id": user_authorization_id,
    }
    return {**core, "approval_receipt_id": sha256_json(core)}


def verify_approval(
    approval: Mapping[str, Any],
    *,
    plan: Mapping[str, Any],
    plan_sha256: str,
) -> str:
    approved_at = approval.get("approved_at")
    user_id = approval.get("user_authorization_id")
    core = {
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "operation": OPERATION,
        "plan_id": plan.get("plan_id"),
        "plan_sha256": plan_sha256,
        "approved_at": approved_at,
        "user_authorization_id": user_id,
    }
    if (
        dict(approval)
        != {**core, "approval_receipt_id": approval.get("approval_receipt_id")}
        or not isinstance(approved_at, str)
        or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", approved_at)
        is None
        or not isinstance(user_id, str)
        or _HASH.fullmatch(user_id) is None
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise UnauthorizedOperation("successor migration lacks exact hash-bound approval")
    return str(approval["approval_receipt_id"])


def _successor_manifest(
    *,
    parent: DataReleaseManifest,
    inventory: Mapping[str, Any],
    approval_id: str,
) -> DataReleaseManifest:
    candidate_entries = _inventory_entries(inventory)
    files = tuple(sorted((*parent.files, *candidate_entries)))
    embedded = {
        "phase1a_receipt": {
            "approval_receipt_id": approval_id,
            "parent_release_id": parent.release_id,
            "source_inventory_id": inventory["inventory_id"],
            "status": "COMPLETE_VERIFIED_IMMUTABLE_SUCCESSOR",
            "total_bytes": 25592717852,
            "total_files": 8982,
        }
    }
    metadata = {
        "approval_receipt_id": approval_id,
        "parent_release_id": parent.release_id,
        "source_inventory_id": inventory["inventory_id"],
    }
    provisional = {
        "embedded_documents": embedded,
        "files": [item.as_dict() for item in files],
        "layout_version": "2.0.0",
        "manifest_version": "2.0.0",
        "metadata": metadata,
        "phase": "dbn",
        "release_kind": "futures_phase1a_verified_dbn",
        "schema_version": "1.0.0",
        "source_release_ids": [parent.release_id],
    }
    return DataReleaseManifest(
        release_id=sha256_json(provisional),
        phase="dbn",
        release_kind="futures_phase1a_verified_dbn",
        schema_version="1.0.0",
        source_release_ids=(parent.release_id,),
        files=files,
        embedded_documents=embedded,
        metadata=metadata,
    )


def _updated_contracts(
    *,
    root: Path,
    successor: DataReleaseReceipt,
    manifest: DataReleaseManifest,
    approval_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    source_path = root / "configs" / "source_contract.json"
    universe_path = root / "configs" / "research_universe_contract.json"
    source = _load_json(source_path, canonical=False)
    universe = _load_json(universe_path, canonical=False)
    source["contract_version"] = "2.1.0"
    source["legacy_repository"] = None
    source["external_repository_access"] = "FORBIDDEN"
    source["canonical_dbn_release"] = {
        "phase": successor.phase,
        "release_id": successor.release_id,
        "release_kind": successor.release_kind,
        "schema_version": successor.schema_version,
        "manifest_path": successor.manifest_path,
        "manifest_sha256": successor.manifest_sha256,
        "dbn_files": 4491,
        "sidecar_files": 4491,
        "combined_files": 8982,
        "combined_bytes": 25592717852,
    }
    source["vault_expectations"] = {
        "dbn_files": 4491,
        "sidecar_files": 4491,
        "combined_files": 8982,
        "combined_bytes": 25592717852,
    }
    dbn_counts: dict[str, int] = {}
    for entry in manifest.files:
        if entry.logical_path.endswith(".dbn.zst"):
            family = Path(entry.logical_path).parts[2]
            dbn_counts[family] = dbn_counts.get(family, 0) + 1
    for family in source.get("source_families", []):
        if isinstance(family, dict) and str(family.get("id", "")).startswith("dbn_"):
            family_name = str(family["id"]).removeprefix("dbn_")
            family["expected_dbn_files"] = dbn_counts.get(family_name, 0)
    universe["status"] = "APPROVED"
    universe["approval_receipt_id"] = approval_id
    return source, universe


def execute(
    *,
    repository_root: Path,
    plan_path: Path,
    approval_path: Path,
) -> dict[str, Any]:
    root = repository_root.resolve(strict=True)
    legacy = Path(r"C:\Users\example\Desktop\futures_intraday_model")
    boundary = RepoBoundary(active_root=root, legacy_roots=(legacy,))
    plan_path = plan_path.resolve(strict=True)
    approval_path = approval_path.resolve(strict=True)
    plan = _load_json(plan_path)
    approval = _load_json(approval_path)
    approval_id = verify_approval(
        approval, plan=plan, plan_sha256=sha256_file(plan_path)
    )
    scope = plan.get("scope")
    if (
        not isinstance(scope, dict)
        or plan.get("schema_version") != PLAN_SCHEMA
        or plan.get("operation") != OPERATION
        or plan.get("execution_authorized") is not False
    ):
        raise UnauthorizedOperation("successor migration plan is invalid")

    live_plan = build_plan(
        repository_root=root,
        candidate_contract_path=root / str(scope["candidate_contract_path"]),
        inventory_path=root / str(scope["inventory_path"]),
        parent_manifest_path=root / str(scope["parent_manifest_path"]),
        verify_parent_files=True,
    )
    if live_plan != plan:
        raise UnauthorizedOperation("successor migration plan differs from live inputs")
    inventory = _load_json(root / str(scope["inventory_path"]))
    parent = verify_data_release_manifest(
        root / str(scope["parent_manifest_path"]), boundary, verify_files=True
    )
    manifest = _successor_manifest(
        parent=parent, inventory=inventory, approval_id=approval_id
    )
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={
            "approval_receipt_id": approval_id,
            "parent_release_id": parent.release_id,
            "successor_release_id": manifest.release_id,
        },
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=root / "state" / "locks" / "data-publication.lock",
    )
    stage = publisher.create_stage("eight_market_successor")
    source_root = boundary.assert_legacy_read_root(legacy)
    staged_paths: dict[str, str] = {
        entry.logical_path: str(
            Path(entry.logical_path).relative_to(Path("data") / "dbn")
        ).replace("\\", "/")
        for entry in manifest.files
    }
    records = inventory["records"]
    assert isinstance(records, list)
    for record in records:
        assert isinstance(record, dict)
        for source_key, destination_key, size_key, hash_key in (
            ("source_path", "destination_path", "dbn_bytes", "dbn_sha256"),
            ("sidecar_path", "sidecar_path", "sidecar_bytes", "sidecar_sha256"),
        ):
            source = source_root / str(record[source_key])
            logical = str(record[destination_key])
            relative = staged_paths[logical]
            target = stage / relative
            if is_linklike(source):
                raise IntegrityError("candidate source is link-like")
            assert_plain_file(source, reject_hardlinks=False)
            destination = root / logical
            if destination.exists():
                raise IntegrityError("candidate destination already exists")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            if (
                target.stat().st_size != record[size_key]
                or sha256_file(target) != record[hash_key]
            ):
                raise IntegrityError("staged candidate differs from verified source")

    manifest_path = publisher.publish(
        stage, manifest, staged_paths=staged_paths
    )
    successor = DataReleaseReceipt.from_manifest(manifest_path, boundary)
    source, universe = _updated_contracts(
        root=root,
        successor=successor,
        manifest=manifest,
        approval_id=approval_id,
    )
    _replace_exact(
        root / "configs" / "source_contract.json",
        expected_sha256=str(scope["source_contract_sha256"]),
        payload=source,
    )
    _replace_exact(
        root / "configs" / "research_universe_contract.json",
        expected_sha256=str(scope["research_universe_contract_sha256"]),
        payload=universe,
    )
    receipt_core = {
        "schema_version": RECEIPT_SCHEMA,
        "status": "COMPLETE_VERIFIED_IMMUTABLE_SUCCESSOR",
        "approval_receipt_id": approval_id,
        "plan_id": plan["plan_id"],
        "inventory_id": inventory["inventory_id"],
        "parent_release_id": parent.release_id,
        "successor_release_id": successor.release_id,
        "successor_manifest_path": successor.manifest_path,
        "successor_manifest_sha256": successor.manifest_sha256,
        "source_contract_sha256": sha256_file(root / "configs" / "source_contract.json"),
        "research_universe_contract_sha256": sha256_file(
            root / "configs" / "research_universe_contract.json"
        ),
        "combined_files": 8982,
        "combined_bytes": 25592717852,
        "provider_calls": 0,
        "legacy_mutations": 0,
    }
    receipt = {**receipt_core, "receipt_id": sha256_json(receipt_core)}
    receipt_path = (
        root
        / "manifests"
        / "migration_receipts"
        / f"eight_market_successor_{successor.release_id}.json"
    )
    _write_new(receipt_path, receipt)
    return receipt


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan_parser = subparsers.add_parser("plan")
    plan_parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    plan_parser.add_argument(
        "--candidate-contract",
        type=Path,
        default=Path("configs/eight_market_successor_candidate.json"),
    )
    plan_parser.add_argument(
        "--inventory",
        type=Path,
        default=Path(
            "manifests/migration_candidates/eight_market_successor_inventory.json"
        ),
    )
    plan_parser.add_argument(
        "--parent-manifest",
        type=Path,
        default=Path(
            "manifests/data_releases/dbn/"
            "9e5a9f2a405e50b0cda6702b67506b0951b057500781d37c45171da3967e9b51.json"
        ),
    )
    plan_parser.add_argument(
        "--plan-output",
        type=Path,
        default=Path("configs/eight_market_successor_migration_plan.json"),
    )
    plan_parser.add_argument(
        "--approval-output",
        type=Path,
        default=Path("configs/eight_market_successor_migration_approval.json"),
    )
    execute_parser = subparsers.add_parser("execute")
    execute_parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    execute_parser.add_argument(
        "--plan",
        type=Path,
        default=Path("configs/eight_market_successor_migration_plan.json"),
    )
    execute_parser.add_argument(
        "--approval",
        type=Path,
        default=Path("configs/eight_market_successor_migration_approval.json"),
    )
    args = parser.parse_args(argv)
    try:
        if args.command == "plan":
            root = args.repository_root.resolve(strict=True)
            plan = build_plan(
                repository_root=root,
                candidate_contract_path=(root / args.candidate_contract).resolve(),
                inventory_path=(root / args.inventory).resolve(),
                parent_manifest_path=(root / args.parent_manifest).resolve(),
            )
            _write_new(root / args.plan_output, plan)
            _write_new(root / args.approval_output, approval_draft(plan))
            print(
                json.dumps(
                    {
                        "status": "PENDING_APPROVAL",
                        "plan_id": plan["plan_id"],
                        "execution_authorized": False,
                    },
                    sort_keys=True,
                )
            )
        else:
            receipt = execute(
                repository_root=args.repository_root,
                plan_path=args.plan,
                approval_path=args.approval,
            )
            print(json.dumps(receipt, sort_keys=True))
        return 0
    except (
        SuccessorMigrationError,
        ContractError,
        IntegrityError,
        UnauthorizedOperation,
        OSError,
    ) as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}, sort_keys=True))
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
