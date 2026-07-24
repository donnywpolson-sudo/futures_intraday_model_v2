"""Exact, resumable, copy-only migration planning and execution.

Planning is the default and writes nothing. Copy mode requires a manifest
authorization flag plus the exact reviewed manifest and source-inventory hashes.
"""

from __future__ import annotations

import argparse
import json
import ntpath
import os
import re
import shutil
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Mapping

from .boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from .canonical import (
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    is_linklike,
    sha256_bytes,
    sha256_file,
    sha256_json,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .locking import MINIMUM_STALE_AGE, FileLease

CHECKPOINT_BATCH_FILES = 25
SNAPSHOT_RECEIPT_VERSION = "1.0.0"
SNAPSHOT_RECEIPT_STATUS = "COMPLETE_VERIFIED_IMMUTABLE"
MIGRATION_APPROVAL_SCOPE = "CONTROLLED_REBUILD_HASH_COPY_TO_IMMUTABLE_SOURCE_SNAPSHOT"
AUTHORIZED_MIGRATION_MANIFEST_RELATIVE = Path(
    "configs/migration_manifest_authorized.json"
)
AUTHORIZED_MIGRATION_APPROVAL_RELATIVE = Path(
    "configs/migration_approval_authorized.json"
)
AUTHORIZED_MIGRATION_MANIFEST_SHA256 = (
    "57bcac635731c0bf85a0c0cad5c810cf9173b68813d501b0a94ce15a6ddacda7"
)
AUTHORIZED_MIGRATION_SOURCE_SCOPE_SHA256 = (
    "6f9c47cb65e9b2198163de8c9f069b25f8b07ca276d9fb07cccdee9dc0c371cb"
)
MIGRATION_APPROVAL_ARTIFACT_VERSION = "1.0.0"
MIGRATION_APPROVAL_PENDING = "PENDING_DETAILED_INVENTORY_REVIEW"
MIGRATION_APPROVAL_COMPLETE = "APPROVED_HASH_COPY"
MIGRATION_STALE_LOCK_RECOVERY_OPERATION = "RECOVER_MIGRATION_LEASE"
MIGRATION_INVENTORY_REVIEW_OPERATION = "WRITE_MIGRATION_INVENTORY_REVIEW"
MIGRATION_APPROVAL_DRAFT_OPERATION = "WRITE_MIGRATION_APPROVAL_DRAFT"
MIGRATION_REVIEW_ARTIFACT_VERSION = "1.0.0"
MIGRATION_INVENTORY_REVIEW_STATUS = "NON_AUTHORIZING_DETAILED_INVENTORY_REVIEW"
MIGRATION_APPROVAL_DRAFT_STATUS = "NON_AUTHORIZING_APPROVAL_PAYLOAD_DRAFT"
CONTROLLED_REBUILD_AUTHORIZATION_ID = (
    "4977eb04c13f92045a3c020b9a8c9f691e21df583a81d8cc5040a13104cb8793"
)
MIGRATION_IMPLEMENTATION_PATHS = (
    "src/futures_rebuild/migration.py",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/locking.py",
    "configs/controlled_rebuild_authorization.json",
    "pyproject.toml",
    "requirements.lock",
    "requirements.sha256.lock",
)
SNAPSHOT_RECEIPT_FIELDS = {
    "approval_id",
    "files",
    "files_index_sha256",
    "inventory_sha256",
    "manifest_sha256",
    "migration_implementation_sha256",
    "receipt_version",
    "source_snapshot_id",
    "status",
    "total_bytes",
    "total_files",
    "user_authorization_id",
}


def migration_implementation_manifest() -> dict[str, str]:
    root = Path(__file__).resolve().parents[2]
    result: dict[str, str] = {}
    for relative in MIGRATION_IMPLEMENTATION_PATHS:
        path = root / Path(relative)
        result[relative] = sha256_file(path)
    return dict(sorted(result.items()))


def migration_implementation_sha256() -> str:
    return sha256_json(migration_implementation_manifest())


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def migration_source_scope(manifest: Mapping[str, object]) -> dict[str, object]:
    required = ("entries", "policy", "source_root")
    if any(name not in manifest for name in required):
        raise ContractError("migration manifest lacks its pinned source scope")
    return {
        name: manifest[name]
        for name in ("authoritative_group", "entries", "policy", "source_root")
        if name in manifest
    }


def _validate_authorized_repository_manifest(
    manifest: dict[str, object], manifest_hash: str
) -> None:
    path = _repository_root() / AUTHORIZED_MIGRATION_MANIFEST_RELATIVE
    assert_plain_file(path)
    try:
        repository_payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise UnauthorizedOperation(
            "repository-authorized migration manifest is invalid"
        ) from exc
    if (
        not isinstance(repository_payload, dict)
        or sha256_json(repository_payload) != AUTHORIZED_MIGRATION_MANIFEST_SHA256
        or manifest_hash != AUTHORIZED_MIGRATION_MANIFEST_SHA256
        or repository_payload != manifest
        or manifest.get("copy_authorized") is not True
        or sha256_json(migration_source_scope(manifest))
        != AUTHORIZED_MIGRATION_SOURCE_SCOPE_SHA256
    ):
        raise UnauthorizedOperation(
            "copy execution is not the exact repository-authorized manifest/scope"
        )


def _validate_controlled_rebuild_authorization() -> str:
    root = Path(__file__).resolve().parents[2]
    path = root / "configs" / "controlled_rebuild_authorization.json"
    assert_plain_file(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise UnauthorizedOperation("controlled rebuild authorization is invalid") from exc
    expected = {
        "authorization_version",
        "authorization_source",
        "task_thread_id",
        "authorization_text",
        "data_reuse_policy",
        "hard_pauses",
        "project",
        "legacy_root",
        "active_root",
        "allowed_actions",
        "authorization_id",
    }
    if not isinstance(payload, dict) or set(payload) != expected:
        raise UnauthorizedOperation("controlled rebuild authorization fields differ")
    authorization_id = payload.pop("authorization_id")
    if (
        authorization_id != sha256_json(payload)
        or authorization_id != CONTROLLED_REBUILD_AUTHORIZATION_ID
        or payload["authorization_version"] != "1.0.0"
        or payload["authorization_source"] != "CODEX_TASK_USER_MESSAGE"
        or payload["task_thread_id"] != "019f6329-fb5d-7b53-8db6-93ab519f4da8"
        or payload["project"] != "futures_intraday_model_v2"
        or Path(str(payload["active_root"])).resolve(strict=True) != root
        or "hash_copy_approved_legacy_data" not in set(payload["allowed_actions"])
        or payload["data_reuse_policy"]
        != {
            "blanket_redownload_allowed": False,
            "copy_mode": "HASH_VERIFIED_COPY_NOT_MOVE",
            "links_allowed": False,
            "legacy_bytes_remain_unchanged": True,
        }
        or not {
            "candidate_sealing",
            "legacy_repository_write",
            "paid_databento_download",
            "real_history_hypothesis_or_wfa_execution",
        }.issubset(set(payload["hard_pauses"]))
    ):
        raise UnauthorizedOperation("controlled rebuild authorization does not match this task")
    return str(authorization_id)


@dataclass(frozen=True)
class MigrationApproval:
    schema_version: int
    approval_scope: str
    manifest_sha256: str
    inventory_sha256: str
    migration_implementation_manifest: Mapping[str, str]
    migration_implementation_sha256: str
    user_authorization_id: str
    total_files: int
    total_bytes: int
    approved_at: str
    approval_id: str

    def unsigned_dict(self) -> dict[str, object]:
        value = asdict(self)
        value.pop("approval_id")
        return value

    def validate(
        self,
        *,
        manifest_hash: str,
        source_inventory: Mapping[str, object],
    ) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise UnauthorizedOperation("migration approval schema is invalid")
        if self.approval_scope != MIGRATION_APPROVAL_SCOPE:
            raise UnauthorizedOperation("migration approval scope is invalid")
        try:
            approved = datetime.fromisoformat(self.approved_at.replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise UnauthorizedOperation("migration approval time is invalid") from exc
        if approved.tzinfo is None or approved.utcoffset() != timedelta(0):
            raise UnauthorizedOperation("migration approval time must be UTC")
        expected_manifest = migration_implementation_manifest()
        expected = {
            "manifest_sha256": manifest_hash,
            "inventory_sha256": str(source_inventory["inventory_sha256"]),
            "migration_implementation_manifest": expected_manifest,
            "migration_implementation_sha256": sha256_json(expected_manifest),
            "user_authorization_id": _validate_controlled_rebuild_authorization(),
            "total_files": source_inventory["total_files"],
            "total_bytes": source_inventory["total_bytes"],
        }
        for name, value in expected.items():
            if getattr(self, name) != value:
                raise UnauthorizedOperation(f"migration approval does not bind current {name}")
        if self.approval_id != sha256_json(self.unsigned_dict()):
            raise UnauthorizedOperation("migration approval ID is invalid")

    @classmethod
    def from_dict(cls, payload: object) -> "MigrationApproval":
        if not isinstance(payload, dict) or set(payload) != set(cls.__dataclass_fields__):
            raise UnauthorizedOperation("migration approval fields differ")
        if (
            type(payload["schema_version"]) is not int
            or type(payload["total_files"]) is not int
            or type(payload["total_bytes"]) is not int
            or not isinstance(payload["migration_implementation_manifest"], dict)
        ):
            raise UnauthorizedOperation("migration approval JSON types are invalid")
        result = cls(
            schema_version=payload["schema_version"],
            approval_scope=str(payload["approval_scope"]),
            manifest_sha256=str(payload["manifest_sha256"]),
            inventory_sha256=str(payload["inventory_sha256"]),
            migration_implementation_manifest={
                str(key): str(value)
                for key, value in payload["migration_implementation_manifest"].items()
            },
            migration_implementation_sha256=str(payload["migration_implementation_sha256"]),
            user_authorization_id=str(payload["user_authorization_id"]),
            total_files=payload["total_files"],
            total_bytes=payload["total_bytes"],
            approved_at=str(payload["approved_at"]),
            approval_id=str(payload["approval_id"]),
        )
        return result


def approval_payload_for_review(
    *,
    manifest_hash: str,
    source_inventory: Mapping[str, object],
    approved_at: str,
) -> dict[str, object]:
    implementation = migration_implementation_manifest()
    unsigned: dict[str, object] = {
        "schema_version": 1,
        "approval_scope": MIGRATION_APPROVAL_SCOPE,
        "manifest_sha256": manifest_hash,
        "inventory_sha256": str(source_inventory["inventory_sha256"]),
        "migration_implementation_manifest": implementation,
        "migration_implementation_sha256": sha256_json(implementation),
        "user_authorization_id": _validate_controlled_rebuild_authorization(),
        "total_files": source_inventory["total_files"],
        "total_bytes": source_inventory["total_bytes"],
        "approved_at": approved_at,
    }
    candidate = MigrationApproval.from_dict(
        {**unsigned, "approval_id": sha256_json(unsigned)}
    )
    candidate.validate(manifest_hash=manifest_hash, source_inventory=source_inventory)
    return {**unsigned, "approval_id": candidate.approval_id}


def load_migration_approval(
    path: Path,
    *,
    manifest_hash: str,
    source_inventory: Mapping[str, object],
) -> MigrationApproval:
    assert_plain_file(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise UnauthorizedOperation("migration approval is invalid") from exc
    approval = MigrationApproval.from_dict(payload)
    approval.validate(manifest_hash=manifest_hash, source_inventory=source_inventory)
    return approval


def read_migration_approval(path: Path) -> MigrationApproval:
    """Parse exact approval fields; guarded_copy validates them after one inventory scan."""

    assert_plain_file(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise UnauthorizedOperation("migration approval is invalid") from exc
    return MigrationApproval.from_dict(payload)


_APPROVAL_ARTIFACT_FIELDS = {
    "approval",
    "artifact_id",
    "artifact_version",
    "authorized_manifest_path",
    "authorized_manifest_sha256",
    "authorized_source_scope_sha256",
    "controlled_rebuild_authorization_id",
    "execution_authorized",
    "required_next_step",
    "status",
}


def _approval_artifact_core(
    *,
    status: str,
    execution_authorized: bool,
    approval: dict[str, object] | None,
    required_next_step: str | None,
) -> dict[str, object]:
    return {
        "approval": approval,
        "artifact_version": MIGRATION_APPROVAL_ARTIFACT_VERSION,
        "authorized_manifest_path": AUTHORIZED_MIGRATION_MANIFEST_RELATIVE.as_posix(),
        "authorized_manifest_sha256": AUTHORIZED_MIGRATION_MANIFEST_SHA256,
        "authorized_source_scope_sha256": AUTHORIZED_MIGRATION_SOURCE_SCOPE_SHA256,
        "controlled_rebuild_authorization_id": CONTROLLED_REBUILD_AUTHORIZATION_ID,
        "execution_authorized": execution_authorized,
        "required_next_step": required_next_step,
        "status": status,
    }


def approval_artifact_payload_for_review(
    approval: MigrationApproval,
) -> dict[str, object]:
    if not isinstance(approval, MigrationApproval):
        raise ContractError("checked-in approval artifact requires an exact approval")
    core = _approval_artifact_core(
        status=MIGRATION_APPROVAL_COMPLETE,
        execution_authorized=True,
        approval=asdict(approval),
        required_next_step=None,
    )
    return {**core, "artifact_id": sha256_json(core)}


def _load_checked_in_migration_approval() -> MigrationApproval:
    path = _repository_root() / AUTHORIZED_MIGRATION_APPROVAL_RELATIVE
    assert_plain_file(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise UnauthorizedOperation(
            "checked-in migration approval artifact is invalid"
        ) from exc
    if not isinstance(payload, dict) or set(payload) != _APPROVAL_ARTIFACT_FIELDS:
        raise UnauthorizedOperation(
            "checked-in migration approval artifact fields differ"
        )
    artifact_id = payload.get("artifact_id")
    core = {key: value for key, value in payload.items() if key != "artifact_id"}
    if (
        artifact_id != sha256_json(core)
        or payload.get("artifact_version") != MIGRATION_APPROVAL_ARTIFACT_VERSION
        or payload.get("authorized_manifest_path")
        != AUTHORIZED_MIGRATION_MANIFEST_RELATIVE.as_posix()
        or payload.get("authorized_manifest_sha256")
        != AUTHORIZED_MIGRATION_MANIFEST_SHA256
        or payload.get("authorized_source_scope_sha256")
        != AUTHORIZED_MIGRATION_SOURCE_SCOPE_SHA256
        or payload.get("controlled_rebuild_authorization_id")
        != CONTROLLED_REBUILD_AUTHORIZATION_ID
    ):
        raise UnauthorizedOperation(
            "checked-in migration approval artifact identity is invalid"
        )
    if payload.get("status") == MIGRATION_APPROVAL_PENDING:
        if (
            payload.get("execution_authorized") is not False
            or payload.get("approval") is not None
            or type(payload.get("required_next_step")) is not str
            or not payload["required_next_step"]
        ):
            raise UnauthorizedOperation("pending migration approval is malformed")
        raise UnauthorizedOperation(
            "migration approval remains pending detailed inventory review"
        )
    if (
        payload.get("status") != MIGRATION_APPROVAL_COMPLETE
        or payload.get("execution_authorized") is not True
        or payload.get("required_next_step") is not None
    ):
        raise UnauthorizedOperation(
            "checked-in migration approval does not authorize hash-copy"
        )
    approval = MigrationApproval.from_dict(payload.get("approval"))
    if (
        approval.migration_implementation_sha256
        != migration_implementation_sha256()
    ):
        raise UnauthorizedOperation(
            "checked-in migration approval is historical and cannot authorize "
            "the current implementation"
        )
    return approval


def migration_authorization_scope(
    manifest: dict[str, object],
    manifest_hash: str,
    inventory_hash: str,
    approval: MigrationApproval,
) -> dict[str, str]:
    return {
        "approval_id": approval.approval_id,
        "destination_root": str(Path(str(manifest["destination_root"])).resolve(strict=False)),
        "implementation_sha256": approval.migration_implementation_sha256,
        "inventory_sha256": inventory_hash,
        "manifest_sha256": manifest_hash,
        "migration_id": str(manifest["migration_id"]),
        "source_root": str(Path(str(manifest["source_root"])).resolve(strict=False)),
        "user_authorization_id": approval.user_authorization_id,
    }


def migration_recovery_scope(
    manifest: dict[str, object],
    manifest_hash: str,
    inventory_hash: str,
    approval: MigrationApproval,
    expected_token: str,
) -> dict[str, str]:
    if (
        type(expected_token) is not str
        or re.fullmatch(r"[0-9a-f]{32}", expected_token) is None
    ):
        raise ContractError("stale-lock recovery requires an exact reviewed token")
    return {
        **migration_authorization_scope(
            manifest, manifest_hash, inventory_hash, approval
        ),
        "stale_lock_token": expected_token,
    }


def migration_inventory_review_scope(
    manifest: dict[str, object], manifest_hash: str, output_path: Path
) -> dict[str, str]:
    return {
        "manifest_sha256": manifest_hash,
        "migration_id": str(manifest["migration_id"]),
        "output_path": str(output_path.resolve(strict=False)),
        "source_root": str(
            Path(str(manifest["source_root"])).resolve(strict=False)
        ),
    }


def migration_approval_draft_scope(
    manifest: dict[str, object],
    manifest_hash: str,
    inventory_review_artifact_id: str,
    approved_at: str,
    output_path: Path,
) -> dict[str, str]:
    return {
        "approved_at": approved_at,
        "inventory_review_artifact_id": inventory_review_artifact_id,
        "manifest_sha256": manifest_hash,
        "migration_id": str(manifest["migration_id"]),
        "output_path": str(output_path.resolve(strict=False)),
    }

def load_manifest(path: Path) -> tuple[dict[str, object], str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ContractError(f"cannot load migration manifest: {path}") from exc
    required = {
        "migration_id",
        "source_root",
        "destination_root",
        "copy_authorized",
        "policy",
        "entries",
    }
    if not isinstance(payload, dict) or not required.issubset(payload):
        raise ContractError("migration manifest is missing required fields")
    if type(payload["copy_authorized"]) is not bool:
        raise ContractError("copy_authorized must be an exact boolean")
    source = Path(str(payload["source_root"]))
    destination = Path(str(payload["destination_root"]))
    if not source.is_absolute() or not destination.is_absolute():
        raise ContractError("migration roots must be absolute")
    try:
        destination.resolve(strict=False).relative_to(source.resolve(strict=False))
    except ValueError:
        pass
    else:
        raise ContractError("destination cannot be inside the legacy source root")
    policy = payload["policy"]
    if (
        not isinstance(policy, dict)
        or set(policy)
        != {
            "follow_links",
            "operation",
            "overwrite",
            "require_source_stable_during_copy",
            "verify_destination_sha256",
        }
        or policy.get("operation") != "copy_only"
    ):
        raise ContractError("only copy_only migration manifests are supported")
    if (
        policy.get("overwrite") is not False
        or policy.get("follow_links") is not False
        or policy.get("require_source_stable_during_copy") is not True
        or policy.get("verify_destination_sha256") is not True
    ):
        raise ContractError("overwrite and link-following must be explicitly false")
    if not isinstance(payload["entries"], list) or not payload["entries"]:
        raise ContractError("migration entries cannot be empty")
    return payload, sha256_json(payload)


def _walk_plain_files(root: Path) -> Iterator[Path]:
    if not root.exists() or not root.is_dir() or is_linklike(root):
        raise ContractError(f"allowlisted source directory is absent or link-like: {root}")
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        for directory in list(directories):
            candidate = current_path / directory
            if is_linklike(candidate):
                raise ContractError(f"link-like directory is forbidden: {candidate}")
        for filename in sorted(files):
            candidate = current_path / filename
            if is_linklike(candidate):
                raise ContractError(f"link-like file is forbidden: {candidate}")
            yield candidate


def _compile_patterns(entry: dict[str, object]) -> tuple[re.Pattern[str], tuple[re.Pattern[str], ...]]:
    try:
        include = re.compile(str(entry.get("include_regex", r".+")))
        exclude = tuple(re.compile(str(value)) for value in entry.get("exclude_regexes", []))
    except re.error as exc:
        raise ContractError(f"invalid include/exclude regex for {entry.get('family')}") from exc
    return include, exclude


def _entry_scan(
    source_root: Path, entry: dict[str, object]
) -> tuple[list[tuple[Path, Path]], list[Path]]:
    relative = Path(str(entry["source"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise ContractError("entry source must be a contained relative path")
    source = (source_root / relative).absolute()
    _assert_no_linklike_ancestor(source)
    kind = str(entry.get("kind", "tree"))
    if kind == "file":
        if not source.exists() or not source.is_file() or is_linklike(source):
            raise ContractError(f"allowlisted evidence file is absent or link-like: {source}")
        destination = Path(str(entry["destination"]))
        if destination.is_absolute() or ".." in destination.parts:
            raise ContractError("entry destination must be a contained relative path")
        return [(source, destination)], []
    if kind != "tree":
        raise ContractError(f"unknown migration entry kind: {kind}")
    include, excludes = _compile_patterns(entry)
    destination_root = Path(str(entry["destination"]))
    if destination_root.is_absolute() or ".." in destination_root.parts:
        raise ContractError("entry destination must be a contained relative path")
    included: list[tuple[Path, Path]] = []
    excluded: list[Path] = []
    for path in _walk_plain_files(source):
        item_relative = path.relative_to(source)
        normalized = item_relative.as_posix()
        if include.fullmatch(normalized) and not any(rule.search(normalized) for rule in excludes):
            included.append((path, destination_root / item_relative))
        else:
            excluded.append(path)
    return included, excluded


_WINDOWS_RESERVED_NAMES = frozenset(
    {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{index}" for index in range(1, 10)),
        *(f"LPT{index}" for index in range(1, 10)),
    }
)
_WINDOWS_INVALID_COMPONENT = re.compile(r'[<>:"|?*\x00-\x1f]')


def _windows_destination_key(relative: Path) -> str:
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ContractError("entry destination must be a contained relative path")
    for part in relative.parts:
        if (
            not part
            or part.endswith((" ", "."))
            or _WINDOWS_INVALID_COMPONENT.search(part) is not None
            or part.split(".", 1)[0].upper() in _WINDOWS_RESERVED_NAMES
        ):
            raise ContractError(
                f"destination is unsafe under Windows path semantics: {relative.as_posix()}"
            )
    return ntpath.normcase(str(relative).replace("/", "\\"))


def _scan_manifest_entries(
    manifest: dict[str, object],
) -> tuple[
    tuple[dict[str, object], list[tuple[Path, Path]], list[Path]], ...
]:
    source_root = Path(str(manifest["source_root"]))
    plans: list[tuple[dict[str, object], list[tuple[Path, Path]], list[Path]]] = []
    seen_sources: set[Path] = set()
    seen_destinations: dict[str, str] = {}
    seen_families: set[str] = set()
    for raw_entry in manifest["entries"]:  # type: ignore[assignment]
        if not isinstance(raw_entry, dict):
            raise ContractError("migration entry must be an object")
        family = raw_entry.get("family")
        if type(family) is not str or not family or not family.isascii():
            raise ContractError("migration family must be a nonempty ASCII string")
        if family in seen_families:
            raise ContractError(f"migration family is duplicated: {family}")
        seen_families.add(family)
        if any(
            type(raw_entry.get(name)) is not str or not raw_entry[name]
            for name in ("source", "destination", "disposition")
        ):
            raise ContractError(f"migration family fields are invalid: {family}")
        included, excluded = _entry_scan(source_root, raw_entry)
        if "expected_files" in raw_entry and len(included) != int(
            raw_entry["expected_files"]
        ):
            raise IntegrityError(
                f"{family} expected {raw_entry['expected_files']} files, found {len(included)}"
            )
        if "expected_excluded_files" in raw_entry and len(excluded) != int(
            raw_entry["expected_excluded_files"]
        ):
            raise IntegrityError(
                f"{family} exclusion boundary changed: expected "
                f"{raw_entry['expected_excluded_files']}, found {len(excluded)}"
            )
        for source, destination in included:
            resolved = source.resolve(strict=True)
            if resolved in seen_sources:
                raise ContractError(
                    f"source file appears in multiple families: {resolved}"
                )
            seen_sources.add(resolved)
            normalized = _windows_destination_key(destination)
            observed = seen_destinations.get(normalized)
            destination_text = destination.as_posix()
            if normalized == _windows_destination_key(
                Path("SOURCE_SNAPSHOT_RECEIPT.json")
            ):
                raise IntegrityError(
                    "migration destination collides with the immutable receipt"
                )
            if observed is not None:
                raise IntegrityError(
                    "multiple sources target one Windows-normalized destination: "
                    f"{observed!r} and {destination_text!r}"
                )
            seen_destinations[normalized] = destination_text
        plans.append((raw_entry, included, excluded))
    return tuple(plans)


def inventory(
    manifest: dict[str, object], manifest_hash: str, *, detailed: bool = False
) -> dict[str, object]:
    """Hash all included files and enforce the frozen count/byte boundary."""

    source_root = Path(str(manifest["source_root"]))
    families: list[dict[str, object]] = []
    total_files = 0
    total_bytes = 0
    plans = _scan_manifest_entries(manifest)
    for entry, included, excluded in plans:
        records: list[dict[str, object]] = []
        for path, destination_relative in sorted(included, key=lambda pair: str(pair[0])):
            size = path.stat().st_size
            records.append(
                {
                    "destination": destination_relative.as_posix(),
                    "path": path.relative_to(source_root).as_posix(),
                    "sha256": sha256_file(path),
                    "size": size,
                }
            )
            total_files += 1
            total_bytes += size
        family_bytes = sum(int(item["size"]) for item in records)
        if "expected_bytes" in entry and family_bytes != int(entry["expected_bytes"]):
            raise IntegrityError(
                f"{entry['family']} byte boundary changed: expected "
                f"{entry['expected_bytes']}, found {family_bytes}"
            )
        if "expected_sha256" in entry:
            if len(records) != 1 or records[0]["sha256"] != str(
                entry["expected_sha256"]
            ).lower():
                raise IntegrityError(f"{entry['family']} pinned SHA-256 changed")
        family: dict[str, object] = {
            "content_index_sha256": sha256_json(records),
            "destination": entry["destination"],
            "disposition": entry["disposition"],
            "excluded_files": len(excluded),
            "family": entry["family"],
            "file_count": len(records),
            "source": entry["source"],
            "total_bytes": family_bytes,
        }
        if detailed:
            family["files"] = records
        families.append(family)
    authoritative = manifest.get("authoritative_group")
    if authoritative:
        if not isinstance(authoritative, dict):
            raise ContractError("authoritative_group must be an object")
        names = set(authoritative["families"])
        selected = [item for item in families if item["family"] in names]
        if {item["family"] for item in selected} != names:
            raise IntegrityError("authoritative family list is incomplete")
        if sum(int(item["file_count"]) for item in selected) != int(
            authoritative["expected_files"]
        ):
            raise IntegrityError("authoritative DBN file-count boundary changed")
        if sum(int(item["total_bytes"]) for item in selected) != int(
            authoritative["expected_bytes"]
        ):
            raise IntegrityError("authoritative DBN byte-count boundary changed")
    identity_families = [
        {key: value for key, value in family.items() if key != "files"}
        for family in families
    ]
    identity_core = {
        "copy_authorized": bool(manifest["copy_authorized"]),
        "families": identity_families,
        "manifest_sha256": manifest_hash,
        "migration_id": manifest["migration_id"],
        "total_bytes": total_bytes,
        "total_files": total_files,
    }
    return {
        **identity_core,
        "families": families,
        "inventory_sha256": sha256_json(identity_core),
    }


_DETAILED_INVENTORY_FIELDS = {
    "copy_authorized",
    "families",
    "inventory_sha256",
    "manifest_sha256",
    "migration_id",
    "total_bytes",
    "total_files",
}
_DETAILED_FAMILY_FIELDS = {
    "content_index_sha256",
    "destination",
    "disposition",
    "excluded_files",
    "family",
    "file_count",
    "files",
    "source",
    "total_bytes",
}


def _validate_detailed_inventory_payload(
    manifest: dict[str, object], manifest_hash: str, payload: object
) -> dict[str, object]:
    if not isinstance(payload, dict) or set(payload) != _DETAILED_INVENTORY_FIELDS:
        raise IntegrityError("detailed inventory fields differ")
    families = payload.get("families")
    entries = manifest.get("entries")
    if (
        payload.get("manifest_sha256") != manifest_hash
        or payload.get("migration_id") != manifest.get("migration_id")
        or payload.get("copy_authorized") is not manifest.get("copy_authorized")
        or not isinstance(families, list)
        or not isinstance(entries, list)
        or len(families) != len(entries)
    ):
        raise IntegrityError("detailed inventory does not bind the manifest")
    total_files = 0
    total_bytes = 0
    seen_sources: set[str] = set()
    seen_destinations: set[str] = set()
    identity_families: list[dict[str, object]] = []
    for entry, family in zip(entries, families, strict=True):
        if (
            not isinstance(entry, dict)
            or not isinstance(family, dict)
            or set(family) != _DETAILED_FAMILY_FIELDS
            or any(
                family.get(name) != entry.get(name)
                for name in ("family", "source", "destination", "disposition")
            )
        ):
            raise IntegrityError("detailed inventory family differs from manifest")
        files = family.get("files")
        if not isinstance(files, list) or files != sorted(
            files,
            key=lambda record: str(record.get("path", ""))
            if isinstance(record, dict)
            else "",
        ):
            raise IntegrityError("detailed inventory file index is not canonical")
        family_bytes = 0
        normalized_files: list[dict[str, object]] = []
        entry_source = Path(str(entry["source"]))
        entry_destination = Path(str(entry["destination"]))
        for record in files:
            if not isinstance(record, dict) or set(record) != {
                "destination",
                "path",
                "sha256",
                "size",
            }:
                raise IntegrityError("detailed inventory file fields differ")
            source_text = record.get("path")
            destination_text = record.get("destination")
            size = record.get("size")
            digest = record.get("sha256")
            if (
                type(source_text) is not str
                or not source_text
                or Path(source_text).is_absolute()
                or ".." in Path(source_text).parts
                or Path(source_text).as_posix() != source_text
                or type(destination_text) is not str
                or not destination_text
                or Path(destination_text).is_absolute()
                or ".." in Path(destination_text).parts
                or Path(destination_text).as_posix() != destination_text
                or isinstance(size, bool)
                or not isinstance(size, int)
                or size < 0
                or type(digest) is not str
                or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            ):
                raise IntegrityError("detailed inventory file record is invalid")
            try:
                Path(source_text).relative_to(entry_source)
                Path(destination_text).relative_to(entry_destination)
            except ValueError as exc:
                raise IntegrityError(
                    "detailed inventory file escapes its migration family"
                ) from exc
            destination_key = _windows_destination_key(Path(destination_text))
            if source_text in seen_sources or destination_key in seen_destinations:
                raise IntegrityError(
                    "detailed inventory repeats a source or normalized destination"
                )
            seen_sources.add(source_text)
            seen_destinations.add(destination_key)
            normalized_files.append(record)
            family_bytes += size
        excluded = family.get("excluded_files")
        if (
            isinstance(excluded, bool)
            or not isinstance(excluded, int)
            or excluded < 0
            or family.get("file_count") != len(files)
            or family.get("total_bytes") != family_bytes
            or family.get("content_index_sha256") != sha256_json(normalized_files)
        ):
            raise IntegrityError("detailed inventory family totals/hash are invalid")
        if "expected_files" in entry and len(files) != int(entry["expected_files"]):
            raise IntegrityError("detailed inventory expected file count changed")
        if "expected_bytes" in entry and family_bytes != int(entry["expected_bytes"]):
            raise IntegrityError("detailed inventory expected byte count changed")
        if "expected_excluded_files" in entry and excluded != int(
            entry["expected_excluded_files"]
        ):
            raise IntegrityError("detailed inventory exclusion count changed")
        if "expected_sha256" in entry and (
            len(files) != 1
            or files[0]["sha256"] != str(entry["expected_sha256"]).lower()
        ):
            raise IntegrityError("detailed inventory pinned file hash changed")
        identity_families.append(
            {key: value for key, value in family.items() if key != "files"}
        )
        total_files += len(files)
        total_bytes += family_bytes
    authoritative = manifest.get("authoritative_group")
    if authoritative is not None:
        if not isinstance(authoritative, dict):
            raise IntegrityError("detailed inventory authoritative group is invalid")
        names = authoritative.get("families")
        if (
            not isinstance(names, list)
            or any(type(name) is not str for name in names)
            or len(names) != len(set(names))
        ):
            raise IntegrityError("detailed inventory authoritative families are invalid")
        selected = [family for family in families if family["family"] in set(names)]
        if {family["family"] for family in selected} != set(names):
            raise IntegrityError("detailed inventory authoritative families are incomplete")
        if sum(int(family["file_count"]) for family in selected) != int(
            authoritative["expected_files"]
        ) or sum(int(family["total_bytes"]) for family in selected) != int(
            authoritative["expected_bytes"]
        ):
            raise IntegrityError("detailed inventory authoritative boundary changed")
    identity_core = {
        "copy_authorized": manifest["copy_authorized"],
        "families": identity_families,
        "manifest_sha256": manifest_hash,
        "migration_id": manifest["migration_id"],
        "total_bytes": total_bytes,
        "total_files": total_files,
    }
    if (
        payload.get("total_files") != total_files
        or payload.get("total_bytes") != total_bytes
        or payload.get("inventory_sha256") != sha256_json(identity_core)
    ):
        raise IntegrityError("detailed inventory identity/totals are invalid")
    return payload


def _canonical_json_file(path: Path) -> object:
    assert_plain_file(path)
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"canonical JSON artifact is invalid: {path}") from exc
    if raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"JSON artifact is not canonical: {path}")
    return payload


def _atomic_write_canonical_new(path: Path, payload: object) -> None:
    """Atomically publish one canonical artifact without overwriting evidence."""

    if path.exists():
        raise IntegrityError(f"review artifact already exists: {path}")
    _assert_no_linklike_ancestor(path.parent)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    descriptor = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(payload) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        # A same-directory hard-link publication is atomic and fails if the
        # destination appears concurrently. Removing the temporary name leaves
        # the final artifact with one link before it can be accepted as evidence.
        os.link(temporary, path)
        fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()
            fsync_directory(path.parent)
    assert_plain_file(path)
    if _canonical_json_file(path) != payload:
        raise IntegrityError("published review artifact changed during creation")


def _state_review_artifact_path(
    boundary: RepoBoundary, path: Path, *, purpose: str
) -> Path:
    expected_root = (boundary.active_root / "state" / "migrations").resolve(
        strict=False
    )
    resolved = boundary.assert_active_path(
        path,
        purpose=purpose,
        subtree="state/migrations",
    )
    if resolved.parent != expected_root or resolved.suffix.lower() != ".json":
        raise UnauthorizedOperation(
            "migration review artifacts must be direct JSON children of state/migrations"
        )
    _windows_destination_key(Path(resolved.name))
    return resolved


def _inventory_review_artifact(
    manifest: dict[str, object],
    manifest_hash: str,
    detailed_inventory: dict[str, object],
) -> dict[str, object]:
    _validate_detailed_inventory_payload(
        manifest, manifest_hash, detailed_inventory
    )
    core = {
        "artifact_version": MIGRATION_REVIEW_ARTIFACT_VERSION,
        "execution_authorized": False,
        "inventory": detailed_inventory,
        "manifest_sha256": manifest_hash,
        "source_scope_sha256": sha256_json(migration_source_scope(manifest)),
        "status": MIGRATION_INVENTORY_REVIEW_STATUS,
    }
    return {**core, "artifact_id": sha256_json(core)}


def load_detailed_inventory_review(
    manifest: dict[str, object],
    manifest_hash: str,
    path: Path,
    *,
    boundary: RepoBoundary,
) -> dict[str, object]:
    reviewed_path = _state_review_artifact_path(
        boundary, path, purpose="migration detailed-inventory review"
    )
    payload = _canonical_json_file(reviewed_path)
    required = {
        "artifact_id",
        "artifact_version",
        "execution_authorized",
        "inventory",
        "manifest_sha256",
        "source_scope_sha256",
        "status",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise IntegrityError("detailed-inventory review artifact fields differ")
    core = {key: value for key, value in payload.items() if key != "artifact_id"}
    if (
        payload.get("artifact_id") != sha256_json(core)
        or payload.get("artifact_version") != MIGRATION_REVIEW_ARTIFACT_VERSION
        or payload.get("status") != MIGRATION_INVENTORY_REVIEW_STATUS
        or payload.get("execution_authorized") is not False
        or payload.get("manifest_sha256") != manifest_hash
        or payload.get("source_scope_sha256")
        != sha256_json(migration_source_scope(manifest))
    ):
        raise IntegrityError("detailed-inventory review artifact identity is invalid")
    _validate_detailed_inventory_payload(
        manifest, manifest_hash, payload.get("inventory")
    )
    return payload


def write_detailed_inventory_review(
    manifest: dict[str, object],
    manifest_hash: str,
    output_path: Path,
    *,
    boundary: RepoBoundary,
    operation_receipt: OperationReceipt,
) -> dict[str, object]:
    repository_execution = (
        boundary.active_root.resolve(strict=False)
        == _repository_root().resolve(strict=True)
    )
    if repository_execution:
        boundary.assert_active_root(_repository_root())
        _validate_authorized_repository_manifest(manifest, manifest_hash)
        _validate_controlled_rebuild_authorization()
    source_root = Path(str(manifest["source_root"]))
    boundary.assert_legacy_read_root(source_root)
    output = _state_review_artifact_path(
        boundary, output_path, purpose="migration detailed-inventory review output"
    )
    operation_receipt.verify(
        boundary,
        operation=MIGRATION_INVENTORY_REVIEW_OPERATION,
        classification=(
            OperationClassification.CONTROLLED_REBUILD_NON_ALPHA
            if repository_execution
            else OperationClassification.SYNTHETIC_MECHANICS_ONLY
        ),
        required_scope=migration_inventory_review_scope(
            manifest, manifest_hash, output
        ),
    )
    detailed = inventory(manifest, manifest_hash, detailed=True)
    artifact = _inventory_review_artifact(manifest, manifest_hash, detailed)
    _atomic_write_canonical_new(output, artifact)
    return {
        "artifact_id": artifact["artifact_id"],
        "execution_authorized": False,
        "inventory_sha256": detailed["inventory_sha256"],
        "output_path": str(output),
        "status": MIGRATION_INVENTORY_REVIEW_STATUS,
        "total_bytes": detailed["total_bytes"],
        "total_files": detailed["total_files"],
    }


def _approval_draft_artifact(
    approval: MigrationApproval, inventory_review_artifact_id: str
) -> dict[str, object]:
    proposed = approval_artifact_payload_for_review(approval)
    core = {
        "artifact_version": MIGRATION_REVIEW_ARTIFACT_VERSION,
        "execution_authorized": False,
        "inventory_review_artifact_id": inventory_review_artifact_id,
        "proposed_tracked_artifact": proposed,
        "required_next_step": (
            "REVIEW_THEN_APPLY_PATCH_CONFIGS_MIGRATION_APPROVAL_AUTHORIZED_JSON"
        ),
        "status": MIGRATION_APPROVAL_DRAFT_STATUS,
        "tracked_destination": AUTHORIZED_MIGRATION_APPROVAL_RELATIVE.as_posix(),
    }
    return {**core, "artifact_id": sha256_json(core)}


def write_migration_approval_draft(
    manifest: dict[str, object],
    manifest_hash: str,
    inventory_review_path: Path,
    approved_at: str,
    output_path: Path,
    *,
    boundary: RepoBoundary,
    operation_receipt: OperationReceipt,
) -> dict[str, object]:
    repository_execution = (
        boundary.active_root.resolve(strict=False)
        == _repository_root().resolve(strict=True)
    )
    if repository_execution:
        boundary.assert_active_root(_repository_root())
        _validate_authorized_repository_manifest(manifest, manifest_hash)
    output = _state_review_artifact_path(
        boundary, output_path, purpose="migration approval-payload draft output"
    )
    review = load_detailed_inventory_review(
        manifest,
        manifest_hash,
        inventory_review_path,
        boundary=boundary,
    )
    operation_receipt.verify(
        boundary,
        operation=MIGRATION_APPROVAL_DRAFT_OPERATION,
        classification=(
            OperationClassification.CONTROLLED_REBUILD_NON_ALPHA
            if repository_execution
            else OperationClassification.SYNTHETIC_MECHANICS_ONLY
        ),
        required_scope=migration_approval_draft_scope(
            manifest,
            manifest_hash,
            str(review["artifact_id"]),
            approved_at,
            output,
        ),
    )
    detailed = review["inventory"]
    assert isinstance(detailed, dict)
    approval = MigrationApproval.from_dict(
        approval_payload_for_review(
            manifest_hash=manifest_hash,
            source_inventory=detailed,
            approved_at=approved_at,
        )
    )
    artifact = _approval_draft_artifact(approval, str(review["artifact_id"]))
    _atomic_write_canonical_new(output, artifact)
    return {
        "artifact_id": artifact["artifact_id"],
        "approval_id": approval.approval_id,
        "execution_authorized": False,
        "inventory_review_artifact_id": review["artifact_id"],
        "output_path": str(output),
        "status": MIGRATION_APPROVAL_DRAFT_STATUS,
    }


def _checkpoint_paths(manifest: dict[str, object]) -> tuple[Path, Path, Path]:
    state_root_value = manifest.get("state_root")
    lock_path_value = manifest.get("lock_path")
    if not state_root_value or not lock_path_value:
        raise ContractError("copy mode requires absolute state_root and lock_path")
    state_root = Path(str(state_root_value))
    lock_path = Path(str(lock_path_value))
    if not state_root.is_absolute() or not lock_path.is_absolute():
        raise ContractError("state_root and lock_path must be absolute")
    migration_id = str(manifest["migration_id"])
    if not migration_id.replace("_", "").isalnum():
        raise ContractError("migration_id must be alphanumeric with underscores")
    return state_root / f"{migration_id}.checkpoint.json", lock_path, state_root / "recovery"


def _assert_no_linklike_ancestor(path: Path) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current = current / part
        if current.exists() and is_linklike(current):
            raise ContractError(f"destination/state path crosses a link or junction: {current}")


def _write_checkpoint(path: Path, payload: dict[str, object]) -> None:
    _assert_no_linklike_ancestor(path.parent)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        assert_plain_file(path)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    descriptor = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(payload) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    fsync_directory(path.parent)


def _load_checkpoint(
    path: Path,
    *,
    manifest_hash: str,
    inventory_hash: str,
    approval_id: str,
    migration_implementation_sha256: str,
    user_authorization_id: str,
) -> dict[str, object]:
    if not path.exists():
        return {
            "approval_id": approval_id,
            "completed": {},
            "inventory_sha256": inventory_hash,
            "manifest_sha256": manifest_hash,
            "migration_implementation_sha256": migration_implementation_sha256,
            "recovered_orphans": [],
            "status": "IN_PROGRESS",
            "user_authorization_id": user_authorization_id,
        }
    try:
        assert_plain_file(path)
        checkpoint = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError("migration checkpoint is invalid") from exc
    if checkpoint.get("manifest_sha256") != manifest_hash:
        raise IntegrityError("checkpoint belongs to a different migration manifest")
    if checkpoint.get("inventory_sha256") != inventory_hash:
        raise IntegrityError("source inventory changed since checkpoint creation")
    if (
        checkpoint.get("approval_id") != approval_id
        or checkpoint.get("migration_implementation_sha256")
        != migration_implementation_sha256
        or checkpoint.get("user_authorization_id") != user_authorization_id
    ):
        raise IntegrityError("migration approval/code authority changed since checkpoint creation")
    if not isinstance(checkpoint.get("completed"), dict):
        raise IntegrityError("checkpoint completed map is invalid")
    return checkpoint


def _quarantine_orphan_temps(destination_root: Path, recovery_root: Path) -> list[str]:
    moved: list[str] = []
    if not destination_root.exists():
        return moved
    for source in sorted(destination_root.rglob(".*.tmp")):
        if not source.is_file():
            continue
        recovery_root.mkdir(parents=True, exist_ok=True)
        target = recovery_root / f"{source.name}.{uuid.uuid4().hex}.orphan"
        os.replace(source, target)
        moved.append(str(target))
    return moved


def _snapshot_contract(
    detailed_inventory: dict[str, object],
    manifest_hash: str,
    approval: MigrationApproval,
) -> tuple[dict[str, object], dict[str, dict[str, object]]]:
    expected: dict[str, dict[str, object]] = {}
    for family in detailed_inventory["families"]:  # type: ignore[assignment]
        assert isinstance(family, dict)
        for record in family.get("files", []):
            assert isinstance(record, dict)
            key = str(record["destination"])
            if key in expected:
                raise IntegrityError(f"multiple sources target the same destination: {key}")
            expected[key] = {
                "sha256": str(record["sha256"]),
                "size": int(record["size"]),
            }
    files_index = [
        {"path": path, "sha256": value["sha256"], "size": value["size"]}
        for path, value in sorted(expected.items())
    ]
    receipt_semantics = {
        "approval_id": approval.approval_id,
        "files": files_index,
        "files_index_sha256": sha256_json(files_index),
        "inventory_sha256": detailed_inventory["inventory_sha256"],
        "manifest_sha256": manifest_hash,
        "migration_implementation_sha256": approval.migration_implementation_sha256,
        "receipt_version": SNAPSHOT_RECEIPT_VERSION,
        "status": SNAPSHOT_RECEIPT_STATUS,
        "total_bytes": sum(int(item["size"]) for item in files_index),
        "total_files": len(files_index),
        "user_authorization_id": approval.user_authorization_id,
    }
    snapshot_id = sha256_json(receipt_semantics)
    receipt = {
        **receipt_semantics,
        "source_snapshot_id": snapshot_id,
    }
    return receipt, expected


def _expected_snapshot_directories(expected_paths: set[str]) -> set[str]:
    directories: set[str] = set()
    for value in expected_paths:
        parent = Path(value).parent
        while parent != Path("."):
            directories.add(parent.as_posix())
            parent = parent.parent
    return directories


def _validate_existing_stage(
    root: Path,
    expected: dict[str, dict[str, object]],
    receipt: dict[str, object],
) -> None:
    """Fail before new writes when staging contains anything unplanned."""

    if not root.exists():
        return
    if not root.is_dir() or is_linklike(root):
        raise IntegrityError(f"snapshot staging root is absent or link-like: {root}")
    expected_files = set(expected)
    allowed_files = {*expected_files, "SOURCE_SNAPSHOT_RECEIPT.json"}
    allowed_directories = _expected_snapshot_directories(expected_files)
    observed_files: set[str] = set()
    observed_directories: set[str] = set()
    for path in root.rglob("*"):
        if is_linklike(path):
            raise IntegrityError(f"snapshot staging contains a link or junction: {path}")
        relative = path.relative_to(root).as_posix()
        if path.is_dir():
            observed_directories.add(relative)
        else:
            observed_files.add(relative)
    extra_files = observed_files.difference(allowed_files)
    extra_directories = observed_directories.difference(allowed_directories)
    if extra_files or extra_directories:
        raise IntegrityError(
            "snapshot staging contains unexpected files or directories"
        )
    for relative in observed_files.intersection(expected_files):
        record = expected[relative]
        path = root / Path(relative)
        if (
            path.stat().st_size != int(record["size"])
            or sha256_file(path) != record["sha256"]
        ):
            raise IntegrityError(
                f"existing staging destination is not the verified source: {relative}"
            )
    receipt_path = root / "SOURCE_SNAPSHOT_RECEIPT.json"
    if receipt_path.exists():
        assert_plain_file(receipt_path)
        try:
            raw = receipt_path.read_bytes()
            observed_receipt = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError("existing staging receipt is invalid") from exc
        if (
            raw != canonical_bytes(observed_receipt) + b"\n"
            or observed_receipt != receipt
        ):
            raise IntegrityError(
                "existing staging receipt differs from frozen snapshot identity"
            )


def _verify_final_source_inventory(
    manifest: dict[str, object], manifest_hash: str, expected_inventory_hash: str
) -> None:
    final = inventory(manifest, manifest_hash, detailed=False)
    if final["inventory_sha256"] != expected_inventory_hash:
        raise IntegrityError(
            "source inventory changed after copy and before publication"
        )


def _verify_snapshot_files(
    root: Path,
    expected: dict[str, dict[str, object]],
    *,
    allow_receipt: bool,
) -> None:
    if not root.is_dir() or is_linklike(root):
        raise IntegrityError(f"snapshot root is absent or link-like: {root}")
    actual: set[str] = set()
    actual_directories: set[str] = set()
    for path in root.rglob("*"):
        if is_linklike(path):
            raise IntegrityError(f"snapshot contains a link or junction: {path}")
        if path.is_dir():
            actual_directories.add(path.relative_to(root).as_posix())
            continue
        relative = path.relative_to(root).as_posix()
        if allow_receipt and relative == "SOURCE_SNAPSHOT_RECEIPT.json":
            continue
        actual.add(relative)
    if actual != set(expected):
        raise IntegrityError("snapshot contains missing or unexpected files")
    if actual_directories.difference(
        _expected_snapshot_directories(set(expected))
    ):
        raise IntegrityError("snapshot contains unexpected directories")
    for relative, record in expected.items():
        path = root / Path(relative)
        if path.stat().st_size != int(record["size"]) or sha256_file(path) != record["sha256"]:
            raise IntegrityError(f"snapshot destination failed verification: {relative}")


def _verify_published_snapshot(
    target: Path,
    receipt: dict[str, object],
    expected: dict[str, dict[str, object]],
) -> None:
    _verify_snapshot_files(target, expected, allow_receipt=True)
    receipt_path = target / "SOURCE_SNAPSHOT_RECEIPT.json"
    assert_plain_file(receipt_path)
    try:
        observed = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError("source snapshot receipt is invalid") from exc
    _validate_snapshot_receipt(observed)
    if observed != receipt:
        raise IntegrityError("source snapshot receipt differs from frozen identity")
    if target.name != receipt["source_snapshot_id"]:
        raise IntegrityError("published directory name differs from source snapshot ID")


def _validate_snapshot_receipt(receipt: object) -> dict[str, dict[str, object]]:
    if not isinstance(receipt, dict) or set(receipt) != SNAPSHOT_RECEIPT_FIELDS:
        raise IntegrityError("published source snapshot receipt schema is not exact")
    if receipt["receipt_version"] != SNAPSHOT_RECEIPT_VERSION:
        raise IntegrityError("published source snapshot receipt version is unsupported")
    if receipt["status"] != SNAPSHOT_RECEIPT_STATUS:
        raise IntegrityError("published source snapshot receipt is not complete")
    for key in (
        "approval_id",
        "files_index_sha256",
        "inventory_sha256",
        "manifest_sha256",
        "migration_implementation_sha256",
        "source_snapshot_id",
        "user_authorization_id",
    ):
        if re.fullmatch(r"[0-9a-f]{64}", str(receipt[key])) is None:
            raise IntegrityError("published source snapshot receipt hash is invalid")
    raw_files = receipt["files"]
    if not isinstance(raw_files, list) or not raw_files:
        raise IntegrityError("published source snapshot receipt has no file index")
    expected: dict[str, dict[str, object]] = {}
    normalized_files: list[dict[str, object]] = []
    for raw in raw_files:
        if not isinstance(raw, dict) or set(raw) != {"path", "sha256", "size"}:
            raise IntegrityError("published source snapshot file record schema is invalid")
        relative = Path(str(raw["path"]))
        if (
            not str(raw["path"])
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != str(raw["path"])
            or relative.as_posix() == "SOURCE_SNAPSHOT_RECEIPT.json"
        ):
            raise IntegrityError("published source snapshot path is unsafe")
        key = relative.as_posix()
        if key in expected:
            raise IntegrityError("published source snapshot contains duplicate paths")
        size = raw["size"]
        digest = raw["sha256"]
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or re.fullmatch(r"[0-9a-f]{64}", str(digest)) is None
        ):
            raise IntegrityError("published source snapshot file size/hash is invalid")
        expected[key] = {"size": size, "sha256": digest}
        normalized_files.append({"path": key, "sha256": digest, "size": size})
    if normalized_files != raw_files or normalized_files != sorted(
        normalized_files, key=lambda item: str(item["path"])
    ):
        raise IntegrityError("published source snapshot file index is not canonical")
    total_files = receipt["total_files"]
    total_bytes = receipt["total_bytes"]
    if (
        isinstance(total_files, bool)
        or not isinstance(total_files, int)
        or isinstance(total_bytes, bool)
        or not isinstance(total_bytes, int)
        or total_files != len(expected)
        or total_bytes != sum(int(item["size"]) for item in expected.values())
    ):
        raise IntegrityError("published source snapshot totals are inconsistent")
    if sha256_json(raw_files) != receipt["files_index_sha256"]:
        raise IntegrityError("source snapshot receipt file index hash is invalid")
    semantics = {
        key: receipt[key] for key in SNAPSHOT_RECEIPT_FIELDS if key != "source_snapshot_id"
    }
    if sha256_json(semantics) != receipt["source_snapshot_id"]:
        raise IntegrityError("source snapshot receipt content address is invalid")
    return expected


def verify_published_source_snapshot(target: Path) -> dict[str, object]:
    """Verify a content-addressed source snapshot and return its exact receipt.

    Consumers must call this rather than trusting a receipt or directory name in
    isolation. Every recorded file is rehashed and unexpected paths are fatal.
    """

    if not target.is_dir() or is_linklike(target):
        raise IntegrityError("published source snapshot root is absent or link-like")
    receipt_path = target / "SOURCE_SNAPSHOT_RECEIPT.json"
    try:
        assert_plain_file(receipt_path)
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, ContractError, IntegrityError) as exc:
        raise IntegrityError("published source snapshot receipt is invalid") from exc
    expected = _validate_snapshot_receipt(receipt)
    _verify_published_snapshot(target, receipt, expected)
    return receipt


def _write_immutable_receipt(path: Path, receipt: dict[str, object]) -> None:
    if path.exists():
        assert_plain_file(path)
        if json.loads(path.read_text(encoding="utf-8")) != receipt:
            raise IntegrityError("existing staging receipt differs from frozen identity")
        return
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(receipt) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(path.parent)


def recover_stale_migration_lock(
    manifest: dict[str, object],
    manifest_hash: str,
    approved_inventory_hash: str,
    *,
    migration_approval: MigrationApproval,
    expected_token: str,
    boundary: RepoBoundary,
    operation_receipt: OperationReceipt,
    older_than: timedelta = MINIMUM_STALE_AGE,
) -> dict[str, object]:
    """Quarantine only a locally proven-dead writer lease and retain evidence."""

    repository_execution = (
        boundary.active_root.resolve(strict=False)
        == _repository_root().resolve(strict=True)
    )
    if repository_execution:
        boundary.assert_active_root(_repository_root())
        _validate_authorized_repository_manifest(manifest, manifest_hash)
        if _load_checked_in_migration_approval() != migration_approval:
            raise UnauthorizedOperation(
                "stale-lock recovery approval differs from checked-in authority"
            )
    if (
        not approved_inventory_hash
        or migration_approval.inventory_sha256 != approved_inventory_hash
    ):
        raise UnauthorizedOperation(
            "stale-lock recovery inventory differs from approved authority"
        )
    migration_approval.validate(
        manifest_hash=manifest_hash,
        source_inventory={
            "inventory_sha256": approved_inventory_hash,
            "total_files": migration_approval.total_files,
            "total_bytes": migration_approval.total_bytes,
        },
    )
    checkpoint_path, lock_path, recovery_root = _checkpoint_paths(manifest)
    expected_state_root = (
        boundary.active_root / "state" / "migrations"
    ).resolve(strict=False)
    if checkpoint_path.parent.resolve(strict=False) != expected_state_root:
        raise UnauthorizedOperation(
            "stale-lock recovery state root differs from active repository"
        )
    boundary.assert_active_path(
        lock_path, purpose="migration stale-lock recovery", subtree="state/locks"
    )
    boundary.assert_active_path(
        recovery_root / "_boundary_probe",
        purpose="migration stale-lock recovery evidence",
        subtree="state/migrations",
    )
    _assert_no_linklike_ancestor(recovery_root)
    operation_receipt.verify(
        boundary,
        operation=MIGRATION_STALE_LOCK_RECOVERY_OPERATION,
        classification=(
            OperationClassification.CONTROLLED_REBUILD_NON_ALPHA
            if repository_execution
            else OperationClassification.SYNTHETIC_MECHANICS_ONLY
        ),
        required_scope=migration_recovery_scope(
            manifest,
            manifest_hash,
            approved_inventory_hash,
            migration_approval,
            expected_token,
        ),
    )
    observed = FileLease.inspect(lock_path)
    stale_evidence = FileLease.quarantine_stale(
        lock_path,
        recovery_root,
        older_than=older_than,
        expected_token=expected_token,
    )
    if FileLease.inspect(stale_evidence) != observed:
        raise IntegrityError("quarantined stale-lock evidence changed during recovery")
    recovered_at = datetime.now(timezone.utc).isoformat()
    core = {
        "approval_id": migration_approval.approval_id,
        "inventory_sha256": approved_inventory_hash,
        "manifest_sha256": manifest_hash,
        "operation": MIGRATION_STALE_LOCK_RECOVERY_OPERATION,
        "owner": observed.as_dict(),
        "recovered_at": recovered_at,
        "recovery_version": "1.0.0",
        "repository_id": boundary.repository_id,
        "stale_evidence_path": stale_evidence.relative_to(
            boundary.active_root
        ).as_posix(),
        "stale_evidence_sha256": sha256_file(stale_evidence),
        "stale_lock_token": expected_token,
        "user_authorization_id": migration_approval.user_authorization_id,
    }
    recovery_receipt = {**core, "recovery_id": sha256_json(core)}
    receipt_path = recovery_root / f"migration_lock_recovery.{expected_token}.json"
    if receipt_path.exists():
        raise IntegrityError("stale-lock recovery receipt already exists")
    descriptor = os.open(
        receipt_path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(recovery_receipt) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    fsync_directory(recovery_root)
    return {
        "recovery_evidence": str(stale_evidence),
        "recovery_id": recovery_receipt["recovery_id"],
        "recovery_receipt": str(receipt_path),
        "status": "DEAD_OWNER_LEASE_QUARANTINED",
    }


def guarded_copy(
    manifest: dict[str, object],
    manifest_hash: str,
    approved_manifest_hash: str,
    approved_inventory_hash: str,
    *,
    migration_approval: MigrationApproval,
    boundary: RepoBoundary,
    operation_receipt: OperationReceipt,
) -> dict[str, int | str]:
    """Copy with checkpoint/resume and verify any destination before idempotent skip."""

    if not manifest.get("copy_authorized"):
        raise UnauthorizedOperation("copy mode is disabled in the reviewed manifest")
    if approved_manifest_hash != manifest_hash:
        raise UnauthorizedOperation("approval hash does not match the canonical manifest")
    repository_execution = (
        boundary.active_root.resolve(strict=False)
        == _repository_root().resolve(strict=True)
    )
    if repository_execution:
        boundary.assert_active_root(_repository_root())
        _validate_authorized_repository_manifest(manifest, manifest_hash)
        checked_in_approval = _load_checked_in_migration_approval()
        if checked_in_approval != migration_approval:
            raise UnauthorizedOperation(
                "execution approval differs from the checked-in reviewed artifact"
            )
    source_root = Path(str(manifest["source_root"]))
    destination_root = Path(str(manifest["destination_root"]))
    boundary.assert_legacy_read_root(source_root)
    destination_root = boundary.assert_active_path(
        destination_root,
        purpose="source snapshot staging",
        subtree="data/vault/.staging",
    )
    _assert_no_linklike_ancestor(destination_root)
    publication_root_value = manifest.get("publication_root")
    if not publication_root_value:
        raise ContractError("copy mode requires an absolute publication_root")
    publication_root = Path(str(publication_root_value))
    if not publication_root.is_absolute():
        raise ContractError("publication_root must be absolute")
    expected_publication_root = (
        boundary.active_root / "data" / "vault" / "source_snapshots"
    ).resolve(strict=False)
    boundary.assert_active_path(
        expected_publication_root / "_boundary_probe",
        purpose="source snapshot publication",
        subtree="data/vault/source_snapshots",
    )
    if publication_root.resolve(strict=False) != expected_publication_root:
        raise UnauthorizedOperation(
            "publication_root is not the exact active content-addressed snapshot root"
        )
    _assert_no_linklike_ancestor(publication_root)
    state_root = Path(str(manifest.get("state_root", "")))
    expected_state_root = (boundary.active_root / "state" / "migrations").resolve(
        strict=False
    )
    boundary.assert_active_path(
        expected_state_root / "_boundary_probe",
        purpose="migration state",
        subtree="state/migrations",
    )
    if state_root.resolve(strict=False) != expected_state_root:
        raise UnauthorizedOperation("state_root is not the exact active migration state root")
    lock_value = Path(str(manifest.get("lock_path", "")))
    boundary.assert_active_path(
        lock_value, purpose="migration lock", subtree="state/locks"
    )
    detailed = inventory(manifest, manifest_hash, detailed=True)
    if not approved_inventory_hash or approved_inventory_hash != detailed["inventory_sha256"]:
        raise UnauthorizedOperation(
            "approval inventory hash does not match the current frozen source inventory"
        )
    migration_approval.validate(
        manifest_hash=manifest_hash,
        source_inventory=detailed,
    )
    operation_receipt.verify(
        boundary,
        operation="COPY_SOURCE_SNAPSHOT",
        classification=(
            OperationClassification.CONTROLLED_REBUILD_NON_ALPHA
            if repository_execution
            else OperationClassification.SYNTHETIC_MECHANICS_ONLY
        ),
        required_scope=migration_authorization_scope(
            manifest,
            manifest_hash,
            approved_inventory_hash,
            migration_approval,
        ),
    )
    receipt, expected_by_destination = _snapshot_contract(
        detailed,
        manifest_hash,
        migration_approval,
    )
    published_target = publication_root / str(receipt["source_snapshot_id"])
    checkpoint_path, lock_path, recovery_root = _checkpoint_paths(manifest)
    copied_files = 0
    verified_files = 0
    copied_bytes = 0
    with FileLease(lock_path):
        checkpoint = _load_checkpoint(
            checkpoint_path,
            manifest_hash=manifest_hash,
            inventory_hash=str(detailed["inventory_sha256"]),
            approval_id=migration_approval.approval_id,
            migration_implementation_sha256=migration_approval.migration_implementation_sha256,
            user_authorization_id=migration_approval.user_authorization_id,
        )
        if published_target.exists():
            if destination_root.exists():
                raise IntegrityError(
                    "verified publication and duplicate staging root both exist"
                )
            _verify_published_snapshot(
                published_target, receipt, expected_by_destination
            )
            checkpoint["status"] = "PUBLISHED"
            checkpoint["publication_path"] = str(published_target)
            checkpoint["source_snapshot_id"] = receipt["source_snapshot_id"]
            checkpoint["completed_files"] = len(expected_by_destination)
            _write_checkpoint(checkpoint_path, checkpoint)
            return {
                "checkpoint": str(checkpoint_path),
                "copied_bytes": 0,
                "copied_files": 0,
                "publication_path": str(published_target),
                "source_snapshot_id": str(receipt["source_snapshot_id"]),
                "status": "PUBLISHED",
                "verified_files": len(expected_by_destination),
            }
        recovered = list(checkpoint.get("recovered_orphans", []))
        recovered.extend(_quarantine_orphan_temps(destination_root, recovery_root))
        checkpoint["recovered_orphans"] = recovered
        _validate_existing_stage(
            destination_root, expected_by_destination, receipt
        )
        completed = checkpoint["completed"]
        assert isinstance(completed, dict)
        checkpoint_dirty = 0
        for raw_entry in manifest["entries"]:  # type: ignore[assignment]
            assert isinstance(raw_entry, dict)
            included, _ = _entry_scan(source_root, raw_entry)
            for source, destination_relative in sorted(included, key=lambda pair: str(pair[1])):
                if destination_relative.is_absolute() or ".." in destination_relative.parts:
                    raise ContractError("entry destination must be a contained relative path")
                destination = destination_root / destination_relative
                key = destination_relative.as_posix()
                expected_record = expected_by_destination.get(key)
                if expected_record is None:
                    raise IntegrityError("copy target is absent from the frozen inventory")
                expected_hash = str(expected_record["sha256"])
                expected_size = int(expected_record["size"])
                if source.stat().st_size != expected_size or sha256_file(source) != expected_hash:
                    raise IntegrityError("source changed after the frozen inventory scan")
                if destination.exists():
                    if destination.stat().st_size != expected_size or sha256_file(destination) != expected_hash:
                        raise IntegrityError(f"existing destination is not the verified source: {destination}")
                    completed[key] = {"sha256": expected_hash, "size": expected_size}
                    verified_files += 1
                    checkpoint_dirty += 1
                    if checkpoint_dirty >= CHECKPOINT_BATCH_FILES:
                        _write_checkpoint(checkpoint_path, checkpoint)
                        checkpoint_dirty = 0
                    continue
                if key in completed:
                    raise IntegrityError("checkpoint says complete but destination is absent")
                destination.parent.mkdir(parents=True, exist_ok=True)
                _assert_no_linklike_ancestor(destination.parent)
                temporary = destination.parent / f".{destination.name}.{uuid.uuid4().hex}.tmp"
                before = expected_hash
                shutil.copyfile(source, temporary)
                with temporary.open("r+b") as handle:
                    os.fsync(handle.fileno())
                after = sha256_file(source)
                copied = sha256_file(temporary)
                if before != after or copied != before:
                    raise IntegrityError("source changed or copy hash differed during migration")
                os.replace(temporary, destination)
                fsync_directory(destination.parent)
                if sha256_file(destination) != before:
                    raise IntegrityError("destination failed post-publication verification")
                completed[key] = {"sha256": before, "size": expected_size}
                copied_files += 1
                copied_bytes += expected_size
                checkpoint_dirty += 1
                if checkpoint_dirty >= CHECKPOINT_BATCH_FILES:
                    _write_checkpoint(checkpoint_path, checkpoint)
                    checkpoint_dirty = 0
            if checkpoint_dirty:
                _write_checkpoint(checkpoint_path, checkpoint)
                checkpoint_dirty = 0
        if len(completed) != len(expected_by_destination):
            raise IntegrityError("checkpoint is incomplete after the planned copy loop")
        receipt_path = destination_root / "SOURCE_SNAPSHOT_RECEIPT.json"
        if receipt_path.exists():
            # A crash may occur after the durable receipt is written but before
            # the directory rename. Verify and finish that exact publication.
            _write_immutable_receipt(receipt_path, receipt)
            _verify_snapshot_files(
                destination_root, expected_by_destination, allow_receipt=True
            )
        else:
            _verify_snapshot_files(
                destination_root, expected_by_destination, allow_receipt=False
            )
            _write_immutable_receipt(receipt_path, receipt)
        publication_root.mkdir(parents=True, exist_ok=True)
        _assert_no_linklike_ancestor(publication_root)
        if published_target.exists():
            raise IntegrityError("content-addressed publication target collision")
        # This is deliberately the last substantive check before the atomic
        # publication rename: every legacy source is rehashed after all stage
        # bytes and the immutable receipt have been durably verified.
        _verify_final_source_inventory(
            manifest,
            manifest_hash,
            str(detailed["inventory_sha256"]),
        )
        os.rename(destination_root, published_target)
        fsync_directory(publication_root)
        _verify_published_snapshot(published_target, receipt, expected_by_destination)
        checkpoint["status"] = "PUBLISHED"
        checkpoint["completed_files"] = len(completed)
        checkpoint["publication_path"] = str(published_target)
        checkpoint["source_snapshot_id"] = receipt["source_snapshot_id"]
        _write_checkpoint(checkpoint_path, checkpoint)
    return {
        "checkpoint": str(checkpoint_path),
        "copied_bytes": copied_bytes,
        "copied_files": copied_files,
        "publication_path": str(published_target),
        "source_snapshot_id": str(receipt["source_snapshot_id"]),
        "status": "PUBLISHED",
        "verified_files": verified_files,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--approved-manifest-sha256", default="")
    parser.add_argument("--approved-inventory-sha256", default="")
    parser.add_argument("--approval", type=Path)
    parser.add_argument("--recover-stale-lock-token", default="")
    parser.add_argument("--active-root", type=Path)
    parser.add_argument("--legacy-root", type=Path)
    parser.add_argument("--foreign-root", type=Path, action="append", default=[])
    parser.add_argument("--detailed", action="store_true")
    parser.add_argument("--detailed-inventory-output", type=Path)
    parser.add_argument("--approval-draft-output", type=Path)
    parser.add_argument("--inventory-review", type=Path)
    parser.add_argument("--approved-at")
    args = parser.parse_args(argv)
    manifest, manifest_hash = load_manifest(args.manifest)
    review_mode = args.detailed_inventory_output is not None
    approval_draft_mode = args.approval_draft_output is not None
    if sum((args.execute, review_mode, approval_draft_mode)) > 1:
        parser.error(
            "--execute, --detailed-inventory-output, and --approval-draft-output "
            "are mutually exclusive"
        )
    if review_mode or approval_draft_mode:
        if args.active_root is None or args.legacy_root is None:
            parser.error(
                "review artifact output requires --active-root and --legacy-root"
            )
        if args.approval is not None or args.recover_stale_lock_token:
            parser.error("copy approval/recovery arguments are valid only with --execute")
        boundary = RepoBoundary(
            args.active_root,
            (args.legacy_root,),
            tuple(args.foreign_root),
        )
        repository_execution = (
            boundary.active_root.resolve(strict=False)
            == _repository_root().resolve(strict=True)
        )
        if repository_execution and args.manifest.resolve(strict=False) != (
            _repository_root() / AUTHORIZED_MIGRATION_MANIFEST_RELATIVE
        ).resolve(strict=True):
            parser.error(
                "repository review output requires the exact authorized manifest path"
            )
        classification = (
            OperationClassification.CONTROLLED_REBUILD_NON_ALPHA
            if repository_execution
            else OperationClassification.SYNTHETIC_MECHANICS_ONLY
        )
        if review_mode:
            if args.inventory_review is not None or args.approved_at is not None:
                parser.error(
                    "--inventory-review/--approved-at apply only to approval-draft output"
                )
            assert args.detailed_inventory_output is not None
            receipt = OperationReceipt.issue_local(
                boundary,
                operation=MIGRATION_INVENTORY_REVIEW_OPERATION,
                classification=classification,
                scope=migration_inventory_review_scope(
                    manifest, manifest_hash, args.detailed_inventory_output
                ),
            )
            result = write_detailed_inventory_review(
                manifest,
                manifest_hash,
                args.detailed_inventory_output,
                boundary=boundary,
                operation_receipt=receipt,
            )
        else:
            if args.inventory_review is None or not args.approved_at:
                parser.error(
                    "--approval-draft-output requires --inventory-review and --approved-at"
                )
            assert args.approval_draft_output is not None
            review = load_detailed_inventory_review(
                manifest,
                manifest_hash,
                args.inventory_review,
                boundary=boundary,
            )
            receipt = OperationReceipt.issue_local(
                boundary,
                operation=MIGRATION_APPROVAL_DRAFT_OPERATION,
                classification=classification,
                scope=migration_approval_draft_scope(
                    manifest,
                    manifest_hash,
                    str(review["artifact_id"]),
                    args.approved_at,
                    args.approval_draft_output,
                ),
            )
            result = write_migration_approval_draft(
                manifest,
                manifest_hash,
                args.inventory_review,
                args.approved_at,
                args.approval_draft_output,
                boundary=boundary,
                operation_receipt=receipt,
            )
    elif args.execute:
        if args.active_root is None or args.legacy_root is None or args.approval is None:
            parser.error("--execute requires --active-root, --legacy-root, and --approval")
        if args.manifest.resolve(strict=False) != (
            _repository_root() / AUTHORIZED_MIGRATION_MANIFEST_RELATIVE
        ).resolve(strict=True):
            parser.error("--execute requires the exact authorized manifest path")
        expected_approval_path = (
            _repository_root() / AUTHORIZED_MIGRATION_APPROVAL_RELATIVE
        ).resolve(strict=False)
        if args.approval.resolve(strict=False) != expected_approval_path:
            parser.error(
                "--approval must be the exact checked-in migration approval artifact"
            )
        migration_approval = _load_checked_in_migration_approval()
        boundary = RepoBoundary(
            args.active_root,
            (args.legacy_root,),
            tuple(args.foreign_root),
        )
        if args.recover_stale_lock_token:
            recovery_receipt = OperationReceipt.issue_local(
                boundary,
                operation=MIGRATION_STALE_LOCK_RECOVERY_OPERATION,
                classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
                scope=migration_recovery_scope(
                    manifest,
                    manifest_hash,
                    args.approved_inventory_sha256,
                    migration_approval,
                    args.recover_stale_lock_token,
                ),
            )
            recover_stale_migration_lock(
                manifest,
                manifest_hash,
                args.approved_inventory_sha256,
                migration_approval=migration_approval,
                expected_token=args.recover_stale_lock_token,
                boundary=boundary,
                operation_receipt=recovery_receipt,
            )
        operation_receipt = OperationReceipt.issue_local(
            boundary,
            operation="COPY_SOURCE_SNAPSHOT",
            classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
            scope=migration_authorization_scope(
                manifest,
                manifest_hash,
                args.approved_inventory_sha256,
                migration_approval,
            ),
        )
        result: object = guarded_copy(
            manifest,
            manifest_hash,
            args.approved_manifest_sha256,
            args.approved_inventory_sha256,
            migration_approval=migration_approval,
            boundary=boundary,
            operation_receipt=operation_receipt,
        )
    else:
        if args.approval is not None:
            parser.error("--approval is accepted only with --execute")
        if args.inventory_review is not None or args.approved_at is not None:
            parser.error(
                "--inventory-review/--approved-at require --approval-draft-output"
            )
        result = inventory(manifest, manifest_hash, detailed=args.detailed)
    print(canonical_bytes(result).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
