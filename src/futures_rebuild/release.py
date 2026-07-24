"""Content-addressed release manifests and atomic, collision-safe publication."""

from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .canonical import (
    assert_plain_file,
    assert_no_linklike_ancestors,
    canonical_bytes,
    contained_path,
    fsync_directory,
    is_linklike,
    sha256_file,
    sha256_json,
)
from .boundary import OperationReceipt, RepoBoundary
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .locking import FileLease


@dataclass(frozen=True, order=True)
class FileEntry:
    path: str
    size: int
    sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.path, str)
            or isinstance(self.size, bool)
            or not isinstance(self.size, int)
            or not isinstance(self.sha256, str)
        ):
            raise ContractError("release file fields must use exact JSON scalar types")
        relative = Path(self.path)
        if not self.path or relative.is_absolute() or ".." in relative.parts:
            raise ContractError("release file path must be contained and relative")
        if relative.as_posix() != self.path or self.path == "release_manifest.json":
            raise ContractError("release file path is not canonical")
        if self.size < 0 or re.fullmatch(r"[0-9a-f]{64}", self.sha256) is None:
            raise ContractError("release file size or SHA-256 is invalid")

    def as_dict(self) -> dict[str, object]:
        return {"path": self.path, "sha256": self.sha256, "size": self.size}


@dataclass(frozen=True)
class ReleaseManifest:
    release_id: str
    release_kind: str
    schema_version: str
    source_release_ids: tuple[str, ...]
    files: tuple[FileEntry, ...]
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.release_id, str)
            or not isinstance(self.release_kind, str)
            or not isinstance(self.schema_version, str)
            or not isinstance(self.source_release_ids, tuple)
            or any(not isinstance(item, str) for item in self.source_release_ids)
            or not isinstance(self.files, tuple)
            or any(not isinstance(item, FileEntry) for item in self.files)
            or re.fullmatch(r"[0-9a-f]{64}", self.release_id) is None
            or not self.release_kind
            or not self.schema_version
        ):
            raise ContractError("release identity, kind, and schema version are required")
        if any(re.fullmatch(r"[0-9a-f]{64}", item) is None for item in self.source_release_ids):
            raise ContractError("source release IDs must be SHA-256 identities")
        if self.source_release_ids != tuple(sorted(set(self.source_release_ids))):
            raise ContractError("source release IDs must be unique and canonically sorted")
        if (
            not self.files
            or self.files != tuple(sorted(self.files))
            or len({entry.path for entry in self.files}) != len(self.files)
        ):
            raise ContractError("release file index must be nonempty, unique, and sorted")
        if not isinstance(self.metadata, Mapping):
            raise ContractError("release metadata must be an object")
        try:
            canonical_bytes(dict(self.metadata))
        except (TypeError, ValueError) as exc:
            raise ContractError("release metadata must be canonical JSON") from exc

    @classmethod
    def build(
        cls,
        stage: Path,
        *,
        release_kind: str,
        schema_version: str,
        source_release_ids: Sequence[str] = (),
        metadata: Mapping[str, Any] | None = None,
    ) -> "ReleaseManifest":
        if not release_kind or not schema_version:
            raise ContractError("release kind and schema version are required")
        assert_no_linklike_ancestors(stage)
        if not stage.is_dir() or is_linklike(stage):
            raise ContractError("release stage must be a plain directory")
        entries: list[FileEntry] = []
        for path in sorted(stage.rglob("*")):
            if path.name == "release_manifest.json":
                raise ContractError("stage already contains a release manifest")
            if is_linklike(path):
                raise ContractError(f"link-like staged path is forbidden: {path}")
            if path.is_dir():
                continue
            relative = path.relative_to(stage).as_posix()
            entries.append(
                FileEntry(relative, path.stat().st_size, sha256_file(path))
            )
        if not entries:
            raise ContractError("cannot publish an empty release")
        core = {
            "files": [entry.as_dict() for entry in entries],
            "metadata": dict(metadata or {}),
            "release_kind": release_kind,
            "schema_version": schema_version,
            "source_release_ids": sorted(set(source_release_ids)),
        }
        release_id = sha256_json(core)
        return cls(
            release_id=release_id,
            release_kind=release_kind,
            schema_version=schema_version,
            source_release_ids=tuple(core["source_release_ids"]),
            files=tuple(entries),
            metadata=dict(metadata or {}),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "files": [entry.as_dict() for entry in self.files],
            "metadata": dict(self.metadata),
            "release_id": self.release_id,
            "release_kind": self.release_kind,
            "schema_version": self.schema_version,
            "source_release_ids": list(self.source_release_ids),
        }


@dataclass(frozen=True)
class VerifiedReleaseReceipt:
    """Re-verifiable proof of exact release bytes beneath the active repository."""

    repository_id: str
    relative_root: str
    release_id: str
    release_kind: str
    schema_version: str
    manifest_sha256: str
    receipt_id: str

    @staticmethod
    def _core(
        *,
        repository_id: str,
        relative_root: str,
        release_id: str,
        release_kind: str,
        schema_version: str,
        manifest_sha256: str,
    ) -> dict[str, str]:
        return {
            "manifest_sha256": manifest_sha256,
            "relative_root": relative_root,
            "release_id": release_id,
            "release_kind": release_kind,
            "repository_id": repository_id,
            "schema_version": schema_version,
        }

    @classmethod
    def from_release(
        cls, release_dir: Path, boundary: RepoBoundary
    ) -> "VerifiedReleaseReceipt":
        root = boundary.assert_active_path(
            release_dir, purpose="verified release", subtree="data/vault/releases"
        )
        manifest = verify_release(root)
        relative = root.relative_to(boundary.active_root).as_posix()
        core = cls._core(
            repository_id=boundary.repository_id,
            relative_root=relative,
            release_id=manifest.release_id,
            release_kind=manifest.release_kind,
            schema_version=manifest.schema_version,
            manifest_sha256=sha256_file(root / "release_manifest.json"),
        )
        return cls(**core, receipt_id=sha256_json(core))

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "VerifiedReleaseReceipt":
        expected = {
            "manifest_sha256",
            "receipt_id",
            "relative_root",
            "release_id",
            "release_kind",
            "repository_id",
            "schema_version",
        }
        if set(payload) != expected or any(
            not isinstance(payload.get(field), str) for field in expected
        ):
            raise IntegrityError("verified release receipt schema is invalid")
        receipt = cls(
            repository_id=payload["repository_id"],  # type: ignore[arg-type]
            relative_root=payload["relative_root"],  # type: ignore[arg-type]
            release_id=payload["release_id"],  # type: ignore[arg-type]
            release_kind=payload["release_kind"],  # type: ignore[arg-type]
            schema_version=payload["schema_version"],  # type: ignore[arg-type]
            manifest_sha256=payload["manifest_sha256"],  # type: ignore[arg-type]
            receipt_id=payload["receipt_id"],  # type: ignore[arg-type]
        )
        receipt._verify_hash()
        return receipt

    def _verify_hash(self) -> None:
        relative = Path(self.relative_root)
        if (
            not self.relative_root
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != self.relative_root
            or re.fullmatch(r"[0-9a-f]{64}", self.release_id) is None
            or re.fullmatch(r"[0-9a-f]{64}", self.manifest_sha256) is None
            or re.fullmatch(r"[0-9a-f]{64}", self.receipt_id) is None
            or not self.repository_id
            or not self.release_kind
            or not self.schema_version
            or relative.parts != ("data", "vault", "releases", self.release_id)
        ):
            raise IntegrityError("verified release receipt fields are invalid")
        core = self._core(
            repository_id=self.repository_id,
            relative_root=self.relative_root,
            release_id=self.release_id,
            release_kind=self.release_kind,
            schema_version=self.schema_version,
            manifest_sha256=self.manifest_sha256,
        )
        if sha256_json(core) != self.receipt_id:
            raise IntegrityError("verified release receipt hash is invalid")

    def verify(self, boundary: RepoBoundary) -> ReleaseManifest:
        self._verify_hash()
        if self.repository_id != boundary.repository_id:
            raise IntegrityError("verified release receipt belongs to another repository")
        root = boundary.assert_active_path(
            boundary.active_root / self.relative_root,
            purpose="verified release",
            subtree="data/vault/releases",
        )
        manifest = verify_release(root)
        if (
            manifest.release_id != self.release_id
            or manifest.release_kind != self.release_kind
            or manifest.schema_version != self.schema_version
            or sha256_file(root / "release_manifest.json") != self.manifest_sha256
        ):
            raise IntegrityError("verified release bytes differ from their receipt")
        return manifest

    def as_dict(self) -> dict[str, str]:
        return {
            **self._core(
                repository_id=self.repository_id,
                relative_root=self.relative_root,
                release_id=self.release_id,
                release_kind=self.release_kind,
                schema_version=self.schema_version,
                manifest_sha256=self.manifest_sha256,
            ),
            "receipt_id": self.receipt_id,
        }


def verify_release(release_dir: Path, manifest: ReleaseManifest | None = None) -> ReleaseManifest:
    assert_no_linklike_ancestors(release_dir)
    if not release_dir.is_dir() or is_linklike(release_dir):
        raise IntegrityError("release root must be a plain directory")
    manifest_path = release_dir / "release_manifest.json"
    try:
        assert_plain_file(manifest_path)
        manifest_bytes = manifest_path.read_bytes()
        payload = json.loads(manifest_bytes.decode("utf-8"))
        expected_manifest_keys = {
            "files",
            "metadata",
            "release_id",
            "release_kind",
            "schema_version",
            "source_release_ids",
        }
        if not isinstance(payload, dict) or set(payload) != expected_manifest_keys:
            raise IntegrityError("release manifest schema is not exact")
        if (
            not isinstance(payload["files"], list)
            or not isinstance(payload["source_release_ids"], list)
            or not isinstance(payload["metadata"], dict)
            or not isinstance(payload["release_id"], str)
            or not isinstance(payload["release_kind"], str)
            or not isinstance(payload["schema_version"], str)
            or any(not isinstance(item, str) for item in payload["source_release_ids"])
        ):
            raise IntegrityError("release manifest collection fields are invalid")
        if any(
            not isinstance(item, dict)
            or set(item) != {"path", "sha256", "size"}
            or not isinstance(item["path"], str)
            or not isinstance(item["sha256"], str)
            or isinstance(item["size"], bool)
            or not isinstance(item["size"], int)
            for item in payload["files"]
        ):
            raise IntegrityError("release manifest file-entry schema is not exact")
        if manifest_bytes != canonical_bytes(payload) + b"\n":
            raise IntegrityError("release manifest JSON is not canonical")
        observed = ReleaseManifest(
            release_id=payload["release_id"],
            release_kind=payload["release_kind"],
            schema_version=payload["schema_version"],
            source_release_ids=tuple(payload["source_release_ids"]),
            files=tuple(
                FileEntry(item["path"], item["size"], item["sha256"])
                for item in payload["files"]
            ),
            metadata=dict(payload["metadata"]),
        )
    except (OSError, ValueError, KeyError, TypeError, ContractError, IntegrityError) as exc:
        raise IntegrityError(f"invalid release manifest: {manifest_path}") from exc
    if tuple(observed.files) != tuple(sorted(observed.files)) or len(
        {entry.path for entry in observed.files}
    ) != len(observed.files):
        raise IntegrityError("release file index is not unique and canonically sorted")
    if observed.source_release_ids != tuple(sorted(set(observed.source_release_ids))):
        raise IntegrityError("source release IDs are not unique and canonically sorted")
    expected_core = {
        "files": [entry.as_dict() for entry in observed.files],
        "metadata": dict(observed.metadata),
        "release_kind": observed.release_kind,
        "schema_version": observed.schema_version,
        "source_release_ids": list(observed.source_release_ids),
    }
    if sha256_json(expected_core) != observed.release_id:
        raise IntegrityError("release ID does not match canonical manifest content")
    if release_dir.name != observed.release_id:
        raise IntegrityError("release directory name does not match release ID")
    if manifest is not None and observed.as_dict() != manifest.as_dict():
        raise IntegrityError("published manifest differs from requested manifest")
    expected_paths = {entry.path for entry in observed.files} | {"release_manifest.json"}
    actual_paths: set[str] = set()
    actual_directories: set[str] = set()
    for path in release_dir.rglob("*"):
        if is_linklike(path):
            raise IntegrityError(f"release contains a link or junction: {path}")
        relative = path.relative_to(release_dir).as_posix()
        if path.is_dir():
            actual_directories.add(relative)
        elif path.is_file():
            assert_plain_file(path)
            actual_paths.add(relative)
        else:
            raise IntegrityError(f"release contains a non-regular path: {path}")
    if actual_paths != expected_paths:
        raise IntegrityError("release contains missing or unexpected files")
    expected_directories: set[str] = set()
    for relative in expected_paths:
        parent = Path(relative).parent
        while parent != Path("."):
            expected_directories.add(parent.as_posix())
            parent = parent.parent
    if actual_directories != expected_directories:
        raise IntegrityError("release contains missing or unexpected directories")
    for entry in observed.files:
        path = contained_path(release_dir, entry.path)
        if path.stat().st_size != entry.size or sha256_file(path) != entry.sha256:
            raise IntegrityError(f"release file failed verification: {entry.path}")
    return observed


class AtomicPublisher:
    def __init__(
        self,
        staging_root: Path,
        release_root: Path,
        lock_path: Path,
        *,
        boundary: RepoBoundary,
        operation_receipt: OperationReceipt,
    ) -> None:
        operation_receipt.verify(boundary, operation="PUBLISH_RELEASE")
        layout_contract = boundary.active_root / "configs" / "data_layout_contract.json"
        if layout_contract.exists():
            from .data_layout import verify_layout_contract

            verify_layout_contract(layout_contract)
            raise UnauthorizedOperation(
                "layout-v1 vault publication is disabled by the layout-v2 contract"
            )
        self.boundary = boundary
        self.operation_receipt = operation_receipt
        expected_release_root = (boundary.active_root / "data" / "vault" / "releases").resolve(
            strict=False
        )
        if release_root.resolve(strict=False) != expected_release_root:
            raise UnauthorizedOperation(
                "release publication root must be data/vault/releases"
            )
        boundary.assert_active_path(
            expected_release_root / "_boundary_probe",
            purpose="release publication",
            subtree="data/vault/releases",
        )
        self.release_root = expected_release_root
        self.staging_root = boundary.assert_active_path(
            staging_root,
            purpose="release staging",
            subtree="data/vault/.staging/releases",
        )
        self.lock_path = boundary.assert_active_path(
            lock_path, purpose="release lock", subtree="state/locks"
        )

    def create_stage(self, purpose: str) -> Path:
        if not purpose or not purpose.replace("_", "").isalnum():
            raise ContractError("stage purpose must be an alphanumeric identifier")
        self.operation_receipt.verify(self.boundary, operation="PUBLISH_RELEASE")
        self.boundary.assert_active_path(self.staging_root, purpose="release staging")
        assert_no_linklike_ancestors(self.staging_root)
        self.staging_root.mkdir(parents=True, exist_ok=True)
        stage = self.staging_root / f"{purpose}-{uuid.uuid4().hex}"
        stage.mkdir()
        return stage

    def _remove_current_duplicate_stage(self, stage: Path) -> None:
        resolved = stage.resolve(strict=True)
        resolved.relative_to(self.staging_root.resolve(strict=True))
        for path in resolved.rglob("*"):
            if is_linklike(path):
                raise IntegrityError("duplicate stage contains a link-like entry")
        shutil.rmtree(resolved)
        fsync_directory(self.staging_root)

    def publish(self, stage: Path, manifest: ReleaseManifest) -> Path:
        self.operation_receipt.verify(self.boundary, operation="PUBLISH_RELEASE")
        self.boundary.assert_active_path(self.staging_root, purpose="release staging")
        self.boundary.assert_active_path(self.release_root, purpose="release publication")
        self.boundary.assert_active_path(self.lock_path, purpose="release lock")
        assert_no_linklike_ancestors(self.staging_root)
        assert_no_linklike_ancestors(self.release_root)
        assert_no_linklike_ancestors(self.lock_path)
        stage_resolved = stage.resolve(strict=True)
        try:
            stage_resolved.relative_to(self.staging_root.resolve(strict=True))
        except ValueError as exc:
            raise ContractError("stage is outside the declared staging root") from exc
        with FileLease(self.lock_path):
            rebuilt = ReleaseManifest.build(
                stage_resolved,
                release_kind=manifest.release_kind,
                schema_version=manifest.schema_version,
                source_release_ids=manifest.source_release_ids,
                metadata=manifest.metadata,
            )
            if rebuilt.as_dict() != manifest.as_dict():
                raise IntegrityError("stage bytes changed after manifest construction")
            self.release_root.mkdir(parents=True, exist_ok=True)
            target = self.release_root / manifest.release_id
            if target.exists():
                verify_release(target, manifest)
                self._remove_current_duplicate_stage(stage_resolved)
                return target
            manifest_path = stage_resolved / "release_manifest.json"
            descriptor = os.open(
                manifest_path,
                os.O_CREAT
                | os.O_EXCL
                | os.O_WRONLY
                | getattr(os, "O_BINARY", 0),
            )
            try:
                os.write(descriptor, canonical_bytes(manifest.as_dict()) + b"\n")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            fsync_directory(stage_resolved)
            try:
                os.replace(stage_resolved, target)
            except OSError:
                if target.exists():
                    verify_release(target, manifest)
                    self._remove_current_duplicate_stage(stage_resolved)
                    return target
                raise
            fsync_directory(self.release_root)
            verify_release(target, manifest)
            return target
