"""Read-only verifier for the preserved layout-v1 evidence snapshot.

This adapter exists only so the unresolved legacy-trial census can be
re-derived as layout-v2 evidence.  Foundation and research consumers must use
central layout-v2 manifests instead.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Mapping

from .boundary import RepoBoundary
from .canonical import sha256_file
from .errors import ContractError, IntegrityError
from .migration import verify_published_source_snapshot


@dataclass(frozen=True)
class LegacyEvidenceFile:
    root: Path
    relative_path: str
    size: int
    sha256: str
    source_snapshot_id: str
    migration_manifest_sha256: str
    files_index_sha256: str

    @property
    def path(self) -> Path:
        return self.root / Path(self.relative_path)

    def verify(self) -> Path:
        path = self.path
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != self.size
            or sha256_file(path) != self.sha256
        ):
            raise IntegrityError("legacy evidence bytes differ from the accepted index")
        return path


@dataclass(frozen=True)
class PublishedLegacyEvidenceSnapshot:
    root: Path
    receipt: Mapping[str, object]
    files: Mapping[str, LegacyEvidenceFile]

    @classmethod
    def open(
        cls, path: Path, *, boundary: RepoBoundary
    ) -> "PublishedLegacyEvidenceSnapshot":
        root = boundary.assert_snapshot_path(path)
        receipt = verify_published_source_snapshot(root)
        snapshot_id = receipt["source_snapshot_id"]
        manifest_hash = receipt["manifest_sha256"]
        index_hash = receipt["files_index_sha256"]
        if not all(
            isinstance(item, str) and re.fullmatch(r"[0-9a-f]{64}", item)
            for item in (snapshot_id, manifest_hash, index_hash)
        ):
            raise IntegrityError("legacy evidence receipt hashes are invalid")
        indexed: dict[str, LegacyEvidenceFile] = {}
        for raw in receipt["files"]:  # type: ignore[index]
            if not isinstance(raw, dict):
                raise IntegrityError("legacy evidence file index is invalid")
            relative = str(raw["path"])
            if relative in indexed:
                raise IntegrityError("legacy evidence index contains a duplicate path")
            indexed[relative] = LegacyEvidenceFile(
                root=root,
                relative_path=relative,
                size=int(raw["size"]),
                sha256=str(raw["sha256"]),
                source_snapshot_id=str(snapshot_id),
                migration_manifest_sha256=str(manifest_hash),
                files_index_sha256=str(index_hash),
            )
        return cls(root, MappingProxyType(dict(receipt)), MappingProxyType(indexed))

    @property
    def source_snapshot_id(self) -> str:
        return str(self.receipt["source_snapshot_id"])

    def file(self, relative_path: str) -> LegacyEvidenceFile:
        relative = PurePosixPath(relative_path)
        if (
            not relative_path
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != relative_path
        ):
            raise ContractError("legacy evidence path is not canonical")
        try:
            return self.files[relative_path]
        except KeyError as exc:
            raise IntegrityError("file is absent from the accepted legacy evidence") from exc


__all__ = ["LegacyEvidenceFile", "PublishedLegacyEvidenceSnapshot"]
