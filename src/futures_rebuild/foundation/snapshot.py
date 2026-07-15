"""Exact, verified access to one published source snapshot."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Mapping

from ..boundary import RepoBoundary
from ..canonical import sha256_file
from ..errors import ContractError, IntegrityError
from ..migration import verify_published_source_snapshot


SCHEMA_DIRECTORIES = MappingProxyType(
    {
        "definition": "definition",
        "ohlcv-1d": "ohlcv_1d",
        "ohlcv-1h": "ohlcv_1h",
        "ohlcv-1m": "ohlcv_1m",
        "ohlcv-1s": "ohlcv_1s",
        "statistics": "statistics",
        "status": "status",
        "trades": "trades",
    }
)
DBN_NAME = re.compile(
    r"^(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})\.dbn\.zst$"
)


@dataclass(frozen=True)
class SnapshotFile:
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
            raise IntegrityError("snapshot file bytes differ from their accepted index")
        return path


@dataclass(frozen=True)
class PublishedSourceSnapshot:
    root: Path
    receipt: Mapping[str, object]
    files: Mapping[str, SnapshotFile]

    @classmethod
    def open(
        cls, path: Path, *, boundary: RepoBoundary
    ) -> "PublishedSourceSnapshot":
        root = boundary.assert_snapshot_path(path)
        receipt = verify_published_source_snapshot(root)
        snapshot_id = receipt["source_snapshot_id"]
        manifest_hash = receipt["manifest_sha256"]
        index_hash = receipt["files_index_sha256"]
        if not all(
            isinstance(item, str) and re.fullmatch(r"[0-9a-f]{64}", item)
            for item in (snapshot_id, manifest_hash, index_hash)
        ):
            raise IntegrityError("source snapshot receipt hashes are invalid")
        indexed: dict[str, SnapshotFile] = {}
        for raw in receipt["files"]:  # type: ignore[index]
            if not isinstance(raw, dict):
                raise IntegrityError("source snapshot file index is invalid")
            relative = str(raw["path"])
            indexed[relative] = SnapshotFile(
                root=root,
                relative_path=relative,
                size=int(raw["size"]),
                sha256=str(raw["sha256"]),
                source_snapshot_id=snapshot_id,
                migration_manifest_sha256=manifest_hash,
                files_index_sha256=index_hash,
            )
        return cls(root, MappingProxyType(dict(receipt)), MappingProxyType(indexed))

    @property
    def source_snapshot_id(self) -> str:
        return str(self.receipt["source_snapshot_id"])

    def file(self, relative_path: str) -> SnapshotFile:
        relative = PurePosixPath(relative_path)
        if (
            not relative_path
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != relative_path
        ):
            raise ContractError("snapshot file path is not a canonical relative path")
        try:
            return self.files[relative_path]
        except KeyError as exc:
            raise IntegrityError("file is absent from the accepted source snapshot") from exc

    def dbn_file(
        self,
        *,
        schema: str,
        market: str,
        year: int,
        filename: str,
    ) -> SnapshotFile:
        try:
            directory = SCHEMA_DIRECTORIES[schema]
        except KeyError as exc:
            raise ContractError("DBN schema is not allowed by the foundation") from exc
        if (
            re.fullmatch(r"[0-9A-Z]{2,3}", market) is None
            or isinstance(year, bool)
            or not isinstance(year, int)
            or not 2000 <= year <= 2200
        ):
            raise ContractError("DBN market/year selector is invalid")
        match = DBN_NAME.fullmatch(filename)
        if match is None or int(match.group("start")[:4]) != year:
            raise ContractError("DBN filename does not match its exact year selector")
        relative = f"dbn/{directory}/{market}/{year}/{filename}"
        result = self.file(relative)
        sidecar = self.file(f"{relative}.manifest.json")
        result.verify()
        sidecar.verify()
        return result
