"""Exact, verified access to one immutable layout-v2 DBN release."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Mapping

from ..boundary import RepoBoundary
from ..canonical import sha256_file, sha256_json
from ..data_layout import DataReleaseReceipt, verify_data_release_manifest
from ..errors import ContractError, IntegrityError


DBN_RELEASE_KIND = "futures_phase1a_verified_dbn"
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
class DbnReleaseFile:
    logical_path: str
    physical_path: Path
    relative_path: str
    size: int
    sha256: str
    source_release_id: str
    source_manifest_sha256: str
    files_index_sha256: str

    @property
    def path(self) -> Path:
        return self.physical_path

    def verify(self) -> Path:
        path = self.physical_path
        if (
            not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != self.size
            or sha256_file(path) != self.sha256
        ):
            raise IntegrityError("DBN release file bytes differ from their central manifest")
        return path


@dataclass(frozen=True)
class PublishedDbnRelease:
    manifest_path: Path
    receipt: DataReleaseReceipt
    files: Mapping[str, DbnReleaseFile]
    files_index_sha256: str

    @classmethod
    def open(cls, path: Path, *, boundary: RepoBoundary) -> "PublishedDbnRelease":
        manifest = verify_data_release_manifest(path, boundary)
        if (
            manifest.phase != "dbn"
            or manifest.release_kind != DBN_RELEASE_KIND
            or manifest.schema_version != "1.0.0"
            or not manifest.files
            or manifest.source_release_ids
        ):
            raise IntegrityError("source DBN manifest is not the accepted Phase 1A release")
        # The manifest and every file were verified immediately above.  Building
        # the content-addressed receipt must not repeat the same multi-gigabyte
        # hash pass.
        receipt = DataReleaseReceipt.from_manifest(
            path,
            boundary,
            verify_files=False,
        )
        index_hash = sha256_json([item.as_dict() for item in manifest.files])
        indexed: dict[str, DbnReleaseFile] = {}
        for entry in manifest.files:
            logical = PurePosixPath(entry.logical_path)
            if (
                len(logical.parts) != 6
                or logical.parts[:2] != ("data", "dbn")
                or logical.parts[2] not in SCHEMA_DIRECTORIES.values()
                or re.fullmatch(r"[0-9A-Z]{2,3}", logical.parts[3]) is None
                or re.fullmatch(r"\d{4}", logical.parts[4]) is None
                or not (
                    logical.name.endswith(".dbn.zst")
                    or logical.name.endswith(".dbn.zst.manifest.json")
                )
            ):
                raise IntegrityError("DBN manifest contains a noncanonical logical path")
            relative = PurePosixPath(*logical.parts[1:]).as_posix()
            if relative in indexed:
                raise IntegrityError("DBN manifest contains a duplicate logical path")
            indexed[relative] = DbnReleaseFile(
                logical_path=entry.logical_path,
                physical_path=boundary.active_root / manifest.physical_relative_path(entry),
                relative_path=relative,
                size=entry.size,
                sha256=entry.sha256,
                source_release_id=manifest.release_id,
                source_manifest_sha256=receipt.manifest_sha256,
                files_index_sha256=index_hash,
            )
        dbns = {key for key in indexed if key.endswith(".dbn.zst")}
        sidecars = {
            key.removesuffix(".manifest.json")
            for key in indexed
            if key.endswith(".dbn.zst.manifest.json")
        }
        if dbns != sidecars:
            raise IntegrityError("DBN release does not contain an exact DBN/sidecar pairing")
        return cls(
            manifest_path=path.resolve(strict=True),
            receipt=receipt,
            files=MappingProxyType(indexed),
            files_index_sha256=index_hash,
        )

    @property
    def root(self) -> Path:
        return self.manifest_path.parent

    @property
    def source_release_id(self) -> str:
        return self.receipt.release_id

    @property
    def source_manifest_sha256(self) -> str:
        return self.receipt.manifest_sha256

    def file(self, relative_path: str) -> DbnReleaseFile:
        relative = PurePosixPath(relative_path)
        if (
            not relative_path
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != relative_path
            or relative.parts[0] != "dbn"
        ):
            raise ContractError("DBN release file path is not canonical")
        try:
            return self.files[relative_path]
        except KeyError as exc:
            raise IntegrityError("file is absent from the accepted DBN release") from exc

    def dbn_file(
        self,
        *,
        schema: str,
        market: str,
        year: int,
        filename: str,
    ) -> DbnReleaseFile:
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


# Transitional import spellings are aliases only; the layout-v1 opener no longer exists.
SnapshotFile = DbnReleaseFile
PublishedSourceSnapshot = PublishedDbnRelease
