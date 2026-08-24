"""Exact, verified access to one immutable layout-v2 DBN release."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Mapping

from ..boundary import RepoBoundary
from ..canonical import sha256_file, sha256_json
from ..data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    manifest_relative_path,
    verify_data_release_manifest,
)
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
    r"^(?P<start>\d{4}-\d{2}-\d{2}(?:T\d{6}Z)?)_"
    r"(?P<end>\d{4}-\d{2}-\d{2}(?:T\d{6}Z)?)\.dbn\.zst$"
)
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def dbn_filename_interval(filename: str) -> tuple[str, str]:
    """Return an exact query interval from a canonical DBN filename."""

    match = DBN_NAME.fullmatch(filename)
    if match is None:
        raise IntegrityError("snapshot DBN filename is invalid")
    raw_start, raw_end = match.group("start"), match.group("end")
    timestamped = "T" in raw_start or "T" in raw_end

    def normalized(value: str) -> str:
        if "T" in value:
            return (
                f"{value[:10]}T{value[11:13]}:{value[13:15]}:{value[15:17]}Z"
            )
        return f"{value}T00:00:00Z" if timestamped else value

    return normalized(raw_start), normalized(raw_end)


def _validate_successor_receipt(
    manifest: DataReleaseManifest,
    parent: DataReleaseManifest,
) -> None:
    """Require an exact immutable parent-plus-new-files successor."""

    if len(manifest.source_release_ids) != 1:
        raise IntegrityError("Phase 1A successor must identify one exact parent")
    parent_id = manifest.source_release_ids[0]
    receipt = manifest.embedded_documents.get("phase1a_receipt")
    expected_receipt_keys = {
        "approval_receipt_id",
        "parent_release_id",
        "source_inventory_id",
        "status",
        "total_bytes",
        "total_files",
    }
    if (
        not isinstance(receipt, dict)
        or set(receipt) != expected_receipt_keys
        or receipt.get("status") != "COMPLETE_VERIFIED_IMMUTABLE_SUCCESSOR"
        or receipt.get("parent_release_id") != parent_id
        or not isinstance(receipt.get("approval_receipt_id"), str)
        or SHA256.fullmatch(str(receipt["approval_receipt_id"])) is None
        or not isinstance(receipt.get("source_inventory_id"), str)
        or SHA256.fullmatch(str(receipt["source_inventory_id"])) is None
        or receipt.get("total_files") != len(manifest.files)
        or receipt.get("total_bytes") != sum(item.size for item in manifest.files)
        or dict(manifest.metadata)
        != {
            "approval_receipt_id": receipt["approval_receipt_id"],
            "parent_release_id": parent_id,
            "source_inventory_id": receipt["source_inventory_id"],
        }
        or parent.release_id != parent_id
        or parent.phase != manifest.phase
        or parent.release_kind != manifest.release_kind
        or parent.schema_version != manifest.schema_version
    ):
        raise IntegrityError("Phase 1A successor receipt or parent binding is invalid")
    parent_files = {item.logical_path: item for item in parent.files}
    successor_files = {item.logical_path: item for item in manifest.files}
    if (
        len(successor_files) <= len(parent_files)
        or any(successor_files.get(path) != entry for path, entry in parent_files.items())
    ):
        raise IntegrityError("Phase 1A successor is not an immutable strict superset")


def _verify_phase1a_lineage(
    manifest: DataReleaseManifest,
    *,
    boundary: RepoBoundary,
    visited: frozenset[str] = frozenset(),
) -> None:
    if manifest.release_id in visited:
        raise IntegrityError("Phase 1A release lineage contains a cycle")
    if not manifest.source_release_ids:
        return
    if len(visited) >= 32:
        raise IntegrityError("Phase 1A release lineage is unreasonably deep")
    if len(manifest.source_release_ids) != 1:
        raise IntegrityError("Phase 1A successor must identify one exact parent")
    parent_id = manifest.source_release_ids[0]
    parent_path = boundary.active_root / manifest_relative_path("dbn", parent_id)
    parent = verify_data_release_manifest(parent_path, boundary, verify_files=False)
    _validate_successor_receipt(manifest, parent)
    _verify_phase1a_lineage(
        parent,
        boundary=boundary,
        visited=visited | {manifest.release_id},
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
    allow_registered_hardlinks: bool = False

    @property
    def path(self) -> Path:
        return self.physical_path

    def verify(self) -> Path:
        path = self.physical_path
        if (
            type(self.allow_registered_hardlinks) is not bool
            or not path.is_file()
            or path.is_symlink()
            or path.stat().st_size != self.size
            or sha256_file(
                path,
                reject_hardlinks=not self.allow_registered_hardlinks,
            )
            != self.sha256
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
    def open(
        cls,
        path: Path,
        *,
        boundary: RepoBoundary,
        verify_files: bool = True,
    ) -> "PublishedDbnRelease":
        manifest = verify_data_release_manifest(
            path,
            boundary,
            verify_files=verify_files,
        )
        if (
            manifest.phase != "dbn"
            or manifest.release_kind != DBN_RELEASE_KIND
            or manifest.schema_version != "1.0.0"
            or not manifest.files
        ):
            raise IntegrityError("source DBN manifest is not the accepted Phase 1A release")
        _verify_phase1a_lineage(manifest, boundary=boundary)
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
