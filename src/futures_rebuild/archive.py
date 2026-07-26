"""Copy-audit support for the one-time layout-v1 vault archive."""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path, PurePosixPath
from typing import Mapping

from .canonical import (
    assert_no_linklike_ancestors,
    canonical_bytes,
    fsync_directory,
    is_linklike,
    sha256_file,
    sha256_json,
)
from .data_layout import DataReleaseManifest, manifest_relative_path
from .errors import IntegrityError


ARCHIVE_RELEASE_KIND = "futures_layout_v1_vault_archive_receipt"
ARCHIVE_SCHEMA_VERSION = "1.0.0"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def validate_archive_receipt_document(
    payload: object,
) -> dict[str, object]:
    """Validate the content-addressed inventory embedded in an archive release."""

    expected_keys = {
        "archive_receipt_id",
        "archive_root",
        "files",
        "source_root",
        "status",
        "total_bytes",
        "total_files",
        "tree_sha256",
    }
    if not isinstance(payload, dict) or set(payload) != expected_keys:
        raise IntegrityError("archive receipt document schema is invalid")
    files = payload["files"]
    if not isinstance(files, list):
        raise IntegrityError("archive receipt file inventory is invalid")
    previous: str | None = None
    total_bytes = 0
    for raw in files:
        if not isinstance(raw, dict) or set(raw) != {"path", "sha256", "size"}:
            raise IntegrityError("archive receipt file entry is invalid")
        relative = PurePosixPath(raw["path"] if isinstance(raw["path"], str) else "")
        if (
            not raw["path"]
            or raw["path"] == "."
            or "\\" in raw["path"]
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != raw["path"]
            or (previous is not None and raw["path"] <= previous)
            or type(raw["size"]) is not int
            or raw["size"] < 0
            or type(raw["sha256"]) is not str
            or _SHA256.fullmatch(raw["sha256"]) is None
        ):
            raise IntegrityError("archive receipt file entry is invalid")
        previous = raw["path"]
        total_bytes += raw["size"]
    core = {key: payload[key] for key in payload if key != "archive_receipt_id"}
    if (
        type(payload["archive_root"]) is not str
        or not payload["archive_root"]
        or type(payload["source_root"]) is not str
        or not payload["source_root"]
        or payload["status"] != "COMPLETE_VERIFIED_COPY_ONLY"
        or type(payload["total_bytes"]) is not int
        or payload["total_bytes"] != total_bytes
        or type(payload["total_files"]) is not int
        or payload["total_files"] != len(files)
        or type(payload["tree_sha256"]) is not str
        or payload["tree_sha256"] != sha256_json(files)
        or type(payload["archive_receipt_id"]) is not str
        or payload["archive_receipt_id"] != sha256_json(core)
    ):
        raise IntegrityError("archive receipt document identity is invalid")
    return dict(payload)


def archive_snapshot_receipt_entry(
    payload: object, source_snapshot_id: str
) -> dict[str, object]:
    """Return the one inventory row binding a preserved source snapshot receipt."""

    if _SHA256.fullmatch(source_snapshot_id) is None:
        raise IntegrityError("source snapshot identity is invalid")
    validated = validate_archive_receipt_document(payload)
    expected_path = (
        f"source_snapshots/{source_snapshot_id}/SOURCE_SNAPSHOT_RECEIPT.json"
    )
    matches = [
        dict(raw)
        for raw in validated["files"]  # type: ignore[index]
        if isinstance(raw, Mapping) and raw.get("path") == expected_path
    ]
    if len(matches) != 1:
        raise IntegrityError("archive does not bind the exact source snapshot receipt")
    return matches[0]


def _io_root(root: Path) -> Path:
    resolved = root.resolve(strict=True)
    if os.name == "nt" and not str(resolved).startswith("\\\\?\\"):
        return Path("\\\\?\\" + str(resolved))
    return resolved


def _plain_tree(root: Path) -> dict[str, Path]:
    resolved = root.resolve(strict=True)
    assert_no_linklike_ancestors(resolved)
    if not resolved.is_dir() or is_linklike(resolved):
        raise IntegrityError(f"archive tree is absent or link-like: {resolved}")
    io_root = _io_root(resolved)
    files: dict[str, Path] = {}
    for path in sorted(io_root.rglob("*")):
        if is_linklike(path):
            raise IntegrityError(f"archive tree contains a link-like path: {path}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise IntegrityError(f"archive tree contains a non-file path: {path}")
        relative = path.relative_to(io_root).as_posix()
        if relative in files:
            raise IntegrityError("archive tree contains duplicate logical paths")
        files[relative] = path
    return files


def verify_archive(source: Path, archive: Path) -> dict[str, object]:
    source_root = source.resolve(strict=True)
    archive_root = archive.resolve(strict=True)
    source_files = _plain_tree(source_root)
    archive_files = _plain_tree(archive_root)
    if set(source_files) != set(archive_files):
        raise IntegrityError("archive path inventory differs from the source vault")
    records: list[dict[str, object]] = []
    total_bytes = 0
    for index, relative in enumerate(sorted(source_files), start=1):
        source_path = source_files[relative]
        archive_path = archive_files[relative]
        source_size = source_path.stat().st_size
        if archive_path.stat().st_size != source_size:
            raise IntegrityError(f"archive file size differs: {relative}")
        source_sha256 = sha256_file(source_path)
        if sha256_file(archive_path) != source_sha256:
            raise IntegrityError(f"archive file hash differs: {relative}")
        records.append(
            {"path": relative, "sha256": source_sha256, "size": source_size}
        )
        total_bytes += source_size
        if index % 250 == 0:
            print(f"verified {index}/{len(source_files)} files", flush=True)
    core = {
        "archive_root": str(archive_root),
        "files": records,
        "source_root": str(source_root),
        "status": "COMPLETE_VERIFIED_COPY_ONLY",
        "total_bytes": total_bytes,
        "total_files": len(records),
        "tree_sha256": sha256_json(records),
    }
    return {**core, "archive_receipt_id": sha256_json(core)}


def publish_archive_receipt(
    source: Path, archive: Path, repository_root: Path
) -> Path:
    receipt = verify_archive(source, archive)
    validate_archive_receipt_document(receipt)
    staging = repository_root / "state" / "data_publication_staging" / "archive-receipt"
    staging.mkdir(parents=True, exist_ok=True)
    manifest = DataReleaseManifest.build(
        staging,
        phase="migration",
        release_kind=ARCHIVE_RELEASE_KIND,
        schema_version=ARCHIVE_SCHEMA_VERSION,
        embedded_documents={"archive_receipt": receipt},
        metadata={
            "archive_receipt_id": receipt["archive_receipt_id"],
            "status": receipt["status"],
            "total_bytes": receipt["total_bytes"],
            "total_files": receipt["total_files"],
            "tree_sha256": receipt["tree_sha256"],
        },
    )
    output = repository_root / manifest_relative_path(
        manifest.phase, manifest.release_id
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        if output.read_bytes() != canonical_bytes(manifest.as_dict()) + b"\n":
            raise IntegrityError("existing archive receipt conflicts with verification")
        return output
    temporary = output.parent / f".{manifest.release_id}.tmp"
    descriptor = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(manifest.as_dict()) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, output)
    fsync_directory(output.parent)
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        output = publish_archive_receipt(
            args.source, args.archive, args.repository_root.resolve(strict=True)
        )
    except (OSError, IntegrityError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
