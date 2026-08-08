"""Content-addressed Git worktree snapshots and compact deltas."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from futures_rebuild.canonical import canonical_bytes, sha256_json

from .policy import WorkflowError, canonical_repo_root, git_identity


def _entries(repo: Path) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for line in git_identity(repo)["status_lines"]:
        if len(line) < 4:
            raise WorkflowError(f"malformed porcelain status line: {line!r}")
        entries.append({"status": line[:2], "path": line[3:]})
    return entries


def _write_create_only(path: Path, value: dict[str, Any]) -> None:
    data = canonical_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(data)
    except FileExistsError:
        if path.read_bytes() != data:
            raise WorkflowError(f"content-address collision or overwrite: {path}")


def create_snapshot_or_delta(
    repo: Path,
    output_root: Path,
    *,
    base_path: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    root = canonical_repo_root(repo)
    identity = git_identity(root)
    current = _entries(root)
    if base_path is None:
        body: dict[str, Any] = {
            "schema_version": "workflow_snapshot/1.0.0",
            "repository": str(root),
            "branch": identity["branch"],
            "head": identity["head"],
            "entries": current,
        }
        body["snapshot_id"] = sha256_json(body)
        path = output_root / "snapshots" / f"{body['snapshot_id']}.json"
        _write_create_only(path, body)
        return path, body

    base = json.loads(base_path.read_text(encoding="utf-8"))
    reconstructed = reconstruct_entries(base)
    old = {entry["path"]: entry for entry in reconstructed}
    new = {entry["path"]: entry for entry in current}
    upserts = [new[path] for path in sorted(new) if new[path] != old.get(path)]
    removals = [path for path in sorted(old) if path not in new]
    body = {
        "schema_version": "workflow_delta/1.0.0",
        "base_snapshot_id": base["snapshot_id"],
        "branch": identity["branch"],
        "head": identity["head"],
        "upserts": upserts,
        "removals": removals,
    }
    body["delta_id"] = sha256_json(body)
    path = output_root / "deltas" / f"{body['delta_id']}.json"
    _write_create_only(path, body)
    return path, body


def reconstruct_entries(
    base: dict[str, Any], deltas: list[dict[str, Any]] | None = None
) -> list[dict[str, str]]:
    if base.get("schema_version") != "workflow_snapshot/1.0.0":
        raise WorkflowError("unsupported snapshot schema")
    entries = {entry["path"]: dict(entry) for entry in base["entries"]}
    for delta in deltas or []:
        if delta.get("schema_version") != "workflow_delta/1.0.0":
            raise WorkflowError("unsupported delta schema")
        if delta["base_snapshot_id"] != base["snapshot_id"]:
            raise WorkflowError("delta is bound to a different base snapshot")
        for path in delta["removals"]:
            entries.pop(path, None)
        for entry in delta["upserts"]:
            entries[entry["path"]] = dict(entry)
    return [entries[path] for path in sorted(entries)]
