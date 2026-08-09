"""Freeze exact regenerable-cache candidates without deleting or reading data."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/safe_cleanup_candidate_census_v6/census.json"
)
CLEANUP_POLICY = Path("state/unpublished_evidence/safe_cleanup_preparation_v5/plan.json")
TOPOLOGY_REPORT = Path(
    "state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json"
)
V21_REPORT = Path(
    "state/unpublished_evidence/apex_micro_metadata_preflight_v21/report.json"
)
EXACT_ROOT_CANDIDATES = (Path(".pytest_cache"), Path(".pytest_tmp"))
RECURSIVE_CANDIDATE_ROOTS = (
    Path("scripts"),
    Path("src"),
    Path("tests"),
    Path("manifests/workflow"),
)


def _git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(root), *args],
        check=check,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _inventory(path: Path) -> dict[str, int]:
    files = [item for item in path.rglob("*") if item.is_file()]
    return {
        "file_count": len(files),
        "byte_count_from_filesystem_metadata": sum(item.stat().st_size for item in files),
    }


def _worktree_paths(root: Path) -> list[str]:
    completed = _git(root, "status", "--porcelain=v1", "-z")
    records = completed.stdout.split("\0")
    paths: list[str] = []
    for record in records:
        if not record:
            continue
        path = record[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.append(path.replace("\\", "/"))
    return sorted(set(paths))


def build_census(*, root: Path = ROOT, committed_head: str) -> dict[str, object]:
    root = root.resolve(strict=True)
    if _git(root, "rev-parse", "HEAD").stdout.strip() != committed_head:
        raise IntegrityError("cleanup census must bind the live committed HEAD")
    candidates = [path for path in EXACT_ROOT_CANDIDATES if (root / path).exists()]
    for base in RECURSIVE_CANDIDATE_ROOTS:
        base_path = root / base
        if base_path.exists():
            candidates.extend(
                path.relative_to(root)
                for path in base_path.rglob("__pycache__")
                if path.is_dir()
            )
    candidate_records: list[dict[str, object]] = []
    for relative in sorted(set(candidates), key=lambda value: value.as_posix()):
        absolute = root / relative
        if absolute.is_symlink() or not absolute.is_dir():
            raise IntegrityError("cleanup candidate is link-like or not a directory")
        relative_text = relative.as_posix()
        if relative.parts[0] in {"data", "state", "configs"}:
            raise IntegrityError("data, state, or config path entered cleanup candidates")
        tracked = bool(_git(root, "ls-files", "--", relative_text).stdout.strip())
        ignored = _git(
            root, "check-ignore", "-q", "--", relative_text, check=False
        ).returncode == 0
        if tracked or not ignored:
            raise IntegrityError("cleanup cache candidate is tracked or not ignored")
        candidate_records.append(
            {
                "path": relative_text,
                "classification": "REGENERABLE_IGNORED_CACHE_CANDIDATE",
                "tracked": False,
                "git_ignored": True,
                "inventory": _inventory(absolute),
                "proposed_action": "DELETE_ONLY_AFTER_SEPARATE_EXACT_APPROVAL",
            }
        )
    bindings = {
        CLEANUP_POLICY.as_posix(): sha256_file(root / CLEANUP_POLICY),
        TOPOLOGY_REPORT.as_posix(): sha256_file(root / TOPOLOGY_REPORT),
        V21_REPORT.as_posix(): sha256_file(root / V21_REPORT),
        "data/active/catalog.json": sha256_file(root / "data/active/catalog.json"),
    }
    core: dict[str, object] = {
        "schema_version": "safe_cleanup_candidate_census/6.0.0",
        "state": "PREPARED_NO_MUTATION_SEPARATE_EXACT_CLEANUP_APPROVAL_REQUIRED",
        "committed_head": committed_head,
        "authoritative_resolution": {
            "standard_lane": "data/active/catalog.json",
            "standard_active_root": "data/active/causally_gated_normalized",
            "phase2_release_history_root": "data/causally_gated_normalized",
            "micro_lane": "NO_ACTIVE_POINTER_OR_CATALOG",
            "directory_presence_alone_grants_research_use": False,
        },
        "protected_roots": [
            "configs",
            "data",
            "manifests/data_releases",
            "state/authorization_uses",
            "state/unpublished_evidence",
        ],
        "candidate_count": len(candidate_records),
        "candidates": candidate_records,
        "worktree_paths_preserved": _worktree_paths(root),
        "bindings": bindings,
        "required_immediately_before_cleanup": [
            "REVALIDATE_EXACT_HEAD_WORKTREE_AND_CANDIDATE_EXISTENCE",
            "PROVE_NO_NEW_CATALOG_MANIFEST_RECEIPT_PLAN_OR_SOURCE_BINDING",
            "OBTAIN_SEPARATE_EXACT_CLEANUP_APPROVAL",
            "USE_RECOVERABLE_OR_REGENERABLE_ONLY_OPERATION",
            "RERUN_STANDARD_TOPOLOGY_AND_MICRO_DISK_DESTINATION_GATES",
        ],
        "cleanup_execution": {
            "performed": False,
            "files_deleted": 0,
            "directories_deleted": 0,
            "files_moved": 0,
            "data_changed": False,
            "active_catalog_changed": False,
        },
        "payload_safety": {
            "dbn_or_parquet_payload_opened": False,
            "historical_rows_read": False,
            "year_2025_or_2026_payload_opened": False,
            "inventory_from_filesystem_metadata_only": True,
        },
    }
    return {**core, "census_id": sha256_json(core)}


def write_census_create_only(*, root: Path = ROOT, committed_head: str) -> dict[str, object]:
    census = build_census(root=root, committed_head=committed_head)
    path = root / OUTPUT
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(census) + b"\n")
    return census


def main() -> int:
    head = _git(ROOT, "rev-parse", "HEAD").stdout.strip()
    census = write_census_create_only(root=ROOT, committed_head=head)
    print(
        json.dumps(
            {
                "census_id": census["census_id"],
                "candidate_count": census["candidate_count"],
                "state": census["state"],
                "sha256": sha256_file(ROOT / OUTPUT),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
