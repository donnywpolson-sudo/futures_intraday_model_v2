"""Prepare a no-delete cleanup inventory for data and project paths."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path("state/unpublished_evidence/safe_cleanup_preparation/plan.json")

PROTECTED_PATHS = (
    "configs/active_alpha_research_ladder.json",
    "data/active/catalog.json",
    "data/active/causally_gated_normalized",
    "data/dbn",
    "data/raw",
    "data/causally_gated_normalized",
    "data/vault",
    "manifests",
    "state/authorization_uses",
    "state/unpublished_evidence",
)

REVIEW_PATHS = (
    "data/vault/.staging",
    (
        "data/vault/source_snapshots/"
        "6dc18d3104e37cb1bd65e5387b7a7a92f851d0e3ea571baa3157349872f5a872/"
        "comparison_only"
    ),
    (
        "data/vault/source_snapshots/"
        "6dc18d3104e37cb1bd65e5387b7a7a92f851d0e3ea571baa3157349872f5a872/"
        "evidence/legacy_research"
    ),
)


def _inventory(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"exists": False, "file_count": 0, "byte_count": 0}
    if path.is_file():
        return {
            "exists": True,
            "kind": "FILE",
            "file_count": 1,
            "byte_count": path.stat().st_size,
        }
    files = [item for item in path.rglob("*") if item.is_file()]
    return {
        "exists": True,
        "kind": "DIRECTORY",
        "file_count": len(files),
        "byte_count": sum(item.stat().st_size for item in files),
    }


def _worktree_paths(root: Path) -> list[str]:
    completed = subprocess.run(
        [
            "git", "-C", str(root), "status", "--porcelain=v1", "-z",
            "--untracked-files=all",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    output: list[str] = []
    for record in completed.stdout.decode("utf-8", "strict").split("\0"):
        if not record:
            continue
        path = record[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        output.append(path.replace("\\", "/"))
    return sorted(output)


def _cache_candidates(root: Path) -> list[dict[str, object]]:
    candidates: set[Path] = set()
    for relative in (".pytest_cache", ".pytest_tmp"):
        path = root / relative
        if path.exists():
            candidates.add(path)
    for relative_root in ("scripts", "src", "tests", "manifests/workflow"):
        search_root = root / relative_root
        if search_root.is_dir():
            candidates.update(search_root.rglob("__pycache__"))
    return [
        {
            "path": path.relative_to(root).as_posix(),
            **_inventory(path),
            "classification": "REGENERABLE_CACHE_CANDIDATE",
            "proposed_action": "DELETE_ONLY_AFTER_EXACT_CLEANUP_APPROVAL",
        }
        for path in sorted(candidates, key=lambda item: item.as_posix())
    ]


def _git_value(root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def build_plan(*, root: Path = ROOT) -> dict[str, object]:
    root = root.resolve(strict=True)
    topology_report = root / (
        "state/unpublished_evidence/standard_data_topology_source_safe_audit/"
        "report.json"
    )
    if not topology_report.is_file():
        raise IntegrityError("source-safe standard topology report is absent")
    protected = [
        {
            "path": relative,
            **_inventory(root / relative),
            "classification": (
                "AUTHORITATIVE_ACTIVE_CATALOG_SELECTED_VIEW"
                if relative.startswith("data/active")
                or relative == "configs/active_alpha_research_ladder.json"
                else "IMMUTABLE_PROVENANCE_OR_RELEASE_HISTORY"
            ),
            "proposed_action": "PRESERVE_NO_MOVE_DELETE_OR_RENAME",
        }
        for relative in PROTECTED_PATHS
    ]
    review = [
        {
            "path": relative,
            **_inventory(root / relative),
            "classification": "UNRESOLVED_OR_SNAPSHOT_EVIDENCE_PRESERVE",
            "proposed_action": "NO_ACTION_UNTIL_DEPENDENCY_CLOSURE_PROVEN",
        }
        for relative in REVIEW_PATHS
    ]
    core: dict[str, object] = {
        "schema_version": "safe_data_project_cleanup_preparation/1.0.0",
        "state": "PREPARED_NO_MUTATION_EXACT_CLEANUP_APPROVAL_REQUIRED",
        "repository_root": str(root),
        "branch": _git_value(root, "branch", "--show-current"),
        "observed_head": _git_value(root, "rev-parse", "HEAD"),
        "control_rationale": {
            "concrete_risk_prevented": (
                "DELETING_ACTIVE_CATALOG_FILES_OR_PINNED_RELEASE_HISTORY_BREAKS_"
                "PROVENANCE_HASHES_AND_RESEARCH_GATE_CLOSURE"
            ),
            "decision_improved": (
                "EXACTLY_WHICH_PATHS_MUST_BE_PRESERVED_REVIEWED_OR_MAY_BE_CLEANED"
            ),
            "why_simple_name_rules_are_insufficient": (
                "ACTIVE_FLAT_VIEWS_CONTENT_ADDRESSED_HISTORY_STAGING_SNAPSHOTS_AND_"
                "REGENERABLE_CACHES_HAVE_SIMILAR_OR_GENERIC_FOLDER_NAMES"
            ),
        },
        "authoritative_resolution": {
            "standard_lane": "data/active/catalog.json",
            "standard_active_root": "data/active/causally_gated_normalized",
            "phase2_release_history_root": "data/causally_gated_normalized",
            "micro_lane": "NO_ACTIVE_POINTER_OR_CATALOG",
            "directory_presence_alone_grants_research_use": False,
        },
        "protected_paths": protected,
        "manual_review_preserve_paths": review,
        "regenerable_cache_candidates": _cache_candidates(root),
        "current_worktree_paths_preserve": _worktree_paths(root),
        "cleanup_execution": {
            "performed": False,
            "files_deleted": 0,
            "directories_deleted": 0,
            "files_moved": 0,
            "active_data_changed": False,
            "raw_data_changed": False,
            "manifests_changed": False,
        },
        "required_before_any_cleanup": [
            "EXACT_CANDIDATE_PATH_CENSUS",
            "PROOF_NO_CATALOG_MANIFEST_RECEIPT_OR_PLAN_BINDING",
            "RECOVERABLE_ARCHIVE_OR_REGENERATION_PROOF",
            "SEPARATE_EXACT_CLEANUP_APPROVAL",
            "POST_CLEANUP_CATALOG_MANIFEST_AND_TEST_REVALIDATION",
            "MICRO_ACQUISITION_DESTINATION_AND_DISK_RECENSUS",
        ],
        "recommended_sequence": [
            "SECURE_CURRENT_MICRO_V5_IMPLEMENTATION",
            "RUN_SEPARATELY_APPROVED_V5_METADATA_PREFLIGHT",
            "RECONSTRUCT_CLEANUP_CANDIDATE_CENSUS",
            "REQUEST_SEPARATE_EXACT_CLEANUP_APPROVAL_IF_USEFUL",
            "VERIFY_CLEANUP_WITHOUT_ACTIVE_OR_IMMUTABLE_RELEASE_MUTATION",
            "FREEZE_FINAL_MICRO_ACQUISITION_PLAN_AFTER_CLEANUP_STATE_IS_STABLE",
        ],
        "bindings": {
            "data/active/catalog.json": sha256_file(root / "data/active/catalog.json"),
            "configs/active_alpha_research_ladder.json": sha256_file(
                root / "configs/active_alpha_research_ladder.json"
            ),
            topology_report.relative_to(root).as_posix(): sha256_file(topology_report),
        },
        "payload_safety": {
            "dbn_or_parquet_payload_opened": False,
            "historical_rows_read": False,
            "year_2025_or_2026_payload_opened": False,
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def main() -> int:
    plan = build_plan()
    output = ROOT / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(plan) + b"\n"
    if output.exists():
        if output.read_bytes() != raw:
            raise RuntimeError("existing cleanup preparation differs from live inventory")
    else:
        with output.open("xb") as stream:
            stream.write(raw)
    print(json.dumps({"plan_id": plan["plan_id"], "state": plan["state"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
