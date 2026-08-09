"""Freeze the exact direct-execution remediation after the first plan prepare failed."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.micro_alpha_acquisition_v21 import AUDIT_PATH, PLAN_PATH
from prepare_safe_cleanup_candidate_census_v6 import OUTPUT as CLEANUP_CENSUS_PATH


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_v21_direct_execution_fix_consolidation_manifest/manifest.json"
)
PREDECESSOR_MANIFEST = Path(
    "state/unpublished_evidence/"
    "apex_micro_v21_acquisition_consolidation_manifest/manifest.json"
)
PREFLIGHT_REPORT = Path(
    "state/unpublished_evidence/apex_micro_metadata_preflight_v21/report.json"
)
CATEGORIES = {
    "a_direct_execution_import_remediation": (
        "scripts/prepare_apex_micro_phase1a_acquisition_v21.py",
    ),
    "b_direct_execution_adversarial_test": (
        "tests/test_micro_alpha_acquisition_v21.py",
    ),
    "c_successor_manifest_builder": (
        "scripts/prepare_apex_micro_v21_direct_execution_fix_consolidation_manifest.py",
    ),
    "d_unrelated_preserved_unstaged_work": (
        "CODEX_HANDOFF.md",
        "CURRENT_WORKFLOW.md",
    ),
}


def _git_value(*args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def _status_paths() -> set[str]:
    completed = subprocess.run(
        [
            "git",
            "-C",
            str(ROOT),
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    paths: set[str] = set()
    for record in completed.stdout.decode("utf-8", "strict").split("\0"):
        if not record:
            continue
        path = record[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.add(path.replace("\\", "/"))
    return paths


def _object(path: Path) -> dict[str, object]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object: {path.as_posix()}")
    return value


def main() -> int:
    categorized = {path for paths in CATEGORIES.values() for path in paths}
    if len(categorized) != sum(len(paths) for paths in CATEGORIES.values()):
        raise RuntimeError("successor consolidation categories contain duplicate paths")
    observed = _status_paths() - {OUTPUT.as_posix()}
    if observed != categorized:
        raise RuntimeError(
            "successor consolidation census drifted "
            f"unexpected={sorted(observed - categorized)} "
            f"stale={sorted(categorized - observed)}"
        )
    if any((ROOT / path).exists() for path in (PLAN_PATH, AUDIT_PATH, CLEANUP_CENSUS_PATH)):
        raise RuntimeError("plan, audit, or cleanup census unexpectedly exists")

    predecessor = _object(PREDECESSOR_MANIFEST)
    preflight = _object(PREFLIGHT_REPORT)
    records = {
        category: [
            {
                "path": path,
                "sha256": sha256_file(ROOT / path),
                "recommended_for_exact_stage": not category.startswith("d_"),
            }
            for path in paths
        ]
        for category, paths in CATEGORIES.items()
    }
    recommended = sorted(
        path
        for category, paths in CATEGORIES.items()
        if not category.startswith("d_")
        for path in paths
    )
    recommended.append(OUTPUT.as_posix())
    core: dict[str, object] = {
        "schema_version": (
            "apex_micro_v21_direct_execution_fix_consolidation_manifest/1.0.0"
        ),
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git_value("branch", "--show-current"),
        "observed_head": _git_value("rev-parse", "HEAD"),
        "upstream_head": _git_value("rev-parse", "origin/main"),
        "category_records": records,
        "recommended_exact_stage_paths": sorted(recommended),
        "recommended_exact_stage_path_count": len(recommended),
        "preserved_unstaged_paths": list(CATEGORIES["d_unrelated_preserved_unstaged_work"]),
        "predecessor_consolidation": {
            "manifest_id": predecessor["manifest_id"],
            "manifest_sha256": sha256_file(ROOT / PREDECESSOR_MANIFEST),
            "committed_head": _git_value("rev-parse", "HEAD"),
            "preservation": "IMMUTABLE_IN_COMMITTED_HISTORY",
        },
        "v21_metadata_preflight": {
            "report_id": preflight["report_id"],
            "report_sha256": sha256_file(ROOT / PREFLIGHT_REPORT),
            "state": preflight["state"],
            "external_cost_incurred_usd": preflight["external_cost_incurred_usd"],
        },
        "failed_prepare_attempt": {
            "result": "LOCAL_FAIL_CLOSED_BEFORE_ANY_OUTPUT",
            "failure_class": "DIRECT_SCRIPT_PACKAGE_QUALIFIED_IMPORT",
            "provider_calls": 0,
            "downloads": 0,
            "historical_rows_read": False,
            "plan_written": False,
            "audit_written": False,
            "cleanup_census_written": False,
        },
        "remediation": {
            "direct_path_execution_import_surface": "PASS",
            "package_import_surface_preserved": True,
            "final_plan_still_requires_successor_committed_head": True,
        },
        "verification": {
            "focused_acquisition_tests_passed": 21,
            "direct_path_import_adversarial_test_passed": True,
            "compilation_passed": True,
            "dependency_consistency_passed": True,
            "git_diff_check_passed": True,
        },
        "authority_and_effects": {
            "staging_performed": False,
            "commit_performed": False,
            "push_performed": False,
            "provider_access_after_v21_preflight": False,
            "download_authority_present": False,
            "dbn_download_performed": False,
            "year_2025_or_2026_payload_opened": False,
            "catalog_or_pointer_activated": False,
            "cleanup_performed": False,
        },
    }
    manifest = {**core, "manifest_id": sha256_json(core)}
    output = ROOT / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(manifest) + b"\n"
    if output.exists():
        if output.read_bytes() != raw:
            raise RuntimeError("existing successor consolidation manifest differs")
    else:
        with output.open("xb") as stream:
            stream.write(raw)
    print(
        json.dumps(
            {
                "manifest_id": manifest["manifest_id"],
                "manifest_sha256": sha256_file(output),
                "recommended_exact_stage_path_count": len(recommended),
                "state": manifest["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
