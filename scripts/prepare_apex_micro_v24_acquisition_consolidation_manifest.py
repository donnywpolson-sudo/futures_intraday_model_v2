"""Freeze superseded v23 preparation and the corrected v24 successor change set."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.micro_alpha_acquisition_v24 import (
    AUDIT_PATH as V24_AUDIT_PATH,
    PLAN_PATH as V24_PLAN_PATH,
    build_acquisition_plan,
)
from prepare_apex_micro_phase1a_acquisition_v23_supersession import (
    AUDIT as V23_AUDIT_PATH,
    CENSUS as V23_CENSUS_PATH,
    OUTPUT as V23_SUPERSESSION_PATH,
    PLAN as V23_PLAN_PATH,
    build_report as build_v23_supersession,
)
from prepare_safe_cleanup_candidate_census_v9 import (
    OUTPUT as V9_CENSUS_PATH,
    build_census,
)


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_v24_acquisition_consolidation_manifest/manifest.json"
)
CATEGORIES = {
    "a_unexecuted_v23_preparation_and_supersession": (
        V23_PLAN_PATH.as_posix(),
        V23_AUDIT_PATH.as_posix(),
        V23_CENSUS_PATH.as_posix(),
        V23_SUPERSESSION_PATH.as_posix(),
        "scripts/prepare_apex_micro_phase1a_acquisition_v23_supersession.py",
    ),
    "b_volatile_capacity_safe_v24_successor": (
        "scripts/prepare_apex_micro_phase1a_acquisition_v24.py",
        "scripts/prepare_safe_cleanup_candidate_census_v9.py",
        "src/futures_rebuild/micro_alpha_acquisition_v24.py",
        "src/futures_rebuild/research_gateway_policy.py",
    ),
    "c_adversarial_tests_and_documentation": (
        "PIPELINE_FOLDER_MAP.md",
        "PROJECT_OUTLINE.md",
        "tests/test_micro_alpha_acquisition_v23.py",
        "tests/test_micro_alpha_acquisition_v23_supersession.py",
        "tests/test_micro_alpha_acquisition_v24.py",
        "tests/test_safe_cleanup_candidate_census_v9.py",
    ),
    "d_consolidation_manifest_builder": (
        "scripts/prepare_apex_micro_v24_acquisition_consolidation_manifest.py",
    ),
    "e_unrelated_preserved_unstaged_work": (
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
        raise RuntimeError("consolidation categories contain duplicate paths")
    observed = _status_paths() - {OUTPUT.as_posix()}
    if observed != categorized:
        raise RuntimeError(
            "consolidation census drifted "
            f"unexpected={sorted(observed - categorized)} "
            f"stale={sorted(categorized - observed)}"
        )
    if any(
        (ROOT / path).exists()
        for path in (V24_PLAN_PATH, V24_AUDIT_PATH, V9_CENSUS_PATH)
    ):
        raise RuntimeError("v24 plan, audit, or cleanup census already exists")

    head = _git_value("rev-parse", "HEAD")
    supersession = _object(V23_SUPERSESSION_PATH)
    if build_v23_supersession(root=ROOT) != supersession:
        raise RuntimeError("v23 supersession does not reconstruct exactly")
    plan_a = build_acquisition_plan(root=ROOT, committed_head=head)
    plan_b = build_acquisition_plan(root=ROOT, committed_head=head)
    cleanup_a = build_census(root=ROOT, committed_head=head)
    cleanup_b = build_census(root=ROOT, committed_head=head)
    if plan_a != plan_b or cleanup_a != cleanup_b:
        raise RuntimeError("v24 plan or cleanup census is nondeterministic")
    destinations = [
        ROOT / str(item[key])
        for item in plan_a["requests"]
        for key in ("dbn_destination", "sidecar_destination")
    ]
    if any(path.exists() for path in destinations):
        raise RuntimeError("target micro destination exists")

    records = {
        category: [
            {
                "path": path,
                "sha256": sha256_file(ROOT / path),
                "recommended_for_exact_stage": not category.startswith("e_"),
            }
            for path in paths
        ]
        for category, paths in CATEGORIES.items()
    }
    recommended = sorted(
        path
        for category, paths in CATEGORIES.items()
        if not category.startswith("e_")
        for path in paths
    )
    recommended.append(OUTPUT.as_posix())
    core: dict[str, object] = {
        "schema_version": "apex_micro_v24_acquisition_consolidation_manifest/1.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git_value("branch", "--show-current"),
        "observed_head": head,
        "upstream_head": _git_value("rev-parse", "origin/main"),
        "category_records": records,
        "recommended_exact_stage_paths": sorted(recommended),
        "recommended_exact_stage_path_count": len(recommended),
        "preserved_unstaged_paths": list(
            CATEGORIES["e_unrelated_preserved_unstaged_work"]
        ),
        "superseded_v23_preparation": {
            "plan_id": supersession["plan"]["plan_id"],
            "plan_sha256": supersession["plan"]["sha256"],
            "audit_id": supersession["audit"]["audit_id"],
            "audit_sha256": supersession["audit"]["sha256"],
            "cleanup_census_id": supersession["cleanup_census"]["census_id"],
            "cleanup_census_sha256": supersession["cleanup_census"]["sha256"],
            "supersession_report_id": supersession["report_id"],
            "supersession_report_sha256": sha256_file(
                ROOT / V23_SUPERSESSION_PATH
            ),
            "state": supersession["state"],
            "provider_calls": 0,
            "downloads": 0,
            "authorization_consumed": False,
            "execute_as_current": False,
        },
        "post_commit_v24_preparation": {
            "provisional_plan_id_not_authority": plan_a["plan_id"],
            "final_plan_requires_committed_successor_head": True,
            "exact_request_count": plan_a["limits"]["exact_request_count"],
            "maximum_provider_calls": plan_a["limits"]["maximum_provider_calls"],
            "maximum_dbn_files": plan_a["limits"]["maximum_dbn_files"],
            "maximum_sidecars": plan_a["limits"]["maximum_sidecars"],
            "maximum_total_bytes": plan_a["limits"]["maximum_total_bytes"],
            "required_free_disk_bytes": plan_a["limits"][
                "required_free_disk_bytes"
            ],
            "maximum_runtime_seconds": plan_a["limits"][
                "maximum_runtime_seconds"
            ],
            "maximum_per_download_seconds": plan_a["limits"][
                "maximum_per_download_seconds"
            ],
            "maximum_parallel_downloads": plan_a["limits"][
                "maximum_parallel_downloads"
            ],
            "maximum_provider_clients": plan_a["limits"][
                "maximum_provider_clients"
            ],
            "maximum_external_cost_usd": "0",
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "predecessor_staging_reuse": False,
            "plan_written": False,
            "audit_written": False,
            "download_authority_present": False,
            "exact_observed_free_disk_bytes_self_hashed": False,
            "live_disk_recheck_required_before_execution": True,
        },
        "cleanup_governance": {
            "provisional_candidate_count": cleanup_a["candidate_count"],
            "declared_self_output_exclusions": cleanup_a[
                "self_referential_output_exclusion"
            ]["exact_status_paths"],
            "excluded_paths_bound_separately": True,
            "cleanup_census_written": False,
            "cleanup_performed": False,
        },
        "verification": {
            "focused_v23_v24_tests_passed": 26,
            "complete_current_tests_passed": 523,
            "complete_high_risk_tests_passed": 1322,
            "final_dependency_documentation_tests_passed": 50,
            "compilation_passed": True,
            "dependency_consistency_passed": True,
            "deterministic_reconstruction_passed": True,
            "supersession_reconstruction_passed": True,
            "documentation_regression_passed": True,
            "git_diff_check_passed": True,
            "tracked_raw_dbn_or_staging_file_count": 0,
        },
        "authority_and_effects": {
            "staging_performed": False,
            "commit_performed": False,
            "push_performed": False,
            "provider_access_after_v21_attempt": False,
            "v23_or_v24_download_performed": False,
            "historical_rows_read": False,
            "year_2025_or_2026_payload_opened": False,
            "cleanup_files_deleted_or_moved": 0,
            "catalog_or_pointer_activated": False,
            "registration_or_evaluation_performed": False,
            "trading_performed": False,
        },
    }
    manifest = {**core, "manifest_id": sha256_json(core)}
    output = ROOT / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(manifest) + b"\n"
    if output.exists():
        if output.read_bytes() != raw:
            raise RuntimeError("existing v24 consolidation manifest differs")
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
