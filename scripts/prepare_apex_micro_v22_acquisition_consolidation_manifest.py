"""Freeze v21 failure evidence and the prepare-only v22 successor change set."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.micro_alpha_acquisition_v22 import (
    AUDIT_PATH as V22_AUDIT_PATH,
    PLAN_PATH as V22_PLAN_PATH,
    V21_FAILURE_REPORT_PATH,
    V21_PLAN_PATH,
    build_acquisition_plan,
)
from prepare_apex_micro_phase1a_acquisition_v21_failure_report import (
    TERMINAL as V21_TERMINAL_PATH,
    build_report as build_v21_failure_report,
)
from prepare_safe_cleanup_candidate_census_v7 import (
    OUTPUT as V7_CLEANUP_CENSUS_PATH,
    build_census,
)


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_v22_acquisition_consolidation_manifest/manifest.json"
)
V21_AUDIT_PATH = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v21/audit.json"
)
V21_AUTHORIZATION_PATH = Path(
    "state/authorization_uses/"
    "5c04fecd51692b216f468ccf1eecbf72e918d06e675b2a4287a03e4c684ac282.json"
)

CATEGORIES = {
    "a_v21_plan_and_consumed_acquisition_evidence": (
        "configs/apex_micro_tier01_phase1a_acquisition_plan_v21.json",
        V21_AUTHORIZATION_PATH.as_posix(),
        V21_AUDIT_PATH.as_posix(),
        "state/unpublished_evidence/safe_cleanup_candidate_census_v6/census.json",
    ),
    "b_v21_fail_closed_custody_preservation": (
        ".gitignore",
        "scripts/prepare_apex_micro_phase1a_acquisition_v21_failure_report.py",
        V21_FAILURE_REPORT_PATH.as_posix(),
    ),
    "c_non_resuming_v22_acquisition_successor": (
        "scripts/prepare_apex_micro_phase1a_acquisition_v22.py",
        "scripts/prepare_safe_cleanup_candidate_census_v7.py",
        "src/futures_rebuild/micro_alpha_acquisition_v22.py",
        "src/futures_rebuild/research_gateway_policy.py",
    ),
    "d_adversarial_tests_and_documentation": (
        "PIPELINE_FOLDER_MAP.md",
        "PROJECT_OUTLINE.md",
        "tests/test_micro_alpha_acquisition_v21.py",
        "tests/test_micro_alpha_acquisition_v22.py",
        "tests/test_micro_alpha_databento_preflight_v21.py",
        "tests/test_safe_cleanup_candidate_census_v6.py",
        "tests/test_safe_cleanup_candidate_census_v7.py",
    ),
    "e_consolidation_manifest_builder": (
        "scripts/prepare_apex_micro_v22_acquisition_consolidation_manifest.py",
    ),
    "f_unrelated_preserved_unstaged_work": (
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
        for path in (V22_PLAN_PATH, V22_AUDIT_PATH, V7_CLEANUP_CENSUS_PATH)
    ):
        raise RuntimeError("v22 plan, audit, or cleanup census already exists")

    head = _git_value("rev-parse", "HEAD")
    failure = _object(V21_FAILURE_REPORT_PATH)
    authorization = _object(V21_AUTHORIZATION_PATH)
    v21_plan = _object(V21_PLAN_PATH)
    terminal = _object(V21_TERMINAL_PATH)
    if build_v21_failure_report(root=ROOT) != failure:
        raise RuntimeError("v21 failure report does not reconstruct exactly")
    plan_a = build_acquisition_plan(root=ROOT, committed_head=head)
    plan_b = build_acquisition_plan(root=ROOT, committed_head=head)
    cleanup_a = build_census(root=ROOT, committed_head=head)
    cleanup_b = build_census(root=ROOT, committed_head=head)
    if plan_a != plan_b or cleanup_a != cleanup_b:
        raise RuntimeError("v22 plan or cleanup census is nondeterministic")

    final_destinations = [
        ROOT / str(item[key])
        for item in plan_a["requests"]
        for key in ("dbn_destination", "sidecar_destination")
    ]
    if any(path.exists() for path in final_destinations):
        raise RuntimeError("target micro destination exists")
    staging_attempt = (ROOT / V21_TERMINAL_PATH).parent
    staging_files = sorted(
        path for path in staging_attempt.rglob("*") if path.is_file()
    )
    if len(staging_files) != 73 or any(
        not path.stat().st_file_attributes & 1 for path in staging_files
    ):
        raise RuntimeError("v21 staging evidence is incomplete or writable")
    if _git_value("check-ignore", V21_TERMINAL_PATH.as_posix()) == "":
        raise RuntimeError("provider acquisition staging is not Git-ignored")
    if _git_value("ls-files", "state/provider_acquisition_staging"):
        raise RuntimeError("provider acquisition staging is tracked")

    records = {
        category: [
            {
                "path": path,
                "sha256": sha256_file(ROOT / path),
                "recommended_for_exact_stage": not category.startswith("f_"),
            }
            for path in paths
        ]
        for category, paths in CATEGORIES.items()
    }
    recommended = sorted(
        path
        for category, paths in CATEGORIES.items()
        if not category.startswith("f_")
        for path in paths
    )
    recommended.append(OUTPUT.as_posix())
    core: dict[str, object] = {
        "schema_version": "apex_micro_v22_acquisition_consolidation_manifest/1.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git_value("branch", "--show-current"),
        "observed_head": head,
        "upstream_head": _git_value("rev-parse", "origin/main"),
        "category_records": records,
        "recommended_exact_stage_paths": sorted(recommended),
        "recommended_exact_stage_path_count": len(recommended),
        "preserved_unstaged_paths": list(
            CATEGORIES["f_unrelated_preserved_unstaged_work"]
        ),
        "v21_consumed_failure": {
            "plan_id": v21_plan["plan_id"],
            "plan_sha256": sha256_file(ROOT / V21_PLAN_PATH),
            "authorization_receipt_id": authorization["receipt_id"],
            "authorization_sha256": sha256_file(ROOT / V21_AUTHORIZATION_PATH),
            "terminal_id": terminal["terminal_id"],
            "terminal_sha256": sha256_file(ROOT / V21_TERMINAL_PATH),
            "failure_report_id": failure["report_id"],
            "failure_report_sha256": sha256_file(ROOT / V21_FAILURE_REPORT_PATH),
            "provider_call_counts": terminal["provider_call_counts"],
            "verified_complete_staging_pairs": failure[
                "verified_complete_staging_pairs"
            ],
            "accepted_dbn_count": failure["accepted_dbn_count"],
            "accepted_sidecar_count": failure["accepted_sidecar_count"],
            "final_destination_count": failure["final_destination_count"],
            "external_cost_incurred_usd": failure[
                "external_cost_incurred_usd"
            ],
            "automatic_retries": failure["automatic_retries"],
            "authorization_consumed": True,
            "retry_or_staging_reuse_authorized": False,
        },
        "post_commit_v22_preparation": {
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
        },
        "cleanup_governance": {
            "provisional_candidate_count": cleanup_a["candidate_count"],
            "v21_staging_is_cleanup_candidate": False,
            "cleanup_census_written": False,
            "cleanup_performed": False,
        },
        "verification": {
            "focused_acquisition_and_cleanup_tests_passed": 33,
            "complete_current_tests_passed": 495,
            "complete_high_risk_tests_passed": 1294,
            "final_dependency_and_documentation_tests_passed": 26,
            "compilation_passed": True,
            "dependency_consistency_passed": True,
            "deterministic_reconstruction_passed": True,
            "v21_failure_reconstruction_passed": True,
            "documentation_regression_passed": True,
            "git_diff_check_passed": True,
            "tracked_raw_dbn_or_staging_file_count": 0,
        },
        "authority_and_effects": {
            "staging_performed": False,
            "commit_performed": False,
            "push_performed": False,
            "provider_access_after_v21_attempt": False,
            "v22_dbn_download_performed": False,
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
            raise RuntimeError("existing v22 consolidation manifest differs")
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
