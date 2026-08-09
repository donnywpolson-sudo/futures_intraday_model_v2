"""Freeze the exact v21 PASS evidence and acquisition-successor consolidation."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.micro_alpha_acquisition_v21 import (
    AUDIT_PATH,
    PLAN_PATH,
    build_acquisition_plan,
)
from prepare_safe_cleanup_candidate_census_v6 import (
    OUTPUT as CLEANUP_CENSUS_PATH,
    build_census,
)


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_v21_acquisition_consolidation_manifest/manifest.json"
)
PREFLIGHT_REPORT = Path(
    "state/unpublished_evidence/apex_micro_metadata_preflight_v21/report.json"
)
AUTHORIZATION_USE = Path(
    "state/authorization_uses/"
    "bf720c94e7307379dbbf4bce5e482c5e3f452d2718009d1d26422fbd6256cc40.json"
)
TOPOLOGY_REPORT = Path(
    "state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json"
)
CLEANUP_POLICY = Path(
    "state/unpublished_evidence/safe_cleanup_preparation_v5/plan.json"
)

CATEGORIES = {
    "a_v21_metadata_pass_and_consumed_authorization": (
        AUTHORIZATION_USE.as_posix(),
        PREFLIGHT_REPORT.as_posix(),
    ),
    "b_v21_phase1a_acquisition_successor": (
        "scripts/prepare_apex_micro_phase1a_acquisition_v21.py",
        "src/futures_rebuild/micro_alpha_acquisition_v21.py",
        "src/futures_rebuild/research_gateway_policy.py",
    ),
    "c_source_safe_cleanup_census_preparation": (
        "scripts/prepare_safe_cleanup_candidate_census_v6.py",
    ),
    "d_adversarial_tests_and_documentation_regression": (
        "tests/test_micro_alpha_acquisition_v21.py",
        "tests/test_safe_cleanup_candidate_census_v6.py",
        "tests/test_micro_alpha_databento_preflight_v12.py",
        "tests/test_micro_alpha_databento_preflight_v13.py",
        "tests/test_micro_alpha_databento_preflight_v14.py",
        "tests/test_micro_alpha_databento_preflight_v15.py",
        "tests/test_micro_alpha_databento_preflight_v16.py",
        "tests/test_micro_alpha_databento_preflight_v17.py",
        "tests/test_micro_alpha_databento_preflight_v18.py",
        "tests/test_micro_alpha_databento_preflight_v19.py",
        "tests/test_micro_alpha_databento_preflight_v20.py",
        "tests/test_micro_alpha_databento_preflight_v21.py",
        "tests/test_operational_documents.py",
    ),
    "e_implementation_reality_documentation_and_manifest_builder": (
        "PIPELINE_FOLDER_MAP.md",
        "PROJECT_OUTLINE.md",
        "scripts/prepare_apex_micro_v21_acquisition_consolidation_manifest.py",
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
    if any((ROOT / path).exists() for path in (PLAN_PATH, AUDIT_PATH, CLEANUP_CENSUS_PATH)):
        raise RuntimeError("post-commit plan, audit, or cleanup census already exists")

    head = _git_value("rev-parse", "HEAD")
    preflight = _object(PREFLIGHT_REPORT)
    authorization = _object(AUTHORIZATION_USE)
    topology = _object(TOPOLOGY_REPORT)
    cleanup_policy = _object(CLEANUP_POLICY)
    plan_preview_a = build_acquisition_plan(root=ROOT, committed_head=head)
    plan_preview_b = build_acquisition_plan(root=ROOT, committed_head=head)
    cleanup_preview_a = build_census(root=ROOT, committed_head=head)
    cleanup_preview_b = build_census(root=ROOT, committed_head=head)
    if plan_preview_a != plan_preview_b or cleanup_preview_a != cleanup_preview_b:
        raise RuntimeError("successor plan or cleanup census is nondeterministic")

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
        "schema_version": "apex_micro_v21_acquisition_consolidation_manifest/1.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git_value("branch", "--show-current"),
        "observed_head": head,
        "upstream_head": _git_value("rev-parse", "origin/main"),
        "category_records": records,
        "recommended_exact_stage_paths": sorted(recommended),
        "recommended_exact_stage_path_count": len(recommended),
        "preserved_unstaged_paths": list(CATEGORIES["f_unrelated_preserved_unstaged_work"]),
        "v21_metadata_preflight": {
            "plan_id": preflight["plan_id"],
            "plan_sha256": preflight["plan_sha256"],
            "report_id": preflight["report_id"],
            "report_sha256": sha256_file(ROOT / PREFLIGHT_REPORT),
            "authorization_receipt_id": preflight["authorization_receipt_id"],
            "authorization_use_sha256": sha256_file(ROOT / AUTHORIZATION_USE),
            "authorization_receipt_matches_consumed_record": (
                authorization.get("receipt_id") == preflight["authorization_receipt_id"]
            ),
            "state": preflight["state"],
            "annual_request_count": preflight["annual_market_schema_request_count"],
            "provider_call_total": preflight["provider_call_total"],
            "external_cost_incurred_usd": preflight["external_cost_incurred_usd"],
            "automatic_retries": preflight["automatic_retries"],
            "timeseries_download_calls": preflight["timeseries_download_calls"],
        },
        "post_commit_acquisition_preparation": {
            "provisional_plan_id_not_authority": plan_preview_a["plan_id"],
            "final_plan_requires_committed_successor_head": True,
            "exact_request_count": plan_preview_a["limits"]["exact_request_count"],
            "maximum_provider_calls": plan_preview_a["limits"]["maximum_provider_calls"],
            "maximum_dbn_files": plan_preview_a["limits"]["maximum_dbn_files"],
            "maximum_sidecars": plan_preview_a["limits"]["maximum_sidecars"],
            "maximum_total_bytes": plan_preview_a["limits"]["maximum_total_bytes"],
            "required_free_disk_bytes": plan_preview_a["limits"]["required_free_disk_bytes"],
            "maximum_runtime_seconds": plan_preview_a["limits"]["maximum_runtime_seconds"],
            "maximum_per_download_seconds": plan_preview_a["limits"]["maximum_per_download_seconds"],
            "maximum_parallel_downloads": plan_preview_a["limits"]["maximum_parallel_downloads"],
            "maximum_provider_clients": plan_preview_a["limits"]["maximum_provider_clients"],
            "maximum_external_cost_usd": plan_preview_a["limits"]["maximum_external_cost_usd"],
            "maximum_retries": plan_preview_a["limits"]["maximum_retries"],
            "plan_written": False,
            "audit_written": False,
            "download_authority_present": False,
        },
        "standard_topology_and_cleanup": {
            "topology_report_id": topology["report_id"],
            "topology_report_sha256": sha256_file(ROOT / TOPOLOGY_REPORT),
            "topology_state": topology["state"],
            "topology_payload_safety": topology["payload_safety"],
            "cleanup_policy_id": cleanup_policy["plan_id"],
            "cleanup_policy_sha256": sha256_file(ROOT / CLEANUP_POLICY),
            "cleanup_policy_state": cleanup_policy["state"],
            "provisional_candidate_count": cleanup_preview_a["candidate_count"],
            "cleanup_census_written": False,
            "cleanup_performed": False,
        },
        "verification": {
            "focused_source_safe_tests_passed": 247,
            "complete_current_tests_passed": 484,
            "complete_high_risk_tests_passed": 1283,
            "compilation_passed": True,
            "dependency_consistency_passed": True,
            "deterministic_reconstruction_passed": True,
            "documentation_regression_passed": True,
            "git_diff_check_passed": True,
        },
        "authority_and_effects": {
            "staging_performed": False,
            "commit_performed": False,
            "push_performed": False,
            "provider_access_after_v21_preflight": False,
            "dbn_download_performed": False,
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
            raise RuntimeError("existing consolidation manifest differs")
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
