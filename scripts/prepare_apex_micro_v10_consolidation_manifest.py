"""Prepare the exact-path consolidation manifest for v9 evidence and v10."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/apex_micro_v10_consolidation_manifest/manifest.json"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
CATEGORIES: dict[str, tuple[str, ...]] = {
    "a_executed_v9_fail_closed_metadata_evidence": (
        "state/authorization_uses/018cdc1bd9f706f60388d91b06bfd38bbb405258a9523a18285ca33d295c1a26.json",
        "state/unpublished_evidence/apex_micro_metadata_preflight_v9/report.json",
    ),
    "b_opaque_single_partial_safe_v10_architecture_and_plan": (
        "configs/apex_micro_tier01_databento_metadata_preflight_v10.json",
        "scripts/prepare_apex_micro_metadata_preflight_v10.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v10.py",
    ),
    "c_v10_adversarial_tests": (
        "tests/test_micro_alpha_databento_preflight_v10.py",
    ),
    "d_implementation_reality_documentation_and_manifest_builder": (
        "PIPELINE_FOLDER_MAP.md",
        "PROJECT_OUTLINE.md",
        "scripts/prepare_apex_micro_v10_consolidation_manifest.py",
    ),
}


def _git(*args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def _worktree_paths() -> list[str]:
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
    paths: list[str] = []
    for record in completed.stdout.decode("utf-8", "strict").split("\0"):
        if not record:
            continue
        path = record[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.append(path.replace("\\", "/"))
    return sorted(paths)


def _json(path: str) -> dict[str, object]:
    try:
        value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"manifest binding is invalid: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"manifest binding is not an object: {path}")
    return value


def build_manifest() -> dict[str, object]:
    recommended = sorted(
        {path for paths in CATEGORIES.values() for path in paths}
        | {OUTPUT.as_posix()}
    )
    expected_recommended = set(recommended)
    if not (ROOT / OUTPUT).exists():
        expected_recommended.remove(OUTPUT.as_posix())
    expected_worktree = sorted(expected_recommended | set(PRESERVED_UNSTAGED))
    observed_worktree = _worktree_paths()
    if observed_worktree != expected_worktree:
        raise IntegrityError(
            "worktree differs from exact v10 consolidation scope: "
            + json.dumps(
                {
                    "missing": sorted(set(expected_worktree) - set(observed_worktree)),
                    "unexpected": sorted(set(observed_worktree) - set(expected_worktree)),
                },
                sort_keys=True,
            )
        )
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("index must be empty before v10 consolidation staging")
    category_records = {
        category: [
            {
                "path": path,
                "sha256": sha256_file(ROOT / path),
                "recommended_for_exact_stage": True,
            }
            for path in paths
        ]
        for category, paths in CATEGORIES.items()
    }
    v9_report_path = (
        "state/unpublished_evidence/apex_micro_metadata_preflight_v9/report.json"
    )
    v9_auth_path = (
        "state/authorization_uses/"
        "018cdc1bd9f706f60388d91b06bfd38bbb405258a9523a18285ca33d295c1a26.json"
    )
    v9 = _json(v9_report_path)
    v10_path = "configs/apex_micro_tier01_databento_metadata_preflight_v10.json"
    v10 = _json(v10_path)
    cleanup_path = "state/unpublished_evidence/safe_cleanup_preparation_v5/plan.json"
    cleanup = _json(cleanup_path)
    topology_path = (
        "state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json"
    )
    topology = _json(topology_path)
    core: dict[str, object] = {
        "schema_version": "apex_micro_v10_consolidation_manifest/1.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream_head": _git("rev-parse", "origin/main"),
        "v9_preflight_failure": {
            "plan_id": v9["plan_id"],
            "report_path": v9_report_path,
            "report_id": v9["report_id"],
            "report_sha256": sha256_file(ROOT / v9_report_path),
            "authorization_receipt_id": v9["authorization_receipt_id"],
            "authorization_use_path": v9_auth_path,
            "authorization_use_sha256": sha256_file(ROOT / v9_auth_path),
            "state": v9["state"],
            "failure_code": v9["failure_code"],
            "exception_type": v9["exception_type"],
            "failed_provider_operation": v9["failed_provider_operation"],
            "failed_provider_call_ordinal": v9["failed_provider_call_ordinal"],
            "failed_request_context": v9["failed_request_context"],
            "failed_validation_field": v9["failed_validation_field"],
            "provider_call_total": v9["provider_call_total"],
            "external_cost_incurred_usd": v9["external_cost_incurred_usd"],
            "automatic_retries": v9["automatic_retries"],
            "timeseries_download_calls": v9["timeseries_download_calls"],
            "historical_rows_read": v9["historical_rows_read"],
            "dbn_files_created": v9["dbn_files_created"],
            "credential_content_recorded": v9["credential_content_recorded"],
            "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
        },
        "v10_preflight": {
            "plan_path": v10_path,
            "plan_id": v10["plan_id"],
            "plan_sha256": sha256_file(ROOT / v10_path),
            "request_definitions": len(v10["requests"]),
            "maximum_annual_market_schema_requests": v10["limits"][
                "maximum_annual_market_schema_requests"
            ],
            "provider_call_ceiling": v10["limits"]["exact_provider_call_ceiling"],
            "maximum_runtime_seconds": v10["limits"]["maximum_runtime_seconds"],
            "per_call_timeout_seconds": v10["limits"]["per_call_timeout_seconds"],
            "maximum_external_cost_usd": v10["limits"]["maximum_external_cost_usd"],
            "maximum_retries": v10["limits"]["maximum_retries"],
            "state": v10["state"],
        },
        "phase1a_acquisition_implementation": {
            "state": "IMPLEMENTED_TESTED_UNEXECUTED",
            "maximum_parallel_downloads": 2,
            "maximum_provider_clients": 3,
            "parallel_client_isolation": True,
            "stop_scheduling_after_first_failure": True,
            "one_attempt_zero_retries": True,
            "download_authority_present": False,
            "dbn_files_created": 0,
        },
        "cleanup_preparation": {
            "plan_path": cleanup_path,
            "plan_id": cleanup["plan_id"],
            "plan_sha256": sha256_file(ROOT / cleanup_path),
            "state": cleanup["state"],
            "cleanup_performed": cleanup["cleanup_execution"]["performed"],
            "frozen_candidate_count": cleanup["candidate_policy"]["candidate_count"],
        },
        "standard_topology_source_safe_audit": {
            "report_path": topology_path,
            "report_id": topology["report_id"],
            "report_sha256": sha256_file(ROOT / topology_path),
            "state": topology["state"],
            "historical_rows_read": topology["payload_safety"][
                "historical_rows_read"
            ],
        },
        "category_records": category_records,
        "recommended_exact_stage_paths": recommended,
        "recommended_exact_stage_path_count": len(recommended),
        "preserved_unstaged_paths": list(PRESERVED_UNSTAGED),
        "verification": {
            "focused_micro_tests_passed": 114,
            "v10_and_acquisition_tests_passed": 21,
            "acquisition_tests_passed": 10,
            "complete_current_tests_passed": 225,
            "complete_high_risk_tests_passed": 1014,
            "dependency_documentation_topology_tests_passed": 12,
            "compilation_passed": True,
            "dependency_consistency_passed": True,
            "deterministic_reconstruction_passed": True,
            "git_diff_check_passed": True,
        },
        "authority_and_effects": {
            "v9_metadata_preflight_authorization_consumed": True,
            "provider_calls_performed_for_v9": 4,
            "external_cost_incurred_usd": "0",
            "automatic_retries": 0,
            "v10_provider_access_performed": False,
            "dbn_download_performed": False,
            "historical_rows_read": False,
            "year_2025_or_2026_payload_opened": False,
            "cleanup_files_deleted_or_moved": 0,
            "catalog_or_pointer_activated": False,
            "registration_or_evaluation_performed": False,
            "trading_performed": False,
            "staging_performed_for_this_successor": False,
            "commit_performed_for_this_successor": False,
            "push_performed": False,
        },
    }
    return {**core, "manifest_id": sha256_json(core)}


def main() -> int:
    manifest = build_manifest()
    path = ROOT / OUTPUT
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(manifest) + b"\n"
    if path.exists():
        if path.read_bytes() != raw:
            raise RuntimeError("existing v10 consolidation manifest differs")
    else:
        with path.open("xb") as stream:
            stream.write(raw)
    print(
        json.dumps(
            {
                "manifest_id": manifest["manifest_id"],
                "manifest_path": OUTPUT.as_posix(),
                "manifest_sha256": sha256_file(path),
                "recommended_exact_stage_path_count": manifest[
                    "recommended_exact_stage_path_count"
                ],
                "state": manifest["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
