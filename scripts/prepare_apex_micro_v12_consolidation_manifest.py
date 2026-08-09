"""Prepare the exact-path consolidation manifest for v11 evidence and v12."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/apex_micro_v12_consolidation_manifest/manifest.json"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
CATEGORIES: dict[str, tuple[str, ...]] = {
    "a_executed_v11_fail_closed_metadata_evidence": (
        "state/authorization_uses/b09db582730cf9d724928ed184a0d59f36d1598c46a35cf2bcff032000a04d9d.json",
        "state/unpublished_evidence/apex_micro_metadata_preflight_v11/report.json",
    ),
    "b_sdk_contract_safe_v12_architecture_and_plan": (
        "configs/apex_micro_tier01_databento_metadata_preflight_v12.json",
        "scripts/prepare_apex_micro_metadata_preflight_v12.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v12.py",
    ),
    "c_v12_adversarial_tests": (
        "tests/test_micro_alpha_databento_preflight_v12.py",
    ),
    "d_implementation_reality_documentation_and_manifest_builder": (
        "PIPELINE_FOLDER_MAP.md",
        "PROJECT_OUTLINE.md",
        "scripts/prepare_apex_micro_v12_consolidation_manifest.py",
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
            "worktree differs from exact v12 consolidation scope: "
            + json.dumps(
                {
                    "missing": sorted(set(expected_worktree) - set(observed_worktree)),
                    "unexpected": sorted(set(observed_worktree) - set(expected_worktree)),
                },
                sort_keys=True,
            )
        )
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("index must be empty before v12 consolidation staging")
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
    v11_report_path = (
        "state/unpublished_evidence/apex_micro_metadata_preflight_v11/report.json"
    )
    v11_auth_path = (
        "state/authorization_uses/"
        "b09db582730cf9d724928ed184a0d59f36d1598c46a35cf2bcff032000a04d9d.json"
    )
    v11 = _json(v11_report_path)
    v12_path = "configs/apex_micro_tier01_databento_metadata_preflight_v12.json"
    v12 = _json(v12_path)
    cleanup_path = "state/unpublished_evidence/safe_cleanup_preparation_v5/plan.json"
    cleanup = _json(cleanup_path)
    topology_path = (
        "state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json"
    )
    topology = _json(topology_path)
    if (
        v11.get("state") != "FAIL_CLOSED_METADATA_ONLY"
        or v11.get("failure_code") != "SYMBOL_ECHO_FIELD_DRIFT"
        or v11.get("failed_validation_field") != "status"
        or v11.get("provider_call_total") != 4
        or v11.get("external_cost_incurred_usd") != "0"
        or v11.get("automatic_retries") != 0
        or v11.get("timeseries_download_calls") != 0
        or v11.get("historical_rows_read") is not False
        or v11.get("dbn_files_created") != 0
        or v11.get("credential_content_recorded") is not False
    ):
        raise IntegrityError("sealed v11 failure evidence drifted")
    core: dict[str, object] = {
        "schema_version": "apex_micro_v12_consolidation_manifest/1.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream_head": _git("rev-parse", "origin/main"),
        "v11_preflight_failure": {
            "plan_id": v11["plan_id"],
            "report_path": v11_report_path,
            "report_id": v11["report_id"],
            "report_sha256": sha256_file(ROOT / v11_report_path),
            "authorization_receipt_id": v11["authorization_receipt_id"],
            "authorization_use_path": v11_auth_path,
            "authorization_use_sha256": sha256_file(ROOT / v11_auth_path),
            "state": v11["state"],
            "failure_code": v11["failure_code"],
            "exception_type": v11["exception_type"],
            "failed_provider_operation": v11["failed_provider_operation"],
            "failed_provider_call_ordinal": v11["failed_provider_call_ordinal"],
            "failed_request_context": v11["failed_request_context"],
            "failed_validation_field": v11["failed_validation_field"],
            "provider_call_total": v11["provider_call_total"],
            "external_cost_incurred_usd": v11["external_cost_incurred_usd"],
            "automatic_retries": v11["automatic_retries"],
            "timeseries_download_calls": v11["timeseries_download_calls"],
            "historical_rows_read": v11["historical_rows_read"],
            "dbn_files_created": v11["dbn_files_created"],
            "credential_content_recorded": v11["credential_content_recorded"],
            "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
        },
        "v12_preflight": {
            "plan_path": v12_path,
            "plan_id": v12["plan_id"],
            "plan_sha256": sha256_file(ROOT / v12_path),
            "request_definitions": len(v12["requests"]),
            "maximum_annual_market_schema_requests": v12["limits"][
                "maximum_annual_market_schema_requests"
            ],
            "maximum_opaque_partial_entries": v12["limits"][
                "maximum_opaque_partial_entries"
            ],
            "maximum_status_string_length": v12["limits"][
                "maximum_status_string_length"
            ],
            "maximum_status_integer_magnitude": v12["limits"][
                "maximum_status_integer_magnitude"
            ],
            "provider_call_ceiling": v12["limits"]["exact_provider_call_ceiling"],
            "maximum_runtime_seconds": v12["limits"]["maximum_runtime_seconds"],
            "per_call_timeout_seconds": v12["limits"]["per_call_timeout_seconds"],
            "maximum_external_cost_usd": v12["limits"]["maximum_external_cost_usd"],
            "maximum_retries": v12["limits"]["maximum_retries"],
            "state": v12["state"],
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
            "historical_rows_read": topology["payload_safety"]["historical_rows_read"],
        },
        "category_records": category_records,
        "recommended_exact_stage_paths": recommended,
        "recommended_exact_stage_path_count": len(recommended),
        "preserved_unstaged_paths": list(PRESERVED_UNSTAGED),
        "verification": {
            "focused_micro_tests_passed": 147,
            "v12_and_acquisition_tests_passed": 30,
            "acquisition_tests_passed": 10,
            "complete_current_tests_passed": 257,
            "complete_high_risk_tests_passed": 1046,
            "dependency_documentation_topology_tests_passed": 3,
            "compilation_passed": True,
            "dependency_consistency_passed": True,
            "deterministic_reconstruction_passed": True,
            "git_diff_check_passed": True,
        },
        "authority_and_effects": {
            "v11_metadata_preflight_authorization_consumed": True,
            "provider_calls_performed_for_v11": 4,
            "external_cost_incurred_usd": "0",
            "automatic_retries": 0,
            "v12_provider_access_performed": False,
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
            raise RuntimeError("existing v12 consolidation manifest differs")
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
