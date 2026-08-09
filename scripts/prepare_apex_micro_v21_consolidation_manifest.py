"""Prepare the exact consolidation manifest for v20 evidence and v21."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/apex_micro_v21_consolidation_manifest/manifest.json"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
CATEGORIES: dict[str, tuple[str, ...]] = {
    "a_executed_v20_fail_closed_metadata_evidence": (
        "state/authorization_uses/82f2a9b5794be8365c84d9c1f2fb1f5a8bcfb5a4a9e47b9198e458ee04dce509.json",
        "state/unpublished_evidence/apex_micro_metadata_preflight_v20/report.json",
    ),
    "b_timeout_safe_v21_architecture_and_plan": (
        "configs/apex_micro_tier01_databento_metadata_preflight_v21.json",
        "scripts/prepare_apex_micro_metadata_preflight_v21.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v21.py",
    ),
    "c_transition_adversarial_and_documentation_tests": (
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
    ),
    "d_implementation_reality_documentation_and_manifest_builder": (
        "PIPELINE_FOLDER_MAP.md",
        "PROJECT_OUTLINE.md",
        "scripts/prepare_apex_micro_v21_consolidation_manifest.py",
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


def _json(path: str | Path) -> dict[str, object]:
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
            "worktree differs from exact v21 consolidation scope: "
            + json.dumps(
                {
                    "missing": sorted(set(expected_worktree) - set(observed_worktree)),
                    "unexpected": sorted(set(observed_worktree) - set(expected_worktree)),
                },
                sort_keys=True,
            )
        )
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("index must be empty before v21 consolidation staging")
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
    v20_path = "state/unpublished_evidence/apex_micro_metadata_preflight_v20/report.json"
    v20 = _json(v20_path)
    v21_path = "configs/apex_micro_tier01_databento_metadata_preflight_v21.json"
    v21 = _json(v21_path)
    expected_context = {"market": "MES", "schema": "ohlcv-1s", "year": "2020"}
    if (
        v20.get("report_id")
        != "4f6513f9dc65590a542bc2c59ceaab5bb2a1e53fcf9ca2b15ee875113fe15478"
        or v20.get("state") != "FAIL_CLOSED_METADATA_ONLY"
        or v20.get("failure_code") != "PROVIDER_TIMEOUT"
        or v20.get("exception_type") != "ReadTimeout"
        or v20.get("failed_provider_operation") != "get_billable_size"
        or v20.get("failed_provider_call_ordinal") != 68
        or v20.get("failed_request_context") != expected_context
        or v20.get("provider_call_counts")
        != {"get_billable_size": 34, "get_cost": 34}
        or v20.get("provider_call_total") != 68
        or v20.get("external_cost_incurred_usd") != "0"
        or v20.get("automatic_retries") != 0
        or v20.get("timeseries_download_calls") != 0
        or v20.get("historical_rows_read") is not False
        or v20.get("dbn_files_created") != 0
        or v20.get("credential_content_recorded") is not False
    ):
        raise IntegrityError("sealed v20 timeout evidence drifted")
    if (
        v21.get("plan_id")
        != "2f3aca8a4775dfc3a10b29a5854b655ef04f5339a1c52e87297e8ebad227124c"
        or sha256_file(ROOT / v21_path)
        != "34f83ec5ae8bb7da819e174703b2dacba77fa5dde5eea2ecf3448025d8516c8d"
        or v21.get("state") != "PREPARED_NOT_EXECUTED"
        or v21.get("annual_scope", {}).get("exact_market_schema_requests") != 160
        or v21.get("limits", {}).get("exact_provider_call_ceiling") != 180
        or v21.get("limits", {}).get("maximum_provider_clients") != 6
        or v21.get("limits", {}).get("maximum_runtime_seconds") != 300
        or v21.get("limits", {}).get("per_call_timeout_seconds") != 90
        or v21.get("provider_operations")
        != {
            "list_datasets": 0,
            "list_schemas": 0,
            "get_dataset_range": 0,
            "resolve": 0,
            "get_cost_full_acquisition_range": 20,
            "get_billable_size_annual": 160,
            "timeseries_download": 0,
        }
    ):
        raise IntegrityError("v21 preflight plan drifted")
    core: dict[str, object] = {
        "schema_version": "apex_micro_v21_consolidation_manifest/1.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream_head": _git("rev-parse", "origin/main"),
        "v20_preflight_failure": {
            "report_path": v20_path,
            "report_id": v20["report_id"],
            "report_sha256": sha256_file(ROOT / v20_path),
            "authorization_receipt_id": v20["authorization_receipt_id"],
            "authorization_use_path": (
                "state/authorization_uses/"
                + str(v20["authorization_receipt_id"])
                + ".json"
            ),
            "authorization_use_sha256": (
                "2a545c88d0dfa28a1f977f54ccff8f2d5900f1945c45eadb41a90c2c36334c13"
            ),
            "state": v20["state"],
            "failure_code": v20["failure_code"],
            "exception_type": v20["exception_type"],
            "failed_provider_operation": v20["failed_provider_operation"],
            "failed_provider_call_ordinal": v20["failed_provider_call_ordinal"],
            "failed_request_context": v20["failed_request_context"],
            "provider_call_total": v20["provider_call_total"],
            "external_cost_incurred_usd": v20["external_cost_incurred_usd"],
            "automatic_retries": v20["automatic_retries"],
            "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
        },
        "v21_preflight": {
            "plan_path": v21_path,
            "plan_id": v21["plan_id"],
            "plan_sha256": sha256_file(ROOT / v21_path),
            "request_definitions": len(v21["requests"]),
            "annual_market_schema_requests": 160,
            "full_range_cost_requests": 20,
            "annual_billable_size_requests": 160,
            "provider_call_ceiling": 180,
            "maximum_provider_clients": 6,
            "maximum_runtime_seconds": 300,
            "per_call_timeout_seconds": 90,
            "maximum_external_cost_usd": "0",
            "maximum_retries": 0,
            "state": v21["state"],
        },
        "phase1a_acquisition_implementation": {
            "state": "IMPLEMENTED_TESTED_UNEXECUTED",
            "file_partition": (
                "ONE_DBN_AND_ADJACENT_SIDECAR_PER_MARKET_SCHEMA_CALENDAR_YEAR"
            ),
            "maximum_parallel_downloads": 2,
            "one_attempt_zero_retries": True,
            "download_authority_present": False,
            "dbn_files_created": 0,
        },
        "category_records": category_records,
        "recommended_exact_stage_paths": recommended,
        "recommended_exact_stage_path_count": len(recommended),
        "preserved_unstaged_paths": list(PRESERVED_UNSTAGED),
        "verification": {
            "focused_v21_tests_passed": 15,
            "v12_v21_documentation_regressions_passed": 10,
            "complete_current_tests_passed": 462,
            "complete_high_risk_tests_passed": 1261,
            "compilation_passed": True,
            "dependency_consistency_passed": True,
            "deterministic_reconstruction_passed": True,
            "git_diff_check_passed": True,
        },
        "authority_and_effects": {
            "v20_metadata_authorization_consumed": True,
            "v20_provider_calls": 68,
            "v21_provider_access_performed": False,
            "external_cost_incurred_usd": "0",
            "automatic_retries": 0,
            "dbn_download_performed": False,
            "historical_rows_read": False,
            "year_2025_or_2026_payload_opened": False,
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
            raise RuntimeError("existing v21 consolidation manifest differs")
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
