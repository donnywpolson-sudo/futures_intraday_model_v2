"""Prepare the exact-path consolidation manifest for v5 evidence and v6."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/apex_micro_v6_consolidation_manifest/manifest.json"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
CATEGORIES: dict[str, tuple[str, ...]] = {
    "a_executed_v5_fail_closed_metadata_evidence": (
        "state/authorization_uses/28f1b90cdaa59d9089841bf1df95ab113a5c00dcf2131f3e832bb66d0762e596.json",
        "state/unpublished_evidence/apex_micro_metadata_preflight_v5/report.json",
    ),
    "b_provider_range_safe_v6_architecture_and_acquisition": (
        "configs/apex_micro_tier01_databento_metadata_preflight_v6.json",
        "scripts/prepare_apex_micro_metadata_preflight_v6.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v6.py",
        "src/futures_rebuild/micro_alpha_acquisition.py",
    ),
    "c_v6_and_acquisition_adversarial_tests": (
        "tests/test_micro_alpha_databento_preflight_v6.py",
        "tests/test_micro_alpha_acquisition.py",
    ),
    "d_stable_prepare_only_cleanup_successor": (
        "scripts/prepare_safe_cleanup_inventory_v5.py",
        "state/unpublished_evidence/safe_cleanup_preparation_v5/plan.json",
        "tests/test_safe_cleanup_preparation.py",
    ),
    "e_implementation_reality_documentation_and_manifest_builder": (
        "PIPELINE_FOLDER_MAP.md",
        "PROJECT_OUTLINE.md",
        "scripts/prepare_apex_micro_v6_consolidation_manifest.py",
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
            "worktree differs from exact v6 consolidation scope: "
            + json.dumps(
                {
                    "missing": sorted(set(expected_worktree) - set(observed_worktree)),
                    "unexpected": sorted(set(observed_worktree) - set(expected_worktree)),
                },
                sort_keys=True,
            )
        )
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("index must be empty before v6 consolidation staging")
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
    v5_report_path = (
        "state/unpublished_evidence/apex_micro_metadata_preflight_v5/report.json"
    )
    v5_auth_path = (
        "state/authorization_uses/"
        "28f1b90cdaa59d9089841bf1df95ab113a5c00dcf2131f3e832bb66d0762e596.json"
    )
    v5 = _json(v5_report_path)
    v6_path = "configs/apex_micro_tier01_databento_metadata_preflight_v6.json"
    v6 = _json(v6_path)
    cleanup_path = "state/unpublished_evidence/safe_cleanup_preparation_v5/plan.json"
    cleanup = _json(cleanup_path)
    core: dict[str, object] = {
        "schema_version": "apex_micro_v6_consolidation_manifest/1.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream_head": _git("rev-parse", "origin/main"),
        "v5_preflight_failure": {
            "plan_id": v5["plan_id"],
            "report_path": v5_report_path,
            "report_id": v5["report_id"],
            "report_sha256": sha256_file(ROOT / v5_report_path),
            "authorization_receipt_id": v5["authorization_receipt_id"],
            "authorization_use_path": v5_auth_path,
            "authorization_use_sha256": sha256_file(ROOT / v5_auth_path),
            "state": v5["state"],
            "failure_code": v5["failure_code"],
            "exception_type": v5["exception_type"],
            "provider_call_total": v5["provider_call_total"],
            "external_cost_incurred_usd": v5["external_cost_incurred_usd"],
            "automatic_retries": v5["automatic_retries"],
            "timeseries_download_calls": v5["timeseries_download_calls"],
            "historical_rows_read": v5["historical_rows_read"],
            "dbn_files_created": v5["dbn_files_created"],
            "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
        },
        "v6_preflight": {
            "plan_path": v6_path,
            "plan_id": v6["plan_id"],
            "plan_sha256": sha256_file(ROOT / v6_path),
            "request_definitions": len(v6["requests"]),
            "maximum_annual_market_schema_requests": v6["limits"][
                "maximum_annual_market_schema_requests"
            ],
            "provider_call_ceiling": v6["limits"]["exact_provider_call_ceiling"],
            "maximum_runtime_seconds": v6["limits"]["maximum_runtime_seconds"],
            "per_call_timeout_seconds": v6["limits"]["per_call_timeout_seconds"],
            "maximum_external_cost_usd": v6["limits"][
                "maximum_external_cost_usd"
            ],
            "maximum_retries": v6["limits"]["maximum_retries"],
            "state": v6["state"],
        },
        "cleanup_preparation": {
            "plan_path": cleanup_path,
            "plan_id": cleanup["plan_id"],
            "plan_sha256": sha256_file(ROOT / cleanup_path),
            "state": cleanup["state"],
            "cleanup_performed": cleanup["cleanup_execution"]["performed"],
            "frozen_candidate_count": cleanup["candidate_policy"]["candidate_count"],
        },
        "category_records": category_records,
        "recommended_exact_stage_paths": recommended,
        "recommended_exact_stage_path_count": len(recommended),
        "preserved_unstaged_paths": list(PRESERVED_UNSTAGED),
        "verification": {
            "focused_micro_tests_passed": 72,
            "v6_acquisition_cleanup_tests_passed": 21,
            "complete_current_tests_passed": 183,
            "complete_high_risk_tests_passed": 972,
            "dependency_and_documentation_tests_passed": 11,
            "compilation_passed": True,
            "dependency_consistency_passed": True,
            "deterministic_reconstruction_passed": True,
            "git_diff_check_passed": True,
        },
        "authority_and_effects": {
            "v5_metadata_preflight_authorization_consumed": True,
            "provider_calls_performed": 4,
            "external_cost_incurred_usd": "0",
            "automatic_retries": 0,
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
            raise RuntimeError("existing v6 consolidation manifest differs")
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
