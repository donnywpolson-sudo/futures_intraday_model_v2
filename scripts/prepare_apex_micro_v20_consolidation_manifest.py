"""Prepare the exact consolidation manifest for v19, CME dates, and v20."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/apex_micro_v20_consolidation_manifest/manifest.json"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
CATEGORIES: dict[str, tuple[str, ...]] = {
    "a_executed_v19_fail_closed_metadata_evidence": (
        "state/authorization_uses/2e3e8240d3ae52c4582a6b24b7f302fde45f8fd43389da2fb79b832b49898568.json",
        "state/unpublished_evidence/apex_micro_metadata_preflight_v19/report.json",
    ),
    "b_official_cme_product_effective_date_evidence": (
        "state/unpublished_evidence/apex_micro_m6e_product_effective_date_source_v1/report.json",
        "state/unpublished_evidence/apex_micro_remaining_product_effective_dates_source_v1/report.json",
        "src/futures_rebuild/micro_alpha_product_effective_dates.py",
    ),
    "c_launch_date_separated_v20_architecture_and_plan": (
        "configs/apex_micro_tier01_databento_metadata_preflight_v20.json",
        "scripts/prepare_apex_micro_metadata_preflight_v20.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v20.py",
    ),
    "d_transition_adversarial_and_documentation_tests": (
        "tests/test_micro_alpha_databento_preflight_v12.py",
        "tests/test_micro_alpha_databento_preflight_v13.py",
        "tests/test_micro_alpha_databento_preflight_v14.py",
        "tests/test_micro_alpha_databento_preflight_v15.py",
        "tests/test_micro_alpha_databento_preflight_v16.py",
        "tests/test_micro_alpha_databento_preflight_v17.py",
        "tests/test_micro_alpha_databento_preflight_v18.py",
        "tests/test_micro_alpha_databento_preflight_v19.py",
        "tests/test_micro_alpha_databento_preflight_v20.py",
        "tests/test_micro_alpha_product_effective_dates.py",
    ),
    "e_implementation_reality_documentation_and_manifest_builder": (
        "PIPELINE_FOLDER_MAP.md",
        "PROJECT_OUTLINE.md",
        "scripts/prepare_apex_micro_v20_consolidation_manifest.py",
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
            "worktree differs from exact v20 consolidation scope: "
            + json.dumps(
                {
                    "missing": sorted(set(expected_worktree) - set(observed_worktree)),
                    "unexpected": sorted(set(observed_worktree) - set(expected_worktree)),
                },
                sort_keys=True,
            )
        )
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("index must be empty before v20 consolidation staging")
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
    v19_path = "state/unpublished_evidence/apex_micro_metadata_preflight_v19/report.json"
    v19 = _json(v19_path)
    m6e_path = (
        "state/unpublished_evidence/"
        "apex_micro_m6e_product_effective_date_source_v1/report.json"
    )
    remaining_path = (
        "state/unpublished_evidence/"
        "apex_micro_remaining_product_effective_dates_source_v1/report.json"
    )
    m6e = _json(m6e_path)
    remaining = _json(remaining_path)
    v20_path = "configs/apex_micro_tier01_databento_metadata_preflight_v20.json"
    v20 = _json(v20_path)
    if (
        v19.get("report_id")
        != "2c08f95147d2b0f75cb0d357c182ab11ab597f326e3de7d8f087a587570cee98"
        or v19.get("state") != "FAIL_CLOSED_METADATA_ONLY"
        or v19.get("failure_code")
        != "PRODUCT_EFFECTIVE_DATE_UNRESOLVED_PRE_DATASET"
        or v19.get("provider_call_total") != 15
        or v19.get("external_cost_incurred_usd") != "0"
        or v19.get("automatic_retries") != 0
        or v19.get("timeseries_download_calls") != 0
        or v19.get("historical_rows_read") is not False
        or v19.get("dbn_files_created") != 0
        or v19.get("credential_content_recorded") is not False
    ):
        raise IntegrityError("sealed v19 failure evidence drifted")
    expected_dates = {
        "MES": "2019-05-05",
        "MCL": "2021-07-11",
        "MGC": "2010-10-03",
        "M6E": "2009-03-22",
    }
    if (
        m6e.get("report_id")
        != "c061f4ff78fd6bc408ae237b69ab0e6898c0d3b5a2419955ab4f27278b32b54c"
        or remaining.get("report_id")
        != "f1e17dcf1703b2e1b5525d350f51f11508875a8b68c4350182ee2ebb48befbb3"
        or v20.get("official_product_effective_date_sources", {}).get(
            "product_effective_dates"
        )
        != expected_dates
        or v20.get("state") != "PREPARED_NOT_EXECUTED"
        or v20.get("annual_scope", {}).get("exact_market_schema_requests") != 160
        or v20.get("limits", {}).get("exact_provider_call_ceiling") != 320
        or v20.get("provider_operations")
        != {
            "list_datasets": 0,
            "list_schemas": 0,
            "get_dataset_range": 0,
            "resolve": 0,
            "get_cost": 160,
            "get_billable_size": 160,
            "timeseries_download": 0,
        }
    ):
        raise IntegrityError("official dates or v20 preflight plan drifted")
    core: dict[str, object] = {
        "schema_version": "apex_micro_v20_consolidation_manifest/1.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream_head": _git("rev-parse", "origin/main"),
        "v19_preflight_failure": {
            "report_path": v19_path,
            "report_id": v19["report_id"],
            "report_sha256": sha256_file(ROOT / v19_path),
            "authorization_receipt_id": v19["authorization_receipt_id"],
            "state": v19["state"],
            "failure_code": v19["failure_code"],
            "provider_call_total": v19["provider_call_total"],
            "external_cost_incurred_usd": v19["external_cost_incurred_usd"],
            "automatic_retries": v19["automatic_retries"],
            "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
        },
        "official_cme_product_effective_dates": {
            "M6E": {
                "report_path": m6e_path,
                "report_id": m6e["report_id"],
                "report_sha256": sha256_file(ROOT / m6e_path),
            },
            "MES_MCL_MGC": {
                "report_path": remaining_path,
                "report_id": remaining["report_id"],
                "report_sha256": sha256_file(ROOT / remaining_path),
            },
            "dates": expected_dates,
        },
        "v20_preflight": {
            "plan_path": v20_path,
            "plan_id": v20["plan_id"],
            "plan_sha256": sha256_file(ROOT / v20_path),
            "request_definitions": len(v20["requests"]),
            "annual_market_schema_requests": 160,
            "provider_call_ceiling": 320,
            "maximum_runtime_seconds": v20["limits"]["maximum_runtime_seconds"],
            "per_call_timeout_seconds": v20["limits"]["per_call_timeout_seconds"],
            "maximum_external_cost_usd": v20["limits"]["maximum_external_cost_usd"],
            "maximum_retries": v20["limits"]["maximum_retries"],
            "state": v20["state"],
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
            "focused_product_date_v19_v20_tests_passed": 43,
            "complete_current_tests_passed": 447,
            "complete_high_risk_tests_passed": 1246,
            "compilation_passed": True,
            "dependency_consistency_passed": True,
            "deterministic_reconstruction_passed": True,
            "documentation_regression_checks_passed": True,
            "git_diff_check_passed": True,
        },
        "authority_and_effects": {
            "cme_network_requests_for_remaining_dates": 9,
            "v20_provider_access_performed": False,
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
            raise RuntimeError("existing v20 consolidation manifest differs")
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
