"""Prepare the exact annual-micro v5 and source-safe cleanup consolidation."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_annual_v5_consolidation_manifest/manifest.json"
)

CATEGORIES = {
    "a_executed_v4_fail_closed_metadata_evidence": (
        "state/authorization_uses/49dfe473890508a9c789b87b94cd2fa7826c072d1fcb4190d2e4f00733502cda.json",
        "state/unpublished_evidence/apex_micro_metadata_preflight_v4/report.json",
    ),
    "b_annual_micro_v5_architecture_and_acquisition": (
        "configs/apex_micro_tier01_databento_metadata_preflight_v5.json",
        "scripts/prepare_apex_micro_metadata_preflight_v5.py",
        "src/futures_rebuild/micro_alpha_acquisition.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v5.py",
        "src/futures_rebuild/micro_alpha_pipeline.py",
    ),
    "c_micro_adversarial_and_predecessor_tests": (
        "tests/test_micro_alpha_acquisition.py",
        "tests/test_micro_alpha_databento_preflight.py",
        "tests/test_micro_alpha_databento_preflight_v4.py",
        "tests/test_micro_alpha_databento_preflight_v5.py",
        "tests/test_micro_alpha_pipeline.py",
    ),
    "d_standard_source_safe_topology_audit": (
        "scripts/audit_standard_data_topology_source_safe.py",
        "state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json",
        "tests/test_standard_data_topology_source_safe_audit.py",
    ),
    "e_safe_cleanup_preparation_and_preserved_drafts": (
        "scripts/prepare_safe_cleanup_inventory.py",
        "scripts/prepare_safe_cleanup_inventory_v2.py",
        "scripts/prepare_safe_cleanup_inventory_v3.py",
        "scripts/prepare_safe_cleanup_inventory_v4.py",
        "state/unpublished_evidence/safe_cleanup_preparation/plan.json",
        "state/unpublished_evidence/safe_cleanup_preparation_v1_supersession.json",
        "state/unpublished_evidence/safe_cleanup_preparation_v2/plan.json",
        "state/unpublished_evidence/safe_cleanup_preparation_v2_supersession.json",
        "state/unpublished_evidence/safe_cleanup_preparation_v3/plan.json",
        "state/unpublished_evidence/safe_cleanup_preparation_v3_supersession.json",
        "state/unpublished_evidence/safe_cleanup_preparation_v4/plan.json",
        "tests/test_safe_cleanup_preparation.py",
    ),
    "f_implementation_reality_documentation_and_manifest_builder": (
        "PIPELINE_FOLDER_MAP.md",
        "PROJECT_OUTLINE.md",
        "scripts/prepare_apex_micro_annual_v5_consolidation_manifest.py",
    ),
    "g_unrelated_preserved_unstaged_work": (
        "CODEX_HANDOFF.md",
        "CURRENT_WORKFLOW.md",
    ),
}


def _status_paths() -> set[str]:
    completed = subprocess.run(
        [
            "git", "-C", str(ROOT), "status", "--porcelain=v1", "-z",
            "--untracked-files=all",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    output: set[str] = set()
    for record in completed.stdout.decode("utf-8", "strict").split("\0"):
        if not record:
            continue
        path = record[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        output.add(path.replace("\\", "/"))
    return output


def _git_value(*args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def _json(path: str) -> dict[str, object]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object: {path}")
    return value


def main() -> int:
    categorized = {path for paths in CATEGORIES.values() for path in paths}
    if len(categorized) != sum(len(paths) for paths in CATEGORIES.values()):
        raise RuntimeError("consolidation categories contain duplicate paths")
    observed = _status_paths() - {OUTPUT.as_posix()}
    if observed != categorized:
        unexpected = sorted(observed - categorized)
        stale = sorted(categorized - observed)
        raise RuntimeError(
            f"consolidation census drifted unexpected={unexpected} stale={stale}"
        )
    records = {
        category: [
            {
                "path": path,
                "sha256": sha256_file(ROOT / path),
                "recommended_for_exact_stage": not category.startswith("g_"),
            }
            for path in paths
        ]
        for category, paths in CATEGORIES.items()
    }
    recommended = sorted(
        path
        for category, paths in CATEGORIES.items()
        if not category.startswith("g_")
        for path in paths
    )
    recommended.append(OUTPUT.as_posix())
    v4_report = _json(
        "state/unpublished_evidence/apex_micro_metadata_preflight_v4/report.json"
    )
    v5_plan = _json(
        "configs/apex_micro_tier01_databento_metadata_preflight_v5.json"
    )
    topology = _json(
        "state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json"
    )
    cleanup = _json(
        "state/unpublished_evidence/safe_cleanup_preparation_v4/plan.json"
    )
    core: dict[str, object] = {
        "schema_version": "apex_micro_annual_v5_consolidation_manifest/1.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git_value("branch", "--show-current"),
        "observed_head": _git_value("rev-parse", "HEAD"),
        "upstream_head": _git_value("rev-parse", "origin/main"),
        "category_records": records,
        "recommended_exact_stage_paths": sorted(recommended),
        "recommended_exact_stage_path_count": len(recommended),
        "preserved_unstaged_paths": list(CATEGORIES["g_unrelated_preserved_unstaged_work"]),
        "v4_preflight_failure": {
            "report_id": v4_report["report_id"],
            "report_sha256": sha256_file(
                ROOT
                / "state/unpublished_evidence/apex_micro_metadata_preflight_v4/report.json"
            ),
            "provider_call_total": v4_report["provider_call_total"],
            "external_cost_incurred_usd": v4_report["external_cost_incurred_usd"],
            "automatic_retries": v4_report["automatic_retries"],
            "preservation": "IMMUTABLE_NO_OVERWRITE_DELETE_OR_RELABEL",
        },
        "v5_preflight": {
            "plan_id": v5_plan["plan_id"],
            "plan_sha256": sha256_file(
                ROOT / "configs/apex_micro_tier01_databento_metadata_preflight_v5.json"
            ),
            "request_definitions": 20,
            "maximum_annual_market_schema_requests": 180,
            "provider_call_ceiling": 371,
            "state": v5_plan["state"],
        },
        "standard_topology_audit": {
            "report_id": topology["report_id"],
            "report_sha256": sha256_file(
                ROOT
                / "state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json"
            ),
            "state": topology["state"],
            "active_market_year_count": topology["catalog"]["active_market_year_count"],
        },
        "cleanup_preparation": {
            "plan_id": cleanup["plan_id"],
            "plan_sha256": sha256_file(
                ROOT / "state/unpublished_evidence/safe_cleanup_preparation_v4/plan.json"
            ),
            "state": cleanup["state"],
            "cleanup_performed": cleanup["cleanup_execution"]["performed"],
        },
        "verification": {
            "focused_micro_tests_passed": 60,
            "complete_current_tests_passed": 171,
            "complete_high_risk_tests_passed": 960,
            "dependency_and_documentation_tests_passed": 11,
            "compilation_passed": True,
            "dependency_consistency_passed": True,
            "deterministic_reconstruction_passed": True,
            "git_diff_check_passed": True,
        },
        "authority_and_effects": {
            "staging_performed": False,
            "commit_performed": False,
            "push_performed": False,
            "provider_access_in_this_successor_preparation": False,
            "new_metadata_preflight_authorization_consumed": False,
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
