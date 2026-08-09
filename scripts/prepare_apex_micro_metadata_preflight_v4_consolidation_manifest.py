"""Create the exact post-v2-failure/v4-successor staging manifest."""

from __future__ import annotations

import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_metadata_preflight_v4_consolidation_manifest_v2/manifest.json"
)

CATEGORIES = {
    "a_executed_v2_fail_closed_metadata_evidence": (
        "state/authorization_uses/83f1fdb2ed7a542bf8571edb83b3927d0ce0d4cfedad97ce6aa6b2087171b660.json",
        "state/unpublished_evidence/apex_micro_metadata_preflight_v2/report.json",
    ),
    "b_superseded_v3_local_preparation": (
        "configs/apex_micro_tier01_databento_metadata_preflight_v3.json",
        "state/unpublished_evidence/apex_micro_metadata_preflight_v3_supersession.json",
    ),
    "c_failed_predecessor_staging_preparation": (
        "state/unpublished_evidence/apex_micro_metadata_preflight_v4_consolidation_manifest/manifest.json",
        "state/unpublished_evidence/apex_micro_metadata_preflight_v4_consolidation_manifest_supersession.json",
    ),
    "d_v4_metadata_successor": (
        "configs/apex_micro_tier01_databento_metadata_preflight_v4.json",
        "scripts/prepare_apex_micro_metadata_preflight_v4.py",
        "src/futures_rebuild/micro_alpha_databento_preflight_v4.py",
    ),
    "e_tests_and_implementation_reality_documentation": (
        "PIPELINE_FOLDER_MAP.md",
        "PROJECT_OUTLINE.md",
        "scripts/prepare_apex_micro_metadata_preflight_v4_consolidation_manifest.py",
        "tests/test_micro_alpha_databento_preflight_v4.py",
        "tests/test_operational_documents.py",
    ),
    "f_unrelated_preserved_work": (
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
    records = completed.stdout.decode("utf-8", "strict").split("\0")
    output: set[str] = set()
    for record in records:
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


def main() -> int:
    categorized = {path for paths in CATEGORIES.values() for path in paths}
    if len(categorized) != sum(len(paths) for paths in CATEGORIES.values()):
        raise RuntimeError("successor consolidation categories contain duplicate paths")
    observed = _status_paths() - {OUTPUT.as_posix()}
    if observed != categorized:
        missing = sorted(observed - categorized)
        stale = sorted(categorized - observed)
        raise RuntimeError(
            f"successor consolidation census drifted missing={missing} stale={stale}"
        )
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
    core = {
        "schema_version": (
            "apex_micro_metadata_preflight_v4_consolidation_manifest/2.0.0"
        ),
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git_value("branch", "--show-current"),
        "observed_head": _git_value("rev-parse", "HEAD"),
        "v2_preflight_report_id": (
            "524624df5b4a476abb81e2bb27b817a8ed6b8acd2ad6e9d3db9e03c8cf25ff97"
        ),
        "v2_preflight_state": "FAIL_CLOSED_METADATA_ONLY",
        "v4_plan_id": (
            "7746f9fb42ef91373a4e18b7f625f069053ec32a040d38f552ddcaea32e1c16c"
        ),
        "v4_plan_sha256": (
            "2df100c550653ae6f8c7268934de7b051cb5aabb0617591c69bed050dc701ada"
        ),
        "category_records": records,
        "recommended_exact_stage_paths": sorted(recommended),
        "predecessor_manifest": {
            "path": (
                "state/unpublished_evidence/"
                "apex_micro_metadata_preflight_v4_consolidation_manifest/manifest.json"
            ),
            "manifest_id": (
                "9e4a754477101b2e4d32adf3c9ceffeef82cedb6f37e444bd7174097a0423da5"
            ),
            "sha256": (
                "583845119b9fd610bfcba42a56e83581c04a3b4bbf4126e5831deff9900cba4c"
            ),
            "state": "SUPERSEDED_AFTER_CACHED_DIFF_CHECK_FAILURE",
        },
        "preserved_unstaged_paths": list(CATEGORIES["f_unrelated_preserved_work"]),
        "staging_performed": False,
        "commit_performed": False,
        "push_performed": False,
        "provider_access_performed": True,
        "provider_call_total": 2,
        "external_cost_incurred_usd": "0",
        "automatic_retries": 0,
        "timeseries_download_calls": 0,
        "dbn_download_performed": False,
        "historical_rows_read": False,
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
    print(manifest["manifest_id"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
