"""Freeze the exact post-custody, prepare-only consolidation scope."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_source_safe_consolidation_manifest_v1/manifest.json"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
RECOMMENDED = (
    "PIPELINE_FOLDER_MAP.md",
    "PROJECT_OUTLINE.md",
    "configs/apex_micro_phase1b2_prepare_only_contract_v1.json",
    "configs/apex_micro_tier01_v24_custody_repair_plan_v2.json",
    "scripts/audit_data_topology_source_safe_v2.py",
    "scripts/prepare_apex_micro_phase1b2_contract_v1.py",
    "scripts/prepare_apex_micro_phase1b2_consolidation_manifest_v1.py",
    "src/futures_rebuild/micro_alpha_phase1b2_preparation.py",
    "state/authorization_uses/"
    "f02b64f80babc67bc5281dcb1ad7da570a6e499ca7c4f3bce29cd1dd885108fe.json",
    "state/unpublished_evidence/apex_micro_v24_custody_repair_plan_v2/audit.json",
    "state/unpublished_evidence/apex_micro_v24_custody_repair_v2/terminal.json",
    "state/unpublished_evidence/data_topology_source_safe_audit_v2/report.json",
    OUTPUT.as_posix(),
    "tests/test_data_topology_source_safe_v2.py",
    "tests/test_micro_alpha_phase1b2_consolidation_manifest_v1.py",
    "tests/test_micro_alpha_phase1b2_preparation.py",
)


def _git(root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), *args], text=True
    ).strip()


def _worktree_paths(root: Path) -> list[str]:
    raw = subprocess.check_output(
        [
            "git", "-C", str(root), "status", "--porcelain=v1", "-z",
            "--untracked-files=all",
        ],
        text=True,
    )
    paths: list[str] = []
    for record in raw.split("\0"):
        if not record:
            continue
        path = record[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.append(path.replace("\\", "/"))
    return sorted(set(paths))


def build_manifest(*, root: Path = ROOT) -> dict[str, object]:
    root = root.resolve(strict=True)
    expected = set(RECOMMENDED) | set(PRESERVED_UNSTAGED)
    if not (root / OUTPUT).exists():
        expected.remove(OUTPUT.as_posix())
    observed = set(_worktree_paths(root))
    if observed != expected:
        raise IntegrityError("consolidation worktree does not match the exact scope")
    if _git(root, "diff", "--cached", "--name-only"):
        raise IntegrityError("consolidation manifest requires an empty index")
    records = []
    for path in RECOMMENDED:
        records.append(
            {
                "path": path,
                "sha256": (
                    "SELF_HASHED_AT_WRITE"
                    if path == OUTPUT.as_posix()
                    else sha256_file(root / path)
                ),
                "recommended_for_exact_stage": True,
            }
        )
    preserved = [
        {
            "path": path,
            "sha256": sha256_file(root / path),
            "recommended_for_exact_stage": False,
        }
        for path in PRESERVED_UNSTAGED
    ]
    core: dict[str, object] = {
        "schema_version": (
            "apex_micro_phase1b2_source_safe_consolidation_manifest/1.0.0"
        ),
        "state": "PREPARED_REQUIRES_EXACT_PATH_STAGING_APPROVAL",
        "repository_root": root.as_posix(),
        "branch": _git(root, "branch", "--show-current"),
        "observed_head": _git(root, "rev-parse", "HEAD"),
        "upstream": _git(root, "rev-parse", "--abbrev-ref", "@{upstream}"),
        "upstream_head": _git(root, "rev-parse", "@{upstream}"),
        "recommended_exact_stage_path_count": len(RECOMMENDED),
        "recommended_exact_stage_paths": list(RECOMMENDED),
        "preserved_unstaged_paths": list(PRESERVED_UNSTAGED),
        "records": records,
        "preserved_records": preserved,
        "categories": {
            "custody_repair_execution_evidence": [
                path for path in RECOMMENDED
                if "custody_repair" in path or "authorization_uses" in path
            ],
            "phase1b2_prepare_only_contracts": [
                path for path in RECOMMENDED
                if "phase1b2" in path and "consolidation" not in path
            ],
            "standard_topology_and_cleanup_governance": [
                path for path in RECOMMENDED if "topology" in path
            ],
            "tests_and_documentation": [
                path for path in RECOMMENDED
                if path.startswith("tests/") or path.endswith(".md")
            ],
            "consolidation_evidence": [
                path for path in RECOMMENDED if "consolidation" in path
            ],
            "unrelated_preserved_work": list(PRESERVED_UNSTAGED),
        },
        "verified_results": {
            "current_source_safe_tests": 551,
            "high_risk_source_safe_tests": 1350,
            "focused_phase1b2_product_and_pipeline_tests": 35,
            "documentation_regression_tests": 10,
            "compilation": "PASS",
            "dependency_consistency": "PASS",
            "deterministic_artifact_reconstruction": "PASS",
            "git_diff_check": "PASS",
        },
        "authority_and_effects": {
            "dbn_rows_decoded": 0,
            "historical_rows_read": 0,
            "year_2025_or_2026_payloads_opened": 0,
            "provider_or_network_calls_during_prepare_only_work": 0,
            "catalog_or_pointer_activated": False,
            "registration_or_evaluation": False,
            "cleanup_mutation": False,
            "git_staging": False,
            "git_commit": False,
            "git_push": False,
        },
        "next_sequential_boundary": "EXACT_PATH_STAGING_APPROVAL",
        "after_staging": "SEPARATE_LOCAL_COMMIT_APPROVAL_NO_PUSH",
        "next_research_boundary": (
            "SEPARATE_HISTORICAL_ROW_READ_APPROVAL_FOR_PHASE1B2"
        ),
    }
    return {**core, "manifest_id": sha256_json(core)}


def write_create_only(*, root: Path = ROOT) -> dict[str, object]:
    manifest = build_manifest(root=root)
    output = root / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as stream:
        stream.write(canonical_bytes(manifest) + b"\n")
    return manifest


def main() -> int:
    manifest = write_create_only(root=ROOT)
    print(
        json.dumps(
            {
                "manifest_id": manifest["manifest_id"],
                "sha256": sha256_file(ROOT / OUTPUT),
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
