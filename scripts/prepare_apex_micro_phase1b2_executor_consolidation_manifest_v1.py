"""Freeze the exact source-safe Phase 1B/2 executor implementation scope."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for candidate in (ROOT, ROOT / "src"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json  # noqa: E402
from futures_rebuild.errors import IntegrityError  # noqa: E402


OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_executor_consolidation_manifest_v1/manifest.json"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
RECOMMENDED = (
    "PIPELINE_FOLDER_MAP.md",
    "PROJECT_OUTLINE.md",
    "scripts/prepare_apex_micro_phase1b2_execution_v1.py",
    "scripts/prepare_apex_micro_phase1b2_executor_consolidation_manifest_v1.py",
    "src/futures_rebuild/micro_alpha_phase1b2_decoder.py",
    "src/futures_rebuild/micro_alpha_phase1b2_execution.py",
    "src/futures_rebuild/micro_alpha_phase1b2_preparation.py",
    OUTPUT.as_posix(),
    "tests/test_micro_alpha_phase1b2_decoder_v1.py",
    "tests/test_micro_alpha_phase1b2_executor_consolidation_manifest_v1.py",
    "tests/test_micro_alpha_phase1b2_execution_v1.py",
    "tests/test_micro_alpha_phase1b2_preparation.py",
)


def _git(*args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(ROOT), *args], text=True
    ).strip()


def _worktree_paths() -> set[str]:
    raw = subprocess.check_output(
        [
            "git", "-C", str(ROOT), "status", "--porcelain=v1", "-z",
            "--untracked-files=all",
        ],
        text=True,
    )
    paths: set[str] = set()
    for record in raw.split("\0"):
        if not record:
            continue
        path = record[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.add(path.replace("\\", "/"))
    return paths


def build_manifest() -> dict[str, object]:
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("executor consolidation requires an empty index")
    expected = set(RECOMMENDED) | set(PRESERVED_UNSTAGED)
    if not (ROOT / OUTPUT).exists():
        expected.remove(OUTPUT.as_posix())
    if _worktree_paths() != expected:
        raise IntegrityError("executor worktree does not match the exact consolidation scope")
    records = [
        {
            "path": path,
            "sha256": "SELF_HASHED_AT_WRITE" if path == OUTPUT.as_posix() else sha256_file(ROOT / path),
            "recommended_for_exact_stage": True,
        }
        for path in RECOMMENDED
    ]
    preserved = [
        {
            "path": path,
            "sha256": sha256_file(ROOT / path),
            "recommended_for_exact_stage": False,
        }
        for path in PRESERVED_UNSTAGED
    ]
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1b2_executor_consolidation_manifest/1.0.0",
        "state": "PREPARED_REQUIRES_EXACT_PATH_STAGING_APPROVAL",
        "repository_root": ROOT.resolve().as_posix(),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream": _git("rev-parse", "--abbrev-ref", "@{upstream}"),
        "upstream_head": _git("rev-parse", "@{upstream}"),
        "recommended_exact_stage_path_count": len(RECOMMENDED),
        "recommended_exact_stage_paths": list(RECOMMENDED),
        "preserved_unstaged_paths": list(PRESERVED_UNSTAGED),
        "records": records,
        "preserved_records": preserved,
        "scope": {
            "lane_id": "apex_integer_micro_11",
            "markets": ["MES", "MCL", "MGC", "M6E"],
            "schemas": ["definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"],
            "eligible_years": list(range(2018, 2025)),
            "source_count": 120,
            "source_bytes": 1_232_883_585,
            "coverage_cell_count": 140,
            "prelaunch_cell_count": 20,
            "row_payloads_opened": 0,
        },
        "authority_and_effects": {
            "historical_rows_read": 0,
            "year_2025_or_2026_payloads_opened": 0,
            "provider_or_network_calls": 0,
            "credential_access": False,
            "catalog_or_pointer_activated": False,
            "published_registered_evaluated_or_traded": False,
            "cleanup_mutation": False,
            "git_staging": False,
            "git_commit": False,
            "git_push": False,
        },
        "next_sequential_boundary": "EXACT_PATH_STAGING_APPROVAL",
        "after_staging": "SEPARATE_LOCAL_COMMIT_APPROVAL_NO_PUSH",
        "after_commit": "CREATE_ONLY_EXECUTION_PLAN_AND_AUDIT_PREPARATION",
        "next_research_boundary": "SEPARATE_EXACT_PHASE1B2_HISTORICAL_ROW_CONFIRMATION",
    }
    return {**core, "manifest_id": sha256_json(core)}


def write_create_only() -> dict[str, object]:
    value = build_manifest()
    path = ROOT / OUTPUT
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(value) + b"\n")
    return value


def main() -> int:
    value = write_create_only()
    print(
        json.dumps(
            {
                "manifest_id": value["manifest_id"],
                "sha256": sha256_file(ROOT / OUTPUT),
                "recommended_exact_stage_path_count": len(RECOMMENDED),
                "state": value["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
