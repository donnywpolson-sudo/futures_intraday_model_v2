"""Freeze the completed Phase 2 result with the sealed-row map status restored."""

from __future__ import annotations

import hashlib
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
from scripts import prepare_apex_micro_phase1b2_phase2_completion_manifest_v1 as v1  # noqa: E402
from scripts import prepare_apex_micro_phase1b2_phase2_completion_manifest_v3 as v3  # noqa: E402


OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_phase2_completion_manifest_v4/manifest.json"
)
PRESERVED_UNSTAGED = v3.PRESERVED_UNSTAGED
MAP_PATH = "PIPELINE_FOLDER_MAP.md"
RECOMMENDED = (
    *v3.RECOMMENDED,
    "scripts/prepare_apex_micro_phase1b2_phase2_completion_manifest_v4.py",
    OUTPUT.as_posix(),
    "tests/test_micro_alpha_phase1b2_phase2_completion_manifest_v4.py",
)


def _git(*args: str) -> str:
    return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True).strip()


def _tracked(path: Path) -> bool:
    return subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "--error-unmatch", "--", path.as_posix()],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0


def _committed_bytes(commit: str, path: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(ROOT), "show", f"{commit}:{path}"])


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


def _self_hashed(path: Path, key: str, description: str) -> dict[str, object]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    core = dict(value)
    if core.pop(key, None) != sha256_json(core):
        raise IntegrityError(f"{description} identity drifted")
    return value


def _postcommit_manifest() -> dict[str, object]:
    commit = _git("log", "-1", "--format=%H", "--", OUTPUT.as_posix())
    changed = set(
        _git("diff-tree", "--no-commit-id", "--name-only", "-r", commit).splitlines()
    )
    if changed != set(RECOMMENDED):
        raise IntegrityError("Phase 2 completion v4 commit differs from exact scope")
    value = json.loads(_committed_bytes(commit, OUTPUT.as_posix()))
    core = dict(value)
    if core.pop("manifest_id", None) != sha256_json(core):
        raise IntegrityError("Phase 2 completion v4 manifest identity drifted")
    records = {item["path"]: item["sha256"] for item in value["records"]}
    for path in RECOMMENDED:
        if path == OUTPUT.as_posix():
            if records[path] != "SELF_HASHED_AT_WRITE":
                raise IntegrityError("Phase 2 completion v4 self marker drifted")
        elif hashlib.sha256(_committed_bytes(commit, path)).hexdigest() != records[path]:
            raise IntegrityError("Phase 2 completion v4 committed hash drifted")
    return value


def build_manifest() -> dict[str, object]:
    if _tracked(OUTPUT):
        return _postcommit_manifest()
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("Phase 2 completion v4 manifest requires empty index")
    expected = set(RECOMMENDED) | set(PRESERVED_UNSTAGED)
    if not (ROOT / OUTPUT).exists():
        expected.remove(OUTPUT.as_posix())
    if _worktree_paths() != expected:
        raise IntegrityError("Phase 2 completion v4 worktree differs from exact scope")

    predecessor = _self_hashed(v3.OUTPUT, "manifest_id", "Phase 2 completion v3")
    if (
        predecessor.get("manifest_id")
        != "49fb68cbd9b91ac6bac2d1841866010ebfa1403a9d1da147b80984c9f0eec9c0"
        or sha256_file(ROOT / v3.OUTPUT)
        != "b7d484fe38f2f0939f5e46348366ec64c4285653121c553bc5606d3959668ffb"
    ):
        raise IntegrityError("Phase 2 completion v3 binding drifted")
    predecessor_records = {item["path"]: item["sha256"] for item in predecessor["records"]}
    map_transition: dict[str, str] | None = None
    for path in v3.RECOMMENDED:
        expected_sha = predecessor_records[path]
        if path == v3.OUTPUT.as_posix():
            if expected_sha != "SELF_HASHED_AT_WRITE":
                raise IntegrityError("Phase 2 completion v3 self marker drifted")
            continue
        actual_sha = sha256_file(ROOT / path)
        if path == MAP_PATH:
            if actual_sha == expected_sha:
                raise IntegrityError("sealed-row map status was not restored")
            mapping = (ROOT / path).read_text(encoding="utf-8")
            for classification in (
                "CURRENT_REACHABLE",
                "PREPARED_NOT_EXECUTED",
                "SYNTHETIC_ONLY",
                "HISTORICAL_ROW_APPROVAL_REQUIRED",
                "NOT_IMPLEMENTED",
                "RETIRED",
            ):
                if classification not in mapping:
                    raise IntegrityError("pipeline map classification census drifted")
            if "Inactive 2025 holdout and 2026 forward micro DBNs" not in mapping:
                raise IntegrityError("sealed-row map assignment drifted")
            map_transition = {
                "path": path,
                "predecessor_sha256": expected_sha,
                "successor_sha256": actual_sha,
            }
        elif actual_sha != expected_sha:
            raise IntegrityError("Phase 2 completion v3 preserved path drifted")
    if map_transition is None:
        raise IntegrityError("pipeline map transition is missing")

    plan = _self_hashed(v1.PLAN, "plan_id", "Phase 2 successor plan")
    report = _self_hashed(v1.REPORT, "source_certification_id", "source certification")
    terminal = _self_hashed(v1.TERMINAL, "terminal_id", "Phase 2 terminal")
    output_records = v1._output_records(report, Path(str(plan["staging_root"])))
    if terminal.get("state") != "SUCCESS_CERTIFIED_INACTIVE_PHASE2" or len(output_records) != 24:
        raise IntegrityError("Phase 2 completion result drifted")

    records = [
        {
            "path": path,
            "sha256": "SELF_HASHED_AT_WRITE" if path == OUTPUT.as_posix() else sha256_file(ROOT / path),
            "recommended_for_exact_stage": True,
        }
        for path in RECOMMENDED
    ]
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1b2_phase2_completion_manifest/4.0.0",
        "state": "PREPARED_REQUIRES_EXACT_PATH_STAGING_APPROVAL",
        "repository_root": ROOT.resolve().as_posix(),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream": _git("rev-parse", "--abbrev-ref", "@{upstream}"),
        "upstream_head": _git("rev-parse", "@{upstream}"),
        "predecessor_manifest_id": predecessor["manifest_id"],
        "predecessor_manifest_sha256": sha256_file(ROOT / v3.OUTPUT),
        "predecessor_classification": "SUPERSEDED_PREPARATION_MISSING_REQUIRED_MAP_STATUS",
        "predecessor_manifest_byte_for_byte_preserved": True,
        "map_transition": map_transition,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(ROOT / v1.PLAN),
        "source_certification_id": report["source_certification_id"],
        "source_certification_sha256": sha256_file(ROOT / v1.REPORT),
        "terminal_id": terminal["terminal_id"],
        "terminal_sha256": sha256_file(ROOT / v1.TERMINAL),
        "recommended_exact_stage_path_count": len(RECOMMENDED),
        "recommended_exact_stage_paths": list(RECOMMENDED),
        "preserved_unstaged_paths": list(PRESERVED_UNSTAGED),
        "records": records,
        "preserved_records": [
            {
                "path": path,
                "sha256": sha256_file(ROOT / path),
                "recommended_for_exact_stage": False,
            }
            for path in PRESERVED_UNSTAGED
        ],
        "certified_inactive_result": {
            "state": terminal["state"],
            "phase2_output_count": len(output_records),
            "phase2_output_bytes": sum(int(item["bytes"]) for item in output_records),
            "phase2_output_set_sha256": sha256_json(output_records),
            "raw_or_derived_parquet_recommended_for_git_stage": False,
            "catalog_or_pointer_activated": False,
        },
        "remaining_historical_row_boundary": {
            "classification": "HISTORICAL_ROW_APPROVAL_REQUIRED",
            "scope": "INACTIVE_2025_HOLDOUT_AND_2026_FORWARD_MICRO_ROWS",
            "phase2_success_scope": "2018_THROUGH_2024_ONLY",
            "separate_approval_required": True,
        },
        "authority_and_effects": {
            "new_historical_row_authorization_consumed": False,
            "provider_or_network_calls": 0,
            "dbn_payloads_opened": 0,
            "year_2025_or_2026_payloads_opened": 0,
            "published_registered_evaluated_or_traded": False,
            "standard_lane_mutated": False,
            "git_staging": False,
            "git_commit": False,
            "git_push": False,
        },
        "next_sequential_boundary": "EXACT_PATH_STAGING_APPROVAL",
        "after_staging": "SEPARATE_LOCAL_COMMIT_APPROVAL_NO_PUSH",
        "next_research_boundary": "SEPARATE_MICRO_CATALOG_PUBLICATION_OR_MECHANISM_TIER0_BOUNDARY",
    }
    return {**core, "manifest_id": sha256_json(core)}


def write_create_only() -> dict[str, object]:
    value = build_manifest()
    target = ROOT / OUTPUT
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as stream:
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
