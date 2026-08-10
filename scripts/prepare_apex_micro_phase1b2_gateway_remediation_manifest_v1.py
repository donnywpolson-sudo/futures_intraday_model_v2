"""Freeze the exact central-gateway remediation and v1 failure evidence."""

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


OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_gateway_remediation_manifest_v1/manifest.json"
)
PREDECESSOR = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_executor_consolidation_manifest_v2/manifest.json"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
RECOMMENDED = (
    "PIPELINE_FOLDER_MAP.md",
    "PROJECT_OUTLINE.md",
    "configs/apex_micro_phase1b2_historical_execution_plan_v1.json",
    "scripts/prepare_apex_micro_phase1b2_executor_consolidation_manifest_v2.py",
    "scripts/prepare_apex_micro_phase1b2_gateway_remediation_manifest_v1.py",
    "scripts/prepare_apex_micro_phase1b2_preexecution_failure_v1.py",
    "src/futures_rebuild/micro_alpha_phase1b2_execution.py",
    "src/futures_rebuild/research_gateway_policy.py",
    "state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v1/audit.json",
    "state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v1_supersession/report.json",
    OUTPUT.as_posix(),
    "tests/test_micro_alpha_phase1b2_execution_v1.py",
    "tests/test_micro_alpha_phase1b2_gateway_remediation_manifest_v1.py",
    "tests/test_micro_alpha_phase1b2_preexecution_failure_v1.py",
)


def _git(*args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(ROOT), *args], text=True
    ).strip()


def _tracked(path: Path) -> bool:
    return subprocess.run(
        [
            "git", "-C", str(ROOT), "ls-files", "--error-unmatch", "--",
            path.as_posix(),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0


def _committed_bytes(commit: str, path: str) -> bytes:
    return subprocess.check_output(
        ["git", "-C", str(ROOT), "show", f"{commit}:{path}"]
    )


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


def _postcommit_manifest() -> dict[str, object]:
    commit = _git("log", "-1", "--format=%H", "--", OUTPUT.as_posix())
    if set(
        _git("diff-tree", "--no-commit-id", "--name-only", "-r", commit).splitlines()
    ) != set(RECOMMENDED):
        raise IntegrityError("gateway-remediation commit differs from exact scope")
    value = json.loads(_committed_bytes(commit, OUTPUT.as_posix()))
    core = dict(value)
    observed = core.pop("manifest_id", None)
    if observed != sha256_json(core):
        raise IntegrityError("gateway-remediation manifest identity drifted")
    records = {item["path"]: item["sha256"] for item in value["records"]}
    for path in RECOMMENDED:
        if path == OUTPUT.as_posix():
            if records[path] != "SELF_HASHED_AT_WRITE":
                raise IntegrityError("gateway manifest self marker drifted")
        elif hashlib.sha256(_committed_bytes(commit, path)).hexdigest() != records[path]:
            raise IntegrityError("gateway-remediation committed hash drifted")
    return value


def build_manifest() -> dict[str, object]:
    if _tracked(OUTPUT):
        return _postcommit_manifest()
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("gateway-remediation manifest requires empty index")
    expected = set(RECOMMENDED) | set(PRESERVED_UNSTAGED)
    if not (ROOT / OUTPUT).exists():
        expected.remove(OUTPUT.as_posix())
    if _worktree_paths() != expected:
        raise IntegrityError("gateway-remediation worktree differs from exact scope")
    predecessor = json.loads((ROOT / PREDECESSOR).read_text(encoding="utf-8"))
    if predecessor.get("manifest_id") != (
        "e77ff67d14b2844361598ea3907cb79809650d0c7b5eb82b6870b6cc551950e0"
    ):
        raise IntegrityError("executor consolidation predecessor drifted")
    records = [
        {
            "path": path,
            "sha256": "SELF_HASHED_AT_WRITE" if path == OUTPUT.as_posix() else sha256_file(ROOT / path),
            "recommended_for_exact_stage": True,
        }
        for path in RECOMMENDED
    ]
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1b2_gateway_remediation_manifest/1.0.0",
        "state": "PREPARED_REQUIRES_EXACT_PATH_STAGING_APPROVAL",
        "repository_root": ROOT.resolve().as_posix(),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream": _git("rev-parse", "--abbrev-ref", "@{upstream}"),
        "upstream_head": _git("rev-parse", "@{upstream}"),
        "predecessor_manifest_id": predecessor["manifest_id"],
        "predecessor_manifest_sha256": sha256_file(ROOT / PREDECESSOR),
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
        "remediation": {
            "exact_operation_allowlisted": "BUILD_APEX_MICRO_PHASE1B2_INACTIVE_FOUNDATION_V1_ONCE",
            "wildcard_or_alias_allowlisted": False,
            "v1_plan_preserved": True,
            "v1_audit_preserved": True,
            "v1_authorization_consumed": False,
            "v1_dbn_rows_decoded": 0,
            "successor_plan_version": 2,
        },
        "authority_and_effects": {
            "historical_rows_read": 0,
            "year_2025_or_2026_payloads_opened": 0,
            "provider_or_network_calls": 0,
            "credential_access": False,
            "catalog_or_pointer_activated": False,
            "published_registered_evaluated_or_traded": False,
            "git_staging": False,
            "git_commit": False,
            "git_push": False,
        },
        "next_sequential_boundary": "EXACT_PATH_STAGING_APPROVAL",
        "after_staging": "SEPARATE_LOCAL_COMMIT_APPROVAL_NO_PUSH",
        "after_commit": "CREATE_ONLY_V2_EXECUTION_PLAN_AND_AUDIT",
        "next_research_boundary": "FRESH_EXACT_PHASE1B2_HISTORICAL_ROW_CONFIRMATION",
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
