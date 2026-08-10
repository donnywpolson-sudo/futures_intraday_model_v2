"""Freeze the group result and definition-classifier consolidation scope."""

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
    "apex_micro_phase1b2_definition_duplicate_manifest_v1/manifest.json"
)
PREDECESSOR = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_group_diagnostic_manifest_v4/manifest.json"
)
GROUP_PLAN = Path("configs/apex_micro_phase1b2_phase2_group_diagnostic_plan_v2.json")
GROUP_AUDIT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_phase2_group_diagnostic_plan_v2/audit.json"
)
GROUP_REPORT = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_group_diagnostic_v2/"
    "57515d8f88bca13ec9c9cab3/report.json"
)
GROUP_TERMINAL = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_group_diagnostic_v2/"
    "57515d8f88bca13ec9c9cab3/terminal.json"
)
AUTHORIZATION_USE = Path(
    "state/authorization_uses/"
    "4ab3fc669abe1e91419e702a338a602190ea30954c45f018000f33ab3ccf997e.json"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
RECOMMENDED = (
    "PIPELINE_FOLDER_MAP.md",
    "PROJECT_OUTLINE.md",
    GROUP_PLAN.as_posix(),
    "scripts/prepare_apex_micro_phase1b2_definition_duplicate_diagnostic_v3.py",
    "scripts/prepare_apex_micro_phase1b2_definition_duplicate_manifest_v1.py",
    "src/futures_rebuild/micro_alpha_phase1b2_definition_duplicate_diagnostic.py",
    "src/futures_rebuild/research_gateway_policy.py",
    AUTHORIZATION_USE.as_posix(),
    GROUP_AUDIT.as_posix(),
    GROUP_REPORT.as_posix(),
    GROUP_TERMINAL.as_posix(),
    OUTPUT.as_posix(),
    "tests/test_micro_alpha_phase1b2_definition_duplicate_diagnostic_v3.py",
    "tests/test_micro_alpha_phase1b2_definition_duplicate_manifest_v1.py",
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
        raise IntegrityError("definition diagnostic commit differs from exact scope")
    value = json.loads(_committed_bytes(commit, OUTPUT.as_posix()))
    core = dict(value)
    if core.pop("manifest_id", None) != sha256_json(core):
        raise IntegrityError("definition diagnostic manifest identity drifted")
    records = {item["path"]: item["sha256"] for item in value["records"]}
    for path in RECOMMENDED:
        if path == OUTPUT.as_posix():
            if records[path] != "SELF_HASHED_AT_WRITE":
                raise IntegrityError("definition diagnostic self marker drifted")
        elif hashlib.sha256(_committed_bytes(commit, path)).hexdigest() != records[path]:
            raise IntegrityError("definition diagnostic committed hash drifted")
    return value


def build_manifest() -> dict[str, object]:
    if _tracked(OUTPUT):
        return _postcommit_manifest()
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("definition diagnostic manifest requires empty index")
    expected = set(RECOMMENDED) | set(PRESERVED_UNSTAGED)
    if not (ROOT / OUTPUT).exists():
        expected.remove(OUTPUT.as_posix())
    if _worktree_paths() != expected:
        raise IntegrityError("definition diagnostic worktree differs from exact scope")
    predecessor = _self_hashed(PREDECESSOR, "manifest_id", "group v4 manifest")
    plan = _self_hashed(GROUP_PLAN, "plan_id", "group diagnostic plan")
    audit = _self_hashed(GROUP_AUDIT, "audit_id", "group diagnostic audit")
    report = _self_hashed(GROUP_REPORT, "report_id", "group diagnostic report")
    terminal = _self_hashed(GROUP_TERMINAL, "terminal_id", "group diagnostic terminal")
    authorization = json.loads((ROOT / AUTHORIZATION_USE).read_text(encoding="utf-8"))
    summaries = {
        item["schema"]: item["duplicate_count"]
        for item in report.get("public_decode_summaries", [])
    }
    if (
        predecessor.get("manifest_id")
        != "ef54445e20ce11589c8a9289f780bc0edae0720efa031f7b2873b27fc30aa61b"
        or audit.get("plan_id") != plan.get("plan_id")
        or report.get("state") != "PASS_FIRST_GROUP_TRANSITION_DIAGNOSTIC"
        or terminal.get("state") != "PASS_FIRST_GROUP_TRANSITION_DIAGNOSTIC"
        or terminal.get("report_id") != report.get("report_id")
        or report.get("group_disposition") != "DUPLICATE"
        or report.get("identity_and_roll_certified") is not False
        or summaries
        != {"definition": 308, "ohlcv-1m": 0, "ohlcv-1s": 0, "statistics": 0, "status": 0}
        or authorization.get("receipt_id") != report.get("authorization_receipt_id")
        or authorization.get("operation")
        != "DIAGNOSE_APEX_MICRO_PHASE2_FIRST_GROUP_V2_ONCE"
    ):
        raise IntegrityError("definition diagnostic predecessor evidence drifted")
    records = [
        {
            "path": path,
            "sha256": "SELF_HASHED_AT_WRITE" if path == OUTPUT.as_posix() else sha256_file(ROOT / path),
            "recommended_for_exact_stage": True,
        }
        for path in RECOMMENDED
    ]
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1b2_definition_duplicate_manifest/1.0.0",
        "state": "PREPARED_REQUIRES_EXACT_PATH_STAGING_APPROVAL",
        "repository_root": ROOT.resolve().as_posix(),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream": _git("rev-parse", "--abbrev-ref", "@{upstream}"),
        "upstream_head": _git("rev-parse", "@{upstream}"),
        "predecessor_manifest_id": predecessor["manifest_id"],
        "predecessor_manifest_sha256": sha256_file(ROOT / PREDECESSOR),
        "group_plan_id": plan["plan_id"],
        "group_plan_sha256": sha256_file(ROOT / GROUP_PLAN),
        "group_audit_id": audit["audit_id"],
        "group_audit_sha256": sha256_file(ROOT / GROUP_AUDIT),
        "group_report_id": report["report_id"],
        "group_report_sha256": sha256_file(ROOT / GROUP_REPORT),
        "group_terminal_id": terminal["terminal_id"],
        "group_terminal_sha256": sha256_file(ROOT / GROUP_TERMINAL),
        "authorization_use_sha256": sha256_file(ROOT / AUTHORIZATION_USE),
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
        "preserved_inactive_not_for_git": {
            "phase1b_parquet_count": 120,
            "phase1b_parquet_bytes": 6_627_486_838,
            "causal_diagnostic_parquet_count": 1,
            "raw_or_derived_parquet_recommended_for_git_stage": False,
        },
        "group_result": {
            "state": report["state"],
            "group_disposition": report["group_disposition"],
            "source_count": report["source_count"],
            "source_bytes": report["source_bytes"],
            "definition_legacy_repeat_count": 308,
            "other_schema_duplicate_count": 0,
            "phase2_parquets_created": 0,
            "raw_values_reported": False,
        },
        "definition_diagnostic_successor": {
            "operation": "DIAGNOSE_APEX_MICRO_DEFINITION_DUPLICATE_SEMANTICS_V3_ONCE",
            "source_count": 1,
            "source_bytes": 68_274,
            "market": "M6E",
            "schema": "definition",
            "year": 2018,
            "legacy_repeat_count": 308,
            "maximum_workers": 1,
            "maximum_batch_rows": 100_000,
            "maximum_runtime_seconds": 300,
            "maximum_output_bytes": 4_194_304,
            "required_free_disk_bytes": 536_870_912,
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "provider_calls": 0,
            "external_cost_usd": "0",
            "dbn_reachable": False,
            "second_parquet_source_reachable": False,
            "parquet_creation_reachable": False,
            "raw_values_or_semantic_keys_reported": False,
        },
        "authority_and_effects": {
            "definition_diagnostic_row_batches_opened": 0,
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
        "after_commit": "CREATE_ONLY_DEFINITION_DUPLICATE_DIAGNOSTIC_PLAN_AND_AUDIT",
        "next_research_boundary": "FRESH_EXACT_ONE_FILE_DEFINITION_DIAGNOSTIC_APPROVAL",
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
