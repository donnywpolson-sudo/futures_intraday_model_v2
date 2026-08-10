"""Freeze the passing v1 diagnostic and bounded five-schema v2 successor."""

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
    "apex_micro_phase1b2_group_diagnostic_manifest_v2/manifest.json"
)
PREDECESSOR = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_phase2_diagnostic_manifest_v1/manifest.json"
)
DIAGNOSTIC_PLAN = Path("configs/apex_micro_phase1b2_phase2_diagnostic_plan_v1.json")
DIAGNOSTIC_AUDIT = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_diagnostic_plan_v1/audit.json"
)
DIAGNOSTIC_REPORT = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_diagnostic_v1/"
    "9456f21bd11c75fa6710e1ad/report.json"
)
DIAGNOSTIC_TERMINAL = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_diagnostic_v1/"
    "9456f21bd11c75fa6710e1ad/terminal.json"
)
DIAGNOSTIC_PARQUET = Path(
    "state/data_publication_staging/apex_micro_phase2_diagnostic_v1/"
    "9456f21bd11c75fa6710e1ad/causal_first_interval.parquet"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
RECOMMENDED = (
    "PIPELINE_FOLDER_MAP.md",
    "PROJECT_OUTLINE.md",
    DIAGNOSTIC_PLAN.as_posix(),
    "scripts/prepare_apex_micro_phase1b2_group_diagnostic_manifest_v2.py",
    "scripts/prepare_apex_micro_phase1b2_group_diagnostic_v2.py",
    "src/futures_rebuild/micro_alpha_phase1b2_group_diagnostic.py",
    "src/futures_rebuild/research_gateway_policy.py",
    "state/authorization_uses/b86ba55d714ccac12425e3ceb4ad503c9167a312f86a454a59ee6e71f5503077.json",
    DIAGNOSTIC_AUDIT.as_posix(),
    DIAGNOSTIC_REPORT.as_posix(),
    DIAGNOSTIC_TERMINAL.as_posix(),
    OUTPUT.as_posix(),
    "tests/test_micro_alpha_phase1b2_group_diagnostic_manifest_v2.py",
    "tests/test_micro_alpha_phase1b2_group_diagnostic_v2.py",
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


def _postcommit_manifest() -> dict[str, object]:
    commit = _git("log", "-1", "--format=%H", "--", OUTPUT.as_posix())
    if set(
        _git("diff-tree", "--no-commit-id", "--name-only", "-r", commit).splitlines()
    ) != set(RECOMMENDED):
        raise IntegrityError("group diagnostic commit differs from exact scope")
    value = json.loads(_committed_bytes(commit, OUTPUT.as_posix()))
    core = dict(value)
    observed = core.pop("manifest_id", None)
    if observed != sha256_json(core):
        raise IntegrityError("group diagnostic manifest identity drifted")
    records = {item["path"]: item["sha256"] for item in value["records"]}
    for path in RECOMMENDED:
        if path == OUTPUT.as_posix():
            if records[path] != "SELF_HASHED_AT_WRITE":
                raise IntegrityError("group diagnostic manifest self marker drifted")
        elif hashlib.sha256(_committed_bytes(commit, path)).hexdigest() != records[path]:
            raise IntegrityError("group diagnostic committed hash drifted")
    return value


def _load_self_hashed(path: Path, key: str, description: str) -> dict[str, object]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    core = dict(value)
    observed = core.pop(key, None)
    if observed != sha256_json(core):
        raise IntegrityError(f"{description} identity drifted")
    return value


def build_manifest() -> dict[str, object]:
    if _tracked(OUTPUT):
        return _postcommit_manifest()
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("group diagnostic manifest requires empty index")
    expected = set(RECOMMENDED) | set(PRESERVED_UNSTAGED)
    if not (ROOT / OUTPUT).exists():
        expected.remove(OUTPUT.as_posix())
    if _worktree_paths() != expected:
        raise IntegrityError("group diagnostic worktree differs from exact scope")
    predecessor = _load_self_hashed(PREDECESSOR, "manifest_id", "predecessor manifest")
    plan = _load_self_hashed(DIAGNOSTIC_PLAN, "plan_id", "diagnostic plan")
    audit = _load_self_hashed(DIAGNOSTIC_AUDIT, "audit_id", "diagnostic audit")
    report = _load_self_hashed(DIAGNOSTIC_REPORT, "report_id", "diagnostic report")
    terminal = _load_self_hashed(DIAGNOSTIC_TERMINAL, "terminal_id", "diagnostic terminal")
    if (
        predecessor.get("manifest_id")
        != "d1befaa2d3769f4fba91c742cac93c222f26128f55c001ef7d567f69997c0580"
        or report.get("state") != "PASS_FIRST_INTERVAL_PHASE2_MATERIALIZATION"
        or terminal.get("state") != "PASS_FIRST_INTERVAL_PHASE2_MATERIALIZATION"
        or terminal.get("report_id") != report.get("report_id")
        or audit.get("plan_id") != plan.get("plan_id")
    ):
        raise IntegrityError("passing diagnostic evidence drifted")
    if (
        sha256_file(ROOT / DIAGNOSTIC_PARQUET)
        != report.get("result", {}).get("output_sha256")
        or (ROOT / DIAGNOSTIC_PARQUET).stat().st_size
        != report.get("result", {}).get("output_bytes")
    ):
        raise IntegrityError("passing diagnostic Parquet drifted")
    records = [
        {
            "path": path,
            "sha256": "SELF_HASHED_AT_WRITE" if path == OUTPUT.as_posix() else sha256_file(ROOT / path),
            "recommended_for_exact_stage": True,
        }
        for path in RECOMMENDED
    ]
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1b2_group_diagnostic_manifest/2.0.0",
        "state": "PREPARED_REQUIRES_EXACT_PATH_STAGING_APPROVAL",
        "repository_root": ROOT.resolve().as_posix(),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream": _git("rev-parse", "--abbrev-ref", "@{upstream}"),
        "upstream_head": _git("rev-parse", "@{upstream}"),
        "predecessor_manifest_id": predecessor["manifest_id"],
        "predecessor_manifest_sha256": sha256_file(ROOT / PREDECESSOR),
        "diagnostic_plan_id": plan["plan_id"],
        "diagnostic_plan_sha256": sha256_file(ROOT / DIAGNOSTIC_PLAN),
        "diagnostic_audit_id": audit["audit_id"],
        "diagnostic_audit_sha256": sha256_file(ROOT / DIAGNOSTIC_AUDIT),
        "diagnostic_report_id": report["report_id"],
        "diagnostic_report_sha256": sha256_file(ROOT / DIAGNOSTIC_REPORT),
        "diagnostic_terminal_id": terminal["terminal_id"],
        "diagnostic_terminal_sha256": sha256_file(ROOT / DIAGNOSTIC_TERMINAL),
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
            "diagnostic_parquet_count": 1,
            "diagnostic_parquet_bytes": (ROOT / DIAGNOSTIC_PARQUET).stat().st_size,
            "diagnostic_parquet_sha256": sha256_file(ROOT / DIAGNOSTIC_PARQUET),
            "raw_or_derived_parquet_recommended_for_git_stage": False,
        },
        "group_diagnostic_successor": {
            "operation": "DIAGNOSE_APEX_MICRO_PHASE2_FIRST_GROUP_V2_ONCE",
            "source_count": 5,
            "source_bytes": 86_344_286,
            "source_market": "M6E",
            "source_year": 2018,
            "schemas": ["definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"],
            "maximum_workers": 1,
            "maximum_batch_rows": 100_000,
            "maximum_runtime_seconds": 900,
            "maximum_output_bytes": 16_777_216,
            "required_free_disk_bytes": 1_073_741_824,
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "provider_calls": 0,
            "external_cost_usd": "0",
            "dbn_reachable": False,
            "sixth_parquet_source_reachable": False,
            "phase2_parquet_creation_reachable": False,
            "raw_values_reported": False,
        },
        "authority_and_effects": {
            "group_diagnostic_row_batches_opened": 0,
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
        "after_commit": "CREATE_ONLY_FIRST_GROUP_DIAGNOSTIC_PLAN_AND_AUDIT",
        "next_research_boundary": "FRESH_EXACT_FIVE_SOURCE_GROUP_DIAGNOSTIC_APPROVAL",
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
