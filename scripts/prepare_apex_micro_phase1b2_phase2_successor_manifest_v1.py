"""Freeze the definition result and full Phase 2 successor consolidation."""

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
from futures_rebuild import micro_alpha_phase1b2_phase2_successor as successor  # noqa: E402


OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_phase2_successor_manifest_v1/manifest.json"
)
PREDECESSOR = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_definition_duplicate_manifest_v1/manifest.json"
)
DUPLICATE_PLAN = successor.DUPLICATE_PLAN_PATH
DUPLICATE_AUDIT = successor.DUPLICATE_AUDIT_PATH
DUPLICATE_REPORT = successor.DUPLICATE_REPORT_PATH
DUPLICATE_TERMINAL = successor.DUPLICATE_TERMINAL_PATH
AUTHORIZATION_USE = Path(
    "state/authorization_uses/"
    "7ea844c241e6766a0ece34c4049cd26ff950ce72a554cc8247e9ac59c681e767.json"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
RECOMMENDED = (
    "PIPELINE_FOLDER_MAP.md",
    "PROJECT_OUTLINE.md",
    DUPLICATE_PLAN.as_posix(),
    "scripts/prepare_apex_micro_phase1b2_phase2_successor_v4.py",
    "scripts/prepare_apex_micro_phase1b2_phase2_successor_manifest_v1.py",
    "src/futures_rebuild/micro_alpha_phase1b2_phase2_successor.py",
    "src/futures_rebuild/research_gateway_policy.py",
    AUTHORIZATION_USE.as_posix(),
    DUPLICATE_AUDIT.as_posix(),
    DUPLICATE_REPORT.as_posix(),
    DUPLICATE_TERMINAL.as_posix(),
    OUTPUT.as_posix(),
    "tests/test_micro_alpha_phase1b2_phase2_certification_v4.py",
    "tests/test_micro_alpha_phase1b2_phase2_certification_manifest_v1.py",
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
        raise IntegrityError("Phase 2 successor commit differs from exact scope")
    value = json.loads(_committed_bytes(commit, OUTPUT.as_posix()))
    core = dict(value)
    if core.pop("manifest_id", None) != sha256_json(core):
        raise IntegrityError("Phase 2 successor manifest identity drifted")
    records = {item["path"]: item["sha256"] for item in value["records"]}
    for path in RECOMMENDED:
        if path == OUTPUT.as_posix():
            if records[path] != "SELF_HASHED_AT_WRITE":
                raise IntegrityError("Phase 2 successor self marker drifted")
        elif hashlib.sha256(_committed_bytes(commit, path)).hexdigest() != records[path]:
            raise IntegrityError("Phase 2 successor committed hash drifted")
    return value


def build_manifest() -> dict[str, object]:
    if _tracked(OUTPUT):
        return _postcommit_manifest()
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("Phase 2 successor manifest requires empty index")
    expected = set(RECOMMENDED) | set(PRESERVED_UNSTAGED)
    if not (ROOT / OUTPUT).exists():
        expected.remove(OUTPUT.as_posix())
    if _worktree_paths() != expected:
        raise IntegrityError("Phase 2 successor worktree differs from exact scope")
    predecessor = _self_hashed(PREDECESSOR, "manifest_id", "definition predecessor manifest")
    plan = _self_hashed(DUPLICATE_PLAN, "plan_id", "definition diagnostic plan")
    audit = _self_hashed(DUPLICATE_AUDIT, "audit_id", "definition diagnostic audit")
    report = _self_hashed(DUPLICATE_REPORT, "report_id", "definition diagnostic report")
    terminal = _self_hashed(DUPLICATE_TERMINAL, "terminal_id", "definition diagnostic terminal")
    authorization = json.loads((ROOT / AUTHORIZATION_USE).read_text(encoding="utf-8"))
    result = report.get("result")
    if (
        predecessor.get("manifest_id")
        != "471ed5411dbae57e7a93018a92fabf25963723a789f3e2bb06cf8b6eb7760ee3"
        or audit.get("plan_id") != plan.get("plan_id")
        or report.get("state") != "PASS_DEFINITION_DUPLICATE_SEMANTICS_DIAGNOSTIC"
        or terminal.get("state") != "PASS_DEFINITION_DUPLICATE_SEMANTICS_DIAGNOSTIC"
        or terminal.get("report_id") != report.get("report_id")
        or not isinstance(result, dict)
        or result.get("classification") != "EXACT_SEMANTIC_DUPLICATES"
        or result.get("legacy_repeat_count") != 308
        or result.get("exact_semantic_duplicate_count") != 308
        or result.get("distinct_same_key_update_count") != 0
        or authorization.get("receipt_id") != report.get("authorization_receipt_id")
        or authorization.get("operation")
        != "DIAGNOSE_APEX_MICRO_DEFINITION_DUPLICATE_SEMANTICS_V3_ONCE"
    ):
        raise IntegrityError("Phase 2 successor predecessor evidence drifted")
    preview = successor.build_plan(
        root=ROOT, implementation_head=_git("rev-parse", "HEAD")
    )
    records = [
        {
            "path": path,
            "sha256": "SELF_HASHED_AT_WRITE" if path == OUTPUT.as_posix() else sha256_file(ROOT / path),
            "recommended_for_exact_stage": True,
        }
        for path in RECOMMENDED
    ]
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1b2_phase2_successor_manifest/1.0.0",
        "state": "PREPARED_REQUIRES_EXACT_PATH_STAGING_APPROVAL",
        "repository_root": ROOT.resolve().as_posix(),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream": _git("rev-parse", "--abbrev-ref", "@{upstream}"),
        "upstream_head": _git("rev-parse", "@{upstream}"),
        "predecessor_manifest_id": predecessor["manifest_id"],
        "predecessor_manifest_sha256": sha256_file(ROOT / PREDECESSOR),
        "definition_plan_id": plan["plan_id"],
        "definition_plan_sha256": sha256_file(ROOT / DUPLICATE_PLAN),
        "definition_audit_id": audit["audit_id"],
        "definition_audit_sha256": sha256_file(ROOT / DUPLICATE_AUDIT),
        "definition_report_id": report["report_id"],
        "definition_report_sha256": sha256_file(ROOT / DUPLICATE_REPORT),
        "definition_terminal_id": terminal["terminal_id"],
        "definition_terminal_sha256": sha256_file(ROOT / DUPLICATE_TERMINAL),
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
        "definition_result": {
            "classification": result["classification"],
            "row_count": result["row_count"],
            "legacy_repeat_count": result["legacy_repeat_count"],
            "exact_semantic_duplicate_count": result["exact_semantic_duplicate_count"],
            "distinct_same_key_update_count": result["distinct_same_key_update_count"],
            "definition_rows_deduplicated": 0,
            "raw_values_or_semantic_keys_reported": False,
        },
        "policy_control_value": {
            "concrete_risk_prevented": "DISTINCT_SAME_KEY_DEFINITION_UPDATES_CERTIFIED_AS_HARMLESS_DUPLICATES",
            "decision_improved": "ALLOW_ONLY_PROVEN_EXACT_RETAINED_SEMANTICS_REPEATS_WITHOUT_DEDUPLICATION",
            "simpler_rule_insufficient": "BLANKET_REJECTION_BLOCKS_PROVEN_EXACT_REPEATS_WHILE_BLANKET_ACCEPTANCE_HIDES_AMBIGUOUS_UPDATES",
        },
        "phase2_successor_preview": {
            "preview_plan_id": preview["plan_id"],
            "operation": preview["operation"],
            "source_count": preview["source_count"],
            "source_bytes": preview["source_bytes"],
            "coverage_cell_count": preview["coverage_cell_count"],
            "prelaunch_cell_count": preview["prelaunch_cell_count"],
            "interval_count": preview["interval_count"],
            "maximum_parquet_open_operations": preview["limits"]["maximum_parquet_open_operations"],
            "maximum_parquet_outputs": preview["limits"]["maximum_parquet_outputs"],
            "maximum_workers": preview["limits"]["maximum_workers"],
            "maximum_batch_rows": preview["limits"]["maximum_batch_rows"],
            "maximum_runtime_seconds": preview["limits"]["maximum_runtime_seconds"],
            "maximum_output_bytes": preview["limits"]["maximum_output_bytes"],
            "required_free_disk_bytes": preview["limits"]["required_free_disk_bytes"],
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "provider_calls": 0,
            "external_cost_usd": "0",
            "dbn_reachable": False,
            "year_2025_or_2026_reachable": False,
            "active_catalog_or_pointer_write_reachable": False,
        },
        "preserved_inactive_not_for_git": {
            "phase1b_parquet_count": 120,
            "phase1b_parquet_bytes": 6_627_486_838,
            "causal_diagnostic_parquet_count": 1,
            "raw_or_derived_parquet_recommended_for_git_stage": False,
        },
        "authority_and_effects": {
            "phase2_successor_payloads_opened": 0,
            "phase2_successor_outputs_created": 0,
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
        "after_commit": "CREATE_ONLY_FULL_PHASE2_SUCCESSOR_PLAN_AND_AUDIT",
        "next_research_boundary": "FRESH_EXACT_120_SOURCE_FULL_PHASE2_DERIVED_ROW_APPROVAL",
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
