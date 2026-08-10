from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for candidate in (ROOT, ROOT / "src"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json  # noqa: E402
from futures_rebuild.micro_alpha_custody_repair_v2 import (  # noqa: E402
    FAILURE_REPORT_PATH,
    PLAN_PATH,
    REPAIR_TERMINAL_PATH,
    V1_PLAN_PATH,
    V1_SUPERSESSION_PATH,
)


OUTPUT = Path(
    "state/unpublished_evidence/apex_micro_v24_custody_repair_v2_consolidation_manifest/manifest.json"
)
PRESERVED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
RECOMMENDED = (
    "PIPELINE_FOLDER_MAP.md",
    "PROJECT_OUTLINE.md",
    V1_PLAN_PATH.as_posix(),
    "scripts/prepare_apex_micro_v24_custody_repair_v2.py",
    "scripts/prepare_apex_micro_v24_custody_repair_v2_consolidation_manifest.py",
    "src/futures_rebuild/micro_alpha_custody_repair_v2.py",
    "src/futures_rebuild/research_gateway_policy.py",
    V1_SUPERSESSION_PATH.as_posix(),
    OUTPUT.as_posix(),
    "tests/test_micro_alpha_custody_repair_v1.py",
    "tests/test_micro_alpha_custody_repair_v2.py",
)


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()


def _changed_paths() -> set[str]:
    tracked = set(filter(None, _git("diff", "--name-only").splitlines()))
    staged = set(filter(None, _git("diff", "--cached", "--name-only").splitlines()))
    untracked = set(
        filter(None, _git("ls-files", "--others", "--exclude-standard").splitlines())
    )
    return {item.replace("\\", "/") for item in tracked | staged | untracked}


def _assert_exact_worktree(*, output_exists: bool) -> None:
    expected = set(RECOMMENDED) | set(PRESERVED)
    if not output_exists:
        expected.remove(OUTPUT.as_posix())
    actual = _changed_paths()
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise SystemExit(f"worktree census differs; missing={missing}; extra={extra}")


def _load(path: Path) -> dict[str, object]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if type(value) is not dict:
        raise SystemExit(f"expected JSON object: {path}")
    return value


def build_manifest() -> dict[str, object]:
    v1_plan = _load(V1_PLAN_PATH)
    supersession = _load(V1_SUPERSESSION_PATH)
    failure = _load(FAILURE_REPORT_PATH)
    records = [
        {
            "path": relative,
            "recommended_for_exact_stage": True,
            "sha256": (
                "SELF_HASHED_AT_WRITE"
                if relative == OUTPUT.as_posix()
                else sha256_file(ROOT / relative)
            ),
        }
        for relative in RECOMMENDED
    ]
    core = {
        "schema_version": "apex_micro_v24_custody_repair_v2_consolidation_manifest/1.0.0",
        "state": "PREPARED_EXACT_PATH_STAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream": _git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"),
        "upstream_head": _git("rev-parse", "@{u}"),
        "v24_evidence": {
            "acquisition_plan_id": failure["v24_plan_id"],
            "acquisition_plan_sha256": failure["v24_plan_sha256"],
            "acquisition_terminal_id": failure["v24_terminal_id"],
            "acquisition_terminal_sha256": failure["v24_terminal_sha256"],
            "verification_failure_report_id": failure["report_id"],
            "verification_failure_report_sha256": sha256_file(ROOT / FAILURE_REPORT_PATH),
            "accepted_dbn_count": 160,
            "accepted_sidecar_count": 160,
            "external_cost_usd": "0",
            "automatic_retries": 0,
        },
        "v1_supersession": {
            "plan_id": v1_plan["plan_id"],
            "plan_sha256": sha256_file(ROOT / V1_PLAN_PATH),
            "report_id": supersession["report_id"],
            "report_sha256": sha256_file(ROOT / V1_SUPERSESSION_PATH),
            "state": supersession["state"],
            "authorization_consumed": False,
            "terminal_exists": (ROOT / "state/unpublished_evidence/apex_micro_v24_custody_repair_v1/terminal.json").exists(),
        },
        "v2_preparation": {
            "operation": "REPAIR_APEX_MICRO_V24_HARDLINK_CUSTODY_V2_ONCE",
            "exact_dbn_aliases": 160,
            "exact_sidecar_aliases": 160,
            "exact_total_aliases": 320,
            "provider_calls_permitted": 0,
            "dbn_row_decode_permitted": False,
            "plan_written": (ROOT / PLAN_PATH).exists(),
            "audit_written": (ROOT / "state/unpublished_evidence/apex_micro_v24_custody_repair_plan_v2/audit.json").exists(),
            "terminal_written": (ROOT / REPAIR_TERMINAL_PATH).exists(),
            "alias_mutation_performed": False,
            "implementation_commit_required_before_plan": True,
            "repair_authority_present": False,
        },
        "recommended_exact_stage_path_count": len(RECOMMENDED),
        "recommended_exact_stage_paths": list(RECOMMENDED),
        "records": records,
        "preserved_unstaged_paths": list(PRESERVED),
        "preserved_records": [
            {
                "path": relative,
                "recommended_for_exact_stage": False,
                "sha256": sha256_file(ROOT / relative),
            }
            for relative in PRESERVED
        ],
        "authority_and_effects": {
            "provider_access": False,
            "staging_alias_removed": 0,
            "final_file_deleted_overwritten_replaced_or_relabelled": 0,
            "dbn_payload_read": False,
            "dbn_rows_decoded": False,
            "year_2025_or_2026_payload_opened_for_row_access": False,
            "catalog_or_pointer_activated": False,
            "publication_registration_evaluation_or_trading": False,
            "git_staging_performed": False,
            "commit_performed": False,
            "push_performed": False,
        },
    }
    if core["v1_supersession"]["terminal_exists"] is not False:
        raise SystemExit("v1 repair terminal unexpectedly exists")
    if any(
        core["v2_preparation"][key] is not False
        for key in ("plan_written", "audit_written", "terminal_written")
    ):
        raise SystemExit("v2 plan, audit, or terminal unexpectedly exists")
    return {**core, "manifest_id": sha256_json(core)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("write", "check"))
    args = parser.parse_args()
    if args.command == "write":
        _assert_exact_worktree(output_exists=False)
        manifest = build_manifest()
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        with (ROOT / OUTPUT).open("xb") as stream:
            stream.write(canonical_bytes(manifest) + b"\n")
        _assert_exact_worktree(output_exists=True)
    else:
        _assert_exact_worktree(output_exists=True)
        manifest = build_manifest()
        existing = _load(OUTPUT)
        if manifest != existing:
            raise SystemExit("v2 consolidation manifest reconstruction differs")
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
