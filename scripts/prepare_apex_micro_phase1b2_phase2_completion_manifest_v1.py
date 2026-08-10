"""Freeze the certified inactive micro Phase 2 completion scope."""

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
from futures_rebuild.micro_alpha_phase1b2_preparation import (  # noqa: E402
    ACTIVE_MICRO_CATALOG_PATH,
    ACTIVE_MICRO_POINTER_PATH,
)


OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_phase2_completion_manifest_v1/manifest.json"
)
PLAN = Path("configs/apex_micro_phase1b2_phase2_successor_plan_v4.json")
AUDIT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_phase2_successor_plan_v4/audit.json"
)
AUTHORIZATION_USE = Path(
    "state/authorization_uses/"
    "eb1634299e0ae9d83406cd42e07abff382a6a4b606949f30656bdb4491e3f86b.json"
)
EVIDENCE_ROOT = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_successor_v4/"
    "49beef616481be65a191d2da"
)
REPORT = EVIDENCE_ROOT / "source_certification_report.json"
CANDIDATE = EVIDENCE_ROOT / "inactive_catalog_candidate.json"
TERMINAL = EVIDENCE_ROOT / "terminal.json"
FAILURE_REPORT = EVIDENCE_ROOT / "failure_report.json"
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
RECOMMENDED = (
    "PIPELINE_FOLDER_MAP.md",
    "PROJECT_OUTLINE.md",
    PLAN.as_posix(),
    "scripts/prepare_apex_micro_phase1b2_phase2_completion_manifest_v1.py",
    AUTHORIZATION_USE.as_posix(),
    AUDIT.as_posix(),
    REPORT.as_posix(),
    CANDIDATE.as_posix(),
    TERMINAL.as_posix(),
    OUTPUT.as_posix(),
    "tests/test_micro_alpha_phase1b2_phase2_completion_manifest_v1.py",
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
        raise IntegrityError("Phase 2 completion commit differs from exact scope")
    value = json.loads(_committed_bytes(commit, OUTPUT.as_posix()))
    core = dict(value)
    if core.pop("manifest_id", None) != sha256_json(core):
        raise IntegrityError("Phase 2 completion manifest identity drifted")
    records = {item["path"]: item["sha256"] for item in value["records"]}
    for path in RECOMMENDED:
        if path == OUTPUT.as_posix():
            if records[path] != "SELF_HASHED_AT_WRITE":
                raise IntegrityError("Phase 2 completion self marker drifted")
        elif hashlib.sha256(_committed_bytes(commit, path)).hexdigest() != records[path]:
            raise IntegrityError("Phase 2 completion committed hash drifted")
    return value


def _output_records(report: dict[str, object], staging_root: Path) -> list[dict[str, object]]:
    phase2_records = report.get("phase2_records")
    if not isinstance(phase2_records, list) or len(phase2_records) != 24:
        raise IntegrityError("Phase 2 completion output census drifted")
    root_resolved = (ROOT / staging_root).resolve(strict=True)
    records: list[dict[str, object]] = []
    for record in phase2_records:
        if not isinstance(record, dict):
            raise IntegrityError("Phase 2 completion output record is invalid")
        relative = record.get("output_path")
        expected_sha = record.get("output_sha256")
        expected_bytes = record.get("output_bytes")
        if not isinstance(relative, str) or not isinstance(expected_sha, str):
            raise IntegrityError("Phase 2 completion output binding is invalid")
        if type(expected_bytes) is not int:
            raise IntegrityError("Phase 2 completion output byte binding is invalid")
        path = (ROOT / relative).resolve(strict=True)
        try:
            path.relative_to(root_resolved)
        except ValueError as exc:
            raise IntegrityError("Phase 2 completion output escaped inactive staging") from exc
        if path.is_symlink() or not path.is_file():
            raise IntegrityError("Phase 2 completion output is not a plain file")
        actual_bytes = path.stat().st_size
        actual_sha = sha256_file(path)
        if actual_bytes != expected_bytes or actual_sha != expected_sha:
            raise IntegrityError("Phase 2 completion output identity drifted")
        records.append(
            {
                "path": relative,
                "bytes": actual_bytes,
                "sha256": actual_sha,
                "market": record.get("market"),
                "year": record.get("year"),
                "interval": record.get("interval"),
                "phase2_release_id": record.get("phase2_release_id"),
            }
        )
    records.sort(key=lambda item: str(item["path"]))
    if len({str(item["path"]) for item in records}) != 24:
        raise IntegrityError("Phase 2 completion output paths are not unique")
    return records


def build_manifest() -> dict[str, object]:
    if _tracked(OUTPUT):
        return _postcommit_manifest()
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("Phase 2 completion manifest requires empty index")
    expected = set(RECOMMENDED) | set(PRESERVED_UNSTAGED)
    if not (ROOT / OUTPUT).exists():
        expected.remove(OUTPUT.as_posix())
    if _worktree_paths() != expected:
        raise IntegrityError("Phase 2 completion worktree differs from exact scope")

    plan = _self_hashed(PLAN, "plan_id", "Phase 2 successor plan")
    audit = _self_hashed(AUDIT, "audit_id", "Phase 2 successor audit")
    report = _self_hashed(REPORT, "source_certification_id", "source certification")
    candidate = _self_hashed(CANDIDATE, "catalog_candidate_id", "catalog candidate")
    terminal = _self_hashed(TERMINAL, "terminal_id", "Phase 2 terminal")
    authorization = json.loads((ROOT / AUTHORIZATION_USE).read_text(encoding="utf-8"))
    output_records = _output_records(report, Path(str(plan["staging_root"])))
    output_bytes = sum(int(item["bytes"]) for item in output_records)
    coverage = report.get("coverage_census")
    disposition_counts = coverage.get("disposition_counts") if isinstance(coverage, dict) else None

    evidence_files = {item.name for item in (ROOT / EVIDENCE_ROOT).iterdir() if item.is_file()}
    if (
        plan.get("implementation_head") != "21069d7210afa967557480dcc1035cb61b869fa2"
        or audit.get("plan_id") != plan.get("plan_id")
        or audit.get("plan_sha256") != sha256_file(ROOT / PLAN)
        or report.get("plan_id") != plan.get("plan_id")
        or terminal.get("plan_id") != plan.get("plan_id")
        or terminal.get("audit_id") != audit.get("audit_id")
        or terminal.get("state") != "SUCCESS_CERTIFIED_INACTIVE_PHASE2"
        or report.get("state") != "CERTIFIED_INACTIVE_NOT_PUBLISHED"
        or candidate.get("state") != "CERTIFIED_INACTIVE_NOT_PUBLISHED"
        or terminal.get("completed_source_scan_count") != 120
        or terminal.get("completed_phase2_count") != 24
        or terminal.get("created_output_bytes") != output_bytes
        or terminal.get("source_hashes_verified_after_construction") != 120
        or terminal.get("failure_type") is not None
        or terminal.get("attempts") != 1
        or terminal.get("automatic_retries") != 0
        or terminal.get("provider_calls") != 0
        or terminal.get("external_cost_usd") != "0"
        or terminal.get("dbn_payloads_opened") != 0
        or terminal.get("year_2025_or_2026_payloads_opened") != 0
        or terminal.get("raw_values_or_semantic_keys_reported") is not False
        or terminal.get("published_activated_registered_evaluated_or_traded") is not False
        or terminal.get("catalog_or_pointer_activated") is not False
        or terminal.get("terminal_written_last") is not True
        or report.get("source_count") != 120
        or report.get("source_bytes") != 6_627_486_838
        or report.get("identity_and_roll_continuity_certified") is not True
        or report.get("catalog_candidate_eligible") is not True
        or report.get("definition_rows_deduplicated") != 0
        or report.get("status_and_statistics_used_as_features") is not False
        or report.get("one_second_evidence_semantics") != "REPORTED_TRADE_BARS_ONLY"
        or report.get("one_second_bbo_queue_guaranteed_fill_or_within_second_ordering_claimed") is not False
        or report.get("year_2025_or_2026_materialized") is not False
        or report.get("raw_values_or_semantic_keys_reported") is not False
        or report.get("features_outcomes_predictions_returns_or_evaluation_created") is not False
        or report.get("published") is not False
        or report.get("catalog_or_pointer_activated") is not False
        or not isinstance(coverage, dict)
        or coverage.get("cell_count") != 140
        or disposition_counts != {"ACCEPTED": 120, "PRODUCT_NOT_YET_EFFECTIVE": 20}
        or candidate.get("source_certification_id") != report.get("source_certification_id")
        or candidate.get("source_certification_sha256") != sha256_file(ROOT / REPORT)
        or candidate.get("coverage_cell_count") != 140
        or candidate.get("actual_identity_and_roll_continuity_certified") is not True
        or candidate.get("disposition_census_complete") is not True
        or candidate.get("published") is not False
        or candidate.get("active_pointer_written") is not False
        or candidate.get("holdout_2025_materialized") is not False
        or candidate.get("forward_2026_materialized") is not False
        or authorization.get("receipt_id") != report.get("authorization_receipt_id")
        or authorization.get("operation") != plan.get("operation")
        or evidence_files != {REPORT.name, CANDIDATE.name, TERMINAL.name}
        or (ROOT / FAILURE_REPORT).exists()
        or (ROOT / ACTIVE_MICRO_CATALOG_PATH).exists()
        or (ROOT / ACTIVE_MICRO_POINTER_PATH).exists()
    ):
        raise IntegrityError("Phase 2 completion evidence drifted")

    records = [
        {
            "path": path,
            "sha256": "SELF_HASHED_AT_WRITE" if path == OUTPUT.as_posix() else sha256_file(ROOT / path),
            "recommended_for_exact_stage": True,
        }
        for path in RECOMMENDED
    ]
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1b2_phase2_completion_manifest/1.0.0",
        "state": "PREPARED_REQUIRES_EXACT_PATH_STAGING_APPROVAL",
        "repository_root": ROOT.resolve().as_posix(),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream": _git("rev-parse", "--abbrev-ref", "@{upstream}"),
        "upstream_head": _git("rev-parse", "@{upstream}"),
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(ROOT / PLAN),
        "audit_id": audit["audit_id"],
        "audit_sha256": sha256_file(ROOT / AUDIT),
        "authorization_receipt_id": authorization["receipt_id"],
        "authorization_use_sha256": sha256_file(ROOT / AUTHORIZATION_USE),
        "source_certification_id": report["source_certification_id"],
        "source_certification_sha256": sha256_file(ROOT / REPORT),
        "catalog_candidate_id": candidate["catalog_candidate_id"],
        "catalog_candidate_sha256": sha256_file(ROOT / CANDIDATE),
        "terminal_id": terminal["terminal_id"],
        "terminal_sha256": sha256_file(ROOT / TERMINAL),
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
        "certified_inactive_phase2": {
            "state": terminal["state"],
            "markets": list(plan["markets"]),
            "years": list(plan["years"]),
            "schemas": list(plan["schemas"]),
            "source_count": 120,
            "source_bytes": 6_627_486_838,
            "source_hashes_verified_after_construction": 120,
            "coverage_cell_count": 140,
            "accepted_cell_count": 120,
            "prelaunch_cell_count": 20,
            "five_schema_interval_count": 24,
            "phase2_output_count": 24,
            "phase2_output_bytes": output_bytes,
            "phase2_output_set_sha256": sha256_json(output_records),
            "definition_repeat_classification_counts": terminal[
                "definition_repeat_classification_counts"
            ],
            "definition_rows_deduplicated": 0,
            "identity_and_roll_continuity_certified": True,
            "one_second_evidence_semantics": "REPORTED_TRADE_BARS_ONLY",
            "status_and_statistics_used_as_features": False,
        },
        "inactive_custody_not_for_git": {
            "staging_root": plan["staging_root"],
            "phase2_outputs": output_records,
            "raw_or_derived_parquet_recommended_for_git_stage": False,
            "catalog_candidate_state": candidate["state"],
            "catalog_candidate_published": False,
            "active_catalog_or_pointer_exists": False,
        },
        "authority_and_effects": {
            "attempts": 1,
            "automatic_retries": 0,
            "dbn_payloads_opened": 0,
            "year_2025_or_2026_payloads_opened": 0,
            "provider_or_network_calls": 0,
            "external_cost_usd": "0",
            "credential_access": False,
            "raw_values_or_semantic_keys_reported": False,
            "phase1b_rows_deduplicated": 0,
            "catalog_or_pointer_activated": False,
            "published_registered_evaluated_or_traded": False,
            "standard_lane_mutated": False,
            "git_staging": False,
            "git_commit": False,
            "git_push": False,
        },
        "remaining_boundaries": {
            "micro_catalog_publication_or_activation": "SEPARATE_APPROVAL_REQUIRED",
            "mechanism_tier0_registration_or_evaluation": "SEPARATE_APPROVAL_REQUIRED",
            "holdout_2025_or_forward_2026_access": "SEPARATE_APPROVAL_REQUIRED",
            "official_micro_commission_freeze": "UNRESOLVED_LATER_MECHANISM_BLOCKER",
            "safe_cleanup_mutation": "SEPARATE_EXACT_MANIFEST_AND_APPROVAL_REQUIRED",
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
