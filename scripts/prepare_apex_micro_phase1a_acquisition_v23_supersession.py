"""Seal the unexecuted v23 volatile-capacity audit defect."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
PLAN = Path("configs/apex_micro_tier01_phase1a_acquisition_plan_v23.json")
AUDIT = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v23/audit.json"
)
CENSUS = Path(
    "state/unpublished_evidence/safe_cleanup_candidate_census_v8/census.json"
)
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1a_acquisition_v23_supersession/report.json"
)
EXPECTED_HEAD = "6f5a96e51b31fd722824ebd9ebfd6d384b7ae86e"
EXPECTED_PLAN_ID = (
    "a1121d8aa7980cf757d4492f86c9e160effbd9c767a03666e05e5079c12ffb79"
)
EXPECTED_PLAN_SHA256 = (
    "90b5711d558f04c55bf0b88f5dce516a75fc86b19343c59b80f893d34342d299"
)
EXPECTED_AUDIT_ID = (
    "978ba98c3fde9b1d57ea90b7d095be94695c6043b53744bb8907c7248c8edc94"
)
EXPECTED_AUDIT_SHA256 = (
    "8d31f40d5d8b130fbde311023b27723b004de89d201597d9b58f2944edc94054"
)
EXPECTED_CENSUS_ID = (
    "74c1c04692e7237565c14e226005844614ff074b5f3952584a820c4c60d423f8"
)
EXPECTED_CENSUS_SHA256 = (
    "e727cd6bb2bd4562882ef9206a858de94ed0b2d6c37fda87cac08bf4284976ce"
)


def _object(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise IntegrityError("v23 supersession input is not an object")
    return value


def _self_hashed(value: dict[str, object], key: str) -> bool:
    core = dict(value)
    observed = core.pop(key, None)
    return observed == sha256_json(core)


def build_report(*, root: Path = ROOT) -> dict[str, object]:
    root = root.resolve(strict=True)
    plan = _object(root / PLAN)
    audit = _object(root / AUDIT)
    census = _object(root / CENSUS)
    capacity = audit.get("capacity")
    if (
        plan.get("plan_id") != EXPECTED_PLAN_ID
        or not _self_hashed(plan, "plan_id")
        or sha256_file(root / PLAN) != EXPECTED_PLAN_SHA256
        or audit.get("audit_id") != EXPECTED_AUDIT_ID
        or not _self_hashed(audit, "audit_id")
        or sha256_file(root / AUDIT) != EXPECTED_AUDIT_SHA256
        or census.get("census_id") != EXPECTED_CENSUS_ID
        or not _self_hashed(census, "census_id")
        or sha256_file(root / CENSUS) != EXPECTED_CENSUS_SHA256
        or plan.get("committed_implementation_head") != EXPECTED_HEAD
        or audit.get("observed_head") != EXPECTED_HEAD
        or census.get("committed_head") != EXPECTED_HEAD
        or census.get("worktree_paths_preserved")
        != ["CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md"]
        or not isinstance(capacity, dict)
        or type(capacity.get("observed_free_disk_bytes")) is not int
        or capacity.get("fits_disk") is not True
    ):
        raise IntegrityError("unexecuted v23 preparation evidence drifted")
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1a_acquisition_v23_supersession/1.0.0",
        "state": "SUPERSEDED_PREPARATION_VOLATILE_CAPACITY_SNAPSHOT",
        "committed_implementation_head": EXPECTED_HEAD,
        "plan": {
            "path": PLAN.as_posix(),
            "plan_id": EXPECTED_PLAN_ID,
            "sha256": EXPECTED_PLAN_SHA256,
            "provider_execution_performed": False,
            "authorization_consumed": False,
        },
        "audit": {
            "path": AUDIT.as_posix(),
            "audit_id": EXPECTED_AUDIT_ID,
            "sha256": EXPECTED_AUDIT_SHA256,
            "state_at_creation": audit["state"],
            "plan_and_census_reconstruction_passed": True,
            "post_creation_exact_audit_reconstruction_passed": False,
        },
        "cleanup_census": {
            "path": CENSUS.as_posix(),
            "census_id": EXPECTED_CENSUS_ID,
            "sha256": EXPECTED_CENSUS_SHA256,
            "post_creation_reconstruction_passed": True,
        },
        "defect": {
            "classification": "SELF_HASHED_VOLATILE_FREE_DISK_BYTE_SNAPSHOT",
            "volatile_field": "capacity.observed_free_disk_bytes",
            "disk_capacity_gate_passed_at_creation": True,
            "provider_or_data_fact_implicated": False,
            "raw_dbn_implicated": False,
        },
        "disposition": {
            "execute_v23_plan": False,
            "overwrite_or_delete_v23_artifacts": False,
            "successor_requires_new_committed_implementation": True,
            "successor_omits_exact_volatile_free_space_from_self_hashed_audit": True,
            "successor_rechecks_live_free_space_before_execution": True,
        },
        "effects": {
            "provider_calls": 0,
            "downloads": 0,
            "dbn_rows_decoded": 0,
            "year_2025_or_2026_payloads_opened": 0,
            "catalog_or_pointer_activated": False,
            "cleanup_mutation_performed": False,
            "publication_registration_evaluation_or_trading": False,
        },
    }
    return {**core, "report_id": sha256_json(core)}


def main() -> int:
    report = build_report()
    output = ROOT / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
    print(
        json.dumps(
            {
                "report_id": report["report_id"],
                "report_sha256": sha256_file(output),
                "state": report["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
