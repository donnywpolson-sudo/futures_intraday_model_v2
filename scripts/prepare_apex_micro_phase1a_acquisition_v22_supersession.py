"""Seal the unexecuted v22 plan/audit self-referential census defect."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
PLAN = Path("configs/apex_micro_tier01_phase1a_acquisition_plan_v22.json")
AUDIT = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v22/audit.json"
)
CENSUS = Path(
    "state/unpublished_evidence/safe_cleanup_candidate_census_v7/census.json"
)
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1a_acquisition_v22_supersession/report.json"
)
EXPECTED_HEAD = "84510b45bab61a3efe9cfec5df8d355e1711c767"
EXPECTED_PLAN_ID = (
    "d0fbcaa2e787910d7bf90d55a8ce623380bc5026386dd899042b918f85556e37"
)
EXPECTED_PLAN_SHA256 = (
    "829972937ad2baa35a06eeb131e000a5766f18447d0007aa22e11f1d4c0245de"
)
EXPECTED_AUDIT_ID = (
    "d09bd0855a40c63de428fe8f25e9a3d00b3e85819e2bdbd73d570499d874eb13"
)
EXPECTED_AUDIT_SHA256 = (
    "561a196bc8059e0ac241e3f2f828f09df6489cfa7fd7b2488c19bc4111ab775f"
)
EXPECTED_CENSUS_ID = (
    "4e065c60f31cc9f5c1e40155e01eb2ea21638827a05174252a4693d557ea8639"
)
EXPECTED_CENSUS_SHA256 = (
    "97d145ad6a3f5f25a6c2a27c665f649972573715cefd3e986476da77b4b5cf57"
)


def _object(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise IntegrityError("v22 supersession input is not an object")
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
    ):
        raise IntegrityError("unexecuted v22 preparation evidence drifted")
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1a_acquisition_v22_supersession/1.0.0",
        "state": "SUPERSEDED_PREPARATION_SELF_REFERENTIAL_CENSUS",
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
            "post_creation_reconstruction_passed": False,
        },
        "cleanup_census": {
            "path": CENSUS.as_posix(),
            "census_id": EXPECTED_CENSUS_ID,
            "sha256": EXPECTED_CENSUS_SHA256,
            "recorded_worktree_paths": census["worktree_paths_preserved"],
        },
        "defect": {
            "classification": "WORKTREE_SNAPSHOT_SELF_REFERENTIAL_OUTPUT_SET",
            "post_creation_paths_missing_from_recorded_snapshot": [
                PLAN.as_posix(),
                "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v22/",
                "state/unpublished_evidence/safe_cleanup_candidate_census_v7/",
            ],
            "provider_or_data_fact_implicated": False,
            "raw_dbn_implicated": False,
        },
        "disposition": {
            "execute_v22_plan": False,
            "overwrite_or_delete_v22_artifacts": False,
            "successor_requires_new_committed_implementation": True,
            "successor_census_excludes_only_declared_create_only_outputs": True,
            "successor_binds_superseded_artifacts_explicitly": True,
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
