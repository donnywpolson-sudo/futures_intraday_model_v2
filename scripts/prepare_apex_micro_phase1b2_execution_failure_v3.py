"""Seal the consumed v3 Phase 1B-complete, Phase 2-zero failure metadata."""

from __future__ import annotations

import json
import stat
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for candidate in (ROOT, ROOT / "src"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json  # noqa: E402
from futures_rebuild.errors import IntegrityError  # noqa: E402


PLAN_PATH = Path("configs/apex_micro_phase1b2_historical_execution_plan_v3.json")
AUDIT_PATH = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v3/audit.json"
)
TERMINAL_PATH = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_execution_v3/"
    "f28cb40f23574e6905a10ff2/terminal.json"
)
AUTHORIZATION_USE_PATH = Path(
    "state/authorization_uses/"
    "c9eab43fa35842756ff855dbfb793aacbf8edc7e81d71a6abfd3ea61f21be430.json"
)
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_execution_plan_v3_supersession/report.json"
)
PLAN_ID = "69b3aba72dd86d86651ce3570c57d4eb736623a5525b8a2793819f6c31affcff"
PLAN_SHA256 = "1ffe24dbc6cd2a8ef659565931f9e5b72ef93ecf1eba1cfcbe354c756ed23c62"
AUDIT_ID = "5003e5184883b5000366aaffea749606b854ca57c05e56af90a197929f30ba2f"
AUDIT_SHA256 = "139d3ada49fa98d2f57ff0bc8a21d99aef0a885e0520a74f69a6099a3b0b886c"
TERMINAL_ID = "09dc136e82c25258ca2b9eb3ea84c7a78ca13275e6c5e722319591c41fc4ebcd"
TERMINAL_SHA256 = "24efff0ba370727935e855fb81a13f45d4fc3cdc1d52d11b40a276873d2e61af"
AUTHORIZATION_RECEIPT_ID = (
    "c9eab43fa35842756ff855dbfb793aacbf8edc7e81d71a6abfd3ea61f21be430"
)
IMPLEMENTATION_HEAD = "d678552a2f7014c3bcfa12e8629b0516c46789e8"
EXPECTED_OUTPUT_BYTES = 6_627_486_838


def _load(path: Path, description: str) -> dict[str, object]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if type(value) is not dict:
        raise IntegrityError(f"{description} is invalid")
    return value


def build_report() -> dict[str, object]:
    plan = _load(PLAN_PATH, "v3 plan")
    audit = _load(AUDIT_PATH, "v3 audit")
    terminal = _load(TERMINAL_PATH, "v3 terminal")
    authorization_use = _load(AUTHORIZATION_USE_PATH, "v3 authorization use")
    if (
        plan.get("plan_id") != PLAN_ID
        or sha256_file(ROOT / PLAN_PATH) != PLAN_SHA256
        or audit.get("audit_id") != AUDIT_ID
        or sha256_file(ROOT / AUDIT_PATH) != AUDIT_SHA256
        or terminal.get("terminal_id") != TERMINAL_ID
        or sha256_file(ROOT / TERMINAL_PATH) != TERMINAL_SHA256
        or terminal.get("authorization_receipt_id") != AUTHORIZATION_RECEIPT_ID
        or authorization_use.get("receipt_id") != AUTHORIZATION_RECEIPT_ID
        or plan.get("implementation_head") != IMPLEMENTATION_HEAD
    ):
        raise IntegrityError("v3 Phase 1B/2 failure binding drifted")
    staging = ROOT / str(plan["staging_root"])
    expected = {
        (staging / str(item["phase1b_output_path"])).resolve(strict=False): item
        for item in plan["sources"]
    }
    observed = {path.resolve(strict=False) for path in staging.rglob("*") if path.is_file()}
    if observed != set(expected):
        raise IntegrityError("v3 staged Phase 1B file set differs from the exact plan")
    inventory: list[dict[str, object]] = []
    for path in sorted(observed):
        info = path.stat()
        if not path.is_file() or not bool(info.st_file_attributes & stat.FILE_ATTRIBUTE_READONLY):
            raise IntegrityError("v3 staged Phase 1B file is not sealed read-only")
        item = expected[path]
        inventory.append(
            {
                "request_id": item["request_id"],
                "market": item["market"],
                "schema": item["schema"],
                "year": item["year"],
                "relative_path": path.relative_to(ROOT).as_posix(),
                "bytes": info.st_size,
                "sha256": sha256_file(path),
                "source_sha256": item["source_sha256"],
                "phase1b_release_id": item["phase1b_release_id"],
            }
        )
    if (
        terminal.get("state") != "FAILURE_INACTIVE_PARTIAL_EVIDENCE_PRESERVED"
        or terminal.get("failure") != {"exception_type": "IntegrityError"}
        or terminal.get("source_hashes_verified_before_decode") != 120
        or terminal.get("completed_decode_count") != 120
        or terminal.get("completed_phase2_count") != 0
        or terminal.get("created_output_bytes") != EXPECTED_OUTPUT_BYTES
        or terminal.get("year_2025_or_2026_payloads_opened") != 0
        or len(inventory) != 120
        or sum(int(item["bytes"]) for item in inventory) != EXPECTED_OUTPUT_BYTES
    ):
        raise IntegrityError("v3 Phase 1B-complete failure evidence differs")
    core = {
        "schema_version": "apex_micro_phase1b2_execution_failure/3.0.0",
        "state": "SUPERSEDED_PHASE1B_COMPLETE_PHASE2_TRANSITION_FAILED_CLOSED",
        "plan_id": PLAN_ID,
        "plan_sha256": PLAN_SHA256,
        "audit_id": AUDIT_ID,
        "audit_sha256": AUDIT_SHA256,
        "terminal_id": TERMINAL_ID,
        "terminal_sha256": TERMINAL_SHA256,
        "authorization_receipt_id": AUTHORIZATION_RECEIPT_ID,
        "authorization_use_sha256": sha256_file(ROOT / AUTHORIZATION_USE_PATH),
        "implementation_head": IMPLEMENTATION_HEAD,
        "authorization_receipt_consumed": True,
        "attempts": 1,
        "automatic_retries": 0,
        "source_hashes_verified": 120,
        "completed_phase1b_outputs": 120,
        "completed_phase2_outputs": 0,
        "created_output_bytes": EXPECTED_OUTPUT_BYTES,
        "phase1b_inventory": inventory,
        "phase1b_inventory_id": sha256_json(inventory),
        "failure_type_recorded": "IntegrityError",
        "failure_stage": "AFTER_PHASE1B_BEFORE_FIRST_COMPLETED_PHASE2_OUTPUT",
        "diagnosis_state": "BOUNDED_DERIVED_ROW_DIAGNOSTIC_REQUIRED",
        "dbn_rows_reopened_during_failure_seal": 0,
        "parquet_rows_opened_during_failure_seal": 0,
        "raw_values_reported": False,
        "year_2025_or_2026_payloads_opened": 0,
        "provider_or_network_calls": 0,
        "external_cost_usd": "0",
        "published": False,
        "catalog_or_pointer_activated": False,
        "registered_evaluated_or_traded": False,
        "predecessor_artifacts_overwritten": False,
        "v3_reexecution_permitted": False,
        "successor_requirement": (
            "COMMITTED_BOUNDED_PHASE2_DIAGNOSTIC_PLAN_AND_FRESH_ROW_READ_APPROVAL"
        ),
    }
    return {**core, "report_id": sha256_json(core)}


def write_create_only() -> dict[str, object]:
    value = build_report()
    target = ROOT / OUTPUT
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as stream:
        stream.write(canonical_bytes(value) + b"\n")
    target.chmod(stat.S_IREAD)
    return value


def main() -> int:
    value = write_create_only()
    print(
        json.dumps(
            {
                "report_id": value["report_id"],
                "sha256": sha256_file(ROOT / OUTPUT),
                "state": value["state"],
                "phase1b_output_count": len(value["phase1b_inventory"]),
                "phase1b_output_bytes": value["created_output_bytes"],
                "parquet_rows_opened": 0,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
