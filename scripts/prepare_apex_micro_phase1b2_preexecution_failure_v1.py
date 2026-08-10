"""Seal the v1 pre-consumption central-policy failure without row access."""

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


PLAN_PATH = Path("configs/apex_micro_phase1b2_historical_execution_plan_v1.json")
AUDIT_PATH = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v1/audit.json"
)
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_execution_plan_v1_supersession/report.json"
)
PLAN_ID = "193c85bb209ed107a2c82c76849d7da64cb4f996eca4ccd2935b286513797fa6"
PLAN_SHA256 = "2a22efb9398edafd4027a633d8f8409f1666fb36a303b628b41baf8cd115492f"
AUDIT_ID = "f193208964a4a2b7e9feeca0a5bb2f2cdf8d6e9df4aa538f2dc657ac2f718220"
AUDIT_SHA256 = "ad911a5fb409e162cc44eb774e87402f90dea1fe1f6261d94ea8f6b8763cb720"
IMPLEMENTATION_HEAD = "3a2bfb60414491be2d6fb39ffab0af28a09b7828"


def _load(path: Path) -> dict[str, object]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if type(value) is not dict:
        raise IntegrityError("pre-execution predecessor artifact is invalid")
    return value


def build_report() -> dict[str, object]:
    plan = _load(PLAN_PATH)
    audit = _load(AUDIT_PATH)
    if (
        plan.get("plan_id") != PLAN_ID
        or sha256_file(ROOT / PLAN_PATH) != PLAN_SHA256
        or audit.get("audit_id") != AUDIT_ID
        or sha256_file(ROOT / AUDIT_PATH) != AUDIT_SHA256
        or plan.get("implementation_head") != IMPLEMENTATION_HEAD
    ):
        raise IntegrityError("v1 Phase 1B/2 plan or audit binding drifted")
    core = {
        "schema_version": "apex_micro_phase1b2_preexecution_failure/1.0.0",
        "state": "SUPERSEDED_FAIL_CLOSED_BEFORE_AUTHORIZATION_CONSUMPTION",
        "plan_id": PLAN_ID,
        "plan_sha256": PLAN_SHA256,
        "audit_id": AUDIT_ID,
        "audit_sha256": AUDIT_SHA256,
        "implementation_head": IMPLEMENTATION_HEAD,
        "failure_type": "CENTRAL_PREPARATORY_OPERATION_ALLOWLIST_OMISSION",
        "failure_stage": "AUTHORIZATION_VERIFY_BEFORE_CONSUME",
        "authorization_receipt_consumed": False,
        "authorization_use_record_created": False,
        "execution_attempt_started": False,
        "source_hashes_read": 0,
        "dbn_rows_decoded": 0,
        "year_2025_or_2026_payloads_opened": 0,
        "staging_or_evidence_root_created": False,
        "provider_or_network_calls": 0,
        "external_cost_usd": "0",
        "automatic_retries": 0,
        "published": False,
        "catalog_or_pointer_activated": False,
        "registered_evaluated_or_traded": False,
        "predecessor_plan_or_audit_overwritten": False,
        "successor_requirement": (
            "EXACT_PREPARATORY_OPERATION_ALLOWLIST_AND_NEW_COMMITTED_HEAD_PLAN"
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
                "authorization_receipt_consumed": False,
                "dbn_rows_decoded": 0,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
