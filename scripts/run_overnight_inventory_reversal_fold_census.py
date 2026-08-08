"""Run the approved audit-only fold census; never evaluate strategy economics."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import sha256_file
from futures_rebuild.overnight_inventory_reversal_preexecution_census import (
    OPERATION,
    PLAN_PATH,
    execute_authorized_census_once,
    load_census_plan,
    required_scope,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    boundary = RepoBoundary(ROOT)
    plan = load_census_plan(root=ROOT)
    plan_sha256 = sha256_file(ROOT / PLAN_PATH)
    required = required_scope(root=ROOT, plan=plan)
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={
            key: value for key, value in required.items()
            if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
        },
        approval_command=OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha256,
        approval_line=(
            f"APPROVE {OPERATION} PLAN {plan['plan_id']} SHA256 {plan_sha256}"
        ),
    )
    report = execute_authorized_census_once(
        root=ROOT, boundary=boundary, receipt=receipt,
    )
    certificate = report["fold_readiness_certificate"]
    print(json.dumps({
        "status": "COMPLETED_UNPUBLISHED_READINESS_CENSUS",
        "trial_id": report["trial_id"],
        "report_id": report["report_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "overall_decision": certificate["overall_decision"],
        "runtime_failure_audit": report["runtime_failure_audit"],
        "economics_evaluation": False,
        "holdout_2025_touched": False,
        "provider_or_network_access": False,
        "publication": False,
        "trading": False,
    }, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
