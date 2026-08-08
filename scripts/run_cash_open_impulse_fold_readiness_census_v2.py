"""Run only an approved host-permission cash-open readiness successor."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.boundary import OperationClassification, OperationReceipt, RepoBoundary
from futures_rebuild.canonical import sha256_file
from futures_rebuild.cash_open_impulse_census_v2 import (
    OPERATION, PLAN_PATH, execute_once_v2, load_plan_v2, required_scope_v2,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    boundary = RepoBoundary(ROOT)
    plan = load_plan_v2(root=ROOT)
    plan_sha = sha256_file(ROOT / PLAN_PATH)
    scope = required_scope_v2(root=ROOT, plan=plan)
    receipt = OperationReceipt.issue_user_approved(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={key: value for key, value in scope.items()
               if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}},
        approval_command=OPERATION, approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha,
        approval_line=f"APPROVE {OPERATION} PLAN {plan['plan_id']} SHA256 {plan_sha}",
    )
    report = execute_once_v2(root=ROOT, boundary=boundary, receipt=receipt)
    certificate = report["fold_readiness_certificate"]
    print(json.dumps({
        "status": "COMPLETED_UNPUBLISHED_PRE_REGISTRATION_HOST_SUCCESSOR_CENSUS",
        "protocol_id": report["protocol_id"], "report_id": report["report_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "overall_decision": certificate["overall_decision"],
        "registration_allowed": certificate["registration_allowed"],
        "historical_economics_evaluation": False, "model_fit": False,
        "prediction_generation": False, "holdout_2025_touched": False,
        "provider_or_network_access": False, "publication": False, "trading": False,
    }, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
