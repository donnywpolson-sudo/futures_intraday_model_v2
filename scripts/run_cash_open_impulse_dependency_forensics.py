"""Run the singly authorized cash-open exact local dependency audit."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.boundary import OperationClassification, OperationReceipt, RepoBoundary
from futures_rebuild.canonical import sha256_file
from futures_rebuild.cash_open_impulse_dependency_forensics import (
    OPERATION,
    PLAN_PATH,
    execute_once,
    load_plan,
    required_scope,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    boundary = RepoBoundary(ROOT)
    plan = load_plan(ROOT)
    scope = required_scope(ROOT, plan)
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={
            key: value for key, value in scope.items()
            if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
        },
        approval_command=OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=sha256_file(ROOT / PLAN_PATH),
        approval_line=(
            f"APPROVE {OPERATION} PLAN {plan['plan_id']} "
            f"SHA256 {sha256_file(ROOT / PLAN_PATH)}"
        ),
    )
    report = execute_once(root=ROOT, boundary=boundary, receipt=receipt)
    print(json.dumps({
        "status": "COMPLETED_UNPUBLISHED_EXACT_DEPENDENCY_FORENSICS",
        "report_id": report["report_id"],
        "active_failure_checkpoint_count": report["active_failure_checkpoint_count"],
        "active_failure_reason_counts": report["active_failure_reason_counts"],
        "alternative_resolution_instances": report["alternative_resolution_instances"],
        "output_contains_price_values": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "holdout_2025_access": False,
        "provider_network_credentials": False,
        "publication": False,
        "trading": False,
    }, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
