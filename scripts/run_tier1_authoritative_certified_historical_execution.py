"""Execute the certified authoritative trial after separate real-data approval."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import sha256_file
from futures_rebuild.tier1_authoritative_certified_execution import (
    execute_authorized_certified_authoritative,
    load_certified_registered_context,
)
from futures_rebuild.tier1_authoritative_execution import (
    OPERATION,
    OUTPUT_ROOT,
    PLAN_PATH,
    _required_scope,
    load_authoritative_execution_plan,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    boundary = RepoBoundary(ROOT)
    plan = load_authoritative_execution_plan(root=ROOT)
    plan = {**plan, "plan_sha256": sha256_file(ROOT / PLAN_PATH)}
    trial_id, _, _, _ = load_certified_registered_context(root=ROOT, plan=plan)
    output_root = ROOT / OUTPUT_ROOT
    required = _required_scope(trial_id=trial_id, plan=plan, output_root=output_root)
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={
            key: value
            for key, value in required.items()
            if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
        },
        approval_command=OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=str(plan["plan_sha256"]),
        approval_line=(
            f"APPROVE {OPERATION} PLAN {plan['plan_id']} "
            f"SHA256 {plan['plan_sha256']}"
        ),
    )
    execution = execute_authorized_certified_authoritative(
        root=ROOT, boundary=boundary, receipt=receipt
    )
    stress = execution.result.evaluation["stress"]
    print(
        json.dumps(
            {
                "status": "COMPLETED_WITH_SEALED_UNPUBLISHED_EVIDENCE",
                "trial_id": trial_id,
                "plan_id": plan["plan_id"],
                "authorization_receipt_id": receipt.receipt_id,
                "decision": execution.result.decision,
                "coverage": execution.result.coverage,
                "stress_paths": {
                    strategy: {
                        "admitted_trades": len(path.admitted),
                        "net_pnl_usd": str(path.ending_equity_usd - 100000),
                        "maximum_continuous_drawdown_usd": str(
                            path.maximum_continuous_drawdown_usd
                        ),
                        "complete": path.complete,
                    }
                    for strategy, path in stress.items()
                },
                "unpublished_bundle": execution.unpublished_bundle,
                "publication": False,
                "holdout_or_forward_access": False,
                "provider_access": False,
                "trading": False,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
