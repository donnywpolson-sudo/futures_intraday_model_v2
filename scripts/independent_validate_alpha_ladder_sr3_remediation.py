from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.alpha_ladder_sr3_remediation import (
    DIAGNOSTIC_PLAN_PATH, HISTORY_ROOT, POLICY_PATH, SYNTHETIC_CERTIFICATE_PATH,
    build_consumed_closure, build_diagnostic_plan, build_policy,
    build_synthetic_certificate,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    closure = build_consumed_closure(ROOT)
    stored_closure = json.loads((ROOT / HISTORY_ROOT / str(closure["closure_id"]) / "closure.json").read_text())
    assert stored_closure == closure
    policy = build_policy(ROOT, closure)
    assert json.loads((ROOT / POLICY_PATH).read_text()) == policy
    certificate = build_synthetic_certificate(ROOT, policy)
    assert json.loads((ROOT / SYNTHETIC_CERTIFICATE_PATH).read_text()) == certificate
    plan = build_diagnostic_plan(ROOT, closure, policy, certificate)
    assert json.loads((ROOT / DIAGNOSTIC_PLAN_PATH).read_text()) == plan
    assert plan["authority"]["market_row_read"] is False
    assert plan["outputs"]["raw_rows_forbidden"] is True
    print(json.dumps({
        "status": "PASS_PREPARATION_ONLY",
        "closure_id": closure["closure_id"],
        "policy_id": policy["policy_id"],
        "certificate_id": certificate["certificate_id"],
        "diagnostic_plan_id": plan["plan_id"],
        "real_rows_read": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
