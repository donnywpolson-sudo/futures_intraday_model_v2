"""Describe the prepared V4 census; execution requires separate approval."""

import json
from pathlib import Path

from futures_rebuild.alpha_ladder_limit_readiness_v4 import load_plan, required_scope

ROOT = Path(__file__).resolve().parents[1]
if __name__ == "__main__":
    plan = load_plan(root=ROOT)
    print(json.dumps({"status": "BLOCKED_SEPARATE_WINDOWS_HOST_APPROVAL_REQUIRED",
                      "plan_id": plan["plan_id"],
                      "scope": required_scope(root=ROOT, plan=plan)}, sort_keys=True))
