"""Describe the combined Alpha readiness scope; never execute real history."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.alpha_ladder_combined_readiness import load_plan, required_scope


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    plan = load_plan(root=ROOT)
    print(json.dumps({
        "status": "BLOCKED_SEPARATE_WINDOWS_HOST_APPROVAL_REQUIRED",
        "plan_id": plan["plan_id"],
        "scope": required_scope(root=ROOT, plan=plan),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
