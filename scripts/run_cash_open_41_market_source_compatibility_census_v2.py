"""Describe the Windows-host successor census; never execute real history."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.cash_open_source_compatibility_census_v2 import (
    load_plan_v2,
    required_scope_v2,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    plan = load_plan_v2(root=ROOT)
    print(json.dumps({
        "status": "BLOCKED_SEPARATE_WINDOWS_HOST_APPROVAL_REQUIRED",
        "plan_id": plan["plan_id"],
        "scope": required_scope_v2(root=ROOT, plan=plan),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
