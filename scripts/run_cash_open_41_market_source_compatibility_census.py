"""Describe the gated census; repository CLIs never execute real history."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.cash_open_source_compatibility_census import (
    load_census_plan,
    required_scope,
)


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    plan = load_census_plan(root=ROOT)
    scope = required_scope(root=ROOT, plan=plan)
    print(
        json.dumps(
            {
                "status": "BLOCKED_SEPARATE_PLAIN_LANGUAGE_APPROVAL_REQUIRED",
                "plan_id": plan["plan_id"],
                "scope": scope,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
