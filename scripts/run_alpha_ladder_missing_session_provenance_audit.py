"""Describe the prepared audit; execution requires separate row authority."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.alpha_ladder_missing_session_provenance import (
    load_plan,
    required_scope,
)


ROOT = Path(__file__).resolve().parents[1]


if __name__ == "__main__":
    plan = load_plan(root=ROOT)
    print(
        json.dumps(
            {
                "status": "BLOCKED_SEPARATE_REAL_ROW_AUTHORITY_REQUIRED",
                "plan_id": plan["plan_id"],
                "scope": required_scope(root=ROOT, plan=plan),
            },
            sort_keys=True,
        )
    )
