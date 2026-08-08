"""Validate and describe the prepared 41-market census; never execute it."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.cash_open_impulse_pre_registration_remediation import (
    validate_41_market_plan,
)


PLAN_PATH = Path(
    "configs/cash_open_impulse_41_market_source_compatibility_census_plan.json"
)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    plan = validate_41_market_plan(root, PLAN_PATH)
    print(json.dumps({
        "calendar_coverage_gate": plan["calendar_coverage_gate"],
        "execution_allowed": plan["execution_allowed"],
        "historical_row_read_allowed": plan["historical_row_read_allowed"],
        "plan_id": plan["plan_id"],
        "state": plan["state"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
