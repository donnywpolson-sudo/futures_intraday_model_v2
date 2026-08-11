"""Prepare or verify the generic micro-futures catalog cutover plan."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for candidate in (ROOT, ROOT / "src"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from futures_rebuild.micro_futures_catalog_migration import (  # noqa: E402
    PLAN_PATH,
    build_plan,
    check_plan,
    write_plan_create_only,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=("preview-plan", "write-plan", "check-plan"),
    )
    command = parser.parse_args().command
    if command == "preview-plan":
        plan = build_plan(root=ROOT)
    elif command == "write-plan":
        plan = write_plan_create_only(root=ROOT)
    else:
        plan = check_plan(root=ROOT)
    print(
        json.dumps(
            {
                "active_data_mutated": False,
                "plan_id": plan["plan_id"],
                "plan_path": PLAN_PATH.as_posix(),
                "proposed_catalog_path": plan["proposed_successor"]["catalog_path"],
                "proposed_pointer_path": plan["proposed_successor"]["pointer_path"],
                "state": plan["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
