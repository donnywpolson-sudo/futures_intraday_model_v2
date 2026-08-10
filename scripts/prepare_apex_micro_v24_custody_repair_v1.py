from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for candidate in (ROOT, ROOT / "src"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from futures_rebuild.micro_alpha_custody_repair_v1 import (  # noqa: E402
    FAILURE_REPORT_PATH,
    PLAN_PATH,
    build_failure_report,
    build_repair_plan,
    write_failure_report_create_only,
    write_repair_plan_create_only,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=("write-failure-report", "check-failure-report", "write-plan", "check-plan"),
    )
    parser.add_argument("--committed-head")
    args = parser.parse_args()
    if args.command == "write-failure-report":
        result = write_failure_report_create_only(root=ROOT)
    elif args.command == "check-failure-report":
        result = build_failure_report(root=ROOT)
        existing = json.loads((ROOT / FAILURE_REPORT_PATH).read_text(encoding="utf-8"))
        if result != existing:
            raise SystemExit("failure report reconstruction differs")
    elif args.command == "write-plan":
        if not args.committed_head:
            raise SystemExit("--committed-head is required")
        result = write_repair_plan_create_only(
            root=ROOT, committed_head=args.committed_head
        )
    else:
        if not args.committed_head:
            raise SystemExit("--committed-head is required")
        result = build_repair_plan(root=ROOT, committed_head=args.committed_head)
        existing = json.loads((ROOT / PLAN_PATH).read_text(encoding="utf-8"))
        if result != existing:
            raise SystemExit("repair plan reconstruction differs")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
