from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for candidate in (ROOT, ROOT / "src"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from futures_rebuild.micro_alpha_custody_repair_v2 import (  # noqa: E402
    AUDIT_PATH,
    PLAN_PATH,
    V1_SUPERSESSION_PATH,
    build_plan_audit,
    build_repair_plan,
    build_v1_supersession_report,
    write_plan_audit_create_only,
    write_repair_plan_create_only,
    write_v1_supersession_report_create_only,
)


def _assert_existing(path: Path, rebuilt: dict[str, object], description: str) -> None:
    existing = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if rebuilt != existing:
        raise SystemExit(f"{description} reconstruction differs")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=(
            "write-v1-supersession",
            "check-v1-supersession",
            "write-plan",
            "check-plan",
            "write-audit",
            "check-audit",
        ),
    )
    parser.add_argument("--implementation-head")
    args = parser.parse_args()
    if args.command == "write-v1-supersession":
        result = write_v1_supersession_report_create_only(root=ROOT)
    elif args.command == "check-v1-supersession":
        result = build_v1_supersession_report(root=ROOT)
        _assert_existing(V1_SUPERSESSION_PATH, result, "v1 supersession report")
    elif args.command in {"write-plan", "check-plan"}:
        if not args.implementation_head:
            raise SystemExit("--implementation-head is required")
        if args.command == "write-plan":
            result = write_repair_plan_create_only(
                root=ROOT, implementation_head=args.implementation_head
            )
        else:
            result = build_repair_plan(
                root=ROOT, implementation_head=args.implementation_head
            )
            _assert_existing(PLAN_PATH, result, "v2 repair plan")
    elif args.command == "write-audit":
        result = write_plan_audit_create_only(root=ROOT)
    else:
        result = build_plan_audit(root=ROOT)
        _assert_existing(AUDIT_PATH, result, "v2 repair audit")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
