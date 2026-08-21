from __future__ import annotations

import argparse
import json
from pathlib import Path

from futures_rebuild.ohlcv_historical_backfill_v3 import build_completion_plan, write_plan


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare the provider-free 58-root OHLCV completion plan")
    parser.add_argument("--repository-root", default=".")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    root = Path(args.repository_root).resolve(strict=True)
    output = Path(args.output)
    if not output.is_absolute():
        output = root / output
    plan = build_completion_plan(root)
    write_plan(output, plan)
    print(
        json.dumps(
            {
                "missing_root_count": len(plan["intervals"]),
                "output": output.relative_to(root).as_posix(),
                "plan_id": plan["plan_id"],
                "provider_calls": 0,
                "status": plan["authority"]["status"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
