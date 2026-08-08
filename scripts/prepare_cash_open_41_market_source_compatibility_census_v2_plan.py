"""Create the failure record and host-successor plan without reading rows."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.cash_open_source_compatibility_census_v2 import (
    PLAN_PATH,
    build_failure_record,
    build_plan_v2,
    failure_record_path,
)


ROOT = Path(__file__).resolve().parents[1]


def _create(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def main() -> int:
    failure = build_failure_record(root=ROOT)
    failure_path = failure_record_path(failure)
    if failure_path.exists() or (ROOT / PLAN_PATH).exists():
        raise FileExistsError("host-successor preparation destination already exists")
    _create(ROOT / failure_path, failure)
    plan = build_plan_v2(root=ROOT, failure=failure)
    _create(ROOT / PLAN_PATH, plan)
    print(json.dumps({
        "failure_id": failure["failure_id"],
        "failure_sha256": sha256_file(ROOT / failure_path),
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(ROOT / PLAN_PATH),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
