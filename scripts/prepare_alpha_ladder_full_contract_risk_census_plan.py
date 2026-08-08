"""Prepare the immutable 41-market full-contract risk-census plan."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.alpha_ladder_full_contract_risk_census import PLAN_PATH, build_plan
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    plan = build_plan(root=ROOT)
    destination = ROOT / PLAN_PATH
    payload = canonical_bytes(plan) + b"\n"
    if destination.exists():
        if destination.read_bytes() != payload:
            raise IntegrityError("prepared full-contract risk-census plan already differs")
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb") as stream:
            stream.write(payload)
            stream.flush()
    print(json.dumps({
        "plan_id": plan["plan_id"],
        "plan_path": PLAN_PATH.as_posix(),
        "plan_sha256": sha256_file(destination),
        "status": "PREPARED_NOT_EXECUTED",
    }, sort_keys=True))


if __name__ == "__main__":
    main()
