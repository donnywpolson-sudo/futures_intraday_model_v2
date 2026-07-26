"""Verify a prepared cockpit and passing smoke result before shortcut cutover."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json

from .approval import LiveSmokeApprovalError, validate_live_smoke_plan
from .smoke import RESULT_SCHEMA


_HASH = re.compile(r"[0-9a-f]{64}")
_RESULT_KEYS = {
    "schema_version",
    "status",
    "plan_id",
    "plan_sha256",
    "approval_receipt_id",
    "completed_at",
    "result_output_relative",
    "summary",
    "result_id",
}


def _load_canonical(path: Path, label: str) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LiveSmokeApprovalError(f"{label} is not readable JSON") from exc
    if type(payload) is not dict or raw != canonical_bytes(payload) + b"\n":
        raise LiveSmokeApprovalError(f"{label} is not canonical JSON")
    return payload


def verify_cutover(
    *, plan_path: Path, result_path: Path, executable_path: Path
) -> dict[str, str]:
    plan = validate_live_smoke_plan(_load_canonical(plan_path, "live-smoke plan"))
    result = _load_canonical(result_path, "live-smoke result")
    executable_hash = sha256_file(executable_path)
    core = {key: value for key, value in result.items() if key != "result_id"}
    summary = result.get("summary")
    runtime = summary.get("runtime") if type(summary) is dict else None
    reasons = summary.get("reasons") if type(summary) is dict else None
    if (
        set(result) != _RESULT_KEYS
        or result.get("schema_version") != RESULT_SCHEMA
        or result.get("status") != "PASS"
        or result.get("plan_id") != plan["plan_id"]
        or result.get("plan_sha256") != sha256_file(plan_path)
        or type(result.get("approval_receipt_id")) is not str
        or _HASH.fullmatch(result["approval_receipt_id"]) is None
        or result.get("result_output_relative")
        != plan["scope"]["result_output_relative"]
        or type(result.get("result_id")) is not str
        or result["result_id"] != sha256_json(core)
        or type(summary) is not dict
        or summary.get("status") != "PASS"
        or reasons != []
        or type(runtime) is not dict
        or runtime.get("frozen") is not True
        or runtime.get("executable_sha256") != executable_hash
        or executable_hash != plan["scope"]["prepared_executable_sha256"]
    ):
        raise LiveSmokeApprovalError(
            "prepared cockpit lacks an exact passing package-bound smoke result"
        )
    return {
        "approval_receipt_id": result["approval_receipt_id"],
        "executable_sha256": executable_hash,
        "plan_id": plan["plan_id"],
        "result_id": result["result_id"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--executable", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        evidence = verify_cutover(
            plan_path=args.plan.resolve(strict=True),
            result_path=args.result.resolve(strict=True),
            executable_path=args.executable.resolve(strict=True),
        )
    except (LiveSmokeApprovalError, OSError) as exc:
        print(f"BLOCKED: {exc}")
        return 2
    print(json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
