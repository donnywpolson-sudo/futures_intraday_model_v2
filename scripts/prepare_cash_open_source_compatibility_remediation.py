"""Prepare closure and source-only compatibility specification without rows."""

from __future__ import annotations

import json
import os
import argparse
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.cash_open_source_compatibility import (
    build_predata_spec,
    build_rejected_protocol_closure,
)


ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = Path("configs/cash_open_41_market_source_compatibility_spec.json")
CLOSURE_ROOT = Path(
    "state/unpublished_evidence/cash_open_impulse_pre_registration_rejection"
)


def _write_once(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--grid-calendar", type=Path, required=True)
    args = parser.parse_args()
    grid_path = args.grid_calendar
    if not grid_path.is_absolute():
        grid_path = ROOT / grid_path
    grid_path = grid_path.resolve(strict=True)
    grid_relative = grid_path.relative_to(ROOT)
    grid_sha = sha256_file(grid_path)
    closure = build_rejected_protocol_closure(root=ROOT)
    spec = build_predata_spec(
        root=ROOT, grid_calendar_path=grid_relative, grid_calendar_sha256=grid_sha
    )
    closure_path = CLOSURE_ROOT / str(closure["record_id"]) / "closure.json"
    _write_once(ROOT / closure_path, closure)
    _write_once(ROOT / SPEC_PATH, spec)
    print(
        json.dumps(
            {
                "closure_path": closure_path.as_posix(),
                "closure_sha256": sha256_file(ROOT / closure_path),
                "grid_calendar_id": spec["prepared_calendar"]["calendar_id"],
                "spec_id": spec["spec_id"],
                "spec_path": SPEC_PATH.as_posix(),
                "spec_sha256": sha256_file(ROOT / SPEC_PATH),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
