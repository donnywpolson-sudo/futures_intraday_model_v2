"""Create only the V3 rejection and unregistered counted Alpha successor."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.alpha_ladder_source_compatible_successor import (
    PREDECESSOR_PATH,
    build_rejection,
    build_successor,
    rejection_path,
    successor_path,
    validate_successor,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file


ROOT = Path(__file__).resolve().parents[1]


def _write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush(); os.fsync(stream.fileno())


def main() -> int:
    rejection = build_rejection(root=ROOT)
    rejection_file = ROOT / rejection_path(rejection)
    _write(rejection_file, rejection)
    mechanism = build_successor(root=ROOT, rejection=rejection)
    predecessor = json.loads((ROOT / PREDECESSOR_PATH).read_text(encoding="utf-8"))
    validate_successor(mechanism, predecessor=predecessor, rejection=rejection)
    mechanism_file = ROOT / successor_path(mechanism)
    _write(mechanism_file, mechanism)
    print(json.dumps({
        "rejection_id": rejection["rejection_id"],
        "rejection_path": rejection_path(rejection).as_posix(),
        "rejection_sha256": sha256_file(rejection_file),
        "mechanism_id": mechanism["mechanism_id"],
        "mechanism_path": successor_path(mechanism).as_posix(),
        "mechanism_sha256": sha256_file(mechanism_file),
        "state": mechanism["state"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
