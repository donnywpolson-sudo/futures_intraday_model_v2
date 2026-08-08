"""Prepare sealed-evidence audit, rejection, and replacement protocol only."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.reported_bar_fixed_horizon_protocol import (
    PROTOCOL_PATH,
    build_protocol,
    build_rejection_record,
    build_topology_audit,
    rejection_path,
    topology_path,
)


ROOT = Path(__file__).resolve().parents[1]


def _create(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def main() -> int:
    audit = build_topology_audit(root=ROOT)
    rejection = build_rejection_record(root=ROOT, audit=audit)
    paths = (ROOT / topology_path(audit), ROOT / rejection_path(rejection), ROOT / PROTOCOL_PATH)
    if any(path.exists() for path in paths):
        raise FileExistsError("reported-bar protocol preparation destination exists")
    _create(paths[0], audit)
    _create(paths[1], rejection)
    protocol = build_protocol(root=ROOT, audit=audit, rejection=rejection)
    _create(paths[2], protocol)
    print(json.dumps({
        "topology_id": audit["topology_id"],
        "topology_sha256": sha256_file(paths[0]),
        "rejection_id": rejection["rejection_id"],
        "rejection_sha256": sha256_file(paths[1]),
        "protocol_id": protocol["protocol_id"],
        "protocol_sha256": sha256_file(paths[2]),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
