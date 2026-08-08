"""Preserve the invalid preparation and create its availability-bounded successor."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.reported_bar_trade_triggered_protocol import (
    PROTOCOL_PATH,
    build_invalid_preparation,
    build_protocol,
    build_rejection,
    build_topology,
    invalid_preparation_path,
)


ROOT = Path(__file__).resolve().parents[1]


def _create(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def main() -> int:
    topology = build_topology(root=ROOT)
    rejection = build_rejection(root=ROOT, topology=topology)
    invalid = build_invalid_preparation(root=ROOT)
    invalid_target = ROOT / invalid_preparation_path(invalid)
    protocol_target = ROOT / PROTOCOL_PATH
    if invalid_target.exists() or protocol_target.exists():
        raise FileExistsError("trade-triggered correction destination exists")
    _create(invalid_target, invalid)
    protocol = build_protocol(
        root=ROOT,
        topology=topology,
        rejection=rejection,
        invalid_preparation=invalid,
    )
    _create(protocol_target, protocol)
    print(json.dumps({
        "invalid_preparation_id": invalid["invalid_preparation_id"],
        "invalid_preparation_sha256": sha256_file(invalid_target),
        "protocol_id": protocol["protocol_id"],
        "protocol_sha256": sha256_file(protocol_target),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
