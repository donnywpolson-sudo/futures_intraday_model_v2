"""Create only the unpublished, non-registerable Tier 1 protocol preparation."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.tier1_trade_triggered_trial_design import DECLARATION_PATH, build_declaration


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    declaration = build_declaration(root=ROOT)
    path = ROOT / DECLARATION_PATH
    with path.open("xb") as stream:
        stream.write(canonical_bytes(declaration) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({
        "path": DECLARATION_PATH.as_posix(),
        "protocol_id": declaration["protocol_id"],
        "sha256": sha256_file(path),
        "state": declaration["state"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
