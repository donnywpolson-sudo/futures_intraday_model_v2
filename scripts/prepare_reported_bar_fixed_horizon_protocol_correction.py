"""Preserve the first protocol and create its bounded additive correction."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.reported_bar_fixed_horizon_protocol_correction import (
    CORRECTED_PROTOCOL_PATH,
    build_corrected_protocol,
    build_invalidity,
    invalidity_path,
)


ROOT = Path(__file__).resolve().parents[1]


def _create(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def main() -> int:
    invalidity = build_invalidity(root=ROOT)
    invalidity_target = ROOT / invalidity_path(invalidity)
    corrected_target = ROOT / CORRECTED_PROTOCOL_PATH
    if invalidity_target.exists() or corrected_target.exists():
        raise FileExistsError("reported-bar protocol correction destination exists")
    _create(invalidity_target, invalidity)
    corrected = build_corrected_protocol(root=ROOT, invalidity=invalidity)
    _create(corrected_target, corrected)
    print(json.dumps({
        "invalidity_id": invalidity["invalidity_id"],
        "invalidity_sha256": sha256_file(invalidity_target),
        "protocol_id": corrected["protocol_id"],
        "protocol_sha256": sha256_file(corrected_target),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
