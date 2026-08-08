"""Prepare the transition-stable source-compatibility correction."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.cash_open_source_compatibility_correction import (
    CORRECTED_SPEC_PATH,
    build_correction,
)


ROOT = Path(__file__).resolve().parents[1]
INVALID_ROOT = Path(
    "state/unpublished_evidence/cash_open_source_compatibility_invalid_preparation"
)


def _write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def main() -> int:
    invalidity, corrected = build_correction(root=ROOT)
    invalid_path = INVALID_ROOT / str(invalidity["record_id"]) / "invalidity.json"
    _write(ROOT / invalid_path, invalidity)
    _write(ROOT / CORRECTED_SPEC_PATH, corrected)
    print(json.dumps({
        "invalidity_path": invalid_path.as_posix(),
        "invalidity_sha256": sha256_file(ROOT / invalid_path),
        "corrected_spec_id": corrected["spec_id"],
        "corrected_spec_path": CORRECTED_SPEC_PATH.as_posix(),
        "corrected_spec_sha256": sha256_file(ROOT / CORRECTED_SPEC_PATH),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
