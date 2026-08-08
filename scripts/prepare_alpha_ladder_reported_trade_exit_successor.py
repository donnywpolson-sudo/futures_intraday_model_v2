"""Create only the unpublished closure and counted reported-trade successor."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.alpha_ladder_reported_trade_exit_successor import (
    PREDECESSOR_PATH,
    build_closure,
    build_successor,
    closure_path,
    successor_path,
    validate_closure,
    validate_successor,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.cash_open_source_compatibility_census import _read_canonical


ROOT = Path(__file__).resolve().parents[1]


def _write_once(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush(); os.fsync(stream.fileno())


def _write_once_or_verify(path: Path, payload: dict[str, object]) -> str:
    expected = canonical_bytes(payload) + b"\n"
    if path.exists():
        if path.read_bytes() != expected:
            raise FileExistsError(f"existing artifact differs: {path}")
        return "VERIFIED_EXISTING"
    _write_once(path, payload)
    return "CREATED"


def main() -> int:
    closure = build_closure(root=ROOT)
    validate_closure(closure, root=ROOT)
    predecessor = _read_canonical(ROOT / PREDECESSOR_PATH, name="resting-exit predecessor")
    successor = build_successor(root=ROOT, closure=closure)
    validate_successor(successor, predecessor=predecessor, closure=closure, root=ROOT)
    closure_file = ROOT / closure_path(closure)
    successor_file = ROOT / successor_path(successor)
    closure_write = _write_once_or_verify(closure_file, closure)
    if successor_file.exists():
        raise FileExistsError("successor destination already exists")
    _write_once(successor_file, successor)
    print(json.dumps({
        "closure_id": closure["closure_id"],
        "closure_path": closure_file.relative_to(ROOT).as_posix(),
        "closure_sha256": sha256_file(closure_file),
        "closure_write": closure_write,
        "successor_mechanism_id": successor["mechanism_id"],
        "successor_path": successor_file.relative_to(ROOT).as_posix(),
        "successor_sha256": sha256_file(successor_file),
        "state": successor["state"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
