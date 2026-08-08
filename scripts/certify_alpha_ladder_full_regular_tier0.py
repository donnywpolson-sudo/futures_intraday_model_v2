"""Run and seal synthetic Tier 0 for the full-regular Alpha mechanism."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from futures_rebuild.alpha_ladder_full_regular_tier0 import (
    TIER0_CERTIFICATE_PATH,
    TIER0_DECISION_PATH,
    build_certificate,
    build_decision,
    validate_live_evidence,
)
from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]


def _collected_nodes() -> tuple[str, ...]:
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-m", "high_risk"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise IntegrityError(
            "Tier 0 high-risk collection failed:\n"
            f"{completed.stdout}\n{completed.stderr}"
        )
    nodes = tuple(sorted({
        line.strip().replace("\\", "/")
        for line in completed.stdout.splitlines()
        if line.strip().startswith("tests/") and "::" in line
    }))
    if len(nodes) < 100:
        raise IntegrityError("Tier 0 high-risk collection was unexpectedly sparse")
    return nodes


def _write_once(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(payload) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main() -> int:
    certificate_path = ROOT / TIER0_CERTIFICATE_PATH
    decision_path = ROOT / TIER0_DECISION_PATH
    if certificate_path.exists() or decision_path.exists():
        print(json.dumps(validate_live_evidence(root=ROOT), sort_keys=True))
        return 0
    nodes = _collected_nodes()
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-m", "high_risk"],
        cwd=ROOT,
        check=False,
    )
    if completed.returncode != 0:
        raise IntegrityError("Tier 0 high-risk suite failed; no evidence was written")
    certificate = build_certificate(root=ROOT, collected_test_nodes=nodes)
    decision = build_decision(root=ROOT, certificate=certificate)
    _write_once(certificate_path, certificate)
    _write_once(decision_path, decision)
    print(json.dumps(validate_live_evidence(root=ROOT), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
