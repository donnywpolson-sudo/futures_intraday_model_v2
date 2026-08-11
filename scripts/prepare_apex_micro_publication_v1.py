"""Prepare-only CLI for the certified Apex micro publication boundary."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for candidate in (ROOT, ROOT / "src"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from futures_rebuild.canonical import sha256_file  # noqa: E402
from futures_rebuild.micro_alpha_publication import (  # noqa: E402
    AUDIT_PATH,
    IMPLEMENTATION_PATHS,
    PLAN_PATH,
    build_audit,
    build_plan,
    write_audit_create_only,
    write_plan_create_only,
)


def _head() -> str:
    return subprocess.check_output(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
    ).strip()


def _require_committed_implementation() -> None:
    for path in IMPLEMENTATION_PATHS:
        tracked = subprocess.run(
            ["git", "-C", str(ROOT), "ls-files", "--error-unmatch", "--", path.as_posix()],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if tracked.returncode != 0:
            raise SystemExit("every micro publication implementation path must be committed")
    changed = subprocess.run(
        [
            "git",
            "-C",
            str(ROOT),
            "diff",
            "--quiet",
            "HEAD",
            "--",
            *(path.as_posix() for path in IMPLEMENTATION_PATHS),
        ],
        check=False,
    )
    if changed.returncode != 0:
        raise SystemExit("micro publication implementation paths must be committed")


def _check(path: Path, expected: dict[str, object], description: str) -> None:
    actual = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if actual != expected:
        raise SystemExit(f"{description} reconstruction differs")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=("preview-plan", "write-plan", "check-plan", "write-audit", "check-audit"),
    )
    command = parser.parse_args().command
    head = _head()
    if command == "preview-plan":
        value = build_plan(root=ROOT, implementation_head=head)
        path = None
    elif command == "write-plan":
        _require_committed_implementation()
        value = write_plan_create_only(root=ROOT, implementation_head=head)
        path = PLAN_PATH
    elif command == "check-plan":
        value = build_plan(root=ROOT, implementation_head=head)
        _check(PLAN_PATH, value, "micro publication plan")
        path = PLAN_PATH
    elif command == "write-audit":
        _require_committed_implementation()
        value = write_audit_create_only(root=ROOT)
        path = AUDIT_PATH
    else:
        value = build_audit(root=ROOT)
        _check(AUDIT_PATH, value, "micro publication audit")
        path = AUDIT_PATH
    print(
        json.dumps(
            {
                "artifact_id": value.get("audit_id") or value.get("plan_id"),
                "implementation_head": value.get("implementation_head"),
                "payload_bytes": value.get("payload_bytes")
                or value.get("limits", {}).get("maximum_payload_bytes"),
                "payload_count": value.get("payload_count")
                or value.get("limits", {}).get("maximum_payload_files"),
                "sha256": sha256_file(ROOT / path) if path is not None else None,
                "state": value.get("state"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
