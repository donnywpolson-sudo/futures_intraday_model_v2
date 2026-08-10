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
from futures_rebuild.micro_alpha_phase1b2_execution import (  # noqa: E402
    AUDIT_PATH,
    IMPLEMENTATION_PATHS,
    PLAN_PATH,
    build_execution_plan,
    build_plan_audit,
    write_execution_plan_create_only,
    write_plan_audit_create_only,
)


def _head() -> str:
    return subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _require_committed_implementation() -> None:
    for path in IMPLEMENTATION_PATHS:
        tracked = subprocess.run(
            [
                "git", "-C", str(ROOT), "ls-files", "--error-unmatch", "--",
                path.as_posix(),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if tracked.returncode != 0:
            raise SystemExit(
                "every implementation path must exist in the committed HEAD before plan freeze"
            )
    result = subprocess.run(
        [
            "git",
            "-C",
            str(ROOT),
            "diff",
            "--quiet",
            "HEAD",
            "--",
            *(path.as_posix() for path in IMPLEMENTATION_PATHS),
        ]
    )
    if result.returncode != 0:
        raise SystemExit("implementation paths must be committed before freezing the execution plan")


def _assert_existing(path: Path, value: dict[str, object], description: str) -> None:
    existing = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if existing != value:
        raise SystemExit(f"{description} reconstruction differs")


def _summary(value: dict[str, object], path: Path | None = None) -> dict[str, object]:
    result = {
        "artifact_id": value.get("plan_id", value.get("audit_id")),
        "state": value.get("state"),
        "implementation_head": value.get("implementation_head"),
        "source_count": value.get("source_count", value.get("exact_source_count")),
        "source_bytes": value.get("source_bytes", value.get("exact_source_bytes")),
        "coverage_cell_count": value.get(
            "coverage_cell_count", value.get("exact_coverage_cell_count")
        ),
        "historical_rows_read": value.get("historical_rows_read", 0),
    }
    if path is not None and (ROOT / path).exists():
        result["sha256"] = sha256_file(ROOT / path)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=("preview-plan", "write-plan", "check-plan", "write-audit", "check-audit"),
    )
    args = parser.parse_args()
    head = _head()
    if args.command == "preview-plan":
        value = build_execution_plan(root=ROOT, implementation_head=head)
        path = None
    elif args.command == "write-plan":
        _require_committed_implementation()
        value = write_execution_plan_create_only(root=ROOT, implementation_head=head)
        path = PLAN_PATH
    elif args.command == "check-plan":
        value = build_execution_plan(root=ROOT, implementation_head=head)
        _assert_existing(PLAN_PATH, value, "micro execution plan")
        path = PLAN_PATH
    elif args.command == "write-audit":
        _require_committed_implementation()
        value = write_plan_audit_create_only(root=ROOT)
        path = AUDIT_PATH
    else:
        value = build_plan_audit(root=ROOT)
        _assert_existing(AUDIT_PATH, value, "micro execution audit")
        path = AUDIT_PATH
    print(json.dumps(_summary(value, path), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
