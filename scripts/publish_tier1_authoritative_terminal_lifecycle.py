"""Publish only after the pointer-neutral exact suite passes."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_authoritative_terminal_lifecycle import (
    INVALID_TEST_FILES,
    SUPERSEDED_NODE_IDS,
    _tier1_test_paths,
    persist_terminal_authoritative_lifecycle,
    prepare_terminal_authoritative_lifecycle,
)


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts/run_windows_host_root_pytest.ps1"
JUNIT_PATH = ".pytest_tmp/terminal-authoritative-suite.xml"


def certification_pytest_arguments(*, root: Path) -> tuple[str, ...]:
    selected = tuple(
        path.relative_to(root).as_posix()
        for path in _tier1_test_paths(root)
        if path.relative_to(root).as_posix() not in INVALID_TEST_FILES
    )
    return (
        "-q",
        "-m",
        "high_risk",
        *selected,
        *(f"--deselect={node_id}" for node_id in SUPERSEDED_NODE_IDS),
        f"--junitxml={JUNIT_PATH}",
    )


def run_exact_certification_suite(*, root: Path) -> None:
    completed = subprocess.run(
        (
            "powershell",
            "-NoProfile",
            "-File",
            str(RUNNER),
            *certification_pytest_arguments(root=root),
        ),
        cwd=root,
        check=False,
    )
    if completed.returncode != 0:
        raise IntegrityError(
            f"terminal authoritative synthetic suite failed: {completed.returncode}"
        )


def main() -> None:
    arguments_before = certification_pytest_arguments(root=ROOT)
    run_exact_certification_suite(root=ROOT)
    prepared = prepare_terminal_authoritative_lifecycle(root=ROOT)

    def post_activation_check() -> None:
        if certification_pytest_arguments(root=ROOT) != arguments_before:
            raise IntegrityError("terminal certification selection changed during activation")
        run_exact_certification_suite(root=ROOT)

    result = persist_terminal_authoritative_lifecycle(
        root=ROOT,
        prepared=prepared,
        post_activation_check=post_activation_check,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
