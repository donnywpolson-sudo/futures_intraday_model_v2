from __future__ import annotations

import sys
from pathlib import Path

import pytest

import futures_rebuild.active_data_full_certification as full_certification
import futures_rebuild.runtime_environment as runtime_environment
from futures_rebuild.errors import ContractError
from futures_rebuild.runtime_environment import (
    locked_environment_mismatches,
    require_locked_repository_environment,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_repository_venv_matches_every_locked_package() -> None:
    receipt_id = require_locked_repository_environment(REPOSITORY_ROOT)
    assert len(receipt_id) == 64


def test_environment_audit_reports_all_package_mismatches() -> None:
    def mismatched_version(package: str) -> str:
        if package == "databento":
            return "99.0.0"
        if package == "pypdf":
            return "<missing>"
        import importlib.metadata

        return importlib.metadata.version(package)

    mismatches = locked_environment_mismatches(
        REPOSITORY_ROOT,
        version_lookup=mismatched_version,
    )
    assert mismatches == (
        "package databento expected=0.78.0 actual=99.0.0",
        "package pypdf expected=6.14.2 actual=<missing>",
    )


def test_environment_audit_rejects_global_interpreter() -> None:
    base_executable = Path(getattr(sys, "_base_executable", sys.executable))
    mismatches = locked_environment_mismatches(
        REPOSITORY_ROOT,
        executable=base_executable,
    )
    assert mismatches and mismatches[0].startswith("interpreter expected=")


def test_clean_source_without_embedded_venv_uses_active_locked_interpreter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime_environment,
        "PINNED_PYTHON",
        Path(".venv-clean-source-absent/Scripts/python.exe"),
    )

    assert locked_environment_mismatches(REPOSITORY_ROOT) == ()
    base_executable = Path(getattr(sys, "_base_executable", sys.executable))
    if base_executable.resolve() != Path(sys.executable).resolve():
        mismatches = locked_environment_mismatches(
            REPOSITORY_ROOT,
            executable=base_executable,
        )
        assert mismatches and mismatches[0].startswith("interpreter expected=")


def test_clean_source_without_embedded_venv_rejects_global_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        runtime_environment,
        "PINNED_PYTHON",
        Path(".venv-clean-source-absent/Scripts/python.exe"),
    )
    monkeypatch.setattr(runtime_environment.sys, "prefix", sys.base_prefix)

    mismatches = locked_environment_mismatches(REPOSITORY_ROOT)

    assert mismatches[0] == (
        "interpreter clean-source execution requires an active virtual environment"
    )


def test_full_certification_preflights_before_reading_plan_or_writing_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_environment(_: Path) -> str:
        raise ContractError("test environment mismatch")

    monkeypatch.setattr(
        full_certification,
        "require_locked_repository_environment",
        reject_environment,
    )
    with pytest.raises(ContractError, match="test environment mismatch"):
        full_certification.main(
            [
                "--repository-root",
                str(tmp_path),
                "--plan",
                "missing-plan.json",
                "--approval",
                "missing-approval.json",
            ]
        )
    assert tuple(tmp_path.iterdir()) == ()
