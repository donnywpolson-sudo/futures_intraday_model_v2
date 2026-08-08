from pathlib import Path
from types import SimpleNamespace

import pytest

import tests.conftest as conftest_module
from tests.windows_temp_root import (
    WINDOWS_HOST_ROOT_TEMP_UNAVAILABLE,
    WindowsTestRootUnavailable,
    create_windows_test_root,
)


def test_windows_test_root_is_short_unique_and_directly_below_anchor(
    tmp_path: Path,
) -> None:
    created: list[Path] = []
    root = Path(tmp_path.anchor)

    candidate = create_windows_test_root(
        "cns-",
        anchor=root,
        pid=0x2A,
        nonce="1a2b",
        create_directory=created.append,
    )

    assert candidate == root / "cns-2a1a2b"
    assert created == [candidate]
    assert candidate.parent == root


def test_windows_test_root_denial_fails_closed_without_deep_fallback(
    tmp_path: Path,
) -> None:
    def deny(_candidate: Path) -> None:
        raise PermissionError("synthetic denial")

    with pytest.raises(WindowsTestRootUnavailable) as caught:
        create_windows_test_root(
            "f",
            anchor=tmp_path.anchor,
            pid=1,
            nonce="abcd",
            create_directory=deny,
        )

    message = str(caught.value)
    assert message.startswith(WINDOWS_HOST_ROOT_TEMP_UNAVAILABLE)
    assert "create/delete access" in message
    assert "Repository-local fallback is intentionally disabled" in message
    assert str(Path.cwd() / ".t") not in message


def test_pytest_configure_reports_root_denial_as_usage_error(monkeypatch) -> None:
    config = SimpleNamespace(option=SimpleNamespace(basetemp=None))

    def deny(_prefix: str) -> Path:
        raise WindowsTestRootUnavailable("root capability denied")

    monkeypatch.setattr(conftest_module.os, "name", "nt")
    monkeypatch.setattr(conftest_module, "create_windows_test_root", deny)

    with pytest.raises(pytest.UsageError, match="root capability denied"):
        conftest_module.pytest_configure(config)


def test_explicit_basetemp_remains_an_intentional_override(monkeypatch) -> None:
    config = SimpleNamespace(option=SimpleNamespace(basetemp="explicit-root"))
    called = False

    def unexpected(_prefix: str) -> Path:
        nonlocal called
        called = True
        raise AssertionError("explicit basetemp must bypass automatic root creation")

    monkeypatch.setattr(conftest_module.os, "name", "nt")
    monkeypatch.setattr(conftest_module, "create_windows_test_root", unexpected)

    conftest_module.pytest_configure(config)

    assert not called
    assert config.option.basetemp == "explicit-root"
