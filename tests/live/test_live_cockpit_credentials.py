from __future__ import annotations

import json
from pathlib import Path

import pytest

import futures_rebuild.live_cockpit.credentials as credentials
from futures_rebuild.live_cockpit.feed import ApiKeyResolution
from futures_rebuild.live_cockpit.credentials import (
    CREDENTIAL_LOCATOR_FILENAME,
    CREDENTIAL_LOCATOR_SCHEMA,
    CredentialLocatorError,
    credential_status,
    default_repository_package_api_env_path,
    resolve_cockpit_api_key_source,
)


FILE_KEY = "db-" + "f" * 29
STALE_ENV_KEY = "db-" + "s" * 29


def _write_key(path: Path, key: str = FILE_KEY) -> None:
    path.write_text(f'DATABENTO_API_KEY="{key}"\n', encoding="utf-8")


def _write_locator(path: Path, credential_path: Path, **extra: object) -> None:
    payload: dict[str, object] = {
        "schema": CREDENTIAL_LOCATOR_SCHEMA,
        "api_env_path": str(credential_path),
        **extra,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_locator_precedes_stale_environment_without_copying_key(tmp_path: Path) -> None:
    credential_path = tmp_path / "api.env"
    locator_path = tmp_path / CREDENTIAL_LOCATOR_FILENAME
    _write_key(credential_path)
    _write_locator(locator_path, credential_path)

    resolved = resolve_cockpit_api_key_source(
        {"DATABENTO_API_KEY": STALE_ENV_KEY},
        locator_path=locator_path,
    )

    assert resolved is not None
    assert resolved.key == FILE_KEY
    assert resolved.source == "installed credential locator"
    assert FILE_KEY not in locator_path.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "payload, expected",
    [
        (
            {
                "schema": CREDENTIAL_LOCATOR_SCHEMA,
                "api_env_path": "relative/api.env",
            },
            "must be absolute",
        ),
        (
            {"schema": "wrong", "api_env_path": "C:/missing/api.env"},
            "schema is unsupported",
        ),
        (
            {
                "schema": CREDENTIAL_LOCATOR_SCHEMA,
                "api_env_path": "C:/missing/api.env",
                "unexpected": True,
            },
            "exactly schema and api_env_path",
        ),
    ],
)
def test_locator_schema_is_strict_and_errors_are_secret_safe(
    tmp_path: Path, payload: dict[str, object], expected: str
) -> None:
    locator_path = tmp_path / CREDENTIAL_LOCATOR_FILENAME
    locator_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(CredentialLocatorError, match=expected) as captured:
        resolve_cockpit_api_key_source(
            {"DATABENTO_API_KEY": STALE_ENV_KEY},
            locator_path=locator_path,
        )

    assert STALE_ENV_KEY not in str(captured.value)


def test_missing_or_empty_locator_target_fails_without_environment_fallback(
    tmp_path: Path,
) -> None:
    credential_path = tmp_path / "api.env"
    locator_path = tmp_path / CREDENTIAL_LOCATOR_FILENAME
    _write_locator(locator_path, credential_path)
    with pytest.raises(CredentialLocatorError, match="target is unavailable"):
        resolve_cockpit_api_key_source(
            {"DATABENTO_API_KEY": STALE_ENV_KEY},
            locator_path=locator_path,
        )

    credential_path.write_text("# no key\n", encoding="utf-8")
    with pytest.raises(CredentialLocatorError, match="does not contain"):
        resolve_cockpit_api_key_source(
            {"DATABENTO_API_KEY": STALE_ENV_KEY},
            locator_path=locator_path,
        )


def test_absent_locator_preserves_existing_source_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[object] = []

    def resolver(env):
        calls.append(env)
        return ApiKeyResolution(key=STALE_ENV_KEY, source="test environment")

    monkeypatch.setattr(credentials, "resolve_api_key_source", resolver)
    resolved = resolve_cockpit_api_key_source(
        {"DATABENTO_API_KEY": STALE_ENV_KEY},
        locator_path=tmp_path / "missing.json",
    )

    assert resolved is not None
    assert resolved.key == STALE_ENV_KEY
    assert calls == [{"DATABENTO_API_KEY": STALE_ENV_KEY}]


def test_repository_package_path_is_available_for_existence_only_self_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(credentials, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.setattr(credentials.sys, "frozen", False, raising=False)

    assert default_repository_package_api_env_path() == tmp_path / "api.env"

    monkeypatch.setattr(credentials.sys, "frozen", True, raising=False)
    assert default_repository_package_api_env_path() is None


def test_frozen_repository_package_resolves_exact_parent_api_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository_root = tmp_path / "futures_intraday_model_v2"
    executable_dir = repository_root / "FuturesLiveCockpit"
    executable_dir.mkdir(parents=True)
    (repository_root / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (repository_root / "src" / "futures_rebuild" / "live_cockpit").mkdir(parents=True)
    executable = executable_dir / "FuturesLiveCockpit.exe"
    credential_path = repository_root / "api.env"
    _write_key(credential_path)
    monkeypatch.setattr(credentials.sys, "frozen", True, raising=False)
    monkeypatch.setattr(credentials.sys, "executable", str(executable))

    resolved = resolve_cockpit_api_key_source(None)

    assert default_repository_package_api_env_path() == credential_path
    assert resolved is not None
    assert resolved.key == FILE_KEY
    assert resolved.source == "file api.env"


def test_arbitrary_frozen_package_does_not_read_parent_api_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable_dir = tmp_path / "copied-package"
    executable_dir.mkdir()
    executable = executable_dir / "FuturesLiveCockpit.exe"
    _write_key(tmp_path / "api.env")
    monkeypatch.setattr(credentials.sys, "frozen", True, raising=False)
    monkeypatch.setattr(credentials.sys, "executable", str(executable))

    assert default_repository_package_api_env_path() is None
    assert resolve_cockpit_api_key_source(None) is None


def test_frozen_default_locator_and_status_use_existing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "FuturesLiveCockpit.exe"
    credential_path = tmp_path / "repo" / "api.env"
    credential_path.parent.mkdir()
    _write_key(credential_path)
    _write_locator(tmp_path / CREDENTIAL_LOCATOR_FILENAME, credential_path)
    monkeypatch.setattr(credentials.sys, "frozen", True, raising=False)
    monkeypatch.setattr(credentials.sys, "executable", str(executable))

    status = credential_status({"DATABENTO_API_KEY": STALE_ENV_KEY})

    assert status.configured is True
    assert status.source == "installed credential locator"
    assert status.locator_present is True
    assert status.locator_valid is True
    assert status.error is None


def test_invalid_locator_status_is_actionable_without_key_material(tmp_path: Path) -> None:
    locator_path = tmp_path / CREDENTIAL_LOCATOR_FILENAME
    locator_path.write_text("not-json", encoding="utf-8")

    status = credential_status(
        {"DATABENTO_API_KEY": STALE_ENV_KEY},
        locator_path=locator_path,
    )

    assert status.configured is False
    assert status.locator_present is True
    assert status.locator_valid is False
    assert "valid UTF-8 JSON" in str(status.error)
    assert STALE_ENV_KEY not in str(status.error)
