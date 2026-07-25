from __future__ import annotations

from pathlib import Path

from futures_rebuild.live_cockpit.databento_auth import (
    load_databento_api_key_from_file,
    normalize_api_key,
    redact_databento_text,
    resolve_databento_api_key,
)


def test_normalize_api_key_strips_wrapping_noise() -> None:
    assert normalize_api_key(None) == ""
    assert normalize_api_key("  db-test  ") == "db-test"
    assert normalize_api_key('"db-test"') == "db-test"
    assert normalize_api_key("'db-test'") == "db-test"


def test_load_databento_api_key_from_file_supports_env_assignment(tmp_path: Path) -> None:
    path = tmp_path / "api.env"
    path.write_text(
        "# local ignored secret\nDATABENTO_API_KEY='db-file-test'\n",
        encoding="utf-8",
    )

    assert load_databento_api_key_from_file(path) == "db-file-test"


def test_load_databento_api_key_from_file_supports_raw_key(tmp_path: Path) -> None:
    path = tmp_path / "api.env"
    path.write_text("  db-raw-test  \n", encoding="utf-8")

    assert load_databento_api_key_from_file(path) == "db-raw-test"


def test_resolve_databento_api_key_supports_explicit_test_injection(tmp_path: Path) -> None:
    key_file = tmp_path / "api.env"
    key_file.write_text("DATABENTO_API_KEY=db-file-test\n", encoding="utf-8")

    assert (
        resolve_databento_api_key(
            env={"DATABENTO_API_KEY": "db-env-test"},
            key_files=[key_file],
        )
        == "db-env-test"
    )


def test_resolve_databento_api_key_reads_api_env_and_ignores_ambient_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "db-stale-env-test")
    api_key_file = tmp_path / "api.env"
    api_key_file.write_text("DATABENTO_API_KEY=db-api-test\n", encoding="utf-8")

    assert (
        resolve_databento_api_key(
            key_files=[api_key_file],
        )
        == "db-api-test"
    )


def test_resolve_databento_api_key_missing_api_env_ignores_ambient_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "db-stale-env-test")

    assert resolve_databento_api_key(key_files=[tmp_path / "api.env"]) == ""


def test_injected_empty_env_does_not_read_real_or_fake_files(tmp_path: Path) -> None:
    key_file = tmp_path / "api.env"
    key_file.write_text("DATABENTO_API_KEY=db-file-test\n", encoding="utf-8")

    assert resolve_databento_api_key(env={}, key_files=[key_file]) == ""


def test_redact_databento_text_removes_common_secret_forms() -> None:
    message = (
        "DATABENTO_API_KEY=db-secretvalue123 Authorization: Bearer token-secret "
        "url=https://example.test/?api_key=db-anothersecret456"
    )

    redacted = redact_databento_text(message)

    assert "secretvalue" not in redacted
    assert "token-secret" not in redacted
    assert "anothersecret" not in redacted
    assert redacted.count("<redacted>") == 3
