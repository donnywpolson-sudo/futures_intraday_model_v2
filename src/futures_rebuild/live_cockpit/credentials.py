"""Credential resolution for source and installed Futures Live Cockpit runtimes."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .feed import (
    API_KEY_ENV,
    ApiKeyResolution,
    ROOT as REPOSITORY_ROOT,
    frozen_repository_api_key_file,
    load_databento_api_key_from_file,
    resolve_api_key_source,
)


CREDENTIAL_LOCATOR_FILENAME = "credential-source.json"
CREDENTIAL_LOCATOR_SCHEMA = "futures_live_cockpit_credential_source_v1"
_LOCATOR_KEYS = {"schema", "api_env_path"}
_MAX_LOCATOR_BYTES = 4_096


class CredentialLocatorError(RuntimeError):
    """Raised for an installed locator that cannot safely resolve a key."""


@dataclass(frozen=True)
class CredentialStatus:
    configured: bool
    source: str | None
    locator_present: bool
    locator_valid: bool | None
    error: str | None


def default_credential_locator_path() -> Path | None:
    if not getattr(sys, "frozen", False):
        return None
    return Path(sys.executable).resolve().parent / CREDENTIAL_LOCATOR_FILENAME


def default_repository_package_api_env_path() -> Path | None:
    """Return the source-package credential path for existence-only self-checks."""

    if getattr(sys, "frozen", False):
        return frozen_repository_api_key_file(Path(sys.executable).resolve().parent)
    return REPOSITORY_ROOT / "api.env"


def _credential_path_from_locator(locator_path: Path) -> Path:
    try:
        if locator_path.stat().st_size > _MAX_LOCATOR_BYTES:
            raise CredentialLocatorError("credential locator exceeds 4096 bytes")
        payload = json.loads(locator_path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        raise
    except CredentialLocatorError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CredentialLocatorError("credential locator is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict) or set(payload) != _LOCATOR_KEYS:
        raise CredentialLocatorError(
            "credential locator must contain exactly schema and api_env_path"
        )
    if payload.get("schema") != CREDENTIAL_LOCATOR_SCHEMA:
        raise CredentialLocatorError("credential locator schema is unsupported")
    raw_path = payload.get("api_env_path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise CredentialLocatorError("credential locator api_env_path is missing")
    credential_path = Path(raw_path)
    if not credential_path.is_absolute():
        raise CredentialLocatorError("credential locator api_env_path must be absolute")
    if not credential_path.is_file():
        raise CredentialLocatorError("credential locator target is unavailable")
    return credential_path


def resolve_cockpit_api_key_source(
    env: Mapping[str, str] | None = None,
    *,
    locator_path: Path | None = None,
) -> ApiKeyResolution | None:
    """Resolve the installed locator first, then preserve existing resolution."""

    resolved_locator = (
        default_credential_locator_path() if locator_path is None else locator_path
    )
    if resolved_locator is not None and resolved_locator.exists():
        credential_path = _credential_path_from_locator(resolved_locator)
        key = load_databento_api_key_from_file(
            credential_path,
            key_name=API_KEY_ENV,
        )
        if not key:
            raise CredentialLocatorError(
                "credential locator target does not contain DATABENTO_API_KEY"
            )
        return ApiKeyResolution(key=key, source="installed credential locator")
    return resolve_api_key_source(None if env is None else dict(env))


def credential_status(
    env: Mapping[str, str] | None = None,
    *,
    locator_path: Path | None = None,
) -> CredentialStatus:
    resolved_locator = (
        default_credential_locator_path() if locator_path is None else locator_path
    )
    locator_present = bool(
        resolved_locator is not None and resolved_locator.exists()
    )
    try:
        resolution = resolve_cockpit_api_key_source(env, locator_path=resolved_locator)
    except CredentialLocatorError as exc:
        return CredentialStatus(
            configured=False,
            source=None,
            locator_present=locator_present,
            locator_valid=False,
            error=str(exc),
        )
    return CredentialStatus(
        configured=resolution is not None,
        source=resolution.source if resolution is not None else None,
        locator_present=locator_present,
        locator_valid=True if locator_present else None,
        error=None,
    )
