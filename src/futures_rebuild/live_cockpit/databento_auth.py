"""Local Databento API-key resolution helpers."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Mapping, Sequence


API_KEY_NAME = "DATABENTO_API_KEY"
PROJECT_ROOT = Path(__file__).resolve().parents[3]
API_ENV_FILE = PROJECT_ROOT / "api.env"
API_KEY_FILES = (API_ENV_FILE,)

_SECRET_PATTERNS = (
    re.compile(r"(?i)(DATABENTO_API_KEY\s*[=:]\s*)[^\s,;]+"),
    re.compile(r"(?i)(authorization\s*:\s*bearer\s+)[^\s,;]+"),
    re.compile(r"(?i)\bdb-[A-Za-z0-9_-]{8,}\b"),
    re.compile(r"(?i)([?&](?:api_?key|token)=)[^&\s]+"),
)


def normalize_api_key(value: str | None) -> str:
    if not value:
        return ""
    key = value.strip()
    if len(key) >= 2 and key[0] == key[-1] and key[0] in {"'", '"'}:
        key = key[1:-1].strip()
    return key


def load_databento_api_key_from_file(path: Path, *, key_name: str = API_KEY_NAME) -> str:
    if not path.exists():
        return ""
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        if "=" not in text:
            return normalize_api_key(text)
        name, value = text.split("=", 1)
        if name.strip() == key_name:
            return normalize_api_key(value)
    return ""


def resolve_databento_api_key(
    *,
    env: Mapping[str, str] | None = None,
    key_name: str = API_KEY_NAME,
    key_files: Sequence[Path] = API_KEY_FILES,
) -> str:
    """Resolve production credentials from api.env; explicit env is test-only."""
    if env is not None:
        return normalize_api_key(env.get(key_name, ""))
    for path in key_files:
        key = load_databento_api_key_from_file(path, key_name=key_name)
        if key:
            return key
    return ""


def redact_databento_text(value: object) -> str:
    """Remove common credential forms before provider text is logged or persisted."""
    text = str(value)
    text = _SECRET_PATTERNS[0].sub(r"\1<redacted>", text)
    text = _SECRET_PATTERNS[1].sub(r"\1<redacted>", text)
    text = _SECRET_PATTERNS[2].sub("<redacted>", text)
    return _SECRET_PATTERNS[3].sub(r"\1<redacted>", text)
