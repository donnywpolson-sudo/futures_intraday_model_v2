"""Windows test-root capability contract.

The deepest synthetic repository fixtures need a path directly below the
current drive root to remain below legacy MAX_PATH.  A repository-local
fallback is unsafe because it turns an environment problem into dozens of
unrelated-looking path failures.
"""

from __future__ import annotations

import os
import re
import uuid
from collections.abc import Callable
from pathlib import Path


WINDOWS_HOST_ROOT_TEMP_UNAVAILABLE = "WINDOWS_HOST_ROOT_TEMP_UNAVAILABLE"
_PREFIX = re.compile(r"[a-z0-9-]{1,4}")
_NONCE = re.compile(r"[0-9a-f]{4}")


class WindowsTestRootUnavailable(RuntimeError):
    """The current process cannot create the required short Windows test root."""


def create_windows_test_root(
    prefix: str,
    *,
    anchor: str | Path | None = None,
    pid: int | None = None,
    nonce: str | None = None,
    create_directory: Callable[[Path], None] | None = None,
) -> Path:
    """Create one collision-resistant directory immediately below a drive root.

    The injectable arguments make the capability contract testable without
    writing to a real drive root. Production callers use only ``prefix``.
    """

    if _PREFIX.fullmatch(prefix) is None:
        raise ValueError("Windows test-root prefix must be 1-4 lowercase safe characters")

    raw_anchor = Path.cwd().anchor if anchor is None else str(anchor)
    if not raw_anchor:
        raise WindowsTestRootUnavailable(
            f"{WINDOWS_HOST_ROOT_TEMP_UNAVAILABLE}: "
            "cannot determine the Windows test-drive anchor"
        )
    root = Path(raw_anchor)
    if root.parent != root:
        raise ValueError("Windows test-root anchor must be a filesystem root")

    process_id = os.getpid() if pid is None else pid
    if process_id < 0:
        raise ValueError("Windows test-root process id must be nonnegative")
    unique_nonce = uuid.uuid4().hex[:4] if nonce is None else nonce
    if _NONCE.fullmatch(unique_nonce) is None:
        raise ValueError("Windows test-root nonce must be exactly four lowercase hex characters")

    candidate = root / f"{prefix}{process_id:x}{unique_nonce}"
    creator = create_directory or (lambda path: path.mkdir())
    try:
        creator(candidate)
    except OSError as exc:
        raise WindowsTestRootUnavailable(
            f"{WINDOWS_HOST_ROOT_TEMP_UNAVAILABLE}: cannot create {candidate}. "
            f"Run pytest in an execution environment with create/delete access "
            f"to {root}. Repository-local fallback is intentionally disabled "
            "because deep synthetic paths can exceed legacy Windows MAX_PATH."
        ) from exc
    return candidate
