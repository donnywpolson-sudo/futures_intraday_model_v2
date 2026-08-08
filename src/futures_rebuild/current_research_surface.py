"""Fail-closed boundary for superseded real-history execution surfaces.

The active Alpha ladder owns new trial registration and historical execution.
Older foundation, split, prediction, and evaluation helpers remain importable
for synthetic mechanics and historical evidence, but they may not operate on
the live repository.
"""

from __future__ import annotations

from pathlib import Path

from .errors import UnauthorizedOperation


ACTIVE_ALPHA_POINTER = Path("configs/active_alpha_research_ladder.json")


def is_live_alpha_repository(root: Path) -> bool:
    """Return whether *root* is the active project rather than a test fixture."""

    resolved = root.resolve(strict=False)
    return (
        (resolved / ".git").exists()
        and (resolved / "pyproject.toml").is_file()
        and (resolved / ACTIVE_ALPHA_POINTER).is_file()
    )


def reject_retired_project_execution(*, root: Path, surface: str) -> None:
    """Reject a superseded executor before it can inspect protected inputs."""

    if is_live_alpha_repository(root):
        raise UnauthorizedOperation(
            f"{surface} is retired for the live repository; use the active "
            "Alpha readiness boundary or CertifiedResearchGateway"
        )


def live_alpha_repository_for_path(path: Path) -> Path | None:
    """Find the active project containing *path*, including absent outputs."""

    resolved = path.resolve(strict=False)
    start = resolved if resolved.is_dir() else resolved.parent
    for candidate in (start, *start.parents):
        if is_live_alpha_repository(candidate):
            return candidate
    return None


def reject_retired_path_execution(*, path: Path, surface: str) -> None:
    """Reject an old reader/writer when its input or output is in the live repo."""

    if live_alpha_repository_for_path(path) is not None:
        raise UnauthorizedOperation(
            f"{surface} is retired for the live repository; use the active "
            "Alpha readiness boundary or CertifiedResearchGateway"
        )


def reject_retired_real_history_surface(surface: str) -> None:
    """Unconditionally reject an obsolete row-read or evaluation capability."""

    raise UnauthorizedOperation(
        f"{surface} is retired; real-history work must use the active Alpha "
        "readiness boundary or CertifiedResearchGateway"
    )
