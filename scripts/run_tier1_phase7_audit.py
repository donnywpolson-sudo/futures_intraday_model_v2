"""Retired one-off Phase 7 evidence script.

The historical implementation used a hard-coded prediction release.  It is
preserved by Git history and must not be used for current Alpha research.
"""

from __future__ import annotations

from pathlib import Path

from futures_rebuild.current_research_surface import (
    reject_retired_project_execution,
)


def main(*, repository_root: Path | None = None) -> int:
    root = (repository_root or Path.cwd()).resolve(strict=False)
    reject_retired_project_execution(
        root=root,
        surface="legacy hard-coded Tier 1 Phase 7 audit",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
