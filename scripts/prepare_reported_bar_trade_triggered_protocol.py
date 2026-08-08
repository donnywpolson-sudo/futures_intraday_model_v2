"""Prepare sealed topology, rejection, and trade-triggered protocol only."""

from __future__ import annotations

from pathlib import Path

from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    raise IntegrityError(
        "superseded invalid pre-data preparation; use the additive correction preparer"
    )


if __name__ == "__main__":
    raise SystemExit(main())
