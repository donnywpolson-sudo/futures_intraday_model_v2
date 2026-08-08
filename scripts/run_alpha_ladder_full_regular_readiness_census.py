"""Host runner for a separately authorized full-regular readiness census."""

from __future__ import annotations


def main() -> int:
    raise SystemExit(
        "BLOCKED: this historical-row runner is invoked only by Codex after one "
        "exact approval and a verified single-use authorization receipt"
    )


if __name__ == "__main__":
    raise SystemExit(main())
