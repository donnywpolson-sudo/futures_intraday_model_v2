"""Collect source-safe pytest lane counts without executing tests."""

from __future__ import annotations

import json
from collections import Counter

import pytest


class LaneCounter:
    def pytest_collection_finish(self, session: pytest.Session) -> None:
        counts: Counter[str] = Counter()
        for item in session.items:
            lane = next(
                (
                    name for name in ("current", "high_risk", "legacy", "local_evidence")
                    if item.get_closest_marker(name) is not None
                ),
                "unclassified",
            )
            counts[lane] += 1
        print(json.dumps({"collected": len(session.items), "lanes": dict(sorted(counts.items()))}, sort_keys=True))


def main() -> int:
    return int(pytest.main(
        [
            "--collect-only", "--basetemp=.pytest_tmp/lane-audit-source-safe-v2",
            "-m", "not __never__", "-p", "no:terminal", "-o", "addopts=",
        ],
        plugins=[LaneCounter()],
    ))


if __name__ == "__main__":
    raise SystemExit(main())
