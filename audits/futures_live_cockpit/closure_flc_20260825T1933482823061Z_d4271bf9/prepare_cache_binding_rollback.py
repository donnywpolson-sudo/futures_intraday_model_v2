#!/usr/bin/env python3
"""Prepare a verified cache candidate that removes only incident binding rows."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
from pathlib import Path
from typing import Iterable


EXPECTED_LIVE_SHA256 = "75e2463c8b5398a6fb76f5ce70c7a32833a472e65f11dda5f832f1599d92186a"
INCIDENT_SESSION_START_NS = 1_787_695_200_000_000_000
INCIDENT_UPDATED_MIN_NS = 1_787_696_522_856_389_120
INCIDENT_UPDATED_MAX_NS = 1_787_696_522_861_999_104
EXPECTED_BAR_ROWS = 201_655
EXPECTED_COVERAGE_ROWS = 43
EXPECTED_BINDING_ROWS = 41


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def row_digest(connection: sqlite3.Connection, query: str) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    for row in connection.execute(query):
        encoded = json.dumps(row, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        digest.update(encoded)
        digest.update(b"\n")
        count += 1
    return count, digest.hexdigest()


def logical_state(path: Path, *, writable: bool = False) -> dict[str, object]:
    if writable:
        connection = sqlite3.connect(path, timeout=1.0)
    else:
        connection = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
    try:
        bars = row_digest(
            connection,
            "SELECT dataset, instrument_id, raw_symbol, ts_event_ns, open, high, low, close, volume "
            "FROM bars ORDER BY dataset, instrument_id, ts_event_ns",
        )
        coverage = row_digest(
            connection,
            "SELECT dataset, instrument_id, raw_symbol, start_ns, end_ns "
            "FROM history_coverage ORDER BY dataset, instrument_id, start_ns, end_ns",
        )
        bindings = row_digest(
            connection,
            "SELECT dataset, market, raw_symbol, instrument_id, session_start_ns, updated_ns "
            "FROM market_bindings ORDER BY dataset, market",
        )
        binding_bounds = connection.execute(
            "SELECT MIN(session_start_ns), MAX(session_start_ns), MIN(updated_ns), MAX(updated_ns) "
            "FROM market_bindings"
        ).fetchone()
        quick_check = connection.execute("PRAGMA quick_check").fetchone()
        return {
            "bars": {"rows": bars[0], "sha256": bars[1]},
            "history_coverage": {"rows": coverage[0], "sha256": coverage[1]},
            "market_bindings": {
                "rows": bindings[0],
                "sha256": bindings[1],
                "min_session_start_ns": binding_bounds[0],
                "max_session_start_ns": binding_bounds[1],
                "min_updated_ns": binding_bounds[2],
                "max_updated_ns": binding_bounds[3],
            },
            "quick_check": None if quick_check is None else quick_check[0],
        }
    finally:
        connection.close()


def require_preconditions(state: dict[str, object]) -> None:
    bars = state["bars"]
    coverage = state["history_coverage"]
    bindings = state["market_bindings"]
    if not isinstance(bars, dict) or bars.get("rows") != EXPECTED_BAR_ROWS:
        raise RuntimeError("unexpected bar-row identity")
    if not isinstance(coverage, dict) or coverage.get("rows") != EXPECTED_COVERAGE_ROWS:
        raise RuntimeError("unexpected coverage-row identity")
    expected_bindings = {
        "rows": EXPECTED_BINDING_ROWS,
        "min_session_start_ns": INCIDENT_SESSION_START_NS,
        "max_session_start_ns": INCIDENT_SESSION_START_NS,
        "min_updated_ns": INCIDENT_UPDATED_MIN_NS,
        "max_updated_ns": INCIDENT_UPDATED_MAX_NS,
    }
    if not isinstance(bindings, dict) or any(
        bindings.get(name) != value for name, value in expected_bindings.items()
    ):
        raise RuntimeError("binding rows do not exactly match the incident")
    if state.get("quick_check") != "ok":
        raise RuntimeError("source cache quick_check did not pass")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("live", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()
    live = args.live.resolve()
    candidate = args.candidate.resolve()
    if candidate.exists():
        raise RuntimeError("candidate already exists")
    if candidate.parent != live.parent:
        raise RuntimeError("candidate must be beside the live cache for atomic replacement")
    if file_sha256(live) != EXPECTED_LIVE_SHA256:
        raise RuntimeError("live cache bytes drifted before rollback preparation")
    for suffix in ("-wal", "-shm", "-journal"):
        if Path(str(live) + suffix).exists():
            raise RuntimeError(f"live SQLite sidecar exists: {suffix}")

    before = logical_state(live)
    require_preconditions(before)
    shutil.copy2(live, candidate)
    connection = sqlite3.connect(candidate, timeout=1.0)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("BEGIN IMMEDIATE")
        cursor = connection.execute(
            "DELETE FROM market_bindings "
            "WHERE session_start_ns = ? AND updated_ns BETWEEN ? AND ?",
            (
                INCIDENT_SESSION_START_NS,
                INCIDENT_UPDATED_MIN_NS,
                INCIDENT_UPDATED_MAX_NS,
            ),
        )
        if cursor.rowcount != EXPECTED_BINDING_ROWS:
            connection.rollback()
            raise RuntimeError(f"expected to delete 41 bindings, observed {cursor.rowcount}")
        connection.commit()
        quick_check = connection.execute("PRAGMA quick_check").fetchone()
        if quick_check is None or quick_check[0] != "ok":
            raise RuntimeError("candidate quick_check did not pass")
    finally:
        connection.close()

    after = logical_state(candidate)
    if after["bars"] != before["bars"]:
        raise RuntimeError("bar rows changed in rollback candidate")
    if after["history_coverage"] != before["history_coverage"]:
        raise RuntimeError("coverage rows changed in rollback candidate")
    bindings_after = after["market_bindings"]
    if not isinstance(bindings_after, dict) or bindings_after.get("rows") != 0:
        raise RuntimeError("rollback candidate retains market bindings")
    print(
        json.dumps(
            {
                "status": "PASS",
                "live_sha256": file_sha256(live),
                "candidate_sha256": file_sha256(candidate),
                "before": before,
                "after": after,
                "deleted_binding_rows": EXPECTED_BINDING_ROWS,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
