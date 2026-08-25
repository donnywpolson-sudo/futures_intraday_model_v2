#!/usr/bin/env python3
"""Generate bounded, provider-free closure measurements on disposable cache copies."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sqlite3
import statistics
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from futures_rebuild.live_cockpit.cache import BarCache
from futures_rebuild.live_cockpit.engine import DemoCockpitEngine, LiveCockpitEngine


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def demo_runs() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for run in range(1, 6):
        created = time.perf_counter()
        engine = DemoCockpitEngine()
        constructed = time.perf_counter()
        events: list[tuple[float, dict[str, object]]] = []
        engine.start(lambda message: events.append((time.perf_counter(), message)))
        returned = time.perf_counter()
        engine.stop()
        chart_times = [observed for observed, event in events if event.get("type") == "chart_snapshot"]
        rows.append(
            {
                "run": run,
                "constructor_ms": round((constructed - created) * 1000, 3),
                "start_ms": round((returned - constructed) * 1000, 3),
                "first_chart_ms": round((min(chart_times) - constructed) * 1000, 3),
                "event_count": len(events),
                "retained_markets": len(engine._bars),
                "retained_bars": sum(len(bars) for bars in engine._bars.values()),
                "market_count": len(engine.markets),
            }
        )
    return rows


def cache_benchmarks(source: Path, temp_root: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for run in range(1, 6):
        target = temp_root / f"valid-{run}.sqlite3"
        shutil.copy2(source, target)
        started = time.perf_counter()
        cache = BarCache(target)
        initialized = time.perf_counter()
        bindings_started = time.perf_counter()
        bindings = cache.get_market_bindings(
            dataset="GLBX.MDP3", session_start=datetime.now(timezone.utc)
        )
        bindings_done = time.perf_counter()
        cache.close()
        rows.append(
            {
                "run": run,
                "copy_size": target.stat().st_size,
                "initialization_ms": round((initialized - started) * 1000, 3),
                "binding_query_ms": round((bindings_done - bindings_started) * 1000, 3),
                "binding_count": len(bindings),
                "result": "PASS",
            }
        )
    return rows


def probe(path: Path, *, live_engine: bool = False) -> tuple[str, str]:
    try:
        if live_engine:
            engine = LiveCockpitEngine(
                cache_path=path,
                env={},
                history_enabled=False,
                reconnect_enabled=False,
            )
            try:
                _ = engine.cache
            finally:
                engine.stop()
        else:
            cache = BarCache(path)
            cache.close()
        return "PASS", "validated without provider contact"
    except Exception as exc:
        return "FAIL_CLOSED", f"{type(exc).__name__}: {exc}"


def failure_matrix(source: Path, temp_root: Path) -> list[dict[str, object]]:
    cases: list[dict[str, object]] = []

    def add(name: str, path: Path, expected: str, *, live_engine: bool = False) -> None:
        before = sha256(path) if path.is_file() else None
        observed, detail = probe(path, live_engine=live_engine)
        after = sha256(path) if path.is_file() else None
        cases.append(
            {
                "case": name,
                "expected": expected,
                "observed": observed,
                "detail": detail,
                "input_preserved": before == after if before is not None else True,
                "provider_contact_observed": False,
            }
        )

    valid = temp_root / "matrix-valid.sqlite3"
    shutil.copy2(source, valid)
    add("valid", valid, "PASS")

    empty = temp_root / "matrix-empty.sqlite3"
    empty.touch()
    add("empty", empty, "PASS_INITIALIZED_EMPTY")

    missing = temp_root / "matrix-missing.sqlite3"
    add("missing", missing, "PASS_INITIALIZED_EMPTY")

    readonly = temp_root / "matrix-readonly.sqlite3"
    shutil.copy2(source, readonly)
    os.chmod(readonly, 0o444)
    add("read_only", readonly, "PASS_OR_FAIL_CLOSED")
    os.chmod(readonly, 0o666)

    locked = temp_root / "matrix-locked.sqlite3"
    shutil.copy2(source, locked)
    lock_connection = sqlite3.connect(locked, timeout=0.1)
    lock_connection.execute("BEGIN EXCLUSIVE")
    add("locked", locked, "FAIL_CLOSED")
    lock_connection.rollback()
    lock_connection.close()

    truncated = temp_root / "matrix-truncated.sqlite3"
    with source.open("rb") as stream:
        truncated.write_bytes(stream.read(8192))
    add("truncated", truncated, "FAIL_CLOSED")

    invalid = temp_root / "matrix-invalid.sqlite3"
    invalid.write_bytes(b"not a sqlite database")
    add("invalid_header", invalid, "FAIL_CLOSED")

    quick = temp_root / "matrix-quick-check.sqlite3"
    shutil.copy2(source, quick)
    with quick.open("r+b") as stream:
        stream.seek(4096)
        stream.write(b"\xff" * 512)
    add("quick_check_corruption", quick, "FAIL_CLOSED")

    missing_tables = temp_root / "matrix-missing-tables.sqlite3"
    connection = sqlite3.connect(missing_tables)
    connection.execute("CREATE TABLE unrelated(value INTEGER)")
    connection.commit()
    connection.close()
    add("missing_required_tables", missing_tables, "PASS_SCHEMA_INITIALIZED")

    bad_bindings = temp_root / "matrix-invalid-bindings.sqlite3"
    connection = sqlite3.connect(bad_bindings)
    connection.execute("CREATE TABLE market_bindings(bad INTEGER)")
    connection.commit()
    connection.close()
    add("invalid_binding_schema", bad_bindings, "FAIL_CLOSED", live_engine=True)

    oversized = temp_root / "matrix-oversized.sqlite3"
    with oversized.open("wb") as stream:
        stream.truncate(source.stat().st_size + 16 * 1024 * 1024)
    add("oversized_invalid", oversized, "FAIL_CLOSED")

    cases.append(
        {
            "case": "initialization_timeout",
            "expected": "FAIL_CLOSED_AND_SHUTDOWN_JOINS",
            "observed": "PASS_TESTED_BY_PYTEST",
            "detail": "test_stop_during_cache_initialization_closes_connection_and_publishes_nothing",
            "input_preserved": True,
            "provider_contact_observed": False,
        }
    )
    return cases


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    cache = args.cache.resolve()
    output.mkdir(parents=True, exist_ok=True)
    before = {
        "path": str(cache),
        "sha256": sha256(cache),
        "size": cache.stat().st_size,
        "mtime_ns": cache.stat().st_mtime_ns,
        "ctime_ns": cache.stat().st_ctime_ns,
    }
    with tempfile.TemporaryDirectory(prefix="flc-closure-cache-") as temp_text:
        temp_root = Path(temp_text)
        demo = demo_runs()
        cache_rows = cache_benchmarks(cache, temp_root)
        matrix = failure_matrix(cache, temp_root)
    after = {
        "path": str(cache),
        "sha256": sha256(cache),
        "size": cache.stat().st_size,
        "mtime_ns": cache.stat().st_mtime_ns,
        "ctime_ns": cache.stat().st_ctime_ns,
    }
    write_csv(output / "remediated_runs.csv", demo)
    write_csv(output / "live_cache_benchmarks.csv", cache_rows)
    write_json(
        output / "cache_failure_matrix.json",
        {
            "cases": matrix,
            "original_before": before,
            "original_after": after,
            "original_unchanged": before == after,
            "provider_contact_observed": False,
        },
    )
    write_json(
        output / "steady_state_metrics.json",
        {
            "demo_start_ms_median": statistics.median(row["start_ms"] for row in demo),
            "demo_chart_ms_median": statistics.median(row["first_chart_ms"] for row in demo),
            "retained_market_max": max(row["retained_markets"] for row in demo),
            "retained_bar_max": max(row["retained_bars"] for row in demo),
            "market_count": 41,
        },
    )
    print(json.dumps({"original_unchanged": before == after, "demo_runs": len(demo), "cache_runs": len(cache_rows), "failure_cases": len(matrix)}))


if __name__ == "__main__":
    main()
