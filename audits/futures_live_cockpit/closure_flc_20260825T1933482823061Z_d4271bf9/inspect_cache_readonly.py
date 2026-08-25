#!/usr/bin/env python3
"""Print bounded SQLite structure and time ranges without opening the cache writable."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cache", type=Path)
    args = parser.parse_args()
    cache = args.cache.resolve()
    uri = cache.as_uri() + "?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    try:
        tables = [
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        result: dict[str, object] = {
            "pragmas": {
                name: connection.execute(f"PRAGMA {name}").fetchone()[0]
                for name in (
                    "application_id",
                    "auto_vacuum",
                    "freelist_count",
                    "journal_mode",
                    "page_count",
                    "page_size",
                    "schema_version",
                    "secure_delete",
                    "user_version",
                )
            }
        }
        result["schema_objects"] = [
            {
                "type": row[0],
                "name": row[1],
                "table": row[2],
                "rootpage": row[3],
                "sql": row[4],
            }
            for row in connection.execute(
                "SELECT type, name, tbl_name, rootpage, sql FROM sqlite_master ORDER BY type, name"
            )
        ]
        for table in tables:
            quoted = quote_identifier(table)
            columns = [
                str(row[1])
                for row in connection.execute(f"PRAGMA table_info({quoted})")
            ]
            details: dict[str, object] = {
                "rows": int(connection.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]),
                "columns": columns,
            }
            for column in (
                "time",
                "ts_event",
                "ts_event_ns",
                "observed_at",
                "session_start",
                "session_start_ns",
                "updated_at",
                "updated_ns",
                "start_ns",
                "end_ns",
            ):
                if column not in columns:
                    continue
                quoted_column = quote_identifier(column)
                minimum, maximum = connection.execute(
                    f"SELECT MIN({quoted_column}), MAX({quoted_column}) FROM {quoted}"
                ).fetchone()
                details[f"min_{column}"] = minimum
                details[f"max_{column}"] = maximum
            result[table] = details
        try:
            result["dbstat"] = [
                {
                    "name": row[0],
                    "path": row[1],
                    "pageno": row[2],
                    "pagetype": row[3],
                    "ncell": row[4],
                    "payload": row[5],
                    "unused": row[6],
                }
                for row in connection.execute(
                    "SELECT name, path, pageno, pagetype, ncell, payload, unused "
                    "FROM dbstat WHERE name IN ('bars', 'history_coverage', 'market_bindings') "
                    "ORDER BY name, pageno"
                )
            ]
        except sqlite3.OperationalError as exc:
            result["dbstat_error"] = str(exc)
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
