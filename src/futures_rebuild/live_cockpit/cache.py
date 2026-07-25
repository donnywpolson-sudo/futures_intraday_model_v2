"""Bounded SQLite storage for canonical one-minute bars."""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .feed import normalize_ts_event


DEFAULT_RETENTION_DAYS = 8
DEFAULT_MAX_ROWS = 500_000


class BarCache:
    def __init__(
        self,
        path: Path,
        *,
        retention_days: int = DEFAULT_RETENTION_DAYS,
        max_rows: int = DEFAULT_MAX_ROWS,
    ) -> None:
        if retention_days <= 0:
            raise ValueError("retention_days must be positive")
        if max_rows <= 0:
            raise ValueError("max_rows must be positive")
        self.path = path
        self.retention_days = retention_days
        self.max_rows = max_rows
        self._lock = threading.RLock()
        self._connection = self._open_or_recover()

    def _open(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, check_same_thread=False, timeout=5.0)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
        except Exception:
            connection.close()
            raise
        connection.execute("PRAGMA synchronous=NORMAL")
        result = connection.execute("PRAGMA quick_check").fetchone()
        if result is None or result[0] != "ok":
            connection.close()
            raise sqlite3.DatabaseError("SQLite quick_check failed")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS bars (
                dataset TEXT NOT NULL,
                instrument_id INTEGER NOT NULL,
                raw_symbol TEXT NOT NULL,
                ts_event_ns INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL,
                PRIMARY KEY (dataset, instrument_id, ts_event_ns)
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_bars_lookup "
            "ON bars(dataset, instrument_id, ts_event_ns)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS history_coverage (
                dataset TEXT NOT NULL,
                instrument_id INTEGER NOT NULL,
                raw_symbol TEXT NOT NULL,
                start_ns INTEGER NOT NULL,
                end_ns INTEGER NOT NULL,
                PRIMARY KEY (dataset, instrument_id, start_ns, end_ns),
                CHECK (start_ns < end_ns)
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_history_coverage_lookup "
            "ON history_coverage(dataset, instrument_id, start_ns, end_ns)"
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS market_bindings (
                dataset TEXT NOT NULL,
                market TEXT NOT NULL,
                raw_symbol TEXT NOT NULL,
                instrument_id INTEGER NOT NULL,
                session_start_ns INTEGER NOT NULL,
                updated_ns INTEGER NOT NULL,
                PRIMARY KEY (dataset, market)
            )
            """
        )
        connection.commit()
        return connection

    def _open_or_recover(self) -> sqlite3.Connection:
        try:
            return self._open()
        except sqlite3.DatabaseError:
            if not self.path.exists():
                raise
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            destination = self.path.with_name(f"{self.path.name}.corrupt-{timestamp}")
            suffix = 1
            while destination.exists():
                destination = self.path.with_name(
                    f"{self.path.name}.corrupt-{timestamp}-{suffix}"
                )
                suffix += 1
            self.path.replace(destination)
            for sidecar_suffix in ("-wal", "-shm"):
                sidecar = Path(f"{self.path}{sidecar_suffix}")
                if sidecar.exists():
                    sidecar.replace(Path(f"{destination}{sidecar_suffix}"))
            return self._open()

    @staticmethod
    def _timestamp_ns(value: object) -> int:
        return int(normalize_ts_event(value).timestamp() * 1_000_000_000)

    def put_bars(
        self,
        *,
        dataset: str,
        instrument_id: int,
        raw_symbol: str,
        bars: Iterable[Mapping[str, Any]],
        now: datetime | None = None,
    ) -> int:
        return self.put_bar_batches(
            [(dataset, int(instrument_id), raw_symbol, list(bars))], now=now
        )

    def put_bar_batches(
        self,
        batches: Iterable[
            tuple[str, int, str, Sequence[Mapping[str, Any]]]
        ],
        *,
        now: datetime | None = None,
    ) -> int:
        rows = [
            (
                dataset,
                int(instrument_id),
                raw_symbol,
                self._timestamp_ns(bar["time"]),
                float(bar["open"]),
                float(bar["high"]),
                float(bar["low"]),
                float(bar["close"]),
                int(bar["volume"]),
            )
            for dataset, instrument_id, raw_symbol, bars in batches
            for bar in bars
        ]
        if not rows:
            return 0
        with self._lock:
            self._connection.executemany(
                """
                INSERT INTO bars(
                    dataset, instrument_id, raw_symbol, ts_event_ns,
                    open, high, low, close, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset, instrument_id, ts_event_ns) DO UPDATE SET
                    raw_symbol=excluded.raw_symbol,
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume
                """,
                rows,
            )
            self._prune_locked(now=now)
            self._connection.commit()
        return len(rows)

    def record_coverage(
        self,
        *,
        dataset: str,
        instrument_id: int,
        raw_symbol: str,
        start: datetime,
        end: datetime,
        now: datetime | None = None,
    ) -> None:
        start_ns = self._timestamp_ns(start)
        end_ns = self._timestamp_ns(end)
        if start_ns >= end_ns:
            raise ValueError("coverage start must be before end")
        adjacency_ns = 60 * 1_000_000_000
        with self._lock:
            overlaps = self._connection.execute(
                """
                SELECT start_ns, end_ns FROM history_coverage
                WHERE dataset = ? AND instrument_id = ?
                  AND end_ns >= ? AND start_ns <= ?
                """,
                (
                    dataset,
                    int(instrument_id),
                    start_ns - adjacency_ns,
                    end_ns + adjacency_ns,
                ),
            ).fetchall()
            merged_start = min([start_ns, *(int(row[0]) for row in overlaps)])
            merged_end = max([end_ns, *(int(row[1]) for row in overlaps)])
            for covered_start, covered_end in overlaps:
                self._connection.execute(
                    """
                    DELETE FROM history_coverage
                    WHERE dataset = ? AND instrument_id = ?
                      AND start_ns = ? AND end_ns = ?
                    """,
                    (dataset, int(instrument_id), covered_start, covered_end),
                )
            self._connection.execute(
                """
                INSERT INTO history_coverage(
                    dataset, instrument_id, raw_symbol, start_ns, end_ns
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (dataset, int(instrument_id), raw_symbol, merged_start, merged_end),
            )
            self._prune_locked(now=now)
            self._connection.commit()

    def get_coverage(
        self,
        *,
        dataset: str,
        instrument_id: int,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, datetime]]:
        start_ns = self._timestamp_ns(start)
        end_ns = self._timestamp_ns(end)
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT start_ns, end_ns FROM history_coverage
                WHERE dataset = ? AND instrument_id = ?
                  AND end_ns > ? AND start_ns < ?
                ORDER BY start_ns
                """,
                (dataset, int(instrument_id), start_ns, end_ns),
            ).fetchall()
        return [
            (
                datetime.fromtimestamp(row[0] / 1_000_000_000, tz=timezone.utc),
                datetime.fromtimestamp(row[1] / 1_000_000_000, tz=timezone.utc),
            )
            for row in rows
        ]

    def put_market_binding(
        self,
        *,
        dataset: str,
        market: str,
        raw_symbol: str,
        instrument_id: int,
        session_start: datetime,
        now: datetime | None = None,
    ) -> None:
        updated = now or datetime.now(timezone.utc)
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO market_bindings(
                    dataset, market, raw_symbol, instrument_id,
                    session_start_ns, updated_ns
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset, market) DO UPDATE SET
                    raw_symbol=excluded.raw_symbol,
                    instrument_id=excluded.instrument_id,
                    session_start_ns=excluded.session_start_ns,
                    updated_ns=excluded.updated_ns
                """,
                (
                    dataset,
                    market.strip().upper(),
                    raw_symbol.strip().upper(),
                    int(instrument_id),
                    self._timestamp_ns(session_start),
                    self._timestamp_ns(updated),
                ),
            )
            self._connection.commit()

    def get_market_bindings(
        self,
        *,
        dataset: str,
        session_start: datetime,
    ) -> dict[str, tuple[str, int]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT market, raw_symbol, instrument_id
                FROM market_bindings
                WHERE dataset = ? AND session_start_ns = ?
                ORDER BY market
                """,
                (dataset, self._timestamp_ns(session_start)),
            ).fetchall()
        return {
            str(market): (str(raw_symbol), int(instrument_id))
            for market, raw_symbol, instrument_id in rows
        }

    def get_bars(
        self,
        *,
        dataset: str,
        instrument_id: int,
        start: datetime,
        end: datetime | None = None,
    ) -> list[dict[str, object]]:
        query = (
            "SELECT ts_event_ns, open, high, low, close, volume "
            "FROM bars WHERE dataset = ? AND instrument_id = ? AND ts_event_ns >= ?"
        )
        parameters: list[object] = [dataset, int(instrument_id), self._timestamp_ns(start)]
        if end is not None:
            query += " AND ts_event_ns < ?"
            parameters.append(self._timestamp_ns(end))
        query += " ORDER BY ts_event_ns"
        with self._lock:
            rows = self._connection.execute(query, parameters).fetchall()
        return [
            {
                "time": datetime.fromtimestamp(row[0] / 1_000_000_000, tz=timezone.utc),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": int(row[5]),
            }
            for row in rows
        ]

    def _prune_locked(self, *, now: datetime | None = None) -> None:
        current = now or datetime.now(timezone.utc)
        cutoff_ns = int(
            (current - timedelta(days=self.retention_days)).timestamp() * 1_000_000_000
        )
        self._connection.execute("DELETE FROM bars WHERE ts_event_ns < ?", (cutoff_ns,))
        self._connection.execute(
            "DELETE FROM history_coverage WHERE end_ns <= ?", (cutoff_ns,)
        )
        self._connection.execute(
            "DELETE FROM market_bindings WHERE session_start_ns < ?", (cutoff_ns,)
        )
        self._connection.execute(
            "UPDATE history_coverage SET start_ns = ? "
            "WHERE start_ns < ? AND end_ns > ?",
            (cutoff_ns, cutoff_ns, cutoff_ns),
        )
        row_count = int(self._connection.execute("SELECT COUNT(*) FROM bars").fetchone()[0])
        excess = row_count - self.max_rows
        if excess > 0:
            self._connection.execute(
                "DELETE FROM bars WHERE rowid IN "
                "(SELECT rowid FROM bars ORDER BY ts_event_ns ASC LIMIT ?)",
                (excess,),
            )
            # Row-cap eviction is not exchange-calendar aware. Drop coverage
            # claims conservatively rather than claiming bars that were evicted.
            self._connection.execute("DELETE FROM history_coverage")

    def count(self) -> int:
        with self._lock:
            return int(self._connection.execute("SELECT COUNT(*) FROM bars").fetchone()[0])

    def close(self) -> None:
        with self._lock:
            self._connection.close()
