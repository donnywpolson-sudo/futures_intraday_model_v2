"""Saved-record playback. Never imports a simulator or model."""
from __future__ import annotations

import copy
import random
import threading
import time
from pathlib import Path

from ..simulation_records import load_bundle, load_library, load_library_session
from .protocol import event


class ReplayCockpitEngine:
    def __init__(self, directory: Path) -> None:
        self.directory, self._bundle, self.library_directory, self.library = self._read_source(directory)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._publish = None
        self.cursor = 0
        self.speed = 1
        self.playing = False
        self._credit = 0.0
        self._last_tick = time.monotonic()

    @staticmethod
    def _read_source(path: Path):
        path = Path(path).resolve(strict=True)
        if path.is_dir():
            path = path / ("library.json" if (path / "library.json").exists() else "result.json")
        if path.name == "result.json":
            return path.parent, load_bundle(path.parent), None, None
        if path.name != "library.json":
            raise ValueError("Choose result.json or library.json")
        library = load_library(path)
        first = None
        for entry in library["sessions"]:
            if entry["status"] == "INCLUDED":
                loaded = load_library_session(path.parent, library, entry)
                if first is None:
                    first = loaded
        if first is None:
            raise ValueError("Library has no playable sessions; inspect its recorded exclusions")
        return *first, path.parent, library

    def bootstrap_event(self) -> dict:
        meta = self._bundle["metadata"]
        return event("bootstrap", {
            "markets": [{"symbol": meta["market"], "name": meta["market"], "family": "Other",
                         "description": "Saved offline replay", "status": "WAITING", "last": None}],
            "selected_market": meta["market"], "timeframe": "1m", "timeframes": ["1m"],
            "chart_range": "1D", "chart_ranges": ["1D"], "quick_markets": [],
            "mode": "replay", "observation_only": True, "display_tz": "America/Los_Angeles",
            "prediction_capability": {"mode": "offline", "synthetic": False, "observation_only": True},
            "history_cache_capability": {"enabled": False}, "max_provider_sessions": 0,
        })

    def snapshot(self) -> dict:
        with self._lock:
            return event("replay_update", {
                "metadata": copy.deepcopy(self._bundle["metadata"]),
                "cursor": self.cursor, "total": len(self._bundle["records"]),
                "playing": self.playing, "speed": self.speed,
                # ponytail: one session (1440 bars); bounded prefixes simplify restart/poll recovery.
                "records": copy.deepcopy(self._bundle["records"][:self.cursor]),
                "summary": copy.deepcopy(self._bundle["summary"]) if self.cursor == len(self._bundle["records"]) else None,
                "library": copy.deepcopy(self.library),
            })

    def _emit(self) -> None:
        if self._publish is not None:
            self._publish(self.snapshot())

    def open_run(self, directory: Path) -> None:
        # Validate before replacing the active run; a bad file preserves its cursor.
        directory, bundle, library_directory, library = self._read_source(directory)
        with self._lock:
            if self._stop.is_set():
                raise ValueError("Replay stopped")
            self._bundle = bundle
            self.directory = directory.resolve()
            self.library_directory, self.library = library_directory, library
            self.cursor, self.playing, self._credit = 0, False, 0.0
            self._last_tick = time.monotonic()
            if self._publish is not None:
                self._publish(self.bootstrap_event())
            self._emit()

    def select_session(self, session: str | None = None) -> dict:
        with self._lock:
            if self._stop.is_set() or self.library is None:
                raise ValueError("A session library must be open")
            entries = [e for e in self.library["sessions"] if e["status"] == "INCLUDED"]
            if session is None:
                alternatives = [e for e in entries if e["session"] != self._bundle["metadata"]["session"]]
                entry = random.choice(alternatives or entries)
            else:
                entry = next((e for e in entries if e["session"] == session), None)
                if entry is None:
                    raise ValueError("Session is not playable in this library")
            directory, bundle = load_library_session(self.library_directory, self.library, entry)
            self.directory, self._bundle = directory, bundle
            self.cursor, self.playing, self._credit = 0, False, 0.0
            self._last_tick = time.monotonic()
            if self._publish is not None:
                self._publish(self.bootstrap_event())
            self._emit()
        return {"ok": True}

    def start(self, publish) -> None:
        with self._lock:
            if self._stop.is_set() or self._thread is not None:
                return
            self._publish = publish
            self._emit()
            self._thread = threading.Thread(target=self._worker, name="cockpit-replay", daemon=True)
            self._thread.start()

    def command(self, action: str, speed: int | None = None, index: int | None = None) -> dict:
        with self._lock:
            if self._stop.is_set():
                raise ValueError("Replay stopped")
            if action == "speed":
                if type(speed) is not int or speed not in (1, 10, 100):
                    raise ValueError("Unsupported playback rate")
                self.speed = speed
            elif action == "play":
                self.playing = self.cursor < len(self._bundle["records"])
            elif action == "pause":
                self.playing = False
            elif action == "step":
                self.playing = False
                self.cursor = min(self.cursor + 1, len(self._bundle["records"]))
            elif action == "restart":
                self.playing, self.cursor = False, 0
            elif action == "next_prediction":
                self.playing = False
                target = next((i for i in range(self.cursor, len(self._bundle["records"]))
                    if self._bundle["records"][i].get("inspection", {}).get("status") == "ELIGIBLE"), None)
                if target is not None:
                    self.cursor = target + 1
            elif action == "outcome":
                if type(index) is not int or not 0 <= index < self.cursor:
                    raise ValueError("Select a reached prediction")
                target = next((i for i, row in enumerate(self._bundle["records"]) if any(
                    e["decision_index"] == index for e in row.get("outcomes", []))), None)
                if target is None:
                    raise ValueError("Outcome comparison unavailable for this prediction")
                self.playing, self.cursor = False, max(self.cursor, target + 1)
            else:
                raise ValueError("Unsupported replay command")
            self._credit = 0.0
            self._last_tick = time.monotonic()
            self._emit()
        return {"ok": True}

    def advance(self, elapsed: float) -> None:
        """Advance only the display cursor; elapsed comes from the playback clock."""
        with self._lock:
            if not self.playing or self._stop.is_set():
                return
            self._credit += max(0.0, elapsed) * self.speed
            count = int(self._credit)
            self._credit -= count
            if count:
                self.cursor = min(self.cursor + count, len(self._bundle["records"]))
                if self.cursor == len(self._bundle["records"]):
                    self.playing = False
                self._emit()

    def _worker(self) -> None:
        while not self._stop.wait(0.1):
            with self._lock:
                now = time.monotonic()
                self.advance(now - self._last_tick)
                self._last_tick = now

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def _unavailable(self, *args, **kwargs) -> bool:
        return False

    select_market = select_timeframe = select_chart_range = _unavailable
    retry_history = confirm_history_cache = set_history_cache_paused = _unavailable
    cancel_history_cache_after_current_request = retry_history_cache_estimate = _unavailable

    def set_visual_update_active(self, active: bool) -> float:
        return 10.0
