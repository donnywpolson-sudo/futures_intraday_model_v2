"""Desktop bridge, persisted preferences, self-check, and CLI entrypoint."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Mapping

from .credentials import (
    default_credential_locator_path,
    default_repository_package_api_env_path,
)
from futures_rebuild.high_risk import confirmation_required
from .feed import (
    API_KEY_ENV,
    ROOT,
    SUPPORTED_CHART_TIMEFRAMES,
    chart_market_universe,
    normalize_market,
    normalize_timeframe,
)

from .engine import (
    DEFAULT_VISUAL_UPDATE_MODE,
    VISUAL_UPDATE_HZ,
    CockpitEngine,
    DemoCockpitEngine,
    LiveCockpitEngine,
)
from .market_groups import load_alpha_tier_grouping


APP_NAME = "Futures Live Cockpit"
STATE_FILENAME = "state.json"
CACHE_FILENAME = "bars.sqlite3"
REQUIRED_ASSETS = (
    "index.html",
    "styles.css",
    "time-format.js",
    "app.js",
    "lightweight-charts.standalone.production.js",
    "NOTICE-lightweight-charts.txt",
)
BOOLEAN_UI_PREFERENCE_KEYS = frozenset(
    {
        "prediction_panel_open",
        "show_session_boundaries",
        "show_volume",
        "show_predictions",
    }
)
GROUP_LIST_UI_PREFERENCE_KEYS = frozenset(
    {
        "sector_group_order",
        "alpha_tier_group_order",
        "collapsed_sector_groups",
        "collapsed_alpha_tier_groups",
    }
)
MARKET_GROUPING_MODES = frozenset({"sector", "alpha_tier"})
MAX_PERSISTED_GROUP_IDS = 32


def app_data_dir(env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    local_appdata = values.get("LOCALAPPDATA")
    if local_appdata:
        return Path(local_appdata) / "futures_intraday_model_v2" / "live_cockpit"
    return Path.home() / ".futures_intraday_model_v2" / "live_cockpit"


def assets_dir() -> Path:
    frozen_root = getattr(sys, "_MEIPASS", None)
    if frozen_root:
        return Path(frozen_root) / "futures_rebuild" / "live_cockpit" / "assets"
    return Path(__file__).resolve().parent / "assets"


def desktop_asset_target(*, demo: bool) -> str:
    """Return a native local path so pywebview serves assets over loopback HTTP."""
    mode = "demo" if demo else "live"
    return f"{(assets_dir() / 'index.html').resolve()}?mode={mode}"


def load_state(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return value if isinstance(value, dict) else {}


def save_state(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(dict(state), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _sanitize_group_ids(value: object) -> list[str] | None:
    if not isinstance(value, list):
        return None
    result: list[str] = []
    for item in value[:MAX_PERSISTED_GROUP_IDS]:
        if not isinstance(item, str):
            continue
        normalized = item.strip().lower()
        if (
            not normalized
            or len(normalized) > 48
            or any(
                character not in "abcdefghijklmnopqrstuvwxyz0123456789_-"
                for character in normalized
            )
            or normalized in result
        ):
            continue
        result.append(normalized)
    return result


def sanitize_ui_preferences(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    sanitized: dict[str, Any] = {
        key: item
        for key, item in value.items()
        if key in BOOLEAN_UI_PREFERENCE_KEYS and isinstance(item, bool)
    }
    visual_mode = value.get("visual_update_mode")
    if isinstance(visual_mode, str) and visual_mode.strip().lower() in VISUAL_UPDATE_HZ:
        sanitized["visual_update_mode"] = visual_mode.strip().lower()
    grouping_mode = value.get("market_grouping_mode")
    if (
        isinstance(grouping_mode, str)
        and grouping_mode.strip().lower() in MARKET_GROUPING_MODES
    ):
        sanitized["market_grouping_mode"] = grouping_mode.strip().lower()
    for key in GROUP_LIST_UI_PREFERENCE_KEYS:
        group_ids = _sanitize_group_ids(value.get(key))
        if group_ids is not None:
            sanitized[key] = group_ids
    return sanitized


def _webview2_candidates(env: Mapping[str, str]) -> list[Path]:
    candidates: list[Path] = []
    for key in ("PROGRAMFILES(X86)", "PROGRAMFILES", "LOCALAPPDATA"):
        root = env.get(key)
        if root:
            candidates.append(Path(root) / "Microsoft" / "EdgeWebView" / "Application")
    return candidates


def self_check(*, env: Mapping[str, str] | None = None) -> dict[str, Any]:
    values = dict(os.environ if env is None else env)
    asset_root = assets_dir()
    asset_results = {name: (asset_root / name).is_file() for name in REQUIRED_ASSETS}
    asset_target = desktop_asset_target(demo=True)
    asset_launch_target_local = (
        not asset_target.lower().startswith(("file://", "http://", "https://"))
        and Path(asset_target.split("?", 1)[0]).is_file()
    )
    markets = chart_market_universe()
    alpha_tiers = load_alpha_tier_grouping(
        ROOT / "configs" / "alpha_tiered.yaml",
        [market.symbol for market in markets],
    )
    state_root = app_data_dir(values)
    cache_writeable = False
    cache_error = None
    probe_path: Path | None = None
    try:
        state_root.mkdir(parents=True, exist_ok=True)
        probe_path = state_root / f"self-check-{uuid.uuid4().hex}.tmp"
        with probe_path.open("xb") as probe:
            probe.write(b"ok")
        cache_writeable = probe_path.read_bytes() == b"ok"
    except OSError as exc:
        cache_error = str(exc)
    finally:
        if probe_path is not None:
            try:
                probe_path.unlink()
            except OSError:
                pass

    webview_candidates = _webview2_candidates(values)
    webview2_runtime = any(
        candidate.is_dir() and any(candidate.iterdir()) for candidate in webview_candidates
    )
    imports = {
        "databento": importlib.util.find_spec("databento") is not None,
        "webview": importlib.util.find_spec("webview") is not None,
    }
    locator_path = default_credential_locator_path()
    locator_present = bool(locator_path is not None and locator_path.is_file())
    repository_api_env_path = default_repository_package_api_env_path()
    repository_api_env_present = bool(
        repository_api_env_path is not None and repository_api_env_path.is_file()
    )
    environment_key_present = API_KEY_ENV in values
    credential_source_present = (
        locator_present or repository_api_env_present or environment_key_present
    )
    credential_source = None
    if locator_present:
        credential_source = "installed credential locator (existence only)"
    elif repository_api_env_present:
        credential_source = "repository package api.env (existence only)"
    elif environment_key_present:
        credential_source = "environment variable (existence only)"
    core_pass = (
        len(markets) == 41
        and alpha_tiers.available
        and all(asset_results.values())
        and asset_launch_target_local
        and all(imports.values())
        and cache_writeable
    )
    return {
        "status": "PASS" if core_pass and webview2_runtime else "FAIL",
        "provider_connection_opened": False,
        "market_count": len(markets),
        "alpha_tier_grouping_valid": alpha_tiers.available,
        "assets": asset_results,
        "asset_launch_target_local": asset_launch_target_local,
        "imports": imports,
        "cache_writeable": cache_writeable,
        "cache_error": cache_error,
        "webview2_runtime": webview2_runtime,
        "credential_check_mode": "existence_only",
        "credential_source_present": credential_source_present,
        "api_key_configured": None,
        "credential_source": credential_source,
        "credential_locator_present": locator_present,
        "credential_locator_valid": None,
        "credential_error": None,
    }


class CockpitController:
    def __init__(self, engine: CockpitEngine, *, state_path: Path) -> None:
        self.engine = engine
        self.state_path = state_path
        self.window: object | None = None
        self._ready = False
        self._started = False
        self._stop_started = False
        self._fullscreen = False
        self._stop_complete = threading.Event()
        self._pending: list[dict[str, Any]] = []
        self._lock = threading.RLock()

    def attach_window(self, window: object) -> None:
        self.window = window

    def bootstrap(self) -> dict[str, Any]:
        with self._lock:
            self._ready = True
            bootstrap = self.engine.bootstrap_event()
            payload = bootstrap.get("payload")
            if isinstance(payload, dict):
                persisted = load_state(self.state_path)
                preferences = sanitize_ui_preferences(persisted.get("ui_preferences"))
                visual_mode = str(
                    preferences.get("visual_update_mode", DEFAULT_VISUAL_UPDATE_MODE)
                )
                self.engine.set_visual_update_mode(visual_mode)
                payload["ui_preferences"] = preferences
            if not self._started:
                self._started = True
                threading.Thread(
                    target=self._start_after_bootstrap,
                    name="cockpit-engine-start",
                    daemon=True,
                ).start()
        return bootstrap

    def _start_after_bootstrap(self) -> None:
        time.sleep(0.05)
        with self._lock:
            if self._stop_started:
                return
        self.engine.start(self.publish)

    def publish(self, message: dict[str, Any]) -> None:
        with self._lock:
            if message.get("type") == "bar_update":
                payload = message.get("payload")
                if isinstance(payload, Mapping):
                    identity = (
                        payload.get("market"),
                        payload.get("timeframe"),
                        payload.get("generation"),
                    )
                    self._pending = [
                        pending
                        for pending in self._pending
                        if not (
                            pending.get("type") == "bar_update"
                            and isinstance(pending.get("payload"), Mapping)
                            and (
                                pending["payload"].get("market"),
                                pending["payload"].get("timeframe"),
                                pending["payload"].get("generation"),
                            )
                            == identity
                        )
                    ]
            self._pending.append(message)
            self._pending = self._pending[-500:]

    def poll_events(self, limit: int = 100) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 200))
        with self._lock:
            events = self._pending[:bounded_limit]
            del self._pending[:bounded_limit]
        return events

    def select_market(self, market: str) -> dict[str, Any]:
        accepted = self.engine.select_market(str(market).strip().upper())
        if accepted:
            state = load_state(self.state_path)
            state["market"] = str(market).strip().upper()
            save_state(self.state_path, state)
        return {
            "ok": accepted,
            "generation": int(getattr(self.engine, "generation", 0)),
        }

    def select_timeframe(self, timeframe: str) -> dict[str, Any]:
        accepted = self.engine.select_timeframe(str(timeframe).strip().lower())
        if accepted:
            state = load_state(self.state_path)
            state["timeframe"] = str(timeframe).strip().lower()
            save_state(self.state_path, state)
        return {"ok": accepted}

    def retry_history(self) -> dict[str, Any]:
        accepted = self.engine.retry_history()
        return {
            "ok": accepted,
            "generation": int(getattr(self.engine, "generation", 0)),
        }

    def confirm_history_cache(self, plan_id: str) -> dict[str, bool]:
        return {"ok": self.engine.confirm_history_cache(str(plan_id))}

    def set_history_cache_paused(self, paused: object) -> dict[str, bool]:
        if not isinstance(paused, bool):
            return {"ok": False}
        return {"ok": self.engine.set_history_cache_paused(paused)}

    def retry_history_cache_estimate(self) -> dict[str, bool]:
        return {"ok": self.engine.retry_history_cache_estimate()}

    def set_ui_preferences(self, preferences: object) -> dict[str, Any]:
        sanitized = sanitize_ui_preferences(preferences)
        state = load_state(self.state_path)
        current = sanitize_ui_preferences(state.get("ui_preferences"))
        current.update(sanitized)
        state["ui_preferences"] = current
        save_state(self.state_path, state)
        visual_mode = sanitized.get("visual_update_mode")
        if isinstance(visual_mode, str):
            self.engine.set_visual_update_mode(visual_mode)
        return {"ok": True, "ui_preferences": current}

    def set_visual_update_active(self, active: object) -> dict[str, Any]:
        if not isinstance(active, bool):
            return {"ok": False, "effective_hz": None}
        effective_hz = self.engine.set_visual_update_active(active)
        return {"ok": True, "effective_hz": effective_hz}

    def toggle_fullscreen(self) -> dict[str, bool]:
        with self._lock:
            window = self.window
            current = self._fullscreen
        toggle = getattr(window, "toggle_fullscreen", None)
        if not callable(toggle):
            return {"ok": False, "fullscreen": current}
        try:
            toggle()
        except Exception:
            return {"ok": False, "fullscreen": current}
        with self._lock:
            self._fullscreen = not current
            return {"ok": True, "fullscreen": self._fullscreen}

    def request_stop(self, *_args: object) -> None:
        with self._lock:
            if self._stop_started:
                return
            self._stop_started = True
        threading.Thread(
            target=self._stop_engine,
            name="cockpit-engine-stop",
            daemon=True,
        ).start()

    def _stop_engine(self) -> None:
        try:
            self.engine.stop()
        finally:
            self._stop_complete.set()

    def stop(self, *_args: object) -> None:
        self.request_stop()
        self._stop_complete.wait(timeout=5.0)


class CockpitApi:
    """Small pywebview API exposed to the bundled frontend."""

    def __init__(self, controller: CockpitController) -> None:
        # pywebview recursively exposes public attributes on js_api objects.
        # Keep the controller private so native Window/WebView2 objects are not
        # traversed while the JavaScript bridge is being constructed.
        self._controller = controller

    def bootstrap(self) -> dict[str, Any]:
        return self._controller.bootstrap()

    def poll_events(self, limit: int = 100) -> list[dict[str, Any]]:
        return self._controller.poll_events(limit)

    def select_market(self, market: str) -> dict[str, Any]:
        return self._controller.select_market(market)

    def select_timeframe(self, timeframe: str) -> dict[str, Any]:
        return self._controller.select_timeframe(timeframe)

    def retry_history(self) -> dict[str, Any]:
        return self._controller.retry_history()

    def confirm_history_cache(self, plan_id: str) -> dict[str, bool]:
        return self._controller.confirm_history_cache(plan_id)

    def set_history_cache_paused(self, paused: object) -> dict[str, bool]:
        return self._controller.set_history_cache_paused(paused)

    def retry_history_cache_estimate(self) -> dict[str, bool]:
        return self._controller.retry_history_cache_estimate()

    def set_ui_preferences(self, preferences: object) -> dict[str, Any]:
        return self._controller.set_ui_preferences(preferences)

    def set_visual_update_active(self, active: object) -> dict[str, Any]:
        return self._controller.set_visual_update_active(active)

    def toggle_fullscreen(self) -> dict[str, bool]:
        return self._controller.toggle_fullscreen()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Observation-only Windows futures chart cockpit."
    )
    parser.add_argument("--market", type=normalize_market, default=None)
    parser.add_argument("--timeframe", type=normalize_timeframe, default=None)
    parser.add_argument(
        "--demo", action="store_true", help="Run deterministic data without a provider connection."
    )
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="Verify local assets and runtime dependencies without opening the app.",
    )
    parser.add_argument(
        "--prepare-live-smoke",
        action="store_true",
        help="Describe the bounded provider smoke; this command never runs it.",
    )
    return parser


def run_desktop(*, engine: CockpitEngine, state_path: Path, demo: bool) -> int:
    try:
        import webview
    except ModuleNotFoundError:
        print("Missing pywebview; install the cockpit runtime dependencies.", file=sys.stderr)
        return 2

    asset_target = desktop_asset_target(demo=demo)
    controller = CockpitController(engine, state_path=state_path)
    api = CockpitApi(controller)
    window = webview.create_window(
        APP_NAME,
        url=asset_target,
        js_api=api,
        width=1366,
        height=768,
        min_size=(960, 640),
        background_color="#09111d",
    )
    controller.attach_window(window)
    window.events.closed += controller.request_stop
    try:
        webview.start(gui="edgechromium", debug=False, http_server=True)
    finally:
        controller.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    selected_modes = sum((args.self_check, args.demo, args.prepare_live_smoke))
    if selected_modes > 1:
        raise SystemExit("--self-check, --demo, and --prepare-live-smoke are mutually exclusive")
    if args.self_check:
        result = self_check()
        # PyInstaller's windowed bootloader sets stdout to None. The exit code
        # remains the packaged self-check contract in that environment.
        if sys.stdout is not None:
            print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["status"] == "PASS" else 1
    if args.prepare_live_smoke:
        print(
            json.dumps(
                confirmation_required(
                    "cockpit live smoke",
                    scope={"duration_seconds": "120", "provider_requests": "bounded"},
                    outputs=("reports/live_cockpit/bounded_live_smoke_result.json",),
                    preservation="Leave the installed cockpit and its shortcuts unchanged.",
                ),
                sort_keys=True,
            )
        )
        return 0
    if not args.demo:
        raise SystemExit(
            "BLOCKED: live cockpit launch requires an approved Codex high-risk task; use --prepare-live-smoke first"
        )

    root = app_data_dir()
    state_path = root / STATE_FILENAME
    persisted = load_state(state_path)
    market = args.market or str(persisted.get("market", "ES")).strip().upper()
    timeframe = args.timeframe or str(persisted.get("timeframe", "1m")).strip().lower()
    symbols = {info.symbol for info in chart_market_universe()}
    if market not in symbols:
        market = "ES"
    if timeframe not in SUPPORTED_CHART_TIMEFRAMES:
        timeframe = "1m"
    save_state(state_path, {**persisted, "market": market, "timeframe": timeframe})

    engine: CockpitEngine
    if args.demo:
        engine = DemoCockpitEngine(market=market, timeframe=timeframe)
    else:
        engine = LiveCockpitEngine(
            cache_path=root / CACHE_FILENAME,
            market=market,
            timeframe=timeframe,
        )
    return run_desktop(engine=engine, state_path=state_path, demo=args.demo)
