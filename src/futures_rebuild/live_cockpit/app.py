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
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping

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
    DEFAULT_CHART_RANGE,
    DEFAULT_VISUAL_UPDATE_MODE,
    VISUAL_UPDATE_HZ,
    CockpitEngine,
    DemoCockpitEngine,
    LiveCockpitEngine,
    QUICK_CHART_MARKETS,
    normalize_chart_range,
)
from .market_groups import load_alpha_tier_grouping
from .offline_network import DemoLoopbackDenyProxy
from .single_instance import SingleInstance


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
WEBVIEW2_BROWSER_ARGUMENTS_ENV = "WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS"
DEMO_WEBVIEW2_BACKGROUND_ARGUMENT = "--disable-background-networking"
HISTORY_POLICY_VERSION = 1
HISTORY_POLICY_MODES = frozenset({"UNDECIDED", "MANUAL", "AUTO"})
HISTORY_AUTO_INTERVAL = timedelta(hours=24)
HISTORY_AUTO_MAX_ESTIMATED_COST_USD = Decimal("0.05")
HISTORY_AUTO_OUTCOMES = frozenset(
    {"STARTED", "COMPLETE", "ERROR", "PARTIAL", "REJECTED", "INTERRUPTED"}
)


@contextmanager
def demo_webview2_offline_environment(*, demo: bool) -> Iterator[None]:
    """Route nonlocal WebView2 demo requests into a local rejecting proxy."""

    if not demo:
        yield
        return
    with DemoLoopbackDenyProxy() as deny_proxy:
        was_present = WEBVIEW2_BROWSER_ARGUMENTS_ENV in os.environ
        prior = os.environ.get(WEBVIEW2_BROWSER_ARGUMENTS_ENV)
        os.environ[WEBVIEW2_BROWSER_ARGUMENTS_ENV] = (
            f"{DEMO_WEBVIEW2_BACKGROUND_ARGUMENT} "
            f"--proxy-server={deny_proxy.endpoint}"
        )
        try:
            yield
        finally:
            if was_present and prior is not None:
                os.environ[WEBVIEW2_BROWSER_ARGUMENTS_ENV] = prior
            else:
                os.environ.pop(WEBVIEW2_BROWSER_ARGUMENTS_ENV, None)


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
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(dict(state), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def mutate_state(
    path: Path,
    lock: threading.RLock,
    mutation: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    """Serialize one read-modify-write state update within the cockpit process."""

    with lock:
        state = load_state(path)
        mutation(state)
        save_state(path, state)
        return state


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc_text(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("history policy timestamp is malformed")
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("history policy timestamp lacks a timezone")
    return parsed.astimezone(timezone.utc)


def default_history_update_policy() -> dict[str, Any]:
    return {
        "policy_version": HISTORY_POLICY_VERSION,
        "mode": "UNDECIDED",
        "last_auto_attempt_at": None,
        "last_auto_estimate_usd": None,
        "last_auto_outcome": None,
        "last_auto_plan_fingerprint": None,
        "auto_blocked": False,
        "block_reason": None,
        "last_result_at": None,
    }


def _malformed_history_update_policy() -> dict[str, Any]:
    policy = default_history_update_policy()
    policy.update(
        {
            "mode": "MANUAL",
            "auto_blocked": True,
            "block_reason": "MALFORMED_POLICY",
        }
    )
    return policy


def sanitize_history_update_policy(value: object) -> dict[str, Any]:
    if value is None:
        return default_history_update_policy()
    if not isinstance(value, Mapping):
        return _malformed_history_update_policy()
    if value.get("policy_version") != HISTORY_POLICY_VERSION:
        return default_history_update_policy()
    try:
        mode = str(value.get("mode") or "").upper()
        if mode not in HISTORY_POLICY_MODES:
            raise ValueError("invalid history policy mode")
        last_attempt = _parse_utc_text(value.get("last_auto_attempt_at"))
        last_result = _parse_utc_text(value.get("last_result_at"))
        estimate_value = value.get("last_auto_estimate_usd")
        estimate: str | None = None
        if estimate_value is not None:
            parsed_estimate = Decimal(str(estimate_value))
            if not parsed_estimate.is_finite() or parsed_estimate < 0:
                raise ValueError("invalid history estimate")
            estimate = format(parsed_estimate, "f")
        outcome = value.get("last_auto_outcome")
        if outcome is not None and outcome not in HISTORY_AUTO_OUTCOMES:
            raise ValueError("invalid history outcome")
        fingerprint = value.get("last_auto_plan_fingerprint")
        if fingerprint is not None and (
            not isinstance(fingerprint, str)
            or len(fingerprint) != 64
            or any(character not in "0123456789abcdef" for character in fingerprint)
        ):
            raise ValueError("invalid history plan fingerprint")
        blocked = value.get("auto_blocked")
        if not isinstance(blocked, bool):
            raise ValueError("invalid history block flag")
        reason = value.get("block_reason")
        if reason is not None and (
            not isinstance(reason, str)
            or not reason
            or len(reason) > 64
            or any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for character in reason)
        ):
            raise ValueError("invalid history block reason")
    except (InvalidOperation, ValueError, TypeError, OverflowError):
        return _malformed_history_update_policy()
    return {
        "policy_version": HISTORY_POLICY_VERSION,
        "mode": mode,
        "last_auto_attempt_at": _utc_text(last_attempt) if last_attempt else None,
        "last_auto_estimate_usd": estimate,
        "last_auto_outcome": outcome,
        "last_auto_plan_fingerprint": fingerprint,
        "auto_blocked": blocked,
        "block_reason": reason,
        "last_result_at": _utc_text(last_result) if last_result else None,
    }


def _public_history_policy(policy: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **dict(policy),
        "automatic_limit_usd": format(HISTORY_AUTO_MAX_ESTIMATED_COST_USD, "f"),
        "automatic_interval_hours": int(HISTORY_AUTO_INTERVAL.total_seconds() // 3600),
    }


def _automatic_history_eligibility(
    policy: Mapping[str, Any],
    history_status: Mapping[str, Any],
    *,
    now: datetime,
) -> tuple[bool, str | None, Decimal | None]:
    if policy.get("mode") != "AUTO":
        return False, "AUTOMATIC_OFF", None
    if (
        str(history_status.get("range_key") or DEFAULT_CHART_RANGE).upper()
        != DEFAULT_CHART_RANGE
    ):
        return False, "EXTENDED_RANGE_REQUIRES_REVIEW", None
    if policy.get("auto_blocked") is True:
        return False, str(policy.get("block_reason") or "AUTOMATIC_BLOCKED"), None
    last_attempt = _parse_utc_text(policy.get("last_auto_attempt_at"))
    if last_attempt is not None and now - last_attempt < HISTORY_AUTO_INTERVAL:
        return False, "RECENT_ATTEMPT", None
    if str(history_status.get("state") or "").upper() != "CONFIRMATION_REQUIRED":
        return False, "PLAN_NOT_READY", None
    try:
        estimate = Decimal(str(history_status.get("estimated_cost_usd")))
    except (InvalidOperation, ValueError, TypeError):
        return False, "ESTIMATE_INVALID", None
    if not estimate.is_finite() or estimate < 0:
        return False, "ESTIMATE_INVALID", None
    if estimate > HISTORY_AUTO_MAX_ESTIMATED_COST_USD:
        return False, "ABOVE_CAP", estimate
    plan_id = history_status.get("plan_id")
    fingerprint = history_status.get("plan_fingerprint")
    expires_at = history_status.get("estimate_expires_at")
    if (
        not isinstance(plan_id, str)
        or not plan_id
        or not isinstance(fingerprint, str)
        or len(fingerprint) != 64
        or any(character not in "0123456789abcdef" for character in fingerprint)
        or not isinstance(expires_at, int)
        or expires_at <= int(now.timestamp())
    ):
        return False, "PLAN_INVALID", estimate
    return True, None, estimate


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
        "observation_only": True,
    }


class CockpitController:
    def __init__(
        self,
        engine: CockpitEngine,
        *,
        state_path: Path,
    ) -> None:
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
        self._state_lock = threading.RLock()
        self._last_history_cache_status: dict[str, Any] = {}
        self._history_planning_origin: str | None = None
        self._active_history_origin: str | None = None
        self._active_history_plan_id: str | None = None
        self._recover_interrupted_history_update()

    def _mutate_state(self, mutation: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        return mutate_state(self.state_path, self._state_lock, mutation)

    def _history_policy(self) -> dict[str, Any]:
        with self._state_lock:
            state = load_state(self.state_path)
        return sanitize_history_update_policy(state.get("history_update_policy"))

    def _write_history_policy(self, policy: Mapping[str, Any]) -> dict[str, Any]:
        sanitized = sanitize_history_update_policy(policy)

        def update(state: dict[str, Any]) -> None:
            state["history_update_policy"] = sanitized

        self._mutate_state(update)
        return sanitized

    def _recover_interrupted_history_update(self) -> None:
        policy = self._history_policy()
        if policy.get("last_auto_outcome") != "STARTED":
            return
        policy.update(
            {
                "last_auto_outcome": "INTERRUPTED",
                "auto_blocked": True,
                "block_reason": "INTERRUPTED",
                "last_result_at": _utc_text(_utc_now()),
            }
        )
        self._write_history_policy(policy)

    def _decorate_history_status(
        self,
        payload: Mapping[str, Any],
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        policy = self._history_policy()
        evaluated_at = now or _utc_now()
        eligible, reason, _estimate = _automatic_history_eligibility(
            policy,
            payload,
            now=evaluated_at,
        )
        last_attempt = _parse_utc_text(policy.get("last_auto_attempt_at"))
        return {
            **dict(payload),
            "policy_mode": policy["mode"],
            "automatic_eligible": eligible,
            "automatic_blocked": policy["auto_blocked"],
            "automatic_reason": reason,
            "automatic_limit_usd": float(HISTORY_AUTO_MAX_ESTIMATED_COST_USD),
            "automatic_interval_hours": int(
                HISTORY_AUTO_INTERVAL.total_seconds() // 3600
            ),
            "last_auto_attempt_at": (
                int(last_attempt.timestamp()) if last_attempt is not None else None
            ),
            "last_auto_estimate_usd": (
                float(policy["last_auto_estimate_usd"])
                if policy.get("last_auto_estimate_usd") is not None
                else None
            ),
            "last_auto_outcome": policy.get("last_auto_outcome"),
            "update_origin": self._active_history_origin,
        }

    def _record_history_terminal(self, payload: Mapping[str, Any]) -> None:
        terminal_state = str(payload.get("state") or "").upper()
        if terminal_state not in {"COMPLETE", "PARTIAL", "ERROR"}:
            return
        origin = self._active_history_origin
        policy = self._history_policy()
        now_text = _utc_text(_utc_now())
        reviewed_retry_completed = self._history_planning_origin == "AUTO_REVIEWED"
        if terminal_state == "COMPLETE" and (
            origin in {"AUTO", "MANUAL"} or reviewed_retry_completed
        ):
            if origin == "AUTO":
                policy["last_auto_outcome"] = "COMPLETE"
            policy.update(
                {
                    "auto_blocked": False,
                    "block_reason": None,
                    "last_result_at": now_text,
                }
            )
        elif terminal_state != "COMPLETE" and (origin == "AUTO" or (
            origin is None
            and self._history_planning_origin in {"AUTO", "AUTO_REVIEWED"}
            and policy.get("mode") == "AUTO"
        )):
            user_disabled = policy.get("block_reason") == "USER_DISABLED"
            if policy.get("last_auto_attempt_at") is None:
                policy["last_auto_attempt_at"] = now_text
                policy["last_auto_estimate_usd"] = None
                policy["last_auto_plan_fingerprint"] = None
            policy.update(
                {
                    "last_auto_outcome": (
                        "INTERRUPTED" if user_disabled else terminal_state
                    ),
                    "auto_blocked": True,
                    "block_reason": (
                        "USER_DISABLED"
                        if user_disabled
                        else "AUTO_PARTIAL"
                        if terminal_state == "PARTIAL"
                        else f"AUTO_{str(payload.get('failure_category') or 'ERROR').upper()}"
                    ),
                    "last_result_at": now_text,
                }
            )
        if origin in {"AUTO", "MANUAL"} or reviewed_retry_completed or (
            terminal_state != "COMPLETE"
            and self._history_planning_origin in {"AUTO", "AUTO_REVIEWED"}
        ):
            self._write_history_policy(policy)
        self._active_history_origin = None
        self._active_history_plan_id = None
        self._history_planning_origin = None

    def _reserve_automatic_confirmation(
        self,
        payload: Mapping[str, Any],
    ) -> str | None:
        now = _utc_now()
        eligible, _reason, _estimate = _automatic_history_eligibility(
            self._history_policy(),
            payload,
            now=now,
        )
        if not eligible:
            return None
        reservation: dict[str, str] = {}

        def reserve(state: dict[str, Any]) -> None:
            policy = sanitize_history_update_policy(state.get("history_update_policy"))
            eligible, _reason, estimate = _automatic_history_eligibility(
                policy,
                payload,
                now=now,
            )
            if not eligible or estimate is None:
                return
            plan_id = str(payload["plan_id"])
            fingerprint = str(payload["plan_fingerprint"])
            policy.update(
                {
                    "last_auto_attempt_at": _utc_text(now),
                    "last_auto_estimate_usd": format(estimate, "f"),
                    "last_auto_outcome": "STARTED",
                    "last_auto_plan_fingerprint": fingerprint,
                    "auto_blocked": False,
                    "block_reason": None,
                    "last_result_at": None,
                }
            )
            state["history_update_policy"] = policy
            reservation.update({"plan_id": plan_id, "fingerprint": fingerprint})

        self._mutate_state(reserve)
        plan_id = reservation.get("plan_id")
        if plan_id is None:
            return None
        self._active_history_origin = "AUTO"
        self._active_history_plan_id = plan_id
        self._history_planning_origin = None
        return plan_id

    def attach_window(self, window: object) -> None:
        self.window = window

    def bootstrap(self) -> dict[str, Any]:
        with self._lock:
            self._ready = True
            bootstrap = self.engine.bootstrap_event()
            payload = bootstrap.get("payload")
            if isinstance(payload, dict):
                with self._state_lock:
                    persisted = load_state(self.state_path)
                preferences = sanitize_ui_preferences(persisted.get("ui_preferences"))
                visual_mode = str(
                    preferences.get("visual_update_mode", DEFAULT_VISUAL_UPDATE_MODE)
                )
                self.engine.set_visual_update_mode(visual_mode)
                payload["ui_preferences"] = preferences
                history_capability = payload.get("history_cache_capability")
                if (
                    isinstance(history_capability, Mapping)
                    and history_capability.get("enabled") is True
                ):
                    payload["history_update_policy"] = _public_history_policy(
                        sanitize_history_update_policy(
                            persisted.get("history_update_policy")
                        )
                    )
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
        auto_confirm_plan: str | None = None
        if message.get("type") == "history_cache_status":
            raw_payload = message.get("payload")
            if isinstance(raw_payload, Mapping):
                payload = dict(raw_payload)
                state_name = str(payload.get("state") or "").upper()
                if (
                    state_name == "CHECKING"
                    and str(payload.get("range_key") or DEFAULT_CHART_RANGE).upper()
                    == DEFAULT_CHART_RANGE
                    and self._history_planning_origin is None
                ):
                    if self._history_policy().get("mode") == "AUTO":
                        self._history_planning_origin = "AUTO"
                self._record_history_terminal(payload)
                self._last_history_cache_status = dict(payload)
                auto_confirm_plan = self._reserve_automatic_confirmation(payload)
                message = {**message, "payload": self._decorate_history_status(payload)}
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
        if auto_confirm_plan is not None:
            if not self.engine.confirm_history_cache(auto_confirm_plan):
                policy = self._history_policy()
                policy.update(
                    {
                        "last_auto_outcome": "REJECTED",
                        "auto_blocked": True,
                        "block_reason": "CONFIRMATION_REJECTED",
                        "last_result_at": _utc_text(_utc_now()),
                    }
                )
                self._write_history_policy(policy)
                self._active_history_origin = None
                self._active_history_plan_id = None

    def poll_events(self, limit: int = 100) -> list[dict[str, Any]]:
        bounded_limit = max(1, min(int(limit), 200))
        with self._lock:
            events = self._pending[:bounded_limit]
            del self._pending[:bounded_limit]
        return events

    def select_market(self, market: str) -> dict[str, Any]:
        accepted = self.engine.select_market(str(market).strip().upper())
        if accepted:
            selected = str(market).strip().upper()
            self._mutate_state(lambda state: state.__setitem__("market", selected))
        return {
            "ok": accepted,
            "generation": int(getattr(self.engine, "generation", 0)),
        }

    def select_timeframe(self, timeframe: str) -> dict[str, Any]:
        accepted = self.engine.select_timeframe(str(timeframe).strip().lower())
        if accepted:
            selected = str(timeframe).strip().lower()
            self._mutate_state(lambda state: state.__setitem__("timeframe", selected))
        return {"ok": accepted}

    def select_chart_range(self, chart_range: str) -> dict[str, Any]:
        normalized = str(chart_range).strip().upper()
        accepted = self.engine.select_chart_range(normalized)
        if accepted:
            self._mutate_state(
                lambda state: state.__setitem__("chart_range", normalized)
            )
        return {"ok": accepted}

    def retry_history(self) -> dict[str, Any]:
        accepted = self.engine.retry_history()
        return {
            "ok": accepted,
            "generation": int(getattr(self.engine, "generation", 0)),
        }

    def confirm_history_cache(self, plan_id: str) -> dict[str, bool]:
        normalized = str(plan_id)
        self._active_history_origin = "MANUAL"
        self._active_history_plan_id = normalized
        accepted = self.engine.confirm_history_cache(normalized)
        if not accepted:
            self._active_history_origin = None
            self._active_history_plan_id = None
        return {"ok": accepted}

    def set_history_cache_paused(self, paused: object) -> dict[str, bool]:
        if not isinstance(paused, bool):
            return {"ok": False}
        return {"ok": self.engine.set_history_cache_paused(paused)}

    def retry_history_cache_estimate(self) -> dict[str, bool]:
        self._history_planning_origin = "MANUAL"
        accepted = self.engine.retry_history_cache_estimate()
        if not accepted:
            self._history_planning_origin = None
        return {"ok": accepted}

    def set_history_update_mode(self, mode: object) -> dict[str, Any]:
        normalized = str(mode).strip().upper()
        if normalized not in {"AUTO", "MANUAL"}:
            return {"ok": False, "error": "INVALID_MODE"}
        policy = self._history_policy()
        if normalized == "AUTO" and policy.get("auto_blocked"):
            return {
                "ok": False,
                "error": "REVIEW_REQUIRED",
                "history_update_policy": _public_history_policy(policy),
            }
        policy.update({"mode": normalized})
        if normalized == "MANUAL" and self._active_history_origin == "AUTO":
            cancel = getattr(
                self.engine, "cancel_history_cache_after_current_request", None
            )
            if callable(cancel):
                cancel()
            else:
                self.engine.set_history_cache_paused(True)
            policy.update(
                {
                    "auto_blocked": True,
                    "block_reason": "USER_DISABLED",
                    "last_auto_outcome": "INTERRUPTED",
                    "last_result_at": _utc_text(_utc_now()),
                }
            )
        self._write_history_policy(policy)
        accepted = True
        if normalized == "AUTO":
            self._history_planning_origin = "AUTO"
            plan_id = self._reserve_automatic_confirmation(
                self._last_history_cache_status
            )
            if plan_id is not None:
                accepted = self.engine.confirm_history_cache(plan_id)
                if not accepted:
                    policy = self._history_policy()
                    policy.update(
                        {
                            "last_auto_outcome": "REJECTED",
                            "auto_blocked": True,
                            "block_reason": "CONFIRMATION_REJECTED",
                            "last_result_at": _utc_text(_utc_now()),
                        }
                    )
                    self._write_history_policy(policy)
                    self._active_history_origin = None
                    self._active_history_plan_id = None
            elif str(self._last_history_cache_status.get("state") or "").upper() in {
                "ERROR",
                "PARTIAL",
            }:
                accepted = self.engine.retry_history_cache_estimate()
                if not accepted:
                    self._history_planning_origin = None
        return {
            "ok": accepted,
            "history_update_policy": _public_history_policy(self._history_policy()),
        }

    def retry_automatic_history(self) -> dict[str, Any]:
        policy = self._history_policy()
        now = _utc_now()
        last_attempt = _parse_utc_text(policy.get("last_auto_attempt_at"))
        if last_attempt is not None and now - last_attempt < HISTORY_AUTO_INTERVAL:
            return {
                "ok": False,
                "error": "RECENT_ATTEMPT",
                "history_update_policy": _public_history_policy(policy),
            }
        policy.update(
            {
                "mode": "AUTO",
                "auto_blocked": False,
                "block_reason": None,
            }
        )
        self._write_history_policy(policy)
        self._history_planning_origin = "AUTO_REVIEWED"
        accepted = self.engine.retry_history_cache_estimate()
        if not accepted:
            self._history_planning_origin = None
            policy = self._history_policy()
            policy.update(
                {
                    "auto_blocked": True,
                    "block_reason": "AUTO_RETRY_NOT_STARTED",
                    "last_auto_outcome": "REJECTED",
                    "last_result_at": _utc_text(now),
                }
            )
            self._write_history_policy(policy)
        return {
            "ok": accepted,
            "history_update_policy": _public_history_policy(self._history_policy()),
        }

    def set_ui_preferences(self, preferences: object) -> dict[str, Any]:
        sanitized = sanitize_ui_preferences(preferences)
        current: dict[str, Any] = {}

        def update(state: dict[str, Any]) -> None:
            nonlocal current
            current = sanitize_ui_preferences(state.get("ui_preferences"))
            current.update(sanitized)
            state["ui_preferences"] = current

        self._mutate_state(update)
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

    def select_chart_range(self, chart_range: str) -> dict[str, Any]:
        return self._controller.select_chart_range(chart_range)

    def retry_history(self) -> dict[str, Any]:
        return self._controller.retry_history()

    def confirm_history_cache(self, plan_id: str) -> dict[str, bool]:
        return self._controller.confirm_history_cache(plan_id)

    def set_history_cache_paused(self, paused: object) -> dict[str, bool]:
        return self._controller.set_history_cache_paused(paused)

    def retry_history_cache_estimate(self) -> dict[str, bool]:
        return self._controller.retry_history_cache_estimate()

    def set_history_update_mode(self, mode: object) -> dict[str, Any]:
        return self._controller.set_history_update_mode(mode)

    def retry_automatic_history(self) -> dict[str, Any]:
        return self._controller.retry_automatic_history()

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
    parser.add_argument(
        "--run-approved-live-smoke",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--smoke-plan", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--smoke-approval", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--smoke-credential-locator", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--smoke-result-output", type=Path, help=argparse.SUPPRESS)
    return parser


def run_desktop(*, engine: CockpitEngine, state_path: Path, demo: bool) -> int:
    try:
        import webview
    except ModuleNotFoundError:
        print("Missing pywebview; install the cockpit runtime dependencies.", file=sys.stderr)
        return 2

    instance = SingleInstance(state_path.with_name("cockpit-instance.lock"))
    if not instance.acquire():
        print("Futures Live Cockpit is already running.", file=sys.stderr)
        return 3
    controller: CockpitController | None = None
    try:
        asset_target = desktop_asset_target(demo=demo)
        controller = CockpitController(
            engine,
            state_path=state_path,
        )
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
        with demo_webview2_offline_environment(demo=demo):
            webview.start(gui="edgechromium", debug=False, http_server=True)
    finally:
        if controller is not None:
            controller.stop()
        instance.release()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    selected_modes = sum(
        (
            args.self_check,
            args.demo,
            args.prepare_live_smoke,
            args.run_approved_live_smoke,
        )
    )
    if selected_modes > 1:
        raise SystemExit(
            "--self-check, --demo, --prepare-live-smoke, and the approved smoke task are mutually exclusive"
        )
    smoke_paths = (
        args.smoke_plan,
        args.smoke_approval,
        args.smoke_credential_locator,
        args.smoke_result_output,
    )
    if args.run_approved_live_smoke:
        if any(path is None for path in smoke_paths):
            raise SystemExit(
                "approved smoke task requires plan, approval, credential locator, and result paths"
            )
        if args.market is not None or args.timeframe is not None:
            raise SystemExit("approved smoke task does not accept market or timeframe overrides")
        from .smoke import execute_approved_smoke

        return execute_approved_smoke(
            plan_path=args.smoke_plan,
            approval_path=args.smoke_approval,
            credential_locator=args.smoke_credential_locator,
            result_output=args.smoke_result_output,
        )
    if any(path is not None for path in smoke_paths):
        raise SystemExit("smoke task paths require the approved smoke task mode")
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
    root = app_data_dir()
    if args.demo:
        root = root / "demo"
    state_path = root / STATE_FILENAME
    persisted = load_state(state_path)
    market = args.market or str(persisted.get("market", "ES")).strip().upper()
    timeframe = args.timeframe or str(persisted.get("timeframe", "1m")).strip().lower()
    try:
        chart_range = normalize_chart_range(
            persisted.get("chart_range", DEFAULT_CHART_RANGE)
        )
    except ValueError:
        chart_range = DEFAULT_CHART_RANGE
    symbols = {info.symbol for info in chart_market_universe()}
    if market not in symbols:
        market = "ES"
    if timeframe not in SUPPORTED_CHART_TIMEFRAMES:
        timeframe = "1m"
    state_lock = threading.RLock()

    def persist_selection(state: dict[str, Any]) -> None:
        if market not in QUICK_CHART_MARKETS:
            chart_range_value = DEFAULT_CHART_RANGE
        else:
            chart_range_value = chart_range
        state.update(
            {
                "market": market,
                "timeframe": timeframe,
                "chart_range": chart_range_value,
            }
        )

    mutate_state(state_path, state_lock, persist_selection)

    engine: CockpitEngine
    if args.demo:
        engine = DemoCockpitEngine(
            market=market, timeframe=timeframe, chart_range=chart_range
        )
    else:
        engine = LiveCockpitEngine(
            cache_path=root / CACHE_FILENAME,
            market=market,
            timeframe=timeframe,
            chart_range=chart_range,
        )
    return run_desktop(engine=engine, state_path=state_path, demo=args.demo)
