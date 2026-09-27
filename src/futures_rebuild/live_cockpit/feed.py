#!/usr/bin/env python3
"""Optional Databento live trades chart for research observation only."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import multiprocessing
import os
import queue
import re
import signal
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from datetime import date, datetime, time as datetime_time, timedelta, timezone, tzinfo
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Sequence, TextIO, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

ROOT = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parents[3]))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from .databento_auth import (  # noqa: E402
    load_databento_api_key_from_file,
    normalize_api_key,
)
from .observation_status import OperatorStatusState, print_operator_status  # noqa: E402
from ..time_policy import PROJECT_TIMEZONE_NAME  # noqa: E402


DEFAULT_DATASET = "GLBX.MDP3"
DEFAULT_SCHEMA = "trades"
DEFAULT_HISTORICAL_SCHEMA = "ohlcv-1m"
DEFAULT_SYMBOLS: str | None = None
DEFAULT_STYPE_IN = "instrument_id"
DEFAULT_MARKET_STYPE_IN = "continuous"
DEFAULT_CONTINUOUS_SUFFIX = ".v.0"
DEFAULT_LOOKBACK_HOURS = 168.0
DEFAULT_MAX_RECORDS = 0
DEFAULT_TIMEOUT_SECONDS: float | None = None
DEFAULT_NO_DATA_WARNING_SECONDS = 60.0
DEFAULT_CHART_TIMEFRAME = "1m"
SUPPORTED_CHART_TIMEFRAMES = ("1m", "5m", "15m", "30m", "1h", "4h", "1d")
DEFAULT_CHART_TIMEFRAMES = ",".join(SUPPORTED_CHART_TIMEFRAMES)
DEFAULT_DISPLAY_TZ = PROJECT_TIMEZONE_NAME
SELECTION_STATE_FILENAME = "live_chart_feed_state.json"
VSCODE_RUN_BUTTON_ARGS = (
    "--historical-backfill",
    "--market",
    "ES",
    "--lookback-hours",
    "168",
    "--timeout-seconds",
    "120",
    "--no-data-warning-seconds",
    "15",
)
VSCODE_RUN_CHILD_ENV = "LIVE_CHART_FEED_RUN_CHILD"
VSCODE_ENV_KEYS = ("VSCODE_PID", "VSCODE_CWD", "VSCODE_IPC_HOOK_CLI")
API_KEY_ENV = "DATABENTO_API_KEY"
PROJECT_DIR_NAME = "futures_intraday_model_v2"
ROOT_API_KEY_FILES = (
    ROOT / "api.env",
)
FIXED_PRICE_SCALE = 1_000_000_000
UNDEF_PRICE = 9_223_372_036_854_775_807
OHLCV_FIELDS = ("ts_event", "open", "high", "low", "close", "volume")
OHLCV_PAYLOAD_FIELDS = ("open", "high", "low", "close", "volume")
TRADE_PAYLOAD_FIELDS = ("price", "size")
ALWAYS_REPORTED_RECORD_TYPE_MARKERS = ("Error",)
LIVE_CONNECTION_FAILURE_MARKERS = (
    "connection refused",
    "connection reset",
    "failed to connect",
    "getaddrinfo failed",
    "name or service not known",
    "temporary failure in name resolution",
    "timed out",
)
AUTH_FAILURE_MARKERS = (
    "401",
    "auth_authentication_failed",
    "authentication failed",
    "invalid username or api key",
)
TIMEFRAME_PATTERN = re.compile(r"^(\d+)([smhd])$")
MARKET_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{1,4}$")
FUTURES_CONTRACT_PATTERN = re.compile(r"^([A-Z0-9]{1,4})([FGHJKMNQUVXZ])([0-9]{1,2})$")
TIER3_RESEARCH_PROFILE_CANDIDATES = (
    "tier_3_research",
    "tier3_research",
    "tier3research",
    "tier_3",
    "tier3",
)
MARKET_SEARCH_ALIASES = {
    "CL": ("crude", "crude oil", "oil", "wti"),
    "NQ": ("nasdaq", "nasdaq 100", "nasdaq-100"),
}
START_CLAMP_PATTERN = re.compile(r"Must be\s+([^,\s]+)\s+or later", re.IGNORECASE)
AVAILABLE_END_PATTERN = re.compile(
    r"data available up to(?:,\s*but\s+not\s+including)?\s+['\"]?([0-9]{4}-[0-9]{2}-[0-9]{2})['\"]?",
    re.IGNORECASE,
)
CHART_RENDER_BATCH_RECORDS = 2_000
CHART_RENDER_BATCH_WAIT_SECONDS = 0.01
CHART_RENDER_THROTTLE_SECONDS = 0.50
TOPBAR_STATUS_NAME = "status"
TOPBAR_MODEL_OVERLAY_NAME = "model"
CHART_STATE_RESOLVING = "Resolving"
CHART_STATE_BACKFILLING = "Backfilling"
CHART_STATE_RENDERING = "Rendering"
CHART_STATE_CONNECTING = "Connecting"
CHART_STATE_LIVE = "Live"
CHART_STATE_HISTORICAL_ONLY = "Historical-only"
CHART_STATE_RECONNECTING = "Reconnecting"
CHART_STATE_STALE = "Stale"
LOADING_STATUS_TEXT = f"{CHART_STATE_BACKFILLING}..."
EXCHANGE_TZ_NAME = "America/Chicago"
RTH_OPEN_HOUR = 8
RTH_OPEN_MINUTE = 30
GLOBEX_OPEN_HOUR = 17
GLOBEX_OPEN_MINUTE = 0
MODEL_OVERLAY_UNAVAILABLE_TEXT = "model output unavailable"
CHART_INSTALL_MESSAGE = (
    'Missing lightweight-charts package; install optional chart support with: '
    'python -m pip install "lightweight-charts>=2.1,<3"'
)
CHART_RUNTIME_ASSET_FILES = ("index.html", "bundle.js", "lightweight-charts.js", "styles.css")
CandleValue = datetime | int | float
SubscriptionStart = datetime | str | int | None


def candle_float(value: CandleValue) -> float:
    return float(cast(Any, value))


def candle_int(value: CandleValue) -> int:
    return int(cast(Any, value))


@dataclass
class ChartStatus:
    records_updated: int = 0
    first_time: datetime | None = None
    latest_time: datetime | None = None
    last_close: float | None = None
    status_line_printed: bool = False


@dataclass
class ChartDisplayState:
    raw_candles: list[dict[str, CandleValue]]
    timeframe: str
    timeframe_seconds: int
    display_tz: tzinfo
    display_tz_name: str
    rendered_candle_count: int = 0
    loading: bool = True
    topbar_state: str = CHART_STATE_RESOLVING
    session_marker_objects: list[Any] = field(default_factory=list)
    timeframe_cache: dict[str, list[dict[str, CandleValue]]] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderStartClamp:
    allowed_start: datetime
    message: str


@dataclass(frozen=True)
class MarketInfo:
    symbol: str
    family: str | None = None
    aliases: tuple[str, ...] = ()
    description: str | None = None
    sources: tuple[str, ...] = ()

    def search_values(self) -> tuple[str, ...]:
        values = [self.symbol, *self.aliases]
        if self.family:
            values.append(self.family)
            values.append(self.family.replace("_", " "))
        if self.description:
            values.append(self.description)
        return tuple(values)


@dataclass(frozen=True)
class SymbologyCandidate:
    symbol: str
    start_date: str
    end_date: str


@dataclass(frozen=True)
class ResolvedInstrument:
    market: str
    query_symbol: str
    instrument_id: int
    raw_symbol: str | None
    candidates: tuple[SymbologyCandidate, ...]


@dataclass(frozen=True)
class ApiKeyResolution:
    key: str
    source: str


@dataclass(frozen=True)
class MarketRuntimeState:
    resolved: ResolvedInstrument
    display_symbol: str
    historical_candles: tuple[dict[str, CandleValue], ...]


@dataclass
class LiveChartTiming:
    stderr: TextIO
    clock: Callable[[], float] = time.perf_counter
    started_at: float = field(init=False)
    last_at: float = field(init=False)

    def __post_init__(self) -> None:
        self.started_at = self.clock()
        self.last_at = self.started_at

    def mark(self, label: str) -> None:
        now = self.clock()
        print(
            f"Live chart timing: {label} "
            f"delta={now - self.last_at:.3f}s elapsed={now - self.started_at:.3f}s",
            file=self.stderr,
        )
        self.last_at = now


@dataclass(frozen=True)
class ModelOverlayState:
    available: bool = False
    signal: str | None = None
    score: float | None = None
    position: str | None = None
    confidence: str | None = None
    gate_status: str | None = None
    realized_pnl: float | None = None
    unrealized_pnl: float | None = None


ChartQueueItem = dict[str, CandleValue] | ProviderStartClamp


def nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return parsed


def normalize_timeframe(value: str) -> str:
    text = value.strip().lower()
    match = TIMEFRAME_PATTERN.fullmatch(text)
    if match is None:
        raise argparse.ArgumentTypeError("timeframe must be one of: " + ", ".join(SUPPORTED_CHART_TIMEFRAMES))
    amount = int(match.group(1))
    if amount <= 0:
        raise argparse.ArgumentTypeError("timeframe amount must be > 0")
    normalized = f"{amount}{match.group(2)}"
    if normalized not in SUPPORTED_CHART_TIMEFRAMES:
        raise argparse.ArgumentTypeError("timeframe must be one of: " + ", ".join(SUPPORTED_CHART_TIMEFRAMES))
    return normalized


def timeframe_seconds(value: str) -> int:
    timeframe = normalize_timeframe(value)
    amount = int(timeframe[:-1])
    unit = timeframe[-1]
    if unit == "s":
        return amount
    if unit == "m":
        return amount * 60
    if unit == "h":
        return amount * 60 * 60
    if unit == "d":
        return amount * 24 * 60 * 60
    raise argparse.ArgumentTypeError(f"unsupported timeframe unit: {unit}")


def parse_chart_timeframes(value: str) -> tuple[str, ...]:
    timeframes = tuple(
        dict.fromkeys(
            normalize_timeframe(part)
            for part in value.split(",")
            if part.strip()
        )
    )
    if not timeframes:
        raise argparse.ArgumentTypeError("must include at least one chart timeframe")
    return timeframes


def parse_ohlcv_schema_seconds(schema: str) -> int | None:
    prefix = "ohlcv-"
    if not schema.startswith(prefix):
        return None
    try:
        return timeframe_seconds(schema[len(prefix) :])
    except argparse.ArgumentTypeError:
        return None


def validate_chart_timeframe(
    *,
    chart_timeframe: str,
    chart_timeframes: tuple[str, ...],
    schema: str,
) -> str | None:
    if chart_timeframe not in SUPPORTED_CHART_TIMEFRAMES:
        return f"unsupported timeframe {chart_timeframe!r}; choose one of {', '.join(SUPPORTED_CHART_TIMEFRAMES)}."
    if chart_timeframe not in chart_timeframes:
        return (
            f"--timeframe {chart_timeframe!r} must be included in "
            f"--chart-timeframes {','.join(chart_timeframes)!r}."
        )
    unsupported = [option for option in chart_timeframes if option not in SUPPORTED_CHART_TIMEFRAMES]
    if unsupported:
        return f"unsupported timeframe option(s): {', '.join(unsupported)}."
    source_seconds = parse_ohlcv_schema_seconds(schema)
    if source_seconds is None:
        return None
    for option in chart_timeframes:
        option_seconds = timeframe_seconds(option)
        if option_seconds < source_seconds or option_seconds % source_seconds != 0:
            return (
                f"chart timeframe {option!r} cannot be built from schema {schema!r}; "
                "use the same or a larger multiple of the source OHLCV interval."
            )
    return None


def resolve_display_tz(value: str) -> tuple[tzinfo, str]:
    text = value.strip()
    if not text or text.lower() == "local":
        local_tz = datetime.now().astimezone().tzinfo
        return local_tz or timezone.utc, "local"
    if text.lower() in {"utc", "z"}:
        return timezone.utc, "UTC"
    try:
        return ZoneInfo(text), text
    except ZoneInfoNotFoundError as exc:
        raise argparse.ArgumentTypeError(f"unknown timezone: {value}") from exc


def normalize_market(value: str) -> str:
    text = value.strip().upper()
    if not MARKET_SYMBOL_PATTERN.fullmatch(text):
        raise argparse.ArgumentTypeError(f"invalid market symbol: {value!r}")
    return text


def is_market_symbol(value: object) -> bool:
    return isinstance(value, str) and MARKET_SYMBOL_PATTERN.fullmatch(value.strip().upper()) is not None


def infer_root_symbol(value: str) -> str:
    text = value.strip().upper()
    match = FUTURES_CONTRACT_PATTERN.fullmatch(text)
    if match is not None:
        return match.group(1)
    return text or "n/a"


def add_market(
    markets: dict[str, MarketInfo],
    symbol: object,
    source: str,
    *,
    family: str | None = None,
    aliases: Sequence[str] = (),
    description: str | None = None,
) -> None:
    if not is_market_symbol(symbol):
        return
    normalized = str(symbol).strip().upper()
    current = markets.get(normalized)
    if current is None:
        markets[normalized] = MarketInfo(
            symbol=normalized,
            family=family,
            aliases=tuple(aliases),
            description=description,
            sources=(source,),
        )
    elif source not in current.sources:
        markets[normalized] = MarketInfo(
            symbol=normalized,
            family=current.family or family,
            aliases=tuple(dict.fromkeys((*current.aliases, *aliases))),
            description=current.description or description,
            sources=tuple((*current.sources, source)),
        )


def normalized_profile_name(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


def require_str_mapping(value: object, name: str) -> MappingABC[str, Any]:
    if not isinstance(value, MappingABC):
        raise TypeError(f"{name} must be a mapping, got {type(value).__name__}")
    return cast(MappingABC[str, Any], value)


def load_yaml_mapping(path: Path) -> MappingABC[str, Any]:
    yaml_module = importlib.import_module("yaml")
    with path.open("r", encoding="utf-8-sig") as handle:
        data = yaml_module.safe_load(handle)
    if not isinstance(data, MappingABC):
        raise ValueError(f"YAML root is not a mapping: {path}")
    return cast(MappingABC[str, Any], data)


def tier3_research_profile(config: MappingABC[str, Any]) -> MappingABC[str, Any]:
    profiles = config.get("profiles")
    if not isinstance(profiles, MappingABC):
        raise ValueError("configs/alpha_tiered.yaml has no profiles mapping")
    candidates = {normalized_profile_name(name) for name in TIER3_RESEARCH_PROFILE_CANDIDATES}
    for name, profile in profiles.items():
        if normalized_profile_name(name) in candidates:
            if not isinstance(profile, MappingABC):
                raise ValueError(f"Tier 3 Research profile is not a mapping: {name}")
            return cast(MappingABC[str, Any], profile)
    raise ValueError("Tier 3 Research profile not found in configs/alpha_tiered.yaml")


def discover_available_markets(root: Path = ROOT) -> dict[str, MarketInfo]:
    markets: dict[str, MarketInfo] = {}
    path = root / "configs" / "alpha_tiered.yaml"
    config = load_yaml_mapping(path)
    profile = tier3_research_profile(config)
    symbols = profile.get("markets")
    if symbols is None:
        market_sets = config.get("market_sets")
        market_set_name = profile.get("market_set")
        if isinstance(market_sets, MappingABC) and isinstance(market_set_name, str):
            symbols = market_sets.get(market_set_name)
    if not isinstance(symbols, list):
        raise ValueError("Tier 3 Research profile has no markets list")
    integration = config.get("canonical_universe_integration")
    if isinstance(integration, MappingABC):
        live_symbols = integration.get("live_subscription_markets")
        if not isinstance(live_symbols, list) or not live_symbols:
            raise ValueError("Canonical integration has no live_subscription_markets list")
        unknown_live = sorted(set(map(str, live_symbols)) - set(map(str, symbols)))
        if unknown_live:
            raise ValueError(
                "Live subscription markets are outside the canonical profile: "
                + ",".join(unknown_live)
            )
        symbols = live_symbols
    families = profile.get("market_families")
    if not isinstance(families, MappingABC):
        families = config.get("market_families")
    if not isinstance(families, MappingABC):
        families = {}
    description = profile.get("description")
    source = f"config:{path.relative_to(root).as_posix()}#profiles.tier_3_research"
    for symbol in symbols:
        normalized = str(symbol).strip().upper()
        add_market(
            markets,
            normalized,
            source,
            family=str(families.get(normalized)) if normalized in families else None,
            aliases=MARKET_SEARCH_ALIASES.get(normalized, ()),
            description=str(description) if description is not None else None,
        )
    return markets


def chart_market_universe(root: Path = ROOT) -> tuple[MarketInfo, ...]:
    return tuple(discover_available_markets(root).values())


def default_selection_state_path(env: MappingABC[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    local_appdata = values.get("LOCALAPPDATA")
    if local_appdata:
        return Path(local_appdata) / "futures_intraday_model_v2" / SELECTION_STATE_FILENAME
    return Path.home() / ".futures_intraday_model_v2" / SELECTION_STATE_FILENAME


def load_selection_state(path: Path) -> dict[str, str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    if not isinstance(data, MappingABC):
        return {}
    state: dict[str, str] = {}
    for key in ("market", "timeframe"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            state[key] = value.strip()
    return state


def write_selection_state(
    path: Path,
    *,
    market: str,
    timeframe: str,
    stderr: TextIO,
) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"market": market, "timeframe": timeframe}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except Exception as exc:
        print(f"Could not persist live chart selection: {exc}", file=stderr)


def option_present(args: Sequence[str], names: set[str]) -> bool:
    return any(arg in names or any(arg.startswith(f"{name}=") for name in names) for arg in args)


def resolve_selection_state_path(args: argparse.Namespace, env: dict[str, str] | None) -> Path:
    value = getattr(args, "selection_state_path", None)
    if value:
        return Path(value)
    return default_selection_state_path(env)


def apply_persisted_selection(
    args: argparse.Namespace,
    *,
    markets: MappingABC[str, MarketInfo],
    state: MappingABC[str, str],
) -> None:
    if (
        getattr(args, "market", None) is None
        and getattr(args, "search_market", None) is None
        and getattr(args, "symbols", None) is None
        and not getattr(args, "_market_explicit", False)
    ):
        market = state.get("market", "").strip().upper()
        if market in markets:
            args.market = market
    if not getattr(args, "_timeframe_explicit", False):
        timeframe = state.get("timeframe", "").strip()
        if timeframe:
            try:
                args.chart_timeframe = normalize_timeframe(timeframe)
            except argparse.ArgumentTypeError:
                return


def matching_markets(markets: MappingABC[str, MarketInfo], query: str | None) -> list[MarketInfo]:
    if query is None or not query.strip():
        return list(markets.values())
    needle = query.strip().lower()
    return [
        info
        for info in markets.values()
        if any(needle in value.lower() for value in info.search_values())
    ]


def format_available_markets(markets: Sequence[MarketInfo]) -> str:
    if not markets:
        return "No markets discovered."
    symbols = [info.symbol for info in markets]
    lines = [f"Available Tier 3 Research markets ({len(symbols)}):"]
    for idx in range(0, len(symbols), 16):
        lines.append("  " + " ".join(symbols[idx : idx + 16]))
    return "\n".join(lines)


def format_market_candidates(markets: Sequence[MarketInfo]) -> str:
    if not markets:
        return "No matching markets."
    lines = [f"Matching markets ({len(markets)}):"]
    for info in markets:
        detail = f" [{info.family}]" if info.family else ""
        aliases = f" aliases={','.join(info.aliases)}" if info.aliases else ""
        lines.append(f"  {info.symbol}{detail}{aliases}")
    return "\n".join(lines)


def market_query_symbol(market: str, symbols: str | None) -> str:
    if symbols is not None and symbols.strip():
        parsed = parse_symbols(symbols)
        if not isinstance(parsed, str):
            raise argparse.ArgumentTypeError(
                "--symbols must resolve to exactly one instrument for the live feed chart"
            )
        return parsed
    return f"{market}{DEFAULT_CONTINUOUS_SUFFIX}"


def parse_start(value: str | None) -> str | int | None:
    if value is None or value == "":
        return None
    if value == "0":
        return 0
    return value


def is_vscode_environment(env: MappingABC[str, str] | None = None) -> bool:
    source = os.environ if env is None else env
    if source.get("TERM_PROGRAM", "").lower() == "vscode":
        return True
    return any(bool(source.get(key)) for key in VSCODE_ENV_KEYS)


def resolve_main_argv(
    argv: Sequence[str] | None = None,
    *,
    env: MappingABC[str, str] | None = None,
) -> list[str]:
    values = list(sys.argv[1:] if argv is None else argv)
    if values:
        return values
    return list(VSCODE_RUN_BUTTON_ARGS)


def should_launch_vscode_run_child(
    raw_args: Sequence[str],
    *,
    env: MappingABC[str, str] | None = None,
) -> bool:
    source = os.environ if env is None else env
    return (
        not raw_args
        and is_vscode_environment(source)
        and not source.get(VSCODE_RUN_CHILD_ENV)
    )


def launch_vscode_run_child(stderr: TextIO = sys.stderr) -> int:
    env = dict(os.environ)
    env[VSCODE_RUN_CHILD_ENV] = "1"
    command = [sys.executable, str(Path(__file__).resolve()), *VSCODE_RUN_BUTTON_ARGS]
    creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    previous_handler = signal.getsignal(signal.SIGINT)
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        process = subprocess.Popen(
            command,
            cwd=str(ROOT),
            env=env,
            creationflags=creationflags,
        )
    finally:
        signal.signal(signal.SIGINT, previous_handler)

    try:
        return process.wait()
    except KeyboardInterrupt:
        print(
            "VS Code interrupted the launcher; live chart child process is still running.",
            file=stderr,
        )
        return 0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def resolve_window_start(
    args: argparse.Namespace,
    *,
    now: datetime | None = None,
) -> SubscriptionStart:
    current = now or utc_now()
    if args.start is not None:
        return parse_start(args.start)
    if args.lookback_days is not None:
        return current - timedelta(days=args.lookback_days)
    hours = args.lookback_hours or DEFAULT_LOOKBACK_HOURS
    return current - timedelta(hours=hours)


def format_utc_time(value: datetime | None) -> str:
    if value is None:
        return "n/a"
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00",
        "Z",
    )


def format_close(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def short_display_time_label(value: datetime | None, display_tz: tzinfo) -> str:
    if value is None:
        return "n/a"
    return value.astimezone(display_tz).strftime("%H:%M")


def effective_timeout_seconds(args: argparse.Namespace) -> float | None:
    if getattr(args, "no_timeout", False):
        return None
    return args.timeout_seconds


def is_live_connection_failure(exc: Exception) -> bool:
    text = str(exc).lower()
    if "connection to " in text and " failed" in text:
        return True
    return any(marker in text for marker in LIVE_CONNECTION_FAILURE_MARKERS)


def is_databento_auth_error(exc: BaseException | str) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in AUTH_FAILURE_MARKERS)


def format_databento_auth_error(source: str) -> str:
    return (
        f"Databento authentication failed using {source}. "
        "Refresh the key from https://databento.com/docs/portal/api-keys."
    )


def format_chart_title(
    *,
    symbols: str,
    schema: str,
    mode: str,
    chart_timeframe: str,
    display_tz: tzinfo,
    display_tz_name: str,
    status: ChartStatus | None = None,
) -> str:
    return f"{symbols} \u00b7 {chart_timeframe}"


def model_overlay_state(*, market: str, symbol: str | None = None) -> ModelOverlayState:
    _ = (market, symbol)
    return ModelOverlayState()


def model_overlay_status_text(state: ModelOverlayState) -> str:
    if not state.available:
        return MODEL_OVERLAY_UNAVAILABLE_TEXT
    parts = [
        f"signal={state.signal or 'n/a'}",
        f"score={state.score if state.score is not None else 'n/a'}",
        f"position={state.position or 'n/a'}",
        f"gate={state.gate_status or state.confidence or 'n/a'}",
        f"realized={state.realized_pnl if state.realized_pnl is not None else 'n/a'}",
        f"unrealized={state.unrealized_pnl if state.unrealized_pnl is not None else 'n/a'}",
    ]
    return "model " + " ".join(parts)


def format_topbar_status(
    *,
    symbols: str,
    display: ChartDisplayState,
    status: ChartStatus,
    overlay: ModelOverlayState | None = None,
) -> str:
    overlay_text = model_overlay_status_text(overlay or model_overlay_state(market=symbols, symbol=symbols))
    topbar_state = display.topbar_state
    latest_is_stale = False
    if status.latest_time is not None:
        age_seconds = (datetime.now(timezone.utc) - normalize_ts_event(status.latest_time)).total_seconds()
        latest_is_stale = age_seconds > DEFAULT_NO_DATA_WARNING_SECONDS
    if latest_is_stale and topbar_state not in {CHART_STATE_HISTORICAL_ONLY, CHART_STATE_RECONNECTING}:
        topbar_state = CHART_STATE_STALE
    if display.loading:
        return f"{topbar_state}... {status.records_updated:,} bars | {overlay_text}"
    latest = short_display_time_label(status.latest_time, display.display_tz)
    return (
        f"{topbar_state} | {symbols} {display.timeframe} | "
        f"{status.records_updated:,} bars | last {latest} | {overlay_text}"
    )


def safe_topbar_text(value: str) -> str:
    return value.replace('"', "'").replace("\n", " ")


def configure_status_text(chart: object, text: str) -> None:
    topbar = getattr(chart, "topbar", None)
    textbox = getattr(topbar, "textbox", None)
    if callable(textbox):
        textbox(TOPBAR_STATUS_NAME, safe_topbar_text(text), align="right")


def update_chart_status_text(chart: object, text: str) -> None:
    topbar = getattr(chart, "topbar", None)
    getter = getattr(topbar, "get", None)
    widget = getter(TOPBAR_STATUS_NAME) if callable(getter) else None
    setter = getattr(widget, "set", None)
    if callable(setter):
        setter(safe_topbar_text(text))


def parse_symbols(value: str) -> str | list[str]:
    symbols = [symbol.strip() for symbol in value.split(",") if symbol.strip()]
    if not symbols:
        raise argparse.ArgumentTypeError("must include at least one symbol")
    if len(symbols) == 1:
        return symbols[0]
    return symbols


def api_key_files_for_base(base: Path) -> tuple[Path, ...]:
    return (base / "api.env",)


def frozen_executable_dir() -> Path | None:
    if not getattr(sys, "frozen", False):
        return None
    return Path(sys.executable).resolve().parent


def frozen_project_api_key_files(frozen_base: Path) -> tuple[Path, ...]:
    return (frozen_base / PROJECT_DIR_NAME / "api.env",)


def frozen_repository_api_key_file(frozen_base: Path) -> Path | None:
    """Return the canonical checkout credential path for its packaged app."""

    repository_root = frozen_base.parent
    if (
        frozen_base.name != "FuturesLiveCockpit"
        or repository_root.name != PROJECT_DIR_NAME
        or not (repository_root / "pyproject.toml").is_file()
        or not (repository_root / "src" / "futures_rebuild" / "live_cockpit").is_dir()
    ):
        return None
    return repository_root / "api.env"


def runtime_api_key_base() -> Path:
    return frozen_executable_dir() or ROOT


def runtime_api_key_files() -> tuple[Path, ...]:
    frozen_base = frozen_executable_dir()
    if frozen_base is not None:
        repository_file = frozen_repository_api_key_file(frozen_base)
        return (
            *((repository_file,) if repository_file is not None else ()),
            *frozen_project_api_key_files(frozen_base),
            *api_key_files_for_base(frozen_base),
        )
    return ROOT_API_KEY_FILES


def safe_api_key_file_label(path: Path, base: Path) -> str:
    try:
        return path.resolve().relative_to(base.resolve()).as_posix()
    except ValueError:
        return path.name


def resolve_api_key_source(env: dict[str, str] | None = None) -> ApiKeyResolution | None:
    if env is not None:
        key = normalize_api_key(env.get(API_KEY_ENV, ""))
        if key:
            return ApiKeyResolution(
                key=key,
                source=f"environment {API_KEY_ENV}",
            )
        return None
    key_base = runtime_api_key_base()
    key_files = runtime_api_key_files()
    for path in key_files:
        key = load_databento_api_key_from_file(path, key_name=API_KEY_ENV)
        if key:
            return ApiKeyResolution(
                key=key,
                source=f"file {safe_api_key_file_label(path, key_base)}",
            )
    return None


def fixed_price_to_float(value: object) -> float:
    if value is None:
        raise ValueError("price is missing")
    if isinstance(value, bool):
        raise ValueError("price must be numeric")
    if isinstance(value, int):
        if value == UNDEF_PRICE:
            raise ValueError("price is undefined")
        return value / FIXED_PRICE_SCALE
    if isinstance(value, float):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError("price is empty")
        return float(stripped)
    raise ValueError(f"unsupported price type: {type(value).__name__}")


def normalize_ts_event(value: object) -> datetime:
    """Return a UTC datetime; lightweight_charts misreads numeric seconds as ms."""

    if value is None:
        raise ValueError("ts_event is missing")
    if isinstance(value, bool):
        raise ValueError("ts_event must be a timestamp")
    if isinstance(value, int):
        if value > 100_000_000_000_000_000:
            return datetime.fromtimestamp(value / 1_000_000_000, tz=timezone.utc)
        if value > 100_000_000_000_000:
            return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc)
        if value > 100_000_000_000:
            return datetime.fromtimestamp(value / 1_000, tz=timezone.utc)
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, float):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("ts_event is empty")
        if text.isdigit():
            return normalize_ts_event(int(text))
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    else:
        raise ValueError(f"unsupported ts_event type: {type(value).__name__}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def floor_timeframe(value: datetime, seconds: int) -> datetime:
    timestamp = int(normalize_ts_event(value).timestamp())
    return datetime.fromtimestamp(timestamp - (timestamp % seconds), tz=timezone.utc)


def exchange_tz() -> tzinfo:
    try:
        return ZoneInfo(EXCHANGE_TZ_NAME)
    except ZoneInfoNotFoundError:
        return timezone.utc


def session_trading_day(value: datetime) -> date:
    exchange_value = normalize_ts_event(value).astimezone(exchange_tz())
    globex_open = datetime_time(GLOBEX_OPEN_HOUR, GLOBEX_OPEN_MINUTE)
    if exchange_value.time() >= globex_open:
        return exchange_value.date() + timedelta(days=1)
    return exchange_value.date()


def trading_day_start(value: datetime) -> datetime:
    day = session_trading_day(value)
    start_date = day - timedelta(days=1)
    exchange_start = datetime.combine(
        start_date,
        datetime_time(GLOBEX_OPEN_HOUR, GLOBEX_OPEN_MINUTE),
        tzinfo=exchange_tz(),
    )
    return exchange_start.astimezone(timezone.utc)


def floor_exchange_timeframe(value: datetime, seconds: int) -> datetime:
    exchange_value = normalize_ts_event(value).astimezone(exchange_tz())
    midnight = datetime.combine(exchange_value.date(), datetime_time.min, tzinfo=exchange_tz())
    elapsed = int((exchange_value - midnight).total_seconds())
    bucket_elapsed = elapsed - (elapsed % seconds)
    return (midnight + timedelta(seconds=bucket_elapsed)).astimezone(timezone.utc)


def candle_bucket_time(value: object, timeframe: str) -> datetime:
    normalized = normalize_timeframe(timeframe)
    utc_value = normalize_ts_event(value)
    if normalized == "1d":
        return trading_day_start(utc_value)
    if normalized == "4h":
        return floor_exchange_timeframe(utc_value, timeframe_seconds(normalized))
    return floor_timeframe(utc_value, timeframe_seconds(normalized))


def chart_display_time(value: object, display_tz: tzinfo) -> datetime:
    utc_value = normalize_ts_event(value)
    return utc_value.astimezone(display_tz).replace(tzinfo=None)


def aggregate_candles(
    candles: Sequence[dict[str, CandleValue]],
    *,
    seconds: int,
    timeframe: str | None = None,
) -> list[dict[str, CandleValue]]:
    aggregated: dict[datetime, dict[str, CandleValue]] = {}
    for candle in candles:
        bucket = (
            candle_bucket_time(candle["time"], timeframe)
            if timeframe is not None
            else floor_timeframe(normalize_ts_event(candle["time"]), seconds)
        )
        if bucket not in aggregated:
            aggregated[bucket] = {
                "time": bucket,
                "open": candle_float(candle["open"]),
                "high": candle_float(candle["high"]),
                "low": candle_float(candle["low"]),
                "close": candle_float(candle["close"]),
                "volume": candle_int(candle["volume"]),
            }
            continue

        current = aggregated[bucket]
        current["high"] = max(
            candle_float(current["high"]),
            candle_float(candle["high"]),
        )
        current["low"] = min(
            candle_float(current["low"]),
            candle_float(candle["low"]),
        )
        current["close"] = candle_float(candle["close"])
        current["volume"] = candle_int(current["volume"]) + candle_int(
            candle["volume"]
        )
    return list(aggregated.values())


def candle_for_display(
    candle: dict[str, CandleValue],
    *,
    display_tz: tzinfo,
) -> dict[str, CandleValue]:
    displayed = dict(candle)
    displayed["time"] = chart_display_time(candle["time"], display_tz)
    return displayed


def session_marker_specs(
    display: ChartDisplayState,
    status: ChartStatus,
) -> list[tuple[datetime, str, str]]:
    if display.timeframe == "1d" or status.first_time is None or status.latest_time is None:
        return []
    try:
        exchange_tz = ZoneInfo(EXCHANGE_TZ_NAME)
    except ZoneInfoNotFoundError:
        return []

    first_utc = normalize_ts_event(status.first_time)
    latest_utc = normalize_ts_event(status.latest_time)
    current_day = first_utc.astimezone(exchange_tz).date()
    end_day = latest_utc.astimezone(exchange_tz).date()
    specs: list[tuple[datetime, str, str]] = []

    while current_day <= end_day:
        anchors = (
            (RTH_OPEN_HOUR, RTH_OPEN_MINUTE, "RTH", "#5b8def"),
            (GLOBEX_OPEN_HOUR, GLOBEX_OPEN_MINUTE, "Globex", "#8a94a6"),
        )
        for hour, minute, label, color in anchors:
            exchange_time = datetime(
                current_day.year,
                current_day.month,
                current_day.day,
                hour,
                minute,
                tzinfo=exchange_tz,
            )
            utc_time = exchange_time.astimezone(timezone.utc)
            if first_utc <= utc_time <= latest_utc:
                specs.append((chart_display_time(utc_time, display.display_tz), label, color))
        current_day += timedelta(days=1)
    return specs


def clear_session_markers(display: ChartDisplayState) -> None:
    for marker in display.session_marker_objects:
        delete = getattr(marker, "delete", None)
        if callable(delete):
            try:
                delete()
            except Exception:
                pass
    display.session_marker_objects.clear()


def refresh_session_markers(
    chart: object,
    display: ChartDisplayState,
    status: ChartStatus,
) -> None:
    clear_session_markers(display)
    vertical_line = getattr(chart, "vertical_line", None)
    if not callable(vertical_line):
        return
    for marker_time, label, color in session_marker_specs(display, status):
        try:
            marker = vertical_line(
                marker_time,
                color=color,
                width=1,
                style="dotted",
                text=label,
            )
        except Exception:
            continue
        display.session_marker_objects.append(marker)


def record_value(record: object, name: str) -> object:
    if not hasattr(record, name):
        raise ValueError(f"missing field: {name}")
    value = getattr(record, name)
    return value() if callable(value) else value


def record_error_text(record: object) -> str:
    values: list[str] = []
    for name in ("err", "message", "msg"):
        if not hasattr(record, name):
            continue
        try:
            value = record_value(record, name)
        except Exception:
            continue
        if value:
            values.append(str(value))
    return " ".join(values)


def parse_allowed_start_from_text(value: str) -> datetime | None:
    match = START_CLAMP_PATTERN.search(value)
    if match is None:
        return None
    try:
        return normalize_ts_event(match.group(1))
    except Exception:
        return None


def parse_available_end_from_text(value: str) -> datetime | None:
    match = AVAILABLE_END_PATTERN.search(value)
    if match is None:
        return None
    try:
        return normalize_ts_event(match.group(1))
    except Exception:
        return None


def iso_date_from_value(value: object) -> str | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    try:
        return normalize_ts_event(value).date().isoformat()
    except Exception:
        return None


def available_exclusive_end_from_range(value: object) -> str | None:
    if isinstance(value, MappingABC):
        for key in ("end", "end_date", "data_end", "data_end_date", "available_end_date"):
            parsed = iso_date_from_value(value.get(key))
            if parsed is not None:
                return parsed
        for nested in value.values():
            parsed = available_exclusive_end_from_range(nested)
            if parsed is not None:
                return parsed
    return None


def lookup_dataset_available_exclusive_end(
    historical: object,
    *,
    dataset: str,
    stderr: TextIO,
) -> str | None:
    metadata = getattr(historical, "metadata", None)
    get_dataset_range = getattr(metadata, "get_dataset_range", None)
    if not callable(get_dataset_range):
        return None
    try:
        dataset_range = get_dataset_range(dataset=dataset)
    except TypeError:
        dataset_range = get_dataset_range(dataset)
    except Exception as exc:
        if is_databento_auth_error(exc):
            raise
        print(f"Databento dataset range lookup unavailable: {exc}", file=stderr)
        return None
    return available_exclusive_end_from_range(dataset_range)


def clamp_exclusive_end_date(
    *,
    start_date: str,
    requested_end_date: str,
    available_exclusive_end_date: str | None,
    context: str,
    stderr: TextIO,
) -> str:
    final_end_date = requested_end_date
    if available_exclusive_end_date is not None:
        final_end_date = min(requested_end_date, available_exclusive_end_date)
    print(
        "Databento "
        f"{context} end date: requested={requested_end_date}, "
        f"available_exclusive={available_exclusive_end_date or 'unknown'}, "
        f"final={final_end_date}",
        file=stderr,
    )
    if final_end_date <= start_date:
        raise ValueError(
            "Databento "
            f"{context} end date {final_end_date} is not after start date {start_date}. "
            "Reduce --lookback-hours, use a later start, or wait for newer dataset data."
        )
    return final_end_date


def clamp_historical_end(
    *,
    start: SubscriptionStart,
    requested_end: datetime,
    available_exclusive_end_date: str | None,
    stderr: TextIO,
) -> datetime:
    if available_exclusive_end_date is None:
        return requested_end
    start_dt = normalize_ts_event(start)
    available_end = datetime.combine(
        date.fromisoformat(available_exclusive_end_date),
        datetime_time.min,
        tzinfo=timezone.utc,
    )
    final_end = min(normalize_ts_event(requested_end), available_end)
    print(
        "Databento historical end date: "
        f"requested={normalize_ts_event(requested_end).date().isoformat()}, "
        f"available_exclusive={available_exclusive_end_date}, "
        f"final={final_end.date().isoformat()}",
        file=stderr,
    )
    if final_end <= start_dt:
        raise ValueError(
            "Databento historical end time "
            f"{final_end.isoformat()} is not after start time {start_dt.isoformat()}. "
            "Reduce --lookback-hours, use a later start, or wait for newer dataset data."
        )
    return final_end


def provider_start_clamp_from_record(record: object) -> ProviderStartClamp | None:
    if "Error" not in type(record).__name__:
        return None
    text = record_error_text(record)
    allowed_start = parse_allowed_start_from_text(text)
    if allowed_start is None:
        return None
    return ProviderStartClamp(allowed_start=allowed_start, message=text)


def symbology_candidates(mapping: MappingABC[str, Any]) -> tuple[SymbologyCandidate, ...]:
    result = mapping.get("result")
    if not isinstance(result, MappingABC):
        return ()
    candidates: list[SymbologyCandidate] = []
    for entries in result.values():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, MappingABC):
                continue
            symbol = entry.get("s")
            if symbol in {None, ""}:
                continue
            candidates.append(
                SymbologyCandidate(
                    symbol=str(symbol),
                    start_date=str(entry.get("d0", "")),
                    end_date=str(entry.get("d1", "")),
                )
            )
    return tuple(candidates)


def unique_candidate_symbols(candidates: Sequence[SymbologyCandidate]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(candidate.symbol for candidate in candidates))


def format_candidates(candidates: Sequence[SymbologyCandidate]) -> str:
    if not candidates:
        return "none"
    return ", ".join(
        f"{candidate.symbol}[{candidate.start_date},{candidate.end_date})"
        for candidate in candidates
    )


def parse_iso_date_or_bound(value: str, *, upper: bool) -> date:
    if not value:
        return date.max if upper else date.min
    return date.fromisoformat(value)


def candidate_active_before_end(candidate: SymbologyCandidate, end_date: str) -> bool:
    request_end = date.fromisoformat(end_date)
    candidate_start = parse_iso_date_or_bound(candidate.start_date, upper=False)
    candidate_end = parse_iso_date_or_bound(candidate.end_date, upper=True)
    return candidate_start < request_end <= candidate_end


def candidate_symbols_active_before_end(
    candidates: Sequence[SymbologyCandidate],
    *,
    end_date: str,
) -> tuple[str, ...]:
    active_candidates = [
        candidate
        for candidate in candidates
        if candidate_active_before_end(candidate, end_date)
    ]
    return unique_candidate_symbols(active_candidates)


def symbology_date_window(
    *,
    live_start: SubscriptionStart,
    end: datetime,
    fallback_start: datetime,
) -> tuple[str, str]:
    if isinstance(live_start, datetime):
        start_dt = normalize_ts_event(live_start)
    elif isinstance(live_start, str):
        start_dt = normalize_ts_event(live_start)
    else:
        start_dt = fallback_start
    end_dt = normalize_ts_event(end)
    start_date = start_dt.date()
    end_date = (end_dt + timedelta(days=1)).date()
    if end_date <= start_date:
        end_date = start_date + timedelta(days=1)
    return start_date.isoformat(), end_date.isoformat()


def resolve_raw_symbol(
    historical: object,
    *,
    dataset: str,
    instrument_id: int,
    start_date: str,
    end_date: str,
) -> str | None:
    symbology = getattr(historical, "symbology", None)
    resolve = getattr(symbology, "resolve", None)
    if not callable(resolve):
        return None
    try:
        mapping = require_str_mapping(
            resolve(
                dataset=dataset,
                symbols=instrument_id,
                stype_in="instrument_id",
                stype_out="raw_symbol",
                start_date=start_date,
                end_date=end_date,
            ),
            "instrument symbology response",
        )
    except Exception:
        return None
    raw_symbols = unique_candidate_symbols(symbology_candidates(mapping))
    if len(raw_symbols) == 1:
        return raw_symbols[0]
    return None


def resolve_single_instrument(
    historical: object,
    *,
    dataset: str,
    market: str,
    query_symbol: str,
    start_date: str,
    end_date: str,
) -> ResolvedInstrument:
    symbology = getattr(historical, "symbology", None)
    resolve = getattr(symbology, "resolve", None)
    if not callable(resolve):
        raise RuntimeError("Databento Historical symbology client is unavailable.")
    mapping = require_str_mapping(
        resolve(
            dataset=dataset,
            symbols=query_symbol,
            stype_in=DEFAULT_MARKET_STYPE_IN,
            stype_out="instrument_id",
            start_date=start_date,
            end_date=end_date,
        ),
        "market symbology response",
    )
    candidates = symbology_candidates(mapping)
    candidate_symbols = unique_candidate_symbols(candidates)
    if len(candidate_symbols) != 1:
        active_symbols = candidate_symbols_active_before_end(
            candidates,
            end_date=end_date,
        )
        if len(active_symbols) == 1:
            candidate_symbols = active_symbols
    if len(candidate_symbols) != 1:
        raise ValueError(
            "market resolution did not produce exactly one live instrument for "
            f"{market} using {query_symbol!r} over [{start_date}, {end_date}). "
            f"Candidates: {format_candidates(candidates)}"
        )
    try:
        instrument_id = int(candidate_symbols[0])
    except ValueError as exc:
        raise ValueError(
            f"resolved instrument id is not numeric for {market}: {candidate_symbols[0]!r}"
        ) from exc
    raw_symbol = resolve_raw_symbol(
        historical,
        dataset=dataset,
        instrument_id=instrument_id,
        start_date=start_date,
        end_date=end_date,
    )
    return ResolvedInstrument(
        market=market,
        query_symbol=query_symbol,
        instrument_id=instrument_id,
        raw_symbol=raw_symbol,
        candidates=candidates,
    )


def ohlcv_record_to_candle(record: object) -> dict[str, CandleValue]:
    missing = missing_ohlcv_fields(record)
    if missing:
        raise ValueError(
            f"{type(record).__name__} is not an OHLCV record; missing fields: "
            + ", ".join(missing)
        )

    return {
        "time": normalize_ts_event(record_value(record, "ts_event")),
        "open": fixed_price_to_float(record_value(record, "open")),
        "high": fixed_price_to_float(record_value(record, "high")),
        "low": fixed_price_to_float(record_value(record, "low")),
        "close": fixed_price_to_float(record_value(record, "close")),
        "volume": int(cast(Any, record_value(record, "volume"))),
    }


def dataframe_row_to_candle(row: object) -> dict[str, CandleValue]:
    return {
        "time": normalize_ts_event(record_value(row, "ts_event")),
        "open": fixed_price_to_float(record_value(row, "open")),
        "high": fixed_price_to_float(record_value(row, "high")),
        "low": fixed_price_to_float(record_value(row, "low")),
        "close": fixed_price_to_float(record_value(row, "close")),
        "volume": int(cast(Any, record_value(row, "volume"))),
    }


def trade_record_to_values(record: object) -> tuple[datetime, float, int]:
    missing = [field for field in TRADE_PAYLOAD_FIELDS if not hasattr(record, field)]
    if missing:
        raise ValueError(
            f"{type(record).__name__} is not a trade record; missing fields: "
            + ", ".join(missing)
        )
    timestamp_field = "ts_event" if hasattr(record, "ts_event") else "ts_recv"
    if not hasattr(record, timestamp_field):
        raise ValueError(f"{type(record).__name__} has no trade timestamp")
    size = int(cast(Any, record_value(record, "size")))
    if size < 0:
        raise ValueError("trade size must be >= 0")
    return (
        normalize_ts_event(record_value(record, timestamp_field)),
        fixed_price_to_float(record_value(record, "price")),
        size,
    )


@dataclass
class TradeCandleAggregator:
    timeframe_seconds: int = 60
    timeframe: str | None = None
    current_bucket: datetime | None = None
    current_candle: dict[str, CandleValue] | None = None
    last_trade_ts: datetime | None = None
    ignored_out_of_order: int = 0

    def apply_trade(self, record: object) -> dict[str, CandleValue] | None:
        ts_event, price, size = trade_record_to_values(record)
        if self.last_trade_ts is not None and ts_event < self.last_trade_ts:
            self.ignored_out_of_order += 1
            return None
        bucket = (
            candle_bucket_time(ts_event, self.timeframe)
            if self.timeframe is not None
            else floor_timeframe(ts_event, self.timeframe_seconds)
        )
        if self.current_bucket is not None and bucket < self.current_bucket:
            self.ignored_out_of_order += 1
            return None

        self.last_trade_ts = ts_event
        if self.current_bucket != bucket or self.current_candle is None:
            self.current_bucket = bucket
            self.current_candle = {
                "time": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": size,
            }
            return dict(self.current_candle)

        self.current_candle["high"] = max(candle_float(self.current_candle["high"]), price)
        self.current_candle["low"] = min(candle_float(self.current_candle["low"]), price)
        self.current_candle["close"] = price
        self.current_candle["volume"] = candle_int(self.current_candle["volume"]) + size
        return dict(self.current_candle)


def historical_store_to_candles(store: object) -> list[dict[str, CandleValue]]:
    pandas_module = importlib.import_module("pandas")
    to_df = getattr(store, "to_df")
    frame_or_frames = to_df(
        price_type="float",
        pretty_ts=True,
        map_symbols=True,
    )
    if isinstance(frame_or_frames, pandas_module.DataFrame):
        frame = frame_or_frames
    else:
        frames = list(frame_or_frames)
        if not frames:
            return []
        frame = pandas_module.concat(frames, ignore_index=False)

    if frame.empty:
        return []
    if "ts_event" not in frame.columns:
        if isinstance(frame.index, pandas_module.DatetimeIndex):
            frame = frame.reset_index()
            if frame.columns[0] != "ts_event":
                frame = frame.rename(columns={frame.columns[0]: "ts_event"})
        else:
            raise ValueError("historical data has no ts_event column or DatetimeIndex")

    missing = sorted(set(OHLCV_FIELDS) - set(frame.columns))
    if missing:
        raise ValueError(f"historical data missing required columns: {missing}")

    return [
        dataframe_row_to_candle(row)
        for row in frame.sort_values("ts_event", kind="mergesort").itertuples(
            index=False,
        )
    ]


def describe_record_skip(record: object, exc: Exception) -> str:
    return f"Skipping {type(record).__name__}: {exc}"


def missing_ohlcv_fields(record: object) -> list[str]:
    return [field for field in OHLCV_FIELDS if not hasattr(record, field)]


def should_ignore_record(record: object, exc: Exception) -> bool:
    record_type = type(record).__name__
    if any(marker in record_type for marker in ALWAYS_REPORTED_RECORD_TYPE_MARKERS):
        return False
    if "missing fields" not in str(exc):
        return False
    return not any(hasattr(record, field) for field in OHLCV_PAYLOAD_FIELDS)


def should_ignore_trade_record(record: object, exc: Exception) -> bool:
    record_type = type(record).__name__
    if any(marker in record_type for marker in ALWAYS_REPORTED_RECORD_TYPE_MARKERS):
        return False
    if "missing fields" not in str(exc):
        return False
    return not any(hasattr(record, field) for field in TRADE_PAYLOAD_FIELDS)


def import_databento() -> ModuleType:
    return importlib.import_module("databento")
