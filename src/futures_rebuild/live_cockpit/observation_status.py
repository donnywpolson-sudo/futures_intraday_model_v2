"""Observation-only console status rendering for the Futures Live Cockpit.

This module intentionally contains no operator controls, order-intent types,
broker integration, or trading enablement.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import TextIO


@dataclass(frozen=True)
class OperatorStatusState:
    feed_status: str = "CLOSED"
    active_symbol: str = "n/a"
    active_contract: str = "n/a"
    timeframe: str = "1m"
    records_count: int = 0
    latest_bar_time: datetime | None = None
    latest_bar_age_seconds: float | None = None
    last_close: float | None = None
    model_status: str = "OFF"
    signal: str = "NO_SIGNAL"
    trading_mode: str = "DISABLED"
    kill_switch: str = "OFF"
    risk_status: str = "UNKNOWN"
    reconciliation_status: str = "UNKNOWN"
    paper_position: str | None = None
    last_error_code: str | None = None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("status timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _text(value: object, default: str = "n/a") -> str:
    if value is None:
        return default
    text = " ".join(str(value).split())
    return text or default


def _latest(value: datetime | None) -> str:
    return "n/a" if value is None else _utc(value).strftime("%Y-%m-%d %H:%MZ")


def _age(value: float | None) -> str:
    if value is None:
        return "n/a"
    seconds = max(0, int(value))
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    return f"{minutes}m" if minutes < 60 else f"{minutes // 60}h"


def _close(value: float | None) -> str:
    if value is None:
        return "n/a"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "n/a"
    return f"{number:.2f}" if isfinite(number) else "n/a"


def _symbol_contract(symbol: str, contract: str) -> str:
    clean_symbol = _text(symbol)
    clean_contract = _text(contract)
    if (
        clean_symbol != "n/a"
        and clean_contract != "n/a"
        and clean_symbol != clean_contract
    ):
        return f"{clean_symbol}/{clean_contract}"
    return clean_contract if clean_contract != "n/a" else clean_symbol


def render_operator_status(
    state: OperatorStatusState, width: int | None = None
) -> str:
    width = width or shutil.get_terminal_size((140, 20)).columns
    if width <= 1:
        return ""
    parts = [
        _text(state.feed_status),
        f"{_symbol_contract(state.active_symbol, state.active_contract)} {_text(state.timeframe)}",
        f"rows={state.records_count}",
        f"latest={_latest(state.latest_bar_time)}",
        f"age={_age(state.latest_bar_age_seconds)}",
        f"close={_close(state.last_close)}",
        f"model={_text(state.model_status)}",
        f"sig={_text(state.signal)}",
        "mode=DISABLED",
        f"kill={_text(state.kill_switch)}",
        f"risk={_text(state.risk_status)}",
        f"recon={_text(state.reconciliation_status)}",
    ]
    if state.paper_position:
        parts.append(f"pos={_text(state.paper_position)}")
    if state.last_error_code:
        parts.append(f"err={_text(state.last_error_code)}")
    line = " | ".join(parts)
    return line[: width - 1].ljust(width - 1)


def print_operator_status(
    state: OperatorStatusState,
    *,
    stdout: TextIO,
    width: int | None = None,
    warning: str | None = None,
    error: str | None = None,
) -> None:
    width = width or shutil.get_terminal_size((140, 20)).columns
    stdout.write("\r" + render_operator_status(state, width=width))
    for level, message in (("WARN", warning), ("ERROR", error)):
        if message and width > 1:
            line = f"{level}: {_text(message, '')}"
            stdout.write("\n" + line[: width - 1])
    stdout.flush()
