"""Exact, schema-aware Databento query-symbology contracts."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Mapping

from .canonical import sha256_json
from .errors import ContractError, IntegrityError


DATASET = "GLBX.MDP3"
QUERY_CONTRACT_VERSION = "1.0.0"


_CONTINUOUS_ONLY_SCHEMAS = frozenset(
    {"ohlcv-1d", "ohlcv-1h", "ohlcv-1m", "ohlcv-1s", "trades"}
)
_MIXED_MARKET_STATE_SCHEMAS = frozenset({"statistics", "status"})
_SUPPORTED_SCHEMAS = (
    frozenset({"definition"})
    | _CONTINUOUS_ONLY_SCHEMAS
    | _MIXED_MARKET_STATE_SCHEMAS
)
_DATE_BOUND = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_UTC_SECOND_BOUND = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
)


def _query_bound(value: object) -> tuple[str, datetime]:
    if type(value) is not str or (
        _DATE_BOUND.fullmatch(value) is None
        and _UTC_SECOND_BOUND.fullmatch(value) is None
    ):
        raise ContractError("DBN query contract interval is invalid")
    try:
        rendered = (
            value.removesuffix("Z") + "+00:00"
            if value.endswith("Z")
            else f"{value}T00:00:00+00:00"
        )
        parsed = datetime.fromisoformat(rendered)
    except ValueError as exc:
        raise ContractError("DBN query contract interval is invalid") from exc
    if parsed.tzinfo != timezone.utc:
        raise ContractError("DBN query contract interval is invalid")
    return value, parsed


def allowed_query_symbologies(
    schema: str, market: str
) -> frozenset[tuple[str, tuple[str, ...]]]:
    """Return every explicit query epoch allowed for one schema and market."""

    if schema not in _SUPPORTED_SCHEMAS:
        raise ContractError("DBN query symbology schema is unsupported")
    if re.fullmatch(r"[0-9A-Z]{2,3}", market) is None:
        raise ContractError("DBN query symbology market is invalid")
    continuous = ("continuous", (f"{market}.v.0",))
    parent = ("parent", (f"{market}.FUT",))
    if schema == "definition":
        return frozenset({parent})
    if schema in _MIXED_MARKET_STATE_SCHEMAS:
        return frozenset({continuous, parent})
    return frozenset({continuous})


def require_allowed_query_symbology(
    *,
    schema: str,
    market: str,
    stype_in: object,
    symbols: object,
    allow_diagnostic_parent: bool = False,
) -> tuple[str, tuple[str, ...]]:
    """Validate and normalize one source query epoch without inference."""

    if type(stype_in) is not str or not isinstance(symbols, Sequence) or isinstance(
        symbols, (str, bytes, bytearray)
    ):
        raise IntegrityError("DBN query symbology fields are invalid")
    normalized_symbols = tuple(symbols)
    if any(type(symbol) is not str for symbol in normalized_symbols):
        raise IntegrityError("DBN query symbols must be exact strings")
    if type(allow_diagnostic_parent) is not bool:
        raise ContractError("diagnostic-parent authorization flag must be boolean")
    observed = (stype_in, normalized_symbols)
    diagnostic_parent = ("parent", (f"{market}.FUT",))
    if observed not in allowed_query_symbologies(schema, market) and not (
        allow_diagnostic_parent
        and schema in _CONTINUOUS_ONLY_SCHEMAS
        and observed == diagnostic_parent
    ):
        raise IntegrityError("DBN query symbology differs from its schema/market contract")
    return observed


def build_query_contract(
    *,
    schema: str,
    market: str,
    start: str,
    end: str,
    stype_in: object,
    symbols: object,
    allow_diagnostic_parent: bool = False,
) -> dict[str, object]:
    """Build one content-addressed, exact acquisition-query contract."""

    normalized_stype, normalized_symbols = require_allowed_query_symbology(
        schema=schema,
        market=market,
        stype_in=stype_in,
        symbols=symbols,
        allow_diagnostic_parent=allow_diagnostic_parent,
    )
    normalized_start, start_at = _query_bound(start)
    normalized_end, end_at = _query_bound(end)
    if end_at <= start_at:
        raise ContractError("DBN query contract interval is empty or reversed")
    symbol_template = (
        "{market}.FUT" if normalized_stype == "parent" else "{market}.v.0"
    )
    mode_core: dict[str, object] = {
        "dataset": DATASET,
        "query_contract_version": QUERY_CONTRACT_VERSION,
        "schema": schema,
        "stype_in": normalized_stype,
        "stype_out": "instrument_id",
        "symbol_template": symbol_template,
        "ts_out": False,
        "limit": None,
    }
    mode_id = sha256_json(mode_core)
    contract_core: dict[str, object] = {
        **mode_core,
        "end": normalized_end,
        "market": market,
        "query_mode_id": mode_id,
        "symbols": list(normalized_symbols),
        "start": normalized_start,
    }
    return {**contract_core, "query_contract_id": sha256_json(contract_core)}


def require_query_contract(raw: object) -> dict[str, object]:
    """Validate an exact query contract and return its canonical reconstruction."""

    if not isinstance(raw, Mapping):
        raise IntegrityError("DBN query contract is invalid")
    expected_keys = {
        "dataset",
        "end",
        "limit",
        "market",
        "query_contract_id",
        "query_contract_version",
        "query_mode_id",
        "schema",
        "stype_in",
        "stype_out",
        "symbol_template",
        "symbols",
        "start",
        "ts_out",
    }
    if set(raw) != expected_keys:
        raise IntegrityError("DBN query contract fields are not exact")
    rebuilt = build_query_contract(
        schema=str(raw.get("schema")),
        market=str(raw.get("market")),
        start=str(raw.get("start")),
        end=str(raw.get("end")),
        stype_in=raw.get("stype_in"),
        symbols=raw.get("symbols"),
    )
    if dict(raw) != rebuilt:
        raise IntegrityError("DBN query contract content address is invalid")
    return rebuilt
