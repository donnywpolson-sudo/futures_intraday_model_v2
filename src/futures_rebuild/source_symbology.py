"""Exact, schema-aware Databento query-symbology contracts."""

from __future__ import annotations

import re
from collections.abc import Sequence

from .errors import ContractError, IntegrityError


_CONTINUOUS_ONLY_SCHEMAS = frozenset(
    {"ohlcv-1d", "ohlcv-1h", "ohlcv-1m", "ohlcv-1s", "trades"}
)
_MIXED_MARKET_STATE_SCHEMAS = frozenset({"statistics", "status"})
_SUPPORTED_SCHEMAS = (
    frozenset({"definition"})
    | _CONTINUOUS_ONLY_SCHEMAS
    | _MIXED_MARKET_STATE_SCHEMAS
)


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
) -> tuple[str, tuple[str, ...]]:
    """Validate and normalize one source query epoch without inference."""

    if type(stype_in) is not str or not isinstance(symbols, Sequence) or isinstance(
        symbols, (str, bytes, bytearray)
    ):
        raise IntegrityError("DBN query symbology fields are invalid")
    normalized_symbols = tuple(symbols)
    if any(type(symbol) is not str for symbol in normalized_symbols):
        raise IntegrityError("DBN query symbols must be exact strings")
    observed = (stype_in, normalized_symbols)
    if observed not in allowed_query_symbologies(schema, market):
        raise IntegrityError("DBN query symbology differs from its schema/market contract")
    return observed
