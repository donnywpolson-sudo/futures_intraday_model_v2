"""Validate one accepted active-view market-year as a future Phase 3 input.

This module deliberately validates metadata and file identity only.  It does
not open Parquet rows, create a release, or grant trial authority.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .boundary import RepoBoundary
from .canonical import sha256_file, sha256_json
from .errors import ContractError, IntegrityError


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MARKET = re.compile(r"^[A-Z0-9]{1,8}$")
_SIDECAR_SCHEMA = "causal_active_market_year_manifest/1.0.0"
_REQUIRED_CAPABILITY = "RESEARCH_READY_CAUSAL_PRICE"
_REQUIRED_USE = "DISCOVERY_RESEARCH"


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise IntegrityError(f"{name} must be an exact SHA-256 value")
    return value


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise IntegrityError(f"{name} must be an object")
    return value


@dataclass(frozen=True)
class ActivePhase3Input:
    """A metadata-only identity for one accepted active-view input."""

    market: str
    year: int
    parquet_path: str
    parquet_sha256: str
    sidecar_sha256: str
    causal_release_id: str
    source_raw_release_id: str
    active_view_id: str

    def __post_init__(self) -> None:
        if (
            type(self.market) is not str
            or _MARKET.fullmatch(self.market) is None
            or type(self.year) is not int
            or not 2000 <= self.year <= 2100
            or type(self.parquet_path) is not str
        ):
            raise ContractError("active Phase 3 input identity is invalid")
        for value, name in (
            (self.parquet_sha256, "parquet SHA-256"),
            (self.sidecar_sha256, "sidecar SHA-256"),
            (self.causal_release_id, "causal release ID"),
            (self.source_raw_release_id, "source raw release ID"),
            (self.active_view_id, "active-view ID"),
        ):
            _sha(value, name)

    def core(self) -> dict[str, object]:
        return {
            "active_view_id": self.active_view_id,
            "causal_release_id": self.causal_release_id,
            "market": self.market,
            "parquet_path": self.parquet_path,
            "parquet_sha256": self.parquet_sha256,
            "sidecar_sha256": self.sidecar_sha256,
            "source_raw_release_id": self.source_raw_release_id,
            "year": self.year,
        }

    @property
    def input_id(self) -> str:
        return sha256_json(self.core())


def load_active_phase3_input(
    *, boundary: RepoBoundary, market: str, year: int
) -> ActivePhase3Input:
    """Validate an accepted active-view sidecar without opening Parquet rows."""

    if type(market) is not str or _MARKET.fullmatch(market) is None:
        raise ContractError("market must be an uppercase active-view market")
    if type(year) is not int or not 2000 <= year <= 2100:
        raise ContractError("year must be an active-view calendar year")
    relative = Path("data") / "active" / "causally_gated_normalized" / market / str(year)
    parquet_relative = relative / f"{year}.parquet"
    sidecar_relative = Path(f"{parquet_relative.as_posix()}.manifest.json")
    parquet = boundary.assert_active_path(
        boundary.active_root / parquet_relative,
        purpose="active Phase 3 input parquet",
        subtree="data/active",
    )
    sidecar = boundary.assert_active_path(
        boundary.active_root / sidecar_relative,
        purpose="active Phase 3 input sidecar",
        subtree="data/active",
    )
    try:
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("active Phase 3 input sidecar is unreadable") from exc
    payload = _mapping(payload, "active Phase 3 input sidecar")
    if payload.get("schema_version") != _SIDECAR_SCHEMA:
        raise IntegrityError("active Phase 3 input sidecar schema is invalid")
    access = _mapping(payload.get("access_policy_binding"), "access-policy binding")
    entry = _mapping(payload.get("entry_binding"), "active-view entry binding")
    if (
        access.get("market") != market
        or access.get("year") != year
        or access.get("capability") != _REQUIRED_CAPABILITY
        or access.get("selection_eligible") is not True
        or not isinstance(access.get("permitted_uses"), list)
        or _REQUIRED_USE not in access["permitted_uses"]
        or entry.get("market") != market
        or entry.get("year") != year
        or entry.get("disposition") != _REQUIRED_CAPABILITY
        or entry.get("parquet_path") != parquet_relative.as_posix()
    ):
        raise IntegrityError("active Phase 3 input is not an eligible discovery view")
    expected_parquet_hash = _sha(entry.get("parquet_sha256"), "active-view parquet SHA-256")
    bindings = entry.get("source_bindings")
    if not isinstance(bindings, list) or len(bindings) != 1:
        raise IntegrityError("active Phase 3 input requires one exact source binding")
    source = _mapping(bindings[0], "active-view source binding")
    causal_release_id = _sha(source.get("causal_release_id"), "causal release ID")
    source_raw_release_id = _sha(source.get("raw_release_id"), "source raw release ID")
    active_view_id = _sha(access.get("active_view_id"), "active-view ID")
    if not parquet.is_file() or sha256_file(parquet) != expected_parquet_hash:
        raise IntegrityError("active Phase 3 input parquet hash is invalid")
    return ActivePhase3Input(
        market=market,
        year=year,
        parquet_path=parquet_relative.as_posix(),
        parquet_sha256=expected_parquet_hash,
        sidecar_sha256=sha256_file(sidecar),
        causal_release_id=causal_release_id,
        source_raw_release_id=source_raw_release_id,
        active_view_id=active_view_id,
    )
