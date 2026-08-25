"""Validated market-group metadata for the cockpit sidebar."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


ALPHA_TIER_MARKET_SETS = (
    ("core", "tier_1_core", "Tier 1 · Core confirmation"),
    ("balanced", "tier_2_additions", "Tier 2 · Balanced additions"),
    (
        "traditional",
        "tier_3_traditional_additions",
        "Tier 3 · Traditional additions",
    ),
    ("satellite", "tier_3_satellites", "Tier 3 · Satellite stress"),
)


@dataclass(frozen=True)
class AlphaTierGrouping:
    available: bool
    market_groups: Mapping[str, str]
    groups: tuple[dict[str, Any], ...]

    def capability_payload(self) -> dict[str, Any]:
        return {
            "alpha_tiers_available": self.available,
            "alpha_tier_groups": [dict(group) for group in self.groups],
        }


def _market_set_markets(market_sets: Mapping[str, Any], name: str) -> list[str] | None:
    values = market_sets.get(name)
    if not isinstance(values, list):
        return None
    markets: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            return None
        market = value.strip().upper()
        if market in markets:
            return None
        markets.append(market)
    return markets


def load_alpha_tier_grouping(
    path: Path, expected_markets: Sequence[str]
) -> AlphaTierGrouping:
    """Return an earliest-tier partition, or a sanitized unavailable result."""

    unavailable = AlphaTierGrouping(False, {}, ())
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, yaml.YAMLError):
        return unavailable
    if not isinstance(payload, Mapping):
        return unavailable
    market_sets = payload.get("market_sets")
    if not isinstance(market_sets, Mapping):
        return unavailable

    expected = {str(market).strip().upper() for market in expected_markets}
    configured: list[list[str]] = []
    for market_set_name, _group_id, _label in ALPHA_TIER_MARKET_SETS:
        markets = _market_set_markets(market_sets, market_set_name)
        if markets is None:
            return unavailable
        configured.append(markets)

    core, balanced, traditional, satellite = configured
    core_set, balanced_set, traditional_set, satellite_set = map(set, configured)
    if not core_set <= balanced_set <= traditional_set:
        return unavailable
    if traditional_set & satellite_set:
        return unavailable
    if traditional_set | satellite_set != expected:
        return unavailable

    additions = (
        core,
        [market for market in balanced if market not in core_set],
        [market for market in traditional if market not in balanced_set],
        satellite,
    )
    assignments: dict[str, str] = {}
    groups: list[dict[str, Any]] = []
    for (_market_set_name, group_id, label), markets in zip(
        ALPHA_TIER_MARKET_SETS, additions, strict=True
    ):
        for market in markets:
            assignments[market] = group_id
        groups.append(
            {
                "id": group_id,
                "label": label,
                "market_count": len(markets),
            }
        )

    if set(assignments) != expected or sum(
        group["market_count"] for group in groups
    ) != len(expected):
        return unavailable
    return AlphaTierGrouping(True, assignments, tuple(groups))
