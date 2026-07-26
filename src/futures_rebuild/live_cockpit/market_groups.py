"""Validated market-group metadata for the cockpit sidebar."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


ALPHA_TIER_PROFILES = (
    ("tier_1_research", "tier_1_core", "Tier 1 · Core"),
    ("tier_2_research", "tier_2_additions", "Tier 2 · Additions"),
    ("tier_3_research", "tier_3_additions", "Tier 3 · Additions"),
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


def _profile_markets(
    profiles: Mapping[str, Any],
    market_sets: Mapping[str, Any],
    name: str,
) -> list[str] | None:
    profile = profiles.get(name)
    if not isinstance(profile, Mapping):
        return None
    values = profile.get("markets")
    if values is None:
        market_set = profile.get("market_set")
        if isinstance(market_set, str):
            values = market_sets.get(market_set)
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
    profiles = payload.get("profiles")
    market_sets = payload.get("market_sets")
    if not isinstance(profiles, Mapping) or not isinstance(market_sets, Mapping):
        return unavailable

    expected = {str(market).strip().upper() for market in expected_markets}
    tier_markets: list[list[str]] = []
    for profile_name, _group_id, _label in ALPHA_TIER_PROFILES:
        markets = _profile_markets(profiles, market_sets, profile_name)
        if markets is None:
            return unavailable
        tier_markets.append([market for market in markets if market in expected])

    tier_sets = [set(markets) for markets in tier_markets]
    if not tier_sets[0] <= tier_sets[1] <= tier_sets[2]:
        return unavailable
    if tier_sets[2] != expected:
        return unavailable

    assignments: dict[str, str] = {}
    groups: list[dict[str, Any]] = []
    previous: set[str] = set()
    for (_profile_name, group_id, label), markets, tier_set in zip(
        ALPHA_TIER_PROFILES, tier_markets, tier_sets, strict=True
    ):
        additions = [market for market in markets if market not in previous]
        for market in additions:
            assignments[market] = group_id
        groups.append(
            {
                "id": group_id,
                "label": label,
                "market_count": len(additions),
            }
        )
        previous = tier_set

    if set(assignments) != expected or sum(
        group["market_count"] for group in groups
    ) != len(expected):
        return unavailable
    return AlphaTierGrouping(True, assignments, tuple(groups))
