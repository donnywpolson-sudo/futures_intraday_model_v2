from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml


class ProfileContractError(ValueError):
    """Raised when the operational profile view drifts from its source contract."""


EXPECTED_SCHEMA = "futures_operational_profiles/1.0.0"
EXPECTED_CLASSIFICATION = "NON_AUTHORIZING_OPERATIONAL_VIEW"
REQUIRED_PROFILES = {
    "tier_0",
    "tier_1_research",
    "tier_1_holdout",
    "tier_1_forward",
    "tier_2_research",
    "tier_2_holdout",
    "tier_2_forward",
    "tier_3_research",
    "tier_3_holdout",
    "tier_3_forward",
    "all_raw",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ProfileContractError("profile file must contain one mapping")
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ProfileContractError("universe contract must contain one object")
    return payload


def validate_profiles(
    profile_path: Path,
    *,
    repository_root: Path | None = None,
) -> dict[str, Any]:
    profile_path = profile_path.resolve()
    root = (repository_root or profile_path.parent.parent).resolve()
    payload = _load_yaml(profile_path)
    if payload.get("schema_version") != EXPECTED_SCHEMA:
        raise ProfileContractError("unsupported profile schema")
    if payload.get("classification") != EXPECTED_CLASSIFICATION:
        raise ProfileContractError("profile view must remain non-authorizing")

    contract_reference = payload.get("canonical_universe_contract")
    if not isinstance(contract_reference, str) or not contract_reference:
        raise ProfileContractError("canonical universe contract is missing")
    universe_path = (root / contract_reference).resolve()
    try:
        universe_path.relative_to(root)
    except ValueError as exc:
        raise ProfileContractError("universe contract escapes the repository") from exc
    universe = _load_json(universe_path)

    tiers = universe.get("tiers")
    if not isinstance(tiers, list):
        raise ProfileContractError("canonical universe tiers are missing")
    canonical_by_tier = {
        item.get("tier_id"): tuple(item.get("symbols", ()))
        for item in tiers
        if isinstance(item, dict)
    }
    traditional = canonical_by_tier.get(3)
    satellite = canonical_by_tier.get(4)
    if traditional is None or satellite is None:
        raise ProfileContractError("canonical tier 3 or tier 4 is missing")
    if len(traditional) != 38 or len(satellite) != 3:
        raise ProfileContractError("canonical 38/3 market partition changed")
    canonical = tuple(dict.fromkeys((*traditional, *satellite)))
    if len(canonical) != 41:
        raise ProfileContractError("canonical universe is not exactly 41 markets")

    market_sets = payload.get("market_sets")
    profiles = payload.get("profiles")
    cohorts = payload.get("cohorts")
    authority = payload.get("authority")
    if not all(isinstance(item, dict) for item in (market_sets, profiles, cohorts, authority)):
        raise ProfileContractError("profile mappings are incomplete")
    assert isinstance(market_sets, dict)
    assert isinstance(profiles, dict)
    assert isinstance(cohorts, dict)
    assert isinstance(authority, dict)

    if set(profiles) != REQUIRED_PROFILES:
        raise ProfileContractError("profile name set changed")
    if any(value is not False for value in authority.values()):
        raise ProfileContractError("operational view cannot grant authority")

    canonical_set = set(canonical)
    for name, members in market_sets.items():
        if not isinstance(members, list) or not members:
            raise ProfileContractError(f"market set {name} must be a nonempty list")
        if len(members) != len(set(members)):
            raise ProfileContractError(f"market set {name} has duplicates")
        if not set(members).issubset(canonical_set):
            raise ProfileContractError(f"market set {name} expands the canonical universe")

    if tuple(market_sets.get("traditional", ())) != traditional:
        raise ProfileContractError("traditional market set drifted")
    if tuple(market_sets.get("satellite", ())) != satellite:
        raise ProfileContractError("satellite market set drifted")
    if tuple(market_sets.get("all_approved", ())) != canonical:
        raise ProfileContractError("all-approved market order drifted")

    for name, definition in profiles.items():
        if not isinstance(definition, dict):
            raise ProfileContractError(f"profile {name} must be a mapping")
        market_set = definition.get("market_set")
        cohort = definition.get("cohort")
        if market_set not in market_sets or cohort not in cohorts:
            raise ProfileContractError(f"profile {name} references an unknown view")
        explicit_markets = definition.get("markets")
        if explicit_markets is not None:
            if not isinstance(explicit_markets, list) or not set(explicit_markets).issubset(
                set(market_sets[market_set])
            ):
                raise ProfileContractError(f"profile {name} expands its market set")
        if name.endswith("_holdout") and cohort != "holdout":
            raise ProfileContractError(f"{name} is not bound to the locked holdout")
        if name.endswith("_forward") and cohort != "forward":
            raise ProfileContractError(f"{name} is not bound to the locked forward cohort")
        if name.startswith("tier_3"):
            reporting = definition.get("reporting")
            if market_set != "all_approved" or not isinstance(reporting, dict):
                raise ProfileContractError(f"{name} lost separate full-universe reporting")
            if reporting.get("satellite_can_rescue_traditional_failure") is not False:
                raise ProfileContractError("satellite results cannot rescue traditional failure")

    for name in ("holdout", "forward"):
        cohort = cohorts.get(name)
        if not isinstance(cohort, dict) or cohort.get("locked") is not True:
            raise ProfileContractError(f"{name} cohort must remain locked")
        if cohort.get("selection_eligible") is not False:
            raise ProfileContractError(f"{name} cohort cannot be selection eligible")

    cockpit = payload.get("cockpit")
    if (
        not isinstance(cockpit, dict)
        or cockpit.get("market_set") != "all_approved"
        or cockpit.get("behavior") != "OBSERVATION_ONLY"
        or cockpit.get("provider_smoke_authorized") is not False
        or cockpit.get("order_paths_authorized") is not False
    ):
        raise ProfileContractError("cockpit profile is not safely observation-only")

    return {
        "schema_version": EXPECTED_SCHEMA,
        "classification": EXPECTED_CLASSIFICATION,
        "profile_sha256": _sha256(profile_path),
        "universe_contract_sha256": _sha256(universe_path),
        "universe_contract_id": universe.get("approval_receipt_id"),
        "market_count": len(canonical),
        "traditional_market_count": len(traditional),
        "satellite_market_count": len(satellite),
        "profiles": sorted(profiles),
        "authority": dict(authority),
    }
