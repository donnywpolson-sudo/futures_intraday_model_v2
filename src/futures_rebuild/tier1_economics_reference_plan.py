"""Non-authorizing reference plan required before Tier 1 Phase 8 economics."""

from __future__ import annotations

from pathlib import Path

from .canonical import canonical_bytes, sha256_json
from .errors import IntegrityError


MARKETS = "ALL_41_ADMITTED_MARKETS"
REQUIRED_FIELDS = (
    "actual_identity_hash", "asset_class", "currency", "effective_at",
    "available_at", "point_value", "quote_convention_id", "tick_size",
    "tick_value", "verification_source_ids",
)


def build_tier1_economics_reference_plan(*, root: Path) -> dict[str, object]:
    """Describe immutable economics evidence; do not discover, fetch, or publish it."""
    prediction_manifest = root / "manifests/data_releases/predictions/c65d3da960c025f09d28be8907e884cb10eb39b2ffe54aeb503581257d64c31a.json"
    if not prediction_manifest.is_file():
        raise IntegrityError("Tier 1 Phase 6 prediction manifest is required")
    core = {
        "schema_version": "tier1_economics_reference_plan/1.0.0",
        "markets": MARKETS,
        "phase8_prerequisite": "PASSING_ALL_41_DATABENTO_ECONOMICS_AUDIT_AND_VERIFIED_PHASE8_ACTUAL_CONTRACT_ECONOMICS_INDEX",
        "prediction_manifest": prediction_manifest.relative_to(root).as_posix(),
        "required_contract_fields": list(REQUIRED_FIELDS),
        "verification_rules": [
            "one actual-identity record per observed contract",
            "positive point, tick, and tick-value consistency",
            "available_at is not before effective_at",
            "passing all-market Databento signature audit bound to the protected rulebook",
            "CME document evidence only for an audit conflict or unresolved exception",
            "no ambiguous or missing economics records",
        ],
        "future_phase8_inputs": ["fees", "spread_slippage", "delay", "point_value", "tick_value", "margin", "concentration_limits"],
        "authority": {"provider_access": False, "economics_publication": False, "phase8_evaluation": False},
    }
    return {**core, "plan_id": sha256_json(core)}


def write_tier1_economics_reference_plan(*, root: Path) -> dict[str, object]:
    plan = build_tier1_economics_reference_plan(root=root)
    path = root / "reports/economics_reference/tier1" / f"{plan['plan_id']}.json"
    if path.exists():
        raise IntegrityError("economics reference report already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(plan) + b"\n")
    return {"plan_id": plan["plan_id"], "report_path": path.relative_to(root).as_posix()}
