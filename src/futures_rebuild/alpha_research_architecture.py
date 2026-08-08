"""Dual-lane Alpha architecture contracts without activation authority."""

from __future__ import annotations

from collections.abc import Mapping

from .canonical import sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .micro_alpha_pipeline import (
    LANE_ID as MICRO_LANE,
    SATELLITES as MICRO_SATELLITES,
    TIER_0_MARKETS as MICRO_TIER_0,
    TIER_1_MARKETS as MICRO_TIER_1,
    TIER_2_ADDITIONS as MICRO_TIER_2_ADDITIONS,
    TIER_2_MARKETS as MICRO_TIER_2,
    TIER_3_MARKETS as MICRO_TIER_3,
)


STANDARD_LANE = "standard_full_contract_41"
SCHEMA = "alpha_research_architecture/2.0.0"
MICRO_POINTER_PATH = "configs/active_micro_alpha_research_ladder.json"


def build_architecture_contract(
    *, standard_pointer_sha256: str, standard_contract_id: str,
    micro_contract_id: str, micro_profile_id: str, micro_pointer_id: str,
) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": SCHEMA,
        "state": "PREPARED_NOT_PUBLISHED_NOT_ACTIVE",
        "lanes": {
            STANDARD_LANE: {
                "contract_id": standard_contract_id,
                "active_pointer_path": "configs/active_alpha_research_ladder.json",
                "active_pointer_sha256": standard_pointer_sha256,
                "catalog_path": "data/active/catalog.json",
                "contract_scale": "STANDARD_FULL_CONTRACT",
                "display_name": "standard/full-contract lane",
            },
            MICRO_LANE: {
                "contract_id": micro_contract_id,
                "profile_id": micro_profile_id,
                "prepared_pointer_id": micro_pointer_id,
                "active_pointer_path": MICRO_POINTER_PATH,
                "catalog_path": "data/active/catalogs/apex_micro.json",
                "contract_scale": "MICRO_INTEGER_ONLY",
                "display_name": "Apex integer-micro lane",
                "activation_condition": "PHASE2_SOURCE_CERTIFICATION_PASS",
            },
        },
        "rules": {
            "registration_must_bind_lane": True,
            "cross_lane_catalog_resolution_forbidden": True,
            "cross_lane_promotion_evidence_forbidden": True,
            "holdout_year": 2025,
            "holdout_claim_is_shared_project_level_across_lanes": True,
            "contract_scale_change_never_grants_an_additional_holdout_access": True,
            "forward_starts_after_exact_mechanism_freeze": True,
        },
        "authority": {
            "publication": False,
            "activation": False,
            "historical_rows": False,
            "provider_access": False,
            "registration": False,
        },
    }
    return {**core, "architecture_id": sha256_json(core)}


def build_prepared_micro_pointer(
    *, contract_path: str, contract_sha256: str, contract_id: str,
    profile_path: str, profile_sha256: str, profile_id: str,
) -> dict[str, object]:
    """Describe a future pointer without writing the active pointer path."""

    core: dict[str, object] = {
        "schema_version": "prepared_micro_alpha_pointer/1.0.0",
        "state": "PREPARED_NOT_ACTIVE",
        "lane_id": MICRO_LANE,
        "future_active_path": MICRO_POINTER_PATH,
        "contract_path": contract_path,
        "contract_sha256": contract_sha256,
        "contract_id": contract_id,
        "profile_path": profile_path,
        "profile_sha256": profile_sha256,
        "profile_id": profile_id,
        "catalog_path": "data/active/catalogs/apex_micro.json",
        "activation_requires": "PHASE2_SOURCE_CERTIFICATION_PASS",
    }
    return {**core, "pointer_id": sha256_json(core)}


def build_micro_contract() -> dict[str, object]:
    traditional = list(MICRO_TIER_2)
    core: dict[str, object] = {
        "schema_version": "micro_alpha_ladder_contract/2.0.0",
        "lane_id": MICRO_LANE,
        "state": "PREPARED_NOT_PUBLISHED_NOT_ACTIVE",
        "contract_scale": "MICRO_INTEGER_ONLY",
        "tiers": {
            "tier_0": {"markets": list(MICRO_TIER_0), "roles": ["SYNTHETIC_ENGINEERING", "ES_EQUIVALENT_PILOT"]},
            "tier_1": {
                "markets": list(MICRO_TIER_1),
                "role": "FOUR_MARKET_CORE_CONFIRMATION",
                "represented_families": ["EQUITY", "ENERGY", "METALS", "FX"],
            },
            "tier_2": {
                "markets": list(MICRO_TIER_2),
                "four_market_core": list(MICRO_TIER_1),
                "five_additions": list(MICRO_TIER_2_ADDITIONS),
                "report_cohorts_separately": True,
                "role": "NINE_TRADITIONAL_MICRO_REPLICATION",
            },
            "tier_3": {
                "markets": list(MICRO_TIER_3),
                "traditional": traditional,
                "satellites": list(MICRO_SATELLITES),
                "report_traditional_and_crypto_satellites_separately": True,
                "traditional_must_pass_independently": True,
                "satellites_cannot_rescue_traditional": True,
            },
        },
        "cohorts": {
            "research": {"end_year": 2024, "product_effective_dates_required": True},
            "holdout": {
                "year": 2025, "sealed": True,
                "shared_project_level_claim_with_standard_lane": True,
                "raw_inactive_custody_permitted": True,
                "decode_features_execution_and_activation_forbidden": True,
            },
            "forward": {
                "starts_after_exact_mechanism_freeze_timestamp": True,
                "calendar_year_alone_is_insufficient": True,
                "raw_inactive_custody_permitted": True,
                "pre_freeze_decoding_forbidden": True,
                "monitoring_only": True,
            },
        },
        "sources": {
            "dataset": "GLBX.MDP3",
            "schemas": ["definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"],
            "schema_collective_name": "required Databento Standard historical schemas",
            "collective_raw_l0_label_forbidden": True,
            "feature_schema": "ohlcv-1m",
            "execution_schema": "ohlcv-1s",
            "catalog_path": "data/active/catalogs/apex_micro.json",
            "catalog_must_not_exist_until_phase2_certification": True,
        },
        "selection": {
            "current_acquisition_scope": list(MICRO_TIER_1),
            "returns_predictions_or_observed_strategy_performance_used": False,
            "zn_micro_equivalent_invented": False,
            "rates_addition_requires_preoutcome_apex_provider_and_economics_verification": True,
        },
        "authority": {"provider": False, "row_read": False, "registration": False, "trading": False},
    }
    return {**core, "contract_id": sha256_json(core)}


def build_micro_profile(*, contract_id: str) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": "micro_alpha_ladder_profile/2.0.0",
        "lane_id": MICRO_LANE,
        "contract_id": contract_id,
        "state": "PREPARED_NOT_ACTIVE",
        "tier_0": list(MICRO_TIER_0),
        "tier_1": list(MICRO_TIER_1),
        "tier_2": list(MICRO_TIER_2),
        "tier_3": list(MICRO_TIER_3),
    }
    return {**core, "profile_id": sha256_json(core)}


def validate_lane_binding(
    binding: Mapping[str, object], *, expected_lane: str, expected_contract_id: str,
    expected_catalog_path: str,
) -> None:
    if (
        binding.get("lane_id") != expected_lane
        or binding.get("contract_id") != expected_contract_id
        or binding.get("catalog_path") != expected_catalog_path
    ):
        raise UnauthorizedOperation("registration crosses an Alpha research lane boundary")


def validate_micro_contract(contract: Mapping[str, object]) -> None:
    rebuilt = build_micro_contract()
    if dict(contract) != rebuilt:
        raise IntegrityError("micro Alpha ladder contract drifted")
