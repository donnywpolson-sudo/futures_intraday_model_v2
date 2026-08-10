"""Prepare the Apex micro Phase 1B/2 boundary without opening source rows.

The contracts in this module describe what a later, separately authorized
decoder and causal-foundation build must prove.  They intentionally expose no
DBN reader, provider client, catalog writer, registration surface, or research
evaluation path.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Final

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .micro_alpha_pipeline import (
    LANE_ID,
    SCHEMAS,
    SEALED_HOLDOUT_YEAR,
    TIER_1_MARKETS,
    phase1b_role,
)
from .micro_alpha_product_effective_dates import (
    M6E_REPORT_PATH,
    REMAINING_REPORT_PATH,
    load_official_product_effective_dates,
)


ACQUISITION_PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_phase1a_acquisition_plan_v24.json"
)
CUSTODY_TERMINAL_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_v24_custody_repair_v2/terminal.json"
)
MICRO_CONTRACT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_ladder_preparation_v2/"
    "234eccff53c6620f2f54e73c88165574531f434b441ae808dd36c2f75d1927c8/"
    "universe_contract.json"
)
STANDARD_RISK_POLICY_PATH: Final = Path(
    "configs/apex_tradovate_50k_eod_risk_policy.json"
)
ACTIVE_MICRO_POINTER_PATH: Final = Path(
    "configs/active_micro_alpha_research_ladder.json"
)
ACTIVE_MICRO_CATALOG_PATH: Final = Path("data/active/catalogs/apex_micro.json")
OUTPUT_PATH: Final = Path("configs/apex_micro_phase1b2_prepare_only_contract_v1.json")


SCHEMA_DECODER_CONTRACTS: Final = {
    "definition": {
        "role": "CONTRACT_IDENTITY_REFERENCE",
        "output_family": "definitions.parquet",
        "required_semantics": [
            "ACTUAL_INSTRUMENT_ID",
            "RAW_CONTRACT_SYMBOL",
            "CONTRACT_EXPIRY_OR_TERMINATION",
            "TICK_SIZE",
            "CURRENCY",
            "UNIT_OF_MEASURE_AND_QUANTITY",
            "POINT_VALUE_OR_EQUIVALENT_CONTRACT_MULTIPLIER",
        ],
        "feature_eligible": False,
        "diagnostic_only": False,
    },
    "status": {
        "role": "MARKET_STATE_DIAGNOSTIC",
        "output_family": "status.parquet",
        "required_semantics": [
            "ACTUAL_INSTRUMENT_ID",
            "EVENT_TIMESTAMP",
            "STATUS_ACTION_AND_REASON_PRESERVED",
        ],
        "feature_eligible": False,
        "diagnostic_only": True,
    },
    "statistics": {
        "role": "MARKET_STATE_DIAGNOSTIC",
        "output_family": "statistics.parquet",
        "required_semantics": [
            "ACTUAL_INSTRUMENT_ID",
            "EVENT_TIMESTAMP",
            "STATISTIC_TYPE_AND_VALUE_PRESERVED",
        ],
        "feature_eligible": False,
        "diagnostic_only": True,
    },
    "ohlcv-1m": {
        "role": "CAUSAL_FEATURE_FOUNDATION_INPUT",
        "output_family": "bars.parquet",
        "required_semantics": [
            "ACTUAL_INSTRUMENT_ID",
            "INTERVAL_OPEN_TIMESTAMP",
            "OHLCV_WITH_PROVIDER_NULLABILITY_PRESERVED",
            "CAUSAL_AVAILABILITY_TIMESTAMP",
        ],
        "feature_eligible": True,
        "diagnostic_only": False,
    },
    "ohlcv-1s": {
        "role": "CAUSAL_EXECUTION_EVIDENCE_INPUT",
        "output_family": "reported_trade_bars.parquet",
        "required_semantics": [
            "ACTUAL_INSTRUMENT_ID",
            "INTERVAL_OPEN_TIMESTAMP",
            "OHLCV_WITH_PROVIDER_NULLABILITY_PRESERVED",
            "CAUSAL_AVAILABILITY_TIMESTAMP",
        ],
        "feature_eligible": False,
        "diagnostic_only": False,
    },
}


EXPLICIT_COVERAGE_DISPOSITIONS: Final = (
    "ACCEPTED",
    "MISSING",
    "SPARSE",
    "DUPLICATE",
    "PRODUCT_NOT_YET_EFFECTIVE",
    "AMBIGUOUS_IDENTITY",
    "AMBIGUOUS_ROLL",
    "SOURCE_UNAVAILABLE",
    "SEALED_HOLDOUT_CUSTODY_ONLY",
    "FORWARD_PRE_FREEZE_CUSTODY_ONLY",
)


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is unavailable or invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{description} is not an object")
    return value


def _self_hash(value: Mapping[str, object], *, id_key: str, description: str) -> str:
    core = dict(value)
    observed = core.pop(id_key, None)
    if type(observed) is not str or observed != sha256_json(core):
        raise IntegrityError(f"{description} identity drifted")
    return observed


def decoder_contract(schema: str) -> dict[str, object]:
    """Return one immutable logical decoder contract without decoding rows."""

    if schema not in SCHEMA_DECODER_CONTRACTS:
        raise IntegrityError("micro decoder schema is not approved")
    contract = dict(SCHEMA_DECODER_CONTRACTS[schema])
    contract["required_semantics"] = list(contract["required_semantics"])
    if contract["role"] != phase1b_role(schema):
        raise IntegrityError("micro decoder role drifted")
    return contract


def year_decode_disposition(*, year: int) -> str:
    """Classify custody years without granting permission to read them."""

    if year < 2018 or year > 2026:
        raise IntegrityError("micro custody year is outside the prepared scope")
    if year == SEALED_HOLDOUT_YEAR:
        return "SEALED_HOLDOUT_CUSTODY_ONLY"
    if year > SEALED_HOLDOUT_YEAR:
        return "FORWARD_PRE_FREEZE_CUSTODY_ONLY"
    return "HISTORICAL_ROW_APPROVAL_REQUIRED"


def require_row_certified_catalog_candidate(candidate: Mapping[str, object]) -> None:
    """Validate a future inactive catalog candidate; never publish or activate it."""

    required = {
        "lane_id",
        "contract_scale",
        "state",
        "source_certification_id",
        "source_certification_sha256",
        "coverage_census_id",
        "coverage_cell_count",
        "phase1b_release_id",
        "phase1b_release_sha256",
        "phase2_release_id",
        "phase2_release_sha256",
        "markets",
        "years",
        "disposition_census_complete",
        "actual_identity_and_roll_continuity_certified",
        "holdout_2025_materialized",
        "forward_2026_materialized",
    }
    if set(candidate) != required:
        raise UnauthorizedOperation("micro catalog candidate bindings are incomplete")
    if (
        candidate.get("lane_id") != LANE_ID
        or candidate.get("contract_scale") != "MICRO_INTEGER_ONLY"
        or candidate.get("state") != "CERTIFIED_INACTIVE_NOT_PUBLISHED"
        or candidate.get("markets") != list(TIER_1_MARKETS)
        or candidate.get("years") != list(range(2018, 2025))
        or candidate.get("disposition_census_complete") is not True
        or candidate.get("coverage_cell_count") != 140
        or candidate.get("actual_identity_and_roll_continuity_certified") is not True
        or candidate.get("holdout_2025_materialized") is not False
        or candidate.get("forward_2026_materialized") is not False
    ):
        raise UnauthorizedOperation("micro catalog candidate crosses a certification gate")
    for key in (
        "source_certification_id",
        "source_certification_sha256",
        "coverage_census_id",
        "phase1b_release_id",
        "phase1b_release_sha256",
        "phase2_release_id",
        "phase2_release_sha256",
    ):
        value = candidate.get(key)
        if type(value) is not str or len(value) != 64:
            raise UnauthorizedOperation("micro catalog candidate lacks an exact hash binding")


def build_prepare_only_contract(*, root: Path) -> dict[str, object]:
    """Bind source-safe Phase 1B/2 requirements to the completed custody evidence."""

    root = root.resolve(strict=True)
    plan = _object(root / ACQUISITION_PLAN_PATH, "micro acquisition plan")
    terminal = _object(root / CUSTODY_TERMINAL_PATH, "micro custody repair terminal")
    micro_contract = _object(root / MICRO_CONTRACT_PATH, "micro ladder contract")
    standard_risk = _object(root / STANDARD_RISK_POLICY_PATH, "standard risk policy")
    plan_id = _self_hash(plan, id_key="plan_id", description="micro acquisition plan")
    terminal_id = _self_hash(
        terminal, id_key="terminal_id", description="micro custody repair terminal"
    )
    contract_id = _self_hash(
        micro_contract, id_key="contract_id", description="micro ladder contract"
    )
    risk_policy_id = _self_hash(
        standard_risk, id_key="policy_id", description="standard risk policy"
    )
    dates = load_official_product_effective_dates(root=root)

    if (
        plan.get("markets") != list(TIER_1_MARKETS)
        or plan.get("schemas") != list(SCHEMAS)
        or plan.get("file_partition")
        != "ONE_DBN_AND_ADJACENT_SIDECAR_PER_MARKET_SCHEMA_CALENDAR_YEAR"
        or not isinstance(plan.get("requests"), list)
        or len(plan["requests"]) != 160
    ):
        raise IntegrityError("micro acquisition scope or annual partition drifted")
    limits = plan.get("limits")
    if not isinstance(limits, Mapping) or (
        limits.get("exact_request_count") != 160
        or limits.get("maximum_dbn_files") != 160
        or limits.get("maximum_sidecars") != 160
        or limits.get("maximum_external_cost_usd") != "0"
        or limits.get("maximum_retries") != 0
    ):
        raise IntegrityError("micro acquisition limits drifted")
    if (
        terminal.get("state") != "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY_REPAIRED"
        or terminal.get("completed_alias_removal_count") != 320
        or terminal.get("failure") is not None
        or terminal.get("provider_calls") != 0
        or terminal.get("automatic_retries") != 0
        or terminal.get("dbn_rows_decoded") != 0
        or terminal.get("payloads_opened_for_row_access") != 0
        or terminal.get("year_2025_or_2026_payloads_opened_for_row_access") != 0
        or terminal.get("catalog_or_pointer_activated") is not False
        or terminal.get("terminal_written_last") is not True
    ):
        raise IntegrityError("micro custody terminal does not prove the required safe state")
    if (
        micro_contract.get("lane_id") != LANE_ID
        or micro_contract.get("state") != "PREPARED_NOT_PUBLISHED_NOT_ACTIVE"
        or standard_risk.get("account", {}).get("full_contracts_only") is not True
    ):
        raise IntegrityError("lane or risk-policy boundary drifted")
    if (root / ACTIVE_MICRO_POINTER_PATH).exists() or (
        root / ACTIVE_MICRO_CATALOG_PATH
    ).exists():
        raise UnauthorizedOperation("micro pointer or catalog became active before certification")

    decoder_contracts = {
        schema: decoder_contract(schema) for schema in SCHEMAS
    }
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1b2_prepare_only_contract/1.0.0",
        "state": "PREPARED_NOT_EXECUTED_HISTORICAL_ROW_APPROVAL_REQUIRED",
        "lane_id": LANE_ID,
        "markets": list(TIER_1_MARKETS),
        "schemas": list(SCHEMAS),
        "bindings": {
            ACQUISITION_PLAN_PATH.as_posix(): {
                "artifact_id": plan_id,
                "sha256": sha256_file(root / ACQUISITION_PLAN_PATH),
            },
            CUSTODY_TERMINAL_PATH.as_posix(): {
                "artifact_id": terminal_id,
                "sha256": sha256_file(root / CUSTODY_TERMINAL_PATH),
            },
            MICRO_CONTRACT_PATH.as_posix(): {
                "artifact_id": contract_id,
                "sha256": sha256_file(root / MICRO_CONTRACT_PATH),
            },
            M6E_REPORT_PATH.as_posix(): sha256_file(root / M6E_REPORT_PATH),
            REMAINING_REPORT_PATH.as_posix(): sha256_file(
                root / REMAINING_REPORT_PATH
            ),
            STANDARD_RISK_POLICY_PATH.as_posix(): {
                "artifact_id": risk_policy_id,
                "sha256": sha256_file(root / STANDARD_RISK_POLICY_PATH),
            },
        },
        "product_effective_dates": dates,
        "decoder_contracts": decoder_contracts,
        "phase1b": {
            "decode_scope": "ONE_MARKET_SCHEMA_CALENDAR_YEAR_DBN_AT_A_TIME",
            "create_only_content_addressed_releases": True,
            "source_dbn_and_sidecar_hashes_required": True,
            "definition_routes_identity_and_economics": True,
            "status_and_statistics_are_diagnostic_only": True,
            "ohlcv_1m_routes_feature_foundation": True,
            "ohlcv_1s_routes_execution_foundation": True,
            "no_downloaded_row_is_research_evidence_automatically": True,
        },
        "identity_and_roll": {
            "actual_instrument_id_required_on_every_decoded_record": True,
            "definition_interval_join_required": True,
            "continuous_rank_zero_transition_receipt_required": True,
            "roll_gap_overlap_and_ambiguity_are_explicit": True,
            "parent_calendar_or_economics_inheritance_without_mapping_forbidden": True,
        },
        "causal_availability": {
            "event_time_and_available_time_preserved_separately": True,
            "feature_time_must_be_strictly_after_required_source_availability": True,
            "fold_transforms_fit_on_training_only": True,
            "purge_and_embargo_required": True,
            "future_known_status_or_statistics_forbidden": True,
        },
        "coverage": {
            "allowed_dispositions": list(EXPLICIT_COVERAGE_DISPOSITIONS),
            "every_expected_market_schema_year_requires_one_disposition": True,
            "missing_sparse_duplicate_prelaunch_and_ambiguous_never_silently_dropped": True,
            "fabricated_empty_outputs_forbidden": True,
        },
        "one_second_execution_semantics": {
            "evidence": "REPORTED_TRADE_BARS_ONLY",
            "cannot_prove": [
                "BBO_AVAILABILITY",
                "QUEUE_PRIORITY",
                "GUARANTEED_MARKET_ORDER_EXECUTION",
                "PRECISE_WITHIN_SECOND_TICK_ORDERING",
            ],
            "entry_after_decision_and_causal_availability": True,
            "same_bar_ambiguity": "CONSERVATIVE_ADVERSE_OR_UNFILLED",
            "explicit_states": ["UNFILLED", "NO_TRIGGER"],
            "baselines_scheduled_independently": True,
            "stress_costs_locked_before_outcomes": True,
            "missing_or_sparse_checkpoints_never_removed": True,
        },
        "inactive_catalog_certification": {
            "future_path": ACTIVE_MICRO_CATALOG_PATH.as_posix(),
            "candidate_state": "CERTIFIED_INACTIVE_NOT_PUBLISHED",
            "requires_row_certified_phase1b_and_phase2_receipts": True,
            "requires_complete_identity_roll_and_disposition_census": True,
            "excludes_holdout_2025_and_forward_2026_materialization": True,
            "directory_presence_never_grants_activation": True,
            "activation_is_a_later_separate_boundary": True,
        },
        "certified_research_gateway_preparation": {
            "lane_binding_required": True,
            "lane_id": LANE_ID,
            "contract_id": contract_id,
            "future_catalog_path": ACTIVE_MICRO_CATALOG_PATH.as_posix(),
            "contract_scale": "MICRO_INTEGER_ONLY",
            "catalog_must_be_certified_and_active_before_registration": True,
            "shared_2025_holdout_claim_with_standard_lane": True,
            "registration_or_execution_authorized": False,
        },
        "apex_cost_and_risk_gates": {
            "standard_full_contract_policy_reuse_for_micro_forbidden": True,
            "micro_integer_contract_size_required": 1,
            "official_micro_commission_verification": {
                market: "UNRESOLVED_FAIL_CLOSED_BEFORE_MECHANISM_FREEZE"
                for market in TIER_1_MARKETS
            },
            "owner_r_and_emergency_reserve_required_before_mechanism": True,
            "locked_stress_slippage_required_before_outcomes": True,
            "risk_control_value_case": {
                "risk_prevented": "FULL_CONTRACT_COSTS_OR_LIMITS_SILENTLY_APPLIED_TO_MICROS",
                "decision_improved": "SEPARATES_SOURCE_CERTIFICATION_FROM_TRADING_ELIGIBILITY",
                "why_existing_rule_is_not_enough": (
                    "BOUND_STANDARD_POLICY_EXPLICITLY_STATES_FULL_CONTRACTS_ONLY"
                ),
            },
        },
        "year_decode_dispositions": {
            str(year): year_decode_disposition(year=year)
            for year in range(2018, 2027)
        },
        "authority": {
            "dbn_row_read": False,
            "year_2025_or_2026_payload_read": False,
            "phase1b_decode": False,
            "phase2_construction": False,
            "catalog_write": False,
            "catalog_activation": False,
            "registration": False,
            "evaluation": False,
            "publication": False,
            "trading": False,
        },
        "next_boundary": (
            "SEPARATE_HISTORICAL_ROW_READ_APPROVAL_FOR_PHASE1B_DECODING_"
            "PHASE2_CONSTRUCTION_SOURCE_CERTIFICATION_AND_INACTIVE_CATALOG_PREPARATION"
        ),
    }
    return {**core, "contract_id": sha256_json(core)}


def validate_prepare_only_contract(value: Mapping[str, object], *, root: Path) -> None:
    if dict(value) != build_prepare_only_contract(root=root):
        raise IntegrityError("micro Phase 1B/2 prepare-only contract drifted")


__all__ = [
    "ACTIVE_MICRO_CATALOG_PATH",
    "ACTIVE_MICRO_POINTER_PATH",
    "EXPLICIT_COVERAGE_DISPOSITIONS",
    "OUTPUT_PATH",
    "SCHEMA_DECODER_CONTRACTS",
    "build_prepare_only_contract",
    "decoder_contract",
    "require_row_certified_catalog_candidate",
    "validate_prepare_only_contract",
    "year_decode_disposition",
]
