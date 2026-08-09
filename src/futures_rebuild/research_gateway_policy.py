"""Fail-closed operation policy for current real-history research.

Preparatory row access may build source-bound readiness evidence.  A strategy
execution is different: it must use the one certified gateway operation and
carry the exact immutable registration and readiness bindings in its receipt.
Historic operation names are intentionally rejected.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from .errors import UnauthorizedOperation


CERTIFIED_GATEWAY_SCHEMA = "certified_research_gateway/2.0.0"
CERTIFIED_TRIAL_EXECUTION_OPERATION = (
    "EXECUTE_CERTIFIED_TRIAL_HISTORICAL_SCREEN"
)
SOURCE_COMPATIBILITY_CENSUS_OPERATION = (
    "CENSUS_CASH_OPEN_41_MARKET_SOURCE_COMPATIBILITY_ONCE"
)
ALPHA_LADDER_READINESS_CENSUS_OPERATION = (
    "CENSUS_ALPHA_LADDER_PILOT_TIER1_READINESS_ONCE"
)
RETIRED_PRE_REGISTRATION_PROTOCOL_IDS = frozenset(
    {"3b8e09d65015afd33fc033aa72c8bb0be22425cafac8b8b145eeccb639258067"}
)
CERTIFIED_EXECUTION_SCOPE_KEYS = frozenset(
    {
        "gateway_schema",
        "operation_kind",
        "trial_id",
        "trial_family",
        "protocol_id",
        "registration_path",
        "registration_sha256",
        "readiness_certificate_id",
        "readiness_evidence_sha256",
        "alpha_ladder_contract_id",
        "alpha_ladder_profile_id",
        "alpha_ladder_stage",
        "mechanism_sha256",
        "predecessor_decision_sha256",
        "session_manifest_sha256",
        "pilot_evaluation_session_ids_sha256",
    }
)
_APPROVAL_SCOPE_KEYS = frozenset(
    {"approval_command", "approval_plan_id", "approval_plan_sha256"}
)
PREPARATORY_REAL_HISTORY_OPERATIONS = frozenset(
    {
        "ACQUIRE_APEX_MICRO_TIER01_RAW_DBN_INACTIVE_CUSTODY_ONCE",
        "ACQUIRE_APEX_MICRO_TIER01_RAW_DBN_INACTIVE_CUSTODY_V21_ONCE",
        "ACQUIRE_APEX_MICRO_TIER01_RAW_DBN_INACTIVE_CUSTODY_V22_ONCE",
        "ACQUIRE_FROZEN_TIER1_BBO_1S_RECOVERY_STAGING_AND_PUBLISH",
        "AUDIT_V12_LOCAL_CAUSAL_RELEASE_ALTERNATIVES_READ_ONLY",
        "AUDIT_V9_REGISTERED_SOURCE_DEPENDENCY_WINDOWS_READ_ONLY",
        ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        SOURCE_COMPATIBILITY_CENSUS_OPERATION,
        "CENSUS_FROZEN_TIER1_DIAGNOSTIC_EXECUTION_RECOVERY_AND_PUBLISH",
        "CENSUS_FROZEN_TIER1_LOCAL_CANONICAL_RECOVERY_AND_PUBLISH",
        "CENSUS_FROZEN_TIER1_REPORTED_BAR_SOURCE_ADEQUACY_AND_PUBLISH",
        "CENSUS_OVERNIGHT_REVERSAL_FOLD_READINESS_ONCE",
        "CENSUS_OVERNIGHT_REVERSAL_FOLD_READINESS_PARALLEL_ONCE",
        "CERTIFY_FROZEN_TIER1_SOURCE_SUFFICIENCY_AND_PUBLISH",
        "CLASSIFY_FROZEN_TIER1_DEPENDENCY_GAPS_AND_PUBLISH",
        "PUBLISH_RELEASE",
        "PUBLISH_V12_LOCAL_SOURCE_QUALITY_RECORD_CREATE_ONLY",
        "PREFLIGHT_APEX_MICRO_TIER01_DATABENTO_METADATA_ONCE",
        "QUOTE_FROZEN_TIER1_BBO_1S_RECOVERY_COST_AND_PUBLISH",
    }
)


def require_current_real_history_operation(
    operation: str, scope: Mapping[str, str],
) -> None:
    """Reject trial execution outside the certified gateway.

    Preparatory operations cannot promote, fit, predict, or evaluate a trial;
    their bounded implementations retain their own exact receipt checks.  All
    other real-history operations must be the current certified execution
    operation with the mandatory immutable identity fields.
    """

    if operation in PREPARATORY_REAL_HISTORY_OPERATIONS:
        return
    if operation != CERTIFIED_TRIAL_EXECUTION_OPERATION:
        raise UnauthorizedOperation(
            "real-history trial execution is retired outside the certified gateway"
        )
    research_scope = {
        key: value for key, value in scope.items() if key not in _APPROVAL_SCOPE_KEYS
    }
    missing = CERTIFIED_EXECUTION_SCOPE_KEYS - set(research_scope)
    if missing:
        raise UnauthorizedOperation(
            "certified historical execution scope is incomplete"
        )
    if (
        research_scope.get("gateway_schema") != CERTIFIED_GATEWAY_SCHEMA
        or research_scope.get("operation_kind") != "TRIAL_HISTORICAL_EXECUTION"
        or any(
            re.fullmatch(r"[0-9a-f]{64}", research_scope.get(key, "")) is None
            for key in (
                "trial_id",
                "registration_sha256",
                "readiness_certificate_id",
                "readiness_evidence_sha256",
                "alpha_ladder_contract_id",
                "alpha_ladder_profile_id",
                "mechanism_sha256",
                "predecessor_decision_sha256",
                "session_manifest_sha256",
                "pilot_evaluation_session_ids_sha256",
            )
        )
        or research_scope.get("alpha_ladder_stage")
        not in {"pilot", "tier_1", "tier_2", "tier_3", "holdout", "forward"}
        or not research_scope.get("trial_family")
        or not research_scope.get("protocol_id")
        or not research_scope.get("registration_path", "").startswith(
            "state/trial_registry/"
        )
    ):
        raise UnauthorizedOperation(
            "certified historical execution scope is invalid"
        )
