from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.research_gateway_policy import (
    CERTIFIED_GATEWAY_SCHEMA,
    CERTIFIED_TRIAL_EXECUTION_OPERATION,
    SOURCE_COMPATIBILITY_CENSUS_OPERATION,
    require_current_real_history_operation,
)
from futures_rebuild.tier1_bracket_v4 import execute_authorized_v4
from futures_rebuild.trial import TrialRegistry


ROOT = Path(__file__).resolve().parents[1]
RETIRED_TRIAL_OPERATIONS = (
    "RUN_TRIAL_106",
    "RUN_OVERNIGHT_INVENTORY_REVERSAL_2018_2022_ONCE",
    "EXECUTE_TIER1_BRACKET_SUCCESSOR_V5_HISTORICAL_SCREEN",
    "EXECUTE_TIER1_BRACKET_SUCCESSOR_V6_HISTORICAL_SCREEN",
    "EXECUTE_TIER1_BRACKET_SUCCESSOR_V7_HISTORICAL_SCREEN",
    "EXECUTE_TIER1_BRACKET_SUCCESSOR_V8_HISTORICAL_SCREEN",
    "EXECUTE_TIER1_BRACKET_SUCCESSOR_V9_HISTORICAL_SCREEN",
    "EXECUTE_TIER1_BRACKET_SUCCESSOR_V10_HISTORICAL_SCREEN",
    "EXECUTE_TIER1_BRACKET_SUCCESSOR_V11_HISTORICAL_SCREEN",
    "EXECUTE_TIER1_BRACKET_SUCCESSOR_V12_HISTORICAL_SCREEN",
    "EXECUTE_TIER1_STANDARD_ONLY_HISTORICAL_SCREEN",
    "EXECUTE_FINAL_TIER1_HISTORICAL_SCREEN_AND_STAGE_UNPUBLISHED_EVIDENCE",
    "EXECUTE_AUTHORITATIVE_TIER1_HISTORICAL_SCREEN_AND_STAGE_UNPUBLISHED_EVIDENCE",
    "UNLOCK_FINAL_HOLDOUT",
    "AUDIT_CASH_OPEN_EXACT_LOCAL_DEPENDENCIES_ONCE",
    "AUDIT_CASH_OPEN_EXACT_LOCAL_DEPENDENCIES_SESSIONLESS_SAFE_ONCE",
    "CENSUS_CASH_OPEN_IMPULSE_FOLD_READINESS_ONCE",
    "CENSUS_CASH_OPEN_IMPULSE_FOLD_READINESS_HOST_SUCCESSOR_ONCE",
)


@pytest.mark.parametrize("operation", RETIRED_TRIAL_OPERATIONS)
def test_every_retired_trial_operation_fails_closed(operation: str) -> None:
    with pytest.raises(UnauthorizedOperation, match="retired outside"):
        require_current_real_history_operation(operation, {})


def test_certified_operation_requires_every_immutable_binding() -> None:
    scope = {
        "gateway_schema": CERTIFIED_GATEWAY_SCHEMA,
        "operation_kind": "TRIAL_HISTORICAL_EXECUTION",
        "trial_id": "1" * 64,
        "trial_family": "future",
        "protocol_id": "protocol",
        "registration_path": "state/trial_registry/future/" + "1" * 64 + ".json",
        "registration_sha256": "2" * 64,
        "readiness_certificate_id": "3" * 64,
        "readiness_evidence_sha256": "4" * 64,
        "alpha_ladder_contract_id": "5" * 64,
        "alpha_ladder_profile_id": "6" * 64,
        "alpha_ladder_stage": "tier_1",
        "mechanism_sha256": "7" * 64,
        "predecessor_decision_sha256": "8" * 64,
        "session_manifest_sha256": "9" * 64,
        "pilot_evaluation_session_ids_sha256": "a" * 64,
    }
    require_current_real_history_operation(
        CERTIFIED_TRIAL_EXECUTION_OPERATION, scope,
    )
    for key in tuple(scope):
        changed = dict(scope)
        changed.pop(key)
        with pytest.raises(UnauthorizedOperation):
            require_current_real_history_operation(
                CERTIFIED_TRIAL_EXECUTION_OPERATION, changed,
            )


def test_preparatory_census_is_not_misclassified_as_trial_execution() -> None:
    require_current_real_history_operation(
        "CENSUS_OVERNIGHT_REVERSAL_FOLD_READINESS_PARALLEL_ONCE",
        {"source_scope": "immutable"},
    )
    require_current_real_history_operation(
        SOURCE_COMPATIBILITY_CENSUS_OPERATION,
        {"source_scope": "active-catalog-only"},
    )


def test_unknown_preparatory_prefix_is_not_an_allowlist() -> None:
    with pytest.raises(UnauthorizedOperation, match="retired outside"):
        require_current_real_history_operation(
            "CENSUS_UNREGISTERED_FUTURE_OPERATION",
            {"source_scope": "immutable"},
        )


def test_generic_registry_and_v4_expose_explicit_retirement_guards() -> None:
    registry_source = inspect.getsource(TrialRegistry.register)
    v4_source = inspect.getsource(execute_authorized_v4)
    assert "CertifiedResearchGateway" in registry_source
    assert "REAL_HISTORY_DISCOVERY" in registry_source
    assert "CertifiedResearchGateway" in v4_source


def test_current_console_surface_has_no_real_history_executor() -> None:
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "futures-certified-execution" not in project
    assert "futures-high-risk-prepare" in project
    pipeline = (ROOT / "src/futures_rebuild/pipeline.py").read_text(encoding="utf-8")
    assert "--real-history" in pipeline
    assert "BLOCKED: real-history work requires" in pipeline
