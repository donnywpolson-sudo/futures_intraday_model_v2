"""Fail-closed operation policy for current real-history research.

Preparatory row access may build source-bound readiness evidence.  A strategy
execution is different: it must use the one certified gateway operation and
carry the exact immutable registration and readiness bindings in its receipt.
Historic operation names are intentionally rejected.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path

from .canonical import assert_plain_file, is_linklike, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


CERTIFIED_GATEWAY_SCHEMA = "certified_research_gateway/2.0.0"
BIAS_GATEWAY_SCHEMA = "certified_research_gateway/4.0.0"
RESEARCH_READINESS_POLICY_PATH = Path("configs/research_readiness_contract.json")
LEGACY_TRIAL_PENALTY_PATH = Path("configs/legacy_trial_penalty_decision.json")
GLOBAL_TRIAL_LEDGER_ROOT = Path("state/trial_registry")
CERTIFIED_TRIAL_EXECUTION_OPERATION = (
    "EXECUTE_CERTIFIED_TRIAL_HISTORICAL_SCREEN"
)
SOURCE_COMPATIBILITY_CENSUS_OPERATION = (
    "CENSUS_CASH_OPEN_41_MARKET_SOURCE_COMPATIBILITY_ONCE"
)
ALPHA_LADDER_READINESS_CENSUS_OPERATION = (
    "CENSUS_ALPHA_LADDER_PILOT_TIER1_READINESS_ONCE"
)
CAUSAL_OBSERVATION_CANARY_OPERATION = (
    "READ_DEVELOPMENT_CAUSAL_OBSERVATION_CANARY_ONCE"
)
CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION = (
    "READ_BOUNDED_2025_CAUSAL_OBSERVATION_SMOKE_ONCE"
)
CAUSAL_OBSERVATION_FULL_BUILD_OPERATION = (
    "BUILD_DEVELOPMENT_CAUSAL_OBSERVATION_FOUNDATION_ONCE"
)
CAUSAL_OBSERVATION_V10_CANARY_OPERATION = (
    "READ_V10_ES_2025_CAUSAL_CANARY_TWICE_ONCE"
)
ALPHA_LADDER_MACRO_SUCCESSOR_OPERATION = (
    "RUN_ALPHA_LADDER_MACRO_SUCCESSOR_EXECUTION_PROXY_ONCE"
)
ALPHA_LADDER_SR3_DIAGNOSTIC_OPERATION = (
    "DIAGNOSE_ALPHA_LADDER_SR3_UNKNOWN_ONCE"
)
ALPHA_LADDER_SR3_REMEDIATION_OPERATION = (
    "RUN_ALPHA_LADDER_SR3_REMEDIATION_ONCE"
)
ALPHA_LADDER_MACRO_SUCCESSOR_SCOPE_KEYS = frozenset(
    {
        "operation_kind",
        "markets",
        "years",
        "predecessor_closure_sha256",
        "candidate_order_sha256",
        "carried_evidence_sha256",
        "rank1_pointer_id",
        "ladder_pointer_id",
        "execution_design_pointer_id",
        "catalog_sha256",
        "certificates_sha256",
        "calendars_sha256",
        "schedule_sha256",
        "limits_sha256",
        "implementation_sha256",
        "plan_id",
        "plan_sha256",
    }
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
        "scientific_policy_sha256",
        "global_trial_ledger_head",
        "conservative_legacy_attempt_count",
    }
)
BIAS_GATE_EXECUTION_SCOPE_KEYS = CERTIFIED_EXECUTION_SCOPE_KEYS | {
    "causal_search_access_contract_id",
    "causal_search_access_contract_sha256",
    "pre_evaluation_declaration_id",
    "pre_evaluation_declaration_sha256",
}
_APPROVAL_SCOPE_KEYS = frozenset(
    {"approval_command", "approval_plan_id", "approval_plan_sha256"}
)
PREPARATORY_REAL_HISTORY_OPERATIONS = frozenset(
    {
        "CENSUS_ROLLING_HOUR_V6_PRICE_READINESS_ONCE",
        "DIAGNOSE_ROLLING_HOUR_PRICE_LINEAGE_FORMAT_ONCE",
        "ASSESS_OWNED_ROLLING_HOUR_REFERENCE_INPUTS_ONCE",
        "ACQUIRE_BOUNDED_2025_DEVELOPMENT_DBN_ONCE",
        "ACQUIRE_APEX_MICRO_TIER01_RAW_DBN_INACTIVE_CUSTODY_ONCE",
        "ACQUIRE_APEX_MICRO_TIER01_RAW_DBN_INACTIVE_CUSTODY_V21_ONCE",
        "ACQUIRE_APEX_MICRO_TIER01_RAW_DBN_INACTIVE_CUSTODY_V22_ONCE",
        "ACQUIRE_APEX_MICRO_TIER01_RAW_DBN_INACTIVE_CUSTODY_V24_ONCE",
        "REPAIR_APEX_MICRO_V24_HARDLINK_CUSTODY_V2_ONCE",
        "BUILD_APEX_MICRO_PHASE1B2_INACTIVE_FOUNDATION_V1_ONCE",
        "DIAGNOSE_APEX_MICRO_PHASE2_FIRST_INTERVAL_V1_ONCE",
        "DIAGNOSE_APEX_MICRO_PHASE2_FIRST_GROUP_V2_ONCE",
        "DIAGNOSE_APEX_MICRO_DEFINITION_DUPLICATE_SEMANTICS_V3_ONCE",
        "BUILD_APEX_MICRO_PHASE2_CERTIFIED_INACTIVE_SUCCESSOR_V4_ONCE",
        "ACQUIRE_L0_AUG31_CUTOFF_ALIGNMENT_QUARANTINE_V1_ONCE",
        "AUDIT_L0_AUG31_CUTOFF_ALIGNMENT_ROWS_V1_ONCE",
        "CONSOLIDATE_L0_2026_DBN_CUSTODY_V1_ONCE",
        "ACQUIRE_L1_EXECUTION_BBO_1S_QUARANTINE_V1_ONCE",
        "ACQUIRE_L1_EXECUTION_BBO_1S_MARKET_PARTITION_V1_ONCE",
        "CERTIFY_L1_EXECUTION_BBO_1S_MARKET_PARTITION_V1_ONCE",
        "ACQUIRE_TBBO_16_MARKET_RECENT_YEAR_V1_ONCE",
        "ACQUIRE_AND_SEAL_TBBO_16_MARKET_DEFINITION_STATUS_SUPPLEMENT_V1_ONCE",
        "CERTIFY_TBBO_16_MARKET_ROW_QUALITY_V1_ONCE",
        "DIAGNOSE_TBBO_ES_STATUS_SESSION_EVIDENCE_V1_ONCE",
        "ACQUIRE_FROZEN_TIER1_BBO_1S_RECOVERY_STAGING_AND_PUBLISH",
        "AUDIT_V12_LOCAL_CAUSAL_RELEASE_ALTERNATIVES_READ_ONLY",
        "AUDIT_V9_REGISTERED_SOURCE_DEPENDENCY_WINDOWS_READ_ONLY",
        ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        ALPHA_LADDER_SR3_DIAGNOSTIC_OPERATION,
        CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
        CAUSAL_OBSERVATION_CANARY_OPERATION,
        CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        CAUSAL_OBSERVATION_V10_CANARY_OPERATION,
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


def scientific_governance_binding(root: Path) -> dict[str, object]:
    """Snapshot the scientific policy, legacy burden, and prior registrations."""

    policy_path = root / RESEARCH_READINESS_POLICY_PATH
    penalty_path = root / LEGACY_TRIAL_PENALTY_PATH
    try:
        assert_plain_file(policy_path)
        assert_plain_file(penalty_path)
        policy = json.loads(policy_path.read_bytes())
        penalty = json.loads(penalty_path.read_bytes())
    except (OSError, ValueError, IntegrityError) as exc:
        raise IntegrityError("scientific governance inputs are invalid") from exc
    if not isinstance(policy, dict) or not isinstance(penalty, dict):
        raise IntegrityError("scientific governance inputs must be objects")
    readiness = policy.get("readiness")
    if (
        policy.get("contract_status")
        != "IMPLEMENTED_SYNTHETICALLY_VALIDATED_CAUSAL_OBSERVATION_FOUNDATION_ACTIVE"
        or not isinstance(readiness, dict)
        or readiness.get("current_state")
        != "CAUSAL_OBSERVATION_FOUNDATION_ACTIVE_TRIAL_REGISTRATION_REQUIRES_ROW_CERTIFICATE"
    ):
        raise IntegrityError("research readiness policy is stale")
    count = penalty.get("preregistered_penalty_count")
    observed = penalty.get("observed_attempt_floor")
    if (
        type(count) is not int
        or type(observed) is not int
        or count <= observed
        or re.fullmatch(r"[0-9a-f]{64}", str(penalty.get("decision_id", ""))) is None
    ):
        raise IntegrityError("conservative legacy-attempt decision is invalid")

    inventory: list[dict[str, str]] = []
    ledger_root = root / GLOBAL_TRIAL_LEDGER_ROOT
    if ledger_root.exists():
        if not ledger_root.is_dir() or is_linklike(ledger_root):
            raise IntegrityError("global trial ledger root is invalid")
        for family in sorted(ledger_root.iterdir(), key=lambda item: item.name):
            if not family.is_dir() or is_linklike(family):
                raise IntegrityError("global trial ledger contains an invalid family")
            for path in sorted(family.glob("*.json"), key=lambda item: item.name):
                assert_plain_file(path)
                inventory.append({
                    "path": path.relative_to(root).as_posix(),
                    "sha256": sha256_file(path),
                })
    return {
        "schema_version": "scientific_governance_binding/1.0.0",
        "scientific_policy": {
            "path": RESEARCH_READINESS_POLICY_PATH.as_posix(),
            "sha256": sha256_file(policy_path),
        },
        "global_trial_ledger": {
            "path": GLOBAL_TRIAL_LEDGER_ROOT.as_posix(),
            "head": sha256_json(inventory),
        },
        "legacy_attempts": {
            "path": LEGACY_TRIAL_PENALTY_PATH.as_posix(),
            "decision_id": penalty["decision_id"],
            "conservative_count": count,
        },
    }


def require_current_real_history_operation(
    operation: str, scope: Mapping[str, str],
) -> None:
    """Reject trial execution outside the certified gateway.

    Preparatory operations cannot promote, fit, predict, or evaluate a trial;
    their bounded implementations retain their own exact receipt checks.  All
    other real-history operations must be the current certified execution
    operation with the mandatory immutable identity fields.
    """

    if operation in {"INSPECT_SAVED_ES_MODEL_SESSION_ONCE", "PREPARE_SAVED_ES_OOS_LIBRARY_ONCE"}:
        from .historical_model_inspection import require_scope
        spec = require_scope(Path(__file__).resolve().parents[2], scope)
        if spec["operation"] != operation:
            raise IntegrityError("Historical inspection operation differs from specification")
        return

    if operation == "EXPLORE_ASSUMED_OHLCV_MINUTE_ONCE":
        from .assumed_ohlcv_screen import require_scope
        require_scope(Path(__file__).resolve().parents[2], scope)
        return

    if operation == 'BUILD_CERTIFY_CAUSAL_1S_RANGE_ONCE':
        from .causal_1s_native import require_scope
        require_scope(Path(__file__).resolve().parents[2], scope)
        return

    if operation == 'BUILD_CERTIFY_CAUSAL_1H_RANGE_ONCE':
        from .causal_1h_native import require_scope
        require_scope(Path(__file__).resolve().parents[2], scope)
        return

    if operation == 'BUILD_CERTIFY_CAUSAL_1D_RANGE_ONCE':
        from .causal_1d_native import require_scope
        require_scope(Path(__file__).resolve().parents[2], scope)
        return

    if operation == 'RECOVER_CERTIFY_NATIVE_DAILY_IDENTITY_ONCE':
        from .daily_identity_authorization import require_scope
        require_scope(Path(__file__).resolve().parents[2], scope)
        return

    from .discovery import require_adopted_scope
    require_adopted_scope(Path(__file__).resolve().parents[2], operation, scope)

    if operation == "CENSUS_ES_CLOSE_REFERENCE_METADATA_ONCE":
        # Attempt pre/postflight belongs to the reader; never recurse from a receipt.
        from . import es_close_source_readiness as census
        root = Path(__file__).resolve().parents[2]
        adopted = census._json(census._path(root, census.APPROVED.as_posix()))
        census._identity(adopted, "adoption_id")
        requested = {k: v for k, v in scope.items() if k not in _APPROVAL_SCOPE_KEYS}
        if (set(requested) != {"operation_id", "declaration_sha256", "claim"}
                or adopted.get("state") != "ADOPTED_INACTIVE_RECEIPT_REQUIRED"
                or adopted.get("operation") != operation
                or adopted.get("scope") != requested):
            raise UnauthorizedOperation("ES census adoption or exact scope differs")
        relative = adopted.get("operation_path", "")
        if (not isinstance(relative, str)
                or not relative.startswith(census.BASE.as_posix()+"/")
                or not relative.endswith(".json")):
            raise UnauthorizedOperation("ES census operation must be unpublished metadata")
        path = census._path(root, relative)
        if sha256_file(path) != adopted.get("operation_sha256"):
            raise IntegrityError("ES census operation bytes changed")
        op = census._json(path)
        census._identity(op)
        if (census.scope(op) != requested or op.get("operation") != operation
                or op.get("schema_version") != "es_close_source_census/1"
                or op.get("state") != "PREPARED_INACTIVE"
                or op.get("execution_enabled") is not False
                or Path(op.get("repository_root", "")).resolve() != root
                or op.get("claim") != census.CLAIM
                or op.get("declaration_sha256") != census.DECLARATION_SHA
                or op.get("start") != census.START or op.get("end_exclusive") != census.END
                or op.get("fields") != list(census.FIELDS)
                or op.get("limits") != census.LIMITS):
            raise UnauthorizedOperation("ES census operation exceeds the reader scope")
        if (adopted.get("timeframe_index_state") != "ABSENT"
                or census._path(root, "data/causally_gated_normalized/_control/timeframes.json").exists()):
            raise IntegrityError("ES census timeframe dependencies changed; rebind before access")
        code = op.get("implementation_bindings", {})
        guards = adopted.get("guard_bindings", {})
        if (set(code) != set(census.CODE_FILES)
                or set(guards) != {"configs/active_data_view_contract.json",
                                   "configs/active_dbn_congruence_release_v1.json",
                                   "configs/active_alpha_research_ladder.json"}):
            raise UnauthorizedOperation("ES census code or active-pointer binding is incomplete")
        for relative, digest in {**code, **guards}.items():
            if (not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                    or sha256_file(census._path(root, relative)) != digest):
                raise IntegrityError("ES census code or active pointer changed")
        # Admission is not a receipt. The reader separately verifies and consumes
        # the exact one-use OperationReceipt before its first payload access.
        return
    if operation in {"MATERIALIZE_ES_NQ_TRANSFER_ONCE", "EXECUTE_ES_NQ_TRANSFER_ONCE"}:
        root = Path(__file__).resolve().parents[2]
        approved_path = root / "configs/approved_es_nq_transfer.json"
        if not approved_path.is_file():
            raise UnauthorizedOperation("ES/NQ access is not adopted")
        assert_plain_file(approved_path)
        approved = json.loads(approved_path.read_bytes())
        reader = root / "src/futures_rebuild/es_nq_transfer_access.py"
        if sha256_file(reader) != approved.get("reader_sha256"):
            raise UnauthorizedOperation("ES/NQ access implementation is stale")
        from .es_nq_transfer_access import require_scope
        require_scope(root, operation, scope)
        return
    if operation == "AUDIT_RETROSPECTIVE_SCREEN_ONCE":
        root = Path(__file__).resolve().parents[2]
        clock = scope.get("claim") in {"EXPLORATORY_CLOCK_DIAGNOSTIC_ONLY_NO_REFIT", "EXPLORATORY_CLOCK_DIAGNOSTIC_REPLACEMENT_ONLY_NO_REFIT"}
        folder = root / "state/unpublished_evidence/l0_order_readiness_v1" / ("screen_clock_diagnostic_v1" if clock else "screen_value_audit_v1")
        replacement = scope.get("claim") == "EXPLORATORY_CLOCK_DIAGNOSTIC_REPLACEMENT_ONLY_NO_REFIT"
        if replacement:
            folder = folder / "replacement"
        approved_path, spec_path = folder / "approved_audit.json", folder / "operation.json"
        assert_plain_file(approved_path)
        assert_plain_file(spec_path)
        approved = json.loads(approved_path.read_bytes())
        spec = json.loads(spec_path.read_bytes())
        request = {k: v for k, v in scope.items() if k not in _APPROVAL_SCOPE_KEYS}
        required = {"study_id", "operation_id", "read_set_id", "reader_sha256", "policy_sha256", "claim"}
        if (set(request) != required or approved.get("state") != "APPROVED_EXACT_AUDIT_ONCE"
                or any(not isinstance(request.get(k), str) or re.fullmatch(r"[0-9a-f]{64}", request[k]) is None
                       for k in required - {"claim"})
                or request != approved.get("scope")
                or request != {k: spec.get(k) for k in required}
                or request.get("claim") != ("EXPLORATORY_CLOCK_DIAGNOSTIC_REPLACEMENT_ONLY_NO_REFIT" if replacement else "EXPLORATORY_CLOCK_DIAGNOSTIC_ONLY_NO_REFIT" if clock else "AUDIT_EXISTING_SCREEN_ONLY_NO_REFIT")
                or sha256_file(spec_path) != approved.get("specification_sha256")
                or spec.get("operation") != operation
                or sha256_json({k: v for k, v in spec.items() if k != "operation_id"}) != request["operation_id"]):
            raise UnauthorizedOperation("audit identities are missing, stale or not exact")
        parent_path = folder / "read_scope.json" if clock else root / "state/unpublished_evidence/l0_order_readiness_v1/screen_mechanics_audit_v1/next_operation.json"
        assert_plain_file(parent_path)
        parent = json.loads(parent_path.read_bytes())
        read_set = spec.get("read_set")
        if (sha256_file(parent_path) != spec.get("parent_proposal_sha256")
                or not isinstance(read_set, dict) or read_set != parent.get("read_set")
                or sha256_json(read_set) != request["read_set_id"]):
            raise UnauthorizedOperation("audit read set differs from authorized parent")
        for path, digest in read_set.items():
            if (not isinstance(path, str) or not path or "\\" in path or ":" in path
                    or Path(path).is_absolute() or any(p in ("", ".", "..") for p in path.split("/"))
                    or not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None):
                raise UnauthorizedOperation("audit path or artifact identity is invalid")
        runner = folder / ("diagnostic.py" if clock else "audit_saved_values.py")
        assert_plain_file(runner)
        if sha256_file(runner) != request["reader_sha256"] or sha256_file(Path(__file__)) != request["policy_sha256"]:
            raise UnauthorizedOperation("audit reader or installed policy changed")
        return
    if operation in {"MATERIALIZE_RETROSPECTIVE_SCREEN_ONCE", "EXECUTE_RETROSPECTIVE_SCREEN_ONCE"}:
        research_scope = {k: v for k, v in scope.items() if k not in _APPROVAL_SCOPE_KEYS}
        kind = "materialize" if operation.startswith("MATERIALIZE_") else "execute"
        required = {"study_id", "operation_id", "operation_kind", "source_plan_sha256", "claim"}
        root = Path(__file__).resolve().parents[2]
        approved_path = root / "configs/approved_retrospective_screen.json"
        if not approved_path.is_file():
            raise UnauthorizedOperation("retrospective screen authority is not adopted")
        assert_plain_file(approved_path)
        approved = json.loads(approved_path.read_bytes())
        package = root / "state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_adoption_v1"
        study_path = package / "study.json"
        accounting_path = package / "search_accounting.json"
        assert_plain_file(study_path)
        assert_plain_file(accounting_path)
        operation_ids = approved.get("operation_ids")
        requested = research_scope.get("operation_id")
        adopted = operation_ids.get(kind) if isinstance(operation_ids, dict) else None
        hashes = (requested, adopted, research_scope.get("study_id"), approved.get("study_id"),
                  research_scope.get("source_plan_sha256"), approved.get("source_plan_sha256"))
        if (any(not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None for value in hashes)
                or set(research_scope) != required or approved.get("state") != "APPROVED_FOR_RETROSPECTIVE_ONLY"
                or research_scope.get("study_id") != approved.get("study_id")
                or sha256_file(study_path) != approved.get("study_sha256")
                or sha256_file(accounting_path) != approved.get("search_accounting_sha256")
                or research_scope.get("operation_kind") != kind or requested != adopted
                or research_scope.get("source_plan_sha256") != approved.get("source_plan_sha256")
                or research_scope.get("claim") != "RETROSPECTIVE_OBSERVED_TARGET_ONLY"
                or approved.get("prior_closed_mechanisms_preserved") is not True
                or approved.get("related_trials_charged") != 12):
            raise UnauthorizedOperation("retrospective screen scope is not adopted and exact")
        successor = approved.get("execution_implementation") if kind == "execute" else None
        if successor is not None:
            folder = root / "state/unpublished_evidence/l0_order_readiness_v1/retrospective_screen_completion_v1"
            if not isinstance(successor, dict) or not isinstance(successor.get("path"), str):
                raise UnauthorizedOperation("execution implementation path is invalid")
            implementation_path = root / successor["path"]
            if not implementation_path.resolve().is_relative_to(folder.resolve()) or implementation_path.name != "implementation.json":
                raise UnauthorizedOperation("execution implementation path escapes its approved package")
            operation_path = implementation_path.parent / "execute_operation.json"
            for path in (implementation_path, operation_path):
                assert_plain_file(path)
            implementation = json.loads(implementation_path.read_bytes())
            operation_spec = json.loads(operation_path.read_bytes())
            if (successor != {"path": implementation_path.relative_to(root).as_posix(),
                              "sha256": sha256_file(implementation_path),
                              "implementation_id": implementation.get("implementation_id")}
                    or sha256_json({k:v for k,v in implementation.items() if k != "implementation_id"}) != implementation.get("implementation_id")
                    or implementation.get("study_id") != research_scope["study_id"]
                    or operation_spec.get("implementation_sha256") != successor["sha256"]
                    or operation_spec.get("operation_id") != requested
                    or sha256_json({k:v for k,v in operation_spec.items() if k != "operation_id"}) != requested):
                raise UnauthorizedOperation("execution implementation/operation binding is stale")
            for relative, digest in implementation.get("bindings", {}).items():
                path = root / relative
                if (not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                        or not path.resolve().is_relative_to(root.resolve())):
                    raise UnauthorizedOperation("execution implementation identity is invalid")
                assert_plain_file(path)
                if sha256_file(path) != digest:
                    raise UnauthorizedOperation("execution implementation bytes changed")
            required_files = {"src/futures_rebuild/research_gateway_policy.py", *(folder.relative_to(root).as_posix()+"/"+n
                               for n in ("completion.py", "sizing.py", "execute_approved_once.py"))}
            if not required_files.issubset(implementation.get("bindings", {})):
                raise UnauthorizedOperation("execution implementation binding is incomplete")
        return
    if operation in PREPARATORY_REAL_HISTORY_OPERATIONS:
        return
    if operation == ALPHA_LADDER_SR3_REMEDIATION_OPERATION:
        research_scope = {
            key: value for key, value in scope.items() if key not in _APPROVAL_SCOPE_KEYS
        }
        required = {
            "operation_kind", "market", "years", "consumed_closure_id",
            "policy_id", "diagnostic_evidence_id", "carried_evidence_id",
            "catalog_sha256", "limits_sha256", "implementation_sha256",
            "plan_id", "plan_sha256",
        }
        hashes = required - {"operation_kind", "market", "years"}
        if (
            set(research_scope) != required
            or research_scope.get("operation_kind") != "POST_DIAGNOSTIC_SR3_REMEDIATION_AND_SEAL"
            or research_scope.get("market") != "SR3"
            or research_scope.get("years") != "2018,2019,2020,2021,2022"
            or any(re.fullmatch(r"[0-9a-f]{64}", research_scope.get(key, "")) is None for key in hashes)
        ):
            raise UnauthorizedOperation("SR3 remediation gateway scope is invalid")
        return
    if operation == ALPHA_LADDER_MACRO_SUCCESSOR_OPERATION:
        raise UnauthorizedOperation("consumed macro successor operation is retired")
    if operation != CERTIFIED_TRIAL_EXECUTION_OPERATION:
        raise UnauthorizedOperation(
            "real-history trial execution is retired outside the certified gateway"
        )
    research_scope = {
        key: value for key, value in scope.items() if key not in _APPROVAL_SCOPE_KEYS
    }
    gateway_schema = research_scope.get("gateway_schema")
    from . import l0_development_admission as l0
    if gateway_schema == l0.GATEWAY_SCHEMA:
        l0.validate_execution_scope(research_scope)
        return
    required_scope = (
        BIAS_GATE_EXECUTION_SCOPE_KEYS
        if gateway_schema == BIAS_GATEWAY_SCHEMA
        else frozenset()
    )
    missing = required_scope - set(research_scope)
    if missing:
        raise UnauthorizedOperation(
            "certified historical execution scope is incomplete"
        )
    if (
        gateway_schema not in {CERTIFIED_GATEWAY_SCHEMA, BIAS_GATEWAY_SCHEMA}
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
                "scientific_policy_sha256",
                "global_trial_ledger_head",
                *(
                    (
                        "causal_search_access_contract_id",
                        "causal_search_access_contract_sha256",
                        "pre_evaluation_declaration_id",
                        "pre_evaluation_declaration_sha256",
                    )
                    if gateway_schema == BIAS_GATEWAY_SCHEMA
                    else ()
                ),
            )
        )
        or re.fullmatch(
            r"[1-9][0-9]*", research_scope.get("conservative_legacy_attempt_count", "")
        ) is None
        or research_scope.get("alpha_ladder_stage")
        not in {"pilot", "tier_1", "tier_2", "tier_3", "challenge", "refit", "holdout", "forward"}
        or not research_scope.get("trial_family")
        or not research_scope.get("protocol_id")
        or not research_scope.get("registration_path", "").startswith(
            "state/trial_registry/"
        )
    ):
        raise UnauthorizedOperation(
            "certified historical execution scope is invalid"
        )
