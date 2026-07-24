"""Strict v3 master-audit contract and evidence-only classifier.

The classifier reads only invocation-declared files.  It never imports or runs
research code, reads holdout rows, calls a provider, or publishes readiness.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from futures_rebuild.canonical import contained_path, sha256_file
from futures_rebuild.errors import ContractError, IntegrityError


AUDIT_SCHEMA_VERSION = "systematic_futures_audit/3.0.0"
UNIVERSE_SCHEMA_VERSION = "glbx_research_universe/1.0.0"
_SHA256 = re.compile(r"[0-9a-f]{64}")


class AuditContractError(ContractError):
    """The audit invocation or one of its frozen registries is invalid."""


class AuditStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"
    MISSING_EVIDENCE = "MISSING_EVIDENCE"
    UNKNOWN = "UNKNOWN"
    NOT_RUN = "NOT_RUN"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class AuditDecision(str, Enum):
    SUPPORTABLE = "SUPPORTABLE"
    BLOCKED = "BLOCKED"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


_STATUS_PRECEDENCE = {
    AuditStatus.FAIL: 7,
    AuditStatus.ERROR: 6,
    AuditStatus.MISSING_EVIDENCE: 5,
    AuditStatus.UNKNOWN: 4,
    AuditStatus.NOT_RUN: 3,
    AuditStatus.PASS: 2,
    AuditStatus.NOT_APPLICABLE: 1,
}

TARGET_STATES = (
    "REBUILD_COMPLETE",
    "HISTORICAL_RESEARCH_READY",
    "CANDIDATE_SEALED",
    "PROSPECTIVE_EVIDENCE_PENDING",
    "PROSPECTIVE_PASS",
    "PROSPECTIVE_FAIL",
    "PROSPECTIVE_INCONCLUSIVE",
    "MANUAL_DECISION_SUPPORT_READY",
)

UNIVERSE_OWNED_SUBCHECKS = frozenset(
    {"G1.S2", "G1.S3", "G2.S2", "G5.S1", "G5.S5", "G6.S1", "G6.S2"}
)


def _object(value: object, name: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise AuditContractError(f"{name} must be an exact object")
    return value


def _exact_keys(value: Mapping[str, Any], expected: set[str], name: str) -> None:
    if set(value) != expected:
        raise AuditContractError(f"{name} fields are invalid")


def _strings(value: object, name: str, *, nonempty: bool = False) -> tuple[str, ...]:
    if type(value) is not list or any(type(item) is not str or not item for item in value):
        raise AuditContractError(f"{name} must be a list of nonempty strings")
    result = tuple(value)
    if len(set(result)) != len(result):
        raise AuditContractError(f"{name} contains duplicates")
    if nonempty and not result:
        raise AuditContractError(f"{name} cannot be empty")
    return result


def _digest(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise AuditContractError(f"{name} must be an exact lowercase SHA-256")
    return value


def _load_json(path: Path, name: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AuditContractError(f"{name} is not readable canonical JSON") from exc
    return _object(payload, name)


def _declared_path(root: Path, relative: object, allowed: tuple[str, ...], name: str) -> Path:
    if type(relative) is not str or not relative or "\\" in relative:
        raise AuditContractError(f"{name} must be a nonempty POSIX-style relative path")
    if not any(relative == prefix or relative.startswith(prefix.rstrip("/") + "/") for prefix in allowed):
        raise AuditContractError(f"{name} is outside invocation.allowed_paths")
    return contained_path(root, relative)


def validate_stage_matrix(payload: object) -> dict[str, Any]:
    matrix = _object(payload, "stage matrix")
    _exact_keys(matrix, {"schema_version", "registry_id", "target_states", "gates"}, "stage matrix")
    if matrix["schema_version"] != AUDIT_SCHEMA_VERSION:
        raise AuditContractError("stage matrix schema_version is invalid")
    if type(matrix["registry_id"]) is not str or not matrix["registry_id"]:
        raise AuditContractError("stage matrix registry_id is invalid")
    states = _object(matrix["target_states"], "stage matrix.target_states")
    if set(states) != set(TARGET_STATES):
        raise AuditContractError("stage matrix target states are incomplete")
    gates = matrix["gates"]
    if type(gates) is not list or len(gates) != 8:
        raise AuditContractError("stage matrix must contain exactly eight gates")
    gate_ids: set[str] = set()
    subcheck_ids: set[str] = set()
    for index, raw in enumerate(gates):
        gate = _object(raw, f"stage matrix.gates[{index}]")
        _exact_keys(gate, {"gate_id", "title", "subchecks"}, f"stage matrix.gates[{index}]")
        gate_id = gate["gate_id"]
        if type(gate_id) is not str or not re.fullmatch(r"G[1-8]", gate_id):
            raise AuditContractError("stage matrix gate_id is invalid")
        gate_ids.add(gate_id)
        subchecks = gate["subchecks"]
        if type(subchecks) is not list or not subchecks:
            raise AuditContractError(f"{gate_id} has no subchecks")
        for item in subchecks:
            check = _object(item, f"{gate_id} subcheck")
            _exact_keys(check, {"subcheck_id", "title"}, f"{gate_id} subcheck")
            subcheck_id = check["subcheck_id"]
            if type(subcheck_id) is not str or not subcheck_id.startswith(gate_id + ".S"):
                raise AuditContractError(f"{gate_id} subcheck identity is invalid")
            subcheck_ids.add(subcheck_id)
    if gate_ids != {f"G{index}" for index in range(1, 9)}:
        raise AuditContractError("stage matrix gate identities are incomplete")
    if len(subcheck_ids) != sum(len(gate["subchecks"]) for gate in gates):
        raise AuditContractError("stage matrix contains duplicate subchecks")
    for state, required in states.items():
        required_ids = _strings(required, f"target_states.{state}", nonempty=True)
        if any(item not in subcheck_ids for item in required_ids):
            raise AuditContractError(f"target state {state} references an unknown subcheck")
    return matrix


def validate_universe_contract(payload: object) -> tuple[dict[str, Any], bool]:
    universe = _object(payload, "universe contract")
    _exact_keys(
        universe,
        {
            "schema_version", "classification", "status", "approval_receipt_id",
            "provider", "data_contract", "tiers", "cohorts", "admission_policy",
            "evidence_rules", "closed_legacy_lines",
        },
        "universe contract",
    )
    if universe["schema_version"] != UNIVERSE_SCHEMA_VERSION:
        raise AuditContractError("universe schema_version is invalid")
    if universe["classification"] != "NON_AUTHORIZING_RESEARCH_DESIGN":
        raise AuditContractError("universe classification is invalid")
    if universe["status"] not in {"PENDING_APPROVAL", "APPROVED"}:
        raise AuditContractError("universe status is invalid")
    approved = universe["status"] == "APPROVED"
    receipt = universe["approval_receipt_id"]
    if approved:
        _digest(receipt, "universe approval_receipt_id")
    elif receipt is not None:
        raise AuditContractError("pending universe cannot contain an approval receipt")
    provider = _object(universe["provider"], "universe provider")
    _exact_keys(provider, {"name", "dataset", "provider_calls_authorized"}, "universe provider")
    if provider != {"name": "Databento", "dataset": "GLBX.MDP3", "provider_calls_authorized": False}:
        raise AuditContractError("universe provider boundary is invalid")
    tiers = universe["tiers"]
    if type(tiers) is not list or [item.get("tier_id") for item in tiers if type(item) is dict] != [0, 1, 2, 3, 4]:
        raise AuditContractError("universe must define ordered tiers 0 through 4")
    for tier in tiers:
        _exact_keys(tier, {"tier_id", "role", "symbols", "year_policy"}, "universe tier")
        _strings(tier["symbols"], f"tier {tier['tier_id']} symbols", nonempty=True)
        _strings(tier["year_policy"], f"tier {tier['tier_id']} year_policy", nonempty=True)
    cohorts = universe["cohorts"]
    if type(cohorts) is not list or not cohorts:
        raise AuditContractError("universe cohorts are missing")
    cohort_roles = set()
    for cohort in cohorts:
        _exact_keys(cohort, {"role", "years", "selection_eligible"}, "universe cohort")
        if type(cohort["selection_eligible"]) is not bool:
            raise AuditContractError("cohort selection_eligible must be boolean")
        _strings(cohort["years"], "cohort years", nonempty=True)
        cohort_roles.add(cohort["role"])
    required_roles = {"DISCOVERY", "PREVIOUSLY_USED_NON_PRISTINE_RESEARCH", "LOCKED_UNTOUCHED_FINAL_HOLDOUT", "FORWARD_ONLY"}
    if not required_roles.issubset(cohort_roles):
        raise AuditContractError("universe cohorts omit required research roles")
    _strings(universe["closed_legacy_lines"], "closed_legacy_lines", nonempty=True)
    return universe, approved


@dataclass(frozen=True)
class VerifiedEvidence:
    evidence_id: str
    path: str
    sha256: str
    bytes: int


def _verify_evidence(
    root: Path,
    raw_evidence: object,
    allowed: tuple[str, ...],
    max_files: int,
    max_bytes: int,
) -> dict[str, VerifiedEvidence]:
    if type(raw_evidence) is not list:
        raise AuditContractError("evidence must be a list")
    if len(raw_evidence) > max_files:
        raise AuditContractError("evidence exceeds runtime.max_files")
    verified: dict[str, VerifiedEvidence] = {}
    total = 0
    for index, raw in enumerate(raw_evidence):
        item = _object(raw, f"evidence[{index}]")
        _exact_keys(item, {"evidence_id", "path", "sha256", "bytes", "safe_to_read", "limitations"}, f"evidence[{index}]")
        evidence_id = item["evidence_id"]
        if type(evidence_id) is not str or not evidence_id or evidence_id in verified:
            raise AuditContractError("evidence_id is invalid or duplicated")
        if item["safe_to_read"] is not True:
            raise AuditContractError(f"evidence {evidence_id} is not authorized for reading")
        _strings(item["limitations"], f"evidence {evidence_id} limitations")
        expected_hash = _digest(item["sha256"], f"evidence {evidence_id} sha256")
        expected_bytes = item["bytes"]
        if type(expected_bytes) is not int or expected_bytes < 0:
            raise AuditContractError(f"evidence {evidence_id} bytes is invalid")
        path = _declared_path(root, item["path"], allowed, f"evidence {evidence_id} path")
        try:
            observed_bytes = path.stat().st_size
            observed_hash = sha256_file(path)
        except (OSError, ContractError, IntegrityError) as exc:
            raise AuditContractError(f"evidence {evidence_id} cannot be verified") from exc
        if observed_bytes != expected_bytes or observed_hash != expected_hash:
            raise AuditContractError(f"evidence {evidence_id} identity mismatch")
        total += observed_bytes
        if total > max_bytes:
            raise AuditContractError("evidence exceeds runtime.max_bytes_read")
        verified[evidence_id] = VerifiedEvidence(evidence_id, item["path"], observed_hash, observed_bytes)
    return verified


def _worst(statuses: list[AuditStatus]) -> AuditStatus:
    applicable = [item for item in statuses if item is not AuditStatus.NOT_APPLICABLE]
    if not applicable:
        return AuditStatus.NOT_APPLICABLE
    return max(applicable, key=_STATUS_PRECEDENCE.__getitem__)


def run_audit(root: Path, invocation_payload: object) -> dict[str, Any]:
    """Validate one frozen invocation and return a non-authorizing classification."""

    root = root.resolve(strict=True)
    invocation = _object(invocation_payload, "invocation")
    _exact_keys(
        invocation,
        {
            "schema_version", "classification", "mode", "audit_id", "target_state",
            "allowed_paths", "specification", "stage_matrix", "universe_contract",
            "evidence", "check_results", "runtime",
        },
        "invocation",
    )
    if invocation["schema_version"] != AUDIT_SCHEMA_VERSION:
        raise AuditContractError("invocation schema_version is invalid")
    if invocation["classification"] != "NON_AUTHORIZING_EVIDENCE_CLASSIFICATION" or invocation["mode"] != "EVIDENCE_ONLY":
        raise AuditContractError("invocation safety classification is invalid")
    if type(invocation["audit_id"]) is not str or not invocation["audit_id"]:
        raise AuditContractError("audit_id is invalid")
    target_state = invocation["target_state"]
    if target_state not in TARGET_STATES:
        raise AuditContractError("target_state is invalid")
    allowed = _strings(invocation["allowed_paths"], "allowed_paths", nonempty=True)
    if any(Path(item).is_absolute() or ".." in Path(item).parts or "\\" in item for item in allowed):
        raise AuditContractError("allowed_paths must be contained POSIX-style paths")

    registries: dict[str, dict[str, Any]] = {}
    for name in ("specification", "stage_matrix", "universe_contract"):
        ref = _object(invocation[name], name)
        _exact_keys(ref, {"path", "sha256"}, name)
        path = _declared_path(root, ref["path"], allowed, f"{name}.path")
        if sha256_file(path) != _digest(ref["sha256"], f"{name}.sha256"):
            raise AuditContractError(f"{name} hash mismatch")
        if name != "specification":
            registries[name] = _load_json(path, name)

    matrix = validate_stage_matrix(registries["stage_matrix"])
    universe, universe_approved = validate_universe_contract(registries["universe_contract"])

    runtime = _object(invocation["runtime"], "runtime")
    _exact_keys(runtime, {"max_files", "max_bytes_read", "allowed_command_classes", "forbidden_actions"}, "runtime")
    if type(runtime["max_files"]) is not int or runtime["max_files"] < 0:
        raise AuditContractError("runtime.max_files is invalid")
    if type(runtime["max_bytes_read"]) is not int or runtime["max_bytes_read"] < 0:
        raise AuditContractError("runtime.max_bytes_read is invalid")
    allowed_commands = _strings(runtime["allowed_command_classes"], "allowed_command_classes")
    if any(item not in {"static-read", "hash-read"} for item in allowed_commands):
        raise AuditContractError("runtime permits a non-read-only command class")
    forbidden = set(_strings(runtime["forbidden_actions"], "forbidden_actions", nonempty=True))
    required_forbidden = {"network", "provider-call", "project-code-execution", "holdout-row-access", "readiness-publication", "candidate-sealing", "order-placement", "legacy-write"}
    if not required_forbidden.issubset(forbidden):
        raise AuditContractError("runtime forbidden_actions is incomplete")

    evidence = _verify_evidence(root, invocation["evidence"], allowed, runtime["max_files"], runtime["max_bytes_read"])
    if universe_approved:
        approval_id = universe["approval_receipt_id"]
        approval_evidence = evidence.get(approval_id)
        if approval_evidence is None or approval_evidence.sha256 != approval_id:
            raise AuditContractError(
                "approved universe is not bound to its exact approval receipt evidence"
            )
    all_subchecks = {
        item["subcheck_id"]
        for gate in matrix["gates"]
        for item in gate["subchecks"]
    }
    results_raw = invocation["check_results"]
    if type(results_raw) is not list:
        raise AuditContractError("check_results must be a list")
    supplied: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(results_raw):
        result = _object(raw, f"check_results[{index}]")
        _exact_keys(result, {"subcheck_id", "status", "reason", "evidence_refs", "limitations"}, f"check_results[{index}]")
        subcheck_id = result["subcheck_id"]
        if subcheck_id not in all_subchecks or subcheck_id in supplied:
            raise AuditContractError("check_results contains an unknown or duplicate subcheck")
        try:
            status = AuditStatus(result["status"])
        except (TypeError, ValueError) as exc:
            raise AuditContractError(f"{subcheck_id} status is invalid") from exc
        if status is AuditStatus.NOT_RUN:
            raise AuditContractError("caller-supplied NOT_RUN is forbidden")
        if type(result["reason"]) is not str or not result["reason"]:
            raise AuditContractError(f"{subcheck_id} reason is invalid")
        refs = _strings(result["evidence_refs"], f"{subcheck_id} evidence_refs")
        _strings(result["limitations"], f"{subcheck_id} limitations")
        if any(ref not in evidence for ref in refs):
            raise AuditContractError(f"{subcheck_id} has a dangling evidence reference")
        if status in {AuditStatus.PASS, AuditStatus.FAIL} and not refs:
            raise AuditContractError(f"{subcheck_id} {status.value} requires evidence")
        supplied[subcheck_id] = {**result, "status": status}

    required = set(matrix["target_states"][target_state])
    normalized: list[dict[str, Any]] = []
    gate_statuses: dict[str, AuditStatus] = {}
    for gate in matrix["gates"]:
        statuses: list[AuditStatus] = []
        for check in gate["subchecks"]:
            subcheck_id = check["subcheck_id"]
            if subcheck_id not in required:
                status = AuditStatus.NOT_RUN
                reason = "UNREQUESTED_FOR_TARGET_STATE"
                refs: list[str] = []
                limitations: list[str] = []
            elif not universe_approved and subcheck_id in UNIVERSE_OWNED_SUBCHECKS:
                status = AuditStatus.MISSING_EVIDENCE
                reason = "UNIVERSE_CONTRACT_PENDING_APPROVAL"
                refs = []
                limitations = ["PROVISIONAL_UNIVERSE_CANNOT_SUPPORT_A_STATE_DECISION"]
            elif subcheck_id not in supplied:
                status = AuditStatus.MISSING_EVIDENCE
                reason = "REQUIRED_SUBCHECK_RESULT_NOT_SUPPLIED"
                refs = []
                limitations = []
            else:
                result = supplied[subcheck_id]
                status = result["status"]
                reason = result["reason"]
                refs = list(result["evidence_refs"])
                limitations = list(result["limitations"])
            statuses.append(status)
            normalized.append({
                "gate_id": gate["gate_id"], "subcheck_id": subcheck_id,
                "status": status.value, "reason": reason,
                "evidence_refs": refs, "limitations": limitations,
            })
        gate_statuses[gate["gate_id"]] = _worst(statuses)

    required_statuses = [AuditStatus(item["status"]) for item in normalized if item["subcheck_id"] in required]
    if AuditStatus.FAIL in required_statuses:
        decision = AuditDecision.BLOCKED
        logical_exit_code = 10
    elif all(item in {AuditStatus.PASS, AuditStatus.NOT_APPLICABLE} for item in required_statuses):
        decision = AuditDecision.SUPPORTABLE
        logical_exit_code = 0
    else:
        decision = AuditDecision.INSUFFICIENT_EVIDENCE
        logical_exit_code = 11

    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "classification": "NON_AUTHORIZING_EVIDENCE_CLASSIFICATION",
        "audit_id": invocation["audit_id"],
        "target_state": target_state,
        "target_state_decision": decision.value,
        "logical_exit_code": logical_exit_code,
        "universe_contract_approved": universe_approved,
        "gate_statuses": {key: value.value for key, value in gate_statuses.items()},
        "subchecks": normalized,
        "evidence": [item.__dict__ for item in evidence.values()],
        "authority": {
            "publishes_readiness": False,
            "authorizes_real_history": False,
            "authorizes_holdout_access": False,
            "authorizes_candidate_sealing": False,
            "authorizes_trading": False,
        },
    }
