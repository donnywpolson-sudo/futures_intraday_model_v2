"""Inactive V10 checkpoint/certification gate for SR3 -> ZB -> 6J."""

from __future__ import annotations

import subprocess
from collections.abc import Mapping
from pathlib import Path

from . import causal_observation_market_certification as base
from .alpha_ladder_macro_successor import CONTRACT_PATH, load_successor_contract
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .causal_observation_market_checkpoint import (
    run_authorized_market_checkpoint,
    validate_market_checkpoint_plan,
)
from .causal_observation_priority_successor import _validate_certificate
from .errors import ContractError, IntegrityError, UnauthorizedOperation


IMPLEMENTATION_PATH = Path("src/futures_rebuild/causal_phase2_priority.py")
EXPECTED_HEAD = "01d77e7dfc7a1c6f5180c0592212a5b1b93f30a4"
EXPECTED_CONTRACT_ID = "662f78ebc4a9578759a0c09655006692c6b770ea34d2650e04294b15865582c5"
PHASE2_ORDER = ("SR3", "ZB", "6J")
PREDECESSORS = {"SR3": "6B", "ZB": "SR3", "6J": "ZB"}
GATE_SCHEMA = "causal_observation_phase2_priority_gate/1.0.0"
CERTIFICATION_GATE_SCHEMA = "causal_observation_phase2_priority_certification_gate/1.0.0"


def _head(root: Path) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()


def bind_phase2_priority_gate(
    *,
    repository_root: Path,
    build_plan: Mapping[str, object],
    predecessor_certificate_path: str,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    validate_market_checkpoint_plan(root, build_plan)
    market = str(build_plan.get("target_market"))
    if market not in PHASE2_ORDER:
        raise ContractError("phase-2 priority gate is outside SR3, ZB, 6J")
    if any(name in build_plan for name in ("certified_sequence_gate", "priority_sequence_gate", "phase2_priority_gate")):
        raise IntegrityError("build plan already contains a sequence gate")
    predecessor_path = base._contained(root, predecessor_certificate_path)
    predecessor = _validate_certificate(
        root, path=predecessor_path, expected_market=PREDECESSORS[market]
    )
    contract = load_successor_contract(root)
    core = {key: value for key, value in build_plan.items() if key != "plan_id"}
    core["phase2_priority_gate"] = {
        "schema_version": GATE_SCHEMA,
        "implementation_path": IMPLEMENTATION_PATH.as_posix(),
        "implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
        "git_head": EXPECTED_HEAD,
        "contract_path": CONTRACT_PATH.as_posix(),
        "contract_sha256": sha256_file(root / CONTRACT_PATH),
        "contract_id": contract["contract_id"],
        "phase2_priority": list(PHASE2_ORDER),
        "predecessor_market": PREDECESSORS[market],
        "predecessor_certificate_path": predecessor_certificate_path,
        "predecessor_certificate_sha256": sha256_file(predecessor_path),
        "predecessor_certificate_id": predecessor["certificate_id"],
        "sr3_pre_effective_coverage_exemption": False,
    }
    result = {**core, "plan_id": sha256_json(core)}
    validate_phase2_priority_gate(root, result)
    return result


def validate_phase2_priority_gate(
    repository_root: Path, build_plan: Mapping[str, object]
) -> None:
    root = repository_root.resolve(strict=True)
    validate_market_checkpoint_plan(root, build_plan)
    market = str(build_plan.get("target_market"))
    gate = build_plan.get("phase2_priority_gate")
    if market not in PHASE2_ORDER or not isinstance(gate, Mapping):
        raise UnauthorizedOperation("phase-2 priority gate is absent or out of scope")
    predecessor_path = base._contained(root, gate.get("predecessor_certificate_path"))
    predecessor = _validate_certificate(
        root, path=predecessor_path, expected_market=PREDECESSORS[market]
    )
    contract = load_successor_contract(root)
    if (
        _head(root) != EXPECTED_HEAD
        or gate.get("schema_version") != GATE_SCHEMA
        or gate.get("implementation_path") != IMPLEMENTATION_PATH.as_posix()
        or gate.get("implementation_sha256") != sha256_file(root / IMPLEMENTATION_PATH)
        or gate.get("git_head") != EXPECTED_HEAD
        or gate.get("contract_path") != CONTRACT_PATH.as_posix()
        or gate.get("contract_sha256") != sha256_file(root / CONTRACT_PATH)
        or gate.get("contract_id") != EXPECTED_CONTRACT_ID
        or contract.get("contract_id") != EXPECTED_CONTRACT_ID
        or contract.get("phase2_priority") != list(PHASE2_ORDER)
        or gate.get("phase2_priority") != list(PHASE2_ORDER)
        or gate.get("predecessor_market") != PREDECESSORS[market]
        or gate.get("predecessor_certificate_sha256") != sha256_file(predecessor_path)
        or gate.get("predecessor_certificate_id") != predecessor.get("certificate_id")
        or gate.get("sr3_pre_effective_coverage_exemption") is not False
        or contract["measurement_schedule"]["sr3_pre_effective_coverage_exemption"] is not False
        or predecessor.get("checkpoint_set_id") != build_plan.get("checkpoint_set_id")
        or predecessor.get("source_contract_id") != build_plan["source"].get("source_contract_id")
        or predecessor.get("source_release_id") != build_plan["source"].get("canonical_release_id")
        or predecessor.get("causal_contract_id") != build_plan.get("causal_contract_id")
    ):
        raise IntegrityError("phase-2 priority gate is invalid")


def run_authorized_phase2_market_checkpoint(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path
):
    root = repository_root.resolve(strict=True)
    plan = base._json(plan_path.resolve(strict=True))
    validate_phase2_priority_gate(root, plan)
    return run_authorized_market_checkpoint(
        repository_root=root, receipt=receipt, plan_path=plan_path
    )


def build_phase2_market_certification_plan(
    *,
    repository_root: Path,
    build_plan_path: str,
    build_plan: Mapping[str, object],
    build_plan_sha256: str,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    validate_phase2_priority_gate(root, build_plan)
    base_plan = base.build_market_certification_plan(
        repository_root=root,
        build_plan_path=build_plan_path,
        build_plan=build_plan,
        build_plan_sha256=build_plan_sha256,
    )
    gate = build_plan["phase2_priority_gate"]
    core = {key: value for key, value in base_plan.items() if key != "plan_id"}
    core["phase2_priority_certification_gate"] = {
        "schema_version": CERTIFICATION_GATE_SCHEMA,
        "implementation_path": IMPLEMENTATION_PATH.as_posix(),
        "implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
        "base_certification_plan_id": base_plan["plan_id"],
        "git_head": EXPECTED_HEAD,
        "contract_id": gate["contract_id"],
        "predecessor_market": gate["predecessor_market"],
        "predecessor_certificate_id": gate["predecessor_certificate_id"],
        "predecessor_certificate_sha256": gate["predecessor_certificate_sha256"],
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_phase2_market_certification_plan(
    repository_root: Path, plan: Mapping[str, object]
) -> None:
    root = repository_root.resolve(strict=True)
    build_plan_path = base._contained(root, plan.get("build_plan_path"))
    if sha256_file(build_plan_path) != plan.get("build_plan_sha256"):
        raise IntegrityError("phase-2 certification build plan drifted")
    build_plan = base._json(build_plan_path)
    validate_phase2_priority_gate(root, build_plan)
    expected = build_phase2_market_certification_plan(
        repository_root=root,
        build_plan_path=build_plan_path.relative_to(root).as_posix(),
        build_plan=build_plan,
        build_plan_sha256=str(plan["build_plan_sha256"]),
    )
    if dict(plan) != expected:
        raise IntegrityError("phase-2 certification plan differs from build authority")
    if not base._contained(root, plan["checkpoint_path"]).is_file():
        raise UnauthorizedOperation("phase-2 checkpoint is not terminal")


def run_authorized_phase2_market_certification(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan_path = plan_path.resolve(strict=True)
    try:
        plan_path.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation("phase-2 certification plan is outside repository") from exc
    plan = base._json(plan_path)
    validate_phase2_market_certification_plan(root, plan)
    checkpoint_path = base._contained(root, plan["checkpoint_path"])
    checkpoint = base._json(checkpoint_path)
    checkpoint_core = {key: value for key, value in checkpoint.items() if key != "result_id"}
    if (
        checkpoint.get("result_id") != sha256_json(checkpoint_core)
        or checkpoint.get("status") != "PASS_COMPLETE_MARKET_CHECKPOINT_INACTIVE"
        or checkpoint.get("target_market") != plan["target_market"]
        or checkpoint.get("attempt_id") != plan["attempt_id"]
        or checkpoint.get("checkpoint_set_id") != plan["checkpoint_set_id"]
        or checkpoint.get("publication_authorized") is not False
        or checkpoint.get("activation_authorized") is not False
    ):
        raise IntegrityError("phase-2 checkpoint is not certifiable")
    certificate_path = base._contained(root, plan["certificate_path"])
    failure_path = base._contained(root, plan["failure_path"])
    if base._io_path(certificate_path).exists() or base._io_path(failure_path).exists():
        raise IntegrityError("phase-2 certification destination already exists")
    receipt.consume(
        boundary,
        operation=base.CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=base.required_market_certification_scope(
            plan=plan,
            plan_sha256=sha256_file(plan_path),
            checkpoint_sha256=sha256_file(checkpoint_path),
        ),
    )
    gate = plan["phase2_priority_certification_gate"]
    try:
        passes = tuple(
            base._run_replay_in_fresh_process(root, plan, checkpoint)
            for _ in range(base.REPLAY_PASSES)
        )
        if passes[0] != passes[1]:
            raise IntegrityError("independent phase-2 replay is not deterministic")
        evidence = passes[0]
        core = {
            "schema_version": base.CERTIFICATE_SCHEMA,
            "status": "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE",
            "market": plan["target_market"],
            "attempt_id": plan["attempt_id"],
            "checkpoint_set_id": plan["checkpoint_set_id"],
            "checkpoint_result_id": checkpoint["result_id"],
            "checkpoint_sha256": sha256_file(checkpoint_path),
            "source_contract_id": plan["source"]["source_contract_id"],
            "source_release_id": plan["source"]["canonical_release_id"],
            "causal_contract_id": base.CAUSAL_OBSERVATION_CONTRACT_ID,
            "economics_rulebook_sha256": base.ECONOMICS_RULEBOOK_SHA256,
            "build_plan_id": base._json(base._contained(root, plan["build_plan_path"]))["plan_id"],
            "certification_plan_id": plan["plan_id"],
            "certifier_implementation_bindings_sha256": plan["certifier_implementation_bindings_sha256"],
            "certifier_implementation_sha256": plan["certifier_implementation_bindings"]["src/futures_rebuild/causal_observation_market_certification.py"],
            "phase2_priority_implementation_path": gate["implementation_path"],
            "phase2_priority_implementation_sha256": gate["implementation_sha256"],
            "phase2_priority_contract_id": gate["contract_id"],
            "phase2_priority_predecessor_certificate_id": gate["predecessor_certificate_id"],
            "receipt_id": receipt.receipt_id,
            "replay_passes": base.REPLAY_PASSES,
            "replay_evidence_id": evidence.evidence_id,
            "replay_evidence": evidence.as_dict(),
            "provider_calls": 0,
            "holdout_rows": 0,
            "forward_rows": 0,
            "outcomes": 0,
            "features": 0,
            "fitting": 0,
            "predictions": 0,
            "evaluations": 0,
            "publication_authorized": False,
            "activation_authorized": False,
        }
        certificate = {**core, "certificate_id": sha256_json(core)}
        base._write_create_only(certificate_path, certificate)
        return certificate
    except (Exception, KeyboardInterrupt) as exc:
        failure = {
            "schema_version": "causal_observation_market_certification_failure/1.0.0",
            "status": "FAILED_MARKET_CERTIFICATION_CHECKPOINT_PRESERVED",
            "market": plan["target_market"],
            "attempt_id": plan["attempt_id"],
            "checkpoint_set_id": plan["checkpoint_set_id"],
            "phase2_priority_contract_id": gate["contract_id"],
            "phase2_priority_predecessor_certificate_id": gate["predecessor_certificate_id"],
            "receipt_id": receipt.receipt_id,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "checkpoint_or_partition_reuse_authorized": False,
            "required_remediation": "GENERAL_FIX_NEW_MARKET_ATTEMPT_AND_NEW_CERTIFICATION_RECEIPT",
            "publication_authorized": False,
            "activation_authorized": False,
        }
        base._write_create_only(failure_path, failure)
        raise
