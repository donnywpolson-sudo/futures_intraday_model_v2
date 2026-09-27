"""Additive certification route for the reviewed CL -> ZN -> NQ priority gate.

The frozen V10 checkpoint-set certifier remains byte-for-byte unchanged.  This
module validates the separately hash-bound priority gate, then performs the
same two fresh-process replay procedure and emits the same inactive market
certificate schema with an additional successor implementation binding.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from . import causal_observation_market_certification as base
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .causal_observation_market_checkpoint import validate_market_checkpoint_plan
from .causal_observation_priority_successor import validate_priority_sequence
from .errors import IntegrityError, UnauthorizedOperation


IMPLEMENTATION_PATH = Path(
    "src/futures_rebuild/causal_observation_priority_certification.py"
)
GATE_SCHEMA = "causal_observation_priority_certification_gate/1.0.0"


def extend_priority_certification_plan(
    *,
    repository_root: Path,
    base_plan: Mapping[str, object],
    build_plan: Mapping[str, object],
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    gate = build_plan.get("priority_sequence_gate")
    if not isinstance(gate, Mapping) or build_plan.get("target_market") not in {
        "ZN",
        "NQ",
    }:
        raise UnauthorizedOperation("priority certification gate is absent or out of scope")
    if "certified_sequence_gate" in build_plan:
        raise IntegrityError("priority market cannot claim two sequence gates")
    core = {key: value for key, value in base_plan.items() if key != "plan_id"}
    core["priority_certification_gate"] = {
        "schema_version": GATE_SCHEMA,
        "implementation_path": IMPLEMENTATION_PATH.as_posix(),
        "implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
        "base_certification_plan_id": base_plan["plan_id"],
        "transition_id": gate["transition_id"],
        "predecessor_market": gate["predecessor_market"],
        "predecessor_certificate_id": gate["predecessor_certificate_id"],
        "predecessor_certificate_sha256": gate["predecessor_certificate_sha256"],
    }
    return {**core, "plan_id": sha256_json(core)}


def build_priority_market_certification_plan(
    *,
    repository_root: Path,
    build_plan_path: str,
    build_plan: Mapping[str, object],
    build_plan_sha256: str,
) -> dict[str, object]:
    base_plan = base.build_market_certification_plan(
        repository_root=repository_root,
        build_plan_path=build_plan_path,
        build_plan=build_plan,
        build_plan_sha256=build_plan_sha256,
    )
    return extend_priority_certification_plan(
        repository_root=repository_root,
        base_plan=base_plan,
        build_plan=build_plan,
    )


def validate_priority_market_certification_plan(
    repository_root: Path, plan: Mapping[str, object]
) -> None:
    root = repository_root.resolve(strict=True)
    build_plan_path = base._contained(
        root,
        base._plain_relative(plan.get("build_plan_path"), "build plan").as_posix(),
    )
    if sha256_file(build_plan_path) != plan.get("build_plan_sha256"):
        raise IntegrityError("priority certification build plan drifted")
    build_plan = base._json(build_plan_path)
    validate_market_checkpoint_plan(root, build_plan)
    validate_priority_sequence(root, build_plan)
    expected = build_priority_market_certification_plan(
        repository_root=root,
        build_plan_path=build_plan_path.relative_to(root).as_posix(),
        build_plan=build_plan,
        build_plan_sha256=str(plan["build_plan_sha256"]),
    )
    if dict(plan) != expected:
        raise IntegrityError("priority certification plan differs from its build authority")
    checkpoint = base._contained(root, plan["checkpoint_path"])
    if not checkpoint.is_file():
        raise UnauthorizedOperation("priority market checkpoint is not terminal")


def run_authorized_priority_market_certification(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan_path = plan_path.resolve(strict=True)
    try:
        plan_path.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation(
            "priority certification plan is outside the repository"
        ) from exc
    plan = base._json(plan_path)
    validate_priority_market_certification_plan(root, plan)
    checkpoint_path = base._contained(root, plan["checkpoint_path"])
    checkpoint = base._json(checkpoint_path)
    checkpoint_core = {
        key: value for key, value in checkpoint.items() if key != "result_id"
    }
    if (
        checkpoint.get("result_id") != sha256_json(checkpoint_core)
        or checkpoint.get("status") != "PASS_COMPLETE_MARKET_CHECKPOINT_INACTIVE"
        or checkpoint.get("target_market") != plan["target_market"]
        or checkpoint.get("attempt_id") != plan["attempt_id"]
        or checkpoint.get("checkpoint_set_id") != plan["checkpoint_set_id"]
        or checkpoint.get("publication_authorized") is not False
        or checkpoint.get("activation_authorized") is not False
    ):
        raise IntegrityError("priority market checkpoint is not certifiable")
    certificate_path = base._contained(root, plan["certificate_path"])
    failure_path = base._contained(root, plan["failure_path"])
    if base._io_path(certificate_path).exists() or base._io_path(failure_path).exists():
        raise IntegrityError("priority market certification destination already exists")
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
    priority_gate = plan["priority_certification_gate"]
    try:
        passes = tuple(
            base._run_replay_in_fresh_process(root, plan, checkpoint)
            for _ in range(base.REPLAY_PASSES)
        )
        if passes[0] != passes[1]:
            raise IntegrityError("independent priority replay is not deterministic")
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
            "build_plan_id": base._json(
                base._contained(root, plan["build_plan_path"])
            )["plan_id"],
            "certification_plan_id": plan["plan_id"],
            "certifier_implementation_bindings_sha256": plan[
                "certifier_implementation_bindings_sha256"
            ],
            "certifier_implementation_sha256": plan[
                "certifier_implementation_bindings"
            ]["src/futures_rebuild/causal_observation_market_certification.py"],
            "priority_certification_implementation_path": priority_gate[
                "implementation_path"
            ],
            "priority_certification_implementation_sha256": priority_gate[
                "implementation_sha256"
            ],
            "priority_transition_id": priority_gate["transition_id"],
            "priority_predecessor_certificate_id": priority_gate[
                "predecessor_certificate_id"
            ],
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
            "priority_transition_id": priority_gate["transition_id"],
            "priority_predecessor_certificate_id": priority_gate[
                "predecessor_certificate_id"
            ],
            "receipt_id": receipt.receipt_id,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "checkpoint_or_partition_reuse_authorized": False,
            "required_remediation": (
                "GENERAL_FIX_NEW_MARKET_ATTEMPT_AND_NEW_CERTIFICATION_RECEIPT"
            ),
            "publication_authorized": False,
            "activation_authorized": False,
        }
        base._write_create_only(failure_path, failure)
        raise
