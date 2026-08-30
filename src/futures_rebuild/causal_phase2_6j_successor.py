"""One 6J Phase 2 build bound to the exact certified ZB predecessor."""

from __future__ import annotations

import subprocess
from collections.abc import Mapping
from pathlib import Path

from . import causal_observation_market_certification as certification
from .alpha_ladder_macro_successor import CONTRACT_PATH, load_successor_contract
from .boundary import OperationReceipt
from .canonical import sha256_file, sha256_json
from .causal_observation_market_checkpoint import (
    run_authorized_market_checkpoint,
    validate_market_checkpoint_plan,
)
from .causal_observation_priority_successor import _validate_certificate
from .errors import IntegrityError, UnauthorizedOperation


IMPLEMENTATION_PATH = Path("src/futures_rebuild/causal_phase2_6j_successor.py")
ZB_CERTIFICATE_PATH = Path(
    "state/causal_observation_market_certificates/"
    "c10ee5951a386916d1aacdc4105ac7513db32d79349a8a0a91f48e90f42fb476/"
    "ZB/360526c8c5bf0f085b4d981ff2c661e499da42e572fca817b0a1cc3e5aeb2c13.json"
)
ZB_CERTIFICATE_SHA256 = "58be2e48944759ef3dde39065094da7dd10830f6b9f60947659c8e140227783c"
ZB_CERTIFICATE_ID = "f98c54efadf9cd335771fd793fde313809934fea7e9e1c1fe1120f68da2c0e88"
EXPECTED_CONTRACT_ID = "662f78ebc4a9578759a0c09655006692c6b770ea34d2650e04294b15865582c5"
PHASE2_ORDER = ("SR3", "ZB", "6J")
GATE_SCHEMA = "causal_observation_phase2_6j_successor_gate/1.0.0"
MAXIMUM_PEAK_ADDITIONAL_BYTES = 20_000_000_000
REQUIRED_FREE_AFTER_PEAK_BYTES = 107_374_182_400


def _head(root: Path) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()


def _exact_zb_certificate(root: Path, path: str) -> dict[str, object]:
    certificate_path = certification._contained(root, path)
    if certificate_path != (root / ZB_CERTIFICATE_PATH).resolve(strict=True):
        raise UnauthorizedOperation("6J requires the frozen ZB certificate path")
    certificate = _validate_certificate(root, path=certificate_path, expected_market="ZB")
    if (
        sha256_file(certificate_path) != ZB_CERTIFICATE_SHA256
        or certificate.get("certificate_id") != ZB_CERTIFICATE_ID
        or certificate.get("status") != "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE"
        or certificate.get("publication_authorized") is not False
        or certificate.get("activation_authorized") is not False
    ):
        raise IntegrityError("frozen ZB predecessor certificate differs")
    return certificate


def bind_6j_successor_gate(
    *, repository_root: Path, build_plan: Mapping[str, object],
    predecessor_certificate_path: str,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    validate_market_checkpoint_plan(root, build_plan)
    if build_plan.get("target_market") != "6J" or "phase2_priority_gate" in build_plan:
        raise UnauthorizedOperation("6J successor requires an unbound 6J plan")
    predecessor = _exact_zb_certificate(root, predecessor_certificate_path)
    contract = load_successor_contract(root)
    core = {key: value for key, value in build_plan.items() if key != "plan_id"}
    core["phase2_priority_gate"] = {
        "schema_version": GATE_SCHEMA,
        "implementation_path": IMPLEMENTATION_PATH.as_posix(),
        "implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
        "git_head": _head(root),
        "contract_path": CONTRACT_PATH.as_posix(),
        "contract_sha256": sha256_file(root / CONTRACT_PATH),
        "contract_id": contract["contract_id"],
        "phase2_priority": list(PHASE2_ORDER),
        "predecessor_market": "ZB",
        "predecessor_certificate_path": predecessor_certificate_path,
        "predecessor_certificate_sha256": ZB_CERTIFICATE_SHA256,
        "predecessor_certificate_id": ZB_CERTIFICATE_ID,
        "predecessor_certification_plan_id": predecessor["certification_plan_id"],
        "maximum_peak_additional_bytes": MAXIMUM_PEAK_ADDITIONAL_BYTES,
        "required_free_after_peak_bytes": REQUIRED_FREE_AFTER_PEAK_BYTES,
        "research_rules_changed": False,
        "automatic_retry_authorized": False,
    }
    result = {**core, "plan_id": sha256_json(core)}
    validate_6j_successor_gate(root, result)
    return result


def validate_6j_successor_gate(
    repository_root: Path, build_plan: Mapping[str, object],
) -> None:
    root = repository_root.resolve(strict=True)
    validate_market_checkpoint_plan(root, build_plan)
    gate = build_plan.get("phase2_priority_gate")
    if build_plan.get("target_market") != "6J" or not isinstance(gate, Mapping):
        raise UnauthorizedOperation("6J successor gate is absent")
    predecessor = _exact_zb_certificate(
        root, str(gate.get("predecessor_certificate_path"))
    )
    contract = load_successor_contract(root)
    if (
        gate.get("schema_version") != GATE_SCHEMA
        or gate.get("implementation_path") != IMPLEMENTATION_PATH.as_posix()
        or gate.get("implementation_sha256") != sha256_file(root / IMPLEMENTATION_PATH)
        or gate.get("git_head") != _head(root)
        or gate.get("contract_path") != CONTRACT_PATH.as_posix()
        or gate.get("contract_sha256") != sha256_file(root / CONTRACT_PATH)
        or gate.get("contract_id") != EXPECTED_CONTRACT_ID
        or contract.get("contract_id") != EXPECTED_CONTRACT_ID
        or contract.get("phase2_priority") != list(PHASE2_ORDER)
        or gate.get("phase2_priority") != list(PHASE2_ORDER)
        or gate.get("predecessor_market") != "ZB"
        or gate.get("predecessor_certificate_sha256") != ZB_CERTIFICATE_SHA256
        or gate.get("predecessor_certificate_id") != ZB_CERTIFICATE_ID
        or gate.get("predecessor_certification_plan_id")
        != predecessor.get("certification_plan_id")
        or predecessor.get("checkpoint_set_id") != build_plan.get("checkpoint_set_id")
        or predecessor.get("source_contract_id")
        != build_plan["source"].get("source_contract_id")
        or predecessor.get("source_release_id")
        != build_plan["source"].get("canonical_release_id")
        or predecessor.get("causal_contract_id") != build_plan.get("causal_contract_id")
        or gate.get("maximum_peak_additional_bytes") != MAXIMUM_PEAK_ADDITIONAL_BYTES
        or gate.get("required_free_after_peak_bytes") != REQUIRED_FREE_AFTER_PEAK_BYTES
        or gate.get("research_rules_changed") is not False
        or gate.get("automatic_retry_authorized") is not False
    ):
        raise IntegrityError("6J successor gate differs")


def run_authorized_6j_successor(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path,
):
    root = repository_root.resolve(strict=True)
    plan = certification._json(plan_path.resolve(strict=True))
    validate_6j_successor_gate(root, plan)
    return run_authorized_market_checkpoint(
        repository_root=root, receipt=receipt, plan_path=plan_path,
    )
