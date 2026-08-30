"""Additive, fail-closed CL -> ZN -> NQ V10 sequence successor.

The original checkpoint-set identity and market order remain immutable.  This
module supplies a separately hash-bound execution gate for the reviewed early
completion priority.  Importing it grants no row, certification, publication,
or activation authority.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from .boundary import OperationReceipt
from .canonical import canonical_bytes, io_path, sha256_file, sha256_json
from .causal_observation_market_checkpoint import (
    run_authorized_market_checkpoint,
    validate_market_checkpoint_plan,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation


CONTRACT_PATH = Path("configs/causal_observation_priority_successor_v1.json")
IMPLEMENTATION_PATH = Path(
    "src/futures_rebuild/causal_observation_priority_successor.py"
)
CONTRACT_SCHEMA = "causal_observation_priority_successor/1.0.0"
TRANSITION_SCHEMA = "causal_observation_priority_transition/1.0.0"
GATE_SCHEMA = "causal_observation_priority_sequence_gate/1.0.0"
CERTIFICATE_SCHEMA = "causal_observation_market_certificate/1.0.0"
PASS_STATUS = "PASS_COMPLETE_MARKET_MAXIMUM_ROBUSTNESS_INACTIVE"
CHECKPOINT_SET_ID = (
    "c10ee5951a386916d1aacdc4105ac7513db32d79349a8a0a91f48e90f42fb476"
)
PRIORITY_ORDER = ("ES", "GC", "6E", "CL", "ZN", "NQ")
CERTIFIED_PREFIX = PRIORITY_ORDER[:4]


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return value


def _relative(value: object, description: str) -> PurePosixPath:
    if type(value) is not str or not value:
        raise ContractError(f"{description} is absent")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ContractError(f"{description} is not repository-relative")
    return path


def _contained(root: Path, value: object, description: str) -> Path:
    path = root.joinpath(*_relative(value, description).parts).resolve(strict=False)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation(f"{description} escapes the repository") from exc
    return path


def load_contract(repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    path = root / CONTRACT_PATH
    contract = _object(path, "priority successor contract")
    prefix = contract.get("certified_prefix")
    if (
        contract.get("schema_version") != CONTRACT_SCHEMA
        or contract.get("checkpoint_set_id") != CHECKPOINT_SET_ID
        or contract.get("completion_priority_order") != list(PRIORITY_ORDER)
        or contract.get("development_end_exclusive")
        != "2025-07-13T22:00:00Z"
        or contract.get("original_checkpoint_market_order_preserved") is not True
        or contract.get("historical_row_read_authorized") is not False
        or contract.get("publication_authorized") is not False
        or contract.get("activation_authorized") is not False
        or not isinstance(prefix, list)
        or [item.get("market") for item in prefix if isinstance(item, Mapping)]
        != list(CERTIFIED_PREFIX)
    ):
        raise IntegrityError("priority successor contract is not exact")
    return {**contract, "contract_sha256": sha256_file(path)}


def _validate_certificate(
    root: Path,
    *,
    path: Path,
    expected_market: str,
    expected_sha256: str | None = None,
    expected_id: str | None = None,
) -> dict[str, object]:
    certificate = _object(path, f"{expected_market} certificate")
    evidence = certificate.get("replay_evidence")
    inventory = evidence.get("output_inventory") if isinstance(evidence, Mapping) else None
    core = {key: value for key, value in certificate.items() if key != "certificate_id"}
    if (
        (expected_sha256 is not None and sha256_file(path) != expected_sha256)
        or (expected_id is not None and certificate.get("certificate_id") != expected_id)
        or certificate.get("schema_version") != CERTIFICATE_SCHEMA
        or certificate.get("status") != PASS_STATUS
        or certificate.get("market") != expected_market
        or certificate.get("certificate_id") != sha256_json(core)
        or certificate.get("checkpoint_set_id") != CHECKPOINT_SET_ID
        or certificate.get("publication_authorized") is not False
        or certificate.get("activation_authorized") is not False
        or certificate.get("replay_passes") != 2
        or certificate.get("provider_calls") != 0
        or certificate.get("holdout_rows") != 0
        or certificate.get("forward_rows") != 0
        or not isinstance(inventory, list)
        or not inventory
        or evidence.get("output_inventory_sha256") != sha256_json(inventory)
    ):
        raise IntegrityError(f"{expected_market} certificate is incompatible")
    for item in inventory:
        if not isinstance(item, Mapping):
            raise IntegrityError("certified output inventory entry is invalid")
        output = _contained(root, item.get("path"), "certified output")
        if (
            not io_path(output).is_file()
            or io_path(output).stat().st_size != item.get("size")
            or sha256_file(io_path(output)) != item.get("sha256")
        ):
            raise IntegrityError("certified predecessor output changed")
    return certificate


def build_transition(repository_root: Path) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    contract = load_contract(root)
    shared: dict[str, str] = {}
    prefix: list[dict[str, object]] = []
    all_paths: list[str] = []
    for binding in contract["certified_prefix"]:  # type: ignore[index]
        market = str(binding["market"])
        path = _contained(root, binding["certificate_path"], "prefix certificate")
        certificate = _validate_certificate(
            root,
            path=path,
            expected_market=market,
            expected_sha256=str(binding["certificate_sha256"]),
            expected_id=str(binding["certificate_id"]),
        )
        if not shared:
            shared = {
                key: str(certificate[key])
                for key in (
                    "source_contract_id",
                    "source_release_id",
                    "causal_contract_id",
                )
            }
        elif any(certificate.get(key) != value for key, value in shared.items()):
            raise IntegrityError("certified priority prefix source bindings differ")
        inventory = certificate["replay_evidence"]["output_inventory"]  # type: ignore[index]
        paths = [str(item["path"]) for item in inventory]  # type: ignore[index]
        all_paths.extend(paths)
        prefix.append(
            {
                "market": market,
                "certificate_path": binding["certificate_path"],
                "certificate_sha256": binding["certificate_sha256"],
                "certificate_id": binding["certificate_id"],
                "output_file_count": len(paths),
                "output_paths_sha256": sha256_json(sorted(paths)),
            }
        )
    if len(all_paths) != len(set(all_paths)):
        raise IntegrityError("certified priority prefix outputs overlap")
    core: dict[str, object] = {
        "schema_version": TRANSITION_SCHEMA,
        "status": "PASS_PRIORITY_PREFIX_REVALIDATED_NO_EXECUTION_AUTHORITY",
        "contract_path": CONTRACT_PATH.as_posix(),
        "contract_sha256": contract["contract_sha256"],
        "checkpoint_set_id": CHECKPOINT_SET_ID,
        "completion_priority_order": list(PRIORITY_ORDER),
        "certified_prefix": prefix,
        **shared,
        "complete_output_file_count": len(all_paths),
        "complete_output_paths_sha256": sha256_json(sorted(all_paths)),
        "market_rows_opened": 0,
        "historical_row_read_authorized": False,
        "certification_authorized": False,
        "publication_authorized": False,
        "activation_authorized": False,
    }
    return {**core, "transition_id": sha256_json(core)}


def validate_transition(repository_root: Path, transition: Mapping[str, object]) -> None:
    if dict(transition) != build_transition(repository_root):
        raise IntegrityError("priority transition differs")


def bind_priority_gate(
    *,
    repository_root: Path,
    build_plan: Mapping[str, object],
    transition_path: str,
    predecessor_certificate_path: str,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    validate_market_checkpoint_plan(root, build_plan)
    market = str(build_plan.get("target_market"))
    if market not in ("ZN", "NQ"):
        raise ContractError("priority successor may bind only ZN or NQ")
    if "certified_sequence_gate" in build_plan or "priority_sequence_gate" in build_plan:
        raise IntegrityError("build plan already contains a sequence gate")
    transition_file = _contained(root, transition_path, "transition")
    transition = _object(transition_file, "priority transition")
    validate_transition(root, transition)
    expected_predecessor = PRIORITY_ORDER[PRIORITY_ORDER.index(market) - 1]
    predecessor_file = _contained(
        root, predecessor_certificate_path, "predecessor certificate"
    )
    predecessor = _validate_certificate(
        root, path=predecessor_file, expected_market=expected_predecessor
    )
    if any(
        predecessor.get(key) != build_plan.get(source_key)
        for key, source_key in (
            ("checkpoint_set_id", "checkpoint_set_id"),
            ("causal_contract_id", "causal_contract_id"),
        )
    ) or any(
        predecessor.get(key) != build_plan["source"].get(source_key)  # type: ignore[index]
        for key, source_key in (
            ("source_contract_id", "source_contract_id"),
            ("source_release_id", "canonical_release_id"),
        )
    ):
        raise IntegrityError("priority predecessor bindings differ from build plan")
    gate = {
        "schema_version": GATE_SCHEMA,
        "implementation_path": IMPLEMENTATION_PATH.as_posix(),
        "implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
        "contract_path": CONTRACT_PATH.as_posix(),
        "contract_sha256": sha256_file(root / CONTRACT_PATH),
        "transition_path": transition_path,
        "transition_sha256": sha256_file(transition_file),
        "transition_id": transition["transition_id"],
        "predecessor_market": expected_predecessor,
        "predecessor_certificate_path": predecessor_certificate_path,
        "predecessor_certificate_sha256": sha256_file(predecessor_file),
        "predecessor_certificate_id": predecessor["certificate_id"],
    }
    core = {key: value for key, value in build_plan.items() if key != "plan_id"}
    core["priority_sequence_gate"] = gate
    result = {**core, "plan_id": sha256_json(core)}
    validate_priority_sequence(root, result)
    return result


def validate_priority_sequence(
    repository_root: Path, build_plan: Mapping[str, object]
) -> None:
    root = repository_root.resolve(strict=True)
    validate_market_checkpoint_plan(root, build_plan)
    market = str(build_plan.get("target_market"))
    gate = build_plan.get("priority_sequence_gate")
    if market not in ("ZN", "NQ") or not isinstance(gate, Mapping):
        raise UnauthorizedOperation("priority sequence gate is absent or out of scope")
    transition_file = _contained(root, gate.get("transition_path"), "transition")
    transition = _object(transition_file, "priority transition")
    validate_transition(root, transition)
    expected_predecessor = PRIORITY_ORDER[PRIORITY_ORDER.index(market) - 1]
    predecessor_file = _contained(
        root, gate.get("predecessor_certificate_path"), "predecessor certificate"
    )
    predecessor = _validate_certificate(
        root, path=predecessor_file, expected_market=expected_predecessor
    )
    if (
        gate.get("schema_version") != GATE_SCHEMA
        or gate.get("implementation_path") != IMPLEMENTATION_PATH.as_posix()
        or gate.get("implementation_sha256") != sha256_file(root / IMPLEMENTATION_PATH)
        or gate.get("contract_path") != CONTRACT_PATH.as_posix()
        or gate.get("contract_sha256") != sha256_file(root / CONTRACT_PATH)
        or gate.get("transition_sha256") != sha256_file(transition_file)
        or gate.get("transition_id") != transition.get("transition_id")
        or gate.get("predecessor_market") != expected_predecessor
        or gate.get("predecessor_certificate_sha256") != sha256_file(predecessor_file)
        or gate.get("predecessor_certificate_id") != predecessor.get("certificate_id")
        or predecessor.get("checkpoint_set_id") != build_plan.get("checkpoint_set_id")
        or predecessor.get("source_contract_id")
        != build_plan["source"].get("source_contract_id")  # type: ignore[index]
        or predecessor.get("source_release_id")
        != build_plan["source"].get("canonical_release_id")  # type: ignore[index]
        or predecessor.get("causal_contract_id") != build_plan.get("causal_contract_id")
    ):
        raise IntegrityError("priority sequence gate is invalid")


def run_authorized_priority_market_checkpoint(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path
):
    root = repository_root.resolve(strict=True)
    plan = _object(plan_path.resolve(strict=True), "priority market plan")
    validate_priority_sequence(root, plan)
    return run_authorized_market_checkpoint(
        repository_root=root, receipt=receipt, plan_path=plan_path
    )


def canonical_artifact(value: Mapping[str, object]) -> bytes:
    return canonical_bytes(dict(value)) + b"\n"
