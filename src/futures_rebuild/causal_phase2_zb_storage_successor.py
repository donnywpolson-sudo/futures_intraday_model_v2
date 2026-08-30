"""One fresh ZB Phase 2 attempt after the terminal pre-row storage abort."""

from __future__ import annotations

import subprocess
from collections.abc import Mapping
from pathlib import Path

from . import causal_observation_market_certification as certification
from .boundary import OperationReceipt
from .canonical import sha256_file, sha256_json
from .causal_observation_market_checkpoint import (
    run_authorized_market_checkpoint,
    validate_market_checkpoint_plan,
)
from .causal_observation_priority_successor import _validate_certificate
from .errors import IntegrityError, UnauthorizedOperation


IMPLEMENTATION_PATH = Path("src/futures_rebuild/causal_phase2_zb_storage_successor.py")
FAILED_TERMINAL_PATH = Path(
    "state/causal_full_build_host/causal_observation_full_development_bounded_2025_v10/"
    "ZB/200b21552c61f683584e3f7d58e70da887f67bf98a0412ca750efc7a8bffb638/exit.json"
)
FAILED_TERMINAL_SHA256 = "b647bb2e41328dd99f16c4b756d4e6a72e7fbea8a103ed0d1ed60be94bf0cb5a"
FAILED_ATTEMPT_ID = "200b21552c61f683584e3f7d58e70da887f67bf98a0412ca750efc7a8bffb638"
FAILED_RECEIPT_PATH = Path(
    "reports/bounded_2025_full_build_v10_preparation/"
    "b25fbv10p_zb_20260829T2357598519134Z_01d77e7d/"
    "ZB_COMPLETE_MARKET_AUTHORIZATION_RECEIPT_V10.json"
)
FAILED_RECEIPT_SHA256 = "9b7f36b116eb04d93be5899d662efebe3bcda4fbbde72b5414c65e4fdaec104b"
FAILED_RECEIPT_ID = "cfac1ad3cf927683e25abdedeff8547f77fd3c9f36675e284cd65b6ecb2af905"
FAILED_OUTPUT_PATH = Path(
    "data/causally_gated_normalized/v10/"
    "c10ee5951a386916d1aacdc4105ac7513db32d79349a8a0a91f48e90f42fb476/"
    f"ZB/{FAILED_ATTEMPT_ID}"
)
PREDECESSOR_MARKET = "SR3"
PHASE2_ORDER = ("SR3", "ZB", "6J")
GATE_SCHEMA = "causal_observation_phase2_zb_storage_successor_gate/1.0.0"
MAXIMUM_PEAK_ADDITIONAL_BYTES = 20_000_000_000
REQUIRED_FREE_AFTER_PEAK_BYTES = 107_374_182_400


def _head(root: Path) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()


def _failed_preflight(root: Path) -> dict[str, object]:
    terminal = certification._json(root / FAILED_TERMINAL_PATH)
    receipt = certification._json(root / FAILED_RECEIPT_PATH)
    if (
        sha256_file(root / FAILED_TERMINAL_PATH) != FAILED_TERMINAL_SHA256
        or terminal.get("status") != "FAILED"
        or terminal.get("error_message") != "ZB free-space floor would be breached"
        or terminal.get("automatic_restart_authorized") is not False
        or sha256_file(root / FAILED_RECEIPT_PATH) != FAILED_RECEIPT_SHA256
        or receipt.get("receipt_id") != FAILED_RECEIPT_ID
        or (root / "state/authorization_uses" / f"{FAILED_RECEIPT_ID}.json").exists()
        or (root / FAILED_OUTPUT_PATH).exists()
    ):
        raise IntegrityError("terminal ZB pre-row storage failure differs")
    return terminal


def bind_zb_storage_successor_gate(
    *, repository_root: Path, build_plan: Mapping[str, object],
    predecessor_certificate_path: str,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    validate_market_checkpoint_plan(root, build_plan)
    if build_plan.get("target_market") != "ZB" or "phase2_priority_gate" in build_plan:
        raise UnauthorizedOperation("storage successor is ZB-only and requires an unbound plan")
    predecessor_path = certification._contained(root, predecessor_certificate_path)
    predecessor = _validate_certificate(root, path=predecessor_path, expected_market=PREDECESSOR_MARKET)
    terminal = _failed_preflight(root)
    core = {key: value for key, value in build_plan.items() if key != "plan_id"}
    core["phase2_priority_gate"] = {
        "schema_version": GATE_SCHEMA,
        "implementation_path": IMPLEMENTATION_PATH.as_posix(),
        "implementation_sha256": sha256_file(root / IMPLEMENTATION_PATH),
        "git_head": _head(root),
        "phase2_priority": list(PHASE2_ORDER),
        "predecessor_market": PREDECESSOR_MARKET,
        "predecessor_certificate_path": predecessor_certificate_path,
        "predecessor_certificate_sha256": sha256_file(predecessor_path),
        "predecessor_certificate_id": predecessor["certificate_id"],
        "failed_attempt_id": FAILED_ATTEMPT_ID,
        "failed_terminal_path": FAILED_TERMINAL_PATH.as_posix(),
        "failed_terminal_sha256": FAILED_TERMINAL_SHA256,
        "failed_terminal_id": terminal["exit_id"],
        "failed_receipt_path": FAILED_RECEIPT_PATH.as_posix(),
        "failed_receipt_sha256": FAILED_RECEIPT_SHA256,
        "failed_receipt_id": FAILED_RECEIPT_ID,
        "failed_receipt_consumed": False,
        "failed_attempt_market_rows_read": 0,
        "failure_classification": "INFRASTRUCTURE_PREFLIGHT_STORAGE_ONLY",
        "research_rules_changed": False,
        "maximum_peak_additional_bytes": MAXIMUM_PEAK_ADDITIONAL_BYTES,
        "required_free_after_peak_bytes": REQUIRED_FREE_AFTER_PEAK_BYTES,
        "automatic_retry_authorized": False,
    }
    result = {**core, "plan_id": sha256_json(core)}
    validate_zb_storage_successor_gate(root, result)
    return result


def validate_zb_storage_successor_gate(
    repository_root: Path, build_plan: Mapping[str, object],
) -> None:
    root = repository_root.resolve(strict=True)
    validate_market_checkpoint_plan(root, build_plan)
    gate = build_plan.get("phase2_priority_gate")
    if build_plan.get("target_market") != "ZB" or not isinstance(gate, Mapping):
        raise UnauthorizedOperation("ZB storage-successor gate is absent")
    predecessor_path = certification._contained(root, gate.get("predecessor_certificate_path"))
    predecessor = _validate_certificate(root, path=predecessor_path, expected_market=PREDECESSOR_MARKET)
    terminal = _failed_preflight(root)
    if (
        gate.get("schema_version") != GATE_SCHEMA
        or gate.get("implementation_path") != IMPLEMENTATION_PATH.as_posix()
        or gate.get("implementation_sha256") != sha256_file(root / IMPLEMENTATION_PATH)
        or gate.get("git_head") != _head(root)
        or gate.get("phase2_priority") != list(PHASE2_ORDER)
        or gate.get("predecessor_market") != PREDECESSOR_MARKET
        or gate.get("predecessor_certificate_sha256") != sha256_file(predecessor_path)
        or gate.get("predecessor_certificate_id") != predecessor.get("certificate_id")
        or gate.get("failed_attempt_id") != FAILED_ATTEMPT_ID
        or gate.get("failed_terminal_path") != FAILED_TERMINAL_PATH.as_posix()
        or gate.get("failed_terminal_sha256") != FAILED_TERMINAL_SHA256
        or gate.get("failed_terminal_id") != terminal.get("exit_id")
        or gate.get("failed_receipt_path") != FAILED_RECEIPT_PATH.as_posix()
        or gate.get("failed_receipt_sha256") != FAILED_RECEIPT_SHA256
        or gate.get("failed_receipt_id") != FAILED_RECEIPT_ID
        or gate.get("failed_receipt_consumed") is not False
        or gate.get("failed_attempt_market_rows_read") != 0
        or gate.get("failure_classification") != "INFRASTRUCTURE_PREFLIGHT_STORAGE_ONLY"
        or gate.get("research_rules_changed") is not False
        or gate.get("maximum_peak_additional_bytes") != MAXIMUM_PEAK_ADDITIONAL_BYTES
        or gate.get("required_free_after_peak_bytes") != REQUIRED_FREE_AFTER_PEAK_BYTES
        or gate.get("automatic_retry_authorized") is not False
    ):
        raise IntegrityError("ZB storage-successor gate differs")


def run_authorized_zb_storage_successor(
    *, repository_root: Path, receipt: OperationReceipt, plan_path: Path,
):
    root = repository_root.resolve(strict=True)
    plan = certification._json(plan_path.resolve(strict=True))
    validate_zb_storage_successor_gate(root, plan)
    return run_authorized_market_checkpoint(
        repository_root=root, receipt=receipt, plan_path=plan_path,
    )
