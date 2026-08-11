"""Prepare-only Tradovate smoke scopes; this module never opens a connection."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Sequence

from .config import CONFIG_RELATIVE_PATH, active_connection, load_account_binding, load_execution_config


SCHEMA = "tradovate_smoke_preparation/1.0.0"
READ_ONLY_OPERATION = "tradovate-read-only-smoke"
SIM_ORDER_OPERATION = "tradovate-sim-order-smoke"
LIVE_ORDER_OPERATION = "tradovate-live-order-smoke"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prepare_scope(*, root: Path, executable: Path, operation: str) -> dict[str, object]:
    if operation == LIVE_ORDER_OPERATION:
        return {
            "schema_version": SCHEMA,
            "operation": operation,
            "classification": "BLOCKED_NOT_EXECUTABLE",
            "execution_authorized": False,
            "provider_connection_authorized": False,
            "blockers": [
                "MFF_LIVE_TRANSITION_NOT_ESTABLISHED",
                "WRITTEN_MFF_PERMISSION_UNCONFIRMED",
                "EXACT_LIVE_ENDPOINT_UNCONFIRMED",
                "EXACT_LIVE_ACCOUNT_BINDING_UNSET",
                "PRODUCTION_READINESS_FALSE",
                "SEPARATE_USER_AUTHORIZATION_REQUIRED",
            ],
        }
    config = load_execution_config(root=root)
    connection_id, connection, connection_hash = active_connection(config)
    if not executable.is_file():
        raise ValueError("exact cockpit executable is missing")
    common: dict[str, object] = {
        "schema_version": SCHEMA,
        "operation": operation,
        "classification": "PREPARE_ONLY_SEPARATE_APPROVAL_REQUIRED",
        "executable_path": str(executable.resolve()),
        "executable_sha256": _sha256(executable),
        "execution_connection_id": connection_id,
        "execution_connection_sha256": connection_hash,
        "execution_config_path": CONFIG_RELATIVE_PATH.as_posix(),
        "execution_config_sha256": _sha256(root / CONFIG_RELATIVE_PATH),
        "provider_id": connection["provider_id"],
        "platform_id": connection["platform_id"],
        "intended_account_stage": "sim_funded",
        "endpoint_environment": "UNCONFIRMED",
        "maximum_duration_seconds": 120,
        "provider_connection_authorized": False,
        "execution_authorized": False,
        "production_readiness": False,
    }
    if operation == READ_ONLY_OPERATION:
        return {
            **common,
            "allowed_operations": ["account_list", "account_read", "position_read", "order_read", "fill_read", "user_sync"],
            "forbidden_operations": ["place", "modify", "cancel", "liquidate", "flatten", "order_strategy_start"],
            "expected_output": "reports/live_cockpit/tradovate_read_only_smoke.json",
        }
    if operation != SIM_ORDER_OPERATION:
        raise ValueError("unsupported Tradovate preparation operation")
    binding = load_account_binding(root=root)
    blockers = [] if binding is not None else ["EXACT_SIM_FUNDED_ACCOUNT_BINDING_UNSET"]
    return {
        **common,
        "classification": "BLOCKED_PREPARE_ONLY" if blockers else common["classification"],
        "account_binding_id": binding.binding_id if binding else None,
        "account_binding_sha256": binding.binding_hash if binding else None,
        "maximum_micro_quantity": 1,
        "maximum_entries": 1,
        "native_protective_stop_required": True,
        "cancel_and_flatten_cleanup_required": True,
        "model_auto_execution": False,
        "reconnect_loop": False,
        "retry_on_unknown_outcome": False,
        "expected_output": "reports/live_cockpit/tradovate_sim_order_smoke.json",
        "blockers": blockers + ["SEPARATE_EXACT_USER_AUTHORIZATION_REQUIRED"],
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation", required=True, choices=[READ_ONLY_OPERATION, SIM_ORDER_OPERATION, LIVE_ORDER_OPERATION])
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--executable", type=Path, default=Path("FuturesLiveCockpit/FuturesLiveCockpit.exe"))
    args = parser.parse_args(argv)
    root = args.root.resolve()
    executable = args.executable if args.executable.is_absolute() else root / args.executable
    print(json.dumps(prepare_scope(root=root, executable=executable, operation=args.operation), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
