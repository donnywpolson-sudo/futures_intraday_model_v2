"""Versioned non-secret execution configuration and local account binding."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

from .domain import AccountBinding, ExecutionMode


CONFIG_RELATIVE_PATH = Path("configs/prop_firm_execution_connections.json")
BINDING_RELATIVE_PATH = Path("state/live_cockpit/execution_binding.json")
EXPECTED_SCHEMA = "prop_firm_execution_connections/1.0.0"
EXPECTED_CONNECTION_SCHEMA = "mff_tradovate_connection/1.0.0"


def canonical_json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode("utf-8")


def sha256_value(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _mapping(value: object, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping")
    return value


def load_execution_config(*, root: Path) -> dict[str, Any]:
    path = root / CONFIG_RELATIVE_PATH
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("execution configuration must be an object")
    validate_execution_config(value)
    return value


def validate_execution_config(value: Mapping[str, Any]) -> None:
    if value.get("schema_version") != EXPECTED_SCHEMA:
        raise ValueError("unsupported execution configuration schema")
    if value.get("startup_mode") != ExecutionMode.OBSERVATION_ONLY.value:
        raise ValueError("startup mode must remain observation-only")
    connections = _mapping(value.get("connections"), name="connections")
    active_id = value.get("active_connection_id")
    if not isinstance(active_id, str) or active_id not in connections:
        raise ValueError("active execution connection is missing")
    connection = _mapping(connections[active_id], name="active connection")
    if connection.get("connection_schema_id") != EXPECTED_CONNECTION_SCHEMA:
        raise ValueError("unsupported execution connection schema")
    if connection.get("provider_id") != "my_funded_futures" or connection.get("platform_id") != "tradovate":
        raise ValueError("active execution candidate identity is invalid")
    if connection.get("entitlement_status") != "UNCONFIRMED":
        raise ValueError("unverified entitlement must remain UNCONFIRMED")
    false_fields = (
        "api_key_generation_confirmed", "rest_access_confirmed", "websocket_access_confirmed",
        "order_permission_confirmed", "production_readiness", "read_only_authorized", "execution_authorized",
    )
    if any(connection.get(name) is not False for name in false_fields):
        raise ValueError("execution candidate must remain fail-closed")
    if connection.get("official_cost_profile") != "UNSET":
        raise ValueError("official execution costs must remain UNSET")
    stage_bindings = _mapping(connection.get("stage_bindings"), name="stage bindings")
    if set(stage_bindings) != {"evaluation", "sim_funded", "live"}:
        raise ValueError("stage bindings must be exact")
    for stage, raw in stage_bindings.items():
        binding = _mapping(raw, name=f"{stage} binding")
        if binding.get("endpoint_environment") != "UNCONFIRMED" or binding.get("exact_account_binding") != "UNSET" or binding.get("execution_authorized") is not False:
            raise ValueError(f"{stage} binding must remain unconfirmed and unauthorized")
    roots = _mapping(connection.get("generic_service_roots"), name="generic service roots")
    expected_roots = {
        "demo_rest": "https://demo.tradovateapi.com/v1",
        "demo_websocket": "wss://demo.tradovateapi.com/v1/websocket",
        "live_rest": "https://live.tradovateapi.com/v1",
        "live_websocket": "wss://live.tradovateapi.com/v1/websocket",
    }
    if dict(roots) != expected_roots:
        raise ValueError("generic Tradovate roots are not exact")
    authority = _mapping(connection.get("authority"), name="authority")
    if not authority or any(item is not False for item in authority.values()):
        raise ValueError("execution authority must remain false")
    blockers = connection.get("blockers")
    if not isinstance(blockers, list) or not blockers or len(blockers) > 64 or any(not isinstance(item, str) or not item for item in blockers):
        raise ValueError("execution blockers are invalid")


def active_connection(value: Mapping[str, Any]) -> tuple[str, Mapping[str, Any], str]:
    validate_execution_config(value)
    active_id = str(value["active_connection_id"])
    connection = _mapping(value["connections"][active_id], name="active connection")
    return active_id, connection, sha256_value(connection)


def binding_path(*, root: Path) -> Path:
    return root / BINDING_RELATIVE_PATH


def load_account_binding(*, root: Path) -> AccountBinding | None:
    path = binding_path(root=root)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if not isinstance(raw, dict):
        raise ValueError("account binding must be an object")
    core = {key: value for key, value in raw.items() if key != "binding_hash"}
    if raw.get("binding_hash") != sha256_value(core):
        raise ValueError("account binding hash does not match")
    return AccountBinding(
        **{**raw, "created_at": datetime.fromisoformat(str(raw["created_at"]).replace("Z", "+00:00"))}
    )


def write_account_binding(*, root: Path, value: Mapping[str, Any]) -> AccountBinding:
    """Write a local non-secret binding atomically; never select an account automatically."""

    if "binding_hash" in value:
        raise ValueError("binding_hash is derived")
    core = dict(value)
    core["binding_hash"] = sha256_value(core)
    binding = AccountBinding(
        **{**core, "created_at": datetime.fromisoformat(str(core["created_at"]).replace("Z", "+00:00"))}
    )
    path = binding_path(root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(canonical_json(core))
    os.replace(temporary, path)
    return binding
