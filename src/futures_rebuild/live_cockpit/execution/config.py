"""Versioned non-secret execution configuration and local account binding."""

from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

from .domain import AccountBinding, ExecutionMode


CONFIG_RELATIVE_PATH = Path("configs/prop_firm_execution_connections.json")
CAPABILITY_EVIDENCE_RELATIVE_PATH = Path("configs/mff_execution_capability_evidence.json")
BINDING_RELATIVE_PATH = Path("state/live_cockpit/execution_binding.json")
EXPECTED_SCHEMA = "prop_firm_execution_connections/1.1.0"
EXPECTED_CONNECTION_SCHEMA = "mff_tradovate_connection/1.1.0"
EXECUTION_CAPABILITIES = frozenset({"MANUAL_ONLY", "READ_ONLY_API", "ORDER_API"})
EXPECTED_CAPABILITY_EVIDENCE_ID = "mff_support_simulated_accounts_manual_only_2026_08_12"
EXPECTED_SUPPORTED_CAPABILITY_FIELDS = frozenset(
    {
        "evaluation.execution_capability",
        "evaluation.direct_api_read_access",
        "evaluation.direct_api_order_access",
        "sim_funded.execution_capability",
        "sim_funded.direct_api_read_access",
        "sim_funded.direct_api_order_access",
    }
)


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
    evidence = load_capability_evidence(root=root)
    active = value["connections"][value["active_connection_id"]]
    if active.get("capability_evidence_ids") != [evidence["evidence"][0]["evidence_id"]]:
        raise ValueError("active execution capability evidence binding is invalid")
    return value


def _contains_evidence_forbidden_field(value: object) -> bool:
    markers = ("token", "password", "secret", "authorization", "apikey", "accountid", "chatid", "credential")
    if isinstance(value, Mapping):
        return any(
            any("".join(character for character in str(key).lower() if character.isalnum()).endswith(marker) for marker in markers)
            or _contains_evidence_forbidden_field(item)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(_contains_evidence_forbidden_field(item) for item in value)
    return False


def validate_capability_evidence(value: Mapping[str, Any]) -> None:
    if set(value) != {"schema_version", "evidence"} or value.get("schema_version") != "provider_capability_evidence/1.0.0":
        raise ValueError("unsupported provider capability evidence schema")
    records = value.get("evidence")
    if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0], Mapping):
        raise ValueError("provider capability evidence must contain one bounded record")
    record = records[0]
    expected_fields = {
        "evidence_id", "source_type", "provider", "received_date", "scope", "conclusion",
        "support_statement", "transcript_handling", "supported_configuration_fields", "limitations",
    }
    if set(record) != expected_fields or _contains_evidence_forbidden_field(record):
        raise ValueError("provider capability evidence fields are invalid or secret-bearing")
    if (
        record.get("evidence_id") != EXPECTED_CAPABILITY_EVIDENCE_ID
        or record.get("source_type") != "USER_SUPPLIED_FIRST_PARTY_SUPPORT_RESPONSE"
        or record.get("provider") != "my_funded_futures"
        or record.get("scope") != ["evaluation", "rapid_eod_sim_funded"]
        or record.get("transcript_handling") != "FULL_TRANSCRIPT_EXCLUDED_FROM_GIT_USER_CONTROLLED_OUTSIDE_REPOSITORY"
    ):
        raise ValueError("provider capability evidence identity or scope is invalid")
    try:
        date.fromisoformat(str(record.get("received_date")))
    except ValueError as exc:
        raise ValueError("provider capability evidence date is invalid") from exc
    for name in ("conclusion", "support_statement"):
        text = record.get(name)
        if not isinstance(text, str) or not text or len(text) > 600:
            raise ValueError(f"provider capability evidence {name} is invalid")
    supported = record.get("supported_configuration_fields")
    if not isinstance(supported, list) or any(not isinstance(item, str) for item in supported) or frozenset(supported) != EXPECTED_SUPPORTED_CAPABILITY_FIELDS:
        raise ValueError("provider capability evidence supports unexpected fields")
    required_limitations = {
        "DOES_NOT_ESTABLISH_FUTURE_MFF_LIVE_ACCOUNT_CAPABILITY",
        "DOES_NOT_VERIFY_OFFICIAL_COMMISSIONS",
        "DOES_NOT_VERIFY_FUTURE_TRADOVATE_ENTITLEMENT",
        "DOES_NOT_AUTHORIZE_PROVIDER_CONNECTION",
    }
    limitations = record.get("limitations")
    if not isinstance(limitations, list) or set(limitations) != required_limitations:
        raise ValueError("provider capability evidence limitations are invalid")


def load_capability_evidence(*, root: Path) -> dict[str, Any]:
    value = json.loads((root / CAPABILITY_EVIDENCE_RELATIVE_PATH).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("provider capability evidence must be an object")
    validate_capability_evidence(value)
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
    if connection.get("capability_model_version") != "provider_execution_capability/1.0.0":
        raise ValueError("unsupported provider capability model")
    if connection.get("capability_evidence_ids") != [EXPECTED_CAPABILITY_EVIDENCE_ID]:
        raise ValueError("execution capability evidence IDs are invalid")
    capabilities = _mapping(connection.get("stage_capabilities"), name="stage capabilities")
    if set(capabilities) != {"evaluation", "sim_funded", "live"}:
        raise ValueError("stage capabilities must be exact")
    for stage in ("evaluation", "sim_funded"):
        capability = _mapping(capabilities[stage], name=f"{stage} capability")
        if capability.get("execution_capability") != "MANUAL_ONLY":
            raise ValueError(f"{stage} must be manual-only")
        if capability.get("entitlement_status") != "UNAVAILABLE_FOR_SIMULATED_ACCOUNT":
            raise ValueError(f"{stage} simulated entitlement status is invalid")
        if any(
            capability.get(name) is not False
            for name in (
                "direct_api_read_access",
                "direct_api_order_access",
                "provider_api_readiness",
                "automatic_execution_authorized",
            )
        ):
            raise ValueError(f"{stage} API capability must remain false")
    live_capability = _mapping(capabilities["live"], name="live capability")
    if live_capability.get("execution_capability") != "UNCONFIRMED":
        raise ValueError("MFF Live capability must remain unconfirmed")
    if live_capability.get("entitlement_status") != "PENDING_ACTUAL_LIVE_ACCOUNT_VERIFICATION":
        raise ValueError("MFF Live entitlement status is invalid")
    if any(
        live_capability.get(name) is not False
        for name in (
            "direct_api_read_access",
            "direct_api_order_access",
            "provider_api_readiness",
            "automatic_execution_authorized",
        )
    ):
        raise ValueError("MFF Live API capability must remain false")
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


def stage_capability(connection: Mapping[str, Any], *, stage: str) -> Mapping[str, Any]:
    """Return one explicit stage capability; unknown values fail closed."""

    capabilities = _mapping(connection.get("stage_capabilities"), name="stage capabilities")
    if stage not in capabilities:
        raise ValueError("unknown provider stage")
    capability = _mapping(capabilities[stage], name=f"{stage} capability")
    value = capability.get("execution_capability")
    if value == "UNCONFIRMED" and stage == "live":
        return capability
    if value not in EXECUTION_CAPABILITIES:
        raise ValueError("unknown execution capability")
    return capability


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
