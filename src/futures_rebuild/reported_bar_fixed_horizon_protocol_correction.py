"""Additive correction for missing execution bounds in the first protocol."""

from __future__ import annotations

import json
from pathlib import Path

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError


INVALID_PROTOCOL_PATH = Path("configs/reported_bar_fixed_horizon_source_discovery_protocol.json")
INVALID_PROTOCOL_ID = "d1b564c05c8f52dc576e02bb8a13b627899d257a09f72679e3c5137d37738b7e"
INVALID_PROTOCOL_SHA256 = "c30336d78ef2e86724e1911e62ab245c28ae6273b89bee8052ce05f514c4fa0c"
INVALIDITY_ROOT = Path("state/unpublished_evidence/reported_bar_protocol_invalid_preparation")
CORRECTED_PROTOCOL_PATH = Path("configs/reported_bar_fixed_horizon_source_discovery_protocol_v2.json")


def _invalid_protocol(root: Path) -> dict[str, object]:
    path = root / INVALID_PROTOCOL_PATH
    if sha256_file(path) != INVALID_PROTOCOL_SHA256:
        raise IntegrityError("first reported-bar protocol preparation drifted")
    payload = json.loads(path.read_text(encoding="utf-8"))
    core = {key: value for key, value in payload.items() if key != "protocol_id"}
    if payload.get("protocol_id") != INVALID_PROTOCOL_ID or sha256_json(core) != INVALID_PROTOCOL_ID:
        raise IntegrityError("first reported-bar protocol identity is invalid")
    if "execution_limits" in payload:
        raise IntegrityError("first reported-bar protocol does not have the recorded defect")
    return payload


def build_invalidity(*, root: Path) -> dict[str, object]:
    _invalid_protocol(root)
    core: dict[str, object] = {
        "schema_version": "reported_bar_protocol_invalid_preparation/1.0.0",
        "classification": "INVALID_PRE_DATA_MISSING_EXECUTION_BOUNDS",
        "invalid_protocol_id": INVALID_PROTOCOL_ID,
        "invalid_protocol_path": INVALID_PROTOCOL_PATH.as_posix(),
        "invalid_protocol_sha256": INVALID_PROTOCOL_SHA256,
        "historical_rows_read": False,
        "economic_result": "NOT_PRODUCED",
        "published": False,
        "registered": False,
        "executed": False,
        "research_semantics_changed": False,
        "required_correction": "FREEZE_ATTEMPTS_RETRIES_WORKERS_AND_RUNTIME_BEFORE_SOURCE_READ",
    }
    return {**core, "invalidity_id": sha256_json(core)}


def invalidity_path(record: dict[str, object]) -> Path:
    return INVALIDITY_ROOT / str(record["invalidity_id"]) / "invalidity.json"


def build_corrected_protocol(
    *, root: Path, invalidity: dict[str, object]
) -> dict[str, object]:
    predecessor = _invalid_protocol(root)
    core = {key: value for key, value in predecessor.items() if key != "protocol_id"}
    core["schema_version"] = "reported_bar_fixed_horizon_source_discovery_protocol/2.0.0"
    core["supersedes_invalid_protocol_id"] = INVALID_PROTOCOL_ID
    core["invalidity_id"] = invalidity["invalidity_id"]
    core["execution_limits"] = {
        "maximum_attempts": 1,
        "maximum_retries": 0,
        "maximum_workers": 4,
        "worker_deadline_seconds": 3300,
        "maximum_runtime_seconds": 3600,
        "maximum_external_cost_usd": "0",
        "windows_host_required": True,
    }
    bindings = dict(core["bindings"])
    bindings[INVALID_PROTOCOL_PATH.as_posix()] = INVALID_PROTOCOL_SHA256
    bindings[invalidity_path(invalidity).as_posix()] = sha256_file(root / invalidity_path(invalidity))
    bindings["src/futures_rebuild/reported_bar_fixed_horizon_protocol_correction.py"] = sha256_file(
        Path(__file__)
    )
    core["bindings"] = dict(sorted(bindings.items()))
    return {**core, "protocol_id": sha256_json(core)}
