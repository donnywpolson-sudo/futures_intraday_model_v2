"""Exact content-hash approval gate for the bounded provider-backed smoke."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from futures_rebuild.canonical import sha256_file, sha256_json

from .feed import chart_market_universe


PLAN_SCHEMA = "futures_live_cockpit_smoke_plan/1.1.0"
APPROVAL_SCHEMA = "futures_live_cockpit_smoke_approval/1.1.0"
OPERATION = "RUN_BOUNDED_OBSERVATION_ONLY_DATABENTO_SMOKE_SUCCESSOR"
PREDECESSOR_ATTEMPT = {
    "plan_id": "65a2e0b45a68f595d49609822fe27ad054cfebe156a7eb7da32c84e2b9e624ac",
    "plan_sha256": "0cfb48fbb447cc557f1a9761e73f9acd7ccfe91d214d7cac98d315a4e7212090",
    "approval_receipt_id": (
        "58a968061bc50a009d26ca80fe2c7fd90b0901e554e6b90904f7619d4c050d6d"
    ),
    "result_id": "e3647f34a14da41df8f9a3434d54fd1ff39581709604b450a6c6451cd4fdf74f",
    "result_sha256": (
        "1dc195a4b6aba23d879eaa0d139018efe7bbf3d6a2687cafdc7d0795394a3752"
    ),
    "disposition": "FAIL_NO_CUTOVER",
}
_HASH = re.compile(r"[0-9a-f]{64}")
_UTC_SECOND = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


class LiveSmokeApprovalError(RuntimeError):
    """Raised before any provider client is created."""


def _load_object(path: Path, name: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LiveSmokeApprovalError(f"{name} is not readable JSON") from exc
    if type(payload) is not dict:
        raise LiveSmokeApprovalError(f"{name} must be an exact object")
    return payload


def validate_live_smoke_plan(payload: Mapping[str, Any]) -> dict[str, Any]:
    expected_keys = {
        "schema_version",
        "classification",
        "operation",
        "scope",
        "execution_authorized",
        "plan_id",
        "predecessor_attempt",
    }
    if set(payload) != expected_keys:
        raise LiveSmokeApprovalError("live-smoke plan fields are invalid")
    core = {key: payload[key] for key in payload if key != "plan_id"}
    if (
        payload["schema_version"] != PLAN_SCHEMA
        or payload["classification"] != "PENDING_EXACT_HASH_BOUND_APPROVAL"
        or payload["operation"] != OPERATION
        or payload["execution_authorized"] is not False
        or payload["predecessor_attempt"] != PREDECESSOR_ATTEMPT
        or payload["plan_id"] != sha256_json(core)
    ):
        raise LiveSmokeApprovalError("live-smoke plan identity is invalid")
    scope = payload["scope"]
    if type(scope) is not dict or set(scope) != {
        "dataset",
        "overview_markets",
        "focus_market",
        "duration_seconds",
        "maximum_live_sessions",
        "historical_replay",
        "cache_mutation",
        "reconnect_loop",
        "order_paths",
        "secret_logging",
        "result_output_relative",
        "prepared_executable_sha256",
        "runtime_frozen",
    }:
        raise LiveSmokeApprovalError("live-smoke scope fields are invalid")
    markets = scope["overview_markets"]
    approved_markets = sorted(info.symbol for info in chart_market_universe())
    if (
        scope["dataset"] != "GLBX.MDP3"
        or type(markets) is not list
        or markets != approved_markets
        or scope["focus_market"] != "ES"
        or scope["duration_seconds"] != 120
        or scope["maximum_live_sessions"] != 2
        or scope["result_output_relative"]
        != "reports/live_cockpit/bounded_live_smoke_result_attempt_2.json"
        or type(scope["prepared_executable_sha256"]) is not str
        or _HASH.fullmatch(scope["prepared_executable_sha256"]) is None
        or scope["runtime_frozen"] is not True
        or any(
            scope[key] is not False
            for key in (
                "historical_replay",
                "cache_mutation",
                "reconnect_loop",
                "order_paths",
                "secret_logging",
            )
        )
    ):
        raise LiveSmokeApprovalError("live-smoke scope is broader than allowed")
    return dict(payload)


def verify_live_smoke_approval(
    *, plan_path: Path, approval_path: Path
) -> str:
    plan = validate_live_smoke_plan(_load_object(plan_path, "live-smoke plan"))
    approval = _load_object(approval_path, "live-smoke approval")
    approved_at = approval.get("approved_at")
    user_authorization_id = approval.get("user_authorization_id")
    core = {
        "schema_version": APPROVAL_SCHEMA,
        "status": "APPROVED",
        "operation": OPERATION,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "approved_at": approved_at,
        "user_authorization_id": user_authorization_id,
    }
    if (
        set(approval) != {*core, "approval_receipt_id"}
        or not isinstance(approved_at, str)
        or _UTC_SECOND.fullmatch(approved_at) is None
        or not isinstance(user_authorization_id, str)
        or _HASH.fullmatch(user_authorization_id) is None
        or approval.get("approval_receipt_id") != sha256_json(core)
    ):
        raise LiveSmokeApprovalError(
            "provider-backed cockpit smoke lacks exact hash-bound approval"
        )
    return str(approval["approval_receipt_id"])
