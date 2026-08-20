"""Exact content-hash approval gate for the bounded provider-backed smoke."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Mapping

from futures_rebuild.canonical import sha256_file, sha256_json

from .feed import chart_market_universe


PLAN_SCHEMA = "futures_live_cockpit_smoke_plan/1.2.0"
APPROVAL_SCHEMA = "futures_live_cockpit_smoke_approval/1.3.0"
OPERATION = "RUN_BOUNDED_OBSERVATION_ONLY_DATABENTO_SMOKE_ATTEMPT_8"
RESULT_OUTPUT_RELATIVE = (
    "reports/live_cockpit/bounded_live_smoke_result_attempt_8.json"
)
PREDECESSOR_ATTEMPT = {
    "plan_id": "4d7ba2bfcd64630cd04ad13be3ccfaec43e121ecf5729fe4936a7d4d1b11e0e6",
    "plan_sha256": "24918e2f90d87d8c221d688fbc0a96602aee05f8ca8030b689a7ced8e8d68e36",
    "approval_receipt_id": (
        "fad29c19acc4ecd8dfc604a10d471da1c8989c268d25d36da1c783621e81dada"
    ),
    "result_id": "c27b807d86b43bfae8136897e9497ce1245f9c0aaeee068bda12cf45f88bba08",
    "result_sha256": (
        "6750fea0db3fdb8f5e710659e0bdc193cc7de450dab3cdf97f93e11dfb35c350"
    ),
    "disposition": "PASS_SUPERSEDED_BY_CREATE_ONLY_BOUNDED_SMOKE_ENTRYPOINT",
}
_HASH = re.compile(r"[0-9a-f]{64}")
_UTC_SECOND = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")


class LiveSmokeApprovalError(RuntimeError):
    """Raised before any provider client is created."""


def build_live_smoke_plan(prepared_executable_sha256: str) -> dict[str, Any]:
    if _HASH.fullmatch(prepared_executable_sha256) is None:
        raise LiveSmokeApprovalError("prepared executable hash is invalid")
    body: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA,
        "classification": "PENDING_EXACT_HASH_BOUND_APPROVAL",
        "operation": OPERATION,
        "scope": {
            "dataset": "GLBX.MDP3",
            "overview_markets": sorted(
                info.symbol for info in chart_market_universe()
            ),
            "focus_market": "ES",
            "required_focus_market_calendar_state": "OPEN",
            "minimum_open_window_seconds": 180,
            "duration_seconds": 120,
            "maximum_live_sessions": 2,
            "historical_replay": False,
            "cache_mutation": False,
            "reconnect_loop": False,
            "order_paths": False,
            "secret_logging": False,
            "result_output_relative": RESULT_OUTPUT_RELATIVE,
            "prepared_executable_sha256": prepared_executable_sha256,
            "runtime_frozen": True,
        },
        "execution_authorized": False,
        "predecessor_attempt": dict(PREDECESSOR_ATTEMPT),
    }
    body["plan_id"] = sha256_json(body)
    return body


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
    scope = payload["scope"]
    if type(scope) is not dict or set(scope) != {
        "dataset",
        "overview_markets",
        "focus_market",
        "required_focus_market_calendar_state",
        "minimum_open_window_seconds",
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
    executable_hash = scope.get("prepared_executable_sha256")
    if type(executable_hash) is not str:
        raise LiveSmokeApprovalError("live-smoke plan identity is invalid")
    expected = build_live_smoke_plan(executable_hash)
    if dict(payload) != expected:
        raise LiveSmokeApprovalError("live-smoke plan identity is invalid")
    return dict(payload)


def verify_live_smoke_approval(
    *, plan_path: Path, approval_path: Path, credential_locator: Path
) -> str:
    plan = validate_live_smoke_plan(_load_object(plan_path, "live-smoke plan"))
    approval = _load_object(approval_path, "live-smoke approval")
    try:
        resolved_locator = credential_locator.resolve(strict=True)
    except OSError as exc:
        raise LiveSmokeApprovalError(
            "provider-backed cockpit smoke credential locator is unavailable"
        ) from exc
    if not resolved_locator.is_file():
        raise LiveSmokeApprovalError(
            "provider-backed cockpit smoke credential locator is unavailable"
        )
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
        "credential_locator_path": str(resolved_locator),
        "credential_locator_sha256": sha256_file(resolved_locator),
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
