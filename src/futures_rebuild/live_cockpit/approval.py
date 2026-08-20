"""Exact content-hash approval gate for the bounded provider-backed smoke."""

from __future__ import annotations

from collections.abc import Sequence
import json
from pathlib import Path
import re
from typing import Any, Mapping

from futures_rebuild.canonical import sha256_file, sha256_json

from .feed import chart_market_universe


PLAN_SCHEMA = "futures_live_cockpit_smoke_plan/1.3.0"
APPROVAL_SCHEMA = "futures_live_cockpit_smoke_approval/1.4.0"
OPERATION = "RUN_BOUNDED_OBSERVATION_ONLY_DATABENTO_SMOKE_ATTEMPT_9"
RESULT_OUTPUT_RELATIVE = (
    "reports/live_cockpit/bounded_live_smoke_result_attempt_9.json"
)
PREDECESSOR_ATTEMPT = {
    "plan_id": "566f80f601026a1a3183b7393f8963572026b8c288ac6d39813d6899fec68a81",
    "artifact_sha256": (
        "3a509681595006f6317e185432d9a49fa240e6497e8485675339391790ee5763"
    ),
    "execution_authorized": False,
    "disposition": "PENDING_UNEXECUTED_SUPERSEDED_SOURCE_HASH_DRIFT",
}
_HASH = re.compile(r"[0-9a-f]{64}")
_SOURCE_REVISION = re.compile(r"[0-9a-f]{40,64}")
_UTC_SECOND = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
SUCCESSOR_REASON = "SOURCE_HASH_DRIFT"
UPDATED_VALIDATION_REQUIREMENTS = {
    "offline_cockpit_suite": "PASS_REQUIRED",
    "deterministic_visual_states": [
        "FIRST_LAUNCH_CHOICE",
        "HISTORY_UPDATING",
        "HISTORY_READY",
        "REVIEW_NEEDED",
        "AUTOMATIC_FAILURE",
    ],
    "read_only_live_feed_smoke": "PASS_AFTER_SEPARATE_APPROVAL",
    "isolated_automatic_history_canary": "PASS_AFTER_SEPARATE_APPROVAL",
    "release_executable_sha256": "IDENTICAL_THROUGH_ACTIVATION",
}


class LiveSmokeApprovalError(RuntimeError):
    """Raised before any provider client is created."""


def _package_input_binding(
    package_inputs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in package_inputs:
        if set(item) != {"path", "bytes", "sha256"}:
            raise LiveSmokeApprovalError("package input fields are invalid")
        path = item.get("path")
        size = item.get("bytes")
        digest = item.get("sha256")
        if (
            not isinstance(path, str)
            or not path
            or path in seen
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or not isinstance(digest, str)
            or _HASH.fullmatch(digest) is None
        ):
            raise LiveSmokeApprovalError("package input identity is invalid")
        seen.add(path)
        normalized.append({"path": path, "bytes": size, "sha256": digest})
    if not normalized:
        raise LiveSmokeApprovalError("package input identity is invalid")
    return normalized


def build_live_smoke_plan(
    prepared_executable_sha256: str,
    *,
    source_revision: str,
    package_inputs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if _HASH.fullmatch(prepared_executable_sha256) is None:
        raise LiveSmokeApprovalError("prepared executable hash is invalid")
    if _SOURCE_REVISION.fullmatch(source_revision) is None:
        raise LiveSmokeApprovalError("source revision is invalid")
    bound_inputs = _package_input_binding(package_inputs)
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
        "successor_binding": {
            "supersedes_plan_id": PREDECESSOR_ATTEMPT["plan_id"],
            "predecessor_artifact_sha256": PREDECESSOR_ATTEMPT[
                "artifact_sha256"
            ],
            "reason": SUCCESSOR_REASON,
            "source_revision": source_revision,
            "package_inputs": bound_inputs,
            "candidate_executable_sha256": prepared_executable_sha256,
            "updated_validation_requirements": {
                **UPDATED_VALIDATION_REQUIREMENTS,
                "deterministic_visual_states": list(
                    UPDATED_VALIDATION_REQUIREMENTS[
                        "deterministic_visual_states"
                    ]
                ),
            },
        },
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
        "successor_binding",
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
    binding = payload.get("successor_binding")
    if type(executable_hash) is not str or type(binding) is not dict:
        raise LiveSmokeApprovalError("live-smoke plan identity is invalid")
    source_revision = binding.get("source_revision")
    package_inputs = binding.get("package_inputs")
    if type(source_revision) is not str or type(package_inputs) is not list:
        raise LiveSmokeApprovalError("live-smoke plan identity is invalid")
    expected = build_live_smoke_plan(
        executable_hash,
        source_revision=source_revision,
        package_inputs=package_inputs,
    )
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
