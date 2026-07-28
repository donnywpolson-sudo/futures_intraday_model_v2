"""Fail-closed empirical historical-observability contracts.

This module classifies rows already admitted by an immutable foundation.  It
does not read historical payload files and does not infer exchange open, close,
halt, pause, or holiday states from an absence of rows.
"""

from __future__ import annotations

import argparse
import json
import re
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence

from ..boundary import OperationClassification, OperationReceipt, RepoBoundary
from ..canonical import canonical_bytes, sha256_file, sha256_json
from ..data_layout import (
    DataReleaseManifest,
    DataReleaseReceipt,
    PhasePublisher,
    verify_data_release_manifest,
)
from ..errors import ContractError, IntegrityError
from ..source_contract import legacy_roots_from_contract


POLICY_VERSION = "1.0.0"
FOUNDATION_OBSERVABILITY_SCHEMA_VERSION = "7.0.0"
APPROVAL_SCHEMA = "historical_observability_policy_successor_approval/1.0.0"
APPROVAL_OPERATION = "IMPLEMENT_DBN_EMPIRICAL_HISTORICAL_OBSERVABILITY_POLICY_SUCCESSOR"
EVIDENCE_BASIS = "IMMUTABLE_ACCEPTED_DATABENTO_DBN_OBSERVABILITY"
CALENDAR_CLAIM = "NOT_OFFICIAL_HISTORICAL_CME_SESSION_AUTHORITY"
OFFICIAL_CALENDAR_ROLE = "CURRENT_AND_FORWARD_COCKPIT_SCHEDULING_ONLY"
ROW_ADMISSION = (
    "ACTUAL_DECODED_SOURCE_ROWS_ONLY_NO_FILL_INTERPOLATION_"
    "SYNTHETIC_OPEN_OR_SYNTHETIC_CLOSE"
)
SESSION_ROLL_ROLE = "TRADE_DATE_GROUPING_ONLY_NOT_TRADING_HOURS_AUTHORITY"
UNCERTAINTY_RULE = "UNOBSERVED_TIME_IS_MISSING_NOT_CLOSED"
PRE_STATUS_CAPABILITY = "CAUSAL_PRICE_ONLY_EMPIRICAL_OBSERVABILITY"
STATUS_CAPABILITY = "CAUSAL_PRICE_PLUS_STATUS_GATED_EMPIRICAL_OBSERVABILITY"
READINESS_BLOCKER = "HISTORICAL_OBSERVABILITY_CONTRACT_NOT_BOUND"
PUBLICATION_PLAN_SCHEMA = "foundation_observability_successor_plan/1.0.0"
PUBLICATION_APPROVAL_SCHEMA = "foundation_observability_successor_approval/1.0.0"
PUBLICATION_OPERATION = "PUBLISH_DBN_EMPIRICAL_OBSERVABILITY_FOUNDATION_SUCCESSOR"
_HASH = re.compile(r"^[0-9a-f]{64}$")

_EXPECTED_DOES_NOT_AUTHORIZE = [
    "CANDIDATE_SELECTION",
    "HOLDOUT_OR_FORWARD_ACCESS",
    "MODEL_FIT",
    "PROVIDER_CALL",
    "REAL_HISTORY_EVALUATION",
    "WFA_OR_OOS",
]
_FORBIDDEN_INTERVAL_KEYS = {
    "close",
    "closed",
    "halt",
    "holiday",
    "is_open",
    "open",
    "pause",
    "session_close",
    "session_open",
    "trading_hours",
}


def _read_canonical_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is not readable JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def _require_hash(value: object, *, name: str) -> str:
    if type(value) is not str or _HASH.fullmatch(value) is None:
        raise IntegrityError(f"{name} must be a lowercase SHA-256")
    return value


def validate_historical_observability_policy(
    payload: Mapping[str, object],
) -> dict[str, object]:
    expected_keys = {
        "approval",
        "calendar_claim",
        "capability_labels",
        "does_not_authorize",
        "evidence_basis",
        "foundation_successor_schema_version",
        "interval_count",
        "market_count",
        "official_calendar_role",
        "policy_version",
        "predecessor_foundation_release_id",
        "research_scope_market_year_count",
        "research_scope_status_start",
        "row_admission",
        "session_roll_role",
        "source_dbn_release_id",
        "uncertainty_rule",
    }
    if set(payload) != expected_keys:
        raise IntegrityError("historical-observability policy schema is invalid")
    approval = payload.get("approval")
    labels = payload.get("capability_labels")
    if not isinstance(approval, Mapping) or not isinstance(labels, Mapping):
        raise IntegrityError("historical-observability policy mappings are invalid")
    expected_approval_keys = {
        "approval_receipt_id",
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    approval_core = {
        key: approval[key] for key in approval if key != "approval_receipt_id"
    }
    if (
        set(approval) != expected_approval_keys
        or approval.get("schema_version") != APPROVAL_SCHEMA
        or approval.get("operation") != APPROVAL_OPERATION
        or approval.get("status") != "APPROVED"
        or approval.get("approval_receipt_id") != sha256_json(approval_core)
    ):
        raise IntegrityError("historical-observability approval receipt is invalid")
    for name in (
        "approval_receipt_id",
        "plan_id",
        "plan_sha256",
        "user_authorization_id",
    ):
        _require_hash(approval.get(name), name=f"approval {name}")
    if (
        payload.get("policy_version") != POLICY_VERSION
        or payload.get("foundation_successor_schema_version")
        != FOUNDATION_OBSERVABILITY_SCHEMA_VERSION
        or payload.get("evidence_basis") != EVIDENCE_BASIS
        or payload.get("calendar_claim") != CALENDAR_CLAIM
        or payload.get("official_calendar_role") != OFFICIAL_CALENDAR_ROLE
        or payload.get("row_admission") != ROW_ADMISSION
        or payload.get("session_roll_role") != SESSION_ROLL_ROLE
        or payload.get("uncertainty_rule") != UNCERTAINTY_RULE
        or labels
        != {
            "pre_status_epoch": PRE_STATUS_CAPABILITY,
            "status_eligible": STATUS_CAPABILITY,
        }
        or payload.get("does_not_authorize") != _EXPECTED_DOES_NOT_AUTHORIZE
        or type(payload.get("interval_count")) is not int
        or int(payload["interval_count"]) <= 0
        or type(payload.get("market_count")) is not int
        or int(payload["market_count"]) <= 0
        or type(payload.get("research_scope_market_year_count")) is not int
        or int(payload["research_scope_market_year_count"]) <= 0
        or payload.get("research_scope_status_start") != "2025-01-01"
    ):
        raise IntegrityError("historical-observability policy semantics are invalid")
    _require_hash(
        payload.get("predecessor_foundation_release_id"),
        name="predecessor foundation release ID",
    )
    _require_hash(
        payload.get("source_dbn_release_id"), name="source DBN release ID"
    )
    return dict(payload)


def load_historical_observability_policy(path: Path) -> dict[str, object]:
    return validate_historical_observability_policy(
        _read_canonical_object(path, description="historical-observability policy")
    )


def _interval_evidence(
    interval: Mapping[str, object],
    *,
    source_dbn_release_id: str,
) -> dict[str, object]:
    gate = interval.get("status_epoch_gate")
    causal_receipt = interval.get("causal_release_receipt")
    if not isinstance(gate, Mapping) or not isinstance(causal_receipt, Mapping):
        raise IntegrityError("foundation interval lacks immutable observability evidence")
    interval_key = interval.get("interval_key")
    market = interval.get("market")
    year = interval.get("year")
    in_scope = gate.get("in_research_scope")
    bar_rows = gate.get("bar_rows")
    if (
        type(interval_key) is not str
        or type(market) is not str
        or type(year) is not int
        or type(in_scope) is not bool
        or type(bar_rows) is not int
        or bar_rows <= 0
        or gate.get("interval_key") != interval_key
        or interval.get("bar_source_path") is None
    ):
        raise IntegrityError("foundation interval observability is invalid")
    parts = interval_key.split("/")
    if len(parts) != 3 or parts[0] != market or parts[1] != str(year):
        raise IntegrityError("foundation interval identity is invalid")
    core: dict[str, object] = {
        "bar_query_contract_id": _require_hash(
            interval.get("bar_query_contract_id"), name="bar query contract ID"
        ),
        "bar_source_path": str(interval["bar_source_path"]),
        "bar_source_sha256": _require_hash(
            interval.get("bar_source_sha256"), name="bar source SHA-256"
        ),
        "calendar_claim": CALENDAR_CLAIM,
        "capability": STATUS_CAPABILITY if in_scope else PRE_STATUS_CAPABILITY,
        "causal_release_id": _require_hash(
            causal_receipt.get("release_id"), name="causal release ID"
        ),
        "coverage_disposition": interval.get("coverage_disposition"),
        "end": interval.get("end"),
        "evidence_basis": EVIDENCE_BASIS,
        "interval_key": interval_key,
        "market": market,
        "observed_bar_rows": bar_rows,
        "research_admissible": interval.get("coverage_disposition")
        in {
            "AUTHORITATIVE_INTERVAL",
            "AUTHORITATIVE_INTERVAL_WITH_EXACT_REDUNDANT_CROSSCHECK",
        },
        "session_roll_role": SESSION_ROLL_ROLE,
        "source_dbn_release_id": source_dbn_release_id,
        "start": interval.get("start"),
        "status_evidence_bound": in_scope,
        "uncertainty_rule": UNCERTAINTY_RULE,
        "year": year,
    }
    if core["coverage_disposition"] not in {
        "AUTHORITATIVE_INTERVAL",
        "AUTHORITATIVE_INTERVAL_WITH_EXACT_REDUNDANT_CROSSCHECK",
        "QUARANTINED_PENDING_REVALIDATION",
        "QUARANTINED_PENDING_REVALIDATION_WITH_EXACT_REDUNDANT_CROSSCHECK",
    }:
        raise IntegrityError("foundation interval disposition is invalid")
    if any(key.casefold() in _FORBIDDEN_INTERVAL_KEYS for key in core):
        raise IntegrityError("observability evidence asserts calendar authority")
    return {**core, "observability_evidence_id": sha256_json(core)}


def build_historical_observability_coverage(
    predecessor: Mapping[str, object],
    *,
    predecessor_release_id: str,
    policy: Mapping[str, object],
) -> dict[str, object]:
    validated_policy = validate_historical_observability_policy(policy)
    _require_hash(predecessor_release_id, name="predecessor foundation release ID")
    intervals = predecessor.get("intervals")
    if (
        predecessor.get("schema_version") != "5.0.0"
        or predecessor_release_id
        != validated_policy["predecessor_foundation_release_id"]
        or predecessor.get("source_dbn_release_id")
        != validated_policy["source_dbn_release_id"]
        or not isinstance(intervals, Sequence)
        or isinstance(intervals, (str, bytes))
        or len(intervals) != validated_policy["interval_count"]
    ):
        raise IntegrityError("predecessor foundation does not match observability policy")
    evidence = [
        _interval_evidence(
            item,
            source_dbn_release_id=str(validated_policy["source_dbn_release_id"]),
        )
        for item in intervals
        if isinstance(item, Mapping)
    ]
    if len(evidence) != len(intervals):
        raise IntegrityError("predecessor interval collection is invalid")
    evidence.sort(key=lambda item: str(item["interval_key"]))
    markets = {str(item["market"]) for item in evidence}
    status_items = [
        item for item in evidence if item["capability"] == STATUS_CAPABILITY
    ]
    status_market_years = {
        (str(item["market"]), int(item["year"])) for item in status_items
    }
    if (
        len(markets) != validated_policy["market_count"]
        or len(status_market_years)
        != validated_policy["research_scope_market_year_count"]
    ):
        raise IntegrityError("observability coverage counts do not match policy")
    core: dict[str, object] = {
        "calendar_claim": CALENDAR_CLAIM,
        "evidence_basis": EVIDENCE_BASIS,
        "foundation_schema_version": FOUNDATION_OBSERVABILITY_SCHEMA_VERSION,
        "interval_count": len(evidence),
        "intervals": evidence,
        "market_count": len(markets),
        "quarantined_interval_count": sum(
            item["research_admissible"] is False for item in evidence
        ),
        "pre_status_interval_count": len(evidence) - len(status_items),
        "predecessor_foundation_release_id": predecessor_release_id,
        "research_scope_interval_count": len(status_items),
        "research_scope_market_year_count": len(status_market_years),
        "row_admission": ROW_ADMISSION,
        "source_dbn_release_id": validated_policy["source_dbn_release_id"],
        "status_capability_interval_count": len(status_items),
        "uncertainty_rule": UNCERTAINTY_RULE,
    }
    return {**core, "historical_observability_coverage_id": sha256_json(core)}


def validate_historical_observability_coverage(
    coverage: Mapping[str, object],
    *,
    predecessor: Mapping[str, object],
    predecessor_release_id: str,
    policy: Mapping[str, object],
) -> dict[str, object]:
    expected = build_historical_observability_coverage(
        predecessor,
        predecessor_release_id=predecessor_release_id,
        policy=policy,
    )
    if dict(coverage) != expected:
        raise IntegrityError("historical-observability coverage is invalid")
    return expected


def build_foundation_observability_successor_payload(
    predecessor: Mapping[str, object],
    *,
    predecessor_release_id: str,
    policy: Mapping[str, object],
    policy_sha256: str,
) -> dict[str, object]:
    """Build, but do not publish, the metadata-only schema-7 successor."""

    _require_hash(policy_sha256, name="historical-observability policy SHA-256")
    coverage = build_historical_observability_coverage(
        predecessor,
        predecessor_release_id=predecessor_release_id,
        policy=policy,
    )
    successor = deepcopy(dict(predecessor))
    successor.pop("foundation_set_id", None)
    successor.update(
        {
            "historical_observability_coverage": coverage,
            "historical_observability_policy_sha256": policy_sha256,
            "predecessor_foundation_release_id": predecessor_release_id,
            "schema_version": FOUNDATION_OBSERVABILITY_SCHEMA_VERSION,
        }
    )
    return {**successor, "foundation_set_id": sha256_json(successor)}


def _build_successor_manifest(
    predecessor_manifest: DataReleaseManifest,
    successor: Mapping[str, object],
) -> DataReleaseManifest:
    coverage = successor.get("historical_observability_coverage")
    provenance = successor.get("successor_provenance")
    if not isinstance(coverage, Mapping) or not isinstance(provenance, Mapping):
        raise IntegrityError("schema-7 successor metadata is incomplete")
    core = {
        "embedded_documents": {"foundation_set.json": dict(successor)},
        "files": [],
        "layout_version": predecessor_manifest.layout_version,
        "manifest_version": predecessor_manifest.manifest_version,
        "metadata": {
            "coverage_matrix_id": successor["coverage_matrix_id"],
            "feature_spec_hash": successor["feature_spec_hash"],
            "foundation_set_id": successor["foundation_set_id"],
            "historical_observability_coverage_id": coverage[
                "historical_observability_coverage_id"
            ],
            "historical_observability_policy_sha256": successor[
                "historical_observability_policy_sha256"
            ],
            "interval_count": successor["interval_count"],
            "predecessor_foundation_release_id": successor[
                "predecessor_foundation_release_id"
            ],
            "query_manifest_id": successor["query_manifest_id"],
            "run_id": successor["run_id"],
            "source_dbn_release_id": successor["source_dbn_release_id"],
            "successor_provenance_id": provenance["successor_provenance_id"],
        },
        "phase": "foundation",
        "release_kind": predecessor_manifest.release_kind,
        "schema_version": FOUNDATION_OBSERVABILITY_SCHEMA_VERSION,
        "source_release_ids": sorted(
            {
                *predecessor_manifest.source_release_ids,
                predecessor_manifest.release_id,
            }
        ),
    }
    return DataReleaseManifest(
        release_id=sha256_json(core),
        phase="foundation",
        release_kind=predecessor_manifest.release_kind,
        schema_version=FOUNDATION_OBSERVABILITY_SCHEMA_VERSION,
        source_release_ids=tuple(core["source_release_ids"]),
        files=(),
        embedded_documents=core["embedded_documents"],
        metadata=core["metadata"],
        layout_version=predecessor_manifest.layout_version,
        manifest_version=predecessor_manifest.manifest_version,
    )


def _load_predecessor_foundation_shallow(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
    policy: Mapping[str, object],
) -> tuple[DataReleaseManifest, dict[str, object]]:
    manifest = receipt.verify(boundary)
    payload = manifest.embedded_documents.get("foundation_set.json")
    if (
        manifest.phase != "foundation"
        or manifest.schema_version != "5.0.0"
        or manifest.files
        or not isinstance(payload, dict)
        or manifest.release_id != policy["predecessor_foundation_release_id"]
        or manifest.metadata.get("interval_count") != policy["interval_count"]
        or manifest.metadata.get("source_dbn_release_id")
        != policy["source_dbn_release_id"]
    ):
        raise IntegrityError("accepted predecessor foundation binding is invalid")
    foundation_set_id = payload.get("foundation_set_id")
    payload_core = {
        key: value for key, value in payload.items() if key != "foundation_set_id"
    }
    if (
        foundation_set_id != sha256_json(payload_core)
        or foundation_set_id != manifest.metadata.get("foundation_set_id")
        or payload.get("schema_version") != "5.0.0"
        or payload.get("interval_count") != policy["interval_count"]
        or payload.get("source_dbn_release_id") != policy["source_dbn_release_id"]
        or payload.get("provider_call_count") != 0
        or payload.get("model_fit_count") != 0
        or payload.get("wfa_execution_count") != 0
        or payload.get("historical_outcome_or_label_execution") is not False
        or payload.get("alpha_evidence") is not False
        or payload.get("candidate_eligible") is not False
    ):
        raise IntegrityError("accepted predecessor foundation content is invalid")
    build_historical_observability_coverage(
        payload,
        predecessor_release_id=manifest.release_id,
        policy=policy,
    )
    return manifest, dict(payload)


def load_foundation_observability_successor(
    receipt: DataReleaseReceipt,
    *,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        manifest.phase != "foundation"
        or manifest.schema_version != FOUNDATION_OBSERVABILITY_SCHEMA_VERSION
        or manifest.files
    ):
        raise IntegrityError("schema-7 observability foundation release is invalid")
    policy_path = (
        boundary.active_root / "configs" / "historical_observability_policy.json"
    )
    policy = load_historical_observability_policy(policy_path)
    predecessor_id = str(policy["predecessor_foundation_release_id"])
    predecessor_path = (
        boundary.active_root
        / "manifests"
        / "data_releases"
        / "foundation"
        / f"{predecessor_id}.json"
    )
    predecessor_receipt = DataReleaseReceipt.from_manifest(
        predecessor_path, boundary, verify_files=False
    )
    predecessor_manifest, predecessor = _load_predecessor_foundation_shallow(
        predecessor_receipt,
        boundary=boundary,
        policy=policy,
    )
    expected_payload = build_foundation_observability_successor_payload(
        predecessor,
        predecessor_release_id=predecessor_id,
        policy=policy,
        policy_sha256=sha256_file(policy_path),
    )
    expected_manifest = _build_successor_manifest(
        predecessor_manifest, expected_payload
    )
    if manifest.as_dict() != expected_manifest.as_dict():
        raise IntegrityError("schema-7 observability foundation differs from authority")
    return expected_payload


def _publication_authority(
    *,
    boundary: RepoBoundary,
    predecessor_manifest_path: Path,
    policy_path: Path,
) -> tuple[dict[str, object], DataReleaseManifest, dict[str, object]]:
    predecessor_receipt = DataReleaseReceipt.from_manifest(
        predecessor_manifest_path,
        boundary,
        verify_files=False,
    )
    policy = load_historical_observability_policy(policy_path)
    predecessor_manifest, predecessor = _load_predecessor_foundation_shallow(
        predecessor_receipt,
        boundary=boundary,
        policy=policy,
    )
    successor = build_foundation_observability_successor_payload(
        predecessor,
        predecessor_release_id=predecessor_manifest.release_id,
        policy=policy,
        policy_sha256=sha256_file(policy_path),
    )
    manifest = _build_successor_manifest(predecessor_manifest, successor)
    authority = {
        "expected_foundation_release_id": manifest.release_id,
        "expected_foundation_set_id": successor["foundation_set_id"],
        "historical_observability_coverage_id": successor[
            "historical_observability_coverage"
        ]["historical_observability_coverage_id"],
        "historical_observability_policy_path": policy_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "historical_observability_policy_sha256": sha256_file(policy_path),
        "interval_count": successor["interval_count"],
        "market_count": policy["market_count"],
        "predecessor_foundation_manifest_path": predecessor_manifest_path.relative_to(
            boundary.active_root
        ).as_posix(),
        "predecessor_foundation_manifest_sha256": sha256_file(
            predecessor_manifest_path
        ),
        "predecessor_foundation_release_id": predecessor_manifest.release_id,
        "source_dbn_release_id": successor["source_dbn_release_id"],
    }
    return authority, manifest, successor


def build_publication_plan(
    *,
    boundary: RepoBoundary,
    predecessor_manifest_path: Path,
    policy_path: Path,
) -> dict[str, object]:
    authority, _, _ = _publication_authority(
        boundary=boundary,
        predecessor_manifest_path=predecessor_manifest_path,
        policy_path=policy_path,
    )
    implementation_paths = (
        "src/futures_rebuild/boundary.py",
        "src/futures_rebuild/canonical.py",
        "src/futures_rebuild/data_layout.py",
        "src/futures_rebuild/foundation/historical_observability.py",
        "src/futures_rebuild/foundation/orchestrator.py",
    )
    scope = {
        "authority": authority,
        "bounds": {
            "foundation_manifest_publications": 1,
            "historical_payload_reads": 0,
            "maximum_duration_seconds": 900,
            "network_requests": 0,
            "provider_calls": 0,
        },
        "forbidden_actions": [
            "ACCESS_OUTCOMES_MODELS_PREDICTIONS_HOLDOUT_OR_FORWARD_PAYLOADS",
            "CALL_ANY_NETWORK_OR_PROVIDER",
            "MODIFY_PREDECESSOR_OR_SOURCE_RELEASE",
            "PUBLISH_ANY_NON_FOUNDATION_RELEASE",
            "PUSH_OR_STAGE_PATHS",
            "READ_HISTORICAL_DATA_PAYLOADS",
        ],
        "implementation_sha256": {
            path: sha256_file(boundary.active_root / path)
            for path in implementation_paths
        },
        "output": {
            "approval_path": (
                "configs/foundation_observability_publication_approval.json"
            ),
            "manifest_path": (
                "manifests/data_releases/foundation/"
                f"{authority['expected_foundation_release_id']}.json"
            ),
            "publication_lock": "state/locks/data-publication.lock",
        },
        "stop_conditions": [
            "APPROVAL_OR_PLAN_IDENTITY_MISMATCH",
            "IMPLEMENTATION_POLICY_OR_PREDECESSOR_HASH_DRIFT",
            "EXISTING_CONFLICTING_RELEASE",
            "UNDECLARED_OUTPUT_OR_PAYLOAD_ACCESS",
            "READBACK_OR_LINEAGE_FAILURE",
        ],
    }
    core = {
        "classification": "PENDING_EXACT_HASH_BOUND_PUBLICATION_APPROVAL",
        "execution_authorized": False,
        "operation": PUBLICATION_OPERATION,
        "schema_version": PUBLICATION_PLAN_SCHEMA,
        "scope": scope,
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_publication_plan(
    payload: Mapping[str, object],
    *,
    boundary: RepoBoundary,
    predecessor_manifest_path: Path,
    policy_path: Path,
) -> dict[str, object]:
    expected = build_publication_plan(
        boundary=boundary,
        predecessor_manifest_path=predecessor_manifest_path,
        policy_path=policy_path,
    )
    if dict(payload) != expected:
        raise IntegrityError("foundation observability publication plan drifted")
    return expected


def validate_publication_approval(
    payload: Mapping[str, object],
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> str:
    expected_keys = {
        "approval_receipt_id",
        "approved_at",
        "expected_foundation_release_id",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    core = {key: payload[key] for key in payload if key != "approval_receipt_id"}
    scope = plan.get("scope")
    authority = scope.get("authority") if isinstance(scope, Mapping) else None
    try:
        datetime.fromisoformat(str(payload.get("approved_at")).replace("Z", "+00:00"))
    except ValueError as exc:
        raise IntegrityError("foundation observability approval time is invalid") from exc
    if (
        set(payload) != expected_keys
        or not isinstance(authority, Mapping)
        or payload.get("schema_version") != PUBLICATION_APPROVAL_SCHEMA
        or payload.get("operation") != PUBLICATION_OPERATION
        or payload.get("status") != "APPROVED"
        or payload.get("plan_id") != plan.get("plan_id")
        or payload.get("plan_sha256") != plan_sha256
        or payload.get("expected_foundation_release_id")
        != authority.get("expected_foundation_release_id")
        or payload.get("approval_receipt_id") != sha256_json(core)
    ):
        raise IntegrityError("foundation observability publication approval is invalid")
    for name in (
        "approval_receipt_id",
        "expected_foundation_release_id",
        "plan_id",
        "plan_sha256",
        "user_authorization_id",
    ):
        _require_hash(payload.get(name), name=f"publication approval {name}")
    return str(payload["approval_receipt_id"])


def execute_publication(
    *,
    boundary: RepoBoundary,
    predecessor_manifest_path: Path,
    policy_path: Path,
    plan_path: Path,
    approval_path: Path,
) -> DataReleaseReceipt:
    plan = validate_publication_plan(
        _read_canonical_object(plan_path, description="observability publication plan"),
        boundary=boundary,
        predecessor_manifest_path=predecessor_manifest_path,
        policy_path=policy_path,
    )
    validate_publication_approval(
        _read_canonical_object(
            approval_path, description="observability publication approval"
        ),
        plan=plan,
        plan_sha256=sha256_file(plan_path),
    )
    _, manifest, successor = _publication_authority(
        boundary=boundary,
        predecessor_manifest_path=predecessor_manifest_path,
        policy_path=policy_path,
    )
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={
            "expected_release_id": manifest.release_id,
            "purpose": "PUBLISH_DBN_EMPIRICAL_OBSERVABILITY_FOUNDATION_SUCCESSOR",
        },
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=boundary.active_root / "state" / "locks" / "data-publication.lock",
    )
    stage = publisher.create_stage("foundation_observability_successor")
    manifest_path = publisher.publish(stage, manifest)
    readback = verify_data_release_manifest(
        manifest_path, boundary, verify_files=False
    )
    if readback.embedded_documents.get("foundation_set.json") != successor:
        raise IntegrityError("schema-7 successor readback failed")
    receipt = DataReleaseReceipt.from_manifest(
        manifest_path, boundary, verify_files=False
    )
    load_foundation_observability_successor(receipt, boundary=boundary)
    return receipt


def _boundary_from_source_contract(
    repository_root: Path, source_contract_path: Path
) -> RepoBoundary:
    try:
        payload = json.loads(source_contract_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("source contract is not readable JSON") from exc
    if not isinstance(payload, dict):
        raise IntegrityError("source contract is not a JSON object")
    boundary = RepoBoundary(
        Path(str(payload["active_repository"])),
        legacy_roots=legacy_roots_from_contract(payload),
    )
    boundary.assert_active_root(repository_root)
    return boundary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("print-plan", "publish"))
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    parser.add_argument("--predecessor-manifest", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--approval", type=Path)
    args = parser.parse_args(argv)
    boundary = _boundary_from_source_contract(
        args.repository_root.resolve(strict=True),
        args.source_contract.resolve(strict=True),
    )
    predecessor = args.predecessor_manifest.resolve(strict=True)
    policy = args.policy.resolve(strict=True)
    if args.command == "print-plan":
        print(
            json.dumps(
                build_publication_plan(
                    boundary=boundary,
                    predecessor_manifest_path=predecessor,
                    policy_path=policy,
                ),
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    if args.plan is None or args.approval is None:
        parser.error("publish requires --plan and --approval")
    receipt = execute_publication(
        boundary=boundary,
        predecessor_manifest_path=predecessor,
        policy_path=policy,
        plan_path=args.plan.resolve(strict=True),
        approval_path=args.approval.resolve(strict=True),
    )
    print(receipt.release_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
