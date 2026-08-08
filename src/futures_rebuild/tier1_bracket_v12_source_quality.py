"""Create-only, coverage-only evidence controls for the V12 source census.

This module never reads historical rows and never registers or evaluates a
trial.  It validates an already completed census result, freezes the exact
predeclared ranking decision, and provides a publication function that fails
closed unless a separate publication authorization claim is supplied.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


PLAN_ID = "203dc5c3b50e23522c5fe877a867389f6c67b6bd2485125caeb2efcc6f4f1213"
SELECTION_RULE = (
    "MAXIMIZE_COMPLETE_BOTH_WINDOWS_THEN_COMPLETE_EXECUTION_WINDOWS_"
    "THEN_COMPLETE_FEATURE_WINDOWS_THEN_MINIMIZE_MISSING_SESSIONS_"
    "THEN_AMBIGUOUS_SESSIONS_THEN_LEXICOGRAPHIC_RELEASE_ID"
)
MARKETS = ("6E", "CL", "ES", "ZN")
YEARS = tuple(range(2018, 2023))
COUNT_FIELDS = (
    "expected_open_checkpoints",
    "missing_source_sessions",
    "ambiguous_source_sessions",
    "complete_feature_windows",
    "incomplete_feature_windows",
    "complete_execution_windows",
    "incomplete_execution_windows",
    "complete_both_windows",
)
RECORD_ROOT = Path("state/source_quality/tier1_bracket_v12_local_source_alternatives")
EVENT_ROOT = Path("state/source_quality_events/tier1_bracket_v12_local_source_alternatives")
PUBLISH_OPERATION = "PUBLISH_V12_LOCAL_SOURCE_QUALITY_RECORD_CREATE_ONLY"


def _hex64(value: object) -> bool:
    return type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _rank(item: Mapping[str, object]) -> tuple[object, ...]:
    counts = item["dependency_windows"]
    if not isinstance(counts, Mapping):
        raise IntegrityError("source census dependency counts are invalid")
    return (
        -int(counts["complete_both_windows"]),
        -int(counts["complete_execution_windows"]),
        -int(counts["complete_feature_windows"]),
        int(counts["missing_source_sessions"]),
        int(counts["ambiguous_source_sessions"]),
        str(item["release_id"]),
    )


def _validate_candidate(item: object) -> dict[str, object]:
    if not isinstance(item, Mapping):
        raise IntegrityError("source census candidate is not an object")
    counts = item.get("dependency_windows")
    audit = item.get("source_integrity")
    if (
        set(item) != {"release_id", "payload_sha256", "dependency_windows", "source_integrity"}
        or not _hex64(item.get("release_id"))
        or not _hex64(item.get("payload_sha256"))
        or not isinstance(counts, Mapping)
        or set(counts) != set(COUNT_FIELDS)
        or any(type(counts[name]) is not int or counts[name] < 0 for name in COUNT_FIELDS)
        or counts["complete_feature_windows"] + counts["incomplete_feature_windows"]
        != counts["expected_open_checkpoints"]
        or counts["complete_execution_windows"] + counts["incomplete_execution_windows"]
        != counts["expected_open_checkpoints"]
        or counts["complete_both_windows"] > counts["complete_feature_windows"]
        or counts["complete_both_windows"] > counts["complete_execution_windows"]
        or not isinstance(audit, Mapping)
    ):
        raise IntegrityError("source census candidate fields are invalid")
    return dict(item)


@dataclass(frozen=True)
class PreparedV12SourceQualityRecord:
    record_id: str
    canonical_payload: Mapping[str, object]


def prepare_v12_source_quality_record(
    *, census_output: Mapping[str, object], plan_sha256: str,
) -> PreparedV12SourceQualityRecord:
    """Validate and freeze one completed in-memory census output."""

    expected_keys = {f"{market}/{year}" for market in MARKETS for year in YEARS}
    selected = census_output.get("selected")
    all_candidates = census_output.get("all_candidates")
    if (
        census_output.get("status")
        != "COMPLETED_IN_MEMORY_UNPUBLISHED_COVERAGE_COUNTS_ONLY"
        or census_output.get("plan_id") != PLAN_ID
        or not _hex64(plan_sha256)
        or not _hex64(census_output.get("authorization_claim_sha256"))
        or census_output.get("selection_rule") != SELECTION_RULE
        or census_output.get("model_fit") is not False
        or census_output.get("prediction_generation") is not False
        or census_output.get("historical_evaluation") is not False
        or census_output.get("publication") is not False
        or census_output.get("holdout_or_forward_access") is not False
        or census_output.get("provider_access") is not False
        or not isinstance(selected, Mapping)
        or not isinstance(all_candidates, Mapping)
        or set(selected) != expected_keys
        or set(all_candidates) != expected_keys
    ):
        raise IntegrityError("V12 source census envelope is invalid")

    frozen_candidates: dict[str, list[dict[str, object]]] = {}
    frozen_selected: dict[str, dict[str, object]] = {}
    total = 0
    for key in sorted(expected_keys):
        raw_values = all_candidates[key]
        if not isinstance(raw_values, list) or not raw_values:
            raise IntegrityError("V12 source census market-year candidates are absent")
        values = [_validate_candidate(item) for item in raw_values]
        if len({str(item["release_id"]) for item in values}) != len(values):
            raise IntegrityError("V12 source census repeats a release")
        winner = _validate_candidate(selected[key])
        ranked = sorted(values, key=_rank)
        if winner != ranked[0]:
            raise IntegrityError("V12 source selection differs from the preregistered rule")
        frozen_candidates[key] = sorted(values, key=lambda item: str(item["release_id"]))
        frozen_selected[key] = winner
        total += len(values)
    if total != 61:
        raise IntegrityError("V12 source census does not contain all 61 releases")

    core = {
        "schema_version": "tier1_bracket_v12_local_source_quality_record/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "census_plan_id": PLAN_ID,
        "census_plan_sha256": plan_sha256,
        "census_authorization_claim_sha256": census_output["authorization_claim_sha256"],
        "selection_rule": SELECTION_RULE,
        "candidate_release_count": total,
        "market_year_pair_count": len(expected_keys),
        "selected": frozen_selected,
        "all_candidates": frozen_candidates,
        "research_actions": {
            "model_fit": False,
            "prediction_generation": False,
            "historical_evaluation": False,
            "holdout_or_forward_access": False,
            "provider_access": False,
            "trading": False,
        },
    }
    return PreparedV12SourceQualityRecord(sha256_json(core), core)


def prepare_v12_selected_source_quality_record(
    *, selected: Mapping[str, object], plan_sha256: str,
    census_authorization_claim_sha256: str, candidate_manifest_catalog_id: str,
) -> PreparedV12SourceQualityRecord:
    """Freeze the selected-release snapshot retained from the approved census.

    The census deliberately remained in memory.  This narrower preparation
    publishes exactly the selected 20 releases and their dependency counts,
    while binding the run's 61-release catalog and authorization claim.  It
    does not pretend that all losing-candidate counts were retained.
    """

    expected_keys = {f"{market}/{year}" for market in MARKETS for year in YEARS}
    if (
        set(selected) != expected_keys
        or not _hex64(plan_sha256)
        or not _hex64(census_authorization_claim_sha256)
        or not _hex64(candidate_manifest_catalog_id)
    ):
        raise IntegrityError("V12 selected source-quality envelope is invalid")
    frozen: dict[str, dict[str, object]] = {}
    for key in sorted(expected_keys):
        item = selected[key]
        if not isinstance(item, Mapping):
            raise IntegrityError("V12 selected source item is invalid")
        counts = item.get("dependency_windows")
        if (
            set(item) != {"release_id", "payload_sha256", "dependency_windows"}
            or not _hex64(item.get("release_id"))
            or not _hex64(item.get("payload_sha256"))
            or not isinstance(counts, Mapping)
            or set(counts) != set(COUNT_FIELDS)
            or any(type(counts[name]) is not int or counts[name] < 0 for name in COUNT_FIELDS)
            or counts["complete_feature_windows"] + counts["incomplete_feature_windows"]
            != counts["expected_open_checkpoints"]
            or counts["complete_execution_windows"] + counts["incomplete_execution_windows"]
            != counts["expected_open_checkpoints"]
            or counts["complete_both_windows"] > counts["complete_feature_windows"]
            or counts["complete_both_windows"] > counts["complete_execution_windows"]
        ):
            raise IntegrityError("V12 selected source counts are invalid")
        frozen[key] = {
            "release_id": item["release_id"],
            "payload_sha256": item["payload_sha256"],
            "dependency_windows": dict(counts),
        }
    if len({str(item["release_id"]) for item in frozen.values()}) != 20:
        raise IntegrityError("V12 selected source releases are not unique")
    core = {
        "schema_version": "tier1_bracket_v12_local_source_quality_record/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "census_plan_id": PLAN_ID,
        "census_plan_sha256": plan_sha256,
        "census_authorization_claim_sha256": census_authorization_claim_sha256,
        "candidate_manifest_catalog_id": candidate_manifest_catalog_id,
        "candidate_release_count_assessed": 61,
        "market_year_pair_count": 20,
        "selection_rule": SELECTION_RULE,
        "published_scope": "SELECTED_RELEASES_AND_DEPENDENCY_COUNTS_ONLY",
        "losing_candidate_counts_retained": False,
        "selected": frozen,
        "research_actions": {
            "model_fit": False,
            "prediction_generation": False,
            "historical_evaluation": False,
            "holdout_or_forward_access": False,
            "provider_access": False,
            "trading": False,
        },
    }
    return PreparedV12SourceQualityRecord(sha256_json(core), core)


def persist_v12_source_quality_record(
    *, root: Path, prepared: PreparedV12SourceQualityRecord,
    authorization: OperationReceipt, approval_plan_id: str,
    approval_plan_sha256: str,
) -> dict[str, str]:
    """Publish create-only after consuming one exact publication receipt."""

    if prepared.record_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V12 source-quality record identity is invalid")
    if not _hex64(approval_plan_id) or not _hex64(approval_plan_sha256):
        raise UnauthorizedOperation("source-quality publication plan identity is invalid")
    boundary = RepoBoundary(root)
    expected_scope = {
        "record_id": prepared.record_id,
        "census_plan_id": PLAN_ID,
        "publication": "true",
        "historical_row_read": "false",
        "model_fit": "false",
        "prediction_generation": "false",
        "historical_evaluation": "false",
        "holdout_or_forward_access": "false",
        "provider_access": "false",
        "active_data_mutation": "false",
        "staging": "false",
        "commit": "false",
        "push": "false",
        "trading": "false",
        "approval_command": PUBLISH_OPERATION,
        "approval_plan_id": approval_plan_id,
        "approval_plan_sha256": approval_plan_sha256,
    }
    claim_path = authorization.consume(
        boundary,
        operation=PUBLISH_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=expected_scope,
    )

    registry = root / RECORD_ROOT / f"{prepared.record_id}.json"
    event = root / EVENT_ROOT / f"{prepared.record_id}.json"
    boundary.assert_active_path(
        registry.absolute(), purpose="V12 source-quality registry record",
        subtree=RECORD_ROOT.as_posix(),
    )
    boundary.assert_active_path(
        event.absolute(), purpose="V12 source-quality publication event",
        subtree=EVENT_ROOT.as_posix(),
    )
    if registry.exists() or event.exists():
        raise IntegrityError("V12 source-quality publication is create-only")
    registry.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    with registry.open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload,
            "state": "PUBLISHED_COVERAGE_ONLY",
        }) + b"\n")
    with event.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_v12_local_source_quality_event/1.0.0",
            "event_type": "PUBLISHED",
            "record_id": prepared.record_id,
            "publication_receipt_id": authorization.receipt_id,
            "publication_claim_sha256": sha256_file(claim_path),
        }) + b"\n")
    return {
        "record_id": prepared.record_id,
        "registry_path": registry.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
    }
