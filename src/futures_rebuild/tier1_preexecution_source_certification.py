"""Receipt-gated source sufficiency certification for the frozen successor.

The operation selects among immutable local causal releases using only
dependency coverage.  It never fits a model, creates a prediction, evaluates
performance, opens 2025, or mutates active data.  One approved run consumes a
single-use receipt and publishes one create-only source-quality record.
"""

from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path
from typing import Mapping, Sequence

from scripts.run_v12_local_source_alternative_census import (
    _candidate_path,
    _catalog,
    _census,
)

from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v10 as v10
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment


PLAN_PATH = Path("configs/tier1_preexecution_source_certification_plan.json")
OPERATION = "CERTIFY_FROZEN_TIER1_SOURCE_SUFFICIENCY_AND_PUBLISH"
RECORD_ROOT = Path("state/source_quality/tier1_preexecution_source_certification")
EVENT_ROOT = Path("state/source_quality_events/tier1_preexecution_source_certification")
SELECTION_RULE = (
    "MAXIMIZE_COMPLETE_BOTH_WINDOWS_THEN_COMPLETE_EXECUTION_WINDOWS_"
    "THEN_COMPLETE_FEATURE_WINDOWS_THEN_MINIMIZE_MISSING_SESSIONS_"
    "THEN_AMBIGUOUS_SESSIONS_THEN_LEXICOGRAPHIC_RELEASE_ID"
)


def _load_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid source-certification artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("source-certification artifact is not an object")
    return value


def load_source_certification_plan(*, root: Path) -> dict[str, object]:
    plan_path = root / PLAN_PATH
    plan = _load_object(plan_path)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    forbidden = plan.get("forbidden_actions")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version")
        != "tier1_preexecution_source_certification_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_EXACT_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("candidate_release_count") != 61
        or plan.get("candidate_manifest_catalog_id") != sha256_json(_catalog())
        or plan.get("calendar_release_id")
        != "038940d82031f31e2c66ed37186e98a6ee6cff3e7248f634f2c7a8e94ea6ecf3"
        or plan.get("selection_rule") != SELECTION_RULE
        or plan.get("maximum_host_runtime_seconds") != 900
        or plan.get("estimated_external_cost_usd") != "0"
        or plan.get("implementation_sha256") != sha256_file(Path(__file__))
        or not isinstance(forbidden, dict)
        or not forbidden
        or not all(value is True for value in forbidden.values())
    ):
        raise UnauthorizedOperation("source-certification plan is absent or drifted")
    return plan


def _rank(item: Mapping[str, object]) -> tuple[object, ...]:
    counts = item.get("dependency_windows")
    if not isinstance(counts, Mapping):
        raise IntegrityError("candidate dependency census is absent")
    return (
        -int(counts["complete_both_windows"]),
        -int(counts["complete_execution_windows"]),
        -int(counts["complete_feature_windows"]),
        int(counts["missing_source_sessions"]),
        int(counts["ambiguous_source_sessions"]),
        str(item["release_id"]),
    )


def select_and_certify_sources(
    candidates_by_market_year: Mapping[str, Sequence[Mapping[str, object]]],
) -> dict[str, object]:
    expected_keys = {
        f"{market}/{year}" for market in ("6E", "CL", "ES", "ZN")
        for year in range(2018, 2023)
    }
    if set(candidates_by_market_year) != expected_keys:
        raise IntegrityError("source-certification market-year scope is incomplete")
    selected: dict[str, Mapping[str, object]] = {}
    gates: dict[str, dict[str, object]] = {}
    total = 0
    for key in sorted(expected_keys):
        values = list(candidates_by_market_year[key])
        if not values:
            raise IntegrityError("source-certification candidate cell is empty")
        if len({str(item.get("release_id")) for item in values}) != len(values):
            raise IntegrityError("source-certification candidate release is repeated")
        winner = sorted(values, key=_rank)[0]
        counts = winner.get("dependency_windows")
        if not isinstance(counts, Mapping):
            raise IntegrityError("selected dependency census is absent")
        expected = int(counts["expected_open_checkpoints"])
        checks = {
            "no_missing_source_sessions": int(counts["missing_source_sessions"]) == 0,
            "no_ambiguous_source_sessions": int(counts["ambiguous_source_sessions"]) == 0,
            "all_feature_windows_complete": int(counts["complete_feature_windows"]) == expected,
            "all_execution_windows_complete": int(counts["complete_execution_windows"]) == expected,
            "all_joint_windows_complete": int(counts["complete_both_windows"]) == expected,
        }
        selected[key] = winner
        gates[key] = {
            "status": "PASS" if all(checks.values()) else "FAIL",
            "checks": checks,
        }
        total += len(values)
    if total != 61:
        raise IntegrityError("source certification did not assess all 61 releases")
    decision = "PASS" if all(item["status"] == "PASS" for item in gates.values()) else "FAIL"
    return {
        "decision": decision,
        "selection_rule": SELECTION_RULE,
        "candidate_release_count_assessed": total,
        "selected": dict(selected),
        "market_year_gates": gates,
    }


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "candidate_manifest_catalog_id": str(plan["candidate_manifest_catalog_id"]),
        "candidate_release_count": "61",
        "source_scope": "6E,CL,ES,ZN|2018,2019,2020,2021,2022",
        "publication_root": RECORD_ROOT.as_posix(),
        "historical_row_read": "true",
        "publication": "true",
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
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def execute_authorized_source_certification(
    *, root: Path, authorization: OperationReceipt,
) -> dict[str, object]:
    """Consume approval, read only scoped local rows, and publish one record."""

    boundary = RepoBoundary(root)
    plan = load_source_certification_plan(root=root)
    require_locked_repository_environment(root)
    claim = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=_required_scope(root=root, plan=plan),
    )
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(plan["calendar_release_id"]),
    )
    census = v5.build_expected_census_from_calendar(sessions=sessions)
    expected: dict[tuple[str, int], dict[str, list[v5.CensusCheckpoint]]] = {}
    for checkpoint in census:
        item = checkpoint.expected
        expected.setdefault((item.market, item.year), {}).setdefault(
            item.exchange_session_date, []
        ).append(checkpoint)
    candidates: dict[str, list[dict[str, object]]] = {}
    for item in _catalog():
        market, year = str(item["market"]), int(item["year"])
        path = _candidate_path(boundary=boundary, item=item)
        counts, audit = _census(
            market=market,
            path=path,
            expected_by_session={
                session: tuple(values)
                for session, values in expected[(market, year)].items()
            },
        )
        candidates.setdefault(f"{market}/{year}", []).append({
            "release_id": item["release_id"],
            "payload_sha256": item["payload_sha256"],
            "dependency_windows": counts,
            "source_integrity": audit,
        })
    certification = select_and_certify_sources(candidates)
    core = {
        "schema_version": "tier1_preexecution_source_certification/1.0.0",
        "state": "PREPARED_CREATE_ONLY",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "candidate_manifest_catalog_id": plan["candidate_manifest_catalog_id"],
        "calendar_release_id": plan["calendar_release_id"],
        "certification": certification,
        "all_candidates": {
            key: sorted(values, key=lambda value: str(value["release_id"]))
            for key, values in sorted(candidates.items())
        },
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "active_data_mutation": False,
        "trading": False,
    }
    record_id = sha256_json(core)
    record = root / RECORD_ROOT / f"{record_id}.json"
    event = root / EVENT_ROOT / f"{record_id}.json"
    boundary.assert_active_path(record.absolute(), purpose="source certification record", subtree=RECORD_ROOT.as_posix())
    boundary.assert_active_path(event.absolute(), purpose="source certification event", subtree=EVENT_ROOT.as_posix())
    if record.exists() or event.exists():
        raise IntegrityError("source-certification publication is create-only")
    record.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    with record.open("xb") as stream:
        stream.write(canonical_bytes({**core, "state": "PUBLISHED_SOURCE_QUALITY_ONLY", "record_id": record_id}) + b"\n")
    with event.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_preexecution_source_certification_event/1.0.0",
            "event_type": "PUBLISHED",
            "record_id": record_id,
            "decision": certification["decision"],
            "authorization_receipt_id": authorization.receipt_id,
        }) + b"\n")
    return {
        "record_id": record_id,
        "decision": certification["decision"],
        "record_path": record.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
        "authorization_claim_path": claim.relative_to(root).as_posix(),
    }
