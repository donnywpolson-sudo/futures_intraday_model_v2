"""Receipt-gated dependency-gap inventory for the frozen Tier 1 successor.

This module classifies every registered calendar-open checkpoint in the 20
already-selected immutable releases.  It distinguishes a dependency window
that the registered session horizon makes impossible before any price read
from a genuine missing, non-executable, ambiguous, or causally late source
dependency.  It never fits, predicts, evaluates performance, or opens 2025.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from scripts.run_v12_local_source_alternative_census import _candidate_path, _catalog

from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v10 as v10
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment


PLAN_PATH = Path("configs/tier1_preexecution_gap_inventory_plan.json")
SOURCE_RECORD_PATH = Path(
    "state/source_quality/tier1_preexecution_source_certification/"
    "7a7db45fb4e1a2e3825969e99781fd6f0d02b4dad7a7376b3f0163a0bb41cda5.json"
)
SOURCE_RECORD_ID = SOURCE_RECORD_PATH.stem
SOURCE_RECORD_SHA256 = "6707a0be36384ea992bb8c83a0abb3fcb5cea41ae7299d7781f4b6c7ef41b531"
CALENDAR_RELEASE_ID = "038940d82031f31e2c66ed37186e98a6ee6cff3e7248f634f2c7a8e94ea6ecf3"
OPERATION = "CLASSIFY_FROZEN_TIER1_DEPENDENCY_GAPS_AND_PUBLISH"
RECORD_ROOT = Path("state/source_quality/tier1_preexecution_gap_inventory")
EVENT_ROOT = Path("state/source_quality_events/tier1_preexecution_gap_inventory")
EXECUTION_MINUTES = 61
FEATURE_MINUTES = 61


def _load_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid gap-inventory artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("gap-inventory artifact is not an object")
    return value


def _load_source_record(*, root: Path) -> dict[str, object]:
    path = root / SOURCE_RECORD_PATH
    if sha256_file(path) != SOURCE_RECORD_SHA256:
        raise IntegrityError("selected-source record changed")
    record = _load_object(path)
    if (
        record.get("record_id") != SOURCE_RECORD_ID
        or record.get("state") != "PUBLISHED_SOURCE_QUALITY_ONLY"
        or record.get("calendar_release_id") != CALENDAR_RELEASE_ID
        or record.get("model_fit") is not False
        or record.get("prediction_generation") is not False
        or record.get("historical_evaluation") is not False
    ):
        raise IntegrityError("selected-source record is not the certified research-free record")
    return record


def _selected_sources(record: Mapping[str, object]) -> dict[str, dict[str, object]]:
    certification = record.get("certification")
    selected = certification.get("selected") if isinstance(certification, Mapping) else None
    if not isinstance(selected, Mapping) or len(selected) != 20:
        raise IntegrityError("selected-source record does not contain 20 cells")
    result: dict[str, dict[str, object]] = {}
    for key, value in selected.items():
        if not isinstance(key, str) or not isinstance(value, Mapping):
            raise IntegrityError("selected-source cell is malformed")
        release_id = value.get("release_id")
        payload_sha256 = value.get("payload_sha256")
        if not isinstance(release_id, str) or not isinstance(payload_sha256, str):
            raise IntegrityError("selected-source identity is incomplete")
        result[key] = {"release_id": release_id, "payload_sha256": payload_sha256}
    expected = {
        f"{market}/{year}" for market in v5.MARKETS for year in range(2018, 2023)
    }
    if set(result) != expected:
        raise IntegrityError("selected-source market-year scope is incomplete")
    return result


def load_gap_inventory_plan(*, root: Path) -> dict[str, object]:
    plan = _load_object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    forbidden = plan.get("forbidden_actions")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "tier1_preexecution_gap_inventory_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_EXACT_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("source_record_id") != SOURCE_RECORD_ID
        or plan.get("source_record_sha256") != SOURCE_RECORD_SHA256
        or plan.get("calendar_release_id") != CALENDAR_RELEASE_ID
        or plan.get("selected_release_count") != 20
        or plan.get("maximum_host_runtime_seconds") != 900
        or plan.get("estimated_external_cost_usd") != "0"
        or plan.get("implementation_sha256") != sha256_file(Path(__file__))
        or not isinstance(forbidden, dict)
        or not forbidden
        or not all(value is True for value in forbidden.values())
    ):
        raise UnauthorizedOperation("gap-inventory plan is absent or drifted")
    record = _load_source_record(root=root)
    if plan.get("selected_sources_id") != sha256_json(_selected_sources(record)):
        raise UnauthorizedOperation("gap-inventory selected-source binding drifted")
    return plan


@dataclass(frozen=True)
class CheckpointGap:
    checkpoint_id: str
    market: str
    year: int
    exchange_session_date: str
    checkpoint: str
    decision_at_ns: int
    registered_session_close_at_ns: int
    disposition: str
    reason_codes: tuple[str, ...]
    feature_anchor_at_ns: int | None
    missing_feature_timestamps_ns: tuple[int, ...]
    nonexecutable_feature_timestamps_ns: tuple[int, ...]
    causally_late_feature_timestamps_ns: tuple[int, ...]
    identity_mismatch_feature_timestamps_ns: tuple[int, ...]
    missing_execution_timestamps_ns: tuple[int, ...]
    nonexecutable_execution_timestamps_ns: tuple[int, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "market": self.market,
            "year": self.year,
            "exchange_session_date": self.exchange_session_date,
            "checkpoint": self.checkpoint,
            "decision_at_ns": self.decision_at_ns,
            "registered_session_close_at_ns": self.registered_session_close_at_ns,
            "disposition": self.disposition,
            "reason_codes": list(self.reason_codes),
            "feature_anchor_at_ns": self.feature_anchor_at_ns,
            "missing_feature_timestamps_ns": list(self.missing_feature_timestamps_ns),
            "nonexecutable_feature_timestamps_ns": list(self.nonexecutable_feature_timestamps_ns),
            "causally_late_feature_timestamps_ns": list(self.causally_late_feature_timestamps_ns),
            "identity_mismatch_feature_timestamps_ns": list(self.identity_mismatch_feature_timestamps_ns),
            "missing_execution_timestamps_ns": list(self.missing_execution_timestamps_ns),
            "nonexecutable_execution_timestamps_ns": list(self.nonexecutable_execution_timestamps_ns),
        }


def classify_checkpoint_dependencies(
    *, source_rows: Sequence[v5.V5SourceRecord], checkpoint: v5.CensusCheckpoint,
    registered_session_close_at_ns: int,
) -> CheckpointGap:
    """Classify one open checkpoint without inspecting prices or outcomes."""

    if not checkpoint.calendar_open:
        raise IntegrityError("closed calendar checkpoint is outside the gap inventory")
    expected = checkpoint.expected
    if registered_session_close_at_ns <= expected.decision_at_ns:
        raise IntegrityError("registered session close does not contain the open checkpoint")
    horizon_end = expected.decision_at_ns + EXECUTION_MINUTES * v5.NS_PER_MINUTE
    if horizon_end > registered_session_close_at_ns:
        return CheckpointGap(
            expected.opportunity_id, expected.market, expected.year,
            expected.exchange_session_date, expected.checkpoint,
            expected.decision_at_ns, registered_session_close_at_ns,
            "PRECAUSAL_SESSION_HORIZON_ABSTENTION",
            ("REGISTERED_SESSION_ENDS_BEFORE_REQUIRED_EXECUTION_HORIZON",),
            None, (), (), (), (), (), (),
        )
    for row in source_rows:
        row.validate()
        if row.market != expected.market or row.exchange_session_date != expected.exchange_session_date:
            raise IntegrityError("checkpoint classifier received a foreign source row")

    by_event: dict[int, list[v5.V5SourceRecord]] = {}
    for row in source_rows:
        if row.bar is not None:
            by_event.setdefault(row.bar.event_at_ns, []).append(row)
    if not source_rows:
        return CheckpointGap(
            expected.opportunity_id, expected.market, expected.year,
            expected.exchange_session_date, expected.checkpoint,
            expected.decision_at_ns, registered_session_close_at_ns,
            "MISSING_SOURCE_DEPENDENCIES", ("MISSING_SOURCE_SESSION",),
            None, (), (), (), (), (), (),
        )
    if any(len(rows) != 1 for rows in by_event.values()):
        return CheckpointGap(
            expected.opportunity_id, expected.market, expected.year,
            expected.exchange_session_date, expected.checkpoint,
            expected.decision_at_ns, registered_session_close_at_ns,
            "AMBIGUOUS_SOURCE_DEPENDENCIES", ("DUPLICATE_EVENT_TIMESTAMP",),
            None, (), (), (), (), (), (),
        )

    executable = {
        event: rows[0] for event, rows in by_event.items() if rows[0].executable
    }
    causal_events = [
        event for event, row in executable.items()
        if row.bar is not None and row.bar.available_at_ns <= expected.decision_at_ns
    ]
    feature_anchor = max(causal_events) if causal_events else None
    missing_feature: list[int] = []
    nonexec_feature: list[int] = []
    late_feature: list[int] = []
    identity_feature: list[int] = []
    if feature_anchor is None:
        feature_required: set[int] = set()
    else:
        feature_required = {
            feature_anchor - offset * v5.NS_PER_MINUTE
            for offset in range(FEATURE_MINUTES)
        }
        anchor_identity = executable[feature_anchor].actual_identity_hash
        for event in sorted(feature_required):
            rows = by_event.get(event)
            if rows is None:
                missing_feature.append(event)
                continue
            row = rows[0]
            if not row.executable:
                nonexec_feature.append(event)
            elif row.bar is None or row.bar.available_at_ns > expected.decision_at_ns:
                late_feature.append(event)
            elif row.actual_identity_hash != anchor_identity:
                identity_feature.append(event)

    execution_required = {
        expected.decision_at_ns + offset * v5.NS_PER_MINUTE
        for offset in range(1, EXECUTION_MINUTES + 1)
    }
    missing_execution: list[int] = []
    nonexec_execution: list[int] = []
    for event in sorted(execution_required):
        rows = by_event.get(event)
        if rows is None:
            missing_execution.append(event)
        elif not rows[0].executable:
            nonexec_execution.append(event)

    reasons: list[str] = []
    if feature_anchor is None:
        reasons.append("NO_CAUSAL_EXECUTABLE_FEATURE_ANCHOR")
    if missing_feature:
        reasons.append("MISSING_FEATURE_TIMESTAMPS")
    if nonexec_feature:
        reasons.append("NONEXECUTABLE_FEATURE_TIMESTAMPS")
    if late_feature:
        reasons.append("CAUSALLY_LATE_FEATURE_TIMESTAMPS")
    if identity_feature:
        reasons.append("FEATURE_IDENTITY_MISMATCH")
    if missing_execution:
        reasons.append("MISSING_EXECUTION_TIMESTAMPS")
    if nonexec_execution:
        reasons.append("NONEXECUTABLE_EXECUTION_TIMESTAMPS")
    disposition = "COMPLETE_DEPENDENCIES" if not reasons else "MISSING_SOURCE_DEPENDENCIES"
    return CheckpointGap(
        expected.opportunity_id, expected.market, expected.year,
        expected.exchange_session_date, expected.checkpoint,
        expected.decision_at_ns, registered_session_close_at_ns,
        disposition, tuple(reasons), feature_anchor,
        tuple(missing_feature), tuple(nonexec_feature), tuple(late_feature),
        tuple(identity_feature), tuple(missing_execution), tuple(nonexec_execution),
    )


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "source_record_id": SOURCE_RECORD_ID,
        "source_record_sha256": SOURCE_RECORD_SHA256,
        "selected_release_count": "20",
        "calendar_release_id": CALENDAR_RELEASE_ID,
        "source_scope": "6E,CL,ES,ZN|2018,2019,2020,2021,2022",
        "classification_scope": "MISSING_SOURCE_DEPENDENCIES_VS_PRECAUSAL_SESSION_HORIZON_ABSTENTIONS_ONLY",
        "publication_root": RECORD_ROOT.as_posix(),
        "historical_row_read": "true",
        "publication": "true",
        "model_fit": "false",
        "prediction_generation": "false",
        "historical_evaluation": "false",
        "trial_registration_or_retirement": "false",
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


def execute_authorized_gap_inventory(
    *, root: Path, authorization: OperationReceipt,
) -> dict[str, object]:
    """Consume one approval, scan 20 selected releases, and publish one record."""

    boundary = RepoBoundary(root)
    plan = load_gap_inventory_plan(root=root)
    require_locked_repository_environment(root)
    claim = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=_required_scope(root=root, plan=plan),
    )
    source_record = _load_source_record(root=root)
    selected = _selected_sources(source_record)
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary, registered_calendar_index_release_id=CALENDAR_RELEASE_ID,
    )
    session_map = {(item.market, item.exchange_session_date): item for item in sessions}
    census = v5.build_expected_census_from_calendar(sessions=sessions)
    expected: dict[tuple[str, int], dict[str, list[v5.CensusCheckpoint]]] = {}
    for checkpoint in census:
        item = checkpoint.expected
        expected.setdefault((item.market, item.year), {}).setdefault(
            item.exchange_session_date, []
        ).append(checkpoint)

    catalog_by_release = {str(item["release_id"]): item for item in _catalog()}
    inventory: list[dict[str, object]] = []
    source_audits: dict[str, dict[str, object]] = {}
    for key in sorted(selected):
        market, year_text = key.split("/")
        year = int(year_text)
        selected_item = selected[key]
        item = catalog_by_release.get(str(selected_item["release_id"]))
        if (
            item is None
            or item.get("market") != market
            or item.get("year") != year
            or item.get("payload_sha256") != selected_item["payload_sha256"]
        ):
            raise IntegrityError("selected immutable release is absent or drifted")
        path = _candidate_path(boundary=boundary, item=item)
        audit = v10.SourceIntegrityAuditV10(market)
        stream = v10.iter_source_records_from_parquet_v10(
            market=market, path=path, audit=audit,
        )
        active: str | None = None
        rows: list[v5.V5SourceRecord] = []
        processed: set[str] = set()

        def flush() -> None:
            nonlocal active, rows
            if active is None:
                return
            if active in processed:
                raise IntegrityError("selected source session is not contiguous")
            processed.add(active)
            session = session_map.get((market, active))
            if session is None:
                raise IntegrityError("selected source includes an unregistered session")
            for checkpoint in expected[(market, year)].get(active, []):
                if checkpoint.calendar_open:
                    inventory.append(classify_checkpoint_dependencies(
                        source_rows=tuple(rows), checkpoint=checkpoint,
                        registered_session_close_at_ns=session.close_at_ns,
                    ).as_dict())
            rows = []

        for row in stream:
            if active is None:
                active = row.exchange_session_date
            elif row.exchange_session_date != active:
                flush()
                active = row.exchange_session_date
            rows.append(row)
            if len(rows) > 2_000:
                raise IntegrityError("selected source session buffer exceeded 2,000 rows")
        flush()
        for session_date, checkpoints in expected[(market, year)].items():
            if session_date in processed:
                continue
            session = session_map[(market, session_date)]
            for checkpoint in checkpoints:
                if checkpoint.calendar_open:
                    inventory.append(classify_checkpoint_dependencies(
                        source_rows=(), checkpoint=checkpoint,
                        registered_session_close_at_ns=session.close_at_ns,
                    ).as_dict())
        source_audits[key] = audit.as_dict()

    inventory.sort(key=lambda item: (
        str(item["exchange_session_date"]), str(item["market"]), str(item["checkpoint"])
    ))
    checkpoint_ids = [str(item["checkpoint_id"]) for item in inventory]
    if not inventory or len(checkpoint_ids) != len(set(checkpoint_ids)):
        raise IntegrityError("gap inventory is empty or duplicates a checkpoint")
    disposition_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    for item in inventory:
        disposition = str(item["disposition"])
        disposition_counts[disposition] = disposition_counts.get(disposition, 0) + 1
        for reason in item["reason_codes"]:
            reason_counts[str(reason)] = reason_counts.get(str(reason), 0) + 1

    core = {
        "schema_version": "tier1_preexecution_gap_inventory/1.0.0",
        "state": "PREPARED_CREATE_ONLY",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "source_record_id": SOURCE_RECORD_ID,
        "source_record_sha256": SOURCE_RECORD_SHA256,
        "calendar_release_id": CALENDAR_RELEASE_ID,
        "selected_sources_id": sha256_json(selected),
        "selected_release_count": 20,
        "checkpoint_count": len(inventory),
        "disposition_counts": dict(sorted(disposition_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
        "checkpoint_inventory": inventory,
        "source_integrity_audits": dict(sorted(source_audits.items())),
        "registered_calendar_interpretation": {
            "open_universe": "CHECKPOINT_OPEN_TRUE_ONLY",
            "session_horizon": "REGISTERED_CALENDAR_SESSION_CLOSE_AT_NS",
            "price_rows_do_not_define_session_horizon": True,
        },
        "prices_reported": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "trial_registration_or_retirement": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "active_data_mutation": False,
        "trading": False,
    }
    record_id = sha256_json(core)
    record = root / RECORD_ROOT / f"{record_id}.json"
    event = root / EVENT_ROOT / f"{record_id}.json"
    boundary.assert_active_path(
        record.absolute(), purpose="dependency gap inventory", subtree=RECORD_ROOT.as_posix(),
    )
    boundary.assert_active_path(
        event.absolute(), purpose="dependency gap inventory event", subtree=EVENT_ROOT.as_posix(),
    )
    if record.exists() or event.exists():
        raise IntegrityError("dependency-gap publication is create-only")
    record.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    with record.open("xb") as stream:
        stream.write(canonical_bytes({
            **core, "state": "PUBLISHED_SOURCE_QUALITY_ONLY", "record_id": record_id,
        }) + b"\n")
    with event.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_preexecution_gap_inventory_event/1.0.0",
            "event_type": "PUBLISHED",
            "record_id": record_id,
            "source_record_id": SOURCE_RECORD_ID,
            "authorization_receipt_id": authorization.receipt_id,
        }) + b"\n")
    return {
        "record_id": record_id,
        "record_path": record.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
        "authorization_claim_path": claim.relative_to(root).as_posix(),
        "checkpoint_count": len(inventory),
        "disposition_counts": dict(sorted(disposition_counts.items())),
        "reason_counts": dict(sorted(reason_counts.items())),
    }
