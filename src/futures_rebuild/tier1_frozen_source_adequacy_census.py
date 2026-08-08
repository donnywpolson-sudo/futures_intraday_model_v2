"""Receipt-gated reported-bar source adequacy census for the frozen successor.

This is source-quality evidence only.  It applies the preregistered reported
trade-bar semantics to every calendar-open checkpoint in the already-selected
20 immutable causal releases, retains every checkpoint, and evaluates the
unchanged 95% overall / 90% market-year feature-coverage gates plus a 90%
market-fold and 30-session minimum.  It never fits or evaluates a model.
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
from .tier1_bracket_v4 import FoldSpec, build_v4_folds_from_census
from .tier1_frozen_successor_source_semantics import (
    compute_reported_feature_values,
    select_reported_execution_path,
    select_reported_feature_window,
)
from .tier1_preexecution_gap_inventory import (
    CALENDAR_RELEASE_ID, SOURCE_RECORD_ID, SOURCE_RECORD_PATH,
    SOURCE_RECORD_SHA256, _selected_sources,
)
from .tier1_preexecution_recovery_feasibility import (
    GAP_RECORD_ID, GAP_RECORD_SHA256,
)


PLAN_PATH = Path("configs/tier1_frozen_source_adequacy_census_plan.json")
RECOVERY_RECORD_PATH = Path(
    "state/source_quality/tier1_preexecution_recovery_feasibility/"
    "c343a1ad972f51c9af774b9c7ffb163a3337b9cbf24c0cddeb0d17ca4a35c4eb.json"
)
RECOVERY_RECORD_ID = RECOVERY_RECORD_PATH.stem
RECOVERY_RECORD_SHA256 = "801aa65057651b0021441b0f552b4694894f45a18594921fc90299a0fd50a92a"
OPERATION = "CENSUS_FROZEN_TIER1_REPORTED_BAR_SOURCE_ADEQUACY_AND_PUBLISH"
RECORD_ROOT = Path("state/source_quality/tier1_frozen_source_adequacy")
EVENT_ROOT = Path("state/source_quality_events/tier1_frozen_source_adequacy")
OVERALL_FEATURE_RATE_PERCENT = 95
MARKET_YEAR_FEATURE_RATE_PERCENT = 90
MARKET_FOLD_FEATURE_RATE_PERCENT = 90
MINIMUM_MARKET_FOLD_FEATURE_SESSIONS = 30


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid source-adequacy artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("source-adequacy artifact is not an object")
    return value


def _load_selected_sources(*, root: Path) -> dict[str, dict[str, object]]:
    path = root / SOURCE_RECORD_PATH
    if sha256_file(path) != SOURCE_RECORD_SHA256:
        raise IntegrityError("selected-source record changed")
    record = _object(path)
    if record.get("record_id") != SOURCE_RECORD_ID:
        raise IntegrityError("selected-source record identity changed")
    return _selected_sources(record)


def _load_recovery_record(*, root: Path) -> dict[str, object]:
    path = root / RECOVERY_RECORD_PATH
    if sha256_file(path) != RECOVERY_RECORD_SHA256:
        raise IntegrityError("canonical recovery record changed")
    record = _object(path)
    if (
        record.get("record_id") != RECOVERY_RECORD_ID
        or record.get("state") != "PUBLISHED_SOURCE_QUALITY_ONLY"
        or record.get("gap_record_id") != GAP_RECORD_ID
        or record.get("gap_record_sha256") != GAP_RECORD_SHA256
        or record.get("prices_reported") is not False
        or record.get("historical_evaluation") is not False
    ):
        raise IntegrityError("canonical recovery record is not the certified source-only record")
    return record


@dataclass(frozen=True)
class CheckpointCoverage:
    opportunity_id: str
    market: str
    year: int
    exchange_session_date: str
    checkpoint: str
    feature_status: str
    feature_reason: str | None
    execution_status: str
    execution_reason: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "opportunity_id": self.opportunity_id,
            "market": self.market,
            "year": self.year,
            "exchange_session_date": self.exchange_session_date,
            "checkpoint": self.checkpoint,
            "feature_status": self.feature_status,
            "feature_reason": self.feature_reason,
            "execution_status": self.execution_status,
            "execution_reason": self.execution_reason,
        }


def classify_session_checkpoints(
    *, source_rows: Sequence[v5.V5SourceRecord],
    checkpoints: Sequence[v5.CensusCheckpoint],
) -> tuple[CheckpointCoverage, ...]:
    """Retain each open checkpoint as complete or an explicit abstention."""

    output: list[CheckpointCoverage] = []
    for checkpoint in checkpoints:
        if not checkpoint.calendar_open:
            continue
        expected = checkpoint.expected
        try:
            window = select_reported_feature_window(
                source_rows=source_rows, market=expected.market,
                exchange_session_date=expected.exchange_session_date,
                decision_at_ns=expected.decision_at_ns,
            )
            compute_reported_feature_values(
                window=window, decision_at_ns=expected.decision_at_ns,
            )
            feature_status, feature_reason = "COMPLETE", None
        except IntegrityError as exc:
            feature_status, feature_reason = "CAUSAL_ABSTENTION", str(exc)
        try:
            select_reported_execution_path(
                source_rows=source_rows, market=expected.market,
                exchange_session_date=expected.exchange_session_date,
                decision_at_ns=expected.decision_at_ns,
            )
            execution_status, execution_reason = "COMPLETE", None
        except IntegrityError as exc:
            execution_status, execution_reason = "EXPLICIT_UNAVAILABLE", str(exc)
        output.append(CheckpointCoverage(
            expected.opportunity_id, expected.market, expected.year,
            expected.exchange_session_date, expected.checkpoint,
            feature_status, feature_reason, execution_status, execution_reason,
        ))
    return tuple(output)


def _rate_gate(*, complete: int, expected: int, percent: int) -> bool:
    return expected > 0 and complete * 100 >= expected * percent


def adjudicate_source_adequacy(
    *, records: Sequence[CheckpointCoverage], expected_ids: Sequence[str],
    folds: Sequence[FoldSpec],
) -> dict[str, object]:
    for record in records:
        if (
            record.market not in v5.MARKETS
            or record.year not in range(2018, 2023)
            or record.feature_status not in {"COMPLETE", "CAUSAL_ABSTENTION"}
            or record.execution_status not in {"COMPLETE", "EXPLICIT_UNAVAILABLE"}
        ):
            raise IntegrityError("source-adequacy checkpoint record is invalid")
    ids = [record.opportunity_id for record in records]
    if (
        not expected_ids or len(expected_ids) != len(set(expected_ids))
        or len(ids) != len(set(ids)) or set(ids) != set(expected_ids)
    ):
        raise IntegrityError("source-adequacy checkpoint ledger does not reconcile")
    overall_complete = sum(item.feature_status == "COMPLETE" for item in records)
    overall_gate = _rate_gate(
        complete=overall_complete, expected=len(records),
        percent=OVERALL_FEATURE_RATE_PERCENT,
    )
    market_years: dict[str, dict[str, object]] = {}
    for market in v5.MARKETS:
        for year in range(2018, 2023):
            scoped = [item for item in records if item.market == market and item.year == year]
            complete = sum(item.feature_status == "COMPLETE" for item in scoped)
            market_years[f"{market}/{year}"] = {
                "expected_open_checkpoints": len(scoped),
                "complete_feature_windows": complete,
                "feature_rate": complete / len(scoped) if scoped else 0.0,
                "status": "PASS" if _rate_gate(
                    complete=complete, expected=len(scoped),
                    percent=MARKET_YEAR_FEATURE_RATE_PERCENT,
                ) else "FAIL",
            }
    market_folds: dict[str, dict[str, object]] = {}
    for fold in folds:
        for market in v5.MARKETS:
            for role, sessions in (
                ("training", set(fold.training_sessions)),
                ("test", set(fold.test_sessions)),
            ):
                scoped = [
                    item for item in records
                    if item.market == market and item.exchange_session_date in sessions
                ]
                complete = [item for item in scoped if item.feature_status == "COMPLETE"]
                complete_sessions = len({item.exchange_session_date for item in complete})
                status = (
                    "PASS" if _rate_gate(
                        complete=len(complete), expected=len(scoped),
                        percent=MARKET_FOLD_FEATURE_RATE_PERCENT,
                    ) and complete_sessions >= MINIMUM_MARKET_FOLD_FEATURE_SESSIONS
                    else "FAIL"
                )
                market_folds[f"fold-{fold.outer_fold}/{market}/{role}"] = {
                    "expected_open_checkpoints": len(scoped),
                    "complete_feature_windows": len(complete),
                    "complete_feature_sessions": complete_sessions,
                    "feature_rate": len(complete) / len(scoped) if scoped else 0.0,
                    "status": status,
                }
    execution_counts = {
        "complete": sum(item.execution_status == "COMPLETE" for item in records),
        "explicit_unavailable": sum(
            item.execution_status == "EXPLICIT_UNAVAILABLE" for item in records
        ),
    }
    checks = {
        "terminal_open_checkpoint_ledger_complete": len(records) == len(expected_ids),
        "overall_feature_rate_at_least_95_percent": overall_gate,
        "every_market_year_feature_rate_at_least_90_percent": all(
            item["status"] == "PASS" for item in market_years.values()
        ),
        "every_market_fold_role_feature_rate_at_least_90_percent_and_30_sessions": all(
            item["status"] == "PASS" for item in market_folds.values()
        ),
        "every_execution_path_has_terminal_source_status": sum(execution_counts.values()) == len(records),
        "every_feature_complete_checkpoint_has_complete_execution_path": all(
            item.feature_status != "COMPLETE" or item.execution_status == "COMPLETE"
            for item in records
        ),
        "incomplete_selected_execution_forces_trial_rejection": True,
    }
    return {
        "decision": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "overall": {
            "expected_open_checkpoints": len(records),
            "complete_feature_windows": overall_complete,
            "feature_rate": overall_complete / len(records),
            "execution": execution_counts,
        },
        "market_years": market_years,
        "market_folds": market_folds,
    }


def load_source_adequacy_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    forbidden = plan.get("forbidden_actions")
    selected = _load_selected_sources(root=root)
    recovery = _load_recovery_record(root=root)
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "tier1_frozen_source_adequacy_census_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_SEPARATE_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("source_record_id") != SOURCE_RECORD_ID
        or plan.get("source_record_sha256") != SOURCE_RECORD_SHA256
        or plan.get("selected_sources_id") != sha256_json(selected)
        or plan.get("selected_release_count") != 20
        or plan.get("recovery_record_id") != RECOVERY_RECORD_ID
        or plan.get("recovery_record_sha256") != RECOVERY_RECORD_SHA256
        or recovery.get("dbn_release_id") != plan.get("dbn_release_id")
        or plan.get("calendar_release_id") != CALENDAR_RELEASE_ID
        or plan.get("overall_feature_rate_percent") != OVERALL_FEATURE_RATE_PERCENT
        or plan.get("market_year_feature_rate_percent") != MARKET_YEAR_FEATURE_RATE_PERCENT
        or plan.get("market_fold_feature_rate_percent") != MARKET_FOLD_FEATURE_RATE_PERCENT
        or plan.get("minimum_market_fold_feature_sessions") != MINIMUM_MARKET_FOLD_FEATURE_SESSIONS
        or plan.get("source_semantics_sha256") != sha256_file(
            root / "src/futures_rebuild/tier1_frozen_successor_source_semantics.py"
        )
        or plan.get("implementation_sha256") != sha256_file(Path(__file__))
        or plan.get("maximum_host_runtime_seconds") != 900
        or plan.get("estimated_external_cost_usd") != "0"
        or not isinstance(forbidden, dict) or not forbidden
        or not all(value is True for value in forbidden.values())
    ):
        raise UnauthorizedOperation("source-adequacy census plan is absent or drifted")
    return plan


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "source_record_id": SOURCE_RECORD_ID,
        "source_record_sha256": SOURCE_RECORD_SHA256,
        "selected_release_count": "20",
        "recovery_record_id": RECOVERY_RECORD_ID,
        "recovery_record_sha256": RECOVERY_RECORD_SHA256,
        "calendar_release_id": CALENDAR_RELEASE_ID,
        "source_scope": "6E,CL,ES,ZN|2018,2019,2020,2021,2022|selected-causal-releases",
        "publication_root": RECORD_ROOT.as_posix(),
        "historical_row_read": "true", "publication": "true",
        "provider_access": "false", "diagnostic_source_families": "false",
        "successor_data_creation": "false", "active_data_mutation": "false",
        "model_fit": "false", "prediction_generation": "false",
        "historical_evaluation": "false", "trial_registration_or_retirement": "false",
        "holdout_or_forward_access": "false", "staging": "false",
        "commit": "false", "push": "false", "trading": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def execute_authorized_source_adequacy_census(
    *, root: Path, authorization: OperationReceipt,
) -> dict[str, object]:
    boundary = RepoBoundary(root)
    plan = load_source_adequacy_plan(root=root)
    require_locked_repository_environment(root)
    claim = authorization.consume(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=_required_scope(root=root, plan=plan),
    )
    selected = _load_selected_sources(root=root)
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary, registered_calendar_index_release_id=CALENDAR_RELEASE_ID,
    )
    census = v5.build_expected_census_from_calendar(sessions=sessions)
    open_census = tuple(item for item in census if item.calendar_open)
    expected_ids = [item.expected.opportunity_id for item in open_census]
    by_cell: dict[tuple[str, int], dict[str, list[v5.CensusCheckpoint]]] = {}
    for checkpoint in open_census:
        item = checkpoint.expected
        by_cell.setdefault((item.market, item.year), {}).setdefault(
            item.exchange_session_date, []
        ).append(checkpoint)
    catalog = {str(item["release_id"]): item for item in _catalog()}
    records: list[CheckpointCoverage] = []
    source_audits: dict[str, dict[str, object]] = {}
    for key in sorted(selected):
        market, year_text = key.split("/")
        year = int(year_text)
        source = selected[key]
        item = catalog.get(str(source["release_id"]))
        if (
            item is None or item.get("market") != market or item.get("year") != year
            or item.get("payload_sha256") != source["payload_sha256"]
        ):
            raise IntegrityError("selected causal source identity changed")
        path = _candidate_path(boundary=boundary, item=item)
        audit = v10.SourceIntegrityAuditV10(market)
        stream = v10.iter_source_records_from_parquet_v10(
            market=market, path=path, audit=audit,
        )
        active: str | None = None
        rows: list[v5.V5SourceRecord] = []
        processed: set[str] = set()

        def flush() -> None:
            nonlocal rows, active
            if active is None:
                return
            if active in processed:
                raise IntegrityError("selected causal source session is not contiguous")
            processed.add(active)
            records.extend(classify_session_checkpoints(
                source_rows=tuple(rows),
                checkpoints=tuple(by_cell[(market, year)].get(active, ())),
            ))
            rows = []

        for row in stream:
            if active is None:
                active = row.exchange_session_date
            elif row.exchange_session_date != active:
                flush()
                active = row.exchange_session_date
            rows.append(row)
            if len(rows) > 2_000:
                raise IntegrityError("source-adequacy session buffer exceeded 2,000 rows")
        flush()
        for session_date, checkpoints in by_cell[(market, year)].items():
            if session_date not in processed:
                records.extend(classify_session_checkpoints(
                    source_rows=(), checkpoints=tuple(checkpoints),
                ))
        source_audits[key] = audit.as_dict()
    records.sort(key=lambda item: (
        item.exchange_session_date, item.market, item.checkpoint,
    ))
    folds = build_v4_folds_from_census([item.expected for item in open_census])
    adjudication = adjudicate_source_adequacy(
        records=records, expected_ids=expected_ids, folds=folds,
    )
    core = {
        "schema_version": "tier1_frozen_source_adequacy/1.0.0",
        "state": "PREPARED_CREATE_ONLY", "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "source_record_id": SOURCE_RECORD_ID,
        "source_record_sha256": SOURCE_RECORD_SHA256,
        "selected_sources_id": sha256_json(selected),
        "recovery_record_id": RECOVERY_RECORD_ID,
        "recovery_record_sha256": RECOVERY_RECORD_SHA256,
        "calendar_release_id": CALENDAR_RELEASE_ID,
        "checkpoint_coverage": [item.as_dict() for item in records],
        "adjudication": adjudication,
        "source_integrity_audits": dict(sorted(source_audits.items())),
        "prices_reported": False, "provider_access": False,
        "diagnostic_source_families_read": False, "successor_data_created": False,
        "active_data_mutation": False, "model_fit": False,
        "prediction_generation": False, "historical_evaluation": False,
        "trial_registration_or_retirement": False,
        "holdout_or_forward_access": False, "trading": False,
    }
    record_id = sha256_json(core)
    record = root / RECORD_ROOT / f"{record_id}.json"
    event = root / EVENT_ROOT / f"{record_id}.json"
    boundary.assert_active_path(record.absolute(), purpose="frozen source adequacy", subtree=RECORD_ROOT.as_posix())
    boundary.assert_active_path(event.absolute(), purpose="frozen source adequacy event", subtree=EVENT_ROOT.as_posix())
    if record.exists() or event.exists():
        raise IntegrityError("source-adequacy publication is create-only")
    record.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    with record.open("xb") as stream:
        stream.write(canonical_bytes({
            **core, "state": "PUBLISHED_SOURCE_QUALITY_ONLY", "record_id": record_id,
        }) + b"\n")
    with event.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_frozen_source_adequacy_event/1.0.0",
            "event_type": "PUBLISHED", "record_id": record_id,
            "decision": adjudication["decision"],
            "authorization_receipt_id": authorization.receipt_id,
        }) + b"\n")
    return {
        "record_id": record_id, "decision": adjudication["decision"],
        "record_path": record.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
        "authorization_claim_path": claim.relative_to(root).as_posix(),
        "overall": adjudication["overall"],
        "failed_market_years": [
            key for key, value in adjudication["market_years"].items()
            if value["status"] == "FAIL"
        ],
        "failed_market_folds": [
            key for key, value in adjudication["market_folds"].items()
            if value["status"] == "FAIL"
        ],
    }
