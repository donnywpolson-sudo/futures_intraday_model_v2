"""Pre-data protocol for a source-compatible reported-bar mechanism."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError


REJECTED_REPORT_ID = "3c07d1a690a53f94c00c7dd41ff21b66379ae7d7751a5ac7932f77962584ecdf"
REJECTED_REPORT_PATH = Path(
    "state/unpublished_evidence/cash_open_41_market_source_compatibility_census_v2/"
    f"{REJECTED_REPORT_ID}/source_compatibility.json"
)
REJECTED_REPORT_SHA256 = "32a72ed6a22429e0d2df4b5f4659a4be14808a0172ff5800f700c91f5b545f90"
PROTOCOL_PATH = Path("configs/reported_bar_fixed_horizon_source_discovery_protocol.json")
TOPOLOGY_ROOT = Path("state/unpublished_evidence/reported_bar_source_coverage_topology")
REJECTION_ROOT = Path("state/unpublished_evidence/cash_open_mechanism_source_rejection")
CHECKPOINTS = ("09:00", "09:30", "10:00", "10:30")


def _read_report(root: Path) -> dict[str, object]:
    path = root / REJECTED_REPORT_PATH
    if sha256_file(path) != REJECTED_REPORT_SHA256:
        raise IntegrityError("sealed cash-open source report drifted")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise IntegrityError("sealed cash-open source report is invalid")
    core = {key: value for key, value in payload.items() if key != "report_id"}
    if payload.get("report_id") != REJECTED_REPORT_ID or sha256_json(core) != REJECTED_REPORT_ID:
        raise IntegrityError("sealed cash-open source report identity is invalid")
    return payload


def build_topology_audit(*, root: Path) -> dict[str, object]:
    report = _read_report(root)
    results = report.get("market_configuration_results")
    if not isinstance(results, list) or len(results) != 287:
        raise IntegrityError("sealed source topology is incomplete")
    gate_counts: Counter[str] = Counter()
    single_reason_counts: Counter[str] = Counter()
    checkpoint_rankings: dict[str, list[dict[str, object]]] = {}
    for item in results:
        if not isinstance(item, dict):
            raise IntegrityError("sealed source result is invalid")
        gate_counts.update(str(value) for value in item.get("failed_gates", []))
        checkpoints = item.get("checkpoints")
        folds = item.get("fold_results")
        if isinstance(checkpoints, list) and len(checkpoints) == 1 and isinstance(folds, list):
            checkpoint = str(checkpoints[0])
            percentages: list[float] = []
            for fold in folds:
                if not isinstance(fold, dict):
                    continue
                coverage = fold.get("candidate_path_coverage")
                if isinstance(coverage, dict):
                    percentages.append(float(coverage["percent"]))
                exclusions = fold.get("exclusion_reasons")
                if isinstance(exclusions, dict):
                    single_reason_counts.update({str(key): int(value) for key, value in exclusions.items()})
            checkpoint_rankings.setdefault(checkpoint, []).append({
                "market": item["market"],
                "minimum_fold_percent": min(percentages) if percentages else None,
                "mean_fold_percent": sum(percentages) / len(percentages) if percentages else None,
            })
    best_by_checkpoint: dict[str, list[dict[str, object]]] = {}
    for checkpoint in CHECKPOINTS:
        eligible = [
            item for item in checkpoint_rankings.get(checkpoint, [])
            if item["minimum_fold_percent"] is not None
        ]
        eligible.sort(
            key=lambda item: (
                -float(item["minimum_fold_percent"]),
                -float(item["mean_fold_percent"]),
                str(item["market"]),
            )
        )
        best_by_checkpoint[checkpoint] = eligible[:5]
    source_audits = report.get("source_audits")
    if not isinstance(source_audits, dict):
        raise IntegrityError("sealed source audits are absent")
    sessionless = {
        key: int(value["sessionless_dependency_horizon_rows"])
        for key, value in source_audits.items()
        if isinstance(value, dict) and int(value["sessionless_dependency_horizon_rows"])
    }
    core: dict[str, object] = {
        "schema_version": "reported_bar_source_coverage_topology/1.0.0",
        "classification": "SEALED_EVIDENCE_ONLY_NO_ROW_REREAD_NO_ECONOMICS",
        "rejected_report_id": REJECTED_REPORT_ID,
        "rejected_report_path": REJECTED_REPORT_PATH.as_posix(),
        "rejected_report_sha256": REJECTED_REPORT_SHA256,
        "market_configuration_results": len(results),
        "passing_market_configurations": sum(item.get("status") == "PASS" for item in results),
        "failed_gate_counts": dict(sorted(gate_counts.items())),
        "best_source_coverage_by_single_checkpoint": best_by_checkpoint,
        "single_checkpoint_exclusion_reason_counts": dict(sorted(single_reason_counts.items())),
        "sessionless_source_audits": dict(sorted(sessionless.items())),
        "diagnosis": {
            "current_mechanism_requires_every_feature_minute": True,
            "current_mechanism_requires_every_execution_minute": True,
            "reported_bar_absence_can_mean_no_trade_not_corrupt_data": True,
            "future_incomplete_execution_paths_remain_failures": True,
            "economic_outcome_examined": False,
        },
    }
    return {**core, "topology_id": sha256_json(core)}


def topology_path(audit: Mapping[str, object]) -> Path:
    return TOPOLOGY_ROOT / str(audit["topology_id"]) / "coverage_topology.json"


def build_rejection_record(*, root: Path, audit: Mapping[str, object]) -> dict[str, object]:
    if audit.get("passing_market_configurations") != 0:
        raise IntegrityError("cash-open source rejection is not conclusive")
    core: dict[str, object] = {
        "schema_version": "cash_open_mechanism_source_rejection/1.0.0",
        "classification": "CONCLUSIVE_PRE_REGISTRATION_SOURCE_INCOMPATIBILITY",
        "rejected_mechanism": "CASH_OPEN_EXACT_EVERY_MINUTE_PATH",
        "rejected_report_id": REJECTED_REPORT_ID,
        "rejected_report_path": REJECTED_REPORT_PATH.as_posix(),
        "rejected_report_sha256": REJECTED_REPORT_SHA256,
        "topology_id": audit["topology_id"],
        "topology_path": topology_path(audit).as_posix(),
        "economic_result": "NOT_PRODUCED",
        "trial_registered": False,
        "retry_or_rescue_allowed": False,
        "coverage_gate_weakened": False,
        "conclusion": "NO_EVALUATED_CHECKPOINT_OR_FALLBACK_SUPPORTED_A_USEFUL_MULTI_MARKET_SET_AT_ONE_HUNDRED_PERCENT_PATH_COVERAGE",
        "preservation": "ALL_PREDECESSOR_BYTES_REMAIN_UNCHANGED",
    }
    return {**core, "rejection_id": sha256_json(core)}


def rejection_path(record: Mapping[str, object]) -> Path:
    return REJECTION_ROOT / str(record["rejection_id"]) / "rejection.json"


def build_protocol(
    *, root: Path, audit: Mapping[str, object], rejection: Mapping[str, object]
) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": "reported_bar_fixed_horizon_source_discovery_protocol/1.0.0",
        "state": "PREPARED_NOT_EXECUTED_UNREGISTERED_UNPUBLISHED",
        "classification": "PRE_REGISTRATION_SOURCE_ONLY_NO_ECONOMICS",
        "mechanism": "SINGLE_CHECKPOINT_FIXED_HORIZON_REPORTED_BAR",
        "years": [2018, 2019, 2020, 2021, 2022],
        "market_universe": "ACTIVE_CATALOG_ALL_41_MARKETS_FAIL_CLOSED",
        "checkpoint_grid": list(CHECKPOINTS),
        "checkpoint_configurations": [[value] for value in CHECKPOINTS],
        "decision_rule": {
            "decision_clock": "CHECKPOINT_PLUS_5_SECONDS_AMERICA_CHICAGO",
            "feature_event_window_minutes": 30,
            "feature_rows_must_be_available_by_decision": True,
            "minimum_distinct_reported_feature_minutes": 15,
            "start_anchor_maximum_lag_minutes": 5,
            "terminal_feature_maximum_staleness_minutes": 2,
            "feature_gap_disposition": "EXPLICIT_CAUSAL_ABSTENTION",
        },
        "execution_rule": {
            "entry": "FIRST_REPORTED_BAR_STRICTLY_AFTER_DECISION_WITHIN_2_MINUTES_USING_ONLY_ITS_AVAILABLE_AT_TIME",
            "exit": "FIRST_REPORTED_BAR_AT_OR_AFTER_ENTRY_EVENT_PLUS_30_MINUTES_WITHIN_2_MINUTES_USING_ONLY_ITS_AVAILABLE_AT_TIME",
            "fixed_horizon_minutes": 30,
            "intermediate_every_minute_path_required": False,
            "same_actual_contract_identity": True,
            "missing_or_identity_changing_entry_or_exit": "MANDATORY_PATH_FAILURE",
            "stop_or_target_monitoring": False,
        },
        "coverage_gates": {
            "checkpoint_accounting_percent": 100,
            "feature_complete_overall_percent_minimum": 95,
            "feature_complete_market_year_percent_minimum": 90,
            "feature_complete_market_fold_percent_minimum": 90,
            "minimum_complete_training_sessions_per_fold": 252,
            "minimum_complete_evaluation_sessions_per_fold": 30,
            "feature_complete_candidate_execution_path_percent": 100,
            "feature_complete_each_active_baseline_execution_path_percent": 100,
            "future_incomplete_outcomes_are_never_dropped": True,
        },
        "folds": {
            "construction": "CHECKPOINT_ELIGIBLE_ACTIVE_CALENDAR_SESSIONS_BEFORE_SOURCE_COMPLETENESS",
            "outer_folds": 8,
            "initial_training_calendar_sessions": 504,
            "evaluation_calendar_sessions": 63,
            "embargo_sessions": 1,
            "purge_minutes": 31,
        },
        "baseline_requirements": {
            "flat_no_trade": "EXACT_ZERO_NO_PATH",
            "active_baselines": [
                "ALWAYS_LONG",
                "ALWAYS_SHORT",
                "REPORTED_BAR_CONTINUATION",
                "REPORTED_BAR_REVERSAL",
            ],
            "independent_direction_entries_exits_costs_scheduling_overlap_limits_daily_loss_equity_drawdown": True,
            "candidate_schedule_reuse_forbidden": True,
        },
        "source_resolution": {
            "resolver": "futures_rebuild.active_data_view.resolve",
            "purpose": "SELECTION",
            "active_catalog_only": True,
            "direct_paths_globs_archive_fallbacks_forbidden": True,
        },
        "source_only_selection": {
            "rank": "MAXIMUM_PASSING_MARKET_COUNT_THEN_HIGHEST_WORST_FOLD_FEATURE_COMPLETENESS_THEN_EARLIEST_CHECKPOINT",
            "include_all_passing_markets": True,
            "minimum_markets": 2,
            "returns_costs_predictions_or_outcomes_used": False,
            "no_passing_configuration": "REJECT_AND_DO_NOT_REGISTER",
        },
        "authority": {
            "historical_row_read": False,
            "execution": False,
            "publication": False,
            "registration": False,
            "model_fit": False,
            "prediction_generation": False,
            "performance_evaluation": False,
            "provider_network_credentials": False,
            "year_2025_access": False,
            "trading": False,
        },
        "supersedes_rejected_mechanism_record_id": rejection["rejection_id"],
        "bindings": {
            REJECTED_REPORT_PATH.as_posix(): REJECTED_REPORT_SHA256,
            topology_path(audit).as_posix(): sha256_file(root / topology_path(audit)),
            rejection_path(rejection).as_posix(): sha256_file(root / rejection_path(rejection)),
            "configs/active_cash_open_impulse_historical_calendar.json": sha256_file(
                root / "configs/active_cash_open_impulse_historical_calendar.json"
            ),
            "data/active/catalog.json": sha256_file(root / "data/active/catalog.json"),
            "src/futures_rebuild/active_data_view.py": sha256_file(
                root / "src/futures_rebuild/active_data_view.py"
            ),
            "src/futures_rebuild/reported_bar_fixed_horizon_protocol.py": sha256_file(Path(__file__)),
        },
    }
    return {**core, "protocol_id": sha256_json(core)}


@dataclass(frozen=True)
class ReportedBarEvidence:
    event_at: datetime
    available_at: datetime
    actual_identity_hash: str | None


@dataclass(frozen=True)
class DiscoveryDisposition:
    feature_complete: bool
    execution_complete: bool
    disposition: str


def classify_reported_bar_checkpoint(
    *, checkpoint: datetime, rows: Sequence[ReportedBarEvidence]
) -> DiscoveryDisposition:
    if checkpoint.tzinfo is None:
        raise IntegrityError("checkpoint must be timezone aware")
    decision = checkpoint + timedelta(seconds=5)
    feature_start = checkpoint - timedelta(minutes=30)
    feature = sorted(
        {
            item.event_at: item for item in rows
            if feature_start <= item.event_at < checkpoint and item.available_at <= decision
        }.values(),
        key=lambda item: item.event_at,
    )
    identities = {item.actual_identity_hash for item in feature}
    if (
        len(feature) < 15
        or not feature
        or feature[0].event_at > feature_start + timedelta(minutes=5)
        or feature[-1].event_at < checkpoint - timedelta(minutes=2)
        or len(identities) != 1
        or None in identities
    ):
        return DiscoveryDisposition(False, False, "EXPLICIT_CAUSAL_FEATURE_ABSTENTION")
    entries = sorted(
        (
            item for item in rows
            if item.event_at >= checkpoint + timedelta(minutes=1)
            and item.event_at <= checkpoint + timedelta(minutes=2)
            and item.available_at > decision
        ),
        key=lambda item: (item.available_at, item.event_at),
    )
    if not entries:
        return DiscoveryDisposition(True, False, "EXECUTION_ENTRY_PATH_INCOMPLETE")
    entry = entries[0]
    target = entry.event_at + timedelta(minutes=30)
    exits = sorted(
        (
            item for item in rows
            if target <= item.event_at <= target + timedelta(minutes=2)
            and item.available_at > entry.available_at
        ),
        key=lambda item: (item.available_at, item.event_at),
    )
    if not exits:
        return DiscoveryDisposition(True, False, "EXECUTION_EXIT_PATH_INCOMPLETE")
    exit_bar = exits[0]
    if entry.actual_identity_hash != feature[0].actual_identity_hash or exit_bar.actual_identity_hash != entry.actual_identity_hash:
        return DiscoveryDisposition(True, False, "EXECUTION_IDENTITY_CHANGING")
    return DiscoveryDisposition(True, True, "COMPLETE")
