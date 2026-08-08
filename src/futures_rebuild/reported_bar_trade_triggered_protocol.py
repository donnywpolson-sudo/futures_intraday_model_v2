"""Pre-data trade-triggered reported-bar source-discovery protocol."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError
from .reported_bar_fixed_horizon_protocol import ReportedBarEvidence


REJECTED_REPORT_ID = "4650de0f7fb16c881f38a60dcc0c7861be9d8f76de9e346a1464490e42178731"
REJECTED_REPORT_PATH = Path(
    "state/unpublished_evidence/reported_bar_fixed_horizon_source_census/"
    f"{REJECTED_REPORT_ID}/source_census.json"
)
REJECTED_REPORT_SHA256 = "59828d557bd993aa96f5d20b3e1441b9dd262b5c07e5652d7d3aa45c588a90d9"
PREDECESSOR_PROTOCOL_PATH = Path("configs/reported_bar_fixed_horizon_source_discovery_protocol_v2.json")
PREDECESSOR_PROTOCOL_ID = "29f3384c814a967c2e69c1433e62a9322e37bc1b3596e43334b05c698987321a"
PREDECESSOR_PROTOCOL_SHA256 = "f2d6b5bffe65a46827cadfc45dfbdcebe37131cd4ba7cc7bcabe78f1b4585b47"
TOPOLOGY_ROOT = Path("state/unpublished_evidence/reported_bar_trade_triggered_source_topology")
REJECTION_ROOT = Path("state/unpublished_evidence/reported_bar_fixed_time_entry_rejection")
INVALID_PREPARATION_ROOT = Path(
    "state/unpublished_evidence/reported_bar_trade_triggered_invalid_preparation"
)
ORIGINAL_PROTOCOL_PATH = Path("configs/reported_bar_trade_triggered_source_discovery_protocol.json")
ORIGINAL_PROTOCOL_ID = "e8b86ef7fed1251425b358e73f1608aa5cd401b67f72e7172a13997d33fe6bb4"
ORIGINAL_PROTOCOL_SHA256 = "3215cfb7285de6f5441e27745d49e26f7daf30473cb93f2bb502d099dd5339d1"
PROTOCOL_PATH = Path("configs/reported_bar_trade_triggered_source_discovery_protocol_v2.json")
CHECKPOINTS = ("09:00", "09:30", "10:00", "10:30")


def _read_canonical(path: Path, *, name: str) -> dict[str, object]:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"{name} is invalid") from exc
    if not isinstance(payload, dict):
        raise IntegrityError(f"{name} is invalid")
    return payload


def _rejected_report(root: Path) -> dict[str, object]:
    path = root / REJECTED_REPORT_PATH
    if sha256_file(path) != REJECTED_REPORT_SHA256:
        raise IntegrityError("fixed-time source report drifted")
    report = _read_canonical(path, name="fixed-time source report")
    core = {key: value for key, value in report.items() if key != "report_id"}
    if report.get("report_id") != REJECTED_REPORT_ID or sha256_json(core) != REJECTED_REPORT_ID:
        raise IntegrityError("fixed-time source report identity is invalid")
    return report


def build_topology(*, root: Path) -> dict[str, object]:
    report = _rejected_report(root)
    results = report.get("market_checkpoint_results")
    if not isinstance(results, list) or len(results) != 164:
        raise IntegrityError("fixed-time report topology is incomplete")
    gate_counts: Counter[str] = Counter()
    dispositions: Counter[str] = Counter()
    baseline_only: list[dict[str, str]] = []
    for item in results:
        if not isinstance(item, dict):
            raise IntegrityError("fixed-time result is invalid")
        gates = [str(value) for value in item.get("failed_gates", [])]
        gate_counts.update(gates)
        non_baseline = [value for value in gates if "ALWAYS_DIRECTION_BASELINE" not in value]
        if gates and not non_baseline:
            baseline_only.append({"market": str(item["market"]), "checkpoint": str(item["checkpoint"])})
        overall = item.get("overall")
        if isinstance(overall, dict) and isinstance(overall.get("dispositions"), dict):
            dispositions.update({str(key): int(value) for key, value in overall["dispositions"].items()})
    core: dict[str, object] = {
        "schema_version": "reported_bar_trade_triggered_source_topology/1.0.0",
        "classification": "SEALED_EVIDENCE_ONLY_NO_ROW_REREAD_NO_ECONOMICS",
        "rejected_report_id": REJECTED_REPORT_ID,
        "rejected_report_path": REJECTED_REPORT_PATH.as_posix(),
        "rejected_report_sha256": REJECTED_REPORT_SHA256,
        "market_checkpoint_results": len(results),
        "passing_market_checkpoints": sum(item.get("status") == "PASS" for item in results),
        "failed_gate_counts": dict(sorted(gate_counts.items())),
        "baseline_only_failed_cells": sorted(
            baseline_only, key=lambda item: (item["checkpoint"], item["market"])
        ),
        "baseline_only_failed_cell_count": len(baseline_only),
        "candidate_disposition_counts": dict(sorted(dispositions.items())),
        "diagnosis": {
            "fixed_time_market_order_fill_unobservable_from_reported_bars": True,
            "reported_bar_may_trigger_only_after_available_at": True,
            "trigger_bar_retroactive_fill_forbidden": True,
            "economic_outcome_examined": False,
        },
    }
    return {**core, "topology_id": sha256_json(core)}


def topology_path(topology: Mapping[str, object]) -> Path:
    return TOPOLOGY_ROOT / str(topology["topology_id"]) / "topology.json"


def build_rejection(*, root: Path, topology: Mapping[str, object]) -> dict[str, object]:
    if topology.get("passing_market_checkpoints") != 0:
        raise IntegrityError("fixed-time source rejection is not conclusive")
    core: dict[str, object] = {
        "schema_version": "reported_bar_fixed_time_entry_rejection/1.0.0",
        "classification": "CONCLUSIVE_PRE_REGISTRATION_SOURCE_INCOMPATIBILITY",
        "rejected_protocol_id": PREDECESSOR_PROTOCOL_ID,
        "rejected_protocol_path": PREDECESSOR_PROTOCOL_PATH.as_posix(),
        "rejected_protocol_sha256": PREDECESSOR_PROTOCOL_SHA256,
        "rejected_report_id": REJECTED_REPORT_ID,
        "rejected_report_path": REJECTED_REPORT_PATH.as_posix(),
        "rejected_report_sha256": REJECTED_REPORT_SHA256,
        "topology_id": topology["topology_id"],
        "topology_path": topology_path(topology).as_posix(),
        "reason": "FIXED_TIME_MARKET_ORDER_FILL_CANNOT_BE_PROVEN_FROM_STANDARD_REPORTED_BARS",
        "economic_result": "NOT_PRODUCED",
        "trial_registered": False,
        "retry_or_rescue_allowed": False,
        "coverage_gates_weakened": False,
        "preservation": "ALL_PREDECESSOR_BYTES_REMAIN_UNCHANGED",
    }
    return {**core, "rejection_id": sha256_json(core)}


def rejection_path(rejection: Mapping[str, object]) -> Path:
    return REJECTION_ROOT / str(rejection["rejection_id"]) / "rejection.json"


def build_invalid_preparation(*, root: Path) -> dict[str, object]:
    original_path = root / ORIGINAL_PROTOCOL_PATH
    if sha256_file(original_path) != ORIGINAL_PROTOCOL_SHA256:
        raise IntegrityError("original trade-triggered preparation drifted")
    original = _read_canonical(original_path, name="original trade-triggered preparation")
    if original.get("protocol_id") != ORIGINAL_PROTOCOL_ID:
        raise IntegrityError("original trade-triggered preparation identity is invalid")
    core: dict[str, object] = {
        "schema_version": "reported_bar_trade_triggered_invalid_preparation/1.0.0",
        "classification": "UNPUBLISHED_PRE_DATA_INVALID_PREPARATION",
        "protocol_id": ORIGINAL_PROTOCOL_ID,
        "protocol_path": ORIGINAL_PROTOCOL_PATH.as_posix(),
        "protocol_sha256": ORIGINAL_PROTOCOL_SHA256,
        "reason": "ENTRY_AND_EXIT_TIMEOUTS_WERE_BOUNDED_BY_EVENT_TIME_NOT_EVIDENCE_AVAILABILITY",
        "economic_result": "NOT_PRODUCED",
        "historical_rows_read": False,
        "published_or_registered": False,
        "preservation": "ORIGINAL_BYTES_REMAIN_UNCHANGED",
    }
    return {**core, "invalid_preparation_id": sha256_json(core)}


def invalid_preparation_path(invalid: Mapping[str, object]) -> Path:
    return INVALID_PREPARATION_ROOT / str(invalid["invalid_preparation_id"]) / "invalid_preparation.json"


def build_protocol(
    *,
    root: Path,
    topology: Mapping[str, object],
    rejection: Mapping[str, object],
    invalid_preparation: Mapping[str, object],
) -> dict[str, object]:
    predecessor = _read_canonical(root / PREDECESSOR_PROTOCOL_PATH, name="fixed-time protocol")
    if (
        sha256_file(root / PREDECESSOR_PROTOCOL_PATH) != PREDECESSOR_PROTOCOL_SHA256
        or predecessor.get("protocol_id") != PREDECESSOR_PROTOCOL_ID
    ):
        raise IntegrityError("fixed-time predecessor protocol drifted")
    core: dict[str, object] = {
        "schema_version": "reported_bar_trade_triggered_source_discovery_protocol/1.0.1",
        "state": "PREPARED_NOT_EXECUTED_UNREGISTERED_UNPUBLISHED",
        "classification": "PRE_REGISTRATION_SOURCE_ONLY_NO_ECONOMICS",
        "mechanism": "SINGLE_CHECKPOINT_TRADE_TRIGGERED_REPORTED_BAR_FIXED_HORIZON",
        "years": predecessor["years"],
        "market_universe": predecessor["market_universe"],
        "checkpoint_grid": predecessor["checkpoint_grid"],
        "checkpoint_configurations": predecessor["checkpoint_configurations"],
        "feature_rule": predecessor["decision_rule"],
        "entry_lifecycle": {
            "decision_clock": "CHECKPOINT_PLUS_5_SECONDS_AMERICA_CHICAGO",
            "trigger_observation_timeout_seconds": 120,
            "trigger": "FIRST_REPORTED_BAR_AVAILABLE_AFTER_DECISION_AND_NO_LATER_THAN_DECISION_PLUS_120_SECONDS",
            "no_trigger_by_timeout": "EXPLICIT_CAUSAL_NO_TRADE_TIMEOUT",
            "trigger_price_used_as_fill": False,
            "order_time": "TRIGGER_AVAILABLE_AT",
            "fill": "FIRST_LATER_REPORTED_BAR_EVENT_STRICTLY_AFTER_ORDER_TIME_AND_WITHIN_2_MINUTES",
            "trigger_without_later_fill": "MANDATORY_ENTRY_PATH_FAILURE",
            "same_actual_contract_identity": True,
        },
        "exit_lifecycle": {
            "exit_order_time": "ENTRY_FILL_EVENT_PLUS_30_MINUTES",
            "fill": "FIRST_REPORTED_BAR_EVENT_AT_OR_AFTER_EXIT_ORDER_TIME_AND_WITHIN_2_MINUTES",
            "missing_exit_after_entry": "MANDATORY_EXIT_PATH_FAILURE",
            "same_actual_contract_identity": True,
            "intermediate_every_minute_path_required": False,
            "stop_or_target_monitoring": False,
        },
        "coverage_gates": {
            **predecessor["coverage_gates"],
            "trigger_timeout_checkpoint_accounting_percent": 100,
            "triggered_order_entry_and_exit_path_percent": 100,
            "no_trigger_timeout_excluded_from_path_denominator": True,
            "triggered_order_or_fill_never_dropped": True,
        },
        "folds": predecessor["folds"],
        "baseline_requirements": {
            "flat_no_trade": "EXACT_ZERO_NO_PATH",
            "active_baselines": predecessor["baseline_requirements"]["active_baselines"],
            "each_baseline_uses_its_own_trigger_order_fill_exit_and_timeout_state": True,
            "candidate_schedule_or_trigger_reuse_forbidden": True,
            "no_trigger_timeout_is_no_trade_not_unobservable_fill": True,
        },
        "source_resolution": predecessor["source_resolution"],
        "source_only_selection": predecessor["source_only_selection"],
        "execution_limits": predecessor["execution_limits"],
        "authority": predecessor["authority"],
        "supersedes_rejected_protocol_id": PREDECESSOR_PROTOCOL_ID,
        "supersedes_invalid_preparation_protocol_id": ORIGINAL_PROTOCOL_ID,
        "invalid_preparation_id": invalid_preparation["invalid_preparation_id"],
        "source_rejection_id": rejection["rejection_id"],
        "bindings": {
            REJECTED_REPORT_PATH.as_posix(): REJECTED_REPORT_SHA256,
            PREDECESSOR_PROTOCOL_PATH.as_posix(): PREDECESSOR_PROTOCOL_SHA256,
            topology_path(topology).as_posix(): sha256_file(root / topology_path(topology)),
            rejection_path(rejection).as_posix(): sha256_file(root / rejection_path(rejection)),
            invalid_preparation_path(invalid_preparation).as_posix(): sha256_file(
                root / invalid_preparation_path(invalid_preparation)
            ),
            "configs/active_cash_open_impulse_historical_calendar.json": sha256_file(
                root / "configs/active_cash_open_impulse_historical_calendar.json"
            ),
            "data/active/catalog.json": sha256_file(root / "data/active/catalog.json"),
            "src/futures_rebuild/active_data_view.py": sha256_file(
                root / "src/futures_rebuild/active_data_view.py"
            ),
            "src/futures_rebuild/reported_bar_trade_triggered_protocol.py": sha256_file(Path(__file__)),
        },
    }
    return {**core, "protocol_id": sha256_json(core)}


@dataclass(frozen=True)
class TriggeredDisposition:
    feature_complete: bool
    trigger_observed: bool
    order_placed: bool
    entry_fill_complete: bool
    exit_fill_complete: bool
    path_required: bool
    disposition: str


def _feature_complete(
    *, checkpoint: datetime, rows: Sequence[ReportedBarEvidence]
) -> tuple[bool, str | None]:
    decision = checkpoint + timedelta(seconds=5)
    start = checkpoint - timedelta(minutes=30)
    feature = sorted(
        {
            item.event_at: item for item in rows
            if start <= item.event_at < checkpoint and item.available_at <= decision
        }.values(),
        key=lambda item: item.event_at,
    )
    identities = {item.actual_identity_hash for item in feature}
    if (
        len(feature) < 15
        or feature[0].event_at > start + timedelta(minutes=5)
        or feature[-1].event_at < checkpoint - timedelta(minutes=2)
        or len(identities) != 1
        or None in identities
    ):
        return False, None
    return True, feature[-1].actual_identity_hash


def classify_trade_triggered_checkpoint(
    *, checkpoint: datetime, rows: Sequence[ReportedBarEvidence], feature_required: bool
) -> TriggeredDisposition:
    if checkpoint.tzinfo is None:
        raise IntegrityError("trade-triggered checkpoint must be timezone aware")
    decision = checkpoint + timedelta(seconds=5)
    feature_complete, feature_identity = _feature_complete(checkpoint=checkpoint, rows=rows)
    if feature_required and not feature_complete:
        return TriggeredDisposition(
            False, False, False, False, False, False,
            "EXPLICIT_CAUSAL_FEATURE_ABSTENTION",
        )
    triggers = sorted(
        (
            item for item in rows
            if item.event_at >= checkpoint
            and decision < item.available_at <= decision + timedelta(seconds=120)
        ),
        key=lambda item: (item.available_at, item.event_at),
    )
    if not triggers:
        return TriggeredDisposition(
            feature_complete, False, False, False, False, False,
            "EXPLICIT_CAUSAL_NO_TRADE_TIMEOUT",
        )
    trigger = triggers[0]
    if trigger.actual_identity_hash is None or (
        feature_required and trigger.actual_identity_hash != feature_identity
    ):
        return TriggeredDisposition(
            feature_complete, True, False, False, False, False,
            "TRIGGER_IDENTITY_INVALID",
        )
    order_time = trigger.available_at
    fills = sorted(
        (
            item for item in rows
            if order_time < item.event_at <= order_time + timedelta(minutes=2)
            and order_time < item.available_at <= order_time + timedelta(minutes=2)
        ),
        key=lambda item: (item.available_at, item.event_at),
    )
    if not fills:
        return TriggeredDisposition(
            feature_complete, True, True, False, False, True,
            "TRIGGERED_ORDER_ENTRY_FILL_INCOMPLETE",
        )
    entry = fills[0]
    if entry.actual_identity_hash != trigger.actual_identity_hash:
        return TriggeredDisposition(
            feature_complete, True, True, False, False, True,
            "ENTRY_IDENTITY_CHANGING",
        )
    exit_order_time = entry.event_at + timedelta(minutes=30)
    exits = sorted(
        (
            item for item in rows
            if exit_order_time <= item.event_at <= exit_order_time + timedelta(minutes=2)
            and exit_order_time <= item.available_at <= exit_order_time + timedelta(minutes=2)
        ),
        key=lambda item: (item.available_at, item.event_at),
    )
    if not exits:
        return TriggeredDisposition(
            feature_complete, True, True, True, False, True,
            "TRIGGERED_ORDER_EXIT_FILL_INCOMPLETE",
        )
    if exits[0].actual_identity_hash != entry.actual_identity_hash:
        return TriggeredDisposition(
            feature_complete, True, True, True, False, True,
            "EXIT_IDENTITY_CHANGING",
        )
    return TriggeredDisposition(
        feature_complete, True, True, True, True, True, "COMPLETE"
    )
