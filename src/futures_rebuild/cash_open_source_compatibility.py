"""Price-free mechanics for the 41-market cash-open source census."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .active_data_view import resolve as resolve_active_view
from .active_data_view import validate_catalog
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .tier1_bracket_v5 import TRADABLE_DISPOSITIONS


CHICAGO = ZoneInfo("America/Chicago")
YEARS = tuple(range(2018, 2023))
CHECKPOINT_GRID = ("09:00", "09:30", "10:00", "10:30")
PRIMARY_CONFIG = ("09:00", "10:30")
FALLBACK_PAIRS = (("09:00", "10:00"), ("09:30", "10:30"))
SINGLE_CONFIGS = tuple((item,) for item in CHECKPOINT_GRID)
FEATURE_MINUTES = 30
EXECUTION_MINUTES = 31
INITIAL_TRAINING_SESSIONS = 504
EVALUATION_SESSIONS = 63
OUTER_FOLDS = 8
EMBARGO_SESSIONS = 1
PURGE_MINUTES = 31
MINIMUM_USEFUL_MARKETS = 2
REJECTED_PROTOCOL_ID = "3b8e09d65015afd33fc033aa72c8bb0be22425cafac8b8b145eeccb639258067"
REJECTED_PROTOCOL_PATH = Path("configs/cash_open_impulse_pre_registration_protocol.json")
REJECTED_PROTOCOL_SHA256 = "8d9dbb4bc4355dc9fd753ebde8cce11086b6c7027677c168cd1c000eebeb8c30"
REJECTED_REPORT_PATH = Path(
    "state/unpublished_evidence/cash_open_impulse_fold_readiness_v2/"
    f"{REJECTED_PROTOCOL_ID}/fold_readiness_certificate.json"
)
REJECTED_REPORT_SHA256 = "57b6d368d3cfcaaFF98a247b8d8bd12cc01e52d423a439e887aaf6e6278a8570".lower()
ACTIVE_CATALOG_PATH = Path("data/active/catalog.json")


@dataclass(frozen=True)
class SourceRow:
    market: str
    session: str
    event_at_ns: int
    available_at_ns: int
    executable: bool
    actual_identity_hash: str | None
    source_row_sha256: str


@dataclass(frozen=True)
class CheckpointReadiness:
    checkpoint: str
    decision_available: bool
    entry_after_decision: bool
    feature_complete: bool
    execution_complete: bool
    failure: str | None

    @property
    def complete(self) -> bool:
        return (
            self.decision_available
            and self.entry_after_decision
            and self.feature_complete
            and self.execution_complete
            and self.failure is None
        )


def _hex64(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(
        character in "0123456789abcdef" for character in value
    )


def source_row_from_mapping(*, market: str, row: Mapping[str, object]) -> SourceRow:
    """Normalize only causal identity fields; never read price values."""

    session = row.get("exchange_session_date")
    event = row.get("event_at_ns")
    source_hash = row.get("source_row_sha256")
    disposition = row.get("disposition")
    if (
        not isinstance(session, str)
        or type(event) is not int
        or not _hex64(source_hash)
        or session.startswith("2025")
    ):
        raise IntegrityError("source-only row lacks a bounded causal identity")
    try:
        parsed = date.fromisoformat(session)
    except ValueError as exc:
        raise IntegrityError("source-only row session is invalid") from exc
    if parsed.year not in YEARS:
        raise UnauthorizedOperation("source-only row leaves 2018-2022")
    executable = disposition in TRADABLE_DISPOSITIONS
    identity = row.get("actual_identity_hash")
    if executable and not _hex64(identity):
        raise IntegrityError("executable source-only row lacks contract identity")
    return SourceRow(
        market=market,
        session=session,
        event_at_ns=event,
        available_at_ns=event + 65_000_000_000,
        executable=executable,
        actual_identity_hash=str(identity) if _hex64(identity) else None,
        source_row_sha256=str(source_hash),
    )


def _clock(event_at_ns: int) -> time:
    seconds, remainder = divmod(event_at_ns, 1_000_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc).astimezone(CHICAGO).time().replace(
        microsecond=remainder // 1_000
    )


def _required_clocks(checkpoint: str) -> tuple[tuple[time, ...], tuple[time, ...]]:
    center = time.fromisoformat(checkpoint)
    minute = center.hour * 60 + center.minute
    feature = tuple(time(*divmod(minute - offset, 60)) for offset in range(30, 0, -1))
    execution = tuple(time(*divmod(minute + offset, 60)) for offset in range(1, 32))
    return feature, execution


def classify_checkpoint(
    *, market: str, session: str, checkpoint: str, rows: Sequence[SourceRow]
) -> CheckpointReadiness:
    if checkpoint not in CHECKPOINT_GRID or session.startswith("2025"):
        raise UnauthorizedOperation("checkpoint classification leaves its frozen scope")
    executable = [item for item in rows if item.executable]
    grouped: dict[time, list[SourceRow]] = {}
    for item in executable:
        grouped.setdefault(_clock(item.event_at_ns), []).append(item)
    feature_clocks, execution_clocks = _required_clocks(checkpoint)
    if any(len(grouped.get(clock, ())) != 1 for clock in feature_clocks):
        return CheckpointReadiness(
            checkpoint, False, False, False, False,
            "DECISION_UNAVAILABLE_DUE_TO_FEATURE_GAP",
        )
    feature = [grouped[clock][0] for clock in feature_clocks]
    decision_ns = feature[-1].event_at_ns + 65_000_000_000
    feature_identity = {item.actual_identity_hash for item in feature}
    if (
        any(item.available_at_ns > decision_ns for item in feature)
        or len(feature_identity) != 1
        or None in feature_identity
    ):
        return CheckpointReadiness(
            checkpoint, False, False, False, False,
            "DECISION_UNAVAILABLE_DUE_TO_FEATURE_GAP",
        )
    if any(len(grouped.get(clock, ())) != 1 for clock in execution_clocks):
        return CheckpointReadiness(
            checkpoint, True, False, True, False,
            "EXECUTION_PATH_INCOMPLETE_OR_IDENTITY_CHANGING",
        )
    execution = [grouped[clock][0] for clock in execution_clocks]
    if execution[0].event_at_ns <= decision_ns:
        return CheckpointReadiness(
            checkpoint, True, False, True, False, "ENTRY_NOT_AFTER_DECISION"
        )
    combined_identity = {
        item.actual_identity_hash for item in (*feature, *execution)
    }
    if len(combined_identity) != 1 or None in combined_identity:
        return CheckpointReadiness(
            checkpoint, True, True, True, False,
            "EXECUTION_PATH_INCOMPLETE_OR_IDENTITY_CHANGING",
        )
    return CheckpointReadiness(checkpoint, True, True, True, True, None)


def build_market_calendar_folds(eligible_sessions: Sequence[str]) -> tuple[dict[str, object], ...]:
    """Build fold membership from calendar eligibility before source coverage."""

    sessions = tuple(eligible_sessions)
    if sessions != tuple(sorted(set(sessions))):
        raise IntegrityError("mechanism-eligible sessions are not unique and chronological")
    required = INITIAL_TRAINING_SESSIONS + (OUTER_FOLDS - 1) * EVALUATION_SESSIONS + EMBARGO_SESSIONS + EVALUATION_SESSIONS
    if len(sessions) < required:
        raise IntegrityError("mechanism-eligible calendar cannot support eight folds")
    folds: list[dict[str, object]] = []
    for index in range(OUTER_FOLDS):
        fit_count = INITIAL_TRAINING_SESSIONS + index * EVALUATION_SESSIONS
        training = sessions[:fit_count]
        embargo = sessions[fit_count : fit_count + EMBARGO_SESSIONS]
        evaluation = sessions[
            fit_count + EMBARGO_SESSIONS : fit_count + EMBARGO_SESSIONS + EVALUATION_SESSIONS
        ]
        folds.append(
            {
                "fold_id": f"fold-{index}",
                "training_sessions": list(training),
                "embargo_sessions": list(embargo),
                "evaluation_sessions": list(evaluation),
                "purge_minutes": PURGE_MINUTES,
            }
        )
    return tuple(folds)


def certify_market_configuration(
    *,
    market: str,
    checkpoints: Sequence[str],
    eligible_sessions: Sequence[str],
    rows_by_session: Mapping[str, Sequence[SourceRow]],
    catalog_complete: bool,
    catalog_failures: Sequence[str] = (),
) -> dict[str, object]:
    """Require 100% exact paths in every calendar-selected fold cell."""

    config = tuple(checkpoints)
    if not config or any(item not in CHECKPOINT_GRID for item in config):
        raise IntegrityError("source compatibility configuration is invalid")
    try:
        folds = build_market_calendar_folds(eligible_sessions)
    except IntegrityError as exc:
        return {
            "market": market,
            "checkpoints": list(config),
            "status": "FAIL",
            "failed_gates": ["MECHANISM_ELIGIBLE_CALENDAR_FOLDS"],
            "catalog_failures": list(catalog_failures),
            "fold_results": [],
            "reason": str(exc),
        }
    fold_results: list[dict[str, object]] = []
    all_complete = catalog_complete
    for fold in folds:
        exclusions: dict[str, int] = {}
        year_counts: dict[str, dict[str, int]] = {}
        role_counts: dict[str, dict[str, int]] = {}
        candidate_expected_paths = candidate_complete_paths = 0
        baseline_expected_paths = baseline_complete_paths = 0
        for role, key in (("TRAINING", "training_sessions"), ("EVALUATION", "evaluation_sessions")):
            sessions = tuple(str(item) for item in fold[key])
            complete = 0
            for session in sessions:
                session_rows = rows_by_session.get(session, ())
                outcomes = [
                    classify_checkpoint(
                        market=market, session=session, checkpoint=checkpoint,
                        rows=session_rows,
                    )
                    for checkpoint in config
                ]
                candidate_expected_paths += len(outcomes)
                candidate_complete_paths += sum(item.complete for item in outcomes)
                baseline_expected_paths += 1
                baseline_complete_paths += int(outcomes[0].complete)
                session_complete = all(item.complete for item in outcomes)
                complete += int(session_complete)
                year = session[:4]
                bucket = year_counts.setdefault(
                    year, {"expected": 0, "complete": 0, "failed": 0}
                )
                bucket["expected"] += 1
                bucket["complete"] += int(session_complete)
                bucket["failed"] += int(not session_complete)
                for outcome in outcomes:
                    if not outcome.complete:
                        reason = str(outcome.failure or "UNCLASSIFIED_SOURCE_GAP")
                        name = f"{role}__{outcome.checkpoint}__{reason}"
                        exclusions[name] = exclusions.get(name, 0) + 1
            role_counts[role.lower()] = {
                "expected_sessions": len(sessions),
                "complete_sessions": complete,
            }
            all_complete = all_complete and complete == len(sessions)
        fold_results.append(
            {
                "fold_id": fold["fold_id"],
                "training": role_counts["training"],
                "evaluation": role_counts["evaluation"],
                "embargo_sessions": len(fold["embargo_sessions"]),
                "purge_minutes": fold["purge_minutes"],
                "market_year_counts": dict(sorted(year_counts.items())),
                "exclusion_reasons": dict(sorted(exclusions.items())),
                "candidate_path_coverage": {
                    "expected": candidate_expected_paths,
                    "complete": candidate_complete_paths,
                    "percent": 100 * candidate_complete_paths / candidate_expected_paths,
                },
                "active_baseline_path_coverage": {
                    "expected": baseline_expected_paths,
                    "complete": baseline_complete_paths,
                    "percent": 100 * baseline_complete_paths / baseline_expected_paths,
                },
                "flat_no_trade_path_required": False,
                "status": "PASS" if not exclusions else "FAIL",
            }
        )
    failed_gates: list[str] = []
    if not catalog_complete:
        failed_gates.append("ACTIVE_CATALOG_COMPLETE_2018_2022")
    if any(item["status"] != "PASS" for item in fold_results):
        failed_gates.append("ONE_HUNDRED_PERCENT_CAUSAL_PATH_COVERAGE")
    return {
        "market": market,
        "checkpoints": list(config),
        "status": "PASS" if all_complete and not failed_gates else "FAIL",
        "failed_gates": failed_gates,
        "catalog_failures": list(catalog_failures),
        "fold_results": fold_results,
        "baseline_universe": {
            "flat_no_trade": "NO_PATH_EXACT_ZERO",
            "always_long_first_checkpoint": "INDEPENDENT_FIRST_CHECKPOINT_PATH",
            "always_short_first_checkpoint": "INDEPENDENT_FIRST_CHECKPOINT_PATH",
            "opening_impulse_continuation_first_checkpoint": "INDEPENDENT_FIRST_CHECKPOINT_PATH",
            "opening_impulse_reversal_first_checkpoint": "INDEPENDENT_FIRST_CHECKPOINT_PATH",
        },
    }


def select_compatible_market_set(
    results: Mapping[tuple[str, ...], Sequence[str]],
) -> dict[str, object]:
    """Apply the preregistered coverage-only configuration selection rule."""

    normalized = {
        tuple(config): tuple(sorted(set(markets))) for config, markets in results.items()
    }
    primary = normalized.get(PRIMARY_CONFIG, ())
    if len(primary) >= MINIMUM_USEFUL_MARKETS:
        return {
            "decision": "PASS_SOURCE_COMPATIBLE_MARKET_SET",
            "selected_checkpoints": list(PRIMARY_CONFIG),
            "selected_markets": list(primary),
            "selection_stage": "PRIMARY",
        }
    pair_candidates = [
        (len(normalized.get(config, ())), config, normalized.get(config, ()))
        for config in FALLBACK_PAIRS
    ]
    passing_pairs = [item for item in pair_candidates if item[0] >= MINIMUM_USEFUL_MARKETS]
    if passing_pairs:
        _, config, markets = sorted(passing_pairs, key=lambda item: (-item[0], item[1]))[0]
        return {
            "decision": "PASS_SOURCE_COMPATIBLE_MARKET_SET",
            "selected_checkpoints": list(config),
            "selected_markets": list(markets),
            "selection_stage": "FALLBACK_PAIR",
        }
    single_candidates = [
        (len(normalized.get(config, ())), config, normalized.get(config, ()))
        for config in SINGLE_CONFIGS
    ]
    passing_singles = [item for item in single_candidates if item[0] >= MINIMUM_USEFUL_MARKETS]
    if passing_singles:
        _, config, markets = sorted(passing_singles, key=lambda item: (-item[0], item[1]))[0]
        return {
            "decision": "PASS_SOURCE_COMPATIBLE_MARKET_SET",
            "selected_checkpoints": list(config),
            "selected_markets": list(markets),
            "selection_stage": "FALLBACK_SINGLE",
        }
    return {
        "decision": "REJECTED_NO_USEFUL_MULTI_MARKET_SOURCE_COMPATIBILITY",
        "selected_checkpoints": [],
        "selected_markets": [],
        "selection_stage": "REJECTED",
    }


def resolve_catalog_source(*, root: Path, market: str, year: int) -> Path:
    """The sole current source resolver for the authorized census."""

    if year not in YEARS:
        raise UnauthorizedOperation("source resolution leaves 2018-2022")
    return resolve_active_view(
        repository_root=root,
        market=market,
        year=year,
        purpose="SELECTION",
        require_status=False,
    )


def build_rejected_protocol_closure(*, root: Path) -> dict[str, object]:
    if sha256_file(root / REJECTED_PROTOCOL_PATH) != REJECTED_PROTOCOL_SHA256:
        raise IntegrityError("rejected four-market protocol drifted")
    if sha256_file(root / REJECTED_REPORT_PATH) != REJECTED_REPORT_SHA256:
        raise IntegrityError("rejected four-market readiness report drifted")
    core: dict[str, object] = {
        "schema_version": "cash_open_impulse_pre_registration_rejection/1.0.0",
        "protocol_id": REJECTED_PROTOCOL_ID,
        "classification": "PRE_REGISTRATION_SOURCE_COMPATIBILITY_REJECTION",
        "economic_result": "NOT_PRODUCED",
        "historical_rows_used_for": "SOURCE_READINESS_ONLY",
        "registration_allowed": False,
        "execution_allowed": False,
        "incremental_rescue_allowed": False,
        "bindings": {
            REJECTED_PROTOCOL_PATH.as_posix(): REJECTED_PROTOCOL_SHA256,
            REJECTED_REPORT_PATH.as_posix(): REJECTED_REPORT_SHA256,
        },
        "authority": {
            "published": False,
            "historical_rows_read": False,
            "model_fit": False,
            "prediction_generation": False,
            "performance_evaluation": False,
            "year_2025_accessed": False,
        },
    }
    return {**core, "record_id": sha256_json(core)}


def catalog_inventory(*, root: Path) -> dict[str, object]:
    raw = json.loads((root / ACTIVE_CATALOG_PATH).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise IntegrityError("active catalog is invalid")
    validate_catalog(raw)
    entries = raw["entries"]
    assert isinstance(entries, list)
    markets = sorted({str(item["market"]) for item in entries if isinstance(item, dict)})
    expected = {(market, year) for market in markets for year in YEARS}
    observed = {
        (str(item["market"]), int(item["year"])): item
        for item in entries
        if isinstance(item, dict) and int(item["year"]) in YEARS
    }
    dispositions: list[dict[str, object]] = []
    for market, year in sorted(expected):
        item = observed.get((market, year))
        if item is None:
            dispositions.append({"market": market, "year": year, "disposition": "ABSENT_FROM_ACTIVE_CATALOG"})
        else:
            dispositions.append(
                {"market": market, "year": year, "disposition": item["disposition"]}
            )
    return {
        "market_count": len(markets),
        "markets": markets,
        "expected_market_years": len(expected),
        "market_year_dispositions": dispositions,
    }


def build_predata_spec(
    *, root: Path, grid_calendar_path: Path, grid_calendar_sha256: str
) -> dict[str, object]:
    calendar = json.loads((root / grid_calendar_path).read_text(encoding="utf-8"))
    if sha256_file(root / grid_calendar_path) != grid_calendar_sha256:
        raise IntegrityError("prepared grid calendar drifted")
    if calendar.get("decision") != "PASS_EXACT_REFERENCE_COVERAGE":
        raise IntegrityError("prepared grid calendar did not pass")
    inventory = catalog_inventory(root=root)
    if inventory["market_count"] != 41 or inventory["expected_market_years"] != 205:
        raise IntegrityError("active catalog does not expose the exact 41-market topology")
    core: dict[str, Any] = {
        "schema_version": "cash_open_41_market_source_compatibility_spec/1.0.0",
        "state": "PREPARED_BLOCKED_CALENDAR_ACTIVATION_REQUIRED",
        "classification": "PRE_REGISTRATION_SOURCE_ONLY_NO_ECONOMICS",
        "years": list(YEARS),
        "markets": inventory["markets"],
        "checkpoint_grid": list(CHECKPOINT_GRID),
        "primary_configuration": list(PRIMARY_CONFIG),
        "fallback_pair_configurations": [list(item) for item in FALLBACK_PAIRS],
        "fallback_single_configurations": [list(item) for item in SINGLE_CONFIGS],
        "selection_rule": "PRIMARY_THEN_MAX_PASSING_FALLBACK_COUNT_TIE_EARLIEST_INCLUDE_ALL_PASSING_REQUIRE_TWO",
        "source_requirements": {
            "feature_minutes": FEATURE_MINUTES,
            "execution_minutes": EXECUTION_MINUTES,
            "availability_latency_seconds": 65,
            "decision_offset_seconds": 5,
            "same_actual_contract_identity": True,
            "candidate_path_coverage_percent": 100,
            "active_baseline_path_coverage_percent": 100,
            "future_incomplete_paths_are_explicit_failures": True,
        },
        "fold_requirements": {
            "outer_folds": OUTER_FOLDS,
            "initial_training_sessions": INITIAL_TRAINING_SESSIONS,
            "evaluation_sessions": EVALUATION_SESSIONS,
            "embargo_sessions": EMBARGO_SESSIONS,
            "purge_minutes": PURGE_MINUTES,
            "construction": "MARKET_MECHANISM_ELIGIBLE_CALENDAR_BEFORE_SOURCE_COMPLETENESS",
        },
        "catalog_inventory": inventory,
        "execution_limits": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_workers": 4,
            "worker_deadline_seconds": 3300,
            "maximum_runtime_seconds": 3600,
            "maximum_external_cost_usd": "0",
        },
        "prepared_calendar": {
            "path": grid_calendar_path.as_posix(),
            "sha256": grid_calendar_sha256,
            "calendar_id": calendar["calendar_id"],
            "active": False,
        },
        "source_resolution": {
            "resolver": "futures_rebuild.active_data_view.resolve",
            "purpose": "SELECTION",
            "direct_paths_allowed": False,
            "globbing_allowed": False,
            "fallback_allowed": False,
        },
        "authority": {
            "historical_row_read": False,
            "model_fit": False,
            "prediction_generation": False,
            "performance_evaluation": False,
            "registration": False,
            "publication": False,
            "provider_network_credentials": False,
            "year_2025_access": False,
            "trading": False,
        },
        "bindings": {
            ACTIVE_CATALOG_PATH.as_posix(): sha256_file(root / ACTIVE_CATALOG_PATH),
            grid_calendar_path.as_posix(): grid_calendar_sha256,
            "src/futures_rebuild/active_data_view.py": sha256_file(root / "src/futures_rebuild/active_data_view.py"),
            "src/futures_rebuild/cash_open_source_compatibility.py": sha256_file(Path(__file__)),
            "src/futures_rebuild/research_gateway_policy.py": sha256_file(root / "src/futures_rebuild/research_gateway_policy.py"),
        },
    }
    return {**core, "spec_id": sha256_json(core)}
