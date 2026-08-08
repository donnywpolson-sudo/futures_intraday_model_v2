"""Row-readiness census for the counted reported-trade-exit Alpha mechanism."""

from __future__ import annotations

import hashlib
import multiprocessing
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import timedelta
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from time import monotonic

from .active_data_view import resolve
from .alpha_ladder_combined_readiness import (
    ACTIVE_CATALOG_PATH,
    CHECKPOINT,
    CORE,
    MANDATORY_BASELINES,
    SCENARIOS,
    YEARS,
    _active_calendar,
    _read_canonical,
    _write_once,
)
from .alpha_ladder_combined_readiness_v3 import (
    EMBARGO_SESSIONS,
    EVALUATION_SESSIONS,
    OUTER_FOLDS,
    PURGE_MINUTES,
    TRAINING_SESSIONS,
    _outer_folds,
    select_earliest_executable_pilot,
    validate_selection,
)
from .alpha_ladder_frozen_mechanism import MANDATORY_BASELINES as FROZEN_BASELINES
from .alpha_ladder_limit_readiness import (
    LimitBar,
    SessionReadiness,
    _feature_and_risk,
    _penetrates,
    _read_market,
)
from .alpha_ladder_reported_trade_exit_successor import classify_reported_trade_exit
from .alpha_ladder_reported_trade_exit_tier0 import (
    MECHANISM_ID,
    MECHANISM_PATH,
    MECHANISM_SHA256,
    TIER0_CERTIFICATE_PATH,
    TIER0_DECISION_PATH,
    validate_live_evidence,
)
from .alpha_research_ladder import (
    SESSION_MANIFEST_SCHEMA,
    load_active_ladder,
    validate_session_manifest,
)
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .preexecution_fold_certification import (
    ROW_CERTIFIED,
    build_fold_readiness_certificate,
    validate_fold_readiness_certificate,
)
from .research_gateway_policy import ALPHA_LADDER_READINESS_CENSUS_OPERATION


PLAN_PATH = Path("configs/alpha_ladder_reported_trade_exit_readiness_census_plan.json")
OUTPUT_ROOT = Path(
    "state/unpublished_evidence/alpha_ladder_reported_trade_exit_readiness"
)
PREPARE_SCRIPT_PATH = Path(
    "scripts/prepare_alpha_ladder_reported_trade_exit_readiness_census_plan.py"
)
RUNNER_PATH = Path(
    "scripts/run_alpha_ladder_reported_trade_exit_readiness_census.py"
)
MODULE_PATH = Path(
    "src/futures_rebuild/alpha_ladder_reported_trade_exit_readiness.py"
)
TEST_PATH = Path("tests/test_alpha_ladder_reported_trade_exit_readiness.py")
ACTIVE_CALENDAR_POINTER = Path(
    "configs/active_cash_open_impulse_historical_calendar.json"
)
PUBLISHED_CLOSURE_PATH = Path(
    "state/trial_registry/alpha_ladder_pre_registration_terminal_closure/"
    "f73c6013c972539a42dcb8182e512b4b867c61a4f773513c048983530546b632.json"
)
TRIAL_FAMILY = "alpha_ladder_reported_trade_exit"
EXIT_RESOLUTION_MINUTES = 15
DIRECT_DEPENDENCIES = frozenset({
    MODULE_PATH.as_posix(),
    PREPARE_SCRIPT_PATH.as_posix(),
    RUNNER_PATH.as_posix(),
    TEST_PATH.as_posix(),
    "src/futures_rebuild/active_data_view.py",
    "src/futures_rebuild/alpha_ladder_combined_readiness.py",
    "src/futures_rebuild/alpha_ladder_combined_readiness_v3.py",
    "src/futures_rebuild/alpha_ladder_frozen_mechanism.py",
    "src/futures_rebuild/alpha_ladder_limit_readiness.py",
    "src/futures_rebuild/alpha_ladder_reported_trade_exit_successor.py",
    "src/futures_rebuild/alpha_ladder_reported_trade_exit_tier0.py",
    "src/futures_rebuild/alpha_research_ladder.py",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/preexecution_fold_certification.py",
    "src/futures_rebuild/research_gateway_policy.py",
})


def _direction_path(
    *, bars: Sequence[LimitBar], trigger: LimitBar, feature: Sequence[LimitBar],
    direction: str, scenario: str, adverse_ticks: int,
) -> tuple[bool, bool, str]:
    """Prove entry then stop-or-reported-trade exit without a return-to-limit rule."""

    del adverse_ticks
    order_time = trigger.available_at
    entries = [
        bar for bar in bars
        if order_time < bar.event_at <= order_time + timedelta(minutes=5)
        and _penetrates(bar, direction=direction, limit=trigger.close)
    ]
    if not entries:
        return False, True, f"{direction}__{scenario}__EXPLICIT_CANCELLED_NO_TRADE_TIMEOUT"
    entries = sorted(entries, key=lambda item: item.event_at)
    if sum(bar.event_at == entries[0].event_at for bar in entries) != 1:
        return True, False, f"{direction}__{scenario}__ENTRY_EVIDENCE_AMBIGUOUS"
    entry = entries[0]
    if entry.identity != trigger.identity:
        return True, False, f"{direction}__{scenario}__ENTRY_IDENTITY_CHANGING"
    expected_economics = (feature[-1].tick_size, feature[-1].tick_value)
    if (
        (trigger.tick_size, trigger.tick_value) != expected_economics
        or (entry.tick_size, entry.tick_value) != expected_economics
    ):
        return True, False, f"{direction}__{scenario}__ENTRY_ECONOMICS_CHANGING"
    true_ranges = [
        max(bar.high - bar.low, abs(bar.high - previous.close), abs(bar.low - previous.close))
        for previous, bar in zip(feature, feature[1:])
    ]
    stop_ticks = int((
        Decimal("1.5") * (sum(true_ranges, Decimal(0)) / Decimal(20))
        / entry.tick_size
    ).to_integral_value(rounding=ROUND_CEILING))
    if stop_ticks <= 0:
        return True, False, f"{direction}__{scenario}__STOP_GEOMETRY_INVALID"
    stop = (
        trigger.close - Decimal(stop_ticks) * entry.tick_size
        if direction == "LONG"
        else trigger.close + Decimal(stop_ticks) * entry.tick_size
    )
    conservative_fill_time = entry.event_at + timedelta(minutes=1)
    scheduled = conservative_fill_time + timedelta(minutes=30)
    exit_result = classify_reported_trade_exit(
        bars=bars,
        scheduled_exit_intent=scheduled,
        identity=entry.identity,
        resolution_minutes=EXIT_RESOLUTION_MINUTES,
    )
    resolution_end = exit_result.order_time + timedelta(minutes=EXIT_RESOLUTION_MINUTES)
    exit_candidates = sorted(
        (
            bar for bar in bars
            if exit_result.order_time < bar.event_at <= resolution_end
        ),
        key=lambda item: item.event_at,
    )
    first_exit_event = exit_candidates[0].event_at if exit_candidates else None
    for bar in sorted(bars, key=lambda item: item.event_at):
        if bar.event_at < entry.event_at:
            continue
        if exit_result.complete:
            assert exit_result.fill_time is not None
            if bar.event_at >= exit_result.fill_time:
                break
        elif first_exit_event is not None and bar.event_at >= first_exit_event:
            break
        elif bar.event_at > resolution_end:
            break
        if bar.identity != entry.identity:
            return True, False, f"{direction}__{scenario}__HOLD_IDENTITY_CHANGING"
        if (bar.tick_size, bar.tick_value) != expected_economics:
            return True, False, f"{direction}__{scenario}__HOLD_ECONOMICS_CHANGING"
        stopped = bar.low <= stop if direction == "LONG" else bar.high >= stop
        if stopped:
            return True, True, f"{direction}__{scenario}__VERIFIED_PROTECTIVE_STOP"
    if not exit_result.complete:
        return True, False, f"{direction}__{scenario}__{exit_result.disposition}"
    assert exit_result.evidence_bar is not None
    if (
        exit_result.evidence_bar.tick_size,
        exit_result.evidence_bar.tick_value,
    ) != expected_economics:
        return True, False, f"{direction}__{scenario}__EXIT_ECONOMICS_CHANGING"
    stop_triggered_at_exit_open = (
        exit_result.evidence_bar.open <= stop
        if direction == "LONG"
        else exit_result.evidence_bar.open >= stop
    )
    if stop_triggered_at_exit_open:
        return True, True, f"{direction}__{scenario}__VERIFIED_PROTECTIVE_STOP"
    return True, True, f"{direction}__{scenario}__VERIFIED_REPORTED_TRADE_EXIT"


def _baseline_directions(
    baseline: str | None, feature: Sequence[LimitBar],
) -> tuple[str, ...]:
    if baseline is None or baseline == "fold_local_unconditional_direction":
        return ("LONG", "SHORT")
    if baseline == "risk_matched_always_long":
        return ("LONG",)
    if baseline == "risk_matched_always_short":
        return ("SHORT",)
    delta = feature[-1].close - feature[-2].close
    if delta == 0:
        return ()
    momentum = "LONG" if delta > 0 else "SHORT"
    if baseline == "previous_reported_bar_sign_momentum":
        return (momentum,)
    if baseline == "previous_reported_bar_sign_reversal":
        return ("SHORT" if momentum == "LONG" else "LONG",)
    raise IntegrityError(f"unknown active baseline: {baseline}")


def classify_session(
    *, session: str, bars: Sequence[LimitBar], cost_ticks: Mapping[str, int],
    baseline: str | None = None,
) -> SessionReadiness:
    """Derive candidate or one baseline schedule independently from raw bars."""

    if baseline == "flat_no_trade":
        raise IntegrityError("flat no-trade has no execution classifier")
    ordered_bars = tuple(sorted(bars, key=lambda item: item.event_at))
    if len({bar.event_at for bar in ordered_bars}) != len(ordered_bars):
        return SessionReadiness(
            False, True, False, ("AMBIGUOUS_DUPLICATE_SOURCE_TIMESTAMP",), {},
        )
    bars = ordered_bars
    feature, risk = _feature_and_risk(bars, cost_ticks)
    if feature is None or risk is None:
        return SessionReadiness(
            False, False, True, ("EXPLICIT_CAUSAL_FEATURE_ABSTENTION",), {},
        )
    directions = _baseline_directions(baseline, feature)
    if not directions:
        return SessionReadiness(
            True,
            False,
            True,
            tuple(f"{scenario}__EXPLICIT_CAUSAL_ZERO_SIGN_ABSTENTION" for scenario in SCENARIOS),
            risk,
        )
    from datetime import date, datetime, time
    from .alpha_ladder_limit_readiness import CT

    checkpoint = datetime.combine(date.fromisoformat(session), time(10, 0), CT)
    decision = checkpoint + timedelta(seconds=5)
    triggers = sorted([
        bar for bar in bars
        if bar.event_at >= checkpoint
        and decision < bar.available_at <= decision + timedelta(seconds=120)
    ], key=lambda item: (item.available_at, item.event_at))
    if not triggers:
        return SessionReadiness(
            True, False, True, ("EXPLICIT_CAUSAL_NO_TRIGGER_TIMEOUT",), risk,
        )
    if sum(
        (bar.available_at, bar.event_at)
        == (triggers[0].available_at, triggers[0].event_at)
        for bar in triggers
    ) != 1:
        return SessionReadiness(
            True, True, False, ("TRIGGER_EVIDENCE_AMBIGUOUS",), risk,
        )
    trigger = triggers[0]
    if trigger.identity != feature[-1].identity:
        return SessionReadiness(
            True, True, False, ("TRIGGER_IDENTITY_CHANGING",), risk,
        )
    selected = False
    complete = True
    dispositions: list[str] = []
    for scenario in SCENARIOS:
        if risk[scenario] == "RISK_ABSTENTION":
            dispositions.append(f"{scenario}__RISK_ABSTENTION")
            continue
        for direction in directions:
            filled, path_complete, disposition = _direction_path(
                bars=bars,
                trigger=trigger,
                feature=feature,
                direction=direction,
                scenario=scenario,
                adverse_ticks=int(cost_ticks[scenario]),
            )
            selected = selected or filled
            complete = complete and path_complete
            dispositions.append(disposition)
    return SessionReadiness(True, selected, complete, tuple(dispositions), risk)


def _session_cache(
    *, sessions: Sequence[str], bars_by_session: Mapping[str, Sequence[LimitBar]],
    cost_ticks: Mapping[str, int],
) -> dict[str, object]:
    ordered = tuple(sorted(set(sessions)))
    candidate = {
        session: classify_session(
            session=session,
            bars=bars_by_session.get(session, ()),
            cost_ticks=cost_ticks,
        )
        for session in ordered
    }
    baselines = {
        baseline: {
            session: classify_session(
                session=session,
                bars=bars_by_session.get(session, ()),
                cost_ticks=cost_ticks,
                baseline=baseline,
            )
            for session in ordered
        }
        for baseline in MANDATORY_BASELINES
        if baseline != "flat_no_trade"
    }
    return {"candidate": candidate, "baselines": baselines}


def _failure_reason(item: SessionReadiness) -> str | None:
    if not item.feature_complete:
        return item.dispositions[0]
    if item.selected and not item.path_complete:
        for disposition in item.dispositions:
            if any(token in disposition for token in (
                "MISSING", "CHANGING", "AMBIGUOUS", "INVALID",
            )):
                return disposition
        return "EXECUTION_PATH_INCOMPLETE"
    return None


def _fold_evidence(
    *, market: str, fold: Mapping[str, object],
    rows_by_session: Mapping[str, object], risk_by_session: Mapping[str, object],
    cache: Mapping[str, object] | None = None,
) -> dict[str, object]:
    del risk_by_session
    cost_ticks = rows_by_session["__cost_ticks__"]
    assert isinstance(cost_ticks, Mapping)
    bars = {key: value for key, value in rows_by_session.items() if key != "__cost_ticks__"}
    if cache is None:
        all_sessions = tuple(
            str(item) for item in (*fold["training_sessions"], *fold["evaluation_sessions"])
        )
        cache = _session_cache(
            sessions=all_sessions, bars_by_session=bars, cost_ticks=cost_ticks,
        )
    candidate = cache["candidate"]
    baseline_cache = cache["baselines"]
    assert isinstance(candidate, Mapping) and isinstance(baseline_cache, Mapping)
    training = [(str(session), candidate[str(session)]) for session in fold["training_sessions"]]
    evaluation = [(str(session), candidate[str(session)]) for session in fold["evaluation_sessions"]]

    def complete(items):
        return sum(item.feature_complete and item.path_complete for _, item in items)

    def feature(items):
        return sum(item.feature_complete for _, item in items)

    def selected(items):
        return sum(item.selected for _, item in items)

    def paths(items):
        return sum(item.selected and item.path_complete for _, item in items)

    def risk_counts(items):
        selected_items = [item for _, item in items if item.selected]
        return {
            scenario: {
                "feasible_sessions": sum(
                    item.scenario_risk.get(scenario) == "FEASIBLE"
                    for item in selected_items
                ),
                "risk_abstention_sessions": sum(
                    item.scenario_risk.get(scenario) == "RISK_ABSTENTION"
                    for item in selected_items
                ),
                "unresolved_sessions": sum(
                    item.scenario_risk.get(scenario)
                    not in {"FEASIBLE", "RISK_ABSTENTION"}
                    for item in selected_items
                ),
            }
            for scenario in SCENARIOS
        }

    def exclusions(items, role, universe):
        counts = Counter()
        for _session, item in items:
            reason = _failure_reason(item)
            if reason is not None:
                counts[f"{role}__{universe}__{reason}"] += 1
        return dict(counts)

    candidate_risk = risk_counts(evaluation)
    baseline_results: dict[str, object] = {}
    for baseline in MANDATORY_BASELINES:
        if baseline == "flat_no_trade":
            baseline_results[baseline] = {
                "expected_sessions": len(evaluation),
                "terminal_sessions": len(evaluation),
                "selected_sessions": 0,
                "selected_path_complete_sessions": 0,
                "scenario_risk_dispositions": {
                    scenario: {
                        "feasible_sessions": 0,
                        "risk_abstention_sessions": 0,
                        "unresolved_sessions": 0,
                    }
                    for scenario in SCENARIOS
                },
                "schedule_independently_derived": True,
                "flat_no_trade": True,
            }
            continue
        raw = baseline_cache[baseline]
        assert isinstance(raw, Mapping)
        items = [(session, raw[session]) for session, _item in evaluation]
        baseline_results[baseline] = {
            "expected_sessions": len(items),
            "terminal_sessions": len(items),
            "selected_sessions": selected(items),
            "selected_path_complete_sessions": paths(items),
            "scenario_risk_dispositions": risk_counts(items),
            "schedule_independently_derived": True,
            "flat_no_trade": False,
        }

    exclusion_reasons = {
        **exclusions(training, "TRAINING", "CANDIDATE"),
        **exclusions(evaluation, "EVALUATION", "CANDIDATE"),
    }
    years: dict[str, object] = {}
    for year in sorted({session[:4] for session, _ in (*training, *evaluation)}):
        year_training = [item for item in training if item[0].startswith(year)]
        year_evaluation = [item for item in evaluation if item[0].startswith(year)]
        years[year] = {
            "expected_training_sessions": len(year_training),
            "complete_training_sessions": complete(year_training),
            "expected_evaluation_sessions": len(year_evaluation),
            "feature_complete_evaluation_sessions": feature(year_evaluation),
            "terminal_evaluation_sessions": len(year_evaluation),
            "execution_path_complete_evaluation_sessions": complete(year_evaluation),
            "exclusion_reasons": {
                **exclusions(year_training, "TRAINING", "CANDIDATE"),
                **exclusions(year_evaluation, "EVALUATION", "CANDIDATE"),
            },
        }
    return {
        "fold_id": fold["fold_id"],
        "market": market,
        "role": "OUTER",
        "counts": {
            "expected_training_sessions": len(training),
            "complete_training_sessions": complete(training),
            "feature_complete_training_sessions": feature(training),
            "transformation_ready_training_sessions": feature(training),
            "expected_evaluation_sessions": len(evaluation),
            "feature_complete_evaluation_sessions": feature(evaluation),
            "terminal_evaluation_sessions": len(evaluation),
            "execution_path_complete_evaluation_sessions": complete(evaluation),
            "candidate_selected_sessions": selected(evaluation),
            "candidate_selected_path_complete_sessions": paths(evaluation),
            "scenario_risk_dispositions": candidate_risk,
            "purge_minutes": int(fold["purge_minutes"]),
            "embargo_sessions": len(fold["embargo_sessions"]),
        },
        "checks": {
            "chronological_order": True,
            "purge_applied": True,
            "embargo_applied": True,
            "training_only_transformation": True,
            "contract_identity_discontinuities_terminalized": True,
            "roll_discontinuities_terminalized": True,
            "all_incomplete_sessions_terminalized": True,
            "complete_required_metrics": True,
            "promotion_path_computable": True,
        },
        "baseline_universe_readiness": dict(sorted(baseline_results.items())),
        "exclusion_reasons": dict(sorted(exclusion_reasons.items())),
        "market_year_breakdown": years,
    }


def _file_sha(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_bytes(payload) + b"\n").hexdigest()


def _manifest(core: Mapping[str, object]) -> dict[str, object]:
    return {**core, "manifest_id": sha256_json(core)}


def _selected_sources(*, root: Path) -> tuple[dict[str, str], dict[tuple[str, int], object]]:
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are absent")
    by_key = {
        (str(item["market"]), int(item["year"])): item
        for item in entries if isinstance(item, dict)
    }
    selected: dict[str, str] = {}
    for market in CORE:
        for year in YEARS:
            item = by_key.get((market, year))
            if not isinstance(item, Mapping) or item.get("disposition") != "RESEARCH_READY_CAUSAL_PRICE":
                raise IntegrityError(f"active catalog cannot bind {market} {year}")
            selected[str(item["parquet_path"])] = str(item["parquet_sha256"])
    if len(selected) != 20:
        raise IntegrityError("readiness plan must bind exactly twenty market-year sources")
    return dict(sorted(selected.items())), by_key


def build_plan(*, root: Path) -> dict[str, object]:
    contract, profile = load_active_ladder(root)
    tier0 = validate_live_evidence(root=root)
    pointer, calendar = _active_calendar(root)
    selected, _by_key = _selected_sources(root=root)
    eligible = {
        market: tuple(
            str(item["trade_date"])
            for item in calendar["calendar_rows"]
            if item["market"] == market
            and item["checkpoint_open"].get(CHECKPOINT) is True
        )
        for market in CORE
    }
    if any(
        eligible[market] != tuple(sorted(set(eligible[market])))
        for market in CORE
    ):
        raise IntegrityError("checkpoint-eligible calendar sessions are not unique and chronological")
    required_after_pilot = (
        TRAINING_SESSIONS + (OUTER_FOLDS - 1) * EVALUATION_SESSIONS
        + EMBARGO_SESSIONS + EVALUATION_SESSIONS
    )
    if len(eligible["ES"]) < TRAINING_SESSIONS + EMBARGO_SESSIONS + EVALUATION_SESSIONS:
        raise IntegrityError("calendar cannot support the ES pilot")
    if any(len(eligible[market]) - EVALUATION_SESSIONS < required_after_pilot for market in CORE):
        raise IntegrityError("calendar cannot support per-market Tier 1 folds after pilot exclusion")
    bindings = {
        MECHANISM_PATH.as_posix(): MECHANISM_SHA256,
        TIER0_CERTIFICATE_PATH.as_posix(): str(tier0["certificate_sha256"]),
        TIER0_DECISION_PATH.as_posix(): str(tier0["decision_sha256"]),
        PUBLISHED_CLOSURE_PATH.as_posix(): sha256_file(root / PUBLISHED_CLOSURE_PATH),
        "configs/active_alpha_research_ladder.json": sha256_file(
            root / "configs/active_alpha_research_ladder.json"
        ),
        ACTIVE_CATALOG_PATH.as_posix(): sha256_file(root / ACTIVE_CATALOG_PATH),
        ACTIVE_CALENDAR_POINTER.as_posix(): sha256_file(root / ACTIVE_CALENDAR_POINTER),
        str(pointer["calendar_path"]): str(pointer["calendar_sha256"]),
        **selected,
        **{relative: sha256_file(root / relative) for relative in DIRECT_DEPENDENCIES},
    }
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_reported_trade_exit_readiness_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES",
        "operation": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "contract_id": contract["contract_id"],
        "profile_id": profile["profile_id"],
        "mechanism_id": MECHANISM_ID,
        "mechanism_sha256": MECHANISM_SHA256,
        "tier0_certificate_id": tier0["certificate_id"],
        "tier0_decision_id": tier0["decision_id"],
        "markets": list(CORE),
        "years": list(YEARS),
        "checkpoint": CHECKPOINT,
        "entry_semantics": "ONE_TICK_PENETRATION_RESTING_LIMIT_OR_EXPLICIT_NO_TRADE",
        "entry_fill_time_proxy": "PENETRATION_BAR_INTERVAL_END_CAUSAL_CONSERVATIVE",
        "exit_semantics": (
            "PROTECTIVE_STOP_OR_FIRST_VALID_SAME_IDENTITY_REPORTED_TRADE_BAR_OPEN_"
            "AFTER_SCHEDULED_MARKET_EXIT_ORDER"
        ),
        "exit_price_return_condition": False,
        "exit_resolution_minutes": EXIT_RESOLUTION_MINUTES,
        "protective_stop_active_until_exit_proxy": True,
        "pilot": {
            "market": "ES",
            "training_sessions": TRAINING_SESSIONS,
            "evaluation_sessions": EVALUATION_SESSIONS,
            "embargo_sessions": EMBARGO_SESSIONS,
            "purge_minutes": PURGE_MINUTES,
            "selection_rule": "EARLIEST_ROW_EXECUTABLE_ROLLING_504_1_63_NO_RETURNS",
        },
        "tier_1": {
            "markets": list(CORE),
            "outer_folds": OUTER_FOLDS,
            "initial_training_sessions": TRAINING_SESSIONS,
            "evaluation_sessions": EVALUATION_SESSIONS,
            "calendar_basis": "PER_MARKET_CHECKPOINT_ELIGIBLE_SESSIONS",
            "pilot_sessions_excluded_from_every_market": True,
            "no_cross_market_calendar_intersection_drop": True,
        },
        "required_baselines": list(MANDATORY_BASELINES),
        "baseline_readiness_semantics": {
            "flat_no_trade": "EXACT_ZERO_NO_SCHEDULE",
            "fold_local_unconditional_direction": "BOTH_DIRECTION_SOURCE_SUPERSET",
            "previous_reported_bar_sign_momentum": "OWN_CAUSAL_SIGN_DIRECTION",
            "previous_reported_bar_sign_reversal": "OWN_CAUSAL_OPPOSITE_SIGN_DIRECTION",
            "risk_matched_always_long": "OWN_LONG_DIRECTION",
            "risk_matched_always_short": "OWN_SHORT_DIRECTION",
            "cross_market_ranking": "ALL_MARKET_INTENT_SUPERSET_BEFORE_RANKING",
            "candidate_schedule_reuse": False,
        },
        "required_cost_scenarios": list(SCENARIOS),
        "coverage": {
            "checkpoint_accounting_percent": 100,
            "active_baseline_checkpoint_accounting_percent": 100,
            "filled_entry_verified_exit_percent": 100,
            "future_complete_path_filtering": False,
        },
        "required_outputs": {
            "always": [
                "source_audit.json",
                "pilot_fold_selection.json",
                "readiness_report.json",
            ],
            "when_pilot_selected": [
                "pilot_session_manifest.json",
                "tier1_session_manifest.json",
                "pilot_readiness_certificate.json",
                "tier1_readiness_certificate.json",
            ],
        },
        "execution_limits": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_workers": 4,
            "worker_deadline_seconds": 3300,
            "maximum_runtime_seconds": 3600,
            "maximum_external_cost_usd": "0",
            "windows_host_required": True,
        },
        "output_root": OUTPUT_ROOT.as_posix(),
        "authority": {
            "historical_row_read": True,
            "returns": False,
            "model_fit": False,
            "prediction_generation": False,
            "performance_evaluation": False,
            "registration": False,
            "trial_execution": False,
            "publication": False,
            "provider_network_credentials": False,
            "year_2025_access": False,
            "active_data_mutation": False,
            "trading": False,
        },
        "calendar_id": calendar["calendar_id"],
        "bindings": dict(sorted(bindings.items())),
    }
    return {**core, "plan_id": sha256_json(core)}


def validate_plan(
    plan: Mapping[str, object], *, root: Path,
) -> dict[str, object]:
    contract, profile = load_active_ladder(root)
    tier0 = validate_live_evidence(root=root)
    pointer, calendar = _active_calendar(root)
    selected, _by_key = _selected_sources(root=root)
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    bindings = plan.get("bindings")
    tier1 = plan.get("tier_1")
    pilot = plan.get("pilot")
    limits = plan.get("execution_limits")
    authority = plan.get("authority")
    baseline_semantics = plan.get("baseline_readiness_semantics")
    expected_coverage = {
        "checkpoint_accounting_percent": 100,
        "active_baseline_checkpoint_accounting_percent": 100,
        "filled_entry_verified_exit_percent": 100,
        "future_complete_path_filtering": False,
    }
    expected_limits = {
        "maximum_attempts": 1,
        "maximum_retries": 0,
        "maximum_workers": 4,
        "worker_deadline_seconds": 3300,
        "maximum_runtime_seconds": 3600,
        "maximum_external_cost_usd": "0",
        "windows_host_required": True,
    }
    expected_authority = {
        "historical_row_read": True,
        "returns": False,
        "model_fit": False,
        "prediction_generation": False,
        "performance_evaluation": False,
        "registration": False,
        "trial_execution": False,
        "publication": False,
        "provider_network_credentials": False,
        "year_2025_access": False,
        "active_data_mutation": False,
        "trading": False,
    }
    expected_entry_semantics = (
        "ONE_TICK_PENETRATION_RESTING_LIMIT_OR_EXPLICIT_NO_TRADE"
    )
    expected_exit_semantics = (
        "PROTECTIVE_STOP_OR_FIRST_VALID_SAME_IDENTITY_REPORTED_TRADE_BAR_OPEN_"
        "AFTER_SCHEDULED_MARKET_EXIT_ORDER"
    )
    expected_baseline_semantics = {
        "flat_no_trade": "EXACT_ZERO_NO_SCHEDULE",
        "fold_local_unconditional_direction": "BOTH_DIRECTION_SOURCE_SUPERSET",
        "previous_reported_bar_sign_momentum": "OWN_CAUSAL_SIGN_DIRECTION",
        "previous_reported_bar_sign_reversal": "OWN_CAUSAL_OPPOSITE_SIGN_DIRECTION",
        "risk_matched_always_long": "OWN_LONG_DIRECTION",
        "risk_matched_always_short": "OWN_SHORT_DIRECTION",
        "cross_market_ranking": "ALL_MARKET_INTENT_SUPERSET_BEFORE_RANKING",
        "candidate_schedule_reuse": False,
    }
    required_binding_paths = {
        MECHANISM_PATH.as_posix(),
        TIER0_CERTIFICATE_PATH.as_posix(),
        TIER0_DECISION_PATH.as_posix(),
        PUBLISHED_CLOSURE_PATH.as_posix(),
        "configs/active_alpha_research_ladder.json",
        ACTIVE_CATALOG_PATH.as_posix(),
        ACTIVE_CALENDAR_POINTER.as_posix(),
        str(pointer["calendar_path"]),
        *selected,
        *DIRECT_DEPENDENCIES,
    }
    if (
        plan.get("plan_id") != sha256_json(core)
        or plan.get("schema_version")
        != "alpha_ladder_reported_trade_exit_readiness_plan/1.0.0"
        or plan.get("state") != "PREPARED_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES"
        or plan.get("operation") != ALPHA_LADDER_READINESS_CENSUS_OPERATION
        or plan.get("contract_id") != contract["contract_id"]
        or plan.get("profile_id") != profile["profile_id"]
        or plan.get("mechanism_id") != MECHANISM_ID
        or plan.get("mechanism_sha256") != MECHANISM_SHA256
        or plan.get("tier0_certificate_id") != tier0["certificate_id"]
        or plan.get("tier0_decision_id") != tier0["decision_id"]
        or plan.get("markets") != list(CORE)
        or plan.get("years") != list(YEARS)
        or plan.get("checkpoint") != CHECKPOINT
        or plan.get("entry_semantics") != expected_entry_semantics
        or plan.get("entry_fill_time_proxy")
        != "PENETRATION_BAR_INTERVAL_END_CAUSAL_CONSERVATIVE"
        or plan.get("exit_semantics") != expected_exit_semantics
        or plan.get("exit_price_return_condition") is not False
        or plan.get("exit_resolution_minutes") != EXIT_RESOLUTION_MINUTES
        or plan.get("protective_stop_active_until_exit_proxy") is not True
        or plan.get("required_baselines") != list(FROZEN_BASELINES)
        or plan.get("required_cost_scenarios") != list(SCENARIOS)
        or plan.get("coverage") != expected_coverage
        or not isinstance(pilot, Mapping)
        or pilot.get("market") != "ES"
        or pilot.get("training_sessions") != 504
        or pilot.get("evaluation_sessions") != 63
        or pilot.get("embargo_sessions") != 1
        or pilot.get("purge_minutes") != 40
        or pilot.get("selection_rule")
        != "EARLIEST_ROW_EXECUTABLE_ROLLING_504_1_63_NO_RETURNS"
        or not isinstance(tier1, Mapping)
        or tier1.get("markets") != list(CORE)
        or tier1.get("initial_training_sessions") != 504
        or tier1.get("evaluation_sessions") != 63
        or tier1.get("outer_folds") != 8
        or tier1.get("calendar_basis") != "PER_MARKET_CHECKPOINT_ELIGIBLE_SESSIONS"
        or tier1.get("pilot_sessions_excluded_from_every_market") is not True
        or tier1.get("no_cross_market_calendar_intersection_drop") is not True
        or baseline_semantics != expected_baseline_semantics
        or limits != expected_limits
        or authority != expected_authority
        or plan.get("calendar_id") != calendar["calendar_id"]
        or not isinstance(bindings, Mapping)
        or set(bindings) != required_binding_paths
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("reported-trade-exit readiness plan drifted")
    return dict(plan)


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="reported-trade-exit readiness plan")
    return validate_plan(plan, root=root)


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["execution_limits"]
    assert isinstance(limits, Mapping)
    return {
        "mechanism_id": MECHANISM_ID,
        "period": "2018,2019,2020,2021,2022",
        "markets": ",".join(CORE),
        "checkpoint": CHECKPOINT,
        "purpose": "ALPHA_REPORTED_TRADE_EXIT_PILOT_AND_TIER1_READINESS_ONLY",
        "output_root": OUTPUT_ROOT.as_posix(),
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "maximum_workers": "4",
        "worker_deadline_seconds": str(limits["worker_deadline_seconds"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_external_cost_usd": "0",
        "returns": "false",
        "model_fit": "false",
        "prediction_generation": "false",
        "performance_evaluation": "false",
        "registration": "false",
        "trial_execution": "false",
        "provider_network_access": "false",
        "holdout_2025_access": "false",
        "active_data_mutation": "false",
        "trading": "false",
        "approval_command": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> Mapping[str, object]:
    started = monotonic()
    plan = load_plan(root=root)
    if os.name != "nt" or multiprocessing.current_process().name != "MainProcess":
        raise UnauthorizedOperation("readiness census requires the Windows main process")
    output_root = root / OUTPUT_ROOT
    if output_root.exists():
        raise UnauthorizedOperation("readiness census output already exists")
    use_path = receipt.consume(
        boundary,
        operation=ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    selected, by_key = _selected_sources(root=root)
    mechanism = _read_canonical(root / MECHANISM_PATH, name="reported-trade mechanism")
    costs = mechanism["costs"]["round_trip_adverse_ticks"]
    tasks = []
    market_costs = {}
    for market in CORE:
        sources = []
        for year in YEARS:
            item = by_key[(market, year)]
            assert isinstance(item, Mapping)
            path = resolve(repository_root=root, market=market, year=year, purpose="SELECTION")
            relative = path.relative_to(root).as_posix()
            if selected.get(relative) != sha256_file(path):
                raise IntegrityError(f"active source changed for {market} {year}")
            sources.append((year, str(path)))
        market_costs[market] = {
            scenario: int(costs[scenario][market]) for scenario in SCENARIOS
        }
        tasks.append((market, tuple(sources), market_costs[market]))
    pool = multiprocessing.get_context("spawn").Pool(processes=4)
    try:
        worker_results = pool.map_async(_read_market, tasks, chunksize=1).get(
            timeout=int(plan["execution_limits"]["worker_deadline_seconds"])
        )
        pool.close()
        pool.join()
    except BaseException:
        pool.terminate()
        pool.join()
        raise
    observed = {item[0]: item[1:] for item in worker_results}
    _pointer, calendar = _active_calendar(root)
    eligible = {
        market: tuple(
            str(item["trade_date"])
            for item in calendar["calendar_rows"]
            if item["market"] == market
            and item["checkpoint_open"].get(CHECKPOINT) is True
        )
        for market in CORE
    }
    calendar_counts = {
        market: {
            str(year): sum(session.startswith(str(year)) for session in eligible[market])
            for year in YEARS
        }
        for market in CORE
    }
    worker_audits = {
        market: observed[market][2] for market in CORE
    }
    audit_core = {
        "schema_version": "alpha_ladder_reported_trade_exit_source_audit/1.0.0",
        "plan_id": plan["plan_id"],
        "mechanism_id": MECHANISM_ID,
        "price_free": True,
        "source_bindings": selected,
        "worker_audits": worker_audits,
        "checkpoint_eligible_session_counts": calendar_counts,
    }
    source_audit = {**audit_core, "audit_id": sha256_json(audit_core)}
    es_prices, _es_risk, _es_audit = observed["ES"]
    es_with_costs = {**es_prices, "__cost_ticks__": market_costs["ES"]}
    es_cache = _session_cache(
        sessions=eligible["ES"],
        bars_by_session=es_prices,
        cost_ticks=market_costs["ES"],
    )

    def pilot_evidence_builder(**kwargs):
        return _fold_evidence(**kwargs, cache=es_cache)

    pilot_fold, pilot_evidence, selection = select_earliest_executable_pilot(
        sessions=eligible["ES"],
        rows_by_session=es_with_costs,
        risk_by_session={},
        evidence_builder=pilot_evidence_builder,
    )
    validate_selection(selection, selected_fold=pilot_fold)
    selection_core = {
        "schema_version": "alpha_ladder_reported_trade_exit_pilot_selection/1.0.0",
        "plan_id": plan["plan_id"],
        "mechanism_id": MECHANISM_ID,
        **selection,
    }
    selection_report = {**selection_core, "selection_id": sha256_json(selection_core)}
    source_rel = (OUTPUT_ROOT / "source_audit.json").as_posix()
    selection_rel = (OUTPUT_ROOT / "pilot_fold_selection.json").as_posix()
    bindings = {
        **plan["bindings"],
        PLAN_PATH.as_posix(): sha256_file(root / PLAN_PATH),
        source_rel: _file_sha(source_audit),
        selection_rel: _file_sha(selection_report),
    }
    if pilot_fold is None or pilot_evidence is None:
        report_core = {
            "schema_version": "alpha_ladder_reported_trade_exit_readiness_report/1.0.0",
            "state": "SEALED_UNPUBLISHED_NO_EXECUTABLE_PILOT_FOLD",
            "plan_id": plan["plan_id"],
            "mechanism_id": MECHANISM_ID,
            "authorization_receipt_id": receipt.receipt_id,
            "authorization_use_path": use_path.relative_to(root).as_posix(),
            "authorization_use_sha256": sha256_file(use_path),
            "source_audit_id": source_audit["audit_id"],
            "pilot_decision": "FAIL",
            "tier1_decision": "NOT_RUN",
            "combined_registration_ready": False,
            "pilot_selection_id": selection_report["selection_id"],
            "authority": plan["authority"],
        }
        report = {**report_core, "report_id": sha256_json(report_core)}
        output_root.mkdir(parents=True, exist_ok=False)
        _write_once(root / source_rel, source_audit)
        _write_once(root / selection_rel, selection_report)
        _write_once(output_root / "readiness_report.json", report)
        return report
    exclusions = tuple(str(item) for item in pilot_fold["evaluation_sessions"])
    folds_by_market = {
        market: _outer_folds(
            tuple(session for session in eligible[market] if session not in set(exclusions))
        )
        for market in CORE
    }
    pilot_manifest = _manifest({
        "schema_version": SESSION_MANIFEST_SCHEMA,
        "contract_id": plan["contract_id"],
        "mechanism_sha256": MECHANISM_SHA256,
        "stage": "pilot",
        "markets": ["ES"],
        "fold_ordinal": 0,
        "calendar_start_offset": pilot_fold["calendar_start_offset"],
        "selection_rule": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
        "selection_evidence_path": selection_rel,
        "selection_evidence_sha256": _file_sha(selection_report),
        "training_session_ids": pilot_fold["training_sessions"],
        "evaluation_session_ids": pilot_fold["evaluation_sessions"],
        "purge_applied": True,
        "embargo_applied": True,
    })
    tier1_manifest = _manifest({
        "schema_version": SESSION_MANIFEST_SCHEMA,
        "contract_id": plan["contract_id"],
        "mechanism_sha256": MECHANISM_SHA256,
        "stage": "tier_1",
        "excluded_pilot_evaluation_session_ids": list(exclusions),
        "evaluation_session_ids_by_market": {
            market: sorted({
                str(session)
                for fold in folds_by_market[market]
                for session in fold["evaluation_sessions"]
            })
            for market in CORE
        },
    })
    pilot_rel = (OUTPUT_ROOT / "pilot_session_manifest.json").as_posix()
    tier1_rel = (OUTPUT_ROOT / "tier1_session_manifest.json").as_posix()
    pilot_bindings = {
        **bindings,
        pilot_rel: _file_sha(pilot_manifest),
    }
    tier1_bindings = {
        **bindings,
        tier1_rel: _file_sha(tier1_manifest),
    }
    pilot_cert = build_fold_readiness_certificate(
        trial_family=TRIAL_FAMILY,
        protocol_id=MECHANISM_ID,
        source_bindings=pilot_bindings,
        fold_evidence=(pilot_evidence,),
        required_markets=("ES",),
        required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=SCENARIOS,
        required_outer_fold_ids=("fold-0",),
        required_nested_fold_ids=(),
        expected_outer_folds=1,
        expected_nested_folds=0,
        minimum_training_sessions=TRAINING_SESSIONS,
        minimum_evaluation_sessions=EVALUATION_SESSIONS,
        minimum_purge_minutes=PURGE_MINUTES,
        minimum_embargo_sessions=EMBARGO_SESSIONS,
        evidence_class=ROW_CERTIFIED,
        historical_rows_opened=True,
    )
    tier1_evidence = []
    for market in CORE:
        prices, _risk, _audit = observed[market]
        with_costs = {**prices, "__cost_ticks__": market_costs[market]}
        cache = _session_cache(
            sessions=eligible[market],
            bars_by_session=prices,
            cost_ticks=market_costs[market],
        )
        tier1_evidence.extend(
            _fold_evidence(
                market=market,
                fold=fold,
                rows_by_session=with_costs,
                risk_by_session={},
                cache=cache,
            )
            for fold in folds_by_market[market]
        )
    tier1_cert = build_fold_readiness_certificate(
        trial_family=TRIAL_FAMILY,
        protocol_id=MECHANISM_ID,
        source_bindings=tier1_bindings,
        fold_evidence=tier1_evidence,
        required_markets=CORE,
        required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=SCENARIOS,
        required_outer_fold_ids=tuple(f"fold-{index}" for index in range(OUTER_FOLDS)),
        required_nested_fold_ids=(),
        expected_outer_folds=OUTER_FOLDS,
        expected_nested_folds=0,
        minimum_training_sessions=TRAINING_SESSIONS,
        minimum_evaluation_sessions=EVALUATION_SESSIONS,
        minimum_purge_minutes=PURGE_MINUTES,
        minimum_embargo_sessions=EMBARGO_SESSIONS,
        evidence_class=ROW_CERTIFIED,
        historical_rows_opened=True,
    )
    validate_session_manifest(
        pilot_manifest,
        contract_id=str(plan["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256,
        stage="pilot",
        markets=("ES",),
    )
    validate_session_manifest(
        tier1_manifest,
        contract_id=str(plan["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256,
        stage="tier_1",
        markets=CORE,
        pilot_evaluation_sha256=sha256_json(list(exclusions)),
    )
    if monotonic() - started > int(plan["execution_limits"]["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("readiness census exceeded total runtime")
    report_core = {
        "schema_version": "alpha_ladder_reported_trade_exit_readiness_report/1.0.0",
        "state": "SEALED_UNPUBLISHED_ROW_CERTIFIED_READINESS",
        "plan_id": plan["plan_id"],
        "mechanism_id": MECHANISM_ID,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "source_audit_id": source_audit["audit_id"],
        "pilot_decision": pilot_cert["overall_decision"],
        "tier1_decision": tier1_cert["overall_decision"],
        "combined_registration_ready": (
            pilot_cert["overall_decision"] == "PASS"
            and tier1_cert["overall_decision"] == "PASS"
        ),
        "pilot_selection_id": selection_report["selection_id"],
        "pilot_certificate_id": pilot_cert["certificate_id"],
        "tier1_certificate_id": tier1_cert["certificate_id"],
        "authority": plan["authority"],
    }
    report = {**report_core, "report_id": sha256_json(report_core)}
    output_root.mkdir(parents=True, exist_ok=False)
    _write_once(root / source_rel, source_audit)
    _write_once(root / selection_rel, selection_report)
    _write_once(root / pilot_rel, pilot_manifest)
    _write_once(root / tier1_rel, tier1_manifest)
    validate_fold_readiness_certificate(pilot_cert, root=root)
    validate_fold_readiness_certificate(tier1_cert, root=root)
    _write_once(output_root / "pilot_readiness_certificate.json", pilot_cert)
    _write_once(output_root / "tier1_readiness_certificate.json", tier1_cert)
    _write_once(output_root / "readiness_report.json", report)
    return report
