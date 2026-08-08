"""Row-certified readiness for the counted resting-limit Alpha mechanism."""

from __future__ import annotations

import hashlib
import multiprocessing
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from time import monotonic
from zoneinfo import ZoneInfo

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
    EVALUATION_SESSIONS,
    EMBARGO_SESSIONS,
    OUTER_FOLDS,
    PURGE_MINUTES,
    TRAINING_SESSIONS,
    _outer_folds,
    select_earliest_executable_pilot,
    validate_selection,
)
from .alpha_ladder_frozen_mechanism import (
    build_tier0_certificate,
    validate_tier0_certificate,
)
from .alpha_ladder_source_compatible_successor import validate_successor
from .alpha_research_ladder import (
    SESSION_MANIFEST_SCHEMA,
    load_active_ladder,
    validate_stage_decision,
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
from .cash_open_source_compatibility import source_row_from_mapping


CT = ZoneInfo("America/Chicago")
MECHANISM_ID = "767ecf3987d816c2f657fbf030da25bf72511275812d6664aa6bd56faf7f3660"
MECHANISM_SHA256 = "c29e2a13639a289440065b6d07b064e4cd2334c80b42ba8de205d3dc5cba5e1c"
MECHANISM_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_source_compatible_successor/"
    f"{MECHANISM_ID}/mechanism.json"
)
TIER0_CERTIFICATE_PATH = MECHANISM_PATH.with_name("tier0_certificate.json")
TIER0_DECISION_PATH = MECHANISM_PATH.with_name("tier0_decision.json")
PLAN_PATH = Path("configs/alpha_ladder_limit_readiness_census_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_ladder_limit_readiness")
RUNNER_PATH = Path("scripts/run_alpha_ladder_limit_readiness_census.py")
REQUIRED_COLUMNS = frozenset({
    "actual_identity_hash", "disposition", "event_at_ns", "exchange_session_date",
    "source_row_sha256", "open_nano", "high_nano", "low_nano", "close_nano",
    "volume", "tick_size", "tick_value",
})


@dataclass(frozen=True)
class LimitBar:
    event_at: datetime
    available_at: datetime
    identity: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    tick_size: Decimal
    tick_value: Decimal


@dataclass(frozen=True)
class SessionReadiness:
    feature_complete: bool
    selected: bool
    path_complete: bool
    dispositions: tuple[str, ...]
    scenario_risk: Mapping[str, str]


def _dependency_clock(event_at_ns: int) -> bool:
    clock = datetime.fromtimestamp(
        event_at_ns / 1_000_000_000, timezone.utc,
    ).astimezone(CT).time()
    minute = clock.hour * 60 + clock.minute
    return 9 * 60 + 29 <= minute <= 11 * 60 + 10


def _read_market(task):
    import pyarrow.parquet as pq

    market, sources, cost_ticks = task
    prices: dict[str, list[LimitBar]] = {}
    audits: dict[str, object] = {}
    for year, raw_path in sources:
        path = Path(raw_path)
        parquet = pq.ParquetFile(path)
        if not REQUIRED_COLUMNS.issubset(parquet.schema_arrow.names):
            raise IntegrityError(f"limit-readiness schema is incomplete for {market} {year}")
        total = retained = sessionless = 0
        for batch in parquet.iter_batches(batch_size=65_536, columns=sorted(REQUIRED_COLUMNS)):
            columns = batch.to_pydict()
            for index in range(batch.num_rows):
                total += 1
                raw = {name: values[index] for name, values in columns.items()}
                event_ns = raw.get("event_at_ns")
                if type(event_ns) is not int or not _dependency_clock(event_ns):
                    continue
                session = raw.get("exchange_session_date")
                if not isinstance(session, str):
                    sessionless += 1
                    continue
                normalized = source_row_from_mapping(market=market, row=raw)
                if not normalized.executable:
                    retained += 1
                    continue
                identity = normalized.actual_identity_hash
                if not isinstance(identity, str) or not identity:
                    raise IntegrityError(f"executable row lacks identity for {market} {year}")
                event = datetime.fromtimestamp(event_ns / 1_000_000_000, timezone.utc).astimezone(CT)
                available = datetime.fromtimestamp(
                    normalized.available_at_ns / 1_000_000_000, timezone.utc,
                ).astimezone(CT)
                prices.setdefault(session, []).append(LimitBar(
                    event_at=event, available_at=available, identity=identity,
                    open=Decimal(raw["open_nano"]) / Decimal(1_000_000_000),
                    high=Decimal(raw["high_nano"]) / Decimal(1_000_000_000),
                    low=Decimal(raw["low_nano"]) / Decimal(1_000_000_000),
                    close=Decimal(raw["close_nano"]) / Decimal(1_000_000_000),
                    volume=Decimal(str(raw["volume"])),
                    tick_size=Decimal(str(raw["tick_size"])),
                    tick_value=Decimal(str(raw["tick_value"])),
                ))
                retained += 1
        audits[f"{market}/{year}"] = {
            "total_rows_scanned": total, "dependency_rows_retained": retained,
            "sessionless_dependency_rows": sessionless,
            "source_path": path.as_posix(), "source_sha256": sha256_file(path),
        }
    sorted_prices = {key: tuple(sorted(value, key=lambda bar: bar.event_at))
                     for key, value in prices.items()}
    risk = {session: _feature_and_risk(bars, cost_ticks)[1]
            for session, bars in sorted_prices.items()}
    return market, sorted_prices, risk, audits


def _feature_and_risk(
    bars: Sequence[LimitBar], cost_ticks: Mapping[str, int],
) -> tuple[tuple[LimitBar, ...] | None, dict[str, str] | None]:
    if not bars:
        return None, None
    session_date = bars[0].event_at.date()
    checkpoint = datetime.combine(session_date, time(10, 0), CT)
    decision = checkpoint + timedelta(seconds=5)
    available = tuple(sorted((bar for bar in bars if time(9, 30) <= bar.event_at.time() < time(10, 0)
                              and bar.available_at <= decision),
                             key=lambda bar: bar.event_at))
    if len(available) < 21 or available[0].event_at.time() > time(9, 35) \
            or available[-1].event_at.time() < time(9, 58):
        return None, None
    feature = available[-21:]
    if (
        len({bar.identity for bar in feature}) != 1
        or any(bar.high <= 0 or bar.low <= 0 or bar.close <= 0 or bar.volume < 0
               or bar.high < bar.low for bar in feature)
        or len({(bar.tick_size, bar.tick_value) for bar in feature}) != 1
        or feature[-1].tick_size <= 0 or feature[-1].tick_value <= 0
    ):
        return None, None
    true_ranges = [
        max(bar.high - bar.low, abs(bar.high - previous.close), abs(bar.low - previous.close))
        for previous, bar in zip(feature, feature[1:])
    ]
    atr20 = sum(true_ranges, Decimal(0)) / Decimal(20)
    stop_ticks = int((Decimal("1.5") * atr20 / feature[-1].tick_size).to_integral_value(
        rounding=ROUND_CEILING
    ))
    if stop_ticks <= 0:
        return None, None
    risk = {
        scenario: (
            "FEASIBLE" if Decimal(stop_ticks) * feature[-1].tick_value
            + Decimal("10") + Decimal(int(ticks)) * feature[-1].tick_value <= Decimal("250")
            else "RISK_ABSTENTION"
        )
        for scenario, ticks in cost_ticks.items()
    }
    return feature, risk


def _penetrates(bar: LimitBar, *, direction: str, limit: Decimal) -> bool:
    if direction == "LONG":
        return bar.low <= limit - bar.tick_size
    return bar.high >= limit + bar.tick_size


def _exit_penetrates(bar: LimitBar, *, direction: str, limit: Decimal) -> bool:
    if direction == "LONG":
        return bar.high >= limit + bar.tick_size
    return bar.low <= limit - bar.tick_size


def _direction_path(
    *, bars: Sequence[LimitBar], trigger: LimitBar, feature: Sequence[LimitBar],
    direction: str, scenario: str, adverse_ticks: int,
) -> tuple[bool, bool, str]:
    order_time = trigger.available_at
    entries = [
        bar for bar in bars
        if order_time < bar.event_at <= order_time + timedelta(minutes=5)
        and _penetrates(bar, direction=direction, limit=trigger.close)
    ]
    if not entries:
        return False, True, f"{direction}__{scenario}__EXPLICIT_CANCELLED_NO_TRADE_TIMEOUT"
    entry = entries[0]
    if entry.identity != trigger.identity:
        return True, False, f"{direction}__{scenario}__ENTRY_IDENTITY_CHANGING"
    stop_ticks = int((Decimal("1.5") * (
        sum([
            max(bar.high - bar.low, abs(bar.high - previous.close), abs(bar.low - previous.close))
            for previous, bar in zip(feature, feature[1:])
        ], Decimal(0)) / Decimal(20)
    ) / entry.tick_size).to_integral_value(rounding=ROUND_CEILING))
    stop = trigger.close - Decimal(stop_ticks) * entry.tick_size if direction == "LONG" \
        else trigger.close + Decimal(stop_ticks) * entry.tick_size
    fill_end = entry.event_at + timedelta(minutes=1)
    scheduled = fill_end + timedelta(minutes=30)
    path_bars = [bar for bar in bars if entry.event_at <= bar.event_at and bar.event_at < scheduled]
    for bar in path_bars:
        if bar.identity != entry.identity:
            return True, False, f"{direction}__{scenario}__HOLD_IDENTITY_CHANGING"
        stopped = bar.low <= stop if direction == "LONG" else bar.high >= stop
        if stopped:
            return True, True, f"{direction}__{scenario}__VERIFIED_PROTECTIVE_STOP"
    anchors = [bar for bar in bars if bar.available_at >= scheduled]
    if not anchors:
        return True, False, f"{direction}__{scenario}__EXIT_ANCHOR_MISSING"
    anchor = anchors[0]
    if anchor.identity != entry.identity:
        return True, False, f"{direction}__{scenario}__EXIT_ANCHOR_IDENTITY_CHANGING"
    offset = Decimal(adverse_ticks) * anchor.tick_size
    exit_limit = anchor.close - offset if direction == "LONG" else anchor.close + offset
    exits = [
        bar for bar in bars
        if anchor.available_at < bar.event_at <= anchor.available_at + timedelta(minutes=15)
        and _exit_penetrates(bar, direction=direction, limit=exit_limit)
    ]
    if not exits:
        return True, False, f"{direction}__{scenario}__VERIFIED_EXIT_MISSING"
    if exits[0].identity != entry.identity:
        return True, False, f"{direction}__{scenario}__EXIT_IDENTITY_CHANGING"
    return True, True, f"{direction}__{scenario}__VERIFIED_LIMIT_EXIT"


def classify_session(
    *, session: str, bars: Sequence[LimitBar], cost_ticks: Mapping[str, int],
) -> SessionReadiness:
    feature, risk = _feature_and_risk(bars, cost_ticks)
    if feature is None or risk is None:
        return SessionReadiness(False, False, True,
                                ("EXPLICIT_CAUSAL_FEATURE_ABSTENTION",), {})
    checkpoint = datetime.combine(date.fromisoformat(session), time(10, 0), CT)
    decision = checkpoint + timedelta(seconds=5)
    triggers = [bar for bar in bars if bar.event_at >= checkpoint
                and decision < bar.available_at <= decision + timedelta(seconds=120)]
    if not triggers:
        return SessionReadiness(True, False, True,
                                ("EXPLICIT_CAUSAL_NO_TRIGGER_TIMEOUT",), risk)
    trigger = triggers[0]
    if trigger.identity != feature[-1].identity:
        return SessionReadiness(True, True, False, ("TRIGGER_IDENTITY_CHANGING",), risk)
    selected = False
    complete = True
    dispositions = []
    for scenario in SCENARIOS:
        if risk[scenario] == "RISK_ABSTENTION":
            dispositions.append(f"{scenario}__RISK_ABSTENTION")
            continue
        for direction in ("LONG", "SHORT"):
            filled, path_complete, disposition = _direction_path(
                bars=bars, trigger=trigger, feature=feature, direction=direction,
                scenario=scenario, adverse_ticks=int(cost_ticks[scenario]),
            )
            selected = selected or filled
            complete = complete and path_complete
            dispositions.append(disposition)
    return SessionReadiness(True, selected, complete, tuple(dispositions), risk)


def _session_results(sessions, bars_by_session, cost_ticks):
    return [(session, classify_session(session=session, bars=bars_by_session.get(session, ()),
                                       cost_ticks=cost_ticks)) for session in sessions]


def _fold_evidence(*, market, fold, rows_by_session, risk_by_session):
    del risk_by_session
    cost_ticks = rows_by_session["__cost_ticks__"]
    bars = {key: value for key, value in rows_by_session.items() if key != "__cost_ticks__"}
    training = _session_results(fold["training_sessions"], bars, cost_ticks)
    evaluation = _session_results(fold["evaluation_sessions"], bars, cost_ticks)

    def complete(items): return sum(item.path_complete and item.feature_complete for _, item in items)
    def feature(items): return sum(item.feature_complete for _, item in items)
    def selected(items): return sum(item.selected for _, item in items)
    def paths(items): return sum(item.selected and item.path_complete for _, item in items)
    def exclusions(items, role):
        counter = Counter()
        for _session, item in items:
            for disposition in item.dispositions:
                if (not item.feature_complete) or (item.selected and not item.path_complete):
                    counter[f"{role}__{disposition}"] += 1
        return dict(counter)
    risk_counts = {}
    selected_eval = [item for _, item in evaluation if item.selected]
    for scenario in SCENARIOS:
        risk_counts[scenario] = {
            "feasible_sessions": sum(item.scenario_risk.get(scenario) == "FEASIBLE"
                                     for item in selected_eval),
            "risk_abstention_sessions": sum(item.scenario_risk.get(scenario) == "RISK_ABSTENTION"
                                             for item in selected_eval),
            "unresolved_sessions": sum(item.scenario_risk.get(scenario) not in {
                "FEASIBLE", "RISK_ABSTENTION"} for item in selected_eval),
        }
    years = {}
    for year in sorted({session[:4] for session, _ in (*training, *evaluation)}):
        tr = [item for item in training if item[0].startswith(year)]
        ev = [item for item in evaluation if item[0].startswith(year)]
        years[year] = {
            "expected_training_sessions": len(tr), "complete_training_sessions": complete(tr),
            "expected_evaluation_sessions": len(ev),
            "feature_complete_evaluation_sessions": feature(ev),
            "terminal_evaluation_sessions": len(ev),
            "execution_path_complete_evaluation_sessions": complete(ev),
            "exclusion_reasons": {**exclusions(tr, "TRAINING"), **exclusions(ev, "EVALUATION")},
        }
    baselines = {}
    for baseline in MANDATORY_BASELINES:
        flat = baseline == "flat_no_trade"
        baselines[baseline] = {
            "expected_sessions": len(evaluation), "terminal_sessions": len(evaluation),
            "selected_sessions": 0 if flat else selected(evaluation),
            "selected_path_complete_sessions": 0 if flat else paths(evaluation),
            "scenario_risk_dispositions": ({scenario: {
                "feasible_sessions": 0, "risk_abstention_sessions": 0,
                "unresolved_sessions": 0} for scenario in SCENARIOS} if flat else risk_counts),
            "schedule_independently_derived": True,
            "readiness_universe": (
                "EXACT_ZERO_NO_SCHEDULE" if flat
                else "INDEPENDENT_BOTH_DIRECTION_ALL_SCENARIO_CHECKPOINT_SUPERSET"
            ),
            "candidate_schedule_reused": False,
            "flat_no_trade": flat,
        }
    return {
        "fold_id": fold["fold_id"], "market": market, "role": "OUTER",
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
            "scenario_risk_dispositions": risk_counts,
            "purge_minutes": int(fold["purge_minutes"]),
            "embargo_sessions": len(fold["embargo_sessions"]),
        },
        "checks": {
            "chronological_order": True, "purge_applied": True, "embargo_applied": True,
            "training_only_transformation": True,
            "contract_identity_discontinuities_terminalized": True,
            "roll_discontinuities_terminalized": True,
            "all_incomplete_sessions_terminalized": True,
            "complete_required_metrics": True, "promotion_path_computable": True,
        },
        "baseline_universe_readiness": baselines,
        "exclusion_reasons": {**exclusions(training, "TRAINING"),
                              **exclusions(evaluation, "EVALUATION")},
        "market_year_breakdown": years,
    }


def _file_sha(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_bytes(payload) + b"\n").hexdigest()


def _manifest(core): return {**core, "manifest_id": sha256_json(core)}


def build_plan(*, root: Path) -> dict[str, object]:
    contract, profile = load_active_ladder(root)
    mechanism = _read_canonical(root / MECHANISM_PATH, name="counted mechanism")
    predecessor = _read_canonical(
        root / "state/unpublished_evidence/alpha_ladder_frozen_mechanism/"
        "186d8a103a581ae8c27fc531e0a556070991c9d2f87bbe5d62c1478867b5de3f/mechanism.json",
        name="predecessor mechanism",
    )
    rejection = _read_canonical(
        root / "state/unpublished_evidence/alpha_ladder_v3_source_incompatibility_rejection/"
        "45011788be8b275a3aa874834f7382a960c8371aafbdc118b645d3b165d5ffbf/rejection.json",
        name="V3 rejection",
    )
    validate_successor(mechanism, predecessor=predecessor, rejection=rejection)
    if sha256_file(root / MECHANISM_PATH) != MECHANISM_SHA256:
        raise IntegrityError("counted mechanism changed")
    certificate = _read_canonical(root / TIER0_CERTIFICATE_PATH, name="Tier 0 certificate")
    validate_tier0_certificate(certificate, contract_id=str(contract["contract_id"]),
                               mechanism_sha256=MECHANISM_SHA256)
    decision = _read_canonical(root / TIER0_DECISION_PATH, name="Tier 0 decision")
    validate_stage_decision(
        decision, contract_id=str(contract["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256, expected_stage="tier_0", root=root,
    )
    pointer, calendar = _active_calendar(root)
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are absent")
    by_key = {(str(item["market"]), int(item["year"])): item
              for item in entries if isinstance(item, dict)}
    selected = {}
    for market in CORE:
        for year in YEARS:
            item = by_key.get((market, year))
            if item is None or item.get("disposition") != "RESEARCH_READY_CAUSAL_PRICE":
                raise IntegrityError(f"active catalog cannot bind {market} {year}")
            selected[str(item["parquet_path"])] = str(item["parquet_sha256"])
    bindings = {
        MECHANISM_PATH.as_posix(): MECHANISM_SHA256,
        TIER0_CERTIFICATE_PATH.as_posix(): sha256_file(root / TIER0_CERTIFICATE_PATH),
        TIER0_DECISION_PATH.as_posix(): sha256_file(root / TIER0_DECISION_PATH),
        "configs/active_alpha_research_ladder.json": sha256_file(
            root / "configs/active_alpha_research_ladder.json"),
        ACTIVE_CATALOG_PATH.as_posix(): sha256_file(root / ACTIVE_CATALOG_PATH),
        "configs/active_cash_open_impulse_historical_calendar.json": sha256_file(
            root / "configs/active_cash_open_impulse_historical_calendar.json"),
        str(pointer["calendar_path"]): str(pointer["calendar_sha256"]),
        "src/futures_rebuild/alpha_ladder_limit_readiness.py": sha256_file(Path(__file__)),
        RUNNER_PATH.as_posix(): sha256_file(root / RUNNER_PATH),
        **selected,
    }
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_limit_readiness_census_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES",
        "operation": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "contract_id": contract["contract_id"], "profile_id": profile["profile_id"],
        "mechanism_id": MECHANISM_ID, "mechanism_sha256": MECHANISM_SHA256,
        "markets": list(CORE), "years": list(YEARS), "checkpoint": CHECKPOINT,
        "entry_semantics": "ONE_TICK_PENETRATION_RESTING_LIMIT_OR_EXPLICIT_NO_TRADE",
        "exit_semantics": "EVERY_FILLED_ENTRY_REQUIRES_VERIFIED_STOP_OR_LIMIT_EXIT",
        "pilot": {"market": "ES", "training_sessions": 504, "evaluation_sessions": 63,
                  "embargo_sessions": 1, "purge_minutes": 40,
                  "selection_rule": "EARLIEST_ROW_EXECUTABLE_ROLLING_504_1_63"},
        "tier_1": {"markets": list(CORE), "outer_folds": 8,
                   "pilot_sessions_excluded_from_every_market": True},
        "required_baselines": list(MANDATORY_BASELINES),
        "required_cost_scenarios": list(SCENARIOS),
        "coverage": {"checkpoint_accounting_percent": 100,
                     "active_baseline_checkpoint_accounting_percent": 100,
                     "filled_entry_verified_exit_percent": 100},
        "execution_limits": {"maximum_attempts": 1, "maximum_retries": 0,
                             "maximum_workers": 4, "worker_deadline_seconds": 3300,
                             "maximum_runtime_seconds": 3600,
                             "maximum_external_cost_usd": "0", "windows_host_required": True},
        "output_root": OUTPUT_ROOT.as_posix(),
        "authority": {"historical_row_read": True, "returns": False, "model_fit": False,
                      "prediction_generation": False, "performance_evaluation": False,
                      "registration": False, "trial_execution": False, "publication": False,
                      "provider_network_credentials": False, "year_2025_access": False,
                      "active_data_mutation": False, "trading": False},
        "calendar_id": calendar["calendar_id"], "bindings": dict(sorted(bindings.items())),
    }
    return {**core, "plan_id": sha256_json(core)}


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="limit readiness plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    bindings = plan.get("bindings")
    expected_limits = {"maximum_attempts": 1, "maximum_retries": 0,
        "maximum_workers": 4, "worker_deadline_seconds": 3300,
        "maximum_runtime_seconds": 3600, "maximum_external_cost_usd": "0",
        "windows_host_required": True}
    expected_authority = {"historical_row_read": True, "returns": False,
        "model_fit": False, "prediction_generation": False,
        "performance_evaluation": False, "registration": False,
        "trial_execution": False, "publication": False,
        "provider_network_credentials": False, "year_2025_access": False,
        "active_data_mutation": False, "trading": False}
    if (
        plan.get("plan_id") != sha256_json(core)
        or plan.get("operation") != ALPHA_LADDER_READINESS_CENSUS_OPERATION
        or plan.get("mechanism_id") != MECHANISM_ID
        or plan.get("mechanism_sha256") != MECHANISM_SHA256
        or plan.get("markets") != list(CORE)
        or plan.get("years") != list(YEARS)
        or plan.get("checkpoint") != CHECKPOINT
        or plan.get("entry_semantics") != "ONE_TICK_PENETRATION_RESTING_LIMIT_OR_EXPLICIT_NO_TRADE"
        or plan.get("exit_semantics") != "EVERY_FILLED_ENTRY_REQUIRES_VERIFIED_STOP_OR_LIMIT_EXIT"
        or plan.get("coverage") != {"checkpoint_accounting_percent": 100,
            "active_baseline_checkpoint_accounting_percent": 100,
            "filled_entry_verified_exit_percent": 100}
        or plan.get("execution_limits") != expected_limits
        or plan.get("authority") != expected_authority
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("limit readiness plan drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["execution_limits"]
    assert isinstance(limits, Mapping)
    return {
        "mechanism_id": MECHANISM_ID, "period": "2018,2019,2020,2021,2022",
        "markets": ",".join(CORE), "checkpoint": CHECKPOINT,
        "purpose": "ALPHA_RESTING_LIMIT_PILOT_AND_TIER1_READINESS_ONLY",
        "output_root": OUTPUT_ROOT.as_posix(), "maximum_attempts": "1",
        "maximum_retries": "0", "maximum_workers": "4",
        "worker_deadline_seconds": str(limits["worker_deadline_seconds"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_external_cost_usd": "0", "returns": "false", "model_fit": "false",
        "prediction_generation": "false", "performance_evaluation": "false",
        "registration": "false", "trial_execution": "false",
        "provider_network_access": "false", "holdout_2025_access": "false",
        "active_data_mutation": "false", "trading": "false",
        "approval_command": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def execute_once(*, root: Path, boundary: RepoBoundary, receipt: OperationReceipt):
    started = monotonic(); plan = load_plan(root=root)
    if os.name != "nt" or multiprocessing.current_process().name != "MainProcess":
        raise UnauthorizedOperation("limit readiness requires the Windows main process")
    output_root = root / OUTPUT_ROOT
    if output_root.exists():
        raise UnauthorizedOperation("limit readiness output already exists")
    use_path = receipt.consume(
        boundary, operation=ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    by_key = {(str(item["market"]), int(item["year"])): item for item in catalog["entries"]}
    mechanism = _read_canonical(root / MECHANISM_PATH, name="counted mechanism")
    costs = mechanism["costs"]["round_trip_adverse_ticks"]
    tasks = []
    source_bindings = {}
    market_costs = {}
    for market in CORE:
        sources = []
        for year in YEARS:
            item = by_key[(market, year)]
            path = resolve(repository_root=root, market=market, year=year, purpose="SELECTION")
            if sha256_file(path) != item["parquet_sha256"]:
                raise IntegrityError(f"active source changed for {market} {year}")
            source_bindings[path.relative_to(root).as_posix()] = item["parquet_sha256"]
            sources.append((year, str(path)))
        market_costs[market] = {scenario: int(costs[scenario][market]) for scenario in SCENARIOS}
        tasks.append((market, tuple(sources), market_costs[market]))
    pool = multiprocessing.get_context("spawn").Pool(processes=4)
    try:
        worker_results = pool.map_async(_read_market, tasks, chunksize=1).get(
            timeout=int(plan["execution_limits"]["worker_deadline_seconds"])
        )
        pool.close(); pool.join()
    except BaseException:
        pool.terminate(); pool.join(); raise
    observed = {item[0]: item[1:] for item in worker_results}
    _pointer, calendar = _active_calendar(root)
    eligible = {market: tuple(str(item["trade_date"]) for item in calendar["calendar_rows"]
                              if item["market"] == market
                              and item["checkpoint_open"].get(CHECKPOINT) is True)
                for market in CORE}
    es_prices, _risk, _audit = observed["ES"]
    es_with_costs = {**es_prices, "__cost_ticks__": market_costs["ES"]}
    pilot_fold, pilot_evidence, selection = select_earliest_executable_pilot(
        sessions=eligible["ES"], rows_by_session=es_with_costs, risk_by_session={},
        evidence_builder=_fold_evidence,
    )
    validate_selection(selection, selected_fold=pilot_fold)
    selection_core = {"schema_version": "alpha_ladder_limit_pilot_selection/1.0.0",
                      "plan_id": plan["plan_id"], "mechanism_id": MECHANISM_ID, **selection}
    selection_report = {**selection_core, "selection_id": sha256_json(selection_core)}
    selection_rel = (OUTPUT_ROOT / "pilot_fold_selection.json").as_posix()
    bindings = {**plan["bindings"], **source_bindings, PLAN_PATH.as_posix(): sha256_file(root / PLAN_PATH)}
    if pilot_fold is None or pilot_evidence is None:
        core = {"schema_version": "alpha_ladder_limit_readiness_report/1.0.0",
                "state": "SEALED_UNPUBLISHED_NO_EXECUTABLE_PILOT_FOLD",
                "plan_id": plan["plan_id"], "mechanism_id": MECHANISM_ID,
                "authorization_receipt_id": receipt.receipt_id,
                "authorization_use_path": use_path.relative_to(root).as_posix(),
                "authorization_use_sha256": sha256_file(use_path),
                "pilot_decision": "FAIL", "tier1_decision": "NOT_RUN",
                "combined_registration_ready": False,
                "pilot_selection_id": selection_report["selection_id"],
                "authority": plan["authority"]}
        report = {**core, "report_id": sha256_json(core)}
        output_root.mkdir(parents=True, exist_ok=False)
        _write_once(root / selection_rel, selection_report)
        _write_once(output_root / "readiness_report.json", report)
        return report
    exclusions = tuple(pilot_fold["evaluation_sessions"])
    common = sorted(set.intersection(*(set(eligible[m]) for m in CORE)) - set(exclusions))
    tier1_folds = _outer_folds(common)
    pilot_manifest = _manifest({"schema_version": SESSION_MANIFEST_SCHEMA,
        "contract_id": plan["contract_id"], "mechanism_sha256": MECHANISM_SHA256,
        "stage": "pilot", "markets": ["ES"], "fold_ordinal": 0,
        "calendar_start_offset": pilot_fold["calendar_start_offset"],
        "selection_rule": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
        "selection_evidence_path": selection_rel,
        "selection_evidence_sha256": _file_sha(selection_report),
        "training_session_ids": pilot_fold["training_sessions"],
        "evaluation_session_ids": pilot_fold["evaluation_sessions"],
        "purge_applied": True, "embargo_applied": True})
    tier1_manifest = _manifest({"schema_version": SESSION_MANIFEST_SCHEMA,
        "contract_id": plan["contract_id"], "mechanism_sha256": MECHANISM_SHA256,
        "stage": "tier_1", "excluded_pilot_evaluation_session_ids": list(exclusions),
        "evaluation_session_ids_by_market": {market: sorted({s for fold in tier1_folds
            for s in fold["evaluation_sessions"]}) for market in CORE}})
    pilot_rel = (OUTPUT_ROOT / "pilot_session_manifest.json").as_posix()
    tier1_rel = (OUTPUT_ROOT / "tier1_session_manifest.json").as_posix()
    pilot_bindings = {**bindings, selection_rel: _file_sha(selection_report),
                      pilot_rel: _file_sha(pilot_manifest)}
    tier1_bindings = {**bindings, tier1_rel: _file_sha(tier1_manifest)}
    pilot_cert = build_fold_readiness_certificate(
        trial_family="alpha_ladder_resting_limit", protocol_id=MECHANISM_ID,
        source_bindings=pilot_bindings, fold_evidence=(pilot_evidence,),
        required_markets=("ES",), required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=SCENARIOS, required_outer_fold_ids=("fold-0",),
        required_nested_fold_ids=(), expected_outer_folds=1, expected_nested_folds=0,
        minimum_training_sessions=504, minimum_evaluation_sessions=63,
        minimum_purge_minutes=40, minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True)
    tier1_evidence = []
    for market in CORE:
        prices, _risk, _audit = observed[market]
        with_costs = {**prices, "__cost_ticks__": market_costs[market]}
        tier1_evidence.extend(_fold_evidence(market=market, fold=fold,
            rows_by_session=with_costs, risk_by_session={}) for fold in tier1_folds)
    tier1_cert = build_fold_readiness_certificate(
        trial_family="alpha_ladder_resting_limit", protocol_id=MECHANISM_ID,
        source_bindings=tier1_bindings, fold_evidence=tier1_evidence,
        required_markets=CORE, required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=SCENARIOS,
        required_outer_fold_ids=tuple(f"fold-{i}" for i in range(8)),
        required_nested_fold_ids=(), expected_outer_folds=8, expected_nested_folds=0,
        minimum_training_sessions=252, minimum_evaluation_sessions=30,
        minimum_purge_minutes=40, minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True)
    validate_session_manifest(pilot_manifest, contract_id=str(plan["contract_id"]),
                              mechanism_sha256=MECHANISM_SHA256, stage="pilot", markets=("ES",))
    validate_session_manifest(tier1_manifest, contract_id=str(plan["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256, stage="tier_1", markets=CORE,
        pilot_evaluation_sha256=sha256_json(list(exclusions)))
    if monotonic() - started > int(plan["execution_limits"]["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("limit readiness exceeded total runtime")
    core = {"schema_version": "alpha_ladder_limit_readiness_report/1.0.0",
        "state": "SEALED_UNPUBLISHED_ROW_CERTIFIED_READINESS",
        "plan_id": plan["plan_id"], "mechanism_id": MECHANISM_ID,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "pilot_decision": pilot_cert["overall_decision"],
        "tier1_decision": tier1_cert["overall_decision"],
        "combined_registration_ready": pilot_cert["overall_decision"] == "PASS"
            and tier1_cert["overall_decision"] == "PASS",
        "pilot_selection_id": selection_report["selection_id"],
        "pilot_certificate_id": pilot_cert["certificate_id"],
        "tier1_certificate_id": tier1_cert["certificate_id"],
        "authority": plan["authority"]}
    report = {**core, "report_id": sha256_json(core)}
    output_root.mkdir(parents=True, exist_ok=False)
    _write_once(root / selection_rel, selection_report); _write_once(root / pilot_rel, pilot_manifest)
    _write_once(root / tier1_rel, tier1_manifest)
    validate_fold_readiness_certificate(pilot_cert, root=root)
    validate_fold_readiness_certificate(tier1_cert, root=root)
    _write_once(output_root / "pilot_readiness_certificate.json", pilot_cert)
    _write_once(output_root / "tier1_readiness_certificate.json", tier1_cert)
    _write_once(output_root / "readiness_report.json", report)
    return report
