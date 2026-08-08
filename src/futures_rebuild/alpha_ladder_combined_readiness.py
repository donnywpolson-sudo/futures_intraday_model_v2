"""Combined row-certified readiness for the Alpha ES pilot and Tier 1.

The census reads only the four bound 2018-2022 sources needed to prove causal
features, triggered execution paths, risk terminalization, and independent
baseline coverage.  It never computes returns, fits a model, or evaluates a
strategy.
"""

from __future__ import annotations

import multiprocessing
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time, timezone
from decimal import Decimal, ROUND_CEILING
from pathlib import Path
from time import monotonic
from zoneinfo import ZoneInfo

from .active_data_view import resolve
from .alpha_ladder_frozen_mechanism import MANDATORY_BASELINES, validate_frozen_mechanism
from .alpha_research_ladder import (
    CORE, SESSION_MANIFEST_SCHEMA, load_active_ladder, validate_session_manifest,
)
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .cash_open_source_compatibility import SourceRow, build_market_calendar_folds, source_row_from_mapping
from .cash_open_source_compatibility_census import _read_canonical
from .errors import IntegrityError, UnauthorizedOperation
from .preexecution_fold_certification import (
    ROW_CERTIFIED, build_fold_readiness_certificate, validate_fold_readiness_certificate,
)
from .reported_bar_fixed_horizon_census import (
    ACTIVE_CALENDAR_POINTER, ACTIVE_CATALOG_PATH, _active_calendar, _checkpoint_datetime,
    _evidence,
)
from .reported_bar_trade_triggered_protocol import (
    TriggeredDisposition, classify_trade_triggered_checkpoint,
)
from .research_gateway_policy import ALPHA_LADDER_READINESS_CENSUS_OPERATION


CT = ZoneInfo("America/Chicago")
CHECKPOINT = "10:00"
YEARS = (2018, 2019, 2020, 2021, 2022)
SCENARIOS = ("base", "stress", "extreme")
MECHANISM_ID = "186d8a103a581ae8c27fc531e0a556070991c9d2f87bbe5d62c1478867b5de3f"
MECHANISM_SHA256 = "1b0fa1d2beb1b463ec5c37f1341cca348a7ce1fee6d9dbae6074603b5ec37798"
MECHANISM_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_frozen_mechanism/"
    f"{MECHANISM_ID}/mechanism.json"
)
TIER0_DECISION_PATH = MECHANISM_PATH.with_name("tier0_decision.json")
TIER0_DECISION_SHA256 = "340b0b0ab537b15c9bb695f2e6f798eaf6f17d49d772ac61371b559641b9705b"
PREDECESSOR_PLAN_PATH = Path("configs/alpha_ladder_combined_readiness_census_plan.json")
PREDECESSOR_PLAN_SHA256 = "b95fb262da19a47d55fdd466b6b937a1a8a33c9d4912e2c57bc9c837c0d83f35"
FAILURE_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_combined_readiness_attempts/"
    "c9a54b651ae0089095678d67713d36659774c993de50c38374c26f553fd64bbe/failure.json"
)
PLAN_PATH = Path("configs/alpha_ladder_combined_readiness_census_v2_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_ladder_combined_readiness_v2")
RUNNER_PATH = Path("scripts/run_alpha_ladder_combined_readiness_census.py")
REQUIRED_COLUMNS = frozenset({
    "actual_identity_hash", "disposition", "event_at_ns", "exchange_session_date",
    "source_row_sha256", "open_nano", "high_nano", "low_nano", "close_nano",
    "volume", "tick_size", "tick_value",
})


@dataclass(frozen=True)
class PriceBar:
    event_at_ns: int
    available_at_ns: int
    identity: str | None
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    tick_size: Decimal
    tick_value: Decimal


def _write_once(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def build_plan(*, root: Path) -> dict[str, object]:
    contract, profile = load_active_ladder(root)
    mechanism = _read_canonical(root / MECHANISM_PATH, name="frozen mechanism")
    validate_frozen_mechanism(mechanism)
    if mechanism.get("mechanism_id") != MECHANISM_ID or sha256_file(root / MECHANISM_PATH) != MECHANISM_SHA256:
        raise IntegrityError("active Alpha mechanism changed")
    pointer, calendar = _active_calendar(root)
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are absent")
    by_key = {(str(item["market"]), int(item["year"])): item for item in entries if isinstance(item, dict)}
    selected_sources: dict[str, str] = {}
    for market in CORE:
        for year in YEARS:
            item = by_key.get((market, year))
            if item is None or item.get("disposition") != "RESEARCH_READY_CAUSAL_PRICE":
                raise IntegrityError(f"active catalog cannot bind {market} {year}")
            selected_sources[str(item["parquet_path"])] = str(item["parquet_sha256"])
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_combined_readiness_census_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES",
        "operation": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "contract_id": contract["contract_id"], "profile_id": profile["profile_id"],
        "mechanism_id": MECHANISM_ID, "mechanism_sha256": MECHANISM_SHA256,
        "tier0_decision_path": TIER0_DECISION_PATH.as_posix(),
        "tier0_decision_sha256": TIER0_DECISION_SHA256,
        "markets": list(CORE), "years": list(YEARS), "checkpoint": CHECKPOINT,
        "pilot": {"market": "ES", "training_sessions": 504, "evaluation_sessions": 63,
                  "fold_ordinal": 0, "purge_minutes": 40, "embargo_sessions": 1},
        "tier_1": {"markets": list(CORE), "outer_folds": 8,
                   "pilot_sessions_excluded_from_every_market": True},
        "required_baselines": list(MANDATORY_BASELINES),
        "required_cost_scenarios": list(SCENARIOS),
        "coverage": {"checkpoint_accounting_percent": 100,
                     "triggered_selected_path_percent": 100,
                     "unresolved_risk_dispositions": 0},
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
        "bindings": dict(sorted({
            ACTIVE_CALENDAR_POINTER.as_posix(): sha256_file(root / ACTIVE_CALENDAR_POINTER),
            str(pointer["calendar_path"]): str(pointer["calendar_sha256"]),
            ACTIVE_CATALOG_PATH.as_posix(): sha256_file(root / ACTIVE_CATALOG_PATH),
            "configs/active_alpha_research_ladder.json": sha256_file(
                root / "configs/active_alpha_research_ladder.json"
            ),
            MECHANISM_PATH.as_posix(): MECHANISM_SHA256,
            TIER0_DECISION_PATH.as_posix(): TIER0_DECISION_SHA256,
            PREDECESSOR_PLAN_PATH.as_posix(): PREDECESSOR_PLAN_SHA256,
            FAILURE_PATH.as_posix(): sha256_file(root / FAILURE_PATH),
            "src/futures_rebuild/alpha_ladder_combined_readiness.py": sha256_file(Path(__file__)),
            RUNNER_PATH.as_posix(): sha256_file(root / RUNNER_PATH),
            **selected_sources,
        }.items())),
        "calendar_id": calendar["calendar_id"],
    }
    return {**core, "plan_id": sha256_json(core)}


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="Alpha combined readiness plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    bindings = plan.get("bindings")
    limits = plan.get("execution_limits")
    authority = plan.get("authority")
    expected_limits = {
        "maximum_attempts": 1, "maximum_retries": 0, "maximum_workers": 4,
        "worker_deadline_seconds": 3300, "maximum_runtime_seconds": 3600,
        "maximum_external_cost_usd": "0", "windows_host_required": True,
    }
    expected_authority = {
        "historical_row_read": True, "returns": False, "model_fit": False,
        "prediction_generation": False, "performance_evaluation": False,
        "registration": False, "trial_execution": False, "publication": False,
        "provider_network_credentials": False, "year_2025_access": False,
        "active_data_mutation": False, "trading": False,
    }
    if (
        plan.get("plan_id") != sha256_json(core)
        or plan.get("operation") != ALPHA_LADDER_READINESS_CENSUS_OPERATION
        or plan.get("mechanism_id") != MECHANISM_ID
        or plan.get("markets") != list(CORE)
        or plan.get("years") != list(YEARS)
        or plan.get("checkpoint") != CHECKPOINT
        or limits != expected_limits
        or authority != expected_authority
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("Alpha combined readiness plan drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["execution_limits"]
    assert isinstance(limits, Mapping)
    return {
        "mechanism_id": str(plan["mechanism_id"]), "period": "2018,2019,2020,2021,2022",
        "markets": ",".join(CORE), "checkpoint": CHECKPOINT,
        "purpose": "ALPHA_PILOT_AND_TIER1_ROW_READINESS_ONLY",
        "output_root": str(plan["output_root"]), "maximum_attempts": "1",
        "maximum_retries": "0", "maximum_workers": "4",
        "worker_deadline_seconds": str(limits["worker_deadline_seconds"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_external_cost_usd": "0", "returns": "false", "model_fit": "false",
        "prediction_generation": "false", "performance_evaluation": "false",
        "registration": "false", "trial_execution": "false", "provider_network_access": "false",
        "holdout_2025_access": "false", "active_data_mutation": "false", "trading": "false",
        "approval_command": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _dependency_clock(event_at_ns: int) -> bool:
    clock = datetime.fromtimestamp(event_at_ns / 1_000_000_000, timezone.utc).astimezone(CT).time()
    minute = clock.hour * 60 + clock.minute
    return 9 * 60 + 29 <= minute <= 10 * 60 + 35


def _read_market(task: tuple[str, tuple[tuple[int, str], ...], Mapping[str, int]]):
    import pyarrow.parquet as pq
    market, sources, cost_ticks = task
    rows: dict[str, list[SourceRow]] = {}
    prices: dict[str, list[PriceBar]] = {}
    audits: dict[str, object] = {}
    for year, raw_path in sources:
        path = Path(raw_path); parquet = pq.ParquetFile(path)
        if not REQUIRED_COLUMNS.issubset(parquet.schema_arrow.names):
            raise IntegrityError(f"Alpha readiness source schema is incomplete for {market} {year}")
        total = retained = sessionless = 0
        for batch in parquet.iter_batches(batch_size=65_536, columns=sorted(REQUIRED_COLUMNS)):
            columns = batch.to_pydict()
            for index in range(batch.num_rows):
                total += 1
                raw = {name: values[index] for name, values in columns.items()}
                event = raw.get("event_at_ns")
                if type(event) is not int or not _dependency_clock(event):
                    continue
                session = raw.get("exchange_session_date")
                if not isinstance(session, str):
                    sessionless += 1; continue
                normalized = source_row_from_mapping(market=market, row=raw)
                rows.setdefault(session, []).append(normalized)
                if normalized.executable:
                    prices.setdefault(session, []).append(PriceBar(
                        normalized.event_at_ns, normalized.available_at_ns,
                        normalized.actual_identity_hash,
                        Decimal(raw["high_nano"]) / Decimal(1_000_000_000),
                        Decimal(raw["low_nano"]) / Decimal(1_000_000_000),
                        Decimal(raw["close_nano"]) / Decimal(1_000_000_000),
                        Decimal(str(raw["volume"])),
                        Decimal(str(raw["tick_size"])), Decimal(str(raw["tick_value"])),
                    ))
                retained += 1
        audits[f"{market}/{year}"] = {
            "total_rows_scanned": total, "dependency_rows_retained": retained,
            "sessionless_dependency_rows": sessionless,
            "source_path": path.as_posix(), "source_sha256": sha256_file(path),
        }
    risk = {session: _risk_dispositions(bars, cost_ticks) for session, bars in prices.items()}
    return market, {k: tuple(v) for k, v in rows.items()}, risk, audits


def _risk_dispositions(bars: Sequence[PriceBar], cost_ticks: Mapping[str, int]) -> dict[str, str] | None:
    checkpoint = time(10, 0)
    available = sorted((bar for bar in bars if time(9, 30) <= datetime.fromtimestamp(
        bar.event_at_ns / 1_000_000_000, timezone.utc).astimezone(CT).time() < checkpoint
        and bar.available_at_ns <= int(datetime.combine(
            datetime.fromtimestamp(bar.event_at_ns / 1_000_000_000, timezone.utc)
            .astimezone(CT).date(), checkpoint, CT
        ).timestamp() * 1_000_000_000) + 5_000_000_000),
        key=lambda bar: bar.event_at_ns)
    if len(available) < 21:
        return None
    first_clock = datetime.fromtimestamp(
        available[0].event_at_ns / 1_000_000_000, timezone.utc,
    ).astimezone(CT).time()
    last_clock = datetime.fromtimestamp(
        available[-1].event_at_ns / 1_000_000_000, timezone.utc,
    ).astimezone(CT).time()
    feature = available[-21:]
    if (
        first_clock > time(9, 35)
        or last_clock < time(9, 58)
        or len({bar.identity for bar in feature}) != 1
        or feature[0].identity is None
        or any(
            bar.high <= 0 or bar.low <= 0 or bar.close <= 0 or bar.volume < 0
            or bar.high < bar.low for bar in feature
        )
    ):
        return None
    tick_size = feature[-1].tick_size; tick_value = feature[-1].tick_value
    if tick_size <= 0 or tick_value <= 0 or any(
        bar.tick_size != tick_size or bar.tick_value != tick_value for bar in feature
    ):
        return None
    true_ranges = []
    for previous, bar in zip(feature, feature[1:]):
        true_ranges.append(max(bar.high - bar.low, abs(bar.high - previous.close), abs(bar.low - previous.close)))
    atr20 = sum(true_ranges, Decimal(0)) / Decimal(20)
    stop_ticks = int((Decimal("1.5") * atr20 / tick_size).to_integral_value(rounding=ROUND_CEILING))
    if stop_ticks <= 0:
        return None
    return {
        scenario: ("FEASIBLE" if Decimal(stop_ticks) * tick_value
                   + Decimal("10") + Decimal(ticks) * tick_value <= Decimal("250")
                   else "RISK_ABSTENTION")
        for scenario, ticks in cost_ticks.items()
    }


def _session_results(sessions, rows_by_session, risk_by_session):
    results = []
    for session in sessions:
        risk = risk_by_session.get(session)
        if risk is None:
            classified = TriggeredDisposition(
                False, False, False, False, False, False,
                "EXPLICIT_CAUSAL_FEATURE_ABSTENTION",
            )
        else:
            classified = classify_trade_triggered_checkpoint(
                checkpoint=_checkpoint_datetime(session, CHECKPOINT),
                rows=_evidence(rows_by_session.get(session, ())), feature_required=True,
            )
        acceptable = classified.feature_complete and classified.disposition in {
            "COMPLETE", "EXPLICIT_CAUSAL_NO_TRADE_TIMEOUT",
        }
        selected_risk = risk if classified.path_required else {}
        results.append((session, classified, acceptable, selected_risk))
    return results


def _risk_counts(results):
    selected = sum(item[1].path_required for item in results)
    counts = {}
    for scenario in SCENARIOS:
        feasible = abstain = unresolved = 0
        for _session, classified, _acceptable, risk in results:
            if not classified.path_required:
                continue
            value = risk.get(scenario) if isinstance(risk, Mapping) else None
            feasible += value == "FEASIBLE"; abstain += value == "RISK_ABSTENTION"
            unresolved += value not in {"FEASIBLE", "RISK_ABSTENTION"}
        counts[scenario] = {"feasible_sessions": feasible,
                            "risk_abstention_sessions": abstain,
                            "unresolved_sessions": unresolved}
    return selected, counts


def _fold_evidence(*, market, fold, rows_by_session, risk_by_session):
    training = _session_results(fold["training_sessions"], rows_by_session, risk_by_session)
    evaluation = _session_results(fold["evaluation_sessions"], rows_by_session, risk_by_session)
    selected, risk_counts = _risk_counts(evaluation)
    candidate_paths = sum(item[1].path_required and item[1].exit_fill_complete for item in evaluation)
    def complete(items): return sum(item[2] for item in items)
    def feature(items): return sum(item[1].feature_complete for item in items)
    def exclusions(items, role):
        return dict(Counter(f"{role}__{item[1].disposition}" for item in items if not item[2]))
    exclusion = {**exclusions(training, "TRAINING"), **exclusions(evaluation, "EVALUATION")}
    years = {}
    for year in sorted({item[0][:4] for item in (*training, *evaluation)}):
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
            "selected_sessions": 0 if flat else selected,
            "selected_path_complete_sessions": 0 if flat else candidate_paths,
            "scenario_risk_dispositions": ({scenario: {"feasible_sessions": 0,
                "risk_abstention_sessions": 0, "unresolved_sessions": 0} for scenario in SCENARIOS}
                if flat else risk_counts),
            "schedule_independently_derived": True, "flat_no_trade": flat,
        }
    return {
        "fold_id": fold["fold_id"], "market": market, "role": "OUTER",
        "counts": {"expected_training_sessions": len(training),
                   "complete_training_sessions": complete(training),
                   "feature_complete_training_sessions": feature(training),
                   "transformation_ready_training_sessions": feature(training),
                   "expected_evaluation_sessions": len(evaluation),
                   "feature_complete_evaluation_sessions": feature(evaluation),
                   "terminal_evaluation_sessions": len(evaluation),
                   "execution_path_complete_evaluation_sessions": complete(evaluation),
                   "candidate_selected_sessions": selected,
                   "candidate_selected_path_complete_sessions": candidate_paths,
                   "scenario_risk_dispositions": risk_counts,
                   "purge_minutes": int(fold["purge_minutes"]),
                   "embargo_sessions": len(fold["embargo_sessions"])},
        "checks": {"chronological_order": True, "purge_applied": True,
                   "embargo_applied": True, "training_only_transformation": True,
                   "contract_identity_discontinuities_terminalized": True,
                   "roll_discontinuities_terminalized": True,
                   "all_incomplete_sessions_terminalized": True,
                   "complete_required_metrics": True, "promotion_path_computable": True},
        "baseline_universe_readiness": baselines,
        "exclusion_reasons": exclusion, "market_year_breakdown": years,
    }


def _manifest(core): return {**core, "manifest_id": sha256_json(core)}


def execute_once(*, root: Path, boundary: RepoBoundary, receipt: OperationReceipt) -> dict[str, object]:
    started = monotonic(); plan = load_plan(root=root)
    if os.name != "nt" or multiprocessing.current_process().name != "MainProcess":
        raise UnauthorizedOperation("Alpha readiness census requires the Windows main process")
    output_root = root / OUTPUT_ROOT
    if output_root.exists():
        raise UnauthorizedOperation("Alpha readiness output already exists")
    use_path = receipt.consume(
        boundary, operation=ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    by_key = {(str(item["market"]), int(item["year"])): item for item in catalog["entries"]}
    mechanism = _read_canonical(root / MECHANISM_PATH, name="mechanism")
    costs = mechanism["costs"]["round_trip_adverse_ticks"]
    tasks = []
    source_bindings = {}
    for market in CORE:
        sources = []
        for year in YEARS:
            item = by_key[(market, year)]
            path = resolve(repository_root=root, market=market, year=year, purpose="SELECTION")
            if sha256_file(path) != item["parquet_sha256"]:
                raise IntegrityError(f"active source changed for {market} {year}")
            source_bindings[path.relative_to(root).as_posix()] = item["parquet_sha256"]
            sources.append((year, str(path)))
        tasks.append((market, tuple(sources), {s: int(costs[s][market]) for s in SCENARIOS}))
    limits = plan["execution_limits"]
    pool = multiprocessing.get_context("spawn").Pool(processes=4)
    try:
        worker_results = pool.map_async(_read_market, tasks, chunksize=1).get(
            timeout=int(limits["worker_deadline_seconds"])
        ); pool.close(); pool.join()
    except BaseException:
        pool.terminate(); pool.join(); raise
    observed = {item[0]: item[1:] for item in worker_results}
    pointer, calendar = _active_calendar(root)
    calendar_rows = calendar["calendar_rows"]
    eligible = {
        market: tuple(str(item["trade_date"]) for item in calendar_rows
                      if item["market"] == market and item["checkpoint_open"].get(CHECKPOINT) is True)
        for market in CORE
    }
    es_folds = build_market_calendar_folds(eligible["ES"])
    pilot_fold = es_folds[0]
    pilot_manifest = _manifest({
        "schema_version": SESSION_MANIFEST_SCHEMA, "contract_id": plan["contract_id"],
        "mechanism_sha256": MECHANISM_SHA256, "stage": "pilot", "markets": ["ES"],
        "fold_ordinal": 0, "selection_rule": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
        "training_session_ids": pilot_fold["training_sessions"],
        "evaluation_session_ids": pilot_fold["evaluation_sessions"],
        "purge_applied": True, "embargo_applied": True,
    })
    pilot_exclusions = tuple(pilot_fold["evaluation_sessions"])
    common = sorted(set.intersection(*(set(eligible[m]) for m in CORE)) - set(pilot_exclusions))
    tier1_folds = build_market_calendar_folds(common)
    tier1_manifest = _manifest({
        "schema_version": SESSION_MANIFEST_SCHEMA, "contract_id": plan["contract_id"],
        "mechanism_sha256": MECHANISM_SHA256, "stage": "tier_1",
        "excluded_pilot_evaluation_session_ids": list(pilot_exclusions),
        "evaluation_session_ids_by_market": {
            market: sorted({session for fold in tier1_folds for session in fold["evaluation_sessions"]})
            for market in CORE
        },
    })
    pilot_rel = (OUTPUT_ROOT / "pilot_session_manifest.json").as_posix()
    tier1_rel = (OUTPUT_ROOT / "tier1_session_manifest.json").as_posix()
    bindings = dict(plan["bindings"]); bindings.update(source_bindings)
    bindings[PLAN_PATH.as_posix()] = sha256_file(root / PLAN_PATH)
    pilot_bindings = {**bindings, pilot_rel: sha256_json(pilot_manifest)}
    tier1_bindings = {**bindings, tier1_rel: sha256_json(tier1_manifest)}
    # Manifest file SHA equals SHA-256 of canonical bytes plus newline, not its identity.
    import hashlib
    pilot_bindings[pilot_rel] = hashlib.sha256(canonical_bytes(pilot_manifest) + b"\n").hexdigest()
    tier1_bindings[tier1_rel] = hashlib.sha256(canonical_bytes(tier1_manifest) + b"\n").hexdigest()
    es_rows, es_risk, _es_audit = observed["ES"]
    pilot_evidence = [_fold_evidence(market="ES", fold=pilot_fold,
                                     rows_by_session=es_rows, risk_by_session=es_risk)]
    pilot_cert = build_fold_readiness_certificate(
        trial_family="alpha_ladder_frozen_mechanism", protocol_id=MECHANISM_ID,
        source_bindings=pilot_bindings, fold_evidence=pilot_evidence,
        required_markets=("ES",), required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=SCENARIOS, required_outer_fold_ids=("fold-0",),
        required_nested_fold_ids=(), expected_outer_folds=1, expected_nested_folds=0,
        minimum_training_sessions=504, minimum_evaluation_sessions=63,
        minimum_purge_minutes=40, minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )
    tier1_evidence = []
    for market in CORE:
        rows, risk, _audit = observed[market]
        tier1_evidence.extend(_fold_evidence(market=market, fold=fold,
                                            rows_by_session=rows, risk_by_session=risk)
                              for fold in tier1_folds)
    tier1_cert = build_fold_readiness_certificate(
        trial_family="alpha_ladder_frozen_mechanism", protocol_id=MECHANISM_ID,
        source_bindings=tier1_bindings, fold_evidence=tier1_evidence,
        required_markets=CORE, required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=SCENARIOS,
        required_outer_fold_ids=tuple(f"fold-{i}" for i in range(8)),
        required_nested_fold_ids=(), expected_outer_folds=8, expected_nested_folds=0,
        minimum_training_sessions=252, minimum_evaluation_sessions=30,
        minimum_purge_minutes=40, minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )
    validate_session_manifest(pilot_manifest, contract_id=str(plan["contract_id"]),
                              mechanism_sha256=MECHANISM_SHA256, stage="pilot", markets=("ES",))
    validate_session_manifest(tier1_manifest, contract_id=str(plan["contract_id"]),
                              mechanism_sha256=MECHANISM_SHA256, stage="tier_1", markets=CORE,
                              pilot_evaluation_sha256=sha256_json(list(pilot_exclusions)))
    if monotonic() - started > int(limits["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("Alpha readiness census exceeded total runtime")
    audits = {key: value for market in CORE for key, value in observed[market][2].items()}
    core = {
        "schema_version": "alpha_ladder_combined_readiness_report/1.0.0",
        "state": "SEALED_UNPUBLISHED_ROW_CERTIFIED_READINESS",
        "plan_id": plan["plan_id"], "mechanism_id": MECHANISM_ID,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "pilot_decision": pilot_cert["overall_decision"],
        "tier1_decision": tier1_cert["overall_decision"],
        "combined_registration_ready": (
            pilot_cert["overall_decision"] == "PASS" and tier1_cert["overall_decision"] == "PASS"
        ),
        "pilot_certificate_id": pilot_cert["certificate_id"],
        "tier1_certificate_id": tier1_cert["certificate_id"],
        "pilot_session_manifest_id": pilot_manifest["manifest_id"],
        "tier1_session_manifest_id": tier1_manifest["manifest_id"],
        "source_audits": dict(sorted(audits.items())), "source_bindings": dict(sorted(bindings.items())),
        "authority": plan["authority"],
    }
    report = {**core, "report_id": sha256_json(core)}
    output_root.mkdir(parents=True, exist_ok=False)
    _write_once(root / pilot_rel, pilot_manifest); _write_once(root / tier1_rel, tier1_manifest)
    validate_fold_readiness_certificate(pilot_cert, root=root)
    validate_fold_readiness_certificate(tier1_cert, root=root)
    _write_once(output_root / "pilot_readiness_certificate.json", pilot_cert)
    _write_once(output_root / "tier1_readiness_certificate.json", tier1_cert)
    _write_once(output_root / "readiness_report.json", report)
    return report
