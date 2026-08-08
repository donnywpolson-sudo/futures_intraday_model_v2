"""Price-free source census for the fixed-horizon reported-bar protocol."""

from __future__ import annotations

import json
import multiprocessing
import os
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from time import monotonic
from zoneinfo import ZoneInfo

from .active_data_view import resolve
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .cash_open_source_compatibility import SourceRow, build_market_calendar_folds, source_row_from_mapping
from .cash_open_source_compatibility_census import _read_canonical
from .errors import IntegrityError, UnauthorizedOperation
from .reported_bar_fixed_horizon_protocol import (
    CHECKPOINTS,
    DiscoveryDisposition,
    ReportedBarEvidence,
    classify_reported_bar_checkpoint,
)
from .research_gateway_policy import SOURCE_COMPATIBILITY_CENSUS_OPERATION


PROTOCOL_PATH = Path("configs/reported_bar_fixed_horizon_source_discovery_protocol_v2.json")
PROTOCOL_ID = "29f3384c814a967c2e69c1433e62a9322e37bc1b3596e43334b05c698987321a"
PROTOCOL_SHA256 = "f2d6b5bffe65a46827cadfc45dfbdcebe37131cd4ba7cc7bcabe78f1b4585b47"
PLAN_PATH = Path("configs/reported_bar_fixed_horizon_source_census_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/reported_bar_fixed_horizon_source_census")
ACTIVE_CALENDAR_POINTER = Path("configs/active_cash_open_impulse_historical_calendar.json")
ACTIVE_CATALOG_PATH = Path("data/active/catalog.json")
RUNNER_PATH = Path("scripts/run_reported_bar_fixed_horizon_source_census.py")
REQUIRED_COLUMNS = frozenset(
    {"actual_identity_hash", "disposition", "event_at_ns", "exchange_session_date", "source_row_sha256"}
)
CT = ZoneInfo("America/Chicago")
ACTIVE_BASELINES = (
    "ALWAYS_LONG",
    "ALWAYS_SHORT",
    "REPORTED_BAR_CONTINUATION",
    "REPORTED_BAR_REVERSAL",
)


def _protocol(root: Path) -> dict[str, object]:
    path = root / PROTOCOL_PATH
    if sha256_file(path) != PROTOCOL_SHA256:
        raise IntegrityError("reported-bar protocol hash drifted")
    payload = _read_canonical(path, name="reported-bar source protocol")
    core = {key: value for key, value in payload.items() if key != "protocol_id"}
    if payload.get("protocol_id") != PROTOCOL_ID or sha256_json(core) != PROTOCOL_ID:
        raise IntegrityError("reported-bar protocol identity is invalid")
    return payload


def _active_calendar(root: Path) -> tuple[dict[str, object], dict[str, object]]:
    pointer = _read_canonical(root / ACTIVE_CALENDAR_POINTER, name="active cash-open calendar pointer")
    path = root / str(pointer.get("calendar_path"))
    if sha256_file(path) != pointer.get("calendar_sha256"):
        raise IntegrityError("active cash-open calendar hash drifted")
    calendar = _read_canonical(path, name="active cash-open calendar")
    if calendar.get("calendar_id") != pointer.get("calendar_id"):
        raise IntegrityError("active cash-open calendar identity differs")
    return pointer, calendar


def build_plan(*, root: Path) -> dict[str, object]:
    protocol = _protocol(root)
    pointer, calendar = _active_calendar(root)
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are absent")
    markets = sorted({str(item["market"]) for item in entries if isinstance(item, dict)})
    if len(markets) != 41:
        raise IntegrityError("reported-bar census requires the exact 41-market catalog universe")
    limits = protocol["execution_limits"]
    core: dict[str, object] = {
        "schema_version": "reported_bar_fixed_horizon_source_census_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED",
        "operation": SOURCE_COMPATIBILITY_CENSUS_OPERATION,
        "protocol_id": PROTOCOL_ID,
        "markets": markets,
        "years": protocol["years"],
        "checkpoint_grid": protocol["checkpoint_grid"],
        "active_calendar_id": calendar["calendar_id"],
        "output_root": OUTPUT_ROOT.as_posix(),
        "execution_limits": limits,
        "authority": {
            "historical_row_read": True,
            "price_values_emitted": False,
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
            PROTOCOL_PATH.as_posix(): PROTOCOL_SHA256,
            ACTIVE_CALENDAR_POINTER.as_posix(): sha256_file(root / ACTIVE_CALENDAR_POINTER),
            str(pointer["calendar_path"]): str(pointer["calendar_sha256"]),
            ACTIVE_CATALOG_PATH.as_posix(): sha256_file(root / ACTIVE_CATALOG_PATH),
            "src/futures_rebuild/active_data_view.py": sha256_file(root / "src/futures_rebuild/active_data_view.py"),
            "src/futures_rebuild/reported_bar_fixed_horizon_protocol.py": sha256_file(
                root / "src/futures_rebuild/reported_bar_fixed_horizon_protocol.py"
            ),
            "src/futures_rebuild/reported_bar_fixed_horizon_census.py": sha256_file(Path(__file__)),
            RUNNER_PATH.as_posix(): sha256_file(root / RUNNER_PATH),
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="reported-bar source-census plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    bindings = plan.get("bindings")
    limits = plan.get("execution_limits")
    if (
        plan.get("plan_id") != sha256_json(core)
        or plan.get("state") != "PREPARED_NOT_EXECUTED"
        or plan.get("operation") != SOURCE_COMPATIBILITY_CENSUS_OPERATION
        or plan.get("protocol_id") != PROTOCOL_ID
        or plan.get("checkpoint_grid") != list(CHECKPOINTS)
        or len(plan.get("markets", [])) != 41
        or not isinstance(limits, Mapping)
        or limits.get("maximum_attempts") != 1
        or limits.get("maximum_retries") != 0
        or limits.get("maximum_workers") != 4
        or limits.get("worker_deadline_seconds") != 3300
        or limits.get("maximum_runtime_seconds") != 3600
        or limits.get("maximum_external_cost_usd") != "0"
        or limits.get("windows_host_required") is not True
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("reported-bar source-census plan drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["execution_limits"]
    assert isinstance(limits, Mapping)
    return {
        "protocol_id": str(plan["protocol_id"]),
        "period": "2018,2019,2020,2021,2022",
        "market_count": "41",
        "checkpoint_count": "4",
        "purpose": "PRE_REGISTRATION_REPORTED_BAR_SOURCE_COMPATIBILITY_ONLY",
        "resolver": "ACTIVE_CATALOG_SELECTION_ONLY",
        "output_root": str(plan["output_root"]),
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "maximum_workers": "4",
        "worker_deadline_seconds": str(limits["worker_deadline_seconds"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_external_cost_usd": "0",
        "provider_network_access": "false",
        "holdout_2025_access": "false",
        "price_values_emitted": "false",
        "model_fit": "false",
        "prediction_generation": "false",
        "performance_evaluation": "false",
        "registration": "false",
        "publication": "false",
        "trading": "false",
        "approval_command": SOURCE_COMPATIBILITY_CENSUS_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _dependency_clock(event_at_ns: int) -> bool:
    clock = datetime.fromtimestamp(event_at_ns / 1_000_000_000, timezone.utc).astimezone(CT).time()
    minute = clock.hour * 60 + clock.minute
    return 8 * 60 + 30 <= minute <= 11 * 60 + 4


def _read_market(
    task: tuple[str, tuple[tuple[int, str], ...]]
) -> tuple[str, dict[str, tuple[SourceRow, ...]], dict[str, object]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise IntegrityError("source-only parquet reader is unavailable") from exc
    market, sources = task
    by_session: dict[str, list[SourceRow]] = {}
    audits: dict[str, object] = {}
    for year, raw_path in sources:
        path = Path(raw_path)
        parquet = pq.ParquetFile(path)
        if not REQUIRED_COLUMNS.issubset(parquet.schema_arrow.names):
            raise IntegrityError(f"reported-bar source schema is incomplete for {market} {year}")
        total = retained = sessionless = 0
        for batch in parquet.iter_batches(batch_size=65_536, columns=sorted(REQUIRED_COLUMNS)):
            columns = batch.to_pydict()
            for index in range(batch.num_rows):
                total += 1
                row = {name: values[index] for name, values in columns.items()}
                event = row.get("event_at_ns")
                if type(event) is not int or not _dependency_clock(event):
                    continue
                if not isinstance(row.get("exchange_session_date"), str):
                    sessionless += 1
                    continue
                normalized = source_row_from_mapping(market=market, row=row)
                by_session.setdefault(normalized.session, []).append(normalized)
                retained += 1
        audits[f"{market}/{year}"] = {
            "total_rows_scanned": total,
            "dependency_horizon_rows_retained": retained,
            "sessionless_dependency_horizon_rows": sessionless,
            "source_path": path.as_posix(),
            "source_sha256": sha256_file(path),
        }
    return market, {key: tuple(value) for key, value in by_session.items()}, audits


def _checkpoint_datetime(session: str, checkpoint: str) -> datetime:
    parsed = date.fromisoformat(session)
    clock = time.fromisoformat(checkpoint)
    return datetime.combine(parsed, clock, tzinfo=CT)


def _evidence(rows: Sequence[SourceRow]) -> tuple[ReportedBarEvidence, ...]:
    return tuple(
        ReportedBarEvidence(
            datetime.fromtimestamp(item.event_at_ns / 1_000_000_000, timezone.utc).astimezone(CT),
            datetime.fromtimestamp(item.available_at_ns / 1_000_000_000, timezone.utc).astimezone(CT),
            item.actual_identity_hash,
        )
        for item in rows if item.executable
    )


def _execution_only(
    *, checkpoint: datetime, rows: Sequence[ReportedBarEvidence]
) -> DiscoveryDisposition:
    decision = checkpoint + timedelta(seconds=5)
    entries = sorted(
        (
            item for item in rows
            if checkpoint + timedelta(minutes=1) <= item.event_at <= checkpoint + timedelta(minutes=2)
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
    if entry.actual_identity_hash is None or exits[0].actual_identity_hash != entry.actual_identity_hash:
        return DiscoveryDisposition(True, False, "EXECUTION_IDENTITY_CHANGING")
    return DiscoveryDisposition(True, True, "COMPLETE")


def _percent(numerator: int, denominator: int) -> float:
    return 100.0 * numerator / denominator if denominator else 0.0


def _universe_counts(
    *, sessions: Sequence[str], checkpoint: str, rows_by_session: Mapping[str, Sequence[SourceRow]]
) -> dict[str, object]:
    feature_complete = candidate_complete = baseline_complete = 0
    dispositions: dict[str, int] = {}
    for session in sessions:
        evidence = _evidence(rows_by_session.get(session, ()))
        candidate = classify_reported_bar_checkpoint(
            checkpoint=_checkpoint_datetime(session, checkpoint), rows=evidence
        )
        baseline = _execution_only(checkpoint=_checkpoint_datetime(session, checkpoint), rows=evidence)
        dispositions[candidate.disposition] = dispositions.get(candidate.disposition, 0) + 1
        feature_complete += int(candidate.feature_complete)
        candidate_complete += int(candidate.feature_complete and candidate.execution_complete)
        baseline_complete += int(baseline.execution_complete)
    return {
        "expected_sessions": len(sessions),
        "accounted_sessions": len(sessions),
        "feature_complete_sessions": feature_complete,
        "feature_complete_percent": _percent(feature_complete, len(sessions)),
        "candidate_path_expected": feature_complete,
        "candidate_path_complete": candidate_complete,
        "candidate_path_percent": _percent(candidate_complete, feature_complete),
        "always_direction_baseline_path_expected": len(sessions),
        "always_direction_baseline_path_complete": baseline_complete,
        "always_direction_baseline_path_percent": _percent(baseline_complete, len(sessions)),
        "feature_baseline_path_expected": feature_complete,
        "feature_baseline_path_complete": candidate_complete,
        "feature_baseline_path_percent": _percent(candidate_complete, feature_complete),
        "dispositions": dict(sorted(dispositions.items())),
    }


def certify_market_checkpoint(
    *, market: str, checkpoint: str, eligible_sessions: Sequence[str],
    rows_by_session: Mapping[str, Sequence[SourceRow]], catalog_complete: bool,
    catalog_failures: Sequence[str] = (),
) -> dict[str, object]:
    if checkpoint not in CHECKPOINTS:
        raise IntegrityError("reported-bar checkpoint is outside the frozen grid")
    try:
        folds = build_market_calendar_folds(eligible_sessions)
    except IntegrityError as exc:
        return {
            "market": market, "checkpoint": checkpoint, "status": "FAIL",
            "failed_gates": ["MECHANISM_ELIGIBLE_CALENDAR_FOLDS"],
            "catalog_failures": list(catalog_failures), "fold_results": [],
            "reason": str(exc),
        }
    overall = _universe_counts(
        sessions=eligible_sessions, checkpoint=checkpoint, rows_by_session=rows_by_session
    )
    by_year: dict[str, dict[str, object]] = {}
    for year in sorted({session[:4] for session in eligible_sessions}):
        by_year[year] = _universe_counts(
            sessions=[session for session in eligible_sessions if session.startswith(year)],
            checkpoint=checkpoint, rows_by_session=rows_by_session,
        )
    fold_results: list[dict[str, object]] = []
    gates: set[str] = set()
    if not catalog_complete:
        gates.add("ACTIVE_CATALOG_COMPLETE_2018_2022")
    if overall["accounted_sessions"] != overall["expected_sessions"]:
        gates.add("ONE_HUNDRED_PERCENT_CHECKPOINT_ACCOUNTING")
    if float(overall["feature_complete_percent"]) < 95:
        gates.add("FEATURE_COMPLETE_OVERALL_95_PERCENT")
    if any(float(item["feature_complete_percent"]) < 90 for item in by_year.values()):
        gates.add("FEATURE_COMPLETE_EACH_MARKET_YEAR_90_PERCENT")
    if float(overall["candidate_path_percent"]) != 100:
        gates.add("CANDIDATE_EXECUTION_PATH_100_PERCENT")
    if float(overall["always_direction_baseline_path_percent"]) != 100:
        gates.add("ALWAYS_DIRECTION_BASELINE_PATH_100_PERCENT")
    if float(overall["feature_baseline_path_percent"]) != 100:
        gates.add("FEATURE_BASELINE_PATH_100_PERCENT")
    for fold in folds:
        training = _universe_counts(
            sessions=fold["training_sessions"], checkpoint=checkpoint, rows_by_session=rows_by_session
        )
        evaluation = _universe_counts(
            sessions=fold["evaluation_sessions"], checkpoint=checkpoint, rows_by_session=rows_by_session
        )
        fold_gates: list[str] = []
        if float(training["feature_complete_percent"]) < 90 or float(evaluation["feature_complete_percent"]) < 90:
            fold_gates.append("FEATURE_COMPLETE_MARKET_FOLD_90_PERCENT")
        if int(training["feature_complete_sessions"]) < 252:
            fold_gates.append("MINIMUM_252_COMPLETE_TRAINING_SESSIONS")
        if int(evaluation["feature_complete_sessions"]) < 30:
            fold_gates.append("MINIMUM_30_COMPLETE_EVALUATION_SESSIONS")
        for label, universe in (("TRAINING", training), ("EVALUATION", evaluation)):
            if float(universe["candidate_path_percent"]) != 100:
                fold_gates.append(f"{label}_CANDIDATE_PATH_100_PERCENT")
            if float(universe["always_direction_baseline_path_percent"]) != 100:
                fold_gates.append(f"{label}_ALWAYS_DIRECTION_BASELINE_PATH_100_PERCENT")
            if float(universe["feature_baseline_path_percent"]) != 100:
                fold_gates.append(f"{label}_FEATURE_BASELINE_PATH_100_PERCENT")
        gates.update(fold_gates)
        fold_results.append({
            "fold_id": fold["fold_id"], "training": training, "evaluation": evaluation,
            "embargo_sessions": len(fold["embargo_sessions"]),
            "purge_minutes": fold["purge_minutes"], "failed_gates": sorted(set(fold_gates)),
        })
    baseline_universes = {
        "flat_no_trade": "EXACT_ZERO_NO_PATH",
        "always_long": "INDEPENDENT_CALENDAR_ELIGIBLE_EXECUTION_UNIVERSE",
        "always_short": "INDEPENDENT_CALENDAR_ELIGIBLE_EXECUTION_UNIVERSE",
        "reported_bar_continuation": "INDEPENDENT_FEATURE_COMPLETE_EXECUTION_UNIVERSE",
        "reported_bar_reversal": "INDEPENDENT_FEATURE_COMPLETE_EXECUTION_UNIVERSE",
    }
    return {
        "market": market, "checkpoint": checkpoint,
        "status": "PASS" if not gates else "FAIL",
        "failed_gates": sorted(gates), "catalog_failures": list(catalog_failures),
        "overall": overall, "market_year_results": by_year,
        "fold_results": fold_results, "baseline_universes": baseline_universes,
    }


def select_configuration(results: Sequence[Mapping[str, object]]) -> dict[str, object]:
    candidates: list[tuple[int, float, int, str, list[str]]] = []
    for order, checkpoint in enumerate(CHECKPOINTS):
        passing = sorted(
            str(item["market"]) for item in results
            if item.get("checkpoint") == checkpoint and item.get("status") == "PASS"
        )
        worst = min(
            (
                min(
                    float(side["feature_complete_percent"])
                    for fold in item["fold_results"]
                    for side in (fold["training"], fold["evaluation"])
                )
                for item in results
                if item.get("checkpoint") == checkpoint and item.get("status") == "PASS"
            ),
            default=0.0,
        )
        candidates.append((len(passing), worst, -order, checkpoint, passing))
    selected = max(candidates)
    if selected[0] < 2:
        return {
            "decision": "REJECTED_NO_USEFUL_MULTI_MARKET_SOURCE_COMPATIBILITY",
            "selected_checkpoint": None, "selected_markets": [],
            "selection_stage": "REJECTED",
        }
    return {
        "decision": "PASS_SOURCE_COMPATIBILITY_DISCOVERY",
        "selected_checkpoint": selected[3], "selected_markets": selected[4],
        "selection_stage": "MAX_MARKETS_THEN_WORST_FOLD_FEATURE_COMPLETENESS_THEN_EARLIEST",
    }


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> dict[str, object]:
    started = monotonic()
    plan = load_plan(root=root)
    if os.name != "nt" or multiprocessing.current_process().name != "MainProcess":
        raise UnauthorizedOperation("reported-bar census requires the Windows main process")
    output_root = root / str(plan["output_root"])
    boundary.assert_active_path(
        output_root, purpose="unpublished reported-bar source census",
        subtree="state/unpublished_evidence",
    )
    if output_root.exists():
        raise UnauthorizedOperation("reported-bar source-census output already exists")
    use_path = receipt.consume(
        boundary, operation=SOURCE_COMPATIBILITY_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    entries = catalog["entries"]
    assert isinstance(entries, list)
    by_key = {
        (str(item["market"]), int(item["year"])): item
        for item in entries if isinstance(item, dict)
    }
    tasks: list[tuple[str, tuple[tuple[int, str], ...]]] = []
    catalog_failures: dict[str, list[str]] = {}
    source_bindings: dict[str, str] = {}
    for market in plan["markets"]:
        sources: list[tuple[int, str]] = []
        failures: list[str] = []
        for year in plan["years"]:
            item = by_key.get((str(market), int(year)))
            if item is None or item.get("disposition") != "RESEARCH_READY_CAUSAL_PRICE":
                failures.append(f"{year}:{'ABSENT' if item is None else item.get('disposition')}")
                continue
            resolved = resolve(
                repository_root=root, market=str(market), year=int(year), purpose="SELECTION"
            )
            expected = str(item["parquet_sha256"])
            if sha256_file(resolved) != expected:
                raise IntegrityError(f"active catalog source drifted for {market} {year}")
            source_bindings[resolved.relative_to(root).as_posix()] = expected
            sources.append((int(year), str(resolved)))
        catalog_failures[str(market)] = failures
        tasks.append((str(market), tuple(sources)))
    limits = plan["execution_limits"]
    assert isinstance(limits, Mapping)
    pool = multiprocessing.get_context("spawn").Pool(processes=int(limits["maximum_workers"]))
    try:
        worker_results = pool.map_async(_read_market, tasks, chunksize=1).get(
            timeout=int(limits["worker_deadline_seconds"])
        )
        pool.close()
        pool.join()
    except BaseException:
        pool.terminate()
        pool.join()
        raise
    pointer, calendar = _active_calendar(root)
    calendar_rows = calendar.get("calendar_rows")
    if not isinstance(calendar_rows, list):
        raise IntegrityError("active calendar rows are absent")
    calendar_by_market: dict[str, list[dict[str, object]]] = {}
    for item in calendar_rows:
        if isinstance(item, dict):
            calendar_by_market.setdefault(str(item["market"]), []).append(item)
    observed = {market: (sessions, audits) for market, sessions, audits in worker_results}
    results: list[dict[str, object]] = []
    source_audits: dict[str, object] = {}
    for market in plan["markets"]:
        sessions, audits = observed[str(market)]
        source_audits.update(audits)
        failures = list(catalog_failures[str(market)])
        sessionless = sum(int(item["sessionless_dependency_horizon_rows"]) for item in audits.values())
        if sessionless:
            failures.append(f"SESSIONLESS_DEPENDENCY_HORIZON_ROWS:{sessionless}")
        rows_for_market = calendar_by_market.get(str(market), [])
        for checkpoint in CHECKPOINTS:
            eligible = tuple(
                str(item["trade_date"]) for item in rows_for_market
                if isinstance(item.get("checkpoint_open"), dict)
                and item["checkpoint_open"].get(checkpoint) is True
            )
            results.append(certify_market_checkpoint(
                market=str(market), checkpoint=checkpoint, eligible_sessions=eligible,
                rows_by_session=sessions, catalog_complete=not failures,
                catalog_failures=failures,
            ))
    selection = select_configuration(results)
    if monotonic() - started > int(limits["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("reported-bar source census exceeded total runtime")
    bindings = dict(plan["bindings"])
    bindings.update(source_bindings)
    core: dict[str, object] = {
        "schema_version": "reported_bar_fixed_horizon_source_census_report/1.0.0",
        "state": "SEALED_UNPUBLISHED_PRICE_FREE_SOURCE_EVIDENCE",
        "plan_id": plan["plan_id"], "protocol_id": plan["protocol_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "censused_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection": selection, "market_checkpoint_results": results,
        "source_audits": dict(sorted(source_audits.items())),
        "source_bindings": dict(sorted(bindings.items())),
        "parallel_market_workers": 4,
        "authority": plan["authority"],
    }
    report = {**core, "report_id": sha256_json(core)}
    output = output_root / str(report["report_id"]) / "source_census.json"
    output.parent.mkdir(parents=True, exist_ok=False)
    with output.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    return report
