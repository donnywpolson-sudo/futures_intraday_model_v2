"""Price-free source census for the trade-triggered reported-bar protocol."""

from __future__ import annotations

import multiprocessing
import os
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic

from .active_data_view import resolve
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .cash_open_source_compatibility import SourceRow, build_market_calendar_folds
from .cash_open_source_compatibility_census import _read_canonical
from .errors import IntegrityError, UnauthorizedOperation
from .reported_bar_fixed_horizon_census import (
    ACTIVE_CALENDAR_POINTER,
    ACTIVE_CATALOG_PATH,
    REQUIRED_COLUMNS,
    _active_calendar,
    _checkpoint_datetime,
    _evidence,
    _percent,
    _read_market,
)
from .reported_bar_trade_triggered_protocol import (
    CHECKPOINTS,
    PROTOCOL_PATH,
    classify_trade_triggered_checkpoint,
)
from .research_gateway_policy import SOURCE_COMPATIBILITY_CENSUS_OPERATION


PROTOCOL_ID = "5acd510305cbb4c0de6b813ff92e15b77151ae7cc0f9a2a2c53192cb9967d8d3"
PROTOCOL_SHA256 = "8eefcb8051de4eb94a975688db33c3022dc654b06ce5be35de7b3275c61ef7f6"
PLAN_PATH = Path("configs/reported_bar_trade_triggered_source_census_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/reported_bar_trade_triggered_source_census")
RUNNER_PATH = Path("scripts/run_reported_bar_trade_triggered_source_census.py")
ACTIVE_BASELINES = (
    "ALWAYS_LONG",
    "ALWAYS_SHORT",
    "REPORTED_BAR_CONTINUATION",
    "REPORTED_BAR_REVERSAL",
)


def _protocol(root: Path) -> dict[str, object]:
    path = root / PROTOCOL_PATH
    if sha256_file(path) != PROTOCOL_SHA256:
        raise IntegrityError("trade-triggered protocol hash drifted")
    payload = _read_canonical(path, name="trade-triggered source protocol")
    core = {key: value for key, value in payload.items() if key != "protocol_id"}
    if payload.get("protocol_id") != PROTOCOL_ID or sha256_json(core) != PROTOCOL_ID:
        raise IntegrityError("trade-triggered protocol identity is invalid")
    return payload


def build_plan(*, root: Path) -> dict[str, object]:
    protocol = _protocol(root)
    pointer, calendar = _active_calendar(root)
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are absent")
    markets = sorted({str(item["market"]) for item in entries if isinstance(item, dict)})
    if len(markets) != 41:
        raise IntegrityError("trade-triggered census requires the exact 41-market universe")
    core: dict[str, object] = {
        "schema_version": "reported_bar_trade_triggered_source_census_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED",
        "operation": SOURCE_COMPATIBILITY_CENSUS_OPERATION,
        "protocol_id": PROTOCOL_ID,
        "markets": markets,
        "years": protocol["years"],
        "checkpoint_grid": protocol["checkpoint_grid"],
        "active_calendar_id": calendar["calendar_id"],
        "output_root": OUTPUT_ROOT.as_posix(),
        "execution_limits": protocol["execution_limits"],
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
            "src/futures_rebuild/active_data_view.py": sha256_file(
                root / "src/futures_rebuild/active_data_view.py"
            ),
            "src/futures_rebuild/reported_bar_trade_triggered_protocol.py": sha256_file(
                root / "src/futures_rebuild/reported_bar_trade_triggered_protocol.py"
            ),
            "src/futures_rebuild/reported_bar_trade_triggered_census.py": sha256_file(Path(__file__)),
            RUNNER_PATH.as_posix(): sha256_file(root / RUNNER_PATH),
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="trade-triggered source-census plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    limits = plan.get("execution_limits")
    bindings = plan.get("bindings")
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
        raise IntegrityError("trade-triggered source-census plan drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["execution_limits"]
    assert isinstance(limits, Mapping)
    return {
        "protocol_id": str(plan["protocol_id"]),
        "period": "2018,2019,2020,2021,2022",
        "market_count": "41",
        "checkpoint_count": "4",
        "purpose": "PRE_REGISTRATION_TRADE_TRIGGERED_SOURCE_COMPATIBILITY_ONLY",
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


def _is_acceptable(disposition: str) -> bool:
    return disposition in {
        "COMPLETE",
        "EXPLICIT_CAUSAL_FEATURE_ABSTENTION",
        "EXPLICIT_CAUSAL_NO_TRADE_TIMEOUT",
    }


def _universe_counts(
    *, sessions: Sequence[str], checkpoint: str, rows_by_session: Mapping[str, Sequence[SourceRow]]
) -> dict[str, object]:
    feature_complete = 0
    candidate_required = candidate_complete = candidate_failures = 0
    baseline_required = {name: 0 for name in ACTIVE_BASELINES}
    baseline_complete = {name: 0 for name in ACTIVE_BASELINES}
    baseline_failures = {name: 0 for name in ACTIVE_BASELINES}
    dispositions: dict[str, int] = {}
    baseline_dispositions: dict[str, dict[str, int]] = {name: {} for name in ACTIVE_BASELINES}
    for session in sessions:
        checkpoint_at = _checkpoint_datetime(session, checkpoint)
        evidence = _evidence(rows_by_session.get(session, ()))
        candidate = classify_trade_triggered_checkpoint(
            checkpoint=checkpoint_at, rows=evidence, feature_required=True
        )
        dispositions[candidate.disposition] = dispositions.get(candidate.disposition, 0) + 1
        feature_complete += int(candidate.feature_complete)
        candidate_required += int(candidate.path_required)
        candidate_complete += int(candidate.path_required and candidate.exit_fill_complete)
        candidate_failures += int(not _is_acceptable(candidate.disposition))
        for baseline in ACTIVE_BASELINES:
            feature_required = baseline in {"REPORTED_BAR_CONTINUATION", "REPORTED_BAR_REVERSAL"}
            result = classify_trade_triggered_checkpoint(
                checkpoint=checkpoint_at, rows=evidence, feature_required=feature_required
            )
            counts = baseline_dispositions[baseline]
            counts[result.disposition] = counts.get(result.disposition, 0) + 1
            baseline_required[baseline] += int(result.path_required)
            baseline_complete[baseline] += int(result.path_required and result.exit_fill_complete)
            baseline_failures[baseline] += int(not _is_acceptable(result.disposition))
    return {
        "expected_sessions": len(sessions),
        "accounted_sessions": len(sessions),
        "feature_complete_sessions": feature_complete,
        "feature_complete_percent": _percent(feature_complete, len(sessions)),
        "candidate_triggered_path_expected": candidate_required,
        "candidate_triggered_path_complete": candidate_complete,
        "candidate_triggered_path_percent": _percent(candidate_complete, candidate_required),
        "candidate_mandatory_failures": candidate_failures,
        "active_baseline_triggered_path_expected": baseline_required,
        "active_baseline_triggered_path_complete": baseline_complete,
        "active_baseline_triggered_path_percent": {
            name: _percent(baseline_complete[name], baseline_required[name])
            for name in ACTIVE_BASELINES
        },
        "active_baseline_mandatory_failures": baseline_failures,
        "candidate_dispositions": dict(sorted(dispositions.items())),
        "active_baseline_dispositions": {
            name: dict(sorted(values.items())) for name, values in baseline_dispositions.items()
        },
    }


def _path_gate_failures(universe: Mapping[str, object], *, prefix: str = "") -> set[str]:
    gates: set[str] = set()
    label = f"{prefix}_" if prefix else ""
    if int(universe["candidate_mandatory_failures"]) != 0:
        gates.add(f"{label}CANDIDATE_TRIGGERED_PATH_100_PERCENT")
    failures = universe["active_baseline_mandatory_failures"]
    assert isinstance(failures, Mapping)
    for baseline, count in failures.items():
        if int(count) != 0:
            gates.add(f"{label}{baseline}_TRIGGERED_PATH_100_PERCENT")
    return gates


def certify_market_checkpoint(
    *, market: str, checkpoint: str, eligible_sessions: Sequence[str],
    rows_by_session: Mapping[str, Sequence[SourceRow]], catalog_complete: bool,
    catalog_failures: Sequence[str] = (),
) -> dict[str, object]:
    if checkpoint not in CHECKPOINTS:
        raise IntegrityError("trade-triggered checkpoint is outside the frozen grid")
    try:
        folds = build_market_calendar_folds(eligible_sessions)
    except IntegrityError as exc:
        return {
            "market": market,
            "checkpoint": checkpoint,
            "status": "FAIL",
            "failed_gates": ["MECHANISM_ELIGIBLE_CALENDAR_FOLDS"],
            "catalog_failures": list(catalog_failures),
            "fold_results": [],
            "reason": str(exc),
        }
    overall = _universe_counts(
        sessions=eligible_sessions, checkpoint=checkpoint, rows_by_session=rows_by_session
    )
    by_year = {
        year: _universe_counts(
            sessions=[session for session in eligible_sessions if session.startswith(year)],
            checkpoint=checkpoint,
            rows_by_session=rows_by_session,
        )
        for year in sorted({session[:4] for session in eligible_sessions})
    }
    gates: set[str] = set()
    if not catalog_complete:
        gates.add("ACTIVE_CATALOG_COMPLETE_2018_2022")
    if overall["accounted_sessions"] != overall["expected_sessions"]:
        gates.add("ONE_HUNDRED_PERCENT_CHECKPOINT_ACCOUNTING")
    if float(overall["feature_complete_percent"]) < 95:
        gates.add("FEATURE_COMPLETE_OVERALL_95_PERCENT")
    if any(float(item["feature_complete_percent"]) < 90 for item in by_year.values()):
        gates.add("FEATURE_COMPLETE_EACH_MARKET_YEAR_90_PERCENT")
    gates.update(_path_gate_failures(overall))
    fold_results: list[dict[str, object]] = []
    for fold in folds:
        training = _universe_counts(
            sessions=fold["training_sessions"], checkpoint=checkpoint, rows_by_session=rows_by_session
        )
        evaluation = _universe_counts(
            sessions=fold["evaluation_sessions"], checkpoint=checkpoint, rows_by_session=rows_by_session
        )
        fold_gates: set[str] = set()
        if (
            float(training["feature_complete_percent"]) < 90
            or float(evaluation["feature_complete_percent"]) < 90
        ):
            fold_gates.add("FEATURE_COMPLETE_MARKET_FOLD_90_PERCENT")
        if int(training["feature_complete_sessions"]) < 252:
            fold_gates.add("MINIMUM_252_COMPLETE_TRAINING_SESSIONS")
        if int(evaluation["feature_complete_sessions"]) < 30:
            fold_gates.add("MINIMUM_30_COMPLETE_EVALUATION_SESSIONS")
        fold_gates.update(_path_gate_failures(training, prefix="TRAINING"))
        fold_gates.update(_path_gate_failures(evaluation, prefix="EVALUATION"))
        gates.update(fold_gates)
        fold_results.append({
            "fold_id": fold["fold_id"],
            "training": training,
            "evaluation": evaluation,
            "embargo_sessions": len(fold["embargo_sessions"]),
            "purge_minutes": fold["purge_minutes"],
            "failed_gates": sorted(fold_gates),
        })
    return {
        "market": market,
        "checkpoint": checkpoint,
        "status": "PASS" if not gates else "FAIL",
        "failed_gates": sorted(gates),
        "catalog_failures": list(catalog_failures),
        "overall": overall,
        "market_year_results": by_year,
        "fold_results": fold_results,
        "baseline_universes": {
            "flat_no_trade": "EXACT_ZERO_NO_PATH",
            "always_long": "INDEPENDENT_TRIGGER_ORDER_FILL_EXIT_TIMEOUT_STATE",
            "always_short": "INDEPENDENT_TRIGGER_ORDER_FILL_EXIT_TIMEOUT_STATE",
            "reported_bar_continuation": "INDEPENDENT_FEATURE_AND_TRIGGER_STATE",
            "reported_bar_reversal": "INDEPENDENT_FEATURE_AND_TRIGGER_STATE",
        },
    }


def select_configuration(results: Sequence[Mapping[str, object]]) -> dict[str, object]:
    candidates: list[tuple[int, float, int, str, list[str]]] = []
    for order, checkpoint in enumerate(CHECKPOINTS):
        passing_items = [
            item for item in results
            if item.get("checkpoint") == checkpoint and item.get("status") == "PASS"
        ]
        passing = sorted(str(item["market"]) for item in passing_items)
        worst = min(
            (
                min(
                    float(side["feature_complete_percent"])
                    for fold in item["fold_results"]
                    for side in (fold["training"], fold["evaluation"])
                )
                for item in passing_items
            ),
            default=0.0,
        )
        candidates.append((len(passing), worst, -order, checkpoint, passing))
    selected = max(candidates)
    if selected[0] < 2:
        return {
            "decision": "REJECTED_NO_USEFUL_MULTI_MARKET_SOURCE_COMPATIBILITY",
            "selected_checkpoint": None,
            "selected_markets": [],
            "selection_stage": "REJECTED",
        }
    return {
        "decision": "PASS_SOURCE_COMPATIBILITY_DISCOVERY",
        "selected_checkpoint": selected[3],
        "selected_markets": selected[4],
        "selection_stage": "MAX_MARKETS_THEN_WORST_FOLD_FEATURE_COMPLETENESS_THEN_EARLIEST",
    }


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> dict[str, object]:
    started = monotonic()
    plan = load_plan(root=root)
    if os.name != "nt" or multiprocessing.current_process().name != "MainProcess":
        raise UnauthorizedOperation("trade-triggered census requires the Windows main process")
    output_root = root / str(plan["output_root"])
    boundary.assert_active_path(
        output_root,
        purpose="unpublished trade-triggered source census",
        subtree="state/unpublished_evidence",
    )
    if output_root.exists():
        raise UnauthorizedOperation("trade-triggered source-census output already exists")
    use_path = receipt.consume(
        boundary,
        operation=SOURCE_COMPATIBILITY_CENSUS_OPERATION,
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
                market=str(market),
                checkpoint=checkpoint,
                eligible_sessions=eligible,
                rows_by_session=sessions,
                catalog_complete=not failures,
                catalog_failures=failures,
            ))
    selection = select_configuration(results)
    if monotonic() - started > int(limits["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("trade-triggered source census exceeded total runtime")
    bindings = dict(plan["bindings"])
    bindings.update(source_bindings)
    core: dict[str, object] = {
        "schema_version": "reported_bar_trade_triggered_source_census_report/1.0.0",
        "state": "SEALED_UNPUBLISHED_PRICE_FREE_SOURCE_EVIDENCE",
        "plan_id": plan["plan_id"],
        "protocol_id": plan["protocol_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "censused_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection": selection,
        "market_checkpoint_results": results,
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
