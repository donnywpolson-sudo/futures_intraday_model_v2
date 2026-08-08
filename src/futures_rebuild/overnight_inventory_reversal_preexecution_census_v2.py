"""Parallel, deadline-enforced successor to the consumed readiness census.

The first census authorization was consumed but its serial V10 normalization
did not finish inside the approved runtime.  This successor preserves the same
row parser, observation builder, fold evidence, and certificate semantics.  It
only parallelizes the four independent market streams and terminates the
worker pool before its locked deadline.  It never computes strategy returns.
"""

from __future__ import annotations

import json
import multiprocessing
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .historical_checkpoint_calendar import load_historical_checkpoint_calendar
from .overnight_inventory_reversal_execution import (
    BASELINES,
    COST_TICKS,
    MARKETS,
    SessionObservation,
    iter_ordered_session_observations,
)
from .overnight_inventory_reversal_preexecution_census import (
    _object,
    _source_map,
    build_fold_evidence,
)
from .preexecution_fold_certification import ROW_CERTIFIED, build_fold_readiness_certificate
from .tier1_bracket_v10 import SourceIntegrityAuditV10, iter_source_records_from_parquet_v10
from .tier1_bracket_v5 import load_registered_calendar_sessions_v5


OPERATION = "CENSUS_OVERNIGHT_REVERSAL_FOLD_READINESS_PARALLEL_ONCE"
PLAN_PATH = Path("configs/overnight_inventory_reversal_fold_census_v2_plan.json")
OUTPUT_FILENAME = "fold_readiness_certificate.json"


def load_census_v2_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    bindings = plan.get("bindings")
    limits = plan.get("limits")
    predecessor = plan.get("failed_predecessor")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version")
        != "overnight_inventory_reversal_fold_census_plan/2.0.0"
        or plan.get("state") != "PREPARED_NOT_EXECUTED"
        or plan.get("operation") != OPERATION
        or plan.get("trial_id")
        != "24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c"
        or plan.get("historical_economics_evaluation") is not False
        or plan.get("model_fit") is not False
        or plan.get("prediction_generation") is not False
        or plan.get("holdout_2025_access") is not False
        or plan.get("provider_or_network_access") is not False
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
        or not isinstance(limits, Mapping)
        or limits.get("maximum_attempts") != 1
        or limits.get("maximum_retries") != 0
        or limits.get("maximum_runtime_seconds") != 900
        or type(limits.get("worker_pool_timeout_seconds")) is not int
        or not 1 <= int(limits["worker_pool_timeout_seconds"]) <= 780
        or limits.get("maximum_workers") != 4
        or not isinstance(predecessor, Mapping)
        or predecessor.get("authorization_consumed") is not True
        or predecessor.get("output_created") is not False
        or predecessor.get("runtime_limit_exceeded") is not True
        or predecessor.get("retry_under_predecessor_plan_authorized") is not False
    ):
        raise IntegrityError("parallel readiness census plan drifted")
    return plan


def required_scope_v2(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "trial_id": str(plan["trial_id"]),
        "period": "2018,2019,2020,2021,2022",
        "markets": "ES,CL,ZN,6E",
        "purpose": "FOLD_READINESS_COUNTS_ONLY_NO_ECONOMICS_PARALLEL_SUCCESSOR",
        "output_root": str(plan["output_root"]),
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "maximum_runtime_seconds": "900",
        "maximum_workers": "4",
        "provider_or_network_access": "false",
        "holdout_2025_access": "false",
        "publication": "false",
        "trading": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _read_market_task(
    task: tuple[str, tuple[tuple[int, str], ...]],
) -> tuple[str, tuple[SessionObservation, ...], dict[str, object]]:
    market, year_paths = task
    audits: dict[str, object] = {}

    def records():
        for year, raw_path in year_paths:
            audit = SourceIntegrityAuditV10(market)
            yield from iter_source_records_from_parquet_v10(
                market=market, path=Path(raw_path), audit=audit,
            )
            audits[f"{market}/{year}"] = audit.as_dict()

    observations = tuple(iter_ordered_session_observations(
        market=market, source_records=records(),
    ))
    return market, observations, audits


def collect_market_observations_parallel(
    *, paths: Mapping[tuple[str, int], Path], maximum_workers: int,
    timeout_seconds: int,
) -> tuple[list[SessionObservation], dict[str, object]]:
    """Read the same streams in four processes and terminate on timeout."""

    if maximum_workers != len(MARKETS) or not 1 <= timeout_seconds <= 780:
        raise IntegrityError("parallel readiness bounds are invalid")
    tasks = [
        (
            market,
            tuple((year, str(paths[(market, year)])) for year in range(2018, 2023)),
        )
        for market in MARKETS
    ]
    context = multiprocessing.get_context("spawn")
    pool = context.Pool(processes=maximum_workers)
    started = monotonic()
    try:
        async_result = pool.map_async(_read_market_task, tasks, chunksize=1)
        results = async_result.get(timeout=timeout_seconds)
        pool.close()
        pool.join()
    except multiprocessing.TimeoutError as exc:
        pool.terminate()
        pool.join()
        raise UnauthorizedOperation(
            "parallel readiness census reached its internal worker deadline"
        ) from exc
    except BaseException:
        pool.terminate()
        pool.join()
        raise
    if monotonic() - started > timeout_seconds + 5:
        raise UnauthorizedOperation("parallel readiness census exceeded its worker bound")
    if [item[0] for item in results] != list(MARKETS):
        raise IntegrityError("parallel readiness market order changed")
    observations: list[SessionObservation] = []
    audits: dict[str, object] = {}
    for _, market_observations, market_audits in results:
        observations.extend(market_observations)
        audits.update(market_audits)
    return observations, dict(sorted(audits.items()))


def _bounded_worker_timeout(
    *, configured_timeout: int, maximum_runtime: int, elapsed_seconds: float,
) -> int:
    remaining = min(
        configured_timeout,
        maximum_runtime - int(elapsed_seconds) - 30,
    )
    if remaining < 1:
        raise UnauthorizedOperation(
            "parallel readiness census exhausted its total runtime before row reading"
        )
    return remaining


def execute_authorized_census_v2_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> dict[str, object]:
    operation_started = monotonic()
    plan = load_census_v2_plan(root=root)
    output_root = root / str(plan["output_root"])
    boundary.assert_active_path(
        output_root.absolute(), purpose="unpublished parallel fold readiness census",
        subtree="state/unpublished_evidence",
    )
    if output_root.exists():
        raise UnauthorizedOperation("parallel readiness census output already exists")
    use_path = receipt.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope_v2(root=root, plan=plan),
    )
    manifest = _object(root / str(plan["phase5_manifest_path"]))
    paths, source_bindings = _source_map(root=root, manifest=manifest)
    limits = plan["limits"]
    assert isinstance(limits, Mapping)
    maximum_runtime = int(limits["maximum_runtime_seconds"])
    remaining_worker_budget = _bounded_worker_timeout(
        configured_timeout=int(limits["worker_pool_timeout_seconds"]),
        maximum_runtime=maximum_runtime,
        elapsed_seconds=monotonic() - operation_started,
    )
    observations, source_audits = collect_market_observations_parallel(
        paths=paths,
        maximum_workers=int(limits["maximum_workers"]),
        timeout_seconds=remaining_worker_budget,
    )

    sessions = load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(plan["calendar_release_id"]),
    )
    loaded_calendar = load_historical_checkpoint_calendar(boundary=boundary)
    if loaded_calendar.index_receipt.release_id != str(plan["calendar_release_id"]):
        raise IntegrityError("parallel readiness census calendar dependency changed")
    certification_bindings = dict(source_bindings)
    plan_bindings = plan.get("bindings")
    assert isinstance(plan_bindings, Mapping)
    for relative, digest in plan_bindings.items():
        certification_bindings[str(relative)] = str(digest)
    capture_manifest = loaded_calendar.capture_receipt.verify(boundary)
    for entry in capture_manifest.files:
        certification_bindings[
            capture_manifest.physical_relative_path(entry).as_posix()
        ] = entry.sha256

    expected_open = {
        market: tuple(
            item.exchange_session_date for item in sessions
            if item.market == market
            and item.checkpoint_states is not None
            and item.checkpoint_states.get("08:30") is True
        )
        for market in MARKETS
    }
    folds = manifest.get("outer_folds")
    schedule_sessions = manifest.get("session_dates")
    if (
        not isinstance(folds, list) or len(folds) != 8
        or not isinstance(schedule_sessions, list)
        or any(not isinstance(item, str) for item in schedule_sessions)
    ):
        raise IntegrityError("bound Phase 5 fold schedule is malformed")
    fold_evidence, failure_audit = build_fold_evidence(
        observations=observations,
        outer_folds=folds,
        expected_open_sessions=expected_open,
        ordered_schedule_sessions=schedule_sessions,
    )
    certificate = build_fold_readiness_certificate(
        trial_family="overnight_inventory_reversal_cash_open_closed_audit",
        protocol_id=str(plan["protocol_id"]),
        source_bindings=certification_bindings,
        fold_evidence=fold_evidence,
        required_markets=MARKETS,
        required_baselines=BASELINES,
        required_cost_scenarios=tuple(COST_TICKS),
        required_outer_fold_ids=tuple(f"fold-{index}" for index in range(8)),
        required_nested_fold_ids=(),
        expected_outer_folds=8,
        expected_nested_folds=0,
        minimum_training_sessions=252,
        minimum_evaluation_sessions=63,
        minimum_purge_minutes=60,
        minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED,
        historical_rows_opened=True,
    )
    core = {
        "schema_version": "overnight_inventory_reversal_fold_census/2.0.0",
        "trial_id": plan["trial_id"],
        "plan_id": plan["plan_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "censused_at_utc": datetime.now(timezone.utc).isoformat(),
        "fold_readiness_certificate": certificate,
        "runtime_failure_audit": failure_audit,
        "source_audits": source_audits,
        "parallel_market_workers": len(MARKETS),
        "economics_evaluation": False,
        "model_fit": False,
        "prediction_generation": False,
        "holdout_2025_touched": False,
        "provider_or_network_access": False,
        "publication": False,
        "trading": False,
    }
    if monotonic() - operation_started > maximum_runtime - 5:
        raise UnauthorizedOperation(
            "parallel readiness census exhausted its total runtime before output"
        )
    report = {**core, "report_id": sha256_json(core)}
    output_root.mkdir(parents=True, exist_ok=False)
    output_path = output_root / OUTPUT_FILENAME
    with output_path.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
    if _object(output_path) != report:
        raise IntegrityError("parallel fold readiness census verification failed")
    return report
