"""Windows-host successor for the consumed 41-market source census.

The predecessor failed after consuming its receipt but before creating the
worker pool.  This module is additive: it preserves every predecessor-bound
byte and changes only the execution host boundary and output namespace.
"""

from __future__ import annotations

import multiprocessing
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .cash_open_source_compatibility import (
    ACTIVE_CATALOG_PATH,
    CHECKPOINT_GRID,
    FALLBACK_PAIRS,
    PRIMARY_CONFIG,
    SINGLE_CONFIGS,
    certify_market_configuration,
    resolve_catalog_source,
    select_compatible_market_set,
)
from .cash_open_source_compatibility_census import (
    ACTIVE_CALENDAR_POINTER,
    SPEC_PATH,
    _active_calendar,
    _read_canonical,
    _read_market,
)
from .errors import IntegrityError, UnauthorizedOperation
from .research_gateway_policy import SOURCE_COMPATIBILITY_CENSUS_OPERATION


PREDECESSOR_PLAN_PATH = Path("configs/cash_open_41_market_source_compatibility_census_plan.json")
PREDECESSOR_PLAN_ID = "7f9370263b1124cce214a091b5ffd3e82a3f1977b07465d5270c7e5749f57655"
PREDECESSOR_PLAN_SHA256 = "285eed9fcb6f295e40e990dcf0922ef81748fbdcd3934b0305c27c2ded2ea3b0"
FAILED_USE_PATH = Path(
    "state/authorization_uses/"
    "db9f493ea2c3482090dcc933317be8de95da3943d84405be2b2f9dd48b13c4a3.json"
)
FAILED_USE_SHA256 = "0e3f4674af1a1c8936b39ff673aecffffcb1fcc670d3d8b3c7e35b48ab286098"
FAILURE_ROOT = Path(
    "state/unpublished_evidence/cash_open_41_market_source_compatibility_census_attempts"
)
PLAN_PATH = Path("configs/cash_open_41_market_source_compatibility_census_v2_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/cash_open_41_market_source_compatibility_census_v2")
RUNNER_PATH = Path("scripts/run_cash_open_41_market_source_compatibility_census_v2.py")


def build_failure_record(*, root: Path) -> dict[str, object]:
    if sha256_file(root / PREDECESSOR_PLAN_PATH) != PREDECESSOR_PLAN_SHA256:
        raise IntegrityError("consumed source-census plan drifted")
    if sha256_file(root / FAILED_USE_PATH) != FAILED_USE_SHA256:
        raise IntegrityError("consumed source-census authorization use drifted")
    if (root / "state/unpublished_evidence/cash_open_41_market_source_compatibility_census").exists():
        raise IntegrityError("failed predecessor unexpectedly produced census output")
    core: dict[str, object] = {
        "schema_version": "cash_open_41_market_source_compatibility_attempt_failure/1.0.0",
        "classification": "INCONCLUSIVE_EXECUTION_HOST_PERMISSION",
        "plan_id": PREDECESSOR_PLAN_ID,
        "plan_path": PREDECESSOR_PLAN_PATH.as_posix(),
        "plan_sha256": PREDECESSOR_PLAN_SHA256,
        "authorization_receipt_id": FAILED_USE_PATH.stem,
        "authorization_use_path": FAILED_USE_PATH.as_posix(),
        "authorization_use_sha256": FAILED_USE_SHA256,
        "failure_stage": "WINDOWS_MULTIPROCESSING_POOL_CREATION",
        "failure_type": "PermissionError",
        "failure_code": "WINERROR_5_ACCESS_DENIED",
        "workers_started": False,
        "historical_rows_decoded": 0,
        "census_output_created": False,
        "economic_result": "NOT_PRODUCED",
        "attempt_consumed": True,
        "retry_authorized": False,
        "research_parameters_changed": False,
    }
    return {**core, "failure_id": sha256_json(core)}


def failure_record_path(record: Mapping[str, object]) -> Path:
    return FAILURE_ROOT / str(record["failure_id"]) / "failure.json"


def build_plan_v2(*, root: Path, failure: Mapping[str, object]) -> dict[str, object]:
    predecessor = _read_canonical(root / PREDECESSOR_PLAN_PATH, name="consumed source-census plan")
    if predecessor.get("plan_id") != PREDECESSOR_PLAN_ID:
        raise IntegrityError("consumed source-census plan identity drifted")
    failure_path = failure_record_path(failure)
    limits = predecessor.get("limits")
    if limits != {
        "maximum_attempts": 1,
        "maximum_external_cost_usd": "0",
        "maximum_retries": 0,
        "maximum_runtime_seconds": 3600,
        "maximum_workers": 4,
        "worker_deadline_seconds": 3300,
    }:
        raise IntegrityError("predecessor execution limits drifted")
    core: dict[str, object] = {
        "schema_version": "cash_open_41_market_source_compatibility_census_plan/2.0.0",
        "state": "PREPARED_NOT_EXECUTED",
        "operation": SOURCE_COMPATIBILITY_CENSUS_OPERATION,
        "spec_id": predecessor["spec_id"],
        "markets": predecessor["markets"],
        "years": predecessor["years"],
        "active_calendar_id": predecessor["active_calendar_id"],
        "output_root": OUTPUT_ROOT.as_posix(),
        "limits": limits,
        "authority": predecessor["authority"],
        "host_execution": {
            "platform": "WINDOWS_HOST_REQUIRED",
            "main_process_required": True,
            "multiprocessing_context": "spawn",
            "worker_count": 4,
            "sandbox_execution_forbidden": True,
        },
        "consumed_predecessor": {
            "plan_id": PREDECESSOR_PLAN_ID,
            "plan_sha256": PREDECESSOR_PLAN_SHA256,
            "authorization_use_path": FAILED_USE_PATH.as_posix(),
            "authorization_use_sha256": FAILED_USE_SHA256,
            "failure_id": failure["failure_id"],
            "workers_started": False,
            "historical_rows_decoded": 0,
            "output_created": False,
            "retry_authorized": False,
        },
        "bindings": {
            SPEC_PATH.as_posix(): sha256_file(root / SPEC_PATH),
            ACTIVE_CALENDAR_POINTER.as_posix(): sha256_file(root / ACTIVE_CALENDAR_POINTER),
            ACTIVE_CATALOG_PATH.as_posix(): sha256_file(root / ACTIVE_CATALOG_PATH),
            PREDECESSOR_PLAN_PATH.as_posix(): PREDECESSOR_PLAN_SHA256,
            FAILED_USE_PATH.as_posix(): FAILED_USE_SHA256,
            failure_path.as_posix(): sha256_file(root / failure_path),
            "src/futures_rebuild/active_data_view.py": sha256_file(root / "src/futures_rebuild/active_data_view.py"),
            "src/futures_rebuild/cash_open_source_compatibility.py": sha256_file(root / "src/futures_rebuild/cash_open_source_compatibility.py"),
            "src/futures_rebuild/cash_open_source_compatibility_census.py": sha256_file(root / "src/futures_rebuild/cash_open_source_compatibility_census.py"),
            "src/futures_rebuild/cash_open_source_compatibility_census_v2.py": sha256_file(Path(__file__)),
            RUNNER_PATH.as_posix(): sha256_file(root / RUNNER_PATH),
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def load_plan_v2(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="host-successor source-census plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    bindings = plan.get("bindings")
    predecessor = plan.get("consumed_predecessor")
    if (
        plan.get("plan_id") != sha256_json(core)
        or plan.get("schema_version") != "cash_open_41_market_source_compatibility_census_plan/2.0.0"
        or plan.get("state") != "PREPARED_NOT_EXECUTED"
        or plan.get("operation") != SOURCE_COMPATIBILITY_CENSUS_OPERATION
        or plan.get("output_root") != OUTPUT_ROOT.as_posix()
        or plan.get("limits", {}).get("maximum_workers") != 4
        or plan.get("limits", {}).get("maximum_attempts") != 1
        or plan.get("limits", {}).get("maximum_retries") != 0
        or plan.get("limits", {}).get("worker_deadline_seconds") != 3300
        or plan.get("limits", {}).get("maximum_runtime_seconds") != 3600
        or plan.get("limits", {}).get("maximum_external_cost_usd") != "0"
        or plan.get("host_execution") != {
            "platform": "WINDOWS_HOST_REQUIRED",
            "main_process_required": True,
            "multiprocessing_context": "spawn",
            "worker_count": 4,
            "sandbox_execution_forbidden": True,
        }
        or not isinstance(predecessor, Mapping)
        or predecessor.get("plan_id") != PREDECESSOR_PLAN_ID
        or predecessor.get("retry_authorized") is not False
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("host-successor source-census plan drifted")
    return plan


def required_scope_v2(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["limits"]
    assert isinstance(limits, Mapping)
    return {
        "spec_id": str(plan["spec_id"]),
        "period": "2018,2019,2020,2021,2022",
        "market_count": "41",
        "purpose": "PRE_REGISTRATION_SOURCE_COMPATIBILITY_WINDOWS_HOST_SUCCESSOR_ONLY",
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


def execute_census_v2_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> dict[str, object]:
    started = monotonic()
    plan = load_plan_v2(root=root)
    if os.name != "nt" or multiprocessing.current_process().name != "MainProcess":
        raise UnauthorizedOperation("host-successor requires the Windows main process")
    output_root = root / str(plan["output_root"])
    boundary.assert_active_path(
        output_root, purpose="unpublished host-successor source census",
        subtree="state/unpublished_evidence",
    )
    if output_root.exists():
        raise UnauthorizedOperation("host-successor source-census output already exists")
    use_path = receipt.consume(
        boundary, operation=SOURCE_COMPATIBILITY_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope_v2(root=root, plan=plan),
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
            path = resolve_catalog_source(root=root, market=str(market), year=int(year))
            expected = str(item["parquet_sha256"])
            if sha256_file(path) != expected:
                raise IntegrityError(f"active catalog source drifted for {market} {year}")
            source_bindings[path.relative_to(root).as_posix()] = expected
            sources.append((int(year), str(path)))
        catalog_failures[str(market)] = failures
        tasks.append((str(market), tuple(sources)))

    limits = plan["limits"]
    assert isinstance(limits, Mapping)
    context = multiprocessing.get_context("spawn")
    pool = context.Pool(processes=int(limits["maximum_workers"]))
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
    configurations = (PRIMARY_CONFIG, *FALLBACK_PAIRS, *SINGLE_CONFIGS)
    market_results: list[dict[str, object]] = []
    passing: dict[tuple[str, ...], list[str]] = {tuple(item): [] for item in configurations}
    source_audits: dict[str, object] = {}
    for market in plan["markets"]:
        sessions, audits = observed[str(market)]
        source_audits.update(audits)
        sessionless = sum(int(item["sessionless_dependency_horizon_rows"]) for item in audits.values())
        failures = list(catalog_failures[str(market)])
        if sessionless:
            failures.append(f"SESSIONLESS_DEPENDENCY_HORIZON_ROWS:{sessionless}")
        rows_for_market = calendar_by_market.get(str(market), [])
        for config in configurations:
            eligible = tuple(
                str(item["trade_date"])
                for item in rows_for_market
                if isinstance(item.get("checkpoint_open"), dict)
                and all(item["checkpoint_open"].get(checkpoint) is True for checkpoint in config)
            )
            result = certify_market_configuration(
                market=str(market), checkpoints=config, eligible_sessions=eligible,
                rows_by_session=sessions, catalog_complete=not failures,
                catalog_failures=failures,
            )
            market_results.append(result)
            if result["status"] == "PASS":
                passing[tuple(config)].append(str(market))
    selection = select_compatible_market_set(passing)
    if monotonic() - started > int(limits["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("host-successor census exceeded total runtime")
    bindings = dict(plan["bindings"])
    bindings.update(source_bindings)
    core: dict[str, object] = {
        "schema_version": "cash_open_41_market_source_compatibility_report/2.0.0",
        "state": "SEALED_UNPUBLISHED_SOURCE_ONLY_EVIDENCE",
        "plan_id": plan["plan_id"],
        "spec_id": plan["spec_id"],
        "consumed_predecessor_plan_id": PREDECESSOR_PLAN_ID,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "censused_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection": selection,
        "configuration_passing_markets": {
            "+".join(config): sorted(markets) for config, markets in passing.items()
        },
        "market_configuration_results": market_results,
        "source_audits": dict(sorted(source_audits.items())),
        "source_bindings": dict(sorted(bindings.items())),
        "parallel_market_workers": 4,
        "authority": {
            "historical_rows_read": True,
            "price_values_emitted": False,
            "model_fit": False,
            "prediction_generation": False,
            "performance_evaluation": False,
            "registration": False,
            "publication": False,
            "provider_network_credentials": False,
            "year_2025_accessed": False,
            "trading": False,
        },
    }
    report = {**core, "report_id": sha256_json(core)}
    output = output_root / str(report["report_id"]) / "source_compatibility.json"
    output.parent.mkdir(parents=True, exist_ok=False)
    with output.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    return report
