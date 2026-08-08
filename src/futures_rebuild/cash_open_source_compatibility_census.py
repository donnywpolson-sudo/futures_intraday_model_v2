"""Single-use, price-free 41-market source-compatibility census."""

from __future__ import annotations

import json
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
    SourceRow,
    certify_market_configuration,
    resolve_catalog_source,
    select_compatible_market_set,
    source_row_from_mapping,
)
from .errors import IntegrityError, UnauthorizedOperation
from .research_gateway_policy import SOURCE_COMPATIBILITY_CENSUS_OPERATION


SPEC_PATH = Path("configs/cash_open_41_market_source_compatibility_spec_v2.json")
PLAN_PATH = Path("configs/cash_open_41_market_source_compatibility_census_plan.json")
ACTIVE_CALENDAR_POINTER = Path("configs/active_cash_open_impulse_historical_calendar.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/cash_open_41_market_source_compatibility_census")
REQUIRED_COLUMNS = frozenset(
    {
        "actual_identity_hash",
        "disposition",
        "event_at_ns",
        "exchange_session_date",
        "source_row_sha256",
    }
)


def _read_canonical(path: Path, *, name: str) -> dict[str, object]:
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"{name} is invalid JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{name} is not canonical JSON")
    return payload


def _active_calendar(root: Path) -> tuple[dict[str, object], dict[str, object]]:
    pointer = _read_canonical(root / ACTIVE_CALENDAR_POINTER, name="active cash-open calendar pointer")
    calendar_path = root / str(pointer.get("calendar_path"))
    if sha256_file(calendar_path) != pointer.get("calendar_sha256"):
        raise IntegrityError("active cash-open calendar hash drifted")
    calendar = _read_canonical(calendar_path, name="active cash-open calendar")
    if calendar.get("calendar_id") != pointer.get("calendar_id"):
        raise IntegrityError("active cash-open calendar identity differs from pointer")
    return pointer, calendar


def build_census_plan(*, root: Path) -> dict[str, object]:
    spec = _read_canonical(root / SPEC_PATH, name="source-compatibility specification")
    spec_core = {key: value for key, value in spec.items() if key != "spec_id"}
    if spec.get("spec_id") != sha256_json(spec_core):
        raise IntegrityError("source-compatibility specification identity drifted")
    pointer, calendar = _active_calendar(root)
    prepared = spec.get("prepared_calendar")
    if (
        not isinstance(prepared, dict)
        or calendar.get("calendar_id") != prepared.get("calendar_id")
        or pointer.get("calendar_sha256") != prepared.get("sha256")
        or calendar.get("checkpoint_grid") != list(CHECKPOINT_GRID)
    ):
        raise UnauthorizedOperation("four-checkpoint calendar successor is not active")
    limits = spec.get("execution_limits")
    if not isinstance(limits, dict):
        raise IntegrityError("source-compatibility limits are absent")
    core: dict[str, object] = {
        "schema_version": "cash_open_41_market_source_compatibility_census_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED",
        "operation": SOURCE_COMPATIBILITY_CENSUS_OPERATION,
        "spec_id": spec["spec_id"],
        "markets": spec["markets"],
        "years": spec["years"],
        "active_calendar_id": calendar["calendar_id"],
        "output_root": OUTPUT_ROOT.as_posix(),
        "limits": limits,
        "authority": {
            "historical_row_read": True,
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
            SPEC_PATH.as_posix(): sha256_file(root / SPEC_PATH),
            ACTIVE_CALENDAR_POINTER.as_posix(): sha256_file(root / ACTIVE_CALENDAR_POINTER),
            str(pointer["calendar_path"]): str(pointer["calendar_sha256"]),
            ACTIVE_CATALOG_PATH.as_posix(): sha256_file(root / ACTIVE_CATALOG_PATH),
            "src/futures_rebuild/active_data_view.py": sha256_file(root / "src/futures_rebuild/active_data_view.py"),
            "src/futures_rebuild/cash_open_source_compatibility.py": sha256_file(root / "src/futures_rebuild/cash_open_source_compatibility.py"),
            "src/futures_rebuild/cash_open_source_compatibility_census.py": sha256_file(Path(__file__)),
            "src/futures_rebuild/research_gateway_policy.py": sha256_file(root / "src/futures_rebuild/research_gateway_policy.py"),
            "scripts/run_cash_open_41_market_source_compatibility_census.py": sha256_file(
                root / "scripts/run_cash_open_41_market_source_compatibility_census.py"
            ),
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def load_census_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="source-compatibility census plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    bindings = plan.get("bindings")
    if (
        plan.get("plan_id") != sha256_json(core)
        or plan.get("operation") != SOURCE_COMPATIBILITY_CENSUS_OPERATION
        or plan.get("state") != "PREPARED_NOT_EXECUTED"
        or not isinstance(bindings, dict)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("source-compatibility census plan drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["limits"]
    assert isinstance(limits, Mapping)
    return {
        "spec_id": str(plan["spec_id"]),
        "period": "2018,2019,2020,2021,2022",
        "market_count": "41",
        "purpose": "PRE_REGISTRATION_SOURCE_COMPATIBILITY_ONLY",
        "resolver": "ACTIVE_CATALOG_SELECTION_ONLY",
        "output_root": str(plan["output_root"]),
        "maximum_attempts": str(limits["maximum_attempts"]),
        "maximum_retries": str(limits["maximum_retries"]),
        "maximum_workers": str(limits["maximum_workers"]),
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


def _dependency_clock(event_at_ns: int) -> bool:
    seconds = event_at_ns // 1_000_000_000
    clock = datetime.fromtimestamp(seconds, timezone.utc).astimezone(
        __import__("zoneinfo").ZoneInfo("America/Chicago")
    ).time()
    minute = clock.hour * 60 + clock.minute
    return 8 * 60 + 30 <= minute <= 11 * 60 + 1


def _read_market(
    task: tuple[str, tuple[tuple[int, str], ...]]
) -> tuple[str, dict[str, tuple[SourceRow, ...]], dict[str, object]]:
    market, sources = task
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise IntegrityError("source-only parquet reader is unavailable") from exc
    by_session: dict[str, list[SourceRow]] = {}
    audits: dict[str, object] = {}
    for year, raw_path in sources:
        path = Path(raw_path)
        parquet = pq.ParquetFile(path)
        if not REQUIRED_COLUMNS.issubset(parquet.schema_arrow.names):
            raise IntegrityError(f"source-only schema is incomplete for {market} {year}")
        total = retained = sessionless_required = 0
        for batch in parquet.iter_batches(batch_size=65_536, columns=sorted(REQUIRED_COLUMNS)):
            columns = batch.to_pydict()
            for index in range(batch.num_rows):
                total += 1
                row = {name: values[index] for name, values in columns.items()}
                event = row.get("event_at_ns")
                if type(event) is not int or not _dependency_clock(event):
                    continue
                if not isinstance(row.get("exchange_session_date"), str):
                    sessionless_required += 1
                    continue
                normalized = source_row_from_mapping(market=market, row=row)
                by_session.setdefault(normalized.session, []).append(normalized)
                retained += 1
        audits[f"{market}/{year}"] = {
            "total_rows_scanned": total,
            "dependency_horizon_rows_retained": retained,
            "sessionless_dependency_horizon_rows": sessionless_required,
            "source_path": path.as_posix(),
            "source_sha256": sha256_file(path),
        }
    return market, {key: tuple(value) for key, value in by_session.items()}, audits


def execute_census_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt
) -> dict[str, object]:
    started = monotonic()
    plan = load_census_plan(root=root)
    scope = required_scope(root=root, plan=plan)
    output_root = root / str(plan["output_root"])
    boundary.assert_active_path(
        output_root, purpose="unpublished source-compatibility census",
        subtree="state/unpublished_evidence",
    )
    if output_root.exists():
        raise UnauthorizedOperation("source-compatibility census output already exists")
    use_path = receipt.consume(
        boundary,
        operation=SOURCE_COMPATIBILITY_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    spec = _read_canonical(root / SPEC_PATH, name="source-compatibility specification")
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
    configurations = (PRIMARY_CONFIG, *FALLBACK_PAIRS, *SINGLE_CONFIGS)
    market_results: list[dict[str, object]] = []
    passing: dict[tuple[str, ...], list[str]] = {tuple(item): [] for item in configurations}
    source_audits: dict[str, object] = {}
    for market in plan["markets"]:
        sessions, audits = observed[str(market)]
        source_audits.update(audits)
        sessionless = sum(
            int(item["sessionless_dependency_horizon_rows"])
            for item in audits.values()
        )
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
                market=str(market), checkpoints=config,
                eligible_sessions=eligible, rows_by_session=sessions,
                catalog_complete=not failures, catalog_failures=failures,
            )
            market_results.append(result)
            if result["status"] == "PASS":
                passing[tuple(config)].append(str(market))
    selection = select_compatible_market_set(passing)
    if monotonic() - started > int(limits["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("source-compatibility census exceeded total runtime")
    bindings = dict(plan["bindings"])
    bindings.update(source_bindings)
    core: dict[str, object] = {
        "schema_version": "cash_open_41_market_source_compatibility_report/1.0.0",
        "state": "SEALED_UNPUBLISHED_SOURCE_ONLY_EVIDENCE",
        "plan_id": plan["plan_id"],
        "spec_id": plan["spec_id"],
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
