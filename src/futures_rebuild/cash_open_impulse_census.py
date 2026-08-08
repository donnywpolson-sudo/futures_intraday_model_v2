"""Single-use, readiness-only census for the proposed cash-open mechanism."""

from __future__ import annotations

import json
import multiprocessing
from collections import Counter
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .cash_open_impulse_readiness import (
    MARKETS,
    SessionReadiness,
    build_source_certificate,
    iter_session_readiness,
)
from .errors import IntegrityError, UnauthorizedOperation
from .historical_checkpoint_calendar import load_historical_checkpoint_calendar
from .overnight_inventory_reversal_preexecution_census import _object, _source_map
from .tier1_bracket_v10 import SourceIntegrityAuditV10, iter_source_records_from_parquet_v10
from .tier1_bracket_v5 import load_registered_calendar_sessions_v5


OPERATION = "CENSUS_CASH_OPEN_IMPULSE_FOLD_READINESS_ONCE"
PLAN_PATH = Path("configs/cash_open_impulse_fold_readiness_census_plan.json")
OUTPUT_NAME = "fold_readiness_certificate.json"


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    bindings = plan.get("bindings")
    limits = plan.get("limits")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "cash_open_impulse_fold_readiness_census_plan/1.0.0"
        or plan.get("state") != "PREPARED_NOT_EXECUTED"
        or plan.get("operation") != OPERATION
        or plan.get("protocol_id")
        != "3b8e09d65015afd33fc033aa72c8bb0be22425cafac8b8b145eeccb639258067"
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
        or limits.get("worker_pool_timeout_seconds") != 780
        or limits.get("maximum_workers") != 4
        or limits.get("maximum_external_cost_usd") != "0"
    ):
        raise IntegrityError("cash-open readiness census plan drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "protocol_id": str(plan["protocol_id"]),
        "period": "2018,2019,2020,2021,2022",
        "markets": "ES,CL,ZN,6E",
        "purpose": "PRE_REGISTRATION_FOLD_FEATURE_EXECUTION_BASELINE_SOURCE_READINESS_ONLY",
        "output_root": str(plan["output_root"]),
        "maximum_attempts": "1", "maximum_retries": "0",
        "maximum_runtime_seconds": "900", "maximum_external_cost_usd": "0",
        "provider_or_network_access": "false", "holdout_2025_access": "false",
        "model_fit": "false", "prediction_generation": "false",
        "performance_evaluation": "false", "publication": "false", "trading": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _market_task(
    task: tuple[str, tuple[tuple[int, str], ...]],
) -> tuple[str, tuple[SessionReadiness, ...], dict[str, object]]:
    market, paths = task
    audits: dict[str, object] = {}

    def records():
        for year, path in paths:
            audit = SourceIntegrityAuditV10(market)
            yield from iter_source_records_from_parquet_v10(
                market=market, path=Path(path), audit=audit,
            )
            audits[f"{market}/{year}"] = audit.as_dict()

    return market, tuple(iter_session_readiness(
        market=market, source_records=records(),
    )), audits


def collect_parallel(
    *, paths: Mapping[tuple[str, int], Path], timeout_seconds: int = 780,
) -> tuple[list[SessionReadiness], dict[str, object]]:
    tasks = [
        (market, tuple((year, str(paths[(market, year)])) for year in range(2018, 2023)))
        for market in MARKETS
    ]
    pool = multiprocessing.get_context("spawn").Pool(processes=4)
    try:
        result = pool.map_async(_market_task, tasks, chunksize=1).get(timeout=timeout_seconds)
        pool.close()
        pool.join()
    except multiprocessing.TimeoutError as exc:
        pool.terminate()
        pool.join()
        raise UnauthorizedOperation("cash-open readiness workers reached their deadline") from exc
    except BaseException:
        pool.terminate()
        pool.join()
        raise
    observations: list[SessionReadiness] = []
    audits: dict[str, object] = {}
    for market, values, market_audits in result:
        if market not in MARKETS:
            raise IntegrityError("cash-open census returned an unknown market")
        observations.extend(values)
        audits.update(market_audits)
    return observations, dict(sorted(audits.items()))


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> dict[str, object]:
    started = monotonic()
    plan = load_plan(root=root)
    output_root = root / str(plan["output_root"])
    boundary.assert_active_path(
        output_root.absolute(), purpose="unpublished cash-open fold readiness census",
        subtree="state/unpublished_evidence",
    )
    if output_root.exists():
        raise UnauthorizedOperation("cash-open readiness output already exists")
    use_path = receipt.consume(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    manifest = _object(root / str(plan["split_manifest_path"]))
    paths, source_bindings = _source_map(root=root, manifest=manifest)
    observations, audits = collect_parallel(paths=paths)
    calendar_sessions = load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(plan["calendar_release_id"]),
    )
    loaded_calendar = load_historical_checkpoint_calendar(boundary=boundary)
    if loaded_calendar.index_receipt.release_id != plan["calendar_release_id"]:
        raise IntegrityError("cash-open census calendar changed")
    # The two proposed checkpoints require a normal cash-open session and a
    # still-open 10:30 checkpoint. Calendar-closed days are outside the
    # opportunity universe, never silently missing rows.
    expected = {
        market: tuple(
            item.exchange_session_date for item in calendar_sessions
            if item.market == market and item.checkpoint_states is not None
            and item.checkpoint_states.get("08:30") is True
            and item.checkpoint_states.get("10:30") is True
        ) for market in MARKETS
    }
    bindings = dict(source_bindings)
    raw_bindings = plan["bindings"]
    assert isinstance(raw_bindings, Mapping)
    bindings.update({str(path): str(digest) for path, digest in raw_bindings.items()})
    capture = loaded_calendar.capture_receipt.verify(boundary)
    for entry in capture.files:
        bindings[capture.physical_relative_path(entry).as_posix()] = entry.sha256
    folds = manifest.get("outer_folds")
    if not isinstance(folds, list) or len(folds) != 8:
        raise IntegrityError("cash-open census split topology changed")
    certificate = build_source_certificate(
        protocol_id=str(plan["protocol_id"]), source_bindings=bindings,
        observations=observations, outer_folds=folds,
        expected_sessions_by_market=expected,
    )
    failures = Counter(
        item.failure or "COMPLETE" for item in observations if not item.complete
    )
    if monotonic() - started > 895:
        raise UnauthorizedOperation("cash-open census exhausted runtime before sealing")
    core = {
        "schema_version": "cash_open_impulse_fold_readiness_report/1.0.0",
        "state": "UNPUBLISHED_PRE_REGISTRATION_EVIDENCE",
        "protocol_id": plan["protocol_id"], "plan_id": plan["plan_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "censused_at_utc": datetime.now(timezone.utc).isoformat(),
        "fold_readiness_certificate": certificate,
        "expected_sessions_by_market": {key: len(value) for key, value in expected.items()},
        "incomplete_session_reasons": dict(sorted(failures.items())),
        "source_audits": audits,
        "historical_economics_evaluation": False, "model_fit": False,
        "prediction_generation": False, "holdout_2025_touched": False,
        "provider_or_network_access": False, "publication": False, "trading": False,
    }
    report = {**core, "report_id": sha256_json(core)}
    output_root.mkdir(parents=True, exist_ok=False)
    output_path = output_root / OUTPUT_NAME
    with output_path.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
    if _object(output_path) != report:
        raise IntegrityError("cash-open readiness report failed byte verification")
    return report
