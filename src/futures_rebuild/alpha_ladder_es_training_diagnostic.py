"""Price-free ES diagnostic for the V5 no-executable-pilot decision."""

from __future__ import annotations

import os
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from time import monotonic

from .active_data_view import resolve
from .alpha_ladder_combined_readiness import (
    ACTIVE_CATALOG_PATH,
    CHECKPOINT,
    YEARS,
    _active_calendar,
    _read_canonical,
)
from .alpha_ladder_combined_readiness_v3 import (
    EMBARGO_SESSIONS,
    EVALUATION_SESSIONS,
    PURGE_MINUTES,
    TRAINING_SESSIONS,
)
from .alpha_ladder_limit_readiness import (
    MECHANISM_ID,
    MECHANISM_PATH,
    MECHANISM_SHA256,
    SCENARIOS,
    SessionReadiness,
    _read_market,
    classify_session,
)
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .research_gateway_policy import ALPHA_LADDER_READINESS_CENSUS_OPERATION


PLAN_PATH = Path("configs/alpha_ladder_es_training_diagnostic_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_ladder_es_training_diagnostic")
RUNNER_PATH = Path("scripts/run_alpha_ladder_es_training_diagnostic.py")
V5_PLAN_PATH = Path("configs/alpha_ladder_limit_readiness_census_v5_plan.json")
V5_PLAN_ID = "d9932740cc231f3fe3d23358573f477087f7eeeec6b7c264916e5e9214c3b4cf"
V5_PLAN_SHA256 = "796881c67d2fa94cda6062820af406ee48f64fdf8b695e67b18e7064dcdc06c4"
V5_REPORT_PATH = OUTPUT_ROOT.parent / "alpha_ladder_limit_readiness_v5/readiness_report.json"
V5_REPORT_ID = "2207e76d6e0d2255dec67349385d68d5c72d212c3a9021ca69dd3ebbdef5488b"
V5_SELECTION_PATH = OUTPUT_ROOT.parent / "alpha_ladder_limit_readiness_v5/pilot_fold_selection.json"
V5_SELECTION_ID = "87cef6ab6640c546252c92dda2c9a9ec0e8c902264ebc2e1da835d24770e0b47"


def _write_once(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def _canonical_failure(item: SessionReadiness) -> str | None:
    if item.feature_complete and item.path_complete:
        return None
    if not item.feature_complete:
        return item.dispositions[0]
    failures = tuple(value for value in item.dispositions
                     if "MISSING" in value or "CHANGING" in value)
    return failures[0] if failures else "EXECUTION_PATH_INCOMPLETE"


def summarize_windows(
    *, sessions: Sequence[str], results: Mapping[str, SessionReadiness],
) -> dict[str, object]:
    needed = TRAINING_SESSIONS + EMBARGO_SESSIONS + EVALUATION_SESSIONS
    if tuple(sessions) != tuple(sorted(set(sessions))) or len(sessions) < needed:
        raise IntegrityError("diagnostic sessions cannot form a locked pilot window")
    missing = set(sessions) - set(results)
    if missing:
        raise IntegrityError("diagnostic session accounting is incomplete")
    windows = []
    aggregate_exclusions = Counter()
    for start in range(len(sessions) - needed + 1):
        training = tuple(sessions[start:start + TRAINING_SESSIONS])
        embargo = sessions[start + TRAINING_SESSIONS]
        evaluation = tuple(sessions[
            start + TRAINING_SESSIONS + EMBARGO_SESSIONS:
            start + needed
        ])
        training_items = tuple(results[item] for item in training)
        evaluation_items = tuple(results[item] for item in evaluation)
        exclusions = Counter()
        for item in training_items:
            reason = _canonical_failure(item)
            if reason is not None:
                exclusions[reason] += 1
                aggregate_exclusions[reason] += 1
        complete_training = sum(item.feature_complete and item.path_complete
                                for item in training_items)
        feature_training = sum(item.feature_complete for item in training_items)
        complete_evaluation = sum(item.feature_complete and item.path_complete
                                  for item in evaluation_items)
        feature_evaluation = sum(item.feature_complete for item in evaluation_items)
        selected_evaluation = sum(item.selected for item in evaluation_items)
        selected_paths = sum(item.selected and item.path_complete for item in evaluation_items)
        windows.append({
            "calendar_start_offset": start,
            "training_first_session": training[0], "training_last_session": training[-1],
            "embargo_session": embargo,
            "evaluation_first_session": evaluation[0],
            "evaluation_last_session": evaluation[-1],
            "expected_training_sessions": TRAINING_SESSIONS,
            "complete_training_sessions": complete_training,
            "training_shortfall_sessions": TRAINING_SESSIONS - complete_training,
            "feature_complete_training_sessions": feature_training,
            "path_incomplete_training_sessions": feature_training - complete_training,
            "training_exclusion_reasons": dict(sorted(exclusions.items())),
            "expected_evaluation_sessions": EVALUATION_SESSIONS,
            "feature_complete_evaluation_sessions": feature_evaluation,
            "execution_complete_evaluation_sessions": complete_evaluation,
            "candidate_selected_evaluation_sessions": selected_evaluation,
            "candidate_selected_path_complete_sessions": selected_paths,
        })
    best = max(item["complete_training_sessions"] for item in windows)
    best_windows = [item for item in windows if item["complete_training_sessions"] == best]
    return {
        "candidate_window_count": len(windows),
        "maximum_complete_training_sessions": best,
        "minimum_training_shortfall_sessions": TRAINING_SESSIONS - best,
        "best_window_count": len(best_windows),
        "first_best_window": best_windows[0],
        "last_best_window": best_windows[-1],
        "aggregate_window_training_exclusion_counts": dict(sorted(aggregate_exclusions.items())),
        "windows": windows,
    }


def build_plan(*, root: Path) -> dict[str, object]:
    v5_plan = _read_canonical(root / V5_PLAN_PATH, name="V5 readiness plan")
    report = _read_canonical(root / V5_REPORT_PATH, name="V5 readiness report")
    selection = _read_canonical(root / V5_SELECTION_PATH, name="V5 pilot selection")
    if (
        v5_plan.get("plan_id") != V5_PLAN_ID
        or sha256_file(root / V5_PLAN_PATH) != V5_PLAN_SHA256
        or report.get("report_id") != V5_REPORT_ID
        or report.get("state") != "SEALED_UNPUBLISHED_NO_EXECUTABLE_PILOT_FOLD"
        or selection.get("selection_id") != V5_SELECTION_ID
        or selection.get("decision") != "NO_EXECUTABLE_PILOT_FOLD"
        or selection.get("candidate_count") != 724
    ):
        raise IntegrityError("V5 diagnostic predecessor evidence changed")
    pointer, calendar = _active_calendar(root)
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    entries = {(str(item["market"]), int(item["year"])): item for item in catalog["entries"]}
    sources = {}
    for year in YEARS:
        item = entries.get(("ES", year))
        if item is None or item.get("disposition") != "RESEARCH_READY_CAUSAL_PRICE":
            raise IntegrityError(f"active catalog cannot bind ES {year}")
        sources[str(item["parquet_path"])] = str(item["parquet_sha256"])
    bindings = {
        V5_PLAN_PATH.as_posix(): V5_PLAN_SHA256,
        V5_REPORT_PATH.as_posix(): sha256_file(root / V5_REPORT_PATH),
        V5_SELECTION_PATH.as_posix(): sha256_file(root / V5_SELECTION_PATH),
        MECHANISM_PATH.as_posix(): MECHANISM_SHA256,
        ACTIVE_CATALOG_PATH.as_posix(): sha256_file(root / ACTIVE_CATALOG_PATH),
        "configs/active_cash_open_impulse_historical_calendar.json": sha256_file(
            root / "configs/active_cash_open_impulse_historical_calendar.json"),
        str(pointer["calendar_path"]): str(pointer["calendar_sha256"]),
        "src/futures_rebuild/alpha_ladder_es_training_diagnostic.py": sha256_file(Path(__file__)),
        RUNNER_PATH.as_posix(): sha256_file(root / RUNNER_PATH),
        **sources,
    }
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_es_training_diagnostic_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES",
        "operation": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "mechanism_id": MECHANISM_ID, "mechanism_sha256": MECHANISM_SHA256,
        "predecessor_v5_plan_id": V5_PLAN_ID,
        "predecessor_v5_report_id": V5_REPORT_ID,
        "predecessor_v5_selection_id": V5_SELECTION_ID,
        "market": "ES", "years": list(YEARS), "checkpoint": CHECKPOINT,
        "purpose": "EXACT_ES_504_SESSION_TRAINING_SHORTFALL_DIAGNOSTIC_ONLY",
        "price_free_output": True,
        "required_outputs": [
            "every_eligible_session_terminal_disposition",
            "every_504_1_63_window_exact_counts",
            "canonical_training_exclusion_counts",
            "best_window_and_minimum_shortfall",
        ],
        "execution_limits": {"maximum_attempts": 1, "maximum_retries": 0,
                             "maximum_workers": 1, "maximum_runtime_seconds": 900,
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
    plan = _read_canonical(root / PLAN_PATH, name="ES training diagnostic plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    bindings = plan.get("bindings")
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
        or plan.get("market") != "ES" or plan.get("years") != list(YEARS)
        or plan.get("price_free_output") is not True
        or plan.get("purpose") != "EXACT_ES_504_SESSION_TRAINING_SHORTFALL_DIAGNOSTIC_ONLY"
        or plan.get("output_root") != OUTPUT_ROOT.as_posix()
        or plan.get("authority") != expected_authority
        or plan.get("execution_limits") != {"maximum_attempts": 1, "maximum_retries": 0,
            "maximum_workers": 1, "maximum_runtime_seconds": 900,
            "maximum_external_cost_usd": "0", "windows_host_required": True}
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("ES training diagnostic plan drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "mechanism_id": MECHANISM_ID, "period": "2018,2019,2020,2021,2022",
        "markets": "ES", "checkpoint": CHECKPOINT,
        "purpose": str(plan["purpose"]), "output_root": OUTPUT_ROOT.as_posix(),
        "maximum_attempts": "1", "maximum_retries": "0", "maximum_workers": "1",
        "maximum_runtime_seconds": "900", "maximum_external_cost_usd": "0",
        "returns": "false", "model_fit": "false", "prediction_generation": "false",
        "performance_evaluation": "false", "registration": "false",
        "trial_execution": "false", "provider_network_access": "false",
        "holdout_2025_access": "false", "active_data_mutation": "false",
        "trading": "false", "approval_command": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def execute_once(*, root: Path, boundary: RepoBoundary, receipt: OperationReceipt):
    started = monotonic(); plan = load_plan(root=root)
    if os.name != "nt":
        raise UnauthorizedOperation("ES diagnostic requires Windows host execution")
    if (root / OUTPUT_ROOT).exists():
        raise UnauthorizedOperation("ES diagnostic output already exists")
    use_path = receipt.consume(
        boundary, operation=ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    entries = {(str(item["market"]), int(item["year"])): item for item in catalog["entries"]}
    sources = []
    for year in YEARS:
        item = entries[("ES", year)]
        path = resolve(repository_root=root, market="ES", year=year, purpose="SELECTION")
        if sha256_file(path) != item["parquet_sha256"]:
            raise IntegrityError(f"active ES source changed for {year}")
        sources.append((year, str(path)))
    mechanism = _read_canonical(root / MECHANISM_PATH, name="counted mechanism")
    costs = {scenario: int(mechanism["costs"]["round_trip_adverse_ticks"][scenario]["ES"])
             for scenario in SCENARIOS}
    market, prices, _risk, audits = _read_market(("ES", tuple(sources), costs))
    if market != "ES":
        raise IntegrityError("ES diagnostic worker returned another market")
    _pointer, calendar = _active_calendar(root)
    sessions = tuple(str(item["trade_date"]) for item in calendar["calendar_rows"]
                     if item["market"] == "ES" and item["checkpoint_open"].get(CHECKPOINT) is True)
    results = {session: classify_session(session=session, bars=prices.get(session, ()),
                                         cost_ticks=costs) for session in sessions}
    summary = summarize_windows(sessions=sessions, results=results)
    session_records = [{
        "session": session, "feature_complete": item.feature_complete,
        "selected": item.selected, "path_complete": item.path_complete,
        "canonical_failure": _canonical_failure(item),
        "dispositions": list(item.dispositions),
        "scenario_risk": dict(sorted(item.scenario_risk.items())),
    } for session, item in results.items()]
    if monotonic() - started > 900:
        raise UnauthorizedOperation("ES diagnostic exceeded maximum runtime")
    disposition_counts = Counter(value for item in results.values() for value in item.dispositions)
    failure_counts = Counter(value for item in results.values()
                             if (value := _canonical_failure(item)) is not None)
    core = {
        "schema_version": "alpha_ladder_es_training_diagnostic/1.0.0",
        "state": "SEALED_UNPUBLISHED_PRICE_FREE_DIAGNOSTIC",
        "plan_id": plan["plan_id"], "mechanism_id": MECHANISM_ID,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "eligible_session_count": len(sessions), "terminal_session_count": len(results),
        "session_accounting_percent": 100,
        "session_canonical_failure_counts": dict(sorted(failure_counts.items())),
        "all_disposition_counts": dict(sorted(disposition_counts.items())),
        "source_audits": audits, "window_summary": summary,
        "session_records": session_records, "authority": plan["authority"],
    }
    report = {**core, "report_id": sha256_json(core)}
    _write_once(root / OUTPUT_ROOT / "diagnostic_report.json", report)
    return report
