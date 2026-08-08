"""Transition-stable Alpha readiness successor with exact frozen folds.

This additive implementation preserves the executed V2 census and corrects
two pre-registration mechanics only: the locked 40-minute purge and selection
of the earliest row-executable rolling 504/63 ES pilot window.  Selection uses
readiness dispositions only and has no return, model, prediction, or economic
input.
"""

from __future__ import annotations

import hashlib
import multiprocessing
import os
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from time import monotonic

from .active_data_view import resolve
from .alpha_ladder_combined_readiness import (
    ACTIVE_CALENDAR_POINTER,
    ACTIVE_CATALOG_PATH,
    CHECKPOINT,
    CORE,
    MANDATORY_BASELINES,
    MECHANISM_ID,
    MECHANISM_PATH,
    MECHANISM_SHA256,
    SCENARIOS,
    TIER0_DECISION_PATH,
    TIER0_DECISION_SHA256,
    YEARS,
    _active_calendar,
    _fold_evidence,
    _read_canonical,
    _read_market,
    _write_once,
)
from .alpha_ladder_frozen_mechanism import validate_frozen_mechanism
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


TRAINING_SESSIONS = 504
EVALUATION_SESSIONS = 63
EMBARGO_SESSIONS = 1
OUTER_FOLDS = 8
PURGE_MINUTES = 40
PLAN_PATH = Path("configs/alpha_ladder_combined_readiness_census_v3_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_ladder_combined_readiness_v3")
RUNNER_PATH = Path("scripts/run_alpha_ladder_combined_readiness_census_v3.py")

PREDECESSOR_BINDINGS = {
    "configs/alpha_ladder_combined_readiness_census_v2_plan.json":
        "9ea4d1a481db213f46d30b4313b38adfc0b4574616af5ed914cc74452d66aaba",
    "state/unpublished_evidence/alpha_ladder_combined_readiness_v2/readiness_report.json":
        "1bc41b9093798ee05a33eb509512e1a94952513679452ddfffa6d3b955450efe",
    "state/unpublished_evidence/alpha_ladder_combined_readiness_v2/pilot_readiness_certificate.json":
        "83e2d9454854e317e6ad239ec02c1985c65db4df16da90ac493c87c67fcf37e0",
    "state/unpublished_evidence/alpha_ladder_combined_readiness_v2/pilot_session_manifest.json":
        "dd528e57846f3de410cd5729f4125613a9a58dbe1502605e13ae368ceec7baad",
    "state/unpublished_evidence/alpha_ladder_combined_readiness_v2/tier1_readiness_certificate.json":
        "5e702b02ea91518f3333fe16f960760b132add32281f0eae03cf8c8dae2c02de",
    "state/unpublished_evidence/alpha_ladder_combined_readiness_v2/tier1_session_manifest.json":
        "d4e5d039a67a8faf20059aec4487a7d3c315dd040baa86002ce71e70c339a0db",
    "src/futures_rebuild/alpha_ladder_combined_readiness.py":
        "6cbfdd07e698920773280bbd9912f3ac8f0b93555258f2cbe5dfb753cad4605e",
    "scripts/run_alpha_ladder_combined_readiness_census.py":
        "f77b0f52c181351dd5acdd4d433afb42ba87ef1a9a48c552b861c29f9056263d",
}


def _manifest(core: Mapping[str, object]) -> dict[str, object]:
    return {**core, "manifest_id": sha256_json(core)}


def _rolling_fold(sessions: Sequence[str], start: int) -> dict[str, object]:
    needed = TRAINING_SESSIONS + EMBARGO_SESSIONS + EVALUATION_SESSIONS
    if start < 0 or start + needed > len(sessions):
        raise IntegrityError("rolling pilot fold is outside the eligible calendar")
    training_end = start + TRAINING_SESSIONS
    evaluation_start = training_end + EMBARGO_SESSIONS
    return {
        "fold_id": "fold-0",
        "calendar_start_offset": start,
        "training_sessions": list(sessions[start:training_end]),
        "embargo_sessions": list(sessions[training_end:evaluation_start]),
        "evaluation_sessions": list(
            sessions[evaluation_start:evaluation_start + EVALUATION_SESSIONS]
        ),
        "purge_minutes": PURGE_MINUTES,
    }


def _outer_folds(sessions: Sequence[str]) -> tuple[dict[str, object], ...]:
    ordered = tuple(sessions)
    if ordered != tuple(sorted(set(ordered))):
        raise IntegrityError("Tier 1 eligible sessions are not unique and chronological")
    required = (
        TRAINING_SESSIONS + (OUTER_FOLDS - 1) * EVALUATION_SESSIONS
        + EMBARGO_SESSIONS + EVALUATION_SESSIONS
    )
    if len(ordered) < required:
        raise IntegrityError("eligible calendar cannot support eight locked Tier 1 folds")
    result = []
    for index in range(OUTER_FOLDS):
        fit_count = TRAINING_SESSIONS + index * EVALUATION_SESSIONS
        evaluation_start = fit_count + EMBARGO_SESSIONS
        result.append({
            "fold_id": f"fold-{index}",
            "training_sessions": list(ordered[:fit_count]),
            "embargo_sessions": list(ordered[fit_count:evaluation_start]),
            "evaluation_sessions": list(
                ordered[evaluation_start:evaluation_start + EVALUATION_SESSIONS]
            ),
            "purge_minutes": PURGE_MINUTES,
        })
    return tuple(result)


def _candidate_failed_gates(evidence: Mapping[str, object]) -> tuple[str, ...]:
    certificate = build_fold_readiness_certificate(
        trial_family="alpha_ladder_frozen_mechanism_pilot_selection",
        protocol_id=MECHANISM_ID,
        source_bindings={"selection-probe.json": "0" * 64},
        fold_evidence=(evidence,), required_markets=("ES",),
        required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=SCENARIOS, required_outer_fold_ids=("fold-0",),
        required_nested_fold_ids=(), expected_outer_folds=1, expected_nested_folds=0,
        minimum_training_sessions=TRAINING_SESSIONS,
        minimum_evaluation_sessions=EVALUATION_SESSIONS,
        minimum_purge_minutes=PURGE_MINUTES,
        minimum_embargo_sessions=EMBARGO_SESSIONS,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )
    result = certificate["fold_market_results"][0]
    assert isinstance(result, Mapping)
    failed = result["failed_gates"]
    assert isinstance(failed, list)
    return tuple(str(item) for item in failed)


def select_earliest_executable_pilot(
    *, sessions: Sequence[str], rows_by_session: Mapping[str, object],
    risk_by_session: Mapping[str, object],
    evidence_builder: Callable[..., Mapping[str, object]] = _fold_evidence,
) -> tuple[dict[str, object] | None, Mapping[str, object] | None, dict[str, object]]:
    """Select on readiness only; all earlier candidates must fail the locked gate."""

    needed = TRAINING_SESSIONS + EMBARGO_SESSIONS + EVALUATION_SESSIONS
    candidates = len(sessions) - needed + 1
    if candidates <= 0:
        return None, None, {
            "selection_rule": "EARLIEST_ROW_EXECUTABLE_ROLLING_504_1_63",
            "selection_inputs": "SOURCE_READINESS_ONLY_NO_RETURNS",
            "candidate_count": 0, "selected_calendar_start_offset": None,
            "candidate_results": [], "decision": "NO_EXECUTABLE_PILOT_FOLD",
        }
    summaries = []
    for start in range(candidates):
        fold = _rolling_fold(sessions, start)
        evidence = evidence_builder(
            market="ES", fold=fold, rows_by_session=rows_by_session,
            risk_by_session=risk_by_session,
        )
        failed = _candidate_failed_gates(evidence)
        summaries.append({
            "calendar_start_offset": start,
            "training_first_session": fold["training_sessions"][0],
            "training_last_session": fold["training_sessions"][-1],
            "evaluation_first_session": fold["evaluation_sessions"][0],
            "evaluation_last_session": fold["evaluation_sessions"][-1],
            "status": "PASS" if not failed else "FAIL",
            "failed_gates": list(failed),
        })
        if not failed:
            return fold, evidence, {
                "selection_rule": "EARLIEST_ROW_EXECUTABLE_ROLLING_504_1_63",
                "selection_inputs": "SOURCE_READINESS_ONLY_NO_RETURNS",
                "candidate_count": candidates,
                "candidates_examined": start + 1,
                "selected_calendar_start_offset": start,
                "candidate_results": summaries,
                "decision": "EARLIEST_EXECUTABLE_PILOT_SELECTED",
            }
    return None, None, {
        "selection_rule": "EARLIEST_ROW_EXECUTABLE_ROLLING_504_1_63",
        "selection_inputs": "SOURCE_READINESS_ONLY_NO_RETURNS",
        "candidate_count": candidates, "candidates_examined": candidates,
        "selected_calendar_start_offset": None,
        "candidate_results": summaries, "decision": "NO_EXECUTABLE_PILOT_FOLD",
    }


def validate_selection(
    selection: Mapping[str, object], *, selected_fold: Mapping[str, object] | None,
) -> None:
    results = selection.get("candidate_results")
    if (
        selection.get("selection_rule") != "EARLIEST_ROW_EXECUTABLE_ROLLING_504_1_63"
        or selection.get("selection_inputs") != "SOURCE_READINESS_ONLY_NO_RETURNS"
        or not isinstance(results, list)
    ):
        raise IntegrityError("pilot selection evidence is malformed")
    offsets = [item.get("calendar_start_offset") for item in results if isinstance(item, Mapping)]
    if offsets != list(range(len(results))):
        raise IntegrityError("pilot candidates were not examined chronologically")
    passing = [item for item in results if isinstance(item, Mapping) and item.get("status") == "PASS"]
    selected = selection.get("selected_calendar_start_offset")
    if selected_fold is None:
        if selected is not None or passing:
            raise IntegrityError("absent pilot fold has a passing candidate")
        return
    if (
        type(selected) is not int or selected != selected_fold.get("calendar_start_offset")
        or len(passing) != 1 or passing[0].get("calendar_start_offset") != selected
        or results[-1] != passing[0]
    ):
        raise IntegrityError("pilot selection is not the earliest passing candidate")


def build_plan(*, root: Path) -> dict[str, object]:
    from .alpha_ladder_combined_readiness import build_plan as build_v2_plan

    base = build_v2_plan(root=root)
    for relative, expected in PREDECESSOR_BINDINGS.items():
        if sha256_file(root / relative) != expected:
            raise IntegrityError(f"executed V2 evidence changed: {relative}")
    bindings = dict(base["bindings"])
    bindings.update(PREDECESSOR_BINDINGS)
    bindings.update({
        "src/futures_rebuild/alpha_ladder_combined_readiness_v3.py": sha256_file(Path(__file__)),
        RUNNER_PATH.as_posix(): sha256_file(root / RUNNER_PATH),
    })
    core = {key: value for key, value in base.items() if key != "plan_id"}
    core.update({
        "schema_version": "alpha_ladder_combined_readiness_census_plan/2.0.0",
        "state": "PREPARED_NOT_EXECUTED_ONE_NEW_ATTEMPT_ZERO_RETRIES",
        "output_root": OUTPUT_ROOT.as_posix(),
        "pilot": {
            "market": "ES", "training_sessions": TRAINING_SESSIONS,
            "evaluation_sessions": EVALUATION_SESSIONS,
            "embargo_sessions": EMBARGO_SESSIONS, "purge_minutes": PURGE_MINUTES,
            "selection_rule": "EARLIEST_ROW_EXECUTABLE_ROLLING_504_1_63",
            "selection_inputs": "SOURCE_READINESS_ONLY_NO_RETURNS",
        },
        "tier_1": {
            "markets": list(CORE), "outer_folds": OUTER_FOLDS,
            "initial_training_sessions": TRAINING_SESSIONS,
            "evaluation_sessions": EVALUATION_SESSIONS,
            "embargo_sessions": EMBARGO_SESSIONS, "purge_minutes": PURGE_MINUTES,
            "pilot_sessions_excluded_from_every_market": True,
        },
        "supersedes_report_id":
            "261eeba727ce682c61a096f4f18201ae2403c6ccebd1fe087e291996908b01ba",
        "supersession_reason":
            "V2_USED_31_MINUTE_PURGE_AND_DID_NOT_SEARCH_EARLIEST_EXECUTABLE_PILOT",
        "bindings": dict(sorted(bindings.items())),
    })
    return {**core, "plan_id": sha256_json(core)}


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="Alpha readiness V3 plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    bindings = plan.get("bindings")
    pilot = plan.get("pilot")
    tier1 = plan.get("tier_1")
    if (
        plan.get("plan_id") != sha256_json(core)
        or plan.get("operation") != ALPHA_LADDER_READINESS_CENSUS_OPERATION
        or plan.get("mechanism_id") != MECHANISM_ID
        or plan.get("output_root") != OUTPUT_ROOT.as_posix()
        or not isinstance(bindings, Mapping)
        or not isinstance(pilot, Mapping) or not isinstance(tier1, Mapping)
        or pilot.get("purge_minutes") != PURGE_MINUTES
        or pilot.get("selection_inputs") != "SOURCE_READINESS_ONLY_NO_RETURNS"
        or tier1.get("purge_minutes") != PURGE_MINUTES
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("Alpha readiness V3 plan drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["execution_limits"]
    assert isinstance(limits, Mapping)
    return {
        "mechanism_id": str(plan["mechanism_id"]), "period": "2018,2019,2020,2021,2022",
        "markets": ",".join(CORE), "checkpoint": CHECKPOINT,
        "purpose": "ALPHA_PILOT_AND_TIER1_ROW_READINESS_V3_ONLY",
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


def _file_sha(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_bytes(payload) + b"\n").hexdigest()


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> dict[str, object]:
    started = monotonic()
    plan = load_plan(root=root)
    if os.name != "nt" or multiprocessing.current_process().name != "MainProcess":
        raise UnauthorizedOperation("Alpha readiness V3 requires the Windows main process")
    output_root = root / OUTPUT_ROOT
    if output_root.exists():
        raise UnauthorizedOperation("Alpha readiness V3 output already exists")
    use_path = receipt.consume(
        boundary, operation=ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    catalog = _read_canonical(root / ACTIVE_CATALOG_PATH, name="active catalog")
    by_key = {(str(item["market"]), int(item["year"])): item for item in catalog["entries"]}
    mechanism = _read_canonical(root / MECHANISM_PATH, name="mechanism")
    validate_frozen_mechanism(mechanism)
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
        )
        pool.close(); pool.join()
    except BaseException:
        pool.terminate(); pool.join(); raise
    observed = {item[0]: item[1:] for item in worker_results}
    _pointer, calendar = _active_calendar(root)
    eligible = {
        market: tuple(str(item["trade_date"]) for item in calendar["calendar_rows"]
                      if item["market"] == market
                      and item["checkpoint_open"].get(CHECKPOINT) is True)
        for market in CORE
    }
    es_rows, es_risk, _audit = observed["ES"]
    pilot_fold, pilot_evidence, selection = select_earliest_executable_pilot(
        sessions=eligible["ES"], rows_by_session=es_rows, risk_by_session=es_risk,
    )
    validate_selection(selection, selected_fold=pilot_fold)
    selection_core = {
        "schema_version": "alpha_ladder_pilot_fold_selection/1.0.0",
        "plan_id": plan["plan_id"], "mechanism_id": MECHANISM_ID, **selection,
    }
    selection_report = {**selection_core, "selection_id": sha256_json(selection_core)}
    selection_rel = (OUTPUT_ROOT / "pilot_fold_selection.json").as_posix()
    bindings = dict(plan["bindings"]); bindings.update(source_bindings)
    bindings[PLAN_PATH.as_posix()] = sha256_file(root / PLAN_PATH)
    if pilot_fold is None or pilot_evidence is None:
        core = {
            "schema_version": "alpha_ladder_combined_readiness_report/2.0.0",
            "state": "SEALED_UNPUBLISHED_NO_EXECUTABLE_PILOT_FOLD",
            "plan_id": plan["plan_id"], "mechanism_id": MECHANISM_ID,
            "authorization_receipt_id": receipt.receipt_id,
            "authorization_use_path": use_path.relative_to(root).as_posix(),
            "authorization_use_sha256": sha256_file(use_path),
            "pilot_decision": "FAIL", "tier1_decision": "NOT_RUN",
            "combined_registration_ready": False,
            "pilot_selection_id": selection_report["selection_id"],
            "source_bindings": dict(sorted(bindings.items())), "authority": plan["authority"],
        }
        report = {**core, "report_id": sha256_json(core)}
        output_root.mkdir(parents=True, exist_ok=False)
        _write_once(root / selection_rel, selection_report)
        _write_once(output_root / "readiness_report.json", report)
        return report

    pilot_exclusions = tuple(pilot_fold["evaluation_sessions"])
    common = sorted(set.intersection(*(set(eligible[m]) for m in CORE)) - set(pilot_exclusions))
    tier1_folds = _outer_folds(common)
    pilot_manifest = _manifest({
        "schema_version": SESSION_MANIFEST_SCHEMA, "contract_id": plan["contract_id"],
        "mechanism_sha256": MECHANISM_SHA256, "stage": "pilot", "markets": ["ES"],
        "fold_ordinal": 0, "calendar_start_offset": pilot_fold["calendar_start_offset"],
        "selection_rule": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
        "selection_evidence_path": selection_rel,
        "selection_evidence_sha256": _file_sha(selection_report),
        "training_session_ids": pilot_fold["training_sessions"],
        "evaluation_session_ids": pilot_fold["evaluation_sessions"],
        "purge_applied": True, "embargo_applied": True,
    })
    tier1_manifest = _manifest({
        "schema_version": SESSION_MANIFEST_SCHEMA, "contract_id": plan["contract_id"],
        "mechanism_sha256": MECHANISM_SHA256, "stage": "tier_1",
        "excluded_pilot_evaluation_session_ids": list(pilot_exclusions),
        "evaluation_session_ids_by_market": {
            market: sorted({s for fold in tier1_folds for s in fold["evaluation_sessions"]})
            for market in CORE
        },
    })
    pilot_rel = (OUTPUT_ROOT / "pilot_session_manifest.json").as_posix()
    tier1_rel = (OUTPUT_ROOT / "tier1_session_manifest.json").as_posix()
    pilot_bindings = {
        **bindings, selection_rel: _file_sha(selection_report),
        pilot_rel: _file_sha(pilot_manifest),
    }
    tier1_bindings = {**bindings, tier1_rel: _file_sha(tier1_manifest)}
    pilot_cert = build_fold_readiness_certificate(
        trial_family="alpha_ladder_frozen_mechanism", protocol_id=MECHANISM_ID,
        source_bindings=pilot_bindings, fold_evidence=(pilot_evidence,),
        required_markets=("ES",), required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=SCENARIOS, required_outer_fold_ids=("fold-0",),
        required_nested_fold_ids=(), expected_outer_folds=1, expected_nested_folds=0,
        minimum_training_sessions=TRAINING_SESSIONS,
        minimum_evaluation_sessions=EVALUATION_SESSIONS,
        minimum_purge_minutes=PURGE_MINUTES,
        minimum_embargo_sessions=EMBARGO_SESSIONS,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )
    tier1_evidence = []
    for market in CORE:
        rows, risk, _audit = observed[market]
        tier1_evidence.extend(
            _fold_evidence(market=market, fold=fold, rows_by_session=rows,
                           risk_by_session=risk) for fold in tier1_folds
        )
    tier1_cert = build_fold_readiness_certificate(
        trial_family="alpha_ladder_frozen_mechanism", protocol_id=MECHANISM_ID,
        source_bindings=tier1_bindings, fold_evidence=tier1_evidence,
        required_markets=CORE, required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=SCENARIOS,
        required_outer_fold_ids=tuple(f"fold-{i}" for i in range(OUTER_FOLDS)),
        required_nested_fold_ids=(), expected_outer_folds=OUTER_FOLDS,
        expected_nested_folds=0, minimum_training_sessions=252,
        minimum_evaluation_sessions=30, minimum_purge_minutes=PURGE_MINUTES,
        minimum_embargo_sessions=EMBARGO_SESSIONS,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )
    validate_session_manifest(
        pilot_manifest, contract_id=str(plan["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256, stage="pilot", markets=("ES",),
    )
    validate_session_manifest(
        tier1_manifest, contract_id=str(plan["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256, stage="tier_1", markets=CORE,
        pilot_evaluation_sha256=sha256_json(list(pilot_exclusions)),
    )
    if monotonic() - started > int(limits["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("Alpha readiness V3 exceeded total runtime")
    audits = {key: value for market in CORE for key, value in observed[market][2].items()}
    core = {
        "schema_version": "alpha_ladder_combined_readiness_report/2.0.0",
        "state": "SEALED_UNPUBLISHED_ROW_CERTIFIED_READINESS",
        "plan_id": plan["plan_id"], "mechanism_id": MECHANISM_ID,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "pilot_decision": pilot_cert["overall_decision"],
        "tier1_decision": tier1_cert["overall_decision"],
        "combined_registration_ready": (
            pilot_cert["overall_decision"] == "PASS"
            and tier1_cert["overall_decision"] == "PASS"
        ),
        "pilot_selection_id": selection_report["selection_id"],
        "pilot_certificate_id": pilot_cert["certificate_id"],
        "tier1_certificate_id": tier1_cert["certificate_id"],
        "pilot_session_manifest_id": pilot_manifest["manifest_id"],
        "tier1_session_manifest_id": tier1_manifest["manifest_id"],
        "source_audits": dict(sorted(audits.items())),
        "source_bindings": dict(sorted(bindings.items())), "authority": plan["authority"],
    }
    report = {**core, "report_id": sha256_json(core)}
    output_root.mkdir(parents=True, exist_ok=False)
    _write_once(root / selection_rel, selection_report)
    _write_once(root / pilot_rel, pilot_manifest)
    _write_once(root / tier1_rel, tier1_manifest)
    validate_fold_readiness_certificate(pilot_cert, root=root)
    validate_fold_readiness_certificate(tier1_cert, root=root)
    _write_once(output_root / "pilot_readiness_certificate.json", pilot_cert)
    _write_once(output_root / "tier1_readiness_certificate.json", tier1_cert)
    _write_once(output_root / "readiness_report.json", report)
    return report
