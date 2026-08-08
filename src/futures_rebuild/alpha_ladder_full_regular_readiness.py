"""Decisive row-readiness census for the full-regular counted Alpha mechanism.

Plan construction is metadata-only.  The executor consumes an exact one-use
historical authorization before hashing or opening any bound price payload.
"""

from __future__ import annotations

import json
import multiprocessing
import os
from collections.abc import Mapping
from pathlib import Path
from time import monotonic

from . import alpha_ladder_reported_trade_exit_readiness as base
from .alpha_ladder_full_regular_source_observable_successor import (
    CALENDAR_CLOSED,
    ELIGIBLE,
    HOLIDAY_ABSTENTION,
    SOURCE_ABSTENTION,
    build_calendar_accounting,
    classify_calendar_session,
)
from .alpha_ladder_full_regular_tier0 import (
    MECHANISM_ID,
    MECHANISM_PATH,
    MECHANISM_SHA256,
    TIER0_CERTIFICATE_PATH,
    TIER0_DECISION_PATH,
)
from .alpha_research_ladder import (
    SESSION_MANIFEST_SCHEMA,
    load_active_ladder,
    validate_stage_decision,
    validate_session_manifest,
)
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .preexecution_fold_certification import (
    ROW_CERTIFIED,
    build_fold_readiness_certificate,
    validate_fold_readiness_certificate,
)
from .research_gateway_policy import ALPHA_LADDER_READINESS_CENSUS_OPERATION


PLAN_PATH = Path("configs/alpha_ladder_full_regular_readiness_census_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_ladder_full_regular_readiness")
MODULE_PATH = Path("src/futures_rebuild/alpha_ladder_full_regular_readiness.py")
PREPARE_SCRIPT_PATH = Path(
    "scripts/prepare_alpha_ladder_full_regular_readiness_census_plan.py"
)
RUNNER_PATH = Path("scripts/run_alpha_ladder_full_regular_readiness_census.py")
TEST_PATH = Path("tests/test_alpha_ladder_full_regular_readiness.py")
TRIAL_FAMILY = "alpha_ladder_full_regular_source_observable"

DIRECT_DEPENDENCIES = frozenset({
    MODULE_PATH.as_posix(),
    PREPARE_SCRIPT_PATH.as_posix(),
    RUNNER_PATH.as_posix(),
    TEST_PATH.as_posix(),
    "src/futures_rebuild/active_data_view.py",
    "src/futures_rebuild/alpha_ladder_reported_trade_exit_readiness.py",
    "src/futures_rebuild/alpha_ladder_full_regular_source_observable_successor.py",
    "src/futures_rebuild/alpha_ladder_full_regular_tier0.py",
    "src/futures_rebuild/alpha_research_ladder.py",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/preexecution_fold_certification.py",
    "src/futures_rebuild/research_gateway_policy.py",
})


def _frozen_tier0_evidence(*, root: Path) -> dict[str, str]:
    """Validate the sealed Tier 0 result without reinterpreting its old suite."""

    certificate = base._read_canonical(
        root / TIER0_CERTIFICATE_PATH, name="frozen full-regular Tier 0 certificate",
    )
    decision = base._read_canonical(
        root / TIER0_DECISION_PATH, name="frozen full-regular Tier 0 decision",
    )
    contract, _profile = load_active_ladder(root)
    certificate_sha = sha256_file(root / TIER0_CERTIFICATE_PATH)
    validate_stage_decision(
        decision,
        contract_id=str(contract["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256,
        expected_stage="tier_0",
        root=root,
    )
    if (
        certificate.get("certificate_id")
        != "cc5535ede6b07ef78a82fc6c071f6c90106e55ab9275e408349ddc74a253a36b"
        or certificate.get("mechanism_id") != MECHANISM_ID
        or certificate.get("mechanism_sha256") != MECHANISM_SHA256
        or certificate.get("decision") != "PASS"
        or certificate.get("evidence_class") != "SYNTHETIC_ENGINEERING_ONLY"
        or certificate.get("historical_rows_opened") is not False
        or certificate.get("source_compatibility_claim") is not False
        or certificate.get("registration_authority") is not False
        or decision.get("synthetic_certificate_sha256") != certificate_sha
    ):
        raise IntegrityError("frozen full-regular Tier 0 evidence changed")
    return {
        "certificate_id": str(certificate["certificate_id"]),
        "certificate_sha256": certificate_sha,
        "decision_id": str(decision["decision_id"]),
        "decision_sha256": sha256_file(root / TIER0_DECISION_PATH),
    }


def _eligible_and_accounting(calendar: Mapping[str, object]) -> tuple[dict[str, tuple[str, ...]], dict[str, object]]:
    accounting = build_calendar_accounting(calendar)
    records = calendar.get("source_observability_records")
    rows = calendar.get("calendar_rows")
    if not isinstance(records, list) or not isinstance(rows, list):
        raise IntegrityError("calendar lacks row or source-observability records")
    source_keys = frozenset(
        (str(item["market"]), str(item["trade_date"]), str(item["checkpoint"]))
        for item in records if isinstance(item, Mapping)
    )
    eligible: dict[str, tuple[str, ...]] = {}
    inventory: list[dict[str, str]] = []
    for market in base.CORE:
        sessions: list[str] = []
        for row in rows:
            if not isinstance(row, Mapping) or row.get("market") != market:
                continue
            disposition = classify_calendar_session(
                row, source_unobservable_keys=source_keys,
            )
            session = str(row["trade_date"])
            inventory.append({
                "market": market,
                "trade_date": session,
                "checkpoint": base.CHECKPOINT,
                "disposition": disposition,
            })
            if disposition == ELIGIBLE:
                sessions.append(session)
        normalized = tuple(sessions)
        if normalized != tuple(sorted(set(normalized))):
            raise IntegrityError(f"eligible sessions are not unique and ordered: {market}")
        eligible[market] = normalized
    inventory.sort(key=lambda item: (item["market"], item["trade_date"]))
    if len(inventory) != accounting["totals"]["calendar_rows"]:
        raise IntegrityError("checkpoint inventory did not account for every calendar row")
    return eligible, {**accounting, "inventory": inventory}


def _selected_sources(root: Path) -> tuple[dict[str, str], dict[tuple[str, int], object]]:
    return base._selected_sources(root=root)


def _plan_core(*, root: Path) -> dict[str, object]:
    contract, profile = load_active_ladder(root)
    tier0 = _frozen_tier0_evidence(root=root)
    pointer, calendar = base._active_calendar(root)
    eligible, accounting = _eligible_and_accounting(calendar)
    selected, _entries = _selected_sources(root)
    minimum = base.TRAINING_SESSIONS + base.EMBARGO_SESSIONS + base.EVALUATION_SESSIONS
    required_after_pilot = (
        base.TRAINING_SESSIONS
        + (base.OUTER_FOLDS - 1) * base.EVALUATION_SESSIONS
        + base.EMBARGO_SESSIONS
        + base.EVALUATION_SESSIONS
    )
    if len(eligible["ES"]) < minimum:
        raise IntegrityError("full-regular calendar cannot support the ES pilot")
    if any(len(eligible[market]) - base.EVALUATION_SESSIONS < required_after_pilot for market in base.CORE):
        raise IntegrityError("full-regular calendar cannot support Tier 1 after pilot exclusion")
    non_payload = {
        MECHANISM_PATH.as_posix(): MECHANISM_SHA256,
        TIER0_CERTIFICATE_PATH.as_posix(): str(tier0["certificate_sha256"]),
        TIER0_DECISION_PATH.as_posix(): str(tier0["decision_sha256"]),
        "configs/active_alpha_research_ladder.json": sha256_file(
            root / "configs/active_alpha_research_ladder.json"
        ),
        base.ACTIVE_CATALOG_PATH.as_posix(): sha256_file(root / base.ACTIVE_CATALOG_PATH),
        base.ACTIVE_CALENDAR_POINTER.as_posix(): sha256_file(root / base.ACTIVE_CALENDAR_POINTER),
        str(pointer["calendar_path"]): str(pointer["calendar_sha256"]),
        **{relative: sha256_file(root / relative) for relative in DIRECT_DEPENDENCIES},
    }
    bindings = dict(sorted({**non_payload, **selected}.items()))
    return {
        "schema_version": "alpha_ladder_full_regular_readiness_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES",
        "operation": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "contract_id": contract["contract_id"],
        "profile_id": profile["profile_id"],
        "mechanism_id": MECHANISM_ID,
        "mechanism_sha256": MECHANISM_SHA256,
        "tier0_certificate_id": tier0["certificate_id"],
        "tier0_decision_id": tier0["decision_id"],
        "markets": list(base.CORE),
        "years": list(base.YEARS),
        "checkpoint": base.CHECKPOINT,
        "session_eligibility": {
            "applied_before_fold_construction": True,
            "eligible_disposition": ELIGIBLE,
            "explicit_abstentions": [CALENDAR_CLOSED, HOLIDAY_ABSTENTION, SOURCE_ABSTENTION],
            "all_checkpoint_rows_accounted_percent": 100,
            "silent_drop_allowed": False,
            "selected_using_returns": False,
            "predata_counts": accounting["totals"],
        },
        "pilot": {
            "market": "ES",
            "training_sessions": base.TRAINING_SESSIONS,
            "evaluation_sessions": base.EVALUATION_SESSIONS,
            "embargo_sessions": base.EMBARGO_SESSIONS,
            "purge_minutes": base.PURGE_MINUTES,
            "selection_rule": "EARLIEST_ROW_EXECUTABLE_ROLLING_504_1_63_NO_RETURNS",
        },
        "tier_1": {
            "markets": list(base.CORE),
            "outer_folds": base.OUTER_FOLDS,
            "pilot_sessions_excluded_from_every_market": True,
            "calendar_basis": "FULL_REGULAR_SOURCE_OBSERVABLE_BEFORE_FOLDS",
        },
        "entry_semantics": "ONE_TICK_PENETRATION_RESTING_LIMIT_OR_EXPLICIT_NO_TRADE",
        "exit_semantics": "PROTECTIVE_STOP_OR_FIRST_VALID_SAME_IDENTITY_REPORTED_TRADE_BAR_OPEN_AFTER_SCHEDULED_MARKET_EXIT_ORDER",
        "required_baselines": list(base.MANDATORY_BASELINES),
        "required_cost_scenarios": list(base.SCENARIOS),
        "coverage": {
            "checkpoint_accounting_percent": 100,
            "active_baseline_checkpoint_accounting_percent": 100,
            "filled_entry_verified_exit_percent": 100,
            "future_complete_path_filtering": False,
        },
        "required_outputs": [
            "checkpoint_accounting.json", "source_audit.json",
            "pilot_fold_selection.json", "readiness_report.json",
            "pilot_session_manifest.json", "tier1_session_manifest.json",
            "pilot_readiness_certificate.json", "tier1_readiness_certificate.json",
        ],
        "execution_limits": {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_workers": 4,
            "worker_deadline_seconds": 3300,
            "maximum_runtime_seconds": 3600,
            "maximum_external_cost_usd": "0",
            "windows_host_required": True,
        },
        "output_root": OUTPUT_ROOT.as_posix(),
        "authority": {
            "historical_row_read": True,
            "returns": False,
            "model_fit": False,
            "prediction_generation": False,
            "performance_evaluation": False,
            "registration": False,
            "trial_execution": False,
            "publication": False,
            "provider_network_credentials": False,
            "year_2025_access": False,
            "active_data_mutation": False,
            "trading": False,
        },
        "calendar_id": calendar["calendar_id"],
        "protected_source_paths": sorted(selected),
        "bindings": bindings,
    }


def build_plan(*, root: Path) -> dict[str, object]:
    core = _plan_core(root=root)
    return {**core, "plan_id": sha256_json(core)}


def validate_plan(plan: Mapping[str, object], *, root: Path, verify_protected: bool = False) -> dict[str, object]:
    expected = build_plan(root=root)
    if dict(plan) != expected:
        raise IntegrityError("full-regular readiness plan drifted")
    protected = set(str(item) for item in plan["protected_source_paths"])
    if verify_protected:
        bindings = plan["bindings"]
        assert isinstance(bindings, Mapping)
        for relative in protected:
            if sha256_file(root / relative) != bindings[relative]:
                raise IntegrityError(f"protected source changed: {relative}")
    return dict(plan)


def load_plan(*, root: Path, verify_protected: bool = False) -> dict[str, object]:
    plan = base._read_canonical(root / PLAN_PATH, name="full-regular readiness plan")
    return validate_plan(plan, root=root, verify_protected=verify_protected)


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["execution_limits"]
    assert isinstance(limits, Mapping)
    return {
        "mechanism_id": MECHANISM_ID,
        "period": "2018,2019,2020,2021,2022",
        "markets": ",".join(base.CORE),
        "checkpoint": base.CHECKPOINT,
        "purpose": "ALPHA_FULL_REGULAR_PILOT_AND_TIER1_READINESS_ONLY",
        "output_root": OUTPUT_ROOT.as_posix(),
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "maximum_workers": "4",
        "worker_deadline_seconds": str(limits["worker_deadline_seconds"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_external_cost_usd": "0",
        "returns": "false",
        "model_fit": "false",
        "prediction_generation": "false",
        "performance_evaluation": "false",
        "registration": "false",
        "trial_execution": "false",
        "provider_network_access": "false",
        "holdout_2025_access": "false",
        "active_data_mutation": "false",
        "trading": "false",
        "approval_command": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _write_failure(
    *, root: Path, plan: Mapping[str, object], receipt: OperationReceipt,
    use_path: Path, accounting: Mapping[str, object], source_audit: Mapping[str, object],
    selection: Mapping[str, object], state: str,
) -> dict[str, object]:
    report_core = {
        "schema_version": "alpha_ladder_full_regular_readiness_report/1.0.0",
        "state": state,
        "plan_id": plan["plan_id"],
        "mechanism_id": MECHANISM_ID,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "checkpoint_accounting_id": accounting["accounting_id"],
        "source_audit_id": source_audit["audit_id"],
        "pilot_selection_id": selection["selection_id"],
        "pilot_decision": "FAIL",
        "tier1_decision": "NOT_RUN",
        "combined_registration_ready": False,
        "authority": plan["authority"],
    }
    report = {**report_core, "report_id": sha256_json(report_core)}
    output = root / OUTPUT_ROOT
    output.mkdir(parents=True, exist_ok=False)
    for name, payload in (
        ("checkpoint_accounting.json", accounting),
        ("source_audit.json", source_audit),
        ("pilot_fold_selection.json", selection),
        ("readiness_report.json", report),
    ):
        base._write_once(output / name, payload)
    return report


def execute_once(*, root: Path, boundary: RepoBoundary, receipt: OperationReceipt) -> Mapping[str, object]:
    """Consume authority, then read once and seal a PASS or rejection."""

    started = monotonic()
    plan = load_plan(root=root, verify_protected=False)
    if os.name != "nt" or multiprocessing.current_process().name != "MainProcess":
        raise UnauthorizedOperation("readiness census requires the Windows main process")
    if (root / OUTPUT_ROOT).exists():
        raise UnauthorizedOperation("readiness census output already exists")
    use_path = receipt.consume(
        boundary,
        operation=ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    plan = load_plan(root=root, verify_protected=True)
    selected, by_key = _selected_sources(root)
    mechanism = base._read_canonical(root / MECHANISM_PATH, name="full-regular mechanism")
    costs = mechanism["costs"]["round_trip_adverse_ticks"]
    tasks = []
    market_costs: dict[str, dict[str, int]] = {}
    for market in base.CORE:
        sources = []
        for year in base.YEARS:
            item = by_key[(market, year)]
            assert isinstance(item, Mapping)
            path = base.resolve(repository_root=root, market=market, year=year, purpose="SELECTION")
            relative = path.relative_to(root).as_posix()
            if selected.get(relative) != sha256_file(path):
                raise IntegrityError(f"active source changed for {market} {year}")
            sources.append((year, str(path)))
        market_costs[market] = {
            scenario: int(costs[scenario][market]) for scenario in base.SCENARIOS
        }
        tasks.append((market, tuple(sources), market_costs[market]))
    pool = multiprocessing.get_context("spawn").Pool(processes=4)
    try:
        worker_results = pool.map_async(base._read_market, tasks, chunksize=1).get(
            timeout=int(plan["execution_limits"]["worker_deadline_seconds"])
        )
        pool.close()
        pool.join()
    except BaseException:
        pool.terminate()
        pool.join()
        raise
    observed = {item[0]: item[1:] for item in worker_results}
    _pointer, calendar = base._active_calendar(root)
    eligible, raw_accounting = _eligible_and_accounting(calendar)
    accounting_core = {
        "schema_version": "alpha_ladder_full_regular_checkpoint_accounting/1.0.0",
        "plan_id": plan["plan_id"],
        "mechanism_id": MECHANISM_ID,
        **raw_accounting,
    }
    accounting = {**accounting_core, "accounting_id": sha256_json(accounting_core)}
    audit_core = {
        "schema_version": "alpha_ladder_full_regular_source_audit/1.0.0",
        "plan_id": plan["plan_id"],
        "mechanism_id": MECHANISM_ID,
        "price_free": True,
        "source_bindings": selected,
        "worker_audits": {market: observed[market][2] for market in base.CORE},
        "eligible_session_counts": {market: len(eligible[market]) for market in base.CORE},
    }
    source_audit = {**audit_core, "audit_id": sha256_json(audit_core)}
    es_prices = observed["ES"][0]
    es_cost_rows = {**es_prices, "__cost_ticks__": market_costs["ES"]}
    es_cache = base._session_cache(
        sessions=eligible["ES"], bars_by_session=es_prices,
        cost_ticks=market_costs["ES"],
    )
    def evidence_builder(**kwargs):
        return base._fold_evidence(**kwargs, cache=es_cache)
    pilot_fold, pilot_evidence, selection_raw = base.select_earliest_executable_pilot(
        sessions=eligible["ES"], rows_by_session=es_cost_rows,
        risk_by_session={}, evidence_builder=evidence_builder,
    )
    base.validate_selection(selection_raw, selected_fold=pilot_fold)
    selection_core = {
        "schema_version": "alpha_ladder_full_regular_pilot_selection/1.0.0",
        "plan_id": plan["plan_id"], "mechanism_id": MECHANISM_ID, **selection_raw,
    }
    selection = {**selection_core, "selection_id": sha256_json(selection_core)}
    if pilot_fold is None or pilot_evidence is None:
        return _write_failure(
            root=root, plan=plan, receipt=receipt, use_path=use_path,
            accounting=accounting, source_audit=source_audit, selection=selection,
            state="SEALED_UNPUBLISHED_NO_EXECUTABLE_PILOT_FOLD",
        )
    exclusions = tuple(str(item) for item in pilot_fold["evaluation_sessions"])
    excluded = set(exclusions)
    folds_by_market = {
        market: base._outer_folds(tuple(session for session in eligible[market] if session not in excluded))
        for market in base.CORE
    }
    pilot_manifest = base._manifest({
        "schema_version": SESSION_MANIFEST_SCHEMA,
        "contract_id": plan["contract_id"], "mechanism_sha256": MECHANISM_SHA256,
        "stage": "pilot", "markets": ["ES"], "fold_ordinal": 0,
        "calendar_start_offset": pilot_fold["calendar_start_offset"],
        "selection_rule": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
        "selection_evidence_path": (OUTPUT_ROOT / "pilot_fold_selection.json").as_posix(),
        "selection_evidence_sha256": base._file_sha(selection),
        "training_session_ids": pilot_fold["training_sessions"],
        "evaluation_session_ids": pilot_fold["evaluation_sessions"],
        "purge_applied": True, "embargo_applied": True,
    })
    tier1_manifest = base._manifest({
        "schema_version": SESSION_MANIFEST_SCHEMA,
        "contract_id": plan["contract_id"], "mechanism_sha256": MECHANISM_SHA256,
        "stage": "tier_1", "excluded_pilot_evaluation_session_ids": list(exclusions),
        "evaluation_session_ids_by_market": {
            market: sorted({str(session) for fold in folds_by_market[market] for session in fold["evaluation_sessions"]})
            for market in base.CORE
        },
    })
    common_bindings = {
        **plan["bindings"], PLAN_PATH.as_posix(): sha256_file(root / PLAN_PATH),
        (OUTPUT_ROOT / "checkpoint_accounting.json").as_posix(): base._file_sha(accounting),
        (OUTPUT_ROOT / "source_audit.json").as_posix(): base._file_sha(source_audit),
        (OUTPUT_ROOT / "pilot_fold_selection.json").as_posix(): base._file_sha(selection),
    }
    pilot_cert = build_fold_readiness_certificate(
        trial_family=TRIAL_FAMILY, protocol_id=MECHANISM_ID,
        source_bindings={**common_bindings, (OUTPUT_ROOT / "pilot_session_manifest.json").as_posix(): base._file_sha(pilot_manifest)},
        fold_evidence=(pilot_evidence,), required_markets=("ES",),
        required_baselines=base.MANDATORY_BASELINES, required_cost_scenarios=base.SCENARIOS,
        required_outer_fold_ids=("fold-0",), required_nested_fold_ids=(),
        expected_outer_folds=1, expected_nested_folds=0,
        minimum_training_sessions=base.TRAINING_SESSIONS,
        minimum_evaluation_sessions=base.EVALUATION_SESSIONS,
        minimum_purge_minutes=base.PURGE_MINUTES,
        minimum_embargo_sessions=base.EMBARGO_SESSIONS,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )
    tier1_evidence = []
    for market in base.CORE:
        prices = observed[market][0]
        rows = {**prices, "__cost_ticks__": market_costs[market]}
        cache = base._session_cache(
            sessions=eligible[market], bars_by_session=prices,
            cost_ticks=market_costs[market],
        )
        tier1_evidence.extend(
            base._fold_evidence(
                market=market, fold=fold, rows_by_session=rows,
                risk_by_session={}, cache=cache,
            ) for fold in folds_by_market[market]
        )
    tier1_cert = build_fold_readiness_certificate(
        trial_family=TRIAL_FAMILY, protocol_id=MECHANISM_ID,
        source_bindings={**common_bindings, (OUTPUT_ROOT / "tier1_session_manifest.json").as_posix(): base._file_sha(tier1_manifest)},
        fold_evidence=tier1_evidence, required_markets=base.CORE,
        required_baselines=base.MANDATORY_BASELINES, required_cost_scenarios=base.SCENARIOS,
        required_outer_fold_ids=tuple(f"fold-{index}" for index in range(base.OUTER_FOLDS)),
        required_nested_fold_ids=(), expected_outer_folds=base.OUTER_FOLDS,
        expected_nested_folds=0, minimum_training_sessions=base.TRAINING_SESSIONS,
        minimum_evaluation_sessions=base.EVALUATION_SESSIONS,
        minimum_purge_minutes=base.PURGE_MINUTES,
        minimum_embargo_sessions=base.EMBARGO_SESSIONS,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )
    validate_session_manifest(
        pilot_manifest, contract_id=str(plan["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256, stage="pilot", markets=("ES",),
    )
    validate_session_manifest(
        tier1_manifest, contract_id=str(plan["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256, stage="tier_1", markets=base.CORE,
        pilot_evaluation_sha256=sha256_json(list(exclusions)),
    )
    if monotonic() - started > int(plan["execution_limits"]["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("readiness census exceeded total runtime")
    report_core = {
        "schema_version": "alpha_ladder_full_regular_readiness_report/1.0.0",
        "state": "SEALED_UNPUBLISHED_ROW_CERTIFIED_READINESS",
        "plan_id": plan["plan_id"], "mechanism_id": MECHANISM_ID,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "checkpoint_accounting_id": accounting["accounting_id"],
        "source_audit_id": source_audit["audit_id"],
        "pilot_selection_id": selection["selection_id"],
        "pilot_decision": pilot_cert["overall_decision"],
        "tier1_decision": tier1_cert["overall_decision"],
        "combined_registration_ready": pilot_cert["overall_decision"] == tier1_cert["overall_decision"] == "PASS",
        "pilot_certificate_id": pilot_cert["certificate_id"],
        "tier1_certificate_id": tier1_cert["certificate_id"],
        "authority": plan["authority"],
    }
    report = {**report_core, "report_id": sha256_json(report_core)}
    output = root / OUTPUT_ROOT
    output.mkdir(parents=True, exist_ok=False)
    outputs = {
        "checkpoint_accounting.json": accounting,
        "source_audit.json": source_audit,
        "pilot_fold_selection.json": selection,
        "pilot_session_manifest.json": pilot_manifest,
        "tier1_session_manifest.json": tier1_manifest,
        "pilot_readiness_certificate.json": pilot_cert,
        "tier1_readiness_certificate.json": tier1_cert,
        "readiness_report.json": report,
    }
    validate_fold_readiness_certificate(pilot_cert, root=root)
    validate_fold_readiness_certificate(tier1_cert, root=root)
    for name, payload in outputs.items():
        base._write_once(output / name, payload)
    return report
