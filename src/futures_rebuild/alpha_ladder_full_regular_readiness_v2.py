"""Transition-safe successor for the full-regular Alpha readiness census.

The consumed V1 plan, implementation, receipt, and failure record remain
immutable.  V2 changes only evidence lifecycle mechanics: prerequisites are
durably written and read back before certificate validation, terminal outputs
are written last, and every exception after authorization consumption is
sealed in a separate create-only failure root.
"""

from __future__ import annotations

import json
import multiprocessing
import os
from collections.abc import Callable, Mapping
from pathlib import Path
from time import monotonic
from typing import NoReturn

from . import alpha_ladder_full_regular_readiness as v1
from .alpha_research_ladder import (
    SESSION_MANIFEST_SCHEMA,
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


PLAN_PATH = Path("configs/alpha_ladder_full_regular_readiness_census_plan_v2.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_ladder_full_regular_readiness_v2")
FAILURE_ROOT = Path(
    "state/unpublished_evidence/alpha_ladder_full_regular_readiness_v2_failures"
)
MODULE_PATH = Path("src/futures_rebuild/alpha_ladder_full_regular_readiness_v2.py")
PREPARE_SCRIPT_PATH = Path(
    "scripts/prepare_alpha_ladder_full_regular_readiness_census_plan_v2.py"
)
RUNNER_PATH = Path(
    "scripts/run_alpha_ladder_full_regular_readiness_census_v2.py"
)
TEST_PATH = Path("tests/test_alpha_ladder_full_regular_readiness_v2.py")

PREDECESSOR_PLAN_PATH = v1.PLAN_PATH
PREDECESSOR_PLAN_ID = "b5f3742575aa4b7af4dbf10045c1691243505e918dea52af7cc00fba51be3aca"
PREDECESSOR_PLAN_SHA256 = "5dfb2245010a8eac54f6d4faad07002f24813f373d37b8c759545115d26593d3"
PREDECESSOR_RECEIPT_ID = "ad255d9ba8f2c7de393f1a43ea985633bb374dc8b54fc7f351e22fb01607726e"
PREDECESSOR_RECEIPT_PATH = Path(
    f"state/authorization_uses/{PREDECESSOR_RECEIPT_ID}.json"
)
PREDECESSOR_RECEIPT_SHA256 = "5477383a2e854a9d3dbccea0717238a910bd7365391fac653c477d80710b59b4"
PREDECESSOR_FAILURE_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_full_regular_readiness/execution_failure.json"
)
PREDECESSOR_FAILURE_ID = "2bc7e94dca23273f5cc09569b782a324fda65c413bb2117036a8549be2e3396f"
PREDECESSOR_FAILURE_SHA256 = "45026f74af17e564bb34cf3de8ec641f272237cf4c3c9f817722afb84b6b1b0b"

PREREQUISITE_NAMES = (
    "checkpoint_accounting.json",
    "source_audit.json",
    "pilot_fold_selection.json",
    "pilot_session_manifest.json",
    "tier1_session_manifest.json",
)
CERTIFICATE_NAMES = (
    "pilot_readiness_certificate.json",
    "tier1_readiness_certificate.json",
)
TERMINAL_REPORT_NAME = "readiness_report.json"


def _read_canonical(path: Path, *, name: str) -> dict[str, object]:
    return v1.base._read_canonical(path, name=name)


def _verify_predecessor(*, root: Path) -> None:
    plan = _read_canonical(root / PREDECESSOR_PLAN_PATH, name="consumed V1 plan")
    failure = _read_canonical(
        root / PREDECESSOR_FAILURE_PATH, name="consumed V1 failure record"
    )
    receipt = _read_canonical(
        root / PREDECESSOR_RECEIPT_PATH, name="consumed V1 authorization use"
    )
    if (
        plan.get("plan_id") != PREDECESSOR_PLAN_ID
        or sha256_file(root / PREDECESSOR_PLAN_PATH) != PREDECESSOR_PLAN_SHA256
        or failure.get("failure_id") != PREDECESSOR_FAILURE_ID
        or sha256_file(root / PREDECESSOR_FAILURE_PATH) != PREDECESSOR_FAILURE_SHA256
        or receipt.get("receipt_id") != PREDECESSOR_RECEIPT_ID
        or sha256_file(root / PREDECESSOR_RECEIPT_PATH) != PREDECESSOR_RECEIPT_SHA256
    ):
        raise IntegrityError("consumed V1 attempt or failure evidence changed")


def _plan_core(*, root: Path) -> dict[str, object]:
    _verify_predecessor(root=root)
    predecessor = v1.build_plan(root=root)
    if predecessor.get("plan_id") != PREDECESSOR_PLAN_ID:
        raise IntegrityError("live inputs no longer reproduce the consumed V1 plan")
    core = {key: value for key, value in predecessor.items() if key != "plan_id"}
    bindings = dict(core["bindings"])
    successor_dependencies = (
        MODULE_PATH,
        PREPARE_SCRIPT_PATH,
        RUNNER_PATH,
        TEST_PATH,
        PREDECESSOR_PLAN_PATH,
        PREDECESSOR_RECEIPT_PATH,
        PREDECESSOR_FAILURE_PATH,
    )
    bindings.update(
        {
            path.as_posix(): sha256_file(root / path)
            for path in successor_dependencies
        }
    )
    core.update(
        {
            "schema_version": "alpha_ladder_full_regular_readiness_plan/2.0.0",
            "state": "PREPARED_SUCCESSOR_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES",
            "output_root": OUTPUT_ROOT.as_posix(),
            "terminal_failure_root": FAILURE_ROOT.as_posix(),
            "predecessor": {
                "plan_id": PREDECESSOR_PLAN_ID,
                "plan_sha256": PREDECESSOR_PLAN_SHA256,
                "consumed_receipt_id": PREDECESSOR_RECEIPT_ID,
                "consumed_receipt_sha256": PREDECESSOR_RECEIPT_SHA256,
                "failure_id": PREDECESSOR_FAILURE_ID,
                "failure_sha256": PREDECESSOR_FAILURE_SHA256,
                "classification": "PRE_REGISTRATION_IMPLEMENTATION_INVALID",
                "reusable": False,
            },
            "required_outputs": {
                "pass_or_gate_rejection": [
                    *PREREQUISITE_NAMES,
                    *CERTIFICATE_NAMES,
                    TERMINAL_REPORT_NAME,
                ],
                "no_executable_pilot_rejection": [
                    "checkpoint_accounting.json",
                    "source_audit.json",
                    "pilot_fold_selection.json",
                    TERMINAL_REPORT_NAME,
                ],
                "post_consumption_exception": ["execution_failure.json"],
            },
            "write_protocol": {
                "prerequisites_written_and_read_back_before_certificate_validation": True,
                "certificates_written_after_validation": True,
                "terminal_report_written_last": True,
                "post_consumption_exception_sealed_create_only": True,
                "partial_outputs_never_claim_readiness": True,
            },
            "bindings": dict(sorted(bindings.items())),
        }
    )
    return core


def build_plan(*, root: Path) -> dict[str, object]:
    core = _plan_core(root=root)
    return {**core, "plan_id": sha256_json(core)}


def validate_plan(
    plan: Mapping[str, object], *, root: Path, verify_protected: bool = False,
) -> dict[str, object]:
    expected = build_plan(root=root)
    if dict(plan) != expected:
        raise IntegrityError("full-regular readiness V2 plan drifted")
    if verify_protected:
        protected = set(str(item) for item in plan["protected_source_paths"])
        bindings = plan["bindings"]
        assert isinstance(bindings, Mapping)
        for relative in protected:
            if sha256_file(root / relative) != bindings[relative]:
                raise IntegrityError(f"protected source changed: {relative}")
    return dict(plan)


def load_plan(
    *, root: Path, verify_protected: bool = False,
) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="full-regular readiness V2 plan")
    return validate_plan(plan, root=root, verify_protected=verify_protected)


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    scope = v1.required_scope(root=root, plan=plan)
    scope.update(
        {
            "output_root": OUTPUT_ROOT.as_posix(),
            "approval_plan_id": str(plan["plan_id"]),
            "approval_plan_sha256": sha256_file(root / PLAN_PATH),
        }
    )
    return scope


def _write_and_verify(path: Path, payload: Mapping[str, object]) -> str:
    expected = canonical_bytes(payload) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(expected)
        stream.flush()
        os.fsync(stream.fileno())
    observed = path.read_bytes()
    if observed != expected:
        raise IntegrityError(f"durable evidence readback changed: {path.name}")
    return sha256_file(path)


def _finalize_success(
    *,
    root: Path,
    prerequisites: Mapping[str, Mapping[str, object]],
    certificates: Mapping[str, Mapping[str, object]],
    report: Mapping[str, object],
    written: list[str],
    certificate_validator: Callable[..., object] = validate_fold_readiness_certificate,
) -> None:
    if tuple(prerequisites) != PREREQUISITE_NAMES or tuple(certificates) != CERTIFICATE_NAMES:
        raise IntegrityError("readiness V2 finalization output topology changed")
    output = root / OUTPUT_ROOT
    output.mkdir(parents=True, exist_ok=False)
    for name, payload in prerequisites.items():
        _write_and_verify(output / name, payload)
        written.append(name)
    for payload in certificates.values():
        certificate_validator(payload, root=root)
    for name, payload in certificates.items():
        _write_and_verify(output / name, payload)
        written.append(name)
    _write_and_verify(output / TERMINAL_REPORT_NAME, report)
    written.append(TERMINAL_REPORT_NAME)


def _finalize_no_pilot_rejection(
    *, root: Path, outputs: Mapping[str, Mapping[str, object]], written: list[str],
) -> None:
    expected = (
        "checkpoint_accounting.json",
        "source_audit.json",
        "pilot_fold_selection.json",
        TERMINAL_REPORT_NAME,
    )
    if tuple(outputs) != expected:
        raise IntegrityError("no-pilot rejection output topology changed")
    output = root / OUTPUT_ROOT
    output.mkdir(parents=True, exist_ok=False)
    for name, payload in outputs.items():
        _write_and_verify(output / name, payload)
        written.append(name)


def _seal_post_consumption_failure(
    *,
    root: Path,
    plan: Mapping[str, object],
    receipt: OperationReceipt,
    use_path: Path,
    stage: str,
    written: tuple[str, ...],
    exc: BaseException,
) -> Path:
    core = {
        "schema_version": "alpha_ladder_full_regular_readiness_execution_failure/2.0.0",
        "state": "SEALED_UNPUBLISHED_POST_CONSUMPTION_FAILURE",
        "classification": "PRE_REGISTRATION_IMPLEMENTATION_OR_HOST_FAILURE",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "mechanism_id": v1.MECHANISM_ID,
        "mechanism_sha256": v1.MECHANISM_SHA256,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "failure_stage": stage,
        "exception_type": type(exc).__name__,
        "written_outputs": list(written),
        "readiness_decision_produced": False,
        "retry_authorized": False,
        "retry_count": 0,
        "economic_returns_computed": False,
        "model_fit": False,
        "predictions_generated": False,
        "year_2025_accessed": False,
        "provider_network_credentials_accessed": False,
        "publication_performed": False,
        "active_data_mutated": False,
    }
    payload = {**core, "failure_id": sha256_json(core)}
    path = root / FAILURE_ROOT / receipt.receipt_id / "execution_failure.json"
    _write_and_verify(path, payload)
    return path


def _run_after_consumption(
    *,
    root: Path,
    plan: Mapping[str, object],
    receipt: OperationReceipt,
    use_path: Path,
    operation: Callable[[dict[str, str], list[str]], Mapping[str, object]],
) -> Mapping[str, object]:
    stage = {"value": "POST_CONSUMPTION_INITIALIZATION"}
    written: list[str] = []
    try:
        return operation(stage, written)
    except BaseException as exc:
        _seal_post_consumption_failure(
            root=root,
            plan=plan,
            receipt=receipt,
            use_path=use_path,
            stage=stage["value"],
            written=tuple(written),
            exc=exc,
        )
        raise


def _execute_after_consumption(
    *,
    root: Path,
    plan: Mapping[str, object],
    receipt: OperationReceipt,
    use_path: Path,
    started: float,
    stage: dict[str, str],
    written: list[str],
) -> Mapping[str, object]:
    stage["value"] = "VERIFY_PROTECTED_BINDINGS"
    plan = load_plan(root=root, verify_protected=True)
    selected, by_key = v1._selected_sources(root)
    mechanism = _read_canonical(root / v1.MECHANISM_PATH, name="full-regular mechanism")
    costs = mechanism["costs"]["round_trip_adverse_ticks"]
    tasks = []
    market_costs: dict[str, dict[str, int]] = {}
    for market in v1.base.CORE:
        sources = []
        for year in v1.base.YEARS:
            item = by_key[(market, year)]
            assert isinstance(item, Mapping)
            path = v1.base.resolve(
                repository_root=root, market=market, year=year, purpose="SELECTION"
            )
            relative = path.relative_to(root).as_posix()
            if selected.get(relative) != sha256_file(path):
                raise IntegrityError(f"active source changed for {market} {year}")
            sources.append((year, str(path)))
        market_costs[market] = {
            scenario: int(costs[scenario][market])
            for scenario in v1.base.SCENARIOS
        }
        tasks.append((market, tuple(sources), market_costs[market]))
    stage["value"] = "READ_BOUND_MARKET_ROWS"
    pool = multiprocessing.get_context("spawn").Pool(processes=4)
    try:
        worker_results = pool.map_async(v1.base._read_market, tasks, chunksize=1).get(
            timeout=int(plan["execution_limits"]["worker_deadline_seconds"])
        )
        pool.close()
        pool.join()
    except BaseException:
        pool.terminate()
        pool.join()
        raise
    observed = {item[0]: item[1:] for item in worker_results}
    stage["value"] = "BUILD_PRICE_FREE_READINESS_EVIDENCE"
    _pointer, calendar = v1.base._active_calendar(root)
    eligible, raw_accounting = v1._eligible_and_accounting(calendar)
    accounting_core = {
        "schema_version": "alpha_ladder_full_regular_checkpoint_accounting/2.0.0",
        "plan_id": plan["plan_id"],
        "mechanism_id": v1.MECHANISM_ID,
        **raw_accounting,
    }
    accounting = {**accounting_core, "accounting_id": sha256_json(accounting_core)}
    audit_core = {
        "schema_version": "alpha_ladder_full_regular_source_audit/2.0.0",
        "plan_id": plan["plan_id"],
        "mechanism_id": v1.MECHANISM_ID,
        "price_free": True,
        "source_bindings": selected,
        "worker_audits": {
            market: observed[market][2] for market in v1.base.CORE
        },
        "eligible_session_counts": {
            market: len(eligible[market]) for market in v1.base.CORE
        },
    }
    source_audit = {**audit_core, "audit_id": sha256_json(audit_core)}
    es_prices = observed["ES"][0]
    es_cost_rows = {**es_prices, "__cost_ticks__": market_costs["ES"]}
    es_cache = v1.base._session_cache(
        sessions=eligible["ES"],
        bars_by_session=es_prices,
        cost_ticks=market_costs["ES"],
    )

    def evidence_builder(**kwargs):
        return v1.base._fold_evidence(**kwargs, cache=es_cache)

    pilot_fold, pilot_evidence, selection_raw = v1.base.select_earliest_executable_pilot(
        sessions=eligible["ES"],
        rows_by_session=es_cost_rows,
        risk_by_session={},
        evidence_builder=evidence_builder,
    )
    v1.base.validate_selection(selection_raw, selected_fold=pilot_fold)
    selection_core = {
        "schema_version": "alpha_ladder_full_regular_pilot_selection/2.0.0",
        "plan_id": plan["plan_id"],
        "mechanism_id": v1.MECHANISM_ID,
        **selection_raw,
    }
    selection = {**selection_core, "selection_id": sha256_json(selection_core)}
    if pilot_fold is None or pilot_evidence is None:
        report_core = {
            "schema_version": "alpha_ladder_full_regular_readiness_report/2.0.0",
            "state": "SEALED_UNPUBLISHED_NO_EXECUTABLE_PILOT_FOLD",
            "classification": "PRE_REGISTRATION_SOURCE_INCOMPATIBLE",
            "plan_id": plan["plan_id"],
            "mechanism_id": v1.MECHANISM_ID,
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
        stage["value"] = "WRITE_NO_PILOT_REJECTION"
        _finalize_no_pilot_rejection(
            root=root,
            outputs={
                "checkpoint_accounting.json": accounting,
                "source_audit.json": source_audit,
                "pilot_fold_selection.json": selection,
                TERMINAL_REPORT_NAME: report,
            },
            written=written,
        )
        return report
    exclusions = tuple(str(item) for item in pilot_fold["evaluation_sessions"])
    excluded = set(exclusions)
    folds_by_market = {
        market: v1.base._outer_folds(
            tuple(session for session in eligible[market] if session not in excluded)
        )
        for market in v1.base.CORE
    }
    pilot_manifest = v1.base._manifest(
        {
            "schema_version": SESSION_MANIFEST_SCHEMA,
            "contract_id": plan["contract_id"],
            "mechanism_sha256": v1.MECHANISM_SHA256,
            "stage": "pilot",
            "markets": ["ES"],
            "fold_ordinal": 0,
            "calendar_start_offset": pilot_fold["calendar_start_offset"],
            "selection_rule": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
            "selection_evidence_path": (
                OUTPUT_ROOT / "pilot_fold_selection.json"
            ).as_posix(),
            "selection_evidence_sha256": v1.base._file_sha(selection),
            "training_session_ids": pilot_fold["training_sessions"],
            "evaluation_session_ids": pilot_fold["evaluation_sessions"],
            "purge_applied": True,
            "embargo_applied": True,
        }
    )
    tier1_manifest = v1.base._manifest(
        {
            "schema_version": SESSION_MANIFEST_SCHEMA,
            "contract_id": plan["contract_id"],
            "mechanism_sha256": v1.MECHANISM_SHA256,
            "stage": "tier_1",
            "excluded_pilot_evaluation_session_ids": list(exclusions),
            "evaluation_session_ids_by_market": {
                market: sorted(
                    {
                        str(session)
                        for fold in folds_by_market[market]
                        for session in fold["evaluation_sessions"]
                    }
                )
                for market in v1.base.CORE
            },
        }
    )
    common_bindings = {
        **plan["bindings"],
        PLAN_PATH.as_posix(): sha256_file(root / PLAN_PATH),
        (OUTPUT_ROOT / "checkpoint_accounting.json").as_posix(): v1.base._file_sha(accounting),
        (OUTPUT_ROOT / "source_audit.json").as_posix(): v1.base._file_sha(source_audit),
        (OUTPUT_ROOT / "pilot_fold_selection.json").as_posix(): v1.base._file_sha(selection),
    }
    pilot_cert = build_fold_readiness_certificate(
        trial_family=v1.TRIAL_FAMILY,
        protocol_id=v1.MECHANISM_ID,
        source_bindings={
            **common_bindings,
            (OUTPUT_ROOT / "pilot_session_manifest.json").as_posix(): v1.base._file_sha(pilot_manifest),
        },
        fold_evidence=(pilot_evidence,),
        required_markets=("ES",),
        required_baselines=v1.base.MANDATORY_BASELINES,
        required_cost_scenarios=v1.base.SCENARIOS,
        required_outer_fold_ids=("fold-0",),
        required_nested_fold_ids=(),
        expected_outer_folds=1,
        expected_nested_folds=0,
        minimum_training_sessions=v1.base.TRAINING_SESSIONS,
        minimum_evaluation_sessions=v1.base.EVALUATION_SESSIONS,
        minimum_purge_minutes=v1.base.PURGE_MINUTES,
        minimum_embargo_sessions=v1.base.EMBARGO_SESSIONS,
        evidence_class=ROW_CERTIFIED,
        historical_rows_opened=True,
    )
    tier1_evidence = []
    for market in v1.base.CORE:
        prices = observed[market][0]
        rows = {**prices, "__cost_ticks__": market_costs[market]}
        cache = v1.base._session_cache(
            sessions=eligible[market],
            bars_by_session=prices,
            cost_ticks=market_costs[market],
        )
        tier1_evidence.extend(
            v1.base._fold_evidence(
                market=market,
                fold=fold,
                rows_by_session=rows,
                risk_by_session={},
                cache=cache,
            )
            for fold in folds_by_market[market]
        )
    tier1_cert = build_fold_readiness_certificate(
        trial_family=v1.TRIAL_FAMILY,
        protocol_id=v1.MECHANISM_ID,
        source_bindings={
            **common_bindings,
            (OUTPUT_ROOT / "tier1_session_manifest.json").as_posix(): v1.base._file_sha(tier1_manifest),
        },
        fold_evidence=tier1_evidence,
        required_markets=v1.base.CORE,
        required_baselines=v1.base.MANDATORY_BASELINES,
        required_cost_scenarios=v1.base.SCENARIOS,
        required_outer_fold_ids=tuple(
            f"fold-{index}" for index in range(v1.base.OUTER_FOLDS)
        ),
        required_nested_fold_ids=(),
        expected_outer_folds=v1.base.OUTER_FOLDS,
        expected_nested_folds=0,
        minimum_training_sessions=v1.base.TRAINING_SESSIONS,
        minimum_evaluation_sessions=v1.base.EVALUATION_SESSIONS,
        minimum_purge_minutes=v1.base.PURGE_MINUTES,
        minimum_embargo_sessions=v1.base.EMBARGO_SESSIONS,
        evidence_class=ROW_CERTIFIED,
        historical_rows_opened=True,
    )
    validate_session_manifest(
        pilot_manifest,
        contract_id=str(plan["contract_id"]),
        mechanism_sha256=v1.MECHANISM_SHA256,
        stage="pilot",
        markets=("ES",),
    )
    validate_session_manifest(
        tier1_manifest,
        contract_id=str(plan["contract_id"]),
        mechanism_sha256=v1.MECHANISM_SHA256,
        stage="tier_1",
        markets=v1.base.CORE,
        pilot_evaluation_sha256=sha256_json(list(exclusions)),
    )
    if monotonic() - started > int(plan["execution_limits"]["maximum_runtime_seconds"]):
        raise UnauthorizedOperation("readiness census exceeded total runtime")
    report_core = {
        "schema_version": "alpha_ladder_full_regular_readiness_report/2.0.0",
        "state": "SEALED_UNPUBLISHED_ROW_CERTIFIED_READINESS",
        "plan_id": plan["plan_id"],
        "mechanism_id": v1.MECHANISM_ID,
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "checkpoint_accounting_id": accounting["accounting_id"],
        "source_audit_id": source_audit["audit_id"],
        "pilot_selection_id": selection["selection_id"],
        "pilot_decision": pilot_cert["overall_decision"],
        "tier1_decision": tier1_cert["overall_decision"],
        "combined_registration_ready": (
            pilot_cert["overall_decision"]
            == tier1_cert["overall_decision"]
            == "PASS"
        ),
        "pilot_certificate_id": pilot_cert["certificate_id"],
        "tier1_certificate_id": tier1_cert["certificate_id"],
        "authority": plan["authority"],
    }
    report = {**report_core, "report_id": sha256_json(report_core)}
    stage["value"] = "WRITE_PREREQUISITES_VALIDATE_CERTIFICATES_AND_SEAL_REPORT"
    _finalize_success(
        root=root,
        prerequisites={
            "checkpoint_accounting.json": accounting,
            "source_audit.json": source_audit,
            "pilot_fold_selection.json": selection,
            "pilot_session_manifest.json": pilot_manifest,
            "tier1_session_manifest.json": tier1_manifest,
        },
        certificates={
            "pilot_readiness_certificate.json": pilot_cert,
            "tier1_readiness_certificate.json": tier1_cert,
        },
        report=report,
        written=written,
    )
    return report


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> Mapping[str, object]:
    """Consume one V2 claim, then seal one readiness result or failure."""

    started = monotonic()
    plan = load_plan(root=root, verify_protected=False)
    if os.name != "nt" or multiprocessing.current_process().name != "MainProcess":
        raise UnauthorizedOperation("readiness census V2 requires the Windows main process")
    if (root / OUTPUT_ROOT).exists() or (root / FAILURE_ROOT).exists():
        raise UnauthorizedOperation("readiness census V2 attempt output already exists")
    use_path = receipt.consume(
        boundary,
        operation=ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )

    def operation(stage: dict[str, str], written: list[str]) -> Mapping[str, object]:
        return _execute_after_consumption(
            root=root,
            plan=plan,
            receipt=receipt,
            use_path=use_path,
            started=started,
            stage=stage,
            written=written,
        )

    return _run_after_consumption(
        root=root,
        plan=plan,
        receipt=receipt,
        use_path=use_path,
        operation=operation,
    )
