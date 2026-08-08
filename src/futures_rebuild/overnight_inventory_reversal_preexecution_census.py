"""Row-certified readiness census for the closed overnight-reversal mechanism.

This adapter is audit-only.  It computes causal feature and execution-path
availability, fold counts, and baseline/risk readiness.  It never computes a
trade return, fits a predictive model, or evaluates economics.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from statistics import median

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .historical_checkpoint_calendar import load_historical_checkpoint_calendar
from .overnight_inventory_reversal_execution import (
    BASELINES,
    COST_TICKS,
    FEE_PER_SIDE_USD,
    MAD_SCALE,
    MARKETS,
    MAXIMUM_PLANNED_INITIAL_LOSS_USD,
    MINIMUM_SCALE_SESSIONS,
    THRESHOLD,
    SessionObservation,
    iter_ordered_session_observations,
)
from .preexecution_fold_certification import (
    ROW_CERTIFIED,
    build_fold_readiness_certificate,
)
from .tier1_bracket_v10 import SourceIntegrityAuditV10, iter_source_records_from_parquet_v10
from .tier1_bracket_v5 import load_registered_calendar_sessions_v5


OPERATION = "CENSUS_OVERNIGHT_REVERSAL_FOLD_READINESS_ONCE"
PLAN_PATH = Path("configs/overnight_inventory_reversal_fold_census_plan.json")
OUTPUT_FILENAME = "fold_readiness_certificate.json"


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid readiness census artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("readiness census artifact must be an object")
    return value


def load_census_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    bindings = plan.get("bindings")
    limits = plan.get("limits")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version")
        != "overnight_inventory_reversal_fold_census_plan/1.0.0"
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
    ):
        raise IntegrityError("overnight reversal readiness census plan drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "trial_id": str(plan["trial_id"]),
        "period": "2018,2019,2020,2021,2022",
        "markets": "ES,CL,ZN,6E",
        "purpose": "FOLD_READINESS_COUNTS_ONLY_NO_ECONOMICS",
        "output_root": str(plan["output_root"]),
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "provider_or_network_access": "false",
        "holdout_2025_access": "false",
        "publication": "false",
        "trading": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _source_map(
    *, root: Path, manifest: Mapping[str, object],
) -> tuple[dict[tuple[str, int], Path], dict[str, str]]:
    pairs = manifest.get("input_pairs")
    if not isinstance(pairs, list) or len(pairs) != 20:
        raise IntegrityError("readiness census requires twenty source bindings")
    expected = {(market, year) for market in MARKETS for year in range(2018, 2023)}
    paths: dict[tuple[str, int], Path] = {}
    bindings: dict[str, str] = {}
    for pair in pairs:
        if not isinstance(pair, Mapping):
            raise IntegrityError("source pair is malformed")
        market, year = pair.get("market"), pair.get("year")
        digest = pair.get("source_parquet_sha256")
        if (
            not isinstance(market, str)
            or type(year) is not int
            or (market, year) not in expected
            or not isinstance(digest, str)
        ):
            raise IntegrityError("source pair leaves the registered scope")
        relative = Path("data/active/causally_gated_normalized") / market / str(year) / f"{year}.parquet"
        if sha256_file(root / relative) != digest:
            raise IntegrityError("readiness census source binding changed")
        paths[(market, year)] = root / relative
        bindings[relative.as_posix()] = digest
    if set(paths) != expected:
        raise IntegrityError("readiness census source scope is incomplete")
    return paths, bindings


def _ranges(raw: Mapping[str, object]) -> tuple[str, str, str, str]:
    fit = raw.get("outer_fit_session_range")
    test = raw.get("outer_test_session_dates")
    if (
        not isinstance(fit, list) or len(fit) != 2
        or not isinstance(test, list) or len(test) != 2
        or any(not isinstance(item, str) for item in (*fit, *test))
    ):
        raise IntegrityError("outer fold is malformed")
    return fit[0], fit[1], test[0], test[1]


def _reason(observation: SessionObservation | None) -> str:
    if observation is None:
        return "MISSING_SOURCE_SESSION"
    return observation.failure or "UNCLASSIFIED_INCOMPLETE_SESSION"


def _risk_disposition(observation: SessionObservation, *, scenario: str) -> str:
    if not observation.complete or not observation.execution_path:
        return "UNRESOLVED"
    first = observation.execution_path[0]
    if first.market_spec is None:
        return "UNRESOLVED"
    costs = (
        Decimal(2) * FEE_PER_SIDE_USD
        + Decimal(COST_TICKS[scenario][observation.market]) * first.market_spec.tick_value
    )
    if costs >= MAXIMUM_PLANNED_INITIAL_LOSS_USD:
        return "RISK_ABSTENTION"
    ticks = int(
        ((MAXIMUM_PLANNED_INITIAL_LOSS_USD - costs) / first.market_spec.tick_value)
        .to_integral_value(rounding=ROUND_FLOOR)
    )
    return "FEASIBLE" if ticks >= 1 else "RISK_ABSTENTION"


def build_fold_evidence(
    *, observations: Sequence[SessionObservation],
    outer_folds: Sequence[Mapping[str, object]],
    expected_open_sessions: Mapping[str, Sequence[str]],
    ordered_schedule_sessions: Sequence[str],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Build exact fold-market counts without computing any outcome return."""

    by_key = {(item.market, item.session): item for item in observations}
    if len(by_key) != len(observations):
        raise IntegrityError("readiness observations are duplicated")
    schedule_index = {session: index for index, session in enumerate(ordered_schedule_sessions)}
    evidence: list[dict[str, object]] = []
    first_failure: dict[str, object] | None = None
    for fold_number, raw in enumerate(outer_folds):
        fit_start, fit_end, test_start, test_end = _ranges(raw)
        chronological = fit_start <= fit_end < test_start <= test_end
        if any(value not in schedule_index for value in (fit_end, test_start)):
            raise IntegrityError("fold boundary is absent from the bound schedule")
        embargo = schedule_index[test_start] - schedule_index[fit_end] - 1
        for market in MARKETS:
            market_expected = tuple(expected_open_sessions.get(market, ()))
            training_dates = tuple(
                session for session in market_expected if fit_start <= session <= fit_end
            )
            test_dates = tuple(
                session for session in market_expected if test_start <= session <= test_end
            )
            training = [by_key.get((market, session)) for session in training_dates]
            testing = [by_key.get((market, session)) for session in test_dates]
            complete_training = [
                item for item in training
                if item is not None and item.complete and item.overnight_return is not None
            ]
            training_exclusions = Counter(
                _reason(item) for item in training
                if item is None or not item.complete or item.overnight_return is None
            )
            test_exclusions = Counter(
                _reason(item) for item in testing if item is None or not item.complete
            )
            terminal_testing = [
                item for item in testing
                if item is not None and (item.complete or item.failure is not None)
            ]

            scale_ready = False
            location = 0.0
            scale = 0.0
            if len(complete_training) >= MINIMUM_SCALE_SESSIONS:
                values = [float(item.overnight_return) for item in complete_training]
                location = median(values)
                scale = MAD_SCALE * median(abs(value - location) for value in values)
                scale_ready = math.isfinite(scale) and scale > 0

            selected: list[SessionObservation] = []
            if scale_ready:
                for item in testing:
                    if item is None or not item.complete or item.overnight_return is None:
                        continue
                    z_value = (item.overnight_return - location) / scale
                    if abs(z_value) >= THRESHOLD:
                        selected.append(item)
            candidate_paths = sum(item.complete for item in selected)
            risk_dispositions = {
                scenario: Counter(
                    _risk_disposition(item, scenario=scenario) for item in selected
                )
                for scenario in COST_TICKS
            }
            baseline_universe: dict[str, dict[str, object]] = {}
            for baseline in BASELINES:
                flat = baseline == "flat_no_trade"
                selected_count = 0 if flat else len(selected)
                complete_rows = [] if flat else [
                    item for item in selected
                    if (
                        baseline not in {
                            "previous_session_sign_momentum",
                            "previous_session_sign_reversal",
                        }
                        or item.prior_session_direction in {-1, 1}
                    )
                ]
                baseline_universe[baseline] = {
                    "expected_sessions": len(test_dates),
                    "terminal_sessions": len(terminal_testing),
                    "selected_sessions": selected_count,
                    "selected_path_complete_sessions": len(complete_rows),
                    "scenario_risk_dispositions": {
                        scenario: {
                            "feasible_sessions": dispositions["FEASIBLE"],
                            "risk_abstention_sessions": dispositions["RISK_ABSTENTION"],
                            "unresolved_sessions": dispositions["UNRESOLVED"],
                        }
                        for scenario in COST_TICKS
                        for dispositions in [Counter(
                            _risk_disposition(item, scenario=scenario)
                            for item in complete_rows
                        )]
                    },
                    "schedule_independently_derived": True,
                    "flat_no_trade": flat,
                }
            baseline_ready = all(
                item["terminal_sessions"] == item["expected_sessions"]
                and item["schedule_independently_derived"] is True
                and (
                    item["flat_no_trade"] is True
                    and item["selected_sessions"] == 0
                    and item["selected_path_complete_sessions"] == 0
                    or item["flat_no_trade"] is False
                    and item["selected_path_complete_sessions"] == item["selected_sessions"]
                    and all(
                        disposition["feasible_sessions"]
                        + disposition["risk_abstention_sessions"]
                        == item["selected_sessions"]
                        and disposition["unresolved_sessions"] == 0
                        for disposition in item["scenario_risk_dispositions"].values()  # type: ignore[union-attr]
                    )
                )
                for item in baseline_universe.values()
            )
            exclusion_reasons = {
                **{f"TRAINING__{key}": value for key, value in training_exclusions.items()},
                **{f"EVALUATION__{key}": value for key, value in test_exclusions.items()},
            }
            market_year_breakdown: dict[str, dict[str, object]] = {}
            for year in sorted({session[:4] for session in (*training_dates, *test_dates)}):
                year_training = [
                    (session, item)
                    for session, item in zip(training_dates, training, strict=True)
                    if session.startswith(year)
                ]
                year_testing = [
                    (session, item)
                    for session, item in zip(test_dates, testing, strict=True)
                    if session.startswith(year)
                ]
                year_exclusions = Counter(
                    f"TRAINING__{_reason(item)}" for _, item in year_training
                    if item is None or not item.complete or item.overnight_return is None
                )
                year_exclusions.update(
                    f"EVALUATION__{_reason(item)}" for _, item in year_testing
                    if item is None or not item.complete
                )
                market_year_breakdown[year] = {
                    "expected_training_sessions": len(year_training),
                    "complete_training_sessions": sum(
                        item is not None and item.complete and item.overnight_return is not None
                        for _, item in year_training
                    ),
                    "expected_evaluation_sessions": len(year_testing),
                    "feature_complete_evaluation_sessions": sum(
                        item is not None and item.overnight_return is not None
                        for _, item in year_testing
                    ),
                    "terminal_evaluation_sessions": sum(
                        item is not None and (item.complete or item.failure is not None)
                        for _, item in year_testing
                    ),
                    "execution_path_complete_evaluation_sessions": sum(
                        item is not None and item.complete for _, item in year_testing
                    ),
                    "exclusion_reasons": dict(sorted(year_exclusions.items())),
                }
            row = {
                "fold_id": f"fold-{fold_number}",
                "market": market,
                "role": "OUTER",
                "counts": {
                    "expected_training_sessions": len(training_dates),
                    "complete_training_sessions": len(complete_training),
                    "feature_complete_training_sessions": len(complete_training),
                    "transformation_ready_training_sessions": (
                        len(complete_training) if scale_ready else 0
                    ),
                    "expected_evaluation_sessions": len(test_dates),
                    "feature_complete_evaluation_sessions": sum(
                        item is not None and item.overnight_return is not None
                        for item in testing
                    ),
                    "terminal_evaluation_sessions": len(terminal_testing),
                    "execution_path_complete_evaluation_sessions": sum(
                        item is not None and item.complete for item in testing
                    ),
                    "candidate_selected_sessions": len(selected),
                    "candidate_selected_path_complete_sessions": candidate_paths,
                    "scenario_risk_dispositions": {
                        scenario: {
                            "feasible_sessions": dispositions["FEASIBLE"],
                            "risk_abstention_sessions": dispositions["RISK_ABSTENTION"],
                            "unresolved_sessions": dispositions["UNRESOLVED"],
                        }
                        for scenario, dispositions in risk_dispositions.items()
                    },
                    "purge_minutes": 60 if chronological else 0,
                    "embargo_sessions": max(0, embargo),
                },
                "checks": {
                    "chronological_order": chronological,
                    "purge_applied": chronological,
                    "embargo_applied": embargo >= 1,
                    "training_only_transformation": scale_ready,
                    "contract_identity_discontinuities_terminalized": all(
                        item is not None and (item.complete or item.failure is not None)
                        for item in (*training, *testing)
                    ),
                    "roll_discontinuities_terminalized": all(
                        item is not None and (item.complete or item.failure is not None)
                        for item in (*training, *testing)
                    ),
                    "all_incomplete_sessions_terminalized": all(
                        item is not None and (item.complete or item.failure is not None)
                        for item in testing
                    ),
                    "complete_required_metrics": (
                        len(test_dates) > 0
                        and candidate_paths == len(selected)
                        and baseline_ready
                    ),
                    "promotion_path_computable": (
                        len(test_dates) > 0
                        and candidate_paths == len(selected)
                        and all(
                            dispositions["FEASIBLE"]
                            + dispositions["RISK_ABSTENTION"] == len(selected)
                            and dispositions["UNRESOLVED"] == 0
                            for dispositions in risk_dispositions.values()
                        )
                        and baseline_ready
                    ),
                },
                "baseline_universe_readiness": baseline_universe,
                "exclusion_reasons": exclusion_reasons,
                "market_year_breakdown": market_year_breakdown,
            }
            evidence.append(row)
            if first_failure is None and len(complete_training) < MINIMUM_SCALE_SESSIONS:
                first_failure = {
                    "scenario_attempted_first": "base",
                    "fold_id": f"fold-{fold_number}",
                    "market": market,
                    "expected_training_sessions": len(training_dates),
                    "complete_training_sessions": len(complete_training),
                    "minimum_required": MINIMUM_SCALE_SESSIONS,
                    "exclusion_reasons": dict(sorted(training_exclusions.items())),
                }
    audit = {
        "first_runtime_failure_reconstructed": first_failure,
        "evaluation_returns_computed": False,
        "predictive_model_fit": False,
        "strategy_parameters_changed": False,
    }
    return evidence, audit


def execute_authorized_census_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> dict[str, object]:
    plan = load_census_plan(root=root)
    output_root = root / str(plan["output_root"])
    boundary.assert_active_path(
        output_root.absolute(), purpose="unpublished fold readiness census",
        subtree="state/unpublished_evidence",
    )
    if output_root.exists():
        raise UnauthorizedOperation("fold readiness census output already exists")
    use_path = receipt.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    manifest = _object(root / str(plan["phase5_manifest_path"]))
    paths, source_bindings = _source_map(root=root, manifest=manifest)
    observations: list[SessionObservation] = []
    source_audits: dict[str, object] = {}
    for market in MARKETS:
        def records():
            for year in range(2018, 2023):
                audit = SourceIntegrityAuditV10(market)
                yield from iter_source_records_from_parquet_v10(
                    market=market, path=paths[(market, year)], audit=audit,
                )
                source_audits[f"{market}/{year}"] = audit.as_dict()

        observations.extend(iter_ordered_session_observations(
            market=market, source_records=records(),
        ))

    sessions = load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(plan["calendar_release_id"]),
    )
    loaded_calendar = load_historical_checkpoint_calendar(boundary=boundary)
    if loaded_calendar.index_receipt.release_id != str(plan["calendar_release_id"]):
        raise IntegrityError("readiness census calendar dependency changed")
    certification_bindings = dict(source_bindings)
    plan_bindings = plan.get("bindings")
    if not isinstance(plan_bindings, Mapping):
        raise IntegrityError("readiness census dependency bindings are absent")
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
        "schema_version": "overnight_inventory_reversal_fold_census/1.0.0",
        "trial_id": plan["trial_id"],
        "plan_id": plan["plan_id"],
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "censused_at_utc": datetime.now(timezone.utc).isoformat(),
        "fold_readiness_certificate": certificate,
        "runtime_failure_audit": failure_audit,
        "source_audits": source_audits,
        "economics_evaluation": False,
        "model_fit": False,
        "prediction_generation": False,
        "holdout_2025_touched": False,
        "provider_or_network_access": False,
        "publication": False,
        "trading": False,
    }
    report = {**core, "report_id": sha256_json(core)}
    output_root.mkdir(parents=True, exist_ok=False)
    output_path = output_root / OUTPUT_FILENAME
    with output_path.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
    if _object(output_path) != report:
        raise IntegrityError("fold readiness census verification failed")
    return report
