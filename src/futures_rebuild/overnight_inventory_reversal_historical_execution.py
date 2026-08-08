"""Single-use local 2018-2022 execution for the overnight-reversal trial."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import NormalDist

import numpy as np

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .overnight_inventory_reversal_execution import (
    BASELINES,
    MARKETS,
    TrialEvaluation,
    evaluate_fixed_trial,
    iter_ordered_session_observations,
)
from .research.hac import newey_west_mean
from .research.multiple_testing import romano_wolf_from_differentials
from .tier1_bracket_v10 import (
    SourceIntegrityAuditV10,
    iter_source_records_from_parquet_v10,
)


OPERATION = "RUN_OVERNIGHT_INVENTORY_REVERSAL_2018_2022_ONCE"
PLAN_PATH = Path("configs/overnight_inventory_reversal_historical_execution_plan.json")
OUTPUT_FILENAME = "outer_evaluation.json"
ANCHOR_ROOT = Path("state/trial_events/overnight_inventory_reversal")


@dataclass(frozen=True)
class HistoricalExecutionResult:
    trial_id: str
    authorization_use_path: Path
    anchor_path: Path
    output_path: Path
    report: Mapping[str, object]


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid execution artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("execution artifact must be an object")
    return value


def load_historical_execution_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    limits = plan.get("limits")
    preservation = plan.get("preservation")
    authorization = plan.get("authorization")
    bindings = plan.get("bindings")
    manifest_path = plan.get("phase5_manifest_path")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version")
        != "overnight_inventory_reversal_historical_execution_plan/1.0.0"
        or plan.get("state") != "PREPARED_NOT_EXECUTED"
        or plan.get("operation") != OPERATION
        or plan.get("period") != [2018, 2019, 2020, 2021, 2022]
        or plan.get("markets") != ["ES", "CL", "ZN", "6E"]
        or plan.get("input_market_year_pairs") != 20
        or not isinstance(manifest_path, str)
        or plan.get("phase5_manifest_sha256") != sha256_file(root / manifest_path)
        or not isinstance(limits, Mapping)
        or limits.get("maximum_real_history_attempts") != 1
        or limits.get("maximum_retries") != 0
        or limits.get("maximum_provider_requests") != 0
        or limits.get("maximum_holdout_rows") != 0
        or not isinstance(preservation, Mapping)
        or preservation.get("holdout_2025_untouched") is not True
        or any(preservation.get(name) is not False for name in (
            "provider_or_network_access", "publication", "trading",
            "stage_commit_push", "accepted_source_bytes_mutated",
        ))
        or not isinstance(authorization, Mapping)
        or authorization.get("exact_single_use_user_approval_required") is not True
        or authorization.get("pre_outcome_anchor_required_before_source_open") is not True
        or authorization.get("execution_authorized") is not False
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("overnight reversal execution plan drifted")
    registry = _object(
        root / "state/trial_registry/overnight_inventory_reversal"
        / f"{plan['trial_id']}.json"
    )
    if (
        registry.get("trial_id") != plan.get("trial_id")
        or registry.get("state")
        != "REGISTERED_CORRECTED_BEFORE_SOURCE_OR_OUTCOME_ACCESS"
        or registry.get("source_row_access") is not False
        or registry.get("outcome_row_access") is not False
        or registry.get("economics_evaluation") is not False
        or registry.get("holdout_or_forward_access") is not False
    ):
        raise IntegrityError("corrected registered trial is not outcome locked")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "trial_id": str(plan["trial_id"]),
        "period": "2018,2019,2020,2021,2022",
        "markets": "ES,CL,ZN,6E",
        "output_root": str(plan["output_root"]),
        "maximum_real_history_attempts": "1",
        "maximum_retries": "0",
        "provider_or_network_access": "false",
        "holdout_2025_access": "false",
        "publication": "false",
        "trading": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _resolve_sources_after_authorization(
    *, root: Path, plan: Mapping[str, object],
) -> tuple[dict[tuple[str, int], Path], dict[str, object]]:
    manifest = _object(root / str(plan["phase5_manifest_path"]))
    pairs = manifest.get("input_pairs")
    if not isinstance(pairs, list) or len(pairs) != 20:
        raise IntegrityError("execution requires exactly twenty bound source pairs")
    expected = {(market, year) for market in MARKETS for year in range(2018, 2023)}
    paths: dict[tuple[str, int], Path] = {}
    for pair in pairs:
        if not isinstance(pair, Mapping):
            raise IntegrityError("source binding is malformed")
        market, year = pair.get("market"), pair.get("year")
        if year == 2025:
            raise UnauthorizedOperation("2025 path rejected before construction")
        if not isinstance(market, str) or type(year) is not int or (market, year) not in expected:
            raise IntegrityError("source binding leaves the registered scope")
        relative = (
            Path("data/active/causally_gated_normalized")
            / market / str(year) / f"{year}.parquet"
        )
        sidecar = _object(root / relative.with_suffix(".parquet.manifest.json"))
        entry = sidecar.get("entry_binding")
        expected_hash = pair.get("source_parquet_sha256")
        if (
            not isinstance(entry, Mapping)
            or entry.get("market") != market
            or entry.get("year") != year
            or entry.get("parquet_path") != relative.as_posix()
            or entry.get("parquet_sha256") != expected_hash
            or not isinstance(expected_hash, str)
            or sha256_file(root / relative) != expected_hash
        ):
            raise IntegrityError("source bytes or sidecar differ from the Phase 5 binding")
        paths[(market, year)] = root / relative
    if set(paths) != expected:
        raise IntegrityError("source map is not exactly the twenty registered pairs")
    return paths, manifest


def _decimal_map(values: Mapping[str, Decimal]) -> dict[str, str]:
    return {key: str(value) for key, value in sorted(values.items())}


def _scenario_summary(evaluation: TrialEvaluation) -> dict[str, object]:
    totals = {
        name: sum(
            (values[name] for values in evaluation.baseline_portfolio_net_pnl_by_session.values()),
            Decimal(),
        )
        for name in BASELINES
    }
    candidate_total = sum(evaluation.portfolio_net_pnl_by_session.values(), Decimal())
    return {
        "cost_scenario": evaluation.cost_scenario,
        "complete_portfolio_sessions": len(evaluation.complete_portfolio_sessions),
        "incomplete_market_sessions": evaluation.incomplete_market_sessions,
        "candidate_trade_count": evaluation.candidate_trade_count,
        "candidate_net_pnl_usd": str(candidate_total),
        "candidate_mean_net_pnl_usd_per_complete_session": (
            str(candidate_total / Decimal(len(evaluation.complete_portfolio_sessions)))
            if evaluation.complete_portfolio_sessions else None
        ),
        "baseline_net_pnl_usd": _decimal_map(totals),
    }


def _stress_inference(evaluation: TrialEvaluation) -> dict[str, object]:
    dates = evaluation.complete_portfolio_sessions
    if len(dates) < 30:
        return {"status": "INCONCLUSIVE_CLUSTER_COUNT", "observations": len(dates)}
    candidate = np.asarray(
        [float(evaluation.portfolio_net_pnl_by_session[session]) for session in dates],
        dtype=np.float64,
    )
    hac = newey_west_mean(candidate, lag=5)
    lower = hac.mean - NormalDist().inv_cdf(0.95) * hac.standard_error

    rows_by_market_date = {
        (item.market, item.session): item
        for item in evaluation.sessions if item.complete
    }
    hypotheses: list[str] = []
    columns: list[list[float]] = []
    for scope in ("portfolio", *MARKETS):
        for baseline in BASELINES:
            hypotheses.append(f"{scope}__candidate_minus__{baseline}")
            values: list[float] = []
            for session in dates:
                if scope == "portfolio":
                    candidate_value = evaluation.portfolio_net_pnl_by_session[session]
                    baseline_value = evaluation.baseline_portfolio_net_pnl_by_session[session][baseline]
                else:
                    item = rows_by_market_date[(scope, session)]
                    candidate_value = item.candidate_net_pnl_usd
                    baseline_value = item.baseline_net_pnl_usd[baseline]
                    assert candidate_value is not None and baseline_value is not None
                values.append(float(candidate_value - baseline_value))
            columns.append(values)
    differentials = np.asarray(columns, dtype=np.float64).T
    rw = romano_wolf_from_differentials(
        differentials,
        hypothesis_ids=tuple(hypotheses),
        hac_lag=5,
        mean_block_length=5.0,
        n_resamples=10000,
        seed=20260806,
        tail="greater",
    )
    adjusted = {
        hypothesis: float(rw.adjusted_p_values[index])
        for index, hypothesis in enumerate(hypotheses)
    }
    return {
        "status": "COMPUTED",
        "observations": len(dates),
        "hac_lag_sessions": 5,
        "mean_usd_per_complete_session": hac.mean,
        "hac_standard_error": hac.standard_error,
        "one_sided_95pct_lower_bound_usd": lower,
        "mees_usd": 10.0,
        "romano_wolf_resamples": 10000,
        "romano_wolf_adjusted_p_values": adjusted,
        "global_history_alpha_ceiling": 0.05 / 106.0,
        "deflated_sharpe": {
            "status": "INCONCLUSIVE_MISSING_COMPLETE_BOUND_106_TRIAL_SHARPE_CENSUS",
            "probability_minimum": 0.95,
        },
        "power": {
            "status": "INCONCLUSIVE_MISSING_PREOUTCOME_TRAINING_ONLY_POWER_RECEIPT",
            "minimum": 0.80,
        },
    }


def _terminal_decision(
    *, stress: TrialEvaluation, inference: Mapping[str, object],
) -> tuple[str, str]:
    if stress.incomplete_market_sessions or inference.get("status") != "COMPUTED":
        return "CLOSED", "INCONCLUSIVE_DATA_OR_COVERAGE"
    mean = inference.get("mean_usd_per_complete_session")
    lower = inference.get("one_sided_95pct_lower_bound_usd")
    if not isinstance(mean, float) or not isinstance(lower, float):
        return "CLOSED", "INVALID_INFERENCE"
    if mean <= 0:
        return "CLOSED", "FAIL_NO_EDGE"
    if lower <= 10:
        return "CLOSED", "FAIL_NOT_ECONOMIC_OR_INCONCLUSIVE_EFFECT"
    return "CLOSED", "INCONCLUSIVE_MULTIPLICITY_AND_POWER_EVIDENCE"


def execute_authorized_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> HistoricalExecutionResult:
    plan = load_historical_execution_plan(root=root)
    scope = required_scope(root=root, plan=plan)
    output_root = root / str(plan["output_root"])
    boundary.assert_active_path(
        output_root.absolute(), purpose="overnight reversal unpublished evidence",
        subtree="state/unpublished_evidence",
    )
    if output_root.exists():
        raise UnauthorizedOperation("overnight reversal trial was already executed")
    use_path = receipt.consume(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    anchor_core = {
        "schema_version": "overnight_inventory_reversal_pre_outcome_anchor/1.0.0",
        "event_type": "PRE_OUTCOME_ANCHORED",
        "trial_id": plan["trial_id"],
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "authorization_use_sha256": sha256_file(use_path),
        "anchored_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_row_access_before_anchor": False,
        "outcome_row_access_before_anchor": False,
        "holdout_2025_access": False,
    }
    anchor = {**anchor_core, "event_id": sha256_json(anchor_core)}
    anchor_path = root / ANCHOR_ROOT / (
        f"{plan['trial_id']}_pre_outcome_anchor_{receipt.receipt_id}.json"
    )
    anchor_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with anchor_path.open("xb") as stream:
            stream.write(canonical_bytes(anchor) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("pre-outcome anchor already exists") from exc

    paths, manifest = _resolve_sources_after_authorization(root=root, plan=plan)
    observations = []
    audits: dict[str, object] = {}
    for market in MARKETS:
        def records():
            for year in range(2018, 2023):
                audit = SourceIntegrityAuditV10(market)
                yield from iter_source_records_from_parquet_v10(
                    market=market, path=paths[(market, year)], audit=audit,
                )
                audits[f"{market}/{year}"] = audit.as_dict()

        observations.extend(
            iter_ordered_session_observations(
                market=market, source_records=records(),
            )
        )
    folds = manifest.get("outer_folds")
    if not isinstance(folds, list) or len(folds) != 8:
        raise IntegrityError("Phase 5 outer-fold binding is invalid")
    evaluations = {
        scenario: evaluate_fixed_trial(
            observations=observations, outer_folds=folds,
            cost_scenario=scenario,
        )
        for scenario in ("base", "stress", "extreme")
    }
    stress = evaluations["stress"]
    inference = _stress_inference(stress)
    terminal_state, decision = _terminal_decision(
        stress=stress, inference=inference,
    )
    report_core = {
        "schema_version": "overnight_inventory_reversal_outer_evaluation/1.0.0",
        "trial_id": plan["trial_id"],
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": receipt.receipt_id,
        "pre_outcome_anchor_event_id": anchor["event_id"],
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "period": [2018, 2019, 2020, 2021, 2022],
        "markets": list(MARKETS),
        "phase5_plan_id": manifest["plan_id"],
        "scenario_summaries": {
            name: _scenario_summary(value)
            for name, value in evaluations.items()
        },
        "stress_inference": inference,
        "terminal_state": terminal_state,
        "decision": decision,
        "mechanism_closed_no_incremental_rescue": True,
        "source_audits": audits,
        "source_rows_opened": True,
        "outcome_rows_derived": True,
        "holdout_2025_touched": False,
        "provider_or_network_access": False,
        "publication": False,
        "trading": False,
    }
    report = {**report_core, "report_id": sha256_json(report_core)}
    output_root.mkdir(parents=True, exist_ok=False)
    output_path = output_root / OUTPUT_FILENAME
    with output_path.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
    if _object(output_path) != report:
        raise IntegrityError("durable overnight reversal report verification failed")
    return HistoricalExecutionResult(
        str(plan["trial_id"]), use_path, anchor_path, output_path, report,
    )
