"""Internal Phase 8 evaluation/report seam; there is intentionally no CLI."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from .canonical import canonical_bytes, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .tier1_phase8_evaluator import Tier1Phase8Evaluation, evaluate_tier1_phase8_synthetic
from .tier1_phase8_real_adapter import (
    PinnedPhase8Inputs,
    ScheduledExecutionRows,
    _ApprovedCodexRealRead,
    _READ_SCOPE_SECRET,
    _read_all_pinned_phase8_rows_after_approval,
    _read_pinned_rows_after_approval,
    convert_pinned_source_bars_to_execution_rows,
    derive_fold_local_directions,
    derive_training_outcome_volatilities,
    normalize_phase8_execution_rows,
    schedule_one_contract_execution_rows,
)


@dataclass(frozen=True)
class Phase8EvaluationReports:
    """Canonical, non-published report payloads for one verified evaluation."""

    run_id: str
    model_selection: Mapping[str, object]
    risk: Mapping[str, object]


def _metrics(value: object) -> dict[str, object]:
    if not hasattr(value, "net_pnl_usd"):
        raise IntegrityError("Phase 8 report metrics are invalid")
    return {
        "net_pnl_usd": str(value.net_pnl_usd),
        "sharpe": value.sharpe,
        "sortino": value.sortino,
        "maximum_drawdown_usd": str(value.maximum_drawdown_usd),
        "turnover_contract_equivalents": value.turnover_contract_equivalents,
        "hit_rate": value.hit_rate,
        "gross_exposure_contract_equivalents": value.gross_exposure_contract_equivalents,
        "net_directional_contract_equivalents": value.net_directional_contract_equivalents,
        "observation_count": value.observation_count,
    }


def build_phase8_evaluation_reports(
    *, pinned: PinnedPhase8Inputs, evaluation: Tier1Phase8Evaluation, preparation_id: str,
    excluded_roll_count: int = 0, fold_local_fallback_keys: Sequence[tuple[str, int, int]] = (),
    candidate_count: int = 0, simultaneous_selection_abstentions: int = 0, position_overlap_abstentions: int = 0,
) -> Phase8EvaluationReports:
    """Build report payloads only; this neither reads rows nor writes files."""

    if evaluation.result_label != "PROVISIONAL_EXECUTION_COSTS" or evaluation.exact_apex_live_costs_verified:
        raise IntegrityError("Phase 8 report must retain the provisional-cost label")
    if set(evaluation.scenarios) != {"base", "stress", "extreme"}:
        raise IntegrityError("Phase 8 report requires base, stress, and extreme results")
    if type(excluded_roll_count) is not int or excluded_roll_count < 0:
        raise IntegrityError("Phase 8 report roll count is invalid")
    if any(type(value) is not int or value < 0 for value in (candidate_count, simultaneous_selection_abstentions, position_overlap_abstentions)):
        raise IntegrityError("Phase 8 report scheduling count is invalid")
    if any(not isinstance(market, str) or type(fold) is not int or type(bucket) is not int for market, fold, bucket in fold_local_fallback_keys):
        raise IntegrityError("Phase 8 report fold-local fallback key is invalid")
    scenarios = {
        name: {
            "aggregate": _metrics(result.aggregate),
            "market_year_coverage_complete": result.market_year_coverage_complete,
            "beats_required_baselines": result.beats_required_baselines,
            "identical_fixed_risk_comparator_matches": result.identical_fixed_risk_comparator_matches,
            "baseline_net_pnl_usd": {key: str(value) for key, value in sorted(result.baseline_net_pnl_usd.items())},
            "by_market_year": {key: _metrics(value) for key, value in sorted(result.by_market_year.items())},
            "skipped_trade_count": result.skipped_trade_count,
        }
        for name, result in sorted(evaluation.scenarios.items())
    }
    core = {
        "schema_version": "tier1_phase8_evaluation_reports/1.0.0",
        "preparation_id": preparation_id,
        "prediction_release_id": pinned.prediction_release_id,
        "trial_id": pinned.trial_id,
        "input_pair_count": len(pinned.input_pairs),
        "result_label": evaluation.result_label,
        "excluded_roll_count": excluded_roll_count,
        "fold_local_fallback": {
            "rule": "same_market_outer_fold_unconditional_training_direction",
            "count": len(fold_local_fallback_keys),
            "affected_market_fold_minute_buckets": [list(key) for key in sorted(fold_local_fallback_keys)],
        },
        "trade_scheduling": {
            "candidate_count": candidate_count,
            "simultaneous_selection_abstentions": simultaneous_selection_abstentions,
            "position_overlap_abstentions": position_overlap_abstentions,
        },
        "cost_scenarios": scenarios,
    }
    run_id = sha256_json(core)
    model_selection = {**core, "report_kind": "model_selection", "run_id": run_id}
    risk = {
        "schema_version": core["schema_version"],
        "report_kind": "risk",
        "run_id": run_id,
        "preparation_id": preparation_id,
        "prediction_release_id": pinned.prediction_release_id,
        "result_label": evaluation.result_label,
        "excluded_roll_count": excluded_roll_count,
        "fold_local_fallback_count": len(fold_local_fallback_keys),
        "trade_scheduling": {
            "candidate_count": candidate_count,
            "simultaneous_selection_abstentions": simultaneous_selection_abstentions,
            "position_overlap_abstentions": position_overlap_abstentions,
        },
        "cost_scenarios": {
            name: {
                "maximum_drawdown_usd": scenario["aggregate"]["maximum_drawdown_usd"],
                "gross_exposure_contract_equivalents": scenario["aggregate"]["gross_exposure_contract_equivalents"],
                "skipped_trade_count": scenario["skipped_trade_count"],
            }
            for name, scenario in scenarios.items()
        },
    }
    return Phase8EvaluationReports(run_id, model_selection, risk)


def _evaluate_after_approval(
    *, approved_read: _ApprovedCodexRealRead, pinned: PinnedPhase8Inputs,
    execution_rows: Sequence[Mapping[str, object]], evaluation_config: Mapping[str, object],
) -> Tier1Phase8Evaluation:
    """Internal-only evaluation bridge for a separately approved Codex task."""

    if type(approved_read) is not _ApprovedCodexRealRead or approved_read._secret is not _READ_SCOPE_SECRET:
        raise UnauthorizedOperation("Codex confirmation required before Phase 8 evaluation")
    prediction_rows = _read_pinned_rows_after_approval(approved_read=approved_read, pinned=pinned)
    trades = normalize_phase8_execution_rows(prediction_rows=prediction_rows, execution_rows=execution_rows)
    return evaluate_tier1_phase8_synthetic(trades=trades, evaluation_config=evaluation_config)


def _evaluate_pinned_after_approval(
    *, approved_read: _ApprovedCodexRealRead, pinned: PinnedPhase8Inputs,
    evaluation_config: Mapping[str, object], outer_folds: Sequence[Mapping[str, object]],
) -> tuple[Tier1Phase8Evaluation, int, tuple[tuple[str, int, int], ...], ScheduledExecutionRows]:
    """Internal real-release evaluation path; callable only after approval."""

    if type(approved_read) is not _ApprovedCodexRealRead or approved_read._secret is not _READ_SCOPE_SECRET:
        raise UnauthorizedOperation("Codex confirmation required before Phase 8 evaluation")
    opened = _read_all_pinned_phase8_rows_after_approval(approved_read=approved_read, pinned=pinned)
    directions = derive_fold_local_directions(
        prediction_rows=opened.predictions, outcome_rows=opened.outcomes, outer_folds=outer_folds,
    )
    converted = convert_pinned_source_bars_to_execution_rows(
        prediction_rows=opened.predictions, feature_rows=opened.features, outcome_rows=opened.outcomes,
        source_bar_rows=opened.source_bars, fold_local_directions=directions,
    )
    schedule = schedule_one_contract_execution_rows(
        prediction_rows=opened.predictions,
        execution_rows=converted.execution_rows,
        training_volatilities=derive_training_outcome_volatilities(
            outcome_rows=opened.outcomes, outer_folds=outer_folds,
        ),
    )
    trades = normalize_phase8_execution_rows(
        prediction_rows=opened.predictions, execution_rows=schedule.execution_rows,
    )
    expected_market_years = {
        f"{row['market']}/{row['year']}" for row in opened.predictions
        if isinstance(row.get("market"), str) and type(row.get("year")) is int
    }
    return (
        evaluate_tier1_phase8_synthetic(
            trades=trades,
            evaluation_config=evaluation_config,
            expected_market_years=expected_market_years,
        ),
        converted.excluded_roll_count,
        converted.fold_local_fallback_keys,
        schedule,
    )


def _publish_reports_after_approval(
    *, approved_read: _ApprovedCodexRealRead, root: Path, reports: Phase8EvaluationReports
) -> tuple[Path, Path]:
    """Create the two report bytes once; only Codex approved orchestration may call it."""

    if type(approved_read) is not _ApprovedCodexRealRead or approved_read._secret is not _READ_SCOPE_SECRET:
        raise UnauthorizedOperation("Codex confirmation required before Phase 8 report publication")
    target = root / "reports" / "phase8_evaluation" / reports.run_id
    if target.exists():
        raise IntegrityError("Phase 8 evaluation report collision")
    target.mkdir(parents=True)
    paths = (target / "model_selection.json", target / "risk.json")
    try:
        for path, payload in zip(paths, (reports.model_selection, reports.risk), strict=True):
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
            try:
                os.write(descriptor, canonical_bytes(payload) + b"\n")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    except Exception:
        # A partial run directory is deliberately not treated as an accepted report.
        raise
    return paths
