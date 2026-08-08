"""Approved-read evaluator for the registered Tier 1 bracket trial.

This is intentionally separate from the retired five-minute Phase 8 adapter.
It consumes only the frozen bracket predictions, their checkpoint-bound labels,
and the exact local causal bars needed to recover a next-*eligible* entry.
"""
from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Mapping

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import DataReleaseManifest, DataReleaseReceipt, PhasePublisher
from .errors import IntegrityError
from .tier1_bracket_finalizer import (
    MARKETS, build_bracket_chronological_split_plan, load_canonical_bracket_checkpoints,
)
from .tier1_bracket_interval_resolver import classify_source_disposition
from .tier1_bracket_scheduler import BracketScheduleCandidate, schedule_bracket_candidates
from .tier1_phase8_evaluator import _metrics, _scenario_costs


PREDICTION_INDEX_FILENAME = "data/reference/economics/tier1_bracket_frozen_prediction_index.json"
REQUIRED_BASELINES = (
    "flat_no_trade", "fold_local_unconditional_return_by_market_session",
    "previous_bar_sign_momentum", "previous_bar_sign_reversal",
    "risk_matched_always_long_intraday",
)
ALL_BASELINES = (*REQUIRED_BASELINES, "equal_risk_version_of_candidate_signal")


@dataclass(frozen=True)
class _BaselinePath:
    direction: str
    exit_at_ns: int
    planned_all_in_risk_usd: Decimal
    gross_pnl_usd: Decimal


@dataclass(frozen=True)
class _Candidate:
    key: str
    market: str
    year: int
    session: str
    entry_at_ns: int
    exit_at_ns: int
    direction: str
    score: float
    risk: Decimal
    candidate_gross: Decimal
    baselines: Mapping[str, Decimal]
    tick_value: Decimal
    outer_fold: int
    exit_reason: str
    baseline_paths: Mapping[str, _BaselinePath | None]
    candidate_active: bool = True


def _load_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"bracket evaluation cannot read {path.name}") from exc
    if not isinstance(payload, dict):
        raise IntegrityError("bracket evaluation JSON must be an object")
    return payload


def _read_prediction_index(*, root: Path, release_id: str) -> tuple[DataReleaseReceipt, list[dict[str, object]]]:
    boundary = RepoBoundary(root)
    receipt = DataReleaseReceipt.from_manifest(root / "manifests/data_releases/reference" / f"{release_id}.json", boundary)
    manifest = receipt.verify(boundary)
    if manifest.release_kind != "tier1_bracket_frozen_prediction_index":
        raise IntegrityError("bracket evaluation requires the aggregate frozen-prediction index")
    payload_path = receipt.resolve_file(PREDICTION_INDEX_FILENAME, boundary)
    payload = _load_json(payload_path)
    entries = payload.get("prediction_releases")
    expected = {(market, year) for market in MARKETS for year in (2020, 2021, 2022)}
    if (
        payload.get("schema_version") != "tier1_bracket_frozen_prediction_index/1.0.0"
        or not isinstance(entries, list)
        or {(item.get("market"), item.get("year")) for item in entries if isinstance(item, dict)} != expected
    ):
        raise IntegrityError("bracket frozen-prediction index has incomplete coverage")
    return receipt, [dict(item) for item in entries if isinstance(item, dict)]


def _prediction_rows(*, root: Path, entries: list[dict[str, object]]) -> tuple[dict[tuple[str, int], dict[str, dict[str, object]]], tuple[DataReleaseReceipt, ...]]:
    import pyarrow.parquet as pq

    boundary = RepoBoundary(root)
    result: dict[tuple[str, int], dict[str, dict[str, object]]] = {}
    receipts: list[DataReleaseReceipt] = []
    required = {"market", "year", "exchange_session_date", "actual_identity_hash", "decision_at_ns", "outer_fold", "upstream_source_row_sha256", "bounded_signal", "selected_direction"}
    for entry in entries:
        market, year, release_id = entry.get("market"), entry.get("year"), entry.get("prediction_release_id")
        if not isinstance(market, str) or type(year) is not int or not isinstance(release_id, str):
            raise IntegrityError("bracket frozen-prediction index entry is invalid")
        receipt = DataReleaseReceipt.from_manifest(root / "manifests/data_releases/predictions" / f"{release_id}.json", boundary)
        manifest = receipt.verify(boundary)
        if manifest.release_kind != "tier1_bracket_frozen_predictions" or manifest.metadata.get("market") != market or manifest.metadata.get("year") != year:
            raise IntegrityError("bracket prediction receipt differs from aggregate index")
        matches = [item.logical_path for item in manifest.files if item.logical_path.endswith("/frozen_predictions.parquet")]
        if len(matches) != 1:
            raise IntegrityError("bracket prediction receipt has no unique payload")
        reader = pq.ParquetFile(receipt.resolve_file(matches[0], boundary))
        if not required <= set(reader.schema_arrow.names):
            raise IntegrityError("bracket prediction payload lacks required fields")
        rows: dict[str, dict[str, object]] = {}
        for batch in reader.iter_batches(batch_size=65_536, columns=sorted(required)):
            for row in batch.to_pylist():
                key = row.get("upstream_source_row_sha256")
                if not isinstance(key, str) or key in rows or row.get("market") != market or row.get("year") != year:
                    raise IntegrityError("bracket prediction rows are ambiguous or outside their receipt")
                if row.get("selected_direction") not in {"long", "short", "neutral"} or not isinstance(row.get("bounded_signal"), float):
                    raise IntegrityError("bracket prediction direction or score is invalid")
                rows[key] = dict(row)
        if not rows:
            raise IntegrityError("bracket prediction receipt is empty")
        result[(market, year)] = rows
        receipts.append(receipt)
    return result, tuple(receipts)


def _fold_local_directions(*, root: Path, stage: Path) -> tuple[dict[tuple[str, int, int], str], dict[tuple[str, int], str]]:
    """Derive training-only minute and market fallbacks from checkpoint labels."""
    import pyarrow.parquet as pq

    plan = build_bracket_chronological_split_plan(stage=stage)
    sums: dict[tuple[str, int, int], list[Decimal]] = defaultdict(lambda: [Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")])
    broad: dict[tuple[str, int], list[Decimal]] = defaultdict(lambda: [Decimal("0"), Decimal("0"), Decimal("0"), Decimal("0")])
    manifest = _load_json(stage / "canonical_bracket_stage.json")
    pairs = manifest.get("chunk_pairs")
    if not isinstance(pairs, list):
        raise IntegrityError("bracket canonical stage is invalid")
    for item in pairs:
        if not isinstance(item, dict):
            raise IntegrityError("bracket canonical chunk pair is invalid")
        market = item.get("market")
        if not isinstance(market, str):
            raise IntegrityError("bracket canonical market is invalid")
        features = pq.read_table(stage / str(item["feature"]))
        outcomes = pq.read_table(stage / str(item["outcome"]))
        if features.num_rows != outcomes.num_rows:
            raise IntegrityError("bracket labels/features are not aligned")
        for feature, outcome in zip(features.to_pylist(), outcomes.to_pylist(), strict=True):
            if feature.get("status") != "FEATURE_READY" or outcome.get("status") != "MATURED":
                continue
            date = feature.get("exchange_session_date")
            stamp = feature.get("decision_at_ns")
            if not isinstance(date, str) or type(stamp) is not int:
                raise IntegrityError("bracket training row timing is invalid")
            minute = (stamp // 60_000_000_000) % 1440
            long = Decimal(str(outcome.get("long_realized_net_r")))
            short = Decimal(str(outcome.get("short_realized_net_r")))
            for fold in plan["outer_folds"]:
                number = fold["outer_fold"]
                start, end = fold["fit_session_dates"]
                if not isinstance(number, int) or not isinstance(start, str) or not isinstance(end, str) or not start <= date <= end:
                    continue
                target = sums[(market, number, minute)]
                target[0] += long; target[1] += Decimal(1); target[2] += short; target[3] += Decimal(1)
                all_minutes = broad[(market, number)]
                all_minutes[0] += long; all_minutes[1] += Decimal(1); all_minutes[2] += short; all_minutes[3] += Decimal(1)
    choose = lambda values: "long" if values[0] / values[1] >= values[2] / values[3] else "short"
    return ({key: choose(value) for key, value in sums.items()}, {key: choose(value) for key, value in broad.items()})


def _entry_times(*, root: Path, predictions: Mapping[tuple[str, int], Mapping[str, Mapping[str, object]]], receipts: tuple[DataReleaseReceipt, ...]) -> dict[str, tuple[int, Decimal]]:
    """Find the next actual eligible bar, never ``decision + one minute``."""
    import pyarrow.parquet as pq

    boundary = RepoBoundary(root)
    resolved: dict[str, tuple[int, Decimal]] = {}
    for receipt in receipts:
        manifest = receipt.verify(boundary)
        market, year = manifest.metadata["market"], manifest.metadata["year"]
        if not isinstance(market, str) or type(year) is not int:
            raise IntegrityError("bracket prediction provenance lacks market/year")
        wanted = predictions[(market, year)]
        causal = manifest.metadata.get("causal_release_id")
        if not isinstance(causal, str):
            raise IntegrityError("bracket prediction provenance lacks causal receipt")
        causal_receipt = DataReleaseReceipt.from_manifest(root / "manifests/data_releases/causally_gated_normalized" / f"{causal}.json", boundary)
        causal_manifest = causal_receipt.verify(boundary)
        bars = [item.logical_path for item in causal_manifest.files if item.logical_path.endswith("/bars.parquet")]
        if len(bars) != 1:
            raise IntegrityError("bracket causal receipt has no unique bars payload")
        reader = pq.ParquetFile(causal_receipt.resolve_file(bars[0], boundary))
        columns = {"source_row_sha256", "event_at_ns", "actual_identity_hash", "disposition", "tick_value"}
        if not columns <= set(reader.schema_arrow.names):
            raise IntegrityError("bracket causal bars lack entry-resolution fields")
        pending: list[tuple[str, str]] = []
        for batch in reader.iter_batches(batch_size=65_536, columns=sorted(columns)):
            for row in batch.to_pylist():
                identity = row.get("actual_identity_hash")
                if isinstance(identity, str) and classify_source_disposition(row.get("disposition")):
                    for key, expected_identity in pending:
                        if identity == expected_identity and key not in resolved:
                            event = row.get("event_at_ns")
                            tick_value = row.get("tick_value")
                            if type(event) is not int or tick_value is None:
                                raise IntegrityError("bracket entry bar timestamp is invalid")
                            tick = Decimal(str(tick_value))
                            if not tick.is_finite() or tick <= 0:
                                raise IntegrityError("bracket entry tick value is invalid")
                            resolved[key] = (event, tick)
                    pending = [(key, expected) for key, expected in pending if key not in resolved]
                source = row.get("source_row_sha256")
                prediction = wanted.get(source) if isinstance(source, str) else None
                if prediction is not None:
                    expected = prediction.get("actual_identity_hash")
                    if not isinstance(expected, str):
                        raise IntegrityError("bracket prediction actual identity is invalid")
                    pending.append((source, expected))
        unresolved = {key for key in wanted if key not in resolved}
        if unresolved:
            raise IntegrityError(f"bracket next-eligible entry is unavailable for {market}-{year}: {len(unresolved)} rows")
    return resolved


def _outcome_candidates(
    *, root: Path, prediction_rows: Mapping[tuple[str, int], Mapping[str, Mapping[str, object]]],
    entry_times: Mapping[str, tuple[int, Decimal]], stage: Path,
    include_neutral_baseline_opportunities: bool = False,
) -> tuple[_Candidate, ...]:
    import pyarrow.parquet as pq

    minute_rules, broad_rules = _fold_local_directions(root=root, stage=stage)
    manifest = _load_json(stage / "canonical_bracket_stage.json")
    candidates: list[_Candidate] = []
    for item in manifest["chunk_pairs"]:
        market, year = item["market"], item["year"]
        if (market, year) not in prediction_rows:
            continue
        features = pq.read_table(stage / str(item["feature"]))
        outcomes = pq.read_table(stage / str(item["outcome"]))
        for feature, outcome in zip(features.to_pylist(), outcomes.to_pylist(), strict=True):
            key = feature.get("upstream_source_row_sha256")
            prediction = prediction_rows[(market, year)].get(key) if isinstance(key, str) else None
            if prediction is None:
                continue
            if feature.get("status") != "FEATURE_READY" or outcome.get("status") != "MATURED":
                raise IntegrityError("frozen prediction lacks a mature bracket label")
            direction = prediction["selected_direction"]
            if direction == "neutral":
                if not include_neutral_baseline_opportunities:
                    continue
                candidate_active = False
                prefix = "long"
            else:
                candidate_active = True
                prefix = str(direction)
            try:
                gross = Decimal(str(outcome[f"{prefix}_realized_gross_pnl_usd"]))
                risk = Decimal(str(outcome[f"{prefix}_planned_all_in_risk_usd"]))
                exit_at = outcome[f"{prefix}_exit_at_ns"]
                long_gross = Decimal(str(outcome["long_realized_gross_pnl_usd"]))
                short_gross = Decimal(str(outcome["short_realized_gross_pnl_usd"]))
                long_risk = Decimal(str(outcome["long_planned_all_in_risk_usd"]))
                short_risk = Decimal(str(outcome["short_planned_all_in_risk_usd"]))
                long_exit_at = outcome["long_exit_at_ns"]
                short_exit_at = outcome["short_exit_at_ns"]
            except (KeyError, ArithmeticError) as exc:
                raise IntegrityError("bracket outcome is incomplete") from exc
            if (
                type(exit_at) is not int
                or type(long_exit_at) is not int
                or type(short_exit_at) is not int
                or not all(value.is_finite() and value > 0 for value in (risk, long_risk, short_risk))
                or not gross.is_finite()
            ):
                raise IntegrityError("bracket outcome economics are invalid")
            stamp = prediction["decision_at_ns"]
            minute = (stamp // 60_000_000_000) % 1440
            fold = prediction.get("outer_fold")
            if type(fold) is not int:
                raise IntegrityError("bracket prediction fold is invalid")
            fold_direction = minute_rules.get((market, fold, minute), broad_rules.get((market, fold)))
            if fold_direction not in {"long", "short"}:
                raise IntegrityError("bracket fold-local baseline has no training-only fallback")
            momentum = "long" if float(feature["bar_return"]) >= 0 else "short"
            lookup = {"long": long_gross, "short": short_gross}
            path = {
                "long": _BaselinePath("long", long_exit_at, long_risk, long_gross),
                "short": _BaselinePath("short", short_exit_at, short_risk, short_gross),
            }
            candidates.append(_Candidate(
                key, market, year, str(prediction["exchange_session_date"]), entry_times[key][0], exit_at, prefix,
                float(prediction["bounded_signal"]), risk, gross,
                {
                    "flat_no_trade": Decimal("0"),
                    "fold_local_unconditional_return_by_market_session": lookup[fold_direction],
                    "previous_bar_sign_momentum": lookup[momentum],
                    "previous_bar_sign_reversal": lookup["short" if momentum == "long" else "long"],
                    "risk_matched_always_long_intraday": long_gross,
                    "equal_risk_version_of_candidate_signal": gross if candidate_active else Decimal("0"),
                },
                entry_times[key][1],
                fold,
                str(outcome[f"{prefix}_exit_reason"]),
                {
                    "fold_local_unconditional_return_by_market_session": path[fold_direction],
                    "previous_bar_sign_momentum": path[momentum],
                    "previous_bar_sign_reversal": path["short" if momentum == "long" else "long"],
                    "risk_matched_always_long_intraday": path["long"],
                    "equal_risk_version_of_candidate_signal": path[prefix] if candidate_active else None,
                },
                candidate_active,
            ))
    if not candidates:
        raise IntegrityError("bracket evaluation has no non-neutral candidates")
    return tuple(candidates)


ZERO_METRICS = {
    "net_pnl_usd": "0", "sharpe": 0.0, "sortino": 0.0,
    "maximum_drawdown_usd": "0", "turnover_contract_equivalents": 0,
    "hit_rate": 0.0, "gross_exposure_contract_equivalents": 0,
    "net_directional_contract_equivalents": 0, "observation_count": 0,
}


def _metric_payload(net: list[Decimal], quantities: list[int]) -> dict[str, object]:
    if not net:
        return dict(ZERO_METRICS)
    metric = _metrics(net, quantities)
    return {
        **metric.__dict__,
        "net_pnl_usd": str(metric.net_pnl_usd),
        "maximum_drawdown_usd": str(metric.maximum_drawdown_usd),
    }


def _scheduler_payload(schedule: object | None) -> dict[str, int]:
    if schedule is None:
        return {
            "admitted_count": 0, "neutral_abstentions": 0, "simultaneous_abstentions": 0,
            "overlap_abstentions": 0, "entry_cap_abstentions": 0,
            "daily_stop_abstentions": 0, "drawdown_stop_abstentions": 0,
        }
    return {
        "admitted_count": len(schedule.admitted),
        "neutral_abstentions": schedule.neutral_abstentions,
        "simultaneous_abstentions": schedule.simultaneous_abstentions,
        "overlap_abstentions": schedule.overlap_abstentions,
        "entry_cap_abstentions": schedule.entry_cap_abstentions,
        "daily_stop_abstentions": schedule.daily_stop_abstentions,
        "drawdown_stop_abstentions": schedule.drawdown_stop_abstentions,
    }


def _strategy_path(item: _Candidate, strategy: str) -> _BaselinePath | None:
    if strategy == "flat_no_trade":
        return None
    if strategy == "candidate":
        if not item.candidate_active:
            return None
        return _BaselinePath(item.direction, item.exit_at_ns, item.risk, item.candidate_gross)
    try:
        return item.baseline_paths[strategy]
    except KeyError as exc:
        raise IntegrityError(f"bracket evaluator lacks independent path for {strategy}") from exc


def _evaluate_strategy(
    *, candidates: tuple[_Candidate, ...], evaluation_config: Mapping[str, object], scenario: str, strategy: str,
) -> dict[str, object]:
    """Schedule one strategy independently, including its own exits and risk path."""

    if strategy == "flat_no_trade":
        return {"metrics": dict(ZERO_METRICS), "scheduler": _scheduler_payload(None), "by_market_year": {}}
    scheduled: list[BracketScheduleCandidate] = []
    raw_by_identity: dict[int, _Candidate] = {}
    neutral_abstentions = 0
    for item in candidates:
        path = _strategy_path(item, strategy)
        if path is None:
            if strategy not in {"candidate", "equal_risk_version_of_candidate_signal"}:
                raise IntegrityError("active bracket baseline unexpectedly has no path")
            neutral_abstentions += 1
            continue
        fee, slippage = _scenario_costs(evaluation_config["costs"], scenario, item.market)
        cost = fee * Decimal(2) + Decimal(slippage) * item.tick_value
        candidate = BracketScheduleCandidate(
            item.market, item.session, item.entry_at_ns, path.exit_at_ns, path.direction,
            item.score, path.planned_all_in_risk_usd, path.gross_pnl_usd - cost,
        )
        scheduled.append(candidate)
        raw_by_identity[id(candidate)] = item
    schedule = schedule_bracket_candidates(candidates=scheduled)
    net = [item.realized_net_pnl_usd for item in schedule.admitted]
    quantities = [1 if item.direction == "long" else -1 for item in schedule.admitted]
    by_market_year_net: dict[str, list[Decimal]] = defaultdict(list)
    by_market_year_quantities: dict[str, list[int]] = defaultdict(list)
    for item in schedule.admitted:
        raw = raw_by_identity[id(item)]
        key = f"{raw.market}/{raw.year}"
        by_market_year_net[key].append(item.realized_net_pnl_usd)
        by_market_year_quantities[key].append(1 if item.direction == "long" else -1)
    scheduler_payload = _scheduler_payload(schedule)
    scheduler_payload["neutral_abstentions"] = neutral_abstentions
    return {
        "metrics": _metric_payload(net, quantities),
        "scheduler": scheduler_payload,
        "by_market_year": {
            key: _metric_payload(values, by_market_year_quantities[key])
            for key, values in sorted(by_market_year_net.items())
        },
    }


def _strategy_views(
    *, candidates: tuple[_Candidate, ...], evaluation_config: Mapping[str, object], scenario: str,
) -> dict[str, object]:
    return {
        strategy: _evaluate_strategy(
            candidates=candidates, evaluation_config=evaluation_config, scenario=scenario, strategy=strategy,
        )
        for strategy in ("candidate", *ALL_BASELINES)
    }


def _report_payload(*, candidates: tuple[_Candidate, ...], evaluation_config: Mapping[str, object], trial_id: str, prediction_index_id: str) -> tuple[dict[str, object], dict[str, object]]:
    """Apply independent strategy paths and both continuous and reset diagnostics."""
    expected = {f"{market}/{year}" for market in MARKETS for year in (2020, 2021, 2022)}
    if {f"{item.market}/{item.year}" for item in candidates} != expected:
        raise IntegrityError("bracket candidate inputs lack complete out-of-sample market-year coverage")
    market_year_groups = {
        key: tuple(item for item in candidates if f"{item.market}/{item.year}" == key) for key in sorted(expected)
    }
    fold_groups = {
        str(fold): tuple(item for item in candidates if item.outer_fold == fold)
        for fold in sorted({item.outer_fold for item in candidates})
    }
    scenario_payload: dict[str, object] = {}
    for scenario in ("base", "stress", "extreme"):
        continuous = _strategy_views(candidates=candidates, evaluation_config=evaluation_config, scenario=scenario)
        candidate = continuous["candidate"]
        baseline_totals = {
            name: continuous[name]["metrics"]["net_pnl_usd"] for name in ALL_BASELINES
        }
        independent_market_year = {
            key: _strategy_views(candidates=group, evaluation_config=evaluation_config, scenario=scenario)
            for key, group in market_year_groups.items()
        }
        independent_outer_fold = {
            key: _strategy_views(candidates=group, evaluation_config=evaluation_config, scenario=scenario)
            for key, group in fold_groups.items()
        }
        candidate_total = Decimal(str(candidate["metrics"]["net_pnl_usd"]))
        by_market_year = {
            key: candidate["by_market_year"].get(key, dict(ZERO_METRICS)) for key in sorted(expected)
        }
        scenario_payload[scenario] = {
            "aggregate": candidate["metrics"],
            "by_market_year": by_market_year,
            "baseline_net_pnl_usd": baseline_totals,
            "beats_required_baselines": all(
                candidate_total > Decimal(str(baseline_totals[name])) for name in REQUIRED_BASELINES
            ),
            "identical_fixed_risk_comparator_matches": (
                baseline_totals["equal_risk_version_of_candidate_signal"] == candidate["metrics"]["net_pnl_usd"]
            ),
            "scheduler": candidate["scheduler"],
            "continuous_account": {"strategies": continuous},
            "independent_market_year": independent_market_year,
            "independent_outer_fold": independent_outer_fold,
        }
    core = {"schema_version": "tier1_bracket_evaluation/2.0.0", "trial_id": trial_id, "prediction_index_release_id": prediction_index_id, "result_label": "PROVISIONAL_EXECUTION_COSTS", "cost_scenarios": scenario_payload}
    run_id = sha256_json(core)
    model = {**core, "report_kind": "model_selection", "run_id": run_id}
    risk = {"schema_version": core["schema_version"], "report_kind": "risk", "run_id": run_id, "trial_id": trial_id, "prediction_index_release_id": prediction_index_id, "result_label": core["result_label"], "cost_scenarios": {name: {"aggregate": value["aggregate"], "scheduler": value["scheduler"], "independent_market_year": {key: segment["candidate"] for key, segment in value["independent_market_year"].items()}, "independent_outer_fold": {key: segment["candidate"] for key, segment in value["independent_outer_fold"].items()}} for name, value in scenario_payload.items()}}
    return model, risk


def evaluate_and_publish_tier1_bracket(*, root: Path, prediction_index_release_id: str, evaluation_config: Mapping[str, object]) -> DataReleaseReceipt:
    """Run the approved local evaluation and conditionally publish one report release."""
    index, entries = _read_prediction_index(root=root, release_id=prediction_index_release_id)
    predictions, receipts = _prediction_rows(root=root, entries=entries)
    stage = root / "state/tier1_bracket_canonical_stage/457d01715d13d82248ac33794d02b6e7a8471fc38f12aac8a6349228b91858de"
    entry_times = _entry_times(root=root, predictions=predictions, receipts=receipts)
    candidates = _outcome_candidates(
        root=root, prediction_rows=predictions, entry_times=entry_times, stage=stage,
        include_neutral_baseline_opportunities=True,
    )
    model, risk = _report_payload(candidates=candidates, evaluation_config=evaluation_config, trial_id=str(index.verify(RepoBoundary(root)).metadata["trial_id"]), prediction_index_id=index.release_id)
    boundary = RepoBoundary(root)
    operation = OperationReceipt.issue_local(boundary, operation="PUBLISH_RELEASE", classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA, scope={"operation": "tier1_bracket_historical_evaluation", "scope": "ES_CL_ZN_6E_2020_2022_only"})
    publisher = PhasePublisher(boundary=boundary, operation_receipt=operation, lock_path=root / "state/locks/tier1-bracket-evaluation-publication.lock")
    stage_out = publisher.create_stage("tier1_bracket_evaluation")
    (stage_out / "model_selection.json").write_bytes(canonical_bytes(model) + b"\n")
    (stage_out / "risk.json").write_bytes(canonical_bytes(risk) + b"\n")
    manifest = DataReleaseManifest.build(stage_out, phase="evaluations", release_kind="tier1_bracket_historical_evaluation", schema_version="2.0.0", logical_paths={"model_selection.json": f"data/evaluations/tier1_bracket/{model['trial_id']}/aggregate/model_selection.json", "risk.json": f"data/evaluations/tier1_bracket/{model['trial_id']}/aggregate/risk.json"}, source_release_ids=tuple(sorted({index.release_id, *[item.release_id for item in receipts]})), metadata={"run_id": model["run_id"], "result_label": "PROVISIONAL_EXECUTION_COSTS", "prediction_release_count": len(receipts)})
    receipt = DataReleaseReceipt.from_manifest(publisher.publish(stage_out, manifest), boundary)
    receipt.verify(boundary)
    return receipt
