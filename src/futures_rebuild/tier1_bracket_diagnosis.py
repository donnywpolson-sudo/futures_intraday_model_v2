"""Read-only, reproducible diagnosis of a published Tier 1 bracket evaluation."""
from __future__ import annotations

import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Mapping

from .canonical import canonical_bytes, sha256_json
from .errors import IntegrityError
from .tier1_bracket_evaluation import (
    _Candidate, _entry_times, _load_json, _outcome_candidates, _prediction_rows,
    _read_prediction_index,
)
from .tier1_bracket_scheduler import BracketScheduleCandidate, schedule_bracket_candidates
from .tier1_phase8_evaluator import _scenario_costs


def _sum(groups: dict[str, dict[str, object]], key: str, gross: Decimal, fee: Decimal, slippage: Decimal) -> None:
    value = groups.setdefault(key, {"trades": 0, "gross_pnl_usd": Decimal("0"), "fees_usd": Decimal("0"), "slippage_usd": Decimal("0"), "net_pnl_usd": Decimal("0")})
    value["trades"] = int(value["trades"]) + 1
    value["gross_pnl_usd"] = Decimal(value["gross_pnl_usd"]) + gross
    value["fees_usd"] = Decimal(value["fees_usd"]) + fee
    value["slippage_usd"] = Decimal(value["slippage_usd"]) + slippage
    value["net_pnl_usd"] = Decimal(value["net_pnl_usd"]) + gross - fee - slippage


def _serialise(groups: Mapping[str, Mapping[str, object]]) -> dict[str, object]:
    return {key: {field: str(value) if isinstance(value, Decimal) else value for field, value in row.items()} for key, row in sorted(groups.items())}


def _scenario(*, candidates: tuple[_Candidate, ...], config: Mapping[str, object], name: str) -> dict[str, object]:
    scheduled: list[BracketScheduleCandidate] = []
    lookup: dict[tuple[int, int, str, str], _Candidate] = {}
    for item in candidates:
        fee, slips = _scenario_costs(config["costs"], name, item.market)
        cost = fee * Decimal(2) + Decimal(slips) * item.tick_value
        candidate = BracketScheduleCandidate(item.market, item.session, item.entry_at_ns, item.exit_at_ns, item.direction, item.score, item.risk, item.candidate_gross - cost)
        signature = (candidate.entry_at_ns, candidate.exit_at_ns, candidate.market, candidate.direction)
        if signature in lookup:
            raise IntegrityError("bracket diagnosis scheduler identity is ambiguous")
        lookup[signature] = item; scheduled.append(candidate)
    schedule = schedule_bracket_candidates(candidates=scheduled)
    # Mirror the scheduler's chronological state only to identify the first
    # time its already-locked drawdown guard begins blocking candidates.
    equity = peak = Decimal("0")
    open_until = -1
    first_drawdown_block: dict[str, object] | None = None
    entries: dict[str, int] = {}
    realized: dict[str, Decimal] = {}
    order = {market: index for index, market in enumerate(("ES", "CL", "ZN", "6E"))}
    grouped: dict[int, list[BracketScheduleCandidate]] = defaultdict(list)
    for item in scheduled:
        if item.planned_all_in_risk_usd <= Decimal("250"):
            grouped[item.entry_at_ns].append(item)
    for entry_at in sorted(grouped):
        group = grouped[entry_at]
        if entry_at < open_until:
            continue
        eligible = []
        for item in group:
            if peak - equity >= Decimal("1500"):
                if first_drawdown_block is None:
                    raw = lookup[(item.entry_at_ns, item.exit_at_ns, item.market, item.direction)]
                    first_drawdown_block = {"market": raw.market, "year": raw.year, "exchange_session_date": raw.session, "entry_at_ns": item.entry_at_ns}
            elif realized.get(item.exchange_session_date, Decimal("0")) <= Decimal("-500"):
                continue
            elif entries.get(item.exchange_session_date, 0) >= 3:
                continue
            else:
                eligible.append(item)
        if not eligible:
            continue
        selected = sorted(eligible, key=lambda item: (-abs(item.bounded_signal), order[item.market], item.direction))[0]
        entries[selected.exchange_session_date] = entries.get(selected.exchange_session_date, 0) + 1
        realized[selected.exchange_session_date] = realized.get(selected.exchange_session_date, Decimal("0")) + selected.realized_net_pnl_usd
        equity += selected.realized_net_pnl_usd; peak = max(peak, equity); open_until = selected.exit_at_ns
    by_market: dict[str, dict[str, object]] = {}
    by_market_year: dict[str, dict[str, object]] = {}
    by_fold: dict[str, dict[str, object]] = {}
    by_direction: dict[str, dict[str, object]] = {}
    by_exit: dict[str, dict[str, object]] = {}
    admitted = []
    for item in schedule.admitted:
        raw = lookup[(item.entry_at_ns, item.exit_at_ns, item.market, item.direction)]
        fee, slips = _scenario_costs(config["costs"], name, raw.market)
        fees = fee * Decimal(2); slippage = Decimal(slips) * raw.tick_value
        for target, key in ((by_market, raw.market), (by_market_year, f"{raw.market}/{raw.year}"), (by_fold, str(raw.outer_fold)), (by_direction, raw.direction), (by_exit, raw.exit_reason)):
            _sum(target, key, raw.candidate_gross, fees, slippage)
        admitted.append({"market": raw.market, "year": raw.year, "outer_fold": raw.outer_fold, "direction": raw.direction, "exit_reason": raw.exit_reason, "exchange_session_date": raw.session, "entry_at_ns": raw.entry_at_ns, "exit_at_ns": raw.exit_at_ns, "gross_pnl_usd": str(raw.candidate_gross), "fees_usd": str(fees), "slippage_usd": str(slippage), "net_pnl_usd": str(raw.candidate_gross - fees - slippage)})
    expected_market_years = {f"{market}/{year}" for market in ("ES", "CL", "ZN", "6E") for year in (2020, 2021, 2022)}
    for key in expected_market_years - set(by_market_year):
        by_market_year[key] = {"trades": 0, "gross_pnl_usd": Decimal("0"), "fees_usd": Decimal("0"), "slippage_usd": Decimal("0"), "net_pnl_usd": Decimal("0")}
    return {
        "admitted_trade_count": len(admitted), "by_market": _serialise(by_market), "by_market_year": _serialise(by_market_year),
        "by_outer_fold": _serialise(by_fold), "by_direction": _serialise(by_direction), "by_exit_reason": _serialise(by_exit),
        "scheduler_abstentions": {"candidate_count": len(candidates), "neutral": 0, "simultaneous": schedule.simultaneous_abstentions, "overlap": schedule.overlap_abstentions, "entry_cap": schedule.entry_cap_abstentions, "daily_stop": schedule.daily_stop_abstentions, "drawdown_stop": schedule.drawdown_stop_abstentions},
        "first_drawdown_stop_block": first_drawdown_block,
        "admitted_trades": admitted,
    }


def build_tier1_bracket_diagnosis(*, root: Path, prediction_index_release_id: str, evaluation_release_id: str, evaluation_config: Mapping[str, object]) -> dict[str, object]:
    """Reproduce published scenario totals with explanatory, non-tuning detail."""
    from .boundary import RepoBoundary
    from .data_layout import DataReleaseReceipt

    boundary = RepoBoundary(root)
    receipt = DataReleaseReceipt.from_manifest(root / "manifests/data_releases/evaluations" / f"{evaluation_release_id}.json", boundary)
    manifest = receipt.verify(boundary)
    if manifest.release_kind != "tier1_bracket_historical_evaluation" or prediction_index_release_id not in manifest.source_release_ids:
        raise IntegrityError("diagnosis does not match the published bracket evaluation")
    published = next((_load_json(receipt.resolve_file(item.logical_path, boundary)) for item in manifest.files if item.logical_path.endswith("model_selection.json")), None)
    if not isinstance(published, dict):
        raise IntegrityError("published model-selection report is missing")
    index, entries = _read_prediction_index(root=root, release_id=prediction_index_release_id)
    predictions, prediction_receipts = _prediction_rows(root=root, entries=entries)
    stage = root / "state/tier1_bracket_canonical_stage/457d01715d13d82248ac33794d02b6e7a8471fc38f12aac8a6349228b91858de"
    candidates = _outcome_candidates(root=root, prediction_rows=predictions, entry_times=_entry_times(root=root, predictions=predictions, receipts=prediction_receipts), stage=stage)
    scenarios = {name: _scenario(candidates=candidates, config=evaluation_config, name=name) for name in ("base", "stress", "extreme")}
    for name, result in scenarios.items():
        total = sum(Decimal(row["net_pnl_usd"]) for row in result["admitted_trades"])
        published_total = Decimal(str(published["cost_scenarios"][name]["aggregate"]["net_pnl_usd"]))
        if total != published_total or result["admitted_trade_count"] != published["cost_scenarios"][name]["scheduler"]["admitted_count"]:
            raise IntegrityError("diagnosis does not reproduce published scenario totals")
    core = {"schema_version": "tier1_bracket_post_evaluation_diagnosis/1.0.0", "evaluation_release_id": evaluation_release_id, "prediction_index_release_id": prediction_index_release_id, "result_label": "PROVISIONAL_EXECUTION_COSTS", "scenarios": scenarios, "supported_claims": ["Published scenario totals and admitted-trade counts reproduce exactly.", "Costs and locked scheduler behavior can be decomposed from pinned local artifacts."], "blocked_assumptions": ["No live-fill, spread, queue, partial-fill, halt, disconnect, margin, current Apex-rule, or exact-ZN-fee claim is supported.", "The diagnosis does not support parameter tuning or live-readiness claims."]}
    return {**core, "diagnosis_id": sha256_json(core)}


def write_local_tier1_bracket_diagnosis(*, root: Path, diagnosis: Mapping[str, object]) -> Path:
    """Write one non-release canonical report; accepted releases are untouched."""
    run_id = diagnosis.get("diagnosis_id")
    if not isinstance(run_id, str):
        raise IntegrityError("bracket diagnosis identity is invalid")
    path = root / "reports/tier1_bracket_diagnosis" / run_id / "diagnosis.json"
    data = canonical_bytes(dict(diagnosis)) + b"\n"
    if path.exists():
        if path.read_bytes() != data:
            raise IntegrityError("bracket diagnosis path collides with different bytes")
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path
