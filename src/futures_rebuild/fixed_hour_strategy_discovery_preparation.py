"""Prepare inactive contracts and synthetic evidence; never opens market data."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from .canonical import sha256_file, sha256_json, write_new_canonical_authority
from .errors import IntegrityError
from .exchange_calendar import CalendarSession, TradingStateInterval
from .fixed_hour_globex_pipeline import _synthetic_row
from .fixed_hour_outcome_foundation import synthetic_globex_hourly_opportunity_grid
from .fixed_hour_strategy_discovery import (
    AUTHORITY, FEATURE_NAMES, MARKETS, TerminalDataset, assemble_opportunities,
    provisional_costs,
)
from .fixed_hour_strategy_discovery import candidate_definitions, design, run_market_wfa
from . import fixed_hour_model_comparison as comparison
from . import profit_aligned_research as profit
from .fixed_hour_terminal_preparation import ledger_snapshot as predecessor_ledger_snapshot
from .fixed_hour_strategy_discovery_evaluation import (
    coverage_report, downstream_eligibility, evaluate_outer_predictions,
    qualify_markets, terminal_failure, trial_burden,
)

CONFIG = Path("configs/fixed_hour_strategy_discovery_v1.json")
EVIDENCE = Path("state/unpublished_evidence/fixed_hour_strategy_discovery")
IMPLEMENTATION = (
    "fixed_hour_strategy_discovery.py", "fixed_hour_strategy_discovery_evaluation.py", "fixed_hour_strategy_discovery_preparation.py",
    "profit_aligned_research.py", "canonical.py", "research/contracts.py", "fixed_hour_globex_pipeline.py",
    "fixed_hour_model_comparison.py", "fixed_hour_model_comparison_evaluation.py", "fixed_hour_model_comparison_preparation.py",
    "fixed_hour_terminal_successor.py", "fixed_hour_terminal_evaluation.py", "fixed_hour_terminal_preparation.py",
    "fixed_hour_feature_expansion.py", "fixed_hour_multifamily_discovery.py", "fixed_hour_outcome_foundation.py",
    "fixed_hour_ridge_audit_successor.py", "fixed_hour_ridge_discovery.py", "exchange_calendar.py",
    "research/splits.py", "research/hac.py", "research/dsr.py", "research/multiple_testing.py", "research/bootstrap.py",
)
PREDECESSORS = (
    "configs/fixed_hour_model_comparison_v2.json", "configs/fixed_hour_model_comparison_v6.json",
    "configs/fixed_hour_model_comparison_v1.json",
    "configs/fixed_hour_terminal_successor_v2.json", "requirements-research-ml.lock",
    "configs/fixed_hour_directional_outcome_foundation_v3.json",
    "configs/fixed_hour_feature_expansion_foundation_v3.json",
    "configs/fixed_hour_globex_mechanism_evaluation_v4.json",
    "configs/active_alpha_research_ladder.json",
    "configs/active_alpha_historical_research_design.json",
    "configs/active_alpha_ladder_execution_design.json",
)


def ledger_snapshot(root: Path) -> dict:
    """Registration-only accounting, preserving all existing prepared charges.

    Versioned preparations of the same mechanism are not separate experiments.
    Remove known registrations from the legacy pool before adding each complete
    declared family once, including this requested but unregistered successor.
    """
    prior = predecessor_ledger_snapshot(root)
    families = [comparison.prior.design(), comparison.design(), profit.design(), design()]
    family_ids = [{f"{spec['mechanism_id']}/{m}/{c['candidate_id']}"
                   for m in MARKETS for c in spec["candidates"]} for spec in families]
    if tuple(map(len, family_ids)) != (108, 180, 180, 200):
        raise IntegrityError("declared predecessor or successor search size drifted")
    known = set().union(*family_ids)
    identities = set(prior["candidate_identities"])
    prior_only = identities - {"candidate:" + i for i in known}
    registered = sorted(i for i in family_ids[-1] if "candidate:" + i in identities)
    # Existing terminal successor: 108; comparison: 180; profit-aligned: 180.
    # These are accounting charges, never an observed Sharpe census or upper bound.
    floor = max(181, prior["legacy_penalty"] + len(prior_only)) + 108 + 180 + 180
    entries = ([{"trial_id": design()["mechanism_id"], "count": len(registered),
                 "included_in_prior_floor": False, "configuration_ids": registered}]
               if registered else [])
    return {**prior, "predecessor_burden": prior["burden"],
            "preserved_prepared_families": [{"mechanism_id": s["mechanism_id"], "configuration_count": len(ids)}
                                           for s, ids in zip(families[:-1], family_ids[:-1])],
            "accounting_is_observed_performance": False, "historical_upper_bound_established": False,
            "burden": trial_burden(floor, entries)}


def build_bundle(root: Path) -> dict:
    specification = design()
    mechanism = specification["mechanism_id"]
    phases = {}
    content = {"3": {"mechanism_card": specification["mechanism_card"], "markets": specification["markets"]},
               "4": specification["phase_4"], "5": specification["phase_5"],
               "6": {**specification["phase_6"],
                     "run_verification": "EXACT_DETERMINISTIC_SELECTION_AND_FITTING_REPLAY",
                     "expected_cost_clock": "OBSERVATIONS_BEFORE_EFFECTIVE_START;CERTIFICATION_BEFORE_BOUND_EVALUATION_FREEZE",
                     "evaluation_freeze_trust": "EXPLICIT_TRUSTED_CALLER_INPUT_MATCHING_COST_EVIDENCE;NO_IMPLICIT_WALL_CLOCK"},
               "ladder": specification["ladder"]}
    census = ledger_snapshot(root)
    for phase, rules in content.items():
        core = {"phase": phase, "mechanism_id": mechanism, "rules": rules,
                "state": "INACTIVE_SUCCESSOR", "authority": AUTHORITY, "trial_burden_id": census["burden"]["census_id"]}
        phases[phase] = {**core, "contract_id": sha256_json(core)}
    core = {"schema_version": "fixed_hour_strategy_discovery_preparation/1.0.0", "mechanism_id": mechanism,
            "specification": specification, "phase_contracts": phases, "trial_ledger": census,
            "test_files": {"tests/test_fixed_hour_strategy_discovery.py": sha256_file(root / "tests/test_fixed_hour_strategy_discovery.py")},
            "predecessor_files": {path: sha256_file(root / path) for path in PREDECESSORS},
            "implementation_files": {"src/futures_rebuild/" + name: sha256_file(root / "src/futures_rebuild" / name) for name in IMPLEMENTATION},
            "readiness": {"historical_research": "BLOCKED_CENSUS_RECOVERY_UNRESOLVED",
                          "source_bound_readiness": "NOT_CERTIFIED",
                          "expected_costs": "EXPECTED_COST_UNCALIBRATED",
                          "historical_search_upper_bound_established": False,
                          "profitability_established": False,
                          "selection_score": "SESSION_BALANCED_STRESS_NET_TICKS_PER_ELIGIBLE_OPPORTUNITY",
                          "capital_return_claim": False, "portfolio_claim": False},
            "state": "PREPARED_INACTIVE_NOT_REGISTERED", "authority": AUTHORITY}
    return {**core, "bundle_id": sha256_json(core)}


def validate_bundle(bundle: dict, root: Path) -> None:
    original = dict(bundle)
    if original.get("bundle_id") != sha256_json({k: v for k, v in original.items() if k != "bundle_id"}):
        raise IntegrityError("invalid successor bundle identity")
    certificate = original.pop("synthetic_certificate", None)
    if certificate is not None:
        path = root / certificate["path"]
        if path.resolve() != (root / EVIDENCE / certificate["certificate_id"] / "certificate.json").resolve():
            raise IntegrityError("synthetic certificate path is outside its exact evidence identity")
        if sha256_file(path) != certificate["sha256"]:
            raise IntegrityError("synthetic certificate bytes drifted")
        evidence = json.loads(path.read_bytes())
        if (evidence["certificate_id"] != certificate["certificate_id"]
            or evidence["certificate_id"] != sha256_json({k: v for k, v in evidence.items() if k != "certificate_id"})
            or evidence["mechanism_id"] != original["mechanism_id"] or evidence["state"] != "PASSED_ENGINEERING_ONLY"):
            raise IntegrityError("synthetic evidence belongs to a different mechanism")
        if (evidence.get("implementation_files") != original["implementation_files"]
            or evidence.get("test_files") != original["test_files"]
            or set(evidence["market_runs"]) != set(MARKETS)
            or any(r["outer_folds"] != 8 or r["candidate_count_per_fold"] != 50
                   for r in evidence["market_runs"].values())
            or len(evidence["qualification"]["declared_hypothesis_ids"]) != 28
            or evidence["qualification"]["phase_12_eligible_markets"]):
            raise IntegrityError("synthetic evidence is incomplete or not bound to this implementation")
    original.pop("bundle_id")
    original["bundle_id"] = sha256_json(original)
    if original != build_bundle(root):
        raise IntegrityError("successor bundle, dependencies, or trial ledger drifted; prepare a new immutable successor")


def synthetic_certificate() -> dict:
    """Complete schedule-to-qualification experiment on generated price paths.

    No provider, active data catalog, or historical-row reader is called. These
    intentionally learnable paths test control flow, never an economic premise.
    """
    reports, run_summaries, construction = {}, {}, {}
    dates = tuple((date(2018, 1, 1) + timedelta(days=i)).isoformat() for i in range(1010))
    for market in MARKETS:
        features, terminals, upside, downside, ids, row_dates, quartiles, decisions = [], [], [], [], [], [], [], []
        source_counts = {"scheduled": 0, "insufficient_history": 0, "valid_feature": 0, "invalid_outcome": 0, "quality_errors": 0}
        for day_index, day in enumerate(dates):
            start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
            session = CalendarSession(market, date.fromisoformat(day), (TradingStateInterval("OPEN", start, start + timedelta(hours=4)),))
            grid = synthetic_globex_hourly_opportunity_grid(session, calendar_release_id="a" * 64)
            # Alternating direction, varying magnitude, and varying minute
            # structure avoid degenerate inference while remaining deterministic.
            step = (-1 if day_index % 2 else 1) * (2 + day_index % 3)
            start_ns = int(start.timestamp()) * 1_000_000_000
            rows = [_synthetic_row(start_ns + minute * 60_000_000_000, minute, market) for minute in range(240)]
            for minute, row in enumerate(rows):
                tick = row["min_price_increment_nano"]
                opening = (400 + minute * step + (minute % 7)) * tick
                closing = opening + step * tick
                row.update(open_nano=opening, close_nano=closing, high_nano=max(opening, closing) + tick,
                           low_nano=min(opening, closing) - tick, project_trade_date=day,
                           source_row_sha256=sha256_json({"market": market, "day": day, "minute": minute, "step": step}))
            batch = assemble_opportunities(rows, grid, as_of_ns=rows[-1]["decision_eligible_at_ns"])
            for k in source_counts: source_counts[k] += batch["coverage"][k]
            for record in batch["records"]:
                if record["feature"]["status"] == "INSUFFICIENT_HISTORY": continue
                f, o = record["feature"], record["outcome"]
                if o["status"] != "MATURED": raise IntegrityError("synthetic complete path unexpectedly rejected")
                features.append(list(f["feature_values"].values())); terminals.append(o["fit_target"])
                upside.append(o["diagnostics"]["maximum_upside_excursion_ticks"])
                downside.append(o["diagnostics"]["maximum_downside_excursion_ticks"])
                ids.append(f["opportunity_id"]); row_dates.append(day); quartiles.append(f["quartile"]); decisions.append(f["decision_at_ns"])
        n = len(ids)
        tick_values = np.full(n, 12.5)
        dataset = TerminalDataset(market, tuple(ids), tuple(row_dates), np.array(features, dtype=np.float64),
                                  np.array(terminals, dtype=np.float64), provisional_costs(market, tick_values)["stress"], dates, tick_values)
        run = run_market_wfa(dataset)
        if len(run["outer_folds"]) != 8 or any(len(f["candidate_scores"]) != 50 for f in run["outer_folds"]):
            raise IntegrityError("synthetic comparison omitted frozen folds or candidates")
        count = sum(len(f["audit_indices"]) for f in run["outer_folds"])
        cov = coverage_report(scheduled=count + 504, insufficient_history=504, valid_feature=count, invalid_outcome=0, quality_errors=0)
        reports[market] = evaluate_outer_predictions(dataset, run, upside_ticks=np.array(upside), downside_ticks=np.array(downside),
            tick_values_usd=tick_values, quartiles=np.array(quartiles), decision_ns=np.array(decisions, dtype=np.int64), coverage=cov)
        construction[market] = source_counts
        run_summaries[market] = {"run_id": run["run_id"], "outer_folds": len(run["outer_folds"]), "observations": n,
                                 "selected_candidates": {name: [f["streams"][name]["candidate_id"] for f in run["outer_folds"]]
                                                         for name in ("combined", "ridge_only")},
                                 "candidate_count_per_fold": 50,
                                 "prediction_digest": sha256_json([f["streams"] for f in run["outer_folds"]]),
                                 "result_id": reports[market]["result_id"]}
    burden = trial_burden(649, [])
    qualification = qualify_markets(reports, burden=burden, dsr_censuses={})
    if qualification["phase_12_eligible_markets"]:
        raise IntegrityError("missing historical census must never qualify")
    failures = {**reports, "ES": terminal_failure("ES", reason="injected economic failure")}
    independent = qualify_markets(failures, burden=burden, dsr_censuses={})
    downstream = downstream_eligibility(independent, {})
    if downstream["refit_without_evaluation"]:
        raise IntegrityError("failed qualification must not fall back to Ridge")
    core = {"schema_version": "fixed_hour_strategy_discovery_synthetic/1.0.0", "mechanism_id": design()["mechanism_id"],
            "state": "PASSED_ENGINEERING_ONLY", "construction": construction, "market_runs": run_summaries,
            "qualification": qualification, "independent_failure_check": independent, "downstream": downstream,
            "dsr_evidence": "MISSING_NOT_IMPUTED_FAILS_CLOSED", "authority": AUTHORITY}
    root = Path(__file__).resolve().parents[2]
    core["implementation_files"] = {"src/futures_rebuild/" + name: sha256_file(root / "src/futures_rebuild" / name) for name in IMPLEMENTATION}
    core["test_files"] = {"tests/test_fixed_hour_strategy_discovery.py": sha256_file(root / "tests/test_fixed_hour_strategy_discovery.py")}
    return {**core, "certificate_id": sha256_json(core)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true", help="Write new inactive contracts and synthetic evidence only")
    args = parser.parse_args()
    if not args.prepare:
        parser.error("Explicit --prepare is required; no historical execution option exists")
    root = Path(__file__).resolve().parents[2]
    before = build_bundle(root)
    certificate = synthetic_certificate()
    validate_bundle(before, root)
    certificate_path = EVIDENCE / certificate["certificate_id"] / "certificate.json"
    write_new_canonical_authority(root / certificate_path, certificate)
    bound = {**before, "synthetic_certificate": {"path": certificate_path.as_posix(), "sha256": sha256_file(root / certificate_path), "certificate_id": certificate["certificate_id"]}}
    bound.pop("bundle_id")
    bound["bundle_id"] = sha256_json(bound)
    write_new_canonical_authority(root / CONFIG, bound)
    print(json.dumps({"state": "IMPLEMENTED_AND_SYNTHETICALLY_VALIDATED_INACTIVE", "config": CONFIG.as_posix(),
                      "mechanism_id": bound["mechanism_id"], "certificate_id": certificate["certificate_id"],
                      "trial_burden": bound["trial_ledger"]["burden"]["combined_burden"],
                      "readiness": bound["readiness"]}, sort_keys=True))


if __name__ == "__main__":
    main()
