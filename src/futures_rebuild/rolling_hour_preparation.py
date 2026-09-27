"""Freeze a new inactive experiment and exercise it using generated data only."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path

import numpy as np

from . import rolling_hour_research as research
from . import profit_aligned_validation as failed_validation
from .canonical import sha256_file, sha256_json, write_new_canonical_authority
from .errors import IntegrityError
from .exchange_calendar import CalendarSession, TradingStateInterval
from .fixed_hour_globex_pipeline import _synthetic_row
from .profit_aligned_research import bind, cost_policy, identity

CONFIG = Path("configs/rolling_hour_research_v7.json")
EVIDENCE = Path("state/unpublished_evidence/rolling_hour_research_v1")
FAILED_REPORT = Path("state/unpublished_evidence/profit_aligned_research_v1/1e1ec918e520a9aaf77ed38188cae5783cbdbd6587473c4575be771b9ea1fdcd/report.json")
PREDECESSORS = ("configs/fixed_hour_model_comparison_v6.json", "configs/fixed_hour_strategy_discovery_v1.json",
    "configs/active_alpha_research_ladder.json", "configs/active_alpha_historical_research_design.json",
    "configs/active_alpha_ladder_execution_design.json", "configs/source_contract.json",
    "configs/rolling_hour_research_v1.json", "configs/rolling_hour_research_v2.json",
    "configs/fixed_hour_search_accounting_v1.json", "configs/legacy_trial_penalty_decision.json",
    "configs/rolling_hour_research_v3.json", "configs/rolling_hour_research_v4.json",
    "configs/rolling_hour_research_v5.json", "configs/rolling_hour_research_v6.json")


def statistical_block(root):
    """Recompute the saved complete calibration counts; do not rerun or tune it."""
    report = json.loads((root / FAILED_REPORT).read_bytes())
    identity(report, "report_id")
    scenarios, passed = failed_validation.summarize(report["counts"])
    if (report["completed_replications"] != 28000 or report["scenarios"] != scenarios
            or passed or report["statistical_gate_passed"] is not False):
        raise IntegrityError("preserved failed calibration evidence differs")
    return {**research.qualification_status(), "historical_report_path": FAILED_REPORT.as_posix(),
        "historical_report_sha256": sha256_file(root / FAILED_REPORT),
        "recomputed_from_saved_counts_not_new_monte_carlo": True,
        "failed_null_scenarios": [s["scenario"] for s in scenarios if s["null_gate_passed"] is False],
        "required_regression_design": failed_validation.specification(),
        "failure_mechanism": "FINITE_EMPIRICAL_RESAMPLING_CAN_MISS_RARE_LOSSES_AND_MISREPRESENT_DEPENDENCE",
        "replacement_method": None, "replacement_calibration": "NOT_IMPLEMENTED_OR_PASSED"}


def build_bundle(root):
    root = Path(root)
    specification = research.design()
    previous = json.loads((root / PREDECESSORS[1]).read_bytes())
    identity(previous, "bundle_id")
    prior_burden = previous["trial_ledger"]["burden"]
    predecessor = json.loads((root / "configs/rolling_hour_research_v5.json").read_bytes())
    identity(predecessor, "bundle_id")
    if predecessor["specification"]["candidates"] != specification["candidates"]:
        raise IntegrityError("changed candidate family requires separate trial accounting")
    # Changed loss/weights are a new learning procedure. Keep prior charges;
    # rebuilding this same preparation never charges it again.
    configuration_ids = [f"{research.mechanism_id()}/{market}/{c['candidate_id']}"
        for market in research.MARKETS for c in research.candidate_definitions()]
    if len(configuration_ids) != 60 or len(set(configuration_ids)) != 60:
        raise IntegrityError("bounded candidate family drifted")
    implementation = set(failed_validation.implementation_files(root)) | {
        "src/futures_rebuild/rolling_hour_research.py", "src/futures_rebuild/rolling_hour_preparation.py",
        "src/futures_rebuild/rolling_hour_source.py",
        "src/futures_rebuild/pipeline.py", "src/futures_rebuild/profit_aligned_confirmation.py",
        "tests/test_rolling_hour_research.py"}
    return bind({"schema_version": "rolling_hour_preparation/2.0.0", "specification": specification,
        "predecessor_bundle_id": json.loads((root / "configs/rolling_hour_research_v6.json").read_bytes())["bundle_id"],
        "learning_procedure_predecessor_bundle_id": predecessor["bundle_id"],
        "preparation_revision": "V7_DIAGNOSTIC_FAILURE_ISOLATION;SAME_V6_LEARNING_PROCEDURE_AND_CHARGES",
        "state": "PREPARED_INACTIVE_NOT_REGISTERED", "authority": research.AUTHORITY,
        "phase_contracts": {p: bind({"phase": p, "mechanism_id": research.mechanism_id(),
            "rules": specification["phase_" + p] if p != "ladder" else specification["ladder"],
            "state": "INACTIVE_SUCCESSOR", "authority": research.AUTHORITY}, "contract_id")
            for p in ("3", "4", "5", "6", "ladder")},
        "implementation_files": {p: sha256_file(root / p) for p in sorted(implementation)},
        "preserved_files": {p: sha256_file(root / p) for p in PREDECESSORS},
        "runtime_versions": failed_validation.runtime_versions(),
        "accounting": {"preserved_preparation_burden": prior_burden,
            "experiment_identity": research.mechanism_id(),
            "preserved_prior_configuration_ids": predecessor["accounting"]["new_configuration_ids"],
            "change_reason": "DATE_BALANCED_NORMALIZED_FITTING_AND_OBSERVED_DATE_SUPPORT",
            "new_configuration_ids": configuration_ids, "new_configuration_charges": 60,
            "registered": False, "historical_upper_bound_established": False,
            "observed_performance_census": False, "repeated_preparation_is_new_trial": False},
        "statistical_block": statistical_block(root),
        "history_recovery": history_recovery(root),
        "readiness_preparation": readiness_preparation(root),
        "synthetic_acceptance": {"markets": list(research.MARKETS), "dates_per_market": 1026,
            "fixture": "rich_calendar_v1", "seed": 74161,
            "minutes": "360_EVERY_63RD_DATE;120_EVERY_OTHER_127TH;OTHERWISE_125",
            "decision_quotes": "NO_QUOTES_EVERY_17TH_DATE;VARIED_SPREAD_AND_LATE_OR_MISSING_QUOTES",
            "price_regimes": "ALTERNATING_63_DATE_TREND_AND_ZERO_DRIFT_INNOVATIONS",
            "minimum_quartiles": [1, 2, 3, 4], "multiple_entry_date_required": True,
            "empty_audit_date_required": True, "nonconstant_16_candidate_features_required": True,
            "winner_diversity": "MEASURE_WITHOUT_TUNING;LACK_OF_DIVERSITY_IS_AN_EXPLICIT_LIMITATION",
            "outer_folds": 8, "inner_folds": 4, "candidates_per_outer_fold": 15, "alpha_claim": False},
    }, "bundle_id")


def readiness_preparation(root):
    """Read catalog metadata only. No price/quote/sidecar file is opened here."""
    path = Path(root) / "data/causally_gated_normalized/_control/catalog.json"
    catalog = json.loads(path.read_bytes())
    entries = [{k: e[k] for k in ("market", "year", "parquet_path", "parquet_sha256",
        "sidecar_path", "sidecar_sha256", "coverage_start", "coverage_end", "selection_eligible", "row_count")}
        for e in catalog["entries"] if e["market"] in research.MARKETS and 2018 <= e["year"] <= 2022
        and e["disposition"] == "RESEARCH_READY_CAUSAL_PRICE"]
    accounting = json.loads((Path(root) / "configs/fixed_hour_search_accounting_v1.json").read_bytes())
    expected = {(m, y) for m in research.MARKETS for y in range(2018, 2023)}
    if len(entries) != 20 or {(e["market"], e["year"]) for e in entries} != expected:
        raise IntegrityError("exact four-market five-year source metadata required")
    for entry in entries:
        entry["physical_bytes"] = (Path(root) / entry["parquet_path"]).stat().st_size
    quote_catalogs = []
    for folder in ("annual_bbo_1s_eligibility_catalog_registry", "annual_tbbo_source_catalog_registry",
                   "l1_bbo_1s_eligibility_catalog_registry", "tbbo_eligibility_catalog_registry"):
        for catalog_path in sorted((Path(root) / "state" / folder).glob("*/catalog.json")):
            book = json.loads(catalog_path.read_bytes())
            relevant = [e for e in book.get("entries", []) if e.get("market") in research.MARKETS]
            # Preserve source metadata, not quotes, including explicit coverage fields.
            coverage = [{k: e[k] for k in ("market", "trade_date", "coverage", "coverage_start", "coverage_end",
                "source_eligible", "logical_path", "annual_logical_path") if k in e} for e in relevant]
            quote_catalogs.append({"path": catalog_path.relative_to(root).as_posix(),
                "sha256": sha256_file(catalog_path), "entries": coverage})
    return {"state": "PREPARED_METADATA_ONLY_NOT_READY_FOR_ACCESS", "authority": research.AUTHORITY,
        "scope": {"markets": list(research.MARKETS), "years": [2018, 2019, 2020, 2021, 2022]},
        "price_catalog_sha256": sha256_file(path), "declared_price_inputs": entries,
        "price_bytes_verified": False, "quote_inputs": [], "calendar_known_at_evidence": None,
        "inventoried_quote_catalogs": quote_catalogs,
        "quote_disposition": "INVENTORIED_CATALOGS_COVER_2025_2026_NOT_2018_2022;NO_SUBSTITUTION",
        "fee_and_latency_evidence": None, "row_budget": None, "cost_budget": None,
        "accounting_floor_from_existing_owner": accounting["charged_search_floor"],
        "search_upper_bound": accounting["search_count_upper_bound"],
        "historical_disposition": "REMAIN_BLOCKED;NO_SYNTHETIC_SHARPE_OR_INVENTED_UPPER_BOUND",
        "next_action": "RESOLVE_MISSING_BINDINGS_AND_BOUNDED_READ_PLAN_BEFORE_REQUESTING_ACCESS",
        "required_measurements": ["all_declared_dates_intervals_and_folds", "contract_and_roll_identity",
            "bar_quote_definition_calendar_known_at", "quote_age_spread_and_boundary_print_difference",
            "fees_latency_and_stress_cost_sensitivity"],
        "bounded_price_readiness_operation": {
            "state": "PREPARED_REQUIRES_OWNER_APPROVAL_AND_EXISTING_SOURCE_ACCESS_ROUTE",
            "purpose": "PRICE_ONLY_SUPPORT_AND_PROVENANCE_CENSUS;NO_FIT_PREDICT_OR_EVALUATE",
            "exact_sources": entries, "row_limit": sum(e["row_count"] for e in entries),
            "source_bytes": sum(e["physical_bytes"] for e in entries),
            "estimated_runtime_minutes": [10, 60], "hard_stop_minutes": 60,
            "estimated_output_bytes_max": 64 * 1024**2, "working_memory_bytes_max": 2 * 1024**3,
            "provider_cost_usd": 0, "estimate_basis": "METADATA_COUNTS;NOT_A_MEASURED_HISTORICAL_BENCHMARK",
            "output_directory": "state/unpublished_evidence/rolling_hour_research_v1/price_readiness_v6",
            "outputs": ["per_date_observed_support", "missing_minutes_contract_and_availability_exceptions",
                "source_hash_verification", "unresolved_calendar_quote_cost_joins"],
            "preservation": "READ_ONLY_20_PRICE_FILES_AND_THEIR_20_MANIFESTS;NO_SOURCE_MUTATION;NO_2023_PLUS",
            "limitation": "CANNOT_CERTIFY_COMPLETE_PROFIT_SELECTOR_WITHOUT_HISTORICAL_QUOTES_AND_PIT_CALENDAR"},
        "outputs": ["coverage_and_join_exceptions", "clock_and_cost_calibration_summary"],
        "preservation": "READ_ONLY_SOURCES;NO_HOLDOUT_FORWARD_OR_ACTIVE_DATA_MUTATION"}


def history_recovery(root):
    """Recover recorded declarations and dispositions, never invent unseen results."""
    root = Path(root)
    from .fixed_hour_model_comparison_preparation import ledger_snapshot
    ledger = ledger_snapshot(root)
    registrations = []
    for family in ("fixed_hour_ridge", "fixed_hour_multifamily", "es_flow_rescue", "es_micro_exhaustion"):
        for path in sorted((root / "state/trial_registry" / family).glob("*.json")):
            record = json.loads(path.read_bytes())
            registrations.append({"path": path.relative_to(root).as_posix(), "sha256": sha256_file(path),
                "trial_id": record["trial_id"], "family": family,
                "candidate_ids": record.get("candidate_ids", []),
                "status": "DECLARATION_NOT_PROOF_OF_EXECUTION_OR_RESULTS_VIEWED"})
    paths = [
        ("state/trial_registry/fixed_hour_ridge_results/8762645d7b0243451c3b216bcf0acbf1a49ce41a79b0353703d74af616f33c09.json", "OBSERVED_PARTIAL", "ONE_ES_OUTER_FOLD;REMAINING_FOLDS_NOT_RUN"),
        ("state/fixed_hour_multifamily_governed/b5dd8cbc3322d445257bdf1dc5bd13e8407e7f7226614929e23db1bca1f2f24d/mechanical_abort.json", "ABORTED_NO_PERFORMANCE", "NO_ROWS_DECODED"),
        ("state/fixed_hour_multifamily_governed/b828013f32f1ecb03b83bfd68eed87a90284e8113db0c006e42fa94cde5cdd18/es_pilot_result.json", "OBSERVED_PARTIAL", "ES_2018_2020_INNER_RESULTS;NO_OUTER_SCORE_OR_2021_PLUS"),
        ("state/intraday_research_official_results/5923a09a9719cd18d8925d2ba6b3f2a88ce959adcbdf288f926956b4631d2d2a/result.json", "OBSERVED_COMPLETE_SCREEN", "ES_2018_2020;CONDITIONAL_2021_2022_NOT_RUN"),
        ("state/intraday_research_official_results/ad7f318c5f26a4c5f316999c943546c9c1a95dde0f3dfae91e46056b50f24140/result.json", "OBSERVED_COMPLETE_SCREEN", "ES_2018_2020;CONDITIONAL_2021_2022_NOT_RUN")]
    results = [{"path": p, "sha256": sha256_file(root / p), "status": status, "exposed_scope": scope,
        "viewed_this_task": "EXISTING_RESULT_METADATA_ONLY;NO_NEW_OBSERVATIONS"} for p, status, scope in paths]
    return {"state": "PARTIAL_RECOVERY_NOT_COMPLETE_CENSUS", "ledger_snapshot": ledger,
        "registrations": registrations, "results": results,
        "legacy_penalty": json.loads((root / "configs/legacy_trial_penalty_decision.json").read_bytes()),
        "results_viewed_by_all_prior_researchers": "NOT_RECONSTRUCTIBLE_FROM_REGISTRATIONS_ALONE",
        "result_informed_changes": "CLOSED_FAILED_PREDECESSORS_REMAIN_COUNTED;FULL_DECISION_HISTORY_UNRESOLVED",
        "this_successor_change_basis": "USER_REPORTED_SUPPORT_AND_REGULARIZATION_DEFECTS;NO_NEW_MARKET_RESULT_TUNING",
        "historical_search_upper_bound": None, "historical_execution_authorized": False,
        "required_design_decision": "RECOVER_MISSING_RESULTS_AND_DECISION_HISTORY_OR_APPROVE_EXPLICIT_DESCRIPTIVE_REUSE_SUCCESSOR_THROUGH_EXISTING_GOVERNANCE;NO_FRESHNESS_OR_GLOBAL_SIGNIFICANCE_CLAIM"}


def validate_bundle(bundle, root):
    identity(bundle, "bundle_id")
    if bundle != build_bundle(root):
        raise IntegrityError("successor declaration, implementation, or inherited evidence drifted")


def synthetic_policy():
    return cost_policy(source_release_id="a" * 64, clock_policy_id="b" * 64,
        fee_evidence_id="c" * 64, maximum_quote_age_ns=2_000_000_000, verified_round_trip_fee_usd=5.)


def synthetic_session(market="ES", day="2018-01-01", minutes=125):
    """Generated paths and quotes with synthetic economics; no source-file reads."""
    start = datetime.fromisoformat(day).replace(hour=16, tzinfo=timezone.utc)
    start_ns = int(start.timestamp()) * 10**9
    session = CalendarSession(market, date.fromisoformat(day),
        (TradingStateInterval("OPEN", start, start + timedelta(minutes=minutes)),))
    sign = 1 if date.fromisoformat(day).toordinal() % 2 else -1
    step = sign * (2 + date.fromisoformat(day).toordinal() % 3)
    rows = []
    for minute in range(minutes):
        row = _synthetic_row(start_ns + minute * research.MINUTE, minute, market)
        tick = row["min_price_increment_nano"]
        opening = (500 + minute * step + minute % 7) * tick
        closing = opening + step * tick
        row.update(project_trade_date=day, open_nano=opening, close_nano=closing,
            high_nano=max(opening, closing) + tick, low_nano=min(opening, closing) - tick,
            source_row_sha256=sha256_json({"market": market, "day": day, "minute": minute, "step": step}))
        rows.append(row)
    quotes = {}
    for s in research.opportunity_grid(session):
        fields = {}
        for key, t in (("decision_quote", s["decision_ns"]), ("entry_quote", s["entry_ns"]), ("exit_quote", s["exit_ns"])):
            index = (t - start_ns) // research.MINUTE
            tick = rows[index]["min_price_increment_nano"]
            bid = rows[index]["open_nano"]
            fields[key] = {"market": market, "actual_contract": rows[index]["actual_contract"],
                "interval_end_ns": t - 10**9, "available_at_ns": t - 10**9,
                "bid_nano": bid, "ask_nano": bid + tick, "bid_size": 1, "ask_size": 1,
                "source_release_id": "a" * 64, "clock_policy_id": "b" * 64, "quality_valid": True}
        quotes[s["opportunity_id"]] = fields
    return session, rows, quotes


def rich_synthetic_session(market, day, index):
    """Independent quote-clock events and varied prices; no market data reads."""
    minutes = 360 if index % 63 == 0 else 120 if index % 127 == 0 else 125
    session, rows, _ = synthetic_session(market, day, minutes)
    rng = np.random.default_rng(74161 + date.fromisoformat(day).toordinal() + research.MARKETS.index(market) * 10000)
    drift = (2 if index % 2 else -2) if (index // 63) % 2 == 0 else 0
    increments = rng.integers(-3, 4, size=minutes + 1) + drift
    levels = 20000 + np.r_[0, np.cumsum(increments)]
    tick = rows[0]["min_price_increment_nano"]
    for minute, row in enumerate(rows):
        opening, closing = int(levels[minute]) * tick, int(levels[minute + 1]) * tick
        row.update(open_nano=opening, close_nano=closing, high_nano=max(opening, closing) + tick,
            low_nano=min(opening, closing) - tick, volume=int(rng.integers(1, 200)),
            source_row_sha256=sha256_json({"rich_fixture": [market, day, minute, opening, closing]}))
    start = rows[0]["bar_start_ns"]
    quotes = {}
    # Quotes are observed a second before each requested time. At an entry
    # boundary they use the preceding minute's level, never the later bar open.
    for number, s in enumerate(research.opportunity_grid(session)):
        fields = {}
        for key, at in (("decision_quote", s["decision_ns"]), ("entry_quote", s["entry_ns"]), ("exit_quote", s["exit_ns"])):
            event = at - 10**9
            minute = (event - start) // research.MINUTE
            bid = int(levels[minute]) * tick
            spread = 1 + (minute + index) % 3
            fields[key] = {"market": market, "actual_contract": rows[0]["actual_contract"],
                "interval_end_ns": event, "available_at_ns": event, "bid_nano": bid,
                "ask_nano": bid + spread * tick, "bid_size": 1, "ask_size": 1,
                "source_release_id": "a" * 64, "clock_policy_id": "b" * 64, "quality_valid": True}
        if index % 17 == 0:
            fields["decision_quote"] = None
        elif number == 0:
            fields["entry_quote"] = None
        elif number == 1:
            fields["decision_quote"]["available_at_ns"] = s["decision_ns"] + 1
        quotes[s["opportunity_id"]] = fields
    return session, rows, quotes


def synthetic_dataset(market="ES", n=1026, *, rich=False):
    batches, sessions = [], []
    for i in range(n):
        day = (date(2018, 1, 1) + timedelta(days=i)).isoformat()
        session, rows, quotes = rich_synthetic_session(market, day, i) if rich else synthetic_session(market, day)
        sessions.append(session)
        batches.append(research.assemble_session(session, rows, as_of_ns=rows[-1]["decision_eligible_at_ns"], quotes=quotes))
    return research.RollingDataset(tuple(batches), synthetic_policy(), tuple(sessions))


def run_synthetic(root):
    root = Path(root)
    bundle = json.loads((root / CONFIG).read_bytes())
    validate_bundle(bundle, root)
    markets = {}
    output = root / EVIDENCE / bundle["bundle_id"]
    for market in research.MARKETS:
        dataset = synthetic_dataset(market, rich=True)
        try:
            run = research.run_market_wfa(dataset)
            result = research.evaluate_wfa(dataset, run)
        except research.comparison.LeafSupportError as exc:
            markets[market] = {"state": "BLOCKED_SELECTED_OUTER_REFIT", "reason": str(exc),
                "evidence_id": dataset._evidence_id, "qualification": research.qualification_status(),
                "partial_run": getattr(exc, "partial_run", None)}
            print(json.dumps({"market": market, "state": "BLOCKED_SELECTED_OUTER_REFIT", "reason": str(exc)}), flush=True)
            continue
        # Preserve complete candidates, fold models, predictions and benchmark results.
        write_new_canonical_authority(output / market / "run.json", run)
        write_new_canonical_authority(output / market / "evaluation.json", result)
        try:
            final = research.final_refit(dataset, run)
            final_path = output / market / "final_model.json"
            write_new_canonical_authority(final_path, final)
            loaded = json.loads(final_path.read_bytes())
            identity(loaded, "final_id")
            np.testing.assert_array_equal(research.predict(loaded["model"], dataset.terminal_data.features),
                research.predict(final["model"], dataset.terminal_data.features))
            final_status = {"state": "SYNTHETIC_MODEL_SAVED_RELOADED", "path": final_path.relative_to(root).as_posix(),
                "sha256": sha256_file(final_path), "final_id": final["final_id"]}
            write_new_canonical_authority(output / market / "prediction_check.json", bind({
                "final_id": final["final_id"], "feature_names": list(research.terminal.FEATURE_NAMES),
                "features": dataset.terminal_data.features.tolist(),
                "predictions": research.predict(final["model"], dataset.terminal_data.features).tolist(),
                "scope": "ALL_GENERATED_ROWS_ONLY"}, "check_id"))
        except research.comparison.LeafSupportError as exc:
            final_status = {"state": "BLOCKED_FINAL_REFIT", "reason": str(exc), "replacement": False}
        if len(run["outer_folds"]) != 8 or any(len(f["candidate_scores"]) != 15 for f in run["outer_folds"]):
            raise IntegrityError("synthetic experiment incomplete")
        selected = result["reports"]["selected"]["numerical_kernel_ledger"]
        quartiles = sorted(set(r["quartile"] for r in dataset.opportunities))
        empty_dates = [b["trade_date"] for b in dataset.batches if not b["records"]]
        empty_audit = sorted(set(empty_dates) & set(selected["dates"]))
        variable = {name: bool(np.ptp(dataset.terminal_data.features[:, research.terminal.FEATURE_NAMES.index(name)]) > 0)
            for p in research.PACKS for name in research.terminal.PACKS[p]}
        if (quartiles != [1, 2, 3, 4] or max(selected["daily_trade_counts"].values()) < 2
                or not empty_audit or not all(variable.values())
                or any(selected["daily_usd"][d]["stress"] != "0" for d in empty_audit)):
            raise IntegrityError("rich synthetic branch/calendar/feature coverage incomplete")
        markets[market] = {"run_id": run["run_id"], "result_id": result["result_id"],
            "final_refit": final_status,
            "session_count": len(dataset.batches), "decision_count": len(dataset.opportunities),
            "outer_folds": 8, "candidates_per_outer_fold": 15,
            "selected_executed_trades": result["reports"]["selected"]["numerical_kernel_ledger"]["executed_trades"],
            "flat_executed_trades": result["reports"]["flat_no_trade"]["numerical_kernel_ledger"]["executed_trades"],
            "matched_comparisons": result["matched_comparisons"], "qualification": result["qualification"],
            "coverage": {"quartiles": quartiles, "variable_candidate_features": variable,
                "empty_audit_dates": empty_audit, "maximum_trades_per_date": max(selected["daily_trade_counts"].values()),
                "selected_candidates": sorted(set(f["streams"]["selected"]["model"]["kernel_model"]["candidate"]["candidate_id"]
                    for f in run["outer_folds"]))}, "diagnostics": result["diagnostics"],
            "selected_daily_ledger": selected,
            "incomplete_diagnostics": {name: result["reports"][name] for name in research.PACKS
                if result["reports"][name]["mean_daily_stress_usd"] is None}}
        print(json.dumps({"market": market, "state": "SYNTHETIC_MARKET_COMPLETED"}), flush=True)
    state = "PASSED_ENGINEERING_ONLY"
    if any(m.get("incomplete_diagnostics") or m.get("diagnostics", {}).get("state") == "INCOMPLETE_FORECAST_DIAGNOSTICS"
            for m in markets.values()):
        state = "PASSED_POLICY_ENGINEERING_DIAGNOSTICS_INCOMPLETE"
    if any(m.get("state") == "BLOCKED_SELECTED_OUTER_REFIT" for m in markets.values()):
        state = "BLOCKED_SYNTHETIC_SELECTED_REFIT_FAILURE"
    if any(m.get("final_refit", {}).get("state") == "BLOCKED_FINAL_REFIT" for m in markets.values()):
        state = "BLOCKED_SYNTHETIC_FINAL_REFIT_FAILURE"
    report = bind({"bundle_id": bundle["bundle_id"], "implementation_files": bundle["implementation_files"],
        "state": state, "markets": markets,
        "statistical_calibration_passed": False, "qualification_ready": False, "activation_ready": False,
        "authority": research.AUTHORITY}, "report_id")
    validate_bundle(bundle, root)
    write_new_canonical_authority(root / EVIDENCE / bundle["bundle_id"] / "report.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--synthetic", action="store_true", help="run all four markets using generated bars and quotes")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    if args.synthetic:
        report = run_synthetic(root)
        print(json.dumps({k: report[k] for k in ("report_id", "state", "qualification_ready")}))
    else:
        bundle = build_bundle(root)
        write_new_canonical_authority(root / CONFIG, bundle)
        print(json.dumps({"bundle_id": bundle["bundle_id"], "state": bundle["state"]}))


if __name__ == "__main__":
    main()
