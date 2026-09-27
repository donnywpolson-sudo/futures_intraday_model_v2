"""Tiny generated accounting fixtures for L0 reports; no model, replay or data read."""
import argparse
import json
from pathlib import Path

from .errors import IntegrityError
from .l0_alpha_reporting import PNL_BASIS, candidate_report, load_catalog, write_catalog


def fixture_context(name="candidate"):
    dates = ["2018-01-02", "2018-01-03", "2018-01-04", "2018-01-05", "2018-01-08", "2018-01-09", "2018-01-10", "2018-01-11"]
    return {"candidate_id": name, "experiment_id": "SYNTHETIC_REPORT_FIXTURE_NOT_REGISTERABLE",
            "run_id": "GENERATED_REPORT_DEMO", "attempt_id": name, "hypothesis": "Generated arithmetic only; no market hypothesis",
            "intended_change": "Illustrative model change", "parent_candidate_id": "incumbent" if name == "candidate" else None,
            "started_at_utc": "2023-01-02T18:00:00Z", "finished_at_utc": "2023-01-02T18:00:01Z",
            "provenance": {"data_identity": "HANDWRITTEN_SYNTHETIC_ACCOUNTING;NO_MARKET_ROWS_OR_FIT", "code_identity": None},
            "lineage": {"frozen_at_utc": "2017-12-01T00:00:00Z", "first_result_exposure_utc": "2023-01-02T18:00:01Z",
                        "exposure_history_complete": True, "informed_by_results": [], "declared_family": ["incumbent", "candidate"],
                        "search_space": ["illustrative A", "illustrative B"], "attempted_settings": name,
                        "selection_metric": "NONE_SYNTHETIC_REPORT_ONLY", "decision_rule": "NO_RESEARCH_DECISION"},
            "declared_benchmarks": ["no_trade"], "intended_policy_changes": ["model"],
            "policy": {"model": {"fixture": name}, "features": ["FIXTURE_ONLY"], "selection": "NONE_SYNTHETIC",
                       "refitting": "NONE_SYNTHETIC", "entry": {"rule": "PRECOMPUTED_FIXTURE"}, "exit": "PRECOMPUTED_FIXTURE",
                       "sizing": "ONE_STANDARD_CONTRACT", "opportunity_set": "FIXTURE_DATES", "execution": "REFERENCE_NOT_FILL",
                       "costs": "DECLARED_SYNTHETIC_ALLOWANCE"},
            "scope": {"dates": dates, "instruments": ["ES"], "opportunity_set": "FIXTURE_DATES", "calendar_id": "SYNTHETIC_WEEKDAY_CALENDAR",
                      "session_boundaries": {"start": "06:30:05", "end": "13:00:00"}, "timezone": "America/Los_Angeles",
                      "cutoff_convention": "START_INCLUSIVE_END_EXCLUSIVE", "evaluation_stage": "OUTER_CHRONOLOGICAL",
                      "pnl_basis": PNL_BASIS, "observation_cutoff_utc": "2023-01-01T00:00:00Z", "execution_assumptions": "REFERENCE_PRINT_NOT_FILL",
                      "cost_assumptions": {"basis": "ASSUMED_REFERENCE_ALLOWANCES", "round_trip_fee_usd": 10,
                                           "round_trip_allowances_ticks": {"base": 3, "adverse": 6, "severe": 12}, "scenario": "adverse"},
                      "sizing_basis": "ONE_STANDARD_CONTRACT", "fold_by_date": {d: "fold_" + str(i // 4) for i, d in enumerate(dates)},
                      "segment_by_date": {d: "declared_regime_" + str(i // 4) for i, d in enumerate(dates)},
                      "data_kind": "SYNTHETIC_REPORT_FIXTURE_NOT_RESEARCH_EVIDENCE"}}


def fixture_replay(values, *, dates=None):
    """Accounting inputs, not a trading/P&L simulation; explicit flat zeros."""
    dates = dates or fixture_context()["scope"]["dates"]
    return {"complete": True, "policy": "GENERATED_REFERENCE_ACCOUNTING", "unresolved_position": None,
            "daily": {day: {"gross_usd": float(value) + (1 if value else 0), "cost_usd": float(bool(value)),
                             "net_usd": float(value), "trades": int(bool(value)), "complete": True}
                      for day, value in zip(dates, values, strict=True)}}


def fixture_inference():
    return {"declaration_id": "SYNTHETIC_DEMO_ONLY_NOT_A_RESEARCH_RULE", "declared_at_utc": "2017-12-01T00:00:00Z",
            "estimand": "MEAN_DAILY_NET_DIFFERENCE_USD", "eligible_stages": ["OUTER_CHRONOLOGICAL"],
            "minimum_dates": 8, "minimum_segment_dates": 4, "mean_block_length": 2,
            "sensitivity_block_lengths": [3], "replicates": 64, "seed": 617,
            "confidence_level": .9, "interval_method": "PERCENTILE", "block_method": "STATIONARY_WITHIN_FOLD_REGIME",
            "gap_rule": "NO_MISSING_DATES_NO_BOUNDARY_CROSSING", "stationarity_assumption": "ILLUSTRATIVE_WITHIN_EACH_FOUR_DATE_SEGMENT_NOT_CALIBRATION",
            "drawdown_horizon_dates": 8}


def generate(output):
    contexts = {name: fixture_context(name) for name in ("incumbent", "candidate", "no_trade", "incomplete", "failed", "calendar_mismatch")}
    contexts["no_trade"]["benchmark_id"] = "no_trade"
    contexts["incomplete"]["lineage"]["informed_by_results"] = [{"result_id": "GENERATED_PRIOR_RESULT", "use": "illustrative feature revision"}]
    contexts["calendar_mismatch"]["scope"]["calendar_id"] = "DIFFERENT_SYNTHETIC_CALENDAR"
    a = candidate_report(contexts["incumbent"], fixture_replay([-4, 5, 3, 0, 8, -6, -5, -3]))
    b = candidate_report(contexts["candidate"], fixture_replay([-3, 8, 4, 0, 10, -5, -6, -4]))
    zero = candidate_report(contexts["no_trade"], fixture_replay([0] * 8))
    missing = fixture_replay([-3, 8, 4, 0, 10, -5, -6, -4])
    missing["daily"].pop(contexts["incomplete"]["scope"]["dates"][4])
    incomplete = candidate_report(contexts["incomplete"], missing, execution_status="INCOMPLETE")
    failed = candidate_report(contexts["failed"], execution_status="FAILED", failure_reason="GENERATED_FAILURE_NO_MODEL_WAS_RUN")
    incompatible = candidate_report(contexts["calendar_mismatch"], fixture_replay([-3, 8, 4, 0, 10, -5, -6, -4]))
    reports = [a, b, zero, incomplete, failed, incompatible]
    requests = [{"incumbent_report_id": a["report_id"], "candidate_report_id": b["report_id"], "inference": fixture_inference()},
                {"incumbent_report_id": zero["report_id"], "candidate_report_id": b["report_id"], "role": "benchmark"},
                *[{"incumbent_report_id": a["report_id"], "candidate_report_id": r["report_id"]} for r in (incomplete, incompatible)]]
    catalog = write_catalog(Path(output) / "candidate_reports", reports, requests)
    view = load_catalog(catalog)
    assets = Path(__file__).parent / "live_cockpit" / "assets"
    html = (assets / "index.html").read_text(encoding="utf-8")
    html = html.replace('<link rel="stylesheet" href="styles.css?v=chart-first-2" />', "<style>" + (assets / "styles.css").read_text(encoding="utf-8") + "</style>")
    html = html.replace('<script src="candidate-reports.js"></script>', "<script>window.l0CandidateReportDemo=" + json.dumps(view, sort_keys=True, allow_nan=False).replace("<", "\\u003c") + ";</script><script>" + (assets / "candidate-reports.js").read_text(encoding="utf-8") + "</script>")
    for script in ("lightweight-charts.standalone.production.js", "time-format.js", "app.js?v=chart-first-2"):
        html = html.replace(f'<script src="{script}"></script>', "")
    html = html.replace("<title>Futures Live Cockpit</title>", "<title>L0 synthetic reporting demonstration</title>")
    path = catalog.with_suffix(".html")
    rendered = html.encode("utf-8")
    if path.exists() and path.read_bytes() != rendered:
        raise IntegrityError("synthetic preview changed; use a fresh task output directory")
    if not path.exists():
        with path.open("xb") as stream:
            stream.write(rendered)
    return {"catalog": str(catalog), "markdown": str(catalog.with_suffix(".md")), "cockpit_preview": str(path),
            "scope": "SYNTHETIC_REPORTING_ONLY_NO_FITTING_REPLAY_OR_RESEARCH_REGISTRATION"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Fresh task-owned output directory")
    print(json.dumps(generate(parser.parse_args().output), indent=2))


if __name__ == "__main__":
    main()
