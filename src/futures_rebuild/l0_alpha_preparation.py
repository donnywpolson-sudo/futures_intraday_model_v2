"""Prepare-only historical interface and resumable generated-data acceptance."""
import argparse
import gc
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from . import l0_alpha_candidate as c
from . import l0_alpha_research as research
from .canonical import (canonical_bytes, read_canonical_authority, sha256_file, sha256_json,
                        write_new_canonical_authority as write)
from .errors import IntegrityError, UnauthorizedOperation
from .locking import FileLease

CONFIG = "configs/l0_alpha_candidate_v9.json"
CALENDAR_PATH = "state/unpublished_evidence/l0_order_readiness_v1/calendar_remediation_v4/calendar_candidate.json"
EVIDENCE = "state/unpublished_evidence/l0_alpha_candidate_v1"
FILES = tuple("src/futures_rebuild/" + name + ".py" for name in (
    "l0_alpha_candidate", "l0_alpha_replay", "l0_alpha_research", "l0_alpha_execution_challenge",
    "l0_alpha_preparation", "l0_alpha_source_readiness", "l0_alpha_calendar", "l0_order_readiness",
    "cme_calendar_source_adequacy", "cme_calendar_successor", "rolling_hour_research", "fixed_hour_model_comparison",
    "fixed_hour_terminal_successor", "fixed_hour_ridge_audit_successor", "canonical", "locking",
    "research/splits", "research/contracts", "certified_research_gateway", "active_data_view",
    "l0_development_admission", "preexecution_fold_certification",
    "causal_observation_parquet", "boundary", "research_gateway_policy")) + (
    "tests/test_l0_alpha_candidate.py", "tests/test_l0_alpha_research.py", "tests/test_l0_alpha_execution_challenge.py",
    "tests/test_l0_alpha_source_readiness.py", "tests/test_l0_order_readiness.py",
    "tests/test_l0_development_admission.py", "tests/test_l0_adopted_successor.py")
FILES += ('src/futures_rebuild/__init__.py', 'src/futures_rebuild/errors.py', 'src/futures_rebuild/l0_alpha_report_store.py', 'src/futures_rebuild/l0_alpha_reporting.py', 'src/futures_rebuild/research/__init__.py', 'src/futures_rebuild/research/abstention.py', 'src/futures_rebuild/research/bootstrap.py', 'src/futures_rebuild/research/controls.py', 'src/futures_rebuild/research/cscv.py', 'src/futures_rebuild/research/dsr.py', 'src/futures_rebuild/research/economics.py', 'src/futures_rebuild/research/feature_foundation.py', 'src/futures_rebuild/research/hac.py', 'src/futures_rebuild/research/interval_identity.py', 'src/futures_rebuild/research/multiple_testing.py', 'src/futures_rebuild/research/power.py', 'src/futures_rebuild/research/robustness.py', 'src/futures_rebuild/research/scope_inference.py', 'src/futures_rebuild/research/sleeves.py', 'tests/test_l0_alpha_reporting.py', 'tests/test_l0_reporting_successor.py')
FILES += ('tests/test_l0_reporting_successor_v2.py',)
AS_OF_NS = int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()) * 10**9
GIB = 2**30


def metadata_inventory(root):
    """Catalog JSON only. No DBN, Parquet, quote values or historical results."""
    from .l1_execution_bbo_1s_source import CATALOG_PATH, CATALOG_SHA256
    paths = {"BBO-1s": (CATALOG_PATH, CATALOG_SHA256), "TBBO": (
        Path("state/tbbo_eligibility_catalog_registry/8861c8953391684531600967f409674b3b78f7c2e8f22ef7ec422bd512a54a36/catalog.json"), None)}
    result = {}
    for schema, (relative, expected_sha) in paths.items():
        path = root / relative
        if not path.is_file():
            result[schema] = {"status": "CATALOG_UNAVAILABLE"}
            continue
        catalog = read_canonical_authority(path, expected_sha256=expected_sha, identity_field="catalog_id")
        markets = {}
        for entry in catalog["entries"]:
            if entry["market"] not in c.MARKETS:
                continue
            if schema == "TBBO":
                markets.setdefault(entry["market"], {"inventory_intervals": []})["inventory_intervals"].append({
                    "start_inclusive": entry["coverage_start"], "end_exclusive": entry["coverage_end"],
                    "mandatory_excluded_utc_dates": entry["mandatory_excluded_utc_dates"],
                    "source_eligible": entry["source_eligible"], "daily_value_coverage_verified": False})
                continue
            m = markets.setdefault(entry["market"], {"inventory_dates": [], "source_eligible_dates": []})
            m["inventory_dates"].append(entry["trade_date"])
            if entry.get("source_eligible") is True:
                m["source_eligible_dates"].append(entry["trade_date"])
        result[schema] = {"catalog_path": relative.as_posix(), "catalog_sha256": sha256_file(path),
            "markets": markets if schema == "TBBO" else {m: {key: sorted(set(value)) for key, value in record.items()} for m, record in markets.items()},
            "status": "CATALOG_METADATA_ONLY;PAYLOAD_NOT_OPENED;NO_ACCESS_AUTHORITY"}
    return result


def build_bundle(root):
    from .l0_alpha_source_readiness import prepare as source_preparation
    calendar = read_canonical_authority(root / CALENDAR_PATH, identity_field="calendar_id")
    predecessor_path = "configs/rolling_hour_research_v9.json"
    previous = json.loads((root / predecessor_path).read_bytes())
    # Bind historical search accounting without treating the predecessor as authority.
    c.identity(previous, "bundle_id")
    return c.bind({"schema_version": "l0_alpha_preparation/1", "design": c.design(),
        "entry_calendar": {"path": CALENDAR_PATH, "sha256": sha256_file(root / CALENDAR_PATH), "calendar_id": calendar["calendar_id"], "research_view": "evidence_only"},
        "implementation_files": {name: sha256_file(root / name) for name in FILES},
        "runtime_versions": c.old.comparison.VERSIONS,
        "prior_preparation": {"path": "configs/l0_alpha_candidate_v8.json",
                              "sha256": sha256_file(root / "configs/l0_alpha_candidate_v8.json"),
                              "same_experiment_no_additional_configuration_charge": True,
                              "revision": "RETROSPECTIVE_ACCESS_POLICY_BINDING_SUCCESSOR;L0_SCIENTIFIC_DESIGN_UNCHANGED"},
        "development_admission": {"contract_path": "configs/l0_development_admission_v4.json",
                                  "state": "INACTIVE_REQUIRES_SEPARATE_ACTIVATION_AND_EVIDENCE",
                                  "economic_completeness": "NOT_EVALUATED"},
        "predecessor": {"path": predecessor_path, "sha256": sha256_file(root / predecessor_path),
                        "bundle_id": previous["bundle_id"], "accounting": previous["accounting"],
                        "preserved_accounting": previous["preserved_accounting"], "historical_use": previous["historical_use"]},
        "configuration_ids": [f"{c.mechanism_id()}/{m}/{v['candidate_id']}" for m in c.MARKETS for v in c.candidate_definitions()],
        "configuration_charges": 40, "registered": False, "repeat_preparation_is_new_campaign": False,
        "historical_search_upper_bound_established": False, "l1_inventory": metadata_inventory(root),
        "historical_readiness": {
            "status": "NOT_CERTIFIED_OR_AUTHORIZED", "source_validity": "REQUIRES_EXISTING_TRUSTED_RELEASE_VALIDATION",
            "availability": "DECLARED_ASSUMPTION_NOT_SOURCE_CERTIFICATE_PASS",
            "mapping": "CONDITIONAL_ON_SUPPLIED_SEQUENCE_UNLESS_TIMING_VERIFIED",
            "fee_evidence": "APPLICABLE_STANDARD_CONTRACT_FEE_EVIDENCE_UNRESOLVED",
            "actual_fold_support": "NOT_MEASURED;REQUIRES_APPROVED_ROW_READ",
            "registration_and_execution": "EXISTING_CERTIFIED_RESEARCH_GATEWAY_ONLY;CURRENT_GATES_PRESERVED"},
        "historical_readiness_proposal": source_preparation(root),
        "confirmation": c.design()["confirmation"], "state": "PREPARED_INACTIVE_NOT_REGISTERED",
        "authority": c.old.AUTHORITY}, "bundle_id")


def validate_bundle(root):
    bundle = read_canonical_authority(root / CONFIG, identity_field="bundle_id")
    if bundle != build_bundle(root):
        raise IntegrityError("implementation, design or metadata changed since preparation")
    return bundle


class ResearchStore:
    """Existing create-only JSON and file lease; no replacement of interrupted work."""
    def __init__(self, root, context, *, maximum_bytes=8 * GIB):
        self.root = Path(root).resolve()
        self.context = context
        self.maximum_bytes = maximum_bytes
        self.root.mkdir(parents=True, exist_ok=True)
        self.bytes = sum(p.stat().st_size for p in self.root.rglob("*.json"))
        self.outputs = None

    def path(self, name):
        path = (self.root / (name + ".json")).resolve()
        if not path.is_relative_to(self.root):
            raise IntegrityError("checkpoint path escapes task output")
        return path

    def save(self, name, value):
        path = self.path(name)
        size = len(canonical_bytes(value)) + 1
        if not path.exists():
            if self.bytes + size > self.maximum_bytes or shutil.disk_usage(self.root).free < size + 256 * 2**20:
                raise IntegrityError("bounded output budget or free-space reserve exceeded")
            self.bytes += size
        write(path, value)
        if self.outputs is not None:
            self.outputs[name] = sha256_file(path)

    def checkpoint(self, name, produce):
        completed = self.path(name + "/completed")
        if completed.exists():
            result = read_canonical_authority(completed, identity_field="checkpoint_id")
            if result["context"] != self.context or result["operation"] != name:
                raise IntegrityError("checkpoint belongs to different data, design, implementation or operation")
            for key, digest in result["outputs"].items():
                if sha256_file(self.path(key)) != digest:
                    raise IntegrityError("completed checkpoint artifact changed")
            return result["result"]
        if self.path(name + "/started").exists():
            raise IntegrityError("interrupted or failed checkpoint is sealed; no duplicate research execution")
        self.save(name + "/started", {"context": self.context, "state": "CLAIMED_ONCE"})
        self.outputs = {}
        try:
            value = produce()
            outputs = dict(self.outputs)
            self.outputs = None
            self.save(name + "/completed", c.bind({"context": self.context, "operation": name,
                                                 "result": value, "outputs": outputs}, "checkpoint_id"))
            return value
        except Exception as exc:
            self.outputs = None
            self.save(name + "/failure", {"error": f"{type(exc).__name__}: {exc}",
                "partial_run": getattr(exc, "partial_run", None), "state": "SEALED_FAILURE_NO_AUTOMATIC_RETRY"})
            raise


def synthetic_day(market, day, index=0, *, dense=False):
    from .fixed_hour_globex_pipeline import _synthetic_row
    first = c.decision_times(day)[0] - c.DELAY - 61 * c.MINUTE
    tick, multiplier = {"ES": (250_000_000, 50), "NQ": (250_000_000, 20),
                        "CL": (10_000_000, 1000), "GC": (100_000_000, 100)}[market]
    rng = np.random.default_rng(62817 + index)
    step = (2 + index % 3) * (1 if index % 2 else -1)
    increments = rng.integers(-1, 2, size=453) + step
    levels = 20_000 + np.r_[0, np.cumsum(increments)]
    minutes = range(452) if dense else [*range(61), 62, 122]
    rows = []
    for minute in minutes:
        row = _synthetic_row(first + minute * c.MINUTE, minute, market)
        opening, closing = int(levels[minute]) * tick, int(levels[minute + 1]) * tick
        row.update(project_trade_date=day, min_price_increment_nano=tick, multiplier_nano=multiplier * 10**9,
            definition_received_at_ns=first - c.MINUTE, open_nano=opening, close_nano=closing,
            high_nano=max(opening, closing) + tick, low_nano=min(opening, closing) - tick,
            volume=int(rng.integers(1, 200)), source_row_sha256=sha256_json([market, day, minute, opening, closing]))
        rows.append(row)
    return day, rows, []


def synthetic_dataset(market, *, dense=False):
    return c.assemble_dataset(market, (synthetic_day(market, day, i, dense=dense) for i, day in enumerate(c.weekday_dates())), as_of_ns=AS_OF_NS)


def saved_dataset(dataset):
    """Lossless ledger: fixed clock plus status runs and complete eligible records."""
    statuses, eligible = [], []
    for i, (r, o) in enumerate(zip(dataset.decisions, dataset.outcomes, strict=True)):
        if r["status"] == "ELIGIBLE":
            eligible.append({"index": i, "decision": r, "features": dataset.features[i].tolist(), "outcome": o})
        else:
            state = {"status": r["status"], "coverage": r.get("coverage")}
            if statuses and statuses[-1]["stop"] == i and statuses[-1]["state"] == state:
                statuses[-1]["stop"] = i + 1
            else:
                statuses.append({"start": i, "stop": i + 1, "state": state})
    return {"evidence_id": dataset._evidence_id, "mechanism_id": c.mechanism_id(), "market": dataset.market,
            "dates": list(dataset.calendar_dates), "as_of_ns": dataset.as_of_ns,
            "source_facts": list(dataset.source_facts), "eligible": eligible, "abstention_runs": statuses,
            "clock": "ALL_330_FIXED_MINUTE_DECISIONS_PER_INDEPENDENT_WEEKDAY"}


def run_dataset(dataset, store, *, calendar=None, calendar_view=None, legacy=False):
    store.save("source_ledger", saved_dataset(dataset))
    if store.path("run").exists():
        run = read_canonical_authority(store.path("run"), identity_field="run_id")
        if run["evidence_id"] != dataset._evidence_id:
            raise IntegrityError("saved run belongs to different source evidence")
        for completed in store.root.rglob("completed.json"):
            name = completed.parent.relative_to(store.root).as_posix()
            store.checkpoint(name, lambda: None)
    else:
        run = research.run_market_wfa(dataset, checkpoint=store.checkpoint, save=store.save,
                                     progress=lambda message: print(message, flush=True), calendar=calendar, calendar_view=calendar_view, legacy=legacy)
        store.save("run", run)
    def finish():
        try:
            final = research.final_refit(dataset, run)
        except c.old.comparison.LeafSupportError as exc:
            store.save("rejected_final_model", exc.fitted_model)
            raise
        store.save("final_model", final)
        reloaded = read_canonical_authority(store.path("final_model"), identity_field="final_id")
        x = dataset.features[[i for i, r in enumerate(dataset.decisions) if r["status"] == "ELIGIBLE"]]
        expected = research.predict(final["model"], x)
        np.testing.assert_array_equal(expected, research.predict(reloaded["model"], x))
        store.save("final_predictions", {"final_id": final["final_id"], "predictions": expected.tolist(),
            "scope": "RELOAD_VERIFICATION_ONLY;NO_NEW_PERFORMANCE_EVALUATION"})
        return {"evaluation": research.evaluate_wfa(run), "final_id": final["final_id"], "policy": final["policy"],
                "reload_predictions": "EXACT", "outer_folds": len(run["outer_folds"])}
    return store.checkpoint("final_refit_and_reload", finish)


def run_synthetic(root):
    bundle = validate_bundle(root)
    output = root / EVIDENCE / bundle["bundle_id"] / "synthetic"
    output.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output).free < 8 * GIB:
        raise IntegrityError("generated campaign requires its bounded 8 GiB output reserve")
    from .rolling_hour_price_census import _working_set
    with FileLease(output / "writer.lock"):
        if (output / "report.json").exists():
            report = read_canonical_authority(output / "report.json")
            if report["bundle_id"] != bundle["bundle_id"]:
                raise IntegrityError("saved synthetic campaign binding differs")
            for name, digest in report["artifact_sha256s"].items():
                if sha256_file(output / name) != digest:
                    raise IntegrityError("saved synthetic campaign artifact changed")
            return report
        campaign = {"bundle_id": bundle["bundle_id"], "configuration_ids": bundle["configuration_ids"],
                    "data": "GENERATED_ONLY", "state": "DECLARED_BEFORE_FITTING", "historical_trial_registered": False}
        write(output / "campaign.json", campaign)
        results, started = {}, time.perf_counter()
        for market in c.MARKETS:
            print(f"{market}: construct all {len(c.weekday_dates())} weekday decision grids", flush=True)
            dataset = synthetic_dataset(market)
            store = ResearchStore(output / market, {"bundle_id": bundle["bundle_id"], "evidence_id": dataset._evidence_id}, maximum_bytes=2 * GIB)
            try:
                result = run_dataset(dataset, store, legacy=True)
                results[market] = {"status": "SYNTHETIC_PASSED", **result, "decision_count": len(dataset.decisions),
                                   "forecast_count": sum(r["status"] == "ELIGIBLE" for r in dataset.decisions), "output_bytes": store.bytes}
            except Exception as exc:
                results[market] = {"status": "SYNTHETIC_FAILED", "error": f"{type(exc).__name__}: {exc}"}
            print(f"{market}: {results[market]['status']}", flush=True)
            del dataset, store
            gc.collect()
        report = {"bundle_id": bundle["bundle_id"], "markets": results,
            "status": "SYNTHETIC_PASSED" if all(r["status"] == "SYNTHETIC_PASSED" for r in results.values()) else "SYNTHETIC_FAILED",
            "historical_data_read": False, "alpha_established": False, "peak_working_set_bytes": _working_set(),
            "elapsed_seconds": time.perf_counter() - started,
            "acceptance_scope": "COMPLETE_DATE_CLOCK_AND_NESTED_SEARCH;SPARSE_GENERATED_FITTING_SUPPORT",
            "historical_fold_support": "NOT_MEASURED", "confirmation": c.design()["confirmation"],
            "artifact_sha256s": {path.relative_to(output).as_posix(): sha256_file(path) for path in output.rglob("*.json")}}
        write(output / "report.json", report)
        return report


def run_rehearsal(root):
    """One dense 2018-2022 market, full clock, largest fit, saved ledgers/reload.

    This measures engineering resource use; the sparse four-market acceptance
    separately exercises every nested candidate and outer period.
    """
    from .rolling_hour_price_census import _working_set
    bundle = validate_bundle(root)
    output = root / EVIDENCE / bundle["bundle_id"] / "dense_rehearsal"
    output.mkdir(parents=True, exist_ok=True)
    with FileLease(output / "writer.lock"):
        report_path = output / "report.json"
        if report_path.exists():
            report = read_canonical_authority(report_path)
            for name, digest in report["artifact_sha256s"].items():
                if sha256_file(output / name) != digest:
                    raise IntegrityError("rehearsal artifact changed")
            return report
        if (output / "started.json").exists():
            raise IntegrityError("interrupted rehearsal remains sealed")
        if shutil.disk_usage(output).free < 4 * GIB:
            raise IntegrityError("dense rehearsal requires bounded 4 GiB output reserve")
        write(output / "started.json", {"bundle_id": bundle["bundle_id"], "data": "GENERATED_ONLY",
            "market": "ES", "maximum_output_bytes": 4 * GIB, "maximum_peak_working_set_bytes": 6 * GIB,
            "retention": "IMMUTABLE_ENGINEERING_EVIDENCE;NO_AUTOMATIC_CLEANUP"})
        started = time.perf_counter()
        print("ES: dense 1305-date, 430650-decision source rehearsal", flush=True)
        dataset = synthetic_dataset("ES", dense=True)
        store = ResearchStore(output, {"bundle_id": bundle["bundle_id"], "evidence_id": dataset._evidence_id}, maximum_bytes=4 * GIB)
        store.save("source_ledger", saved_dataset(dataset))
        split = c.fold_schedule(dataset)[-1]
        checks = {}
        for family in ("ridge", "xgboost"):
            candidate = next(v for v in reversed(c.candidate_definitions()) if v["family"] == family)
            print(f"ES: dense full-period {family} fit and reload", flush=True)
            model = research.fit_model(dataset, np.arange(len(dataset.fit_positions)), candidate, fit_kind="final")
            store.save(family + "_model", model)
            reloaded = read_canonical_authority(store.path(family + "_model"), identity_field="model_id")
            expected = research.predict(model, dataset.features)
            np.testing.assert_array_equal(expected, research.predict(reloaded, dataset.features))
            checks[family] = {"rows": model["training_rows"], "reload": "EXACT"}
        # Fitting above is a resource test, never an out-of-sample score.
        prediction = [100.] * (63 * 330)
        for scenario in c.SCENARIOS:
            result = research.compact_replay(research.replay_window(dataset, split["outer_window"], prediction, scenario=scenario, legacy=True))
            research.require_complete(result)
            store.save("replay_" + scenario, result)
        peak = _working_set()
        report = {"bundle_id": bundle["bundle_id"], "decision_count": len(dataset.decisions),
            "source_rows": sum(v["rows"] for v in dataset.source_facts), "fitting_rows": len(dataset.fit_positions),
            "dates": len(dataset.calendar_dates), "checks": checks, "output_bytes": store.bytes,
            "peak_working_set_bytes": peak, "elapsed_seconds": time.perf_counter() - started,
            "status": "REHEARSAL_PASSED" if peak <= 6 * GIB and store.bytes <= 4 * GIB else "REHEARSAL_FAILED",
            "scope": "ONE_DENSE_MARKET_FULL_CLOCK_FULL_PERIOD_FITS;NOT_FULL_DENSE_NESTED_SEARCH_OR_ALPHA",
            "historical_data_read": False,
            "artifact_sha256s": {path.relative_to(output).as_posix(): sha256_file(path) for path in output.rglob("*.json")}}
        write(report_path, report)
        return report


def execute_registered_market(gateway, *, registration_path, registration_sha256, receipt,
                              market, expected_dataset_id, source_contract_id, source_release_id, reader,
                              reporting_metadata=None, previous_report_catalog=None):
    """Task-only entry after existing strict registration/readiness gates pass.

    No CLI executes history. The reader must be the approved trusted source
    route; a hash match here is additional drift detection, not certification.
    """
    from .certified_research_gateway import CertifiedResearchGateway
    from .boundary import OperationReceipt, OperationClassification
    from .research_gateway_policy import CERTIFIED_TRIAL_EXECUTION_OPERATION
    if type(gateway) is not CertifiedResearchGateway or market not in c.MARKETS:
        raise UnauthorizedOperation("existing certified research gateway and declared market required")
    if type(receipt) is not OperationReceipt:
        raise UnauthorizedOperation("verified operation receipt required")
    bundle = validate_bundle(gateway.root)
    c.old.profit.digest(expected_dataset_id)
    extra = {"l0_bundle_id": bundle["bundle_id"], "l0_dataset_id": expected_dataset_id, "l0_market": market}
    scope = gateway.execution_scope(registration_path=registration_path,
        expected_registration_sha256=registration_sha256, additional_scope=extra)
    if scope["protocol_id"] != c.mechanism_id():
        raise UnauthorizedOperation("registered mechanism differs from this successor")
    from .l0_development_admission import GATEWAY_SCHEMA
    if scope.get("gateway_schema") != GATEWAY_SCHEMA or scope.get("l0_market") != market:
        raise UnauthorizedOperation("L0 requires its exact development admission scope")
    output = gateway.root / EVIDENCE / bundle["bundle_id"] / "historical" / market
    with FileLease(output / "writer.lock"):
        binding = {"registration_sha256": registration_sha256, "receipt_id": receipt.receipt_id,
                   "scope": scope, "source_contract_id": source_contract_id, "source_release_id": source_release_id}
        binding_path = output / "execution_binding.json"
        if binding_path.exists():
            if read_canonical_authority(binding_path) != binding:
                raise UnauthorizedOperation("market campaign belongs to another registered execution")
            receipt.assert_consumed(gateway.boundary, operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
                classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
                required_scope=dict(receipt.scope))
        else:
            # Existing strict gates remain; no caller flag can replace them.
            gateway.claim_historical_execution(registration_path=registration_path,
                expected_registration_sha256=registration_sha256, receipt=receipt, additional_scope=extra)
            write(binding_path, binding)
        from .l0_alpha_report_store import ReportingStore, l0_context, operation_contexts
        metadata = dict(reporting_metadata or {})
        if set(metadata) - {"parent_candidate_id", "hypothesis", "intended_change", "lineage", "intended_policy_changes"}:
            raise IntegrityError("undeclared reporting metadata fields")
        base = l0_context(bundle, market, experiment_id=scope["trial_id"], run_id=sha256_json(binding),
            provenance={"working_file_sha256s": bundle["implementation_files"],
                        "working_code_identity": sha256_json(bundle["implementation_files"]),
                        "basis": "EXACT_WORKING_BYTES_VERIFIED_BY_VALIDATE_BUNDLE;INCLUDES_UNCOMMITTED_CODE"},
            lineage=metadata.pop("lineage", None))
        base.update(metadata)
        base["provenance"].update(data_identity=expected_dataset_id, source_contract_id=source_contract_id,
            source_release_id=source_release_id, registration_sha256=registration_sha256, receipt_id=receipt.receipt_id)
        base["scope"].update(observation_cutoff_utc=datetime.fromtimestamp(AS_OF_NS / 10**9, timezone.utc).isoformat(),
                             data_kind="GOVERNED_L0_DEVELOPMENT")
        store = ReportingStore(output, extra, report_contexts=operation_contexts(bundle, base),
                               runner_context=base, previous_catalog=previous_report_catalog)
        store.observe_run("INCOMPLETE")
        try:
            dataset = c.assemble_dataset(market, reader(receipt), as_of_ns=AS_OF_NS,
                source_contract_id=source_contract_id, source_release_id=source_release_id)
            if dataset._evidence_id != expected_dataset_id:
                raise IntegrityError("approved source-bound dataset changed before fitting")
            calendar = read_canonical_authority(gateway.root / bundle["entry_calendar"]["path"], identity_field="calendar_id")
            result = run_dataset(dataset, store, calendar=calendar, calendar_view=bundle["entry_calendar"]["research_view"])
        except Exception as exc:
            store.observe_run("FAILED", type(exc).__name__)
            raise
        store.observe_run("COMPLETE")
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "validate", "synthetic", "rehearsal", "readiness"))
    args = parser.parse_args()
    root = Path.cwd()
    if args.action == "prepare":
        bundle = build_bundle(root)
        write(root / CONFIG, bundle)
        print(bundle["bundle_id"])
    elif args.action == "validate":
        print(validate_bundle(root)["bundle_id"])
    elif args.action == "readiness":
        bundle = validate_bundle(root)
        print(json.dumps({"status": bundle["historical_readiness"], "proposal": bundle["historical_readiness_proposal"]}, indent=2))
    elif args.action == "rehearsal":
        report = run_rehearsal(root)
        print(json.dumps({k: v for k, v in report.items() if k != "artifact_sha256s"}, indent=2))
        if report["status"] != "REHEARSAL_PASSED":
            raise SystemExit(1)
    else:
        report = run_synthetic(root)
        print(json.dumps({"status": report["status"], "markets": {m: r["status"] for m, r in report["markets"].items()}}, indent=2))
        if report["status"] != "SYNTHETIC_PASSED":
            raise SystemExit(1)


if __name__ == "__main__":
    main()
