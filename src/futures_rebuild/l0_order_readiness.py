"""Frozen-window, fixed-benchmark timing diagnostics. No historical CLI executor."""
from collections import Counter, defaultdict
from itertools import chain
import gc
import json
import shutil
import time

from . import l0_alpha_candidate as c, l0_alpha_calendar as cal, l0_alpha_source_readiness as source
from .canonical import read_canonical_authority, sha256_file
from .errors import IntegrityError, UnauthorizedOperation
from .l0_alpha_replay import replay

INVENTORY = "state/source_contract_registry/ce7aa315fab7fb411a3c42ece4e0c0b2c0612f7ab37899f573eebcd4bf09db58/canonical_inventory.json"
DIAG = "state/unpublished_evidence/l0_alpha_gap_diagnosis_v2"


def windows():
    result = {}
    for i, fold in enumerate(c.design()["folds"]):
        for name, item in [("outer", fold["outer"]), *[(f"inner_{j}", w) for j, w in enumerate(fold["inner"])]]:
            key = (item["start_index"], item["stop_index"])
            row = result.setdefault(key, {"start": key[0], "stop": key[1], "dates": item["audit_dates"], "slots": []})
            row["slots"].append(f"outer_{i}/{name}")
    return list(sorted(result.values(), key=lambda v: v["start"]))


def prepare(root, bundle):
    return c.bind({"purpose": "L0_CALENDAR_AND_FROZEN_WINDOW_BENCHMARK_ORDER_TIMING_NO_FIT_NO_PNL",
        "bundle_id": bundle["bundle_id"], "calendar": bundle["entry_calendar"], "windows": windows(),
        "views": list(cal.VIEWS), "benchmarks": ["no_trade", "always_long", "always_short"],
        "source_plan": bundle["historical_readiness_proposal"],
        "evidence": {name: sha256_file(root / name) for name in [INVENTORY, cal.DIAGNOSIS, *[f"{DIAG}/{m}.json" for m in c.MARKETS]]},
        "start_state": "FLAT_PER_WINDOW_NEVER_RESET_DAILY", "unresolved": "STOP_WINDOW_NO_REPLACEMENT_OR_RESTART",
        "limits": {"seconds": 1200, "peak_bytes": 6 * 2**30, "output_bytes": 2**30},
        "authority": "USER_IMPLEMENT_UPDATED_PLAN;DIAGNOSTIC_ONLY;NO_FITTING_L1_PROVIDER_OR_ACTIVATION"}, "plan_id")


def status_runs(ledger):
    runs = []
    for i, row in enumerate(ledger):
        state = row["status"]
        if runs and runs[-1][2] == state:
            runs[-1][1] = i + 1
        else:
            runs.append([i, i + 1, state])
    return runs


def assess(dataset, calendar, gaps, save, check_limit=lambda: None):
    """One opportunity ledger; identical windows and directional timing deduplicated."""
    columns = ["status", "actual_contract", "tick_nano", "tick_value_nano_usd"]
    endpoint_columns = ["entry_status", "exit_status", "entry_open_nano", "exit_open_nano"]
    save("opportunities", {"evidence_id": dataset._evidence_id, "market": dataset.market,
        "mechanism_id": c.mechanism_id(), "dates": list(dataset.calendar_dates), "columns": columns + endpoint_columns,
        "records": [[*[d.get(k) for k in columns], *[o.get(k) for k in endpoint_columns]]
                    for d, o in zip(dataset.decisions, dataset.outcomes, strict=True)],
        "clock": "330_DECISIONS_PER_DATE;IDS_AND_TIMESTAMPS_RECONSTRUCT_FROM_MECHANISM_MARKET_CLOCK",
        "source_facts": list(dataset.source_facts)})
    gap_times = {v["decision_ns"] for v in gaps}
    actual = {d["decision_ns"] for d, o in zip(dataset.decisions, dataset.outcomes, strict=True)
              if d["status"] == "ELIGIBLE" and any(o.get(s + "_status") == "MISSING_REFERENCE" for s in ("entry", "exit"))}
    if actual != gap_times:
        raise IntegrityError("new opportunity stream does not reconcile to every prior missing-reference decision")
    result = {}
    for view in cal.VIEWS:
        exposures, reports = defaultdict(list), []
        census = Counter()
        context = cal.compile_calendar(calendar, view)
        audit_days = {day for w in windows() for day in w["dates"]}
        for d in dataset.decisions:
            if d["trade_date"] in audit_days and d["status"] == "ELIGIBLE":
                census[cal.entry_permission(d, context)["state"]] += 1
        for number, window in enumerate(windows()):
            check_limit()
            a, b = window["start"] * 330, window["stop"] * 330
            decisions, outcomes = dataset.decisions[a:b], dataset.outcomes[a:b]
            long_result = None
            for policy in ("always_long", "always_short", "no_trade"):
                run = replay(decisions, outcomes, None, window["dates"], policy=policy,
                             calendar=calendar, calendar_view=view, timing_only=True)
                if policy == "always_short":
                    if ([r["status"] for r in run["decisions"]] != [r["status"] for r in long_result["decisions"]]
                            or run["events"] != [dict(e, direction=-1) if "direction" in e else e for e in long_result["events"]]):
                        raise IntegrityError("long/short timing is asymmetric")
                    continue
                if policy == "always_long":
                    long_result = run
                    for d, r in zip(decisions, run["decisions"], strict=True):
                        if d["decision_ns"] in gap_times:
                            exposures[d["decision_ns"]].append({"window": number, "slots": len(window["slots"]), "status": r["status"]})
                summary = {k: v for k, v in run.items() if k not in ("decisions", "events", "replay_id")}
                summary.update(window=number, slots=window["slots"], first_date=window["dates"][0], last_date=window["dates"][-1],
                    status_counts=dict(Counter(r["status"] for r in run["decisions"])),
                    assessment="NO_ORDERS_ATTEMPTED" if not run["attempted_entries"] else "REFERENCES_RESOLVED" if run["complete"] else "UNRESOLVED_EXECUTION")
                save(f"{view}/window_{number}/{policy}", {**summary, "status_runs": status_runs(run["decisions"]), "events": run["events"]})
                reports.append(summary)
            del long_result
        unique, repeated, slot_counts, reconciliation = Counter(), Counter(), Counter(), []
        for gap in gaps:
            matches = exposures[gap["decision_ns"]]
            states = sorted({m["status"] for m in matches}) or ["OUTSIDE_AUDIT_WINDOWS"]
            unique["|".join(states)] += 1
            for m in matches:
                repeated[m["status"]] += 1
                slot_counts[m["status"]] += m["slots"]
            reconciliation.append({"market": dataset.market, "decision_ns": gap["decision_ns"], "states": states, "windows": matches})
        save(view + "/gap_reconciliation", reconciliation)
        result[view] = {"windows": reports, "directional_timing": "LONG_SHORT_IDENTICAL_SHARED_LEDGER",
            "calendar_permission_unique_eligible_audit_decisions": dict(census),
            "gap_unique_decisions": dict(unique), "gap_distinct_window_exposures": dict(repeated), "gap_original_slot_exposures": dict(slot_counts),
            "calendar_evidence_complete": census["UNKNOWN"] == 0,
            "benchmark_timing_complete": all(r["complete"] for r in reports),
            "fitting_authorized": False, "model_policy_completeness": "NOT_ESTABLISHED"}
    return {"prior_missing_decisions": len(gaps), "views": result}


def recovery_metadata(root):
    inventory = json.loads((root / INVENTORY).read_text())
    entries = [r for r in inventory["entries"] if r["market"] in c.MARKETS and 2018 <= r["year"] <= 2022
               and r["family"] == "ohlcv_1m" and r["kind"] == "DBN"]
    return {"inventory": INVENTORY, "inventory_sha256": sha256_file(root / INVENTORY), "matching_owned_sources": entries,
        "status": "NO_REPLACEMENT_CANDIDATE_IN_THIS_INVENTORY" if len(entries) == 20 else "CANDIDATES_REQUIRE_IDENTITY_REVIEW",
        "scope": "CURRENT_CANONICAL_INVENTORY_METADATA_ONLY;NOT_EXHAUSTIVE_HOST_SEARCH;NO_CANDIDATE_PAYLOAD_READ",
        "known_sources": "ALREADY_DIAGNOSED_CANONICAL_MINUTE_FILES;COPIES_DO_NOT_REPAIR_ABSENT_RECORDS",
        "source_correction_activated": False}


def execute_once(gateway, receipt, plan):
    from .certified_research_gateway import CertifiedResearchGateway
    from .boundary import OperationReceipt
    from .l0_alpha_preparation import validate_bundle, ResearchStore, AS_OF_NS
    from .locking import FileLease
    from .rolling_hour_price_census import _working_set
    if type(gateway) is not CertifiedResearchGateway or type(receipt) is not OperationReceipt:
        raise UnauthorizedOperation("existing gateway and exact approved receipt required")
    root = gateway.root
    bundle = validate_bundle(root)
    if prepare(root, bundle) != plan:
        raise IntegrityError("order diagnostic inputs changed since freeze")
    required = dict(receipt.scope)
    actual = {k: v for k, v in required.items() if k not in ("approval_command", "approval_plan_id", "approval_plan_sha256")}
    if actual != source.scope(bundle) or required.get("approval_plan_id") != plan["plan_id"]:
        raise UnauthorizedOperation("receipt differs from exact order-readiness plan")
    output = root / cal.OUTPUT / plan["plan_id"]
    with FileLease(output / "writer.lock"):
        store = ResearchStore(output, {"plan_id": plan["plan_id"]}, maximum_bytes=plan["limits"]["output_bytes"])
        if store.path("started").exists():
            raise UnauthorizedOperation("diagnostic attempt already consumed; no automatic retry")
        if shutil.disk_usage(output).free < plan["limits"]["output_bytes"] + 256 * 2**20:
            raise IntegrityError("diagnostic output reserve unavailable")
        receipt.consume(gateway.boundary, operation=source.OPERATION, classification=source.CLASSIFICATION, required_scope=required)
        store.save("started", {"plan": plan, "receipt_id": receipt.receipt_id})
        started, counts, reports = time.monotonic(), {}, {}
        def check():
            if time.monotonic() - started > plan["limits"]["seconds"] or _working_set() > plan["limits"]["peak_bytes"]:
                raise IntegrityError("diagnostic time or memory bound exceeded")
        try:
            calendar = read_canonical_authority(root / cal.ARTIFACT, identity_field="calendar_id")
            for market in c.MARKETS:
                rows = iter(source._verified_rows(root, [r for r in plan["source_plan"]["exact_sources"] if r["market"] == market],
                            receipt, gateway.boundary, required, check, counts))
                first = next(rows)
                dataset = c.assemble_dataset(market, source.daily_inputs(chain([first], rows)), as_of_ns=AS_OF_NS,
                    source_contract_id=first["source_contract_id"], source_release_id=first["source_release_id"])
                gaps = json.loads((root / DIAG / (market + ".json")).read_text())["cases"]
                reports[market] = assess(dataset, calendar, gaps, lambda name, value: store.save(market + "/" + name, value), check)
                support = c.source_completeness(dataset)
                reports[market]["fold_support"] = {k: support[k] for k in ("support_status", "fold_support")}
                store.save(market + "/report", reports[market])
                print(market + ": frozen-window timing and gap reconciliation complete", flush=True)
                del dataset
                gc.collect()
                check()
            if sum(counts.values()) != plan["source_plan"]["row_limit"] or prepare(root, bundle) != plan:
                raise IntegrityError("complete diagnostic input accounting differs")
            report = {"plan_id": plan["plan_id"], "markets": reports, "source_rows": counts, "recovery": recovery_metadata(root),
                "elapsed_seconds": time.monotonic() - started, "peak_working_set_bytes": _working_set(), "output_bytes_before_report": store.bytes,
                "state": "DIAGNOSTIC_COMPLETE_NOT_RESEARCH_ADMISSION", "models_fitted": False, "profitability_computed": False,
                "L1_or_protected_values_accessed": False, "calendar_assumptions": calendar["assumption"],
                "regular_hours": calendar["regular_hours"], "fees_and_admission": bundle["historical_readiness"], "sources_preserved": True}
            store.save("report", report)
            return report
        except Exception as exc:
            store.save("failure", {"error": str(exc), "completed_markets": list(reports), "source_rows": counts, "state": "SEALED_INCOMPLETE_NO_AUTOMATIC_RETRY"})
            raise
