"""One approved price-lineage census; no feature, label, or model computation."""
from __future__ import annotations

from collections import Counter
import ctypes
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import time

import pyarrow.parquet as pq

from .active_data_view import resolve
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .causal_observation_parquet import _decode_physical
from .errors import IntegrityError
from .rolling_hour_preparation import CONFIG, validate_bundle

OPERATION = "CENSUS_ROLLING_HOUR_V6_PRICE_READINESS_ONCE"
BUNDLE_ID = "b698ffc863bf9650f99717e4c69176718dde285f429e12d4e477470b1ec7ae73"
MINUTE = 60_000_000_000
COLUMNS = (
    "market", "source_cadence", "bar_start_ns", "bar_end_ns", "source_timestamp_ns",
    "available_at_ns", "decision_eligible_at_ns", "actual_contract", "raw_symbol",
    "definition_received_at_ns", "listing_activation_ns", "expiration_ns",
    "project_trade_date", "project_grouping_start_ns", "project_grouping_end_ns",
    "official_schedule_state", "source_release_id", "source_contract_id",
    "source_file_sha256", "source_row_sha256", "definition_source_file_sha256",
    "definition_row_sha256",
)


def approved_plan(root):
    bundle = json.loads((root / CONFIG).read_bytes())
    validate_bundle(bundle, root)
    if bundle["bundle_id"] != BUNDLE_ID:
        raise IntegrityError("census approval belongs to a different frozen bundle")
    return bundle, bundle["readiness_preparation"]["bounded_price_readiness_operation"]


def receipt_scope(root, plan):
    return {"bundle_id": BUNDLE_ID, "plan_sha256": sha256_json(plan),
            "implementation_sha256": sha256_file(Path(__file__)),
            "gateway_policy_sha256": sha256_file(root / "src/futures_rebuild/research_gateway_policy.py"),
            "purpose": plan["purpose"]}


def summarize(rows, source, check_limit=lambda: None):
    """Report clock/provenance exceptions, retaining every observed project date.

    Consecutive clock gaps are unclassified: a missing official calendar prevents
    deciding whether a minute should exist. No fitting eligibility is inferred.
    """
    days, exceptions, schedules, contracts = {}, Counter(), Counter(), Counter()
    previous = None
    total = 0
    lower = int(datetime(source["year"], 1, 1, tzinfo=timezone.utc).timestamp()) * 10**9
    upper = int(datetime(source["year"] + 1, 1, 1, tzinfo=timezone.utc).timestamp()) * 10**9
    for row in rows:
        total += 1
        if total > source["row_count"]:
            raise IntegrityError("source exceeds its frozen row budget")
        if total % 8192 == 0:
            check_limit()
        if any(row.get(k) is None for k in COLUMNS):
            raise IntegrityError("required census metadata is absent or null")
        start, end = row["bar_start_ns"], row["bar_end_ns"]
        day = str(row["project_trade_date"])
        date.fromisoformat(day)
        item = days.setdefault(day, {"rows": 0, "unclassified_internal_gap_minutes": 0,
            "contract_changes": 0, "max_consecutive_same_contract_bars": 0})
        item["rows"] += 1
        schedules[row["official_schedule_state"]] += 1
        contract = row["actual_contract"]
        contracts[contract] += 1
        checks = {
            "market": row["market"] == source["market"],
            "cadence": row["source_cadence"] == "1m" and end - start == MINUTE and start % MINUTE == 0,
            "period": lower <= start < upper,
            "source_timestamp": row["source_timestamp_ns"] == start,
            "availability": end <= row["available_at_ns"] <= row["decision_eligible_at_ns"],
            "definition_known_at_bar_start": row["definition_received_at_ns"] <= start,
            "contract_active": row["listing_activation_ns"] <= start < row["expiration_ns"],
            "contract_identity": bool(contract) and contract == row["raw_symbol"],
            "project_grouping": row["project_grouping_start_ns"] <= start < end <= row["project_grouping_end_ns"],
            "lineage_ids": all(isinstance(row[k], str) and len(row[k]) == 64
                and all(c in "0123456789abcdef" for c in row[k]) for k in (
                    "source_release_id", "source_contract_id", "source_file_sha256",
                    "source_row_sha256", "definition_source_file_sha256", "definition_row_sha256")),
        }
        for name, valid in checks.items():
            if not valid:
                exceptions[name] += 1
        run = 1
        if previous is not None:
            prev_start, prev_day, prev_contract, prev_run = previous
            if start <= prev_start:
                exceptions["chronological_unique_minute"] += 1
            if day == prev_day:
                if start > prev_start + MINUTE:
                    item["unclassified_internal_gap_minutes"] += (start - prev_start) // MINUTE - 1
                item["contract_changes"] += int(contract != prev_contract)
                if start == prev_start + MINUTE and contract == prev_contract:
                    run = prev_run + 1
        item["max_consecutive_same_contract_bars"] = max(item["max_consecutive_same_contract_bars"], run)
        previous = start, day, contract, run
    if total != source["row_count"]:
        raise IntegrityError("decoded count differs from frozen source count")
    return {"market": source["market"], "year": source["year"], "row_count": total,
            "observed_project_dates": len(days), "dates": days, "exceptions": dict(exceptions),
            "official_schedule_states": dict(schedules), "contracts": dict(contracts)}


def _working_set():
    # Windows native process counters avoid adding a monitoring dependency.
    class Counters(ctypes.Structure):
        _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong)] + [
            (name, ctypes.c_size_t) for name in ("PeakWorkingSetSize", "WorkingSetSize",
                "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]
    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    fn = ctypes.windll.psapi.GetProcessMemoryInfo
    fn.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]
    if not fn(ctypes.c_void_p(-1), ctypes.byref(counters), counters.cb):
        raise OSError("cannot measure census working set")
    return counters.PeakWorkingSetSize


def execute_once(root: Path, receipt: OperationReceipt):
    """Called by the approved task, never by an approval-accepting CLI."""
    root = root.resolve(strict=True)
    bundle, plan = approved_plan(root)
    boundary = RepoBoundary(root)
    scope = receipt_scope(root, plan) | {
        "approval_command": OPERATION, "approval_plan_id": sha256_json(plan),
        "approval_plan_sha256": sha256_json(plan)}
    classification = OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
    receipt.verify(boundary, operation=OPERATION, classification=classification, required_scope=scope)
    output = boundary.assert_active_path(root / plan["output_directory"], purpose="price census evidence")
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    written = 0

    def save(name, payload):
        nonlocal written
        encoded = json.dumps(payload, sort_keys=True, indent=2).encode() + b"\n"
        if written + len(encoded) > plan["estimated_output_bytes_max"]:
            raise IntegrityError("census output budget exceeded")
        with (output / name).open("xb") as stream:
            stream.write(encoded)
        written += len(encoded)

    def check_limit():
        if time.monotonic() - started >= plan["hard_stop_minutes"] * 60:
            raise IntegrityError("census time budget exhausted")
        if _working_set() >= plan["working_memory_bytes_max"]:
            raise IntegrityError("census memory budget exhausted")

    save("approval.json", {"user_message": "Approve", "approved_scope": plan,
        "receipt": receipt.as_dict(), "source": "CONVERSATIONAL_APPROVAL_OF_PRECEDING_BOUNDED_CENSUS_REQUEST"})
    receipt.consume(boundary, operation=OPERATION, classification=classification, required_scope=scope)
    results = []
    try:
        check_limit()
        if sha256_file(root / "data/causally_gated_normalized/_control/catalog.json") != bundle["readiness_preparation"]["price_catalog_sha256"]:
            raise IntegrityError("active catalog changed since preparation")
        for source in plan["exact_sources"]:
            check_limit()
            receipt.assert_consumed(boundary, operation=OPERATION, classification=classification, required_scope=scope)
            path = resolve(repository_root=root, market=source["market"], year=source["year"], purpose="LINEAGE_AUDIT")
            if path != root / source["parquet_path"] or path.stat().st_size != source["physical_bytes"]:
                raise IntegrityError("resolved path or size differs from frozen source")
            manifest = root / source["sidecar_path"]
            if sha256_file(path) != source["parquet_sha256"] or sha256_file(manifest) != source["sidecar_sha256"]:
                raise IntegrityError("frozen source hashes differ")
            with pq.ParquetFile(path) as parquet:
                if parquet.metadata.num_rows != source["row_count"] or not set(COLUMNS) <= set(parquet.schema_arrow.names):
                    raise IntegrityError("census schema/count differs")
                rows = (_decode_physical("observations", row)
                    for batch in parquet.iter_batches(batch_size=8192, columns=list(COLUMNS))
                    for row in batch.to_pylist())
                result = summarize(rows, source, check_limit)
            if sha256_file(path) != source["parquet_sha256"] or sha256_file(manifest) != source["sidecar_sha256"]:
                raise IntegrityError("source changed during census")
            result["source_hashes_verified_before_and_after"] = True
            result["source_binding"] = source
            save(f"{source['market']}_{source['year']}.json", result)
            results.append(result)
            print(f"{source['market']} {source['year']}: {result['row_count']} rows; {result['observed_project_dates']} observed dates", flush=True)
        if sum(r["row_count"] for r in results) != plan["row_limit"]:
            raise IntegrityError("complete census row total differs")
        check_limit()
        markets = {}
        for market in ("ES", "NQ", "CL", "GC"):
            days = Counter()
            for result in results:
                if result["market"] == market:
                    days.update({d: v["rows"] for d, v in result["dates"].items()})
            first, last = min(date(2018, 1, 1), date.fromisoformat(min(days))), max(date(2022, 12, 31), date.fromisoformat(max(days)))
            calendar = {str(first + timedelta(days=i)): days[str(first + timedelta(days=i))] for i in range((last-first).days+1)}
            markets[market] = {"rows": sum(days.values()), "observed_project_dates": len(days),
                "calendar_date_row_counts": calendar, "zero_row_calendar_dates_unclassified": sum(v == 0 for v in calendar.values())}
        report = {"state": "COMPLETE_PRICE_CENSUS_NOT_TRAINING_READINESS", "bundle_id": BUNDLE_ID,
            "receipt_id": receipt.receipt_id, "files_completed": len(results), "rows": plan["row_limit"],
            "markets": markets, "elapsed_seconds": time.monotonic()-started,
            "peak_working_set_bytes": _working_set(), "metadata_exception_counts": dict(sum((Counter(r["exceptions"]) for r in results), Counter())),
            "report_bytes_before_summary": written, "fitting_eligibility": "UNDETERMINED",
            "calendar": "PROJECT_GROUPINGS_ONLY;OFFICIAL_APPLICABILITY_AND_PIT_EVIDENCE_MISSING",
            "missing_minutes": "INTERNAL_CLOCK_GAPS_REPORTED;EXPECTED_MINUTES_UNDETERMINED_WITHOUT_OFFICIAL_CALENDAR",
            "quotes_costs": "COMPATIBLE_2018_2022_QUOTES_FEES_AND_CLOCK_EVIDENCE_MISSING",
            "models_features_labels_predictions_evaluation_computed": False,
            "artifacts": {p.name: sha256_file(p) for p in output.glob("*.json")}}
        save("report.json", report)
        return report
    except Exception as exc:
        save("failure.json", {"state": "CONSUMED_INCOMPLETE_NO_RETRY", "error": str(exc),
            "completed_files": len(results), "receipt_id": receipt.receipt_id})
        raise
