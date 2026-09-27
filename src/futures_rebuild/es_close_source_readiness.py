"""Inactive ES reference-metadata census. CLI prepares; no operation is adopted.

No OHLCV value columns, features, targets, models or trading policy are consumed.
Receipt/resolver/calendar doubles belong in tests, never in this module.
"""
import argparse
from collections import Counter
import ctypes
from datetime import date, datetime, time as day_time, timedelta, timezone
from importlib.metadata import version
import json
from pathlib import Path
import platform
import re
import shutil
import time
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from . import active_data_view
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (assert_no_linklike_ancestors, assert_plain_file, canonical_bytes,
                        io_path, sha256_file, sha256_json, write_new_canonical_authority)
from .causal_observation_parquet import SCHEMAS, _decode_physical, _parquet_io_path
from .errors import IntegrityError, UnauthorizedOperation
from .research_gateway_policy import require_current_real_history_operation

OPERATION = "CENSUS_ES_CLOSE_REFERENCE_METADATA_ONCE"
CLAIM = "ES_REFERENCE_LINEAGE_ONLY_NO_TARGETS_OR_FITS"
BASE = Path("state/unpublished_evidence")
PARENT = BASE / "es_close_anchored_continuation_historical_readiness_v1"
PROPOSAL = PARENT / "read_proposal.json"
PROPOSAL_SHA = "3c41ab3dfacdced7428f41f58665879c392db63c1a1c76f31337a8873ea014e5"
DECLARATION = BASE / "es_close_anchored_continuation_v1_proposed/DECLARATION.md"
DECLARATION_SHA = "4a1ea185dc0aa36b8ead679cbdf7bee464920ef5b1d1d2e972e9cae34887acc7"
CALENDAR = BASE / "l0_order_readiness_v1/es_close_momentum_readiness_v1/cash_calendar_integration/integration.json"
APPROVED = Path("configs/approved_es_close_source_readiness.json")  # Not created/adopted here.
DESTINATION = BASE / "es_close_source_readiness_v1"
SOURCE_YEARS = tuple(range(2010, 2026))
START = "2010-06-06"
END = "2025-07-13T22:00:00Z"
NY, PACIFIC = ZoneInfo("America/New_York"), ZoneInfo("America/Los_Angeles")
CLOCKS = ("15:28", "15:30", "15:59", "16:00")
NS = 10**9
EXCLUDED = frozenset(("open_nano", "high_nano", "low_nano", "close_nano", "volume"))
FIELDS = tuple(n for n in SCHEMAS["observations"].names if n not in EXCLUDED)
LIMITS = dict(batch_rows=4096, row_group_uncompressed_bytes=256*2**20,
              peak_working_set_bytes=2*2**30, peak_commit_bytes=2*2**30,
              wall_seconds=1800, total_output_bytes=8*2**20,
              date_record_bytes=1024, control_bytes=2**20,
              failure_bytes=65536, completion_bytes=65536, receipt_ledger_bytes=4096)
CODE_FILES = tuple("src/futures_rebuild/" + p + ".py" for p in (
    "es_close_source_readiness", "active_data_view", "causal_observation_parquet",
    "boundary", "canonical", "errors", "research_gateway_policy", "discovery",
    "l0_alpha_preparation", "locking"))
CODE_ROOT = Path(__file__).resolve().parents[2]


def _path(root, relative):
    if (not isinstance(relative, str) or not relative or ":" in relative or "\\" in relative
            or any(p in ("", ".", "..") for p in relative.split("/"))):
        raise UnauthorizedOperation("noncanonical census path")
    p = Path(root) / relative
    assert_no_linklike_ancestors(p)
    if not p.resolve().is_relative_to(Path(root).resolve()):
        raise UnauthorizedOperation("census path escapes root")
    return p


def _json(path):
    assert_no_linklike_ancestors(path)
    assert_plain_file(path)
    return json.loads(io_path(path).read_bytes())


def _ns(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(dt.replace(tzinfo=timezone.utc).timestamp())*NS if dt.tzinfo is None else int(dt.timestamp())*NS


def _stamp(day, clock):
    return int(datetime.combine(date.fromisoformat(day), day_time.fromisoformat(clock), NY).timestamp())*NS


def _dates():
    d, last = date.fromisoformat(START), datetime.fromisoformat(END.replace("Z", "+00:00")).date()
    while d <= last:
        yield d.isoformat()
        d += timedelta(days=1)


def _identity(value, field="operation_id"):
    if value.get(field) != sha256_json({k: v for k, v in value.items() if k != field}):
        raise IntegrityError("census identity differs: " + field)


def prepare(root, output=None):
    """Exact source-safe metadata only. Never resolve, hash or open Parquet."""
    root = Path(root).resolve()
    proposal_path = _path(root, PROPOSAL.as_posix())
    if sha256_file(proposal_path) != PROPOSAL_SHA:
        raise IntegrityError("read proposal changed")
    p = _json(proposal_path)
    if (p["execution_enabled"] is not False or any(p["authority"].values())
            or p["proposed_operation"] != OPERATION or p["purpose"] != "LINEAGE_AUDIT"
            or p["projected_fields"] != list(FIELDS)
            or p["scope"]["markets"] != ["ES"] or p["scope"]["calendar_start"] != START
            or p["scope"]["available_at_exclusive_end"] != END
            or p["scope"]["candidate_clock_minutes_ny"] != list(CLOCKS)
            or p["declaration_path"] != DECLARATION.as_posix()
            or p["declaration_sha256"] != DECLARATION_SHA
            or any(p["caps_proposed_not_measured"][k] != v for k, v in LIMITS.items())):
        raise UnauthorizedOperation("proposal scope, authority, fields or caps differ")
    bindings = {PROPOSAL.as_posix(): PROPOSAL_SHA, DECLARATION.as_posix(): DECLARATION_SHA,
                "data/active/catalog.json": p["catalog_sha256"]}
    catalog = _json(_path(root, "data/active/catalog.json"))
    sources = p["source_files"]
    if [s["year"] for s in sources] != list(SOURCE_YEARS):
        raise UnauthorizedOperation("exact ordered ES source years required")
    for s in sources:
        name = f"data/active/causally_gated_normalized/ES/{s['year']}/{s['year']}.parquet"
        if (s["market"] != "ES" or s["parquet_path"] != name
                or s["sidecar_path"] != name + ".manifest.json"
                or "LINEAGE_AUDIT" not in s["permitted_uses"]
                or not _ns(START) <= _ns(s["coverage_start"]) < _ns(s["coverage_end"]) <= _ns(END)):
            raise UnauthorizedOperation("source market, path, role or interval differs")
        matches = [e for e in catalog["entries"] if e["market"] == "ES" and e["year"] == s["year"]]
        if len(matches) != 1 or any(matches[0].get(k) != v for k, v in s.items()
                                   if k in matches[0]):
            raise IntegrityError("catalog source binding differs")
        bindings[s["sidecar_path"]] = s["sidecar_sha256"]
        sidecar = _json(_path(root, s["sidecar_path"]))
        if sidecar["entry_binding"] != {k: v for k, v in matches[0].items() if k != "sidecar_sha256"}:
            raise IntegrityError("sidecar entry differs")
        active_data_view.validate_access_policy_binding(sidecar["access_policy_binding"])
        active_data_view.validate_content_validation_receipt(sidecar["content_validation_receipt"])
        if (sidecar["access_policy_binding"]["access_policy_binding_id"] != s["access_policy_binding_id"]
                or sidecar["content_validation_receipt"]["content_validation_receipt_id"]
                != s["content_validation_receipt_id"]):
            raise IntegrityError("sidecar policy or certificate differs")
        # stat is metadata. The resolver's payload hash is intentionally NOT called here.
        if _path(root, name).stat().st_size != s["payload_bytes_by_stat"]:
            raise IntegrityError("source stat size differs")
    limits = p["caps_proposed_not_measured"]
    if (len(sources) != limits["source_files"]
            or sum(s["row_count"] for s in sources) != limits["declared_rows"]
            or sum(s["payload_bytes_by_stat"] for s in sources) != limits["source_file_bytes_by_stat"]
            or any(type(s["row_count"]) is not int or s["row_count"] <= 0 for s in sources)):
        raise IntegrityError("source-count or byte budget differs")
    calendar = _json(_path(root, CALENDAR.as_posix()))
    bindings[CALENDAR.as_posix()] = sha256_file(_path(root, CALENDAR.as_posix()))
    for ref in calendar["source_files"].values():
        relative = ref["path"]
        if (not relative.startswith(CALENDAR.parent.parent.as_posix()+"/cash_calendar_evidence/")
                or Path(relative).suffix not in (".html", ".pdf")):
            raise UnauthorizedOperation("calendar reference outside source-safe documents")
        bindings[relative] = ref["sha256"]
    for relative, digest in bindings.items():
        if sha256_file(_path(root, relative)) != digest:
            raise IntegrityError("metadata binding changed: " + relative)
    days = {}
    for day, rec in calendar["cash_days"].items():
        date.fromisoformat(day)
        state, known = rec["cash_state"], rec["available_at_ns"]
        if state not in ("FULL", "SHORT", "CLOSED", "UNKNOWN") or (known is not None and type(known) is not int):
            raise IntegrityError("invalid cash-calendar fact")
        if state != "UNKNOWN" and (known is None or not rec["evidence_refs"]):
            raise IntegrityError("cash-calendar classification lacks availability/evidence")
        days[day] = {"state": state, "known_ns": known, "evidence": list(rec["evidence_refs"])}
    op = {"schema_version": "es_close_source_census/1", "state": "PREPARED_INACTIVE",
          "operation": OPERATION, "claim": CLAIM, "repository_root": str(root),
          "declaration_sha256": DECLARATION_SHA, "metadata_bindings": bindings,
          "implementation_bindings": {n: sha256_file(CODE_ROOT/n) for n in CODE_FILES},
          "runtime": {"python": platform.python_version(), **{n: version(n) for n in ("pyarrow", "tzdata")}},
          "sources": sources, "fields": list(FIELDS), "calendar": days,
          "start": START, "end_exclusive": END, "limits": dict(LIMITS),
          "maximum_dates": len(tuple(_dates())), "execution_enabled": False}
    op["operation_id"] = sha256_json(op)
    if output is not None:
        output = Path(output).absolute()
        assert_no_linklike_ancestors(output)
        output = output.resolve()
        if not output.is_relative_to(root/BASE):
            raise UnauthorizedOperation("preparation output must be unpublished")
        write_new_canonical_authority(output, op)
    return op


def scope(op):
    return {"operation_id": op["operation_id"], "declaration_sha256": op["declaration_sha256"], "claim": CLAIM}


def require_scope(root, op):
    """Future policy hook: exact inactive adoption, never a receipt substitute."""
    _identity(op)
    adopted = _json(_path(root, APPROVED.as_posix()))
    _identity(adopted, "adoption_id")
    if (adopted.get("state") != "ADOPTED_INACTIVE_RECEIPT_REQUIRED"
            or adopted.get("operation") != OPERATION or adopted.get("scope") != scope(op)):
        raise UnauthorizedOperation("exact census operation is not adopted")
    relative = adopted.get("operation_path", "")
    if not relative.startswith(BASE.as_posix()+"/") or not relative.endswith(".json"):
        raise UnauthorizedOperation("operation reference must be unpublished metadata")
    path = _path(root, relative)
    if sha256_file(path) != adopted.get("operation_sha256") or _json(path) != op:
        raise IntegrityError("adopted operation bytes differ")
    if prepare(root) != op:
        raise IntegrityError("current source, code, calendar or runtime differs")


def _authorize(boundary, receipt, op, *, consumed):
    if type(boundary) is not RepoBoundary or type(receipt) is not OperationReceipt:
        raise UnauthorizedOperation("real boundary and exact operation receipt required")
    if boundary.active_root != Path(op["repository_root"]).resolve():
        raise UnauthorizedOperation("census receipt root differs")
    received = dict(receipt.scope)
    approval_keys = {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    if set(received)-set(scope(op))-approval_keys:
        raise UnauthorizedOperation("unexpected receipt scope keys")
    required = {**scope(op), **{k: v for k, v in received.items() if k in approval_keys}}
    # Never infer approval from an installed reader, a caller flag or a local receipt.
    require_current_real_history_operation(OPERATION, required)
    method = receipt.assert_consumed if consumed else receipt.verify
    return method(boundary, operation=OPERATION,
                  classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
                  required_scope=required)


def _memory():
    """Existing Windows process-counter pattern, extended to include commit."""
    class Counters(ctypes.Structure):
        _fields_ = [("cb", ctypes.c_ulong), ("PageFaultCount", ctypes.c_ulong)] + [
            (n, ctypes.c_size_t) for n in ("PeakWorkingSetSize", "WorkingSetSize",
             "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
             "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]
    c = Counters()
    c.cb = ctypes.sizeof(c)
    fn = ctypes.windll.psapi.GetProcessMemoryInfo
    fn.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_ulong]
    if not fn(ctypes.c_void_p(-1), ctypes.byref(c), c.cb):
        raise OSError("census process memory unavailable")
    return c.PeakWorkingSetSize, c.PeakPagefileUsage


class _Budget:
    """One create-only attempt, exact serialization and reserved failure bytes."""
    def __init__(self, path, op):
        from .l0_alpha_preparation import ResearchStore
        self.path, self.op = path, op
        self.started = time.perf_counter()
        self.used = self.control = 0
        self.external = None
        self.peak_ws = self.peak_commit = 0
        self.failed = False
        self.files = {}
        if path.exists():
            raise UnauthorizedOperation("attempt exists; no automatic retry")
        if shutil.disk_usage(path.parent).free < LIMITS["total_output_bytes"] + 256*2**20:
            raise IntegrityError("insufficient output reserve")
        allowance = (op["maximum_dates"]*LIMITS["date_record_bytes"] + LIMITS["control_bytes"]
                     + LIMITS["failure_bytes"] + LIMITS["completion_bytes"] + LIMITS["receipt_ledger_bytes"])
        if allowance > LIMITS["total_output_bytes"]:
            raise IntegrityError("complete census output allowance exceeds cap")
        path.mkdir()  # Atomic attempt claim; existing/partial attempts are never resumed.
        self.store = ResearchStore(path, scope(op), maximum_bytes=LIMITS["total_output_bytes"])

    def check(self):
        ws, commit = _memory()
        self.peak_ws, self.peak_commit = max(ws, self.peak_ws), max(commit, self.peak_commit)
        if (self.failed or time.perf_counter()-self.started >= LIMITS["wall_seconds"]
                or ws >= LIMITS["peak_working_set_bytes"] or commit >= LIMITS["peak_commit_bytes"]):
            raise IntegrityError("census permanently stopped by resource limit")

    def save(self, name, value, *, kind="control"):
        self.check()
        size = len(canonical_bytes(value))+1
        ceiling = LIMITS[{"day": "date_record_bytes", "control": "control_bytes",
                         "complete": "completion_bytes"}[kind]]
        if size > ceiling or (kind == "control" and self.control+size > ceiling):
            raise IntegrityError("census record/control budget exceeded")
        if self.used+size+LIMITS["failure_bytes"] > LIMITS["total_output_bytes"]:
            raise IntegrityError("census output reserve exhausted")
        if self.store.path(name).exists():
            raise IntegrityError("census artifact already exists")
        self.store.save(name, value)
        if self.store.path(name).stat().st_size != size:
            raise IntegrityError("census serialization size differs")
        self.used += size
        if kind == "control":
            self.control += size
        self.files[name] = {"sha256": sha256_file(self.store.path(name)), "bytes": size}

    def fail(self, exc):
        self.failed = True
        # Fixed codes only: exceptions must never serialize source row values or prices.
        value = {"status": "FAILED_INCOMPLETE_NO_RETRY", "error_type": type(exc).__name__,
                 "saved_files": len(self.files), "complete": False, "operation_id": self.op["operation_id"]}
        size = len(canonical_bytes(value))+1
        if size <= LIMITS["failure_bytes"] and self.used+size <= LIMITS["total_output_bytes"]:
            self.store.save("failure", value)

    def resources(self):
        return {"seconds": time.perf_counter()-self.started, "peak_working_set_bytes": self.peak_ws,
                "peak_commit_bytes": self.peak_commit, "retained_bytes": self.used, "fits": 0, "cost_usd": 0}


def _rows(boundary, receipt, op, budget, counts):
    previous = None
    for s in op["sources"]:
        budget.check()
        _authorize(boundary, receipt, op, consumed=True)
        for relative in ("data/active/catalog.json", s["sidecar_path"]):
            if sha256_file(_path(boundary.active_root, relative)) != op["metadata_bindings"][relative]:
                raise IntegrityError("source metadata changed before payload access")
        expected = _path(boundary.active_root, s["parquet_path"])
        stamp = expected.stat()
        if stamp.st_size != s["payload_bytes_by_stat"]:
            raise IntegrityError("source stat changed")
        path = active_data_view.resolve(repository_root=boundary.active_root, market="ES",
                                        year=s["year"], purpose="LINEAGE_AUDIT")
        if path != expected:
            raise IntegrityError("resolved source path differs")
        # resolve already checked the exact sidecar and payload SHA. No duplicate hash pass.
        with pq.ParquetFile(_parquet_io_path(path)) as parquet:
            if (parquet.metadata.num_rows != s["row_count"]
                    or not parquet.schema_arrow.equals(SCHEMAS["observations"], check_metadata=True)):
                raise IntegrityError("source schema or count differs")
            if any(parquet.metadata.row_group(i).total_byte_size > LIMITS["row_group_uncompressed_bytes"]
                   for i in range(parquet.num_row_groups)):
                raise IntegrityError("source row group exceeds pre-decode memory allowance")
            count = 0
            for batch in parquet.iter_batches(batch_size=LIMITS["batch_rows"], columns=list(FIELDS), use_threads=False):
                budget.check()
                _authorize(boundary, receipt, op, consumed=True)
                if batch.schema.names != list(FIELDS):
                    raise IntegrityError("non-price projection differs")
                for raw in batch.to_pylist():
                    row = _decode_physical("observations", raw)
                    count += 1
                    t = row["bar_start_ns"]
                    if (row["market"] != "ES" or row["source_cadence"] != "1m"
                            or not _ns(s["coverage_start"]) <= t < _ns(s["coverage_end"])
                            or row["available_at_ns"] >= _ns(END) or row["decision_eligible_at_ns"] >= _ns(END)
                            or count > s["row_count"] or (previous is not None and t < previous)):
                        raise IntegrityError("source scope, boundary or chronological order differs")
                    previous = t
                    yield row
                budget.check()
            if count != s["row_count"]:
                raise IntegrityError("source row count did not reconcile")
            counts[str(s["year"])] = count
        after = expected.stat()
        if (stamp.st_size, stamp.st_mtime_ns, stamp.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
            raise IntegrityError("source changed during census")


def _reference(row):
    t = row["bar_start_ns"]
    if (row["source_timestamp_ns"] != t or row["bar_end_ns"] != t+60*NS
            or not t+65*NS <= row["available_at_ns"] <= row["decision_eligible_at_ns"]
            or not row["definition_event_at_ns"] <= row["definition_received_at_ns"] <= row["available_at_ns"]):
        return "INVALID_TIMING"
    if (not row["actual_contract"] or not row["raw_symbol"] or not row["source_file_path"]
            or not row["definition_source_file_path"] or row["instrument_id"] <= 0 or row["publisher_id"] <= 0
            or row["min_price_increment_nano"] <= 0 or row["multiplier_nano"] <= 0 or row["currency"] != "USD"
            or not row["listing_activation_ns"] <= t < row["expiration_ns"]):
        return "INVALID_SPECIFICATION"
    return "OBSERVED"


def _contract_key(row):
    fields = ("actual_contract", "instrument_id", "publisher_id", "currency",
              "min_price_increment_nano", "multiplier_nano")
    return sha256_json([row[k] for k in fields])


def _same(left, right):
    return _contract_key(left) == _contract_key(right)


def _calendar(op, day, decision):
    rec = op["calendar"].get(day)
    if rec is None or rec["known_ns"] is None or rec["known_ns"] > decision:
        return "UNKNOWN"
    return rec["state"]


def _previous_cash(op, day, decision):
    previous = date.fromisoformat(day)-timedelta(days=1)
    while previous >= date.fromisoformat(START):
        state = _calendar(op, previous.isoformat(), decision)
        if state == "UNKNOWN":
            return None, "UNKNOWN"
        if state != "CLOSED":
            return previous.isoformat(), state
        previous -= timedelta(days=1)
    return None, "UNKNOWN"


def _census(rows, op, budget):
    """Stream candidate clocks. No price arithmetic and no model integration."""
    iterator = iter(rows)
    pending = next(iterator, None)
    totals = Counter()
    prior_references = {}  # One small hashed close identity per bounded civil date.
    last_key = None
    for day in _dates():
        refs, states = {}, {c: "MISSING" for c in CLOCKS}
        tomorrow = _stamp((date.fromisoformat(day)+timedelta(days=1)).isoformat(), "00:00")
        while pending is not None and pending["bar_start_ns"] < tomorrow:
            row = pending
            status = _reference(row)
            totals["rows"] += 1
            totals[status] += 1
            key = row["bar_start_ns"]
            if key == last_key:
                totals["additional_rows_at_same_timestamp"] += 1
            last_key = key
            stamp = datetime.fromtimestamp(row["bar_start_ns"]/NS, NY)
            clock = stamp.strftime("%H:%M")
            if clock in CLOCKS and row["bar_start_ns"] == _stamp(day, clock):
                if clock in refs:
                    states[clock] = "AMBIGUOUS"
                else:
                    refs[clock] = row
                    states[clock] = status
            pending = next(iterator, None)
        decision = _stamp(day, "15:29:05")
        cash = _calendar(op, day, decision)
        prev_day, prev_state = _previous_cash(op, day, decision)
        prior = prior_references.get(prev_day)
        signal = refs.get("15:28")
        input_state = "CALENDAR_UNKNOWN"
        if cash == "CLOSED":
            input_state = "CALENDAR_CLOSED"
        elif cash == "SHORT" or (cash == "FULL" and prev_state == "SHORT"):
            input_state = "SHORT_CURRENT_OR_PRECEDING"
        elif cash == "FULL" and prev_state == "FULL":
            if (states["15:28"] != "OBSERVED" or prior is None or prior["status"] != "OBSERVED"):
                input_state = "MISSING_OR_INVALID_INPUT_REFERENCE"
            elif _contract_key(signal) != prior["identity"]:
                input_state = "PREVIOUS_CONTRACT_OR_SPEC_MISMATCH"
            elif max(signal["decision_eligible_at_ns"], prior["eligible_ns"]) > decision:
                input_state = "INPUT_LATE"
            else:
                input_state = "REFERENCES_PRESENT_SELECTION_UNVERIFIED"
        outcome = "UNOBSERVABLE"
        if states["15:30"] == states["16:00"] == states["15:28"] == "OBSERVED":
            if _same(signal, refs["15:30"]) and _same(signal, refs["16:00"]):
                outcome = "REFERENCES_PRESENT"
            else:
                outcome = "CONTRACT_OR_SPEC_MISMATCH"
        record = {"date": day, "cash": cash, "previous_cash_date": prev_day,
                  "references": [states[c] for c in CLOCKS], "input_state": input_state,
                  "outcome_references": outcome, "selection": "UNVERIFIED",
                  "decision_utc_ns": decision,
                  "decision_pacific": datetime.fromtimestamp(decision/NS, PACIFIC).isoformat()}
        budget.save("dates/"+day, record, kind="day")
        totals["dates"] += 1
        totals["input:"+input_state] += 1
        totals["outcome:"+outcome] += 1
        previous_close = refs.get("15:59")
        prior_references[day] = {"status": states["15:59"],
                                "identity": _contract_key(previous_close) if previous_close else None,
                                "eligible_ns": previous_close["decision_eligible_at_ns"] if previous_close else None}
    if pending is not None:
        raise IntegrityError("source rows extend beyond complete candidate calendar")
    return dict(totals)


def execute_once(boundary, receipt, op):
    """Dormant historical entry point; current policy rejects this operation."""
    _identity(op)
    _authorize(boundary, receipt, op, consumed=False)
    require_scope(boundary.active_root, op)
    parent = _path(boundary.active_root, DESTINATION.as_posix())
    parent.mkdir(parents=True, exist_ok=True)
    path = parent/op["operation_id"][:16]
    budget = _Budget(path, op)
    try:
        budget.check()
        used = receipt.consume(boundary, operation=OPERATION,
                               classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
                               required_scope={**dict(receipt.scope), **scope(op)})
        budget.external = used
        budget.used += used.stat().st_size
        if used.stat().st_size > LIMITS["receipt_ledger_bytes"]:
            raise IntegrityError("receipt use ledger exceeds budget")
        budget.save("started", {"operation_id": op["operation_id"], "status": "CLAIMED_ONCE_NO_RETRY"})
        counts = {}
        rows = _rows(boundary, receipt, op, budget, counts)
        try:
            totals = _census(rows, op, budget)
        finally:
            rows.close()
        _authorize(boundary, receipt, op, consumed=True)
        require_scope(boundary.active_root, op)
        if (totals["rows"] != sum(s["row_count"] for s in op["sources"])
                or totals["dates"] != op["maximum_dates"]
                or counts != {str(s["year"]): s["row_count"] for s in op["sources"]}):
            raise IntegrityError("complete census population did not reconcile")
        actual = sum(p.stat().st_size for p in path.rglob("*") if p.is_file()) + used.stat().st_size
        if actual != budget.used:
            raise IntegrityError("census output accounting differs")
        result = {"status": "COMPLETE_SOURCE_CENSUS", "operation_id": op["operation_id"],
                  "claim": CLAIM, "counts": counts, "totals": totals, "resources": budget.resources(),
                  "qualification": "UNAVAILABLE", "historical_decision_readiness": False,
                  "selection_evidence": "UNVERIFIED", "calendar_and_vintage_limits_retained": True}
        # Include the completion record and receipt ledger in the reported byte total.
        for _ in range(4):
            total = budget.used + len(canonical_bytes(result)) + 1
            if result["resources"]["retained_bytes"] == total:
                break
            result["resources"]["retained_bytes"] = total
        else:
            raise IntegrityError("completion accounting did not converge")
        budget.save("complete", result, kind="complete")
        return result
    except BaseException as exc:
        budget.fail(exc)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare",))
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    op = prepare(args.repository_root, args.output)
    print(json.dumps({"operation_id": op["operation_id"], "state": op["state"], "execution_enabled": False}))


if __name__ == "__main__":
    main()
