"""Source-bound scheduled intervals; publication evidence and assumptions stay separate."""
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime, time, timedelta
from hashlib import sha256
from pathlib import Path
from zipfile import ZipFile
from zoneinfo import ZoneInfo
import io
import json

from . import l0_alpha_candidate as c
from .canonical import sha256_file
from .cme_calendar_source_adequacy import read_xls_bytes, parse_family_schedule
from .cme_calendar_successor import FAMILY_PATTERN
from .errors import IntegrityError

VIEWS = ("evidence_only", "assumed_availability")
FAMILIES = {"ES": "EQUITY_INDEX", "NQ": "EQUITY_INDEX", "CL": "ENERGY", "GC": "METALS"}
CHICAGO = ZoneInfo("America/Chicago")
INPUT_PLAN = "state/unpublished_evidence/l0_alpha_gap_diagnosis_v2/plan.json"
TIMING = "state/unpublished_evidence/rolling_hour_predictive_v1/source_readiness_v1/calendar_timing.json"
NUMERIC = "state/unpublished_evidence/l0_alpha_gap_diagnosis_v2/calendar_numeric_evidence.json"
DIAGNOSIS = "state/unpublished_evidence/l0_alpha_gap_diagnosis_v2/diagnosis.json"
OUTPUT = "state/unpublished_evidence/l0_order_readiness_v1"
ARTIFACT = OUTPUT + "/calendar.json"


def calendar_inputs(root):
    plan = json.loads((root / INPUT_PLAN).read_text())
    return {"calendar_sources": plan["calendar_sources"],
            "evidence": {name: sha256_file(root / name) for name in (INPUT_PLAN, TIMING, NUMERIC, DIAGNOSIS)}}


def build_calendar(root, inputs):
    if calendar_inputs(root) != inputs:
        raise IntegrityError("calendar input manifest changed")
    timing = json.loads((root / TIMING).read_text())
    recorded = {(r["source_key"], r["member"], r["family"]): r for r in timing["results"]}
    expected = json.loads((root / NUMERIC).read_text())
    expected_schedules = {(Path(r["source"]).stem, r["member"], r["family"]): r for r in expected["parsed_schedules"]}
    schedules, checks = [], []
    for source, digest in inputs["calendar_sources"].items():
        path = Path(source)
        raw = path.read_bytes()
        if sha256(raw).hexdigest() != digest:
            raise IntegrityError("owned calendar source hash differs")
        if path.suffix == ".zip":
            with ZipFile(io.BytesIO(raw)) as archive:
                members = [(name, archive.read(name)) for name in sorted(archive.namelist()) if name.lower().endswith(".xls")]
        else:
            members = [(path.name, raw)]
        for member, data in members:
            cells = read_xls_bytes(data)
            for family in sorted(set(FAMILIES.values())):
                parsed = parse_family_schedule(cells, label_pattern=FAMILY_PATTERN[family])
                key = (path.stem, member, family)
                events = [asdict(e) | {"at": e.at.isoformat()} for e in parsed.events]
                if events != expected_schedules[key]["events"]:
                    raise IntegrityError("corrected calendar differs from saved source-cell diagnosis")
                publication = recorded[key]
                available = (int(datetime.fromisoformat(publication["recorded_capture_utc"]).timestamp()) * 10**9
                             if publication["cdx_content_digest_matches"] else None)
                segments = []
                times = sorted({e.at for e in parsed.events})
                for start, stop in zip(times, times[1:]):
                    state = "OPEN" if parsed.state_at(start) is True else "CLOSED"
                    a, b = (int(v.replace(tzinfo=CHICAGO).timestamp()) * 10**9 for v in (start, stop))
                    if segments and segments[-1]["state"] == state and segments[-1]["end_ns"] == a:
                        segments[-1]["end_ns"] = b
                    else:
                        segments.append({"start_ns": a, "end_ns": b, "state": state})
                # No terminal OPEN is extended without a subsequent explicit boundary.
                if times and parsed.state_at(times[-1]) is False:
                    stop = datetime.combine(times[-1].date() + timedelta(days=1), time())
                    segments.append({"start_ns": int(times[-1].replace(tzinfo=CHICAGO).timestamp()) * 10**9,
                                     "end_ns": int(stop.replace(tzinfo=CHICAGO).timestamp()) * 10**9, "state": "CLOSED"})
                schedules.append({"source": str(path), "source_sha256": digest, "member": member,
                    "member_sha256": sha256(data).hexdigest(), "family": family, "sheet": parsed.sheet,
                    "row_zero_based": parsed.row, "events": events, "segments": segments,
                    "available_at_ns": available, "availability_basis": "RECORDED_ARCHIVE_CAPTURE_CONTENT_MATCH_NOT_APPLICATION_RECEIPT",
                    "dates": sorted({d.isoformat() for d in parsed.calendar_dates})})
                checks.append({"source_key": path.stem, "member": member, "family": family, "schedule_matches_diagnosis": True})
        if sha256_file(path) != digest:
            raise IntegrityError("calendar source changed during read")
    # Preserve the separately evidenced mourning closure without inventing its publication timestamp.
    day = "2018-12-05"
    a = int(datetime(2018, 12, 5, 8, 30, tzinfo=CHICAGO).timestamp()) * 10**9
    b = int(datetime(2018, 12, 5, 17, tzinfo=CHICAGO).timestamp()) * 10**9
    schedules.append({"family": "EQUITY_INDEX", "dates": [day], "segments": [{"start_ns": a, "end_ns": b, "state": "CLOSED"}],
        "available_at_ns": None, "availability_basis": "PUBLIC_ANNOUNCEMENT_DATE_RECORDED_WITHOUT_VERIFIED_CAPTURE",
        "source": "https://www.cmegroup.com/media-room/press-releases/2018/12/02/cme_group_u_s_equityinterestratemarketstoclosefornationaldayofmo.html",
        "source_sha256": sha256_file(root / DIAGNOSIS), "evidence_role": "CLOSURE_CORROBORATED_BY_SAVED_DIAGNOSIS;HASH_IS_DIAGNOSTIC_NOT_ANNOUNCEMENT_BYTES"})
    return c.bind({"schedules": schedules, "source_inputs": inputs, "checks": checks,
        "regular_hours": "NO_INDEPENDENTLY_BOUND_REGULAR_HOURS_COVERAGE;UNREPRESENTED_DATES_UNKNOWN",
        "views": list(VIEWS), "assumption": "APPLICABLE_DAILY_SCHEDULE_AVAILABLE_BY_00_00_PACIFIC;NO_CONTENT_OR_COVERAGE_IMPUTATION",
        "calendar_completeness_certified": False, "active": False}, "calendar_id")


def compile_calendar(calendar, view):
    if view not in VIEWS:
        raise IntegrityError("explicit declared calendar view required")
    c.identity(calendar, "calendar_id")
    days = defaultdict(list)
    for item in calendar["schedules"]:
        if item["family"] not in FAMILIES.values():
            raise IntegrityError("undeclared calendar family")
        c.old.profit.digest(item["source_sha256"])
        available = item.get("available_at_ns")
        if available is not None and (type(available) is not int or available < 0):
            raise IntegrityError("invalid calendar availability timestamp")
        previous = None
        for segment in item["segments"]:
            a, b = segment["start_ns"], segment["end_ns"]
            if (type(a) is not int or type(b) is not int or not 0 <= a < b or
                    segment["state"] not in ("OPEN", "CLOSED") or previous is not None and a < previous):
                raise IntegrityError("invalid or overlapping scheduled intervals")
            previous = b
        for day in item["dates"]:
            datetime.fromisoformat(day)
            days[(item["family"], day)].append(item)
    return {"calendar_id": calendar["calendar_id"], "view": view, "days": days}


def entry_permission(record, context):
    at, entry, exit_at = (record[k] for k in ("decision_ns", "entry_ns", "exit_ns"))
    allowed, blocked, unavailable = [], [], 0
    known_segments = []
    for item in context["days"].get((FAMILIES[record["market"]], record["trade_date"]), []):
        available = item["available_at_ns"]
        if context["view"] == "assumed_availability":
            available = int(datetime.fromisoformat(record["trade_date"]).replace(tzinfo=c.ZONE).timestamp()) * 10**9
        if available is None or available > at:
            unavailable += 1
            continue
        for segment in item["segments"]:
            a, b = segment["start_ns"], segment["end_ns"]
            if b <= at or a > exit_at:
                continue
            known_segments.append(segment)
            if segment["state"] == "CLOSED" and (a <= at < b or a <= exit_at and b > entry):
                blocked.append(item["source_sha256"])
            elif segment["state"] == "OPEN" and a <= at and a <= entry and exit_at < b:
                allowed.append(item["source_sha256"])
    conflict = any(a["state"] != b["state"] and max(a["start_ns"], b["start_ns"], at) <
                   min(a["end_ns"], b["end_ns"], exit_at + 1) for a in known_segments for b in known_segments)
    state = "UNKNOWN" if conflict else "BLOCKED" if blocked else "ALLOWED" if allowed else "UNKNOWN"
    return {"state": state, "reason": "CONFLICTING_SCHEDULES" if conflict else "SCHEDULED_CLOSURE" if blocked
            else "SUPPORTED_OPEN_INTERVAL" if allowed else "CALENDAR_COVERAGE_OR_AVAILABILITY_UNKNOWN",
            "evidence": sorted(set(allowed + blocked)), "unavailable_sources": unavailable, "view": context["view"]}
