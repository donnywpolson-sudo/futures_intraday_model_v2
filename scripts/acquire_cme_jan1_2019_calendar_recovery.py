"""Acquire one exact archived CME schedule into immutable inactive staging."""

from __future__ import annotations

import hashlib
import json
import os
import urllib.request
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.cme_calendar_source_adequacy import read_xls_bytes
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
ORIGINAL_URL = (
    "http://www.cmegroup.com:80/tools-information/holiday-calendar/files/"
    "2019-new-years-holiday-schedule-compact.xls"
)
ARCHIVE_TIMESTAMP = "20180508163804"
REPLAY_URL = f"https://web.archive.org/web/{ARCHIVE_TIMESTAMP}id_/{ORIGINAL_URL}"
EXPECTED_SHA256 = "2684a5f1b3a9f65802f6911ca6089e2cb68c3cdf2520dfaf3cdcf3105328c188"
EXPECTED_TITLE = "CME Group Globex New Years Holiday Schedule: December 31, 2018 - January 2, 2019"
FAMILY_LABELS = {
    "EQUITY_INDEX": "Equity",
    "CRYPTO": "Bitcoin",
    "RATES": "Interest Rate",
    "FX": "FX",
    "ENERGY_METALS": "Energy, Metals & DME",
    "GRAIN_OILSEED": "Grain & Oilseed",
    "LIVESTOCK": "Livestock",
}
LOCAL_SEARCH = Path(
    "state/unpublished_evidence/cash_open_impulse_jan1_2019_local_source_search/"
    "37c90ab2c71849410c510dd930cfbad2c8b4f10d2fb95f4a3e468c94ce32adcb/"
    "source_selection.json"
)


def _validate(raw: bytes) -> dict[str, object]:
    if hashlib.sha256(raw).hexdigest() != EXPECTED_SHA256:
        raise IntegrityError("archived CME workbook hash differs from inspected candidate")
    cells = read_xls_bytes(raw)
    rows: dict[int, dict[int, str | float]] = {}
    for cell in cells:
        rows.setdefault(cell.row, {})[cell.column] = cell.value
    title = " ".join(str(rows.get(0, {}).get(1, "")).split())
    if title != EXPECTED_TITLE:
        raise IntegrityError("archived CME workbook title does not bind the target holiday")
    if " ".join(str(rows.get(2, {}).get(2, "")).split()) != "Tuesday, Jan 1":
        raise IntegrityError("archived CME workbook does not bind January 1, 2019")
    observed: dict[str, dict[str, object]] = {}
    for family, label in FAMILY_LABELS.items():
        matches = [
            (row, values) for row, values in rows.items()
            if " ".join(str(values.get(0, "")).split()).casefold() == label.casefold()
        ]
        if len(matches) != 1:
            raise IntegrityError(f"archived CME workbook lacks one exact {family} row")
        row, values = matches[0]
        disposition = " ".join(str(values.get(2, "")).split())
        if disposition.casefold() != "closed for new year's":
            raise IntegrityError(f"archived CME workbook does not close {family} on January 1")
        observed[family] = {"row": row, "label": label, "january_1_disposition": disposition}
    return {"sheet": cells[0].sheet, "title": title, "family_rows": observed}


def main() -> int:
    request = urllib.request.Request(REPLAY_URL, headers={"User-Agent": "futures-research-reference-recovery/1.0"})
    with urllib.request.urlopen(request, timeout=30) as response:
        raw = response.read(200_000)
        status = int(response.status)
        content_type = response.headers.get("Content-Type", "")
    if status != 200 or len(raw) >= 200_000:
        raise IntegrityError("archived CME workbook response violated bounds")
    validation = _validate(raw)
    plan_core = {
        "schema_version": "cme_jan1_2019_calendar_recovery_acquisition/1.0.0",
        "archive_timestamp": ARCHIVE_TIMESTAMP,
        "original_url": ORIGINAL_URL,
        "replay_url": REPLAY_URL,
        "expected_raw_sha256": EXPECTED_SHA256,
        "script_sha256": sha256_file(Path(__file__)),
        "predecessor_local_search_sha256": sha256_file(ROOT / LOCAL_SEARCH),
    }
    acquisition_id = sha256_json(plan_core)
    stage = ROOT / "state/data_publication_staging/cash_open_impulse_jan1_2019_calendar_recovery" / acquisition_id
    stage.mkdir(parents=True, exist_ok=False)
    raw_path = stage / "2019-new-years-holiday-schedule-compact.xls"
    with raw_path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    sidecar = {
        "schema_version": "immutable_raw_reference_sidecar/1.0.0",
        "acquisition_id": acquisition_id,
        "raw_filename": raw_path.name,
        "raw_sha256": EXPECTED_SHA256,
        "raw_size": len(raw),
        "archive_timestamp": ARCHIVE_TIMESTAMP,
        "original_url": ORIGINAL_URL,
        "replay_url": REPLAY_URL,
    }
    sidecar_path = stage / "2019-new-years-holiday-schedule-compact.xls.sidecar.json"
    sidecar_path.write_bytes(canonical_bytes(sidecar) + b"\n")
    core = {
        **plan_core,
        "acquisition_id": acquisition_id,
        "decision": "PASS_EXACT_AUTHORITATIVE_SCHEDULE_ACQUIRED_INACTIVE",
        "http": {"status": status, "content_type": content_type},
        "raw": {
            "path": raw_path.relative_to(ROOT).as_posix(),
            "sha256": sha256_file(raw_path),
            "size": len(raw),
        },
        "sidecar": {
            "path": sidecar_path.relative_to(ROOT).as_posix(),
            "sha256": sha256_file(sidecar_path),
        },
        "validation": validation,
        "request_accounting": {
            "maximum_authorized_requests": 20,
            "conservative_requests_before_final_download": 13,
            "final_download_requests": 1,
            "conservative_total_requests": 14,
        },
        "authority": {
            "external_cost_usd": "0", "price_rows_read": False,
            "credentials_accessed": False, "year_2025_accessed": False,
            "published": False, "activated": False, "active_data_mutated": False,
        },
    }
    record = {**core, "record_id": sha256_json(core)}
    evidence = ROOT / "state/unpublished_evidence/cash_open_impulse_jan1_2019_calendar_recovery" / str(record["record_id"])
    evidence.mkdir(parents=True, exist_ok=False)
    record_path = evidence / "acquisition_record.json"
    record_path.write_bytes(canonical_bytes(record) + b"\n")
    print(json.dumps({
        "record_id": record["record_id"], "acquisition_id": acquisition_id,
        "decision": record["decision"], "raw_sha256": record["raw"]["sha256"],
        "record_path": record_path.relative_to(ROOT).as_posix(),
        "record_sha256": sha256_file(record_path),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
