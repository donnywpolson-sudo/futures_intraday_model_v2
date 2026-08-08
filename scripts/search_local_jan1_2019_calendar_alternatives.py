"""Search immutable local CME reference releases for the January 1, 2019 schedule."""

from __future__ import annotations

import html
import hashlib
import json
import os
import re
from pathlib import Path
from zipfile import ZipFile

from pypdf import PdfReader

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.cme_calendar_source_adequacy import (
    parse_family_schedule,
    read_xls_bytes,
    read_xls_cells,
)
from futures_rebuild.cme_calendar_successor import FAMILY_PATTERN


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_ROOT = ROOT / "manifests/data_releases/reference"
CALENDAR_ROOT = ROOT / "data/reference/exchange_calendars"
OUTPUT_ROOT = ROOT / "state/unpublished_evidence/cash_open_impulse_jan1_2019_local_source_search"
TARGET = "2019-01-01"
RELEVANT_RELEASE_KINDS = {
    "cme_historical_checkpoint_calendar_capture",
    "cme_historical_holiday_schedule_capture",
    "cme_historical_notice_attachment_capture",
    "cme_historical_notice_document_union_capture",
    "cme_historical_notice_pagination_capture",
}
EXACT_DATE = re.compile(r"(?:January|Jan\.?)\s+1,?\s+2019|2019-01-01", re.I)
NEW_YEAR = re.compile(r"New\s+Year", re.I)


def _manifest_inventory() -> tuple[list[dict[str, object]], dict[str, dict[str, object]]]:
    inventory: list[dict[str, object]] = []
    payloads: dict[str, dict[str, object]] = {}
    for path in sorted(MANIFEST_ROOT.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, ValueError):
            continue
        files = payload.get("files", [])
        if not any("exchange_calendars/" in str(item.get("logical_path", "")) for item in files):
            continue
        release_id = str(payload["release_id"])
        payloads[release_id] = payload
        inventory.append({
            "release_id": release_id,
            "release_kind": payload.get("release_kind"),
            "manifest_path": path.relative_to(ROOT).as_posix(),
            "manifest_sha256": sha256_file(path),
            "file_count": len(files),
            "in_exact_search_scope": payload.get("release_kind") in RELEVANT_RELEASE_KINDS,
        })
    return inventory, payloads


def _capture_responses(payload: dict[str, object]) -> list[dict[str, object]]:
    embedded = payload.get("embedded_documents", {})
    receipt = embedded.get("capture_receipt.json", {}) if isinstance(embedded, dict) else {}
    responses = receipt.get("responses", []) if isinstance(receipt, dict) else []
    return [item for item in responses if isinstance(item, dict)]


def _resolve(release_id: str, logical_path: str) -> Path:
    return CALENDAR_ROOT / release_id / Path(logical_path).name


def _scan_workbooks(payload: dict[str, object]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    inspected: list[dict[str, object]] = []
    matches: list[dict[str, object]] = []
    release_id = str(payload["release_id"])
    for item in payload.get("files", []):
        logical = str(item["logical_path"])
        path = _resolve(release_id, logical)
        if path.suffix.lower() == ".xls":
            cells = read_xls_cells(path)
            sources = [(logical, str(item["sha256"]), cells)]
        elif path.suffix.lower() == ".zip":
            sources = []
            with ZipFile(path) as archive:
                for member in sorted(name for name in archive.namelist() if name.lower().endswith(".xls")):
                    raw = archive.read(member)
                    sources.append((f"{logical}::{member}", hashlib.sha256(raw).hexdigest(), read_xls_bytes(raw)))
        else:
            continue
        for source, source_hash, cells in sources:
            covered_families = []
            for family, pattern in FAMILY_PATTERN.items():
                try:
                    schedule = parse_family_schedule(cells, label_pattern=pattern)
                except Exception:
                    continue
                if TARGET in {value.isoformat() for value in schedule.calendar_dates}:
                    covered_families.append(family)
            complete = set(covered_families) == set(FAMILY_PATTERN)
            inspected.append({
                "source": source, "source_binding": source_hash,
                "target_family_coverage": sorted(covered_families),
                "complete_target_family_coverage": complete,
            })
            if complete:
                matches.append({"source": source, "source_binding": source_hash, "classification": "EXACT_COMPLETE_FAMILY_SCHEDULE_CANDIDATE"})
    return inspected, matches


def _pdf_text(path: Path) -> str:
    return "\n".join((page.extract_text() or "") for page in PdfReader(path).pages)


def build_record() -> dict[str, object]:
    inventory, payloads = _manifest_inventory()
    inspected: list[dict[str, object]] = []
    candidates: list[dict[str, object]] = []
    release_bindings: list[dict[str, object]] = []
    for item in inventory:
        if not item["in_exact_search_scope"]:
            continue
        release_id = str(item["release_id"])
        payload = payloads[release_id]
        release_bindings.append(item)
        kind = payload.get("release_kind")
        if kind == "cme_historical_checkpoint_calendar_capture":
            observed, matches = _scan_workbooks(payload)
            inspected.extend(observed)
            candidates.extend(matches)
            continue
        responses = _capture_responses(payload)
        if kind == "cme_historical_notice_pagination_capture":
            for file_item in payload.get("files", []):
                logical = str(file_item["logical_path"])
                if "holiday-page-" not in logical:
                    continue
                page = json.loads(_resolve(release_id, logical).read_text(encoding="utf-8"))
                for result in page.get("results", []):
                    text = html.unescape(re.sub(r"<[^>]+>", " ", str(result.get("text", ""))))
                    if EXACT_DATE.search(text) and NEW_YEAR.search(text):
                        candidates.append({"source": logical, "url": result.get("url"), "classification": "NOTICE_INDEX_EXACT_DATE_CANDIDATE"})
            inspected.append({"release_id": release_id, "kind": kind, "holiday_page_count": sum("holiday-page-" in str(x["logical_path"]) for x in payload.get("files", []))})
            continue
        selected = []
        for response in responses:
            url = str(response.get("url", ""))
            title = str(response.get("metadata_title", ""))
            if kind == "cme_historical_notice_document_union_capture":
                if not re.search(r"/(?:2018|2019)/", url):
                    continue
                selected.append(response)
                path = _resolve(release_id, str(response["logical_path"]))
                text = path.read_text(encoding="utf-8", errors="replace")
            elif kind in {"cme_historical_notice_attachment_capture", "cme_historical_holiday_schedule_capture"}:
                if kind == "cme_historical_notice_attachment_capture" and not re.search(r"2018|2019", url):
                    continue
                path = _resolve(release_id, str(response["logical_path"]))
                if path.suffix.lower() != ".pdf":
                    selected.append(response)
                    text = ""
                else:
                    selected.append(response)
                    text = _pdf_text(path)
            else:
                continue
            exact = bool(EXACT_DATE.search(text + " " + title + " " + url))
            related = exact and bool(NEW_YEAR.search(text + " " + title + " " + url))
            if related:
                candidates.append({"source": str(response["logical_path"]), "url": url, "sha256": response.get("sha256"), "classification": "EXACT_DATE_DOCUMENT_CANDIDATE"})
        inspected.append({"release_id": release_id, "kind": kind, "selected_document_count": len(selected), "selected_document_set_sha256": sha256_json(sorted(str(x.get("sha256", "")) for x in selected))})
    core = {
        "schema_version": "cash_open_impulse_jan1_2019_local_source_search/1.0.0",
        "target_trade_date": TARGET,
        "decision": "PASS_EXACT_LOCAL_SOURCE_SELECTED" if candidates else "FAIL_NO_EXACT_LOCAL_SOURCE",
        "selected_source": candidates[0] if len(candidates) == 1 else None,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "reference_release_inventory": inventory,
        "exact_search_release_bindings": release_bindings,
        "inspection_evidence": inspected,
        "authority": {
            "external_cost_usd": "0", "price_rows_read": False,
            "provider_network_credentials_accessed": False, "year_2025_accessed": False,
            "published": False, "activated": False,
        },
        "conclusion": (
            "No immutable local CME artifact explicitly binds the January 1, 2019 "
            "New Year schedule to product-family open/halt states. The prepared "
            "calendar successor must retain its 80 fail-closed abstentions."
        ),
        "bindings": {
            "scripts/search_local_jan1_2019_calendar_alternatives.py": sha256_file(Path(__file__)),
            "state/unpublished_evidence/cash_open_impulse_41_market_calendar_successor_preparation/a6365e1f31be73b7039f5425d261f9f4287b54f598f8cd96bea6af2e70429584/historical_calendar_successor.json": sha256_file(ROOT / "state/unpublished_evidence/cash_open_impulse_41_market_calendar_successor_preparation/a6365e1f31be73b7039f5425d261f9f4287b54f598f8cd96bea6af2e70429584/historical_calendar_successor.json"),
        },
    }
    return {**core, "record_id": sha256_json(core)}


def main() -> int:
    record = build_record()
    target = OUTPUT_ROOT / str(record["record_id"]) / "source_selection.json"
    target.parent.mkdir(parents=True, exist_ok=False)
    with target.open("xb") as stream:
        stream.write(canonical_bytes(record) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({
        "record_id": record["record_id"], "decision": record["decision"],
        "candidate_count": record["candidate_count"],
        "output_path": target.relative_to(ROOT).as_posix(),
        "output_sha256": sha256_file(target),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
