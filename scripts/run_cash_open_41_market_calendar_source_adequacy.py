"""Run the approved local-reference-only 2018-2022 calendar adequacy audit."""

from __future__ import annotations

import json
import os
import re
from collections import Counter, defaultdict
from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path
from zipfile import ZipFile

from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.cme_calendar_source_adequacy import read_xls_bytes, read_xls_cells
from futures_rebuild.data_layout import DataReleaseReceipt
from futures_rebuild.errors import IntegrityError
from futures_rebuild.historical_checkpoint_calendar import (
    MARKETS as CERTIFIED_CALENDAR_MARKETS,
    load_historical_checkpoint_calendar,
)


ROOT = Path(__file__).resolve().parents[1]
YEARS = tuple(range(2018, 2023))
ALL_MARKETS = (
    "6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S", "BTC", "CL", "ES",
    "ETH", "GC", "GF", "HE", "HG", "HO", "KE", "LE", "NG", "NQ", "PA",
    "PL", "RB", "RTY", "SI", "SR1", "SR3", "TN", "UB", "YM", "ZB", "ZC",
    "ZF", "ZL", "ZM", "ZN", "ZQ", "ZS", "ZT", "ZW",
)
REMAINING_MARKETS = tuple(x for x in ALL_MARKETS if x not in CERTIFIED_CALENDAR_MARKETS)
FOUNDATION_MANIFEST = Path(
    "manifests/data_releases/foundation/"
    "637f16b3c23c9f2215858f49754965738fe9c00095661d7a29d6877d566ae5e3.json"
)
OUTPUT_ROOT = Path("state/unpublished_evidence/cash_open_impulse_41_market_calendar_source_adequacy")
FAMILY_PATTERNS = {
    "CRYPTO": re.compile(r"^(bitcoin|cryptocurrency)$", re.I),
    "ENERGY_METALS": re.compile(r"^energy.*metals", re.I),
    "EQUITY_INDEX": re.compile(r"^equity products$", re.I),
    "FX": re.compile(r"^fx products$", re.I),
    "GRAIN_OILSEED": re.compile(r"^grain,?\s*oilseed", re.I),
    "INTEREST_RATES": re.compile(r"^interest rate products$", re.I),
    "LIVESTOCK": re.compile(r"^livestock$", re.I),
}
ASSET_TO_SCHEDULE_FAMILY = {
    "CRYPTO": "CRYPTO",
    "ENERGY": "ENERGY_METALS",
    "EQUITY_INDEX": "EQUITY_INDEX",
    "FX": "FX",
    "METALS": "ENERGY_METALS",
    "RATES": "INTEREST_RATES",
}


def _calendar_dates(year: int) -> list[str]:
    value = date(year, 1, 1)
    end = date(year, 12, 31)
    result: list[str] = []
    while value <= end:
        result.append(value.isoformat())
        value += timedelta(days=1)
    return result


def _foundation_asset_classes(boundary: RepoBoundary) -> tuple[dict[tuple[str, int], str], list[dict[str, object]]]:
    raw = json.loads((ROOT / FOUNDATION_MANIFEST).read_text(encoding="utf-8"))
    foundation = raw["embedded_documents"]["foundation_set.json"]
    classes: dict[tuple[str, int], str] = {}
    bindings: list[dict[str, object]] = []
    seen_releases: set[str] = set()
    for interval in foundation["intervals"]:
        market, year = interval["market"], interval["year"]
        if market not in ALL_MARKETS or year not in YEARS:
            continue
        receipt = DataReleaseReceipt.from_dict(interval["economics_release_receipt"])
        path = receipt.resolve_unique_filename("contract_economics.json", boundary)
        payload = json.loads(path.read_text(encoding="utf-8"))
        observed = sorted({item["asset_class"] for item in payload["records"]})
        if len(observed) != 1:
            raise IntegrityError(f"economics asset class is ambiguous for {market} {year}")
        key = (market, year)
        if key in classes and classes[key] != observed[0]:
            raise IntegrityError(f"economics asset class drifted for {market} {year}")
        classes[key] = observed[0]
        if receipt.release_id not in seen_releases:
            bindings.append({
                "asset_class": observed[0],
                "file_sha256": sha256_file(path),
                "release_id": receipt.release_id,
            })
            seen_releases.add(receipt.release_id)
    return classes, sorted(bindings, key=lambda x: str(x["release_id"]))


def _family_labels(cells: object) -> dict[str, list[dict[str, object]]]:
    result: dict[str, list[dict[str, object]]] = defaultdict(list)
    for cell in cells:  # type: ignore[union-attr]
        if not isinstance(cell.value, str):
            continue
        normalized = " ".join(cell.value.split())
        for family, pattern in FAMILY_PATTERNS.items():
            if pattern.search(normalized):
                result[family].append({
                    "column": cell.column,
                    "label": normalized,
                    "row": cell.row,
                    "sheet": cell.sheet,
                })
    return dict(result)


def _workbook_inventory(boundary: RepoBoundary) -> tuple[list[dict[str, object]], dict[int, set[str]], str]:
    loaded = load_historical_checkpoint_calendar(boundary=boundary)
    receipt = loaded.capture_receipt
    manifest = receipt.verify(boundary)
    inventory: list[dict[str, object]] = []
    families_by_year: dict[int, set[str]] = defaultdict(set)
    for entry in manifest.files:
        path = receipt.resolve_file(entry.logical_path, boundary)
        if path.suffix.lower() == ".xls":
            cells = read_xls_cells(path)
            year = int(path.name[:4])
            labels = _family_labels(cells)
            families_by_year[year].update(labels)
            inventory.append({
                "cell_count": len(cells),
                "family_labels": labels,
                "logical_path": entry.logical_path,
                "sha256": entry.sha256,
                "sheets": sorted({cell.sheet for cell in cells}),
                "size": entry.size,
                "year": year,
            })
            continue
        if path.suffix.lower() != ".zip":
            raise IntegrityError("unexpected CME calendar capture file type")
        with ZipFile(path) as archive:
            for member in sorted(x for x in archive.namelist() if x.lower().endswith(".xls")):
                data = archive.read(member)
                cells = read_xls_bytes(data)
                match = re.search(r"(20\d{2})", Path(member).name)
                if match is None:
                    raise IntegrityError("annual CME workbook lacks a year")
                year = int(match.group(1))
                if year not in YEARS:
                    continue
                labels = _family_labels(cells)
                families_by_year[year].update(labels)
                inventory.append({
                    "archive_logical_path": entry.logical_path,
                    "cell_count": len(cells),
                    "family_labels": labels,
                    "member": member,
                    "member_sha256": sha256(data).hexdigest(),
                    "member_size": len(data),
                    "sheets": sorted({cell.sheet for cell in cells}),
                    "year": year,
                })
    return inventory, families_by_year, receipt.release_id


def build_audit() -> dict[str, object]:
    boundary = RepoBoundary(ROOT)
    inventory, families_by_year, capture_release_id = _workbook_inventory(boundary)
    asset_classes, economics_bindings = _foundation_asset_classes(boundary)
    market_years: list[dict[str, object]] = []
    reason_counts: Counter[str] = Counter()
    for market in REMAINING_MARKETS:
        for year in YEARS:
            dates = _calendar_dates(year)
            asset_class = asset_classes.get((market, year))
            if asset_class is None:
                reason = "NO_BOUND_MARKET_YEAR_ASSET_CLASS_REFERENCE"
                family = None
            elif asset_class == "AGRICULTURE":
                reason = "AGRICULTURE_ASSET_CLASS_DOES_NOT_BIND_GRAIN_VERSUS_LIVESTOCK_CME_ROW"
                family = None
            else:
                family = ASSET_TO_SCHEDULE_FAMILY.get(asset_class)
                if family is None:
                    reason = "ASSET_CLASS_HAS_NO_CME_SCHEDULE_FAMILY_RULE"
                elif family not in families_by_year.get(year, set()):
                    reason = "CME_FAMILY_ROW_NOT_PRESENT_FOR_YEAR"
                else:
                    reason = "EXACT_08_29_TO_11_01_OPEN_HALT_TRANSLATION_NOT_CERTIFIED"
            reason_counts[reason] += 1
            market_years.append({
                "asset_class": asset_class,
                "calendar_date_count": len(dates),
                "calendar_dates_sha256": sha256_json(dates),
                "market": market,
                "required_checkpoints_chicago": ["09:00", "10:30"],
                "required_dependency_horizon_chicago": ["08:29", "11:01"],
                "schedule_family": family,
                "session_state": "UNVERIFIED_FAIL_CLOSED",
                "unverified_checkpoint_session_count": len(dates) * 2,
                "unverified_reason": reason,
                "year": year,
            })
    core = {
        "authority": {
            "external_cost_usd": "0",
            "historical_price_rows_read": False,
            "provider_network_credentials_accessed": False,
            "publication_or_activation": False,
            "year_2025_accessed": False,
        },
        "bindings": {
            FOUNDATION_MANIFEST.as_posix(): sha256_file(ROOT / FOUNDATION_MANIFEST),
            "configs/research_universe_contract.json": sha256_file(ROOT / "configs/research_universe_contract.json"),
            "configs/tier1_historical_checkpoint_calendar_v5.json": sha256_file(ROOT / "configs/tier1_historical_checkpoint_calendar_v5.json"),
            "scripts/run_cash_open_41_market_calendar_source_adequacy.py": sha256_file(Path(__file__)),
            "src/futures_rebuild/cme_calendar_source_adequacy.py": sha256_file(ROOT / "src/futures_rebuild/cme_calendar_source_adequacy.py"),
        },
        "calendar_capture_release_id": capture_release_id,
        "checkpoint_session_evidence": market_years,
        "decision": "FAIL_INSUFFICIENT_FOR_EXACT_41_MARKET_CALENDAR_SUCCESSOR",
        "economics_reference_bindings": economics_bindings,
        "existing_certified_calendar_markets": list(CERTIFIED_CALENDAR_MARKETS),
        "family_label_coverage_by_year": {
            str(year): sorted(families_by_year.get(year, set())) for year in YEARS
        },
        "market_year_reason_counts": dict(sorted(reason_counts.items())),
        "remaining_market_count": len(REMAINING_MARKETS),
        "remaining_markets": list(REMAINING_MARKETS),
        "schema_version": "cash_open_41_market_calendar_source_adequacy/1.0.0",
        "source_workbook_count": len(inventory),
        "source_workbooks": inventory,
        "verdict_explanation": [
            "CME holiday workbooks contain the necessary broad family rows, but no bound translator proves exact open/halt state over both 08:29-09:31 and 10:00-11:01 dependency windows.",
            "The immutable economics references distinguish broad asset classes, but AGRICULTURE does not distinguish CME grain/oilseed rows from livestock rows.",
            "The existing authoritative calendar certifies only 08:30, 10:30, and 13:30 for 6E, CL, ES, and ZN; it cannot be reused as proof of the new 09:00 dependency window or the remaining markets.",
        ],
    }
    return {**core, "audit_id": sha256_json(core)}


def main() -> int:
    report = build_audit()
    target = ROOT / OUTPUT_ROOT / str(report["audit_id"]) / "calendar_source_adequacy.json"
    target.parent.mkdir(parents=True, exist_ok=False)
    encoded = canonical_bytes(report) + b"\n"
    with target.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({
        "audit_id": report["audit_id"],
        "decision": report["decision"],
        "market_year_reason_counts": report["market_year_reason_counts"],
        "output_path": target.relative_to(ROOT).as_posix(),
        "output_sha256": sha256_file(target),
        "source_workbook_count": report["source_workbook_count"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
