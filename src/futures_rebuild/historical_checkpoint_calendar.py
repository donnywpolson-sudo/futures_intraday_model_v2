"""Immutable CME-authored checkpoint calendar for the Tier 1 V5 trial.

This module intentionally certifies only the three preregistered Chicago
decision checkpoints.  The archived CME holiday workbooks are authoritative
for exceptional dates, while their repeated regular-close rows anchor the
ordinary weekday rule.  It does not claim a complete intraday session tape.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, fsync_directory, sha256_file, sha256_json
from .data_layout import DataReleaseManifest, DataReleaseReceipt, PhasePublisher
from .errors import IntegrityError


CAPTURE_KIND = "cme_historical_checkpoint_calendar_capture"
CAPTURE_SCHEMA = "1.0.0"
CALENDAR_KIND = "verified_historical_checkpoint_calendar"
CALENDAR_SCHEMA = "1.0.0"
INDEX_KIND = "historical_checkpoint_calendar_index"
INDEX_SCHEMA = "1.0.0"
POINTER_SCHEMA = "tier1_historical_checkpoint_calendar_pointer/1.0.0"
CHECKPOINTS = ("08:30", "10:30", "13:30")
MARKETS = ("6E", "CL", "ES", "ZN")
START = date(2018, 1, 1)
END = date(2022, 12, 31)
POINTER_PATH = Path("configs/tier1_historical_checkpoint_calendar_v5.json")


@dataclass(frozen=True)
class ArchivedCmeSource:
    key: str
    timestamp: str
    original_url: str
    cdx_digest: str

    @property
    def replay_url(self) -> str:
        return (
            f"https://web.archive.org/web/{self.timestamp}id_/"
            f"{self.original_url}"
        )

    @property
    def suffix(self) -> str:
        return Path(self.original_url).suffix.lower()


def authoritative_sources() -> tuple[ArchivedCmeSource, ...]:
    """Return the frozen, minimal CME-authored source inventory."""

    raw = (
        ("2018_new_year", "20180106225726", "http://www.cmegroup.com:80/tools-information/holiday-calendar/files/2018-new-years-holiday-schedule.xls", "ERL5ECBOJQHSUAWLTNSV5DMRHQA2HEQX"),
        ("2018_mlk", "20180508162432", "http://www.cmegroup.com:80/tools-information/holiday-calendar/files/2018-martin-luther-king-holiday-schedule.xls", "VCBIZNDLHW3VWTNWVXI3RO6NDWIX4GDL"),
        ("2018_presidents", "20180508164640", "http://www.cmegroup.com:80/tools-information/holiday-calendar/files/2018-presidents-day-holiday-schedule.xls", "IBH6G7QLQNU7LNUCHNHCG43P2QRMHH2R"),
        ("2018_good_friday", "20180106225628", "http://www.cmegroup.com:80/tools-information/holiday-calendar/files/2018-good-friday-holiday-schedule.xls", "JJWI6MLNXWG4WYPXJTKAGGI6322LLQMM"),
        ("2018_memorial", "20180508163829", "http://www.cmegroup.com:80/tools-information/holiday-calendar/files/2018-memorial-day-holiday-schedule.xls", "VWSUZ7S7ZFV2KCXAP243RGIBLARIDADN"),
        ("2018_independence", "20180508160456", "http://www.cmegroup.com:80/tools-information/holiday-calendar/files/2018-4th-of-july-holiday-schedule.xls", "4UG4E5IXS4DHTKQJZBTW4PYGKBVNRRNQ"),
        ("2018_labor", "20180508162427", "http://www.cmegroup.com:80/tools-information/holiday-calendar/files/2018-labor-day-holiday-schedule.xls", "VSSZL6KJKRDSO2GXRQGDO3ZLDNYG4D6C"),
        ("2018_thanksgiving", "20180508162447", "http://www.cmegroup.com:80/tools-information/holiday-calendar/files/2018-thanksgiving-holiday-schedule.xls", "Q6X5UU5JRZ7GGMGKGYPM7BDHKW3HDREB"),
        ("2018_christmas", "20180106225502", "http://www.cmegroup.com:80/tools-information/holiday-calendar/files/2018-christmas-holiday-schedule.xls", "HFGF4IZLW5BL7WIEKVJOPJ474E7JJEBV"),
        ("2019_annual", "20210126094837", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2019-holiday-calendars.zip", "AKTAO6IUIFZMU3WMCMPB2AZU67F372Q5"),
        ("2020_annual", "20260730111834", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2020-holiday-calendars.zip", "HCBYC3GF6NEPJX4QCW4KAQSBY5I62RXP"),
        ("2021_new_year", "20250818185509", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2021-new-years-holiday-schedule.xls", "OW3SQX6B7LVQCJQERPQQBTBACP2UJR4A"),
        ("2021_mlk", "20220917190720", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2021-mlk-day-holiday-schedule.xls", "2OKYFXVGZK2GWX32QEKCWNTLBWDJ5G7S"),
        ("2021_presidents", "20210214015315", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2021-presidents-day-holiday-schedule.xls", "FWTMYPOUN3KA3I3USFBPH236F6EGU3KJ"),
        ("2021_good_friday", "20210402141803", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2021-good-friday-holiday-schedule.xls", "CBF5LQWGXM2J4ZPFVQ2PN346GM7I3QCP"),
        ("2021_memorial", "20220917171238", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2021-memorial-day-holiday-schedule.xls", "HI3BRLFJZ67ETOU7NQIRZ25ERE6GR44G"),
        ("2021_independence", "20210702051538", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2021-independence-day-holiday-schedule-compact.xls", "EWRPK7RSKBO4GN6ISYS3L457VBSXGR7R"),
        ("2021_labor", "20210903210804", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2021-labor-day-holiday-schedule.xls", "CUCW2U2RLUWDAVVH4EWBTENR6HYE5D3A"),
        ("2021_thanksgiving", "20220917175604", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2021-thanksgiving-holiday-schedule.xls", "KEQGNHM4L4YP6TK5F733OMAL5MHI3KR5"),
        ("2021_christmas", "20211223222653", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2021-christmas-holiday-schedule-compact.xls", "7SLEWUE65GJH6XQ6XVUOY522AQDDU245"),
        ("2022_new_year", "20220102130655", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2022-new-years-holiday-schedule.xls", "SHOAU27T6QR2TAI6VVT62V7Y5YFDWD4F"),
        ("2022_mlk", "20220117212230", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2022-mlk-day-holiday-schedule.xls", "4BKOPZUW5UNOOW3MGRV4GHVDIRDZET23"),
        ("2022_presidents", "20220704073810", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2022-presidents-day-holiday-schedule.xls", "ZSFQE2NYGYQCQU4TX6LNQPCR57BBEHIQ"),
        ("2022_good_friday", "20220704065501", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2022-good-friday-holiday-schedule.xls", "65DMHYADPN2XQTFTYAZMPKTLRK3DDVL3"),
        ("2022_memorial", "20220704065438", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2022-memorial-day-holiday-schedule.xls", "W3BQLD6E3KNOH4LPFCI3XXFCIGPVUD3U"),
        ("2022_juneteenth", "20220620200210", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2022-juneteenth-holiday-schedule.xls", "QDXISNUVLQ4ZAP7I5RR5EREL66ZG5RQF"),
        ("2022_independence", "20220704065450", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2022-independence-day-holiday-schedule.xls", "SHATY7VPXGQALBA2W6GXFYO43QV25PNC"),
        ("2022_labor", "20220704065441", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2022-labor-day-holiday-schedule.xls", "45ME2LXKYJV2UDDE5BZY4RAV4QUPYH2Y"),
        ("2022_thanksgiving", "20221122060801", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2022-thanksgiving-holiday-schedule.xls", "IEUI6WROREDGEL3HWOU3RATIHSCMCL4P"),
        ("2022_christmas", "20220704065430", "https://www.cmegroup.com/tools-information/holiday-calendar/files/2022-christmas-holiday-schedule.xls", "DGSXESWX6ZIXQQ2PVUFKNW2OAM455GEC"),
    )
    return tuple(ArchivedCmeSource(*item) for item in raw)


FULL_CLOSE = {
    "2018-01-01": "2018_new_year", "2018-03-30": "2018_good_friday", "2018-12-25": "2018_christmas",
    "2019-01-01": "2019_annual", "2019-04-19": "2019_annual", "2019-12-25": "2019_annual",
    "2020-01-01": "2019_annual", "2020-04-10": "2020_annual", "2020-12-25": "2020_annual",
    "2021-01-01": "2021_new_year", "2021-04-02": "2021_good_friday", "2021-12-24": "2021_christmas",
    "2022-04-15": "2022_good_friday", "2022-12-26": "2022_christmas",
}

EARLY_CLOSE = {
    "2018-01-15": "2018_mlk", "2018-02-19": "2018_presidents", "2018-05-28": "2018_memorial",
    "2018-07-04": "2018_independence", "2018-09-03": "2018_labor", "2018-11-22": "2018_thanksgiving",
    "2018-11-23": "2018_thanksgiving", "2018-12-24": "2018_christmas",
    "2019-01-21": "2019_annual", "2019-02-18": "2019_annual", "2019-05-27": "2019_annual",
    "2019-07-04": "2019_annual", "2019-09-02": "2019_annual", "2019-11-28": "2019_annual",
    "2019-11-29": "2019_annual", "2019-12-24": "2019_annual",
    "2020-01-20": "2020_annual", "2020-02-17": "2020_annual", "2020-05-25": "2020_annual",
    "2020-07-03": "2020_annual", "2020-09-07": "2020_annual", "2020-11-26": "2020_annual",
    "2020-11-27": "2020_annual", "2020-12-24": "2020_annual",
    "2021-01-18": "2021_mlk", "2021-02-15": "2021_presidents", "2021-05-31": "2021_memorial",
    "2021-07-05": "2021_independence", "2021-09-06": "2021_labor", "2021-11-25": "2021_thanksgiving",
    "2021-11-26": "2021_thanksgiving",
    "2022-01-17": "2022_mlk", "2022-02-21": "2022_presidents", "2022-05-30": "2022_memorial",
    "2022-06-20": "2022_juneteenth", "2022-07-04": "2022_independence", "2022-09-05": "2022_labor",
    "2022-11-24": "2022_thanksgiving", "2022-11-25": "2022_thanksgiving",
}

# Exact product-group deviations visible in the CME workbooks.  These are the
# cases a single US-holiday label gets wrong at one of V5's decision instants.
MARKET_CHECKPOINT_OVERRIDES: dict[tuple[str, str], tuple[bool, bool, bool, str]] = {
    ("2018-07-03", "ES"): (True, True, False, "2018_independence"),
    ("2019-07-03", "ES"): (True, True, False, "2019_annual"),
    ("2021-04-02", "6E"): (True, False, False, "2021_good_friday"),
    ("2021-04-02", "ZN"): (True, False, False, "2021_good_friday"),
    ("2022-01-17", "6E"): (True, True, True, "2022_mlk"),
    ("2022-02-21", "6E"): (True, True, True, "2022_presidents"),
    ("2022-05-30", "6E"): (True, True, True, "2022_memorial"),
    ("2022-06-20", "6E"): (True, True, True, "2022_juneteenth"),
    ("2022-07-04", "6E"): (True, True, True, "2022_independence"),
    ("2022-09-05", "6E"): (True, True, True, "2022_labor"),
    ("2022-11-24", "6E"): (True, True, True, "2022_thanksgiving"),
}


def _read_canonical(path: Path, *, name: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{name} is unreadable") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{name} is not canonical JSON")
    return payload


def _dates() -> Sequence[date]:
    result = []
    value = START
    while value <= END:
        result.append(value)
        value += timedelta(days=1)
    return result


def build_checkpoint_sessions() -> list[dict[str, object]]:
    sessions: list[dict[str, object]] = []
    for market in MARKETS:
        for trade_date in _dates():
            text = trade_date.isoformat()
            override = MARKET_CHECKPOINT_OVERRIDES.get((text, market))
            if override is not None:
                values = list(override[:3])
                evidence = override[3]
                state = "CME_MARKET_SPECIFIC_CHECKPOINT_SCHEDULE"
            elif trade_date.weekday() >= 5:
                state = "WEEKEND_CLOSED"
                evidence = None
                values = [False, False, False]
            elif text in FULL_CLOSE:
                state = "CME_FULL_CLOSE"
                evidence = FULL_CLOSE[text]
                values = [False, False, False]
            elif text in EARLY_CLOSE:
                state = "CME_EARLY_CLOSE_BEFORE_13_30"
                evidence = EARLY_CLOSE[text]
                values = [True, True, False]
            else:
                state = "CME_REGULAR_WEEKDAY"
                evidence = f"{trade_date.year}_annual_or_distributed_schedule_set"
                values = [True, True, True]
            sessions.append(
                {
                    "checkpoint_open": dict(zip(CHECKPOINTS, values)),
                    "evidence_key": evidence,
                    "market": market,
                    "state_class": state,
                    "trade_date": text,
                }
            )
    return sessions


def _receipt(value: object, *, name: str) -> DataReleaseReceipt:
    if not isinstance(value, Mapping):
        raise IntegrityError(f"{name} receipt is invalid")
    try:
        return DataReleaseReceipt.from_dict(value)
    except Exception as exc:
        raise IntegrityError(f"{name} receipt is invalid") from exc


@dataclass(frozen=True)
class LoadedHistoricalCheckpointCalendar:
    index_receipt: DataReleaseReceipt
    calendar_receipt: DataReleaseReceipt
    capture_receipt: DataReleaseReceipt
    sessions: Mapping[tuple[str, str], Mapping[str, bool]]


def load_historical_checkpoint_calendar(
    *, boundary: RepoBoundary, pointer_path: Path | None = None,
) -> LoadedHistoricalCheckpointCalendar:
    path = pointer_path or boundary.active_root / POINTER_PATH
    pointer = _read_canonical(path, name="historical checkpoint calendar pointer")
    if set(pointer) != {"calendar_index_receipt", "schema_version"} or pointer.get("schema_version") != POINTER_SCHEMA:
        raise IntegrityError("historical checkpoint calendar pointer schema is invalid")
    index_receipt = _receipt(pointer["calendar_index_receipt"], name="calendar index")
    index_manifest = index_receipt.verify(boundary)
    if (
        index_manifest.release_kind != INDEX_KIND
        or index_manifest.schema_version != INDEX_SCHEMA
        or set(index_manifest.embedded_documents) != {"historical_checkpoint_calendar_index.json"}
        or index_manifest.files
    ):
        raise IntegrityError("historical checkpoint calendar index kind is invalid")
    index = index_receipt.embedded_document("historical_checkpoint_calendar_index.json", boundary)
    if not isinstance(index, dict):
        raise IntegrityError("historical checkpoint calendar index is invalid")
    index_core = dict(index)
    index_id = index_core.pop("index_id", None)
    if (
        set(index_core) != {"calendar_receipt", "coverage_end", "coverage_start", "markets", "schema_version"}
        or index_id != sha256_json(index_core)
        or index_core["schema_version"] != INDEX_SCHEMA
        or index_core["coverage_start"] != START.isoformat()
        or index_core["coverage_end"] != END.isoformat()
        or index_core["markets"] != list(MARKETS)
    ):
        raise IntegrityError("historical checkpoint calendar index identity is invalid")
    calendar_receipt = _receipt(index_core["calendar_receipt"], name="checkpoint calendar")
    if tuple(index_manifest.source_release_ids) != (calendar_receipt.release_id,):
        raise IntegrityError("historical checkpoint calendar index lineage is invalid")
    calendar_manifest = calendar_receipt.verify(boundary)
    if (
        calendar_manifest.release_kind != CALENDAR_KIND
        or calendar_manifest.schema_version != CALENDAR_SCHEMA
        or set(calendar_manifest.embedded_documents) != {"historical_checkpoint_calendar.json"}
        or calendar_manifest.files
    ):
        raise IntegrityError("historical checkpoint calendar release kind is invalid")
    calendar = calendar_receipt.embedded_document("historical_checkpoint_calendar.json", boundary)
    if not isinstance(calendar, dict):
        raise IntegrityError("historical checkpoint calendar is invalid")
    calendar_core = dict(calendar)
    calendar_id = calendar_core.pop("calendar_id", None)
    expected_keys = {
        "certification_scope", "checkpoints_chicago", "coverage_end", "coverage_start",
        "derivation_policy", "markets", "schema_version", "sessions", "source_capture_receipt",
    }
    if (
        set(calendar_core) != expected_keys
        or calendar_id != sha256_json(calendar_core)
        or calendar_core["schema_version"] != CALENDAR_SCHEMA
        or calendar_core["certification_scope"] != "ONLY_08_30_10_30_13_30_AMERICA_CHICAGO"
        or calendar_core["checkpoints_chicago"] != list(CHECKPOINTS)
        or calendar_core["coverage_start"] != START.isoformat()
        or calendar_core["coverage_end"] != END.isoformat()
        or calendar_core["markets"] != list(MARKETS)
    ):
        raise IntegrityError("historical checkpoint calendar identity is invalid")
    capture_receipt = _receipt(calendar_core["source_capture_receipt"], name="CME source capture")
    if tuple(calendar_manifest.source_release_ids) != (capture_receipt.release_id,):
        raise IntegrityError("historical checkpoint calendar source lineage is invalid")
    capture_manifest = capture_receipt.verify(boundary)
    if (
        capture_manifest.release_kind != CAPTURE_KIND
        or capture_manifest.schema_version != CAPTURE_SCHEMA
        or set(capture_manifest.embedded_documents) != {"cme_historical_checkpoint_sources.json"}
    ):
        raise IntegrityError("historical checkpoint calendar capture kind is invalid")
    capture = capture_receipt.embedded_document("cme_historical_checkpoint_sources.json", boundary)
    if not isinstance(capture, dict) or set(capture) != {
        "coverage_end", "coverage_start", "records", "schema_version",
        "source_author", "transport_cost_usd", "transport_provider",
    }:
        raise IntegrityError("historical checkpoint calendar capture schema is invalid")
    records = capture["records"]
    specs = {item.key: item for item in authoritative_sources()}
    files = {item.logical_path: item for item in capture_manifest.files}
    if (
        capture["coverage_start"] != START.isoformat()
        or capture["coverage_end"] != END.isoformat()
        or capture["schema_version"] != CAPTURE_SCHEMA
        or capture["source_author"] != "CME_GROUP"
        or capture["transport_provider"] != "INTERNET_ARCHIVE"
        or capture["transport_cost_usd"] != "0"
        or not isinstance(records, list)
        or len(records) != len(specs)
    ):
        raise IntegrityError("historical checkpoint calendar capture identity is invalid")
    observed_source_keys: set[str] = set()
    for record in records:
        if not isinstance(record, dict) or set(record) != {
            "archive_timestamp", "cdx_digest", "key", "logical_path",
            "original_cme_url", "retrieval_url", "sha256", "size", "transport",
        }:
            raise IntegrityError("historical checkpoint calendar capture record is invalid")
        key = record["key"]
        spec = specs.get(key) if isinstance(key, str) else None
        entry = files.get(str(record["logical_path"]))
        if (
            spec is None or key in observed_source_keys or entry is None
            or record["archive_timestamp"] != spec.timestamp
            or record["cdx_digest"] != spec.cdx_digest
            or record["original_cme_url"] != spec.original_url
            or record["retrieval_url"] != spec.replay_url
            or record["transport"] != "INTERNET_ARCHIVE_IDENTITY_REPLAY_OF_CME_AUTHORED_FILE"
            or record["sha256"] != entry.sha256 or record["size"] != entry.size
        ):
            raise IntegrityError("historical checkpoint calendar capture record drifted")
        observed_source_keys.add(key)
    if observed_source_keys != set(specs) or set(files) != {str(item["logical_path"]) for item in records}:
        raise IntegrityError("historical checkpoint calendar capture is incomplete")
    raw_sessions = calendar_core["sessions"]
    if not isinstance(raw_sessions, list) or raw_sessions != build_checkpoint_sessions():
        raise IntegrityError("historical checkpoint calendar sessions are invalid")
    sessions: dict[tuple[str, str], Mapping[str, bool]] = {}
    for row in raw_sessions:
        if not isinstance(row, dict) or set(row) != {"checkpoint_open", "evidence_key", "market", "state_class", "trade_date"}:
            raise IntegrityError("historical checkpoint calendar row schema is invalid")
        states = row["checkpoint_open"]
        key = (str(row["market"]), str(row["trade_date"]))
        if (
            key in sessions or key[0] not in MARKETS or not isinstance(states, dict)
            or set(states) != set(CHECKPOINTS) or any(type(value) is not bool for value in states.values())
        ):
            raise IntegrityError("historical checkpoint calendar row is invalid")
        sessions[key] = dict(states)
    expected = {(market, day.isoformat()) for market in MARKETS for day in _dates()}
    if set(sessions) != expected or len(sessions) != len(MARKETS) * 1826:
        raise IntegrityError("historical checkpoint calendar is not gapless")
    return LoadedHistoricalCheckpointCalendar(index_receipt, calendar_receipt, capture_receipt, sessions)


def publish_successor(*, root: Path) -> LoadedHistoricalCheckpointCalendar:
    """Download frozen sources, publish immutable releases, and write V5 pointer."""

    boundary = RepoBoundary(root.resolve())
    receipt = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={"purpose": "CME_2018_2022_V5_CHECKPOINT_CALENDAR_ONLY"},
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=receipt,
        lock_path=root / "state/locks/tier1_historical_checkpoint_calendar_v5.lock",
    )
    stage = publisher.create_stage("historical_calendar_capture")
    records: list[dict[str, object]] = []
    logical_paths: dict[str, str] = {}
    for source in authoritative_sources():
        filename = f"{source.key}{source.suffix}"
        target = stage / filename
        request = urllib.request.Request(source.replay_url, headers={"User-Agent": "futures-research-calendar-capture/1.0"})
        with urllib.request.urlopen(request, timeout=120) as response, target.open("wb") as output:
            shutil.copyfileobj(response, output)
        if target.stat().st_size <= 0:
            raise IntegrityError(f"empty archived CME source: {source.key}")
        records.append(
            {
                "archive_timestamp": source.timestamp,
                "cdx_digest": source.cdx_digest,
                "key": source.key,
                "logical_path": f"data/reference/exchange_calendars/{filename}",
                "original_cme_url": source.original_url,
                "retrieval_url": source.replay_url,
                "sha256": sha256_file(target),
                "size": target.stat().st_size,
                "transport": "INTERNET_ARCHIVE_IDENTITY_REPLAY_OF_CME_AUTHORED_FILE",
            }
        )
        logical_paths[filename] = f"data/reference/exchange_calendars/{filename}"
    capture_document = {
        "coverage_end": END.isoformat(),
        "coverage_start": START.isoformat(),
        "records": records,
        "schema_version": CAPTURE_SCHEMA,
        "source_author": "CME_GROUP",
        "transport_provider": "INTERNET_ARCHIVE",
        "transport_cost_usd": "0",
    }
    capture_manifest = DataReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CAPTURE_KIND,
        schema_version=CAPTURE_SCHEMA,
        logical_paths=logical_paths,
        embedded_documents={"cme_historical_checkpoint_sources.json": capture_document},
        metadata={"coverage_end": END.isoformat(), "coverage_start": START.isoformat(), "source_count": len(records)},
    )
    capture_receipt = DataReleaseReceipt.from_manifest(publisher.publish(stage, capture_manifest), boundary)

    return _publish_derived_successor(
        root=root, boundary=boundary, publisher=publisher,
        capture_receipt=capture_receipt,
    )


def publish_derived_successor(
    *, root: Path, capture_receipt: DataReleaseReceipt,
) -> LoadedHistoricalCheckpointCalendar:
    """Publish a corrected calendar/index while preserving a verified capture."""

    boundary = RepoBoundary(root.resolve())
    capture_receipt.verify(boundary)
    receipt = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={"purpose": "CORRECTED_CME_2018_2022_V5_CHECKPOINT_CALENDAR_ONLY"},
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=receipt,
        lock_path=root / "state/locks/tier1_historical_checkpoint_calendar_v5.lock",
    )
    return _publish_derived_successor(
        root=root, boundary=boundary, publisher=publisher,
        capture_receipt=capture_receipt,
    )


def _publish_derived_successor(
    *, root: Path, boundary: RepoBoundary, publisher: PhasePublisher,
    capture_receipt: DataReleaseReceipt,
) -> LoadedHistoricalCheckpointCalendar:
    sessions = build_checkpoint_sessions()
    calendar_core = {
        "certification_scope": "ONLY_08_30_10_30_13_30_AMERICA_CHICAGO",
        "checkpoints_chicago": list(CHECKPOINTS),
        "coverage_end": END.isoformat(),
        "coverage_start": START.isoformat(),
        "derivation_policy": {
            "full_close": "ALL_THREE_CHECKPOINTS_CLOSED",
            "market_specific_overrides": "EXACT_CME_PRODUCT_GROUP_ROWS_TAKE_PRIORITY_OVER_GENERIC_HOLIDAY_CLASS",
            "ordinary_weekday": "ALL_THREE_CHECKPOINTS_OPEN_PER_REPEATED_CME_REGULAR_PRODUCT_ROWS",
            "scheduled_early_close": "08_30_AND_10_30_OPEN_13_30_CLOSED",
            "weekend": "ALL_THREE_CHECKPOINTS_CLOSED",
            "whole_session_claim": False,
        },
        "markets": list(MARKETS),
        "schema_version": CALENDAR_SCHEMA,
        "sessions": sessions,
        "source_capture_receipt": capture_receipt.as_dict(),
    }
    calendar_document = {**calendar_core, "calendar_id": sha256_json(calendar_core)}
    calendar_stage = publisher.create_stage("historical_checkpoint_calendar")
    calendar_manifest = DataReleaseManifest.build(
        calendar_stage,
        phase="reference",
        release_kind=CALENDAR_KIND,
        schema_version=CALENDAR_SCHEMA,
        source_release_ids=(capture_receipt.release_id,),
        embedded_documents={"historical_checkpoint_calendar.json": calendar_document},
        metadata={"calendar_id": calendar_document["calendar_id"], "coverage_end": END.isoformat(), "coverage_start": START.isoformat(), "session_count": len(sessions)},
    )
    calendar_receipt = DataReleaseReceipt.from_manifest(publisher.publish(calendar_stage, calendar_manifest), boundary)

    index_core = {
        "calendar_receipt": calendar_receipt.as_dict(),
        "coverage_end": END.isoformat(),
        "coverage_start": START.isoformat(),
        "markets": list(MARKETS),
        "schema_version": INDEX_SCHEMA,
    }
    index_document = {**index_core, "index_id": sha256_json(index_core)}
    index_stage = publisher.create_stage("historical_checkpoint_calendar_index")
    index_manifest = DataReleaseManifest.build(
        index_stage,
        phase="controls",
        release_kind=INDEX_KIND,
        schema_version=INDEX_SCHEMA,
        source_release_ids=(calendar_receipt.release_id,),
        embedded_documents={"historical_checkpoint_calendar_index.json": index_document},
        metadata={"coverage_end": END.isoformat(), "coverage_start": START.isoformat(), "index_id": index_document["index_id"]},
    )
    index_receipt = DataReleaseReceipt.from_manifest(publisher.publish(index_stage, index_manifest), boundary)
    pointer = {"calendar_index_receipt": index_receipt.as_dict(), "schema_version": POINTER_SCHEMA}
    pointer_path = root / POINTER_PATH
    encoded = canonical_bytes(pointer) + b"\n"
    descriptor, temporary_text = tempfile.mkstemp(
        prefix=f".{pointer_path.name}.", suffix=".tmp", dir=pointer_path.parent,
    )
    temporary = Path(temporary_text)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        # Verify the entire immutable dependency closure before rebinding V5.
        load_historical_checkpoint_calendar(boundary=boundary, pointer_path=temporary)
        os.replace(temporary, pointer_path)
        fsync_directory(pointer_path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()
    return load_historical_checkpoint_calendar(boundary=boundary, pointer_path=pointer_path)
