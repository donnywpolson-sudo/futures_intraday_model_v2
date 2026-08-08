from __future__ import annotations

import struct
import json
from pathlib import Path

import pytest
from datetime import datetime
import re

from futures_rebuild.cme_calendar_source_adequacy import (
    FamilySchedule,
    ScheduleEvent,
    WorkbookCell,
    parse_biff8_cells,
    parse_family_schedule,
)
from futures_rebuild.cme_calendar_successor import (
    RECOVERY_RECORD,
    _effective,
    _load_recovered_jan1_schedule,
    _state,
)
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
AUDIT_PATH = ROOT / (
    "state/unpublished_evidence/cash_open_impulse_41_market_calendar_source_adequacy/"
    "d9e7d1963708519a487654186373ca26fbbcdc336570d19f5e4a115b4e05b0fc/"
    "calendar_source_adequacy.json"
)
OLD_GAP_SUCCESSOR_PATH = ROOT / (
    "state/unpublished_evidence/cash_open_impulse_41_market_calendar_successor_preparation/"
    "a6365e1f31be73b7039f5425d261f9f4287b54f598f8cd96bea6af2e70429584/"
    "historical_calendar_successor.json"
)
SUCCESSOR_PATH = ROOT / (
    "state/unpublished_evidence/cash_open_impulse_41_market_calendar_successor_preparation/"
    "54bc5550a0ba28af2a509fb32c756b39041686ba10ffa6bd832e6d96469c0397/"
    "historical_calendar_successor.json"
)
LOCAL_SOURCE_SEARCH_PATH = ROOT / (
    "state/unpublished_evidence/cash_open_impulse_jan1_2019_local_source_search/"
    "37c90ab2c71849410c510dd930cfbad2c8b4f10d2fb95f4a3e468c94ce32adcb/"
    "source_selection.json"
)
RECOVERY_RECORD_PATH = ROOT / (
    "state/unpublished_evidence/cash_open_impulse_jan1_2019_calendar_recovery/"
    "fbca0af01eee949039e8efd572c0e298d5d9644b9bbaa8369708f0b876a55161/"
    "acquisition_record.json"
)


def _record(record_id: int, data: bytes) -> bytes:
    return struct.pack("<HH", record_id, len(data)) + data


def test_biff_parser_recovers_compressed_shared_string_cell() -> None:
    text = b"Equity Index"
    sst = struct.pack("<IIHB", 1, 1, len(text), 0) + text
    sheet_offset = 4 + 7 + len("Hours") + 4 + len(sst)
    bound = struct.pack("<IBBB", sheet_offset, 0, 0, 5) + b"Hours"
    sheet = _record(0x00FD, struct.pack("<HHHI", 3, 2, 0, 0)) + _record(0x000A, b"")
    stream = _record(0x0085, bound) + _record(0x00FC, sst) + sheet
    assert parse_biff8_cells(stream)[0].value == "Equity Index"


def test_biff_parser_fails_closed_without_shared_strings() -> None:
    with pytest.raises(IntegrityError, match="shared-string"):
        parse_biff8_cells(_record(0x000A, b""))


def test_sealed_audit_is_exact_and_excludes_2025() -> None:
    from futures_rebuild.canonical import sha256_file, sha256_json

    assert sha256_file(AUDIT_PATH) == (
        "ef62fa5579d102a692e347b0286973095cae0a14e8f56c463779346ac05a503a"
    )
    report = json.loads(AUDIT_PATH.read_text(encoding="utf-8"))
    core = {key: value for key, value in report.items() if key != "audit_id"}
    assert report["audit_id"] == sha256_json(core)
    assert report["decision"] == "FAIL_INSUFFICIENT_FOR_EXACT_41_MARKET_CALENDAR_SUCCESSOR"
    assert len(report["checkpoint_session_evidence"]) == 37 * 5
    assert {item["year"] for item in report["checkpoint_session_evidence"]} == set(range(2018, 2023))
    assert sum(
        item["unverified_checkpoint_session_count"]
        for item in report["checkpoint_session_evidence"]
    ) == 135124
    assert report["authority"]["year_2025_accessed"] is False


def _cell(row: int, column: int, value: str | float) -> WorkbookCell:
    return WorkbookCell("Hours", row, column, value)


def test_family_schedule_accepts_repeated_dates_and_abbreviated_months() -> None:
    cells = [
        _cell(0, 0, "2019 Christmas Holiday Schedule"),
        _cell(1, 0, "Calendar Date"),
        _cell(1, 1, "Tuesday, Dec 24"),
        _cell(1, 2, "Wednesday, Dec 25"),
        _cell(1, 3, "Wednesday, Dec 25"),
        _cell(2, 1, "Close"),
        _cell(2, 2, "Halt"),
        _cell(2, 3, "Open"),
        _cell(3, 0, "Equity Products"),
        _cell(3, 1, 0.5),
        _cell(3, 2, "Globex Closed"),
        _cell(3, 3, 0.75),
    ]
    schedule = parse_family_schedule(cells, label_pattern=re.compile("Equity Products"))
    assert tuple(value.isoformat() for value in schedule.calendar_dates) == (
        "2019-12-24", "2019-12-25", "2019-12-25",
    )
    assert schedule.state_at(datetime(2019, 12, 25, 8, 29)) is False


def test_family_schedule_fails_closed_for_duplicate_family_rows() -> None:
    cells = [
        _cell(0, 0, "2020 Schedule"),
        _cell(1, 0, "Calendar Date"), _cell(1, 1, "Monday, May 25"),
        _cell(2, 1, "Close"),
        _cell(3, 0, "FX Products"), _cell(3, 1, 0.5),
        _cell(4, 0, "FX Products"), _cell(4, 1, 0.5),
    ]
    with pytest.raises(IntegrityError, match="one exact family"):
        parse_family_schedule(cells, label_pattern=re.compile("FX Products"))


def test_family_schedule_uses_trade_day_clause_in_compact_into_heading() -> None:
    cells = [
        _cell(0, 0, "2020 Labor Day Schedule"),
        _cell(1, 0, "Calendar Date"),
        _cell(1, 1, "Sunday, Sept. 6 into Monday, Sept. 7"),
        _cell(2, 1, "Halt"),
        _cell(3, 0, "Interest Rate Products"), _cell(3, 1, 0.5),
    ]
    schedule = parse_family_schedule(
        cells, label_pattern=re.compile("Interest Rate Products")
    )
    assert schedule.calendar_dates == (datetime(2020, 9, 7).date(),)


def test_compact_schedule_parses_day_only_heading_and_textual_close() -> None:
    cells = [
        _cell(0, 1, "Christmas Schedule: December 23, 2021 - December 27, 2021"),
        _cell(1, 0, "Trade Date"),
        _cell(2, 0, "Products"),
        _cell(2, 1, "Friday, Dec 24"), _cell(2, 2, "Sunday 26"),
        _cell(3, 1, "CLOSED"), _cell(3, 2, "OPEN"),
        _cell(4, 0, "Equity"),
        _cell(4, 1, "Closed for Christmas"),
        _cell(4, 2, "Regular @ 1700 CT / 2300 UTC"),
    ]
    schedule = parse_family_schedule(cells, label_pattern=re.compile("Equity"))
    assert schedule.state_at(datetime(2021, 12, 24, 9, 0)) is False
    assert schedule.state_at(datetime(2021, 12, 26, 18, 0)) is True


def test_window_fails_when_a_halt_occurs_inside_it() -> None:
    day = datetime(2022, 7, 4)
    schedule = FamilySchedule(
        "Hours", 4, "Equity", (day.date(),),
        (
            ScheduleEvent(day.replace(hour=8), 1, "OPEN", "08:00"),
            ScheduleEvent(day.replace(hour=9), 2, "HALT", "09:00"),
        ),
    )
    assert _state(schedule, day.replace(hour=8, minute=29), day.replace(hour=9, minute=31)) is False


def test_product_effectiveness_is_half_open_at_expiration() -> None:
    from zoneinfo import ZoneInfo

    start = datetime(2020, 1, 1, tzinfo=ZoneInfo("UTC"))
    end = datetime(2020, 2, 1, tzinfo=ZoneInfo("UTC"))
    interval = [(int(start.timestamp() * 1e9), int(end.timestamp() * 1e9))]
    assert _effective(interval, start)
    assert not _effective(interval, end)


def test_prepared_successor_is_hash_bound_inactive_and_has_exact_coverage(
    local_evidence_root: Path,
) -> None:
    from futures_rebuild.canonical import sha256_file, sha256_json

    successor_path = local_evidence_root / SUCCESSOR_PATH.relative_to(ROOT)
    assert sha256_file(successor_path) == "7860a57f7b64288be333d82cfc7e0f1b889c06304f9cedbb3a8abb3caff795ec"
    payload = json.loads(successor_path.read_text(encoding="utf-8"))
    core = {key: value for key, value in payload.items() if key != "calendar_id"}
    assert payload["calendar_id"] == sha256_json(core)
    assert payload["decision"] == "PASS_EXACT_REFERENCE_COVERAGE"
    assert payload["authority"] == {
        "active": False, "price_rows_read": False, "provider_network_credentials_accessed": False,
        "published": False, "year_2025_accessed": False,
    }
    assert len(payload["market_to_schedule_family"]) == 41
    assert set(payload["product_effective_intervals"]) == set(payload["market_to_schedule_family"])
    assert all(payload["product_effective_intervals"].values())
    assert len(payload["calendar_rows"]) == 41 * 1826
    assert {row["trade_date"][:4] for row in payload["calendar_rows"]} == {
        "2018", "2019", "2020", "2021", "2022",
    }
    assert payload["unresolved_reference_count"] == 0
    assert payload["unresolved_reference_states"] == []
    assert payload["additive_january_1_2019_recovery"]["record_sha256"] == (
        "2c0dcb6c12316ef582e220590c48a5ad29760a62c149412d685706931f43f0dc"
    )
    for relative, expected in payload["bindings"].items():
        assert sha256_file(local_evidence_root / relative) == expected


def test_recovery_changes_exactly_80_abstentions_and_preserves_every_other_row() -> None:
    from futures_rebuild.canonical import sha256_file, sha256_json

    assert sha256_file(OLD_GAP_SUCCESSOR_PATH) == (
        "69b3cef694c0dfa405c87c51083553c11ade9bcd68a9644fa074446e76ee097e"
    )
    old = json.loads(OLD_GAP_SUCCESSOR_PATH.read_text(encoding="utf-8"))
    assert old["calendar_id"] == sha256_json(
        {key: value for key, value in old.items() if key != "calendar_id"}
    )
    new = json.loads(SUCCESSOR_PATH.read_text(encoding="utf-8"))
    changed_rows = [
        (before, after)
        for before, after in zip(old["calendar_rows"], new["calendar_rows"], strict=True)
        if before != after
    ]
    assert len(changed_rows) == 40
    assert {before["trade_date"] for before, _ in changed_rows} == {"2019-01-01"}
    assert "ETH" not in {before["market"] for before, _ in changed_rows}
    changed_checkpoints = 0
    for before, after in changed_rows:
        assert before["market"] == after["market"]
        assert before["checkpoint_open"] == after["checkpoint_open"] == {
            "09:00": False, "10:30": False,
        }
        for checkpoint in ("09:00", "10:30"):
            assert before["disposition"][checkpoint] == "UNVERIFIED_REFERENCE_ABSTENTION"
            assert after["disposition"][checkpoint] == "EXACT_CME_FAMILY_SCHEDULE"
            changed_checkpoints += 1
    assert changed_checkpoints == 80


def test_local_jan1_2019_search_is_hash_bound_and_selects_nothing() -> None:
    from futures_rebuild.canonical import sha256_file, sha256_json

    assert sha256_file(LOCAL_SOURCE_SEARCH_PATH) == (
        "515cc166f71ab387456563bb628e5288afaf7418622d072e875b339bac7d43c0"
    )
    payload = json.loads(LOCAL_SOURCE_SEARCH_PATH.read_text(encoding="utf-8"))
    core = {key: value for key, value in payload.items() if key != "record_id"}
    assert payload["record_id"] == sha256_json(core)
    assert payload["decision"] == "FAIL_NO_EXACT_LOCAL_SOURCE"
    assert payload["candidate_count"] == 0
    assert payload["selected_source"] is None
    assert len(payload["reference_release_inventory"]) == 19
    assert len(payload["exact_search_release_bindings"]) == 5
    assert sum("source" in item for item in payload["inspection_evidence"]) == 64
    assert payload["authority"] == {
        "activated": False, "external_cost_usd": "0", "price_rows_read": False,
        "provider_network_credentials_accessed": False, "published": False,
        "year_2025_accessed": False,
    }
    for relative, expected in payload["bindings"].items():
        assert sha256_file(ROOT / relative) == expected


def test_jan1_2019_recovery_is_exact_inactive_and_complete(
    local_evidence_root: Path,
) -> None:
    from futures_rebuild.canonical import sha256_file, sha256_json

    recovery_record_path = local_evidence_root / RECOVERY_RECORD_PATH.relative_to(ROOT)
    assert sha256_file(recovery_record_path) == (
        "2c0dcb6c12316ef582e220590c48a5ad29760a62c149412d685706931f43f0dc"
    )
    payload = json.loads(recovery_record_path.read_text(encoding="utf-8"))
    core = {key: value for key, value in payload.items() if key != "record_id"}
    assert payload["record_id"] == sha256_json(core)
    assert payload["decision"] == "PASS_EXACT_AUTHORITATIVE_SCHEDULE_ACQUIRED_INACTIVE"
    assert payload["raw"]["sha256"] == (
        "2684a5f1b3a9f65802f6911ca6089e2cb68c3cdf2520dfaf3cdcf3105328c188"
    )
    assert sha256_file(local_evidence_root / payload["raw"]["path"]) == payload["raw"]["sha256"]
    assert sha256_file(local_evidence_root / payload["sidecar"]["path"]) == payload["sidecar"]["sha256"]
    assert set(payload["validation"]["family_rows"]) == {
        "CRYPTO", "ENERGY_METALS", "EQUITY_INDEX", "FX",
        "GRAIN_OILSEED", "LIVESTOCK", "RATES",
    }
    assert all(
        row["january_1_disposition"] == "Closed for New Year's"
        for row in payload["validation"]["family_rows"].values()
    )
    assert payload["request_accounting"]["conservative_total_requests"] <= 20
    assert payload["authority"] == {
        "activated": False, "active_data_mutated": False, "credentials_accessed": False,
        "external_cost_usd": "0", "price_rows_read": False, "published": False,
        "year_2025_accessed": False,
    }


def test_recovered_schedule_closes_every_bound_family(
    local_evidence_root: Path,
) -> None:
    schedules, binding = _load_recovered_jan1_schedule(local_evidence_root)
    assert set(schedules) == {
        "CRYPTO", "ENERGY", "EQUITY_INDEX", "FX",
        "GRAIN_OILSEED", "LIVESTOCK", "METALS", "RATES",
    }
    instant = datetime(2019, 1, 1, 9, 0)
    assert all(schedule.state_at(instant) is False for schedule in schedules.values())
    assert binding["raw_sha256"] == (
        "2684a5f1b3a9f65802f6911ca6089e2cb68c3cdf2520dfaf3cdcf3105328c188"
    )


def test_recovered_schedule_fails_closed_when_staged_raw_bytes_change(
    tmp_path: Path, local_evidence_root: Path
) -> None:
    import shutil

    record = json.loads(
        (local_evidence_root / RECOVERY_RECORD).read_text(encoding="utf-8")
    )
    paths = [RECOVERY_RECORD, Path(record["raw"]["path"]), Path(record["sidecar"]["path"])]
    for relative in paths:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_evidence_root / relative, target)
    raw_path = tmp_path / record["raw"]["path"]
    raw_path.write_bytes(raw_path.read_bytes() + b"tamper")
    with pytest.raises(IntegrityError, match="workbook hash drifted"):
        _load_recovered_jan1_schedule(tmp_path)
