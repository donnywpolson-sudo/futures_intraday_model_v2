"""Read-only parsing and source-adequacy checks for immutable CME XLS files.

This deliberately implements only the Compound Binary File and BIFF8 subset
needed to recover worksheet cell values from the bound CME holiday schedules.
It has no price-data, provider, publication, or active-pointer surface.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
import re
from typing import Iterator, Mapping, Sequence

from .errors import IntegrityError


CFB_MAGIC = bytes.fromhex("d0cf11e0a1b11ae1")
FREESECT = 0xFFFFFFFF
ENDOFCHAIN = 0xFFFFFFFE
FATSECT = 0xFFFFFFFD
DIFSECT = 0xFFFFFFFC


def _u16(data: bytes, offset: int) -> int:
    return struct.unpack_from("<H", data, offset)[0]


def _u32(data: bytes, offset: int) -> int:
    return struct.unpack_from("<I", data, offset)[0]


def _u64(data: bytes, offset: int) -> int:
    return struct.unpack_from("<Q", data, offset)[0]


def _chain(start: int, table: Sequence[int], *, limit: int) -> tuple[int, ...]:
    if start in (FREESECT, ENDOFCHAIN):
        return ()
    result: list[int] = []
    seen: set[int] = set()
    value = start
    while value != ENDOFCHAIN:
        if value in seen or value >= len(table) or value in (FREESECT, FATSECT, DIFSECT):
            raise IntegrityError("CME workbook compound-file chain is invalid")
        seen.add(value)
        result.append(value)
        if len(result) > limit:
            raise IntegrityError("CME workbook compound-file chain is unbounded")
        value = table[value]
    return tuple(result)


@dataclass(frozen=True)
class _DirectoryEntry:
    name: str
    kind: int
    start_sector: int
    size: int


def extract_workbook_stream_bytes(data: bytes) -> bytes:
    if len(data) < 512 or data[:8] != CFB_MAGIC:
        raise IntegrityError("CME workbook is not an OLE compound file")
    sector_size = 1 << _u16(data, 30)
    mini_sector_size = 1 << _u16(data, 32)
    if sector_size not in (512, 4096) or mini_sector_size != 64:
        raise IntegrityError("CME workbook compound-file sector size is unsupported")
    sector_count = (len(data) - 512) // sector_size

    def sector(sid: int) -> bytes:
        if sid >= sector_count:
            raise IntegrityError("CME workbook sector leaves the file")
        start = 512 + sid * sector_size
        return data[start : start + sector_size]

    difat = [value for value in struct.unpack_from("<109I", data, 76) if value != FREESECT]
    next_difat = _u32(data, 68)
    for _ in range(_u32(data, 72)):
        raw = sector(next_difat)
        values = struct.unpack(f"<{sector_size // 4}I", raw)
        difat.extend(value for value in values[:-1] if value != FREESECT)
        next_difat = values[-1]
    fat_sector_count = _u32(data, 44)
    if len(difat) < fat_sector_count:
        raise IntegrityError("CME workbook FAT sector inventory is incomplete")
    fat: list[int] = []
    for sid in difat[:fat_sector_count]:
        fat.extend(struct.unpack(f"<{sector_size // 4}I", sector(sid)))

    directory_bytes = b"".join(
        sector(sid) for sid in _chain(_u32(data, 48), fat, limit=sector_count)
    )
    entries: list[_DirectoryEntry] = []
    for offset in range(0, len(directory_bytes), 128):
        raw = directory_bytes[offset : offset + 128]
        if len(raw) < 128:
            break
        name_length = _u16(raw, 64)
        if name_length < 2 or name_length > 64 or name_length % 2:
            continue
        name = raw[: name_length - 2].decode("utf-16le", errors="strict")
        entries.append(_DirectoryEntry(name, raw[66], _u32(raw, 116), _u64(raw, 120)))
    root = next((item for item in entries if item.kind == 5), None)
    workbook = next(
        (item for item in entries if item.kind == 2 and item.name in ("Workbook", "Book")),
        None,
    )
    if root is None or workbook is None:
        raise IntegrityError("CME workbook stream is absent")
    cutoff = _u32(data, 56)
    if workbook.size >= cutoff:
        raw = b"".join(sector(sid) for sid in _chain(workbook.start_sector, fat, limit=sector_count))
        return raw[: workbook.size]

    mini_fat_raw = b"".join(
        sector(sid) for sid in _chain(_u32(data, 60), fat, limit=sector_count)
    )
    mini_fat = list(struct.unpack(f"<{len(mini_fat_raw) // 4}I", mini_fat_raw))
    mini_stream = b"".join(
        sector(sid) for sid in _chain(root.start_sector, fat, limit=sector_count)
    )[: root.size]
    raw = b"".join(
        mini_stream[sid * mini_sector_size : (sid + 1) * mini_sector_size]
        for sid in _chain(workbook.start_sector, mini_fat, limit=len(mini_fat))
    )
    return raw[: workbook.size]


def _records(stream: bytes, *, start: int = 0) -> Iterator[tuple[int, int, bytes]]:
    offset = start
    while offset + 4 <= len(stream):
        record_id, length = struct.unpack_from("<HH", stream, offset)
        end = offset + 4 + length
        if end > len(stream):
            raise IntegrityError("CME workbook BIFF record is truncated")
        yield offset, record_id, stream[offset + 4 : end]
        offset = end


class _SstCursor:
    def __init__(self, chunks: Sequence[bytes]) -> None:
        self.chunks = chunks
        self.chunk = 0
        self.offset = 0

    def _advance(self) -> None:
        self.chunk += 1
        self.offset = 0
        if self.chunk >= len(self.chunks):
            raise IntegrityError("CME workbook shared-string table is truncated")

    def raw(self, count: int) -> bytes:
        result = bytearray()
        while count:
            available = len(self.chunks[self.chunk]) - self.offset
            if not available:
                self._advance()
                continue
            take = min(count, available)
            result.extend(self.chunks[self.chunk][self.offset : self.offset + take])
            self.offset += take
            count -= take
        return bytes(result)

    def characters(self, count: int, *, wide: bool) -> str:
        pieces: list[str] = []
        remaining = count
        current_wide = wide
        while remaining:
            width = 2 if current_wide else 1
            available_bytes = len(self.chunks[self.chunk]) - self.offset
            available_chars = available_bytes // width
            take = min(remaining, available_chars)
            if take:
                raw = self.chunks[self.chunk][self.offset : self.offset + take * width]
                pieces.append(raw.decode("utf-16le" if current_wide else "latin1"))
                self.offset += take * width
                remaining -= take
            if remaining:
                if self.offset != len(self.chunks[self.chunk]):
                    raise IntegrityError("CME workbook string splits a UTF-16 code unit")
                self._advance()
                option = self.raw(1)[0]
                current_wide = bool(option & 0x01)
        return "".join(pieces)


def _shared_strings(stream: bytes) -> list[str]:
    records = list(_records(stream))
    for index, (_, record_id, data) in enumerate(records):
        if record_id != 0x00FC:
            continue
        chunks = [data]
        cursor_index = index + 1
        while cursor_index < len(records) and records[cursor_index][1] == 0x003C:
            chunks.append(records[cursor_index][2])
            cursor_index += 1
        cursor = _SstCursor(chunks)
        cursor.raw(4)
        unique = struct.unpack("<I", cursor.raw(4))[0]
        values: list[str] = []
        for _ in range(unique):
            count = struct.unpack("<H", cursor.raw(2))[0]
            flags = cursor.raw(1)[0]
            rich_runs = struct.unpack("<H", cursor.raw(2))[0] if flags & 0x08 else 0
            extension = struct.unpack("<I", cursor.raw(4))[0] if flags & 0x04 else 0
            values.append(cursor.characters(count, wide=bool(flags & 0x01)))
            cursor.raw(rich_runs * 4 + extension)
        return values
    raise IntegrityError("CME workbook shared-string table is absent")


@dataclass(frozen=True)
class WorkbookCell:
    sheet: str
    row: int
    column: int
    value: str | float


def parse_biff8_cells(stream: bytes) -> tuple[WorkbookCell, ...]:
    strings = _shared_strings(stream)
    sheets: list[tuple[int, str]] = []
    for _, record_id, data in _records(stream):
        if record_id == 0x0085 and len(data) >= 8:
            offset = _u32(data, 0)
            count, flags = data[6], data[7]
            width = 2 if flags & 0x01 else 1
            name = data[8 : 8 + count * width].decode("utf-16le" if width == 2 else "latin1")
            sheets.append((offset, name))
    if not sheets:
        raise IntegrityError("CME workbook has no BIFF worksheets")
    results: list[WorkbookCell] = []
    for offset, name in sheets:
        for _, record_id, data in _records(stream, start=offset):
            if record_id == 0x000A:
                break
            if record_id == 0x00FD and len(data) == 10:
                row, column = struct.unpack_from("<HH", data)
                string_index = _u32(data, 6)
                if string_index >= len(strings):
                    raise IntegrityError("CME workbook cell references an absent string")
                results.append(WorkbookCell(name, row, column, strings[string_index]))
            elif record_id == 0x0203 and len(data) == 14:
                row, column = struct.unpack_from("<HH", data)
                results.append(WorkbookCell(name, row, column, struct.unpack_from("<d", data, 6)[0]))
            elif record_id == 0x027E and len(data) == 10:
                row, column = struct.unpack_from("<HH", data)
                rk = _u32(data, 6)
                if rk & 0x02:
                    value = float(struct.unpack("<i", struct.pack("<I", rk))[0] >> 2)
                else:
                    value = struct.unpack("<d", struct.pack("<Q", (rk & 0xFFFFFFFC) << 32))[0]
                if rk & 0x01:
                    value /= 100
                results.append(WorkbookCell(name, row, column, value))
    return tuple(results)


def read_xls_cells(path: Path) -> tuple[WorkbookCell, ...]:
    return parse_biff8_cells(extract_workbook_stream_bytes(path.read_bytes()))


def read_xls_bytes(data: bytes) -> tuple[WorkbookCell, ...]:
    return parse_biff8_cells(extract_workbook_stream_bytes(data))


_MONTHS = {
    name: index for index, name in enumerate(
        (
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december",
        ),
        start=1,
    )
}
_MONTHS.update({name[:3]: index for name, index in tuple(_MONTHS.items())})
_WEEKDAYS = {
    name: index for index, name in enumerate(
        ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
    )
}
_DATE_LABEL = re.compile(
    r"^\s*(?:(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s*,?\s*)?"
    r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s+(\d{1,2})(?:\s*,?\s*(20\d{2}))?\s*$",
    re.I,
)
_DATE_DAY_ONLY = re.compile(
    r"^\s*(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s*,?\s*(\d{1,2})\s*$",
    re.I,
)


@dataclass(frozen=True)
class ScheduleEvent:
    at: datetime
    column: int
    event: str
    source_value: str


@dataclass(frozen=True)
class FamilySchedule:
    sheet: str
    row: int
    label: str
    calendar_dates: tuple[date, ...]
    events: tuple[ScheduleEvent, ...]

    def state_at(self, value: datetime) -> bool | None:
        state: bool | None = None
        for event in self.events:
            if event.at > value:
                break
            if event.event == "OPEN":
                state = True
            elif event.event in {"HALT", "CLOSE", "PREOPEN", "CLOSED_ALL_DAY"}:
                state = False
        return state

    def continuously_open(self, start: datetime, end: datetime) -> bool | None:
        state = self.state_at(start)
        if state is not True:
            return state
        return not any(
            start < event.at <= end
            and event.event in {"HALT", "CLOSE", "PREOPEN", "CLOSED_ALL_DAY"}
            for event in self.events
        )


def _calendar_date_candidates(
    label: str, *, title_years: set[int], title_months: set[int],
) -> tuple[date, ...]:
    normalized = " ".join(label.replace(".", "").split())
    # Compact CME sheets occasionally combine the prior evening and trade day
    # in one heading ("Sunday ... into Monday ...").  Its event columns apply
    # to the trade-day clause after ``into``.
    clauses = re.split(r"\s+into\s+", normalized, flags=re.I)
    normalized = clauses[-1]
    match = _DATE_LABEL.fullmatch(normalized)
    if match is None:
        day_only = _DATE_DAY_ONLY.fullmatch(normalized)
        if day_only is None or len(title_months) != 1:
            raise IntegrityError(f"CME calendar date label is unsupported: {label!r}")
        weekday_text, day_text = day_only.groups()
        month_values = title_months
        year_text = None
    else:
        weekday_text, month_text, day_text, year_text = match.groups()
        month_values = {_MONTHS[month_text.lower()[:3]]}
    years = {int(year_text)} if year_text else {
        year + delta for year in title_years for delta in (-1, 0, 1)
    }
    values: list[date] = []
    for year in sorted(years):
        for month_value in sorted(month_values):
            try:
                value = date(year, month_value, int(day_text))
            except ValueError:
                continue
            if weekday_text is None or value.weekday() == _WEEKDAYS[weekday_text.lower()]:
                values.append(value)
    if not values:
        raise IntegrityError(f"CME calendar date label has no valid year: {label!r}")
    return tuple(values)


def _choose_chronological_dates(
    labels: Sequence[str], *, title: str,
) -> tuple[date, ...]:
    title_years = {int(value) for value in re.findall(r"20\d{2}", title)}
    if not title_years:
        raise IntegrityError("CME calendar title has no year")
    title_months = {
        month for token, month in _MONTHS.items() if len(token) > 3
        and re.search(rf"\b{re.escape(token)}\b", title, re.I)
    }
    candidates = [
        _calendar_date_candidates(
            label, title_years=title_years, title_months=title_months,
        ) for label in labels
    ]
    solutions: set[tuple[date, ...]] = set()

    def visit(index: int, chosen: list[date]) -> None:
        if index == len(candidates):
            solutions.add(tuple(chosen))
            return
        for value in candidates[index]:
            # A CME calendar date spans multiple adjacent event columns, so
            # repeated dates are valid; backward dates are not.
            if chosen and value < chosen[-1]:
                continue
            if value < date(min(title_years) - 1, 1, 1) or value > date(max(title_years) + 1, 12, 31):
                continue
            visit(index + 1, [*chosen, value])

    visit(0, [])
    if len(solutions) != 1:
        raise IntegrityError("CME calendar date labels are chronologically ambiguous")
    return next(iter(solutions))


def _minutes(value: float) -> int:
    minutes = round(value * 24 * 60)
    if not 0 <= minutes < 24 * 60 or abs(value * 24 * 60 - minutes) > 1e-6:
        raise IntegrityError("CME calendar time is not an exact minute")
    return minutes


def parse_family_schedule(
    cells: Sequence[WorkbookCell], *, label_pattern: re.Pattern[str],
) -> FamilySchedule:
    by_sheet_row: dict[tuple[str, int], dict[int, str | float]] = {}
    for cell in cells:
        by_sheet_row.setdefault((cell.sheet, cell.row), {})[cell.column] = cell.value
    candidates: list[FamilySchedule] = []
    sheets = sorted({cell.sheet for cell in cells})
    for sheet in sheets:
        rows = {
            row: values for (observed_sheet, row), values in by_sheet_row.items()
            if observed_sheet == sheet
        }
        calendar_row = next(
            (row for row, values in rows.items() if values.get(0) in {"Calendar Date", "Products"}),
            None,
        )
        if calendar_row is None:
            continue
        header_row = calendar_row + 1
        headers = rows.get(header_row)
        if headers is None or not any(
            isinstance(value, str) and value.strip().lower() in {"open", "halt", "close", "halt/close"}
            for value in headers.values()
        ):
            raise IntegrityError("CME calendar event header is absent")
        title = " ".join(
            str(value) for row in sorted(rows) if row < calendar_row
            for value in rows[row].values() if isinstance(value, str)
        )
        date_columns = sorted(
            (column, value) for column, value in rows[calendar_row].items()
            if column > 0 and isinstance(value, str) and value.strip()
        )
        parsed_dates = _choose_chronological_dates(
            [str(value) for _, value in date_columns], title=title,
        )
        column_dates = dict(zip((column for column, _ in date_columns), parsed_dates))
        family_rows = []
        for row, values in rows.items():
            label = values.get(0)
            if isinstance(label, str) and label_pattern.fullmatch(" ".join(label.split())):
                family_rows.append((row, values, " ".join(label.split())))
        if len(family_rows) != 1:
            continue
        row, values, label = family_rows[0]
        events: list[ScheduleEvent] = []
        date_starts = sorted(column_dates)
        for column, raw in sorted(values.items()):
            if column == 0 or column not in headers:
                continue
            prior_starts = [start for start in date_starts if start <= column]
            if not prior_starts:
                continue
            event_date = column_dates[prior_starts[-1]]
            header = str(headers[column]).strip().upper()
            source_value = str(raw).strip()
            if isinstance(raw, str) and "closed" in source_value.lower():
                events.append(ScheduleEvent(datetime.combine(event_date, time()), column, "CLOSED_ALL_DAY", source_value))
                continue
            if isinstance(raw, float):
                minutes = _minutes(raw)
            elif isinstance(raw, str):
                matches = re.findall(r"(?<!\d)(\d{1,4})(?::(\d{2}))?\s*CT\b", source_value, re.I)
                if not matches:
                    continue
                hour_text, minute_text = matches[-1] if "OPEN" in header else matches[0]
                hour, minute = int(hour_text), int(minute_text or "0")
                if hour >= 100:
                    hour, minute = divmod(hour, 100)
                if not 0 <= hour < 24 or not 0 <= minute < 60:
                    raise IntegrityError("CME compact calendar time is invalid")
                minutes = hour * 60 + minute
            else:
                continue
            at = datetime.combine(event_date, time()) + timedelta(minutes=minutes)
            if "PRE-OPEN" in header or "PREOPEN" in header:
                event = "PREOPEN"
            elif "OPEN" in header:
                event = "OPEN"
            elif "HALT" in header:
                event = "HALT"
            elif "CLOSE" in header:
                event = "CLOSE"
            else:
                continue
            events.append(ScheduleEvent(at, column, event, source_value))
        candidates.append(FamilySchedule(
            sheet, row, label, parsed_dates,
            tuple(sorted(events, key=lambda item: (item.at, item.column))),
        ))
    if len(candidates) != 1:
        raise IntegrityError("CME workbook does not contain one exact family schedule")
    return candidates[0]
