"""One bounded, mechanics-only read of an accepted active Phase 3 input."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .active_phase3_validation import ActivePhase3MechanicsValidation
from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_json
from .errors import ContractError, IntegrityError


REPORT_SCHEMA_VERSION = "active_phase3_mechanics_report/1.0.0"
REPORT_RELATIVE_PATH = "reports/phase3_mechanics/trial106/mechanics_report.json"
MAXIMUM_ROW_READS = 512
_REQUIRED_COLUMNS = (
    "market",
    "event_at_ns",
    "available_at_ns",
    "open_nano",
    "close_nano",
    "disposition",
    "actual_identity_hash",
    "exchange_session_date",
    "tick_size",
    "point_value",
    "tick_value",
    "currency",
)


@dataclass(frozen=True)
class ActivePhase3MechanicsReport:
    report_relative_path: str
    report_id: str
    rows_read: int
    row_read_cap: int

    def __post_init__(self) -> None:
        if (
            type(self.report_relative_path) is not str
            or self.report_relative_path != REPORT_RELATIVE_PATH
            or type(self.report_id) is not str
            or len(self.report_id) != 64
            or type(self.rows_read) is not int
            or not 0 < self.rows_read <= MAXIMUM_ROW_READS
            or type(self.row_read_cap) is not int
            or not 0 < self.row_read_cap <= MAXIMUM_ROW_READS
            or self.rows_read > self.row_read_cap
        ):
            raise ContractError("Phase 3 mechanics report is outside its bounded contract")


def _bool(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise IntegrityError(f"Phase 3 mechanics check did not establish {name}")
    return value


def _report_payload(
    *, validation: ActivePhase3MechanicsValidation, rows: list[Mapping[str, object]], row_read_cap: int
) -> dict[str, object]:
    events = [row.get("event_at_ns") for row in rows]
    available = [row.get("available_at_ns") for row in rows]
    markets = [row.get("market") for row in rows]
    required_fields_present = all(
        all(column in row and row[column] is not None for column in _REQUIRED_COLUMNS)
        for row in rows
    )
    ordered = all(
        type(events[index]) is int
        and type(events[index - 1]) is int
        and events[index - 1] <= events[index]
        for index in range(1, len(events))
    )
    causal_availability = all(
        type(event) is int and type(received) is int and received >= event
        for event, received in zip(events, available, strict=True)
    )
    market_matches = all(market == validation.active_input.market for market in markets)
    checks = {
        "causal_availability": causal_availability,
        "market_matches": market_matches,
        "ordered_by_event_time": ordered,
        "required_fields_present": required_fields_present,
        "row_cap_respected": len(rows) <= row_read_cap <= MAXIMUM_ROW_READS,
    }
    for name, value in checks.items():
        _bool(value, name)
    payload = {
        "checks": checks,
        "entry_delay_seconds": validation.entry_delay_seconds,
        "input_id": validation.active_input.input_id,
        "input_record_path": validation.input_record_path,
        "input_record_sha256": validation.input_record_sha256,
        "label_horizon_seconds": validation.label_horizon_seconds,
        "market": validation.active_input.market,
        "maximum_row_reads": MAXIMUM_ROW_READS,
        "mechanics_only": True,
        "report_schema_version": REPORT_SCHEMA_VERSION,
        "row_read_cap": row_read_cap,
        "rows_read": len(rows),
        "source_parquet_sha256": validation.active_input.parquet_sha256,
        "validation_id": validation.validation_id,
        "year": validation.active_input.year,
    }
    core = dict(payload)
    payload["report_id"] = sha256_json(core)
    return payload


def run_active_phase3_mechanics_check(
    *,
    boundary: RepoBoundary,
    validation: ActivePhase3MechanicsValidation,
    row_read_cap: int = MAXIMUM_ROW_READS,
) -> ActivePhase3MechanicsReport:
    """Read at most ``row_read_cap`` local rows and publish one non-alpha report."""

    if type(validation) is not ActivePhase3MechanicsValidation:
        raise ContractError("Phase 3 mechanics check requires its exact preflight")
    if type(row_read_cap) is not int or not 0 < row_read_cap <= MAXIMUM_ROW_READS:
        raise ContractError("Phase 3 mechanics row cap must be between one and 512")
    report_path = boundary.active_root / REPORT_RELATIVE_PATH
    report_root = report_path.parent
    if report_root.exists():
        raise IntegrityError("Phase 3 mechanics report root must be absent before the run")
    parquet_path = boundary.assert_active_path(
        boundary.active_root / validation.active_input.parquet_path,
        purpose="Phase 3 mechanics input parquet",
        subtree="data/active",
    )
    try:
        import pyarrow.parquet as pq

        reader = pq.ParquetFile(parquet_path)
        available_columns = set(reader.schema_arrow.names)
        if not set(_REQUIRED_COLUMNS).issubset(available_columns):
            raise IntegrityError("Phase 3 mechanics input is missing required columns")
        batch = next(reader.iter_batches(batch_size=row_read_cap, columns=list(_REQUIRED_COLUMNS)), None)
        if batch is None:
            raise IntegrityError("Phase 3 mechanics input has no rows")
        rows = batch.to_pylist()
    except IntegrityError:
        raise
    except Exception as exc:
        raise IntegrityError("Phase 3 mechanics input could not be read") from exc
    if not rows or len(rows) > row_read_cap:
        raise IntegrityError("Phase 3 mechanics reader exceeded its row cap")
    payload = _report_payload(validation=validation, rows=rows, row_read_cap=row_read_cap)
    report_root.mkdir(parents=True)
    report_path.write_bytes(canonical_bytes(payload) + b"\n")
    try:
        readback = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("Phase 3 mechanics report readback failed") from exc
    if readback != payload:
        raise IntegrityError("Phase 3 mechanics report readback differs from its postimage")
    return ActivePhase3MechanicsReport(
        report_relative_path=REPORT_RELATIVE_PATH,
        report_id=payload["report_id"],
        rows_read=len(rows),
        row_read_cap=row_read_cap,
    )
