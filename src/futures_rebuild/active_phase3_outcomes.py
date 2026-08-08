"""Build one immutable, outcome-only Phase 3 release from an active ES view."""

from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path
from typing import Iterable, Mapping

from .active_phase3_validation import ActivePhase3MechanicsValidation
from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import ContractError, IntegrityError


SCHEMA_VERSION = "active_phase3_outcome_release/1.0.0"
LABEL_METHOD_ID = "active_es_60s_300s_v2"
_NS_PER_SECOND = 1_000_000_000
_REQUIRED_COLUMNS = (
    "market", "event_at_ns", "available_at_ns", "open_nano", "disposition",
    "actual_identity_hash", "exchange_session_date", "tick_size", "point_value",
    "tick_value", "currency", "source_row_sha256",
)


def _decision_at_ns(row: Mapping[str, object]) -> int:
    available = row.get("available_at_ns")
    event = row.get("event_at_ns")
    if type(available) is not int or type(event) is not int or available < event:
        raise IntegrityError("active Phase 3 availability time is invalid")
    return ((available + _NS_PER_SECOND * 60 - 1) // (_NS_PER_SECOND * 60)) * (_NS_PER_SECOND * 60)


def _row_status(decision: Mapping[str, object], groups: Mapping[int, list[Mapping[str, object]]]) -> tuple[str, float | None, int]:
    decision_at = _decision_at_ns(decision)
    if decision.get("disposition") != "ELIGIBLE":
        return "MISSING_SOURCE", None, decision_at
    start_at = decision_at + 60 * _NS_PER_SECOND
    end_at = decision_at + 300 * _NS_PER_SECOND
    expected = range(start_at, end_at + 1, 60 * _NS_PER_SECOND)
    identity = (decision.get("actual_identity_hash"), decision.get("exchange_session_date"), decision.get("tick_size"), decision.get("point_value"), decision.get("tick_value"), decision.get("currency"))
    prices: dict[int, int] = {}
    for event in expected:
        candidates = groups.get(event, [])
        if len(candidates) != 1:
            return "MISSING_SOURCE", None, decision_at
        row = candidates[0]
        if row.get("disposition") != "ELIGIBLE":
            return "MISSING_SOURCE", None, decision_at
        if (row.get("actual_identity_hash"), row.get("exchange_session_date"), row.get("tick_size"), row.get("point_value"), row.get("tick_value"), row.get("currency")) != identity:
            return "ROLL_UNRESOLVED", None, decision_at
        value = row.get("open_nano")
        if type(value) is not int or value <= 0:
            return "MISSING_SOURCE", None, decision_at
        prices[event] = value
    start, end = prices[start_at], prices[end_at]
    return "MATURED", end / start - 1.0, decision_at


def _load_rows(parquet_path: Path) -> list[Mapping[str, object]]:
    try:
        import pyarrow.parquet as pq
        reader = pq.ParquetFile(parquet_path)
        if not set(_REQUIRED_COLUMNS).issubset(reader.schema_arrow.names):
            raise IntegrityError("active Phase 3 input is missing required outcome columns")
        rows: list[Mapping[str, object]] = []
        for batch in reader.iter_batches(batch_size=65_536, columns=list(_REQUIRED_COLUMNS)):
            rows.extend(batch.to_pylist())
    except IntegrityError:
        raise
    except Exception as exc:
        raise IntegrityError("active Phase 3 input could not be read") from exc
    if not rows:
        raise IntegrityError("active Phase 3 input has no rows")
    return rows


def build_active_phase3_outcomes(*, boundary: RepoBoundary, validation: ActivePhase3MechanicsValidation) -> dict[str, str | int]:
    """Read the one bound active input and create an immutable outcome-only release."""
    if type(validation) is not ActivePhase3MechanicsValidation:
        raise ContractError("active Phase 3 outcome build requires exact mechanics preflight")
    parquet = boundary.assert_active_path(boundary.active_root / validation.active_input.parquet_path, purpose="active Phase 3 outcomes parquet", subtree="data/active")
    rows = _load_rows(parquet)
    if any(row.get("market") != validation.active_input.market for row in rows):
        raise IntegrityError("active Phase 3 input market differs from its binding")
    if any(type(row.get("event_at_ns")) is not int for row in rows):
        raise IntegrityError("active Phase 3 input event identity is invalid")
    if any(rows[index - 1]["event_at_ns"] > rows[index]["event_at_ns"] for index in range(1, len(rows))):
        raise IntegrityError("active Phase 3 input is not ordered by event time")
    groups: dict[int, list[Mapping[str, object]]] = {}
    for row in rows:
        groups.setdefault(int(row["event_at_ns"]), []).append(row)
    outcomes: list[dict[str, object]] = []
    for row in rows:
        status, price_return, decision_at = _row_status(row, groups)
        outcomes.append({
            "source_bar_event_at_ns": row["event_at_ns"],
            "decision_at_ns": decision_at,
            "entry_at_ns": decision_at + 60 * _NS_PER_SECOND,
            "label_unlock_at_ns": decision_at + 300 * _NS_PER_SECOND,
            "status": status,
            "price_return": price_return,
            "actual_identity_hash": row["actual_identity_hash"],
            "exchange_session_date": row["exchange_session_date"],
            "upstream_source_row_sha256": row["source_row_sha256"],
        })
    stage = boundary.active_root / "state" / "data_publication_staging" / f"active_phase3_outcomes-{uuid.uuid4().hex}"
    stage.mkdir(parents=True)
    outcome_stage = stage / "outcomes.parquet"
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        pq.write_table(pa.Table.from_pylist(outcomes), outcome_stage, compression="zstd")
    except Exception as exc:
        raise IntegrityError("active Phase 3 outcome release could not be staged") from exc
    file_hash = sha256_file(outcome_stage)
    market, year = validation.active_input.market, validation.active_input.year
    logical_path = f"data/outcomes/{LABEL_METHOD_ID}/{market}/{year}/{year}/outcomes.parquet"
    core = {"schema_version": SCHEMA_VERSION, "label_method_id": LABEL_METHOD_ID, "logical_path": logical_path, "outcomes_sha256": file_hash, "outcome_count": len(outcomes), "input_id": validation.active_input.input_id, "input_record_sha256": validation.input_record_sha256, "entry_delay_seconds": 60, "label_horizon_seconds": 300}
    release_id = sha256_json(core)
    target = boundary.active_root / "data" / "outcomes" / LABEL_METHOD_ID / market / str(year) / str(year) / release_id / "outcomes.parquet"
    manifest_path = boundary.active_root / "manifests" / "data_releases" / "outcomes" / f"{release_id}.json"
    report_path = boundary.active_root / "reports" / "phase3_outcomes" / "tier1_core" / market / str(year) / release_id / "report.json"
    if target.exists() or manifest_path.exists() or report_path.exists():
        raise IntegrityError("active Phase 3 outcome target already exists")
    target.parent.mkdir(parents=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True)
    shutil.copy2(outcome_stage, target)
    report = {**core, "release_id": release_id, "decision_time_basis": "first_minute_boundary_at_or_after_available_at_ns", "matured_count": sum(item["status"] == "MATURED" for item in outcomes), "missing_source_count": sum(item["status"] == "MISSING_SOURCE" for item in outcomes), "roll_unresolved_count": sum(item["status"] == "ROLL_UNRESOLVED" for item in outcomes), "mechanics_only": False, "model_fitting": False, "prediction_generation": False, "economics_evaluation": False}
    manifest = {"schema_version": SCHEMA_VERSION, "release_id": release_id, "source_active_input_id": validation.active_input.input_id, "source_parquet_sha256": validation.active_input.parquet_sha256, "files": [{"logical_path": logical_path, "sha256": file_hash, "size": target.stat().st_size}], "metadata": report}
    manifest_path.write_bytes(canonical_bytes(manifest) + b"\n")
    report_path.write_bytes(canonical_bytes(report) + b"\n")
    shutil.rmtree(stage)
    return {"release_id": release_id, "outcome_count": len(outcomes), "manifest_path": manifest_path.relative_to(boundary.active_root).as_posix(), "report_path": report_path.relative_to(boundary.active_root).as_posix()}
