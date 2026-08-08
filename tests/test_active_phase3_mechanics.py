from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild.active_phase3_input import ActivePhase3Input
from futures_rebuild.active_phase3_mechanics import (
    REPORT_RELATIVE_PATH,
    run_active_phase3_mechanics_check,
)
from futures_rebuild.active_phase3_validation import ActivePhase3MechanicsValidation
from futures_rebuild.errors import ContractError, IntegrityError


def _validation(boundary) -> ActivePhase3MechanicsValidation:
    relative = "data/active/causally_gated_normalized/ES/2019/2019.parquet"
    path = boundary.active_root / relative
    path.parent.mkdir(parents=True)
    pq.write_table(
        pa.table(
            {
                "market": ["ES"] * 5,
                "event_at_ns": [100, 200, 300, 400, 500],
                "available_at_ns": [101, 201, 301, 401, 501],
                "open_nano": [1, 2, 3, 4, 5],
                "close_nano": [2, 3, 4, 5, 6],
                "disposition": ["ELIGIBLE"] * 5,
                "actual_identity_hash": ["a" * 64] * 5,
                "exchange_session_date": ["2019-01-02"] * 5,
                "tick_size": [1] * 5,
                "point_value": [50] * 5,
                "tick_value": [50] * 5,
                "currency": ["USD"] * 5,
            }
        ),
        path,
    )
    return ActivePhase3MechanicsValidation(
        active_input=ActivePhase3Input(
            market="ES",
            year=2019,
            parquet_path=relative,
            parquet_sha256="a" * 64,
            sidecar_sha256="b" * 64,
            causal_release_id="c" * 64,
            source_raw_release_id="d" * 64,
            active_view_id="e" * 64,
        ),
        input_record_path="manifests/phase3_inputs/record.json",
        input_record_sha256="f" * 64,
    )


def test_writes_one_bounded_mechanics_only_report(boundary) -> None:
    validation = _validation(boundary)

    report = run_active_phase3_mechanics_check(
        boundary=boundary, validation=validation, row_read_cap=3
    )

    payload = json.loads((boundary.active_root / REPORT_RELATIVE_PATH).read_text(encoding="utf-8"))
    assert report.rows_read == 3
    assert payload["mechanics_only"] is True
    assert payload["checks"] == {
        "causal_availability": True,
        "market_matches": True,
        "ordered_by_event_time": True,
        "required_fields_present": True,
        "row_cap_respected": True,
    }


def test_rejects_output_reuse_or_invalid_cap(boundary) -> None:
    validation = _validation(boundary)
    with pytest.raises(ContractError, match="between one and 512"):
        run_active_phase3_mechanics_check(boundary=boundary, validation=validation, row_read_cap=513)

    run_active_phase3_mechanics_check(boundary=boundary, validation=validation)
    with pytest.raises(IntegrityError, match="root must be absent"):
        run_active_phase3_mechanics_check(boundary=boundary, validation=validation)
