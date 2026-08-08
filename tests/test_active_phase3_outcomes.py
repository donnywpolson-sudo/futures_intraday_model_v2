from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq

from futures_rebuild.active_phase3_input import ActivePhase3Input
from futures_rebuild.active_phase3_outcomes import build_active_phase3_outcomes
from futures_rebuild.active_phase3_validation import ActivePhase3MechanicsValidation


def test_builds_matured_and_missing_outcomes(boundary) -> None:
    path = boundary.active_root / "data/active/causally_gated_normalized/ES/2019/2019.parquet"
    path.parent.mkdir(parents=True)
    times = [index * 60_000_000_000 for index in range(7)]
    pq.write_table(pa.table({"market":["ES"]*7,"event_at_ns":times,"available_at_ns":[item + 1 for item in times],"open_nano":[100,101,102,103,104,105,106],"disposition":["ELIGIBLE"]*7,"actual_identity_hash":["a"*64]*7,"exchange_session_date":["2019-01-01"]*7,"tick_size":[1]*7,"point_value":[50]*7,"tick_value":[50]*7,"currency":["USD"]*7,"source_row_sha256":["b"*64]*7}), path)
    active = ActivePhase3Input("ES", 2019, path.relative_to(boundary.active_root).as_posix(), "a"*64, "b"*64, "c"*64, "d"*64, "e"*64)
    validation = ActivePhase3MechanicsValidation(active, "manifests/phase3_inputs/test.json", "f"*64)

    result = build_active_phase3_outcomes(boundary=boundary, validation=validation)

    report = json.loads((boundary.active_root / result["report_path"]).read_text(encoding="utf-8"))
    assert result["outcome_count"] == 7
    assert report["matured_count"] == 1
    assert report["missing_source_count"] == 6
    assert report["decision_time_basis"] == "first_minute_boundary_at_or_after_available_at_ns"
    assert report["model_fitting"] is False


def test_builds_non_es_outcomes_into_the_bound_market_year(boundary) -> None:
    path = boundary.active_root / "data/active/causally_gated_normalized/ZN/2021/2021.parquet"
    path.parent.mkdir(parents=True)
    times = [index * 60_000_000_000 for index in range(7)]
    pq.write_table(pa.table({"market":["ZN"]*7,"event_at_ns":times,"available_at_ns":[item + 1 for item in times],"open_nano":[100,101,102,103,104,105,106],"disposition":["ELIGIBLE"]*7,"actual_identity_hash":["a"*64]*7,"exchange_session_date":["2021-01-01"]*7,"tick_size":[1]*7,"point_value":[50]*7,"tick_value":[50]*7,"currency":["USD"]*7,"source_row_sha256":["b"*64]*7}), path)
    active = ActivePhase3Input("ZN", 2021, path.relative_to(boundary.active_root).as_posix(), "a"*64, "b"*64, "c"*64, "d"*64, "e"*64)
    validation = ActivePhase3MechanicsValidation(active, "manifests/phase3_inputs/direct_active_binding.json", "f"*64)

    result = build_active_phase3_outcomes(boundary=boundary, validation=validation)

    assert (boundary.active_root / "data/outcomes/active_es_60s_300s_v2/ZN/2021/2021" / result["release_id"] / "outcomes.parquet").is_file()
