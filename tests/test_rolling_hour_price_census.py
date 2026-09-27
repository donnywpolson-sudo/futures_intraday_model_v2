from datetime import datetime, timezone
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild import rolling_hour_price_census as census
from futures_rebuild.boundary import OperationClassification, OperationReceipt, RepoBoundary
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.causal_observation_parquet import SCHEMAS
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def row(market="ES", year=2018, minute=0):
    start = int(datetime(year, 1, 2, tzinfo=timezone.utc).timestamp()) * 10**9
    return dict.fromkeys(census.COLUMNS, "a" * 64) | {
        "market": market, "source_cadence": "1m", "bar_start_ns": start + minute*census.MINUTE,
        "bar_end_ns": start+(minute+1)*census.MINUTE, "source_timestamp_ns": start+minute*census.MINUTE,
        "available_at_ns": start+(minute+1)*census.MINUTE+5_000_000_000,
        "decision_eligible_at_ns": start+(minute+1)*census.MINUTE+5_000_000_000,
        "actual_contract": market+"H8", "raw_symbol": market+"H8",
        "definition_received_at_ns": start-1, "listing_activation_ns": start-2,
        "expiration_ns": start+86400*10**9, "project_trade_date": f"{year}-01-02",
        "project_grouping_start_ns": start, "project_grouping_end_ns": start+86400*10**9,
        "official_schedule_state": "UNKNOWN_FAIL_CLOSED"}


@pytest.mark.current
def test_census_gaps_and_future_known_metadata():
    rows = [row(minute=i) for i in (0, 1, 3)]
    rows[-1]["definition_received_at_ns"] = rows[-1]["bar_start_ns"]+1
    result = census.summarize(iter(rows), {"market": "ES", "year": 2018, "row_count": 3})
    assert result["exceptions"] == {"definition_known_at_bar_start": 1}
    assert result["dates"]["2018-01-02"]["unclassified_internal_gap_minutes"] == 1
    assert result["dates"]["2018-01-02"]["max_consecutive_same_contract_bars"] == 2
    with pytest.raises(IntegrityError, match="count differs"):
        census.summarize(iter(rows), {"market": "ES", "year": 2018, "row_count": 4})
    with pytest.raises(IntegrityError, match="row budget"):
        census.summarize(iter(rows), {"market": "ES", "year": 2018, "row_count": 2})


@pytest.mark.current
def test_complete_synthetic_census_and_one_use_boundary(tmp_path, monkeypatch):
    root = tmp_path
    (root / "data/active").mkdir(parents=True)
    (root / "data/active/catalog.json").write_text("{}")
    policy = root / "src/futures_rebuild/research_gateway_policy.py"
    policy.parent.mkdir(parents=True)
    policy.write_text("synthetic policy binding")
    sources = []
    for market in ("ES", "NQ", "CL", "GC"):
        for year in range(2018, 2023):
            path = root / f"data/active/{market}_{year}.parquet"
            schema = pa.schema([SCHEMAS["observations"].field(k) for k in census.COLUMNS])
            physical = [{k:bytes.fromhex(v) if pa.types.is_fixed_size_binary(schema.field(k).type) else v
                for k,v in row(market, year, i).items()} for i in (0, 1, 3)]
            pq.write_table(pa.Table.from_pylist(physical, schema=schema), path)
            manifest = path.with_suffix(".json")
            manifest.write_text("{}")
            sources.append({"market": market, "year": year, "row_count": 3,
                "parquet_path": path.relative_to(root).as_posix(), "physical_bytes": path.stat().st_size,
                "parquet_sha256": sha256_file(path), "sidecar_path": manifest.relative_to(root).as_posix(),
                "sidecar_sha256": sha256_file(manifest)})
    plan = {"purpose": "SYNTHETIC_TEST", "exact_sources": sources, "output_directory": "evidence",
        "row_limit": 60, "hard_stop_minutes": 1, "working_memory_bytes_max": 2**31,
        "estimated_output_bytes_max": 2**26}
    bundle = {"readiness_preparation": {"price_catalog_sha256": sha256_file(root / "data/active/catalog.json")}}
    monkeypatch.setattr(census, "approved_plan", lambda root: (bundle, plan))
    calls = []

    def resolver(**args):
        calls.append((args["market"], args["year"]))
        assert args["purpose"] == "LINEAGE_AUDIT"
        return root / f"data/active/{args['market']}_{args['year']}.parquet"

    monkeypatch.setattr(census, "resolve", resolver)
    boundary = RepoBoundary(root)
    local = OperationReceipt.issue_local(boundary, operation=census.OPERATION,
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY, scope={})
    with pytest.raises(UnauthorizedOperation):
        census.execute_once(root, local)
    assert not calls
    identity = sha256_json(plan)
    receipt = OperationReceipt.issue_user_approved(boundary, operation=census.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=census.receipt_scope(root, plan), approval_command=census.OPERATION,
        approval_plan_id=identity, approval_plan_sha256=identity,
        approval_line=f"APPROVE {census.OPERATION} PLAN {identity} SHA256 {identity}")
    report = census.execute_once(root, receipt)
    assert len(calls) == 20 and report["rows"] == 60 and report["files_completed"] == 20
    assert report["metadata_exception_counts"] == {}
    assert all(r["observed_project_dates"] == 5 and r["zero_row_calendar_dates_unclassified"] == 1821
        for r in report["markets"].values())
    assert json.loads((root / "evidence/report.json").read_bytes()) == report
    with pytest.raises((UnauthorizedOperation, FileExistsError)):
        census.execute_once(root, receipt)
    assert len(calls) == 20
