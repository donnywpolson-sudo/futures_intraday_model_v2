"""Synthetic annual compaction, fail-closed resume, and protected coverage."""
import pytest
import runpy
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from futures_rebuild.canonical import sha256_file
from futures_rebuild.causal_compact_successor import catalog_entry, materialize_unit, sources_by_year
from futures_rebuild.active_data_view import CatalogEntry
from futures_rebuild.errors import IntegrityError

pytestmark = pytest.mark.high_risk


def test_annual_compaction_resume_and_corruption(tmp_path):
    sources = []
    for month, values in ((1, [1, 2]), (2, [3])):
        path = tmp_path / f"month{month}.parquet"
        pq.write_table(pa.table({"bar_start_ns": values, "source_row_sha256": ["a" * 64] * len(values)}), path)
        sources.append({"path": path.name, "sha256": sha256_file(path), "size": path.stat().st_size})
    unit = {"market": "ES", "year": 2018, "sources": sources, "row_count": 3,
            "coverage_start": "2018-01-01", "coverage_end": "2019-01-01", "cohort": "DISCOVERY_SELECTION"}
    plan = {"semantic_bindings": {}, "implementation_bindings": {}, "environment_bindings": {},
            "bindings": {"configs/research_universe_contract.json": "a" * 64}}
    stage, records = tmp_path / "stage", tmp_path / "records"
    output = stage / "causally_gated_normalized/ES/2018/2018.parquet"
    output.parent.mkdir(parents=True)
    output.write_bytes(b"interrupted synthetic output")
    entry, record = materialize_unit(tmp_path, stage, records, unit, plan, "b" * 64)
    assert entry.row_count == 3
    assert pq.read_table(output)["bar_start_ns"].to_pylist() == [1, 2, 3]
    assert next((records / "interrupted").rglob("*.parquet")).read_bytes() == b"interrupted synthetic output"
    timestamp = output.stat().st_mtime_ns
    assert materialize_unit(tmp_path, stage, records, unit, plan, "b" * 64) == (entry, record)
    assert output.stat().st_mtime_ns == timestamp
    output.write_bytes(b"corrupt synthetic checkpoint")
    with pytest.raises(IntegrityError, match="completed annual file changed"):
        materialize_unit(tmp_path, stage, records, unit, plan, "b" * 64)


def test_protected_coverage_rejected_before_payload_access():
    certificate = {"market": "ES", "certificate_id": "a" * 64,
                   "replay_evidence": {"output_inventory": [{"path": "absent/sealed/2025/2025-01-01_2026-01-01/observations.parquet"}]}}
    with pytest.raises(IntegrityError, match="protected coverage"):
        sources_by_year([certificate])


def test_protected_catalog_entry_preserved():
    entry = CatalogEntry(market="SR3", year=2010, coverage_start="2010-01-01", coverage_end="2011-01-01",
                         coverage_kind="PRE_EFFECTIVE", cohort="DATA_QUALITY_ONLY", disposition="QUARANTINED_NOT_MATERIALIZED",
                         selection_eligible=False, permitted_uses=(), source_bindings=(), reason="Not effective")
    assert catalog_entry(entry.as_dict()) == entry


def test_cutover_comparison_does_not_hide_price_changes(tmp_path):
    compare = runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/review_causal_compact_cutover.py"))["compare_annual_columns"]
    before, lineage, price = (tmp_path / name for name in ("before.parquet", "lineage.parquet", "price.parquet"))
    for path, release, close in ((before, "old", 10), (lineage, "new", 10), (price, "new", 11)):
        pq.write_table(pa.table({"source_release_id": [release], "close_nano": [close]}), path)
    assert compare(before, lineage)["values_equal_except_lineage_versions"]
    assert not compare(before, price)["values_equal_except_lineage_versions"]
