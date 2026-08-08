from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.causal_market_year_materialization import (
    APPROVAL_VERSION,
    OPERATION,
    IntervalSource,
    MarketYearTarget,
    _materialize_parquet,
    _schema_fingerprint,
    build_approval_draft,
    build_plan,
    group_market_year_sources,
    partition_price_research_targets,
    verify_approval,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _source(
    *,
    market: str = "ES",
    year: int = 2026,
    start: str,
    end: str,
    release: str,
    coverage_disposition: str = "AUTHORITATIVE_INTERVAL",
    research_in_scope: bool = True,
    research_disposition: str = "ELIGIBLE",
    status_gated_feature_ready_rows: int = 5,
) -> IntervalSource:
    return IntervalSource(
        market=market,
        year=year,
        start=start,
        end=end,
        coverage_disposition=coverage_disposition,
        release_id=release,
        manifest_path=f"manifests/data_releases/causally_gated_normalized/{release}.json",
        manifest_sha256="f" * 64,
        bars_logical_path=(
            f"data/causally_gated_normalized/{market}/{year}/"
            f"{start}_{end}/bars.parquet"
        ),
        bars_physical_path=Path("data") / release / "bars.parquet",
        bars_sha256="a" * 64,
        bars_size=100,
        receipt_logical_path=(
            f"data/causally_gated_normalized/{market}/{year}/"
            f"{start}_{end}/causal_interval_receipt.json"
        ),
        receipt_physical_path=Path("data") / release / "causal_interval_receipt.json",
        receipt_sha256="b" * 64,
        receipt_size=10,
        row_count=5,
        schema_fingerprint="c" * 64,
        research_in_scope=research_in_scope,
        research_disposition=research_disposition,
        research_scope_policy_hash="d" * 64,
        status_epoch_gate_id="e" * 64,
        status_gated_feature_ready_rows=status_gated_feature_ready_rows,
    )


def test_group_market_year_sources_builds_one_contiguous_target() -> None:
    first = _source(
        start="2026-01-01", end="2026-06-13", release="1" * 64
    )
    second = _source(
        start="2026-06-13", end="2026-07-14", release="2" * 64
    )

    targets = group_market_year_sources((second, first))

    assert len(targets) == 1
    assert targets[0].logical_path == (
        "data/causally_gated_normalized/ES/2026/2026.parquet"
    )
    assert targets[0].row_count == 10
    assert [item.release_id for item in targets[0].sources] == [
        "1" * 64,
        "2" * 64,
    ]


@pytest.mark.parametrize(
    ("first_end", "second_start"),
    [
        ("2026-06-12", "2026-06-13"),
        ("2026-06-14", "2026-06-13"),
    ],
)
def test_group_market_year_sources_rejects_gap_or_overlap(
    first_end: str, second_start: str
) -> None:
    first = _source(
        start="2026-01-01", end=first_end, release="1" * 64
    )
    second = _source(
        start=second_start, end="2026-07-14", release="2" * 64
    )

    with pytest.raises(IntegrityError, match="not exactly contiguous"):
        group_market_year_sources((first, second))


def test_partition_price_research_targets_excludes_quarantined_market_year() -> None:
    eligible = MarketYearTarget(
        "ES",
        2026,
        (
            _source(
                start="2026-01-01",
                end="2027-01-01",
                release="1" * 64,
            ),
        ),
    )
    quarantined = _source(
        market="NQ",
        start="2026-01-01",
        end="2027-01-01",
        release="2" * 64,
        coverage_disposition="QUARANTINED_PENDING_REVALIDATION",
        research_in_scope=False,
        research_disposition="ABSTAIN_PRE_STATUS_CAPABILITY_EPOCH",
        status_gated_feature_ready_rows=0,
    )

    selected, excluded = partition_price_research_targets(
        (eligible, MarketYearTarget("NQ", 2026, (quarantined,)))
    )

    assert [(item.market, item.year) for item in selected] == [("ES", 2026)]
    assert [(item.market, item.year) for item in excluded] == [("NQ", 2026)]


def test_pre_status_authoritative_target_is_included_as_price_only() -> None:
    source = _source(
        year=2024,
        start="2024-01-01",
        end="2025-01-01",
        release="3" * 64,
        research_in_scope=False,
        research_disposition="ABSTAIN_PRE_STATUS_CAPABILITY_EPOCH",
        status_gated_feature_ready_rows=0,
    )
    target = MarketYearTarget("ES", 2024, (source,))

    selected, excluded = partition_price_research_targets((target,))

    assert selected == (target,)
    assert excluded == ()
    assert target.research_capability == "CAUSAL_PRICE_ONLY"
    assert target.status_research_eligible is False


def test_approval_is_exactly_hash_bound() -> None:
    plan = build_plan({"foundation_release_id": "d" * 64})
    pending = build_approval_draft(plan)
    with pytest.raises(UnauthorizedOperation, match="exact hash-bound approval"):
        verify_approval(pending, plan)

    core = {
        "approval_version": APPROVAL_VERSION,
        "approved_at": "2026-07-25T00:00:00Z",
        "materialization_plan_id": plan["materialization_plan_id"],
        "operation": OPERATION,
        "scope": plan["scope"],
        "status": "APPROVED",
        "user_authorization_id": "e" * 64,
    }
    approval = {**core, "approval_receipt_id": sha256_json(core)}
    assert verify_approval(approval, plan) == approval["approval_receipt_id"]

    approval["scope"] = {"foundation_release_id": "f" * 64}
    with pytest.raises(UnauthorizedOperation):
        verify_approval(approval, plan)

    superseded = dict(plan)
    superseded["materialization_plan_version"] = "1.0.0"
    with pytest.raises(UnauthorizedOperation, match="plan is invalid"):
        verify_approval(pending, superseded)


def test_materialize_parquet_merges_contiguous_sources(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            pa.field("event_at_ns", pa.int64(), nullable=False),
            pa.field("value", pa.int64(), nullable=False),
        ],
        metadata={b"schema_id": b"TEST_CAUSAL"},
    )
    first_path = tmp_path / "first.parquet"
    second_path = tmp_path / "second.parquet"
    pq.write_table(
        pa.Table.from_arrays(
            [pa.array([1, 2], type=pa.int64()), pa.array([10, 20], type=pa.int64())],
            schema=schema,
        ),
        first_path,
    )
    pq.write_table(
        pa.Table.from_arrays(
            [pa.array([3, 4], type=pa.int64()), pa.array([30, 40], type=pa.int64())],
            schema=schema,
        ),
        second_path,
    )

    def source(path: Path, start: str, end: str, release: str) -> IntervalSource:
        return IntervalSource(
            market="ES",
            year=2026,
            start=start,
            end=end,
            coverage_disposition="AUTHORITATIVE_INTERVAL",
            release_id=release,
            manifest_path=f"manifests/{release}.json",
            manifest_sha256="f" * 64,
            bars_logical_path=f"data/{path.name}",
            bars_physical_path=path,
            bars_sha256=sha256_file(path),
            bars_size=path.stat().st_size,
            receipt_logical_path="data/receipt.json",
            receipt_physical_path=tmp_path / "receipt.json",
            receipt_sha256="b" * 64,
            receipt_size=1,
            row_count=2,
            schema_fingerprint=_schema_fingerprint(path),
            research_in_scope=True,
            research_disposition="ELIGIBLE",
            research_scope_policy_hash="c" * 64,
            status_epoch_gate_id="d" * 64,
            status_gated_feature_ready_rows=2,
        )

    target = MarketYearTarget(
        "ES",
        2026,
        (
            source(first_path, "2026-01-01", "2026-06-13", "1" * 64),
            source(second_path, "2026-06-13", "2026-07-14", "2" * 64),
        ),
    )
    destination = tmp_path / "output" / "2026.parquet"

    _materialize_parquet(target, destination)

    observed = pq.read_table(destination)
    assert observed.column("event_at_ns").to_pylist() == [1, 2, 3, 4]
    assert observed.column("value").to_pylist() == [10, 20, 30, 40]
