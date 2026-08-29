from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from futures_rebuild.causal_incremental_release import (
    ACTIVE_RELEASE_MARKETS,
    COMPLETION_PRIORITY_ORDER,
    MARKET_PLAN_SCHEMA,
    TIER1_MARKETS,
    build_certificate_package,
    build_snapshot,
    load_contract,
    next_priority_market,
)
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from futures_rebuild.active_data_certification import canonical_parquet_sequence_fingerprint
from futures_rebuild.active_data_view import (
    build_access_policy_binding,
    cohort_for_coverage,
    cohort_for_year,
    materialize_parquet,
    validate_access_policy_binding,
)
from futures_rebuild.canonical import io_path, sha256_file


ROOT = Path(__file__).resolve().parents[1]


def test_contract_matches_the_six_market_active_alpha_ladder() -> None:
    contract = load_contract(ROOT)

    assert COMPLETION_PRIORITY_ORDER == ("ES", "GC", "6E", "CL", "ZN", "NQ")
    assert contract["completion_priority_order"] == list(COMPLETION_PRIORITY_ORDER)
    assert ACTIVE_RELEASE_MARKETS == ("ES", "NQ", "CL", "GC", "ZN", "6E")
    assert TIER1_MARKETS == ACTIVE_RELEASE_MARKETS
    assert contract["active_release_markets"] == list(ACTIVE_RELEASE_MARKETS)
    assert contract["alpha_tier1_semantics"]["fixed_additions"] == ["NQ", "CL", "GC"]
    assert contract["alpha_tier1_semantics"]["macro_candidates"] == ["ZN", "6E"]
    assert contract["publication_authorized"] is False
    assert contract["activation_authorized"] is False
    assert contract["materialized_year_range"] == [2010, 2025]
    assert contract["development_end_exclusive"] == "2025-07-13T22:00:00Z"
    assert contract["protected_years"] == []


def test_2025_is_admitted_only_as_the_bounded_development_prefix() -> None:
    assert cohort_for_year(2025) == "LOCKED_HOLDOUT"
    assert cohort_for_coverage(
        year=2025,
        coverage_start="2025-01-01",
        coverage_end="2025-07-13T220000Z",
    ) == "NON_PRISTINE_RESEARCH"
    assert cohort_for_coverage(
        year=2025,
        coverage_start="2025-01-01T00:00:00Z",
        coverage_end="2025-07-13T22:00:01Z",
    ) == "LOCKED_HOLDOUT"


def test_2025_access_binding_is_interval_bound_and_fails_on_drift() -> None:
    binding = build_access_policy_binding(
        market="ES",
        year=2025,
        universe_contract_sha256="a" * 64,
        active_view_id="b" * 64,
        content_validation_receipt_id="c" * 64,
        coverage_start="2025-01-01T000000Z",
        coverage_end="2025-07-13T220000Z",
    )
    assert validate_access_policy_binding(binding) == binding["access_policy_binding_id"]

    drifted = {**binding, "coverage_end": "2025-07-13T220001Z"}
    with pytest.raises((IntegrityError, UnauthorizedOperation)):
        validate_access_policy_binding(drifted)


@pytest.mark.parametrize(
    "certificate_paths",
    [
        {"ES": "es.json", "NQ": "nq.json", "CL": "cl.json", "GC": "gc.json", "6E": "6e.json"},
        {
            "ES": "es.json",
            "NQ": "nq.json",
            "CL": "cl.json",
            "GC": "gc.json",
            "ZN": "zn.json",
            "6E": "6e.json",
            "RTY": "rty.json",
        },
        {"NQ": "nq.json", "ES": "es.json", "CL": "cl.json", "GC": "gc.json", "ZN": "zn.json", "6E": "6e.json"},
    ],
)
def test_certificate_package_fails_closed_on_missing_extra_or_reordered_market(
    certificate_paths: dict[str, str],
) -> None:
    with pytest.raises(ContractError, match="ordered ES, NQ, CL, GC, ZN, and 6E"):
        build_certificate_package(
            repository_root=ROOT, certificate_paths=certificate_paths
        )


def _market_plan(market: str, package_id: str) -> dict[str, object]:
    return {
        "schema_version": MARKET_PLAN_SCHEMA,
        "package_id": package_id,
        "market": market,
        "market_plan_id": market.lower().ljust(64, "0"),
        "year_count": 16,
        "publication_authorized": False,
        "activation_authorized": False,
    }


def test_snapshot_is_all_and_only_the_six_active_ladder_markets() -> None:
    package = {"package_id": "a" * 64}
    snapshot = build_snapshot(
        package=package,
        market_plans=[_market_plan(market, "a" * 64) for market in TIER1_MARKETS],
    )

    assert snapshot["markets"] == list(ACTIVE_RELEASE_MARKETS)
    assert snapshot["results_may_claim_full_41_market_coverage"] is False
    assert snapshot["publication_authorized"] is False
    assert snapshot["activation_authorized"] is False


def test_snapshot_rejects_an_extra_market() -> None:
    package = {"package_id": "a" * 64}
    plans = [_market_plan(market, "a" * 64) for market in TIER1_MARKETS]
    plans.append(_market_plan("RTY", "a" * 64))

    with pytest.raises(UnauthorizedOperation, match="exactly six markets"):
        build_snapshot(package=package, market_plans=plans)


def test_priority_sequence_advances_zn_then_nq_after_cl() -> None:
    assert next_priority_market(("ES", "GC", "6E", "CL")) == "ZN"
    assert next_priority_market(("ES", "GC", "6E", "CL", "ZN")) == "NQ"
    assert next_priority_market(COMPLETION_PRIORITY_ORDER) is None


def test_priority_sequence_rejects_nq_before_zn() -> None:
    with pytest.raises(ContractError, match="exact priority prefix"):
        next_priority_market(("ES", "GC", "6E", "CL", "NQ"))


def test_materializer_handles_deep_windows_paths(tmp_path: Path) -> None:
    deep = tmp_path.joinpath(*(["x" * 40] * 6))
    io_path(deep).mkdir(parents=True)
    schema = pa.schema(
        [
            ("bar_start_ns", pa.int64()),
            ("source_row_sha256", pa.binary(32)),
        ]
    )
    sources = [deep / "one.parquet", deep / "two.parquet"]
    pq.write_table(pa.Table.from_pylist([
        {"bar_start_ns": 1, "source_row_sha256": b"a" * 32}
    ], schema=schema), io_path(sources[0]))
    pq.write_table(pa.Table.from_pylist([
        {"bar_start_ns": 2, "source_row_sha256": b"b" * 32}
    ], schema=schema), io_path(sources[1]))
    fingerprint = canonical_parquet_sequence_fingerprint(
        [io_path(path) for path in sources]
    )
    destination = deep / "annual" / "2018.parquet"

    _, row_hash, row_count = materialize_parquet(
        sources=sources,
        source_sha256s=[sha256_file(path) for path in sources],
        destination=destination,
        expected_row_count=2,
        expected_schema_fingerprint=str(fingerprint["schema_fingerprint"]),
    )

    assert row_count == 2
    assert row_hash == fingerprint["canonical_row_hash"]
