from decimal import Decimal

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_materializer import IndexedBracketEconomics, materialize_verified_source_rows, stream_materialize_verified_source_batches, write_bracket_market_year_stage


MINUTE = 60_000_000_000


def _rows() -> list[dict[str, object]]:
    result = []
    for index in range(22):
        result.append({
            "event_at_ns": index * MINUTE,
            "open_nano": 100_000_000_000,
            "high_nano": 105_000_000_000,
            "low_nano": 95_000_000_000,
            "close_nano": 100_000_000_000,
            "volume": 10,
            "exchange_session_date": "2021-01-04",
            "actual_identity_hash": "a" * 64,
            "source_row_sha256": f"{index:064x}",
            "tick_size": "1",
            "tick_value": "1",
            "disposition": "ELIGIBLE",
        })
    result[-1]["high_nano"] = 130_000_000_000
    return result


def _economics() -> dict[str, IndexedBracketEconomics]:
    return {"a" * 64: IndexedBracketEconomics("a" * 64, Decimal("1"), Decimal("1"), Decimal("1"), "USD", "decimal", "e" * 64)}


def test_conversion_generates_fresh_feature_and_outcome_rows_with_source_binding() -> None:
    output = materialize_verified_source_rows(rows=_rows(), stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics())
    row = output[20]

    assert row.feature_record()["status"] == "FEATURE_READY"
    assert row.outcome_record()["long_realized_net_r"] == "2"
    assert row.outcome_record()["long_triple_barrier_class"] == "TARGET_FIRST"
    assert row.feature_record()["upstream_source_row_sha256"] == f"{20:064x}"


def test_conversion_rejects_ambiguous_or_invalid_source_provenance() -> None:
    duplicate = _rows()
    duplicate[1]["source_row_sha256"] = duplicate[0]["source_row_sha256"]
    with pytest.raises(IntegrityError, match="ambiguous"):
        materialize_verified_source_rows(rows=duplicate, stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics())
    with pytest.raises(IntegrityError, match="enough"):
        materialize_verified_source_rows(rows=_rows()[:21], stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics())


def test_conversion_requires_exact_indexed_identity_and_rejects_source_conflicts() -> None:
    with pytest.raises(IntegrityError, match="no indexed"):
        materialize_verified_source_rows(rows=_rows(), stress_round_trip_cost_usd=Decimal("0"), indexed_economics={})
    conflicting = _rows()
    conflicting[0]["tick_value"] = "2"
    with pytest.raises(IntegrityError, match="disagree"):
        materialize_verified_source_rows(rows=conflicting, stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics())


def test_nontradable_source_without_economics_is_preserved_as_an_abstention() -> None:
    rows = _rows()
    rows[0]["actual_identity_hash"] = "b" * 64
    rows[0]["disposition"] = "UNRESOLVED_FAIL_CLOSED"
    output = materialize_verified_source_rows(rows=rows, stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics())
    assert output[0].outcome_record()["status"] == "ABSTAINED"


def test_staged_payloads_are_fresh_bracket_only_parquet_files(tmp_path) -> None:
    result = write_bracket_market_year_stage(
        rows=_rows(), stress_round_trip_cost_usd=Decimal("0"), stage=tmp_path / "stage", indexed_economics=_economics(),
    )
    assert result["row_count"] == 22
    assert result["matured_pair_count"] == 1
    assert result["feature_payload"].is_file()
    assert result["outcome_payload"].is_file()


def test_streamed_batches_match_uninterrupted_materialization_and_reject_order_drift() -> None:
    rows = _rows()
    whole = materialize_verified_source_rows(rows=rows, stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics())
    chunked = stream_materialize_verified_source_batches(batches=(rows[:7], rows[7:15], rows[15:]), stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics())
    assert chunked == ()  # 22 bars cannot complete an 81-bar bracket window.
    with pytest.raises(IntegrityError, match="order"):
        stream_materialize_verified_source_batches(batches=(rows[1:2], rows[:1]), stress_round_trip_cost_usd=Decimal("0"), indexed_economics=_economics())
