from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_v5 import NS_PER_MINUTE
from futures_rebuild.tier1_bracket_v10 import (
    SourceIntegrityAuditV10,
    normalize_source_mappings_v10,
)
from futures_rebuild.tier1_frozen_successor_source_semantics import (
    select_reported_execution_path,
    select_reported_feature_window,
)


ROOT = Path(__file__).resolve().parents[1]
DECISION = 1_600_020_000_000_000_000


def _mapping(
    event: int, *, disposition: str = "ELIGIBLE", identity: str = "b" * 64,
    volume: int = 10,
) -> dict[str, object]:
    return {
        "event_at_ns": event,
        "exchange_session_date": "2020-01-02",
        "source_row_sha256": f"{abs(event // NS_PER_MINUTE):064x}"[-64:],
        "disposition": disposition,
        "prediction_in_coverage_denominator": True,
        "failure_code": "NONE" if disposition == "ELIGIBLE" else "UNRESOLVED",
        "failure_detail_sha256": "a" * 64,
        "actual_identity_hash": identity if disposition == "ELIGIBLE" else None,
        "open_nano": 100_000_000_000,
        "high_nano": 101_000_000_000,
        "low_nano": 99_000_000_000,
        "close_nano": 100_000_000_000,
        "volume": volume,
        "tick_size": "0.25",
        "tick_value": "12.50",
        "point_value": "50",
    }


def _rows(
    offsets: list[int], *, foreign_offset: int | None = None,
    nonexec_offset: int | None = None,
):
    mappings = [
        _mapping(
            DECISION + offset * NS_PER_MINUTE,
            disposition=(
                "UNRESOLVED_FAIL_CLOSED" if offset == nonexec_offset else "ELIGIBLE"
            ),
            identity="c" * 64 if offset == foreign_offset else "b" * 64,
            volume=index + 1,
        )
        for index, offset in enumerate(offsets)
    ]
    return tuple(normalize_source_mappings_v10(
        market="ES", rows=iter(mappings), audit=SourceIntegrityAuditV10("ES"),
    ))


def test_feature_window_uses_sparse_reported_bars_without_forward_fill() -> None:
    offsets = [offset for offset in range(-64, -1) if offset not in {-30, -20}]
    result = select_reported_feature_window(
        source_rows=_rows(offsets), market="ES",
        exchange_session_date="2020-01-02", decision_at_ns=DECISION,
    )
    assert len(result.rows) == 61
    assert result.rows[-1].bar.event_at_ns == DECISION - 2 * NS_PER_MINUTE
    assert result.missing_clock_minutes == 2
    assert result.observed_bar_density == 61 / 63
    # No synthetic row was manufactured for either absent timestamp.
    observed = {row.bar.event_at_ns for row in result.rows if row.bar is not None}
    assert DECISION - 30 * NS_PER_MINUTE not in observed
    assert DECISION - 20 * NS_PER_MINUTE not in observed


def test_reported_features_use_observed_bar_ordinals_without_filling_gaps() -> None:
    from futures_rebuild.tier1_frozen_successor_source_semantics import (
        compute_reported_feature_values,
    )

    offsets = [offset for offset in range(-64, -1) if offset not in {-30, -20}]
    window = select_reported_feature_window(
        source_rows=_rows(offsets), market="ES",
        exchange_session_date="2020-01-02", decision_at_ns=DECISION,
    )
    result = compute_reported_feature_values(
        window=window, decision_at_ns=DECISION,
    )
    assert tuple(result.values) == (
        "bar_return_1", "return_5", "return_20", "intrabar_range_fraction",
        "atr_20_fraction", "range_to_atr_20", "realized_volatility_20",
        "log1p_volume", "volume_zscore_60", "session_minute_sin",
        "session_minute_cos",
    )
    assert result.atr > 0
    assert window.missing_clock_minutes == 2


def test_feature_selection_is_unchanged_by_future_rows() -> None:
    offsets = list(range(-62, -1))
    base = select_reported_feature_window(
        source_rows=_rows(offsets), market="ES",
        exchange_session_date="2020-01-02", decision_at_ns=DECISION,
    )
    mutated = select_reported_feature_window(
        source_rows=_rows([*offsets, 1, 2, 3]), market="ES",
        exchange_session_date="2020-01-02", decision_at_ns=DECISION,
    )
    assert [row.source_row_sha256 for row in base.rows] == [
        row.source_row_sha256 for row in mutated.rows
    ]


@pytest.mark.parametrize("kind", ["identity", "nonexec"])
def test_feature_span_fails_closed_on_qualified_identity_defect(kind: str) -> None:
    offsets = list(range(-63, -1))
    kwargs = {"foreign_offset": -20} if kind == "identity" else {"nonexec_offset": -20}
    with pytest.raises(IntegrityError, match="non-qualified|identity"):
        select_reported_feature_window(
            source_rows=_rows(offsets, **kwargs), market="ES",
            exchange_session_date="2020-01-02", decision_at_ns=DECISION,
        )


def test_feature_window_rejects_excessive_sparsity_and_staleness() -> None:
    sparse = list(range(-80, -19))
    with pytest.raises(IntegrityError, match="stale|sparse"):
        select_reported_feature_window(
            source_rows=_rows(sparse), market="ES",
            exchange_session_date="2020-01-02", decision_at_ns=DECISION,
        )


def test_execution_allows_no_trade_gaps_but_requires_exact_real_entry() -> None:
    offsets = [1, 2, 5, 20, 40, 60, 61]
    result = select_reported_execution_path(
        source_rows=_rows(offsets), market="ES",
        exchange_session_date="2020-01-02", decision_at_ns=DECISION,
    )
    assert result.entry_bar.event_at_ns == DECISION + NS_PER_MINUTE
    assert result.liquidation_at_ns == DECISION + 61 * NS_PER_MINUTE
    assert result.missing_clock_minutes == 61 - len(offsets)
    with pytest.raises(IntegrityError, match="entry"):
        select_reported_execution_path(
            source_rows=_rows(offsets[1:]), market="ES",
            exchange_session_date="2020-01-02", decision_at_ns=DECISION,
        )


def test_execution_uses_first_observed_bar_after_timeout_with_bounded_delay() -> None:
    result = select_reported_execution_path(
        source_rows=_rows([1, 2, 30, 60, 62]), market="ES",
        exchange_session_date="2020-01-02", decision_at_ns=DECISION,
    )
    assert result.timeout_at_ns == DECISION + 61 * NS_PER_MINUTE
    assert result.liquidation_at_ns == DECISION + 62 * NS_PER_MINUTE
    with pytest.raises(IntegrityError, match="liquidation"):
        select_reported_execution_path(
            source_rows=_rows([1, 2, 30, 60, 67]), market="ES",
            exchange_session_date="2020-01-02", decision_at_ns=DECISION,
        )


def test_execution_never_skips_nonqualified_or_foreign_identity_rows() -> None:
    offsets = [1, 2, 20, 40, 61]
    with pytest.raises(IntegrityError, match="non-qualified"):
        select_reported_execution_path(
            source_rows=_rows(offsets, nonexec_offset=20), market="ES",
            exchange_session_date="2020-01-02", decision_at_ns=DECISION,
        )
    with pytest.raises(IntegrityError, match="foreign identity"):
        select_reported_execution_path(
            source_rows=_rows(offsets, foreign_offset=20), market="ES",
            exchange_session_date="2020-01-02", decision_at_ns=DECISION,
        )
