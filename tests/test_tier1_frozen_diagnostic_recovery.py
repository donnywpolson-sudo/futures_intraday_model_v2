from __future__ import annotations

from pathlib import Path

from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.foundation.decoder import ProviderObservationHeader
from futures_rebuild.tier1_frozen_diagnostic_recovery import (
    DiagnosticGapTarget,
    EXPECTED_GAP_COUNTS,
    SOURCE_ADEQUACY_RECORD_PATH,
    _load_frozen_inputs,
    classify_target_observations,
    derive_gap_targets,
    diagnostic_catalog,
    load_diagnostic_recovery_plan,
)


ROOT = Path(__file__).resolve().parents[1]


def _targets(root: Path = ROOT):
    source, checkpoints = _load_frozen_inputs(root=root)
    return derive_gap_targets(
        source_record=source, expected_checkpoints=checkpoints,
    )


def _synthetic_target() -> DiagnosticGapTarget:
    return DiagnosticGapTarget(
        opportunity_id="synthetic-gap",
        market="ES",
        year=2022,
        exchange_session_date="2022-06-01",
        checkpoint="10:00",
        decision_at_ns=1_000_000_000,
        category="ENTRY",
        source_reason="synthetic",
        window_start_ns=2_000_000_000,
        window_end_exclusive_ns=5_000_000_000,
    )


def _header(target, *, schema="ohlcv-1s", offset=0, instrument=17):
    return ProviderObservationHeader(
        market=target.market,
        schema=schema,
        event_at_ns=target.window_start_ns + offset,
        received_at_ns=None if schema == "ohlcv-1s" else target.window_start_ns + offset + 1,
        publisher_id=1,
        instrument_id=instrument,
        source_file_sha256="a" * 64,
    )


def _keys(value):
    if isinstance(value, dict):
        return set(value) | set().union(*(_keys(item) for item in value.values()), set())
    if isinstance(value, list):
        return set().union(*(_keys(item) for item in value), set())
    return set()


def test_exact_failed_feature_complete_target_set_is_frozen(
    local_evidence_root: Path,
) -> None:
    targets = _targets(local_evidence_root)
    assert len(targets) == 34
    assert len({item.opportunity_id for item in targets}) == 34
    assert {year for year in (item.year for item in targets)} <= set(range(2018, 2023))
    assert 2025 not in {item.year for item in targets}
    assert {
        category: sum(item.category == category for item in targets)
        for category in EXPECTED_GAP_COUNTS
    } == EXPECTED_GAP_COUNTS
    assert (local_evidence_root / SOURCE_ADEQUACY_RECORD_PATH).is_file()


def test_diagnostic_catalog_binds_only_target_cells_and_records_absent_trades(
    local_evidence_root: Path,
) -> None:
    targets = _targets(local_evidence_root)
    _, catalog = diagnostic_catalog(
        root=local_evidence_root,
        boundary=RepoBoundary(local_evidence_root),
        targets=targets,
    )
    assert len(catalog) == 28
    assert sum(item["status"] == "BOUND_IMMUTABLE_FILE" for item in catalog) == 14
    assert sum(item["status"] == "SOURCE_FILE_ABSENT" for item in catalog) == 14
    assert all(item["year"] in range(2018, 2023) for item in catalog)
    assert all(
        item["schema"] == "trades"
        for item in catalog if item["status"] == "SOURCE_FILE_ABSENT"
    )


def test_single_identity_is_only_a_diagnostic_recovery_candidate() -> None:
    target = _synthetic_target()
    result = classify_target_observations(
        target=target,
        observations_by_schema={"ohlcv-1s": [_header(target)], "trades": []},
    )
    assert result["disposition"] == "DIAGNOSTIC_RECOVERY_CANDIDATE"
    assert result["families"]["ohlcv-1s"]["disposition"] == "OBSERVED_SINGLE_IDENTITY"
    assert result["prices_reported"] is False
    keys = _keys(result)
    assert not ({"open", "high", "low", "close", "price", "size", "volume"} & keys)


def test_multiple_identities_fail_closed_even_when_observations_exist() -> None:
    target = _synthetic_target()
    result = classify_target_observations(
        target=target,
        observations_by_schema={
            "ohlcv-1s": [
                _header(target, instrument=17),
                _header(target, offset=1_000_000_000, instrument=18),
            ],
            "trades": [],
        },
    )
    assert result["disposition"] == "DIAGNOSTIC_IDENTITY_AMBIGUOUS_FAIL_CLOSED"
    assert result["families"]["ohlcv-1s"]["instrument_identity_count"] == 2


def test_out_of_window_observations_cannot_recover_a_gap() -> None:
    target = _synthetic_target()
    outside = _header(
        target,
        offset=target.window_end_exclusive_ns - target.window_start_ns,
    )
    result = classify_target_observations(
        target=target,
        observations_by_schema={"ohlcv-1s": [outside], "trades": []},
    )
    assert result["disposition"] == "NOT_OBSERVED_IN_BOUND_DIAGNOSTIC_SOURCES"
    assert result["families"]["ohlcv-1s"]["observation_count"] == 0


def test_plan_is_hash_bound_and_authorizes_no_row_read_by_itself(
    local_evidence_root: Path,
) -> None:
    plan = load_diagnostic_recovery_plan(root=local_evidence_root)
    assert plan["plan_id"] == (
        "31dc42fa4e757183568d873e9678d0f7692cf3e6efa633170f1ca1095b4281a1"
    )
    assert plan["target_count"] == 34
    assert plan["diagnostic_semantics"]["prices_or_sizes_serialized"] is False
    assert plan["diagnostic_semantics"]["successor_source_creation_authorized"] is False
    assert set(plan["forbidden_actions"].values()) == {True}
