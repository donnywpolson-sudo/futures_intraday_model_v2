from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from types import MappingProxyType

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.data_layout import PhasePublisher as AtomicPublisher
from futures_rebuild.errors import ContractError, IntegrityError
from futures_rebuild.foundation.coverage import StatusResearchScopePolicy
from futures_rebuild.foundation.market_state import (
    AsOfStatisticsLedger,
    AsOfStatusLedger,
    FoundationCoveragePolicy,
    StatisticsRolePolicy,
)
from futures_rebuild.foundation.records import (
    INT64_NULL,
    StatisticsRecordV1,
    StatusRecordV1,
)
from futures_rebuild.foundation.selection import (
    load_source_selection,
    publish_catalog_selection,
    resolve_foundation_selection,
)
from futures_rebuild.foundation.snapshot import PublishedSourceSnapshot, SnapshotFile
from futures_rebuild.source_symbology import build_query_contract


REPO = Path(__file__).resolve().parents[1]
DAY_NS = 86_400_000_000_000
START_NS = 1_704_067_200_000_000_000
START_DATE = "2024-01-01"


def _status(
    ordinal: int,
    *,
    ts_event_ns: int = START_NS,
    ts_recv_ns: int | None = None,
    action: str = "TRADING",
    is_trading: str = "YES",
    is_quoting: str = "YES",
    short_state: str = "NO",
) -> StatusRecordV1:
    received = ts_event_ns + 1 if ts_recv_ns is None else ts_recv_ns
    row_hash = sha256_json({"kind": "status", "ordinal": ordinal})
    return StatusRecordV1(
        dataset="GLBX.MDP3",
        market="ES",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=datetime.fromtimestamp(
            received // 1_000_000_000, tz=timezone.utc
        ).date().isoformat(),
        ts_event_ns=ts_event_ns,
        ts_recv_ns=received,
        action=action,
        reason="NONE",
        trading_event="NONE",
        is_trading=is_trading,
        is_quoting=is_quoting,
        is_short_sell_restricted=short_state,
        source_release_id="a" * 64,
        source_manifest_sha256="b" * 64,
        source_file_path="dbn/status/ES/2024/status.dbn.zst",
        source_file_sha256="c" * 64,
        row_ordinal=ordinal,
        row_sha256=row_hash,
    )


def _stat(
    ordinal: int,
    *,
    update_action: str,
    ts_event_ns: int,
    ts_recv_ns: int,
    price_nano: int = 5_000_000_000_000,
    quantity: int = 100,
) -> StatisticsRecordV1:
    received_date = datetime.fromtimestamp(
        ts_recv_ns // 1_000_000_000, tz=timezone.utc
    ).date().isoformat()
    return StatisticsRecordV1(
        dataset="GLBX.MDP3",
        market="ES",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=received_date,
        ts_event_ns=ts_event_ns,
        ts_recv_ns=ts_recv_ns,
        ts_ref_ns=START_NS,
        ts_in_delta=1,
        stat_type="OPEN_INTEREST",
        update_action=update_action,
        price_nano=price_nano,
        quantity=quantity,
        sequence=ordinal,
        channel_id=1,
        flags=0,
        source_release_id="a" * 64,
        source_manifest_sha256="b" * 64,
        source_file_path="dbn/statistics/ES/2024/statistics.dbn.zst",
        source_file_sha256="d" * 64,
        row_ordinal=ordinal,
        row_sha256=sha256_json({"kind": "statistics", "ordinal": ordinal}),
    )


def test_status_asof_is_bitemporal_and_never_backward_fills_poison() -> None:
    eligible = _status(0)
    future_halt = _status(
        1,
        ts_event_ns=START_NS + 10,
        ts_recv_ns=START_NS + 1_000,
        action="HALT",
        is_trading="NO",
        is_quoting="NO",
    )
    ledger = AsOfStatusLedger((future_halt, eligible))
    before_future_receive = ledger.as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        decision_at_ns=START_NS + 100,
    )
    assert before_future_receive.status_disposition == "STATUS_ELIGIBLE"
    assert before_future_receive.foundation_eligible is True
    after_future_receive = ledger.as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        decision_at_ns=START_NS + 1_000,
    )
    assert after_future_receive.status_disposition == "STATUS_HALTED"
    assert after_future_receive.foundation_eligible is False
    missing = ledger.as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=999,
        instrument_id_date_utc=START_DATE,
        decision_at_ns=START_NS + 1_000,
    )
    assert missing.status_disposition == "STATUS_UNRESOLVED"
    assert missing.in_coverage_denominator is True
    reused_next_date = ledger.as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc="2024-01-02",
        decision_at_ns=START_NS + DAY_NS + 1,
    )
    assert reused_next_date.status_disposition == "STATUS_UNRESOLVED"


def test_status_visibility_uses_receive_time_and_preserves_cross_clock_skew() -> None:
    future_exchange_clock = _status(
        0,
        ts_event_ns=START_NS + 10_000,
        ts_recv_ns=START_NS + 10,
        action="HALT",
        is_trading="NO",
        is_quoting="NO",
    )
    decision = AsOfStatusLedger((future_exchange_clock,)).as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        decision_at_ns=START_NS + 20,
    )
    assert decision.status_disposition == "STATUS_HALTED"
    assert decision.status_ts_event_ns > START_NS + 20
    assert decision.status_ts_recv_ns <= START_NS + 20

    crossing = _status(
        1,
        ts_event_ns=START_NS - 1,
        ts_recv_ns=START_NS + 1,
    )
    crossing_decision = AsOfStatusLedger((crossing,)).as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        decision_at_ns=START_NS + 2,
    )
    assert crossing_decision.status_disposition == "STATUS_ELIGIBLE"


def test_status_equal_receive_order_is_explicit_and_cross_file_conflicts_fail() -> None:
    receive_ns = START_NS + 5
    first = _status(0, ts_recv_ns=receive_ns)
    terminal = _status(
        1,
        ts_recv_ns=receive_ns,
        action="HALT",
        is_trading="NO",
        is_quoting="NO",
    )
    same_file = AsOfStatusLedger((terminal, first)).as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        decision_at_ns=receive_ns,
    )
    assert same_file.status_disposition == "STATUS_HALTED"

    conflict = replace(
        terminal,
        source_file_path="dbn/status/ES/2024/conflict.dbn.zst",
        source_file_sha256="f" * 64,
        row_sha256="1" * 64,
    )
    with pytest.raises(ContractError, match="equal-receive cross-file"):
        AsOfStatusLedger((first, conflict))

    duplicate = replace(
        first,
        source_file_path="dbn/status/ES/2024/duplicate.dbn.zst",
        source_file_sha256="e" * 64,
        row_sha256="2" * 64,
    )
    identical = AsOfStatusLedger((first, duplicate)).as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        decision_at_ns=receive_ns,
    )
    assert identical.status_disposition == "STATUS_ELIGIBLE"


@pytest.mark.parametrize(
    ("action", "trading", "quoting", "expected"),
    [
        ("SUSPEND", "NO", "NO", "STATUS_SUSPENDED"),
        ("PAUSE", "NO", "YES", "STATUS_PAUSED"),
        ("UNKNOWN_255", "YES", "YES", "STATUS_UNKNOWN"),
        ("TRADING", "NOT_AVAILABLE", "YES", "STATUS_UNKNOWN"),
    ],
)
def test_status_halt_suspend_unknown_are_explicitly_ineligible(
    action: str, trading: str, quoting: str, expected: str
) -> None:
    decision = AsOfStatusLedger(
        (_status(0, action=action, is_trading=trading, is_quoting=quoting),)
    ).as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        decision_at_ns=START_NS + 100,
    )
    assert decision.status_disposition == expected
    assert decision.foundation_eligible is False
    assert decision.in_coverage_denominator is True


def test_futures_short_restriction_not_available_blocks_short_only() -> None:
    decision = AsOfStatusLedger((_status(0, short_state="NOT_AVAILABLE"),)).as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        decision_at_ns=START_NS + 100,
    )
    assert decision.foundation_eligible is True
    assert decision.long_eligible is True
    assert decision.short_eligible is False


def test_statistics_new_delete_and_unknown_never_become_features() -> None:
    roles = StatisticsRolePolicy.from_file(
        REPO / "configs" / "statistics_foundation_roles.json"
    )
    new = _stat(
        0,
        update_action="NEW",
        ts_event_ns=START_NS,
        ts_recv_ns=START_NS + 1,
    )
    delete = _stat(
        1,
        update_action="DELETE",
        ts_event_ns=START_NS + 10,
        ts_recv_ns=START_NS + 100,
    )
    ledger = AsOfStatisticsLedger((delete, new), roles=roles)
    before_delete = ledger.as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        stat_type="OPEN_INTEREST",
        ts_ref_ns=START_NS,
        decision_at_ns=START_NS + 50,
    )
    assert before_delete.state == "NEW_KNOWN"
    assert before_delete.feature_eligible is False
    after_delete = ledger.as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        stat_type="OPEN_INTEREST",
        ts_ref_ns=START_NS,
        decision_at_ns=START_NS + 100,
    )
    assert after_delete.state == "DELETED"
    assert after_delete.price_nano is None
    unknown = AsOfStatisticsLedger(
        (
            _stat(
                2,
                update_action="NEW",
                ts_event_ns=START_NS,
                ts_recv_ns=START_NS + 1,
                price_nano=INT64_NULL,
            ),
        ),
        roles=roles,
    ).as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        stat_type="OPEN_INTEREST",
        ts_ref_ns=START_NS,
        decision_at_ns=START_NS + 2,
    )
    assert unknown.state == "NEW_UNKNOWN_VALUE"
    assert unknown.feature_eligible is False


def test_statistics_visibility_uses_receive_time_not_provider_event_clock() -> None:
    roles = StatisticsRolePolicy.from_file(
        REPO / "configs" / "statistics_foundation_roles.json"
    )
    record = _stat(
        0,
        update_action="NEW",
        ts_event_ns=START_NS + 10_000,
        ts_recv_ns=START_NS + 10,
    )
    state = AsOfStatisticsLedger((record,), roles=roles).as_of(
        dataset="GLBX.MDP3",
        publisher_id=1,
        instrument_id=123,
        instrument_id_date_utc=START_DATE,
        stat_type="OPEN_INTEREST",
        ts_ref_ns=START_NS,
        decision_at_ns=START_NS + 20,
    )
    assert state.state == "NEW_KNOWN"


def _snapshot_file(root: Path, relative: str, content: bytes) -> SnapshotFile:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return SnapshotFile(
        logical_path=f"data/{relative}",
        physical_path=path,
        relative_path=relative,
        size=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        source_release_id="a" * 64,
        source_manifest_sha256="b" * 64,
        files_index_sha256="c" * 64,
    )


def _selection_fixture(
    tmp_path: Path,
    *,
    overlap: bool = False,
    known_anomalies_sha256: str = "d" * 64,
):
    root = tmp_path / "snapshot"
    entries = []
    files = {}

    def add(schema: str, market: str, year: int, start: str, end: str) -> None:
        directory = schema.replace("-", "_")
        relative = f"dbn/{directory}/{market}/{year}/{start}_{end}.dbn.zst"
        binding = _snapshot_file(root, relative, relative.encode("ascii"))
        files[relative] = binding
        sidecar_relative = f"{relative}.manifest.json"
        sidecar = _snapshot_file(root, sidecar_relative, b"{}\n")
        files[sidecar_relative] = sidecar
        family = {
            "definition": "dbn_definition",
            "ohlcv-1m": "dbn_ohlcv_1m",
            "statistics": "dbn_statistics",
            "status": "dbn_status",
        }[schema]
        stype_in = "parent" if schema == "definition" else "continuous"
        symbols = [f"{market}.FUT" if schema == "definition" else f"{market}.v.0"]
        query = build_query_contract(
            schema=schema,
            market=market,
            start=start,
            end=end,
            stype_in=stype_in,
            symbols=symbols,
        )
        entry = {
                "coverage_disposition": "AUTHORITATIVE_INTERVAL",
                "end": end,
                "family": family,
                "market": market,
                "path": f"data/{relative}",
                "query_contract": query,
                "query_contract_id": query["query_contract_id"],
                "query_mode_id": query["query_mode_id"],
                "query_stype_in": stype_in,
                "query_symbols": symbols,
                "schema": schema,
                "sha256": binding.sha256,
                "sidecar_path": f"data/{sidecar_relative}",
                "sidecar_sha256": sidecar.sha256,
                "sidecar_size": sidecar.size,
                "size": binding.size,
                "start": start,
                "year": year,
            }
        entries.append({**entry, "validation_sha256": sha256_json(entry)})

    add("definition", "ES", 2024, "2024-01-01", "2025-01-01")
    add("ohlcv-1m", "ES", 2024, "2024-01-01", "2025-01-01")
    add("statistics", "ES", 2024, "2024-01-01", "2025-01-01")
    add("status", "NQ", 2023, "2023-01-01", "2024-01-01")
    if overlap:
        add("statistics", "ES", 2024, "2024-06-01", "2025-01-01")
    snapshot = PublishedSourceSnapshot(
        manifest_path=root / "manifest.json",
        receipt=SimpleNamespace(release_id="a" * 64, manifest_sha256="b" * 64),
        files=MappingProxyType(files),
        files_index_sha256="c" * 64,
    )
    core = {
        "catalog_contract_version": "2.0.0",
        "dataset": "GLBX.MDP3",
        "families": [
            {"family": name}
            for name in (
                "dbn_definition",
                "dbn_ohlcv_1m",
                "dbn_statistics",
                "dbn_status",
            )
        ],
        "files": entries,
        "selection_policy": "EXACT_CONTRACT_ALL_FILES_NO_RECURSIVE_NEWEST",
        "selection_scope": "FILTERED",
        "source_scope": "VERIFIED_LAYOUT_V2_DBN_RELEASE",
        "known_anomalies_sha256": known_anomalies_sha256,
        "source_dbn_manifest_sha256": "b" * 64,
        "source_dbn_release_id": "a" * 64,
    }
    return {**core, "selection_manifest_id": sha256_json(core)}, snapshot


def test_coverage_matrix_preserves_missing_status_and_extra_family(tmp_path) -> None:
    selection, snapshot = _selection_fixture(tmp_path)
    resolved = resolve_foundation_selection(selection, snapshot=snapshot)
    rows = {(row["market"], row["year"]): row for row in resolved.coverage_matrix}
    assert rows[("ES", 2024)]["status_disposition"] == "STATUS_UNRESOLVED"
    assert rows[("ES", 2024)]["required_for_bar_foundation"] is True
    assert rows[("NQ", 2023)]["required_for_bar_foundation"] is False
    assert rows[("NQ", 2023)]["status_file_count"] == 1
    assert resolved.coverage_matrix_id == sha256_json(list(resolved.coverage_matrix))


def test_rehashed_selection_cannot_bless_stale_file_validation_hash(tmp_path) -> None:
    selection, snapshot = _selection_fixture(tmp_path)
    selection["files"][0]["query_symbols"] = ["NQ.FUT"]
    core = {
        key: value for key, value in selection.items() if key != "selection_manifest_id"
    }
    selection["selection_manifest_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="file validation hash"):
        resolve_foundation_selection(selection, snapshot=snapshot)


def test_unresolved_authoritative_overlap_fails_closed(tmp_path) -> None:
    selection, snapshot = _selection_fixture(tmp_path, overlap=True)
    with pytest.raises(IntegrityError, match="overlapping statistics"):
        resolve_foundation_selection(selection, snapshot=snapshot)


def test_exact_quarantined_redundant_overlap_is_not_treated_as_authoritative(
    tmp_path,
) -> None:
    selection, snapshot = _selection_fixture(tmp_path, overlap=True)
    statistics = [
        item for item in selection["files"] if item["schema"] == "statistics"
    ]
    statistics[-1]["coverage_disposition"] = (
        "QUARANTINED_REDUNDANT_EXACT_CROSSCHECK_ONLY"
    )
    statistics[-1]["validation_sha256"] = sha256_json(
        {key: value for key, value in statistics[-1].items() if key != "validation_sha256"}
    )
    core = {
        key: value for key, value in selection.items() if key != "selection_manifest_id"
    }
    selection["selection_manifest_id"] = sha256_json(core)
    resolved = resolve_foundation_selection(selection, snapshot=snapshot)
    assert len(resolved.statistics_files) == 1


def test_authoritative_overlap_with_redundant_crosscheck_remains_present(
    tmp_path,
) -> None:
    selection, snapshot = _selection_fixture(tmp_path, overlap=True)
    statistics = [
        item for item in selection["files"] if item["schema"] == "statistics"
    ]
    statistics[0]["coverage_disposition"] = (
        "AUTHORITATIVE_INTERVAL_WITH_EXACT_REDUNDANT_CROSSCHECK"
    )
    statistics[1]["coverage_disposition"] = "REDUNDANT_EXACT_CROSSCHECK_ONLY"
    for item in statistics:
        item["validation_sha256"] = sha256_json(
            {key: value for key, value in item.items() if key != "validation_sha256"}
        )
    core = {
        key: value for key, value in selection.items() if key != "selection_manifest_id"
    }
    selection["selection_manifest_id"] = sha256_json(core)
    resolved = resolve_foundation_selection(selection, snapshot=snapshot)
    assert len(resolved.statistics_files) == 1
    assert resolved.statistics_files[0].coverage_disposition == (
        "AUTHORITATIVE_INTERVAL_WITH_EXACT_REDUNDANT_CROSSCHECK"
    )
    row = next(
        item
        for item in resolved.coverage_matrix
        if item["market"] == "ES" and item["year"] == 2024
    )
    assert row["statistics_file_count"] == 1
    assert row["redundant_crosscheck_file_count"] == 1


def test_tracked_coverage_policy_has_nonzero_and_source_gates() -> None:
    policy = FoundationCoveragePolicy.from_file(
        REPO / "configs" / "foundation_coverage_policy.json"
    )
    assert policy.minimum_bar_rows >= 1_000_000
    assert policy.minimum_status_gated_feature_ready_fraction >= Decimal("0.95")
    assert policy.minimum_status_gated_feature_ready_rows >= 100_000
    assert policy.minimum_status_eligible_rows >= 100_000
    assert policy.minimum_status_resolved_decision_fraction >= Decimal("0.95")
    assert policy.minimum_status_source_market_year_fraction >= Decimal("0.99")
    assert policy.minimum_statistics_source_market_year_fraction == Decimal("1")


def test_tracked_status_research_scope_is_source_defined_and_conservative() -> None:
    policy = StatusResearchScopePolicy.from_file(
        REPO / "configs" / "status_research_scope_policy.json"
    )
    assert policy.provider_status_launch_month == "2024-07"
    assert policy.research_interval_start == "2025-01-01"
    assert (
        policy.disposition(
            start="2024-01-01",
            end="2025-01-01",
            coverage_passed=True,
        )
        == "ABSTAIN_PRE_STATUS_CAPABILITY_EPOCH"
    )
    assert (
        policy.disposition(
            start="2025-01-01",
            end="2026-01-01",
            coverage_passed=True,
        )
        == "ELIGIBLE"
    )
    assert (
        policy.disposition(
            start="2025-01-01",
            end="2026-01-01",
            coverage_passed=False,
        )
        == "FAIL_STATUS_COVERAGE"
    )
    mutated = policy.as_dict()
    mutated["research_interval_start"] = "2024-07-01"
    with pytest.raises(ContractError):
        StatusResearchScopePolicy.from_dict(mutated)


def test_verified_catalog_has_one_immutable_publication_phase(
    boundary, operation_factory
) -> None:
    known_anomalies = boundary.active_root / "configs" / "known_anomalies.json"
    known_anomalies.write_bytes(
        (REPO / "configs" / "known_anomalies.json").read_bytes()
    )
    selection, synthetic = _selection_fixture(
        boundary.active_root / "fixture",
        known_anomalies_sha256=sha256_file(known_anomalies),
    )
    snapshot = synthetic
    catalog = boundary.active_root / "state" / "source_selection" / "catalog.json"
    catalog.parent.mkdir(parents=True, exist_ok=True)
    catalog.write_bytes(canonical_bytes(selection) + b"\n")
    publisher = AtomicPublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "selection.lock",
    )
    receipt = publish_catalog_selection(
        catalog,
        snapshot=snapshot,
        boundary=boundary,
        publisher=publisher,
    )
    assert load_source_selection(
        receipt, snapshot=snapshot, boundary=boundary
    ) == selection
    catalog.write_bytes(catalog.read_bytes() + b"poison")
    with pytest.raises(IntegrityError, match="catalog JSON is invalid"):
        publish_catalog_selection(
            catalog,
            snapshot=snapshot,
            boundary=boundary,
            publisher=publisher,
        )
