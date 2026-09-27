from __future__ import annotations

import ast
import inspect
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild import research_gateway_policy
from futures_rebuild import tbbo_quality_certification as quality


ROOT = Path(__file__).resolve().parents[1]


def test_frozen_plan_and_exact_ledgers() -> None:
    plan = quality.load_plan(root=ROOT)
    supplement = quality.build_supplement_ledger(plan)
    registry = json.loads((ROOT / quality.TBBO_REGISTRY).read_bytes())

    assert tuple(plan["markets"]) == quality.MARKETS
    assert tuple(plan["degraded_utc_dates"]) == quality.DEGRADED_DATES
    assert tuple(plan["boundary_sessions"]) == quality.BOUNDARY_SESSIONS
    assert plan["market_quarantined_utc_dates"] == {"SR3": ["2026-05-17"]}
    assert plan["status_utc_midnight_snapshot_policy"] == quality.STATUS_MIDNIGHT_SNAPSHOT_POLICY
    assert plan["status_only_session_policy"] == quality.STATUS_ONLY_SESSION_POLICY
    assert plan["identical_tbbo_row_policy"] == quality.IDENTICAL_TBBO_ROW_POLICY
    assert len(registry["entries"]) == 192
    assert {item["query"]["market"] for item in registry["entries"]} == set(quality.MARKETS)
    assert len(supplement) == 32
    assert len(quality.SUPPORT_ACTIVE_PATHS) == 64
    assert sum(
        str(path).startswith(str(quality.SUPPORT_ARCHIVE_ROOT))
        for path in quality.SUPPORT_ACTIVE_PATHS
    ) == 32
    assert [(item.market, item.schema) for item in supplement] == [
        (market, schema)
        for market in quality.MARKETS
        for schema in ("definition", "status")
    ]
    assert all(
        item.query_contract["start"] == quality.SUPPLEMENT_START
        and item.query_contract["end"] == quality.SUPPLEMENT_END
        for item in supplement
    )
    assert plan["supplement"] == {
        "dataset": "GLBX.MDP3",
        "start_inclusive": "2026-07-14T00:00:00Z",
        "end_exclusive": "2026-09-01T00:00:00Z",
        "request_count": 32,
        "maximum_billable_bytes": 650_000_000,
        "maximum_staged_bytes": 650_000_000,
        "required_free_disk_bytes": 1_000_000_000,
        "maximum_external_cost_usd": "0",
    }


def test_preparation_is_header_only_and_create_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    plan = quality.load_plan(root=ROOT)
    monkeypatch.setattr(quality, "load_plan", lambda **_: plan)
    monkeypatch.setattr(
        quality,
        "verify_tbbo_custody",
        lambda **_: {"snapshot_id": quality.TBBO_SNAPSHOT_ID, "dbn_rows_decoded": 0},
    )
    monkeypatch.setattr(quality, "_active_support_bindings", lambda **_: [])
    monkeypatch.setattr(
        quality,
        "protected_active_state",
        lambda _: {"state_id": "a" * 64, "dbn_rows_decoded": 0},
    )
    monkeypatch.setattr(quality, "sha256_file", lambda _: "h" * 64)
    monkeypatch.setattr(
        quality,
        "_default_chunk_loader",
        lambda _: (_ for _ in ()).throw(AssertionError("row decoder called")),
    )

    prepared = quality.prepare_quality_certification(root=tmp_path)

    assert prepared["state"] == "PREPARED_REQUIRES_DEFINITION_STATUS_SUPPLEMENT"
    assert prepared["provider_calls"] == 0
    assert prepared["dbn_rows_decoded"] == 0
    assert prepared["decoded_datasets_created"] == 0
    assert (tmp_path / prepared["preparation_path"]).is_file()
    assert "to_ndarray" not in inspect.getsource(quality.prepare_quality_certification)


def _provider_apis(
    *, cost: object = 0, size: object = 1, conditions: object = None,
    incomplete: bool = False,
) -> quality.SupplementProviderApis:
    def resolve(**kwargs: object) -> dict[str, object]:
        symbols = list(kwargs["symbols"])  # type: ignore[arg-type]
        result = {
            str(symbol): [{"d0": "2026-07-14", "d1": "2026-09-01", "s": "1"}]
            for symbol in symbols
        }
        if incomplete:
            result.pop(str(symbols[-1]))
        return {
            "stype_in": kwargs["stype_in"],
            "stype_out": "instrument_id",
            "start_date": "2026-07-14",
            "end_date": "2026-09-01",
            "partial": [],
            "not_found": [],
            "result": result,
        }

    return quality.SupplementProviderApis(
        get_dataset_condition=lambda **_: [] if conditions is None else conditions,
        get_dataset_range=lambda **_: {
            "start": "2010-01-01T00:00:00Z",
            "end": "2027-01-01T00:00:00Z",
        },
        resolve=resolve,
        get_cost=lambda **_: cost,
        get_billable_size=lambda **_: size,
        get_range=lambda **_: (_ for _ in ()).throw(AssertionError("download called")),
    )


def test_supplement_preflight_is_zero_cost_complete_and_bounded() -> None:
    plan = quality.load_plan(root=ROOT)
    result = quality.preflight_supplement(
        plan=plan,
        ledger=quality.build_supplement_ledger(plan),
        apis=_provider_apis(),
        clock=lambda: 0.0,
        sleeper=lambda _: None,
    )
    assert result["state"] == "PASS_ZERO_COST_COMPLETE_MAPPING"
    assert result["total_billable_bytes"] == 32
    assert result["external_cost_usd"] == "0"


def test_wrapped_databento_stream_failure_is_retried() -> None:
    BentoError = type("BentoError", (Exception,), {})
    attempts = 0
    delays: list[float] = []

    def stream(**_: object) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise BentoError("Error streaming response: Response ended prematurely")
        return "complete"

    assert quality._call_with_retry(
        stream,
        kwargs={},
        deadline=10.0,
        clock=lambda: 0.0,
        sleeper=delays.append,
    ) == "complete"
    assert attempts == 2
    assert delays == [1.0]


def test_stream_retry_uses_a_fresh_partial_and_preserves_the_failure(
    tmp_path: Path,
) -> None:
    BentoError = type("BentoError", (Exception,), {})
    paths: list[Path] = []

    def stream(**kwargs: object) -> object:
        path = Path(str(kwargs["path"]))
        path.write_bytes(b"partial" if not paths else b"complete")
        paths.append(path)
        if len(paths) == 1:
            raise BentoError("Error streaming response: Response ended prematurely")
        return object()

    store, completed = quality._download_supplement_request(
        method=stream,
        query={},
        final=tmp_path / "request.dbn.zst",
        attempt=tmp_path,
        deadline=10.0,
        clock=lambda: 0.0,
        sleeper=lambda _: None,
    )
    assert store is not None
    assert quality.io_path(completed) == paths[1]
    assert paths[0] != paths[1]
    assert not paths[0].exists()
    assert (tmp_path / "failed_partials" / paths[0].name).read_bytes() == b"partial"
    assert paths[1].read_bytes() == b"complete"


@pytest.mark.parametrize(
    ("apis", "message"),
    [
        (_provider_apis(cost=1), "cost"),
        (_provider_apis(size=25_000_000), "ceiling"),
        (_provider_apis(size=0), "positive billable"),
        (_provider_apis(incomplete=True), "scope differs"),
        (_provider_apis(conditions=[{"date": "2026-08-01", "status": "pending"}]), "missing or pending"),
    ],
)
def test_supplement_preflight_fails_closed(
    apis: quality.SupplementProviderApis, message: str,
) -> None:
    plan = quality.load_plan(root=ROOT)
    with pytest.raises(UnauthorizedOperation, match=message):
        quality.preflight_supplement(
            plan=plan,
            ledger=quality.build_supplement_ledger(plan),
            apis=apis,
            clock=lambda: 0.0,
            sleeper=lambda _: None,
        )


def test_provider_conditions_add_degradation_but_never_remove_known_dates() -> None:
    result = quality.normalize_provider_conditions([
        {"date": "2026-08-30", "status": "degraded"},
    ])
    assert set(quality.DEGRADED_DATES) < set(result["degraded_utc_dates"])
    assert "2026-08-30" in result["degraded_utc_dates"]


def test_header_mapping_may_extend_beyond_the_exact_query_window() -> None:
    metadata = SimpleNamespace(
        partial=[],
        not_found=[],
        mappings={
            "6E.v.0": [{
                "start_date": "2025-10-31",
                "end_date": "2025-12-01",
                "symbol": "4274",
            }],
        },
    )
    assert quality._mapping_intervals(
        metadata,
        symbol="6E.v.0",
        start_date="2025-11-01",
        end_date="2025-12-01",
    ) == (("2025-11-01", "2025-12-01", 4274),)


def _tbbo_rows(timestamps: list[int], *, instrument_id: int = 100) -> np.ndarray:
    rows = np.zeros(len(timestamps), dtype=quality.TBBO_NUMPY_DTYPE)
    rows["length"] = quality.TBBO_NUMPY_DTYPE.itemsize // 4
    rows["rtype"] = 1
    rows["publisher_id"] = 1
    rows["instrument_id"] = instrument_id
    rows["ts_event"] = timestamps
    rows["ts_recv"] = timestamps
    return rows


def _scan(
    chunks: list[np.ndarray], *, degraded: tuple[str, ...] = (),
    mapped_instrument: int = 100,
) -> quality._TbboScanState:
    state = quality._TbboScanState()
    quality._scan_tbbo_chunks(
        chunks,
        state=state,
        start_ns=quality._iso_ns("2025-09-01T00:00:00Z"),
        end_ns=quality._iso_ns("2025-10-01T00:00:00Z"),
        tbbo_intervals=(("2025-09-01", "2025-10-01", mapped_instrument),),
        status_intervals=(("2025-09-01", "2025-10-01", mapped_instrument),),
        degraded_dates=degraded,
        deadline=1.0,
        clock=lambda: 0.0,
    )
    return state


def test_exact_mbp1_dtype_order_and_interval_are_structural() -> None:
    first = quality._iso_ns("2025-09-02T12:00:00Z")
    wrong = np.zeros(1, dtype=[(name, "u8") for name in quality.TBBO_DTYPE])
    with pytest.raises(IntegrityError, match="dtype"):
        _scan([wrong])
    with pytest.raises(IntegrityError, match="timestamp"):
        _scan([_tbbo_rows([first + 1, first])], degraded=("2025-09-02",))
    with pytest.raises(IntegrityError, match="interval"):
        _scan([_tbbo_rows([quality._iso_ns("2025-10-01T00:00:00Z")])])


def test_cross_chunk_duplicates_mapping_flags_and_quarantine() -> None:
    eligible_ts = quality._iso_ns("2025-09-02T12:00:00Z")
    eligible = _tbbo_rows([eligible_ts])
    state = _scan([eligible, eligible.copy()])
    assert state.duplicate_rows == 1
    assert state.duplicate_dates == {"2025-09-02": 1}

    degraded_ts = quality._iso_ns("2025-09-17T12:00:00Z")
    quarantined = _tbbo_rows([degraded_ts, degraded_ts], instrument_id=999)
    quarantined["flags"] = quality.F_BAD_TS_RECV | quality.F_MAYBE_BAD_BOOK
    state = _scan([quarantined], degraded=("2025-09-17",))
    assert state.eligible_rows == 0
    assert state.quarantined_rows == 2
    assert state.quarantined_duplicate_rows == 1
    assert state.quarantined_mapping_mismatches == 2
    assert state.quarantined_bad_flag_rows == 2


def test_status_sessions_before_tbbo_period_are_not_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dtype = np.dtype([
        (name, "S1" if name in {"action", "reason", "trading_event", "is_trading", "is_quoting", "is_short_sell_restricted", "_reserved"} else "u8")
        for name in quality.STATUS_DTYPE
    ])
    rows = np.zeros(3, dtype=dtype)
    january = quality._iso_ns("2025-01-02T12:00:00Z")
    midnight_snapshot = quality._iso_ns("2025-09-01T00:00:00Z") + 1
    september = quality._iso_ns("2025-09-02T12:00:00Z")
    rows["instrument_id"] = 100
    rows["ts_event"] = [january, january, september]
    rows["ts_recv"] = [january, midnight_snapshot, september]
    rows["is_trading"] = b"Y"
    monkeypatch.setattr(quality, "_bound_file", lambda *_, **__: iter((rows,)))
    monkeypatch.setattr(
        quality,
        "_continuous_intervals",
        lambda **_: (("2025-01-01", "2026-09-01", 100),),
    )
    sessions, first_use, _ = quality._scan_status(
        [(Path("status.dbn.zst"), "h" * 64)],
        market="ES",
        degraded_dates=(),
        chunk_loader=lambda _: (),
        header_loader=lambda _: object(),
        deadline=1.0,
        clock=lambda: 0.0,
    )
    assert sessions == {quality._trade_session(september)}
    assert first_use == {100: (september, september)}


def test_identical_rows_are_retained_nonblocking_and_sr3_date_is_quarantined() -> None:
    use = {100: (20, 20)}
    state = quality._TbboScanState(
        sessions={"2025-09-02"}, first_use=use, duplicate_rows=1,
    )
    disposition = quality._market_quality_disposition(
        monthly=[{}] * 12,
        state=state,
        expected_sessions={"2025-09-02"},
        definitions={100: [(10, 0, quality.UINT64_NULL)]},
    )
    assert disposition["quality_passed"] is True
    assert quality._market_quarantined_dates("ES", ()) == ()
    assert quality._market_quarantined_dates("SR3", ()) == ("2026-05-17",)
    assert {
        quality._trade_session(quality._iso_ns("2026-05-17T00:00:00Z")),
        quality._trade_session(quality._iso_ns("2026-05-17T23:59:59Z")),
    } <= quality._excluded_sessions(quality._market_quarantined_dates("SR3", ()))


def test_monthly_continuity_definitions_sessions_and_boundaries() -> None:
    months = [
        SimpleNamespace(start_date=f"2025-{month:02d}-01", end_date=f"2025-{month + 1:02d}-01")
        for month in range(9, 12)
    ]
    months.extend([
        SimpleNamespace(start_date=f"2026-{month:02d}-01", end_date=f"2026-{month + 1:02d}-01")
        for month in range(1, 8)
    ])
    months.extend([
        SimpleNamespace(start_date="2026-08-01", end_date="2026-09-01"),
        SimpleNamespace(start_date="2026-07-01", end_date="2026-08-01"),
    ])
    with pytest.raises(IntegrityError, match="gap or overlap"):
        quality._require_monthly_sequence(months)

    use = {100: (20, 20)}
    assert quality._definition_covers({100: [(10, 0, quality.UINT64_NULL)]}, use)
    assert not quality._definition_covers({100: [(21, 0, quality.UINT64_NULL)]}, use)
    state = quality._TbboScanState(sessions={"2025-09-02"}, first_use=use)
    disposition = quality._market_quality_disposition(
        monthly=[{}] * 12,
        state=state,
        expected_sessions={"2025-09-02", "2025-09-03"},
        definitions={100: [(10, 0, quality.UINT64_NULL)]},
    )
    assert disposition == {
        "quality_passed": True,
        "missing_empirical_sessions": ["2025-09-03"],
        "missing_empirical_sessions_blocking": False,
        "status_only_session_policy": quality.STATUS_ONLY_SESSION_POLICY,
        "definition_coverage_passed": True,
    }
    excluded = quality._excluded_sessions(quality.DEGRADED_DATES)
    assert set(quality.BOUNDARY_SESSIONS) <= excluded
    assert quality._trade_session(quality._iso_ns("2025-09-17T12:00:00Z")) in excluded


def test_separate_operations_are_registered_and_scopes_are_distinct() -> None:
    plan = quality.load_plan(root=ROOT)
    supplement = quality.required_supplement_scope(root=ROOT, plan=plan)
    preparation = {
        "state": "PREPARED_REQUIRES_SEPARATE_PROTECTED_ROW_SCAN_APPROVAL",
        "preparation_id": "p" * 64,
        "preparation_path": quality.PLAN_PATH.as_posix(),
        "supplement": {"state": "SEALED_HEADER_VERIFIED", "snapshot_id": "s" * 64},
    }
    scan = quality.required_quality_scope(root=ROOT, plan=plan, preparation=preparation)
    assert supplement["approval_command"] == quality.SUPPLEMENT_OPERATION
    assert scan["approval_command"] == quality.CERTIFY_OPERATION
    assert supplement["row_decode"] == "false"
    assert scan["provider_calls"] == "0"
    assert {
        quality.SUPPLEMENT_OPERATION,
        quality.CERTIFY_OPERATION,
    } <= research_gateway_policy.PREPARATORY_REAL_HISTORY_OPERATIONS


def test_quality_scan_consumes_one_exact_approval_and_reports_aggregates_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    preparation_id = "p" * 64
    supplement_id = "s" * 64
    preparation_path = tmp_path / quality.PREPARATION_ROOT / preparation_id / "preparation.json"
    preparation_path.parent.mkdir(parents=True)
    preparation_path.write_text("{}", encoding="utf-8")
    preparation = {
        "state": "PREPARED_REQUIRES_SEPARATE_PROTECTED_ROW_SCAN_APPROVAL",
        "preparation_id": preparation_id,
        "preparation_path": preparation_path.relative_to(tmp_path).as_posix(),
        "supplement": {
            "state": "SEALED_HEADER_VERIFIED",
            "snapshot_id": supplement_id,
            "provider_conditions": {"degraded_utc_dates": list(quality.DEGRADED_DATES)},
        },
        "protected_active_baseline": {"state_id": "b" * 64},
    }
    plan = {"plan_id": "x" * 64, "implementation_sha256": "i" * 64}
    expected_scope = {"bound": "exact"}
    monkeypatch.setattr(quality, "load_plan", lambda **_: plan)
    monkeypatch.setattr(quality, "load_preparation", lambda **_: preparation)
    monkeypatch.setattr(
        quality,
        "prepare_quality_certification",
        lambda **_: {"preparation_id": preparation_id},
    )
    monkeypatch.setattr(quality, "required_quality_scope", lambda **_: expected_scope)
    monkeypatch.setattr(
        quality, "protected_active_state", lambda _: preparation["protected_active_baseline"],
    )
    monkeypatch.setattr(quality, "sha256_file", lambda _: "f" * 64)

    def result(*, market: str, **_: object) -> dict[str, object]:
        return {
            "market": market,
            "monthly": [{"interval": "bound", "row_count": 1}] * 12,
            "total_rows": 1,
            "eligible_rows": 1,
            "quarantined_rows": 0,
            "eligible_identical_rows_retained": 0,
            "eligible_bad_flag_rows": 0,
            "eligible_mapping_mismatches": 0,
            "quarantined_identical_rows_retained": 0,
            "quarantined_bad_flag_rows": 0,
            "quarantined_mapping_mismatches": 0,
            "empirical_expected_session_count": 1,
            "empirical_observed_session_count": 1,
            "missing_empirical_sessions": [],
            "defined_eligible_instrument_count": 1,
            "quality_passed": True,
            "definition_coverage_passed": True,
        }

    monkeypatch.setattr(quality, "_scan_market", result)

    class Authorization:
        receipt_id = "receipt"
        calls = 0
        kwargs: dict[str, object] = {}

        def consume(self, _boundary: object, **kwargs: object) -> None:
            self.calls += 1
            self.kwargs = kwargs
            if self.calls > 1:
                raise UnauthorizedOperation("receipt already consumed")

    authorization = Authorization()
    completed = quality.execute_authorized_quality_scan(
        root=tmp_path,
        preparation_id=preparation_id,
        authorization=authorization,  # type: ignore[arg-type]
        environment_check=lambda _: None,
        clock=lambda: 0.0,
    )
    assert authorization.calls == 1
    assert authorization.kwargs["required_scope"] == expected_scope
    certificate = json.loads((tmp_path / completed["certificate_path"]).read_bytes())
    encoded = json.dumps(certificate, sort_keys=True).casefold()
    assert not any(token in encoded for token in ('"price"', '"size"', '"side"', '"return"', '"prediction"'))
    with pytest.raises(UnauthorizedOperation, match="already consumed"):
        quality.execute_authorized_quality_scan(
            root=tmp_path,
            preparation_id=preparation_id,
            authorization=authorization,  # type: ignore[arg-type]
            environment_check=lambda _: None,
            clock=lambda: 0.0,
        )


def test_quality_failure_preserves_aggregate_market_dispositions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    preparation_id = "p" * 64
    preparation_path = tmp_path / quality.PREPARATION_ROOT / preparation_id / "preparation.json"
    preparation_path.parent.mkdir(parents=True)
    preparation_path.write_text("{}", encoding="utf-8")
    preparation = {
        "state": "PREPARED_REQUIRES_SEPARATE_PROTECTED_ROW_SCAN_APPROVAL",
        "preparation_id": preparation_id,
        "preparation_path": preparation_path.relative_to(tmp_path).as_posix(),
        "supplement": {
            "state": "SEALED_HEADER_VERIFIED",
            "snapshot_id": "s" * 64,
            "provider_conditions": {"degraded_utc_dates": list(quality.DEGRADED_DATES)},
        },
        "protected_active_baseline": {"state_id": "b" * 64},
    }
    monkeypatch.setattr(
        quality,
        "load_plan",
        lambda **_: {"plan_id": "x" * 64, "implementation_sha256": "i" * 64},
    )
    monkeypatch.setattr(quality, "load_preparation", lambda **_: preparation)
    monkeypatch.setattr(
        quality, "prepare_quality_certification", lambda **_: {"preparation_id": preparation_id},
    )
    monkeypatch.setattr(quality, "required_quality_scope", lambda **_: {"bound": "exact"})
    monkeypatch.setattr(
        quality, "protected_active_state", lambda _: preparation["protected_active_baseline"],
    )

    def result(*, market: str, **_: object) -> dict[str, object]:
        failed = market == quality.MARKETS[0]
        return {
            "market": market,
            "monthly": [{"interval": "bound", "row_count": 1}] * 12,
            "total_rows": 1,
            "eligible_rows": 1,
            "quarantined_rows": 0,
            "eligible_identical_rows_retained": 0,
            "eligible_bad_flag_rows": 0,
            "eligible_mapping_mismatches": int(failed),
            "quarantined_identical_rows_retained": 0,
            "quarantined_bad_flag_rows": 0,
            "quarantined_mapping_mismatches": 0,
            "empirical_expected_session_count": 1,
            "empirical_observed_session_count": 1,
            "missing_empirical_sessions": [],
            "defined_eligible_instrument_count": 1,
            "quality_passed": not failed,
            "definition_coverage_passed": True,
        }

    monkeypatch.setattr(quality, "_scan_market", result)

    class Authorization:
        receipt_id = "receipt"

        def consume(self, *_: object, **__: object) -> None:
            pass

    with pytest.raises(IntegrityError, match="quality gate"):
        quality.execute_authorized_quality_scan(
            root=tmp_path,
            preparation_id=preparation_id,
            authorization=Authorization(),  # type: ignore[arg-type]
            environment_check=lambda _: None,
            clock=lambda: 0.0,
        )
    failures = list((preparation_path.parent / "attempts" / "receipt").glob("failure_*.json"))
    assert len(failures) == 1
    failure = json.loads(failures[0].read_bytes())
    assert failure["disposition"] == "ELIGIBLE_ROW_QUALITY_GATE_FAILED"
    assert len(failure["market_results"]) == len(quality.MARKETS)
    assert failure["market_results"][0]["eligible_mapping_mismatches"] == 1
    encoded = json.dumps(failure, sort_keys=True).casefold()
    assert not any(token in encoded for token in ('"price"', '"size"', '"side"', '"return"', '"prediction"'))


def test_scan_report_literal_has_no_raw_market_fields() -> None:
    tree = ast.parse(inspect.getsource(quality._scan_market))
    keys = {
        node.value
        for item in ast.walk(tree)
        if isinstance(item, ast.Return) and isinstance(item.value, ast.Dict)
        for node in item.value.keys
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    assert {"price", "size", "side", "feature", "return", "prediction"}.isdisjoint(keys)
