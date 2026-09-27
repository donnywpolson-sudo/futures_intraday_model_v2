from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild import l1_execution_bbo_1s_market_partition as bbo
from futures_rebuild import l1_execution_quarantine_acquisition as predecessor
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.research_gateway_policy import require_current_real_history_operation


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.current
PREDECESSOR_MANIFEST = Path(
    "state/provider_acquisition_staging/l1_execution_quarantine_v1/"
    "2a5621a9a895512cbd499f7a034217f255e3d509bbb0f86efff977e86b675730/"
    "manifest.json"
)
PREDECESSOR_RECORD = Path(
    "state/provider_acquisitions/l1_execution_quarantine_v1/"
    "15dc5ccf4fdfcf4f48d586086f8fad2c2dee36f40f53e7763e47102b042edd83.json"
)


def _metadata(request: bbo.MarketRequest) -> SimpleNamespace:
    return bbo.synthetic_metadata(request)


def _provider(
    ledger: tuple[bbo.MarketRequest, ...],
    *,
    get_range,
    cost: str = "0",
    size=None,
) -> predecessor.ProviderApis:
    plan = bbo.load_plan(root=ROOT)
    period = plan["period"]
    by_query = {
        (request.start, tuple(request.symbols)): request for request in ledger
    }
    size = size or (
        lambda **kwargs: (
            0
            if by_query[(kwargs["start"], tuple(kwargs["symbols"]))].trade_date
            in bbo.PREDECESSOR_EMPTY_DATES
            else 400_000
        )
    )
    return predecessor.ProviderApis(
        get_dataset_range=lambda **_kwargs: {
            "schema": {
                bbo.SCHEMA: {
                    "start": "2025-09-01T00:00:00Z",
                    "end": "2026-09-01T00:00:00Z",
                }
            }
        },
        resolve=lambda **_kwargs: {
            "result": {
                symbol: [{"d0": "2025-09-01", "d1": "2026-09-01", "s": 1}]
                for symbol in bbo.SYMBOLS
            },
            "stype_in": bbo.STYPE_IN,
            "stype_out": bbo.STYPE_OUT,
            "start_date": period["start_date_inclusive"],
            "end_date": "2026-09-01",
            "partial": [],
            "not_found": [],
        },
        get_cost=lambda **_kwargs: cost,
        get_billable_size=size,
        get_range=get_range,
    )


def _copy_input(root: Path, relative: Path) -> None:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes((ROOT / relative).read_bytes())


def _receipt_path(root: Path) -> tuple[dict[str, object], Path]:
    for relative in (
        bbo.PLAN_PATH,
        predecessor.PLAN_PATH,
        PREDECESSOR_MANIFEST,
        PREDECESSOR_RECORD,
    ):
        _copy_input(root, relative)
    plan = bbo.load_plan(root=root)
    boundary = RepoBoundary(root)
    scope = bbo.required_scope(root=root, plan=plan)
    base_scope = {
        key: value
        for key, value in scope.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    approval = (
        f"APPROVE {bbo.OPERATION} PLAN {plan['plan_id']} "
        f"SHA256 {sha256_file(root / bbo.PLAN_PATH)}"
    )
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=bbo.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=base_scope,
        approval_command=bbo.OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=sha256_file(root / bbo.PLAN_PATH),
        approval_line=approval,
    )
    path = root / "state" / "authorizations" / f"{receipt.receipt_id}.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(canonical_bytes(receipt.as_dict()) + b"\n")
    return plan, path


def test_market_partition_successor_freezes_all_eighteen_markets() -> None:
    plan = bbo.load_plan(root=ROOT)
    requests = bbo.build_request_ledger(predecessor.load_plan(root=ROOT))

    assert len(requests) == 4_698
    assert len({request.request_id for request in requests}) == 4_698
    assert tuple(bbo.MARKET_SYMBOLS) == (
        "ES", "NQ", "CL", "GC", "ZN", "6E", "MES", "MCL",
        "NG", "RB", "HG", "SR3", "ZB", "6J", "ZC", "ZS", "LE", "HE",
    )
    assert len(bbo.SYMBOLS) == 20
    assert sum(request.trade_date in bbo.DEGRADED_DATES for request in requests) == 108
    assert plan["limits"] == {
        "maximum_billable_bytes": 10_000_000_000,
        "maximum_host_runtime_seconds": 21_600,
        "maximum_metadata_attempts": 37_592,
        "maximum_metadata_calls": 9_398,
        "maximum_parallel_preflight_requests": 6,
        "maximum_parallel_downloads": 4,
        "maximum_retries_per_request": 3,
        "maximum_staged_bytes": 5_000_000_000,
        "maximum_timeseries_attempts": 18_792,
        "minimum_billable_bytes": 1_400_000_000,
        "minimum_nonempty_requests": 4_500,
        "minimum_nonempty_requests_per_market": 250,
        "request_count": 4_698,
        "required_free_disk_bytes": 5_000_000_000,
        "throttled_parallel_downloads": 2,
    }
    assert not any(plan["authority"].values())
    require_current_real_history_operation(bbo.OPERATION, {})

    request = next(item for item in requests if item.market == "SR3")
    assert bbo.canonical_paths(request) == (
        Path(f"data/dbn/bbo_1s/SR3/2025/{request.trade_date}.dbn.zst"),
        Path(
            f"data/dbn/bbo_1s/SR3/2025/"
            f"{request.trade_date}.dbn.zst.manifest.json"
        ),
    )


def test_preflight_covers_all_requests_and_fails_on_cost_or_thin_coverage() -> None:
    plan = bbo.load_plan(root=ROOT)
    ledger = bbo.build_request_ledger(predecessor.load_plan(root=ROOT))
    lock = threading.Lock()
    barrier = threading.Barrier(6)
    state = {"active": 0, "maximum_active": 0, "ordinal": 0}

    def cost(**_kwargs) -> str:
        with lock:
            state["active"] += 1
            state["maximum_active"] = max(state["maximum_active"], state["active"])
            state["ordinal"] += 1
            ordinal = state["ordinal"]
        if ordinal <= 6:
            barrier.wait(timeout=5)
        with lock:
            state["active"] -= 1
        return "0"

    apis = _provider(ledger, get_range=lambda **_kwargs: None)
    apis = predecessor.ProviderApis(
        get_dataset_range=apis.get_dataset_range,
        resolve=apis.resolve,
        get_cost=cost,
        get_billable_size=apis.get_billable_size,
        get_range=apis.get_range,
    )
    preflight = bbo.run_preflight(
        plan=plan,
        ledger=ledger,
        apis=apis,
    )
    assert preflight.total_billable_bytes == 1_864_800_000
    assert preflight.empty_request_count == 36
    assert preflight.metadata_calls == preflight.metadata_attempts == 9_398
    assert state["maximum_active"] == 6

    with pytest.raises(UnauthorizedOperation, match="exactly zero"):
        bbo.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=_provider(ledger, get_range=lambda **_kwargs: None, cost="0.01"),
        )
    with pytest.raises(UnauthorizedOperation, match="coverage"):
        bbo.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=_provider(
                ledger,
                get_range=lambda **_kwargs: None,
                size=lambda **_kwargs: 0,
            ),
        )


def test_sidecars_bind_market_bytes_metadata_and_zero_disposition(tmp_path: Path) -> None:
    plan = bbo.load_plan(root=ROOT)
    ledger = bbo.build_request_ledger(predecessor.load_plan(root=ROOT))
    request = next(
        item for item in ledger if item.market == "NG" and item.trade_date == "2025-09-17"
    )
    dbn_path = tmp_path / f"{request.trade_date}.dbn.zst"
    sidecar_path = dbn_path.with_name(f"{dbn_path.name}.manifest.json")
    dbn_path.write_bytes(b"synthetic-not-a-real-dbn")
    sidecar = bbo.build_candidate_sidecar(
        request=request,
        dbn_path=dbn_path,
        plan_id=str(plan["plan_id"]),
        predecessor_manifest_id=str(plan["predecessor"]["acquisition_manifest_id"]),
        billable_bytes=400_000,
    )
    sidecar_path.write_bytes(canonical_bytes(sidecar) + b"\n")
    observed = bbo.validate_candidate_pair(
        request=request,
        dbn_path=dbn_path,
        sidecar_path=sidecar_path,
        plan_id=str(plan["plan_id"]),
        predecessor_manifest_id=str(plan["predecessor"]["acquisition_manifest_id"]),
        billable_bytes=400_000,
        metadata_loader=lambda _: _metadata(request),
    )
    assert observed["state"] == "PROVIDER_DEGRADED_QUARANTINED"
    assert observed["quality_certificate_id"] is None
    assert observed["row_quality_certified"] is False

    zero = next(item for item in ledger if item.market == "HE")
    empty = bbo.build_empty_sidecar(
        request=zero,
        plan_id=str(plan["plan_id"]),
        predecessor_manifest_id=str(plan["predecessor"]["acquisition_manifest_id"]),
    )
    assert empty["state"] == (
        "PROVIDER_ZERO_BILLABLE_NO_DBN_NOT_ROW_QUALITY_CERTIFIED"
    )
    assert empty["file_sha256"] is None

    with pytest.raises(IntegrityError, match="metadata differs"):
        bbo.validate_candidate_pair(
            request=request,
            dbn_path=dbn_path,
            sidecar_path=sidecar_path,
            plan_id=str(plan["plan_id"]),
            predecessor_manifest_id=str(plan["predecessor"]["acquisition_manifest_id"]),
            billable_bytes=400_000,
            metadata_loader=lambda _: object(),
        )


def test_full_synthetic_run_fails_closed_resumes_and_never_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bbo.os, "fsync", lambda _fd: None)
    plan, receipt_path = _receipt_path(tmp_path)
    ledger = bbo.build_request_ledger(predecessor.load_plan(root=tmp_path))
    by_query = {
        (request.start, tuple(request.symbols)): request for request in ledger
    }
    by_market_date = {
        (request.market, request.trade_date): request for request in ledger
    }
    calls: dict[str, int] = {}
    state = {"active": 0, "maximum_active": 0, "ordinal": 0, "armed": True}
    lock = threading.Lock()
    barrier = threading.Barrier(4)
    failure_request = ledger[0]

    def provider_factory() -> predecessor.ProviderApis:
        def get_range(**kwargs):
            request = by_query[(kwargs["start"], tuple(kwargs["symbols"]))]
            with lock:
                calls[request.request_id] = calls.get(request.request_id, 0) + 1
                state["ordinal"] += 1
                ordinal = state["ordinal"]
                state["active"] += 1
                state["maximum_active"] = max(state["maximum_active"], state["active"])
                should_fail = state["armed"] and request == failure_request
                if should_fail:
                    state["armed"] = False
            if ordinal <= 4:
                barrier.wait(timeout=5)
            Path(kwargs["path"]).write_bytes(request.request_id.encode("ascii"))
            with lock:
                state["active"] -= 1
            if should_fail:
                raise RuntimeError("synthetic terminal transport failure")
            return SimpleNamespace(metadata=_metadata(request))

        return _provider(ledger, get_range=get_range)

    def metadata_loader(path: Path) -> object:
        request = by_market_date[(path.parent.parent.name, path.name[:10])]
        return _metadata(request)

    arguments = {
        "root": tmp_path,
        "authorization_path": receipt_path,
        "provider_factory": provider_factory,
        "disk_usage": lambda _path: SimpleNamespace(free=20_000_000_000),
        "environment_check": lambda _root: None,
        "metadata_loader": metadata_loader,
        "sleeper": lambda _seconds: None,
    }
    with pytest.raises(IntegrityError, match="failed closed"):
        bbo.execute_receipt_file_acquisition(**arguments)
    attempt = tmp_path / bbo.STAGING_ROOT / receipt_path.stem
    failure_paths = list((attempt / "failures").glob("*.json"))
    assert len(failure_paths) == 1
    assert bbo._object(failure_paths[0], "failure")["failed_request_id"] == (
        failure_request.request_id
    )

    first = bbo.execute_receipt_file_acquisition(**arguments)
    second = bbo.execute_receipt_file_acquisition(**arguments)
    assert first == second
    assert first["request_count"] == 4_698
    assert first["nonempty_request_count"] == 4_662
    assert first["empty_request_count"] == 36
    assert first["provider_degraded_request_count"] == 108
    assert first["fresh_total_billable_bytes"] == 1_864_800_000
    assert state["maximum_active"] == 4
    assert calls[failure_request.request_id] == 2
    assert all(
        count == 1
        for request_id, count in calls.items()
        if request_id != failure_request.request_id
    )
    assert set(calls) == {
        request.request_id
        for request in ledger
        if request.trade_date not in bbo.PREDECESSOR_EMPTY_DATES
    }
    assert not (tmp_path / "data" / "dbn").exists()

    empty_request = next(
        request
        for request in ledger
        if request.market == "ZC" and request.trade_date == "2025-12-25"
    )
    raw, sidecar = bbo._paths(attempt, empty_request)
    assert not raw.exists()
    assert bbo._object(sidecar, "empty sidecar")["state"] == (
        "PROVIDER_ZERO_BILLABLE_NO_DBN_NOT_ROW_QUALITY_CERTIFIED"
    )
