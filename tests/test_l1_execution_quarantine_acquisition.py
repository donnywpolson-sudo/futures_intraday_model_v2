from __future__ import annotations

import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import futures_rebuild.l1_execution_quarantine_acquisition as l1
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


def _metadata(request: l1.DailyRequest) -> SimpleNamespace:
    return SimpleNamespace(
        dataset=l1.DATASET,
        schema=l1.SCHEMA,
        stype_in=l1.STYPE_IN,
        stype_out=l1.STYPE_OUT,
        start=l1._iso_to_ns(request.start),
        end=l1._iso_to_ns(request.end),
        symbols=list(l1.SYMBOLS),
    )


def _provider(
    *, get_range, cost="0", size=6_000_000, end="2026-09-01T00:00:00Z",
) -> l1.ProviderApis:
    plan = l1.load_plan(root=ROOT)
    period = plan["period"]
    return l1.ProviderApis(
        get_dataset_range=lambda **_kwargs: {
            "schema": {
                l1.SCHEMA: {
                    "start": "2025-09-01T00:00:00Z",
                    "end": end,
                }
            }
        },
        resolve=lambda **_kwargs: {
            "result": {symbol: [{"d0": "2025-09-01", "d1": "2026-09-01", "s": 1}] for symbol in l1.SYMBOLS},
            "stype_in": l1.STYPE_IN,
            "stype_out": l1.STYPE_OUT,
            "start_date": period["start_date_inclusive"],
            "end_date": "2026-09-01",
            "partial": [],
            "not_found": [],
        },
        get_cost=lambda **_kwargs: cost,
        get_billable_size=(size if callable(size) else lambda **_kwargs: size),
        get_range=get_range,
    )


def _receipt_path(root: Path) -> tuple[dict[str, object], Path]:
    (root / "configs").mkdir(exist_ok=True)
    plan_path = root / l1.PLAN_PATH
    plan_path.write_bytes((ROOT / l1.PLAN_PATH).read_bytes())
    plan = l1.load_plan(root=root)
    boundary = RepoBoundary(root)
    scope = l1.required_scope(root=root, plan=plan)
    base_scope = {
        key: value
        for key, value in scope.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    approval = (
        f"APPROVE {l1.OPERATION} PLAN {plan['plan_id']} "
        f"SHA256 {sha256_file(plan_path)}"
    )
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=l1.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=base_scope,
        approval_command=l1.OPERATION,
        approval_plan_id=plan["plan_id"],
        approval_plan_sha256=sha256_file(plan_path),
        approval_line=approval,
    )
    path = root / "state" / "authorizations" / f"{receipt.receipt_id}.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(canonical_bytes(receipt.as_dict()) + b"\n")
    return plan, path


def test_plan_freezes_scope_four_workers_and_all_seven_remediations() -> None:
    plan = l1.load_plan(root=ROOT)
    ledger = l1.build_request_ledger(plan)
    by_date = {item.trade_date: item for item in ledger}
    assert len(ledger) == 261
    assert tuple(plan["request_symbol_order"]) == l1.SYMBOLS
    assert plan["limits"]["maximum_parallel_downloads"] == 4
    assert plan["limits"]["throttled_parallel_downloads"] == 2
    assert plan["limits"]["minimum_billable_bytes"] == 1_400_000_000
    assert plan["limits"]["minimum_nonempty_requests"] == 250
    assert plan["implementation_sha256"] == sha256_file(Path(l1.__file__))
    assert {item["finding"] for item in plan["audit_remediations"]} == set(range(1, 8))
    assert by_date["2025-10-31"].start == "2025-10-31T14:55:00Z"
    assert by_date["2025-11-03"].start == "2025-11-03T15:55:00Z"
    assert plan["canonical_promotion"] == {
        "active_source_contract_change": False,
        "alpha_source_allowlist_change": False,
        "catalog_compatible_now": False,
        "destination": "data/dbn/bbo_1s/execution_bundle_v1",
        "mode": "CREATE_ONLY_VERIFIED_COPY_PRESERVE_PROVIDER_STAGING",
        "publication_staging_root": "state/data_publication_staging/l1_bbo1s_canonical_promotion",
        "requires_independent_validation": True,
        "requires_separate_approval": True,
        "state": "DEFERRED_PROVIDER_FREE_NOT_PART_OF_ACQUISITION",
    }
    require_current_real_history_operation(l1.OPERATION, {})


def test_preflight_revalidates_entitlement_symbols_zero_cost_and_size() -> None:
    plan = l1.load_plan(root=ROOT)
    ledger = l1.build_request_ledger(plan)
    preflight = l1.run_preflight(
        plan=plan,
        ledger=ledger,
        apis=_provider(get_range=lambda **_kwargs: None),
    )
    assert preflight.total_billable_bytes == 1_566_000_000
    assert preflight.metadata_calls == 524
    assert preflight.metadata_attempts == 524
    with pytest.raises(UnauthorizedOperation, match="exactly zero"):
        l1.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=_provider(get_range=lambda **_kwargs: None, cost="0.01"),
        )
    with pytest.raises(UnauthorizedOperation, match="does not cover"):
        l1.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=_provider(
                get_range=lambda **_kwargs: None,
                end="2026-08-31T00:00:00Z",
            ),
        )
    with pytest.raises(UnauthorizedOperation, match="billable-size bounds"):
        l1.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=_provider(get_range=lambda **_kwargs: None, size=10_000_000),
        )
    with pytest.raises(UnauthorizedOperation, match="coverage, or billable-size bounds"):
        l1.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=_provider(get_range=lambda **_kwargs: None, size=0),
        )


def test_metadata_retries_are_bounded_and_deadline_is_enforced() -> None:
    plan = l1.load_plan(root=ROOT)
    ledger = l1.build_request_ledger(plan)
    base = _provider(get_range=lambda **_kwargs: None)
    calls = 0

    class RetryableError(RuntimeError):
        status_code = 500

    def get_cost(**_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RetryableError()
        return "0"

    retried = l1.ProviderApis(
        base.get_dataset_range, base.resolve, get_cost, base.get_billable_size,
        base.get_range,
    )
    preflight = l1.run_preflight(
        plan=plan, ledger=ledger, apis=retried, sleeper=lambda _seconds: None,
    )
    assert preflight.metadata_attempts == 525

    now = {"value": 0.0}

    def overrun(**_kwargs):
        now["value"] = 2.0
        return {}

    expired = l1.ProviderApis(
        overrun, base.resolve, base.get_cost, base.get_billable_size, base.get_range,
    )
    with pytest.raises(UnauthorizedOperation, match="runtime exhausted"):
        l1.run_preflight(
            plan=plan, ledger=ledger, apis=expired, deadline=1.0,
            clock=lambda: now["value"], sleeper=lambda _seconds: None,
        )

    class BoundedApi:
        TIMEOUT = 100

        def call(self) -> None:
            return None

    api = BoundedApi()
    l1._provider_call(api.call, deadline=25.0, clock=lambda: 0.0, kwargs={})
    assert api.TIMEOUT == 25.0


def test_plan_rejects_implementation_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    real_sha256_file = l1.sha256_file

    def drift(path: Path) -> str:
        if Path(path).resolve() == Path(l1.__file__).resolve():
            return "0" * 64
        return real_sha256_file(path)

    monkeypatch.setattr(l1, "sha256_file", drift)
    with pytest.raises(IntegrityError, match="plan drifted"):
        l1.load_plan(root=ROOT)


def test_four_workers_download_unique_dates_into_quarantine(tmp_path: Path) -> None:
    plan = l1.load_plan(root=ROOT)
    requests = l1.build_request_ledger(plan)[:12]
    sizes = {item.request_id: 1 for item in requests}
    preflight = l1.Preflight(sizes, len(sizes), 0, 2 + 2 * len(sizes))
    lock = threading.Lock()
    barrier = threading.Barrier(4)
    state = {"clients": 0, "active": 0, "maximum_active": 0, "calls": 0}

    def factory() -> l1.ProviderApis:
        with lock:
            state["clients"] += 1

        def get_range(**kwargs):
            request = next(item for item in requests if item.start == kwargs["start"])
            with lock:
                state["calls"] += 1
                ordinal = state["calls"]
                state["active"] += 1
                state["maximum_active"] = max(state["maximum_active"], state["active"])
            if ordinal <= 4:
                barrier.wait(timeout=5)
            Path(kwargs["path"]).open("xb").write(request.request_id.encode("ascii"))
            time.sleep(0.005)
            with lock:
                state["active"] -= 1

            class OpenStore:
                def __init__(self) -> None:
                    self.metadata = _metadata(request)
                    self.stream = Path(kwargs["path"]).open("rb")

                def __del__(self) -> None:
                    self.stream.close()

            return OpenStore()

        return _provider(get_range=get_range)

    attempt = tmp_path / l1.STAGING_ROOT / "attempt"
    results = l1.download_missing(
        root=tmp_path,
        attempt=attempt,
        plan=plan,
        requests=requests,
        preflight=preflight,
        provider_factory=factory,
        sleeper=lambda _seconds: None,
    )
    assert len(results) == state["clients"] == state["maximum_active"] == 4
    assert state["calls"] == len(requests)
    assert {request.request_id for request in requests} == {
        request_id for result in results for request_id in result.completed_request_ids
    }
    for request in requests:
        raw, sidecar = l1._paths(attempt, request)
        assert raw.is_file() and sidecar.is_file()
        assert l1._verify_sidecar(
            root=tmp_path,
            attempt=attempt,
            plan=plan,
            request=request,
            billable_bytes=1,
        ) is not None
    assert not (tmp_path / "data" / "dbn").exists()


def test_transient_retry_preserves_failed_partial_and_never_overwrites(tmp_path: Path) -> None:
    plan = l1.load_plan(root=ROOT)
    request = l1.build_request_ledger(plan)[0]
    preflight = l1.Preflight({request.request_id: 1}, 1, 0, 4)
    calls = 0

    class RetryableError(RuntimeError):
        status_code = 500

    def factory() -> l1.ProviderApis:
        def get_range(**kwargs):
            nonlocal calls
            calls += 1
            path = Path(kwargs["path"])
            if calls == 1:
                path.open("xb").write(b"preserved-failed-partial")
                raise RetryableError()
            path.open("xb").write(b"complete-dbn")
            return SimpleNamespace(metadata=_metadata(request))

        return _provider(get_range=get_range)

    attempt = tmp_path / l1.STAGING_ROOT / "attempt"
    l1.download_missing(
        root=tmp_path,
        attempt=attempt,
        plan=plan,
        requests=[request],
        preflight=preflight,
        provider_factory=factory,
        sleeper=lambda _seconds: None,
    )
    raw, _ = l1._paths(attempt, request)
    assert calls == 2
    assert raw.read_bytes() == b"complete-dbn"
    assert len(list(raw.parent.glob("*.partial.*"))) == 1
    raw.write_bytes(b"corrupt")
    with pytest.raises(IntegrityError, match="differs"):
        l1._verify_sidecar(
            root=tmp_path,
            attempt=attempt,
            plan=plan,
            request=request,
            billable_bytes=1,
        )


def test_empty_provider_session_has_sidecar_but_no_dbn(tmp_path: Path) -> None:
    plan = l1.load_plan(root=ROOT)
    request = l1.build_request_ledger(plan)[0]
    attempt = tmp_path / l1.STAGING_ROOT / "attempt"
    raw, sidecar = l1._paths(attempt, request)
    l1._atomic_json(
        sidecar,
        l1._sidecar(
            root=tmp_path,
            attempt=attempt,
            plan=plan,
            request=request,
            billable_bytes=0,
            raw=None,
        ),
    )
    assert not raw.exists()
    assert l1._verify_sidecar(
        root=tmp_path,
        attempt=attempt,
        plan=plan,
        request=request,
        billable_bytes=0,
    )["state"] == "PROVIDER_EMPTY_SESSION_NO_DBN"


def test_process_lock_blocks_a_concurrent_receipt_run(tmp_path: Path) -> None:
    _plan, receipt_path = _receipt_path(tmp_path)
    instance = l1.SingleInstance(tmp_path / l1.LOCK_PATH)
    assert instance.acquire() is True
    calls = 0
    try:
        def factory() -> l1.ProviderApis:
            nonlocal calls
            calls += 1
            return _provider(get_range=lambda **_kwargs: None)

        with pytest.raises(UnauthorizedOperation, match="already running"):
            l1.execute_receipt_file_acquisition(
                root=tmp_path,
                authorization_path=receipt_path,
                provider_factory=factory,
                disk_usage=lambda _path: SimpleNamespace(free=10_000_000_000),
                environment_check=lambda _root: None,
            )
    finally:
        instance.release()
    assert calls == 0


def test_full_provider_free_run_fails_closed_then_resumes_without_duplicates(
    tmp_path: Path,
) -> None:
    plan, receipt_path = _receipt_path(tmp_path)
    ledger = l1.build_request_ledger(plan)
    by_start = {item.start: item for item in ledger}
    empty = {item.start for item in ledger[:3]}
    positive = ledger[3:]
    base_size, extra = divmod(plan["expected_billable_bytes"], len(positive))
    sizes = {
        item.start: base_size + (index < extra)
        for index, item in enumerate(positive)
    }
    sizes.update({start: 0 for start in empty})
    calls: dict[str, int] = {}
    lock = threading.Lock()
    failed = {"armed": True}
    failure_request = positive[0]

    def factory() -> l1.ProviderApis:
        def get_range(**kwargs):
            request = by_start[kwargs["start"]]
            with lock:
                calls[request.request_id] = calls.get(request.request_id, 0) + 1
                should_fail = failed["armed"] and request == failure_request
                if should_fail:
                    failed["armed"] = False
            Path(kwargs["path"]).open("xb").write(request.request_id.encode("ascii"))
            if should_fail:
                raise RuntimeError("synthetic terminal transport failure")
            return SimpleNamespace(metadata=_metadata(request))

        return _provider(
            get_range=get_range,
            size=lambda **kwargs: sizes[kwargs["start"]],
        )

    arguments = {
        "root": tmp_path,
        "authorization_path": receipt_path,
        "provider_factory": factory,
        "disk_usage": lambda _path: SimpleNamespace(free=10_000_000_000),
        "environment_check": lambda _root: None,
        "sleeper": lambda _seconds: None,
    }
    with pytest.raises(IntegrityError, match="failed closed"):
        l1.execute_receipt_file_acquisition(**arguments)
    attempt = tmp_path / l1.STAGING_ROOT / receipt_path.stem
    failure_paths = list((attempt / "failures").glob("*.json"))
    assert len(failure_paths) == 1
    failure = l1._object(failure_paths[0], "failure receipt")
    assert failure["state"] == "FAILED_CLOSED_RESUMABLE"
    assert failure["failed_request_id"] == failure_request.request_id
    assert failure["automatic_resume_supported"] is True

    first = l1.execute_receipt_file_acquisition(**arguments)
    second = l1.execute_receipt_file_acquisition(**arguments)
    assert first == second
    assert first["nonempty_request_count"] == 258
    assert first["fresh_total_billable_bytes"] == plan["expected_billable_bytes"]
    assert calls[failure_request.request_id] == 2
    assert all(
        count == 1
        for request_id, count in calls.items()
        if request_id != failure_request.request_id
    )
    assert set(calls) == {item.request_id for item in positive}
    assert first["maximum_parallel_downloads"] == 4
    assert first["row_quality_certified"] is False
    assert first["canonical_promotion_performed"] is False
    assert not (tmp_path / "data" / "dbn").exists()
