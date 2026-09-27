from __future__ import annotations

import json
import threading
import time
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest

import futures_rebuild.l0_cutoff_alignment_acquisition as l0
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.research_gateway_policy import require_current_real_history_operation


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.current


def _metadata(request: l0.L0Request) -> SimpleNamespace:
    return SimpleNamespace(
        dataset=l0.DATASET,
        schema=request.family,
        stype_in=request.stype_in,
        stype_out=l0.STYPE_OUT,
        start=l0._iso_to_ns(l0.START_INCLUSIVE),
        end=l0._iso_to_ns(l0.END_EXCLUSIVE),
        symbols=[request.symbol],
    )


def _provider(
    *, get_range, cost: object = "0", size: object = 1_000_000,
    entitlement_end: str = l0.END_EXCLUSIVE, unresolved: bool = False,
) -> l0.ProviderApis:
    def resolve(**kwargs):
        symbols = kwargs["symbols"]
        result_symbols = (
            [f"{symbol.removesuffix('.FUT')}U6" for symbol in symbols]
            if kwargs["stype_in"] == "parent"
            else symbols
        )
        return {
            "result": {
                symbol: [{"d0": "2026-07-14", "d1": "2026-09-01", "s": 1}]
                for symbol in result_symbols
            },
            "status": 1 if kwargs["stype_in"] == "parent" else 0,
            "symbols": symbols,
            "stype_in": kwargs["stype_in"],
            "stype_out": kwargs["stype_out"],
            "start_date": kwargs["start_date"],
            "end_date": kwargs["end_date"],
            "partial": ["LEGITIMATE_CHILD_PARTIAL"] if kwargs["stype_in"] == "parent" else [],
            "not_found": [symbols[0]] if unresolved else [],
        }

    return l0.ProviderApis(
        get_dataset_range=lambda **_kwargs: {
            "schema": {
                family: {"start": "2010-01-01T00:00:00Z", "end": entitlement_end}
                for family in l0.FAMILIES
            }
        },
        resolve=resolve,
        get_cost=(cost if callable(cost) else lambda **_kwargs: cost),
        get_billable_size=(size if callable(size) else lambda **_kwargs: size),
        get_range=get_range,
    )


def _copy_plan_source(root: Path) -> None:
    (root / "configs").mkdir(exist_ok=True)
    for relative in (l0.PLAN_PATH, l0.SOURCE_CONTRACT_PATH):
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((ROOT / relative).read_bytes())


def _receipt_path(root: Path) -> tuple[dict[str, object], Path]:
    _copy_plan_source(root)
    plan = l0.load_plan(root=root)
    boundary = RepoBoundary(root)
    scope = l0.required_scope(root=root, plan=plan)
    base_scope = {
        key: value
        for key, value in scope.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    approval = (
        f"APPROVE {l0.OPERATION} PLAN {plan['plan_id']} "
        f"SHA256 {sha256_file(root / l0.PLAN_PATH)}"
    )
    receipt = OperationReceipt.issue_user_approved(
        boundary,
        operation=l0.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=base_scope,
        approval_command=l0.OPERATION,
        approval_plan_id=plan["plan_id"],
        approval_plan_sha256=sha256_file(root / l0.PLAN_PATH),
        approval_line=approval,
    )
    path = root / "state" / "authorizations" / f"{receipt.receipt_id}.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(canonical_bytes(receipt.as_dict()) + b"\n")
    return plan, path


def test_plan_freezes_58_by_7_scope_queries_paths_and_limits() -> None:
    plan = l0.load_plan(root=ROOT)
    ledger = l0.build_request_ledger(plan)
    assert len(l0.STANDARD_ROOTS) == 41
    assert len(l0.MICRO_ROOTS) == 17
    assert len(ledger) == 406
    assert len({item.request_id for item in ledger}) == 406
    assert ledger[0].symbol == "ES.FUT"
    assert ledger[0].stype_in == "parent"
    assert ledger[1].symbol == "ES.v.0"
    assert all(
        item.symbol == (f"{item.market}.FUT" if item.family == "definition" else f"{item.market}.v.0")
        for item in ledger
    )
    raw, sidecar = l0._paths(Path("attempt"), ledger[0])
    assert raw.as_posix().endswith(
        "raw/definition/ES/2026/2026-07-14_2026-09-01.dbn.zst"
    )
    assert sidecar.name.endswith(".dbn.zst.manifest.json")
    production_attempt = ROOT / l0.STAGING_ROOT / ("f" * 64)
    assert max(
        len(str(l0._paths(production_attempt, item)[1].with_name(
            f"{l0._paths(production_attempt, item)[1].name}.p.{'f' * 8}"
        )))
        for item in ledger
    ) < 260
    assert plan["implementation_sha256"] == sha256_file(Path(l0.__file__))
    assert plan["limits"]["maximum_parallel_downloads"] == 4
    assert plan["limits"]["maximum_concurrent_ohlcv_1s"] == 2
    require_current_real_history_operation(l0.OPERATION, {})


def test_preflight_revalidates_entitlement_symbology_zero_cost_and_size() -> None:
    plan = l0.load_plan(root=ROOT)
    ledger = l0.build_request_ledger(plan)
    status_request = next(item for item in ledger if item.family == "status")

    def size(**kwargs):
        return 0 if kwargs["schema"] == status_request.family and kwargs["symbols"] == [status_request.symbol] else 1

    preflight = l0.run_preflight(
        plan=plan,
        ledger=ledger,
        apis=_provider(get_range=lambda **_kwargs: None, size=size),
    )
    assert preflight.metadata_calls == 815
    assert preflight.metadata_attempts == 815
    assert preflight.empty_request_count == 1
    assert preflight.total_billable_bytes == 405

    with pytest.raises(UnauthorizedOperation, match="exactly zero"):
        l0.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=_provider(get_range=lambda **_kwargs: None, cost="0.01"),
        )
    with pytest.raises(UnauthorizedOperation, match="does not cover"):
        l0.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=_provider(
                get_range=lambda **_kwargs: None,
                entitlement_end="2026-08-31T00:00:00Z",
            ),
        )
    with pytest.raises(UnauthorizedOperation, match="partial or unresolved"):
        l0.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=_provider(get_range=lambda **_kwargs: None, unresolved=True),
        )
    with pytest.raises(UnauthorizedOperation, match="OHLCV request"):
        l0.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=_provider(get_range=lambda **_kwargs: None, size=0),
        )
    with pytest.raises(UnauthorizedOperation, match="billable-size bounds"):
        l0.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=_provider(get_range=lambda **_kwargs: None, size=20_000_000),
        )


def test_metadata_retry_runtime_and_implementation_drift_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    streaming_error = type("BentoError", (Exception,), {})
    assert l0._transient(streaming_error("Error streaming response: connection reset"))
    assert not l0._transient(streaming_error("Cannot decode DBN stream"))

    plan = l0.load_plan(root=ROOT)
    ledger = l0.build_request_ledger(plan)
    base = _provider(get_range=lambda **_kwargs: None, size=1)
    calls = 0

    class RetryableError(RuntimeError):
        status_code = 500

    def get_cost(**_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RetryableError()
        return "0"

    retried = l0.ProviderApis(
        base.get_dataset_range,
        base.resolve,
        get_cost,
        base.get_billable_size,
        base.get_range,
    )
    assert l0.run_preflight(
        plan=plan,
        ledger=ledger,
        apis=retried,
        sleeper=lambda _seconds: None,
    ).metadata_attempts == 816

    now = {"value": 0.0}

    def overrun(**_kwargs):
        now["value"] = 2.0
        return {}

    expired = l0.ProviderApis(
        overrun,
        base.resolve,
        base.get_cost,
        base.get_billable_size,
        base.get_range,
    )
    with pytest.raises(UnauthorizedOperation, match="runtime exhausted"):
        l0.run_preflight(
            plan=plan,
            ledger=ledger,
            apis=expired,
            deadline=1.0,
            clock=lambda: now["value"],
            sleeper=lambda _seconds: None,
        )

    real_sha256_file = l0.sha256_file

    def drift(path: Path) -> str:
        if Path(path).resolve() == Path(l0.__file__).resolve():
            return "0" * 64
        return real_sha256_file(path)

    monkeypatch.setattr(l0, "sha256_file", drift)
    with pytest.raises(IntegrityError, match="plan drifted"):
        l0.load_plan(root=ROOT)


def test_predecessor_binding_and_existing_delta_conflict(tmp_path: Path) -> None:
    plan = l0.load_plan(root=ROOT)
    request = l0.build_request_ledger(plan)[0]
    prior = Path("data/dbn/definition/ES/2026/2026-01-01_2026-07-14.dbn.zst")
    prior_sidecar = prior.with_name(f"{prior.name}.manifest.json")
    for relative, content in ((prior, b"prior"), (prior_sidecar, b"sidecar")):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    entries = [
        {
            "family": "definition",
            "interval_end_exclusive": "2026-07-14",
            "interval_start_inclusive": "2026-01-01",
            "kind": kind,
            "market": "ES",
            "path": relative.as_posix(),
            "sha256": sha256_file(tmp_path / relative),
            "size_bytes": (tmp_path / relative).stat().st_size,
        }
        for relative, kind in ((prior, "DBN"), (prior_sidecar, "SIDECAR"))
    ]
    inventory_core = {
        "content_inventory_sha256": "c" * 64,
        "entries": entries,
        "file_count": 2,
        "inventory_id": "i" * 64,
    }
    inventory_path = tmp_path / "state" / "inventory.json"
    inventory_path.parent.mkdir(parents=True)
    inventory_path.write_bytes(canonical_bytes(inventory_core) + b"\n")
    small_plan = {
        "source_binding": {
            "complete_inventory_path": inventory_path.relative_to(tmp_path).as_posix(),
            "complete_inventory_sha256": sha256_file(inventory_path),
            "complete_inventory_content_sha256": "c" * 64,
            "complete_inventory_id": "i" * 64,
            "complete_inventory_file_count": 2,
        }
    }
    assert len(
        l0.verify_predecessor_custody(
            root=tmp_path,
            plan=small_plan,
            ledger=[request],
        )
    ) == 64
    destination = (
        tmp_path / "data/dbn/definition/ES/2026/2026-07-14_2026-09-01.dbn.zst"
    )
    destination.write_bytes(b"conflict")
    with pytest.raises(IntegrityError, match="already exists"):
        l0.verify_predecessor_custody(
            root=tmp_path,
            plan=small_plan,
            ledger=[request],
        )


def test_four_workers_two_ohlcv_1s_slots_open_store_and_warning_capture(
    tmp_path: Path,
) -> None:
    plan = l0.load_plan(root=ROOT)
    ledger = l0.build_request_ledger(plan)
    light = [item for item in ledger if item.family != "ohlcv-1s"][:8]
    heavy = [item for item in ledger if item.family == "ohlcv-1s"][:8]
    requests = light + heavy
    preflight = l0.Preflight(
        {item.request_id: 1 for item in requests}, len(requests), 0, 3 + 2 * len(requests), 0
    )
    by_key = {(item.family, item.symbol): item for item in requests}
    lock = threading.Lock()
    barrier = threading.Barrier(4)
    state = {"calls": 0, "active": 0, "maximum": 0, "active_1s": 0, "maximum_1s": 0}

    def factory() -> l0.ProviderApis:
        def get_range(**kwargs):
            request = by_key[(kwargs["schema"], kwargs["symbols"][0])]
            with lock:
                state["calls"] += 1
                ordinal = state["calls"]
                state["active"] += 1
                state["maximum"] = max(state["maximum"], state["active"])
                if request.family == "ohlcv-1s":
                    state["active_1s"] += 1
                    state["maximum_1s"] = max(state["maximum_1s"], state["active_1s"])
            if ordinal <= 4:
                barrier.wait(timeout=5)
            if request == heavy[0]:
                warnings.warn("provider degraded 2026-08-07", UserWarning)
            Path(kwargs["path"]).write_bytes(request.request_id.encode("ascii"))
            time.sleep(0.01)
            with lock:
                state["active"] -= 1
                if request.family == "ohlcv-1s":
                    state["active_1s"] -= 1

            class OpenStore:
                def __init__(self) -> None:
                    self.metadata = _metadata(request)
                    self.stream = Path(kwargs["path"]).open("rb")

                def __del__(self) -> None:
                    self.stream.close()

            return OpenStore()

        return _provider(get_range=get_range, size=1)

    attempt = tmp_path / l0.STAGING_ROOT / "attempt"
    with l0.ProviderWarningCollector() as collector:
        results = l0.download_missing(
            root=tmp_path,
            attempt=attempt,
            plan=plan,
            requests=requests,
            preflight=preflight,
            provider_factory=factory,
            warnings_collector=collector,
            sleeper=lambda _seconds: None,
        )
    assert len(results) == 4
    assert state["maximum"] == 4
    assert state["maximum_1s"] == 2
    assert state["calls"] == len(requests)
    heavy_sidecar = l0._verify_sidecar(
        root=tmp_path,
        attempt=attempt,
        plan=plan,
        request=heavy[0],
        billable_bytes=1,
    )
    assert heavy_sidecar["provider_warnings"] == [
        {"category": "UserWarning", "dates": ["2026-08-07"], "count": 1}
    ]


def test_full_406_request_run_fails_resumes_and_is_idempotent(tmp_path: Path) -> None:
    plan, receipt_path = _receipt_path(tmp_path)
    ledger = l0.build_request_ledger(plan)
    by_key = {(item.family, item.symbol): item for item in ledger}
    empty_request = next(item for item in ledger if item.family == "status")
    failure_request = next(item for item in ledger if item.family == "definition")
    warning_request = ledger[-1]
    calls: dict[str, int] = {}
    lock = threading.Lock()
    failed = {"armed": True}

    def size(**kwargs):
        request = by_key[(kwargs["schema"], kwargs["symbols"][0])]
        return 0 if request == empty_request else 1

    def factory() -> l0.ProviderApis:
        def get_range(**kwargs):
            request = by_key[(kwargs["schema"], kwargs["symbols"][0])]
            with lock:
                calls[request.request_id] = calls.get(request.request_id, 0) + 1
                should_fail = failed["armed"] and request == failure_request
                if should_fail:
                    failed["armed"] = False
            if request == warning_request:
                warnings.warn("reduced quality on 2026-08-28", UserWarning)
            Path(kwargs["path"]).write_bytes(request.request_id.encode("ascii"))
            if should_fail:
                raise RuntimeError("synthetic terminal transport failure")
            return SimpleNamespace(metadata=_metadata(request))

        return _provider(get_range=get_range, size=size)

    arguments = {
        "root": tmp_path,
        "authorization_path": receipt_path,
        "provider_factory": factory,
        "disk_usage": lambda _path: SimpleNamespace(free=10_000_000_000),
        "environment_check": lambda _root: None,
        "predecessor_check": lambda **_kwargs: "p" * 64,
        "sleeper": lambda _seconds: None,
    }
    with pytest.raises(IntegrityError, match="failed closed"):
        l0.execute_receipt_file_acquisition(**arguments)
    attempt = tmp_path / l0.STAGING_ROOT / receipt_path.stem
    failures = list((attempt / "failures").glob("*.json"))
    assert len(failures) == 1
    failure = json.loads(failures[0].read_text(encoding="utf-8"))
    assert failure["state"] == "FAILED_CLOSED_RESUMABLE"
    assert failure["automatic_resume_supported"] is True

    first = l0.execute_receipt_file_acquisition(**arguments)
    second = l0.execute_receipt_file_acquisition(**arguments)
    verified = l0.verify_completed_acquisition(
        root=tmp_path,
        authorization_receipt_id=receipt_path.stem,
        predecessor_check=lambda **_kwargs: "p" * 64,
    )
    assert first == second
    assert first["request_count"] == 406
    assert first["nonempty_request_count"] == 405
    assert first["provider_warning_count"] == 1
    assert verified["request_count"] == 406
    assert calls[failure_request.request_id] == 2
    assert all(count == 1 for request_id, count in calls.items() if request_id != failure_request.request_id)
    assert set(calls) == {item.request_id for item in ledger if item != empty_request}
    assert not (tmp_path / "data/dbn").exists()
    record_path = tmp_path / l0.RECORD_ROOT / f"{verified['manifest_id']}.json"
    record_path.write_bytes(b"{}\n")
    with pytest.raises(IntegrityError, match="record or event differs"):
        l0.verify_completed_acquisition(
            root=tmp_path,
            authorization_receipt_id=receipt_path.stem,
            predecessor_check=lambda **_kwargs: "p" * 64,
        )


def test_execution_rejects_insufficient_disk_before_provider(tmp_path: Path) -> None:
    _plan, receipt_path = _receipt_path(tmp_path)
    calls = 0

    def factory() -> l0.ProviderApis:
        nonlocal calls
        calls += 1
        return _provider(get_range=lambda **_kwargs: None)

    with pytest.raises(UnauthorizedOperation, match="insufficient free space"):
        l0.execute_receipt_file_acquisition(
            root=tmp_path,
            authorization_path=receipt_path,
            provider_factory=factory,
            disk_usage=lambda _path: SimpleNamespace(free=1),
            environment_check=lambda _root: None,
            predecessor_check=lambda **_kwargs: "p" * 64,
        )
    assert calls == 0
