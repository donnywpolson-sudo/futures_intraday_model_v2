from __future__ import annotations

import shutil
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import futures_rebuild.tbbo_16_market_acquisition as acquisition
import futures_rebuild.tbbo_custody_publication as publication
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.custody_successor import binding_matches
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.current


def _test_root(tmp_path: Path) -> Path:
    for relative in (
        acquisition.PLAN_PATH,
        Path("src/futures_rebuild/source_symbology.py"),
    ):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    (tmp_path / "data/dbn/tiny").mkdir(parents=True)
    (tmp_path / "data/dbn/tiny/source.dbn.zst").write_bytes(b"active-source")
    for relative in acquisition.PROTECTED_ACTIVE_PATHS[1:]:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"{}\n")
    # This independent synthetic repository has no production custody activation.
    # Bind its fresh test plan to current code; never rewrite the real frozen plan.
    plan_path = tmp_path / acquisition.PLAN_PATH
    plan = json.loads(plan_path.read_bytes())
    plan["implementation_sha256"] = sha256_file(Path(acquisition.__file__))
    plan["publication"]["publication_implementation_sha256"] = sha256_file(Path(publication.__file__))
    plan["plan_id"] = sha256_json({k: v for k, v in plan.items() if k != "plan_id"})
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    return tmp_path


def _metadata(request: acquisition.MonthlyRequest) -> SimpleNamespace:
    return SimpleNamespace(
        dataset=acquisition.DATASET,
        schema=acquisition.SCHEMA,
        stype_in=acquisition.STYPE_IN,
        stype_out=acquisition.STYPE_OUT,
        start=acquisition._iso_ns(request.start),
        end=acquisition._iso_ns(request.end),
        symbols=[f"{request.market}.v.0"],
        ts_out=False,
        limit=None,
    )


def _provider(
    plan: dict[str, object], *, get_range=lambda **_kwargs: None,
    cost: object = "0", size: object = 1,
    range_end: str = acquisition.END,
    mapping_end: str = acquisition.END[:10],
) -> acquisition.ProviderApis:
    return acquisition.ProviderApis(
        get_dataset_range=lambda **_kwargs: {
            "schema": {
                acquisition.SCHEMA: {
                    "start": acquisition.START,
                    "end": range_end,
                }
            }
        },
        resolve=lambda **_kwargs: {
            "result": {
                f"{market}.v.0": [{
                    "d0": acquisition.START[:10],
                    "d1": mapping_end,
                    "s": 1,
                }]
                for market in acquisition.MARKETS
            },
            "stype_in": acquisition.STYPE_IN,
            "stype_out": acquisition.STYPE_OUT,
            "start_date": acquisition.START[:10],
            "end_date": acquisition.END[:10],
            "partial": [],
            "not_found": [],
        },
        get_cost=(cost if callable(cost) else lambda **_kwargs: cost),
        get_billable_size=(size if callable(size) else lambda **_kwargs: size),
        get_range=get_range,
    )


def _approved(
    root: Path, *, operation: str, classification: OperationClassification,
    full_scope: dict[str, str], plan_id: str, plan_sha256: str,
) -> OperationReceipt:
    scope = {
        key: value for key, value in full_scope.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    return OperationReceipt.issue_user_approved(
        RepoBoundary(root), operation=operation, classification=classification,
        scope=scope, approval_command=operation, approval_plan_id=plan_id,
        approval_plan_sha256=plan_sha256,
        approval_line=f"APPROVE {operation} PLAN {plan_id} SHA256 {plan_sha256}",
    )


def test_plan_freezes_exact_192_monthly_requests_and_40_gb_limits() -> None:
    plan = acquisition.load_plan(root=ROOT)
    ledger = acquisition.build_request_ledger(plan)
    assert tuple(plan["markets"]) == acquisition.MARKETS
    assert len(ledger) == 192
    assert ledger[0].market == "ES" and ledger[0].start == acquisition.START
    assert ledger[11].end == acquisition.END
    assert ledger[12].market == "NQ"
    assert ledger[-1].market == "HE" and ledger[-1].end == acquisition.END
    assert plan["limits"]["maximum_billable_bytes"] == 40_000_000_000
    assert plan["limits"]["maximum_staged_bytes"] == 40_000_000_000
    assert plan["limits"]["required_free_disk_bytes"] == 45_000_000_000
    assert plan["expected_billable_bytes_informational"] == 28_934_630_640
    assert binding_matches(ROOT, Path(acquisition.__file__), plan["implementation_sha256"])
    assert binding_matches(ROOT, Path(publication.__file__), plan["publication"]["publication_implementation_sha256"])


def test_preflight_accepts_40_gb_and_rejects_cost_coverage_empty_and_excess() -> None:
    plan = acquisition.load_plan(root=ROOT)
    ledger = acquisition.build_request_ledger(plan)
    base, extra = divmod(40_000_000_000, len(ledger))

    def exact_size(**kwargs) -> int:
        index = next(
            i for i, request in enumerate(ledger)
            if request.start == kwargs["start"] and request.provider_query()["symbols"] == kwargs["symbols"]
        )
        return base + (index < extra)

    accepted = acquisition.run_preflight(
        plan=plan, ledger=ledger,
        apis=_provider(plan, size=exact_size), sleeper=lambda _seconds: None,
    )
    assert accepted.total_billable_bytes == 40_000_000_000
    assert accepted.metadata_calls == accepted.metadata_attempts == 386
    with pytest.raises(UnauthorizedOperation, match="exactly zero"):
        acquisition.run_preflight(plan=plan, ledger=ledger, apis=_provider(plan, cost="0.01"))
    with pytest.raises(UnauthorizedOperation, match="does not cover"):
        acquisition.run_preflight(
            plan=plan, ledger=ledger,
            apis=_provider(plan, range_end="2026-08-31T00:00:00Z"),
        )
    with pytest.raises(UnauthorizedOperation, match="coverage gap|does not cover"):
        acquisition.run_preflight(
            plan=plan, ledger=ledger,
            apis=_provider(plan, mapping_end="2026-08-31"),
        )
    with pytest.raises(UnauthorizedOperation, match="positive billable"):
        acquisition.run_preflight(plan=plan, ledger=ledger, apis=_provider(plan, size=0))
    with pytest.raises(UnauthorizedOperation, match="40 GB"):
        acquisition.run_preflight(
            plan=plan, ledger=ledger,
            apis=_provider(plan, size=base + 1_000_000),
        )


def test_download_uses_four_workers_retries_429_and_preserves_partial(tmp_path: Path) -> None:
    plan = acquisition.load_plan(root=ROOT)
    requests = acquisition.build_request_ledger(plan)[:16]
    preflight = acquisition.Preflight(
        {request.request_id: 1 for request in requests}, len(requests), 0, 0,
    )
    by_key = {(request.start, request.market): request for request in requests}
    lock = threading.Lock()
    barrier = threading.Barrier(4)
    state = {"calls": 0, "active": 0, "maximum": 0, "failed": False}

    class TooManyRequests(RuntimeError):
        status_code = 429

    def factory() -> acquisition.ProviderApis:
        def get_range(**kwargs):
            request = by_key[(kwargs["start"], kwargs["symbols"][0].split(".")[0])]
            path = Path(kwargs["path"])
            with lock:
                state["calls"] += 1
                ordinal = state["calls"]
                state["active"] += 1
                state["maximum"] = max(state["maximum"], state["active"])
                fail = not state["failed"]
                if fail:
                    state["failed"] = True
            if ordinal <= 4:
                barrier.wait(timeout=5)
            path.write_bytes(request.request_id.encode("ascii"))
            time.sleep(0.003)
            with lock:
                state["active"] -= 1
            if fail:
                raise TooManyRequests()
            return SimpleNamespace(metadata=_metadata(request))

        return _provider(plan, get_range=get_range)

    attempt = tmp_path / "attempt"
    snapshot = attempt / "snapshot"
    results = acquisition.download_missing(
        attempt=attempt, snapshot=snapshot, plan=plan, requests=requests,
        preflight=preflight, provider_factory=factory,
        deadline=time.monotonic() + 60, sleeper=lambda _seconds: None,
    )
    assert state["maximum"] == 4
    assert state["calls"] == len(requests) + 1
    assert sum(len(result.completed_request_ids) for result in results) == len(requests)
    assert len(list(snapshot.rglob("*.partial.*"))) == 1
    for request in requests:
        assert acquisition.verify_sidecar(
            plan=plan, snapshot=snapshot, request=request,
            metadata_loader=lambda _path, request=request: _metadata(request),
        ) is not None


def test_download_staging_ceiling_preserves_the_failed_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = acquisition.load_plan(root=ROOT)
    request = acquisition.build_request_ledger(plan)[0]
    preflight = acquisition.Preflight({request.request_id: 1}, 1, 0, 0)

    def factory() -> acquisition.ProviderApis:
        def get_range(**kwargs):
            Path(kwargs["path"]).write_bytes(b"too-large")
            return SimpleNamespace(metadata=_metadata(request))

        return _provider(plan, get_range=get_range)

    monkeypatch.setattr(acquisition, "MAXIMUM_STAGED_BYTES", 4)
    attempt = tmp_path / "attempt"
    with pytest.raises(IntegrityError, match="failed closed"):
        acquisition.download_missing(
            attempt=attempt, snapshot=attempt / "snapshot", plan=plan,
            requests=[request], preflight=preflight, provider_factory=factory,
            deadline=time.monotonic() + 60, sleeper=lambda _seconds: None,
        )
    assert len(list((attempt / "snapshot").rglob("*.partial.*"))) == 1


def test_full_custody_certification_publication_rollback_and_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _test_root(tmp_path)
    plan = acquisition.load_plan(root=root)
    ledger = acquisition.build_request_ledger(plan)
    by_key = {(request.start, f"{request.market}.v.0"): request for request in ledger}
    downloads = {"count": 0}
    download_lock = threading.Lock()

    def factory() -> acquisition.ProviderApis:
        def get_range(**kwargs):
            request = by_key[(kwargs["start"], kwargs["symbols"][0])]
            with download_lock:
                downloads["count"] += 1
            Path(kwargs["path"]).write_bytes(request.request_id.encode("ascii"))
            return SimpleNamespace(metadata=_metadata(request))

        return _provider(plan, get_range=get_range)

    acquisition_scope = acquisition.required_scope(root=root, plan=plan)
    acquisition_receipt = _approved(
        root, operation=acquisition.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        full_scope=acquisition_scope, plan_id=str(plan["plan_id"]),
        plan_sha256=sha256_file(root / acquisition.PLAN_PATH),
    )

    def metadata_loader(path: Path) -> SimpleNamespace:
        relative = path.relative_to(path.parents[4]).as_posix()
        request = next(item for item in ledger if item.snapshot_relative_path == relative)
        return _metadata(request)

    result = acquisition.execute_authorized_acquisition(
        root=root, authorization=acquisition_receipt, provider_factory=factory,
        disk_usage=lambda _path: SimpleNamespace(free=50_000_000_000),
        environment_check=lambda _root: None, metadata_loader=metadata_loader,
        sleeper=lambda _seconds: None,
    )
    resumed = acquisition.execute_authorized_acquisition(
        root=root, authorization=acquisition_receipt, provider_factory=factory,
        disk_usage=lambda _path: SimpleNamespace(free=50_000_000_000),
        environment_check=lambda _root: None, metadata_loader=metadata_loader,
        sleeper=lambda _seconds: None,
    )
    assert resumed == result and downloads["count"] == 192
    staging_snapshot = root / str(result["attempt_path"]) / "snapshot"
    extra = staging_snapshot / "extra"
    extra.write_bytes(b"x")
    with pytest.raises(IntegrityError, match="exactly 192 pairs"):
        publication.certify_staged_snapshot(
            root=root, record_id=str(result["record_id"]),
            metadata_loader=metadata_loader,
        )
    extra.unlink()
    preparation = publication.certify_staged_snapshot(
        root=root, record_id=str(result["record_id"]),
        metadata_loader=metadata_loader,
    )
    assert preparation["dbn_file_count"] == preparation["sidecar_file_count"] == 192
    assert preparation["dbn_rows_decoded"] == 0
    snapshot_id = str(preparation["snapshot_id"])
    staging = root / str(preparation["staging_snapshot_path"])
    destination = root / str(preparation["vault_snapshot_path"])

    def publication_receipt() -> OperationReceipt:
        scope = publication.required_scope(root=root, preparation=preparation)
        preparation_path = root / publication.PREPARATION_ROOT / f"{snapshot_id}.json"
        return _approved(
            root, operation=publication.OPERATION,
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            full_scope=scope, plan_id=snapshot_id,
            plan_sha256=sha256_file(preparation_path),
        )

    destination.mkdir(parents=True)
    with pytest.raises(IntegrityError, match="already exists"):
        publication.publish_authorized_snapshot(
            root=root, record_id=str(result["record_id"]),
            authorization=publication_receipt(), environment_check=lambda _root: None,
            metadata_loader=metadata_loader,
        )
    destination.rmdir()

    original_verify = publication._verify_published
    monkeypatch.setattr(
        publication, "_verify_published",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(IntegrityError("synthetic post-move failure")),
    )
    with pytest.raises(IntegrityError, match="synthetic post-move failure"):
        publication.publish_authorized_snapshot(
            root=root, record_id=str(result["record_id"]),
            authorization=publication_receipt(), environment_check=lambda _root: None,
            metadata_loader=metadata_loader,
        )
    assert staging.is_dir() and not destination.exists()

    monkeypatch.setattr(publication, "_verify_published", original_verify)
    published = publication.publish_authorized_snapshot(
        root=root, record_id=str(result["record_id"]),
        authorization=publication_receipt(), environment_check=lambda _root: None,
        metadata_loader=metadata_loader,
    )
    assert published["state"] == publication.TERMINAL_STATE
    assert destination.is_dir() and not staging.exists()
    assert len(list(destination.rglob("*.dbn.zst"))) == 192
    assert len(list(destination.rglob("*.manifest.json"))) == 192
    assert acquisition.protected_active_state(root) == preparation["protected_active_baseline"]
    assert (root / str(preparation["registry_path"])).is_file()


def test_custody_rejects_extra_files_without_decoding_rows(tmp_path: Path) -> None:
    source = Path(acquisition.__file__).read_text(encoding="utf-8")
    assert "for record in" not in source
    assert "dbn_rows_decoded\": 0" in source
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    (snapshot / "extra").write_bytes(b"x")
    assert publication._inventory_tree(snapshot) == ["extra"]


def test_publication_rejects_cross_volume_move(monkeypatch: pytest.MonkeyPatch) -> None:
    devices = iter((1, 2))
    monkeypatch.setattr(
        publication, "io_path",
        lambda _path: SimpleNamespace(stat=lambda: SimpleNamespace(st_dev=next(devices))),
    )
    with pytest.raises(IntegrityError, match="requires one volume"):
        publication._require_same_volume(Path("source"), Path("destination"))
