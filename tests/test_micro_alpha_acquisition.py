from __future__ import annotations

import json
import shutil
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

import futures_rebuild.micro_alpha_acquisition as acquisition
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.micro_alpha_acquisition import (
    CREDENTIAL_SOURCE,
    OPERATION,
    PLAN_PATH,
    DownloadProviderApis,
    execute_authorized_acquisition,
    required_scope,
    verify_completed_acquisition,
    write_acquisition_plan_create_only,
)
from futures_rebuild.micro_alpha_databento_preflight_v5 import (
    LOCAL_SUPERSESSION_PATH,
    PREDECESSOR_AUTHORIZATION_PATH as V4_AUTHORIZATION_PATH,
    PREDECESSOR_PLAN_PATH as V4_PLAN_PATH,
    PREDECESSOR_REPORT_PATH as V4_REPORT_PATH,
    SUPERSEDED_LOCAL_PLAN_PATH,
)
from futures_rebuild.micro_alpha_databento_preflight_v7 import (
    PLAN_PATH as PREFLIGHT_PLAN_PATH,
    REFERENCE_PATH,
    REPORT_PATH,
    SUPERSESSION_PATH,
    PREDECESSOR_AUTHORIZATION_PATH,
    PREDECESSOR_PLAN_PATH,
    PREDECESSOR_REPORT_PATH,
    MetadataProviderApis,
    build_plan as build_preflight_plan,
    execute_preflight,
    required_scope as preflight_scope,
)


pytestmark = [pytest.mark.current, pytest.mark.high_risk]
ROOT = Path(__file__).resolve().parents[1]
HEAD = "a" * 40
IMPLEMENTATION_PATHS = (
    "configs/dependency_lock_receipt.json",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/live_cockpit/databento_auth.py",
    "src/futures_rebuild/micro_alpha_databento_preflight.py",
    "src/futures_rebuild/micro_alpha_pipeline.py",
    "src/futures_rebuild/micro_alpha_databento_preflight_v5.py",
    "src/futures_rebuild/micro_alpha_databento_preflight_v6.py",
    "src/futures_rebuild/micro_alpha_databento_preflight_v7.py",
    "src/futures_rebuild/micro_alpha_acquisition.py",
    "src/futures_rebuild/alpha_research_architecture.py",
    "src/futures_rebuild/runtime_environment.py",
)


class _Metadata:
    def list_datasets(self, **_kwargs: object) -> object:
        return ["GLBX.MDP3"]

    def list_schemas(self, **_kwargs: object) -> object:
        return ["definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"]

    def get_dataset_range(self, **_kwargs: object) -> object:
        return {
            "start": "2010-01-01T00:00:00+00:00",
            "end": "2026-08-08T00:00:00+00:00",
            "schema": {
                schema: {
                    "start": "2010-01-01T00:00:00+00:00",
                    "end": "2026-08-08T00:00:00+00:00",
                }
                for schema in (
                    "definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"
                )
            },
        }

    def resolve(self, **kwargs: object) -> object:
        symbol = kwargs["symbols"][0]
        market = str(symbol).split(".")[0]
        effective = {"MES": "2019-05-06", "MCL": "2021-07-12", "MGC": "2010-10-03", "M6E": "2010-03-23"}[market]
        return {
            "result": {symbol: [{"d0": effective, "d1": kwargs["end_date"], "s": 1}]},
            "symbols": [symbol], "stype_in": kwargs["stype_in"],
            "stype_out": "instrument_id", "start_date": kwargs["start_date"],
            "end_date": kwargs["end_date"], "partial": [],
            "not_found": [], "message": "", "status": 0,
        }

    def get_cost(self, **_kwargs: object) -> object:
        return 0

    def get_billable_size(self, **_kwargs: object) -> object:
        return 100

    def capability(self) -> MetadataProviderApis:
        return MetadataProviderApis(
            self.list_datasets, self.list_schemas, self.get_dataset_range,
            self.resolve, self.get_cost, self.get_billable_size,
        )


def _external_receipt(
    *, root: Path, operation: str, plan_path: Path,
    scope_builder,
) -> OperationReceipt:
    plan = json.loads((root / plan_path).read_text(encoding="utf-8"))
    full = scope_builder(root=root, plan=plan)
    scope = {
        key: value for key, value in full.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    plan_sha = sha256_file(root / plan_path)
    return OperationReceipt.issue_user_approved(
        RepoBoundary(root), operation=operation,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope, approval_command=operation,
        approval_plan_id=str(plan["plan_id"]), approval_plan_sha256=plan_sha,
        approval_line=f"APPROVE {operation} PLAN {plan['plan_id']} SHA256 {plan_sha}",
    )


def _prepared_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "active"
    for relative in (
        *IMPLEMENTATION_PATHS,
        REFERENCE_PATH.as_posix(),
        SUPERSESSION_PATH.as_posix(),
        PREDECESSOR_PLAN_PATH.as_posix(),
        PREDECESSOR_REPORT_PATH.as_posix(),
        PREDECESSOR_AUTHORIZATION_PATH.as_posix(),
        V4_PLAN_PATH.as_posix(),
        V4_REPORT_PATH.as_posix(),
        V4_AUTHORIZATION_PATH.as_posix(),
        SUPERSEDED_LOCAL_PLAN_PATH.as_posix(),
        LOCAL_SUPERSESSION_PATH.as_posix(),
    ):
        source = ROOT / relative
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    preflight = build_preflight_plan(root=root)
    preflight_path = root / PREFLIGHT_PLAN_PATH
    preflight_path.parent.mkdir(parents=True, exist_ok=True)
    preflight_path.write_bytes(canonical_bytes(preflight) + b"\n")
    receipt = _external_receipt(
        root=root,
        operation="PREFLIGHT_APEX_MICRO_TIER01_DATABENTO_METADATA_ONCE",
        plan_path=PREFLIGHT_PLAN_PATH, scope_builder=preflight_scope,
    )
    report = execute_preflight(
        root=root, authorization=receipt, provider_factory=_Metadata().capability,
        credential_source=CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
        environment_check=lambda _root: "synthetic-lock",
    )
    assert report["state"] == "PASS_METADATA_ONLY"
    monkeypatch.setattr(acquisition, "_git_head", lambda _root: HEAD)
    write_acquisition_plan_create_only(root=root, committed_head=HEAD)
    return root


class FakeDownloadProvider:
    def __init__(self, *, cost: object = 0, empty: bool = False, fail_message: str | None = None) -> None:
        self.cost = cost
        self.empty = empty
        self.fail_message = fail_message
        self.cost_calls = 0
        self.download_calls = 0
        self.queries: list[dict[str, object]] = []

    def get_cost(self, **kwargs: object) -> object:
        self.cost_calls += 1
        self.queries.append(dict(kwargs))
        if self.fail_message is not None:
            message, self.fail_message = self.fail_message, None
            raise RuntimeError(message)
        return self.cost

    def get_range(self, **kwargs: object) -> object:
        self.download_calls += 1
        path = Path(str(kwargs.pop("path")))
        self.queries.append(dict(kwargs))
        path.write_bytes(b"" if self.empty else canonical_bytes(kwargs))
        return object()

    def capability(self) -> DownloadProviderApis:
        return DownloadProviderApis(self.get_cost, self.get_range)


class BoundedParallelDownloadProvider(FakeDownloadProvider):
    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._first_pair = threading.Barrier(2)
        self.active_downloads = 0
        self.peak_downloads = 0

    def get_range(self, **kwargs: object) -> object:
        with self._lock:
            self.download_calls += 1
            call_number = self.download_calls
            self.active_downloads += 1
            self.peak_downloads = max(self.peak_downloads, self.active_downloads)
        try:
            if call_number <= 2:
                self._first_pair.wait(timeout=5)
            path = Path(str(kwargs.pop("path")))
            with self._lock:
                self.queries.append(dict(kwargs))
            path.write_bytes(canonical_bytes(kwargs))
            return object()
        finally:
            with self._lock:
                self.active_downloads -= 1


class FailingParallelDownloadProvider(FakeDownloadProvider):
    def __init__(self, secret: str) -> None:
        super().__init__()
        self.secret = secret
        self._lock = threading.Lock()
        self._first_pair = threading.Barrier(2)

    def get_range(self, **kwargs: object) -> object:
        with self._lock:
            self.download_calls += 1
            call_number = self.download_calls
        path = Path(str(kwargs.pop("path")))
        if call_number <= 2:
            self._first_pair.wait(timeout=5)
        path.write_bytes(canonical_bytes(kwargs))
        if call_number == 1:
            raise RuntimeError(self.secret)
        return object()


def _acquisition_receipt(root: Path) -> OperationReceipt:
    return _external_receipt(
        root=root, operation=OPERATION, plan_path=PLAN_PATH,
        scope_builder=required_scope,
    )


def _run(root: Path, provider: FakeDownloadProvider) -> dict[str, object]:
    return execute_authorized_acquisition(
        root=root, authorization=_acquisition_receipt(root),
        provider_factory=provider.capability, credential_source=CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
        environment_check=lambda _root: "synthetic-lock",
        mark_immutable=lambda _path: None,
    )


def test_plan_freezes_exact_scope_paths_prelaunch_and_inactive_controls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _prepared_root(tmp_path, monkeypatch)
    plan = json.loads((root / PLAN_PATH).read_text(encoding="utf-8"))
    assert plan["markets"] == ["MES", "MCL", "MGC", "M6E"]
    assert plan["schemas"] == ["definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"]
    assert len(plan["requests"]) == 160
    assert plan["limits"]["maximum_dbn_files"] == 180
    assert plan["limits"]["maximum_sidecars"] == 180
    assert plan["limits"]["maximum_provider_calls"] == 320
    assert plan["limits"]["maximum_parallel_downloads"] == 2
    assert plan["limits"]["maximum_provider_clients"] == 3
    assert plan["file_partition"] == (
        "ONE_DBN_AND_ADJACENT_SIDECAR_PER_MARKET_SCHEMA_CALENDAR_YEAR"
    )
    assert plan["limits"]["maximum_external_cost_usd"] == "0"
    assert plan["limits"]["maximum_retries"] == 0
    assert plan["custody"]["inactive_staging_first"] is True
    assert plan["forbidden"]["dbn_row_decode"] is True
    assert {item["market"] for item in plan["prelaunch_coverage"]} == {"MES", "MCL"}
    for item in plan["requests"]:
        assert item["dbn_destination"].startswith("data/dbn/")
        assert item["sidecar_destination"] == item["dbn_destination"] + ".manifest.json"
        parts = item["dbn_destination"].split("/")
        assert len(parts) == 6
        assert parts[:2] == ["data", "dbn"]
        assert parts[2] in {"definition", "status", "statistics", "ohlcv_1m", "ohlcv_1s"}
        assert parts[3] in {"MES", "MCL", "MGC", "M6E"}
        assert parts[4] == str(item["year"])
        assert "/micro/" not in item["dbn_destination"]
        assert item["query"]["start"][:4] == parts[4]
        assert item["query"]["end"] <= f"{int(parts[4]) + 1:04d}-01-01"
        if item["query"]["schema"].startswith("ohlcv-"):
            assert "/ohlcv_" in item["dbn_destination"]
        assert item["query"]["schema"] not in {"trades", "bbo-1s", "mbp-1", "mbp-10"}


def test_successful_mechanics_create_exact_verified_pairs_without_decoding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _prepared_root(tmp_path, monkeypatch)
    provider = FakeDownloadProvider()
    terminal = _run(root, provider)
    assert terminal["state"] == "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY", (
        terminal.get("failure_stage"), terminal.get("exception_type"),
    )
    assert terminal["provider_call_counts"] == {"get_cost": 160, "get_range": 160}
    assert terminal["provider_client_count"] == 3
    assert terminal["download_worker_count"] == 2
    assert terminal["accepted_dbn_count"] == 160
    assert terminal["accepted_sidecar_count"] == 160
    assert terminal["dbn_rows_decoded"] == 0
    assert provider.cost_calls == 160
    assert provider.download_calls == 160
    for item in terminal["accepted_files"]:
        dbn = root / item["dbn_destination"]
        sidecar = root / item["sidecar_destination"]
        assert dbn.is_file() and sidecar.is_file()
        assert dbn.stat().st_size == item["byte_count"]
        assert sha256_file(dbn) == item["sha256"]
        manifest = json.loads(sidecar.read_text(encoding="utf-8"))
        assert manifest["exact_authorized_query"]["dataset"] == "GLBX.MDP3"
        assert manifest["dbn_rows_decoded"] == 0
        assert manifest["payload_opened_for_row_access"] is False
        assert manifest["catalog_activation"] is False
    assert not (root / "configs/active_micro_alpha_research_ladder.json").exists()
    assert not (root / "data/active/catalogs/apex_micro.json").exists()
    terminal_path = next(
        (root / "state/provider_acquisition_staging/apex_micro_tier01").glob("*/terminal.json")
    )
    verification = verify_completed_acquisition(root=root, terminal_path=terminal_path)
    assert verification["status"] == "PASS_INACTIVE_CUSTODY_NO_ROW_DECODE"
    assert verification["dbn_count"] == verification["sidecar_count"] == 160


def test_download_parallelism_is_bounded_to_two_isolated_workers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _prepared_root(tmp_path, monkeypatch)
    provider = BoundedParallelDownloadProvider()
    terminal = _run(root, provider)
    assert terminal["state"] == "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY"
    assert terminal["download_worker_count"] == 2
    assert terminal["provider_client_count"] == 3
    assert provider.peak_downloads == 2
    assert terminal["automatic_retries"] == 0


def test_parallel_failure_stops_new_work_and_preserves_sanitized_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _prepared_root(tmp_path, monkeypatch)
    secret = "credential-shaped-provider-message"
    provider = FailingParallelDownloadProvider(secret)
    terminal = _run(root, provider)
    assert terminal["state"] == "FAILURE_INACTIVE_EVIDENCE_PRESERVED"
    assert terminal["automatic_retries"] == 0
    assert terminal["provider_call_counts"]["get_range"] == 2
    assert provider.download_calls == 2
    assert terminal["download_worker_failures"][0]["exception_type"] == "RuntimeError"
    assert terminal["staging_file_census"]
    assert secret not in json.dumps(terminal)
    assert terminal["accepted_dbn_count"] == terminal["accepted_sidecar_count"] == 0
    assert not list((root / "data/dbn").rglob("*.dbn.zst")) if (root / "data/dbn").exists() else True


@pytest.mark.parametrize("mode", ["nonzero", "empty", "single_failure"])
def test_cost_partial_and_provider_failures_are_preserved_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str,
) -> None:
    root = _prepared_root(tmp_path, monkeypatch)
    secret = "credential-shaped-secret-that-must-not-be-recorded"
    provider = FakeDownloadProvider(
        cost=1 if mode == "nonzero" else 0,
        empty=mode == "empty",
        fail_message=secret if mode == "single_failure" else None,
    )
    terminal = _run(root, provider)
    assert terminal["state"] == "FAILURE_INACTIVE_EVIDENCE_PRESERVED"
    assert terminal["automatic_retries"] == 0
    assert secret not in json.dumps(terminal)
    if mode in {"nonzero", "single_failure"}:
        assert provider.cost_calls == 1
        assert provider.download_calls == 0
    if mode == "empty":
        assert provider.cost_calls == 160
        assert 1 <= provider.download_calls <= 2
        partials = list((root / "state/provider_acquisition_staging/apex_micro_tier01").rglob("*.partial"))
        assert partials
    assert not list((root / "data/dbn").rglob("*.dbn.zst")) if (root / "data/dbn").exists() else True


def test_collision_and_disk_gates_stop_before_provider_or_authorization_use(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _prepared_root(tmp_path / "collision", monkeypatch)
    plan = json.loads((root / PLAN_PATH).read_text(encoding="utf-8"))
    collision = root / plan["requests"][0]["dbn_destination"]
    collision.parent.mkdir(parents=True, exist_ok=True)
    collision.write_bytes(b"existing")
    provider = FakeDownloadProvider()
    with pytest.raises(IntegrityError, match="already exists"):
        _run(root, provider)
    assert provider.cost_calls == provider.download_calls == 0

    disk_root = _prepared_root(tmp_path / "disk", monkeypatch)
    disk_provider = FakeDownloadProvider()
    with pytest.raises(UnauthorizedOperation, match="disk"):
        execute_authorized_acquisition(
            root=disk_root, authorization=_acquisition_receipt(disk_root),
            provider_factory=disk_provider.capability, credential_source=CREDENTIAL_SOURCE,
            disk_usage=lambda _path: SimpleNamespace(free=0),
            environment_check=lambda _root: "synthetic-lock",
            mark_immutable=lambda _path: None,
        )
    assert disk_provider.cost_calls == disk_provider.download_calls == 0


def test_plan_validation_rejects_schema_symbology_path_prelaunch_and_reference_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _prepared_root(tmp_path, monkeypatch)
    original = json.loads((root / PLAN_PATH).read_text(encoding="utf-8"))
    for mutation in ("schema", "symbology", "destination", "prelaunch", "reference"):
        plan = json.loads(json.dumps(original))
        if mutation == "schema":
            plan["requests"][0]["query"]["schema"] = "trades"
        elif mutation == "symbology":
            plan["requests"][0]["query"]["symbols"] = ["ZN.v.0"]
        elif mutation == "destination":
            plan["requests"][0]["dbn_destination"] = plan["requests"][0][
                "dbn_destination"
            ].replace("ohlcv_1m", "ohlcv-1m").replace("definition", "definition_bad")
        elif mutation == "prelaunch":
            plan["prelaunch_coverage"] = []
        else:
            plan["product_reference_requirements_sha256"] = "0" * 64
        core = {key: value for key, value in plan.items() if key != "plan_id"}
        from futures_rebuild.canonical import sha256_json
        plan["plan_id"] = sha256_json(core)
        (root / PLAN_PATH).write_bytes(canonical_bytes(plan) + b"\n")
        with pytest.raises((IntegrityError, UnauthorizedOperation)):
            acquisition.load_acquisition_plan(root=root)
    (root / PLAN_PATH).write_bytes(canonical_bytes(original) + b"\n")


def test_independent_verifier_rejects_sidecar_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _prepared_root(tmp_path, monkeypatch)
    terminal = _run(root, FakeDownloadProvider())
    assert terminal["state"] == "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY"
    sidecar_path = root / terminal["accepted_files"][0]["sidecar_destination"]
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["exact_authorized_query"]["symbols"] = ["ZN.v.0"]
    sidecar_path.write_bytes(canonical_bytes(sidecar) + b"\n")
    terminal_path = next(
        (root / "state/provider_acquisition_staging/apex_micro_tier01").glob("*/terminal.json")
    )
    with pytest.raises(IntegrityError, match="sidecar"):
        verify_completed_acquisition(root=root, terminal_path=terminal_path)
