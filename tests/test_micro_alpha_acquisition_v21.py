from __future__ import annotations

import inspect
import json
import runpy
import shutil
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild import micro_alpha_acquisition_v21 as acquisition
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.research_gateway_policy import PREPARATORY_REAL_HISTORY_OPERATIONS


pytestmark = [pytest.mark.current, pytest.mark.high_risk]
ROOT = Path(__file__).resolve().parents[1]
HEAD = "b" * 40
COPY_PATHS = (
    "configs/apex_micro_product_reference_requirements.json",
    "configs/apex_micro_tier01_databento_metadata_preflight_v21.json",
    "configs/dependency_lock_receipt.json",
    "scripts/prepare_apex_micro_phase1a_acquisition_v21.py",
    "scripts/prepare_safe_cleanup_candidate_census_v6.py",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/live_cockpit/databento_auth.py",
    "src/futures_rebuild/micro_alpha_acquisition.py",
    "src/futures_rebuild/micro_alpha_acquisition_v21.py",
    "src/futures_rebuild/micro_alpha_pipeline.py",
    "src/futures_rebuild/research_gateway_policy.py",
    "src/futures_rebuild/runtime_environment.py",
    "state/authorization_uses/bf720c94e7307379dbbf4bce5e482c5e3f452d2718009d1d26422fbd6256cc40.json",
    "state/unpublished_evidence/apex_micro_metadata_preflight_v21/report.json",
    "state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json",
)


def test_prepare_script_supports_direct_path_execution_imports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    script_root = ROOT / "scripts"
    root_text = str(ROOT.resolve()).casefold()
    direct_path = [str(script_root)] + [
        item
        for item in sys.path
        if item and str(Path(item).resolve()).casefold() != root_text
    ]
    monkeypatch.setattr(sys, "path", direct_path)

    namespace = runpy.run_path(
        str(script_root / "prepare_apex_micro_phase1a_acquisition_v21.py"),
        run_name="_direct_script_import_probe",
    )

    assert namespace["CLEANUP_CENSUS_PATH"] == acquisition.CLEANUP_CENSUS_PATH
    assert callable(namespace["build_report"])


def _copy_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "active"
    for relative in COPY_PATHS:
        source = ROOT / relative
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    monkeypatch.setattr(acquisition, "_git_head", lambda _root: HEAD)
    cleanup_core = {
        "schema_version": "safe_cleanup_candidate_census/6.0.0",
        "state": "PREPARED_NO_MUTATION_SEPARATE_EXACT_CLEANUP_APPROVAL_REQUIRED",
        "committed_head": HEAD,
        "candidate_count": 0,
        "candidates": [],
        "cleanup_execution": {
            "performed": False,
            "data_changed": False,
        },
        "payload_safety": {
            "historical_rows_read": False,
            "year_2025_or_2026_payload_opened": False,
        },
    }
    cleanup_path = root / acquisition.CLEANUP_CENSUS_PATH
    cleanup_path.parent.mkdir(parents=True, exist_ok=True)
    cleanup_path.write_bytes(
        canonical_bytes(
            {**cleanup_core, "census_id": sha256_json(cleanup_core)}
        )
        + b"\n"
    )
    acquisition.write_acquisition_plan_create_only(root=root, committed_head=HEAD)
    return root


def _receipt(root: Path) -> OperationReceipt:
    plan = json.loads((root / acquisition.PLAN_PATH).read_text(encoding="utf-8"))
    full = acquisition.required_scope(root=root, plan=plan)
    scope = {
        key: value
        for key, value in full.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    plan_sha = sha256_file(root / acquisition.PLAN_PATH)
    return OperationReceipt.issue_user_approved(
        RepoBoundary(root),
        operation=acquisition.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=acquisition.OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha,
        approval_line=(
            f"APPROVE {acquisition.OPERATION} PLAN {plan['plan_id']} "
            f"SHA256 {plan_sha}"
        ),
    )


class FakeFleet:
    def __init__(
        self,
        *,
        cost: object = 0,
        empty: bool = False,
        fail_download: bool = False,
        secret: str = "provider-secret-must-not-appear",
    ) -> None:
        self.cost = cost
        self.empty = empty
        self.fail_download = fail_download
        self.secret = secret
        self.factory_calls = 0
        self.cost_calls = 0
        self.download_calls = 0
        self.active_downloads = 0
        self.peak_downloads = 0
        self._lock = threading.Lock()
        self._barrier = threading.Barrier(2)

    def factory(self) -> acquisition.DownloadProviderApis:
        with self._lock:
            self.factory_calls += 1

        def get_cost(**_kwargs: object) -> object:
            with self._lock:
                self.cost_calls += 1
            return self.cost

        def get_range(**kwargs: object) -> object:
            with self._lock:
                self.download_calls += 1
                call_number = self.download_calls
                self.active_downloads += 1
                self.peak_downloads = max(
                    self.peak_downloads, self.active_downloads
                )
            try:
                if call_number <= 2:
                    self._barrier.wait(timeout=5)
                path = Path(str(kwargs.pop("path")))
                path.write_bytes(b"" if self.empty else canonical_bytes(kwargs))
                if self.fail_download and call_number == 1:
                    raise RuntimeError(self.secret)
                return object()
            finally:
                with self._lock:
                    self.active_downloads -= 1

        return acquisition.DownloadProviderApis(get_cost, get_range)


def _run(root: Path, fleet: FakeFleet) -> dict[str, object]:
    return acquisition.execute_authorized_acquisition(
        root=root,
        authorization=_receipt(root),
        provider_factory=fleet.factory,
        credential_source=acquisition.CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
        environment_check=lambda _root: "synthetic-lock",
        mark_immutable=lambda _path: None,
    )


def test_live_v21_plan_is_preserved_after_successor_implementation_drift() -> None:
    plan_path = ROOT / acquisition.PLAN_PATH
    before = plan_path.read_bytes()
    before_mtime = plan_path.stat().st_mtime_ns
    report = json.loads(
        (ROOT / acquisition.PREFLIGHT_REPORT_PATH).read_text(encoding="utf-8")
    )
    core = dict(report)
    report_id = core.pop("report_id")
    assert report_id == acquisition.PREFLIGHT_REPORT_ID == sha256_json(core)
    stored = json.loads(before)
    stored_core = dict(stored)
    stored_id = stored_core.pop("plan_id")
    assert stored_id == (
        "a21652882790dfe2a9d56ebce9edab7b223e5d29d49af7edcae2774e3517899b"
    ) == sha256_json(stored_core)
    reconstructed_under_successor_code = acquisition.build_acquisition_plan(
        root=ROOT,
        committed_head=acquisition._git_head(ROOT),
        require_destination_absence=False,
    )
    assert len(stored["requests"]) == 160
    assert stored["limits"]["maximum_total_bytes"] == 11_350_292_377
    assert stored["limits"]["required_free_disk_bytes"] == 12_424_034_201
    assert reconstructed_under_successor_code != stored
    assert plan_path.read_bytes() == before
    assert plan_path.stat().st_mtime_ns == before_mtime


def test_plan_freezes_exact_scope_annual_paths_prelaunch_and_wire_format(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    plan = acquisition.load_acquisition_plan(root=root)
    assert plan["markets"] == ["MES", "MCL", "MGC", "M6E"]
    assert plan["schemas"] == [
        "definition",
        "status",
        "statistics",
        "ohlcv-1m",
        "ohlcv-1s",
    ]
    assert len(plan["requests"]) == 160
    assert plan["limits"] == {
        "exact_request_count": 160,
        "maximum_provider_calls": 320,
        "maximum_dbn_files": 160,
        "maximum_sidecars": 160,
        "maximum_total_bytes": 11_350_292_377,
        "required_free_disk_bytes": 12_424_034_201,
        "maximum_external_cost_usd": "0",
        "maximum_runtime_seconds": 7200,
        "maximum_per_download_seconds": 900,
        "maximum_attempts": 1,
        "maximum_retries": 0,
        "maximum_parallel_downloads": 2,
        "maximum_provider_clients": 3,
    }
    assert len(plan["prelaunch_coverage"]) == 6
    assert {item["market"] for item in plan["prelaunch_coverage"]} == {
        "MES",
        "MCL",
    }
    for item in plan["requests"]:
        dbn = item["dbn_destination"]
        assert dbn.startswith("data/dbn/") and "/micro/" not in dbn
        assert item["sidecar_destination"] == dbn + ".manifest.json"
        assert len(dbn.split("/")) == 6
        assert item["wire_format"] == {
            "encoding": "dbn",
            "compression": "zstd",
            "contract": "LOCKED_DATABENTO_GET_RANGE_ALWAYS_DBN_ZSTD",
        }
        assert item["fresh_exact_cost_requote_required_before_download"] is True
        assert item["query"]["schema"] not in {
            "trades",
            "bbo-1s",
            "mbp-1",
            "mbp-10",
        }


@pytest.mark.parametrize("artifact", ["plan", "report", "authorization"])
def test_sealed_v21_evidence_drift_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, artifact: str
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    target = {
        "plan": root / acquisition.PREFLIGHT_PLAN_PATH,
        "report": root / acquisition.PREFLIGHT_REPORT_PATH,
        "authorization": root / acquisition.PREFLIGHT_AUTHORIZATION_PATH,
    }[artifact]
    target.write_bytes(target.read_bytes() + b" ")
    with pytest.raises(IntegrityError, match="evidence bytes"):
        acquisition.build_acquisition_plan(root=root, committed_head=HEAD)


@pytest.mark.parametrize(
    "mutation",
    ["schema", "symbology", "destination", "prelaunch", "wire", "head"],
)
def test_frozen_plan_adversarial_mutations_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    plan_path = root / acquisition.PLAN_PATH
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    if mutation == "schema":
        plan["requests"][0]["query"]["schema"] = "trades"
    elif mutation == "symbology":
        plan["requests"][0]["query"]["symbols"] = ["ZN.v.0"]
    elif mutation == "destination":
        plan["requests"][0]["dbn_destination"] += ".collision"
    elif mutation == "prelaunch":
        plan["prelaunch_coverage"] = []
    elif mutation == "wire":
        plan["requests"][0]["wire_format"]["compression"] = "none"
    else:
        plan["committed_implementation_head"] = "c" * 40
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    plan["plan_id"] = sha256_json(core)
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    with pytest.raises((IntegrityError, UnauthorizedOperation)):
        acquisition.load_acquisition_plan(root=root)


def test_successful_two_client_mechanics_create_exact_inactive_pairs_without_decode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    fleet = FakeFleet()
    terminal = _run(root, fleet)
    assert terminal["state"] == "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY"
    assert terminal["provider_call_counts"] == {"get_cost": 160, "get_range": 160}
    assert terminal["provider_client_count"] == fleet.factory_calls == 3
    assert terminal["download_worker_count"] == 2
    assert fleet.peak_downloads == 2
    assert terminal["accepted_dbn_count"] == terminal["accepted_sidecar_count"] == 160
    assert terminal["dbn_rows_decoded"] == 0
    assert terminal["payloads_opened_for_row_access"] == 0
    for item in terminal["accepted_files"]:
        dbn = root / item["dbn_destination"]
        sidecar_path = root / item["sidecar_destination"]
        assert dbn.stat().st_size == item["byte_count"]
        assert sha256_file(dbn) == item["sha256"]
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        assert sidecar["exact_authorized_query"]["encoding"] == "dbn"
        assert sidecar["exact_authorized_query"]["compression"] == "zstd"
        assert sidecar["dbn_rows_decoded"] == 0
        assert sidecar["payload_opened_for_row_access"] is False
    terminal_path = next(
        (root / acquisition.STAGING_ROOT).glob("*/terminal.json")
    )
    verification = acquisition.verify_completed_acquisition(
        root=root, terminal_path=terminal_path
    )
    assert verification["status"] == "PASS_INACTIVE_CUSTODY_NO_ROW_DECODE"
    assert verification["dbn_count"] == verification["sidecar_count"] == 160


def test_first_parallel_failure_stops_new_work_and_sanitizes_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    fleet = FakeFleet(fail_download=True)
    terminal = _run(root, fleet)
    assert terminal["state"] == "FAILURE_INACTIVE_EVIDENCE_PRESERVED"
    assert terminal["automatic_retries"] == 0
    assert terminal["provider_call_counts"]["get_range"] == 2
    assert fleet.download_calls == 2
    assert fleet.secret not in json.dumps(terminal)
    assert terminal["accepted_dbn_count"] == terminal["accepted_sidecar_count"] == 0
    assert terminal["staging_file_census"]
    assert not list((root / "data/dbn").rglob("*.dbn.zst"))


@pytest.mark.parametrize("mode", ["nonzero", "empty"])
def test_nonzero_cost_and_empty_download_fail_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    fleet = FakeFleet(cost=1 if mode == "nonzero" else 0, empty=mode == "empty")
    terminal = _run(root, fleet)
    assert terminal["state"] == "FAILURE_INACTIVE_EVIDENCE_PRESERVED"
    assert terminal["automatic_retries"] == 0
    if mode == "nonzero":
        assert fleet.cost_calls == 1 and fleet.download_calls == 0
    else:
        assert fleet.cost_calls == 160
        assert 1 <= fleet.download_calls <= 2
        assert list((root / acquisition.STAGING_ROOT).rglob("*.partial"))


def test_collision_and_disk_gates_precede_provider_and_authorization_use(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path / "collision", monkeypatch)
    plan = acquisition.load_acquisition_plan(root=root)
    collision = root / plan["requests"][0]["dbn_destination"]
    collision.parent.mkdir(parents=True, exist_ok=True)
    collision.write_bytes(b"existing")
    fleet = FakeFleet()
    with pytest.raises(IntegrityError, match="already exists"):
        _run(root, fleet)
    assert fleet.factory_calls == fleet.cost_calls == fleet.download_calls == 0

    disk_root = _copy_root(tmp_path / "disk", monkeypatch)
    disk_fleet = FakeFleet()
    with pytest.raises(UnauthorizedOperation, match="disk"):
        acquisition.execute_authorized_acquisition(
            root=disk_root,
            authorization=_receipt(disk_root),
            provider_factory=disk_fleet.factory,
            credential_source=acquisition.CREDENTIAL_SOURCE,
            disk_usage=lambda _path: SimpleNamespace(free=0),
            environment_check=lambda _root: "synthetic-lock",
            mark_immutable=lambda _path: None,
        )
    assert disk_fleet.factory_calls == 0


def test_mid_pair_finalization_failure_is_preserved_exactly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    fleet = FakeFleet()
    link_calls = 0

    def fail_second_link(source: Path, destination: Path) -> None:
        nonlocal link_calls
        link_calls += 1
        if link_calls == 2:
            raise OSError("synthetic-sidecar-link-failure")
        acquisition.os.link(source, destination)

    terminal = acquisition.execute_authorized_acquisition(
        root=root,
        authorization=_receipt(root),
        provider_factory=fleet.factory,
        credential_source=acquisition.CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
        environment_check=lambda _root: "synthetic-lock",
        mark_immutable=lambda _path: None,
        link_file=fail_second_link,
    )
    assert terminal["state"] == "FAILURE_INACTIVE_EVIDENCE_PRESERVED"
    assert terminal["accepted_dbn_count"] == terminal["accepted_sidecar_count"] == 0
    attempt = terminal["finalization_attempts"][0]
    assert attempt["dbn_link_created"] is True
    assert attempt["sidecar_link_created"] is False
    assert attempt["staging_sources_removed"] is False
    assert (root / attempt["dbn_destination"]).is_file()
    assert not (root / attempt["sidecar_destination"]).exists()
    assert "synthetic-sidecar-link-failure" not in json.dumps(terminal)


def test_plan_audit_binds_fresh_standard_topology_and_capacity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    topology = json.loads(
        (root / acquisition.STANDARD_TOPOLOGY_PATH).read_text(encoding="utf-8")
    )
    cleanup = json.loads(
        (root / acquisition.CLEANUP_CENSUS_PATH).read_text(encoding="utf-8")
    )
    audit = acquisition.build_plan_audit(
        root=root,
        fresh_standard_topology_report=topology,
        fresh_cleanup_census=cleanup,
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
    )
    assert audit["state"] == "PASS_EXACT_DOWNLOAD_APPROVAL_PREPARATION_ONLY"
    assert audit["scope"]["exact_requests"] == 160
    assert audit["scope"]["destination_conflicts"] == 0
    assert audit["standard_topology"]["fresh_reconstruction_match"] is True
    assert audit["cleanup_governance"]["cleanup_performed"] is False
    assert audit["safety"]["historical_rows_read"] is False


def test_independent_verifier_rejects_sidecar_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    terminal = _run(root, FakeFleet())
    sidecar_path = root / terminal["accepted_files"][0]["sidecar_destination"]
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["exact_authorized_query"]["symbols"] = ["ZN.v.0"]
    sidecar_path.write_bytes(canonical_bytes(sidecar) + b"\n")
    terminal_path = next(
        (root / acquisition.STAGING_ROOT).glob("*/terminal.json")
    )
    with pytest.raises(IntegrityError, match="sidecar"):
        acquisition.verify_completed_acquisition(
            root=root, terminal_path=terminal_path
        )


def test_executor_has_no_decoder_catalog_publication_registration_or_retry_surface() -> None:
    source = inspect.getsource(acquisition.execute_authorized_acquisition)
    module_source = inspect.getsource(acquisition)
    assert "automatic_retries\": 0" in source
    assert "dbn_rows_decoded\": 0" in source
    assert "payloads_opened_for_row_access\": 0" in source
    assert "DBNStore.from_file" not in module_source
    assert "read_dbn" not in module_source
    assert "pandas" not in module_source
    assert "catalog.json" not in module_source
    assert "register_trial" not in module_source
    assert acquisition.OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS
