from __future__ import annotations

import inspect
import json
import runpy
import shutil
import sys
import threading
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild import micro_alpha_acquisition_v24 as acquisition
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
HEAD = "d" * 40
COPY_PATHS = (
    "configs/apex_micro_tier01_phase1a_acquisition_plan_v21.json",
    "configs/apex_micro_tier01_phase1a_acquisition_plan_v22.json",
    "configs/apex_micro_tier01_phase1a_acquisition_plan_v23.json",
    "configs/dependency_lock_receipt.json",
    "scripts/prepare_apex_micro_phase1a_acquisition_v24.py",
    "scripts/prepare_apex_micro_phase1a_acquisition_v22_supersession.py",
    "scripts/prepare_apex_micro_phase1a_acquisition_v23_supersession.py",
    "scripts/audit_standard_data_topology_source_safe.py",
    "scripts/prepare_safe_cleanup_candidate_census_v6.py",
    "scripts/prepare_safe_cleanup_candidate_census_v7.py",
    "scripts/prepare_safe_cleanup_candidate_census_v8.py",
    "scripts/prepare_safe_cleanup_candidate_census_v9.py",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/live_cockpit/databento_auth.py",
    "src/futures_rebuild/micro_alpha_acquisition.py",
    "src/futures_rebuild/micro_alpha_acquisition_v21.py",
    "src/futures_rebuild/micro_alpha_acquisition_v24.py",
    "src/futures_rebuild/micro_alpha_pipeline.py",
    "src/futures_rebuild/research_gateway_policy.py",
    "src/futures_rebuild/runtime_environment.py",
    "state/authorization_uses/"
    "5c04fecd51692b216f468ccf1eecbf72e918d06e675b2a4287a03e4c684ac282.json",
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v21/audit.json",
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_v21_failure/report.json",
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v22/audit.json",
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_v22_supersession/report.json",
    "state/unpublished_evidence/safe_cleanup_candidate_census_v7/census.json",
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_plan_v23/audit.json",
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_v23_supersession/report.json",
    "state/unpublished_evidence/safe_cleanup_candidate_census_v8/census.json",
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
        str(script_root / "prepare_apex_micro_phase1a_acquisition_v24.py"),
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
        fail_download: bool = False,
        emit_warning: bool = False,
        secret: str = "provider-secret-must-not-appear",
    ) -> None:
        self.cost = cost
        self.fail_download = fail_download
        self.emit_warning = emit_warning
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
                path.write_bytes(json.dumps(kwargs, sort_keys=True).encode("utf-8"))
                if self.emit_warning:
                    warnings.warn(self.secret, UserWarning, stacklevel=1)
                if self.fail_download and call_number == 1:
                    raise RuntimeError(self.secret)
                return object()
            finally:
                with self._lock:
                    self.active_downloads -= 1

        return acquisition.DownloadProviderApis(get_cost, get_range)


def _run(
    root: Path,
    fleet: FakeFleet,
    **overrides: object,
) -> dict[str, object]:
    arguments: dict[str, object] = {
        "root": root,
        "authorization": _receipt(root),
        "provider_factory": fleet.factory,
        "credential_source": acquisition.CREDENTIAL_SOURCE,
        "disk_usage": lambda _path: SimpleNamespace(free=10**12),
        "environment_check": lambda _root: "synthetic-lock",
        "mark_immutable": lambda _path: None,
    }
    arguments.update(overrides)
    return acquisition.execute_authorized_acquisition(**arguments)


def test_v24_plan_is_exact_annual_non_resuming_successor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    plan = acquisition.load_acquisition_plan(root=root)
    predecessor = json.loads(
        (root / acquisition.V21_PLAN_PATH).read_text(encoding="utf-8")
    )
    assert plan["markets"] == ["MES", "MCL", "MGC", "M6E"]
    assert plan["schemas"] == [
        "definition",
        "status",
        "statistics",
        "ohlcv-1m",
        "ohlcv-1s",
    ]
    assert plan["requests"] == predecessor["requests"]
    assert len(plan["requests"]) == 160
    assert plan["limits"]["maximum_runtime_seconds"] == 43_200
    assert plan["limits"]["maximum_per_download_seconds"] == 900
    assert plan["limits"]["maximum_provider_calls"] == 320
    assert plan["limits"]["maximum_parallel_downloads"] == 2
    assert plan["limits"]["maximum_provider_clients"] == 3
    assert plan["limits"]["maximum_attempts"] == 1
    assert plan["limits"]["maximum_retries"] == 0
    assert plan["predecessor_failure_evidence"]["staging_reuse"] is False
    assert plan["superseded_v22_preparation"]["execute_as_current"] is False
    assert plan["superseded_v22_preparation"][
        "provider_execution_performed"
    ] is False
    assert plan["superseded_v23_preparation"]["execute_as_current"] is False
    assert plan["superseded_v23_preparation"][
        "provider_execution_performed"
    ] is False
    assert plan["custody"]["successor_redownloads_every_request"] is True
    assert acquisition.STAGING_ROOT.as_posix().endswith("apex_micro_tier01_v24")
    for item in plan["requests"]:
        assert f"/{item['year']}/" in item["dbn_destination"]
        assert item["sidecar_destination"] == (
            item["dbn_destination"] + ".manifest.json"
        )
        query = item["query"]
        if item["schema"] == "definition":
            assert query["stype_in"] == "parent"
            assert query["symbols"] == [f"{item['market']}.FUT"]
        else:
            assert query["stype_in"] == "continuous"
            assert query["symbols"] == [f"{item['market']}.v.0"]


def test_v24_audit_is_exactly_reconstructible_across_free_space_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    topology_core = {
        "state": "PASS_SOURCE_SAFE_PROVENANCE_METADATA_ONLY",
        "payload_safety": {"historical_rows_read": 0},
    }
    topology = {**topology_core, "report_id": sha256_json(topology_core)}
    cleanup_core = {
        "state": "PREPARED_NO_MUTATION_SEPARATE_EXACT_CLEANUP_APPROVAL_REQUIRED",
        "committed_head": HEAD,
        "candidate_count": 0,
        "cleanup_execution": {"performed": False},
        "payload_safety": {"historical_rows_read": False},
    }
    cleanup = {**cleanup_core, "census_id": sha256_json(cleanup_core)}
    for relative, value in (
        (acquisition.STANDARD_TOPOLOGY_PATH, topology),
        (acquisition.CLEANUP_CENSUS_PATH, cleanup),
    ):
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(canonical_bytes(value) + b"\n")
    first = acquisition.build_plan_audit(
        root=root,
        fresh_standard_topology_report=topology,
        fresh_cleanup_census=cleanup,
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
    )
    second = acquisition.build_plan_audit(
        root=root,
        fresh_standard_topology_report=topology,
        fresh_cleanup_census=cleanup,
        disk_usage=lambda _path: SimpleNamespace(free=10**12 - 999_999),
    )
    assert first == second
    assert first["capacity"]["observed_free_disk_bytes_recorded"] is False
    assert "observed_free_disk_bytes" not in first["capacity"]
    assert first["capacity"][
        "live_capacity_recheck_required_immediately_before_execution"
    ] is True
    with pytest.raises(UnauthorizedOperation, match="insufficient disk"):
        acquisition.build_plan_audit(
            root=root,
            fresh_standard_topology_report=topology,
            fresh_cleanup_census=cleanup,
            disk_usage=lambda _path: SimpleNamespace(free=0),
        )


def test_v24_execution_rechecks_live_disk_before_consuming_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    fleet = FakeFleet()
    with pytest.raises(UnauthorizedOperation, match="insufficient disk capacity"):
        _run(root, fleet, disk_usage=lambda _path: SimpleNamespace(free=0))
    assert fleet.factory_calls == 0
    assert not (root / acquisition.STAGING_ROOT).exists()


def test_success_is_bounded_and_warning_messages_are_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    fleet = FakeFleet(emit_warning=True)
    terminal = _run(root, fleet)
    assert terminal["state"] == "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY", {
        "failure_stage": terminal.get("failure_stage"),
        "failure_substage": terminal.get("failure_substage"),
        "exception_type": terminal.get("exception_type"),
        "provider_call_counts": terminal.get("provider_call_counts"),
        "download_worker_failures": terminal.get("download_worker_failures"),
        "finalization_rollback_failures": terminal.get(
            "finalization_rollback_failures"
        ),
        "finalization_attempt_count": len(terminal.get("finalization_attempts", [])),
        "staged_complete_pair_count": len(terminal.get("staged_complete_pairs", [])),
    }
    assert terminal["provider_call_counts"] == {"get_cost": 160, "get_range": 160}
    assert fleet.factory_calls == 3
    assert fleet.peak_downloads == 2
    assert terminal["accepted_dbn_count"] == 160
    assert terminal["accepted_sidecar_count"] == 160
    assert terminal["provider_warning_count"] == 160
    assert terminal["staging_cleanup_failures"] == []
    serialized = json.dumps(terminal, sort_keys=True)
    assert fleet.secret not in serialized
    assert set(
        category
        for item in terminal["accepted_files"]
        for category in item["provider_warning_categories"]
    ) == {"UserWarning"}
    verified = acquisition.verify_completed_acquisition(
        root=root,
        terminal_path=(
            acquisition.STAGING_ROOT / _receipt_id_from_terminal(terminal) / "terminal.json"
        ),
    )
    assert verified["status"] == "PASS_INACTIVE_CUSTODY_NO_ROW_DECODE"


def _receipt_id_from_terminal(terminal: dict[str, object]) -> str:
    return str(terminal["authorization_receipt_id"])[:16]


def test_provider_failure_is_inactive_and_secret_free(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _copy_root(tmp_path, monkeypatch)
    fleet = FakeFleet(fail_download=True)
    terminal = _run(root, fleet)
    assert terminal["state"] == "FAILURE_INACTIVE_EVIDENCE_PRESERVED"
    assert terminal["accepted_dbn_count"] == terminal["accepted_sidecar_count"] == 0
    assert fleet.secret not in json.dumps(terminal, sort_keys=True)
    assert terminal["automatic_retries"] == 0
    plan = acquisition.load_acquisition_plan(root=root)
    assert not any(
        (root / item[key]).exists()
        for item in plan["requests"]
        for key in ("dbn_destination", "sidecar_destination")
    )


def test_mid_pair_finalization_failure_rolls_back_final_link(
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

    terminal = _run(root, fleet, link_file=fail_second_link)
    assert terminal["state"] == "FAILURE_INACTIVE_EVIDENCE_PRESERVED"
    attempt = terminal["finalization_attempts"][0]
    assert attempt["dbn_link_created"] is True
    assert attempt["sidecar_link_created"] is False
    assert attempt["rollback_dbn_removed"] is True
    assert terminal["finalization_rollback_failures"] == []
    assert not (root / attempt["dbn_destination"]).exists()
    assert not (root / attempt["sidecar_destination"]).exists()
    assert (root / terminal["staged_complete_pairs"][0]["staging_dbn"]).is_file()


def test_nonzero_cost_and_destination_collision_fail_before_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cost_root = _copy_root(tmp_path / "cost", monkeypatch)
    cost_fleet = FakeFleet(cost="0.01")
    cost_terminal = _run(cost_root, cost_fleet)
    assert cost_terminal["state"] == "FAILURE_INACTIVE_EVIDENCE_PRESERVED"
    assert cost_fleet.download_calls == 0

    collision_root = _copy_root(tmp_path / "collision", monkeypatch)
    plan = acquisition.load_acquisition_plan(root=collision_root)
    collision = collision_root / plan["requests"][0]["dbn_destination"]
    collision.parent.mkdir(parents=True, exist_ok=True)
    collision.write_bytes(b"existing")
    collision_fleet = FakeFleet()
    with pytest.raises(IntegrityError, match="already exists"):
        _run(collision_root, collision_fleet)
    assert collision_fleet.factory_calls == 0


def test_v24_operation_is_allowlisted_but_executor_has_no_decode_surface() -> None:
    assert acquisition.OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS
    source = inspect.getsource(acquisition)
    assert "read_dbn" not in source
    assert "DBNStore" not in source
    assert "to_df" not in source
    assert "automatic_retries\": 0" in source
    assert "provider_warning_messages_recorded\": False" in source
    assert "V21_AUTHORIZATION_RECEIPT_ID" in source


def test_v21_failure_report_is_self_hashed_and_accepts_no_source() -> None:
    report_path = ROOT / acquisition.V21_FAILURE_REPORT_PATH
    report = json.loads(report_path.read_text(encoding="utf-8"))
    core = dict(report)
    report_id = core.pop("report_id")
    assert report_id == acquisition.V21_FAILURE_REPORT_ID == sha256_json(core)
    assert sha256_file(report_path) == acquisition.V21_FAILURE_REPORT_SHA256
    assert report["verified_complete_staging_pairs"] == 36
    assert report["accepted_dbn_count"] == 0
    assert report["accepted_sidecar_count"] == 0
    assert report["final_destination_count"] == 0
    assert report["predecessor_staging_reusable_by_successor"] is False
