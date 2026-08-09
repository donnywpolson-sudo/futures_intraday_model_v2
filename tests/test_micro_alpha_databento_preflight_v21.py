from __future__ import annotations

import inspect
import json
import shutil
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from requests.exceptions import ReadTimeout

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
    _personal_approval_line,
)
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError
from futures_rebuild.micro_alpha_databento_preflight import (
    CREDENTIAL_SOURCE,
    MetadataProviderApis,
    OPERATION,
)
from futures_rebuild import micro_alpha_databento_preflight_v21 as v21


pytestmark = [pytest.mark.current, pytest.mark.high_risk]
ROOT = Path(__file__).resolve().parents[1]


def _copy_root(tmp_path: Path) -> Path:
    root = tmp_path / "active"
    plan = json.loads((ROOT / v21.PLAN_PATH).read_text(encoding="utf-8"))
    for relative in plan["plan_bindings"]:
        source = ROOT / relative
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    destination = root / v21.PLAN_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / v21.PLAN_PATH, destination)
    return root


def _receipt(root: Path) -> OperationReceipt:
    plan = json.loads((root / v21.PLAN_PATH).read_text(encoding="utf-8"))
    full = v21.required_scope(root=root, plan=plan)
    scope = {
        key: value
        for key, value in full.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    plan_sha = sha256_file(root / v21.PLAN_PATH)
    return OperationReceipt.issue_user_approved(
        RepoBoundary(root),
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha,
        approval_line=_personal_approval_line(
            OPERATION, str(plan["plan_id"]), plan_sha
        ),
    )


class FakeFleet:
    def __init__(
        self,
        *,
        cost: object = 0,
        size: object = 1000,
        fail_size_once: bool = False,
        delay_seconds: float = 0.001,
    ) -> None:
        self.cost = cost
        self.size = size
        self.fail_size_once = fail_size_once
        self.delay_seconds = delay_seconds
        self.lock = threading.Lock()
        self.factory_calls = 0
        self.active = 0
        self.peak_active = 0
        self.calls: list[tuple[int, str, dict[str, object], float]] = []
        self.size_failure_emitted = False

    def factory(self) -> MetadataProviderApis:
        with self.lock:
            self.factory_calls += 1
            client_id = self.factory_calls
        client = FakeClient(fleet=self, client_id=client_id)
        return client.capability()

    def invoke(
        self,
        *,
        client_id: int,
        operation: str,
        timeout: float,
        kwargs: dict[str, object],
    ) -> object:
        with self.lock:
            self.active += 1
            self.peak_active = max(self.peak_active, self.active)
            self.calls.append((client_id, operation, kwargs, timeout))
            fail_size = (
                operation == "get_billable_size"
                and self.fail_size_once
                and not self.size_failure_emitted
            )
            if fail_size:
                self.size_failure_emitted = True
        try:
            time.sleep(self.delay_seconds)
            if fail_size:
                raise ReadTimeout("synthetic timeout value must not be recorded")
            return self.cost if operation == "get_cost" else self.size
        finally:
            with self.lock:
                self.active -= 1


class FakeClient:
    TIMEOUT = 30.0

    def __init__(self, *, fleet: FakeFleet, client_id: int) -> None:
        self.fleet = fleet
        self.client_id = client_id

    def forbidden(self, **_kwargs: object) -> object:
        raise AssertionError("a cumulatively satisfied operation was re-queried")

    def get_cost(self, **kwargs: object) -> object:
        return self.fleet.invoke(
            client_id=self.client_id,
            operation="get_cost",
            timeout=float(self.TIMEOUT),
            kwargs=dict(kwargs),
        )

    def get_billable_size(self, **kwargs: object) -> object:
        return self.fleet.invoke(
            client_id=self.client_id,
            operation="get_billable_size",
            timeout=float(self.TIMEOUT),
            kwargs=dict(kwargs),
        )

    def capability(self) -> MetadataProviderApis:
        return MetadataProviderApis(
            self.forbidden,
            self.forbidden,
            self.forbidden,
            self.forbidden,
            self.get_cost,
            self.get_billable_size,
        )


def _run(
    root: Path,
    fleet: FakeFleet,
    *,
    free: int = 10**12,
) -> dict[str, object]:
    return v21.execute_preflight(
        root=root,
        authorization=_receipt(root),
        provider_factory=fleet.factory,
        credential_source=CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=free),
        environment_check=lambda _root: "synthetic-lock",
    )


def test_v21_plan_preserves_scope_and_binds_v20_timeout() -> None:
    plan = v21.validate_plan(
        json.loads((ROOT / v21.PLAN_PATH).read_text(encoding="utf-8")), root=ROOT
    )
    assert plan == v21.build_plan(root=ROOT)
    assert len(plan["requests"]) == 20
    assert {request["market"] for request in plan["requests"]} == {
        "MES",
        "MCL",
        "MGC",
        "M6E",
    }
    assert {request["schema"] for request in plan["requests"]} == {
        "definition",
        "status",
        "statistics",
        "ohlcv-1m",
        "ohlcv-1s",
    }
    assert plan["predecessor_execution"]["report_id"] == v21.PREDECESSOR_REPORT_ID
    assert plan["predecessor_execution"]["failure_code"] == "PROVIDER_TIMEOUT"
    assert plan["limits"]["exact_provider_call_ceiling"] == 180
    assert plan["limits"]["maximum_provider_clients"] == 6
    assert plan["limits"]["maximum_runtime_seconds"] == 300
    assert plan["limits"]["per_call_timeout_seconds"] == 90
    assert plan["provider_operations"] == {
        "list_datasets": 0,
        "list_schemas": 0,
        "get_dataset_range": 0,
        "resolve": 0,
        "get_cost_full_acquisition_range": 20,
        "get_billable_size_annual": 160,
        "timeseries_download": 0,
    }
    assert v21.load_predecessor_failure(root=ROOT)["provider_call_total"] == 68


def test_cost_dominance_keeps_exact_annual_download_requotes() -> None:
    plan = v21.load_plan(root=ROOT)
    rule = plan["cost_dominance"]
    assert rule["full_range_request_count"] == 20
    assert rule["full_range_zero_cost_required"] is True
    assert rule["nonnegative_full_range_zero_cost_dominates_annual_subsets"] is True
    assert rule["exact_annual_requote_required_immediately_before_download"] is True
    assert plan["annual_scope"]["exact_market_schema_requests"] == 160


def test_v21_bounded_parallel_metadata_mechanics_pass(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    fleet = FakeFleet(delay_seconds=0.003)
    report = _run(root, fleet)
    assert report["state"] == "PASS_METADATA_ONLY"
    assert report["provider_call_counts"] == {
        "get_billable_size": 160,
        "get_cost": 20,
    }
    assert report["provider_call_total"] == 180
    assert report["annual_market_schema_request_count"] == 160
    assert len(report["request_estimates"]) == 160
    assert len(report["full_range_zero_cost_proofs"]) == 20
    assert all(
        proof["estimated_cost_usd"] == "0"
        and proof["annual_subset_dominance"] is True
        for proof in report["full_range_zero_cost_proofs"]
    )
    assert all(
        estimate["exact_annual_requote_required_before_download"] is True
        and estimate["estimated_cost_usd"] == "0_FROM_FULL_RANGE_DOMINANCE"
        for estimate in report["request_estimates"]
    )
    assert fleet.factory_calls == 6
    assert 2 <= fleet.peak_active <= 6
    assert len({client_id for client_id, *_rest in fleet.calls}) == 6
    assert all(0 < timeout <= 90 for *_prefix, timeout in fleet.calls)
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0
    assert report["historical_rows_read"] is False
    assert report["dbn_files_created"] == 0


def test_nonzero_cost_stops_before_size_census_without_retry(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    fleet = FakeFleet(cost="0.01", delay_seconds=0.003)
    report = _run(root, fleet)
    operations = [operation for _client, operation, _kwargs, _timeout in fleet.calls]
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "UNEXPECTED_NONZERO_COST"
    assert 1 <= report["provider_call_total"] <= 6
    assert set(operations) == {"get_cost"}
    assert report["automatic_retries"] == 0
    assert report["provider_error_message_recorded"] is False


def test_one_size_timeout_stops_new_work_without_retry_or_message(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    fleet = FakeFleet(fail_size_once=True, delay_seconds=0.003)
    report = _run(root, fleet)
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "PROVIDER_TIMEOUT"
    assert report["exception_type"] == "ReadTimeout"
    assert report["failed_provider_operation"] == "get_billable_size"
    # Other isolated workers may finish one call and start another before the
    # failing worker's exception becomes observable and sets the shared stop
    # event. The census must still truncate far below its 180-call PASS scope.
    assert 21 <= report["provider_call_total"] < 40
    assert report["automatic_retries"] == 0
    assert report["provider_error_message_recorded"] is False
    assert "synthetic timeout" not in json.dumps(report)


@pytest.mark.parametrize("bad_size", [-1, 1.5, "1000", True, None])
def test_invalid_billable_size_fails_closed(
    tmp_path: Path, bad_size: object
) -> None:
    root = _copy_root(tmp_path)
    report = _run(root, FakeFleet(size=bad_size, delay_seconds=0.003))
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "BILLABLE_SIZE_RESPONSE_DRIFT"
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0


def test_insufficient_disk_fails_after_complete_metadata_census(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    report = _run(root, FakeFleet(), free=0)
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "INSUFFICIENT_DISK"
    assert report["provider_call_total"] == 180


def test_destination_collision_is_preserved_and_fails_closed(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    conflict = (
        root
        / "data/dbn/definition/MES/2019/2019-05-05_2020-01-01.dbn.zst"
    )
    conflict.parent.mkdir(parents=True, exist_ok=True)
    conflict.write_bytes(b"preserve")
    report = _run(root, FakeFleet())
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "DESTINATION_CONFLICT"
    assert report["provider_call_total"] == 180
    assert conflict.read_bytes() == b"preserve"


def test_report_destination_is_create_only(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    report_path = root / v21.REPORT_PATH
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("preserve", encoding="utf-8")
    with pytest.raises(IntegrityError, match="create-only"):
        _run(root, FakeFleet())
    assert report_path.read_text(encoding="utf-8") == "preserve"


def test_executor_has_only_cost_and_size_provider_call_surfaces() -> None:
    source = inspect.getsource(v21.execute_preflight)
    assert "apis.list_datasets" not in source
    assert "apis.list_schemas" not in source
    assert "apis.get_dataset_range" not in source
    assert "apis.resolve" not in source
    assert ".download(" not in source.lower()
    assert ".batch" not in source.lower()
    assert ".decode(" not in source.lower()
    assert "read_dbn" not in source.lower()


def test_v21_documentation_matches_executed_metadata_only_reality() -> None:
    outline = " ".join((ROOT / "PROJECT_OUTLINE.md").read_text(encoding="utf-8").split())
    folder_map = " ".join(
        (ROOT / "PIPELINE_FOLDER_MAP.md").read_text(encoding="utf-8").split()
    )
    assert "v20" in outline and "68 metadata calls" in outline
    assert "PASS_METADATA_ONLY" in outline
    assert "executed v21 timeout-safe successor" in outline
    assert "180" in outline and "Six isolated" in outline
    assert "apex_micro_metadata_preflight_v21" in folder_map
    assert "CURRENT_REACHABLE" in folder_map
    assert "grants no download authority" in folder_map
    assert "7,200-second global ceiling" in folder_map
    assert "zero accepted/finalized pairs" in folder_map
    assert "no provider or download authority" in folder_map
