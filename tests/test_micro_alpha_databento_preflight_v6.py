from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
from databento.common.error import BentoClientError

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
    _personal_approval_line,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.micro_alpha_databento_preflight import (
    CREDENTIAL_SOURCE,
    MetadataProviderApis,
    OPERATION,
)
from futures_rebuild.micro_alpha_databento_preflight_v5 import (
    LOCAL_SUPERSESSION_PATH,
    PLAN_PATH as V5_PLAN_PATH,
    PREDECESSOR_AUTHORIZATION_PATH as V4_AUTHORIZATION_PATH,
    PREDECESSOR_PLAN_PATH as V4_PLAN_PATH,
    PREDECESSOR_REPORT_PATH as V4_REPORT_PATH,
    SUPERSEDED_LOCAL_PLAN_PATH,
)
from futures_rebuild.micro_alpha_databento_preflight_v6 import (
    MAXIMUM_ANNUAL_REQUESTS,
    MAXIMUM_PROVIDER_CALLS,
    MAXIMUM_RUNTIME_SECONDS,
    PER_CALL_TIMEOUT_SECONDS,
    PLAN_PATH,
    PREDECESSOR_AUTHORIZATION_PATH,
    PREDECESSOR_REPORT_ID,
    PREDECESSOR_REPORT_PATH,
    REFERENCE_PATH,
    REPORT_PATH,
    SUPERSESSION_PATH,
    _dataset_bounds,
    build_file_metadata_provider_apis,
    build_plan,
    execute_preflight,
    load_predecessor_failure,
    required_scope,
    validate_plan,
)


pytestmark = [pytest.mark.current, pytest.mark.high_risk]
ROOT = Path(__file__).resolve().parents[1]
BINDING_PATHS = (
    "configs/dependency_lock_receipt.json",
    "src/futures_rebuild/boundary.py",
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/errors.py",
    "src/futures_rebuild/live_cockpit/databento_auth.py",
    "src/futures_rebuild/micro_alpha_databento_preflight.py",
    "src/futures_rebuild/micro_alpha_pipeline.py",
    "src/futures_rebuild/micro_alpha_databento_preflight_v5.py",
    "src/futures_rebuild/micro_alpha_databento_preflight_v6.py",
    "src/futures_rebuild/micro_alpha_acquisition.py",
    "src/futures_rebuild/alpha_research_architecture.py",
    "src/futures_rebuild/runtime_environment.py",
    REFERENCE_PATH.as_posix(),
    SUPERSESSION_PATH.as_posix(),
    V4_PLAN_PATH.as_posix(),
    V4_REPORT_PATH.as_posix(),
    V4_AUTHORIZATION_PATH.as_posix(),
    SUPERSEDED_LOCAL_PLAN_PATH.as_posix(),
    LOCAL_SUPERSESSION_PATH.as_posix(),
    V5_PLAN_PATH.as_posix(),
    PREDECESSOR_REPORT_PATH.as_posix(),
    PREDECESSOR_AUTHORIZATION_PATH.as_posix(),
)


def _copy_root(tmp_path: Path) -> Path:
    root = tmp_path / "active"
    for relative in BINDING_PATHS:
        source = ROOT / relative
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    plan = build_plan(root=root)
    path = root / PLAN_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(plan) + b"\n")
    return root


def _receipt(root: Path) -> OperationReceipt:
    plan = json.loads((root / PLAN_PATH).read_text(encoding="utf-8"))
    full = required_scope(root=root, plan=plan)
    scope = {
        key: value
        for key, value in full.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    plan_sha = sha256_file(root / PLAN_PATH)
    line = _personal_approval_line(OPERATION, str(plan["plan_id"]), plan_sha)
    return OperationReceipt.issue_user_approved(
        RepoBoundary(root),
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha,
        approval_line=line,
    )


class FakeMetadataProvider:
    def __init__(
        self,
        *,
        effective_dates: dict[str, str] | None = None,
        list_schema_failure: Exception | None = None,
        resolve_failure: Exception | None = None,
        cost: object = 0,
        billable_size: object = 1000,
    ) -> None:
        self.effective_dates = effective_dates or {
            "MES": "2019-05-06",
            "MCL": "2021-07-12",
            "MGC": "2010-10-03",
            "M6E": "2010-03-23",
        }
        self.list_schema_failure = list_schema_failure
        self.resolve_failure = resolve_failure
        self.cost = cost
        self.billable_size = billable_size
        self.calls: list[tuple[str, dict[str, object]]] = []

    def list_datasets(self, **kwargs: object) -> object:
        self.calls.append(("list_datasets", kwargs))
        return ["GLBX.MDP3"]

    def list_schemas(self, **kwargs: object) -> object:
        self.calls.append(("list_schemas", kwargs))
        if self.list_schema_failure is not None:
            raise self.list_schema_failure
        return ["definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"]

    def get_dataset_range(self, **kwargs: object) -> object:
        self.calls.append(("get_dataset_range", kwargs))
        return {
            "start": "2010-01-01T00:00:00+00:00",
            "end": "2026-08-09T00:00:00+00:00",
            "schema": {
                schema: {
                    "start": "2010-01-01T00:00:00+00:00",
                    "end": (
                        "2026-08-08T00:00:00+00:00"
                        if schema == "statistics"
                        else "2026-08-09T00:00:00+00:00"
                    ),
                }
                for schema in (
                    "definition",
                    "status",
                    "statistics",
                    "ohlcv-1m",
                    "ohlcv-1s",
                )
            },
        }

    def resolve(self, **kwargs: object) -> object:
        self.calls.append(("resolve", kwargs))
        if self.resolve_failure is not None:
            raise self.resolve_failure
        symbol = str(kwargs["symbols"][0])
        market = symbol.split(".")[0]
        return {
            "result": {
                symbol: [
                    {
                        "d0": self.effective_dates[market],
                        "d1": kwargs["end_date"],
                        "s": 123,
                    }
                ]
            },
            "symbols": [symbol],
            "stype_in": kwargs["stype_in"],
            "stype_out": "instrument_id",
            "start_date": kwargs["start_date"],
            "end_date": kwargs["end_date"],
            "partial": False,
            "not_found": [],
            "message": "",
            "status": 0,
        }

    def get_cost(self, **kwargs: object) -> object:
        self.calls.append(("get_cost", kwargs))
        return self.cost

    def get_billable_size(self, **kwargs: object) -> object:
        self.calls.append(("get_billable_size", kwargs))
        return self.billable_size

    def capability(self) -> MetadataProviderApis:
        return MetadataProviderApis(
            list_datasets=self.list_datasets,
            list_schemas=self.list_schemas,
            get_dataset_range=self.get_dataset_range,
            resolve=self.resolve,
            get_cost=self.get_cost,
            get_billable_size=self.get_billable_size,
        )


def _run(
    root: Path, provider: FakeMetadataProvider, *, free_disk: int = 10**12
) -> dict[str, object]:
    return execute_preflight(
        root=root,
        authorization=_receipt(root),
        provider_factory=provider.capability,
        credential_source=CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=free_disk),
        environment_check=lambda _root: "synthetic-lock",
    )


def test_v6_preserves_v5_failure_and_binds_exact_scope(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    failure = load_predecessor_failure(root=root)
    plan = build_plan(root=root)
    validate_plan(plan, root=root)
    assert failure["report_id"] == PREDECESSOR_REPORT_ID
    assert plan["predecessor_execution"]["failed_call_inference"] == {
        "basis": "DETERMINISTIC_EXECUTOR_ORDER_AND_SEALED_CALL_COUNTS",
        "call_ordinal": 4,
        "operation": "resolve",
        "market": "MES",
        "symbol": "MES.FUT",
        "stype_in": "parent",
        "rejected_start_date": "2000-01-01",
        "provider_message_recorded": False,
    }
    assert len(plan["requests"]) == 20
    assert {item["market"] for item in plan["requests"]} == {
        "MES",
        "MCL",
        "MGC",
        "M6E",
    }
    assert {item["schema"] for item in plan["requests"]} == {
        "definition",
        "status",
        "statistics",
        "ohlcv-1m",
        "ohlcv-1s",
    }
    assert plan["limits"]["exact_provider_call_ceiling"] == MAXIMUM_PROVIDER_CALLS
    assert plan["limits"]["maximum_external_cost_usd"] == "0"
    assert plan["limits"]["maximum_retries"] == 0
    assert plan["forbidden"]["timeseries_download"] is True


def test_v6_capability_has_no_download_and_sets_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Api:
        TIMEOUT = 100

        def list_datasets(self, **_kwargs: object) -> object:
            return []

        def list_schemas(self, **_kwargs: object) -> object:
            return []

        def get_dataset_range(self, **_kwargs: object) -> object:
            return {}

        def resolve(self, **_kwargs: object) -> object:
            return {}

        def get_cost(self, **_kwargs: object) -> object:
            return 0

        def get_billable_size(self, **_kwargs: object) -> object:
            return 0

    metadata = Api()
    symbology = Api()
    client = SimpleNamespace(metadata=metadata, symbology=symbology)
    monkeypatch.setattr(
        "futures_rebuild.micro_alpha_databento_preflight_v5.resolve_databento_api_key",
        lambda **_kwargs: "synthetic-secret",
    )
    capability = build_file_metadata_provider_apis(
        root=tmp_path, historical_factory=lambda **_kwargs: client
    )
    assert set(MetadataProviderApis.__annotations__) == {
        "list_datasets",
        "list_schemas",
        "get_dataset_range",
        "resolve",
        "get_cost",
        "get_billable_size",
    }
    assert not hasattr(capability, "get_range")
    assert metadata.TIMEOUT == PER_CALL_TIMEOUT_SECONDS
    assert symbology.TIMEOUT == PER_CALL_TIMEOUT_SECONDS


def test_v6_success_uses_provider_start_and_annual_destinations(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    provider = FakeMetadataProvider()
    report = _run(root, provider)
    assert report["state"] == "PASS_METADATA_ONLY"
    assert report["provider_dataset_start_date"] == "2010-01-01"
    assert report["latest_complete_end_exclusive"] == "2026-08-08"
    assert report["provider_call_total"] == 331
    assert report["provider_call_counts"] == {
        "get_billable_size": 160,
        "get_cost": 160,
        "get_dataset_range": 1,
        "list_datasets": 1,
        "list_schemas": 1,
        "resolve": 8,
    }
    resolve_calls = [kwargs for name, kwargs in provider.calls if name == "resolve"]
    assert len(resolve_calls) == 8
    assert {call["start_date"] for call in resolve_calls} == {"2010-01-01"}
    assert report["annual_market_schema_request_count"] == 160
    assert len(report["request_estimates"]) == 160
    destinations = [item["dbn_destination"] for item in report["request_estimates"]]
    assert len(set(destinations)) == 160
    assert all(path.startswith("data/dbn/") for path in destinations)
    assert report["external_cost_incurred_usd"] == "0"
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0
    assert report["historical_rows_read"] is False
    assert report["dbn_files_created"] == 0


def test_v6_fails_closed_when_effective_date_is_truncated_by_dataset_start(
    tmp_path: Path,
) -> None:
    root = _copy_root(tmp_path)
    effective = {
        "MES": "2019-05-06",
        "MCL": "2021-07-12",
        "MGC": "2010-10-03",
        "M6E": "2010-01-01",
    }
    report = _run(root, FakeMetadataProvider(effective_dates=effective))
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "PRODUCT_EFFECTIVE_DATE_UNRESOLVED_PRE_DATASET"
    assert report["unresolved_product_effective_date_markets"] == ["M6E"]
    assert report["provider_call_total"] == 11
    assert report["external_cost_incurred_usd"] == "0"
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0


def test_v6_records_price_free_http_status_and_call_context_only(
    tmp_path: Path,
) -> None:
    root = _copy_root(tmp_path)
    secret = "provider detail must not be recorded"
    failure = BentoClientError(http_status=422, message=secret)
    report = _run(root, FakeMetadataProvider(resolve_failure=failure))
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "PROVIDER_HTTP_CLIENT_ERROR"
    assert report["provider_call_total"] == 4
    assert report["failed_provider_operation"] == "resolve"
    assert report["failed_request_context"] == {
        "market": "MES",
        "stype_in": "parent",
        "symbol": "MES.FUT",
    }
    assert report["provider_http_status"] == 422
    assert report["provider_error_message_recorded"] is False
    assert secret not in (root / REPORT_PATH).read_text(encoding="utf-8")


def test_v6_timeout_is_one_attempt_and_report_is_create_only(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    report = _run(
        root,
        FakeMetadataProvider(list_schema_failure=TimeoutError("synthetic timeout")),
    )
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "PROVIDER_TIMEOUT"
    assert report["provider_call_counts"] == {
        "list_datasets": 1,
        "list_schemas": 1,
    }
    assert report["automatic_retries"] == 0
    with pytest.raises(IntegrityError, match="create-only"):
        _run(root, FakeMetadataProvider())


def test_v6_nonzero_cost_fails_before_any_size_estimate(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    provider = FakeMetadataProvider(cost="0.01")
    report = _run(root, provider)
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "UNEXPECTED_NONZERO_COST"
    assert report["provider_call_counts"] == {
        "get_cost": 1,
        "get_dataset_range": 1,
        "list_datasets": 1,
        "list_schemas": 1,
        "resolve": 8,
    }
    assert report["external_cost_incurred_usd"] == "0"
    assert report["automatic_retries"] == 0


def test_v6_insufficient_disk_and_destination_collision_fail_closed(
    tmp_path: Path,
) -> None:
    disk_root = _copy_root(tmp_path / "disk")
    disk_report = _run(disk_root, FakeMetadataProvider(), free_disk=0)
    assert disk_report["failure_code"] == "INSUFFICIENT_DISK"
    assert disk_report["provider_call_total"] == 331

    collision_root = _copy_root(tmp_path / "collision")
    collision = (
        collision_root
        / "data/dbn/definition/MES/2019/2019-05-06_2020-01-01.dbn.zst"
    )
    collision.parent.mkdir(parents=True, exist_ok=True)
    collision.write_bytes(b"synthetic-collision")
    collision_report = _run(collision_root, FakeMetadataProvider())
    assert collision_report["failure_code"] == "DESTINATION_CONFLICT"
    assert collision_report["provider_call_total"] == 331


def test_v6_runtime_ceiling_is_checked_after_each_call(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    clock_values = iter((0.0, 0.0, 301.0))
    report = execute_preflight(
        root=root,
        authorization=_receipt(root),
        provider_factory=FakeMetadataProvider().capability,
        credential_source=CREDENTIAL_SOURCE,
        clock=lambda: next(clock_values),
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
        environment_check=lambda _root: "synthetic-lock",
    )
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "RUNTIME_CEILING"
    assert report["provider_call_counts"] == {"list_datasets": 1}
    assert report["automatic_retries"] == 0


def test_v6_refuses_drifted_v5_failure(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    (root / PREDECESSOR_REPORT_PATH).write_text("{}\n", encoding="utf-8")
    with pytest.raises(IntegrityError, match="preserved byte-for-byte"):
        load_predecessor_failure(root=root)


def test_v6_requires_exact_external_authorization(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    assert MAXIMUM_RUNTIME_SECONDS == 300
    assert MAXIMUM_ANNUAL_REQUESTS == 180
    with pytest.raises(UnauthorizedOperation):
        execute_preflight(
            root=root,
            authorization=OperationReceipt.issue_local(
                RepoBoundary(root),
                operation=OPERATION,
                classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
                scope={"purpose": "not-provider-authority"},
            ),
            provider_factory=FakeMetadataProvider().capability,
            credential_source=CREDENTIAL_SOURCE,
            disk_usage=lambda _path: SimpleNamespace(free=10**12),
            environment_check=lambda _root: "synthetic-lock",
        )


def test_v6_dataset_bounds_are_nested_and_schema_conservative() -> None:
    value = FakeMetadataProvider().get_dataset_range()
    assert _dataset_bounds(value) == {
        "dataset_start": "2010-01-01",
        "schema_starts": {
            "definition": "2010-01-01",
            "ohlcv-1m": "2010-01-01",
            "ohlcv-1s": "2010-01-01",
            "statistics": "2010-01-01",
            "status": "2010-01-01",
        },
        "latest_complete_end_exclusive": "2026-08-08",
    }
    missing = json.loads(json.dumps(value))
    del missing["schema"]["status"]
    with pytest.raises(IntegrityError, match="required schema"):
        _dataset_bounds(missing)
    with pytest.raises(IntegrityError, match="unexpected fields"):
        _dataset_bounds({**value, "unexpected": True})
