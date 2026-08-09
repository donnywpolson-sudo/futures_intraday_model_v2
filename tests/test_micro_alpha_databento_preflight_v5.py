from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

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
    MAXIMUM_ANNUAL_REQUESTS,
    MAXIMUM_PROVIDER_CALLS,
    MAXIMUM_RUNTIME_SECONDS,
    PER_CALL_TIMEOUT_SECONDS,
    PLAN_PATH,
    PREDECESSOR_AUTHORIZATION_PATH,
    PREDECESSOR_PLAN_PATH,
    PREDECESSOR_REPORT_ID,
    PREDECESSOR_REPORT_PATH,
    REFERENCE_PATH,
    REPORT_PATH,
    LOCAL_SUPERSESSION_PATH,
    SUPERSEDED_LOCAL_PLAN_PATH,
    SUPERSESSION_PATH,
    build_file_metadata_provider_apis,
    build_plan,
    execute_preflight,
    _latest_complete_end,
    load_predecessor_failure,
    required_scope,
    validate_plan,
)


pytestmark = [pytest.mark.current, pytest.mark.high_risk]
ROOT = Path(__file__).resolve().parents[1]
BINDING_PATHS = (
    "src/futures_rebuild/micro_alpha_pipeline.py",
    "src/futures_rebuild/micro_alpha_databento_preflight_v5.py",
    "src/futures_rebuild/micro_alpha_acquisition.py",
    "src/futures_rebuild/alpha_research_architecture.py",
    REFERENCE_PATH.as_posix(),
    SUPERSESSION_PATH.as_posix(),
    PREDECESSOR_PLAN_PATH.as_posix(),
    PREDECESSOR_REPORT_PATH.as_posix(),
    PREDECESSOR_AUTHORIZATION_PATH.as_posix(),
    SUPERSEDED_LOCAL_PLAN_PATH.as_posix(),
    LOCAL_SUPERSESSION_PATH.as_posix(),
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
    def __init__(self, *, failure: Exception | None = None) -> None:
        self.failure = failure
        self.calls: list[tuple[str, dict[str, object]]] = []

    def list_datasets(self, **kwargs: object) -> object:
        self.calls.append(("list_datasets", kwargs))
        return ["GLBX.MDP3"]

    def list_schemas(self, **kwargs: object) -> object:
        self.calls.append(("list_schemas", kwargs))
        if self.failure is not None:
            raise self.failure
        return ["definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"]

    def get_dataset_range(self, **kwargs: object) -> object:
        self.calls.append(("get_dataset_range", kwargs))
        return {
            "start": "2010-01-01T00:00:00+00:00",
            "end": "2026-08-08T12:00:00+00:00",
            "schema": {
                schema: {
                    "start": "2010-01-01T00:00:00+00:00",
                    "end": "2026-08-08T12:00:00+00:00",
                }
                for schema in (
                    "definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"
                )
            },
        }

    def resolve(self, **kwargs: object) -> object:
        self.calls.append(("resolve", kwargs))
        symbol = kwargs["symbols"][0]
        market = str(symbol).split(".")[0]
        effective = {
            "MES": "2019-05-06",
            "MCL": "2021-07-12",
            "MGC": "2010-10-03",
            "M6E": "2009-03-23",
        }[market]
        return {
            "result": {
                symbol: [
                    {"d0": effective, "d1": kwargs["end_date"], "s": 123}
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
        return 0

    def get_billable_size(self, **kwargs: object) -> object:
        self.calls.append(("get_billable_size", kwargs))
        return 1000

    def capability(self) -> MetadataProviderApis:
        return MetadataProviderApis(
            list_datasets=self.list_datasets,
            list_schemas=self.list_schemas,
            get_dataset_range=self.get_dataset_range,
            resolve=self.resolve,
            get_cost=self.get_cost,
            get_billable_size=self.get_billable_size,
        )


def _run(root: Path, provider: FakeMetadataProvider) -> dict[str, object]:
    return execute_preflight(
        root=root,
        authorization=_receipt(root),
        provider_factory=provider.capability,
        credential_source=CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
        environment_check=lambda _root: "synthetic-lock",
    )


def test_successor_preserves_v4_failure_and_binds_annual_layout(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    failure = load_predecessor_failure(root=root)
    plan = build_plan(root=root)
    validate_plan(plan, root=root)
    assert failure["report_id"] == PREDECESSOR_REPORT_ID
    assert plan["predecessor_execution"]["report_id"] == PREDECESSOR_REPORT_ID
    assert plan["correction"] == {
        "reason": "VALID_NESTED_DATASET_RANGE_AND_ANNUAL_MARKET_YEAR_RECONCILIATION",
        "predecessor_range_shape": "START_END_SCHEMA_NESTED_RANGES",
        "predecessor_file_partition": "ONE_MULTI_YEAR_FILE_PER_MARKET_SCHEMA",
        "successor_file_partition": "ONE_FILE_PER_MARKET_SCHEMA_CALENDAR_YEAR",
        "successor_per_call_timeout_seconds": 30,
        "scope_change": "NO_MARKET_SCHEMA_DATA_ENDPOINT_OR_COST_CHANGE",
    }
    assert len(plan["requests"]) == 20
    assert {item["market"] for item in plan["requests"]} == {
        "MES", "MCL", "MGC", "M6E"
    }
    assert plan["limits"]["exact_provider_call_ceiling"] == 371
    assert plan["limits"]["maximum_annual_market_schema_requests"] == 180
    assert plan["limits"]["maximum_runtime_seconds"] == 300
    assert plan["limits"]["per_call_timeout_seconds"] == 30
    assert plan["limits"]["maximum_external_cost_usd"] == "0"
    assert plan["limits"]["maximum_retries"] == 0
    assert plan["forbidden"]["timeseries_download"] is True
    assert plan["output"]["path"] != PREDECESSOR_REPORT_PATH.as_posix()


def test_successor_metadata_capability_has_no_download_and_sets_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
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
        "list_datasets", "list_schemas", "get_dataset_range", "resolve",
        "get_cost", "get_billable_size",
    }
    assert not hasattr(capability, "get_range")
    assert metadata.TIMEOUT == PER_CALL_TIMEOUT_SECONDS
    assert symbology.TIMEOUT == PER_CALL_TIMEOUT_SECONDS


def test_successor_synthetic_success_is_annual_and_bounded(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    provider = FakeMetadataProvider()
    report = _run(root, provider)
    assert report["state"] == "PASS_METADATA_ONLY"
    assert report["provider_call_total"] == 331
    assert report["provider_call_counts"] == {
        "get_billable_size": 160,
        "get_cost": 160,
        "get_dataset_range": 1,
        "list_datasets": 1,
        "list_schemas": 1,
        "resolve": 8,
    }
    assert report["external_cost_incurred_usd"] == "0"
    assert report["timeseries_download_calls"] == 0
    assert report["historical_rows_read"] is False
    assert report["dbn_files_created"] == 0
    assert report["predecessor_report_id"] == PREDECESSOR_REPORT_ID
    assert len(provider.calls) == 331
    assert report["annual_market_schema_request_count"] == 160
    assert report["maximum_annual_market_schema_requests"] == MAXIMUM_ANNUAL_REQUESTS
    assert len(report["request_estimates"]) == 160
    destinations = [item["dbn_destination"] for item in report["request_estimates"]]
    assert len(set(destinations)) == 160
    assert all("data/dbn/" in path for path in destinations)
    assert all("_2027-" not in path for path in destinations)


def test_successor_timeout_fails_closed_once_and_cannot_overwrite(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    provider = FakeMetadataProvider(failure=TimeoutError("synthetic timeout"))
    report = _run(root, provider)
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "PROVIDER_TIMEOUT"
    assert report["provider_call_counts"] == {
        "list_datasets": 1,
        "list_schemas": 1,
    }
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0
    assert report["exception_type"] == "TimeoutError"
    with pytest.raises(IntegrityError, match="create-only"):
        _run(root, FakeMetadataProvider())


def test_successor_refuses_missing_or_drifted_predecessor_evidence(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    (root / PREDECESSOR_REPORT_PATH).write_text("{}\n", encoding="utf-8")
    with pytest.raises(IntegrityError, match="preserved byte-for-byte"):
        load_predecessor_failure(root=root)


def test_successor_requires_exact_authorization_and_runtime_scope(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    assert MAXIMUM_RUNTIME_SECONDS == 300
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


def test_nested_range_uses_common_required_schema_end_and_rejects_drift() -> None:
    value = {
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
            for schema in ("definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s")
        },
    }
    assert _latest_complete_end(value) == "2026-08-08"
    missing = json.loads(json.dumps(value))
    del missing["schema"]["status"]
    with pytest.raises(IntegrityError, match="required schema"):
        _latest_complete_end(missing)
    unexpected = {**value, "unexpected": True}
    with pytest.raises(IntegrityError, match="unexpected fields"):
        _latest_complete_end(unexpected)
