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
from futures_rebuild.micro_alpha_databento_preflight_v8 import (
    MAXIMUM_ANNUAL_REQUESTS,
    MAXIMUM_PROVIDER_CALLS,
    PLAN_PATH,
    PREDECESSOR_AUTHORIZATION_PATH,
    PREDECESSOR_REPORT_ID,
    PREDECESSOR_REPORT_PATH,
    REFERENCE_PATH,
    REPORT_PATH,
    SUPERSESSION_PATH,
    _symbology_summary,
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
    "src/futures_rebuild/micro_alpha_databento_preflight_v5.py",
    "src/futures_rebuild/micro_alpha_databento_preflight_v6.py",
    "src/futures_rebuild/micro_alpha_databento_preflight_v7.py",
    "src/futures_rebuild/micro_alpha_databento_preflight_v8.py",
    "src/futures_rebuild/micro_alpha_pipeline.py",
    "src/futures_rebuild/micro_alpha_acquisition.py",
    "src/futures_rebuild/alpha_research_architecture.py",
    "src/futures_rebuild/runtime_environment.py",
    REFERENCE_PATH.as_posix(),
    SUPERSESSION_PATH.as_posix(),
    PREDECESSOR_REPORT_PATH.as_posix(),
    PREDECESSOR_AUTHORIZATION_PATH.as_posix(),
    "configs/apex_micro_tier01_databento_metadata_preflight_v7.json",
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
        partial: object | None = None,
        not_found: object | None = None,
        effective_dates: dict[str, str] | None = None,
        cost: object = 0,
        message: object = "OK",
    ) -> None:
        self.partial = [] if partial is None else partial
        self.not_found = [] if not_found is None else not_found
        self.effective_dates = effective_dates or {
            "MES": "2019-05-06",
            "MCL": "2021-07-12",
            "MGC": "2010-10-03",
            "M6E": "2010-03-23",
        }
        self.cost = cost
        self.message = message
        self.calls: list[str] = []

    def list_datasets(self, **_kwargs: object) -> object:
        self.calls.append("list_datasets")
        return ["GLBX.MDP3"]

    def list_schemas(self, **_kwargs: object) -> object:
        self.calls.append("list_schemas")
        return ["definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"]

    def get_dataset_range(self, **_kwargs: object) -> object:
        self.calls.append("get_dataset_range")
        return {
            "start": "2010-01-01T00:00:00+00:00",
            "end": "2026-08-09T00:00:00+00:00",
            "schema": {
                schema: {
                    "start": "2010-01-01T00:00:00+00:00",
                    "end": "2026-08-09T00:00:00+00:00",
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
        self.calls.append("resolve")
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
            "partial": self.partial,
            "not_found": self.not_found,
            "message": self.message,
            "status": 0,
        }

    def get_cost(self, **_kwargs: object) -> object:
        self.calls.append("get_cost")
        return self.cost

    def get_billable_size(self, **_kwargs: object) -> object:
        self.calls.append("get_billable_size")
        return 1000

    def capability(self) -> MetadataProviderApis:
        return MetadataProviderApis(
            self.list_datasets,
            self.list_schemas,
            self.get_dataset_range,
            self.resolve,
            self.get_cost,
            self.get_billable_size,
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


def _response(
    *, partial: object, not_found: object, message: object = "OK"
) -> dict[str, object]:
    return {
        "result": {"MES.FUT": [{"d0": "2019-05-06", "d1": "2026-08-09", "s": 1}]},
        "symbols": ["MES.FUT"],
        "stype_in": "parent",
        "stype_out": "instrument_id",
        "start_date": "2010-01-01",
        "end_date": "2026-08-09",
        "partial": partial,
        "not_found": not_found,
        "message": message,
        "status": 0,
    }


def test_v8_preserves_v7_failure_and_binds_exact_scope(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    failure = load_predecessor_failure(root=root)
    plan = build_plan(root=root)
    validate_plan(plan, root=root)
    assert failure["report_id"] == PREDECESSOR_REPORT_ID
    assert plan["correction"]["reason"] == (
        "DATABENTO_SUCCESS_MESSAGE_ECHO_REQUIRES_BOUNDED_ALLOWLIST"
    )
    assert len(plan["requests"]) == 20
    assert plan["limits"]["exact_provider_call_ceiling"] == MAXIMUM_PROVIDER_CALLS
    assert plan["limits"]["maximum_annual_market_schema_requests"] == (
        MAXIMUM_ANNUAL_REQUESTS
    )


def test_v8_accepts_only_exact_empty_status_lists_and_success_messages() -> None:
    summary = _symbology_summary(
        _response(partial=[], not_found=[]),
        symbol="MES.FUT",
        stype_in="parent",
        query_start="2010-01-01",
        end="2026-08-09",
    )
    assert summary["partial_count"] == summary["not_found_count"] == 0
    empty_message_summary = _symbology_summary(
        _response(partial=[], not_found=[], message=""),
        symbol="MES.FUT",
        stype_in="parent",
        query_start="2010-01-01",
        end="2026-08-09",
    )
    assert empty_message_summary["partial_count"] == 0
    for field, value in (
        ("partial", False),
        ("partial", {}),
        ("partial", [1]),
        ("not_found", False),
        ("not_found", {}),
        ("not_found", [1]),
    ):
        payload = _response(partial=[], not_found=[])
        payload[field] = value
        with pytest.raises(IntegrityError, match="exact string list"):
            _symbology_summary(
                payload,
                symbol="MES.FUT",
                stype_in="parent",
                query_start="2010-01-01",
                end="2026-08-09",
            )
    for field in ("partial", "not_found"):
        payload = _response(partial=[], not_found=[])
        payload[field] = ["MES.FUT"]
        with pytest.raises(UnauthorizedOperation, match="nonempty"):
            _symbology_summary(
                payload,
                symbol="MES.FUT",
                stype_in="parent",
                query_start="2010-01-01",
                end="2026-08-09",
            )
    for message in (None, False, "SUCCESS", "not recorded"):
        with pytest.raises(IntegrityError, match="success message echo"):
            _symbology_summary(
                _response(partial=[], not_found=[], message=message),
                symbol="MES.FUT",
                stype_in="parent",
                query_start="2010-01-01",
                end="2026-08-09",
            )


def test_v8_successful_metadata_mechanics_are_price_free_and_download_free(
    tmp_path: Path,
) -> None:
    root = _copy_root(tmp_path)
    provider = FakeMetadataProvider()
    report = _run(root, provider)
    assert report["state"] == "PASS_METADATA_ONLY"
    count = report["annual_market_schema_request_count"]
    assert report["provider_call_total"] == 11 + (2 * count)
    assert report["provider_call_total"] <= MAXIMUM_PROVIDER_CALLS
    assert report["external_cost_incurred_usd"] == "0"
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0
    assert report["historical_rows_read"] is False
    assert report["dbn_files_created"] == 0
    for market in ("MES", "MCL", "MGC", "M6E"):
        for stype in ("parent", "continuous"):
            summary = report["symbology_summaries"][market][stype]
            assert summary["partial_count"] == summary["not_found_count"] == 0
    assert not (root / "data/dbn").exists()


@pytest.mark.parametrize(
    ("partial", "not_found", "failure_code"),
    [
        (False, [], "SYMBOL_STATUS_SHAPE_DRIFT"),
        ([], False, "SYMBOL_STATUS_SHAPE_DRIFT"),
        (["MES.FUT"], [], "PARTIAL_OR_NOT_FOUND_SYMBOLOGY"),
        ([], ["MES.FUT"], "PARTIAL_OR_NOT_FOUND_SYMBOLOGY"),
    ],
)
def test_v8_status_shape_and_resolution_failures_are_sanitized(
    tmp_path: Path,
    partial: object,
    not_found: object,
    failure_code: str,
) -> None:
    root = _copy_root(tmp_path)
    report = _run(
        root,
        FakeMetadataProvider(partial=partial, not_found=not_found),
    )
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == failure_code
    assert report["provider_call_total"] == 4
    assert report["automatic_retries"] == 0
    assert report["provider_error_message_recorded"] is False
    assert report["credential_content_recorded"] is False
    assert report["timeseries_download_calls"] == 0


def test_v8_message_drift_reports_only_the_field_name(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    report = _run(root, FakeMetadataProvider(message="UNEXPECTED_PROVIDER_TEXT"))
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "SYMBOL_SUCCESS_MESSAGE_DRIFT"
    assert report["failed_validation_field"] == "message"
    assert report["provider_call_total"] == 4
    assert report["provider_error_message_recorded"] is False
    assert "UNEXPECTED_PROVIDER_TEXT" not in json.dumps(report, sort_keys=True)


def test_v8_nonzero_cost_and_unresolved_pre_dataset_date_fail_closed(
    tmp_path: Path,
) -> None:
    cost_root = _copy_root(tmp_path / "cost")
    cost_report = _run(cost_root, FakeMetadataProvider(cost=1))
    assert cost_report["failure_code"] == "UNEXPECTED_NONZERO_COST"
    assert cost_report["automatic_retries"] == 0

    date_root = _copy_root(tmp_path / "date")
    dates = {
        "MES": "2019-05-06",
        "MCL": "2021-07-12",
        "MGC": "2010-10-03",
        "M6E": "2010-01-01",
    }
    date_report = _run(date_root, FakeMetadataProvider(effective_dates=dates))
    assert date_report["failure_code"] == (
        "PRODUCT_EFFECTIVE_DATE_UNRESOLVED_PRE_DATASET"
    )
    assert date_report["provider_call_total"] == 11


def test_v8_requires_exact_authorization_and_create_only_output(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    plan = json.loads((root / PLAN_PATH).read_text(encoding="utf-8"))
    with pytest.raises(UnauthorizedOperation):
        execute_preflight(
            root=root,
            authorization=OperationReceipt.issue_local(
                RepoBoundary(root),
                operation=OPERATION,
                classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
                scope=required_scope(root=root, plan=plan),
            ),
            provider_factory=FakeMetadataProvider().capability,
            credential_source=CREDENTIAL_SOURCE,
            environment_check=lambda _root: "synthetic-lock",
        )
    report = _run(root, FakeMetadataProvider())
    assert report["state"] == "PASS_METADATA_ONLY"
    with pytest.raises(IntegrityError, match="create-only"):
        _run(root, FakeMetadataProvider())
