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
from futures_rebuild.canonical import sha256_file
from futures_rebuild.errors import IntegrityError
from futures_rebuild.micro_alpha_databento_preflight import (
    CREDENTIAL_SOURCE,
    MetadataProviderApis,
    OPERATION,
)
from futures_rebuild.micro_alpha_databento_preflight_v12 import (
    MAXIMUM_STATUS_INTEGER_MAGNITUDE,
    MAXIMUM_STATUS_STRING_LENGTH,
    PLAN_PATH,
    REPORT_PATH,
    _opaque_application_status,
    _symbology_summary,
    build_plan,
    execute_preflight,
    load_predecessor_failure,
    required_scope,
    validate_plan,
)


pytestmark = [pytest.mark.current, pytest.mark.high_risk]
ROOT = Path(__file__).resolve().parents[1]
_AUTO_PARTIAL = object()


def _copy_root(tmp_path: Path) -> Path:
    root = tmp_path / "active"
    plan = json.loads((ROOT / PLAN_PATH).read_text(encoding="utf-8"))
    for relative in plan["plan_bindings"]:
        source = ROOT / relative
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    destination = root / PLAN_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / PLAN_PATH, destination)
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
    return OperationReceipt.issue_user_approved(
        RepoBoundary(root),
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha,
        approval_line=_personal_approval_line(OPERATION, str(plan["plan_id"]), plan_sha),
    )


class FakeMetadataProvider:
    def __init__(
        self,
        *,
        status: object = 200,
        partial: object = _AUTO_PARTIAL,
        cost: object = 0,
    ) -> None:
        self.status = status
        self.partial = partial
        self.cost = cost
        self.calls: list[str] = []
        self.effective_dates = {
            "MES": "2019-05-06",
            "MCL": "2021-07-12",
            "MGC": "2010-10-03",
            "M6E": "2010-03-23",
        }

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
        query_start = str(kwargs["start_date"])
        effective = max(self.effective_dates[market], query_start)
        partial = (
            ["OPAQUE_A", "OPAQUE_B"]
            if self.partial is _AUTO_PARTIAL and effective > query_start
            else []
            if self.partial is _AUTO_PARTIAL
            else self.partial
        )
        return _response(
            symbol=symbol,
            stype_in=str(kwargs["stype_in"]),
            start=query_start,
            end=str(kwargs["end_date"]),
            effective=effective,
            partial=partial,
            status=self.status,
        )

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


def _response(
    *,
    symbol: str = "MES.FUT",
    stype_in: str = "parent",
    start: str = "2010-01-01",
    end: str = "2026-08-09",
    effective: str = "2019-05-06",
    partial: object = None,
    status: object = 200,
) -> dict[str, object]:
    return {
        "result": {symbol: [{"d0": effective, "d1": end, "s": 123}]},
        "symbols": [symbol],
        "stype_in": stype_in,
        "stype_out": "instrument_id",
        "start_date": start,
        "end_date": end,
        "partial": ["OPAQUE"] if partial is None else partial,
        "not_found": [],
        "message": "OK",
        "status": status,
    }


def _run(root: Path, provider: FakeMetadataProvider) -> dict[str, object]:
    return execute_preflight(
        root=root,
        authorization=_receipt(root),
        provider_factory=provider.capability,
        credential_source=CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
        environment_check=lambda _root: "synthetic-lock",
    )


def test_v12_plan_preserves_exact_scope_and_forbids_download() -> None:
    plan = validate_plan(
        json.loads((ROOT / PLAN_PATH).read_text(encoding="utf-8")), root=ROOT
    )
    assert plan == build_plan(root=ROOT)
    assert len(plan["requests"]) == 20
    assert {request["market"] for request in plan["requests"]} == {
        "MES",
        "MCL",
        "MGC",
        "M6E",
    }
    assert plan["forbidden"]["timeseries_download"] is True
    assert plan["forbidden"]["historical_row_read"] is True
    assert plan["checks"]["application_status_bounded_opaque_scalar_shape"] is True


def test_v12_preserves_v11_failure_byte_for_byte() -> None:
    report = load_predecessor_failure(root=ROOT)
    assert report["report_id"] == (
        "86541273eff2dcfc3e20912b00d91e6c742a2c62f5411528d3b312d68bf0405a"
    )
    assert report["failed_validation_field"] == "status"
    assert report["timeseries_download_calls"] == 0


@pytest.mark.parametrize("value", [0, 200, -1, "OK", "success"])
def test_v12_accepts_bounded_opaque_status_shapes(value: object) -> None:
    disposition = _opaque_application_status(value)
    assert disposition.endswith("VALUE_NOT_RECORDED")
    assert str(value) not in disposition


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        1.0,
        "",
        "x" * (MAXIMUM_STATUS_STRING_LENGTH + 1),
        MAXIMUM_STATUS_INTEGER_MAGNITUDE + 1,
    ],
)
def test_v12_rejects_malformed_or_unbounded_status(value: object) -> None:
    with pytest.raises(IntegrityError, match="application status"):
        _opaque_application_status(value)


def test_v12_summary_records_status_shape_not_value() -> None:
    summary = _symbology_summary(
        _response(status=200),
        symbol="MES.FUT",
        stype_in="parent",
        query_start="2010-01-01",
        end="2026-08-09",
        allow_discovery_partial=True,
    )
    assert summary["application_status_shape"] == "BOUNDED_INTEGER_VALUE_NOT_RECORDED"
    assert summary["application_status_value_recorded"] is False
    assert 200 not in summary.values()


def test_v12_status_correction_does_not_weaken_exact_echo_gate() -> None:
    response = _response(status=200)
    response["symbols"] = ["MCL.FUT"]
    with pytest.raises(IntegrityError, match="symbols echo drifted"):
        _symbology_summary(
            response,
            symbol="MES.FUT",
            stype_in="parent",
            query_start="2010-01-01",
            end="2026-08-09",
            allow_discovery_partial=True,
        )


def test_v12_full_synthetic_metadata_mechanics_pass_without_download(
    tmp_path: Path,
) -> None:
    root = _copy_root(tmp_path)
    report = _run(root, FakeMetadataProvider(status=200))
    assert report["state"] == "PASS_METADATA_ONLY"
    assert report["external_cost_incurred_usd"] == "0"
    assert report["timeseries_download_calls"] == 0
    assert report["historical_rows_read"] is False
    assert report["dbn_files_created"] == 0
    assert report["annual_market_schema_request_count"] <= 180
    assert report["provider_call_total"] <= 375
    assert (root / REPORT_PATH).is_file()
    assert not (root / "data/dbn").exists()


def test_v12_malformed_live_status_fails_closed_and_sanitized(
    tmp_path: Path,
) -> None:
    root = _copy_root(tmp_path)
    report = _run(root, FakeMetadataProvider(status={"secret": "value"}))
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "OPAQUE_APPLICATION_STATUS_SHAPE"
    assert report["failed_validation_field"] == "status"
    assert report["provider_call_total"] == 4
    assert report["provider_error_message_recorded"] is False
    assert "secret" not in json.dumps(report)
    assert report["timeseries_download_calls"] == 0


def test_v12_nonzero_cost_still_fails_closed(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    report = _run(root, FakeMetadataProvider(status=200, cost="0.01"))
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "UNEXPECTED_NONZERO_COST"
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0


def test_v12_report_is_create_only(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    first = _run(root, FakeMetadataProvider(status=200))
    assert first["state"] == "PASS_METADATA_ONLY"
    with pytest.raises(IntegrityError, match="create-only"):
        _run(root, FakeMetadataProvider(status=200))


def test_v12_documentation_preserves_implementation_reality() -> None:
    outline = (ROOT / "PROJECT_OUTLINE.md").read_text(encoding="utf-8")
    folder_map = (ROOT / "PIPELINE_FOLDER_MAP.md").read_text(encoding="utf-8")
    normalized_outline = " ".join(outline.split())
    assert "v11 preflight -> FAIL_CLOSED_METADATA_ONLY" in outline
    assert "v12 preflight -> FAIL_CLOSED_METADATA_ONLY" in outline
    assert "v14 preflight -> FAIL_CLOSED_METADATA_ONLY" in outline
    assert "immutable v19 opaque-partial-semantic-safe successor" in outline
    assert "v19 may contact Databento" in normalized_outline
    assert "v11 bounded opaque-partial-flag successor" in folder_map
    assert "RETIRED / fail-closed status-semantic evidence" in folder_map
    assert "v12 SDK-contract-safe status successor" in folder_map
    assert "RETIRED / fail-closed message-semantic evidence" in folder_map
    assert "v14 provider-result-group-safe predecessor" in folder_map
    assert "RETIRED / fail-closed post-effective partial evidence" in folder_map
    assert "immutable v19 opaque-partial-semantic-safe successor" in folder_map
    assert "passing v19 preflight and committed HEAD required first" in folder_map
