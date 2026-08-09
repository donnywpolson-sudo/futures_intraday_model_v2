from __future__ import annotations

import inspect
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
from futures_rebuild import micro_alpha_databento_preflight_v20 as v20


pytestmark = [pytest.mark.current, pytest.mark.high_risk]
ROOT = Path(__file__).resolve().parents[1]


def _copy_root(tmp_path: Path) -> Path:
    root = tmp_path / "active"
    plan = json.loads((ROOT / v20.PLAN_PATH).read_text(encoding="utf-8"))
    for relative in plan["plan_bindings"]:
        source = ROOT / relative
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    destination = root / v20.PLAN_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / v20.PLAN_PATH, destination)
    return root


def _receipt(root: Path) -> OperationReceipt:
    plan = json.loads((root / v20.PLAN_PATH).read_text(encoding="utf-8"))
    full = v20.required_scope(root=root, plan=plan)
    scope = {
        key: value
        for key, value in full.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    plan_sha = sha256_file(root / v20.PLAN_PATH)
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


class CostSizeOnlyProvider:
    TIMEOUT = 30

    def __init__(self, *, cost: object = 0, size: object = 1000) -> None:
        self.cost = cost
        self.size = size
        self.calls: list[tuple[str, dict[str, object]]] = []

    def forbidden(self, **_kwargs: object) -> object:
        raise AssertionError("a cumulatively satisfied provider operation was re-queried")

    def get_cost(self, **kwargs: object) -> object:
        self.calls.append(("get_cost", dict(kwargs)))
        return self.cost

    def get_billable_size(self, **kwargs: object) -> object:
        self.calls.append(("get_billable_size", dict(kwargs)))
        return self.size

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
    provider: CostSizeOnlyProvider,
    *,
    free: int = 10**12,
) -> dict[str, object]:
    return v20.execute_preflight(
        root=root,
        authorization=_receipt(root),
        provider_factory=provider.capability,
        credential_source=CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=free),
        environment_check=lambda _root: "synthetic-lock",
    )


def test_v20_plan_is_exact_cumulative_and_annual() -> None:
    plan = v20.validate_plan(
        json.loads((ROOT / v20.PLAN_PATH).read_text(encoding="utf-8")), root=ROOT
    )
    assert plan == v20.build_plan(root=ROOT)
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
    assert plan["limits"]["exact_provider_call_ceiling"] == 320
    assert plan["limits"]["maximum_annual_market_schema_requests"] == 160
    assert plan["provider_operations"] == {
        "list_datasets": 0,
        "list_schemas": 0,
        "get_dataset_range": 0,
        "resolve": 0,
        "get_cost": 160,
        "get_billable_size": 160,
        "timeseries_download": 0,
    }
    assert plan["annual_scope"]["market_interval_counts"] == {
        "MES": 8,
        "MCL": 6,
        "MGC": 9,
        "M6E": 9,
    }


def test_v20_uses_sealed_metadata_and_official_dates() -> None:
    report = v20.load_predecessor_metadata(root=ROOT)
    assert report["provider_call_total"] == 15
    plan = v20.load_plan(root=ROOT)
    assert plan["official_product_effective_date_sources"][
        "product_effective_dates"
    ] == {
        "M6E": "2009-03-22",
        "MCL": "2021-07-11",
        "MES": "2019-05-05",
        "MGC": "2010-10-03",
    }
    assert plan["correction"]["passed_v19_metadata_requeried"] is False
    assert plan["checks"][
        "sealed_v19_coverage_envelops_each_official_date_acquisition_start"
    ] is True


def test_prelaunch_intervals_are_explicit_and_create_no_fake_dbn() -> None:
    plan = v20.load_plan(root=ROOT)
    dispositions = plan["annual_scope"]["prelaunch_dispositions"]
    assert dispositions["MGC"] == []
    assert dispositions["M6E"] == []
    assert dispositions["MES"] == [
        {
            "year": 2018,
            "start": "2018-01-01",
            "end_exclusive": "2019-01-01",
            "disposition": "PRODUCT_PRELAUNCH_NO_DBN_FABRICATED",
        },
        {
            "year": 2019,
            "start": "2019-01-01",
            "end_exclusive": "2019-05-05",
            "disposition": "PRODUCT_PRELAUNCH_NO_DBN_FABRICATED",
        },
    ]
    assert len(dispositions["MCL"]) == 4


def test_v20_full_bounded_cost_size_mechanics_pass(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    provider = CostSizeOnlyProvider()
    report = _run(root, provider)
    assert report["state"] == "PASS_METADATA_ONLY"
    assert report["provider_call_counts"] == {
        "get_billable_size": 160,
        "get_cost": 160,
    }
    assert report["provider_call_total"] == 320
    assert report["annual_market_schema_request_count"] == 160
    assert len(report["request_estimates"]) == 160
    assert len(provider.calls) == 320
    assert {name for name, _ in provider.calls} == {
        "get_cost",
        "get_billable_size",
    }
    assert report["external_cost_incurred_usd"] == "0"
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0
    assert report["historical_rows_read"] is False
    assert report["dbn_files_created"] == 0
    assert (root / v20.REPORT_PATH).is_file()
    assert all(
        estimate["dbn_destination"].endswith(".dbn.zst")
        and estimate["sidecar_destination"].endswith(".manifest.json")
        for estimate in report["request_estimates"]
    )


def test_nonzero_cost_fails_at_first_call_without_retry(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    provider = CostSizeOnlyProvider(cost="0.01")
    report = _run(root, provider)
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "UNEXPECTED_NONZERO_COST"
    assert report["provider_call_total"] == 1
    assert report["failed_provider_operation"] == "get_cost"
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0


@pytest.mark.parametrize("bad_size", [-1, 1.5, "1000", True, None])
def test_invalid_billable_size_fails_closed(
    tmp_path: Path, bad_size: object
) -> None:
    root = _copy_root(tmp_path)
    report = _run(root, CostSizeOnlyProvider(size=bad_size))
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "BILLABLE_SIZE_RESPONSE_DRIFT"
    assert report["provider_call_total"] == 2
    assert report["automatic_retries"] == 0


def test_insufficient_disk_fails_after_bounded_census(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    report = _run(root, CostSizeOnlyProvider(), free=0)
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "INSUFFICIENT_DISK"
    assert report["provider_call_total"] == 320


def test_exact_destination_conflict_fails_without_overwrite(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    conflict = (
        root
        / "data/dbn/definition/MES/2019/2019-05-05_2020-01-01.dbn.zst"
    )
    conflict.parent.mkdir(parents=True, exist_ok=True)
    conflict.write_bytes(b"preserve")
    report = _run(root, CostSizeOnlyProvider())
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "DESTINATION_CONFLICT"
    assert conflict.read_bytes() == b"preserve"


def test_report_destination_is_create_only(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    path = root / v20.REPORT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("preserve", encoding="utf-8")
    with pytest.raises(IntegrityError, match="create-only"):
        _run(root, CostSizeOnlyProvider())
    assert path.read_text(encoding="utf-8") == "preserve"


def test_executor_has_no_repeated_metadata_download_or_decode_surface() -> None:
    source = inspect.getsource(v20.execute_preflight)
    assert "apis.list_datasets" not in source
    assert "apis.list_schemas" not in source
    assert "apis.get_dataset_range" not in source
    assert "apis.resolve" not in source
    assert ".download(" not in source.lower()
    assert ".batch" not in source.lower()
    assert ".decode(" not in source.lower()
    assert "read_dbn" not in source.lower()


def test_v20_documentation_matches_execution_reality() -> None:
    outline = " ".join((ROOT / "PROJECT_OUTLINE.md").read_text(encoding="utf-8").split())
    folder_map = " ".join(
        (ROOT / "PIPELINE_FOLDER_MAP.md").read_text(encoding="utf-8").split()
    )
    assert "v20" in outline
    assert "68 metadata calls" in outline
    assert "PROVIDER_TIMEOUT" in outline
    assert "RETIRED" in folder_map
    assert "apex_micro_metadata_preflight_v20" in folder_map
    assert "v21 timeout-safe cumulative metadata successor" in folder_map
    assert "download authority remains absent" in folder_map
