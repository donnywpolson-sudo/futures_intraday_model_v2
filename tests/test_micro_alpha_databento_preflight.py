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
)
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.micro_alpha_databento_preflight import (
    CREDENTIAL_SOURCE,
    MAXIMUM_PROVIDER_CALLS,
    OBSOLETE_PLAN_ID,
    OBSOLETE_PLAN_PATH,
    OBSOLETE_PLAN_SHA256,
    OPERATION,
    PLAN_PATH,
    REFERENCE_PATH,
    REPORT_PATH,
    SUPERSESSION_PATH,
    MetadataProviderApis,
    build_plan,
    execute_obsolete_preflight,
    execute_preflight,
    required_scope,
    validate_plan,
)


pytestmark = [pytest.mark.current, pytest.mark.high_risk]
ROOT = Path(__file__).resolve().parents[1]
IMPLEMENTATION_PATHS = (
    "src/futures_rebuild/micro_alpha_pipeline.py",
    "src/futures_rebuild/micro_alpha_databento_preflight.py",
    "src/futures_rebuild/micro_alpha_acquisition.py",
    "src/futures_rebuild/alpha_research_architecture.py",
)


def _copy_preflight_root(tmp_path: Path) -> Path:
    root = tmp_path / "active"
    for relative in (
        *IMPLEMENTATION_PATHS,
        OBSOLETE_PLAN_PATH.as_posix(),
        REFERENCE_PATH.as_posix(),
        SUPERSESSION_PATH.as_posix(),
    ):
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
        key: value for key, value in full.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    plan_sha = sha256_file(root / PLAN_PATH)
    line = f"APPROVE {OPERATION} PLAN {plan['plan_id']} SHA256 {plan_sha}"
    return OperationReceipt.issue_user_approved(
        RepoBoundary(root), operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope, approval_command=OPERATION,
        approval_plan_id=str(plan["plan_id"]), approval_plan_sha256=plan_sha,
        approval_line=line,
    )


class FakeMetadataProvider:
    def __init__(self, *, cost: object = 0, size: int = 1000) -> None:
        self.cost = cost
        self.size = size
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.fail_once = False
        self.unexpected_symbology_field = False

    def list_datasets(self, **kwargs: object) -> object:
        self.calls.append(("list_datasets", kwargs))
        return ["GLBX.MDP3"]

    def list_schemas(self, **kwargs: object) -> object:
        self.calls.append(("list_schemas", kwargs))
        return ["definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"]

    def get_dataset_range(self, **kwargs: object) -> object:
        self.calls.append(("get_dataset_range", kwargs))
        return {"start": "2010-01-01T00:00:00+00:00", "end": "2026-08-08T12:00:00+00:00"}

    def resolve(self, **kwargs: object) -> object:
        self.calls.append(("resolve", kwargs))
        symbol = kwargs["symbols"][0]
        market = str(symbol).split(".")[0]
        dates = {"MES": "2019-05-06", "MCL": "2021-07-12", "MGC": "2010-10-03", "M6E": "2009-03-23"}
        value = {
            "result": {symbol: [{"d0": dates[market], "d1": kwargs["end_date"], "s": 123}]},
            "symbols": [symbol], "stype_in": kwargs["stype_in"],
            "stype_out": "instrument_id", "start_date": kwargs["start_date"],
            "end_date": kwargs["end_date"], "partial": False,
            "not_found": [], "message": "", "status": 0,
        }
        if self.unexpected_symbology_field:
            value["unexpected"] = "forbidden"
        return value

    def get_cost(self, **kwargs: object) -> object:
        self.calls.append(("get_cost", kwargs))
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("provider failed once; retry would be unsafe")
        return self.cost

    def get_billable_size(self, **kwargs: object) -> object:
        self.calls.append(("get_billable_size", kwargs))
        return self.size

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
        root=root, authorization=_receipt(root),
        provider_factory=provider.capability, credential_source=CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
        environment_check=lambda _root: "synthetic-lock",
    )


def test_obsolete_preparation_is_byte_preserved_and_unexecutable() -> None:
    assert sha256_file(ROOT / OBSOLETE_PLAN_PATH) == OBSOLETE_PLAN_SHA256
    obsolete = json.loads((ROOT / OBSOLETE_PLAN_PATH).read_text(encoding="utf-8"))
    assert obsolete["plan_id"] == OBSOLETE_PLAN_ID
    with pytest.raises(UnauthorizedOperation, match="SUPERSEDED_PREPARATION"):
        execute_obsolete_preflight()


def test_successor_has_exact_corrected_twenty_zero_cost_requests() -> None:
    plan = build_plan(root=ROOT)
    validate_plan(plan, root=ROOT)
    assert len(plan["requests"]) == 20
    assert {request["market"] for request in plan["requests"]} == {"MES", "MCL", "MGC", "M6E"}
    assert {request["schema"] for request in plan["requests"]} == {
        "definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s",
    }
    assert all(request["maximum_cost_usd"] == 0 for request in plan["requests"])
    assert plan["limits"]["exact_provider_call_ceiling"] == MAXIMUM_PROVIDER_CALLS
    assert plan["forbidden"]["timeseries_download"] is True
    assert plan["forbidden"]["data_dbn_write"] is True
    assert "get_range" not in MetadataProviderApis.__annotations__


def test_definition_uses_parent_and_every_other_schema_uses_continuous() -> None:
    for request in build_plan(root=ROOT)["requests"]:
        if request["schema"] == "definition":
            assert request["stype_in"] == "parent"
            assert request["symbols"] == [f"{request['market']}.FUT"]
        else:
            assert request["stype_in"] == "continuous"
            assert request["symbols"] == [f"{request['market']}.v.0"]
        assert request["stype_out"] == "instrument_id"


def test_superseded_multi_year_preflight_now_fails_closed_price_free(tmp_path: Path) -> None:
    root = _copy_preflight_root(tmp_path)
    provider = FakeMetadataProvider()
    report = _run(root, provider)
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["exception_type"] == "ContractError"
    assert report["timeseries_download_calls"] == 0
    assert report["historical_rows_read"] is False
    assert report["dbn_files_created"] == 0
    assert (root / REPORT_PATH).is_file()
    assert not (root / "data" / "dbn").exists()


@pytest.mark.parametrize("mode", ["nonzero_cost", "unexpected_field", "single_failure"])
def test_adversarial_provider_responses_fail_closed_without_retry(
    tmp_path: Path, mode: str,
) -> None:
    root = _copy_preflight_root(tmp_path)
    provider = FakeMetadataProvider(cost=1 if mode == "nonzero_cost" else 0)
    provider.unexpected_symbology_field = mode == "unexpected_field"
    provider.fail_once = mode == "single_failure"
    report = _run(root, provider)
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0
    if mode == "single_failure":
        assert sum(1 for name, _ in provider.calls if name == "get_cost") == 1
    assert not (root / "data" / "dbn").exists()


def test_provider_is_unreachable_without_exact_authorization(tmp_path: Path) -> None:
    root = _copy_preflight_root(tmp_path)
    provider = FakeMetadataProvider()
    wrong = OperationReceipt.issue_local(
        RepoBoundary(root), operation=OPERATION,
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    with pytest.raises(UnauthorizedOperation):
        execute_preflight(
            root=root, authorization=wrong, provider_factory=provider.capability,
            credential_source=CREDENTIAL_SOURCE,
            disk_usage=lambda _path: SimpleNamespace(free=10**12),
            environment_check=lambda _root: "synthetic-lock",
        )
    assert provider.calls == []
    assert not (root / REPORT_PATH).exists()


def test_credential_like_error_content_never_enters_report(tmp_path: Path) -> None:
    root = _copy_preflight_root(tmp_path)
    secret = "db-secret-value-must-not-appear"

    def factory() -> MetadataProviderApis:
        raise RuntimeError(secret)

    report = execute_preflight(
        root=root, authorization=_receipt(root), provider_factory=factory,
        credential_source=CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
        environment_check=lambda _root: "synthetic-lock",
    )
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert secret not in (root / REPORT_PATH).read_text(encoding="utf-8")


def test_existing_destination_and_insufficient_disk_fail_closed(tmp_path: Path) -> None:
    root = _copy_preflight_root(tmp_path / "collision")
    provider = FakeMetadataProvider()
    conflict = root / "data/dbn/definition/MES/2019/2019-05-06_2026-08-08.dbn.zst"
    conflict.parent.mkdir(parents=True)
    conflict.write_bytes(b"synthetic-conflict")
    report = _run(root, provider)
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["exception_type"] == "ContractError"

    low_root = _copy_preflight_root(tmp_path / "disk")
    low_report = execute_preflight(
        root=low_root, authorization=_receipt(low_root),
        provider_factory=FakeMetadataProvider().capability,
        credential_source=CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=1),
        environment_check=lambda _root: "synthetic-lock",
    )
    assert low_report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert low_report["exception_type"] == "ContractError"
