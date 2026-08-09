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
from futures_rebuild.micro_alpha_databento_preflight_v17 import (
    PLAN_PATH,
    PREDECESSOR_REPORT_ID,
    REPORT_PATH,
    _symbology_summary,
    _valid_opaque_group_key,
    build_plan,
    execute_preflight,
    load_predecessor_failure,
    required_scope,
    validate_plan,
)


pytestmark = [pytest.mark.current, pytest.mark.high_risk]
ROOT = Path(__file__).resolve().parents[1]


def _response(
    *,
    symbol: str = "MCL.FUT",
    stype_in: str = "parent",
    start: str = "2010-01-01",
    end: str = "2026-08-09",
    group_key: object = "PROVIDER_CONSTITUENT_1",
    entries: list[dict[str, object]] | None = None,
    partial: object = None,
) -> dict[str, object]:
    return {
        "result": {
            group_key: entries
            if entries is not None
            else [{"d0": "2021-07-12", "d1": end, "s": 123}]
        },
        "symbols": [symbol],
        "stype_in": stype_in,
        "stype_out": "instrument_id",
        "start_date": start,
        "end_date": end,
        "partial": ["OPAQUE"] if partial is None else partial,
        "not_found": [],
        "message": "Resolved successfully",
        "status": 200,
    }


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
    def __init__(self, *, bad_group_key: object | None = None, cost: object = 0) -> None:
        self.bad_group_key = bad_group_key
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
        start = str(kwargs["start_date"])
        end = str(kwargs["end_date"])
        effective = max(self.effective_dates[market], start)
        partial: object = ["OPAQUE"] if effective > start else []
        if start == self.effective_dates[market]:
            partial = ["OPAQUE_POST_EFFECTIVE_STATUS"]
        entries = [{"d0": effective, "d1": end, "s": 123}]
        if str(kwargs["stype_in"]) == "continuous" and start == effective:
            entries = [{"d0": "2009-12-31", "d1": "2099-01-01", "s": 123}]
        group_key: object = f"{market}_PROVIDER_GROUP"
        if market == "MCL" and start == "2010-01-01":
            group_key = "OPAQUE_NOT_MCL_PREFIX"
            if self.bad_group_key is not None:
                group_key = self.bad_group_key
        return _response(
            symbol=symbol,
            stype_in=str(kwargs["stype_in"]),
            start=start,
            end=end,
            group_key=group_key,
            entries=entries,
            partial=partial,
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


def _run(root: Path, provider: FakeMetadataProvider) -> dict[str, object]:
    return execute_preflight(
        root=root,
        authorization=_receipt(root),
        provider_factory=provider.capability,
        credential_source=CREDENTIAL_SOURCE,
        disk_usage=lambda _path: SimpleNamespace(free=10**12),
        environment_check=lambda _root: "synthetic-lock",
    )


def test_v17_plan_preserves_exact_scope_and_v16_failure() -> None:
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
    assert {request["schema"] for request in plan["requests"]} == {
        "definition",
        "status",
        "statistics",
        "ohlcv-1m",
        "ohlcv-1s",
    }
    assert plan["predecessor_execution"]["report_id"] == PREDECESSOR_REPORT_ID
    assert plan["limits"]["exact_provider_call_ceiling"] == 375
    assert plan["limits"]["maximum_external_cost_usd"] == "0"
    assert plan["forbidden"]["timeseries_download"] is True
    assert load_predecessor_failure(root=ROOT)["failure_code"] == "RESULT_GROUP_ROOT_DRIFT"


def test_foreign_root_group_key_is_bounded_by_exact_echo_without_recording() -> None:
    response = _response()
    summary = _symbology_summary(
        response,
        symbol="MCL.FUT",
        stype_in="parent",
        query_start="2010-01-01",
        end="2026-08-09",
        allow_discovery_partial=True,
    )
    assert summary["result_group_key_semantics"].startswith("BOUNDED_OPAQUE")
    assert summary["result_group_keys_bound_by_exact_response_echoes"] is True
    assert summary["market_root_prefix_assumed"] is False
    assert summary["result_group_keys_recorded"] is False


@pytest.mark.parametrize(
    "key",
    ["", " leading", "trailing ", "line\nbreak", "é", "X" * 129, 123],
)
def test_malformed_or_unbounded_group_keys_fail_closed(key: object) -> None:
    assert _valid_opaque_group_key(key) is False
    with pytest.raises(IntegrityError, match="group key shape"):
        _symbology_summary(
            _response(group_key=key),
            symbol="MCL.FUT",
            stype_in="parent",
            query_start="2010-01-01",
            end="2026-08-09",
            allow_discovery_partial=True,
        )


def test_exact_echo_and_interval_gates_remain_fail_closed() -> None:
    echo = _response()
    echo["symbols"] = ["OTHER.FUT"]
    with pytest.raises(IntegrityError, match="symbols echo drifted"):
        _symbology_summary(
            echo,
            symbol="MCL.FUT",
            stype_in="parent",
            query_start="2010-01-01",
            end="2026-08-09",
            allow_discovery_partial=True,
        )
    with pytest.raises(IntegrityError, match="wholly outside"):
        _symbology_summary(
            _response(entries=[{"d0": "2000-01-01", "d1": "2001-01-01", "s": 1}]),
            symbol="MCL.FUT",
            stype_in="parent",
            query_start="2010-01-01",
            end="2026-08-09",
            allow_discovery_partial=True,
        )


def test_v17_full_bounded_metadata_mechanics_pass(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    report = _run(root, FakeMetadataProvider())
    assert report["state"] == "PASS_METADATA_ONLY"
    assert report["provider_call_total"] <= 375
    assert report["external_cost_incurred_usd"] == "0"
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0
    assert report["historical_rows_read"] is False
    assert report["dbn_files_created"] == 0
    assert report["request_definition_count"] == 20
    assert (root / REPORT_PATH).is_file()
    discovery = report["symbology_summaries"]["MCL"]["discovery_parent"]
    assert discovery["market_root_prefix_assumed"] is False
    assert discovery["result_group_keys_recorded"] is False


def test_bad_provider_group_key_is_classified_without_value(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    report = _run(root, FakeMetadataProvider(bad_group_key="bad\nkey"))
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "RESULT_GROUP_KEY_SHAPE_DRIFT"
    assert report["failed_validation_field"] == "result"
    assert report["provider_error_message_recorded"] is False
    assert "symbology_summaries" not in report


def test_nonzero_cost_still_fails_without_retry_or_download(tmp_path: Path) -> None:
    root = _copy_root(tmp_path)
    report = _run(root, FakeMetadataProvider(cost="0.01"))
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["failure_code"] == "UNEXPECTED_NONZERO_COST"
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0
    assert report["dbn_files_created"] == 0


def test_v17_documentation_matches_execution_reality() -> None:
    outline = (ROOT / "PROJECT_OUTLINE.md").read_text(encoding="utf-8")
    folder_map = (ROOT / "PIPELINE_FOLDER_MAP.md").read_text(encoding="utf-8")
    assert "v18 preflight -> FAIL_CLOSED_METADATA_ONLY (13 calls" in outline
    assert "immutable v19 opaque-partial-semantic-safe successor" in outline
    assert "executed v21 timeout-safe successor" in " ".join(outline.split())
    assert "RETIRED / fail-closed discovery-partial-semantic evidence" in folder_map
    assert "v21 timeout-safe cumulative metadata successor" in folder_map
    assert "Authorization consumed" in folder_map
    assert "grants no download authority" in folder_map
