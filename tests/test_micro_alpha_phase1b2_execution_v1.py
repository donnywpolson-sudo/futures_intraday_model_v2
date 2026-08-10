from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from futures_rebuild import micro_alpha_phase1b2_execution as execution
from futures_rebuild.errors import IntegrityError
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.micro_alpha_phase1b2_decoder import DecodeResult
from futures_rebuild.micro_alpha_phase1b2_decoder import CreatedByteBudget
from scripts import prepare_apex_micro_phase1b2_execution_v1 as prepare_script
from futures_rebuild.research_gateway_policy import (
    PREPARATORY_REAL_HISTORY_OPERATIONS,
    require_current_real_history_operation,
)


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def _result(
    schema: str,
    *,
    rows: int = 1,
    instruments: tuple[int, ...] = (11,),
    economics: tuple[tuple[int, int | None, int | None, str], ...] = (),
    duplicates: int = 0,
    ambiguity: int = 0,
) -> DecodeResult:
    return DecodeResult(
        schema=schema,
        row_count=rows,
        output_path="C:/inactive/result.parquet" if rows else None,
        output_sha256="a" * 64 if rows else None,
        output_bytes=1 if rows else 0,
        duplicate_count=duplicates,
        ambiguous_identity_count=ambiguity,
        null_field_count=0,
        roll_transition_count=0,
        non_contiguous_instrument_count=0,
        roll_sequence=instruments if schema in {"ohlcv-1m", "ohlcv-1s"} else (),
        instrument_ids=instruments,
        economics=economics,
    )


def _accepted_results() -> dict[str, DecodeResult]:
    tick, quantity, currency = execution._expected_economics("MES")
    return {
        schema: _result(
            schema,
            economics=((11, tick, quantity, currency),) if schema == "definition" else (),
        )
        for schema in execution.SCHEMAS
    }


def test_live_plan_preview_is_exact_and_source_safe() -> None:
    plan = execution.build_execution_plan(
        root=ROOT, implementation_head=execution._git_head(ROOT)
    )
    assert plan["state"] == "PREPARED_REQUIRES_SEPARATE_HISTORICAL_ROW_CONFIRMATION"
    assert plan["source_count"] == 120
    assert plan["source_bytes"] == 1_232_883_585
    assert plan["coverage_cell_count"] == 140
    assert plan["prelaunch_cell_count"] == 20
    assert plan["interval_count"] == 24
    assert {item["year"] for item in plan["sources"]} == set(range(2018, 2025))
    assert all("/2025/" not in item["dbn_path"] and "/2026/" not in item["dbn_path"] for item in plan["sources"])
    assert plan["limits"] == {
        "maximum_attempts": 1,
        "maximum_retries": 0,
        "maximum_workers": 2,
        "batch_rows": 100_000,
        "maximum_runtime_seconds": 43_200,
        "maximum_output_bytes": 64 * 1024**3,
        "required_free_disk_bytes": 80 * 1024**3,
        "maximum_parquet_outputs": 144,
        "provider_calls": 0,
        "external_cost_usd": "0",
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("query.stype_in", "raw_symbol"),
        ("query.symbols", ["MES.FUT"]),
        ("dbn_destination", "data/dbn/ohlcv-1m/MES/2024/wrong.dbn.zst"),
    ],
)
def test_annual_contract_rejects_symbology_and_path_drift(
    field: str, value: object
) -> None:
    item = {
        "market": "MES",
        "schema": "ohlcv-1m",
        "year": 2024,
        "query": {
            "dataset": "GLBX.MDP3",
            "end": "2025-01-01",
            "schema": "ohlcv-1m",
            "start": "2024-01-01",
            "stype_in": "continuous",
            "stype_out": "instrument_id",
            "symbols": ["MES.v.0"],
        },
        "dbn_destination": "data/dbn/ohlcv_1m/MES/2024/2024-01-01_2025-01-01.dbn.zst",
        "sidecar_destination": "data/dbn/ohlcv_1m/MES/2024/2024-01-01_2025-01-01.dbn.zst.manifest.json",
    }
    if field.startswith("query."):
        item["query"][field.split(".", 1)[1]] = value
    else:
        item[field] = value
    with pytest.raises(IntegrityError, match="annual"):
        execution._validate_annual_source_contract(
            item=item, product_effective_date="2019-05-05"
        )


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        (lambda value: value.pop("status"), "SOURCE_UNAVAILABLE"),
        (lambda value: value.__setitem__("status", _result("status", rows=0)), "MISSING"),
        (lambda value: value.__setitem__("statistics", _result("statistics", duplicates=1)), "DUPLICATE"),
        (lambda value: value.__setitem__("ohlcv-1s", _result("ohlcv-1s", ambiguity=1)), "AMBIGUOUS_ROLL"),
        (lambda value: value.__setitem__("ohlcv-1m", _result("ohlcv-1m", instruments=(99,))), "AMBIGUOUS_IDENTITY"),
    ],
)
def test_explicit_fail_closed_group_dispositions(mutation, expected: str) -> None:
    results = _accepted_results()
    mutation(results)
    assert execution._group_disposition(market="MES", results=results) == (expected, False)


def test_accepted_group_requires_exact_identity_and_economics() -> None:
    assert execution._group_disposition(
        market="MES", results=_accepted_results()
    ) == ("ACCEPTED", True)
    drifted = _accepted_results()
    drifted["definition"] = _result(
        "definition", economics=((11, 1, 1, "USD"),)
    )
    assert execution._group_disposition(
        market="MES", results=drifted
    ) == ("AMBIGUOUS_IDENTITY", False)


def test_interval_receipt_binds_exact_source_query_and_release(tmp_path: Path) -> None:
    output = tmp_path / "inactive" / "bars.parquet"
    output.parent.mkdir()
    output.write_bytes(b"synthetic-parquet-receipt-fixture")
    result = _result("ohlcv-1m")
    result = DecodeResult(
        **{
            **result.__dict__,
            "output_path": output.as_posix(),
            "output_sha256": "a" * 64,
            "output_bytes": output.stat().st_size,
        }
    )
    item = {
        "request_id": "b" * 64,
        "market": "MES",
        "schema": "ohlcv-1m",
        "year": 2024,
        "interval": "2024-01-01_2025-01-01",
        "source_sha256": "c" * 64,
        "source_bytes": 123,
        "sidecar_sha256": "d" * 64,
        "sidecar_manifest_id": "e" * 64,
        "exact_query": {"dataset": "GLBX.MDP3", "schema": "ohlcv-1m"},
        "phase1b_release_id": "f" * 64,
    }
    receipt = execution._serialize_result(tmp_path, item=item, result=result)
    assert receipt["schema_version"] == execution.INTERVAL_RECEIPT_SCHEMA
    assert receipt["request_id"] == item["request_id"]
    assert receipt["source_sha256"] == item["source_sha256"]
    assert receipt["sidecar_manifest_id"] == item["sidecar_manifest_id"]
    assert receipt["phase1b_release_id"] == item["phase1b_release_id"]
    assert receipt["exact_query_sha256"] == execution.sha256_json(
        item["exact_query"]
    )
    assert receipt["output_path"] == "inactive/bars.parquet"


def test_executor_source_has_no_provider_network_credential_or_activation_surface() -> None:
    source = inspect.getsource(execution)
    forbidden = (
        "import requests", "import urllib", "import databento", "Historical(",
        "api.env", "data/active/catalogs/apex_micro.json\", \"w", "git add",
        "git commit", "git push",
    )
    assert not any(token.lower() in source.lower() for token in forbidden)
    assert execution.ACTIVE_MICRO_CATALOG_PATH.as_posix() not in {
        execution.PLAN_PATH.as_posix(), execution.AUDIT_PATH.as_posix()
    }
    prepare_source = inspect.getsource(prepare_script)
    assert "execute_authorized_phase1b2" not in prepare_source


def test_exact_operation_crosses_central_preparatory_gate_only() -> None:
    assert execution.OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS
    require_current_real_history_operation(execution.OPERATION, {})
    with pytest.raises(UnauthorizedOperation, match="certified gateway"):
        require_current_real_history_operation(f"{execution.OPERATION}_ALIAS", {})


def test_plan_freeze_refuses_uncommitted_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan_existed_before = (ROOT / execution.PLAN_PATH).exists()
    plan_sha_before = (
        execution.sha256_file(ROOT / execution.PLAN_PATH)
        if plan_existed_before
        else None
    )
    monkeypatch.setattr(
        prepare_script.subprocess,
        "run",
        lambda *args, **kwargs: type("Result", (), {"returncode": 1})(),
    )
    with pytest.raises(SystemExit, match="committed HEAD"):
        prepare_script._require_committed_implementation()
    assert (ROOT / execution.PLAN_PATH).exists() is plan_existed_before
    if plan_existed_before:
        assert execution.sha256_file(ROOT / execution.PLAN_PATH) == plan_sha_before


def test_plan_scope_freezes_execution_and_excludes_holdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = execution.build_execution_plan(
        root=ROOT, implementation_head=execution._git_head(ROOT)
    )
    original_sha256_file = execution.sha256_file
    monkeypatch.setattr(
        execution,
        "sha256_file",
        lambda path: "f" * 64
        if Path(path) == ROOT / execution.PLAN_PATH
        else original_sha256_file(Path(path)),
    )
    scope = execution.required_scope(root=ROOT, plan=plan)
    assert scope["eligible_years"] == "2018-2024"
    assert scope["excluded_payload_years"] == "2025,2026"
    assert scope["exact_source_count"] == "120"
    assert scope["exact_source_bytes"] == "1232883585"
    assert scope["maximum_workers"] == "2"
    assert scope["provider_calls"] == "0"
    assert scope["external_cost_usd"] == "0"


def test_output_families_are_inactive_and_do_not_collide() -> None:
    plan = execution.build_execution_plan(
        root=ROOT, implementation_head=execution._git_head(ROOT)
    )
    outputs = [item["phase1b_output_path"] for item in plan["sources"]]
    outputs += [item["phase2_output_path"] for item in plan["phase2"]]
    assert len(outputs) == len(set(outputs)) == 144
    assert not any(path.startswith("data/active/") for path in outputs)
    assert not (ROOT / plan["staging_root"]).exists()
    assert not (ROOT / plan["evidence_root"]).exists()


def test_worker_stops_scheduling_after_first_failure(tmp_path: Path) -> None:
    root = tmp_path.resolve()
    (root / "data" / "dbn" / "ohlcv_1m" / "MES" / "2024").mkdir(parents=True)
    source = root / "data/dbn/ohlcv_1m/MES/2024/source.dbn.zst"
    source.write_bytes(b"synthetic")
    items = tuple(
        {
            "request_id": str(index),
            "dbn_path": source.relative_to(root).as_posix(),
            "phase1b_output_path": f"out/{index}.parquet",
            "market": "MES",
            "schema": "ohlcv-1m",
            "exact_query": {},
            "source_sha256": "a" * 64,
        }
        for index in range(3)
    )
    calls: list[str] = []

    def fail_first(**kwargs):
        calls.append(Path(kwargs["output_path"]).name)
        raise IntegrityError("synthetic failure")

    result = execution._decode_worker(
        root=root,
        staging=root / "inactive",
        items=items,
        stop=execution.threading.Event(),
        decode_one=fail_first,
        started=0.0,
        clock=lambda: 1.0,
        created_byte_budget=CreatedByteBudget(1024),
    )
    assert result.failure_type == "IntegrityError"
    assert calls == ["0.parquet"]
