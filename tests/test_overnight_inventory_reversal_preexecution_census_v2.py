from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.overnight_inventory_reversal_execution import SessionObservation
from futures_rebuild import overnight_inventory_reversal_preexecution_census_v2 as census_v2


def test_parallel_worker_reuses_locked_parser_and_observation_builder(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_rows(*, market, path, audit):
        calls.append((market, path.name))
        audit.total_rows = 1
        yield "locked-record"

    expected = SessionObservation("ES", "2018-01-02", 0.01, (), 1, True, None)

    def fake_observations(*, market, source_records):
        assert market == "ES"
        assert list(source_records) == ["locked-record"]
        yield expected

    monkeypatch.setattr(census_v2, "iter_source_records_from_parquet_v10", fake_rows)
    monkeypatch.setattr(census_v2, "iter_ordered_session_observations", fake_observations)
    market, observations, audits = census_v2._read_market_task(
        ("ES", ((2018, "synthetic.parquet"),)),
    )
    assert market == "ES"
    assert observations == (expected,)
    assert audits["ES/2018"]["total_rows"] == 1
    assert calls == [("ES", "synthetic.parquet")]


def test_parallel_timeout_terminates_and_joins_every_worker(monkeypatch, tmp_path) -> None:
    events: list[str] = []

    class AsyncResult:
        def get(self, *, timeout):
            assert timeout == 10
            raise census_v2.multiprocessing.TimeoutError

    class Pool:
        def map_async(self, function, tasks, chunksize):
            assert function is census_v2._read_market_task
            assert len(tasks) == 4 and chunksize == 1
            return AsyncResult()

        def terminate(self):
            events.append("terminate")

        def join(self):
            events.append("join")

    class Context:
        def Pool(self, *, processes):
            assert processes == 4
            return Pool()

    monkeypatch.setattr(census_v2.multiprocessing, "get_context", lambda method: Context())
    paths = {
        (market, year): tmp_path / market / f"{year}.parquet"
        for market in census_v2.MARKETS for year in range(2018, 2023)
    }
    with pytest.raises(UnauthorizedOperation, match="worker deadline"):
        census_v2.collect_market_observations_parallel(
            paths=paths, maximum_workers=4, timeout_seconds=10,
        )
    assert events == ["terminate", "join"]


def test_parallel_adapter_has_no_economic_evaluator() -> None:
    source = Path(census_v2.__file__).read_text(encoding="utf-8")
    assert "evaluate_fixed_trial" not in source
    assert "_net_for_direction" not in source
    assert "portfolio_net_pnl" not in source


def test_worker_timeout_reserves_time_inside_total_runtime() -> None:
    assert census_v2._bounded_worker_timeout(
        configured_timeout=780, maximum_runtime=900, elapsed_seconds=15.9,
    ) == 780
    assert census_v2._bounded_worker_timeout(
        configured_timeout=780, maximum_runtime=900, elapsed_seconds=200.1,
    ) == 670
    with pytest.raises(UnauthorizedOperation, match="total runtime"):
        census_v2._bounded_worker_timeout(
            configured_timeout=780, maximum_runtime=900, elapsed_seconds=870,
        )


def test_parallel_successor_plan_is_hash_bound_and_preserves_consumed_attempt() -> None:
    root = Path(__file__).resolve().parents[1]
    plan = census_v2.load_census_v2_plan(root=root)
    assert plan["historical_economics_evaluation"] is False
    assert plan["model_fit"] is False
    assert plan["prediction_generation"] is False
    assert plan["holdout_2025_access"] is False
    assert plan["provider_or_network_access"] is False
    assert plan["failed_predecessor"]["authorization_consumed"] is True
    assert plan["failed_predecessor"]["retry_under_predecessor_plan_authorized"] is False
    assert plan["limits"] == {
        "maximum_attempts": 1,
        "maximum_retries": 0,
        "maximum_runtime_seconds": 900,
        "worker_pool_timeout_seconds": 780,
        "maximum_workers": 4,
        "maximum_external_cost_usd": "0",
    }


def test_completed_census_and_unpublished_clarification_are_hash_bound() -> None:
    root = Path(__file__).resolve().parents[1]
    trial_id = "24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c"
    report_path = (
        root / "state/unpublished_evidence/overnight_inventory_reversal_fold_readiness_v2"
        / trial_id / census_v2.OUTPUT_FILENAME
    )
    clarification_path = (
        root / "state/unpublished_evidence/overnight_inventory_reversal"
        / trial_id / "terminal_closure_clarification.json"
    )
    event_path = clarification_path.with_name("terminal_closure_clarification_event.json")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    clarification = json.loads(clarification_path.read_text(encoding="utf-8"))
    event = json.loads(event_path.read_text(encoding="utf-8"))
    report_core = dict(report)
    assert report_core.pop("report_id") == sha256_json(report_core)
    clarification_core = dict(clarification)
    assert clarification_core.pop("clarification_id") == sha256_json(clarification_core)
    event_core = dict(event)
    assert event_core.pop("event_id") == sha256_json(event_core)
    assert clarification["row_certified_readiness_evidence"]["sha256"] == sha256_file(report_path)
    assert clarification["row_certified_readiness_evidence"]["report_id"] == report["report_id"]
    assert clarification["terminal_disposition"] == "INCONCLUSIVE_DATA_OR_COVERAGE"
    assert clarification["audit_classification"]["strategy_failure_proven"] is False
    assert clarification["audit_classification"]["economic_evaluation_occurred"] is False
    assert event["clarification_sha256"] == sha256_file(clarification_path)
    assert event["publication_authorized"] is False
    certificate = report["fold_readiness_certificate"]
    certificate_core = dict(certificate)
    assert certificate_core.pop("certificate_id") == sha256_json(certificate_core)
    assert certificate["overall_decision"] == "FAIL"
    assert all(item["status"] == "FAIL" for item in certificate["fold_market_results"])
