from __future__ import annotations

import json
import hashlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from futures_rebuild import bounded_2025_acquisition as acquisition
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
    _personal_approval_line,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.micro_alpha_acquisition import DownloadProviderApis
from futures_rebuild.research_gateway_policy import (
    PREPARATORY_REAL_HISTORY_OPERATIONS,
    require_current_real_history_operation,
)


ROOTS = [
    "ES", "NQ", "RTY", "YM", "CL", "NG", "RB", "HO", "GC", "SI", "HG",
    "PL", "SR3", "SR1", "ZQ", "TN", "ZT", "ZF", "ZN", "ZB", "UB", "6A",
    "6B", "6C", "6E", "6J", "6M", "6N", "6S", "ZC", "ZS", "ZL", "ZM",
    "ZW", "KE", "LE", "HE", "GF", "BTC", "ETH", "PA",
]
MICROS = [
    "M2K", "M6A", "M6B", "M6E", "MBT", "MCD", "MCL", "MES", "MET",
    "MGC", "MHG", "MJY", "MNG", "MNQ", "MSF", "MYM", "SIL",
]


def test_exact_acquisition_operation_is_allowed_and_aliases_fail_closed() -> None:
    assert acquisition.OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS
    require_current_real_history_operation(acquisition.OPERATION, {})

    for operation in (
        f"{acquisition.OPERATION}_V2",
        acquisition.OPERATION.removesuffix("_ONCE"),
        "ACQUIRE_BOUNDED_2025_DEVELOPMENT_DBN",
    ):
        with pytest.raises(UnauthorizedOperation, match="retired outside"):
            require_current_real_history_operation(operation, {})


def _write_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(value) + b"\n")


def _synthetic_repository(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path
    source = {
        "contract_id": "47ad7a1c100bec86494f3c1eb1e78ba56a4d35c6be993da6ded8e2e7f925823f",
        "active_canonical_source": {
            "release_id": "9867aedac9cfe732d015489fc4093ffc4aaab5ad698b75a5fa00ca7e1f457995"
        },
        "universe": {"standard_roots": ROOTS, "deferred_micro_roots": MICROS},
    }
    boundary = {
        "development_2025_boundary_assessment_v2_id": "aa731fa0721ce566eae7820cede12940d6e55ebbd728e191e1a40305604a3eb1",
        "development_start_inclusive": acquisition.DEVELOPMENT_START,
        "development_end_exclusive": acquisition.DEVELOPMENT_END_EXCLUSIVE,
        "required_exact_successor": {"root_family_pairs": 287},
        "source_authority": {
            "causal_contract_id": "a11f587644168555d23042b945799b16947723203e5a592af6451027d301bdc7"
        },
    }
    benchmark = {
        "parquet_benchmark_v2_id": "6fa42a5690a000ec653c86fbf0e0b0df403d4bc000f204b12abee8e80305a4d4",
        "projection": {
            "proposed_output_ceiling_bytes": 19_100_000_000,
            "proposed_peak_incremental_ceiling_bytes": 21_100_000_000,
        },
    }
    _write_json(root / acquisition.SOURCE_CONTRACT_PATH, source)
    _write_json(root / acquisition.BOUNDARY_ASSESSMENT_PATH, boundary)
    _write_json(root / acquisition.PARQUET_BENCHMARK_PATH, benchmark)
    for market in ROOTS:
        for family in acquisition.FAMILIES:
            schema = family.replace("_", "-")
            dbn = (
                root / "data/dbn" / family / market / "2025"
                / "2025-01-01_2026-01-01.dbn.zst"
            )
            dbn.parent.mkdir(parents=True, exist_ok=True)
            dbn.write_bytes(b"opaque-not-opened")
            sidecar = Path(f"{dbn}.manifest.json")
            _write_json(
                sidecar,
                {
                    "vendor": "databento",
                    "dataset": "GLBX.MDP3",
                    "schema": schema,
                    "market": market,
                    "symbols_requested": [
                        f"{market}.FUT" if family == "definition" else f"{market}.v.0"
                    ],
                    "start": "2025-01-01",
                    "end": "2026-01-01",
                    "stype_in": "parent" if family == "definition" else "continuous",
                    "stype_out": "instrument_id",
                    "encoding": "dbn",
                    "compression": "zstd",
                    "file_size_bytes": dbn.stat().st_size,
                    "file_sha256": "a" * 64,
                },
            )
    monkeypatch.setattr(acquisition, "_git_head", lambda _root: "b" * 40)
    _seed_resume_evidence(root)
    return root


def _seed_resume_evidence(root: Path) -> None:
    fresh = acquisition.build_fresh_acquisition_plan(root=root)
    heavy = [item for item in fresh["requests"] if item["family"] == "ohlcv_1s"]
    light = [
        item
        for item in fresh["requests"]
        if item["family"] not in {"definition", "ohlcv_1s"}
    ]
    reusable = heavy[:37] + light[:28]
    records = []
    for item in reusable:
        request_id = str(item["request_id"])
        worker = "heavy_ohlcv_1s" if item["family"] == "ohlcv_1s" else "light_families"
        dbn = root / acquisition.V5_ATTEMPT / worker / f"{request_id}.dbn.zst"
        sidecar = root / acquisition.V5_ATTEMPT / worker / f"{request_id}.manifest.json"
        dbn.parent.mkdir(parents=True, exist_ok=True)
        payload = f"opaque-{request_id}".encode()
        dbn.write_bytes(payload)
        digest = sha256_file(dbn)
        sidecar_body = {
            "schema_version": "bounded_2025_provider_staging_sidecar/1.0.0",
            "state": "INACTIVE_UNREGISTERED_PROVIDER_STAGING",
            "request_id": request_id,
            "market": item["market"],
            "family": item["family"],
            "exact_query": item["query"],
            "byte_count": len(payload),
            "sha256": digest,
            "canonical_destination": item["canonical_destination"],
            "canonical_sidecar_destination": item["canonical_sidecar_destination"],
            "dbn_rows_decoded": 0,
            "holdout_or_forward_access": False,
            "registered": False,
            "published": False,
            "activated": False,
        }
        _write_json(
            sidecar, {**sidecar_body, "manifest_id": sha256_json(sidecar_body)}
        )
        records.append(
            {
                "request_id": request_id,
                "staging_dbn": dbn.relative_to(root / acquisition.V5_ATTEMPT).as_posix(),
                "staging_sidecar": sidecar.relative_to(
                    root / acquisition.V5_ATTEMPT
                ).as_posix(),
                "byte_count": len(payload),
                "sha256": digest,
            }
        )
    for attempt, completed in (
        (acquisition.V5_ATTEMPT, records),
        (acquisition.V6_ATTEMPT, []),
    ):
        terminal_body = {
            "state": "FAILURE_INACTIVE_EVIDENCE_PRESERVED",
            "completed_records": completed,
            "dbn_rows_decoded": 0,
            "canonical_registration": False,
            "publication": False,
            "activation": False,
        }
        _write_json(
            root / attempt / "terminal.json",
            {**terminal_body, "terminal_id": sha256_json(terminal_body)},
        )
    _write_json(
        root / acquisition.V6_FAILURE_AUDIT_PATH,
        {
            "failed_execution_audit_v6_id": (
                "1139fc8ec916b7b320d70d56c1b695bcdd412091f8d7aeaf01b4973263caebfc"
            ),
            "checks_passed": 23,
            "checks_failed": 0,
            "cross_attempt_reconciliation": {
                "reusable_non_definition_requests": 65
            },
        },
    )
    journal = root / acquisition.V5_ATTEMPT / "batch_job_journal.jsonl"
    journal.parent.mkdir(parents=True, exist_ok=True)
    gf_request_id = next(
        str(item["request_id"])
        for item in fresh["requests"]
        if item["market"] == "GF" and item["family"] == "ohlcv_1s"
    )
    journal.write_bytes(
        canonical_bytes(
            {
                "event": "BATCH_JOB_SUBMITTED",
                "request_id": gf_request_id,
                "job_id": "GLBX-SYNTHETIC-JOB",
            }
        )
        + b"\n"
    )


def test_plan_is_exact_source_safe_and_two_worker_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _synthetic_repository(tmp_path, monkeypatch)
    original_open = Path.open

    def reject_dbn_open(path: Path, *args: object, **kwargs: object):
        if str(path).endswith(".dbn.zst"):
            raise AssertionError("planner opened a DBN payload")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", reject_dbn_open)
    plan = acquisition.build_fresh_acquisition_plan(root=root)
    assert plan["counts"] == {
        "requests": 287,
        "expected_dbns": 287,
        "expected_sidecars": 287,
        "heavy_requests": 41,
        "light_requests": 246,
    }
    assert plan["worker_contract"]["maximum_parallel_downloads"] == 2
    assert plan["worker_contract"]["maximum_concurrent_ohlcv_1s"] == 1
    assert plan["worker_contract"]["whole_request_resubmission_retries"] == 0
    assert plan["worker_contract"]["batch_submission_retries"] == 0
    assert plan["worker_contract"]["maximum_batch_get_attempts"] == 6
    assert plan["worker_contract"]["maximum_batch_poll_attempts_per_job"] == 240
    assert plan["worker_contract"]["maximum_batch_job_seconds"] == 3_600.0
    assert plan["limits"]["maximum_batch_internal_calls"] == 278_677
    assert plan["limits"]["maximum_provider_calls"] == 278_964
    assert {item["family"] for item in plan["requests"]} == set(acquisition.FAMILIES)
    assert {item["market"] for item in plan["requests"]} == set(ROOTS)
    assert all(
        item["query"]["end"] == acquisition.DEVELOPMENT_END_EXCLUSIVE
        for item in plan["requests"]
    )
    assert plan["custody"]["canonical_registration_during_acquisition"] is False
    assert plan["custody"]["raw_annual_2025_and_2026_preserved"] is True


def test_plan_rejects_destination_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _synthetic_repository(tmp_path, monkeypatch)
    collision = (
        root / "data/dbn/definition/ES/2025"
        / f"{acquisition.CANONICAL_INTERVAL_NAME}.dbn.zst"
    )
    collision.write_bytes(b"unapproved-existing")
    with pytest.raises(IntegrityError, match="destination already exists"):
        acquisition.build_acquisition_plan(root=root)


def test_resume_plan_references_65_and_downloads_only_222(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _synthetic_repository(tmp_path, monkeypatch)
    original_open = Path.open

    def reject_dbn_open(path: Path, *args: object, **kwargs: object):
        if str(path).endswith(".dbn.zst"):
            raise AssertionError("resume planner opened a DBN payload")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", reject_dbn_open)
    plan = acquisition.build_acquisition_plan(root=root)
    assert plan["counts"] == {
        "complete_requests": 287,
        "reused_non_definition_requests": 65,
        "network_requests": 222,
        "network_definitions": 41,
        "network_heavy_requests": 4,
        "expected_dbns": 287,
        "expected_sidecars": 287,
    }
    assert plan["limits"]["reused_payload_bytes_additional_disk"] == 0
    assert all(item["family"] != "definition" for item in plan["reuse_records"])
    assert plan["resumable_provider_jobs"][0]["request_id"] in {
        item["request_id"]
        for item in plan["network_requests"]
        if item["market"] == "GF" and item["family"] == "ohlcv_1s"
    }


class _ConcurrencyProbe:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.heavy_active = 0
        self.max_heavy = 0

    def provider(self) -> DownloadProviderApis:
        def get_range(**kwargs: object) -> None:
            path = Path(str(kwargs["path"]))
            heavy = kwargs["schema"] == "ohlcv-1s"
            with self.lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                if heavy:
                    self.heavy_active += 1
                    self.max_heavy = max(self.max_heavy, self.heavy_active)
            time.sleep(0.005)
            path.write_bytes(b"opaque-provider-response")
            with self.lock:
                self.active -= 1
                if heavy:
                    self.heavy_active -= 1

        return DownloadProviderApis(get_cost=lambda **_kwargs: 0, get_range=get_range)


def _item(name: str, family: str) -> dict[str, object]:
    return {
        "request_id": name * 64,
        "market": "ES",
        "family": family,
        "query": {
            "dataset": "GLBX.MDP3",
            "symbols": ["ES.v.0"],
            "schema": family.replace("_", "-"),
            "start": "x",
            "end": "y",
            "stype_in": "continuous",
            "stype_out": "instrument_id",
            "encoding": "dbn",
            "compression": "zstd",
        },
        "request_timeout_seconds": 900,
        "request_byte_ceiling": 1000,
        "canonical_destination": f"data/dbn/{family}/ES/result.dbn.zst",
        "canonical_sidecar_destination": f"data/dbn/{family}/ES/result.dbn.zst.manifest.json",
    }


def test_workers_are_disjoint_bounded_and_finalize_only_staging(tmp_path: Path) -> None:
    probe = _ConcurrencyProbe()
    stop = threading.Event()
    total = {"bytes": 0}
    lock = threading.Lock()
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=2) as executor:
        heavy = executor.submit(
            acquisition._download_worker,
            root=tmp_path,
            worker_name="heavy",
            items=[_item("a", "ohlcv_1s"), _item("b", "ohlcv_1s")],
            provider_factory=probe.provider,
            stop_event=stop,
            total_state=total,
            total_lock=lock,
            maximum_total_bytes=10_000,
            started=started,
            clock=time.monotonic,
        )
        light = executor.submit(
            acquisition._download_worker,
            root=tmp_path,
            worker_name="light",
            items=[_item("c", "definition"), _item("d", "ohlcv_1m")],
            provider_factory=probe.provider,
            stop_event=stop,
            total_state=total,
            total_lock=lock,
            maximum_total_bytes=10_000,
            started=started,
            clock=time.monotonic,
        )
        results = [heavy.result(), light.result()]
    assert all(result.failure_type is None for result in results)
    assert sum(len(result.records) for result in results) == 4
    assert probe.max_active == 2
    assert probe.max_heavy == 1
    assert not list(tmp_path.rglob("*.partial"))
    assert not (tmp_path / "data").exists()


def test_provider_queries_match_strict_databento_method_signatures() -> None:
    item = {
        **_item("a", "ohlcv_1m"),
        "query": {
            "dataset": "GLBX.MDP3",
            "symbols": ["ES.v.0"],
            "schema": "ohlcv-1m",
            "start": acquisition.DEVELOPMENT_START,
            "end": acquisition.DEVELOPMENT_END_EXCLUSIVE,
            "stype_in": "continuous",
            "stype_out": "instrument_id",
            "encoding": "dbn",
            "compression": "zstd",
        },
    }
    observed: list[tuple[str, dict[str, object]]] = []

    def strict_get_cost(
        *, dataset: object, symbols: object, schema: object, start: object,
        end: object, stype_in: object,
    ) -> int:
        observed.append(("cost", locals()))
        return 0

    def strict_get_range(
        *, dataset: object, symbols: object, schema: object, start: object,
        end: object, stype_in: object, stype_out: object, path: object,
    ) -> None:
        observed.append(("range", locals()))

    assert strict_get_cost(**acquisition._provider_cost_query(item)) == 0
    strict_get_range(
        **acquisition._provider_range_query(item), path="candidate.dbn.zst.partial"
    )
    assert [name for name, _ in observed] == ["cost", "range"]
    assert "stype_out" not in observed[0][1]
    assert "encoding" not in observed[0][1]
    assert "compression" not in observed[0][1]
    assert observed[1][1]["stype_out"] == "instrument_id"
    assert "encoding" not in observed[1][1]
    assert "compression" not in observed[1][1]


def test_provider_query_filter_rejects_archival_contract_drift() -> None:
    item = {
        **_item("a", "ohlcv_1m"),
        "query": {
            "dataset": "GLBX.MDP3",
            "symbols": ["ES.v.0"],
            "schema": "ohlcv-1m",
            "start": acquisition.DEVELOPMENT_START,
            "end": acquisition.DEVELOPMENT_END_EXCLUSIVE,
            "stype_in": "continuous",
            "stype_out": "instrument_id",
            "encoding": "dbn",
            "compression": "zstd",
            "unexpected": True,
        },
    }
    with pytest.raises(IntegrityError, match="query contract drifted"):
        acquisition._provider_cost_query(item)


class _BatchFixture:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.submissions: list[dict[str, object]] = []

    def submit_job(self, **kwargs: object) -> dict[str, object]:
        self.submissions.append(kwargs)
        return {"id": "job-1", "cost_usd": "0"}

    def list_jobs(self, **_kwargs: object) -> list[dict[str, object]]:
        return [{"id": "job-1", "state": "done"}]

    def list_files(self, **_kwargs: object) -> list[dict[str, object]]:
        digest = hashlib.sha256(self.payload).hexdigest()
        return [{
            "filename": "bounded.dbn.zst",
            "size": len(self.payload),
            "hash": f"sha256:{digest}",
            "urls": {"https": "https://example.invalid/bounded.dbn.zst"},
        }]


class _DelayedBatchFixture(_BatchFixture):
    def __init__(self, payload: bytes, *, done_on_poll: int) -> None:
        super().__init__(payload)
        self.done_on_poll = done_on_poll
        self.polls = 0

    def list_jobs(self, **_kwargs: object) -> list[dict[str, object]]:
        self.polls += 1
        state = "done" if self.polls >= self.done_on_poll else "processing"
        return [{"id": "job-1", "state": state}]


class _StreamResponse:
    def __init__(
        self,
        *,
        chunks: list[bytes],
        status_code: int,
        headers: dict[str, str] | None = None,
        fail_after_first: bool = False,
    ) -> None:
        self._chunks = chunks
        self.status_code = status_code
        self.headers = headers or {}
        self._fail_after_first = fail_after_first

    def __enter__(self):
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def iter_content(self, **_kwargs: object):
        for index, chunk in enumerate(self._chunks):
            yield chunk
            if self._fail_after_first and index == 0:
                raise ConnectionError("synthetic interrupted transfer")


def test_batch_transport_is_manifest_bound_and_resumes_interrupted_get(
    tmp_path: Path,
) -> None:
    payload = b"opaque-synthetic-dbn"
    batch = _BatchFixture(payload)
    calls: list[dict[str, object]] = []

    def http_get(**kwargs: object) -> _StreamResponse:
        calls.append(kwargs)
        if len(calls) == 1:
            return _StreamResponse(
                chunks=[payload[:6]], status_code=200, fail_after_first=True
            )
        return _StreamResponse(
            chunks=[payload[6:]],
            status_code=206,
            headers={"Content-Range": f"bytes 6-{len(payload) - 1}/{len(payload)}"},
        )

    counter = acquisition._BatchCallCounter(20)
    output = tmp_path / "candidate.dbn.zst.partial"
    adapter = acquisition._BatchRangeAdapter(
        batch=batch,
        api_key="not-recorded",
        journal_path=tmp_path / "journal.jsonl",
        counter=counter,
        http_get=http_get,
        sleeper=lambda _seconds: None,
    )
    adapter.get_range(
        **acquisition._provider_range_query(_item("a", "definition")),
        path=str(output),
        request_id="a" * 64,
        request_byte_ceiling=1000,
    )
    assert output.read_bytes() == payload
    assert len(calls) == 2
    assert calls[0]["headers"] == {}
    assert calls[1]["headers"] == {"Range": f"bytes=6-{len(payload) - 1}"}
    assert batch.submissions[0]["encoding"] == "dbn"
    assert batch.submissions[0]["compression"] == "zstd"
    assert batch.submissions[0]["split_duration"] == "none"
    assert "request_id" not in batch.submissions[0]
    assert "request_byte_ceiling" not in batch.submissions[0]
    assert counter.snapshot() == {
        "batch_submit_job": 1,
        "batch_list_jobs": 1,
        "batch_list_files": 1,
        "batch_download_get": 2,
    }


def test_batch_transport_waits_beyond_old_fifteen_minute_ceiling(
    tmp_path: Path,
) -> None:
    payload = b"opaque-synthetic-dbn"
    batch = _DelayedBatchFixture(payload, done_on_poll=62)
    elapsed = [0.0]

    def advance(seconds: float) -> None:
        elapsed[0] += seconds

    adapter = acquisition._BatchRangeAdapter(
        batch=batch,
        api_key="not-recorded",
        journal_path=tmp_path / "journal.jsonl",
        counter=acquisition._BatchCallCounter(100),
        http_get=lambda **_kwargs: _StreamResponse(
            chunks=[payload], status_code=200
        ),
        clock=lambda: elapsed[0],
        sleeper=advance,
    )
    output = tmp_path / "candidate.dbn.zst.partial"
    adapter.get_range(
        **acquisition._provider_range_query(_item("a", "ohlcv_1s")),
        path=str(output),
        request_id="a" * 64,
        request_byte_ceiling=1000,
    )
    assert output.read_bytes() == payload
    assert batch.polls == 62
    assert elapsed[0] == 915.0


def test_batch_transport_remains_bounded_at_one_hour(tmp_path: Path) -> None:
    payload = b"opaque-synthetic-dbn"
    batch = _DelayedBatchFixture(payload, done_on_poll=999)
    elapsed = [0.0]

    adapter = acquisition._BatchRangeAdapter(
        batch=batch,
        api_key="not-recorded",
        journal_path=tmp_path / "journal.jsonl",
        counter=acquisition._BatchCallCounter(1000),
        clock=lambda: elapsed[0],
        sleeper=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds),
    )
    with pytest.raises(UnauthorizedOperation, match="poll-attempt ceiling"):
        adapter.get_range(
            **acquisition._provider_range_query(_item("a", "ohlcv_1s")),
            path=str(tmp_path / "candidate.dbn.zst.partial"),
            request_id="a" * 64,
            request_byte_ceiling=1000,
        )
    assert batch.polls == 240
    assert elapsed[0] == 3_600.0


def test_batch_transport_rejects_provider_size_before_download(tmp_path: Path) -> None:
    payload = b"larger-than-bound"
    batch = _BatchFixture(payload)
    called = False

    def http_get(**_kwargs: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("oversized provider file must not be downloaded")

    adapter = acquisition._BatchRangeAdapter(
        batch=batch,
        api_key="not-recorded",
        journal_path=tmp_path / "journal.jsonl",
        counter=acquisition._BatchCallCounter(20),
        http_get=http_get,
        sleeper=lambda _seconds: None,
    )
    with pytest.raises(UnauthorizedOperation, match="request byte ceiling"):
        adapter.get_range(
            **acquisition._provider_range_query(_item("a", "definition")),
            path=str(tmp_path / "candidate.dbn.zst.partial"),
            request_id="a" * 64,
            request_byte_ceiling=1,
        )
    assert called is False


def test_batch_transport_resumes_bound_job_without_resubmission(tmp_path: Path) -> None:
    payload = b"opaque-synthetic-dbn"
    batch = _BatchFixture(payload)
    batch.submissions.clear()
    adapter = acquisition._BatchRangeAdapter(
        batch=batch,
        api_key="not-recorded",
        journal_path=tmp_path / "resume-journal.jsonl",
        counter=acquisition._BatchCallCounter(20),
        http_get=lambda **_kwargs: _StreamResponse(chunks=[payload], status_code=200),
        sleeper=lambda _seconds: None,
        resume_job_ids={"a" * 64: "job-1"},
    )
    output = tmp_path / "candidate.dbn.zst.partial"
    adapter.get_range(
        **acquisition._provider_range_query(_item("a", "ohlcv_1s")),
        path=str(output),
        request_id="a" * 64,
        request_byte_ceiling=1000,
    )
    assert output.read_bytes() == payload
    assert batch.submissions == []
    events = [
        json.loads(line)
        for line in (tmp_path / "resume-journal.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    assert events[0]["event"] == "BATCH_JOB_RESUMED"


def test_failure_summary_retains_mechanism_without_url_or_credential() -> None:
    value = acquisition._safe_failure_summary(
        RuntimeError(
            "stream failed https://example.invalid/private?token=abc "
            "api_key=secret-value"
        )
    )
    assert "stream failed" in value
    assert "example.invalid" not in value
    assert "secret-value" not in value


def test_first_failure_stops_new_requests_and_preserves_partial(
    tmp_path: Path,
) -> None:
    calls = 0

    def factory() -> DownloadProviderApis:
        def get_range(**kwargs: object) -> None:
            nonlocal calls
            calls += 1
            path = Path(str(kwargs["path"]))
            path.write_bytes(b"partial")
            raise RuntimeError("synthetic provider failure")

        return DownloadProviderApis(get_cost=lambda **_kwargs: 0, get_range=get_range)

    result = acquisition._download_worker(
        root=tmp_path,
        worker_name="worker",
        items=[_item("a", "definition"), _item("b", "ohlcv_1m")],
        provider_factory=factory,
        stop_event=threading.Event(),
        total_state={"bytes": 0},
        total_lock=threading.Lock(),
        maximum_total_bytes=10_000,
        started=time.monotonic(),
        clock=time.monotonic,
    )
    assert calls == 1
    assert result.failure_type == "RuntimeError"
    assert list(tmp_path.rglob("*.partial"))


def test_load_plan_rejects_identity_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _synthetic_repository(tmp_path, monkeypatch)
    plan = acquisition.build_acquisition_plan(root=root)
    _write_json(root / acquisition.PLAN_PATH, plan)
    assert acquisition.load_acquisition_plan(root=root)["plan_id"] == plan["plan_id"]
    plan["worker_contract"]["maximum_parallel_downloads"] = 3
    _write_json(root / acquisition.PLAN_PATH, plan)
    with pytest.raises(IntegrityError, match="semantics"):
        acquisition.load_acquisition_plan(root=root)


def test_required_scope_accepts_exact_user_approved_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _synthetic_repository(tmp_path, monkeypatch)
    plan = acquisition.build_acquisition_plan(root=root)
    _write_json(root / acquisition.PLAN_PATH, plan)
    plan_sha256 = sha256_file(root / acquisition.PLAN_PATH)
    required = acquisition.required_scope(root=root, plan=plan)
    assert required["approval_command"] == acquisition.OPERATION
    assert required["approval_plan_id"] == plan["plan_id"]
    assert required["approval_plan_sha256"] == plan_sha256
    assert required["complete_request_count"] == "287"
    assert required["reused_request_count"] == "65"
    assert required["network_request_count"] == "222"
    assert required["maximum_batch_job_seconds"] == "14400.0"

    issue_scope = {
        key: value for key, value in required.items() if not key.startswith("approval_")
    }
    receipt = OperationReceipt.issue_user_approved(
        RepoBoundary(root),
        operation=acquisition.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=issue_scope,
        approval_command=acquisition.OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha256,
        approval_line=_personal_approval_line(
            acquisition.OPERATION, str(plan["plan_id"]), plan_sha256
        ),
    )
    receipt.verify(
        RepoBoundary(root),
        operation=acquisition.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required,
    )


def test_source_contains_no_decoder_registration_or_publication_route() -> None:
    source = Path(acquisition.__file__).read_text(encoding="utf-8")
    assert "from .foundation.decoder" not in source
    assert "PhasePublisher" not in source
    assert "catalog_or_pointer" not in source
    assert "MAXIMUM_PARALLEL_DOWNLOADS: Final = 2" in source
    assert "MAXIMUM_CONCURRENT_OHLCV_1S: Final = 1" in source
