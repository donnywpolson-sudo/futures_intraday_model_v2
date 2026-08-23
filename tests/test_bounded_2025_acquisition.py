from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from futures_rebuild import bounded_2025_acquisition as acquisition
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError
from futures_rebuild.micro_alpha_acquisition import DownloadProviderApis


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
    return root


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
    plan = acquisition.build_acquisition_plan(root=root)
    assert plan["counts"] == {
        "requests": 287,
        "expected_dbns": 287,
        "expected_sidecars": 287,
        "heavy_requests": 41,
        "light_requests": 246,
    }
    assert plan["worker_contract"]["maximum_parallel_downloads"] == 2
    assert plan["worker_contract"]["maximum_concurrent_ohlcv_1s"] == 1
    assert plan["worker_contract"]["automatic_retries"] == 0
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
        "query": {"schema": family.replace("_", "-"), "start": "x", "end": "y"},
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
    with pytest.raises(IntegrityError, match="identity"):
        acquisition.load_acquisition_plan(root=root)


def test_source_contains_no_decoder_registration_or_publication_route() -> None:
    source = Path(acquisition.__file__).read_text(encoding="utf-8")
    assert "from .foundation.decoder" not in source
    assert "PhasePublisher" not in source
    assert "catalog_or_pointer" not in source
    assert "MAXIMUM_PARALLEL_DOWNLOADS: Final = 2" in source
    assert "MAXIMUM_CONCURRENT_OHLCV_1S: Final = 1" in source
