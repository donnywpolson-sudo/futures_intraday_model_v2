from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from futures_rebuild.live_cockpit.databento_auth import redact_databento_text
from futures_rebuild.ohlcv_historical_backfill import (
    DATASET,
    MANIFEST_SCHEMA,
    aggregate_storage_and_cost,
    apply_incremental_estimates,
    atomic_install_directory,
    build_expected_targets,
    build_metadata_quote,
    build_quote_cache,
    classify_target,
    compression_extension,
    _active_release_markets,
    _ledger_reuse_job_id,
    _targets_to_jobs,
    execute_manifest,
    half_open_year_slices,
    normalized_request,
    reconcile_market_sets,
    request_fingerprint,
    resumable_https_download,
    select_reusable_job,
)


pytestmark = pytest.mark.current


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _market_fixture(root: Path, *, mismatch: bool = False) -> list[str]:
    full = [f"F{index:02d}" for index in range(41)]
    micro = [f"M{index:02d}" for index in range(17)]
    markets = sorted([*full, *micro])
    _write_json(
        root / "configs/research_universe_contract.json",
        {"tiers": [{"tier_id": 3, "symbols": full[:38]}, {"tier_id": 4, "symbols": full[38:]}]},
    )
    _write_json(
        root / "configs/micro_contract_universe_v1.json",
        {"tiers": {"tier_3": micro}},
    )
    for local in ("ohlcv_1d", "ohlcv_1h", "ohlcv_1m"):
        for market in markets:
            (root / "data/dbn" / local / market).mkdir(parents=True)
    if mismatch:
        (root / "data/dbn/ohlcv_1h" / markets[-1]).rmdir()
    release = {
        "release_core": {
            "normalized_unit_manifests": [
                {"family": family, "market": market}
                for family in ("ohlcv-1d", "ohlcv-1h")
                for market in markets
            ]
        }
    }
    release_path = root / "reports/release.json"
    _write_json(release_path, release)
    digest = hashlib.sha256(release_path.read_bytes()).hexdigest()
    _write_json(
        root / "configs/active_dbn_congruence_release_v1.json",
        {
            "release_manifest_path": "reports/release.json",
            "release_manifest_sha256": digest,
            "status": "ACTIVE",
        },
    )
    return markets


def _target(tmp_path: Path, *, year: int = 2024, schema: str = "ohlcv-1h") -> dict[str, object]:
    start = f"{year}-01-01T00:00:00Z"
    end = f"{year + 1}-01-01T00:00:00Z"
    local = schema.replace("-", "_")
    filename = f"{year}-01-01_{year + 1}-01-01.dbn.zst"
    return {
        "final_path": f"data/dbn/{local}/ES/{year}/{filename}",
        "intended_end_exclusive": end,
        "intended_start_inclusive": start,
        "market": "ES",
        "request_fingerprint": "a" * 64,
        "schema": schema,
        "sidecar_path": f"data/dbn/{local}/ES/{year}/{filename}.manifest.json",
        "symbol_specification": {
            "segments": [{"start_inclusive": start, "end_exclusive": end, "symbols": ["ES.v.0"]}],
            "stype_in": "continuous",
            "stype_out": "instrument_id",
            "symbols": ["ES.v.0"],
        },
        "target_id": "b" * 64,
        "year": year,
    }


def _good_probe(target: dict[str, object]) -> dict[str, object]:
    return {
        "dataset": DATASET,
        "dbn_format_version": 1,
        "metadata_end": target["intended_end_exclusive"],
        "metadata_start": target["intended_start_inclusive"],
        "min_ts_event": target["intended_start_inclusive"],
        "max_ts_event": f"{target['year']}-12-31T23:00:00Z",
        "monotonic": True,
        "not_found": [],
        "partial": [],
        "record_count": 10,
        "schema": target["schema"],
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "symbols": ["ES.v.0"],
    }


def _install_pair(
    root: Path,
    target: dict[str, object],
    *,
    executor_sidecar_fields: bool = False,
) -> None:
    data_path = root / str(target["final_path"])
    data_path.parent.mkdir(parents=True, exist_ok=True)
    data_path.write_bytes(b"dbn-test")
    sidecar = (
        {
            "sha256": hashlib.sha256(b"dbn-test").hexdigest(),
            "dbn_byte_size": len(b"dbn-test"),
        }
        if executor_sidecar_fields
        else {
            "file_sha256": hashlib.sha256(b"dbn-test").hexdigest(),
            "file_size_bytes": len(b"dbn-test"),
        }
    )
    _write_json(
        root / str(target["sidecar_path"]),
        sidecar,
    )


def _manifest_row(target: dict[str, object]) -> dict[str, object]:
    return {
        "activation_status": "NOT_ACTIVE",
        "current_state": "MISSING",
        "execution_action": "DOWNLOAD_VALIDATE_INSTALL_ABSENT_TARGET_ONLY",
        "existing_bytes": 0,
        "expected_incremental_bytes": 1,
        "manifest_schema": MANIFEST_SCHEMA,
        "parent_planned_job_id": "job-test",
        "provider_condition_hash": "c" * 64,
        "provider_metadata_hash": "d" * 64,
        "provider_record_count": 10,
        "run_id": "test",
        "validation_requirements": ["DBN_DECODER_READABLE"],
        **target,
    }


def test_exactly_matching_58_market_inventories(tmp_path: Path) -> None:
    expected = _market_fixture(tmp_path)
    rows, summary = reconcile_market_sets(tmp_path)
    assert summary["verified_target_markets"] == expected
    assert summary["conflict"] is False
    assert sum(bool(row["final_inclusion"]) for row in rows) == 58


def test_mismatched_target_root_market_sets_fail_closed(tmp_path: Path) -> None:
    _market_fixture(tmp_path, mismatch=True)
    _, summary = reconcile_market_sets(tmp_path)
    assert summary["conflict"] is True
    assert len(summary["verified_target_markets"]) == 57


@pytest.mark.parametrize(
    ("manifest_relative_path", "expected_count", "expect_msf"),
    [
        (
            "reports/remediations/canonical_dbn_356_no_trades_successor_20260816T2335369052352Z/"
            "successor_release_manifest.json",
            33,
            False,
        ),
        (
            "reports/ohlcv_msf_1d_publication_successor/"
            "msf1dpub_9ea418c5b30e41bd8f9e3dc8/successor_release_manifest.json",
            34,
            True,
        ),
    ],
)
def test_active_release_markets_reads_canonical_hyphenated_families(
    tmp_path: Path,
    manifest_relative_path: str,
    expected_count: int,
    expect_msf: bool,
) -> None:
    repository_root = Path(__file__).resolve().parents[1]
    source_manifest = repository_root / manifest_relative_path
    copied_manifest = tmp_path / "reports/release.json"
    copied_manifest.parent.mkdir(parents=True)
    copied_manifest.write_bytes(source_manifest.read_bytes())
    _write_json(
        tmp_path / "configs/active_dbn_congruence_release_v1.json",
        {
            "release_manifest_path": "reports/release.json",
            "release_manifest_sha256": hashlib.sha256(copied_manifest.read_bytes()).hexdigest(),
            "status": "ACTIVE",
        },
    )

    markets, evidence_paths = _active_release_markets(tmp_path)

    assert len(markets) == expected_count
    assert ("MSF" in markets) is expect_msf
    assert evidence_paths == [
        "configs/active_dbn_congruence_release_v1.json",
        "reports/release.json",
    ]


def test_market_beginning_after_2010() -> None:
    rows = half_open_year_slices("2018-04-23T00:00:00Z", "2020-01-01T00:00:00Z")
    assert [row["year"] for row in rows] == [2018, 2019]


def test_partial_first_year() -> None:
    rows = half_open_year_slices("2017-06-05T00:00:00Z", "2018-01-01T00:00:00Z")
    assert rows[0]["first_year_partial"] is True
    assert rows[0]["start_inclusive"] == "2017-06-05T00:00:00Z"


def test_partial_current_year() -> None:
    rows = half_open_year_slices("2025-01-01T00:00:00Z", "2026-07-14T00:00:00Z")
    assert rows[-1]["terminal_year_partial"] is True
    assert rows[-1]["end_exclusive"] == "2026-07-14T00:00:00Z"


def test_multiple_dated_symbol_segments_in_one_market(tmp_path: Path) -> None:
    registry = [{
        "market": "ES",
        "intended_start_inclusive": "2020-01-01T00:00:00Z",
        "intended_end_exclusive": "2021-01-01T00:00:00Z",
        "symbol_segments": [
            {"start_inclusive": "2020-01-01T00:00:00Z", "end_exclusive": "2020-07-01T00:00:00Z", "symbols": ["OLD"]},
            {"start_inclusive": "2020-07-01T00:00:00Z", "end_exclusive": "2021-01-01T00:00:00Z", "symbols": ["NEW"]},
        ],
    }]
    targets = build_expected_targets(tmp_path, registry)
    assert len(targets) == 2
    assert targets[0]["symbol_specification"]["symbols"] == ["NEW", "OLD"]


def test_valid_existing_annual_pair_is_skipped(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _install_pair(tmp_path, target)
    result = classify_target(tmp_path, target, dbn_probe=lambda _: _good_probe(target))
    assert result["current_state"] == "COMPLETE_VALID"


def test_executor_sidecar_fields_are_readback_compatible(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _install_pair(tmp_path, target, executor_sidecar_fields=True)
    result = classify_target(tmp_path, target, dbn_probe=lambda _: _good_probe(target))
    assert result["current_state"] == "COMPLETE_VALID"


def test_executor_sidecar_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _install_pair(tmp_path, target, executor_sidecar_fields=True)
    sidecar_path = tmp_path / str(target["sidecar_path"])
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    sidecar["sha256"] = "0" * 64
    _write_json(sidecar_path, sidecar)
    result = classify_target(tmp_path, target, dbn_probe=lambda _: _good_probe(target))
    assert result["current_state"] == "SIDECAR_INVALID"


def test_missing_sidecar(tmp_path: Path) -> None:
    target = _target(tmp_path)
    data_path = tmp_path / str(target["final_path"])
    data_path.parent.mkdir(parents=True)
    data_path.write_bytes(b"dbn")
    assert classify_target(tmp_path, target)["current_state"] == "SIDECAR_MISSING"


def test_corrupt_dbn(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _install_pair(tmp_path, target)
    def broken(_: Path) -> dict[str, object]:
        raise ValueError("corrupt")
    assert classify_target(tmp_path, target, dbn_probe=broken)["current_state"] == "CORRUPT"


def test_request_parameter_mismatch(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _install_pair(tmp_path, target)
    probe = _good_probe(target)
    probe["schema"] = "ohlcv-1d"
    assert classify_target(tmp_path, target, dbn_probe=lambda _: probe)["current_state"] == "REQUEST_PARAMETER_MISMATCH"


def test_stale_current_year_file(tmp_path: Path) -> None:
    target = _target(tmp_path, year=2026)
    target["intended_end_exclusive"] = "2026-07-14T00:00:00Z"
    alternate = tmp_path / "data/dbn/ohlcv_1h/ES/2026/2026-01-01_2026-06-01.dbn.zst"
    alternate.parent.mkdir(parents=True)
    alternate.write_bytes(b"old")
    assert classify_target(tmp_path, target)["current_state"] == "STALE_CURRENT_YEAR"


def test_confirmed_no_data_year(tmp_path: Path) -> None:
    assert classify_target(tmp_path, _target(tmp_path), confirmed_record_count=0)["current_state"] == "NO_DATA_CONFIRMED"


def test_half_open_year_boundaries() -> None:
    rows = half_open_year_slices("2020-01-01T00:00:00Z", "2022-01-01T00:00:00Z")
    assert rows[0]["end_exclusive"] == rows[1]["start_inclusive"]
    with pytest.raises(ContractError):
        half_open_year_slices("2020-01-01T00:00:01Z", "2021-01-01T00:00:00Z")


def test_compression_extension_handling() -> None:
    assert compression_extension("zstd") == ".dbn.zst"
    assert compression_extension("none") == ".dbn"
    with pytest.raises(ContractError):
        compression_extension("zip")


def test_exact_byte_aggregation_without_rounding() -> None:
    inventory = [
        {"current_state": "COMPLETE_VALID", "schema": "ohlcv-1d", "actual_dbn_bytes": 101, "actual_sidecar_bytes": 11, "existing_bytes": 112},
        {"current_state": "COMPLETE_VALID", "schema": "ohlcv-1h", "actual_dbn_bytes": 1001, "actual_sidecar_bytes": 13, "existing_bytes": 1014},
    ]
    quote = [
        {"schema": "ohlcv-1d", "api_billable_uncompressed_bytes": 1000, "estimated_cost_usd": "0.123456"},
        {"schema": "ohlcv-1h", "api_billable_uncompressed_bytes": 9000, "estimated_cost_usd": "0.876544"},
    ]
    result = aggregate_storage_and_cost(inventory, quote, current_free_bytes=10_000_000_000, audit_support_bytes=17)
    assert result["combined_full_final"]["bytes"] == 1126
    assert result["api_billable_uncompressed_bytes"]["bytes"] == 10_000
    assert result["cost"]["full_corpus_theoretical_cost_usd"] == "1.000000"


def test_full_versus_incremental_storage_totals() -> None:
    inventory = [
        {"current_state": "COMPLETE_VALID", "schema": "ohlcv-1d", "actual_dbn_bytes": 10, "actual_sidecar_bytes": 2, "existing_bytes": 12},
        {"current_state": "MISSING", "schema": "ohlcv-1d", "actual_dbn_bytes": 0, "actual_sidecar_bytes": 0, "existing_bytes": 0, "expected_incremental_bytes": 7},
    ]
    quote = [{"schema": "ohlcv-1d", "api_billable_uncompressed_bytes": 20, "estimated_cost_usd": "0"}]
    result = aggregate_storage_and_cost(inventory, quote, current_free_bytes=2_000_000_000, audit_support_bytes=0)
    assert result["existing_valid_target_bytes"]["bytes"] == 12
    assert result["incremental_final_bytes"]["bytes"] == 7


def test_incremental_job_quote_drives_compressed_and_cost_estimates() -> None:
    inventory = [
        {"target_id": "present", "market": "TN", "schema": "ohlcv-1h", "current_state": "COMPLETE_VALID", "actual_dbn_bytes": 100, "actual_sidecar_bytes": 10, "existing_bytes": 110},
        {"target_id": "missing", "market": "TN", "schema": "ohlcv-1h", "current_state": "MISSING", "actual_dbn_bytes": 0, "actual_sidecar_bytes": 0, "existing_bytes": 0, "provider_record_count": 5, "expected_incremental_bytes": 0},
    ]
    full = [{"market": "TN", "schema": "ohlcv-1h", "api_billable_uncompressed_bytes": 1000, "estimated_cost_usd": "0"}]
    incremental = [{"request_fingerprint": "f" * 64, "schema": "ohlcv-1h", "api_billable_uncompressed_bytes": 200, "estimated_cost_usd": "0.25", "quote_timestamp_utc": "2026-01-01T00:00:00Z"}]
    jobs = [{"market": "TN", "schema": "ohlcv-1h", "request_fingerprint": "f" * 64, "target_ids": ["missing"]}]
    apply_incremental_estimates(inventory, jobs, full, incremental)
    assert inventory[1]["expected_incremental_dbn_bytes"] == 20
    assert inventory[1]["expected_incremental_bytes"] == 30
    result = aggregate_storage_and_cost(
        inventory,
        full,
        incremental_quote_units=incremental,
        current_free_bytes=2_000_000_000,
        audit_support_bytes=0,
    )
    assert result["combined_full_final"]["bytes"] == 140
    assert result["cost"]["incremental_completion_cost_usd"] == "0.25"


def test_insufficient_disk_space_blocks() -> None:
    result = aggregate_storage_and_cost([], [], current_free_bytes=1, audit_support_bytes=0)
    assert result["sufficient_space"] is False


def test_quote_cache_and_request_fingerprint_are_deterministic() -> None:
    request = normalized_request(schema="ohlcv-1h", symbols=["ES.v.0"], start="2020-01-01T00:00:00Z", end="2021-01-01T00:00:00Z")
    assert request_fingerprint(request) == request_fingerprint(dict(reversed(list(request.items()))))
    unit = {"request_fingerprint": request_fingerprint(request), "request": request}
    assert build_quote_cache([unit]) == build_quote_cache([dict(unit)])


def test_duplicate_job_reuse() -> None:
    request = normalized_request(schema="ohlcv-1h", symbols=["ES.v.0"], start="2020-01-01T00:00:00Z", end="2021-01-01T00:00:00Z")
    fingerprint = request_fingerprint(request)
    job = {"id": "JOB1", "state": "done", "request_fingerprint": fingerprint}
    assert select_reusable_job(fingerprint, [job])["id"] == "JOB1"
    with pytest.raises(IntegrityError):
        select_reusable_job(fingerprint, [job, {**job, "id": "JOB2"}])


def test_expired_job_handling() -> None:
    fingerprint = "e" * 64
    expired = {"id": "JOB1", "state": "expired", "request_fingerprint": fingerprint}
    assert select_reusable_job(fingerprint, [expired]) is None
    with pytest.raises(IntegrityError):
        select_reusable_job(fingerprint, [expired], required_job_id="JOB1")


def test_resumable_ledger_requires_the_one_previously_submitted_job() -> None:
    fingerprint = "e" * 64
    assert _ledger_reuse_job_id(fingerprint, {}) is None
    assert _ledger_reuse_job_id(fingerprint, {"JOB1": fingerprint}) == "JOB1"
    with pytest.raises(IntegrityError, match="multiple submitted jobs"):
        _ledger_reuse_job_id(fingerprint, {"JOB1": fingerprint, "JOB2": fingerprint})


class _Response:
    def __init__(self, payload: bytes, status: int) -> None:
        self.payload = payload
        self.status_code = status

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def iter_content(self, chunk_size: int) -> list[bytes]:
        del chunk_size
        return [self.payload]


def test_interrupted_download_resume(tmp_path: Path) -> None:
    payload = b"abcdefghij"
    path = tmp_path / "file.partial"
    path.write_bytes(payload[:4])
    def get(**kwargs: object) -> _Response:
        assert kwargs["headers"] == {"Range": "bytes=4-9"}
        return _Response(payload[4:], 206)
    resumable_https_download(
        url="https://example.invalid/file",
        path=path,
        expected_size=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
        api_key="fake",
        http_get=get,
        sleeper=lambda _: None,
    )
    assert path.read_bytes() == payload


def test_atomic_installation(tmp_path: Path) -> None:
    source = tmp_path / "stage/year"
    source.mkdir(parents=True)
    (source / "x").write_text("ok", encoding="utf-8")
    destination = tmp_path / "final/year"
    destination.parent.mkdir(parents=True)
    atomic_install_directory(source, destination)
    assert (destination / "x").read_text(encoding="utf-8") == "ok"


def test_destination_race_blocks(tmp_path: Path) -> None:
    source = tmp_path / "stage/year"
    destination = tmp_path / "final/year"
    source.mkdir(parents=True)
    destination.mkdir(parents=True)
    with pytest.raises(IntegrityError):
        atomic_install_directory(source, destination)


def test_api_key_redaction() -> None:
    secret = "db-ABCDEF1234567890"
    redacted = redact_databento_text(f"DATABENTO_API_KEY={secret}")
    assert secret not in redacted
    assert "<redacted>" in redacted


def test_metadata_quote_uses_no_batch_submission(tmp_path: Path) -> None:
    (tmp_path / "api.env").write_text("DATABENTO_API_KEY=db-FAKE123456789\n", encoding="utf-8")
    calls: list[str] = []
    class Meta:
        def list_schemas(self, **_: object) -> list[str]: calls.append("list_schemas"); return ["ohlcv-1d", "ohlcv-1h"]
        def get_dataset_range(self, **_: object) -> dict[str, str]: calls.append("range"); return {"start": "2010-01-01", "end": "2026-01-01"}
        def get_dataset_condition(self, **_: object) -> list[dict[str, str]]: calls.append("condition"); return []
        def get_record_count(self, **_: object) -> int: calls.append("count"); return 10
        def get_billable_size(self, **_: object) -> int: calls.append("size"); return 100
        def get_cost(self, **_: object) -> float: calls.append("cost"); return 0.0
    class Sym:
        def resolve(self, **_: object) -> dict[str, object]: calls.append("resolve"); return {"result": {"ES.v.0": []}, "partial": [], "not_found": []}
    class Historical:
        def __init__(self, **_: object) -> None:
            self.metadata = Meta()
            self.symbology = Sym()
            self.batch = object()
    registry = [{
        "market": "ES",
        "intended_start_inclusive": "2020-01-01T00:00:00Z",
        "intended_end_exclusive": "2021-01-01T00:00:00Z",
        "symbol_segments": [{"symbols": ["ES.v.0"]}],
    }]
    quote, raw, no_data = build_metadata_quote(
        tmp_path,
        registry,
        [],
        historical_factory=Historical,
        sleeper=lambda _: None,
    )
    assert len(quote["quote_units"]) == 2
    assert raw["credentials_recorded"] is False
    assert no_data == {}
    assert "submit_job" not in calls


def test_dry_run_performs_no_paid_submission(tmp_path: Path) -> None:
    target = _target(tmp_path)
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(json.dumps(_manifest_row(target), sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    called = 0
    def provider(_: Path) -> object:
        nonlocal called
        called += 1
        raise AssertionError("provider must not be constructed")
    result = execute_manifest(root=tmp_path, manifest_path=manifest, provider_factory=provider)
    assert result["dry_run"] is True
    assert result["paid_submissions"] == 0
    assert called == 0


def test_execute_refuses_without_manifest_hash_and_cost_cap(tmp_path: Path) -> None:
    target = _target(tmp_path)
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(json.dumps(_manifest_row(target), sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    with pytest.raises(UnauthorizedOperation, match="manifest SHA"):
        execute_manifest(root=tmp_path, manifest_path=manifest, execute=True)
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    with pytest.raises(UnauthorizedOperation, match="maximum USD cost"):
        execute_manifest(root=tmp_path, manifest_path=manifest, execute=True, manifest_sha256=digest)


def test_completed_resume_is_an_offline_idempotent_noop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _target(tmp_path)
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(
        json.dumps(_manifest_row(target), sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    _install_pair(tmp_path, target, executor_sidecar_fields=True)
    data_path = tmp_path / str(target["final_path"])
    sidecar_path = tmp_path / str(target["sidecar_path"])
    before = (data_path.read_bytes(), sidecar_path.read_bytes())
    called = 0

    def provider(_: Path) -> object:
        nonlocal called
        called += 1
        raise AssertionError("provider must not be constructed for a completed resume")

    def synthetic_classify(
        root: Path,
        row: dict[str, object],
        *,
        confirmed_record_count: int | None = None,
    ) -> dict[str, object]:
        return classify_target(
            root,
            row,
            confirmed_record_count=confirmed_record_count,
            dbn_probe=lambda _: _good_probe(row),
        )

    monkeypatch.setattr(
        "futures_rebuild.ohlcv_historical_backfill.classify_target",
        synthetic_classify,
    )

    result = execute_manifest(
        root=tmp_path,
        manifest_path=manifest,
        execute=True,
        manifest_sha256=digest,
        maximum_authorized_cost_usd="0",
        resume=True,
        provider_factory=provider,
    )

    assert result["result"] == "NO_ACTION_ALL_TARGETS_COMPLETE_OR_NO_DATA"
    assert result["resume_requested"] is True
    assert result["actions"] == 0
    assert result["dbn_files_decoded"] == 1
    assert result["state_counts"] == {"COMPLETE_VALID": 1}
    assert result["paid_submissions"] == 0
    assert called == 0
    assert (data_path.read_bytes(), sidecar_path.read_bytes()) == before


def test_partially_completed_resume_job_fails_before_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _target(tmp_path, year=2024)
    second = _target(tmp_path, year=2025)
    first["target_id"] = "1" * 64
    second["target_id"] = "2" * 64
    second["symbol_specification"] = first["symbol_specification"]
    rows = [_manifest_row(first), _manifest_row(second)]
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(
        "".join(
            json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    _install_pair(tmp_path, first, executor_sidecar_fields=True)
    called = 0

    def provider(_: Path) -> object:
        nonlocal called
        called += 1
        raise AssertionError("provider must not be constructed for partial resume drift")

    def synthetic_classify(
        root: Path,
        row: dict[str, object],
        *,
        confirmed_record_count: int | None = None,
    ) -> dict[str, object]:
        return classify_target(
            root,
            row,
            confirmed_record_count=confirmed_record_count,
            dbn_probe=lambda _: _good_probe(row),
        )

    monkeypatch.setattr(
        "futures_rebuild.ohlcv_historical_backfill.classify_target",
        synthetic_classify,
    )

    with pytest.raises(IntegrityError, match="partially complete immutable provider job"):
        execute_manifest(
            root=tmp_path,
            manifest_path=manifest,
            execute=True,
            manifest_sha256=digest,
            maximum_authorized_cost_usd="0",
            resume=True,
            provider_factory=provider,
        )

    assert called == 0


def _certified_no_data_evidence() -> dict[str, object]:
    return {
        "evidence_path": "reports/ohlcv_58_completion/test/no_data.json",
        "evidence_sha256": "e" * 64,
        "job_id": "GLBX-TEST-JOB",
        "provider_error_code": "symbology_invalid_request",
        "provider_error_message": "None of the symbols could be resolved",
        "provider_error_status": 422,
        "provider_manifest_hash": "f" * 64,
        "request_fingerprint": "1" * 64,
        "schema_version": "ohlcv_provider_no_data_evidence/1.0.0",
    }


def test_certified_no_data_gap_preserves_one_contiguous_provider_request(tmp_path: Path) -> None:
    targets = [_target(tmp_path, year=year) for year in (2022, 2023, 2024)]
    common_symbols = {
        "segments": [
            {
                "start_inclusive": "2022-01-01T00:00:00Z",
                "end_exclusive": "2025-01-01T00:00:00Z",
                "symbols": ["ES.v.0"],
            }
        ],
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "symbols": ["ES.v.0"],
    }
    rows = []
    for index, target in enumerate(targets):
        target["target_id"] = str(index + 1) * 64
        target["symbol_specification"] = common_symbols
        rows.append(_manifest_row(target))
    rows[1].update(
        {
            "activation_status": "NO_DATA_EVIDENCE_ONLY",
            "current_state": "NO_DATA_CONFIRMED",
            "execution_action": "NO_FILE_CREATE",
            "expected_incremental_bytes": 0,
            "no_data_evidence": _certified_no_data_evidence(),
            "provider_record_count": 0,
        }
    )

    jobs = _targets_to_jobs(rows)

    assert len(jobs) == 1
    assert jobs[0]["request"]["start"] == "2022-01-01T00:00:00Z"
    assert jobs[0]["request"]["end"] == "2025-01-01T00:00:00Z"
    assert jobs[0]["target_ids"] == ["1" * 64, "2" * 64, "3" * 64]


def test_certified_no_data_evidence_fails_closed_on_weakened_provider_error(tmp_path: Path) -> None:
    target = _target(tmp_path)
    row = _manifest_row(target)
    row.update(
        {
            "current_state": "NO_DATA_CONFIRMED",
            "execution_action": "NO_FILE_CREATE",
            "no_data_evidence": {**_certified_no_data_evidence(), "provider_error_status": 200},
            "provider_record_count": 0,
        }
    )
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")

    with pytest.raises(IntegrityError, match="provider status"):
        execute_manifest(root=tmp_path, manifest_path=manifest)


def test_bound_successor_allows_partial_resume_to_reach_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = _target(tmp_path, year=2024)
    second = _target(tmp_path, year=2025)
    first["target_id"] = "1" * 64
    second["target_id"] = "2" * 64
    second["symbol_specification"] = first["symbol_specification"]
    predecessor = "9" * 64
    rows = [_manifest_row(first), _manifest_row(second)]
    for row in rows:
        row["manifest_predecessor_sha256"] = predecessor
    manifest = tmp_path / "manifest.jsonl"
    manifest.write_text(
        "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    _install_pair(tmp_path, first, executor_sidecar_fields=True)

    def synthetic_classify(
        root: Path,
        row: dict[str, object],
        *,
        confirmed_record_count: int | None = None,
    ) -> dict[str, object]:
        return classify_target(
            root,
            row,
            confirmed_record_count=confirmed_record_count,
            dbn_probe=lambda _: _good_probe(row),
        )

    monkeypatch.setattr(
        "futures_rebuild.ohlcv_historical_backfill.classify_target",
        synthetic_classify,
    )

    def provider(_: Path) -> object:
        raise RuntimeError("provider reached after certified partial-resume checks")

    with pytest.raises(RuntimeError, match="provider reached"):
        execute_manifest(
            root=tmp_path,
            manifest_path=manifest,
            execute=True,
            manifest_sha256=digest,
            maximum_authorized_cost_usd="0",
            resume=True,
            resume_from_manifest_sha256=predecessor,
            provider_factory=provider,
        )
