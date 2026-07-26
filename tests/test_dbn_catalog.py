import json
import hashlib
import socket
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import databento_dbn as dbn
import pytest
import zstandard

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.anomaly_acceptance import (
    ACCEPTANCE_DOCUMENT,
    publish_anomaly_materialization_acceptance,
)
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.dbn_catalog import (
    CONTINUOUS_CONTRACT_POLICY_CORE,
    CONTINUOUS_CONTRACT_POLICY_HASH,
    FULL_SCAN_CHUNK_RECORDS,
    SYMBOL_CSTR_LEN_BY_METADATA_VERSION,
    _atomic_write,
    _canonical_metadata_mappings,
    _validated_catalog_output,
    _decode_summary,
    _iter_arrays,
    _load_overlap_resolutions,
    _normalize_schema,
    _symbology_resolution_disposition,
    build_source_selection_manifest,
    assert_m2b_source_eligible,
    validate_dbn_pair,
)
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from futures_rebuild.data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from futures_rebuild.foundation.snapshot import PublishedDbnRelease


START_NS = 1_767_225_600_000_000_000
END_NS = START_NS + 86_400_000_000_000


def test_repository_overlap_contract_has_exact_sha256_fields() -> None:
    root = Path(__file__).resolve().parents[1]
    resolutions = _load_overlap_resolutions(
        root / "configs" / "dbn_overlap_resolutions.json"
    )
    assert len(resolutions) == 12


def test_overlap_resolution_cannot_silently_cross_query_modes(tmp_path) -> None:
    import futures_rebuild.dbn_catalog as module

    authoritative = {
        "coverage_disposition": "AUTHORITATIVE_INTERVAL",
        "end": "2026-01-03",
        "market": "ES",
        "path": "data/dbn/ohlcv_1m/ES/2026/2026-01-01_2026-01-03.dbn.zst",
        "query_mode_id": "a" * 64,
        "schema": "ohlcv-1m",
        "sha256": "1" * 64,
        "start": "2026-01-01",
    }
    redundant = {
        "coverage_disposition": "AUTHORITATIVE_INTERVAL",
        "end": "2026-01-03",
        "market": "ES",
        "path": "data/dbn/ohlcv_1m/ES/2026/2026-01-02_2026-01-03.dbn.zst",
        "query_mode_id": "b" * 64,
        "schema": "ohlcv-1m",
        "sha256": "2" * 64,
        "start": "2026-01-02",
    }
    resolution = {
        "authoritative_file_sha256": "1" * 64,
        "authoritative_path": authoritative["path"],
        "family": "dbn_ohlcv_1m",
        "market": "ES",
        "overlap_end": "2026-01-03",
        "overlap_start": "2026-01-02",
        "record_count": 1,
        "record_subset_sha256": "3" * 64,
        "redundant_file_sha256": "2" * 64,
        "redundant_path": redundant["path"],
        "resolution_id": "4" * 64,
        "schema": "ohlcv-1m",
        "timestamp_field": "ts_event",
    }
    with pytest.raises(IntegrityError, match="crosses query modes"):
        module._apply_overlap_resolution(
            family_id="dbn_ohlcv_1m",
                prior=authoritative,
                current=redundant,
                resolutions=(resolution,),
                resolve_logical_path=lambda _logical: tmp_path,
            )


def _write_pair(
    repository: Path,
    *,
    partial: list[str] | None = None,
    not_found: list[str] | None = None,
    mappings: list[object] | None = None,
) -> Path:
    folder = repository / "data" / "dbn" / "ohlcv_1m" / "ES" / "2026"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "2026-01-01_2026-01-02.dbn.zst"
    metadata = dbn.Metadata(
        "GLBX.MDP3",
        START_NS,
        dbn.SType.CONTINUOUS,
        dbn.SType.INSTRUMENT_ID,
        dbn.Schema.OHLCV_1M,
        symbols=["ES.v.0"],
        partial=partial,
        not_found=not_found,
        mappings=mappings,
        end=END_NS,
    )
    records = [
        dbn.OHLCVMsg(
            dbn.RType.OHLCV_1M, 1, 100 + index, START_NS + index * 60_000_000_000,
            1000000000, 1100000000, 900000000, 1050000000, 10 + index,
        )
        for index in range(2)
    ]
    encoded = metadata.encode() + b"".join(bytes(record) for record in records)
    path.write_bytes(zstandard.ZstdCompressor().compress(encoded))
    sidecar = {
        "vendor": "databento",
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "market": "ES",
        "symbols_requested": ["ES.v.0"],
        "start": "2026-01-01",
        "end": "2026-01-02",
        "stype_in": "continuous",
        "stype_out": "instrument_id",
        "encoding": "dbn",
        "compression": "zstd",
        "path": "data/dbn/ohlcv_1m/ES/2026/2026-01-01_2026-01-02.dbn.zst",
        "file_size_bytes": path.stat().st_size,
        "file_sha256": sha256_file(path),
        "request_status": "ok",
    }
    Path(f"{path}.manifest.json").write_text(json.dumps(sidecar), encoding="utf-8")
    return path


def _write_custom_pair(
    repository: Path,
    *,
    filename: str,
    metadata_start: int,
    metadata_end: int,
    records: list[dbn.OHLCVMsg],
    market: str = "ES",
    year: int = 2026,
    stype_in: str = "continuous",
) -> Path:
    symbol = f"{market}.FUT" if stype_in == "parent" else f"{market}.v.0"
    folder = repository / "data" / "dbn" / "ohlcv_1m" / market / str(year)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / filename
    metadata = dbn.Metadata(
        "GLBX.MDP3",
        metadata_start,
        dbn.SType.PARENT if stype_in == "parent" else dbn.SType.CONTINUOUS,
        dbn.SType.INSTRUMENT_ID,
        dbn.Schema.OHLCV_1M,
        symbols=[symbol],
        end=metadata_end,
    )
    encoded = metadata.encode() + b"".join(bytes(record) for record in records)
    path.write_bytes(zstandard.ZstdCompressor().compress(encoded))
    coverage_name = filename.removesuffix(".parent.dbn.zst")
    if coverage_name == filename:
        coverage_name = filename.removesuffix(".dbn.zst")
    start, end = coverage_name.split("_")
    sidecar = {
        "vendor": "databento",
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "market": market,
        "symbols_requested": [symbol],
        "start": start,
        "end": end,
        "encoding": "dbn",
        "compression": "zstd",
        "path": f"data/dbn/ohlcv_1m/{market}/{year}/{filename}",
        "file_size_bytes": path.stat().st_size,
        "file_sha256": sha256_file(path),
        "request_status": "ok",
        "stype_in": stype_in,
        "stype_out": "instrument_id",
    }
    Path(f"{path}.manifest.json").write_text(json.dumps(sidecar), encoding="utf-8")
    return path


def _catalog_context(
    base: Path,
    legacy: Path,
    *,
    duplicate: bool = False,
    active: Path | None = None,
) -> tuple[RepoBoundary, Path, Path]:
    active = active or (base / "active")
    (active / "configs").mkdir(parents=True, exist_ok=True)
    family = {
        "id": "dbn_ohlcv_1m",
        "path": "data/dbn/ohlcv_1m",
        "role": "immutable_provider_source_and_canonical_research_input",
        "schema": "ohlcv-1m",
    }
    anomaly_payload = {
        "contract_version": "1.0.0",
        "default_disposition": "QUARANTINE_FAIL_CLOSED",
        "waivers_allowed": False,
        "families": [
            {"market": "KE", "year": 2019},
            {"market": "KE", "year": 2021},
            {"market": "KE", "year": 2023},
            {"market": "KE", "year": 2024},
            {"market": "SR1", "year": 2020},
            {"market": "SR3", "year": 2020},
        ],
        "promotion_requirement": "anomaly_specific_source_alignment_and_causal_tests_pass",
    }
    anomaly_path = active / "configs" / "known_anomalies.json"
    anomaly_path.write_text(json.dumps(anomaly_payload), encoding="utf-8")
    payload = {
        "active_repository": str(active.resolve()),
        "continuous_contract_policy": {
            **CONTINUOUS_CONTRACT_POLICY_CORE,
            "policy_hash": CONTINUOUS_CONTRACT_POLICY_HASH,
        },
        "legacy_repository": str(legacy.resolve()),
        "known_anomalies_sha256": sha256_file(anomaly_path),
        "source_families": [family, {**family, "id": "dbn_duplicate"}]
        if duplicate
        else [family],
    }
    path = active / "configs" / ("source_contract_duplicate.json" if duplicate else "source_contract.json")
    path.write_text(json.dumps(payload), encoding="utf-8")
    boundary = RepoBoundary(active.resolve(), (legacy.resolve(),), ())
    return boundary, path, anomaly_path


def test_offline_pair_validates_hash_metadata_and_first_last_records(tmp_path) -> None:
    path = _write_pair(tmp_path)
    result = validate_dbn_pair(
        path,
        dbn_root=tmp_path / "data" / "dbn",
        expected_schema="ohlcv-1m",
        role="canonical",
        sample_records=1,
        scan_to_end=True,
    )
    assert result["decode"]["record_count"] == 2
    assert result["decode"]["first_records"][0]["instrument_id"] == 100
    assert result["decode"]["last_records"][0]["instrument_id"] == 101
    assert result["continuous_selection_rule"] == "V_PREVIOUS_DAY_VOLUME_RANK_0"
    assert result["symbology_resolution_disposition"] == "COMPLETE"
    assert result["decode"]["not_found_symbols"] == []
    assert result["decode"]["partial_symbols"] == []
    assert result["decode"]["symbol_cstr_len"] == SYMBOL_CSTR_LEN_BY_METADATA_VERSION[
        result["decode"]["metadata_version"]
    ]


def test_parent_partial_child_mappings_are_recorded_but_never_used_as_coverage() -> None:
    assert _symbology_resolution_disposition(
        query_stype_in="parent",
        query_symbols=["ES.FUT"],
        partial_symbols=["ESM6-ESU6", "ESM6"],
        not_found_symbols=[],
        mapping_symbols=["ESM6", "ESM6-ESU6", "ESU6"],
    ) == "PARENT_CHILD_PARTIAL_RECORDED_RECONCILIATION_ONLY"


@pytest.mark.parametrize(
    ("query_stype_in", "partial", "not_found", "mappings", "message"),
    [
        ("continuous", ["ES.v.0"], [], ["ES.v.0"], "non-parent"),
        ("parent", ["ES.FUT"], [], ["ES.FUT"], "query symbol"),
        ("parent", ["ESM6"], [], ["ESU6"], "absent from mappings"),
        ("parent", [], ["ES.FUT"], ["ES.FUT"], "not_found"),
        ("parent", ["ESM6", "ESM6"], [], ["ESM6"], "duplicate"),
    ],
)
def test_incomplete_or_malformed_symbology_resolution_fails_closed(
    query_stype_in, partial, not_found, mappings, message
) -> None:
    with pytest.raises(IntegrityError, match=message):
        _symbology_resolution_disposition(
            query_stype_in=query_stype_in,
            query_symbols=["ES.FUT"],
            partial_symbols=partial,
            not_found_symbols=not_found,
            mapping_symbols=mappings,
        )


def test_mapping_hash_is_order_stable_and_dates_are_canonical() -> None:
    first = {
        "ESU6": [
            {
                "start_date": date(2026, 3, 15),
                "end_date": date(2026, 6, 21),
                "symbol": "200",
            }
        ],
        "ESM6": [
            {
                "start_date": date(2026, 1, 1),
                "end_date": date(2026, 3, 15),
                "symbol": "100",
            }
        ],
    }
    second = {key: first[key] for key in reversed(first)}
    symbols_first, canonical_first = _canonical_metadata_mappings(first)
    symbols_second, canonical_second = _canonical_metadata_mappings(second)
    assert symbols_first == symbols_second == ["ESM6", "ESU6"]
    assert canonical_first == canonical_second
    assert sha256_json(canonical_first) == sha256_json(canonical_second)
    assert canonical_first[0]["intervals"][0]["start_date"] == "2026-01-01"


def test_continuous_partial_metadata_fails_at_pair_validation(tmp_path) -> None:
    path = _write_pair(
        tmp_path,
        partial=["ES.v.0"],
        mappings=[
            SimpleNamespace(
                raw_symbol="ES.v.0",
                intervals=[
                    SimpleNamespace(
                        start_date=date(2026, 1, 1),
                        end_date=date(2026, 1, 2),
                        symbol="100",
                    )
                ],
            )
        ],
    )
    with pytest.raises(IntegrityError, match="non-parent"):
        validate_dbn_pair(
            path,
            dbn_root=tmp_path / "data" / "dbn",
            expected_schema="ohlcv-1m",
            role="canonical",
        )


def test_metadata_version_and_symbol_width_pair_must_be_exact(
    tmp_path, monkeypatch
) -> None:
    path = _write_pair(tmp_path)
    import futures_rebuild.dbn_catalog as module

    original = module._decode_summary

    def mismatched(*args, **kwargs):
        result = original(*args, **kwargs)
        result["symbol_cstr_len"] += 1
        return result

    monkeypatch.setattr(module, "_decode_summary", mismatched)
    with pytest.raises(IntegrityError, match="version/symbol width"):
        validate_dbn_pair(
            path,
            dbn_root=tmp_path / "data" / "dbn",
            expected_schema="ohlcv-1m",
            role="canonical",
        )


def test_checked_in_continuous_contract_policy_is_exact_and_causal() -> None:
    path = Path(__file__).parents[1] / "configs" / "source_contract.json"
    policy = json.loads(path.read_text(encoding="utf-8"))["continuous_contract_policy"]
    assert policy == {
        **CONTINUOUS_CONTRACT_POLICY_CORE,
        "policy_hash": CONTINUOUS_CONTRACT_POLICY_HASH,
    }
    assert policy["mapping_interval_end_fields_feature_eligible"] is False
    assert policy["price_adjustment"] == "NONE_ORIGINAL_UNADJUSTED"


def test_changed_continuous_contract_policy_fails_before_catalog_scan(tmp_path) -> None:
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    boundary, contract_path, anomalies = _catalog_context(tmp_path, legacy)
    payload = json.loads(contract_path.read_text(encoding="utf-8"))
    payload["continuous_contract_policy"]["selection_basis"] = "SAME_DAY_VOLUME"
    contract_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(IntegrityError, match="pinned causal rule"):
        build_source_selection_manifest(
            legacy,
            contract_path,
            boundary=boundary,
            known_anomaly_contract_path=anomalies,
        )


def test_exact_known_anomaly_set_is_pinned() -> None:
    root = Path(__file__).parents[1]
    path = root / "configs" / "known_anomalies.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    source_contract = json.loads(
        (root / "configs" / "source_contract.json").read_text(encoding="utf-8")
    )
    foundation_policy = json.loads(
        (root / "configs" / "foundation_policy.json").read_text(encoding="utf-8")
    )
    observed_hash = sha256_file(path)
    assert observed_hash == (
        "eb7c83bf69d1c7a1b57878a66ac86581fc5d1572e252db7f48e6dedc4e49f923"
    )
    assert source_contract["known_anomalies_sha256"] == observed_hash
    assert foundation_policy["known_anomalies_sha256"] == observed_hash
    assert {(item["market"], item["year"]) for item in payload["families"]} == {
        ("KE", 2019), ("KE", 2021), ("KE", 2023), ("KE", 2024),
        ("SR1", 2020), ("SR3", 2020),
    }
    assert payload["default_disposition"] == "QUARANTINE_FAIL_CLOSED"
    assert payload["waivers_allowed"] is False


def test_known_anomaly_is_quarantined_until_exact_verified_acceptance(tmp_path) -> None:
    start_dt = datetime(2019, 1, 1, tzinfo=timezone.utc)
    start_ns = int(start_dt.timestamp() * 1_000_000_000)
    end_ns = start_ns + 86_400_000_000_000
    legacy = tmp_path / "legacy"
    _write_custom_pair(
        legacy,
        filename="2019-01-01_2019-01-02.dbn.zst",
        metadata_start=start_ns,
        metadata_end=end_ns,
        records=[
            dbn.OHLCVMsg(
                dbn.RType.OHLCV_1M, 1, 100, start_ns,
                1000000000, 1100000000, 900000000, 1050000000, 10,
            )
        ],
        market="KE",
        year=2019,
    )
    project = tmp_path / "project"
    boundary, _, anomalies = _catalog_context(
        tmp_path, legacy, active=project
    )
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    publisher = AtomicPublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=boundary.active_root / "state" / "locks" / "publication.lock",
    )
    source = next((legacy / "data" / "dbn" / "ohlcv_1m" / "KE" / "2019").glob("*.dbn.zst"))
    sidecar = Path(f"{source}.manifest.json")
    stage = publisher.create_stage("dbn")
    (stage / source.name).write_bytes(source.read_bytes())
    (stage / sidecar.name).write_bytes(sidecar.read_bytes())
    logical_root = "data/dbn/ohlcv_1m/KE/2019"
    source_manifest = ReleaseManifest.build(
        stage,
        phase="dbn",
        release_kind="futures_phase1a_verified_dbn",
        schema_version="1.0.0",
        logical_paths={
            source.name: f"{logical_root}/{source.name}",
            sidecar.name: f"{logical_root}/{sidecar.name}",
        },
    )
    source_manifest_path = publisher.publish(stage, source_manifest)
    snapshot = PublishedDbnRelease.open(source_manifest_path, boundary=boundary)
    binding = snapshot.file(
        f"dbn/ohlcv_1m/KE/2019/{source.name}"
    )
    sidecar_binding = snapshot.file(f"{binding.relative_path}.manifest.json")
    validated = validate_dbn_pair(
        binding.path,
        logical_path=f"data/{binding.relative_path}",
        sidecar_path=sidecar_binding.path,
        expected_schema="ohlcv-1m",
        role="immutable_provider_source_and_canonical_research_input",
    )
    entry_core = {
        **{key: value for key, value in validated.items() if key != "validation_sha256"},
        "coverage_disposition": "QUARANTINED_PENDING_REVALIDATION",
        "family": "dbn_ohlcv_1m",
    }
    quarantined = {
        **entry_core,
        "validation_sha256": sha256_json(entry_core),
    }
    selection_core = {
        "catalog_contract_version": "2.0.0",
        "dataset": "GLBX.MDP3",
        "families": [{"family": "dbn_ohlcv_1m"}],
        "files": [quarantined],
        "known_anomalies_sha256": sha256_file(anomalies),
        "selection_policy": "EXACT_CONTRACT_ALL_FILES_NO_RECURSIVE_NEWEST",
        "selection_scope": "FILTERED",
        "source_dbn_manifest_sha256": snapshot.source_manifest_sha256,
        "source_dbn_release_id": snapshot.source_release_id,
        "source_scope": "VERIFIED_LAYOUT_V2_DBN_RELEASE",
    }
    selection = {
        **selection_core,
        "selection_manifest_id": sha256_json(selection_core),
    }
    with pytest.raises(IntegrityError, match="one exact aggregate acceptance"):
        assert_m2b_source_eligible(
            selection, acceptance_receipts=(), boundary=boundary
        )
    with pytest.raises(ContractError, match="exceed the explicit scan cap"):
        publish_anomaly_materialization_acceptance(
            selection,
            snapshot=snapshot,
            publisher=publisher,
            maximum_total_bytes=binding.size - 1,
        )
    receipt = publish_anomaly_materialization_acceptance(
        selection,
        snapshot=snapshot,
        publisher=publisher,
        maximum_total_bytes=binding.size,
    )
    assert_m2b_source_eligible(
        selection, acceptance_receipts=(receipt,), boundary=boundary
    )
    acceptance = receipt.embedded_document(ACCEPTANCE_DOCUMENT, boundary)
    assert acceptance["causal_quarantine_retained"] is True
    assert acceptance["research_eligibility_granted"] is False
    assert acceptance["provider_call_count"] == 0
    assert acceptance["validations"][0]["record_count"] == 1

    anomaly_payload = json.loads(anomalies.read_text(encoding="utf-8"))
    anomaly_payload["promotion_requirement"] = "mutated"
    anomalies.write_text(json.dumps(anomaly_payload), encoding="utf-8")
    with pytest.raises(IntegrityError, match="changed after catalog creation"):
        assert_m2b_source_eligible(
            selection, acceptance_receipts=(receipt,), boundary=boundary
        )


def test_default_decode_consumes_only_one_bounded_chunk(tmp_path, monkeypatch) -> None:
    path = _write_pair(tmp_path)
    import futures_rebuild.dbn_catalog as module

    observed = 0
    original = module._record_summary

    def count(record):
        nonlocal observed
        observed += 1
        return original(record)

    monkeypatch.setattr(module, "_record_summary", count)
    validate_dbn_pair(
        path,
        dbn_root=tmp_path / "data" / "dbn",
        expected_schema="ohlcv-1m",
        role="canonical",
        sample_records=1,
        scan_to_end=False,
    )
    assert observed == 1


def test_array_iteration_modes_never_materialize_unbounded_full_scan() -> None:
    class FakeStore:
        def __init__(self):
            self.counts = []
            self.consumed = 0

        def to_ndarray(self, *, count):
            self.counts.append(count)

            def chunks():
                self.consumed += 1
                yield [1]
                self.consumed += 1
                yield [2]

            return chunks()

    sampled = FakeStore()
    assert list(_iter_arrays(sampled, scan_to_end=False, sample_records=1)) == [[1]]
    assert sampled.counts == [1] and sampled.consumed == 1
    full = FakeStore()
    assert list(_iter_arrays(full, scan_to_end=True, sample_records=1)) == [[1], [2]]
    assert full.counts == [FULL_SCAN_CHUNK_RECORDS] and full.consumed == 2


def test_decoded_metadata_coverage_mismatch_fails(tmp_path, monkeypatch) -> None:
    path = _write_pair(tmp_path)
    import futures_rebuild.dbn_catalog as module

    original = module._decode_summary

    def mismatched(*args, **kwargs):
        result = original(*args, **kwargs)
        result["metadata_end_ns"] -= 1
        return result

    monkeypatch.setattr(module, "_decode_summary", mismatched)
    with pytest.raises(IntegrityError, match="coverage disagrees"):
        validate_dbn_pair(
            path,
            dbn_root=tmp_path / "data" / "dbn",
            expected_schema="ohlcv-1m",
            role="canonical",
        )


def test_unsupported_decoder_failure_is_not_accepted(tmp_path, monkeypatch) -> None:
    path = _write_pair(tmp_path)
    import futures_rebuild.dbn_catalog as module

    def unsupported(*args, **kwargs):
        raise ValueError("unsupported")

    monkeypatch.setattr(module, "_iter_arrays", unsupported)
    with pytest.raises(IntegrityError, match="cannot be decoded"):
        _decode_summary(path, sample_records=1, scan_to_end=False)


def test_sidecar_tamper_fails_closed(tmp_path) -> None:
    path = _write_pair(tmp_path)
    sidecar_path = Path(f"{path}.manifest.json")
    payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    payload["file_size_bytes"] += 1
    sidecar_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(IntegrityError):
        validate_dbn_pair(
            path,
            dbn_root=tmp_path / "data" / "dbn",
            expected_schema="ohlcv-1m",
            role="canonical",
        )


def test_source_selection_includes_exact_files_and_refuses_ambiguous_duplicate(tmp_path, monkeypatch) -> None:
    def forbid_network(*args, **kwargs):
        raise AssertionError("offline catalog attempted network access")

    monkeypatch.setattr(socket, "create_connection", forbid_network)
    legacy = tmp_path / "legacy"
    _write_pair(legacy)
    boundary, contract, anomalies = _catalog_context(tmp_path, legacy)
    manifest = build_source_selection_manifest(
        legacy,
        contract,
        boundary=boundary,
        known_anomaly_contract_path=anomalies,
    )
    assert len(manifest["files"]) == 1
    assert manifest["selection_policy"] == "EXACT_CONTRACT_ALL_FILES_NO_RECURSIVE_NEWEST"
    assert "INSTRUMENT_ID_DATE_UTC" in manifest["actual_identity_authority"]
    assert manifest["record_scan_policy"] == "METADATA_PLUS_FIRST_SAMPLE"
    assert manifest["files"][0]["family"] == "dbn_ohlcv_1m"
    assert manifest["files"][0]["query_stype_in"] == "continuous"
    assert manifest["files"][0]["query_symbols"] == ["ES.v.0"]
    assert manifest["files"][0]["decode"]["stype_in"] == "continuous"
    assert manifest["files"][0]["decode"]["symbols"] == ["ES.v.0"]
    assert manifest["files"][0]["decode"]["record_count"] is None
    _, duplicate_contract, _ = _catalog_context(
        tmp_path, legacy, duplicate=True, active=boundary.active_root
    )
    with pytest.raises(IntegrityError):
        build_source_selection_manifest(
            legacy,
            duplicate_contract,
            boundary=boundary,
            known_anomaly_contract_path=anomalies,
        )


def test_full_scan_requires_family_filter_and_resource_ceiling(tmp_path) -> None:
    legacy = tmp_path / "legacy"
    path = _write_pair(legacy)
    boundary, contract, anomalies = _catalog_context(tmp_path, legacy)
    with pytest.raises(ContractError):
        build_source_selection_manifest(
            legacy, contract, boundary=boundary,
            known_anomaly_contract_path=anomalies, scan_to_end=True
        )
    with pytest.raises(ContractError):
        build_source_selection_manifest(
            legacy,
            contract,
            boundary=boundary,
            known_anomaly_contract_path=anomalies,
            scan_to_end=True,
            family_ids=("dbn_ohlcv_1m",),
            max_full_scan_bytes=path.stat().st_size - 1,
        )
    result = build_source_selection_manifest(
        legacy,
        contract,
        boundary=boundary,
        known_anomaly_contract_path=anomalies,
        scan_to_end=True,
        family_ids=("dbn_ohlcv_1m",),
        max_full_scan_bytes=path.stat().st_size,
    )
    assert result["files"][0]["decode"]["record_count"] == 2


def test_all_supported_schema_enums_normalize_exactly() -> None:
    expected = {
        dbn.Schema.DEFINITION: "definition",
        dbn.Schema.OHLCV_1D: "ohlcv-1d",
        dbn.Schema.OHLCV_1H: "ohlcv-1h",
        dbn.Schema.OHLCV_1M: "ohlcv-1m",
        dbn.Schema.OHLCV_1S: "ohlcv-1s",
        dbn.Schema.STATISTICS: "statistics",
        dbn.Schema.STATUS: "status",
        dbn.Schema.TRADES: "trades",
    }
    assert {_normalize_schema(value) for value in expected} == set(expected.values())


def test_immutable_catalog_output_collision_leaves_no_temp(tmp_path) -> None:
    target = tmp_path / "catalog.json"
    _atomic_write(target, {"id": 1})
    assert target.read_bytes() == b'{"id":1}\n'
    with pytest.raises(IntegrityError):
        _atomic_write(target, {"id": 2})
    assert not list(tmp_path.glob(".*.tmp"))


def test_catalog_output_is_confined_to_exact_active_state_subtree(boundary, tmp_path) -> None:
    valid = boundary.active_root / "state" / "source_selection" / "selection.json"
    assert _validated_catalog_output(boundary, valid) == valid.resolve(strict=False)
    for invalid in (
        tmp_path / "outside.json",
        boundary.active_root / "catalog.json",
        boundary.active_root / "state" / "source_selection" / "nested" / "x.json",
        boundary.active_root / "state" / "source_selection" / ".hidden.json",
        boundary.active_root / "state" / "source_selection" / "not-json.txt",
    ):
        with pytest.raises((ContractError, UnauthorizedOperation)):
            _validated_catalog_output(boundary, invalid)


def test_exact_hash_pinned_overlap_selects_broad_and_preserves_redundant(tmp_path) -> None:
    day = 86_400_000_000_000
    first = dbn.OHLCVMsg(
        dbn.RType.OHLCV_1M, 1, 100, START_NS,
        1000000000, 1100000000, 900000000, 1050000000, 10,
    )
    overlap_record = dbn.OHLCVMsg(
        dbn.RType.OHLCV_1M, 1, 101, START_NS + day,
        1000000000, 1100000000, 900000000, 1050000000, 11,
    )
    legacy = tmp_path / "legacy"
    broad = _write_custom_pair(
        legacy,
        filename="2026-01-01_2026-01-03.dbn.zst",
        metadata_start=START_NS,
        metadata_end=START_NS + 2 * day,
        records=[first, overlap_record],
    )
    narrow = _write_custom_pair(
        legacy,
        filename="2026-01-02_2026-01-03.dbn.zst",
        metadata_start=START_NS + day,
        metadata_end=START_NS + 2 * day,
        records=[overlap_record],
    )
    boundary, contract, anomalies = _catalog_context(tmp_path, legacy)
    proof = hashlib.sha256(bytes(overlap_record)).hexdigest()
    resolution = {
        "contract_version": "1.0.0",
        "decoder_versions": {"databento": "0.78.0", "databento-dbn": "0.58.0"},
        "proof_algorithm": "sha256_sorted_full_record_bytes_v1",
        "resolutions": [{
            "family": "dbn_ohlcv_1m",
            "market": "ES",
            "schema": "ohlcv-1m",
            "authoritative_path": "data/dbn/ohlcv_1m/ES/2026/2026-01-01_2026-01-03.dbn.zst",
            "authoritative_file_sha256": sha256_file(broad),
            "redundant_path": "data/dbn/ohlcv_1m/ES/2026/2026-01-02_2026-01-03.dbn.zst",
            "redundant_file_sha256": sha256_file(narrow),
            "overlap_start": "2026-01-02",
            "overlap_end": "2026-01-03",
            "timestamp_field": "ts_event",
            "record_count": 1,
            "record_subset_sha256": proof,
        }],
    }
    resolution_path = boundary.active_root / "configs" / "overlaps.json"
    resolution_path.write_text(json.dumps(resolution), encoding="utf-8")
    result = build_source_selection_manifest(
        legacy,
        contract,
        boundary=boundary,
        known_anomaly_contract_path=anomalies,
        overlap_contract_path=resolution_path,
    )
    dispositions = {item["path"]: item["coverage_disposition"] for item in result["files"]}
    assert dispositions[resolution["resolutions"][0]["authoritative_path"]].startswith(
        "AUTHORITATIVE"
    )
    assert dispositions[resolution["resolutions"][0]["redundant_path"]] == (
        "REDUNDANT_EXACT_CROSSCHECK_ONLY"
    )
    resolution["resolutions"][0]["record_subset_sha256"] = "0" * 64
    resolution_path.write_text(json.dumps(resolution), encoding="utf-8")
    with pytest.raises(IntegrityError, match="record-subset proof"):
        build_source_selection_manifest(
            legacy,
            contract,
            boundary=boundary,
            known_anomaly_contract_path=anomalies,
            overlap_contract_path=resolution_path,
        )


def test_parent_suffix_is_validated_but_excluded_from_foundation_selection(
    tmp_path,
) -> None:
    record = dbn.OHLCVMsg(
        dbn.RType.OHLCV_1M,
        1,
        100,
        START_NS,
        1000000000,
        1100000000,
        900000000,
        1050000000,
        10,
    )
    legacy = tmp_path / "legacy"
    _write_custom_pair(
        legacy,
        filename="2026-01-01_2026-01-02.dbn.zst",
        metadata_start=START_NS,
        metadata_end=END_NS,
        records=[record],
    )
    _write_custom_pair(
        legacy,
        filename="2026-01-01_2026-01-02.parent.dbn.zst",
        metadata_start=START_NS,
        metadata_end=END_NS,
        records=[record],
        stype_in="parent",
    )
    boundary, contract, anomalies = _catalog_context(tmp_path, legacy)
    result = build_source_selection_manifest(
        legacy,
        contract,
        boundary=boundary,
        known_anomaly_contract_path=anomalies,
    )
    assert len(result["files"]) == 1
    assert len(result["diagnostic_files"]) == 1
    diagnostic = result["diagnostic_files"][0]
    assert diagnostic["coverage_disposition"] == (
        "DIAGNOSTIC_PARENT_QUERY_IDENTITY_ONLY_NOT_FOUNDATION_ELIGIBLE"
    )
    assert diagnostic["query_stype_in"] == "parent"


def test_catalog_binds_verified_layout_v2_dbn_manifest(tmp_path) -> None:
    legacy = tmp_path / "legacy"
    source = _write_pair(legacy)
    project = tmp_path / "project"
    project.mkdir()
    (project / "bundles").mkdir()
    copy_boundary = RepoBoundary(project.resolve(), (legacy.resolve(),), ())
    operation = OperationReceipt.issue_local(
        copy_boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
    )
    publisher = AtomicPublisher(
        boundary=copy_boundary,
        operation_receipt=operation,
        lock_path=project / "state" / "locks" / "dbn-layout.lock",
    )
    stage = publisher.create_stage("dbn")
    filename = source.name
    sidecar = Path(f"{source}.manifest.json")
    (stage / filename).write_bytes(source.read_bytes())
    (stage / sidecar.name).write_bytes(sidecar.read_bytes())
    logical_root = "data/dbn/ohlcv_1m/ES/2026"
    manifest = ReleaseManifest.build(
        stage,
        phase="dbn",
        release_kind="futures_phase1a_verified_dbn",
        schema_version="1.0.0",
        logical_paths={
            filename: f"{logical_root}/{filename}",
            sidecar.name: f"{logical_root}/{sidecar.name}",
        },
    )
    dbn_manifest_path = publisher.publish(stage, manifest)
    dbn_receipt = VerifiedReleaseReceipt.from_manifest(
        dbn_manifest_path, copy_boundary
    )
    boundary, source_contract, anomalies = _catalog_context(
        tmp_path, legacy, active=project
    )
    layout_contract = project / "configs" / "data_layout_contract.json"
    layout_contract.write_bytes(
        (
            Path(__file__).resolve().parents[1]
            / "configs"
            / "data_layout_contract.json"
        ).read_bytes()
    )
    contract_payload = json.loads(source_contract.read_text(encoding="utf-8"))
    contract_payload.update(
        {
            "contract_version": "2.0.0",
            "provider": {
                "name": "Databento",
                "dataset": "GLBX.MDP3",
                "paid_calls_authorized": False,
                "downloads_authorized": False,
            },
            "data_layout": {
                "layout_version": "2.0.0",
                "layout_contract_path": "configs/data_layout_contract.json",
                "layout_contract_sha256": sha256_file(layout_contract),
                "manifest_root": "manifests/data_releases",
                "staging_root": "state/data_publication_staging",
                "phase1b_logical_template": (
                    "data/raw/{market}/{year}/{interval}/{filename}"
                ),
                "phase1b_physical_template": (
                    "data/raw/{market}/{year}/{interval}/{release-id}/{filename}"
                ),
                "phase2_logical_template": (
                    "data/causally_gated_normalized/{market}/{year}/{interval}/{filename}"
                ),
                "phase2_physical_template": (
                    "data/causally_gated_normalized/{market}/{year}/{interval}/"
                    "{release-id}/{filename}"
                ),
            },
            "canonical_dbn_release": {
                "phase": dbn_receipt.phase,
                "release_id": dbn_receipt.release_id,
                "release_kind": dbn_receipt.release_kind,
                "schema_version": dbn_receipt.schema_version,
                "manifest_path": dbn_receipt.manifest_path,
                "manifest_sha256": dbn_receipt.manifest_sha256,
                "dbn_files": 1,
                "sidecar_files": 1,
                "combined_files": 2,
                "combined_bytes": sum(item.size for item in manifest.files),
            },
        }
    )
    source_contract.write_text(json.dumps(contract_payload), encoding="utf-8")
    result = build_source_selection_manifest(
        project,
        source_contract,
        boundary=boundary,
        known_anomaly_contract_path=anomalies,
        source_dbn_manifest_path=dbn_manifest_path,
    )
    assert result["source_scope"] == "VERIFIED_LAYOUT_V2_DBN_RELEASE"
    assert result["source_dbn_release_id"] == manifest.release_id
    contract_payload["contract_version"] = "2.1.0"
    contract_payload["legacy_repository"] = None
    contract_payload["external_repository_access"] = "FORBIDDEN"
    source_contract.write_text(json.dumps(contract_payload), encoding="utf-8")
    retired_result = build_source_selection_manifest(
        project,
        source_contract,
        boundary=boundary,
        known_anomaly_contract_path=anomalies,
        source_dbn_manifest_path=dbn_manifest_path,
    )
    assert retired_result["source_dbn_release_id"] == manifest.release_id
    payload_path = dbn_receipt.resolve_file(f"{logical_root}/{filename}", boundary)
    payload_path.write_bytes(payload_path.read_bytes() + b"tamper")
    with pytest.raises(IntegrityError, match="failed verification"):
        build_source_selection_manifest(
            project,
            source_contract,
            boundary=boundary,
            known_anomaly_contract_path=anomalies,
            source_dbn_manifest_path=dbn_manifest_path,
        )
