"""Immutable Phase 1B/Phase 2 materialization from a verified layout-v2 DBN release."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from ..boundary import RepoBoundary
from ..canonical import canonical_bytes, sha256_file, sha256_json
from ..errors import ContractError, IntegrityError
from ..data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from ..source_symbology import require_query_contract
from .parquet import (
    CAUSAL_BAR_SCHEMA,
    DEFINITION_SCHEMA,
    RAW_BAR_SCHEMA,
    read_definition_audit,
    read_raw_bar_audit,
    read_causal_bar_census,
    write_raw_bars,
    write_relevant_definitions,
    write_causal_bars,
)
from .snapshot import DBN_NAME, SnapshotFile
from .support import VerifiedFoundationPolicies


RAW_RELEASE_KIND = "futures_phase1b_actual_raw_interval"
RAW_SCHEMA_VERSION = "3.0.0"
CAUSAL_RELEASE_KIND = "futures_phase2_causal_interval"
CAUSAL_SCHEMA_VERSION = "2.0.0"
CAUSAL_CLOCK_CONTRACT = "TS_RECV_INDEX_TS_EVENT_AUDIT_ONLY"
FOUNDATION_TRANSFORMS = (
    "EXACT_DBN_DECODE",
    "ACTUAL_BAR_PUBLISHER_INSTRUMENT_UTC_DATE_SELECTION",
    "DEFINITION_LIFECYCLE_FIELDS_PRESERVED",
    "NANOUNITS_PRESERVED_AS_INT64",
    "PROVIDER_CLOCKS_PRESERVED_WITHOUT_CROSS_CLOCK_ORDER_ASSUMPTION",
    "UTC_NANOSECONDS_PRESERVED_AS_INTEGERS",
)


@dataclass(frozen=True)
class LoadedRawInterval:
    receipt: VerifiedReleaseReceipt
    interval_receipt: dict[str, object]
    bars_path: Path
    definitions_path: Path

    def verify(self, boundary: RepoBoundary) -> None:
        manifest = self.receipt.verify(boundary)
        logical_root = str(self.interval_receipt.get("logical_root", ""))
        if (
            manifest.release_kind != RAW_RELEASE_KIND
            or manifest.schema_version != RAW_SCHEMA_VERSION
            or self.bars_path
            != self.receipt.resolve_file(f"{logical_root}/bars.parquet", boundary)
            or self.definitions_path
            != self.receipt.resolve_file(f"{logical_root}/definitions.parquet", boundary)
        ):
            raise IntegrityError("loaded raw interval no longer matches its verified release")


_RAW_INTERVAL_AUDIT_CACHE: dict[
    tuple[str, str], tuple[Path, Path, dict[str, object]]
] = {}
_CAUSAL_INTERVAL_AUDIT_CACHE: dict[
    tuple[str, str], tuple[Path, dict[str, object]]
] = {}


def _relative_root(market: str, year: int, filename: str) -> str:
    match = DBN_NAME.fullmatch(filename)
    if (
        match is None
        or re.fullmatch(r"[0-9A-Z]{2,3}", market) is None
        or isinstance(year, bool)
        or not isinstance(year, int)
        or int(match.group("start")[:4]) != year
    ):
        raise ContractError("raw interval market/year/filename selector is invalid")
    interval = f"{match.group('start')}_{match.group('end')}"
    return f"data/raw/{market}/{year}/{interval}"


def materialize_raw_interval(
    *,
    definition_binding: SnapshotFile,
    bar_binding: SnapshotFile,
    definition_query_contract: Mapping[str, object],
    bar_query_contract: Mapping[str, object],
    market: str,
    year: int,
    filename: str,
    source_selection_release_id: str,
    publisher: AtomicPublisher,
    batch_rows: int = 100_000,
) -> VerifiedReleaseReceipt:
    if re.fullmatch(r"[0-9a-f]{64}", source_selection_release_id) is None:
        raise ContractError("source selection release ID is invalid")
    definition_query = require_query_contract(definition_query_contract)
    bar_query = require_query_contract(bar_query_contract)
    if (
        definition_binding.source_release_id != bar_binding.source_release_id
        or definition_binding.source_manifest_sha256
        != bar_binding.source_manifest_sha256
        or Path(definition_binding.relative_path).name != filename
        or Path(bar_binding.relative_path).name != filename
        or definition_query["schema"] != "definition"
        or bar_query["schema"] != "ohlcv-1m"
        or definition_query["market"] != market
        or bar_query["market"] != market
        or definition_query["start"] != bar_query["start"]
        or definition_query["end"] != bar_query["end"]
    ):
        raise IntegrityError("definition/bar interval bindings do not share one DBN release")
    logical_root = _relative_root(market, year, filename)
    stage = publisher.create_stage("phase1b_raw")
    bars_path = stage / logical_root / "bars.parquet"
    definitions_path = stage / logical_root / "definitions.parquet"
    bar_count, instrument_dates = write_raw_bars(
        bar_binding,
        market=market,
        expected_query_contract=bar_query,
        output=bars_path,
        batch_rows=batch_rows,
    )
    (
        scanned,
        selected,
        definition_timestamp_census,
        definition_identity_date_keys,
    ) = write_relevant_definitions(
        definition_binding,
        market=market,
        expected_query_contract=definition_query,
        required_instrument_dates=instrument_dates,
        output=definitions_path,
        batch_rows=batch_rows,
    )
    unmatched_identity_date_keys = instrument_dates - definition_identity_date_keys
    core = {
        "bar_identity_date_key_count": len(instrument_dates),
        "bar_identity_date_key_set_sha256": sha256_json(sorted(instrument_dates)),
        "bar_rows": bar_count,
        "bars_parquet_sha256": sha256_file(bars_path),
        "bars_schema": RAW_BAR_SCHEMA.metadata[b"schema_id"].decode("ascii"),
        "definition_rows_scanned": scanned,
        "definition_rows_selected": selected,
        "definition_identity_date_key_count": len(definition_identity_date_keys),
        "definition_identity_date_key_set_sha256": sha256_json(
            sorted(definition_identity_date_keys)
        ),
        "definition_timestamp_census": definition_timestamp_census,
        "definition_query_contract": definition_query,
        "definition_query_contract_id": definition_query["query_contract_id"],
        "definitions_parquet_sha256": sha256_file(definitions_path),
        "definitions_schema": DEFINITION_SCHEMA.metadata[b"schema_id"].decode("ascii"),
        "foundation_transforms": list(FOUNDATION_TRANSFORMS),
        "learned_or_outcome_informed_transform_count": 0,
        "logical_root": logical_root,
        "market": market,
        "bar_query_contract": bar_query,
        "bar_query_contract_id": bar_query["query_contract_id"],
        "source_bar_file_path": bar_binding.relative_path,
        "source_bar_file_sha256": bar_binding.sha256,
        "source_definition_file_path": definition_binding.relative_path,
        "source_definition_file_sha256": definition_binding.sha256,
        "source_dbn_manifest_sha256": bar_binding.source_manifest_sha256,
        "source_dbn_release_id": bar_binding.source_release_id,
        "source_selection_release_id": source_selection_release_id,
        "unmatched_bar_identity_date_key_count": len(unmatched_identity_date_keys),
        "unmatched_bar_identity_date_key_set_sha256": sha256_json(
            sorted(unmatched_identity_date_keys)
        ),
        "year": year,
    }
    interval_receipt = {**core, "interval_id": sha256_json(core)}
    receipt_path = stage / logical_root / "interval_receipt.json"
    receipt_path.write_bytes(canonical_bytes(interval_receipt) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        phase="raw",
        release_kind=RAW_RELEASE_KIND,
        schema_version=RAW_SCHEMA_VERSION,
        logical_paths={
            path.relative_to(stage).as_posix(): path.relative_to(stage).as_posix()
            for path in stage.rglob("*")
            if path.is_file()
        },
        source_release_ids=(
            bar_binding.source_release_id,
            source_selection_release_id,
        ),
        metadata={
            "interval_id": interval_receipt["interval_id"],
            "logical_root": logical_root,
            "market": market,
            "year": year,
        },
    )
    manifest_path = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )
    # The producer already performed the exact row/timestamp census while
    # building these immutable files.  Prime only the process-local semantic
    # audit cache; load_raw_interval still verifies the manifest, every file
    # hash, the canonical receipt, and the cached payload before using it.
    bars = receipt.resolve_file(f"{logical_root}/bars.parquet", publisher.boundary)
    definitions = receipt.resolve_file(
        f"{logical_root}/definitions.parquet", publisher.boundary
    )
    _RAW_INTERVAL_AUDIT_CACHE[
        (publisher.boundary.repository_id, receipt.receipt_id)
    ] = (bars, definitions, dict(interval_receipt))
    return receipt


def load_raw_interval(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> LoadedRawInterval:
    manifest = receipt.verify(boundary)
    if (
        manifest.release_kind != RAW_RELEASE_KIND
        or manifest.schema_version != RAW_SCHEMA_VERSION
        or set(manifest.metadata) != {"interval_id", "logical_root", "market", "year"}
    ):
        raise IntegrityError("raw interval release kind/schema/metadata is invalid")
    logical_root = manifest.metadata["logical_root"]
    market = manifest.metadata["market"]
    year = manifest.metadata["year"]
    if (
        not isinstance(logical_root, str)
        or not isinstance(market, str)
        or isinstance(year, bool)
        or not isinstance(year, int)
        or not logical_root.startswith(f"data/raw/{market}/{year}/")
    ):
        raise IntegrityError("raw interval release selectors are invalid")
    expected_paths = {
        f"{logical_root}/bars.parquet",
        f"{logical_root}/definitions.parquet",
        f"{logical_root}/interval_receipt.json",
    }
    if {entry.path for entry in manifest.files} != expected_paths:
        raise IntegrityError("raw interval release file set is not exact")
    receipt_path = receipt.resolve_file(f"{logical_root}/interval_receipt.json", boundary)
    try:
        raw = receipt_path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("raw interval receipt JSON is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("raw interval receipt is not canonical JSON")
    expected_payload_fields = {
        "bar_query_contract",
        "bar_query_contract_id",
        "bar_identity_date_key_count",
        "bar_identity_date_key_set_sha256",
        "bar_rows",
        "bars_parquet_sha256",
        "bars_schema",
        "definition_query_contract",
        "definition_query_contract_id",
        "definition_identity_date_key_count",
        "definition_identity_date_key_set_sha256",
        "definition_rows_scanned",
        "definition_rows_selected",
        "definition_timestamp_census",
        "definitions_parquet_sha256",
        "definitions_schema",
        "foundation_transforms",
        "interval_id",
        "learned_or_outcome_informed_transform_count",
        "logical_root",
        "market",
        "source_bar_file_path",
        "source_bar_file_sha256",
        "source_definition_file_path",
        "source_definition_file_sha256",
        "source_dbn_manifest_sha256",
        "source_dbn_release_id",
        "source_selection_release_id",
        "unmatched_bar_identity_date_key_count",
        "unmatched_bar_identity_date_key_set_sha256",
        "year",
    }
    if set(payload) != expected_payload_fields:
        raise IntegrityError("raw interval receipt JSON schema is not exact")
    interval_id = payload.pop("interval_id", None)
    if (
        interval_id != sha256_json(payload)
        or interval_id != manifest.metadata["interval_id"]
        or payload.get("logical_root") != logical_root
        or payload.get("market") != market
        or payload.get("year") != year
        or payload.get("bars_schema")
        != RAW_BAR_SCHEMA.metadata[b"schema_id"].decode("ascii")
        or payload.get("definitions_schema")
        != DEFINITION_SCHEMA.metadata[b"schema_id"].decode("ascii")
        or tuple(payload.get("foundation_transforms", ())) != FOUNDATION_TRANSFORMS
        or payload.get("learned_or_outcome_informed_transform_count") != 0
    ):
        raise IntegrityError("raw interval receipt content address is invalid")
    try:
        definition_query = require_query_contract(payload.get("definition_query_contract"))
        bar_query = require_query_contract(payload.get("bar_query_contract"))
    except (ContractError, IntegrityError) as exc:
        raise IntegrityError("raw interval query contracts are invalid") from exc
    if (
        payload.get("definition_query_contract_id")
        != definition_query["query_contract_id"]
        or payload.get("bar_query_contract_id") != bar_query["query_contract_id"]
        or definition_query["schema"] != "definition"
        or bar_query["schema"] != "ohlcv-1m"
        or definition_query["market"] != market
        or bar_query["market"] != market
        or definition_query["start"] != bar_query["start"]
        or definition_query["end"] != bar_query["end"]
        or set(manifest.source_release_ids)
        != {
            payload.get("source_dbn_release_id"),
            payload.get("source_selection_release_id"),
        }
    ):
        raise IntegrityError("raw interval query/dependency binding is invalid")
    payload["interval_id"] = interval_id
    bars = receipt.resolve_file(f"{logical_root}/bars.parquet", boundary)
    definitions = receipt.resolve_file(f"{logical_root}/definitions.parquet", boundary)
    if (
        sha256_file(bars) != payload["bars_parquet_sha256"]
        or sha256_file(definitions) != payload["definitions_parquet_sha256"]
    ):
        raise IntegrityError("raw interval Parquet hashes differ from interval receipt")
    cache_key = (boundary.repository_id, receipt.receipt_id)
    cached = _RAW_INTERVAL_AUDIT_CACHE.get(cache_key)
    if cached is not None:
        cached_bars, cached_definitions, cached_payload = cached
        if (
            cached_bars != bars
            or cached_definitions != definitions
            or cached_payload != payload
        ):
            raise IntegrityError("cached raw interval audit differs from verified bytes")
        loaded = LoadedRawInterval(
            receipt,
            dict(cached_payload),
            cached_bars,
            cached_definitions,
        )
        loaded.verify(boundary)
        return loaded
    observed_bar_count, observed_bar_keys = read_raw_bar_audit(bars)
    observed_definition_census, observed_definition_keys = read_definition_audit(
        definitions
    )
    observed_unmatched_keys = observed_bar_keys - observed_definition_keys
    if (
        observed_bar_count != payload.get("bar_rows")
        or len(observed_bar_keys) != payload.get("bar_identity_date_key_count")
        or sha256_json(sorted(observed_bar_keys))
        != payload.get("bar_identity_date_key_set_sha256")
        or observed_definition_census != payload.get("definition_timestamp_census")
        or observed_definition_census["row_count"]
        != payload.get("definition_rows_selected")
        or len(observed_definition_keys)
        != payload.get("definition_identity_date_key_count")
        or sha256_json(sorted(observed_definition_keys))
        != payload.get("definition_identity_date_key_set_sha256")
        or len(observed_unmatched_keys)
        != payload.get("unmatched_bar_identity_date_key_count")
        or sha256_json(sorted(observed_unmatched_keys))
        != payload.get("unmatched_bar_identity_date_key_set_sha256")
        or not isinstance(payload.get("definition_rows_scanned"), int)
        or payload["definition_rows_scanned"] < payload["definition_rows_selected"]
    ):
        raise IntegrityError("raw interval row/timestamp census is invalid")
    loaded = LoadedRawInterval(receipt, payload, bars, definitions)
    loaded.verify(boundary)
    _RAW_INTERVAL_AUDIT_CACHE[cache_key] = (
        bars,
        definitions,
        dict(payload),
    )
    return loaded


def materialize_causal_interval(
    *,
    raw_receipt: VerifiedReleaseReceipt,
    policies: VerifiedFoundationPolicies,
    publisher: AtomicPublisher,
    batch_rows: int = 100_000,
) -> VerifiedReleaseReceipt:
    if policies.boundary.repository_id != publisher.boundary.repository_id:
        raise IntegrityError("foundation policies belong to a different repository")
    loaded = load_raw_interval(raw_receipt, boundary=publisher.boundary)
    policies.verify()
    raw_root = str(loaded.interval_receipt["logical_root"])
    if not raw_root.startswith("data/raw/"):
        raise IntegrityError("raw interval logical root is invalid")
    causal_root = (
        "data/causally_gated_normalized/"
        f"{raw_root.removeprefix('data/raw/')}"
    )
    stage = publisher.create_stage("phase2_causal")
    output = stage / causal_root / "bars.parquet"
    row_count, dispositions, epoch_counts = write_causal_bars(
        raw_bars_path=loaded.bars_path,
        definitions_path=loaded.definitions_path,
        policies=policies,
        source_raw_release_id=raw_receipt.release_id,
        output=output,
        batch_rows=batch_rows,
    )
    core = {
        "causal_clock_contract": CAUSAL_CLOCK_CONTRACT,
        "causal_parquet_sha256": sha256_file(output),
        "causal_schema": CAUSAL_BAR_SCHEMA.metadata[b"schema_id"].decode("ascii"),
        "disposition_counts": dispositions,
        "foundation_policy_release_id": policies.receipt.release_id,
        "foundation_policy_set_id": policies.policy_set_id,
        "learned_or_outcome_informed_transform_count": 0,
        "logical_root": causal_root,
        "market": loaded.interval_receipt["market"],
        "prediction_in_coverage_denominator_rows": row_count,
        "provider_data_epochs_sha256": policies.foundation.provider_data_epochs_sha256,
        "provider_timestamp_epoch_counts": epoch_counts,
        "row_count": row_count,
        "source_raw_interval_id": loaded.interval_receipt["interval_id"],
        "source_raw_release_id": raw_receipt.release_id,
        "year": loaded.interval_receipt["year"],
    }
    interval_receipt = {**core, "causal_interval_id": sha256_json(core)}
    receipt_path = stage / causal_root / "causal_interval_receipt.json"
    receipt_path.write_bytes(canonical_bytes(interval_receipt) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        phase="causally_gated_normalized",
        release_kind=CAUSAL_RELEASE_KIND,
        schema_version=CAUSAL_SCHEMA_VERSION,
        logical_paths={
            path.relative_to(stage).as_posix(): path.relative_to(stage).as_posix()
            for path in stage.rglob("*")
            if path.is_file()
        },
        source_release_ids=(raw_receipt.release_id, policies.receipt.release_id),
        metadata={
            "causal_interval_id": interval_receipt["causal_interval_id"],
            "logical_root": causal_root,
            "market": loaded.interval_receipt["market"],
            "year": loaded.interval_receipt["year"],
        },
    )
    manifest_path = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_manifest(
        manifest_path, publisher.boundary
    )
    # write_causal_bars produced the exact disposition/epoch census embedded
    # above.  Avoid rescanning the same immutable Parquet in this process;
    # load_causal_interval still authenticates the release, file hash, receipt,
    # and cached payload before accepting this semantic evidence.
    bars = receipt.resolve_file(f"{causal_root}/bars.parquet", publisher.boundary)
    _CAUSAL_INTERVAL_AUDIT_CACHE[
        (publisher.boundary.repository_id, receipt.receipt_id)
    ] = (bars, dict(interval_receipt))
    return receipt


def load_causal_interval(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> tuple[Path, dict[str, object]]:
    manifest = receipt.verify(boundary)
    if (
        manifest.release_kind != CAUSAL_RELEASE_KIND
        or manifest.schema_version != CAUSAL_SCHEMA_VERSION
        or set(manifest.metadata)
        != {"causal_interval_id", "logical_root", "market", "year"}
    ):
        raise IntegrityError("causal interval release kind/schema/metadata is invalid")
    logical_root = manifest.metadata["logical_root"]
    if not isinstance(logical_root, str) or not logical_root.startswith(
        "data/causally_gated_normalized/"
    ):
        raise IntegrityError("causal interval logical root is invalid")
    expected = {
        f"{logical_root}/bars.parquet",
        f"{logical_root}/causal_interval_receipt.json",
    }
    if {entry.path for entry in manifest.files} != expected:
        raise IntegrityError("causal interval release file set is not exact")
    report_path = receipt.resolve_file(
        f"{logical_root}/causal_interval_receipt.json", boundary
    )
    try:
        raw = report_path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("causal interval receipt JSON is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("causal interval receipt is not canonical JSON")
    expected_payload_fields = {
        "causal_clock_contract",
        "causal_interval_id",
        "causal_parquet_sha256",
        "causal_schema",
        "disposition_counts",
        "foundation_policy_release_id",
        "foundation_policy_set_id",
        "learned_or_outcome_informed_transform_count",
        "logical_root",
        "market",
        "prediction_in_coverage_denominator_rows",
        "provider_data_epochs_sha256",
        "provider_timestamp_epoch_counts",
        "row_count",
        "source_raw_interval_id",
        "source_raw_release_id",
        "year",
    }
    if set(payload) != expected_payload_fields:
        raise IntegrityError("causal interval receipt JSON schema is not exact")
    interval_id = payload.pop("causal_interval_id", None)
    if (
        interval_id != sha256_json(payload)
        or interval_id != manifest.metadata["causal_interval_id"]
        or payload.get("logical_root") != logical_root
        or payload.get("row_count")
        != payload.get("prediction_in_coverage_denominator_rows")
        or payload.get("causal_clock_contract") != CAUSAL_CLOCK_CONTRACT
        or payload.get("causal_schema")
        != CAUSAL_BAR_SCHEMA.metadata[b"schema_id"].decode("ascii")
        or payload.get("market") != manifest.metadata["market"]
        or payload.get("year") != manifest.metadata["year"]
        or payload.get("learned_or_outcome_informed_transform_count") != 0
        or set(manifest.source_release_ids)
        != {
            payload.get("source_raw_release_id"),
            payload.get("foundation_policy_release_id"),
        }
    ):
        raise IntegrityError("causal interval receipt content address/count is invalid")
    payload["causal_interval_id"] = interval_id
    bars = receipt.resolve_file(f"{logical_root}/bars.parquet", boundary)
    if sha256_file(bars) != payload["causal_parquet_sha256"]:
        raise IntegrityError("causal Parquet hash differs from interval receipt")
    cache_key = (boundary.repository_id, receipt.receipt_id)
    cached = _CAUSAL_INTERVAL_AUDIT_CACHE.get(cache_key)
    if cached is not None:
        cached_bars, cached_payload = cached
        if cached_bars != bars or cached_payload != payload:
            raise IntegrityError(
                "cached causal interval audit differs from verified bytes"
            )
        return cached_bars, dict(cached_payload)
    observed = read_causal_bar_census(bars)
    if (
        observed["row_count"] != payload.get("row_count")
        or observed["prediction_in_coverage_denominator_rows"]
        != payload.get("prediction_in_coverage_denominator_rows")
        or observed["disposition_counts"] != payload.get("disposition_counts")
        or observed["provider_timestamp_epoch_counts"]
        != payload.get("provider_timestamp_epoch_counts")
        or observed["foundation_policy_set_ids"]
        != [payload.get("foundation_policy_set_id")]
        or observed["source_raw_release_ids"]
        != [payload.get("source_raw_release_id")]
    ):
        raise IntegrityError("causal Parquet census differs from its interval receipt")
    _CAUSAL_INTERVAL_AUDIT_CACHE[cache_key] = (bars, dict(payload))
    return bars, payload
