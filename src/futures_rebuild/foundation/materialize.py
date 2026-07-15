"""Immutable Phase 1B interval materialization from verified DBN snapshots."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from ..boundary import RepoBoundary
from ..canonical import canonical_bytes, sha256_file, sha256_json
from ..errors import ContractError, IntegrityError
from ..release import AtomicPublisher, ReleaseManifest, VerifiedReleaseReceipt
from .parquet import (
    CAUSAL_BAR_SCHEMA,
    DEFINITION_SCHEMA,
    RAW_BAR_SCHEMA,
    read_definitions,
    write_raw_bars,
    write_relevant_definitions,
    write_causal_bars,
)
from .snapshot import DBN_NAME, SnapshotFile
from .support import VerifiedFoundationPolicies


RAW_RELEASE_KIND = "futures_phase1b_actual_raw_interval"
RAW_SCHEMA_VERSION = "1.0.0"
CAUSAL_RELEASE_KIND = "futures_phase2_causal_interval"
CAUSAL_SCHEMA_VERSION = "1.0.0"


@dataclass(frozen=True)
class LoadedRawInterval:
    receipt: VerifiedReleaseReceipt
    interval_receipt: dict[str, object]
    bars_path: Path
    definitions_path: Path

    def verify(self, boundary: RepoBoundary) -> None:
        manifest = self.receipt.verify(boundary)
        expected_root = boundary.active_root / self.receipt.relative_root
        if (
            manifest.release_kind != RAW_RELEASE_KIND
            or manifest.schema_version != RAW_SCHEMA_VERSION
            or self.bars_path.parent != self.definitions_path.parent
            or self.bars_path.parent.parent.parent.parent.parent != expected_root
        ):
            raise IntegrityError("loaded raw interval no longer matches its verified release")


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
    return f"raw/{market}/{year}/{interval}"


def materialize_raw_interval(
    *,
    definition_binding: SnapshotFile,
    bar_binding: SnapshotFile,
    market: str,
    year: int,
    filename: str,
    source_selection_release_id: str,
    publisher: AtomicPublisher,
    batch_rows: int = 100_000,
) -> VerifiedReleaseReceipt:
    if re.fullmatch(r"[0-9a-f]{64}", source_selection_release_id) is None:
        raise ContractError("source selection release ID is invalid")
    if (
        definition_binding.source_snapshot_id != bar_binding.source_snapshot_id
        or definition_binding.migration_manifest_sha256
        != bar_binding.migration_manifest_sha256
        or Path(definition_binding.relative_path).name != filename
        or Path(bar_binding.relative_path).name != filename
    ):
        raise IntegrityError("definition/bar interval bindings do not share one source snapshot")
    logical_root = _relative_root(market, year, filename)
    stage = publisher.create_stage("phase1b_raw")
    bars_path = stage / logical_root / "bars.parquet"
    definitions_path = stage / logical_root / "definitions.parquet"
    bar_count, instrument_ids = write_raw_bars(
        bar_binding,
        market=market,
        output=bars_path,
        batch_rows=batch_rows,
    )
    scanned, selected = write_relevant_definitions(
        definition_binding,
        market=market,
        required_instrument_ids=instrument_ids,
        output=definitions_path,
        batch_rows=batch_rows,
    )
    core = {
        "bar_rows": bar_count,
        "bars_parquet_sha256": sha256_file(bars_path),
        "bars_schema": RAW_BAR_SCHEMA.metadata[b"schema_id"].decode("ascii"),
        "definition_rows_scanned": scanned,
        "definition_rows_selected": selected,
        "definitions_parquet_sha256": sha256_file(definitions_path),
        "definitions_schema": DEFINITION_SCHEMA.metadata[b"schema_id"].decode("ascii"),
        "foundation_transforms": [
            "EXACT_DBN_DECODE",
            "ACTUAL_BAR_INSTRUMENT_ID_SELECTION",
            "NANOUNITS_PRESERVED_AS_INT64",
            "UTC_NANOSECONDS_PRESERVED_AS_INT64",
        ],
        "learned_or_outcome_informed_transform_count": 0,
        "logical_root": logical_root,
        "market": market,
        "source_bar_file_path": bar_binding.relative_path,
        "source_bar_file_sha256": bar_binding.sha256,
        "source_definition_file_path": definition_binding.relative_path,
        "source_definition_file_sha256": definition_binding.sha256,
        "source_manifest_sha256": bar_binding.migration_manifest_sha256,
        "source_selection_release_id": source_selection_release_id,
        "source_snapshot_id": bar_binding.source_snapshot_id,
        "year": year,
    }
    interval_receipt = {**core, "interval_id": sha256_json(core)}
    receipt_path = stage / logical_root / "interval_receipt.json"
    receipt_path.write_bytes(canonical_bytes(interval_receipt) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        release_kind=RAW_RELEASE_KIND,
        schema_version=RAW_SCHEMA_VERSION,
        source_release_ids=(
            bar_binding.source_snapshot_id,
            source_selection_release_id,
        ),
        metadata={
            "interval_id": interval_receipt["interval_id"],
            "logical_root": logical_root,
            "market": market,
            "year": year,
        },
    )
    release = publisher.publish(stage, manifest)
    return VerifiedReleaseReceipt.from_release(release, publisher.boundary)


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
        or not logical_root.startswith(f"raw/{market}/{year}/")
    ):
        raise IntegrityError("raw interval release selectors are invalid")
    expected_paths = {
        f"{logical_root}/bars.parquet",
        f"{logical_root}/definitions.parquet",
        f"{logical_root}/interval_receipt.json",
    }
    if {entry.path for entry in manifest.files} != expected_paths:
        raise IntegrityError("raw interval release file set is not exact")
    root = boundary.active_root / receipt.relative_root
    receipt_path = root / logical_root / "interval_receipt.json"
    try:
        raw = receipt_path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("raw interval receipt JSON is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("raw interval receipt is not canonical JSON")
    interval_id = payload.pop("interval_id", None)
    if (
        interval_id != sha256_json(payload)
        or interval_id != manifest.metadata["interval_id"]
        or payload.get("logical_root") != logical_root
        or payload.get("market") != market
        or payload.get("year") != year
    ):
        raise IntegrityError("raw interval receipt content address is invalid")
    payload["interval_id"] = interval_id
    bars = root / logical_root / "bars.parquet"
    definitions = root / logical_root / "definitions.parquet"
    if (
        sha256_file(bars) != payload["bars_parquet_sha256"]
        or sha256_file(definitions) != payload["definitions_parquet_sha256"]
    ):
        raise IntegrityError("raw interval Parquet hashes differ from interval receipt")
    read_definitions(definitions, batch_rows=1)
    loaded = LoadedRawInterval(receipt, payload, bars, definitions)
    loaded.verify(boundary)
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
    if not raw_root.startswith("raw/"):
        raise IntegrityError("raw interval logical root is invalid")
    causal_root = f"causal/{raw_root.removeprefix('raw/')}"
    stage = publisher.create_stage("phase2_causal")
    output = stage / causal_root / "bars.parquet"
    row_count, dispositions = write_causal_bars(
        raw_bars_path=loaded.bars_path,
        definitions_path=loaded.definitions_path,
        policies=policies,
        source_raw_release_id=raw_receipt.release_id,
        output=output,
        batch_rows=batch_rows,
    )
    core = {
        "causal_parquet_sha256": sha256_file(output),
        "causal_schema": CAUSAL_BAR_SCHEMA.metadata[b"schema_id"].decode("ascii"),
        "disposition_counts": dispositions,
        "foundation_policy_release_id": policies.receipt.release_id,
        "foundation_policy_set_id": policies.policy_set_id,
        "learned_or_outcome_informed_transform_count": 0,
        "logical_root": causal_root,
        "prediction_in_coverage_denominator_rows": row_count,
        "row_count": row_count,
        "source_raw_interval_id": loaded.interval_receipt["interval_id"],
        "source_raw_release_id": raw_receipt.release_id,
    }
    interval_receipt = {**core, "causal_interval_id": sha256_json(core)}
    receipt_path = stage / causal_root / "causal_interval_receipt.json"
    receipt_path.write_bytes(canonical_bytes(interval_receipt) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        release_kind=CAUSAL_RELEASE_KIND,
        schema_version=CAUSAL_SCHEMA_VERSION,
        source_release_ids=(raw_receipt.release_id, policies.receipt.release_id),
        metadata={
            "causal_interval_id": interval_receipt["causal_interval_id"],
            "logical_root": causal_root,
            "market": loaded.interval_receipt["market"],
            "year": loaded.interval_receipt["year"],
        },
    )
    release = publisher.publish(stage, manifest)
    return VerifiedReleaseReceipt.from_release(release, publisher.boundary)


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
    if not isinstance(logical_root, str) or not logical_root.startswith("causal/"):
        raise IntegrityError("causal interval logical root is invalid")
    expected = {
        f"{logical_root}/bars.parquet",
        f"{logical_root}/causal_interval_receipt.json",
    }
    if {entry.path for entry in manifest.files} != expected:
        raise IntegrityError("causal interval release file set is not exact")
    root = boundary.active_root / receipt.relative_root
    report_path = root / logical_root / "causal_interval_receipt.json"
    try:
        raw = report_path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("causal interval receipt JSON is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("causal interval receipt is not canonical JSON")
    interval_id = payload.pop("causal_interval_id", None)
    if (
        interval_id != sha256_json(payload)
        or interval_id != manifest.metadata["causal_interval_id"]
        or payload.get("logical_root") != logical_root
        or payload.get("row_count")
        != payload.get("prediction_in_coverage_denominator_rows")
    ):
        raise IntegrityError("causal interval receipt content address/count is invalid")
    payload["causal_interval_id"] = interval_id
    bars = root / logical_root / "bars.parquet"
    if sha256_file(bars) != payload["causal_parquet_sha256"]:
        raise IntegrityError("causal Parquet hash differs from interval receipt")
    return bars, payload
