"""Offline-only Databento DBN vault validation and source selection.

This module imports the local DBN decoder but never constructs a Databento
client and contains no network path.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.metadata
import json
import os
import re
import uuid
from collections import deque
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterable

import databento

from .canonical import (
    canonical_bytes,
    fsync_directory,
    is_linklike,
    sha256_file,
    sha256_json,
)
from .boundary import RepoBoundary
from .errors import ContractError, IntegrityError
from .migration import verify_published_source_snapshot
from .release import VerifiedReleaseReceipt


SUPPORTED_DATABENTO_VERSION = "0.78.0"
SUPPORTED_DATABENTO_DBN_VERSION = "0.58.0"
FULL_SCAN_CHUNK_RECORDS = 100_000
DATASET = "GLBX.MDP3"
SUPPORTED_SCHEMAS = {
    "definition",
    "ohlcv-1d",
    "ohlcv-1h",
    "ohlcv-1m",
    "ohlcv-1s",
    "statistics",
    "status",
    "trades",
}
RECEIVE_TIME_COVERAGE_SCHEMAS = {
    "definition",
    "statistics",
    "status",
    "trades",
}
DATE_RANGE_NAME = re.compile(
    r"^(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})\.dbn\.zst$"
)
REQUIRED_ANOMALIES = {
    ("KE", 2019),
    ("KE", 2021),
    ("KE", 2023),
    ("KE", 2024),
    ("SR1", 2020),
    ("SR3", 2020),
}
CONTINUOUS_CONTRACT_POLICY_CORE = {
    "bar_identity_authority": "ACTUAL_INSTRUMENT_ID_FROM_BAR",
    "mapping_interval_end_fields_feature_eligible": False,
    "price_adjustment": "NONE_ORIGINAL_UNADJUSTED",
    "provider_documentation_url": (
        "https://databento.com/docs/standards-and-conventions/symbology"
    ),
    "selection_basis": "PREVIOUS_TRADING_DAY_VOLUME",
    "selection_rule": "V_PREVIOUS_DAY_VOLUME_RANK_0",
}
CONTINUOUS_CONTRACT_POLICY_HASH = sha256_json(CONTINUOUS_CONTRACT_POLICY_CORE)


def _validate_continuous_contract_policy(contract: dict[str, object]) -> dict[str, object]:
    payload = contract.get("continuous_contract_policy")
    if not isinstance(payload, dict) or set(payload) != {
        *CONTINUOUS_CONTRACT_POLICY_CORE,
        "policy_hash",
    }:
        raise ContractError("continuous-contract selection policy schema is invalid")
    core = {key: payload[key] for key in CONTINUOUS_CONTRACT_POLICY_CORE}
    if (
        core != CONTINUOUS_CONTRACT_POLICY_CORE
        or payload["policy_hash"] != CONTINUOUS_CONTRACT_POLICY_HASH
        or sha256_json(core) != payload["policy_hash"]
    ):
        raise IntegrityError("continuous-contract selection policy is not the pinned causal rule")
    return dict(payload)


def _load_known_anomalies(
    path: Path, *, expected_sha256: str
) -> tuple[set[tuple[str, int]], str]:
    observed_hash = sha256_file(path)
    if observed_hash != expected_sha256:
        raise IntegrityError("known-anomaly contract hash differs from source contract")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ContractError("known-anomaly contract is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload)
        != {
            "contract_version",
            "default_disposition",
            "families",
            "promotion_requirement",
            "waivers_allowed",
        }
        or payload.get("contract_version") != "1.0.0"
        or payload.get("default_disposition") != "QUARANTINE_FAIL_CLOSED"
        or payload.get("waivers_allowed") is not False
        or payload.get("promotion_requirement")
        != "anomaly_specific_source_alignment_and_causal_tests_pass"
        or not isinstance(payload.get("families"), list)
    ):
        raise ContractError("known-anomaly policy/version is unsupported")
    families: set[tuple[str, int]] = set()
    for raw in payload["families"]:
        if not isinstance(raw, dict) or set(raw) != {"market", "year"}:
            raise ContractError("known-anomaly family schema is invalid")
        family = (str(raw["market"]), int(raw["year"]))
        if family in families:
            raise ContractError("known-anomaly families are duplicated")
        families.add(family)
    if families != REQUIRED_ANOMALIES:
        raise IntegrityError("known-anomaly family set changed")
    return families, observed_hash


def _load_overlap_resolutions(path: Path | None) -> tuple[dict[str, object], ...]:
    if path is None:
        return ()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ContractError("DBN overlap-resolution contract is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != {
            "contract_version",
            "decoder_versions",
            "proof_algorithm",
            "resolutions",
        }
        or payload.get("contract_version") != "1.0.0"
        or payload.get("proof_algorithm") != "sha256_sorted_full_record_bytes_v1"
        or payload.get("decoder_versions")
        != {
            "databento": SUPPORTED_DATABENTO_VERSION,
            "databento-dbn": SUPPORTED_DATABENTO_DBN_VERSION,
        }
        or importlib.metadata.version("databento-dbn")
        != SUPPORTED_DATABENTO_DBN_VERSION
        or not isinstance(payload.get("resolutions"), list)
    ):
        raise ContractError("unsupported DBN overlap-resolution contract")
    resolutions: list[dict[str, object]] = []
    seen_pairs: set[frozenset[str]] = set()
    required = {
        "family",
        "market",
        "schema",
        "authoritative_path",
        "authoritative_file_sha256",
        "redundant_path",
        "redundant_file_sha256",
        "overlap_start",
        "overlap_end",
        "timestamp_field",
        "record_count",
        "record_subset_sha256",
    }
    for raw in payload["resolutions"]:
        if not isinstance(raw, dict) or set(raw) != required:
            raise ContractError("DBN overlap resolution has missing or unexpected fields")
        pair = frozenset(
            (str(raw["authoritative_path"]), str(raw["redundant_path"]))
        )
        if len(pair) != 2 or pair in seen_pairs:
            raise ContractError("DBN overlap resolution paths are duplicate or ambiguous")
        seen_pairs.add(pair)
        for key in (
            "authoritative_file_sha256",
            "redundant_file_sha256",
            "record_subset_sha256",
        ):
            if re.fullmatch(r"[0-9a-f]{64}", str(raw[key])) is None:
                raise ContractError("DBN overlap resolution hash is invalid")
        if raw["timestamp_field"] not in {"ts_event", "ts_recv"}:
            raise ContractError("DBN overlap proof timestamp field is invalid")
        start = date.fromisoformat(str(raw["overlap_start"]))
        end = date.fromisoformat(str(raw["overlap_end"]))
        if end <= start or int(raw["record_count"]) <= 0:
            raise ContractError("DBN overlap proof interval/count is invalid")
        resolution = dict(raw)
        resolution["resolution_id"] = sha256_json(raw)
        resolutions.append(resolution)
    return tuple(resolutions)


def _record_subset_proof(
    path: Path,
    *,
    start: date,
    end: date,
    timestamp_field: str,
    expected_count: int,
) -> tuple[int, str]:
    if databento.__version__ != SUPPORTED_DATABENTO_VERSION:
        raise ContractError("overlap proof requires the pinned offline DBN decoder")
    start_ns = _date_boundary_ns(start.isoformat())
    end_ns = _date_boundary_ns(end.isoformat())
    records: list[bytes] = []
    store = databento.DBNStore.from_file(path)
    try:
        for record in store:
            if not hasattr(record, timestamp_field):
                raise IntegrityError(
                    f"overlap proof record lacks {timestamp_field}: {path}"
                )
            timestamp = int(getattr(record, timestamp_field))
            if start_ns <= timestamp < end_ns:
                records.append(bytes(record))
                if len(records) > expected_count:
                    raise IntegrityError("DBN overlap proof row count exceeds its pin")
    except (NotImplementedError, ValueError, TypeError) as exc:
        raise IntegrityError(f"DBN overlap proof decode failed: {path}") from exc
    finally:
        del store
        gc.collect()
    digest = hashlib.sha256()
    for encoded in sorted(records):
        digest.update(encoded)
    return len(records), digest.hexdigest()


def _apply_overlap_resolution(
    *,
    family_id: str,
    prior: dict[str, object],
    current: dict[str, object],
    resolutions: tuple[dict[str, object], ...],
    dbn_root: Path,
) -> dict[str, object]:
    paths = frozenset((str(prior["path"]), str(current["path"])))
    matches = [
        item
        for item in resolutions
        if item["family"] == family_id
        and item["market"] == current["market"]
        and item["schema"] == current["schema"]
        and frozenset(
            (str(item["authoritative_path"]), str(item["redundant_path"]))
        )
        == paths
    ]
    if len(matches) != 1:
        raise IntegrityError("overlapping DBN coverage lacks one exact resolution contract")
    resolution = matches[0]
    by_path = {str(prior["path"]): prior, str(current["path"]): current}
    authoritative = by_path[str(resolution["authoritative_path"])]
    redundant = by_path[str(resolution["redundant_path"])]
    if (
        authoritative["sha256"] != resolution["authoritative_file_sha256"]
        or redundant["sha256"] != resolution["redundant_file_sha256"]
    ):
        raise IntegrityError("DBN overlap file bytes differ from the resolution contract")
    auth_start = date.fromisoformat(str(authoritative["start"]))
    auth_end = date.fromisoformat(str(authoritative["end"]))
    red_start = date.fromisoformat(str(redundant["start"]))
    red_end = date.fromisoformat(str(redundant["end"]))
    overlap_start = date.fromisoformat(str(resolution["overlap_start"]))
    overlap_end = date.fromisoformat(str(resolution["overlap_end"]))
    if (
        not (auth_start <= red_start and red_end <= auth_end)
        or (overlap_start, overlap_end) != (red_start, red_end)
    ):
        raise IntegrityError("DBN overlap contract does not describe an exact redundant slice")
    expected_count = int(resolution["record_count"])
    expected_hash = str(resolution["record_subset_sha256"])
    for item in (authoritative, redundant):
        relative = Path(str(item["path"])).relative_to(Path("data") / "dbn")
        observed = _record_subset_proof(
            dbn_root / relative,
            start=overlap_start,
            end=overlap_end,
            timestamp_field=str(resolution["timestamp_field"]),
            expected_count=expected_count,
        )
        if observed != (expected_count, expected_hash):
            raise IntegrityError("DBN overlap record-subset proof does not match its pin")
    resolution_id = str(resolution["resolution_id"])
    if str(authoritative["coverage_disposition"]).startswith("QUARANTINED"):
        authoritative["coverage_disposition"] = (
            "QUARANTINED_PENDING_REVALIDATION_WITH_EXACT_REDUNDANT_CROSSCHECK"
        )
        redundant["coverage_disposition"] = (
            "QUARANTINED_REDUNDANT_EXACT_CROSSCHECK_ONLY"
        )
    else:
        authoritative["coverage_disposition"] = (
            "AUTHORITATIVE_INTERVAL_WITH_EXACT_REDUNDANT_CROSSCHECK"
        )
        redundant["coverage_disposition"] = "REDUNDANT_EXACT_CROSSCHECK_ONLY"
    authoritative["overlap_resolution_id"] = resolution_id
    redundant["overlap_resolution_id"] = resolution_id
    return {
        key: value
        for key, value in resolution.items()
        if key not in {"authoritative_file_sha256", "redundant_file_sha256"}
    }


def _plain_scalar(value: Any) -> object:
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, bytes):
        return value.rstrip(b"\x00").decode("utf-8", errors="replace")
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _record_summary(record: Any) -> dict[str, object]:
    names = getattr(getattr(record, "dtype", None), "names", None) or ()
    preferred = ("rtype", "publisher_id", "instrument_id", "ts_event", "ts_recv")
    return {name: _plain_scalar(record[name]) for name in preferred if name in names}


def _normalize_schema(value: object) -> str:
    normalized = str(value).lower()
    if normalized not in SUPPORTED_SCHEMAS:
        raise IntegrityError(f"unsupported or missing DBN schema: {value!r}")
    return normalized


def _iter_arrays(
    store: databento.DBNStore, *, scan_to_end: bool, sample_records: int
) -> Iterable[Any]:
    """Yield one bounded sample or the complete stream in bounded chunks.

    Databento's ``count`` is a chunk size, not a total-record limit. Therefore
    the default mode must consume exactly one iterator chunk, while a full scan
    must never pass ``count=None`` (which materializes the whole file).
    """

    chunk_size = FULL_SCAN_CHUNK_RECORDS if scan_to_end else sample_records
    decoded = store.to_ndarray(count=chunk_size)
    if hasattr(decoded, "dtype"):
        yield decoded if scan_to_end else decoded[:sample_records]
        return
    iterator = iter(decoded)
    if scan_to_end:
        yield from iterator
    else:
        first = next(iterator, None)
        if first is not None:
            yield first


def _metadata_ns(value: object, name: str) -> int:
    if hasattr(value, "item"):
        value = value.item()  # type: ignore[union-attr]
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise IntegrityError(f"DBN metadata {name} must be a positive Unix-nanosecond integer")
    return value


def _date_boundary_ns(value: str) -> int:
    parsed = date.fromisoformat(value)
    return int(datetime.combine(parsed, time.min, timezone.utc).timestamp()) * 1_000_000_000


def _decode_summary(
    path: Path, *, sample_records: int, scan_to_end: bool
) -> dict[str, object]:
    if databento.__version__ != SUPPORTED_DATABENTO_VERSION:
        raise ContractError(
            f"offline DBN decoder must be {SUPPORTED_DATABENTO_VERSION}, found "
            f"{databento.__version__}"
        )
    if sample_records <= 0 or sample_records > 10:
        raise ContractError("sample_records must be between one and ten")
    store = databento.DBNStore.from_file(path)
    metadata = store.metadata
    metadata_schema = _normalize_schema(metadata.schema)
    store_schema = _normalize_schema(store.schema)
    if metadata_schema != store_schema:
        raise IntegrityError("DBN store and metadata schema disagree")
    first: list[dict[str, object]] = []
    last: deque[dict[str, object]] = deque(maxlen=sample_records)
    count = 0
    try:
        for array in _iter_arrays(
            store, scan_to_end=scan_to_end, sample_records=sample_records
        ):
            for record in array:
                summary = _record_summary(record)
                required = {"publisher_id", "instrument_id", "ts_event"}
                if metadata_schema in RECEIVE_TIME_COVERAGE_SCHEMAS:
                    required.add("ts_recv")
                if not required.issubset(summary):
                    raise IntegrityError(
                        f"DBN record lacks actual-contract identity/timing fields: {path}"
                    )
                if (
                    int(summary["publisher_id"]) <= 0
                    or int(summary["instrument_id"]) <= 0
                    or int(summary["ts_event"]) <= 0
                    or (
                        metadata_schema in RECEIVE_TIME_COVERAGE_SCHEMAS
                        and int(summary["ts_recv"]) <= 0
                    )
                ):
                    raise IntegrityError(f"DBN record identity/timestamp is invalid: {path}")
                if len(first) < sample_records:
                    first.append(summary)
                last.append(summary)
                count += 1
    except (NotImplementedError, ValueError, TypeError) as exc:
        raise IntegrityError(f"DBN cannot be decoded with the pinned offline decoder: {path}") from exc
    finally:
        del store
        gc.collect()
    if count == 0:
        raise IntegrityError(f"DBN contains no decodable records: {path}")
    return {
        "dataset": str(metadata.dataset),
        "decode_status": "FULL_SCAN" if scan_to_end else "FIRST_SAMPLE_ONLY",
        "first_records": first,
        "last_records": list(last) if scan_to_end else None,
        "metadata_end_ns": _metadata_ns(metadata.end, "end"),
        "metadata_start_ns": _metadata_ns(metadata.start, "start"),
        "record_count": count if scan_to_end else None,
        "schema": metadata_schema,
    }


def validate_dbn_pair(
    dbn_path: Path,
    *,
    dbn_root: Path,
    expected_schema: str,
    role: str,
    sample_records: int = 1,
    scan_to_end: bool = False,
) -> dict[str, object]:
    """Validate one DBN/sidecar pair using local bytes and the offline decoder."""

    if is_linklike(dbn_path) or not dbn_path.is_file():
        raise IntegrityError(f"DBN path is absent or link-like: {dbn_path}")
    sidecar = Path(f"{dbn_path}.manifest.json")
    if not sidecar.exists() or is_linklike(sidecar):
        raise IntegrityError(f"DBN sidecar is absent or link-like: {sidecar}")
    try:
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"invalid DBN sidecar: {sidecar}") from exc
    relative = dbn_path.relative_to(dbn_root)
    if len(relative.parts) != 4:
        raise ContractError(
            f"DBN must use exact schema/market/year/file layout: {relative.as_posix()}"
        )
    schema_dir, market, year, filename = relative.parts
    match = DATE_RANGE_NAME.fullmatch(filename)
    if match is None or not re.fullmatch(r"\d{4}", year):
        raise ContractError(f"invalid DBN coverage filename/layout: {relative.as_posix()}")
    declared_path = f"data/dbn/{relative.as_posix()}"
    expected = {
        "compression": "zstd",
        "dataset": DATASET,
        "encoding": "dbn",
        "end": match.group("end"),
        "file_size_bytes": dbn_path.stat().st_size,
        "market": market,
        "path": declared_path,
        "request_status": "ok",
        "schema": expected_schema,
        "start": match.group("start"),
        "vendor": "databento",
    }
    if schema_dir != expected_schema.replace("-", "_"):
        raise IntegrityError(
            f"schema directory {schema_dir} disagrees with {expected_schema}"
        )
    for key, value in expected.items():
        if payload.get(key) != value:
            raise IntegrityError(
                f"sidecar {key} mismatch for {relative.as_posix()}: "
                f"expected {value!r}, found {payload.get(key)!r}"
            )
    observed_hash = sha256_file(dbn_path)
    if payload.get("file_sha256") != observed_hash:
        raise IntegrityError(f"sidecar hash mismatch: {relative.as_posix()}")
    decoded = _decode_summary(
        dbn_path, sample_records=sample_records, scan_to_end=scan_to_end
    )
    if decoded["dataset"] != DATASET or decoded["schema"] != expected_schema:
        raise IntegrityError(
            f"decoded metadata disagrees with sidecar for {relative.as_posix()}"
        )
    try:
        expected_start_ns = _date_boundary_ns(match.group("start"))
        expected_end_ns = _date_boundary_ns(match.group("end"))
    except ValueError as exc:
        raise ContractError(f"invalid DBN coverage date: {relative.as_posix()}") from exc
    if year != match.group("start")[:4]:
        raise IntegrityError(f"DBN year directory disagrees with coverage start: {relative.as_posix()}")
    if expected_end_ns <= expected_start_ns:
        raise IntegrityError(f"DBN coverage interval is empty or reversed: {relative.as_posix()}")
    if (
        decoded["metadata_start_ns"] != expected_start_ns
        or decoded["metadata_end_ns"] != expected_end_ns
    ):
        raise IntegrityError(
            f"decoded DBN coverage disagrees with filename/sidecar: {relative.as_posix()}"
        )
    sampled_records = list(decoded.get("first_records") or []) + list(
        decoded.get("last_records") or []
    )
    coverage_timestamp_field = (
        "ts_recv" if expected_schema in RECEIVE_TIME_COVERAGE_SCHEMAS else "ts_event"
    )
    if any(
        not expected_start_ns
        <= int(record[coverage_timestamp_field])
        < expected_end_ns
        for record in sampled_records
    ):
        raise IntegrityError(
            f"sampled DBN {coverage_timestamp_field} lies outside declared coverage: "
            f"{relative.as_posix()}"
        )
    sidecar_hash = sha256_file(sidecar)
    core = {
        "actual_identity_authority": "DATASET_PUBLISHER_INSTRUMENT_INSTRUMENT_ID_DATE_UTC_EXCHANGE_SESSION_DATE_PLUS_AS_OF_DEFINITION",
        "continuous_selection_rule": "V_PREVIOUS_DAY_VOLUME_RANK_0",
        "continuous_metadata_mapping_policy": "RECONCILIATION_ONLY_NEVER_CAUSAL_FEATURE_OR_ELIGIBILITY",
        "coverage_disposition": "AUTHORITATIVE_INTERVAL",
        "coverage_timestamp_field": coverage_timestamp_field,
        "dataset": DATASET,
        "decode": decoded,
        "end": match.group("end"),
        "market": market,
        "path": declared_path,
        "role": role,
        "schema": expected_schema,
        "sha256": observed_hash,
        "sidecar_path": f"{declared_path}.manifest.json",
        "sidecar_sha256": sidecar_hash,
        "sidecar_size": sidecar.stat().st_size,
        "size": dbn_path.stat().st_size,
        "start": match.group("start"),
        "year": int(year),
    }
    return {**core, "validation_sha256": sha256_json(core)}


def _exact_dbn_paths(schema_root: Path) -> tuple[Path, ...]:
    """Enumerate the one permitted layout; never choose a recursively newest file."""

    if not schema_root.is_dir() or is_linklike(schema_root):
        raise ContractError(f"schema root is absent or link-like: {schema_root}")
    dbns: list[Path] = []
    for market in sorted(schema_root.iterdir()):
        if not market.is_dir() or is_linklike(market) or market.name.startswith("_"):
            raise ContractError(f"unexpected market-level entry: {market}")
        for year in sorted(market.iterdir()):
            if (
                not year.is_dir()
                or is_linklike(year)
                or re.fullmatch(r"\d{4}", year.name) is None
            ):
                raise ContractError(f"unexpected year-level entry: {year}")
            children = sorted(year.iterdir())
            for child in children:
                if child.is_dir() or is_linklike(child):
                    raise ContractError(f"unexpected nested or link-like DBN entry: {child}")
                if child.name.endswith(".dbn.zst"):
                    dbns.append(child)
                elif not child.name.endswith(".dbn.zst.manifest.json"):
                    raise ContractError(f"unexpected DBN vault file: {child}")
            sidecars = [item for item in children if item.name.endswith(".dbn.zst.manifest.json")]
            local_dbns = [item for item in children if item.name.endswith(".dbn.zst")]
            if len(sidecars) != len(local_dbns):
                raise IntegrityError(f"DBN/sidecar count mismatch in {year}")
    return tuple(dbns)


def build_source_selection_manifest(
    repository_root: Path,
    source_contract_path: Path,
    *,
    boundary: RepoBoundary,
    source_snapshot_root: Path | None = None,
    overlap_contract_path: Path | None = None,
    known_anomaly_contract_path: Path,
    sample_records: int = 1,
    scan_to_end: bool = False,
    family_ids: tuple[str, ...] = (),
    max_full_scan_bytes: int | None = None,
) -> dict[str, object]:
    """Validate all exact source families and return a content-selected catalog."""

    boundary.assert_active_path(source_contract_path, purpose="source contract", subtree="configs")
    boundary.assert_active_path(
        known_anomaly_contract_path, purpose="known-anomaly contract", subtree="configs"
    )
    if overlap_contract_path is not None:
        boundary.assert_active_path(
            overlap_contract_path, purpose="overlap contract", subtree="configs"
        )
    try:
        contract = json.loads(source_contract_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ContractError("source contract is invalid") from exc
    if not isinstance(contract, dict):
        raise ContractError("source contract must be an object")
    continuous_policy = _validate_continuous_contract_policy(contract)
    anomaly_families, anomaly_contract_hash = _load_known_anomalies(
        known_anomaly_contract_path,
        expected_sha256=str(contract.get("known_anomalies_sha256", "")),
    )
    snapshot_receipt: dict[str, object] | None = None
    if source_snapshot_root is None:
        boundary.assert_legacy_read_root(repository_root)
        dbn_root = repository_root / "data" / "dbn"
        source_scope = "LEGACY_PRECOPY_VALIDATION"
        source_snapshot_id: str | None = None
        source_snapshot_receipt_sha256: str | None = None
    else:
        boundary.assert_active_root(repository_root)
        source_snapshot_root = boundary.assert_snapshot_path(source_snapshot_root)
        snapshot_receipt = verify_published_source_snapshot(source_snapshot_root)
        dbn_root = source_snapshot_root / "dbn"
        source_scope = "VERIFIED_PUBLISHED_SOURCE_SNAPSHOT"
        source_snapshot_id = str(snapshot_receipt["source_snapshot_id"])
        source_snapshot_receipt_sha256 = sha256_file(
            source_snapshot_root / "SOURCE_SNAPSHOT_RECEIPT.json"
        )
    if scan_to_end and (not family_ids or max_full_scan_bytes is None or max_full_scan_bytes <= 0):
        raise ContractError(
            "full scan requires at least one --family and a positive byte ceiling"
        )
    normalized_family_ids = tuple(sorted(set(family_ids)))
    if normalized_family_ids != family_ids:
        raise ContractError("family filters must be unique and sorted")
    dbn_families = [
        family
        for family in contract.get("source_families", [])
        if str(family.get("id", "")).startswith("dbn_")
    ]
    known_family_ids = {str(family["id"]) for family in dbn_families}
    if family_ids and not set(family_ids).issubset(known_family_ids):
        raise ContractError("family filter contains an unknown DBN source family")
    overlap_resolutions = _load_overlap_resolutions(overlap_contract_path)
    used_overlap_resolution_ids: set[str] = set()
    applied_overlap_resolutions: list[dict[str, object]] = []
    entries: list[dict[str, object]] = []
    coverage_intervals: dict[
        tuple[str, str, str], list[tuple[date, date, dict[str, object]]]
    ] = {}
    family_summaries: list[dict[str, object]] = []
    selected_full_scan_bytes = 0
    for family in dbn_families:
        if family_ids and str(family["id"]) not in family_ids:
            continue
        schema = str(family["schema"])
        role = str(family["role"])
        logical_family_path = Path(str(family["path"]))
        try:
            schema_relative = logical_family_path.relative_to(Path("data") / "dbn")
        except ValueError as exc:
            raise ContractError("DBN family path must be beneath logical data/dbn") from exc
        if len(schema_relative.parts) != 1:
            raise ContractError("DBN family path must identify exactly one schema directory")
        schema_root = dbn_root / schema_relative
        paths = _exact_dbn_paths(schema_root)
        if "expected_dbn_files" in family and len(paths) != int(
            family["expected_dbn_files"]
        ):
            raise IntegrityError(
                f"{family['id']} expected {family['expected_dbn_files']} DBNs, found {len(paths)}"
            )
        if scan_to_end:
            selected_full_scan_bytes += sum(path.stat().st_size for path in paths)
            if selected_full_scan_bytes > int(max_full_scan_bytes):
                raise ContractError(
                    "selected families exceed the explicit full-scan resource ceiling"
                )
        family_entries: list[dict[str, object]] = []
        for dbn_path in paths:
            validated = validate_dbn_pair(
                dbn_path,
                dbn_root=dbn_root,
                expected_schema=schema,
                role=role,
                sample_records=sample_records,
                scan_to_end=scan_to_end,
            )
            if (str(validated["market"]), int(validated["year"])) in anomaly_families:
                validated["coverage_disposition"] = "QUARANTINED_PENDING_REVALIDATION"
                core_without_validation = {
                    key: value
                    for key, value in validated.items()
                    if key != "validation_sha256"
                }
                validated["validation_sha256"] = sha256_json(core_without_validation)
            interval_key = (
                str(validated["dataset"]),
                str(validated["schema"]),
                str(validated["market"]),
            )
            start = date.fromisoformat(str(validated["start"]))
            end = date.fromisoformat(str(validated["end"]))
            prior_intervals = coverage_intervals.setdefault(interval_key, [])
            for prior_start, prior_end, prior_entry in prior_intervals:
                if start < prior_end and prior_start < end:
                    applied = _apply_overlap_resolution(
                        family_id=str(family["id"]),
                        prior=prior_entry,
                        current=validated,
                        resolutions=overlap_resolutions,
                        dbn_root=dbn_root,
                    )
                    resolution_id = str(applied["resolution_id"])
                    if resolution_id in used_overlap_resolution_ids:
                        raise IntegrityError("DBN overlap resolution was applied more than once")
                    used_overlap_resolution_ids.add(resolution_id)
                    applied_overlap_resolutions.append(applied)
            prior_intervals.append((start, end, validated))
            family_entries.append(validated)
            entries.append(validated)
        family_summaries.append(
            {
                "coverage_end_max": max(
                    (str(item["end"]) for item in family_entries), default=None
                ),
                "coverage_start_min": min(
                    (str(item["start"]) for item in family_entries), default=None
                ),
                "family": family["id"],
                "file_count": len(family_entries),
                "markets": sorted({str(item["market"]) for item in family_entries}),
                "role": role,
                "schema": schema,
                "source_path": family["path"],
                "years": sorted({int(item["year"]) for item in family_entries}),
            }
        )
    selected_resolution_ids = {
        str(item["resolution_id"])
        for item in overlap_resolutions
        if not family_ids or str(item["family"]) in family_ids
    }
    if used_overlap_resolution_ids != selected_resolution_ids:
        raise IntegrityError(
            "DBN overlap-resolution contract contains unused or missing selected-family entries"
        )
    for entry in entries:
        entry_core = {key: value for key, value in entry.items() if key != "validation_sha256"}
        entry["validation_sha256"] = sha256_json(entry_core)
    core = {
        "actual_identity_authority": "DATASET_PUBLISHER_INSTRUMENT_INSTRUMENT_ID_DATE_UTC_EXCHANGE_SESSION_DATE_PLUS_AS_OF_DEFINITION",
        "continuous_contract_policy": continuous_policy,
        "continuous_metadata_mapping_policy": "RECONCILIATION_ONLY_NEVER_CAUSAL_FEATURE_OR_ELIGIBILITY",
        "dataset": DATASET,
        "decoder_version": databento.__version__,
        "families": family_summaries,
        "files": entries,
        "overlap_resolution_contract_sha256": (
            sha256_file(overlap_contract_path) if overlap_contract_path else None
        ),
        "known_anomalies_sha256": anomaly_contract_hash,
        "overlap_resolutions": sorted(
            applied_overlap_resolutions, key=lambda item: str(item["resolution_id"])
        ),
        "selection_policy": "EXACT_CONTRACT_ALL_FILES_NO_RECURSIVE_NEWEST",
        "selection_scope": "FILTERED" if family_ids else "ALL_SOURCE_FAMILIES",
        "record_scan_policy": "FULL_STREAM_BOUNDED_MEMORY" if scan_to_end else "METADATA_PLUS_FIRST_SAMPLE",
        "source_scope": source_scope,
        "source_snapshot_id": source_snapshot_id,
        "source_snapshot_receipt_sha256": source_snapshot_receipt_sha256,
        "source_contract_sha256": sha256_file(source_contract_path),
    }
    return {**core, "selection_manifest_id": sha256_json(core)}


def assert_m2b_source_eligible(
    selection_manifest: dict[str, object],
    *,
    acceptance_receipts: tuple[VerifiedReleaseReceipt, ...],
    boundary: RepoBoundary,
) -> None:
    """Block M2B release construction until every quarantined file is accepted."""

    accepted: set[str] = set()
    for receipt in acceptance_receipts:
        manifest = receipt.verify(boundary)
        if manifest.release_kind != "anomaly_acceptance" or {
            entry.path for entry in manifest.files
        } != {"acceptance.json"}:
            raise IntegrityError("anomaly acceptance release has unexpected content")
        path = boundary.active_root / receipt.relative_root / "acceptance.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("anomaly acceptance evidence is invalid") from exc
        if (
            not isinstance(payload, dict)
            or set(payload)
            != {
                "anomaly_contract_sha256",
                "accepted_validation_sha256",
                "status",
            }
            or payload.get("status") != "ACCEPTED_AFTER_CAUSAL_REVALIDATION"
            or payload.get("anomaly_contract_sha256")
            != selection_manifest.get("known_anomalies_sha256")
        ):
            raise IntegrityError("anomaly acceptance evidence is not exact")
        accepted.add(str(payload["accepted_validation_sha256"]))
    for raw in selection_manifest.get("files", []):
        if not isinstance(raw, dict):
            raise IntegrityError("selection manifest file entry is invalid")
        if str(raw.get("coverage_disposition", "")).startswith("QUARANTINED") and str(
            raw.get("validation_sha256", "")
        ) not in accepted:
            raise IntegrityError("M2B source includes an unaccepted quarantined anomaly")


def _atomic_write(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise IntegrityError("source selection manifests are immutable and cannot be overwritten")
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(descriptor, canonical_bytes(payload) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        # On Windows os.rename fails if another writer won the immutable race.
        os.rename(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    fsync_directory(path.parent)


def _validated_catalog_output(boundary: RepoBoundary, path: Path) -> Path:
    candidate = boundary.assert_active_path(
        path,
        purpose="DBN source-selection catalog output",
        subtree="state/source_selection",
    )
    expected_parent = (boundary.active_root / "state" / "source_selection").resolve(
        strict=False
    )
    if (
        candidate.parent != expected_parent
        or candidate.suffix != ".json"
        or candidate.name.startswith(".")
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*\.json", candidate.name) is None
    ):
        raise ContractError(
            "catalog output must be one named JSON file directly under state/source_selection"
        )
    return candidate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    parser.add_argument("--source-snapshot-root", type=Path)
    parser.add_argument("--overlap-contract", type=Path)
    parser.add_argument("--known-anomalies", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--sample-records", type=int, default=1)
    parser.add_argument("--full-scan", action="store_true")
    parser.add_argument("--family", action="append", default=[])
    parser.add_argument("--max-full-scan-bytes", type=int)
    args = parser.parse_args(argv)
    contract_payload = json.loads(args.source_contract.read_text(encoding="utf-8"))
    boundary = RepoBoundary(
        Path(str(contract_payload["active_repository"])),
        legacy_roots=(Path(str(contract_payload["legacy_repository"])),),
        foreign_roots=(
            Path.home() / "Desktop" / "US_stocks_swing_model",
            Path.home() / "Desktop" / "US_stocks_swing_model_v2",
        ),
    )
    boundary.assert_active_root(args.repository_root)
    boundary.assert_active_path(
        args.source_contract,
        purpose="source contract",
        subtree="configs",
    )
    boundary.assert_active_path(
        args.known_anomalies,
        purpose="known anomaly contract",
        subtree="configs",
    )
    result = build_source_selection_manifest(
        args.repository_root,
        args.source_contract,
        boundary=boundary,
        source_snapshot_root=args.source_snapshot_root,
        overlap_contract_path=args.overlap_contract,
        known_anomaly_contract_path=args.known_anomalies,
        sample_records=args.sample_records,
        scan_to_end=args.full_scan,
        family_ids=tuple(sorted(args.family)),
        max_full_scan_bytes=args.max_full_scan_bytes,
    )
    if args.output:
        _atomic_write(_validated_catalog_output(boundary, args.output), result)
    else:
        summary = {
            "actual_identity_authority": result["actual_identity_authority"],
            "continuous_metadata_mapping_policy": result[
                "continuous_metadata_mapping_policy"
            ],
            "dataset": result["dataset"],
            "decoder_version": result["decoder_version"],
            "families": result["families"],
            "file_count": len(result["files"]),
            "overlap_resolutions": result["overlap_resolutions"],
            "selection_manifest_id": result["selection_manifest_id"],
            "selection_policy": result["selection_policy"],
            "record_scan_policy": result["record_scan_policy"],
            "selection_scope": result["selection_scope"],
            "source_scope": result["source_scope"],
            "source_snapshot_id": result["source_snapshot_id"],
        }
        print(canonical_bytes(summary).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
