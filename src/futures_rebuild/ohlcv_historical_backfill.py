"""Governed OHLCV-1D/1H historical backfill planning and execution.

Planning is metadata-only and never submits a Databento time-series request.
Execution is dry-run by default and requires an exact manifest hash plus a
cost cap before any batch submission is reachable.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import socket
import sys
import time
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, MutableMapping, Sequence

from .canonical import canonical_bytes, contained_path, sha256_file, sha256_json
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .live_cockpit.databento_auth import (
    redact_databento_text,
    resolve_databento_api_key,
)
from .locking import FileLease


DATASET = "GLBX.MDP3"
TARGET_SCHEMAS: Mapping[str, str] = {
    "ohlcv-1d": "ohlcv_1d",
    "ohlcv-1h": "ohlcv_1h",
}
REFERENCE_LOCAL_SCHEMA = "ohlcv_1m"
REFERENCE_DATABENTO_SCHEMA = "ohlcv-1m"
ENCODING = "dbn"
COMPRESSION = "zstd"
STYPE_IN = "continuous"
STYPE_OUT = "instrument_id"
SPLIT_DURATION = "year"
SPLIT_SYMBOLS = False
MAP_SYMBOLS = False
SIDECAR_SUFFIX = ".manifest.json"
DBN_SUFFIX = ".dbn.zst"
MANIFEST_SCHEMA = "ohlcv_historical_backfill_manifest/1.0.0"
SIDECAR_SCHEMA = "ohlcv_historical_backfill_sidecar/1.0.0"
VALIDATION_SCHEMA = "ohlcv_historical_backfill_validation/1.0.0"
NO_DATA_EVIDENCE_SCHEMA = "ohlcv_provider_no_data_evidence/1.0.0"
LOCK_RELATIVE = Path("state/locks/ohlcv_1d_1h_historical_backfill.lock")
STAGING_RELATIVE = Path("state/provider_acquisition_staging/ohlcv_1d_1h_historical_backfill")
SAFETY_MARGIN_FLOOR_BYTES = 1_073_741_824
_SHA256 = re.compile(r"[0-9a-f]{64}")
_JOB_ID = re.compile(r"[A-Za-z0-9_-]{1,160}")
_TRANSIENT_STATUS = {408, 429, 500, 502, 503, 504}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ContractError("UTC timestamp must be a nonempty string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ContractError(f"timestamp is not explicit UTC: {value}")
    return parsed.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ContractError("naive datetime cannot be serialized as UTC")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def date_token(value: datetime) -> str:
    value = value.astimezone(timezone.utc)
    if value.time() != datetime.min.time():
        raise ContractError("annual DBN boundaries must be UTC midnight")
    return value.date().isoformat()


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.partial")
    if temporary.exists():
        raise IntegrityError(f"temporary output already exists: {temporary}")
    with temporary.open("xb") as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False).encode("utf-8"))
        stream.write(b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.partial")
    if temporary.exists():
        raise IntegrityError(f"temporary output already exists: {temporary}")
    with temporary.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(value)
        if not value.endswith("\n"):
            stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def atomic_jsonl(path: Path, rows: Iterable[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.partial")
    if temporary.exists():
        raise IntegrityError(f"temporary output already exists: {temporary}")
    with temporary.open("xb") as stream:
        for row in rows:
            stream.write(canonical_bytes(row) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def atomic_csv(path: Path, rows: Sequence[Mapping[str, object]], fields: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.partial")
    if temporary.exists():
        raise IntegrityError(f"temporary output already exists: {temporary}")
    with temporary.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            flattened: dict[str, object] = {}
            for field in fields:
                value = row.get(field)
                if isinstance(value, (dict, list, tuple)):
                    flattened[field] = json.dumps(value, sort_keys=True, separators=(",", ":"))
                elif value is None:
                    flattened[field] = ""
                else:
                    flattened[field] = value
            writer.writerow(flattened)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def load_object(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"cannot load {label}: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{label} must be a JSON object: {path}")
    return value


def compression_extension(compression: str) -> str:
    if compression == "zstd":
        return ".dbn.zst"
    if compression == "none":
        return ".dbn"
    raise ContractError(f"unsupported DBN compression: {compression}")


def normalized_request(
    *,
    schema: str,
    symbols: Sequence[str],
    start: str,
    end: str,
    dataset: str = DATASET,
    stype_in: str = STYPE_IN,
    stype_out: str = STYPE_OUT,
    encoding: str = ENCODING,
    compression: str = COMPRESSION,
    split_duration: str = SPLIT_DURATION,
    split_symbols: bool = SPLIT_SYMBOLS,
    map_symbols: bool = MAP_SYMBOLS,
) -> dict[str, object]:
    if schema not in TARGET_SCHEMAS:
        raise ContractError(f"unsupported target schema: {schema}")
    start_dt = parse_utc(start)
    end_dt = parse_utc(end)
    if not start_dt < end_dt:
        raise ContractError("request start must precede request end")
    clean_symbols = sorted(set(symbols))
    if not clean_symbols or not all(isinstance(item, str) and item for item in clean_symbols):
        raise ContractError("request symbols must be nonempty strings")
    return {
        "compression": compression,
        "dataset": dataset,
        "encoding": encoding,
        "end": iso_utc(end_dt),
        "map_symbols": map_symbols,
        "schema": schema,
        "split_duration": split_duration,
        "split_symbols": split_symbols,
        "start": iso_utc(start_dt),
        "stype_in": stype_in,
        "stype_out": stype_out,
        "symbols": clean_symbols,
    }


def request_fingerprint(request: Mapping[str, object]) -> str:
    required = {
        "compression",
        "dataset",
        "encoding",
        "end",
        "map_symbols",
        "schema",
        "split_duration",
        "split_symbols",
        "start",
        "stype_in",
        "stype_out",
        "symbols",
    }
    if set(request) != required:
        raise ContractError("request fingerprint input fields are not exact")
    return sha256_json(dict(request))


def half_open_year_slices(start: str, end: str) -> list[dict[str, object]]:
    first = parse_utc(start)
    terminal = parse_utc(end)
    if first.time() != datetime.min.time() or terminal.time() != datetime.min.time():
        raise ContractError("year slices require UTC-midnight boundaries")
    if not first < terminal:
        raise ContractError("year slice start must precede end")
    result: list[dict[str, object]] = []
    cursor = first
    while cursor < terminal:
        next_year = datetime(cursor.year + 1, 1, 1, tzinfo=timezone.utc)
        stop = min(next_year, terminal)
        result.append(
            {
                "end_exclusive": iso_utc(stop),
                "first_year_partial": cursor.month != 1 or cursor.day != 1,
                "start_inclusive": iso_utc(cursor),
                "terminal_year_partial": stop != next_year,
                "year": cursor.year,
            }
        )
        cursor = stop
    return result


def _immediate_directories(path: Path) -> list[str]:
    if not path.is_dir():
        return []
    return sorted(item.name for item in path.iterdir() if item.is_dir())


def _active_release_markets(root: Path) -> tuple[list[str], list[str]]:
    pointer_path = root / "configs/active_dbn_congruence_release_v1.json"
    pointer = load_object(pointer_path, "active DBN pointer")
    if pointer.get("status") != "ACTIVE":
        raise IntegrityError("active DBN pointer is not ACTIVE")
    relative = pointer.get("release_manifest_path")
    expected_sha = pointer.get("release_manifest_sha256")
    if not isinstance(relative, str) or not isinstance(expected_sha, str):
        raise IntegrityError("active DBN pointer does not bind a release manifest")
    manifest_path = contained_path(root, relative)
    if sha256_file(manifest_path) != expected_sha:
        raise IntegrityError("active DBN release manifest hash differs from pointer")
    manifest = load_object(manifest_path, "active DBN release manifest")
    release_core = manifest.get("release_core")
    if not isinstance(release_core, dict):
        raise IntegrityError("active DBN release lacks release_core")
    units = release_core.get("normalized_unit_manifests")
    if not isinstance(units, list):
        raise IntegrityError("active DBN release lacks normalized unit manifests")
    markets = sorted(
        {
            str(item["market"])
            for item in units
            if isinstance(item, dict)
            and item.get("family") in {"ohlcv-1d", "ohlcv-1h"}
            and isinstance(item.get("market"), str)
        }
    )
    return markets, [
        pointer_path.relative_to(root).as_posix(),
        manifest_path.relative_to(root).as_posix(),
    ]


def reconcile_market_sets(root: Path) -> tuple[list[dict[str, object]], dict[str, object]]:
    from .ohlcv_historical_backfill_v3 import authoritative_universe

    roots = {
        "ohlcv_1d": root / "data/dbn/ohlcv_1d",
        "ohlcv_1h": root / "data/dbn/ohlcv_1h",
        "ohlcv_1m": root / "data/dbn/ohlcv_1m",
    }
    observed = {name: _immediate_directories(path) for name, path in roots.items()}
    registered, registry_evidence = _active_release_markets(root)
    expected = authoritative_universe(root)["roots"]
    union = sorted(set().union(*(set(value) for value in observed.values()), registered))
    rows: list[dict[str, object]] = []
    target_union = set(observed["ohlcv_1d"]) | set(observed["ohlcv_1h"])
    target_conflict = set(observed["ohlcv_1d"]) != set(observed["ohlcv_1h"])
    for market in union:
        in_1d = market in observed["ohlcv_1d"]
        in_1h = market in observed["ohlcv_1h"]
        in_1m = market in observed["ohlcv_1m"]
        in_registry = market in registered
        included = market in target_union and in_registry and in_1d and in_1h
        conflicts: list[str] = []
        if in_1d != in_1h:
            conflicts.append("TARGET_ROOT_SET_MISMATCH")
        if market in target_union and not in_registry:
            conflicts.append("TARGET_NOT_IN_ACTIVE_RELEASE")
        if market in target_union and not in_1m:
            conflicts.append("TARGET_LACKS_OHLCV_1M_EVIDENCE")
        rows.append(
            {
                "conflict_status": "|".join(conflicts) if conflicts else "NONE",
                "evidence_paths": [
                    f"data/dbn/ohlcv_1d/{market}" if in_1d else None,
                    f"data/dbn/ohlcv_1h/{market}" if in_1h else None,
                    f"data/dbn/ohlcv_1m/{market}" if in_1m else None,
                    *(registry_evidence if in_registry else []),
                ],
                "final_inclusion": included,
                "market": market,
                "present_authoritative_registry": in_registry,
                "present_ohlcv_1d": in_1d,
                "present_ohlcv_1h": in_1h,
                "present_ohlcv_1m": in_1m,
                "resolution_reason": (
                    "EXACT_TARGET_ROOT_AND_ACTIVE_RELEASE_AGREEMENT_WITH_OHLCV_1M_EVIDENCE"
                    if included
                    else "OUTSIDE_VERIFIED_TARGET_SCOPE_OR_CONFLICT"
                ),
            }
        )
        rows[-1]["evidence_paths"] = [item for item in rows[-1]["evidence_paths"] if item]
    verified = sorted(row["market"] for row in rows if row["final_inclusion"])
    summary = {
        "authoritative_registered_markets": registered,
        "conflict": (
            target_conflict
            or registered != expected
            or observed["ohlcv_1d"] != expected
            or observed["ohlcv_1h"] != expected
            or verified != expected
        ),
        "expected_authoritative_markets": expected,
        "ohlcv_1d_markets": observed["ohlcv_1d"],
        "ohlcv_1h_markets": observed["ohlcv_1h"],
        "ohlcv_1m_markets": observed["ohlcv_1m"],
        "verified_target_markets": verified,
    }
    return rows, summary


def _ns_to_utc(value: int) -> str:
    seconds, nanoseconds = divmod(int(value), 1_000_000_000)
    base = datetime.fromtimestamp(seconds, tz=timezone.utc)
    if nanoseconds:
        return base.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return iso_utc(base)


def _metadata_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_metadata_value(item) for item in value]
    return str(value)


def reconstruct_coverage_registry(
    root: Path,
    markets: Sequence[str],
    *,
    end_exclusive: str,
) -> list[dict[str, object]]:
    from databento import DBNStore

    endpoint = parse_utc(end_exclusive)
    rows: list[dict[str, object]] = []
    for market in markets:
        market_root = root / "data/dbn" / REFERENCE_LOCAL_SCHEMA / market
        dbns = sorted(market_root.glob(f"*/*{DBN_SUFFIX}"))
        if not dbns:
            raise IntegrityError(f"verified target lacks OHLCV-1M DBN evidence: {market}")
        observations: list[dict[str, object]] = []
        symbols: set[str] = set()
        stypes_in: set[str] = set()
        stypes_out: set[str] = set()
        starts: list[int] = []
        ends: list[int] = []
        for path in dbns:
            store = DBNStore.from_file(path)
            metadata = store.metadata
            if str(metadata.dataset) != DATASET or str(metadata.schema) != REFERENCE_DATABENTO_SCHEMA:
                raise IntegrityError(f"OHLCV-1M evidence metadata mismatch: {path}")
            observed_symbols = [str(item) for item in metadata.symbols]
            symbols.update(observed_symbols)
            if metadata.stype_in is not None:
                stypes_in.add(str(metadata.stype_in))
            stypes_out.add(str(metadata.stype_out))
            starts.append(int(metadata.start))
            if metadata.end is not None:
                ends.append(int(metadata.end))
            sidecar = path.with_name(path.name + SIDECAR_SUFFIX)
            if not sidecar.is_file():
                raise IntegrityError(f"OHLCV-1M evidence sidecar is missing: {sidecar}")
            observations.append(
                {
                    "dbn_path": path.relative_to(root).as_posix(),
                    "dbn_sha256": sha256_file(path),
                    "metadata_end": _ns_to_utc(int(metadata.end)) if metadata.end is not None else None,
                    "metadata_start": _ns_to_utc(int(metadata.start)),
                    "sidecar_path": sidecar.relative_to(root).as_posix(),
                    "sidecar_sha256": sha256_file(sidecar),
                    "symbols": observed_symbols,
                    "year": int(path.parent.name),
                }
            )
        if symbols != {f"{market}.v.0"} or stypes_in != {STYPE_IN} or stypes_out != {STYPE_OUT}:
            raise IntegrityError(f"OHLCV-1M request convention is inconsistent for {market}")
        intended_start = _ns_to_utc(min(starts))
        slices = half_open_year_slices(intended_start, end_exclusive)
        present_years = {int(item["year"]) for item in observations}
        gap_years = [int(item["year"]) for item in slices if int(item["year"]) not in present_years]
        rows.append(
            {
                "actual_compression": COMPRESSION,
                "construction": "CONTINUOUS_FRONT_CONTRACT",
                "current_year_handling": "FIXED_END_EXCLUSIVE_NO_SILENT_EXPANSION",
                "dataset": DATASET,
                "dbn_encoding": ENCODING,
                "evidence": observations,
                "file_basename_template": "{start_utc_date}_{end_utc_date}.dbn.zst",
                "first_year_partial": parse_utc(intended_start).month != 1 or parse_utc(intended_start).day != 1,
                "intended_end_exclusive": iso_utc(endpoint),
                "intended_start_inclusive": intended_start,
                "local_market_folder": market,
                "local_ohlcv_1m_gap_years": gap_years,
                "local_schema_reference": REFERENCE_LOCAL_SCHEMA,
                "market": market,
                "multiple_source_symbols_per_year": False,
                "sidecar_filename_template": "{data_filename}.manifest.json",
                "sidecar_schema_observed": [
                    "legacy_databento_acquisition_manifest",
                    "dbn_canonical_publication_unit_manifest/1.0.0",
                ],
                "start_boundary_basis": "EARLIEST_OHLCV_1M_DBN_METADATA_PRESERVED",
                "stype_in": STYPE_IN,
                "stype_out": STYPE_OUT,
                "symbol_segments": [
                    {
                        "end_exclusive": iso_utc(endpoint),
                        "start_inclusive": intended_start,
                        "symbols": [f"{market}.v.0"],
                    }
                ],
                "utc_year_boundaries": True,
            }
        )
    return rows


def build_expected_targets(
    root: Path,
    registry: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    targets: list[dict[str, object]] = []
    for market_row in registry:
        market = str(market_row["market"])
        raw_segments = market_row["symbol_segments"]
        if not isinstance(raw_segments, list) or not raw_segments:
            raise IntegrityError(f"coverage registry lacks symbol segments: {market}")
        slices = half_open_year_slices(
            str(market_row["intended_start_inclusive"]),
            str(market_row["intended_end_exclusive"]),
        )
        for schema, local_schema in TARGET_SCHEMAS.items():
            for interval in slices:
                start = str(interval["start_inclusive"])
                end = str(interval["end_exclusive"])
                intersecting = [
                    segment
                    for segment in raw_segments
                    if isinstance(segment, Mapping)
                    and parse_utc(str(segment["start_inclusive"])) < parse_utc(end)
                    and parse_utc(str(segment["end_exclusive"])) > parse_utc(start)
                ]
                symbols = sorted(
                    {
                        str(symbol)
                        for segment in intersecting
                        for symbol in segment["symbols"]  # type: ignore[index]
                    }
                )
                if not symbols:
                    raise IntegrityError(f"year slice lacks a symbol segment: {market} {interval['year']}")
                basename = f"{date_token(parse_utc(start))}_{date_token(parse_utc(end))}{DBN_SUFFIX}"
                relative = Path("data/dbn") / local_schema / market / str(interval["year"]) / basename
                sidecar = relative.with_name(relative.name + SIDECAR_SUFFIX)
                request = normalized_request(
                    schema=schema,
                    symbols=symbols,
                    start=start,
                    end=end,
                    split_duration="none",
                )
                core = {
                    "final_path": relative.as_posix(),
                    "intended_end_exclusive": end,
                    "intended_start_inclusive": start,
                    "market": market,
                    "schema": schema,
                    "sidecar_path": sidecar.as_posix(),
                    "symbol_specification": {
                        "segments": intersecting,
                        "stype_in": STYPE_IN,
                        "stype_out": STYPE_OUT,
                        "symbols": symbols,
                    },
                    "year": int(interval["year"]),
                }
                targets.append(
                    {
                        **core,
                        "request_fingerprint": request_fingerprint(request),
                        "target_id": sha256_json(core),
                    }
                )
    return targets


def _sidecar_expected_hash(sidecar: Mapping[str, object]) -> tuple[str | None, int | None]:
    direct_hash = sidecar.get("file_sha256", sidecar.get("sha256"))
    direct_size = sidecar.get("file_size_bytes", sidecar.get("dbn_byte_size"))
    canonical = sidecar.get("canonical_dbn")
    if isinstance(canonical, dict):
        direct_hash = direct_hash or canonical.get("sha256")
        direct_size = direct_size if direct_size is not None else canonical.get("size_bytes")
    return (
        str(direct_hash) if isinstance(direct_hash, str) else None,
        int(direct_size) if isinstance(direct_size, int) else None,
    )


def _probe_dbn(path: Path) -> dict[str, object]:
    from databento import DBNStore

    store = DBNStore.from_file(path)
    metadata = store.metadata
    array = store.to_ndarray()
    timestamps = array["ts_event"] if len(array) else []
    monotonic = all(int(timestamps[index]) <= int(timestamps[index + 1]) for index in range(len(timestamps) - 1))
    return {
        "dataset": str(metadata.dataset),
        "dbn_format_version": int(metadata.version),
        "metadata_end": _ns_to_utc(int(metadata.end)) if metadata.end is not None else None,
        "metadata_start": _ns_to_utc(int(metadata.start)),
        "min_ts_event": _ns_to_utc(int(timestamps[0])) if len(timestamps) else None,
        "max_ts_event": _ns_to_utc(int(timestamps[-1])) if len(timestamps) else None,
        "monotonic": monotonic,
        "not_found": [_metadata_value(item) for item in metadata.not_found],
        "partial": [_metadata_value(item) for item in metadata.partial],
        "record_count": int(len(array)),
        "schema": str(metadata.schema),
        "stype_in": str(metadata.stype_in) if metadata.stype_in is not None else None,
        "stype_out": str(metadata.stype_out),
        "symbols": [str(item) for item in metadata.symbols],
    }


def classify_target(
    root: Path,
    target: Mapping[str, object],
    *,
    confirmed_record_count: int | None = None,
    dbn_probe: Callable[[Path], Mapping[str, object]] = _probe_dbn,
) -> dict[str, object]:
    data_path = contained_path(root, str(target["final_path"]))
    sidecar_path = contained_path(root, str(target["sidecar_path"]))
    expected_start = str(target["intended_start_inclusive"])
    expected_end = str(target["intended_end_exclusive"])
    expected_schema = str(target["schema"])
    expected_symbols = list(target["symbol_specification"]["symbols"])  # type: ignore[index]
    result: dict[str, object] = {
        **dict(target),
        "actual_dbn_bytes": 0,
        "actual_sidecar_bytes": 0,
        "current_state": "UNVERIFIABLE",
        "errors": [],
        "existing_bytes": 0,
        "expected_incremental_bytes": 0,
        "probe": None,
    }
    if not data_path.exists() and not sidecar_path.exists():
        if confirmed_record_count is not None:
            result["provider_record_count"] = confirmed_record_count
        if confirmed_record_count == 0:
            result["current_state"] = "NO_DATA_CONFIRMED"
        else:
            year_dir = data_path.parent
            candidates = sorted(year_dir.glob(f"*{DBN_SUFFIX}")) if year_dir.is_dir() else []
            if candidates and int(target["year"]) == parse_utc(expected_end).year:
                result["current_state"] = "STALE_CURRENT_YEAR"
                result["conflicting_candidates"] = [item.relative_to(root).as_posix() for item in candidates]
            elif candidates:
                result["current_state"] = "PARTIAL"
                result["conflicting_candidates"] = [item.relative_to(root).as_posix() for item in candidates]
            else:
                result["current_state"] = "MISSING"
        return result
    if data_path.exists() and not sidecar_path.exists():
        result["current_state"] = "SIDECAR_MISSING"
        result["actual_dbn_bytes"] = data_path.stat().st_size
        result["existing_bytes"] = data_path.stat().st_size
        return result
    if sidecar_path.exists() and not data_path.exists():
        result["current_state"] = "PARTIAL"
        result["actual_sidecar_bytes"] = sidecar_path.stat().st_size
        result["existing_bytes"] = sidecar_path.stat().st_size
        return result
    result["actual_dbn_bytes"] = data_path.stat().st_size
    result["actual_sidecar_bytes"] = sidecar_path.stat().st_size
    result["existing_bytes"] = data_path.stat().st_size + sidecar_path.stat().st_size
    errors: list[str] = []
    try:
        sidecar = load_object(sidecar_path, "target sidecar")
    except IntegrityError:
        result["current_state"] = "SIDECAR_INVALID"
        return result
    expected_hash, expected_size = _sidecar_expected_hash(sidecar)
    observed_hash = sha256_file(data_path)
    if expected_hash is None or expected_hash != observed_hash:
        errors.append("SIDECAR_HASH_MISMATCH")
    if expected_size is None or expected_size != data_path.stat().st_size:
        errors.append("SIDECAR_SIZE_MISMATCH")
    try:
        probe = dict(dbn_probe(data_path))
        result["probe"] = probe
    except Exception as exc:
        result["current_state"] = "CORRUPT"
        result["errors"] = [f"DBN_DECODE_{type(exc).__name__}"]
        return result
    if probe.get("dataset") != DATASET or probe.get("schema") != expected_schema:
        errors.append("REQUEST_PARAMETER_MISMATCH")
    if probe.get("stype_in") != STYPE_IN or probe.get("stype_out") != STYPE_OUT:
        errors.append("REQUEST_PARAMETER_MISMATCH")
    if list(probe.get("symbols", [])) != expected_symbols:
        errors.append("REQUEST_PARAMETER_MISMATCH")
    if probe.get("metadata_start") != expected_start or probe.get("metadata_end") != expected_end:
        errors.append("REQUEST_BOUNDARY_MISMATCH")
    if not bool(probe.get("monotonic")):
        errors.append("TIMESTAMPS_NOT_NONDECREASING")
    minimum = probe.get("min_ts_event")
    maximum = probe.get("max_ts_event")
    if minimum is not None and parse_utc(str(minimum)) < parse_utc(expected_start):
        errors.append("EVENT_BEFORE_START")
    if maximum is not None and parse_utc(str(maximum)) >= parse_utc(expected_end):
        errors.append("EVENT_AT_OR_AFTER_END")
    result["errors"] = sorted(set(errors))
    if any(item == "REQUEST_PARAMETER_MISMATCH" or item == "REQUEST_BOUNDARY_MISMATCH" for item in errors):
        result["current_state"] = "REQUEST_PARAMETER_MISMATCH"
    elif any(item.startswith("SIDECAR_") for item in errors):
        result["current_state"] = "SIDECAR_INVALID"
    elif errors:
        result["current_state"] = "CORRUPT"
    else:
        result["current_state"] = "COMPLETE_VALID"
    return result


def _provider_json(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Mapping):
        result: dict[str, object] = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text.casefold() in {"url", "urls", "download_url", "signed_url"}:
                result[key_text] = "<redacted-signed-url>"
            else:
                result[key_text] = _provider_json(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_provider_json(item) for item in value]
    return redact_databento_text(value)


class MetadataQuoteClient:
    """Sequential metadata-only provider adapter with bounded transient retry."""

    def __init__(
        self,
        historical: object,
        *,
        sleeper: Callable[[float], None] = time.sleep,
        retry_delays: Sequence[float] = (1.0, 2.0, 4.0, 8.0),
        success_pause_seconds: float = 0.05,
    ) -> None:
        self.historical = historical
        self.metadata = getattr(historical, "metadata", None)
        self.symbology = getattr(historical, "symbology", None)
        if self.metadata is None or self.symbology is None:
            raise IntegrityError("Databento Historical client lacks metadata or symbology")
        self.sleeper = sleeper
        self.retry_delays = tuple(retry_delays)
        self.success_pause_seconds = success_pause_seconds
        self.call_counts: Counter[str] = Counter()

    def call(self, group: str, name: str, **kwargs: object) -> object:
        owner = self.metadata if group == "metadata" else self.symbology
        function = getattr(owner, name, None)
        if not callable(function):
            raise IntegrityError(f"Databento API is unavailable: {group}.{name}")
        for attempt in range(len(self.retry_delays) + 1):
            self.call_counts[f"{group}.{name}"] += 1
            try:
                value = function(**kwargs)
                if self.success_pause_seconds:
                    self.sleeper(self.success_pause_seconds)
                return value
            except Exception as exc:
                status = getattr(exc, "http_status", getattr(exc, "status_code", None))
                if status not in _TRANSIENT_STATUS or attempt >= len(self.retry_delays):
                    raise IntegrityError(
                        f"metadata call failed: {group}.{name}: {redact_databento_text(exc)}"
                    ) from exc
                self.sleeper(self.retry_delays[attempt])
        raise IntegrityError("unreachable metadata retry state")


def build_metadata_quote(
    root: Path,
    registry: Sequence[Mapping[str, object]],
    inventory_without_no_data: Sequence[Mapping[str, object]],
    *,
    historical_factory: Callable[..., object] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    checkpoint_path: Path | None = None,
    seed_raw_provider_path: Path | None = None,
) -> tuple[dict[str, object], dict[str, object], dict[str, int]]:
    key = resolve_databento_api_key(key_files=(root / "api.env",))
    if not key:
        raise UnauthorizedOperation("project-root api.env credential is unavailable")
    if historical_factory is None:
        from databento import Historical

        historical_factory = Historical
    adapter = MetadataQuoteClient(historical_factory(key=key), sleeper=sleeper)
    checkpoint: dict[str, object] = {}
    if checkpoint_path is not None and checkpoint_path.is_file():
        checkpoint = load_object(checkpoint_path, "metadata quote checkpoint")
    elif seed_raw_provider_path is not None:
        seed_raw = load_object(seed_raw_provider_path, "seed raw provider evidence")
        seed_units = {
            str(item["request_fingerprint"]): item
            for item in seed_raw.get("quote_units", [])
            if isinstance(item, dict) and isinstance(item.get("request_fingerprint"), str)
        }
        target_by_key = {
            (str(item["market"]), str(item["schema"]), int(item["year"])): str(item["target_id"])
            for item in inventory_without_no_data
        }
        seed_no_data = {
            target_by_key[(str(item["market"]), str(item["schema"]), int(item["year"]))]: int(item["record_count"])
            for item in seed_raw.get("no_data_checks", [])
            if isinstance(item, dict)
            and (str(item.get("market")), str(item.get("schema")), int(item.get("year", -1))) in target_by_key
            and isinstance(item.get("record_count"), int)
        }
        checkpoint = {"no_data_counts": seed_no_data, "quote_units": seed_units, "raw": seed_raw}
    cached_raw = checkpoint.get("raw")
    cached_units = checkpoint.get("quote_units")
    cached_no_data = checkpoint.get("no_data_counts")
    if not isinstance(cached_raw, dict):
        cached_raw = {}
    if not isinstance(cached_units, dict):
        cached_units = {}
    if not isinstance(cached_no_data, dict):
        cached_no_data = {}

    def persist() -> None:
        if checkpoint_path is not None:
            atomic_json(
                checkpoint_path,
                {
                    "no_data_counts": cached_no_data,
                    "quote_units": cached_units,
                    "raw": _provider_json(raw),
                    "schema_version": "ohlcv_historical_backfill_planning_checkpoint/1.0.0",
                    "updated_utc": utc_now(),
                },
            )

    raw: dict[str, object] = {
        "credentials_recorded": False,
        "dataset_condition": cached_raw.get("dataset_condition") if "dataset_condition" in cached_raw else adapter.call("metadata", "get_dataset_condition", dataset=DATASET),
        "dataset_range": cached_raw.get("dataset_range") if "dataset_range" in cached_raw else adapter.call("metadata", "get_dataset_range", dataset=DATASET),
        "list_schemas": cached_raw.get("list_schemas") if "list_schemas" in cached_raw else adapter.call("metadata", "list_schemas", dataset=DATASET),
        "quote_units": list(cached_raw.get("quote_units", [])) if isinstance(cached_raw.get("quote_units"), list) else [],
        "symbology": list(cached_raw.get("symbology", [])) if isinstance(cached_raw.get("symbology"), list) else [],
        "no_data_checks": list(cached_raw.get("no_data_checks", [])) if isinstance(cached_raw.get("no_data_checks"), list) else [],
    }
    persist()
    schemas = raw["list_schemas"]
    if not isinstance(schemas, list) or not all(schema in schemas for schema in TARGET_SCHEMAS):
        raise IntegrityError("target OHLCV schemas are unavailable under current entitlement")
    quote_units: list[dict[str, object]] = []
    for market_row in registry:
        market = str(market_row["market"])
        start = str(market_row["intended_start_inclusive"])
        end = str(market_row["intended_end_exclusive"])
        symbols = sorted(
            {
                str(symbol)
                for segment in market_row["symbol_segments"]  # type: ignore[index]
                for symbol in segment["symbols"]
            }
        )
        existing_resolve = next(
            (item for item in raw["symbology"] if isinstance(item, dict) and item.get("market") == market),  # type: ignore[union-attr]
            None,
        )
        if existing_resolve is None:
            resolved = adapter.call(
                "symbology",
                "resolve",
                dataset=DATASET,
                symbols=symbols,
                stype_in=STYPE_IN,
                stype_out=STYPE_OUT,
                start_date=parse_utc(start).date().isoformat(),
                end_date=parse_utc(end).date().isoformat(),
            )
            raw["symbology"].append({"market": market, "response": _provider_json(resolved)})  # type: ignore[union-attr]
            persist()
        for schema in TARGET_SCHEMAS:
            request = normalized_request(schema=schema, symbols=symbols, start=start, end=end)
            fingerprint = request_fingerprint(request)
            cached_unit = cached_units.get(fingerprint)
            if isinstance(cached_unit, dict):
                quote_units.append(dict(cached_unit))
                continue
            provider_args = {
                "dataset": DATASET,
                "schema": schema,
                "symbols": symbols,
                "start": start,
                "end": end,
                "stype_in": STYPE_IN,
            }
            record_count = adapter.call("metadata", "get_record_count", **provider_args)
            billable_size = adapter.call("metadata", "get_billable_size", **provider_args)
            cost = adapter.call("metadata", "get_cost", **provider_args)
            if type(record_count) is not int or record_count < 0:
                raise IntegrityError("provider record count is invalid")
            if type(billable_size) is not int or billable_size < 0:
                raise IntegrityError("provider billable size is invalid")
            try:
                cost_decimal = Decimal(str(cost))
            except InvalidOperation as exc:
                raise IntegrityError("provider cost is invalid") from exc
            if not cost_decimal.is_finite() or cost_decimal < 0:
                raise IntegrityError("provider cost is invalid")
            unit = {
                "api_billable_uncompressed_bytes": billable_size,
                "estimated_cost_usd": format(cost_decimal, "f"),
                "estimated_record_count": record_count,
                "market": market,
                "quote_timestamp_utc": utc_now(),
                "request": request,
                "request_fingerprint": fingerprint,
                "schema": schema,
            }
            quote_units.append(unit)
            raw["quote_units"].append(_provider_json(unit))  # type: ignore[union-attr]
            cached_units[fingerprint] = unit
            persist()
    absent = [item for item in inventory_without_no_data if item["current_state"] == "MISSING"]
    no_data_counts: dict[str, int] = {}
    for item in absent:
        target_id = str(item["target_id"])
        if target_id in cached_no_data:
            no_data_counts[target_id] = int(cached_no_data[target_id])
            continue
        spec = item["symbol_specification"]
        outcome = "RECORD_COUNT_RESPONSE"
        try:
            count = adapter.call(
                "metadata",
                "get_record_count",
                dataset=DATASET,
                schema=item["schema"],
                symbols=spec["symbols"],  # type: ignore[index]
                start=item["intended_start_inclusive"],
                end=item["intended_end_exclusive"],
                stype_in=spec["stype_in"],  # type: ignore[index]
            )
        except IntegrityError as exc:
            message = str(exc)
            if "422 symbology_invalid_request" not in message or "None of the symbols could be resolved" not in message:
                raise
            count = 0
            outcome = "SYMBOL_UNRESOLVABLE_CONFIRMED_NO_DATA"
        if type(count) is not int or count < 0:
            raise IntegrityError("provider no-data check record count is invalid")
        no_data_counts[target_id] = count
        cached_no_data[target_id] = count
        raw["no_data_checks"].append(  # type: ignore[union-attr]
            {
                "market": item["market"],
                "outcome": outcome,
                "record_count": count,
                "request_fingerprint": item["request_fingerprint"],
                "schema": item["schema"],
                "year": item["year"],
            }
        )
        persist()
    incremental_quote_units: list[dict[str, object]] = []
    pending_inventory = [
        dict(item)
        for item in absent
        if no_data_counts.get(str(item["target_id"]), 0) > 0
    ]
    for job in build_job_plan(pending_inventory):
        request = job["request"]
        fingerprint = str(job["request_fingerprint"])
        cached_unit = cached_units.get(fingerprint)
        if isinstance(cached_unit, dict):
            incremental_quote_units.append(dict(cached_unit))
            continue
        provider_args = {
            "dataset": request["dataset"],  # type: ignore[index]
            "schema": request["schema"],  # type: ignore[index]
            "symbols": request["symbols"],  # type: ignore[index]
            "start": request["start"],  # type: ignore[index]
            "end": request["end"],  # type: ignore[index]
            "stype_in": request["stype_in"],  # type: ignore[index]
        }
        record_count = adapter.call("metadata", "get_record_count", **provider_args)
        billable_size = adapter.call("metadata", "get_billable_size", **provider_args)
        cost = adapter.call("metadata", "get_cost", **provider_args)
        if type(record_count) is not int or record_count < 0 or type(billable_size) is not int or billable_size < 0:
            raise IntegrityError("provider incremental quote response is invalid")
        try:
            cost_decimal = Decimal(str(cost))
        except InvalidOperation as exc:
            raise IntegrityError("provider incremental cost is invalid") from exc
        if not cost_decimal.is_finite() or cost_decimal < 0:
            raise IntegrityError("provider incremental cost is invalid")
        unit = {
            "api_billable_uncompressed_bytes": billable_size,
            "estimated_cost_usd": format(cost_decimal, "f"),
            "estimated_record_count": record_count,
            "market": job["market"],
            "quote_scope": "INCREMENTAL_PLANNED_JOB",
            "quote_timestamp_utc": utc_now(),
            "request": request,
            "request_fingerprint": fingerprint,
            "schema": job["schema"],
        }
        incremental_quote_units.append(unit)
        raw["quote_units"].append(_provider_json(unit))  # type: ignore[union-attr]
        cached_units[fingerprint] = unit
        persist()
    quote = {
        "call_counts": dict(sorted(adapter.call_counts.items())),
        "dataset": DATASET,
        "incremental_quote_units": incremental_quote_units,
        "quote_units": quote_units,
        "quoted_at_utc": utc_now(),
        "schema_availability": {schema: schema in schemas for schema in TARGET_SCHEMAS},
    }
    return quote, _provider_json(raw), no_data_counts


def decimal_total(values: Iterable[object]) -> Decimal:
    total = Decimal("0")
    for value in values:
        try:
            item = Decimal(str(value))
        except InvalidOperation as exc:
            raise IntegrityError("cost aggregation input is invalid") from exc
        if not item.is_finite():
            raise IntegrityError("cost aggregation input is non-finite")
        total += item
    return total


def storage_triplet(value: int) -> dict[str, object]:
    if type(value) is not int or value < 0:
        raise ContractError("storage byte total must be a nonnegative integer")
    return {
        "bytes": value,
        "decimal_gb": format(Decimal(value) / Decimal(1_000_000_000), ".9f"),
        "binary_gib": format(Decimal(value) / Decimal(1_073_741_824), ".9f"),
    }


def _ceil_ratio(value: int, numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise IntegrityError("compression ratio denominator must be positive")
    return (value * numerator + denominator - 1) // denominator


def apply_incremental_estimates(
    inventory: list[dict[str, object]],
    jobs: list[dict[str, object]],
    full_quote_units: Sequence[Mapping[str, object]],
    incremental_quote_units: Sequence[Mapping[str, object]],
) -> None:
    full_by_key = {
        (str(item["market"]), str(item["schema"])): item for item in full_quote_units
    }
    incremental_by_fingerprint = {
        str(item["request_fingerprint"]): item for item in incremental_quote_units
    }
    observed: dict[str, list[tuple[int, int]]] = {schema: [] for schema in TARGET_SCHEMAS}
    actual_by_key: dict[tuple[str, str], dict[str, int]] = {}
    for market in sorted({str(item["market"]) for item in inventory}):
        for schema in TARGET_SCHEMAS:
            valid = [
                item for item in inventory
                if item["current_state"] == "COMPLETE_VALID"
                and item["market"] == market and item["schema"] == schema
            ]
            if not valid:
                continue
            payload = sum(int(item["actual_dbn_bytes"]) for item in valid)
            sidecars = sum(int(item["actual_sidecar_bytes"]) for item in valid)
            full = full_by_key.get((market, schema))
            if full is None:
                raise IntegrityError("missing full quote unit for compression estimate")
            billable = int(full["api_billable_uncompressed_bytes"])
            if billable <= 0 or payload <= 0:
                raise IntegrityError("invalid empirical compression sample")
            actual_by_key[(market, schema)] = {
                "billable": billable,
                "payload": payload,
                "sidecar_average": (sidecars + len(valid) - 1) // len(valid),
            }
            observed[schema].append((payload, billable))
    item_by_id = {str(item["target_id"]): item for item in inventory}
    for job in jobs:
        fingerprint = str(job["request_fingerprint"])
        unit = incremental_by_fingerprint.get(fingerprint)
        if unit is None:
            raise IntegrityError("missing exact incremental quote for planned job")
        market = str(job["market"])
        schema = str(job["schema"])
        sample = actual_by_key[(market, schema)]
        billable = int(unit["api_billable_uncompressed_bytes"])
        base_payload = min(billable, _ceil_ratio(billable, sample["payload"], sample["billable"]))
        ratios = observed[schema]
        low_ratio = min(ratios, key=lambda value: Decimal(value[0]) / Decimal(value[1]))
        high_ratio = max(ratios, key=lambda value: Decimal(value[0]) / Decimal(value[1]))
        low_payload = min(billable, _ceil_ratio(billable, low_ratio[0], low_ratio[1]))
        high_payload = min(billable, _ceil_ratio(billable, high_ratio[0], high_ratio[1]))
        target_ids = [str(value) for value in job["target_ids"]]  # type: ignore[index]
        sidecars = sample["sidecar_average"] * len(target_ids)
        job.update(
            {
                "api_billable_uncompressed_bytes": billable,
                "estimated_cost_usd": str(unit["estimated_cost_usd"]),
                "estimated_final_dbn_bytes": base_payload,
                "estimated_final_dbn_bytes_low": low_payload,
                "estimated_final_dbn_bytes_high": high_payload,
                "estimated_final_sidecar_bytes": sidecars,
                "estimated_network_transfer_bytes": base_payload,
                "estimated_peak_staging_bytes": 2 * (high_payload + sidecars),
                "quote_timestamp_utc": unit["quote_timestamp_utc"],
            }
        )
        target_rows = [item_by_id[target_id] for target_id in target_ids]
        weights = [int(item.get("provider_record_count", 0)) for item in target_rows]
        weight_total = sum(weights)
        if weight_total <= 0:
            raise IntegrityError("planned job targets lack positive provider record counts")
        remaining_payload = base_payload
        remaining_low = low_payload
        remaining_high = high_payload
        remaining_weight = weight_total
        for index, (item, weight) in enumerate(zip(target_rows, weights)):
            if index == len(target_rows) - 1:
                payload_part, low_part, high_part = remaining_payload, remaining_low, remaining_high
            else:
                payload_part = remaining_payload * weight // remaining_weight
                low_part = remaining_low * weight // remaining_weight
                high_part = remaining_high * weight // remaining_weight
            remaining_payload -= payload_part
            remaining_low -= low_part
            remaining_high -= high_part
            remaining_weight -= weight
            item["expected_incremental_dbn_bytes"] = payload_part
            item["expected_incremental_dbn_bytes_low"] = low_part
            item["expected_incremental_dbn_bytes_high"] = high_part
            item["expected_incremental_sidecar_bytes"] = sample["sidecar_average"]
            item["expected_incremental_bytes"] = payload_part + sample["sidecar_average"]


def aggregate_storage_and_cost(
    inventory: Sequence[Mapping[str, object]],
    quote_units: Sequence[Mapping[str, object]],
    *,
    current_free_bytes: int,
    audit_support_bytes: int,
    peak_staging_bytes: int = 0,
    incremental_quote_units: Sequence[Mapping[str, object]] = (),
) -> dict[str, object]:
    if any(type(value) is not int or value < 0 for value in (current_free_bytes, audit_support_bytes, peak_staging_bytes)):
        raise ContractError("storage aggregation inputs must be nonnegative integers")
    valid = [item for item in inventory if item["current_state"] == "COMPLETE_VALID"]
    incomplete = [
        item
        for item in inventory
        if item["current_state"] not in {"COMPLETE_VALID", "NO_DATA_CONFIRMED"}
    ]
    schema_payload: dict[str, int] = {}
    schema_sidecars: dict[str, int] = {}
    schema_incremental: dict[str, int] = {}
    for schema in TARGET_SCHEMAS:
        schema_payload[schema] = sum(int(item["actual_dbn_bytes"]) for item in valid if item["schema"] == schema)
        schema_sidecars[schema] = sum(int(item["actual_sidecar_bytes"]) for item in valid if item["schema"] == schema)
        schema_incremental[schema] = sum(int(item.get("expected_incremental_bytes", 0)) for item in incomplete if item["schema"] == schema)
    existing_valid = sum(int(item["existing_bytes"]) for item in valid)
    incremental_final = sum(int(item.get("expected_incremental_bytes", 0)) for item in incomplete)
    incremental_dbn = sum(int(item.get("expected_incremental_dbn_bytes", 0)) for item in incomplete)
    incremental_dbn_low = sum(int(item.get("expected_incremental_dbn_bytes_low", 0)) for item in incomplete)
    incremental_dbn_high = sum(int(item.get("expected_incremental_dbn_bytes_high", 0)) for item in incomplete)
    incremental_sidecars = sum(int(item.get("expected_incremental_sidecar_bytes", 0)) for item in incomplete)
    billable = sum(int(item["api_billable_uncompressed_bytes"]) for item in quote_units)
    safety_basis = incremental_final + peak_staging_bytes + audit_support_bytes
    safety_margin = max(SAFETY_MARGIN_FLOOR_BYTES, (safety_basis + 9) // 10)
    recommended = safety_basis + safety_margin
    cost_by_schema = {
        schema: decimal_total(
            item["estimated_cost_usd"] for item in quote_units if item["schema"] == schema
        )
        for schema in TARGET_SCHEMAS
    }
    full_cost = decimal_total(item["estimated_cost_usd"] for item in quote_units)
    incremental_cost = decimal_total(item["estimated_cost_usd"] for item in incremental_quote_units)
    incremental_cost_by_schema = {
        schema: decimal_total(
            item["estimated_cost_usd"] for item in incremental_quote_units if item["schema"] == schema
        ) for schema in TARGET_SCHEMAS
    }
    compressed_payload = sum(schema_payload.values()) + incremental_dbn
    return {
        "api_billable_uncompressed_bytes": storage_triplet(billable),
        "combined_full_final": storage_triplet(existing_valid + incremental_final),
        "compression": COMPRESSION,
        "conservative_uncompressed_ceiling": storage_triplet(billable + sum(schema_sidecars.values()) + incremental_sidecars),
        "cost": {
            "full_corpus_theoretical_cost_usd": format(full_cost, "f"),
            "incremental_completion_cost_usd": format(incremental_cost, "f"),
            "incremental_schema_subtotals_usd": {key: format(value, "f") for key, value in incremental_cost_by_schema.items()},
            "schema_subtotals_usd": {key: format(value, "f") for key, value in cost_by_schema.items()},
        },
        "current_free_space": storage_triplet(current_free_bytes),
        "existing_valid_target_bytes": storage_triplet(existing_valid),
        "expected_network_transfer": {
            "incremental": storage_triplet(incremental_dbn),
            "theoretical_full_compressed_dbn_payload": storage_triplet(compressed_payload),
            "theoretical_full_uncompressed_ceiling": storage_triplet(billable),
        },
        "full_final_by_schema": {
            schema: storage_triplet(schema_payload[schema] + schema_sidecars[schema] + schema_incremental[schema])
            for schema in TARGET_SCHEMAS
        },
        "full_final_dbn_payload_bytes": storage_triplet(compressed_payload),
        "final_dbn_compression_estimate": {
            "base": storage_triplet(compressed_payload),
            "confidence_limitations": "Existing payload bytes are exact; missing payload bytes use empirical same-toolchain compression ratios and a future provider regeneration may differ.",
            "estimation_method": "Measured COMPLETE_VALID zstd DBNs plus same-market/schema base ratios and schema-wide observed low/high ratios for missing targets.",
            "high": storage_triplet(sum(schema_payload.values()) + incremental_dbn_high),
            "low": storage_triplet(sum(schema_payload.values()) + incremental_dbn_low),
            "sample_count": len(valid),
        },
        "incremental_final_bytes": storage_triplet(incremental_final),
        "peak_staging_bytes": storage_triplet(peak_staging_bytes),
        "provider_audit_support_bytes": storage_triplet(audit_support_bytes),
        "recommended_free_space": storage_triplet(recommended),
        "safety_margin": {
            "bytes": safety_margin,
            "formula": "max(1_GiB, ceil_10_percent(incremental_final + peak_staging + audit_support))",
        },
        "sufficient_space": current_free_bytes >= recommended,
    }


def _drive_capacity(path: Path) -> dict[str, int | str]:
    usage = shutil.disk_usage(path)
    return {
        "filesystem": "NTFS" if os.name == "nt" else "UNKNOWN",
        "free_bytes": int(usage.free),
        "total_bytes": int(usage.total),
        "used_bytes": int(usage.used),
    }


def _installed_versions() -> dict[str, str | None]:
    import importlib.metadata

    result: dict[str, str | None] = {"python": sys.version.split()[0]}
    for package in ("databento", "databento-dbn"):
        try:
            result[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            result[package] = None
    return result


def _git_output(root: Path, *args: str) -> str:
    import subprocess

    completed = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return completed.stdout.rstrip("\r\n")


def baseline_repository_state(root: Path, *, task_start_utc: str) -> dict[str, object]:
    status = _git_output(root, "-c", "core.quotepath=false", "status", "--porcelain=v1", "--untracked-files=all").splitlines()
    staged = [item for item in status if len(item) >= 2 and item[0] not in {" ", "?"}]
    return {
        "branch": _git_output(root, "branch", "--show-current"),
        "concurrency_note": "FOUR_CODEX_SESSIONS_REPORTED_ACTIVE_ISOLATED_PATHS_REQUIRED",
        "git_status_counts": {
            "modified_tracked": sum(item.startswith(" M") for item in status),
            "staged": len(staged),
            "total_entries": len(status),
            "untracked": sum(item.startswith("??") for item in status),
        },
        "git_status_sha256": hashlib.sha256(("\n".join(status) + "\n").encode("utf-8")).hexdigest(),
        "head": _git_output(root, "rev-parse", "HEAD"),
        "repository_root": root.as_posix(),
        "target_volume": _drive_capacity(root),
        "task_start_utc": task_start_utc,
        "tool_versions": _installed_versions(),
    }


def build_job_plan(inventory: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    actionable = [
        dict(item)
        for item in inventory
        if item["current_state"]
        not in {"COMPLETE_VALID", "NO_DATA_CONFIRMED"}
    ]
    grouped: dict[tuple[str, str, str], list[dict[str, object]]] = {}
    for item in actionable:
        symbol_key = json.dumps(item["symbol_specification"], sort_keys=True, separators=(",", ":"))
        grouped.setdefault((str(item["market"]), str(item["schema"]), symbol_key), []).append(item)
    jobs: list[dict[str, object]] = []
    for (market, schema, symbol_key), rows in sorted(grouped.items()):
        rows.sort(key=lambda item: str(item["intended_start_inclusive"]))
        segments: list[list[dict[str, object]]] = []
        for item in rows:
            if (
                segments
                and str(segments[-1][-1]["intended_end_exclusive"])
                == str(item["intended_start_inclusive"])
            ):
                segments[-1].append(item)
            else:
                segments.append([item])
        specification = json.loads(symbol_key)
        for segment in segments:
            request = normalized_request(
                schema=schema,
                symbols=specification["symbols"],
                start=str(segment[0]["intended_start_inclusive"]),
                end=str(segment[-1]["intended_end_exclusive"]),
            )
            fingerprint = request_fingerprint(request)
            jobs.append(
                {
                    "activation_status": "NOT_ACTIVE_EXECUTION_REQUIRES_EXPLICIT_FLAG_HASH_AND_COST_CAP",
                    "job_group_id": f"job-{fingerprint[:20]}",
                    "market": market,
                    "request": request,
                    "request_fingerprint": fingerprint,
                    "schema": schema,
                    "target_ids": [item["target_id"] for item in segment],
                    "target_years": [item["year"] for item in segment],
                }
            )
    return jobs


def bind_manifest(
    inventory: Sequence[Mapping[str, object]],
    jobs: Sequence[Mapping[str, object]],
    *,
    run_id: str,
    provider_metadata_hash: str,
    provider_condition_hash: str,
) -> list[dict[str, object]]:
    job_by_target = {
        str(target_id): str(job["job_group_id"])
        for job in jobs
        for target_id in job["target_ids"]  # type: ignore[index]
    }
    rows: list[dict[str, object]] = []
    for item in inventory:
        state = str(item["current_state"])
        target_id = str(item["target_id"])
        if state == "COMPLETE_VALID":
            action = "PRESERVE_EXISTING_VALID"
            activation = "EXISTING_ACTIVE_BYTES_UNCHANGED"
        elif state == "NO_DATA_CONFIRMED":
            action = "NO_FILE_CREATE"
            activation = "NO_DATA_EVIDENCE_ONLY"
        elif state == "MISSING":
            action = "DOWNLOAD_VALIDATE_INSTALL_ABSENT_TARGET_ONLY"
            activation = "NOT_ACTIVE_PENDING_EXPLICIT_EXECUTION"
        else:
            action = "STAGE_CANDIDATE_NO_REPLACEMENT"
            activation = "BLOCKED_PENDING_SEPARATE_CONFLICT_RESOLUTION"
        rows.append(
            {
                "activation_status": activation,
                "current_state": state,
                "execution_action": action,
                "existing_bytes": int(item["existing_bytes"]),
                "expected_incremental_bytes": int(item.get("expected_incremental_bytes", 0)),
                "final_path": item["final_path"],
                "intended_end_exclusive": item["intended_end_exclusive"],
                "intended_start_inclusive": item["intended_start_inclusive"],
                "manifest_schema": MANIFEST_SCHEMA,
                "market": item["market"],
                "parent_planned_job_id": job_by_target.get(target_id),
                "provider_condition_hash": provider_condition_hash,
                "provider_metadata_hash": provider_metadata_hash,
                "provider_record_count": item.get("provider_record_count"),
                "request_fingerprint": item["request_fingerprint"],
                "run_id": run_id,
                "schema": item["schema"],
                "sidecar_path": item["sidecar_path"],
                "symbol_specification": item["symbol_specification"],
                "target_id": target_id,
                "validation_requirements": [
                    "NONEMPTY_WHEN_RECORDS_EXPECTED",
                    "PROVIDER_OR_INDEPENDENT_SHA256",
                    "DBN_DECODER_READABLE",
                    "DATASET_AND_SCHEMA_EXACT",
                    "SYMBOL_CONTRACT_COMPATIBLE",
                    "HALF_OPEN_INTERVAL",
                    "NONDECREASING_TS_EVENT",
                    "RECORD_COUNT_MATCH",
                    "SIDECAR_BINDS_EXACT_BYTES",
                    "FINAL_PATH_MATCH",
                    "NO_OVERWRITE",
                    "NO_UNEXPECTED_INSTALL",
                ],
                "year": item["year"],
            }
        )
    return rows


def _quote_unit_csv_rows(quote_units: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for item in quote_units:
        request = item["request"]
        result.append(
            {
                "api_billable_uncompressed_bytes": item["api_billable_uncompressed_bytes"],
                "estimated_cost_usd": item["estimated_cost_usd"],
                "estimated_record_count": item["estimated_record_count"],
                "market": item["market"],
                "quote_timestamp_utc": item["quote_timestamp_utc"],
                "request_end": request["end"],  # type: ignore[index]
                "request_fingerprint": item["request_fingerprint"],
                "request_start": request["start"],  # type: ignore[index]
                "schema": item["schema"],
                "stype_in": request["stype_in"],  # type: ignore[index]
                "stype_out": request["stype_out"],  # type: ignore[index]
                "symbols": request["symbols"],  # type: ignore[index]
            }
        )
    return result


def build_quote_cache(quote_units: Sequence[Mapping[str, object]]) -> dict[str, object]:
    cache: dict[str, object] = {}
    for item in quote_units:
        fingerprint = str(item.get("request_fingerprint", ""))
        if _SHA256.fullmatch(fingerprint) is None:
            raise IntegrityError("quote cache unit lacks a valid request fingerprint")
        value = dict(item)
        existing = cache.setdefault(fingerprint, value)
        if existing != value:
            raise IntegrityError("quote cache has conflicting values for one request")
    return {
        "entries": cache,
        "request_count": len(cache),
        "schema_version": "ohlcv_historical_backfill_quote_cache/1.0.0",
    }


def _inventory_csv_rows(inventory: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "actual_dbn_bytes": item["actual_dbn_bytes"],
            "actual_sidecar_bytes": item["actual_sidecar_bytes"],
            "current_state": item["current_state"],
            "errors": item["errors"],
            "existing_bytes": item["existing_bytes"],
            "final_path": item["final_path"],
            "intended_end_exclusive": item["intended_end_exclusive"],
            "intended_start_inclusive": item["intended_start_inclusive"],
            "market": item["market"],
            "schema": item["schema"],
            "sidecar_path": item["sidecar_path"],
            "target_id": item["target_id"],
            "year": item["year"],
        }
        for item in inventory
    ]


def _conditions_summary(raw_provider: Mapping[str, object]) -> dict[str, object]:
    conditions = raw_provider.get("dataset_condition")
    return {
        "dataset": DATASET,
        "dataset_condition": conditions,
        "dataset_range": raw_provider.get("dataset_range"),
        "degraded_missing_or_pending": [
            item
            for item in conditions if isinstance(conditions, list) and isinstance(item, dict)
            and any(
                str(value).casefold() in {"degraded", "missing", "pending"}
                for value in item.values()
            )
        ] if isinstance(conditions, list) else [],
        "schema_availability": {
            schema: schema in list(raw_provider.get("list_schemas", []))
            for schema in TARGET_SCHEMAS
        },
    }


def _quote_markdown(
    storage: Mapping[str, object],
    *,
    quote: Mapping[str, object],
    inventory: Sequence[Mapping[str, object]],
    jobs: Sequence[Mapping[str, object]],
) -> str:
    counts = Counter(str(item["current_state"]) for item in inventory)
    cost = storage["cost"]
    lines = [
        "# OHLCV-1D + OHLCV-1H storage and cost quote",
        "",
        f"Quote timestamp: `{quote['quoted_at_utc']}`",
        "",
        "The active project cutoff is fixed. The quote does not expand it at execution time.",
        "",
        "| Item | Bytes | Decimal GB | Binary GiB |",
        "|---|---:|---:|---:|",
    ]
    for label, key in (
        ("Full final corpus", "combined_full_final"),
        ("Existing valid target footprint", "existing_valid_target_bytes"),
        ("Incremental final footprint", "incremental_final_bytes"),
        ("Conservative uncompressed ceiling", "conservative_uncompressed_ceiling"),
        ("Provider audit/support", "provider_audit_support_bytes"),
        ("Peak staging", "peak_staging_bytes"),
        ("Recommended free-space requirement", "recommended_free_space"),
        ("Current free space", "current_free_space"),
    ):
        value = storage[key]
        lines.append(f"| {label} | {value['bytes']} | {value['decimal_gb']} | {value['binary_gib']} |")  # type: ignore[index]
    lines.extend(
        [
            "",
            f"Free-space decision: **{'PASS' if storage['sufficient_space'] else 'FAIL'}**",
            "",
            f"- Complete valid pairs: {counts['COMPLETE_VALID']}",
            f"- Confirmed no-data targets: {counts['NO_DATA_CONFIRMED']}",
            f"- Other incomplete/invalid targets: {sum(value for key, value in counts.items() if key not in {'COMPLETE_VALID', 'NO_DATA_CONFIRMED'})}",
            f"- Planned batch jobs: {len(jobs)}",
            f"- Expected paid submissions for this fixed manifest: {len(jobs)}",
            "",
            "## Databento cost",
            "",
            f"- OHLCV-1D theoretical full-corpus cost: USD {cost['schema_subtotals_usd']['ohlcv-1d']}",  # type: ignore[index]
            f"- OHLCV-1H theoretical full-corpus cost: USD {cost['schema_subtotals_usd']['ohlcv-1h']}",  # type: ignore[index]
            f"- Combined theoretical full-corpus cost: USD {cost['full_corpus_theoretical_cost_usd']}",  # type: ignore[index]
            f"- OHLCV-1D incremental completion cost: USD {cost['incremental_schema_subtotals_usd']['ohlcv-1d']}",  # type: ignore[index]
            f"- OHLCV-1H incremental completion cost: USD {cost['incremental_schema_subtotals_usd']['ohlcv-1h']}",  # type: ignore[index]
            f"- Incremental completion cost: USD {cost['incremental_completion_cost_usd']}",  # type: ignore[index]
            "",
            "Costs are the API responses under the active account/plan for the exact fixed requests; no public unit-price inference was used.",
            "",
            "## Compression and transfer",
            "",
            "The certified local convention is Zstandard-compressed DBN with the `.dbn.zst` extension and an adjacent `.dbn.zst.manifest.json` sidecar. Existing bytes are measured; missing payload bytes are estimated from same-toolchain local compression evidence. A theoretical full re-download is bounded separately by the uncompressed ceiling.",
            "",
            f"- Expected incremental network transfer: {storage['expected_network_transfer']['incremental']['bytes']} bytes ({storage['expected_network_transfer']['incremental']['decimal_gb']} GB / {storage['expected_network_transfer']['incremental']['binary_gib']} GiB)",  # type: ignore[index]
            f"- Theoretical full compressed DBN transfer: {storage['expected_network_transfer']['theoretical_full_compressed_dbn_payload']['bytes']} bytes ({storage['expected_network_transfer']['theoretical_full_compressed_dbn_payload']['decimal_gb']} GB / {storage['expected_network_transfer']['theoretical_full_compressed_dbn_payload']['binary_gib']} GiB)",  # type: ignore[index]
            f"- Compression sample count: {storage['final_dbn_compression_estimate']['sample_count']} validated DBNs",  # type: ignore[index]
        ]
    )
    return "\n".join(lines) + "\n"


def _runbook_text(
    *,
    report_relative: str,
    manifest_sha256: str,
    run_id: str,
    activation_cost_cap_usd: str,
    planned_jobs: int,
) -> str:
    manifest = f"{report_relative}/09_DOWNLOAD_EXECUTION_MANIFEST.jsonl"
    common = ".\\.venv\\Scripts\\python.exe .\\scripts\\run_ohlcv_1d_1h_backfill.py"
    quote = ".\\.venv\\Scripts\\python.exe .\\scripts\\quote_ohlcv_1d_1h_backfill.py"
    return f"""# OHLCV-1D + OHLCV-1H execution runbook

All commands are repository-root PowerShell commands. Quote/plan mode makes metadata-only calls. The executor is dry-run unless `--execute` is present.

## 1. Rebuild a metadata-only quote and plan

```powershell
$RunId = "{run_id}-refresh"
{quote} --report-root "reports/ohlcv_1d_1h_historical_backfill/$RunId" --end-exclusive "2026-07-14T00:00:00Z" --metadata-only
```

## 2. Complete dry run

```powershell
{common} --manifest "{manifest}"
```

## 3. Selected market/schema/year dry run

```powershell
{common} --manifest "{manifest}" --market ES --schema ohlcv-1h --year 2024
```

## 4. Validate existing files only

```powershell
{common} --manifest "{manifest}" --validate-only
```

## 5. Later paid activation boundary — NOT RUN

```powershell
{common} --manifest "{manifest}" --execute --manifest-sha256 "{manifest_sha256}" --maximum-authorized-cost-usd "{activation_cost_cap_usd}"
```

The fixed certified manifest plans {planned_jobs} provider batch jobs. This command is shown for later review only and was not run in this task.

## 6. Resume interrupted jobs

```powershell
{common} --manifest "{manifest}" --execute --resume --manifest-sha256 "{manifest_sha256}" --maximum-authorized-cost-usd "{activation_cost_cap_usd}"
```

## 7. Download a completed unexpired job without resubmitting

```powershell
{common} --manifest "{manifest}" --execute --resume --reuse-job-id "GLBX-JOB-ID" --manifest-sha256 "{manifest_sha256}" --maximum-authorized-cost-usd "{activation_cost_cap_usd}"
```

## 8. Revalidate installed files

```powershell
{common} --manifest "{manifest}" --validate-only
```

## 9. Produce completion certification

```powershell
{common} --manifest "{manifest}" --validate-only --certification-output "{report_relative}/completion_certification.json"
```

## 10. Future current-year successor preparation

Choose an explicit fully usable UTC endpoint; never use `today` and never append or overwrite the active 2026 file.

```powershell
$EndExclusive = "2026-09-01T00:00:00Z"
$RunId = "ohlcv1d1h_current_year_successor_20260901"
{quote} --report-root "reports/ohlcv_1d_1h_historical_backfill/$RunId" --end-exclusive $EndExclusive --metadata-only --current-year-successor
```

That command creates a replacement-candidate plan. It does not overwrite or activate a current-year DBN.
"""


def _validation_contract() -> dict[str, object]:
    return {
        "checks": [
            "FILE_NONEMPTY_WHEN_RECORDS_EXPECTED",
            "SHA256_MATCHES_PROVIDER_OR_INDEPENDENT_SOURCE_HASH",
            "DBN_DECODER_OPENS_WITHOUT_TRUNCATION_OR_SCHEMA_ERROR",
            "DATASET_IS_GLBX_MDP3",
            "SCHEMA_EXACTLY_REQUESTED",
            "SYMBOLOGY_COMPATIBLE",
            "EVENTS_WITHIN_HALF_OPEN_INTERVAL",
            "EVENT_TIMESTAMPS_NONDECREASING",
            "RECORD_COUNT_MATCHES_PROVIDER_METADATA",
            "SIDECAR_BINDS_EXACT_DBN_BYTES",
            "FINAL_PATH_MATCHES_MANIFEST",
            "NO_EXISTING_CERTIFIED_TARGET_MODIFIED",
            "NO_UNEXPECTED_FILE_INSTALLED",
        ],
        "cross_schema_diagnostic": {
            "blocking": False,
            "method": "STRATIFIED_OHLCV_1M_AGGREGATION_BY_CONTINUOUS_SYMBOL_AND_DATABENTO_INTERVAL",
            "status": "AVAILABLE_AS_NON_BLOCKING_FUTURE_DIAGNOSTIC_NO_SOURCE_MUTATION",
        },
        "dbn_sidecar_schema": SIDECAR_SCHEMA,
        "schema_version": VALIDATION_SCHEMA,
    }


def _hash_artifacts(report_root: Path) -> dict[str, object]:
    files = sorted(
        item
        for item in report_root.rglob("*")
        if item.is_file() and item.name != "SHA256SUMS.json" and not item.name.endswith(".partial")
    )
    entries = [
        {
            "path": item.relative_to(report_root).as_posix(),
            "sha256": sha256_file(item),
            "size_bytes": item.stat().st_size,
        }
        for item in files
    ]
    return {
        "artifact_count": len(entries),
        "entries": entries,
        "generated_utc": utc_now(),
        "schema_version": "ohlcv_historical_backfill_sha256s/1.0.0",
    }


def finalize_test_receipt(
    report_root: Path,
    *,
    commands: Sequence[Mapping[str, object]],
    live_metadata_smoke_status: str,
) -> dict[str, object]:
    passed = sum(int(item.get("passed", 0)) for item in commands)
    failed = sum(int(item.get("failed", 0)) for item in commands)
    receipt = {
        "commands": list(commands),
        "failed": failed,
        "finalized_utc": utc_now(),
        "live_metadata_smoke_status": live_metadata_smoke_status,
        "passed": passed,
        "schema_version": "ohlcv_historical_backfill_test_receipt/1.0.0",
        "tool_versions": _installed_versions(),
    }
    atomic_json(report_root / "13_TEST_RECEIPT.json", receipt)
    atomic_json(report_root / "SHA256SUMS.json", _hash_artifacts(report_root))
    return receipt


def build_report_package(
    root: Path,
    report_root: Path,
    *,
    end_exclusive: str,
    task_start_utc: str,
    historical_factory: Callable[..., object] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    current_year_successor: bool = False,
    seed_raw_provider_path: Path | None = None,
) -> dict[str, object]:
    root = root.resolve(strict=True)
    report_root = report_root.resolve(strict=False)
    if seed_raw_provider_path is not None:
        seed_raw_provider_path = seed_raw_provider_path.resolve(strict=True)
        try:
            seed_raw_provider_path.relative_to(root)
        except ValueError as exc:
            raise ContractError("seed provider evidence must be within the repository") from exc
    try:
        report_root.relative_to(root)
    except ValueError as exc:
        raise ContractError("report root must be within the repository") from exc
    checkpoint_path = report_root / ".planning_quote_checkpoint.json"
    if report_root.exists():
        unexpected = [path for path in report_root.iterdir() if path != checkpoint_path]
        if unexpected:
            raise IntegrityError(f"report root contains non-checkpoint artifacts: {report_root}")
    report_root.mkdir(parents=True, exist_ok=True)
    baseline = baseline_repository_state(root, task_start_utc=task_start_utc)
    if baseline["git_status_counts"]["staged"] != 0:  # type: ignore[index]
        raise IntegrityError("planner requires zero staged paths")
    reconciliation_rows, reconciliation = reconcile_market_sets(root)
    markets = reconciliation["verified_target_markets"]
    if reconciliation["conflict"] or len(markets) != 33:
        raise IntegrityError("exact 33-market target reconciliation failed")
    registry = reconstruct_coverage_registry(root, markets, end_exclusive=end_exclusive)
    targets = build_expected_targets(root, registry)
    prequote_inventory = [classify_target(root, item) for item in targets]
    quote, raw_provider, no_data_counts = build_metadata_quote(
        root,
        registry,
        prequote_inventory,
        historical_factory=historical_factory,
        sleeper=sleeper,
        checkpoint_path=checkpoint_path,
        seed_raw_provider_path=seed_raw_provider_path,
    )
    if checkpoint_path.exists():
        checkpoint_path.unlink()
    inventory = [
        classify_target(
            root,
            item,
            confirmed_record_count=no_data_counts.get(str(item["target_id"])),
        )
        for item in targets
    ]
    jobs = build_job_plan(inventory)
    apply_incremental_estimates(
        inventory,
        jobs,
        quote["quote_units"],  # type: ignore[arg-type,index]
        quote["incremental_quote_units"],  # type: ignore[arg-type,index]
    )
    run_id = report_root.name
    manifest_rows = bind_manifest(
        inventory,
        jobs,
        run_id=run_id,
        provider_metadata_hash=sha256_json(raw_provider),
        provider_condition_hash=sha256_json(_conditions_summary(raw_provider)),
    )
    report_relative = report_root.relative_to(root).as_posix()
    atomic_json(report_root / "01_BASELINE_REPOSITORY_STATE.json", baseline)
    atomic_json(
        report_root / "02_MARKET_SET_RECONCILIATION.json",
        {"rows": reconciliation_rows, "summary": reconciliation},
    )
    atomic_csv(
        report_root / "02_MARKET_SET_RECONCILIATION.csv",
        reconciliation_rows,
        [
            "market",
            "present_ohlcv_1d",
            "present_ohlcv_1h",
            "present_ohlcv_1m",
            "present_authoritative_registry",
            "final_inclusion",
            "conflict_status",
            "resolution_reason",
            "evidence_paths",
        ],
    )
    atomic_json(
        report_root / "03_MARKET_COVERAGE_REGISTRY.json",
        {"rows": registry, "schema_version": "market_coverage_registry/1.0.0"},
    )
    atomic_csv(
        report_root / "03_MARKET_COVERAGE_REGISTRY.csv",
        registry,
        [
            "market",
            "local_market_folder",
            "dataset",
            "stype_in",
            "stype_out",
            "construction",
            "intended_start_inclusive",
            "intended_end_exclusive",
            "first_year_partial",
            "local_ohlcv_1m_gap_years",
            "symbol_segments",
            "dbn_encoding",
            "actual_compression",
            "file_basename_template",
            "sidecar_filename_template",
            "evidence",
        ],
    )
    atomic_json(
        report_root / "04_EXISTING_TARGET_INVENTORY.json",
        {"rows": inventory, "state_counts": dict(Counter(str(item["current_state"]) for item in inventory))},
    )
    inventory_csv = _inventory_csv_rows(inventory)
    atomic_csv(
        report_root / "04_EXISTING_TARGET_INVENTORY.csv",
        inventory_csv,
        list(inventory_csv[0]) if inventory_csv else [],
    )
    conditions = _conditions_summary(raw_provider)
    atomic_json(report_root / "05_DATABENTO_AVAILABILITY_AND_CONDITIONS.json", conditions)
    raw_root = report_root / "06_RAW_REDACTED_METADATA_RESPONSES"
    atomic_json(raw_root / "metadata_responses.json", raw_provider)
    atomic_json(
        raw_root / "quote_cache.json",
        build_quote_cache([*quote["quote_units"], *quote["incremental_quote_units"]]),  # type: ignore[index]
    )
    quote_detail = {
        **quote,
        "fixed_schema_endpoints": {schema: end_exclusive for schema in TARGET_SCHEMAS},
        "quote_limitations": [
            "COST_AND_BILLABLE_BYTES_ARE_PROVIDER_ESTIMATES_AT_QUOTE_TIME",
            "COMPRESSED_FINAL_BYTES_ARE_MEASURED_FROM_EXISTING_CERTIFIED_TARGETS",
            "NO_PAID_REQUEST_OR_SAMPLE_DOWNLOAD_PERFORMED",
        ],
    }
    atomic_json(report_root / "07_QUOTE_DETAIL.json", quote_detail)
    all_quote_units = [*quote["quote_units"], *quote["incremental_quote_units"]]  # type: ignore[index]
    quote_csv = _quote_unit_csv_rows(all_quote_units)  # type: ignore[arg-type]
    atomic_csv(report_root / "07_QUOTE_DETAIL.csv", quote_csv, list(quote_csv[0]) if quote_csv else [])
    atomic_jsonl(report_root / "09_DOWNLOAD_EXECUTION_MANIFEST.jsonl", manifest_rows)
    manifest_sha = sha256_file(report_root / "09_DOWNLOAD_EXECUTION_MANIFEST.jsonl")
    atomic_json(
        report_root / "10_DOWNLOAD_JOB_PLAN.json",
        {
            "batch_job_count": len(jobs),
            "jobs": jobs,
            "manifest_sha256": manifest_sha,
            "maximum_concurrent_jobs": 1,
            "state": "NON_ACTIVE_DRY_RUN_DEFAULT",
        },
    )
    activation_cost_cap = format(
        decimal_total(item["estimated_cost_usd"] for item in quote["incremental_quote_units"]),  # type: ignore[index]
        "f",
    )
    atomic_text(
        report_root / "11_EXECUTION_RUNBOOK.md",
        _runbook_text(
            report_relative=report_relative,
            manifest_sha256=manifest_sha,
            run_id=run_id,
            activation_cost_cap_usd=activation_cost_cap,
            planned_jobs=len(jobs),
        ),
    )
    atomic_json(report_root / "12_VALIDATION_CONTRACT.json", _validation_contract())
    provisional_tests = {
        "commands": [],
        "failed": 0,
        "live_metadata_smoke_status": "PASSED_AS_PART_OF_QUOTE_BUILD",
        "passed": 0,
        "schema_version": "ohlcv_historical_backfill_test_receipt/1.0.0",
        "status": "PENDING_FINAL_TEST_RUN",
        "tool_versions": _installed_versions(),
    }
    atomic_json(report_root / "13_TEST_RECEIPT.json", provisional_tests)
    state_counts = Counter(str(item["current_state"]) for item in inventory)
    atomic_text(
        report_root / "14_RISKS_EXCEPTIONS_AND_BLOCKERS.md",
        "# Risks, exceptions, and blockers\n\n"
        "- Four concurrent Codex sessions were reported. All task writes are isolated to unique additive paths; `data/dbn` is read-only in this task.\n"
        "- The user reported permanently deleting the obsolete `data/dbn/trades` tree. This package neither requires nor restores it.\n"
        "- The active cutoff is 2026-07-14T00:00:00Z. A later current-year extension is an immutable successor, not an append or overwrite.\n"
        "- Databento metadata estimates can change after the quote timestamp; execute mode rechecks exact cost and enforces the explicit cap.\n"
        "- Provider batch job history does not expose a user-defined idempotency key. The executor binds normalized provider fields plus the local fsynced job ledger and fails closed on ambiguity.\n"
        "- Existing current canonical publication sidecars are path-bound successors; new acquisition candidates use a more complete versioned sidecar and require later publication certification before activation.\n"
        + ("- This is a current-year replacement-candidate plan; installation into existing year directories is blocked.\n" if current_year_successor else ""),
    )
    atomic_text(
        report_root / "15_HANDOFF.md",
        "# Handoff\n\n"
        f"Run ID: `{run_id}`  \n"
        f"Manifest SHA-256: `{manifest_sha}`  \n"
        f"Targets: {len(manifest_rows)}  \n"
        f"Complete valid: {state_counts['COMPLETE_VALID']}  \n"
        f"Confirmed no data: {state_counts['NO_DATA_CONFIRMED']}  \n"
        f"Planned paid batch jobs: {len(jobs)}  \n\n"
        "The package is non-active and dry-run by default. Do not run the activation command without a later explicit review of the exact manifest hash and cost cap.\n",
    )
    audit_support_estimate = 0
    for _ in range(5):
        storage = aggregate_storage_and_cost(
            inventory,
            quote["quote_units"],  # type: ignore[arg-type,index]
            current_free_bytes=int(baseline["target_volume"]["free_bytes"]),  # type: ignore[index]
            audit_support_bytes=audit_support_estimate,
            peak_staging_bytes=0 if not jobs else max(int(item["estimated_peak_staging_bytes"]) for item in jobs),
            incremental_quote_units=quote["incremental_quote_units"],  # type: ignore[arg-type,index]
        )
        atomic_text(report_root / "08_STORAGE_AND_COST_QUOTE.md", _quote_markdown(storage, quote=quote, inventory=inventory, jobs=jobs))
        measured = sum(item.stat().st_size for item in report_root.rglob("*") if item.is_file() and item.name != "SHA256SUMS.json")
        if measured == audit_support_estimate:
            break
        audit_support_estimate = measured
    storage = aggregate_storage_and_cost(
        inventory,
        quote["quote_units"],  # type: ignore[arg-type,index]
        current_free_bytes=int(baseline["target_volume"]["free_bytes"]),  # type: ignore[index]
        audit_support_bytes=audit_support_estimate,
        peak_staging_bytes=0 if not jobs else max(int(item["estimated_peak_staging_bytes"]) for item in jobs),
        incremental_quote_units=quote["incremental_quote_units"],  # type: ignore[arg-type,index]
    )
    atomic_text(report_root / "08_STORAGE_AND_COST_QUOTE.md", _quote_markdown(storage, quote=quote, inventory=inventory, jobs=jobs))
    status = {
        "current_state_counts": dict(state_counts),
        "manifest_sha256": manifest_sha,
        "paid_requests_submitted": 0,
        "planned_batch_jobs": len(jobs),
        "quote_timestamp_utc": quote["quoted_at_utc"],
        "status": "READY_FOR_REVIEW_NO_PAID_REQUESTS_SUBMITTED",
        "storage": storage,
        "verified_markets": len(markets),
    }
    atomic_text(
        report_root / "00_STATUS.md",
        "# Status\n\n"
        f"`{status['status']}`\n\n"
        f"- Verified markets: {status['verified_markets']}\n"
        f"- Complete valid DBN/sidecar pairs: {state_counts['COMPLETE_VALID']}\n"
        f"- Confirmed no-data targets: {state_counts['NO_DATA_CONFIRMED']}\n"
        f"- Planned batch jobs: {len(jobs)}\n"
        f"- Paid requests submitted: 0\n"
        f"- Manifest SHA-256: `{manifest_sha}`\n",
    )
    atomic_json(report_root / "SHA256SUMS.json", _hash_artifacts(report_root))
    return {
        "conditions": conditions,
        "inventory": inventory,
        "jobs": jobs,
        "manifest_rows": manifest_rows,
        "manifest_sha256": manifest_sha,
        "market_registry": registry,
        "quote": quote,
        "reconciliation": reconciliation,
        "report_root": report_relative,
        "status": status,
        "storage": storage,
    }


def load_manifest(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    try:
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line:
                continue
            value = json.loads(line)
            if not isinstance(value, dict) or value.get("manifest_schema") != MANIFEST_SCHEMA:
                raise IntegrityError(f"invalid manifest row at line {line_number}")
            if "no_data_evidence" in value:
                _validated_no_data_evidence(value)
            rows.append(value)
    except (OSError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"cannot read execution manifest: {path}") from exc
    if not rows:
        raise IntegrityError("execution manifest is empty")
    ids = [str(item["target_id"]) for item in rows]
    if len(ids) != len(set(ids)):
        raise IntegrityError("execution manifest target IDs are not unique")
    return rows


def _validated_no_data_evidence(row: Mapping[str, object]) -> dict[str, object]:
    evidence = row.get("no_data_evidence")
    if not isinstance(evidence, Mapping):
        raise IntegrityError("certified no-data target lacks structured provider evidence")
    required = {
        "evidence_path",
        "evidence_sha256",
        "job_id",
        "provider_error_code",
        "provider_error_message",
        "provider_error_status",
        "provider_manifest_hash",
        "request_fingerprint",
        "schema_version",
    }
    if set(evidence) != required:
        raise IntegrityError("certified no-data evidence fields differ from contract")
    if (
        row.get("current_state") != "NO_DATA_CONFIRMED"
        or row.get("execution_action") != "NO_FILE_CREATE"
        or row.get("provider_record_count") != 0
    ):
        raise IntegrityError("certified no-data target state differs from contract")
    if evidence.get("schema_version") != NO_DATA_EVIDENCE_SCHEMA:
        raise IntegrityError("certified no-data evidence schema differs")
    if evidence.get("provider_error_status") != 422:
        raise IntegrityError("certified no-data provider status differs")
    if evidence.get("provider_error_code") != "symbology_invalid_request":
        raise IntegrityError("certified no-data provider error code differs")
    if evidence.get("provider_error_message") != "None of the symbols could be resolved":
        raise IntegrityError("certified no-data provider error message differs")
    for key in ("evidence_sha256", "provider_manifest_hash", "request_fingerprint"):
        if not isinstance(evidence.get(key), str) or _SHA256.fullmatch(str(evidence[key])) is None:
            raise IntegrityError(f"certified no-data {key} is invalid")
    if not isinstance(evidence.get("job_id"), str) or _JOB_ID.fullmatch(str(evidence["job_id"])) is None:
        raise IntegrityError("certified no-data job ID is invalid")
    evidence_path = Path(str(evidence.get("evidence_path", "")))
    if evidence_path.is_absolute() or ".." in evidence_path.parts:
        raise IntegrityError("certified no-data evidence path is invalid")
    return dict(evidence)


def filter_manifest(
    rows: Sequence[Mapping[str, object]],
    *,
    markets: Sequence[str] = (),
    schemas: Sequence[str] = (),
    years: Sequence[int] = (),
) -> list[dict[str, object]]:
    market_set = set(markets)
    schema_set = set(schemas)
    year_set = set(years)
    if schema_set - set(TARGET_SCHEMAS):
        raise ContractError("selector contains an unsupported schema")
    selected = [
        dict(item)
        for item in rows
        if (not market_set or item["market"] in market_set)
        and (not schema_set or item["schema"] in schema_set)
        and (not year_set or item["year"] in year_set)
    ]
    if (market_set or schema_set or year_set) and not selected:
        raise ContractError("selectors matched no manifest targets")
    return selected


def _job_state(job: Mapping[str, object]) -> str:
    value = job.get("state", job.get("status"))
    if not isinstance(value, str):
        raise IntegrityError("provider job state is invalid")
    state = value.casefold()
    if state not in {"queued", "processing", "done", "expired"}:
        raise IntegrityError(f"unsupported provider job state: {state}")
    return state


def _job_identifier(job: Mapping[str, object]) -> str:
    value = job.get("id", job.get("job_id"))
    if not isinstance(value, str) or _JOB_ID.fullmatch(value) is None:
        raise IntegrityError("provider job identifier is invalid")
    return value


def provider_job_request(job: Mapping[str, object]) -> dict[str, object] | None:
    source: Mapping[str, object] = job
    for key in ("request", "query", "params"):
        nested = job.get(key)
        if isinstance(nested, Mapping):
            source = nested
            break
    required = {"dataset", "schema", "symbols", "start", "end", "stype_in"}
    if not required.issubset(source):
        return None
    symbols = source["symbols"]
    if isinstance(symbols, str):
        symbols = [symbols]
    if not isinstance(symbols, (list, tuple)):
        return None
    try:
        return normalized_request(
            dataset=str(source["dataset"]),
            schema=str(source["schema"]),
            symbols=[str(item) for item in symbols],
            start=iso_utc(parse_utc(str(source["start"]))),
            end=iso_utc(parse_utc(str(source["end"]))),
            stype_in=str(source["stype_in"]),
            stype_out=str(source.get("stype_out", STYPE_OUT)),
            encoding=str(source.get("encoding", ENCODING)),
            compression=str(source.get("compression", COMPRESSION)),
            split_duration=str(source.get("split_duration", SPLIT_DURATION)),
            split_symbols=bool(source.get("split_symbols", SPLIT_SYMBOLS)),
            map_symbols=bool(source.get("map_symbols", MAP_SYMBOLS)),
        )
    except (ContractError, ValueError, TypeError):
        return None


def select_reusable_job(
    fingerprint: str,
    jobs: Sequence[Mapping[str, object]],
    *,
    required_job_id: str | None = None,
) -> dict[str, object] | None:
    matches: list[dict[str, object]] = []
    expired: list[dict[str, object]] = []
    for raw in jobs:
        job = dict(raw)
        job_id = _job_identifier(job)
        if required_job_id is not None and job_id != required_job_id:
            continue
        observed = provider_job_request(job)
        explicit = job.get("request_fingerprint")
        observed_fingerprint = (
            str(explicit)
            if isinstance(explicit, str) and _SHA256.fullmatch(explicit)
            else request_fingerprint(observed) if observed is not None else None
        )
        if observed_fingerprint != fingerprint:
            continue
        if _job_state(job) == "expired":
            expired.append(job)
        else:
            matches.append(job)
    if required_job_id is not None and not matches and not expired:
        raise IntegrityError("required reusable job ID does not match the request")
    if required_job_id is not None and expired:
        raise IntegrityError("required reusable job ID is expired")
    if len(matches) > 1:
        raise IntegrityError("multiple reusable nonexpired jobs match one request")
    if matches:
        return matches[0]
    return None


def _ledger_reuse_job_id(
    fingerprint: str,
    ledger_bindings: Mapping[str, str],
) -> str | None:
    matches = sorted(
        job_id for job_id, bound_fingerprint in ledger_bindings.items()
        if bound_fingerprint == fingerprint
    )
    if len(matches) > 1:
        raise IntegrityError("multiple submitted jobs are bound to one request")
    return matches[0] if matches else None


def _append_ledger(path: Path, event: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as stream:
        stream.write(canonical_bytes(event) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def _read_ledger(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    for index, line in enumerate(path.read_bytes().splitlines(), 1):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise IntegrityError(f"job ledger is invalid at line {index}") from exc
        if not isinstance(value, dict):
            raise IntegrityError(f"job ledger row is invalid at line {index}")
        rows.append(value)
    return rows


class DatabentoBatchProvider:
    def __init__(self, root: Path, *, historical_factory: Callable[..., object] | None = None) -> None:
        key = resolve_databento_api_key(key_files=(root / "api.env",))
        if not key:
            raise UnauthorizedOperation("project-root api.env credential is unavailable")
        if historical_factory is None:
            from databento import Historical

            historical_factory = Historical
        self.key = key
        self.client = historical_factory(key=key)
        self.metadata = getattr(self.client, "metadata", None)
        self.batch = getattr(self.client, "batch", None)
        if self.metadata is None or self.batch is None:
            raise IntegrityError("Databento client lacks metadata or batch APIs")

    def get_cost(self, request: Mapping[str, object]) -> Decimal:
        value = self.metadata.get_cost(
            dataset=request["dataset"],
            schema=request["schema"],
            symbols=request["symbols"],
            start=request["start"],
            end=request["end"],
            stype_in=request["stype_in"],
        )
        parsed = Decimal(str(value))
        if not parsed.is_finite() or parsed < 0:
            raise IntegrityError("live provider cost is invalid")
        return parsed

    def get_billable_size(self, request: Mapping[str, object]) -> int:
        value = self.metadata.get_billable_size(
            dataset=request["dataset"],
            schema=request["schema"],
            symbols=request["symbols"],
            start=request["start"],
            end=request["end"],
            stype_in=request["stype_in"],
        )
        if type(value) is not int or value < 0:
            raise IntegrityError("live provider billable size is invalid")
        return value

    def get_record_count(self, request: Mapping[str, object]) -> int:
        value = self.metadata.get_record_count(
            dataset=request["dataset"],
            schema=request["schema"],
            symbols=request["symbols"],
            start=request["start"],
            end=request["end"],
            stype_in=request["stype_in"],
        )
        if type(value) is not int or value <= 0:
            raise IntegrityError("live provider record count is invalid")
        return value

    def list_jobs(self) -> list[dict[str, object]]:
        value = self.batch.list_jobs(states="queued,processing,done,expired")
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise IntegrityError("provider job history response is invalid")
        return [dict(item) for item in value]

    def submit(self, request: Mapping[str, object]) -> dict[str, object]:
        value = self.batch.submit_job(
            dataset=request["dataset"],
            schema=request["schema"],
            symbols=request["symbols"],
            start=request["start"],
            end=request["end"],
            stype_in=request["stype_in"],
            stype_out=request["stype_out"],
            encoding=request["encoding"],
            compression=request["compression"],
            map_symbols=request["map_symbols"],
            split_symbols=request["split_symbols"],
            split_duration=request["split_duration"],
            delivery="download",
        )
        if not isinstance(value, dict):
            raise IntegrityError("provider submit response is invalid")
        return dict(value)

    def list_files(self, job_id: str) -> list[dict[str, object]]:
        value = self.batch.list_files(job_id=job_id)
        if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
            raise IntegrityError("provider file manifest is invalid")
        return [dict(item) for item in value]


def _control_retry(
    function: Callable[[], object],
    *,
    sleeper: Callable[[float], None] = time.sleep,
    delays: Sequence[float] = (1.0, 2.0, 4.0, 8.0),
) -> object:
    for attempt in range(len(delays) + 1):
        try:
            return function()
        except Exception as exc:
            status = getattr(exc, "http_status", getattr(exc, "status_code", None))
            if status not in _TRANSIENT_STATUS or attempt >= len(delays):
                raise
            sleeper(delays[attempt])
    raise IntegrityError("unreachable control retry state")


def _file_manifest_fields(item: Mapping[str, object]) -> tuple[str, int, str, str]:
    filename = item.get("filename")
    size = item.get("size")
    hash_value = item.get("hash", item.get("sha256"))
    urls = item.get("urls")
    url = urls.get("https") if isinstance(urls, Mapping) else item.get("url")
    if (
        not isinstance(filename, str)
        or Path(filename).name != filename
        or type(size) is not int
        or size < 0
        or not isinstance(hash_value, str)
        or not isinstance(url, str)
        or not url.startswith("https://")
    ):
        raise IntegrityError("provider file manifest fields are invalid")
    digest = hash_value.split(":", 1)[-1]
    if _SHA256.fullmatch(digest) is None:
        raise IntegrityError("provider file manifest SHA-256 is invalid")
    return filename, size, digest, url


def resumable_https_download(
    *,
    url: str,
    path: Path,
    expected_size: int,
    expected_sha256: str,
    api_key: str,
    http_get: Callable[..., object] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    retry_delays: Sequence[float] = (1.0, 2.0, 4.0, 8.0, 16.0),
) -> None:
    if http_get is None:
        import requests

        http_get = requests.get
    from requests.auth import HTTPBasicAuth

    path.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(len(retry_delays) + 1):
        existing = path.stat().st_size if path.exists() else 0
        if existing > expected_size:
            raise IntegrityError("partial download exceeds provider size")
        if existing == expected_size:
            if sha256_file(path, reject_hardlinks=False) == expected_sha256:
                return
            raise IntegrityError("complete-sized download has wrong SHA-256")
        headers: dict[str, str] = {}
        mode = "xb"
        if existing:
            headers["Range"] = f"bytes={existing}-{expected_size - 1}"
            mode = "ab"
        try:
            response_context = http_get(
                url=url,
                headers=headers,
                auth=HTTPBasicAuth(username=api_key, password=""),
                allow_redirects=True,
                stream=True,
                timeout=(30.0, 900.0),
            )
            with response_context as response:
                status = int(getattr(response, "status_code", 0))
                if status in _TRANSIENT_STATUS:
                    error = RuntimeError("transient batch download response")
                    setattr(error, "status_code", status)
                    raise error
                if status >= 400:
                    raise IntegrityError("batch download returned permanent HTTP error")
                if existing and status != 206:
                    raise IntegrityError("batch resume did not honor Range")
                with path.open(mode) as stream:
                    for chunk in response.iter_content(chunk_size=1_048_576):
                        if chunk:
                            stream.write(chunk)
                    stream.flush()
                    os.fsync(stream.fileno())
        except IntegrityError:
            raise
        except Exception:
            if attempt >= len(retry_delays):
                raise
            sleeper(retry_delays[attempt])
    if not path.is_file() or path.stat().st_size != expected_size:
        raise IntegrityError("resumable download size differs from provider manifest")
    if sha256_file(path, reject_hardlinks=False) != expected_sha256:
        raise IntegrityError("resumable download SHA-256 differs from provider manifest")


def atomic_install_directory(candidate: Path, destination: Path) -> None:
    if destination.exists():
        raise IntegrityError(f"destination appeared before atomic installation: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if candidate.stat().st_dev != destination.parent.stat().st_dev:
        raise IntegrityError("candidate and destination are not on the same volume")
    try:
        os.replace(candidate, destination)
    except FileExistsError as exc:
        raise IntegrityError(f"destination race blocked installation: {destination}") from exc


def _canonical_sidecar(
    *,
    target: Mapping[str, object],
    data_path: Path,
    probe: Mapping[str, object],
    provider_file_hash: str,
    provider_metadata_hash: str,
    provider_condition_hash: str,
    provider_manifest_hash: str,
    job_id: str,
) -> dict[str, object]:
    versions = _installed_versions()
    return {
        "compression": COMPRESSION,
        "created_at_utc": utc_now(),
        "data_condition_summary": "SEE_HASH_BOUND_PROVIDER_CONDITION_EVIDENCE",
        "databento_client_version": versions["databento"],
        "databento_schema": target["schema"],
        "dataset": DATASET,
        "dbn_byte_size": data_path.stat().st_size,
        "dbn_format_version": probe["dbn_format_version"],
        "databento_dbn_version": versions["databento-dbn"],
        "encoding": ENCODING,
        "evidence_paths": [],
        "installation_status": "INSTALLED_NOT_ACTIVATED",
        "job_id": job_id,
        "local_schema": TARGET_SCHEMAS[str(target["schema"])],
        "market": target["market"],
        "maximum_ts_event": probe["max_ts_event"],
        "minimum_ts_event": probe["min_ts_event"],
        "provider_condition_hash": provider_condition_hash,
        "provider_file_sha256": provider_file_hash,
        "provider_manifest_hash": provider_manifest_hash,
        "provider_metadata_hash": provider_metadata_hash,
        "record_count": probe["record_count"],
        "request_end_exclusive": target["intended_end_exclusive"],
        "request_fingerprint": target["request_fingerprint"],
        "request_start_inclusive": target["intended_start_inclusive"],
        "schema_version": SIDECAR_SCHEMA,
        "sha256": sha256_file(data_path, reject_hardlinks=False),
        "source_symbols": target["symbol_specification"]["symbols"],  # type: ignore[index]
        "stype_in": STYPE_IN,
        "stype_out": STYPE_OUT,
        "validation_results": {"passed": True, "requirements": target["validation_requirements"]},
    }


def _targets_to_jobs(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    actionable = [
        {
            **dict(item),
            "current_state": item["current_state"],
            "target_id": item["target_id"],
        }
        for item in rows
        if item["execution_action"] in {"DOWNLOAD_VALIDATE_INSTALL_ABSENT_TARGET_ONLY", "STAGE_CANDIDATE_NO_REPLACEMENT"}
    ]
    if not actionable:
        return []
    actionable_parents = {
        str(item["parent_planned_job_id"])
        for item in actionable
        if isinstance(item.get("parent_planned_job_id"), str)
    }
    certified_gaps: list[dict[str, object]] = []
    for item in rows:
        if (
            item.get("current_state") == "NO_DATA_CONFIRMED"
            and item.get("execution_action") == "NO_FILE_CREATE"
            and item.get("parent_planned_job_id") in actionable_parents
            and "no_data_evidence" in item
        ):
            _validated_no_data_evidence(item)
            certified_gaps.append({**dict(item), "current_state": "MISSING"})
    actionable_ids = {str(item["target_id"]) for item in actionable}
    jobs = build_job_plan([*actionable, *certified_gaps])
    return [
        job
        for job in jobs
        if actionable_ids.intersection(str(value) for value in job["target_ids"])
    ]


def _revalidate_manifest_targets(
    root: Path,
    rows: Sequence[Mapping[str, object]],
    *,
    allow_completed_resume: bool = False,
) -> tuple[Counter[str], int, set[str]]:
    states: Counter[str] = Counter()
    decoded = 0
    completed_resume_targets: set[str] = set()
    for row in rows:
        planned_state = str(row["current_state"])
        confirmed = row.get("provider_record_count")
        if planned_state == "NO_DATA_CONFIRMED":
            confirmed = 0
        observed = classify_target(
            root,
            row,
            confirmed_record_count=int(confirmed) if isinstance(confirmed, int) else None,
        )
        observed_state = str(observed["current_state"])
        completed_resume = (
            allow_completed_resume
            and planned_state == "MISSING"
            and observed_state == "COMPLETE_VALID"
        )
        if observed_state != planned_state and not completed_resume:
            raise IntegrityError(
                f"target state changed since manifest certification: {row['target_id']} "
                f"planned={planned_state} observed={observed_state}"
            )
        if completed_resume:
            completed_resume_targets.add(str(row["target_id"]))
        states[observed_state] += 1
        if observed_state == "COMPLETE_VALID":
            decoded += 1
    return states, decoded, completed_resume_targets


def execute_manifest(
    *,
    root: Path,
    manifest_path: Path,
    execute: bool = False,
    manifest_sha256: str | None = None,
    maximum_authorized_cost_usd: str | None = None,
    markets: Sequence[str] = (),
    schemas: Sequence[str] = (),
    years: Sequence[int] = (),
    validate_only: bool = False,
    resume: bool = False,
    reuse_job_id: str | None = None,
    resume_from_manifest_sha256: str | None = None,
    certification_output: Path | None = None,
    provider_factory: Callable[[Path], DatabentoBatchProvider] = DatabentoBatchProvider,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    root = root.resolve(strict=True)
    manifest_path = manifest_path.resolve(strict=True)
    observed_manifest_hash = sha256_file(manifest_path)
    rows = filter_manifest(
        load_manifest(manifest_path),
        markets=markets,
        schemas=schemas,
        years=years,
    )
    if resume_from_manifest_sha256 is not None:
        if _SHA256.fullmatch(resume_from_manifest_sha256) is None:
            raise UnauthorizedOperation("predecessor manifest SHA-256 is invalid")
        predecessor_bindings = {row.get("manifest_predecessor_sha256") for row in rows}
        if predecessor_bindings != {resume_from_manifest_sha256}:
            raise UnauthorizedOperation("successor manifest does not bind the accepted predecessor")
    if validate_only:
        validation, decoded, _ = _revalidate_manifest_targets(
            root,
            rows,
            allow_completed_resume=resume,
        )
    else:
        validation = Counter(str(item["current_state"]) for item in rows)
        decoded = 0
    actions = _targets_to_jobs(rows)
    base_result: dict[str, object] = {
        "actions": len(actions),
        "dry_run": not execute,
        "dbn_files_decoded": decoded,
        "manifest_sha256": observed_manifest_hash,
        "paid_submissions": 0,
        "selected_targets": len(rows),
        "state_counts": dict(validation),
        "validated_at_utc": utc_now(),
    }
    if certification_output is not None:
        output = certification_output.resolve(strict=False)
        try:
            output.relative_to(root)
        except ValueError as exc:
            raise ContractError("certification output must be inside repository") from exc
        atomic_json(output, {**base_result, "schema_version": "ohlcv_backfill_completion_certification/1.0.0"})
    if validate_only or not execute:
        return base_result
    if manifest_sha256 is None or _SHA256.fullmatch(manifest_sha256) is None:
        raise UnauthorizedOperation("execute mode requires an exact manifest SHA-256")
    if manifest_sha256 != observed_manifest_hash:
        raise UnauthorizedOperation("accepted manifest SHA-256 differs from current bytes")
    if maximum_authorized_cost_usd is None:
        raise UnauthorizedOperation("execute mode requires an explicit maximum USD cost")
    try:
        cost_cap = Decimal(maximum_authorized_cost_usd)
    except InvalidOperation as exc:
        raise UnauthorizedOperation("maximum authorized cost is invalid") from exc
    if not cost_cap.is_finite() or cost_cap < 0:
        raise UnauthorizedOperation("maximum authorized cost is invalid")
    live_validation, live_decoded, completed_resume_targets = _revalidate_manifest_targets(
        root,
        rows,
        allow_completed_resume=resume,
    )
    if completed_resume_targets:
        remaining_actions: list[dict[str, object]] = []
        row_by_target_id = {str(row["target_id"]): row for row in rows}
        for job in actions:
            job_target_ids = {str(value) for value in job["target_ids"]}  # type: ignore[index]
            job_data_target_ids = {
                target_id
                for target_id in job_target_ids
                if row_by_target_id[target_id].get("current_state") != "NO_DATA_CONFIRMED"
            }
            completed_job_targets = job_data_target_ids.intersection(completed_resume_targets)
            if (
                completed_job_targets
                and completed_job_targets != job_data_target_ids
                and resume_from_manifest_sha256 is None
            ):
                raise IntegrityError("resume found a partially complete immutable provider job")
            if completed_job_targets != job_data_target_ids:
                remaining_actions.append(job)
        actions = remaining_actions
    base_result["state_counts"] = dict(live_validation)
    base_result["dbn_files_decoded"] = live_decoded
    base_result["actions"] = len(actions)
    if not actions:
        return {
            **base_result,
            "dry_run": False,
            "result": "NO_ACTION_ALL_TARGETS_COMPLETE_OR_NO_DATA",
            "resume_requested": resume,
        }
    lock_path = root / LOCK_RELATIVE
    with FileLease(lock_path):
        provider = provider_factory(root)
        staging_manifest_hash = resume_from_manifest_sha256 or observed_manifest_hash
        staging_root = root / STAGING_RELATIVE / staging_manifest_hash[:16]
        staging_root.mkdir(parents=True, exist_ok=True)
        ledger_path = staging_root / "job_ledger.jsonl"
        ledger = _read_ledger(ledger_path)
        live_costs: dict[str, Decimal] = {}
        for job in actions:
            request = job["request"]
            live_costs[str(job["request_fingerprint"])] = _control_retry(
                lambda request=request: provider.get_cost(request), sleeper=sleeper
            )  # type: ignore[assignment]
        total_cost = sum(live_costs.values(), Decimal("0"))
        if total_cost > cost_cap:
            raise UnauthorizedOperation("live estimated cost exceeds explicit maximum")
        capacity = _drive_capacity(root)
        required = sum(int(item.get("expected_incremental_bytes", 0)) for item in rows) + SAFETY_MARGIN_FLOOR_BYTES
        if int(capacity["free_bytes"]) < required:
            raise UnauthorizedOperation("insufficient free disk space for execution")
        history = _control_retry(provider.list_jobs, sleeper=sleeper)
        if not isinstance(history, list):
            raise IntegrityError("provider job history is invalid")
        ledger_bindings = {
            str(item["job_id"]): str(item["request_fingerprint"])
            for item in ledger
            if isinstance(item.get("job_id"), str)
            and isinstance(item.get("request_fingerprint"), str)
            and _JOB_ID.fullmatch(str(item["job_id"]))
            and _SHA256.fullmatch(str(item["request_fingerprint"]))
        }
        history = [
            {
                **dict(item),
                **(
                    {"request_fingerprint": ledger_bindings[_job_identifier(item)]}
                    if _job_identifier(item) in ledger_bindings
                    else {}
                ),
            }
            for item in history
        ]
        paid_submissions = 0
        processed_jobs: list[dict[str, object]] = []
        for job in actions:
            fingerprint = str(job["request_fingerprint"])
            ledger_job_id = _ledger_reuse_job_id(fingerprint, ledger_bindings)
            if reuse_job_id is not None and ledger_job_id not in {None, reuse_job_id}:
                raise IntegrityError("required reusable job ID differs from the resumable ledger")
            reusable = select_reusable_job(
                fingerprint,
                history,
                required_job_id=reuse_job_id or ledger_job_id,
            )
            if reusable is None:
                response = provider.submit(job["request"])
                job_id = _job_identifier(response)
                paid_submissions += 1
                _append_ledger(
                    ledger_path,
                    {
                        "event": "BATCH_JOB_SUBMITTED_ONCE",
                        "job_id": job_id,
                        "request_fingerprint": fingerprint,
                        "submitted_time": utc_now(),
                    },
                )
                ledger_bindings[job_id] = fingerprint
                state = _job_state(response)
            else:
                job_id = _job_identifier(reusable)
                state = _job_state(reusable)
                _append_ledger(
                    ledger_path,
                    {
                        "event": "BATCH_JOB_REUSED_NO_SUBMISSION",
                        "job_id": job_id,
                        "request_fingerprint": fingerprint,
                        "state": state,
                        "time": utc_now(),
                    },
                )
            polls = 0
            while state in {"queued", "processing"}:
                if polls >= 120:
                    raise UnauthorizedOperation("batch polling attempt ceiling reached")
                sleeper(min(60.0, 2.0 * (2 ** min(polls, 5))))
                history = _control_retry(provider.list_jobs, sleeper=sleeper)
                matches = [item for item in history if _job_identifier(item) == job_id]
                if len(matches) != 1:
                    raise IntegrityError("submitted job is missing or duplicated in history")
                state = _job_state(matches[0])
                polls += 1
            if state == "expired":
                raise IntegrityError("batch job expired before download; replan is required")
            files = _control_retry(lambda: provider.list_files(job_id), sleeper=sleeper)
            if not isinstance(files, list):
                raise IntegrityError("provider file manifest is invalid")
            provider_manifest = [
                {key: value for key, value in item.items() if str(key).casefold() not in {"url", "urls", "download_url", "signed_url"}}
                for item in files
            ]
            provider_manifest_hash = sha256_json(provider_manifest)
            job_root = staging_root / job_id
            support_root = job_root / "provider_support"
            downloads_root = job_root / "downloads"
            support_root.mkdir(parents=True, exist_ok=True)
            downloads_root.mkdir(parents=True, exist_ok=True)
            downloaded: list[dict[str, object]] = []
            for item in files:
                filename, size, digest, url = _file_manifest_fields(item)
                partial = downloads_root / f"{filename}.partial"
                final_download = downloads_root / filename
                if final_download.exists():
                    if final_download.stat().st_size != size or sha256_file(final_download, reject_hardlinks=False) != digest:
                        raise IntegrityError("existing staged provider file differs from manifest")
                else:
                    resumable_https_download(
                        url=url,
                        path=partial,
                        expected_size=size,
                        expected_sha256=digest,
                        api_key=provider.key,
                        sleeper=sleeper,
                    )
                    os.replace(partial, final_download)
                downloaded.append({"filename": filename, "sha256": digest, "size": size})
            atomic_json(support_root / "provider_file_manifest_redacted.json", provider_manifest)
            job_targets = [
                item
                for item in rows
                if item["target_id"] in set(job["target_ids"])
            ]
            mapped: dict[str, tuple[Path, dict[str, object], str]] = {}
            provider_hashes = {str(item["filename"]): str(item["sha256"]) for item in downloaded}
            for candidate in downloads_root.glob(f"*{DBN_SUFFIX}"):
                probe = _probe_dbn(candidate)
                matches = [
                    target
                    for target in job_targets
                    if target["schema"] == probe["schema"]
                    and target["intended_start_inclusive"] == probe["metadata_start"]
                    and target["intended_end_exclusive"] == probe["metadata_end"]
                    and target["symbol_specification"]["symbols"] == probe["symbols"]
                ]
                if len(matches) != 1:
                    raise IntegrityError("downloaded DBN does not map to exactly one target")
                target_id = str(matches[0]["target_id"])
                if target_id in mapped:
                    raise IntegrityError("multiple downloaded DBNs map to one target")
                mapped[target_id] = (candidate, probe, provider_hashes[candidate.name])
            installed: list[str] = []
            no_data: list[str] = []
            preserved: list[str] = []
            staged_candidates: list[str] = []
            for target in job_targets:
                target_id = str(target["target_id"])
                if target.get("current_state") == "NO_DATA_CONFIRMED":
                    evidence = _validated_no_data_evidence(target)
                    if evidence["job_id"] != job_id:
                        raise IntegrityError("certified no-data job ID differs from reused job")
                    if evidence["request_fingerprint"] != fingerprint:
                        raise IntegrityError("certified no-data request fingerprint differs")
                    if evidence["provider_manifest_hash"] != provider_manifest_hash:
                        raise IntegrityError("certified no-data provider manifest differs")
                    if target_id in mapped:
                        raise IntegrityError("provider returned a DBN for a certified no-data target")
                    no_data.append(target_id)
                    continue
                if target_id not in mapped:
                    raise IntegrityError("batch package lacks an expected annual DBN")
                source, probe, provider_file_hash = mapped[target_id]
                provider_count = target.get("provider_record_count")
                if provider_count is not None and int(provider_count) != int(probe["record_count"]):
                    raise IntegrityError("downloaded DBN record count differs from quote evidence")
                if int(probe["record_count"]) <= 0:
                    raise IntegrityError("downloaded DBN is empty where records were expected")
                if target_id in completed_resume_targets:
                    destination_data = contained_path(root, str(target["final_path"]))
                    destination_sidecar = contained_path(root, str(target["sidecar_path"]))
                    if sha256_file(destination_data, reject_hardlinks=False) != sha256_file(source, reject_hardlinks=False):
                        raise IntegrityError("completed resume target differs from reused provider bytes")
                    sidecar = load_object(destination_sidecar, "completed resume sidecar")
                    if (
                        sidecar.get("job_id") != job_id
                        or sidecar.get("provider_manifest_hash") != provider_manifest_hash
                    ):
                        raise IntegrityError("completed resume sidecar differs from reused provider evidence")
                    preserved.append(target_id)
                    continue
                candidate_year = (
                    job_root
                    / "candidates"
                    / TARGET_SCHEMAS[str(target["schema"])]
                    / str(target["market"])
                    / str(target["year"])
                )
                candidate_year.mkdir(parents=True, exist_ok=True)
                candidate_data = candidate_year / Path(str(target["final_path"])).name
                if candidate_data.exists():
                    if sha256_file(candidate_data, reject_hardlinks=False) != sha256_file(source, reject_hardlinks=False):
                        raise IntegrityError("existing candidate DBN differs from downloaded bytes")
                else:
                    with source.open("rb") as incoming, candidate_data.open("xb") as outgoing:
                        shutil.copyfileobj(incoming, outgoing, length=1_048_576)
                        outgoing.flush()
                        os.fsync(outgoing.fileno())
                candidate_sidecar = candidate_year / Path(str(target["sidecar_path"])).name
                sidecar = _canonical_sidecar(
                    target=target,
                    data_path=candidate_data,
                    probe=probe,
                    provider_file_hash=provider_file_hash,
                    provider_metadata_hash=str(target["provider_metadata_hash"]),
                    provider_condition_hash=str(target["provider_condition_hash"]),
                    provider_manifest_hash=provider_manifest_hash,
                    job_id=job_id,
                )
                atomic_json(candidate_sidecar, sidecar)
                destination_year = contained_path(root, str(Path(str(target["final_path"])).parent))
                if target["execution_action"] == "DOWNLOAD_VALIDATE_INSTALL_ABSENT_TARGET_ONLY":
                    if destination_year.exists():
                        raise IntegrityError("destination race blocked annual pair installation")
                    atomic_install_directory(candidate_year, destination_year)
                    installed.append(target_id)
                else:
                    staged_candidates.append(target_id)
            _append_ledger(
                ledger_path,
                {
                    "completion_time": utc_now(),
                    "downloaded_files": downloaded,
                    "event": "BATCH_PACKAGE_DOWNLOADED_AND_HASH_VERIFIED",
                    "installed_target_ids": installed,
                    "installation_state": "INSTALLED_ABSENT_TARGETS_AND_PRESERVED_REPLACEMENT_CANDIDATES",
                    "job_id": job_id,
                    "no_data_target_ids": no_data,
                    "preserved_target_ids": preserved,
                    "provider_manifest_hash": provider_manifest_hash,
                    "request_fingerprint": fingerprint,
                    "staged_candidate_target_ids": staged_candidates,
                    "validation_state": "PASSED",
                },
            )
            processed_jobs.append(
                {
                    "job_id": job_id,
                    "provider_manifest_hash": provider_manifest_hash,
                    "request_fingerprint": fingerprint,
                    "state": state,
                }
            )
        return {
            **base_result,
            "dry_run": False,
            "estimated_cost_usd": format(total_cost, "f"),
            "paid_submissions": paid_submissions,
            "processed_jobs": processed_jobs,
            "resume_requested": resume,
        }


def quote_cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a metadata-only OHLCV-1D/1H quote and plan")
    parser.add_argument("--repository-root", default=".")
    parser.add_argument("--report-root", required=True)
    parser.add_argument("--end-exclusive", required=True)
    parser.add_argument("--task-start-utc", default=utc_now())
    parser.add_argument("--metadata-only", action="store_true", required=True)
    parser.add_argument("--current-year-successor", action="store_true")
    parser.add_argument("--seed-raw-provider-evidence")
    args = parser.parse_args(argv)
    if not args.metadata_only:
        raise UnauthorizedOperation("quote builder is metadata-only")
    result = build_report_package(
        Path(args.repository_root),
        Path(args.report_root),
        end_exclusive=args.end_exclusive,
        task_start_utc=args.task_start_utc,
        current_year_successor=args.current_year_successor,
        seed_raw_provider_path=Path(args.seed_raw_provider_evidence) if args.seed_raw_provider_evidence else None,
    )
    print(json.dumps({
        "manifest_sha256": result["manifest_sha256"],
        "paid_requests_submitted": 0,
        "report_root": result["report_root"],
        "status": result["status"]["status"],  # type: ignore[index]
    }, sort_keys=True))
    return 0


def execute_cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Dry-run-default OHLCV-1D/1H batch executor")
    parser.add_argument("--repository-root", default=".")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--manifest-sha256")
    parser.add_argument("--maximum-authorized-cost-usd")
    parser.add_argument("--market", action="append", default=[])
    parser.add_argument("--schema", action="append", default=[])
    parser.add_argument("--year", action="append", type=int, default=[])
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--reuse-job-id")
    parser.add_argument("--certification-output")
    args = parser.parse_args(argv)
    result = execute_manifest(
        root=Path(args.repository_root),
        manifest_path=Path(args.manifest),
        execute=args.execute,
        manifest_sha256=args.manifest_sha256,
        maximum_authorized_cost_usd=args.maximum_authorized_cost_usd,
        markets=args.market,
        schemas=args.schema,
        years=args.year,
        validate_only=args.validate_only,
        resume=args.resume,
        reuse_job_id=args.reuse_job_id,
        certification_output=Path(args.certification_output) if args.certification_output else None,
    )
    print(json.dumps(result, sort_keys=True))
    return 0
