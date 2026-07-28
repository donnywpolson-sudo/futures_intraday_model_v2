"""Independent, bounded DBN-to-causal certification for the active data view."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
import ctypes
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .active_data_view import (
    CERTIFICATION_STATE,
    build_content_validation_receipt,
)
from .boundary import RepoBoundary
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    sha256_bytes,
    sha256_file,
    sha256_json,
)
from .data_layout import (
    DataReleaseReceipt,
    manifest_relative_path,
    verify_data_release_manifest,
)
from .errors import ContractError, IntegrityError
from .foundation.materialize import load_causal_interval, load_raw_interval
from .foundation.decoder import _chunks
from .foundation.parquet import (
    CAUSAL_BAR_SCHEMA,
    DEFINITION_SCHEMA,
    RAW_BAR_SCHEMA,
    write_causal_bars,
    write_raw_bars,
    write_relevant_definitions,
)
from .foundation.snapshot import PublishedDbnRelease
from .foundation.support import VerifiedFoundationPolicies
from .source_symbology import build_query_contract


CERTIFICATION_REPORT_SCHEMA = "causal_market_year_certification_report/1.0.0"
INTERVAL_REPORT_SCHEMA = "causal_interval_independent_reproduction/1.0.0"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _process_memory_bytes() -> dict[str, int]:
    if os.name == "nt":
        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.c_ulong),
                ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        get_current_process = kernel32.GetCurrentProcess
        get_current_process.argtypes = []
        get_current_process.restype = ctypes.c_void_p
        get_process_memory_info = psapi.GetProcessMemoryInfo
        get_process_memory_info.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ProcessMemoryCounters),
            ctypes.c_ulong,
        ]
        get_process_memory_info.restype = ctypes.c_int

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        handle = get_current_process()
        ok = get_process_memory_info(handle, ctypes.byref(counters), counters.cb)
        if not ok:
            raise IntegrityError(
                "cannot measure certification process memory "
                f"(Win32 error {ctypes.get_last_error()})"
            )
        return {
            "peak_working_set_bytes": int(counters.PeakWorkingSetSize),
            "working_set_bytes": int(counters.WorkingSetSize),
        }
    import resource

    usage = resource.getrusage(resource.RUSAGE_SELF)
    scale = 1024 if os.name != "darwin" else 1
    peak = int(usage.ru_maxrss) * scale
    return {"peak_working_set_bytes": peak, "working_set_bytes": peak}


def _schema_fingerprint(schema: pa.Schema) -> str:
    return sha256_bytes(schema.serialize().to_pybytes())


def _parquet_batches(
    paths: Sequence[Path], *, batch_rows: int
) -> Iterator[pa.RecordBatch]:
    if (
        not paths
        or isinstance(batch_rows, bool)
        or not isinstance(batch_rows, int)
        or not 1 <= batch_rows <= 1_000_000
    ):
        raise ContractError("canonical Parquet scan bounds are invalid")
    schema: pa.Schema | None = None
    pending: pa.Table | None = None
    for path in paths:
        assert_plain_file(path)
        parquet = pq.ParquetFile(path)
        if schema is None:
            schema = parquet.schema_arrow
        elif not parquet.schema_arrow.equals(schema, check_metadata=True):
            raise IntegrityError("Parquet sequence schemas differ")
        for raw_batch in parquet.iter_batches(batch_size=batch_rows):
            table = pa.Table.from_batches([raw_batch], schema=schema)
            pending = table if pending is None else pa.concat_tables([pending, table])
            while pending.num_rows >= batch_rows:
                head = pending.slice(0, batch_rows).combine_chunks()
                batches = head.to_batches(max_chunksize=batch_rows)
                if len(batches) != 1 or batches[0].num_rows != batch_rows:
                    raise IntegrityError("canonical Parquet rechunking failed")
                yield batches[0]
                pending = pending.slice(batch_rows)
    if pending is not None and pending.num_rows:
        tail = pending.combine_chunks().to_batches(max_chunksize=batch_rows)
        if len(tail) != 1:
            raise IntegrityError("canonical Parquet tail rechunking failed")
        yield tail[0]


def canonical_parquet_fingerprint(
    path: Path, *, batch_rows: int = 100_000
) -> dict[str, object]:
    return canonical_parquet_sequence_fingerprint((path,), batch_rows=batch_rows)


def canonical_parquet_sequence_fingerprint(
    paths: Sequence[Path], *, batch_rows: int = 100_000
) -> dict[str, object]:
    if not paths:
        raise ContractError("canonical Parquet sequence is empty")
    first = pq.ParquetFile(paths[0])
    schema = first.schema_arrow
    schema_fingerprint = _schema_fingerprint(schema)
    digest = hashlib.sha256()
    digest.update(b"CAUSAL_CANONICAL_ARROW_ROWS_V1\0")
    digest.update(bytes.fromhex(schema_fingerprint))
    rows = 0
    batches = 0
    for batch in _parquet_batches(paths, batch_rows=batch_rows):
        encoded = batch.serialize().to_pybytes()
        digest.update(batch.num_rows.to_bytes(8, "big"))
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        rows += batch.num_rows
        batches += 1
    if rows <= 0:
        raise IntegrityError("canonical Parquet sequence has no rows")
    return {
        "batch_rows": batch_rows,
        "canonical_batch_count": batches,
        "canonical_row_hash": digest.hexdigest(),
        "row_count": rows,
        "schema_fingerprint": schema_fingerprint,
    }


def compare_parquet_canonical(
    expected: Path,
    reproduced: Path,
    *,
    expected_schema: pa.Schema,
    batch_rows: int = 100_000,
) -> dict[str, object]:
    left = pq.ParquetFile(expected)
    right = pq.ParquetFile(reproduced)
    if (
        not left.schema_arrow.equals(expected_schema, check_metadata=True)
        or not right.schema_arrow.equals(expected_schema, check_metadata=True)
    ):
        raise IntegrityError("expected or reproduced Parquet schema differs")
    left_batches = _parquet_batches((expected,), batch_rows=batch_rows)
    right_batches = _parquet_batches((reproduced,), batch_rows=batch_rows)
    digest = hashlib.sha256()
    digest.update(b"CAUSAL_CANONICAL_ARROW_ROWS_V1\0")
    schema_fingerprint = _schema_fingerprint(expected_schema)
    digest.update(bytes.fromhex(schema_fingerprint))
    rows = 0
    batches = 0
    while True:
        left_batch = next(left_batches, None)
        right_batch = next(right_batches, None)
        if left_batch is None or right_batch is None:
            if left_batch is not right_batch:
                raise IntegrityError("reproduced Parquet row count differs")
            break
        if not left_batch.equals(right_batch):
            raise IntegrityError("reproduced canonical Parquet rows differ")
        encoded = left_batch.serialize().to_pybytes()
        digest.update(left_batch.num_rows.to_bytes(8, "big"))
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        rows += left_batch.num_rows
        batches += 1
    if rows <= 0:
        raise IntegrityError("canonical Parquet comparison has no rows")
    return {
        "canonical_batch_count": batches,
        "canonical_row_hash": digest.hexdigest(),
        "container_hash_equal": sha256_file(expected) == sha256_file(reproduced),
        "row_count": rows,
        "schema_fingerprint": schema_fingerprint,
    }


def validate_causal_invariants(
    path: Path, *, batch_rows: int = 100_000
) -> dict[str, object]:
    parquet = pq.ParquetFile(path)
    if not parquet.schema_arrow.equals(CAUSAL_BAR_SCHEMA, check_metadata=True):
        raise IntegrityError("causal invariant scan received the wrong schema")
    previous: tuple[int, int, int] | None = None
    row_count = 0
    denominator_rows = 0
    failure_rows = 0
    eligible_rows = 0
    dispositions: dict[str, int] = {}
    first_event: int | None = None
    last_event: int | None = None
    required = [
        "event_at_ns",
        "available_at_ns",
        "resolution_as_of_ns",
        "publisher_id",
        "instrument_id",
        "open_nano",
        "high_nano",
        "low_nano",
        "close_nano",
        "volume",
        "disposition",
        "prediction_in_coverage_denominator",
        "failure_code",
        "actual_identity_hash",
        "exchange_session_date",
    ]
    for batch in parquet.iter_batches(batch_size=batch_rows, columns=required):
        event = batch.column(0).to_numpy(zero_copy_only=False)
        available = batch.column(1).to_numpy(zero_copy_only=False)
        resolution = batch.column(2).to_numpy(zero_copy_only=False)
        publisher = batch.column(3).to_numpy(zero_copy_only=False)
        instrument = batch.column(4).to_numpy(zero_copy_only=False)
        open_value = batch.column(5).to_numpy(zero_copy_only=False)
        high = batch.column(6).to_numpy(zero_copy_only=False)
        low = batch.column(7).to_numpy(zero_copy_only=False)
        close = batch.column(8).to_numpy(zero_copy_only=False)
        volume = batch.column(9).to_numpy(zero_copy_only=False)
        if (
            np.any(available <= event)
            or np.any(resolution > available)
            or np.any(high < open_value)
            or np.any(high < close)
            or np.any(high < low)
            or np.any(low > open_value)
            or np.any(low > close)
        ):
            raise IntegrityError("causal timestamps or OHLC relationships are invalid")
        # volume is uint64 in the exact schema, so a negative value cannot be
        # represented.  The schema check above is the fail-closed type proof.
        order = np.lexsort((instrument, publisher, event))
        if not np.array_equal(order, np.arange(batch.num_rows)):
            raise IntegrityError("causal primary keys are not deterministically ordered")
        keys = list(zip(event.tolist(), publisher.tolist(), instrument.tolist()))
        if keys:
            if previous is not None and keys[0] <= previous:
                raise IntegrityError("causal primary keys duplicate or reverse across batches")
            if any(current <= prior for prior, current in zip(keys, keys[1:])):
                raise IntegrityError("causal primary keys duplicate or reverse")
            previous = tuple(int(value) for value in keys[-1])
            first_event = int(keys[0][0]) if first_event is None else first_event
            last_event = int(keys[-1][0])
        for disposition, denominator, failure, identity, session_date in zip(
            batch.column(10).to_pylist(),
            batch.column(11).to_pylist(),
            batch.column(12).to_pylist(),
            batch.column(13).to_pylist(),
            batch.column(14).to_pylist(),
        ):
            if not isinstance(disposition, str) or not disposition:
                raise IntegrityError("causal disposition is invalid")
            if type(denominator) is not bool or denominator is not True:
                raise IntegrityError("missing/degraded rows left the coverage denominator")
            dispositions[disposition] = dispositions.get(disposition, 0) + 1
            denominator_rows += 1
            if disposition == "ELIGIBLE":
                eligible_rows += 1
                if failure is not None or identity is None or session_date is None:
                    raise IntegrityError("eligible causal row lacks identity/session evidence")
            else:
                failure_rows += 1
                if not isinstance(failure, str) or not failure:
                    raise IntegrityError("fail-closed causal row lacks a failure code")
            if session_date is not None and (
                not isinstance(session_date, str) or _DATE.fullmatch(session_date) is None
            ):
                raise IntegrityError("causal exchange-session date is invalid")
        row_count += batch.num_rows
    if (
        row_count <= 0
        or denominator_rows != row_count
        or eligible_rows + failure_rows != row_count
        or first_event is None
        or last_event is None
    ):
        raise IntegrityError("causal coverage census is invalid")
    return {
        "disposition_counts": dict(sorted(dispositions.items())),
        "eligible_rows": eligible_rows,
        "failure_rows": failure_rows,
        "first_event_at_ns": first_event,
        "last_event_at_ns": last_event,
        "prediction_in_coverage_denominator_rows": denominator_rows,
        "row_count": row_count,
    }


def _load_canonical_json(path: Path, description: str) -> dict[str, object]:
    assert_plain_file(path)
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def _verify_download_sidecar(
    path: Path,
    *,
    expected_data_path: str,
    expected_sha256: str,
    expected_size: int,
) -> dict[str, object]:
    assert_plain_file(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("DBN download sidecar is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise IntegrityError("DBN download sidecar is not a JSON object")
    required = {
        "api_client_version",
        "compression",
        "dataset",
        "downloaded_at",
        "encoding",
        "end",
        "file_sha256",
        "file_size_bytes",
        "job_id",
        "market",
        "path",
        "request_status",
        "schema",
        "start",
        "stype_in",
        "stype_out",
        "symbols_requested",
        "vendor",
    }
    if (
        set(payload) != required
        or payload["vendor"] != "databento"
        or payload["dataset"] != "GLBX.MDP3"
        or payload["encoding"] != "dbn"
        or payload["compression"] != "zstd"
        or payload["request_status"] != "ok"
        or payload["path"] != expected_data_path
        or payload["file_sha256"] != expected_sha256
        or payload["file_size_bytes"] != expected_size
        or not isinstance(payload["job_id"], str)
        or not payload["job_id"]
    ):
        raise IntegrityError("DBN download sidecar provenance is invalid")
    return payload


def _aggregate_raw_minutes(
    path: Path, *, interval_ns: int, batch_rows: int
) -> dict[int, tuple[int, int, int, int, int]]:
    parquet = pq.ParquetFile(path)
    if not parquet.schema_arrow.equals(RAW_BAR_SCHEMA, check_metadata=True):
        raise IntegrityError("aggregation source is not canonical raw one-minute data")
    aggregated: dict[int, tuple[int, int, int, int, int]] = {}
    for batch in parquet.iter_batches(
        batch_size=batch_rows,
        columns=[
            "event_at_ns",
            "open_nano",
            "high_nano",
            "low_nano",
            "close_nano",
            "volume",
        ],
    ):
        for row in batch.to_pylist():
            bucket = int(row["event_at_ns"]) // interval_ns * interval_ns
            current = aggregated.get(bucket)
            if current is None:
                aggregated[bucket] = (
                    int(row["open_nano"]),
                    int(row["high_nano"]),
                    int(row["low_nano"]),
                    int(row["close_nano"]),
                    int(row["volume"]),
                )
            else:
                aggregated[bucket] = (
                    current[0],
                    max(current[1], int(row["high_nano"])),
                    min(current[2], int(row["low_nano"])),
                    int(row["close_nano"]),
                    current[4] + int(row["volume"]),
                )
    if not aggregated:
        raise IntegrityError("one-minute aggregation produced no intervals")
    return aggregated


def _decode_provider_aggregate(
    binding: object,
    *,
    schema: str,
    market: str,
    query_contract: Mapping[str, object],
    batch_rows: int,
) -> dict[int, tuple[int, int, int, int, int]]:
    rows: dict[int, tuple[int, int, int, int, int]] = {}
    for chunk in _chunks(  # type: ignore[arg-type]
        binding,
        schema=schema,
        market=market,
        expected_query_contract=query_contract,
        batch_rows=batch_rows,
    ):
        for raw in chunk:
            key = int(raw["ts_event"])
            value = (
                int(raw["open"]),
                int(raw["high"]),
                int(raw["low"]),
                int(raw["close"]),
                int(raw["volume"]),
            )
            if key in rows:
                raise IntegrityError("provider aggregation source contains a duplicate bucket")
            rows[key] = value
    if not rows:
        raise IntegrityError("provider aggregation source contains no rows")
    return rows


def validate_aggregation_crosschecks(
    *,
    raw_bars_path: Path,
    dbn_release: PublishedDbnRelease,
    market: str,
    aggregation_sources: Sequence[Mapping[str, object]],
    batch_rows: int,
) -> dict[str, object]:
    if not aggregation_sources:
        return {
            "reason": "NO_OVERLAPPING_PROVIDER_AGGREGATE_BOUND_BY_PLAN",
            "state": "NOT_AVAILABLE",
        }
    results: list[dict[str, object]] = []
    intervals = {
        "ohlcv-1h": 3_600_000_000_000,
        "ohlcv-1d": 86_400_000_000_000,
    }
    for source in aggregation_sources:
        schema = source.get("schema")
        if schema not in intervals:
            raise IntegrityError("aggregation plan contains an unsupported schema")
        relative_path = source.get("relative_path")
        sidecar_relative_path = source.get("sidecar_relative_path")
        if not isinstance(relative_path, str) or not isinstance(
            sidecar_relative_path, str
        ):
            raise IntegrityError("aggregation source paths are invalid")
        binding = dbn_release.file(relative_path)
        sidecar_binding = dbn_release.file(sidecar_relative_path)
        if (
            binding.sha256 != source.get("sha256")
            or binding.size != source.get("size")
            or sidecar_binding.sha256 != source.get("sidecar_sha256")
            or sidecar_binding.size != source.get("sidecar_size")
        ):
            raise IntegrityError("aggregation source differs from the exact pilot plan")
        binding.verify()
        sidecar_binding.verify()
        sidecar = _verify_download_sidecar(
            sidecar_binding.path,
            expected_data_path=binding.logical_path,
            expected_sha256=binding.sha256,
            expected_size=binding.size,
        )
        query = build_query_contract(
            schema=str(schema),
            market=market,
            start=str(sidecar["start"]),
            end=str(sidecar["end"]),
            stype_in=sidecar["stype_in"],
            symbols=sidecar["symbols_requested"],
        )
        if query["query_contract_id"] != source.get("query_contract_id"):
            raise IntegrityError("aggregation query contract differs from the pilot plan")
        minute = _aggregate_raw_minutes(
            raw_bars_path,
            interval_ns=intervals[str(schema)],
            batch_rows=batch_rows,
        )
        provider = _decode_provider_aggregate(
            binding,
            schema=str(schema),
            market=market,
            query_contract=query,
            batch_rows=batch_rows,
        )
        aligned = sorted(set(minute) & set(provider))
        if not aligned:
            raise IntegrityError("aggregation sources have no aligned intervals")
        mismatches = [key for key in aligned if minute[key] != provider[key]]
        minute_only = sorted(set(minute) - set(provider))
        provider_only = sorted(set(provider) - set(minute))
        boundary_keys = {
            min((*minute, *provider)),
            max((*minute, *provider)),
        }
        if (
            mismatches
            or any(key not in boundary_keys for key in minute_only)
            or any(key not in boundary_keys for key in provider_only)
        ):
            raise IntegrityError(
                f"unexplained {schema} aggregation disagreement"
            )
        results.append(
            {
                "aligned_intervals": len(aligned),
                "boundary_minute_only": len(minute_only),
                "boundary_provider_only": len(provider_only),
                "provider_rows": len(provider),
                "query_contract_id": query["query_contract_id"],
                "schema": schema,
                "source_sha256": binding.sha256,
                "state": "PASS",
            }
        )
    return {"results": results, "state": "PASS"}


def _policy_receipt_for_causal(
    causal_receipt: DataReleaseReceipt,
    raw_release_id: str,
    *,
    boundary: RepoBoundary,
) -> DataReleaseReceipt:
    manifest = causal_receipt.verify(boundary)
    candidates = [
        release_id
        for release_id in manifest.source_release_ids
        if release_id != raw_release_id
    ]
    if len(candidates) != 1:
        raise IntegrityError("causal release has ambiguous policy lineage")
    path = boundary.active_root / manifest_relative_path("controls", candidates[0])
    return DataReleaseReceipt.from_manifest(path, boundary)


def certify_interval(
    *,
    boundary: RepoBoundary,
    foundation_interval: Mapping[str, object],
    workspace: Path,
    batch_rows: int = 100_000,
) -> dict[str, object]:
    """Reproduce one selected interval without publishing any release."""

    market = foundation_interval.get("market")
    year = foundation_interval.get("year")
    start = foundation_interval.get("start")
    end = foundation_interval.get("end")
    if (
        not isinstance(market, str)
        or isinstance(year, bool)
        or not isinstance(year, int)
        or not isinstance(start, str)
        or not isinstance(end, str)
        or foundation_interval.get("coverage_disposition")
        not in {
            "AUTHORITATIVE_INTERVAL",
            "AUTHORITATIVE_INTERVAL_WITH_EXACT_REDUNDANT_CROSSCHECK",
        }
    ):
        raise IntegrityError("certification interval selection is invalid")
    raw_payload = foundation_interval.get("raw_release_receipt")
    causal_payload = foundation_interval.get("causal_release_receipt")
    if not isinstance(raw_payload, dict) or not isinstance(causal_payload, dict):
        raise IntegrityError("certification interval release receipts are absent")
    raw_receipt = DataReleaseReceipt.from_dict(raw_payload)
    causal_receipt = DataReleaseReceipt.from_dict(causal_payload)
    loaded_raw = load_raw_interval(raw_receipt, boundary=boundary)
    selected_causal, causal_interval_receipt = load_causal_interval(
        causal_receipt, boundary=boundary
    )
    raw_interval_receipt = loaded_raw.interval_receipt
    if (
        raw_interval_receipt.get("market") != market
        or raw_interval_receipt.get("year") != year
        or causal_interval_receipt.get("market") != market
        or causal_interval_receipt.get("year") != year
        or causal_interval_receipt.get("source_raw_release_id")
        != raw_receipt.release_id
    ):
        raise IntegrityError("certification interval selectors or lineage differ")
    dbn_release_id = str(raw_interval_receipt["source_dbn_release_id"])
    dbn_manifest = boundary.active_root / manifest_relative_path("dbn", dbn_release_id)
    dbn_release = PublishedDbnRelease.open(
        dbn_manifest, boundary=boundary, verify_files=False
    )
    bar_binding = dbn_release.file(str(raw_interval_receipt["source_bar_file_path"]))
    definition_binding = dbn_release.file(
        str(raw_interval_receipt["source_definition_file_path"])
    )
    for binding in (bar_binding, definition_binding):
        binding.verify()
        sidecar = dbn_release.file(f"{binding.relative_path}.manifest.json")
        sidecar.verify()
        _verify_download_sidecar(
            sidecar.path,
            expected_data_path=binding.logical_path,
            expected_sha256=binding.sha256,
            expected_size=binding.size,
        )
    for key in (
        "definition_release_receipt",
        "economics_release_receipt",
    ):
        receipt_payload = foundation_interval.get(key)
        if not isinstance(receipt_payload, dict):
            raise IntegrityError(f"foundation interval lacks {key}")
        DataReleaseReceipt.from_dict(receipt_payload).verify(boundary)
    policy_receipt = _policy_receipt_for_causal(
        causal_receipt, raw_receipt.release_id, boundary=boundary
    )
    assert_no_linklike_ancestors(workspace)
    if workspace.exists():
        raise IntegrityError("certification interval workspace already exists")
    workspace.mkdir(parents=True)
    policies = VerifiedFoundationPolicies.from_embedded_release(
        policy_receipt,
        boundary=boundary,
        workspace=workspace / "policy",
        required_market=market,
    )
    if (
        causal_interval_receipt.get("foundation_policy_release_id")
        != policy_receipt.release_id
        or causal_interval_receipt.get("foundation_policy_set_id")
        != policies.policy_set_id
    ):
        raise IntegrityError(
            "causal interval policy identity differs from its embedded release"
        )
    reproduced_bars = workspace / "raw" / "bars.parquet"
    reproduced_definitions = workspace / "raw" / "definitions.parquet"
    reproduced_causal = workspace / "causal" / "bars.parquet"
    started = time.perf_counter()
    bar_rows, instrument_dates = write_raw_bars(
        bar_binding,
        market=market,
        expected_query_contract=raw_interval_receipt["bar_query_contract"],
        output=reproduced_bars,
        batch_rows=batch_rows,
    )
    scanned, definition_rows, definition_census, definition_keys = (
        write_relevant_definitions(
            definition_binding,
            market=market,
            expected_query_contract=raw_interval_receipt[
                "definition_query_contract"
            ],
            required_instrument_dates=instrument_dates,
            output=reproduced_definitions,
            batch_rows=batch_rows,
        )
    )
    decode_seconds = time.perf_counter() - started
    raw_bars_comparison = compare_parquet_canonical(
        loaded_raw.bars_path,
        reproduced_bars,
        expected_schema=RAW_BAR_SCHEMA,
        batch_rows=batch_rows,
    )
    definitions_comparison = compare_parquet_canonical(
        loaded_raw.definitions_path,
        reproduced_definitions,
        expected_schema=DEFINITION_SCHEMA,
        batch_rows=batch_rows,
    )
    aggregation_sources = foundation_interval.get("aggregation_sources", ())
    if not isinstance(aggregation_sources, list):
        raise IntegrityError("aggregation source plan is invalid")
    aggregation_check = validate_aggregation_crosschecks(
        raw_bars_path=reproduced_bars,
        dbn_release=dbn_release,
        market=market,
        aggregation_sources=aggregation_sources,
        batch_rows=batch_rows,
    )
    phase2_started = time.perf_counter()
    causal_rows, disposition_counts, epoch_counts = write_causal_bars(
        raw_bars_path=reproduced_bars,
        definitions_path=reproduced_definitions,
        policies=policies,
        source_raw_release_id=raw_receipt.release_id,
        output=reproduced_causal,
        batch_rows=batch_rows,
    )
    phase2_seconds = time.perf_counter() - phase2_started
    causal_comparison = compare_parquet_canonical(
        selected_causal,
        reproduced_causal,
        expected_schema=CAUSAL_BAR_SCHEMA,
        batch_rows=batch_rows,
    )
    invariants = validate_causal_invariants(selected_causal, batch_rows=batch_rows)
    if (
        bar_rows != raw_bars_comparison["row_count"]
        or definition_rows != definitions_comparison["row_count"]
        or causal_rows != causal_comparison["row_count"]
        or causal_rows != invariants["row_count"]
        or set(instrument_dates) - set(definition_keys)
        or disposition_counts != invariants["disposition_counts"]
    ):
        raise IntegrityError("independent interval reproduction census differs")
    core: dict[str, object] = {
        "canonical_causal": causal_comparison,
        "canonical_raw_bars": raw_bars_comparison,
        "canonical_raw_definitions": definitions_comparison,
        "aggregation_check": aggregation_check,
        "causal_release_id": causal_receipt.release_id,
        "coverage_disposition": foundation_interval["coverage_disposition"],
        "dbn_release_id": dbn_release_id,
        "definition_rows_scanned": scanned,
        "definition_timestamp_census": definition_census,
        "end": end,
        "historical_calendar_claim": (
            "NOT_OFFICIAL_HISTORICAL_CME_SESSION_AUTHORITY"
        ),
        "historical_evidence_basis": (
            "IMMUTABLE_ACCEPTED_DATABENTO_DBN_OBSERVABILITY"
        ),
        "invariants": invariants,
        "market": market,
        "measurements": {
            "dbn_decode_seconds": format(decode_seconds, ".9f"),
            "phase2_reproduction_seconds": format(phase2_seconds, ".9f"),
            "temporary_bytes": sum(
                path.stat().st_size
                for path in workspace.rglob("*")
                if path.is_file()
            ),
        },
        "policy_release_id": policy_receipt.release_id,
        "raw_release_id": raw_receipt.release_id,
        "schema_version": INTERVAL_REPORT_SCHEMA,
        "start": start,
        "status": "PASS",
        "uncertainty_rule": "UNOBSERVED_TIME_IS_MISSING_NOT_CLOSED",
        "year": year,
    }
    return {**core, "interval_report_id": sha256_json(core)}


def certify_market_year(
    *,
    boundary: RepoBoundary,
    foundation_release_id: str,
    foundation_manifest_sha256: str,
    foundation_intervals: Sequence[Mapping[str, object]],
    workspace: Path,
    semantic_bindings: Mapping[str, str],
    implementation_bindings: Mapping[str, str],
    environment_bindings: Mapping[str, str],
    batch_rows: int = 100_000,
) -> tuple[dict[str, object], dict[str, object]]:
    if not foundation_intervals:
        raise ContractError("market-year certification has no intervals")
    market = foundation_intervals[0].get("market")
    year = foundation_intervals[0].get("year")
    if any(
        item.get("market") != market or item.get("year") != year
        for item in foundation_intervals
    ):
        raise IntegrityError("market-year certification intervals are mixed")
    ordered = sorted(
        foundation_intervals,
        key=lambda item: (str(item.get("start")), str(item.get("end"))),
    )
    for previous, current in zip(ordered, ordered[1:]):
        if previous.get("end") != current.get("start"):
            raise IntegrityError("market-year certification intervals are not contiguous")
    reports: list[dict[str, object]] = []
    selected_paths: list[Path] = []
    source_bindings: list[dict[str, object]] = []
    started = time.perf_counter()
    cpu_started = time.process_time()
    memory_started = _process_memory_bytes()
    for index, interval in enumerate(ordered):
        report = certify_interval(
            boundary=boundary,
            foundation_interval=interval,
            workspace=workspace / f"interval-{index:03d}",
            batch_rows=batch_rows,
        )
        reports.append(report)
        causal_receipt = DataReleaseReceipt.from_dict(
            interval["causal_release_receipt"]  # type: ignore[arg-type]
        )
        causal_path, _ = load_causal_interval(causal_receipt, boundary=boundary)
        selected_paths.append(causal_path)
        source_bindings.append(
            {
                "causal_release_id": report["causal_release_id"],
                "dbn_release_id": report["dbn_release_id"],
                "end": report["end"],
                "raw_release_id": report["raw_release_id"],
                "start": report["start"],
            }
        )
    fingerprint = canonical_parquet_sequence_fingerprint(
        selected_paths, batch_rows=batch_rows
    )
    aggregation_states = {
        str(report["aggregation_check"]["state"])  # type: ignore[index]
        for report in reports
    }
    if not aggregation_states.issubset({"PASS", "NOT_AVAILABLE"}):
        raise IntegrityError("aggregation cross-check has an invalid state")
    aggregation_check = {
        "interval_results": [
            report["aggregation_check"] for report in reports
        ],
        "state": (
            "PASS"
            if aggregation_states == {"PASS"}
            else "NOT_AVAILABLE"
        ),
    }
    checks = {
        "aggregation_availability_classification": "PASS",
        "causal_reproduction": "PASS",
        "coverage": "PASS",
        "dbn_to_raw_reconciliation": "PASS",
        "deterministic_ordering": "PASS",
        "historical_observability_boundary": "PASS",
        "identity_and_roll": "PASS",
        "missing_state_preservation": "PASS",
        "ohlcv_invariants": "PASS",
        "provider_download_provenance": "PASS",
        "timestamp_causality": "PASS",
    }
    content_receipt = build_content_validation_receipt(
        market=str(market),
        year=int(year),
        coverage_start=str(ordered[0]["start"]),
        coverage_end=str(ordered[-1]["end"]),
        source_bindings=source_bindings,
        semantic_bindings={
            **semantic_bindings,
            "foundation_manifest_sha256": foundation_manifest_sha256,
            "foundation_release_id": foundation_release_id,
        },
        implementation_bindings=implementation_bindings,
        environment_bindings=environment_bindings,
        canonical_schema_fingerprint=str(fingerprint["schema_fingerprint"]),
        canonical_row_hash=str(fingerprint["canonical_row_hash"]),
        row_count=int(fingerprint["row_count"]),
        checks=checks,
        limitations=[
            "EMPIRICAL_OBSERVABILITY_NOT_OFFICIAL_HISTORICAL_CME_CALENDAR",
            (
                "AGGREGATION_CROSSCHECK_NOT_AVAILABLE"
                if aggregation_check["state"] == "NOT_AVAILABLE"
                else ""
            ),
        ],
    )
    limitations = [
        item for item in content_receipt["limitations"] if item
    ]
    content_core = {
        key: value
        for key, value in content_receipt.items()
        if key != "content_validation_receipt_id"
    }
    content_core["limitations"] = limitations
    content_receipt = {
        **content_core,
        "content_validation_receipt_id": sha256_json(content_core),
    }
    core: dict[str, object] = {
        "aggregation_check": dict(aggregation_check),
        "canonical_market_year": fingerprint,
        "content_validation_receipt_id": content_receipt[
            "content_validation_receipt_id"
        ],
        "coverage_end": ordered[-1]["end"],
        "coverage_start": ordered[0]["start"],
        "foundation_manifest_sha256": foundation_manifest_sha256,
        "foundation_release_id": foundation_release_id,
        "interval_reports": reports,
        "market": market,
        "measurements": {
            "certification_seconds": format(time.perf_counter() - started, ".9f"),
            "cpu_seconds": format(time.process_time() - cpu_started, ".9f"),
            "interval_count": len(reports),
            "peak_working_set_bytes": _process_memory_bytes()[
                "peak_working_set_bytes"
            ],
            "starting_working_set_bytes": memory_started["working_set_bytes"],
            "selected_causal_bytes": sum(path.stat().st_size for path in selected_paths),
            "temporary_bytes": sum(
                path.stat().st_size
                for path in workspace.rglob("*")
                if path.is_file()
            ),
        },
        "schema_version": CERTIFICATION_REPORT_SCHEMA,
        "source_paths": [
            path.relative_to(boundary.active_root).as_posix()
            for path in selected_paths
        ],
        "source_sha256s": [sha256_file(path) for path in selected_paths],
        "state": CERTIFICATION_STATE,
        "status": "PASS",
        "year": year,
    }
    return {**core, "certification_report_id": sha256_json(core)}, content_receipt
