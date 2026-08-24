"""Prepare and execute the exact bounded-2025 provider acquisition.

Preparation reads only JSON sidecars and filesystem metadata.  The execution
surface streams opaque DBN responses into a unique inactive staging root; it
does not decode rows, register canonical files, publish, or activate anything.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Final

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .micro_alpha_acquisition import (
    DownloadProviderApis,
    build_file_download_provider_apis,
)
from .runtime_environment import require_locked_repository_environment


PLAN_PATH: Final = Path("configs/bounded_2025_development_acquisition_v1.json")
SOURCE_CONTRACT_PATH: Final = Path("configs/source_contract.json")
BOUNDARY_ASSESSMENT_PATH: Final = Path(
    "reports/post_rebuild_causal_observation_storage_and_2025_boundary_v2/"
    "pcosbv2_20260823T2124026356081Z_f32f4a77/"
    "DEVELOPMENT_2025_BOUNDARY_ASSESSMENT_V2.json"
)
PARQUET_BENCHMARK_PATH: Final = Path(
    "reports/post_rebuild_causal_observation_storage_and_2025_boundary_v2/"
    "pcosbv2_20260823T2124026356081Z_f32f4a77/PARQUET_BENCHMARK_V2.json"
)
OPERATION: Final = "ACQUIRE_BOUNDED_2025_DEVELOPMENT_DBN_ONCE"
STAGING_ROOT: Final = Path(
    "state/provider_acquisition_staging/bounded_2025_development_v1"
)
DEVELOPMENT_START: Final = "2025-01-01T00:00:00Z"
DEVELOPMENT_END_EXCLUSIVE: Final = "2025-07-13T22:00:00Z"
ANNUAL_START: Final = "2025-01-01"
ANNUAL_END: Final = "2026-01-01"
CANONICAL_INTERVAL_NAME: Final = "2025-01-01_2025-07-13T220000Z"
FAMILIES: Final = (
    "definition",
    "ohlcv_1d",
    "ohlcv_1h",
    "ohlcv_1m",
    "ohlcv_1s",
    "statistics",
    "status",
)
MAXIMUM_PARALLEL_DOWNLOADS: Final = 2
MAXIMUM_CONCURRENT_OHLCV_1S: Final = 1
MAXIMUM_RETRIES: Final = 0
MAXIMUM_RUNTIME_SECONDS: Final = 43_200
MINIMUM_REQUEST_TIMEOUT_SECONDS: Final = 900
MAXIMUM_REQUEST_TIMEOUT_SECONDS: Final = 1_800
ASSUMED_PLANNING_BITS_PER_SECOND: Final = 25_000_000
MINIMUM_POST_PEAK_FREE_BYTES: Final = 100 * 1024**3
MAXIMUM_EXTERNAL_COST_USD: Final = "0"
_PLAN_QUERY_FIELDS: Final = frozenset(
    {
        "dataset",
        "symbols",
        "schema",
        "start",
        "end",
        "stype_in",
        "stype_out",
        "encoding",
        "compression",
    }
)
_COST_QUERY_FIELDS: Final = (
    "dataset",
    "symbols",
    "schema",
    "start",
    "end",
    "stype_in",
)
_RANGE_QUERY_FIELDS: Final = (*_COST_QUERY_FIELDS, "stype_out")


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{description} is not an object")
    return value


def _git_head(root: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    value = completed.stdout.strip()
    if len(value) != 40:
        raise IntegrityError("Git HEAD is invalid")
    return value


def _self_hashed(value: Mapping[str, object], key: str) -> bool:
    core = dict(value)
    observed = core.pop(key, None)
    return observed == sha256_json(core)


def _provider_schema(family: str) -> str:
    return family.replace("_", "-")


def _normalized_annual_sidecar(
    sidecar: Mapping[str, object], *, family: str
) -> dict[str, object]:
    """Normalize the two certified sidecar shapes without weakening identity."""

    if sidecar.get("schema_version") == "ohlcv_historical_backfill_sidecar/1.0.0":
        return {
            "vendor": "databento",
            "dataset": sidecar.get("dataset"),
            "schema": sidecar.get("databento_schema"),
            "local_schema": sidecar.get("local_schema"),
            "market": sidecar.get("market"),
            "symbols_requested": sidecar.get("source_symbols"),
            "start": sidecar.get("request_start_inclusive"),
            "end": sidecar.get("request_end_exclusive"),
            "stype_in": sidecar.get("stype_in"),
            "stype_out": sidecar.get("stype_out"),
            "encoding": sidecar.get("encoding"),
            "compression": sidecar.get("compression"),
            "file_size_bytes": sidecar.get("dbn_byte_size"),
            "file_sha256": sidecar.get("sha256"),
            "source_sidecar_schema": sidecar.get("schema_version"),
        }
    return {
        "vendor": sidecar.get("vendor"),
        "dataset": sidecar.get("dataset"),
        "schema": sidecar.get("schema"),
        "local_schema": family,
        "market": sidecar.get("market"),
        "symbols_requested": sidecar.get("symbols_requested"),
        "start": sidecar.get("start"),
        "end": sidecar.get("end"),
        "stype_in": sidecar.get("stype_in"),
        "stype_out": sidecar.get("stype_out"),
        "encoding": sidecar.get("encoding"),
        "compression": sidecar.get("compression"),
        "file_size_bytes": sidecar.get("file_size_bytes"),
        "file_sha256": sidecar.get("file_sha256"),
        "source_sidecar_schema": sidecar.get("schema_version")
        or "legacy_provider_download_sidecar",
    }


def _request_timeout_seconds(annual_size: int) -> int:
    estimated = max(1.0, annual_size * 8 / ASSUMED_PLANNING_BITS_PER_SECOND)
    return min(
        MAXIMUM_REQUEST_TIMEOUT_SECONDS,
        max(MINIMUM_REQUEST_TIMEOUT_SECONDS, int(estimated * 2 + 60)),
    )


def _request_byte_ceiling(annual_size: int) -> int:
    return max(1_048_576, (annual_size * 11 + 9) // 10 + 1_048_576)


def _validate_source_authorities(
    *, root: Path
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    source = _object(root / SOURCE_CONTRACT_PATH, "source contract")
    boundary = _object(root / BOUNDARY_ASSESSMENT_PATH, "2025 boundary assessment")
    benchmark = _object(root / PARQUET_BENCHMARK_PATH, "Parquet benchmark")
    if (
        source.get("contract_id")
        != "47ad7a1c100bec86494f3c1eb1e78ba56a4d35c6be993da6ded8e2e7f925823f"
        or source.get("active_canonical_source", {}).get("release_id")
        != "9867aedac9cfe732d015489fc4093ffc4aaab5ad698b75a5fa00ca7e1f457995"
        or boundary.get("development_2025_boundary_assessment_v2_id")
        != "aa731fa0721ce566eae7820cede12940d6e55ebbd728e191e1a40305604a3eb1"
        or boundary.get("development_start_inclusive") != DEVELOPMENT_START
        or boundary.get("development_end_exclusive") != DEVELOPMENT_END_EXCLUSIVE
        or boundary.get("required_exact_successor", {}).get("root_family_pairs")
        != 287
        or benchmark.get("parquet_benchmark_v2_id")
        != "6fa42a5690a000ec653c86fbf0e0b0df403d4bc000f204b12abee8e80305a4d4"
        or benchmark.get("projection", {}).get("proposed_output_ceiling_bytes")
        != 19_100_000_000
        or benchmark.get("projection", {}).get(
            "proposed_peak_incremental_ceiling_bytes"
        )
        != 21_100_000_000
    ):
        raise IntegrityError("bounded-2025 source authority drifted")
    return source, boundary, benchmark


def build_acquisition_plan(*, root: Path) -> dict[str, object]:
    """Build the exact source-safe plan without opening a DBN payload."""

    root = root.resolve(strict=True)
    source, boundary, benchmark = _validate_source_authorities(root=root)
    universe = source.get("universe", {})
    roots = universe.get("standard_roots")
    micros = universe.get("deferred_micro_roots")
    if (
        not isinstance(roots, list)
        or len(roots) != 41
        or len(set(roots)) != 41
        or not all(type(item) is str and item for item in roots)
        or not isinstance(micros, list)
        or len(micros) != 17
        or set(roots) & set(micros)
    ):
        raise IntegrityError("41/17 source universe drifted")

    requests: list[dict[str, object]] = []
    total_annual_bytes = 0
    total_byte_ceiling = 0
    for market in roots:
        for family in FAMILIES:
            schema = _provider_schema(family)
            annual_dbn = (
                root
                / "data"
                / "dbn"
                / family
                / market
                / "2025"
                / "2025-01-01_2026-01-01.dbn.zst"
            )
            annual_sidecar = Path(f"{annual_dbn}.manifest.json")
            if (
                not annual_dbn.is_file()
                or annual_dbn.is_symlink()
                or not annual_sidecar.is_file()
                or annual_sidecar.is_symlink()
            ):
                raise IntegrityError(f"annual source pair is unavailable: {market}/{family}")
            sidecar_document = _object(annual_sidecar, "annual source sidecar")
            sidecar = _normalized_annual_sidecar(sidecar_document, family=family)
            annual_size = annual_dbn.stat().st_size
            if (
                sidecar.get("vendor") != "databento"
                or sidecar.get("dataset") != "GLBX.MDP3"
                or sidecar.get("schema") != schema
                or sidecar.get("market") != market
                or str(sidecar.get("start"))[:10] != ANNUAL_START
                or str(sidecar.get("end"))[:10] != ANNUAL_END
                or sidecar.get("encoding") != "dbn"
                or sidecar.get("compression") != "zstd"
                or sidecar.get("file_size_bytes") != annual_size
                or type(sidecar.get("file_sha256")) is not str
                or len(str(sidecar["file_sha256"])) != 64
                or not isinstance(sidecar.get("symbols_requested"), list)
                or len(sidecar["symbols_requested"]) != 1
            ):
                raise IntegrityError(f"annual sidecar drifted: {market}/{family}")
            query = {
                "dataset": "GLBX.MDP3",
                "symbols": list(sidecar["symbols_requested"]),
                "schema": schema,
                "start": DEVELOPMENT_START,
                "end": DEVELOPMENT_END_EXCLUSIVE,
                "stype_in": sidecar.get("stype_in"),
                "stype_out": sidecar.get("stype_out"),
                "encoding": "dbn",
                "compression": "zstd",
            }
            if query["stype_in"] not in {"parent", "continuous"} or query[
                "stype_out"
            ] != "instrument_id":
                raise IntegrityError(f"symbology drifted: {market}/{family}")
            canonical_dbn = (
                Path("data/dbn")
                / family
                / market
                / "2025"
                / f"{CANONICAL_INTERVAL_NAME}.dbn.zst"
            )
            canonical_sidecar = Path(f"{canonical_dbn}.manifest.json")
            if (root / canonical_dbn).exists() or (root / canonical_sidecar).exists():
                raise IntegrityError(f"bounded destination already exists: {market}/{family}")
            ceiling = _request_byte_ceiling(annual_size)
            core = {
                "market": market,
                "family": family,
                "worker_class": "HEAVY_OHLCV_1S" if family == "ohlcv_1s" else "LIGHT",
                "query": query,
                "annual_source_metadata": {
                    "dbn_path": annual_dbn.relative_to(root).as_posix(),
                    "declared_dbn_sha256": sidecar["file_sha256"],
                    "dbn_size_bytes": annual_size,
                    "sidecar_path": annual_sidecar.relative_to(root).as_posix(),
                    "sidecar_sha256": sha256_file(annual_sidecar),
                    "sidecar_schema": sidecar["source_sidecar_schema"],
                    "scientific_reuse": False,
                    "purpose": "REQUEST_SHAPE_AND_SIZE_PROXY_ONLY_NO_DBN_OPEN",
                },
                "canonical_destination": canonical_dbn.as_posix(),
                "canonical_sidecar_destination": canonical_sidecar.as_posix(),
                "request_byte_ceiling": ceiling,
                "request_timeout_seconds": _request_timeout_seconds(annual_size),
            }
            requests.append({**core, "request_id": sha256_json(core)})
            total_annual_bytes += annual_size
            total_byte_ceiling += ceiling

    if len(requests) != 287 or sum(r["family"] == "ohlcv_1s" for r in requests) != 41:
        raise IntegrityError("bounded request inventory is not exactly 287/41-heavy")
    causal_peak = int(
        benchmark["projection"]["proposed_peak_incremental_ceiling_bytes"]
    )
    required_free = MINIMUM_POST_PEAK_FREE_BYTES + total_byte_ceiling + causal_peak
    core = {
        "schema_version": "bounded_2025_development_acquisition_plan/1.0.0",
        "state": "PREPARED_IMPLEMENTATION_COMMIT_AND_NETWORK_APPROVAL_REQUIRED",
        "operation": OPERATION,
        "prepared_parent_head": _git_head(root),
        "source_authority": {
            "source_contract_id": source["contract_id"],
            "canonical_release_id": source["active_canonical_source"]["release_id"],
            "causal_contract_id": boundary["source_authority"]["causal_contract_id"],
            "boundary_assessment_id": boundary[
                "development_2025_boundary_assessment_v2_id"
            ],
            "parquet_benchmark_v2_id": benchmark["parquet_benchmark_v2_id"],
        },
        "interval": {
            "start_inclusive": DEVELOPMENT_START,
            "end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
            "annual_crossing_sources_rejected_for_development": True,
        },
        "universe": {
            "standard_roots": roots,
            "deferred_micros": micros,
            "standard_root_count": 41,
            "deferred_micro_count": 17,
        },
        "families": list(FAMILIES),
        "requests": requests,
        "counts": {
            "requests": 287,
            "expected_dbns": 287,
            "expected_sidecars": 287,
            "heavy_requests": 41,
            "light_requests": 246,
        },
        "worker_contract": {
            "maximum_parallel_downloads": 2,
            "maximum_concurrent_ohlcv_1s": 1,
            "heavy_worker_families": ["ohlcv_1s"],
            "light_worker_families": [
                item for item in FAMILIES if item != "ohlcv_1s"
            ],
            "maximum_submitted_worker_tasks": 2,
            "stop_new_requests_on_first_failure": True,
            "automatic_retries": 0,
        },
        "limits": {
            "maximum_external_cost_usd": MAXIMUM_EXTERNAL_COST_USD,
            "maximum_provider_calls": 574,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "minimum_request_timeout_seconds": MINIMUM_REQUEST_TIMEOUT_SECONDS,
            "maximum_request_timeout_seconds": MAXIMUM_REQUEST_TIMEOUT_SECONDS,
            "annual_size_proxy_bytes": total_annual_bytes,
            "maximum_download_bytes": total_byte_ceiling,
            "causal_build_peak_ceiling_bytes": causal_peak,
            "minimum_post_peak_free_bytes": MINIMUM_POST_PEAK_FREE_BYTES,
            "required_free_before_acquisition_bytes": required_free,
        },
        "custody": {
            "provider_staging_root": STAGING_ROOT.as_posix(),
            "download_to_unique_partial": True,
            "same_filesystem_atomic_staging_finalize": True,
            "canonical_registration_during_acquisition": False,
            "create_only": True,
            "dbn_payload_decode": False,
            "holdout_or_forward_access": False,
            "publication": False,
            "activation": False,
            "raw_annual_2025_and_2026_preserved": True,
        },
        "execution_authority": {
            "provider_access": False,
            "requires_separate_network_approval_after_commit_and_push": True,
            "single_use_authorization": True,
            "credential_content_recording": False,
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def load_acquisition_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH, "bounded-2025 acquisition plan")
    if not _self_hashed(plan, "plan_id"):
        raise IntegrityError("bounded-2025 acquisition plan identity is invalid")
    if (
        plan.get("operation") != OPERATION
        or plan.get("counts")
        != {
            "requests": 287,
            "expected_dbns": 287,
            "expected_sidecars": 287,
            "heavy_requests": 41,
            "light_requests": 246,
        }
        or plan.get("worker_contract", {}).get("maximum_parallel_downloads") != 2
        or plan.get("worker_contract", {}).get("maximum_concurrent_ohlcv_1s")
        != 1
        or plan.get("worker_contract", {}).get("automatic_retries") != 0
        or len(plan.get("requests", [])) != 287
    ):
        raise IntegrityError("bounded-2025 acquisition plan semantics drifted")
    request_ids = [item.get("request_id") for item in plan["requests"]]
    if len(set(request_ids)) != 287:
        raise IntegrityError("bounded-2025 request IDs are not unique")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    plan_sha256 = sha256_file(root / PLAN_PATH)
    return {
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": plan_sha256,
        "plan_id": str(plan["plan_id"]),
        "plan_sha256": plan_sha256,
        "implementation_commit": _git_head(root),
        "request_count": "287",
        "maximum_parallel_downloads": "2",
        "maximum_concurrent_ohlcv_1s": "1",
        "maximum_download_bytes": str(plan["limits"]["maximum_download_bytes"]),
        "maximum_external_cost_usd": "0",
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "canonical_registration": "false",
        "dbn_payload_decode": "false",
        "holdout_or_forward_access": "false",
    }


def _set_timeout(function: Callable[..., object], seconds: float) -> None:
    owner = getattr(function, "__self__", None)
    client = getattr(owner, "_client", None)
    options = getattr(client, "_options", None)
    if options is not None and hasattr(options, "timeout"):  # pragma: no branch
        options.timeout = seconds


def _zero_cost(value: object) -> None:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError("provider cost response is invalid") from exc
    if not amount.is_finite() or amount != Decimal("0"):
        raise UnauthorizedOperation("bounded-2025 provider cost is not exactly zero")


def _provider_query(
    item: Mapping[str, object], *, fields: Sequence[str]
) -> dict[str, object]:
    query = item.get("query")
    if (
        not isinstance(query, Mapping)
        or set(query) != _PLAN_QUERY_FIELDS
        or query.get("encoding") != "dbn"
        or query.get("compression") != "zstd"
    ):
        raise IntegrityError("bounded-2025 provider query contract drifted")
    return {field: query[field] for field in fields}


def _provider_cost_query(item: Mapping[str, object]) -> dict[str, object]:
    """Return only arguments accepted by Databento Metadata.get_cost."""

    return _provider_query(item, fields=_COST_QUERY_FIELDS)


def _provider_range_query(item: Mapping[str, object]) -> dict[str, object]:
    """Return only arguments accepted by Databento Timeseries.get_range."""

    return _provider_query(item, fields=_RANGE_QUERY_FIELDS)


@dataclass(frozen=True)
class _WorkerResult:
    records: tuple[dict[str, object], ...]
    calls: int
    failure_type: str | None
    failed_request_id: str | None


def _download_worker(
    *,
    root: Path,
    worker_name: str,
    items: Sequence[Mapping[str, object]],
    provider_factory: Callable[[], DownloadProviderApis],
    stop_event: threading.Event,
    total_state: dict[str, int],
    total_lock: threading.Lock,
    maximum_total_bytes: int,
    started: float,
    clock: Callable[[], float],
) -> _WorkerResult:
    records: list[dict[str, object]] = []
    calls = 0
    failed_request_id: str | None = None
    try:
        provider = provider_factory()
        worker_root = root / worker_name
        worker_root.mkdir()
        for item in items:
            if stop_event.is_set():
                break
            if clock() - started >= MAXIMUM_RUNTIME_SECONDS:
                raise UnauthorizedOperation("bounded-2025 runtime ceiling reached")
            failed_request_id = str(item["request_id"])
            partial = worker_root / f"{failed_request_id}.dbn.zst.partial"
            sidecar_partial = worker_root / f"{failed_request_id}.manifest.json.partial"
            final_dbn = worker_root / f"{failed_request_id}.dbn.zst"
            final_sidecar = worker_root / f"{failed_request_id}.manifest.json"
            if any(
                path.exists()
                for path in (partial, sidecar_partial, final_dbn, final_sidecar)
            ):
                raise IntegrityError("bounded-2025 staging collision")
            _set_timeout(provider.get_range, float(item["request_timeout_seconds"]))
            calls += 1
            provider.get_range(**_provider_range_query(item), path=str(partial))
            if not partial.is_file() or partial.is_symlink():
                raise IntegrityError("provider did not create a regular partial DBN")
            size = partial.stat().st_size
            if size <= 0 or size > int(item["request_byte_ceiling"]):
                raise UnauthorizedOperation("download is empty or exceeds its ceiling")
            digest = sha256_file(partial)
            with total_lock:
                proposed = total_state["bytes"] + size
                if proposed > maximum_total_bytes:
                    raise UnauthorizedOperation("total download byte ceiling exceeded")
                total_state["bytes"] = proposed
            sidecar_core = {
                "schema_version": "bounded_2025_provider_staging_sidecar/1.0.0",
                "state": "INACTIVE_UNREGISTERED_PROVIDER_STAGING",
                "request_id": failed_request_id,
                "market": item["market"],
                "family": item["family"],
                "exact_query": item["query"],
                "byte_count": size,
                "sha256": digest,
                "canonical_destination": item["canonical_destination"],
                "canonical_sidecar_destination": item[
                    "canonical_sidecar_destination"
                ],
                "dbn_rows_decoded": 0,
                "holdout_or_forward_access": False,
                "registered": False,
                "published": False,
                "activated": False,
            }
            sidecar = {**sidecar_core, "manifest_id": sha256_json(sidecar_core)}
            with sidecar_partial.open("xb") as stream:
                stream.write(canonical_bytes(sidecar) + b"\n")
            partial.rename(final_dbn)
            sidecar_partial.rename(final_sidecar)
            records.append(
                {
                    "request_id": failed_request_id,
                    "worker": worker_name,
                    "staging_dbn": final_dbn.relative_to(root).as_posix(),
                    "staging_sidecar": final_sidecar.relative_to(root).as_posix(),
                    "byte_count": size,
                    "sha256": digest,
                }
            )
            failed_request_id = None
    except Exception as exc:
        stop_event.set()
        return _WorkerResult(tuple(records), calls, type(exc).__name__, failed_request_id)
    return _WorkerResult(tuple(records), calls, None, None)


def execute_authorized_acquisition(
    *,
    root: Path,
    authorization: OperationReceipt,
    provider_factory: Callable[[], DownloadProviderApis] | None = None,
    clock: Callable[[], float] = time.monotonic,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
    environment_check: Callable[[Path], object] = require_locked_repository_environment,
) -> dict[str, object]:
    """Execute once into inactive staging; never register canonical paths."""

    root = root.resolve(strict=True)
    plan = load_acquisition_plan(root=root)
    environment_check(root)
    free = getattr(disk_usage(root), "free", None)
    required_free = int(plan["limits"]["required_free_before_acquisition_bytes"])
    if type(free) is not int or free < required_free:
        raise UnauthorizedOperation("insufficient free space for bounded acquisition")
    boundary = RepoBoundary(root)
    claim = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=required_scope(root=root, plan=plan),
    )
    attempt = root / STAGING_ROOT / authorization.receipt_id[:16]
    boundary.assert_active_path(
        attempt.absolute(),
        purpose="bounded-2025 inactive provider staging",
        subtree=STAGING_ROOT.as_posix(),
    )
    attempt.mkdir(parents=True, exist_ok=False)
    provider_factory = provider_factory or (
        lambda: build_file_download_provider_apis(root=root)
    )
    started = clock()
    cost_calls = 0
    range_calls = 0
    records: list[dict[str, object]] = []
    failures: list[dict[str, object]] = []
    try:
        cost_provider = provider_factory()
        for item in plan["requests"]:
            _set_timeout(cost_provider.get_cost, 90.0)
            cost_calls += 1
            _zero_cost(cost_provider.get_cost(**_provider_cost_query(item)))
        heavy = [item for item in plan["requests"] if item["family"] == "ohlcv_1s"]
        light = [item for item in plan["requests"] if item["family"] != "ohlcv_1s"]
        stop_event = threading.Event()
        total_state = {"bytes": 0}
        total_lock = threading.Lock()
        with ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="bounded-2025-dbn"
        ) as executor:
            futures = [
                executor.submit(
                    _download_worker,
                    root=attempt,
                    worker_name="heavy_ohlcv_1s",
                    items=heavy,
                    provider_factory=provider_factory,
                    stop_event=stop_event,
                    total_state=total_state,
                    total_lock=total_lock,
                    maximum_total_bytes=int(
                        plan["limits"]["maximum_download_bytes"]
                    ),
                    started=started,
                    clock=clock,
                ),
                executor.submit(
                    _download_worker,
                    root=attempt,
                    worker_name="light_families",
                    items=light,
                    provider_factory=provider_factory,
                    stop_event=stop_event,
                    total_state=total_state,
                    total_lock=total_lock,
                    maximum_total_bytes=int(
                        plan["limits"]["maximum_download_bytes"]
                    ),
                    started=started,
                    clock=clock,
                ),
            ]
            results = [future.result() for future in futures]
        range_calls = sum(result.calls for result in results)
        records = [record for result in results for record in result.records]
        failures = [
            {
                "worker": index,
                "failure_type": result.failure_type,
                "failed_request_id": result.failed_request_id,
            }
            for index, result in enumerate(results)
            if result.failure_type is not None
        ]
        expected_ids = {str(item["request_id"]) for item in plan["requests"]}
        observed_ids = {str(item["request_id"]) for item in records}
        if (
            failures
            or len(records) != 287
            or range_calls != 287
            or observed_ids != expected_ids
        ):
            raise IntegrityError("bounded-2025 acquisition did not complete exactly")
        state = "SUCCESS_INACTIVE_UNREGISTERED_STAGING"
        failure_type = None
    except Exception as exc:
        state = "FAILURE_INACTIVE_EVIDENCE_PRESERVED"
        failure_type = type(exc).__name__
    core = {
        "schema_version": "bounded_2025_acquisition_terminal/1.0.0",
        "state": state,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "implementation_commit": _git_head(root),
        "download_worker_count": 2,
        "maximum_concurrent_ohlcv_1s": 1,
        "automatic_retries": 0,
        "provider_call_counts": {"get_cost": cost_calls, "get_range": range_calls},
        "completed_records": sorted(records, key=lambda item: item["request_id"]),
        "worker_failures": failures,
        "failure_type": failure_type,
        "canonical_registration": False,
        "dbn_rows_decoded": 0,
        "holdout_or_forward_access": False,
        "publication": False,
        "activation": False,
    }
    terminal = {**core, "terminal_id": sha256_json(core)}
    with (attempt / "terminal.json").open("xb") as stream:
        stream.write(canonical_bytes(terminal) + b"\n")
    if state != "SUCCESS_INACTIVE_UNREGISTERED_STAGING":
        raise IntegrityError("bounded-2025 acquisition failed closed")
    return terminal
