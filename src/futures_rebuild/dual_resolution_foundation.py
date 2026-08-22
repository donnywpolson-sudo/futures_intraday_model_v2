"""Resumable, foundation-only dual-resolution certification orchestration.

This module has no feature, label, model, strategy, prediction, WFA, broker,
order, or trading surface.  It is deliberately bounded to GLBX.MDP3 source
custody through 2024 and fails closed on the sealed 2025/2026 row domain.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .foundation_operation_firewall import reject_retired_dual_resolution_operation


RUN_SCHEMA = "dual_resolution_tier01_foundation_run/1.0.0"
MAX_ELAPSED_HOURS = 24
FINALIZATION_RESERVE_MINUTES = 60
POLL_INTERVAL_SECONDS = 300
MAX_TRANSIENT_RETRIES = 2
MAX_UNAPPROVED_PROVIDER_COST_USD = 0

FULL_SIZE_MARKETS = ("ES", "CL", "ZN", "6E")
MICRO_MARKETS = ("MES", "MCL", "MGC", "M6E")
ALL_MARKETS = FULL_SIZE_MARKETS + MICRO_MARKETS
SCHEMAS = ("ohlcv-1s", "ohlcv-1m", "definition", "statistics", "status")
SCHEMA_DIRECTORIES = {
    "ohlcv-1s": "ohlcv_1s",
    "ohlcv-1m": "ohlcv_1m",
    "definition": "definition",
    "statistics": "statistics",
    "status": "status",
}
MICRO_START_YEAR = {"MES": 2019, "MCL": 2021, "MGC": 2018, "M6E": 2018}
EXPECTED_HEAD = "0b5714828505b80ccf6ab3641c538492c0dc9cd1"
EXPECTED_BASELINES = {
    "reports/futures_data_foundation_remediation_and_recertification.zip":
        "b7a3a5a3af73861c7d8eb66bf5af37b52d5ce1e5ce1f55788c09a913bfe3e324",
    "reports/historical_data_capability_assessment/hdca_20260813T0023195310566Z_0b571482/historical_data_capability_and_alpha_investigability_assessment.zip":
        "d5abd912dcc1cc439057554d86f9210a9fbe39acf2f88edb069aaf01d39bf8d1",
    "reports/data_phase_closure_apply/dpca_20260813T2050210492551Z_0b571482/data_phase_closure_apply_and_verification.zip":
        "5e13199023ce32f459891b4f258fedb7c0e0f5960d8f96a9d493654efc2e12a2",
}
EXPECTED_CONFIG_HASHES = {
    "configs/micro_contract_universe_v1.json":
        "5557893f5525be19c06580907e4fd5d72d4076ca2889e07120e73d00688a2e18",
    "configs/core_databento_standard_l0_dependency_policy_v1.json":
        "a6eae838c54fb9ac2f190522316e47d4dfa3ab43b31fe658a4bb4da4d7bc08f8",
    "configs/data_surface_registry_v1.json":
        "04bce15f3ea06c0a2083a8d03b8cb5e7dcd368626a9698d53e3392a419059e55",
    "configs/data_phase_closed_v1.json":
        "a156514ca0f86b745600f2dc9c013b290f394e59073e64db969352081355bbf7",
}
ACTIVE_GUARD_PATHS = (
    "data/active/catalog.json",
    "data/active/catalogs/apex_micro.json",
    "configs/active_micro_alpha_research_ladder.json",
)
FOUNDATION_BASELINE_IDS = {
    "implementation": "dfri_20260812T2150075258586Z_0b571482",
    "recertification": "dfrc_20260812T2332571365058Z_0b571482",
    "capability_assessment": "hdca_20260813T0023195310566Z_0b571482",
    "data_phase_closure": "dpca_20260813T2050210492551Z_0b571482",
}


class FoundationStop(RuntimeError):
    """A governed stop with one exact code and preserved run state."""

    def __init__(self, stop_code: str, detail: str) -> None:
        super().__init__(f"{stop_code}: {detail}")
        self.stop_code = stop_code
        self.detail = detail


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_json(value: object) -> str:
    return sha256_bytes(canonical_json_bytes(value))


def sha256_file(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            block = stream.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    with temporary.open("wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    for attempt in range(MAX_TRANSIENT_RETRIES + 1):
        try:
            os.replace(temporary, path)
            break
        except PermissionError:
            if attempt >= MAX_TRANSIENT_RETRIES:
                raise
            # Windows readers and anti-malware scanners can briefly hold the
            # destination open. The governed retry ceiling remains two.
            time.sleep(0.05 * (attempt + 1))
    _fsync_directory(path.parent)


def atomic_write_json(path: Path, payload: object) -> None:
    atomic_write_bytes(path, canonical_json_bytes(payload) + b"\n")


def atomic_write_text(path: Path, payload: str) -> None:
    atomic_write_bytes(path, payload.encode("utf-8"))


def relative(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _decode_output(value: bytes) -> str:
    return value.decode("utf-8", errors="replace").replace("\r\n", "\n")


@dataclass(frozen=True)
class GitSnapshot:
    branch: str
    head: str
    upstream: str | None
    ahead: int | None
    behind: int | None
    staged_paths: tuple[str, ...]
    modified_tracked_paths: tuple[str, ...]
    untracked_paths: tuple[str, ...]
    porcelain_v2: str

    def as_dict(self) -> dict[str, object]:
        return {
            "branch": self.branch,
            "head": self.head,
            "upstream": self.upstream,
            "ahead": self.ahead,
            "behind": self.behind,
            "staged_paths": list(self.staged_paths),
            "modified_tracked_paths": list(self.modified_tracked_paths),
            "untracked_paths": list(self.untracked_paths),
            "porcelain_v2": self.porcelain_v2,
        }


class FoundationRun:
    def __init__(self, root: Path, run_id: str, *, operation_context: object = None) -> None:
        reject_retired_dual_resolution_operation(operation_context)
        self.root = root.resolve(strict=True)
        self.run_id = run_id
        self.report_root = (
            self.root / "reports" / "dual_resolution_tier01_foundation" / run_id
        )
        self.report_root.mkdir(parents=True, exist_ok=True)
        self.state_path = self.report_root / "STATE.json"
        self.started_monotonic = time.monotonic()
        self._ensure_ledger_files()

    def _ensure_ledger_files(self) -> None:
        headers = {
            "FILE_ACCESS_LEDGER.csv": (
                "observed_at_utc", "operation", "path", "sha256", "bytes",
                "row_decode_scope", "result",
            ),
            "PROCESS_LEDGER.csv": (
                "observed_at_utc", "process_id", "name", "command_line",
                "writer_candidate", "result",
            ),
            "SOURCE_FILE_LEDGER.csv": (
                "market", "schema", "year", "source_path", "source_bytes",
                "source_sha256", "declared_sha256", "hash_matches", "dataset",
                "dbn_schema", "request_start", "request_end", "request_id",
                "source_job_id", "acquisition_timestamp", "sdk_version",
                "dbn_version", "provider_generation", "protocol_epoch",
                "row_decode_scope", "verdict",
            ),
            "RELEASE_LEDGER.csv": (
                "market", "resolution", "release_kind", "release_id", "path",
                "sha256", "row_count", "source_hash_set_id", "state", "verdict",
            ),
            "CHANGED_FILE_LEDGER.csv": (
                "path", "starting_status", "starting_sha256", "task_action",
                "ending_sha256", "scope_owner",
            ),
            "BLOCKERS.csv": (
                "observed_at_utc", "stop_code", "phase", "detail", "next_action",
            ),
            "RETRY_LEDGER.csv": (
                "observed_at_utc", "phase", "operation", "attempt", "error_class",
                "result",
            ),
            "DISK_MONITOR.csv": (
                "observed_at_utc", "path", "volume_id", "free_bytes",
                "required_bytes", "result",
            ),
        }
        for name, columns in headers.items():
            path = self.report_root / name
            if not path.exists():
                atomic_write_text(path, ",".join(columns) + "\n")
        command_log = self.report_root / "COMMAND_LOG.txt"
        if not command_log.exists():
            atomic_write_text(command_log, "")

    def append_csv(self, name: str, values: Sequence[object]) -> None:
        path = self.report_root / name
        with path.open("a", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            writer.writerow(values)
            stream.flush()
            os.fsync(stream.fileno())

    def log(self, message: str) -> None:
        path = self.report_root / "COMMAND_LOG.txt"
        with path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(f"{utc_now()} {message}\n")
            stream.flush()
            os.fsync(stream.fileno())

    def heartbeat(self, phase: str, detail: str) -> None:
        atomic_write_json(
            self.report_root / "HEARTBEAT.json",
            {
                "schema_version": "dual_resolution_heartbeat/1.0.0",
                "run_id": self.run_id,
                "observed_at_utc": utc_now(),
                "phase": phase,
                "detail": detail,
                "process_id": os.getpid(),
            },
        )

    def write_state(self, payload: Mapping[str, object]) -> None:
        state = {
            "schema_version": RUN_SCHEMA,
            "run_id": self.run_id,
            "updated_at_utc": utc_now(),
            "limits": {
                "max_elapsed_hours": MAX_ELAPSED_HOURS,
                "finalization_reserve_minutes": FINALIZATION_RESERVE_MINUTES,
                "poll_interval_seconds": POLL_INTERVAL_SECONDS,
                "max_transient_retries": MAX_TRANSIENT_RETRIES,
                "max_unapproved_provider_cost_usd": MAX_UNAPPROVED_PROVIDER_COST_USD,
            },
            **dict(payload),
        }
        previous_1 = self.report_root / "STATE.previous.1.json"
        previous_2 = self.report_root / "STATE.previous.2.json"
        if previous_1.exists():
            atomic_write_bytes(previous_2, previous_1.read_bytes())
        if self.state_path.exists():
            atomic_write_bytes(previous_1, self.state_path.read_bytes())
        atomic_write_json(self.state_path, state)

    def stop(self, stop_code: str, phase: str, detail: str, next_action: str) -> None:
        self.append_csv(
            "BLOCKERS.csv", (utc_now(), stop_code, phase, detail, next_action)
        )
        self.write_state(
            {
                "state": "STOPPED_SAFE",
                "phase": phase,
                "stop_code": stop_code,
                "stop_detail": detail,
                "next_action": next_action,
            }
        )
        raise FoundationStop(stop_code, detail)

    def run_command(self, arguments: Sequence[str]) -> str:
        self.log("RUN " + subprocess.list2cmdline(list(arguments)))
        result = subprocess.run(
            list(arguments),
            cwd=self.root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        stdout = _decode_output(result.stdout)
        stderr = _decode_output(result.stderr)
        self.log(f"EXIT {result.returncode}")
        if result.returncode:
            raise FoundationStop(
                "LOCAL_COMMAND_FAILED",
                f"{arguments[0]} exited {result.returncode}: {stderr[-2000:]}",
            )
        return stdout

    def git_snapshot(self) -> GitSnapshot:
        branch = self.run_command(("git", "branch", "--show-current")).strip()
        head = self.run_command(("git", "rev-parse", "HEAD")).strip()
        porcelain = self.run_command(
            ("git", "status", "--porcelain=v2", "--branch", "--untracked-files=all")
        ).rstrip("\n")
        upstream: str | None = None
        ahead: int | None = None
        behind: int | None = None
        staged: list[str] = []
        modified: list[str] = []
        untracked: list[str] = []
        for line in porcelain.splitlines():
            if line.startswith("# branch.upstream "):
                upstream = line.split(" ", 2)[2]
            elif line.startswith("# branch.ab "):
                parts = line.split()
                ahead = int(parts[2].lstrip("+"))
                behind = int(parts[3].lstrip("-"))
            elif line.startswith("? "):
                untracked.append(line[2:])
            elif line.startswith(("1 ", "2 ")):
                parts = line.split(" ")
                status = parts[1]
                path = parts[-1]
                if status[0] != ".":
                    staged.append(path)
                if status[1] != ".":
                    modified.append(path)
        return GitSnapshot(
            branch=branch,
            head=head,
            upstream=upstream,
            ahead=ahead,
            behind=behind,
            staged_paths=tuple(sorted(staged)),
            modified_tracked_paths=tuple(sorted(modified)),
            untracked_paths=tuple(sorted(untracked)),
            porcelain_v2=porcelain,
        )

    def _record_access(
        self, path: Path, operation: str, *, row_decode_scope: str = "NONE"
    ) -> str:
        digest = sha256_file(path)
        self.append_csv(
            "FILE_ACCESS_LEDGER.csv",
            (
                utc_now(), operation, relative(self.root, path), digest,
                path.stat().st_size, row_decode_scope, "PASS",
            ),
        )
        return digest

    def _json(self, path: Path) -> dict[str, Any]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise FoundationStop("INVALID_JSON_AUTHORITY", f"not an object: {path}")
        return payload

    def _find_baseline(self, expected_relative: str) -> Path:
        direct = self.root / expected_relative
        if direct.is_file():
            return direct
        expected_name = Path(expected_relative).name
        matches = sorted((self.root / "reports").rglob(expected_name))
        if len(matches) != 1:
            raise FoundationStop(
                "ACCEPTED_BASELINE_MISSING",
                f"expected exactly one {expected_name}; found {len(matches)}",
            )
        return matches[0]

    def _verify_baselines(self) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        for configured_path, expected in EXPECTED_BASELINES.items():
            path = self._find_baseline(configured_path)
            observed = self._record_access(path, "HASH_ACCEPTED_BASELINE")
            result = {
                "path": relative(self.root, path),
                "expected_sha256": expected,
                "observed_sha256": observed,
                "bytes": path.stat().st_size,
                "matches": observed == expected,
            }
            results.append(result)
            if observed != expected:
                self.stop(
                    "ACCEPTED_FULLSIZE_FOUNDATION_HASH_CHANGED",
                    "PHASE_0",
                    f"accepted baseline hash changed: {configured_path}",
                    "Restore or adjudicate the accepted immutable baseline before resuming.",
                )
        for configured_path, expected in EXPECTED_CONFIG_HASHES.items():
            path = self.root / configured_path
            if not path.is_file():
                self.stop(
                    "ACCEPTED_BASELINE_MISSING", "PHASE_0", configured_path,
                    "Restore the accepted baseline config without overwriting other work.",
                )
            observed = self._record_access(path, "HASH_ACCEPTED_CONFIG")
            results.append(
                {
                    "path": configured_path,
                    "expected_sha256": expected,
                    "observed_sha256": observed,
                    "bytes": path.stat().st_size,
                    "matches": observed == expected,
                }
            )
            if observed != expected:
                self.stop(
                    "ACCEPTED_CONFIG_DRIFT", "PHASE_0", configured_path,
                    "Adjudicate whether the drift is a later accepted task or unexpected change.",
                )
        return results

    def _load_host_census(self) -> dict[str, Any]:
        path = self.report_root / "HOST_CENSUS.json"
        if not path.is_file():
            self.stop(
                "HOST_CENSUS_REQUIRED", "PHASE_0",
                "HOST_CENSUS.json is absent",
                "Run scripts/Get-DualResolutionFoundationHostCensus.ps1 read-only.",
            )
        payload = self._json(path)
        candidates = payload.get("repository_writer_candidates")
        if not isinstance(candidates, list):
            self.stop(
                "HOST_CENSUS_INVALID", "PHASE_0", "writer census is not a list",
                "Repeat the read-only host census.",
            )
        for item in candidates:
            if not isinstance(item, dict):
                continue
            self.append_csv(
                "PROCESS_LEDGER.csv",
                (
                    payload.get("observed_at_utc"), item.get("process_id"),
                    item.get("name"), item.get("command_line"), True, "CANDIDATE",
                ),
            )
        if candidates:
            self.stop(
                "ACTIVE_DATA_WRITER_DETECTED", "PHASE_0",
                f"{len(candidates)} repository writer candidate(s) are active",
                "Wait for the competing writer to finish, then repeat preflight.",
            )
        volume = payload.get("volume")
        if not isinstance(volume, dict) or volume.get("health_status") != "Healthy":
            self.stop(
                "DISK_SAFETY_UNESTABLISHED", "PHASE_0", "volume is not healthy",
                "Establish a healthy same-volume staging/publication target.",
            )
        return payload

    def _source_years(self, market: str) -> range:
        return range(2010, 2025) if market in FULL_SIZE_MARKETS else range(
            MICRO_START_YEAR[market], 2025
        )

    def _single_dbn(self, market: str, schema: str, year: int) -> Path:
        if year in {2025, 2026} or year > 2024:
            raise FoundationStop(
                "SEALED_MARKET_VALUE_ROW_ACCESS_REJECTED",
                f"sealed source selector rejected: {market}/{schema}/{year}",
            )
        directory = self.root / "data" / "dbn" / SCHEMA_DIRECTORIES[schema] / market / str(year)
        files = sorted(
            path for path in directory.glob("*.dbn*")
            if path.is_file() and not path.name.endswith(".json")
        )
        if len(files) != 1:
            raise FoundationStop(
                "SOURCE_SELECTION_AMBIGUOUS",
                f"expected one {market}/{schema}/{year} DBN; found {len(files)}",
            )
        return files[0]

    @staticmethod
    def _protocol_epoch(year: int) -> str:
        if year < 2017:
            return "LEGACY_FIX_FAST_PRE_2017"
        if year > 2017:
            return "MDP3_POST_2017"
        return "MIXED_PROTOCOL_INTERVAL"

    @staticmethod
    def _manifest_value(sidecar: Mapping[str, object], *names: str) -> object | None:
        for name in names:
            if name in sidecar:
                return sidecar[name]
        query = sidecar.get("exact_authorized_query")
        if isinstance(query, dict):
            for name in names:
                if name in query:
                    return query[name]
        return None

    def source_inventory(self) -> list[dict[str, object]]:
        import databento
        import databento_dbn

        rows: list[dict[str, object]] = []
        for market in ALL_MARKETS:
            for schema in SCHEMAS:
                for year in self._source_years(market):
                    path = self._single_dbn(market, schema, year)
                    sidecar_path = path.with_name(path.name + ".manifest.json")
                    if not sidecar_path.is_file():
                        raise FoundationStop(
                            "PROVIDER_MANIFEST_CONFLICT",
                            f"source sidecar missing: {relative(self.root, path)}",
                        )
                    observed_hash = self._record_access(
                        path, "HASH_SOURCE_BYTES", row_decode_scope="HEADER_ONLY_NO_ROWS"
                    )
                    sidecar_hash = self._record_access(
                        sidecar_path, "HASH_SOURCE_SIDECAR", row_decode_scope="NONE"
                    )
                    sidecar = self._json(sidecar_path)
                    declared = self._manifest_value(sidecar, "file_sha256", "sha256")
                    store = databento.DBNStore.from_file(path)
                    metadata = store.metadata
                    observed_schema = str(store.schema)
                    dataset = str(store.dataset)
                    if dataset != "GLBX.MDP3" or observed_schema != schema:
                        raise FoundationStop(
                            "SOURCE_HEADER_CONFLICT",
                            f"header mismatch: {relative(self.root, path)}",
                        )
                    if declared != observed_hash:
                        raise FoundationStop(
                            "SOURCE_HASH_CONFLICT",
                            f"sidecar hash mismatch: {relative(self.root, path)}",
                        )
                    full_size = market in FULL_SIZE_MARKETS
                    generation = (
                        "GLBX_MDP3_PRE_2026_08_NORMALIZATION"
                        if full_size else "UNKNOWN_OR_UNCLASSIFIED"
                    )
                    request_id = self._manifest_value(sidecar, "request_id")
                    source_job_id = self._manifest_value(sidecar, "job_id")
                    acquisition_timestamp = self._manifest_value(
                        sidecar, "downloaded_at", "acquisition_timestamp"
                    )
                    sdk_version = self._manifest_value(
                        sidecar, "api_client_version", "sdk_version"
                    )
                    row = {
                        "market": market,
                        "schema": schema,
                        "year": year,
                        "source_path": relative(self.root, path),
                        "source_bytes": path.stat().st_size,
                        "source_sha256": observed_hash,
                        "sidecar_path": relative(self.root, sidecar_path),
                        "sidecar_sha256": sidecar_hash,
                        "declared_sha256": declared,
                        "hash_matches": True,
                        "dataset": dataset,
                        "dbn_schema": observed_schema,
                        "request_start_ns": int(metadata.start),
                        "request_end_ns": int(metadata.end),
                        "request_id": request_id,
                        "source_job_id": source_job_id,
                        "acquisition_timestamp": acquisition_timestamp,
                        "sdk_version": sdk_version,
                        "dbn_version": str(getattr(databento_dbn, "__version__", "UNKNOWN")),
                        "provider_generation": generation,
                        "protocol_epoch": self._protocol_epoch(year),
                        "row_decode_scope": "HEADER_ONLY_NO_ROWS",
                        "verdict": (
                            "PROVENANCE_COMPLETE" if full_size
                            else "REACQUISITION_REQUIRED"
                        ),
                    }
                    rows.append(row)
                    self.append_csv(
                        "SOURCE_FILE_LEDGER.csv",
                        (
                            market, schema, year, row["source_path"], row["source_bytes"],
                            observed_hash, declared, True, dataset, observed_schema,
                            row["request_start_ns"], row["request_end_ns"], request_id,
                            source_job_id, acquisition_timestamp, sdk_version,
                            row["dbn_version"], generation, row["protocol_epoch"],
                            "HEADER_ONLY_NO_ROWS", row["verdict"],
                        ),
                    )
        return rows

    def _hash_guard_paths(self) -> dict[str, str]:
        result: dict[str, str] = {}
        for name in ACTIVE_GUARD_PATHS:
            path = self.root / name
            if not path.is_file():
                raise FoundationStop("ACTIVE_POINTER_MISSING", name)
            result[name] = self._record_access(path, "HASH_ACTIVE_GUARD")
        return result

    def existing_release_inventory(self) -> list[dict[str, object]]:
        import pyarrow.parquet as pq

        standard_catalog = self._json(self.root / "data" / "active" / "catalog.json")
        micro_catalog = self._json(
            self.root / "data" / "active" / "catalogs" / "apex_micro.json"
        )
        standard_entries = standard_catalog.get("entries")
        micro_entries = micro_catalog.get("entries")
        if not isinstance(standard_entries, list) or not isinstance(micro_entries, list):
            raise FoundationStop("ACTIVE_CATALOG_INVALID", "entry list is absent")
        results: list[dict[str, object]] = []
        for market in FULL_SIZE_MARKETS:
            for year in range(2010, 2025):
                matches = [
                    item for item in standard_entries
                    if isinstance(item, dict)
                    and item.get("market") == market and item.get("year") == year
                ]
                if len(matches) != 1:
                    raise FoundationStop(
                        "FULLSIZE_1M_AUTHORITY_AMBIGUOUS",
                        f"catalog cell {market}/{year} count={len(matches)}",
                    )
                entry = matches[0]
                path = self.root / str(entry["parquet_path"])
                digest = self._record_access(path, "HASH_ACCEPTED_FULLSIZE_1M")
                if digest != entry.get("parquet_sha256"):
                    raise FoundationStop(
                        "ACCEPTED_FULLSIZE_FOUNDATION_HASH_CHANGED",
                        relative(self.root, path),
                    )
                row_count = pq.ParquetFile(path).metadata.num_rows
                if row_count != entry.get("row_count"):
                    raise FoundationStop(
                        "ACCEPTED_FULLSIZE_FOUNDATION_ROWCOUNT_CHANGED",
                        relative(self.root, path),
                    )
                source_bindings = entry.get("source_bindings")
                release_id = None
                if isinstance(source_bindings, list) and len(source_bindings) == 1:
                    binding = source_bindings[0]
                    if isinstance(binding, dict):
                        release_id = binding.get("causal_release_id")
                results.append(
                    {
                        "market": market,
                        "year": year,
                        "resolution": "ohlcv-1m",
                        "classification": "CERTIFIED_EXISTING_RELEASE",
                        "release_id": release_id,
                        "path": relative(self.root, path),
                        "sha256": digest,
                        "row_count": row_count,
                        "registered": True,
                        "active": True,
                        "authorized_for_research": False,
                        "sealed": False,
                    }
                )
        for market in MICRO_MARKETS:
            for year in self._source_years(market):
                matches = [
                    item for item in micro_entries
                    if isinstance(item, dict)
                    and item.get("market") == market and item.get("year") == year
                ]
                if len(matches) != 1:
                    raise FoundationStop(
                        "MICRO_RELEASE_AUTHORITY_AMBIGUOUS",
                        f"catalog cell {market}/{year} count={len(matches)}",
                    )
                entry = matches[0]
                phase1b = entry.get("phase1b")
                phase2 = entry.get("phase2")
                if not isinstance(phase1b, dict) or not isinstance(phase2, dict):
                    raise FoundationStop("MICRO_RELEASE_AUTHORITY_AMBIGUOUS", f"{market}/{year}")
                for resolution, descriptor in (
                    ("ohlcv-1s", phase1b.get("ohlcv-1s")),
                    ("ohlcv-1m", phase2),
                ):
                    if not isinstance(descriptor, dict):
                        raise FoundationStop(
                            "MICRO_RELEASE_AUTHORITY_AMBIGUOUS", f"{market}/{year}/{resolution}"
                        )
                    path = self.root / str(descriptor["physical_path"])
                    digest = self._record_access(path, f"HASH_MICRO_{resolution.upper()}")
                    if digest != descriptor.get("sha256"):
                        raise FoundationStop("MICRO_RELEASE_HASH_CHANGED", relative(self.root, path))
                    row_count = pq.ParquetFile(path).metadata.num_rows
                    declared_rows = descriptor.get("row_count")
                    if declared_rows is not None and row_count != declared_rows:
                        raise FoundationStop("MICRO_RELEASE_ROWCOUNT_CHANGED", relative(self.root, path))
                    results.append(
                        {
                            "market": market,
                            "year": year,
                            "resolution": resolution,
                            "classification": "UNCERTIFIED_EXISTING_RELEASE",
                            "release_id": descriptor.get("release_id"),
                            "path": relative(self.root, path),
                            "sha256": digest,
                            "row_count": row_count,
                            "registered": True,
                            "active": False,
                            "authorized_for_research": False,
                            "sealed": False,
                        }
                    )
        return results

    def _environment(self, host: Mapping[str, object]) -> dict[str, object]:
        import databento
        import databento_dbn
        import pyarrow
        import zoneinfo

        volume = host.get("volume")
        core = {
            "python": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "databento_sdk": databento.__version__,
            "databento_dbn": getattr(databento_dbn, "__version__", "UNKNOWN"),
            "pyarrow": pyarrow.__version__,
            "timezone_database_source": (
                "zoneinfo.TZPATH=" + repr(tuple(zoneinfo.TZPATH))
                if zoneinfo.TZPATH else "WINDOWS_OR_PYTHON_BUNDLED_TZDATA"
            ),
            "operating_system": platform.platform(),
            "filesystem_volume": volume,
        }
        return {**core, "environment_id": sha256_json(core)}

    def _write_inventory_reports(
        self,
        releases: Sequence[Mapping[str, object]],
        sources: Sequence[Mapping[str, object]],
    ) -> None:
        columns = (
            "market", "year", "resolution", "classification", "release_id", "path",
            "sha256", "row_count", "registered", "active",
            "authorized_for_research", "sealed",
        )
        csv_path = self.report_root / "01_EXISTING_RELEASE_INVENTORY.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(releases)
            stream.flush()
            os.fsync(stream.fileno())
        atomic_write_json(
            self.report_root / "01_EXISTING_RELEASE_INVENTORY.json",
            {"schema_version": "existing_release_inventory/1.0.0", "entries": list(releases)},
        )
        full_1m = sum(
            int(row["row_count"]) for row in releases
            if row["market"] in FULL_SIZE_MARKETS and row["resolution"] == "ohlcv-1m"
        )
        micro_1m = sum(
            int(row["row_count"]) for row in releases
            if row["market"] in MICRO_MARKETS and row["resolution"] == "ohlcv-1m"
        )
        micro_1s = sum(
            int(row["row_count"]) for row in releases
            if row["market"] in MICRO_MARKETS and row["resolution"] == "ohlcv-1s"
        )
        atomic_write_text(
            self.report_root / "01_AUTHORITY_AND_LINEAGE.md",
            "# Authority and lineage\n\n"
            f"Run: `{self.run_id}`\n\n"
            f"- Accepted full-size causal one-minute rows: {full_1m:,}; reused without rewrite.\n"
            f"- Existing micro causal one-minute rows: {micro_1m:,}; certification pending known provider generation.\n"
            f"- Existing seekable micro causal-availability-aware one-second rows: {micro_1s:,}; certification pending known provider generation.\n"
            "- Full-size one-second state: source-only; a new immutable seekable causal release is required.\n"
            "- No folder timestamp, newest-path glob, or largest-path heuristic selected authority.\n"
            "- Active alpha-research authority remains false.\n",
        )
        provenance_counts: dict[str, int] = {}
        for row in sources:
            verdict = str(row["verdict"])
            provenance_counts[verdict] = provenance_counts.get(verdict, 0) + 1
        atomic_write_text(
            self.report_root / "02_PROVIDER_PROVENANCE.md",
            "# Provider provenance\n\n"
            f"Source partitions structurally verified through 2024: {len(sources):,}.\n\n"
            f"- Full-size known-generation partitions: {provenance_counts.get('PROVENANCE_COMPLETE', 0):,}.\n"
            f"- Micro partitions requiring a hash-bound successor acquisition: {provenance_counts.get('REACQUISITION_REQUIRED', 0):,}.\n"
            "- Legacy micro custody hashes, headers, sidecars, and existing Parquet lineage are preserved and remain useful comparison evidence.\n"
            "- They are not admitted as a certified research foundation while provider normalization remains `UNKNOWN_OR_UNCLASSIFIED`.\n"
            "- The controlled fallback is bounded to MES/MCL/MGC/M6E, the five approved schemas, end 2025-01-01 exclusive, and exact incremental cost USD 0.\n",
        )
        provenance_columns = (
            "market", "schema", "year", "source_path", "source_sha256",
            "request_id", "source_job_id", "acquisition_timestamp", "sdk_version",
            "dbn_version", "provider_generation", "protocol_epoch", "verdict",
        )
        with (self.report_root / "02_provider_provenance_ledger.csv").open(
            "w", encoding="utf-8", newline=""
        ) as stream:
            writer = csv.DictWriter(stream, fieldnames=provenance_columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(sources)
        lifecycle_rows = []
        starts = {**{m: 2010 for m in FULL_SIZE_MARKETS}, **MICRO_START_YEAR}
        for market in ALL_MARKETS:
            lifecycle_rows.append(
                {
                    "market": market,
                    "first_requested_source_year": starts[market],
                    "first_production_session": "LOCAL_SOURCE_BOUNDARY_REQUIRES_REFERENCE_CERTIFICATION",
                    "first_trade_date": "NOT_INFERRED_FROM_PRELAUNCH_REFERENCE_METADATA",
                    "first_observed_mapping_date": "TO_BE_EXHAUSTIVELY_CERTIFIED",
                    "first_observed_market_value_date": "TO_BE_EXHAUSTIVELY_CERTIFIED",
                }
            )
        with (self.report_root / "02_lifecycle_ledger.csv").open(
            "w", encoding="utf-8", newline=""
        ) as stream:
            writer = csv.DictWriter(stream, fieldnames=tuple(lifecycle_rows[0]))
            writer.writeheader()
            writer.writerows(lifecycle_rows)

    def _write_policy_and_architecture_reports(self, sources: Sequence[Mapping[str, object]]) -> None:
        observation = self.root / "configs" / "dual_resolution_observation_policy_v1.json"
        alignment = self.root / "configs" / "dual_resolution_alignment_policy_v1.json"
        schema = self.root / "configs" / "dual_resolution_release_schema_v1.json"
        observation_hash = self._record_access(observation, "HASH_NEW_OBSERVATION_POLICY")
        alignment_hash = self._record_access(alignment, "HASH_NEW_ALIGNMENT_POLICY")
        schema_hash = self._record_access(schema, "HASH_NEW_RELEASE_SCHEMA")
        vectors = {
            "schema_version": "dual_resolution_observation_vectors/1.0.0",
            "policy_sha256": observation_hash,
            "vectors": [
                {
                    "resolution": "ohlcv-1s", "ts_event_ns": 0,
                    "interval_end_ns": 1_000_000_000, "available_at_ns": 6_000_000_000,
                    "decision_at_ns": 5_999_999_999, "usable": False,
                },
                {
                    "resolution": "ohlcv-1s", "ts_event_ns": 0,
                    "interval_end_ns": 1_000_000_000, "available_at_ns": 6_000_000_000,
                    "decision_at_ns": 6_000_000_000, "usable": True,
                },
                {
                    "resolution": "ohlcv-1m", "ts_event_ns": 0,
                    "interval_end_ns": 60_000_000_000, "available_at_ns": 65_000_000_000,
                    "decision_at_ns": 64_999_999_999, "usable": False,
                },
                {
                    "resolution": "ohlcv-1m", "ts_event_ns": 0,
                    "interval_end_ns": 60_000_000_000, "available_at_ns": 65_000_000_000,
                    "decision_at_ns": 65_000_000_000, "usable": True,
                },
            ],
        }
        atomic_write_json(self.report_root / "03_dual_resolution_test_vectors.json", vectors)
        atomic_write_json(
            self.report_root / "03_dual_resolution_observation_policy_v1.json",
            self._json(observation),
        )
        atomic_write_text(
            self.report_root / "03_DUAL_RESOLUTION_OBSERVATION_POLICY.md",
            "# Dual-resolution observation policy\n\n"
            f"Policy SHA-256: `{observation_hash}`.\n\n"
            "The accepted one-minute rule remains exactly bar close plus five seconds: `ts_event + 65 seconds`. "
            "The one-second successor applies the same conservative invariant at its native interval: `ts_event + 1 second + 5 seconds`, totaling 6 seconds. "
            "A row is usable only when `available_at <= decision time`. Equal source timestamps never override independent availability.\n\n"
            "Absent reported-trading seconds or minutes remain absent. No grid, zero-volume insertion, fill, interpolation, future definition, or historical price rounding is permitted.\n",
        )
        total_source_bytes = sum(
            int(row["source_bytes"]) for row in sources
            if row["schema"] == "ohlcv-1s" and row["market"] in FULL_SIZE_MARKETS
        )
        projected_new = total_source_bytes * 5
        projected_temp = projected_new
        reserve = 30 * 1024**3
        required = int(1.5 * projected_new) + projected_temp + reserve
        projection_rows = [
            {
                "component": "fullsize_causal_1s_release",
                "basis_bytes": total_source_bytes,
                "multiplier": 5,
                "projected_bytes": projected_new,
            },
            {
                "component": "maximum_temporary_working_set",
                "basis_bytes": projected_new,
                "multiplier": 1,
                "projected_bytes": projected_temp,
            },
            {
                "component": "required_30_gib_reserve",
                "basis_bytes": reserve,
                "multiplier": 1,
                "projected_bytes": reserve,
            },
            {
                "component": "disk_gate_required_total",
                "basis_bytes": required,
                "multiplier": 1,
                "projected_bytes": required,
            },
        ]
        with (self.report_root / "04_projected_storage.csv").open(
            "w", encoding="utf-8", newline=""
        ) as stream:
            writer = csv.DictWriter(stream, fieldnames=tuple(projection_rows[0]))
            writer.writeheader()
            writer.writerows(projection_rows)
        partition_rows = [
            {
                "market": market,
                "year": year,
                "resolution": "ohlcv-1s",
                "format": "PARQUET_ZSTD",
                "raw_stage_mode": "SOURCE_DIRECT_CERTIFIED",
                "publication_state": "INACTIVE_FOUNDATION_RELEASE",
            }
            for market in FULL_SIZE_MARKETS for year in range(2010, 2025)
        ]
        with (self.report_root / "04_release_partition_plan.csv").open(
            "w", encoding="utf-8", newline=""
        ) as stream:
            writer = csv.DictWriter(stream, fieldnames=tuple(partition_rows[0]))
            writer.writeheader()
            writer.writerows(partition_rows)
        atomic_write_json(self.report_root / "04_release_schema.json", self._json(schema))
        atomic_write_text(
            self.report_root / "04_RELEASE_ARCHITECTURE.md",
            "# Immutable dual-resolution release architecture\n\n"
            f"Release schema SHA-256: `{schema_hash}`. Alignment policy SHA-256: `{alignment_hash}`.\n\n"
            "Full-size one-minute files are reused from the accepted immutable release. Existing micro one-minute and seekable one-second Parquet remain immutable candidate releases until known-generation successor sources are certified. "
            "Full-size one-second output uses annual ZSTD Parquet partitions staged and published atomically on C: into the ignored immutable vault. "
            "The original DBNs are certified directly as the raw stage; they are never copied or overwritten.\n",
        )
        atomic_write_json(
            self.report_root / "04_STORAGE_GATE.json",
            {
                "source_1s_compressed_bytes": total_source_bytes,
                "projected_new_release_bytes": projected_new,
                "maximum_temporary_working_set_bytes": projected_temp,
                "reserve_bytes": reserve,
                "required_free_bytes": required,
            },
        )

    def preflight(self) -> dict[str, object]:
        self.heartbeat("PHASE_0", "starting")
        starting = self.git_snapshot()
        atomic_write_json(self.report_root / "00_STARTING_GIT_SNAPSHOT.json", starting.as_dict())
        atomic_write_text(
            self.report_root / "PRETASK_TRACKED_DIFF.patch",
            self.run_command(("git", "diff", "--binary", "--")),
        )
        if starting.branch != "main":
            self.stop(
                "REPOSITORY_BRANCH_CHANGED", "PHASE_0", starting.branch,
                "Return to the intended branch without overwriting work, or authorize the new branch.",
            )
        if starting.head != EXPECTED_HEAD:
            self.stop(
                "REPOSITORY_HEAD_CHANGED", "PHASE_0", starting.head,
                "Adjudicate the new HEAD before any historical materialization.",
            )
        if starting.staged_paths:
            self.stop(
                "BLOCKED_BY_PREEXISTING_WORKTREE_STATE", "PHASE_0",
                f"staged paths exist: {len(starting.staged_paths)}",
                "Resolve the staged state without reset, stash, or cleanup, then resume.",
            )
        host = self._load_host_census()
        environment = self._environment(host)
        atomic_write_json(self.report_root / "ENVIRONMENT.json", environment)
        baseline_results = self._verify_baselines()
        guards_before = self._hash_guard_paths()
        sources = self.source_inventory()
        releases = self.existing_release_inventory()
        self._write_inventory_reports(releases, sources)
        self._write_policy_and_architecture_reports(sources)
        storage = self._json(self.report_root / "04_STORAGE_GATE.json")
        volume = host["volume"]
        assert isinstance(volume, dict)
        free_bytes = int(volume["free_bytes"])
        required_bytes = int(storage["required_free_bytes"])
        self.append_csv(
            "DISK_MONITOR.csv",
            (
                utc_now(), str(self.root), volume.get("unique_id"), free_bytes,
                required_bytes, "PASS" if free_bytes >= required_bytes else "FAIL",
            ),
        )
        if free_bytes < required_bytes:
            self.stop(
                "DISK_SAFETY_INSUFFICIENT", "PHASE_0",
                f"free={free_bytes}, required={required_bytes}",
                "Provide sufficient same-volume free space without deleting accepted artifacts.",
            )
        ending = self.git_snapshot()
        guards_after = self._hash_guard_paths()
        if ending.head != starting.head:
            self.stop(
                "REPOSITORY_HEAD_CHANGED_DURING_PREFLIGHT", "PHASE_0",
                f"{starting.head} -> {ending.head}",
                "Adjudicate the unexpected HEAD change.",
            )
        if guards_after != guards_before:
            self.stop(
                "ACTIVE_POINTER_CHANGED_DURING_PREFLIGHT", "PHASE_0",
                "one or more active catalog/pointer hashes changed",
                "Identify the competing writer and repeat preflight from stable authority.",
            )
        full_rows = sum(
            int(item["row_count"]) for item in releases
            if item["market"] in FULL_SIZE_MARKETS and item["resolution"] == "ohlcv-1m"
        )
        micro_1s_rows = sum(
            int(item["row_count"]) for item in releases
            if item["market"] in MICRO_MARKETS and item["resolution"] == "ohlcv-1s"
        )
        micro_1m_rows = sum(
            int(item["row_count"]) for item in releases
            if item["market"] in MICRO_MARKETS and item["resolution"] == "ohlcv-1m"
        )
        preflight = {
            "schema_version": "dual_resolution_preflight/1.0.0",
            "run_id": self.run_id,
            "repository_root": str(self.root),
            "branch": starting.branch,
            "head": starting.head,
            "expected_head_matches": starting.head == EXPECTED_HEAD,
            "upstream": starting.upstream,
            "ahead": starting.ahead,
            "behind": starting.behind,
            "starting_staged_path_count": len(starting.staged_paths),
            "starting_modified_tracked_path_count": len(starting.modified_tracked_paths),
            "starting_untracked_path_count": len(starting.untracked_paths),
            "baseline_results": baseline_results,
            "source_partition_count": len(sources),
            "source_hash_failures": 0,
            "fullsize_accepted_1m_rows": full_rows,
            "micro_existing_1s_rows": micro_1s_rows,
            "micro_existing_1m_rows": micro_1m_rows,
            "writer_candidate_count": 0,
            "active_guard_hashes": guards_before,
            "environment_id": environment["environment_id"],
            "volume_id": volume.get("unique_id"),
            "free_bytes": free_bytes,
            "required_free_bytes": required_bytes,
            "disk_gate": "PASS",
            "sealed_2025_market_value_rows_decoded": 0,
            "sealed_2026_market_value_rows_decoded": 0,
            "trades_rows_decoded": 0,
            "phase_0_verdict": "PASS",
            "phase_1_verdict": "PASS_WITH_MICRO_PROVENANCE_GATE",
            "phase_2_verdict": "MICRO_REACQUISITION_REQUIRED",
            "alpha_research_authorized": False,
        }
        atomic_write_json(self.report_root / "00_preflight.json", preflight)
        atomic_write_text(
            self.report_root / "00_PREFLIGHT.md",
            "# Preflight\n\n"
            f"Run ID: `{self.run_id}`  \n"
            f"Repository: `{self.root}`  \n"
            f"Branch / HEAD: `{starting.branch}` / `{starting.head}`  \n"
            f"Upstream divergence: ahead {starting.ahead}, behind {starting.behind}  \n\n"
            f"PASS: {len(sources):,} required source partitions through 2024 matched their sidecars and DBN headers without row iteration. "
            f"PASS: the accepted full-size one-minute denominator is {full_rows:,} rows and every registered annual Parquet hash matched. "
            f"PASS: no competing repository data writer was found. PASS: C: has {free_bytes:,} free bytes against a conservative {required_bytes:,}-byte publication gate.\n\n"
            f"Worktree preservation: {len(starting.modified_tracked_paths)} modified tracked paths, {len(starting.untracked_paths)} non-ignored untracked paths, and zero staged paths were recorded. "
            "No pre-existing tracked file will be modified. Task changes are additive only.\n\n"
            "The legacy micro source set remains non-admissible for final certification because its provider normalization generation is unclassified. "
            "The controlled USD 0 successor reacquisition is therefore the next gated phase.\n",
        )
        self.write_state(
            {
                "state": "PHASES_0_TO_4_COMPLETE",
                "phase": "PHASE_4",
                "completed_phases": [0, 1, 2, 3, 4],
                "revalidated_outputs": {
                    "preflight_sha256": sha256_file(self.report_root / "00_preflight.json"),
                    "release_inventory_sha256": sha256_file(
                        self.report_root / "01_EXISTING_RELEASE_INVENTORY.json"
                    ),
                    "observation_policy_sha256": sha256_file(
                        self.root / "configs" / "dual_resolution_observation_policy_v1.json"
                    ),
                },
                "next_phase": "CONTROLLED_MICRO_REACQUISITION",
                "provider_cost_ceiling_usd": 0,
                "alpha_research_authorized": False,
            }
        )
        self.heartbeat("PHASE_4", "complete")
        return preflight


def discover_repository(start: Path, *, operation_context: object = None) -> Path:
    reject_retired_dual_resolution_operation(operation_context)
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=start,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode:
        raise FoundationStop("REPOSITORY_DISCOVERY_FAILED", _decode_output(result.stderr))
    root = Path(_decode_output(result.stdout).strip()).resolve(strict=True)
    if not (root / "AGENTS.md").is_file():
        raise FoundationStop("REPOSITORY_INSTRUCTIONS_MISSING", str(root))
    return root
