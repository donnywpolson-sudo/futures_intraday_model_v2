"""Provider-free consolidation of the two canonical 2026 L0 DBN shards."""

from __future__ import annotations

import gc
import json
import os
import shutil
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Final

import databento
import databento_dbn
import zstandard

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    canonical_authority_file_bytes,
    io_path,
    read_canonical_authority,
    sha256_bytes,
    sha256_file,
    sha256_json,
    write_new_canonical_authority,
)
from .causal_source_closure import validate_source_closure
from .dbn_catalog import RECEIVE_TIME_COVERAGE_SCHEMAS, SUPPORTED_DATABENTO_VERSION
from .errors import ContractError, IntegrityError
from .foundation_operation_firewall import issue_current_source_closure_context
from .l0_cutoff_alignment_acceptance import _inventory_rows, _inventory_summary
from .l0_cutoff_alignment_acquisition import STANDARD_ROOTS
from .live_cockpit.single_instance import SingleInstance
from .runtime_environment import require_locked_repository_environment


ACTIVE_POINTER: Final = Path("configs/active_dbn_congruence_release_v1.json")
ACTIVE_SOURCE_CONTRACT: Final = Path("configs/source_contract.json")
PREDECESSOR_SOURCE_CONTRACT: Final = Path("configs/source_contract_v5.json")
SUCCESSOR_SOURCE_CONTRACT: Final = Path("configs/source_contract_v6.json")
ACTIVE_ALPHA_POINTER: Final = Path("configs/active_alpha_research_ladder.json")
DBN_ROOT: Final = Path("data/dbn")
PLAN_ROOT: Final = Path("state/l0_2026_dbn_consolidation_v1")
REPORT_ROOT: Final = Path("reports/l0_2026_dbn_consolidation_v1")
STAGE_ROOT: Final = Path("state/data_publication_staging/l0_2026_dbn_consolidation_v1")
ARCHIVE_PARENT: Final = Path("state/dbn_congruence_release_archive")
LOCK_PATH: Final = Path("state/locks/l0_2026_dbn_consolidation_v1.lock")
MERGE_OPERATION: Final = "CONSOLIDATE_L0_2026_DBN_CUSTODY_V1_ONCE"
PUBLICATION_OPERATION: Final = "PUBLISH_L0_2026_DBN_CONSOLIDATION_V1_ONCE"
FIRST_NAME: Final = "2026-01-01_2026-07-14.dbn.zst"
SECOND_NAME: Final = "2026-07-14_2026-09-01.dbn.zst"
MERGED_NAME: Final = "2026-01-01_2026-09-01.dbn.zst"
START: Final = "2026-01-01T00:00:00Z"
END: Final = "2026-09-01T00:00:00Z"
EXPECTED_UNITS: Final = 406
EXPECTED_SOURCE_FILES: Final = 1_624
EXPECTED_SUCCESSOR_FILES: Final = 11_474
MIN_FREE_BYTES: Final = 5_000_000_000


def _json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(io_path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise IntegrityError(f"{label} is not readable JSON") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{label} must be an object")
    return value


def _relative(root: Path, path: Path) -> str:
    try:
        return path.resolve(strict=False).relative_to(root).as_posix()
    except ValueError as exc:
        raise ContractError(f"path escapes repository: {path}") from exc


def _implementation_sha(root: Path) -> str:
    return sha256_file(root / Path(__file__).resolve().relative_to(root))


def _exists(path: Path) -> bool:
    return io_path(path).exists()


def _stat(path: Path) -> os.stat_result:
    return io_path(path).stat()


def _write_exact_or_new(path: Path, payload: Mapping[str, Any]) -> bool:
    expected = canonical_authority_file_bytes(payload)
    if path.exists():
        if path.read_bytes() != expected:
            raise IntegrityError(f"existing authority bytes differ: {path}")
        return False
    write_new_canonical_authority(path, payload)
    return True


def _atomic_replace(path: Path, data: bytes, suffix: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{suffix}.tmp")
    if temporary.exists():
        raise IntegrityError(f"atomic replacement temporary exists: {temporary}")
    with io_path(temporary).open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(io_path(temporary), io_path(path))


def _stable_union(*sources: Sequence[str]) -> list[str]:
    result: list[str] = []
    for source in sources:
        for value in source:
            if value not in result:
                result.append(value)
    return result


def _merged_mappings(
    *sources: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[SimpleNamespace]:
    merged: dict[str, list[dict[str, Any]]] = {}
    for source in sources:
        for raw_symbol, intervals in source.items():
            rows = merged.setdefault(raw_symbol, [])
            for interval in intervals:
                row = dict(interval)
                if row["start_date"] >= row["end_date"]:
                    raise IntegrityError("symbology mapping interval is empty")
                if rows and row["start_date"] < rows[-1]["end_date"]:
                    prior = rows[-1]
                    prior_identity = {
                        key: value
                        for key, value in prior.items()
                        if key not in {"start_date", "end_date"}
                    }
                    row_identity = {
                        key: value
                        for key, value in row.items()
                        if key not in {"start_date", "end_date"}
                    }
                    if prior_identity != row_identity:
                        raise IntegrityError("symbology mapping intervals overlap")
                    prior["end_date"] = max(prior["end_date"], row["end_date"])
                    continue
                rows.append(row)
    return [
        SimpleNamespace(
            raw_symbol=raw_symbol,
            intervals=[SimpleNamespace(**interval) for interval in intervals],
        )
        for raw_symbol, intervals in merged.items()
    ]


def _metadata(path: Path) -> dict[str, Any]:
    store = databento.DBNStore.from_file(path)
    try:
        metadata = store.metadata
        return {
            "dataset": store.dataset,
            "start": int(metadata.start),
            "end": int(metadata.end) if metadata.end is not None else None,
            "stype_in": metadata.stype_in,
            "stype_out": metadata.stype_out,
            "schema": metadata.schema,
            "symbols": list(metadata.symbols),
            "partial": list(metadata.partial),
            "not_found": list(metadata.not_found),
            "mappings": dict(metadata.mappings),
            "limit": metadata.limit,
            "ts_out": metadata.ts_out,
            "version": int(metadata.version),
        }
    finally:
        del store
        gc.collect()


def _require_compatible(
    *sources: Mapping[str, Any],
    expected_start_ns: int = 1_767_225_600_000_000_000,
    expected_end_ns: int = 1_788_220_800_000_000_000,
    require_contiguous: bool = True,
) -> None:
    if not sources:
        raise IntegrityError("DBN consolidation requires at least one source")
    first = sources[0]
    prior_end: int | None = None
    for source in sources:
        for key in ("dataset", "stype_in", "stype_out", "schema", "symbols", "ts_out"):
            if source[key] != first[key]:
                raise IntegrityError(f"source DBN metadata differs: {key}")
        if source["limit"] is not None:
            raise IntegrityError("limited source DBN cannot be consolidated")
        if source["start"] >= source["end"]:
            raise IntegrityError("source DBN interval is empty")
        if source["start"] < expected_start_ns or source["end"] > expected_end_ns:
            raise IntegrityError("source DBN interval exceeds requested coverage")
        if prior_end is not None and source["start"] < prior_end:
            raise IntegrityError("source DBN metadata intervals overlap")
        if require_contiguous and prior_end is not None and source["start"] != prior_end:
            raise IntegrityError("source DBN metadata intervals are not contiguous")
        prior_end = source["end"]
    if require_contiguous and (
        sources[0]["start"] != expected_start_ns or sources[-1]["end"] != expected_end_ns
    ):
        raise IntegrityError("source DBN coverage differs")


def _timestamp(record: Any, schema: Any) -> int:
    field = "ts_recv" if str(schema) in RECEIVE_TIME_COVERAGE_SCHEMAS else "ts_event"
    try:
        return int(getattr(record, field))
    except (AttributeError, TypeError, ValueError) as exc:
        raise IntegrityError(f"record lacks valid {field}") from exc


def _stream_records(
    path: Path, writer: Any | None = None, sequence_digest: Any | None = None,
) -> dict[str, Any]:
    store = databento.DBNStore.from_file(path)
    digest = sha256()
    count = 0
    first: int | None = None
    last: int | None = None
    first_record_sha256: str | None = None
    last_record_sha256: str | None = None
    try:
        schema = store.schema
        for record in store:
            encoded = bytes(record)
            timestamp = _timestamp(record, schema)
            if last is not None and timestamp < last:
                raise IntegrityError(f"record timestamps are not nondecreasing: {path}")
            first = timestamp if first is None else first
            last = timestamp
            digest.update(encoded)
            if sequence_digest is not None:
                sequence_digest.update(encoded)
            if writer is not None:
                writer.write(encoded)
            encoded_sha256 = sha256(encoded).hexdigest()
            first_record_sha256 = (
                encoded_sha256 if first_record_sha256 is None else first_record_sha256
            )
            last_record_sha256 = encoded_sha256
            count += 1
    finally:
        del store
        gc.collect()
    if count <= 0 or first is None or last is None:
        raise IntegrityError(f"source DBN contains no records: {path}")
    return {
        "record_count": count,
        "record_sequence_sha256": digest.hexdigest(),
        "first_timestamp_ns": first,
        "last_timestamp_ns": last,
        "first_record_sha256": first_record_sha256,
        "last_record_sha256": last_record_sha256,
    }


def _require_record_boundary(prior: Mapping[str, Any], current: Mapping[str, Any]) -> None:
    if prior["last_timestamp_ns"] > current["first_timestamp_ns"]:
        raise IntegrityError("source shard boundary is out of order")
    if (
        prior["last_timestamp_ns"] == current["first_timestamp_ns"]
        and prior["last_record_sha256"] == current["first_record_sha256"]
    ):
        raise IntegrityError("source shard boundary contains a duplicate record")


def merge_dbn_sources(
    sources: Sequence[Path],
    destination: Path,
    *,
    expected_start_ns: int,
    expected_end_ns: int,
    require_contiguous: bool,
    maximum_output_bytes: int | None = None,
) -> dict[str, Any]:
    """Merge an ordered DBN sequence through one decoded DBN v3 writer."""

    paths = tuple(sources)
    if len(paths) != len({path.resolve(strict=True) for path in paths}):
        raise IntegrityError("DBN consolidation source path is duplicated")
    metadata_rows = [_metadata(path) for path in paths]
    _require_compatible(
        *metadata_rows,
        expected_start_ns=expected_start_ns,
        expected_end_ns=expected_end_ns,
        require_contiguous=require_contiguous,
    )
    first = metadata_rows[0]
    metadata = databento_dbn.Metadata(
        dataset=first["dataset"],
        start=expected_start_ns,
        end=expected_end_ns,
        stype_in=first["stype_in"],
        stype_out=first["stype_out"],
        schema=first["schema"],
        symbols=first["symbols"],
        partial=_stable_union(*(row["partial"] for row in metadata_rows)),
        not_found=_stable_union(*(row["not_found"] for row in metadata_rows)),
        mappings=_merged_mappings(*(row["mappings"] for row in metadata_rows)),
        limit=None,
        ts_out=first["ts_out"],
        version=3,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".partial")
    if destination.exists() or partial.exists():
        raise IntegrityError(f"candidate output already exists: {destination}")
    results: list[dict[str, Any]] = []
    combined_digest = sha256()
    with io_path(partial).open("xb") as raw:
        with zstandard.ZstdCompressor(write_checksum=True).stream_writer(
            raw, closefd=False
        ) as writer:
            writer.write(metadata.encode())
            for path in paths:
                result = _stream_records(path, writer, combined_digest)
                if results:
                    _require_record_boundary(results[-1], result)
                results.append(result)
                if maximum_output_bytes is not None:
                    writer.flush(zstandard.FLUSH_BLOCK)
                    if raw.tell() > maximum_output_bytes:
                        raise IntegrityError("candidate output exceeds its byte ceiling")
        raw.flush()
        os.fsync(raw.fileno())
    os.replace(io_path(partial), io_path(destination))
    if maximum_output_bytes is not None and destination.stat().st_size > maximum_output_bytes:
        raise IntegrityError("candidate output exceeds its byte ceiling")
    combined = combined_digest.hexdigest()
    verified = _stream_records(destination)
    if (
        verified["record_count"]
        != sum(result["record_count"] for result in results)
        or verified["record_sequence_sha256"] != combined
    ):
        raise IntegrityError("merged record sequence differs from sources")
    output = _metadata(destination)
    if (
        output["version"] != 3
        or output["start"] != expected_start_ns
        or output["end"] != expected_end_ns
    ):
        raise IntegrityError("merged DBN metadata differs")
    return {
        "sources": results,
        "merged": verified,
        "combined_record_sequence_sha256": combined,
        "source_metadata": metadata_rows,
        "merged_metadata": output,
        "source_versions": [row["version"] for row in metadata_rows],
        "output_version": 3,
        "compatibility_checks": {
            "metadata_identity_equal": True,
            "source_intervals_ordered_non_overlapping": True,
            "source_intervals_contiguous": require_contiguous,
            "record_values_and_order_preserved": True,
        },
    }


def build_plan(root: Path) -> dict[str, Any]:
    """Bind the exact active v5 inventory and 406 source pairs without row reads."""

    root = root.resolve(strict=True)
    require_locked_repository_environment(root)
    if databento.__version__ != SUPPORTED_DATABENTO_VERSION:
        raise IntegrityError("Databento decoder version differs")
    contract_path = root / ACTIVE_SOURCE_CONTRACT
    contract = _json(contract_path, "active source contract")
    if contract_path.read_bytes() != (root / PREDECESSOR_SOURCE_CONTRACT).read_bytes():
        raise IntegrityError("active source contract is not exact v5")
    inventory_path = root / str(contract["complete_inventory"]["path"])
    inventory = _json(inventory_path, "complete canonical inventory")
    entries = inventory.get("entries")
    if not isinstance(entries, list) or len(entries) != 12_286:
        raise IntegrityError("active v5 inventory does not contain 12,286 files")
    by_path = {str(item["path"]): dict(item) for item in entries}
    units: list[dict[str, Any]] = []
    for first_path in sorted(path for path in by_path if path.endswith("/2026/" + FIRST_NAME)):
        base = first_path[: -len(FIRST_NAME)]
        second_path = base + SECOND_NAME
        paths = [first_path, first_path + ".manifest.json", second_path, second_path + ".manifest.json"]
        if any(path not in by_path for path in paths):
            raise IntegrityError(f"2026 source pair is incomplete: {base}")
        parts = Path(first_path).parts
        family, market = parts[2], parts[3]
        destination = base + MERGED_NAME
        if destination in by_path or (root / destination).exists():
            raise IntegrityError(f"merged destination already exists: {destination}")
        units.append(
            {
                "family": family,
                "market": market,
                "source_artifacts": [by_path[path] for path in paths],
                "source_dbns": [first_path, second_path],
                "destination_dbn": destination,
                "destination_sidecar": destination + ".manifest.json",
            }
        )
    if len(units) != EXPECTED_UNITS or len({(u["family"], u["market"]) for u in units}) != EXPECTED_UNITS:
        raise IntegrityError("2026 consolidation ledger is not exactly 58 x 7")
    source_paths = {str(item["path"]) for unit in units for item in unit["source_artifacts"]}
    if len(source_paths) != EXPECTED_SOURCE_FILES:
        raise IntegrityError("source artifact ledger is not exactly 1,624 files")
    bindings = {
        "active_pointer_sha256": sha256_file(root / ACTIVE_POINTER),
        "active_release_id": str(contract["active_canonical_source"]["release_id"]),
        "active_source_contract_sha256": sha256_file(contract_path),
        "predecessor_source_contract_sha256": sha256_file(root / PREDECESSOR_SOURCE_CONTRACT),
        "complete_inventory_path": _relative(root, inventory_path),
        "complete_inventory_sha256": sha256_file(inventory_path),
        "complete_inventory_id": str(inventory["inventory_id"]),
        "implementation_sha256": _implementation_sha(root),
        "active_alpha_pointer_sha256": sha256_file(root / ACTIVE_ALPHA_POINTER),
        "data_active_summary": _inventory_summary(_inventory_rows(root, root / "data/active")),
    }
    core = {
        "schema_version": "l0_2026_dbn_consolidation_plan/1.0.0",
        "status": "PREPARED_PROVIDER_FREE_REAL_ROW_CONSOLIDATION",
        "interval": {"start_inclusive_utc": START, "end_exclusive_utc": END},
        "counts": {"units": EXPECTED_UNITS, "source_artifacts": EXPECTED_SOURCE_FILES},
        "bindings": bindings,
        "units": units,
        "authority": {
            "provider_access": False,
            "research_activation": False,
            "holdout_or_forward_selection": True,
            "model_fit_prediction_or_evaluation": False,
            "trading_or_order_access": False,
        },
    }
    plan = {**core, "plan_id": sha256_json(core)}
    path = root / PLAN_ROOT / str(plan["plan_id"]) / "plan.json"
    _write_exact_or_new(path, plan)
    return {**plan, "plan_path": _relative(root, path)}


def _load_plan(root: Path, path: Path) -> dict[str, Any]:
    plan = read_canonical_authority(path)
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    if plan.get("plan_id") != sha256_json(core):
        raise IntegrityError("consolidation plan identity differs")
    return plan


def merge_scope(plan: Mapping[str, Any], plan_sha256: str) -> dict[str, str]:
    return {
        "plan_id": str(plan["plan_id"]),
        "unit_count": "406",
        "source_artifact_count": "1624",
        "implementation_sha256": str(plan["bindings"]["implementation_sha256"]),
        "provider_access": "false",
        "research_activation": "false",
        "holdout_or_forward_row_read": "true",
        "model_fit_prediction_or_evaluation": "false",
        "trading_or_order_access": "false",
        "approval_command": MERGE_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": plan_sha256,
    }


def _sidecar(unit: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "l0_2026_merged_canonical_sidecar/1.0.0",
        "activation_status": "CANONICAL_CUSTODY_NOT_RESEARCH_ADMITTED",
        "coverage_interval": {"start_inclusive_utc": START, "end_exclusive_utc": END},
        "dataset": "GLBX.MDP3",
        "family": unit["family"],
        "market": unit["market"],
        "year": 2026,
        "canonical_dbn": {
            "project_relative_path": unit["destination_dbn"],
            "copy_semantics": "DECODED_ORDER_PRESERVING_DBN_V3_CONSOLIDATION",
        },
        "record_integrity": {
            "source_record_counts": [row["record_count"] for row in result["sources"]],
            "source_record_sequence_sha256": [row["record_sequence_sha256"] for row in result["sources"]],
            "merged_record_count": result["merged"]["record_count"],
            "merged_record_sequence_sha256": result["merged"]["record_sequence_sha256"],
            "source_versions": result["source_versions"],
            "output_version": 3,
            "record_values_and_order_preserved": True,
            "provider_byte_identity_preserved": False,
            "provider_completeness_certified": False,
        },
        "input_provenance": {"source_artifacts": unit["source_artifacts"]},
        "authority": {
            "admitted_standard_foundation": False,
            "research_activation": False,
            "holdout_or_forward_selection": False,
            "model_or_evaluation": False,
            "trading_or_order_access": False,
        },
    }


def _candidate_entries(plan: Mapping[str, Any], payload: Path) -> list[dict[str, Any]]:
    standard = set(STANDARD_ROOTS)
    rows: list[dict[str, Any]] = []
    for unit in plan["units"]:
        lane = "STANDARD_41" if unit["market"] in standard else "DEFERRED_MICRO_17"
        for kind, path in (("DBN", unit["destination_dbn"]), ("SIDECAR", unit["destination_sidecar"])):
            candidate = payload / Path(path).relative_to(DBN_ROOT)
            rows.append(
                {
                    "admitted_standard_foundation": False,
                    "family": unit["family"],
                    "identity_basis": "DECODED_ORDER_PRESERVING_DBN_V3_CONSOLIDATION" if kind == "DBN" else "SIDECAR_BYTES_HASHED_METADATA_ONLY",
                    "interval_start_inclusive": "2026-01-01",
                    "interval_end_exclusive": "2026-09-01",
                    "kind": kind,
                    "lane": lane,
                    "market": unit["market"],
                    "path": path,
                    "sha256": sha256_file(candidate),
                    "sidecar_schema_version": "l0_2026_merged_canonical_sidecar/1.0.0",
                    "size_bytes": candidate.stat().st_size,
                    "year": 2026,
                }
            )
    return sorted(rows, key=lambda item: item["path"])


def prepare_merged_candidate(root: Path, plan_path: Path, authorization: OperationReceipt) -> dict[str, Any]:
    """Decode, merge, verify, and seal all 406 candidates outside data/dbn."""

    root = root.resolve(strict=True)
    plan_path = plan_path.resolve(strict=True)
    plan = _load_plan(root, plan_path)
    bindings = plan["bindings"]
    for key, relative in (
        ("active_pointer_sha256", ACTIVE_POINTER),
        ("active_source_contract_sha256", ACTIVE_SOURCE_CONTRACT),
        ("complete_inventory_sha256", Path(str(bindings["complete_inventory_path"]))),
        ("implementation_sha256", Path(__file__).resolve().relative_to(root)),
    ):
        if sha256_file(root / relative) != bindings[key]:
            raise IntegrityError(f"consolidation binding drifted: {key}")
    basis = {"plan_id": plan["plan_id"], "implementation_sha256": bindings["implementation_sha256"]}
    run_id = "l02026merge_" + sha256_json(basis)[:32]
    report = root / REPORT_ROOT / run_id
    payload = root / STAGE_ROOT / run_id / "candidate/data/dbn"
    publication_path = report / "publication_plan.json"
    if publication_path.exists():
        verified = verify_merged_candidate(root, publication_path)
        return {**read_canonical_authority(publication_path), "verification": verified, "publication_plan_path": _relative(root, publication_path)}
    if report.exists() or payload.exists():
        raise IntegrityError("partial consolidation run already exists")
    if shutil.disk_usage(root).free < MIN_FREE_BYTES:
        raise IntegrityError("less than five gigabytes are free for consolidation")
    authorization.consume(
        RepoBoundary(root),
        operation=MERGE_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=merge_scope(plan, sha256_file(plan_path)),
    )
    report.mkdir(parents=True, exist_ok=False)
    results: list[dict[str, Any]] = []
    try:
        for index, unit in enumerate(plan["units"], start=1):
            first, second = (root / path for path in unit["source_dbns"])
            for artifact in unit["source_artifacts"]:
                path = root / str(artifact["path"])
                if path.stat().st_size != artifact["size_bytes"] or sha256_file(path) != artifact["sha256"]:
                    raise IntegrityError(f"source artifact differs: {path}")
            destination = payload / Path(unit["destination_dbn"]).relative_to(DBN_ROOT)
            result = merge_dbn_sources(
                (first, second),
                destination,
                expected_start_ns=1_767_225_600_000_000_000,
                expected_end_ns=1_788_220_800_000_000_000,
                require_contiguous=True,
            )
            sidecar = _sidecar(unit, result)
            sidecar["canonical_dbn"].update({"sha256": sha256_file(destination), "size_bytes": destination.stat().st_size})
            write_new_canonical_authority(Path(f"{destination}.manifest.json"), sidecar)
            results.append({"unit_index": index, "family": unit["family"], "market": unit["market"], **result})
        candidate_rows = _inventory_rows(root, payload)
        candidate_summary = _inventory_summary(candidate_rows)
        if candidate_summary["file_count"] != 812:
            raise IntegrityError("candidate does not contain exactly 812 files")
        prior_contract = _json(root / ACTIVE_SOURCE_CONTRACT, "active source contract")
        prior_inventory = _json(root / str(prior_contract["complete_inventory"]["path"]), "active inventory")
        prior_entries = prior_inventory["entries"]
        removed = {str(item["path"]) for unit in plan["units"] for item in unit["source_artifacts"]}
        additions = _candidate_entries(plan, payload)
        successor_entries = sorted([dict(item) for item in prior_entries if item["path"] not in removed] + additions, key=lambda item: item["path"])
        if len(successor_entries) != EXPECTED_SUCCESSOR_FILES or len({item["path"] for item in successor_entries}) != EXPECTED_SUCCESSOR_FILES:
            raise IntegrityError("successor inventory path set differs")
        content_rows = [{"relative_path": str(item["path"])[len("data/dbn/"):], "size_bytes": int(item["size_bytes"]), "sha256": str(item["sha256"])} for item in successor_entries]
        shadow = {"file_count": len(content_rows), "total_bytes": sum(item["size_bytes"] for item in content_rows), "inventory_sha256": sha256_json(content_rows)}
        release_core = {
            "schema_version": "l0_2026_dbn_consolidation_successor_core/1.0.0",
            "scope": "REPLACE_TWO_2026_SHARDS_WITH_ONE_ORDER_PRESERVING_DBN_V3_UNIT",
            "physical_publication_successor_to": bindings["active_release_id"],
            "plan_id": plan["plan_id"],
            "removed_source_artifact_count": EXPECTED_SOURCE_FILES,
            "added_merged_artifact_count": 812,
            "successor_inventory_sha256": sha256_json(successor_entries),
            "complete_shadow_tree": shadow,
            "research_admitted_addition_count": 0,
        }
        release_id = sha256_json(release_core)
        release = {"schema_version": "l0_2026_dbn_consolidation_successor_release/1.0.0", "release_id": release_id, "release_status": "CERTIFIED_NON_ACTIVE_REQUIRES_BOUND_PUBLICATION", "publication_authorized": False, "release_core": release_core, "complete_shadow_tree": shadow, "successor_inventory": successor_entries}
        release_path = report / "successor_release_manifest.json"
        write_new_canonical_authority(release_path, release)
        inventory_core = {"schema_version": "canonical_dbn_complete_inventory/1.0.0", "status": "PASS_COMPLETE_IMMUTABLE_CANONICAL_SOURCE_SUCCESSOR", "active_release_id": release_id, "content_inventory_sha256": shadow["inventory_sha256"], "entries": successor_entries, "file_count": shadow["file_count"], "total_bytes": shadow["total_bytes"]}
        inventory = {**inventory_core, "inventory_id": sha256_json(inventory_core)}
        inventory_path = report / "canonical_inventory.json"
        write_new_canonical_authority(inventory_path, inventory)
        archive_entries = []
        archive_relative_root = ARCHIVE_PARENT / bindings["active_release_id"] / run_id / "retired_2026_shards"
        for unit in plan["units"]:
            for artifact in unit["source_artifacts"]:
                archive_entries.append({**artifact, "archive_path": (archive_relative_root / str(artifact["path"])).as_posix()})
        archive_core = {"schema_version": "l0_2026_dbn_source_archive_inventory/1.0.0", "status": "PREPARED_EXACT_BYTES_NOT_YET_MOVED", "prior_release_id": bindings["active_release_id"], "source_artifact_count": len(archive_entries), "entries": archive_entries}
        archive_inventory = {**archive_core, "archive_inventory_id": sha256_json(archive_core)}
        archive_inventory_path = report / "archive_inventory.json"
        write_new_canonical_authority(archive_inventory_path, archive_inventory)
        wrapper = {"schema_version": "canonical_data_dbn_release_wrapper/2.0.0", "status": "IMMUTABLE_SUCCESSOR_WRAPPER", "release_id": release_id, "release_manifest_path": _relative(root, release_path), "release_manifest_sha256": sha256_file(release_path)}
        wrapper_path = report / "active_wrapper_v2.json"
        write_new_canonical_authority(wrapper_path, wrapper)
        final_wrapper = Path("state/dbn_congruence_release_registry") / release_id / "active_wrapper_v2.json"
        pointer_core = {"activated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), "canonical_artifact_root": DBN_ROOT.as_posix(), "physical_publication_successor_to": bindings["active_release_id"], "release_id": release_id, "release_manifest_path": _relative(root, release_path), "release_manifest_sha256": sha256_file(release_path), "schema_version": "canonical_data_dbn_active_pointer/2.0.0", "scope": release_core["scope"], "status": "ACTIVE", "wrapper_path": final_wrapper.as_posix(), "wrapper_sha256": sha256_file(wrapper_path)}
        pointer = {**pointer_core, "pointer_id": sha256_json(pointer_core)}
        pointer_path = report / "active_pointer.json"
        write_new_canonical_authority(pointer_path, pointer)
        final_inventory = Path("state/source_contract_registry") / release_id / "canonical_inventory.json"
        successor_contract = dict(prior_contract)
        successor_contract["active_canonical_source"] = {"canonical_root": DBN_ROOT.as_posix(), "complete_root_file_count": shadow["file_count"], "complete_root_total_bytes": shadow["total_bytes"], "pointer_id": pointer["pointer_id"], "pointer_path": ACTIVE_POINTER.as_posix(), "pointer_sha256": sha256_file(pointer_path), "release_id": release_id, "release_manifest_path": _relative(root, release_path), "release_manifest_sha256": sha256_file(release_path)}
        successor_contract["complete_inventory"] = {"content_inventory_sha256": shadow["inventory_sha256"], "file_count": shadow["file_count"], "inventory_id": inventory["inventory_id"], "path": final_inventory.as_posix(), "sha256": sha256_file(inventory_path), "size_bytes": inventory_path.stat().st_size, "total_bytes": shadow["total_bytes"]}
        counts = dict(prior_contract["source_family_dbn_counts"])
        for family in ("definition", "status", "statistics", "ohlcv_1m", "ohlcv_1s", "ohlcv_1h", "ohlcv_1d"):
            counts[family] = int(counts[family]) - 58
        successor_contract["source_family_dbn_counts"] = counts
        successor_contract.pop("contract_id", None)
        successor_contract["contract_id"] = sha256_json(successor_contract)
        source_path = report / "source_contract_v6.json"
        write_new_canonical_authority(source_path, successor_contract)
        rollback = {"schema_version": "l0_2026_dbn_consolidation_rollback/1.0.0", "prior_pointer_sha256": bindings["active_pointer_sha256"], "prior_source_contract_sha256": bindings["active_source_contract_sha256"], "archive_artifact_count": EXPECTED_SOURCE_FILES, "candidate_artifact_count": 812, "failure_policy": "RESTORE_ALIASES_MOVE_MERGED_BACK_TO_STAGING_MOVE_ORIGINALS_BACK_TO_EXACT_PATHS"}
        rollback_path = report / "rollback_plan.json"
        write_new_canonical_authority(rollback_path, rollback)
        core = {
            "schema_version": "l0_2026_dbn_consolidation_publication_plan/1.0.0",
            "state": "CERTIFIED_CONDITIONAL_PUBLICATION_AUTHORIZED_BY_CURRENT_USER_REQUEST",
            "run_id": run_id,
            "plan_id": plan["plan_id"],
            "successor_release_id": release_id,
            "prior_active_pointer_sha256": bindings["active_pointer_sha256"],
            "prior_source_contract_sha256": bindings["active_source_contract_sha256"],
            "predecessor_source_contract_sha256": bindings["predecessor_source_contract_sha256"],
            "active_alpha_pointer_sha256": bindings["active_alpha_pointer_sha256"],
            "data_active_summary": bindings["data_active_summary"],
            "candidate_payload_root": _relative(root, payload),
            "candidate_summary": candidate_summary,
            "successor_summary": shadow,
            "archive_relative_root": archive_relative_root.as_posix(),
            "archive_inventory_path": _relative(root, archive_inventory_path),
            "archive_inventory_sha256": sha256_file(archive_inventory_path),
            "release_manifest_path": _relative(root, release_path), "release_manifest_sha256": sha256_file(release_path),
            "inventory_candidate_path": _relative(root, inventory_path), "inventory_candidate_sha256": sha256_file(inventory_path),
            "wrapper_candidate_path": _relative(root, wrapper_path), "wrapper_candidate_sha256": sha256_file(wrapper_path),
            "pointer_candidate_path": _relative(root, pointer_path), "pointer_candidate_sha256": sha256_file(pointer_path),
            "source_contract_candidate_path": _relative(root, source_path), "source_contract_candidate_sha256": sha256_file(source_path),
            "rollback_plan_path": _relative(root, rollback_path), "rollback_plan_sha256": sha256_file(rollback_path),
            "results": results,
            "targets": [{"family": unit["family"], "market": unit["market"], "sources": [{"canonical_path": item["path"], "archive_path": (archive_relative_root / str(item["path"])).as_posix(), "sha256": item["sha256"], "size_bytes": item["size_bytes"]} for item in unit["source_artifacts"]], "candidate_dbn": _relative(root, payload / Path(unit["destination_dbn"]).relative_to(DBN_ROOT)), "candidate_sidecar": _relative(root, payload / Path(unit["destination_sidecar"]).relative_to(DBN_ROOT)), "destination_dbn": unit["destination_dbn"], "destination_sidecar": unit["destination_sidecar"]} for unit in plan["units"]],
            "authority": {"provider_access": False, "research_activation": False, "holdout_or_forward_selection": False, "model_fit_prediction_or_evaluation": False, "trading_or_order_access": False, "canonical_custody_publication": True},
        }
        publication = {**core, "publication_plan_id": sha256_json(core)}
        write_new_canonical_authority(publication_path, publication)
        verification = verify_merged_candidate(root, publication_path)
        return {**publication, "verification": verification, "publication_plan_path": _relative(root, publication_path)}
    except Exception as exc:
        failure = report / "candidate_failure.json"
        if not failure.exists():
            write_new_canonical_authority(failure, {"schema_version": "l0_2026_dbn_consolidation_failure/1.0.0", "status": "FAIL_CLOSED_NO_CANONICAL_MUTATION", "run_id": run_id, "error_type": type(exc).__name__, "canonical_mutation": False})
        raise


def _load_publication_plan(root: Path, path: Path, *, candidate_required: bool = True) -> dict[str, Any]:
    plan = read_canonical_authority(path)
    core = {key: value for key, value in plan.items() if key != "publication_plan_id"}
    if plan.get("publication_plan_id") != sha256_json(core):
        raise IntegrityError("publication plan identity differs")
    for path_key, hash_key in (("archive_inventory_path", "archive_inventory_sha256"), ("release_manifest_path", "release_manifest_sha256"), ("inventory_candidate_path", "inventory_candidate_sha256"), ("wrapper_candidate_path", "wrapper_candidate_sha256"), ("pointer_candidate_path", "pointer_candidate_sha256"), ("source_contract_candidate_path", "source_contract_candidate_sha256"), ("rollback_plan_path", "rollback_plan_sha256")):
        if sha256_file(root / str(plan[path_key])) != plan[hash_key]:
            raise IntegrityError(f"publication plan binding differs: {path_key}")
    if candidate_required and _inventory_summary(_inventory_rows(root, root / str(plan["candidate_payload_root"]))) != plan["candidate_summary"]:
        raise IntegrityError("candidate payload differs")
    return plan


def verify_merged_candidate(root: Path, publication_plan_path: Path) -> dict[str, Any]:
    root = root.resolve(strict=True)
    plan = _load_publication_plan(root, publication_plan_path.resolve(strict=True))
    if len(plan["targets"]) != EXPECTED_UNITS:
        raise IntegrityError("candidate target count differs")
    total = 0
    mixed = 0
    for target, result in zip(plan["targets"], plan["results"], strict=True):
        dbn = root / target["candidate_dbn"]
        sidecar = root / target["candidate_sidecar"]
        observed = _stream_records(dbn)
        if observed != result["merged"] or sha256_file(dbn) != read_canonical_authority(sidecar)["canonical_dbn"]["sha256"]:
            raise IntegrityError("candidate record sequence or sidecar differs")
        metadata = _metadata(dbn)
        if metadata["version"] != 3 or metadata["start"] != 1_767_225_600_000_000_000 or metadata["end"] != 1_788_220_800_000_000_000:
            raise IntegrityError("candidate metadata interval differs")
        total += observed["record_count"]
        mixed += result["source_versions"] == [1, 3]
    return {"status": "PASS_406_ORDER_PRESERVING_DBN_V3_CANDIDATES", "unit_count": EXPECTED_UNITS, "candidate_file_count": 812, "record_count": total, "mixed_version_unit_count": mixed, "provider_calls": 0, "research_activation": False}


def publication_scope(plan: Mapping[str, Any], plan_sha256: str) -> dict[str, str]:
    return {
        "run_id": str(plan["run_id"]), "successor_release_id": str(plan["successor_release_id"]),
        "archived_source_artifact_count": "1624", "installed_merged_artifact_count": "812", "successor_file_count": "11474",
        "provider_access": "false", "research_activation": "false", "holdout_or_forward_selection": "false", "model_fit_prediction_or_evaluation": "false", "trading_or_order_access": "false", "canonical_custody_publication": "true",
        "approval_command": PUBLICATION_OPERATION, "approval_plan_id": str(plan["publication_plan_id"]), "approval_plan_sha256": plan_sha256,
    }


def publish_authorized(root: Path, publication_plan_path: Path, authorization: OperationReceipt, *, failpoint: str | None = None) -> dict[str, Any]:
    """Archive exact predecessors, install merged custody, activate last, and rollback."""

    root = root.resolve(strict=True)
    publication_plan_path = publication_plan_path.resolve(strict=True)
    plan = _load_publication_plan(root, publication_plan_path, candidate_required=False)
    pointer_path, source_path = root / ACTIVE_POINTER, root / ACTIVE_SOURCE_CONTRACT
    if _json(pointer_path, "active pointer").get("release_id") == plan["successor_release_id"]:
        return {"status": "NO_ACTION_ALREADY_ACTIVE_SAME_RELEASE", "verification": verify_publication(root, publication_plan_path)}
    lock = SingleInstance(root / LOCK_PATH)
    if not lock.acquire():
        raise IntegrityError("2026 consolidation publication is already running")
    prior_pointer, prior_source = pointer_path.read_bytes(), source_path.read_bytes()
    archived: list[tuple[Path, Path]] = []
    installed: list[tuple[Path, Path]] = []
    pointer_replaced = source_replaced = v6_created = False
    try:
        if sha256_bytes(prior_pointer) != plan["prior_active_pointer_sha256"] or sha256_bytes(prior_source) != plan["prior_source_contract_sha256"] or sha256_file(root / PREDECESSOR_SOURCE_CONTRACT) != plan["predecessor_source_contract_sha256"]:
            raise IntegrityError("publication predecessor controls drifted")
        verify_merged_candidate(root, publication_plan_path)
        for target in plan["targets"]:
            if (root / target["destination_dbn"]).exists() or (root / target["destination_sidecar"]).exists():
                raise IntegrityError("merged canonical destination appeared")
            for item in target["sources"]:
                source, archive = root / item["canonical_path"], root / item["archive_path"]
                if _exists(archive) or _stat(source).st_size != item["size_bytes"] or sha256_file(source) != item["sha256"]:
                    raise IntegrityError("source artifact differs or archive destination exists")
        authorization.consume(RepoBoundary(root), operation=PUBLICATION_OPERATION, classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION, required_scope=publication_scope(plan, sha256_file(publication_plan_path)))
        archive_root = root / plan["archive_relative_root"]
        io_path(archive_root).mkdir(parents=True, exist_ok=True)
        for path, expected in (
            (archive_root / "prior_active_pointer.json", prior_pointer),
            (archive_root / "prior_source_contract.json", prior_source),
            (
                archive_root / "archive_inventory.json",
                (root / plan["archive_inventory_path"]).read_bytes(),
            ),
        ):
            if _exists(path) and io_path(path).read_bytes() != expected:
                raise IntegrityError("existing rollback archive control differs")
            if not _exists(path):
                io_path(path).write_bytes(expected)
        for index, target in enumerate(plan["targets"], start=1):
            for item in target["sources"]:
                source, archive = root / item["canonical_path"], root / item["archive_path"]
                io_path(archive).parent.mkdir(parents=True, exist_ok=True)
                os.replace(io_path(source), io_path(archive))
                archived.append((archive, source))
            if failpoint == f"after_archive_{index}":
                raise IntegrityError(f"injected failure after archive {index}")
        for index, target in enumerate(plan["targets"], start=1):
            for candidate_key, destination_key in (("candidate_dbn", "destination_dbn"), ("candidate_sidecar", "destination_sidecar")):
                candidate, destination = root / target[candidate_key], root / target[destination_key]
                destination.parent.mkdir(parents=True, exist_ok=True)
                os.replace(io_path(candidate), io_path(destination))
                installed.append((destination, candidate))
            if failpoint == f"after_install_{index}":
                raise IntegrityError(f"injected failure after install {index}")
        release_id = str(plan["successor_release_id"])
        wrapper_destination = root / "state/dbn_congruence_release_registry" / release_id / "active_wrapper_v2.json"
        inventory_destination = root / "state/source_contract_registry" / release_id / "canonical_inventory.json"
        _write_exact_or_new(wrapper_destination, read_canonical_authority(root / plan["wrapper_candidate_path"]))
        _write_exact_or_new(inventory_destination, read_canonical_authority(root / plan["inventory_candidate_path"]))
        successor_source = (root / plan["source_contract_candidate_path"]).read_bytes()
        successor_pointer = (root / plan["pointer_candidate_path"]).read_bytes()
        if (root / SUCCESSOR_SOURCE_CONTRACT).exists():
            if (root / SUCCESSOR_SOURCE_CONTRACT).read_bytes() != successor_source:
                raise IntegrityError("source_contract_v6 exists with different bytes")
        else:
            (root / SUCCESSOR_SOURCE_CONTRACT).write_bytes(successor_source)
            v6_created = True
        _atomic_replace(source_path, successor_source, str(plan["run_id"]))
        source_replaced = True
        if failpoint == "after_source_replace":
            raise IntegrityError("injected failure after source replace")
        _atomic_replace(pointer_path, successor_pointer, str(plan["run_id"]))
        pointer_replaced = True
        if failpoint == "after_pointer_replace":
            raise IntegrityError("injected failure after pointer replace")
        verification = verify_publication(root, publication_plan_path)
        receipt = {"schema_version": "l0_2026_dbn_consolidation_publication_receipt/1.0.0", "status": "ACTIVATED_CONSOLIDATED_CANONICAL_CUSTODY_AND_VERIFIED", "run_id": plan["run_id"], "successor_release_id": release_id, "authorization_receipt_id": authorization.receipt_id, "archived_source_artifacts": EXPECTED_SOURCE_FILES, "installed_merged_dbns": 406, "installed_merged_sidecars": 406, "provider_calls": 0, "research_activation": False, "verification": verification}
        write_new_canonical_authority(publication_plan_path.parent / "publication_receipt.json", receipt)
        return receipt
    except Exception:
        if pointer_replaced:
            _atomic_replace(pointer_path, prior_pointer, str(plan["run_id"]) + ".rollback")
        if source_replaced:
            _atomic_replace(source_path, prior_source, str(plan["run_id"]) + ".rollback")
        if v6_created:
            (root / SUCCESSOR_SOURCE_CONTRACT).unlink(missing_ok=True)
        for destination, candidate in reversed(installed):
            candidate.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                os.replace(io_path(destination), io_path(candidate))
        for archive, source in reversed(archived):
            source.parent.mkdir(parents=True, exist_ok=True)
            if _exists(archive):
                os.replace(io_path(archive), io_path(source))
        if sha256_file(pointer_path) != plan["prior_active_pointer_sha256"] or sha256_file(source_path) != plan["prior_source_contract_sha256"]:
            raise IntegrityError("publication rollback did not restore predecessor controls")
        failure = publication_plan_path.parent / "publication_failure.json"
        if not failure.exists():
            write_new_canonical_authority(failure, {"schema_version": "l0_2026_dbn_consolidation_publication_failure/1.0.0", "status": "ROLLED_BACK_NO_ACTIVE_CANONICAL_MUTATION", "run_id": plan["run_id"], "failpoint": failpoint})
        raise
    finally:
        lock.release()


def verify_publication(root: Path, publication_plan_path: Path) -> dict[str, Any]:
    root = root.resolve(strict=True)
    plan = _load_publication_plan(root, publication_plan_path.resolve(strict=True), candidate_required=False)
    source_candidate = root / plan["source_contract_candidate_path"]
    pointer_candidate = root / plan["pointer_candidate_path"]
    if (root / ACTIVE_SOURCE_CONTRACT).read_bytes() != source_candidate.read_bytes() or (root / SUCCESSOR_SOURCE_CONTRACT).read_bytes() != source_candidate.read_bytes() or (root / ACTIVE_POINTER).read_bytes() != pointer_candidate.read_bytes():
        raise IntegrityError("active successor controls differ")
    predecessor = _json(root / PREDECESSOR_SOURCE_CONTRACT, "v5 source contract")
    contract = _json(root / ACTIVE_SOURCE_CONTRACT, "v6 source contract")
    for key in ("selection_policy", "universe", "authority"):
        if contract.get(key) != predecessor.get(key):
            raise IntegrityError(f"successor changed research control: {key}")
    inventory = _json(root / str(contract["complete_inventory"]["path"]), "successor inventory")
    entries = inventory.get("entries")
    if not isinstance(entries, list) or len(entries) != EXPECTED_SUCCESSOR_FILES:
        raise IntegrityError("successor inventory count differs")
    merged = [item for item in entries if item.get("interval_start_inclusive") == "2026-01-01" and item.get("interval_end_exclusive") == "2026-09-01"]
    if len(merged) != 812 or any(item.get("admitted_standard_foundation") is not False for item in merged):
        raise IntegrityError("merged custody is incomplete or research-admitted")
    observed = _inventory_rows(root, root / DBN_ROOT)
    expected = [{"relative_path": str(item["path"])[len("data/dbn/"):], "size_bytes": int(item["size_bytes"]), "sha256": str(item["sha256"])} for item in entries]
    if observed != expected:
        raise IntegrityError("complete canonical root differs from successor inventory")
    archive_inventory_path = root / plan["archive_inventory_path"]
    archive_inventory = read_canonical_authority(archive_inventory_path)
    archive_root = root / plan["archive_relative_root"]
    if (
        len(archive_inventory.get("entries", ())) != EXPECTED_SOURCE_FILES
        or sha256_file(archive_root / "archive_inventory.json")
        != sha256_file(archive_inventory_path)
        or sha256_file(archive_root / "prior_active_pointer.json")
        != plan["prior_active_pointer_sha256"]
        or sha256_file(archive_root / "prior_source_contract.json")
        != plan["prior_source_contract_sha256"]
    ):
        raise IntegrityError("archive controls differ")
    for item in archive_inventory["entries"]:
        path = root / item["archive_path"]
        if _exists(root / item["path"]) or _stat(path).st_size != item["size_bytes"] or sha256_file(path) != item["sha256"]:
            raise IntegrityError("archived source custody differs")
    if sha256_file(root / ACTIVE_ALPHA_POINTER) != plan["active_alpha_pointer_sha256"] or _inventory_summary(_inventory_rows(root, root / "data/active")) != plan["data_active_summary"]:
        raise IntegrityError("research authority or active data changed")
    context = issue_current_source_closure_context(root, contract_path=root / ACTIVE_SOURCE_CONTRACT)
    closure = validate_source_closure(root, operation_context=context, contract_path=root / ACTIVE_SOURCE_CONTRACT)
    return {"status": "PASS_ACTIVE_2026_CONSOLIDATED_CANONICAL_CUSTODY", "release_id": plan["successor_release_id"], "canonical_file_count": len(observed), "canonical_total_bytes": sum(item["size_bytes"] for item in observed), "merged_dbn_count": 406, "merged_sidecar_count": 406, "archived_source_artifact_count": EXPECTED_SOURCE_FILES, "new_research_admitted_count": 0, "source_closure": closure, "provider_calls": 0, "research_activation": False, "holdout_or_forward_selection": False, "model_fit_prediction_or_evaluation": False, "trading_or_order_access": False}
