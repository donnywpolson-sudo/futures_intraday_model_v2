"""Provider-free annual TBBO and BBO-1s successor construction."""

from __future__ import annotations

import json
import os
import shutil
from collections import Counter
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Final

from . import dbn_catalog
from .boundary import RepoBoundary
from .canonical import (
    canonical_authority_file_bytes,
    canonical_bytes,
    io_path,
    sha256_file,
    sha256_json,
    write_new_canonical_authority,
)
from .data_layout import DataReleaseManifest, manifest_relative_path, verify_data_release_manifest
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .l0_2026_dbn_consolidation import (
    _metadata,
    _require_record_boundary,
    _require_compatible,
    _stream_records,
    merge_dbn_sources,
)
from .live_cockpit.single_instance import SingleInstance


ACTIVE_DBN_POINTER: Final = Path("configs/active_dbn_congruence_release_v1.json")
ACTIVE_SOURCE_CONTRACT: Final = Path("configs/source_contract.json")
ACTIVE_CAUSAL_CATALOG: Final = Path("data/active/catalog.json")
PLAN_ROOT: Final = Path("state/annual_dbn_consolidation_v1")
STAGE_ROOT: Final = Path("state/data_publication_staging/annual_dbn_consolidation_v1")
LOCK_PATH: Final = Path("state/locks/annual_dbn_consolidation_v1.lock")
TBBO_MARKETS: Final = (
    "6E", "6J", "CL", "ES", "GC", "HE", "HG", "LE",
    "NG", "NQ", "RB", "SR3", "ZB", "ZC", "ZN", "ZS",
)
BBO_MARKETS: Final = (
    "6E", "6J", "CL", "ES", "GC", "HE", "HG", "LE", "MCL",
    "MES", "NG", "NQ", "RB", "SR3", "ZB", "ZC", "ZN", "ZS",
)
INTERVALS: Final = {
    2025: ("2025-09-01", "2026-01-01"),
    2026: ("2026-01-01", "2026-09-01"),
}
SOURCE_RELEASES: Final = {
    "tbbo": {
        "release_id": "a1f6e4153edc34dd5baff3b8579e1db6b2e4f7c54a5bd868b769239b41dff911",
        "manifest_sha256": "49ddf3f804e919f3c467de1d2e5a3fb0e2d5e6ffea15a653601090199b12190f",
        "release_kind": "tbbo_16_market_certified_canonical_custody",
        "file_count": 384,
        "catalog": Path(
            "state/tbbo_eligibility_catalog_registry/"
            "8861c8953391684531600967f409674b3b78f7c2e8f22ef7ec422bd512a54a36/"
            "catalog.json"
        ),
        "catalog_sha256": "25f24cc10e7f5c9bd88a0effe88c9dea2bdcc3c4c58ed81d1b631a2b528984f1",
        "certificate": Path(
            "state/tbbo_quality_certification_registry/"
            "8ecc811ec914e7c0d4a258377c3977991db3832bfb9bd375a3c34403673d035c/"
            "certificate.json"
        ),
        "certificate_sha256": "f6a160705f6bc45cb4cf0cf40a197f6ac025670d5e835598c5412756f0851ffd",
    },
    "bbo_1s": {
        "release_id": "991b45e7b5833f547863550ae64008192a6599ffab270b28dbbcb1453d54c96f",
        "manifest_sha256": "28a86de15f549c3881c120de401b34d5e34d7e5b4f1731ff4ff33d6f6f4d161d",
        "release_kind": "l1_bbo_1s_market_partition_canonical_custody",
        "file_count": 9_323,
        "catalog": Path(
            "state/l1_bbo_1s_eligibility_catalog_registry/"
            "6ef726aaf448e12c3f2a89a882e1873159ab09481ebe6b910f4b51e5d186b628/"
            "catalog.json"
        ),
        "catalog_sha256": "8dbc194eb87cd4b427596ca5719281498f4ffabe57345848660f7efbb5eedba6",
        "certificate": Path(
            "state/l1_bbo_1s_quality_certification_registry/"
            "22367a4a82764f1ffacecaad2ee5bee4ad3e922833aae8e0730b6a7f04a84114/"
            "certificate.json"
        ),
        "certificate_sha256": "c6b5908556c07672bab593d8492c010bee7eb727f35b60f1ede08e466a7474dd",
    },
}
EXPECTED_SOURCE_COUNTS: Final = {
    "tbbo_dbns": 192,
    "tbbo_sidecars": 192,
    "bbo_1s_dbns": 4_625,
    "bbo_1s_sidecars": 4_698,
}
EXPECTED_UNIT_COUNT: Final = 68
EXPECTED_OUTPUT_FILE_COUNT: Final = 136
OUTPUT_HEADROOM_BYTES: Final = 16 * 1024 * 1024
FREE_SPACE_RESERVE_BYTES: Final = 10 * 1024 * 1024 * 1024


def _json(path: Path, label: str) -> dict[str, Any]:
    try:
        raw = io_path(path).read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise IntegrityError(f"{label} is not readable JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{label} is not canonical JSON")
    return payload


def _write_new_or_exact(path: Path, payload: Mapping[str, Any]) -> None:
    expected = canonical_authority_file_bytes(payload)
    if path.exists():
        if path.read_bytes() != expected:
            raise IntegrityError(f"existing immutable artifact differs: {path}")
        return
    write_new_canonical_authority(path, payload)


def _iso_ns(value: str) -> int:
    return int(datetime.combine(date.fromisoformat(value), time(), timezone.utc).timestamp() * 1e9)


def _plain(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_plain(item) for item in value]
    if hasattr(value, "__dict__"):
        return _plain(vars(value))
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _unit_key(family: str, market: str, year: int) -> str:
    return f"{family}:{market}:{year}"


def _release(root: Path, family: str) -> tuple[DataReleaseManifest, dict[str, object]]:
    binding = SOURCE_RELEASES[family]
    manifest_path = root / manifest_relative_path("dbn", str(binding["release_id"]))
    if sha256_file(manifest_path) != binding["manifest_sha256"]:
        raise IntegrityError(f"{family} source release manifest differs")
    manifest = verify_data_release_manifest(manifest_path, RepoBoundary(root), verify_files=False)
    if (
        manifest.release_kind != binding["release_kind"]
        or len(manifest.files) != binding["file_count"]
    ):
        raise IntegrityError(f"{family} source release contract differs")
    return manifest, binding


def _artifact(root: Path, manifest: DataReleaseManifest, logical: str, *, hash_bytes: bool) -> dict[str, Any]:
    entries = {entry.logical_path: entry for entry in manifest.files}
    entry = entries.get(logical)
    if entry is None:
        raise IntegrityError(f"source release entry is absent: {logical}")
    path = root / manifest.physical_relative_path(entry)
    info = io_path(path).stat()
    if info.st_size != entry.size or (hash_bytes and sha256_file(path) != entry.sha256):
        raise IntegrityError(f"source release entry differs: {logical}")
    return {"path": logical, "sha256": entry.sha256, "size_bytes": entry.size}


def _source_row(
    root: Path, manifest: DataReleaseManifest, sidecar_logical: str, family: str,
) -> dict[str, Any]:
    sidecar = _artifact(root, manifest, sidecar_logical, hash_bytes=True)
    payload = _json(root / sidecar_logical, f"{family} source sidecar")
    dbn_logical = sidecar_logical.removesuffix(".manifest.json")
    declared_hash = payload.get("source_dbn_sha256")
    if declared_hash is None:
        if family != "bbo_1s" or payload.get("provider_condition") != "ZERO_BILLABLE_NO_DBN":
            raise IntegrityError("manifest-only source lacks the zero-billable disposition")
        dbn = None
    else:
        dbn = _artifact(root, manifest, dbn_logical, hash_bytes=False)
        if declared_hash != dbn["sha256"] or payload.get("source_dbn_size_bytes") != dbn["size_bytes"]:
            raise IntegrityError("source sidecar DBN identity differs from its release")
    sidecar_id = payload.get("sidecar_id")
    if type(sidecar_id) is not str or len(sidecar_id) != 64:
        raise IntegrityError("source sidecar identity is absent")
    return {
        "dbn": dbn,
        "sidecar": sidecar,
        "sidecar_id": sidecar_id,
        "source_disposition": payload,
    }


def _unit(
    root: Path,
    family: str,
    market: str,
    year: int,
    manifest: DataReleaseManifest,
) -> dict[str, Any]:
    start, end = INTERVALS[year]
    prefix = f"data/dbn/{family}/{market}/{year}/"
    if family == "tbbo":
        suffixes = sorted(
            entry.logical_path
            for entry in manifest.files
            if entry.logical_path.startswith(prefix)
            and entry.logical_path.endswith(".dbn.zst.manifest.json")
        )
    else:
        suffixes = sorted(
            entry.logical_path
            for entry in manifest.files
            if entry.logical_path.startswith(prefix)
            and entry.logical_path.endswith(".dbn.zst.manifest.json")
            and "_" not in Path(entry.logical_path).name
        )
    ledger = [_source_row(root, manifest, logical, family) for logical in suffixes]
    if family == "tbbo":
        expected = 4 if year == 2025 else 8
        if len(ledger) != expected or any(item["dbn"] is None for item in ledger):
            raise IntegrityError("TBBO monthly source ledger is incomplete")
        prior = start
        for item in ledger:
            payload = item["source_disposition"]
            if payload.get("market") != market or payload.get("coverage_start") != prior:
                raise IntegrityError("TBBO monthly source chronology differs")
            prior = str(payload.get("coverage_end"))
        if prior != end:
            raise IntegrityError("TBBO monthly source coverage is incomplete")
    else:
        dates: list[str] = []
        for item in ledger:
            payload = item["source_disposition"]
            trade_date = payload.get("trade_date")
            if (
                payload.get("market") != market
                or type(trade_date) is not str
                or not start <= trade_date < end
            ):
                raise IntegrityError("BBO-1s daily source ledger differs")
            dates.append(trade_date)
        if dates != sorted(set(dates)):
            raise IntegrityError("BBO-1s daily source ledger is duplicated or unordered")
    source_dbns = [item["dbn"] for item in ledger if item["dbn"] is not None]
    destination = f"{prefix}{start}_{end}.dbn.zst"
    if (root / destination).exists() or (root / f"{destination}.manifest.json").exists():
        raise IntegrityError(f"annual destination already exists: {destination}")
    input_bytes = sum(int(item["size_bytes"]) for item in source_dbns)
    return {
        "unit_key": _unit_key(family, market, year),
        "family": family,
        "market": market,
        "year": year,
        "coverage": {"start_inclusive": start, "end_exclusive": end},
        "source_ledger": ledger,
        "source_dbns": source_dbns,
        "destination_dbn": destination,
        "destination_sidecar": f"{destination}.manifest.json",
        "input_dbn_bytes": input_bytes,
        "maximum_output_bytes": input_bytes * 2 + OUTPUT_HEADROOM_BYTES,
        "require_contiguous_source_metadata": family == "tbbo",
    }


def build_plan(root: Path) -> dict[str, Any]:
    """Bind all 68 annual units without opening any DBN record stream."""

    root = root.resolve(strict=True)
    releases = {family: _release(root, family) for family in SOURCE_RELEASES}
    units = [
        _unit(root, family, market, year, releases[family][0])
        for family, markets in (("tbbo", TBBO_MARKETS), ("bbo_1s", BBO_MARKETS))
        for market in markets
        for year in INTERVALS
    ]
    counts = Counter(
        f"{unit['family']}_{'dbns' if row['dbn'] is not None else 'manifest_only'}"
        for unit in units
        for row in unit["source_ledger"]
    )
    sidecar_counts = Counter(
        f"{unit['family']}_sidecars" for unit in units for _ in unit["source_ledger"]
    )
    observed = {
        "tbbo_dbns": counts["tbbo_dbns"],
        "tbbo_sidecars": sidecar_counts["tbbo_sidecars"],
        "bbo_1s_dbns": counts["bbo_1s_dbns"],
        "bbo_1s_sidecars": sidecar_counts["bbo_1s_sidecars"],
    }
    if len(units) != EXPECTED_UNIT_COUNT or observed != EXPECTED_SOURCE_COUNTS:
        raise IntegrityError("annual source scope differs from 68 units and 9,707 artifacts")
    source_paths = [
        artifact["path"]
        for unit in units
        for row in unit["source_ledger"]
        for artifact in (row["sidecar"], row["dbn"])
        if artifact is not None
    ]
    if len(source_paths) != len(set(source_paths)):
        raise IntegrityError("annual source artifact ledger contains duplicates")
    controls = {}
    for family, (_, binding) in releases.items():
        for key in ("catalog", "certificate"):
            path = root / binding[key]
            if sha256_file(path) != binding[f"{key}_sha256"]:
                raise IntegrityError(f"{family} {key} binding differs")
        controls[family] = {
            "release_id": binding["release_id"],
            "release_manifest_path": manifest_relative_path(
                "dbn", str(binding["release_id"])
            ).as_posix(),
            "release_manifest_sha256": binding["manifest_sha256"],
            "catalog_path": Path(binding["catalog"]).as_posix(),
            "catalog_sha256": binding["catalog_sha256"],
            "certificate_path": Path(binding["certificate"]).as_posix(),
            "certificate_sha256": binding["certificate_sha256"],
        }
    output_ceiling = sum(int(unit["maximum_output_bytes"]) for unit in units)
    bindings = {
        "head": _git_output(root, "rev-parse", "HEAD"),
        "branch": _git_output(root, "branch", "--show-current"),
        "active_dbn_pointer_sha256": sha256_file(root / ACTIVE_DBN_POINTER),
        "active_source_contract_sha256": sha256_file(root / ACTIVE_SOURCE_CONTRACT),
        "active_causal_catalog_sha256": sha256_file(root / ACTIVE_CAUSAL_CATALOG),
        "implementation_sha256": sha256_file(Path(__file__)),
        "merger_implementation_sha256": sha256_file(
            Path(merge_dbn_sources.__code__.co_filename)
        ),
        "coverage_schema_implementation_sha256": sha256_file(Path(dbn_catalog.__file__)),
        "source_controls": controls,
    }
    free_bytes = shutil.disk_usage(root).free
    core = {
        "schema_version": "annual_dbn_consolidation_plan/1.0.0",
        "status": "PREPARED_NO_DBN_ROWS_OPENED",
        "counts": {
            **observed,
            "source_artifacts": len(source_paths),
            "annual_units": len(units),
            "candidate_files": EXPECTED_OUTPUT_FILE_COUNT,
        },
        "storage": {
            "source_dbn_bytes": sum(int(unit["input_dbn_bytes"]) for unit in units),
            "candidate_output_ceiling_bytes": output_ceiling,
            "required_free_bytes": output_ceiling + FREE_SPACE_RESERVE_BYTES,
            "observed_free_bytes": free_bytes,
            "publication_and_rebuild_not_included": True,
        },
        "bindings": bindings,
        "units": units,
        "authority": {
            "provider_or_network_access": False,
            "real_dbn_rows_opened": False,
            "canonical_publication": False,
            "derived_rebuild": False,
            "deletion": False,
            "research_or_trading": False,
        },
    }
    plan = {**core, "plan_id": sha256_json(core)}
    path = root / PLAN_ROOT / str(plan["plan_id"]) / "plan.json"
    _write_new_or_exact(path, plan)
    return {**plan, "plan_path": path.relative_to(root).as_posix()}


def _git_output(root: Path, *arguments: str) -> str:
    import subprocess

    result = subprocess.run(
        ("git", *arguments), cwd=root, check=True, capture_output=True, text=True,
    )
    return result.stdout.strip()


def load_plan(root: Path, plan_path: Path) -> dict[str, Any]:
    root = root.resolve(strict=True)
    path = plan_path if plan_path.is_absolute() else root / plan_path
    plan = _json(path.resolve(strict=True), "annual consolidation plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    if plan.get("plan_id") != sha256_json(core) or len(plan.get("units", ())) != EXPECTED_UNIT_COUNT:
        raise IntegrityError("annual consolidation plan identity differs")
    return plan


def _validate_bindings(root: Path, plan: Mapping[str, Any], *, implementation: bool) -> None:
    bindings = plan["bindings"]
    checks = {
        ACTIVE_DBN_POINTER: bindings["active_dbn_pointer_sha256"],
        ACTIVE_SOURCE_CONTRACT: bindings["active_source_contract_sha256"],
        ACTIVE_CAUSAL_CATALOG: bindings["active_causal_catalog_sha256"],
    }
    if implementation:
        checks[Path(__file__).resolve().relative_to(root)] = bindings["implementation_sha256"]
        checks[
            Path(merge_dbn_sources.__code__.co_filename).resolve().relative_to(root)
        ] = bindings["merger_implementation_sha256"]
        checks[Path(dbn_catalog.__file__).resolve().relative_to(root)] = bindings[
            "coverage_schema_implementation_sha256"
        ]
    for relative, expected in checks.items():
        if sha256_file(root / relative) != expected:
            raise IntegrityError(f"annual consolidation binding drifted: {relative}")
    for controls in bindings["source_controls"].values():
        for label in ("release_manifest", "catalog", "certificate"):
            if sha256_file(root / controls[f"{label}_path"]) != controls[f"{label}_sha256"]:
                raise IntegrityError(f"annual consolidation {label} binding drifted")


def _verify_source_artifact(root: Path, artifact: Mapping[str, Any]) -> Path:
    path = root / str(artifact["path"])
    info = io_path(path).stat()
    if info.st_size != artifact["size_bytes"] or sha256_file(path) != artifact["sha256"]:
        raise IntegrityError(f"annual source artifact drifted: {artifact['path']}")
    return path


def _candidate_paths(root: Path, plan: Mapping[str, Any], unit: Mapping[str, Any]) -> tuple[Path, Path]:
    payload = root / STAGE_ROOT / str(plan["plan_id"]) / "payload"
    dbn = payload / Path(str(unit["destination_dbn"])).relative_to(Path("data/dbn"))
    return dbn, Path(f"{dbn}.manifest.json")


def _annual_sidecar(unit: Mapping[str, Any], result: Mapping[str, Any], dbn: Path) -> dict[str, Any]:
    core = {
        "schema_version": "annual_dbn_successor_sidecar/1.0.0",
        "state": "STAGED_PROVIDER_FREE_ANNUAL_SUCCESSOR_NOT_ACTIVE",
        "family": unit["family"],
        "market": unit["market"],
        "year": unit["year"],
        "coverage": unit["coverage"],
        "canonical_dbn": {
            "logical_path": unit["destination_dbn"],
            "size_bytes": dbn.stat().st_size,
            "sha256": sha256_file(dbn),
            "copy_semantics": "DECODED_ORDER_PRESERVING_DBN_V3_CONSOLIDATION",
        },
        "source_ledger": unit["source_ledger"],
        "record_integrity": {
            "source_record_counts": [item["record_count"] for item in result["sources"]],
            "source_record_sequence_sha256": [
                item["record_sequence_sha256"] for item in result["sources"]
            ],
            "source_first_timestamp_ns": [item["first_timestamp_ns"] for item in result["sources"]],
            "source_last_timestamp_ns": [item["last_timestamp_ns"] for item in result["sources"]],
            "merged_record_count": result["merged"]["record_count"],
            "merged_record_sequence_sha256": result["merged"]["record_sequence_sha256"],
            "merged_first_timestamp_ns": result["merged"]["first_timestamp_ns"],
            "merged_last_timestamp_ns": result["merged"]["last_timestamp_ns"],
            "record_values_and_order_preserved": True,
            "provider_byte_identity_preserved": False,
        },
        "metadata": {
            "sources": _plain(result["source_metadata"]),
            "merged": _plain(result["merged_metadata"]),
            "ordered_non_overlapping_merged_mappings": _plain(
                result["merged_metadata"]["mappings"]
            ),
            "compatibility_checks": result["compatibility_checks"],
        },
        "authority": {
            "provider_or_network_access": False,
            "research_or_trading_authority": False,
            "canonical_publication": False,
            "deletion": False,
        },
    }
    return {**core, "sidecar_id": sha256_json(core)}


def _build_one(root: Path, plan: Mapping[str, Any], unit: Mapping[str, Any]) -> dict[str, Any]:
    dbn, sidecar = _candidate_paths(root, plan, unit)
    if dbn.exists() and sidecar.exists():
        return _verify_one(root, plan, unit)
    if dbn.exists() or sidecar.exists():
        dbn.unlink(missing_ok=True)
        sidecar.unlink(missing_ok=True)
    source_paths = [
        _verify_source_artifact(root, artifact) for artifact in unit["source_dbns"]
    ]
    for row in unit["source_ledger"]:
        _verify_source_artifact(root, row["sidecar"])
    coverage = unit["coverage"]
    try:
        result = merge_dbn_sources(
            source_paths,
            dbn,
            expected_start_ns=_iso_ns(str(coverage["start_inclusive"])),
            expected_end_ns=_iso_ns(str(coverage["end_exclusive"])),
            require_contiguous=bool(unit["require_contiguous_source_metadata"]),
            maximum_output_bytes=int(unit["maximum_output_bytes"]),
        )
        payload = _annual_sidecar(unit, result, dbn)
        _write_new_or_exact(sidecar, payload)
    except Exception:
        Path(f"{dbn}.partial").unlink(missing_ok=True)
        dbn.unlink(missing_ok=True)
        sidecar.unlink(missing_ok=True)
        raise
    return _verify_one(root, plan, unit)


def _source_sequence(root: Path, unit: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str]:
    digest = sha256()
    rows: list[dict[str, Any]] = []
    for artifact in unit["source_dbns"]:
        path = _verify_source_artifact(root, artifact)
        result = _stream_records(path, sequence_digest=digest)
        if rows:
            _require_record_boundary(rows[-1], result)
        rows.append(result)
    return rows, digest.hexdigest()


def _verify_one(root: Path, plan: Mapping[str, Any], unit: Mapping[str, Any]) -> dict[str, Any]:
    dbn, sidecar = _candidate_paths(root, plan, unit)
    payload = _json(sidecar, "annual candidate sidecar")
    core = {key: value for key, value in payload.items() if key != "sidecar_id"}
    if payload.get("sidecar_id") != sha256_json(core):
        raise IntegrityError("annual candidate sidecar identity differs")
    sources, combined = _source_sequence(root, unit)
    merged = _stream_records(dbn)
    if (
        merged["record_count"] != sum(item["record_count"] for item in sources)
        or merged["record_sequence_sha256"] != combined
        or payload["record_integrity"]["merged_record_sequence_sha256"] != combined
        or payload["canonical_dbn"]["sha256"] != sha256_file(dbn)
        or payload["canonical_dbn"]["size_bytes"] != dbn.stat().st_size
    ):
        raise IntegrityError("annual candidate record sequence or byte identity differs")
    metadata_rows = [_metadata(root / artifact["path"]) for artifact in unit["source_dbns"]]
    coverage = unit["coverage"]
    start_ns = _iso_ns(str(coverage["start_inclusive"]))
    end_ns = _iso_ns(str(coverage["end_exclusive"]))
    _require_compatible(
        *metadata_rows,
        expected_start_ns=start_ns,
        expected_end_ns=end_ns,
        require_contiguous=bool(unit["require_contiguous_source_metadata"]),
    )
    output = _metadata(dbn)
    if output["start"] != start_ns or output["end"] != end_ns or output["version"] != 3:
        raise IntegrityError("annual candidate metadata coverage differs")
    return {
        "unit_key": unit["unit_key"],
        "candidate_dbn": dbn.relative_to(root).as_posix(),
        "candidate_sidecar": sidecar.relative_to(root).as_posix(),
        "candidate_dbn_size_bytes": dbn.stat().st_size,
        "candidate_dbn_sha256": sha256_file(dbn),
        "candidate_sidecar_sha256": sha256_file(sidecar),
        "record_count": merged["record_count"],
        "record_sequence_sha256": combined,
        "first_timestamp_ns": merged["first_timestamp_ns"],
        "last_timestamp_ns": merged["last_timestamp_ns"],
        "metadata_compatibility_verified": True,
        "source_lineage_verified": True,
        "successful_decode": True,
    }


def _selected_units(plan: Mapping[str, Any], unit_keys: Sequence[str]) -> list[dict[str, Any]]:
    requested = tuple(unit_keys)
    if len(requested) != len(set(requested)):
        raise ContractError("annual unit selection contains duplicates")
    by_key = {str(unit["unit_key"]): unit for unit in plan["units"]}
    if not requested or not set(requested) <= set(by_key):
        raise ContractError("annual unit selection is empty or unknown")
    return [by_key[key] for key in requested]


def build_candidates(
    root: Path,
    plan_path: Path,
    unit_keys: Sequence[str],
    *,
    maximum_workers: int = 1,
) -> dict[str, Any]:
    """Build selected immutable candidates; call only inside the approved task gate."""

    root = root.resolve(strict=True)
    plan = load_plan(root, plan_path)
    _validate_bindings(root, plan, implementation=True)
    units = _selected_units(plan, unit_keys)
    if maximum_workers not in (1, 2):
        raise ContractError("annual candidate construction allows one or two workers")
    required = sum(int(unit["maximum_output_bytes"]) for unit in units) + FREE_SPACE_RESERVE_BYTES
    if shutil.disk_usage(root).free < required:
        raise UnauthorizedOperation("annual candidate free-space floor failed")
    lock = SingleInstance(root / LOCK_PATH)
    if not lock.acquire():
        raise UnauthorizedOperation("another annual consolidation writer is running")
    try:
        if maximum_workers == 1:
            results = [_build_one(root, plan, unit) for unit in units]
        else:
            with ThreadPoolExecutor(max_workers=maximum_workers) as pool:
                results = list(pool.map(lambda unit: _build_one(root, plan, unit), units))
    finally:
        lock.release()
    core = {
        "schema_version": "annual_dbn_candidate_build/1.0.0",
        "plan_id": plan["plan_id"],
        "unit_keys": list(unit_keys),
        "maximum_workers": maximum_workers,
        "results": results,
        "provider_calls": 0,
        "canonical_mutations": 0,
        "deletions": 0,
    }
    report = {**core, "build_id": sha256_json(core)}
    path = root / STAGE_ROOT / str(plan["plan_id"]) / "builds" / f"{report['build_id']}.json"
    _write_new_or_exact(path, report)
    return {**report, "build_path": path.relative_to(root).as_posix()}


def verify_candidates(root: Path, plan_path: Path, unit_keys: Sequence[str]) -> dict[str, Any]:
    """Independently reread every selected source and candidate record."""

    root = root.resolve(strict=True)
    plan = load_plan(root, plan_path)
    _validate_bindings(root, plan, implementation=True)
    units = _selected_units(plan, unit_keys)
    results = [_verify_one(root, plan, unit) for unit in units]
    all_keys = {str(unit["unit_key"]) for unit in plan["units"]}
    selected = set(unit_keys)
    status = (
        "PASS_COMPLETE_68_ANNUAL_UNITS"
        if selected == all_keys
        else "PASS_HE_2026_PILOT"
        if selected == {"tbbo:HE:2026", "bbo_1s:HE:2026"}
        else "PASS_PARTIAL_SELECTION"
    )
    core = {
        "schema_version": "annual_dbn_candidate_validation/1.0.0",
        "status": status,
        "plan_id": plan["plan_id"],
        "unit_keys": list(unit_keys),
        "unit_count": len(results),
        "results": results,
        "complete_record_sequences_verified": True,
        "provider_calls": 0,
        "canonical_mutations": 0,
        "deletions": 0,
    }
    certificate = {**core, "certificate_id": sha256_json(core)}
    path = (
        root / STAGE_ROOT / str(plan["plan_id"]) / "validations"
        / f"{certificate['certificate_id']}.json"
    )
    _write_new_or_exact(path, certificate)
    return {**certificate, "certificate_path": path.relative_to(root).as_posix()}


def seal_full_candidate(root: Path, plan_path: Path, certificate_path: Path) -> dict[str, Any]:
    """Seal successor release, inventories, and catalogs without publishing them."""

    root = root.resolve(strict=True)
    plan = load_plan(root, plan_path)
    _validate_bindings(root, plan, implementation=True)
    path = certificate_path if certificate_path.is_absolute() else root / certificate_path
    certificate = _json(path.resolve(strict=True), "annual validation certificate")
    if (
        certificate.get("status") != "PASS_COMPLETE_68_ANNUAL_UNITS"
        or certificate.get("plan_id") != plan["plan_id"]
        or certificate.get("unit_count") != EXPECTED_UNIT_COUNT
    ):
        raise IntegrityError("full annual validation certificate is incomplete")
    payload = root / STAGE_ROOT / str(plan["plan_id"]) / "payload"
    logical_paths: dict[str, str] = {}
    result_by_key = {item["unit_key"]: item for item in certificate["results"]}
    for unit in plan["units"]:
        dbn, sidecar = _candidate_paths(root, plan, unit)
        logical_paths[dbn.relative_to(payload).as_posix()] = unit["destination_dbn"]
        logical_paths[sidecar.relative_to(payload).as_posix()] = unit["destination_sidecar"]
    release = DataReleaseManifest.build(
        payload,
        phase="dbn",
        release_kind="annual_tbbo_bbo_1s_provider_free_successor",
        schema_version="1.0.0",
        logical_paths=logical_paths,
        source_release_ids=tuple(
            str(SOURCE_RELEASES[family]["release_id"]) for family in ("tbbo", "bbo_1s")
        ),
        embedded_documents={
            "validation_certificate": {
                "id": certificate["certificate_id"],
                "sha256": sha256_file(path),
            }
        },
        metadata={
            "annual_dbn_count": 68,
            "annual_sidecar_count": 68,
            "tbbo_market_year_count": 32,
            "bbo_1s_market_year_count": 36,
            "provider_calls": 0,
            "research_or_trading_authority": False,
        },
    )
    controls = root / STAGE_ROOT / str(plan["plan_id"]) / "controls"
    release_payload = release.as_dict()
    _write_new_or_exact(controls / "successor_release_manifest.json", release_payload)
    release_entries = {entry.logical_path: entry for entry in release.files}
    bbo_entries: list[dict[str, Any]] = []
    tbbo_entries: list[dict[str, Any]] = []
    for unit in plan["units"]:
        result = result_by_key[str(unit["unit_key"])]
        output = release_entries[str(unit["destination_dbn"])]
        if unit["family"] == "bbo_1s":
            for row in unit["source_ledger"]:
                source = row["source_disposition"]
                trade_date = str(source["trade_date"])
                bbo_entries.append({
                    "market": unit["market"],
                    "trade_date": trade_date,
                    "annual_logical_path": unit["destination_dbn"],
                    "annual_sha256": output.sha256,
                    "annual_size_bytes": output.size,
                    "filter_start_inclusive_ns": _iso_ns(trade_date),
                    "filter_end_exclusive_ns": _iso_ns(
                        (date.fromordinal(
                            date.fromisoformat(trade_date).toordinal() + 1
                        )).isoformat()
                    ),
                    "timestamp_field": "ts_recv",
                    "provider_condition": source["provider_condition"],
                    "disposition": source["disposition"],
                    "source_eligible": source["source_eligible"],
                    "row_quality_certified": source["row_quality_certified"],
                    "source_request_id": source["request_id"],
                    "source_sidecar_id": row["sidecar_id"],
                })
        else:
            tbbo_entries.append({
                "market": unit["market"],
                "year": unit["year"],
                "annual_logical_path": unit["destination_dbn"],
                "annual_sha256": output.sha256,
                "annual_size_bytes": output.size,
                "coverage": unit["coverage"],
                "record_count": result["record_count"],
                "record_sequence_sha256": result["record_sequence_sha256"],
                "monthly_lineage": unit["source_ledger"],
            })
    bbo_core = {
        "schema_version": "annual_bbo_1s_eligibility_catalog/1.0.0",
        "state": "STAGED_SUCCESSOR_NOT_ACTIVE",
        "release_id": release.release_id,
        "entries": bbo_entries,
        "entry_count": len(bbo_entries),
        "eligible_count": sum(item["source_eligible"] is True for item in bbo_entries),
        "daily_filter_required": True,
        "alpha_source_authorized": False,
    }
    bbo_catalog = {**bbo_core, "catalog_id": sha256_json(bbo_core)}
    tbbo_core = {
        "schema_version": "annual_tbbo_source_catalog/1.0.0",
        "state": "STAGED_SUCCESSOR_NOT_ACTIVE",
        "release_id": release.release_id,
        "entries": tbbo_entries,
        "entry_count": len(tbbo_entries),
        "monthly_diagnostic_lineage_retained": True,
        "alpha_source_authorized": False,
    }
    tbbo_catalog = {**tbbo_core, "catalog_id": sha256_json(tbbo_core)}
    _write_new_or_exact(controls / "bbo_1s_eligibility_catalog.json", bbo_catalog)
    _write_new_or_exact(controls / "tbbo_source_catalog.json", tbbo_catalog)
    inventory_core = {
        "schema_version": "annual_dbn_candidate_inventory/1.0.0",
        "release_id": release.release_id,
        "entries": [
            {"path": entry.logical_path, "size_bytes": entry.size, "sha256": entry.sha256}
            for entry in release.files
        ],
        "file_count": len(release.files),
        "total_bytes": sum(entry.size for entry in release.files),
    }
    inventory = {**inventory_core, "inventory_id": sha256_json(inventory_core)}
    _write_new_or_exact(controls / "annual_candidate_inventory.json", inventory)
    contract_core = {
        "schema_version": "annual_dbn_source_contract/1.0.0",
        "state": "STAGED_SUCCESSOR_NOT_ACTIVE",
        "predecessor_active_dbn_pointer_sha256": plan["bindings"]["active_dbn_pointer_sha256"],
        "predecessor_source_contract_sha256": plan["bindings"]["active_source_contract_sha256"],
        "annual_release_id": release.release_id,
        "annual_inventory_id": inventory["inventory_id"],
        "bbo_1s_catalog_id": bbo_catalog["catalog_id"],
        "tbbo_catalog_id": tbbo_catalog["catalog_id"],
        "candidate_file_count": EXPECTED_OUTPUT_FILE_COUNT,
        "provider_calls": 0,
        "research_or_trading_authority": False,
    }
    contract = {**contract_core, "contract_id": sha256_json(contract_core)}
    _write_new_or_exact(controls / "annual_source_contract.json", contract)
    return {
        "status": "PASS_FULL_CANDIDATE_SEALED_NOT_ACTIVE",
        "release_id": release.release_id,
        "release_manifest_path": (controls / "successor_release_manifest.json").relative_to(root).as_posix(),
        "inventory_id": inventory["inventory_id"],
        "bbo_1s_catalog_id": bbo_catalog["catalog_id"],
        "tbbo_catalog_id": tbbo_catalog["catalog_id"],
        "controls_path": controls.relative_to(root).as_posix(),
    }


def resolve_bbo_1s_date(
    *, root: Path, catalog_path: Path, market: str, trade_date: str,
    candidate_payload_root: Path | None = None,
) -> dict[str, Any]:
    catalog = _json(catalog_path, "annual BBO-1s catalog")
    entries = [
        item for item in catalog.get("entries", ())
        if item.get("market") == market and item.get("trade_date") == trade_date
    ]
    if len(entries) != 1:
        raise IntegrityError("annual BBO-1s date has no unique catalog disposition")
    entry = entries[0]
    if entry["source_eligible"] is not True:
        return {"source": None, "disposition": entry["disposition"], "eligible": False}
    base = candidate_payload_root or root / "data/dbn"
    relative = Path(str(entry["annual_logical_path"])).relative_to("data/dbn")
    source = base / relative
    if source.stat().st_size != entry["annual_size_bytes"] or sha256_file(source) != entry["annual_sha256"]:
        raise IntegrityError("annual BBO-1s resolved source differs")
    return {
        "source": source,
        "eligible": True,
        "disposition": entry["disposition"],
        "timestamp_field": "ts_recv",
        "start_inclusive_ns": entry["filter_start_inclusive_ns"],
        "end_exclusive_ns": entry["filter_end_exclusive_ns"],
    }


def resolve_tbbo_year(
    *, root: Path, catalog_path: Path, market: str, year: int,
    candidate_payload_root: Path | None = None,
) -> dict[str, Any]:
    catalog = _json(catalog_path, "annual TBBO catalog")
    entries = [
        item for item in catalog.get("entries", ())
        if item.get("market") == market and item.get("year") == year
    ]
    if len(entries) != 1:
        raise IntegrityError("annual TBBO market-year is not uniquely cataloged")
    entry = entries[0]
    base = candidate_payload_root or root / "data/dbn"
    source = base / Path(str(entry["annual_logical_path"])).relative_to("data/dbn")
    if source.stat().st_size != entry["annual_size_bytes"] or sha256_file(source) != entry["annual_sha256"]:
        raise IntegrityError("annual TBBO resolved source differs")
    return {"source": source, "coverage": entry["coverage"], "monthly_lineage": entry["monthly_lineage"]}


__all__ = [
    "BBO_MARKETS",
    "INTERVALS",
    "TBBO_MARKETS",
    "build_candidates",
    "build_plan",
    "load_plan",
    "resolve_bbo_1s_date",
    "resolve_tbbo_year",
    "seal_full_candidate",
    "verify_candidates",
]
