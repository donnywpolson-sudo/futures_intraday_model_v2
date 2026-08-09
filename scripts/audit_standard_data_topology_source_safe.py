"""Audit standard-lane provenance and folder roles without opening row payloads."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Mapping

from futures_rebuild.active_data_view import validate_catalog
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = Path("data/active/catalog.json")
SOURCE_CONTRACT_PATH = Path("configs/source_contract.json")
FOUNDATION_ROOT = Path("manifests/data_releases/foundation")
DBN_RELEASE_ROOT = Path("manifests/data_releases/dbn")
ACTIVE_ROOT = Path("data/active/causally_gated_normalized")
PHASE2_ROOT = Path("data/causally_gated_normalized")
PHASE1B_ROOT = Path("data/raw")
OUTPUT = Path(
    "state/unpublished_evidence/standard_data_topology_source_safe_audit/report.json"
)


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{description} is not an object")
    return value


def _plain_payload(path: Path, description: str) -> None:
    """Use filesystem metadata only; never open a DBN or parquet payload."""

    if not path.is_file() or path.is_symlink() or path.stat().st_size <= 0:
        raise IntegrityError(f"{description} is absent, link-like, or empty")


def _release_paths(
    *, market: str, year: int, start: str, end: str,
    causal_release_id: str, raw_release_id: str,
) -> tuple[tuple[Path, ...], tuple[Path, ...]]:
    interval = f"{start}_{end}"
    causal = ROOT / PHASE2_ROOT / market / str(year) / interval / causal_release_id
    raw = ROOT / PHASE1B_ROOT / market / str(year) / interval / raw_release_id
    return (
        (causal / "bars.parquet", causal / "causal_interval_receipt.json"),
        (raw / "bars.parquet", raw / "definitions.parquet", raw / "interval_receipt.json"),
    )


def _inventory(root: Path) -> dict[str, int]:
    files = [path for path in root.rglob("*") if path.is_file()]
    return {
        "file_count": len(files),
        "parquet_count": sum(path.suffix == ".parquet" for path in files),
        "json_count": sum(path.suffix == ".json" for path in files),
        "dbn_count": sum(path.name.endswith(".dbn.zst") for path in files),
        "byte_count_from_filesystem_metadata": sum(path.stat().st_size for path in files),
    }


def build_report(*, root: Path = ROOT) -> dict[str, object]:
    root = root.resolve(strict=True)
    catalog_path = root / CATALOG_PATH
    source_contract_path = root / SOURCE_CONTRACT_PATH
    catalog = _object(catalog_path, "active standard catalog")
    catalog_self_hash = validate_catalog(catalog)
    source_contract = _object(source_contract_path, "source contract")
    layout = source_contract.get("data_layout")
    if not isinstance(layout, Mapping) or (
        layout.get("phase1b_physical_template")
        != "data/raw/{market}/{year}/{interval}/{release-id}/{filename}"
        or layout.get("phase2_physical_template")
        != (
            "data/causally_gated_normalized/"
            "{market}/{year}/{interval}/{release-id}/{filename}"
        )
    ):
        raise IntegrityError("standard Phase 1B/2 layout contract drifted")

    foundation_release_id = str(catalog["foundation_release_id"])
    foundation_path = root / FOUNDATION_ROOT / f"{foundation_release_id}.json"
    if sha256_file(foundation_path) != catalog["foundation_manifest_sha256"]:
        raise IntegrityError("active catalog foundation manifest binding drifted")
    foundation = _object(foundation_path, "active foundation manifest")
    if foundation.get("release_id") != foundation_release_id:
        raise IntegrityError("active foundation release identity drifted")
    metadata = foundation.get("metadata")
    if not isinstance(metadata, Mapping):
        raise IntegrityError("active foundation metadata is absent")
    dbn_release_id = str(metadata.get("source_dbn_release_id"))
    dbn_manifest_path = root / DBN_RELEASE_ROOT / f"{dbn_release_id}.json"
    dbn_manifest = _object(dbn_manifest_path, "source DBN release manifest")
    if (
        dbn_manifest.get("release_id") != dbn_release_id
        or dbn_manifest.get("release_kind") != "futures_phase1a_verified_dbn"
    ):
        raise IntegrityError("active foundation source DBN release drifted")

    active_entries = [
        entry for entry in catalog["entries"]
        if isinstance(entry, dict)
        and entry.get("disposition") == "RESEARCH_READY_CAUSAL_PRICE"
    ]
    expected_active_files: set[str] = set()
    source_binding_count = 0
    content_check_counts: Counter[str] = Counter()
    markets: set[str] = set()
    years: set[int] = set()
    for entry in active_entries:
        market = str(entry["market"])
        year = int(entry["year"])
        markets.add(market)
        years.add(year)
        parquet_rel = str(entry["parquet_path"])
        sidecar_rel = str(entry["sidecar_path"])
        expected_prefix = f"data/active/causally_gated_normalized/{market}/{year}/"
        if (
            not parquet_rel.startswith(expected_prefix)
            or not sidecar_rel.startswith(expected_prefix)
            or sidecar_rel != parquet_rel + ".manifest.json"
        ):
            raise IntegrityError("active catalog points outside the canonical active root")
        parquet_path = root / parquet_rel
        sidecar_path = root / sidecar_rel
        _plain_payload(parquet_path, "active parquet payload")
        if sha256_file(sidecar_path) != entry["sidecar_sha256"]:
            raise IntegrityError("active sidecar hash differs from catalog")
        expected_active_files.update(
            {
                parquet_path.relative_to(root / ACTIVE_ROOT).as_posix(),
                sidecar_path.relative_to(root / ACTIVE_ROOT).as_posix(),
            }
        )
        sidecar = _object(sidecar_path, "active market-year sidecar")
        sidecar_core = dict(sidecar)
        sidecar_id = sidecar_core.pop("sidecar_id", None)
        expected_entry_binding = {
            key: value for key, value in entry.items() if key != "sidecar_sha256"
        }
        if (
            sidecar_id != sha256_json(sidecar_core)
            or sidecar.get("schema_version")
            != "causal_active_market_year_manifest/1.0.0"
            or sidecar.get("entry_binding") != expected_entry_binding
        ):
            raise IntegrityError("active market-year sidecar identity drifted")
        receipt = sidecar.get("content_validation_receipt")
        if not isinstance(receipt, Mapping):
            raise IntegrityError("active content-validation receipt is absent")
        receipt_core = dict(receipt)
        receipt_id = receipt_core.pop("content_validation_receipt_id", None)
        checks = receipt.get("checks")
        if (
            receipt_id != sha256_json(receipt_core)
            or receipt_id != entry["content_validation_receipt_id"]
            or not isinstance(checks, Mapping)
            or not checks
            or set(checks.values()) != {"PASS"}
        ):
            raise IntegrityError("active content-validation receipt drifted")
        content_check_counts.update(str(name) for name in checks)
        bindings = entry.get("source_bindings")
        if not isinstance(bindings, list) or not bindings:
            raise IntegrityError("active source binding is absent")
        for binding in bindings:
            if not isinstance(binding, Mapping):
                raise IntegrityError("active source binding is malformed")
            if binding.get("dbn_release_id") != dbn_release_id:
                raise IntegrityError("active source binding uses another DBN release")
            causal_paths, raw_paths = _release_paths(
                market=market,
                year=year,
                start=str(binding["start"]),
                end=str(binding["end"]),
                causal_release_id=str(binding["causal_release_id"]),
                raw_release_id=str(binding["raw_release_id"]),
            )
            for path in (*causal_paths, *raw_paths):
                _plain_payload(path, "bound immutable release artifact")
            source_binding_count += 1

    actual_active_files = {
        path.relative_to(root / ACTIVE_ROOT).as_posix()
        for path in (root / ACTIVE_ROOT).rglob("*")
        if path.is_file()
    }
    if actual_active_files != expected_active_files:
        raise IntegrityError("active folder contains files outside the catalog census")

    dispositions = dict(catalog["disposition_counts"])
    if (
        dispositions.get("LOCKED_HOLDOUT_NOT_MATERIALIZED") != 41
        or dispositions.get("FORWARD_ONLY_NOT_MATERIALIZED") != 41
        or any(
            entry.get("parquet_path") is not None or entry.get("sidecar_path") is not None
            for entry in catalog["entries"]
            if isinstance(entry, dict)
            and entry.get("disposition") != "RESEARCH_READY_CAUSAL_PRICE"
        )
    ):
        raise IntegrityError("holdout/forward or non-materialized catalog state drifted")

    core: dict[str, object] = {
        "schema_version": "standard_data_topology_source_safe_audit/1.0.0",
        "state": "PASS_SOURCE_SAFE_PROVENANCE_METADATA_ONLY",
        "lane": "standard/full-contract lane",
        "catalog": {
            "path": CATALOG_PATH.as_posix(),
            "file_sha256": sha256_file(catalog_path),
            "catalog_self_hash": catalog_self_hash,
            "active_view_id": catalog["active_view_id"],
            "entry_count": len(catalog["entries"]),
            "active_market_year_count": len(active_entries),
            "disposition_counts": dispositions,
        },
        "lineage": {
            "phase1a_dbn_release_id": dbn_release_id,
            "phase1a_dbn_manifest_path": dbn_manifest_path.relative_to(root).as_posix(),
            "phase1a_dbn_manifest_sha256": sha256_file(dbn_manifest_path),
            "phase2_foundation_release_id": foundation_release_id,
            "phase2_foundation_manifest_path": foundation_path.relative_to(root).as_posix(),
            "phase2_foundation_manifest_sha256": sha256_file(foundation_path),
            "active_source_binding_count": source_binding_count,
            "content_validation_check_counts": dict(sorted(content_check_counts.items())),
        },
        "folder_roles": {
            "immutable_phase1b_release_store": PHASE1B_ROOT.as_posix(),
            "immutable_phase2_release_store": PHASE2_ROOT.as_posix(),
            "authoritative_catalog_selected_view": ACTIVE_ROOT.as_posix(),
            "active_resolution_rule": "DATA_ACTIVE_CATALOG_ONLY_NO_DIRECT_ARCHIVE_GLOB",
        },
        "inventory": {
            "phase1b_release_store": _inventory(root / PHASE1B_ROOT),
            "phase2_release_store": _inventory(root / PHASE2_ROOT),
            "active_catalog_selected_view": _inventory(root / ACTIVE_ROOT),
        },
        "markets_observed": sorted(markets),
        "years_observed": sorted(years),
        "payload_safety": {
            "dbn_payloads_opened": 0,
            "parquet_payloads_opened": 0,
            "historical_rows_read": 0,
            "payload_sha256_recomputed": False,
            "year_2025_or_2026_payload_opened": False,
        },
        "conclusion": {
            "duplicate_named_roots_are_conflicting_active_sources": False,
            "top_level_phase2_root_role": "CONTENT_ADDRESSED_IMMUTABLE_RELEASE_HISTORY",
            "nested_active_phase2_root_role": "ONLY_CATALOG_SELECTED_RESEARCH_VIEW",
            "row_level_recertification_performed": False,
            "row_level_recertification_requires_separate_approval": True,
        },
    }
    return {**core, "report_id": sha256_json(core)}


def main() -> int:
    report = build_report()
    output = ROOT / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(report) + b"\n"
    if output.exists():
        if output.read_bytes() != raw:
            raise RuntimeError("existing topology audit report differs from live metadata")
    else:
        with output.open("xb") as stream:
            stream.write(raw)
    print(json.dumps({"report_id": report["report_id"], "state": report["state"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
