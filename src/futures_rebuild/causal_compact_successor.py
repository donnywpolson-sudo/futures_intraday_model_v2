"""Prepare the recovered 41-market active view; publication is a separate task.

Only certified development Parquet is read. Completed annual files are reusable
after hash verification; unfinished task-owned files are quarantined, not deleted.
"""
from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import fields, replace
from pathlib import Path

import pyarrow.parquet as pq

from .active_data_certification import canonical_parquet_fingerprint, canonical_parquet_sequence_fingerprint
from .active_data_view import (
    ACTIVE_ROOT, STAGING_ROOT, ROLLBACK_ROOT, COHORT_PERMISSIONS, CERTIFICATION_STATE,
    CatalogEntry, UpdateMode, build_plan, build_catalog, build_sidecar,
    build_content_validation_receipt, build_access_policy_binding,
    validate_access_policy_binding, cohort_for_coverage, materialize_parquet, verify_view,
)
from .canonical import io_path, sha256_file, sha256_json, write_new_canonical_authority
from .causal_incremental_release import _coverage
from .causal_market_binding_compatibility import audit_campaign
from .errors import IntegrityError
from .locking import FileLease

REPORT_ROOT = Path("reports/causal_compact_successor_v1")
RECOVERY_ROOT = Path("reports/gate_b_binding_compatibility_v1")
MODULE = "src/futures_rebuild/causal_compact_successor.py"
FREE_FLOOR = 64 * 1024**3
MAXIMUM_SECONDS = 12 * 60 * 60


def read(path):
    return json.loads(io_path(path).read_bytes())


def require(ok, message):
    if not ok:
        raise IntegrityError(message)


def sign(value, key):
    return {**value, key: sha256_json(value)}


def catalog_entry(raw):
    values = {field.name: raw[field.name] for field in fields(CatalogEntry)}
    for key in ("permitted_uses", "source_bindings"):
        values[key] = tuple(values[key])
    return CatalogEntry(**values)


def verify_files(root, bindings):
    for path, digest in bindings.items():
        require(sha256_file(io_path(root / path)) == digest, f"bound input changed: {path}")


def sources_by_year(certificates):
    units = []
    for certificate in certificates:
        years = {}
        for item in certificate["replay_evidence"]["output_inventory"]:
            if item["path"].endswith("/observations.parquet"):
                parts = item["path"].split("/")
                year = int(parts[parts.index("sealed") + 1])
                years.setdefault(year, []).append(item)
        for year, items in sorted(years.items()):
            items.sort(key=lambda item: item["path"])
            start, end = _coverage(items)
            cohort = cohort_for_coverage(year=year, coverage_start=start, coverage_end=end)
            require(cohort not in {"LOCKED_HOLDOUT", "FORWARD_ONLY"}, "protected coverage cannot be materialized")
            units.append({"market": certificate["market"], "year": year, "sources": items,
                          "coverage_start": start, "coverage_end": end,
                          "certificate_id": certificate["certificate_id"], "cohort": cohort})
    return sorted(units, key=lambda unit: (unit["market"], unit["year"]))


def prepare_plan(root):
    baseline = read(root / RECOVERY_ROOT / "baseline_audit.json")
    manifest, certificates = audit_campaign(root, saved_snapshots=baseline["snapshots"])
    require(manifest == read(root / RECOVERY_ROOT / "compatibility_manifest.json"), "recovered evidence drifted")
    result = read(root / RECOVERY_ROOT / "result.json")
    verification = read(root / RECOVERY_ROOT / "independent_validation.json")
    complete_path = result["complete_certificate_path"]
    complete = read(root / complete_path)
    require(sha256_file(root / complete_path) == result["complete_certificate_sha256"]
            and complete["certificate_id"] == verification["certificate_id"]
            and verification["status"] == "PASS_INDEPENDENT_COMPLETE_SET_VERIFICATION", "recovered certificate is not verified")
    current = verify_view(root / ACTIVE_ROOT)
    units = sources_by_year(certificates)
    keys = {(u["market"], u["year"]) for u in units}
    require(len(units) == len(keys) == 609 and len({u["market"] for u in units}) == 41, "compact source coverage differs")
    protected = [e for e in current["entries"] if e["disposition"] != CERTIFICATION_STATE]
    require(not keys.intersection((e["market"], e["year"]) for e in protected), "protected predecessor entry overlaps input")
    for unit in units:
        unit["row_count"] = sum(pq.read_metadata(io_path(root / f["path"])).num_rows for f in unit["sources"])
    require(sum(u["row_count"] for u in units) == complete["total_observation_count"], "source footer totals differ")
    source_bytes = sum(f["size"] for u in units for f in u["sources"])
    semantic_paths = sorted(set(current["semantic_bindings"]) | {"configs/active_data_view_contract.json", "configs/research_universe_contract.json",
                      "configs/source_contract.json", complete_path,
                      (RECOVERY_ROOT / "compatibility_manifest.json").as_posix(),
                      (RECOVERY_ROOT / "independent_validation.json").as_posix()})
    implementation_paths = [MODULE, "src/futures_rebuild/active_data_view.py",
                            "src/futures_rebuild/active_data_certification.py", "src/futures_rebuild/canonical.py",
                            "src/futures_rebuild/causal_incremental_release.py"]
    bindings = {p: sha256_file(root / p) for p in semantic_paths + implementation_paths
                + ["configs/environment.lock.json", "requirements.lock"]}
    # A successor binds current policy; the saved predecessor retains historical hashes.
    semantic = {p: bindings[p] for p in semantic_paths}
    core = {"schema_version": "causal_compact_preparation/1.0.0", "units": units,
            "protected_entries": protected, "source_bytes": source_bytes,
            "policy_binding_changes": {p: {"predecessor": digest, "successor": bindings[p]}
                                       for p, digest in current["semantic_bindings"].items() if digest != bindings[p]},
            "expected_rows": complete["total_observation_count"], "source_certificate_id": complete["certificate_id"],
            "source_certificate_path": complete_path, "source_release_id": complete["source_release_id"],
            "bindings": {**bindings, **semantic}, "semantic_bindings": semantic,
            "implementation_bindings": {p: bindings[p] for p in implementation_paths},
            "environment_bindings": {p: bindings[p] for p in ("configs/environment.lock.json", "requirements.lock")},
            "predecessor": {"active_view_id": current["active_view_id"], "catalog_sha256": current["catalog_sha256"],
                            "catalog_file_sha256": sha256_file(root / ACTIVE_ROOT / "catalog.json")},
            "maximum_staging_bytes": source_bytes * 2 + 256 * 1024**2,
            "minimum_free_bytes": FREE_FLOOR, "maximum_runtime_seconds": MAXIMUM_SECONDS,
            "publication_authorized": False, "deletion_authorized": False}
    return sign(core, "preparation_id"), current


def materialize_unit(root, stage, records, unit, plan, active_view_id):
    """Persist one hash-verified annual checkpoint outside the publishable tree."""
    market, year = unit["market"], unit["year"]
    output = stage / "causally_gated_normalized" / market / str(year) / f"{year}.parquet"
    sidecar_path = output.with_suffix(".parquet.manifest.json")
    checkpoint = records / market / f"{year}.json"
    if checkpoint.exists():
        done = read(checkpoint)
        require(done["record_id"] == sha256_json({k: v for k, v in done.items() if k != "record_id"})
                and done["unit_id"] == sha256_json(unit), "annual checkpoint identity differs")
        entry = catalog_entry(done["entry"])
        require(sha256_file(output) == entry.parquet_sha256 and sha256_file(sidecar_path) == entry.sidecar_sha256,
                "completed annual file changed")
        return entry, done
    # A failed current year is not certified. Preserve its exact bytes in quarantine
    # before a fresh attempt; completed annual checkpoints never enter this branch.
    for path in (output, sidecar_path):
        if path.exists():
            target = records / "interrupted" / market / str(year) / str(time.time_ns()) / path.name
            target.parent.mkdir(parents=True, exist_ok=False)
            require(path.resolve().is_relative_to(stage.resolve()) and target.resolve().is_relative_to(records.resolve()),
                    "interrupted annual path escapes task")
            os.replace(path, target)
    started = time.monotonic()
    paths = [io_path(root / item["path"]) for item in unit["sources"]]
    verify_files(root, {item["path"]: item["sha256"] for item in unit["sources"]})
    fingerprint = canonical_parquet_sequence_fingerprint(paths)
    require(fingerprint["row_count"] == unit["row_count"], "monthly source row count differs")
    output_hash, row_hash, rows = materialize_parquet(
        sources=paths, source_sha256s=[f["sha256"] for f in unit["sources"]], destination=output,
        expected_row_count=unit["row_count"], expected_schema_fingerprint=fingerprint["schema_fingerprint"])
    require(row_hash == fingerprint["canonical_row_hash"], "annual content differs from monthly source")
    receipt = build_content_validation_receipt(
        market=market, year=year, coverage_start=unit["coverage_start"], coverage_end=unit["coverage_end"],
        source_bindings=unit["sources"], semantic_bindings=plan["semantic_bindings"],
        implementation_bindings=plan["implementation_bindings"], environment_bindings=plan["environment_bindings"],
        canonical_schema_fingerprint=fingerprint["schema_fingerprint"], canonical_row_hash=row_hash, row_count=rows,
        checks={"recovered_certificate_chain": "PASS", "monthly_to_annual_canonical_content": "PASS",
                "row_count_schema_order_and_source_hashes": "PASS"},
        limitations=("CERTIFIED_DEVELOPMENT_PREFIX_ONLY", "NO_NEW_RESEARCH_EXECUTION_AUTHORITY", "NO_DBN_REPLAY"))
    access = build_access_policy_binding(
        market=market, year=year, universe_contract_sha256=plan["bindings"]["configs/research_universe_contract.json"],
        active_view_id=active_view_id, content_validation_receipt_id=receipt["content_validation_receipt_id"],
        coverage_start=unit["coverage_start"], coverage_end=unit["coverage_end"])
    cohort = unit["cohort"]
    entry = CatalogEntry(
        market=market, year=year, coverage_start=unit["coverage_start"], coverage_end=unit["coverage_end"],
        coverage_kind="CERTIFIED_V10_PARTIAL_2025_DEVELOPMENT_SOURCE" if year == 2025 else "CERTIFIED_V10_DEVELOPMENT_SOURCE",
        cohort=cohort, disposition=CERTIFICATION_STATE, selection_eligible=cohort == "DISCOVERY_SELECTION",
        permitted_uses=COHORT_PERMISSIONS[cohort], source_bindings=tuple(unit["sources"]),
        content_validation_receipt_id=receipt["content_validation_receipt_id"],
        access_policy_binding_id=validate_access_policy_binding(access), parquet_sha256=output_hash,
        sidecar_sha256="0" * 64, row_count=rows, schema_fingerprint=fingerprint["schema_fingerprint"])
    write_new_canonical_authority(sidecar_path, build_sidecar(entry=entry, content_receipt=receipt, access_binding=access))
    entry = replace(entry, sidecar_sha256=sha256_file(sidecar_path))
    record = sign({"unit_id": sha256_json(unit), "entry": entry.as_dict(), "fingerprint": fingerprint,
                   "elapsed_seconds": time.monotonic() - started, "output_bytes": output.stat().st_size + sidecar_path.stat().st_size}, "record_id")
    write_new_canonical_authority(checkpoint, record)
    return entry, record


def execute_preparation(root):
    started = time.monotonic()
    root = root.resolve()
    plan, current = prepare_plan(root)
    identity = plan["preparation_id"]
    report = root / REPORT_ROOT / identity
    stage = root / STAGING_ROOT / identity
    records = report / "annual_checkpoints"
    report.mkdir(parents=True, exist_ok=True)
    write_new_canonical_authority(report / "preparation_plan.json", plan)
    write_new_canonical_authority(report / "predecessor_catalog.json", current)
    active_view_id = sha256_json({"preparation_id": identity, "source_certificate_id": plan["source_certificate_id"]})
    with FileLease(report / "preparation.lock"):
        if (report / "result.json").exists():
            result = read(report / "result.json")
            verify_files(root, plan["bindings"])
            catalog = verify_view(stage)
            publication = read(report / "publication_plan.json")
            require(result["preparation_id"] == identity
                    and catalog["catalog_sha256"] == publication["publication_input_binding"]["catalog_sha256"],
                    "completed preparation identity differs")
            print(json.dumps(result), flush=True)
            return result
        stage.mkdir(parents=True, exist_ok=True)
        entries, done = [catalog_entry(e) for e in plan["protected_entries"]], []
        for unit in plan["units"]:
            require(time.monotonic() - started < MAXIMUM_SECONDS, "preparation runtime ceiling reached; checkpoints preserved")
            verify_files(root, plan["bindings"])
            require(sha256_file(root / ACTIVE_ROOT / "catalog.json") == plan["predecessor"]["catalog_file_sha256"], "active predecessor changed")
            used = sum(p.stat().st_size for p in stage.rglob("*") if p.is_file())
            reserve = sum(f["size"] for f in unit["sources"]) * 2 + 64 * 1024**2
            require(used + reserve <= plan["maximum_staging_bytes"] and shutil.disk_usage(root).free >= FREE_FLOOR + reserve,
                    "staging storage ceiling or free-space floor reached")
            entry, record = materialize_unit(root, stage, records, unit, plan, active_view_id)
            entries.append(entry)
            done.append(record)
            print(f"VERIFIED {len(done)}/609 {unit['market']}/{unit['year']} rows={entry.row_count} seconds={record['elapsed_seconds']:.2f}", flush=True)
            if len(done) == 1:
                seconds = record["elapsed_seconds"] * plan["expected_rows"] / entry.row_count
                estimate = {"basis": "FIRST_COMPLETED_MARKET_YEAR_LINEAR_ROW_PROJECTION_UNCERTAIN",
                            "expected_seconds": round(seconds), "conservative_seconds": round(seconds * 2),
                            "hard_runtime_limit_seconds": MAXIMUM_SECONDS,
                            "maximum_staging_bytes": plan["maximum_staging_bytes"], "minimum_free_bytes": FREE_FLOOR,
                            "measured_first_year_seconds": record["elapsed_seconds"], "existing_active_preserved_as_rollback": True}
                write_new_canonical_authority(report / "sizing.json", estimate)
                print(json.dumps(estimate), flush=True)
        require(sum(e.row_count or 0 for e in entries) == plan["expected_rows"], "annual row total differs")
        catalog = build_catalog(active_view_id=active_view_id, plan_id=identity,
                                foundation_release_id=plan["source_release_id"],
                                foundation_manifest_sha256=plan["bindings"][plan["source_certificate_path"]],
                                semantic_bindings=plan["semantic_bindings"], entries=sorted(entries, key=lambda e: (e.market, e.year)))
        write_new_canonical_authority(stage / "catalog.json", catalog)
        verify_view(stage)
        # A separate full readback checks the persisted annual files against source
        # fingerprints. Resume never treats a file's existence alone as a PASS.
        for record in done:
            require(time.monotonic() - started < MAXIMUM_SECONDS, "readback runtime ceiling reached; checkpoints preserved")
            raw = record["entry"]
            path = stage / raw["parquet_path"].removeprefix("data/active/")
            require(canonical_parquet_fingerprint(io_path(path)) == record["fingerprint"], "independent annual readback differs")
        verify_files(root, plan["bindings"])
        require(sha256_file(root / ACTIVE_ROOT / "catalog.json") == plan["predecessor"]["catalog_file_sha256"], "active catalog drifted")
        files = sorted(p for p in stage.rglob("*") if p.is_file())
        size = sum(p.stat().st_size for p in files)
        publication = build_plan(
            operation="PUBLISH_CAUSAL_ACTIVE_VIEW", mode=UpdateMode.FULL_SUCCESSOR,
            foundation_release_id=plan["source_release_id"], foundation_manifest_sha256=catalog["foundation_manifest_sha256"],
            semantic_bindings=plan["semantic_bindings"], entries=catalog["entries"],
            limits={"maximum_active_files": len(files), "maximum_catalog_entries": len(entries),
                    "maximum_input_bytes": size, "maximum_input_files": len(files), "maximum_publications": 1,
                    "maximum_retries": 0, "maximum_workers": 1},
            forbidden_actions=("DBN_REPLAY", "SOURCE_V10_MUTATION", "HOLDOUT_OR_FORWARD_ACCESS", "DELETION", "PROVIDER_ACCESS", "TRADING"),
            outputs=("data/active", "state/active_data_view_publication"),
            implementation_bindings=plan["implementation_bindings"], environment_bindings=plan["environment_bindings"],
            recovery_boundary="FULL_SUCCESSOR_DIRECTORY_TRANSACTION_PREDECESSOR_RETAINED")
        core = {k: v for k, v in publication.items() if k != "plan_id"}
        core["publication_input_binding"] = {"staging_path": stage.relative_to(root).as_posix(),
            "active_view_id": active_view_id, "catalog_sha256": catalog["catalog_sha256"],
            "catalog_file_sha256": sha256_file(stage / "catalog.json"),
            "predecessor_active_view_id": current["active_view_id"], "predecessor_catalog_sha256": current["catalog_sha256"]}
        publication = sign(core, "plan_id")
        write_new_canonical_authority(report / "publication_plan.json", publication)
        result = {"status": "VERIFIED_STAGED_SUCCESSOR_AWAITING_CUTOVER_APPROVAL", "preparation_id": identity,
                  "publication_plan_id": publication["plan_id"], "staging_path": stage.relative_to(root).as_posix(),
                  "market_count": 41, "annual_files": 609, "catalog_entries": len(entries), "output_files": len(files),
                  "output_bytes": size, "row_count": plan["expected_rows"], "canonical_readback_passed": True,
                  "predecessor": plan["predecessor"], "rollback_path": (ROLLBACK_ROOT / current["active_view_id"]).as_posix(),
                  "active_catalog_unchanged": True, "publication_performed": False, "deletions": 0,
                  "dbn_replays": 0, "elapsed_seconds": round(time.monotonic() - started, 3)}
        write_new_canonical_authority(report / "result.json", result)
        print(json.dumps(result), flush=True)
    return result


if __name__ == "__main__":
    execute_preparation(Path.cwd())
