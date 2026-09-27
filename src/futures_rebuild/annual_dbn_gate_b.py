"""Prepare and publish the annual DBN Gate B custody successor."""

from __future__ import annotations

import copy
import json
import os
import shutil
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

from .annual_dbn_consolidation import (
    ACTIVE_CAUSAL_CATALOG,
    ACTIVE_DBN_POINTER,
    ACTIVE_SOURCE_CONTRACT,
    EXPECTED_OUTPUT_FILE_COUNT,
    STAGE_ROOT as ANNUAL_STAGE_ROOT,
    _validate_bindings as _validate_annual_bindings,
    load_plan as load_annual_plan,
)
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    canonical_authority_file_bytes,
    canonical_bytes,
    io_path,
    sha256_bytes,
    sha256_file,
    sha256_json,
    write_new_canonical_authority,
)
from .causal_observation_full_build import (
    EXPECTED_DBN_COUNT,
    EXPECTED_ENTRY_COUNT,
    EXPECTED_PAYLOAD_BYTES,
    EXPECTED_PRIMARY_1M_DBN_COUNT,
    EXPECTED_SIDECAR_COUNT,
    EXPECTED_SOURCE_BYTES,
    EXPECTED_WORK_UNIT_COUNT,
    MAXIMUM_OUTPUT_BYTES,
    MAXIMUM_PARTITION_COUNT,
    MAXIMUM_PEAK_ADDITIONAL_BYTES,
    MINIMUM_FREE_AFTER_PEAK_BYTES,
)
from .causal_source_closure import (
    select_preferred_standard_source_entries,
    validate_full_build_selection_contract,
    validate_source_contract_metadata,
)
from .data_layout import (
    DataReleaseManifest,
    PhasePublisher,
    _parse_manifest,
    manifest_relative_path,
    verify_data_release_manifest,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation_operation_firewall import issue_current_source_closure_context
from .l0_2026_dbn_consolidation import _atomic_replace
from .live_cockpit.single_instance import SingleInstance


GATE_ROOT: Final = Path("state/annual_dbn_gate_b_v1")
LOCK_PATH: Final = Path("state/locks/annual_dbn_gate_b_v1.lock")
SOURCE_V8: Final = Path("configs/source_contract_v8.json")
PUBLISH_OPERATION: Final = "PUBLISH_RELEASE"
EXPECTED_SUCCESSOR_FILES: Final = 11_610
EXPECTED_ANNUAL_DBNS: Final = 68
EXPECTED_FOUNDATION_REMOVED_DBNS: Final = 8


def _json(path: Path, label: str) -> dict[str, Any]:
    try:
        raw = io_path(path).read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise IntegrityError(f"{label} is not readable JSON") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"{label} is not canonical JSON")
    return value


def _write_new_or_exact(path: Path, payload: Mapping[str, Any]) -> None:
    expected = canonical_authority_file_bytes(payload)
    if path.exists():
        if path.read_bytes() != expected:
            raise IntegrityError(f"existing immutable artifact differs: {path}")
        return
    write_new_canonical_authority(path, payload)


def _write_bytes_new_or_exact(path: Path, payload: bytes) -> None:
    if path.exists():
        if path.read_bytes() != payload:
            raise IntegrityError(f"existing immutable byte snapshot differs: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with io_path(path).open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _relative(root: Path, path: Path) -> str:
    try:
        return path.resolve(strict=False).relative_to(root).as_posix()
    except ValueError as exc:
        raise ContractError(f"path escapes repository: {path}") from exc


def _identity(payload: Mapping[str, Any], field: str, label: str) -> None:
    observed = payload.get(field)
    core = {key: value for key, value in payload.items() if key != field}
    if type(observed) is not str or observed != sha256_json(core):
        raise IntegrityError(f"{label} identity differs")


def _tree_bytes(path: Path) -> int:
    total = 0
    for directory, directories, filenames in os.walk(path):
        directories.sort()
        for filename in sorted(filenames):
            total += io_path(Path(directory) / filename).stat().st_size
    return total


def _annual_controls(
    root: Path, annual_plan_path: Path, certificate_path: Path,
) -> tuple[
    dict[str, Any], dict[str, Any], DataReleaseManifest, dict[str, Any],
    dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Path],
]:
    annual_plan = load_annual_plan(root, annual_plan_path)
    _validate_annual_bindings(root, annual_plan, implementation=True)
    certificate = _json(certificate_path, "annual validation certificate")
    _identity(certificate, "certificate_id", "annual validation certificate")
    if (
        certificate.get("status") != "PASS_COMPLETE_68_ANNUAL_UNITS"
        or certificate.get("plan_id") != annual_plan["plan_id"]
        or certificate.get("unit_count") != 68
        or certificate.get("complete_record_sequences_verified") is not True
        or certificate.get("provider_calls") != 0
        or certificate.get("canonical_mutations") != 0
        or certificate.get("deletions") != 0
    ):
        raise IntegrityError("annual validation certificate is incomplete")
    controls = root / ANNUAL_STAGE_ROOT / str(annual_plan["plan_id"]) / "controls"
    paths = {
        "release": controls / "successor_release_manifest.json",
        "annual_inventory": controls / "annual_candidate_inventory.json",
        "bbo_catalog": controls / "bbo_1s_eligibility_catalog.json",
        "tbbo_catalog": controls / "tbbo_source_catalog.json",
        "annual_contract": controls / "annual_source_contract.json",
    }
    release = _parse_manifest(_json(paths["release"], "annual release manifest"))
    annual_inventory = _json(paths["annual_inventory"], "annual candidate inventory")
    bbo_catalog = _json(paths["bbo_catalog"], "annual BBO catalog")
    tbbo_catalog = _json(paths["tbbo_catalog"], "annual TBBO catalog")
    annual_contract = _json(paths["annual_contract"], "annual source contract")
    for value, field, label in (
        (annual_inventory, "inventory_id", "annual candidate inventory"),
        (bbo_catalog, "catalog_id", "annual BBO catalog"),
        (tbbo_catalog, "catalog_id", "annual TBBO catalog"),
        (annual_contract, "contract_id", "annual source contract"),
    ):
        _identity(value, field, label)
    release_rows = [
        {"path": item.logical_path, "size_bytes": item.size, "sha256": item.sha256}
        for item in release.files
    ]
    if (
        len(release.files) != EXPECTED_OUTPUT_FILE_COUNT
        or annual_inventory.get("release_id") != release.release_id
        or annual_inventory.get("entries") != release_rows
        or annual_inventory.get("file_count") != EXPECTED_OUTPUT_FILE_COUNT
        or annual_contract.get("annual_release_id") != release.release_id
        or annual_contract.get("annual_inventory_id") != annual_inventory["inventory_id"]
        or annual_contract.get("bbo_1s_catalog_id") != bbo_catalog["catalog_id"]
        or annual_contract.get("tbbo_catalog_id") != tbbo_catalog["catalog_id"]
        or bbo_catalog.get("release_id") != release.release_id
        or bbo_catalog.get("entry_count") != 4_698
        or tbbo_catalog.get("release_id") != release.release_id
        or tbbo_catalog.get("entry_count") != 32
    ):
        raise IntegrityError("sealed annual controls differ")
    return (
        annual_plan, certificate, release, annual_inventory, bbo_catalog,
        tbbo_catalog, annual_contract, paths,
    )


def _annual_inventory_entries(
    annual_plan: Mapping[str, Any],
    release: DataReleaseManifest,
    *,
    standard_roots: Sequence[str],
    deferred_micro_roots: Sequence[str],
) -> list[dict[str, Any]]:
    units = {str(item["destination_dbn"]): item for item in annual_plan["units"]}
    standard = set(standard_roots)
    deferred = set(deferred_micro_roots)
    rows: list[dict[str, Any]] = []
    for item in release.files:
        dbn_path = item.logical_path.removesuffix(".manifest.json")
        unit = units.get(dbn_path)
        if unit is None:
            raise IntegrityError("annual release path is absent from its build plan")
        market = str(unit["market"])
        if market in standard:
            lane = "STANDARD_41"
        elif market in deferred:
            lane = "DEFERRED_MICRO_17"
        else:
            raise IntegrityError("annual market is outside the source universe")
        sidecar = item.logical_path.endswith(".manifest.json")
        rows.append({
            "admitted_standard_foundation": False,
            "family": unit["family"],
            "identity_basis": (
                "SIDECAR_BYTES_HASHED_METADATA_ONLY"
                if sidecar else "DECODED_ORDER_PRESERVING_DBN_V3_CONSOLIDATION"
            ),
            "interval_end_exclusive": unit["coverage"]["end_exclusive"],
            "interval_start_inclusive": unit["coverage"]["start_inclusive"],
            "kind": "SIDECAR" if sidecar else "DBN",
            "lane": lane,
            "market": market,
            "path": item.logical_path,
            "sha256": item.sha256,
            "sidecar_schema_version": "annual_dbn_successor_sidecar/1.0.0",
            "size_bytes": item.size,
            "year": unit["year"],
        })
    rows.sort(key=lambda row: str(row["path"]))
    return rows


def _successor_entries(
    prior: Sequence[Mapping[str, Any]], annual: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = sorted(
        (copy.deepcopy(dict(item)) for item in (*prior, *annual)),
        key=lambda row: str(row["path"]),
    )
    paths = [str(item["path"]) for item in rows]
    if (
        len(rows) != EXPECTED_SUCCESSOR_FILES
        or len(paths) != len(set(paths))
        or len(annual) != EXPECTED_OUTPUT_FILE_COUNT
        or sum(item.get("kind") == "DBN" for item in annual) != EXPECTED_ANNUAL_DBNS
        or any(item.get("admitted_standard_foundation") is not False for item in annual)
    ):
        raise IntegrityError("annual successor inventory is not exact")
    return rows


def _prefer_primary_ohlcv_1m_entries(
    root: Path, entries: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    candidates = [
        item
        for item in entries
        if item.get("admitted_standard_foundation") is True
        and item.get("family") == "ohlcv_1m"
    ]
    preferred_paths = {
        str(item["path"])
        for item in select_preferred_standard_source_entries(root, candidates)
    }
    return [
        {
            **copy.deepcopy(dict(item)),
            "admitted_standard_foundation": str(item["path"]) in preferred_paths,
        }
        if item.get("admitted_standard_foundation") is True
        and item.get("family") == "ohlcv_1m"
        else copy.deepcopy(dict(item))
        for item in entries
    ]


def _final_paths(release_id: str, annual: Mapping[str, Any]) -> dict[str, Path]:
    return {
        "complete_release": Path(
            f"state/annual_dbn_consolidation_registry/{release_id}/complete_release_manifest.json"
        ),
        "complete_inventory": Path(
            f"state/source_contract_registry/{release_id}/canonical_inventory.json"
        ),
        "wrapper": Path(
            f"state/dbn_congruence_release_registry/{release_id}/active_wrapper_v2.json"
        ),
        "annual_inventory": Path(
            "state/annual_dbn_candidate_inventory_registry/"
            f"{annual['inventory_id']}/inventory.json"
        ),
        "bbo_catalog": Path(
            "state/annual_bbo_1s_eligibility_catalog_registry/"
            f"{annual['bbo_catalog_id']}/catalog.json"
        ),
        "tbbo_catalog": Path(
            "state/annual_tbbo_source_catalog_registry/"
            f"{annual['tbbo_catalog_id']}/catalog.json"
        ),
        "annual_contract": Path(
            "state/annual_dbn_source_contract_registry/"
            f"{annual['annual_contract_id']}/contract.json"
        ),
    }


def prepare_gate_b(
    *, root: Path, annual_plan_path: Path, certificate_path: Path,
) -> dict[str, Any]:
    """Prepare exact Gate B controls without publishing, rebuilding, or deleting."""

    root = root.resolve(strict=True)
    annual_path = annual_plan_path if annual_plan_path.is_absolute() else root / annual_plan_path
    certificate = certificate_path if certificate_path.is_absolute() else root / certificate_path
    (
        annual_plan, annual_certificate, annual_release, annual_inventory,
        bbo_catalog, tbbo_catalog, annual_contract, annual_paths,
    ) = _annual_controls(root, annual_path.resolve(strict=True), certificate.resolve(strict=True))
    published_annual_path = root / manifest_relative_path(
        "dbn", annual_release.release_id
    )
    annual_release_already_published = published_annual_path.exists()
    if annual_release_already_published and (
        verify_data_release_manifest(published_annual_path, RepoBoundary(root)).as_dict()
        != annual_release.as_dict()
    ):
        raise IntegrityError("published annual release differs from its sealed candidate")

    predecessor_bytes = (root / ACTIVE_SOURCE_CONTRACT).read_bytes()
    pointer_bytes = (root / ACTIVE_DBN_POINTER).read_bytes()
    active_catalog_bytes = (root / ACTIVE_CAUSAL_CATALOG).read_bytes()
    predecessor = _json(root / ACTIVE_SOURCE_CONTRACT, "active source contract")
    pointer = _json(root / ACTIVE_DBN_POINTER, "active DBN pointer")
    active_catalog = _json(root / ACTIVE_CAUSAL_CATALOG, "active causal catalog")
    _identity(predecessor, "contract_id", "active source contract")
    _identity(pointer, "pointer_id", "active DBN pointer")
    if pointer.get("release_id") != predecessor["active_canonical_source"]["release_id"]:
        raise IntegrityError("active source contract and DBN pointer differ")
    prior_inventory_path = root / str(predecessor["complete_inventory"]["path"])
    if sha256_file(prior_inventory_path) != predecessor["complete_inventory"]["sha256"]:
        raise IntegrityError("predecessor source inventory differs")
    prior_inventory = _json(prior_inventory_path, "predecessor source inventory")
    validate_full_build_selection_contract(predecessor, prior_inventory["entries"])

    implementation_sha = sha256_file(Path(__file__))
    run_id = sha256_json({
        "annual_plan_id": annual_plan["plan_id"],
        "annual_certificate_id": annual_certificate["certificate_id"],
        "predecessor_pointer_sha256": sha256_bytes(pointer_bytes),
        "predecessor_source_contract_sha256": sha256_bytes(predecessor_bytes),
        "predecessor_active_catalog_sha256": sha256_bytes(active_catalog_bytes),
        "implementation_sha256": implementation_sha,
    })
    run_root = root / GATE_ROOT / run_id
    candidates = run_root / "candidates"
    rollback = run_root / "rollback"

    annual_rows = _annual_inventory_entries(
        annual_plan,
        annual_release,
        standard_roots=predecessor["universe"]["standard_roots"],
        deferred_micro_roots=predecessor["universe"]["deferred_micro_roots"],
    )
    entries = _prefer_primary_ohlcv_1m_entries(
        root, _successor_entries(prior_inventory["entries"], annual_rows)
    )
    prior_admitted_dbns = {
        str(item["path"])
        for item in prior_inventory["entries"]
        if item.get("kind") == "DBN"
        and item.get("admitted_standard_foundation") is True
    }
    admitted_dbns = [
        dict(item)
        for item in entries
        if item.get("kind") == "DBN"
        and item.get("admitted_standard_foundation") is True
    ]
    removed_admitted_dbns = prior_admitted_dbns - {
        str(item["path"]) for item in admitted_dbns
    }
    if (
        len(removed_admitted_dbns) != EXPECTED_FOUNDATION_REMOVED_DBNS
        or any(
            "/ohlcv_1m/" not in path or not path.endswith(".parent.dbn.zst")
            for path in removed_admitted_dbns
        )
    ):
        raise IntegrityError("annual successor foundation correction is not exact")
    content_rows = [
        {
            "relative_path": str(item["path"])[len("data/dbn/"):],
            "size_bytes": item["size_bytes"],
            "sha256": item["sha256"],
        }
        for item in entries
    ]
    content_sha = sha256_json(content_rows)
    total_bytes = sum(int(item["size_bytes"]) for item in entries)
    release_core = {
        "schema_version": "annual_dbn_complete_successor_core/1.0.0",
        "scope": "ANNUAL_CUSTODY_WITH_FOUNDATION_SELECTION_CORRECTION",
        "physical_publication_successor_to": pointer["release_id"],
        "annual_release_id": annual_release.release_id,
        "annual_validation_certificate_id": annual_certificate["certificate_id"],
        "added_annual_artifact_count": (
            0 if annual_release_already_published else EXPECTED_OUTPUT_FILE_COUNT
        ),
        "registered_annual_artifact_count": EXPECTED_OUTPUT_FILE_COUNT,
        "research_admitted_addition_count": 0,
        "research_admitted_removal_count": len(removed_admitted_dbns),
        "complete_shadow_tree": {
            "file_count": len(entries),
            "total_bytes": total_bytes,
            "inventory_sha256": content_sha,
        },
        "successor_inventory_sha256": sha256_json(entries),
    }
    release_id = sha256_json(release_core)
    complete_release = {
        "schema_version": "annual_dbn_complete_successor_release/1.0.0",
        "release_status": "CERTIFIED_NON_ACTIVE_REQUIRES_BOUND_PUBLICATION",
        "publication_authorized": False,
        "release_core": release_core,
        "release_id": release_id,
        "complete_shadow_tree": release_core["complete_shadow_tree"],
    }
    final = _final_paths(release_id, {
        "inventory_id": annual_inventory["inventory_id"],
        "bbo_catalog_id": bbo_catalog["catalog_id"],
        "tbbo_catalog_id": tbbo_catalog["catalog_id"],
        "annual_contract_id": annual_contract["contract_id"],
    })
    complete_release_path = candidates / "complete_release_manifest.json"
    _write_new_or_exact(complete_release_path, complete_release)

    inventory_core = {
        "schema_version": prior_inventory["schema_version"],
        "status": "ACTIVE_EXACT_CANONICAL_INVENTORY_ONLY_AT_BOUND_ALIAS",
        "active_release_id": release_id,
        "entries": entries,
        "file_count": len(entries),
        "total_bytes": total_bytes,
        "content_inventory_sha256": content_sha,
    }
    complete_inventory = {**inventory_core, "inventory_id": sha256_json(inventory_core)}
    complete_inventory_path = candidates / "canonical_inventory.json"
    _write_new_or_exact(complete_inventory_path, complete_inventory)

    complete_release_sha = sha256_file(complete_release_path)
    wrapper = {
        "schema_version": "canonical_data_dbn_release_wrapper/2.0.0",
        "status": "IMMUTABLE_SUCCESSOR_WRAPPER",
        "release_id": release_id,
        "release_manifest_path": final["complete_release"].as_posix(),
        "release_manifest_sha256": complete_release_sha,
    }
    wrapper_path = candidates / "active_wrapper_v2.json"
    _write_new_or_exact(wrapper_path, wrapper)
    pointer_core = {
        "schema_version": "canonical_data_dbn_active_pointer/2.0.0",
        "status": "ACTIVE",
        "activated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "canonical_artifact_root": "data/dbn",
        "physical_publication_successor_to": pointer["release_id"],
        "scope": "ANNUAL_CUSTODY_WITH_FOUNDATION_SELECTION_CORRECTION",
        "release_id": release_id,
        "release_manifest_path": final["complete_release"].as_posix(),
        "release_manifest_sha256": complete_release_sha,
        "wrapper_path": final["wrapper"].as_posix(),
        "wrapper_sha256": sha256_file(wrapper_path),
    }
    pointer_candidate = {**pointer_core, "pointer_id": sha256_json(pointer_core)}
    pointer_candidate_path = candidates / "active_dbn_pointer.json"
    _write_new_or_exact(pointer_candidate_path, pointer_candidate)

    for name, payload in (
        ("annual_candidate_inventory.json", annual_inventory),
        ("bbo_1s_eligibility_catalog.json", bbo_catalog),
        ("tbbo_source_catalog.json", tbbo_catalog),
        ("annual_source_contract.json", annual_contract),
    ):
        _write_new_or_exact(candidates / name, payload)

    inventory_sha = sha256_file(complete_inventory_path)
    successor = copy.deepcopy(predecessor)
    successor["active_canonical_source"] = {
        "canonical_root": "data/dbn",
        "complete_root_file_count": len(entries),
        "complete_root_total_bytes": total_bytes,
        "pointer_id": pointer_candidate["pointer_id"],
        "pointer_path": ACTIVE_DBN_POINTER.as_posix(),
        "pointer_sha256": sha256_file(pointer_candidate_path),
        "release_id": release_id,
        "release_manifest_path": final["complete_release"].as_posix(),
        "release_manifest_sha256": complete_release_sha,
    }
    successor["complete_inventory"] = {
        "content_inventory_sha256": content_sha,
        "file_count": len(entries),
        "inventory_id": complete_inventory["inventory_id"],
        "path": final["complete_inventory"].as_posix(),
        "sha256": inventory_sha,
        "size_bytes": complete_inventory_path.stat().st_size,
        "total_bytes": total_bytes,
    }
    family_counts = dict(successor["source_family_dbn_counts"])
    family_counts.update({"bbo_1s": 36, "tbbo": 32})
    successor["source_family_dbn_counts"] = dict(sorted(family_counts.items()))
    successor["annual_consolidation"] = {
        "annual_release_id": annual_release.release_id,
        "annual_release_manifest_path": manifest_relative_path(
            "dbn", annual_release.release_id
        ).as_posix(),
        "annual_release_manifest_sha256": sha256_file(annual_paths["release"]),
        "annual_inventory_id": annual_inventory["inventory_id"],
        "annual_inventory_path": final["annual_inventory"].as_posix(),
        "annual_inventory_sha256": sha256_file(candidates / "annual_candidate_inventory.json"),
        "bbo_1s_catalog_id": bbo_catalog["catalog_id"],
        "bbo_1s_catalog_path": final["bbo_catalog"].as_posix(),
        "bbo_1s_catalog_sha256": sha256_file(candidates / "bbo_1s_eligibility_catalog.json"),
        "tbbo_catalog_id": tbbo_catalog["catalog_id"],
        "tbbo_catalog_path": final["tbbo_catalog"].as_posix(),
        "tbbo_catalog_sha256": sha256_file(candidates / "tbbo_source_catalog.json"),
        "annual_source_contract_id": annual_contract["contract_id"],
        "annual_source_contract_path": final["annual_contract"].as_posix(),
        "annual_source_contract_sha256": sha256_file(candidates / "annual_source_contract.json"),
        "daily_bbo_resolution_requires_ts_recv_filter": True,
        "research_admitted_addition_count": 0,
        "research_admitted_removal_count": len(removed_admitted_dbns),
    }
    selection_policy = dict(successor["selection_policy"])
    selection_policy.update(
        {
            "admitted_standard_dbn_file_count": len(admitted_dbns),
            "admitted_standard_dbn_inventory_sha256": sha256_json(admitted_dbns),
        }
    )
    successor["selection_policy"] = selection_policy
    successor.pop("contract_id", None)
    successor["contract_id"] = sha256_json(successor)
    validate_full_build_selection_contract(successor, entries)
    source_candidate_path = candidates / "source_contract_v8.json"
    _write_new_or_exact(source_candidate_path, successor)

    selected = [item for item in entries if item["admitted_standard_foundation"] is True]
    selected_dbns = [item for item in selected if item["kind"] == "DBN"]
    selected_counts = Counter(item["family"] for item in selected_dbns)
    if (
        len(selected) != EXPECTED_ENTRY_COUNT
        or len(selected_dbns) != EXPECTED_DBN_COUNT
        or len(selected) - len(selected_dbns) != EXPECTED_SIDECAR_COUNT
        or sum(int(item["size_bytes"]) for item in selected) != EXPECTED_SOURCE_BYTES
        or sum(int(item["size_bytes"]) for item in selected_dbns) != EXPECTED_PAYLOAD_BYTES
        or selected_counts["ohlcv_1m"] != EXPECTED_PRIMARY_1M_DBN_COUNT
    ):
        raise IntegrityError("annual successor changed the admitted causal source selection")
    selected_core = {
        "schema_version": "annual_dbn_gate_b_causal_source_inventory/1.0.0",
        "source_contract_id": successor["contract_id"],
        "canonical_release_id": release_id,
        "entries": selected,
        "entry_count": len(selected),
        "dbn_file_count": len(selected_dbns),
        "sidecar_file_count": len(selected) - len(selected_dbns),
        "total_source_bytes": sum(int(item["size_bytes"]) for item in selected),
        "maximum_payload_bytes": sum(int(item["size_bytes"]) for item in selected_dbns),
    }
    selected_inventory = {**selected_core, "inventory_id": sha256_json(selected_core)}
    selected_inventory_path = candidates / "causal_source_inventory.json"
    _write_new_or_exact(selected_inventory_path, selected_inventory)

    _write_bytes_new_or_exact(rollback / "active_dbn_pointer.json", pointer_bytes)
    _write_bytes_new_or_exact(rollback / "source_contract.json", predecessor_bytes)
    _write_bytes_new_or_exact(rollback / "active_catalog.json", active_catalog_bytes)
    active_markets = sorted({str(item["market"]) for item in active_catalog["entries"]})
    active_bytes = _tree_bytes(root / "data/active")
    maximum_peak = MAXIMUM_PEAK_ADDITIONAL_BYTES + active_bytes
    required_free = MINIMUM_FREE_AFTER_PEAK_BYTES + maximum_peak
    observed_free = shutil.disk_usage(root).free
    if observed_free < required_free:
        raise UnauthorizedOperation("Gate B free space is below the bounded rebuild floor")

    registry_documents = [
        {
            "candidate_path": _relative(root, candidate),
            "final_path": final[key].as_posix(),
            "sha256": sha256_file(candidate),
        }
        for key, candidate in (
            ("complete_release", complete_release_path),
            ("complete_inventory", complete_inventory_path),
            ("wrapper", wrapper_path),
            ("annual_inventory", candidates / "annual_candidate_inventory.json"),
            ("bbo_catalog", candidates / "bbo_1s_eligibility_catalog.json"),
            ("tbbo_catalog", candidates / "tbbo_source_catalog.json"),
            ("annual_contract", candidates / "annual_source_contract.json"),
        )
    ]
    core = {
        "schema_version": "annual_dbn_gate_b_plan/1.0.0",
        "status": "PREPARED_EXACT_GATE_B_NOT_AUTHORIZED",
        "run_id": run_id,
        "implementation_sha256": implementation_sha,
        "annual_candidate": {
            "plan_id": annual_plan["plan_id"],
            "plan_path": _relative(root, annual_path),
            "plan_sha256": sha256_file(annual_path),
            "certificate_id": annual_certificate["certificate_id"],
            "certificate_path": _relative(root, certificate),
            "certificate_sha256": sha256_file(certificate),
            "release_id": annual_release.release_id,
            "release_manifest_candidate_path": _relative(root, annual_paths["release"]),
            "release_manifest_sha256": sha256_file(annual_paths["release"]),
            "payload_stage": (
                ANNUAL_STAGE_ROOT / str(annual_plan["plan_id"]) / "payload"
            ).as_posix(),
            "release_already_published": annual_release_already_published,
            "file_count": EXPECTED_OUTPUT_FILE_COUNT,
            "total_bytes": sum(item.size for item in annual_release.files),
        },
        "successor": {
            "release_id": release_id,
            "complete_file_count": len(entries),
            "complete_total_bytes": total_bytes,
            "annual_dbn_count": EXPECTED_ANNUAL_DBNS,
            "annual_sidecar_count": EXPECTED_ANNUAL_DBNS,
            "research_admitted_addition_count": 0,
            "research_admitted_removal_count": len(removed_admitted_dbns),
            "source_contract_id": successor["contract_id"],
            "source_contract_candidate_path": _relative(root, source_candidate_path),
            "source_contract_candidate_sha256": sha256_file(source_candidate_path),
            "pointer_candidate_path": _relative(root, pointer_candidate_path),
            "pointer_candidate_sha256": sha256_file(pointer_candidate_path),
            "versioned_source_contract_path": SOURCE_V8.as_posix(),
            "annual_release_manifest_path": manifest_relative_path(
                "dbn", annual_release.release_id
            ).as_posix(),
            "registry_documents": registry_documents,
        },
        "predecessor": {
            "release_id": pointer["release_id"],
            "pointer_sha256": sha256_bytes(pointer_bytes),
            "source_contract_sha256": sha256_bytes(predecessor_bytes),
            "active_catalog_sha256": sha256_bytes(active_catalog_bytes),
            "pointer_snapshot_path": _relative(root, rollback / "active_dbn_pointer.json"),
            "source_contract_snapshot_path": _relative(root, rollback / "source_contract.json"),
            "active_catalog_snapshot_path": _relative(root, rollback / "active_catalog.json"),
            "active_view_id": active_catalog["active_view_id"],
        },
        "causal_rebuild": {
            "market_count": len(predecessor["universe"]["standard_roots"]),
            "market_order": predecessor["universe"]["standard_roots"],
            "source_inventory_path": _relative(root, selected_inventory_path),
            "source_inventory_sha256": sha256_file(selected_inventory_path),
            "source_entry_count": EXPECTED_ENTRY_COUNT,
            "dbn_file_count": EXPECTED_DBN_COUNT,
            "sidecar_file_count": EXPECTED_SIDECAR_COUNT,
            "source_bytes": EXPECTED_SOURCE_BYTES,
            "payload_bytes": EXPECTED_PAYLOAD_BYTES,
            "work_unit_count": EXPECTED_WORK_UNIT_COUNT,
            "maximum_partition_count": MAXIMUM_PARTITION_COUNT,
            "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
            "maximum_peak_additional_bytes": MAXIMUM_PEAK_ADDITIONAL_BYTES,
            "maximum_workers": 1,
            "independent_replay_passes": 2,
            "provider_calls": 0,
            "holdout_or_forward_access": False,
        },
        "active_rebuild": {
            "market_count": len(active_markets),
            "markets": active_markets,
            "entry_count": len(active_catalog["entries"]),
            "materialized_entry_count": sum(
                item["disposition"] == "RESEARCH_READY_CAUSAL_PRICE"
                for item in active_catalog["entries"]
            ),
            "maximum_stage_bytes": active_bytes,
            "scope_expansion": False,
            "atomic_swap_and_rollback_required": True,
        },
        "storage": {
            "annual_payload_already_staged_bytes": (
                0
                if annual_release_already_published
                else sum(item.size for item in annual_release.files)
            ),
            "publication_moves_staged_bytes": not annual_release_already_published,
            "maximum_peak_additional_bytes": maximum_peak,
            "required_free_after_peak_bytes": MINIMUM_FREE_AFTER_PEAK_BYTES,
            "required_free_before_gate_b_bytes": required_free,
            "observed_free_bytes": observed_free,
        },
        "rollback": {
            "restore_aliases_byte_for_byte": True,
            "restore_active_view_atomically": True,
            "published_annual_release_remains_immutable_and_inert": True,
            "delete_or_move_old_shards": False,
        },
        "authority": {
            "gate_b_approved": False,
            "provider_or_network_access": False,
            "canonical_publication": False,
            "real_dbn_row_read": False,
            "derived_rebuild": False,
            "active_view_replacement": False,
            "deletion_or_cleanup": False,
            "holdout_or_forward_access": False,
            "research_selection_change": False,
            "trading_or_order_access": False,
        },
    }
    plan = {**core, "plan_id": sha256_json(core)}
    plan_path = run_root / "plan.json"
    _write_new_or_exact(plan_path, plan)
    return {**plan, "plan_path": _relative(root, plan_path)}


def load_gate_b_plan(root: Path, plan_path: Path) -> dict[str, Any]:
    root = root.resolve(strict=True)
    path = plan_path if plan_path.is_absolute() else root / plan_path
    plan = _json(path.resolve(strict=True), "annual Gate B plan")
    _identity(plan, "plan_id", "annual Gate B plan")
    if (
        plan.get("schema_version") != "annual_dbn_gate_b_plan/1.0.0"
        or plan.get("status") != "PREPARED_EXACT_GATE_B_NOT_AUTHORIZED"
    ):
        raise IntegrityError("annual Gate B plan schema or status differs")
    if sha256_file(Path(__file__)) != plan.get("implementation_sha256"):
        raise IntegrityError("annual Gate B implementation binding drifted")
    successor = plan["successor"]
    for item in successor["registry_documents"]:
        if sha256_file(root / item["candidate_path"]) != item["sha256"]:
            raise IntegrityError("annual Gate B registry candidate drifted")
    for path_key, hash_key in (
        ("source_contract_candidate_path", "source_contract_candidate_sha256"),
        ("pointer_candidate_path", "pointer_candidate_sha256"),
    ):
        if sha256_file(root / successor[path_key]) != successor[hash_key]:
            raise IntegrityError("annual Gate B alias candidate drifted")
    return plan


def publication_scope(plan: Mapping[str, Any], plan_sha256: str) -> dict[str, str]:
    annual = plan["annual_candidate"]
    successor = plan["successor"]
    return {
        "operation_kind": "ANNUAL_TBBO_BBO_1S_GATE_B",
        "annual_release_id": str(annual["release_id"]),
        "successor_release_id": str(successor["release_id"]),
        "annual_file_count": str(annual["file_count"]),
        "canonical_publication": "true",
        "source_alias_replacement": "true",
        "provider_or_network_access": "false",
        "deletion_or_cleanup": "false",
        "research_selection_change": "true",
        "trading_or_order_access": "false",
        "approval_command": PUBLISH_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": plan_sha256,
    }


def _validate_predecessor(root: Path, plan: Mapping[str, Any]) -> None:
    predecessor = plan["predecessor"]
    for relative, expected in (
        (ACTIVE_DBN_POINTER, predecessor["pointer_sha256"]),
        (ACTIVE_SOURCE_CONTRACT, predecessor["source_contract_sha256"]),
        (ACTIVE_CAUSAL_CATALOG, predecessor["active_catalog_sha256"]),
    ):
        if sha256_file(root / relative) != expected:
            raise IntegrityError(f"Gate B predecessor drifted: {relative}")
    if shutil.disk_usage(root).free < plan["storage"]["required_free_before_gate_b_bytes"]:
        raise UnauthorizedOperation("Gate B free space no longer meets the plan")


def publish_authorized(
    *, root: Path, plan_path: Path, authorization: OperationReceipt,
) -> dict[str, Any]:
    """Publish annual custody and activate its source controls; never delete shards."""

    root = root.resolve(strict=True)
    path = plan_path if plan_path.is_absolute() else root / plan_path
    path = path.resolve(strict=True)
    plan = load_gate_b_plan(root, path)
    _validate_predecessor(root, plan)
    lock = SingleInstance(root / LOCK_PATH)
    if not lock.acquire():
        raise UnauthorizedOperation("annual Gate B writer lock is active")
    source_replaced = pointer_replaced = False
    try:
        scope = publication_scope(plan, sha256_file(path))
        authorization.consume(
            RepoBoundary(root),
            operation=PUBLISH_OPERATION,
            classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
            required_scope=scope,
        )
        annual = plan["annual_candidate"]
        manifest = _parse_manifest(
            _json(root / annual["release_manifest_candidate_path"], "annual release manifest")
        )
        staged_paths = {
            item.logical_path: Path(item.logical_path).relative_to("data/dbn").as_posix()
            for item in manifest.files
        }
        if annual["release_already_published"]:
            manifest_path = root / manifest_relative_path("dbn", manifest.release_id)
            if (
                verify_data_release_manifest(manifest_path, RepoBoundary(root)).as_dict()
                != manifest.as_dict()
            ):
                raise IntegrityError("published annual release differs")
        else:
            publisher = PhasePublisher(
                boundary=RepoBoundary(root),
                operation_receipt=authorization,
                lock_path=root / "state/locks/annual-dbn-release-publication.lock",
            )
            manifest_path = publisher.publish(
                root / annual["payload_stage"], manifest, staged_paths=staged_paths,
            )
        successor = plan["successor"]
        for item in successor["registry_documents"]:
            payload = _json(root / item["candidate_path"], "Gate B registry candidate")
            _write_new_or_exact(root / item["final_path"], payload)
        source_bytes = (root / successor["source_contract_candidate_path"]).read_bytes()
        pointer_bytes = (root / successor["pointer_candidate_path"]).read_bytes()
        _write_bytes_new_or_exact(root / successor["versioned_source_contract_path"], source_bytes)
        _atomic_replace(root / ACTIVE_SOURCE_CONTRACT, source_bytes, plan["run_id"])
        source_replaced = True
        _atomic_replace(root / ACTIVE_DBN_POINTER, pointer_bytes, plan["run_id"])
        pointer_replaced = True
        verification = verify_publication(root=root, plan_path=path)
        receipt_core = {
            "schema_version": "annual_dbn_gate_b_publication_receipt/1.0.0",
            "status": "PASS_ANNUAL_CUSTODY_AND_SOURCE_CONTROLS_ACTIVE",
            "plan_id": plan["plan_id"],
            "authorization_receipt_id": authorization.receipt_id,
            "annual_manifest_path": _relative(root, manifest_path),
            "verification": verification,
            "provider_calls": 0,
            "deletions": 0,
        }
        receipt = {**receipt_core, "receipt_id": sha256_json(receipt_core)}
        receipt_path = path.parent / "publication_receipt.json"
        _write_new_or_exact(receipt_path, receipt)
        return {**receipt, "receipt_path": _relative(root, receipt_path)}
    except Exception:
        if pointer_replaced or source_replaced:
            rollback_aliases(root=root, plan_path=path)
        raise
    finally:
        lock.release()


def rollback_aliases(*, root: Path, plan_path: Path) -> dict[str, Any]:
    """Restore the exact predecessor aliases while retaining immutable annual custody."""

    root = root.resolve(strict=True)
    path = plan_path if plan_path.is_absolute() else root / plan_path
    plan = load_gate_b_plan(root, path.resolve(strict=True))
    successor = plan["successor"]
    current_source = sha256_file(root / ACTIVE_SOURCE_CONTRACT)
    current_pointer = sha256_file(root / ACTIVE_DBN_POINTER)
    allowed_source = {
        plan["predecessor"]["source_contract_sha256"],
        successor["source_contract_candidate_sha256"],
    }
    allowed_pointer = {
        plan["predecessor"]["pointer_sha256"],
        successor["pointer_candidate_sha256"],
    }
    if current_source not in allowed_source or current_pointer not in allowed_pointer:
        raise IntegrityError("refusing to overwrite drifted Gate B aliases")
    source_bytes = (root / plan["predecessor"]["source_contract_snapshot_path"]).read_bytes()
    pointer_bytes = (root / plan["predecessor"]["pointer_snapshot_path"]).read_bytes()
    _atomic_replace(root / ACTIVE_SOURCE_CONTRACT, source_bytes, plan["run_id"] + ".rollback")
    _atomic_replace(root / ACTIVE_DBN_POINTER, pointer_bytes, plan["run_id"] + ".rollback")
    if (
        sha256_file(root / ACTIVE_SOURCE_CONTRACT)
        != plan["predecessor"]["source_contract_sha256"]
        or sha256_file(root / ACTIVE_DBN_POINTER)
        != plan["predecessor"]["pointer_sha256"]
    ):
        raise IntegrityError("Gate B alias rollback failed")
    return {"status": "ROLLED_BACK_EXACT_ALIASES_ANNUAL_CUSTODY_RETAINED"}


def verify_publication(*, root: Path, plan_path: Path) -> dict[str, Any]:
    root = root.resolve(strict=True)
    path = plan_path if plan_path.is_absolute() else root / plan_path
    plan = load_gate_b_plan(root, path.resolve(strict=True))
    successor = plan["successor"]
    if (
        sha256_file(root / ACTIVE_SOURCE_CONTRACT)
        != successor["source_contract_candidate_sha256"]
        or sha256_file(root / ACTIVE_DBN_POINTER)
        != successor["pointer_candidate_sha256"]
        or (root / SOURCE_V8).read_bytes()
        != (root / successor["source_contract_candidate_path"]).read_bytes()
    ):
        raise IntegrityError("active Gate B aliases differ")
    for item in successor["registry_documents"]:
        if sha256_file(root / item["final_path"]) != item["sha256"]:
            raise IntegrityError("active Gate B registry document differs")
    annual_manifest = verify_data_release_manifest(
        root / successor["annual_release_manifest_path"], RepoBoundary(root)
    )
    context = issue_current_source_closure_context(root)
    metadata = validate_source_contract_metadata(
        root, operation_context=context, contract_path=root / ACTIVE_SOURCE_CONTRACT
    )
    if (
        annual_manifest.release_id != plan["annual_candidate"]["release_id"]
        or metadata["release_id"] != successor["release_id"]
        or metadata["file_count"] != successor["complete_file_count"]
        or metadata["total_bytes"] != successor["complete_total_bytes"]
    ):
        raise IntegrityError("active Gate B source metadata differs")
    return {
        "status": "PASS_ANNUAL_GATE_B_SOURCE_PUBLICATION",
        "annual_release_id": annual_manifest.release_id,
        "successor_release_id": successor["release_id"],
        "annual_file_count": len(annual_manifest.files),
        "complete_file_count": metadata["file_count"],
        "research_admitted_addition_count": 0,
        "research_admitted_removal_count": plan["successor"][
            "research_admitted_removal_count"
        ],
        "provider_calls": 0,
        "deletions": 0,
    }


__all__ = [
    "PUBLISH_OPERATION",
    "load_gate_b_plan",
    "prepare_gate_b",
    "publication_scope",
    "publish_authorized",
    "rollback_aliases",
    "verify_publication",
]
