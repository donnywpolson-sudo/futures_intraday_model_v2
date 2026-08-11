"""Publish and activate the certified Apex micro source foundation.

Planning is source-safe and stat-only. Execution is a separate, one-use
publication boundary: it hash-verifies the 120 preserved Phase 1B Parquets and
24 certified Phase 2 Parquets, publishes exact byte copies through the layout-v2
manifest writer, writes the lane catalog, and creates the micro pointer last.

The operation never opens Parquet rows, DBNs, the 2025 holdout, or 2026 forward
payloads. Published data releases are immutable. If activation validation fails,
the newly created catalog and pointer are moved to failure evidence while any
already published immutable releases remain inactive and auditable.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
from collections import defaultdict
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Final

from .alpha_research_architecture import MICRO_POINTER_PATH
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    contained_path,
    fsync_directory,
    sha256_bytes,
    sha256_file,
    sha256_json,
)
from .data_layout import (
    DataFileEntry,
    DataReleaseManifest,
    PhasePublisher,
    manifest_relative_path,
    verify_data_release_manifest,
    verify_layout_contract,
)
from .errors import IntegrityError, UnauthorizedOperation
from .locking import FileLease
from .micro_alpha_phase1b2_preparation import (
    ACTIVE_MICRO_CATALOG_PATH,
    require_row_certified_catalog_candidate,
)
from .micro_alpha_pipeline import LANE_ID, SCHEMAS, TIER_1_MARKETS


OPERATION: Final = "PUBLISH_RELEASE"
APPROVAL_COMMAND: Final = (
    "PUBLISH_ACTIVATE_APEX_MICRO_CERTIFIED_FOUNDATION_V1_ONCE"
)
PLAN_PATH: Final = Path("configs/apex_micro_publication_activation_plan_v1.json")
AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_publication_activation_plan_v1/audit.json"
)
REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_phase2_successor_v4/"
    "49beef616481be65a191d2da/source_certification_report.json"
)
CANDIDATE_PATH: Final = REPORT_PATH.with_name("inactive_catalog_candidate.json")
TERMINAL_PATH: Final = REPORT_PATH.with_name("terminal.json")
PREPARED_ROOT: Final = Path(
    "state/unpublished_evidence/apex_micro_ladder_preparation_v2/"
    "234eccff53c6620f2f54e73c88165574531f434b441ae808dd36c2f75d1927c8"
)
PREPARED_POINTER_PATH: Final = PREPARED_ROOT / "prepared_active_pointer.json"
CONTRACT_PATH: Final = PREPARED_ROOT / "universe_contract.json"
PROFILE_PATH: Final = PREPARED_ROOT / "alpha_tiered.json"
LAYOUT_CONTRACT_PATH: Final = Path("configs/data_layout_contract.json")
STANDARD_ACTIVE_CATALOG_PATH: Final = Path("data/active/catalog.json")
PUBLICATION_LOCK: Final = Path("state/locks/apex_micro_publication_v1.lock")
DATA_PUBLICATION_LOCK: Final = Path("state/locks/data-publication.lock")
EVIDENCE_PARENT: Final = Path(
    "state/unpublished_evidence/apex_micro_publication_v1"
)
FAILED_PARENT: Final = Path("state/apex_micro_publication_failed")

PLAN_SCHEMA: Final = "apex_micro_publication_activation_plan/1.0.0"
AUDIT_SCHEMA: Final = "apex_micro_publication_activation_audit/1.0.0"
CATALOG_SCHEMA: Final = "apex_micro_active_catalog/1.0.0"
POINTER_SCHEMA: Final = "active_micro_alpha_research_ladder/1.0.0"
REPORT_SCHEMA: Final = "apex_micro_publication_activation_report/1.0.0"
TERMINAL_SCHEMA: Final = "apex_micro_publication_activation_terminal/1.0.0"
FAILURE_SCHEMA: Final = "apex_micro_publication_activation_failure/1.0.0"
PHASE1B_MANIFEST_SCHEMA: Final = "apex_micro_published_phase1b/1.0.0"
PHASE2_MANIFEST_SCHEMA: Final = "apex_micro_published_phase2/1.0.0"

EXPECTED_PHASE1B_COUNT: Final = 120
EXPECTED_PHASE2_COUNT: Final = 24
EXPECTED_PHASE1B_BYTES: Final = 6_627_486_838
EXPECTED_PHASE2_BYTES: Final = 454_578_644
EXPECTED_TOTAL_BYTES: Final = EXPECTED_PHASE1B_BYTES + EXPECTED_PHASE2_BYTES
EXPECTED_MARKET_YEARS: Final = {
    "M6E": tuple(range(2018, 2025)),
    "MCL": tuple(range(2021, 2025)),
    "MES": tuple(range(2019, 2025)),
    "MGC": tuple(range(2018, 2025)),
}
MAXIMUM_WORKERS: Final = 1
MAXIMUM_ATTEMPTS: Final = 1
MAXIMUM_RETRIES: Final = 0
REQUIRED_FREE_DISK_BYTES: Final = EXPECTED_TOTAL_BYTES + 2 * 1024**3
_HEX_24 = re.compile(r"^[0-9a-f]{24}$")
_HEX_40 = re.compile(r"^[0-9a-f]{40}$")
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")

IMPLEMENTATION_PATHS: Final = (
    Path("src/futures_rebuild/micro_alpha_publication.py"),
    Path("src/futures_rebuild/data_layout.py"),
    Path("src/futures_rebuild/boundary.py"),
    Path("src/futures_rebuild/canonical.py"),
    Path("src/futures_rebuild/locking.py"),
    Path("src/futures_rebuild/micro_alpha_phase1b2_preparation.py"),
    Path("src/futures_rebuild/alpha_research_architecture.py"),
    Path("scripts/prepare_apex_micro_publication_v1.py"),
    LAYOUT_CONTRACT_PATH,
)
SEMANTIC_PATHS: Final = (
    Path("AGENTS.md"),
    Path("CURRENT_WORKFLOW.md"),
    Path("PROJECT_OUTLINE.md"),
    Path("PIPELINE_FOLDER_MAP.md"),
    REPORT_PATH,
    CANDIDATE_PATH,
    TERMINAL_PATH,
    PREPARED_POINTER_PATH,
    CONTRACT_PATH,
    PROFILE_PATH,
)


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if type(value) is not dict:
        raise IntegrityError(f"{description} is not an object")
    return value


def _self_hash(
    value: Mapping[str, object], key: str, description: str,
) -> str:
    core = dict(value)
    observed = core.pop(key, None)
    if type(observed) is not str or observed != sha256_json(core):
        raise IntegrityError(f"{description} identity drifted")
    return observed


def _plain_hash(value: object, description: str) -> str:
    if type(value) is not str or _HEX_64.fullmatch(value) is None:
        raise IntegrityError(f"{description} is not a SHA-256 identity")
    return value


def _git_head(root: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
    ).strip()


def _canonical_file_sha(value: Mapping[str, object]) -> str:
    return sha256_bytes(canonical_bytes(dict(value)) + b"\n")


def _write_create_only(path: Path, value: Mapping[str, object]) -> None:
    payload = canonical_bytes(dict(value)) + b"\n"
    assert_no_linklike_ancestors(path.absolute())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(stat.S_IREAD)
    fsync_directory(path.parent)


def _manifest_from_dict(value: Mapping[str, object]) -> DataReleaseManifest:
    files = value.get("files")
    if not isinstance(files, list):
        raise IntegrityError("micro publication manifest files are invalid")
    try:
        return DataReleaseManifest(
            release_id=str(value["release_id"]),
            phase=str(value["phase"]),
            release_kind=str(value["release_kind"]),
            schema_version=str(value["schema_version"]),
            source_release_ids=tuple(value["source_release_ids"]),  # type: ignore[arg-type]
            files=tuple(
                DataFileEntry(
                    logical_path=str(item["logical_path"]),
                    size=int(item["size"]),
                    sha256=str(item["sha256"]),
                )
                for item in files
                if isinstance(item, Mapping)
            ),
            embedded_documents=dict(value["embedded_documents"]),  # type: ignore[arg-type]
            metadata=dict(value["metadata"]),  # type: ignore[arg-type]
            layout_version=str(value["layout_version"]),
            manifest_version=str(value["manifest_version"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise IntegrityError("micro publication manifest is invalid") from exc


def logical_path_from_inactive(staged_path: str) -> str:
    """Remove only the collision-checked 96-bit staging alias."""

    pure = PurePosixPath(staged_path)
    if pure.is_absolute() or ".." in pure.parts or "data" not in pure.parts:
        raise IntegrityError("inactive micro path is outside the repository")
    data_index = pure.parts.index("data")
    tail = list(pure.parts[data_index:])
    if len(tail) < 6 or _HEX_24.fullmatch(tail[-2]) is None:
        raise IntegrityError("inactive micro path lacks its 96-bit release alias")
    del tail[-2]
    return PurePosixPath(*tail).as_posix()


def _release_manifest(
    *, record: Mapping[str, object], logical_path: str, source_release_ids: tuple[str, ...],
    phase2: bool, source_certification_id: str,
) -> DataReleaseManifest:
    phase = PurePosixPath(logical_path).parts[1]
    output_sha = _plain_hash(record.get("output_sha256"), "published source hash")
    output_bytes = record.get("output_bytes")
    if type(output_bytes) is not int or output_bytes <= 0:
        raise IntegrityError("published source byte count is invalid")
    entry = DataFileEntry(logical_path, output_bytes, output_sha)
    certified_key = "phase2_release_id" if phase2 else "phase1b_release_id"
    record_key = "phase2_record_id" if phase2 else "decode_record_id"
    metadata = {
        "certified_release_id": _plain_hash(
            record.get(certified_key), "certified micro release ID"
        ),
        "interval": str(record.get("interval")),
        "lane_id": LANE_ID,
        "market": str(record.get("market")),
        "record_id": _plain_hash(record.get(record_key), "micro certification record ID"),
        "source_certification_id": source_certification_id,
        "year": record.get("year"),
    }
    if not phase2:
        metadata["schema"] = str(record.get("schema"))
    core: dict[str, object] = {
        "embedded_documents": {},
        "files": [entry.as_dict()],
        "layout_version": "2.0.0",
        "manifest_version": "2.0.0",
        "metadata": metadata,
        "phase": phase,
        "release_kind": (
            "apex_micro_phase2_causal_1m"
            if phase2
            else f"apex_micro_phase1b_{str(record.get('schema')).replace('-', '_')}"
        ),
        "schema_version": (
            PHASE2_MANIFEST_SCHEMA if phase2 else PHASE1B_MANIFEST_SCHEMA
        ),
        "source_release_ids": sorted(set(source_release_ids)),
    }
    return DataReleaseManifest(
        release_id=sha256_json(core),
        phase=phase,
        release_kind=str(core["release_kind"]),
        schema_version=str(core["schema_version"]),
        source_release_ids=tuple(core["source_release_ids"]),  # type: ignore[arg-type]
        files=(entry,),
        embedded_documents={},
        metadata=metadata,
    )


def _descriptor(
    *, record: Mapping[str, object], manifest: DataReleaseManifest, role: str,
) -> dict[str, object]:
    if len(manifest.files) != 1:
        raise IntegrityError("micro publication release must contain exactly one file")
    entry = manifest.files[0]
    return {
        "logical_path": entry.logical_path,
        "manifest": manifest.as_dict(),
        "manifest_path": manifest_relative_path(
            manifest.phase, manifest.release_id
        ).as_posix(),
        "physical_path": manifest.physical_relative_path(entry).as_posix(),
        "role": role,
        "source_bytes": entry.size,
        "source_path": str(record["output_path"]),
        "source_sha256": entry.sha256,
    }


def _candidate_base(candidate: Mapping[str, object]) -> dict[str, object]:
    excluded = {
        "schema_version",
        "future_active_path",
        "published",
        "active_pointer_written",
        "catalog_candidate_id",
    }
    return {key: value for key, value in candidate.items() if key not in excluded}


def build_publication_documents(*, root: Path) -> dict[str, object]:
    """Build exact manifest, catalog, and pointer documents without hashing payloads."""

    root = root.resolve(strict=True)
    verify_layout_contract(root / LAYOUT_CONTRACT_PATH)
    report = _object(root / REPORT_PATH, "micro source certification")
    candidate = _object(root / CANDIDATE_PATH, "inactive micro catalog candidate")
    terminal = _object(root / TERMINAL_PATH, "micro Phase 2 terminal")
    prepared_pointer = _object(root / PREPARED_POINTER_PATH, "prepared micro pointer")
    contract = _object(root / CONTRACT_PATH, "micro ladder contract")
    profile = _object(root / PROFILE_PATH, "micro ladder profile")

    report_id = _self_hash(report, "source_certification_id", "micro source certification")
    candidate_id = _self_hash(candidate, "catalog_candidate_id", "micro catalog candidate")
    terminal_id = _self_hash(terminal, "terminal_id", "micro Phase 2 terminal")
    prepared_pointer_id = _self_hash(prepared_pointer, "pointer_id", "prepared micro pointer")
    contract_id = _self_hash(contract, "contract_id", "micro ladder contract")
    profile_id = _self_hash(profile, "profile_id", "micro ladder profile")
    require_row_certified_catalog_candidate(_candidate_base(candidate))
    if (
        report.get("state") != "CERTIFIED_INACTIVE_NOT_PUBLISHED"
        or report.get("published") is not False
        or report.get("catalog_or_pointer_activated") is not False
        or report.get("catalog_candidate_eligible") is not True
        or report.get("identity_and_roll_continuity_certified") is not True
        or report.get("year_2025_or_2026_materialized") is not False
        or report.get("source_count") != EXPECTED_PHASE1B_COUNT
        or report.get("source_bytes") != EXPECTED_PHASE1B_BYTES
        or candidate.get("source_certification_id") != report_id
        or candidate.get("source_certification_sha256") != sha256_file(root / REPORT_PATH)
        or candidate.get("future_active_path") != ACTIVE_MICRO_CATALOG_PATH.as_posix()
        or candidate.get("published") is not False
        or candidate.get("active_pointer_written") is not False
        or terminal.get("state") != "SUCCESS_CERTIFIED_INACTIVE_PHASE2"
        or terminal.get("source_certification_id") != report_id
        or terminal.get("inactive_catalog_candidate_id") != candidate_id
        or terminal.get("completed_source_scan_count") != EXPECTED_PHASE1B_COUNT
        or terminal.get("completed_phase2_count") != EXPECTED_PHASE2_COUNT
        or terminal.get("year_2025_or_2026_payloads_opened") != 0
        or prepared_pointer.get("future_active_path") != MICRO_POINTER_PATH
        or prepared_pointer.get("catalog_path") != ACTIVE_MICRO_CATALOG_PATH.as_posix()
        or prepared_pointer.get("contract_id") != contract_id
        or prepared_pointer.get("profile_id") != profile_id
        or contract.get("lane_id") != LANE_ID
        or profile.get("lane_id") != LANE_ID
    ):
        raise UnauthorizedOperation("micro publication predecessor gate is not satisfied")

    decode_records = report.get("decode_records")
    phase2_records = report.get("phase2_records")
    if (
        not isinstance(decode_records, list)
        or len(decode_records) != EXPECTED_PHASE1B_COUNT
        or not isinstance(phase2_records, list)
        or len(phase2_records) != EXPECTED_PHASE2_COUNT
    ):
        raise IntegrityError("micro publication certification inventory is incomplete")

    phase1b_descriptors: list[dict[str, object]] = []
    group_manifests: dict[tuple[str, int, str], dict[str, dict[str, object]]] = defaultdict(dict)
    for record in decode_records:
        if not isinstance(record, Mapping):
            raise IntegrityError("micro Phase 1B publication record is invalid")
        market = str(record.get("market"))
        year = record.get("year")
        schema = str(record.get("schema"))
        interval = str(record.get("interval"))
        if (
            market not in TIER_1_MARKETS
            or type(year) is not int
            or year not in EXPECTED_MARKET_YEARS[market]
            or schema not in SCHEMAS
            or record.get("output_created") is not True
        ):
            raise UnauthorizedOperation("micro Phase 1B publication scope drifted")
        source = contained_path(root, str(record.get("output_path", "")))
        info = assert_plain_file(source)
        if info.st_size != record.get("output_bytes"):
            raise IntegrityError("micro Phase 1B staged size drifted")
        logical = logical_path_from_inactive(str(record["output_path"]))
        manifest = _release_manifest(
            record=record,
            logical_path=logical,
            source_release_ids=(
                _plain_hash(record.get("sidecar_manifest_id"), "DBN sidecar manifest ID"),
            ),
            phase2=False,
            source_certification_id=report_id,
        )
        descriptor = _descriptor(record=record, manifest=manifest, role="PHASE1B")
        phase1b_descriptors.append(descriptor)
        group_manifests[(market, year, interval)][schema] = descriptor

    if (
        len(group_manifests) != EXPECTED_PHASE2_COUNT
        or any(set(group) != set(SCHEMAS) for group in group_manifests.values())
        or sum(int(item["source_bytes"]) for item in phase1b_descriptors)
        != EXPECTED_PHASE1B_BYTES
    ):
        raise IntegrityError("micro Phase 1B five-schema publication groups drifted")

    phase2_descriptors: list[dict[str, object]] = []
    catalog_entries: list[dict[str, object]] = []
    for record in phase2_records:
        if not isinstance(record, Mapping):
            raise IntegrityError("micro Phase 2 publication record is invalid")
        market = str(record.get("market"))
        year = record.get("year")
        interval = str(record.get("interval"))
        key = (market, year, interval)
        group = group_manifests.get(key)
        if (
            market not in TIER_1_MARKETS
            or type(year) is not int
            or year not in EXPECTED_MARKET_YEARS[market]
            or not isinstance(group, dict)
            or set(group) != set(SCHEMAS)
            or record.get("identity_and_economics_certified") is not True
            or record.get("roll_continuity_certified") is not True
        ):
            raise UnauthorizedOperation("micro Phase 2 publication scope drifted")
        source = contained_path(root, str(record.get("output_path", "")))
        info = assert_plain_file(source)
        if info.st_size != record.get("output_bytes"):
            raise IntegrityError("micro Phase 2 staged size drifted")
        logical = logical_path_from_inactive(str(record["output_path"]))
        source_release_ids = tuple(
            sorted(str(group[schema]["manifest"]["release_id"]) for schema in SCHEMAS)
        )
        manifest = _release_manifest(
            record=record,
            logical_path=logical,
            source_release_ids=source_release_ids,
            phase2=True,
            source_certification_id=report_id,
        )
        descriptor = _descriptor(record=record, manifest=manifest, role="PHASE2")
        phase2_descriptors.append(descriptor)
        phase1b_catalog = {
            schema: {
                "logical_path": group[schema]["logical_path"],
                "manifest_path": group[schema]["manifest_path"],
                "physical_path": group[schema]["physical_path"],
                "release_id": group[schema]["manifest"]["release_id"],
                "sha256": group[schema]["source_sha256"],
            }
            for schema in SCHEMAS
        }
        catalog_entries.append(
            {
                "disposition": "RESEARCH_READY_CAUSAL_PRICE",
                "interval": interval,
                "market": market,
                "phase1b": phase1b_catalog,
                "phase2": {
                    "logical_path": descriptor["logical_path"],
                    "manifest_path": descriptor["manifest_path"],
                    "physical_path": descriptor["physical_path"],
                    "release_id": descriptor["manifest"]["release_id"],
                    "row_count": record.get("row_count"),
                    "sha256": descriptor["source_sha256"],
                },
                "permitted_uses": [
                    "LINEAGE_AUDIT",
                    "SOURCE_READINESS_CENSUS_AFTER_SEPARATE_ROW_AUTHORITY",
                    "FEATURE_GENERATION_AFTER_SEPARATE_MECHANISM_AND_TRIAL_AUTHORITY",
                ],
                "selection_eligible": False,
                "year": year,
            }
        )

    if (
        sum(int(item["source_bytes"]) for item in phase2_descriptors)
        != EXPECTED_PHASE2_BYTES
        or {(str(item["market"]), int(item["year"])) for item in catalog_entries}
        != {
            (market, year)
            for market, years in EXPECTED_MARKET_YEARS.items()
            for year in years
        }
    ):
        raise IntegrityError("micro Phase 2 publication coverage drifted")

    publications = sorted(
        [*phase1b_descriptors, *phase2_descriptors],
        key=lambda item: (str(item["role"]), str(item["logical_path"])),
    )
    if len({str(item["physical_path"]) for item in publications}) != len(publications):
        raise IntegrityError("micro publication target paths collide")

    catalog_core: dict[str, object] = {
        "catalog_candidate_id": candidate_id,
        "contract_id": contract_id,
        "contract_scale": "MICRO_INTEGER_ONLY",
        "entries": sorted(
            catalog_entries, key=lambda item: (str(item["market"]), int(item["year"]))
        ),
        "forward_2026_materialized": False,
        "holdout_2025_materialized": False,
        "lane_id": LANE_ID,
        "limitations": [
            "NO_BBO_OR_QUEUE_PRIORITY_CLAIM",
            "NO_GUARANTEED_MARKET_ORDER_FILL_CLAIM",
            "NO_PRECISE_WITHIN_SECOND_ORDERING_CLAIM",
            "MECHANISM_NOT_FROZEN",
            "REGISTRATION_AND_EVALUATION_NOT_AUTHORIZED",
        ],
        "phase1b_manifest_count": EXPECTED_PHASE1B_COUNT,
        "phase2_manifest_count": EXPECTED_PHASE2_COUNT,
        "profile_id": profile_id,
        "schema_version": CATALOG_SCHEMA,
        "source_certification_id": report_id,
        "source_certification_sha256": sha256_file(root / REPORT_PATH),
        "standard_active_catalog_mutated": False,
        "state": "ACTIVE_CERTIFIED_SOURCE_ONLY",
    }
    catalog = {**catalog_core, "catalog_id": sha256_json(catalog_core)}
    pointer_core: dict[str, object] = {
        "activation_scope": "SOURCE_CATALOG_ONLY",
        "catalog_id": catalog["catalog_id"],
        "catalog_path": ACTIVE_MICRO_CATALOG_PATH.as_posix(),
        "catalog_sha256": _canonical_file_sha(catalog),
        "contract_id": contract_id,
        "contract_path": CONTRACT_PATH.as_posix(),
        "contract_sha256": sha256_file(root / CONTRACT_PATH),
        "forward_2026_access_authorized": False,
        "holdout_2025_access_authorized": False,
        "historical_evaluation_authorized": False,
        "lane_id": LANE_ID,
        "mechanism_frozen": False,
        "prepared_pointer_id": prepared_pointer_id,
        "profile_id": profile_id,
        "profile_path": PROFILE_PATH.as_posix(),
        "profile_sha256": sha256_file(root / PROFILE_PATH),
        "registration_authorized": False,
        "schema_version": POINTER_SCHEMA,
        "source_certification_id": report_id,
        "state": "ACTIVE_SOURCE_CATALOG_MECHANISM_NOT_FROZEN",
    }
    pointer = {**pointer_core, "pointer_id": sha256_json(pointer_core)}
    return {
        "candidate_id": candidate_id,
        "catalog": catalog,
        "contract_id": contract_id,
        "phase1b_bytes": EXPECTED_PHASE1B_BYTES,
        "phase1b_count": EXPECTED_PHASE1B_COUNT,
        "phase2_bytes": EXPECTED_PHASE2_BYTES,
        "phase2_count": EXPECTED_PHASE2_COUNT,
        "pointer": pointer,
        "profile_id": profile_id,
        "publications": publications,
        "source_certification_id": report_id,
        "terminal_id": terminal_id,
    }


def build_plan(*, root: Path, implementation_head: str) -> dict[str, object]:
    root = root.resolve(strict=True)
    if _HEX_40.fullmatch(implementation_head) is None:
        raise IntegrityError("micro publication implementation HEAD is invalid")
    documents = build_publication_documents(root=root)
    implementation_bindings = {
        path.as_posix(): sha256_file(root / path) for path in IMPLEMENTATION_PATHS
    }
    semantic_bindings = {
        path.as_posix(): sha256_file(root / path) for path in SEMANTIC_PATHS
    }
    standard_active_catalog_sha256 = sha256_file(root / STANDARD_ACTIVE_CATALOG_PATH)
    scope_core = {
        "candidate_id": documents["candidate_id"],
        "catalog_id": documents["catalog"]["catalog_id"],
        "contract_id": documents["contract_id"],
        "implementation_bindings": implementation_bindings,
        "implementation_head": implementation_head,
        "pointer_id": documents["pointer"]["pointer_id"],
        "profile_id": documents["profile_id"],
        "semantic_bindings": semantic_bindings,
        "standard_active_catalog_sha256": standard_active_catalog_sha256,
        "source_certification_id": documents["source_certification_id"],
        "terminal_id": documents["terminal_id"],
    }
    scope_id = sha256_json(scope_core)
    path_id = scope_id[:24]
    evidence_root = EVIDENCE_PARENT / path_id
    failed_root = FAILED_PARENT / path_id
    core: dict[str, object] = {
        "approval_command": APPROVAL_COMMAND,
        "catalog": documents["catalog"],
        "evidence_root": evidence_root.as_posix(),
        "failed_activation_root": failed_root.as_posix(),
        "forbidden": {
            "dbn_payload_open": True,
            "delete_or_overwrite_existing_release": True,
            "feature_outcome_prediction_or_evaluation": True,
            "holdout_2025_or_forward_2026_access": True,
            "network_or_provider_access": True,
            "registration_or_trading": True,
            "standard_active_catalog_mutation": True,
        },
        "implementation_bindings": implementation_bindings,
        "implementation_head": implementation_head,
        "lane_id": LANE_ID,
        "limits": {
            "maximum_attempts": MAXIMUM_ATTEMPTS,
            "maximum_catalog_writes": 1,
            "maximum_data_manifests": EXPECTED_PHASE1B_COUNT + EXPECTED_PHASE2_COUNT,
            "maximum_payload_bytes": EXPECTED_TOTAL_BYTES,
            "maximum_payload_files": EXPECTED_PHASE1B_COUNT + EXPECTED_PHASE2_COUNT,
            "maximum_pointer_writes": 1,
            "maximum_retries": MAXIMUM_RETRIES,
            "maximum_workers": MAXIMUM_WORKERS,
            "required_free_disk_bytes": REQUIRED_FREE_DISK_BYTES,
        },
        "operation": OPERATION,
        "outputs": {
            "active_catalog": ACTIVE_MICRO_CATALOG_PATH.as_posix(),
            "active_pointer": MICRO_POINTER_PATH,
            "failure_report": (failed_root / "failure.json").as_posix(),
            "publication_report": (evidence_root / "report.json").as_posix(),
            "terminal": (evidence_root / "terminal.json").as_posix(),
        },
        "pointer": documents["pointer"],
        "preservation": {
            "inactive_phase1b_staging": "PRESERVE_BYTE_FOR_BYTE",
            "inactive_phase2_staging": "PRESERVE_BYTE_FOR_BYTE",
            "partial_published_releases": "IMMUTABLE_INACTIVE_RECOVERY_INPUT",
            "pointer_last": True,
        },
        "publications": documents["publications"],
        "schema_version": PLAN_SCHEMA,
        "scope_id": scope_id,
        "semantic_bindings": semantic_bindings,
        "standard_active_catalog_sha256": standard_active_catalog_sha256,
        "state": "PREPARED_ACTIVE_DATA_MUTATION_APPROVAL_REQUIRED",
    }
    plan = {**core, "plan_id": sha256_json(core)}
    for target in (
        root / ACTIVE_MICRO_CATALOG_PATH,
        root / MICRO_POINTER_PATH,
        root / PUBLICATION_LOCK,
        root / DATA_PUBLICATION_LOCK,
        root / evidence_root,
        root / failed_root,
    ):
        if target.exists():
            raise IntegrityError(f"micro publication output or lock already exists: {target}")
    for item in documents["publications"]:
        if (
            (root / str(item["physical_path"])).exists()
            or (root / str(item["manifest_path"])).exists()
        ):
            raise IntegrityError("micro publication immutable destination already exists")
    return plan


def write_plan_create_only(*, root: Path, implementation_head: str) -> dict[str, object]:
    plan = build_plan(root=root, implementation_head=implementation_head)
    _write_create_only(root / PLAN_PATH, plan)
    return plan


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH, "micro publication plan")
    _self_hash(plan, "plan_id", "micro publication plan")
    return plan


def build_audit(*, root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    plan = load_plan(root=root)
    rebuilt = build_plan(root=root, implementation_head=str(plan["implementation_head"]))
    if rebuilt != plan:
        raise IntegrityError("micro publication plan reconstruction differs")
    publications = plan.get("publications")
    if not isinstance(publications, list):
        raise IntegrityError("micro publication plan inventory is invalid")
    core: dict[str, object] = {
        "active_catalog_exists": (root / ACTIVE_MICRO_CATALOG_PATH).exists(),
        "active_pointer_exists": (root / MICRO_POINTER_PATH).exists(),
        "catalog_id": plan["catalog"]["catalog_id"],
        "implementation_head": plan["implementation_head"],
        "parquet_payloads_opened": 0,
        "payload_bytes": sum(int(item["source_bytes"]) for item in publications),
        "payload_count": len(publications),
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "provider_calls": 0,
        "schema_version": AUDIT_SCHEMA,
        "state": "PASS_PREPARED_ACTIVE_DATA_MUTATION_APPROVAL_REQUIRED",
        "standard_active_catalog_mutated": False,
        "year_2025_or_2026_payloads_opened": 0,
    }
    return {**core, "audit_id": sha256_json(core)}


def write_audit_create_only(*, root: Path) -> dict[str, object]:
    audit = build_audit(root=root)
    _write_create_only(root / AUDIT_PATH, audit)
    return audit


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan.get("limits")
    catalog = plan.get("catalog")
    pointer = plan.get("pointer")
    if not isinstance(limits, Mapping) or not isinstance(catalog, Mapping) or not isinstance(pointer, Mapping):
        raise IntegrityError("micro publication approval scope is incomplete")
    return {
        "approval_command": APPROVAL_COMMAND,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
        "catalog_id": str(catalog["catalog_id"]),
        "implementation_head": str(plan["implementation_head"]),
        "lane_id": LANE_ID,
        "maximum_payload_bytes": str(limits["maximum_payload_bytes"]),
        "maximum_payload_files": str(limits["maximum_payload_files"]),
        "maximum_retries": str(limits["maximum_retries"]),
        "pointer_id": str(pointer["pointer_id"]),
        "preserve_inactive_staging": "true",
        "provider_calls": "0",
        "standard_active_catalog_mutation": "false",
        "standard_active_catalog_sha256": str(plan["standard_active_catalog_sha256"]),
        "year_2025_or_2026_payload_reads": "0",
    }


def _publish_one(
    *, root: Path, boundary: RepoBoundary, authorization: OperationReceipt,
    plan: Mapping[str, object], item: Mapping[str, object], ordinal: int,
) -> dict[str, str]:
    source = contained_path(root, str(item["source_path"]))
    if (
        assert_plain_file(source).st_size != item["source_bytes"]
        or sha256_file(source) != item["source_sha256"]
    ):
        raise IntegrityError("micro publication source hash drifted before copy")
    stage = root / "state" / "data_publication_staging" / (
        f"apex_micro_publish_{str(plan['scope_id'])[:12]}_{ordinal:03d}"
    )
    if stage.exists():
        raise IntegrityError("micro publication stage already exists")
    stage.mkdir(parents=True, exist_ok=False)
    staged = stage / "payload.parquet"
    shutil.copy2(source, staged)
    if (
        assert_plain_file(staged).st_size != item["source_bytes"]
        or sha256_file(staged) != item["source_sha256"]
    ):
        raise IntegrityError("micro publication copied bytes differ")
    manifest_value = item.get("manifest")
    if not isinstance(manifest_value, Mapping):
        raise IntegrityError("micro publication manifest binding is invalid")
    manifest = _manifest_from_dict(manifest_value)
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=authorization,
        lock_path=root / DATA_PUBLICATION_LOCK,
    )
    manifest_path = publisher.publish(
        stage,
        manifest,
        staged_paths={str(item["logical_path"]): staged.name},
    )
    observed = verify_data_release_manifest(manifest_path, boundary)
    target = root / str(item["physical_path"])
    if (
        observed.as_dict() != manifest.as_dict()
        or assert_plain_file(target).st_size != item["source_bytes"]
        or sha256_file(target) != item["source_sha256"]
        or sha256_file(source) != item["source_sha256"]
    ):
        raise IntegrityError("micro publication post-copy verification failed")
    target.chmod(stat.S_IREAD)
    return {
        "manifest_path": manifest_path.relative_to(root).as_posix(),
        "release_id": manifest.release_id,
        "target_path": target.relative_to(root).as_posix(),
    }


def _quarantine_activation(
    *, root: Path, plan: Mapping[str, object], catalog: Mapping[str, object],
    pointer: Mapping[str, object],
) -> list[str]:
    failed_root = contained_path(root, str(plan["failed_activation_root"]))
    moved: list[str] = []
    for active_path, expected, name in (
        (root / MICRO_POINTER_PATH, pointer, "active_pointer.json"),
        (root / ACTIVE_MICRO_CATALOG_PATH, catalog, "active_catalog.json"),
    ):
        if not active_path.exists():
            continue
        if _object(active_path, name) != dict(expected):
            raise IntegrityError("active micro artifact differs; refusing rollback move")
        failed_root.mkdir(parents=True, exist_ok=True)
        target = failed_root / name
        if target.exists():
            raise IntegrityError("micro activation rollback destination exists")
        os.replace(active_path, target)
        fsync_directory(active_path.parent)
        moved.append(target.relative_to(root).as_posix())
    return moved


def verify_active(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    catalog = plan.get("catalog")
    pointer = plan.get("pointer")
    if not isinstance(catalog, Mapping) or not isinstance(pointer, Mapping):
        raise IntegrityError("micro activation documents are absent")
    observed_catalog = _object(root / ACTIVE_MICRO_CATALOG_PATH, "active micro catalog")
    observed_pointer = _object(root / MICRO_POINTER_PATH, "active micro pointer")
    if (
        observed_catalog != dict(catalog)
        or observed_pointer != dict(pointer)
        or _self_hash(observed_catalog, "catalog_id", "active micro catalog")
        != catalog["catalog_id"]
        or _self_hash(observed_pointer, "pointer_id", "active micro pointer")
        != pointer["pointer_id"]
        or observed_pointer.get("catalog_sha256")
        != sha256_file(root / ACTIVE_MICRO_CATALOG_PATH)
        or sha256_file(root / STANDARD_ACTIVE_CATALOG_PATH)
        != plan.get("standard_active_catalog_sha256")
    ):
        raise IntegrityError("micro active catalog or pointer verification failed")
    return {
        "catalog_id": str(catalog["catalog_id"]),
        "catalog_path": ACTIVE_MICRO_CATALOG_PATH.as_posix(),
        "pointer_id": str(pointer["pointer_id"]),
        "pointer_path": MICRO_POINTER_PATH,
    }


def execute_once(
    *, root: Path, authorization: OperationReceipt,
    disk_usage=shutil.disk_usage,
) -> dict[str, object]:
    """Execute one exact publication and activate the micro pointer last."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_plan(root=root)
    if _git_head(root) != plan["implementation_head"]:
        raise IntegrityError("micro publication committed HEAD drifted")
    if build_plan(root=root, implementation_head=str(plan["implementation_head"])) != plan:
        raise IntegrityError("micro publication plan drifted")
    audit = _object(root / AUDIT_PATH, "micro publication audit")
    _self_hash(audit, "audit_id", "micro publication audit")
    if build_audit(root=root) != audit:
        raise IntegrityError("micro publication audit drifted")
    free = getattr(disk_usage(root), "free", None)
    required_free = int(plan["limits"]["required_free_disk_bytes"])
    if type(free) is not int or free < required_free:
        raise UnauthorizedOperation("insufficient disk for micro publication")
    scope = required_scope(root=root, plan=plan)
    authorization.verify(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    use_path = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    publications = plan.get("publications")
    catalog = plan.get("catalog")
    pointer = plan.get("pointer")
    if (
        not isinstance(publications, list)
        or not isinstance(catalog, Mapping)
        or not isinstance(pointer, Mapping)
    ):
        raise IntegrityError("micro publication execution plan is incomplete")
    completed: list[dict[str, str]] = []
    failure: Exception | None = None
    rollback_moves: list[str] = []
    evidence_root = contained_path(root, str(plan["evidence_root"]))
    try:
        with FileLease(root / PUBLICATION_LOCK):
            for ordinal, item in enumerate(publications):
                if not isinstance(item, Mapping):
                    raise IntegrityError("micro publication entry is invalid")
                completed.append(
                    _publish_one(
                        root=root,
                        boundary=boundary,
                        authorization=authorization,
                        plan=plan,
                        item=item,
                        ordinal=ordinal,
                    )
                )
            _write_create_only(root / ACTIVE_MICRO_CATALOG_PATH, catalog)
            if sha256_file(root / ACTIVE_MICRO_CATALOG_PATH) != pointer["catalog_sha256"]:
                raise IntegrityError("micro catalog hash differs before pointer activation")
            _write_create_only(root / MICRO_POINTER_PATH, pointer)
            active = verify_active(root=root, plan=plan)
            report_core: dict[str, object] = {
                "active": active,
                "authorization_receipt_id": authorization.receipt_id,
                "authorization_use_path": use_path.relative_to(root).as_posix(),
                "completed_manifest_count": len(completed),
                "completed_payload_bytes": sum(
                    int(item["source_bytes"]) for item in publications
                ),
                "completed_publications": completed,
                "inactive_staging_preserved": True,
                "plan_id": plan["plan_id"],
                "provider_calls": 0,
                "schema_version": REPORT_SCHEMA,
                "standard_active_catalog_mutated": False,
                "state": "PUBLISHED_AND_ACTIVE_SOURCE_CATALOG_ONLY",
                "year_2025_or_2026_payloads_opened": 0,
            }
            report = {**report_core, "report_id": sha256_json(report_core)}
            _write_create_only(evidence_root / "report.json", report)
    except Exception as exc:
        failure = exc
        rollback_moves = _quarantine_activation(
            root=root, plan=plan, catalog=catalog, pointer=pointer
        )
        failed_root = contained_path(root, str(plan["failed_activation_root"]))
        failure_core: dict[str, object] = {
            "authorization_receipt_id": authorization.receipt_id,
            "authorization_use_path": use_path.relative_to(root).as_posix(),
            "completed_manifest_count": len(completed),
            "failure_type": type(exc).__name__,
            "inactive_staging_preserved": True,
            "partial_published_releases_preserved_inactive": True,
            "plan_id": plan["plan_id"],
            "provider_calls": 0,
            "rollback_moves": rollback_moves,
            "schema_version": FAILURE_SCHEMA,
            "standard_active_catalog_mutated": False,
            "state": "FAIL_CLOSED_NO_ACTIVE_MICRO_POINTER",
            "year_2025_or_2026_payloads_opened": 0,
        }
        failure_report = {
            **failure_core,
            "failure_id": sha256_json(failure_core),
        }
        _write_create_only(failed_root / "failure.json", failure_report)
    terminal_core: dict[str, object] = {
        "active_catalog_exists": (root / ACTIVE_MICRO_CATALOG_PATH).exists(),
        "active_pointer_exists": (root / MICRO_POINTER_PATH).exists(),
        "attempts": 1,
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "automatic_retries": 0,
        "completed_manifest_count": len(completed),
        "failure_type": None if failure is None else type(failure).__name__,
        "inactive_staging_preserved": True,
        "plan_id": plan["plan_id"],
        "provider_calls": 0,
        "schema_version": TERMINAL_SCHEMA,
        "standard_active_catalog_mutated": False,
        "state": (
            "SUCCESS_PUBLISHED_ACTIVE_MICRO_SOURCE_CATALOG"
            if failure is None
            else "FAIL_CLOSED_NO_ACTIVE_MICRO_POINTER"
        ),
        "terminal_written_last": True,
        "year_2025_or_2026_payloads_opened": 0,
    }
    terminal = {**terminal_core, "terminal_id": sha256_json(terminal_core)}
    _write_create_only(evidence_root / "terminal.json", terminal)
    if failure is not None:
        raise failure
    return terminal


__all__ = [
    "APPROVAL_COMMAND",
    "AUDIT_PATH",
    "IMPLEMENTATION_PATHS",
    "OPERATION",
    "PLAN_PATH",
    "build_audit",
    "build_plan",
    "build_publication_documents",
    "execute_once",
    "load_plan",
    "logical_path_from_inactive",
    "required_scope",
    "verify_active",
    "write_audit_create_only",
    "write_plan_create_only",
]
