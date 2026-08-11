"""Prepare-only migration from the immutable legacy micro catalog namespace.

This module reads catalog and publication metadata only.  It never opens market
rows and has no function that writes an active catalog, pointer, lock, failure
record, publication, registration, evaluation, or trading artifact.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Final, Mapping

from .canonical import (
    assert_no_linklike_ancestors,
    canonical_bytes,
    fsync_directory,
    sha256_bytes,
    sha256_file,
    sha256_json,
)
from .errors import IntegrityError


OPERATION: Final = "PREPARE_MICRO_FUTURES_ACTIVE_CATALOG_CUTOVER_V1"
PLAN_PATH: Final = Path("configs/micro_futures_catalog_migration_plan_v1.json")
LEGACY_CATALOG_PATH: Final = Path("data/active/catalogs/apex_micro.json")
LEGACY_POINTER_PATH: Final = Path("configs/active_micro_alpha_research_ladder.json")
LEGACY_TERMINAL_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_publication_v1/"
    "99851971375a57eacf7a1acb/terminal.json"
)
LEGACY_REPORT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_publication_v1/"
    "99851971375a57eacf7a1acb/report.json"
)
GENERIC_CATALOG_PATH: Final = Path("data/active/catalogs/micro_futures.json")
GENERIC_POINTER_PATH: Final = Path("configs/active_micro_futures_research_ladder.json")
GENERIC_PUBLICATION_LOCK: Final = Path("state/locks/micro_futures_publication.lock")
GENERIC_FAILURE_PARENT: Final = Path("state/micro_futures_publication_failed")
GENERIC_EVIDENCE_PARENT: Final = Path(
    "state/unpublished_evidence/micro_futures_catalog_migration_v1"
)
GENERIC_LANE_ID: Final = "micro_futures_integer_11"
EXPECTED_LEGACY_STATE: Final = "ACTIVE_CERTIFIED_SOURCE_ONLY"
EXPECTED_TERMINAL_STATE: Final = "SUCCESS_PUBLISHED_ACTIVE_MICRO_SOURCE_CATALOG"
EXPECTED_REPORT_STATE: Final = "PUBLISHED_AND_ACTIVE_SOURCE_CATALOG_ONLY"


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if type(value) is not dict:
        raise IntegrityError(f"{description} is not an object")
    return value


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


def _self_hash(value: Mapping[str, object], key: str, description: str) -> str:
    core = dict(value)
    observed = core.pop(key, None)
    if type(observed) is not str or observed != sha256_json(core):
        raise IntegrityError(f"{description} identity drifted")
    return observed


def load_legacy_publication(
    *, root: Path,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], dict[str, object]]:
    """Validate the successful legacy publication without opening row payloads."""

    catalog = _object(root / LEGACY_CATALOG_PATH, "legacy active micro catalog")
    pointer = _object(root / LEGACY_POINTER_PATH, "legacy active micro pointer")
    terminal = _object(root / LEGACY_TERMINAL_PATH, "legacy publication terminal")
    report = _object(root / LEGACY_REPORT_PATH, "legacy publication report")
    if catalog.get("state") != EXPECTED_LEGACY_STATE:
        raise IntegrityError("legacy catalog is not the accepted active source catalog")
    if terminal.get("state") != EXPECTED_TERMINAL_STATE:
        raise IntegrityError("legacy publication did not terminate successfully")
    if report.get("state") != EXPECTED_REPORT_STATE:
        raise IntegrityError("legacy publication report is not successful")
    if pointer.get("catalog_path") != LEGACY_CATALOG_PATH.as_posix():
        raise IntegrityError("legacy pointer does not select the legacy catalog")
    if pointer.get("catalog_sha256") != sha256_file(root / LEGACY_CATALOG_PATH):
        raise IntegrityError("legacy pointer catalog hash drifted")
    if pointer.get("catalog_id") != catalog.get("catalog_id"):
        raise IntegrityError("legacy pointer catalog identity drifted")
    if terminal.get("standard_active_catalog_mutated") is not False:
        raise IntegrityError("legacy publication unexpectedly changed the standard catalog")
    if terminal.get("year_2025_or_2026_payloads_opened") != 0:
        raise IntegrityError("legacy publication opened sealed or forward payloads")
    if terminal.get("terminal_written_last") is not True:
        raise IntegrityError("legacy publication terminal ordering is invalid")
    return catalog, pointer, terminal, report


def build_successor_catalog(
    *, legacy_catalog: Mapping[str, object], legacy_catalog_sha256: str
) -> dict[str, object]:
    """Build proposed generic catalog bytes while retaining legacy provenance."""

    core = {
        key: value
        for key, value in legacy_catalog.items()
        if key not in {"catalog_id", "schema_version", "lane_id"}
    }
    core.update(
        {
            "schema_version": "micro_futures_active_catalog/1.0.0",
            "lane_id": GENERIC_LANE_ID,
            "source_lane_id": legacy_catalog.get("lane_id"),
            "legacy_source": {
                "catalog_id": legacy_catalog.get("catalog_id"),
                "catalog_path": LEGACY_CATALOG_PATH.as_posix(),
                "catalog_sha256": legacy_catalog_sha256,
                "preserved_unchanged": True,
            },
        }
    )
    return {**core, "catalog_id": sha256_json(core)}


def build_successor_pointer(
    *,
    legacy_pointer: Mapping[str, object],
    legacy_pointer_sha256: str,
    catalog: Mapping[str, object],
) -> dict[str, object]:
    """Build proposed generic pointer bytes without writing the pointer."""

    core = {
        key: value
        for key, value in legacy_pointer.items()
        if key
        not in {
            "pointer_id",
            "schema_version",
            "lane_id",
            "catalog_id",
            "catalog_path",
            "catalog_sha256",
        }
    }
    core.update(
        {
            "schema_version": "active_micro_futures_research_ladder/1.0.0",
            "lane_id": GENERIC_LANE_ID,
            "source_lane_id": legacy_pointer.get("lane_id"),
            "catalog_id": catalog["catalog_id"],
            "catalog_path": GENERIC_CATALOG_PATH.as_posix(),
            "catalog_sha256": _canonical_file_sha(catalog),
            "legacy_source_pointer": {
                "pointer_id": legacy_pointer.get("pointer_id"),
                "pointer_path": LEGACY_POINTER_PATH.as_posix(),
                "pointer_sha256": legacy_pointer_sha256,
                "preserved_unchanged": True,
            },
        }
    )
    return {**core, "pointer_id": sha256_json(core)}


def build_plan(*, root: Path) -> dict[str, object]:
    """Describe a future cutover; create no active or execution artifact."""

    catalog, pointer, terminal, report = load_legacy_publication(root=root)
    legacy_catalog_sha = sha256_file(root / LEGACY_CATALOG_PATH)
    legacy_pointer_sha = sha256_file(root / LEGACY_POINTER_PATH)
    successor_catalog = build_successor_catalog(
        legacy_catalog=catalog,
        legacy_catalog_sha256=legacy_catalog_sha,
    )
    successor_pointer = build_successor_pointer(
        legacy_pointer=pointer,
        legacy_pointer_sha256=legacy_pointer_sha,
        catalog=successor_catalog,
    )
    core: dict[str, object] = {
        "schema_version": "micro_futures_catalog_migration_plan/1.0.0",
        "operation": OPERATION,
        "state": "PREPARED_ACTIVE_DATA_CUTOVER_APPROVAL_REQUIRED",
        "source": {
            "catalog_id": catalog["catalog_id"],
            "catalog_path": LEGACY_CATALOG_PATH.as_posix(),
            "catalog_sha256": legacy_catalog_sha,
            "pointer_id": pointer["pointer_id"],
            "pointer_path": LEGACY_POINTER_PATH.as_posix(),
            "pointer_sha256": legacy_pointer_sha,
            "terminal_id": terminal["terminal_id"],
            "terminal_path": LEGACY_TERMINAL_PATH.as_posix(),
            "terminal_sha256": sha256_file(root / LEGACY_TERMINAL_PATH),
            "report_path": LEGACY_REPORT_PATH.as_posix(),
            "report_sha256": sha256_file(root / LEGACY_REPORT_PATH),
            "preserve_all_bytes_and_paths": True,
        },
        "proposed_successor": {
            "catalog_id": successor_catalog["catalog_id"],
            "catalog_path": GENERIC_CATALOG_PATH.as_posix(),
            "catalog_sha256": _canonical_file_sha(successor_catalog),
            "pointer_id": successor_pointer["pointer_id"],
            "pointer_path": GENERIC_POINTER_PATH.as_posix(),
            "pointer_sha256": _canonical_file_sha(successor_pointer),
            "publication_lock": GENERIC_PUBLICATION_LOCK.as_posix(),
            "failure_parent": GENERIC_FAILURE_PARENT.as_posix(),
            "evidence_parent": GENERIC_EVIDENCE_PARENT.as_posix(),
            "lane_id": GENERIC_LANE_ID,
            "catalog": successor_catalog,
            "pointer": successor_pointer,
        },
        "authority": {
            "provider_or_network_access": False,
            "historical_row_read": False,
            "holdout_or_forward_read": False,
            "active_catalog_write": False,
            "active_pointer_write": False,
            "publication": False,
            "registration": False,
            "evaluation": False,
            "trading": False,
            "git_staging_commit_or_push": False,
        },
        "cutover_requirements": {
            "separate_plain_language_approval": True,
            "revalidate_source_hashes_immediately_before_cutover": True,
            "write_generic_catalog_create_only_before_pointer": True,
            "verify_generic_catalog_before_pointer": True,
            "preserve_legacy_catalog_and_pointer": True,
            "rollback": "REMOVE_ONLY_UNACCEPTED_GENERIC_SUCCESSOR_OUTPUTS_BEFORE_POINTER_ACCEPTANCE",
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def write_plan_create_only(*, root: Path) -> dict[str, object]:
    plan = build_plan(root=root)
    _write_create_only(root / PLAN_PATH, plan)
    return plan


def check_plan(*, root: Path) -> dict[str, object]:
    observed = _object(root / PLAN_PATH, "micro-futures migration plan")
    _self_hash(observed, "plan_id", "micro-futures migration plan")
    expected = build_plan(root=root)
    if observed != expected:
        raise IntegrityError("micro-futures migration plan reconstruction differs")
    return observed


__all__ = [
    "GENERIC_CATALOG_PATH",
    "GENERIC_EVIDENCE_PARENT",
    "GENERIC_FAILURE_PARENT",
    "GENERIC_LANE_ID",
    "GENERIC_POINTER_PATH",
    "GENERIC_PUBLICATION_LOCK",
    "LEGACY_CATALOG_PATH",
    "LEGACY_POINTER_PATH",
    "OPERATION",
    "PLAN_PATH",
    "build_plan",
    "build_successor_catalog",
    "build_successor_pointer",
    "check_plan",
    "load_legacy_publication",
    "write_plan_create_only",
]
