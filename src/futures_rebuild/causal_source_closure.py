"""Operation-bound metadata closure for the active canonical DBN source.

This module selects paths and verifies metadata.  It never opens DBN payloads.
A later separately authorized reader may consume only explicitly selected paths.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .canonical import sha256_file, sha256_json
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation_operation_firewall import (
    FoundationOperationContext,
    require_current_source_closure_context,
)


ACTIVE_CONTRACT_PATH = Path("configs/source_contract.json")
VERSIONED_CONTRACT_PATH = Path("configs/source_contract_v4.json")
_DBN_SUFFIX = ".dbn.zst"
_SIDECAR_SUFFIX = ".dbn.zst.manifest.json"
_FILE_INTERVAL = re.compile(
    r"^(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})(?:\.parent)?\.dbn\.zst$"
)


def _json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise IntegrityError(f"control document is not an object: {path}")
    return payload


def _contained(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve(strict=True)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ContractError("source-closure path escapes the repository") from exc
    return candidate


def _contract_and_context(
    root: Path,
    contract_path: Path | None,
    operation_context: FoundationOperationContext,
) -> tuple[Path, dict[str, Any]]:
    path = (contract_path or root / ACTIVE_CONTRACT_PATH).resolve(strict=True)
    contract = _json(path)
    contract_id = contract.get("contract_id")
    if not isinstance(contract_id, str):
        raise IntegrityError("current source contract ID is absent")
    require_current_source_closure_context(
        operation_context,
        source_contract_id=contract_id,
    )
    return path, contract


def _declared_dbn_identity(sidecar: Mapping[str, Any], dbn_path: Path) -> tuple[str, int]:
    canonical = sidecar.get("canonical_dbn")
    if isinstance(canonical, Mapping):
        if canonical.get("project_relative_path") != dbn_path.as_posix():
            raise IntegrityError("canonical sidecar path binding differs")
        digest, size = canonical.get("sha256"), canonical.get("size_bytes")
    elif "file_sha256" in sidecar:
        digest, size = sidecar.get("file_sha256"), sidecar.get("file_size_bytes")
    elif sidecar.get("schema_version") == "ohlcv_historical_backfill_sidecar/1.0.0":
        digest, size = sidecar.get("sha256"), sidecar.get("dbn_byte_size")
    else:
        digest, size = sidecar.get("sha256"), sidecar.get("byte_count")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise IntegrityError("canonical sidecar DBN hash is invalid")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise IntegrityError("canonical sidecar DBN size is invalid")
    return digest, size


def _utc_timestamp(value: object, name: str) -> datetime:
    if type(value) is not str or not value:
        raise ContractError(f"{name} is not an exact UTC timestamp")
    rendered = value if "T" in value else f"{value}T00:00:00Z"
    try:
        parsed = datetime.fromisoformat(rendered.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError(f"{name} is not an exact UTC timestamp") from exc
    if parsed.tzinfo != timezone.utc:
        raise ContractError(f"{name} is not an exact UTC timestamp")
    return parsed


def validate_full_build_selection_contract(
    contract: Mapping[str, Any],
    inventory_entries: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Validate the exact admitted full-build lane without opening DBN payloads."""

    contract_id = contract.get("contract_id")
    core = {key: value for key, value in contract.items() if key != "contract_id"}
    if type(contract_id) is not str or sha256_json(core) != contract_id:
        raise IntegrityError("source contract canonical identity differs")
    policy = contract.get("selection_policy")
    if not isinstance(policy, Mapping):
        raise IntegrityError("source selection policy is absent")
    admitted_dbns = [
        dict(item)
        for item in inventory_entries
        if item.get("admitted_standard_foundation") is True
        and item.get("kind") == "DBN"
    ]
    if (
        policy.get("admitted_standard_dbn_file_count") != len(admitted_dbns)
        or policy.get("admitted_standard_dbn_inventory_sha256")
        != sha256_json(admitted_dbns)
    ):
        raise IntegrityError("source contract admitted DBN identity differs")
    development_end = _utc_timestamp(
        policy.get("development_end_exclusive"), "development end"
    )
    maximum_file_end = _utc_timestamp(
        policy.get("file_interval_end_maximum"), "maximum file end"
    )
    if maximum_file_end != development_end:
        raise IntegrityError("source contract file maximum differs from development end")
    return {
        "admitted_standard_dbn_file_count": len(admitted_dbns),
        "admitted_standard_dbn_inventory_sha256": sha256_json(admitted_dbns),
        "development_end_exclusive": policy["development_end_exclusive"],
    }


def reconstruct_content_inventory(
    repository_root: Path,
    *,
    operation_context: FoundationOperationContext,
    contract_path: Path | None = None,
) -> list[dict[str, object]]:
    """Reconstruct exact source metadata without opening a DBN payload."""

    root = repository_root.resolve(strict=True)
    _, contract = _contract_and_context(root, contract_path, operation_context)
    dbn_root = _contained(root, str(contract["active_canonical_source"]["canonical_root"]))
    rows: list[dict[str, object]] = []
    sidecars: list[Path] = []
    for directory, directories, filenames in os.walk(dbn_root):
        directories.sort()
        for filename in sorted(filenames):
            if filename.endswith(_SIDECAR_SUFFIX):
                sidecars.append(Path(directory) / filename)
    for sidecar_path in sorted(sidecars):
        sidecar_bytes = sidecar_path.read_bytes()
        sidecar = json.loads(sidecar_bytes)
        if not isinstance(sidecar, dict):
            raise IntegrityError("canonical sidecar is not an object")
        dbn_path = Path(str(sidecar_path)[: -len(".manifest.json")])
        project_dbn = dbn_path.relative_to(root).as_posix()
        digest, size = _declared_dbn_identity(sidecar, Path(project_dbn))
        stat = dbn_path.stat()
        if dbn_path.is_symlink() or not dbn_path.is_file() or stat.st_size != size:
            raise IntegrityError("canonical DBN metadata differs from its sidecar")
        rows.append({"relative_path": dbn_path.relative_to(dbn_root).as_posix(), "size_bytes": size, "sha256": digest})
        rows.append({"relative_path": sidecar_path.relative_to(dbn_root).as_posix(), "size_bytes": len(sidecar_bytes), "sha256": hashlib.sha256(sidecar_bytes).hexdigest()})
    rows.sort(key=lambda item: str(item["relative_path"]))
    return rows


def _validate_contract_documents(
    root: Path,
    contract: Mapping[str, Any],
    *,
    inventory_path_override: Path | None,
) -> tuple[Mapping[str, Any], list[dict[str, object]]]:
    if contract.get("schema_version") != "canonical_dbn_source_contract/4.0.0":
        raise IntegrityError("current source contract schema differs")
    if contract.get("authority") != {
        "activation": False,
        "deletion": False,
        "evaluation": False,
        "holdout": False,
        "model": False,
        "provider": False,
        "row_read": False,
        "trading": False,
    }:
        raise UnauthorizedOperation("source contract grants unexpected authority")
    source = contract.get("active_canonical_source")
    if not isinstance(source, Mapping):
        raise IntegrityError("active canonical source binding is absent")
    pointer_path = _contained(root, str(source["pointer_path"]))
    if sha256_file(pointer_path) != source.get("pointer_sha256"):
        raise IntegrityError("active DBN pointer hash differs")
    pointer = _json(pointer_path)
    if pointer.get("release_id") != source.get("release_id"):
        raise IntegrityError("active DBN release ID differs")
    release_path = _contained(root, str(source["release_manifest_path"]))
    if sha256_file(release_path) != source.get("release_manifest_sha256"):
        raise IntegrityError("active DBN release manifest differs")
    release = _json(release_path)
    if release.get("release_id") != source.get("release_id"):
        raise IntegrityError("active release document ID differs")
    inventory_binding = contract.get("complete_inventory")
    if not isinstance(inventory_binding, Mapping):
        raise IntegrityError("complete inventory binding is absent")
    inventory_path = (
        inventory_path_override.resolve(strict=True)
        if inventory_path_override is not None
        else _contained(root, str(inventory_binding["path"]))
    )
    if sha256_file(inventory_path) != inventory_binding.get("sha256"):
        raise IntegrityError("complete inventory document differs")
    inventory = _json(inventory_path)
    entries = inventory.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("complete inventory entries are absent")
    content_rows = [
        {"relative_path": str(item["path"])[len("data/dbn/"):], "size_bytes": int(item["size_bytes"]), "sha256": str(item["sha256"])}
        for item in entries
    ]
    if sha256_json(content_rows) != inventory_binding.get("content_inventory_sha256"):
        raise IntegrityError("complete inventory identity differs")
    shadow = release.get("complete_shadow_tree")
    if not isinstance(shadow, Mapping) or (
        shadow.get("file_count") != len(content_rows)
        or shadow.get("total_bytes") != sum(int(item["size_bytes"]) for item in content_rows)
        or shadow.get("inventory_sha256") != sha256_json(content_rows)
    ):
        raise IntegrityError("active release complete-root binding differs")
    universe = contract.get("universe")
    if not isinstance(universe, Mapping) or set(universe["standard_roots"]) & set(universe["deferred_micro_roots"]):
        raise IntegrityError("standard and micro source lanes are absent or overlap")
    return source, content_rows


def _result(contract: Mapping[str, Any], source: Mapping[str, Any], rows: list[dict[str, object]]) -> dict[str, object]:
    universe = contract["universe"]
    return {
        "contract_id": contract["contract_id"],
        "release_id": source["release_id"],
        "content_inventory_sha256": sha256_json(rows),
        "file_count": len(rows),
        "total_bytes": sum(int(item["size_bytes"]) for item in rows),
        "standard_root_count": len(universe["standard_roots"]),
        "deferred_micro_root_count": len(universe["deferred_micro_roots"]),
        "payload_files_opened": 0,
        "row_reads": 0,
        "valid": True,
    }


def validate_source_contract_metadata(
    repository_root: Path,
    *,
    operation_context: FoundationOperationContext,
    contract_path: Path | None = None,
    inventory_path_override: Path | None = None,
) -> dict[str, object]:
    """Validate installed contract paths and immutable metadata without source traversal."""

    root = repository_root.resolve(strict=True)
    _, contract = _contract_and_context(root, contract_path, operation_context)
    source, rows = _validate_contract_documents(root, contract, inventory_path_override=inventory_path_override)
    return _result(contract, source, rows)


def validate_source_closure(
    repository_root: Path,
    *,
    operation_context: FoundationOperationContext,
    contract_path: Path | None = None,
    inventory_path_override: Path | None = None,
) -> dict[str, object]:
    """Validate physical source metadata without decoding a market row."""

    root = repository_root.resolve(strict=True)
    _, contract = _contract_and_context(root, contract_path, operation_context)
    source, rows = _validate_contract_documents(root, contract, inventory_path_override=inventory_path_override)
    reconstructed = reconstruct_content_inventory(root, operation_context=operation_context, contract_path=contract_path)
    if reconstructed != rows:
        raise IntegrityError("canonical root differs from the exact inventory")
    return _result(contract, source, rows)


def select_standard_dbn_paths(
    repository_root: Path,
    *,
    operation_context: FoundationOperationContext,
    market: str,
    family: str,
    contract_path: Path | None = None,
    inventory_path_override: Path | None = None,
) -> tuple[str, ...]:
    root = repository_root.resolve(strict=True)
    _, contract = _contract_and_context(root, contract_path, operation_context)
    universe = contract["universe"]
    if market not in universe["standard_roots"] or market in universe["deferred_micro_roots"]:
        raise UnauthorizedOperation("market is not admitted to the standard source lane")
    if family not in contract["selection_policy"]["allowed_families"]:
        raise UnauthorizedOperation("source family is not admitted to causal foundation")
    inventory_path = inventory_path_override.resolve(strict=True) if inventory_path_override else _contained(root, contract["complete_inventory"]["path"])
    inventory = _json(inventory_path)
    selected = tuple(
        str(item["path"])
        for item in inventory["entries"]
        if item["kind"] == "DBN" and item["market"] == market and item["family"] == family and item["admitted_standard_foundation"] is True
    )
    if not selected:
        raise IntegrityError("exact admitted source selection is empty")
    if selected != tuple(sorted(selected)) or len(set(selected)) != len(selected):
        raise IntegrityError("exact admitted source selection is not unique and sorted")
    return selected


def reject_unlisted_source_path(
    repository_root: Path,
    path: str,
    *,
    operation_context: FoundationOperationContext,
    contract_path: Path | None = None,
    inventory_path_override: Path | None = None,
) -> None:
    root = repository_root.resolve(strict=True)
    _, contract = _contract_and_context(root, contract_path, operation_context)
    inventory_path = inventory_path_override.resolve(strict=True) if inventory_path_override else _contained(root, contract["complete_inventory"]["path"])
    inventory = _json(inventory_path)
    admitted = {str(item["path"]) for item in inventory["entries"] if item.get("admitted_standard_foundation") is True}
    if path not in admitted:
        raise UnauthorizedOperation("path is not an exact admitted current source")


def interval_end(path: str) -> str:
    match = _FILE_INTERVAL.fullmatch(Path(path).name)
    if match is None:
        raise IntegrityError("canonical DBN filename lacks an exact interval")
    return match.group("end")


def select_exact_standard_source_entries(
    repository_root: Path,
    *,
    operation_context: FoundationOperationContext,
    source_entries: Sequence[Mapping[str, object]],
    windows: Mapping[str, Mapping[str, str]],
) -> tuple[dict[str, object], ...]:
    """Validate one literal development-only source set without payload access.

    The active alias and its complete inventory are the only authorities.  The
    caller supplies a literal plan set; no directory, filename, version, or
    recency-based release discovery is performed.
    """

    root = repository_root.resolve(strict=True)
    _, contract = _contract_and_context(root, None, operation_context)
    _validate_contract_documents(root, contract, inventory_path_override=None)
    policy = contract.get("selection_policy")
    universe = contract.get("universe")
    if not isinstance(policy, Mapping) or not isinstance(universe, Mapping):
        raise IntegrityError("active source selection policy is absent")
    if (
        policy.get("recursive_discovery") is not False
        or policy.get("new" + "est_or_" + "lat" + "est_fallback") is not False
        or policy.get("filename_or_version_guessing") is not False
        or policy.get("crossing_partition_policy") != "REJECT_WHOLE_FILE_BEFORE_OPEN"
        or policy.get("sealed_holdout_or_forward_file_open") is not False
    ):
        raise UnauthorizedOperation("active source policy permits fallback or protected access")
    standard = tuple(universe.get("standard_roots", ()))
    deferred = tuple(universe.get("deferred_micro_roots", ()))
    if len(standard) != 41 or len(deferred) != 17 or set(standard) & set(deferred):
        raise IntegrityError("active standard and deferred source lanes differ")

    inventory_path = _contained(root, str(contract["complete_inventory"]["path"]))
    inventory = _json(inventory_path)
    inventory_entries = inventory.get("entries")
    if not isinstance(inventory_entries, list):
        raise IntegrityError("active complete source inventory is absent")
    by_path = {str(item["path"]): item for item in inventory_entries}
    if len(by_path) != len(inventory_entries):
        raise IntegrityError("active complete source inventory has duplicate paths")

    development_end = _utc_timestamp(
        policy.get("development_end_exclusive"), "development end"
    )
    maximum_file_end = _utc_timestamp(
        policy.get("file_interval_end_maximum"), "maximum file end"
    )
    allowed_families = set(policy.get("allowed_families", ()))
    normalized_windows: dict[str, tuple[datetime, datetime]] = {}
    for market, window in windows.items():
        if market not in standard or market in deferred or not isinstance(window, Mapping):
            raise UnauthorizedOperation("canary window uses a nonstandard market")
        try:
            start = _utc_timestamp(window["start"], "window start")
            end = _utc_timestamp(window["end"], "window end")
        except KeyError as exc:
            raise ContractError("canary window is not an exact UTC interval") from exc
        if (
            start.tzinfo != timezone.utc
            or end.tzinfo != timezone.utc
            or not start < end <= development_end
        ):
            raise UnauthorizedOperation("canary window crosses the development boundary")
        normalized_windows[market] = (start, end)

    selected: list[dict[str, object]] = []
    seen: set[str] = set()
    pairs: dict[str, set[str]] = {}
    for raw in source_entries:
        if not isinstance(raw, Mapping) or type(raw.get("path")) is not str:
            raise IntegrityError("planned source entry is malformed")
        path = str(raw["path"])
        if path in seen:
            raise IntegrityError("planned source set contains a duplicate path")
        seen.add(path)
        current = by_path.get(path)
        if current is None or dict(raw) != current:
            raise IntegrityError("planned source entry differs from the active exact inventory")
        market = str(current.get("market"))
        family = str(current.get("family"))
        kind = str(current.get("kind"))
        interval_start = _utc_timestamp(
            current.get("interval_start_inclusive"), "source interval start"
        )
        interval_end = _utc_timestamp(
            current.get("interval_end_exclusive"), "source interval end"
        )
        requested = normalized_windows.get(market)
        if (
            requested is None
            or market not in standard
            or market in deferred
            or family not in allowed_families
            or current.get("lane") != "STANDARD_41"
            or current.get("admitted_standard_foundation") is not True
            or kind not in {"DBN", "SIDECAR"}
            or not interval_start < interval_end <= maximum_file_end
        ):
            raise UnauthorizedOperation("planned source entry is not admitted development source")
        base = path[:-len(".manifest.json")] if kind == "SIDECAR" else path
        if not base.endswith(_DBN_SUFFIX):
            raise IntegrityError("planned source entry is not a canonical DBN or sidecar")
        pairs.setdefault(base, set()).add(kind)
        selected.append(dict(current))
    if not selected or any(kinds != {"DBN", "SIDECAR"} for kinds in pairs.values()):
        raise IntegrityError("planned source set lacks exact DBN/sidecar pairs")
    if tuple(str(item["path"]) for item in selected) != tuple(sorted(seen)):
        raise IntegrityError("planned source entries are not uniquely sorted")
    if set(normalized_windows) != {str(item["market"]) for item in selected}:
        raise IntegrityError("canary windows and exact source markets differ")
    return tuple(selected)
