"""Read-only verification for the declared eight-market successor candidates."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Mapping

from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    contained_path,
    sha256_bytes,
    sha256_file,
    sha256_json,
)
from .errors import ContractError, IntegrityError


SCHEMA_VERSION = "eight_market_successor_candidate/1.0.0"
INVENTORY_SCHEMA_VERSION = "eight_market_successor_inventory/1.0.0"
_TOP_LEVEL_KEYS = {
    "schema_version",
    "classification",
    "source_root",
    "parent_release",
    "markets",
    "families",
    "expected_candidate",
    "expected_union",
    "excluded_relative_paths",
    "authority",
}
_SIDECAR_REQUIRED = {
    "dataset",
    "file_sha256",
    "file_size_bytes",
    "market",
    "path",
    "request_status",
    "schema",
    "stype_in",
    "symbols_requested",
    "vendor",
}


class SuccessorInventoryError(ContractError):
    """The declared successor inventory failed closed."""


def _object(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SuccessorInventoryError(f"{name} must be an object")
    return dict(value)


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SuccessorInventoryError(f"{name} must be a positive integer")
    return value


def _strings(value: object, name: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise SuccessorInventoryError(f"{name} must be a non-empty string list")
    result = tuple(value)
    if len(set(result)) != len(result):
        raise SuccessorInventoryError(f"{name} must be unique")
    return result


def load_candidate_contract(path: Path) -> dict[str, Any]:
    assert_plain_file(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SuccessorInventoryError("candidate contract is not valid UTF-8 JSON") from exc
    contract = _object(payload, "candidate contract")
    if set(contract) != _TOP_LEVEL_KEYS:
        raise SuccessorInventoryError("candidate contract keys are not exact")
    if contract["schema_version"] != SCHEMA_VERSION:
        raise SuccessorInventoryError("candidate contract schema is unsupported")
    if contract["classification"] != "NON_AUTHORIZING_READ_ONLY_SOURCE_INVENTORY":
        raise SuccessorInventoryError("candidate contract classification is invalid")
    if not isinstance(contract["source_root"], str) or not Path(
        contract["source_root"]
    ).is_absolute():
        raise SuccessorInventoryError("source_root must be absolute")
    _strings(contract["markets"], "markets")
    families = _object(contract["families"], "families")
    if not families or any(
        not isinstance(key, str)
        or not key
        or not isinstance(value, str)
        or not value
        for key, value in families.items()
    ):
        raise SuccessorInventoryError("families must map names to schemas")
    _strings(contract["excluded_relative_paths"], "excluded_relative_paths")
    authority = _object(contract["authority"], "authority")
    if set(authority) != {
        "provider_calls_authorized",
        "copy_authorized",
        "destination_mutation_authorized",
        "legacy_mutation_authorized",
    } or any(value is not False for value in authority.values()):
        raise SuccessorInventoryError("candidate inventory cannot grant authority")
    for section in ("parent_release", "expected_candidate", "expected_union"):
        values = _object(contract[section], section)
        for key, value in values.items():
            if key == "release_id":
                if not isinstance(value, str) or len(value) != 64:
                    raise SuccessorInventoryError("parent release_id is invalid")
            else:
                _positive_int(value, f"{section}.{key}")
    return contract


def _relative(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError as exc:
        raise SuccessorInventoryError(f"path escapes source root: {path}") from exc


def _expected_symbol(*, family: str, market: str, filename: str) -> tuple[str, str]:
    if family == "definition" or filename.endswith(".parent.dbn.zst"):
        return f"{market}.FUT", "parent"
    return f"{market}.v.0", "continuous"


def _read_sidecar(path: Path) -> Mapping[str, Any]:
    assert_plain_file(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SuccessorInventoryError(f"invalid sidecar JSON: {path}") from exc
    if not isinstance(value, dict) or not _SIDECAR_REQUIRED <= set(value):
        raise SuccessorInventoryError(f"sidecar required fields are missing: {path}")
    return value


def build_inventory(
    contract_path: Path, *, contract_reference: str | None = None
) -> dict[str, Any]:
    contract = load_candidate_contract(contract_path)
    root = Path(contract["source_root"]).resolve(strict=True)
    if not root.is_dir():
        raise SuccessorInventoryError("source_root is not a directory")
    assert_no_linklike_ancestors(root)
    markets = tuple(contract["markets"])
    families: Mapping[str, str] = contract["families"]
    exclusions = set(contract["excluded_relative_paths"])
    observed_exclusions: set[str] = set()
    records: list[dict[str, Any]] = []

    for family in sorted(families):
        expected_schema = families[family]
        for market in markets:
            directory = contained_path(root, f"data/dbn/{family}/{market}")
            if not directory.is_dir():
                raise SuccessorInventoryError(
                    f"declared source directory is missing: {family}/{market}"
                )
            assert_no_linklike_ancestors(directory)
            for candidate in sorted(directory.rglob("*")):
                relative = _relative(root, candidate)
                if relative in exclusions:
                    assert_plain_file(candidate, reject_hardlinks=False)
                    observed_exclusions.add(relative)
                    continue
                if candidate.is_dir():
                    continue
                if ".tmp/" in relative or ".tmp\\" in relative:
                    raise SuccessorInventoryError(
                        f"undeclared temporary file is present: {relative}"
                    )
                if not candidate.name.endswith(".dbn.zst"):
                    if candidate.name.endswith(".dbn.zst.manifest.json"):
                        continue
                    raise SuccessorInventoryError(
                        f"undeclared source file is present: {relative}"
                    )
                info = assert_plain_file(candidate, reject_hardlinks=False)
                sidecar = Path(f"{candidate}.manifest.json")
                sidecar_info = assert_plain_file(sidecar, reject_hardlinks=False)
                metadata = _read_sidecar(sidecar)
                digest = sha256_file(candidate, reject_hardlinks=False)
                sidecar_digest = sha256_file(sidecar, reject_hardlinks=False)
                expected_symbol, expected_stype = _expected_symbol(
                    family=family, market=market, filename=candidate.name
                )
                expected_relative = relative
                checks = {
                    "dataset": metadata.get("dataset") == "GLBX.MDP3",
                    "vendor": metadata.get("vendor") == "databento",
                    "request_status": metadata.get("request_status") == "ok",
                    "schema": metadata.get("schema") == expected_schema,
                    "market": metadata.get("market") == market,
                    "path": metadata.get("path") == expected_relative,
                    "file_sha256": metadata.get("file_sha256") == digest,
                    "file_size_bytes": metadata.get("file_size_bytes") == info.st_size,
                    "symbols_requested": metadata.get("symbols_requested")
                    == [expected_symbol],
                    "stype_in": metadata.get("stype_in") == expected_stype,
                }
                failed = sorted(key for key, passed in checks.items() if not passed)
                if failed:
                    raise SuccessorInventoryError(
                        f"sidecar mismatch for {relative}: {','.join(failed)}"
                    )
                records.append(
                    {
                        "destination_path": expected_relative,
                        "source_path": expected_relative,
                        "sidecar_path": f"{expected_relative}.manifest.json",
                        "family": family,
                        "schema": expected_schema,
                        "market": market,
                        "start": metadata.get("start"),
                        "end": metadata.get("end"),
                        "job_id": metadata.get("job_id"),
                        "dbn_sha256": digest,
                        "dbn_bytes": info.st_size,
                        "sidecar_sha256": sidecar_digest,
                        "sidecar_bytes": sidecar_info.st_size,
                    }
                )

    if observed_exclusions != exclusions:
        missing = sorted(exclusions - observed_exclusions)
        raise SuccessorInventoryError(
            f"declared exclusions are missing: {','.join(missing)}"
        )
    destinations = [item["destination_path"] for item in records]
    if len(destinations) != len(set(destinations)):
        raise SuccessorInventoryError("candidate destination paths are duplicated")

    dbn_bytes = sum(item["dbn_bytes"] for item in records)
    sidecar_bytes = sum(item["sidecar_bytes"] for item in records)
    candidate = {
        "dbn_files": len(records),
        "sidecar_files": len(records),
        "combined_files": 2 * len(records),
        "combined_bytes": dbn_bytes + sidecar_bytes,
    }
    if candidate != contract["expected_candidate"]:
        raise SuccessorInventoryError(
            f"candidate totals mismatch: expected {contract['expected_candidate']}, "
            f"observed {candidate}"
        )
    parent = contract["parent_release"]
    union = {
        "dbn_files": parent["dbn_files"] + candidate["dbn_files"],
        "sidecar_files": parent["sidecar_files"] + candidate["sidecar_files"],
        "combined_files": parent["combined_files"] + candidate["combined_files"],
        "combined_bytes": parent["combined_bytes"] + candidate["combined_bytes"],
        "market_count": contract["expected_union"]["market_count"],
    }
    if union != contract["expected_union"]:
        raise SuccessorInventoryError("expected union does not reconcile")

    reference = contract_path.as_posix() if contract_reference is None else contract_reference
    reference_path = Path(reference)
    if contract_reference is not None and (
        not reference
        or reference_path.is_absolute()
        or ".." in reference_path.parts
        or reference_path.as_posix() != reference
    ):
        raise SuccessorInventoryError("contract reference must be canonical and relative")
    inventory_core = {
        "schema_version": INVENTORY_SCHEMA_VERSION,
        "classification": "NON_AUTHORIZING_VERIFIED_SOURCE_INVENTORY",
        "contract_path": reference,
        "contract_sha256": sha256_file(contract_path),
        "parent_release_id": parent["release_id"],
        "markets": list(markets),
        "candidate_totals": candidate,
        "expected_union": union,
        "excluded_relative_paths": sorted(exclusions),
        "records": records,
        "authority": {
            "provider_calls_authorized": False,
            "copy_authorized": False,
            "destination_mutation_authorized": False,
            "legacy_mutation_authorized": False,
        },
    }
    inventory_core["inventory_id"] = sha256_json(inventory_core)
    return inventory_core


def _write_new(path: Path, payload: Mapping[str, Any]) -> None:
    if path.exists():
        raise SuccessorInventoryError(f"refusing to overwrite output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    data = canonical_bytes(payload) + b"\n"
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path("configs/eight_market_successor_candidate.json"),
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        inventory = build_inventory(args.contract)
        if args.output is not None:
            _write_new(args.output, inventory)
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "inventory_id": inventory["inventory_id"],
                    "candidate_totals": inventory["candidate_totals"],
                    "expected_union": inventory["expected_union"],
                    "copy_authorized": False,
                },
                sort_keys=True,
            )
        )
        return 0
    except (SuccessorInventoryError, ContractError, IntegrityError, OSError) as exc:
        print(json.dumps({"status": "FAIL", "error": str(exc)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
