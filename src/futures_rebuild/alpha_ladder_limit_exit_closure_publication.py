"""Create-only publication for the closed Alpha resting-exit mechanism."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path

from .alpha_ladder_reported_trade_exit_successor import validate_closure
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError


CLOSURE_ID = "f73c6013c972539a42dcb8182e512b4b867c61a4f773513c048983530546b632"
PREPARED_CLOSURE_SHA256 = (
    "8da2bd762653ac1205d3685317eb4eb29ef21dfc24079d63847ab2663a1e17f5"
)
PREPARED_CLOSURE_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_limit_exit_closure"
) / CLOSURE_ID / "closure.json"
REGISTRY_ROOT = Path(
    "state/trial_registry/alpha_ladder_pre_registration_terminal_closure"
)


def _canonical_object(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"Alpha terminal closure is unreadable: {path}") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"Alpha terminal closure is not canonical: {path}")
    return value


def load_prepared_closure(*, root: Path) -> dict[str, object]:
    path = root / PREPARED_CLOSURE_PATH
    closure = _canonical_object(path)
    validate_closure(closure, root=root)
    if (
        closure.get("closure_id") != CLOSURE_ID
        or sha256_file(path) != PREPARED_CLOSURE_SHA256
        or closure.get("state") != "PREPARED_UNPUBLISHED_TERMINAL_CLOSURE"
        or closure.get("classification")
        != "PRE_REGISTRATION_SOURCE_INCOMPATIBLE_UNRESOLVED_EXIT_PATHS"
        or closure.get("mechanism_id")
        != "767ecf3987d816c2f657fbf030da25bf72511275812d6664aa6bd56faf7f3660"
        or closure.get("economic_result") != "NOT_PRODUCED"
        or closure.get("strategy_failure") is not False
        or closure.get("pilot_registration_status") != "FORBIDDEN"
        or closure.get("activation_authorized") is not False
    ):
        raise IntegrityError("prepared Alpha terminal closure changed")
    return closure


def build_published_closure(*, root: Path) -> dict[str, object]:
    prepared = load_prepared_closure(root=root)
    core = {
        **prepared,
        "schema_version": "alpha_ladder_published_pre_registration_closure/1.0.0",
        "prepared_schema_version": prepared["schema_version"],
        "prepared_state": prepared["state"],
        "state": "CLOSED_PRE_REGISTRATION_SOURCE_INCOMPATIBLE",
        "publication_authorized": True,
        "publication": True,
        "create_only": True,
        "prepared_source_path": PREPARED_CLOSURE_PATH.as_posix(),
        "prepared_source_sha256": PREPARED_CLOSURE_SHA256,
        "active_pointer_mutated": False,
    }
    return {**core, "publication_record_id": sha256_json(core)}


def registry_path() -> Path:
    return REGISTRY_ROOT / f"{CLOSURE_ID}.json"


def _create_or_verify(path: Path, payload: Mapping[str, object]) -> None:
    expected = canonical_bytes(dict(payload)) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != expected:
            raise IntegrityError(f"published Alpha closure differs: {path}")
        return
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, expected)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_closure(*, root: Path) -> dict[str, str]:
    payload = build_published_closure(root=root)
    destination = root / registry_path()
    _create_or_verify(destination, payload)
    return verify_published_closure(root=root)


def verify_published_closure(*, root: Path) -> dict[str, str]:
    expected = build_published_closure(root=root)
    destination = root / registry_path()
    published = _canonical_object(destination)
    published_core = dict(published)
    record_id = published_core.pop("publication_record_id", None)
    if (
        published != expected
        or record_id != sha256_json(published_core)
        or published.get("closure_id") != CLOSURE_ID
        or published.get("state")
        != "CLOSED_PRE_REGISTRATION_SOURCE_INCOMPATIBLE"
        or published.get("publication") is not True
        or published.get("create_only") is not True
        or published.get("active_pointer_mutated") is not False
        or published.get("activation_authorized") is not False
        or published.get("economic_result") != "NOT_PRODUCED"
        or published.get("strategy_failure") is not False
    ):
        raise IntegrityError("published Alpha terminal closure is inconsistent")
    return {
        "closure_id": CLOSURE_ID,
        "publication_record_id": str(record_id),
        "registry_path": registry_path().as_posix(),
        "registry_sha256": sha256_file(destination),
    }
