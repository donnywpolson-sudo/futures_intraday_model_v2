"""Atomic local checkpoints for resumable bracket market-year staging.

The checkpoint is an append-only ledger of already-written chunk pairs.  A
payload becomes visible only after both parquet files exist and their hashes
are recorded in one canonical, atomically replaced checkpoint file.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Mapping, Sequence

from .canonical import canonical_bytes, contained_path, sha256_file, sha256_json
from .errors import IntegrityError


SCHEMA = "tier1_bracket_checkpoint/2.0.0"
MAX_CARRY_ROWS = 80


def checkpoint_path(*, root: Path, context: Mapping[str, str], market: str, year: int) -> Path:
    if market not in {"ES", "CL", "ZN", "6E"} or type(year) is not int:
        raise IntegrityError("bracket checkpoint scope is invalid")
    return root / "state" / "tier1_bracket_checkpoints" / sha256_json(dict(context)) / f"{market}-{year}.json"


def _initial_payload(*, context: Mapping[str, str]) -> dict[str, object]:
    return {
        "schema_version": SCHEMA,
        "context": dict(context),
        "chunks": [],
        "input_rows": 0,
        "output_rows": 0,
        "resolved_identity_hashes": [],
        "cursor": None,
        "carry_rows": [],
        "complete": False,
    }


def _with_id(core: Mapping[str, object]) -> dict[str, object]:
    value = dict(core)
    value["checkpoint_id"] = sha256_json(value)
    return value


def _validate_chunk(*, root: Path, chunk: Mapping[str, object], expected_sequence: int) -> None:
    if chunk.get("sequence") != expected_sequence or chunk.get("row_count") is None or type(chunk["row_count"]) is not int or chunk["row_count"] <= 0:
        raise IntegrityError("bracket checkpoint chunk metadata is invalid")
    for name, hash_name in (("feature_payload", "feature_payload_sha256"), ("outcome_payload", "outcome_payload_sha256")):
        relative, digest = chunk.get(name), chunk.get(hash_name)
        if not isinstance(relative, str) or not isinstance(digest, str) or len(digest) != 64:
            raise IntegrityError("bracket checkpoint chunk path is invalid")
        path = contained_path(root, relative)
        if not path.is_file() or sha256_file(path) != digest:
            raise IntegrityError("bracket checkpoint chunk differs from its recorded hash")


def _validate_payload(*, payload: object, context: Mapping[str, str], root: Path) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise IntegrityError("bracket checkpoint is invalid")
    core = {key: value for key, value in payload.items() if key != "checkpoint_id"}
    if (
        payload.get("schema_version") != SCHEMA
        or payload.get("context") != dict(context)
        or payload.get("checkpoint_id") != sha256_json(core)
        or not isinstance(payload.get("chunks"), list)
        or not isinstance(payload.get("carry_rows"), list)
        or len(payload["carry_rows"]) > MAX_CARRY_ROWS
        or not isinstance(payload.get("complete"), bool)
    ):
        raise IntegrityError("bracket checkpoint differs from current inputs")
    for name in ("input_rows", "output_rows"):
        if type(payload.get(name)) is not int or int(payload[name]) < 0:
            raise IntegrityError("bracket checkpoint counts are invalid")
    if int(payload["output_rows"]) > int(payload["input_rows"]):
        raise IntegrityError("bracket checkpoint row counts are invalid")
    cursor = payload.get("cursor")
    if cursor is not None:
        if not isinstance(cursor, dict) or type(cursor.get("ordinal")) is not int or type(cursor.get("event_at_ns")) is not int or not isinstance(cursor.get("source_row_sha256"), str):
            raise IntegrityError("bracket checkpoint cursor is invalid")
        if cursor["ordinal"] + 1 != payload["input_rows"]:
            raise IntegrityError("bracket checkpoint cursor does not match input count")
    elif payload["input_rows"] != 0:
        raise IntegrityError("bracket checkpoint input count lacks a cursor")
    identities = payload.get("resolved_identity_hashes")
    if not isinstance(identities, list) or identities != sorted(set(identities)) or any(not isinstance(value, str) or len(value) != 64 for value in identities):
        raise IntegrityError("bracket checkpoint identities are invalid")
    for sequence, chunk in enumerate(payload["chunks"]):
        if not isinstance(chunk, dict):
            raise IntegrityError("bracket checkpoint chunk is invalid")
        _validate_chunk(root=root, chunk=chunk, expected_sequence=sequence)
    return payload


def load_checkpoint(*, path: Path, context: Mapping[str, str], root: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("bracket checkpoint is unreadable") from exc
    if raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError("bracket checkpoint is invalid")
    return _validate_payload(payload=payload, context=context, root=root)


def _write_payload(*, path: Path, payload: Mapping[str, object], context: Mapping[str, str], root: Path) -> dict[str, object]:
    final = _with_id(payload)
    _validate_payload(payload=final, context=context, root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_bytes(canonical_bytes(final) + b"\n")
    os.replace(temporary, path)
    return final


def append_chunk(
    *, path: Path, root: Path, context: Mapping[str, str], chunk: Mapping[str, object],
    input_rows: int, cursor: Mapping[str, object], carry_rows: Sequence[Mapping[str, object]],
    resolved_identity_hashes: Sequence[str],
) -> dict[str, object]:
    """Atomically make one already-hashed feature/outcome pair resumable."""

    existing = load_checkpoint(path=path, context=context, root=root)
    core = _initial_payload(context=context) if existing is None else {key: value for key, value in existing.items() if key != "checkpoint_id"}
    if core["complete"]:
        raise IntegrityError("completed bracket checkpoint cannot accept another chunk")
    chunks = list(core["chunks"])
    new_chunk = dict(chunk)
    _validate_chunk(root=root, chunk=new_chunk, expected_sequence=len(chunks))
    if type(input_rows) is not int or input_rows < int(core["input_rows"]):
        raise IntegrityError("bracket checkpoint input cursor moved backward")
    if input_rows == int(core["input_rows"]) and core.get("cursor") != dict(cursor):
        raise IntegrityError("bracket checkpoint cursor changed without new input")
    if len(carry_rows) > MAX_CARRY_ROWS:
        raise IntegrityError("bracket writer retained too many carry rows")
    core.update({
        "chunks": [*chunks, new_chunk],
        "input_rows": input_rows,
        "output_rows": int(core["output_rows"]) + int(new_chunk["row_count"]),
        "resolved_identity_hashes": sorted(set(resolved_identity_hashes)),
        "cursor": dict(cursor),
        "carry_rows": [dict(row) for row in carry_rows],
    })
    return _write_payload(path=path, payload=core, context=context, root=root)


def finalize_checkpoint(
    *, path: Path, root: Path, context: Mapping[str, str], input_rows: int,
    cursor: Mapping[str, object] | None, carry_rows: Sequence[Mapping[str, object]],
    resolved_identity_hashes: Sequence[str],
) -> dict[str, object]:
    """Mark a fully consumed stream complete without adding a synthetic chunk."""

    existing = load_checkpoint(path=path, context=context, root=root)
    core = _initial_payload(context=context) if existing is None else {key: value for key, value in existing.items() if key != "checkpoint_id"}
    if core["complete"]:
        return existing  # Exact validated reuse.
    if input_rows < int(core["input_rows"]) or len(carry_rows) > MAX_CARRY_ROWS:
        raise IntegrityError("bracket checkpoint final state is invalid")
    core.update({
        "input_rows": input_rows,
        "cursor": None if cursor is None else dict(cursor),
        "carry_rows": [dict(row) for row in carry_rows],
        "resolved_identity_hashes": sorted(set(resolved_identity_hashes)),
        "complete": True,
    })
    return _write_payload(path=path, payload=core, context=context, root=root)
