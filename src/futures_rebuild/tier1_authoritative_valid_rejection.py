"""Publish the executed authoritative trial as a valid rejection.

This lane is deliberately distinct from invalid retirement.  It accepts only
the already-sealed terminal decision, publishes a create-only closure, and
atomically replaces the active pointer with a non-executable tombstone.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from .tier1_final_unpublished_evidence import verify_unpublished_evidence


TRIAL_ID = "5b01056d2ce98641262988df21695ce0cc3eae845c2276bcc12e415f1c192fa4"
MANIFEST_ID = "c18ef7e97ace8794473cf1163459a2a2202795f205a710817db4cc9cb6cb4a94"
AUTHORIZATION_RECEIPT_ID = "44a6fc7c03fcac6390245eca83fc853dbe4562b9e56308a0fe8ffcbe044b36c6"
PREPARATION_PATH = Path("configs/tier1_authoritative_valid_rejection_closure_preparation.json")
ACTIVE_POINTER_PATH = Path("configs/active_tier1_trial.json")
BUNDLE_PATH = Path("state/tier1_authoritative_unpublished_evidence") / MANIFEST_ID
CLOSURE_ROOT = Path("state/trial_registry/tier1_authoritative_valid_rejection_closure")
EVENT_ROOT = Path("state/trial_events/tier1_authoritative_valid_rejection_closure")


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"valid-rejection artifact is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("valid-rejection artifact is not an object")
    return value


def load_valid_rejection_preparation(*, root: Path) -> dict[str, object]:
    preparation = _object(root / PREPARATION_PATH)
    core = dict(preparation)
    record_id = core.pop("record_id", None)
    bindings = preparation.get("sealed_bindings")
    decision = preparation.get("decision")
    if (
        record_id != sha256_json(core)
        or preparation.get("schema_version")
        != "tier1_authoritative_valid_rejection_closure_preparation/1.0.0"
        or preparation.get("state") != "PREPARED_REQUIRES_PUBLICATION_APPROVAL"
        or preparation.get("trial_id") != TRIAL_ID
        or preparation.get("manifest_id") != MANIFEST_ID
        or preparation.get("authorization_receipt_id") != AUTHORIZATION_RECEIPT_ID
        or preparation.get("disposition") != "VALID_REJECTION_AFTER_SEALED_HISTORICAL_EXECUTION"
        or not isinstance(bindings, Mapping)
        or not isinstance(decision, Mapping)
        or decision.get("classification") != "REJECT_HISTORICAL_SCREEN_MANDATORY_GATE"
        or decision.get("candidate_selected_path_complete") is not True
        or decision.get("inference_executed") is not True
        or decision.get("missing_data_helped_decision") is not False
        or decision.get("promotion_possible") is not False
        or decision.get("failed_mandatory_gates")
        != ["STRESS_NET_PNL_NOT_POSITIVE", "CONTINUOUS_DRAWDOWN_EXCEEDS_1500_USD"]
        or bindings.get(ACTIVE_POINTER_PATH.as_posix()) != sha256_file(root / ACTIVE_POINTER_PATH)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("valid-rejection closure preparation is incomplete or drifted")

    manifest = verify_unpublished_evidence(root=root, bundle_path=BUNDLE_PATH)
    sealed_decision = _object(root / BUNDLE_PATH / "decision.json").get("payload")
    authorization = _object(
        root / "state/authorization_uses" / f"{AUTHORIZATION_RECEIPT_ID}.json"
    )
    if (
        manifest.get("manifest_id") != MANIFEST_ID
        or manifest.get("trial_id") != TRIAL_ID
        or manifest.get("authorization_receipt_id") != AUTHORIZATION_RECEIPT_ID
        or sealed_decision != decision
        or authorization.get("trial_id") != TRIAL_ID
        or authorization.get("receipt_id") != AUTHORIZATION_RECEIPT_ID
        or authorization.get("unpublished_evidence_staging") is not True
        or authorization.get("publication") is not False
    ):
        raise IntegrityError("sealed evidence does not prove this valid rejection")
    return preparation


def prepare_valid_rejection_closure(*, root: Path) -> tuple[dict[str, object], dict[str, object]]:
    preparation = load_valid_rejection_preparation(root=root)
    closure = {
        **{key: value for key, value in preparation.items() if key != "record_id"},
        "state": "CLOSED_VALID_REJECTION",
        "closure_id": preparation["record_id"],
        "publication": True,
        "invalid_retirement": False,
        "active_execution_authority": False,
    }
    tombstone_core = {
        "schema_version": "active_tier1_trial_retirement/1.0.0",
        "state": "NO_ACTIVE_TRIAL_VALID_REJECTION",
        "retired_trial_id": TRIAL_ID,
        "valid_rejection_closure_id": preparation["record_id"],
        "sealed_manifest_id": MANIFEST_ID,
        "former_pointer_sha256": preparation["sealed_bindings"][ACTIVE_POINTER_PATH.as_posix()],
        "active_execution_authority": False,
        "historical_execution_authority": False,
        "holdout_or_forward_access": False,
        "trading": False,
    }
    tombstone = {**tombstone_core, "pointer_id": sha256_json(tombstone_core)}
    return closure, tombstone


def _create_or_verify(path: Path, payload: Mapping[str, object]) -> None:
    expected = canonical_bytes(payload) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != expected:
            raise IntegrityError(f"existing valid-rejection artifact differs: {path}")
        return
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
    try:
        os.write(descriptor, expected)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_valid_rejection_closure(*, root: Path) -> dict[str, str]:
    """Publish after separate approval and retire the active pointer last."""
    current_pointer = _object(root / ACTIVE_POINTER_PATH)
    if current_pointer.get("state") == "NO_ACTIVE_TRIAL_VALID_REJECTION":
        verified = verify_published_valid_rejection(root=root)
        closure_id = verified["closure_id"]
        return {
            "closure_id": closure_id,
            "registry_path": (CLOSURE_ROOT / f"{closure_id}.json").as_posix(),
            "event_path": (EVENT_ROOT / f"{closure_id}.json").as_posix(),
            "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
            "active_pointer_state": verified["pointer_state"],
            "manifest_id": MANIFEST_ID,
        }
    closure, tombstone = prepare_valid_rejection_closure(root=root)
    closure_id = str(closure["closure_id"])
    registry = root / CLOSURE_ROOT / f"{closure_id}.json"
    event = root / EVENT_ROOT / f"{closure_id}.json"
    published = closure
    event_payload = {
        "schema_version": "tier1_authoritative_valid_rejection_event/1.0.0",
        "event_type": "CLOSED_VALID_REJECTION",
        "trial_id": TRIAL_ID,
        "closure_id": closure_id,
        "manifest_id": MANIFEST_ID,
    }
    pointer = root / ACTIVE_POINTER_PATH
    expected_pointer = str(closure["sealed_bindings"][ACTIVE_POINTER_PATH.as_posix()])
    if sha256_file(pointer) != expected_pointer:
        raise IntegrityError("active pointer changed before valid-rejection publication")
    _create_or_verify(registry, published)
    _create_or_verify(event, event_payload)
    if sha256_file(pointer) != expected_pointer:
        raise IntegrityError("active pointer changed during valid-rejection publication")
    temporary = pointer.with_suffix(".json.valid-rejection-new")
    temporary.write_bytes(canonical_bytes(tombstone) + b"\n")
    os.replace(temporary, pointer)
    return {
        "closure_id": closure_id,
        "registry_path": registry.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
        "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
        "active_pointer_state": str(tombstone["state"]),
        "manifest_id": MANIFEST_ID,
    }


def verify_published_valid_rejection(*, root: Path) -> dict[str, str]:
    preparation = load_valid_rejection_preparation_after_publication(root=root)
    closure_id = str(preparation["record_id"])
    registry = _object(root / CLOSURE_ROOT / f"{closure_id}.json")
    event = _object(root / EVENT_ROOT / f"{closure_id}.json")
    pointer = _object(root / ACTIVE_POINTER_PATH)
    pointer_core = dict(pointer)
    pointer_id = pointer_core.pop("pointer_id", None)
    if (
        registry.get("state") != "CLOSED_VALID_REJECTION"
        or registry.get("invalid_retirement") is not False
        or registry.get("manifest_id") != MANIFEST_ID
        or event.get("event_type") != "CLOSED_VALID_REJECTION"
        or event.get("closure_id") != closure_id
        or pointer.get("state") != "NO_ACTIVE_TRIAL_VALID_REJECTION"
        or pointer.get("valid_rejection_closure_id") != closure_id
        or pointer.get("former_pointer_sha256")
        != preparation["sealed_bindings"][ACTIVE_POINTER_PATH.as_posix()]
        or pointer.get("active_execution_authority") is not False
        or pointer.get("historical_execution_authority") is not False
        or pointer_id != sha256_json(pointer_core)
    ):
        raise IntegrityError("published valid-rejection closure is inconsistent")
    verify_unpublished_evidence(root=root, bundle_path=BUNDLE_PATH)
    return {"closure_id": closure_id, "pointer_state": str(pointer["state"])}


def load_valid_rejection_preparation_after_publication(*, root: Path) -> dict[str, object]:
    """Validate immutable bindings while allowing only the expected pointer transition."""
    preparation = _object(root / PREPARATION_PATH)
    bindings = dict(preparation.get("sealed_bindings", {}))
    pointer_hash = bindings.pop(ACTIVE_POINTER_PATH.as_posix(), None)
    core = dict(preparation)
    record_id = core.pop("record_id", None)
    if (
        record_id != sha256_json(core)
        or preparation.get("schema_version")
        != "tier1_authoritative_valid_rejection_closure_preparation/1.0.0"
        or preparation.get("trial_id") != TRIAL_ID
        or preparation.get("manifest_id") != MANIFEST_ID
        or pointer_hash is None
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("published valid-rejection bindings drifted")
    verify_unpublished_evidence(root=root, bundle_path=BUNDLE_PATH)
    return preparation
