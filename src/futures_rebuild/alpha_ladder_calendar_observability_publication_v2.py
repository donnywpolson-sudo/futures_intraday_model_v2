"""Transition-stable publication after the V1 activation rolled back safely."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path

from .alpha_ladder_calendar_observability_publication import (
    ACTIVE_POINTER_PATH,
    ACTIVE_POINTER_SHA256,
    CALENDAR_ID,
    CALENDAR_SHA256,
    EVENT_DIR,
    MECHANISM_ID,
    MECHANISM_PATH,
    MECHANISM_SHA256,
    PREDECESSOR_CALENDAR_ID,
    PREDECESSOR_CALENDAR_PATH,
    PREDECESSOR_CALENDAR_SHA256,
    PREDECESSOR_POINTER_SNAPSHOT_PATH,
    PREPARED_PATH,
    PUBLISHED_CALENDAR_PATH,
    REGISTRY_DIR,
    _activate_pointer_with_rollback,
    _read_canonical,
    _write_new_or_exact,
)
from .alpha_ladder_calendar_observability_successor import validate_successor
from .canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from .errors import IntegrityError


V1_REGISTRATION_ID = (
    "b55f8ae4c30929a10f797420dfc5deaf4503e0d6f8d77fb2111f6998b6c22b2c"
)
V1_REGISTRATION_PATH = REGISTRY_DIR / "registration.json"
V1_REGISTRATION_SHA256 = (
    "57ce77a8ea77441c8b3d9ee36263ac84ee58a8a485f32cd5f4c5ae5f3164c446"
)
V1_EVENT_ID = "cc8c62b968b4a6fafef96b0c747896449dc6b735ca6fdb593e1fb51ecde6bbf5"
V1_EVENT_PATH = EVENT_DIR / f"{V1_EVENT_ID}.json"
V1_EVENT_SHA256 = (
    "72f8a7cc2d26bca69f0942265270501b7a28093c72fed5b419e88e31e62834ec"
)
V1_POINTER_ID = "8de52e76da46631e2f9a8ad908e015c9b2aeeceb94ec9b7322d5c659bdb98b1c"
V1_MODULE_PATH = Path(
    "src/futures_rebuild/alpha_ladder_calendar_observability_publication.py"
)
V1_MODULE_SHA256 = (
    "83e7efff6428d941372205d7634c0fd585adce1059313ec46cc7a7e3bd0d14d0"
)
V1_SCRIPT_PATH = Path(
    "scripts/publish_alpha_ladder_calendar_observability_successor.py"
)
V1_SCRIPT_SHA256 = (
    "b541672251f3d22832a128f03a02d6054a8208dac8fefe410f3f82b817c1e53c"
)

FAILURE_PATH = REGISTRY_DIR / "activation_failure_v1.json"
REGISTRATION_PATH = REGISTRY_DIR / "registration_v2.json"
MODULE_PATH = Path(
    "src/futures_rebuild/alpha_ladder_calendar_observability_publication_v2.py"
)
PUBLISH_SCRIPT_PATH = Path(
    "scripts/publish_alpha_ladder_calendar_observability_successor_v2.py"
)


def _validate_v1_artifacts(*, root: Path) -> None:
    expected = (
        (V1_REGISTRATION_PATH, V1_REGISTRATION_SHA256, "V1 registration"),
        (V1_EVENT_PATH, V1_EVENT_SHA256, "V1 event"),
        (V1_MODULE_PATH, V1_MODULE_SHA256, "V1 publication module"),
        (V1_SCRIPT_PATH, V1_SCRIPT_SHA256, "V1 publication script"),
        (
            PREDECESSOR_POINTER_SNAPSHOT_PATH,
            ACTIVE_POINTER_SHA256,
            "predecessor pointer snapshot",
        ),
        (PUBLISHED_CALENDAR_PATH, CALENDAR_SHA256, "published calendar"),
    )
    for relative, digest, name in expected:
        if sha256_file(root / relative) != digest:
            raise IntegrityError(f"{name} hash drifted")
    registration = _read_canonical(
        root / V1_REGISTRATION_PATH, name="V1 calendar registration"
    )
    event = _read_canonical(root / V1_EVENT_PATH, name="V1 calendar event")
    if (
        registration.get("registration_id") != V1_REGISTRATION_ID
        or event.get("event_id") != V1_EVENT_ID
        or event.get("registration_id") != V1_REGISTRATION_ID
        or registration.get("mechanism_registered") is not False
        or event.get("mechanism_registration_performed") is not False
    ):
        raise IntegrityError("V1 failed lifecycle identity drifted")


def _failure_record() -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": (
            "cash_open_impulse_calendar_observability_activation_failure/1.0.0"
        ),
        "classification": "FAILED_POST_ACTIVATION_VALIDATION_POINTER_ROLLED_BACK",
        "calendar_id": CALENDAR_ID,
        "calendar_sha256": CALENDAR_SHA256,
        "failed_registration_id": V1_REGISTRATION_ID,
        "failed_registration_path": V1_REGISTRATION_PATH.as_posix(),
        "failed_registration_sha256": V1_REGISTRATION_SHA256,
        "failed_event_id": V1_EVENT_ID,
        "failed_event_path": V1_EVENT_PATH.as_posix(),
        "failed_event_sha256": V1_EVENT_SHA256,
        "failed_pointer_id": V1_POINTER_ID,
        "cause": (
            "MUTABLE_PREDECESSOR_POINTER_BINDING_EVALUATED_AFTER_POINTER_REPLACEMENT"
        ),
        "remediation": (
            "VERIFY_THE_PREDECESSOR_POINTER_BINDING_AGAINST_ITS_IMMUTABLE_"
            "SNAPSHOT_AFTER_ACTIVATION"
        ),
        "predecessor_pointer_restored": True,
        "predecessor_pointer_sha256": ACTIVE_POINTER_SHA256,
        "successor_bytes_changed": False,
        "prior_publication_artifacts_preserved": True,
        "mechanism_id": MECHANISM_ID,
        "mechanism_registered": False,
        "economic_result": "NOT_PRODUCED",
        "authority": {
            "external_cost_usd": "0",
            "historical_rows_read": False,
            "performance_evaluation": False,
            "provider_network_credentials_accessed": False,
            "year_2025_accessed": False,
        },
    }
    return {**core, "failure_id": sha256_json(core)}


def _validate_successor_transition_stable(
    successor: Mapping[str, object], *, root: Path
) -> None:
    """Relocate only the mutable predecessor-pointer binding to its snapshot."""

    validate_successor(successor, root=root, verify_bindings=False)
    bindings = successor.get("bindings")
    if not isinstance(bindings, Mapping):
        raise IntegrityError("calendar-observability successor bindings are absent")
    if bindings.get(ACTIVE_POINTER_PATH.as_posix()) != ACTIVE_POINTER_SHA256:
        raise IntegrityError("successor predecessor pointer binding drifted")
    if (
        sha256_file(root / PREDECESSOR_POINTER_SNAPSHOT_PATH)
        != ACTIVE_POINTER_SHA256
    ):
        raise IntegrityError("immutable predecessor pointer snapshot drifted")
    for relative, digest in bindings.items():
        if str(relative) == ACTIVE_POINTER_PATH.as_posix():
            continue
        if sha256_file(root / str(relative)) != digest:
            raise IntegrityError(
                f"calendar-observability successor binding drifted: {relative}"
            )


def publication_documents(*, root: Path) -> dict[str, dict[str, object]]:
    _validate_v1_artifacts(root=root)
    successor = _read_canonical(
        root / PUBLISHED_CALENDAR_PATH,
        name="published calendar-observability successor",
    )
    _validate_successor_transition_stable(successor, root=root)
    failure = _failure_record()
    failure_sha = sha256_bytes(canonical_bytes(failure) + b"\n")
    immutable_bindings = dict(
        sorted(
            {
                PREPARED_PATH.as_posix(): CALENDAR_SHA256,
                PUBLISHED_CALENDAR_PATH.as_posix(): CALENDAR_SHA256,
                PREDECESSOR_CALENDAR_PATH.as_posix(): PREDECESSOR_CALENDAR_SHA256,
                PREDECESSOR_POINTER_SNAPSHOT_PATH.as_posix(): ACTIVE_POINTER_SHA256,
                V1_REGISTRATION_PATH.as_posix(): V1_REGISTRATION_SHA256,
                V1_EVENT_PATH.as_posix(): V1_EVENT_SHA256,
                V1_MODULE_PATH.as_posix(): V1_MODULE_SHA256,
                V1_SCRIPT_PATH.as_posix(): V1_SCRIPT_SHA256,
                FAILURE_PATH.as_posix(): failure_sha,
                MECHANISM_PATH.as_posix(): MECHANISM_SHA256,
                MODULE_PATH.as_posix(): sha256_file(root / MODULE_PATH),
                PUBLISH_SCRIPT_PATH.as_posix(): sha256_file(root / PUBLISH_SCRIPT_PATH),
            }.items()
        )
    )
    registration_core: dict[str, object] = {
        "schema_version": (
            "cash_open_impulse_calendar_observability_registration/2.0.0"
        ),
        "calendar_id": CALENDAR_ID,
        "calendar_path": PUBLISHED_CALENDAR_PATH.as_posix(),
        "calendar_sha256": CALENDAR_SHA256,
        "predecessor_calendar_id": PREDECESSOR_CALENDAR_ID,
        "predecessor_pointer_snapshot_path": (
            PREDECESSOR_POINTER_SNAPSHOT_PATH.as_posix()
        ),
        "predecessor_pointer_sha256": ACTIVE_POINTER_SHA256,
        "failed_activation_record_id": failure["failure_id"],
        "failed_activation_record_path": FAILURE_PATH.as_posix(),
        "failed_activation_record_sha256": failure_sha,
        "supersedes_failed_registration_id": V1_REGISTRATION_ID,
        "calendar_correction_count": 2,
        "source_observability_record_count": 6,
        "publication_mode": (
            "CREATE_ONLY_TRANSITION_STABLE_POINTER_LAST_AUTOMATIC_ROLLBACK"
        ),
        "transition_validation": (
            "PREDECESSOR_POINTER_BINDING_VERIFIED_AGAINST_IMMUTABLE_SNAPSHOT"
        ),
        "mechanism_id": MECHANISM_ID,
        "mechanism_registered": False,
        "bindings": immutable_bindings,
        "authority": {
            "external_cost_usd": "0",
            "historical_rows_read": False,
            "mechanism_registration": False,
            "performance_evaluation": False,
            "provider_network_credentials_accessed": False,
            "year_2025_accessed": False,
        },
    }
    registration = {
        **registration_core,
        "registration_id": sha256_json(registration_core),
    }
    registration_sha = sha256_bytes(canonical_bytes(registration) + b"\n")
    event_core: dict[str, object] = {
        "schema_version": "cash_open_impulse_calendar_observability_event/2.0.0",
        "event_type": (
            "PUBLISHED_AND_ACTIVATED_TRANSITION_STABLE_CALENDAR_"
            "OBSERVABILITY_SUCCESSOR"
        ),
        "calendar_id": CALENDAR_ID,
        "predecessor_calendar_id": PREDECESSOR_CALENDAR_ID,
        "registration_id": registration["registration_id"],
        "failed_activation_record_id": failure["failure_id"],
        "calendar_correction_count": 2,
        "source_observability_record_count": 6,
        "mechanism_id": MECHANISM_ID,
        "mechanism_registration_performed": False,
    }
    event = {**event_core, "event_id": sha256_json(event_core)}
    event_path = EVENT_DIR / f"{event['event_id']}.json"
    event_sha = sha256_bytes(canonical_bytes(event) + b"\n")
    pointer_core: dict[str, object] = {
        "schema_version": "active_cash_open_impulse_historical_calendar/3.1.0",
        "state": "ACTIVE_CALENDAR_AND_SOURCE_OBSERVABILITY",
        "calendar_id": CALENDAR_ID,
        "calendar_path": PUBLISHED_CALENDAR_PATH.as_posix(),
        "calendar_sha256": CALENDAR_SHA256,
        "calendar_correction_count": 2,
        "source_observability_record_count": 6,
        "registration_id": registration["registration_id"],
        "registration_path": REGISTRATION_PATH.as_posix(),
        "registration_sha256": registration_sha,
        "event_id": event["event_id"],
        "event_path": event_path.as_posix(),
        "event_sha256": event_sha,
        "failed_activation_record_id": failure["failure_id"],
        "failed_activation_record_path": FAILURE_PATH.as_posix(),
        "failed_activation_record_sha256": failure_sha,
        "predecessor": {
            "calendar_id": PREDECESSOR_CALENDAR_ID,
            "pointer_snapshot_path": PREDECESSOR_POINTER_SNAPSHOT_PATH.as_posix(),
            "pointer_sha256": ACTIVE_POINTER_SHA256,
        },
        "mechanism_id": MECHANISM_ID,
        "mechanism_registered": False,
    }
    pointer = {**pointer_core, "pointer_id": sha256_json(pointer_core)}
    return {
        "failure": failure,
        "registration": registration,
        "event": event,
        "pointer": pointer,
    }


def load_active_successor(*, root: Path) -> dict[str, object]:
    documents = publication_documents(root=root)
    pointer = _read_canonical(
        root / ACTIVE_POINTER_PATH,
        name="active transition-stable calendar-observability pointer",
    )
    pointer_core = {
        key: value for key, value in pointer.items() if key != "pointer_id"
    }
    if (
        pointer != documents["pointer"]
        or pointer.get("pointer_id") != sha256_json(pointer_core)
        or pointer.get("mechanism_registered") is not False
    ):
        raise IntegrityError("active transition-stable pointer is invalid")
    expected = (
        (
            root / PUBLISHED_CALENDAR_PATH,
            CALENDAR_SHA256,
            "published calendar",
        ),
        (
            root / PREDECESSOR_POINTER_SNAPSHOT_PATH,
            ACTIVE_POINTER_SHA256,
            "predecessor snapshot",
        ),
        (
            root / FAILURE_PATH,
            str(pointer["failed_activation_record_sha256"]),
            "failed activation record",
        ),
        (
            root / REGISTRATION_PATH,
            str(pointer["registration_sha256"]),
            "corrected registration",
        ),
        (
            root / str(pointer["event_path"]),
            str(pointer["event_sha256"]),
            "corrected activation event",
        ),
    )
    for path, digest, name in expected:
        if sha256_file(path) != digest:
            raise IntegrityError(f"{name} hash drifted")
    for name, path in (
        ("failure", FAILURE_PATH),
        ("registration", REGISTRATION_PATH),
        ("event", Path(str(pointer["event_path"]))),
    ):
        if _read_canonical(root / path, name=name) != documents[name]:
            raise IntegrityError(f"{name} document differs from its deterministic form")
    registration = documents["registration"]
    bindings = registration.get("bindings")
    if not isinstance(bindings, Mapping):
        raise IntegrityError("corrected registration bindings are absent")
    for relative, digest in bindings.items():
        if sha256_file(root / str(relative)) != digest:
            raise IntegrityError(f"corrected lifecycle binding drifted: {relative}")
    successor = _read_canonical(
        root / PUBLISHED_CALENDAR_PATH,
        name="published calendar-observability successor",
    )
    _validate_successor_transition_stable(successor, root=root)
    if (
        (root / PUBLISHED_CALENDAR_PATH).read_bytes()
        != (root / PREPARED_PATH).read_bytes()
        or sha256_file(root / MECHANISM_PATH) != MECHANISM_SHA256
    ):
        raise IntegrityError("successor or mechanism preservation drifted")
    return successor


def persist_publication(*, root: Path) -> dict[str, str]:
    documents = publication_documents(root=root)
    event_path = EVENT_DIR / f"{documents['event']['event_id']}.json"
    pointer_bytes = canonical_bytes(documents["pointer"]) + b"\n"
    current_bytes = (root / ACTIVE_POINTER_PATH).read_bytes()
    if current_bytes == pointer_bytes:
        load_active_successor(root=root)
    else:
        if sha256_bytes(current_bytes) != ACTIVE_POINTER_SHA256:
            raise IntegrityError("active calendar pointer is neither predecessor nor V2")
        payloads = (
            (root / FAILURE_PATH, canonical_bytes(documents["failure"]) + b"\n"),
            (
                root / REGISTRATION_PATH,
                canonical_bytes(documents["registration"]) + b"\n",
            ),
            (root / event_path, canonical_bytes(documents["event"]) + b"\n"),
        )
        for path, raw in payloads:
            _write_new_or_exact(path, raw)
        _activate_pointer_with_rollback(
            pointer_path=root / ACTIVE_POINTER_PATH,
            new_bytes=pointer_bytes,
            postcheck=lambda: load_active_successor(root=root),
        )
    return {
        "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
        "calendar_id": CALENDAR_ID,
        "calendar_sha256": CALENDAR_SHA256,
        "event_id": str(documents["event"]["event_id"]),
        "failure_id": str(documents["failure"]["failure_id"]),
        "pointer_id": str(documents["pointer"]["pointer_id"]),
        "registration_id": str(documents["registration"]["registration_id"]),
        "mechanism_registered": "false",
    }
