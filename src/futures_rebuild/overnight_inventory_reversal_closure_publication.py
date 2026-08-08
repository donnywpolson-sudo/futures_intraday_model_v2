"""Create-only publication of the audited inconclusive terminal clarification."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError


TRIAL_ID = "24772e41730b16bfdf3187d0c9e79b2491e6118962cfdffbc16a86d4e241169c"
UNPUBLISHED_ROOT = Path("state/unpublished_evidence/overnight_inventory_reversal") / TRIAL_ID
CLARIFICATION_SOURCE = UNPUBLISHED_ROOT / "terminal_closure_clarification.json"
EVENT_SOURCE = UNPUBLISHED_ROOT / "terminal_closure_clarification_event.json"
READINESS_SOURCE = (
    Path("state/unpublished_evidence/overnight_inventory_reversal_fold_readiness_v2")
    / TRIAL_ID / "fold_readiness_certificate.json"
)
ACTIVE_POINTER = Path("configs/active_tier1_trial.json")
CLOSURE_ROOT = Path("state/trial_registry/overnight_inventory_reversal_terminal_closure")
READINESS_ROOT = Path("state/trial_registry/overnight_inventory_reversal_fold_readiness")
EVENT_ROOT = Path("state/trial_events/overnight_inventory_reversal")


def _canonical_object(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid closure publication artifact: {path}") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"noncanonical closure publication artifact: {path}")
    return value


def load_closure_publication(*, root: Path) -> dict[str, object]:
    clarification = _canonical_object(root / CLARIFICATION_SOURCE)
    event = _canonical_object(root / EVENT_SOURCE)
    readiness = _canonical_object(root / READINESS_SOURCE)
    pointer = _canonical_object(root / ACTIVE_POINTER)
    clarification_core = dict(clarification)
    clarification_id = clarification_core.pop("clarification_id", None)
    event_core = dict(event)
    event_id = event_core.pop("event_id", None)
    readiness_core = dict(readiness)
    report_id = readiness_core.pop("report_id", None)
    certificate = readiness.get("fold_readiness_certificate")
    original = clarification.get("existing_terminal_closure")
    evidence = clarification.get("row_certified_readiness_evidence")
    classification = clarification.get("audit_classification")
    if (
        clarification_id != sha256_json(clarification_core)
        or clarification.get("state") != "CLOSED_INCONCLUSIVE_CLARIFIED"
        or clarification.get("trial_id") != TRIAL_ID
        or clarification.get("terminal_disposition") != "INCONCLUSIVE_DATA_OR_COVERAGE"
        or not isinstance(original, Mapping)
        or original.get("preserved_byte_for_byte") is not True
        or sha256_file(root / str(original.get("path"))) != original.get("sha256")
        or not isinstance(evidence, Mapping)
        or evidence.get("sha256") != sha256_file(root / READINESS_SOURCE)
        or evidence.get("report_id") != report_id
        or sha256_file(root / str(evidence.get("authorization_use_path")))
        != evidence.get("authorization_use_sha256")
        or report_id != sha256_json(readiness_core)
        or readiness.get("economics_evaluation") is not False
        or readiness.get("holdout_2025_touched") is not False
        or readiness.get("provider_or_network_access") is not False
        or not isinstance(certificate, Mapping)
        or certificate.get("certificate_id") != evidence.get("certificate_id")
        or certificate.get("overall_decision") != "FAIL"
        or len(certificate.get("fold_market_results", ())) != 32
        or any(
            not isinstance(item, Mapping) or item.get("status") != "FAIL"
            for item in certificate.get("fold_market_results", ())
        )
        or not isinstance(classification, Mapping)
        or classification.get("source_protocol_compatibility_failure_proven") is not True
        or classification.get("preexecution_certification_omission_proven") is not True
        or classification.get("historical_execution_implementation_error_proven") is not False
        or classification.get("strategy_failure_proven") is not False
        or classification.get("economic_evaluation_occurred") is not False
        or event_id != sha256_json(event_core)
        or event.get("trial_id") != TRIAL_ID
        or event.get("clarification_id") != clarification_id
        or event.get("clarification_sha256") != sha256_file(root / CLARIFICATION_SOURCE)
        or event.get("readiness_report_id") != report_id
        or event.get("economic_evaluation") is not False
        or pointer.get("state") != "NO_ACTIVE_TRIAL_VALID_REJECTION"
        or pointer.get("active_execution_authority") is not False
        or pointer.get("historical_execution_authority") is not False
        or pointer.get("holdout_or_forward_access") is not False
        or pointer.get("trading") is not False
    ):
        raise IntegrityError("overnight closure publication preparation drifted")
    return {
        "clarification": clarification,
        "event": event,
        "readiness": readiness,
        "clarification_id": clarification_id,
        "event_id": event_id,
        "report_id": report_id,
        "pointer_sha256": sha256_file(root / ACTIVE_POINTER),
    }


def _create_or_verify(path: Path, payload: Mapping[str, object]) -> None:
    expected = canonical_bytes(dict(payload)) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != expected:
            raise IntegrityError(f"published closure artifact differs: {path}")
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


def publish_closure_clarification(*, root: Path) -> dict[str, str]:
    prepared = load_closure_publication(root=root)
    clarification_path = root / CLOSURE_ROOT / f"{prepared['clarification_id']}.json"
    readiness_path = root / READINESS_ROOT / f"{prepared['report_id']}.json"
    event_path = root / EVENT_ROOT / f"{prepared['event_id']}.json"
    _create_or_verify(readiness_path, prepared["readiness"])  # type: ignore[arg-type]
    _create_or_verify(clarification_path, prepared["clarification"])  # type: ignore[arg-type]
    _create_or_verify(event_path, prepared["event"])  # type: ignore[arg-type]
    if sha256_file(root / ACTIVE_POINTER) != prepared["pointer_sha256"]:
        raise IntegrityError("active pointer changed during closure publication")
    return verify_published_closure_clarification(root=root)


def verify_published_closure_clarification(*, root: Path) -> dict[str, str]:
    prepared = load_closure_publication(root=root)
    targets = {
        "closure_path": CLOSURE_ROOT / f"{prepared['clarification_id']}.json",
        "readiness_path": READINESS_ROOT / f"{prepared['report_id']}.json",
        "event_path": EVENT_ROOT / f"{prepared['event_id']}.json",
    }
    sources = {
        "closure_path": CLARIFICATION_SOURCE,
        "readiness_path": READINESS_SOURCE,
        "event_path": EVENT_SOURCE,
    }
    for name, relative in targets.items():
        if (root / relative).read_bytes() != (root / sources[name]).read_bytes():
            raise IntegrityError("published closure bytes differ from preparation")
    return {
        "clarification_id": str(prepared["clarification_id"]),
        "report_id": str(prepared["report_id"]),
        "event_id": str(prepared["event_id"]),
        **{name: value.as_posix() for name, value in targets.items()},
        "active_pointer_state": "NO_ACTIVE_TRIAL_VALID_REJECTION",
    }
