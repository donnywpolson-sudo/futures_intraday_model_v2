"""Prepare one frozen trial, one active pointer, and one validity certificate.

Publication is intentionally absent from this module until a passing source
adequacy record exists and the user separately approves the create-only state
mutation.  Preparation is deterministic and has no data-reading authority.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError
from .tier1_frozen_trial_protocol import (
    PROTOCOL_PATH,
    SYNTHETIC_VERIFICATION_PATH,
    load_frozen_synthetic_verification,
    load_frozen_trial_protocol,
)


MECHANICS_CERTIFICATE_PATH = Path(
    "configs/tier1_bracket_preexecution_validity_certificate.json"
)
RECONCILIATION_PATH = Path("configs/tier1_bracket_version_freeze_reconciliation.json")
TRIAL_REGISTRY_ROOT = Path("state/trial_registry/tier1_frozen_trial")
TRIAL_EVENT_ROOT = Path("state/trial_events/tier1_frozen_trial")
CERTIFICATE_ROOT = Path("state/preexecution_certificates/tier1_frozen_trial")
ACTIVE_POINTER_PATH = Path("configs/active_tier1_trial.json")


@dataclass(frozen=True)
class PreparedFrozenLifecycle:
    trial_id: str
    trial: Mapping[str, object]
    pointer_id: str
    pointer: Mapping[str, object]
    certificate_id: str
    certificate: Mapping[str, object]


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid frozen lifecycle artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("frozen lifecycle artifact is not an object")
    return value


def _passing_mechanics_gates(mechanics: Mapping[str, object]) -> tuple[str, ...]:
    gates = mechanics.get("gates")
    if not isinstance(gates, list):
        raise IntegrityError("mechanics certificate lacks gates")
    passing = tuple(
        str(item.get("gate")) for item in gates
        if isinstance(item, dict) and item.get("status") == "PASS"
    )
    required = {
        "VERSION_LINEAGE_AND_FREEZE",
        "ALL_V4_THROUGH_V12_DEFECTS_HAVE_ADVERSARIAL_TESTS",
        "COMPLETE_SYNTHETIC_SOURCE_TO_TERMINAL_PIPELINE",
        "INDEPENDENT_BASELINE_UNIVERSES_SCHEDULES_COSTS_AND_ACCOUNT_PATHS",
        "PREDICTION_ELIGIBILITY_IS_OUTCOME_INDEPENDENT",
        "COST_RISK_STATISTICS_AND_PROTOCOL_ALIGNMENT",
        "EVIDENCE_RUNTIME_AND_SINGLE_USE_AUTHORIZATION",
        "HOLDOUT_2025_FAILS_CLOSED_BEFORE_OPEN",
    }
    if not required <= set(passing):
        raise IntegrityError("mechanics certificate no longer proves every required control")
    return tuple(sorted(required))


def build_frozen_lifecycle_payloads(
    *, protocol: Mapping[str, object], synthetic: Mapping[str, object],
    mechanics: Mapping[str, object], reconciliation: Mapping[str, object],
    source_adequacy: Mapping[str, object], bindings: Mapping[str, str],
) -> PreparedFrozenLifecycle:
    """Build mutually consistent identities; fail before any publication."""

    source = protocol.get("source")
    adjudication = source_adequacy.get("adjudication")
    checks = adjudication.get("checks") if isinstance(adjudication, dict) else None
    if (
        protocol.get("state") != "PREPARED_NOT_REGISTERED_SOURCE_ADEQUACY_PENDING"
        or not isinstance(source, dict)
        or synthetic.get("applicable_results", {}).get("failed") != 0  # type: ignore[union-attr]
        or mechanics.get("holdout_2025_touched") is not False
        or reconciliation.get("authoritative_current_trial") is not None
        or source_adequacy.get("state") != "PUBLISHED_SOURCE_QUALITY_ONLY"
        or not isinstance(adjudication, dict)
        or adjudication.get("decision") != "PASS"
        or not isinstance(checks, dict) or not checks
        or not all(value is True for value in checks.values())
        or source_adequacy.get("selected_sources_id") != source.get("selected_sources_id")
        or source_adequacy.get("calendar_release_id") != source.get("calendar_release_id")
        or source_adequacy.get("holdout_or_forward_access") is not False
        or source_adequacy.get("historical_evaluation") is not False
        or not bindings
    ):
        raise IntegrityError("frozen lifecycle cannot be prepared before every gate passes")
    mechanics_gates = _passing_mechanics_gates(mechanics)
    source_record_id = source_adequacy.get("record_id")
    protocol_id = protocol.get("protocol_id")
    verification_id = synthetic.get("verification_id")
    if not all(
        isinstance(value, str) and len(value) == 64
        for value in (source_record_id, protocol_id, verification_id)
    ):
        raise IntegrityError("frozen lifecycle evidence identity is invalid")
    trial_core = {
        "schema_version": "tier1_frozen_trial_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": protocol["classification"],
        "protocol_id": protocol_id,
        "source_adequacy_record_id": source_record_id,
        "selected_sources_id": source["selected_sources_id"],
        "calendar_release_id": source["calendar_release_id"],
        "bindings": dict(sorted(bindings.items())),
        "source_row_access": False, "model_fit": False,
        "prediction_generation": False, "historical_evaluation": False,
        "holdout_or_forward_access": False, "provider_access": False,
        "publication": False, "trading": False,
    }
    trial_id = sha256_json(trial_core)
    certificate_path = (CERTIFICATE_ROOT / f"{trial_id}.json").as_posix()
    pointer_core = {
        "schema_version": "active_tier1_trial/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "trial_id": trial_id,
        "trial_registry_path": (TRIAL_REGISTRY_ROOT / f"{trial_id}.json").as_posix(),
        "preexecution_certificate_path": certificate_path,
        "protocol_id": protocol_id,
        "source_adequacy_record_id": source_record_id,
        "holdout_or_forward_access": False,
    }
    pointer_id = sha256_json(pointer_core)
    certificate_core = {
        "schema_version": "tier1_frozen_preexecution_certificate/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "overall_decision": "PASS",
        "trial_id": trial_id, "active_pointer_id": pointer_id,
        "protocol_id": protocol_id,
        "synthetic_verification_id": verification_id,
        "source_adequacy_record_id": source_record_id,
        "mechanics_gates": list(mechanics_gates),
        "source_gates": dict(sorted(checks.items())),
        "bindings": dict(sorted(bindings.items())),
        "one_immutable_source_set_bound": True,
        "one_authoritative_active_trial_pointer_bound": True,
        "cost_risk_statistics_evidence_runtime_and_authorization_aligned": True,
        "holdout_2025_touched": False, "provider_access": False,
        "model_fit": False, "prediction_generation": False,
        "historical_evaluation": False, "trading": False,
    }
    certificate_id = sha256_json(certificate_core)
    return PreparedFrozenLifecycle(
        trial_id, trial_core, pointer_id, pointer_core,
        certificate_id, certificate_core,
    )


def prepare_frozen_lifecycle(
    *, root: Path, source_adequacy_path: Path,
) -> PreparedFrozenLifecycle:
    """Load exact local evidence without opening any historical source rows."""

    protocol = load_frozen_trial_protocol(root=root)
    synthetic = load_frozen_synthetic_verification(root=root)
    mechanics = _object(root / MECHANICS_CERTIFICATE_PATH)
    reconciliation = _object(root / RECONCILIATION_PATH)
    source_adequacy = _object(root / source_adequacy_path)
    bindings = {
        path.as_posix(): sha256_file(root / path)
        for path in (
            PROTOCOL_PATH, SYNTHETIC_VERIFICATION_PATH,
            MECHANICS_CERTIFICATE_PATH, RECONCILIATION_PATH,
            source_adequacy_path,
            Path("src/futures_rebuild/tier1_frozen_successor_source_semantics.py"),
            Path("src/futures_rebuild/tier1_frozen_trial_pipeline.py"),
            Path("src/futures_rebuild/tier1_frozen_trial_protocol.py"),
            Path("src/futures_rebuild/tier1_frozen_trial_lifecycle.py"),
        )
    }
    return build_frozen_lifecycle_payloads(
        protocol=protocol, synthetic=synthetic, mechanics=mechanics,
        reconciliation=reconciliation, source_adequacy=source_adequacy,
        bindings=bindings,
    )
