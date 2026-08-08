"""Metadata-only preflight for the first Tier 1 Phase 8 evaluation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .canonical import sha256_json
from .errors import IntegrityError
from .tier1_phase8_preparation import (
    AUDIT_RELEASE_ID,
    INDEX_RELEASE_ID,
    prepare_phase8_confirmation,
    prepare_tier1_phase8,
)
from .tier1_phase8_evaluation_config import load_tier1_phase8_evaluation_config
from .tier1_bracket_trial import (
    load_registered_tier1_bracket_trial,
    load_tier1_bracket_trial_contract,
)
from .tier1_phase8_risk_audit import run_default_tier1_risk_realism_audit


def _read_object(path: Path) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"Phase 8 readiness cannot read {path.name}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("Phase 8 readiness metadata must be an object")
    return value


def _validate_index_metadata(
    index: Mapping[str, object], *, rulebook_hash: str, audit_release_id: str
) -> None:
    metadata = index.get("metadata")
    source_release_ids = index.get("source_release_ids")
    if (
        index.get("release_kind") != "phase8_actual_contract_economics_index"
        or not isinstance(metadata, dict)
        or metadata.get("interval_count") != 677
        or metadata.get("market_year_count") != 644
        or metadata.get("rulebook_hash") != rulebook_hash
        or not isinstance(source_release_ids, list)
        or audit_release_id not in source_release_ids
    ):
        raise IntegrityError("Phase 8 index metadata is not the current audited selection")


@dataclass(frozen=True)
class Tier1Phase8Readiness:
    status: str
    blocker: str | None
    index_release_id: str
    audit_release_id: str
    rulebook_hash: str
    evaluation_config_hash: str
    result_label: str
    checked_paths: tuple[str, ...]

    def report(self) -> dict[str, object]:
        core = {
            "schema_version": "tier1_phase8_readiness/1.0.0",
            "status": self.status,
            "blocker": self.blocker,
            "index_release_id": self.index_release_id,
            "audit_release_id": self.audit_release_id,
            "rulebook_hash": self.rulebook_hash,
            "evaluation_config_hash": self.evaluation_config_hash,
            "result_label": self.result_label,
            "checked_paths": list(self.checked_paths),
            "market_data_read": False,
            "release_publication": False,
            "provider_access": False,
            "trading": False,
        }
        return {**core, "readiness_id": sha256_json(core)}


@dataclass(frozen=True)
class Tier1Phase8BracketReadiness:
    """Metadata-only status check for the research-only bracket successor."""

    status: str
    index_release_id: str
    audit_release_id: str
    rulebook_hash: str
    risk_audit_policy_controls_pass: bool
    old_five_minute_predictions_blocked: bool
    registered_trial_id: str | None
    registered_trial_state: str | None
    checked_paths: tuple[str, ...]

    def report(self) -> dict[str, object]:
        core = {
            "schema_version": "tier1_phase8_bracket_readiness/1.0.0",
            "status": self.status,
            "index_release_id": self.index_release_id,
            "audit_release_id": self.audit_release_id,
            "rulebook_hash": self.rulebook_hash,
            "risk_audit_policy_controls_pass": self.risk_audit_policy_controls_pass,
            "old_five_minute_predictions_blocked": self.old_five_minute_predictions_blocked,
            "registered_trial_id": self.registered_trial_id,
            "registered_trial_state": self.registered_trial_state,
            "checked_paths": list(self.checked_paths),
            "market_data_read": False,
            "release_publication": False,
            "provider_access": False,
            "trading": False,
            "live_realism_claim_supported": False,
        }
        return {**core, "readiness_id": sha256_json(core)}


def audit_tier1_phase8_readiness(*, root: Path) -> Tier1Phase8Readiness:
    """Verify only release manifests, trial registration, and local controls."""

    preparation = prepare_tier1_phase8(root=root)
    if not preparation.evaluation_ready:
        raise IntegrityError("Phase 8 settings are incomplete")
    confirmation = prepare_phase8_confirmation(preparation)
    scope = confirmation.get("scope")
    if not isinstance(scope, dict) or scope.get("result_label") != "PROVISIONAL_EXECUTION_COSTS":
        raise IntegrityError("Phase 8 readiness requires the provisional-cost boundary")

    rulebook_document = _read_object(root / "configs" / "contract_economics_rules.json")
    rulebook_hash = sha256_json(rulebook_document)
    index_path = root / "manifests" / "data_releases" / "reference" / f"{INDEX_RELEASE_ID}.json"
    audit_path = root / "manifests" / "data_releases" / "reference" / f"{AUDIT_RELEASE_ID}.json"
    index = _read_object(index_path)
    audit = _read_object(audit_path)
    if audit.get("metadata", {}).get("status") != "PASSED":
        raise IntegrityError("Phase 8 readiness requires the passing all-market audit")
    status = "READY_FOR_SEPARATE_HIGH_RISK_EVALUATION_APPROVAL"
    blocker: str | None = None
    try:
        _validate_index_metadata(
            index, rulebook_hash=rulebook_hash, audit_release_id=AUDIT_RELEASE_ID
        )
    except IntegrityError as exc:
        status = "BLOCKED_METADATA_DRIFT"
        blocker = str(exc)
    evaluation_config, _ = load_tier1_phase8_evaluation_config(root=root)
    sizing = evaluation_config.get("position_sizing")
    is_bracket_successor = (
        isinstance(sizing, dict)
        and sizing.get("method") == "atr_bracket_fixed_one_contract"
    )
    if status == "READY_FOR_SEPARATE_HIGH_RISK_EVALUATION_APPROVAL" and is_bracket_successor:
        status = "BLOCKED_NEW_BRACKET_TRIAL_NOT_REGISTERED"
        blocker = "the frozen five-minute prediction release is incompatible with the new bracket trial"

    return Tier1Phase8Readiness(
        status=status,
        blocker=blocker,
        index_release_id=INDEX_RELEASE_ID,
        audit_release_id=AUDIT_RELEASE_ID,
        rulebook_hash=rulebook_hash,
        evaluation_config_hash=preparation.evaluation_config_hash,
        result_label="PROVISIONAL_EXECUTION_COSTS",
        checked_paths=(
            "state/trial_registry/phase6_prediction_only",
            "manifests/data_releases/reference",
            "configs/contract_economics_rules.json",
            "configs/prop_firm_risk_profile.json",
            "configs/tier1_phase8_evaluation.json",
        ),
    )


def audit_tier1_phase8_bracket_readiness(*, root: Path) -> Tier1Phase8BracketReadiness:
    """Check local bracket mechanics without opening real release rows."""

    legacy = audit_tier1_phase8_readiness(root=root)
    if legacy.status != "BLOCKED_NEW_BRACKET_TRIAL_NOT_REGISTERED":
        raise IntegrityError("Phase 8 bracket readiness requires the old prediction path to be blocked")
    contract = load_tier1_bracket_trial_contract(root=root)
    if contract.get("trial_status") != "LOCAL_IMPLEMENTATION_ONLY_NOT_REGISTERED":
        raise IntegrityError("bracket trial must remain local and unregistered")
    evaluation_config, _ = load_tier1_phase8_evaluation_config(root=root)
    risk_audit = run_default_tier1_risk_realism_audit(
        evaluation_config=evaluation_config
    )
    if risk_audit.policy_bypasses:
        raise IntegrityError("local bracket risk audit found a policy bypass")
    registered = load_registered_tier1_bracket_trial(root=root)
    return Tier1Phase8BracketReadiness(
        status=(
            "REGISTERED_BRACKET_TRIAL_AWAITING_SEPARATE_REAL_DATA_APPROVAL"
            if registered is not None
            else "LOCAL_BRACKET_IMPLEMENTATION_READY_FOR_SEPARATE_TRIAL_REGISTRATION_APPROVAL"
        ),
        index_release_id=legacy.index_release_id,
        audit_release_id=legacy.audit_release_id,
        rulebook_hash=legacy.rulebook_hash,
        risk_audit_policy_controls_pass=True,
        old_five_minute_predictions_blocked=True,
        registered_trial_id=None if registered is None else str(registered["trial_id"]),
        registered_trial_state=None if registered is None else str(registered["registration_state"]),
        checked_paths=(
            "manifests/data_releases/reference",
            "configs/contract_economics_rules.json",
            "configs/tier1_phase8_evaluation.json",
            "configs/tier1_bracket_trial.json",
            "reports/audits/final/tier1_bracket_policy_risk_audit.json",
        ),
    )
