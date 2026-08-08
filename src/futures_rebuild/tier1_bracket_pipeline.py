"""Create-only registration for the research-only Tier 1 bracket successor.

Registration proves that the bracket trial was declared before any of its
source rows are opened.  It deliberately does not build labels, features,
splits, fit a model, or materialize predictions.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

from .active_phase5_splits import ReleasePair, discover_tier1_release_pairs
from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError
from .tier1_bracket_trial import load_tier1_bracket_trial_contract
from .tier1_phase8_evaluation_config import (
    CONFIG_RELATIVE_PATH,
    RISK_PROFILE_RELATIVE_PATH,
    load_tier1_phase8_evaluation_config,
)
from .tier1_phase8_readiness import audit_tier1_phase8_bracket_readiness


SCHEMA_VERSION = "tier1_bracket_prediction_trial/1.0.0"
REGISTRY_RELATIVE_ROOT = Path("state/trial_registry/tier1_bracket_prediction")
EVENT_RELATIVE_ROOT = Path("state/trial_events/tier1_bracket_prediction")
MODEL_CONTRACT_RELATIVE_ROOT = Path("state/trial_registry/tier1_bracket_model_contract")
SIGNAL_CONTRACT_RELATIVE_ROOT = Path("state/trial_registry/tier1_bracket_signal_contract")
SIGNAL_EVENT_RELATIVE_ROOT = Path("state/trial_events/tier1_bracket_signal_contract")


def _source_pair(pair: ReleasePair) -> dict[str, object]:
    return {
        "market": pair.market,
        "year": pair.year,
        "prior_feature_release_id": pair.feature_release_id,
        "prior_outcome_release_id": pair.outcome_release_id,
        "source_parquet_sha256": pair.source_parquet_sha256,
    }


@dataclass(frozen=True)
class Tier1BracketPipelineDeclaration:
    """Fully bound declaration that precedes all bracket source-row access."""

    trial_id: str
    payload: Mapping[str, object]


def build_tier1_bracket_model_contract(*, parent_trial_id: str) -> Tier1BracketPipelineDeclaration:
    """Lock the fresh directional model before any bracket source rows open."""

    if not isinstance(parent_trial_id, str) or len(parent_trial_id) != 64:
        raise IntegrityError("bracket model contract requires its registered parent trial")
    core = {
        "schema_version": "tier1_bracket_model_contract/1.0.0",
        "parent_trial_id": parent_trial_id,
        "state": "LOCKED_BEFORE_BRACKET_SOURCE_ROW_OPEN",
        "targets": {
            "long": "realized_net_r_after_locked_bracket_costs",
            "short": "realized_net_r_after_locked_bracket_costs",
        },
        "features": ["bar_body_fraction", "bar_return", "intrabar_range_fraction", "volume"],
        "model": {"family": "RIDGE_LINEAR_DIRECTIONAL_BRACKET", "ridge_penalty": 1.0, "seed": 106},
        "splits": {
            "family": "nested_chronological_eight_fold",
            "session_embargo": 1,
            "purge_horizon_minutes": 60,
            "rebuild_from_bracket_valid_sessions": True,
        },
        "old_five_minute_labels_features_predictions": "FORBIDDEN",
        "holdout_or_forward_access": False,
        "economics_evaluation": False,
        "provider_access": False,
        "trading": False,
    }
    return Tier1BracketPipelineDeclaration(trial_id=sha256_json(core), payload=core)


def persist_tier1_bracket_model_contract(
    *, root: Path, contract: Tier1BracketPipelineDeclaration
) -> dict[str, str]:
    """Create the model contract exactly once, prior to any row processing."""

    target = root / MODEL_CONTRACT_RELATIVE_ROOT / f"{contract.trial_id}.json"
    if target.exists():
        raise IntegrityError("Tier 1 bracket model contract already exists")
    payload = {**contract.payload, "model_contract_id": contract.trial_id, "locked_at_utc": datetime.now(timezone.utc).isoformat()}
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
    try:
        os.write(descriptor, canonical_bytes(payload) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return {"model_contract_id": contract.trial_id, "path": target.relative_to(root).as_posix()}


def build_tier1_bracket_signal_contract(
    *, parent_trial_id: str, model_contract_id: str
) -> Tier1BracketPipelineDeclaration:
    """Lock signal selection separately from the directional model contract.

    This is intentionally a successor contract: the primary model target stays
    realized net R, while the triple-barrier class is retained only as a
    diagnostic.  The threshold is fitted from each fold's training scores, not
    its test data.
    """

    if not all(isinstance(value, str) and len(value) == 64 for value in (parent_trial_id, model_contract_id)):
        raise IntegrityError("bracket signal contract requires registered trial and model contract")
    core = {
        "schema_version": "tier1_bracket_signal_contract/1.0.0",
        "parent_trial_id": parent_trial_id,
        "model_contract_id": model_contract_id,
        "state": "LOCKED_BEFORE_BRACKET_SOURCE_ROW_OPEN",
        "primary_targets": {
            "long": "realized_net_r_after_locked_bracket_costs",
            "short": "realized_net_r_after_locked_bracket_costs",
        },
        "diagnostic_label": {
            "family": "triple_barrier_outcome_class",
            "classes": ["TARGET_FIRST", "STOP_FIRST", "VERTICAL_OR_SAFETY_EXIT", "UNAVAILABLE"],
            "not_a_training_target": True,
        },
        "signal": {
            "formula": "(long_prediction_net_r-short_prediction_net_r)/(abs(long_prediction_net_r)+abs(short_prediction_net_r)+1e-12)",
            "bounded_range": [-1.0, 1.0],
            "neutral_threshold": {
                "method": "nearest_rank_quantile",
                "quantile": 0.60,
                "fit_scope": "outer_fold_training_rows_only",
                "test_or_holdout_rows_forbidden": True,
            },
            "selection": {"long": "score >= threshold", "short": "score <= -threshold", "neutral": "otherwise_or_invalid_score"},
        },
        "old_five_minute_labels_features_predictions": "FORBIDDEN",
        "holdout_or_forward_access": False,
        "economics_evaluation": False,
        "provider_access": False,
        "trading": False,
    }
    return Tier1BracketPipelineDeclaration(trial_id=sha256_json(core), payload=core)


def persist_tier1_bracket_signal_contract(
    *, root: Path, contract: Tier1BracketPipelineDeclaration
) -> dict[str, str]:
    """Create the immutable signal contract and evidence event exactly once."""

    registry = root / SIGNAL_CONTRACT_RELATIVE_ROOT / f"{contract.trial_id}.json"
    event = root / SIGNAL_EVENT_RELATIVE_ROOT / f"{contract.trial_id}.json"
    if registry.exists() or event.exists():
        raise IntegrityError("Tier 1 bracket signal contract already exists")
    locked_at = datetime.now(timezone.utc).isoformat()
    payload = {**contract.payload, "signal_contract_id": contract.trial_id, "locked_at_utc": locked_at}
    event_payload = {
        "event_type": "DECLARED",
        "signal_contract_id": contract.trial_id,
        "locked_at_utc": locked_at,
        "source_row_access": False,
        "model_fit": False,
        "prediction_materialization": False,
    }
    for path, document in ((registry, payload), (event, event_payload)):
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
        try:
            os.write(descriptor, canonical_bytes(document) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return {"signal_contract_id": contract.trial_id, "registry_path": registry.relative_to(root).as_posix(), "event_path": event.relative_to(root).as_posix()}


def build_tier1_bracket_pipeline_declaration(
    *,
    bracket_contract: Mapping[str, object],
    evaluation_config_hash: str,
    risk_profile_hash: str,
    rulebook_hash: str,
    index_release_id: str,
    audit_release_id: str,
    source_pairs: Sequence[Mapping[str, object]],
) -> Tier1BracketPipelineDeclaration:
    """Build, but do not persist, the immutable bracket-trial declaration."""

    if bracket_contract.get("trial_status") != "LOCAL_IMPLEMENTATION_ONLY_NOT_REGISTERED":
        raise IntegrityError("bracket trial contract is not eligible for first registration")
    if not all(isinstance(value, str) and len(value) == 64 for value in (
        evaluation_config_hash, risk_profile_hash, rulebook_hash, index_release_id, audit_release_id,
    )):
        raise IntegrityError("bracket trial provenance hashes are invalid")
    expected_pairs = [(market, year) for market in ("ES", "CL", "ZN", "6E") for year in range(2018, 2023)]
    observed_pairs = [(pair.get("market"), pair.get("year")) for pair in source_pairs]
    if observed_pairs != expected_pairs:
        raise IntegrityError("bracket trial must bind the canonical 20 Tier 1 source pairs")
    if any(
        not isinstance(pair.get("prior_feature_release_id"), str)
        or not isinstance(pair.get("prior_outcome_release_id"), str)
        or not isinstance(pair.get("source_parquet_sha256"), str)
        for pair in source_pairs
    ):
        raise IntegrityError("bracket trial source-pair provenance is incomplete")

    core = {
        "schema_version": SCHEMA_VERSION,
        "phase": "tier1_bracket_successor",
        "state": "REGISTERED_BEFORE_BRACKET_SOURCE_ROW_OPEN",
        "research_only": True,
        "live_readiness": False,
        "markets": ["ES", "CL", "ZN", "6E"],
        "discovery_period": "2018-2022",
        "locked_untouched_holdout": "2025",
        "bracket_contract": dict(bracket_contract),
        "bracket_contract_sha256": sha256_json(bracket_contract),
        "phase8_evaluation_config_sha256": evaluation_config_hash,
        "risk_profile_sha256": risk_profile_hash,
        "rulebook_sha256": rulebook_hash,
        "phase8_index_release_id": index_release_id,
        "phase8_audit_release_id": audit_release_id,
        "source_pairs": [dict(pair) for pair in source_pairs],
        "old_five_minute_feature_outcome_reuse": "FORBIDDEN",
        "pipeline_outputs": {
            "directional_bracket_outcomes": "NOT_CREATED_REQUIRES_SEPARATE_REAL_DATA_APPROVAL",
            "mechanical_features": "NOT_CREATED_REQUIRES_SEPARATE_REAL_DATA_APPROVAL",
            "chronological_splits": "NOT_CREATED_REQUIRES_SEPARATE_REAL_DATA_APPROVAL",
            "frozen_predictions": "NOT_CREATED_REQUIRES_SEPARATE_REAL_DATA_APPROVAL",
        },
        "forbidden_without_separate_approval": [
            "historical_source_row_read",
            "bracket_label_materialization",
            "feature_materialization",
            "split_materialization",
            "model_fit",
            "prediction_materialization",
            "economics_evaluation",
            "provider_access",
            "trading",
            "live_deployment",
            "git_actions",
        ],
    }
    return Tier1BracketPipelineDeclaration(trial_id=sha256_json(core), payload=core)


def prepare_tier1_bracket_pipeline_registration(*, root: Path) -> Tier1BracketPipelineDeclaration:
    """Verify metadata-only prerequisites and form the first declaration."""

    readiness = audit_tier1_phase8_bracket_readiness(root=root)
    if readiness.status != "LOCAL_BRACKET_IMPLEMENTATION_READY_FOR_SEPARATE_TRIAL_REGISTRATION_APPROVAL":
        raise IntegrityError("bracket trial is not locally ready for registration")
    bracket_contract = load_tier1_bracket_trial_contract(root=root)
    _, evaluation_config_hash = load_tier1_phase8_evaluation_config(root=root)
    source_pairs = tuple(
        _source_pair(pair)
        for pair in discover_tier1_release_pairs(boundary=RepoBoundary(active_root=root))
    )
    return build_tier1_bracket_pipeline_declaration(
        bracket_contract=bracket_contract,
        evaluation_config_hash=evaluation_config_hash,
        risk_profile_hash=sha256_file(root / RISK_PROFILE_RELATIVE_PATH),
        rulebook_hash=readiness.rulebook_hash,
        index_release_id=readiness.index_release_id,
        audit_release_id=readiness.audit_release_id,
        source_pairs=source_pairs,
    )


def persist_tier1_bracket_pipeline_registration(
    *, root: Path, declaration: Tier1BracketPipelineDeclaration
) -> dict[str, str]:
    """Persist exactly one immutable declaration and event using create-only I/O."""

    registry = root / REGISTRY_RELATIVE_ROOT / f"{declaration.trial_id}.json"
    event = root / EVENT_RELATIVE_ROOT / f"{declaration.trial_id}.json"
    if registry.exists() or event.exists():
        raise IntegrityError("Tier 1 bracket trial registration already exists")
    registered_at = datetime.now(timezone.utc).isoformat()
    registration = {
        **declaration.payload,
        "trial_id": declaration.trial_id,
        "registered_at_utc": registered_at,
    }
    event_payload = {
        "event_type": "DECLARED",
        "trial_id": declaration.trial_id,
        "registered_at_utc": registered_at,
        "source_row_access": False,
        "model_fit": False,
        "prediction_materialization": False,
    }
    for path, payload in ((registry, registration), (event, event_payload)):
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0))
        try:
            os.write(descriptor, canonical_bytes(payload) + b"\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return {
        "trial_id": declaration.trial_id,
        "registry_path": registry.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
    }


def register_tier1_bracket_pipeline(*, root: Path) -> dict[str, str]:
    """High-risk orchestration seam: declare the bracket trial, without row reads."""

    return persist_tier1_bracket_pipeline_registration(
        root=root,
        declaration=prepare_tier1_bracket_pipeline_registration(root=root),
    )
