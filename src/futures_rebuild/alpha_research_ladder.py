"""Fail-closed contracts for the staged Alpha research ladder.

The ladder governs evidence reuse and stage progression.  It does not fit a
model, read market rows, evaluate returns, publish research, or grant access.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from .canonical import canonical_bytes, contained_path, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


CONTRACT_SCHEMA = "alpha_research_ladder_contract/1.0.0"
PROFILE_SCHEMA = "alpha_research_ladder_profile/1.0.0"
POINTER_SCHEMA = "active_alpha_research_ladder/1.0.0"
SESSION_MANIFEST_SCHEMA = "alpha_ladder_session_manifest/1.0.0"
DECISION_SCHEMA = "alpha_ladder_stage_decision/1.0.0"

ACTIVE_POINTER_PATH = Path("configs/active_alpha_research_ladder.json")
STAGES = ("pilot", "tier_1", "tier_2", "tier_3", "holdout", "forward")
PREDECESSOR = {
    "pilot": "tier_0",
    "tier_1": "pilot",
    "tier_2": "tier_1",
    "tier_3": "tier_2",
    "holdout": "tier_3",
    "forward": "holdout",
}

CORE = ("ES", "CL", "ZN", "6E")
BALANCED = (
    "ES", "NQ", "CL", "NG", "RB", "GC", "HG", "SR3",
    "ZN", "ZB", "6E", "6J", "ZC", "ZS", "LE", "HE",
)
TRADITIONAL = (
    "ES", "NQ", "RTY", "YM", "CL", "NG", "RB", "HO", "GC", "SI",
    "HG", "PL", "SR3", "SR1", "ZQ", "TN", "ZT", "ZF", "ZN", "ZB",
    "UB", "6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S", "ZC",
    "ZS", "ZL", "ZM", "ZW", "KE", "LE", "HE", "GF",
)
SATELLITE = ("BTC", "ETH", "PA")
ALL_APPROVED = (*TRADITIONAL, *SATELLITE)

FROZEN_MECHANISM_FIELDS = (
    "features", "transformations", "model_family", "model_parameters",
    "checkpoint", "entry_rules", "ranking", "costs", "stop", "sizing",
    "baselines", "fold_construction", "metrics", "promotion_gates",
)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise IntegrityError(f"{name} must be a mapping")
    return value


def _strings(value: object, name: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or (not value and not allow_empty)
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise IntegrityError(f"{name} must be a string sequence")
    return tuple(value)


def _digest(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise IntegrityError(f"{name} must be a SHA-256 digest")
    return value


def _identity(payload: Mapping[str, object], key: str, schema: str) -> None:
    core = dict(payload)
    identity = core.pop(key, None)
    if core.get("schema_version") != schema or identity != sha256_json(core):
        raise IntegrityError(f"{key} is invalid")


def build_contract(*, predecessor_path: str, predecessor_sha256: str) -> dict[str, object]:
    """Build the inactive authoritative successor semantics."""

    _digest(predecessor_sha256, "predecessor SHA-256")
    core: dict[str, object] = {
        "schema_version": CONTRACT_SCHEMA,
        "classification": "PREPARED_NON_AUTHORIZING_SUCCESSOR",
        "state": "PREPARED_NOT_PUBLISHED_NOT_ACTIVE",
        "publication_layout": {
            "contract_path_template": (
                "state/alpha_ladder_registry/{contract_id}/universe_contract.json"
            ),
            "profile_path_template": (
                "state/alpha_ladder_registry/{contract_id}/alpha_tiered.yaml"
            ),
            "active_pointer_path": ACTIVE_POINTER_PATH.as_posix(),
            "active_pointer_written_last": True,
        },
        "predecessor": {
            "path": predecessor_path,
            "sha256": predecessor_sha256,
            "preserved_byte_for_byte": True,
        },
        "authority": {
            "historical_rows": False,
            "registration": False,
            "execution": False,
            "holdout_2025": False,
            "provider_network_credentials": False,
            "trading": False,
        },
        "stages": {
            "tier_0": {
                "role": "SYNTHETIC_ENGINEERING_ONLY",
                "markets": ["ES"],
                "historical_years": [],
                "alpha_evidence": False,
            },
            "pilot": {
                "role": "GO_NO_GO_SCREEN_ONLY",
                "markets": ["ES"],
                "training_sessions": 504,
                "evaluation_sessions": 63,
                "fold_selection": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
                "purge_and_embargo_required": True,
                "exact_session_ids_frozen_before_outcomes": True,
                "alpha_confirmation": False,
            },
            "tier_1": {
                "role": "FIRST_FORMAL_MULTI_MARKET_CONFIRMATION",
                "markets": list(CORE),
                "pilot_evaluation_sessions_excluded_for_every_market": True,
            },
            "tier_2": {
                "role": "FROZEN_BALANCED_REPLICATION",
                "markets": list(BALANCED),
                "report_core_and_additions_separately": True,
            },
            "tier_3": {
                "role": "FULL_UNIVERSE_REPLICATION",
                "markets": list(ALL_APPROVED),
                "traditional_markets": list(TRADITIONAL),
                "satellite_markets": list(SATELLITE),
                "traditional_must_pass_independently": True,
                "satellite_can_rescue_traditional_failure": False,
            },
            "holdout": {
                "role": "ONE_PROJECT_LEVEL_FINAL_HOLDOUT",
                "years": [2025],
                "maximum_accesses": 1,
                "terminal_tier": "tier_3",
            },
            "forward": {
                "role": "MONITORING_ONLY",
                "period": "2026_ONWARD",
                "can_rescue_failure": False,
            },
        },
        "transition_order": [
            "tier_0", "pilot", "tier_1", "tier_2", "tier_3", "holdout", "forward",
        ],
        "frozen_mechanism_fields": list(FROZEN_MECHANISM_FIELDS),
        "semantic_change": "NEW_COUNTED_MECHANISM_RESTARTS_AT_PILOT",
        "failed_higher_tier": "NO_FALLBACK_SCOPE_UNLESS_PREDECLARED_BEFORE_OUTCOMES",
        "missing_or_ambiguous_evidence": "FAIL_CLOSED",
    }
    return {**core, "contract_id": sha256_json(core)}


def validate_contract(payload: Mapping[str, object]) -> dict[str, object]:
    _identity(payload, "contract_id", CONTRACT_SCHEMA)
    stages = _mapping(payload.get("stages"), "stages")
    authority = _mapping(payload.get("authority"), "authority")
    if any(value is not False for value in authority.values()):
        raise IntegrityError("ladder contract cannot grant authority")
    expected = build_contract(
        predecessor_path=str(_mapping(payload.get("predecessor"), "predecessor").get("path", "")),
        predecessor_sha256=str(_mapping(payload.get("predecessor"), "predecessor").get("sha256", "")),
    )
    if dict(payload) != expected:
        raise IntegrityError("ladder contract semantics drifted")
    if set(stages) != {"tier_0", *STAGES}:
        raise IntegrityError("ladder stage topology drifted")
    return dict(payload)


def build_profile(
    *, contract_path: str, contract_sha256: str, contract_id: str,
) -> dict[str, object]:
    _digest(contract_sha256, "contract SHA-256")
    _digest(contract_id, "contract identity")
    core: dict[str, object] = {
        "schema_version": PROFILE_SCHEMA,
        "classification": "PREPARED_NON_AUTHORIZING_OPERATIONAL_VIEW",
        "state": "PREPARED_NOT_ACTIVE",
        "contract_binding": {
            "path": contract_path,
            "sha256": contract_sha256,
            "contract_id": contract_id,
        },
        "market_sets": {
            "core": list(CORE),
            "balanced": list(BALANCED),
            "traditional": list(TRADITIONAL),
            "satellite": list(SATELLITE),
            "all_approved": list(ALL_APPROVED),
        },
        "profiles": {
            "tier_0": {"markets": ["ES"], "data": "SYNTHETIC_ONLY"},
            "pilot": {
                "markets": ["ES"],
                "training_sessions": 504,
                "evaluation_sessions": 63,
                "result_use": "ADVANCE_OR_REJECT_EXACT_FROZEN_MECHANISM_ONLY",
            },
            "tier_1": {"market_set": "core"},
            "tier_2": {"market_set": "balanced"},
            "tier_3": {
                "market_set": "all_approved",
                "mandatory_subgroups": ["traditional", "satellite"],
                "traditional_must_pass_independently": True,
                "satellite_can_rescue_traditional_failure": False,
            },
            "holdout": {"years": [2025], "maximum_accesses": 1},
            "forward": {"period": "2026_ONWARD", "monitoring_only": True},
        },
        "authority": {
            "historical_rows": False,
            "registration": False,
            "execution": False,
            "holdout_2025": False,
            "provider_network_credentials": False,
            "trading": False,
        },
    }
    return {**core, "profile_id": sha256_json(core)}


def validate_profile(
    payload: Mapping[str, object], *, root: Path,
    prepared_contract_path: Path | None = None,
) -> dict[str, object]:
    _identity(payload, "profile_id", PROFILE_SCHEMA)
    binding = _mapping(payload.get("contract_binding"), "contract binding")
    contract_path = (
        prepared_contract_path.resolve(strict=False)
        if prepared_contract_path is not None
        else contained_path(root, str(binding.get("path", "")))
    )
    if sha256_file(contract_path) != _digest(binding.get("sha256"), "contract SHA-256"):
        raise IntegrityError("profile contract binding changed")
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    if not isinstance(contract, dict):
        raise IntegrityError("profile contract is malformed")
    validate_contract(contract)
    if binding.get("contract_id") != contract.get("contract_id"):
        raise IntegrityError("profile contract identity changed")
    expected = build_profile(
        contract_path=str(binding.get("path")),
        contract_sha256=str(binding.get("sha256")),
        contract_id=str(binding.get("contract_id")),
    )
    if dict(payload) != expected:
        raise IntegrityError("ladder profile semantics drifted")
    return dict(payload)


def _load_json(root: Path, relative: str, expected_sha256: str) -> dict[str, object]:
    path = contained_path(root, relative)
    if sha256_file(path) != _digest(expected_sha256, f"{relative} SHA-256"):
        raise IntegrityError(f"bound artifact changed: {relative}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise IntegrityError(f"bound artifact is invalid: {relative}") from exc
    if not isinstance(payload, dict) or path.read_bytes() != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"bound artifact is not canonical: {relative}")
    return payload


def load_active_ladder(root: Path) -> tuple[dict[str, object], dict[str, object]]:
    pointer_path = root / ACTIVE_POINTER_PATH
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise UnauthorizedOperation("no active Alpha research ladder") from exc
    if not isinstance(pointer, dict):
        raise IntegrityError("active Alpha ladder pointer is malformed")
    _identity(pointer, "pointer_id", POINTER_SCHEMA)
    contract = _load_json(
        root, str(pointer.get("contract_path", "")), str(pointer.get("contract_sha256", "")),
    )
    profile_path = contained_path(root, str(pointer.get("profile_path", "")))
    if sha256_file(profile_path) != _digest(pointer.get("profile_sha256"), "profile SHA-256"):
        raise IntegrityError("active Alpha ladder profile binding changed")
    try:
        profile = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise IntegrityError("active Alpha ladder profile is malformed") from exc
    if not isinstance(profile, dict):
        raise IntegrityError("active Alpha ladder profile is malformed")
    validate_contract(contract)
    validate_profile(profile, root=root)
    if (
        pointer.get("contract_id") != contract.get("contract_id")
        or pointer.get("profile_id") != profile.get("profile_id")
    ):
        raise IntegrityError("active Alpha ladder pointer identity drifted")
    return contract, profile


def build_active_pointer(
    *, contract_path: str, contract_sha256: str, contract_id: str,
    profile_path: str, profile_sha256: str, profile_id: str,
) -> dict[str, object]:
    core = {
        "schema_version": POINTER_SCHEMA,
        "contract_path": contract_path,
        "contract_sha256": contract_sha256,
        "contract_id": contract_id,
        "profile_path": profile_path,
        "profile_sha256": profile_sha256,
        "profile_id": profile_id,
    }
    return {**core, "pointer_id": sha256_json(core)}


def validate_stage_decision(
    payload: Mapping[str, object], *, contract_id: str, mechanism_sha256: str,
    expected_stage: str, root: Path | None = None,
) -> dict[str, object]:
    _identity(payload, "decision_id", DECISION_SCHEMA)
    if (
        payload.get("contract_id") != contract_id
        or payload.get("mechanism_sha256") != mechanism_sha256
        or payload.get("stage") != expected_stage
        or payload.get("decision") != "PASS"
    ):
        raise UnauthorizedOperation("required predecessor stage did not pass")
    if expected_stage == "tier_0":
        if root is None:
            raise IntegrityError("Tier 0 decision validation requires repository context")
        certificate_path = str(payload.get("synthetic_certificate_path", ""))
        certificate_sha = str(payload.get("synthetic_certificate_sha256", ""))
        certificate = _load_json(root, certificate_path, certificate_sha)
        from .alpha_ladder_frozen_mechanism import validate_tier0_certificate
        validate_tier0_certificate(
            certificate, contract_id=contract_id, mechanism_sha256=mechanism_sha256,
        )
    elif expected_stage in {"pilot", "tier_1", "tier_2", "tier_3"}:
        evidence = _mapping(payload.get("promotion_evidence"), "promotion evidence")
        from .alpha_ladder_frozen_mechanism import validate_promotion_evidence
        stage_markets = {
            "pilot": ("ES",), "tier_1": CORE, "tier_2": BALANCED,
            "tier_3": TRADITIONAL,
        }[expected_stage]
        validate_promotion_evidence(evidence, stage=expected_stage, markets=stage_markets)
    if expected_stage == "tier_3":
        subgroup = _mapping(payload.get("subgroup_decisions"), "Tier 3 subgroup decisions")
        if (
            subgroup.get("traditional") != "PASS"
            or subgroup.get("combined") != "PASS"
            or subgroup.get("satellite_can_rescue_traditional_failure") is not False
        ):
            raise UnauthorizedOperation("Tier 3 traditional subgroup did not pass independently")
    return dict(payload)


def validate_session_manifest(
    payload: Mapping[str, object], *, contract_id: str, mechanism_sha256: str,
    stage: str, markets: Sequence[str], pilot_evaluation_sha256: str | None = None,
) -> dict[str, object]:
    _identity(payload, "manifest_id", SESSION_MANIFEST_SCHEMA)
    if (
        payload.get("contract_id") != contract_id
        or payload.get("mechanism_sha256") != mechanism_sha256
        or payload.get("stage") != stage
    ):
        raise IntegrityError("session manifest identity is mismatched")
    if stage == "pilot":
        training = _strings(payload.get("training_session_ids"), "pilot training sessions")
        evaluation = _strings(payload.get("evaluation_session_ids"), "pilot evaluation sessions")
        if (
            len(training) != 504
            or len(evaluation) != 63
            or len(set((*training, *evaluation))) != 567
            or tuple(sorted((*training, *evaluation))) != (*training, *evaluation)
            or payload.get("markets") != ["ES"]
            or payload.get("fold_ordinal") != 0
            or payload.get("selection_rule")
            != "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD"
            or payload.get("purge_applied") is not True
            or payload.get("embargo_applied") is not True
        ):
            raise UnauthorizedOperation("pilot fold is not the locked 504/63 chronological fold")
    else:
        by_market = _mapping(payload.get("evaluation_session_ids_by_market"), "stage sessions")
        if set(by_market) != set(markets):
            raise IntegrityError("stage session manifest does not cover its exact markets")
        pilot_ids = _strings(payload.get("excluded_pilot_evaluation_session_ids"), "pilot exclusions")
        if (
            len(pilot_ids) != 63
            or len(set(pilot_ids)) != 63
            or tuple(sorted(pilot_ids)) != pilot_ids
            or sha256_json(list(pilot_ids)) != pilot_evaluation_sha256
        ):
            raise IntegrityError("stage session manifest lost the pilot exclusion binding")
        for market, values in by_market.items():
            sessions = _strings(values, f"{market} evaluation sessions")
            if len(set(sessions)) != len(sessions) or tuple(sorted(sessions)) != sessions:
                raise IntegrityError("stage evaluation sessions are not unique and chronological")
            if set(sessions) & set(pilot_ids):
                raise UnauthorizedOperation("pilot evaluation sessions were reused")
    return dict(payload)


def validate_stage_registration(
    registration: Mapping[str, object], *, certificate: Mapping[str, object], root: Path,
) -> dict[str, str]:
    contract, profile = load_active_ladder(root)
    binding = _mapping(registration.get("alpha_ladder_binding"), "Alpha ladder binding")
    stage = str(binding.get("stage", ""))
    if stage not in STAGES:
        raise UnauthorizedOperation("real-history registration has no current Alpha ladder stage")
    if stage == "tier_0":
        raise UnauthorizedOperation("Tier 0 is synthetic-only")
    mechanism = _digest(binding.get("mechanism_sha256"), "mechanism SHA-256")
    if (
        binding.get("contract_id") != contract.get("contract_id")
        or binding.get("profile_id") != profile.get("profile_id")
    ):
        raise IntegrityError("registration is not bound to the active Alpha ladder")

    mechanism_path = str(binding.get("mechanism_path", ""))
    frozen_mechanism = _load_json(root, mechanism_path, mechanism)
    from .alpha_ladder_frozen_mechanism import validate_frozen_mechanism
    validate_frozen_mechanism(frozen_mechanism)
    mechanism_id = frozen_mechanism.get("mechanism_id")
    ladder_binding = _mapping(
        frozen_mechanism.get("ladder_binding"), "frozen mechanism ladder binding",
    )
    if (
        registration.get("protocol_id") != mechanism_id
        or binding.get("mechanism_id") != mechanism_id
        or ladder_binding.get("contract_id") != contract.get("contract_id")
        or ladder_binding.get("profile_id") != profile.get("profile_id")
    ):
        raise IntegrityError("registration frozen mechanism binding changed")

    expected_markets = {
        "pilot": ("ES",), "tier_1": CORE, "tier_2": BALANCED,
        "tier_3": ALL_APPROVED, "holdout": ALL_APPROVED, "forward": ALL_APPROVED,
    }[stage]
    requirements = _mapping(certificate.get("requirements"), "readiness requirements")
    if certificate.get("protocol_id") != mechanism_id:
        raise UnauthorizedOperation("row certificate covers a different frozen mechanism")
    if tuple(requirements.get("required_markets", ())) != expected_markets:
        raise UnauthorizedOperation("row certificate does not cover the exact ladder market set")
    if stage == "pilot" and (
        requirements.get("minimum_training_sessions") != 504
        or requirements.get("minimum_evaluation_sessions") != 63
        or requirements.get("minimum_purge_minutes", 0) <= 0
        or requirements.get("minimum_embargo_sessions", 0) <= 0
    ):
        raise UnauthorizedOperation("pilot readiness does not certify the locked fold")

    predecessor_path = str(binding.get("predecessor_decision_path", ""))
    predecessor_sha = str(binding.get("predecessor_decision_sha256", ""))
    predecessor = _load_json(root, predecessor_path, predecessor_sha)
    validate_stage_decision(
        predecessor, contract_id=str(contract["contract_id"]),
        mechanism_sha256=mechanism, expected_stage=PREDECESSOR[stage], root=root,
    )

    manifest_path = str(binding.get("session_manifest_path", ""))
    manifest_sha = str(binding.get("session_manifest_sha256", ""))
    manifest = _load_json(root, manifest_path, manifest_sha)
    source_bindings = _mapping(certificate.get("source_bindings"), "certificate source bindings")
    if source_bindings.get(manifest_path) != manifest_sha:
        raise IntegrityError("row certificate does not bind the exact stage sessions")
    pilot_eval_sha = binding.get("pilot_evaluation_session_ids_sha256")
    if stage != "pilot":
        pilot_eval_sha = _digest(pilot_eval_sha, "pilot evaluation session identity")
    validate_session_manifest(
        manifest, contract_id=str(contract["contract_id"]),
        mechanism_sha256=mechanism, stage=stage, markets=expected_markets,
        pilot_evaluation_sha256=pilot_eval_sha if isinstance(pilot_eval_sha, str) else None,
    )
    if stage == "pilot":
        evaluation = manifest["evaluation_session_ids"]
        assert isinstance(evaluation, list)
        pilot_eval_sha = sha256_json(evaluation)
        next_path = str(binding.get("tier_1_readiness_evidence_path", ""))
        next_manifest_path = str(binding.get("tier_1_session_manifest_path", ""))
        if not next_path or not next_manifest_path:
            raise UnauthorizedOperation(
                "pilot registration requires executable four-market Tier 1 readiness"
            )
        next_sha = _digest(
            binding.get("tier_1_readiness_evidence_sha256"),
            "Tier 1 readiness evidence SHA-256",
        )
        next_manifest_sha = _digest(
            binding.get("tier_1_session_manifest_sha256"),
            "Tier 1 session manifest SHA-256",
        )
        next_certificate = _load_json(root, next_path, next_sha)
        next_manifest = _load_json(root, next_manifest_path, next_manifest_sha)
        from .preexecution_fold_certification import require_registration_ready
        require_registration_ready(next_certificate, root=root)
        next_requirements = _mapping(
            next_certificate.get("requirements"), "Tier 1 readiness requirements",
        )
        next_sources = _mapping(
            next_certificate.get("source_bindings"), "Tier 1 readiness source bindings",
        )
        if (
            next_certificate.get("protocol_id") != mechanism_id
            or tuple(next_requirements.get("required_markets", ())) != CORE
            or next_sources.get(next_manifest_path) != next_manifest_sha
        ):
            raise UnauthorizedOperation(
                "pilot registration requires executable four-market Tier 1 readiness"
            )
        validate_session_manifest(
            next_manifest, contract_id=str(contract["contract_id"]),
            mechanism_sha256=mechanism, stage="tier_1", markets=CORE,
            pilot_evaluation_sha256=str(pilot_eval_sha),
        )

    if stage == "tier_2":
        reporting = _mapping(registration.get("reporting"), "Tier 2 reporting")
        if reporting != {
            "core_markets": list(CORE),
            "addition_markets": [market for market in BALANCED if market not in CORE],
            "report_separately": True,
        }:
            raise UnauthorizedOperation("Tier 2 core/addition reporting is incomplete")
    if stage == "tier_3":
        reporting = _mapping(registration.get("reporting"), "Tier 3 reporting")
        if reporting != {
            "traditional_markets": list(TRADITIONAL),
            "satellite_markets": list(SATELLITE),
            "combined_markets": list(ALL_APPROVED),
            "traditional_must_pass_independently": True,
            "satellite_can_rescue_traditional_failure": False,
        }:
            raise UnauthorizedOperation("Tier 3 mandatory 38/3 reporting is incomplete")

    return {
        "alpha_ladder_contract_id": str(contract["contract_id"]),
        "alpha_ladder_profile_id": str(profile["profile_id"]),
        "alpha_ladder_stage": stage,
        "mechanism_sha256": mechanism,
        "predecessor_decision_sha256": predecessor_sha,
        "session_manifest_sha256": manifest_sha,
        "pilot_evaluation_session_ids_sha256": str(pilot_eval_sha),
    }
