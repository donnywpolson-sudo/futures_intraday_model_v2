"""Inactive Stage 2.5 contract validation and derived governance views."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from .canonical import canonical_bytes, contained_path, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


SCHEMA = "causal_search_access_contract/1.0.0"
REQUIRED_ALPHA_LADDER_SCHEMA = "alpha_research_ladder_contract/9.0.0"
REGISTRY_ROOT = Path("state/causal_search_access_contract_registry")
REGISTRATION_BINDING = "causal_search_access_binding"
CLAIM_LABELS = ("ROBUST_PREDICTIVE_ASSOCIATION", "CAUSALLY_SUPPORTED_MECHANISM")
EVIDENCE_CLASSES = (
    "DECLARED",
    "ENGINEERING_ONLY",
    "DEVELOPMENT_EVIDENCE",
    "CHALLENGE_EVIDENCE",
    "REFIT_NO_EVALUATION",
    "SEALED_UNREAD",
    "FORWARD_MONITORING",
)
STAGE_ORDER = (
    "2.5_CAUSAL_SEARCH_ACCESS_CONTRACT",
    "3_OUTCOME_TARGET_FOUNDATION",
    "4_FEATURE_FOUNDATION",
    "5_CHRONOLOGICAL_VALIDATION_WFA",
    "6_FINAL_CANDIDATE_IMPLEMENTATION_FREEZE",
    "7_TIER_0",
    "8_TIER_1",
    "9_TIER_2_IF_BROAD",
    "10_TIER_3_IF_BROAD",
    "10.5_RECENT_REGIME_CHALLENGE",
    "10.6_DETERMINISTIC_REFIT_NO_EVALUATION",
    "11_FINAL_252",
    "12_FORWARD_MONITORING",
)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise IntegrityError(f"{name} must be an object")
    return value


def _strings(value: object, name: str) -> tuple[str, ...]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or not value
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise IntegrityError(f"{name} must be a non-empty string sequence")
    return tuple(value)


def _digest(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise IntegrityError(f"{name} must be a SHA-256 digest")
    return value


def validate_contract(payload: Mapping[str, object], *, root: Path) -> dict[str, object]:
    """Validate one immutable, non-authorizing Stage 2.5 declaration."""

    if payload.get("schema_version") in {
        "causal_search_access_contract/2.0.0",
        "causal_search_access_contract/2.1.0",
        "causal_search_access_contract/2.2.0",
        "causal_search_access_contract/2.3.0",
        "causal_search_access_contract/2.4.0",
        "causal_search_access_contract/2.5.0",
    }:
        from .bias_robustness_successor import validate_contract as validate_successor
        return validate_successor(payload, root=root)

    core = dict(payload)
    contract_id = core.pop("contract_id", None)
    if payload.get("schema_version") != SCHEMA or contract_id != sha256_json(core):
        raise IntegrityError("causal/search/access contract identity is invalid")
    if payload.get("state") != "PREPARED_NOT_ACTIVE" or payload.get("authority") != {
        "development_rows": False,
        "challenge_rows": False,
        "final_252_or_forward": False,
        "registration_or_execution": False,
        "provider_or_network": False,
        "trading": False,
    }:
        raise IntegrityError("causal/search/access contract must remain non-authorizing")

    protocol = _mapping(payload.get("protocol_binding"), "protocol binding")
    protocol_path = str(protocol.get("path", ""))
    if sha256_file(contained_path(root, protocol_path)) != _digest(
        protocol.get("sha256"), "protocol SHA-256"
    ):
        raise IntegrityError("bound search protocol changed")
    _digest(protocol.get("contract_id"), "protocol identity")

    question = _mapping(payload.get("question"), "question")
    for key in ("causal_question", "estimand", "mechanism_family", "claim_domain"):
        if not isinstance(question.get(key), str) or not question[key]:
            raise IntegrityError(f"question.{key} is absent")
    if question.get("claim_label") not in CLAIM_LABELS:
        raise IntegrityError("claim label is unknown")

    timing = _mapping(payload.get("timing"), "timing")
    for key in ("exposure_available_before", "outcome_starts_after", "outcome_matures_by"):
        if not isinstance(timing.get(key), str) or not timing[key]:
            raise IntegrityError(f"timing.{key} is absent")

    scope = _mapping(payload.get("scope"), "scope")
    markets = _strings(scope.get("development_markets"), "development markets")
    predicted = _strings(scope.get("predicted_effect_markets"), "predicted-effect markets")
    if not set(predicted) <= set(markets):
        raise IntegrityError("predicted-effect markets leave the authorized development scope")
    if scope.get("development_start_inclusive") != "2018-01-01T00:00:00Z" or scope.get(
        "development_end_exclusive"
    ) != "2023-01-01T00:00:00Z":
        raise IntegrityError("result-responsive development period drifted")

    search = _mapping(payload.get("search"), "search")
    if (
        not isinstance(search.get("trial_family"), str)
        or not search["trial_family"]
        or type(search.get("candidate_budget")) is not int
        or int(search["candidate_budget"]) <= 0
        or search.get("adaptive_candidate_creation") is not False
        or search.get("candidate_retry") is not False
        or not isinstance(search.get("evaluated_trial_definition"), str)
    ):
        raise IntegrityError("research-search declaration is incomplete")

    explanations = payload.get("competing_explanations")
    if not isinstance(explanations, list) or not 2 <= len(explanations) <= 3:
        raise IntegrityError("two or three competing explanations are required")
    for item in explanations:
        candidate = _mapping(item, "competing explanation")
        if any(not isinstance(candidate.get(key), str) or not candidate[key] for key in (
            "name", "discriminating_test", "expected_boundary"
        )):
            raise IntegrityError("competing explanation is incomplete")

    macro = _mapping(payload.get("zn_6e_selection"), "ZN/6E selection")
    if _strings(macro.get("candidates"), "macro candidates") != ("ZN", "6E"):
        raise IntegrityError("ZN/6E candidate order drifted")
    forbidden = set(_strings(macro.get("forbidden_evidence_fields"), "forbidden macro evidence"))
    if not {"pnl", "sharpe", "prediction_accuracy", "wfa", "holdout_result"} <= forbidden:
        raise IntegrityError("ZN/6E selection admits result-responsive evidence")

    wfa = _mapping(payload.get("wfa"), "WFA")
    if wfa.get("purpose") != "DEVELOPMENT_SELECTION_AND_ESTIMATION_NOT_CONFIRMATION":
        raise IntegrityError("WFA purpose is ambiguous")
    _strings(payload.get("robustness_families"), "robustness families")

    challenge = _mapping(payload.get("recent_regime_challenge"), "recent-regime challenge")
    if challenge != {
        "start_inclusive": "2023-01-01T00:00:00Z",
        "end_exclusive": "2025-07-13T22:00:00Z",
        "maximum_attempts": 1,
        "after_last_applicable_replication": True,
        "same_frozen_candidate_required": True,
        "tuning_design_change_or_retry_after_result": False,
        "failure_action": "CLOSE_MECHANISM_NO_RETRY",
    }:
        raise IntegrityError("recent-regime challenge rules drifted")
    refit = _mapping(payload.get("deterministic_refit"), "deterministic refit")
    if refit != {
        "training_start_inclusive": "2018-01-01T00:00:00Z",
        "training_end_exclusive": "2025-07-13T22:00:00Z",
        "same_algorithm_required": True,
        "fit_parameters_only": True,
        "performance_evaluation_allowed": False,
        "required_status": "REFIT_COMPLETE_NO_EVALUATION",
    }:
        raise IntegrityError("deterministic refit rules drifted")
    if tuple(payload.get("stage_order", ())) != STAGE_ORDER:
        raise IntegrityError("Stage 2.5 successor order drifted")
    if tuple(payload.get("evidence_classes", ())) != EVIDENCE_CLASSES:
        raise IntegrityError("evidence-status labels drifted")
    if tuple(payload.get("claim_labels", ())) != CLAIM_LABELS:
        raise IntegrityError("claim labels drifted")
    return dict(payload)


def load_registration_contract(
    root: Path, registration: Mapping[str, object], *, required: bool,
) -> dict[str, object] | None:
    """Load the exact Stage 2.5 contract bound by a registration."""

    binding = registration.get(REGISTRATION_BINDING)
    if binding is None and not required:
        return None
    binding = _mapping(binding, "causal/search/access binding")
    path = str(binding.get("path", ""))
    expected_sha = _digest(binding.get("sha256"), "causal/search/access SHA-256")
    contract_path = contained_path(root, path)
    if sha256_file(contract_path) != expected_sha:
        raise IntegrityError("causal/search/access registration binding changed")
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise IntegrityError("causal/search/access contract is unreadable") from exc
    if (
        not isinstance(payload, dict)
        or contract_path.read_bytes() != canonical_bytes(payload) + b"\n"
    ):
        raise IntegrityError("causal/search/access contract is not canonical")
    validate_contract(payload, root=root)
    if binding.get("contract_id") != payload.get("contract_id"):
        raise IntegrityError("causal/search/access contract identity changed")
    return payload


def registration_contract_scope(
    root: Path, registration: Mapping[str, object], *, required: bool,
) -> dict[str, str]:
    contract = load_registration_contract(root, registration, required=required)
    if contract is None:
        return {}
    binding = _mapping(registration[REGISTRATION_BINDING], "causal/search/access binding")
    if contract.get("schema_version") not in {
        "causal_search_access_contract/2.0.0",
        "causal_search_access_contract/2.1.0",
        "causal_search_access_contract/2.2.0",
        "causal_search_access_contract/2.3.0",
        "causal_search_access_contract/2.4.0",
        "causal_search_access_contract/2.5.0",
    }:
        return {
            "causal_search_access_contract_id": str(contract["contract_id"]),
            "causal_search_access_contract_sha256": str(binding["sha256"]),
        }
    from .bias_robustness_successor import load_declaration
    declaration_binding = _mapping(
        registration.get("pre_evaluation_declaration_binding"),
        "pre-evaluation declaration binding",
    )
    declaration = load_declaration(root, declaration_binding)
    search = _mapping(contract.get("search"), "search")
    if contract.get("schema_version") in {
        "causal_search_access_contract/2.4.0",
        "causal_search_access_contract/2.5.0",
    }:
        foundation = _mapping(contract.get("foundation_bindings"), "foundation bindings")
        protocol_id = foundation.get("feature_contract_id")
    else:
        protocol = _mapping(contract.get("protocol_binding"), "protocol binding")
        protocol_id = protocol.get("contract_id")
    if (
        declaration.get("stage_2_5_contract_id") != contract.get("contract_id")
        or declaration.get("protocol_id") != protocol_id
        or declaration.get("trial_family") != search.get("trial_family")
    ):
        raise IntegrityError("pre-evaluation declaration authority is mismatched")
    return {
        "causal_search_access_contract_id": str(contract["contract_id"]),
        "causal_search_access_contract_sha256": str(binding["sha256"]),
        "pre_evaluation_declaration_id": str(declaration["declaration_id"]),
        "pre_evaluation_declaration_sha256": str(declaration_binding["sha256"]),
    }


def governance_ledgers(root: Path) -> dict[str, object]:
    """Project the three ledgers from immutable declarations and registrations."""

    contracts: list[tuple[Path, dict[str, object]]] = []
    if (root / REGISTRY_ROOT).exists():
        for path in sorted((root / REGISTRY_ROOT).glob("*/*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise IntegrityError(f"invalid governance contract: {path}")
            if payload.get("schema_version") != SCHEMA:
                continue
            contracts.append((path, validate_contract(payload, root=root)))

    registrations: list[dict[str, object]] = []
    registry = root / "state/trial_registry"
    if registry.exists():
        for path in sorted(registry.glob("*/*.json")):
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and REGISTRATION_BINDING in payload:
                load_registration_contract(root, payload, required=True)
                registrations.append(payload)

    search_ledger: list[dict[str, object]] = []
    exposure_ledger: list[dict[str, object]] = []
    claim_ledger: list[dict[str, object]] = []
    unresolved_count = 0
    for path, contract in contracts:
        contract_id = str(contract["contract_id"])
        search = _mapping(contract["search"], "search")
        matching = [
            item for item in registrations
            if _mapping(item.get(REGISTRATION_BINDING), "registration binding").get(
                "contract_id"
            ) == contract_id
        ]
        source = path.relative_to(root).as_posix()
        source_sha = sha256_file(path)
        search_ledger.append({
            "contract_id": contract_id,
            "trial_family": search["trial_family"],
            "candidate_budget": search["candidate_budget"],
            "registered_evaluations": len(matching),
            "remaining_budget": max(0, int(search["candidate_budget"]) - len(matching)),
            "evaluated_trial_definition": search["evaluated_trial_definition"],
            "source_path": source,
            "source_sha256": source_sha,
        })
        scope = _mapping(contract["scope"], "scope")
        for period, status in (
            ("DEVELOPMENT_2018_2022", "DECLARED_NO_ACCESS"),
            ("RECENT_REGIME_2023_TO_2025_07_13", "FORBIDDEN_UNTIL_CHALLENGE_GATE"),
            ("FINAL_252", "SEALED_UNREAD"),
            ("FORWARD", "NOT_STARTED"),
        ):
            exposure_ledger.append({
                "contract_id": contract_id,
                "period": period,
                "markets": list(scope["predicted_effect_markets"]),
                "status": status,
                "source_path": source,
                "source_sha256": source_sha,
            })
        claim_ledger.append({
            "contract_id": contract_id,
            "claim_label": _mapping(contract["question"], "question")["claim_label"],
            "evidence_class": "DECLARED",
            "decision": "NOT_EVALUATED",
            "source_path": source,
            "source_sha256": source_sha,
        })

        bound_trials = {
            str(item.get("trial_id")): item for item in matching
            if isinstance(item.get("trial_id"), str)
        }
        if bound_trials:
            for evidence_path in sorted(registry.glob("**/*.json")):
                evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
                if not isinstance(evidence, dict):
                    continue
                trial_id = evidence.get("trial_id")
                use_relative = evidence.get("authorization_use_path")
                if (
                    not isinstance(trial_id, str)
                    or trial_id not in bound_trials
                    or use_relative is None
                ):
                    continue
                registration = bound_trials[str(trial_id)]
                ladder = _mapping(
                    registration.get("alpha_ladder_binding"), "Alpha ladder binding",
                )
                stage = str(ladder.get("stage", ""))
                period = {
                    "challenge": "RECENT_REGIME_2023_TO_2025_07_13",
                    "holdout": "FINAL_252",
                    "forward": "FORWARD",
                }.get(stage, "DEVELOPMENT_2018_2022")
                use_path = contained_path(root, str(use_relative))
                status = "CONSUMED" if use_path.is_file() else "UNRESOLVED"
                unresolved_count += int(status == "UNRESOLVED")
                exposure_ledger.append({
                    "contract_id": contract_id,
                    "trial_id": trial_id,
                    "period": period,
                    "markets": list(scope["predicted_effect_markets"]),
                    "status": status,
                    "source_path": evidence_path.relative_to(root).as_posix(),
                    "source_sha256": sha256_file(evidence_path),
                    "authorization_use_path": str(use_relative),
                })
                claim_ledger.append({
                    "contract_id": contract_id,
                    "trial_id": trial_id,
                    "claim_label": _mapping(contract["question"], "question")[
                        "claim_label"
                    ],
                    "evidence_class": {
                        "challenge": "CHALLENGE_EVIDENCE",
                        "refit": "REFIT_NO_EVALUATION",
                        "holdout": "SEALED_UNREAD",
                        "forward": "FORWARD_MONITORING",
                    }.get(stage, "DEVELOPMENT_EVIDENCE"),
                    "decision": str(registration.get("state", "UNRESOLVED")),
                    "source_path": evidence_path.relative_to(root).as_posix(),
                    "source_sha256": sha256_file(evidence_path),
                })

    core = {
        "schema_version": "research_governance_ledgers/1.0.0",
        "research_search": search_ledger,
        "protected_period_exposure": exposure_ledger,
        "claim_evidence_status": claim_ledger,
        "unresolved_count": unresolved_count,
        "authority": "DERIVED_READ_ONLY_NOT_AUTHORITY",
    }
    return {**core, "ledger_id": sha256_json(core)}


def require_inactive(contract: Mapping[str, object]) -> None:
    if contract.get("state") != "PREPARED_NOT_ACTIVE":
        raise UnauthorizedOperation("Stage 2.5 successor is not an inactive preparation")
