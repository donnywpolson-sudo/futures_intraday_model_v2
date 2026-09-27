"""Frozen feature-search declarations and fold-local audit manifests."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

from ..canonical import sha256_json
from .contracts import ResearchContractError


SCHEMA = "feature_foundation_contract/1.1.0"
MANIFEST_SCHEMA = "fold_feature_selection_manifest/1.0.0"
RUN_SCHEMA = "feature_foundation_run_audit/1.0.0"
STABILITY_METRICS = {
    "MEAN_PAIRWISE_JACCARD",
    "MINIMUM_INCLUSION_FREQUENCY",
    "MINIMUM_SIGN_AGREEMENT",
}


def optional_feature_jaccard(
    selected_feature_names: Sequence[Sequence[str]], optional_names: Sequence[str],
) -> float | None:
    """Return optional-only mean pairwise Jaccard; baseline-only is not applicable."""

    optional = set(optional_names)
    sets = [set(values) & optional for values in selected_feature_names]
    if not sets or all(not value for value in sets):
        return None
    pairs = [
        len(left & right) / len(left | right) if left | right else 1.0
        for index, left in enumerate(sets)
        for right in sets[index + 1:]
    ]
    return float(sum(pairs) / len(pairs)) if pairs else 1.0


def _candidate_features(value: object, all_features: tuple[str, ...]) -> tuple[str, ...]:
    return all_features if value == "ALL_DECLARED" else _strings(
        value, name="candidate features"
    )


def _strings(value: object, *, name: str, allow_empty: bool = False) -> tuple[str, ...]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or (not value and not allow_empty)
        or any(type(item) is not str or not item or not item.isascii() for item in value)
    ):
        raise ResearchContractError(f"{name} must be an ASCII string sequence")
    result = tuple(value)
    if len(set(result)) != len(result):
        raise ResearchContractError(f"{name} contains duplicates")
    return result


def _digest(value: object, *, name: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ResearchContractError(f"{name} must be a SHA-256 digest")
    return value


def _identified(payload: Mapping[str, object], *, field: str) -> None:
    core = dict(payload)
    recorded = core.pop(field, None)
    if recorded != sha256_json(core):
        raise ResearchContractError(f"invalid {field}")


def validate_feature_foundation_contract(
    payload: Mapping[str, object],
) -> dict[str, object]:
    """Require the complete library, expansion grammar, search, and stability rule."""

    required = {
        "schema_version",
        "state",
        "authority",
        "outcome_foundation_contract_id",
        "stage_order",
        "core_library",
        "expanded_feature_names",
        "feature_definitions",
        "expansion_grammar",
        "feature_discovery",
        "candidate_budget",
        "candidates",
        "selector",
        "stability",
        "outer_results_may_change_features",
        "contract_id",
    }
    if set(payload) != required or payload.get("schema_version") != SCHEMA:
        raise ResearchContractError("feature-foundation contract schema is invalid")
    _identified(payload, field="contract_id")
    if payload.get("state") != "PREPARED_NOT_ACTIVE":
        raise ResearchContractError("feature-foundation contract must remain inactive")
    if payload.get("authority") != {
        "real_history": False,
        "holdout": False,
        "provider": False,
        "activation": False,
        "trading": False,
    }:
        raise ResearchContractError("feature-foundation contract cannot grant authority")
    _digest(
        payload.get("outcome_foundation_contract_id"),
        name="outcome foundation contract",
    )
    if payload.get("stage_order") not in ([
        "BASELINE_FEATURES",
        "FEATURE_DISCOVERY_EXPANSION",
        "FEATURE_SELECTION",
        "FREEZE_FEATURE_PROCEDURE",
    ], [
        "BASELINE_FEATURES",
        "BOUNDED_PREDEFINED_FEATURE_RANKING",
        "FEATURE_SELECTION",
        "FREEZE_FEATURE_PROCEDURE",
    ]):
        raise ResearchContractError("feature-foundation stage order is invalid")

    library = payload.get("core_library")
    if not isinstance(library, Mapping) or not library:
        raise ResearchContractError("core feature library is missing")
    core_names: list[str] = []
    for family, names in library.items():
        if type(family) is not str or not family or not family.isascii():
            raise ResearchContractError("feature family name is invalid")
        core_names.extend(_strings(names, name=f"{family} features"))
    if len(set(core_names)) != len(core_names):
        raise ResearchContractError("core features repeat across families")

    expanded = _strings(
        payload.get("expanded_feature_names"),
        name="expanded feature names",
        allow_empty=True,
    )
    if set(core_names) & set(expanded):
        raise ResearchContractError("core and expanded features overlap")
    all_features = tuple((*core_names, *expanded))
    definitions = payload.get("feature_definitions")
    if (
        not isinstance(definitions, Mapping)
        or set(definitions) != set(all_features)
        or any(type(value) is not str or not value for value in definitions.values())
    ):
        raise ResearchContractError("feature definitions do not cover the exact library")

    grammar = payload.get("expansion_grammar")
    grammar_keys = {
        "operators",
        "lags",
        "windows",
        "interactions",
        "maximum_expanded_features",
    }
    if not isinstance(grammar, Mapping) or set(grammar) != grammar_keys:
        raise ResearchContractError("feature expansion grammar is incomplete")
    _strings(grammar["operators"], name="expansion operators", allow_empty=not expanded)
    for key in ("lags", "windows"):
        values = grammar[key]
        if (
            not isinstance(values, Sequence)
            or isinstance(values, (str, bytes))
            or any(type(value) is not int or value < 1 for value in values)
            or len(set(values)) != len(values)
        ):
            raise ResearchContractError(f"expansion {key} are invalid")
    if type(grammar["interactions"]) is not bool:
        raise ResearchContractError("expansion interactions flag is invalid")
    maximum = grammar["maximum_expanded_features"]
    if type(maximum) is not int or maximum < 0 or len(expanded) > maximum:
        raise ResearchContractError("expanded feature budget was exceeded")

    discovery = payload.get("feature_discovery")
    if discovery != {
        "fit_scope": "INNER_TRAIN_ONLY",
        "attempt_budget": len(expanded),
        "ranking": "RMS_ABS_TRAIN_ONLY_CORRELATION_ACROSS_THREE_TARGETS",
        "output": "ORDERED_EXPANSION_CANDIDATES",
    }:
        raise ResearchContractError("feature discovery rule or budget is invalid")

    budget = payload.get("candidate_budget")
    candidates = payload.get("candidates")
    if type(budget) is not int or budget < 1 or not isinstance(candidates, list):
        raise ResearchContractError("candidate budget is invalid")
    if len(candidates) != budget:
        raise ResearchContractError("candidate list must exactly consume the declared budget")
    candidate_ids: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, Mapping) or set(candidate) != {
            "candidate_id",
            "feature_names",
            "hyperparameters",
            "hyperparameters_sha256",
        }:
            raise ResearchContractError("feature candidate schema is invalid")
        candidate_id = candidate["candidate_id"]
        if type(candidate_id) is not str or not candidate_id or not candidate_id.isascii():
            raise ResearchContractError("candidate ID is invalid")
        candidate_ids.append(candidate_id)
        names = _candidate_features(candidate["feature_names"], all_features)
        if not set(names) <= set(all_features):
            raise ResearchContractError("candidate uses an undeclared feature")
        if not isinstance(candidate["hyperparameters"], Mapping):
            raise ResearchContractError("candidate hyperparameters are invalid")
        if sha256_json(candidate["hyperparameters"]) != candidate["hyperparameters_sha256"]:
            raise ResearchContractError("candidate hyperparameter identity changed")
        _digest(candidate["hyperparameters_sha256"], name="candidate hyperparameters")
    if len(set(candidate_ids)) != len(candidate_ids):
        raise ResearchContractError("candidate IDs repeat")

    selector = payload.get("selector")
    selector_keys = {"fit_scope", "score_direction", "tie_break"}
    if isinstance(selector, Mapping) and "objective" in selector:
        selector_keys.add("objective")
    if (
        not isinstance(selector, Mapping)
        or set(selector) != selector_keys
        or selector.get("fit_scope") != "INNER_TRAIN_ONLY"
        or selector.get("tie_break") != "CANDIDATE_ID_ASCENDING"
        or selector.get("score_direction") not in {"MINIMIZE", "MAXIMIZE"}
        or (
            "objective" in selector
            and selector["objective"]
            != "SESSION_AND_MARKET_BALANCED_STRESS_NET_TICKS_THEN_TARGET_MSE"
        )
    ):
        raise ResearchContractError("feature selector is not deterministically frozen")
    stability = payload.get("stability")
    if not isinstance(stability, Mapping) or set(stability) != {
        "metric",
        "threshold",
        "feature_names",
        "failure_action",
    }:
        raise ResearchContractError("feature stability rule is incomplete")
    stability_names = _strings(stability["feature_names"], name="stability features")
    threshold = stability["threshold"]
    if (
        stability["metric"] not in STABILITY_METRICS
        or isinstance(threshold, bool)
        or not isinstance(threshold, (int, float))
        or not 0.0 <= float(threshold) <= 1.0
        or not set(stability_names) <= set(all_features)
        or stability["failure_action"] != "FAIL_PROMOTION"
    ):
        raise ResearchContractError("feature stability rule is invalid")
    if payload.get("outer_results_may_change_features") is not False:
        raise ResearchContractError("outer results cannot change the feature procedure")
    return dict(payload)


def build_fold_feature_manifest(
    contract: Mapping[str, object], result: object,
) -> dict[str, object]:
    """Bind one builder fold's candidates, selections, and exact fit samples."""

    frozen = validate_feature_foundation_contract(contract)
    try:
        selected_id = str(result.selected_candidate_id)
        scores = {str(key): float(value) for key, value in result.inner_selection_scores.items()}
        inner_audits = tuple(result.inner_fit_audits)
        outer_audit = result.outer_fit_audit
        artifact = result.artifact
        artifact_payload = artifact.payload()
    except (AttributeError, TypeError, ValueError) as exc:
        raise ResearchContractError("builder result cannot produce a feature manifest") from exc
    candidates = {str(item["candidate_id"]): item for item in frozen["candidates"]}
    all_features = tuple(
        name
        for family in frozen["core_library"].values()
        for name in family
    ) + tuple(frozen["expanded_feature_names"])
    if set(scores) != set(candidates) or selected_id not in candidates:
        raise ResearchContractError("builder result does not cover the frozen candidates")
    direction = frozen["selector"]["score_direction"]
    expected = (
        min(scores, key=lambda key: (scores[key], key))
        if direction == "MINIMIZE"
        else min(scores, key=lambda key: (-scores[key], key))
    )
    if selected_id != expected:
        raise ResearchContractError("selected candidate violates the frozen selector")
    if any(not math.isfinite(value) for value in scores.values()):
        raise ResearchContractError("selection scores are nonfinite")
    candidate_audits = {candidate_id: [] for candidate_id in candidates}
    for audit in inner_audits:
        audit.validate()
        if audit.candidate_id not in candidate_audits:
            raise ResearchContractError("fit audit contains an undeclared candidate")
        candidate_audits[audit.candidate_id].append({
            "inner_fold_id": audit.fold_id,
            "fit_sample_ids": list(audit.fit_sample_ids),
            "audit_sample_ids": list(audit.audit_sample_ids),
            "record_id": audit.record_id,
        })
    counts = {len(items) for items in candidate_audits.values()}
    if counts == {0} or len(counts) != 1:
        raise ResearchContractError("every candidate must have the same nonzero inner-fold audit count")
    outer_audit.validate()
    selected_names = tuple(artifact_payload["feature_names"])
    selected_weights = tuple(float(value) for value in artifact_payload["weights"])
    if (
        outer_audit.candidate_id != selected_id
        or not set(selected_names) <= set(
            _candidate_features(candidates[selected_id]["feature_names"], all_features)
        )
        or not set(
            name
            for family in frozen["core_library"].values()
            for name in family
        ) <= set(selected_names)
        or len(selected_names) != len(selected_weights)
    ):
        raise ResearchContractError("outer refit does not match the selected feature set")
    core: dict[str, object] = {
        "schema_version": MANIFEST_SCHEMA,
        "feature_foundation_contract_id": frozen["contract_id"],
        "fold_id": outer_audit.fold_id,
        "candidate_definitions": list(frozen["candidates"]),
        "selection_scores": {key: scores[key] for key in sorted(scores)},
        "selected_candidate_id": selected_id,
        "selected_feature_names": list(selected_names),
        "selected_coefficients": dict(zip(selected_names, selected_weights, strict=True)),
        "inner_fit_audits": candidate_audits,
        "outer_fit_audit": {
            "fit_sample_ids": list(outer_audit.fit_sample_ids),
            "audit_sample_ids": list(outer_audit.audit_sample_ids),
            "record_id": outer_audit.record_id,
        },
        "artifact_id": artifact.artifact_id,
        "state": "FOLD_LOCAL_SELECTION_AUDITED",
    }
    return {**core, "manifest_id": sha256_json(core)}


def validate_fold_feature_manifest(
    payload: Mapping[str, object], *, contract_id: str,
) -> dict[str, object]:
    _identified(payload, field="manifest_id")
    if (
        payload.get("schema_version") != MANIFEST_SCHEMA
        or payload.get("feature_foundation_contract_id") != contract_id
        or payload.get("state") != "FOLD_LOCAL_SELECTION_AUDITED"
    ):
        raise ResearchContractError("fold feature manifest binding is invalid")
    return dict(payload)


def build_feature_foundation_run_audit(
    contract: Mapping[str, object], results: Sequence[object],
) -> dict[str, object]:
    """Aggregate fold manifests and apply the mechanism-declared stability metric."""

    frozen = validate_feature_foundation_contract(contract)
    if not results:
        raise ResearchContractError("feature-foundation run has no folds")
    manifests = [build_fold_feature_manifest(frozen, result) for result in results]
    if len({item["fold_id"] for item in manifests}) != len(manifests):
        raise ResearchContractError("feature-foundation fold IDs repeat")
    stability = frozen["stability"]
    names = tuple(stability["feature_names"])
    selected_sets = [set(item["selected_feature_names"]) & set(names) for item in manifests]
    metric = stability["metric"]
    per_feature: dict[str, float] = {}
    baseline_only = all(not selected for selected in selected_sets)
    if metric == "MEAN_PAIRWISE_JACCARD":
        score = optional_feature_jaccard(
            [item["selected_feature_names"] for item in manifests], names,
        )
    elif metric == "MINIMUM_INCLUSION_FREQUENCY":
        per_feature = {
            name: sum(name in selected for selected in selected_sets) / len(selected_sets)
            for name in names
        }
        score = min(per_feature.values())
    else:
        if any(not set(names) <= selected for selected in selected_sets):
            raise ResearchContractError(
                "sign-agreement stability requires every declared feature in every fold"
            )
        for name in names:
            values = [float(item["selected_coefficients"][name]) for item in manifests]
            positive = sum(value > 0.0 for value in values)
            negative = sum(value < 0.0 for value in values)
            per_feature[name] = max(positive, negative) / len(values)
        score = min(per_feature.values())
    threshold = float(stability["threshold"])
    passed = baseline_only or bool(score is not None and score >= threshold)
    core: dict[str, object] = {
        "schema_version": RUN_SCHEMA,
        "feature_foundation_contract_id": frozen["contract_id"],
        "fold_manifest_ids": [item["manifest_id"] for item in manifests],
        "fold_manifests": manifests,
        "stability": {
            "metric": metric,
            "threshold": threshold,
            "score": score,
            "per_feature": per_feature,
            "applicability": "NOT_APPLICABLE_BASELINE_ONLY" if baseline_only else "OPTIONAL_EXPANSIONS_ONLY",
            "passed": passed,
            "failure_action": stability["failure_action"],
        },
        "state": "PASS" if passed else "FAIL_PROMOTION",
        "authority": {
            "real_history": False,
            "holdout": False,
            "activation": False,
        },
    }
    return {**core, "run_audit_id": sha256_json(core)}
