"""Fail-closed fold readiness certification for future research trials.

The certificate is deliberately separate from model fitting and economics.  It
accepts counts produced by an explicitly authorized source census and answers
one question: can every registered fold be executed exactly as declared?
Synthetic inputs prove mechanics only; they can never certify real sources.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any

from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    contained_path,
    sha256_file,
    sha256_json,
)
from .errors import IntegrityError, UnauthorizedOperation


SCHEMA_VERSION = "preexecution_fold_readiness_certificate/1.0.0"
ROW_CERTIFIED = "AUTHORIZED_HISTORICAL_ROW_CENSUS"
SYNTHETIC_ONLY = "SYNTHETIC_MECHANICS_ONLY"


def _integer(value: object, *, name: str) -> int:
    if type(value) is not int or value < 0:
        raise IntegrityError(f"{name} must be a nonnegative integer")
    return value


def _bool(value: object, *, name: str) -> bool:
    if type(value) is not bool:
        raise IntegrityError(f"{name} must be boolean")
    return value


def _strings(value: object, *, name: str) -> tuple[str, ...]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or not value
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise IntegrityError(f"{name} must be a nonempty string sequence")
    return tuple(value)


def _fold_ids(value: object, *, name: str) -> tuple[str, ...]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or any(not isinstance(item, str) or not item for item in value)
        or len(set(value)) != len(value)
    ):
        raise IntegrityError(f"{name} must be a unique string sequence")
    return tuple(value)


def _binding_map(value: object, *, name: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or not value:
        raise IntegrityError(f"{name} must be a nonempty mapping")
    result: dict[str, str] = {}
    for path, digest in value.items():
        if (
            not isinstance(path, str)
            or not path
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise IntegrityError(f"{name} contains an invalid binding")
        result[path] = digest
    return dict(sorted(result.items()))


def _risk_dispositions(
    value: object, *, scenarios: tuple[str, ...], selected: int, name: str,
) -> tuple[dict[str, dict[str, int]], bool]:
    if not isinstance(value, Mapping) or set(value) != set(scenarios):
        raise IntegrityError(f"{name} does not cover the locked scenarios")
    result: dict[str, dict[str, int]] = {}
    terminal = True
    for scenario in scenarios:
        raw = value.get(scenario)
        if not isinstance(raw, Mapping) or set(raw) != {
            "feasible_sessions", "risk_abstention_sessions", "unresolved_sessions",
        }:
            raise IntegrityError(f"{name} scenario disposition is malformed")
        feasible = _integer(
            raw.get("feasible_sessions"), name=f"{name} {scenario} feasible sessions",
        )
        abstentions = _integer(
            raw.get("risk_abstention_sessions"),
            name=f"{name} {scenario} risk abstentions",
        )
        unresolved = _integer(
            raw.get("unresolved_sessions"), name=f"{name} {scenario} unresolved sessions",
        )
        if feasible + abstentions + unresolved > selected:
            raise IntegrityError(f"{name} scenario dispositions exceed selected sessions")
        if feasible + abstentions != selected or unresolved != 0:
            terminal = False
        result[scenario] = {
            "feasible_sessions": feasible,
            "risk_abstention_sessions": abstentions,
            "unresolved_sessions": unresolved,
        }
    return dict(sorted(result.items())), terminal


def _fold_result(
    raw: Mapping[str, object], *, required_baselines: tuple[str, ...],
    required_cost_scenarios: tuple[str, ...],
    minimum_training_sessions: int, minimum_evaluation_sessions: int,
    minimum_purge_minutes: int, minimum_embargo_sessions: int,
) -> dict[str, object]:
    fold_id = raw.get("fold_id")
    market = raw.get("market")
    role = raw.get("role")
    if (
        not isinstance(fold_id, str) or not fold_id
        or not isinstance(market, str) or not market
        or role not in {"OUTER", "NESTED"}
    ):
        raise IntegrityError("fold evidence identity is malformed")

    counts = raw.get("counts")
    checks = raw.get("checks")
    exclusions = raw.get("exclusion_reasons")
    market_years = raw.get("market_year_breakdown")
    baselines = raw.get("baseline_universe_readiness")
    if (
        not isinstance(counts, Mapping)
        or not isinstance(checks, Mapping)
        or not isinstance(exclusions, Mapping)
        or not isinstance(market_years, Mapping)
        or not isinstance(baselines, Mapping)
    ):
        raise IntegrityError("fold evidence sections are malformed")

    expected_training = _integer(
        counts.get("expected_training_sessions"), name="expected training sessions",
    )
    complete_training = _integer(
        counts.get("complete_training_sessions"), name="complete training sessions",
    )
    feature_training = _integer(
        counts.get("feature_complete_training_sessions"),
        name="feature-complete training sessions",
    )
    transform_training = _integer(
        counts.get("transformation_ready_training_sessions"),
        name="transformation-ready training sessions",
    )
    expected_evaluation = _integer(
        counts.get("expected_evaluation_sessions"), name="expected evaluation sessions",
    )
    feature_evaluation = _integer(
        counts.get("feature_complete_evaluation_sessions"),
        name="feature-complete evaluation sessions",
    )
    terminal_evaluation = _integer(
        counts.get("terminal_evaluation_sessions"), name="terminal evaluation sessions",
    )
    execution_complete_evaluation = _integer(
        counts.get("execution_path_complete_evaluation_sessions"),
        name="execution-path-complete evaluation sessions",
    )
    candidate_selected = _integer(
        counts.get("candidate_selected_sessions"), name="candidate selected sessions",
    )
    candidate_paths = _integer(
        counts.get("candidate_selected_path_complete_sessions"),
        name="candidate selected-path-complete sessions",
    )
    risk_dispositions, candidate_risk_terminal = _risk_dispositions(
        counts.get("scenario_risk_dispositions"),
        scenarios=required_cost_scenarios,
        selected=candidate_selected,
        name="candidate risk evidence",
    )
    purge_minutes = _integer(counts.get("purge_minutes"), name="purge minutes")
    embargo_sessions = _integer(
        counts.get("embargo_sessions"), name="embargo sessions",
    )
    if (
        any(value > expected_training for value in (
            complete_training, feature_training, transform_training,
        ))
        or complete_training > feature_training
        or transform_training > feature_training
    ):
        raise IntegrityError("training readiness count exceeds its denominator")
    if (
        feature_evaluation > expected_evaluation
        or terminal_evaluation > expected_evaluation
        or execution_complete_evaluation > expected_evaluation
    ):
        raise IntegrityError("evaluation readiness count exceeds its denominator")
    if candidate_selected > feature_evaluation or candidate_paths > candidate_selected:
        raise IntegrityError("selected-path readiness count exceeds selected sessions")

    baseline_results: dict[str, dict[str, object]] = {}
    baseline_risk_terminal_by_name: dict[str, bool] = {}
    for baseline in required_baselines:
        baseline_raw = baselines.get(baseline)
        if not isinstance(baseline_raw, Mapping) or set(baseline_raw) != {
            "expected_sessions", "terminal_sessions", "selected_sessions",
            "selected_path_complete_sessions", "scenario_risk_dispositions",
            "schedule_independently_derived", "flat_no_trade",
        }:
            raise IntegrityError("baseline universe readiness is malformed")
        baseline_expected = _integer(
            baseline_raw.get("expected_sessions"), name=f"{baseline} expected sessions",
        )
        baseline_terminal = _integer(
            baseline_raw.get("terminal_sessions"), name=f"{baseline} terminal sessions",
        )
        baseline_selected = _integer(
            baseline_raw.get("selected_sessions"), name=f"{baseline} selected sessions",
        )
        baseline_paths = _integer(
            baseline_raw.get("selected_path_complete_sessions"),
            name=f"{baseline} selected path-complete sessions",
        )
        baseline_risk, baseline_risk_terminal = _risk_dispositions(
            baseline_raw.get("scenario_risk_dispositions"),
            scenarios=required_cost_scenarios,
            selected=baseline_selected,
            name=f"{baseline} risk evidence",
        )
        independently_derived = _bool(
            baseline_raw.get("schedule_independently_derived"),
            name=f"{baseline} independent scheduling",
        )
        flat_no_trade = _bool(
            baseline_raw.get("flat_no_trade"), name=f"{baseline} flat no-trade",
        )
        if (
            baseline_terminal > baseline_expected
            or baseline_selected > baseline_expected
            or baseline_paths > baseline_selected
        ):
            raise IntegrityError("baseline readiness count exceeds its denominator")
        baseline_results[baseline] = {
            "expected_sessions": baseline_expected,
            "terminal_sessions": baseline_terminal,
            "selected_sessions": baseline_selected,
            "selected_path_complete_sessions": baseline_paths,
            "scenario_risk_dispositions": baseline_risk,
            "schedule_independently_derived": independently_derived,
            "flat_no_trade": flat_no_trade,
        }
        baseline_risk_terminal_by_name[baseline] = baseline_risk_terminal
    if set(baselines) != set(required_baselines):
        raise IntegrityError("fold evidence does not cover exactly the required baselines")
    if sum(bool(item["flat_no_trade"]) for item in baseline_results.values()) != 1:
        raise IntegrityError("fold evidence requires exactly one flat no-trade baseline")

    exclusion_counts: dict[str, int] = {}
    for reason, count in exclusions.items():
        if not isinstance(reason, str) or not reason:
            raise IntegrityError("fold exclusion reason is malformed")
        exclusion_counts[reason] = _integer(count, name=f"{reason} exclusion count")
    if (
        sum(value for reason, value in exclusion_counts.items() if reason.startswith("TRAINING__"))
        != expected_training - complete_training
        or sum(
            value for reason, value in exclusion_counts.items()
            if reason.startswith("EVALUATION__")
        ) != expected_evaluation - execution_complete_evaluation
    ):
        raise IntegrityError("fold exclusion reasons do not reconcile to readiness counts")

    market_year_breakdown: dict[str, dict[str, object]] = {}
    market_year_totals = Counter()
    for year, raw_year in market_years.items():
        if not isinstance(year, str) or len(year) != 4 or not year.isdigit():
            raise IntegrityError("market-year readiness key is malformed")
        if not isinstance(raw_year, Mapping):
            raise IntegrityError("market-year readiness evidence is malformed")
        raw_exclusions = raw_year.get("exclusion_reasons")
        if not isinstance(raw_exclusions, Mapping):
            raise IntegrityError("market-year exclusions are malformed")
        year_counts = {
            name: _integer(raw_year.get(name), name=f"{year} {name}")
            for name in (
                "expected_training_sessions",
                "complete_training_sessions",
                "expected_evaluation_sessions",
                "feature_complete_evaluation_sessions",
                "terminal_evaluation_sessions",
                "execution_path_complete_evaluation_sessions",
            )
        }
        if (
            year_counts["complete_training_sessions"]
            > year_counts["expected_training_sessions"]
            or year_counts["terminal_evaluation_sessions"]
            > year_counts["expected_evaluation_sessions"]
            or year_counts["feature_complete_evaluation_sessions"]
            > year_counts["expected_evaluation_sessions"]
            or year_counts["execution_path_complete_evaluation_sessions"]
            > year_counts["expected_evaluation_sessions"]
        ):
            raise IntegrityError("market-year readiness count exceeds its denominator")
        year_exclusions = {
            reason: _integer(count, name=f"{year} {reason} exclusions")
            for reason, count in raw_exclusions.items()
            if isinstance(reason, str) and reason
        }
        if len(year_exclusions) != len(raw_exclusions):
            raise IntegrityError("market-year exclusion reason is malformed")
        if (
            sum(
                value for reason, value in year_exclusions.items()
                if reason.startswith("TRAINING__")
            )
            != year_counts["expected_training_sessions"]
            - year_counts["complete_training_sessions"]
            or sum(
                value for reason, value in year_exclusions.items()
                if reason.startswith("EVALUATION__")
            )
            != year_counts["expected_evaluation_sessions"]
            - year_counts["execution_path_complete_evaluation_sessions"]
        ):
            raise IntegrityError("market-year exclusions do not reconcile")
        market_year_breakdown[year] = {
            **year_counts,
            "exclusion_reasons": dict(sorted(year_exclusions.items())),
        }
        market_year_totals.update(year_counts)
    if not market_year_breakdown or any(
        market_year_totals[name] != expected
        for name, expected in {
            "expected_training_sessions": expected_training,
            "complete_training_sessions": complete_training,
            "expected_evaluation_sessions": expected_evaluation,
            "feature_complete_evaluation_sessions": feature_evaluation,
            "terminal_evaluation_sessions": terminal_evaluation,
            "execution_path_complete_evaluation_sessions": execution_complete_evaluation,
        }.items()
    ):
        raise IntegrityError("market-year readiness does not reconcile to fold totals")

    gate_checks = {
        "chronological_order": _bool(
            checks.get("chronological_order"), name="chronological order",
        ),
        "purge_applied": _bool(checks.get("purge_applied"), name="purge applied"),
        "embargo_applied": _bool(
            checks.get("embargo_applied"), name="embargo applied",
        ),
        "training_only_transformation": _bool(
            checks.get("training_only_transformation"),
            name="training-only transformation",
        ),
        "contract_identity_discontinuities_terminalized": _bool(
            checks.get("contract_identity_discontinuities_terminalized"),
            name="contract identity discontinuity terminalization",
        ),
        "roll_discontinuities_terminalized": _bool(
            checks.get("roll_discontinuities_terminalized"),
            name="roll discontinuity terminalization",
        ),
        "all_incomplete_sessions_terminalized": _bool(
            checks.get("all_incomplete_sessions_terminalized"),
            name="incomplete-session terminalization",
        ),
        "complete_required_metrics": _bool(
            checks.get("complete_required_metrics"),
            name="complete required metrics",
        ),
        "promotion_path_computable": _bool(
            checks.get("promotion_path_computable"),
            name="promotion path computable",
        ),
    }

    failed: list[str] = []
    if complete_training < minimum_training_sessions:
        failed.append("MINIMUM_COMPLETE_TRAINING_SESSIONS")
    if feature_training < minimum_training_sessions:
        failed.append("MINIMUM_FEATURE_COMPLETE_TRAINING_SESSIONS")
    if transform_training < minimum_training_sessions:
        failed.append("MINIMUM_TRANSFORMATION_READY_TRAINING_SESSIONS")
    if expected_evaluation < minimum_evaluation_sessions:
        failed.append("MINIMUM_EXPECTED_EVALUATION_SESSIONS")
    if feature_evaluation < minimum_evaluation_sessions:
        failed.append("MINIMUM_FEATURE_COMPLETE_EVALUATION_SESSIONS")
    if terminal_evaluation != expected_evaluation:
        failed.append("TERMINAL_EVALUATION_SESSION_COVERAGE")
    if candidate_paths != candidate_selected:
        failed.append("CANDIDATE_SELECTED_PATH_COVERAGE")
    if not candidate_risk_terminal:
        failed.append("SCENARIO_SPECIFIC_RISK_TERMINALIZATION")
    for baseline_name, baseline in baseline_results.items():
        if baseline["terminal_sessions"] != baseline["expected_sessions"]:
            failed.append("MANDATORY_BASELINE_TERMINAL_UNIVERSE_COVERAGE")
        if baseline["schedule_independently_derived"] is not True:
            failed.append("MANDATORY_BASELINE_INDEPENDENT_SCHEDULING")
        if baseline["flat_no_trade"] is True:
            if (
                baseline["selected_sessions"] != 0
                or baseline["selected_path_complete_sessions"] != 0
                or any(
                    any(disposition.values())
                    for disposition in baseline["scenario_risk_dispositions"].values()  # type: ignore[union-attr]
                )
            ):
                failed.append("FLAT_BASELINE_MUST_MAKE_ZERO_TRADES")
        else:
            if baseline["selected_path_complete_sessions"] != baseline["selected_sessions"]:
                failed.append("MANDATORY_BASELINE_SELECTED_PATH_COVERAGE")
            if baseline_risk_terminal_by_name[baseline_name] is not True:
                failed.append("MANDATORY_BASELINE_SCENARIO_RISK_TERMINALIZATION")
    if purge_minutes < minimum_purge_minutes or not gate_checks["purge_applied"]:
        failed.append("PURGE_REQUIREMENT")
    if (
        embargo_sessions < minimum_embargo_sessions
        or not gate_checks["embargo_applied"]
    ):
        failed.append("EMBARGO_REQUIREMENT")
    for check, passed in gate_checks.items():
        if not passed and check not in {"purge_applied", "embargo_applied"}:
            failed.append(check.upper())

    return {
        "fold_id": fold_id,
        "market": market,
        "role": role,
        "counts": {
            "expected_training_sessions": expected_training,
            "complete_training_sessions": complete_training,
            "feature_complete_training_sessions": feature_training,
            "transformation_ready_training_sessions": transform_training,
            "expected_evaluation_sessions": expected_evaluation,
            "feature_complete_evaluation_sessions": feature_evaluation,
            "terminal_evaluation_sessions": terminal_evaluation,
            "execution_path_complete_evaluation_sessions": execution_complete_evaluation,
            "candidate_selected_sessions": candidate_selected,
            "candidate_selected_path_complete_sessions": candidate_paths,
            "scenario_risk_dispositions": risk_dispositions,
            "purge_minutes": purge_minutes,
            "embargo_sessions": embargo_sessions,
        },
        "checks": gate_checks,
        "baseline_universe_readiness": dict(sorted(baseline_results.items())),
        "exclusion_reasons": dict(sorted(exclusion_counts.items())),
        "market_year_breakdown": dict(sorted(market_year_breakdown.items())),
        "failed_gates": sorted(set(failed)),
        "status": "PASS" if not failed else "FAIL",
    }


def build_fold_readiness_certificate(
    *, trial_family: str, protocol_id: str, source_bindings: Mapping[str, str],
    fold_evidence: Sequence[Mapping[str, object]],
    required_markets: Sequence[str], required_baselines: Sequence[str],
    required_cost_scenarios: Sequence[str],
    required_outer_fold_ids: Sequence[str], required_nested_fold_ids: Sequence[str],
    expected_outer_folds: int, expected_nested_folds: int,
    minimum_training_sessions: int, minimum_evaluation_sessions: int,
    minimum_purge_minutes: int, minimum_embargo_sessions: int,
    evidence_class: str, historical_rows_opened: bool,
) -> dict[str, object]:
    """Build a deterministic certificate; only row-certified PASS is authoritative."""

    if not isinstance(trial_family, str) or not trial_family:
        raise IntegrityError("trial family is absent")
    if not isinstance(protocol_id, str) or not protocol_id:
        raise IntegrityError("protocol identity is absent")
    markets = _strings(required_markets, name="required markets")
    baselines = _strings(required_baselines, name="required baselines")
    scenarios = _strings(required_cost_scenarios, name="required cost scenarios")
    outer_fold_ids = _fold_ids(required_outer_fold_ids, name="required outer fold IDs")
    nested_fold_ids = _fold_ids(required_nested_fold_ids, name="required nested fold IDs")
    bindings = _binding_map(source_bindings, name="source bindings")
    for name, value in (
        ("expected outer folds", expected_outer_folds),
        ("expected nested folds", expected_nested_folds),
        ("minimum training sessions", minimum_training_sessions),
        ("minimum evaluation sessions", minimum_evaluation_sessions),
        ("minimum purge minutes", minimum_purge_minutes),
        ("minimum embargo sessions", minimum_embargo_sessions),
    ):
        _integer(value, name=name)
    if minimum_training_sessions <= 0 or minimum_evaluation_sessions <= 0:
        raise IntegrityError("minimum fold sample requirements must be positive")
    if (
        len(outer_fold_ids) != expected_outer_folds
        or len(nested_fold_ids) != expected_nested_folds
    ):
        raise IntegrityError("required fold identities do not match locked fold counts")
    if evidence_class not in {ROW_CERTIFIED, SYNTHETIC_ONLY}:
        raise IntegrityError("unknown readiness evidence class")
    rows_opened = _bool(historical_rows_opened, name="historical rows opened")
    if rows_opened != (evidence_class == ROW_CERTIFIED):
        raise IntegrityError("evidence class and row-access declaration disagree")

    results = [
        _fold_result(
            item,
            required_baselines=baselines,
            required_cost_scenarios=scenarios,
            minimum_training_sessions=minimum_training_sessions,
            minimum_evaluation_sessions=minimum_evaluation_sessions,
            minimum_purge_minutes=minimum_purge_minutes,
            minimum_embargo_sessions=minimum_embargo_sessions,
        )
        for item in fold_evidence
    ]
    identities = {(item["role"], item["fold_id"], item["market"]) for item in results}
    if len(identities) != len(results):
        raise IntegrityError("fold-market readiness evidence is duplicated")
    expected_outer = expected_outer_folds * len(markets)
    expected_nested = expected_nested_folds * len(markets)
    observed_outer = sum(item["role"] == "OUTER" for item in results)
    observed_nested = sum(item["role"] == "NESTED" for item in results)
    def role_topology_complete(role: str, expected_ids: tuple[str, ...]) -> bool:
        fold_markets: dict[str, set[str]] = {}
        for item in results:
            if item["role"] == role:
                fold_markets.setdefault(str(item["fold_id"]), set()).add(str(item["market"]))
        return (
            set(fold_markets) == set(expected_ids)
            and all(value == set(markets) for value in fold_markets.values())
        )

    outer_topology_complete = role_topology_complete("OUTER", outer_fold_ids)
    nested_topology_complete = role_topology_complete("NESTED", nested_fold_ids)
    coverage_complete = outer_topology_complete and nested_topology_complete
    all_fold_gates_pass = bool(results) and all(item["status"] == "PASS" for item in results)
    authoritative = evidence_class == ROW_CERTIFIED and rows_opened
    overall = "PASS" if coverage_complete and all_fold_gates_pass and authoritative else "FAIL"
    failed_global: list[str] = []
    if not coverage_complete:
        failed_global.append("EXACT_FOLD_MARKET_COVERAGE")
    if not all_fold_gates_pass:
        failed_global.append("FOLD_READINESS")
    if not authoritative:
        failed_global.append("AUTHORIZED_ROW_CERTIFICATION_REQUIRED")

    core: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "trial_family": trial_family,
        "protocol_id": protocol_id,
        "evidence_class": evidence_class,
        "historical_rows_opened": rows_opened,
        "synthetic_evidence_proves_alpha": False,
        "source_bindings": bindings,
        "requirements": {
            "required_markets": list(markets),
            "required_baselines": list(baselines),
            "required_cost_scenarios": list(scenarios),
            "required_outer_fold_ids": list(outer_fold_ids),
            "required_nested_fold_ids": list(nested_fold_ids),
            "expected_outer_folds": expected_outer_folds,
            "expected_nested_folds": expected_nested_folds,
            "minimum_training_sessions": minimum_training_sessions,
            "minimum_evaluation_sessions": minimum_evaluation_sessions,
            "minimum_purge_minutes": minimum_purge_minutes,
            "minimum_embargo_sessions": minimum_embargo_sessions,
        },
        "coverage": {
            "expected_outer_fold_markets": expected_outer,
            "observed_outer_fold_markets": observed_outer,
            "expected_nested_fold_markets": expected_nested,
            "observed_nested_fold_markets": observed_nested,
            "outer_fold_market_topology_complete": outer_topology_complete,
            "nested_fold_market_topology_complete": nested_topology_complete,
        },
        "fold_market_results": sorted(
            results, key=lambda item: (str(item["role"]), str(item["fold_id"]), str(item["market"])),
        ),
        "failed_global_gates": failed_global,
        "registration_allowed": overall == "PASS",
        "historical_execution_authorization_allowed": overall == "PASS",
        "overall_decision": overall,
    }
    return {**core, "certificate_id": sha256_json(core)}


def validate_fold_readiness_certificate(
    certificate: Mapping[str, object], *, root: Path,
) -> dict[str, object]:
    core = dict(certificate)
    certificate_id = core.pop("certificate_id", None)
    if certificate_id != sha256_json(core) or core.get("schema_version") != SCHEMA_VERSION:
        raise IntegrityError("fold readiness certificate identity is invalid")
    requirements = core.get("requirements")
    evidence = core.get("fold_market_results")
    if (
        not isinstance(requirements, Mapping)
        or not isinstance(evidence, Sequence)
        or isinstance(evidence, (str, bytes))
        or any(not isinstance(item, Mapping) for item in evidence)
    ):
        raise IntegrityError("fold readiness certificate semantics are malformed")
    bindings = _binding_map(core.get("source_bindings"), name="certificate source bindings")
    for relative, expected_digest in bindings.items():
        assert_no_linklike_ancestors(root / relative)
        source = contained_path(root, relative)
        if sha256_file(source) != expected_digest:
            raise IntegrityError("fold readiness certificate source binding changed")
    rebuilt = build_fold_readiness_certificate(
        trial_family=str(core.get("trial_family", "")),
        protocol_id=str(core.get("protocol_id", "")),
        source_bindings=bindings,
        fold_evidence=evidence,  # type: ignore[arg-type]
        required_markets=requirements.get("required_markets"),  # type: ignore[arg-type]
        required_baselines=requirements.get("required_baselines"),  # type: ignore[arg-type]
        required_cost_scenarios=requirements.get("required_cost_scenarios"),  # type: ignore[arg-type]
        required_outer_fold_ids=requirements.get("required_outer_fold_ids"),  # type: ignore[arg-type]
        required_nested_fold_ids=requirements.get("required_nested_fold_ids"),  # type: ignore[arg-type]
        expected_outer_folds=requirements.get("expected_outer_folds"),  # type: ignore[arg-type]
        expected_nested_folds=requirements.get("expected_nested_folds"),  # type: ignore[arg-type]
        minimum_training_sessions=requirements.get("minimum_training_sessions"),  # type: ignore[arg-type]
        minimum_evaluation_sessions=requirements.get("minimum_evaluation_sessions"),  # type: ignore[arg-type]
        minimum_purge_minutes=requirements.get("minimum_purge_minutes"),  # type: ignore[arg-type]
        minimum_embargo_sessions=requirements.get("minimum_embargo_sessions"),  # type: ignore[arg-type]
        evidence_class=str(core.get("evidence_class", "")),
        historical_rows_opened=core.get("historical_rows_opened"),  # type: ignore[arg-type]
    )
    if rebuilt != dict(certificate):
        raise IntegrityError("fold readiness certificate semantics do not reproduce")
    return dict(certificate)


def require_registration_ready(
    certificate: Mapping[str, object], *, root: Path,
) -> str:
    validated = validate_fold_readiness_certificate(
        certificate, root=root,
    )
    if (
        validated.get("overall_decision") != "PASS"
        or validated.get("registration_allowed") is not True
        or validated.get("evidence_class") != ROW_CERTIFIED
    ):
        raise UnauthorizedOperation("trial registration lacks a passing row-certified fold gate")
    return str(validated["certificate_id"])


def load_registration_ready_certificate(
    *, root: Path, certificate_evidence_path: Path,
) -> tuple[dict[str, object], str, str]:
    """Load the exact row-certified PASS before any registration write."""

    certificate, relative, evidence_sha256 = _load_certificate_evidence(
        root=root, path=certificate_evidence_path,
    )
    require_registration_ready(certificate, root=root)
    return certificate, relative, evidence_sha256


def require_execution_ready_before_claim(
    *, root: Path, registration_path: Path,
    expected_registration_sha256: str,
    claim_authorization: Callable[[], object],
) -> object:
    """Reload the exact registration and certificate before a one-use claim."""

    registration, validated = _load_bound_registration_and_certificate(
        root=root,
        registration_path=registration_path,
        expected_registration_sha256=expected_registration_sha256,
    )
    if (
        validated.get("overall_decision") != "PASS"
        or validated.get("historical_execution_authorization_allowed") is not True
        or validated.get("evidence_class") != ROW_CERTIFIED
    ):
        raise UnauthorizedOperation("historical execution lacks a passing fold gate")
    return claim_authorization()


def load_execution_ready_registration(
    *, root: Path, registration_path: Path,
    expected_registration_sha256: str,
) -> tuple[dict[str, object], dict[str, object]]:
    """Return exact registered context only when its row gate still passes."""

    registration, validated = _load_bound_registration_and_certificate(
        root=root,
        registration_path=registration_path,
        expected_registration_sha256=expected_registration_sha256,
    )
    if (
        validated.get("overall_decision") != "PASS"
        or validated.get("historical_execution_authorization_allowed") is not True
        or validated.get("evidence_class") != ROW_CERTIFIED
    ):
        raise UnauthorizedOperation("historical execution lacks a passing fold gate")
    return registration, validated


def create_registration_after_gate(
    *, root: Path, path: Path, payload: Mapping[str, object],
    certificate_evidence_path: Path,
) -> None:
    """Create a registration bound to one immutable readiness evidence file."""

    certificate, evidence_relative, evidence_sha256 = _load_certificate_evidence(
        root=root, path=certificate_evidence_path,
    )
    require_registration_ready(certificate, root=root)
    _validate_registration_binding(
        registration=payload,
        certificate=certificate,
        evidence_relative=evidence_relative,
        evidence_sha256=evidence_sha256,
    )
    registry_root = (root / "state" / "trial_registry").resolve(strict=False)
    try:
        path.resolve(strict=False).relative_to(registry_root)
    except ValueError as exc:
        raise UnauthorizedOperation("trial registration path leaves the registry") from exc
    if path.suffix != ".json" or path.stem != payload.get("trial_id"):
        raise UnauthorizedOperation("trial registration path does not match its identity")
    assert_no_linklike_ancestors(path.parent)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(canonical_bytes(dict(payload)) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("trial registration already exists") from exc


def _load_json_bytes(
    path: Path, *, expected_sha256: str | None = None,
) -> tuple[dict[str, object], bytes]:
    try:
        assert_plain_file(path)
        raw = path.read_bytes()
    except (OSError, IntegrityError) as exc:
        raise IntegrityError("fold readiness bound artifact is invalid") from exc
    if expected_sha256 is not None and sha256(raw).hexdigest() != expected_sha256:
        raise IntegrityError("fold readiness evidence binding changed")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("fold readiness bound artifact is invalid") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError("fold readiness bound artifact is not canonical")
    return value, raw


def _load_certificate_evidence(
    *, root: Path, path: Path, expected_sha256: str | None = None,
) -> tuple[dict[str, object], str, str]:
    try:
        relative = path.resolve(strict=False).relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise UnauthorizedOperation("fold readiness evidence leaves the repository") from exc
    contained = contained_path(root, relative)
    assert_no_linklike_ancestors(contained)
    wrapper, raw = _load_json_bytes(contained, expected_sha256=expected_sha256)
    actual_sha256 = sha256(raw).hexdigest()
    candidate = wrapper.get("fold_readiness_certificate", wrapper)
    if not isinstance(candidate, Mapping):
        raise IntegrityError("fold readiness evidence lacks its certificate")
    validated = validate_fold_readiness_certificate(candidate, root=root)
    return validated, relative, actual_sha256


def _validate_registration_binding(
    *, registration: Mapping[str, object], certificate: Mapping[str, object],
    evidence_relative: str, evidence_sha256: str,
) -> None:
    core = dict(registration)
    trial_id = core.pop("trial_id", None)
    binding = registration.get("fold_readiness_binding")
    if (
        trial_id != sha256_json(core)
        or not isinstance(binding, Mapping)
        or set(binding) != {"evidence_path", "evidence_sha256", "certificate_id"}
        or binding.get("evidence_path") != evidence_relative
        or binding.get("evidence_sha256") != evidence_sha256
        or binding.get("certificate_id") != certificate.get("certificate_id")
        or registration.get("trial_family") != certificate.get("trial_family")
        or registration.get("protocol_id") != certificate.get("protocol_id")
    ):
        raise IntegrityError("trial registration is not bound to this fold certificate")


def _load_bound_registration_and_certificate(
    *, root: Path, registration_path: Path,
    expected_registration_sha256: str,
) -> tuple[dict[str, object], dict[str, object]]:
    registry_root = (root / "state" / "trial_registry").resolve(strict=False)
    try:
        registration_path.resolve(strict=False).relative_to(registry_root)
    except ValueError as exc:
        raise UnauthorizedOperation("trial registration path leaves the registry") from exc
    registration, raw = _load_json_bytes(registration_path)
    if sha256(raw).hexdigest() != expected_registration_sha256:
        raise IntegrityError("trial registration binding changed")
    if registration_path.suffix != ".json" or registration_path.stem != registration.get("trial_id"):
        raise IntegrityError("trial registration path does not match its identity")
    binding = registration.get("fold_readiness_binding")
    if not isinstance(binding, Mapping):
        raise IntegrityError("trial registration lacks a fold readiness binding")
    evidence_relative = binding.get("evidence_path")
    evidence_sha256 = binding.get("evidence_sha256")
    if not isinstance(evidence_relative, str) or not isinstance(evidence_sha256, str):
        raise IntegrityError("trial registration fold readiness binding is malformed")
    certificate, actual_relative, actual_sha256 = _load_certificate_evidence(
        root=root,
        path=contained_path(root, evidence_relative),
        expected_sha256=evidence_sha256,
    )
    _validate_registration_binding(
        registration=registration,
        certificate=certificate,
        evidence_relative=actual_relative,
        evidence_sha256=actual_sha256,
    )
    return registration, certificate
