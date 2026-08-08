"""Additive host successor for the consumed limit-readiness census attempt.

V1 is preserved byte-for-byte.  This successor removes two descriptive fields
that V1 incorrectly added to the exact baseline-readiness schema before handing
the evidence to the universal preexecution certificate validator.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from pathlib import Path

from . import alpha_ladder_limit_readiness as v1
from .alpha_ladder_combined_readiness import _read_canonical
from .boundary import OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .errors import IntegrityError


PLAN_PATH = Path("configs/alpha_ladder_limit_readiness_census_v2_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_ladder_limit_readiness_v2")
RUNNER_PATH = Path("scripts/run_alpha_ladder_limit_readiness_census_v2.py")
PREDECESSOR_PLAN_PATH = v1.PLAN_PATH
PREDECESSOR_PLAN_SHA256 = "c34cb337a62159613f17176a61032dd2630b552ff2637f9e578f994484716469"
FAILURE_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_limit_readiness_attempts/"
    "d28225ad335cfd2754e95bf3e6283f725a1e6d5565fbcd6bea86a68937830c46/failure.json"
)
FAILURE_ID = "5ef9b253bc423c4a3eebb3e98ea6d9b076b24c1b452762dbfec613d6ad5042c7"
_V1_REQUIRED_SCOPE = v1.required_scope
_V1_FOLD_EVIDENCE = v1._fold_evidence


def _fold_evidence(*, market, fold, rows_by_session, risk_by_session):
    evidence = _V1_FOLD_EVIDENCE(
        market=market, fold=fold, rows_by_session=rows_by_session,
        risk_by_session=risk_by_session,
    )
    for raw in evidence["baseline_universe_readiness"].values():
        raw.pop("readiness_universe")
        raw.pop("candidate_schedule_reused")
    cost_ticks = rows_by_session["__cost_ticks__"]
    bars = {key: value for key, value in rows_by_session.items() if key != "__cost_ticks__"}
    training = v1._session_results(fold["training_sessions"], bars, cost_ticks)
    evaluation = v1._session_results(fold["evaluation_sessions"], bars, cost_ticks)

    def exclusions(items, role):
        counts = Counter()
        for _session, item in items:
            if item.feature_complete and item.path_complete:
                continue
            if not item.feature_complete:
                reason = item.dispositions[0]
            else:
                failures = tuple(value for value in item.dispositions
                                 if "MISSING" in value or "CHANGING" in value)
                reason = failures[0] if failures else "EXECUTION_PATH_INCOMPLETE"
            counts[f"{role}__{reason}"] += 1
        return dict(counts)

    evidence["exclusion_reasons"] = {
        **exclusions(training, "TRAINING"), **exclusions(evaluation, "EVALUATION")}
    for year, year_evidence in evidence["market_year_breakdown"].items():
        year_training = [item for item in training if item[0].startswith(year)]
        year_evaluation = [item for item in evaluation if item[0].startswith(year)]
        year_evidence["exclusion_reasons"] = {
            **exclusions(year_training, "TRAINING"),
            **exclusions(year_evaluation, "EVALUATION"),
        }
    return evidence


def build_plan(*, root: Path) -> dict[str, object]:
    predecessor = v1.load_plan(root=root)
    if (
        predecessor.get("plan_id") != "afa24b35830e2950d31645ab4c766155a35253afbc7c7d910c363d43ffa924ef"
        or sha256_file(root / PREDECESSOR_PLAN_PATH) != PREDECESSOR_PLAN_SHA256
    ):
        raise IntegrityError("consumed predecessor plan changed")
    failure = _read_canonical(root / FAILURE_PATH, name="V1 attempt failure")
    if (
        failure.get("failure_id") != FAILURE_ID
        or failure.get("attempt_consumed") is not True
        or failure.get("retry_authorized") is not False
        or failure.get("failure_class") != "IMPLEMENTATION_BASELINE_EVIDENCE_SCHEMA_MISMATCH"
    ):
        raise IntegrityError("V1 attempt failure evidence changed")
    core = {key: value for key, value in predecessor.items() if key != "plan_id"}
    core.update({
        "schema_version": "alpha_ladder_limit_readiness_census_plan/2.0.0",
        "state": "PREPARED_HOST_SUCCESSOR_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES",
        "output_root": OUTPUT_ROOT.as_posix(),
        "predecessor_plan_id": predecessor["plan_id"],
        "predecessor_attempt_consumed": True,
        "predecessor_failure_id": FAILURE_ID,
    })
    bindings = dict(core["bindings"])
    bindings.update({
        PREDECESSOR_PLAN_PATH.as_posix(): PREDECESSOR_PLAN_SHA256,
        FAILURE_PATH.as_posix(): sha256_file(root / FAILURE_PATH),
        "src/futures_rebuild/alpha_ladder_limit_readiness_v2.py": sha256_file(Path(__file__)),
        RUNNER_PATH.as_posix(): sha256_file(root / RUNNER_PATH),
    })
    core["bindings"] = dict(sorted(bindings.items()))
    return {**core, "plan_id": sha256_json(core)}


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="limit readiness V2 plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    bindings = plan.get("bindings")
    if (
        plan.get("plan_id") != sha256_json(core)
        or plan.get("schema_version") != "alpha_ladder_limit_readiness_census_plan/2.0.0"
        or plan.get("predecessor_plan_id")
        != "afa24b35830e2950d31645ab4c766155a35253afbc7c7d910c363d43ffa924ef"
        or plan.get("predecessor_attempt_consumed") is not True
        or plan.get("predecessor_failure_id") != FAILURE_ID
        or plan.get("output_root") != OUTPUT_ROOT.as_posix()
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("limit readiness V2 plan drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    scope = _V1_REQUIRED_SCOPE(root=root, plan=plan)
    scope["output_root"] = OUTPUT_ROOT.as_posix()
    scope["approval_plan_sha256"] = sha256_file(root / PLAN_PATH)
    return scope


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> Mapping[str, object]:
    plan = load_plan(root=root)
    original_plan = v1.PLAN_PATH
    original_output = v1.OUTPUT_ROOT
    original_fold_evidence = v1._fold_evidence
    original_load_plan = v1.load_plan
    original_required_scope = v1.required_scope
    try:
        v1.PLAN_PATH = PLAN_PATH
        v1.OUTPUT_ROOT = OUTPUT_ROOT
        v1._fold_evidence = _fold_evidence
        v1.load_plan = lambda *, root: plan
        v1.required_scope = required_scope
        return v1.execute_once(root=root, boundary=boundary, receipt=receipt)
    finally:
        v1.PLAN_PATH = original_plan
        v1.OUTPUT_ROOT = original_output
        v1._fold_evidence = original_fold_evidence
        v1.load_plan = original_load_plan
        v1.required_scope = original_required_scope
