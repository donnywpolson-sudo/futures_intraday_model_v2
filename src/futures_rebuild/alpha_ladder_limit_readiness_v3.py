"""Transition-stable V3 host successor for counted Alpha readiness."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from . import alpha_ladder_limit_readiness_v2 as v2
from .alpha_ladder_combined_readiness import _read_canonical
from .boundary import OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .errors import IntegrityError
from .research_gateway_policy import ALPHA_LADDER_READINESS_CENSUS_OPERATION


PLAN_PATH = Path("configs/alpha_ladder_limit_readiness_census_v3_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_ladder_limit_readiness_v3")
RUNNER_PATH = Path("scripts/run_alpha_ladder_limit_readiness_census_v3.py")
INVALID_V2_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_limit_readiness_v2_invalid_preparation/"
    "dc2fa792527fdf0a7fc30f8f04959a245d8a97e98249c2a67e82ee10aad60bee/record.json"
)
INVALID_V2_ID = "dc2fa792527fdf0a7fc30f8f04959a245d8a97e98249c2a67e82ee10aad60bee"


def build_plan(*, root: Path) -> dict[str, object]:
    predecessor = v2.v1.load_plan(root=root)
    invalid = _read_canonical(root / INVALID_V2_PATH, name="invalid V2 preparation")
    if invalid.get("record_id") != INVALID_V2_ID or invalid.get("execution_occurred") is not False:
        raise IntegrityError("invalid V2 preparation evidence changed")
    core = {key: value for key, value in predecessor.items() if key != "plan_id"}
    core.update({
        "schema_version": "alpha_ladder_limit_readiness_census_plan/3.0.0",
        "state": "PREPARED_TRANSITION_STABLE_SUCCESSOR_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES",
        "output_root": OUTPUT_ROOT.as_posix(),
        "predecessor_plan_id": predecessor["plan_id"],
        "predecessor_attempt_consumed": True,
        "invalid_intermediate_plan_id": "f72fb5493ea9ddc7fbbf205fd02b38e3da3d8726477f133e921f17b59283156b",
        "invalid_intermediate_record_id": INVALID_V2_ID,
    })
    bindings = dict(core["bindings"])
    bindings.update({
        v2.PLAN_PATH.as_posix(): "60c38c7c2faa5b3a44280d22a4a8ba7c94324f024d587b532d9f64d10fdda544",
        INVALID_V2_PATH.as_posix(): sha256_file(root / INVALID_V2_PATH),
        "src/futures_rebuild/alpha_ladder_limit_readiness_v2.py": sha256_file(
            root / "src/futures_rebuild/alpha_ladder_limit_readiness_v2.py"),
        "src/futures_rebuild/alpha_ladder_limit_readiness_v3.py": sha256_file(Path(__file__)),
        RUNNER_PATH.as_posix(): sha256_file(root / RUNNER_PATH),
    })
    core["bindings"] = dict(sorted(bindings.items()))
    return {**core, "plan_id": sha256_json(core)}


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="limit readiness V3 plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    bindings = plan.get("bindings")
    if (
        plan.get("plan_id") != sha256_json(core)
        or plan.get("schema_version") != "alpha_ladder_limit_readiness_census_plan/3.0.0"
        or plan.get("invalid_intermediate_record_id") != INVALID_V2_ID
        or plan.get("output_root") != OUTPUT_ROOT.as_posix()
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("limit readiness V3 plan drifted")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    limits = plan["execution_limits"]
    assert isinstance(limits, Mapping)
    return {
        "mechanism_id": str(plan["mechanism_id"]), "period": "2018,2019,2020,2021,2022",
        "markets": "ES,CL,ZN,6E", "checkpoint": "10:00",
        "purpose": "ALPHA_RESTING_LIMIT_PILOT_AND_TIER1_READINESS_ONLY",
        "output_root": OUTPUT_ROOT.as_posix(), "maximum_attempts": "1",
        "maximum_retries": "0", "maximum_workers": "4",
        "worker_deadline_seconds": str(limits["worker_deadline_seconds"]),
        "maximum_runtime_seconds": str(limits["maximum_runtime_seconds"]),
        "maximum_external_cost_usd": "0", "returns": "false", "model_fit": "false",
        "prediction_generation": "false", "performance_evaluation": "false",
        "registration": "false", "trial_execution": "false",
        "provider_network_access": "false", "holdout_2025_access": "false",
        "active_data_mutation": "false", "trading": "false",
        "approval_command": ALPHA_LADDER_READINESS_CENSUS_OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def execute_once(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> Mapping[str, object]:
    plan = load_plan(root=root)
    original_plan = v2.PLAN_PATH
    original_output = v2.OUTPUT_ROOT
    original_load = v2.load_plan
    original_scope = v2.required_scope
    try:
        v2.PLAN_PATH = PLAN_PATH
        v2.OUTPUT_ROOT = OUTPUT_ROOT
        v2.load_plan = lambda *, root: plan
        v2.required_scope = required_scope
        return v2.execute_once(root=root, boundary=boundary, receipt=receipt)
    finally:
        v2.PLAN_PATH = original_plan
        v2.OUTPUT_ROOT = original_output
        v2.load_plan = original_load
        v2.required_scope = original_scope
