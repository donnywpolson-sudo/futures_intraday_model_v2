"""V5 successor after complete exclusion and pilot-certificate synthetic replay."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from . import alpha_ladder_limit_readiness_v4 as v4
from .alpha_ladder_combined_readiness import _read_canonical
from .boundary import OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .errors import IntegrityError
from .research_gateway_policy import ALPHA_LADDER_READINESS_CENSUS_OPERATION


PLAN_PATH = Path("configs/alpha_ladder_limit_readiness_census_v5_plan.json")
OUTPUT_ROOT = Path("state/unpublished_evidence/alpha_ladder_limit_readiness_v5")
RUNNER_PATH = Path("scripts/run_alpha_ladder_limit_readiness_census_v5.py")
V4_FAILURE_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_limit_readiness_v4_attempts/"
    "780f0d47fac767195f63bf3d86b24c540e4a6561d235986e196ac3f92c55cadc/failure.json"
)
V4_FAILURE_ID = "70c752a8cd69a2642a8730548e18606d5dac8d01ac9293ead7a0b72defa93103"


def build_plan(*, root: Path) -> dict[str, object]:
    predecessor = v4.v3.v2.v1.load_plan(root=root)
    failure = _read_canonical(root / V4_FAILURE_PATH, name="V4 attempt failure")
    if failure.get("failure_id") != V4_FAILURE_ID or failure.get("attempt_consumed") is not True:
        raise IntegrityError("V4 failure evidence changed")
    core = {key: value for key, value in predecessor.items() if key != "plan_id"}
    core.update({
        "schema_version": "alpha_ladder_limit_readiness_census_plan/5.0.0",
        "state": "PREPARED_FULL_CERTIFICATE_REPLAYED_SUCCESSOR_NOT_EXECUTED_ONE_ATTEMPT_ZERO_RETRIES",
        "output_root": OUTPUT_ROOT.as_posix(),
        "predecessor_plan_id": predecessor["plan_id"],
        "consumed_v4_plan_id": "88e716e3a384fd23e88e947aea929a22c5091ec61ac475072b78f905f8cbf296",
        "consumed_v4_failure_id": V4_FAILURE_ID,
    })
    bindings = dict(core["bindings"])
    bindings.update({
        v4.PLAN_PATH.as_posix(): "d372907aaa865110bea2d3968a19222a4ae1fc69cada7347a8fd138d7840dc9f",
        V4_FAILURE_PATH.as_posix(): sha256_file(root / V4_FAILURE_PATH),
        "src/futures_rebuild/alpha_ladder_limit_readiness_v2.py": sha256_file(
            root / "src/futures_rebuild/alpha_ladder_limit_readiness_v2.py"),
        "src/futures_rebuild/alpha_ladder_limit_readiness_v3.py": sha256_file(
            root / "src/futures_rebuild/alpha_ladder_limit_readiness_v3.py"),
        "src/futures_rebuild/alpha_ladder_limit_readiness_v4.py": sha256_file(
            root / "src/futures_rebuild/alpha_ladder_limit_readiness_v4.py"),
        "src/futures_rebuild/alpha_ladder_limit_readiness_v5.py": sha256_file(Path(__file__)),
        RUNNER_PATH.as_posix(): sha256_file(root / RUNNER_PATH),
    })
    core["bindings"] = dict(sorted(bindings.items()))
    return {**core, "plan_id": sha256_json(core)}


def load_plan(*, root: Path) -> dict[str, object]:
    plan = _read_canonical(root / PLAN_PATH, name="limit readiness V5 plan")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    bindings = plan.get("bindings")
    if (
        plan.get("plan_id") != sha256_json(core)
        or plan.get("schema_version") != "alpha_ladder_limit_readiness_census_plan/5.0.0"
        or plan.get("consumed_v4_failure_id") != V4_FAILURE_ID
        or plan.get("output_root") != OUTPUT_ROOT.as_posix()
        or not isinstance(bindings, Mapping)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("limit readiness V5 plan drifted")
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
    original_plan, original_output, original_load = v4.PLAN_PATH, v4.OUTPUT_ROOT, v4.load_plan
    try:
        v4.PLAN_PATH = PLAN_PATH
        v4.OUTPUT_ROOT = OUTPUT_ROOT
        v4.load_plan = lambda *, root: plan
        return v4.execute_once(root=root, boundary=boundary, receipt=receipt)
    finally:
        v4.PLAN_PATH, v4.OUTPUT_ROOT, v4.load_plan = original_plan, original_output, original_load
