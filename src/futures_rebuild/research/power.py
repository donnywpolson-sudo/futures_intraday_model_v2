"""Training-only HAC-LRV minimum detectable effect planning."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from math import ceil
from statistics import NormalDist

import numpy as np

from ..canonical import sha256_json
from .contracts import (
    ResearchContractError,
    array_sha256,
    explicit_int,
    explicit_real,
    finite_float64,
)
from .hac import newey_west_mean


_NORMAL = NormalDist()


@dataclass(frozen=True)
class PowerPlan:
    training_observations: int
    planned_evaluation_observations: int
    hac_lag: int
    long_run_variance: float
    variance_inflation: float
    alpha: float
    target_power: float
    alternative: str
    minimum_detectable_mean: float
    economic_mean_hurdle: float
    required_evaluation_observations: int
    adequately_powered: bool
    status: str


def training_only_mde(
    training_differentials: np.ndarray,
    *,
    partition_role: str,
    hac_lag: int,
    planned_evaluation_observations: int,
    alpha: float,
    target_power: float,
    alternative: str,
    economic_mean_hurdle: float,
    variance_inflation: float = 1.0,
) -> PowerPlan:
    values = finite_float64(training_differentials, name="training_differentials", ndim=1)
    if partition_role != "TRAIN":
        raise ResearchContractError("MDE inputs must be TRAIN partition data")
    planned = explicit_int(
        planned_evaluation_observations, name="planned_evaluation_observations"
    )
    checked_alpha = explicit_real(alpha, name="alpha")
    checked_power = explicit_real(target_power, name="target_power")
    hurdle = explicit_real(economic_mean_hurdle, name="economic_mean_hurdle")
    inflation = explicit_real(variance_inflation, name="variance_inflation")
    if planned < 1 or not (0.0 < checked_alpha < 1.0) or not (
        0.0 < checked_power < 1.0
    ):
        raise ResearchContractError("power-plan counts/probabilities are invalid")
    if alternative not in {"greater", "two-sided"}:
        raise ResearchContractError("alternative must be greater or two-sided")
    if hurdle <= 0.0 or inflation < 1.0:
        raise ResearchContractError("hurdle must be positive and inflation at least one")
    hac = newey_west_mean(values, lag=hac_lag)
    if hac.status != "OK" or hac.long_run_variance <= 0.0:
        raise ResearchContractError("training LRV is degenerate")
    probability = 1.0 - (
        checked_alpha if alternative == "greater" else checked_alpha / 2.0
    )
    critical = _NORMAL.inv_cdf(probability)
    power_quantile = _NORMAL.inv_cdf(checked_power)
    if critical <= 0.0 or power_quantile <= 0.0:
        raise ResearchContractError("requires alpha < .5 and target power > .5")
    z_sum = critical + power_quantile
    inflated_lrv = hac.long_run_variance * inflation
    mde = z_sum * np.sqrt(inflated_lrv / planned)
    required_float = inflated_lrv * (z_sum / hurdle) ** 2
    if not all(np.isfinite(item) for item in (inflated_lrv, mde, required_float)):
        raise ResearchContractError("power-plan arithmetic is non-finite")
    required = ceil(required_float)
    adequately_powered = bool(planned >= required and hurdle >= mde)
    return PowerPlan(
        len(values),
        planned,
        hac.lag,
        hac.long_run_variance,
        inflation,
        checked_alpha,
        checked_power,
        alternative,
        float(mde),
        hurdle,
        required,
        adequately_powered,
        "TRAINING_PLAN_ONLY",
    )


FINAL_252_POWER_SCHEMA = "final_252_power_plan/1.0.0"


def _digest(value: object, *, name: str) -> str:
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ResearchContractError(f"{name} must be a SHA-256 digest")
    return value


def build_final_252_power_plan(
    training_differentials: np.ndarray,
    *,
    estimand: str,
    mechanism_sha256: str,
    config_sha256: str,
    session_manifest_id: str,
    session_manifest_sha256: str,
    ordered_session_sha256: str,
    hac_lag: int,
    alpha: float,
    target_power: float,
    alternative: str,
    economic_mean_hurdle: float,
    variance_inflation: float = 1.0,
) -> dict[str, object]:
    """Bind a training-only MDE calculation to the sealed 252-session design."""

    values = finite_float64(
        training_differentials, name="training_differentials", ndim=1
    )
    if type(estimand) is not str or not estimand:
        raise ResearchContractError("Final-252 estimand is required")
    for name, value in (
        ("mechanism SHA-256", mechanism_sha256),
        ("config SHA-256", config_sha256),
        ("session manifest identity", session_manifest_id),
        ("session manifest SHA-256", session_manifest_sha256),
        ("ordered session SHA-256", ordered_session_sha256),
    ):
        _digest(value, name=name)
    checked_target_power = explicit_real(target_power, name="target_power")
    if checked_target_power < 0.8:
        raise ResearchContractError("Final-252 target power must be at least 0.80")
    plan = training_only_mde(
        values,
        partition_role="TRAIN",
        hac_lag=hac_lag,
        planned_evaluation_observations=252,
        alpha=alpha,
        target_power=checked_target_power,
        alternative=alternative,
        economic_mean_hurdle=economic_mean_hurdle,
        variance_inflation=variance_inflation,
    )
    status = (
        "READY_FOR_SEPARATE_FINAL_252_ACCESS_AUTHORITY"
        if plan.adequately_powered
        else "INCONCLUSIVE_DATA_OR_POWER"
    )
    core: dict[str, object] = {
        "schema_version": FINAL_252_POWER_SCHEMA,
        "estimand": estimand,
        "mechanism_sha256": mechanism_sha256,
        "config_sha256": config_sha256,
        "session_manifest_id": session_manifest_id,
        "session_manifest_sha256": session_manifest_sha256,
        "ordered_session_sha256": ordered_session_sha256,
        "planned_evaluation_observations": 252,
        "training_differentials_sha256": array_sha256(values),
        "training_observations": plan.training_observations,
        "hac_lag": plan.hac_lag,
        "long_run_variance": plan.long_run_variance,
        "variance_inflation": plan.variance_inflation,
        "alpha": plan.alpha,
        "target_power": plan.target_power,
        "alternative": plan.alternative,
        "minimum_detectable_mean": plan.minimum_detectable_mean,
        "economic_mean_hurdle": plan.economic_mean_hurdle,
        "required_evaluation_observations": plan.required_evaluation_observations,
        "adequately_powered": plan.adequately_powered,
        "status": status,
        "authority": {
            "holdout_access": False,
            "evaluation": False,
            "activation": False,
        },
    }
    return {**core, "power_plan_id": sha256_json(core)}


def validate_final_252_power_plan(
    payload: Mapping[str, object],
    *,
    mechanism_sha256: str,
    config_sha256: str,
    session_manifest_id: str,
    session_manifest_sha256: str,
    ordered_session_sha256: str,
    require_adequate: bool = True,
) -> dict[str, object]:
    """Fail closed on identity drift or an underpowered sealed evaluation."""

    required = {
        "schema_version",
        "estimand",
        "mechanism_sha256",
        "config_sha256",
        "session_manifest_id",
        "session_manifest_sha256",
        "ordered_session_sha256",
        "planned_evaluation_observations",
        "training_differentials_sha256",
        "training_observations",
        "hac_lag",
        "long_run_variance",
        "variance_inflation",
        "alpha",
        "target_power",
        "alternative",
        "minimum_detectable_mean",
        "economic_mean_hurdle",
        "required_evaluation_observations",
        "adequately_powered",
        "status",
        "authority",
        "power_plan_id",
    }
    if set(payload) != required:
        raise ResearchContractError("Final-252 power-plan schema is invalid")
    core = dict(payload)
    recorded = core.pop("power_plan_id", None)
    if (
        core.get("schema_version") != FINAL_252_POWER_SCHEMA
        or recorded != sha256_json(core)
        or core.get("mechanism_sha256") != mechanism_sha256
        or core.get("config_sha256") != config_sha256
        or core.get("session_manifest_id") != session_manifest_id
        or core.get("session_manifest_sha256") != session_manifest_sha256
        or core.get("ordered_session_sha256") != ordered_session_sha256
        or core.get("planned_evaluation_observations") != 252
        or core.get("authority") != {
            "holdout_access": False,
            "evaluation": False,
            "activation": False,
        }
    ):
        raise ResearchContractError("Final-252 power-plan binding is invalid")
    for name, value in (
        ("training differentials SHA-256", core.get("training_differentials_sha256")),
        ("mechanism SHA-256", mechanism_sha256),
        ("config SHA-256", config_sha256),
        ("session manifest identity", session_manifest_id),
        ("session manifest SHA-256", session_manifest_sha256),
        ("ordered session SHA-256", ordered_session_sha256),
    ):
        _digest(value, name=name)
    try:
        numeric = tuple(
            float(core[name])
            for name in (
                "long_run_variance",
                "variance_inflation",
                "alpha",
                "target_power",
                "minimum_detectable_mean",
                "economic_mean_hurdle",
            )
        )
    except (TypeError, ValueError) as exc:
        raise ResearchContractError("Final-252 power-plan values are invalid") from exc
    if (
        any(not np.isfinite(value) for value in numeric)
        or type(core.get("training_observations")) is not int
        or core["training_observations"] < 1
        or type(core.get("hac_lag")) is not int
        or core["hac_lag"] < 0
        or type(core.get("required_evaluation_observations")) is not int
        or core["required_evaluation_observations"] < 1
        or core["alternative"] not in {"greater", "two-sided"}
    ):
        raise ResearchContractError("Final-252 power-plan values are invalid")
    adequately_powered = core.get("adequately_powered") is True
    calculated_adequacy = bool(
        core["planned_evaluation_observations"]
        >= core["required_evaluation_observations"]
        and float(core["economic_mean_hurdle"])
        >= float(core["minimum_detectable_mean"])
    )
    expected_status = (
        "READY_FOR_SEPARATE_FINAL_252_ACCESS_AUTHORITY"
        if adequately_powered
        else "INCONCLUSIVE_DATA_OR_POWER"
    )
    if (
        adequately_powered != calculated_adequacy
        or core.get("status") != expected_status
        or float(core["target_power"]) < 0.8
    ):
        raise ResearchContractError("Final-252 power-plan decision is inconsistent")
    if require_adequate and not adequately_powered:
        raise ResearchContractError("INCONCLUSIVE_DATA_OR_POWER")
    return dict(payload)
