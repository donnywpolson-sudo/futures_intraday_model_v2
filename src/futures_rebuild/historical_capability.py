"""Production-shaped historical research capability with execution disabled.

The module proves that the exact foundation and statistical machinery are
present and can derive gates from row-level returns.  It deliberately does not
open real history: that boundary requires a separately signed, single-use
``EXTERNAL_REAL_HISTORY_AUTHORIZATION`` receipt.
"""

from __future__ import annotations

import ast
import json
import math
import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

import numpy as np

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation.market_state import FoundationCoveragePolicy
from .foundation.orchestrator import load_foundation_set
from .release import VerifiedReleaseReceipt
from .research.bootstrap import stationary_bootstrap_index_rows
from .research.contracts import (
    ResearchContractError,
    array_sha256,
    finite_float64,
    require_unique_ascii_ids,
)
from .research.controls import (
    NegativeControlOutcome,
    NegativeControlState,
    evaluate_negative_controls,
)
from .research.cscv import exhaustive_cscv_pbo
from .research.dsr import deflated_sharpe_ratio
from .research.hac import newey_west_mean
from .research.multiple_testing import romano_wolf_from_differentials
from .research.power import training_only_mde


PROJECT = "futures_intraday_model_v2"
CAPABILITY_CONFIG = "configs/historical_capability.json"
REAL_HISTORY_OPERATION = "RUN_HISTORICAL_HYPOTHESIS_WFA"
REAL_HISTORY_CLASSIFICATION = OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
_SHA = re.compile(r"[0-9a-f]{64}")
REQUIRED_COMPONENTS = {
    "futures_rebuild.foundation.selection": ["resolve_foundation_selection"],
    "futures_rebuild.foundation.orchestrator": ["load_foundation_set"],
    "futures_rebuild.historical_builder": ["build_synthetic_research_run"],
    "futures_rebuild.historical_evaluator": ["evaluate_frozen_research_run"],
    "futures_rebuild.historical_splitter": ["split_synthetic_research_run"],
    "futures_rebuild.producer_bridge": [
        "generate_causal_outcomes",
        "load_causal_feature_release",
        "load_causal_outcome_release",
    ],
    "futures_rebuild.research.bootstrap": ["stationary_bootstrap_index_rows"],
    "futures_rebuild.research.cscv": ["exhaustive_cscv_pbo"],
    "futures_rebuild.research.dsr": ["deflated_sharpe_ratio"],
    "futures_rebuild.research.hac": ["newey_west_mean"],
    "futures_rebuild.research.multiple_testing": [
        "romano_wolf_from_differentials"
    ],
    "futures_rebuild.research.power": ["training_only_mde"],
    "futures_rebuild.source_symbology": ["require_query_contract"],
    "futures_rebuild.trial": ["TrialEventLedger", "TrialRegistry"],
}
REQUIRED_DERIVED_GATES = [
    "SESSION_LEVEL_NET_RETURNS",
    "HAC_MEAN_AND_STANDARD_ERROR",
    "STATIONARY_BLOCK_BOOTSTRAP_LOWER_BOUND",
    "ROMANO_WOLF_MAX_T",
    "DEFLATED_SHARPE_RATIO",
    "EXHAUSTIVE_CSCV_PBO",
    "TRAINING_ONLY_POWER",
    "COST_MONOTONICITY",
    "INDEPENDENT_MARKET_DIRECTION_SLEEVES",
    "NEGATIVE_CONTROLS",
    "ONE_TIME_FINAL_HOLDOUT",
]
REQUIRED_HARD_PAUSES = [
    "CANDIDATE_SEALING",
    "PAID_DATABENTO_DOWNLOAD",
    "REAL_HISTORY_HYPOTHESIS_OR_WFA_EXECUTION",
    "TRADING",
]


def _json_object(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("historical capability JSON is invalid") from exc
    if not isinstance(payload, dict):
        raise IntegrityError("historical capability JSON must be an object")
    return payload


def load_historical_capability_config(root: Path) -> dict[str, object]:
    payload = _json_object(root / CAPABILITY_CONFIG)
    expected = {
        "authority",
        "capability_version",
        "derived_evaluation_gates",
        "foundation_contract",
        "hard_pauses",
        "production_policy",
        "project",
        "required_components",
        "status",
    }
    if (
        set(payload) != expected
        or payload.get("capability_version") != "2.0.0"
        or payload.get("project") != PROJECT
        or payload.get("status") != "IMPLEMENTED_EXECUTION_DISABLED"
        or payload.get("authority")
        != {
            "alpha_evidence": False,
            "candidate_sealing_authorized": False,
            "real_history_execution_authorized": False,
            "required_external_classification": (
                "EXTERNAL_REAL_HISTORY_AUTHORIZATION"
            ),
            "required_external_operation": REAL_HISTORY_OPERATION,
        }
        or payload.get("foundation_contract")
        != {
            "labels_materialized_at_readiness": False,
            "outcome_source_inputs_required": True,
            "query_manifest_content_addressed_required": True,
            "query_symbology_role": "PROVENANCE_ONLY_NEVER_FEATURE",
            "mixed_status_statistics_query_epochs_explicit": True,
            "statistics_as_features": False,
            "status_gated_features_required": True,
            "verified_actual_contract_economics_required": True,
        }
        or payload.get("required_components") != REQUIRED_COMPONENTS
        or payload.get("derived_evaluation_gates") != REQUIRED_DERIVED_GATES
        or payload.get("hard_pauses") != REQUIRED_HARD_PAUSES
        or not isinstance(payload.get("production_policy"), dict)
    ):
        raise IntegrityError("historical capability contract is unsafe or incomplete")
    policy = payload["production_policy"]
    if policy != {
        "bootstrap_resamples": 10000,
        "confidence_level": "0.95",
        "dsr_probability_minimum": "0.95",
        "minimum_comparable_configurations": 10,
        "minimum_cscv_blocks": 8,
        "pbo_conservative_maximum": "0.2",
        "romano_wolf_adjusted_p_maximum": "0.05",
    }:
        raise IntegrityError("historical production gate policy is not exact")
    return payload


def verify_production_capability_closure(root: Path) -> dict[str, object]:
    """Verify and hash every required production/mechanics component."""

    payload = load_historical_capability_config(root)
    components = payload["required_components"]
    assert isinstance(components, dict)
    files: dict[str, dict[str, object]] = {}
    symbols: list[str] = []
    for module_name in sorted(components):
        raw_symbols = components[module_name]
        if (
            not isinstance(module_name, str)
            or not isinstance(raw_symbols, list)
            or not raw_symbols
            or any(not isinstance(symbol, str) or not symbol for symbol in raw_symbols)
        ):
            raise IntegrityError("historical capability component registry is invalid")
        relative = "src/" + module_name.replace(".", "/") + ".py"
        source = (root / relative).resolve(strict=True)
        try:
            source.relative_to(root.resolve(strict=True))
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        except (OSError, SyntaxError, ValueError) as exc:
            raise IntegrityError("historical capability source is invalid") from exc
        top_level = {
            node.name
            for node in tree.body
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        }
        files[relative] = {
            "path": relative,
            "sha256": sha256_file(source),
            "size": source.stat().st_size,
        }
        for symbol in raw_symbols:
            if symbol not in top_level:
                raise IntegrityError(f"historical capability symbol is absent: {module_name}.{symbol}")
            symbols.append(f"{module_name}:{symbol}")
    own_relative = "src/futures_rebuild/historical_capability.py"
    own_source = (root / own_relative).resolve(strict=True)
    files[own_relative] = {
        "path": own_relative,
        "sha256": sha256_file(own_source),
        "size": own_source.stat().st_size,
    }
    trial_tree = ast.parse(
        (root / "src" / "futures_rebuild" / "trial.py").read_text(encoding="utf-8")
    )
    registry = next(
        (
            node
            for node in trial_tree.body
            if isinstance(node, ast.ClassDef) and node.name == "TrialRegistry"
        ),
        None,
    )
    if registry is None or "unlock_final_holdout" not in {
        node.name
        for node in registry.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }:
        raise IntegrityError("one-time final holdout access is not implemented")
    evaluator = (root / "src" / "futures_rebuild" / "historical_evaluator.py").read_text(
        encoding="utf-8"
    ).casefold()
    if ".fit(" in evaluator or "historical_builder" in evaluator:
        raise IntegrityError("historical evaluator can fit or import the builder")
    closure = {
        "alpha_evidence": False,
        "candidate_eligible": False,
        "capability_config_sha256": sha256_file(root / CAPABILITY_CONFIG),
        "component_files": [files[key] for key in sorted(files)],
        "component_symbols": sorted(symbols),
        "execution_authorized": False,
        "project": PROJECT,
        "status": "PRODUCTION_SHAPED_EXECUTION_DISABLED",
    }
    return {**closure, "capability_closure_id": sha256_json(closure)}


@dataclass(frozen=True)
class FoundationResearchBlueprint:
    foundation_release_id: str
    foundation_receipt_id: str
    foundation_set_id: str
    run_id: str
    source_snapshot_id: str
    source_selection_release_id: str
    source_selection_receipt_id: str
    selection_manifest_id: str
    query_manifest_id: str
    query_manifest_file_count: int
    query_mode_census: tuple[dict[str, object], ...]
    foundation_interval_count: int
    interval_count: int
    bar_rows: int
    status_eligible_rows: int
    status_gated_feature_ready_rows: int
    status_resolved_decision_fraction: str
    feature_release_ids: tuple[str, ...]
    outcome_source_release_ids: tuple[str, ...]
    economics_release_ids: tuple[str, ...]
    blueprint_id: str

    def as_dict(self) -> dict[str, object]:
        return {
            "bar_rows": self.bar_rows,
            "blueprint_id": self.blueprint_id,
            "economics_release_ids": list(self.economics_release_ids),
            "feature_release_ids": list(self.feature_release_ids),
            "foundation_receipt_id": self.foundation_receipt_id,
            "foundation_release_id": self.foundation_release_id,
            "foundation_set_id": self.foundation_set_id,
            "foundation_interval_count": self.foundation_interval_count,
            "interval_count": self.interval_count,
            "outcome_source_release_ids": list(self.outcome_source_release_ids),
            "run_id": self.run_id,
            "source_snapshot_id": self.source_snapshot_id,
            "source_selection_release_id": self.source_selection_release_id,
            "source_selection_receipt_id": self.source_selection_receipt_id,
            "selection_manifest_id": self.selection_manifest_id,
            "query_manifest_id": self.query_manifest_id,
            "query_manifest_file_count": self.query_manifest_file_count,
            "query_mode_census": list(self.query_mode_census),
            "status_eligible_rows": self.status_eligible_rows,
            "status_gated_feature_ready_rows": (
                self.status_gated_feature_ready_rows
            ),
            "status_resolved_decision_fraction": (
                self.status_resolved_decision_fraction
            ),
        }


def build_foundation_research_blueprint(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> FoundationResearchBlueprint:
    foundation = load_foundation_set(receipt, boundary=boundary)
    coverage = foundation.get("coverage_gate")
    intervals = foundation.get("intervals")
    query_manifest = foundation.get("query_manifest")
    query_mode_census = foundation.get("query_mode_census")
    run_contract = foundation.get("run_contract")
    selection_receipt = foundation.get("source_selection_receipt")
    if (
        not isinstance(coverage, dict)
        or not isinstance(intervals, list)
        or not isinstance(query_manifest, list)
        or not isinstance(query_mode_census, list)
        or not isinstance(run_contract, dict)
        or not isinstance(selection_receipt, dict)
    ):
        raise IntegrityError("foundation cannot produce a historical blueprint")
    policy = FoundationCoveragePolicy.from_dict(coverage.get("coverage_policy"))
    eligible_intervals = [
        item
        for item in intervals
        if isinstance(item, dict)
        and isinstance(item.get("status_epoch_gate"), dict)
        and item["status_epoch_gate"].get("research_disposition") == "ELIGIBLE"
    ]
    if (
        not eligible_intervals
        or len(eligible_intervals)
        != coverage.get("research_eligible_interval_count")
        or len(intervals) - len(eligible_intervals)
        != coverage.get("research_abstained_interval_count")
    ):
        raise IntegrityError("foundation research epoch eligibility is invalid")
    gates = [item["status_epoch_gate"] for item in eligible_intervals]
    fields = (
        "bar_rows",
        "status_eligible_rows",
        "status_gated_feature_ready_rows",
        "status_resolved_rows",
    )
    if any(
        type(gate.get(field)) is not int
        for gate in gates
        for field in fields
    ):
        raise IntegrityError("foundation blueprint epoch counts are invalid")
    bar_rows = sum(int(gate["bar_rows"]) for gate in gates)
    eligible = sum(int(gate["status_eligible_rows"]) for gate in gates)
    ready = sum(int(gate["status_gated_feature_ready_rows"]) for gate in gates)
    resolved_rows = sum(int(gate["status_resolved_rows"]) for gate in gates)
    resolved = str(Decimal(resolved_rows) / Decimal(bar_rows)) if bar_rows else "0"
    resolved_value = float(resolved)
    if (
        bar_rows < policy.minimum_bar_rows
        or eligible < policy.minimum_status_eligible_rows
        or ready < policy.minimum_status_gated_feature_ready_rows
        or resolved_value < float(policy.minimum_status_resolved_decision_fraction)
        or foundation.get("historical_outcome_or_label_execution") is not False
        or foundation.get("model_fit_count") != 0
        or foundation.get("wfa_execution_count") != 0
    ):
        raise IntegrityError("foundation is below production research coverage policy")
    feature_ids: list[str] = []
    outcome_ids: list[str] = []
    economics_ids: list[str] = []
    for interval in eligible_intervals:
        if not isinstance(interval, dict):
            raise IntegrityError("foundation interval blueprint is invalid")
        try:
            feature_ids.append(str(interval["feature_input_release_receipt"]["release_id"]))
            outcome_ids.append(str(interval["outcome_source_input_release_receipt"]["release_id"]))
            economics_ids.append(str(interval["economics_release_receipt"]["release_id"]))
        except (KeyError, TypeError) as exc:
            raise IntegrityError("foundation interval lacks research dependencies") from exc
    collections = (feature_ids, outcome_ids, economics_ids)
    if any(
        not values
        or len(values) != len(eligible_intervals)
        or any(_SHA.fullmatch(value) is None for value in values)
        for values in collections
    ):
        raise IntegrityError("foundation research dependency IDs are invalid")
    query_manifest_id = str(foundation.get("query_manifest_id"))
    source_snapshot_id = str(foundation.get("source_snapshot_id"))
    selection_manifest_id = str(run_contract.get("selection_manifest_id"))
    source_selection_release_id = str(selection_receipt.get("release_id"))
    source_selection_receipt_id = str(selection_receipt.get("receipt_id"))
    if (
        query_manifest_id != sha256_json(query_manifest)
        or any(
            _SHA.fullmatch(value) is None
            for value in (
                query_manifest_id,
                source_snapshot_id,
                selection_manifest_id,
                source_selection_release_id,
                source_selection_receipt_id,
            )
        )
        or not query_mode_census
    ):
        raise IntegrityError("foundation query provenance is invalid")
    core = {
        "bar_rows": bar_rows,
        "economics_release_ids": sorted(economics_ids),
        "feature_release_ids": sorted(feature_ids),
        "foundation_receipt_id": receipt.receipt_id,
        "foundation_release_id": receipt.release_id,
        "foundation_set_id": foundation["foundation_set_id"],
        "foundation_interval_count": len(intervals),
        "interval_count": len(eligible_intervals),
        "outcome_source_release_ids": sorted(outcome_ids),
        "run_id": foundation["run_id"],
        "source_snapshot_id": source_snapshot_id,
        "source_selection_release_id": source_selection_release_id,
        "source_selection_receipt_id": source_selection_receipt_id,
        "selection_manifest_id": selection_manifest_id,
        "query_manifest_id": query_manifest_id,
        "query_manifest_file_count": len(query_manifest),
        "query_mode_census": query_mode_census,
        "status_eligible_rows": eligible,
        "status_gated_feature_ready_rows": ready,
        "status_resolved_decision_fraction": resolved,
    }
    return FoundationResearchBlueprint(
        foundation_release_id=receipt.release_id,
        foundation_receipt_id=receipt.receipt_id,
        foundation_set_id=str(foundation["foundation_set_id"]),
        run_id=str(foundation["run_id"]),
        source_snapshot_id=source_snapshot_id,
        source_selection_release_id=source_selection_release_id,
        source_selection_receipt_id=source_selection_receipt_id,
        selection_manifest_id=selection_manifest_id,
        query_manifest_id=query_manifest_id,
        query_manifest_file_count=len(query_manifest),
        query_mode_census=tuple(query_mode_census),
        foundation_interval_count=len(intervals),
        interval_count=len(eligible_intervals),
        bar_rows=bar_rows,
        status_eligible_rows=eligible,
        status_gated_feature_ready_rows=ready,
        status_resolved_decision_fraction=resolved,
        feature_release_ids=tuple(sorted(feature_ids)),
        outcome_source_release_ids=tuple(sorted(outcome_ids)),
        economics_release_ids=tuple(sorted(economics_ids)),
        blueprint_id=sha256_json(core),
    )


@dataclass(frozen=True)
class AuthorizedHistoricalRun:
    receipt: OperationReceipt
    foundation_release_id: str
    foundation_research_blueprint_id: str
    query_manifest_id: str
    trial_charter_id: str

    def verify(self, boundary: RepoBoundary) -> None:
        scope = _historical_authorization_scope(
            foundation_release_id=self.foundation_release_id,
            foundation_research_blueprint_id=(
                self.foundation_research_blueprint_id
            ),
            query_manifest_id=self.query_manifest_id,
            trial_charter_id=self.trial_charter_id,
        )
        self.receipt.assert_consumed(
            boundary,
            operation=REAL_HISTORY_OPERATION,
            classification=REAL_HISTORY_CLASSIFICATION,
            required_scope=scope,
        )


def _historical_authorization_scope(
    *,
    foundation_release_id: str,
    foundation_research_blueprint_id: str,
    query_manifest_id: str,
    trial_charter_id: str,
) -> dict[str, str]:
    scope = {
        "foundation_release_id": foundation_release_id,
        "foundation_research_blueprint_id": foundation_research_blueprint_id,
        "query_manifest_id": query_manifest_id,
        "trial_charter_id": trial_charter_id,
    }
    if any(_SHA.fullmatch(value) is None for value in scope.values()):
        raise ContractError("historical authorization scope is invalid")
    return scope


def open_authorized_historical_run(
    *,
    receipt: OperationReceipt,
    blueprint: FoundationResearchBlueprint,
    trial_charter_id: str,
    boundary: RepoBoundary,
) -> AuthorizedHistoricalRun:
    if _SHA.fullmatch(trial_charter_id) is None:
        raise ContractError("historical run trial charter ID is invalid")
    scope = _historical_authorization_scope(
        foundation_release_id=blueprint.foundation_release_id,
        foundation_research_blueprint_id=blueprint.blueprint_id,
        query_manifest_id=blueprint.query_manifest_id,
        trial_charter_id=trial_charter_id,
    )
    receipt.consume(
        boundary,
        operation=REAL_HISTORY_OPERATION,
        classification=REAL_HISTORY_CLASSIFICATION,
        required_scope=scope,
    )
    result = AuthorizedHistoricalRun(
        receipt,
        blueprint.foundation_release_id,
        blueprint.blueprint_id,
        blueprint.query_manifest_id,
        trial_charter_id,
    )
    result.verify(boundary)
    return result


@dataclass(frozen=True)
class DerivedGatePolicy:
    hac_lag: int
    mean_block_length: float
    bootstrap_resamples: int
    seed: int
    alpha: float
    confidence_level: float
    minimum_effect: float
    dsr_probability_minimum: float
    pbo_conservative_maximum: float
    cscv_blocks: int
    target_power: float

    def validate(self) -> None:
        if (
            type(self.hac_lag) is not int
            or self.hac_lag < 0
            or type(self.bootstrap_resamples) is not int
            or self.bootstrap_resamples < 31
            or type(self.seed) is not int
            or not 0 <= self.seed < 2**64
            or type(self.cscv_blocks) is not int
            or self.cscv_blocks < 4
            or self.cscv_blocks % 2
        ):
            raise ResearchContractError("derived gate integer policy is invalid")
        values = (
            self.mean_block_length,
            self.alpha,
            self.confidence_level,
            self.minimum_effect,
            self.dsr_probability_minimum,
            self.pbo_conservative_maximum,
            self.target_power,
        )
        if any(type(value) is not float or not math.isfinite(value) for value in values):
            raise ResearchContractError("derived gate policy requires finite floats")
        if not (
            self.mean_block_length >= 1.0
            and 0.0 < self.alpha < 0.5
            and 0.5 < self.confidence_level < 1.0
            and self.minimum_effect > 0.0
            and 0.0 < self.dsr_probability_minimum < 1.0
            and 0.0 <= self.pbo_conservative_maximum < 1.0
            and 0.5 < self.target_power < 1.0
        ):
            raise ResearchContractError("derived gate probability/effect policy is invalid")


@dataclass(frozen=True)
class DerivedGateEvidence:
    evidence: Mapping[str, object]
    evidence_id: str

    def as_dict(self) -> dict[str, object]:
        return {**dict(self.evidence), "derived_gate_evidence_id": self.evidence_id}


def derive_gate_evidence_from_returns(
    *,
    scenario_strategy_returns: Mapping[str, np.ndarray],
    strategy_ids: tuple[str, ...],
    selected_strategy_index: int,
    trial_sharpes: np.ndarray,
    selected_trial_index: int,
    training_differentials: np.ndarray,
    negative_controls: tuple[NegativeControlOutcome, ...],
    policy: DerivedGatePolicy,
    source_kind: str = "SYNTHETIC_MECHANICS_ONLY",
    authorized_run: AuthorizedHistoricalRun | None = None,
    boundary: RepoBoundary | None = None,
) -> DerivedGateEvidence:
    """Derive every numeric gate from aligned return rows, never caller metrics."""

    policy.validate()
    ids = require_unique_ascii_ids(strategy_ids, name="strategy_ids")
    if source_kind == "EXTERNALLY_AUTHORIZED_REAL_HISTORY":
        if authorized_run is None or boundary is None:
            raise UnauthorizedOperation("real-history gate derivation lacks consumed authority")
        authorized_run.verify(boundary)
    elif source_kind != "SYNTHETIC_MECHANICS_ONLY" or authorized_run is not None:
        raise UnauthorizedOperation("gate evidence source authority is invalid")
    if set(scenario_strategy_returns) != {"base", "extreme", "stress", "zero"}:
        raise ResearchContractError("cost scenarios must be exact")
    scenarios: dict[str, np.ndarray] = {}
    shape: tuple[int, int] | None = None
    for name in ("zero", "base", "stress", "extreme"):
        values = finite_float64(
            scenario_strategy_returns[name], name=f"{name}_returns", ndim=2
        )
        if shape is None:
            shape = values.shape
        if values.shape != shape or values.shape[1] != len(ids):
            raise ResearchContractError("cost scenario return matrices are not aligned")
        scenarios[name] = values
    assert shape is not None
    if not 0 <= selected_strategy_index < len(ids):
        raise ResearchContractError("selected strategy index is invalid")
    tolerance = 1e-12
    if any(
        bool(np.any(scenarios[left] + tolerance < scenarios[right]))
        for left, right in (("zero", "base"), ("base", "stress"), ("stress", "extreme"))
    ):
        raise ResearchContractError("cost scenarios are not monotonically conservative")
    stress = scenarios["stress"]
    selected = np.ascontiguousarray(stress[:, selected_strategy_index], dtype=np.float64)
    if len(selected) <= max(policy.hac_lag, policy.cscv_blocks):
        raise ResearchContractError("too few aligned sessions for derived gates")
    hac = newey_west_mean(selected, lag=policy.hac_lag)
    means = np.asarray(
        [
            float(np.mean(selected[row], dtype=np.float64))
            for row in stationary_bootstrap_index_rows(
                n_observations=len(selected),
                n_resamples=policy.bootstrap_resamples,
                mean_block_length=policy.mean_block_length,
                seed=policy.seed,
            )
        ],
        dtype=np.float64,
    )
    lower = float(np.quantile(means, 1.0 - policy.confidence_level))
    romano = romano_wolf_from_differentials(
        stress,
        hypothesis_ids=ids,
        hac_lag=policy.hac_lag,
        mean_block_length=policy.mean_block_length,
        n_resamples=policy.bootstrap_resamples,
        seed=policy.seed,
        minimum_resamples=policy.bootstrap_resamples,
    )
    dsr = deflated_sharpe_ratio(
        selected,
        finite_float64(trial_sharpes, name="trial_sharpes", ndim=1),
        raw_trial_count=len(trial_sharpes),
        selected_trial_index=selected_trial_index,
    )
    pbo = exhaustive_cscv_pbo(
        stress,
        strategy_ids=ids,
        blocks=policy.cscv_blocks,
        metric="sharpe",
    )
    power = training_only_mde(
        finite_float64(
            training_differentials, name="training_differentials", ndim=1
        ),
        partition_role="TRAIN",
        hac_lag=policy.hac_lag,
        planned_evaluation_observations=len(selected),
        alpha=policy.alpha,
        target_power=policy.target_power,
        alternative="greater",
        economic_mean_hurdle=policy.minimum_effect,
    )
    controls = evaluate_negative_controls(negative_controls)
    selected_adjusted_p = float(
        romano.adjusted_p_values[selected_strategy_index]
    )
    gates = {
        "confidence_lower_bound": lower > policy.minimum_effect,
        "cost_monotonicity": True,
        "deflated_sharpe": dsr.probability >= policy.dsr_probability_minimum,
        "mean_after_stress_costs": hac.mean > policy.minimum_effect,
        "negative_controls": controls.state is NegativeControlState.CLEAR,
        "power": power.adequately_powered,
        "romano_wolf": selected_adjusted_p <= policy.alpha,
        "pbo": pbo.pbo_conservative <= policy.pbo_conservative_maximum,
    }
    core: dict[str, object] = {
        "alpha_evidence": False,
        "candidate_eligible": False,
        "cost_scenario_hashes": {
            name: array_sha256(scenarios[name]) for name in sorted(scenarios)
        },
        "derived_gates": gates,
        "dsr_probability": dsr.probability,
        "hac_mean": hac.mean,
        "hac_standard_error": hac.standard_error,
        "mechanics_gate_passed": all(gates.values()),
        "negative_control_state": controls.state.value,
        "pbo_conservative": pbo.pbo_conservative,
        "power_adequate": power.adequately_powered,
        "romano_wolf_adjusted_p": selected_adjusted_p,
        "selected_strategy_id": ids[selected_strategy_index],
        "source_kind": source_kind,
        "stationary_bootstrap_lower_bound": lower,
        "status": "DERIVED_FROM_ALIGNED_RETURNS_NO_ALPHA_AUTHORITY",
        "strategy_ids": list(ids),
    }
    return DerivedGateEvidence(MappingProxyType(core), sha256_json(core))
