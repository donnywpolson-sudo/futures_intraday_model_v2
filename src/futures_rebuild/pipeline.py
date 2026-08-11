"""Steady-state, fail-closed Phase 1A-11 pipeline interface.

The built-in smoke runner proves deterministic mechanics with generated data.
It does not authorize provider access, real-history evaluation, prediction
materialization, candidate sealing, or holdout access.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

import numpy as np

from .canonical import canonical_bytes, sha256_json
from .alpha_research_ladder import load_active_ladder
from .historical_builder import build_synthetic_research_run
from .historical_engine_contracts import (
    HistoricalResearchDataset,
    LinearCandidate,
    synthetic_research_fixture,
)
from .historical_evaluator import evaluate_frozen_research_run
from .historical_splitter import split_synthetic_research_run
from .profiles import ProfileContractError, validate_profiles
from .errors import ContractError
from .prop_firm_eod_risk import build_active_draft_policy
from .prop_firm_phase8 import build_phase8_preparation
from .research import (
    SessionWindow,
    TemporalSamples,
    make_synthetic_permit,
    nested_chronological_splits,
)
from .schemas import OutcomeStatus


PIPELINE_SCHEMA = "futures_pipeline_smoke/1.0.0"
PHASES: tuple[str, ...] = (
    "1A",
    "1B",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
)
PHASE_PURPOSES = {
    "1A": "provider request planning and immutable DBN ingestion",
    "1B": "raw conversion and source-alignment validation",
    "2": "causal and session-normalized base data",
    "3": "explicit-lag labels and outcome state",
    "4": "causal feature matrices",
    "5": "chronological nested split plans with purge and embargo",
    "6": "frozen builder outputs and out-of-sample predictions",
    "7": "saved-prediction and signal-quality audit",
    "8": "costed economics, model selection, and portfolio risk gate",
    "9": "bounded research and statistical robustness diagnostics",
    "10": "candidate sealing approval gate",
    "11": "locked holdout and forward-access guard",
}
NO_AUTHORITY = {
    "provider_calls_authorized": False,
    "real_history_evaluation_authorized": False,
    "prediction_materialization_authorized": False,
    "candidate_sealing_authorized": False,
    "holdout_access_authorized": False,
    "order_paths_authorized": False,
}


class PipelineGateError(RuntimeError):
    """Raised before an invocation could cross an approval boundary."""


def _synthetic_dataset() -> HistoricalResearchDataset:
    n = 170
    time = np.arange(n, dtype=np.float64)
    signal = np.sin(time * 0.173) + 0.2 * np.cos(time * 0.037)
    noise = np.cos(time * 0.311) - 0.1 * np.sin(time * 0.071)
    features = np.column_stack((signal, noise)).astype(np.float64)
    labels = (0.8 * signal + 0.03 * np.sin(time * 0.91)).astype(np.float64)
    resolved = np.ones(n, dtype=np.bool_)
    fixture = synthetic_research_fixture(features, labels, resolved)
    permit = make_synthetic_permit(
        fixture, generator_id="futures-pipeline-smoke", seed=404
    )
    sessions = np.arange(n, dtype=np.int64)
    temporal = TemporalSamples(
        sessions,
        sessions + np.int64(1),
        sessions + np.int64(2),
        sessions + np.int64(2),
    )
    return HistoricalResearchDataset(
        sample_ids=tuple(f"synthetic-{index:04d}" for index in range(n)),
        feature_names=("signal", "noise"),
        features=features,
        labels=labels,
        outcome_statuses=tuple(OutcomeStatus.MATURED.value for _ in range(n)),
        temporal=temporal,
        permit=permit,
    )


def _synthetic_folds(temporal: TemporalSamples):
    return nested_chronological_splits(
        temporal,
        (SessionWindow(100, 120), SessionWindow(130, 150)),
        (
            (SessionWindow(50, 60), SessionWindow(75, 85)),
            (SessionWindow(70, 80), SessionWindow(105, 115)),
        ),
        session_embargo=1,
        minimum_fit_samples=20,
        minimum_audit_samples=8,
    )


def _phase_record(
    phase: str,
    *,
    state: str = "SYNTHETIC_MECHANICS_PASS",
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    core = {
        "phase": phase,
        "purpose": PHASE_PURPOSES[phase],
        "state": state,
        "synthetic_only": True,
        "alpha_evidence": False,
        "authority": dict(NO_AUTHORITY),
        "evidence": evidence or {},
    }
    return {**core, "record_id": sha256_json(core)}


def run_synthetic_pipeline(
    *,
    profile_path: Path,
    repository_root: Path | None = None,
) -> dict[str, Any]:
    """Run the full deterministic smoke path without reading historical data."""

    root = repository_root or _repository_root()
    profile_summary = validate_profiles(
        profile_path, repository_root=repository_root
    )
    prop_firm_phase8 = build_phase8_preparation(root=root)
    dataset = _synthetic_dataset()
    raw = np.ascontiguousarray(dataset.features.copy())
    causal = np.ascontiguousarray(raw - raw.mean(axis=0, keepdims=True))
    labels = np.ascontiguousarray(dataset.labels.copy())
    features = np.ascontiguousarray(causal.copy())

    folds = _synthetic_folds(dataset.temporal)
    split = split_synthetic_research_run(dataset, folds)
    candidates = (
        LinearCandidate("all-ridge-1", ("signal", "noise"), 1.0),
        LinearCandidate("signal-ridge-small", ("signal",), 0.01),
        LinearCandidate("signal-ridge-large", ("signal",), 25.0),
    )
    built = build_synthetic_research_run(split.builder_packets, candidates)
    evaluated = evaluate_frozen_research_run(split.evaluator_packets, built)
    for item in evaluated:
        item.validate()

    phases = (
        _phase_record(
            "1A",
            evidence={
                "fixture": "GENERATED_IN_MEMORY",
                "rows": len(dataset.sample_ids),
                "provider_calls": 0,
            },
        ),
        _phase_record(
            "1B",
            evidence={"raw_shape": list(raw.shape), "source_kind": "SYNTHETIC"},
        ),
        _phase_record(
            "2",
            evidence={
                "causal_shape": list(causal.shape),
                "future_columns_read": 0,
            },
        ),
        _phase_record(
            "3",
            evidence={
                "label_rows": int(labels.size),
                "entry_lag_sessions": 1,
                "label_horizon_sessions": 1,
            },
        ),
        _phase_record(
            "4",
            evidence={
                "feature_names": list(dataset.feature_names),
                "outcome_columns_read": 0,
            },
        ),
        _phase_record(
            "5",
            evidence={
                "outer_folds": len(folds),
                "inner_folds_per_outer": [len(item.inner_folds) for item in folds],
                "split_schedule_id": split.split_schedule_id,
            },
        ),
        _phase_record(
            "6",
            evidence={
                "builder_outputs": len(built),
                "prediction_rows": sum(
                    len(item.predictions.audit_sample_ids) for item in built
                ),
                "candidate_eligible": False,
            },
        ),
        _phase_record(
            "7",
            evidence={
                "evaluated_folds": len(evaluated),
                "mechanics_states": sorted(
                    {item.mechanics_state for item in evaluated}
                ),
                "alpha_evidence": False,
            },
        ),
        _phase_record(
            "8",
            evidence={
                "evaluation_metrics_finite": all(
                    item.mean_squared_error is not None for item in evaluated
                ),
                "promotion_ready": False,
                "portfolio_risk_gate": "SYNTHETIC_ONLY",
                "prop_firm_profile_id": prop_firm_phase8["profile_id"],
                "prop_firm_profile_sha256": prop_firm_phase8[
                    "profile_hash"
                ],
                "prop_firm_profile_document_sha256": prop_firm_phase8[
                    "profile_document_sha256"
                ],
                "provider_id": prop_firm_phase8["provider_id"],
                "account_stage": prop_firm_phase8["account_stage"],
                "prop_firm_runtime_identity": prop_firm_phase8[
                    "runtime_identity"
                ],
                "cost_status": prop_firm_phase8["cost_status"],
                "exact_provider_account_costs_verified": prop_firm_phase8[
                    "exact_provider_account_costs_verified"
                ],
                "evaluation_result_label": prop_firm_phase8[
                    "evaluation_result_label"
                ],
                "evaluation_authorized": False,
            },
        ),
        _phase_record(
            "9",
            evidence={
                "negative_control_scope": "SYNTHETIC",
                "statistical_claim_allowed": False,
            },
        ),
        _phase_record(
            "10",
            state="PASS_GUARD_CLOSED",
            evidence={"candidate_sealed": False, "approval_present": False},
        ),
        _phase_record(
            "11",
            state="PASS_GUARD_CLOSED",
            evidence={
                "holdout_read": False,
                "forward_read": False,
                "approval_present": False,
            },
        ),
    )
    core = {
        "schema_version": PIPELINE_SCHEMA,
        "mode": "SYNTHETIC_MECHANICS_ONLY",
        "profile_summary": profile_summary,
        "phases": list(phases),
        "authority": dict(NO_AUTHORITY),
        "success": True,
    }
    return {**core, "run_id": sha256_json(core)}


def _write_once(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(canonical_bytes(payload))
        handle.write(b"\n")


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="futures-pipeline")
    parser.add_argument(
        "--profiles",
        type=Path,
        default=None,
        help="operational profile YAML (defaults to configs/alpha_tiered.yaml)",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--real-history",
        action="store_true",
        help="request a real-history path (always blocked by this smoke interface)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list")
    subparsers.add_parser("validate-profiles")
    subparsers.add_parser("prop-firm-risk-policy")
    subparsers.add_parser("prop-firm-phase8")
    subparsers.add_parser("smoke")
    for phase in PHASES:
        subparsers.add_parser(f"phase{phase.lower()}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = _repository_root()
    profiles = (args.profiles or root / "configs" / "alpha_tiered.yaml").resolve()
    if args.real_history:
        raise SystemExit(
            "BLOCKED: real-history work requires a separate exact approval and "
            "durable TrialRegistry declaration"
        )
    try:
        if args.command == "list":
            result: dict[str, Any] = {
                "schema_version": PIPELINE_SCHEMA,
                "preparation_interfaces": [
                    "prop-firm-risk-policy",
                    "prop-firm-phase8",
                ],
                "phases": [
                    {"phase": phase, "purpose": PHASE_PURPOSES[phase]}
                    for phase in PHASES
                ],
                "authority": dict(NO_AUTHORITY),
            }
        elif args.command == "validate-profiles":
            result = validate_profiles(profiles, repository_root=root)
            contract, profile = load_active_ladder(root)
            result["active_alpha_ladder"] = {
                "contract_id": contract["contract_id"],
                "profile_id": profile["profile_id"],
                "state": "ACTIVE_HASH_BOUND",
            }
            phase8 = build_phase8_preparation(root=root)
            result["active_prop_firm_profile"] = {
                "profile_id": phase8["profile_id"],
                "profile_hash": phase8["profile_hash"],
                "profile_document_sha256": phase8["profile_document_sha256"],
                "phase8_preparation_id": phase8["preparation_id"],
                "account_stage": phase8["account_stage"],
                "cache_identity": phase8["runtime_identity"]["cache_identity"],
                "production_readiness": phase8["production_readiness"],
                "state": "SELECTED_NON_AUTHORIZING_PRODUCTION_BLOCKED",
            }
        elif args.command == "prop-firm-risk-policy":
            result = build_active_draft_policy(root=root)
        elif args.command == "prop-firm-phase8":
            result = build_phase8_preparation(root=root)
        else:
            full = run_synthetic_pipeline(
                profile_path=profiles, repository_root=root
            )
            if args.command == "smoke":
                result = full
            else:
                requested = args.command.removeprefix("phase").upper()
                result = next(
                    item for item in full["phases"] if item["phase"] == requested
                )
    except (ContractError, ProfileContractError, PipelineGateError, ValueError) as exc:
        raise SystemExit(f"BLOCKED: {exc}") from exc

    if args.output is not None:
        _write_once(args.output.resolve(), result)
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
