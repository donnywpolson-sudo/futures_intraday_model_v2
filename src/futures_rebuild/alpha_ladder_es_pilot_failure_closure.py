"""Prepare the immutable closure for the failed counted Alpha ES pilot.

This module reads only sealed, price-free execution evidence.  It does not
open historical source rows, publish records, mutate pointers, or authorize a
successor mechanism.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError


TRIAL_ID = "a6ae7b8394906c3661b9f1456f30cf513d5a1df43a072c9e8a601bc8989c82bc"
MECHANISM_ID = "cfefe8ce78e46d1e6a68184cbebdf4f4fe6d46169dc7bbfcfcd501c595563dc3"
PLAN_ID = "aeff50fab7f7f4733a9b3931821fe7020ecae827a52bc8ff923a930d49623ff9"
PLAN_SHA256 = "1ec9b67d94336ca38ab39f9eafaee886f3168b114347fbbfa751de374f698411"
RECEIPT_ID = "8f6fd8c6adfd2370a521436e3592b9660846597878ec50dd0eedc39e6c91a60e"

PLAN_PATH = Path("configs/alpha_ladder_es_pilot_execution_plan_v2.json")
REGISTRATION_PATH = Path("state/trial_registry/alpha_ladder_es_pilot") / f"{TRIAL_ID}.json"
AUTHORIZATION_USE_PATH = Path("state/authorization_uses") / f"{RECEIPT_ID}.json"
EXECUTION_ROOT = (
    Path("state/unpublished_evidence/alpha_ladder_es_pilot_execution")
    / TRIAL_ID
    / "attempt-1"
)
AUDIT_PATH = (
    Path("state/unpublished_evidence/alpha_ladder_es_pilot_failure_audit")
    / TRIAL_ID
    / "attempt-1"
    / "failure_audit.json"
)
CLOSURE_ROOT = Path("state/unpublished_evidence/alpha_ladder_es_pilot_failure_closure")
MANIFEST_PATH = Path("reports/alpha_ladder_es_pilot_failure_publication_manifest.json")

EXPECTED_BINDINGS = {
    PLAN_PATH.as_posix(): PLAN_SHA256,
    REGISTRATION_PATH.as_posix(): "b2b78123080cf4eb9a09778f0815f12a0d7b1839e4e39d4c147566f6af2e8e44",
    AUTHORIZATION_USE_PATH.as_posix(): "20a9f8a801d6d6789c03ccba18a629f2ef2979796c1eeafcfc170169c3a36771",
    (EXECUTION_ROOT / "baseline_executions.json").as_posix(): "0ee81821aeff8888969719d128c7767e6623eca063485630209f4c89c7f71007",
    (EXECUTION_ROOT / "candidate_execution.json").as_posix(): "d676c5e5168ee1ef67dcfc57bf1848bc3c00540b83e10befd3bcc3ec1ac097b1",
    (EXECUTION_ROOT / "input_audit.json").as_posix(): "ead08df45a4b1a215c2e194f29529fd096bac5aae258dd0bed784185f7ffbc8a",
    (EXECUTION_ROOT / "metrics.json").as_posix(): "ffed0f7b2dd14a77c2b295346a857941aa30c9d7f65c6658478795426e7825da",
    (EXECUTION_ROOT / "model.json").as_posix(): "4c4943f460d7a6b8d97ef65bada3e020aff4d08d944b3a9ca5d35d22918ccdad",
    (EXECUTION_ROOT / "pilot_decision.json").as_posix(): "404b0e3751eb7874565a34bd33852d7ee8f3e253bfc095eb3d6fafce4ddaa4d5",
    (EXECUTION_ROOT / "predictions.json").as_posix(): "dd0a15093d84686707a6211f3347e615c771080f392fee593084d395a918a85b",
    (EXECUTION_ROOT / "terminal_report.json").as_posix(): "6ec7c370eea00958dea41d4ffa7f746a1b6f89d74d33720c49a9f837fe6e4dc7",
}

ARTIFACT_ID_KEYS = {
    "baseline_executions.json": "baseline_execution_id",
    "candidate_execution.json": "candidate_execution_id",
    "input_audit.json": "input_audit_id",
    "metrics.json": "metrics_artifact_id",
    "model.json": "model_artifact_id",
    "pilot_decision.json": "pilot_decision_id",
    "predictions.json": "prediction_artifact_id",
    "terminal_report.json": "terminal_report_id",
}


def _canonical_object(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"pilot closure input is unreadable: {path}") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"pilot closure input is not canonical: {path}")
    return value


def _artifact(path: Path, *, identity_key: str) -> dict[str, object]:
    payload = _canonical_object(path)
    core = dict(payload)
    identity = core.pop(identity_key, None)
    if identity != sha256_json(core):
        raise IntegrityError(f"pilot artifact identity changed: {path}")
    return payload


def _verify_bindings(root: Path) -> None:
    for relative, expected_sha in EXPECTED_BINDINGS.items():
        path = root / relative
        if not path.is_file() or sha256_file(path) != expected_sha:
            raise IntegrityError(f"pilot closure binding changed: {relative}")


def load_sealed_evidence(*, root: Path) -> dict[str, dict[str, object]]:
    root = root.resolve(strict=False)
    _verify_bindings(root)
    evidence = {
        name.removesuffix(".json"): _artifact(
            root / EXECUTION_ROOT / name,
            identity_key=identity_key,
        )
        for name, identity_key in ARTIFACT_ID_KEYS.items()
    }
    receipt = _canonical_object(root / AUTHORIZATION_USE_PATH)
    plan = _canonical_object(root / PLAN_PATH)
    registration = _canonical_object(root / REGISTRATION_PATH)
    decision = evidence["pilot_decision"]
    terminal = evidence["terminal_report"]
    if (
        receipt.get("receipt_id") != RECEIPT_ID
        or receipt.get("operation") != "EXECUTE_CERTIFIED_TRIAL_HISTORICAL_SCREEN"
        or plan.get("plan_id") != PLAN_ID
        or plan.get("trial_id") != TRIAL_ID
        or plan.get("mechanism_id") != MECHANISM_ID
        or registration.get("trial_id") != TRIAL_ID
        or registration.get("protocol_id") != MECHANISM_ID
        or decision.get("decision") != "FAIL"
        or terminal.get("decision") != "FAIL"
        or terminal.get("state") != "SEALED_UNPUBLISHED_ECONOMIC_SCREEN_COMPLETE"
        or terminal.get("economic_evaluation_occurred") is not True
        or terminal.get("retry_authorized") is not False
        or terminal.get("holdout_2025_accessed") is not False
    ):
        raise IntegrityError("sealed ES pilot lifecycle changed")
    return {**evidence, "receipt": receipt, "plan": plan, "registration": registration}


def build_failure_audit(*, root: Path) -> dict[str, object]:
    evidence = load_sealed_evidence(root=root)
    predictions = evidence["predictions"].get("rows")
    model = evidence["model"]
    metrics = evidence["metrics"].get("strategies")
    candidate = evidence["candidate_execution"].get("scenarios")
    input_audit = evidence["input_audit"]
    decision = evidence["pilot_decision"]
    if (
        not isinstance(predictions, list)
        or len(predictions) != 63
        or not isinstance(metrics, Mapping)
        or not isinstance(candidate, Mapping)
    ):
        raise IntegrityError("sealed ES pilot evidence is structurally incomplete")

    scores = sorted(Decimal(str(row["selected_predicted_stress_net_r"])) for row in predictions)
    hurdle_sessions = {
        str(row["session"]) for row in predictions if row.get("hurdle_passed") is True
    }
    positive_predictions = sum(
        Decimal(str(row["selected_predicted_stress_net_r"])) > 0 for row in predictions
    )
    stress_account = candidate["stress"]["account"]  # type: ignore[index]
    stress_daily = stress_account["daily"]  # type: ignore[index]
    risk_sessions = {
        str(row["session"])
        for row in stress_daily
        if row.get("disposition") == "RISK_ABSTENTION"
    }
    candidate_stress = metrics["candidate"]["stress"]  # type: ignore[index]
    baseline_stress = {
        name: {
            "trade_count": value["stress"]["trade_count"],
            "net_pnl_usd": value["stress"]["net_pnl_usd"],
            "maximum_continuous_drawdown_usd": value["stress"][
                "maximum_continuous_drawdown_usd"
            ],
        }
        for name, value in metrics.items()
        if name != "candidate"
    }
    source_audit = input_audit.get("source_audit")
    if (
        hurdle_sessions != risk_sessions
        or len(hurdle_sessions) != 20
        or candidate_stress.get("trade_count") != 0
        or candidate_stress.get("net_pnl_usd") != "0"
        or candidate_stress.get("coverage")
        != {"expected_sessions": 63, "terminal_sessions": 63, "complete": True}
        or not isinstance(source_audit, Mapping)
        or any(item.get("sessionless_dependency_rows") != 0 for item in source_audit.values())
        or decision.get("failed_gates")
        != [
            "MINIMUM_EIGHT_TRADES",
            "POSITIVE_STRESS_NET_PNL",
            "BEAT_BASELINE__flat_no_trade",
        ]
    ):
        raise IntegrityError("sealed ES pilot diagnosis no longer reconciles")

    core: dict[str, object] = {
        "schema_version": "alpha_ladder_es_pilot_failure_audit/1.0.0",
        "state": "PREPARED_UNPUBLISHED_PRICE_FREE_AUDIT",
        "trial_id": TRIAL_ID,
        "mechanism_id": MECHANISM_ID,
        "plan_id": PLAN_ID,
        "authorization_receipt_id": RECEIPT_ID,
        "terminal_decision": "FAIL",
        "classification": "CONCLUSIVE_PILOT_ECONOMIC_REJECTION_ZERO_TRADABLE_SIGNALS",
        "scope": {
            "market": "ES",
            "training_sessions": 504,
            "embargo_sessions": 1,
            "evaluation_sessions": 63,
            "evaluation_start": "2020-01-14",
            "evaluation_end": "2020-04-14",
        },
        "exact_reconciliation": {
            "prediction_rows": len(predictions),
            "positive_selected_predictions": positive_predictions,
            "hurdle_passes": len(hurdle_sessions),
            "risk_abstentions": len(risk_sessions),
            "hurdle_pass_sessions_equal_risk_abstention_sessions": True,
            "below_hurdle": len(predictions) - len(hurdle_sessions),
            "candidate_trades": candidate_stress["trade_count"],
            "candidate_stress_net_pnl_usd": candidate_stress["net_pnl_usd"],
            "candidate_maximum_continuous_drawdown_usd": candidate_stress[
                "maximum_continuous_drawdown_usd"
            ],
            "prediction_score_min_stress_net_r": str(scores[0]),
            "prediction_score_median_stress_net_r": str(scores[len(scores) // 2]),
            "prediction_score_max_stress_net_r": str(scores[-1]),
            "failed_gates": decision["failed_gates"],
            "baseline_stress_results": baseline_stress,
        },
        "model_diagnostics": {
            "training_transformation_sessions": model["transformation_session_count"],
            "long_target_count": model["target_counts"]["LONG"],  # type: ignore[index]
            "short_target_count": model["target_counts"]["SHORT"],  # type: ignore[index]
            "unconditional_long_stress_net_r_mean": model[
                "unconditional_stress_net_r_means"
            ]["LONG"],  # type: ignore[index]
            "unconditional_short_stress_net_r_mean": model[
                "unconditional_stress_net_r_means"
            ]["SHORT"],  # type: ignore[index]
            "parameter_search": model["parameter_search"],
            "training_only_standardization": True,
        },
        "root_cause": {
            "primary": "FROZEN_STANDARD_CONTRACT_STOP_COST_RISK_CAP_MISMATCH",
            "secondary": "MOST_SESSIONS_BELOW_LOCKED_POSITIVE_EXPECTANCY_HURDLE",
            "explanation": (
                "All 20 sessions whose selected model forecast cleared +0.25R were "
                "blocked because one standard ES contract with the causal 1.5-ATR stop "
                "and locked costs exceeded the $250 planned-loss cap. The other 43 "
                "sessions did not clear the hurdle. No candidate order was therefore "
                "eligible to reach trigger, fill, stop, or scheduled-exit simulation."
            ),
        },
        "fault_classification": {
            "source_or_calendar_defect_proven": False,
            "missing_or_silently_dropped_evaluation_sessions": False,
            "causal_timing_defect_proven": False,
            "training_leakage_proven": False,
            "cost_or_pnl_arithmetic_defect_proven": False,
            "baseline_schedule_reuse_proven": False,
            "gate_reconstruction_defect_proven": False,
            "implementation_exception_occurred": False,
            "exact_frozen_mechanism_failed_pilot": True,
            "predictive_alpha_separately_disproved": False,
        },
        "interpretation": {
            "what_is_proven": (
                "The complete frozen mechanism, including its standard-contract sizing "
                "and risk policy, failed its preregistered pilot screen."
            ),
            "what_is_not_proven": (
                "Because the candidate admitted zero trades, this pilot does not isolate "
                "whether the model's predictions would have earned positive realized "
                "returns under a different sizing or risk design."
            ),
            "governance_result": (
                "Tier 1 advancement, retry, parameter rescue, and post-outcome tuning of "
                "this counted mechanism are forbidden."
            ),
        },
        "data_assurance": {
            "source_years": sorted(source_audit),
            "source_hashes_reconciled": True,
            "complete_63_session_daily_accounting": True,
            "raw_rows_copied_to_audit": False,
            "holdout_2025_accessed": False,
        },
        "implementation_assurance": {
            "same_attempt_deterministic_replay": True,
            "all_execution_artifact_identities_verified": True,
            "six_mandatory_baselines_simulated_independently": True,
            "true_zero_baseline_exactly_zero": True,
            "no_proven_current_implementation_defect": True,
        },
        "bindings": dict(sorted(EXPECTED_BINDINGS.items())),
        "publication_authorized": False,
        "active_pointer_mutation_authorized": False,
        "successor_authorized": False,
        "retry_authorized": False,
    }
    return {**core, "audit_id": sha256_json(core)}


def build_closure(*, root: Path, audit: Mapping[str, object] | None = None) -> dict[str, object]:
    root = root.resolve(strict=False)
    audit_payload = dict(audit) if audit is not None else build_failure_audit(root=root)
    audit_core = dict(audit_payload)
    audit_id = audit_core.pop("audit_id", None)
    if audit_id != sha256_json(audit_core):
        raise IntegrityError("ES pilot failure audit identity changed")
    audit_sha = sha256_json(audit_payload)
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_es_pilot_failure_closure/1.0.0",
        "state": "PREPARED_UNPUBLISHED_TERMINAL_CLOSURE",
        "classification": "CONCLUSIVE_PILOT_ECONOMIC_REJECTION_ZERO_TRADABLE_SIGNALS",
        "trial_id": TRIAL_ID,
        "mechanism_id": MECHANISM_ID,
        "plan_id": PLAN_ID,
        "authorization_receipt_id": RECEIPT_ID,
        "economic_result": "FAIL",
        "strategy_failure": True,
        "strategy_failure_scope": "EXACT_FROZEN_MECHANISM_AT_PREREGISTERED_ES_PILOT",
        "data_failure": False,
        "implementation_failure": False,
        "candidate_trade_count": 0,
        "candidate_stress_net_pnl_usd": "0",
        "terminal_decision_id": "f08d8dd53e6d3d33b94d59b617bed9800553069e8f172e5c27bc2273cd31defa",
        "terminal_report_id": "b240d05f7c060f4b9ade4d8ea0389df7d8a56f7140319e3bb583cc94c1972a31",
        "failure_audit": {
            "path": AUDIT_PATH.as_posix(),
            "audit_id": audit_id,
            "canonical_payload_sha256": audit_sha,
        },
        "evidence_bindings": dict(sorted(EXPECTED_BINDINGS.items())),
        "attempt_consumed": True,
        "retry_authorized": False,
        "tier1_advancement_authorized": False,
        "parameter_rescue_authorized": False,
        "automatic_successor_authorized": False,
        "holdout_2025_accessed": False,
        "live_readiness_claim": False,
        "publication_authorized": False,
        "active_pointer_mutation_authorized": False,
        "preservation": "REGISTRATION_PLAN_AUTHORIZATION_AND_ALL_EXECUTION_BYTES_UNCHANGED",
    }
    return {**core, "closure_id": sha256_json(core)}


def closure_path(closure: Mapping[str, object]) -> Path:
    return CLOSURE_ROOT / str(closure["closure_id"]) / "closure.json"


def build_publication_manifest(
    *, root: Path, audit: Mapping[str, object], closure: Mapping[str, object]
) -> dict[str, object]:
    root = root.resolve(strict=False)
    audit_path = root / AUDIT_PATH
    closure_source = closure_path(closure)
    if not audit_path.is_file() or _canonical_object(audit_path) != dict(audit):
        raise IntegrityError("prepared ES pilot audit is missing or changed")
    if not (root / closure_source).is_file() or _canonical_object(root / closure_source) != dict(closure):
        raise IntegrityError("prepared ES pilot closure is missing or changed")

    copies = [
        {
            "source_path": (EXECUTION_ROOT / name).as_posix(),
            "source_sha256": EXPECTED_BINDINGS[(EXECUTION_ROOT / name).as_posix()],
            "destination_path": (
                Path("state/trial_registry/alpha_ladder_es_pilot_execution")
                / TRIAL_ID
                / "attempt-1"
                / name
            ).as_posix(),
        }
        for name in sorted(ARTIFACT_ID_KEYS)
    ]
    copies.extend(
        [
            {
                "source_path": AUDIT_PATH.as_posix(),
                "source_sha256": sha256_file(audit_path),
                "destination_path": (
                    Path("state/trial_registry/alpha_ladder_es_pilot_failure_audit")
                    / f"{audit['audit_id']}.json"
                ).as_posix(),
            },
            {
                "source_path": closure_source.as_posix(),
                "source_sha256": sha256_file(root / closure_source),
                "destination_path": (
                    Path("state/trial_registry/alpha_ladder_es_pilot_terminal_closure")
                    / f"{closure['closure_id']}.json"
                ).as_posix(),
            },
        ]
    )
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_es_pilot_failure_publication_manifest/1.0.0",
        "state": "PREPARED_UNPUBLISHED_NOT_AUTHORIZED",
        "trial_id": TRIAL_ID,
        "mechanism_id": MECHANISM_ID,
        "audit_id": audit["audit_id"],
        "closure_id": closure["closure_id"],
        "publication_mode": "CREATE_ONLY_EXACT_BYTE_COPY",
        "create_only_copies": copies,
        "preserve_in_place": [
            {
                "path": AUTHORIZATION_USE_PATH.as_posix(),
                "sha256": EXPECTED_BINDINGS[AUTHORIZATION_USE_PATH.as_posix()],
            },
            {
                "path": REGISTRATION_PATH.as_posix(),
                "sha256": EXPECTED_BINDINGS[REGISTRATION_PATH.as_posix()],
            },
            {"path": PLAN_PATH.as_posix(), "sha256": PLAN_SHA256},
        ],
        "publication_authorized": False,
        "active_pointer_mutation": False,
        "tier1_registration_or_execution": False,
        "successor_creation": False,
        "holdout_2025_access": False,
        "provider_network_credentials": False,
        "staging_commit_push": False,
        "rollback_boundary": "NO_POINTER_OR_EXISTING_BYTE_MAY_CHANGE",
    }
    return {**core, "manifest_id": sha256_json(core)}


def _create_or_verify(path: Path, payload: Mapping[str, object]) -> None:
    expected = canonical_bytes(dict(payload)) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != expected:
            raise IntegrityError(f"prepared ES pilot closure artifact differs: {path}")
        return
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, expected)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def prepare_failure_closure(*, root: Path) -> dict[str, str]:
    root = root.resolve(strict=False)
    audit = build_failure_audit(root=root)
    _create_or_verify(root / AUDIT_PATH, audit)
    closure = build_closure(root=root, audit=audit)
    source = closure_path(closure)
    _create_or_verify(root / source, closure)
    manifest = build_publication_manifest(root=root, audit=audit, closure=closure)
    _create_or_verify(root / MANIFEST_PATH, manifest)
    return verify_prepared_failure_closure(root=root)


def verify_prepared_failure_closure(*, root: Path) -> dict[str, str]:
    root = root.resolve(strict=False)
    expected_audit = build_failure_audit(root=root)
    audit = _canonical_object(root / AUDIT_PATH)
    if audit != expected_audit:
        raise IntegrityError("prepared ES pilot failure audit changed")
    expected_closure = build_closure(root=root, audit=audit)
    source = closure_path(expected_closure)
    closure = _canonical_object(root / source)
    if closure != expected_closure:
        raise IntegrityError("prepared ES pilot failure closure changed")
    expected_manifest = build_publication_manifest(root=root, audit=audit, closure=closure)
    manifest = _canonical_object(root / MANIFEST_PATH)
    if manifest != expected_manifest:
        raise IntegrityError("prepared ES pilot publication manifest changed")
    return {
        "audit_id": str(audit["audit_id"]),
        "audit_path": AUDIT_PATH.as_posix(),
        "audit_sha256": sha256_file(root / AUDIT_PATH),
        "closure_id": str(closure["closure_id"]),
        "closure_path": source.as_posix(),
        "closure_sha256": sha256_file(root / source),
        "manifest_id": str(manifest["manifest_id"]),
        "manifest_path": MANIFEST_PATH.as_posix(),
        "manifest_sha256": sha256_file(root / MANIFEST_PATH),
    }
