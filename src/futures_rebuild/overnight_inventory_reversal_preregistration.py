"""Outcome-locked preregistration for one materially new futures hypothesis."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError


PREREGISTRATION_PATH = Path(
    "configs/overnight_inventory_reversal_preregistration.json"
)
CORRECTION_PATH = Path(
    "configs/overnight_inventory_reversal_preoutcome_correction.json"
)
EXECUTION_PATH = Path(
    "src/futures_rebuild/overnight_inventory_reversal_execution.py"
)
CORRECTED_DECLARATION_EXECUTION_SHA256 = (
    "f7b9b9c47e1aaf24097f4667e9a23a2adf2200a7a906a67172e05cb2c5a7ae6f"
)
REGISTRY_ROOT = Path("state/trial_registry/overnight_inventory_reversal")
EVENT_ROOT = Path("state/trial_events/overnight_inventory_reversal")
POLICY_NAMES = (
    "data_policy",
    "feature_policy",
    "target_and_execution_policy",
    "cost_policy",
    "fold_policy",
    "baseline_policy",
    "inference_policy",
    "multiplicity_policy",
    "closure_policy",
)


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid overnight-reversal artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("overnight-reversal artifact must be an object")
    return value


def load_overnight_inventory_reversal_preregistration(
    *, root: Path,
) -> dict[str, object]:
    """Verify the complete prepared declaration without opening evaluation rows."""

    protocol = _object(root / PREREGISTRATION_PATH)
    core = dict(protocol)
    preregistration_id = core.pop("preregistration_id", None)
    policy_hashes = protocol.get("policy_hashes")
    novelty = protocol.get("material_novelty")
    exposure = protocol.get("prior_evidence_exposure")
    data = protocol.get("data_policy")
    feature = protocol.get("feature_policy")
    execution = protocol.get("target_and_execution_policy")
    inference = protocol.get("inference_policy")
    multiplicity = protocol.get("multiplicity_policy")
    closure = protocol.get("closure_policy")
    authority = protocol.get("authority")
    bindings = protocol.get("governance_bindings")
    if (
        preregistration_id != sha256_json(core)
        or protocol.get("schema_version")
        != "overnight_inventory_reversal_preregistration/1.0.0"
        or protocol.get("state") != "PREPARED_NOT_REGISTERED_OUTCOMES_LOCKED"
        or protocol.get("classification")
        != "ONE_MATERIALLY_NEW_MECHANISM_DISCOVERY_TRIAL"
        or protocol.get("hypothesis_id")
        != "overnight_inventory_reversal_cash_open"
        or not isinstance(policy_hashes, Mapping)
        or set(policy_hashes) != set(POLICY_NAMES)
        or any(
            policy_hashes[name] != sha256_json(protocol[name])
            for name in POLICY_NAMES
        )
        or not isinstance(novelty, Mapping)
        or novelty.get("mechanism")
        != "OVERNIGHT_INVENTORY_DISPLACEMENT_MEAN_REVERSION"
        or novelty.get("incremental_rescue") is not False
        or novelty.get("not_a_successor_version_of")
        != "TIER1_BRACKET_DIRECTIONAL_NET_R_FAMILY"
        or not isinstance(exposure, Mapping)
        or exposure.get("new_hypothesis_evaluation_outcomes_computed_or_examined")
        is not False
        or exposure.get("historical_period_claimed_pristine") is not False
        or exposure.get("multiplicity_reset_claimed") is not False
        or not isinstance(data, Mapping)
        or data.get("phase5_manifest_sha256")
        != sha256_file(root / str(data.get("phase5_manifest_path")))
        or data.get("bound_market_year_pairs") != 20
        or data.get("outer_folds") != 8
        or data.get("source_row_access_before_registration") is not False
        or data.get("outcome_row_access_before_registration") is not False
        or not isinstance(data.get("holdout_2025"), Mapping)
        or data["holdout_2025"].get("state") != "LOCKED_NOT_ACCESSED"  # type: ignore[index]
        or not isinstance(feature, Mapping)
        or feature.get("standardized_displacement_threshold") != "1.5"
        or feature.get("threshold_search") is not False
        or feature.get("additional_features") != []
        or not isinstance(execution, Mapping)
        or execution.get("profit_target") is not False
        or execution.get("maximum_hold_minutes") != 60
        or execution.get("maximum_planned_initial_loss_usd") != "250"
        or not isinstance(inference, Mapping)
        or inference.get("primary_metric")
        != "MEAN_STRESS_COST_NET_PNL_USD_PER_COMPLETE_EVALUATION_SESSION"
        or inference.get("candidate_must_beat_every_required_baseline") is not True
        or not isinstance(multiplicity, Mapping)
        or multiplicity.get("prior_preregistered_penalty_count") != 105
        or multiplicity.get("this_counted_trial_number_floor") != 106
        or multiplicity.get("same_2018_2022_outcomes_do_not_reset_family") is not True
        or not isinstance(closure, Mapping)
        or closure.get("v15_v16_style_rescue_trials") is not False
        or len(closure.get("forbidden_incremental_successors", ())) != 8
        or not isinstance(authority, Mapping)
        or any(value is not False for value in authority.values())
        or not isinstance(bindings, Mapping)
        or any(
            sha256_file(root / str(path)) != digest
            for path, digest in bindings.items()
        )
    ):
        raise IntegrityError(
            "overnight inventory reversal preregistration is incomplete or drifted"
        )
    return protocol


def prepare_registration_documents(
    *, root: Path, registered_at_utc: datetime,
) -> tuple[dict[str, object], dict[str, object]]:
    """Build deterministic create-only documents; this function performs no write."""

    if (
        registered_at_utc.tzinfo is None
        or registered_at_utc.utcoffset() is None
        or registered_at_utc.utcoffset().total_seconds() != 0
    ):
        raise IntegrityError("registration time must be explicit UTC")
    protocol = load_overnight_inventory_reversal_preregistration(root=root)
    core = {
        "schema_version": "overnight_inventory_reversal_trial_registration/1.0.0",
        "state": "REGISTERED_BEFORE_NEW_HYPOTHESIS_OUTCOME_ACCESS",
        "preregistration_id": protocol["preregistration_id"],
        "preregistration_path": PREREGISTRATION_PATH.as_posix(),
        "preregistration_sha256": sha256_file(root / PREREGISTRATION_PATH),
        "hypothesis_id": protocol["hypothesis_id"],
        "registered_at_utc": registered_at_utc.astimezone(timezone.utc).isoformat(),
        "counted_trial_number_floor": 106,
        "multiplicity_family_id": protocol["multiplicity_policy"]["family_id"],  # type: ignore[index]
        "trusted_external_pre_outcome_anchor": False,
        "evaluation_authority": False,
        "source_row_access": False,
        "outcome_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "economics_evaluation": False,
        "holdout_or_forward_access": False,
        "provider_or_network_access": False,
        "publication": False,
        "trading": False,
    }
    trial_id = sha256_json(core)
    registration = {**core, "trial_id": trial_id}
    event_core = {
        "schema_version": "overnight_inventory_reversal_trial_event/1.0.0",
        "event_type": "DECLARED",
        "trial_id": trial_id,
        "preregistration_id": protocol["preregistration_id"],
        "registered_at_utc": core["registered_at_utc"],
        "trusted_external_pre_outcome_anchor": False,
        "evaluation_authority": False,
        "source_row_access": False,
        "outcome_row_access": False,
        "evaluation": False,
    }
    event = {**event_core, "event_id": sha256_json(event_core)}
    return registration, event


def registration_paths(trial_id: str) -> tuple[Path, Path]:
    if len(trial_id) != 64 or any(char not in "0123456789abcdef" for char in trial_id):
        raise IntegrityError("overnight-reversal trial identity is invalid")
    return REGISTRY_ROOT / f"{trial_id}.json", EVENT_ROOT / f"{trial_id}.json"


def load_preoutcome_correction(*, root: Path) -> dict[str, object]:
    """Verify the correction without changing the preserved base declaration."""

    correction = _object(root / CORRECTION_PATH)
    core = dict(correction)
    correction_id = core.pop("correction_id", None)
    authority = correction.get("authority")
    causal = correction.get("causal_feature_correction")
    inference = correction.get("inference_clarifications")
    if (
        correction_id != sha256_json(core)
        or correction.get("schema_version")
        != "overnight_inventory_reversal_preoutcome_correction/1.0.0"
        or correction.get("state")
        != "PREPARED_BEFORE_ANY_SOURCE_OR_OUTCOME_ACCESS"
        or correction.get("base_preregistration_id")
        != load_overnight_inventory_reversal_preregistration(root=root)[
            "preregistration_id"
        ]
        or correction.get("base_preregistration_sha256")
        != sha256_file(root / PREREGISTRATION_PATH)
        or correction.get("outcome_informed") is not False
        or correction.get("source_rows_opened") is not False
        or correction.get("outcome_rows_opened") is not False
        or correction.get("counted_trial_number_floor") != 106
        or correction.get("mechanism_or_parameter_search_changed") is not False
        or not isinstance(causal, Mapping)
        or causal.get("decision_at_chicago") != "08:30:05"
        or not isinstance(inference, Mapping)
        or inference.get("hac_lag_sessions") != 5
        or inference.get("stationary_bootstrap_resamples") != 10000
        or inference.get("stationary_bootstrap_seed") != 20260806
        or inference.get("global_history_alpha_ceiling") != "0.05_DIVIDED_BY_106"
        or not isinstance(authority, Mapping)
        or any(value is not False for value in authority.values())
    ):
        raise IntegrityError("overnight reversal pre-outcome correction drifted")
    return correction


def prepare_corrected_registration_documents(
    *, root: Path, registered_at_utc: datetime,
) -> tuple[dict[str, object], dict[str, object]]:
    """Build the outcome-free corrected declaration; this function does not write."""

    if (
        registered_at_utc.tzinfo is None
        or registered_at_utc.utcoffset() is None
        or registered_at_utc.utcoffset().total_seconds() != 0
    ):
        raise IntegrityError("corrected registration time must be explicit UTC")
    base = load_overnight_inventory_reversal_preregistration(root=root)
    correction = load_preoutcome_correction(root=root)
    core = {
        "schema_version": "overnight_inventory_reversal_trial_registration/1.1.0",
        "state": "REGISTERED_CORRECTED_BEFORE_SOURCE_OR_OUTCOME_ACCESS",
        "hypothesis_id": base["hypothesis_id"],
        "base_preregistration_id": base["preregistration_id"],
        "base_preregistration_sha256": sha256_file(root / PREREGISTRATION_PATH),
        "correction_id": correction["correction_id"],
        "correction_sha256": sha256_file(root / CORRECTION_PATH),
        # This is the implementation snapshot present when the immutable
        # corrected declaration was written.  The later execution plan binds
        # the final BUILT implementation separately before any outcome access.
        "execution_implementation_sha256": (
            CORRECTED_DECLARATION_EXECUTION_SHA256
        ),
        "supersedes_unevaluated_local_trial_id": correction[
            "superseded_local_trial_id"
        ],
        "counted_trial_number_floor": 106,
        "registered_at_utc": registered_at_utc.astimezone(timezone.utc).isoformat(),
        "trusted_external_pre_outcome_anchor": False,
        "evaluation_authority": False,
        "source_row_access": False,
        "outcome_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "economics_evaluation": False,
        "holdout_or_forward_access": False,
        "provider_or_network_access": False,
        "publication": False,
        "trading": False,
    }
    trial_id = sha256_json(core)
    registration = {**core, "trial_id": trial_id}
    event_core = {
        "schema_version": "overnight_inventory_reversal_trial_event/1.1.0",
        "event_type": "PRE_OUTCOME_CORRECTED_DECLARATION",
        "trial_id": trial_id,
        "supersedes_unevaluated_local_trial_id": correction[
            "superseded_local_trial_id"
        ],
        "correction_id": correction["correction_id"],
        "registered_at_utc": core["registered_at_utc"],
        "source_row_access": False,
        "outcome_row_access": False,
        "evaluation": False,
        "evaluation_authority": False,
        "trusted_external_pre_outcome_anchor": False,
    }
    event = {**event_core, "event_id": sha256_json(event_core)}
    return registration, event
