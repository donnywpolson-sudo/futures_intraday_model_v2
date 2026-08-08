"""Validation and create-only publication for the Standard-only Tier 1 protocol.

This lane changes source-governance only. It never opens historical market rows,
registers a trial, fits a model, or evaluates performance.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError


PROTOCOL_PATH = Path("configs/tier1_standard_only_trial_protocol.json")
PREDECESSOR_PATH = Path("configs/tier1_frozen_trial_protocol.json")
SOURCE_ADEQUACY_PATH = Path(
    "state/source_quality/tier1_frozen_source_adequacy/"
    "b3d8efbb010631922a944f13aff2de77e20d6775a2d98e5333994eca33cb5fbf.json"
)
PUBLICATION_ROOT = Path("state/source_quality/tier1_standard_only_source_policy")

PREDECESSOR_ID = "d647438200d54b60f9c7ddb69117adcd0abc23050b971dae542cda3fbdc21867"
PREDECESSOR_SHA256 = "7b6dcc144f52ef9feac7298dc87bbfbb6cb51f9f4628bfaaad773923d70a9662"
SOURCE_ADEQUACY_ID = "b3d8efbb010631922a944f13aff2de77e20d6775a2d98e5333994eca33cb5fbf"
SOURCE_ADEQUACY_SHA256 = "81057522b9038f3580f32ce51807b07435ebdd80f390ca812fad2c0cce010c9f"


def _load_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"unreadable governed artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"governed artifact is not an object: {path}")
    return value


def load_standard_only_protocol(*, root: Path) -> dict[str, object]:
    protocol = _load_object(root / PROTOCOL_PATH)
    core = dict(protocol)
    protocol_id = core.pop("protocol_id", None)
    source = protocol.get("source")
    correction = protocol.get("source_policy_correction")
    coverage = protocol.get("coverage")
    authority = protocol.get("authority")
    bindings = protocol.get("bindings")
    if (
        protocol_id != sha256_json(core)
        or protocol.get("schema_version") != "tier1_standard_only_trial_protocol/1.0.0"
        or protocol.get("state") != "PUBLISHED_PRE_REGISTRATION_PROTOCOL_ONLY"
        or not isinstance(source, Mapping)
        or source.get("historical_l1_bbo_dependency") is not False
        or source.get("terminal_execution_status_required_for_every_checkpoint") is not True
        or source.get("explicit_unavailable_rows_retained") is not True
        or source.get("missing_prices_or_fills_may_be_invented") is not False
        or not isinstance(correction, Mapping)
        or correction.get("calendar_open_checkpoints") != 15343
        or correction.get("complete_feature_windows") != 15288
        or correction.get("feature_complete_execution_complete") != 15254
        or correction.get("feature_complete_execution_unavailable") != 34
        or correction.get("selected_missing_execution_path") != "INCONCLUSIVE_DATA_OR_COVERAGE"
        or correction.get("runner_up_substitution") is not False
        or correction.get("zero_return_imputation_for_missing_execution_path") is not False
        or correction.get("applies_equally_to_candidate_and_every_active_baseline") is not True
        or not isinstance(coverage, Mapping)
        or coverage.get("terminal_checkpoint_ledger_rate") != "1.0"
        or coverage.get("terminal_execution_source_status_rate_before_registration") != "1.0"
        or coverage.get("selected_execution_path_rate_required_for_valid_evaluation") != "1.0"
        or not isinstance(authority, Mapping)
        or authority.get("trial_registration_authorized") is not False
        or authority.get("historical_execution_authorized") is not False
        or authority.get("provider_or_network_access") is not False
        or authority.get("credential_access") is not False
        or authority.get("holdout_or_forward_access") is not False
        or authority.get("stage_commit_push") is not False
        or authority.get("trading") is not False
        or not isinstance(bindings, Mapping) or not bindings
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise IntegrityError("Standard-only Tier 1 protocol is incomplete or drifted")
    return protocol


def build_source_policy_correction_record(*, root: Path) -> dict[str, object]:
    protocol = load_standard_only_protocol(root=root)
    if sha256_file(root / PREDECESSOR_PATH) != PREDECESSOR_SHA256:
        raise IntegrityError("predecessor protocol was not preserved")
    predecessor = _load_object(root / PREDECESSOR_PATH)
    if predecessor.get("protocol_id") != PREDECESSOR_ID:
        raise IntegrityError("predecessor protocol identity drifted")
    if sha256_file(root / SOURCE_ADEQUACY_PATH) != SOURCE_ADEQUACY_SHA256:
        raise IntegrityError("source-adequacy evidence drifted")
    adequacy = _load_object(root / SOURCE_ADEQUACY_PATH)
    adjudication = adequacy.get("adjudication")
    if not isinstance(adjudication, Mapping):
        raise IntegrityError("source-adequacy adjudication is absent")
    checks = adjudication.get("checks")
    overall = adjudication.get("overall")
    if (
        adequacy.get("record_id") != SOURCE_ADEQUACY_ID
        or not isinstance(checks, Mapping)
        or checks.get("terminal_open_checkpoint_ledger_complete") is not True
        or checks.get("every_execution_path_has_terminal_source_status") is not True
        or checks.get("incomplete_selected_execution_forces_trial_rejection") is not True
        or checks.get("overall_feature_rate_at_least_95_percent") is not True
        or checks.get("every_market_year_feature_rate_at_least_90_percent") is not True
        or checks.get("every_market_fold_role_feature_rate_at_least_90_percent_and_30_sessions") is not True
        or not isinstance(overall, Mapping)
        or overall.get("expected_open_checkpoints") != 15343
        or overall.get("complete_feature_windows") != 15288
    ):
        raise IntegrityError("source-adequacy evidence does not support the correction")
    core: dict[str, object] = {
        "schema_version": "tier1_standard_only_source_policy_correction/1.0.0",
        "state": "PUBLISHED_PRE_REGISTRATION_SOURCE_POLICY_CORRECTION_ONLY",
        "classification": "PRE_DATA_SOURCE_GOVERNANCE_CORRECTION_NOT_MODEL_TUNING",
        "predecessor_protocol": {
            "path": PREDECESSOR_PATH.as_posix(),
            "protocol_id": PREDECESSOR_ID,
            "sha256": PREDECESSOR_SHA256,
            "preserved": True,
            "disposition": "SUPERSEDED_BEFORE_REGISTRATION_PRESERVED_AS_AUDIT_EVIDENCE",
        },
        "replacement_protocol": {
            "path": PROTOCOL_PATH.as_posix(),
            "protocol_id": protocol["protocol_id"],
            "sha256": sha256_file(root / PROTOCOL_PATH),
            "numbered_successor": False,
            "trial_registered": False,
        },
        "evidence": {
            "source_adequacy_record_path": SOURCE_ADEQUACY_PATH.as_posix(),
            "source_adequacy_record_id": SOURCE_ADEQUACY_ID,
            "source_adequacy_record_sha256": SOURCE_ADEQUACY_SHA256,
            "calendar_open_checkpoints": 15343,
            "complete_feature_windows": 15288,
            "feature_complete_execution_complete": 15254,
            "feature_complete_execution_unavailable": 34,
        },
        "correction": {
            "historical_l1_bbo_required": False,
            "all_checkpoints_retained_with_terminal_source_status": True,
            "prediction_eligibility_independent_of_future_execution_availability": True,
            "missing_prices_or_fills_invented": False,
            "missing_execution_return_imputed_as_zero": False,
            "runner_up_substitution_after_missing_selected_path": False,
            "selected_missing_path_result": "INCONCLUSIVE_DATA_OR_COVERAGE",
            "same_rule_for_candidate_and_every_active_baseline": True,
            "promotion_possible_with_selected_missing_path": False,
        },
        "publication_scope": {
            "trial_registration": False,
            "historical_rows_read": False,
            "model_fit": False,
            "predictions_generated": False,
            "performance_evaluated": False,
            "provider_or_network_access": False,
            "credential_access": False,
            "holdout_2025_access": False,
            "active_data_mutation": False,
            "git_stage_commit_push": False,
            "trading_or_order_access": False,
        },
    }
    return {**core, "record_id": sha256_json(core)}


def publish_source_policy_correction(*, root: Path) -> Path:
    record = build_source_policy_correction_record(root=root)
    destination = root / PUBLICATION_ROOT / f"{record['record_id']}.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("x", encoding="utf-8", newline="\n") as handle:
            json.dump(record, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
    except FileExistsError as exc:
        raise IntegrityError("source-policy correction publication is not create-only") from exc
    return destination
