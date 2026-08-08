"""Validation for the one unversioned frozen Tier 1 protocol."""

from __future__ import annotations

import json
from pathlib import Path

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError


PROTOCOL_PATH = Path("configs/tier1_frozen_trial_protocol.json")
SYNTHETIC_VERIFICATION_PATH = Path("configs/tier1_frozen_synthetic_verification.json")


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("frozen Tier 1 artifact is unreadable") from exc
    if not isinstance(value, dict):
        raise IntegrityError("frozen Tier 1 artifact is not an object")
    return value


def load_frozen_trial_protocol(*, root: Path) -> dict[str, object]:
    try:
        protocol = _object(root / PROTOCOL_PATH)
    except IntegrityError as exc:
        raise IntegrityError("frozen Tier 1 protocol is unreadable") from exc
    core = dict(protocol)
    protocol_id = core.pop("protocol_id", None)
    bindings = protocol.get("bindings")
    source = protocol.get("source")
    decision = protocol.get("decision_time")
    position = protocol.get("position_and_risk")
    promotion = protocol.get("promotion")
    authority = protocol.get("authority")
    if (
        protocol_id != sha256_json(core)
        or protocol.get("schema_version") != "tier1_frozen_trial_protocol/1.0.0"
        or protocol.get("state") != "PREPARED_NOT_REGISTERED_SOURCE_ADEQUACY_PENDING"
        or not isinstance(bindings, dict) or not bindings
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
        or not isinstance(source, dict)
        or source.get("selected_release_count") != 20
        or source.get("source_adequacy_record_id") is not None
        or source.get("every_feature_complete_checkpoint_requires_complete_execution_path") is not True
        or not isinstance(decision, dict)
        or decision.get("feature_available_at_must_not_exceed_decision") is not True
        or decision.get("prediction_eligibility_independent_of_future_outcome_availability") is not True
        or not isinstance(position, dict)
        or position.get("continuous_drawdown_threshold_usd") != "1500"
        or not isinstance(promotion, dict)
        or promotion.get("stress_net_pnl_positive") is not True
        or promotion.get("beats_true_zero_and_every_independently_simulated_baseline") is not True
        or promotion.get("maximum_continuous_drawdown_usd") != "1500"
        or promotion.get("live_readiness_claim") is not False
        or not isinstance(authority, dict)
        or authority.get("provider_or_network_access") is not False
        or authority.get("holdout_or_forward_access") is not False
        or authority.get("stage_commit_push") is not False
        or authority.get("trading") is not False
    ):
        raise IntegrityError("frozen Tier 1 protocol is incomplete or drifted")
    return protocol


def load_frozen_synthetic_verification(*, root: Path) -> dict[str, object]:
    verification = _object(root / SYNTHETIC_VERIFICATION_PATH)
    core = dict(verification)
    verification_id = core.pop("verification_id", None)
    paths = sorted(
        path for path in (root / "tests").glob("test_tier1_*.py")
        if path.name.startswith((
            "test_tier1_bracket", "test_tier1_frozen", "test_tier1_preexecution",
        ))
    )
    tree = sha256_json({
        "files": [
            {"path": path.relative_to(root).as_posix(), "sha256": sha256_file(path)}
            for path in paths
        ]
    })
    results = verification.get("applicable_results")
    if (
        verification_id != sha256_json(core)
        or verification.get("schema_version")
        != "tier1_frozen_synthetic_verification/1.0.0"
        or verification.get("state") != "PREPARED_NOT_PUBLISHED"
        or verification.get("test_file_count") != len(paths)
        or verification.get("test_tree_id") != tree
        or not isinstance(results, dict)
        or results.get("passed") != 262
        or results.get("failed") != 0
        or verification.get("complete_synthetic_source_to_crossfit_pipeline") is not True
        or verification.get("independent_baseline_universes_and_account_paths") is not True
        or verification.get("future_outcome_mutation_leaves_prediction_eligibility_unchanged") is not True
        or verification.get("flat_no_trade_is_true_zero") is not True
        or any(verification.get(field) is not False for field in (
            "historical_source_rows_opened", "provider_or_network_access",
            "holdout_or_forward_access", "model_fit_on_real_data",
            "historical_performance_evaluation", "trading",
        ))
    ):
        raise IntegrityError("frozen Tier 1 synthetic verification is incomplete or drifted")
    return verification
