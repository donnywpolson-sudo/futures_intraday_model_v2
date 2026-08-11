"""Provider-neutral Phase 8 preparation bound to immutable runtime identity."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from .canonical import sha256_file, sha256_json
from .errors import ContractError
from .prop_firm_account_runtime import (
    build_runtime_identity,
    load_runtime_bindings,
    mapping,
    resolve_execution_instrument,
)
from .prop_firm_eod_risk import PROFILE_RELATIVE_PATH, load_active_profile


CONFIG_RELATIVE_PATH = Path("configs/prop_firm_phase8_evaluation.json")


def _mapping(value: object, *, name: str) -> Mapping[str, object]:
    return mapping(value, name=name)


def load_phase8_config(*, root: Path) -> Mapping[str, object]:
    path = root / CONFIG_RELATIVE_PATH
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ContractError(f"cannot read prop-firm Phase 8 config: {path}") from exc
    config = _mapping(config, name="prop-firm Phase 8 config")
    if config.get("schema_version") != "prop_firm_phase8_evaluation/2.0.0":
        raise ContractError("prop-firm Phase 8 schema is unsupported")
    if config.get("phase") != 8:
        raise ContractError("model evaluation must remain Phase 8")
    if config.get("profile_document_path") != PROFILE_RELATIVE_PATH.as_posix():
        raise ContractError("Phase 8 must resolve the generic selected profile")
    if config.get("account_stage") not in {"evaluation", "sim_funded", "live"}:
        raise ContractError("Phase 8 requires an explicit prop-firm account stage")
    roots = config.get("signal_roots")
    if not isinstance(roots, list) or not roots or not all(
        isinstance(root_name, str) and root_name for root_name in roots
    ):
        raise ContractError("Phase 8 signal roots must be a nonempty string list")
    if len(set(roots)) != len(roots):
        raise ContractError("Phase 8 signal roots must be unique")
    labels = _mapping(config.get("evaluation_result_labels"), name="result labels")
    expected = {
        "exact": "EXACT_SELECTED_PROVIDER_ACCOUNT_COSTS",
        "provisional": "PROVISIONAL_SELECTED_PROVIDER_ACCOUNT_COSTS",
        "unresolved": "UNRESOLVED_SELECTED_PROVIDER_ACCOUNT_COSTS",
    }
    if dict(labels) != expected:
        raise ContractError("provider/account result labels drifted")
    authority = _mapping(config.get("authority"), name="authority")
    if any(value is not False for value in authority.values()):
        raise ContractError("Phase 8 preparation grants no operating authority")
    return config


def build_phase8_preparation(*, root: Path) -> dict[str, object]:
    """Bind Phase 8 to profile/policy/mapping/cost/payout hashes without rows."""

    config = load_phase8_config(root=root)
    account_stage = str(config["account_stage"])
    profile_id, profile = load_active_profile(root=root, account_stage=account_stage)
    identity = build_runtime_identity(
        root=root, profile_id=profile_id, account_stage=account_stage
    )
    bindings = load_runtime_bindings(root=root, profile=profile)
    _, instrument_mapping = bindings["mapping"]
    _, costs = bindings["cost"]

    dispositions: dict[str, dict[str, object]] = {}
    enabled_roots: list[str] = []
    for signal_root in config["signal_roots"]:
        try:
            instrument = resolve_execution_instrument(instrument_mapping, signal_root)
        except ContractError as exc:
            dispositions[signal_root] = {
                "enabled": False,
                "disposition": "DISABLED_FAIL_CLOSED",
                "reason": str(exc),
            }
        else:
            enabled_roots.append(signal_root)
            dispositions[signal_root] = {
                "enabled": True,
                "execution_symbol": instrument["execution_symbol"],
                "execution_model_status": instrument["execution_model_status"],
            }

    exact = costs.get("exact_provider_account_costs_verified")
    if not isinstance(exact, bool):
        raise ContractError("exact_provider_account_costs_verified must be boolean")
    fee_map = _mapping(costs.get("round_turn_commission_usd"), name="round-turn fees")
    if exact and set(fee_map) != {
        dispositions[root_name]["execution_symbol"] for root_name in enabled_roots
    }:
        raise ContractError("exact selected-platform fees must cover every enabled micro")
    if exact:
        label_key = "exact"
    elif fee_map:
        label_key = "provisional"
    else:
        label_key = "unresolved"
    labels = _mapping(config.get("evaluation_result_labels"), name="result labels")
    result_label = labels[label_key]

    blockers = list(profile.get("readiness_blockers", ()))
    blockers.extend(item for item in costs.get("blockers", ()) if item not in blockers)
    if any(not dispositions[root_name]["enabled"] for root_name in dispositions):
        blockers.append("ONE_OR_MORE_SIGNAL_ROOTS_DISABLED_IN_MICRO_ONLY_MODE")
    core: dict[str, object] = {
        "schema_version": "prop_firm_phase8_preparation/2.0.0",
        "phase": 8,
        "state": "PREPARED_MODEL_EVALUATION_NOT_AUTHORIZED_PRODUCTION_BLOCKED",
        "profile_id": profile_id,
        "profile_document_path": PROFILE_RELATIVE_PATH.as_posix(),
        "profile_document_sha256": sha256_file(root / PROFILE_RELATIVE_PATH),
        "profile_hash": identity["profile_hash"],
        "provider_id": profile["provider_id"],
        "account_program": profile["program"],
        "account_stage": account_stage,
        "rules_as_of": identity["rules_as_of"],
        "signal_roots": list(config["signal_roots"]),
        "enabled_signal_roots": enabled_roots,
        "execution_dispositions": dispositions,
        "round_turn_commission_usd": {
            dispositions[root_name]["execution_symbol"]: fee_map.get(
                dispositions[root_name]["execution_symbol"], "UNSET"
            )
            for root_name in enabled_roots
        },
        "cost_status": costs["schedule_status"],
        "exact_provider_account_costs_verified": exact,
        "exact_live_costs_verified": exact,
        "evaluation_result_label": result_label,
        "runtime_identity": identity,
        "production_readiness": False,
        "readiness_blockers": sorted(set(blockers)),
        "official_sources": profile["official_sources"],
        "confirmation_label": (
            "Prepare Phase 8 model evaluation using the selected provider/account "
            f"stage ({result_label}); no evaluation authority granted"
        ),
        "authority": dict(_mapping(config.get("authority"), name="authority")),
    }
    return {**core, "preparation_id": sha256_json(core)}


def validate_phase8_result_label(
    *, preparation: Mapping[str, object], observed_label: str
) -> None:
    if observed_label != preparation.get("evaluation_result_label"):
        raise ContractError("Phase 8 result label does not match selected-profile costs")


__all__ = [
    "CONFIG_RELATIVE_PATH",
    "build_phase8_preparation",
    "load_phase8_config",
    "validate_phase8_result_label",
]
