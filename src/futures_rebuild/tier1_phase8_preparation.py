"""Prepare, but never execute, the first Tier 1 Phase 8 evaluation."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .canonical import sha256_file, sha256_json
from .errors import IntegrityError
from .high_risk import confirmation_required
from .tier1_phase8_evaluation_config import load_tier1_phase8_evaluation_config

TRIAL_ID = "5b30b629cb8ecdd1560cdc3d8bc7cadf3d9d5216719e5bb37f10a89abe05fabf"
AUDIT_RELEASE_ID = "efb8943fccb79b12b43f603e5b2078b847a38d868aa79fd6e1ab15fa35a638d5"
INDEX_RELEASE_ID = "2f84c409233f44140b709a80ba41935083eccf04c7d6c06f80be1ac88d2f8c02"
REQUIRED_SETTINGS = ("fees", "spread_slippage", "delay", "position_sizing", "margin", "concentration_limits", "baseline", "pass_fail_metrics")

def _json(path: Path) -> Mapping[str, object]:
    try: value=json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc: raise IntegrityError(f"Phase 8 preparation cannot read {path.name}") from exc
    if not isinstance(value, dict): raise IntegrityError("Phase 8 preparation metadata must be an object")
    return value

@dataclass(frozen=True)
class Tier1Phase8Preparation:
    trial_id: str
    index_release_id: str
    audit_release_id: str
    pairs: tuple[dict[str, object], ...]
    rulebook_hash: str
    risk_profile_id: str
    risk_profile_hash: str
    evaluation_config_hash: str
    execution_cost_status: str
    unresolved_settings: tuple[str, ...]

    @property
    def evaluation_ready(self) -> bool: return not self.unresolved_settings

    def declaration(self) -> dict[str, object]:
        exact_costs = self.execution_cost_status == "EXACT_APEX_TRADOVATE_SCHEDULE"
        core={"schema_version":"tier1_phase8_evaluation_preparation/1.0.0","phase":8,"trial_id":self.trial_id,"index_release_id":self.index_release_id,"audit_release_id":self.audit_release_id,"input_pairs":list(self.pairs),"rulebook_hash":self.rulebook_hash,"risk_profile_id":self.risk_profile_id,"risk_profile_hash":self.risk_profile_hash,"evaluation_config_hash":self.evaluation_config_hash,"execution_cost_status":self.execution_cost_status,"exact_apex_live_costs_verified":exact_costs,"permitted_result_label":"EXACT_APEX_LIVE_COSTS" if exact_costs else "PROVISIONAL_EXECUTION_COSTS","unresolved_settings":list(self.unresolved_settings),"evaluation_ready":self.evaluation_ready,"forbidden_without_approval":["provider_access","row_read","model_fit","prediction_generation","holdout_forward_access","trading","git","installation"]}
        return {**core,"preparation_id":sha256_json(core)}

def prepare_tier1_phase8(*, root: Path, settings: Mapping[str, object] | None = None) -> Tier1Phase8Preparation:
    trial=_json(root/"state"/"trial_registry"/"phase6_prediction_only"/f"{TRIAL_ID}.json")
    if trial.get("state")!="REGISTERED_BEFORE_OUTCOME_OPEN" or not isinstance(trial.get("input_pairs"),list) or len(trial["input_pairs"])!=20: raise IntegrityError("Phase 8 requires the fixed registered 20-pair Phase 6 trial")
    index=_json(root/"manifests"/"data_releases"/"reference"/f"{INDEX_RELEASE_ID}.json")
    if index.get("release_kind")!="phase8_actual_contract_economics_index": raise IntegrityError("Phase 8 index manifest is invalid")
    audit=_json(root/"manifests"/"data_releases"/"reference"/f"{AUDIT_RELEASE_ID}.json")
    if audit.get("metadata",{}).get("status")!="PASSED": raise IntegrityError("Phase 8 requires the passing all-market audit")
    rules=root/"configs"/"contract_economics_rules.json"
    risk_profile=root/"configs"/"prop_firm_risk_profile.json"
    profile=_json(risk_profile)
    profile_id=profile.get("active_profile_id")
    profiles=profile.get("profiles")
    if not isinstance(profile_id,str) or not isinstance(profiles,dict) or not isinstance(profiles.get(profile_id),dict):
        raise IntegrityError("Phase 8 requires a valid active prop-firm risk profile")
    evaluation_config, evaluation_config_hash = load_tier1_phase8_evaluation_config(root=root)
    configured_settings = {
        "delay": evaluation_config["delay"],
        "position_sizing": evaluation_config["position_sizing"],
        "margin": evaluation_config["margin"],
        "concentration_limits": evaluation_config["concentration_limits"],
        "baseline": evaluation_config["baselines"],
        "pass_fail_metrics": evaluation_config["pass_fail_metrics"],
    }
    cost_status = evaluation_config["costs"].get("assumption_status")
    if cost_status in {"PROVISIONAL_APEX_TRADOVATE_ZN_FEE", "EXACT_APEX_TRADOVATE_SCHEDULE"}:
        configured_settings["fees"] = evaluation_config["costs"]
        configured_settings["spread_slippage"] = evaluation_config["costs"]
    effective_settings = {**configured_settings, **(settings or {})}
    if cost_status not in {"PROVISIONAL_APEX_TRADOVATE_ZN_FEE", "EXACT_APEX_TRADOVATE_SCHEDULE"}:
        effective_settings.pop("fees", None)
        effective_settings.pop("spread_slippage", None)
    missing=tuple(name for name in REQUIRED_SETTINGS if name not in effective_settings)
    if not isinstance(cost_status, str):
        raise IntegrityError("Phase 8 execution-cost status is invalid")
    return Tier1Phase8Preparation(TRIAL_ID,INDEX_RELEASE_ID,AUDIT_RELEASE_ID,tuple(trial["input_pairs"]),sha256_file(rules),profile_id,sha256_file(risk_profile),evaluation_config_hash,cost_status,missing)

def prepare_phase8_confirmation(preparation: Tier1Phase8Preparation) -> dict[str, object]:
    if not preparation.evaluation_ready: raise IntegrityError("Phase 8 economics/risk settings remain unresolved: "+", ".join(preparation.unresolved_settings))
    label = "provisional execution costs" if preparation.execution_cost_status == "PROVISIONAL_APEX_TRADOVATE_ZN_FEE" else "exact Apex live costs"
    return confirmation_required(f"Evaluate fixed Tier 1 predictions under pinned net economics ({label})",scope={"markets":"ES, CL, ZN, 6E","period":"2018 through 2022","market_years":"20","provider_calls":"0","model_fit":"0","result_label":"PROVISIONAL_EXECUTION_COSTS" if preparation.execution_cost_status == "PROVISIONAL_APEX_TRADOVATE_ZN_FEE" else "EXACT_APEX_LIVE_COSTS"},outputs=("one immutable model-selection report","one immutable risk report"),preservation="Read only pinned local releases; preserve all accepted releases and do not trade, commit, push, install, or alter active data.")
