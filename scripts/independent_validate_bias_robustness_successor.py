"""Independent metadata-only validation for the inactive bias successor."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
STAGE_PATH = Path("state/causal_search_access_contract_registry/5bff4a0914247a49db2f552caa199232490bab5d277051d468db01c539d11741/contract.json")
LADDER_ROOT = Path("state/alpha_ladder_registry/2bc9e04a5f53b57c1c70517ee3b098b05784fd4b918b00596e7a724e83e11c87")
DESIGN_ROOT = Path("state/bias_robustness_successor_registry/df2980b778cdbf22b1f9529dc27dae09e21ade47bfa972262f5e6db7ee11076d")
EXPECTED_HASHES = {
    STAGE_PATH: "3c1070ba3c70498c0f66bfebb2cd428a92853d94074b8a0fa1ff28f5b83d0f96",
    LADDER_ROOT / "universe_contract.json": "0ea419a34957bd30b8bd7345001952dc470ad6f8a3178beef59872e8c7eb0a73",
    LADDER_ROOT / "alpha_tiered.yaml": "6c7aac40bdb11176c502354a9336c1df7c417c6166441a88fd8344a0a1b1d99a",
    DESIGN_ROOT / "historical_design.json": "459fc63ea7a77e7696b7b5e35fc008b955ef88ea4cbc4614fb55964025b076a0",
    DESIGN_ROOT / "execution_design.json": "d6f30dd6fe7da7efc53f4314450eff4a33cee4b48580fbf5024277543408cf0d",
    Path("configs/active_alpha_research_ladder.json"): "ed7d1daa9c3d16f4607652fd5ef566841443903c82b481c40ac5af455108f36b",
    Path("configs/active_alpha_historical_research_design.json"): "f4c78f8f74237441555387d56810fc2030c33b79122c687ec811dda93c680979",
    Path("configs/active_alpha_ladder_execution_design.json"): "9b50abbbfbf7ac089fb5f6192bfe88dd7bf38329e805588aa91dcc40f2e6d982",
    Path("data/active/catalog.json"): "927e596928f64677b6f170827230d012388e800c83fd20c1bd41c4b16c58a74e",
    Path("state/final_evaluation_session_manifest_registry/ad1baab30b52fdda95e72777a4930ea18bd78037925cc36a27594f484c876549/final_252_session_manifest.json"): "afb99958c3aa17e617f69ad1d1e80200169f4296ffcdbae9c003b5421d0f5040",
    Path("state/causal_search_access_contract_registry/1ddbbc62f08bf4905a4cfb0efea5f8f39bbbbdf9003869eb87e780b59bc58d5e/contract.json"): "abd4300b8de1986a9a6fa6fdbe9c0a90a0514335b215102300e9f5b523b19515",
    Path("state/alpha_ladder_registry/69ba41a7fc2fae72bb771d043c7f78ceeb9e36165808c8988983a7ec0093fa16/universe_contract.json"): "a70589d5e16756db94a14cd18d188233daf3e73f86237a325457025512244e2d",
}


def digest(path: Path) -> str:
    return hashlib.sha256((ROOT / path).read_bytes()).hexdigest()


def identity(payload: dict[str, object], field: str) -> str:
    core = {key: value for key, value in payload.items() if key != field}
    raw = json.dumps(core, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(raw).hexdigest()


def load_json(path: Path) -> dict[str, object]:
    payload = json.loads((ROOT / path).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def main() -> int:
    checks: list[str] = []
    for path, expected in EXPECTED_HASHES.items():
        assert digest(path) == expected, path
        checks.append(f"sha256:{path.as_posix()}")

    stage = load_json(STAGE_PATH)
    ladder = load_json(LADDER_ROOT / "universe_contract.json")
    historical = load_json(DESIGN_ROOT / "historical_design.json")
    execution = load_json(DESIGN_ROOT / "execution_design.json")
    profile = yaml.safe_load((ROOT / LADDER_ROOT / "alpha_tiered.yaml").read_text(encoding="utf-8"))
    assert isinstance(profile, dict)
    assert set(stage) == {"schema_version", "state", "authority", "predecessor", "protocol_binding", "question", "timing", "scope", "search", "macro_disposition", "stage_order", "reset", "evidence_classes", "challenge", "refit", "claim_gate", "forward_gate", "contract_id"}
    assert identity(stage, "contract_id") == stage["contract_id"]
    protocol_binding = stage["protocol_binding"]
    assert isinstance(protocol_binding, dict) and digest(Path(str(protocol_binding["path"]))) == protocol_binding["sha256"]
    protocol = load_json(Path(str(protocol_binding["path"])))
    assert protocol["contract_id"] == protocol_binding["contract_id"]
    assert identity(ladder, "contract_id") == ladder["contract_id"]
    assert identity(profile, "profile_id") == profile["profile_id"]
    assert ladder["schema_version"] == "alpha_research_ladder_contract/8.0.0"
    assert profile["schema_version"] == "alpha_research_ladder_profile/8.0.0"
    assert stage["schema_version"] == "causal_search_access_contract/2.0.0"
    assert stage["stage_order"] == ["2.5", "3", "4", "5", "6", "SYNTHETIC_ENGINEERING", "ES_PILOT", "SCOPE_MATCHED_REPLICATION", "10.5", "10.6", "11", "12"]
    assert stage["question"]["claim_ceiling"] == "ROBUST_PREDICTIVE_ASSOCIATION"
    assert ladder["macro_disposition"] == profile["macro_disposition"] == historical["macro_disposition"]["state"] == execution["macro_disposition"] == "NO_ELIGIBLE_MACRO_DIVERSIFIER"
    assert historical["stage_2_5_binding"]["contract_id"] == execution["stage_2_5_contract_id"] == stage["contract_id"]
    assert historical["ladder_binding"]["contract_id"] == execution["ladder_contract_id"] == ladder["contract_id"]
    assert historical["challenge_order"] == "ONE_SCOPE_WIDE_AFTER_LAST_APPLICABLE_REPLICATION"
    assert historical["refit_order"] == "AFTER_PASSING_CHALLENGE_NO_EVALUATION"
    assert historical["development_interval"] == ["2018-01-01T00:00:00Z", "2023-01-01T00:00:00Z"]
    assert historical["challenge_interval"] == ["2023-01-01T00:00:00Z", "2025-07-13T22:00:00Z"]
    assert historical["refit_interval"] == ["2018-01-01T00:00:00Z", "2025-07-13T22:00:00Z"]
    assert all(value is False for value in historical["authority"].values())
    assert all(value is False for value in execution["authority"].values())
    assert load_json(Path("configs/active_alpha_research_ladder.json"))["contract_id"] != ladder["contract_id"]
    workflow = (ROOT / "CURRENT_WORKFLOW.md").read_text(encoding="utf-8")
    outline = (ROOT / "PROJECT_OUTLINE.md").read_text(encoding="utf-8")
    for text in (workflow, outline):
        assert "BLOCKED_STAGE_2_5_ACTIVE_AUTHORITY_REQUIRED" in text
        assert "inactive" in text.lower()
    checks.extend(("exact_keys", "internal_ids", "authority_chain", "stage_order", "macro_state", "time_boundaries", "claim_ceiling", "active_preimages", "all_authority_false"))
    report = {"schema_version": "bias_robustness_independent_validation/1.0.0", "status": "PASS", "checks": checks, "protected_access_performed": False, "builder_called": False}
    report["validation_id"] = identity(report, "validation_id")
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
