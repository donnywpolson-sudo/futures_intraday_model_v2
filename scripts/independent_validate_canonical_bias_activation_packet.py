"""Builder-independent validation of exact prospective active pointer bytes."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"


def digest(path: Path) -> str:
    return hashlib.sha256((ROOT / path).read_bytes()).hexdigest()


def identity(value: dict[str, object], field: str) -> str:
    core = {key: item for key, item in value.items() if key != field}
    return hashlib.sha256(canonical(core)[:-1]).hexdigest()


def load(path: Path, field: str | None = None) -> dict[str, object]:
    raw = (ROOT / path).read_bytes()
    value = json.loads(raw)
    assert isinstance(value, dict) and raw == canonical(value), path
    if field:
        assert value[field] == identity(value, field), path
    return value


def validate(plan_path: Path, packet_path: Path | None) -> dict[str, object]:
    plan = load(plan_path, "plan_id")
    assert plan["schema_version"] == "bias_robustness_canonical_activation_plan/2.0.0" and plan["state"] == "PREPARED_NOT_ACTIVE"
    assert subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip() == plan["git"]["branch"]
    assert subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip() == plan["git"]["head"]
    assert hashlib.sha256((ROOT / ".git/index").read_bytes()).hexdigest() == plan["git"]["index_sha256"]
    assert all(digest(Path(path)) == expected for path, expected in plan["source_hashes"].items())
    predecessor = plan["predecessor_packet"]
    assert digest(Path(predecessor["path"])) == predecessor["sha256"] and load(Path(predecessor["path"]), "packet_id")["packet_id"] == predecessor["packet_id"]
    bundle = load(Path(plan["bundle"]["path"]), "bundle_id")
    assert digest(Path(plan["bundle"]["path"])) == plan["bundle"]["sha256"] and bundle["bundle_id"] == plan["bundle"]["id"]

    decoded: dict[str, dict[str, object]] = {}
    for relative, item in plan["pointer_successors"].items():
        predecessor_bytes = base64.b64decode(item["predecessor_bytes_base64"], validate=True)
        successor_bytes = base64.b64decode(item["successor_bytes_base64"], validate=True)
        assert (ROOT / relative).read_bytes() == predecessor_bytes
        assert hashlib.sha256(predecessor_bytes).hexdigest() == item["predecessor_sha256"]
        assert hashlib.sha256(successor_bytes).hexdigest() == item["successor_sha256"] and len(successor_bytes) == item["successor_byte_count"]
        value = json.loads(successor_bytes)
        assert isinstance(value, dict) and successor_bytes == canonical(value) and value["pointer_id"] == identity(value, "pointer_id")
        decoded[relative] = value
    ladder_path = "configs/active_alpha_research_ladder.json"
    historical_path = "configs/active_alpha_historical_research_design.json"
    execution_path = "configs/active_alpha_ladder_execution_design.json"
    ladder, historical, execution = decoded[ladder_path], decoded[historical_path], decoded[execution_path]
    artifacts = bundle["artifacts"]
    assert ladder["contract_id"] == artifacts["ladder"]["id"] and ladder["profile_id"] == artifacts["profile"]["id"]
    assert historical["schema_version"] == "active_alpha_historical_research_design/3.0.0"
    assert execution["schema_version"] == "active_alpha_ladder_execution_design/2.0.0"
    assert historical["ladder_contract_id"] == execution["ladder_contract_id"] == ladder["contract_id"]
    assert historical["ladder_profile_id"] == execution["ladder_profile_id"] == ladder["profile_id"]
    assert historical["stage_2_5_binding"]["contract_id"] == execution["stage_2_5_contract_id"] == artifacts["stage_2_5"]["id"]
    assert plan["write_order"] == [historical_path, execution_path, ladder_path] and plan["rollback_order"] == list(reversed(plan["write_order"])) and plan["ladder_pointer_written_last"] is True
    assert all(value is False for value in plan["authority"].values())

    copies = {Path(item["path"]) for item in artifacts.values()} | {
        Path("state/causal_search_access_contract_registry/5bff4a0914247a49db2f552caa199232490bab5d277051d468db01c539d11741/contract.json"),
        Path("configs/bounded_alpha_discovery_contract_v1.json"),
        Path("state/alpha_ladder_macro_successor_terminal_closure/7ff6ded9e3cbb45b17cb127783f3065f4e9f6f80ed38c3c623b1c15319c35cab/closure.json"),
    }
    with tempfile.TemporaryDirectory(prefix="independent-bias-activation-") as temporary:
        candidate = Path(temporary)
        for relative in copies:
            target = candidate / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, target)
        for relative, value in decoded.items():
            target = candidate / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(canonical(value))
        from futures_rebuild.bias_robustness_successor import require_stage3_active_authority
        active_contract, active_profile, active_stage = require_stage3_active_authority(candidate)
        assert active_contract["contract_id"] == ladder["contract_id"] and active_profile["profile_id"] == ladder["profile_id"] and active_stage["contract_id"] == artifacts["stage_2_5"]["id"]

    from futures_rebuild.repository_surface import validate_active_alpha_authority_candidate
    required = validate_active_alpha_authority_candidate(ROOT, ladder)["required_paths"]
    assert "configs/bounded_alpha_discovery_contract_v1.json" in required
    assert not any(path.startswith("data/") for path in required)

    checks = ["plan_identity", "source_and_git_preimages", "predecessor_packet", "bundle", "exact_predecessor_bytes", "exact_successor_bytes", "pointer_identities", "dependency_bindings", "pointer_last_order", "prospective_active_state", "prospective_repository_surface", "row_opener_invocations_zero"]
    if packet_path is not None:
        packet = load(packet_path, "packet_id")
        assert packet["activation_plan"]["plan_id"] == plan["plan_id"] and packet["activation_plan"]["sha256"] == digest(plan_path)
        assert packet["pointer_successors_sha256"] == hashlib.sha256(canonical(plan["pointer_successors"])[:-1]).hexdigest()
        assert packet["write_order"] == plan["write_order"] and packet["ladder_pointer_written_last"] is True
        checks.append("final_packet")
    report = {"schema_version": "bias_robustness_canonical_activation_validation/1.0.0", "status": "PASS_EXACT_POINTER_SUCCESSOR_PREPARATION", "plan_id": plan["plan_id"], "plan_path": plan_path.as_posix(), "plan_sha256": digest(plan_path), "checks": checks, "builder_called": False, "fresh_process": True, "row_opener_invocations": 0, "protected_access_performed": False}
    report["validation_id"] = identity(report, "validation_id")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--packet", type=Path)
    parser.add_argument("--write-certificate", action="store_true")
    args = parser.parse_args()
    report = validate(args.plan, args.packet)
    if args.write_certificate:
        from futures_rebuild.canonical import write_new_canonical_authority
        path = Path("state/bias_robustness_activation_validation_registry") / str(report["validation_id"]) / "certificate.json"
        write_new_canonical_authority(ROOT / path, report)
        print(json.dumps({"validation_id": report["validation_id"], "path": path.as_posix(), "sha256": digest(path)}, sort_keys=True, separators=(",", ":")))
    else:
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
