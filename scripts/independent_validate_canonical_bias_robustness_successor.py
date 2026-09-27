"""Fresh-process, metadata-only validation of the corrected inactive successor."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OLD_STAGE = Path("state/causal_search_access_contract_registry/5bff4a0914247a49db2f552caa199232490bab5d277051d468db01c539d11741/contract.json")
OLD_PACKET = Path("reports/post_stage_2_5_bias_robustness_successor/activation_review_packet.json")
OLD_BUNDLE_ID = "b6ad4c5c178dbacea877c5a761186998569c62f4e73d95600ebd81f9c788ab57"
ACTIVE = {
    "configs/active_alpha_research_ladder.json": "ed7d1daa9c3d16f4607652fd5ef566841443903c82b481c40ac5af455108f36b",
    "configs/active_alpha_historical_research_design.json": "f4c78f8f74237441555387d56810fc2030c33b79122c687ec811dda93c680979",
    "configs/active_alpha_ladder_execution_design.json": "9b50abbbfbf7ac089fb5f6192bfe88dd7bf38329e805588aa91dcc40f2e6d982",
    "data/active/catalog.json": "927e596928f64677b6f170827230d012388e800c83fd20c1bd41c4b16c58a74e",
    "state/final_evaluation_session_manifest_registry/ad1baab30b52fdda95e72777a4930ea18bd78037925cc36a27594f484c876549/final_252_session_manifest.json": "afb99958c3aa17e617f69ad1d1e80200169f4296ffcdbae9c003b5421d0f5040",
}
VALIDATION_SOURCES = (
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/alpha_research_ladder.py",
    "src/futures_rebuild/bias_robustness_successor.py",
    "src/futures_rebuild/causal_search_access_contract.py",
    "scripts/prepare_canonical_bias_robustness_successor.py",
    "scripts/independent_validate_canonical_bias_robustness_successor.py",
    "tests/test_causal_search_access_contract.py",
    "tests/test_canonical_authority_successor.py",
)


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"


def digest(path: Path) -> str:
    return hashlib.sha256((ROOT / path).read_bytes()).hexdigest()


def identity(value: dict[str, object], field: str) -> str:
    return hashlib.sha256(canonical({key: item for key, item in value.items() if key != field})[:-1]).hexdigest()


def load_canonical(path: Path, field: str | None = None) -> dict[str, object]:
    raw = (ROOT / path).read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf") and b"\r" not in raw and raw.endswith(b"\n") and not raw.endswith(b"\n\n"), path
    value = json.loads(raw)
    assert isinstance(value, dict) and raw == canonical(value), path
    if field:
        assert value[field] == identity(value, field), path
    return value


def validate(bundle_path: Path, packet_path: Path | None) -> dict[str, object]:
    checks: list[str] = []
    old_raw = (ROOT / OLD_STAGE).read_bytes()
    old_stage = json.loads(old_raw)
    assert hashlib.sha256(old_raw).hexdigest() == "3c1070ba3c70498c0f66bfebb2cd428a92853d94074b8a0fa1ff28f5b83d0f96"
    assert old_raw == canonical(old_stage) + b"\n"
    old_packet = load_canonical(OLD_PACKET)
    assert old_packet["successor"]["stage_2_5"]["sha256"] == hashlib.sha256(old_raw).hexdigest()
    checks += ["defect_reproduced", "failed_packet_matches", "non_drift_classification"]

    bundle = load_canonical(bundle_path, "bundle_id")
    assert bundle["supersedes_bundle_id"] == OLD_BUNDLE_ID
    loaded: dict[str, dict[str, object]] = {}
    fields = {"stage_2_5": "contract_id", "ladder": "contract_id", "profile": "profile_id", "historical_design": "design_id", "execution_design": "execution_design_id"}
    for name, item in bundle["artifacts"].items():
        path = Path(item["path"])
        value = load_canonical(path, fields[name])
        assert digest(path) == item["sha256"] and value[fields[name]] == item["id"] and value["schema_version"] == item["schema_version"]
        expected_dir = item["id"] if name in {"stage_2_5", "ladder", "historical_design"} else bundle["artifacts"]["ladder" if name == "profile" else "historical_design"]["id"]
        assert expected_dir in path.parts
        loaded[name] = value
    checks += ["canonical_bytes", "internal_ids", "file_hashes", "registry_paths"]

    stage, ladder, profile = loaded["stage_2_5"], loaded["ladder"], loaded["profile"]
    historical, execution = loaded["historical_design"], loaded["execution_design"]
    stage_binding = ladder["causal_search_access_contract"]["contract_binding"]
    assert stage_binding["contract_id"] == stage["contract_id"] and stage_binding["sha256"] == bundle["artifacts"]["stage_2_5"]["sha256"]
    assert profile["causal_search_access_contract"]["contract_binding"] == stage_binding
    assert historical["stage_2_5_binding"] == stage_binding and execution["stage_2_5_binding"] == stage_binding
    assert historical["ladder_binding"]["contract_id"] == execution["ladder_contract_id"] == ladder["contract_id"]
    assert stage["predecessor"]["contract_id"] == old_stage["contract_id"] and stage["supersession"]["reason"] == "NONCANONICAL_TERMINAL_NEWLINE"
    assert ladder["canonical_predecessor"]["contract_id"] == old_packet["successor"]["ladder"]["id"]
    assert profile["canonical_predecessor"]["profile_id"] == old_packet["successor"]["profile"]["id"]
    checks += ["predecessor_lineage", "dependency_closure"]

    old_science = {key: value for key, value in old_stage.items() if key not in {"schema_version", "contract_id", "predecessor"}}
    new_science = {key: value for key, value in stage.items() if key not in {"schema_version", "contract_id", "predecessor", "supersession"}}
    assert new_science == old_science
    assert stage["stage_order"] == ["2.5", "3", "4", "5", "6", "SYNTHETIC_ENGINEERING", "ES_PILOT", "SCOPE_MATCHED_REPLICATION", "10.5", "10.6", "11", "12"]
    assert stage["question"]["claim_ceiling"] == historical["claim_ceiling"] == execution["claim_ceiling"] == "ROBUST_PREDICTIVE_ASSOCIATION"
    assert historical["macro_disposition"]["state"] == execution["macro_disposition"] == ladder["macro_disposition"] == profile["macro_disposition"] == "NO_ELIGIBLE_MACRO_DIVERSIFIER"
    assert historical["challenge_order"] == "ONE_SCOPE_WIDE_AFTER_LAST_APPLICABLE_REPLICATION" and historical["refit_order"] == "AFTER_PASSING_CHALLENGE_NO_EVALUATION"
    assert stage["challenge"]["session_manifest_status"] == "NOT_FROZEN_BLOCKED" and stage["refit"]["instance_status"] == "NOT_FROZEN_BLOCKED" and stage["forward_gate"]["instance_status"] == "NOT_FROZEN_BLOCKED"
    checks += ["scientific_semantics", "stage_order", "rank1_periods", "macro_disposition", "claim_ceiling", "reset_and_prerequisites"]

    for path, expected in ACTIVE.items():
        assert digest(Path(path)) == expected, path
    assert load_canonical(Path("configs/active_alpha_research_ladder.json"))["contract_id"] != ladder["contract_id"]
    checks += ["active_pointers_preserved", "active_v6_still_distinct", "row_opener_invocations_zero"]

    if packet_path is not None:
        packet = load_canonical(packet_path, "packet_id")
        assert packet["bundle"]["id"] == bundle["bundle_id"] and packet["bundle"]["sha256"] == digest(bundle_path)
        assert packet["pointer_last_required"] is True and all(value is False for value in packet["authority"].values())
        assert packet["active_preimages"] == ACTIVE
        checks.append("activation_packet")
    report = {
        "schema_version": "bias_robustness_canonical_independent_validation/1.0.0",
        "status": "PASS_CANONICAL_SUCCESSOR_PRE_ACTIVATION_VALIDATION",
        "bundle_id": bundle["bundle_id"], "bundle_path": bundle_path.as_posix(),
        "bundle_sha256": digest(bundle_path), "checks": checks,
        "fresh_process": True, "builder_called": False, "protected_access_performed": False,
        "row_opener_invocations": 0,
        "source_hashes": {path: digest(Path(path)) for path in VALIDATION_SOURCES},
    }
    report["validation_id"] = identity(report, "validation_id")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--packet", type=Path)
    parser.add_argument("--write-certificate", action="store_true")
    args = parser.parse_args()
    report = validate(args.bundle, args.packet)
    if args.write_certificate:
        from futures_rebuild.canonical import write_new_canonical_authority
        path = Path("state/bias_robustness_validation_registry") / str(report["validation_id"]) / "certificate.json"
        write_new_canonical_authority(ROOT / path, report)
        print(json.dumps({"validation_id": report["validation_id"], "path": path.as_posix(), "sha256": digest(path)}, sort_keys=True, separators=(",", ":")))
    else:
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
