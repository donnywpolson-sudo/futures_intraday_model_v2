"""Fresh-process validation of the inactive intraday research successor."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VALIDATION_ROOT = Path("state/intraday_research_validation_registry")
ACTIVE_PATHS = (
    "configs/active_alpha_historical_research_design.json",
    "configs/active_alpha_ladder_execution_design.json",
    "configs/active_alpha_research_ladder.json",
)


def canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8") + b"\n"


def digest(path: Path) -> str:
    return hashlib.sha256((ROOT / path).read_bytes()).hexdigest()


def identity(value: dict[str, object], field: str) -> str:
    core = {key: item for key, item in value.items() if key != field}
    return hashlib.sha256(canonical(core)[:-1]).hexdigest()


def load(relative: Path, field: str) -> dict[str, object]:
    absolute = (ROOT / relative).resolve(strict=True)
    absolute.relative_to(ROOT.resolve(strict=True))
    raw = absolute.read_bytes()
    value = json.loads(raw)
    assert isinstance(value, dict) and raw == canonical(value), relative
    assert value[field] == identity(value, field), relative
    return value


def validate(authority_path: Path) -> dict[str, object]:
    checks: list[str] = []
    authority = load(authority_path, "authority_id")
    assert authority["schema_version"] == "intraday_research_authority_successor/1.1.0"
    assert authority["state"] == "PREPARED_NOT_ACTIVE"
    assert all(value is False for value in authority["authority"].values())
    checks += ["authority_identity", "authority_is_non_active"]

    program_binding = authority["program"]
    program_path = Path(program_binding["path"])
    program = load(program_path, "program_id")
    assert program_binding["program_id"] == program["program_id"]
    assert program_binding["sha256"] == digest(program_path)
    assert program["schema_version"] == "intraday_research_program/1.1.0"
    assert program["candidate_count"] == 36
    assert [item["program_name"] for item in program["programs"]] == [
        "FLOW_CONTINUATION",
        "LIQUIDITY_SHOCK_REVERSAL",
        "CROSS_MARKET_INFORMATION_TRANSFER",
    ]
    assert [len(item["candidates"]) for item in program["programs"]] == [12, 12, 12]
    candidate_ids = []
    for program_item in program["programs"]:
        for candidate in program_item["candidates"]:
            assert candidate["candidate_id"] == identity(candidate, "candidate_id")
            candidate_ids.append(candidate["candidate_id"])
    assert len(candidate_ids) == len(set(candidate_ids)) == 36
    assert program["search_accounting"] == {
        "legacy": 105,
        "abandoned_pre_result": 27,
        "current": 36,
        "minimum_information_family": 168,
        "missing_historical_trial_values": "DSR_DIAGNOSTIC_NOT_AVAILABLE_NEVER_FABRICATE",
    }
    for path, expected in program["bindings"]["implementation"].items():
        assert digest(Path(path)) == expected
    checks += ["program_identity", "three_mechanisms", "candidate_grid", "search_accounting", "implementation_bindings"]

    synthetic_binding = authority["synthetic_acceptance"]
    synthetic_path = Path(synthetic_binding["path"])
    synthetic = load(synthetic_path, "acceptance_id")
    assert synthetic_binding["acceptance_id"] == synthetic["acceptance_id"]
    assert synthetic_binding["sha256"] == digest(synthetic_path)
    assert synthetic["schema_version"] == "intraday_research_synthetic_acceptance/1.1.0"
    assert synthetic["status"] == "PASS_FULL_SHARED_PROMOTION_GATE_SYNTHETIC_CERTIFICATION"
    assert synthetic["acceptance_scope"] == "SHARED_PROMOTION_GATE_WITH_MARKET_SHAPED_SYNTHETIC_SUMMARIES"
    assert synthetic["uses_shared_promotion_gate"] is True
    assert synthetic["full_statistical_decision_function_campaigns_completed"] is True
    assert synthetic["activation_gate_satisfied"] is True
    assert synthetic["real_history_accessed"] is False
    assert synthetic["null"]["campaigns"] == 10_000
    assert synthetic["null"]["one_sided_95_wilson_upper"] <= 0.05
    assert synthetic["known_edge"]["campaigns"] == 5_000
    assert synthetic["known_edge"]["power"] >= 0.8
    assert all(synthetic["tail_traps"].values())
    checks += ["synthetic_shared_gate_null", "synthetic_shared_gate_power", "tail_traps", "full_synthetic_gate_complete"]

    activation = authority["activation"]
    assert activation["active_pointer_paths"] == list(ACTIVE_PATHS)
    assert activation["write_order"][-1] == "configs/active_alpha_research_ladder.json"
    assert activation["ladder_pointer_written_last"] is True
    assert activation["candidate_pointer_bytes_prepared"] is False
    assert activation["synthetic_preparation_gate_satisfied"] is True
    assert activation["unsatisfied_gates"] == []
    for path, expected in activation["preimages"].items():
        assert digest(Path(path)) == expected
    active = json.loads((ROOT / ACTIVE_PATHS[-1]).read_bytes())
    assert active["contract_id"] != authority["authority_id"]
    checks += ["active_preimages", "pointer_last", "active_pointer_not_mutated"]

    catalog = program["bindings"]["catalog"]
    assert digest(Path(catalog["path"])) == catalog["sha256"]
    assert catalog["markets"] == ["6E", "6J", "CL", "ES", "GC", "NQ", "SR3", "ZB", "ZN"]
    assert catalog["entry_count"] == 144 and catalog["research_ready_entries"] == 136
    assert authority["reconciled_semantics"]["macro_disposition"] == "NO_ELIGIBLE_MACRO_DIVERSIFIER"
    assert authority["reconciled_semantics"]["challenge_order"] == "ONE_SCOPE_WIDE_AFTER_LAST_APPLICABLE_REPLICATION"
    assert authority["reconciled_semantics"]["execution_research_scope"] == ["ES", "NQ", "CL", "GC", "ZN", "ZB"]
    checks += ["nine_market_catalog", "authority_reconciliation", "v10_lane_bound_unchanged"]

    source = (ROOT / "src/futures_rebuild/intraday_alpha_discovery.py").read_text(encoding="utf-8")
    assert "ParquetFile" not in source and "read_table(" not in source and "pyarrow" not in source
    dsr = (ROOT / "src/futures_rebuild/research/dsr.py").read_text(encoding="utf-8")
    assert "PREREGISTERED_INNER_WFA_WINNER" in dsr
    checks += ["no_market_row_io", "inner_wfa_dsr_supported", "row_opener_invocations_zero"]

    report: dict[str, object] = {
        "schema_version": "intraday_research_independent_validation/1.0.0",
        "status": "PASS_PREPARATION_ONLY_NOT_ACTIVE",
        "authority_id": authority["authority_id"],
        "authority_path": authority_path.as_posix(),
        "authority_sha256": digest(authority_path),
        "program_id": program["program_id"],
        "synthetic_acceptance_id": synthetic["acceptance_id"],
        "checks": checks,
        "fresh_process": True,
        "producer_imported": False,
        "protected_access_performed": False,
        "row_opener_invocations": 0,
        "source_hashes": {
            path: digest(Path(path))
            for path in (
                "src/futures_rebuild/intraday_alpha_discovery.py",
                "src/futures_rebuild/research/dsr.py",
                "scripts/prepare_intraday_alpha_discovery.py",
                "scripts/independent_validate_intraday_alpha_discovery.py",
                "tests/test_intraday_alpha_discovery.py",
            )
        },
    }
    report["validation_id"] = identity(report, "validation_id")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--authority", type=Path, required=True)
    parser.add_argument("--write-certificate", action="store_true")
    args = parser.parse_args()
    report = validate(args.authority)
    if args.write_certificate:
        path = VALIDATION_ROOT / str(report["validation_id"]) / "certificate.json"
        absolute = ROOT / path
        expected = canonical(report)
        absolute.parent.mkdir(parents=True, exist_ok=True)
        if absolute.exists():
            assert absolute.read_bytes() == expected
        else:
            with absolute.open("xb") as stream:
                stream.write(expected)
        print(json.dumps({"validation_id": report["validation_id"], "path": path.as_posix(), "sha256": digest(path)}, sort_keys=True, separators=(",", ":")))
    else:
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
