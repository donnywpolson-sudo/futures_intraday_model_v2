import json
import shutil
from pathlib import Path

import pytest

from futures_rebuild.audit import AuditContractError, run_audit
from futures_rebuild.canonical import sha256_file


ROOT = Path(__file__).resolve().parents[1]


def _load_example() -> dict:
    return json.loads(
        (ROOT / "configs" / "master_audit_v3" / "invocation.example.json").read_text(
            encoding="utf-8"
        )
    )


def test_checked_in_approved_universe_without_receipt_evidence_fails_closed() -> None:
    with pytest.raises(AuditContractError, match="approval receipt evidence"):
        run_audit(ROOT, _load_example())


def test_checked_in_self_hashed_approval_receipt_is_valid_evidence() -> None:
    invocation = _load_example()
    approval_path = (
        ROOT / "configs" / "eight_market_successor_migration_approval.json"
    )
    approval = json.loads(approval_path.read_text(encoding="utf-8"))
    invocation["evidence"] = [
        {
            "evidence_id": approval["approval_receipt_id"],
            "path": "configs/eight_market_successor_migration_approval.json",
            "sha256": sha256_file(approval_path),
            "bytes": approval_path.stat().st_size,
            "safe_to_read": True,
            "limitations": [],
        }
    ]

    report = run_audit(ROOT, invocation)

    assert report["universe_contract_approved"] is True
    assert report["target_state_decision"] == "INSUFFICIENT_EVIDENCE"


def test_active_target_states_use_steady_state_vocabulary() -> None:
    matrix = json.loads(
        (
            ROOT
            / "configs"
            / "master_audit_v3"
            / "stage_requirement_matrix.json"
        ).read_text(encoding="utf-8")
    )
    assert "REBUILD_COMPLETE" not in matrix["target_states"]
    assert {
        "FOUNDATION_READY",
        "HISTORICAL_RESEARCH_READY",
        "OBSERVATION_COCKPIT_READY",
    }.issubset(matrix["target_states"])
    cockpit_required = set(
        matrix["target_states"]["OBSERVATION_COCKPIT_READY"]
    )
    assert {f"G8.S{index}" for index in range(5, 13)}.issubset(
        cockpit_required
    )


def test_checked_in_universe_preserves_frozen_tiers_and_cohort_roles() -> None:
    universe = json.loads(
        (ROOT / "configs" / "research_universe_contract.json").read_text(
            encoding="utf-8"
        )
    )
    tiers = {item["tier_id"]: item for item in universe["tiers"]}
    assert tiers[0]["symbols"] == ["ES"]
    assert tiers[1]["symbols"] == ["ES", "ZN", "6E", "CL", "NG", "GC", "ZC", "LE"]
    assert len(tiers[2]["symbols"]) == 16
    assert len(tiers[3]["symbols"]) == 38
    assert tiers[4]["symbols"] == ["BTC", "ETH", "PA"]
    assert "2018-2022_DISCOVERY" in tiers[1]["year_policy"]
    assert "2018-2022_FROZEN_REPLICATION_NOT_SELECTION" in tiers[2]["year_policy"]
    assert "2018-2024_REPLICATION_NOT_DISCOVERY" in tiers[3]["year_policy"]
    roles = {item["role"]: item for item in universe["cohorts"]}
    assert roles["LOCKED_UNTOUCHED_FINAL_HOLDOUT"]["selection_eligible"] is False
    assert roles["FORWARD_ONLY"]["selection_eligible"] is False


def _approved_fixture(tmp_path: Path) -> tuple[Path, dict]:
    (tmp_path / "docs").mkdir()
    (tmp_path / "configs" / "master_audit_v3").mkdir(parents=True)
    shutil.copyfile(ROOT / "MASTER_AUDIT.md", tmp_path / "MASTER_AUDIT.md")
    shutil.copyfile(
        ROOT / "configs" / "master_audit_v3" / "stage_requirement_matrix.json",
        tmp_path / "configs" / "master_audit_v3" / "stage_requirement_matrix.json",
    )
    approval_path = tmp_path / "docs" / "universe-approval.json"
    approval_path.write_text(
        '{"classification":"SYNTHETIC_TEST_APPROVAL_ONLY"}\n', encoding="utf-8"
    )
    approval_id = sha256_file(approval_path)
    universe = json.loads((ROOT / "configs" / "research_universe_contract.json").read_text(encoding="utf-8"))
    universe["status"] = "APPROVED"
    universe["approval_receipt_id"] = approval_id
    universe_path = tmp_path / "configs" / "research_universe_contract.json"
    universe_path.write_text(json.dumps(universe, indent=2) + "\n", encoding="utf-8")
    evidence_path = tmp_path / "docs" / "evidence.txt"
    evidence_path.write_text("synthetic structural evidence\n", encoding="utf-8")

    invocation = _load_example()
    invocation["audit_id"] = "SYNTHETIC-APPROVED-UNIVERSE"
    for name, relative in {
        "specification": "MASTER_AUDIT.md",
        "stage_matrix": "configs/master_audit_v3/stage_requirement_matrix.json",
        "universe_contract": "configs/research_universe_contract.json",
    }.items():
        invocation[name]["sha256"] = sha256_file(tmp_path / relative)
    invocation["evidence"] = [
        {
            "evidence_id": "E-SYNTHETIC",
            "path": "docs/evidence.txt",
            "sha256": sha256_file(evidence_path),
            "bytes": evidence_path.stat().st_size,
            "safe_to_read": True,
            "limitations": ["SYNTHETIC_MECHANICS_ONLY"],
        },
        {
            "evidence_id": approval_id,
            "path": "docs/universe-approval.json",
            "sha256": approval_id,
            "bytes": approval_path.stat().st_size,
            "safe_to_read": True,
            "limitations": ["SYNTHETIC_TEST_APPROVAL_ONLY"],
        },
    ]
    required = json.loads(
        (tmp_path / "configs" / "master_audit_v3" / "stage_requirement_matrix.json").read_text(
            encoding="utf-8"
        )
    )["target_states"]["FOUNDATION_READY"]
    invocation["check_results"] = [
        {
            "subcheck_id": subcheck_id,
            "status": "PASS",
            "reason": "SYNTHETIC_FIXTURE_SATISFIES_STRUCTURAL_CONTRACT",
            "evidence_refs": ["E-SYNTHETIC"],
            "limitations": ["NOT_REAL_WORLD_EVIDENCE"],
        }
        for subcheck_id in required
    ]
    return tmp_path, invocation


def test_approved_hash_bound_universe_can_support_complete_evidence(tmp_path: Path) -> None:
    root, invocation = _approved_fixture(tmp_path)
    report = run_audit(root, invocation)
    assert report["target_state_decision"] == "SUPPORTABLE"
    assert report["logical_exit_code"] == 0
    assert report["universe_contract_approved"] is True
    assert report["gate_statuses"]["G1"] == "PASS"
    assert report["gate_statuses"]["G2"] == "PASS"
    assert report["gate_statuses"]["G8"] == "NOT_RUN"


def test_fail_is_blocking_and_cannot_be_offset(tmp_path: Path) -> None:
    root, invocation = _approved_fixture(tmp_path)
    invocation["check_results"][0]["status"] = "FAIL"
    invocation["check_results"][0]["reason"] = "SYNTHETIC_PROVEN_FAILURE"
    report = run_audit(root, invocation)
    assert report["target_state_decision"] == "BLOCKED"
    assert report["logical_exit_code"] == 10
    assert report["gate_statuses"]["G1"] == "FAIL"


def test_pass_without_evidence_is_rejected(tmp_path: Path) -> None:
    root, invocation = _approved_fixture(tmp_path)
    invocation["check_results"][0]["evidence_refs"] = []
    with pytest.raises(AuditContractError, match="requires evidence"):
        run_audit(root, invocation)


def test_approved_universe_requires_exact_approval_receipt_evidence(tmp_path: Path) -> None:
    root, invocation = _approved_fixture(tmp_path)
    invocation["evidence"] = [
        item
        for item in invocation["evidence"]
        if item["evidence_id"] == "E-SYNTHETIC"
    ]
    with pytest.raises(AuditContractError, match="approval receipt evidence"):
        run_audit(root, invocation)


def test_legacy_or_escaping_paths_are_rejected() -> None:
    invocation = _load_example()
    invocation["specification"]["path"] = "../futures_intraday_model/MASTER_AUDIT.md"
    with pytest.raises(AuditContractError, match="relative and contained|outside"):
        run_audit(ROOT, invocation)


def test_runtime_cannot_enable_project_execution() -> None:
    invocation = _load_example()
    invocation["runtime"]["allowed_command_classes"].append("project-code-execution")
    with pytest.raises(AuditContractError, match="non-read-only"):
        run_audit(ROOT, invocation)
