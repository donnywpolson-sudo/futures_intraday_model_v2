from __future__ import annotations

import json
from pathlib import Path

import futures_rebuild.retirement as retirement
from futures_rebuild.retirement import (
    FINAL_AUDIT_REPORTS,
    META_REPORT,
    scan_retirement_readiness,
)


ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def test_current_classifier_matches_evidence_without_opening_legacy_root() -> None:
    report = scan_retirement_readiness(ROOT)
    ready = all(item["status"] == "PASS" for item in report["checks"])
    assert report["classification"] == (
        "LEGACY_RETIREMENT_READY" if ready else "LEGACY_RETIREMENT_BLOCKED"
    )
    assert report["standalone_runtime_ready"] is ready
    assert report["legacy_root_opened"] is False
    by_id = {item["check_id"]: item for item in report["checks"]}
    assert by_id["SOURCE_CONTRACT_STANDALONE_41_MARKET"]["status"] == "PASS"
    assert by_id["APPROVED_41_MARKET_UNIVERSE"]["status"] == "PASS"
    assert by_id["NO_PUBLIC_MIGRATION_OR_LEGACY_ENTRYPOINTS"]["status"] == "PASS"
    assert by_id["NO_LEGACY_RUNTIME_IMPORTS"]["status"] == "PASS"
    assert report["authority"]["legacy_delete_authorized"] is False


def test_current_classifier_fails_closed_when_git_is_not_clean(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        retirement,
        "_git_clean",
        lambda _root: (False, "WORKTREE_NOT_CLEAN"),
    )

    report = scan_retirement_readiness(ROOT)

    assert report["classification"] == "LEGACY_RETIREMENT_BLOCKED"
    assert report["standalone_runtime_ready"] is False
    assert report["legacy_root_opened"] is False
    by_id = {item["check_id"]: item for item in report["checks"]}
    assert by_id["CLEAN_COMMITTED_HEAD"] == {
        "check_id": "CLEAN_COMMITTED_HEAD",
        "status": "FAIL",
        "evidence": [".git"],
        "reason": "WORKTREE_NOT_CLEAN",
    }
    assert report["authority"]["legacy_delete_authorized"] is False


def test_synthetic_complete_repo_can_be_classified_without_external_root(
    tmp_path: Path, monkeypatch
) -> None:
    for relative in (
        "AGENTS.md",
        "PROJECT_OUTLINE.md",
        "README.md",
        "MASTER_AUDIT.md",
        "META_MASTER_AUDIT.md",
    ):
        path = tmp_path / relative
        path.write_text("steady state v2\n", encoding="utf-8")
    (tmp_path / ".gitignore").write_text(
        ".env\n.env.*\napi.env\ndatabento.env\n", encoding="utf-8"
    )
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "synthetic"\nversion = "1"\n'
        '[project.scripts]\nfutures-pipeline = "x:y"\n',
        encoding="utf-8",
    )
    _write_json(
        tmp_path / "configs" / "source_contract.json",
        {
            "legacy_repository": None,
            "external_repository_access": "FORBIDDEN",
            "canonical_dbn_release": {
                "dbn_files": 4491,
                "sidecar_files": 4491,
                "combined_files": 8982,
                "combined_bytes": 25_592_717_852,
            },
        },
    )
    markets = [f"M{index:02d}" for index in range(41)]
    _write_json(
        tmp_path / "configs" / "research_universe_contract.json",
        {
            "status": "APPROVED",
            "approval_receipt_id": "a" * 64,
            "tiers": [{"symbols": markets}],
        },
    )
    (tmp_path / "configs" / "alpha_tiered.yaml").write_text(
        "classification: NON_AUTHORIZING_OPERATIONAL_VIEW\n",
        encoding="utf-8",
    )
    _write_json(
        tmp_path
        / "reports"
        / "migration"
        / "legacy_retirement_inventory.json",
        {
            "overall_state": "LEGACY_RETIREMENT_READY",
            "entries": [
                {
                    "path": f"item-{index}",
                    "classification": "MIGRATED_FUNCTIONALITY",
                }
                for index in range(41)
            ],
        },
    )
    for target, relative in FINAL_AUDIT_REPORTS.items():
        _write_json(
            tmp_path / relative,
            {
                "target_state": target,
                "target_state_decision": "SUPPORTABLE",
                "logical_exit_code": 0,
                "authority": {"authorizes_trading": False},
            },
        )
    _write_json(
        tmp_path / META_REPORT,
        {
            "classification": "SUPPORTABLE",
            "unresolved_critical_high_count": 0,
            "unresolved_p0_p1_count": 0,
        },
    )
    monkeypatch.setattr(retirement, "_git_clean", lambda _root: (True, "CLEAN"))

    report = scan_retirement_readiness(tmp_path)

    assert report["classification"] == "LEGACY_RETIREMENT_READY"
    assert report["standalone_runtime_ready"] is True
    assert report["legacy_root_opened"] is False
    assert all(item["status"] == "PASS" for item in report["checks"])


def test_operational_absolute_legacy_path_is_detected(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(
        retirement,
        "OPERATIONAL_PATHS",
        ("README.md",),
    )
    (tmp_path / "README.md").write_text(
        r"C:\Users\donny\Desktop\futures_intraday_model\api.env",
        encoding="utf-8",
    )
    passed, offenders = retirement._operational_path_scan(tmp_path)
    assert passed is False
    assert offenders == ["README.md"]
