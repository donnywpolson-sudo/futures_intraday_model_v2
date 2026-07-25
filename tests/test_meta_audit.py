from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.meta_audit import EVIDENCE_SCHEMA, run_meta_audit


ROOT = Path(__file__).resolve().parents[1]


def _suite_evidence(tmp_path: Path) -> Path:
    coverage = json.loads(
        (ROOT / "configs" / "meta_master_audit_coverage.json").read_text(
            encoding="utf-8"
        )
    )
    test_files = sorted(
        {
            node_id.split("::", 1)[0]
            for control in coverage["controls"]
            for node_id in control["tests"]
        }
    )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    core = {
        "schema_version": EVIDENCE_SCHEMA,
        "status": "PASS",
        "command": r".\.venv\Scripts\python.exe -m pytest -q",
        "git_head": head,
        "passed": 1,
        "failed": 0,
        "errors": 0,
        "skipped": 0,
        "test_file_sha256": {
            relative: sha256_file(ROOT / relative)
            for relative in test_files
        },
    }
    path = tmp_path / "suite-evidence.json"
    path.write_text(
        json.dumps(
            {**core, "evidence_id": sha256_json(core)},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def test_blind_coverage_is_structurally_closed_before_suite_evidence() -> None:
    report = run_meta_audit(ROOT)
    assert report["classification"] == "INSUFFICIENT_EVIDENCE"
    assert report["blind_first_contract_pass"] is True
    assert report["unresolved_critical_high_count"] == 0
    assert report["unresolved_p0_p1_count"] == 0
    assert len(report["controls"]) == 12
    assert all(item["status"] == "PASS" for item in report["controls"])
    assert report["suite_evidence_reason"] == "FULL_SUITE_EVIDENCE_NOT_SUPPLIED"
    assert report["authority"]["authorizes_trading"] is False


def test_exact_hash_bound_suite_evidence_can_support_meta_audit(
    tmp_path: Path,
) -> None:
    report = run_meta_audit(
        ROOT, suite_evidence_path=_suite_evidence(tmp_path)
    )
    assert report["classification"] == "SUPPORTABLE"
    assert report["suite_evidence_status"] == "PASS"
    assert report["unresolved_critical_high_count"] == 0
    assert report["unresolved_p0_p1_count"] == 0


def test_stale_test_hash_blocks_meta_support(tmp_path: Path) -> None:
    path = _suite_evidence(tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    first = next(iter(payload["test_file_sha256"]))
    payload["test_file_sha256"][first] = "0" * 64
    core = {key: payload[key] for key in payload if key != "evidence_id"}
    payload["evidence_id"] = sha256_json(core)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    report = run_meta_audit(ROOT, suite_evidence_path=path)

    assert report["classification"] == "INSUFFICIENT_EVIDENCE"
    assert report["suite_evidence_reason"] == "SUITE_TEST_FILE_DRIFT"
