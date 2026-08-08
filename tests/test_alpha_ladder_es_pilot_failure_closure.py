from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from futures_rebuild.alpha_ladder_es_pilot_failure_closure import (
    AUDIT_PATH,
    AUTHORIZATION_USE_PATH,
    CLOSURE_ROOT,
    EXPECTED_BINDINGS,
    EXECUTION_ROOT,
    MANIFEST_PATH,
    PLAN_PATH,
    REGISTRATION_PATH,
    build_failure_audit,
    closure_path,
    prepare_failure_closure,
    verify_prepared_failure_closure,
)
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]


def _copy(root: Path, relative: Path) -> None:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / relative, target)


def _shadow_root(tmp_path: Path) -> Path:
    for relative in EXPECTED_BINDINGS:
        _copy(tmp_path, Path(relative))
    return tmp_path


def test_live_audit_reconciles_failure_to_signal_and_risk_interaction() -> None:
    audit = build_failure_audit(root=ROOT)
    reconciliation = audit["exact_reconciliation"]
    assert reconciliation["prediction_rows"] == 63
    assert reconciliation["hurdle_passes"] == 20
    assert reconciliation["risk_abstentions"] == 20
    assert reconciliation["hurdle_pass_sessions_equal_risk_abstention_sessions"] is True
    assert reconciliation["below_hurdle"] == 43
    assert reconciliation["candidate_trades"] == 0
    assert audit["fault_classification"]["source_or_calendar_defect_proven"] is False
    assert audit["fault_classification"]["implementation_exception_occurred"] is False
    assert audit["fault_classification"]["exact_frozen_mechanism_failed_pilot"] is True


def test_shadow_preparation_is_create_only_deterministic_and_nonpublishing(tmp_path: Path) -> None:
    root = _shadow_root(tmp_path)
    first = prepare_failure_closure(root=root)
    second = prepare_failure_closure(root=root)
    assert first == second == verify_prepared_failure_closure(root=root)
    manifest = json.loads((root / MANIFEST_PATH).read_text(encoding="utf-8"))
    closure = json.loads((root / first["closure_path"]).read_text(encoding="utf-8"))
    assert closure["economic_result"] == "FAIL"
    assert closure["strategy_failure"] is True
    assert closure["data_failure"] is False
    assert closure["implementation_failure"] is False
    assert manifest["state"] == "PREPARED_UNPUBLISHED_NOT_AUTHORIZED"
    assert manifest["publication_authorized"] is False
    assert manifest["active_pointer_mutation"] is False
    assert not (root / "state/trial_registry/alpha_ladder_es_pilot_terminal_closure").exists()


def test_preparation_fails_closed_if_terminal_evidence_drifts(tmp_path: Path) -> None:
    root = _shadow_root(tmp_path)
    target = root / EXECUTION_ROOT / "pilot_decision.json"
    payload = json.loads(target.read_text(encoding="utf-8"))
    payload["decision"] = "PASS"
    target.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(IntegrityError):
        prepare_failure_closure(root=root)


def test_preparation_fails_closed_if_existing_manifest_differs(tmp_path: Path) -> None:
    root = _shadow_root(tmp_path)
    prepare_failure_closure(root=root)
    (root / MANIFEST_PATH).write_text("{}\n", encoding="utf-8")
    with pytest.raises(IntegrityError):
        prepare_failure_closure(root=root)


def test_manifest_covers_exact_evidence_and_preserves_lifecycle_bytes(tmp_path: Path) -> None:
    root = _shadow_root(tmp_path)
    prepared = prepare_failure_closure(root=root)
    manifest = json.loads((root / MANIFEST_PATH).read_text(encoding="utf-8"))
    sources = {item["source_path"] for item in manifest["create_only_copies"]}
    assert {str((EXECUTION_ROOT / name).as_posix()) for name in (
        "baseline_executions.json",
        "candidate_execution.json",
        "input_audit.json",
        "metrics.json",
        "model.json",
        "pilot_decision.json",
        "predictions.json",
        "terminal_report.json",
    )}.issubset(sources)
    assert AUDIT_PATH.as_posix() in sources
    assert prepared["closure_path"] in sources
    preserved = {item["path"] for item in manifest["preserve_in_place"]}
    assert preserved == {
        AUTHORIZATION_USE_PATH.as_posix(),
        PLAN_PATH.as_posix(),
        REGISTRATION_PATH.as_posix(),
    }
    assert Path(prepared["closure_path"]).is_relative_to(CLOSURE_ROOT)
