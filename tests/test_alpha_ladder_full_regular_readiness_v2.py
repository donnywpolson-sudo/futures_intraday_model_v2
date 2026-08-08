from __future__ import annotations

import json
from pathlib import Path

import pytest

import futures_rebuild.alpha_ladder_full_regular_readiness as v1
import futures_rebuild.alpha_ladder_full_regular_readiness_v2 as v2
from futures_rebuild.canonical import canonical_bytes
from futures_rebuild.errors import UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def test_v2_preserves_research_semantics_and_consumed_attempt() -> None:
    predecessor = json.loads((ROOT / v1.PLAN_PATH).read_text(encoding="utf-8"))
    successor = json.loads((ROOT / v2.PLAN_PATH).read_text(encoding="utf-8"))
    for key in (
        "contract_id",
        "profile_id",
        "mechanism_id",
        "mechanism_sha256",
        "calendar_id",
        "markets",
        "years",
        "checkpoint",
        "pilot",
        "tier_1",
        "entry_semantics",
        "exit_semantics",
        "required_baselines",
        "required_cost_scenarios",
        "coverage",
        "session_eligibility",
        "execution_limits",
        "authority",
        "protected_source_paths",
    ):
        assert successor[key] == predecessor[key]
    assert successor["predecessor"] == {
        "plan_id": v2.PREDECESSOR_PLAN_ID,
        "plan_sha256": v2.PREDECESSOR_PLAN_SHA256,
        "consumed_receipt_id": v2.PREDECESSOR_RECEIPT_ID,
        "consumed_receipt_sha256": v2.PREDECESSOR_RECEIPT_SHA256,
        "failure_id": v2.PREDECESSOR_FAILURE_ID,
        "failure_sha256": v2.PREDECESSOR_FAILURE_SHA256,
        "classification": "PRE_REGISTRATION_IMPLEMENTATION_INVALID",
        "reusable": False,
    }
    assert successor["output_root"] != predecessor["output_root"]
    assert successor["execution_limits"]["maximum_attempts"] == 1
    assert successor["execution_limits"]["maximum_retries"] == 0
    with pytest.raises(UnauthorizedOperation, match="predecessor stage did not pass"):
        v2.build_plan(root=ROOT)


def test_v2_plan_binds_its_lifecycle_and_has_conditional_output_topology() -> None:
    plan = json.loads((ROOT / v2.PLAN_PATH).read_text(encoding="utf-8"))
    for path in (
        v2.MODULE_PATH,
        v2.PREPARE_SCRIPT_PATH,
        v2.RUNNER_PATH,
        v2.TEST_PATH,
        v2.PREDECESSOR_PLAN_PATH,
        v2.PREDECESSOR_RECEIPT_PATH,
        v2.PREDECESSOR_FAILURE_PATH,
    ):
        assert path.as_posix() in plan["bindings"]
    outputs = plan["required_outputs"]
    assert outputs["pass_or_gate_rejection"][-1] == "readiness_report.json"
    assert outputs["post_consumption_exception"] == ["execution_failure.json"]
    assert plan["write_protocol"] == {
        "prerequisites_written_and_read_back_before_certificate_validation": True,
        "certificates_written_after_validation": True,
        "terminal_report_written_last": True,
        "post_consumption_exception_sealed_create_only": True,
        "partial_outputs_never_claim_readiness": True,
    }


def test_success_finalization_writes_prerequisites_before_validation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr(v2, "OUTPUT_ROOT", Path("out"))
    prerequisites = {
        name: {"name": name} for name in v2.PREREQUISITE_NAMES
    }
    certificates = {
        name: {"name": name} for name in v2.CERTIFICATE_NAMES
    }
    validations: list[str] = []

    def validate(certificate, *, root: Path):
        assert all((root / "out" / name).is_file() for name in v2.PREREQUISITE_NAMES)
        assert not any((root / "out" / name).exists() for name in v2.CERTIFICATE_NAMES)
        assert not (root / "out" / v2.TERMINAL_REPORT_NAME).exists()
        validations.append(str(certificate["name"]))

    written: list[str] = []
    v2._finalize_success(
        root=tmp_path,
        prerequisites=prerequisites,
        certificates=certificates,
        report={"terminal": True},
        written=written,
        certificate_validator=validate,
    )
    assert validations == list(v2.CERTIFICATE_NAMES)
    assert written == [
        *v2.PREREQUISITE_NAMES,
        *v2.CERTIFICATE_NAMES,
        v2.TERMINAL_REPORT_NAME,
    ]
    assert json.loads(
        (tmp_path / "out" / v2.TERMINAL_REPORT_NAME).read_text(encoding="utf-8")
    ) == {"terminal": True}


def test_interruption_after_prerequisites_seals_terminal_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr(v2, "OUTPUT_ROOT", Path("out"))
    monkeypatch.setattr(v2, "FAILURE_ROOT", Path("failures"))
    monkeypatch.setattr(v2, "PLAN_PATH", Path("plan.json"))
    (tmp_path / "plan.json").write_bytes(canonical_bytes({"plan": 2}) + b"\n")
    use_path = tmp_path / "use.json"
    use_path.write_bytes(canonical_bytes({"used": True}) + b"\n")

    class Receipt:
        receipt_id = "a" * 64

    prerequisites = {
        name: {"name": name} for name in v2.PREREQUISITE_NAMES
    }
    certificates = {
        name: {"name": name} for name in v2.CERTIFICATE_NAMES
    }

    def operation(stage: dict[str, str], written: list[str]):
        stage["value"] = "CERTIFICATE_VALIDATION"

        def fail_validation(_certificate, *, root: Path):
            raise RuntimeError("synthetic interruption")

        v2._finalize_success(
            root=tmp_path,
            prerequisites=prerequisites,
            certificates=certificates,
            report={"terminal": True},
            written=written,
            certificate_validator=fail_validation,
        )
        raise AssertionError("unreachable")

    with pytest.raises(RuntimeError, match="synthetic interruption"):
        v2._run_after_consumption(
            root=tmp_path,
            plan={"plan_id": "b" * 64},
            receipt=Receipt(),  # type: ignore[arg-type]
            use_path=use_path,
            operation=operation,
        )
    failure_path = (
        tmp_path / "failures" / Receipt.receipt_id / "execution_failure.json"
    )
    failure = json.loads(failure_path.read_text(encoding="utf-8"))
    assert failure["failure_stage"] == "CERTIFICATE_VALIDATION"
    assert failure["exception_type"] == "RuntimeError"
    assert failure["written_outputs"] == list(v2.PREREQUISITE_NAMES)
    assert failure["readiness_decision_produced"] is False
    assert not (tmp_path / "out" / v2.TERMINAL_REPORT_NAME).exists()


def test_write_readback_mismatch_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    original = Path.read_bytes

    def changed(self: Path) -> bytes:
        data = original(self)
        return data + b"changed"

    monkeypatch.setattr(Path, "read_bytes", changed)
    with pytest.raises(Exception, match="readback changed"):
        v2._write_and_verify(tmp_path / "evidence.json", {"value": 1})


def test_no_pilot_rejection_writes_terminal_report_last(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr(v2, "OUTPUT_ROOT", Path("out"))
    calls: list[str] = []
    original = v2._write_and_verify

    def recording(path: Path, payload):
        calls.append(path.name)
        return original(path, payload)

    monkeypatch.setattr(v2, "_write_and_verify", recording)
    outputs = {
        "checkpoint_accounting.json": {"a": 1},
        "source_audit.json": {"a": 2},
        "pilot_fold_selection.json": {"a": 3},
        "readiness_report.json": {"a": 4},
    }
    written: list[str] = []
    v2._finalize_no_pilot_rejection(
        root=tmp_path, outputs=outputs, written=written
    )
    assert calls == list(outputs)
    assert calls[-1] == v2.TERMINAL_REPORT_NAME
