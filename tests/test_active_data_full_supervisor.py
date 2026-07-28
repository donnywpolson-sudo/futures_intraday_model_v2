from pathlib import Path

import pytest

from futures_rebuild.active_data_full_supervisor import (
    HEARTBEAT_INTERVAL_SECONDS,
    SUPERVISION_SCHEMA,
    SUPERVISOR_LAUNCHER_PATH,
    SUPERVISOR_PATH,
    TERMINAL_GRACE_SECONDS,
    build_child_command,
    build_supervision_binding,
    validate_supervision_binding,
)
from futures_rebuild.errors import IntegrityError


def _copy_supervisor_files(tmp_path: Path) -> None:
    source_root = Path(__file__).resolve().parents[1]
    for relative in (SUPERVISOR_PATH, SUPERVISOR_LAUNCHER_PATH):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((source_root / relative).read_bytes())


def test_supervision_binding_is_bounded_nonrecurring_and_plan_exact(
    tmp_path: Path,
) -> None:
    _copy_supervisor_files(tmp_path)
    binding = build_supervision_binding(
        repository_root=tmp_path,
        interruption_id="a" * 64,
        attempt_number=6,
        maximum_duration_seconds=72_000,
    )
    plan = {
        "execution_attempt": {
            "attempt_number": 6,
            "interruption_id": "a" * 64,
        },
        "limits": {"maximum_duration_seconds": 72_000},
        "supervision": binding,
    }

    assert validate_supervision_binding(tmp_path, plan) == binding
    assert binding["schema_version"] == SUPERVISION_SCHEMA
    assert binding["heartbeat_interval_seconds"] == HEARTBEAT_INTERVAL_SECONDS
    assert (
        binding["scheduler_execution_time_limit_seconds"]
        == 72_000 + TERMINAL_GRACE_SECONDS
    )
    assert binding["transport"] == "WINDOWS_TASK_SCHEDULER_MANUAL_ONE_SHOT"
    assert binding["duplicate_start_policy"] == "FAIL_BEFORE_CHILD_PROCESS"
    assert str(binding["task_name"]).startswith("FIMV2-Stage6-")


def test_supervision_binding_rejects_launcher_drift(tmp_path: Path) -> None:
    _copy_supervisor_files(tmp_path)
    binding = build_supervision_binding(
        repository_root=tmp_path,
        interruption_id="b" * 64,
        attempt_number=6,
        maximum_duration_seconds=72_000,
    )
    (tmp_path / SUPERVISOR_LAUNCHER_PATH).write_text("drifted\n", encoding="utf-8")
    plan = {
        "execution_attempt": {
            "attempt_number": 6,
            "interruption_id": "b" * 64,
        },
        "limits": {"maximum_duration_seconds": 72_000},
        "supervision": binding,
    }

    with pytest.raises(
        IntegrityError,
        match="full-certification supervision binding changed",
    ):
        validate_supervision_binding(tmp_path, plan)


def test_supervised_child_command_uses_explicit_local_interpreter(
    tmp_path: Path,
) -> None:
    interpreter = tmp_path / ".venv/Scripts/python.exe"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_bytes(b"")
    plan_path = tmp_path / "plan.json"
    approval_path = tmp_path / "approval.json"
    plan_path.write_bytes(b"{}\n")
    approval_path.write_bytes(b"{}\n")

    command = build_child_command(
        repository_root=tmp_path,
        plan_path=plan_path,
        approval_path=approval_path,
    )

    assert command[0] == str(interpreter)
    assert command[1:3] == (
        "-m",
        "futures_rebuild.active_data_full_certification",
    )
    assert command[-4:] == (
        "--plan",
        str(plan_path),
        "--approval",
        str(approval_path),
    )
