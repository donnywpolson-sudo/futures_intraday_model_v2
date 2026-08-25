from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.closure_workflow.policy import WorkflowError
from futures_rebuild.live_cockpit import workflow


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "cockpit@example.invalid")
    _git(repo, "config", "user.name", "Cockpit Test")
    for relative, text in {
        "AGENTS.md": "rules\n",
        "PROJECT_OUTLINE.md": "outline\n",
        "CURRENT_WORKFLOW.md": "workflow\n",
        "pyproject.toml": '[project]\nname = "futures-intraday-model-v2"\n',
        "configs/source_contract.json": "{}\n",
    }.items():
        path = repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    _git(repo, "add", "--", ".")
    _git(repo, "commit", "-m", "initial")
    return repo


def _plan(repo: Path, relative: str = "plans/smoke.json") -> tuple[str, str]:
    plan = {"plan_id": "a" * 64}
    path = repo / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(plan) + b"\n")
    return relative, sha256_file(path)


def _outcome(
    repo: Path,
    *,
    phases: list[tuple[str, str, str]] | None = None,
) -> tuple[Path, dict[str, object]]:
    plan_relative, plan_sha = _plan(repo)
    selected = phases or [
        ("SMOKE", "SEPARATELY_APPROVED_HIGH_RISK", "RUN_SMOKE")
    ]
    stages = []
    for index, (phase, authority, operation) in enumerate(selected):
        relative = plan_relative
        if index:
            relative, plan_sha = _plan(repo, f"plans/{phase.lower()}.json")
        stages.append(
            {
                "phase": phase,
                "authority_class": authority,
                "operation": operation,
                "plan_path": relative,
                "plan_sha256": plan_sha,
                "terminal_path": f"reports/{phase.lower()}-terminal.json",
            }
        )
    core: dict[str, object] = {
        "schema_version": workflow.OUTCOME_SCHEMA,
        "goal": "activate the observation-only cockpit",
        "basis": {"branch": "main", "head": _git(repo, "rev-parse", "HEAD")},
        "stages": stages,
        "interaction_budget": {
            "provider_free_local_transition": 1,
            "atomic_local_stage_and_commit": 1,
        },
        "paused_installation": {"version": "prepared-but-inactive"},
    }
    value = {**core, "outcome_id": sha256_json(core)}
    root = repo / "reports" / "live_cockpit" / "workflow" / value["outcome_id"]
    root.mkdir(parents=True)
    (root / "outcome.json").write_bytes(canonical_bytes(value) + b"\n")
    return root, value


def test_closed_market_suppresses_smoke_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _repo(tmp_path)
    root, _ = _outcome(repo)
    monkeypatch.setattr(
        workflow,
        "es_open_window",
        lambda repo: {
            "eligible": False,
            "state": "CLOSED",
            "remaining_open_seconds": 0,
            "minimum_open_seconds": 180,
            "trade_date": "2026-07-30",
        },
    )
    decision = workflow.workflow_status(repo, root)
    assert decision["status"] == "WAITING"
    assert "approval" not in decision
    assert decision["phase"] == "SMOKE"


def test_open_market_requests_plain_language_confirmation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _repo(tmp_path)
    root, value = _outcome(repo)
    monkeypatch.setattr(
        workflow,
        "es_open_window",
        lambda repo: {
            "eligible": True,
            "state": "OPEN",
            "remaining_open_seconds": 600,
            "minimum_open_seconds": 180,
            "trade_date": "2026-07-30",
        },
    )
    decision = workflow.workflow_status(repo, root)
    assert decision == {
        "status": "APPROVAL_REQUIRED",
        "phase": "SMOKE",
        "decision": "Confirm the smoke stage in plain language",
        "confirmation_request": {
            "operation": "RUN_SMOKE",
            "confirmation": "Plain-language user confirmation is required before this high-risk stage.",
        },
    }


def test_historic_outcome_keeps_its_original_local_stage_metadata(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    root, value = _outcome(
        repo,
        phases=[
            ("LOCAL_TRANSITION", "PROVIDER_FREE_LOCAL_TRANSITION", "RUN_LOCAL"),
            ("PACKAGE", "PROVIDER_FREE_LOCAL_TRANSITION", "RUN_PACKAGE"),
        ],
    )
    del root
    assert workflow.validate_outcome_spec(value)["outcome_id"] == value["outcome_id"]


def test_passed_terminal_advances_without_regenerating_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _repo(tmp_path)
    root, value = _outcome(
        repo,
        phases=[
            (
                "LOCAL_TRANSITION",
                "PROVIDER_FREE_LOCAL_TRANSITION",
                "RUN_PROVIDER_FREE_LOCAL_TRANSITION",
            ),
            ("SMOKE", "SEPARATELY_APPROVED_HIGH_RISK", "RUN_SMOKE"),
        ],
    )
    first = value["stages"][0]
    terminal = repo / first["terminal_path"]
    terminal.parent.mkdir(parents=True, exist_ok=True)
    terminal.write_text(json.dumps({"status": "PASS"}), encoding="utf-8")
    monkeypatch.setattr(
        workflow,
        "es_open_window",
        lambda repo: {
            "eligible": False,
            "state": "CLOSED",
            "remaining_open_seconds": 0,
            "minimum_open_seconds": 180,
            "trade_date": "2026-07-30",
        },
    )
    decision = workflow.workflow_status(repo, root)
    assert decision["phase"] == "SMOKE"
    assert decision["status"] == "WAITING"
    assert (repo / first["plan_path"]).is_file()


@pytest.mark.parametrize(
    ("phase", "schema", "terminal_state"),
    [
        (
            "PACKAGE",
            "live_cockpit_package_candidate_terminal/1.1.0",
            "CANDIDATE_VERIFIED",
        ),
        (
            "INSTALL",
            "live_cockpit_installation_terminal/1.1.0",
            "INSTALLATION_PREPARED",
        ),
    ],
)
def test_real_typed_terminal_advances_outcome(
    tmp_path: Path, phase: str, schema: str, terminal_state: str
) -> None:
    repo = _repo(tmp_path)
    root, value = _outcome(
        repo, phases=[(phase, "SEPARATELY_APPROVED_HIGH_RISK", f"RUN_{phase}")]
    )
    terminal = repo / value["stages"][0]["terminal_path"]
    terminal.parent.mkdir(parents=True, exist_ok=True)
    terminal.write_text(
        json.dumps({"schema_version": schema, "terminal_state": terminal_state}),
        encoding="utf-8",
    )
    assert workflow.workflow_status(repo, root, probe_market=False)["status"] == "COMPLETE"


def test_failed_smoke_result_blocks_for_classification(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    root, value = _outcome(
        repo,
        phases=[
            (
                "SMOKE",
                "SEPARATELY_APPROVED_HIGH_RISK",
                "RUN_BOUNDED_OBSERVATION_ONLY_SMOKE",
            )
        ],
    )
    terminal = repo / value["stages"][0]["terminal_path"]
    terminal.parent.mkdir(parents=True, exist_ok=True)
    terminal.write_text(
        json.dumps(
            {
                "schema_version": "futures_live_cockpit_smoke_result/1.0.0",
                "status": "FAIL",
            }
        ),
        encoding="utf-8",
    )
    decision = workflow.workflow_status(repo, root, probe_market=False)
    assert decision["status"] == "BLOCKED"
    assert decision["phase"] == "SMOKE"
    assert decision["decision"] == "SMOKE ended FAIL; classify before successor"


def test_status_rejects_tampered_immutable_event_chain(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    root, _ = _outcome(repo)
    events = root / "events"
    events.mkdir()
    (events / "000001.json").write_text(
        json.dumps(
            {
                "schema_version": workflow.EVENT_SCHEMA,
                "sequence": 1,
                "recorded_at_utc": "2026-07-30T00:00:00Z",
                "event_type": "OUTCOME_INITIALIZED",
                "phase": None,
                "previous_event_id": None,
                "details": {},
                "event_id": "0" * 64,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(WorkflowError, match="event chain"):
        workflow.workflow_status(repo, root, probe_market=False)
