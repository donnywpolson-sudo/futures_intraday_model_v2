"""Decision-oriented coordinator for the observation-only cockpit outcome."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.canonical import canonical_bytes, is_linklike, sha256_file, sha256_json
from futures_rebuild.exchange_calendar import (
    CME_TIMEZONE,
    load_active_calendar_index,
    verify_calendar_freshness,
)
from futures_rebuild.errors import ContractError
from futures_rebuild.live_cockpit.feed import chart_market_universe


OUTCOME_SCHEMA = "futures_live_cockpit_workflow/1.0.0"
EVENT_SCHEMA = "futures_live_cockpit_workflow_event/1.0.0"
PHASES = (
    "LOCAL_TRANSITION",
    "LOCAL_COMMIT",
    "PACKAGE",
    "INSTALL",
    "SMOKE",
    "ACTIVATE",
)
AUTHORITIES = {
    "AUTONOMOUS_READ_ONLY",
    "PROVIDER_FREE_LOCAL_TRANSITION",
    "ATOMIC_LOCAL_STAGE_AND_COMMIT",
    "SEPARATELY_APPROVED_HIGH_RISK",
}
EXPECTED_PROJECT = "futures-intraday-model-v2"
REQUIRED_ROOT_FILES = (
    "AGENTS.md",
    "PROJECT_OUTLINE.md",
    "pyproject.toml",
    "configs/source_contract.json",
)


class WorkflowError(ContractError):
    """A live-cockpit workflow contract was not satisfied."""


def _run_git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", *args], cwd=repo, check=False, capture_output=True, text=True
    )
    if result.returncode:
        raise WorkflowError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result


def canonical_repo_root(start: Path) -> Path:
    root = Path(_run_git(start, "rev-parse", "--show-toplevel").stdout.strip()).resolve()
    if is_linklike(root):
        raise WorkflowError(f"repository root is link-like: {root}")
    if not (root / ".git").is_dir():
        raise WorkflowError(f"repository is not a primary worktree: {root}")
    for relative in REQUIRED_ROOT_FILES:
        if not (root / relative).is_file():
            raise WorkflowError(f"required root file is absent: {relative}")
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    if f'name = "{EXPECTED_PROJECT}"' not in pyproject:
        raise WorkflowError("pyproject project identity mismatch")
    return root


def git_identity(repo: Path) -> dict[str, Any]:
    status = _run_git(repo, "status", "--short", "--untracked-files=all").stdout
    return {
        "branch": _run_git(repo, "branch", "--show-current").stdout.strip(),
        "head": _run_git(repo, "rev-parse", "HEAD").stdout.strip(),
        "status_lines": status.splitlines(),
        "staged_paths": [
            line
            for line in _run_git(repo, "diff", "--cached", "--name-only")
            .stdout.splitlines()
            if line
        ],
    }


def _read_object(path: Path, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WorkflowError(f"{name} is not readable JSON: {path}") from exc
    if type(value) is not dict:
        raise WorkflowError(f"{name} must be an object")
    return value


def _create_only(path: Path, value: Mapping[str, Any]) -> None:
    payload = canonical_bytes(dict(value)) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(payload)
    except FileExistsError as exc:
        raise WorkflowError(f"refusing overwrite: {path}") from exc
    if path.read_bytes() != payload:
        raise WorkflowError(f"create-only write verification failed: {path}")


def validate_outcome_spec(value: Mapping[str, Any]) -> dict[str, Any]:
    required = {
        "schema_version",
        "goal",
        "basis",
        "stages",
        "interaction_budget",
        "paused_installation",
        "outcome_id",
    }
    if set(value) != required or value.get("schema_version") != OUTCOME_SCHEMA:
        raise WorkflowError("cockpit outcome fields or schema are invalid")
    stages = value.get("stages")
    if type(stages) is not list or not stages:
        raise WorkflowError("cockpit outcome requires ordered stages")
    observed: list[str] = []
    for stage in stages:
        if type(stage) is not dict or set(stage) != {
            "phase",
            "authority_class",
            "operation",
            "plan_path",
            "plan_sha256",
            "terminal_path",
        }:
            raise WorkflowError("cockpit stage fields are invalid")
        phase = stage["phase"]
        if phase not in PHASES or phase in observed:
            raise WorkflowError("cockpit phases must be unique and supported")
        if observed and PHASES.index(phase) <= PHASES.index(observed[-1]):
            raise WorkflowError("cockpit phases are not dependency ordered")
        observed.append(phase)
        if stage["authority_class"] not in AUTHORITIES:
            raise WorkflowError("cockpit stage authority class is invalid")
        if not all(
            isinstance(stage[key], str)
            for key in ("operation", "plan_path", "plan_sha256", "terminal_path")
        ):
            raise WorkflowError("cockpit stage binding is invalid")
    if type(value["interaction_budget"]) is not dict:
        raise WorkflowError("cockpit outcome interaction metadata is invalid")
    core = {key: item for key, item in value.items() if key != "outcome_id"}
    if value["outcome_id"] != sha256_json(core):
        raise WorkflowError("cockpit outcome content hash is invalid")
    return dict(value)


def build_outcome_spec(template: Mapping[str, Any]) -> dict[str, Any]:
    core = dict(template)
    core["schema_version"] = OUTCOME_SCHEMA
    core.setdefault(
        "interaction_budget",
        {
            "provider_free_local_transition": 1,
            "atomic_local_stage_and_commit": 1,
        },
    )
    core["outcome_id"] = sha256_json(core)
    return validate_outcome_spec(core)


def _event_path(outcome_root: Path, sequence: int) -> Path:
    return outcome_root / "events" / f"{sequence:06d}.json"


def _append_event(
    outcome_root: Path, event_type: str, phase: str | None, details: Mapping[str, Any]
) -> dict[str, Any]:
    existing = sorted((outcome_root / "events").glob("*.json"))
    previous_id = None
    if existing:
        previous_id = _read_object(existing[-1], "workflow event")["event_id"]
    core: dict[str, Any] = {
        "schema_version": EVENT_SCHEMA,
        "sequence": len(existing) + 1,
        "recorded_at_utc": datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "event_type": event_type,
        "phase": phase,
        "previous_event_id": previous_id,
        "details": dict(details),
    }
    event = {**core, "event_id": sha256_json(core)}
    _create_only(_event_path(outcome_root, event["sequence"]), event)
    return event


def _validate_event_chain(outcome_root: Path) -> None:
    previous_id = None
    events = sorted((outcome_root / "events").glob("*.json"))
    for sequence, path in enumerate(events, start=1):
        event = _read_object(path, "workflow event")
        core = {key: item for key, item in event.items() if key != "event_id"}
        if (
            event.get("schema_version") != EVENT_SCHEMA
            or event.get("sequence") != sequence
            or event.get("previous_event_id") != previous_id
            or event.get("event_id") != sha256_json(core)
        ):
            raise WorkflowError("cockpit workflow event chain is invalid")
        previous_id = event["event_id"]


def initialize_outcome(repo: Path, template_path: Path, output_root: Path) -> Path:
    root = canonical_repo_root(repo)
    template = _read_object(template_path, "cockpit outcome template")
    template["basis"] = git_identity(root)
    spec = build_outcome_spec(template)
    outcome_root = output_root / spec["outcome_id"]
    _create_only(outcome_root / "outcome.json", spec)
    _append_event(
        outcome_root,
        "OUTCOME_INITIALIZED",
        None,
        {"paused_installation": spec["paused_installation"]},
    )
    return outcome_root


def _bound_path(repo: Path, relative: str) -> Path:
    path = (repo / relative).resolve(strict=False)
    try:
        path.relative_to(repo)
    except ValueError as exc:
        raise WorkflowError(f"cockpit workflow path escapes repository: {relative}") from exc
    return path


def es_open_window(
    repo: Path,
    *,
    now: datetime | None = None,
    minimum_seconds: int = 180,
) -> dict[str, Any]:
    instant = now or datetime.now(timezone.utc)
    if instant.tzinfo is None or instant.utcoffset() != timedelta(0):
        raise WorkflowError("calendar gate instant must be timezone-aware UTC")
    instant = instant.astimezone(timezone.utc)
    boundary = RepoBoundary(active_root=repo)
    expected_markets = tuple(
        sorted(item.symbol for item in chart_market_universe())
    )
    index = load_active_calendar_index(
        boundary=boundary, expected_markets=expected_markets
    )
    verify_calendar_freshness(
        index, expected_markets=expected_markets, now=instant
    )
    local_date = instant.astimezone(ZoneInfo(CME_TIMEZONE)).date()
    for offset in (-1, 0, 1):
        trade_date = local_date + timedelta(days=offset)
        try:
            calendar = index.calendar_for("ES", trade_date)
            session = calendar.sessions[("ES", trade_date)]
        except (KeyError, ContractError):
            continue
        for interval in session.intervals:
            if interval.starts_at_utc <= instant < interval.ends_at_utc:
                remaining = int((interval.ends_at_utc - instant).total_seconds())
                eligible = interval.state == "OPEN" and remaining >= minimum_seconds
                return {
                    "eligible": eligible,
                    "state": interval.state,
                    "remaining_open_seconds": remaining if interval.state == "OPEN" else 0,
                    "minimum_open_seconds": minimum_seconds,
                    "trade_date": trade_date.isoformat(),
                }
    return {
        "eligible": False,
        "state": "OUTSIDE_VERIFIED_WINDOW",
        "remaining_open_seconds": 0,
        "minimum_open_seconds": minimum_seconds,
        "trade_date": None,
    }


def _terminal_status(path: Path) -> str | None:
    if not path.is_file():
        return None
    value = _read_object(path, "stage terminal")
    status = value.get("status")
    if status is None:
        schema = value.get("schema_version")
        terminal_state = value.get("terminal_state")
        successful_terminal_states = {
            "live_cockpit_package_candidate_terminal/1.1.0": "CANDIDATE_VERIFIED",
            "live_cockpit_installation_terminal/1.1.0": "INSTALLATION_PREPARED",
        }
        if schema in successful_terminal_states and isinstance(terminal_state, str):
            status = (
                "PASS"
                if terminal_state == successful_terminal_states[schema]
                else "FAILED"
            )
    if status not in {"PASS", "FAIL", "FAILED", "BLOCKED", "INCONCLUSIVE_NO_DATA"}:
        raise WorkflowError(f"unsupported stage terminal status: {status}")
    return str(status)


def _confirmation_request(stage: Mapping[str, Any], plan_path: Path) -> dict[str, str]:
    """Describe a high-risk stage without exposing a pasted approval token."""

    if sha256_file(plan_path) != stage["plan_sha256"]:
        raise WorkflowError("active cockpit plan hash drift")
    return {
        "operation": str(stage["operation"]),
        "confirmation": "Plain-language user confirmation is required before this high-risk stage.",
    }


def workflow_status(
    repo: Path, outcome_root: Path, *, probe_market: bool = True
) -> dict[str, Any]:
    root = canonical_repo_root(repo)
    spec = validate_outcome_spec(
        _read_object(outcome_root / "outcome.json", "cockpit outcome")
    )
    _validate_event_chain(outcome_root)
    for stage in spec["stages"]:
        terminal_path = _bound_path(root, stage["terminal_path"])
        terminal = _terminal_status(terminal_path)
        if terminal == "PASS":
            continue
        if terminal is not None:
            return {
                "status": "BLOCKED",
                "phase": stage["phase"],
                "decision": f"{stage['phase']} ended {terminal}; classify before successor",
            }
        plan_path = _bound_path(root, stage["plan_path"])
        if not plan_path.is_file():
            return {
                "status": "BLOCKED",
                "phase": stage["phase"],
                "decision": "Active stage plan is absent",
            }
        if stage["phase"] == "SMOKE" and probe_market:
            gate = es_open_window(root)
            if not gate["eligible"]:
                return {
                    "status": "WAITING",
                    "phase": "SMOKE",
                    "decision": "Wait for a verified ES OPEN window of at least 180 seconds",
                    "market_gate": gate,
                }
        if stage["authority_class"] == "AUTONOMOUS_READ_ONLY":
            return {
                "status": "READY_AUTONOMOUS",
                "phase": stage["phase"],
                "decision": "Run the bound read-only stage automatically",
            }
        return {
            "status": "APPROVAL_REQUIRED",
            "phase": stage["phase"],
            "decision": f"Confirm the {stage['phase'].lower()} stage in plain language",
            "confirmation_request": _confirmation_request(stage, plan_path),
        }
    return {
        "status": "COMPLETE",
        "phase": None,
        "decision": "Cockpit upgrade outcome is complete",
    }


def resume_outcome(repo: Path, outcome_root: Path) -> dict[str, Any]:
    decision = workflow_status(repo, outcome_root)
    if decision["status"] != "READY_AUTONOMOUS":
        return decision
    raise WorkflowError(
        "legacy cockpit workflow cannot execute new work; run normal local work directly"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="futures-live-cockpit-workflow")
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("--template", type=Path, required=True)
    init.add_argument(
        "--output-root",
        type=Path,
        default=Path("reports/live_cockpit/workflow"),
    )
    for name in ("resume", "status"):
        child = commands.add_parser(name)
        child.add_argument("--outcome-root", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "init":
        result: Any = {
            "outcome_root": str(
                initialize_outcome(args.repo, args.template, args.output_root)
            )
        }
    elif args.command == "status":
        result = workflow_status(args.repo, args.outcome_root)
    elif args.command == "resume":
        result = resume_outcome(args.repo, args.outcome_root)
    else:
        result = resume_outcome(args.repo, args.outcome_root)
    print(canonical_bytes(result).decode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
