"""Typed transition generation, authentication, execution, and status."""

from __future__ import annotations

import json
import shutil
import subprocess
import time
from fnmatch import fnmatch
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json

from .policy import (
    WorkflowError,
    assert_file_hash,
    canonical_repo_root,
    git_identity,
    closure_workflow_is_legacy,
    load_policy,
)


Executor = Callable[[list[str], Path], subprocess.CompletedProcess[str]]
MUTATING_ACTIONS = {"no_overwrite_backup", "frozen_patch"}
PREFLIGHT_ACTIONS = {"reconciliation", "preflight_command"}
TARGET_ACTIONS = {
    "focused_pytest",
    "windows_host_root_suite",
    "meta_audit",
    "master_audit",
}


def _write_create_only(path: Path, value: dict[str, Any]) -> None:
    data = canonical_bytes(value) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(data)
    except FileExistsError:
        if path.read_bytes() != data:
            raise WorkflowError(f"refusing overwrite: {path}")


def _default_executor(argv: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        shell=False,
    )


def validate_transition_plan(plan: dict[str, Any], policy: dict[str, Any]) -> None:
    if closure_workflow_is_legacy(policy):
        raise WorkflowError(
            "closure workflow is retired for new work; historic plans remain readable evidence"
        )
    if plan.get("schema_version") not in {
        "closure_transition_plan/2.0.0",
        "closure_transition_plan/2.1.0",
    }:
        raise WorkflowError("unsupported transition-plan schema")
    authority = plan.get("authority_class")
    if authority not in {"AUTONOMOUS_READ_ONLY", "PROVIDER_FREE_LOCAL_TRANSITION"}:
        raise WorkflowError("unsupported transition authority class")
    action_types = [action.get("type") for action in plan.get("actions", [])]
    allowed = set(policy["authority_classes"][authority])
    unknown = [item for item in action_types if item not in allowed]
    if unknown:
        raise WorkflowError(f"action types are not allowlisted: {unknown}")
    if (
        authority == "PROVIDER_FREE_LOCAL_TRANSITION"
        and not any(item in MUTATING_ACTIONS for item in action_types)
    ):
        raise WorkflowError("provider-free transition must contain a declared mutation")
    if authority == "AUTONOMOUS_READ_ONLY" and any(
        item in MUTATING_ACTIONS for item in action_types
    ):
        raise WorkflowError("autonomous read-only plan cannot mutate")
    ids = [action.get("id") for action in plan["actions"]]
    if not ids or len(ids) != len(set(ids)) or any(not item for item in ids):
        raise WorkflowError("action IDs must be nonempty and unique")
    seen: set[str] = set()
    action_type_by_id: dict[str, str] = {}
    for action in plan["actions"]:
        dependencies = action.get("depends_on", [])
        if any(item not in seen for item in dependencies):
            raise WorkflowError(f"action dependency is not topological: {action['id']}")
        seen.add(action["id"])
        action_type_by_id[action["id"]] = action["type"]
        if "argv" in action:
            if not isinstance(action["argv"], list) or not all(
                isinstance(item, str) for item in action["argv"]
            ):
                raise WorkflowError(f"argv must be a string array: {action['id']}")
            _validate_argv(action, policy)
        if action["type"] in {"no_overwrite_backup", "frozen_patch"}:
            mutable_paths = [
                action[key]
                for key in ("destination", "target")
                if key in action
            ]
            for path in mutable_paths:
                normalized = path.replace("\\", "/")
                if any(
                    fnmatch(normalized, pattern)
                    for pattern in policy["protected_path_patterns"]
                ):
                    raise WorkflowError(f"action targets protected path: {path}")
        dependency_types = {
            action_type_by_id[item] for item in action.get("depends_on", [])
        }
        if action["type"] == "meta_audit" and "windows_host_root_suite" not in dependency_types:
            raise WorkflowError("Meta Audit must depend directly on the full suite")
        if action["type"] == "master_audit" and "meta_audit" not in dependency_types:
            raise WorkflowError("Master Audit must depend directly on Meta Audit")
    limits = plan.get("limits", {})
    if limits.get("maximum_retries_after_start") != 0:
        raise WorkflowError("post-start retries must be zero")
    if limits.get("maximum_prestart_launcher_retries") not in (0, 1):
        raise WorkflowError("pre-start launcher retry limit must be zero or one")
    claimed_id = plan.get("plan_id")
    unhashed = {key: value for key, value in plan.items() if key != "plan_id"}
    if claimed_id != sha256_json(unhashed):
        raise WorkflowError("transition plan content hash does not match plan_id")


def _validate_argv(action: dict[str, Any], policy: dict[str, Any]) -> None:
    argv = action["argv"]
    executable = argv[0].replace("/", "\\").lower()
    allowlist = {
        item.replace("/", "\\").lower() for item in policy["executable_allowlist"]
    }
    if executable not in allowlist:
        raise WorkflowError(f"executable is not allowlisted: {argv[0]}")
    kind = action["type"]
    joined = " ".join(argv).lower()
    if kind == "focused_pytest":
        if argv[1:3] != ["-m", "pytest"] or "tests" not in joined:
            raise WorkflowError("focused_pytest must use repository venv pytest")
    elif kind == "preflight_command":
        if argv[1:3] != ["-m", "pytest"] or not any(
            item in argv for item in ("--collect-only", "--help")
        ):
            raise WorkflowError(
                "preflight_command must be a non-executing repository venv pytest probe"
            )
    elif kind == "windows_host_root_suite":
        if "run_windows_host_root_pytest.ps1" not in joined:
            raise WorkflowError("suite must use the Windows host-root launcher")
        if "--basetemp" in joined:
            raise WorkflowError("explicit basetemp is forbidden")
    elif kind == "meta_audit" and "futures-meta-audit.exe" not in executable:
        raise WorkflowError("Meta Audit must use its explicit venv entry point")
    elif kind == "master_audit" and "futures-master-audit.exe" not in executable:
        raise WorkflowError("Master Audit must use its explicit venv entry point")


def _replace_tokens(value: str, repo: Path, run_root: Path, plan_id: str) -> str:
    relative_run_root = str(run_root.relative_to(repo)).replace("/", "\\")
    return (
        value.replace("<PLAN_ID>", plan_id)
        .replace("{PLAN_ID}", plan_id)
        .replace("{RUN_ROOT}", relative_run_root)
    )


def _replace_strings(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        for old, new in replacements.items():
            value = value.replace(old, new)
        return value
    if isinstance(value, list):
        return [_replace_strings(item, replacements) for item in value]
    if isinstance(value, dict):
        return {
            key: _replace_strings(item, replacements) for key, item in value.items()
        }
    return value


def generate_transition(
    spec: dict[str, Any], output_root: Path, policy: dict[str, Any]
) -> tuple[Path, Path, dict[str, Any]]:
    body = dict(spec)
    body["schema_version"] = "closure_transition_plan/2.1.0"
    authority = body.get("authority_class")
    body["status"] = (
        "READY_AUTONOMOUS"
        if authority == "AUTONOMOUS_READ_ONLY"
        else "PENDING_EXACT_APPROVAL"
    )
    body["plan_id"] = sha256_json(body)
    validate_transition_plan(body, policy)
    plan_path = output_root / "plans" / f"{body['plan_id']}.json"
    _write_create_only(plan_path, body)
    plan_sha = sha256_file(plan_path)
    approval_line = ""
    if authority == "PROVIDER_FREE_LOCAL_TRANSITION":
        approval_line = (
            "APPROVE RUN_PROVIDER_FREE_LOCAL_TRANSITION "
            f"PLAN {body['plan_id']} SHA256 {plan_sha}"
        )
    request = {
        "schema_version": "closure_transition_approval_request/2.0.0",
        "operation": (
            "RUN_AUTONOMOUS_READ_ONLY"
            if authority == "AUTONOMOUS_READ_ONLY"
            else "RUN_PROVIDER_FREE_LOCAL_TRANSITION"
        ),
        "plan_id": body["plan_id"],
        "plan_sha256": plan_sha,
        "approval_to_paste": approval_line,
        "status": (
            "READY_AUTONOMOUS"
            if authority == "AUTONOMOUS_READ_ONLY"
            else "WAITING_FOR_EXACT_USER_APPROVAL"
        ),
    }
    request_path = output_root / "approvals" / f"{body['plan_id']}.json"
    _write_create_only(request_path, request)
    return plan_path, request_path, request


@dataclass
class TransitionRunner:
    repo: Path
    plan_path: Path
    approval_line: str = ""
    executor: Executor = _default_executor

    def run(self) -> dict[str, Any]:
        started_at = time.monotonic()
        repo = canonical_repo_root(self.repo)
        policy = load_policy(repo)
        plan = json.loads(self.plan_path.read_text(encoding="utf-8"))
        validate_transition_plan(plan, policy)
        plan_sha = sha256_file(self.plan_path)
        authority = plan["authority_class"]
        if authority == "PROVIDER_FREE_LOCAL_TRANSITION":
            expected = (
                "APPROVE RUN_PROVIDER_FREE_LOCAL_TRANSITION "
                f"PLAN {plan['plan_id']} SHA256 {plan_sha}"
            )
            if self.approval_line != expected:
                raise WorkflowError("literal hash-bound approval does not match")
        elif self.approval_line:
            raise WorkflowError("autonomous read-only run rejects approval text")
        run_root = (
            repo
            / "reports"
            / "workflow"
            / "closure"
            / plan["plan_id"]
            / "attempt-001"
        )
        progress_path = run_root / "progress.json"
        progress = self._initial_progress(plan, plan_sha)
        if progress_path.exists():
            progress = json.loads(progress_path.read_text(encoding="utf-8"))
            if progress.get("target_started"):
                raise WorkflowError("attempt already started; retries are forbidden")
            if progress.get("prestart_retry_count", 0) >= 1:
                raise WorkflowError("pre-start launcher retry already consumed")
            progress["prestart_retry_count"] += 1
            progress["state"] = "AUTHENTICATED"
            progress["status"] = "RUNNING"
            progress.pop("failure", None)
        else:
            run_root.mkdir(parents=True, exist_ok=False)
        self._replace_progress(progress_path, progress)

        suite_actions = [
            action
            for action in plan["actions"]
            if action["type"] == "windows_host_root_suite"
        ]
        if len(suite_actions) > 1:
            raise WorkflowError("a transition may contain at most one full suite")
        if suite_actions and not progress.get("host_root_preflight_passed", False):
            try:
                self._windows_host_root_preflight(suite_actions[0], repo)
            except Exception as exc:
                progress["status"] = "FAILED"
                progress["state"] = "PREFLIGHT_FAILED_RESUMABLE"
                progress["failure"] = {
                    "action_id": suite_actions[0]["id"],
                    "message": str(exc),
                }
                self._replace_progress(progress_path, progress)
                raise
            progress["host_root_preflight_passed"] = True
            progress["state"] = "PREFLIGHT_PASSED"
            self._replace_progress(progress_path, progress)

        completed: set[str] = set(progress["completed_actions"])
        substantive_seconds = 0.0
        for action in plan["actions"]:
            if action["id"] in completed:
                continue
            if any(item not in completed for item in action.get("depends_on", [])):
                raise WorkflowError(f"unsatisfied dependency for {action['id']}")
            if action.get("condition") == "PREVIOUS_ACTIONS_PASS" and progress["status"] != "RUNNING":
                progress["skipped_actions"].append(action["id"])
                continue
            if action["type"] in MUTATING_ACTIONS or (
                action["type"] in TARGET_ACTIONS
                and plan["authority_class"] == "PROVIDER_FREE_LOCAL_TRANSITION"
            ):
                progress["approval_consumed"] = True
                progress["state"] = "STARTED"
            if action["type"] in TARGET_ACTIONS:
                progress["target_started"] = True
                action_started = time.monotonic()
            try:
                receipt = self._execute(action, repo, run_root, plan)
            except Exception as exc:
                progress["status"] = "FAILED"
                progress["failure"] = {"action_id": action["id"], "message": str(exc)}
                if not progress["target_started"] and not progress["approval_consumed"]:
                    progress["state"] = "PREFLIGHT_FAILED_RESUMABLE"
                self._replace_progress(progress_path, progress)
                if progress["approval_consumed"] or progress["target_started"]:
                    self._record_terminal(
                        run_root,
                        plan,
                        progress,
                        started_at,
                        substantive_seconds,
                    )
                raise
            if action["type"] in TARGET_ACTIONS:
                substantive_seconds += time.monotonic() - action_started
            progress["receipts"].append(receipt)
            progress["completed_actions"].append(action["id"])
            completed.add(action["id"])
            if (
                action["type"] == "reconciliation"
                and not progress["approval_consumed"]
                and not progress["target_started"]
            ):
                progress["state"] = "PREFLIGHT_PASSED"
            self._replace_progress(progress_path, progress)
        progress["status"] = "PASS"
        progress["state"] = "TERMINAL"
        self._replace_progress(progress_path, progress)
        return self._record_terminal(
            run_root, plan, progress, started_at, substantive_seconds
        )

    @staticmethod
    def _initial_progress(plan: dict[str, Any], plan_sha: str) -> dict[str, Any]:
        return {
            "schema_version": "closure_progress/1.0.0",
            "plan_id": plan["plan_id"],
            "plan_sha256": plan_sha,
            "state": "AUTHENTICATED",
            "status": "RUNNING",
            "approval_count": (
                1 if plan["authority_class"] == "PROVIDER_FREE_LOCAL_TRANSITION" else 0
            ),
            "approval_consumed": False,
            "target_started": False,
            "host_root_preflight_passed": False,
            "prestart_retry_count": 0,
            "completed_actions": [],
            "skipped_actions": [],
            "receipts": [],
        }

    @staticmethod
    def _replace_progress(path: Path, value: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_bytes(canonical_bytes(value) + b"\n")
        temporary.replace(path)

    def _execute(
        self, action: dict[str, Any], repo: Path, run_root: Path, plan: dict[str, Any]
    ) -> dict[str, Any]:
        kind = action["type"]
        if kind == "reconciliation":
            identity = git_identity(repo)
            basis = plan["basis"]
            if identity["head"] != basis["head"] or identity["branch"] != basis["branch"]:
                raise WorkflowError("repository identity drift")
            for binding in action.get("file_hashes", []):
                assert_file_hash(repo, binding["path"], binding["sha256"])
            for relative in action.get("required_absent", []):
                if (repo / relative).exists():
                    raise WorkflowError(f"required absence drift: {relative}")
            return {"action_id": action["id"], "status": "PASS", "target_started": False}
        if kind == "preflight_command":
            result = self.executor(list(action["argv"]), repo)
            if result.returncode:
                raise WorkflowError(f"{action['id']} exited {result.returncode}")
            return {
                "action_id": action["id"],
                "status": "PASS",
                "exit_code": 0,
                "target_started": True,
            }
        if kind == "no_overwrite_backup":
            source = repo / action["source"]
            destination = repo / _replace_tokens(
                action["destination"], repo, run_root, plan["plan_id"]
            )
            if destination.exists():
                raise WorkflowError(f"backup destination exists: {action['destination']}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            with source.open("rb") as reader, destination.open("xb") as writer:
                shutil.copyfileobj(reader, writer)
            expected = action["source_sha256"]
            if sha256_file(destination) != expected:
                raise WorkflowError("backup verification failed")
            return {"action_id": action["id"], "status": "PASS", "sha256": expected}
        if kind == "frozen_patch":
            assert_file_hash(repo, action["patch"], action["patch_sha256"])
            check = self.executor(
                ["git", "apply", "--check", "--", action["patch"]], repo
            )
            if check.returncode:
                raise WorkflowError(check.stderr.strip() or "patch preflight failed")
            result = self.executor(["git", "apply", "--", action["patch"]], repo)
            if result.returncode:
                raise WorkflowError(result.stderr.strip() or "patch application failed")
            assert_file_hash(repo, action["target"], action["postimage_sha256"])
            return {"action_id": action["id"], "status": "PASS"}
        if kind == "windows_host_root_suite":
            result = self.executor(
                [
                    _replace_tokens(item, repo, run_root, plan["plan_id"])
                    for item in action["argv"]
                ],
                repo,
            )
        elif kind in TARGET_ACTIONS:
            argv = [
                _replace_tokens(item, repo, run_root, plan["plan_id"])
                for item in action["argv"]
            ]
            if kind == "master_audit":
                self._materialize_master_invocation(action, repo, run_root, plan)
            result = self.executor(argv, repo)
        elif kind == "terminal_recording":
            return {"action_id": action["id"], "status": "PASS"}
        else:
            raise WorkflowError(f"unimplemented typed action: {kind}")
        stdout_path = run_root / f"{action['id']}.stdout.log"
        stderr_path = run_root / f"{action['id']}.stderr.log"
        stdout_path.write_text(result.stdout, encoding="utf-8")
        stderr_path.write_text(result.stderr, encoding="utf-8")
        if result.returncode:
            raise WorkflowError(f"{action['id']} exited {result.returncode}")
        return {
            "action_id": action["id"],
            "status": "PASS",
            "exit_code": result.returncode,
            "stdout_sha256": sha256_file(stdout_path),
            "stderr_sha256": sha256_file(stderr_path),
            "target_started": True,
        }

    def _windows_host_root_preflight(
        self, action: dict[str, Any], repo: Path
    ) -> None:
        argv = list(action["argv"])
        try:
            script_index = next(
                index
                for index, value in enumerate(argv)
                if value.replace("/", "\\").lower().endswith(
                    "scripts\\run_windows_host_root_pytest.ps1"
                )
            )
        except StopIteration as exc:
            raise WorkflowError("host-root launcher script is absent from argv") from exc
        preflight = argv[: script_index + 1] + ["-PreflightOnly"]
        result = self.executor(preflight, repo)
        if result.returncode:
            raise WorkflowError(
                result.stderr.strip() or "Windows host-root capability preflight failed"
            )

    @staticmethod
    def _materialize_master_invocation(
        action: dict[str, Any], repo: Path, run_root: Path, plan: dict[str, Any]
    ) -> None:
        source_relative = action["base_invocation"]
        assert_file_hash(repo, source_relative, action["base_invocation_sha256"])
        invocation = json.loads((repo / source_relative).read_text(encoding="utf-8"))
        invocation = _replace_strings(invocation, action.get("text_replacements", {}))
        overrides = action.get("evidence_overrides", {})
        for evidence in invocation.get("evidence", []):
            evidence_id = evidence["evidence_id"]
            if evidence_id in overrides:
                override = overrides[evidence_id]
                evidence["path"] = _replace_tokens(
                    override["path"], repo, run_root, plan["plan_id"]
                ).replace("\\", "/")
                evidence["limitations"] = override.get(
                    "limitations", evidence.get("limitations", [])
                )
            path = repo / evidence["path"]
            if evidence.get("safe_to_read") is True and path.is_file():
                evidence["bytes"] = path.stat().st_size
                evidence["sha256"] = sha256_file(path)
        destination_relative = _replace_tokens(
            action["invocation_output"], repo, run_root, plan["plan_id"]
        )
        destination = repo / destination_relative
        _write_create_only(destination, invocation)

    @staticmethod
    def _record_terminal(
        run_root: Path,
        plan: dict[str, Any],
        progress: dict[str, Any],
        started_at: float,
        substantive_seconds: float,
    ) -> dict[str, Any]:
        files = [path for path in run_root.rglob("*") if path.is_file()]
        terminal = {
            "schema_version": "closure_terminal/2.0.0",
            "plan_id": plan["plan_id"],
            "status": progress["status"],
            "target_started": progress["target_started"],
            "approval_count": progress["approval_count"],
            "approval_consumed": progress["approval_consumed"],
            "prestart_retry_count": progress["prestart_retry_count"],
            "control_plane_seconds": round(time.monotonic() - started_at - substantive_seconds, 6),
            "substantive_execution_seconds": round(substantive_seconds, 6),
            "artifact_count": len(files),
            "artifact_bytes": sum(path.stat().st_size for path in files),
            "full_census_count": plan.get("full_census_count", 0),
            "delta_entry_count": plan.get("delta_entry_count", 0),
            "delta_bytes": plan.get("delta_bytes", 0),
            "retry_count": progress["prestart_retry_count"],
            "snapshot_reused": plan.get("snapshot_reused", False),
            "reused_evidence_count": len(plan.get("reused_evidence", [])),
            "failure": progress.get("failure"),
        }
        terminal["terminal_id"] = sha256_json(terminal)
        _write_create_only(run_root / "terminal.json", terminal)
        return terminal


def read_status(run_root: Path) -> dict[str, Any]:
    progress = run_root / "progress.json"
    if not progress.is_file():
        raise WorkflowError(f"progress receipt is absent: {progress}")
    return json.loads(progress.read_text(encoding="utf-8"))
