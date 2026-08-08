"""Exact-scope local commit planning and execution. No push capability exists."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json

from .policy import WorkflowError, canonical_repo_root, git_identity, run_git


def _scope(repo: Path, paths: list[str]) -> list[dict[str, Any]]:
    scope: list[dict[str, Any]] = []
    for relative in sorted(set(paths)):
        path = (repo / relative).resolve()
        try:
            path.relative_to(repo)
        except ValueError as exc:
            raise WorkflowError(f"commit path escapes repository: {relative}") from exc
        if not path.is_file():
            raise WorkflowError(f"commit path is not a regular file: {relative}")
        normalized = relative.replace("\\", "/")
        blob = run_git(
            repo,
            "hash-object",
            "--path",
            normalized,
            "--",
            normalized,
        ).stdout.strip()
        scope.append(
            {
                "path": normalized,
                "sha256": sha256_file(path),
                "git_blob_sha1": blob,
            }
        )
    if not scope:
        raise WorkflowError("commit scope is empty")
    return scope


def generate_commit_plan(
    repo: Path, terminal_path: Path, paths: list[str], message: str, output: Path
) -> dict[str, Any]:
    root = canonical_repo_root(repo)
    identity = git_identity(root)
    if identity["staged_paths"]:
        raise WorkflowError("staging area must be empty")
    terminal = json.loads(terminal_path.read_text(encoding="utf-8"))
    if terminal.get("status") != "PASS":
        raise WorkflowError("commit plan requires a passing transition terminal")
    scope = _scope(root, paths)
    body = {
        "schema_version": "local_commit_plan/1.0.0",
        "authority_class": "ATOMIC_LOCAL_STAGE_AND_COMMIT",
        "basis": {
            "branch": identity["branch"],
            "head": identity["head"],
            "terminal_path": str(terminal_path.relative_to(root)).replace("\\", "/"),
            "terminal_sha256": sha256_file(terminal_path),
        },
        "paths": scope,
        "working_tree_scope_sha256": sha256_json(scope),
        "expected_staged_scope_sha256": sha256_json(scope),
        "message": message,
        "push_authorized": False,
    }
    body["plan_id"] = sha256_json(body)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as handle:
        handle.write(canonical_bytes(body) + b"\n")
    return body


def execute_commit(repo: Path, plan_path: Path, approval_line: str) -> str:
    root = canonical_repo_root(repo)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    if plan.get("schema_version") != "local_commit_plan/1.0.0":
        raise WorkflowError("unsupported local commit plan")
    plan_sha = sha256_file(plan_path)
    expected = f"APPROVE LOCAL_STAGE_AND_COMMIT PLAN {plan['plan_id']} SHA256 {plan_sha}"
    if approval_line != expected:
        raise WorkflowError("literal local commit approval does not match")
    identity = git_identity(root)
    if identity["staged_paths"]:
        raise WorkflowError("staging area is not empty")
    if identity["head"] != plan["basis"]["head"] or identity["branch"] != plan["basis"]["branch"]:
        raise WorkflowError("commit-plan repository identity drift")
    paths = [entry["path"] for entry in plan["paths"]]
    if sha256_json(_scope(root, paths)) != plan["working_tree_scope_sha256"]:
        raise WorkflowError("commit scope content drift")
    run_git(root, "add", "--", *paths)
    try:
        staged = git_identity(root)["staged_paths"]
        if sorted(staged) != sorted(paths):
            raise WorkflowError("staged path set mismatch")
        index_scope: list[dict[str, str]] = []
        for entry in plan["paths"]:
            staged_line = run_git(
                root, "ls-files", "--stage", "--", entry["path"]
            ).stdout.strip()
            fields = staged_line.split(maxsplit=3)
            if len(fields) != 4:
                raise WorkflowError(f"cannot resolve staged blob: {entry['path']}")
            index_scope.append(
                {
                    "path": entry["path"],
                    "sha256": entry["sha256"],
                    "git_blob_sha1": fields[1],
                }
            )
        if sha256_json(index_scope) != plan["expected_staged_scope_sha256"]:
            raise WorkflowError("staged content hash mismatch")
        run_git(root, "commit", "-m", plan["message"])
    except Exception:
        run_git(root, "restore", "--staged", "--", *paths, check=False)
        raise
    return run_git(root, "rev-parse", "HEAD").stdout.strip()
