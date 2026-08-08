"""Policy loading and invariant checks for the closure workflow."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

from futures_rebuild.canonical import is_linklike, sha256_file
from futures_rebuild.errors import ContractError, IntegrityError


EXPECTED_PROJECT = "futures-intraday-model-v2"
REQUIRED_ROOT_FILES = (
    "AGENTS.md",
    "PROJECT_OUTLINE.md",
    "CODEX_HANDOFF.md",
    "pyproject.toml",
    "configs/source_contract.json",
)


class WorkflowError(ContractError):
    """A closure-workflow contract was not satisfied."""


def run_git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=False,
        capture_output=True,
        text=True,
    )
    if check and result.returncode:
        raise WorkflowError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result


def canonical_repo_root(start: Path) -> Path:
    root_text = run_git(start, "rev-parse", "--show-toplevel").stdout.strip()
    root = Path(root_text).resolve()
    if is_linklike(root):
        raise WorkflowError(f"repository root is link-like: {root}")
    if (root / ".git").is_dir() is False:
        raise WorkflowError(f"repository is not a primary worktree: {root}")
    for relative in REQUIRED_ROOT_FILES:
        if not (root / relative).is_file():
            raise WorkflowError(f"required root file is absent: {relative}")
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    if f'name = "{EXPECTED_PROJECT}"' not in pyproject:
        raise WorkflowError("pyproject project identity mismatch")
    return root


def load_policy(repo: Path) -> dict[str, Any]:
    path = repo / "configs" / "closure_workflow_policy.json"
    if not path.is_file():
        raise IntegrityError(f"closure workflow policy is absent: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema_version") not in {
        "closure_workflow_policy/1.0.0",
        "two_tier_workflow_policy/2.0.0",
    }:
        raise WorkflowError("unsupported closure workflow policy schema")
    return value


def closure_workflow_is_legacy(policy: dict[str, Any]) -> bool:
    """Return whether new closure-workflow plans are intentionally retired."""

    return policy.get("schema_version") == "two_tier_workflow_policy/2.0.0"


def assert_file_hash(repo: Path, relative: str, expected: str) -> None:
    path = (repo / relative).resolve()
    try:
        path.relative_to(repo)
    except ValueError as exc:
        raise WorkflowError(f"path escapes repository: {relative}") from exc
    actual = sha256_file(path)
    if actual != expected:
        raise WorkflowError(
            f"hash mismatch for {relative}: expected {expected}, observed {actual}"
        )


def git_identity(repo: Path) -> dict[str, Any]:
    status = run_git(repo, "status", "--short", "--untracked-files=all").stdout
    return {
        "branch": run_git(repo, "branch", "--show-current").stdout.strip(),
        "head": run_git(repo, "rev-parse", "HEAD").stdout.strip(),
        "status_lines": status.splitlines(),
        "staged_paths": [
            line
            for line in run_git(repo, "diff", "--cached", "--name-only")
            .stdout.splitlines()
            if line
        ],
    }
