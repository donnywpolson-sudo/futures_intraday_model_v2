"""Create-only, hash-bound packaging for the observation-only live cockpit."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any, Mapping, Sequence

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.live_cockpit.approval import (
    PREDECESSOR_ATTEMPT,
    RESULT_OUTPUT_RELATIVE,
    build_live_smoke_plan,
    validate_live_smoke_plan,
)


PLAN_SCHEMA = "live_cockpit_package_candidate_plan/1.1.0"
APPROVAL_SCHEMA = "live_cockpit_package_candidate_approval/1.1.0"  # historic reader
CONFIRMATION_SCHEMA = "live_cockpit_package_candidate_confirmation/2.0.0"
INVENTORY_SCHEMA = "live_cockpit_package_candidate_inventory/1.1.0"
CANDIDATE_SCHEMA = "live_cockpit_package_candidate_receipt/1.1.0"
TERMINAL_SCHEMA = "live_cockpit_package_candidate_terminal/1.1.0"
OPERATION = "RUN_LIVE_COCKPIT_PACKAGE_CANDIDATE"
MAX_DURATION_SECONDS = 900
SELF_CHECK_TIMEOUT_SECONDS = 60
MAX_CANDIDATE_FILES = 5_000
MAX_CANDIDATE_BYTES = 750_000_000
PLAN_PREFIX_LENGTH = 16
MAX_WINDOWS_PACKAGE_PATH = 240
MAX_PACKAGE_RELATIVE_PATH_CHARS = 120
PLAN_ROOT = Path("manifests/live_cockpit/package_candidate/plans")
APPROVAL_ROOT = Path("manifests/live_cockpit/package_candidate/approvals")
ARTIFACT_TEMPLATE = "artifacts/flcp/<PLAN_PREFIX>"
REPORT_TEMPLATE = "reports/live_cockpit/package_candidate/<PLAN_ID>"
SCRATCH_TEMPLATE = str(Path(tempfile.gettempdir()) / "flcp" / "<PLAN_PREFIX>")
SUCCESSFUL_CANARY = (
    "reports/live_cockpit/history_canary/"
    "ef7fb64d66a2735f05c2d0f8cacea4e7329ece29ae42e245d0d10a51e0dade95/"
    "terminal.json"
)
REMEDIATION_TERMINAL = (
    "reports/workflow/closure/"
    "6aa0d2c432718d12835917ff19b643567139fac769beb92ba175a2cee93855df/"
    "attempt-001/terminal.json"
)
SMOKE_PLAN_PLACEHOLDER = "configs/live_cockpit_smoke_plan.json"
SMOKE_PLAN_PLACEHOLDER_SHA256 = (
    "ab15788ee839298c32e6561bc5eb993a9902a3ba539899d8fdc0d6b8c704407e"
)

RUNTIME_OVERLAYS = (
    "src/futures_rebuild/live_cockpit/__init__.py",
    "src/futures_rebuild/live_cockpit/__main__.py",
    "src/futures_rebuild/live_cockpit/app.py",
    "src/futures_rebuild/live_cockpit/approval.py",
    "src/futures_rebuild/live_cockpit/assets/NOTICE-lightweight-charts.txt",
    "src/futures_rebuild/live_cockpit/assets/app.js",
    "src/futures_rebuild/live_cockpit/assets/index.html",
    "src/futures_rebuild/live_cockpit/assets/lightweight-charts.standalone.production.js",
    "src/futures_rebuild/live_cockpit/assets/styles.css",
    "src/futures_rebuild/live_cockpit/assets/time-format.js",
    "src/futures_rebuild/live_cockpit/cache.py",
    "src/futures_rebuild/live_cockpit/credentials.py",
    "src/futures_rebuild/live_cockpit/cutover_guard.py",
    "src/futures_rebuild/live_cockpit/databento_auth.py",
    "src/futures_rebuild/live_cockpit/engine.py",
    "src/futures_rebuild/live_cockpit/execution/__init__.py",
    "src/futures_rebuild/live_cockpit/execution/adapter.py",
    "src/futures_rebuild/live_cockpit/execution/arm_state.py",
    "src/futures_rebuild/live_cockpit/execution/config.py",
    "src/futures_rebuild/live_cockpit/execution/credential_store.py",
    "src/futures_rebuild/live_cockpit/execution/domain.py",
    "src/futures_rebuild/live_cockpit/execution/errors.py",
    "src/futures_rebuild/live_cockpit/execution/fake.py",
    "src/futures_rebuild/live_cockpit/execution/gate.py",
    "src/futures_rebuild/live_cockpit/execution/order_ledger.py",
    "src/futures_rebuild/live_cockpit/execution/reconciliation.py",
    "src/futures_rebuild/live_cockpit/execution/runtime.py",
    "src/futures_rebuild/live_cockpit/execution/tradovate_adapter.py",
    "src/futures_rebuild/live_cockpit/execution/tradovate_auth.py",
    "src/futures_rebuild/live_cockpit/execution/tradovate_rest.py",
    "src/futures_rebuild/live_cockpit/execution/tradovate_websocket.py",
    "src/futures_rebuild/live_cockpit/feed.py",
    "src/futures_rebuild/live_cockpit/history.py",
    "src/futures_rebuild/live_cockpit/market_groups.py",
    "src/futures_rebuild/live_cockpit/observation_status.py",
    "src/futures_rebuild/live_cockpit/predictions.py",
    "src/futures_rebuild/live_cockpit/protocol.py",
    "src/futures_rebuild/live_cockpit/smoke.py",
    "src/futures_rebuild/live_cockpit/single_instance.py",
)
PACKAGE_INPUTS = (
    "src/futures_rebuild/live_cockpit/package_candidate.py",
    *RUNTIME_OVERLAYS,
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/errors.py",
    "FuturesLiveCockpit/_internal/FuturesLiveCockpit.spec",
    "FuturesLiveCockpit/_internal/futures_live_cockpit.py",
    "configs/alpha_tiered.yaml",
    "configs/prop_firm_execution_connections.json",
    "configs/prop_firm_profiles.json",
    "configs/prop_firm_execution_costs.json",
    "configs/prop_firm_execution_instruments.json",
    "configs/prop_firm_strategy_risk_policies.json",
    "configs/prop_firm_payout_policies.json",
    "configs/live_cockpit_smoke_plan.json",
    "configs/source_contract.json",
    "configs/dependency_lock_receipt.json",
    "configs/environment.lock.json",
    "configs/offline_vault_environment.lock.json",
    "configs/runtime_wheel_lock.json",
    "THIRD_PARTY_NOTICES.md",
    "pyproject.toml",
    "requirements-runtime.lock",
    "requirements.lock",
    "requirements.sha256.lock",
    SUCCESSFUL_CANARY,
    REMEDIATION_TERMINAL,
    "FuturesLiveCockpit/FuturesLiveCockpit.exe",
    "reports/live_cockpit/cockpit_activation_verification.json",
)
ARCHIVE_PATHS = (
    "src/futures_rebuild",
    "FuturesLiveCockpit/_internal/FuturesLiveCockpit.spec",
    "FuturesLiveCockpit/_internal/futures_live_cockpit.py",
    "configs/alpha_tiered.yaml",
    "configs/live_cockpit_smoke_plan.json",
    "THIRD_PARTY_NOTICES.md",
)
FORBIDDEN_PACKAGE_NAMES = frozenset(
    {
        "api.env",
        "databento.env",
        "credential-source.json",
        ".env",
    }
)
FORBIDDEN_PACKAGE_PATH_PARTS = frozenset(
    {
        "authorization_uses",
        "unpublished_evidence",
        "execution_binding.json",
    }
)
PRIVATE_KEY_HEADERS = (
    b"-----BEGIN PRIVATE KEY-----",
    b"-----BEGIN RSA PRIVATE KEY-----",
    b"-----BEGIN EC PRIVATE KEY-----",
    b"-----BEGIN OPENSSH PRIVATE KEY-----",
)


class PackageCandidateError(RuntimeError):
    """The package-candidate contract is absent, stale, or violated."""


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args],
        check=False,
        capture_output=True,
        text=True,
        shell=False,
    )
    if result.returncode:
        raise PackageCandidateError("repository identity is unavailable")
    return result.stdout.strip()


def _write_create_only(path: Path, value: Mapping[str, Any]) -> None:
    data = canonical_bytes(dict(value)) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(data)
    except FileExistsError as exc:
        raise PackageCandidateError(f"create-only output exists: {path.name}") from exc


def _input_hashes(root: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": path,
            "bytes": (root / path).stat().st_size,
            "sha256": sha256_file(root / path),
        }
        for path in PACKAGE_INPUTS
    ]


def _validate_dependency_lock(root: Path) -> str:
    path = root / "configs/dependency_lock_receipt.json"
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PackageCandidateError("dependency lock receipt is unavailable") from exc
    if not isinstance(receipt, Mapping) or set(receipt) != {
        "files",
        "receipt_id",
        "receipt_version",
        "runtime",
    }:
        raise PackageCandidateError("dependency lock receipt is malformed")
    core = {key: receipt[key] for key in receipt if key != "receipt_id"}
    if receipt.get("receipt_id") != sha256_json(core):
        raise PackageCandidateError("dependency lock receipt identity mismatch")
    files = receipt.get("files")
    if not isinstance(files, list):
        raise PackageCandidateError("dependency lock file bindings are malformed")
    for item in files:
        if (
            not isinstance(item, Mapping)
            or not isinstance(item.get("path"), str)
            or not isinstance(item.get("sha256"), str)
            or sha256_file(root / str(item["path"])) != item["sha256"]
        ):
            raise PackageCandidateError("dependency lock file binding mismatch")
    runtime = receipt.get("runtime")
    if (
        not isinstance(runtime, Mapping)
        or runtime.get("implementation") != platform.python_implementation()
        or runtime.get("platform") != sys.platform
        or runtime.get("python") != platform.python_version()
        or not isinstance(runtime.get("packages"), Mapping)
    ):
        raise PackageCandidateError("dependency lock runtime mismatch")
    actual_packages = {
        str(package): importlib.metadata.version(str(package))
        for package in runtime["packages"]
    }
    if actual_packages != runtime["packages"]:
        raise PackageCandidateError("dependency lock package mismatch")
    return str(receipt["receipt_id"])


def _validate_canary(root: Path) -> str:
    path = root / SUCCESSFUL_CANARY
    try:
        terminal = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PackageCandidateError("successful canary terminal is unavailable") from exc
    if not isinstance(terminal, Mapping):
        raise PackageCandidateError("successful canary terminal is malformed")
    body = dict(terminal)
    terminal_id = body.pop("terminal_id", None)
    counts = body.get("request_counts")
    if (
        terminal_id != sha256_json(body)
        or body.get("terminal_state") != "CONFIRMATION_REQUIRED"
        or body.get("estimated_cost_usd") != 0.0
        or not isinstance(counts, Mapping)
        or counts.get("timeseries_download") != 0
        or counts.get("live_client") != 0
        or counts.get("production_cache_write") != 0
    ):
        raise PackageCandidateError("successful canary terminal does not match")
    return str(terminal_id)


def build_plan(root: Path) -> dict[str, Any]:
    canonical_root = Path(_git(root, "rev-parse", "--show-toplevel")).resolve()
    if canonical_root != root.resolve():
        raise PackageCandidateError("repository root mismatch")
    if root.is_symlink() or bool(
        getattr(root.stat(), "st_file_attributes", 0) & 0x400
    ):
        raise PackageCandidateError("repository root is link-like")
    body: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA,
        "operation": OPERATION,
        "basis": {
            "repository": str(root.resolve()),
            "branch": _git(root, "branch", "--show-current"),
            "head": _git(root, "rev-parse", "HEAD"),
        },
        "inputs": _input_hashes(root),
        "reviewed_successor": {
            "remediation_terminal_id": (
                "bd857807fbe145e9b2dd7ea0a490e57c0c9a5769314d582127cb4398ee20c0aa"
            ),
            "successful_canary_terminal_id": _validate_canary(root),
            "runtime_overlay_count": len(RUNTIME_OVERLAYS),
            "smoke_plan_finalization": "AFTER_EXECUTABLE_HASH_BEFORE_INVENTORY",
            "smoke_plan_placeholder_sha256": SMOKE_PLAN_PLACEHOLDER_SHA256,
            "smoke_plan_predecessor_attempt": dict(PREDECESSOR_ATTEMPT),
            "smoke_result_output_relative": RESULT_OUTPUT_RELATIVE,
        },
        "source_isolation": {
            "base": "EXACT_GIT_HEAD_ARCHIVE",
            "archive_paths": list(ARCHIVE_PATHS),
            "overlays": list(RUNTIME_OVERLAYS),
            "working_tree_other_paths": "EXCLUDED",
        },
        "dependency_lock_receipt_id": _validate_dependency_lock(root),
        "limits": {
            "maximum_duration_seconds": MAX_DURATION_SECONDS,
            "maximum_self_check_seconds": SELF_CHECK_TIMEOUT_SECONDS,
            "maximum_candidate_files": MAX_CANDIDATE_FILES,
            "maximum_candidate_bytes": MAX_CANDIDATE_BYTES,
            "maximum_windows_package_path": MAX_WINDOWS_PACKAGE_PATH,
            "maximum_package_relative_path_chars": MAX_PACKAGE_RELATIVE_PATH_CHARS,
            "maximum_provider_requests": 0,
            "maximum_network_requests": 0,
            "maximum_timeseries_downloads": 0,
            "maximum_live_clients": 0,
            "maximum_installations": 0,
            "maximum_shortcut_changes": 0,
            "maximum_production_cache_writes": 0,
            "maximum_retries": 0,
        },
        "paths": {
            "artifact_root": ARTIFACT_TEMPLATE,
            "candidate": f"{ARTIFACT_TEMPLATE}/FuturesLiveCockpit",
            "report_root": REPORT_TEMPLATE,
            "scratch_root": SCRATCH_TEMPLATE,
            "terminal": f"{REPORT_TEMPLATE}/terminal.json",
        },
        "preservation": {
            "existing_repository_package": {
                "path": "FuturesLiveCockpit",
                "executable_sha256": sha256_file(
                    root / "FuturesLiveCockpit/FuturesLiveCockpit.exe"
                ),
                "classification": "LAST_KNOWN_GOOD_DO_NOT_MUTATE",
            },
            "current_installation": "NO_ACCESS_NO_MUTATION",
            "shortcut_metadata": "NO_ACCESS_NO_MUTATION",
            "production_cache": "NO_ACCESS_NO_MUTATION",
            "provider": "UNREACHABLE",
            "partial_candidate": "PRESERVE_CREATE_ONLY_NO_PROMOTION",
            "scratch": "DISPOSABLE_ONLY_AFTER_VERIFIED_SUCCESS",
        },
        "success_condition": "CANDIDATE_VERIFIED",
    }
    body["plan_id"] = sha256_json(body)
    return body


def validate_plan(root: Path, plan: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(plan)
    claimed = value.pop("plan_id", None)
    if value.get("schema_version") != PLAN_SCHEMA or value.get("operation") != OPERATION:
        raise PackageCandidateError("unsupported package-candidate plan")
    if claimed != sha256_json(value):
        raise PackageCandidateError("package-candidate plan identity mismatch")
    expected = build_plan(root)
    if dict(plan) != expected:
        raise PackageCandidateError("package-candidate plan bindings drifted")
    return dict(plan)


def prepare_confirmation(
    root: Path,
    *,
    plan_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    plan = build_plan(root)
    selected_plan_root = root / (plan_root or PLAN_ROOT)
    plan_path = selected_plan_root / f"{plan['plan_id']}.json"
    _write_create_only(plan_path, plan)
    confirmation = {
        "schema_version": CONFIRMATION_SCHEMA,
        "status": "CONFIRMATION_REQUIRED",
        "operation": OPERATION,
        "summary": "Build and verify one isolated cockpit package candidate without provider access, installation, shortcut changes, or push.",
        "limits": plan["limits"],
        "outputs": plan["paths"],
        "preservation": plan["preservation"],
    }
    return plan_path, confirmation


def _safe_extract(archive: Path, destination: Path) -> None:
    with zipfile.ZipFile(archive) as bundle:
        for member in bundle.infolist():
            relative = Path(member.filename)
            mode = member.external_attr >> 16
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or (mode & 0o170000) == 0o120000
            ):
                raise PackageCandidateError("source archive contains an unsafe entry")
        bundle.extractall(destination)


def _inventory(candidate: Path) -> tuple[list[dict[str, Any]], int]:
    files: list[dict[str, Any]] = []
    total_bytes = 0
    for path in sorted(item for item in candidate.rglob("*") if item.is_file()):
        relative = path.relative_to(candidate).as_posix()
        size = path.stat().st_size
        total_bytes += size
        files.append({"path": relative, "bytes": size, "sha256": sha256_file(path)})
    return files, total_bytes


def _contains_private_key(path: Path) -> bool:
    overlap = max(len(value) for value in PRIVATE_KEY_HEADERS) - 1
    previous = b""
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value = previous + chunk
            if any(header in value for header in PRIVATE_KEY_HEADERS):
                return True
            previous = value[-overlap:]
    return False


def _scoped_path(root: Path, template: str, plan_id: str) -> Path:
    rendered = template.replace("<PLAN_ID>", plan_id).replace(
        "<PLAN_PREFIX>", plan_id[:PLAN_PREFIX_LENGTH]
    )
    path = Path(rendered)
    return path if path.is_absolute() else root / path


def _validate_path_budget(
    *,
    scratch_root: Path,
    artifact_root: Path,
) -> dict[str, int]:
    scratch_candidate = scratch_root / "dist/FuturesLiveCockpit"
    artifact_candidate = artifact_root / "FuturesLiveCockpit"
    projected = {
        "scratch": (
            len(str(scratch_candidate.resolve(strict=False)))
            + 1
            + MAX_PACKAGE_RELATIVE_PATH_CHARS
        ),
        "artifact": (
            len(str(artifact_candidate.resolve(strict=False)))
            + 1
            + MAX_PACKAGE_RELATIVE_PATH_CHARS
        ),
    }
    if max(projected.values()) > MAX_WINDOWS_PACKAGE_PATH:
        raise PackageCandidateError("package path budget was exceeded")
    return projected


def _validate_candidate(candidate: Path) -> tuple[list[dict[str, Any]], int]:
    if candidate.is_symlink() or bool(
        getattr(candidate.stat(), "st_file_attributes", 0) & 0x400
    ):
        raise PackageCandidateError("candidate root is link-like")
    top_level = sorted(path.name for path in candidate.iterdir())
    if top_level != ["FuturesLiveCockpit.exe", "_internal"]:
        raise PackageCandidateError("candidate top-level topology is invalid")
    required = (
        candidate / "FuturesLiveCockpit.exe",
        candidate / "_internal",
        candidate / "_internal/FuturesLiveCockpit.spec",
        candidate / "_internal/futures_live_cockpit.py",
    )
    if not all(path.exists() for path in required):
        raise PackageCandidateError("candidate is missing required package entries")
    descendants = list(candidate.rglob("*"))
    if any(
        path.is_symlink()
        or bool(getattr(path.stat(), "st_file_attributes", 0) & 0x400)
        for path in descendants
    ):
        raise PackageCandidateError("candidate contains a link-like entry")
    forbidden = []
    for path in descendants:
        relative_parts = tuple(part.lower() for part in path.relative_to(candidate).parts)
        if (
            path.name.lower() in FORBIDDEN_PACKAGE_NAMES
            or any(part in FORBIDDEN_PACKAGE_PATH_PARTS for part in relative_parts)
            or relative_parts[:3] == ("_internal", "state", "live_cockpit")
        ):
            forbidden.append(path.relative_to(candidate).as_posix())
    if forbidden:
        raise PackageCandidateError("candidate contains a forbidden secret, binding, or evidence path")
    if any(_contains_private_key(path) for path in descendants if path.is_file()):
        raise PackageCandidateError("candidate contains plaintext private-key material")
    files, total_bytes = _inventory(candidate)
    if not files or len(files) > MAX_CANDIDATE_FILES:
        raise PackageCandidateError("candidate file limit was violated")
    if total_bytes <= 0 or total_bytes > MAX_CANDIDATE_BYTES:
        raise PackageCandidateError("candidate byte limit was violated")
    return files, total_bytes


def _finalize_smoke_plan(candidate: Path) -> tuple[dict[str, Any], Path]:
    executable = candidate / "FuturesLiveCockpit.exe"
    plan_path = candidate / "_internal/configs/live_cockpit_smoke_plan.json"
    if sha256_file(plan_path) != SMOKE_PLAN_PLACEHOLDER_SHA256:
        raise PackageCandidateError("packaged smoke-plan placeholder drifted")
    plan = build_live_smoke_plan(sha256_file(executable))
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")
    validate_live_smoke_plan(
        json.loads(plan_path.read_text(encoding="utf-8"))
    )
    return plan, plan_path


def _sanitized_environment(local_appdata: Path) -> dict[str, str]:
    environment = dict(os.environ)
    for name in (
        "DATABENTO_API_KEY",
        "PYTHONHOME",
        "PYTHONPATH",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ):
        environment.pop(name, None)
    environment["LOCALAPPDATA"] = str(local_appdata)
    environment["SETUPTOOLS_USE_DISTUTILS"] = "stdlib"
    environment["PIP_NO_INDEX"] = "1"
    environment["NO_PROXY"] = "*"
    return environment


def _terminate_tree(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    subprocess.run(
        ["taskkill", "/PID", str(process.pid), "/T", "/F"],
        check=False,
        capture_output=True,
        shell=False,
    )


def _run_process(
    argv: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    timeout: int,
) -> tuple[int, str, str]:
    process = subprocess.Popen(
        list(argv),
        cwd=str(cwd),
        env=dict(environment),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        _terminate_tree(process)
        process.communicate()
        raise PackageCandidateError("bounded process timed out") from exc
    return (
        int(process.returncode),
        __import__("hashlib").sha256(stdout).hexdigest(),
        __import__("hashlib").sha256(stderr).hexdigest(),
    )


def _terminal(
    *,
    plan_id: str,
    terminal_state: str,
    category: str | None,
    build_started: bool,
    candidate_published: bool,
    elapsed_seconds: float,
    details: Mapping[str, Any],
) -> dict[str, Any]:
    body = {
        "schema_version": TERMINAL_SCHEMA,
        "plan_id": plan_id,
        "terminal_state": terminal_state,
        "diagnostic_category": category,
        "build_started": build_started,
        "candidate_published": candidate_published,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "provider_requests": 0,
        "network_requests": 0,
        "timeseries_downloads": 0,
        "live_clients": 0,
        "installations": 0,
        "shortcut_changes": 0,
        "production_cache_writes": 0,
        "retries": 0,
        "details": dict(details),
    }
    body["terminal_id"] = sha256_json(body)
    return body


def run_candidate(
    root: Path,
    *,
    plan_path: Path,
) -> dict[str, Any]:
    plan = validate_plan(root, json.loads(plan_path.read_text(encoding="utf-8")))
    plan_id = str(plan["plan_id"])
    artifact_root = _scoped_path(root, ARTIFACT_TEMPLATE, plan_id)
    report_root = _scoped_path(root, REPORT_TEMPLATE, plan_id)
    scratch_root = _scoped_path(root, SCRATCH_TEMPLATE, plan_id)
    _validate_path_budget(
        scratch_root=scratch_root,
        artifact_root=artifact_root,
    )
    if any(path.exists() for path in (artifact_root, report_root, scratch_root)):
        raise PackageCandidateError("create-only package output already exists")

    report_root.mkdir(parents=True, exist_ok=False)
    scratch_root.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    build_started = False
    candidate_published = False
    terminal_state = "ERROR"
    category: str | None = "UNAVAILABLE"
    details: dict[str, Any] = {}
    try:
        _validate_dependency_lock(root)
        archive = scratch_root / "source.zip"
        source_root = scratch_root / "source"
        source_root.mkdir()
        archive_result = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "archive",
                "--format=zip",
                f"--output={archive}",
                str(plan["basis"]["head"]),
                *ARCHIVE_PATHS,
            ],
            check=False,
            capture_output=True,
            shell=False,
        )
        if archive_result.returncode:
            category = "SOURCE_ISOLATION"
            raise PackageCandidateError("exact source archive failed")
        _safe_extract(archive, source_root)
        for relative in RUNTIME_OVERLAYS:
            destination = source_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(root / relative, destination)
        spec = source_root / "FuturesLiveCockpit/_internal/FuturesLiveCockpit.spec"
        dist = scratch_root / "dist"
        work = scratch_root / "work"
        environment = _sanitized_environment(scratch_root / "self-check-state")
        build_started = True
        build_code, build_stdout_sha, build_stderr_sha = _run_process(
            [
                str(root / ".venv/Scripts/python.exe"),
                "-m",
                "PyInstaller",
                "--noconfirm",
                "--distpath",
                str(dist),
                "--workpath",
                str(work),
                str(spec),
            ],
            cwd=source_root,
            environment=environment,
            timeout=MAX_DURATION_SECONDS,
        )
        details.update(
            {
                "build_exit_code": build_code,
                "build_stdout_sha256": build_stdout_sha,
                "build_stderr_sha256": build_stderr_sha,
            }
        )
        if build_code:
            category = "BUILD_FAILED"
            raise PackageCandidateError("package build failed")
        staged_candidate = dist / "FuturesLiveCockpit"
        smoke_plan, smoke_plan_path = _finalize_smoke_plan(staged_candidate)
        files, total_bytes = _validate_candidate(staged_candidate)
        self_check_code, self_stdout_sha, self_stderr_sha = _run_process(
            [str(staged_candidate / "FuturesLiveCockpit.exe"), "--self-check"],
            cwd=staged_candidate,
            environment=environment,
            timeout=SELF_CHECK_TIMEOUT_SECONDS,
        )
        details.update(
            {
                "self_check_exit_code": self_check_code,
                "self_check_stdout_sha256": self_stdout_sha,
                "self_check_stderr_sha256": self_stderr_sha,
            }
        )
        if self_check_code:
            category = "SELF_CHECK_FAILED"
            raise PackageCandidateError("packaged self-check failed")
        artifact_root.mkdir(parents=True, exist_ok=False)
        candidate = artifact_root / "FuturesLiveCockpit"
        staged_candidate.replace(candidate)
        candidate_published = True
        inventory = {
            "schema_version": INVENTORY_SCHEMA,
            "plan_id": plan_id,
            "file_count": len(files),
            "total_bytes": total_bytes,
            "files": files,
        }
        inventory_path = report_root / "inventory.json"
        _write_create_only(inventory_path, inventory)
        smoke_plan_report_path = report_root / "live-smoke-plan.json"
        _write_create_only(smoke_plan_report_path, smoke_plan)
        candidate_receipt = {
            "schema_version": CANDIDATE_SCHEMA,
            "plan_id": plan_id,
            "candidate_path": candidate.relative_to(root).as_posix(),
            "executable_sha256": sha256_file(candidate / "FuturesLiveCockpit.exe"),
            "live_smoke_plan_id": smoke_plan["plan_id"],
            "live_smoke_plan_sha256": sha256_file(smoke_plan_report_path),
            "live_smoke_result_output_relative": RESULT_OUTPUT_RELATIVE,
            "inventory_sha256": sha256_file(inventory_path),
            "file_count": len(files),
            "total_bytes": total_bytes,
            "self_check": "PASS",
            "provider_connection_opened": False,
            "credential_source_read": False,
            "install_ready": False,
        }
        candidate_receipt["candidate_id"] = sha256_json(candidate_receipt)
        candidate_path = report_root / "candidate.json"
        _write_create_only(candidate_path, candidate_receipt)
        details.update(
            {
                "candidate_id": candidate_receipt["candidate_id"],
                "candidate_receipt_sha256": sha256_file(candidate_path),
                "executable_sha256": candidate_receipt["executable_sha256"],
                "live_smoke_plan_id": candidate_receipt["live_smoke_plan_id"],
                "live_smoke_plan_sha256": candidate_receipt[
                    "live_smoke_plan_sha256"
                ],
                "inventory_sha256": candidate_receipt["inventory_sha256"],
                "file_count": len(files),
                "total_bytes": total_bytes,
            }
        )
        existing_hash = sha256_file(
            root / "FuturesLiveCockpit/FuturesLiveCockpit.exe"
        )
        if (
            existing_hash
            != plan["preservation"]["existing_repository_package"][
                "executable_sha256"
            ]
        ):
            category = "PRESERVATION_FAILED"
            raise PackageCandidateError("existing package preservation failed")
        terminal_state = "CANDIDATE_VERIFIED"
        category = None
        shutil.rmtree(scratch_root)
    except PackageCandidateError as exc:
        if str(exc) == "bounded process timed out":
            category = "TIMEOUT"
    except Exception:
        category = "UNAVAILABLE"
    terminal = _terminal(
        plan_id=plan_id,
        terminal_state=terminal_state,
        category=category,
        build_started=build_started,
        candidate_published=candidate_published,
        elapsed_seconds=time.monotonic() - started,
        details=details,
    )
    _write_create_only(report_root / "terminal.json", terminal)
    return terminal


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="live-cockpit-package-candidate")
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate = subparsers.add_parser("generate")
    generate.add_argument("--plan-root", type=Path)
    run = subparsers.add_parser("run")
    run.add_argument("--plan", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = _repo_root()
    if args.command == "generate":
        plan_path, confirmation = prepare_confirmation(
            root,
            plan_root=args.plan_root,
        )
        print(
            json.dumps(
                {
                    "plan": str(plan_path),
                    "confirmation": confirmation,
                }
            )
        )
        return 0
    terminal = run_candidate(
        root,
        plan_path=args.plan,
    )
    print(json.dumps(terminal))
    return 0 if terminal["terminal_state"] == "CANDIDATE_VERIFIED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
