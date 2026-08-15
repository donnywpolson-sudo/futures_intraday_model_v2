"""Create-only, hash-bound packaging for the observation-only live cockpit."""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.metadata
import json
import os
import platform
import re
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
CANDIDATE_SCHEMA = "live_cockpit_package_candidate_receipt/1.2.0"
TERMINAL_SCHEMA = "live_cockpit_package_candidate_terminal/1.1.0"
PRIVATE_KEY_SCANNER_VERSION = "structured-private-key-scanner/1.0.0"
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
    "src/futures_rebuild/live_cockpit/execution/manual_assistant.py",
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
    "src/futures_rebuild/live_cockpit/offline_network.py",
    "src/futures_rebuild/live_cockpit/predictions.py",
    "src/futures_rebuild/live_cockpit/protocol.py",
    "src/futures_rebuild/live_cockpit/smoke.py",
    "src/futures_rebuild/live_cockpit/single_instance.py",
    "FuturesLiveCockpit/_internal/FuturesLiveCockpit.spec",
    "configs/prop_firm_execution_connections.json",
    "configs/mff_execution_capability_evidence.json",
)
PACKAGE_INPUTS = (
    "src/futures_rebuild/live_cockpit/package_candidate.py",
    *RUNTIME_OVERLAYS,
    "src/futures_rebuild/canonical.py",
    "src/futures_rebuild/errors.py",
    "FuturesLiveCockpit/_internal/futures_live_cockpit.py",
    "configs/alpha_tiered.yaml",
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
    "configs/prop_firm_execution_connections.json",
    "configs/prop_firm_profiles.json",
    "configs/prop_firm_execution_costs.json",
    "configs/prop_firm_execution_instruments.json",
    "configs/prop_firm_strategy_risk_policies.json",
    "configs/prop_firm_payout_policies.json",
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
PRIVATE_KEY_LABELS = (
    "PRIVATE KEY",
    "RSA PRIVATE KEY",
    "EC PRIVATE KEY",
    "DSA PRIVATE KEY",
    "OPENSSH PRIVATE KEY",
    "ENCRYPTED PRIVATE KEY",
    "PGP PRIVATE KEY BLOCK",
)
PRIVATE_KEY_ENCODINGS = (
    ("ASCII", "ascii"),
    ("UTF-16LE", "utf-16le"),
    ("UTF-16BE", "utf-16be"),
)
TEXT_LIKE_SUFFIXES = frozenset(
    {
        ".bat",
        ".cfg",
        ".cmd",
        ".conf",
        ".css",
        ".csv",
        ".env",
        ".html",
        ".ini",
        ".js",
        ".json",
        ".log",
        ".md",
        ".ps1",
        ".py",
        ".spec",
        ".toml",
        ".tsv",
        ".txt",
        ".xml",
        ".yaml",
        ".yml",
    }
)
NATIVE_SUFFIXES = frozenset({".dll", ".exe", ".pyd"})
MAX_PRIVATE_KEY_BLOCK_BYTES = 1024 * 1024
MIN_PRIVATE_KEY_PAYLOAD_CHARS = 32


class PackageCandidateError(RuntimeError):
    """The package-candidate contract is absent, stale, or violated."""


class PrivateKeyScanError(PackageCandidateError):
    """A package contains a fail-closed private-key scanner finding."""

    def __init__(self, result: Mapping[str, Any]) -> None:
        super().__init__("candidate contains rejected private-key material")
        self.result = dict(result)


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


def _validated_dependency_receipt(root: Path) -> dict[str, Any]:
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
    return dict(receipt)


def _validate_dependency_lock(root: Path) -> str:
    return str(_validated_dependency_receipt(root)["receipt_id"])


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


def _all_offsets(value: bytes, marker: bytes) -> list[int]:
    offsets: list[int] = []
    start = 0
    while True:
        offset = value.find(marker, start)
        if offset < 0:
            return offsets
        offsets.append(offset)
        start = offset + len(marker)


def _context_sha256(value: bytes, offset: int, marker_size: int) -> str:
    start = max(0, offset - 64)
    stop = min(len(value), offset + marker_size + 64)
    return hashlib.sha256(value[start:stop]).hexdigest()


def _is_text_like(path: Path, value: bytes) -> bool:
    if path.suffix.lower() in TEXT_LIKE_SUFFIXES:
        return True
    sample = value[: 64 * 1024]
    if b"\x00" in sample:
        return False
    try:
        text = sample.decode("utf-8")
    except UnicodeDecodeError:
        return False
    if not text:
        return True
    printable = sum(
        character.isprintable() or character.isspace() for character in text
    )
    return printable / len(text) >= 0.90


def _unescape_text_with_offsets(value: str) -> tuple[str, list[int]]:
    decoded: list[str] = []
    offsets: list[int] = []
    index = 0
    simple = {"n": "\n", "r": "\r", "t": "\t", "\\": "\\", '"': '"', "/": "/"}
    while index < len(value):
        if value[index] != "\\" or index + 1 >= len(value):
            decoded.append(value[index])
            offsets.append(index)
            index += 1
            continue
        kind = value[index + 1]
        length = 0
        digits = ""
        if kind == "u" and index + 6 <= len(value):
            length = 6
            digits = value[index + 2 : index + 6]
        elif kind == "x" and index + 4 <= len(value):
            length = 4
            digits = value[index + 2 : index + 4]
        if length and re.fullmatch(r"[0-9a-fA-F]+", digits):
            decoded.append(chr(int(digits, 16)))
            offsets.append(index)
            index += length
            continue
        if kind in simple:
            decoded.append(simple[kind])
            offsets.append(index)
            index += 2
            continue
        decoded.append(value[index])
        offsets.append(index)
        index += 1
    return "".join(decoded), offsets


def _escaped_text_findings(
    *, value: bytes, relative: str
) -> list[dict[str, Any]]:
    if b"\\" not in value:
        return []
    findings: list[dict[str, Any]] = []
    views: list[tuple[str, str, int]] = [("ESCAPED_UTF-8", "utf-8-sig", 3)]
    if value.startswith(b"\xff\xfe"):
        views.append(("ESCAPED_UTF-16LE", "utf-16le", 2))
    elif value.startswith(b"\xfe\xff"):
        views.append(("ESCAPED_UTF-16BE", "utf-16be", 2))
    elif b"\x00" in value[: 64 * 1024]:
        views.extend(
            (
                ("ESCAPED_UTF-16LE", "utf-16le", 0),
                ("ESCAPED_UTF-16BE", "utf-16be", 0),
            )
        )
    for encoding_label, encoding, possible_bom_size in views:
        try:
            text = value.decode(encoding)
        except UnicodeDecodeError:
            continue
        has_bom = (
            value.startswith(b"\xef\xbb\xbf")
            if possible_bom_size == 3
            else value.startswith((b"\xff\xfe", b"\xfe\xff"))
            if possible_bom_size == 2
            else False
        )
        bom_size = possible_bom_size if has_bom else 0
        if bom_size == 2 and text.startswith("\ufeff"):
            text = text[1:]
        decoded, source_offsets = _unescape_text_with_offsets(text)
        for label in PRIVATE_KEY_LABELS:
            marker = f"-----BEGIN {label}-----"
            start = 0
            while True:
                decoded_offset = decoded.find(marker, start)
                if decoded_offset < 0:
                    break
                source_character_offset = source_offsets[decoded_offset]
                byte_offset = bom_size + len(
                    text[:source_character_offset].encode(
                        "utf-8" if encoding == "utf-8-sig" else encoding
                    )
                )
                raw_marker = marker.encode(
                    "ascii" if encoding == "utf-8-sig" else encoding
                )
                if not value[byte_offset:].startswith(raw_marker):
                    findings.append(
                        {
                            "file_relative_path": relative,
                            "classification": "TEXT_PRIVATE_KEY_MARKER",
                            "label": label,
                            "encoding": encoding_label,
                            "offset": byte_offset,
                            "context_sha256": _context_sha256(value, byte_offset, 1),
                            "verification_reason": (
                                "escaped private-key marker in text-like content"
                            ),
                            "dependency": None,
                        }
                    )
                start = decoded_offset + len(marker)
    return findings


def _decode_record_hash(value: str) -> str:
    padding = "=" * (-len(value) % 4)
    try:
        return base64.b64decode(value + padding, altchars=b"-_", validate=True).hex()
    except (ValueError, base64.binascii.Error) as exc:
        raise PackageCandidateError(
            "native dependency RECORD hash is malformed"
        ) from exc


def _normalized_distribution_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _valid_pe(path: Path) -> dict[str, Any]:
    try:
        import pefile

        image = pefile.PE(str(path), fast_load=True)
    except Exception as exc:
        raise PackageCandidateError(
            "marker-bearing native dependency is not a valid PE image"
        ) from exc
    try:
        if image.NT_HEADERS.Signature != 0x4550:
            raise PackageCandidateError(
                "marker-bearing native dependency is not a valid PE image"
            )
        return {
            "machine": f"0x{image.FILE_HEADER.Machine:04x}",
            "optional_header_magic": f"0x{image.OPTIONAL_HEADER.Magic:04x}",
            "sections": int(image.FILE_HEADER.NumberOfSections),
        }
    finally:
        image.close()


def _verified_native_dependency(
    *, root: Path, candidate: Path, path: Path
) -> dict[str, Any]:
    relative_parts = path.relative_to(candidate).parts
    if len(relative_parts) < 3 or relative_parts[0].lower() != "_internal":
        raise PackageCandidateError(
            "marker-bearing binary has unknown package ownership"
        )
    if path.suffix.lower() not in NATIVE_SUFFIXES:
        raise PackageCandidateError(
            "marker-bearing binary is not an expected native dependency"
        )
    distribution_relative = "/".join(relative_parts[1:])
    receipt = _validated_dependency_receipt(root)
    locked_packages = receipt["runtime"]["packages"]
    readable_lock_lines = (root / "requirements.lock").read_text(
        encoding="utf-8"
    ).splitlines()
    readable_lock = {
        _normalized_distribution_name(line.split("==", 1)[0]): line.split("==", 1)[1]
        for line in readable_lock_lines
        if line.strip() and not line.lstrip().startswith("#") and "==" in line
    }
    hashed_lock: dict[str, dict[str, str]] = {}
    target_artifact: str | None = None
    for line in (root / "requirements.sha256.lock").read_text(
        encoding="utf-8"
    ).splitlines():
        if line.startswith(("# target-wheel: ", "# target-sdist: ")):
            target_artifact = line.split(": ", 1)[1]
            continue
        match = re.fullmatch(
            r"([^\s=]+)==([^\s=]+) --hash=sha256:([0-9a-f]{64})",
            line.strip(),
        )
        if match is not None and target_artifact is not None:
            hashed_lock[_normalized_distribution_name(match.group(1))] = {
                "version": match.group(2),
                "artifact": target_artifact,
                "sha256": match.group(3),
            }
            target_artifact = None
    owners: list[tuple[str, str, Any, Any]] = []
    for package, version in locked_packages.items():
        normalized = _normalized_distribution_name(str(package))
        if readable_lock.get(normalized) != str(version):
            continue
        try:
            distribution = importlib.metadata.distribution(str(package))
        except importlib.metadata.PackageNotFoundError:
            continue
        if distribution.version != str(version):
            continue
        records = [
            item
            for item in (distribution.files or [])
            if str(item).replace("\\", "/").lower()
            == distribution_relative.lower()
        ]
        if len(records) == 1:
            owners.append((str(package), str(version), distribution, records[0]))
    if len(owners) != 1:
        raise PackageCandidateError(
            "marker-bearing binary has unknown package ownership"
        )
    package, version, distribution, record = owners[0]
    locked_artifact = hashed_lock.get(_normalized_distribution_name(package))
    if (
        locked_artifact is None
        or locked_artifact["version"] != version
        or not locked_artifact["artifact"].lower().endswith(".whl")
    ):
        raise PackageCandidateError(
            "native dependency is absent from the hashed wheel lock"
        )
    if record.hash is None or record.hash.mode != "sha256" or record.size is None:
        raise PackageCandidateError("native dependency RECORD provenance is incomplete")
    record_path = Path(str(record))
    if record_path.is_absolute() or ".." in record_path.parts:
        raise PackageCandidateError("native dependency RECORD path is unsafe")
    source = Path(distribution.locate_file(record)).resolve()
    if (
        not source.is_file()
        or source.is_symlink()
        or bool(getattr(source.stat(), "st_file_attributes", 0) & 0x400)
    ):
        raise PackageCandidateError("native dependency source file is unavailable")
    source_sha256 = sha256_file(source)
    packaged_sha256 = sha256_file(path)
    record_sha256 = _decode_record_hash(record.hash.value)
    if (
        source.stat().st_size != record.size
        or record_sha256 != source_sha256
        or path.stat().st_size != source.stat().st_size
        or packaged_sha256 != source_sha256
    ):
        raise PackageCandidateError("native dependency provenance hash mismatch")
    pe_metadata = _valid_pe(source)
    if _valid_pe(path) != pe_metadata:
        raise PackageCandidateError("packaged native dependency PE metadata mismatch")
    return {
        "distribution": package,
        "version": version,
        "record_path": str(record).replace("\\", "/"),
        "record_sha256": record_sha256,
        "record_size": int(record.size),
        "source_sha256": source_sha256,
        "packaged_sha256": packaged_sha256,
        "candidate_relative_path": path.relative_to(candidate).as_posix(),
        "dependency_lock_receipt_id": str(receipt["receipt_id"]),
        "locked_wheel_filename": locked_artifact["artifact"],
        "locked_wheel_sha256": locked_artifact["sha256"],
        "native_format": "PE",
        **pe_metadata,
    }


def _private_key_scan_result(findings: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rejected = [
        item
        for item in findings
        if item["classification"] != "VERIFIED_DEPENDENCY_PARSER_LITERAL"
    ]
    counts: dict[str, int] = {}
    for item in findings:
        classification = str(item["classification"])
        counts[classification] = counts.get(classification, 0) + 1
    return {
        "scanner_version": PRIVATE_KEY_SCANNER_VERSION,
        "result": (
            "REJECTED_PRIVATE_KEY_MATERIAL"
            if rejected
            else "VERIFIED_DEPENDENCY_PARSER_LITERALS_ONLY"
            if findings
            else "NO_PRIVATE_KEY_MATERIAL"
        ),
        "classification_counts": dict(sorted(counts.items())),
        "findings": [dict(item) for item in findings],
    }


def _scan_candidate_private_keys(
    candidate: Path, *, root: Path | None = None
) -> dict[str, Any]:
    selected_root = (root or _repo_root()).resolve()
    findings: list[dict[str, Any]] = []
    for path in sorted(item for item in candidate.rglob("*") if item.is_file()):
        value = path.read_bytes()
        relative = path.relative_to(candidate).as_posix()
        text_like = _is_text_like(path, value)
        path_findings: list[dict[str, Any]] = []
        for label in PRIVATE_KEY_LABELS:
            for encoding_label, encoding in PRIVATE_KEY_ENCODINGS:
                begin = f"-----BEGIN {label}-----".encode(encoding)
                end = f"-----END {label}-----".encode(encoding)
                end_offsets = _all_offsets(value, end)
                for offset in _all_offsets(value, begin):
                    matching = next(
                        (
                            end_offset
                            for end_offset in end_offsets
                            if offset
                            < end_offset
                            <= offset + MAX_PRIVATE_KEY_BLOCK_BYTES
                        ),
                        None,
                    )
                    actual = False
                    suspicious = False
                    if matching is not None:
                        payload_bytes = value[offset + len(begin) : matching]
                        unit = 2 if encoding_label.startswith("UTF-16") else 1
                        nul_terminated = payload_bytes.startswith(b"\x00" * unit)
                        try:
                            payload_text = payload_bytes.decode(encoding)
                        except UnicodeDecodeError:
                            payload_text = ""
                        compact = "".join(payload_text.split())
                        base64_like = bool(
                            len(compact) >= MIN_PRIVATE_KEY_PAYLOAD_CHARS
                            and re.fullmatch(r"[A-Za-z0-9+/]*={0,2}", compact)
                        )
                        decoded = b""
                        if base64_like:
                            try:
                                decoded = base64.b64decode(compact, validate=True)
                            except (ValueError, base64.binascii.Error):
                                decoded = b""
                        actual = bool(
                            decoded
                            and (
                                decoded.startswith(b"0")
                                or decoded.startswith(b"openssh-key-v1\x00")
                            )
                        )
                        boundary_layout = payload_text.startswith(("\r", "\n"))
                        suspicious = bool(
                            actual
                            or boundary_layout
                            or base64_like
                            or not nul_terminated
                        )
                    if actual:
                        classification = "ACTUAL_PRIVATE_KEY_MATERIAL"
                        reason = (
                            "complete private-key block with decodable key-shaped payload"
                        )
                    elif suspicious:
                        classification = "SUSPICIOUS_COMPLETE_PRIVATE_KEY_BLOCK"
                        reason = "complete or ambiguous private-key block"
                    elif text_like:
                        classification = "TEXT_PRIVATE_KEY_MARKER"
                        reason = "private-key marker in text-like content"
                    else:
                        classification = "UNVERIFIED_BINARY_PRIVATE_KEY_MARKER"
                        reason = (
                            "isolated private-key marker lacks verified dependency provenance"
                        )
                    path_findings.append(
                        {
                            "file_relative_path": relative,
                            "classification": classification,
                            "label": label,
                            "encoding": encoding_label,
                            "offset": offset,
                            "context_sha256": _context_sha256(
                                value, offset, len(begin)
                            ),
                            "verification_reason": reason,
                            "dependency": None,
                        }
                    )
        if text_like:
            path_findings.extend(
                _escaped_text_findings(value=value, relative=relative)
            )
        unverified = [
            item
            for item in path_findings
            if item["classification"] == "UNVERIFIED_BINARY_PRIVATE_KEY_MARKER"
        ]
        if unverified:
            provenance_failure: str | None = None
            try:
                provenance = _verified_native_dependency(
                    root=selected_root, candidate=candidate, path=path
                )
            except PackageCandidateError as exc:
                provenance = None
                provenance_failure = str(exc)
            if provenance is not None:
                for item in unverified:
                    item["classification"] = "VERIFIED_DEPENDENCY_PARSER_LITERAL"
                    item["verification_reason"] = (
                        "isolated parser literal in exact locked native dependency bytes"
                    )
                    item["dependency"] = dict(provenance)
            elif provenance_failure is not None:
                for item in unverified:
                    item["verification_reason"] = provenance_failure
        findings.extend(path_findings)
    result = _private_key_scan_result(findings)
    if result["result"] == "REJECTED_PRIVATE_KEY_MATERIAL":
        raise PrivateKeyScanError(result)
    return result


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


def _validate_candidate(
    candidate: Path, *, root: Path | None = None
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
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
    private_key_scan = _scan_candidate_private_keys(candidate, root=root)
    files, total_bytes = _inventory(candidate)
    if not files or len(files) > MAX_CANDIDATE_FILES:
        raise PackageCandidateError("candidate file limit was violated")
    if total_bytes <= 0 or total_bytes > MAX_CANDIDATE_BYTES:
        raise PackageCandidateError("candidate byte limit was violated")
    return files, total_bytes, private_key_scan


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
        files, total_bytes, private_key_scan = _validate_candidate(
            staged_candidate, root=root
        )
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
            "private_key_scan": private_key_scan,
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
                "private_key_scan": private_key_scan,
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
    except PrivateKeyScanError as exc:
        category = "PRIVATE_KEY_SCAN_REJECTED"
        details["private_key_scan"] = exc.result
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
