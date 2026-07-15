"""Mechanical, non-authorizing readiness assessment and receipt publication.

Readiness means that exact code, data-foundation, synthetic-test, isolation, and
legacy-census prerequisites are present.  It never authorizes provider access,
historical evaluation, candidate sealing, execution, or trading.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import platform
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from .boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from .canonical import assert_plain_file, canonical_bytes, sha256_file, sha256_json
from .errors import ContractError, IntegrityError
from .foundation.orchestrator import (
    FOUNDATION_SET_RELEASE_KIND,
    load_foundation_set,
)
from .historical_capability import (
    build_foundation_research_blueprint,
    verify_production_capability_closure,
)
from .legacy_trial_census import (
    INDETERMINATE_COUNT_STATE,
    LEGACY_CENSUS_FILENAME,
    LEGACY_CENSUS_SCHEMA_VERSION,
    UNRESOLVED_STATUS,
    validate_legacy_trial_census_payload,
)
from .release import AtomicPublisher, ReleaseManifest, VerifiedReleaseReceipt
from .trial import LegacyCensusReceipt


PROJECT = "futures_intraday_model_v2"
SCHEMA_VERSION = "1.0.0"
SYNTHETIC_TEST_RELEASE_KIND = "futures_synthetic_test_evidence"
ENGINE_REGISTRATION_RELEASE_KIND = "futures_synthetic_engine_registration"
ISOLATION_RELEASE_KIND = "futures_project_isolation_evidence"
REBUILD_COMPLETE_RELEASE_KIND = "futures_rebuild_complete"
HISTORICAL_READY_RELEASE_KIND = "futures_historical_research_ready"
_SHA_RE = re.compile(r"[0-9a-f]{40,64}")

ENGINE_SEED_MODULES = (
    "futures_rebuild.dbn_catalog",
    "futures_rebuild.foundation.selection",
    "futures_rebuild.historical_builder",
    "futures_rebuild.historical_capability",
    "futures_rebuild.historical_engine_contracts",
    "futures_rebuild.historical_evaluator",
    "futures_rebuild.historical_splitter",
    "futures_rebuild.producer_bridge",
    "futures_rebuild.readiness",
    "futures_rebuild.source_symbology",
)
ENGINE_CONFIG_PATHS = (
    "configs/dependency_lock_receipt.json",
    "configs/environment.lock.json",
    "configs/historical_capability.json",
    "configs/research_readiness_contract.json",
    "configs/synthetic_research_engine.json",
)
ENGINE_TEST_PATHS = (
    "tests/test_dbn_catalog.py",
    "tests/test_dependency_lock.py",
    "tests/test_foundation_dbn_causality.py",
    "tests/test_foundation_market_state.py",
    "tests/test_foundation_orchestrator.py",
    "tests/test_historical_capability.py",
    "tests/test_historical_engine.py",
    "tests/test_producer_bridge.py",
    "tests/test_project_isolation.py",
    "tests/test_readiness.py",
    "tests/test_research_contracts_splits.py",
    "tests/test_research_controls_sleeves_boundary.py",
    "tests/test_research_dsr_cscv_power.py",
    "tests/test_research_futures_economics_abstention.py",
    "tests/test_research_governance_contract.py",
    "tests/test_research_hac_bootstrap_rw.py",
    "tests/test_source_symbology.py",
    "tests/test_trial_bundle_inference.py",
)
SYNTHETIC_TEST_COMMAND = "python -m pytest -q " + " ".join(ENGINE_TEST_PATHS)
SYNTHETIC_TEST_COMMAND_SHA256 = hashlib.sha256(
    SYNTHETIC_TEST_COMMAND.encode("utf-8")
).hexdigest()
DEPENDENCY_LOCK_PATHS = (
    "configs/environment.lock.json",
    "configs/offline_vault_environment.lock.json",
    "configs/runtime_wheel_lock.json",
    "pyproject.toml",
    "requirements-runtime.lock",
    "requirements.lock",
    "requirements.sha256.lock",
)
REQUIRED_HARD_PAUSES = frozenset(
    {
        "candidate_sealing",
        "destructive_cutover",
        "external_push",
        "legacy_repository_write",
        "paid_databento_download",
        "real_history_hypothesis_or_wfa_execution",
        "trading",
    }
)
REQUIRED_ISOLATION_CHECKS = (
    "DISTINCT_REPOSITORY_ROOTS",
    "NO_CROSS_IMPORT",
    "NO_CROSS_WRITE",
    "NO_SHARED_MUTABLE_DATA_STATE_BUNDLES",
)
CLOSED_RESEARCH_LINES = (
    {
        "active": False,
        "line_id": "LEGACY_CURRENT_ALPHA_LINE",
        "may_be_rescued": False,
        "status": "CLOSED_NO_ALPHA_EVIDENCE",
    },
    {
        "active": False,
        "line_id": "OPENING_RANGE_ACCEPTANCE_CONTINUATION_30M_V1_ORAC",
        "may_be_rescued": False,
        "status": "CLOSED_FAILED_NO_RESCUE",
    },
    {
        "active": False,
        "line_id": "DISTRIBUTIONAL_30M_LEGACY_WORK",
        "may_be_rescued": False,
        "status": "CLOSED_NOT_MIGRATED_REQUIRES_NEW_PREDECLARED_PROGRAM",
    },
)


@dataclass(frozen=True)
class SyntheticTestAttestation:
    passed_test_count: int
    test_output_sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.passed_test_count) is not int
            or self.passed_test_count < len(ENGINE_TEST_PATHS)
            or re.fullmatch(r"[0-9a-f]{64}", self.test_output_sha256) is None
        ):
            raise ContractError("synthetic test attestation fields are invalid")

    def as_dict(self) -> dict[str, object]:
        return {
            "passed_test_count": self.passed_test_count,
            "pytest_exit_code": 0,
            "test_command": SYNTHETIC_TEST_COMMAND,
            "test_command_sha256": SYNTHETIC_TEST_COMMAND_SHA256,
            "test_output_sha256": self.test_output_sha256,
        }


@dataclass(frozen=True, order=True)
class ReadinessBlocker:
    state: str
    code: str

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "state": self.state}


@dataclass(frozen=True)
class ReadinessAssessment:
    blockers: tuple[ReadinessBlocker, ...]

    @property
    def publication_allowed(self) -> bool:
        return not self.blockers

    def as_dict(self) -> dict[str, object]:
        if self.blockers:
            return {
                "blockers": [item.as_dict() for item in self.blockers],
                "publication_allowed": False,
                "status": "BLOCKED",
            }
        return {
            "blockers": [],
            "publication_allowed": True,
            "status": "EXACT_NON_ALPHA_PREREQUISITES_VERIFIED",
        }


@dataclass(frozen=True)
class ReadinessPublication:
    rebuild_complete_receipt: VerifiedReleaseReceipt
    historical_research_ready_receipt: VerifiedReleaseReceipt

    def as_dict(self) -> dict[str, object]:
        return {
            "historical_research_ready_receipt": (
                self.historical_research_ready_receipt.as_dict()
            ),
            "rebuild_complete_receipt": self.rebuild_complete_receipt.as_dict(),
            "status": "MECHANICAL_STATES_PUBLISHED_NO_EXECUTION_AUTHORITY",
        }


def _read_json_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        assert_plain_file(path)
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} JSON is invalid") from exc
    if not isinstance(payload, dict):
        raise IntegrityError(f"{description} JSON must be an object")
    return payload


def _read_canonical(path: Path, *, description: str) -> dict[str, object]:
    payload = _read_json_object(path, description=description)
    if path.read_bytes() != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def _file_entries(root: Path, paths: Sequence[str]) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    if tuple(paths) != tuple(sorted(set(paths))):
        raise ContractError("closure paths must be unique and canonically sorted")
    for relative in paths:
        path = root / Path(relative)
        entries.append(
            {
                "path": relative,
                "sha256": sha256_file(path),
                "size": path.stat().st_size,
            }
        )
    return entries


def committed_git_closure(root: Path) -> dict[str, object]:
    """Return an exact clean-commit identity without refreshing the index."""

    environment = dict(os.environ)
    environment["GIT_OPTIONAL_LOCKS"] = "0"

    def run(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
        result = subprocess.run(
            ("git", "-C", str(root), *arguments),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            env=environment,
        )
        if check and result.returncode != 0:
            raise IntegrityError("readiness requires a valid committed Git repository")
        return result

    top = Path(run("rev-parse", "--show-toplevel").stdout.decode().strip()).resolve()
    if top != root.resolve(strict=True):
        raise IntegrityError("readiness Git root differs from the active repository")
    head = run("rev-parse", "--verify", "HEAD").stdout.decode().strip()
    tree = run("rev-parse", "HEAD^{tree}").stdout.decode().strip()
    branch_result = run("symbolic-ref", "--quiet", "--short", "HEAD", check=False)
    branch = branch_result.stdout.decode().strip()
    status = run("status", "--porcelain=v1", "--untracked-files=all").stdout
    unstaged = run("diff", "--quiet", check=False)
    staged = run("diff", "--cached", "--quiet", check=False)
    tracked_index = run("ls-files", "-s", "-z").stdout
    remotes = tuple(
        item
        for item in run("remote").stdout.decode().splitlines()
        if item
    )
    if (
        _SHA_RE.fullmatch(head) is None
        or _SHA_RE.fullmatch(tree) is None
        or branch_result.returncode != 0
        or not branch
        or status
        or unstaged.returncode != 0
        or staged.returncode != 0
        or not tracked_index
    ):
        raise IntegrityError("readiness requires one clean committed branch state")
    core = {
        "branch": branch,
        "git_optional_locks": False,
        "head_commit": head,
        "head_tree": tree,
        "remote_names": list(sorted(remotes)),
        "status_porcelain_sha256": hashlib.sha256(status).hexdigest(),
        "tracked_index_sha256": hashlib.sha256(tracked_index).hexdigest(),
    }
    return {**core, "git_closure_id": sha256_json(core)}


def _module_map(root: Path) -> dict[str, tuple[str, Path]]:
    source = root / "src" / "futures_rebuild"
    result: dict[str, tuple[str, Path]] = {}
    for path in sorted(source.rglob("*.py")):
        relative = path.relative_to(root).as_posix()
        module_parts = list(path.relative_to(root / "src").with_suffix("").parts)
        if module_parts[-1] == "__init__":
            module_parts.pop()
        module = ".".join(module_parts)
        result[module] = (relative, path)
    return result


def _local_imports(module: str, path: Path, modules: Mapping[str, object]) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    package = module if path.name == "__init__.py" else module.rpartition(".")[0]
    found: set[str] = set()
    for node in ast.walk(tree):
        candidates: list[str] = []
        if isinstance(node, ast.Import):
            candidates.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            try:
                base = (
                    importlib.util.resolve_name(
                        "." * node.level + (node.module or ""), package
                    )
                    if node.level
                    else (node.module or "")
                )
            except (ImportError, ValueError) as exc:
                raise IntegrityError("engine source has an invalid relative import") from exc
            if base:
                candidates.append(base)
                candidates.extend(f"{base}.{alias.name}" for alias in node.names)
        for candidate in candidates:
            if candidate in modules:
                found.add(candidate)
                parent = candidate
                while "." in parent:
                    parent = parent.rpartition(".")[0]
                    if parent in modules:
                        found.add(parent)
    return found


def engine_code_closure(root: Path) -> list[dict[str, object]]:
    modules = _module_map(root)
    missing = sorted(set(ENGINE_SEED_MODULES) - set(modules))
    if missing:
        raise IntegrityError(f"engine seed modules are absent: {missing}")
    pending = list(ENGINE_SEED_MODULES)
    selected: set[str] = set()
    while pending:
        module = pending.pop()
        if module in selected:
            continue
        selected.add(module)
        _, path = modules[module]
        pending.extend(sorted(_local_imports(module, path, modules) - selected))
    paths = tuple(sorted(modules[module][0] for module in selected))
    return _file_entries(root, paths)


def engine_config_closure(root: Path) -> list[dict[str, object]]:
    return _file_entries(root, ENGINE_CONFIG_PATHS)


def engine_test_closure(root: Path) -> list[dict[str, object]]:
    return _file_entries(root, ENGINE_TEST_PATHS)


def _verify_dependency_lock(root: Path) -> str:
    receipt = _read_json_object(
        root / "configs" / "dependency_lock_receipt.json",
        description="dependency lock receipt",
    )
    if set(receipt) != {"files", "receipt_id", "receipt_version", "runtime"}:
        raise IntegrityError("dependency lock receipt schema is invalid")
    core = {key: receipt[key] for key in receipt if key != "receipt_id"}
    if (
        receipt.get("receipt_version") != "1.1.0"
        or receipt.get("receipt_id") != sha256_json(core)
        or not isinstance(receipt.get("files"), list)
        or not isinstance(receipt.get("runtime"), dict)
    ):
        raise IntegrityError("dependency lock receipt identity is invalid")
    files = receipt["files"]
    if (
        files != sorted(files, key=lambda item: str(item.get("path")))
        or tuple(item.get("path") for item in files) != DEPENDENCY_LOCK_PATHS
    ):
        raise IntegrityError("dependency lock files are not canonically ordered")
    for raw in files:
        if not isinstance(raw, dict) or set(raw) != {"path", "sha256"}:
            raise IntegrityError("dependency lock file entry is invalid")
        path = raw.get("path")
        relative = Path(path) if type(path) is str else Path("..")
        if (
            type(path) is not str
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != path
            or re.fullmatch(r"[0-9a-f]{64}", str(raw.get("sha256"))) is None
            or sha256_file(root / relative) != raw.get("sha256")
        ):
            raise IntegrityError("dependency lock file hash differs from active bytes")
    runtime = receipt["runtime"]
    if (
        set(runtime) != {"implementation", "packages", "platform", "python"}
        or runtime.get("implementation") != platform.python_implementation()
        or runtime.get("platform") != sys.platform
        or runtime.get("python") != platform.python_version()
        or not isinstance(runtime.get("packages"), dict)
    ):
        raise IntegrityError("dependency lock runtime differs from the active interpreter")
    requirements = [
        line.strip()
        for line in (root / "requirements.lock").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    try:
        pinned = dict(line.split("==", 1) for line in requirements)
    except ValueError as exc:
        raise IntegrityError("runtime requirements are not exact pins") from exc
    packages = runtime["packages"]
    if packages != pinned or not packages:
        raise IntegrityError("dependency receipt package closure differs from exact pins")
    hashed_lines = [
        line.strip()
        for line in (root / "requirements.sha256.lock")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if (
        len(hashed_lines) != len(requirements)
        or [line.split(" --hash=", 1)[0] for line in hashed_lines] != requirements
        or any(
            re.fullmatch(r"[^\s=]+==[^\s=]+ --hash=sha256:[0-9a-f]{64}", line)
            is None
            for line in hashed_lines
        )
    ):
        raise IntegrityError("hashed dependency closure differs from exact pins")
    environment = _read_json_object(
        root / "configs" / "environment.lock.json",
        description="environment lock",
    )
    closure = environment.get("complete_binary_closure")
    if (
        not isinstance(closure, dict)
        or closure.get("package_count") != len(packages)
        or closure.get("requirements_path") != "requirements.sha256.lock"
        or closure.get("requirements_sha256")
        != sha256_file(root / "requirements.sha256.lock")
        or closure.get("install_policy") != "--require-hashes --only-binary=:all:"
    ):
        raise IntegrityError("environment lock does not bind the complete package closure")
    for package, version in packages.items():
        if type(package) is not str or type(version) is not str:
            raise IntegrityError("dependency receipt package entry is invalid")
        try:
            installed = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError as exc:
            raise IntegrityError("dependency lock package is absent from runtime") from exc
        if installed != version:
            raise IntegrityError("dependency lock package version differs from runtime")
    return str(receipt["receipt_id"])


def _load_synthetic_engine_config(root: Path) -> dict[str, object]:
    payload = _read_json_object(
        root / "configs" / "synthetic_research_engine.json",
        description="synthetic research engine config",
    )
    authority = payload.get("authority")
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("project") != PROJECT
        or payload.get("status") != "SYNTHETIC_MECHANICS_VALIDATED_ONLY"
        or not isinstance(authority, dict)
        or any(
            authority.get(name) is not False
            for name in (
                "alpha_evidence",
                "candidate_eligible",
                "historical_research_ready",
                "real_history_execution_authorized",
                "candidate_sealing_authorized",
            )
        )
        or not isinstance(payload.get("synthetic_controls"), list)
        or not payload["synthetic_controls"]
    ):
        raise IntegrityError("synthetic research engine config weakens its safety posture")
    return payload


def _publish_json_release(
    *,
    publisher: AtomicPublisher,
    purpose: str,
    filename: str,
    payload: Mapping[str, object],
    release_kind: str,
    source_release_ids: Sequence[str] = (),
    metadata: Mapping[str, object],
) -> VerifiedReleaseReceipt:
    stage = publisher.create_stage(purpose)
    (stage / filename).write_bytes(canonical_bytes(dict(payload)) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        release_kind=release_kind,
        schema_version=SCHEMA_VERSION,
        source_release_ids=source_release_ids,
        metadata=metadata,
    )
    release = publisher.publish(stage, manifest)
    return VerifiedReleaseReceipt.from_release(release, publisher.boundary)


def publish_synthetic_test_evidence(
    *,
    attestation: SyntheticTestAttestation,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    if publisher.boundary.repository_id != boundary.repository_id:
        raise IntegrityError("synthetic evidence publisher belongs to another repository")
    config = _load_synthetic_engine_config(boundary.active_root)
    code = engine_code_closure(boundary.active_root)
    configs = engine_config_closure(boundary.active_root)
    tests = engine_test_closure(boundary.active_root)
    dependency_receipt_id = _verify_dependency_lock(boundary.active_root)
    core = {
        "alpha_evidence": False,
        "candidate_eligible": False,
        "code_closure": code,
        "code_closure_sha256": sha256_json(code),
        "config_closure": configs,
        "config_closure_sha256": sha256_json(configs),
        "dependency_lock_receipt_id": dependency_receipt_id,
        "external_write_count": 0,
        "paid_provider_call_count": 0,
        "project": PROJECT,
        "real_history_model_fit_count": 0,
        "real_history_row_count": 0,
        "schema_version": SCHEMA_VERSION,
        "status": "PASS_SYNTHETIC_MECHANICS_ONLY",
        "synthetic_only": True,
        "test_attestation": attestation.as_dict(),
        "test_closure": tests,
        "test_closure_sha256": sha256_json(tests),
        "verified_controls": list(config["synthetic_controls"]),
        "verified_isolation_checks": list(REQUIRED_ISOLATION_CHECKS),
    }
    payload = {**core, "synthetic_test_evidence_id": sha256_json(core)}
    receipt = _publish_json_release(
        publisher=publisher,
        purpose="synthetic_test_evidence",
        filename="synthetic_test_evidence.json",
        payload=payload,
        release_kind=SYNTHETIC_TEST_RELEASE_KIND,
        metadata={
            "code_closure_sha256": payload["code_closure_sha256"],
            "synthetic_test_evidence_id": payload["synthetic_test_evidence_id"],
            "test_closure_sha256": payload["test_closure_sha256"],
        },
    )
    load_synthetic_test_evidence(receipt, boundary=boundary)
    return receipt


def load_synthetic_test_evidence(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        manifest.release_kind != SYNTHETIC_TEST_RELEASE_KIND
        or manifest.schema_version != SCHEMA_VERSION
        or {entry.path for entry in manifest.files} != {"synthetic_test_evidence.json"}
        or set(manifest.metadata)
        != {
            "code_closure_sha256",
            "synthetic_test_evidence_id",
            "test_closure_sha256",
        }
        or manifest.source_release_ids
    ):
        raise IntegrityError("synthetic test evidence release contract is invalid")
    payload = _read_canonical(
        boundary.active_root / receipt.relative_root / "synthetic_test_evidence.json",
        description="synthetic test evidence",
    )
    evidence_id = payload.pop("synthetic_test_evidence_id", None)
    code = engine_code_closure(boundary.active_root)
    configs = engine_config_closure(boundary.active_root)
    tests = engine_test_closure(boundary.active_root)
    config = _load_synthetic_engine_config(boundary.active_root)
    attestation = payload.get("test_attestation")
    if (
        evidence_id != sha256_json(payload)
        or evidence_id != manifest.metadata["synthetic_test_evidence_id"]
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("project") != PROJECT
        or payload.get("status") != "PASS_SYNTHETIC_MECHANICS_ONLY"
        or payload.get("synthetic_only") is not True
        or payload.get("alpha_evidence") is not False
        or payload.get("candidate_eligible") is not False
        or payload.get("real_history_row_count") != 0
        or payload.get("real_history_model_fit_count") != 0
        or payload.get("paid_provider_call_count") != 0
        or payload.get("external_write_count") != 0
        or payload.get("code_closure") != code
        or payload.get("code_closure_sha256") != sha256_json(code)
        or payload.get("config_closure") != configs
        or payload.get("config_closure_sha256") != sha256_json(configs)
        or payload.get("test_closure") != tests
        or payload.get("test_closure_sha256") != sha256_json(tests)
        or payload.get("dependency_lock_receipt_id")
        != _verify_dependency_lock(boundary.active_root)
        or payload.get("verified_controls") != config["synthetic_controls"]
        or payload.get("verified_isolation_checks")
        != list(REQUIRED_ISOLATION_CHECKS)
        or not isinstance(attestation, dict)
        or set(attestation)
        != {
            "passed_test_count",
            "pytest_exit_code",
            "test_command",
            "test_command_sha256",
            "test_output_sha256",
        }
        or attestation.get("pytest_exit_code") != 0
        or attestation.get("test_command") != SYNTHETIC_TEST_COMMAND
        or attestation.get("test_command_sha256")
        != SYNTHETIC_TEST_COMMAND_SHA256
        or type(attestation.get("passed_test_count")) is not int
        or attestation["passed_test_count"] < len(ENGINE_TEST_PATHS)
        or re.fullmatch(
            r"[0-9a-f]{64}", str(attestation.get("test_output_sha256"))
        )
        is None
        or manifest.metadata["code_closure_sha256"] != sha256_json(code)
        or manifest.metadata["test_closure_sha256"] != sha256_json(tests)
    ):
        raise IntegrityError("synthetic test evidence is stale, substituted, or unsafe")
    payload["synthetic_test_evidence_id"] = evidence_id
    return payload


def publish_engine_registration(
    *,
    synthetic_test_evidence_receipt: VerifiedReleaseReceipt,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    evidence = load_synthetic_test_evidence(
        synthetic_test_evidence_receipt, boundary=boundary
    )
    config = _load_synthetic_engine_config(boundary.active_root)
    capability = verify_production_capability_closure(boundary.active_root)
    core = {
        "alpha_evidence": False,
        "candidate_eligible": False,
        "code_closure_sha256": evidence["code_closure_sha256"],
        "config_closure_sha256": evidence["config_closure_sha256"],
        "dependency_lock_receipt_id": evidence["dependency_lock_receipt_id"],
        "engine_roles": config["research_roles"],
        "historical_execution_authorized": False,
        "historical_capability_closure": capability,
        "project": PROJECT,
        "schema_version": SCHEMA_VERSION,
        "status": "REGISTERED_PRODUCTION_SHAPED_EXECUTION_DISABLED",
        "synthetic_test_evidence_receipt": (
            synthetic_test_evidence_receipt.as_dict()
        ),
    }
    payload = {**core, "engine_registration_id": sha256_json(core)}
    receipt = _publish_json_release(
        publisher=publisher,
        purpose="engine_registration",
        filename="engine_registration.json",
        payload=payload,
        release_kind=ENGINE_REGISTRATION_RELEASE_KIND,
        source_release_ids=(synthetic_test_evidence_receipt.release_id,),
        metadata={
            "capability_closure_id": capability["capability_closure_id"],
            "engine_registration_id": payload["engine_registration_id"],
            "status": payload["status"],
        },
    )
    load_engine_registration(receipt, boundary=boundary)
    return receipt


def load_engine_registration(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        manifest.release_kind != ENGINE_REGISTRATION_RELEASE_KIND
        or manifest.schema_version != SCHEMA_VERSION
        or {entry.path for entry in manifest.files} != {"engine_registration.json"}
        or set(manifest.metadata)
        != {"capability_closure_id", "engine_registration_id", "status"}
    ):
        raise IntegrityError("engine registration release contract is invalid")
    payload = _read_canonical(
        boundary.active_root / receipt.relative_root / "engine_registration.json",
        description="engine registration",
    )
    registration_id = payload.pop("engine_registration_id", None)
    evidence_receipt = _receipt_from(payload.get("synthetic_test_evidence_receipt"))
    evidence = load_synthetic_test_evidence(evidence_receipt, boundary=boundary)
    config = _load_synthetic_engine_config(boundary.active_root)
    capability = verify_production_capability_closure(boundary.active_root)
    if (
        registration_id != sha256_json(payload)
        or registration_id != manifest.metadata["engine_registration_id"]
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("project") != PROJECT
        or payload.get("status")
        != "REGISTERED_PRODUCTION_SHAPED_EXECUTION_DISABLED"
        or payload.get("alpha_evidence") is not False
        or payload.get("candidate_eligible") is not False
        or payload.get("historical_execution_authorized") is not False
        or payload.get("historical_capability_closure") != capability
        or manifest.metadata.get("capability_closure_id")
        != capability["capability_closure_id"]
        or payload.get("engine_roles") != config["research_roles"]
        or payload.get("code_closure_sha256") != evidence["code_closure_sha256"]
        or payload.get("config_closure_sha256")
        != evidence["config_closure_sha256"]
        or payload.get("dependency_lock_receipt_id")
        != evidence["dependency_lock_receipt_id"]
        or manifest.source_release_ids != (evidence_receipt.release_id,)
    ):
        raise IntegrityError("engine registration is stale, substituted, or unsafe")
    payload["engine_registration_id"] = registration_id
    return payload


def _scan_no_cross_import(root: Path) -> str:
    records: list[dict[str, object]] = []
    violations: list[str] = []
    source_root = root / "src"
    for path in sorted(source_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        relative = path.relative_to(root).as_posix()
        records.append({"path": relative, "sha256": sha256_file(path)})
        for node in ast.walk(tree):
            modules: tuple[str, ...] = ()
            if isinstance(node, ast.Import):
                modules = tuple(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = (node.module,)
            if any(module.startswith("us_stocks_swing_model") for module in modules):
                violations.append(f"{relative}:{node.lineno}")
    if not records or violations:
        raise IntegrityError("project source has a stock-project import or is absent")
    return sha256_json(records)


def _boundary_fingerprints(boundary: RepoBoundary) -> dict[str, object]:
    peers = sorted(
        sha256_json(str(path.resolve(strict=False)).casefold())
        for path in (*boundary.legacy_roots, *boundary.foreign_roots)
    )
    if not peers or len(peers) != len(set(peers)):
        raise IntegrityError("project isolation peer roots are absent or duplicated")
    return {
        "active_repository_id": boundary.repository_id,
        "peer_root_fingerprints": peers,
    }


def publish_project_isolation_evidence(
    *,
    synthetic_test_evidence_receipt: VerifiedReleaseReceipt,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    evidence = load_synthetic_test_evidence(
        synthetic_test_evidence_receipt, boundary=boundary
    )
    if evidence["verified_isolation_checks"] != list(REQUIRED_ISOLATION_CHECKS):
        raise IntegrityError("synthetic evidence lacks exact isolation checks")
    core = {
        **_boundary_fingerprints(boundary),
        "checks": {name: "PASS" for name in REQUIRED_ISOLATION_CHECKS},
        "project": PROJECT,
        "schema_version": SCHEMA_VERSION,
        "source_scan_sha256": _scan_no_cross_import(boundary.active_root),
        "status": "PASS_PROJECT_ISOLATED",
        "synthetic_test_evidence_receipt": (
            synthetic_test_evidence_receipt.as_dict()
        ),
    }
    payload = {**core, "isolation_evidence_id": sha256_json(core)}
    receipt = _publish_json_release(
        publisher=publisher,
        purpose="project_isolation",
        filename="project_isolation.json",
        payload=payload,
        release_kind=ISOLATION_RELEASE_KIND,
        source_release_ids=(synthetic_test_evidence_receipt.release_id,),
        metadata={
            "isolation_evidence_id": payload["isolation_evidence_id"],
            "status": payload["status"],
        },
    )
    load_project_isolation_evidence(receipt, boundary=boundary)
    return receipt


def load_project_isolation_evidence(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        manifest.release_kind != ISOLATION_RELEASE_KIND
        or manifest.schema_version != SCHEMA_VERSION
        or {entry.path for entry in manifest.files} != {"project_isolation.json"}
        or set(manifest.metadata) != {"isolation_evidence_id", "status"}
    ):
        raise IntegrityError("project isolation release contract is invalid")
    payload = _read_canonical(
        boundary.active_root / receipt.relative_root / "project_isolation.json",
        description="project isolation evidence",
    )
    evidence_id = payload.pop("isolation_evidence_id", None)
    test_receipt = _receipt_from(payload.get("synthetic_test_evidence_receipt"))
    test_evidence = load_synthetic_test_evidence(test_receipt, boundary=boundary)
    expected_boundary = _boundary_fingerprints(boundary)
    if (
        evidence_id != sha256_json(payload)
        or evidence_id != manifest.metadata["isolation_evidence_id"]
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("project") != PROJECT
        or payload.get("status") != "PASS_PROJECT_ISOLATED"
        or payload.get("checks")
        != {name: "PASS" for name in REQUIRED_ISOLATION_CHECKS}
        or payload.get("active_repository_id")
        != expected_boundary["active_repository_id"]
        or payload.get("peer_root_fingerprints")
        != expected_boundary["peer_root_fingerprints"]
        or payload.get("source_scan_sha256")
        != _scan_no_cross_import(boundary.active_root)
        or test_evidence["verified_isolation_checks"]
        != list(REQUIRED_ISOLATION_CHECKS)
        or manifest.source_release_ids != (test_receipt.release_id,)
    ):
        raise IntegrityError("project isolation evidence is stale or substituted")
    payload["isolation_evidence_id"] = evidence_id
    return payload


def _receipt_from(payload: object) -> VerifiedReleaseReceipt:
    if not isinstance(payload, dict):
        raise IntegrityError("embedded release receipt is invalid")
    return VerifiedReleaseReceipt.from_dict(payload)


def _load_mechanical_legacy_census(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> tuple[LegacyCensusReceipt, dict[str, object]]:
    """Load only the snapshot-bound census that keeps history trust closed."""

    census = LegacyCensusReceipt.from_release(receipt, boundary)
    census.verify()
    manifest = receipt.verify(boundary)
    payload = validate_legacy_trial_census_payload(
        _read_canonical(
            boundary.active_root / receipt.relative_root / LEGACY_CENSUS_FILENAME,
            description="production-derived unresolved legacy census",
        )
    )
    if (
        manifest.schema_version != LEGACY_CENSUS_SCHEMA_VERSION
        or census.status != UNRESOLVED_STATUS
        or census.exact_count_state != INDETERMINATE_COUNT_STATE
        or census.preregistered_penalty_count != 0
        or census.trusted_gate is not False
        or census.source_snapshot_id != payload["source_snapshot_id"]
        or census.observed_attempt_floor != payload["observed_attempt_floor"]
        or census.census_sha256 != payload["census_sha256"]
        or census.rationale_sha256 != payload["rationale_sha256"]
        or census.source_evidence_sha256 != payload["source_evidence_sha256"]
        or census.unresolved_reference_count
        != len(payload["unresolved_references"])
    ):
        raise IntegrityError(
            "legacy census does not preserve the exact unresolved trust closure"
        )
    return census, payload


def _safety_contract(boundary: RepoBoundary) -> dict[str, object]:
    root = boundary.active_root
    authorization = _read_json_object(
        root / "configs" / "controlled_rebuild_authorization.json",
        description="controlled rebuild authorization",
    )
    authorization_id = authorization.pop("authorization_id", None)
    if (
        authorization_id != sha256_json(authorization)
        or authorization.get("project") != PROJECT
        or authorization.get("active_root") != str(boundary.active_root)
        or set(authorization.get("hard_pauses", [])) != REQUIRED_HARD_PAUSES
    ):
        raise IntegrityError("controlled rebuild authorization is invalid or weakened")
    authorization["authorization_id"] = authorization_id
    readiness = _read_json_object(
        root / "configs" / "research_readiness_contract.json",
        description="research readiness contract",
    )
    research_pauses = (
        readiness.get("authorization", {}).get(
            "hard_pauses_requiring_new_user_authorization"
        )
        if isinstance(readiness.get("authorization"), dict)
        else None
    )
    if (
        readiness.get("project") != PROJECT
        or readiness.get("contract_version") != "2.0.0-controlled-rebuild"
        or readiness.get("readiness", {}).get("readiness_is_execution_authority")
        is not False
        or set(research_pauses or ()) != REQUIRED_HARD_PAUSES
        or readiness.get("source_roles", {}).get("query_manifest_required")
        is not True
        or readiness.get("source_roles", {}).get("query_symbology_role")
        != "PROVENANCE_ONLY_NEVER_FEATURE"
        or readiness.get("source_roles", {}).get(
            "mixed_status_statistics_query_epochs_explicit"
        )
        is not True
    ):
        raise IntegrityError("research readiness contract weakens hard pauses")
    return {
        "authorization_id": authorization_id,
        "authority": {
            "candidate_sealing_authorized": False,
            "execution_authority_granted": False,
            "paid_provider_download_authorized": False,
            "real_history_execution_authorized": False,
            "trading_authorized": False,
        },
        "claims": {
            "alpha": False,
            "candidate": False,
            "live_readiness": False,
            "promotion": False,
        },
        "closed_research_lines": [dict(item) for item in CLOSED_RESEARCH_LINES],
        "future_research_rule": "NEW_SEPARATELY_PREDECLARED_PROGRAM_ONLY",
        "hard_pauses": sorted(REQUIRED_HARD_PAUSES),
        "readiness_meaning": "MECHANICAL_PREREQUISITES_ONLY",
        "research_readiness_contract_sha256": sha256_file(
            root / "configs" / "research_readiness_contract.json"
        ),
    }


def assess_readiness(
    *,
    boundary: RepoBoundary,
    foundation_set_receipt: VerifiedReleaseReceipt | None,
    engine_registration_receipt: VerifiedReleaseReceipt | None,
    isolation_evidence_receipt: VerifiedReleaseReceipt | None,
    legacy_census_release_receipt: VerifiedReleaseReceipt | None,
) -> ReadinessAssessment:
    blockers: list[ReadinessBlocker] = []
    both_states = ("REBUILD_COMPLETE", "HISTORICAL_RESEARCH_READY")
    if foundation_set_receipt is None:
        blockers.extend(
            ReadinessBlocker(state, "MISSING_FOUNDATION_SET") for state in both_states
        )
    else:
        foundation = load_foundation_set(foundation_set_receipt, boundary=boundary)
        build_foundation_research_blueprint(
            foundation_set_receipt, boundary=boundary
        )
        run_contract = foundation.get("run_contract")
        if (
            foundation_set_receipt.release_kind != FOUNDATION_SET_RELEASE_KIND
            or not isinstance(run_contract, dict)
            or run_contract.get("repository_id") != boundary.repository_id
        ):
            raise IntegrityError("foundation set belongs to a different repository")
    engine: dict[str, object] | None = None
    if engine_registration_receipt is None:
        blockers.append(
            ReadinessBlocker(
                "HISTORICAL_RESEARCH_READY", "MISSING_SYNTHETIC_ENGINE_REGISTRATION"
            )
        )
    else:
        engine = load_engine_registration(
            engine_registration_receipt, boundary=boundary
        )
        if engine.get("historical_capability_closure") != (
            verify_production_capability_closure(boundary.active_root)
        ):
            raise IntegrityError("engine lacks the exact production capability closure")
    isolation: dict[str, object] | None = None
    if isolation_evidence_receipt is None:
        blockers.extend(
            ReadinessBlocker(state, "MISSING_PROJECT_ISOLATION_EVIDENCE")
            for state in both_states
        )
    else:
        isolation = load_project_isolation_evidence(
            isolation_evidence_receipt, boundary=boundary
        )
    if legacy_census_release_receipt is None:
        blockers.append(
            ReadinessBlocker(
                "HISTORICAL_RESEARCH_READY",
                "MISSING_LEGACY_TRIAL_CENSUS",
            )
        )
    else:
        try:
            _load_mechanical_legacy_census(
                legacy_census_release_receipt, boundary=boundary
            )
        except (ContractError, IntegrityError):
            blockers.append(
                ReadinessBlocker(
                    "HISTORICAL_RESEARCH_READY",
                    "INVALID_OR_TAMPERED_LEGACY_TRIAL_CENSUS",
                )
            )
    if engine is not None and isolation is not None:
        engine_test = _receipt_from(engine["synthetic_test_evidence_receipt"])
        isolation_test = _receipt_from(isolation["synthetic_test_evidence_receipt"])
        if engine_test != isolation_test:
            raise IntegrityError("engine and isolation evidence use different test runs")
    try:
        committed_git_closure(boundary.active_root)
    except IntegrityError:
        blockers.extend(
            ReadinessBlocker(state, "MISSING_CLEAN_COMMITTED_GIT_CLOSURE")
            for state in both_states
        )
    _safety_contract(boundary)
    return ReadinessAssessment(tuple(sorted(set(blockers))))


def _common_state_fields(
    safety: Mapping[str, object], git_closure: Mapping[str, object]
) -> dict[str, object]:
    return {
        "alpha_claim": False,
        "candidate_claim": False,
        "execution_authority_granted": False,
        "git_closure": dict(git_closure),
        "live_trading_ready": False,
        "project": PROJECT,
        "real_history_execution_authorized": False,
        "readiness_is_execution_authority": False,
        "safety_contract": dict(safety),
        "schema_version": SCHEMA_VERSION,
    }


def publish_readiness_states(
    *,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
    foundation_set_receipt: VerifiedReleaseReceipt | None,
    engine_registration_receipt: VerifiedReleaseReceipt | None,
    isolation_evidence_receipt: VerifiedReleaseReceipt | None,
    legacy_census_release_receipt: VerifiedReleaseReceipt | None,
) -> ReadinessAssessment | ReadinessPublication:
    assessment = assess_readiness(
        boundary=boundary,
        foundation_set_receipt=foundation_set_receipt,
        engine_registration_receipt=engine_registration_receipt,
        isolation_evidence_receipt=isolation_evidence_receipt,
        legacy_census_release_receipt=legacy_census_release_receipt,
    )
    if not assessment.publication_allowed:
        return assessment
    if publisher.boundary.repository_id != boundary.repository_id:
        raise IntegrityError("readiness publisher belongs to another repository")
    assert foundation_set_receipt is not None
    assert engine_registration_receipt is not None
    assert isolation_evidence_receipt is not None
    assert legacy_census_release_receipt is not None
    engine = load_engine_registration(engine_registration_receipt, boundary=boundary)
    blueprint = build_foundation_research_blueprint(
        foundation_set_receipt, boundary=boundary
    )
    test_receipt = _receipt_from(engine["synthetic_test_evidence_receipt"])
    safety = _safety_contract(boundary)
    git_closure = committed_git_closure(boundary.active_root)

    rebuild_core = {
        **_common_state_fields(safety, git_closure),
        "foundation_set_receipt": foundation_set_receipt.as_dict(),
        "foundation_research_blueprint_id": blueprint.blueprint_id,
        "query_manifest_id": blueprint.query_manifest_id,
        "isolation_evidence_receipt": isolation_evidence_receipt.as_dict(),
        "state": "REBUILD_COMPLETE",
        "status": "PASS_MECHANICAL_REBUILD_COMPLETE",
        "synthetic_test_evidence_receipt": test_receipt.as_dict(),
    }
    rebuild_payload = {
        **rebuild_core,
        "readiness_receipt_id": sha256_json(rebuild_core),
    }
    rebuild_receipt = _publish_json_release(
        publisher=publisher,
        purpose="rebuild_complete",
        filename="rebuild_complete.json",
        payload=rebuild_payload,
        release_kind=REBUILD_COMPLETE_RELEASE_KIND,
        source_release_ids=(
            foundation_set_receipt.release_id,
            isolation_evidence_receipt.release_id,
            test_receipt.release_id,
        ),
        metadata={
            "readiness_receipt_id": rebuild_payload["readiness_receipt_id"],
            "state": "REBUILD_COMPLETE",
        },
    )
    load_rebuild_complete(rebuild_receipt, boundary=boundary)

    census, census_payload = _load_mechanical_legacy_census(
        legacy_census_release_receipt, boundary=boundary
    )
    capability = engine["historical_capability_closure"]
    if not isinstance(capability, dict):
        raise IntegrityError("engine historical capability closure is invalid")
    historical_core = {
        **_common_state_fields(safety, git_closure),
        "engine_registration_receipt": engine_registration_receipt.as_dict(),
        "foundation_set_receipt": foundation_set_receipt.as_dict(),
        "foundation_research_blueprint_id": blueprint.blueprint_id,
        "query_manifest_id": blueprint.query_manifest_id,
        "historical_capability_closure": dict(capability),
        "historical_capability_closure_id": capability["capability_closure_id"],
        "isolation_evidence_receipt": isolation_evidence_receipt.as_dict(),
        "legacy_census": census_payload,
        "legacy_census_receipt_id": census.receipt_id,
        "legacy_census_release_receipt": legacy_census_release_receipt.as_dict(),
        "mechanical_readiness_scope": (
            "NO_ALPHA_CLAIM_NO_REAL_HISTORY_AUTHORITY_NO_TRUST_GATE"
        ),
        "real_history_trust_gate": {
            "execution_authorized": False,
            "status": "CLOSED_INVALID_TRIAL_CENSUS_UNRESOLVED",
            "trusted": False,
        },
        "rebuild_complete_receipt": rebuild_receipt.as_dict(),
        "state": "HISTORICAL_RESEARCH_READY",
        "status": "PASS_MECHANICAL_HISTORICAL_RESEARCH_READY",
        "synthetic_test_evidence_receipt": test_receipt.as_dict(),
    }
    historical_payload = {
        **historical_core,
        "readiness_receipt_id": sha256_json(historical_core),
    }
    historical_receipt = _publish_json_release(
        publisher=publisher,
        purpose="historical_research_ready",
        filename="historical_research_ready.json",
        payload=historical_payload,
        release_kind=HISTORICAL_READY_RELEASE_KIND,
        source_release_ids=(
            rebuild_receipt.release_id,
            engine_registration_receipt.release_id,
            foundation_set_receipt.release_id,
            isolation_evidence_receipt.release_id,
            legacy_census_release_receipt.release_id,
            test_receipt.release_id,
        ),
        metadata={
            "readiness_receipt_id": historical_payload["readiness_receipt_id"],
            "state": "HISTORICAL_RESEARCH_READY",
        },
    )
    load_historical_research_ready(historical_receipt, boundary=boundary)
    return ReadinessPublication(rebuild_receipt, historical_receipt)


def _load_state_payload(
    receipt: VerifiedReleaseReceipt,
    *,
    boundary: RepoBoundary,
    release_kind: str,
    filename: str,
    state: str,
) -> tuple[dict[str, object], object]:
    manifest = receipt.verify(boundary)
    if (
        manifest.release_kind != release_kind
        or manifest.schema_version != SCHEMA_VERSION
        or {entry.path for entry in manifest.files} != {filename}
        or set(manifest.metadata) != {"readiness_receipt_id", "state"}
        or manifest.metadata["state"] != state
    ):
        raise IntegrityError("readiness state release contract is invalid")
    payload = _read_canonical(
        boundary.active_root / receipt.relative_root / filename,
        description=state,
    )
    receipt_id = payload.pop("readiness_receipt_id", None)
    if (
        receipt_id != sha256_json(payload)
        or receipt_id != manifest.metadata["readiness_receipt_id"]
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("project") != PROJECT
        or payload.get("state") != state
        or payload.get("execution_authority_granted") is not False
        or payload.get("readiness_is_execution_authority") is not False
        or payload.get("alpha_claim") is not False
        or payload.get("candidate_claim") is not False
        or payload.get("live_trading_ready") is not False
        or payload.get("real_history_execution_authorized") is not False
        or payload.get("safety_contract") != _safety_contract(boundary)
        or payload.get("git_closure") != committed_git_closure(
            boundary.active_root
        )
    ):
        raise IntegrityError("readiness state identity or safety posture is invalid")
    payload["readiness_receipt_id"] = receipt_id
    return payload, manifest


def load_rebuild_complete(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    payload, manifest = _load_state_payload(
        receipt,
        boundary=boundary,
        release_kind=REBUILD_COMPLETE_RELEASE_KIND,
        filename="rebuild_complete.json",
        state="REBUILD_COMPLETE",
    )
    if set(payload) != {
        "alpha_claim",
        "candidate_claim",
        "execution_authority_granted",
        "foundation_set_receipt",
        "foundation_research_blueprint_id",
        "git_closure",
        "isolation_evidence_receipt",
        "live_trading_ready",
        "project",
        "query_manifest_id",
        "real_history_execution_authorized",
        "readiness_is_execution_authority",
        "readiness_receipt_id",
        "safety_contract",
        "schema_version",
        "state",
        "status",
        "synthetic_test_evidence_receipt",
    } or payload.get("status") != "PASS_MECHANICAL_REBUILD_COMPLETE":
        raise IntegrityError("REBUILD_COMPLETE payload schema/status is invalid")
    foundation = _receipt_from(payload["foundation_set_receipt"])
    isolation = _receipt_from(payload["isolation_evidence_receipt"])
    test_evidence = _receipt_from(payload["synthetic_test_evidence_receipt"])
    blueprint = build_foundation_research_blueprint(foundation, boundary=boundary)
    isolation_payload = load_project_isolation_evidence(isolation, boundary=boundary)
    load_synthetic_test_evidence(test_evidence, boundary=boundary)
    if (
        _receipt_from(isolation_payload["synthetic_test_evidence_receipt"])
        != test_evidence
        or payload["foundation_research_blueprint_id"] != blueprint.blueprint_id
        or payload["query_manifest_id"] != blueprint.query_manifest_id
        or manifest.source_release_ids
        != tuple(
            sorted(
                (
                    foundation.release_id,
                    isolation.release_id,
                    test_evidence.release_id,
                )
            )
        )
    ):
        raise IntegrityError("REBUILD_COMPLETE exact dependency closure is invalid")
    return payload


def load_historical_research_ready(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    payload, manifest = _load_state_payload(
        receipt,
        boundary=boundary,
        release_kind=HISTORICAL_READY_RELEASE_KIND,
        filename="historical_research_ready.json",
        state="HISTORICAL_RESEARCH_READY",
    )
    if set(payload) != {
        "alpha_claim",
        "candidate_claim",
        "engine_registration_receipt",
        "execution_authority_granted",
        "foundation_set_receipt",
        "foundation_research_blueprint_id",
        "git_closure",
        "historical_capability_closure",
        "historical_capability_closure_id",
        "isolation_evidence_receipt",
        "legacy_census",
        "legacy_census_receipt_id",
        "legacy_census_release_receipt",
        "live_trading_ready",
        "mechanical_readiness_scope",
        "project",
        "query_manifest_id",
        "real_history_execution_authorized",
        "real_history_trust_gate",
        "readiness_is_execution_authority",
        "readiness_receipt_id",
        "rebuild_complete_receipt",
        "safety_contract",
        "schema_version",
        "state",
        "status",
        "synthetic_test_evidence_receipt",
    } or payload.get("status") != "PASS_MECHANICAL_HISTORICAL_RESEARCH_READY":
        raise IntegrityError("HISTORICAL_RESEARCH_READY payload schema/status is invalid")
    rebuild = _receipt_from(payload["rebuild_complete_receipt"])
    engine = _receipt_from(payload["engine_registration_receipt"])
    foundation = _receipt_from(payload["foundation_set_receipt"])
    isolation = _receipt_from(payload["isolation_evidence_receipt"])
    census_release = _receipt_from(payload["legacy_census_release_receipt"])
    test_evidence = _receipt_from(payload["synthetic_test_evidence_receipt"])
    rebuild_payload = load_rebuild_complete(rebuild, boundary=boundary)
    engine_payload = load_engine_registration(engine, boundary=boundary)
    blueprint = build_foundation_research_blueprint(foundation, boundary=boundary)
    isolation_payload = load_project_isolation_evidence(
        isolation, boundary=boundary
    )
    census, census_payload = _load_mechanical_legacy_census(
        census_release, boundary=boundary
    )
    load_synthetic_test_evidence(test_evidence, boundary=boundary)
    capability = engine_payload["historical_capability_closure"]
    if (
        payload["historical_capability_closure"] != capability
        or payload["historical_capability_closure_id"]
        != capability["capability_closure_id"]
        or payload["legacy_census"] != census_payload
        or payload["legacy_census_receipt_id"] != census.receipt_id
        or payload["foundation_research_blueprint_id"] != blueprint.blueprint_id
        or payload["query_manifest_id"] != blueprint.query_manifest_id
        or payload["mechanical_readiness_scope"]
        != "NO_ALPHA_CLAIM_NO_REAL_HISTORY_AUTHORITY_NO_TRUST_GATE"
        or payload["real_history_trust_gate"]
        != {
            "execution_authorized": False,
            "status": "CLOSED_INVALID_TRIAL_CENSUS_UNRESOLVED",
            "trusted": False,
        }
        or _receipt_from(rebuild_payload["foundation_set_receipt"]) != foundation
        or rebuild_payload["foundation_research_blueprint_id"]
        != blueprint.blueprint_id
        or rebuild_payload["query_manifest_id"] != blueprint.query_manifest_id
        or _receipt_from(rebuild_payload["isolation_evidence_receipt"])
        != isolation
        or _receipt_from(rebuild_payload["synthetic_test_evidence_receipt"])
        != test_evidence
        or _receipt_from(engine_payload["synthetic_test_evidence_receipt"])
        != test_evidence
        or _receipt_from(isolation_payload["synthetic_test_evidence_receipt"])
        != test_evidence
        or manifest.source_release_ids
        != tuple(
            sorted(
                (
                    rebuild.release_id,
                    engine.release_id,
                    foundation.release_id,
                    isolation.release_id,
                    census_release.release_id,
                    test_evidence.release_id,
                )
            )
        )
    ):
        raise IntegrityError(
            "HISTORICAL_RESEARCH_READY exact dependency closure is invalid"
        )
    return payload


def _cli_boundary(repository_root: Path, source_contract: Path) -> RepoBoundary:
    contract = _read_json_object(source_contract, description="source contract")
    active = contract.get("active_repository")
    legacy = contract.get("legacy_repository")
    if type(active) is not str or not active or type(legacy) is not str or not legacy:
        raise ContractError("source contract repository boundaries are invalid")
    boundary = RepoBoundary(
        Path(active),
        legacy_roots=(Path(legacy),),
        foreign_roots=(
            Path.home() / "Desktop" / "US_stocks_swing_model",
            Path.home() / "Desktop" / "US_stocks_swing_model_v2",
        ),
    )
    boundary.assert_active_root(repository_root)
    boundary.assert_active_path(
        source_contract, purpose="source contract", subtree="configs"
    )
    return boundary


def _cli_release(
    path: Path | None, *, boundary: RepoBoundary, description: str
) -> VerifiedReleaseReceipt | None:
    if path is None:
        return None
    release_root = boundary.assert_active_path(
        path,
        purpose=description,
        subtree="data/vault/releases",
    )
    relative = release_root.relative_to(
        (boundary.active_root / "data" / "vault" / "releases").resolve(
            strict=False
        )
    )
    if len(relative.parts) != 1:
        raise ContractError(f"{description} must name one immutable release root")
    return VerifiedReleaseReceipt.from_release(release_root, boundary)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Assess or publish mechanical futures readiness without alpha, "
            "real-history, trust-gate, provider, candidate, or trading authority"
        )
    )
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--source-contract", type=Path, required=True)
    parser.add_argument("--foundation-set-release", type=Path)
    parser.add_argument("--engine-registration-release", type=Path)
    parser.add_argument("--isolation-evidence-release", type=Path)
    parser.add_argument("--legacy-census-release", type=Path)
    parser.add_argument(
        "--publish",
        action="store_true",
        help="publish only the two non-authorizing mechanical readiness states",
    )
    args = parser.parse_args(argv)

    boundary = _cli_boundary(args.repository_root, args.source_contract)
    foundation = _cli_release(
        args.foundation_set_release,
        boundary=boundary,
        description="foundation-set release",
    )
    engine = _cli_release(
        args.engine_registration_release,
        boundary=boundary,
        description="synthetic engine-registration release",
    )
    isolation = _cli_release(
        args.isolation_evidence_release,
        boundary=boundary,
        description="project-isolation release",
    )
    census = _cli_release(
        args.legacy_census_release,
        boundary=boundary,
        description="production-derived unresolved legacy-census release",
    )
    assessment = assess_readiness(
        boundary=boundary,
        foundation_set_receipt=foundation,
        engine_registration_receipt=engine,
        isolation_evidence_receipt=isolation,
        legacy_census_release_receipt=census,
    )
    if not args.publish or not assessment.publication_allowed:
        print(canonical_bytes(assessment.as_dict()).decode("utf-8"))
        return 0 if assessment.publication_allowed else 2

    assert foundation is not None
    assert engine is not None
    assert isolation is not None
    assert census is not None
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={
            "foundation_set_receipt_id": foundation.receipt_id,
            "engine_registration_receipt_id": engine.receipt_id,
            "isolation_evidence_receipt_id": isolation.receipt_id,
            "legacy_census_receipt_id": census.receipt_id,
            "readiness_scope": "MECHANICAL_ONLY_NO_ALPHA_OR_REAL_HISTORY_AUTHORITY",
        },
    )
    publisher = AtomicPublisher(
        boundary.active_root
        / "data"
        / "vault"
        / ".staging"
        / "releases"
        / "readiness-cli",
        boundary.active_root / "data" / "vault" / "releases",
        boundary.active_root / "state" / "locks" / "readiness-cli.lock",
        boundary=boundary,
        operation_receipt=operation,
    )
    result = publish_readiness_states(
        boundary=boundary,
        publisher=publisher,
        foundation_set_receipt=foundation,
        engine_registration_receipt=engine,
        isolation_evidence_receipt=isolation,
        legacy_census_release_receipt=census,
    )
    print(canonical_bytes(result.as_dict()).decode("utf-8"))
    return 0 if isinstance(result, ReadinessPublication) else 2


if __name__ == "__main__":
    raise SystemExit(main())
