"""Read-only validation for the canonical repository-surface registry.

The registry classifies paths; it does not grant permission to mutate them or
to cross any provider, research, publication, installation, trading, or Git
boundary.  Validation deliberately uses metadata and small control documents
only.  It never opens market-data payloads or credential files.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from .errors import ContractError


SCHEMA_VERSION = "repository_surface/1.0.0"
REGISTRY_PATH = Path("configs/repository_surface.json")
SOURCE_OF_TRUTH_PATH = Path("SOURCE_OF_TRUTH.md")
SOURCE_OF_TRUTH_ROLE = "HUMAN_SOURCE_OF_TRUTH_VIEW"
SOURCE_OF_TRUTH_MAX_WORDS = 1_600
SOURCE_OF_TRUTH_MAX_BYTES = 16 * 1024
PIPELINE_FOLDER_MAP_PATH = Path("PIPELINE_FOLDER_MAP.md")
PIPELINE_FOLDER_MAP_ROLE = "GENERATED_PIPELINE_FOLDER_MAP_VIEW"
PIPELINE_FOLDER_MAP_MAX_WORDS = 2_100
PIPELINE_FOLDER_MAP_MAX_BYTES = 20 * 1024
PIPELINE_FOLDER_MAP_MAX_TABLE_ROWS = 50
ACTIVE_SOURCE_FILES_PATH = Path("ACTIVE_SOURCE_FILES.txt")
ACTIVE_SOURCE_FILES_ROLE = "GENERATED_ACTIVE_SOURCE_FILES_VIEW"
ACTIVE_SOURCE_FILES_MAX_BYTES = 256 * 1024
ACTIVE_SOURCE_CLASSIFICATIONS = (
    "CURRENT_OPERATIONAL",
    "CURRENT_SUPPORTING",
)
ACTIVE_SOURCE_HEADER = (
    "# ACTIVE_SOURCE_FILES",
    "# Deterministically generated from configs/repository_surface.json and the tracked repository file inventory.",
    "# Inclusion: CURRENT_OPERATIONAL and CURRENT_SUPPORTING.",
    "# Virtual view only; no file is moved or hidden, and this file grants no operational authority.",
)
EXPECTED_REGISTRY_ENTRY_COUNT = 189
DIRECT_AUTHORITY_REGISTRY_ENTRY_COUNT = 212
EXPECTED_UNRESOLVED_ENTRY_COUNT = 14
EXPECTED_PUBLIC_COMMAND_COUNT = 7

SOURCE_OF_TRUTH_SECTIONS = (
    "Purpose and authority",
    "Start here",
    "Active machine-readable pointers",
    "Public commands",
    "Major folder roles",
    "Historical and retired material",
    "Generated, local-only, and unresolved material",
    "Cleanup and deletion rules",
    "Supersession rules",
    "What this document does not authorize",
)
PIPELINE_FOLDER_MAP_SECTIONS = (
    "Purpose and authority",
    "Current authority and pointer surfaces",
    "Public commands",
    "Major repository topology",
    "Classification summary",
    "Historical and retired boundaries",
    "Generated, local-only, mixed, and unresolved material",
    "Safety and non-authority boundary",
)


def _expected_registry_entry_count(surface: Mapping[str, object]) -> int:
    return (
        DIRECT_AUTHORITY_REGISTRY_ENTRY_COUNT
        if surface.get("current_direct_authority_registry_id")
        else EXPECTED_REGISTRY_ENTRY_COUNT
    )


def _direct_documentation_section(
    surface: Mapping[str, object], key: str
) -> str | None:
    payload = surface.get("current_direct_authority_documentation")
    if payload is None:
        return None
    if not isinstance(payload, Mapping):
        raise RepositorySurfaceError("current direct-authority documentation is invalid")
    section = payload.get(key)
    if not isinstance(section, str) or not section.startswith("## ") or not section.endswith("\n"):
        raise RepositorySurfaceError("current direct-authority documentation section is invalid")
    return section
PIPELINE_FOLDER_MAP_AUTHORITY_ROLES = (
    ("Normal work", "NORMAL_WORKFLOW_AUTHORITY"),
    ("Durable safety policy", "DURABLE_SAFETY_POLICY_AUTHORITY"),
    ("Generated source-of-truth view", SOURCE_OF_TRUTH_ROLE),
    ("Generated pipeline-folder-map view", PIPELINE_FOLDER_MAP_ROLE),
    ("Generated active-source-files view", ACTIVE_SOURCE_FILES_ROLE),
    ("Public package and commands", "PUBLIC_PACKAGE_AND_COMMAND_AUTHORITY"),
    ("Standard Alpha pointer", "ACTIVE_STANDARD_ALPHA_IDENTITY"),
    ("Standard active data catalog", "ACTIVE_STANDARD_DATA_SELECTION"),
    ("Micro source-selection pointer", "ACTIVE_MICRO_SOURCE_SELECTION"),
    ("Micro source catalog", "ACTIVE_MICRO_DATA_SELECTION"),
    ("Current real-history boundary", "CURRENT_REAL_HISTORY_GATEWAY"),
    ("Synthetic-only public pipeline", "PUBLIC_SYNTHETIC_PIPELINE_TARGET"),
    ("Retired workflow registry", "RETIRED_WORKFLOW_REGISTRY"),
)
ACTIVE_POINTER_ROLES = (
    ("Standard Alpha pointer", "ACTIVE_STANDARD_ALPHA_IDENTITY"),
    ("Standard active data catalog", "ACTIVE_STANDARD_DATA_SELECTION"),
    ("Micro source-selection pointer", "ACTIVE_MICRO_SOURCE_SELECTION"),
    ("Micro source catalog", "ACTIVE_MICRO_DATA_SELECTION"),
)
MAJOR_FOLDER_PATHS = (
    "src",
    "configs",
    "data",
    "manifests",
    "reports",
    "state",
    "scripts",
    "tests",
    "docs",
    "FuturesLiveCockpit",
    "build",
    "dist",
    "tmp",
    "artifacts",
    "bundles",
)

MATCH_TYPES = frozenset({"EXACT", "PREFIX", "GLOB"})
TRACKED_EXPECTATIONS = frozenset(
    {
        "TRACKED",
        "IGNORED_LOCAL",
        "UNTRACKED_GENERATED",
        "MIXED",
        "OPTIONAL",
        "ABSENT_EXPECTED",
    }
)
PROTECTED_ROOTS = frozenset(
    {"configs", "data", "manifests", "reports", "state", "src", "scripts", "tests", "docs"}
)
PUBLIC_COMMAND_CLASSIFICATIONS = frozenset(
    {"CURRENT_OPERATIONAL", "CURRENT_SUPPORTING"}
)
FORBIDDEN_PUBLIC_COMMAND_CLASSIFICATIONS = frozenset(
    {
        "HISTORICAL_HASH_BOUND",
        "HISTORICAL_UNBOUND",
        "PREPARED_NOT_EXECUTED",
        "GENERATED_OUTPUT",
        "REGENERABLE_CACHE",
        "LOCAL_RUNTIME_STATE",
        "LOCAL_SECRET",
        "UNRESOLVED_MANUAL_REVIEW",
    }
)
REQUIRED_NON_AUTHORITY_KEYS = frozenset(
    {
        "grants_deletion_authority",
        "grants_move_or_rename_authority",
        "grants_provider_or_research_authority",
        "grants_publication_installation_activation_authority",
        "grants_trading_or_order_authority",
        "grants_git_stage_commit_push_authority",
        "grants_active_state_mutation_authority",
    }
)
REQUIRED_ENTRY_FIELDS = frozenset(
    {
        "path_or_pattern",
        "match_type",
        "classification",
        "authority_role",
        "current_replacement",
        "hash_bound",
        "tracked_expected",
        "local_only",
        "deletion_policy",
        "owner",
        "notes",
    }
)
REQUIRED_ROLE_PATHS = {
    "NORMAL_WORKFLOW_AUTHORITY": "CURRENT_WORKFLOW.md",
    "DURABLE_SAFETY_POLICY_AUTHORITY": "AGENTS.md",
    "PUBLIC_PACKAGE_AND_COMMAND_AUTHORITY": "pyproject.toml",
    "ACTIVE_STANDARD_ALPHA_IDENTITY": "configs/active_alpha_research_ladder.json",
    "ACTIVE_STANDARD_DATA_SELECTION": "data/active/catalog.json",
    "ACTIVE_MICRO_SOURCE_SELECTION": "configs/active_micro_alpha_research_ladder.json",
    "ACTIVE_MICRO_DATA_SELECTION": "data/active/catalogs/apex_micro.json",
    "MICRO_CONTRACT_UNIVERSE_POLICY": "configs/micro_contract_universe_v1.json",
    "CORE_DATABENTO_L0_DEPENDENCY_POLICY": "configs/core_databento_standard_l0_dependency_policy_v1.json",
    "DATA_SURFACE_SELECTION_POLICY": "configs/data_surface_registry_v1.json",
    "DATA_CAPABILITY_BASELINE_BINDING": "configs/data_capability_baseline_v1.json",
    "DATA_PHASE_CLOSURE_RECORD": "configs/data_phase_closed_v1.json",
}
GENERATED_OR_AMBIGUOUS_ROOTS = (
    "build",
    "dist",
    "tmp",
    "artifacts",
    "FuturesLiveCockpit",
)

_DRIVE_PATH = re.compile(r"^[A-Za-z]:")
_MACHINE_PATH = re.compile(
    r"(?i)(?:[A-Z]:[\\/]|/Users/|/home/|AppData[\\/]Local[\\/]Temp|"
    r"futures-v2-repository-audit-\d{8})"
)
_AFFIRMATIVE_AUTHORITY = re.compile(
    r"(?i)\b(?:authorizes?|grants?)\s+(?:automatic\s+)?"
    r"(?:deletion|delete|move|rename|provider|historical-row|publication|"
    r"installation|activation|trading|orders?|staging|commit|push)\b"
)
_HEX_256 = re.compile(r"^[0-9a-f]{64}$")
_ABSOLUTE_POSIX_PATH = re.compile(r"(?m)(?:^|[\s(])/(?!/)")
_UNC_PATH = re.compile(r"\\\\[^\\\s]+\\")
_DATE_OR_TIMESTAMP = re.compile(
    r"\b(?:19|20)\d{2}-\d{2}-\d{2}(?:[T ][0-2]\d:[0-5]\d(?::[0-5]\d)?)?\b"
)
_COMMIT_SHA = re.compile(r"\b[0-9a-f]{40}\b")
_CREDENTIAL_VALUE = re.compile(
    r"(?i)\b(?:api[_-]?key|password|secret|token)\s*[:=]\s*[^\s`]+"
)


class RepositorySurfaceError(ContractError):
    """The repository-surface registry or checkout violates its contract."""


def _expect_object(value: object, name: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise RepositorySurfaceError(f"{name} must be an object")
    return value


def _expect_string(value: object, name: str) -> str:
    if type(value) is not str or not value:
        raise RepositorySurfaceError(f"{name} must be a non-empty string")
    return value


def _expect_string_list(value: object, name: str) -> list[str]:
    if type(value) is not list or not value:
        raise RepositorySurfaceError(f"{name} must be a non-empty list")
    result: list[str] = []
    for index, item in enumerate(value):
        result.append(_expect_string(item, f"{name}[{index}]"))
    if len(result) != len(set(result)):
        raise RepositorySurfaceError(f"{name} contains duplicates")
    return result


def _validate_relative_pattern(pattern: object, match_type: str, name: str) -> str:
    value = _expect_string(pattern, name)
    if value != value.strip():
        raise RepositorySurfaceError(f"{name} is not normalized")
    if "\\" in value:
        raise RepositorySurfaceError(f"{name} must use POSIX separators")
    if value.startswith(("/", "//")) or _DRIVE_PATH.match(value):
        raise RepositorySurfaceError(f"{name} must be repository-relative")
    if "//" in value or value.endswith("/"):
        raise RepositorySurfaceError(f"{name} is not normalized")
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise RepositorySurfaceError(f"{name} escapes or traverses the repository")
    if match_type != "GLOB" and any(char in value for char in "*?["):
        raise RepositorySurfaceError(f"{name} uses glob syntax with {match_type}")
    return value


def _safe_control_path(root: Path, relative: str) -> Path:
    _validate_relative_pattern(relative, "EXACT", "control path")
    resolved_root = root.resolve(strict=True)
    candidate = root / PurePosixPath(relative)
    resolved = candidate.resolve(strict=False)
    if not resolved.is_relative_to(resolved_root):
        raise RepositorySurfaceError(f"control path escapes repository: {relative}")
    return candidate


def load_repository_surface(
    repository_root: Path | str,
    registry_path: Path | str | None = None,
) -> dict[str, Any]:
    """Load and structurally validate the registry without mutating the checkout."""

    root = Path(repository_root).resolve(strict=True)
    path = Path(registry_path) if registry_path is not None else root / REGISTRY_PATH
    if not path.is_absolute():
        path = root / path
    resolved = path.resolve(strict=False)
    if not resolved.is_relative_to(root):
        raise RepositorySurfaceError("registry path escapes repository")
    if path.is_symlink() or not path.is_file():
        raise RepositorySurfaceError("registry must be a regular in-repository file")
    try:
        raw = path.read_text(encoding="utf-8")
        payload = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RepositorySurfaceError("repository surface is not readable JSON") from exc
    if _MACHINE_PATH.search(raw):
        raise RepositorySurfaceError("registry contains a machine-specific path")
    surface = _expect_object(payload, "repository surface")
    validate_repository_surface(surface)
    return surface


def validate_repository_surface(
    surface: Mapping[str, object],
    *,
    repository_root: Path | str | None = None,
) -> None:
    """Validate schema, entries, authority roles, and optional checkout bindings."""

    if type(surface) is not dict:
        raise RepositorySurfaceError("repository surface must be an object")
    if surface.get("schema_version") != SCHEMA_VERSION:
        raise RepositorySurfaceError(f"schema_version must be {SCHEMA_VERSION}")
    _expect_string(surface.get("purpose"), "purpose")
    allowed_classifications = frozenset(
        _expect_string_list(
            surface.get("allowed_classifications"), "allowed_classifications"
        )
    )
    allowed_policies = frozenset(
        _expect_string_list(
            surface.get("allowed_deletion_policies"), "allowed_deletion_policies"
        )
    )
    if frozenset(_expect_string_list(surface.get("allowed_match_types"), "allowed_match_types")) != MATCH_TYPES:
        raise RepositorySurfaceError("allowed_match_types must declare EXACT, PREFIX, and GLOB")
    if frozenset(
        _expect_string_list(
            surface.get("allowed_tracked_expectations"),
            "allowed_tracked_expectations",
        )
    ) != TRACKED_EXPECTATIONS:
        raise RepositorySurfaceError("allowed_tracked_expectations is incomplete")
    if surface.get("matching_precedence") != [
        "EXACT",
        "LONGEST_PREFIX",
        "MOST_SPECIFIC_GLOB",
    ]:
        raise RepositorySurfaceError("matching_precedence is not deterministic")

    non_authority = _expect_object(surface.get("non_authority"), "non_authority")
    if set(non_authority) != REQUIRED_NON_AUTHORITY_KEYS or any(
        value is not False for value in non_authority.values()
    ):
        raise RepositorySurfaceError("non_authority must deny every controlled action")
    principles = _expect_string_list(surface.get("principles"), "principles")
    required_principle_fragments = (
        "ignored does not imply disposable",
        "tracked does not imply current",
        "directory presence does not grant research authority",
        "version suffix does not determine currentness",
        "fresh machine-local census and separate approval",
        "reference and hash closure",
    )
    lower_principles = "\n".join(principles).lower()
    for fragment in required_principle_fragments:
        if fragment not in lower_principles:
            raise RepositorySurfaceError(f"missing safety principle: {fragment}")

    authority_precedence = surface.get("authority_precedence")
    if type(authority_precedence) is not list or not authority_precedence:
        raise RepositorySurfaceError("authority_precedence must be a non-empty list")
    seen_decisions: set[str] = set()
    for index, raw in enumerate(authority_precedence):
        item = _expect_object(raw, f"authority_precedence[{index}]")
        if set(item) != {"decision", "path", "role", "precedence"}:
            raise RepositorySurfaceError("authority precedence fields are invalid")
        decision = _expect_string(item["decision"], "authority decision")
        _validate_relative_pattern(item["path"], "EXACT", "authority path")
        _expect_string(item["role"], "authority role")
        if type(item["precedence"]) is not int or item["precedence"] < 1:
            raise RepositorySurfaceError("authority precedence must be a positive integer")
        if decision in seen_decisions:
            raise RepositorySurfaceError(f"duplicate authority decision: {decision}")
        seen_decisions.add(decision)

    raw_entries = surface.get("entries")
    if type(raw_entries) is not list or not raw_entries:
        raise RepositorySurfaceError("entries must be a non-empty list")
    entries: list[dict[str, Any]] = []
    seen_patterns: set[tuple[str, str]] = set()
    seen_glob_regexes: set[str] = set()
    for index, raw in enumerate(raw_entries):
        entry = _expect_object(raw, f"entries[{index}]")
        missing = REQUIRED_ENTRY_FIELDS - set(entry)
        if missing:
            raise RepositorySurfaceError(
                f"entries[{index}] missing fields: {', '.join(sorted(missing))}"
            )
        match_type = _expect_string(entry["match_type"], "match_type")
        if match_type not in MATCH_TYPES:
            raise RepositorySurfaceError(f"unknown match_type: {match_type}")
        pattern = _validate_relative_pattern(
            entry["path_or_pattern"], match_type, f"entries[{index}].path_or_pattern"
        )
        key = (match_type, pattern)
        if key in seen_patterns:
            raise RepositorySurfaceError(f"duplicate {match_type} entry: {pattern}")
        seen_patterns.add(key)
        if match_type == "GLOB":
            translated = _glob_regex(pattern).pattern
            if translated in seen_glob_regexes:
                raise RepositorySurfaceError(f"indistinguishable GLOB entry: {pattern}")
            seen_glob_regexes.add(translated)
        classification = _expect_string(entry["classification"], "classification")
        if classification not in allowed_classifications:
            raise RepositorySurfaceError(f"undeclared classification: {classification}")
        policy = _expect_string(entry["deletion_policy"], "deletion_policy")
        if policy not in allowed_policies:
            raise RepositorySurfaceError(f"undeclared deletion policy: {policy}")
        tracked = _expect_string(entry["tracked_expected"], "tracked_expected")
        if tracked not in TRACKED_EXPECTATIONS:
            raise RepositorySurfaceError(f"unknown tracked expectation: {tracked}")
        if type(entry["hash_bound"]) not in {bool, type(None)}:
            raise RepositorySurfaceError("hash_bound must be true, false, or null")
        if type(entry["local_only"]) is not bool:
            raise RepositorySurfaceError("local_only must be boolean")
        _expect_string(entry["authority_role"], "authority_role")
        _expect_string(entry["owner"], "owner")
        _expect_string(entry["notes"], "notes")
        replacement = entry["current_replacement"]
        if replacement is not None:
            _validate_relative_pattern(replacement, "EXACT", "current_replacement")
        if (
            classification == "REGENERABLE_CACHE"
            and pattern.split("/", 1)[0] in PROTECTED_ROOTS
            and (
                "__pycache__" not in pattern.split("/")
                or match_type not in {"EXACT", "PREFIX"}
            )
        ):
            raise RepositorySurfaceError(
                f"protected root cannot be a broad regenerable cache: {pattern}"
            )
        if classification == "LOCAL_SECRET" and policy != "SECRET_CONTENT_NEVER_INSPECT_OR_REPORT":
            raise RepositorySurfaceError("secret entries require the secret-content policy")
        entries.append(entry)

    serialization_surface = dict(surface)
    direct_documentation = serialization_surface.pop(
        "current_direct_authority_documentation", None
    )
    if direct_documentation is not None:
        _direct_documentation_section(surface, "source_of_truth_section")
        _direct_documentation_section(surface, "pipeline_folder_map_section")
    serialized = json.dumps(serialization_surface, sort_keys=True)
    if _MACHINE_PATH.search(serialized):
        raise RepositorySurfaceError("registry contains a machine-specific path")
    if _AFFIRMATIVE_AUTHORITY.search(serialized):
        raise RepositorySurfaceError("registry text grants a controlled authority")

    _validate_authority_roles(entries, authority_precedence)
    _validate_source_of_truth_registry_entry(entries)
    _validate_pipeline_folder_map_registry_entry(entries)
    _validate_active_source_files_registry_entry(entries)
    _validate_generated_root_policies(entries)
    _validate_standard_and_micro_roles(entries)

    if repository_root is not None:
        root = Path(repository_root).resolve(strict=True)
        validate_tracked_root_coverage(surface, root)
        validate_public_command_surfaces(surface, root)
        _validate_pointer_metadata(surface, root)
        compare_source_of_truth_file(surface, root)
        compare_pipeline_folder_map_file(surface, root)
        compare_active_source_files_file(surface, root)


def _validate_authority_roles(
    entries: Sequence[Mapping[str, object]],
    authority_precedence: Sequence[Mapping[str, object]],
) -> None:
    by_role: dict[str, list[Mapping[str, object]]] = {}
    for entry in entries:
        by_role.setdefault(str(entry["authority_role"]), []).append(entry)
    required_role_paths = dict(REQUIRED_ROLE_PATHS)
    if any(
        entry.get("authority_role") == "DATA_SURFACE_SELECTION_POLICY"
        and entry.get("path_or_pattern") == "configs/data_surface_registry_v3.json"
        for entry in entries
    ):
        required_role_paths["DATA_SURFACE_SELECTION_POLICY"] = (
            "configs/data_surface_registry_v3.json"
        )
    if any(
        entry.get("authority_role") == "DATA_PHASE_CLOSURE_RECORD"
        and entry.get("path_or_pattern") == "configs/data_phase_closed_v3.json"
        for entry in entries
    ):
        required_role_paths["DATA_PHASE_CLOSURE_RECORD"] = (
            "configs/data_phase_closed_v3.json"
        )
    elif any(
        entry.get("authority_role") == "DATA_PHASE_CLOSURE_RECORD"
        and entry.get("path_or_pattern") == "configs/data_phase_closed_v2.json"
        for entry in entries
    ):
        required_role_paths["DATA_PHASE_CLOSURE_RECORD"] = (
            "configs/data_phase_closed_v2.json"
        )
    for role, expected_path in required_role_paths.items():
        matches = by_role.get(role, [])
        if len(matches) != 1 or matches[0]["path_or_pattern"] != expected_path:
            raise RepositorySurfaceError(
                f"{role} must be uniquely assigned to {expected_path}"
            )
        if matches[0]["match_type"] != "EXACT":
            raise RepositorySurfaceError(f"{role} must use an exact path")
    precedence_bindings = {
        (str(item["role"]), str(item["path"])) for item in authority_precedence
    }
    for role, path in required_role_paths.items():
        if (role, path) not in precedence_bindings:
            raise RepositorySurfaceError(
                f"authority_precedence omits {role} at {path}"
            )


def _validate_standard_and_micro_roles(entries: Sequence[Mapping[str, object]]) -> None:
    role_paths = {
        str(entry["authority_role"]): str(entry["path_or_pattern"])
        for entry in entries
        if str(entry["authority_role"]) in REQUIRED_ROLE_PATHS
    }
    if role_paths["ACTIVE_STANDARD_ALPHA_IDENTITY"] == role_paths["ACTIVE_MICRO_SOURCE_SELECTION"]:
        raise RepositorySurfaceError("standard and micro pointers must remain separate")
    if role_paths["ACTIVE_STANDARD_DATA_SELECTION"] == role_paths["ACTIVE_MICRO_DATA_SELECTION"]:
        raise RepositorySurfaceError("standard and micro catalogs must remain separate")


def _validate_source_of_truth_registry_entry(
    entries: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    matches = [
        entry
        for entry in entries
        if entry.get("path_or_pattern") == SOURCE_OF_TRUTH_PATH.as_posix()
    ]
    if len(matches) != 1:
        raise RepositorySurfaceError(
            "SOURCE_OF_TRUTH.md must have exactly one registry entry"
        )
    entry = matches[0]
    expected = {
        "match_type": "EXACT",
        "classification": "CURRENT_SUPPORTING",
        "authority_role": SOURCE_OF_TRUTH_ROLE,
        "tracked_expected": "TRACKED",
        "local_only": False,
        "hash_bound": False,
        "deletion_policy": "PRESERVE",
    }
    for field, value in expected.items():
        if entry.get(field) != value:
            raise RepositorySurfaceError(
                f"SOURCE_OF_TRUTH.md {field} must be {value!r}"
            )
    role_matches = [
        item for item in entries if item.get("authority_role") == SOURCE_OF_TRUTH_ROLE
    ]
    if role_matches != matches:
        raise RepositorySurfaceError(
            "human source-of-truth view role must be unique to SOURCE_OF_TRUTH.md"
        )
    notes = str(entry.get("notes", "")).lower()
    required = (
        "deterministic human navigation view",
        "configs/repository_surface.json",
        "pyproject.toml",
        "grants no",
    )
    if any(fragment not in notes for fragment in required):
        raise RepositorySurfaceError(
            "SOURCE_OF_TRUTH.md registry notes must preserve derivation and non-authority"
        )
    return entry


def _validate_pipeline_folder_map_registry_entry(
    entries: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    matches = [
        entry
        for entry in entries
        if entry.get("path_or_pattern") == PIPELINE_FOLDER_MAP_PATH.as_posix()
    ]
    if len(matches) != 1:
        raise RepositorySurfaceError(
            "PIPELINE_FOLDER_MAP.md must have exactly one registry entry"
        )
    entry = matches[0]
    expected = {
        "match_type": "EXACT",
        "classification": "CURRENT_SUPPORTING",
        "authority_role": PIPELINE_FOLDER_MAP_ROLE,
        "tracked_expected": "TRACKED",
        "local_only": False,
        "hash_bound": False,
        "deletion_policy": "PRESERVE",
    }
    for field, value in expected.items():
        if entry.get(field) != value:
            raise RepositorySurfaceError(
                f"PIPELINE_FOLDER_MAP.md {field} must be {value!r}"
            )
    role_matches = [
        item for item in entries if item.get("authority_role") == PIPELINE_FOLDER_MAP_ROLE
    ]
    if role_matches != matches:
        raise RepositorySurfaceError(
            "generated pipeline-folder-map role must be unique to PIPELINE_FOLDER_MAP.md"
        )
    notes = str(entry.get("notes", "")).lower()
    required = (
        "deterministically rendered",
        "configs/repository_surface.json",
        "pyproject.toml",
        "current topology",
        "not the normal-work authority",
        "canonical machine-readable registry",
        "historical evidence ledger",
        "docs/history/",
        "grants no",
    )
    if any(fragment not in notes for fragment in required):
        raise RepositorySurfaceError(
            "PIPELINE_FOLDER_MAP.md registry notes must preserve derivation and non-authority"
        )
    return entry


def _validate_active_source_files_registry_entry(
    entries: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    matches = [
        entry
        for entry in entries
        if entry.get("path_or_pattern") == ACTIVE_SOURCE_FILES_PATH.as_posix()
    ]
    if len(matches) != 1:
        raise RepositorySurfaceError(
            "ACTIVE_SOURCE_FILES.txt must have exactly one registry entry"
        )
    entry = matches[0]
    expected = {
        "match_type": "EXACT",
        "classification": "CURRENT_SUPPORTING",
        "authority_role": ACTIVE_SOURCE_FILES_ROLE,
        "tracked_expected": "TRACKED",
        "local_only": False,
        "hash_bound": False,
        "deletion_policy": "PRESERVE",
    }
    for field, value in expected.items():
        if entry.get(field) != value:
            raise RepositorySurfaceError(
                f"ACTIVE_SOURCE_FILES.txt {field} must be {value!r}"
            )
    role_matches = [
        item for item in entries if item.get("authority_role") == ACTIVE_SOURCE_FILES_ROLE
    ]
    if role_matches != matches:
        raise RepositorySurfaceError(
            "generated active-source-files role must be unique to ACTIVE_SOURCE_FILES.txt"
        )
    notes = str(entry.get("notes", "")).lower()
    required = (
        "deterministically rendered",
        "tracked path inventory",
        "configs/repository_surface.json",
        "current_operational",
        "current_supporting",
        "virtual active-source view",
        "does not physically move or hide",
        "not normal-work authority",
        "not safety-policy authority",
        "not the canonical registry",
        "grants no",
        "historical paths remain preserved in place",
        "sanitized no-git exports",
    )
    if any(fragment not in notes for fragment in required):
        raise RepositorySurfaceError(
            "ACTIVE_SOURCE_FILES.txt registry notes must preserve derivation, virtual-view scope, and non-authority"
        )
    return entry


def _validate_generated_root_policies(entries: Sequence[Mapping[str, object]]) -> None:
    for root in GENERATED_OR_AMBIGUOUS_ROOTS:
        entry = resolve_surface_entry(entries, root)
        if entry is None:
            raise RepositorySurfaceError(f"generated or ambiguous root is unclassified: {root}")
        if entry["deletion_policy"] == "DELETE_ONLY_AFTER_FRESH_CENSUS_AND_SEPARATE_APPROVAL":
            raise RepositorySurfaceError(
                f"ambiguous generated root has cleanup-candidate policy: {root}"
            )


def _glob_regex(pattern: str) -> re.Pattern[str]:
    pieces: list[str] = ["^"]
    index = 0
    while index < len(pattern):
        char = pattern[index]
        if char == "*":
            if index + 1 < len(pattern) and pattern[index + 1] == "*":
                pieces.append(".*")
                index += 2
            else:
                pieces.append("[^/]*")
                index += 1
        elif char == "?":
            pieces.append("[^/]")
            index += 1
        elif char == "[":
            end = pattern.find("]", index + 1)
            if end == -1:
                pieces.append(re.escape(char))
                index += 1
            else:
                content = pattern[index + 1 : end]
                if content.startswith("!"):
                    content = "^" + content[1:]
                pieces.append("[" + content + "]")
                index = end + 1
        else:
            pieces.append(re.escape(char))
            index += 1
    pieces.append("$")
    return re.compile("".join(pieces))


def _glob_specificity(pattern: str) -> tuple[int, int, int]:
    literal = re.sub(r"\[[^]]*\]|[*?]", "", pattern)
    return (len(literal), pattern.count("/") + 1, len(pattern))


def resolve_surface_entry(
    surface_or_entries: Mapping[str, object] | Sequence[Mapping[str, object]],
    repository_path: str | Path,
) -> Mapping[str, object] | None:
    """Resolve one relative path using EXACT, PREFIX, then GLOB precedence."""

    entries_object: object
    if isinstance(surface_or_entries, Mapping):
        entries_object = surface_or_entries.get("entries")
    else:
        entries_object = surface_or_entries
    if not isinstance(entries_object, Sequence):
        raise RepositorySurfaceError("entries are unavailable")
    path = Path(repository_path).as_posix() if isinstance(repository_path, Path) else repository_path
    path = _validate_relative_pattern(path, "EXACT", "repository path")
    entries = [entry for entry in entries_object if isinstance(entry, Mapping)]

    exact = [
        entry
        for entry in entries
        if entry.get("match_type") == "EXACT" and entry.get("path_or_pattern") == path
    ]
    if exact:
        if len(exact) != 1:
            raise RepositorySurfaceError(f"ambiguous exact match: {path}")
        return exact[0]

    prefixes = [
        entry
        for entry in entries
        if entry.get("match_type") == "PREFIX"
        and (
            path == entry.get("path_or_pattern")
            or path.startswith(str(entry.get("path_or_pattern")) + "/")
        )
    ]
    if prefixes:
        longest = max(len(str(entry["path_or_pattern"])) for entry in prefixes)
        winners = [entry for entry in prefixes if len(str(entry["path_or_pattern"])) == longest]
        if len(winners) != 1:
            raise RepositorySurfaceError(f"ambiguous prefix match: {path}")
        return winners[0]

    globs = [
        entry
        for entry in entries
        if entry.get("match_type") == "GLOB"
        and _glob_regex(str(entry.get("path_or_pattern"))).fullmatch(path)
    ]
    if not globs:
        return None
    best = max(_glob_specificity(str(entry["path_or_pattern"])) for entry in globs)
    winners = [
        entry for entry in globs if _glob_specificity(str(entry["path_or_pattern"])) == best
    ]
    if len(winners) != 1:
        raise RepositorySurfaceError(f"ambiguous glob match: {path}")
    return winners[0]


def _normalize_repository_inventory_path(value: str, *, name: str) -> str:
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise RepositorySurfaceError(f"{name} contains a control character")
    normalized = value.replace("\\", "/")
    return _validate_relative_pattern(normalized, "EXACT", name)


def collect_tracked_repository_paths(repository_root: Path | str) -> list[str]:
    """Return the exact stage-zero Git file inventory using binary-safe output."""

    root = Path(repository_root).resolve(strict=True)
    try:
        result = subprocess.run(
            ["git", "ls-files", "--stage", "-z"],
            cwd=root,
            check=False,
            capture_output=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RepositorySurfaceError("git ls-files could not be executed") from exc
    if result.returncode != 0:
        raise RepositorySurfaceError("git ls-files failed")
    paths: list[str] = []
    seen: set[str] = set()
    for index, raw_record in enumerate(result.stdout.split(b"\0")):
        if not raw_record:
            continue
        metadata, separator, raw_path = raw_record.partition(b"\t")
        if not separator:
            raise RepositorySurfaceError("git ls-files returned malformed stage data")
        fields = metadata.split()
        if len(fields) != 3:
            raise RepositorySurfaceError("git ls-files returned malformed metadata")
        mode, _object_id, stage = fields
        if stage != b"0":
            raise RepositorySurfaceError("git index contains an unmerged tracked path")
        if mode == b"160000":
            raise RepositorySurfaceError("submodules are not supported by the active-source view")
        try:
            decoded = raw_path.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RepositorySurfaceError("git ls-files returned a non-UTF-8 path") from exc
        path = _normalize_repository_inventory_path(
            decoded, name=f"git tracked path[{index}]"
        )
        if path in seen:
            raise RepositorySurfaceError(f"git tracked inventory contains a duplicate: {path}")
        seen.add(path)
        paths.append(path)
    return sorted(paths)


def _git_tracked_paths(root: Path) -> list[str]:
    return collect_tracked_repository_paths(root)


def validate_tracked_root_coverage(
    surface: Mapping[str, object],
    repository_root: Path | str,
    *,
    tracked_paths: Iterable[str] | None = None,
) -> dict[str, object]:
    """Fail when a tracked (or exported-present) top-level path is unclassified."""

    root = Path(repository_root).resolve(strict=True)
    if tracked_paths is not None:
        paths = [
            _normalize_repository_inventory_path(str(path), name="supplied tracked path")
            for path in tracked_paths
        ]
        mode = "SUPPLIED_TRACKED_PATHS"
    elif (root / ".git").exists():
        paths = _git_tracked_paths(root)
        mode = "GIT_LS_FILES"
    else:
        paths = [item.name for item in root.iterdir() if item.name != ".git"]
        mode = "EXPORTED_PRESENT_PATHS_ONLY"
    roots = sorted({path.replace("\\", "/").split("/", 1)[0] for path in paths if path})
    missing = [root_path for root_path in roots if resolve_surface_entry(surface, root_path) is None]
    if missing:
        raise RepositorySurfaceError(
            "unclassified tracked/exported root paths: " + ", ".join(missing)
        )
    return {"mode": mode, "classified_roots": roots, "omitted_private_paths_known": False}


def _module_target_path(root: Path, module: str) -> str:
    if not module or any(part in {"", ".", ".."} for part in module.split(".")):
        raise RepositorySurfaceError(f"invalid public command module: {module}")
    base = "src/" + module.replace(".", "/")
    candidates = (base + ".py", base + "/__init__.py")
    existing = [relative for relative in candidates if _safe_control_path(root, relative).is_file()]
    if len(existing) != 1:
        raise RepositorySurfaceError(f"public command target does not resolve once: {module}")
    return existing[0]


def _load_public_command_targets(repository_root: Path | str) -> dict[str, str]:
    root = Path(repository_root).resolve(strict=True)
    pyproject = _safe_control_path(root, "pyproject.toml")
    if not pyproject.is_file():
        raise RepositorySurfaceError("pyproject.toml is missing")
    try:
        payload = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise RepositorySurfaceError("pyproject.toml is unreadable") from exc
    scripts = payload.get("project", {}).get("scripts")
    if type(scripts) is not dict or not scripts:
        raise RepositorySurfaceError("pyproject public scripts are missing")
    targets: dict[str, str] = {}
    for name, target in sorted(scripts.items()):
        if type(name) is not str or not name or type(target) is not str or ":" not in target:
            raise RepositorySurfaceError("pyproject public script is malformed")
        targets[name] = target
    return targets


def validate_public_command_surfaces(
    surface: Mapping[str, object], repository_root: Path | str
) -> dict[str, str]:
    """Ensure every pyproject command points at an explicitly current module."""

    root = Path(repository_root).resolve(strict=True)
    resolved: dict[str, str] = {}
    for name, target in _load_public_command_targets(root).items():
        module, _callable = target.split(":", 1)
        relative = _module_target_path(root, module)
        entry = resolve_surface_entry(surface, relative)
        if entry is None:
            raise RepositorySurfaceError(f"public command target is unclassified: {relative}")
        classification = str(entry["classification"])
        if (
            classification not in PUBLIC_COMMAND_CLASSIFICATIONS
            or classification in FORBIDDEN_PUBLIC_COMMAND_CLASSIFICATIONS
            or entry["match_type"] != "EXACT"
        ):
            raise RepositorySurfaceError(
                f"public command {name} targets non-current surface {relative}: {classification}"
            )
        resolved[name] = relative
    return resolved


def _entry_for_role(
    surface: Mapping[str, object], authority_role: str
) -> Mapping[str, object]:
    raw_entries = surface.get("entries")
    if not isinstance(raw_entries, Sequence):
        raise RepositorySurfaceError("entries are unavailable")
    matches = [
        entry
        for entry in raw_entries
        if isinstance(entry, Mapping)
        and entry.get("authority_role") == authority_role
    ]
    if len(matches) != 1:
        raise RepositorySurfaceError(
            f"authority role must resolve exactly once: {authority_role}"
        )
    return matches[0]


def _exact_entry(
    surface: Mapping[str, object], repository_path: str
) -> Mapping[str, object]:
    entry = resolve_surface_entry(surface, repository_path)
    if (
        entry is None
        or entry.get("match_type") != "EXACT"
        or entry.get("path_or_pattern") != repository_path
    ):
        raise RepositorySurfaceError(
            f"human-view path requires an exact registry entry: {repository_path}"
        )
    return entry


def _markdown_cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def render_source_of_truth(
    surface: Mapping[str, object], public_commands: Mapping[str, str]
) -> str:
    """Render the deterministic human navigation view from canonical controls."""

    workflow = _entry_for_role(surface, "NORMAL_WORKFLOW_AUTHORITY")
    policy = _entry_for_role(surface, "DURABLE_SAFETY_POLICY_AUTHORITY")
    package = _entry_for_role(surface, "PUBLIC_PACKAGE_AND_COMMAND_AUTHORITY")
    _validate_source_of_truth_registry_entry(
        [
            entry
            for entry in surface.get("entries", [])  # type: ignore[union-attr]
            if isinstance(entry, Mapping)
        ]
    )
    pointer_entries = [
        (label, _entry_for_role(surface, role))
        for label, role in ACTIVE_POINTER_ROLES
    ]
    folder_entries = [
        (path, _exact_entry(surface, path)) for path in MAJOR_FOLDER_PATHS
    ]
    unresolved_count = sum(
        1
        for entry in surface.get("entries", [])  # type: ignore[union-attr]
        if isinstance(entry, Mapping)
        and entry.get("classification") == "UNRESOLVED_MANUAL_REVIEW"
    )

    lines = [
        "# Repository source of truth",
        "",
        "> **Generated navigation view.** This file is deterministically rendered from",
        "> `configs/repository_surface.json` and `pyproject.toml`. Do not maintain",
        "> repository roles independently in this file.",
        "",
        "## Purpose and authority",
        "",
        f"- `{workflow['path_or_pattern']}` controls normal day-to-day work.",
        f"- `{policy['path_or_pattern']}` contains durable safety and research-integrity policy.",
        "- `configs/repository_surface.json` is the canonical machine-readable path-role registry.",
        "- `SOURCE_OF_TRUTH.md` is this generated navigation view only; it is not a workflow or safety authority.",
        f"- `{package['path_or_pattern']}` defines the public package and command surface.",
        "- `README.md` provides setup and operator orientation.",
        "- `PROJECT_OUTLINE.md` is the detailed research runbook.",
        "- `PIPELINE_FOLDER_MAP.md` is a topology and reference guide, not authority.",
        "- `docs/LEGACY_WORKFLOWS.md` classifies retired workflow material.",
        "- `MASTER_AUDIT.md` and `META_MASTER_AUDIT.md` are audit specifications, not current-state dashboards.",
        "- `CODEX_HANDOFF.md` is continuation context only and grants no authority.",
        "- `PUBLIC_SNAPSHOT.md` is not an operational workflow authority.",
        "",
        "## Start here",
        "",
        f"1. Read `{workflow['path_or_pattern']}` for normal work.",
        f"2. Read `{policy['path_or_pattern']}` for durable safety policy.",
        "3. Use `SOURCE_OF_TRUTH.md` to navigate repository roles.",
        "4. Use `README.md` for setup.",
        "5. Use `PROJECT_OUTLINE.md` for the detailed research process.",
        "",
        "## Active machine-readable pointers",
        "",
        "| Role | Registry path |",
        "| --- | --- |",
    ]
    for label, entry in pointer_entries:
        lines.append(
            f"| {_markdown_cell(label)} | `{_markdown_cell(entry['path_or_pattern'])}` |"
        )
    lines.extend(
        [
            "",
            "Local-only pointers or catalogs may be absent from a clean provider-free source export.",
            "The micro pointer and catalog establish source selection only. They do not by themselves establish a frozen mechanism, trial registration, historical-row authority, research passage, holdout authority, production readiness, live-execution readiness, provider authorization, or trading authority. Rendering and validation do not open referenced market-data payloads.",
            "",
            "## Public commands",
            "",
            "The exact public command mapping comes from `[project.scripts]` in `pyproject.toml`.",
            "",
            "| Command | Python target |",
            "| --- | --- |",
        ]
    )
    for name, target in sorted(public_commands.items()):
        lines.append(f"| `{_markdown_cell(name)}` | `{_markdown_cell(target)}` |")
    lines.extend(
        [
            "",
            "Private helpers, documentation-only commands, ignored installation candidates, historical commands, and untracked execution-looking modules are not public commands.",
            "",
            "## Major folder roles",
            "",
            "These concise roles come from each exact registry classification and note. More-specific entries override the family role.",
            "",
            "| Family | Classification | Registry-derived role |",
            "| --- | --- | --- |",
        ]
    )
    for path, entry in folder_entries:
        lines.append(
            f"| `{path}/` | `{_markdown_cell(entry['classification'])}` | {_markdown_cell(entry['notes'])} |"
        )
    lines.extend(
        [
            "",
            "`FuturesLiveCockpit/` is a mixed packaging source/output surface and is not automatically disposable. Build, distribution, temporary, artifact, log, package, backup, and generated-report material still requires exact classification and review.",
            "",
            "## Historical and retired material",
            "",
            "Tracked does not imply current, and ignored does not imply disposable. Exact historical paths may remain at their original locations because plans, manifests, tests, receipts, reports, or other evidence bind their names or bytes. Historical material is not current workflow or command authority. A replacement must be explicitly declared with `current_replacement`; physical relocation requires separate reference and hash closure.",
            "",
            "## Generated, local-only, and unresolved material",
            "",
            "- `GENERATED_OUTPUT`: produced material whose existence does not provide deletion authority.",
            "- `REGENERABLE_CACHE`: a narrowly identified cache with understood regeneration; cleanup still needs a fresh census and separate approval.",
            "- `LOCAL_RUNTIME_STATE`: machine-local operating state that must be preserved unless separately governed.",
            "- `LOCAL_SECRET`: credential or secret material whose contents must never be inspected or reported by this view.",
            "- `MIXED_PACKAGING_SOURCE_OUTPUT`: tracked packaging inputs and generated output coexist under one family.",
            f"- `UNRESOLVED_MANUAL_REVIEW`: evidence is insufficient for an automatic decision. The registry currently contains {unresolved_count} such entries.",
            "- `PREPARED_NOT_EXECUTED`: a plan or preparation exists, but execution, activation, or publication is not established.",
            "",
            "Use `configs/repository_surface.json` for exact classifications. A present, ignored, or untracked file that looks like execution, publication, activation, installation, or cleanup code does not become current merely because it exists.",
            "",
            "## Cleanup and deletion rules",
            "",
            "The registry grants no deletion authority, and `SOURCE_OF_TRUTH.md` grants no deletion authority. Only exact regenerable cache paths may become cleanup candidates, after a fresh machine-local census and separate exact approval. Modified, staged, and non-ignored untracked work is preserved by default.",
            "",
            "Active catalogs, data, manifests, reports, state, receipts, authorization uses, credentials, and unpublished evidence are protected. Build output, distributions, `.venv`, temporary material, artifacts, packages, backups, logs, and reports are not automatically disposable. Git ignore status does not establish deletion safety, and broad cleanup commands must not be used.",
            "",
            "Prohibited broad cleanup commands:",
            "",
            "```text",
            "git clean -fdx",
            "git clean -fdX",
            "```",
            "",
            "## Supersession rules",
            "",
            "Currentness is not determined by the highest version number, newest modification timestamp, newest-looking filename, tracked status, ignored status, directory presence, or words such as final, authoritative, successor, current, active, live, old, retired, or legacy.",
            "",
            "Resolve currentness through `CURRENT_WORKFLOW.md`, `AGENTS.md`, `configs/repository_surface.json`, exact active pointers, explicit `current_replacement` relationships, `pyproject.toml` public command definitions, and current fail-closed policy boundaries.",
            "",
            "## What this document does not authorize",
            "",
            "Neither `SOURCE_OF_TRUTH.md` nor the registry authorizes deletion, movement or renaming, provider access, credential access, market-data reads, real-history research, holdout or forward access, prediction publication, candidate sealing, active-data mutation, publication, installation, activation, live smoke, trading, order placement, staging, commit, or push.",
        ]
    )
    direct_section = _direct_documentation_section(
        surface, "source_of_truth_section"
    )
    if direct_section is not None:
        lines.extend(("", *direct_section.rstrip("\n").splitlines()))
    return "\n".join(lines) + "\n"


def _surface_entries(surface: Mapping[str, object]) -> list[Mapping[str, object]]:
    raw_entries = surface.get("entries")
    if not isinstance(raw_entries, Sequence):
        raise RepositorySurfaceError("entries are unavailable")
    entries = [entry for entry in raw_entries if isinstance(entry, Mapping)]
    if len(entries) != len(raw_entries):
        raise RepositorySurfaceError("entries contain a non-object value")
    return entries


def _classification_counts(surface: Mapping[str, object]) -> list[tuple[str, int]]:
    entries = _surface_entries(surface)
    raw_order = surface.get("allowed_classifications")
    if not isinstance(raw_order, Sequence):
        raise RepositorySurfaceError("allowed classifications are unavailable")
    order = [str(item) for item in raw_order]
    return [
        (classification, sum(entry.get("classification") == classification for entry in entries))
        for classification in order
    ]


def _major_family_classifications(
    surface: Mapping[str, object], family: str
) -> list[str]:
    entries = _surface_entries(surface)
    represented = {
        str(entry["classification"])
        for entry in entries
        if str(entry["path_or_pattern"]).split("/", 1)[0] == family
    }
    return [
        classification
        for classification, _count in _classification_counts(surface)
        if classification in represented
    ]


def _markdown_table_body_row_count(text: str) -> int:
    lines = text.splitlines()
    count = 0
    for index in range(len(lines) - 1):
        if not lines[index].startswith("|") or not lines[index + 1].startswith("| ---"):
            continue
        cursor = index + 2
        while cursor < len(lines) and lines[cursor].startswith("|"):
            count += 1
            cursor += 1
    return count


def render_pipeline_folder_map(
    surface: Mapping[str, object], public_commands: Mapping[str, str]
) -> str:
    """Render the concise topology guide from the registry and pyproject scripts."""

    entries = _surface_entries(surface)
    _validate_pipeline_folder_map_registry_entry(entries)
    registry_match = resolve_surface_entry(surface, REGISTRY_PATH.as_posix())
    if registry_match is None:
        raise RepositorySurfaceError("canonical registry path is unclassified")
    authority_entries = [
        (label, _entry_for_role(surface, role))
        for label, role in PIPELINE_FOLDER_MAP_AUTHORITY_ROLES
    ]
    folder_entries = [
        (path, _exact_entry(surface, path)) for path in MAJOR_FOLDER_PATHS
    ]
    classification_counts = _classification_counts(surface)
    unresolved_count = dict(classification_counts).get("UNRESOLVED_MANUAL_REVIEW", 0)

    lines = [
        "# Current pipeline folder map",
        "",
        "> **Generated topology view.** This file is deterministically rendered from",
        "> `configs/repository_surface.json` and `pyproject.toml`. Do not maintain",
        "> topology classifications independently in this file.",
        "",
        "## Purpose and authority",
        "",
        "This map is concise current navigation. It is generated and is not an authority system.",
        "",
        "- `CURRENT_WORKFLOW.md` controls normal work.",
        "- `AGENTS.md` contains durable safety and research-integrity policy.",
        "- `configs/repository_surface.json` is the canonical machine-readable path-role registry.",
        "- `SOURCE_OF_TRUTH.md` is the broader generated repository navigation view.",
        "- `PIPELINE_FOLDER_MAP.md` is this generated topology view only.",
        "- `ACTIVE_SOURCE_FILES.txt` is the generated virtual view of tracked current operational and supporting files.",
        "- `pyproject.toml` defines the public package and command surface.",
        "- `docs/LEGACY_WORKFLOWS.md` controls interpretation of retired workflow material.",
        "- The complete former map is preserved at `docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.md`.",
        "",
        "This view does not replace the workflow, safety policy, canonical registry, or broader source-of-truth view, and it does not establish research, provider, production, execution, or trading readiness.",
        "",
        "## Current authority and pointer surfaces",
        "",
        "Rows below resolve from canonical registry roles. Directory or file presence never supplies authority.",
        "",
        "| Role | Path | Classification | Local only | Registry boundary |",
        "| --- | --- | --- | --- | --- |",
        f"| Canonical path-role registry | `{REGISTRY_PATH.as_posix()}` | `{_markdown_cell(registry_match['classification'])}` | {'yes' if registry_match['local_only'] else 'no'} | {_markdown_cell(surface['purpose'])} |",
    ]
    for label, entry in authority_entries:
        lines.append(
            "| "
            f"{_markdown_cell(label)} | `{_markdown_cell(entry['path_or_pattern'])}` | "
            f"`{_markdown_cell(entry['classification'])}` | "
            f"{'yes' if entry['local_only'] else 'no'} | {_markdown_cell(entry['notes'])} |"
        )
    lines.extend(
        [
            "",
            "The standard Alpha pointer/catalog and micro source pointer/catalog remain separate. Local-only controls may be absent from clean provider-free exports. Micro source selection does not establish a frozen mechanism, registered trial, historical-row authority, research passage, holdout authority, production readiness, execution readiness, or trading authority.",
            "",
            "`CertifiedResearchGateway` is the sole current real-history registration and trial-execution boundary; use remains separately controlled. `futures-pipeline` is synthetic-only. No other public command provides a real-history execution surface.",
            "",
            "## Public commands",
            "",
            "This is the exact deterministic `[project.scripts]` mapping from `pyproject.toml`.",
            "",
            "| Command | Python target |",
            "| --- | --- |",
        ]
    )
    for name, target in sorted(public_commands.items()):
        lines.append(f"| `{_markdown_cell(name)}` | `{_markdown_cell(target)}` |")
    lines.extend(
        [
            "",
            "Private helpers, documentation-only commands, historical scripts, ignored installation candidates, and untracked execution-looking modules are not public commands.",
            "",
            "## Major repository topology",
            "",
            "Each family appears once. Its exact root entry supplies the role, tracking/local state, deletion policy, and notes; represented classifications summarize all registry entries in that family.",
            "",
            "| Family | Current role | Represented classifications | Tracking and locality | Deletion policy | Registry notes |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
    )
    for path, entry in folder_entries:
        represented = ", ".join(
            f"`{classification}`"
            for classification in _major_family_classifications(surface, path)
        )
        lines.append(
            f"| `{path}/` | `{_markdown_cell(entry['authority_role'])}` | {represented} | "
            f"`{_markdown_cell(entry['tracked_expected'])}`; local-only: "
            f"{'yes' if entry['local_only'] else 'no'} | "
            f"`{_markdown_cell(entry['deletion_policy'])}` | {_markdown_cell(entry['notes'])} |"
        )
    lines.extend(
        [
            "",
            "## Classification summary",
            "",
            "The counts below use only the canonical classification vocabulary and count registry entries, not files present in this checkout.",
            "",
            "| Classification | Entry count |",
            "| --- | --- |",
        ]
    )
    for classification, count in classification_counts:
        lines.append(f"| `{classification}` | {count} |")
    lines.extend(
        [
            "",
            "## Historical and retired boundaries",
            "",
            "The former complete map, including its version-by-version status chronology, is preserved at `docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.md`; its provenance manifest is `docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.json`. That historical record is not a runtime renderer input.",
            "",
            "Historical exact paths may remain at their original locations. `docs/LEGACY_WORKFLOWS.md` controls interpretation of retired workflow material. Historical path presence does not make a surface current; tracked does not imply current; ignored does not imply disposable; and a current replacement must be explicitly declared. No version-by-version history belongs in this generated root map.",
            "",
            "## Generated, local-only, mixed, and unresolved material",
            "",
            "Local-only files may be absent from clean provider-free exports. Generated-looking paths are not automatically disposable. `FuturesLiveCockpit/` is a mixed packaging source/output surface. Directory presence does not grant authority, and untracked execution-looking code is not current merely because it exists.",
            "",
            f"The registry currently contains {unresolved_count} `UNRESOLVED_MANUAL_REVIEW` entries. They require explicit review; use `configs/repository_surface.json` for their exact paths and policies.",
            "",
            "## Safety and non-authority boundary",
            "",
            "Neither this generated map nor the registry authorizes deletion, movement or renaming, provider access, credential access, market-data reads, historical-row access, holdout or forward access, research execution, prediction publication, candidate sealing, active-data mutation, publication, installation, activation, live smoke, trading, order placement, staging, commit, or push.",
            "",
            "Cache deletion still requires a fresh exact machine-local census and separate approval.",
        ]
    )
    direct_section = _direct_documentation_section(
        surface, "pipeline_folder_map_section"
    )
    if direct_section is not None:
        lines.extend(("", *direct_section.rstrip("\n").splitlines()))
    return "\n".join(lines) + "\n"


def validate_pipeline_folder_map(
    document: bytes,
    *,
    surface: Mapping[str, object],
    public_commands: Mapping[str, str],
) -> dict[str, object]:
    """Validate exact deterministic topology bytes and non-authority limits."""

    expected_entry_count = _expected_registry_entry_count(surface)
    if len(_surface_entries(surface)) != expected_entry_count:
        raise RepositorySurfaceError(
            f"registry entry count must remain {expected_entry_count}"
        )
    classification_counts = dict(_classification_counts(surface))
    if classification_counts.get("UNRESOLVED_MANUAL_REVIEW") != EXPECTED_UNRESOLVED_ENTRY_COUNT:
        raise RepositorySurfaceError(
            f"unresolved entry count must remain {EXPECTED_UNRESOLVED_ENTRY_COUNT}"
        )
    expected = render_pipeline_folder_map(surface, public_commands).encode("utf-8")
    if document != expected:
        raise RepositorySurfaceError(
            "PIPELINE_FOLDER_MAP.md is absent, stale, manually edited, or inconsistent"
        )
    if len(document) > PIPELINE_FOLDER_MAP_MAX_BYTES:
        raise RepositorySurfaceError("PIPELINE_FOLDER_MAP.md exceeds the byte limit")
    if b"\r" in document or not document.endswith(b"\n") or document.endswith(b"\n\n"):
        raise RepositorySurfaceError(
            "PIPELINE_FOLDER_MAP.md must use LF and exactly one final newline"
        )
    try:
        text = document.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RepositorySurfaceError("PIPELINE_FOLDER_MAP.md is not UTF-8") from exc
    word_count = len(text.split())
    if word_count > PIPELINE_FOLDER_MAP_MAX_WORDS:
        raise RepositorySurfaceError("PIPELINE_FOLDER_MAP.md exceeds the word limit")
    table_row_count = _markdown_table_body_row_count(text)
    if table_row_count > PIPELINE_FOLDER_MAP_MAX_TABLE_ROWS:
        raise RepositorySurfaceError("PIPELINE_FOLDER_MAP.md exceeds the table-row limit")
    headings = [line[3:] for line in text.splitlines() if line.startswith("## ")]
    direct_section = _direct_documentation_section(
        surface, "pipeline_folder_map_section"
    )
    expected_headings = list(PIPELINE_FOLDER_MAP_SECTIONS)
    if direct_section is not None:
        expected_headings.append(direct_section.splitlines()[0][3:])
    if headings != expected_headings:
        raise RepositorySurfaceError("PIPELINE_FOLDER_MAP.md section order is invalid")
    scrubbed = text.replace(
        "docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.md", ""
    ).replace("docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.json", "")
    if (
        _MACHINE_PATH.search(text)
        or _ABSOLUTE_POSIX_PATH.search(text)
        or _UNC_PATH.search(text)
        or "futures-v2-" in text
    ):
        raise RepositorySurfaceError("PIPELINE_FOLDER_MAP.md contains a machine path")
    if direct_section is not None:
        scrubbed = scrubbed.replace(direct_section.rstrip("\n"), "")
    if _DATE_OR_TIMESTAMP.search(scrubbed) or _COMMIT_SHA.search(scrubbed):
        raise RepositorySurfaceError("PIPELINE_FOLDER_MAP.md contains transient identity")
    if _CREDENTIAL_VALUE.search(text):
        raise RepositorySurfaceError("PIPELINE_FOLDER_MAP.md contains a credential value")
    for variable in ("USERNAME", "USER"):
        username = os.environ.get(variable, "").strip()
        if len(username) >= 3 and re.search(rf"(?i)\b{re.escape(username)}\b", text):
            raise RepositorySurfaceError("PIPELINE_FOLDER_MAP.md contains a user name")
    for path in MAJOR_FOLDER_PATHS:
        if text.count(f"| `{path}/` |") != 1:
            raise RepositorySurfaceError(
                f"PIPELINE_FOLDER_MAP.md major family must appear once: {path}"
            )
    text_lines = text.splitlines()
    for classification, count in classification_counts.items():
        if text_lines.count(f"| `{classification}` | {count} |") != 1:
            raise RepositorySurfaceError(
                f"PIPELINE_FOLDER_MAP.md classification row is invalid: {classification}"
            )
    required_fragments = (
        "CURRENT_WORKFLOW.md` controls normal work",
        "AGENTS.md` contains durable safety",
        "canonical machine-readable path-role registry",
        "SOURCE_OF_TRUTH.md` is the broader generated",
        "ACTIVE_SOURCE_FILES.txt` is the generated virtual view",
        "CertifiedResearchGateway",
        "sole current real-history",
        "futures-pipeline` is synthetic-only",
        "docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.md",
        "docs/LEGACY_WORKFLOWS.md",
        "Neither this generated map nor the registry authorizes",
        "fresh exact machine-local census and separate approval",
    )
    for fragment in required_fragments:
        if fragment not in text:
            raise RepositorySurfaceError(
                f"PIPELINE_FOLDER_MAP.md omits required boundary: {fragment}"
            )
    for forbidden in (
        "`CURRENT_REACHABLE`",
        "`SYNTHETIC_ONLY`",
        "`HISTORICAL_ROW_APPROVAL_REQUIRED`",
        "`RETIRED`",
        "Phase 1A",
        "Phase 11",
        "metadata preflight v2",
        "acquisition v21",
        "PASS_METADATA_ONLY",
        "live_cockpit/execution",
        "Tradovate",
    ):
        if forbidden.lower() in text.lower():
            raise RepositorySurfaceError(
                f"PIPELINE_FOLDER_MAP.md contains historical or untracked material: {forbidden}"
            )
    return {
        "valid": True,
        "word_count": word_count,
        "byte_count": len(document),
        "table_row_count": table_row_count,
        "public_command_count": len(public_commands),
    }


def validate_source_of_truth(
    document: bytes,
    *,
    surface: Mapping[str, object],
    public_commands: Mapping[str, str],
) -> dict[str, object]:
    """Validate deterministic bytes and the non-authorizing human-view contract."""

    expected = render_source_of_truth(surface, public_commands).encode("utf-8")
    if document != expected:
        raise RepositorySurfaceError(
            "SOURCE_OF_TRUTH.md is absent, stale, manually edited, or inconsistent"
        )
    if len(document) > SOURCE_OF_TRUTH_MAX_BYTES:
        raise RepositorySurfaceError("SOURCE_OF_TRUTH.md exceeds the byte limit")
    if b"\r" in document or not document.endswith(b"\n") or document.endswith(b"\n\n"):
        raise RepositorySurfaceError(
            "SOURCE_OF_TRUTH.md must use LF and exactly one final newline"
        )
    try:
        text = document.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RepositorySurfaceError("SOURCE_OF_TRUTH.md is not UTF-8") from exc
    word_count = len(text.split())
    if word_count > SOURCE_OF_TRUTH_MAX_WORDS:
        raise RepositorySurfaceError("SOURCE_OF_TRUTH.md exceeds the word limit")
    headings = [line[3:] for line in text.splitlines() if line.startswith("## ")]
    direct_section = _direct_documentation_section(surface, "source_of_truth_section")
    expected_headings = list(SOURCE_OF_TRUTH_SECTIONS)
    if direct_section is not None:
        expected_headings.append(direct_section.splitlines()[0][3:])
    if headings != expected_headings:
        raise RepositorySurfaceError("SOURCE_OF_TRUTH.md section order is invalid")
    if (
        _MACHINE_PATH.search(text)
        or _ABSOLUTE_POSIX_PATH.search(text)
        or _UNC_PATH.search(text)
        or "futures-v2-repository-audit-" in text
    ):
        raise RepositorySurfaceError("SOURCE_OF_TRUTH.md contains a machine path")
    transient_scrubbed = (
        text.replace(direct_section.rstrip("\n"), "")
        if direct_section is not None
        else text
    )
    if _DATE_OR_TIMESTAMP.search(transient_scrubbed) or _COMMIT_SHA.search(transient_scrubbed):
        raise RepositorySurfaceError("SOURCE_OF_TRUTH.md contains transient identity")
    if _CREDENTIAL_VALUE.search(text):
        raise RepositorySurfaceError("SOURCE_OF_TRUTH.md contains a credential value")
    for variable in ("USERNAME", "USER"):
        username = os.environ.get(variable, "").strip()
        if len(username) >= 3 and re.search(rf"(?i)\b{re.escape(username)}\b", text):
            raise RepositorySurfaceError("SOURCE_OF_TRUTH.md contains a user name")
    required_fragments = (
        "The registry grants no deletion authority",
        "`SOURCE_OF_TRUTH.md` grants no deletion authority",
        "git clean -fdx",
        "git clean -fdX",
        "Neither `SOURCE_OF_TRUTH.md` nor the registry authorizes",
    )
    for fragment in required_fragments:
        if fragment not in text:
            raise RepositorySurfaceError(
                f"SOURCE_OF_TRUTH.md omits required safety text: {fragment}"
            )
    if "live_cockpit/execution" in text or "Tradovate" in text:
        raise RepositorySurfaceError(
            "SOURCE_OF_TRUTH.md promotes an untracked execution surface"
        )
    return {
        "valid": True,
        "word_count": word_count,
        "byte_count": len(document),
        "public_command_count": len(public_commands),
    }


def expected_source_of_truth_bytes(
    surface: Mapping[str, object], repository_root: Path | str
) -> bytes:
    """Return validated deterministic UTF-8 bytes without writing a file."""

    root = Path(repository_root).resolve(strict=True)
    validate_public_command_surfaces(surface, root)
    commands = _load_public_command_targets(root)
    rendered = render_source_of_truth(surface, commands).encode("utf-8")
    validate_source_of_truth(
        rendered,
        surface=surface,
        public_commands=commands,
    )
    return rendered


def compare_source_of_truth_file(
    surface: Mapping[str, object], repository_root: Path | str
) -> dict[str, object]:
    """Fail closed unless the in-repository Markdown equals the renderer bytes."""

    root = Path(repository_root).resolve(strict=True)
    path = _safe_control_path(root, SOURCE_OF_TRUTH_PATH.as_posix())
    if path.is_symlink() or not path.is_file():
        raise RepositorySurfaceError("SOURCE_OF_TRUTH.md is missing")
    try:
        actual = path.read_bytes()
    except OSError as exc:
        raise RepositorySurfaceError("SOURCE_OF_TRUTH.md is unreadable") from exc
    validate_public_command_surfaces(surface, root)
    commands = _load_public_command_targets(root)
    return validate_source_of_truth(
        actual,
        surface=surface,
        public_commands=commands,
    )


def expected_pipeline_folder_map_bytes(
    surface: Mapping[str, object], repository_root: Path | str
) -> bytes:
    """Return validated deterministic topology bytes without writing a file."""

    root = Path(repository_root).resolve(strict=True)
    validate_public_command_surfaces(surface, root)
    commands = _load_public_command_targets(root)
    rendered = render_pipeline_folder_map(surface, commands).encode("utf-8")
    validate_pipeline_folder_map(
        rendered,
        surface=surface,
        public_commands=commands,
    )
    return rendered


def compare_pipeline_folder_map_file(
    surface: Mapping[str, object], repository_root: Path | str
) -> dict[str, object]:
    """Fail closed unless the tracked topology map equals deterministic bytes."""

    root = Path(repository_root).resolve(strict=True)
    path = _safe_control_path(root, PIPELINE_FOLDER_MAP_PATH.as_posix())
    if path.is_symlink() or not path.is_file():
        raise RepositorySurfaceError("PIPELINE_FOLDER_MAP.md is missing")
    try:
        actual = path.read_bytes()
    except OSError as exc:
        raise RepositorySurfaceError("PIPELINE_FOLDER_MAP.md is unreadable") from exc
    validate_public_command_surfaces(surface, root)
    commands = _load_public_command_targets(root)
    return validate_pipeline_folder_map(
        actual,
        surface=surface,
        public_commands=commands,
    )


def active_source_paths(
    surface: Mapping[str, object], tracked_paths: Iterable[str]
) -> dict[str, list[str]]:
    """Resolve one tracked inventory through the canonical registry matcher."""

    result = {classification: [] for classification in ACTIVE_SOURCE_CLASSIFICATIONS}
    seen: set[str] = set()
    for index, raw_path in enumerate(tracked_paths):
        path = _normalize_repository_inventory_path(
            str(raw_path), name=f"active-source inventory path[{index}]"
        )
        if path in seen:
            raise RepositorySurfaceError(
                f"active-source inventory contains a duplicate: {path}"
            )
        seen.add(path)
        entry = resolve_surface_entry(surface, path)
        if entry is None:
            raise RepositorySurfaceError(
                f"tracked path does not resolve through the registry: {path}"
            )
        classification = str(entry["classification"])
        if classification in result:
            result[classification].append(path)
    for paths in result.values():
        paths.sort()
    return result


def render_active_source_files(
    surface: Mapping[str, object], tracked_paths: Iterable[str]
) -> str:
    """Render the deterministic virtual active-source file list."""

    _validate_active_source_files_registry_entry(_surface_entries(surface))
    classified = active_source_paths(surface, tracked_paths)
    lines = [*ACTIVE_SOURCE_HEADER, ""]
    for index, classification in enumerate(ACTIVE_SOURCE_CLASSIFICATIONS):
        lines.append(f"# {classification}")
        lines.extend(classified[classification])
        if index != len(ACTIVE_SOURCE_CLASSIFICATIONS) - 1:
            lines.append("")
    return "\n".join(lines) + "\n"


def _parse_active_source_files(document: bytes) -> dict[str, list[str]]:
    if len(document) > ACTIVE_SOURCE_FILES_MAX_BYTES:
        raise RepositorySurfaceError("ACTIVE_SOURCE_FILES.txt exceeds the byte limit")
    if b"\r" in document or not document.endswith(b"\n") or document.endswith(b"\n\n"):
        raise RepositorySurfaceError(
            "ACTIVE_SOURCE_FILES.txt must use LF and exactly one final newline"
        )
    try:
        text = document.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RepositorySurfaceError("ACTIVE_SOURCE_FILES.txt is not UTF-8") from exc
    lines = text.splitlines()
    prefix = [*ACTIVE_SOURCE_HEADER, "", f"# {ACTIVE_SOURCE_CLASSIFICATIONS[0]}"]
    if lines[: len(prefix)] != prefix:
        raise RepositorySurfaceError("ACTIVE_SOURCE_FILES.txt header or section order is invalid")
    cursor = len(prefix)
    result: dict[str, list[str]] = {ACTIVE_SOURCE_CLASSIFICATIONS[0]: []}
    while cursor < len(lines) and lines[cursor] != "":
        result[ACTIVE_SOURCE_CLASSIFICATIONS[0]].append(lines[cursor])
        cursor += 1
    if cursor + 1 >= len(lines) or lines[cursor : cursor + 2] != [
        "",
        f"# {ACTIVE_SOURCE_CLASSIFICATIONS[1]}",
    ]:
        raise RepositorySurfaceError("ACTIVE_SOURCE_FILES.txt section boundary is invalid")
    result[ACTIVE_SOURCE_CLASSIFICATIONS[1]] = lines[cursor + 2 :]
    all_paths: list[str] = []
    for classification in ACTIVE_SOURCE_CLASSIFICATIONS:
        paths = result[classification]
        if not paths:
            raise RepositorySurfaceError(
                f"ACTIVE_SOURCE_FILES.txt has no {classification} files"
            )
        normalized = [
            _validate_relative_pattern(path, "EXACT", "active-source listed path")
            for path in paths
        ]
        if normalized != paths:
            raise RepositorySurfaceError("ACTIVE_SOURCE_FILES.txt paths are not normalized")
        if paths != sorted(paths):
            raise RepositorySurfaceError(
                f"ACTIVE_SOURCE_FILES.txt {classification} paths are not sorted"
            )
        if len(paths) != len(set(paths)):
            raise RepositorySurfaceError(
                f"ACTIVE_SOURCE_FILES.txt {classification} paths contain duplicates"
            )
        all_paths.extend(paths)
    if len(all_paths) != len(set(all_paths)):
        raise RepositorySurfaceError("ACTIVE_SOURCE_FILES.txt repeats a path across sections")
    return result


def _present_export_file_paths(root: Path) -> list[str]:
    paths: list[str] = []
    for directory, directory_names, file_names in os.walk(root, topdown=True, followlinks=False):
        directory_path = Path(directory)
        directory_names[:] = sorted(
            name
            for name in directory_names
            if name != ".git" and not (directory_path / name).is_symlink()
        )
        for name in sorted(file_names):
            candidate = directory_path / name
            if candidate.is_symlink() or not candidate.is_file():
                continue
            relative = candidate.relative_to(root).as_posix()
            paths.append(
                _normalize_repository_inventory_path(
                    relative, name="present export path"
                )
            )
    return sorted(paths)


def _active_source_inventory(
    surface: Mapping[str, object],
    root: Path,
    *,
    tracked_paths: Iterable[str] | None = None,
) -> tuple[str, list[str]]:
    entry = _validate_active_source_files_registry_entry(_surface_entries(surface))
    if tracked_paths is not None:
        mode = "SUPPLIED_TRACKED_EXACT"
        paths = [
            _normalize_repository_inventory_path(str(path), name="supplied tracked path")
            for path in tracked_paths
        ]
    elif (root / ".git").exists():
        mode = "GIT_TRACKED_EXACT"
        paths = collect_tracked_repository_paths(root)
    else:
        return "PRESENT_EXPORT_SUBSET", _present_export_file_paths(root)
    pending_self = ACTIVE_SOURCE_FILES_PATH.as_posix()
    if pending_self not in paths:
        if entry.get("tracked_expected") != "TRACKED":
            raise RepositorySurfaceError(
                "pending ACTIVE_SOURCE_FILES.txt requires TRACKED registry expectation"
            )
        paths.append(pending_self)
    return mode, sorted(paths)


def validate_active_source_files(
    document: bytes,
    *,
    surface: Mapping[str, object],
    repository_root: Path | str,
    tracked_paths: Iterable[str] | None = None,
) -> dict[str, object]:
    """Validate exact Git-backed completeness or an explicit no-Git subset."""

    root = Path(repository_root).resolve(strict=True)
    expected_entry_count = _expected_registry_entry_count(surface)
    if len(_surface_entries(surface)) != expected_entry_count:
        raise RepositorySurfaceError(
            f"registry entry count must remain {expected_entry_count}"
        )
    parsed = _parse_active_source_files(document)
    mode, inventory = _active_source_inventory(
        surface, root, tracked_paths=tracked_paths
    )
    listed = [path for classification in ACTIVE_SOURCE_CLASSIFICATIONS for path in parsed[classification]]
    if ACTIVE_SOURCE_FILES_PATH.as_posix() not in listed:
        raise RepositorySurfaceError("ACTIVE_SOURCE_FILES.txt must include itself")
    for classification in ACTIVE_SOURCE_CLASSIFICATIONS:
        for path in parsed[classification]:
            entry = resolve_surface_entry(surface, path)
            if entry is None or entry.get("classification") != classification:
                raise RepositorySurfaceError(
                    f"ACTIVE_SOURCE_FILES.txt classification mismatch: {path}"
                )
    completeness_reconstructed = mode != "PRESENT_EXPORT_SUBSET"
    if completeness_reconstructed:
        expected = render_active_source_files(surface, inventory).encode("utf-8")
        if document != expected:
            raise RepositorySurfaceError(
                "ACTIVE_SOURCE_FILES.txt is missing, stale, manually edited, overinclusive, or incomplete"
            )
    else:
        listed_set = set(listed)
        missing_present: list[str] = []
        for path in inventory:
            entry = resolve_surface_entry(surface, path)
            if entry is None:
                continue
            if (
                entry.get("classification") in ACTIVE_SOURCE_CLASSIFICATIONS
                and entry.get("local_only") is False
                and path not in listed_set
            ):
                missing_present.append(path)
        if missing_present:
            raise RepositorySurfaceError(
                "ACTIVE_SOURCE_FILES.txt omits present export active files: "
                + ", ".join(missing_present)
            )
    commands = validate_public_command_surfaces(surface, root)
    missing_targets = sorted(set(commands.values()) - set(listed))
    if missing_targets:
        raise RepositorySurfaceError(
            "ACTIVE_SOURCE_FILES.txt omits public command targets: "
            + ", ".join(missing_targets)
        )
    return {
        "valid": True,
        "inventory_mode": mode,
        "completeness_reconstructed": completeness_reconstructed,
        "operational_file_count": len(parsed["CURRENT_OPERATIONAL"]),
        "supporting_file_count": len(parsed["CURRENT_SUPPORTING"]),
        "total_file_count": len(listed),
        "byte_count": len(document),
        "line_count": len(document.splitlines()),
    }


def expected_active_source_files_bytes(
    surface: Mapping[str, object], repository_root: Path | str
) -> bytes:
    """Return validated deterministic active-source bytes without writing."""

    root = Path(repository_root).resolve(strict=True)
    if not (root / ".git").exists():
        path = _safe_control_path(root, ACTIVE_SOURCE_FILES_PATH.as_posix())
        if path.is_symlink() or not path.is_file():
            raise RepositorySurfaceError("ACTIVE_SOURCE_FILES.txt is missing")
        document = path.read_bytes()
        validate_active_source_files(
            document, surface=surface, repository_root=root
        )
        return document
    _mode, inventory = _active_source_inventory(surface, root)
    rendered = render_active_source_files(surface, inventory).encode("utf-8")
    validate_active_source_files(
        rendered, surface=surface, repository_root=root
    )
    return rendered


def compare_active_source_files_file(
    surface: Mapping[str, object], repository_root: Path | str
) -> dict[str, object]:
    """Validate the tracked active-source view without mutating the checkout."""

    root = Path(repository_root).resolve(strict=True)
    path = _safe_control_path(root, ACTIVE_SOURCE_FILES_PATH.as_posix())
    if path.is_symlink() or not path.is_file():
        raise RepositorySurfaceError("ACTIVE_SOURCE_FILES.txt is missing")
    try:
        actual = path.read_bytes()
    except OSError as exc:
        raise RepositorySurfaceError("ACTIVE_SOURCE_FILES.txt is unreadable") from exc
    return validate_active_source_files(
        actual,
        surface=surface,
        repository_root=root,
    )


def _validate_pointer_document(path: Path, *, schema: str) -> None:
    if path.stat().st_size > 262_144:
        raise RepositorySurfaceError(f"pointer control document is unexpectedly large: {path.name}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RepositorySurfaceError(f"pointer is not readable JSON: {path.name}") from exc
    pointer = _expect_object(payload, "pointer")
    if pointer.get("schema_version") != schema:
        raise RepositorySurfaceError(f"pointer schema mismatch: {path.name}")
    for key, value in pointer.items():
        if key.endswith("_path"):
            _validate_relative_pattern(value, "EXACT", f"pointer {key}")
        elif key.endswith("_sha256") and (
            type(value) is not str or _HEX_256.fullmatch(value) is None
        ):
            raise RepositorySurfaceError(f"pointer hash is invalid: {key}")


def _validate_pointer_metadata(surface: Mapping[str, object], root: Path) -> None:
    pointer_specs = (
        (
            "configs/active_alpha_research_ladder.json",
            "active_alpha_research_ladder/1.0.0",
        ),
        (
            "configs/active_micro_alpha_research_ladder.json",
            "active_micro_alpha_research_ladder/1.0.0",
        ),
    )
    for relative, schema in pointer_specs:
        entry = resolve_surface_entry(surface, relative)
        if entry is None:
            raise RepositorySurfaceError(f"active pointer is unclassified: {relative}")
        path = _safe_control_path(root, relative)
        if not path.exists():
            if entry["local_only"] or entry["tracked_expected"] in {"OPTIONAL", "IGNORED_LOCAL", "ABSENT_EXPECTED"}:
                continue
            raise RepositorySurfaceError(f"required active pointer is absent: {relative}")
        if path.is_symlink() or not path.is_file():
            raise RepositorySurfaceError(f"active pointer is not a regular file: {relative}")
        _validate_pointer_document(path, schema=schema)
    micro_path = _safe_control_path(root, "configs/active_micro_alpha_research_ladder.json")
    if micro_path.is_file():
        micro = json.loads(micro_path.read_text(encoding="utf-8"))
        if (
            micro.get("activation_scope") != "SOURCE_CATALOG_ONLY"
            or micro.get("historical_evaluation_authorized") is not False
            or micro.get("holdout_2025_access_authorized") is not False
            or micro.get("forward_2026_access_authorized") is not False
        ):
            raise RepositorySurfaceError("micro pointer exceeds source-selection authority")


def validate_repository_checkout(repository_root: Path | str) -> dict[str, object]:
    """Run the complete read-only validation used by CI and the module CLI."""

    root = Path(repository_root).resolve(strict=True)
    surface = load_repository_surface(root)
    validate_repository_surface(surface, repository_root=root)
    coverage = validate_tracked_root_coverage(surface, root)
    commands = validate_public_command_surfaces(surface, root)
    if len(commands) != EXPECTED_PUBLIC_COMMAND_COUNT:
        raise RepositorySurfaceError(
            f"public command count must remain {EXPECTED_PUBLIC_COMMAND_COUNT}"
        )
    source_of_truth = compare_source_of_truth_file(surface, root)
    pipeline_folder_map = compare_pipeline_folder_map_file(surface, root)
    active_source_files = compare_active_source_files_file(surface, root)
    unresolved_count = dict(_classification_counts(surface))[
        "UNRESOLVED_MANUAL_REVIEW"
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "valid": True,
        "registry_valid": True,
        "source_of_truth_valid": source_of_truth["valid"],
        "pipeline_folder_map_valid": pipeline_folder_map["valid"],
        "active_source_files_valid": active_source_files["valid"],
        "entry_count": len(surface["entries"]),
        "unresolved_entry_count": unresolved_count,
        "tracked_root_mode": coverage["mode"],
        "active_source_inventory_mode": active_source_files["inventory_mode"],
        "public_commands": commands,
        "public_command_count": len(commands),
        "source_of_truth_word_count": source_of_truth["word_count"],
        "source_of_truth_byte_count": source_of_truth["byte_count"],
        "pipeline_folder_map_word_count": pipeline_folder_map["word_count"],
        "pipeline_folder_map_byte_count": pipeline_folder_map["byte_count"],
        "pipeline_folder_map_table_row_count": pipeline_folder_map[
            "table_row_count"
        ],
        "active_source_operational_file_count": active_source_files[
            "operational_file_count"
        ],
        "active_source_supporting_file_count": active_source_files[
            "supporting_file_count"
        ],
        "active_source_total_file_count": active_source_files["total_file_count"],
        "active_source_byte_count": active_source_files["byte_count"],
        "active_source_line_count": active_source_files["line_count"],
        "mutations_performed": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate repository surface registry")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    output = parser.add_mutually_exclusive_group()
    output.add_argument(
        "--print-source-of-truth",
        action="store_true",
        help="print the deterministic Markdown view without writing files",
    )
    output.add_argument(
        "--print-pipeline-folder-map",
        action="store_true",
        help="print the deterministic topology view without writing files",
    )
    output.add_argument(
        "--print-active-source-files",
        action="store_true",
        help="print the deterministic virtual active-source view without writing files",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.print_source_of_truth:
            root = args.root.resolve(strict=True)
            surface = load_repository_surface(root)
            validate_repository_surface(surface, repository_root=None)
            sys.stdout.buffer.write(expected_source_of_truth_bytes(surface, root))
            return 0
        if args.print_pipeline_folder_map:
            root = args.root.resolve(strict=True)
            surface = load_repository_surface(root)
            validate_repository_surface(surface, repository_root=None)
            sys.stdout.buffer.write(expected_pipeline_folder_map_bytes(surface, root))
            return 0
        if args.print_active_source_files:
            root = args.root.resolve(strict=True)
            surface = load_repository_surface(root)
            validate_repository_surface(surface, repository_root=None)
            sys.stdout.buffer.write(expected_active_source_files_bytes(surface, root))
            return 0
        report = validate_repository_checkout(args.root)
    except RepositorySurfaceError as exc:
        print(json.dumps({"valid": False, "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
