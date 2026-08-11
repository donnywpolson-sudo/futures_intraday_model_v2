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

    serialized = json.dumps(surface, sort_keys=True)
    if _MACHINE_PATH.search(serialized):
        raise RepositorySurfaceError("registry contains a machine-specific path")
    if _AFFIRMATIVE_AUTHORITY.search(serialized):
        raise RepositorySurfaceError("registry text grants a controlled authority")

    _validate_authority_roles(entries, authority_precedence)
    _validate_source_of_truth_registry_entry(entries)
    _validate_generated_root_policies(entries)
    _validate_standard_and_micro_roles(entries)

    if repository_root is not None:
        root = Path(repository_root).resolve(strict=True)
        validate_tracked_root_coverage(surface, root)
        validate_public_command_surfaces(surface, root)
        _validate_pointer_metadata(surface, root)
        compare_source_of_truth_file(surface, root)


def _validate_authority_roles(
    entries: Sequence[Mapping[str, object]],
    authority_precedence: Sequence[Mapping[str, object]],
) -> None:
    by_role: dict[str, list[Mapping[str, object]]] = {}
    for entry in entries:
        by_role.setdefault(str(entry["authority_role"]), []).append(entry)
    for role, expected_path in REQUIRED_ROLE_PATHS.items():
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
    for role, path in REQUIRED_ROLE_PATHS.items():
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


def _git_tracked_paths(root: Path) -> list[str]:
    try:
        result = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=root,
            check=False,
            capture_output=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RepositorySurfaceError("git ls-files could not be executed") from exc
    if result.returncode != 0:
        raise RepositorySurfaceError("git ls-files failed")
    try:
        return [item.decode("utf-8") for item in result.stdout.split(b"\0") if item]
    except UnicodeDecodeError as exc:
        raise RepositorySurfaceError("git ls-files returned non-UTF-8 paths") from exc


def validate_tracked_root_coverage(
    surface: Mapping[str, object],
    repository_root: Path | str,
    *,
    tracked_paths: Iterable[str] | None = None,
) -> dict[str, object]:
    """Fail when a tracked (or exported-present) top-level path is unclassified."""

    root = Path(repository_root).resolve(strict=True)
    if tracked_paths is not None:
        paths = list(tracked_paths)
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
    return "\n".join(lines) + "\n"


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
    if headings != list(SOURCE_OF_TRUTH_SECTIONS):
        raise RepositorySurfaceError("SOURCE_OF_TRUTH.md section order is invalid")
    if (
        _MACHINE_PATH.search(text)
        or _ABSOLUTE_POSIX_PATH.search(text)
        or _UNC_PATH.search(text)
        or "futures-v2-repository-audit-" in text
    ):
        raise RepositorySurfaceError("SOURCE_OF_TRUTH.md contains a machine path")
    if _DATE_OR_TIMESTAMP.search(text) or _COMMIT_SHA.search(text):
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
    source_of_truth = compare_source_of_truth_file(surface, root)
    return {
        "schema_version": SCHEMA_VERSION,
        "valid": True,
        "registry_valid": True,
        "source_of_truth_valid": source_of_truth["valid"],
        "entry_count": len(surface["entries"]),
        "tracked_root_mode": coverage["mode"],
        "public_commands": commands,
        "public_command_count": len(commands),
        "source_of_truth_word_count": source_of_truth["word_count"],
        "source_of_truth_byte_count": source_of_truth["byte_count"],
        "mutations_performed": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate repository surface registry")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--print-source-of-truth",
        action="store_true",
        help="print the deterministic Markdown view without writing files",
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
        report = validate_repository_checkout(args.root)
    except RepositorySurfaceError as exc:
        print(json.dumps({"valid": False, "error": str(exc)}, sort_keys=True))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
