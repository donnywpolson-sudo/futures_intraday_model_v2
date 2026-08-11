from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from futures_rebuild.repository_surface import (
    RepositorySurfaceError,
    load_repository_surface,
    resolve_surface_entry,
    validate_public_command_surfaces,
    validate_repository_surface,
    validate_tracked_root_coverage,
)


ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "configs" / "repository_surface.json"
pytestmark = pytest.mark.current


def _surface() -> dict[str, object]:
    return copy.deepcopy(load_repository_surface(ROOT))


def _entry(surface: dict[str, object], path: str) -> dict[str, object]:
    matches = [
        entry
        for entry in surface["entries"]  # type: ignore[index]
        if entry["path_or_pattern"] == path  # type: ignore[index]
    ]
    assert len(matches) == 1
    return matches[0]


def _minimal_pointer(schema: str) -> dict[str, object]:
    return {
        "schema_version": schema,
        "contract_path": "state/example/contract.json",
        "contract_sha256": "a" * 64,
        "profile_path": "state/example/profile.json",
        "profile_sha256": "b" * 64,
    }


def _write_export_controls(root: Path) -> None:
    (root / "configs").mkdir(parents=True)
    (root / "src" / "futures_rebuild").mkdir(parents=True)
    (root / "configs" / "active_alpha_research_ladder.json").write_text(
        json.dumps(_minimal_pointer("active_alpha_research_ladder/1.0.0")),
        encoding="utf-8",
    )
    (root / "src" / "futures_rebuild" / "pipeline.py").write_text(
        "def main(): return 0\n", encoding="utf-8"
    )
    (root / "pyproject.toml").write_text(
        '[project]\nname = "surface-export"\nversion = "1"\n'
        '[project.scripts]\nfutures-pipeline = "futures_rebuild.pipeline:main"\n',
        encoding="utf-8",
    )


def test_valid_registry_loads_and_validates_current_checkout() -> None:
    surface = load_repository_surface(ROOT)

    validate_repository_surface(surface, repository_root=ROOT)

    assert surface["schema_version"] == "repository_surface/1.0.0"
    assert len(surface["entries"]) == 177


def test_unknown_classification_is_rejected() -> None:
    surface = _surface()
    _entry(surface, "README.md")["classification"] = "CURRENTISH"

    with pytest.raises(RepositorySurfaceError, match="undeclared classification"):
        validate_repository_surface(surface)


def test_unknown_deletion_policy_is_rejected() -> None:
    surface = _surface()
    _entry(surface, "README.md")["deletion_policy"] = "DELETE_WHEN_OLD"

    with pytest.raises(RepositorySurfaceError, match="undeclared deletion policy"):
        validate_repository_surface(surface)


@pytest.mark.parametrize(
    "bad_path",
    [
        "C:/Users/example/repository/file.txt",
        "/absolute/file.txt",
        "../outside.txt",
        "configs/../../outside.txt",
        r"configs\machine-specific.json",
        "configs//unnormalized.json",
    ],
)
def test_absolute_escaping_and_unnormalized_paths_are_rejected(
    bad_path: str,
) -> None:
    surface = _surface()
    _entry(surface, "README.md")["path_or_pattern"] = bad_path

    with pytest.raises(RepositorySurfaceError):
        validate_repository_surface(surface)


def test_exact_prefix_and_glob_precedence_is_deterministic() -> None:
    exact = {"match_type": "EXACT", "path_or_pattern": "a/exact.py"}
    broad_prefix = {"match_type": "PREFIX", "path_or_pattern": "a"}
    narrow_prefix = {"match_type": "PREFIX", "path_or_pattern": "a/nested"}
    broad_glob = {"match_type": "GLOB", "path_or_pattern": "g/**"}
    narrow_glob = {"match_type": "GLOB", "path_or_pattern": "g/*.py"}
    entries = [broad_glob, broad_prefix, exact, narrow_glob, narrow_prefix]

    assert resolve_surface_entry(entries, "a/exact.py") is exact
    assert resolve_surface_entry(entries, "a/nested/file.py") is narrow_prefix
    assert resolve_surface_entry(entries, "a/other.py") is broad_prefix
    assert resolve_surface_entry(entries, "g/module.py") is narrow_glob
    assert resolve_surface_entry(entries, "g/nested/module.py") is broad_glob


def test_duplicate_and_indistinguishable_entries_are_rejected() -> None:
    surface = _surface()
    surface["entries"].append(copy.deepcopy(_entry(surface, "README.md")))  # type: ignore[index]

    with pytest.raises(RepositorySurfaceError, match="duplicate EXACT entry"):
        validate_repository_surface(surface)


def test_equal_specificity_matching_globs_fail_closed() -> None:
    entries = [
        {"match_type": "GLOB", "path_or_pattern": "x/a*.py"},
        {"match_type": "GLOB", "path_or_pattern": "x/*a.py"},
    ]

    with pytest.raises(RepositorySurfaceError, match="ambiguous glob match"):
        resolve_surface_entry(entries, "x/aa.py")


def test_git_tracked_root_coverage_is_complete() -> None:
    report = validate_tracked_root_coverage(_surface(), ROOT)

    assert report["mode"] == "GIT_LS_FILES"
    assert "configs" in report["classified_roots"]
    assert "src" in report["classified_roots"]


def test_synthetic_tracked_root_fails_when_unclassified(tmp_path: Path) -> None:
    surface = _surface()

    with pytest.raises(RepositorySurfaceError, match="new-tracked-root"):
        validate_tracked_root_coverage(
            surface,
            tmp_path,
            tracked_paths=["README.md", "new-tracked-root/file.txt"],
        )


def test_export_fallback_is_explicit_about_present_paths_only(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("export\n", encoding="utf-8")

    report = validate_tracked_root_coverage(_surface(), tmp_path)

    assert report == {
        "mode": "EXPORTED_PRESENT_PATHS_ONLY",
        "classified_roots": ["README.md"],
        "omitted_private_paths_known": False,
    }


def test_workflow_authority_is_unique_and_exact() -> None:
    surface = _surface()
    duplicate = copy.deepcopy(_entry(surface, "README.md"))
    duplicate["path_or_pattern"] = "WORKFLOW_COPY.md"
    duplicate["authority_role"] = "NORMAL_WORKFLOW_AUTHORITY"
    surface["entries"].append(duplicate)  # type: ignore[index]

    with pytest.raises(RepositorySurfaceError, match="NORMAL_WORKFLOW_AUTHORITY"):
        validate_repository_surface(surface)


def test_durable_policy_authority_is_unique_and_exact() -> None:
    surface = _surface()
    _entry(surface, "AGENTS.md")["authority_role"] = "SUPPORTING_POLICY"

    with pytest.raises(
        RepositorySurfaceError, match="DURABLE_SAFETY_POLICY_AUTHORITY"
    ):
        validate_repository_surface(surface)


def test_standard_and_micro_pointer_roles_cannot_replace_each_other() -> None:
    surface = _surface()
    micro = _entry(surface, "configs/active_micro_alpha_research_ladder.json")
    micro["authority_role"] = "ACTIVE_STANDARD_ALPHA_IDENTITY"

    with pytest.raises(RepositorySurfaceError, match="ACTIVE_STANDARD_ALPHA_IDENTITY"):
        validate_repository_surface(surface)


def test_absent_local_only_micro_pointer_and_catalog_are_ci_safe(
    tmp_path: Path,
) -> None:
    _write_export_controls(tmp_path)

    validate_repository_surface(_surface(), repository_root=tmp_path)

    assert not (tmp_path / "configs" / "active_micro_alpha_research_ladder.json").exists()
    assert not (tmp_path / "data" / "active" / "catalogs" / "apex_micro.json").exists()


def test_protected_root_cannot_be_broad_regenerable_cache() -> None:
    surface = _surface()
    bad = copy.deepcopy(_entry(surface, "configs"))
    bad.update(
        {
            "path_or_pattern": "configs/generated",
            "match_type": "PREFIX",
            "classification": "REGENERABLE_CACHE",
            "deletion_policy": "DELETE_ONLY_AFTER_FRESH_CENSUS_AND_SEPARATE_APPROVAL",
        }
    )
    surface["entries"].append(bad)  # type: ignore[index]

    with pytest.raises(RepositorySurfaceError, match="protected root"):
        validate_repository_surface(surface)


def test_nested_pycache_specificity_overrides_source_parent() -> None:
    surface = _surface()

    entry = resolve_surface_entry(
        surface, "src/futures_rebuild/__pycache__/pipeline.cpython-311.pyc"
    )

    assert entry is not None
    assert entry["path_or_pattern"] == "src/futures_rebuild/__pycache__"
    assert entry["classification"] == "REGENERABLE_CACHE"


@pytest.mark.parametrize(
    "path",
    ["api.env", ".env", "databento.env", "configs/local_credentials.json"],
)
def test_secret_paths_have_noninspection_policy(path: str) -> None:
    entry = resolve_surface_entry(_surface(), path)

    assert entry is not None
    assert entry["classification"] == "LOCAL_SECRET"
    assert entry["deletion_policy"] == "SECRET_CONTENT_NEVER_INSPECT_OR_REPORT"


def test_registry_rejects_affirmative_deletion_authority_semantics() -> None:
    surface = _surface()
    _entry(surface, "README.md")["notes"] = "This entry authorizes deletion."

    with pytest.raises(RepositorySurfaceError, match="controlled authority"):
        validate_repository_surface(surface)


def test_non_authority_matrix_denies_every_controlled_action() -> None:
    surface = _surface()

    assert surface["non_authority"]
    assert set(surface["non_authority"].values()) == {False}  # type: ignore[union-attr]


def test_public_command_targets_are_explicitly_current() -> None:
    resolved = validate_public_command_surfaces(_surface(), ROOT)

    assert set(resolved) == {
        "futures-dbn-catalog",
        "futures-readiness",
        "futures-master-audit",
        "futures-pipeline",
        "futures-retirement-audit",
        "futures-meta-audit",
        "futures-high-risk-prepare",
    }
    for relative in resolved.values():
        entry = resolve_surface_entry(_surface(), relative)
        assert entry is not None
        assert entry["match_type"] == "EXACT"
        assert entry["classification"] in {"CURRENT_OPERATIONAL", "CURRENT_SUPPORTING"}


def test_public_command_targeting_retired_surface_is_rejected(tmp_path: Path) -> None:
    module = tmp_path / "src" / "futures_rebuild" / "micro_alpha_publication.py"
    module.parent.mkdir(parents=True)
    module.write_text("def main(): return 0\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "retired-target"\nversion = "1"\n'
        '[project.scripts]\nretired = "futures_rebuild.micro_alpha_publication:main"\n',
        encoding="utf-8",
    )

    with pytest.raises(RepositorySurfaceError, match="non-current surface"):
        validate_public_command_surfaces(_surface(), tmp_path)


def test_versioned_historical_module_does_not_become_current_by_name() -> None:
    entry = resolve_surface_entry(_surface(), "src/futures_rebuild/tier1_bracket_v4.py")

    assert entry is not None
    assert entry["classification"] == "HISTORICAL_HASH_BOUND"
    assert entry["current_replacement"] == "src/futures_rebuild/certified_research_gateway.py"


def test_futures_live_cockpit_is_mixed_but_inputs_override_output() -> None:
    surface = _surface()
    root_entry = resolve_surface_entry(surface, "FuturesLiveCockpit")
    spec_entry = resolve_surface_entry(
        surface, "FuturesLiveCockpit/_internal/FuturesLiveCockpit.spec"
    )
    output_entry = resolve_surface_entry(
        surface, "FuturesLiveCockpit/_internal/python311.dll"
    )

    assert root_entry is not None
    assert root_entry["classification"] == "MIXED_PACKAGING_SOURCE_OUTPUT"
    assert spec_entry is not None
    assert spec_entry["classification"] == "CURRENT_OPERATIONAL"
    assert output_entry is not None
    assert output_entry["classification"] == "GENERATED_OUTPUT"
    assert output_entry["deletion_policy"] == "NO_AUTOMATIC_DELETE"


def test_requirements_duplicates_are_compatibility_protected() -> None:
    surface = _surface()
    conventional = _entry(surface, "requirements.txt")
    cockpit = _entry(surface, "requirements-live-cockpit.txt")

    assert (ROOT / "requirements.txt").read_bytes() == (
        ROOT / "requirements-live-cockpit.txt"
    ).read_bytes()
    assert conventional["classification"] == "CURRENT_SUPPORTING"
    assert cockpit["classification"] == "CURRENT_SUPPORTING"
    assert conventional["deletion_policy"] == "NO_AUTOMATIC_DELETE"
    assert cockpit["deletion_policy"] == "NO_AUTOMATIC_DELETE"
    assert "external" in conventional["notes"]
    assert "external" in cockpit["notes"]


@pytest.mark.parametrize(
    "path",
    [
        "scripts/prepare_safe_cleanup_inventory_v5.py",
        "state/unpublished_evidence/safe_cleanup_preparation_v5/plan.json",
        "scripts/prepare_safe_cleanup_candidate_census_v6.py",
        "state/unpublished_evidence/safe_cleanup_candidate_census_v6/census.json",
    ],
)
def test_stale_cleanup_artifacts_are_not_current_cleanup_authority(path: str) -> None:
    entry = resolve_surface_entry(_surface(), path)

    assert entry is not None
    assert entry["classification"] == "PREPARED_NOT_EXECUTED"
    assert entry["hash_bound"] is True
    assert "NO_CURRENT_CLEANUP_AUTHORITY" in entry["authority_role"]
    assert entry["deletion_policy"] == "PRESERVE"


@pytest.mark.parametrize("path", ["build", "dist", "tmp", "artifacts"])
def test_ambiguous_generated_roots_have_no_cleanup_candidate_policy(path: str) -> None:
    entry = resolve_surface_entry(_surface(), path)

    assert entry is not None
    assert entry["deletion_policy"] != (
        "DELETE_ONLY_AFTER_FRESH_CENSUS_AND_SEPARATE_APPROVAL"
    )


def test_registry_contains_no_machine_specific_audit_directory() -> None:
    raw = REGISTRY_PATH.read_text(encoding="utf-8")

    assert "AppData" not in raw
    assert "futures-v2-repository-audit-" not in raw
    assert "C:/Users/" not in raw
    assert "C:\\Users\\" not in raw


def test_control_rationale_records_why_simple_rules_fail() -> None:
    rationale = _surface()["control_rationale"]

    assert "active catalogs" in rationale["risk_prevented"]  # type: ignore[index]
    assert "without guessing" in rationale["decision_improved"]  # type: ignore[index]
    assert "ignored" in rationale["why_simpler_rules_are_insufficient"]  # type: ignore[index]
