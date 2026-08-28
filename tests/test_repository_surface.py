from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib

import pytest

from futures_rebuild.repository_surface import (
    ACTIVE_SOURCE_CLASSIFICATIONS,
    ACTIVE_SOURCE_FILES_MAX_BYTES,
    ACTIVE_SOURCE_FILES_ROLE,
    EXPECTED_PUBLIC_COMMAND_COUNT,
    EXPECTED_REGISTRY_ENTRY_COUNT,
    EXPECTED_UNRESOLVED_ENTRY_COUNT,
    MAJOR_FOLDER_PATHS,
    PIPELINE_FOLDER_MAP_AUTHORITY_ROLES,
    PIPELINE_FOLDER_MAP_MAX_BYTES,
    PIPELINE_FOLDER_MAP_MAX_TABLE_ROWS,
    PIPELINE_FOLDER_MAP_MAX_WORDS,
    PIPELINE_FOLDER_MAP_ROLE,
    PIPELINE_FOLDER_MAP_SECTIONS,
    RepositorySurfaceError,
    SOURCE_OF_TRUTH_MAX_BYTES,
    SOURCE_OF_TRUTH_MAX_WORDS,
    SOURCE_OF_TRUTH_SECTIONS,
    active_source_paths,
    collect_tracked_repository_paths,
    compare_active_source_files_file,
    compare_pipeline_folder_map_file,
    compare_source_of_truth_file,
    expected_active_source_files_bytes,
    expected_pipeline_folder_map_bytes,
    expected_source_of_truth_bytes,
    load_repository_surface,
    render_active_source_files,
    render_pipeline_folder_map,
    render_source_of_truth,
    resolve_surface_entry,
    validate_active_source_files,
    validate_active_alpha_authority_candidate,
    validate_active_alpha_authority_closure,
    validate_public_command_surfaces,
    validate_pipeline_folder_map,
    validate_repository_checkout,
    validate_repository_surface,
    validate_source_of_truth,
    validate_tracked_root_coverage,
)


ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "configs" / "repository_surface.json"
SOURCE_OF_TRUTH_PATH = ROOT / "SOURCE_OF_TRUTH.md"
PIPELINE_FOLDER_MAP_PATH = ROOT / "PIPELINE_FOLDER_MAP.md"
ACTIVE_SOURCE_FILES_PATH = ROOT / "ACTIVE_SOURCE_FILES.txt"
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


def _public_commands(root: Path = ROOT) -> dict[str, str]:
    payload = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    return dict(sorted(payload["project"]["scripts"].items()))


def _write_export_controls(
    root: Path,
    *,
    include_document: bool = True,
    include_pipeline_map: bool = True,
    include_active_source_files: bool = True,
) -> None:
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
    if include_document:
        (root / "SOURCE_OF_TRUTH.md").write_bytes(
            expected_source_of_truth_bytes(_surface(), root)
        )
    if include_pipeline_map:
        (root / "PIPELINE_FOLDER_MAP.md").write_bytes(
            expected_pipeline_folder_map_bytes(_surface(), root)
        )
    if include_active_source_files:
        (root / "ACTIVE_SOURCE_FILES.txt").write_bytes(
            ACTIVE_SOURCE_FILES_PATH.read_bytes()
        )


def test_active_alpha_authority_is_tracked_and_loadable() -> None:
    pointer = json.loads((ROOT / "configs/active_alpha_research_ladder.json").read_text(encoding="utf-8"))
    prospective = validate_active_alpha_authority_candidate(ROOT, pointer)
    live = validate_active_alpha_authority_closure(ROOT)
    assert prospective == live
    assert prospective["valid"] is True
    assert set(prospective["required_paths"]) == {
        "configs/research_universe_contract.json",
        "state/alpha_ladder_registry/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/alpha_tiered.yaml",
        "state/alpha_ladder_registry/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/universe_contract.json",
        "state/alpha_ladder_registry/d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18/universe_contract.json",
    }



ALPHA_AUTHORITY_FIXTURE_PATHS = {
    "configs/research_universe_contract.json",
    "state/alpha_ladder_registry/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/alpha_tiered.yaml",
    "state/alpha_ladder_registry/53252c8d2351937105103aa6884719f9599cc1448a7908c63795b8fbd2362815/universe_contract.json",
    "state/alpha_ladder_registry/d3ab84356351568473ccdef935b20eda6779dcd681478415125a668d913dfd18/universe_contract.json",
}


def _write_alpha_authority_fixture(root: Path) -> dict[str, object]:
    pointer = json.loads(
        (ROOT / "configs/active_alpha_research_ladder.json").read_text(
            encoding="utf-8"
        )
    )
    for relative in sorted(ALPHA_AUTHORITY_FIXTURE_PATHS):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(
        ["git", "add", "--", *sorted(ALPHA_AUTHORITY_FIXTURE_PATHS)],
        cwd=root,
        check=True,
    )
    return pointer


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("absent", "Alpha authority target is absent"),
        ("hash_mismatch", "Alpha authority target hash changed"),
        ("untracked", "Alpha authority target is not Git-tracked"),
        ("transitive_absent", "Alpha authority target is absent"),
        ("schema_incompatible", "registered Alpha authority does not load"),
    ],
)
def test_active_alpha_authority_candidate_fails_closed(
    tmp_path: Path, case: str, message: str
) -> None:
    pointer = _write_alpha_authority_fixture(tmp_path)
    contract = tmp_path / str(pointer["contract_path"])
    transitive = tmp_path / "configs/research_universe_contract.json"
    if case == "absent":
        contract.unlink()
    elif case == "hash_mismatch":
        contract.write_bytes(contract.read_bytes() + b"\n")
    elif case == "untracked":
        subprocess.run(
            ["git", "rm", "--cached", "-q", "--", str(pointer["contract_path"])],
            cwd=tmp_path,
            check=True,
        )
    elif case == "transitive_absent":
        transitive.unlink()
    elif case == "schema_incompatible":
        document = json.loads(contract.read_text(encoding="utf-8"))
        document["schema_version"] = "alpha_research_ladder_contract/incompatible"
        contract.write_bytes(
            json.dumps(
                document, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            + b"\n"
        )
        pointer["contract_sha256"] = hashlib.sha256(contract.read_bytes()).hexdigest()
    else:
        raise AssertionError(case)
    with pytest.raises(RepositorySurfaceError, match=message):
        validate_active_alpha_authority_candidate(tmp_path, pointer)


def test_valid_registry_loads_and_validates_current_checkout() -> None:
    surface = load_repository_surface(ROOT)

    validate_repository_surface(surface, repository_root=ROOT)

    assert surface["schema_version"] == "repository_surface/1.0.0"
    assert len(surface["entries"]) == (
        251 if surface.get("current_direct_authority_registry_id") else 189
    )


AUDITS_SURFACE_PATHS = {
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/activation_receipt.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/application_startup_runs.csv",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/artifact_hashes.sha256",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/authority_sources.md",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/baseline_runs.csv",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/build_manifest.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/cache_failure_matrix.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/cache_incident_and_rollback.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/closure_status.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/collect_measurements.py",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/current_state.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/deletion_reconciliation.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/executable_inventory.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/final_report.md",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/functional_regression.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/governance_reconciliation.md",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/inspect_cache_readonly.py",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/launcher_inventory.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/live_cache_benchmarks.csv",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/package_inventory.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/prepare_cache_binding_rollback.py",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/prior_claims_verification.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/process_tree_metrics.csv",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/race_and_shutdown_tests.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/recover_cache_candidate.py",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/release_manifest.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/remediated_runs.csv",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/reproducibility.md",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/rollback_rebuild_receipt.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/source_change_inventory.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/startup_path.md",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/steady_state_metrics.csv",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/steady_state_metrics.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/test_results.json",
    "audits/futures_live_cockpit/closure_flc_20260825T1933482823061Z_d4271bf9/ui_render_metrics.json",
    "audits/futures_live_cockpit/main_integration_flc_20260825T2329100495034Z_c1807e4d/certification.json",
}


FINAL_EVALUATION_SUCCESSOR_SURFACE_PATHS = {
    "scripts/prepare_final_252_pipeline_successor_v1.py",
    "src/futures_rebuild/final_evaluation_recalibration.py",
    "tests/test_final_evaluation_recalibration.py",
}



def test_audits_surface_entries_are_exact_closed_and_complete() -> None:
    surface = _surface()
    tracked = set(
        subprocess.check_output(
            ["git", "ls-files", "--", "audits"],
            cwd=ROOT,
            text=True,
        ).splitlines()
    )
    selected = {
        entry["path_or_pattern"]: entry
        for entry in surface["entries"]
        if entry["path_or_pattern"] in AUDITS_SURFACE_PATHS
    }
    root = _entry(surface, "audits")
    assert tracked == AUDITS_SURFACE_PATHS
    assert set(selected) == AUDITS_SURFACE_PATHS
    assert root["match_type"] == "EXACT"
    assert root["classification"] == "HISTORICAL_HASH_BOUND"
    assert root["authority_role"] == "IMMUTABLE_COCKPIT_AUDIT_EVIDENCE_ROOT"
    assert root["hash_bound"] is None
    assert all(entry["match_type"] == "EXACT" for entry in selected.values())
    assert all(entry["classification"] == "HISTORICAL_HASH_BOUND" for entry in selected.values())
    assert all(entry["authority_role"] == "IMMUTABLE_COCKPIT_AUDIT_EVIDENCE" for entry in selected.values())
    assert all(entry["hash_bound"] is True for entry in selected.values())


def test_unknown_future_audits_descendant_fails_closed() -> None:
    surface = _surface()
    known = sorted(AUDITS_SURFACE_PATHS)[0]
    report = validate_tracked_root_coverage(surface, ROOT, tracked_paths=[known])
    assert report["classified_roots"] == ["audits"]
    with pytest.raises(
        RepositorySurfaceError, match="unclassified exact-descendant paths"
    ):
        validate_tracked_root_coverage(
            surface,
            ROOT,
            tracked_paths=[known, "audits/future-unclassified-evidence.json"],
        )


def test_final_evaluation_successor_surface_entries_are_exact() -> None:
    surface = _surface()
    selected = {
        entry["path_or_pattern"]: entry
        for entry in surface["entries"]
        if entry["path_or_pattern"] in FINAL_EVALUATION_SUCCESSOR_SURFACE_PATHS
    }
    assert set(selected) == FINAL_EVALUATION_SUCCESSOR_SURFACE_PATHS
    assert selected["src/futures_rebuild/final_evaluation_recalibration.py"]["classification"] == "CURRENT_OPERATIONAL"
    assert selected["scripts/prepare_final_252_pipeline_successor_v1.py"]["classification"] == "CURRENT_SUPPORTING"
    assert selected["tests/test_final_evaluation_recalibration.py"]["classification"] == "CURRENT_SUPPORTING"


@pytest.mark.parametrize("successor_count", [250, 252])
def test_direct_authority_registry_count_rejects_non_251(successor_count: int) -> None:
    surface = _surface()
    if successor_count == 250:
        surface["entries"] = [
            entry
            for entry in surface["entries"]
            if entry["path_or_pattern"] != "tests/test_final_evaluation_recalibration.py"
        ]
    else:
        extra = copy.deepcopy(_entry(surface, "README.md"))
        extra["path_or_pattern"] = "docs/nonexistent-final-252-count-sentinel.md"
        extra["authority_role"] = "FINAL_252_COUNT_SENTINEL"
        extra["tracked_expected"] = "ABSENT_EXPECTED"
        surface["entries"].append(extra)
    assert len(surface["entries"]) == successor_count
    with pytest.raises(RepositorySurfaceError, match="registry entry count must remain 251"):
        expected_pipeline_folder_map_bytes(surface, ROOT)


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


def test_source_of_truth_registry_entry_is_unique_and_supporting() -> None:
    surface = _surface()
    entry = _entry(surface, "SOURCE_OF_TRUTH.md")

    assert entry == {
        "path_or_pattern": "SOURCE_OF_TRUTH.md",
        "match_type": "EXACT",
        "classification": "CURRENT_SUPPORTING",
        "authority_role": "HUMAN_SOURCE_OF_TRUTH_VIEW",
        "current_replacement": None,
        "hash_bound": False,
        "tracked_expected": "TRACKED",
        "local_only": False,
        "deletion_policy": "PRESERVE",
        "owner": "repository_governance",
        "notes": entry["notes"],
    }
    assert sum(
        item["authority_role"] == "HUMAN_SOURCE_OF_TRUTH_VIEW"
        for item in surface["entries"]  # type: ignore[index]
    ) == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("match_type", "PREFIX"),
        ("classification", "CURRENT_OPERATIONAL"),
        ("authority_role", "NORMAL_WORKFLOW_AUTHORITY"),
        ("tracked_expected", "OPTIONAL"),
        ("local_only", True),
        ("hash_bound", None),
        ("deletion_policy", "NO_AUTOMATIC_DELETE"),
    ],
)
def test_source_of_truth_registry_contract_fails_closed(
    field: str, value: object
) -> None:
    surface = _surface()
    _entry(surface, "SOURCE_OF_TRUTH.md")[field] = value

    with pytest.raises(RepositorySurfaceError):
        validate_repository_surface(surface)


def test_generated_view_role_cannot_be_duplicated() -> None:
    surface = _surface()
    _entry(surface, "README.md")["authority_role"] = "HUMAN_SOURCE_OF_TRUTH_VIEW"

    with pytest.raises(RepositorySurfaceError, match="source-of-truth view role"):
        validate_repository_surface(surface)


def test_source_of_truth_rendering_is_deterministic_and_matches_tracked_file() -> None:
    surface = _surface()
    first = expected_source_of_truth_bytes(surface, ROOT)
    second = expected_source_of_truth_bytes(copy.deepcopy(surface), ROOT)

    assert first == second == SOURCE_OF_TRUTH_PATH.read_bytes()
    assert compare_source_of_truth_file(surface, ROOT)["valid"] is True


def test_source_of_truth_section_order_is_stable() -> None:
    text = SOURCE_OF_TRUTH_PATH.read_text(encoding="utf-8")
    headings = [line[3:] for line in text.splitlines() if line.startswith("## ")]

    expected_headings = list(SOURCE_OF_TRUTH_SECTIONS)
    direct_documentation = _surface().get("current_direct_authority_documentation")
    if isinstance(direct_documentation, dict):
        direct_section = direct_documentation.get("source_of_truth_section")
        if isinstance(direct_section, str):
            expected_headings.append(direct_section.splitlines()[0][3:])

    assert headings == expected_headings


def test_source_of_truth_public_commands_are_sorted_and_complete() -> None:
    text = SOURCE_OF_TRUTH_PATH.read_text(encoding="utf-8")
    commands = _public_commands()
    expected_rows = [f"| `{name}` | `{target}` |" for name, target in commands.items()]
    rendered_rows = [line for line in text.splitlines() if line in expected_rows]

    assert list(commands) == sorted(commands)
    assert rendered_rows == expected_rows
    assert len(rendered_rows) == len(commands) == 7


def test_source_of_truth_active_pointer_paths_are_exact_and_complete() -> None:
    surface = _surface()
    text = SOURCE_OF_TRUTH_PATH.read_text(encoding="utf-8")
    expected = {
        "configs/active_alpha_research_ladder.json",
        "data/active/catalog.json",
        "configs/active_micro_alpha_research_ladder.json",
        "data/active/catalogs/apex_micro.json",
    }

    for path in expected:
        entry = resolve_surface_entry(surface, path)
        assert entry is not None
        assert entry["match_type"] == "EXACT"
        assert f"`{path}`" in text
    assert text.count("| Standard Alpha pointer |") == 1
    assert text.count("| Micro source-selection pointer |") == 1


def test_source_of_truth_is_lf_utf8_with_one_final_newline() -> None:
    document = SOURCE_OF_TRUTH_PATH.read_bytes()

    assert document.decode("utf-8")
    assert b"\r" not in document
    assert document.endswith(b"\n")
    assert not document.endswith(b"\n\n")


def test_source_of_truth_stays_within_word_and_byte_limits() -> None:
    document = SOURCE_OF_TRUTH_PATH.read_bytes()

    assert len(document) <= SOURCE_OF_TRUTH_MAX_BYTES
    assert len(document.decode("utf-8").split()) <= SOURCE_OF_TRUTH_MAX_WORDS


def test_source_of_truth_contains_no_transient_machine_identity() -> None:
    text = SOURCE_OF_TRUTH_PATH.read_text(encoding="utf-8")

    assert not re.search(r"[A-Za-z]:[\\/]", text)
    assert not re.search(r"\\\\[^\\\s]+\\", text)
    assert "AppData" not in text
    assert "futures-v2-repository-audit-" not in text
    assert not re.search(r"\b(?:19|20)\d{2}-\d{2}-\d{2}\b", text)
    assert not re.search(r"\b[0-9a-f]{40}\b", text)
    for variable in ("USERNAME", "USER"):
        username = os.environ.get(variable, "").strip()
        if len(username) >= 3:
            assert not re.search(rf"(?i)\b{re.escape(username)}\b", text)


def test_source_of_truth_contains_cleanup_and_non_authority_rules() -> None:
    text = SOURCE_OF_TRUTH_PATH.read_text(encoding="utf-8")

    assert "The registry grants no deletion authority" in text
    assert "`SOURCE_OF_TRUTH.md` grants no deletion authority" in text
    assert "git clean -fdx" in text
    assert "git clean -fdX" in text
    assert "provider access" in text
    assert "trading" in text
    assert "order placement" in text
    assert "staging, commit, or push" in text


def test_source_of_truth_does_not_promote_untracked_execution_code() -> None:
    text = SOURCE_OF_TRUTH_PATH.read_text(encoding="utf-8")

    assert "live_cockpit/execution" not in text
    assert "tradovate" not in text.lower()
    assert "untracked execution-looking modules are not public commands" in text


def test_source_of_truth_does_not_reproduce_historical_chronology() -> None:
    text = SOURCE_OF_TRUTH_PATH.read_text(encoding="utf-8")

    for fragment in ("Phase 1A", "Phase 11", "V4-V12", "2010 through", "commit history"):
        assert fragment not in text


def test_manual_source_of_truth_edit_is_rejected() -> None:
    surface = _surface()
    edited = SOURCE_OF_TRUTH_PATH.read_bytes().replace(
        b"generated navigation view", b"manually maintained navigation view", 1
    )

    with pytest.raises(RepositorySurfaceError, match="stale|manually edited"):
        validate_source_of_truth(
            edited,
            surface=surface,
            public_commands=_public_commands(),
        )


def test_missing_source_of_truth_file_is_rejected(tmp_path: Path) -> None:
    _write_export_controls(tmp_path, include_document=False)

    with pytest.raises(RepositorySurfaceError, match="missing"):
        compare_source_of_truth_file(_surface(), tmp_path)


def test_changed_public_command_makes_document_stale(tmp_path: Path) -> None:
    _write_export_controls(tmp_path)
    (tmp_path / "src" / "futures_rebuild" / "readiness.py").write_text(
        "def main(): return 0\n", encoding="utf-8"
    )
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "surface-export"\nversion = "1"\n'
        '[project.scripts]\nfutures-readiness = "futures_rebuild.readiness:main"\n',
        encoding="utf-8",
    )

    with pytest.raises(RepositorySurfaceError, match="stale|inconsistent"):
        compare_source_of_truth_file(_surface(), tmp_path)


def test_changed_pointer_role_path_changes_rendering() -> None:
    surface = _surface()
    baseline = render_source_of_truth(surface, _public_commands())
    pointer = _entry(surface, "configs/active_alpha_research_ladder.json")
    pointer["path_or_pattern"] = "configs/active_alpha_successor.json"

    changed = render_source_of_truth(surface, _public_commands())

    assert changed != baseline
    assert "`configs/active_alpha_successor.json`" in changed


def test_clean_export_rendering_needs_no_local_pointer_or_payload(
    tmp_path: Path,
) -> None:
    _write_export_controls(tmp_path)

    report = compare_source_of_truth_file(_surface(), tmp_path)

    assert report["valid"] is True
    assert not (tmp_path / "configs" / "active_micro_alpha_research_ladder.json").exists()
    assert not (tmp_path / "data").exists()


def test_default_cli_reports_all_generated_surface_validity() -> None:
    env = os.environ.copy()
    result = subprocess.run(
        [sys.executable, "-B", "-m", "futures_rebuild.repository_surface"],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        timeout=30,
    )
    report = json.loads(result.stdout)

    assert result.stderr == b""
    assert report["registry_valid"] is True
    assert report["source_of_truth_valid"] is True
    assert report["pipeline_folder_map_valid"] is True
    assert report["active_source_files_valid"] is True
    expected_entry_count = (
        251 if _surface().get("current_direct_authority_registry_id") else 189
    )
    assert report["entry_count"] == expected_entry_count
    assert report["unresolved_entry_count"] == EXPECTED_UNRESOLVED_ENTRY_COUNT == 14
    assert report["public_command_count"] == EXPECTED_PUBLIC_COMMAND_COUNT == 7
    assert report["tracked_root_mode"] == "GIT_LS_FILES"
    assert report["active_source_inventory_mode"] == "GIT_TRACKED_EXACT"
    assert report["mutations_performed"] is False


def test_print_source_of_truth_cli_is_stdout_only_and_read_only() -> None:
    observed = [
        SOURCE_OF_TRUTH_PATH,
        REGISTRY_PATH,
        ROOT / "src" / "futures_rebuild" / "repository_surface.py",
        ROOT / "tests" / "test_repository_surface.py",
    ]
    before = {path: path.read_bytes() for path in observed}
    env = os.environ.copy()

    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "futures_rebuild.repository_surface",
            "--print-source-of-truth",
        ],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        timeout=30,
    )

    assert result.stdout == SOURCE_OF_TRUTH_PATH.read_bytes()
    assert result.stderr == b""
    assert {path: path.read_bytes() for path in observed} == before


def test_validate_repository_checkout_reports_source_of_truth_metrics() -> None:
    report = validate_repository_checkout(ROOT)

    assert report["source_of_truth_valid"] is True
    assert report["source_of_truth_word_count"] == len(
        SOURCE_OF_TRUTH_PATH.read_text(encoding="utf-8").split()
    )
    assert report["source_of_truth_byte_count"] == SOURCE_OF_TRUTH_PATH.stat().st_size
    assert report["pipeline_folder_map_valid"] is True
    assert report["pipeline_folder_map_word_count"] == len(
        PIPELINE_FOLDER_MAP_PATH.read_text(encoding="utf-8").split()
    )
    assert report["pipeline_folder_map_byte_count"] == PIPELINE_FOLDER_MAP_PATH.stat().st_size
    assert report["active_source_files_valid"] is True
    assert report["active_source_total_file_count"] > 0
    assert report["active_source_byte_count"] == ACTIVE_SOURCE_FILES_PATH.stat().st_size


def test_pipeline_folder_map_registry_entry_is_unique_generated_view() -> None:
    surface = _surface()
    entry = _entry(surface, "PIPELINE_FOLDER_MAP.md")

    assert entry["match_type"] == "EXACT"
    assert entry["classification"] == "CURRENT_SUPPORTING"
    assert entry["authority_role"] == PIPELINE_FOLDER_MAP_ROLE
    assert entry["tracked_expected"] == "TRACKED"
    assert entry["local_only"] is False
    assert entry["hash_bound"] is False
    assert entry["deletion_policy"] == "PRESERVE"
    assert sum(
        item["authority_role"] == PIPELINE_FOLDER_MAP_ROLE
        for item in surface["entries"]  # type: ignore[index]
    ) == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("match_type", "PREFIX"),
        ("classification", "CURRENT_OPERATIONAL"),
        ("authority_role", "TOPOLOGY_REFERENCE_NO_AUTHORITY"),
        ("tracked_expected", "OPTIONAL"),
        ("local_only", True),
        ("hash_bound", None),
        ("deletion_policy", "NO_AUTOMATIC_DELETE"),
    ],
)
def test_pipeline_folder_map_registry_contract_fails_closed(
    field: str, value: object
) -> None:
    surface = _surface()
    _entry(surface, "PIPELINE_FOLDER_MAP.md")[field] = value

    with pytest.raises(RepositorySurfaceError):
        validate_repository_surface(surface)


def test_pipeline_folder_map_rendering_is_deterministic_and_matches_tracked_file() -> None:
    surface = _surface()
    first = expected_pipeline_folder_map_bytes(surface, ROOT)
    second = expected_pipeline_folder_map_bytes(copy.deepcopy(surface), ROOT)

    assert first == second == PIPELINE_FOLDER_MAP_PATH.read_bytes()
    assert compare_pipeline_folder_map_file(surface, ROOT)["valid"] is True


def test_pipeline_folder_map_format_sections_and_limits_are_stable() -> None:
    document = PIPELINE_FOLDER_MAP_PATH.read_bytes()
    text = document.decode("utf-8")
    headings = [line[3:] for line in text.splitlines() if line.startswith("## ")]
    table_rows = 0
    lines = text.splitlines()
    for index in range(len(lines) - 1):
        if lines[index].startswith("|") and lines[index + 1].startswith("| ---"):
            cursor = index + 2
            while cursor < len(lines) and lines[cursor].startswith("|"):
                table_rows += 1
                cursor += 1

    assert b"\r" not in document
    assert document.endswith(b"\n") and not document.endswith(b"\n\n")
    expected_headings = list(PIPELINE_FOLDER_MAP_SECTIONS)
    direct_documentation = _surface().get("current_direct_authority_documentation")
    if isinstance(direct_documentation, dict):
        direct_section = direct_documentation.get("pipeline_folder_map_section")
        if isinstance(direct_section, str):
            expected_headings.append(direct_section.splitlines()[0][3:])

    assert headings == expected_headings
    assert len(text.split()) <= PIPELINE_FOLDER_MAP_MAX_WORDS
    assert len(document) <= PIPELINE_FOLDER_MAP_MAX_BYTES
    assert table_rows <= PIPELINE_FOLDER_MAP_MAX_TABLE_ROWS


def test_pipeline_folder_map_authority_rows_are_complete_unique_and_stable() -> None:
    surface = _surface()
    text = PIPELINE_FOLDER_MAP_PATH.read_text(encoding="utf-8")
    positions = [text.index("| Canonical path-role registry |")]

    assert "`configs/repository_surface.json`" in text
    for label, role in PIPELINE_FOLDER_MAP_AUTHORITY_ROLES:
        matches = [
            entry
            for entry in surface["entries"]  # type: ignore[index]
            if entry["authority_role"] == role
        ]
        assert len(matches) == 1
        row_start = f"| {label} | `{matches[0]['path_or_pattern']}` |"
        assert text.count(row_start) == 1
        positions.append(text.index(row_start))
    assert positions == sorted(positions)


def test_pipeline_folder_map_public_commands_are_exact_and_sorted() -> None:
    text = PIPELINE_FOLDER_MAP_PATH.read_text(encoding="utf-8")
    commands = _public_commands()
    expected_rows = [f"| `{name}` | `{target}` |" for name, target in commands.items()]
    rendered_rows = [line for line in text.splitlines() if line in expected_rows]

    assert list(commands) == sorted(commands)
    assert rendered_rows == expected_rows
    assert len(rendered_rows) == len(commands) == EXPECTED_PUBLIC_COMMAND_COUNT


def test_pipeline_folder_map_major_roots_and_classification_counts_are_exact() -> None:
    surface = _surface()
    text = PIPELINE_FOLDER_MAP_PATH.read_text(encoding="utf-8")
    counts = {
        classification: sum(
            entry["classification"] == classification
            for entry in surface["entries"]  # type: ignore[index]
        )
        for classification in surface["allowed_classifications"]  # type: ignore[index]
    }

    for family in MAJOR_FOLDER_PATHS:
        assert text.count(f"| `{family}/` |") == 1
    for classification, count in counts.items():
        assert text.splitlines().count(f"| `{classification}` | {count} |") == 1
    assert counts["UNRESOLVED_MANUAL_REVIEW"] == EXPECTED_UNRESOLVED_ENTRY_COUNT


def test_pipeline_folder_map_has_no_transient_identity_or_second_taxonomy() -> None:
    text = PIPELINE_FOLDER_MAP_PATH.read_text(encoding="utf-8")
    scrubbed = text.replace(
        "docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.md", ""
    ).replace("docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.json", "")

    assert not re.search(r"[A-Za-z]:[\\/]", text)
    assert not re.search(r"\\\\[^\\\s]+\\", text)
    assert "AppData" not in text and "futures-v2-" not in text
    assert not re.search(r"\b(?:19|20)\d{2}-\d{2}-\d{2}\b", scrubbed)
    assert not re.search(r"\b[0-9a-f]{40}\b", text)
    for forbidden in (
        "`CURRENT_REACHABLE`",
        "`SYNTHETIC_ONLY`",
        "`HISTORICAL_ROW_APPROVAL_REQUIRED`",
        "`RETIRED`",
        "Phase 1A",
        "Phase 11",
        "PASS_METADATA_ONLY",
        "live_cockpit/execution",
        "Tradovate",
    ):
        assert forbidden.lower() not in text.lower()


def test_pipeline_folder_map_preserves_historical_and_non_authority_boundaries() -> None:
    text = PIPELINE_FOLDER_MAP_PATH.read_text(encoding="utf-8")

    for required in (
        "docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.md",
        "docs/LEGACY_WORKFLOWS.md",
        "tracked does not imply current",
        "ignored does not imply disposable",
        "FuturesLiveCockpit/` is a mixed packaging source/output surface",
        "untracked execution-looking code is not current",
        "Neither this generated map nor the registry authorizes",
        "fresh exact machine-local census and separate approval",
    ):
        assert required in text


def test_manual_or_stale_pipeline_folder_map_is_rejected() -> None:
    edited = PIPELINE_FOLDER_MAP_PATH.read_bytes().replace(
        b"Generated topology view", b"Manually maintained topology view", 1
    )

    with pytest.raises(RepositorySurfaceError, match="stale|manually edited"):
        validate_pipeline_folder_map(
            edited,
            surface=_surface(),
            public_commands=_public_commands(),
        )


def test_changed_registry_data_changes_pipeline_map_and_rejects_old_bytes() -> None:
    surface = _surface()
    baseline = PIPELINE_FOLDER_MAP_PATH.read_bytes()
    _entry(surface, "dist")["classification"] = "CURRENT_SUPPORTING"
    changed = render_pipeline_folder_map(surface, _public_commands()).encode("utf-8")

    assert changed != baseline
    with pytest.raises(RepositorySurfaceError, match="stale|inconsistent"):
        validate_pipeline_folder_map(
            baseline,
            surface=surface,
            public_commands=_public_commands(),
        )


def test_changed_public_command_changes_pipeline_map_and_rejects_old_bytes() -> None:
    commands = _public_commands()
    changed_commands = dict(commands)
    changed_commands["futures-example"] = "futures_rebuild.example:main"
    changed = render_pipeline_folder_map(_surface(), changed_commands).encode("utf-8")

    assert changed != PIPELINE_FOLDER_MAP_PATH.read_bytes()
    with pytest.raises(RepositorySurfaceError, match="stale|inconsistent"):
        validate_pipeline_folder_map(
            PIPELINE_FOLDER_MAP_PATH.read_bytes(),
            surface=_surface(),
            public_commands=changed_commands,
        )


def test_missing_pipeline_folder_map_file_is_rejected(tmp_path: Path) -> None:
    _write_export_controls(tmp_path, include_pipeline_map=False)

    with pytest.raises(RepositorySurfaceError, match="missing"):
        compare_pipeline_folder_map_file(_surface(), tmp_path)


def test_clean_export_pipeline_rendering_needs_no_local_pointer_or_payload(
    tmp_path: Path,
) -> None:
    _write_export_controls(tmp_path)

    report = compare_pipeline_folder_map_file(_surface(), tmp_path)

    assert report["valid"] is True
    assert not (tmp_path / "configs" / "active_micro_alpha_research_ladder.json").exists()
    assert not (tmp_path / "data").exists()


def test_print_pipeline_folder_map_cli_is_stdout_only_and_read_only() -> None:
    observed = [
        PIPELINE_FOLDER_MAP_PATH,
        SOURCE_OF_TRUTH_PATH,
        REGISTRY_PATH,
        ROOT / "src" / "futures_rebuild" / "repository_surface.py",
        ROOT / "tests" / "test_repository_surface.py",
    ]
    before = {path: path.read_bytes() for path in observed}

    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "futures_rebuild.repository_surface",
            "--print-pipeline-folder-map",
        ],
        cwd=ROOT,
        env=os.environ.copy(),
        check=True,
        capture_output=True,
        timeout=30,
    )

    assert result.stdout == PIPELINE_FOLDER_MAP_PATH.read_bytes()
    assert result.stderr == b""
    assert {path: path.read_bytes() for path in observed} == before


def test_active_source_registry_entry_is_unique_generated_view() -> None:
    surface = _surface()
    entry = _entry(surface, "ACTIVE_SOURCE_FILES.txt")

    assert entry["match_type"] == "EXACT"
    assert entry["classification"] == "CURRENT_SUPPORTING"
    assert entry["authority_role"] == ACTIVE_SOURCE_FILES_ROLE
    assert entry["tracked_expected"] == "TRACKED"
    assert entry["local_only"] is False
    assert entry["hash_bound"] is False
    assert entry["deletion_policy"] == "PRESERVE"
    assert sum(
        item["authority_role"] == ACTIVE_SOURCE_FILES_ROLE
        for item in surface["entries"]  # type: ignore[index]
    ) == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("match_type", "PREFIX"),
        ("classification", "CURRENT_OPERATIONAL"),
        ("authority_role", "HUMAN_SOURCE_OF_TRUTH_VIEW"),
        ("tracked_expected", "OPTIONAL"),
        ("local_only", True),
        ("hash_bound", None),
        ("deletion_policy", "NO_AUTOMATIC_DELETE"),
    ],
)
def test_active_source_registry_contract_fails_closed(
    field: str, value: object
) -> None:
    surface = _surface()
    _entry(surface, "ACTIVE_SOURCE_FILES.txt")[field] = value

    with pytest.raises(RepositorySurfaceError):
        validate_repository_surface(surface)


def test_git_backed_tracked_inventory_collection_is_binary_safe(tmp_path: Path) -> None:
    subprocess.run(
        ["git", "-c", "core.longpaths=true", "init", "-q"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "alpha file.py").write_text("pass\n", encoding="utf-8")
    (tmp_path / "README.md").write_text("read me\n", encoding="utf-8")
    subprocess.run(
        ["git", "add", "--", "README.md", "src/alpha file.py"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )

    assert collect_tracked_repository_paths(tmp_path) == [
        "README.md",
        "src/alpha file.py",
    ]


def test_active_source_rendering_is_deterministic_complete_and_exact() -> None:
    surface = _surface()
    tracked = collect_tracked_repository_paths(ROOT)
    first = expected_active_source_files_bytes(surface, ROOT)
    second = render_active_source_files(
        copy.deepcopy(surface), tracked
    ).encode("utf-8")
    report = compare_active_source_files_file(surface, ROOT)

    assert first == second == ACTIVE_SOURCE_FILES_PATH.read_bytes()
    assert report["valid"] is True
    assert report["inventory_mode"] == "GIT_TRACKED_EXACT"
    assert report["completeness_reconstructed"] is True
    assert report["operational_file_count"] > 0
    assert report["supporting_file_count"] > 0
    assert report["total_file_count"] == (
        report["operational_file_count"] + report["supporting_file_count"]
    )


def test_active_source_validation_accepts_pending_generated_self_path() -> None:
    surface = _surface()
    document = ACTIVE_SOURCE_FILES_PATH.read_bytes()
    pending_self = ACTIVE_SOURCE_FILES_PATH.relative_to(ROOT).as_posix()
    listed = [
        line
        for line in document.decode("utf-8").splitlines()
        if line and not line.startswith("#")
    ]
    supplied_tracked_paths = [path for path in listed if path != pending_self]

    assert listed.count(pending_self) == 1
    assert pending_self not in supplied_tracked_paths
    assert len(supplied_tracked_paths) == len(listed) - 1
    assert set(listed) - set(supplied_tracked_paths) == {pending_self}

    report = validate_active_source_files(
        document,
        surface=surface,
        repository_root=ROOT,
        tracked_paths=supplied_tracked_paths,
    )
    classified = active_source_paths(
        surface, [*supplied_tracked_paths, pending_self]
    )
    validated_paths = [
        path
        for classification in ACTIVE_SOURCE_CLASSIFICATIONS
        for path in classified[classification]
    ]
    self_entry = _entry(surface, pending_self)

    assert report["valid"] is True
    assert report["inventory_mode"] == "SUPPLIED_TRACKED_EXACT"
    assert report["completeness_reconstructed"] is True
    assert validated_paths.count(pending_self) == 1
    assert report["operational_file_count"] == len(
        classified["CURRENT_OPERATIONAL"]
    )
    assert report["supporting_file_count"] == len(
        classified["CURRENT_SUPPORTING"]
    )
    assert report["total_file_count"] == len(validated_paths) == len(listed)
    assert report["total_file_count"] == (
        report["operational_file_count"] + report["supporting_file_count"]
    )
    assert resolve_surface_entry(surface, pending_self) == self_entry
    assert {
        "match_type": self_entry["match_type"],
        "classification": self_entry["classification"],
        "authority_role": self_entry["authority_role"],
        "tracked_expected": self_entry["tracked_expected"],
    } == {
        "match_type": "EXACT",
        "classification": "CURRENT_SUPPORTING",
        "authority_role": ACTIVE_SOURCE_FILES_ROLE,
        "tracked_expected": "TRACKED",
    }

    with pytest.raises(
        RepositorySurfaceError,
        match=(
            r"active-source inventory contains a duplicate: "
            r"ACTIVE_SOURCE_FILES\.txt"
        ),
    ):
        validate_active_source_files(
            document,
            surface=surface,
            repository_root=ROOT,
            tracked_paths=[
                *supplied_tracked_paths,
                pending_self,
                pending_self,
            ],
        )


def test_active_source_format_sections_ordering_and_limits_are_exact() -> None:
    document = ACTIVE_SOURCE_FILES_PATH.read_bytes()
    text = document.decode("utf-8")
    lines = text.splitlines()
    operational_index = lines.index("# CURRENT_OPERATIONAL")
    supporting_index = lines.index("# CURRENT_SUPPORTING")
    operational = lines[operational_index + 1 : supporting_index - 1]
    supporting = lines[supporting_index + 1 :]

    assert b"\r" not in document
    assert document.endswith(b"\n") and not document.endswith(b"\n\n")
    assert len(document) <= ACTIVE_SOURCE_FILES_MAX_BYTES
    assert operational and supporting
    assert operational == sorted(operational)
    assert supporting == sorted(supporting)
    assert len(operational + supporting) == len(set(operational + supporting))
    assert all("\\" not in path and not Path(path).is_absolute() for path in operational + supporting)


def test_active_source_includes_governing_generated_and_public_targets() -> None:
    listed = set(
        line
        for line in ACTIVE_SOURCE_FILES_PATH.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    )
    required = {
        "AGENTS.md",
        "CURRENT_WORKFLOW.md",
        "README.md",
        "SOURCE_OF_TRUTH.md",
        "PIPELINE_FOLDER_MAP.md",
        "ACTIVE_SOURCE_FILES.txt",
        "configs/repository_surface.json",
        "pyproject.toml",
        ".github/workflows/ci.yml",
        "src/futures_rebuild/repository_surface.py",
        *validate_public_command_surfaces(_surface(), ROOT).values(),
    }

    assert required <= listed


def test_active_source_excludes_every_tracked_noncurrent_classification() -> None:
    surface = _surface()
    tracked = collect_tracked_repository_paths(ROOT)
    listed = {
        line
        for line in ACTIVE_SOURCE_FILES_PATH.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    }
    excluded = {
        path
        for path in tracked
        if resolve_surface_entry(surface, path)["classification"]  # type: ignore[index]
        not in ACTIVE_SOURCE_CLASSIFICATIONS
    }
    excluded_classifications = {
        resolve_surface_entry(surface, path)["classification"]  # type: ignore[index]
        for path in excluded
    }

    assert excluded
    assert {
        "HISTORICAL_HASH_BOUND",
        "HISTORICAL_UNBOUND",
        "PREPARED_NOT_EXECUTED",
        "GENERATED_OUTPUT",
        "LOCAL_RUNTIME_STATE",
        "UNRESOLVED_MANUAL_REVIEW",
    } <= excluded_classifications
    assert "LOCAL_SECRET" not in excluded_classifications
    synthetic_noncurrent = active_source_paths(
        surface,
        [
            "src/futures_rebuild/__pycache__/example.pyc",
            "FuturesLiveCockpit",
        ],
    )
    assert synthetic_noncurrent == {
        "CURRENT_OPERATIONAL": [],
        "CURRENT_SUPPORTING": [],
    }
    assert listed.isdisjoint(excluded)
    assert "docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.md" not in listed
    assert "docs/history/PROJECT_OUTLINE_SNAPSHOT_2026-08-11.md" not in listed
    assert "src/futures_rebuild/live_cockpit/offline_network.py" in listed
    assert "configs/active_micro_alpha_research_ladder.json" not in listed


@pytest.mark.parametrize(
    "mutator",
    [
        lambda text: text.replace("AGENTS.md\n", "AGENTS.md\nAGENTS.md\n", 1),
        lambda text: text.replace("AGENTS.md\nCURRENT_WORKFLOW.md", "CURRENT_WORKFLOW.md\nAGENTS.md", 1),
        lambda text: text.replace("AGENTS.md", "C:/outside/AGENTS.md", 1),
        lambda text: text.replace("AGENTS.md", "outside\\AGENTS.md", 1),
        lambda text: text.replace("AGENTS.md", "../AGENTS.md", 1),
        lambda text: text.replace("# Virtual view only", "# Manually curated view", 1),
    ],
)
def test_active_source_rejects_duplicate_unsorted_unsafe_or_manual_edits(
    mutator: object,
) -> None:
    edited = mutator(ACTIVE_SOURCE_FILES_PATH.read_text(encoding="utf-8")).encode(  # type: ignore[operator]
        "utf-8"
    )

    with pytest.raises(RepositorySurfaceError):
        validate_active_source_files(
            edited,
            surface=_surface(),
            repository_root=ROOT,
        )


def test_active_source_changes_with_registry_and_tracked_inventory() -> None:
    surface = _surface()
    tracked = collect_tracked_repository_paths(ROOT)
    baseline = ACTIVE_SOURCE_FILES_PATH.read_bytes()

    changed_surface = copy.deepcopy(surface)
    _entry(changed_surface, "README.md")["classification"] = "HISTORICAL_UNBOUND"
    assert render_active_source_files(
        changed_surface, tracked
    ).encode("utf-8") != baseline
    with pytest.raises(RepositorySurfaceError):
        validate_active_source_files(
            baseline,
            surface=changed_surface,
            repository_root=ROOT,
        )

    added = render_active_source_files(
        surface, [*tracked, "tests/new_active_source_test.py"]
    )
    removed = render_active_source_files(
        surface,
        [path for path in tracked if path != "README.md"],
    )
    assert "tests/new_active_source_test.py" in added
    assert "README.md" not in removed.splitlines()


def test_missing_active_source_file_is_rejected(tmp_path: Path) -> None:
    _write_export_controls(tmp_path, include_active_source_files=False)

    with pytest.raises(RepositorySurfaceError, match="missing"):
        compare_active_source_files_file(_surface(), tmp_path)


def test_no_git_export_uses_explicit_subset_mode_and_tolerates_absence(
    tmp_path: Path,
) -> None:
    _write_export_controls(tmp_path)

    report = compare_active_source_files_file(_surface(), tmp_path)

    assert report["valid"] is True
    assert report["inventory_mode"] == "PRESENT_EXPORT_SUBSET"
    assert report["completeness_reconstructed"] is False
    assert not (tmp_path / ".git").exists()
    assert not (tmp_path / "README.md").exists()


def test_no_git_export_rejects_present_active_file_missing_from_view(
    tmp_path: Path,
) -> None:
    _write_export_controls(tmp_path)
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "new_present.py").write_text("pass\n", encoding="utf-8")

    with pytest.raises(RepositorySurfaceError, match="classification mismatch|present export active files"):
        compare_active_source_files_file(_surface(), tmp_path)


def test_changed_public_command_target_must_be_in_active_source_view(
    tmp_path: Path,
) -> None:
    _write_export_controls(tmp_path)
    (tmp_path / "src" / "futures_rebuild" / "extra.py").write_text(
        "def main(): return 0\n", encoding="utf-8"
    )
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname = "surface-export"\nversion = "1"\n'
        '[project.scripts]\nfutures-pipeline = "futures_rebuild.pipeline:main"\n'
        'futures-extra = "futures_rebuild.extra:main"\n',
        encoding="utf-8",
    )

    with pytest.raises(
        RepositorySurfaceError,
        match="targets non-current surface|present export active files|public command targets",
    ):
        compare_active_source_files_file(_surface(), tmp_path)


def test_print_active_source_files_cli_is_stdout_only_and_read_only() -> None:
    observed = [
        ACTIVE_SOURCE_FILES_PATH,
        PIPELINE_FOLDER_MAP_PATH,
        SOURCE_OF_TRUTH_PATH,
        REGISTRY_PATH,
        ROOT / "src" / "futures_rebuild" / "repository_surface.py",
        ROOT / "tests" / "test_repository_surface.py",
    ]
    before = {path: path.read_bytes() for path in observed}

    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "futures_rebuild.repository_surface",
            "--print-active-source-files",
        ],
        cwd=ROOT,
        env=os.environ.copy(),
        check=True,
        capture_output=True,
        timeout=30,
    )

    assert result.stdout == ACTIVE_SOURCE_FILES_PATH.read_bytes()
    assert result.stderr == b""
    assert {path: path.read_bytes() for path in observed} == before


def test_phase4a_pipeline_map_and_source_of_truth_generated_views_are_exact() -> None:
    surface = _surface()
    pipeline = PIPELINE_FOLDER_MAP_PATH.read_text(encoding="utf-8")

    assert expected_pipeline_folder_map_bytes(surface, ROOT) == PIPELINE_FOLDER_MAP_PATH.read_bytes()
    assert expected_source_of_truth_bytes(surface, ROOT) == SOURCE_OF_TRUTH_PATH.read_bytes()
    assert "Generated active-source-files view" in pipeline
    assert "`ACTIVE_SOURCE_FILES.txt`" in pipeline
    assert "| `CURRENT_SUPPORTING` |" in pipeline
