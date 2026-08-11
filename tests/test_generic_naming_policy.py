import json
import re
import subprocess
from hashlib import sha256
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[1]


def _git_paths(*args: str) -> tuple[str, ...]:
    if not (ROOT / ".git").exists():
        if "--others" in args:
            return ()
        return tuple(
            sorted(
                path.relative_to(ROOT).as_posix()
                for path in ROOT.rglob("*")
                if path.is_file()
                and ".venv" not in path.parts
                and "__pycache__" not in path.parts
                and path.suffix != ".pyc"
                and not any(part.startswith(".") for part in path.relative_to(ROOT).parts)
            )
        )
    output = subprocess.check_output(
        ["git", "-C", str(ROOT), *args], text=True
    )
    return tuple(
        normalized
        for line in output.splitlines()
        if line.strip()
        for normalized in (line.strip().replace("\\", "/"),)
        if not normalized.startswith((".clean-checkout-sim/", ".pytest-"))
    )


def _is_preserved_apex_path(path: str) -> bool:
    return (
        path.startswith("configs/apex_micro_")
        or path.startswith("scripts/prepare_apex_micro_")
        or path.startswith("state/unpublished_evidence/apex_micro_")
        or path
        in {
            "configs/apex_tradovate_50k_eod_risk_policy.json",
            "reports/apex_tradovate_50k_eod_risk_policy.md",
            "src/futures_rebuild/apex_tradovate_eod_risk.py",
            "tests/test_apex_tradovate_eod_risk.py",
        }
    )


def _apex_filesystem_class(path: str) -> str | None:
    if (
        path.startswith("configs/apex_micro_")
        or path.startswith("scripts/prepare_apex_micro_")
        or path.startswith("state/unpublished_evidence/apex_micro_")
    ):
        return "immutable_micro_lineage"
    if path.startswith("state/data_publication_staging/apex_integer_micro_11") or path.startswith(
        "state/data_publication_staging/apex_micro_phase2_diagnostic_v1"
    ):
        return "completed_publication_staging_lineage"
    if path.startswith("state/provider_acquisition_staging/apex_micro_"):
        return "historical_provider_staging_lineage"
    if "/__pycache__/" in f"/{path}" and path.endswith(".pyc"):
        return "ignored_legacy_bytecode"
    if path in {
        "configs/apex_tradovate_50k_eod_risk_policy.json",
        "reports/apex_tradovate_50k_eod_risk_policy.md",
        "src/futures_rebuild/apex_tradovate_eod_risk.py",
        "tests/test_apex_tradovate_eod_risk.py",
    }:
        return "hash_bound_prop_firm_lineage"
    if path == "data/active/catalogs/apex_micro.json":
        return "active_legacy_catalog_cutover_required"
    return None


def test_every_apex_named_git_path_is_explicit_legacy_lineage() -> None:
    paths = _git_paths("ls-files") + _git_paths("ls-files", "--others", "--exclude-standard")
    apex_paths = tuple(path for path in paths if "apex" in path.lower())
    assert apex_paths
    assert all(_is_preserved_apex_path(path) for path in apex_paths)


def test_every_apex_named_scoped_filesystem_path_is_classified() -> None:
    roots = (
        "configs",
        "src",
        "tests",
        "scripts",
        "reports",
        "docs",
        "state",
        "data/active/catalogs",
    )
    apex_paths = tuple(
        sorted(
            path.relative_to(ROOT).as_posix()
            for root in roots
            for path in (ROOT / root).rglob("*")
            if "apex" in path.relative_to(ROOT).as_posix().lower()
        )
    )
    assert apex_paths
    assert all(_apex_filesystem_class(path) is not None for path in apex_paths)


def test_operational_successor_paths_are_generic() -> None:
    expected = {
        "configs/prop_firm_profiles.json",
        "configs/prop_firm_phase8_evaluation.json",
        "configs/prop_firm_strategy_risk_policies.json",
        "configs/prop_firm_execution_instruments.json",
        "configs/prop_firm_execution_costs.json",
        "configs/prop_firm_payout_policies.json",
        "configs/micro_futures_legacy_lineage.json",
        "reports/prop_firm_eod_risk_policy.md",
        "scripts/prepare_micro_futures_catalog_migration_v1.py",
        "src/futures_rebuild/micro_futures_catalog_migration.py",
        "src/futures_rebuild/prop_firm_eod_risk.py",
        "src/futures_rebuild/prop_firm_account_runtime.py",
        "src/futures_rebuild/prop_firm_phase8.py",
        "tests/test_micro_futures_catalog_migration.py",
        "tests/test_prop_firm_eod_risk.py",
        "tests/test_prop_firm_account_runtime.py",
        "tests/test_prop_firm_phase8.py",
    }
    assert all((ROOT / path).is_file() for path in expected)
    assert all("apex" not in path.lower() for path in expected)


def test_generic_cutover_plan_writes_no_apex_named_successor() -> None:
    plan = json.loads(
        (ROOT / "configs/micro_futures_catalog_migration_plan_v1.json").read_text(
            encoding="utf-8"
        )
    )
    successor = plan["proposed_successor"]
    prospective_paths = (
        successor["catalog_path"],
        successor["pointer_path"],
        successor["publication_lock"],
        successor["failure_parent"],
        successor["evidence_parent"],
    )
    assert all("apex" not in path.lower() for path in prospective_paths)
    assert successor["lane_id"] == "micro_futures_integer_11"
    assert plan["authority"]["active_catalog_write"] is False
    assert plan["authority"]["active_pointer_write"] is False


def test_provider_neutral_runtime_contains_no_apex_interface_name() -> None:
    sources = tuple(
        (ROOT / path).read_text(encoding="utf-8")
        for path in (
            "src/futures_rebuild/prop_firm_eod_risk.py",
            "src/futures_rebuild/prop_firm_phase8.py",
            "configs/prop_firm_phase8_evaluation.json",
        )
    )
    assert all("apex" not in source.lower() for source in sources)
    assert "current_firm_threshold_usd" in sources[0]
    assert "exact_live_costs_verified" in "".join(sources)
    assert "exact_apex_live_costs_verified" not in "".join(sources)


def test_public_runtime_reaches_only_generic_prop_firm_interfaces() -> None:
    pipeline = (ROOT / "src/futures_rebuild/pipeline.py").read_text(encoding="utf-8")
    workflow = (ROOT / "CURRENT_WORKFLOW.md").read_text(encoding="utf-8")
    assert "from .prop_firm_eod_risk import" in pipeline
    assert "from .prop_firm_phase8 import" in pipeline
    assert "prop-firm-risk-policy" in pipeline
    assert "prop-firm-phase8" in pipeline
    assert "apex" not in pipeline.lower()
    assert "prepare_apex" not in workflow.lower()


def test_all_preserved_apex_operation_ids_are_classified_as_legacy() -> None:
    lineage = json.loads(
        (ROOT / "configs/micro_futures_legacy_lineage.json").read_text(
            encoding="utf-8"
        )
    )
    policy = (ROOT / "src/futures_rebuild/research_gateway_policy.py").read_text(
        encoding="utf-8"
    )
    observed = set(re.findall(r'"([A-Z0-9_]*APEX[A-Z0-9_]*)"', policy))
    assert observed == set(lineage["retired_operation_ids"])
    assert lineage["state"] == "IMMUTABLE_LEGACY_LINEAGE_NOT_CURRENT_AUTHORITY"
    assert all(value is False for value in lineage["authority"].values())
    assert lineage["generic_successor"]["future_operation_namespace"] == (
        "MICRO_FUTURES"
    )


@pytest.mark.local_evidence
def test_legacy_lineage_bindings_still_match_exact_bytes(local_evidence_root: Path) -> None:
    lineage = json.loads(
        (ROOT / "configs/micro_futures_legacy_lineage.json").read_text(
            encoding="utf-8"
        )
    )
    for relative, expected in lineage["lineage_bindings"].items():
        assert sha256((local_evidence_root / relative).read_bytes()).hexdigest() == expected


def test_packaged_public_command_surface_has_no_provider_brand() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    pipeline = (ROOT / "src/futures_rebuild/pipeline.py").read_text(encoding="utf-8")
    assert "apex" not in pyproject.lower()
    assert "apex" not in pipeline.lower()


def test_legacy_families_and_cutover_boundary_are_documented() -> None:
    naming = (ROOT / "docs/NAMING_AND_LINEAGE.md").read_text(encoding="utf-8")
    legacy = (ROOT / "docs/LEGACY_WORKFLOWS.md").read_text(encoding="utf-8")
    for text in (naming, legacy):
        assert "configs/apex_micro_*" in text
        assert "scripts/prepare_apex_micro_*" in text
        assert "active-data" in text
    assert "micro_futures_*" in naming
    assert "prop_firm_*" in naming
