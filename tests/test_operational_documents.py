from pathlib import Path
import subprocess

import json


ROOT = Path(__file__).resolve().parents[1]


def _text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _assert_public_snapshot_is_historical() -> None:
    snapshot = _text("PUBLIC_SNAPSHOT.md")
    lowered = " ".join(snapshot.split()).lower()

    for required in (
        "Historical public source snapshot record",
        "e9363688873d90af41c998054d4b219f5e950f0e",
        "2026-07-25",
        "sanitized public source export",
        "does not describe the current operational checkout",
        "files omitted from this historical snapshot",
        "not evidence that any named file is absent from the current checkout",
        "The snapshot omitted:",
        "CURRENT_WORKFLOW.md",
        "AGENTS.md",
        "SOURCE_OF_TRUTH.md",
        "does not authorize",
        "not the complete current operational test command",
        "not a Master Audit, Meta Audit",
        "model-trust result",
        "provider authorization",
        "trading-readiness claim",
    ):
        assert required.lower() in lowered

    for required_non_authority in (
        "provider access",
        "market-data reads",
        "real-history evaluation",
        "prediction materialization",
        "candidate sealing",
        "holdout access",
        "publication",
        "installation",
        "activation",
        "live smoke",
        "trading",
        "order placement",
        "deletion",
        "movement or renaming",
        "staging",
        "commit",
        "push",
    ):
        assert required_non_authority in lowered

    for misleading in (
        "This repository is a sanitized source snapshot",
        "The current repository omits CODEX_HANDOFF.md",
        "The operational checkout omits all mutable continuation state",
        "This document defines current workflow",
        "The snapshot commit is the current HEAD",
    ):
        assert misleading.lower() not in lowered


def test_current_documents_use_one_plain_language_workflow_surface() -> None:
    agents = _text("AGENTS.md")
    readme = _text("README.md")
    outline = _text("PROJECT_OUTLINE.md")
    current = _text("CURRENT_WORKFLOW.md")
    combined = "\n".join((agents, readme, outline, current))
    for required in (
        "CURRENT_WORKFLOW.md",
        "plain-language",
        "Normal local work",
        "High-risk work",
        "real-data",
        "remote push",
    ):
        assert required.lower() in combined.lower()
    assert "--approval-line" not in combined
    assert "futures-live-cockpit-workflow" not in combined
    assert "futures-closure-workflow" not in combined
    assert "this guide controls normal-work procedure" in current.lower()
    _assert_public_snapshot_is_historical()


def test_public_snapshot_is_an_explicit_historical_record() -> None:
    _assert_public_snapshot_is_historical()


def test_handoff_describes_the_active_alpha_ladder_and_next_boundary(
    local_evidence_root: Path,
) -> None:
    handoff = (local_evidence_root / "CODEX_HANDOFF.md").read_text(encoding="utf-8")

    for required in (
        "53252c8d...362815",
        "cfefe8ce...563dc3",
        "CertifiedResearchGateway",
        "a6ae7b...c82bc",
        "SEALED_UNPUBLISHED_ECONOMIC_SCREEN_COMPLETE",
        "aeff50fa...23ff9",
        "CONCLUSIVE_PILOT_ECONOMIC_REJECTION_ZERO_TRADABLE_SIGNALS",
        "Tier 1 advancement is forbidden",
        "26bbde28...4e71",
        "c82f91b...b902d",
        "7bbaefec...9defd",
        "R and the emergency reserve",
        "remain\nunset",
        "synthetic row-loader injection hook",
    ):
        assert required.lower() in handoff.lower()
    assert "codex/tier1-phase8-economics" not in handoff
    assert "103 and 852" not in handoff


def test_current_workflow_names_one_certified_real_history_surface() -> None:
    current = _text("CURRENT_WORKFLOW.md")
    legacy = _text("docs/LEGACY_WORKFLOWS.md")
    assert "The only current code surface" in current
    assert "CertifiedResearchGateway" in current
    assert "shared receipt boundary rejects" in current
    assert "V4-V12" in legacy
    assert "registration through it is disabled" in legacy


def test_current_workflow_exposes_only_generic_prop_firm_preparation() -> None:
    current = _text("CURRENT_WORKFLOW.md")
    for command in (
        "futures_rebuild.pipeline prop-firm-risk-policy",
        "futures_rebuild.pipeline prop-firm-phase8",
    ):
        assert command in current
    assert "deterministic, non-authorizing preparation records" in current


def test_agents_requires_a_value_case_for_new_policy_controls() -> None:
    agents = _text("AGENTS.md")
    for required in ("risk it prevents", "decision it improves", "simpler rule"):
        assert required in agents


def test_legacy_registry_lists_retired_surface_and_preservation_rule() -> None:
    legacy = _text("docs/LEGACY_WORKFLOWS.md")
    for required in (
        "active_data_full_successor_v11_3.py",
        "closure engine",
        "byte-for-byte",
        "Force-adding",
    ):
        assert required.lower() in legacy.lower()


def test_root_git_hygiene_declares_and_hides_legacy_evidence_paths() -> None:
    ignore = _text(".gitignore").splitlines()
    for ignored in (
        "FuturesLiveCockpit.backup-*/",
        "artifacts/flcp/",
        "data/active/",
        "manifests/workflow/closure/",
        "reports/workflow/closure/",
        "src/futures_rebuild/active_data_*successor*.py",
    ):
        assert ignored in ignore
    legacy_relative = "src/futures_rebuild/active_data_full_successor_v11_3.py"
    if (ROOT / ".git").exists():
        assert subprocess.run(
            ["git", "check-ignore", "-q", "--", legacy_relative], cwd=ROOT, check=False
        ).returncode == 0
        assert subprocess.run(
            ["git", "ls-files", "--error-unmatch", "--", legacy_relative],
            cwd=ROOT, check=False, capture_output=True, text=True,
        ).returncode != 0
    else:
        assert not (ROOT / legacy_relative).exists()


def test_public_scripts_expose_no_token_era_high_risk_runner() -> None:
    scripts = _text("pyproject.toml")
    for retired in (
        "futures-calendar",
        "futures-active-view",
        "futures-live-cockpit",
        "futures-foundation-calendar-successor",
        "futures-closure-workflow",
    ):
        assert retired not in scripts
    assert "futures-high-risk-prepare" in scripts


def test_pipeline_map_names_only_the_current_real_history_gateway() -> None:
    mapping = _text("PIPELINE_FOLDER_MAP.md")
    assert "CertifiedResearchGateway" in mapping
    assert "No other public script" in mapping
    assert "retired" in mapping.lower()
    assert "local_evidence" in mapping


def test_micro_pipeline_map_distinguishes_design_from_implementation() -> None:
    mapping = _text("PIPELINE_FOLDER_MAP.md")
    outline = _text("PROJECT_OUTLINE.md")
    for classification in (
        "CURRENT_REACHABLE",
        "PREPARED_NOT_EXECUTED",
            "SYNTHETIC_ONLY",
            "HISTORICAL_ROW_APPROVAL_REQUIRED",
            "RETIRED",
    ):
        assert classification in mapping
    for historical_path in (
        "src/futures_rebuild/micro_alpha_pipeline.py",
        "src/futures_rebuild/micro_alpha_acquisition.py",
        "scripts/prepare_apex_micro_infrastructure.py",
        "configs/apex_micro_tier01_databento_metadata_preflight_v2.json",
        "configs/apex_micro_tier01_databento_metadata_preflight_v4.json",
        "configs/apex_micro_product_reference_requirements.json",
        "state/unpublished_evidence/apex_micro_preparation_supersessions/micro_tier1_scope_reconciliation.json",
    ):
        assert historical_path in mapping or historical_path in outline
    assert "machine-local" in mapping.lower()
    assert "local_evidence" in mapping
    assert "legacy micro source catalog" in mapping
    assert "configs/micro_futures_catalog_migration_plan_v1.json" in mapping
    assert "PASS_METADATA_ONLY" in mapping


def test_micro_preflight_is_metadata_only_and_download_has_no_public_command() -> None:
    obsolete = json.loads(
        (ROOT / "configs/apex_micro_tier01_databento_preflight_plan.json").read_text(
            encoding="utf-8"
        )
    )
    assert obsolete["plan_id"] == "c9bf6a86a9ca501cc4682ed10e63bf8cc984bfd27c3c44d35097e0aeeeba2ecc"
    plan = json.loads(
        (ROOT / "configs/apex_micro_tier01_databento_metadata_preflight_v2.json").read_text(
            encoding="utf-8"
        )
    )
    assert plan["state"] == "PREPARED_NOT_EXECUTED"
    assert {request["market"] for request in plan["requests"]} == {"MES", "MCL", "MGC", "M6E"}
    assert plan["limits"]["exact_provider_call_ceiling"] == 51
    assert plan["limits"]["maximum_external_cost_usd"] == "0"
    assert plan["forbidden"]["timeseries_download"] is True
    assert plan["forbidden"]["data_dbn_write"] is True
    report = json.loads(
        (
            ROOT
            / "state/unpublished_evidence/apex_micro_metadata_preflight_v2/report.json"
        ).read_text(encoding="utf-8")
    )
    assert report["state"] == "FAIL_CLOSED_METADATA_ONLY"
    assert report["exception_type"] == "ReadTimeout"
    assert report["provider_call_counts"] == {
        "list_datasets": 1,
        "list_schemas": 1,
    }
    assert report["external_cost_incurred_usd"] == "0"
    assert report["automatic_retries"] == 0
    assert report["timeseries_download_calls"] == 0
    invalid_preparation = json.loads(
        (
            ROOT
            / "configs/apex_micro_tier01_databento_metadata_preflight_v3.json"
        ).read_text(encoding="utf-8")
    )
    supersession = json.loads(
        (
            ROOT
            / "state/unpublished_evidence/apex_micro_metadata_preflight_v3_supersession.json"
        ).read_text(encoding="utf-8")
    )
    assert supersession["classification"] == "SUPERSEDED_LOCAL_PREPARATION"
    assert supersession["plan_id"] == invalid_preparation["plan_id"]
    assert supersession["provider_access_performed"] is False
    assert supersession["execution_forbidden"] is True
    successor = json.loads(
        (
            ROOT
            / "configs/apex_micro_tier01_databento_metadata_preflight_v4.json"
        ).read_text(encoding="utf-8")
    )
    assert successor["state"] == "PREPARED_NOT_EXECUTED"
    assert successor["predecessor_execution"]["report_id"] == report["report_id"]
    assert successor["correction"]["scope_change"] == (
        "TIMEOUT_ONLY_NO_MARKET_SCHEMA_OR_ENDPOINT_CHANGE"
    )
    assert successor["limits"]["per_call_timeout_seconds"] == 30
    assert successor["limits"]["maximum_runtime_seconds"] == 300
    assert successor["forbidden"]["timeseries_download"] is True
    pyproject = _text("pyproject.toml")
    assert "futures-pipeline = \"futures_rebuild.pipeline:main\"" in pyproject
    assert "apex-micro-download" not in pyproject
