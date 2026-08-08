from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def _text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


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


def test_handoff_describes_the_current_phase8_chain_and_valid_rejection() -> None:
    handoff = _text("CODEX_HANDOFF.md")

    for required in (
        "efb8943f...638d5",
        "a9656ec5...ff7d",
        "5b01056d...192fa4",
        "c18ef7e9...b4a94",
        "42e1f97c...30a80",
        "valid rejection, not an invalid retirement",
        "NO_ACTIVE_TRIAL_VALID_REJECTION",
        "There is no active Tier 1 trial",
    ):
        assert required.lower() in handoff.lower()
    assert "all-market audit must finish" not in handoff
    assert "registered bracket trial is bound to superseded index" not in handoff


def test_current_workflow_names_one_certified_real_history_surface() -> None:
    current = _text("CURRENT_WORKFLOW.md")
    legacy = _text("docs/LEGACY_WORKFLOWS.md")
    assert "The only current code surface" in current
    assert "CertifiedResearchGateway" in current
    assert "shared receipt boundary rejects" in current
    assert "V4-V12" in legacy
    assert "registration through it is disabled" in legacy


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
    result = subprocess.run(
        ["git", "check-ignore", "-q", "--", legacy_relative],
        cwd=ROOT,
        check=False,
    )
    assert result.returncode == 0
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", legacy_relative],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert tracked.returncode != 0


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
