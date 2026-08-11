from __future__ import annotations

from pathlib import Path
import re

import pytest

from futures_rebuild.closure_workflow.engine import validate_transition_plan
from futures_rebuild.closure_workflow.policy import WorkflowError, load_policy


REPO = Path(__file__).resolve().parents[1]


def _markdown_section(document: str, heading: str) -> str:
    marker = f"## {heading}"
    assert marker in document
    return document.split(marker, 1)[1].split("\n## ", 1)[0]


def _normalized(document: str) -> str:
    return " ".join(document.replace("`", "").lower().split())


def test_two_tier_policy_allows_normal_local_work_without_approval_artifacts() -> None:
    policy = load_policy(REPO)
    assert policy["schema_version"] == "two_tier_workflow_policy/2.0.0"
    assert policy["normal_local_work"]["autonomous"] is True
    assert policy["normal_local_work"]["actions"] == [
        "repository_inspection",
        "local_code_and_document_edits",
        "tests",
        "non_research_generated_artifacts",
        "explicit_path_staging",
        "local_commit",
    ]
    assert policy["legacy_closure_workflow"]["new_plan_generation"] is False
    assert policy["legacy_closure_workflow"]["new_hash_bound_approval"] is False


def test_high_risk_work_requires_one_plain_language_confirmation() -> None:
    policy = load_policy(REPO)
    high_risk = policy["high_risk_work"]
    assert high_risk["requires_plain_language_confirmation"] is True
    assert high_risk["confirmation_contents"] == [
        "scope",
        "impact_or_cost",
        "outputs",
        "rollback_or_preservation",
    ]
    assert set(high_risk["actions"]) == {
        "provider_or_network_access",
        "real_data_read_or_evaluation",
        "data_publication_or_active_data_mutation",
        "installation_or_activation",
        "deletion_or_cutover",
        "holdout_or_forward_access",
        "trading_or_order_path",
        "remote_push",
    }


def test_legacy_closure_engine_rejects_new_plan_execution() -> None:
    plan = {
        "schema_version": "closure_transition_plan/2.0.0",
        "authority_class": "AUTONOMOUS_READ_ONLY",
        "actions": [{"id": "reconcile", "type": "reconciliation"}],
    }
    with pytest.raises(WorkflowError, match="retired for new work"):
        validate_transition_plan(plan, load_policy(REPO))


def test_governing_documents_do_not_require_copied_approval_lines() -> None:
    agents = (REPO / "AGENTS.md").read_text(encoding="utf-8")
    current = (REPO / "CURRENT_WORKFLOW.md").read_text(encoding="utf-8")
    outline = (REPO / "PROJECT_OUTLINE.md").read_text(encoding="utf-8")
    handoff = (REPO / "CODEX_HANDOFF.md").read_text(encoding="utf-8")
    assert "Do not ask the user to copy a plan ID, hash, command, approval line" in agents
    assert "two-tier workflow" in outline

    normal_work = _normalized(_markdown_section(current, "Normal local work"))
    assert re.search(r"\bdo(?:es)? not (?:need|require) to copy\b", normal_work)
    assert {"commands", "hashes", "plan", "ids", "approval", "lines"} <= set(
        re.findall(r"[a-z0-9]+", normal_work)
    )
    assert not re.search(r"\b(?:must|shall) copy\b|\brequired to copy\b", normal_work)

    handoff_intro = _normalized(handoff.split("\n## ", 1)[0])
    assert "current_workflow.md" in handoff_intro
    assert re.search(r"\bcontrols? normal work\b", handoff_intro)
    assert re.search(r"\bgrants? no authority\b", handoff_intro)
