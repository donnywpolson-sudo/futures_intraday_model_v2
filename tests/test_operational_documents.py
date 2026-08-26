import hashlib
import json
from pathlib import Path
import re
import subprocess
import tomllib


ROOT = Path(__file__).resolve().parents[1]
PROJECT_OUTLINE_SNAPSHOT_PATH = (
    "docs/history/PROJECT_OUTLINE_SNAPSHOT_2026-08-11.md"
)
PROJECT_OUTLINE_SNAPSHOT_MANIFEST_PATH = (
    "docs/history/PROJECT_OUTLINE_SNAPSHOT_2026-08-11.json"
)
PROJECT_OUTLINE_SOURCE_COMMIT = "f4a0444e92f80124c3340fd6ad81fc242953d2bc"
PROJECT_OUTLINE_SOURCE_GIT_BLOB_SHA1 = "66ec967e98d76a1abade9c3fc5f30cdc81c5ade2"
PROJECT_OUTLINE_SOURCE_SHA256 = (
    "a08c53978ac7a47175ed96640a5f32dad977ce9a52a169a4368ded2910931879"
)
PROJECT_OUTLINE_SOURCE_BYTE_COUNT = 68_467
PROJECT_OUTLINE_SOURCE_LINE_COUNT = 1_036
PROJECT_OUTLINE_SNAPSHOT_SHA256 = (
    "ec0f62dc7f5294d49429cbc619a09df831a266172d249c54dbf6ce990cbe8b91"
)
PROJECT_OUTLINE_SNAPSHOT_BYTE_COUNT = 69_556
PROJECT_OUTLINE_BODY_MARKER = "<!-- BEGIN EXACT PROJECT_OUTLINE SOURCE BODY -->"
PIPELINE_FOLDER_MAP_SNAPSHOT_PATH = (
    "docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.md"
)
PIPELINE_FOLDER_MAP_SNAPSHOT_MANIFEST_PATH = (
    "docs/history/PIPELINE_FOLDER_MAP_SNAPSHOT_2026-08-11.json"
)
PIPELINE_FOLDER_MAP_SOURCE_COMMIT = "981501502c49c68b791a96652cd2bd65bb82daaf"
PIPELINE_FOLDER_MAP_SOURCE_GIT_BLOB_SHA1 = (
    "28174fb1651e692d1b18b33b1cb14d00dac2f297"
)
PIPELINE_FOLDER_MAP_SOURCE_SHA256 = (
    "0e5372ec4ab257dfa8fa411f9e06fddcbc8be77d416784beeb2e19c1d1d6e827"
)
PIPELINE_FOLDER_MAP_SOURCE_BYTE_COUNT = 39_521
PIPELINE_FOLDER_MAP_SOURCE_LINE_COUNT = 230
PIPELINE_FOLDER_MAP_SNAPSHOT_SHA256 = (
    "db41420fa8943c12eb329ea56d36b4151f793364843c629fd0a043c9928357bd"
)
PIPELINE_FOLDER_MAP_SNAPSHOT_BYTE_COUNT = 40_854
PIPELINE_FOLDER_MAP_MANIFEST_SHA256 = (
    "0d6f96383dfe4a7c34b4eaf90538b9deb4d423e59428fd01b80b23315b1d5cd7"
)
PIPELINE_FOLDER_MAP_MANIFEST_BYTE_COUNT = 1_948
PIPELINE_FOLDER_MAP_BODY_MARKER = (
    "<!-- BEGIN EXACT PIPELINE_FOLDER_MAP SOURCE BODY -->"
)
PROJECT_OUTLINE_RETAINED_HEADINGS = (
    ("Futures intraday research project", "Purpose and scope"),
    ("Research discipline", "Research invariants"),
    ("Non-negotiable data rules", "Research invariants"),
    ("Label, feature, and split rules", "Research invariants"),
    ("Stop conditions", "Stop conditions"),
)
PROJECT_OUTLINE_CONDENSED_HEADINGS = (
    ("Objective", "Purpose and scope"),
    ("Source-of-truth roles", "Authority and navigation"),
    ("Data manifest and rule index", "Current source-of-truth inputs"),
    ("Alpha research lanes", "Current research lanes"),
    ("Standard/full-contract 41-market lane", "Standard/full-contract Alpha lane"),
    (
        "Micro-futures integer lane (legacy Apex source lineage)",
        "Micro-source lane",
    ),
    ("Synthetic Phase 1A-11 mechanics", "Current phase map"),
    ("Runnable commands", "Current phase map"),
    ("Audit commands", "Evidence, outputs, and folder roles"),
    ("Cockpit workflow", "Cockpit and execution boundary"),
    ("Evaluation and model-trust standard", "Research invariants"),
    ("Acceptance standards", "Stop conditions"),
    ("Reporting standard", "Evidence, outputs, and folder roles"),
)
PROJECT_OUTLINE_DELEGATED_HEADINGS = (
    ("Active layout", "SOURCE_OF_TRUTH.md", "SOURCE_OF_TRUTH.md"),
    (
        "Preserved legacy micro Phase 1A/1B/2 route",
        PROJECT_OUTLINE_SNAPSHOT_PATH,
        PROJECT_OUTLINE_SNAPSHOT_PATH,
    ),
    ("Approval gates", "CURRENT_WORKFLOW.md", "CURRENT_WORKFLOW.md"),
    ("Bounded execution policy", "AGENTS.md", "AGENTS.md"),
)
PROJECT_OUTLINE_UNRESOLVED_HEADINGS = frozenset()
PROJECT_OUTLINE_EXPECTED_FORMER_HEADINGS = frozenset(
    {
        "Futures intraday research project",
        "Objective",
        "Source-of-truth roles",
        "Research discipline",
        "Data manifest and rule index",
        "Active layout",
        "Alpha research lanes",
        "Standard/full-contract 41-market lane",
        "Micro-futures integer lane (legacy Apex source lineage)",
        "Synthetic Phase 1A-11 mechanics",
        "Preserved legacy micro Phase 1A/1B/2 route",
        "Non-negotiable data rules",
        "Label, feature, and split rules",
        "Runnable commands",
        "Audit commands",
        "Cockpit workflow",
        "Approval gates",
        "Evaluation and model-trust standard",
        "Bounded execution policy",
        "Acceptance standards",
        "Reporting standard",
        "Stop conditions",
    }
)


def _text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _markdown_headings(text: str) -> set[str]:
    return {
        line.lstrip("#").strip()
        for line in text.splitlines()
        if re.fullmatch(r"#{1,6} .+", line)
    }


def _assert_project_outline_heading_disposition_contract() -> None:
    outline = _text("PROJECT_OUTLINE.md")
    current_headings = _markdown_headings(outline)
    assert current_headings == {
        "Futures Intraday Research Pipeline",
        "1. Purpose and authority",
        "2. Current status",
        "3. Full pipeline",
        "4. Stage table",
        "5. Next-stage requirements",
        "6. Universe, tiers, and time boundaries",
        "7. Core anti-bias rules",
        "8. Evidence and history",
    }


def _assert_project_outline_historical_snapshot_contract() -> None:
    snapshot_path = ROOT / PROJECT_OUTLINE_SNAPSHOT_PATH
    manifest_path = ROOT / PROJECT_OUTLINE_SNAPSHOT_MANIFEST_PATH
    assert snapshot_path.is_file()
    assert manifest_path.is_file()

    snapshot_bytes = snapshot_path.read_bytes()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert isinstance(manifest, dict)
    assert manifest["schema_version"] == "project_outline_historical_snapshot/1.0.0"
    assert manifest["record_type"] == "PROJECT_OUTLINE_COPY_FIRST_SNAPSHOT"
    assert manifest["record_date"] == "2026-08-11"
    assert (
        manifest["state"]
        == "HISTORICAL_EXACT_SOURCE_BODY_PRESERVED_NON_AUTHORIZING"
    )
    assert manifest["current_replacement"] == "PROJECT_OUTLINE.md"

    source = manifest["source"]
    assert source == {
        "byte_count": PROJECT_OUTLINE_SOURCE_BYTE_COUNT,
        "commit": PROJECT_OUTLINE_SOURCE_COMMIT,
        "final_newline": True,
        "git_blob_sha1": PROJECT_OUTLINE_SOURCE_GIT_BLOB_SHA1,
        "line_endings": "LF",
        "line_count": PROJECT_OUTLINE_SOURCE_LINE_COUNT,
        "path": "PROJECT_OUTLINE.md",
        "sha256": PROJECT_OUTLINE_SOURCE_SHA256,
    }

    snapshot = manifest["snapshot"]
    assert snapshot["path"] == PROJECT_OUTLINE_SNAPSHOT_PATH
    assert snapshot["body_marker"] == PROJECT_OUTLINE_BODY_MARKER
    assert snapshot["exact_source_body"] is True
    assert snapshot["sha256"] == PROJECT_OUTLINE_SNAPSHOT_SHA256
    assert snapshot["byte_count"] == PROJECT_OUTLINE_SNAPSHOT_BYTE_COUNT
    assert snapshot["preserved_body_sha256"] == PROJECT_OUTLINE_SOURCE_SHA256
    assert snapshot["preserved_body_byte_count"] == PROJECT_OUTLINE_SOURCE_BYTE_COUNT
    assert hashlib.sha256(snapshot_bytes).hexdigest() == PROJECT_OUTLINE_SNAPSHOT_SHA256
    assert len(snapshot_bytes) == PROJECT_OUTLINE_SNAPSHOT_BYTE_COUNT

    marker_with_lf = (PROJECT_OUTLINE_BODY_MARKER + "\n").encode("utf-8")
    assert snapshot_bytes.count(marker_with_lf) == 1
    preamble_bytes, preserved_body = snapshot_bytes.split(marker_with_lf, 1)
    assert hashlib.sha256(preserved_body).hexdigest() == PROJECT_OUTLINE_SOURCE_SHA256
    assert len(preserved_body) == PROJECT_OUTLINE_SOURCE_BYTE_COUNT
    assert preserved_body.endswith(b"\n") and not preserved_body.endswith(b"\n\n")
    assert b"\r" not in preserved_body

    preamble = preamble_bytes.decode("utf-8")
    for required in (
        "Historical PROJECT_OUTLINE snapshot",
        "2026-08-11",
        PROJECT_OUTLINE_SOURCE_COMMIT,
        "PROJECT_OUTLINE.md",
        "non-authoritative",
        "does not control normal work",
        "CURRENT_WORKFLOW.md",
        "does not claim to be the current research runbook",
        "does not claim that those embedded historical statements remain current",
        "authorize provider access",
    ):
        assert required in preamble

    expected_authority_fields = {
        "active_state_mutation",
        "activation",
        "commit",
        "deletion",
        "historical_row_access",
        "installation",
        "live_smoke",
        "move_or_rename",
        "normal_work",
        "order_placement",
        "provider_access",
        "publication",
        "push",
        "research",
        "safety_policy",
        "staging",
        "trading",
    }
    assert set(manifest["authority"]) == expected_authority_fields
    assert all(value is False for value in manifest["authority"].values())

    registry = json.loads(_text("configs/repository_surface.json"))
    for path, role in (
        (PROJECT_OUTLINE_SNAPSHOT_PATH, "PROJECT_OUTLINE_HISTORICAL_SNAPSHOT"),
        (
            PROJECT_OUTLINE_SNAPSHOT_MANIFEST_PATH,
            "PROJECT_OUTLINE_HISTORICAL_SNAPSHOT_MANIFEST",
        ),
    ):
        matches = [
            entry
            for entry in registry["entries"]
            if entry["match_type"] == "EXACT" and entry["path_or_pattern"] == path
        ]
        assert len(matches) == 1
        entry = matches[0]
        assert entry["classification"] == "HISTORICAL_HASH_BOUND"
        assert entry["authority_role"] == role
        assert entry["current_replacement"] == "PROJECT_OUTLINE.md"
        assert entry["hash_bound"] is True
        assert entry["tracked_expected"] == "TRACKED"
        assert entry["local_only"] is False
        assert entry["deletion_policy"] == "PRESERVE"
        for required_note in (
            "normal work",
            "authoriz",
            "Preserve",
            "relocation or deletion",
        ):
            assert required_note.lower() in entry["notes"].lower()


def _assert_pipeline_folder_map_historical_snapshot_contract() -> None:
    snapshot_path = ROOT / PIPELINE_FOLDER_MAP_SNAPSHOT_PATH
    manifest_path = ROOT / PIPELINE_FOLDER_MAP_SNAPSHOT_MANIFEST_PATH
    assert snapshot_path.is_file()
    assert manifest_path.is_file()

    snapshot_bytes = snapshot_path.read_bytes()
    manifest_bytes = manifest_path.read_bytes()
    assert b"\r" not in snapshot_bytes
    assert b"\r" not in manifest_bytes
    assert manifest_bytes.endswith(b"\n") and not manifest_bytes.endswith(b"\n\n")
    assert hashlib.sha256(manifest_bytes).hexdigest() == PIPELINE_FOLDER_MAP_MANIFEST_SHA256
    assert len(manifest_bytes) == PIPELINE_FOLDER_MAP_MANIFEST_BYTE_COUNT

    manifest = json.loads(manifest_bytes.decode("utf-8"))
    assert isinstance(manifest, dict)
    assert (
        manifest["schema_version"]
        == "pipeline_folder_map_historical_snapshot/1.0.0"
    )
    assert manifest["record_type"] == "PIPELINE_FOLDER_MAP_COPY_FIRST_SNAPSHOT"
    assert manifest["record_date"] == "2026-08-11"
    assert (
        manifest["state"]
        == "HISTORICAL_EXACT_SOURCE_BODY_PRESERVED_NON_AUTHORIZING"
    )
    assert manifest["current_replacement"] == "PIPELINE_FOLDER_MAP.md"
    assert "copy-first preservation boundary" in manifest["purpose"].lower()
    assert "deterministic generation" in manifest["purpose"].lower()

    source = manifest["source"]
    assert source == {
        "byte_count": PIPELINE_FOLDER_MAP_SOURCE_BYTE_COUNT,
        "commit": PIPELINE_FOLDER_MAP_SOURCE_COMMIT,
        "final_newline": True,
        "git_blob_sha1": PIPELINE_FOLDER_MAP_SOURCE_GIT_BLOB_SHA1,
        "line_endings": "LF",
        "line_count": PIPELINE_FOLDER_MAP_SOURCE_LINE_COUNT,
        "path": "PIPELINE_FOLDER_MAP.md",
        "role": "TOPOLOGY_REFERENCE_NOT_AUTHORITY",
        "sha256": PIPELINE_FOLDER_MAP_SOURCE_SHA256,
    }

    snapshot = manifest["snapshot"]
    assert snapshot == {
        "body_marker": PIPELINE_FOLDER_MAP_BODY_MARKER,
        "byte_count": PIPELINE_FOLDER_MAP_SNAPSHOT_BYTE_COUNT,
        "exact_source_body": True,
        "path": PIPELINE_FOLDER_MAP_SNAPSHOT_PATH,
        "preserved_body_byte_count": PIPELINE_FOLDER_MAP_SOURCE_BYTE_COUNT,
        "preserved_body_sha256": PIPELINE_FOLDER_MAP_SOURCE_SHA256,
        "role": "HISTORICAL_REFERENCE_ONLY",
        "sha256": PIPELINE_FOLDER_MAP_SNAPSHOT_SHA256,
    }
    assert hashlib.sha256(snapshot_bytes).hexdigest() == PIPELINE_FOLDER_MAP_SNAPSHOT_SHA256
    assert len(snapshot_bytes) == PIPELINE_FOLDER_MAP_SNAPSHOT_BYTE_COUNT

    marker_with_lf = (PIPELINE_FOLDER_MAP_BODY_MARKER + "\n").encode("utf-8")
    assert snapshot_bytes.count(marker_with_lf) == 1
    preamble_bytes, preserved_body = snapshot_bytes.split(marker_with_lf, 1)
    assert hashlib.sha256(preserved_body).hexdigest() == PIPELINE_FOLDER_MAP_SOURCE_SHA256
    assert len(preserved_body) == PIPELINE_FOLDER_MAP_SOURCE_BYTE_COUNT
    assert preserved_body.count(b"\n") == PIPELINE_FOLDER_MAP_SOURCE_LINE_COUNT
    assert preserved_body.endswith(b"\n") and not preserved_body.endswith(b"\n\n")
    assert b"\r" not in preserved_body

    preamble = preamble_bytes.decode("utf-8")
    normalized_preamble = " ".join(preamble.split())
    for required in (
        "Historical PIPELINE_FOLDER_MAP snapshot",
        "2026-08-11",
        PIPELINE_FOLDER_MAP_SOURCE_COMMIT,
        "PIPELINE_FOLDER_MAP.md",
        "non-authoritative",
        "does not control normal work",
        "CURRENT_WORKFLOW.md",
        "configs/repository_surface.json",
        "canonical machine-readable",
        "does not claim that those embedded classifications remain current",
        "does not claim to be the generated current topology guide",
        "authorize provider access",
    ):
        assert required in normalized_preamble

    for forbidden_claim in (
        "This document controls normal work",
        "This document is the canonical machine-readable repository-role registry",
        "This document defines public commands",
        "This document is the generated current topology guide",
    ):
        assert forbidden_claim.lower() not in preamble.lower()

    expected_authority_fields = {
        "active_state_mutation",
        "activation",
        "commit",
        "deletion",
        "historical_row_access",
        "installation",
        "live_smoke",
        "move_or_rename",
        "normal_work",
        "order_placement",
        "provider_access",
        "public_command_authority",
        "publication",
        "push",
        "repository_role_registry",
        "research",
        "safety_policy",
        "staging",
        "topology_authority",
        "trading",
    }
    assert set(manifest["authority"]) == expected_authority_fields
    assert all(value is False for value in manifest["authority"].values())

    registry = json.loads(_text("configs/repository_surface.json"))
    for path, role in (
        (
            PIPELINE_FOLDER_MAP_SNAPSHOT_PATH,
            "PIPELINE_FOLDER_MAP_HISTORICAL_SNAPSHOT",
        ),
        (
            PIPELINE_FOLDER_MAP_SNAPSHOT_MANIFEST_PATH,
            "PIPELINE_FOLDER_MAP_HISTORICAL_SNAPSHOT_MANIFEST",
        ),
    ):
        matches = [
            entry
            for entry in registry["entries"]
            if entry["match_type"] == "EXACT" and entry["path_or_pattern"] == path
        ]
        assert len(matches) == 1
        entry = matches[0]
        assert entry["classification"] == "HISTORICAL_HASH_BOUND"
        assert entry["authority_role"] == role
        assert entry["current_replacement"] == "PIPELINE_FOLDER_MAP.md"
        assert entry["hash_bound"] is True
        assert entry["tracked_expected"] == "TRACKED"
        assert entry["local_only"] is False
        assert entry["deletion_policy"] == "PRESERVE"
        for required_note in (
            PIPELINE_FOLDER_MAP_SOURCE_COMMIT,
            "exact",
            "normal work",
            "repository-surface registry",
            "public commands",
            "authoriz",
            "Phase 3C2",
            "relocation or deletion",
        ):
            assert required_note.lower() in entry["notes"].lower()


def _assert_pipeline_folder_map_is_concise_generated_current_view() -> None:
    path = ROOT / "PIPELINE_FOLDER_MAP.md"
    document = path.read_bytes()
    text = document.decode("utf-8")
    lowered = " ".join(text.split()).lower()

    assert b"\r" not in document
    assert document.endswith(b"\n") and not document.endswith(b"\n\n")
    assert len(text.split()) <= 1_800
    assert len(document) <= 20 * 1_024

    table_rows = 0
    lines = text.splitlines()
    for index in range(len(lines) - 1):
        if lines[index].startswith("|") and lines[index + 1].startswith("| ---"):
            cursor = index + 2
            while cursor < len(lines) and lines[cursor].startswith("|"):
                table_rows += 1
                cursor += 1
    assert table_rows <= 50

    for required in (
        "deterministically rendered from",
        "configs/repository_surface.json",
        "pyproject.toml",
        "CURRENT_WORKFLOW.md` controls normal work",
        "AGENTS.md` contains durable safety",
        "SOURCE_OF_TRUTH.md` is the broader generated",
        PIPELINE_FOLDER_MAP_SNAPSHOT_PATH,
        "docs/LEGACY_WORKFLOWS.md",
        "CertifiedResearchGateway",
        "sole current real-history",
        "futures-pipeline` is synthetic-only",
        "standard Alpha pointer/catalog and micro source pointer/catalog remain separate",
        "Micro source selection does not establish",
        "FuturesLiveCockpit/` is a mixed packaging source/output surface",
        "UNRESOLVED_MANUAL_REVIEW",
        "Neither this generated map nor the registry authorizes",
    ):
        assert required.lower() in lowered

    registry = json.loads(_text("configs/repository_surface.json"))
    map_entries = [
        entry
        for entry in registry["entries"]
        if entry["path_or_pattern"] == "PIPELINE_FOLDER_MAP.md"
    ]
    assert len(map_entries) == 1
    map_entry = map_entries[0]
    assert map_entry["match_type"] == "EXACT"
    assert map_entry["classification"] == "CURRENT_SUPPORTING"
    assert map_entry["authority_role"] == "GENERATED_PIPELINE_FOLDER_MAP_VIEW"
    assert map_entry["tracked_expected"] == "TRACKED"
    assert map_entry["local_only"] is False
    assert map_entry["hash_bound"] is False
    assert map_entry["deletion_policy"] == "PRESERVE"

    scripts = tomllib.loads(_text("pyproject.toml"))["project"]["scripts"]
    expected_command_rows = [
        f"| `{name}` | `{target}` |" for name, target in sorted(scripts.items())
    ]
    assert [line for line in lines if line in expected_command_rows] == expected_command_rows

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


def _assert_project_outline_is_concise_current_runbook() -> None:
    outline_path = ROOT / "PROJECT_OUTLINE.md"
    outline_bytes = outline_path.read_bytes()
    outline = outline_bytes.decode("utf-8")
    lowered = " ".join(outline.split()).lower()

    assert b"\r" not in outline_bytes
    assert outline_bytes.endswith(b"\n")
    assert not outline_bytes.endswith(b"\n\n")
    assert len(outline.split()) <= 3_500
    assert len(outline_bytes) <= 24 * 1_024
    assert len(outline.splitlines()) <= 450

    for required in (
        "Futures Intraday Research Pipeline",
        "CURRENT_WORKFLOW.md",
        "AGENTS.md",
        "docs/LEGACY_WORKFLOWS.md",
        "CURRENT_WORKFLOW.md` controls procedure and approvals",
        "Causal observation release",
        "NOT BUILT",
        "Direct DBN use by features, models, WFA, or backtests is forbidden",
        "seven-market development canary ran once under consumed authority",
        "Final Sealed 252-Session Holdout",
        "SEALED / PRISTINE / UNREAD",
            "NEXT REAL-DATA GATE: FRESH BOUNDED ES-2025 V10 CANARY AUTHORITY",
    ):
        assert required.lower() in lowered

    assert "project_outline.md controls normal work" not in lowered
    assert "project_outline.md is the normal-work workflow authority" not in lowered
    assert "metadata preflight v2" not in lowered
    assert "acquisition v21" not in lowered
    assert "custody repair v2" not in lowered
    assert "preserved legacy micro phase 1a/1b/2 route" not in lowered
    assert lowered.count("consumed authoriz") <= 1

    assert re.search(r"(?i)\b[A-Z]:\\", outline) is None
    assert "%TEMP%" not in outline
    assert "AppData\\Local\\Temp" not in outline
    assert re.search(r"(?i)(api[_-]?key|password|token)\s*[:=]\s*\S+", outline) is None
    assert re.search(r"\b[0-9a-f]{40}\b", outline) is None
    timestamps = re.findall(r"\b20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", outline)
    assert timestamps == ["2025-07-13T22:00:00Z", "2026-07-14T00:00:00Z"]
    for mutable_status in (
        "current head",
        "branch:",
        "ahead of origin",
        "working tree is",
    ):
        assert mutable_status not in lowered

    named_commands = set(re.findall(r"\bfutures-[a-z0-9-]+\b", outline))
    assert not named_commands
    for retired in (
        "futures-live-cockpit",
        "futures-closure-workflow",
        "futures-calendar",
        "futures-active-view",
    ):
        assert retired not in named_commands

    snapshot_bytes = (ROOT / PROJECT_OUTLINE_SNAPSHOT_PATH).read_bytes()
    marker_with_lf = (PROJECT_OUTLINE_BODY_MARKER + "\n").encode("utf-8")
    _, preserved_body = snapshot_bytes.split(marker_with_lf, 1)
    assert outline_bytes != preserved_body


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
    _assert_project_outline_heading_disposition_contract()
    _assert_project_outline_is_concise_current_runbook()
    _assert_public_snapshot_is_historical()
    _assert_project_outline_historical_snapshot_contract()
    _assert_pipeline_folder_map_historical_snapshot_contract()
    _assert_pipeline_folder_map_is_concise_generated_current_view()


def test_public_snapshot_is_an_explicit_historical_record() -> None:
    _assert_public_snapshot_is_historical()


def test_project_outline_copy_first_snapshot_is_exact_and_non_authorizing() -> None:
    _assert_project_outline_historical_snapshot_contract()


def test_pipeline_folder_map_copy_first_snapshot_is_exact_and_non_authorizing() -> None:
    _assert_pipeline_folder_map_historical_snapshot_contract()


def test_pipeline_folder_map_is_a_concise_generated_current_view() -> None:
    _assert_pipeline_folder_map_is_concise_generated_current_view()


def test_project_outline_is_current_runbook_not_historical_ledger() -> None:
    _assert_project_outline_is_concise_current_runbook()


def test_project_outline_heading_dispositions_are_complete_and_resolved() -> None:
    _assert_project_outline_heading_disposition_contract()


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
    current = _text("CURRENT_WORKFLOW.md")
    mapping = _text("PIPELINE_FOLDER_MAP.md")
    registry = json.loads(_text("configs/repository_surface.json"))
    scripts = tomllib.loads(_text("pyproject.toml"))["project"]["scripts"]

    assert "The only current code surface" in current
    assert "CertifiedResearchGateway" in mapping
    matches = [
        entry
        for entry in registry["entries"]
        if entry["authority_role"] == "CURRENT_REAL_HISTORY_GATEWAY"
    ]
    assert len(matches) == 1
    assert matches[0]["path_or_pattern"] == (
        "src/futures_rebuild/certified_research_gateway.py"
    )
    assert "No other public command provides a real-history execution surface" in mapping
    assert all("certified_research_gateway" not in target for target in scripts.values())
    assert "retired" in mapping.lower()


def test_micro_pipeline_map_distinguishes_design_from_implementation() -> None:
    mapping = _text("PIPELINE_FOLDER_MAP.md")
    registry = json.loads(_text("configs/repository_surface.json"))

    roles = {
        entry["authority_role"]: entry["path_or_pattern"]
        for entry in registry["entries"]
        if entry["authority_role"]
        in {
            "ACTIVE_STANDARD_ALPHA_IDENTITY",
            "ACTIVE_STANDARD_DATA_SELECTION",
            "ACTIVE_MICRO_SOURCE_SELECTION",
            "ACTIVE_MICRO_DATA_SELECTION",
        }
    }
    assert roles == {
        "ACTIVE_STANDARD_ALPHA_IDENTITY": "configs/active_alpha_research_ladder.json",
        "ACTIVE_STANDARD_DATA_SELECTION": "data/active/catalog.json",
        "ACTIVE_MICRO_SOURCE_SELECTION": "configs/active_micro_alpha_research_ladder.json",
        "ACTIVE_MICRO_DATA_SELECTION": "data/active/catalogs/apex_micro.json",
    }
    for path in roles.values():
        assert f"`{path}`" in mapping
    assert "machine-local" in mapping.lower()
    assert "Micro source selection does not establish" in mapping
    assert "untracked execution-looking code is not current" in mapping
    for old_taxonomy in (
        "`CURRENT_REACHABLE`",
        "`SYNTHETIC_ONLY`",
        "`HISTORICAL_ROW_APPROVAL_REQUIRED`",
        "`RETIRED`",
    ):
        assert old_taxonomy not in mapping


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
