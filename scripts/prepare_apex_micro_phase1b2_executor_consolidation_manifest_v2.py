"""Freeze the binding-complete Phase 1B/2 executor consolidation successor."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for candidate in (ROOT, ROOT / "src"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json  # noqa: E402
from futures_rebuild.errors import IntegrityError  # noqa: E402


PREDECESSOR = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_executor_consolidation_manifest_v1/manifest.json"
)
OUTPUT_ROOT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_executor_consolidation_manifest_v2"
)
SUPERSESSION = OUTPUT_ROOT / "predecessor_supersession.json"
OUTPUT = OUTPUT_ROOT / "manifest.json"
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
RECOMMENDED = (
    "PIPELINE_FOLDER_MAP.md",
    "PROJECT_OUTLINE.md",
    "scripts/prepare_apex_micro_phase1b2_execution_v1.py",
    "scripts/prepare_apex_micro_phase1b2_executor_consolidation_manifest_v1.py",
    "scripts/prepare_apex_micro_phase1b2_executor_consolidation_manifest_v2.py",
    "src/futures_rebuild/micro_alpha_phase1b2_decoder.py",
    "src/futures_rebuild/micro_alpha_phase1b2_execution.py",
    "src/futures_rebuild/micro_alpha_phase1b2_preparation.py",
    PREDECESSOR.as_posix(),
    SUPERSESSION.as_posix(),
    OUTPUT.as_posix(),
    "tests/test_micro_alpha_phase1b2_decoder_v1.py",
    "tests/test_micro_alpha_phase1b2_execution_v1.py",
    "tests/test_micro_alpha_phase1b2_executor_consolidation_manifest_v1.py",
    "tests/test_micro_alpha_phase1b2_executor_consolidation_manifest_v2.py",
    "tests/test_micro_alpha_phase1b2_preparation.py",
)


def _git(*args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(ROOT), *args], text=True
    ).strip()


def _worktree_paths() -> set[str]:
    raw = subprocess.check_output(
        [
            "git", "-C", str(ROOT), "status", "--porcelain=v1", "-z",
            "--untracked-files=all",
        ],
        text=True,
    )
    paths: set[str] = set()
    for record in raw.split("\0"):
        if not record:
            continue
        path = record[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.add(path.replace("\\", "/"))
    return paths


def _tracked(path: Path) -> bool:
    return subprocess.run(
        [
            "git", "-C", str(ROOT), "ls-files", "--error-unmatch", "--",
            path.as_posix(),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0


def _committed_bytes(commit: str, path: str) -> bytes:
    return subprocess.check_output(
        ["git", "-C", str(ROOT), "show", f"{commit}:{path}"]
    )


def _postcommit_manifest() -> dict[str, object]:
    commit = _git("log", "-1", "--format=%H", "--", OUTPUT.as_posix())
    committed_paths = set(
        _git(
            "diff-tree", "--no-commit-id", "--name-only", "-r", commit
        ).splitlines()
    )
    if committed_paths != set(RECOMMENDED):
        raise IntegrityError("executor consolidation commit differs from exact scope")
    value = json.loads(_committed_bytes(commit, OUTPUT.as_posix()))
    core = dict(value)
    observed_id = core.pop("manifest_id", None)
    if observed_id != sha256_json(core):
        raise IntegrityError("committed executor manifest identity drifted")
    if (
        value.get("recommended_exact_stage_paths") != list(RECOMMENDED)
        or value.get("observed_head") != _git("rev-parse", f"{commit}^")
    ):
        raise IntegrityError("committed executor manifest bindings drifted")
    records = {item["path"]: item["sha256"] for item in value["records"]}
    for path in RECOMMENDED:
        if path == OUTPUT.as_posix():
            if records[path] != "SELF_HASHED_AT_WRITE":
                raise IntegrityError("executor manifest self-hash marker drifted")
        elif hashlib.sha256(_committed_bytes(commit, path)).hexdigest() != records[path]:
            raise IntegrityError("committed executor manifest file hash drifted")
    return value


def build_supersession() -> dict[str, object]:
    predecessor = json.loads((ROOT / PREDECESSOR).read_text(encoding="utf-8"))
    if predecessor.get("manifest_id") != (
        "1d7c1b0257408f9ca8c201cbe544ff5ac29e9264142579784dfdb0471f6750d5"
    ):
        raise IntegrityError("executor predecessor manifest identity drifted")
    core = {
        "schema_version": "apex_micro_phase1b2_executor_manifest_supersession/1.0.0",
        "state": "SUPERSEDED_PREPARATION_INTERVAL_RECEIPT_BINDING_COMPLETION",
        "predecessor_manifest_id": predecessor["manifest_id"],
        "predecessor_manifest_sha256": sha256_file(ROOT / PREDECESSOR),
        "cause": "PER_INTERVAL_RECEIPT_DID_NOT_BIND_EXACT_REQUEST_SOURCE_SIDECAR_QUERY_AND_RELEASE",
        "correction": "SUCCESSOR_RECEIPT_BINDS_REQUEST_MARKET_SCHEMA_YEAR_INTERVAL_SOURCE_SIDECAR_QUERY_AND_PHASE1B_RELEASE",
        "predecessor_overwritten_or_deleted": False,
        "historical_rows_read": 0,
        "provider_or_network_calls": 0,
        "credential_access": False,
        "staging_commit_or_push": False,
    }
    return {**core, "supersession_id": sha256_json(core)}


def _canonical_sha256(value: dict[str, object]) -> str:
    return hashlib.sha256(canonical_bytes(value) + b"\n").hexdigest()


def build_manifest() -> dict[str, object]:
    if _tracked(OUTPUT):
        return _postcommit_manifest()
    if _git("diff", "--cached", "--name-only"):
        raise IntegrityError("executor successor consolidation requires an empty index")
    expected = set(RECOMMENDED) | set(PRESERVED_UNSTAGED)
    for optional in (SUPERSESSION, OUTPUT):
        if not (ROOT / optional).exists():
            expected.remove(optional.as_posix())
    if _worktree_paths() != expected:
        raise IntegrityError("executor successor worktree differs from the exact scope")
    supersession = build_supersession()
    records = []
    for path in RECOMMENDED:
        if path == OUTPUT.as_posix():
            digest = "SELF_HASHED_AT_WRITE"
        elif path == SUPERSESSION.as_posix() and not (ROOT / SUPERSESSION).exists():
            digest = _canonical_sha256(supersession)
        else:
            digest = sha256_file(ROOT / path)
        records.append(
            {
                "path": path,
                "sha256": digest,
                "recommended_for_exact_stage": True,
            }
        )
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1b2_executor_consolidation_manifest/2.0.0",
        "state": "PREPARED_REQUIRES_EXACT_PATH_STAGING_APPROVAL",
        "repository_root": ROOT.resolve().as_posix(),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "upstream": _git("rev-parse", "--abbrev-ref", "@{upstream}"),
        "upstream_head": _git("rev-parse", "@{upstream}"),
        "predecessor_supersession_id": supersession["supersession_id"],
        "recommended_exact_stage_path_count": len(RECOMMENDED),
        "recommended_exact_stage_paths": list(RECOMMENDED),
        "preserved_unstaged_paths": list(PRESERVED_UNSTAGED),
        "records": records,
        "preserved_records": [
            {
                "path": path,
                "sha256": sha256_file(ROOT / path),
                "recommended_for_exact_stage": False,
            }
            for path in PRESERVED_UNSTAGED
        ],
        "scope": {
            "lane_id": "apex_integer_micro_11",
            "markets": ["MES", "MCL", "MGC", "M6E"],
            "schemas": ["definition", "status", "statistics", "ohlcv-1m", "ohlcv-1s"],
            "eligible_years": list(range(2018, 2025)),
            "source_count": 120,
            "source_bytes": 1_232_883_585,
            "coverage_cell_count": 140,
            "prelaunch_cell_count": 20,
            "per_interval_receipt_exact_source_binding": True,
            "row_payloads_opened": 0,
        },
        "authority_and_effects": {
            "historical_rows_read": 0,
            "year_2025_or_2026_payloads_opened": 0,
            "provider_or_network_calls": 0,
            "credential_access": False,
            "catalog_or_pointer_activated": False,
            "published_registered_evaluated_or_traded": False,
            "cleanup_mutation": False,
            "git_staging": False,
            "git_commit": False,
            "git_push": False,
        },
        "next_sequential_boundary": "EXACT_PATH_STAGING_APPROVAL",
        "after_staging": "SEPARATE_LOCAL_COMMIT_APPROVAL_NO_PUSH",
        "after_commit": "CREATE_ONLY_EXECUTION_PLAN_AND_AUDIT_PREPARATION",
        "next_research_boundary": "SEPARATE_EXACT_PHASE1B2_HISTORICAL_ROW_CONFIRMATION",
    }
    return {**core, "manifest_id": sha256_json(core)}


def _write_create_only(path: Path, value: dict[str, object]) -> None:
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as stream:
        stream.write(canonical_bytes(value) + b"\n")


def write_create_only() -> dict[str, object]:
    supersession = build_supersession()
    if not (ROOT / SUPERSESSION).exists():
        _write_create_only(SUPERSESSION, supersession)
    elif json.loads((ROOT / SUPERSESSION).read_text(encoding="utf-8")) != supersession:
        raise IntegrityError("executor predecessor supersession differs")
    value = build_manifest()
    _write_create_only(OUTPUT, value)
    return value


def main() -> int:
    value = write_create_only()
    print(
        json.dumps(
            {
                "manifest_id": value["manifest_id"],
                "sha256": sha256_file(ROOT / OUTPUT),
                "recommended_exact_stage_path_count": len(RECOMMENDED),
                "state": value["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
