"""Freeze the exact post-commit reconstruction remediation scope."""

from __future__ import annotations

import json
import subprocess
from hashlib import sha256
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError

if __package__:
    from scripts.prepare_apex_micro_phase1b2_consolidation_manifest_v1 import (
        _git,
        _tracked,
        _worktree_paths,
        _committed_bytes,
    )
else:
    from prepare_apex_micro_phase1b2_consolidation_manifest_v1 import (
        _git,
        _tracked,
        _worktree_paths,
        _committed_bytes,
    )


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_postcommit_reconstruction_manifest_v1/manifest.json"
)
PREDECESSOR_MANIFEST = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_source_safe_consolidation_manifest_v1/manifest.json"
)
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")
RECOMMENDED = (
    "scripts/prepare_apex_micro_phase1b2_consolidation_manifest_v1.py",
    "scripts/prepare_apex_micro_phase1b2_postcommit_reconstruction_manifest_v1.py",
    OUTPUT.as_posix(),
    "tests/test_micro_alpha_phase1b2_consolidation_manifest_v1.py",
)


def build_manifest(*, root: Path = ROOT) -> dict[str, object]:
    root = root.resolve(strict=True)
    historical_commit: str | None = None
    if _tracked(root, OUTPUT.as_posix()):
        historical_commit = _git(
            root, "log", "-1", "--format=%H", "--", OUTPUT.as_posix()
        )
        committed_paths = set(
            _git(
                root, "diff-tree", "--no-commit-id", "--name-only", "-r",
                historical_commit,
            ).splitlines()
        )
        if committed_paths != set(RECOMMENDED):
            raise IntegrityError("remediation commit does not contain the exact scope")
        binding_head = _git(root, "rev-parse", f"{historical_commit}^")
    else:
        expected = set(RECOMMENDED) | set(PRESERVED_UNSTAGED)
        if not (root / OUTPUT).exists():
            expected.remove(OUTPUT.as_posix())
        if set(_worktree_paths(root)) != expected:
            raise IntegrityError("remediation worktree does not match the exact scope")
        binding_head = _git(root, "rev-parse", "HEAD")
    if _git(root, "diff", "--cached", "--name-only"):
        raise IntegrityError("remediation manifest requires an empty index")

    records = []
    for path in RECOMMENDED:
        if path == OUTPUT.as_posix():
            digest = "SELF_HASHED_AT_WRITE"
        elif historical_commit is not None:
            digest = sha256(
                _committed_bytes(root, commit=historical_commit, path=path)
            ).hexdigest()
        else:
            digest = sha256_file(root / path)
        records.append(
            {
                "path": path,
                "sha256": digest,
                "recommended_for_exact_stage": True,
            }
        )
    core: dict[str, object] = {
        "schema_version": (
            "apex_micro_phase1b2_postcommit_reconstruction_manifest/1.0.0"
        ),
        "state": "PREPARED_REQUIRES_EXACT_PATH_STAGING_APPROVAL",
        "repository_root": root.as_posix(),
        "branch": _git(root, "branch", "--show-current"),
        "observed_head": binding_head,
        "predecessor_manifest": {
            "path": PREDECESSOR_MANIFEST.as_posix(),
            "manifest_id": (
                "53a253dfd1f9c27cb292c0c945829e459b7bea495cc0d33f720af5cee415218d"
            ),
            "sha256": sha256_file(root / PREDECESSOR_MANIFEST),
            "committed_head": "a9ce488de09c81c89da3361e38c4c3690ca96ba5",
        },
        "defect": {
            "state": "POST_COMMIT_RECONSTRUCTION_DEFECT_REMEDIATED",
            "cause": "BUILDER_REQUIRED_PRECOMMIT_WORKTREE_AFTER_COMMIT",
            "correction": (
                "RECONSTRUCT_FROM_EXACT_HISTORICAL_CONSOLIDATION_COMMIT"
            ),
            "predecessor_manifest_overwritten": False,
        },
        "recommended_exact_stage_path_count": len(RECOMMENDED),
        "recommended_exact_stage_paths": list(RECOMMENDED),
        "preserved_unstaged_paths": list(PRESERVED_UNSTAGED),
        "records": records,
        "authority_and_effects": {
            "historical_rows_read": 0,
            "dbn_payloads_opened": 0,
            "year_2025_or_2026_payloads_opened": 0,
            "provider_or_network_calls": 0,
            "catalog_or_pointer_activated": False,
            "cleanup_mutation": False,
            "git_staging": False,
            "git_commit": False,
            "git_push": False,
        },
        "next_sequential_boundary": "EXACT_PATH_STAGING_APPROVAL",
        "after_staging": "SEPARATE_LOCAL_COMMIT_APPROVAL_NO_PUSH",
    }
    return {**core, "manifest_id": sha256_json(core)}


def write_create_only(*, root: Path = ROOT) -> dict[str, object]:
    manifest = build_manifest(root=root)
    output = root / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("xb") as stream:
        stream.write(canonical_bytes(manifest) + b"\n")
    return manifest


def main() -> int:
    manifest = write_create_only(root=ROOT)
    print(
        json.dumps(
            {
                "manifest_id": manifest["manifest_id"],
                "sha256": sha256_file(ROOT / OUTPUT),
                "recommended_exact_stage_path_count": manifest[
                    "recommended_exact_stage_path_count"
                ],
                "state": manifest["state"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
