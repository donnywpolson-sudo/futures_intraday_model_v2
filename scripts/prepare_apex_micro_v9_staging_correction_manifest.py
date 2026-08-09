"""Prepare the immutable successor manifest for the v9 staging correction."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR = Path(
    "state/unpublished_evidence/apex_micro_v9_consolidation_manifest/manifest.json"
)
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_v9_staging_correction_manifest/manifest.json"
)
BUILDER = Path("scripts/prepare_apex_micro_v9_staging_correction_manifest.py")
CORRECTED_PATH = Path("scripts/prepare_apex_micro_metadata_preflight_v9.py")
PRESERVED_UNSTAGED = ("CODEX_HANDOFF.md", "CURRENT_WORKFLOW.md")


def _git(*args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(ROOT), *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def _json(path: Path) -> dict[str, object]:
    try:
        value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid JSON binding: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"JSON binding is not an object: {path.as_posix()}")
    return value


def _lines(*args: str) -> list[str]:
    output = _git(*args)
    return [] if not output else output.splitlines()


def _status_paths() -> list[str]:
    completed = subprocess.run(
        [
            "git",
            "-C",
            str(ROOT),
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    paths: list[str] = []
    for record in completed.stdout.decode("utf-8", "strict").split("\0"):
        if not record:
            continue
        path = record[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        paths.append(path.replace("\\", "/"))
    return sorted(paths)


def build_manifest() -> dict[str, object]:
    predecessor = _json(PREDECESSOR)
    predecessor_paths = sorted(predecessor["recommended_exact_stage_paths"])
    cached_paths = sorted(_lines("diff", "--cached", "--name-only"))
    if cached_paths != predecessor_paths:
        raise IntegrityError("index no longer matches the approved predecessor path set")

    corrected = CORRECTED_PATH.as_posix()
    expected_unstaged = sorted({corrected, *PRESERVED_UNSTAGED})
    if sorted(_lines("diff", "--name-only")) != expected_unstaged:
        raise IntegrityError(
            "only the two preserved workflow files and whitespace-corrected "
            "v9 preparation script may differ"
        )
    if _git("diff", "--check"):
        raise IntegrityError("corrected worktree fails git diff --check")

    recommended = sorted(
        set(predecessor_paths) | {BUILDER.as_posix(), OUTPUT.as_posix()}
    )
    expected_status = sorted(
        (set(recommended) - {OUTPUT.as_posix()}) | set(PRESERVED_UNSTAGED)
    )
    if (ROOT / OUTPUT).exists():
        expected_status = sorted(set(expected_status) | {OUTPUT.as_posix()})
    observed_status = _status_paths()
    if observed_status != expected_status:
        raise IntegrityError(
            "worktree differs from the staging-correction scope: "
            + json.dumps(
                {
                    "missing": sorted(set(expected_status) - set(observed_status)),
                    "unexpected": sorted(set(observed_status) - set(expected_status)),
                },
                sort_keys=True,
            )
        )

    bound_paths = [path for path in recommended if path != OUTPUT.as_posix()]
    records = [
        {"path": path, "sha256": sha256_file(ROOT / path)}
        for path in bound_paths
    ]
    core: dict[str, object] = {
        "schema_version": "apex_micro_v9_staging_correction_manifest/1.0.0",
        "state": "PREPARED_SUCCESSOR_EXACT_PATH_RESTAGING_APPROVAL_REQUIRED",
        "repository_root": str(ROOT),
        "branch": _git("branch", "--show-current"),
        "observed_head": _git("rev-parse", "HEAD"),
        "predecessor_manifest": {
            "path": PREDECESSOR.as_posix(),
            "manifest_id": predecessor["manifest_id"],
            "sha256": sha256_file(ROOT / PREDECESSOR),
            "approved_exact_path_staging_consumed": True,
            "staged_path_count": len(cached_paths),
        },
        "correction": {
            "path": corrected,
            "reason": "INDEX_DIFF_CHECK_TRAILING_BLANK_LINE_REMEDIATED",
            "behavior_changed": False,
            "v9_plan_changed": False,
            "v9_provider_access_performed": False,
        },
        "records": records,
        "recommended_exact_stage_paths": recommended,
        "recommended_exact_stage_path_count": len(recommended),
        "preserved_unstaged_paths": list(PRESERVED_UNSTAGED),
        "verification": {
            "predecessor_index_path_set_exact": True,
            "corrected_worktree_diff_check_passed": True,
            "deterministic_reconstruction_required": True,
        },
        "authority_and_effects": {
            "commit_performed": False,
            "push_performed": False,
            "provider_access_performed_for_v9": False,
            "dbn_download_performed": False,
            "historical_rows_read": False,
            "cleanup_mutation_performed": False,
        },
    }
    return {**core, "manifest_id": sha256_json(core)}


def main() -> int:
    manifest = build_manifest()
    path = ROOT / OUTPUT
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(manifest) + b"\n"
    if path.exists():
        if path.read_bytes() != raw:
            raise RuntimeError("existing staging-correction manifest differs")
    else:
        with path.open("xb") as stream:
            stream.write(raw)
    print(
        json.dumps(
            {
                "manifest_id": manifest["manifest_id"],
                "manifest_path": OUTPUT.as_posix(),
                "manifest_sha256": sha256_file(path),
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
