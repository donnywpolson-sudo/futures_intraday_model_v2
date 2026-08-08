"""Create-only durable staging for complete, explicitly unpublished evidence."""

from __future__ import annotations

import tempfile
from collections.abc import Mapping
from pathlib import Path

from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from .errors import IntegrityError


REQUIRED_PAYLOADS = (
    "model", "predictions", "opportunity_ledger", "fills",
    "continuous_equity_marks", "segmented_metrics", "inference",
    "decision", "runtime_receipt", "source_integrity_audit",
)


def build_unpublished_manifest(
    *, trial_id: str, authorization_receipt_id: str,
    payloads: Mapping[str, object],
) -> dict[str, object]:
    if (
        len(trial_id) != 64 or len(authorization_receipt_id) != 64
        or set(payloads) != set(REQUIRED_PAYLOADS)
    ):
        raise IntegrityError("unpublished evidence identity or payload set is incomplete")
    files = {
        f"{name}.json": sha256_bytes(
            canonical_bytes({"payload": payloads[name]}) + b"\n"
        )
        for name in sorted(payloads)
    }
    core = {
        "schema_version": "tier1_final_unpublished_evidence_manifest/1.0.0",
        "state": "SEALED_UNPUBLISHED",
        "trial_id": trial_id,
        "authorization_receipt_id": authorization_receipt_id,
        "files": files,
        "publication": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "trading": False,
    }
    return {**core, "manifest_id": sha256_json(core)}


def stage_unpublished_evidence(
    *, root: Path, boundary: RepoBoundary, output_root: Path,
    trial_id: str, authorization_receipt_id: str,
    payloads: Mapping[str, object],
) -> dict[str, str]:
    """Seal a complete bundle once; never publish, overwrite, or silently discard it."""

    boundary.assert_active_path(
        output_root.absolute(), purpose="final unpublished evidence root",
    )
    manifest = build_unpublished_manifest(
        trial_id=trial_id,
        authorization_receipt_id=authorization_receipt_id,
        payloads=payloads,
    )
    # One manifest identity already commits to the full trial and receipt IDs.
    # Keeping the directory shallow also prevents Windows legacy path overflow.
    destination = output_root / str(manifest["manifest_id"])
    boundary.assert_active_path(
        destination.absolute(), purpose="final unpublished evidence bundle",
    )
    if destination.exists():
        raise IntegrityError("unpublished evidence staging is create-only")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".staging-{str(manifest['manifest_id'])[:12]}-",
        dir=destination.parent,
    ))
    for filename, expected_hash in manifest["files"].items():
        name = filename.removesuffix(".json")
        path = staging / filename
        with path.open("xb") as stream:
            stream.write(canonical_bytes({"payload": payloads[name]}) + b"\n")
        if sha256_file(path) != expected_hash:
            raise IntegrityError("staged unpublished evidence hash mismatch")
    with (staging / "manifest.json").open("xb") as stream:
        stream.write(canonical_bytes(manifest) + b"\n")
    if destination.exists():
        raise IntegrityError("unpublished evidence destination appeared during staging")
    staging.replace(destination)
    return {
        "state": "SEALED_UNPUBLISHED",
        "manifest_id": str(manifest["manifest_id"]),
        "bundle_path": destination.relative_to(root).as_posix(),
        "manifest_sha256": sha256_file(destination / "manifest.json"),
        "publication": "false",
    }


def verify_unpublished_evidence(
    *, root: Path, bundle_path: Path,
) -> dict[str, object]:
    """Verify a sealed bundle without converting it into published evidence."""

    import json

    destination = root / bundle_path
    try:
        manifest = json.loads((destination / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("unpublished evidence manifest is unreadable") from exc
    core = dict(manifest)
    manifest_id = core.pop("manifest_id", None)
    files = manifest.get("files")
    if (
        manifest_id != sha256_json(core)
        or manifest.get("state") != "SEALED_UNPUBLISHED"
        or manifest.get("publication") is not False
        or not isinstance(files, Mapping)
        or set(files) != {f"{name}.json" for name in REQUIRED_PAYLOADS}
        or any(sha256_file(destination / name) != digest for name, digest in files.items())
    ):
        raise IntegrityError("unpublished evidence bundle is incomplete or drifted")
    return manifest
