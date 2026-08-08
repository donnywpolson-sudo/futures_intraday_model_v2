"""Fail-closed verification of the repository-local locked Python environment."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import platform
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import ContractError, IntegrityError


DEPENDENCY_RECEIPT = Path("configs/dependency_lock_receipt.json")
PINNED_PYTHON = Path(".venv/Scripts/python.exe")


def _json_object(path: Path, description: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise IntegrityError(f"{description} is not a JSON object")
    return payload


def _locked_runtime(
    repository_root: Path,
) -> tuple[dict[str, object], Mapping[str, str]]:
    root = repository_root.resolve(strict=True)
    receipt = _json_object(
        root / DEPENDENCY_RECEIPT,
        "dependency lock receipt",
    )
    if set(receipt) != {"files", "receipt_id", "receipt_version", "runtime"}:
        raise IntegrityError("dependency lock receipt schema is invalid")
    core = {key: receipt[key] for key in receipt if key != "receipt_id"}
    if (
        receipt.get("receipt_version") != "1.1.0"
        or receipt.get("receipt_id") != sha256_json(core)
        or not isinstance(receipt.get("files"), list)
        or not isinstance(receipt.get("runtime"), dict)
    ):
        raise IntegrityError("dependency lock receipt identity is invalid")
    files = receipt["files"]
    for item in files:
        if not isinstance(item, dict) or set(item) != {"path", "sha256"}:
            raise IntegrityError("dependency lock file entry is invalid")
        relative_text = item.get("path")
        relative = (
            Path(relative_text) if type(relative_text) is str else Path("..")
        )
        if (
            type(relative_text) is not str
            or relative.is_absolute()
            or ".." in relative.parts
            or sha256_file(root / relative) != item.get("sha256")
        ):
            raise IntegrityError(
                "dependency lock file differs from its accepted receipt"
            )
    runtime = receipt["runtime"]
    packages = runtime.get("packages")
    if (
        set(runtime) != {"implementation", "packages", "platform", "python"}
        or not isinstance(packages, dict)
        or not packages
        or any(type(name) is not str or type(version) is not str for name, version in packages.items())
    ):
        raise IntegrityError("dependency lock runtime schema is invalid")
    return receipt, packages


def locked_environment_mismatches(
    repository_root: Path,
    *,
    executable: Path | None = None,
    implementation: str | None = None,
    platform_name: str | None = None,
    python_version: str | None = None,
    version_lookup: Callable[[str], str] = importlib.metadata.version,
) -> tuple[str, ...]:
    """Return every interpreter or package mismatch against the accepted lock."""

    root = repository_root.resolve(strict=True)
    receipt, packages = _locked_runtime(root)
    runtime = receipt["runtime"]
    active_executable = (
        Path(sys.executable) if executable is None else executable
    ).resolve(strict=True)
    pinned_executable = (root / PINNED_PYTHON).resolve(strict=True)
    observed_implementation = (
        platform.python_implementation()
        if implementation is None
        else implementation
    )
    observed_platform = sys.platform if platform_name is None else platform_name
    observed_python = (
        platform.python_version() if python_version is None else python_version
    )
    mismatches: list[str] = []
    if active_executable != pinned_executable:
        mismatches.append(
            f"interpreter expected={pinned_executable} actual={active_executable}"
        )
    for name, expected, actual in (
        ("implementation", runtime["implementation"], observed_implementation),
        ("platform", runtime["platform"], observed_platform),
        ("python", runtime["python"], observed_python),
    ):
        if actual != expected:
            mismatches.append(f"{name} expected={expected} actual={actual}")
    for package, expected in packages.items():
        try:
            actual = version_lookup(package)
        except importlib.metadata.PackageNotFoundError:
            actual = "<missing>"
        if actual != expected:
            mismatches.append(
                f"package {package} expected={expected} actual={actual}"
            )
    return tuple(mismatches)


def require_locked_repository_environment(repository_root: Path) -> str:
    """Require the exact local interpreter and every accepted package version."""

    root = repository_root.resolve(strict=True)
    receipt, _ = _locked_runtime(root)
    mismatches = locked_environment_mismatches(root)
    if mismatches:
        raise ContractError(
            "repository Python environment differs from the accepted lock: "
            + "; ".join(mismatches)
        )
    return str(receipt["receipt_id"])


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.repository_root.resolve(strict=True)
    receipt_id = require_locked_repository_environment(root)
    print(
        canonical_bytes(
            {
                "dependency_lock_receipt_id": receipt_id,
                "executable": str(Path(sys.executable).resolve(strict=True)),
                "status": "EXACT_LOCK_MATCH",
            }
        ).decode()
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
