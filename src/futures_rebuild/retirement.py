"""Evidence-only standalone and legacy-retirement classifier."""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import tomllib
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import ContractError


SCHEMA_VERSION = "standalone_retirement_audit/1.0.0"
EXPECTED_DBNS = 4491
EXPECTED_SIDECARS = 4491
EXPECTED_FILES = 8982
EXPECTED_BYTES = 25_592_717_852
EXPECTED_MARKETS = 41
PROHIBITED_PUBLIC_SCRIPTS = frozenset(
    {
        "futures-migrate",
        "futures-legacy-census",
        "futures-successor-inventory",
        "futures-successor-migrate",
        "futures-layout-migration",
        "futures-dbn-flat-layout",
        "futures-dbn-flat-cutover",
    }
)
PROHIBITED_IMPORT_ROOTS = frozenset(
    {"live_cockpit", "live_ops", "live_chart_feed"}
)
OPERATIONAL_PATHS = (
    "AGENTS.md",
    "PROJECT_OUTLINE.md",
    "README.md",
    "MASTER_AUDIT.md",
    "META_MASTER_AUDIT.md",
    "configs/alpha_tiered.yaml",
    "configs/research_universe_contract.json",
    "configs/source_contract.json",
    "src/futures_rebuild/audit",
    "src/futures_rebuild/live_cockpit",
    "src/futures_rebuild/pipeline.py",
    "src/futures_rebuild/profiles.py",
    "src/futures_rebuild/source_contract.py",
)
FINAL_AUDIT_REPORTS = {
    "FOUNDATION_READY": "reports/audits/final/foundation_ready.json",
    "HISTORICAL_RESEARCH_READY": (
        "reports/audits/final/historical_research_ready.json"
    ),
    "OBSERVATION_COCKPIT_READY": (
        "reports/audits/final/observation_cockpit_ready.json"
    ),
}
META_REPORT = "reports/audits/final/meta_master_audit.json"
_LEGACY_ABSOLUTE = re.compile(
    r"(?i)c:\\users\\donny\\desktop\\futures_intraday_model(?!_v2)"
)


class RetirementAuditError(ContractError):
    """The standalone audit input is malformed."""


def _load_object(path: Path, name: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RetirementAuditError(f"{name} is not readable JSON") from exc
    if type(payload) is not dict:
        raise RetirementAuditError(f"{name} must be an object")
    return payload


def _check(
    check_id: str,
    passed: bool,
    *,
    evidence: Iterable[str] = (),
    reason: str,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "FAIL",
        "reason": reason,
        "evidence": sorted(set(evidence)),
    }


def _iter_files(root: Path, relatives: Iterable[str]) -> Iterable[Path]:
    for relative in relatives:
        path = root / relative
        if path.is_file():
            yield path
        elif path.is_dir():
            yield from sorted(item for item in path.rglob("*") if item.is_file())


def _operational_path_scan(root: Path) -> tuple[bool, list[str]]:
    offenders: list[str] = []
    for path in _iter_files(root, OPERATIONAL_PATHS):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeError:
            continue
        if _LEGACY_ABSOLUTE.search(text):
            offenders.append(path.relative_to(root).as_posix())
    return not offenders, offenders


def _import_scan(root: Path) -> tuple[bool, list[str]]:
    offenders: list[str] = []
    for path in _iter_files(
        root,
        (
            "src/futures_rebuild/live_cockpit",
            "src/futures_rebuild/pipeline.py",
            "src/futures_rebuild/profiles.py",
        ),
    ):
        if path.suffix != ".py":
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, UnicodeError, SyntaxError) as exc:
            raise RetirementAuditError(f"cannot parse operational module: {path}") from exc
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                names.append(node.module)
            for name in names:
                if name.split(".", 1)[0] in PROHIBITED_IMPORT_ROOTS:
                    offenders.append(
                        f"{path.relative_to(root).as_posix()}:{node.lineno}:{name}"
                    )
    return not offenders, sorted(offenders)


def _universe_markets(universe: Mapping[str, Any]) -> set[str]:
    markets: set[str] = set()
    tiers = universe.get("tiers")
    if type(tiers) is not list:
        return markets
    for tier in tiers:
        if type(tier) is dict and type(tier.get("symbols")) is list:
            markets.update(
                symbol
                for symbol in tier["symbols"]
                if type(symbol) is str and symbol
            )
    return markets


def _git_clean(root: Path) -> tuple[bool, str]:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return False, f"GIT_STATUS_ERROR:{type(exc).__name__}"
    if result.returncode != 0:
        return False, "GIT_STATUS_FAILED"
    return not result.stdout.strip(), "CLEAN" if not result.stdout.strip() else "DIRTY"


def _audit_report_check(
    root: Path, target: str, relative: str
) -> tuple[bool, str]:
    path = root / relative
    if not path.is_file():
        return False, "FINAL_AUDIT_REPORT_MISSING"
    try:
        payload = _load_object(path, f"{target} audit report")
    except RetirementAuditError:
        return False, "FINAL_AUDIT_REPORT_INVALID"
    if (
        payload.get("target_state") != target
        or payload.get("target_state_decision") != "SUPPORTABLE"
        or payload.get("logical_exit_code") != 0
        or payload.get("authority", {}).get("authorizes_trading") is not False
    ):
        return False, "FINAL_AUDIT_NOT_SUPPORTABLE"
    return True, "FINAL_AUDIT_SUPPORTABLE"


def scan_retirement_readiness(repository_root: Path) -> dict[str, Any]:
    """Classify current v2 state without resolving or opening any legacy path."""

    root = repository_root.resolve(strict=True)
    source_path = root / "configs" / "source_contract.json"
    universe_path = root / "configs" / "research_universe_contract.json"
    inventory_path = root / "reports" / "migration" / "legacy_retirement_inventory.json"
    source = _load_object(source_path, "source contract")
    universe = _load_object(universe_path, "research universe")
    inventory = _load_object(inventory_path, "legacy retirement inventory")
    checks: list[dict[str, Any]] = []

    source_release = source.get("canonical_dbn_release")
    source_ready = (
        source.get("legacy_repository") is None
        and source.get("external_repository_access") == "FORBIDDEN"
        and type(source_release) is dict
        and source_release.get("dbn_files") == EXPECTED_DBNS
        and source_release.get("sidecar_files") == EXPECTED_SIDECARS
        and source_release.get("combined_files") == EXPECTED_FILES
        and source_release.get("combined_bytes") == EXPECTED_BYTES
    )
    checks.append(
        _check(
            "SOURCE_CONTRACT_STANDALONE_41_MARKET",
            source_ready,
            evidence=["configs/source_contract.json"],
            reason=(
                "SUCCESSOR_SOURCE_IS_V2_LOCAL_AND_EXTERNAL_ACCESS_FORBIDDEN"
                if source_ready
                else "SUCCESSOR_SOURCE_OR_EXTERNAL_ACCESS_CLOSURE_INCOMPLETE"
            ),
        )
    )

    markets = _universe_markets(universe)
    universe_ready = (
        universe.get("status") == "APPROVED"
        and type(universe.get("approval_receipt_id")) is str
        and len(str(universe.get("approval_receipt_id"))) == 64
        and len(markets) == EXPECTED_MARKETS
    )
    checks.append(
        _check(
            "APPROVED_41_MARKET_UNIVERSE",
            universe_ready,
            evidence=["configs/research_universe_contract.json"],
            reason=(
                "UNIVERSE_APPROVED_AND_COMPLETE"
                if universe_ready
                else "UNIVERSE_PENDING_OR_INCOMPLETE"
            ),
        )
    )

    clean_paths, path_offenders = _operational_path_scan(root)
    checks.append(
        _check(
            "NO_OPERATIONAL_LEGACY_ABSOLUTE_PATH",
            clean_paths,
            evidence=OPERATIONAL_PATHS,
            reason=(
                "NO_OPERATIONAL_LEGACY_PATH"
                if clean_paths
                else "LEGACY_PATHS:" + ",".join(path_offenders)
            ),
        )
    )

    clean_imports, import_offenders = _import_scan(root)
    checks.append(
        _check(
            "NO_LEGACY_RUNTIME_IMPORTS",
            clean_imports,
            evidence=[
                "src/futures_rebuild/live_cockpit",
                "src/futures_rebuild/pipeline.py",
                "src/futures_rebuild/profiles.py",
            ],
            reason=(
                "RUNTIME_IMPORTS_ARE_V2_OWNED"
                if clean_imports
                else "LEGACY_IMPORTS:" + ",".join(import_offenders)
            ),
        )
    )

    pyproject = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    scripts = set(pyproject.get("project", {}).get("scripts", {}))
    public_ready = not scripts.intersection(PROHIBITED_PUBLIC_SCRIPTS)
    checks.append(
        _check(
            "NO_PUBLIC_MIGRATION_OR_LEGACY_ENTRYPOINTS",
            public_ready,
            evidence=["pyproject.toml"],
            reason=(
                "PUBLIC_INTERFACES_ARE_STEADY_STATE"
                if public_ready
                else "PROHIBITED_SCRIPTS:"
                + ",".join(sorted(scripts.intersection(PROHIBITED_PUBLIC_SCRIPTS)))
            ),
        )
    )

    ignore_lines = {
        line.strip()
        for line in (root / ".gitignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    secret_ready = {"api.env", "databento.env", ".env", ".env.*"}.issubset(
        ignore_lines
    )
    checks.append(
        _check(
            "LOCAL_SECRET_IGNORE_BOUNDARY",
            secret_ready,
            evidence=[".gitignore"],
            reason=(
                "LOCAL_SECRET_NAMES_IGNORED"
                if secret_ready
                else "LOCAL_SECRET_IGNORE_RULES_INCOMPLETE"
            ),
        )
    )

    entries = inventory.get("entries")
    inventory_ready = (
        type(entries) is list
        and len(entries) == 41
        and not any(
            type(item) is not dict
            or item.get("classification") == "UNRESOLVED"
            for item in entries
        )
        and inventory.get("overall_state") == "LEGACY_RETIREMENT_READY"
    )
    checks.append(
        _check(
            "LEGACY_TOP_LEVEL_RETENTION_CLOSED",
            inventory_ready,
            evidence=[
                "reports/migration/legacy_retirement_inventory.json"
            ],
            reason=(
                "ALL_LEGACY_TOP_LEVEL_PATHS_CLASSIFIED_AND_CLOSED"
                if inventory_ready
                else "RETENTION_CONDITIONS_REMAIN_OPEN"
            ),
        )
    )

    for target, relative in FINAL_AUDIT_REPORTS.items():
        passed, reason = _audit_report_check(root, target, relative)
        checks.append(
            _check(
                f"MASTER_AUDIT_{target}",
                passed,
                evidence=[relative],
                reason=reason,
            )
        )

    meta_path = root / META_REPORT
    meta_ready = False
    if meta_path.is_file():
        try:
            meta = _load_object(meta_path, "final meta audit")
            meta_ready = (
                meta.get("classification") == "SUPPORTABLE"
                and meta.get("unresolved_critical_high_count") == 0
                and meta.get("unresolved_p0_p1_count") == 0
            )
        except RetirementAuditError:
            meta_ready = False
    checks.append(
        _check(
            "META_AUDIT_PROMPT_QUALITY",
            meta_ready,
            evidence=[META_REPORT],
            reason=(
                "META_AUDIT_SUPPORTABLE_NO_HIGH_GAPS"
                if meta_ready
                else "META_AUDIT_CLOSURE_MISSING"
            ),
        )
    )

    git_clean, git_reason = _git_clean(root)
    checks.append(
        _check(
            "CLEAN_COMMITTED_HEAD",
            git_clean,
            evidence=[".git"],
            reason=git_reason,
        )
    )

    ready = all(item["status"] == "PASS" for item in checks)
    core = {
        "schema_version": SCHEMA_VERSION,
        "classification": (
            "LEGACY_RETIREMENT_READY"
            if ready
            else "LEGACY_RETIREMENT_BLOCKED"
        ),
        "standalone_runtime_ready": ready,
        "legacy_root_opened": False,
        "legacy_delete_authorized": False,
        "checks": checks,
        "authority": {
            "provider_calls_authorized": False,
            "research_authorized": False,
            "holdout_access_authorized": False,
            "trading_authorized": False,
            "legacy_delete_authorized": False,
        },
    }
    return {**core, "report_id": sha256_json(core)}


def _write_new(path: Path, payload: Mapping[str, Any]) -> None:
    if path.exists():
        raise RetirementAuditError(f"refusing to overwrite output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(canonical_bytes(dict(payload)))
        handle.write(b"\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Classify standalone and legacy-retirement evidence"
    )
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        report = scan_retirement_readiness(args.repository_root)
        if args.output:
            output = args.output
            if output.is_absolute():
                raise RetirementAuditError("output must be repository-relative")
            _write_new(
                args.repository_root.resolve(strict=True) / output,
                report,
            )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["standalone_runtime_ready"] else 11
    except (RetirementAuditError, OSError, ValueError) as exc:
        print(json.dumps({"classification": "PRECHECK_ERROR", "error": str(exc)}))
        return 12


if __name__ == "__main__":
    raise SystemExit(main())
