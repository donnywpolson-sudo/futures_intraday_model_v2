"""Machine-checkable blind-first quality audit for the root Master Audit."""

from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Mapping, Sequence

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import ContractError


COVERAGE_SCHEMA = "meta_master_audit_coverage/1.0.0"
EVIDENCE_SCHEMA = "meta_master_audit_test_evidence/1.0.0"
REPORT_SCHEMA = "meta_master_audit_report/1.0.0"
_HASH = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
_ALLOWED_SEVERITY = {"Critical", "High", "Medium", "Low"}
_ALLOWED_PRIORITY = {"P0", "P1", "P2", "P3"}


class MetaAuditError(ContractError):
    """The Meta Audit coverage or evidence contract is invalid."""


def _load_object(path: Path, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise MetaAuditError(f"{name} is not readable JSON") from exc
    if type(value) is not dict:
        raise MetaAuditError(f"{name} must be an object")
    return value


def _test_functions(path: Path) -> set[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, UnicodeError, SyntaxError) as exc:
        raise MetaAuditError(f"cannot parse mapped test file: {path}") from exc
    return {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    }


def _matrix_subchecks(matrix: Mapping[str, Any]) -> set[str]:
    gates = matrix.get("gates")
    if type(gates) is not list:
        raise MetaAuditError("stage matrix gates are invalid")
    return {
        str(check["subcheck_id"])
        for gate in gates
        if type(gate) is dict and type(gate.get("subchecks")) is list
        for check in gate["subchecks"]
        if type(check) is dict and type(check.get("subcheck_id")) is str
    }


def _git_head(root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    value = result.stdout.strip()
    return value if result.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}", value) else None


def _validate_suite_evidence(
    root: Path,
    payload: Mapping[str, Any],
    *,
    required_test_files: set[str],
) -> tuple[bool, str]:
    expected_keys = {
        "schema_version",
        "status",
        "command",
        "git_head",
        "passed",
        "failed",
        "errors",
        "skipped",
        "test_file_sha256",
        "evidence_id",
    }
    if set(payload) != expected_keys:
        return False, "SUITE_EVIDENCE_FIELDS_INVALID"
    core = {key: payload[key] for key in payload if key != "evidence_id"}
    hashes = payload.get("test_file_sha256")
    if (
        payload.get("schema_version") != EVIDENCE_SCHEMA
        or payload.get("status") != "PASS"
        or payload.get("command")
        != (
            ".\\.venv\\Scripts\\python.exe -m pytest -q "
            "--junitxml=.pytest_tmp/full-suite.xml"
        )
        or payload.get("git_head") != _git_head(root)
        or type(payload.get("passed")) is not int
        or payload.get("passed", 0) < 1
        or payload.get("failed") != 0
        or payload.get("errors") != 0
        or type(payload.get("skipped")) is not int
        or type(hashes) is not dict
        or set(hashes) != required_test_files
        or payload.get("evidence_id") != sha256_json(core)
    ):
        return False, "SUITE_EVIDENCE_IDENTITY_OR_RESULT_INVALID"
    for relative, expected in hashes.items():
        if type(expected) is not str or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
            return False, "SUITE_TEST_HASH_INVALID"
        path = root / relative
        if not path.is_file() or sha256_file(path) != expected:
            return False, "SUITE_TEST_FILE_DRIFT"
    return True, "FULL_SUITE_EVIDENCE_VERIFIED"


def _required_test_files(coverage: Mapping[str, Any]) -> set[str]:
    controls = coverage.get("controls")
    if type(controls) is not list:
        raise MetaAuditError("coverage controls are missing")
    result: set[str] = set()
    for control in controls:
        if type(control) is not dict or type(control.get("tests")) is not list:
            raise MetaAuditError("coverage test mappings are invalid")
        for node_id in control["tests"]:
            if type(node_id) is not str or "::" not in node_id:
                raise MetaAuditError("coverage test node is invalid")
            result.add(node_id.split("::", 1)[0])
    result.add("tests/conftest.py")
    return result


def build_suite_evidence(
    repository_root: Path,
    *,
    junit_xml_path: Path,
) -> dict[str, Any]:
    """Build an exact suite receipt from pytest's machine-generated JUnit XML."""

    root = repository_root.resolve(strict=True)
    coverage = _load_object(
        root / "configs" / "meta_master_audit_coverage.json",
        "Meta Audit coverage",
    )
    resolved_junit = junit_xml_path.resolve(strict=True)
    try:
        junit_relative = resolved_junit.relative_to(root).as_posix()
    except ValueError:
        junit_relative = "[external]"
    try:
        xml_root = ET.parse(resolved_junit).getroot()
    except (OSError, ET.ParseError) as exc:
        raise MetaAuditError("JUnit evidence is not readable XML") from exc
    suites = (
        [xml_root]
        if xml_root.tag.rsplit("}", 1)[-1] == "testsuite"
        else [
            item
            for item in xml_root
            if item.tag.rsplit("}", 1)[-1] == "testsuite"
        ]
    )
    if not suites:
        raise MetaAuditError("JUnit evidence contains no test suite")

    def total(attribute: str) -> int:
        try:
            return sum(int(suite.attrib.get(attribute, "0")) for suite in suites)
        except ValueError as exc:
            raise MetaAuditError("JUnit counts are invalid") from exc

    tests = total("tests")
    failures = total("failures")
    errors = total("errors")
    skipped = total("skipped")
    passed = tests - failures - errors - skipped
    if tests < 1 or min(passed, failures, errors, skipped) < 0:
        raise MetaAuditError("JUnit count closure is invalid")
    head = _git_head(root)
    if head is None:
        raise MetaAuditError("Git HEAD cannot be resolved")
    required_files = _required_test_files(coverage)
    core = {
        "schema_version": EVIDENCE_SCHEMA,
        "status": "PASS" if failures == 0 and errors == 0 else "FAIL",
        "command": (
            r".\.venv\Scripts\python.exe -m pytest -q "
            f"--junitxml={junit_relative}"
        ),
        "git_head": head,
        "passed": passed,
        "failed": failures,
        "errors": errors,
        "skipped": skipped,
        "test_file_sha256": {
            relative: sha256_file(root / relative)
            for relative in sorted(required_files)
        },
    }
    return {**core, "evidence_id": sha256_json(core)}


def run_meta_audit(
    repository_root: Path,
    *,
    suite_evidence_path: Path | None = None,
) -> dict[str, Any]:
    root = repository_root.resolve(strict=True)
    coverage_path = root / "configs" / "meta_master_audit_coverage.json"
    master_path = root / "MASTER_AUDIT.md"
    meta_path = root / "META_MASTER_AUDIT.md"
    matrix_path = root / "configs" / "master_audit_v3" / "stage_requirement_matrix.json"
    coverage = _load_object(coverage_path, "Meta Audit coverage")
    matrix = _load_object(matrix_path, "stage matrix")
    master = " ".join(
        master_path.read_text(encoding="utf-8").split()
    ).casefold()
    meta = " ".join(meta_path.read_text(encoding="utf-8").split()).casefold()
    if (
        set(coverage)
        != {
            "schema_version",
            "classification",
            "derived_before_master_review",
            "derivation_sources",
            "controls",
        }
        or coverage.get("schema_version") != COVERAGE_SCHEMA
        or coverage.get("classification") != "INDEPENDENT_BLIND_COVERAGE_STANDARD"
        or coverage.get("derived_before_master_review") is not True
        or "master_audit.md" in {
            str(item).casefold() for item in coverage.get("derivation_sources", [])
        }
    ):
        raise MetaAuditError("coverage standard is not independently blind-first")
    required_meta_terms = (
        "before reading `master_audit.md` closely",
        "only then read the master audit",
        "no unresolved critical/high or p0/p1 item remains",
        "false-pass",
    )
    meta_contract_pass = all(term in meta for term in required_meta_terms)
    known_subchecks = _matrix_subchecks(matrix)
    controls = coverage.get("controls")
    if type(controls) is not list or not controls:
        raise MetaAuditError("coverage controls are missing")
    normalized: list[dict[str, Any]] = []
    required_test_files: set[str] = set()
    seen: set[str] = set()
    for raw in controls:
        if type(raw) is not dict or set(raw) != {
            "control_id",
            "severity",
            "priority",
            "threat",
            "master_terms",
            "matrix_subchecks",
            "tests",
        }:
            raise MetaAuditError("coverage control fields are invalid")
        control_id = raw["control_id"]
        if (
            type(control_id) is not str
            or control_id in seen
            or raw["severity"] not in _ALLOWED_SEVERITY
            or raw["priority"] not in _ALLOWED_PRIORITY
            or type(raw["threat"]) is not str
            or not raw["threat"]
        ):
            raise MetaAuditError("coverage control identity is invalid")
        seen.add(control_id)
        terms = raw["master_terms"]
        subchecks = raw["matrix_subchecks"]
        tests = raw["tests"]
        if (
            type(terms) is not list
            or not terms
            or type(subchecks) is not list
            or not subchecks
            or type(tests) is not list
            or not tests
        ):
            raise MetaAuditError(f"{control_id} coverage mappings are empty")
        missing_terms = [
            term for term in terms
            if type(term) is not str or term.casefold() not in master
        ]
        missing_subchecks = [
            item for item in subchecks if item not in known_subchecks
        ]
        missing_tests: list[str] = []
        for node_id in tests:
            if type(node_id) is not str or "::" not in node_id:
                missing_tests.append(str(node_id))
                continue
            relative, function_name = node_id.split("::", 1)
            required_test_files.add(relative)
            path = root / relative
            if not path.is_file() or function_name not in _test_functions(path):
                missing_tests.append(node_id)
        passed = (
            not missing_terms
            and not missing_subchecks
            and not missing_tests
        )
        normalized.append(
            {
                "control_id": control_id,
                "severity": raw["severity"],
                "priority": raw["priority"],
                "status": "PASS" if passed else "FAIL",
                "missing_master_terms": missing_terms,
                "missing_matrix_subchecks": missing_subchecks,
                "missing_test_nodes": missing_tests,
            }
        )
    required_test_files.add("tests/conftest.py")

    suite_pass = False
    suite_reason = "FULL_SUITE_EVIDENCE_NOT_SUPPLIED"
    suite_evidence_id = None
    if suite_evidence_path is not None:
        suite = _load_object(
            suite_evidence_path.resolve(strict=True), "full-suite evidence"
        )
        suite_pass, suite_reason = _validate_suite_evidence(
            root, suite, required_test_files=required_test_files
        )
        suite_evidence_id = suite.get("evidence_id")

    unresolved_high = sum(
        item["status"] != "PASS"
        and item["severity"] in {"Critical", "High"}
        for item in normalized
    )
    unresolved_p0_p1 = sum(
        item["status"] != "PASS"
        and item["priority"] in {"P0", "P1"}
        for item in normalized
    )
    structural_pass = (
        meta_contract_pass
        and unresolved_high == 0
        and unresolved_p0_p1 == 0
    )
    classification = (
        "SUPPORTABLE"
        if structural_pass and suite_pass
        else "BLOCKED"
        if not structural_pass
        else "INSUFFICIENT_EVIDENCE"
    )
    core = {
        "schema_version": REPORT_SCHEMA,
        "classification": classification,
        "blind_first_contract_pass": meta_contract_pass,
        "coverage_standard_sha256": sha256_file(coverage_path),
        "master_audit_sha256": sha256_file(master_path),
        "meta_master_audit_sha256": sha256_file(meta_path),
        "stage_matrix_sha256": sha256_file(matrix_path),
        "controls": normalized,
        "unresolved_critical_high_count": unresolved_high,
        "unresolved_p0_p1_count": unresolved_p0_p1,
        "suite_evidence_status": "PASS" if suite_pass else "MISSING_OR_INVALID",
        "suite_evidence_reason": suite_reason,
        "suite_evidence_id": suite_evidence_id,
        "authority": {
            "publishes_readiness": False,
            "authorizes_provider_calls": False,
            "authorizes_research": False,
            "authorizes_holdout_access": False,
            "authorizes_trading": False,
        },
    }
    return {**core, "report_id": sha256_json(core)}


def _write_new(path: Path, payload: Mapping[str, Any]) -> None:
    if path.exists():
        raise MetaAuditError(f"refusing to overwrite output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(canonical_bytes(dict(payload)))
        handle.write(b"\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument("--suite-evidence", type=Path)
    parser.add_argument("--junitxml", type=Path)
    parser.add_argument("--suite-evidence-output", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        suite_evidence = args.suite_evidence
        if args.junitxml is not None:
            if args.suite_evidence is not None or args.suite_evidence_output is None:
                raise MetaAuditError(
                    "--junitxml requires --suite-evidence-output and cannot "
                    "be combined with --suite-evidence"
                )
            if args.suite_evidence_output.is_absolute():
                raise MetaAuditError(
                    "suite evidence output must be repository-relative"
                )
            suite_evidence = (
                args.repository_root.resolve(strict=True)
                / args.suite_evidence_output
            )
            _write_new(
                suite_evidence,
                build_suite_evidence(
                    args.repository_root,
                    junit_xml_path=args.junitxml,
                ),
            )
        elif args.suite_evidence_output is not None:
            raise MetaAuditError(
                "--suite-evidence-output is valid only with --junitxml"
            )
        report = run_meta_audit(
            args.repository_root, suite_evidence_path=suite_evidence
        )
        if args.output:
            if args.output.is_absolute():
                raise MetaAuditError("output must be repository-relative")
            _write_new(
                args.repository_root.resolve(strict=True) / args.output,
                report,
            )
        print(json.dumps(report, indent=2, sort_keys=True))
        return {
            "SUPPORTABLE": 0,
            "BLOCKED": 10,
            "INSUFFICIENT_EVIDENCE": 11,
        }[report["classification"]]
    except (MetaAuditError, OSError, ValueError) as exc:
        print(json.dumps({"classification": "PRECHECK_ERROR", "error": str(exc)}))
        return 12


if __name__ == "__main__":
    raise SystemExit(main())
