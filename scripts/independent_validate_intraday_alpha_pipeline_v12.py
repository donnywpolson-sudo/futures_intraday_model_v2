"""Independent v1.2.3 inactive certification; never activates or reads providers."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ACTIVE = (
    Path("configs/active_alpha_historical_research_design.json"),
    Path("configs/active_alpha_ladder_execution_design.json"),
    Path("configs/active_alpha_research_ladder.json"),
)
V10_ROOT = Path("data/causally_gated_normalized/v10")
VALIDATION_ROOT = Path("state/intraday_research_validation_registry")


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode() + b"\n"


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _contained(relative: Path | str) -> Path:
    path = Path(relative)
    if path.is_absolute():
        raise ValueError("repository-relative path required")
    target = (ROOT / path).resolve(strict=True)
    target.relative_to(ROOT.resolve(strict=True))
    return target


def _load(path: Path, identity: str) -> dict[str, object]:
    raw = _contained(path).read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict) or raw != _canonical(value):
        raise ValueError(f"noncanonical object: {path}")
    core = {key: item for key, item in value.items() if key != identity}
    if value.get(identity) != hashlib.sha256(_canonical(core)[:-1]).hexdigest():
        raise ValueError(f"invalid identity: {path}")
    return value


def _tree_snapshot(root: Path) -> dict[str, object]:
    digest = hashlib.sha256()
    count = size = 0
    files: list[Path] = []
    start = str((ROOT / root).resolve())
    if os.name == "nt" and not start.startswith("\\\\?\\"):
        start = "\\\\?\\" + start
    stack = [start]
    while stack:
        directory = stack.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    stack.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    files.append(Path(entry.path))
    for path in sorted(files):
        display = Path(str(path).removeprefix("\\\\?\\"))
        item = {"path": display.relative_to(ROOT).as_posix(), "sha256": _sha(path), "size": path.stat().st_size}
        digest.update(_canonical(item)[:-1])
        count += 1
        size += int(item["size"])
    return {"file_count": count, "size_bytes": size, "stream_sha256": digest.hexdigest()}


def _run_id(manifest_id: str, ordinal: int) -> str:
    return hashlib.sha256(_canonical({"manifest_id": manifest_id, "run_ordinal": ordinal})[:-1]).hexdigest()


def _run_exact(manifest_path: Path, run_root: Path, ordinal: int) -> dict[str, object]:
    subprocess.run(
        [sys.executable, str(ROOT / "scripts/run_intraday_synthetic_certification_v12.py"), "--manifest", manifest_path.as_posix(), "--run-root", run_root.as_posix(), "--run-ordinal", str(ordinal), "--exact"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.DEVNULL,
    )
    manifest = json.loads(_contained(manifest_path).read_bytes())
    return _load(run_root / _run_id(str(manifest["manifest_id"]), ordinal) / "completed_run.json", "acceptance_id")


def _acceptance_failed(run: dict[str, object]) -> bool:
    return bool(
        run.get("acceptance_passed") is not True
        or run.get("settings", {}).get("null_campaigns") != 10_000
        or run.get("settings", {}).get("known_edge_campaigns") != 5_000
        or run.get("null", {}).get("wilson_upper_95", 1.0) > 0.05
        or run.get("known_edge", {}).get("power", 0.0) < 0.8
        or any(trap.get("passed_promotion_tail_gates") for trap in run.get("gamma_traps", []))
        or run.get("performance", {}).get("limits_passed") is not True
    )


def validate(authority_path: Path, run_root: Path) -> dict[str, object]:
    gates: list[str] = []
    checks: list[str] = []
    active_before = {path.as_posix(): _sha(ROOT / path) for path in ACTIVE}
    v10_before = _tree_snapshot(V10_ROOT)
    authority = _load(authority_path, "authority_id")
    if authority.get("schema_version") != "intraday_research_authority_successor/1.2.3" or authority.get("state") not in {"PREPARED_NOT_ACTIVE", "INACTIVE_CERTIFIED_NOT_ACTIVE"} or any(authority.get("authority", {}).values()):
        gates.append("INACTIVE_AUTHORITY_INVALID")
    else:
        checks.append("inactive_authority")

    failure = _load(Path(authority["failure_record"]["path"]), "failure_id")
    predecessor = failure["predecessor"]
    dense_grid = failure.get("superseded_candidate", {}).get("authority", {})
    if failure.get("state") != "SUPERSEDED_NON_ACTIVATABLE_BLOCKED_REMEDIATION" or predecessor["authority"]["sha256"] != _sha(_contained(predecessor["authority"]["path"])) or predecessor["certificate"]["sha256"] != _sha(_contained(predecessor["certificate"]["path"])) or not isinstance(dense_grid, dict) or not isinstance(dense_grid.get("path"), str) or dense_grid.get("sha256") != _sha(_contained(dense_grid["path"])):
        gates.append("FLAWED_PREDECESSOR_NOT_PERMANENTLY_BLOCKED")
    else:
        checks.append("predecessor_permanent_block")

    program_binding = authority["program"]
    program = _load(Path(program_binding["path"]), "program_id")
    if program_binding["program_id"] != program.get("program_id") or program.get("schema_version") != "intraday_research_program/1.2.3" or program.get("historical_interval") != {"start_inclusive": "2018-01-02T00:00:00Z", "end_exclusive": "2025-07-13T22:00:00Z"}:
        gates.append("PROGRAM_IDENTITY_OR_INTERVAL_INVALID")
    for relative, expected in program.get("bindings", {}).get("implementation", {}).items():
        if _sha(_contained(relative)) != expected:
            gates.append("IMPLEMENTATION_HASH_DRIFT")
            break
    if "IMPLEMENTATION_HASH_DRIFT" not in gates:
        checks.append("implementation_hashes")

    source_binding = authority["source_registration"]
    source = _load(Path(source_binding["path"]), "registration_id")
    completeness = source.get("row_completeness", {})
    if source_binding["registration_id"] != source.get("registration_id") or source.get("schema_version") != "intraday_research_source_registration/1.2.3" or source.get("state") != "ROW_COMPLETE_CERTIFIED_NOT_ACTIVE" or completeness.get("status") != "COMPLETE":
        gates.append("CERTIFIED_SOURCE_REGISTRATION_MISSING")
    else:
        certificate = _load(Path(completeness["certificate"]["path"]), "certificate_id")
        calendar = _load(Path(completeness["calendar_successor"]["path"]), "calendar_id")
        sessions = calendar.get("common_exchange_session_dates", [])
        evidence = certificate.get("source_scan_evidence", {})
        excluded = calendar.get("excluded_dates", [])
        if (
            certificate.get("schema_version") != "intraday_research_row_completeness/1.2.3"
            or calendar.get("schema_version") != "intraday_research_empirical_calendar/1.2.3"
            or certificate.get("status") != "PASS_COMPLETE"
            or certificate.get("common_exchange_session_dates_sha256") != hashlib.sha256(_canonical(sessions)[:-1]).hexdigest()
            or len(sessions) < 1010
            or sessions != sorted(set(sessions))
            or calendar.get("selection_policy") != "COMPLETE_COMMON_SESSIONS_ONLY_EXCLUDE_INCOMPLETE_NEVER_ZERO_RETURN"
            or certificate.get("complete_session_count") != len(sessions)
            or certificate.get("excluded_date_count") != len(excluded)
            or certificate.get("incomplete_session_count") != sum(item.get("reason") == "INCOMPLETE_REGISTERED_SESSION_BOUNDARY_COVERAGE" for item in excluded)
            or not isinstance(evidence, dict)
            or evidence.get("payload_reread") is not False
            or evidence.get("completed_pair_count") != 144
            or not isinstance(evidence.get("checkpoint"), dict)
            or not isinstance(evidence["checkpoint"].get("path"), str)
            or evidence["checkpoint"].get("sha256") != _sha(_contained(evidence["checkpoint"].get("path", "")))
        ):
            gates.append("ROW_COMPLETENESS_OR_CALENDAR_INVALID")
        else:
            checks.append("source_and_row_completeness")

    synthetic_binding = authority["synthetic_acceptance"]
    manifest_path = Path(synthetic_binding["path"])
    manifest = _load(manifest_path, "manifest_id")
    performance = manifest.get("performance_evidence")
    if synthetic_binding["manifest_id"] != manifest.get("manifest_id") or not isinstance(performance, dict) or performance.get("qualifies") is not True:
        gates.append("EXACT_MANIFEST_OR_PERFORMANCE_INVALID")
    else:
        checks.append("manifest_and_performance")

    producer_runs: list[dict[str, object]] = []
    if authority.get("state") == "INACTIVE_CERTIFIED_NOT_ACTIVE":
        bindings = authority.get("certification", {}).get("runs", ())
        if not isinstance(bindings, list) or len(bindings) != 2:
            gates.append("PRODUCER_EXACT_RUN_BINDINGS_MISSING")
        else:
            for binding in bindings:
                run = _load(Path(binding["path"]), "acceptance_id")
                if binding.get("acceptance_id") != run.get("acceptance_id") or binding.get("sha256") != _sha(_contained(binding["path"])) or _acceptance_failed(run):
                    gates.append("PRODUCER_EXACT_ACCEPTANCE_INVALID")
                    break
                producer_runs.append(run)
            if len(producer_runs) == 2:
                certification = authority["certification"]
                if (
                    _canonical(producer_runs[0]["terminal_outcomes"]) != _canonical(producer_runs[1]["terminal_outcomes"])
                    or producer_runs[0]["merkle_digest"] != producer_runs[1]["merkle_digest"]
                    or certification.get("terminal_sha256") != hashlib.sha256(_canonical(producer_runs[0]["terminal_outcomes"])[:-1]).hexdigest()
                    or certification.get("merkle_digest") != producer_runs[0]["merkle_digest"]
                ):
                    gates.append("PRODUCER_EXACT_RUNS_DIFFER")
                else:
                    checks.append("producer_exact_run_bindings")

    if not gates:
        subprocess.run([sys.executable, "-m", "pytest", "tests/test_intraday_alpha_pipeline_v12.py", "-m", "high_risk", "-q"], cwd=ROOT, check=True, stdout=subprocess.DEVNULL)
        first = _run_exact(manifest_path, run_root, 1)
        second = _run_exact(manifest_path, run_root, 2)
        if (
            _canonical(first["terminal_outcomes"]) != _canonical(second["terminal_outcomes"])
            or first["merkle_digest"] != second["merkle_digest"]
            or (producer_runs and (_canonical(first["terminal_outcomes"]) != _canonical(producer_runs[0]["terminal_outcomes"]) or first["merkle_digest"] != producer_runs[0]["merkle_digest"]))
        ):
            gates.append("FRESH_PROCESS_TERMINALS_OR_MERKLE_DIFFER")
        for run in (first, second):
            if _acceptance_failed(run):
                gates.append("EXACT_ACCEPTANCE_GATE_FAILED")
                break
        if not gates:
            checks.extend(["materializer_and_boundaries", "two_fresh_exact_runs", "terminal_and_merkle_identity"])

    if {path.as_posix(): _sha(ROOT / path) for path in ACTIVE} != active_before:
        gates.append("ACTIVE_POINTER_BYTES_CHANGED")
    if _tree_snapshot(V10_ROOT) != v10_before:
        gates.append("V10_TREE_CHANGED")
    if not {"ACTIVE_POINTER_BYTES_CHANGED", "V10_TREE_CHANGED"} & set(gates):
        checks.append("protected_snapshots_unchanged")
    gates.extend(gate for gate in authority.get("certification", {}).get("unsatisfied_gates", ()) if gate not in gates)
    core = {
        "schema_version": "intraday_research_independent_validation/1.2.3",
        "status": "PASS_INACTIVE_NOT_ACTIVE" if not gates else "BLOCKED_NOT_CERTIFIED",
        "authority_id": authority["authority_id"],
        "checks": checks,
        "unsatisfied_certification_gates": sorted(set(gates)),
        "unsatisfied_activation_gates": list(authority.get("activation", {}).get("unsatisfied_gates", ())),
        "active_pointer_snapshot": active_before,
        "v10_snapshot": v10_before,
        "protected_access_performed": False,
        "producer_flags_trusted": False,
    }
    return {**core, "validation_id": hashlib.sha256(_canonical(core)[:-1]).hexdigest()}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--authority", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--write-certificate", action="store_true")
    args = parser.parse_args()
    report = validate(args.authority, args.run_root)
    if args.write_certificate:
        if report["unsatisfied_certification_gates"]:
            raise SystemExit("certificate refused: unsatisfied certification gates remain")
        path = ROOT / VALIDATION_ROOT / str(report["validation_id"]) / "certificate.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        expected = _canonical(report)
        try:
            with path.open("xb") as stream:
                stream.write(expected)
        except FileExistsError:
            if path.read_bytes() != expected:
                raise SystemExit("certificate identity collision")
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
