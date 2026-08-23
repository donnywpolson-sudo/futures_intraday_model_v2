"""Command-line entry point for the non-authorizing v3 master audit."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from futures_rebuild.audit.contract import AuditContractError, run_audit
from futures_rebuild.canonical import canonical_bytes, contained_path


def render_human_report(report: dict[str, object]) -> str:
    """Render the minimal human companion without changing machine semantics."""

    decision = report.get("target_state_decision", "UNKNOWN")
    target = report.get("target_state", "UNRESOLVED")
    lines = [
        "# Futures v2 master audit",
        "",
        f"Decision: {decision}",
        f"Target state: {target}",
        "Authority: evidence classification only; no readiness or trading authority.",
    ]
    gates = report.get("gate_statuses")
    if isinstance(gates, dict):
        lines.extend(["", "Gate statuses:"])
        lines.extend(f"- {gate_id}: {status}" for gate_id, status in gates.items())
    if "error" in report:
        lines.extend(["", f"Precheck error: {report['error']}"])
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Classify frozen v2 audit evidence")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--invocation", type=Path, required=True)
    parser.add_argument("--output", help="optional repo-relative JSON output path")
    parser.add_argument("--format", choices=("json", "human"), default="json")
    args = parser.parse_args(argv)
    root = args.repo_root.resolve(strict=True)
    invocation_path = args.invocation.resolve(strict=True)
    try:
        invocation_path.relative_to(root)
        payload = json.loads(invocation_path.read_text(encoding="utf-8"))
        report = run_audit(root, payload)
    except (AuditContractError, OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        report = {
            "classification": "NON_AUTHORIZING_EVIDENCE_CLASSIFICATION",
            "target_state_decision": "PRECHECK_ERROR",
            "logical_exit_code": 12,
            "error": str(exc),
        }
    encoded = (
        canonical_bytes(report) + b"\n"
        if args.format == "json"
        else render_human_report(report).encode("utf-8")
    )
    if args.output:
        if not args.output.replace("\\", "/").startswith("reports/audits/"):
            parser.error("--output must be beneath reports/audits")
        destination = contained_path(root, args.output)
        if destination.exists():
            parser.error("--output refuses to overwrite an existing artifact")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb") as handle:
            handle.write(encoded)
    else:
        sys.stdout.buffer.write(encoded)
    return int(report["logical_exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
