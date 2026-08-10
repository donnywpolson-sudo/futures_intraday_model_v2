"""Seal the consumed v2 zero-decode path-length failure without row access."""

from __future__ import annotations

import json
import stat
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
for candidate in (ROOT, ROOT / "src"):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json  # noqa: E402
from futures_rebuild.errors import IntegrityError  # noqa: E402


PLAN_PATH = Path("configs/apex_micro_phase1b2_historical_execution_plan_v2.json")
AUDIT_PATH = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v2/audit.json"
)
TERMINAL_PATH = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_execution_v2/"
    "92c3ff618ae1b4face18b280a9d2c619a1007e55bbf9e3d449647d13e444eacb/"
    "terminal.json"
)
AUTHORIZATION_USE_PATH = Path(
    "state/authorization_uses/"
    "2de5cf5722d3755774f8b31d110b40009fefb64412334b623c58b8db857b4fba.json"
)
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1b2_execution_plan_v2_supersession/report.json"
)
PLAN_ID = "74bba15b53cea8047c98db3941c63418453e13871dad09870834fe49f6931432"
PLAN_SHA256 = "f682e738c2188640a46d6653f39732565268edb20221f1b910f54d570a7c6ce9"
AUDIT_ID = "48fabd2465d6755fd3531f983b6f4380f3669ecf41dbd8d4602b70b05137aceb"
AUDIT_SHA256 = "8b3522442b80cf5f9ef4a7b0e3b94c3f063a96535a663bad6177d0f6ffca54e6"
TERMINAL_ID = "0655f29912bd8b14d4baf465d798304d619b82d4c050d3e5ddcc605502834a1b"
TERMINAL_SHA256 = "2f2b6b4ee5dbdef47544cb434b2cece9b32ed237fad0b442ab8ab7f0e57af2f9"
AUTHORIZATION_RECEIPT_ID = (
    "2de5cf5722d3755774f8b31d110b40009fefb64412334b623c58b8db857b4fba"
)
IMPLEMENTATION_HEAD = "a3dcd8671fc3a69e5b38515ddc176588e36f1a53"
WINDOWS_LEGACY_MAX_PATH_CHARS = 260


def _load(path: Path, description: str) -> dict[str, object]:
    value = json.loads((ROOT / path).read_text(encoding="utf-8"))
    if type(value) is not dict:
        raise IntegrityError(f"{description} is invalid")
    return value


def build_report() -> dict[str, object]:
    plan = _load(PLAN_PATH, "v2 plan")
    audit = _load(AUDIT_PATH, "v2 audit")
    terminal = _load(TERMINAL_PATH, "v2 terminal")
    authorization_use = _load(AUTHORIZATION_USE_PATH, "v2 authorization use")
    if (
        plan.get("plan_id") != PLAN_ID
        or sha256_file(ROOT / PLAN_PATH) != PLAN_SHA256
        or audit.get("audit_id") != AUDIT_ID
        or sha256_file(ROOT / AUDIT_PATH) != AUDIT_SHA256
        or terminal.get("terminal_id") != TERMINAL_ID
        or sha256_file(ROOT / TERMINAL_PATH) != TERMINAL_SHA256
        or terminal.get("authorization_receipt_id") != AUTHORIZATION_RECEIPT_ID
        or authorization_use.get("receipt_id") != AUTHORIZATION_RECEIPT_ID
        or plan.get("implementation_head") != IMPLEMENTATION_HEAD
    ):
        raise IntegrityError("v2 Phase 1B/2 failure binding drifted")
    first = plan["sources"][0]
    first_partial = (
        ROOT / str(plan["staging_root"]) / str(first["phase1b_output_path"])
    )
    first_partial_chars = len(str(first_partial.resolve(strict=False))) + len(".partial")
    staging = ROOT / str(plan["staging_root"])
    staging_files = [path for path in staging.rglob("*") if path.is_file()]
    staging_directories = [path for path in staging.rglob("*") if path.is_dir()]
    if (
        terminal.get("state") != "FAILURE_INACTIVE_PARTIAL_EVIDENCE_PRESERVED"
        or terminal.get("failure") != {"exception_type": "IntegrityError"}
        or terminal.get("source_hashes_verified_before_decode") != 120
        or terminal.get("completed_decode_count") != 0
        or terminal.get("completed_phase2_count") != 0
        or terminal.get("created_output_bytes") != 0
        or terminal.get("year_2025_or_2026_payloads_opened") != 0
        or first_partial_chars <= WINDOWS_LEGACY_MAX_PATH_CHARS
        or staging_files
        or not staging_directories
    ):
        raise IntegrityError("v2 zero-decode failure evidence differs")
    core = {
        "schema_version": "apex_micro_phase1b2_execution_failure/2.0.0",
        "state": "SUPERSEDED_FAIL_CLOSED_AFTER_HASH_CENSUS_BEFORE_DECODE_COMPLETION",
        "plan_id": PLAN_ID,
        "plan_sha256": PLAN_SHA256,
        "audit_id": AUDIT_ID,
        "audit_sha256": AUDIT_SHA256,
        "terminal_id": TERMINAL_ID,
        "terminal_sha256": TERMINAL_SHA256,
        "authorization_receipt_id": AUTHORIZATION_RECEIPT_ID,
        "authorization_use_sha256": sha256_file(ROOT / AUTHORIZATION_USE_PATH),
        "implementation_head": IMPLEMENTATION_HEAD,
        "authorization_receipt_consumed": True,
        "attempts": 1,
        "automatic_retries": 0,
        "source_hashes_verified": 120,
        "dbn_rows_decoded": 0,
        "completed_phase1b_outputs": 0,
        "completed_phase2_outputs": 0,
        "created_output_bytes": 0,
        "staging_file_count": 0,
        "staging_directory_count": len(staging_directories),
        "failure_type_recorded": "IntegrityError",
        "diagnosis": "WINDOWS_STAGED_OUTPUT_PATH_LENGTH_CONTRACT_DEFECT",
        "diagnosis_basis": {
            "first_attempted_market": first["market"],
            "first_attempted_schema": first["schema"],
            "first_attempted_year": first["year"],
            "first_partial_path_chars": first_partial_chars,
            "windows_legacy_max_path_chars": WINDOWS_LEGACY_MAX_PATH_CHARS,
            "parent_directories_created_without_file": True,
            "raw_values_reported": False,
        },
        "year_2025_or_2026_payloads_opened": 0,
        "provider_or_network_calls": 0,
        "external_cost_usd": "0",
        "published": False,
        "catalog_or_pointer_activated": False,
        "registered_evaluated_or_traded": False,
        "predecessor_artifacts_overwritten": False,
        "v2_reexecution_permitted": False,
        "successor_requirement": (
            "COLLISION_CHECKED_BOUNDED_PATH_IDENTITIES_NEW_COMMITTED_HEAD_AND_FRESH_PLAN"
        ),
    }
    return {**core, "report_id": sha256_json(core)}


def write_create_only() -> dict[str, object]:
    value = build_report()
    target = ROOT / OUTPUT
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as stream:
        stream.write(canonical_bytes(value) + b"\n")
    target.chmod(stat.S_IREAD)
    return value


def main() -> int:
    value = write_create_only()
    print(
        json.dumps(
            {
                "report_id": value["report_id"],
                "sha256": sha256_file(ROOT / OUTPUT),
                "state": value["state"],
                "authorization_receipt_consumed": True,
                "dbn_rows_decoded": 0,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
