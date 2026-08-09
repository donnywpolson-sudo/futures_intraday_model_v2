"""Seal the v21 timeout failure without decoding any staged DBN rows."""

from __future__ import annotations

import json
from pathlib import Path

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
PLAN = Path("configs/apex_micro_tier01_phase1a_acquisition_plan_v21.json")
TERMINAL = Path(
    "state/provider_acquisition_staging/apex_micro_tier01_v21/"
    "5c04fecd51692b21/terminal.json"
)
AUTHORIZATION = Path(
    "state/authorization_uses/"
    "5c04fecd51692b216f468ccf1eecbf72e918d06e675b2a4287a03e4c684ac282.json"
)
OUTPUT = Path(
    "state/unpublished_evidence/"
    "apex_micro_phase1a_acquisition_v21_failure/report.json"
)
EXPECTED_TERMINAL_ID = (
    "af92eb08822b40523bcd045404962ec4624627f5ac7d5a5bae3270ccd81111cc"
)
EXPECTED_TERMINAL_SHA256 = (
    "454ca37e90bad6b45c3bd1cb83ee6f927a7ba33ee64e24549c9660e431e77bc3"
)


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{description} is not an object")
    return value


def build_report(*, root: Path = ROOT) -> dict[str, object]:
    root = root.resolve(strict=True)
    terminal_path = root / TERMINAL
    terminal = _object(terminal_path, "v21 acquisition terminal")
    terminal_core = dict(terminal)
    terminal_id = terminal_core.pop("terminal_id", None)
    if (
        terminal_id != EXPECTED_TERMINAL_ID
        or terminal_id != sha256_json(terminal_core)
        or sha256_file(terminal_path) != EXPECTED_TERMINAL_SHA256
        or terminal.get("state") != "FAILURE_INACTIVE_EVIDENCE_PRESERVED"
        or terminal.get("failure_stage") != "DOWNLOAD_TO_INACTIVE_STAGING"
        or terminal.get("provider_call_counts")
        != {"get_cost": 160, "get_range": 36}
        or terminal.get("accepted_dbn_count") != 0
        or terminal.get("accepted_sidecar_count") != 0
        or terminal.get("completed_finalized_pairs") != []
        or terminal.get("finalization_attempts") != []
        or terminal.get("external_cost_incurred_usd") != "0"
        or terminal.get("automatic_retries") != 0
        or terminal.get("credential_content_recorded") is not False
        or terminal.get("dbn_rows_decoded") != 0
        or terminal.get("payloads_opened_for_row_access") != 0
        or terminal.get("year_2025_or_2026_payloads_opened") != 0
    ):
        raise IntegrityError("v21 fail-closed terminal drifted")
    failures = terminal.get("download_worker_failures")
    if failures != [
        {
            "worker_index": 0,
            "exception_type": "UnauthorizedOperation",
            "failed_request_id": None,
        }
    ]:
        raise IntegrityError("v21 runtime-ceiling failure evidence drifted")
    records = terminal.get("staged_complete_pairs")
    if not isinstance(records, list) or len(records) != 36:
        raise IntegrityError("v21 complete staging-pair count drifted")
    attempt = terminal_path.parent
    actual_files = sorted(
        path.relative_to(root).as_posix()
        for path in (attempt / "downloads").iterdir()
        if path.is_file()
    )
    if actual_files != terminal.get("staging_file_census") or len(actual_files) != 72:
        raise IntegrityError("v21 staging census drifted")
    verified_dbn_bytes = 0
    warning_count = 0
    for record in records:
        if not isinstance(record, dict):
            raise IntegrityError("v21 staging record is malformed")
        dbn = root / str(record["staging_dbn"])
        sidecar_path = root / str(record["staging_sidecar"])
        if (
            not dbn.is_file()
            or not sidecar_path.is_file()
            or dbn.stat().st_size != record["byte_count"]
            or sha256_file(dbn) != record["sha256"]
        ):
            raise IntegrityError("v21 staged DBN hash or size drifted")
        verified_dbn_bytes += int(record["byte_count"])
        sidecar = _object(sidecar_path, "v21 staged sidecar")
        sidecar_core = dict(sidecar)
        sidecar_id = sidecar_core.pop("manifest_id", None)
        if (
            sidecar_id != sha256_json(sidecar_core)
            or sidecar.get("state") != "INACTIVE_CUSTODY_NOT_A_RESEARCH_SOURCE"
            or sidecar.get("plan_id") != terminal["plan_id"]
            or sidecar.get("request_id") != record["request_id"]
            or sidecar.get("byte_count") != record["byte_count"]
            or sidecar.get("sha256") != record["sha256"]
            or sidecar.get("dbn_rows_decoded") != 0
            or sidecar.get("payload_opened_for_row_access") is not False
        ):
            raise IntegrityError("v21 staged sidecar drifted")
        warning_count += int(sidecar.get("provider_warning_count", 0))
    authorization = _object(root / AUTHORIZATION, "v21 authorization use")
    if (
        authorization.get("receipt_id") != terminal.get("authorization_receipt_id")
        or authorization.get("operation")
        != "ACQUIRE_APEX_MICRO_TIER01_RAW_DBN_INACTIVE_CUSTODY_V21_ONCE"
    ):
        raise IntegrityError("v21 authorization binding drifted")
    plan = _object(root / PLAN, "v21 acquisition plan")
    final_destinations = [
        root / str(item[key])
        for item in plan["requests"]
        for key in ("dbn_destination", "sidecar_destination")
    ]
    if any(path.exists() for path in final_destinations):
        raise IntegrityError("v21 failure unexpectedly created a final destination")
    protected = [terminal_path, root / AUTHORIZATION] + [
        root / relative for relative in actual_files
    ]
    if any(not path.stat().st_file_attributes & 1 for path in protected):
        raise IntegrityError("v21 failure evidence is not read-only")
    core: dict[str, object] = {
        "schema_version": "apex_micro_phase1a_acquisition_v21_failure/1.0.0",
        "state": "SEALED_FAIL_CLOSED_RUNTIME_CEILING_NO_ACCEPTED_SOURCE",
        "plan_id": terminal["plan_id"],
        "plan_sha256": sha256_file(root / PLAN),
        "terminal_path": TERMINAL.as_posix(),
        "terminal_id": terminal_id,
        "terminal_sha256": EXPECTED_TERMINAL_SHA256,
        "authorization_path": AUTHORIZATION.as_posix(),
        "authorization_receipt_id": authorization["receipt_id"],
        "authorization_sha256": sha256_file(root / AUTHORIZATION),
        "failure_classification": (
            "GLOBAL_RUNTIME_CEILING_REACHED_BEFORE_NEXT_REQUEST_"
            "INFERRED_FROM_BOUNDED_EXECUTOR_AND_NULL_FAILED_REQUEST"
        ),
        "provider_call_counts": terminal["provider_call_counts"],
        "provider_client_count": terminal["provider_client_count"],
        "verified_complete_staging_pairs": len(records),
        "verified_staging_dbn_bytes": verified_dbn_bytes,
        "staging_file_count": len(actual_files),
        "provider_warning_count_recorded_by_v21_sidecars": warning_count,
        "accepted_dbn_count": 0,
        "accepted_sidecar_count": 0,
        "final_destination_count": 0,
        "external_cost_incurred_usd": "0",
        "automatic_retries": 0,
        "predecessor_staging_reusable_by_successor": False,
        "predecessor_staging_preserved_read_only": True,
        "payload_safety": {
            "dbn_hashes_verified_without_decode": True,
            "dbn_rows_decoded": 0,
            "payloads_opened_for_row_access": 0,
            "year_2025_or_2026_payloads_opened": 0,
            "raw_values_reported": False,
        },
        "authority": {
            "v21_authorization_consumed": True,
            "v21_retry_authorized": False,
            "successor_requires_new_plan_commit_audit_and_download_approval": True,
        },
    }
    return {**core, "report_id": sha256_json(core)}


def main() -> int:
    report = build_report()
    output = ROOT / OUTPUT
    output.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(report) + b"\n"
    if output.exists():
        if output.read_bytes() != raw:
            raise RuntimeError("existing v21 failure report differs")
    else:
        with output.open("xb") as stream:
            stream.write(raw)
    print(
        json.dumps(
            {
                "report_id": report["report_id"],
                "report_sha256": sha256_file(output),
                "state": report["state"],
                "verified_complete_staging_pairs": report[
                    "verified_complete_staging_pairs"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
