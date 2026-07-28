"""Measured, non-authorizing full-certification planning after the bounded pilot."""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath

from .active_data_plan import (
    ENVIRONMENT_PATHS,
    IMPLEMENTATION_PATHS,
    SEMANTIC_PATHS,
    _aggregation_sources,
    _bindings,
    _canonical_object,
    _manifest_source_objects,
    build_supersession_record,
    derive_inventory,
    verify_policy_acceptance,
)
from .active_data_view import (
    CERTIFICATION_STATE,
    UpdateMode,
    build_pending_approval,
    build_plan,
    validate_content_validation_receipt,
    verify_approval,
    verify_contract,
    verify_plan_bindings,
)
from .boundary import RepoBoundary
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    sha256_file,
    sha256_json,
)
from .data_layout import (
    DataReleaseManifest,
    manifest_relative_path,
    verify_data_release_manifest,
)
from .errors import IntegrityError


PILOT_EVIDENCE_SCHEMA = "causal_active_pilot_evidence/1.0.0"
FULL_PLAN_GENERATOR = "src/futures_rebuild/active_data_full_plan.py"
FULL_CERTIFICATION_EXECUTOR = (
    "src/futures_rebuild/active_data_full_certification.py"
)
GIB = 1_073_741_824


def _load_canonical(path: Path, description: str) -> dict[str, object]:
    assert_plain_file(path)
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def _write_new_or_exact(path: Path, payload: Mapping[str, object]) -> None:
    assert_no_linklike_ancestors(path)
    encoded = canonical_bytes(payload) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        assert_plain_file(path)
        if path.read_bytes() != encoded:
            raise IntegrityError(f"refusing to overwrite different evidence: {path}")
        return
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
    fsync_directory(path.parent)


def _report_identity(report: Mapping[str, object]) -> str:
    report_id = report.get("certification_report_id")
    if (
        not isinstance(report_id, str)
        or report_id
        != sha256_json(
            {
                key: value
                for key, value in report.items()
                if key != "certification_report_id"
            }
        )
    ):
        raise IntegrityError("pilot certification report self-hash is invalid")
    intervals = report.get("interval_reports")
    if not isinstance(intervals, list) or not intervals:
        raise IntegrityError("pilot certification interval reports are absent")
    for interval in intervals:
        if not isinstance(interval, dict):
            raise IntegrityError("pilot certification interval report is invalid")
        interval_id = interval.get("interval_report_id")
        if (
            not isinstance(interval_id, str)
            or interval_id
            != sha256_json(
                {
                    key: value
                    for key, value in interval.items()
                    if key != "interval_report_id"
                }
            )
        ):
            raise IntegrityError("pilot interval report self-hash is invalid")
    return report_id


def _correctness_projection(report: Mapping[str, object]) -> dict[str, object]:
    projected = {
        key: value
        for key, value in report.items()
        if key not in {"certification_report_id", "measurements"}
    }
    intervals = projected.get("interval_reports")
    if not isinstance(intervals, list):
        raise IntegrityError("pilot interval reports are invalid")
    projected["interval_reports"] = [
        {
            key: value
            for key, value in interval.items()
            if key not in {"interval_report_id", "measurements"}
        }
        for interval in intervals
        if isinstance(interval, dict)
    ]
    if len(projected["interval_reports"]) != len(intervals):
        raise IntegrityError("pilot interval report projection is incomplete")
    return projected


def _measurement(report: Mapping[str, object], name: str) -> int | float:
    measurements = report.get("measurements")
    if not isinstance(measurements, dict):
        raise IntegrityError("pilot measurements are absent")
    value = measurements.get(name)
    if name.endswith("_bytes") or name == "interval_count":
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise IntegrityError(f"pilot {name} measurement is invalid")
        return value
    try:
        measured = float(str(value))
    except (TypeError, ValueError) as exc:
        raise IntegrityError(f"pilot {name} measurement is invalid") from exc
    if not math.isfinite(measured) or measured < 0:
        raise IntegrityError(f"pilot {name} measurement is invalid")
    return measured


def build_pilot_evidence(
    *,
    repository_root: Path,
    pilot_plan_path: Path,
    pilot_approval_path: Path,
) -> tuple[dict[str, object], dict[tuple[int, str, int], tuple[dict[str, object], dict[str, object]]]]:
    """Verify the two-run pilot and return immutable non-authorizing evidence."""

    root = repository_root.resolve(strict=True)
    verify_contract(root)
    plan = _load_canonical(pilot_plan_path, "pilot plan")
    approval = _load_canonical(pilot_approval_path, "pilot approval")
    verify_plan_bindings(root, plan)
    approval_receipt_id = verify_approval(
        approval,
        plan,
        expected_operation="CERTIFY_CAUSAL_ACTIVE_VIEW",
    )
    scope_id = plan.get("pilot_scope_id")
    run_ids = plan.get("pilot_run_ids")
    entries = plan.get("entries")
    if (
        not isinstance(scope_id, str)
        or not isinstance(run_ids, list)
        or len(run_ids) != 2
        or len(set(run_ids)) != 2
        or not isinstance(entries, list)
        or len(entries) != 2
    ):
        raise IntegrityError("pilot plan scope is invalid")
    state_root = (
        root / "state" / "active_data_view_certification" / "pilot" / scope_id
    )
    records: dict[
        tuple[int, str, int], tuple[dict[str, object], dict[str, object]]
    ] = {}
    run_evidence: list[dict[str, object]] = []
    for run_number in (1, 2):
        run_items: list[dict[str, object]] = []
        for entry in sorted(entries, key=lambda item: (str(item["market"]), int(item["year"]))):
            market = str(entry["market"])
            year = int(entry["year"])
            workspace = state_root / f"run-{run_number}" / market / str(year)
            report_path = workspace / "certification_report.json"
            receipt_path = workspace / "content_validation_receipt.json"
            report = _load_canonical(report_path, "pilot certification report")
            receipt = _load_canonical(receipt_path, "pilot content receipt")
            report_id = _report_identity(report)
            receipt_id = validate_content_validation_receipt(receipt)
            if (
                report.get("status") != "PASS"
                or report.get("state") != CERTIFICATION_STATE
                or report.get("market") != market
                or report.get("year") != year
                or report.get("content_validation_receipt_id") != receipt_id
            ):
                raise IntegrityError("pilot result does not match its approved candidate")
            records[(run_number, market, year)] = (report, receipt)
            run_items.append(
                {
                    "certification_report_id": report_id,
                    "certification_report_path": report_path.relative_to(root).as_posix(),
                    "certification_report_sha256": sha256_file(report_path),
                    "content_validation_receipt_id": receipt_id,
                    "content_validation_receipt_path": receipt_path.relative_to(root).as_posix(),
                    "content_validation_receipt_sha256": sha256_file(receipt_path),
                    "market": market,
                    "measurements": dict(report["measurements"]),
                    "year": year,
                }
            )
        run_evidence.append(
            {
                "candidates": run_items,
                "run_id": run_ids[run_number - 1],
                "run_number": run_number,
            }
        )
    for entry in entries:
        market = str(entry["market"])
        year = int(entry["year"])
        first_report, first_receipt = records[(1, market, year)]
        second_report, second_receipt = records[(2, market, year)]
        if (
            _correctness_projection(first_report)
            != _correctness_projection(second_report)
            or first_receipt != second_receipt
        ):
            raise IntegrityError(
                f"pilot correctness is not deterministic for {market}/{year}"
            )
    reports = [item[0] for item in records.values()]
    observed = {
        "maximum_certification_seconds": format(
            max(float(_measurement(report, "certification_seconds")) for report in reports),
            ".9f",
        ),
        "maximum_cpu_seconds": format(
            max(float(_measurement(report, "cpu_seconds")) for report in reports),
            ".9f",
        ),
        "maximum_peak_working_set_bytes": max(
            int(_measurement(report, "peak_working_set_bytes")) for report in reports
        ),
        "maximum_selected_causal_bytes": max(
            int(_measurement(report, "selected_causal_bytes")) for report in reports
        ),
        "maximum_temporary_bytes": max(
            int(_measurement(report, "temporary_bytes")) for report in reports
        ),
    }
    core: dict[str, object] = {
        "approval_receipt_id": approval_receipt_id,
        "authority": "NON_AUTHORIZING_PILOT_EVIDENCE_ONLY",
        "correctness_projection_excludes": [
            "certification_report_id",
            "interval_report_id",
            "measurements",
        ],
        "deterministic_content_receipts": True,
        "deterministic_correctness_results": True,
        "does_not_authorize": [
            "ACTIVE_ROOT_MUTATION",
            "ARCHIVE_OR_DELETE",
            "FULL_CERTIFICATION",
            "HOLDOUT_OR_FORWARD_PAYLOAD_ACCESS",
            "MODEL_FIT_OR_EVALUATION",
            "OUTCOME_LABEL_PREDICTION_ACCESS",
            "PROVIDER_CALL_OR_DOWNLOAD",
            "PUBLICATION",
            "TRADING",
        ],
        "observed_bounds": observed,
        "pilot_plan_id": plan["plan_id"],
        "pilot_plan_sha256": sha256_json(plan),
        "pilot_scope_id": scope_id,
        "runs": run_evidence,
        "schema_version": PILOT_EVIDENCE_SCHEMA,
        "status": "PASS",
    }
    return {**core, "pilot_evidence_id": sha256_json(core)}, records


def publish_pilot_evidence(
    *,
    repository_root: Path,
    evidence: Mapping[str, object],
    records: Mapping[
        tuple[int, str, int], tuple[dict[str, object], dict[str, object]]
    ],
) -> Path:
    root = repository_root.resolve(strict=True)
    scope_id = str(evidence["pilot_scope_id"])
    report_root = root / "reports" / "active_data_view" / "pilot" / scope_id
    for (run_number, market, year), (report, receipt) in sorted(records.items()):
        destination = report_root / f"run-{run_number}" / market / str(year)
        _write_new_or_exact(destination / "certification_report.json", report)
        _write_new_or_exact(destination / "content_validation_receipt.json", receipt)
    evidence_path = report_root / "run-2" / "pilot_evidence.json"
    _write_new_or_exact(evidence_path, evidence)
    return evidence_path


def _add_object(
    source_objects: dict[str, dict[str, object]],
    entry_files: set[str],
    item: Mapping[str, object],
) -> None:
    path = str(item["path"])
    existing = source_objects.setdefault(path, dict(item))
    if existing != dict(item):
        raise IntegrityError("full-plan source object has conflicting identities")
    entry_files.add(path)


def _planned_candidates(
    *,
    root: Path,
    inventory_entries: Sequence[Mapping[str, object]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    boundary = RepoBoundary(active_root=root)
    source_objects: dict[str, dict[str, object]] = {}
    planned_entries: list[dict[str, object]] = []
    for entry in inventory_entries:
        if entry.get("disposition") != CERTIFICATION_STATE:
            planned_entries.append(dict(entry))
            continue
        planned_intervals: list[dict[str, object]] = []
        entry_files: set[str] = set()
        entry_rows = 0
        for raw_interval in entry["intervals"]:  # type: ignore[index]
            if not isinstance(raw_interval, dict):
                raise IntegrityError("full-plan foundation interval is invalid")
            interval = dict(raw_interval)
            dbn_manifest_path = root / manifest_relative_path(
                "dbn", str(interval["source_dbn_release_id"])
            )
            dbn_manifest = verify_data_release_manifest(
                dbn_manifest_path,
                boundary,
                verify_files=False,
            )
            aggregates = _aggregation_sources(
                root=root,
                dbn_manifest=dbn_manifest,
                interval=interval,
            )
            interval["aggregation_sources"] = aggregates
            available = {str(item["schema"]) for item in aggregates}
            interval["aggregation_expectations"] = {
                schema: ("REQUIRED" if schema in available else "NOT_AVAILABLE")
                for schema in ("ohlcv-1d", "ohlcv-1h")
            }
            for receipt_name, object_class in (
                ("causal_release_receipt", "CAUSAL"),
                ("raw_release_receipt", "RAW"),
                ("definition_release_receipt", "REFERENCE_DEFINITION"),
                ("economics_release_receipt", "REFERENCE_ECONOMICS"),
            ):
                receipt = interval.get(receipt_name)
                if not isinstance(receipt, dict):
                    raise IntegrityError("full-plan release receipt is absent")
                manifest_path = root / str(receipt["manifest_path"])
                manifest = verify_data_release_manifest(
                    manifest_path,
                    boundary,
                    verify_files=False,
                )
                if (
                    manifest.release_id != receipt.get("release_id")
                    or sha256_file(manifest_path) != receipt.get("manifest_sha256")
                ):
                    raise IntegrityError("full-plan release receipt differs from manifest")
                for item in _manifest_source_objects(
                    root=root,
                    manifest=manifest,
                    manifest_path=manifest_path,
                    object_class=object_class,
                ):
                    _add_object(source_objects, entry_files, item)
                if object_class == "RAW":
                    receipt_entries = [
                        item
                        for item in manifest.files
                        if Path(item.logical_path).name == "interval_receipt.json"
                    ]
                    if len(receipt_entries) != 1:
                        raise IntegrityError("full-plan raw receipt is ambiguous")
                    raw_receipt = _canonical_object(
                        root / manifest.physical_relative_path(receipt_entries[0]),
                        "full-plan raw interval receipt",
                    )
                    entry_rows += int(raw_receipt["bar_rows"])
                    entry_rows += int(raw_receipt["definition_rows_scanned"])
            indexed = {item.logical_path: item for item in dbn_manifest.files}
            for relative in (
                str(interval["bar_source_path"]),
                f"{interval['bar_source_path']}.manifest.json",
                str(interval["definition_source_path"]),
                f"{interval['definition_source_path']}.manifest.json",
            ):
                logical = f"data/{relative}"
                item = indexed.get(logical)
                if item is None:
                    raise IntegrityError("full-plan DBN source is absent")
                physical = root / dbn_manifest.physical_relative_path(item)
                _add_object(
                    source_objects,
                    entry_files,
                    {
                        "logical_path": logical,
                        "object_class": "DBN_OR_DOWNLOAD_SIDECAR",
                        "path": physical.relative_to(root).as_posix(),
                        "sha256": item.sha256,
                        "size": item.size,
                    },
                )
            _add_object(
                source_objects,
                entry_files,
                {
                    "object_class": "DBN_MANIFEST",
                    "path": dbn_manifest_path.relative_to(root).as_posix(),
                    "sha256": sha256_file(dbn_manifest_path),
                    "size": dbn_manifest_path.stat().st_size,
                },
            )
            for aggregate in aggregates:
                for relative, hash_key, size_key in (
                    (str(aggregate["relative_path"]), "sha256", "size"),
                    (
                        str(aggregate["sidecar_relative_path"]),
                        "sidecar_sha256",
                        "sidecar_size",
                    ),
                ):
                    path = root / "data" / PurePosixPath(relative)
                    _add_object(
                        source_objects,
                        entry_files,
                        {
                            "object_class": "AGGREGATION_DBN_OR_SIDECAR",
                            "path": path.relative_to(root).as_posix(),
                            "sha256": aggregate[hash_key],
                            "size": aggregate[size_key],
                        },
                    )
                entry_rows += int(aggregate["size"])
            planned_intervals.append(interval)
        planned = dict(entry)
        planned["intervals"] = planned_intervals
        planned["source_ceiling"] = {
            "maximum_rows": entry_rows,
            "maximum_source_bytes": sum(
                int(source_objects[path]["size"]) for path in entry_files
            ),
            "maximum_source_files": len(entry_files),
        }
        planned_entries.append(planned)
    return planned_entries, sorted(
        source_objects.values(), key=lambda item: str(item["path"])
    )


def _round_up(value: float, unit: int) -> int:
    return max(unit, int(math.ceil(value / unit)) * unit)


def build_measured_full_plan(
    *,
    repository_root: Path,
    foundation_release_id: str,
    accepted_policy_release_id: str,
    policy_acceptance_receipt_id: str,
    pilot_evidence: Mapping[str, object],
    pilot_records: Mapping[
        tuple[int, str, int], tuple[dict[str, object], dict[str, object]]
    ],
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    root = repository_root.resolve(strict=True)
    verify_policy_acceptance(
        repository_root=root,
        policy_release_id=accepted_policy_release_id,
        policy_acceptance_receipt_id=policy_acceptance_receipt_id,
    )
    if (
        pilot_evidence.get("schema_version") != PILOT_EVIDENCE_SCHEMA
        or pilot_evidence.get("status") != "PASS"
        or pilot_evidence.get("pilot_evidence_id")
        != sha256_json(
            {
                key: value
                for key, value in pilot_evidence.items()
                if key != "pilot_evidence_id"
            }
        )
    ):
        raise IntegrityError("pilot evidence is invalid")
    inventory = derive_inventory(
        repository_root=root,
        foundation_release_id=foundation_release_id,
    )
    planned_entries, source_objects = _planned_candidates(
        root=root,
        inventory_entries=inventory["entries"],  # type: ignore[arg-type]
    )
    candidate_entries = [
        entry for entry in planned_entries if entry["disposition"] == CERTIFICATION_STATE
    ]
    source_rows = sum(
        int(entry["source_ceiling"]["maximum_rows"])  # type: ignore[index]
        for entry in candidate_entries
    )
    source_bytes = sum(int(item["size"]) for item in source_objects)
    selected_causal_bytes = sum(
        int(item["size"])
        for item in source_objects
        if item.get("object_class") == "CAUSAL"
        and str(item["path"]).endswith("/bars.parquet")
    )
    reports = [report for report, _ in pilot_records.values()]
    worst_seconds = max(
        float(_measurement(report, "certification_seconds")) for report in reports
    )
    worst_seconds_per_row = max(
        float(_measurement(report, "certification_seconds"))
        / int(report["canonical_market_year"]["row_count"])  # type: ignore[index]
        for report in reports
    )
    candidate_count = int(inventory["counts"]["certification_candidates"])  # type: ignore[index]
    selected_row_count = int(inventory["counts"]["selected_row_count"])  # type: ignore[index]
    baseline_seconds = max(
        worst_seconds * candidate_count,
        worst_seconds_per_row * selected_row_count,
    )
    estimated_seconds = _round_up(baseline_seconds * 1.25, 3600)
    duration_ceiling = _round_up(baseline_seconds * 2.0, 3600)
    peak_memory = max(
        int(_measurement(report, "peak_working_set_bytes")) for report in reports
    )
    memory_ceiling = max(4 * GIB, _round_up(peak_memory * 2.0, GIB))
    worst_temp_ratio = max(
        int(_measurement(report, "temporary_bytes"))
        / int(_measurement(report, "selected_causal_bytes"))
        for report in reports
    )
    temporary_ceiling = max(
        30_000_000_000,
        _round_up(selected_causal_bytes * worst_temp_ratio * 1.5, GIB),
    )
    semantic = _bindings(root, SEMANTIC_PATHS)
    semantic["accepted_active_price_policy_release_id"] = accepted_policy_release_id
    semantic["accepted_active_price_policy_receipt_id"] = (
        policy_acceptance_receipt_id
    )
    implementation = _bindings(root, IMPLEMENTATION_PATHS)
    implementation[FULL_PLAN_GENERATOR] = sha256_file(root / FULL_PLAN_GENERATOR)
    implementation[FULL_CERTIFICATION_EXECUTOR] = sha256_file(
        root / FULL_CERTIFICATION_EXECUTOR
    )
    environment = _bindings(root, ENVIRONMENT_PATHS)
    projection = {
        "approval_duration_ceiling_seconds": duration_ceiling,
        "estimated_serial_duration_seconds": estimated_seconds,
        "maximum_workers": 1,
        "memory_safety_factor": "2.0",
        "pilot_candidate_duration_safety_factor": "2.0",
        "pilot_evidence_id": pilot_evidence["pilot_evidence_id"],
        "pilot_temporary_storage_safety_factor": "1.5",
        "temporary_bytes_per_selected_causal_byte": format(
            worst_temp_ratio, ".9f"
        ),
    }
    scope_id = sha256_json(
        {
            "entries": planned_entries,
            "environment_bindings": environment,
            "foundation_release_id": foundation_release_id,
            "implementation_bindings": implementation,
            "measured_projection": projection,
            "semantic_bindings": semantic,
            "source_objects": source_objects,
        }
    )
    plan = build_plan(
        operation="CERTIFY_CAUSAL_ACTIVE_VIEW",
        mode=UpdateMode.INITIAL,
        foundation_release_id=str(inventory["foundation_release_id"]),
        foundation_manifest_sha256=str(inventory["foundation_manifest_sha256"]),
        semantic_bindings=semantic,
        entries=planned_entries,
        limits={
            "maximum_candidates": candidate_count,
            "maximum_duration_seconds": duration_ceiling,
            "maximum_memory_bytes": memory_ceiling,
            "maximum_processed_rows": source_rows * 10,
            "maximum_rows": source_rows,
            "maximum_source_bytes": source_bytes,
            "maximum_source_files": len(source_objects),
            "maximum_temporary_bytes": temporary_ceiling,
            "maximum_workers": 1,
        },
        forbidden_actions=[
            "ACTIVE_ROOT_MUTATION",
            "ARCHIVE_OR_DELETE",
            "HOLDOUT_OR_FORWARD_PAYLOAD_ACCESS",
            "MODEL_FIT_OR_EVALUATION",
            "OUTCOME_LABEL_PREDICTION_ACCESS",
            "PROVIDER_CALL_OR_DOWNLOAD",
            "PUBLICATION",
            "TRADING",
        ],
        outputs=[
            f"reports/active_data_view/full/{scope_id}",
            f"state/active_data_view_certification/full/{scope_id}",
        ],
        implementation_bindings=implementation,
        environment_bindings=environment,
        recovery_boundary="CERTIFICATION_STATE_ONLY_ACTIVE_ROOT_ABSENT",
    )
    plan["aggregation_policy"] = {
        "available_source": "REQUIRED_EXACT_CROSSCHECK",
        "missing_source": "PREDECLARED_NOT_AVAILABLE_NOT_AN_AUTOMATIC_PASS",
        "schemas": ["ohlcv-1d", "ohlcv-1h"],
    }
    plan["certification_scope_id"] = scope_id
    plan["measured_projection"] = projection
    plan["pilot_evidence_id"] = pilot_evidence["pilot_evidence_id"]
    plan["source_objects"] = source_objects
    plan["plan_id"] = sha256_json(
        {key: value for key, value in plan.items() if key != "plan_id"}
    )
    approval = build_pending_approval(plan)
    report_core: dict[str, object] = {
        "aggregation_expectation_counts": {
            state: sum(
                1
                for entry in candidate_entries
                for interval in entry["intervals"]  # type: ignore[index]
                for value in interval["aggregation_expectations"].values()
                if value == state
            )
            for state in ("NOT_AVAILABLE", "REQUIRED")
        },
        "authority": "NON_AUTHORIZING_FULL_CERTIFICATION_PLAN_ONLY",
        "counts": inventory["counts"],
        "limits": plan["limits"],
        "measured_projection": projection,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_json(plan),
        "schema_version": "causal_active_full_dry_run/1.0.0",
        "status": "PENDING_EXACT_APPROVAL",
    }
    return (
        plan,
        approval,
        {**report_core, "dry_run_report_id": sha256_json(report_core)},
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument("--pilot-plan", type=Path, required=True)
    parser.add_argument("--pilot-approval", type=Path, required=True)
    parser.add_argument("--foundation-release-id", required=True)
    parser.add_argument("--accepted-policy-release-id", required=True)
    parser.add_argument("--policy-acceptance-receipt-id", required=True)
    parser.add_argument("--plan-output", type=Path, required=True)
    parser.add_argument("--approval-output", type=Path, required=True)
    parser.add_argument("--superseded-plan", type=Path, required=True)
    parser.add_argument("--supersession-output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.repository_root.resolve(strict=True)
    resolve = lambda path: path if path.is_absolute() else root / path
    pilot_evidence, pilot_records = build_pilot_evidence(
        repository_root=root,
        pilot_plan_path=resolve(args.pilot_plan),
        pilot_approval_path=resolve(args.pilot_approval),
    )
    evidence_path = publish_pilot_evidence(
        repository_root=root,
        evidence=pilot_evidence,
        records=pilot_records,
    )
    plan, approval, dry_run = build_measured_full_plan(
        repository_root=root,
        foundation_release_id=args.foundation_release_id,
        accepted_policy_release_id=args.accepted_policy_release_id,
        policy_acceptance_receipt_id=args.policy_acceptance_receipt_id,
        pilot_evidence=pilot_evidence,
        pilot_records=pilot_records,
    )
    supersession = build_supersession_record(
        repository_root=root,
        predecessor_plan_path=resolve(args.superseded_plan)
        .relative_to(root)
        .as_posix(),
        successor_plan=plan,
    )
    plan_path = resolve(args.plan_output)
    approval_path = resolve(args.approval_output)
    supersession_path = resolve(args.supersession_output)
    _write_new_or_exact(plan_path, plan)
    _write_new_or_exact(approval_path, approval)
    _write_new_or_exact(supersession_path, supersession)
    report_root = root / "reports" / "active_data_view" / "full" / str(
        plan["certification_scope_id"]
    )
    _write_new_or_exact(report_root / "dry_run_report.json", dry_run)
    print(
        canonical_bytes(
            {
                "approval_status": "PENDING",
                "dry_run_report_id": dry_run["dry_run_report_id"],
                "pilot_evidence_path": evidence_path.relative_to(root).as_posix(),
                "plan_id": plan["plan_id"],
            }
        ).decode()
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
