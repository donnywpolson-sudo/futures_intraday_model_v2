"""Plan and execute one bounded Apex micro Phase 1B/2 historical-row build.

Planning is source-safe and never opens a DBN.  Execution requires one exact
single-use real-history authorization, verifies all approved DBN hashes before
the first decode, writes only inactive staging/evidence, and cannot activate a
catalog or reach provider/network code.
"""

from __future__ import annotations

import json
import shutil
import stat
import subprocess
import threading
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Final

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    contained_path,
    sha256_file,
    sha256_json,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .micro_alpha_phase1b2_decoder import (
    AVAILABILITY_BASIS,
    MAX_BATCH_ROWS,
    PINNED_PUBLICATION_LATENCY_NS,
    CausalResult,
    CreatedByteBudget,
    DecodeResult,
    decode_dbn_to_inactive_parquet,
    materialize_causal_1m_inactive,
)
from .micro_alpha_phase1b2_preparation import (
    ACTIVE_MICRO_CATALOG_PATH,
    ACTIVE_MICRO_POINTER_PATH,
    OUTPUT_PATH as PREPARE_CONTRACT_PATH,
    require_row_certified_catalog_candidate,
)
from .micro_alpha_pipeline import (
    DATASET,
    LANE_ID,
    PRODUCT_REFERENCE_REQUIREMENTS,
    SCHEMAS,
    TIER_1_MARKETS,
    phase1b_destination,
)


OPERATION: Final = "BUILD_APEX_MICRO_PHASE1B2_INACTIVE_FOUNDATION_V1_ONCE"
PLAN_PATH: Final = Path("configs/apex_micro_phase1b2_historical_execution_plan_v2.json")
AUDIT_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_phase1b2_execution_plan_v2/audit.json"
)
ACQUISITION_PLAN_PATH: Final = Path(
    "configs/apex_micro_tier01_phase1a_acquisition_plan_v24.json"
)
CUSTODY_TERMINAL_PATH: Final = Path(
    "state/unpublished_evidence/apex_micro_v24_custody_repair_v2/terminal.json"
)
ACQUISITION_TERMINAL_PATH: Final = Path(
    "state/provider_acquisition_staging/apex_micro_tier01_v24/eaee71b9128cf8e6/terminal.json"
)
STAGING_ROOT: Final = Path("state/data_publication_staging/apex_integer_micro_11")
EVIDENCE_ROOT: Final = Path("state/unpublished_evidence/apex_micro_phase1b2_execution_v2")
PLAN_SCHEMA: Final = "apex_micro_phase1b2_historical_execution_plan/2.0.0"
AUDIT_SCHEMA: Final = "apex_micro_phase1b2_execution_audit/2.0.0"
TERMINAL_SCHEMA: Final = "apex_micro_phase1b2_execution_terminal/1.0.0"
REPORT_SCHEMA: Final = "apex_micro_phase1b2_source_certification/1.0.0"
CATALOG_SCHEMA: Final = "apex_micro_inactive_catalog_candidate/1.0.0"
AUTHORIZATION_SCHEMA: Final = "apex_micro_phase1b2_historical_authorization/1.0.0"
INTERVAL_RECEIPT_SCHEMA: Final = "apex_micro_phase1b_interval_receipt/1.0.0"
COVERAGE_SCHEMA: Final = "apex_micro_phase1b2_coverage_census/1.0.0"
ELIGIBLE_YEARS: Final = tuple(range(2018, 2025))
SEALED_YEARS: Final = (2025, 2026)
EXPECTED_SOURCE_COUNT: Final = 120
EXPECTED_COVERAGE_CELL_COUNT: Final = 140
EXPECTED_INTERVAL_COUNT: Final = 24
EXPECTED_SOURCE_BYTES: Final = 1_232_883_585
MAXIMUM_PARQUET_OUTPUTS: Final = 144
MAXIMUM_OUTPUT_BYTES: Final = 64 * 1024**3
REQUIRED_FREE_DISK_BYTES: Final = 80 * 1024**3
MAXIMUM_RUNTIME_SECONDS: Final = 43_200
MAXIMUM_WORKERS: Final = 2
MAXIMUM_ATTEMPTS: Final = 1
MAXIMUM_RETRIES: Final = 0

OUTPUT_FILENAME: Final = {
    "definition": "definitions.parquet",
    "status": "status.parquet",
    "statistics": "statistics.parquet",
    "ohlcv-1m": "bars.parquet",
    "ohlcv-1s": "reported_trade_bars.parquet",
}
SCHEMA_FOLDER: Final = {
    "definition": "definition",
    "status": "status",
    "statistics": "statistics",
    "ohlcv-1m": "ohlcv_1m",
    "ohlcv-1s": "ohlcv_1s",
}

IMPLEMENTATION_PATHS: Final = (
    Path("src/futures_rebuild/micro_alpha_phase1b2_decoder.py"),
    Path("src/futures_rebuild/micro_alpha_phase1b2_execution.py"),
    Path("src/futures_rebuild/micro_alpha_phase1b2_preparation.py"),
    Path("src/futures_rebuild/micro_alpha_pipeline.py"),
    Path("src/futures_rebuild/boundary.py"),
    Path("src/futures_rebuild/research_gateway_policy.py"),
    Path("src/futures_rebuild/canonical.py"),
    Path("configs/dependency_lock_receipt.json"),
)


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid") from exc
    if type(value) is not dict:
        raise IntegrityError(f"{description} is not an object")
    return value


def _self_hash(value: Mapping[str, object], key: str, description: str) -> str:
    core = dict(value)
    observed = core.pop(key, None)
    if type(observed) is not str or observed != sha256_json(core):
        raise IntegrityError(f"{description} identity drifted")
    return observed


def _git_head(root: Path) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    head = result.stdout.strip()
    if len(head) != 40:
        raise IntegrityError("committed implementation HEAD is invalid")
    return head


def _write_create_only(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(value) + b"\n")
    path.chmod(stat.S_IREAD)


def _year_from_request(item: Mapping[str, object]) -> int:
    value = item.get("year")
    if isinstance(value, bool) or not isinstance(value, int):
        raise IntegrityError("micro request year is invalid")
    return value


def _interval(item: Mapping[str, object]) -> str:
    query = item.get("query")
    if not isinstance(query, Mapping):
        raise IntegrityError("micro request query is absent")
    start = query.get("start")
    end = query.get("end")
    if type(start) is not str or type(end) is not str:
        raise IntegrityError("micro request date bounds are invalid")
    return f"{start}_{end}"


def _validate_sidecar(
    *, root: Path, item: Mapping[str, object], acquisition_plan_id: str
) -> dict[str, object]:
    year = _year_from_request(item)
    if year not in ELIGIBLE_YEARS:
        raise UnauthorizedOperation("holdout or forward source entered the execution plan")
    dbn_path = contained_path(root, str(item.get("dbn_destination", "")))
    sidecar_path = contained_path(root, str(item.get("sidecar_destination", "")))
    assert_no_linklike_ancestors(dbn_path)
    assert_no_linklike_ancestors(sidecar_path)
    dbn_stat = assert_plain_file(dbn_path)
    sidecar_stat = assert_plain_file(sidecar_path)
    if dbn_stat.st_nlink != 1 or sidecar_stat.st_nlink != 1:
        raise IntegrityError("micro source custody is not single-link")
    sidecar = _object(sidecar_path, "micro inactive sidecar")
    manifest_id = _self_hash(sidecar, "manifest_id", "micro inactive sidecar")
    query = item.get("query")
    exact_query = sidecar.get("exact_authorized_query")
    expected_query = {
        **(dict(query) if isinstance(query, Mapping) else {}),
        "compression": "zstd",
        "encoding": "dbn",
    }
    if (
        sidecar.get("state") != "INACTIVE_CUSTODY_NOT_A_RESEARCH_SOURCE"
        or sidecar.get("plan_id") != acquisition_plan_id
        or sidecar.get("request_id") != item.get("request_id")
        or sidecar.get("byte_count") != dbn_stat.st_size
        or exact_query != expected_query
        or sidecar.get("source_certification_required") is not True
        or sidecar.get("dbn_rows_decoded") != 0
        or sidecar.get("payload_opened_for_row_access") is not False
        or sidecar.get("catalog_activation") is not False
    ):
        raise IntegrityError("micro inactive sidecar binding drifted")
    source_sha = sidecar.get("sha256")
    byte_count = sidecar.get("byte_count")
    if type(source_sha) is not str or len(source_sha) != 64 or type(byte_count) is not int:
        raise IntegrityError("micro sidecar source hash or byte count is invalid")
    return {
        "dbn_path": dbn_path.relative_to(root).as_posix(),
        "sidecar_path": sidecar_path.relative_to(root).as_posix(),
        "sidecar_sha256": sha256_file(sidecar_path),
        "sidecar_manifest_id": manifest_id,
        "source_sha256": source_sha,
        "source_bytes": byte_count,
        "exact_query": exact_query,
    }


def _release_id(
    *, item: Mapping[str, object], source: Mapping[str, object], implementation_hashes: Mapping[str, str]
) -> str:
    return sha256_json(
        {
            "lane_id": LANE_ID,
            "request_id": item["request_id"],
            "source_sha256": source["source_sha256"],
            "sidecar_sha256": source["sidecar_sha256"],
            "schema": item["schema"],
            "decoder_sha256": implementation_hashes[
                "src/futures_rebuild/micro_alpha_phase1b2_decoder.py"
            ],
            "availability_basis": AVAILABILITY_BASIS,
            "pinned_publication_latency_ns": PINNED_PUBLICATION_LATENCY_NS,
        }
    )


def _validate_annual_source_contract(
    *, item: Mapping[str, object], product_effective_date: str
) -> None:
    """Validate exact annual scope and path without opening source bytes."""

    market = str(item.get("market", ""))
    schema = str(item.get("schema", ""))
    year = _year_from_request(item)
    query = item.get("query")
    if market not in TIER_1_MARKETS or schema not in SCHEMAS or not isinstance(query, Mapping):
        raise IntegrityError("micro annual source selector is invalid")
    expected_start = max(f"{year:04d}-01-01", product_effective_date)
    expected_end = f"{year + 1:04d}-01-01"
    expected_stype = "parent" if schema == "definition" else "continuous"
    expected_symbol = f"{market}.FUT" if schema == "definition" else f"{market}.v.0"
    expected_query = {
        "dataset": DATASET,
        "end": expected_end,
        "schema": schema,
        "start": expected_start,
        "stype_in": expected_stype,
        "stype_out": "instrument_id",
        "symbols": [expected_symbol],
    }
    interval = f"{expected_start}_{expected_end}"
    expected_dbn = (
        f"data/dbn/{SCHEMA_FOLDER[schema]}/{market}/{year}/{interval}.dbn.zst"
    )
    if dict(query) != expected_query:
        raise IntegrityError("micro annual query, symbology, or product date drifted")
    if item.get("dbn_destination") != expected_dbn:
        raise IntegrityError("micro annual DBN destination drifted")
    if item.get("sidecar_destination") != f"{expected_dbn}.manifest.json":
        raise IntegrityError("micro annual sidecar destination drifted")


def _phase2_release_id(group: list[Mapping[str, object]]) -> str:
    return sha256_json(
        {
            "lane_id": LANE_ID,
            "source_release_ids": sorted(str(item["phase1b_release_id"]) for item in group),
            "availability_basis": AVAILABILITY_BASIS,
            "transformations": [
                "EXACT_PHASE1B_PARQUET_READ",
                "INTERVAL_END_PLUS_PINNED_LATENCY",
                "NULLABILITY_PRESERVED",
                "NO_LEARNED_OR_OUTCOME_INFORMED_TRANSFORMS",
            ],
        }
    )


def _validate_bound_evidence(root: Path) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    prepare = _object(root / PREPARE_CONTRACT_PATH, "micro prepare-only contract")
    acquisition = _object(root / ACQUISITION_PLAN_PATH, "micro acquisition plan")
    custody = _object(root / CUSTODY_TERMINAL_PATH, "micro custody terminal")
    acquisition_terminal = _object(
        root / ACQUISITION_TERMINAL_PATH, "micro acquisition terminal"
    )
    _self_hash(prepare, "contract_id", "micro prepare-only contract")
    acquisition_id = _self_hash(acquisition, "plan_id", "micro acquisition plan")
    _self_hash(custody, "terminal_id", "micro custody terminal")
    _self_hash(
        acquisition_terminal, "terminal_id", "micro acquisition terminal"
    )
    if (
        prepare.get("state") != "PREPARED_NOT_EXECUTED_HISTORICAL_ROW_APPROVAL_REQUIRED"
        or prepare.get("markets") != list(TIER_1_MARKETS)
        or prepare.get("schemas") != list(SCHEMAS)
        or custody.get("state") != "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY_REPAIRED"
        or custody.get("failure") is not None
        or custody.get("year_2025_or_2026_payloads_opened_for_row_access") != 0
        or acquisition_terminal.get("state")
        != "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY"
        or acquisition_terminal.get("failure") is not None
        or acquisition_terminal.get("payloads_opened_for_row_access") != 0
        or acquisition.get("plan_id") != acquisition_id
    ):
        raise IntegrityError("micro Phase 1B/2 predecessor evidence is not complete")
    if (root / ACTIVE_MICRO_POINTER_PATH).exists() or (root / ACTIVE_MICRO_CATALOG_PATH).exists():
        raise UnauthorizedOperation("micro catalog or pointer became active")
    return prepare, acquisition, custody


def build_execution_plan(*, root: Path, implementation_head: str) -> dict[str, object]:
    """Build an exact post-commit plan without opening any DBN payload."""

    root = root.resolve(strict=True)
    if implementation_head != _git_head(root):
        raise IntegrityError("micro execution plan must bind the live committed HEAD")
    prepare, acquisition, custody = _validate_bound_evidence(root)
    implementation_hashes = {
        path.as_posix(): sha256_file(root / path) for path in IMPLEMENTATION_PATHS
    }
    requests = acquisition.get("requests")
    if not isinstance(requests, list) or len(requests) != 160:
        raise IntegrityError("micro acquisition plan request count drifted")
    effective_dates = prepare.get("product_effective_dates")
    if not isinstance(effective_dates, Mapping):
        raise IntegrityError("micro product dates are absent")
    sources: list[dict[str, object]] = []
    for raw in requests:
        if not isinstance(raw, Mapping):
            raise IntegrityError("micro acquisition request is malformed")
        year = _year_from_request(raw)
        if year in SEALED_YEARS:
            continue
        if year not in ELIGIBLE_YEARS:
            raise UnauthorizedOperation("micro execution source year is unauthorized")
        market = str(raw.get("market", ""))
        schema = str(raw.get("schema", ""))
        if market not in TIER_1_MARKETS or schema not in SCHEMAS:
            raise UnauthorizedOperation("micro execution scope expanded")
        _validate_annual_source_contract(
            item=raw, product_effective_date=str(effective_dates[market])
        )
        source = _validate_sidecar(root=root, item=raw, acquisition_plan_id=str(acquisition["plan_id"]))
        interval = _interval(raw)
        release_id = _release_id(item=raw, source=source, implementation_hashes=implementation_hashes)
        output_root = phase1b_destination(
            market=market,
            schema=schema,
            year=year,
            interval=interval,
            release_id=release_id,
        )
        sources.append(
            {
                "request_id": raw["request_id"],
                "market": market,
                "schema": schema,
                "year": year,
                "interval": interval,
                **source,
                "phase1b_release_id": release_id,
                "phase1b_output_path": f"{output_root}/{OUTPUT_FILENAME[schema]}",
            }
        )
    sources.sort(key=lambda value: (str(value["market"]), int(value["year"]), str(value["schema"])))
    counts = Counter(str(item["schema"]) for item in sources)
    if (
        len(sources) != EXPECTED_SOURCE_COUNT
        or counts != Counter({schema: 24 for schema in SCHEMAS})
        or sum(int(item["source_bytes"]) for item in sources) != EXPECTED_SOURCE_BYTES
    ):
        raise IntegrityError("micro eligible source census drifted")

    source_by_key = {
        (str(item["market"]), int(item["year"]), str(item["schema"])): item
        for item in sources
    }
    coverage: list[dict[str, object]] = []
    for market in TIER_1_MARKETS:
        effective_year = int(str(effective_dates[market])[:4])
        for year in ELIGIBLE_YEARS:
            for schema in SCHEMAS:
                source = source_by_key.get((market, year, schema))
                if year < effective_year:
                    if source is not None:
                        raise IntegrityError("prelaunch cell unexpectedly has a DBN")
                    coverage.append(
                        {
                            "market": market,
                            "year": year,
                            "schema": schema,
                            "planned_disposition": "PRODUCT_NOT_YET_EFFECTIVE",
                            "source_request_id": None,
                        }
                    )
                else:
                    if source is None:
                        raise IntegrityError("postlaunch micro source cell is absent")
                    coverage.append(
                        {
                            "market": market,
                            "year": year,
                            "schema": schema,
                            "planned_disposition": "ROW_CERTIFICATION_REQUIRED",
                            "source_request_id": source["request_id"],
                        }
                    )
    if len(coverage) != EXPECTED_COVERAGE_CELL_COUNT:
        raise IntegrityError("micro coverage census is not exact")

    grouped: dict[tuple[str, int, str], list[Mapping[str, object]]] = defaultdict(list)
    for item in sources:
        grouped[(str(item["market"]), int(item["year"]), str(item["interval"]))].append(item)
    phase2: list[dict[str, object]] = []
    for (market, year, interval), group in sorted(grouped.items()):
        if {str(item["schema"]) for item in group} != set(SCHEMAS):
            raise IntegrityError("micro market-year interval lacks all five schemas")
        release_id = _phase2_release_id(group)
        phase2.append(
            {
                "market": market,
                "year": year,
                "interval": interval,
                "source_phase1b_release_ids": sorted(str(item["phase1b_release_id"]) for item in group),
                "phase2_release_id": release_id,
                "phase2_output_path": (
                    f"data/causally_gated_normalized/{market}/{year}/{interval}/"
                    f"{release_id}/bars.parquet"
                ),
            }
        )
    if len(phase2) != EXPECTED_INTERVAL_COUNT:
        raise IntegrityError("micro Phase 2 interval count drifted")

    scope_id = sha256_json(
        {
            "implementation_head": implementation_head,
            "source_request_ids": [item["request_id"] for item in sources],
            "source_hashes": [item["source_sha256"] for item in sources],
            "phase2_release_ids": [item["phase2_release_id"] for item in phase2],
        }
    )
    staging_root = (STAGING_ROOT / scope_id).as_posix()
    evidence_root = (EVIDENCE_ROOT / scope_id).as_posix()
    core: dict[str, object] = {
        "schema_version": PLAN_SCHEMA,
        "state": "PREPARED_REQUIRES_SEPARATE_HISTORICAL_ROW_CONFIRMATION",
        "operation": OPERATION,
        "lane_id": LANE_ID,
        "dataset": DATASET,
        "implementation_head": implementation_head,
        "implementation_hashes": implementation_hashes,
        "bindings": {
            PREPARE_CONTRACT_PATH.as_posix(): {
                "artifact_id": prepare["contract_id"],
                "sha256": sha256_file(root / PREPARE_CONTRACT_PATH),
            },
            ACQUISITION_PLAN_PATH.as_posix(): {
                "artifact_id": acquisition["plan_id"],
                "sha256": sha256_file(root / ACQUISITION_PLAN_PATH),
            },
            CUSTODY_TERMINAL_PATH.as_posix(): {
                "artifact_id": custody["terminal_id"],
                "sha256": sha256_file(root / CUSTODY_TERMINAL_PATH),
            },
            ACQUISITION_TERMINAL_PATH.as_posix(): {
                "artifact_id": _object(root / ACQUISITION_TERMINAL_PATH, "micro acquisition terminal")["terminal_id"],
                "sha256": sha256_file(root / ACQUISITION_TERMINAL_PATH),
            },
        },
        "scope_id": scope_id,
        "markets": list(TIER_1_MARKETS),
        "schemas": list(SCHEMAS),
        "eligible_years": list(ELIGIBLE_YEARS),
        "sealed_years_excluded_before_file_open": list(SEALED_YEARS),
        "source_count": len(sources),
        "source_bytes": sum(int(item["source_bytes"]) for item in sources),
        "coverage_cell_count": len(coverage),
        "prelaunch_cell_count": sum(
            item["planned_disposition"] == "PRODUCT_NOT_YET_EFFECTIVE" for item in coverage
        ),
        "interval_count": len(phase2),
        "sources": sources,
        "coverage": coverage,
        "phase2": phase2,
        "staging_root": staging_root,
        "evidence_root": evidence_root,
        "outputs": {
            "source_certification_report": f"{evidence_root}/source_certification_report.json",
            "inactive_catalog_candidate": f"{evidence_root}/inactive_catalog_candidate.json",
            "terminal": f"{evidence_root}/terminal.json",
            "active_micro_catalog_written": False,
            "active_micro_pointer_written": False,
        },
        "availability_policies": {
            "ohlcv-1m": {
                "interval_ns": 60_000_000_000,
                "pinned_publication_latency_ns": PINNED_PUBLICATION_LATENCY_NS,
                "basis": AVAILABILITY_BASIS,
            },
            "ohlcv-1s": {
                "interval_ns": 1_000_000_000,
                "pinned_publication_latency_ns": PINNED_PUBLICATION_LATENCY_NS,
                "basis": AVAILABILITY_BASIS,
                "evidence": "REPORTED_TRADE_BARS_ONLY",
            },
        },
        "limits": {
            "maximum_attempts": MAXIMUM_ATTEMPTS,
            "maximum_retries": MAXIMUM_RETRIES,
            "maximum_workers": MAXIMUM_WORKERS,
            "batch_rows": MAX_BATCH_ROWS,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_output_bytes": MAXIMUM_OUTPUT_BYTES,
            "required_free_disk_bytes": REQUIRED_FREE_DISK_BYTES,
            "maximum_parquet_outputs": MAXIMUM_PARQUET_OUTPUTS,
            "provider_calls": 0,
            "external_cost_usd": "0",
        },
        "forbidden": {
            "year_2025_or_2026_payload_open": True,
            "provider_or_network_access": True,
            "credential_access": True,
            "publication_or_active_data_mutation": True,
            "catalog_activation": True,
            "registration": True,
            "model_fit_prediction_or_evaluation": True,
            "raw_values_in_reports": True,
            "cleanup": True,
            "trading": True,
            "git_staging_commit_or_push": True,
        },
        "failure_policy": {
            "stop_scheduling_after_first_failure": True,
            "already_running_second_decode_may_finish_in_inactive_staging": True,
            "partial_outputs_preserved_inactive": True,
            "automatic_retry": False,
            "terminal_written_last": True,
        },
        "record_schemas": {
            "execution_plan": PLAN_SCHEMA,
            "authorization_receipt": AUTHORIZATION_SCHEMA,
            "per_interval_receipt": INTERVAL_RECEIPT_SCHEMA,
            "coverage_census": COVERAGE_SCHEMA,
            "aggregate_source_certificate": REPORT_SCHEMA,
            "terminal_record": TERMINAL_SCHEMA,
            "inactive_catalog_candidate": CATALOG_SCHEMA,
        },
    }
    return {**core, "plan_id": sha256_json(core)}


def write_execution_plan_create_only(*, root: Path, implementation_head: str) -> dict[str, object]:
    plan = build_execution_plan(root=root, implementation_head=implementation_head)
    _write_create_only(root / PLAN_PATH, plan)
    return plan


def load_execution_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH, "micro Phase 1B/2 execution plan")
    _self_hash(plan, "plan_id", "micro Phase 1B/2 execution plan")
    if plan.get("state") != "PREPARED_REQUIRES_SEPARATE_HISTORICAL_ROW_CONFIRMATION":
        raise IntegrityError("micro Phase 1B/2 plan state is invalid")
    return plan


def build_plan_audit(*, root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    plan = load_execution_plan(root=root)
    rebuilt = build_execution_plan(root=root, implementation_head=str(plan["implementation_head"]))
    if rebuilt != plan:
        raise IntegrityError("micro Phase 1B/2 plan reconstruction differs")
    staging = contained_path(root, str(plan["staging_root"]))
    evidence = contained_path(root, str(plan["evidence_root"]))
    if staging.exists() or evidence.exists():
        raise IntegrityError("micro Phase 1B/2 create-only output already exists")
    core = {
        "schema_version": AUDIT_SCHEMA,
        "state": "PASS_SOURCE_SAFE_EXECUTION_PLAN_AUDIT",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "exact_source_count": plan["source_count"],
        "exact_source_bytes": plan["source_bytes"],
        "exact_coverage_cell_count": plan["coverage_cell_count"],
        "exact_prelaunch_cell_count": plan["prelaunch_cell_count"],
        "exact_interval_count": plan["interval_count"],
        "sealed_source_record_count": sum(
            int(item["year"]) in SEALED_YEARS for item in plan["sources"]
        ),
        "destination_conflict_count": 0,
        "dbn_payloads_opened": 0,
        "historical_rows_read": 0,
        "provider_calls": 0,
        "external_cost_usd": "0",
        "catalog_or_pointer_activated": False,
        "published_registered_evaluated_or_traded": False,
        "deterministic_reconstruction": True,
    }
    return {**core, "audit_id": sha256_json(core)}


def write_plan_audit_create_only(*, root: Path) -> dict[str, object]:
    audit = build_plan_audit(root=root)
    _write_create_only(root / AUDIT_PATH, audit)
    return audit


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "lane_id": LANE_ID,
        "markets": ",".join(TIER_1_MARKETS),
        "schemas": ",".join(SCHEMAS),
        "eligible_years": "2018-2024",
        "excluded_payload_years": "2025,2026",
        "exact_source_count": str(EXPECTED_SOURCE_COUNT),
        "exact_source_bytes": str(EXPECTED_SOURCE_BYTES),
        "exact_coverage_cell_count": str(EXPECTED_COVERAGE_CELL_COUNT),
        "maximum_workers": str(MAXIMUM_WORKERS),
        "maximum_runtime_seconds": str(MAXIMUM_RUNTIME_SECONDS),
        "maximum_output_bytes": str(MAXIMUM_OUTPUT_BYTES),
        "required_free_disk_bytes": str(REQUIRED_FREE_DISK_BYTES),
        "maximum_attempts": str(MAXIMUM_ATTEMPTS),
        "maximum_retries": str(MAXIMUM_RETRIES),
        "provider_calls": "0",
        "external_cost_usd": "0",
        "publication_or_activation": "false",
        "registration_evaluation_or_trading": "false",
        "authorization_schema": AUTHORIZATION_SCHEMA,
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def _validate_execution_bindings(root: Path, plan: Mapping[str, object]) -> None:
    if _git_head(root) != plan.get("implementation_head"):
        raise IntegrityError("micro execution HEAD drifted")
    implementation_hashes = plan.get("implementation_hashes")
    if not isinstance(implementation_hashes, Mapping):
        raise IntegrityError("micro implementation hashes are absent")
    for relative, expected in implementation_hashes.items():
        path = contained_path(root, str(relative))
        if sha256_file(path) != expected:
            raise IntegrityError("micro execution implementation drifted")
    _validate_bound_evidence(root)
    if build_execution_plan(root=root, implementation_head=str(plan["implementation_head"])) != plan:
        raise IntegrityError("micro execution plan no longer reconstructs")
    audit = _object(root / AUDIT_PATH, "micro execution audit")
    _self_hash(audit, "audit_id", "micro execution audit")
    if build_plan_audit(root=root) != audit:
        raise IntegrityError("micro execution audit no longer reconstructs")


def _pre_authority_source_census(root: Path, plan: Mapping[str, object]) -> None:
    """Validate paths, sidecars and sizes without opening a DBN payload."""

    for item in plan["sources"]:
        year = int(item["year"])
        if year not in ELIGIBLE_YEARS:
            raise UnauthorizedOperation("sealed source entered pre-authority census")
        dbn = contained_path(root, str(item["dbn_path"]))
        sidecar = contained_path(root, str(item["sidecar_path"]))
        dbn_stat = assert_plain_file(dbn)
        assert_plain_file(sidecar)
        if (
            dbn_stat.st_nlink != 1
            or dbn_stat.st_size != item["source_bytes"]
            or sha256_file(sidecar) != item["sidecar_sha256"]
        ):
            raise IntegrityError("micro source metadata changed before authorization")


@dataclass(frozen=True)
class _WorkerResult:
    completed: tuple[tuple[str, DecodeResult], ...]
    failure_type: str | None
    failure_request_id: str | None


def _decode_worker(
    *,
    root: Path,
    staging: Path,
    items: tuple[Mapping[str, object], ...],
    stop: threading.Event,
    decode_one: Callable[..., DecodeResult],
    started: float,
    clock: Callable[[], float],
    created_byte_budget: CreatedByteBudget,
) -> _WorkerResult:
    completed: list[tuple[str, DecodeResult]] = []
    request_id: str | None = None
    try:
        for item in items:
            if stop.is_set():
                break
            request_id = str(item["request_id"])
            if clock() - started > MAXIMUM_RUNTIME_SECONDS:
                raise TimeoutError("micro historical execution runtime ceiling reached")
            source = contained_path(root, str(item["dbn_path"]))
            output = contained_path(staging, str(item["phase1b_output_path"]))
            result = decode_one(
                source_path=source,
                output_path=output,
                market=str(item["market"]),
                source_schema=str(item["schema"]),
                exact_query=item["exact_query"],
                expected_source_sha256=str(item["source_sha256"]),
                batch_rows=MAX_BATCH_ROWS,
                created_byte_budget=created_byte_budget,
                deadline=started + MAXIMUM_RUNTIME_SECONDS,
                clock=clock,
            )
            completed.append((request_id, result))
            request_id = None
        return _WorkerResult(tuple(completed), None, None)
    except Exception as exc:
        stop.set()
        return _WorkerResult(tuple(completed), type(exc).__name__, request_id)


def _expected_economics(market: str) -> tuple[int, int, str]:
    reference = PRODUCT_REFERENCE_REQUIREMENTS[market]
    tick = int(Decimal(str(reference["tick_size"])) * Decimal(1_000_000_000))
    quantity = int(
        Decimal(str(reference["point_value_usd"])) * Decimal(1_000_000_000)
    )
    return tick, quantity, str(reference["currency"])


def _group_disposition(
    *, market: str, results: Mapping[str, DecodeResult]
) -> tuple[str, bool]:
    if set(results) != set(SCHEMAS):
        return "SOURCE_UNAVAILABLE", False
    if any(result.row_count == 0 for result in results.values()):
        return "MISSING", False
    if any(result.duplicate_count for result in results.values()):
        return "DUPLICATE", False
    if any(
        result.ambiguous_identity_count or result.non_contiguous_instrument_count
        for result in results.values()
    ):
        return "AMBIGUOUS_ROLL", False
    if results["ohlcv-1s"].row_count < results["ohlcv-1m"].row_count:
        return "SPARSE", False
    definitions = set(results["definition"].instrument_ids)
    observed = set().union(
        *(set(results[schema].instrument_ids) for schema in SCHEMAS if schema != "definition")
    )
    if not observed or not observed.issubset(definitions):
        return "AMBIGUOUS_IDENTITY", False
    expected_tick, expected_quantity, expected_currency = _expected_economics(market)
    economics: dict[int, set[tuple[int | None, int | None, str]]] = defaultdict(set)
    for instrument, tick, quantity, currency in results["definition"].economics:
        economics[instrument].add((tick, quantity, currency))
    for instrument in observed:
        if economics.get(instrument) != {(expected_tick, expected_quantity, expected_currency)}:
            return "AMBIGUOUS_IDENTITY", False
    minute_rolls = results["ohlcv-1m"].roll_sequence
    second_rolls = results["ohlcv-1s"].roll_sequence
    if minute_rolls != second_rolls:
        return "AMBIGUOUS_ROLL", False
    return "ACCEPTED", True


def _serialize_result(
    root: Path, *, item: Mapping[str, object], result: DecodeResult
) -> dict[str, object]:
    public = result.public_record()
    public.pop("decode_record_id")
    if public["output_path"] is not None:
        output = Path(str(public["output_path"]))
        if not output.is_absolute():
            output = root / output
        output = output.resolve(strict=False)
        try:
            public["output_path"] = output.relative_to(root.resolve()).as_posix()
        except ValueError as exc:
            raise IntegrityError("micro staged output escaped the repository") from exc
    core = {
        "schema_version": INTERVAL_RECEIPT_SCHEMA,
        "request_id": item["request_id"],
        "market": item["market"],
        "schema": item["schema"],
        "year": item["year"],
        "interval": item["interval"],
        "source_sha256": item["source_sha256"],
        "source_bytes": item["source_bytes"],
        "sidecar_sha256": item["sidecar_sha256"],
        "sidecar_manifest_id": item["sidecar_manifest_id"],
        "exact_query_sha256": sha256_json(item["exact_query"]),
        "phase1b_release_id": item["phase1b_release_id"],
        **public,
    }
    return {**core, "decode_record_id": sha256_json(core)}


def _write_terminal(path: Path, core: Mapping[str, object]) -> dict[str, object]:
    terminal = {**core, "terminal_id": sha256_json(core)}
    _write_create_only(path, terminal)
    return terminal


def _make_existing_outputs_read_only(root: Path) -> None:
    """Seal every completed or partial inactive file before terminal evidence."""

    if not root.exists():
        return
    for path in root.rglob("*"):
        if path.is_file():
            path.chmod(stat.S_IREAD)


def execute_authorized_phase1b2(
    *,
    root: Path,
    authorization: OperationReceipt,
    decode_one: Callable[..., DecodeResult] = decode_dbn_to_inactive_parquet,
    materialize_one: Callable[..., CausalResult] = materialize_causal_1m_inactive,
    clock: Callable[[], float] = time.monotonic,
    disk_usage: Callable[[Path], object] = shutil.disk_usage,
) -> dict[str, object]:
    """Execute one exact pre-2025 local row build into inactive staging only."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_execution_plan(root=root)
    _validate_execution_bindings(root, plan)
    _pre_authority_source_census(root, plan)
    staging = contained_path(root, str(plan["staging_root"]))
    evidence = contained_path(root, str(plan["evidence_root"]))
    if staging.exists() or evidence.exists():
        raise IntegrityError("micro Phase 1B/2 create-only output already exists")
    free = getattr(disk_usage(root), "free", None)
    if type(free) is not int or free < REQUIRED_FREE_DISK_BYTES:
        raise UnauthorizedOperation("insufficient disk for bounded micro row build")
    scope = required_scope(root=root, plan=plan)
    authorization.verify(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    staging.mkdir(parents=True, exist_ok=False)
    evidence.mkdir(parents=True, exist_ok=False)
    use_path = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    terminal_path = contained_path(root, str(plan["outputs"]["terminal"]))
    started = clock()
    base = {
        "schema_version": TERMINAL_SCHEMA,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "audit_id": _object(root / AUDIT_PATH, "micro execution audit")["audit_id"],
        "audit_sha256": sha256_file(root / AUDIT_PATH),
        "implementation_head": plan["implementation_head"],
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_schema": AUTHORIZATION_SCHEMA,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "provider_calls": 0,
        "external_cost_usd": "0",
        "automatic_retries": 0,
        "year_2025_or_2026_payloads_opened": 0,
        "credential_accessed": False,
        "raw_values_reported": False,
        "published": False,
        "catalog_or_pointer_activated": False,
        "registered": False,
        "model_fit_prediction_or_evaluation": False,
        "cleanup_performed": False,
        "trading": False,
    }
    completed: dict[str, DecodeResult] = {}
    phase2_records: list[dict[str, object]] = []
    failure: dict[str, object] | None = None
    source_hashes_verified = 0
    created_byte_budget = CreatedByteBudget(MAXIMUM_OUTPUT_BYTES)
    try:
        # Authorized opaque-byte verification is complete before the first decode.
        for item in plan["sources"]:
            if clock() - started > MAXIMUM_RUNTIME_SECONDS:
                raise TimeoutError("runtime exhausted during source hash census")
            source = contained_path(root, str(item["dbn_path"]))
            if int(item["year"]) not in ELIGIBLE_YEARS:
                raise UnauthorizedOperation("sealed source reached hash census")
            if sha256_file(source) != item["source_sha256"]:
                raise IntegrityError("micro source hash differs before decoding")
            source_hashes_verified += 1
        workers = min(MAXIMUM_WORKERS, len(plan["sources"]))
        queues = tuple(
            tuple(plan["sources"][index::workers]) for index in range(workers)
        )
        stop = threading.Event()
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="apex-micro-phase1b") as pool:
            worker_results = [
                future.result()
                for future in [
                    pool.submit(
                        _decode_worker,
                        root=root,
                        staging=staging,
                        items=queue,
                        stop=stop,
                        decode_one=decode_one,
                        started=started,
                        clock=clock,
                        created_byte_budget=created_byte_budget,
                    )
                    for queue in queues
                ]
            ]
        for worker in worker_results:
            completed.update(worker.completed)
        failures = [worker for worker in worker_results if worker.failure_type is not None]
        if failures:
            first = failures[0]
            raise IntegrityError(
                f"decode worker failed closed: {first.failure_type}:{first.failure_request_id}"
            )
        if len(completed) != EXPECTED_SOURCE_COUNT:
            raise IntegrityError("micro decode did not complete every source")
        total_output_bytes = sum(result.output_bytes for result in completed.values())
        if total_output_bytes > MAXIMUM_OUTPUT_BYTES:
            raise UnauthorizedOperation("micro staged output byte ceiling exceeded")

        item_by_id = {str(item["request_id"]): item for item in plan["sources"]}
        result_by_group: dict[tuple[str, int, str], dict[str, DecodeResult]] = defaultdict(dict)
        for request_id, result in completed.items():
            item = item_by_id[request_id]
            result_by_group[(str(item["market"]), int(item["year"]), str(item["interval"]))][str(item["schema"])] = result
        group_dispositions: dict[tuple[str, int, str], str] = {}
        for phase2 in plan["phase2"]:
            key = (str(phase2["market"]), int(phase2["year"]), str(phase2["interval"]))
            results = result_by_group[key]
            disposition, identity_certified = _group_disposition(market=key[0], results=results)
            group_dispositions[key] = disposition
            if not identity_certified:
                continue
            one_minute = results["ohlcv-1m"]
            if one_minute.output_path is None:
                raise IntegrityError("accepted group lacks one-minute Parquet")
            source_path = Path(one_minute.output_path)
            if not source_path.is_absolute():
                source_path = root / source_path
            source_path = source_path.resolve(strict=False)
            try:
                source_path.relative_to(staging.resolve())
            except ValueError as exc:
                raise IntegrityError("micro Phase 1B output escaped inactive staging") from exc
            output_path = contained_path(staging, str(phase2["phase2_output_path"]))
            causal = materialize_one(
                source_path=source_path,
                output_path=output_path,
                identity_certified=True,
                created_byte_budget=created_byte_budget,
                deadline=started + MAXIMUM_RUNTIME_SECONDS,
                clock=clock,
            )
            record = causal.public_record()
            record["output_path"] = output_path.relative_to(root).as_posix()
            record.update(
                {
                    "market": key[0],
                    "year": key[1],
                    "interval": key[2],
                    "phase2_release_id": phase2["phase2_release_id"],
                    "identity_and_economics_certified": True,
                    "roll_continuity_certified": True,
                }
            )
            phase2_records.append(record)
        total_output_bytes += sum(int(item["output_bytes"]) for item in phase2_records)
        if total_output_bytes > MAXIMUM_OUTPUT_BYTES or len(completed) + len(phase2_records) > MAXIMUM_PARQUET_OUTPUTS:
            raise UnauthorizedOperation("micro final staged output ceiling exceeded")

        decode_record_by_request = {
            request_id: _serialize_result(
                root, item=item_by_id[request_id], result=completed[request_id]
            )
            for request_id in sorted(completed)
        }
        coverage: list[dict[str, object]] = []
        for cell in plan["coverage"]:
            if cell["planned_disposition"] == "PRODUCT_NOT_YET_EFFECTIVE":
                disposition = "PRODUCT_NOT_YET_EFFECTIVE"
                decode_record_id = None
            else:
                request_id = str(cell["source_request_id"])
                result = completed[request_id]
                group_key = (str(cell["market"]), int(cell["year"]), _interval(item_by_id[request_id]))
                group_disposition = group_dispositions[group_key]
                if result.row_count == 0:
                    disposition = "MISSING"
                elif result.duplicate_count:
                    disposition = "DUPLICATE"
                elif result.ambiguous_identity_count:
                    disposition = "AMBIGUOUS_ROLL"
                elif group_disposition != "ACCEPTED":
                    disposition = group_disposition
                else:
                    disposition = "ACCEPTED"
                decode_record_id = decode_record_by_request[request_id][
                    "decode_record_id"
                ]
            coverage.append(
                {
                    "market": cell["market"],
                    "year": cell["year"],
                    "schema": cell["schema"],
                    "disposition": disposition,
                    "decode_record_id": decode_record_id,
                }
            )
        accepted = all(
            item["disposition"] in {"ACCEPTED", "PRODUCT_NOT_YET_EFFECTIVE"}
            for item in coverage
        ) and len(phase2_records) == EXPECTED_INTERVAL_COUNT
        decode_records = list(decode_record_by_request.values())
        coverage_census_core = {
            "schema_version": COVERAGE_SCHEMA,
            "cell_count": len(coverage),
            "cells": coverage,
            "disposition_counts": dict(
                sorted(Counter(str(item["disposition"]) for item in coverage).items())
            ),
        }
        coverage_census = {
            **coverage_census_core,
            "coverage_census_id": sha256_json(coverage_census_core),
        }
        report_core: dict[str, object] = {
            "schema_version": REPORT_SCHEMA,
            "state": "PASS_CERTIFIED_INACTIVE" if accepted else "FAIL_CLOSED_SOURCE_DISPOSITIONS_PRESERVED",
            "plan_id": plan["plan_id"],
            "authorization_receipt_id": authorization.receipt_id,
            "source_count": len(completed),
            "source_bytes_verified": plan["source_bytes"],
            "decode_records": decode_records,
            "phase2_records": phase2_records,
            "coverage_census": coverage_census,
            "coverage_cell_count": len(coverage),
            "disposition_counts": coverage_census["disposition_counts"],
            "identity_and_roll_continuity_certified": accepted,
            "catalog_candidate_eligible": accepted,
            "year_2025_or_2026_materialized": False,
            "provider_calls": 0,
            "external_cost_usd": "0",
            "raw_values_reported": False,
            "features_outcomes_predictions_or_evaluation_created": False,
            "published": False,
            "catalog_or_pointer_activated": False,
        }
        report = {**report_core, "source_certification_id": sha256_json(report_core)}
        report_path = contained_path(root, str(plan["outputs"]["source_certification_report"]))
        _write_create_only(report_path, report)
        catalog_written = False
        if accepted:
            aggregate_phase1b_release_id = sha256_json(
                sorted(str(item["phase1b_release_id"]) for item in plan["sources"])
            )
            aggregate_phase1b_sha = sha256_json(
                sorted(
                    str(result.output_sha256)
                    for result in completed.values()
                    if result.output_sha256 is not None
                )
            )
            aggregate_phase2_sha = sha256_json(
                sorted(str(item["output_sha256"]) for item in phase2_records)
            )
            candidate = {
                "lane_id": LANE_ID,
                "contract_scale": "MICRO_INTEGER_ONLY",
                "state": "CERTIFIED_INACTIVE_NOT_PUBLISHED",
                "source_certification_id": report["source_certification_id"],
                "source_certification_sha256": sha256_file(report_path),
                "coverage_census_id": coverage_census["coverage_census_id"],
                "coverage_cell_count": len(coverage),
                "phase1b_release_id": aggregate_phase1b_release_id,
                "phase1b_release_sha256": aggregate_phase1b_sha,
                "phase2_release_id": sha256_json(
                    sorted(str(item["phase2_release_id"]) for item in phase2_records)
                ),
                "phase2_release_sha256": aggregate_phase2_sha,
                "markets": list(TIER_1_MARKETS),
                "years": list(ELIGIBLE_YEARS),
                "disposition_census_complete": True,
                "actual_identity_and_roll_continuity_certified": True,
                "holdout_2025_materialized": False,
                "forward_2026_materialized": False,
            }
            require_row_certified_catalog_candidate(candidate)
            candidate_core = {
                "schema_version": CATALOG_SCHEMA,
                "future_active_path": ACTIVE_MICRO_CATALOG_PATH.as_posix(),
                "published": False,
                "activated": False,
                "candidate": candidate,
            }
            candidate_document = {
                **candidate_core,
                "candidate_document_id": sha256_json(candidate_core),
            }
            candidate_path = contained_path(root, str(plan["outputs"]["inactive_catalog_candidate"]))
            _write_create_only(candidate_path, candidate_document)
            catalog_written = True
        state = "SUCCESS_CERTIFIED_INACTIVE_NOT_PUBLISHED" if accepted else "FAIL_CLOSED_SOURCE_CERTIFICATION"
        terminal_core = {
            **base,
            "state": state,
            "source_hashes_verified_before_decode": source_hashes_verified,
            "completed_decode_count": len(completed),
            "completed_phase2_count": len(phase2_records),
            "coverage_cell_count": len(coverage),
            "source_certification_id": report["source_certification_id"],
            "source_certification_sha256": sha256_file(report_path),
            "inactive_catalog_candidate_written": catalog_written,
            "created_output_bytes": created_byte_budget.used,
            "failure": None if accepted else {"type": "SourceCertificationFailure"},
            "attempts": 1,
            "terminal_written_last": True,
        }
        _make_existing_outputs_read_only(staging)
        return _write_terminal(terminal_path, terminal_core)
    except Exception as exc:
        failure = {"exception_type": type(exc).__name__}
        terminal_core = {
            **base,
            "state": "FAILURE_INACTIVE_PARTIAL_EVIDENCE_PRESERVED",
            "source_hashes_verified_before_decode": source_hashes_verified,
            "completed_decode_count": len(completed),
            "completed_phase2_count": len(phase2_records),
            "coverage_cell_count": 0,
            "source_certification_id": None,
            "source_certification_sha256": None,
            "inactive_catalog_candidate_written": False,
            "created_output_bytes": created_byte_budget.used,
            "failure": failure,
            "attempts": 1,
            "terminal_written_last": True,
        }
        _make_existing_outputs_read_only(staging)
        return _write_terminal(terminal_path, terminal_core)


__all__ = [
    "AUDIT_PATH",
    "ELIGIBLE_YEARS",
    "EVIDENCE_ROOT",
    "EXPECTED_COVERAGE_CELL_COUNT",
    "EXPECTED_SOURCE_BYTES",
    "EXPECTED_SOURCE_COUNT",
    "MAXIMUM_OUTPUT_BYTES",
    "MAXIMUM_RUNTIME_SECONDS",
    "MAXIMUM_WORKERS",
    "OPERATION",
    "PLAN_PATH",
    "REQUIRED_FREE_DISK_BYTES",
    "STAGING_ROOT",
    "build_execution_plan",
    "build_plan_audit",
    "execute_authorized_phase1b2",
    "load_execution_plan",
    "required_scope",
    "write_execution_plan_create_only",
    "write_plan_audit_create_only",
]
