"""Approved read-only readiness census for the multi-family OHLCV mechanism."""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Mapping, Sequence

import pyarrow.parquet as pq

from .canonical import sha256_file, sha256_json, write_new_canonical_authority
from .causal_observation_parquet import SCHEMAS
from .errors import IntegrityError
from .fixed_hour_multifamily_discovery import FEATURE_NAMES, NANO, frozen_design, prepare_session
from .fixed_hour_ridge_audit_successor import (
    purged_embargoed_inner_folds,
    purged_embargoed_outer_pilot,
)
from .fixed_hour_ridge_discovery import checkpoint_ns
from .fixed_hour_ridge_readiness import BATCH_ROWS, READ_COLUMNS, _valid_row, load_fixed_hour_schedule


SELF = Path("src/futures_rebuild/fixed_hour_multifamily_readiness.py")
IMPLEMENTATION = Path("src/futures_rebuild/fixed_hour_multifamily_discovery.py")
CORRECTED_MECHANICS = Path("src/futures_rebuild/fixed_hour_ridge_audit_successor.py")
CATALOG = Path("data/causally_gated_normalized/_control/catalog.json")
OUTPUT_ROOT = Path("state/fixed_hour_multifamily_readiness")
APPROVED_YEARS = (2018, 2019, 2020, 2021, 2022)
APPROVED_ROWS = 1_755_614
APPROVED_BYTES = 147_844_292


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"multi-family readiness input is unreadable: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"multi-family readiness input is not an object: {path}")
    return value


def _sources(root: Path) -> list[dict[str, object]]:
    catalog = _read_json(root / CATALOG)
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise IntegrityError("active catalog entries are missing")
    selected = sorted(
        (
            entry for entry in entries
            if isinstance(entry, dict) and entry.get("market") == "ES"
            and entry.get("selection_eligible") is True
            and entry.get("year") in APPROVED_YEARS
        ),
        key=lambda entry: int(entry["year"]),
    )
    expected_paths = [
        f"data/active/causally_gated_normalized/ES/{year}/{year}.parquet"
        for year in APPROVED_YEARS
    ]
    if (
        len(selected) != len(APPROVED_YEARS)
        or [entry.get("year") for entry in selected] != list(APPROVED_YEARS)
        or [entry.get("parquet_path") for entry in selected] != expected_paths
        or sum(int(entry.get("row_count", -1)) for entry in selected) != APPROVED_ROWS
    ):
        raise IntegrityError("active catalog does not match the approved ES scope")
    for entry in selected:
        expected_sidecar = f"{entry['parquet_path']}.manifest.json"
        if entry.get("sidecar_path") != expected_sidecar:
            raise IntegrityError("active catalog sidecar path is outside the approved scope")
    if sum((root / str(entry["parquet_path"])).stat().st_size for entry in selected) != APPROVED_BYTES:
        raise IntegrityError("approved ES physical byte scope changed")
    return selected


def _assess_session(
    rows: Sequence[Mapping[str, object]], authoritative_schedule_state: str,
) -> dict[str, object]:
    if not rows:
        raise IntegrityError("cannot assess an empty multi-family session")
    session_id = str(rows[0].get("project_session_id", ""))
    trade_date = str(rows[0].get("project_trade_date", ""))
    for row in rows:
        _valid_row(row, session_id, "ES")
    if (
        len({row.get("project_trade_date") for row in rows}) != 1
        or len({row.get("project_timezone") for row in rows}) != 1
        or len({row.get("project_grouping_start_ns") for row in rows}) != 1
        or len({row.get("project_grouping_end_ns") for row in rows}) != 1
        or authoritative_schedule_state not in {
            "AUTHORITATIVE_OPEN", "AUTHORITATIVE_CALENDAR_INELIGIBLE",
            "AUTHORITATIVE_SOURCE_UNOBSERVABLE",
        }
    ):
        raise IntegrityError("multi-family session or schedule identity is invalid")
    base = {
        "session_id": session_id,
        "trade_date": trade_date,
        "schedule_state": authoritative_schedule_state,
        "feature_ready": False,
        "mechanics_ready": False,
        "label_start_ns": None,
        "label_end_ns": None,
    }
    if authoritative_schedule_state == "AUTHORITATIVE_CALENDAR_INELIGIBLE":
        return {**base, "status": "CALENDAR_INELIGIBLE"}
    if authoritative_schedule_state == "AUTHORITATIVE_SOURCE_UNOBSERVABLE":
        return {**base, "status": "SOURCE_UNOBSERVABLE"}
    decision = checkpoint_ns(trade_date, str(rows[0]["project_timezone"]))
    if not int(rows[0]["project_grouping_start_ns"]) <= decision < int(rows[0]["project_grouping_end_ns"]):
        return {**base, "status": "CHECKPOINT_OUTSIDE_SESSION"}
    tick_size = int(rows[0]["min_price_increment_nano"]) / NANO
    multiplier = int(rows[0]["multiplier_nano"]) / NANO
    stress_cost_usd = 10.0 + 4.0 * tick_size * multiplier
    try:
        prepared = prepare_session(rows, decision, stress_cost_usd=stress_cost_usd)
    except IntegrityError as exc:
        return {**base, "status": "FEATURE_OR_IDENTITY_INDETERMINATE", "reason": str(exc)}
    if prepared["status"] != "SCORABLE":
        return {
            **base,
            "feature_ready": True,
            "status": str(prepared["status"]),
        }
    features = prepared["features"]
    outcome = prepared["outcome"]
    ratios = prepared["stress_net_mfe_mae"]
    if (
        not isinstance(features, Mapping) or tuple(features) != FEATURE_NAMES
        or not isinstance(outcome, Mapping) or not isinstance(ratios, Mapping)
        or not all(math.isfinite(float(value)) for value in features.values())
        or not math.isfinite(float(prepared["signed_gross_r"]))
        or not math.isfinite(float(prepared["stress_cost_r"]))
        or not all(math.isfinite(float(value)) for value in ratios.values())
    ):
        raise IntegrityError("multi-family feature or outcome mechanics are incomplete")
    return {
        **base,
        "status": "READY",
        "feature_ready": True,
        "mechanics_ready": True,
        "feature_count": len(features),
        "actual_contract": prepared["feature_audit"]["actual_contract"],
        "label_start_ns": int(outcome["entry"]["bar_start_ns"]),
        "label_end_ns": int(outcome["exit"]["bar_start_ns"]),
    }


def _fold_readiness(sessions: Sequence[Mapping[str, object]]) -> dict[str, object]:
    ready = {
        str(session["session_id"]): session
        for session in sessions if session.get("status") == "READY"
    }
    if len(ready) != sum(session.get("status") == "READY" for session in sessions):
        raise IntegrityError("ready sessions have duplicate identities")
    ordered = sorted(ready, key=lambda session_id: int(ready[session_id]["label_start_ns"]))
    outer = purged_embargoed_outer_pilot(ready, ordered)
    inner = purged_embargoed_inner_folds(ready, outer["training_session_ids"])
    return {
        "ready_session_count": len(ready),
        "outer": {
            "training_sessions": len(outer["training_session_ids"]),
            "purged_sessions": len(outer["purged_session_ids"]),
            "embargo_sessions": len(outer["embargo_session_ids"]),
            "evaluation_sessions": len(outer["evaluation_session_ids"]),
            "training_ids_sha256": sha256_json(outer["training_session_ids"]),
            "embargo_ids_sha256": sha256_json(outer["embargo_session_ids"]),
            "evaluation_ids_sha256": sha256_json(outer["evaluation_session_ids"]),
        },
        "inner": [
            {
                "fold_id": fold["fold_id"],
                "training_sessions": len(fold["training_session_ids"]),
                "purged_sessions": len(fold["purged_session_ids"]),
                "embargo_sessions": len(fold["embargo_session_ids"]),
                "validation_sessions": len(fold["validation_session_ids"]),
                "training_ids_sha256": sha256_json(fold["training_session_ids"]),
                "embargo_ids_sha256": sha256_json(fold["embargo_session_ids"]),
                "validation_ids_sha256": sha256_json(fold["validation_session_ids"]),
            }
            for fold in inner
        ],
        "checks": {
            "outer_504_train_1_embargo_63_evaluation": (
                len(outer["training_session_ids"]) == 504
                and len(outer["embargo_session_ids"]) == 1
                and len(outer["evaluation_session_ids"]) == 63
            ),
            "four_inner_folds": len(inner) == 4,
            "one_embargo_session_each_inner_fold": all(len(fold["embargo_session_ids"]) == 1 for fold in inner),
            "forty_two_validation_sessions_each_inner_fold": all(len(fold["validation_session_ids"]) == 42 for fold in inner),
            "minimum_252_training_sessions_each_inner_fold": all(len(fold["training_session_ids"]) >= 252 for fold in inner),
        },
    }


def run_census(root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    design = frozen_design()
    schedule = load_fixed_hour_schedule(root, "ES")
    schedule_sessions = schedule["sessions"]
    assert isinstance(schedule_sessions, Mapping)
    sources = _sources(root)
    sessions: list[dict[str, object]] = []
    current_id: str | None = None
    current_rows: list[dict[str, object]] = []
    last_start: int | None = None
    observed_rows = observed_bytes = 0
    source_reads = []

    def finish_session() -> None:
        nonlocal current_rows
        if not current_rows:
            return
        trade_date = str(current_rows[0]["project_trade_date"])
        schedule_state = schedule_sessions.get(trade_date)
        if not isinstance(schedule_state, str):
            raise IntegrityError("multi-family session lacks calendar authority")
        sessions.append(_assess_session(current_rows, schedule_state))
        current_rows = []

    for source in sources:
        relative = str(source["parquet_path"])
        sidecar_relative = str(source["sidecar_path"])
        path = root / relative
        sidecar_path = root / sidecar_relative
        sidecar = _read_json(sidecar_path)
        binding = sidecar.get("entry_binding")
        if (
            not isinstance(binding, Mapping)
            or sha256_file(path) != source.get("parquet_sha256")
            or sha256_file(sidecar_path) != source.get("sidecar_sha256")
            or binding.get("parquet_path") != relative
            or binding.get("parquet_sha256") != source.get("parquet_sha256")
            or binding.get("row_count") != source.get("row_count")
            or binding.get("selection_eligible") is not True
        ):
            raise IntegrityError("multi-family source or manifest binding changed")
        parquet = pq.ParquetFile(path)
        if (
            parquet.schema_arrow.names != SCHEMAS["observations"].names
            or not set(READ_COLUMNS).issubset(parquet.schema_arrow.names)
            or parquet.metadata.num_rows != int(source["row_count"])
        ):
            raise IntegrityError("multi-family Parquet schema or row count changed")
        file_rows = 0
        for batch in parquet.iter_batches(batch_size=BATCH_ROWS, columns=list(READ_COLUMNS)):
            file_rows += batch.num_rows
            for row in batch.to_pylist():
                start = int(row["bar_start_ns"])
                if last_start is not None and start <= last_start:
                    raise IntegrityError("approved ES sources are not globally chronological")
                last_start = start
                session_id = str(row["project_session_id"])
                if current_id is None:
                    current_id = session_id
                if session_id != current_id:
                    finish_session()
                    current_id = session_id
                current_rows.append(row)
        if file_rows != int(source["row_count"]):
            raise IntegrityError("multi-family source row count changed during scan")
        size = path.stat().st_size
        observed_rows += file_rows
        observed_bytes += size
        source_reads.append({
            "year": source["year"],
            "parquet_path": relative,
            "sidecar_path": sidecar_relative,
            "rows_read": file_rows,
            "physical_bytes": size,
            "parquet_sha256": source["parquet_sha256"],
            "sidecar_sha256": source["sidecar_sha256"],
        })
    finish_session()
    if observed_rows != APPROVED_ROWS or observed_bytes != APPROVED_BYTES:
        raise IntegrityError("multi-family census exceeded or missed its approved scope")

    status_counts = Counter(str(session["status"]) for session in sessions)
    schedule_counts = Counter(str(session["schedule_state"]) for session in sessions)
    folds = _fold_readiness(sessions)
    checks = {
        "exact_approved_files_rows_and_bytes": len(source_reads) == 5,
        "all_18_features_finite_when_ready": all(
            session.get("feature_count") == len(FEATURE_NAMES)
            for session in sessions if session.get("status") == "READY"
        ),
        "timing_contract_and_economics_fail_closed": True,
        "complete_10_01_to_11_01_outcome_required": True,
        "corrected_purge_and_embargo_folds": all(folds["checks"].values()),
        "enough_ready_sessions_for_pilot": int(folds["ready_session_count"]) >= 568,
        "every_session_terminalized": sum(status_counts.values()) == len(sessions),
        "no_model_fit_selection_prediction_or_performance_evaluation": True,
        "scope_excludes_2023_plus_holdout_forward_raw_dbn_provider_and_trading": True,
    }
    state = "PASS_READINESS_ONLY_MODEL_EXECUTION_NOT_AUTHORIZED" if all(checks.values()) else "FAIL_READINESS"
    core = {
        "schema_version": "fixed_hour_multifamily_readiness/1.0.0",
        "state": state,
        "design_id": design["design_id"],
        "bindings": {
            "implementation": {"path": IMPLEMENTATION.as_posix(), "sha256": sha256_file(root / IMPLEMENTATION)},
            "corrected_mechanics": {"path": CORRECTED_MECHANICS.as_posix(), "sha256": sha256_file(root / CORRECTED_MECHANICS)},
            "readiness": {"path": SELF.as_posix(), "sha256": sha256_file(root / SELF)},
            "active_catalog": {"path": CATALOG.as_posix(), "sha256": sha256_file(root / CATALOG)},
        },
        "approved_scope": {
            "market": "ES",
            "years": list(APPROVED_YEARS),
            "parquet_files": [source["parquet_path"] for source in sources],
            "sidecar_files": [source["sidecar_path"] for source in sources],
            "rows": APPROVED_ROWS,
            "physical_bytes": APPROVED_BYTES,
        },
        "observed_access": {
            "parquet_files_read": len(source_reads),
            "sidecar_files_read": len(source_reads),
            "rows_read": observed_rows,
            "physical_bytes": observed_bytes,
            "sources": source_reads,
        },
        "coverage": {
            "source_session_count": len(sessions),
            "status_counts": dict(sorted(status_counts.items())),
            "schedule_state_counts": dict(sorted(schedule_counts.items())),
            "feature_ready_sessions": sum(bool(session["feature_ready"]) for session in sessions),
            "mechanics_ready_sessions": sum(bool(session["mechanics_ready"]) for session in sessions),
            "nonready_sessions": [
                {
                    key: session[key]
                    for key in ("session_id", "trade_date", "schedule_state", "status", "reason")
                    if key in session
                }
                for session in sessions if session["status"] != "READY"
            ],
        },
        "fold_readiness": folds,
        "checks": checks,
        "does_not_report": ["FEATURE_VALUES", "TARGET_VALUES", "RETURNS", "PNL", "MFE_MAE_VALUES", "PREDICTIONS", "ALPHA_METRICS"],
        "authority": {
            "model_fit_selection_or_evaluation": False,
            "trial_registration_or_execution": False,
            "2023_plus_holdout_forward_or_provider": False,
            "publication_activation_trading_or_push": False,
        },
    }
    return {**core, "readiness_id": sha256_json(core)}


def execute(root: Path) -> dict[str, object]:
    result = run_census(root)
    output = OUTPUT_ROOT / str(result["readiness_id"]) / "readiness.json"
    write_new_canonical_authority(root.resolve(strict=True) / output, result)
    return {
        "state": result["state"],
        "readiness_id": result["readiness_id"],
        "readiness_path": output.as_posix(),
        "rows_read": result["observed_access"]["rows_read"],
        "physical_bytes": result["observed_access"]["physical_bytes"],
    }


if __name__ == "__main__":
    print(json.dumps(execute(Path.cwd()), sort_keys=True))
