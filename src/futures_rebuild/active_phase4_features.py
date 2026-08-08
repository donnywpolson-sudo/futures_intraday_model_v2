"""Build a bounded causal feature release from one verified active ES view."""

from __future__ import annotations

import json
import math
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .active_phase3_input import ActivePhase3Input, load_active_phase3_input
from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import ContractError, IntegrityError


SCHEMA_VERSION = "active_phase4_feature_release/1.0.0"
FEATURE_METHOD_ID = "active_es_mechanical_v3"
INPUT_RECORD_PATH = (
    "manifests/phase3_inputs/"
    "cf850301855f9763888ababd50a3400bd2e28e73be698ea3c16f06700717630a.json"
)
FEATURE_SPEC_PATH = "configs/mechanical_feature_spec.json"
FEATURE_NAMES = (
    "bar_body_fraction",
    "bar_return",
    "intrabar_range_fraction",
    "volume",
)
_REQUIRED_COLUMNS = (
    "market",
    "event_at_ns",
    "available_at_ns",
    "open_nano",
    "high_nano",
    "low_nano",
    "close_nano",
    "volume",
    "disposition",
    "actual_identity_hash",
    "exchange_session_date",
    "source_row_sha256",
)


def _load_json(path: Path, *, description: str) -> Mapping[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is unreadable") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{description} must be an object")
    return value


def _verify_input_record(
    *, boundary: RepoBoundary, active_input: ActivePhase3Input, relative_path: str
) -> str:
    path = boundary.assert_active_path(
        boundary.active_root / relative_path,
        purpose="active feature input record",
        subtree="manifests/phase3_inputs",
    )
    expected = {
        "active_view_id": active_input.active_view_id,
        "causal_release_id": active_input.causal_release_id,
        "input_id": active_input.input_id,
        "market": active_input.market,
        "parquet_path": active_input.parquet_path,
        "parquet_sha256": active_input.parquet_sha256,
        "schema_version": "phase3_active_input_record/1.0.0",
        "sidecar_sha256": active_input.sidecar_sha256,
        "source_raw_release_id": active_input.source_raw_release_id,
        "year": active_input.year,
    }
    if dict(_load_json(path, description="active feature input record")) != expected:
        raise IntegrityError("active feature input record differs from the verified active view")
    return sha256_file(path)


def _load_feature_spec(*, boundary: RepoBoundary, relative_path: str) -> tuple[dict[str, object], str]:
    path = boundary.assert_active_path(
        boundary.active_root / relative_path,
        purpose="active mechanical feature specification",
        subtree="configs",
    )
    spec = dict(_load_json(path, description="active mechanical feature specification"))
    expected = {
        "entry_delay_seconds": 60,
        "feature_names": list(FEATURE_NAMES),
        "formulas": {
            "bar_body_fraction": "(close_nano-open_nano)/open_nano",
            "bar_return": "close_nano/open_nano-1",
            "intrabar_range_fraction": "(high_nano-low_nano)/open_nano",
            "volume": "exact_nonnegative_volume",
        },
        "label_horizon_seconds": 300,
        "spec_version": "1.0.0",
    }
    if spec != expected:
        raise IntegrityError("active mechanical feature specification is not the approved fixed spec")
    return spec, sha256_file(path)


@dataclass(frozen=True)
class ActivePhase4FeatureBinding:
    """The complete source and specification binding for one feature build."""

    active_input: ActivePhase3Input
    input_record_path: str
    input_record_sha256: str
    feature_spec_path: str
    feature_spec_sha256: str
    feature_spec: Mapping[str, object]

    def __post_init__(self) -> None:
        if (
            type(self.active_input) is not ActivePhase3Input
            or not self.input_record_path.startswith("manifests/phase3_inputs/")
            or self.feature_spec_path != FEATURE_SPEC_PATH
            or not all(
                isinstance(value, str) and len(value) == 64
                for value in (self.input_record_sha256, self.feature_spec_sha256)
            )
            or list(self.feature_spec.get("feature_names", [])) != list(FEATURE_NAMES)
        ):
            raise ContractError("active Phase 4 feature binding is not exact")


def prepare_active_phase4_feature_binding(*, boundary: RepoBoundary) -> ActivePhase4FeatureBinding:
    """Bind the sole allowed ES 2019 source and fixed feature specification."""
    active_input = load_active_phase3_input(boundary=boundary, market="ES", year=2019)
    input_hash = _verify_input_record(
        boundary=boundary, active_input=active_input, relative_path=INPUT_RECORD_PATH
    )
    spec, spec_hash = _load_feature_spec(boundary=boundary, relative_path=FEATURE_SPEC_PATH)
    return ActivePhase4FeatureBinding(
        active_input=active_input,
        input_record_path=INPUT_RECORD_PATH,
        input_record_sha256=input_hash,
        feature_spec_path=FEATURE_SPEC_PATH,
        feature_spec_sha256=spec_hash,
        feature_spec=spec,
    )


def _load_rows(parquet_path: Path) -> list[Mapping[str, object]]:
    try:
        import pyarrow.parquet as pq

        reader = pq.ParquetFile(parquet_path)
        if not set(_REQUIRED_COLUMNS).issubset(reader.schema_arrow.names):
            raise IntegrityError("active feature input is missing required columns")
        rows: list[Mapping[str, object]] = []
        for batch in reader.iter_batches(batch_size=65_536, columns=list(_REQUIRED_COLUMNS)):
            rows.extend(batch.to_pylist())
    except IntegrityError:
        raise
    except Exception as exc:
        raise IntegrityError("active feature input could not be read") from exc
    if not rows:
        raise IntegrityError("active feature input has no rows")
    return rows


def _feature_values(row: Mapping[str, object]) -> dict[str, float] | None:
    event_at = row.get("event_at_ns")
    available_at = row.get("available_at_ns")
    open_nano = row.get("open_nano")
    high_nano = row.get("high_nano")
    low_nano = row.get("low_nano")
    close_nano = row.get("close_nano")
    volume = row.get("volume")
    if (
        row.get("disposition") != "ELIGIBLE"
        or type(event_at) is not int
        or type(available_at) is not int
        or available_at < event_at
        or not all(type(value) is int for value in (open_nano, high_nano, low_nano, close_nano, volume))
        or open_nano <= 0
        or high_nano < low_nano
        or volume < 0
    ):
        return None
    values = {
        "bar_body_fraction": (close_nano - open_nano) / open_nano,
        "bar_return": close_nano / open_nano - 1.0,
        "intrabar_range_fraction": (high_nano - low_nano) / open_nano,
        "volume": float(volume),
    }
    if not all(math.isfinite(value) for value in values.values()):
        return None
    return values


def _decision_at_ns(row: Mapping[str, object]) -> int:
    available = row.get("available_at_ns")
    event = row.get("event_at_ns")
    if type(available) is not int or type(event) is not int:
        raise IntegrityError("active feature availability time is invalid")
    minute = 60 * 1_000_000_000
    return ((max(available, event) + minute - 1) // minute) * minute


def build_active_phase4_features(
    *, boundary: RepoBoundary, binding: ActivePhase4FeatureBinding
) -> dict[str, str | int]:
    """Create one immutable, outcome-independent ES 2019 feature release."""
    from .current_research_surface import reject_retired_project_execution

    reject_retired_project_execution(
        root=boundary.active_root, surface="legacy active Phase 4 feature builder",
    )
    if type(binding) is not ActivePhase4FeatureBinding:
        raise ContractError("active Phase 4 feature build requires an exact binding")
    parquet = boundary.assert_active_path(
        boundary.active_root / binding.active_input.parquet_path,
        purpose="active Phase 4 features parquet",
        subtree="data/active",
    )
    rows = _load_rows(parquet)
    if any(row.get("market") != binding.active_input.market for row in rows):
        raise IntegrityError("active feature input market differs from its binding")
    if any(type(row.get("event_at_ns")) is not int for row in rows):
        raise IntegrityError("active feature input event identity is invalid")
    if any(rows[index - 1]["event_at_ns"] > rows[index]["event_at_ns"] for index in range(1, len(rows))):
        raise IntegrityError("active feature input is not ordered by event time")

    feature_rows: list[dict[str, object]] = []
    for row in rows:
        values = _feature_values(row)
        feature_rows.append(
            {
                "bar_event_at_ns": row["event_at_ns"],
                "decision_at_ns": _decision_at_ns(row),
                "planned_entry_at_ns": _decision_at_ns(row) + 60 * 1_000_000_000,
                "label_unlock_at_ns": _decision_at_ns(row) + 300 * 1_000_000_000,
                "available_at_ns": row["available_at_ns"],
                "status": "FEATURE_READY" if values is not None else "UNAVAILABLE_OR_INELIGIBLE",
                "actual_identity_hash": row["actual_identity_hash"],
                "exchange_session_date": row["exchange_session_date"],
                "upstream_source_row_sha256": row["source_row_sha256"],
                **{name: values[name] if values is not None else None for name in FEATURE_NAMES},
            }
        )

    stage = boundary.active_root / "state" / "data_publication_staging" / f"active_phase4_features-{uuid.uuid4().hex}"
    stage.mkdir(parents=True)
    staged_features = stage / "features.parquet"
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        pq.write_table(pa.Table.from_pylist(feature_rows), staged_features, compression="zstd")
        feature_hash = sha256_file(staged_features)
        market, year = binding.active_input.market, binding.active_input.year
        logical_path = f"data/features/{FEATURE_METHOD_ID}/{market}/{year}/{year}/features.parquet"
        core = {
            "schema_version": SCHEMA_VERSION,
            "feature_method_id": FEATURE_METHOD_ID,
            "logical_path": logical_path,
            "features_sha256": feature_hash,
            "feature_count": len(feature_rows),
            "source_active_input_id": binding.active_input.input_id,
            "source_input_record_sha256": binding.input_record_sha256,
            "feature_spec_sha256": binding.feature_spec_sha256,
        }
        release_id = sha256_json(core)
        target = boundary.active_root / "data" / "features" / FEATURE_METHOD_ID / market / str(year) / str(year) / release_id / "features.parquet"
        manifest_path = boundary.active_root / "manifests" / "data_releases" / "features" / f"{release_id}.json"
        report_path = boundary.active_root / "reports" / "phase4_features" / "tier1_core" / market / str(year) / release_id / "report.json"
        if target.exists() or manifest_path.exists() or report_path.exists():
            raise IntegrityError("active Phase 4 feature target already exists")
        target.parent.mkdir(parents=True)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.parent.mkdir(parents=True)
        shutil.copy2(staged_features, target)
        ready_count = sum(item["status"] == "FEATURE_READY" for item in feature_rows)
        report = {
            **core,
            "release_id": release_id,
            "feature_names": list(FEATURE_NAMES),
            "decision_time_basis": "first_minute_boundary_at_or_after_available_at_ns",
            "feature_ready_count": ready_count,
            "unavailable_or_ineligible_count": len(feature_rows) - ready_count,
            "model_fitting": False,
            "prediction_generation": False,
            "economics_evaluation": False,
        }
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "release_id": release_id,
            "source_active_input_id": binding.active_input.input_id,
            "source_parquet_sha256": binding.active_input.parquet_sha256,
            "feature_spec_sha256": binding.feature_spec_sha256,
            "files": [{"logical_path": logical_path, "sha256": feature_hash, "size": target.stat().st_size}],
            "metadata": report,
        }
        manifest_path.write_bytes(canonical_bytes(manifest) + b"\n")
        report_path.write_bytes(canonical_bytes(report) + b"\n")
    finally:
        shutil.rmtree(stage, ignore_errors=True)
    return {
        "release_id": release_id,
        "feature_count": len(feature_rows),
        "manifest_path": manifest_path.relative_to(boundary.active_root).as_posix(),
        "report_path": report_path.relative_to(boundary.active_root).as_posix(),
    }
