"""Prepare one bounded Phase 3 mechanics check from an accepted active view.

This is deliberately a metadata-only preflight.  It binds the existing active
input record and frozen timing policy, but never parses Parquet rows, labels an
outcome, registers a trial, or creates a research result.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from .active_phase3_input import ActivePhase3Input, load_active_phase3_input
from .boundary import RepoBoundary
from .canonical import sha256_file, sha256_json
from .errors import ContractError, IntegrityError


SCHEMA_VERSION = "active_phase3_mechanics_validation/1.0.0"
INPUT_SCHEMA_VERSION = "phase3_active_input_record/1.0.0"
ENTRY_DELAY_SECONDS = 60
LABEL_HORIZON_SECONDS = 300


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise IntegrityError(f"{name} must be an object")
    return value


def _load_input_record(
    *, boundary: RepoBoundary, relative_path: str, active_input: ActivePhase3Input
) -> str:
    path = boundary.assert_active_path(
        boundary.active_root / relative_path,
        purpose="Phase 3 active input record",
        subtree="manifests/phase3_inputs",
    )
    try:
        payload = _mapping(json.loads(path.read_text(encoding="utf-8")), "input record")
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("Phase 3 active input record is unreadable") from exc
    expected = {
        "active_view_id": active_input.active_view_id,
        "causal_release_id": active_input.causal_release_id,
        "input_id": active_input.input_id,
        "market": active_input.market,
        "parquet_path": active_input.parquet_path,
        "parquet_sha256": active_input.parquet_sha256,
        "schema_version": INPUT_SCHEMA_VERSION,
        "sidecar_sha256": active_input.sidecar_sha256,
        "source_raw_release_id": active_input.source_raw_release_id,
        "year": active_input.year,
    }
    if dict(payload) != expected:
        raise IntegrityError("Phase 3 active input record differs from the verified active view")
    return sha256_file(path)


@dataclass(frozen=True)
class ActivePhase3MechanicsValidation:
    """A fixed, non-executing specification for one later bounded row check."""

    active_input: ActivePhase3Input
    input_record_path: str
    input_record_sha256: str
    entry_delay_seconds: int = ENTRY_DELAY_SECONDS
    label_horizon_seconds: int = LABEL_HORIZON_SECONDS
    maximum_markets: int = 1
    maximum_years: int = 1
    maximum_row_reads: int = 0

    def __post_init__(self) -> None:
        if (
            type(self.active_input) is not ActivePhase3Input
            or type(self.input_record_path) is not str
            or not self.input_record_path.startswith("manifests/phase3_inputs/")
            or type(self.input_record_sha256) is not str
            or len(self.input_record_sha256) != 64
            or type(self.entry_delay_seconds) is not int
            or self.entry_delay_seconds != ENTRY_DELAY_SECONDS
            or type(self.label_horizon_seconds) is not int
            or self.label_horizon_seconds != LABEL_HORIZON_SECONDS
            or type(self.maximum_markets) is not int
            or self.maximum_markets != 1
            or type(self.maximum_years) is not int
            or self.maximum_years != 1
            or type(self.maximum_row_reads) is not int
            or self.maximum_row_reads != 0
        ):
            raise ContractError("Phase 3 mechanics validation must remain bounded and metadata-only")

    def core(self) -> dict[str, object]:
        return {
            "entry_delay_seconds": self.entry_delay_seconds,
            "input_id": self.active_input.input_id,
            "input_record_path": self.input_record_path,
            "input_record_sha256": self.input_record_sha256,
            "label_horizon_seconds": self.label_horizon_seconds,
            "maximum_markets": self.maximum_markets,
            "maximum_row_reads": self.maximum_row_reads,
            "maximum_years": self.maximum_years,
            "schema_version": SCHEMA_VERSION,
        }

    @property
    def validation_id(self) -> str:
        return sha256_json(self.core())


def prepare_active_phase3_mechanics_validation(
    *,
    boundary: RepoBoundary,
    market: str = "ES",
    year: int = 2019,
    input_record_path: str = (
        "manifests/phase3_inputs/"
        "cf850301855f9763888ababd50a3400bd2e28e73be698ea3c16f06700717630a.json"
    ),
) -> ActivePhase3MechanicsValidation:
    """Bind the approved ES input without parsing any historical price rows."""

    active_input = load_active_phase3_input(boundary=boundary, market=market, year=year)
    input_hash = _load_input_record(
        boundary=boundary,
        relative_path=input_record_path,
        active_input=active_input,
    )
    return ActivePhase3MechanicsValidation(
        active_input=active_input,
        input_record_path=input_record_path,
        input_record_sha256=input_hash,
    )
