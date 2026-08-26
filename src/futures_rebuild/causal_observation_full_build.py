"""Bounded, non-public full development causal-observation builder.

The module is inert without an exact externally issued one-use receipt.  It
validates the complete active v4 source inventory without payload access,
consumes that receipt, and only then decodes one standard-market year at a
time.  It creates independently verified monthly candidates beneath the
packet-bound staging root; it cannot publish or activate them.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import databento

from .boundary import OperationReceipt, RepoBoundary
from .canonical import (
    canonical_bytes,
    io_path,
    iter_plain_files,
    sha256_file,
    sha256_json,
)
from .causal_observation_canary import (
    DecodedMarket,
    _CanaryStageCreator,
    _build_market_candidate_with_state,
    _decode_selected_sources,
    _load_economics_rulebook,
    _query_contract,
)
from .causal_observation_foundation import (
    CAUSAL_OBSERVATION_CONTRACT_ID,
    CausalObservationOperationContext,
    ECONOMICS_RULEBOOK_PATH,
    ECONOMICS_RULEBOOK_SHA256,
    authorize_full_build_row_read,
    prepared_inventory,
)
from .causal_observation_verifier import verify_observation_candidate
from .causal_source_closure import (
    select_exact_standard_source_entries,
    validate_full_build_selection_contract,
)
from .data_layout import STAGING_ROOT
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .locking import FileLease
from .foundation_operation_firewall import issue_current_source_closure_context
from .foundation.decoder import SUPPORTED_DATABENTO_VERSION
from .foundation.economics import EconomicsRuleBook
from .research_gateway_policy import CAUSAL_OBSERVATION_FULL_BUILD_OPERATION


PLAN_SCHEMA = "development_causal_observation_full_build_plan/1.5.0"
RESULT_SCHEMA = "development_causal_observation_full_build_result/1.0.0"
FAILURE_SCHEMA = "development_causal_observation_full_build_failure/1.1.0"
RUNTIME_PROJECTION_SCHEMA = "causal_full_build_runtime_projection/1.0.0"
EXPECTED_ENTRY_COUNT = 8_506
EXPECTED_DBN_COUNT = 4_253
EXPECTED_SIDECAR_COUNT = 4_253
EXPECTED_SOURCE_BYTES = 17_123_147_852
EXPECTED_PAYLOAD_BYTES = 17_119_024_382
EXPECTED_PRIMARY_1M_DBN_COUNT = 617
EXPECTED_WORK_UNIT_COUNT = 609
MAXIMUM_PARTITION_COUNT = 6_963
MAXIMUM_OUTPUT_BYTES = 18_000_000_000
MAXIMUM_PEAK_ADDITIONAL_BYTES = 20_000_000_000
MINIMUM_FREE_AFTER_PEAK_BYTES = 100 * 1024**3
MAXIMUM_RUNTIME_SECONDS = 216_000
MAXIMUM_PROJECTED_RUNTIME_HIGH_SECONDS = 181_000
MINIMUM_MEASURED_WORK_UNIT_COUNT = 64
WORK_UNIT_PRIORITY_MARKETS = ("ES", "GC", "6E", "CL", "NQ")
REMAINING_WORK_UNIT_ORDER = "MARKET_LEXICOGRAPHIC_THEN_YEAR_ASCENDING"
PINNED_PYTHON_EXECUTABLE = ".venv/Scripts/python.exe"
STANDARD_ROOT_COUNT = 41
DEFERRED_MICRO_COUNT = 17
DEVELOPMENT_END_EXCLUSIVE = "2025-07-13T22:00:00Z"
BOUNDARY_START_INCLUSIVE = "2025-01-01T00:00:00Z"
BOUNDARY_SOURCE_FAMILIES = frozenset(
    {
        "definition",
        "ohlcv_1d",
        "ohlcv_1h",
        "ohlcv_1m",
        "ohlcv_1s",
        "statistics",
        "status",
    }
)
WORK_UNIT_SEAL_SCHEMA = "causal_observation_v10_work_unit_seal/1.0.0"
WORK_UNIT_REUSE_SCHEMA = "causal_observation_v10_work_unit_reuse/1.0.0"
COMPLETE_MARKET_EXECUTION_ROLE = "COMPLETE_MARKET_CHECKPOINT"
ES_2025_CANARY_EXECUTION_ROLE = "V10_ES_2025_CANARY"


def _execution_outcome(plan: Mapping[str, object]) -> tuple[str, str, bool]:
    role = plan.get("execution_role")
    if role == COMPLETE_MARKET_EXECUTION_ROLE:
        return "PASS_COMPLETE_MARKET_CHECKPOINT_INACTIVE", "market_checkpoint.json", True
    if role == ES_2025_CANARY_EXECUTION_ROLE:
        return (
            "PASS_V10_ES_2025_CANARY_PRODUCER_INACTIVE",
            "canary_producer_result.json",
            False,
        )
    return (
        "PASS_AUTHORIZED_FULL_DEVELOPMENT_CANDIDATE_NOT_PUBLISHED_NOT_ACTIVE",
        "full_build_result.json",
        False,
    )


@dataclass(frozen=True, slots=True)
class FullBuildRunResult:
    result_path: Path
    payload: Mapping[str, object]


class _V10StageCreator:
    """Create one exact inactive V10 data stage with no publication capability."""

    def __init__(self, *, boundary: RepoBoundary, relative: str) -> None:
        path = Path(relative)
        if (
            path.is_absolute()
            or ".." in path.parts
            or path.as_posix() != relative
            or not relative.startswith("data/causally_gated_normalized/v10/")
        ):
            raise ContractError("V10 causal stage path is not exact and contained")
        self.boundary = boundary
        self.relative = relative
        self.created = False

    def create_stage(self, purpose: str) -> Path:
        if purpose != "causal_observation" or self.created:
            raise UnauthorizedOperation("V10 causal stage creator is exact and one-use")
        stage = self.boundary.assert_active_path(
            self.boundary.active_root / self.relative,
            purpose="inactive V10 causal-observation stage",
            subtree="data/causally_gated_normalized/v10",
        )
        io_path(stage.parent).mkdir(parents=True, exist_ok=True)
        io_path(stage).mkdir()
        self.created = True
        return stage


def _v10_file_inventory(root: Path, directory: Path) -> list[dict[str, object]]:
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "size": io_path(path).stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in iter_plain_files(directory)
    ]


def _validate_v10_reuse_binding_shape(plan: Mapping[str, object]) -> None:
    binding = plan.get("sealed_work_unit_reuse")
    if binding is None:
        return
    seals = binding.get("seals") if isinstance(binding, Mapping) else None
    if (
        not isinstance(binding, Mapping)
        or set(binding)
        != {
            "schema_version",
            "predecessor_output_staging_path",
            "predecessor_plan_id",
            "predecessor_failure_path",
            "predecessor_failure_sha256",
            "seals",
        }
        or binding.get("schema_version") != WORK_UNIT_REUSE_SCHEMA
        or type(binding.get("predecessor_plan_id")) is not str
        or type(binding.get("predecessor_failure_sha256")) is not str
        or not isinstance(seals, list)
        or not seals
        or any(
            not isinstance(item, Mapping)
            or set(item) != {"year", "path", "sha256"}
            or type(item.get("year")) is not int
            or type(item.get("path")) is not str
            or type(item.get("sha256")) is not str
            for item in seals
        )
    ):
        raise UnauthorizedOperation("V10 sealed work-unit reuse binding is invalid")


def _load_v10_reused_work_units(
    *,
    root: Path,
    plan: Mapping[str, object],
    selected: Sequence[Mapping[str, object]],
    current_output: Path,
) -> dict[int, dict[str, object]]:
    """Validate and load only a contiguous prefix of immutable sealed years."""

    _validate_v10_reuse_binding_shape(plan)
    binding = plan.get("sealed_work_unit_reuse")
    if binding is None:
        return {}
    assert isinstance(binding, Mapping)
    predecessor = _contained(root, binding["predecessor_output_staging_path"])
    expected_parent = (
        root
        / "data/causally_gated_normalized/v10"
        / str(plan["checkpoint_set_id"])
        / str(plan["target_market"])
    )
    if (
        predecessor == current_output
        or predecessor.parent != expected_parent
        or current_output.parent != expected_parent
    ):
        raise UnauthorizedOperation("V10 sealed work-unit predecessor is not exact")
    failure_path = _contained(root, binding["predecessor_failure_path"])
    if (
        failure_path != predecessor / "failure.json"
        or sha256_file(failure_path) != binding["predecessor_failure_sha256"]
    ):
        raise IntegrityError("V10 predecessor failure evidence differs")
    failure = _json(failure_path)
    if (
        failure.get("status")
        != "FAILED_MARKET_TERMINAL_OTHER_CHECKPOINTS_UNAFFECTED"
        or failure.get("target_market") != plan["target_market"]
        or failure.get("checkpoint_set_id") != plan["checkpoint_set_id"]
        or failure.get("plan_id") != binding["predecessor_plan_id"]
        or failure.get("sealed_work_units_reusable_if_all_bindings_match") is not True
    ):
        raise IntegrityError("V10 predecessor is not reusable failure evidence")

    ordered_units = list(
        _work_units(selected, expected_count=int(plan["source"]["work_unit_count"]))
    )
    seals = binding["seals"]
    assert isinstance(seals, list)
    years = [int(item["year"]) for item in seals]
    expected_years = [year for _, year, _ in ordered_units[: len(years)]]
    if years != expected_years or len(set(years)) != len(years):
        raise UnauthorizedOperation("V10 reusable work units are not an exact prefix")

    entries_by_year = {year: entries for _, year, entries in ordered_units}
    result: dict[int, dict[str, object]] = {}
    for reference in seals:
        year = int(reference["year"])
        seal_path = _contained(root, reference["path"])
        if (
            seal_path != predecessor / "work_unit_seals" / f"{year}.json"
            or sha256_file(seal_path) != reference["sha256"]
        ):
            raise IntegrityError("V10 work-unit seal binding differs")
        seal = _json(seal_path)
        seal_core = {key: value for key, value in seal.items() if key != "seal_id"}
        partitions = seal.get("partitions")
        files = seal.get("files")
        last_observation = seal.get("last_observation")
        carried_support = seal.get("carried_support")
        sealed_year = predecessor / "sealed" / str(year)
        if (
            seal.get("schema_version") != WORK_UNIT_SEAL_SCHEMA
            or seal.get("status") != "PASS_SEALED_INACTIVE_WORK_UNIT"
            or seal.get("seal_id") != sha256_json(seal_core)
            or seal.get("market") != plan["target_market"]
            or seal.get("year") != year
            or seal.get("checkpoint_set_id") != plan["checkpoint_set_id"]
            or seal.get("plan_id") != binding["predecessor_plan_id"]
            or seal.get("source_entries_sha256")
            != sha256_json(list(entries_by_year[year]))
            or type(seal.get("decoded_record_count")) is not int
            or int(seal["decoded_record_count"]) <= 0
            or not isinstance(partitions, list)
            or not partitions
            or seal.get("partition_count") != len(partitions)
            or seal.get("partitions_sha256") != sha256_json(partitions)
            or not isinstance(files, list)
            or seal.get("files_sha256") != sha256_json(files)
            or not isinstance(last_observation, Mapping)
            or seal.get("last_observation_sha256")
            != sha256_json(dict(last_observation))
            or not isinstance(carried_support, list)
            or seal.get("carried_support_sha256") != sha256_json(carried_support)
            or seal.get("publication_authorized") is not False
            or seal.get("activation_authorized") is not False
        ):
            raise IntegrityError("V10 work-unit seal identity is invalid")
        actual_files = _v10_file_inventory(root, sealed_year)
        if files != actual_files:
            raise IntegrityError("V10 sealed work-unit files differ")
        stage_prefix = sealed_year.relative_to(root).as_posix() + "/"
        if any(
            not isinstance(item, Mapping)
            or item.get("market") != plan["target_market"]
            or item.get("year") != year
            or not str(item.get("stage", "")).startswith(stage_prefix)
            for item in partitions
        ):
            raise IntegrityError("V10 sealed partition inventory is invalid")
        if seal.get("output_bytes") != sum(int(item["size"]) for item in files):
            raise IntegrityError("V10 sealed work-unit byte total differs")
        result[year] = dict(seal)
    return result


def validate_full_build_storage_floor(
    *,
    free_bytes: int,
    maximum_peak_additional_bytes: int = MAXIMUM_PEAK_ADDITIONAL_BYTES,
    minimum_free_after_peak_bytes: int = MINIMUM_FREE_AFTER_PEAK_BYTES,
) -> int:
    """Return projected free bytes or fail before any payload access."""

    if (
        type(free_bytes) is not int
        or type(maximum_peak_additional_bytes) is not int
        or type(minimum_free_after_peak_bytes) is not int
        or free_bytes < 0
        or maximum_peak_additional_bytes < 0
        or minimum_free_after_peak_bytes < 0
    ):
        raise ContractError("full development storage budget is invalid")
    projected = free_bytes - maximum_peak_additional_bytes
    if projected < minimum_free_after_peak_bytes:
        raise UnauthorizedOperation("full development storage floor would be breached")
    return projected


def _json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(io_path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"full-build JSON is invalid: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"full-build JSON is not an object: {path}")
    return value


def _contained(root: Path, relative: object) -> Path:
    if type(relative) is not str or not relative:
        raise ContractError("full-build path is absent")
    value = Path(relative)
    if value.is_absolute() or ".." in value.parts or value.as_posix() != relative:
        raise ContractError("full-build path is not canonical and relative")
    candidate = (root / value).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation("full-build path escapes the repository") from exc
    return candidate


def _write_create_only(path: Path, payload: Mapping[str, object]) -> None:
    physical = io_path(path)
    physical.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        physical,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(dict(payload)) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_runtime_projection(
    root: Path, plan: Mapping[str, object]
) -> dict[str, object]:
    binding = plan.get("runtime_projection")
    if not isinstance(binding, Mapping):
        raise UnauthorizedOperation("full development runtime projection is absent")
    projection_path = _contained(root, binding.get("path"))
    if sha256_file(projection_path) != binding.get("sha256"):
        raise IntegrityError("full development runtime projection differs")
    projection = _json(projection_path)
    projection_id = projection.get("projection_id")
    if (
        projection.get("schema_version") != RUNTIME_PROJECTION_SCHEMA
        or projection.get("status") != "PASS_RUNTIME_CEILING_SIZED_FROM_INTERRUPTED_V6"
        or projection.get("source_receipt_id")
        != "708733f6638be78f266bf6615731f250e9a410335e67a91324b9ee6f46f60689"
        or projection.get("total_work_unit_count") != EXPECTED_WORK_UNIT_COUNT
        or type(projection.get("completed_work_unit_count")) is not int
        or int(projection["completed_work_unit_count"])
        < MINIMUM_MEASURED_WORK_UNIT_COUNT
        or type(projection.get("observed_elapsed_seconds")) is not int
        or int(projection["observed_elapsed_seconds"]) <= 0
        or type(projection.get("projected_runtime_seconds")) is not int
        or type(projection.get("projected_runtime_high_seconds")) is not int
        or int(projection["projected_runtime_seconds"])
        > int(projection["projected_runtime_high_seconds"])
        or int(projection["projected_runtime_high_seconds"])
        > MAXIMUM_PROJECTED_RUNTIME_HIGH_SECONDS
        or int(projection["projected_runtime_high_seconds"])
        >= MAXIMUM_RUNTIME_SECONDS
        or projection.get("successor_runtime_ceiling_seconds")
        != MAXIMUM_RUNTIME_SECONDS
        or projection.get("partial_output_reuse_allowed") is not False
        or type(projection_id) is not str
        or len(projection_id) != 64
        or sha256_json(
            {key: value for key, value in projection.items() if key != "projection_id"}
        )
        != projection_id
        or binding.get("projection_id") != projection_id
    ):
        raise UnauthorizedOperation(
            "full development runtime projection is not exact or sufficient"
        )
    return projection


def _validate_plan(root: Path, plan: Mapping[str, object]) -> None:
    source = plan.get("source")
    limits = plan.get("limits")
    execution = plan.get("execution")
    storage = plan.get("storage")
    authority = plan.get("authority")
    economics = plan.get("economics")
    active_contract = _json(root / "configs/source_contract.json")
    active_contract_id = active_contract.get("contract_id")
    active_contract_core = {
        key: value for key, value in active_contract.items() if key != "contract_id"
    }
    active_source = active_contract.get("active_canonical_source")
    active_policy = active_contract.get("selection_policy")
    if (
        plan.get("schema_version") != PLAN_SCHEMA
        or plan.get("operation") != CAUSAL_OBSERVATION_FULL_BUILD_OPERATION
        or plan.get("causal_contract_id") != CAUSAL_OBSERVATION_CONTRACT_ID
        or economics
        != {
            "rulebook_path": ECONOMICS_RULEBOOK_PATH,
            "rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        }
        or plan.get("development_end_exclusive") != "2025-07-13T22:00:00Z"
        or plan.get("holdout_allowed") is not False
        or plan.get("forward_allowed") is not False
        or plan.get("provider_calls") != 0
        or plan.get("execution_authorized") is not False
        or not isinstance(source, Mapping)
        or not isinstance(limits, Mapping)
        or not isinstance(execution, Mapping)
        or not isinstance(storage, Mapping)
        or not isinstance(authority, Mapping)
        or any(bool(value) for value in authority.values())
        or type(active_contract_id) is not str
        or sha256_json(active_contract_core) != active_contract_id
        or not isinstance(active_source, Mapping)
        or not isinstance(active_policy, Mapping)
        or source.get("source_contract_id") != active_contract_id
        or source.get("canonical_release_id") != active_source.get("release_id")
        or source.get("exact_source_entry_count") != EXPECTED_ENTRY_COUNT
        or source.get("exact_dbn_file_count") != EXPECTED_DBN_COUNT
        or source.get("exact_sidecar_file_count") != EXPECTED_SIDECAR_COUNT
        or source.get("total_source_bytes") != EXPECTED_SOURCE_BYTES
        or source.get("maximum_payload_bytes") != EXPECTED_PAYLOAD_BYTES
        or source.get("primary_1m_dbn_count") != EXPECTED_PRIMARY_1M_DBN_COUNT
        or source.get("work_unit_count") != EXPECTED_WORK_UNIT_COUNT
        or source.get("exact_dbn_entries_sha256")
        != active_policy.get("admitted_standard_dbn_inventory_sha256")
        or source.get("exact_dbn_file_count")
        != active_policy.get("admitted_standard_dbn_file_count")
        or source.get("standard_root_count") != STANDARD_ROOT_COUNT
        or source.get("deferred_micro_count") != DEFERRED_MICRO_COUNT
        or source.get("minimum_year") != 2010
        or source.get("maximum_year") != 2025
        or limits.get("maximum_payload_bytes") != EXPECTED_PAYLOAD_BYTES
        or limits.get("maximum_output_bytes") != MAXIMUM_OUTPUT_BYTES
        or limits.get("maximum_partition_count") != MAXIMUM_PARTITION_COUNT
        or limits.get("maximum_peak_additional_bytes")
        != MAXIMUM_PEAK_ADDITIONAL_BYTES
        or execution
        != {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_workers": 1,
            "priority_markets": list(WORK_UNIT_PRIORITY_MARKETS),
            "remaining_order": REMAINING_WORK_UNIT_ORDER,
            "python_executable": PINNED_PYTHON_EXECUTABLE,
            "databento_version": SUPPORTED_DATABENTO_VERSION,
        }
        or storage.get("required_free_after_peak_bytes")
        != MINIMUM_FREE_AFTER_PEAK_BYTES
        or storage.get("publication_authorized") is not False
        or storage.get("activation_authorized") is not False
        or storage.get("partitioning") != "market/year/month"
        or storage.get("empty_partitions") is not False
        or storage.get("full_1s_duplication") is not False
        or plan.get("reuse_canary_candidates") is not False
        or plan.get("reuse_prior_partitions") is not False
    ):
        raise UnauthorizedOperation("full development build plan is not exact and nonauthorizing")
    plan_id = plan.get("plan_id")
    if type(plan_id) is not str or len(plan_id) != 64:
        raise ContractError("full development build plan ID is invalid")
    if sha256_json({key: value for key, value in plan.items() if key != "plan_id"}) != plan_id:
        raise IntegrityError("full development build plan identity differs")
    _validate_runtime_projection(root, plan)
    inventory = _contained(root, source.get("inventory_path"))
    if sha256_file(inventory) != source.get("inventory_sha256"):
        raise IntegrityError("full development source inventory differs")
    complete_inventory = _json(
        _contained(root, active_contract["complete_inventory"]["path"])
    )
    complete_entries = complete_inventory.get("entries")
    if not isinstance(complete_entries, list) or any(
        not isinstance(item, Mapping) for item in complete_entries
    ):
        raise IntegrityError("active complete source inventory entries are absent")
    validate_full_build_selection_contract(active_contract, complete_entries)
    output = _contained(root, plan.get("output_staging_path"))
    try:
        output.relative_to(root / STAGING_ROOT)
    except ValueError as exc:
        raise UnauthorizedOperation("full development output is outside staging") from exc


def _load_exact_source_entries(
    root: Path, plan: Mapping[str, object]
) -> tuple[dict[str, object], ...]:
    source = plan["source"]
    inventory = _json(_contained(root, source["inventory_path"]))
    entries = inventory.get("entries")
    if not isinstance(entries, list) or any(not isinstance(item, dict) for item in entries):
        raise IntegrityError("full development source entries are absent")
    selected = tuple(dict(item) for item in entries)
    dbn_entries = tuple(item for item in selected if item.get("kind") == "DBN")
    if (
        len(selected) != EXPECTED_ENTRY_COUNT
        or sha256_json(selected) != source.get("exact_source_entries_sha256")
        or sha256_json(dbn_entries) != source.get("exact_dbn_entries_sha256")
        or sum(item["kind"] == "DBN" for item in selected) != EXPECTED_DBN_COUNT
        or sum(item["kind"] == "SIDECAR" for item in selected)
        != EXPECTED_SIDECAR_COUNT
        or sum(int(item["size_bytes"]) for item in selected)
        != EXPECTED_SOURCE_BYTES
        or sum(
            int(item["size_bytes"])
            for item in selected
            if item["kind"] == "DBN"
        )
        != EXPECTED_PAYLOAD_BYTES
        or sum(
            item["kind"] == "DBN" and item["family"] == "ohlcv_1m"
            for item in selected
        )
        != EXPECTED_PRIMARY_1M_DBN_COUNT
    ):
        raise IntegrityError("full development source inventory counts or identity differ")
    return selected


def validate_full_build_execution_environment(
    root: Path, plan: Mapping[str, object]
) -> None:
    execution = plan.get("execution")
    if not isinstance(execution, Mapping):
        raise UnauthorizedOperation("full-build execution environment is absent")
    expected = (root / PINNED_PYTHON_EXECUTABLE).resolve(strict=True)
    observed = Path(sys.executable).resolve(strict=False)
    if (
        execution.get("python_executable") != PINNED_PYTHON_EXECUTABLE
        or execution.get("databento_version") != SUPPORTED_DATABENTO_VERSION
        or observed != expected
        or databento.__version__ != SUPPORTED_DATABENTO_VERSION
    ):
        raise UnauthorizedOperation(
            "full-build execution requires the pinned project interpreter and DBN decoder"
        )


def validate_complete_development_boundary_metadata(
    root: Path,
    entries: Sequence[Mapping[str, object]],
    *,
    standard_roots: frozenset[str],
) -> dict[str, object]:
    """Require exact boundary-safe 2025 sources before any payload operation."""

    boundary: dict[tuple[str, str, str], Mapping[str, object]] = {}
    for entry in entries:
        market = str(entry.get("market", ""))
        family = str(entry.get("family", ""))
        kind = str(entry.get("kind", ""))
        if market not in standard_roots:
            raise UnauthorizedOperation("full development source includes a nonstandard root")
        if int(entry.get("year", 0)) != 2025:
            continue
        if family not in BOUNDARY_SOURCE_FAMILIES or kind not in {"DBN", "SIDECAR"}:
            raise UnauthorizedOperation("2025 boundary source family or kind is invalid")
        if (
            entry.get("interval_start_inclusive") != BOUNDARY_START_INCLUSIVE
            or entry.get("interval_end_exclusive") != DEVELOPMENT_END_EXCLUSIVE
        ):
            raise UnauthorizedOperation(
                "2025 source crosses or does not exactly bind the development boundary"
            )
        key = (market, family, kind)
        if key in boundary:
            raise IntegrityError("2025 boundary source identity is duplicate")
        boundary[key] = entry
    expected = {
        (market, family, kind)
        for market in standard_roots
        for family in BOUNDARY_SOURCE_FAMILIES
        for kind in ("DBN", "SIDECAR")
    }
    if set(boundary) != expected:
        raise UnauthorizedOperation(
            "full development source omits exact January-July 2025 coverage"
        )
    query_contracts: list[dict[str, object]] = []
    for market in sorted(standard_roots):
        for family in sorted(BOUNDARY_SOURCE_FAMILIES):
            dbn = boundary[(market, family, "DBN")]
            sidecar = boundary[(market, family, "SIDECAR")]
            dbn_path = _contained(root, dbn.get("path"))
            sidecar_path = _contained(root, sidecar.get("path"))
            dbn_stat = io_path(dbn_path).stat()
            sidecar_stat = io_path(sidecar_path).stat()
            if (
                not dbn_path.is_file()
                or dbn_path.is_symlink()
                or dbn_stat.st_size != dbn.get("size_bytes")
                or dbn_stat.st_nlink < 2
                or sidecar_stat.st_size != sidecar.get("size_bytes")
                or sidecar_stat.st_nlink < 2
                or sha256_file(sidecar_path, reject_hardlinks=False)
                != sidecar.get("sha256")
            ):
                raise IntegrityError("2025 registered file custody identity differs")
            document = _json(sidecar_path)
            coverage = document.get("coverage_interval")
            canonical = document.get("canonical_dbn")
            if (
                sidecar.get("sidecar_schema_version")
                != "bounded_2025_canonical_registration_sidecar/1.0.0"
                or document.get("schema_version")
                != "bounded_2025_canonical_registration_sidecar/1.0.0"
                or document.get("market") != market
                or document.get("family") != family
                or not isinstance(coverage, Mapping)
                or coverage.get("start_inclusive_utc") != BOUNDARY_START_INCLUSIVE
                or coverage.get("end_exclusive_utc") != DEVELOPMENT_END_EXCLUSIVE
                or not isinstance(canonical, Mapping)
                or canonical.get("project_relative_path") != dbn.get("path")
                or canonical.get("sha256") != dbn.get("sha256")
                or canonical.get("size_bytes") != dbn.get("size_bytes")
            ):
                raise IntegrityError("2025 boundary sidecar does not bind its DBN")
            selected_sidecar = dict(sidecar)
            selected_sidecar["paired_dbn_sha256"] = dbn["sha256"]
            selected_sidecar["paired_dbn_size_bytes"] = dbn["size_bytes"]
            query_contracts.append(_query_contract(root, selected_sidecar))
    count = len(standard_roots) * len(BOUNDARY_SOURCE_FAMILIES)
    return {
        "boundary_dbn_count": count,
        "boundary_sidecar_count": count,
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "registered_hardlinked_dbn_count": count,
        "registered_hardlinked_sidecar_count": count,
        "query_contract_count": len(query_contracts),
        "query_contracts_sha256": sha256_json(query_contracts),
        "standard_root_count": len(standard_roots),
    }


def _market_windows(
    entries: Sequence[Mapping[str, object]],
) -> dict[str, dict[str, str]]:
    def rendered(value: object) -> str:
        text = str(value)
        candidate = text if "T" in text else f"{text}T00:00:00Z"
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ContractError("source interval is not an exact UTC timestamp") from exc
        if parsed.tzinfo != timezone.utc:
            raise ContractError("source interval is not an exact UTC timestamp")
        return parsed.isoformat().replace("+00:00", "Z")

    windows: dict[str, dict[str, str]] = {}
    for item in entries:
        market = str(item["market"])
        start = rendered(item["interval_start_inclusive"])
        end = rendered(item["interval_end_exclusive"])
        current = windows.setdefault(market, {"start": start, "end": end})
        current["start"] = min(current["start"], start)
        current["end"] = max(current["end"], end)
    return dict(sorted(windows.items()))


def _work_unit_sort_key(key: tuple[str, int]) -> tuple[int, str, int]:
    market, year = key
    try:
        priority = WORK_UNIT_PRIORITY_MARKETS.index(market)
    except ValueError:
        priority = len(WORK_UNIT_PRIORITY_MARKETS)
    return priority, "" if priority < len(WORK_UNIT_PRIORITY_MARKETS) else market, year


def _work_units(
    selected: Sequence[Mapping[str, object]],
    *,
    expected_count: int = EXPECTED_WORK_UNIT_COUNT,
) -> tuple[tuple[str, int, tuple[dict[str, object], ...]], ...]:
    grouped: dict[tuple[str, int], list[dict[str, object]]] = defaultdict(list)
    families: dict[tuple[str, int], set[str]] = defaultdict(set)
    for item in selected:
        key = (str(item["market"]), int(item["year"]))
        grouped[key].append(dict(item))
        if item["kind"] == "DBN":
            families[key].add(str(item["family"]))
    units: list[tuple[str, int, tuple[dict[str, object], ...]]] = []
    for key in sorted(grouped, key=_work_unit_sort_key):
        if "ohlcv_1m" not in families[key]:
            continue
        if "definition" not in families[key]:
            raise IntegrityError("primary market-year lacks point-in-time definitions")
        rows = tuple(sorted(grouped[key], key=lambda item: str(item["path"])))
        units.append((key[0], key[1], rows))
    if len(units) != expected_count:
        raise IntegrityError("full development market-year work-unit count differs")
    return tuple(units)


def _work_unit_window(
    entries: Sequence[Mapping[str, object]],
) -> dict[str, str]:
    market_windows = _market_windows(entries)
    if len(market_windows) != 1:
        raise IntegrityError("market-year work unit spans multiple markets")
    return next(iter(market_windows.values()))


def _month_windows(
    start_inclusive: str,
    end_exclusive: str,
) -> tuple[tuple[int, int, str, dict[str, str]], ...]:
    start_bound = datetime.fromisoformat(start_inclusive.replace("Z", "+00:00"))
    end_bound = datetime.fromisoformat(end_exclusive.replace("Z", "+00:00"))
    if (
        start_bound.tzinfo != timezone.utc
        or end_bound.tzinfo != timezone.utc
        or not start_bound < end_bound
        or end_bound
        > datetime.fromisoformat(DEVELOPMENT_END_EXCLUSIVE.replace("Z", "+00:00"))
    ):
        raise UnauthorizedOperation("work-unit window crosses the development boundary")
    result: list[tuple[int, int, str, dict[str, str]]] = []
    start = start_bound
    while start < end_bound:
        next_month = (
            datetime(start.year + 1, 1, 1, tzinfo=timezone.utc)
            if start.month == 12
            else datetime(start.year, start.month + 1, 1, tzinfo=timezone.utc)
        )
        end = min(next_month, end_bound)
        start_ns = int(start.timestamp() * 1_000_000_000)
        end_ns = int(end.timestamp() * 1_000_000_000)
        rendered_end = (
            end.date().isoformat()
            if end.time() == datetime.min.time()
            else end.strftime("%Y-%m-%dT%H%M%SZ")
        )
        interval = f"{start.date().isoformat()}_{rendered_end}"
        result.append(
            (
                start_ns,
                end_ns,
                interval,
                {
                    "start": start.isoformat().replace("+00:00", "Z"),
                    "end": end.isoformat().replace("+00:00", "Z"),
                },
            )
        )
        start = end
    return tuple(result)


def _slice_decoded(
    decoded: DecodedMarket,
    *,
    start_ns: int,
    end_ns: int,
    definitions: Sequence[object],
    carried_support: Sequence[tuple[int, str, str]],
) -> DecodedMarket:
    return DecodedMarket(
        definitions=tuple(definitions),  # type: ignore[arg-type]
        primary_1m=tuple(
            row for row in decoded.primary_1m if start_ns <= row.event_at_ns < end_ns
        ),
        reference_1s={
            key: value
            for key, value in decoded.reference_1s.items()
            if start_ns <= key < end_ns
        },
        reference_1h={
            key: value
            for key, value in decoded.reference_1h.items()
            if start_ns <= key < end_ns
        },
        reference_1d={
            key: value
            for key, value in decoded.reference_1d.items()
            if start_ns <= key < end_ns
        },
        support_rows=tuple(
            sorted(
                tuple(carried_support)
                + tuple(
                    row
                    for row in decoded.support_rows
                    if start_ns <= row[0] < end_ns
                )
            )
        ),
        decoded_record_count=decoded.decoded_record_count,
    )


def _execute(
    *,
    root: Path,
    boundary: RepoBoundary,
    plan: Mapping[str, object],
    plan_sha256: str,
    context: CausalObservationOperationContext,
    selected: Sequence[Mapping[str, object]],
    economics_rulebook: EconomicsRuleBook,
    progress: dict[str, object],
    started: float,
) -> FullBuildRunResult:
    output = _contained(root, plan["output_staging_path"])
    execution_role = plan.get("execution_role")
    v10_market = execution_role in {
        COMPLETE_MARKET_EXECUTION_ROLE,
        ES_2025_CANARY_EXECUTION_ROLE,
    } and output.is_relative_to(
        root / "data/causally_gated_normalized/v10"
    )
    relative_output = (
        output.relative_to(root).as_posix()
        if v10_market
        else output.relative_to(root / STAGING_ROOT).as_posix()
    )
    source_contract = _json(root / "configs/source_contract.json")
    standard_roots = frozenset(source_contract["universe"]["standard_roots"])
    prior_observation: dict[str, Mapping[str, object]] = {}
    carried_support: dict[str, tuple[tuple[int, str, str], ...]] = {}
    partitions: list[dict[str, object]] = []
    decoded_records = 0
    output_bytes = 0
    complete_work_units = 0
    reused = (
        _load_v10_reused_work_units(
            root=root,
            plan=plan,
            selected=selected,
            current_output=output,
        )
        if v10_market
        else {}
    )

    for market, year, unit_entries in _work_units(
        selected, expected_count=int(plan["source"]["work_unit_count"])
    ):
        runtime_ceiling = int(
            plan.get("execution", {}).get(
                "maximum_runtime_seconds", MAXIMUM_RUNTIME_SECONDS
            )
        )
        if time.monotonic() - started > runtime_ceiling:
            raise UnauthorizedOperation("full development runtime ceiling exceeded")
        window = _work_unit_window(unit_entries)
        unit_partition_start = len(partitions)
        unit_dbns = tuple(item for item in unit_entries if item["kind"] == "DBN")
        progress.update(
            {
                "current_market": market,
                "current_year": year,
                "current_work_unit_dbn_count": len(unit_dbns),
                "current_work_unit_dbn_bytes": sum(int(item["size_bytes"]) for item in unit_dbns),
                "current_work_unit_decode_state": "STARTED",
                "current_work_unit_state": "STARTED",
            }
        )
        if year in reused:
            seal = reused[year]
            reused_partitions = seal["partitions"]
            assert isinstance(reused_partitions, list)
            partitions.extend(dict(item) for item in reused_partitions)
            output_bytes += int(seal["output_bytes"])
            decoded_records += int(seal["decoded_record_count"])
            prior_observation[market] = dict(seal["last_observation"])
            carried_support[market] = tuple(
                (int(item[0]), str(item[1]), str(item[2]))
                for item in seal["carried_support"]
            )
            complete_work_units += 1
            progress.update(
                {
                    "current_work_unit_decode_state": "REUSED_SEALED",
                    "current_work_unit_state": "COMPLETE_REUSED_SEALED",
                    "decoded_record_count": decoded_records,
                    "complete_work_unit_count": complete_work_units,
                    "complete_partition_count": len(partitions),
                    "output_bytes": output_bytes,
                }
            )
            if (
                decoded_records > int(plan["limits"]["maximum_decoded_records"])
                or len(partitions) > int(plan["limits"]["maximum_partition_count"])
                or output_bytes > int(plan["limits"]["maximum_output_bytes"])
            ):
                raise UnauthorizedOperation("V10 reused work-unit limits are exceeded")
            continue

        def record_dbn_open(path: Path) -> None:
            relative = path.resolve(strict=True).relative_to(root).as_posix()
            opened = progress["dbn_paths_opened"]
            if not isinstance(opened, list) or relative in opened:
                raise IntegrityError("full development DBN open ledger is invalid")
            opened.append(relative)
            progress["dbn_files_opened"] = int(progress["dbn_files_opened"]) + 1
            progress["dbn_payload_bytes_opened"] = int(
                progress["dbn_payload_bytes_opened"]
            ) + io_path(path).stat().st_size

        decoded = _decode_selected_sources(
            root=root,
            selected=unit_entries,
            windows={market: window},
            source_contract=source_contract,
            maximum_decoded_records=int(plan["limits"]["maximum_decoded_records"]),
            on_dbn_open=record_dbn_open,
        )[market]
        progress["current_work_unit_decode_state"] = "COMPLETE"
        decoded_records += decoded.decoded_record_count
        progress["decoded_record_count"] = decoded_records
        if decoded_records > int(plan["limits"]["maximum_decoded_records"]):
            raise UnauthorizedOperation("full development decoded-record ceiling exceeded")
        for start_ns, end_ns, interval, month_window in _month_windows(
            window["start"], window["end"]
        ):
            month = _slice_decoded(
                decoded,
                start_ns=start_ns,
                end_ns=end_ns,
                definitions=decoded.definitions,
                carried_support=carried_support.get(market, ()),
            )
            if not month.primary_1m:
                continue
            partition_relative = (
                f"{relative_output}/work/{year}/{interval}"
                if v10_market
                else f"{relative_output}/{year}/{interval}"
                if type(plan.get("target_market")) is str
                else f"{relative_output}/{market}/{year}/{interval}"
            )
            creator = (
                _V10StageCreator(boundary=boundary, relative=partition_relative)
                if v10_market
                else _CanaryStageCreator(
                    boundary=boundary,
                    relative=partition_relative,
                )
            )
            built = _build_market_candidate_with_state(
                publisher=creator,
                context=context,
                market=market,
                window=month_window,
                decoded=month,
                allowed_roots=standard_roots,
                economics_rulebook=economics_rulebook,
                prior_observation=prior_observation.get(market),
            )
            certificate = verify_observation_candidate(
                stage=built.prepared.stage,
                manifest=built.prepared.manifest,
                economics_rulebook=economics_rulebook,
            )
            inventory = prepared_inventory(built.prepared)
            partition_bytes = sum(int(item["size"]) for item in inventory["files"])
            output_bytes += partition_bytes
            partitions.append(
                {
                    "market": market,
                    "year": year,
                    "interval": interval,
                    "release_id": built.prepared.manifest.release_id,
                    "certificate_id": certificate["certificate_id"],
                    "inventory_sha256": inventory["files_sha256"],
                    "output_bytes": partition_bytes,
                    "stage": built.prepared.stage.relative_to(root).as_posix(),
                }
            )
            progress["complete_partition_count"] = len(partitions)
            progress["output_bytes"] = output_bytes
            prior_observation[market] = built.last_observation
            last_bar_end = int(built.last_observation["bar_end_ns"])
            carried_support[market] = tuple(
                row
                for row in decoded.support_rows
                if last_bar_end <= row[0] < end_ns
            )
            if len(partitions) > int(plan["limits"]["maximum_partition_count"]):
                raise UnauthorizedOperation("full development partition ceiling exceeded")
            if output_bytes > int(plan["limits"]["maximum_output_bytes"]):
                raise UnauthorizedOperation("full development output byte ceiling exceeded")
        if v10_market:
            if len(partitions) == unit_partition_start:
                raise IntegrityError("V10 work unit produced no certifiable partitions")
            work_year = output / "work" / str(year)
            sealed_year = output / "sealed" / str(year)
            if io_path(work_year).exists():
                if io_path(sealed_year).exists():
                    raise IntegrityError("V10 sealed work-unit destination already exists")
                io_path(sealed_year.parent).mkdir(parents=True, exist_ok=True)
                os.replace(io_path(work_year), io_path(sealed_year))
                for item in partitions[unit_partition_start:]:
                    item["stage"] = str(item["stage"]).replace(
                        f"/{relative_output}/work/{year}/".removeprefix("/"),
                        f"{relative_output}/sealed/{year}/",
                        1,
                    )
            unit_partitions = partitions[unit_partition_start:]
            last_observation = dict(prior_observation[market])
            support = [list(item) for item in carried_support.get(market, ())]
            files = _v10_file_inventory(root, sealed_year)
            seal_core = {
                "schema_version": WORK_UNIT_SEAL_SCHEMA,
                "status": "PASS_SEALED_INACTIVE_WORK_UNIT",
                "market": market,
                "year": year,
                "checkpoint_set_id": plan.get("checkpoint_set_id"),
                "plan_id": plan["plan_id"],
                "source_entries_sha256": sha256_json(list(unit_entries)),
                "decoded_record_count": decoded.decoded_record_count,
                "partition_count": len(unit_partitions),
                "partitions": unit_partitions,
                "partitions_sha256": sha256_json(unit_partitions),
                "files": files,
                "files_sha256": sha256_json(files),
                "output_bytes": sum(int(item["size"]) for item in files),
                "last_observation": last_observation,
                "last_observation_sha256": sha256_json(last_observation),
                "carried_support": support,
                "carried_support_sha256": sha256_json(support),
                "publication_authorized": False,
                "activation_authorized": False,
            }
            _write_create_only(
                output / "work_unit_seals" / f"{year}.json",
                {**seal_core, "seal_id": sha256_json(seal_core)},
            )
        complete_work_units += 1
        progress["complete_work_unit_count"] = complete_work_units
        progress["current_work_unit_state"] = "COMPLETE"
    checkpoint_market = plan.get("target_market")
    status, result_filename, is_market_checkpoint = _execution_outcome(plan)
    core: dict[str, object] = {
        "schema_version": RESULT_SCHEMA,
        "status": status,
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "receipt_id": context.receipt_id,
        "source_contract_id": context.source_contract_id,
        "source_release_id": context.source_release_id,
        "causal_contract_id": context.causal_contract_id,
        "source_entry_count": len(selected),
        "dbn_file_count": sum(item["kind"] == "DBN" for item in selected),
        "payload_bytes_opened_maximum": sum(
            int(item["size_bytes"]) for item in selected if item["kind"] == "DBN"
        ),
        "decoded_record_count": decoded_records,
        "complete_work_unit_count": complete_work_units,
        "partition_count": len(partitions),
        "output_bytes": output_bytes,
        "partition_inventory_sha256": sha256_json(partitions),
        "partitions": partitions,
        "provider_calls": 0,
        "holdout_rows": 0,
        "forward_rows": 0,
        "outcomes": 0,
        "features": 0,
        "wfa": 0,
        "fitting": 0,
        "predictions": 0,
        "evaluations": 0,
        "mechanism_executions": 0,
        "publication_authorized": False,
        "activation_authorized": False,
        "target_market": checkpoint_market,
        "attempt_id": plan.get("attempt_id"),
        "checkpoint_set_id": plan.get("checkpoint_set_id"),
        "complete_market_checkpoint": is_market_checkpoint,
        "reusable_in_same_checkpoint_set": is_market_checkpoint,
        "execution_role": execution_role,
        "can_seed_complete_market_checkpoint": False,
    }
    payload = {**core, "result_id": sha256_json(core)}
    result_path = output / result_filename
    _write_create_only(result_path, payload)
    return FullBuildRunResult(result_path=result_path, payload=payload)


def run_authorized_full_build(
    *,
    repository_root: Path,
    receipt: OperationReceipt,
    plan_path: Path,
) -> FullBuildRunResult:
    """Consume one approval and build verified inactive monthly candidates."""

    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(root)
    path = plan_path.resolve(strict=True)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation("full development plan is outside the repository") from exc
    plan = _json(path)
    plan_sha = sha256_file(path)
    _validate_plan(root, plan)
    economics_rulebook = _load_economics_rulebook(root)
    selected = _load_exact_source_entries(root, plan)
    source_contract = _json(root / "configs/source_contract.json")
    validate_complete_development_boundary_metadata(
        root,
        selected,
        standard_roots=frozenset(source_contract["universe"]["standard_roots"]),
    )
    closure_context = issue_current_source_closure_context(root)
    selected = select_exact_standard_source_entries(
        root,
        operation_context=closure_context,
        source_entries=selected,
        windows=_market_windows(selected),
    )
    output = _contained(root, plan["output_staging_path"])
    if output.exists():
        raise IntegrityError("full development output staging path already exists")
    validate_full_build_storage_floor(free_bytes=shutil.disk_usage(root).free)
    global_lock = root / "state/locks/foundation-build.lock"
    run_lock = root / f"state/locks/causal-observation-{plan['plan_id']}.lock"
    if global_lock.exists() or run_lock.exists():
        raise UnauthorizedOperation("full development build lock is already active")
    validate_full_build_execution_environment(root, plan)
    context = authorize_full_build_row_read(
        boundary=boundary,
        receipt=receipt,
        plan=plan,
        plan_sha256=plan_sha,
    )
    started = time.monotonic()
    progress: dict[str, object] = {
        "current_market": None,
        "current_year": None,
        "current_work_unit_dbn_count": 0,
        "current_work_unit_dbn_bytes": 0,
        "current_work_unit_decode_state": "NOT_STARTED",
        "current_work_unit_state": "NOT_STARTED",
        "dbn_files_opened": 0,
        "dbn_payload_bytes_opened": 0,
        "dbn_paths_opened": [],
        "decoded_record_count": 0,
        "complete_work_unit_count": 0,
        "complete_partition_count": 0,
        "output_bytes": 0,
    }
    try:
        with FileLease(global_lock), FileLease(run_lock):
            return _execute(
                root=root,
                boundary=boundary,
                plan=plan,
                plan_sha256=plan_sha,
                context=context,
                selected=selected,
                economics_rulebook=economics_rulebook,
                progress=progress,
                started=started,
            )
    except (Exception, KeyboardInterrupt) as exc:
        failure = {
            "schema_version": FAILURE_SCHEMA,
            "status": "FAILED_AUTHORIZATION_CONSUMED_NO_AUTOMATIC_RETRY",
            "plan_id": plan["plan_id"],
            "plan_sha256": plan_sha,
            "receipt_id": context.receipt_id,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "error_details": getattr(exc, "details", {}),
            "progress": progress,
            "terminal": True,
            "receipt_reuse_authorized": False,
            "partial_partition_reuse_authorized": False,
            "automatic_retry_authorized": False,
            "required_successor": "NEW_PLAN_NEW_RECEIPT_NEW_OUTPUT_ROOT",
            "publication_authorized": False,
            "activation_authorized": False,
        }
        io_path(output).mkdir(parents=True, exist_ok=True)
        failure_path = output / "failure.json"
        if not failure_path.exists():
            _write_create_only(failure_path, failure)
        raise
