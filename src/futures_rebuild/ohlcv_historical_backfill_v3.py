"""Prepare the additive 58-root OHLCV-1D/1H completion successor.

This module is deliberately provider-free.  It freezes the live delta from the
registered full-size and micro universe contracts, the active canonical DBN
release, and retained OHLCV-1M sidecars.  Provider quotation, acquisition, and
canonical publication remain separate approved operations.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import ContractError, IntegrityError, UnauthorizedOperation


DATASET = "GLBX.MDP3"
END_EXCLUSIVE = "2026-07-14T00:00:00Z"
SCHEMAS = ("ohlcv-1d", "ohlcv-1h")
SCHEMA_DIRECTORIES = {"ohlcv-1d": "ohlcv_1d", "ohlcv-1h": "ohlcv_1h"}
REFERENCE_SCHEMAS = ("ohlcv-1m", "ohlcv-1s")
SAFETY_MARGIN_BYTES = 1_073_741_824
PUBLICATION_METADATA_ALLOWANCE_BYTES = 67_108_864
ACTIVE_POINTER = Path("configs/active_dbn_congruence_release_v1.json")
FULL_SIZE_UNIVERSE = Path("configs/research_universe_contract.json")
MICRO_UNIVERSE = Path("configs/micro_contract_universe_v1.json")
HISTORICAL_QUOTE = Path(
    "reports/ohlcv_1d_1h_historical_backfill/"
    "ohlcv1d1h_20260816T2222113045364Z_e4f0afd/07_QUOTE_DETAIL.json"
)
PLAN_ROOT = Path("reports/ohlcv_58_completion")
PLAN_SCHEMA = "ohlcv_58_completion_plan/2.0.0"
BATCH_SCHEMA = "ohlcv_58_completion_batch/1.0.0"
PLAN_STATUS = "PREPARED_REQUIRES_SEPARATE_QUOTE_ACQUISITION_AND_PUBLICATION_APPROVALS"
_SHA256 = re.compile(r"[0-9a-f]{64}")


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"unable to load {label}: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{label} is not a JSON object: {path}")
    return value


def _parse_utc(value: object, label: str) -> datetime:
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ContractError(f"{label} is not an ISO timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _year_partition_count(start: str, end: str) -> int:
    first = _parse_utc(start, "start")
    terminal = _parse_utc(end, "end")
    if first >= terminal:
        raise ContractError("coverage start must precede the exclusive end")
    return terminal.year - first.year + 1


def _tier_roots(document: Mapping[str, Any], tier_ids: Sequence[int]) -> list[str]:
    tiers = document.get("tiers")
    if not isinstance(tiers, list):
        raise IntegrityError("full-size universe lacks tier records")
    roots: set[str] = set()
    for tier in tiers:
        if isinstance(tier, dict) and tier.get("tier_id") in tier_ids:
            symbols = tier.get("symbols")
            if not isinstance(symbols, list) or not all(isinstance(item, str) for item in symbols):
                raise IntegrityError("full-size universe tier symbols are invalid")
            roots.update(symbols)
    return sorted(roots)


def authoritative_universe(root: Path) -> dict[str, Any]:
    full_path = root / FULL_SIZE_UNIVERSE
    micro_path = root / MICRO_UNIVERSE
    full_document = _load_json(full_path, "full-size universe")
    micro_document = _load_json(micro_path, "micro universe")
    full = _tier_roots(full_document, (3, 4))
    tiers = micro_document.get("tiers")
    micro = tiers.get("tier_3") if isinstance(tiers, dict) else None
    if not isinstance(micro, list) or not all(isinstance(item, str) for item in micro):
        raise IntegrityError("micro universe lacks a valid tier_3 list")
    micro = sorted(set(micro))
    overlap = sorted(set(full) & set(micro))
    universe = sorted(set(full) | set(micro))
    if len(full) != 41 or len(micro) != 17 or overlap or len(universe) != 58:
        raise IntegrityError(
            "authoritative universe is not the required disjoint 41 full-size plus 17 micro roots"
        )
    return {
        "full_size_roots": full,
        "micro_roots": micro,
        "roots": universe,
        "bindings": {
            FULL_SIZE_UNIVERSE.as_posix(): sha256_file(full_path),
            MICRO_UNIVERSE.as_posix(): sha256_file(micro_path),
        },
    }


def _active_release(root: Path) -> tuple[dict[str, Any], dict[str, Any], Path]:
    pointer_path = root / ACTIVE_POINTER
    pointer = _load_json(pointer_path, "active DBN pointer")
    if pointer.get("status") != "ACTIVE":
        raise IntegrityError("active DBN pointer is not ACTIVE")
    relative = pointer.get("release_manifest_path")
    expected = pointer.get("release_manifest_sha256")
    if not isinstance(relative, str) or not isinstance(expected, str):
        raise IntegrityError("active DBN pointer lacks a manifest binding")
    manifest_path = root / relative
    observed = sha256_file(manifest_path)
    if observed != expected:
        raise IntegrityError("active DBN release manifest differs from its pointer binding")
    manifest = _load_json(manifest_path, "active DBN release manifest")
    if manifest.get("release_id") != pointer.get("release_id"):
        raise IntegrityError("active pointer and release manifest IDs differ")
    return pointer, manifest, manifest_path


def _release_root_sets(manifest: Mapping[str, Any]) -> dict[str, list[str]]:
    raw = manifest.get("canonical_artifact_index")
    if not isinstance(raw, list):
        raise IntegrityError("active DBN release lacks a canonical artifact index")
    result: dict[str, set[str]] = {schema: set() for schema in (*REFERENCE_SCHEMAS, *SCHEMAS)}
    for item in raw:
        if not isinstance(item, dict) or item.get("kind") != "DBN":
            continue
        unit_id = item.get("unit_id")
        if not isinstance(unit_id, str):
            continue
        parts = unit_id.split("|")
        if len(parts) == 4 and parts[0] == DATASET and parts[1] in result:
            result[parts[1]].add(parts[2])
    return {schema: sorted(values) for schema, values in result.items()}


def _sidecar_start(sidecar: Mapping[str, Any], market: str) -> str:
    candidates: list[object] = [sidecar.get("start")]
    for key in ("exact_authorized_query", "request"):
        nested = sidecar.get(key)
        if isinstance(nested, dict):
            candidates.append(nested.get("start"))
    value = next((item for item in candidates if isinstance(item, str) and item), None)
    if value is None:
        raise IntegrityError(f"earliest OHLCV-1M sidecar lacks a start boundary: {market}")
    parsed = _parse_utc(value, f"{market} start")
    if parsed.time() != datetime.min.time():
        raise IntegrityError(f"OHLCV-1M start is not a UTC-midnight boundary: {market}")
    return parsed.strftime("%Y-%m-%dT00:00:00Z")


def _earliest_1m_start(root: Path, market: str) -> tuple[str, str, str]:
    market_root = root / "data/dbn/ohlcv_1m" / market
    sidecars = sorted(market_root.glob("*/*.dbn.zst.manifest.json"))
    if not sidecars:
        raise IntegrityError(f"OHLCV-1M evidence is absent: {market}")
    sidecar_path = sidecars[0]
    sidecar = _load_json(sidecar_path, f"{market} earliest OHLCV-1M sidecar")
    return (
        _sidecar_start(sidecar, market),
        sidecar_path.relative_to(root).as_posix(),
        sha256_file(sidecar_path),
    )


def _dbn_bytes(root: Path, schema_directory: str, market: str) -> int:
    market_root = root / "data/dbn" / schema_directory / market
    return sum(path.stat().st_size for path in market_root.glob("*/*.dbn.zst"))


def _ratio(values: Sequence[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise IntegrityError("cannot estimate storage without retained comparison roots")
    index = int((len(ordered) - 1) * percentile)
    return ordered[index]


def _historical_quote_evidence(root: Path) -> dict[str, Any]:
    path = root / HISTORICAL_QUOTE
    if not path.is_file():
        return {
            "available": False,
            "expected_cost_usd": None,
            "proportional_billable_bytes": None,
        }
    document = _load_json(path, "historical provider quote")
    units = document.get("quote_units")
    if not isinstance(units, list):
        raise IntegrityError("historical provider quote lacks quote units")
    billable = sum(int(item["api_billable_uncompressed_bytes"]) for item in units if isinstance(item, dict))
    costs = [Decimal(str(item["estimated_cost_usd"])) for item in units if isinstance(item, dict)]
    return {
        "available": True,
        "billable_bytes": billable,
        "cost_usd": format(sum(costs, Decimal("0")), "f"),
        "path": HISTORICAL_QUOTE.as_posix(),
        "sha256": sha256_file(path),
    }


def build_completion_plan(root: Path, *, end_exclusive: str = END_EXCLUSIVE) -> dict[str, Any]:
    root = root.resolve(strict=True)
    if end_exclusive != END_EXCLUSIVE:
        raise ContractError(f"completion endpoint must be exactly {END_EXCLUSIVE}")
    universe = authoritative_universe(root)
    pointer, release, release_path = _active_release(root)
    sets = _release_root_sets(release)
    universe_roots = universe["roots"]
    for schema in REFERENCE_SCHEMAS:
        if sets[schema] != universe_roots:
            raise IntegrityError(f"active {schema} root set differs from the authoritative universe")
    missing_by_schema = {
        schema: sorted(set(universe_roots) - set(sets[schema]))
        for schema in SCHEMAS
    }
    for schema in SCHEMAS:
        extra = sorted(set(sets[schema]) - set(universe_roots))
        if extra:
            raise IntegrityError(f"active {schema} roots fall outside the universe: {extra}")
    missing = sorted(set().union(*(set(values) for values in missing_by_schema.values())))

    intervals: list[dict[str, Any]] = []
    missing_reference_bytes = 0
    for market in missing:
        missing_schemas = [schema for schema in SCHEMAS if market in missing_by_schema[schema]]
        start, evidence_path, evidence_sha = _earliest_1m_start(root, market)
        one_minute_bytes = _dbn_bytes(root, "ohlcv_1m", market)
        if one_minute_bytes <= 0:
            raise IntegrityError(f"OHLCV-1M byte evidence is empty: {market}")
        missing_reference_bytes += one_minute_bytes * len(missing_schemas)
        cohort = "MICRO" if market in universe["micro_roots"] else "FULL_SIZE"
        intervals.append(
            {
                "classification": "ACQUIRE_MISSING_DELTA",
                "cohort": cohort,
                "end_exclusive": end_exclusive,
                "market": market,
                "ohlcv_1m_evidence": {"path": evidence_path, "sha256": evidence_sha},
                "ohlcv_1m_retained_bytes": one_minute_bytes,
                "schemas": missing_schemas,
                "start_inclusive": start,
                "year_partition_count_per_schema": _year_partition_count(start, end_exclusive),
            }
        )

    ratio_samples: dict[str, list[float]] = {schema: [] for schema in SCHEMAS}
    present_reference_bytes = 0
    for schema in SCHEMAS:
        local_schema = SCHEMA_DIRECTORIES[schema]
        for market in sets[schema]:
            minute = _dbn_bytes(root, "ohlcv_1m", market)
            observed = _dbn_bytes(root, local_schema, market)
            if min(minute, observed) <= 0:
                raise IntegrityError(f"retained ratio evidence is empty: {schema} {market}")
            present_reference_bytes += minute
            ratio_samples[schema].append(observed / minute)
    ratios = {
        schema: {
            "comparison_root_count": len(ratio_samples[schema]),
            "median": _ratio(ratio_samples[schema], 0.50),
            "p95": _ratio(ratio_samples[schema], 0.95),
        }
        for schema in SCHEMAS
    }
    per_root_expected: dict[str, int] = {}
    per_root_high: dict[str, int] = {}
    for item in intervals:
        minute_bytes = int(item["ohlcv_1m_retained_bytes"])
        item_schemas = [str(value) for value in item["schemas"]]
        by_schema = {
            schema: {
                "expected": int(minute_bytes * ratios[schema]["median"]),
                "high": int(minute_bytes * ratios[schema]["p95"]),
            }
            for schema in item_schemas
        }
        item["estimated_final_bytes_by_schema"] = by_schema
        per_root_expected[str(item["market"])] = sum(value["expected"] for value in by_schema.values())
        per_root_high[str(item["market"])] = sum(value["high"] for value in by_schema.values())
    expected_final = sum(per_root_expected.values())
    high_final = sum(per_root_high.values())
    largest_batch = max(per_root_high.values(), default=0)
    historical = _historical_quote_evidence(root)
    billable = None
    if historical["available"] and present_reference_bytes:
        billable = int(
            historical["billable_bytes"] * missing_reference_bytes / present_reference_bytes
        )
    for item in intervals:
        item["estimated_final_bytes_high"] = per_root_high[item["market"]]
    full_canary = min(
        (item for item in intervals if item["cohort"] == "FULL_SIZE"),
        key=lambda item: (item["estimated_final_bytes_high"], item["market"]),
        default=None,
    )
    micro_canary = min(
        (item for item in intervals if item["cohort"] == "MICRO"),
        key=lambda item: (item["estimated_final_bytes_high"], item["market"]),
        default=None,
    )
    target_partitions = sum(
        item["year_partition_count_per_schema"] * len(item["schemas"])
        for item in intervals
    )
    free_bytes = shutil.disk_usage(root).free
    peak_temporary = SAFETY_MARGIN_BYTES + PUBLICATION_METADATA_ALLOWANCE_BYTES + 3 * largest_batch
    required_free = high_final + peak_temporary
    requests = [
        {
            "compression": "zstd",
            "dataset": DATASET,
            "encoding": "dbn",
            "end": item["end_exclusive"],
            "map_symbols": False,
            "market": item["market"],
            "schema": schema,
            "split_duration": "year",
            "split_symbols": False,
            "start": item["start_inclusive"],
            "stype_in": "continuous",
            "stype_out": "instrument_id",
            "symbols": [f"{item['market']}.v.0"],
        }
        for item in intervals
        for schema in item["schemas"]
    ]
    coverage = {
        schema: {
            "classification": "REUSE_COMPLETE" if schema in REFERENCE_SCHEMAS else "MIXED_REUSE_AND_ACQUIRE",
            "missing_roots": [] if schema in REFERENCE_SCHEMAS else missing_by_schema[schema],
            "present_root_count": len(sets[schema]),
            "present_roots": sets[schema],
        }
        for schema in (*REFERENCE_SCHEMAS, *SCHEMAS)
    }
    plan: dict[str, Any] = {
        "authority": {
            "active_data_mutation": False,
            "credential_access": False,
            "provider_network_access": False,
            "publication": False,
            "status": PLAN_STATUS,
        },
        "bindings": {
            **universe["bindings"],
            ACTIVE_POINTER.as_posix(): sha256_file(root / ACTIVE_POINTER),
            release_path.relative_to(root).as_posix(): sha256_file(release_path),
        },
        "canaries": [
            {"market": item["market"], "schemas": list(item["schemas"])}
            for item in (full_canary, micro_canary)
            if item is not None
        ],
        "coverage": coverage,
        "dataset": DATASET,
        "end_exclusive": end_exclusive,
        "estimates": {
            "current_free_bytes": free_bytes,
            "decoded_or_extracted_retained_bytes": 0,
            "download_bytes_expected": expected_final,
            "download_bytes_high": high_final,
            "final_retained_bytes_expected": expected_final,
            "final_retained_bytes_high": high_final,
            "largest_root_batch_bytes_high": largest_batch,
            "network_billable_uncompressed_bytes_expected": billable,
            "peak_temporary_bytes_high": peak_temporary,
            "publication_existing_payload_copy_bytes": 0,
            "required_free_bytes": required_free,
            "safety_margin_bytes": SAFETY_MARGIN_BYTES,
            "temporary_reclamation_expected_bytes": high_final,
        },
        "execution_limits": {
            "maximum_concurrent_provider_jobs": 1,
            "maximum_roots_per_batch": 1,
            "provider_cost_cap_usd": "0.0",
            "provider_request_count": len(requests),
            "target_dbn_file_count_maximum": target_partitions,
        },
        "historical_quote_evidence": historical,
        "intervals": intervals,
        "provider": "Databento",
        "publication": {
            "active_pointer": ACTIVE_POINTER.as_posix(),
            "canonical_root": "data/dbn",
            "mode": "PLAIN_FILE_ADDITIVE_ABSENT_MARKET_DIRECTORIES_THEN_POINTER_SUCCESSOR",
            "overwrite_existing": False,
            "prior_release_id": pointer["release_id"],
            "rollback": "RESTORE_PRIOR_ROOT_AND_POINTER_ON_ANY_TRANSITION_FAILURE",
            "staging_root": "state/provider_acquisition_staging/ohlcv_58_completion_v3/<plan_id>",
        },
        "requests": requests,
        "schema_version": PLAN_SCHEMA,
        "universe": {
            "full_size_count": len(universe["full_size_roots"]),
            "full_size_roots": universe["full_size_roots"],
            "micro_count": len(universe["micro_roots"]),
            "micro_roots": universe["micro_roots"],
            "root_count": len(universe_roots),
            "roots": universe_roots,
        },
    }
    plan["plan_id"] = sha256_json(plan)
    if free_bytes < required_free:
        raise IntegrityError("free space is below the locally estimated completion requirement")
    return plan


def load_bound_completion_plan(
    root: Path,
    plan_path: Path,
    *,
    expected_plan_id: str,
    expected_sha256: str,
) -> dict[str, Any]:
    """Revalidate one frozen plan without granting provider or publication authority.

    The frozen plan records free space at preparation time.  That observation is
    allowed to drift, but every target, request, universe/release binding, size
    estimate, and publication constraint must still equal a freshly derived
    provider-free plan.  Current free space must independently remain above the
    frozen requirement.
    """

    root = root.resolve(strict=True)
    if _SHA256.fullmatch(expected_plan_id) is None or _SHA256.fullmatch(expected_sha256) is None:
        raise ContractError("completion plan identifiers must be lowercase SHA-256 values")
    candidate = plan_path.resolve(strict=True)
    permitted = (root / PLAN_ROOT).resolve(strict=True)
    try:
        candidate.relative_to(permitted)
    except ValueError as exc:
        raise ContractError("completion plan must be inside the designated report root") from exc
    raw = candidate.read_bytes()
    if sha256_file(candidate) != expected_sha256:
        raise IntegrityError("completion plan file differs from its expected SHA-256")
    if b"data/raw" in raw or re.search(rb"futures_intraday_model(?!_v2)", raw):
        raise IntegrityError("completion plan references a prohibited historical data or repository path")
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IntegrityError("completion plan is not valid UTF-8 JSON") from exc
    if not isinstance(document, dict) or canonical_bytes(document) + b"\n" != raw:
        raise IntegrityError("completion plan is not canonically encoded")
    if document.get("schema_version") != PLAN_SCHEMA:
        raise ContractError("completion plan schema is not supported")
    plan_id = document.get("plan_id")
    core = dict(document)
    core.pop("plan_id", None)
    if plan_id != expected_plan_id or sha256_json(core) != expected_plan_id:
        raise IntegrityError("completion plan ID does not bind its complete content")
    authority = document.get("authority")
    if authority != {
        "active_data_mutation": False,
        "credential_access": False,
        "provider_network_access": False,
        "publication": False,
        "status": PLAN_STATUS,
    }:
        raise UnauthorizedOperation("completion plan claims authority it does not possess")

    fresh = build_completion_plan(root, end_exclusive=END_EXCLUSIVE)
    fresh_core = dict(fresh)
    fresh_core.pop("plan_id", None)
    frozen_estimates = core.get("estimates")
    fresh_estimates = fresh_core.get("estimates")
    if not isinstance(frozen_estimates, dict) or not isinstance(fresh_estimates, dict):
        raise IntegrityError("completion plan storage estimates are invalid")
    recorded_free = frozen_estimates.get("current_free_bytes")
    if type(recorded_free) is not int or recorded_free < 0:
        raise IntegrityError("completion plan recorded free space is invalid")
    fresh_estimates["current_free_bytes"] = recorded_free
    if fresh_core != core:
        raise IntegrityError("completion plan differs from the current provider-free live delta")
    required_free = frozen_estimates.get("required_free_bytes")
    if type(required_free) is not int or required_free < 0:
        raise IntegrityError("completion plan required free space is invalid")
    if shutil.disk_usage(root).free < required_free:
        raise IntegrityError("current free space is below the frozen completion requirement")
    return document


def derive_completion_batch(
    plan: Mapping[str, Any],
    *,
    selection: Mapping[str, Sequence[str]],
    canary_markets: Sequence[str] = (),
    automatic_continuation_after_canaries: bool = False,
) -> dict[str, Any]:
    """Freeze an exact provider/publication batch from one non-authorizing live-delta plan."""

    if plan.get("schema_version") != PLAN_SCHEMA or _SHA256.fullmatch(str(plan.get("plan_id"))) is None:
        raise ContractError("completion parent plan identity is invalid")
    if not selection:
        raise ContractError("completion batch selection is empty")
    normalized_selection: dict[str, tuple[str, ...]] = {}
    for market, schemas in selection.items():
        values = tuple(str(value) for value in schemas)
        if (
            type(market) is not str
            or not market
            or not values
            or len(values) != len(set(values))
            or any(value not in SCHEMAS for value in values)
        ):
            raise ContractError("completion batch selection is invalid")
        normalized_selection[market] = values
    intervals = plan.get("intervals")
    requests = plan.get("requests")
    if not isinstance(intervals, list) or not isinstance(requests, list):
        raise IntegrityError("completion parent plan lacks intervals or requests")
    by_market = {
        str(item["market"]): item
        for item in intervals
        if isinstance(item, Mapping) and isinstance(item.get("market"), str)
    }
    if sorted(set(normalized_selection) - set(by_market)):
        raise ContractError("completion batch selects a market outside the live delta")
    canaries = tuple(str(value) for value in canary_markets)
    if len(canaries) != len(set(canaries)) or any(value not in normalized_selection for value in canaries):
        raise ContractError("completion batch canary selection is invalid")
    ordered_markets = [*canaries, *sorted(set(normalized_selection) - set(canaries))]
    selected_intervals: list[dict[str, Any]] = []
    selected_pairs: list[tuple[str, str]] = []
    expected_final = 0
    high_final = 0
    target_count = 0
    for market in ordered_markets:
        source = dict(by_market[market])
        available = [str(value) for value in source.get("schemas", [])]
        chosen = list(normalized_selection[market])
        if any(schema not in available for schema in chosen):
            raise ContractError("completion batch selects a schema that is not missing")
        estimates = source.get("estimated_final_bytes_by_schema")
        if not isinstance(estimates, Mapping):
            raise IntegrityError("completion interval lacks per-schema storage estimates")
        selected_estimates = {
            schema: dict(estimates[schema])
            for schema in chosen
            if isinstance(estimates.get(schema), Mapping)
        }
        if len(selected_estimates) != len(chosen):
            raise IntegrityError("completion interval per-schema storage estimate is incomplete")
        source["schemas"] = chosen
        source["estimated_final_bytes_by_schema"] = selected_estimates
        source["estimated_final_bytes_expected"] = sum(
            int(value["expected"]) for value in selected_estimates.values()
        )
        source["estimated_final_bytes_high"] = sum(
            int(value["high"]) for value in selected_estimates.values()
        )
        expected_final += int(source["estimated_final_bytes_expected"])
        high_final += int(source["estimated_final_bytes_high"])
        target_count += int(source["year_partition_count_per_schema"]) * len(chosen)
        selected_intervals.append(source)
        selected_pairs.extend((market, schema) for schema in chosen)
    request_index = {
        (str(item.get("market")), str(item.get("schema"))): dict(item)
        for item in requests
        if isinstance(item, Mapping)
    }
    selected_requests = [request_index[pair] for pair in selected_pairs if pair in request_index]
    if len(selected_requests) != len(selected_pairs):
        raise IntegrityError("completion batch request binding is incomplete")
    parent_estimates = plan.get("estimates")
    if not isinstance(parent_estimates, Mapping):
        raise IntegrityError("completion parent plan lacks storage estimates")
    largest = max((int(item["estimated_final_bytes_high"]) for item in selected_intervals), default=0)
    peak = SAFETY_MARGIN_BYTES + PUBLICATION_METADATA_ALLOWANCE_BYTES + 3 * largest
    batch: dict[str, Any] = {
        "authority": dict(plan["authority"]),
        "bindings": dict(plan.get("bindings", {})),
        "canaries": list(canaries),
        "end_exclusive": plan["end_exclusive"],
        "estimates": {
            "current_free_bytes": int(parent_estimates["current_free_bytes"]),
            "download_bytes_expected": expected_final,
            "download_bytes_high": high_final,
            "final_retained_bytes_expected": expected_final,
            "final_retained_bytes_high": high_final,
            "largest_root_batch_bytes_high": largest,
            "peak_temporary_bytes_high": peak,
            "required_free_bytes": high_final + peak,
            "safety_margin_bytes": SAFETY_MARGIN_BYTES,
        },
        "execution_limits": {
            "maximum_concurrent_provider_jobs": 1,
            "maximum_roots_per_provider_job": 1,
            "provider_cost_cap_usd": "0.0",
            "provider_request_count": len(selected_requests),
            "target_dbn_file_count_maximum": target_count,
        },
        "execution_policy": {
            "automatic_continuation_after_canaries": automatic_continuation_after_canaries,
            "canary_markets": list(canaries),
            "expired_job_replacement_authorized": False,
        },
        "intervals": selected_intervals,
        "parent_plan_id": plan["plan_id"],
        "provider": plan["provider"],
        "publication": dict(plan["publication"]),
        "requests": selected_requests,
        "schema_version": BATCH_SCHEMA,
        "selection": {market: list(normalized_selection[market]) for market in ordered_markets},
        "universe": dict(plan["universe"]),
    }
    batch["plan_id"] = sha256_json(batch)
    return batch


def quote_completion_plan(
    plan: Mapping[str, Any],
    *,
    plan_sha256: str,
    get_cost: Callable[..., object],
    get_billable_size: Callable[..., object] | None = None,
    get_record_count: Callable[..., object] | None = None,
) -> dict[str, Any]:
    """Quote every frozen request once without downloading or publishing rows."""

    if _SHA256.fullmatch(plan_sha256) is None:
        raise ContractError("completion plan SHA-256 is invalid")
    if plan.get("schema_version") not in {PLAN_SCHEMA, BATCH_SCHEMA} or _SHA256.fullmatch(str(plan.get("plan_id"))) is None:
        raise ContractError("completion plan identity is invalid")
    authority = plan.get("authority")
    if not isinstance(authority, Mapping) or any(
        authority.get(key) is not False
        for key in ("active_data_mutation", "credential_access", "provider_network_access", "publication")
    ):
        raise UnauthorizedOperation("only a non-authorizing frozen plan may be quoted")
    limits = plan.get("execution_limits")
    requests = plan.get("requests")
    if not isinstance(limits, Mapping) or not isinstance(requests, list):
        raise IntegrityError("completion plan request limits are invalid")
    expected_count = limits.get("provider_request_count")
    if type(expected_count) is not int or expected_count != len(requests) or expected_count > 50:
        raise IntegrityError("completion plan provider request count changed")
    try:
        cap = Decimal(str(limits.get("provider_cost_cap_usd")))
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError("completion plan provider cost cap is invalid") from exc
    if not cap.is_finite() or cap != Decimal("0"):
        raise UnauthorizedOperation("completion quote cost cap must remain exactly zero USD")

    quotes: list[dict[str, Any]] = []
    total = Decimal("0")
    for request in requests:
        if not isinstance(request, Mapping):
            raise IntegrityError("completion plan provider request is invalid")
        kwargs = {
            key: request[key]
            for key in ("dataset", "schema", "symbols", "start", "end", "stype_in")
        }
        try:
            cost = Decimal(str(get_cost(**kwargs)))
        except (InvalidOperation, ValueError) as exc:
            raise IntegrityError("provider returned an invalid completion cost") from exc
        if not cost.is_finite() or cost < 0:
            raise IntegrityError("provider returned an invalid completion cost")
        total += cost
        billable_size = get_billable_size(**kwargs) if get_billable_size is not None else None
        record_count = get_record_count(**kwargs) if get_record_count is not None else None
        if billable_size is not None and (type(billable_size) is not int or billable_size < 0):
            raise IntegrityError("provider returned an invalid completion billable size")
        if record_count is not None and (type(record_count) is not int or record_count <= 0):
            raise IntegrityError("provider returned an invalid completion record count")
        quotes.append(
            {
                "api_billable_uncompressed_bytes": billable_size,
                "estimated_data_cost_usd": format(cost, "f"),
                "market": str(request["market"]),
                "provider_record_count": record_count,
                "provider_rows_downloaded": False,
                "query_id": sha256_json(dict(request)),
                "schema": str(request["schema"]),
            }
        )
    return {
        "authority": {
            "download": False,
            "provider_row_read": False,
            "publication": False,
            "submission": False,
        },
        "cost_cap_usd": format(cap, "f"),
        "estimated_data_cost_usd": format(total, "f"),
        "provider_record_count": (
            sum(int(item["provider_record_count"]) for item in quotes)
            if all(type(item["provider_record_count"]) is int for item in quotes)
            else None
        ),
        "provider_uncompressed_billable_bytes": (
            sum(int(item["api_billable_uncompressed_bytes"]) for item in quotes)
            if all(type(item["api_billable_uncompressed_bytes"]) is int for item in quotes)
            else None
        ),
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha256,
        "provider": "Databento",
        "provider_call_count": len(quotes),
        "quotes": quotes,
        "schema_version": "ohlcv_58_completion_quote/1.0.0",
        "status": "PASS_WITHIN_APPROVED_ZERO_COST_CAP" if total <= cap else "BLOCKED_COST_EXCEEDS_APPROVED_CAP",
    }


def build_staged_execution_manifest(
    plan: Mapping[str, Any],
    *,
    markets: Sequence[str],
    provider_metadata_sha256: str,
) -> list[dict[str, Any]]:
    """Adapt frozen requests to the existing annual acquisition manifest contract.

    The returned paths are relative to an isolated execution root.  They must
    not be executed with the repository root as the acquisition root.
    """

    from .ohlcv_historical_backfill import (
        MANIFEST_SCHEMA,
        bind_manifest,
        build_job_plan,
        half_open_year_slices,
        normalized_request,
        request_fingerprint,
    )

    if _SHA256.fullmatch(provider_metadata_sha256) is None:
        raise ContractError("provider metadata SHA-256 is invalid")
    requested = list(markets)
    if not requested or len(requested) != len(set(requested)):
        raise ContractError("staged execution markets must be unique and nonempty")
    intervals = plan.get("intervals")
    plan_requests = plan.get("requests")
    if not isinstance(intervals, list) or not isinstance(plan_requests, list):
        raise IntegrityError("completion plan execution scope is invalid")
    by_market = {
        str(item["market"]): item
        for item in intervals
        if isinstance(item, Mapping) and isinstance(item.get("market"), str)
    }
    unknown = sorted(set(requested) - set(by_market))
    if unknown:
        raise ContractError(f"staged execution markets are outside the frozen delta: {unknown}")
    inventory: list[dict[str, Any]] = []
    for market in requested:
        interval = by_market[market]
        start = str(interval["start_inclusive"])
        end = str(interval["end_exclusive"])
        slices = half_open_year_slices(start, end)
        interval_schemas = [str(value) for value in interval["schemas"]]
        if not interval_schemas or any(schema not in SCHEMAS for schema in interval_schemas):
            raise IntegrityError("completion interval schemas are invalid")
        per_target_estimate = max(
            1,
            (int(interval["estimated_final_bytes_high"]) + len(slices) * len(interval_schemas) - 1)
            // (len(slices) * len(interval_schemas)),
        )
        for schema in interval_schemas:
            local_schema = SCHEMA_DIRECTORIES[schema]
            for annual in slices:
                annual_start = str(annual["start_inclusive"])
                annual_end = str(annual["end_exclusive"])
                filename = f"{annual_start[:10]}_{annual_end[:10]}.dbn.zst"
                final_path = f"data/dbn/{local_schema}/{market}/{annual['year']}/{filename}"
                target_core = {
                    "final_path": final_path,
                    "intended_end_exclusive": annual_end,
                    "intended_start_inclusive": annual_start,
                    "market": market,
                    "schema": schema,
                    "symbols": [f"{market}.v.0"],
                    "year": int(annual["year"]),
                }
                annual_request = normalized_request(
                    schema=schema,
                    symbols=[f"{market}.v.0"],
                    start=annual_start,
                    end=annual_end,
                    split_duration="none",
                )
                inventory.append(
                    {
                        "actual_dbn_bytes": 0,
                        "actual_sidecar_bytes": 0,
                        "current_state": "MISSING",
                        "existing_bytes": 0,
                        "expected_incremental_bytes": per_target_estimate,
                        "final_path": final_path,
                        "intended_end_exclusive": annual_end,
                        "intended_start_inclusive": annual_start,
                        "market": market,
                        "provider_record_count": None,
                        "request_fingerprint": request_fingerprint(annual_request),
                        "schema": schema,
                        "sidecar_path": final_path + ".manifest.json",
                        "symbol_specification": {
                            "segments": [
                                {
                                    "end_exclusive": end,
                                    "start_inclusive": start,
                                    "symbols": [f"{market}.v.0"],
                                }
                            ],
                            "stype_in": "continuous",
                            "stype_out": "instrument_id",
                            "symbols": [f"{market}.v.0"],
                        },
                        "target_id": sha256_json(target_core),
                        "year": int(annual["year"]),
                    }
                )
    jobs = build_job_plan(inventory)
    expected_requests = [
        dict(item)
        for item in plan_requests
        if isinstance(item, Mapping) and item.get("market") in requested
    ]
    observed_requests = [{**dict(job["request"]), "market": job["market"]} for job in jobs]
    if observed_requests != expected_requests:
        raise IntegrityError("staged acquisition jobs differ from the frozen provider requests")
    return bind_manifest(
        inventory,
        jobs,
        run_id=str(plan["plan_id"]),
        provider_metadata_hash=provider_metadata_sha256,
        provider_condition_hash=provider_metadata_sha256,
    )


def write_jsonl_create_only(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path = path.resolve(strict=False)
    if path.exists():
        raise IntegrityError(f"refusing to overwrite an existing JSONL artifact: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            for row in rows:
                handle.write(canonical_bytes(dict(row)) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        Path(temporary).unlink(missing_ok=True)
        raise


def write_plan(path: Path, plan: Mapping[str, Any]) -> None:
    path = path.resolve(strict=False)
    if path.exists():
        raise IntegrityError(f"refusing to overwrite an existing completion plan: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_bytes(dict(plan)))
            handle.write(b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        Path(temporary).unlink(missing_ok=True)
        raise
