"""Exact, one-use development-only causal-observation canary runner.

The runner is deliberately not a public CLI.  It accepts one externally issued
receipt, consumes it before the first DBN payload open, reads only the literal
packet-bound 2024 sources, creates candidate stages, and independently verifies
them.  It cannot publish, activate, construct research fields, or access the
sealed holdout or forward periods.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from .boundary import OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .causal_observation_foundation import (
    CANARY_CANONICAL_RELEASE_ID,
    CANARY_SOURCE_CONTRACT_ID,
    CAUSAL_OBSERVATION_CONTRACT_ID,
    CausalObservationOperationContext,
    ECONOMICS_RULEBOOK_PATH,
    ECONOMICS_RULEBOOK_SHA256,
    PreparedObservationPartition,
    authorize_canary_row_read,
    prepare_observation_partition,
    prepared_inventory,
)
from .causal_observation_verifier import verify_observation_candidate
from .causal_source_closure import select_exact_standard_source_entries
from .data_layout import STAGING_ROOT
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation.decoder import (
    iter_bars,
    iter_definitions,
    iter_statistics,
    iter_statuses,
)
from .foundation.identity import DefinitionIndex
from .foundation.economics import EconomicsRuleBook
from .foundation.records import (
    INT32_NULL,
    INT64_NULL,
    ProviderBar,
    ProviderDefinition,
    NANO,
)
from .foundation.snapshot import DbnReleaseFile
from .foundation_operation_firewall import issue_current_source_closure_context
from .research_gateway_policy import CAUSAL_OBSERVATION_CANARY_OPERATION
from .source_symbology import build_query_contract


PLAN_SCHEMA = "causal_observation_canary_operation/1.0.0"
RESULT_SCHEMA = "causal_observation_canary_result/1.0.0"
MINUTE_NS = 60_000_000_000
HOUR_NS = 60 * MINUTE_NS
DAY_NS = 24 * HOUR_NS
AVAILABILITY_LAG_NS = 5_000_000_000
PROJECT_ZONE = ZoneInfo("America/Chicago")
SUPPORTED_ROOTS = frozenset({"ES", "CL", "ZN", "6E", "GC", "ZC", "LE"})
ORDERED_ROOTS = ("ES", "CL", "ZN", "6E", "GC", "ZC", "LE")
SOURCE_FAMILIES = frozenset(
    {"definition", "ohlcv_1d", "ohlcv_1h", "ohlcv_1m", "ohlcv_1s", "statistics", "status"}
)


@dataclass(frozen=True, slots=True)
class DecodedMarket:
    definitions: tuple[ProviderDefinition, ...]
    primary_1m: tuple[ProviderBar, ...]
    reference_1s: Mapping[int, Mapping[str, int]]
    reference_1h: Mapping[int, ProviderBar]
    reference_1d: Mapping[int, ProviderBar]
    support_rows: tuple[tuple[int, str, str], ...]
    decoded_record_count: int


@dataclass(frozen=True, slots=True)
class CanaryRunResult:
    result_path: Path
    payload: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class BuiltMarketCandidate:
    prepared: PreparedObservationPartition
    first_observation: Mapping[str, object]
    last_observation: Mapping[str, object]


class MultiplierResolutionError(IntegrityError):
    """Fail closed while retaining source-safe definition diagnostics."""

    def __init__(
        self,
        message: str,
        *,
        market: str,
        definition: ProviderDefinition,
        multiplier_state: str,
    ) -> None:
        super().__init__(message)
        self.details = {
            "market": market,
            "definition_source_file_path": definition.source_file_path,
            "definition_source_file_sha256": definition.source_file_sha256,
            "definition_row_sha256": definition.row_sha256,
            "multiplier_state": multiplier_state,
        }


def _load_economics_rulebook(root: Path) -> EconomicsRuleBook:
    path = root / ECONOMICS_RULEBOOK_PATH
    if sha256_file(path) != ECONOMICS_RULEBOOK_SHA256:
        raise IntegrityError("causal-observation economics rulebook differs")
    return EconomicsRuleBook.from_file(path)


def _resolve_multiplier(
    *,
    rulebook: EconomicsRuleBook,
    market: str,
    definition: ProviderDefinition,
) -> tuple[int, str]:
    raw = definition.unit_of_measure_qty_nano
    if raw < 0:
        raise MultiplierResolutionError(
            "causal definition contains a negative multiplier",
            market=market,
            definition=definition,
            multiplier_state="NEGATIVE_PROVIDER_VALUE",
        )
    try:
        resolved = rulebook.resolve(market, definition)
        expected = rulebook.rules[market].expected_unit_qty * NANO
    except (ContractError, KeyError) as exc:
        raise MultiplierResolutionError(
            "causal definition multiplier contradicts the pinned economics rulebook",
            market=market,
            definition=definition,
            multiplier_state="CONTRADICTORY_OR_UNRESOLVED_PROVIDER_VALUE",
        ) from exc
    integral = expected.to_integral_value()
    if expected != integral or integral <= 0:
        raise IntegrityError("pinned economics multiplier is not a positive nanounit integer")
    if raw not in {0, INT32_NULL, INT64_NULL} and resolved.provider_unit_qty_state != "PROVIDER_DEFINITION_CROSSCHECK_MATCH":
        raise IntegrityError("positive provider multiplier lacks an exact rulebook crosscheck")
    return int(integral), resolved.provider_unit_qty_state


class _StageCreator(Protocol):
    def create_stage(self, purpose: str) -> Path: ...


class _CanaryStageCreator:
    """Create one exact plan-bound stage and expose no publication capability."""

    def __init__(self, *, boundary: RepoBoundary, relative: str) -> None:
        candidate = Path(relative)
        if (
            not relative
            or candidate.is_absolute()
            or ".." in candidate.parts
            or candidate.as_posix() != relative
        ):
            raise ContractError("canary stage path is not an exact relative path")
        self._boundary = boundary
        self._relative = relative
        self._created = False

    def create_stage(self, purpose: str) -> Path:
        if purpose != "causal_observation" or self._created:
            raise UnauthorizedOperation("canary stage creator is exact and one-use")
        stage = self._boundary.assert_active_path(
            self._boundary.active_root / STAGING_ROOT / self._relative,
            purpose="causal-observation canary stage",
            subtree=STAGING_ROOT.as_posix(),
        )
        stage.parent.mkdir(parents=True, exist_ok=True)
        stage.mkdir()
        self._created = True
        return stage


def _json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"canary JSON is invalid: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"canary JSON is not an object: {path}")
    return value


def _contained(root: Path, relative: object) -> Path:
    if type(relative) is not str:
        raise IntegrityError("canary path is not an exact string")
    value = Path(relative)
    if value.is_absolute() or ".." in value.parts or value.as_posix() != relative:
        raise IntegrityError("canary path is not canonical and relative")
    path = (root / value).resolve(strict=False)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation("canary path escapes the repository") from exc
    return path


def _window_ns(window: Mapping[str, object]) -> tuple[int, int]:
    try:
        start = datetime.fromisoformat(str(window["start"]).replace("Z", "+00:00"))
        end = datetime.fromisoformat(str(window["end"]).replace("Z", "+00:00"))
    except (KeyError, ValueError) as exc:
        raise ContractError("canary window is invalid") from exc
    if start.tzinfo != timezone.utc or end.tzinfo != timezone.utc or not start < end:
        raise ContractError("canary window is not an exact UTC interval")
    return int(start.timestamp() * 1_000_000_000), int(end.timestamp() * 1_000_000_000)


def _validate_plan(root: Path, plan: Mapping[str, object]) -> None:
    """Validate the complete nonauthorizing successor before source selection."""

    core = {key: value for key, value in plan.items() if key != "plan_id"}
    authority = plan.get("authority")
    source = plan.get("source")
    limits = plan.get("limits")
    one_use = plan.get("one_use_authorization")
    bindings = plan.get("implementation_bindings")
    windows = plan.get("windows")
    if (
        plan.get("schema_version") != PLAN_SCHEMA
        or plan.get("plan_id") != sha256_json(core)
        or plan.get("status") != "PREPARED_NOT_AUTHORIZED_NO_ROW_READ"
        or plan.get("operation") != CAUSAL_OBSERVATION_CANARY_OPERATION
        or plan.get("causal_contract_id") != CAUSAL_OBSERVATION_CONTRACT_ID
        or plan.get("execution_authorized") is not False
        or plan.get("provider_calls") != 0
        or plan.get("holdout_allowed") is not False
        or plan.get("forward_allowed") is not False
        or plan.get("development_end_exclusive") != "2025-07-13T22:00:00Z"
        or plan.get("roots") != list(ORDERED_ROOTS)
        or not isinstance(authority, Mapping)
        or set(authority)
        != {
            "activation", "evaluation", "features", "fitting", "forward",
            "holdout", "mechanism", "outcomes", "prediction", "provider",
            "publication", "wfa",
        }
        or any(value is not False for value in authority.values())
        or not isinstance(source, Mapping)
        or source.get("source_contract_id") != CANARY_SOURCE_CONTRACT_ID
        or source.get("canonical_release_id") != CANARY_CANONICAL_RELEASE_ID
        or source.get("exact_source_entry_count") != 66
        or source.get("exact_dbn_file_count") != 33
        or source.get("exact_sidecar_file_count") != 33
        or source.get("total_source_bytes") != 176_952_087
        or not isinstance(limits, Mapping)
        or limits.get("maximum_payload_bytes") != 176_929_782
        or not isinstance(limits.get("maximum_decoded_records"), int)
        or int(limits["maximum_decoded_records"]) <= 0
        or not isinstance(limits.get("maximum_output_bytes"), int)
        or int(limits["maximum_output_bytes"]) <= 0
        or one_use
        != {
            "consumed": False,
            "issued": False,
            "required_classification": "EXTERNAL_REAL_HISTORY_AUTHORIZATION",
            "silent_reuse": False,
        }
        or not isinstance(bindings, Mapping)
        or not bindings
        or not isinstance(windows, Mapping)
        or set(windows) != SUPPORTED_ROOTS
    ):
        raise UnauthorizedOperation("canary successor plan is not exact and nonauthorizing")
    output = _contained(root, plan.get("output_staging_path"))
    try:
        output.relative_to(root / STAGING_ROOT)
    except ValueError as exc:
        raise UnauthorizedOperation("canary output is outside the staging authority") from exc
    cutoff = int(
        datetime.fromisoformat("2025-07-13T22:00:00+00:00").timestamp()
        * 1_000_000_000
    )
    for market in ORDERED_ROOTS:
        window = windows[market]
        if not isinstance(window, Mapping):
            raise IntegrityError("canary window is not an object")
        start_ns, end_ns = _window_ns(window)
        if start_ns >= end_ns or end_ns > cutoff:
            raise UnauthorizedOperation("canary window crosses the development boundary")
    required_bindings = {
        "configs/causal_observation_contract_v1.json",
        "src/futures_rebuild/causal_observation_canary.py",
        "src/futures_rebuild/causal_observation_foundation.py",
        "src/futures_rebuild/causal_observation_verifier.py",
        "src/futures_rebuild/causal_source_closure.py",
        "src/futures_rebuild/foundation/decoder.py",
        "src/futures_rebuild/foundation/records.py",
        "src/futures_rebuild/research_gateway_policy.py",
        "tests/test_causal_observation_canary.py",
    }
    if not required_bindings.issubset(bindings):
        raise IntegrityError("canary implementation binding set is incomplete")
    for relative, expected in bindings.items():
        path = _contained(root, relative)
        if type(expected) is not str or sha256_file(path) != expected:
            raise IntegrityError(f"canary implementation binding differs: {relative}")


def _binding(
    *,
    root: Path,
    entry: Mapping[str, object],
    release_id: str,
    release_manifest_sha256: str,
    files_index_sha256: str,
) -> DbnReleaseFile:
    relative = str(entry["path"])
    return DbnReleaseFile(
        logical_path=relative,
        physical_path=_contained(root, relative),
        relative_path=relative,
        size=int(entry["size_bytes"]),
        sha256=str(entry["sha256"]),
        source_release_id=release_id,
        source_manifest_sha256=release_manifest_sha256,
        files_index_sha256=files_index_sha256,
    )


def _query_contract(root: Path, sidecar_entry: Mapping[str, object]) -> dict[str, object]:
    path = _contained(root, sidecar_entry["path"])
    if sha256_file(path) != sidecar_entry["sha256"]:
        raise IntegrityError("canary sidecar identity differs")
    payload = _json(path)
    if (
        payload.get("path") != str(sidecar_entry["path"]).removesuffix(".manifest.json")
        or payload.get("file_sha256") is None
        or payload.get("file_sha256") != sidecar_entry["paired_dbn_sha256"]
        or payload.get("file_size_bytes") != sidecar_entry["paired_dbn_size_bytes"]
    ):
        raise IntegrityError("canary sidecar does not bind its selected DBN")
    return build_query_contract(
        schema=str(payload["schema"]),
        market=str(payload["market"]),
        start=str(payload["start"]),
        end=str(payload["end"]),
        stype_in=payload["stype_in"],
        symbols=payload["symbols_requested"],
    )


def _bar_aggregate(rows: Iterable[ProviderBar]) -> dict[str, int]:
    ordered = sorted(rows, key=lambda row: (row.event_at_ns, row.row_sha256))
    if not ordered:
        raise ContractError("cannot aggregate an empty cadence interval")
    return {
        "open_nano": ordered[0].open_nano,
        "high_nano": max(row.high_nano for row in ordered),
        "low_nano": min(row.low_nano for row in ordered),
        "close_nano": ordered[-1].close_nano,
        "volume": sum(row.volume for row in ordered),
        "count": len(ordered),
    }


def _stream_aggregate(
    target: dict[int, dict[str, int]], key: int, row: ProviderBar
) -> None:
    current = target.get(key)
    if current is None:
        target[key] = {
            "open_nano": row.open_nano,
            "high_nano": row.high_nano,
            "low_nano": row.low_nano,
            "close_nano": row.close_nano,
            "volume": row.volume,
            "count": 1,
        }
        return
    current["high_nano"] = max(current["high_nano"], row.high_nano)
    current["low_nano"] = min(current["low_nano"], row.low_nano)
    current["close_nano"] = row.close_nano
    current["volume"] += row.volume
    current["count"] += 1


def _decode_selected_sources(
    *,
    root: Path,
    selected: Sequence[Mapping[str, object]],
    windows: Mapping[str, Mapping[str, object]],
    source_contract: Mapping[str, object],
    maximum_decoded_records: int,
) -> dict[str, DecodedMarket]:
    """Open and decode only after the caller has consumed exact authority."""

    pairs: dict[str, dict[str, Mapping[str, object]]] = defaultdict(dict)
    for entry in selected:
        path = str(entry["path"])
        base = path.removesuffix(".manifest.json")
        pairs[base][str(entry["kind"])] = entry
    release = source_contract["active_canonical_source"]
    inventory = source_contract["complete_inventory"]
    decoded: dict[str, dict[str, object]] = {
        market: {
            "definitions": [],
            "primary_1m": [],
            "reference_1s": {},
            "reference_1h": {},
            "reference_1d": {},
            "support_rows": [],
            "count": 0,
        }
        for market in windows
    }
    total_count = 0
    for base in sorted(pairs):
        pair = pairs[base]
        if set(pair) != {"DBN", "SIDECAR"}:
            raise IntegrityError("canary DBN/sidecar pairing differs")
        dbn_entry = pair["DBN"]
        sidecar_entry = dict(pair["SIDECAR"])
        sidecar_entry["paired_dbn_sha256"] = dbn_entry["sha256"]
        sidecar_entry["paired_dbn_size_bytes"] = dbn_entry["size_bytes"]
        market = str(dbn_entry["market"])
        family = str(dbn_entry["family"])
        if family not in SOURCE_FAMILIES or market not in decoded:
            raise UnauthorizedOperation("canary decoder received an unapproved source")
        start_ns, end_ns = _window_ns(windows[market])
        binding = _binding(
            root=root,
            entry=dbn_entry,
            release_id=str(release["release_id"]),
            release_manifest_sha256=str(release["release_manifest_sha256"]),
            files_index_sha256=str(inventory["content_inventory_sha256"]),
        )
        query = _query_contract(root, sidecar_entry)
        target = decoded[market]
        if family == "definition":
            iterator = iter_definitions(binding, market=market, expected_query_contract=query)
            for record in iterator:
                total_count += 1
                target["count"] = int(target["count"]) + 1
                if record.ts_recv_ns < end_ns and record.expiration_ns > start_ns:
                    target["definitions"].append(record)  # type: ignore[union-attr]
        elif family in {"ohlcv_1m", "ohlcv_1s", "ohlcv_1h", "ohlcv_1d"}:
            schema = family.replace("_", "-")
            iterator = iter_bars(
                binding, market=market, expected_query_contract=query, schema=schema
            )
            for record in iterator:
                total_count += 1
                target["count"] = int(target["count"]) + 1
                if not start_ns <= record.event_at_ns < end_ns:
                    continue
                if family == "ohlcv_1m":
                    target["primary_1m"].append(record)  # type: ignore[union-attr]
                elif family == "ohlcv_1s":
                    _stream_aggregate(
                        target["reference_1s"],  # type: ignore[arg-type]
                        record.event_at_ns // MINUTE_NS * MINUTE_NS,
                        record,
                    )
                elif family == "ohlcv_1h":
                    target["reference_1h"][record.event_at_ns] = record  # type: ignore[index]
                else:
                    target["reference_1d"][record.event_at_ns // DAY_NS * DAY_NS] = record  # type: ignore[index]
        elif family == "status":
            for record in iter_statuses(binding, market=market, expected_query_contract=query):
                total_count += 1
                target["count"] = int(target["count"]) + 1
                if start_ns <= record.ts_event_ns < end_ns:
                    target["support_rows"].append((record.ts_event_ns, family, record.row_sha256))  # type: ignore[union-attr]
        else:
            for record in iter_statistics(binding, market=market, expected_query_contract=query):
                total_count += 1
                target["count"] = int(target["count"]) + 1
                if start_ns <= record.ts_event_ns < end_ns:
                    target["support_rows"].append((record.ts_event_ns, family, record.row_sha256))  # type: ignore[union-attr]
        if total_count > maximum_decoded_records:
            raise UnauthorizedOperation("canary decoded-record ceiling exceeded")
    return {
        market: DecodedMarket(
            definitions=tuple(value["definitions"]),  # type: ignore[arg-type]
            primary_1m=tuple(value["primary_1m"]),  # type: ignore[arg-type]
            reference_1s=dict(value["reference_1s"]),  # type: ignore[arg-type]
            reference_1h=dict(value["reference_1h"]),  # type: ignore[arg-type]
            reference_1d=dict(value["reference_1d"]),  # type: ignore[arg-type]
            support_rows=tuple(sorted(value["support_rows"])),  # type: ignore[arg-type]
            decoded_record_count=int(value["count"]),
        )
        for market, value in decoded.items()
    }


def _causal_definition(
    index: DefinitionIndex,
    bar: ProviderBar,
    available_at_ns: int,
) -> ProviderDefinition:
    decision_at = datetime.fromtimestamp(
        available_at_ns / 1_000_000_000, tz=timezone.utc
    )
    return index.resolve(bar, decision_at=decision_at)


def _project_grouping(timestamp_ns: int) -> tuple[str, str, int, int]:
    utc = datetime.fromtimestamp(timestamp_ns // 1_000_000_000, tz=timezone.utc)
    local = utc.astimezone(PROJECT_ZONE)
    trade_date = local.date() + timedelta(days=1) if local.timetz().replace(tzinfo=None) >= time(17) else local.date()
    start_local = datetime.combine(trade_date - timedelta(days=1), time(17), PROJECT_ZONE)
    end_local = datetime.combine(trade_date, time(17), PROJECT_ZONE)
    return (
        f"PROJECT-{trade_date.isoformat()}",
        trade_date.isoformat(),
        int(start_local.timestamp() * 1_000_000_000),
        int(end_local.timestamp() * 1_000_000_000),
    )


def _comparison(
    *,
    row_id: str,
    comparison_cadence: str,
    observed: Mapping[str, int],
    reference: Mapping[str, int] | None,
    complete: bool,
) -> dict[str, object]:
    if reference is None:
        result, exception = "SOURCE_MISSING", "REFERENCE_SOURCE_MISSING_NO_OVERWRITE"
    elif not complete:
        result, exception = "NOT_COMPARABLE", "INCOMPLETE_INTERVAL_NO_OVERWRITE"
    else:
        equal = all(observed[name] == reference[name] for name in ("open_nano", "high_nano", "low_nano", "close_nano", "volume"))
        result = "MATCH" if equal else "DISAGREEMENT"
        exception = "NONE" if equal else "PRESERVE_BOTH_NO_OVERWRITE"
    core = {
        "row_id": row_id,
        "source_cadence": "1m",
        "comparison_cadence": comparison_cadence,
        "interval_boundary_compatible": True,
        "result": result,
        "exception_state": exception,
    }
    return {"comparison_id": sha256_json(core), **core}


def _bar_dict(row: ProviderBar) -> dict[str, int]:
    return {
        "open_nano": row.open_nano,
        "high_nano": row.high_nano,
        "low_nano": row.low_nano,
        "close_nano": row.close_nano,
        "volume": row.volume,
        "count": 1,
    }


def _build_market_candidate_with_state(
    *,
    publisher: _StageCreator,
    context: CausalObservationOperationContext,
    market: str,
    window: Mapping[str, object],
    decoded: DecodedMarket,
    allowed_roots: frozenset[str],
    economics_rulebook: EconomicsRuleBook,
    prior_observation: Mapping[str, object] | None = None,
) -> BuiltMarketCandidate:
    """Build one deterministic market candidate from already-authorized rows."""

    if market not in allowed_roots:
        raise UnauthorizedOperation("causal observation market is outside the exact scope")
    start_ns, end_ns = _window_ns(window)
    definitions = DefinitionIndex(decoded.definitions)
    bars = tuple(sorted(decoded.primary_1m, key=lambda row: (row.event_at_ns, row.row_sha256)))
    if not bars or any(not start_ns <= row.event_at_ns < end_ns for row in bars):
        raise IntegrityError("canary primary rows are empty or outside the exact window")
    if len({(row.event_at_ns, row.instrument_id) for row in bars}) != len(bars):
        raise IntegrityError("canary primary source has duplicate time/instrument rows")

    observations: list[dict[str, object]] = []
    missingness: list[dict[str, object]] = []
    rolls: list[dict[str, object]] = []
    quality: list[dict[str, object]] = []
    cadence: list[dict[str, object]] = []
    initial_prior = dict(prior_observation) if prior_observation is not None else None
    previous_observation = initial_prior
    by_hour: dict[int, list[ProviderBar]] = defaultdict(list)
    by_day: dict[int, list[ProviderBar]] = defaultdict(list)

    for bar in bars:
        bar_end = bar.event_at_ns + MINUTE_NS
        available = bar_end + AVAILABILITY_LAG_NS
        definition = _causal_definition(definitions, bar, available)
        multiplier, multiplier_state = _resolve_multiplier(
            rulebook=economics_rulebook,
            market=market,
            definition=definition,
        )
        session_id, trade_date, group_start, group_end = _project_grouping(bar.event_at_ns)
        core: dict[str, object] = {
            "market": market,
            "source_contract_id": context.source_contract_id,
            "source_release_id": context.source_release_id,
            "source_file_path": bar.source_file_path,
            "source_file_sha256": bar.source_file_sha256,
            "source_row_sha256": bar.row_sha256,
            "source_cadence": "1m",
            "bar_start_ns": bar.event_at_ns,
            "bar_end_ns": bar_end,
            "source_timestamp_ns": bar.event_at_ns,
            "available_at_ns": available,
            "decision_eligible_at_ns": available,
            "publisher_id": bar.publisher_id,
            "instrument_id": bar.instrument_id,
            "raw_symbol": definition.raw_symbol,
            "actual_contract": definition.raw_symbol,
            "definition_source_file_path": definition.source_file_path,
            "definition_source_file_sha256": definition.source_file_sha256,
            "definition_row_sha256": definition.row_sha256,
            "definition_event_at_ns": definition.ts_event_ns,
            "definition_received_at_ns": definition.ts_recv_ns,
            "listing_activation_ns": definition.activation_ns,
            "expiration_ns": definition.expiration_ns,
            "open_nano": bar.open_nano,
            "high_nano": bar.high_nano,
            "low_nano": bar.low_nano,
            "close_nano": bar.close_nano,
            "volume": bar.volume,
            "currency": definition.currency,
            "min_price_increment_nano": definition.min_price_increment_nano,
            "multiplier_nano": multiplier,
            "project_session_id": session_id,
            "project_trade_date": trade_date,
            "project_grouping_start_ns": group_start,
            "project_grouping_end_ns": group_end,
            "project_timezone": "America/Chicago",
            "official_schedule_state": "UNKNOWN_FAIL_CLOSED",
        }
        row = {"row_id": sha256_json(core), **core}
        observations.append(row)
        evidence = {
            "market": market,
            "source_row_sha256": bar.row_sha256,
            "interval_start_ns": bar.event_at_ns,
            "interval_end_ns": bar_end,
            "authority": "DECODED_CANONICAL_SOURCE_ROW",
        }
        evidence_sha = sha256_json(evidence)
        missing_core = {
            "observation_row_id": row["row_id"],
            "market": market,
            "interval_start_ns": bar.event_at_ns,
            "interval_end_ns": bar_end,
            "state": "OBSERVED_VALID",
            "authority": "DECODED_CANONICAL_SOURCE_ROW",
            "evidence_sha256": evidence_sha,
        }
        missingness.append({"evidence_id": sha256_json(missing_core), **missing_core})
        prior_contract = str(previous_observation["actual_contract"]) if previous_observation else definition.raw_symbol
        is_roll = previous_observation is not None and prior_contract != definition.raw_symbol
        rolls.append(
            {
                "row_id": row["row_id"],
                "actual_contract_before": prior_contract,
                "actual_contract_after": definition.raw_symbol,
                "effective_time_ns": bar.event_at_ns if is_roll else None,
                "causal_selection_evidence_sha256": sha256_json(
                    {
                        "definition_row_sha256": definition.row_sha256,
                        "definition_received_at_ns": definition.ts_recv_ns,
                        "prior_contract": prior_contract,
                    }
                ),
                "roll_flag": is_roll,
                "price_discontinuity_flag": bool(
                    is_roll and int(previous_observation["close_nano"]) != bar.open_nano
                ) if previous_observation else False,
                "crossing_status": "ROLL_BOUNDARY_UNADJUSTED" if is_roll else "NO_CROSSING",
            }
        )
        flags = [
            "OHLC_VOLUME_TIMESTAMP_VALID",
            f"MULTIPLIER_{multiplier_state}",
            f"ECONOMICS_RULEBOOK_SHA256_{ECONOMICS_RULEBOOK_SHA256}",
        ]
        if min(bar.open_nano, bar.high_nano, bar.low_nano, bar.close_nano) < 0:
            flags.append("PROVIDER_VALID_NEGATIVE_PRICE")
        quality.append(
            {
                "row_id": row["row_id"],
                "row_identity_sha256": row["row_id"],
                "ohlc_valid": True,
                "volume_valid": True,
                "timestamp_order_valid": True,
                "duplicate_state": "UNIQUE",
                "source_contract_id": context.source_contract_id,
                "source_release_id": context.source_release_id,
                "source_file_sha256": bar.source_file_sha256,
                "quality_flags": flags,
            }
        )
        by_hour[bar.event_at_ns // HOUR_NS * HOUR_NS].append(bar)
        by_day[bar.event_at_ns // DAY_NS * DAY_NS].append(bar)
        previous_observation = row

    if initial_prior is not None:
        gap_start = int(initial_prior["bar_end_ns"])
        gap_end = int(observations[0]["bar_start_ns"])
        if gap_end > gap_start:
            support = [
                {"event_at_ns": ts, "family": family, "row_sha256": row_sha}
                for ts, family, row_sha in decoded.support_rows
                if gap_start <= ts < gap_end
            ]
            evidence_sha = sha256_json(
                {"gap_start_ns": gap_start, "gap_end_ns": gap_end, "support_rows": support}
            )
            gap_core = {
                "observation_row_id": None,
                "market": market,
                "interval_start_ns": gap_start,
                "interval_end_ns": gap_end,
                "state": "UNKNOWN_FAIL_CLOSED",
                "authority": "OBSERVED_ABSENCE_WITH_STATUS_REVIEW_NO_SCHEDULE_AUTHORITY",
                "evidence_sha256": evidence_sha,
            }
            missingness.append({"evidence_id": sha256_json(gap_core), **gap_core})

    for left, right in zip(observations, observations[1:]):
        gap_start = int(left["bar_end_ns"])
        gap_end = int(right["bar_start_ns"])
        if gap_end <= gap_start:
            continue
        support = [
            {"event_at_ns": ts, "family": family, "row_sha256": row_sha}
            for ts, family, row_sha in decoded.support_rows
            if gap_start <= ts < gap_end
        ]
        evidence_sha = sha256_json(
            {"gap_start_ns": gap_start, "gap_end_ns": gap_end, "support_rows": support}
        )
        gap_core = {
            "observation_row_id": None,
            "market": market,
            "interval_start_ns": gap_start,
            "interval_end_ns": gap_end,
            "state": "UNKNOWN_FAIL_CLOSED",
            "authority": "OBSERVED_ABSENCE_WITH_STATUS_REVIEW_NO_SCHEDULE_AUTHORITY",
            "evidence_sha256": evidence_sha,
        }
        missingness.append({"evidence_id": sha256_json(gap_core), **gap_core})

    observation_by_start = {int(row["bar_start_ns"]): row for row in observations}
    if decoded.reference_1s:
        for start, row in observation_by_start.items():
            reference = decoded.reference_1s.get(start)
            observed = {
                name: int(row[name])
                for name in ("open_nano", "high_nano", "low_nano", "close_nano", "volume")
            }
            cadence.append(
                _comparison(
                    row_id=str(row["row_id"]),
                    comparison_cadence="1s",
                    observed=observed,
                    reference=reference,
                    complete=reference is not None and int(reference["count"]) == 60,
                )
            )
    for start, rows in sorted(by_hour.items()):
        first = observation_by_start[min(row.event_at_ns for row in rows)]
        observed = _bar_aggregate(rows)
        reference_row = decoded.reference_1h.get(start)
        cadence.append(
            _comparison(
                row_id=str(first["row_id"]),
                comparison_cadence="1h",
                observed=observed,
                reference=None if reference_row is None else _bar_dict(reference_row),
                complete=len(rows) == 60,
            )
        )
    for start, rows in sorted(by_day.items()):
        first = observation_by_start[min(row.event_at_ns for row in rows)]
        observed = _bar_aggregate(rows)
        reference_row = decoded.reference_1d.get(start)
        cadence.append(
            _comparison(
                row_id=str(first["row_id"]),
                comparison_cadence="1d",
                observed=observed,
                reference=None if reference_row is None else _bar_dict(reference_row),
                complete=len(rows) == 1_440,
            )
        )

    start_date = datetime.fromtimestamp(start_ns // 1_000_000_000, tz=timezone.utc).date()
    end_date = datetime.fromtimestamp(end_ns // 1_000_000_000, tz=timezone.utc).date()
    prepared = prepare_observation_partition(
        publisher=publisher,
        context=context,
        market=market,
        year=start_date.year,
        interval=f"{start_date.isoformat()}_{end_date.isoformat()}",
        observations=observations,
        missingness=missingness,
        rolls=rolls,
        quality=quality,
        cadence=cadence,
    )
    return BuiltMarketCandidate(
        prepared=prepared,
        first_observation=observations[0],
        last_observation=observations[-1],
    )


def build_market_candidate(
    *,
    publisher: _StageCreator,
    context: CausalObservationOperationContext,
    market: str,
    window: Mapping[str, object],
    decoded: DecodedMarket,
    economics_rulebook: EconomicsRuleBook,
) -> PreparedObservationPartition:
    """Build one seven-root canary candidate with the shared causal mechanics."""

    return _build_market_candidate_with_state(
        publisher=publisher,
        context=context,
        market=market,
        window=window,
        decoded=decoded,
        allowed_roots=SUPPORTED_ROOTS,
        economics_rulebook=economics_rulebook,
    ).prepared


Decoder = Callable[..., dict[str, DecodedMarket]]


def _authorize_then_decode(
    *,
    boundary: RepoBoundary,
    receipt: OperationReceipt,
    plan: Mapping[str, object],
    plan_sha256: str,
    selected: Sequence[Mapping[str, object]],
    source_contract: Mapping[str, object],
    decoder: Decoder,
) -> tuple[CausalObservationOperationContext, dict[str, DecodedMarket]]:
    context = authorize_canary_row_read(
        boundary=boundary,
        receipt=receipt,
        plan=plan,
        plan_sha256=plan_sha256,
    )
    decoded = decoder(
        root=boundary.active_root,
        selected=selected,
        windows=plan["windows"],
        source_contract=source_contract,
        maximum_decoded_records=int(plan["limits"]["maximum_decoded_records"]),
    )
    return context, decoded


def run_authorized_canary(
    *,
    repository_root: Path,
    receipt: OperationReceipt,
    plan_path: Path | None = None,
) -> CanaryRunResult:
    """Consume one exact approval and create verified, nonpublished candidates."""

    root = repository_root.resolve(strict=True)
    boundary = RepoBoundary(root)
    path = (plan_path or root / "configs/causal_observation_canary_plan_v2.json").resolve(strict=True)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise UnauthorizedOperation("canary plan is outside the repository") from exc
    plan = _json(path)
    plan_sha = sha256_file(path)
    _validate_plan(root, plan)
    predecessor_path = _contained(root, plan["source"]["predecessor_canary_plan_path"])
    if sha256_file(predecessor_path) != plan["source"]["predecessor_canary_plan_sha256"]:
        raise IntegrityError("representative canary source plan differs")
    predecessor = _json(predecessor_path)
    entries = predecessor.get("source_files")
    if not isinstance(entries, list) or sha256_json(entries) != plan["source"]["exact_source_entries_sha256"]:
        raise IntegrityError("representative canary source entries differ")
    closure_context = issue_current_source_closure_context(root)
    selected = select_exact_standard_source_entries(
        root,
        operation_context=closure_context,
        source_entries=entries,
        windows=plan["windows"],
    )
    if (
        len(selected) != 66
        or sum(item["kind"] == "DBN" for item in selected) != 33
        or sum(int(item["size_bytes"]) for item in selected if item["kind"] == "DBN")
        != int(plan["limits"]["maximum_payload_bytes"])
    ):
        raise IntegrityError("selected canary source scope differs")
    output = _contained(root, plan["output_staging_path"])
    if output.exists():
        raise IntegrityError("canary output staging path already exists")
    source_contract_path = root / "configs/source_contract.json"
    source_contract = _json(source_contract_path)
    economics_rulebook = _load_economics_rulebook(root)
    context, decoded = _authorize_then_decode(
        boundary=boundary,
        receipt=receipt,
        plan=plan,
        plan_sha256=plan_sha,
        selected=selected,
        source_contract=source_contract,
        decoder=_decode_selected_sources,
    )
    relative_output = output.relative_to(root / STAGING_ROOT).as_posix()
    candidates: dict[str, object] = {}
    total_output_bytes = 0
    total_decoded = 0
    for market in plan["roots"]:
        market_decoded = decoded[str(market)]
        publisher = _CanaryStageCreator(
            boundary=boundary,
            relative=f"{relative_output}/{str(market)}",
        )
        prepared = build_market_candidate(
            publisher=publisher,
            context=context,
            market=str(market),
            window=plan["windows"][str(market)],
            decoded=market_decoded,
            economics_rulebook=economics_rulebook,
        )
        certificate = verify_observation_candidate(
            stage=prepared.stage,
            manifest=prepared.manifest,
            economics_rulebook=economics_rulebook,
        )
        inventory = prepared_inventory(prepared)
        total_output_bytes += sum(item["size"] for item in inventory["files"])
        total_decoded += market_decoded.decoded_record_count
        candidates[str(market)] = {
            "stage": prepared.stage.relative_to(root).as_posix(),
            "release_id": prepared.manifest.release_id,
            "certificate": certificate,
            "inventory_sha256": inventory["files_sha256"],
        }
    if total_output_bytes > int(plan["limits"]["maximum_output_bytes"]):
        raise UnauthorizedOperation("canary output byte ceiling exceeded")
    result_core: dict[str, object] = {
        "schema_version": RESULT_SCHEMA,
        "status": "PASS_AUTHORIZED_DEVELOPMENT_CANARY_CANDIDATE_NOT_PUBLISHED_NOT_ACTIVE",
        "plan_id": plan["plan_id"],
        "plan_sha256": plan_sha,
        "receipt_id": receipt.receipt_id,
        "source_contract_id": context.source_contract_id,
        "source_release_id": context.source_release_id,
        "causal_contract_id": context.causal_contract_id,
        "source_entry_count": len(selected),
        "dbn_file_count": 33,
        "payload_bytes_opened_maximum": int(plan["limits"]["maximum_payload_bytes"]),
        "decoded_record_count": total_decoded,
        "output_bytes": total_output_bytes,
        "candidates": candidates,
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
    }
    payload = {**result_core, "result_id": sha256_json(result_core)}
    output.mkdir(parents=True, exist_ok=True)
    result_path = output / "canary_result.json"
    descriptor = os.open(
        result_path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, canonical_bytes(payload) + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return CanaryRunResult(result_path=result_path, payload=payload)
