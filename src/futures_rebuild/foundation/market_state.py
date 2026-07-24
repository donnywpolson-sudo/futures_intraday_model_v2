"""Causal status/statistics ledgers and immutable non-alpha foundation releases.

Status controls mechanical eligibility as of a decision. Statistics are retained
with exact NEW/DELETE semantics for predeclared foundation diagnostics only;
they are never silently admitted to the feature matrix or label construction.
"""

from __future__ import annotations

import bisect
import json
import os
import re
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Iterator, Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from ..boundary import RepoBoundary
from ..canonical import (
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    is_linklike,
    sha256_file,
    sha256_json,
)
from ..errors import ContractError, IntegrityError
from ..data_layout import (
    MANIFEST_ROOT,
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
    verify_data_release_manifest,
)
from .decoder import iter_statistics, iter_statuses
from .materialize import CAUSAL_RELEASE_KIND, load_causal_interval
from .records import INT64_NULL, StatisticsRecordV1, StatusRecordV1
from .selection import ResolvedFoundationSelection, SelectedFamilyFile
from .support import VerifiedFoundationPolicies


MARKET_STATE_RELEASE_KIND = "futures_status_statistics_foundation"
MARKET_STATE_SCHEMA_VERSION = "3.0.0"
MARKET_STATE_RESUME_VERSION = "1.0.0"
MARKET_STATE_ATTEMPT_CAP = 4
STATUS_ELIGIBILITY_RELEASE_KIND = "futures_status_asof_eligibility"
STATUS_ELIGIBILITY_SCHEMA_VERSION = "2.0.0"
STATUS_ELIGIBLE_KEY_SCHEMA = pa.schema(
    [
        pa.field("actual_identity_hash", pa.string(), nullable=False),
        pa.field("bar_event_at_ns", pa.int64(), nullable=False),
        pa.field("decision_at_ns", pa.int64(), nullable=False),
    ],
    metadata={"schema_id": "FUTURES_STATUS_ELIGIBLE_JOIN_KEYS_V1"},
)

_KNOWN_TRI_STATES = {"YES", "NO"}
_INELIGIBLE_ACTIONS = {
    "CLOSE": "STATUS_CLOSED",
    "HALT": "STATUS_HALTED",
    "NOT_AVAILABLE_FOR_TRADING": "STATUS_NOT_AVAILABLE_FOR_TRADING",
    "PAUSE": "STATUS_PAUSED",
    "POST_CLOSE": "STATUS_CLOSED",
    "PRE_CLOSE": "STATUS_CLOSING",
    "PRE_OPEN": "STATUS_NOT_TRADING",
    "SUSPEND": "STATUS_SUSPENDED",
}


class _InterruptedStatisticsPair(IntegrityError):
    """The staged raw/ledger pair is an exact but incomplete source prefix."""


def _status_semantic_state(record: StatusRecordV1) -> tuple[object, ...]:
    return (
        record.market,
        record.ts_event_ns,
        record.action,
        record.reason,
        record.trading_event,
        record.is_trading,
        record.is_quoting,
        record.is_short_sell_restricted,
    )


def _statistics_semantic_state(record: StatisticsRecordV1) -> tuple[object, ...]:
    return (
        record.market,
        record.ts_event_ns,
        record.ts_ref_ns,
        record.ts_in_delta,
        record.stat_type,
        record.update_action,
        record.price_nano,
        record.quantity,
        record.sequence,
        record.channel_id,
        record.flags,
    )


def _normalize_equal_receive_rows(
    values: Sequence[StatusRecordV1] | Sequence[StatisticsRecordV1],
    *,
    kind: str,
) -> tuple[StatusRecordV1, ...] | tuple[StatisticsRecordV1, ...]:
    """Resolve source order without inventing an order across source files."""

    grouped: dict[int, list[StatusRecordV1 | StatisticsRecordV1]] = {}
    positions: set[tuple[str, int]] = set()
    for item in values:
        position = (item.source_file_path, item.row_ordinal)
        if position in positions:
            raise ContractError(f"{kind} ledger repeats a source row position")
        positions.add(position)
        grouped.setdefault(item.ts_recv_ns, []).append(item)
    result: list[StatusRecordV1 | StatisticsRecordV1] = []
    for receive_ns in sorted(grouped):
        by_file: dict[str, list[StatusRecordV1 | StatisticsRecordV1]] = {}
        for item in grouped[receive_ns]:
            by_file.setdefault(item.source_file_path, []).append(item)
        terminal: dict[str, StatusRecordV1 | StatisticsRecordV1] = {}
        for source_path, rows in by_file.items():
            terminal[source_path] = max(rows, key=lambda item: item.row_ordinal)
        signatures = {
            source_path: (
                _status_semantic_state(item)
                if kind == "status"
                else _statistics_semantic_state(item)  # type: ignore[arg-type]
            )
            for source_path, item in terminal.items()
        }
        if len(set(signatures.values())) != 1:
            raise ContractError(
                f"{kind} ledger has a conflicting equal-receive cross-file state"
            )
        result.append(terminal[min(terminal)])
    return tuple(result)  # type: ignore[return-value]


def _canonical_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} JSON is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


@dataclass
class _ProviderTimestampCensus:
    row_count: int = 0
    negative_delta_rows: int = 0
    zero_delta_rows: int = 0
    positive_delta_rows: int = 0
    cross_utc_date_rows: int = 0
    undefined_timestamp_rows: int = 0
    receive_order_violation_rows: int = 0
    minimum_delta_ns: int | None = None
    maximum_delta_ns: int | None = None
    _prior_receive_ns: int | None = None

    def observe(self, record: StatusRecordV1 | StatisticsRecordV1) -> None:
        self.row_count += 1
        undefined = int(
            record.ts_event_ns in {0, 2**64 - 1}
            or record.ts_recv_ns in {0, 2**64 - 1}
        )
        self.undefined_timestamp_rows += undefined
        delta = record.ts_recv_ns - record.ts_event_ns
        self.negative_delta_rows += int(delta < 0)
        self.zero_delta_rows += int(delta == 0)
        self.positive_delta_rows += int(delta > 0)
        self.cross_utc_date_rows += int(
            record.ts_recv_ns // 86_400_000_000_000
            != record.ts_event_ns // 86_400_000_000_000
        )
        if (
            self._prior_receive_ns is not None
            and record.ts_recv_ns < self._prior_receive_ns
        ):
            self.receive_order_violation_rows += 1
        self._prior_receive_ns = record.ts_recv_ns
        self.minimum_delta_ns = (
            delta if self.minimum_delta_ns is None else min(self.minimum_delta_ns, delta)
        )
        self.maximum_delta_ns = (
            delta if self.maximum_delta_ns is None else max(self.maximum_delta_ns, delta)
        )

    def as_dict(self) -> dict[str, object]:
        if self.row_count <= 0 or self.minimum_delta_ns is None or self.maximum_delta_ns is None:
            raise IntegrityError("provider timestamp census cannot describe an empty stream")
        return {
            "clock_contract": "TS_RECV_INDEX_TS_EVENT_AUDIT_ONLY",
            "cross_utc_date_rows": self.cross_utc_date_rows,
            "maximum_ts_recv_minus_ts_event_ns": self.maximum_delta_ns,
            "minimum_ts_recv_minus_ts_event_ns": self.minimum_delta_ns,
            "negative_delta_rows": self.negative_delta_rows,
            "positive_delta_rows": self.positive_delta_rows,
            "receive_order_violation_rows": self.receive_order_violation_rows,
            "row_count": self.row_count,
            "schema_version": "1.0.0",
            "undefined_timestamp_rows": self.undefined_timestamp_rows,
            "zero_delta_rows": self.zero_delta_rows,
        }


def _aggregate_timestamp_censuses(
    censuses: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    if not censuses:
        raise IntegrityError("provider timestamp census family is empty")
    expected_keys = set(_ProviderTimestampCensus(
        row_count=1,
        minimum_delta_ns=0,
        maximum_delta_ns=0,
    ).as_dict())
    if any(set(item) != expected_keys for item in censuses):
        raise IntegrityError("provider timestamp census schema is not exact")
    result = {
        "clock_contract": "TS_RECV_INDEX_TS_EVENT_AUDIT_ONLY",
        "cross_utc_date_rows": sum(int(item["cross_utc_date_rows"]) for item in censuses),
        "maximum_ts_recv_minus_ts_event_ns": max(
            int(item["maximum_ts_recv_minus_ts_event_ns"]) for item in censuses
        ),
        "minimum_ts_recv_minus_ts_event_ns": min(
            int(item["minimum_ts_recv_minus_ts_event_ns"]) for item in censuses
        ),
        "negative_delta_rows": sum(int(item["negative_delta_rows"]) for item in censuses),
        "positive_delta_rows": sum(int(item["positive_delta_rows"]) for item in censuses),
        "receive_order_violation_rows": sum(
            int(item["receive_order_violation_rows"]) for item in censuses
        ),
        "row_count": sum(int(item["row_count"]) for item in censuses),
        "schema_version": "1.0.0",
        "undefined_timestamp_rows": sum(
            int(item["undefined_timestamp_rows"]) for item in censuses
        ),
        "zero_delta_rows": sum(int(item["zero_delta_rows"]) for item in censuses),
    }
    if (
        result["negative_delta_rows"]
        + result["zero_delta_rows"]
        + result["positive_delta_rows"]
        != result["row_count"]
    ):
        raise IntegrityError("provider timestamp census sign counts are invalid")
    return result


@dataclass(frozen=True)
class FoundationCoveragePolicy:
    minimum_bar_rows: int
    minimum_status_gated_feature_ready_fraction: Decimal
    minimum_status_gated_feature_ready_rows: int
    minimum_status_eligible_rows: int
    minimum_status_resolved_decision_fraction: Decimal
    minimum_statistics_source_market_year_fraction: Decimal
    minimum_status_source_market_year_fraction: Decimal

    @classmethod
    def from_file(cls, path: Path) -> "FoundationCoveragePolicy":
        payload = _canonical_object(path, description="foundation coverage policy")
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: object) -> "FoundationCoveragePolicy":
        if not isinstance(payload, dict):
            raise ContractError("foundation coverage policy must be an object")
        if set(payload) != {
            "minimum_bar_rows",
            "minimum_status_gated_feature_ready_fraction",
            "minimum_status_gated_feature_ready_rows",
            "minimum_statistics_source_market_year_fraction",
            "minimum_status_eligible_rows",
            "minimum_status_resolved_decision_fraction",
            "minimum_status_source_market_year_fraction",
            "policy_version",
        } or payload.get("policy_version") != "1.0.0":
            raise ContractError("foundation coverage policy schema/version is invalid")
        try:
            result = cls(
                minimum_bar_rows=payload["minimum_bar_rows"],  # type: ignore[arg-type]
                minimum_status_gated_feature_ready_fraction=Decimal(
                    payload["minimum_status_gated_feature_ready_fraction"]  # type: ignore[arg-type]
                ),
                minimum_status_gated_feature_ready_rows=payload[
                    "minimum_status_gated_feature_ready_rows"
                ],  # type: ignore[arg-type]
                minimum_status_eligible_rows=payload["minimum_status_eligible_rows"],  # type: ignore[arg-type]
                minimum_status_resolved_decision_fraction=Decimal(
                    payload["minimum_status_resolved_decision_fraction"]  # type: ignore[arg-type]
                ),
                minimum_statistics_source_market_year_fraction=Decimal(
                    payload["minimum_statistics_source_market_year_fraction"]  # type: ignore[arg-type]
                ),
                minimum_status_source_market_year_fraction=Decimal(
                    payload["minimum_status_source_market_year_fraction"]  # type: ignore[arg-type]
                ),
            )
        except (InvalidOperation, TypeError) as exc:
            raise ContractError("foundation coverage policy values are invalid") from exc
        for value in (
            result.minimum_bar_rows,
            result.minimum_status_gated_feature_ready_rows,
            result.minimum_status_eligible_rows,
        ):
            if type(value) is not int or value <= 0:
                raise ContractError("foundation minimum row gates must be positive integers")
        for value in (
            result.minimum_statistics_source_market_year_fraction,
            result.minimum_status_gated_feature_ready_fraction,
            result.minimum_status_resolved_decision_fraction,
            result.minimum_status_source_market_year_fraction,
        ):
            if not value.is_finite() or value < 0 or value > 1:
                raise ContractError("foundation source coverage fraction is invalid")
        if result.as_dict() != payload:
            raise ContractError("foundation coverage policy is not canonical")
        return result

    def as_dict(self) -> dict[str, object]:
        return {
            "minimum_bar_rows": self.minimum_bar_rows,
            "minimum_status_gated_feature_ready_fraction": str(
                self.minimum_status_gated_feature_ready_fraction
            ),
            "minimum_status_gated_feature_ready_rows": (
                self.minimum_status_gated_feature_ready_rows
            ),
            "minimum_statistics_source_market_year_fraction": str(
                self.minimum_statistics_source_market_year_fraction
            ),
            "minimum_status_eligible_rows": self.minimum_status_eligible_rows,
            "minimum_status_resolved_decision_fraction": str(
                self.minimum_status_resolved_decision_fraction
            ),
            "minimum_status_source_market_year_fraction": str(
                self.minimum_status_source_market_year_fraction
            ),
            "policy_version": "1.0.0",
        }

    @property
    def policy_hash(self) -> str:
        return sha256_json(self.as_dict())


@dataclass(frozen=True)
class StatisticsRolePolicy:
    roles: Mapping[str, str]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.roles, Mapping)
            or not self.roles
            or any(
                type(key) is not str
                or not key
                or type(value) is not str
                or not value
                for key, value in self.roles.items()
            )
        ):
            raise ContractError("statistics role mapping is invalid")
        object.__setattr__(
            self, "roles", MappingProxyType(dict(sorted(self.roles.items())))
        )

    @classmethod
    def from_file(cls, path: Path) -> "StatisticsRolePolicy":
        payload = _canonical_object(path, description="statistics role policy")
        if set(payload) != {
            "default_role",
            "feature_eligible_statistic_types",
            "policy_version",
            "roles",
        } or payload.get("policy_version") != "1.0.0":
            raise ContractError("statistics role policy schema/version is invalid")
        raw_roles = payload.get("roles")
        if (
            payload.get("default_role") != "UNDECLARED_NO_FOUNDATION_ROLE"
            or payload.get("feature_eligible_statistic_types") != []
            or not isinstance(raw_roles, dict)
            or not raw_roles
            or any(type(key) is not str or type(value) is not str for key, value in raw_roles.items())
        ):
            raise ContractError("statistics role policy does not fail closed")
        result = cls(raw_roles)
        if result.as_dict() != payload:
            raise ContractError("statistics role policy is not canonical")
        return result

    def role_for(self, stat_type: str) -> str:
        return self.roles.get(stat_type, "UNDECLARED_NO_FOUNDATION_ROLE")

    def as_dict(self) -> dict[str, object]:
        return {
            "default_role": "UNDECLARED_NO_FOUNDATION_ROLE",
            "feature_eligible_statistic_types": [],
            "policy_version": "1.0.0",
            "roles": dict(self.roles),
        }

    @property
    def policy_hash(self) -> str:
        return sha256_json(self.as_dict())


@dataclass(frozen=True)
class StatusDecisionV1:
    status_disposition: str
    status_resolved: bool
    foundation_eligible: bool
    long_eligible: bool
    short_eligible: bool
    in_coverage_denominator: bool
    matched_status_record_id: str | None
    status_ts_event_ns: int | None
    status_ts_recv_ns: int | None

    def as_dict(self) -> dict[str, object]:
        return {
            "foundation_eligible": self.foundation_eligible,
            "in_coverage_denominator": self.in_coverage_denominator,
            "long_eligible": self.long_eligible,
            "matched_status_record_id": self.matched_status_record_id,
            "short_eligible": self.short_eligible,
            "status_disposition": self.status_disposition,
            "status_resolved": self.status_resolved,
            "status_ts_event_ns": self.status_ts_event_ns,
            "status_ts_recv_ns": self.status_ts_recv_ns,
        }


class AsOfStatusLedger:
    """Deterministic as-received ledger; provider event time is audit-only."""

    def __init__(self, records: Iterable[StatusRecordV1]) -> None:
        grouped: dict[tuple[str, int, int, str], list[StatusRecordV1]] = {}
        seen: set[str] = set()
        for record in records:
            if type(record) is not StatusRecordV1 or record.record_id in seen:
                raise ContractError("status ledger contains a duplicate or invalid record")
            seen.add(record.record_id)
            grouped.setdefault(
                (
                    record.dataset,
                    record.publisher_id,
                    record.instrument_id,
                    record.instrument_id_date_utc,
                ),
                [],
            ).append(record)
        self._records: dict[tuple[str, int, int, str], tuple[StatusRecordV1, ...]] = {}
        self._receive_keys: dict[tuple[str, int, int, str], tuple[int, ...]] = {}
        for key, values in grouped.items():
            ordered = _normalize_equal_receive_rows(
                values,
                kind="status",
            )
            assert all(type(item) is StatusRecordV1 for item in ordered)
            self._records[key] = ordered
            self._receive_keys[key] = tuple(item.ts_recv_ns for item in ordered)

    def as_of(
        self,
        *,
        dataset: str,
        publisher_id: int,
        instrument_id: int,
        instrument_id_date_utc: str,
        decision_at_ns: int,
    ) -> StatusDecisionV1:
        if type(decision_at_ns) is not int or decision_at_ns < 0:
            raise ContractError("status decision time must be nonnegative exact nanoseconds")
        key = (dataset, publisher_id, instrument_id, instrument_id_date_utc)
        values = self._records.get(key, ())
        receive_keys = self._receive_keys.get(key, ())
        index = bisect.bisect_right(receive_keys, decision_at_ns) - 1
        matched = values[index] if index >= 0 else None
        if matched is None:
            return StatusDecisionV1(
                "STATUS_UNRESOLVED",
                False,
                False,
                False,
                False,
                True,
                None,
                None,
                None,
            )
        if any(
            state not in _KNOWN_TRI_STATES
            for state in (
                matched.is_trading,
                matched.is_quoting,
            )
        ) or matched.action.startswith("UNKNOWN_"):
            disposition = "STATUS_UNKNOWN"
            eligible = False
        elif matched.action in _INELIGIBLE_ACTIONS:
            disposition = _INELIGIBLE_ACTIONS[matched.action]
            eligible = False
        elif matched.action != "TRADING" or matched.is_trading != "YES" or matched.is_quoting != "YES":
            disposition = "STATUS_NOT_TRADING"
            eligible = False
        else:
            disposition = "STATUS_ELIGIBLE"
            eligible = True
        return StatusDecisionV1(
            disposition,
            True,
            eligible,
            eligible,
            eligible and matched.is_short_sell_restricted == "NO",
            True,
            matched.record_id,
            matched.ts_event_ns,
            matched.ts_recv_ns,
        )


@dataclass(frozen=True)
class StatisticsStateV1:
    state: str
    role: str
    feature_eligible: bool
    record_id: str | None
    price_nano: int | None
    quantity: int | None


class AsOfStatisticsLedger:
    """As-received NEW/DELETE statistics state with fail-closed unknowns."""

    def __init__(
        self, records: Iterable[StatisticsRecordV1], *, roles: StatisticsRolePolicy
    ) -> None:
        seen: set[str] = set()
        grouped: dict[
            tuple[str, int, int, str, str, int], list[StatisticsRecordV1]
        ] = {}
        for record in records:
            if type(record) is not StatisticsRecordV1 or record.record_id in seen:
                raise ContractError("statistics ledger contains a duplicate or invalid record")
            seen.add(record.record_id)
            key = (
                record.dataset,
                record.publisher_id,
                record.instrument_id,
                record.instrument_id_date_utc,
                record.stat_type,
                record.ts_ref_ns,
            )
            grouped.setdefault(key, []).append(record)
        self._records = {}
        self._receive_keys: dict[
            tuple[str, int, int, str, str, int], tuple[int, ...]
        ] = {}
        for key, values in grouped.items():
            ordered = _normalize_equal_receive_rows(values, kind="statistics")
            assert all(type(item) is StatisticsRecordV1 for item in ordered)
            self._records[key] = ordered
            self._receive_keys[key] = tuple(item.ts_recv_ns for item in ordered)
        self.roles = roles

    def as_of(
        self,
        *,
        dataset: str,
        publisher_id: int,
        instrument_id: int,
        instrument_id_date_utc: str,
        stat_type: str,
        ts_ref_ns: int,
        decision_at_ns: int,
    ) -> StatisticsStateV1:
        if type(decision_at_ns) is not int or decision_at_ns < 0:
            raise ContractError("statistics decision time must be nonnegative exact nanoseconds")
        key = (
            dataset,
            publisher_id,
            instrument_id,
            instrument_id_date_utc,
            stat_type,
            ts_ref_ns,
        )
        records = self._records.get(key, ())
        receive_keys = self._receive_keys.get(key, ())
        index = bisect.bisect_right(receive_keys, decision_at_ns) - 1
        role = self.roles.role_for(stat_type)
        if index < 0:
            return StatisticsStateV1("STATISTICS_UNRESOLVED", role, False, None, None, None)
        latest = records[index]
        if latest.update_action == "DELETE":
            return StatisticsStateV1("DELETED", role, False, latest.record_id, None, None)
        if latest.update_action != "NEW":
            return StatisticsStateV1(
                "UNKNOWN_UPDATE_ACTION", role, False, latest.record_id, None, None
            )
        if latest.price_nano == INT64_NULL or latest.quantity == INT64_NULL:
            return StatisticsStateV1(
                "NEW_UNKNOWN_VALUE", role, False, latest.record_id, None, None
            )
        state = "NEW_KNOWN" if role != "UNDECLARED_NO_FOUNDATION_ROLE" else "NEW_UNDECLARED_ROLE"
        return StatisticsStateV1(
            state, role, False, latest.record_id, latest.price_nano, latest.quantity
        )


def _statistics_ledger_event(
    record: StatisticsRecordV1, *, roles: StatisticsRolePolicy
) -> dict[str, object]:
    role = roles.role_for(record.stat_type)
    if record.update_action == "DELETE":
        state = "DELETED"
        price: int | None = None
        quantity: int | None = None
    elif record.update_action != "NEW":
        state = "UNKNOWN_UPDATE_ACTION"
        price = None
        quantity = None
    elif record.price_nano == INT64_NULL or record.quantity == INT64_NULL:
        state = "NEW_UNKNOWN_VALUE"
        price = None
        quantity = None
    elif role == "UNDECLARED_NO_FOUNDATION_ROLE":
        state = "NEW_UNDECLARED_ROLE"
        price = record.price_nano
        quantity = record.quantity
    else:
        state = "NEW_KNOWN"
        price = record.price_nano
        quantity = record.quantity
    core = {
        "feature_eligible": False,
        "price_nano_after": price,
        "quantity_after": quantity,
        "role": role,
        "source_record_id": record.record_id,
        "state_after": state,
        "statistics_key": {
            "dataset": record.dataset,
            "instrument_id": record.instrument_id,
            "instrument_id_date_utc": record.instrument_id_date_utc,
            "publisher_id": record.publisher_id,
            "stat_type": record.stat_type,
            "ts_ref_ns": record.ts_ref_ns,
        },
        "ts_event_ns": record.ts_event_ns,
        "ts_recv_ns": record.ts_recv_ns,
        "update_action": record.update_action,
    }
    return {**core, "statistics_ledger_event_id": sha256_json(core)}


@dataclass(frozen=True)
class LoadedMarketStateFoundation:
    receipt: VerifiedReleaseReceipt
    contract: Mapping[str, object]
    coverage_matrix: tuple[dict[str, object], ...]
    boundary: RepoBoundary
    release_paths: Mapping[str, Path]
    status_outputs: tuple[dict[str, object], ...]
    statistics_outputs: tuple[dict[str, object], ...]

    def iter_status_records(self, *, market: str, year: int) -> Iterator[StatusRecordV1]:
        for output in self.status_outputs:
            if output["market"] != market or int(output["year"]) != year:
                continue
            path = self.release_paths.get(str(output["output_path"]))
            if path is None:
                raise IntegrityError(
                    "status output is absent from the verified market-state manifest"
                )
            if sha256_file(path) != output.get("output_sha256"):
                raise IntegrityError(
                    "consumed status output differs from the checkpointed release"
                )
            with path.open("rb") as handle:
                for line in handle:
                    try:
                        payload = json.loads(line.decode("utf-8"))
                    except (UnicodeDecodeError, ValueError) as exc:
                        raise IntegrityError("status foundation JSONL is invalid") from exc
                    if line != canonical_bytes(payload) + b"\n":
                        raise IntegrityError("status foundation JSONL is not canonical")
                    try:
                        yield StatusRecordV1.from_dict(payload)
                    except ContractError as exc:
                        raise IntegrityError("status foundation row is invalid") from exc


def _output_path(prefix: str, item: SelectedFamilyFile) -> str:
    if prefix not in {"status", "statistics"}:
        raise ContractError("market-state output family is invalid")
    interval = f"{item.start}_{item.end}"
    return (
        f"data/market_state/{prefix}/{item.market}/{item.year}/{interval}/"
        f"{item.binding.sha256[:16]}.jsonl"
    )


def _atomic_canonical_document(path: Path, payload: Mapping[str, object]) -> None:
    """Write an exact restart document without accepting conflicting bytes."""

    encoded = canonical_bytes(dict(payload)) + b"\n"
    if path.exists():
        assert_plain_file(path)
        if path.read_bytes() != encoded:
            raise IntegrityError(f"restart document conflicts with existing bytes: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{uuid.uuid4().hex[:12]}.tmp"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        )
        remaining = memoryview(encoded)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("restart document write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        if path.exists():
            assert_plain_file(path)
            if path.read_bytes() != encoded:
                raise IntegrityError(
                    f"restart document appeared with conflicting bytes: {path}"
                )
        else:
            os.replace(temporary, path)
            fsync_directory(path.parent)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _market_state_output_paths(
    selection: ResolvedFoundationSelection,
) -> tuple[str, ...]:
    paths = [_output_path("status", item) for item in selection.status_files]
    for item in selection.statistics_files:
        output_path = _output_path("statistics", item)
        paths.extend(
            (output_path, output_path.removesuffix(".jsonl") + ".ledger.jsonl")
        )
    if len(paths) != len(set(paths)):
        raise IntegrityError("market-state selection resolves duplicate output paths")
    return tuple(sorted(paths))


def _market_state_resume_contract(
    *,
    selection: ResolvedFoundationSelection,
    source_selection_receipt: VerifiedReleaseReceipt,
    source_selection_manifest_id: str,
    policies: VerifiedFoundationPolicies,
    coverage_policy: FoundationCoveragePolicy,
    statistics_roles: StatisticsRolePolicy,
    batch_rows: int,
) -> dict[str, object]:
    source_bindings = [
        {"kind": "status", **item.as_coverage_binding()}
        for item in selection.status_files
    ] + [
        {"kind": "statistics", **item.as_coverage_binding()}
        for item in selection.statistics_files
    ]
    output_paths = list(_market_state_output_paths(selection))
    core = {
        "batch_rows": batch_rows,
        "coverage_matrix_id": selection.coverage_matrix_id,
        "coverage_policy_hash": coverage_policy.policy_hash,
        "expected_output_paths": output_paths,
        "expected_output_paths_sha256": sha256_json(output_paths),
        "foundation_policy_release_id": policies.receipt.release_id,
        "foundation_policy_release_receipt_id": policies.receipt.receipt_id,
        "foundation_policy_set_id": policies.policy_set_id,
        "query_manifest_id": selection.query_manifest_id,
        "resume_version": MARKET_STATE_RESUME_VERSION,
        "source_bindings_sha256": sha256_json(source_bindings),
        "source_selection_manifest_id": source_selection_manifest_id,
        "source_selection_receipt": source_selection_receipt.as_dict(),
        "statistics_role_policy_hash": statistics_roles.policy_hash,
    }
    return {**core, "resume_id": sha256_json(core)}


def _observed_stage_files(stage: Path) -> set[str]:
    if not stage.exists() or not stage.is_dir() or is_linklike(stage):
        raise IntegrityError("market-state restart stage is absent or link-like")
    observed: set[str] = set()
    for path in sorted(stage.rglob("*")):
        if is_linklike(path):
            raise IntegrityError("market-state restart stage contains a link-like path")
        if path.is_file():
            assert_plain_file(path)
            observed.add(path.relative_to(stage).as_posix())
    return observed


def _market_state_resume_stage(
    *,
    publisher: AtomicPublisher,
    contract: Mapping[str, object],
) -> tuple[Path, Path]:
    resume_id = contract.get("resume_id")
    expected_paths = contract.get("expected_output_paths")
    if (
        type(resume_id) is not str
        or not isinstance(expected_paths, list)
        or any(type(path) is not str for path in expected_paths)
    ):
        raise IntegrityError("market-state restart contract is invalid")
    run_root = publisher.boundary.assert_active_path(
        publisher.boundary.active_root
        / "state"
        / "msr"
        / resume_id[:32],
        purpose="market-state restart state",
        subtree="state/msr",
    )
    run_path = run_root / "run.json"
    if run_path.exists():
        payload = _canonical_object(run_path, description="market-state restart run")
        core = {key: value for key, value in payload.items() if key != "run_document_id"}
        if (
            set(payload) != {"contract", "run_document_id", "stage_name"}
            or payload.get("run_document_id") != sha256_json(core)
            or payload.get("contract") != dict(contract)
            or type(payload.get("stage_name")) is not str
            or re.fullmatch(
                r"market_state_foundation-[0-9a-f]{32}",
                str(payload["stage_name"]),
            )
            is None
        ):
            raise IntegrityError("market-state restart run binding is invalid")
        stage = publisher.staging_root / str(payload["stage_name"])
    else:
        publisher.staging_root.mkdir(parents=True, exist_ok=True)
        stages = sorted(
            path
            for path in publisher.staging_root.glob("market_state_foundation-*")
            if path.is_dir()
        )
        if len(stages) > 1:
            raise IntegrityError(
                "multiple market-state stages exist; ownership is ambiguous"
            )
        stage = stages[0] if stages else publisher.create_stage(
            "market_state_foundation"
        )
        observed = _observed_stage_files(stage)
        if "_publication_intent.json" in observed:
            raise IntegrityError(
                "prepared publication cannot be adopted as a decode restart stage"
            )
        if not observed.issubset(set(expected_paths)):
            raise IntegrityError("market-state stage contains unexpected output paths")
        run_core = {"contract": dict(contract), "stage_name": stage.name}
        _atomic_canonical_document(
            run_path,
            {**run_core, "run_document_id": sha256_json(run_core)},
        )
    observed = _observed_stage_files(stage)
    if "_publication_intent.json" in observed:
        raise IntegrityError(
            "market-state restart stage unexpectedly has a publication intent"
        )
    if not observed.issubset(set(expected_paths)):
        raise IntegrityError("market-state restart stage contains foreign files")
    return stage, run_root


def _market_state_source_key(kind: str, item: SelectedFamilyFile) -> str:
    output_path = _output_path(kind, item)
    output_paths = [output_path]
    if kind == "statistics":
        output_paths.append(
            output_path.removesuffix(".jsonl") + ".ledger.jsonl"
        )
    core = {
        "kind": kind,
        "output_paths": output_paths,
        "source_binding": item.as_coverage_binding(),
    }
    return sha256_json(core)


def _progress_path(
    run_root: Path, *, kind: str, item: SelectedFamilyFile
) -> Path:
    return (
        run_root
        / "progress"
        / kind
        / f"{_market_state_source_key(kind, item)}.json"
    )


def _validate_output_metadata(
    output: object,
    *,
    kind: str,
    item: SelectedFamilyFile,
) -> dict[str, object]:
    if not isinstance(output, dict):
        raise IntegrityError("market-state restart output metadata is invalid")
    expected = item.as_coverage_binding()
    for key, value in expected.items():
        if output.get(key) != value:
            raise IntegrityError(
                "market-state restart output differs from its source binding"
            )
    output_path = _output_path(kind, item)
    expected_keys = set(expected) | {
        "output_path",
        "output_sha256",
        "row_count",
        "timestamp_census",
        "timestamp_census_sha256",
    }
    if kind == "statistics":
        expected_keys |= {"ledger_output_path", "ledger_output_sha256"}
    if (
        set(output) != expected_keys
        or output.get("output_path") != output_path
        or type(output.get("output_sha256")) is not str
        or type(output.get("row_count")) is not int
        or int(output["row_count"]) <= 0
        or not isinstance(output.get("timestamp_census"), dict)
        or output.get("timestamp_census_sha256")
        != sha256_json(output["timestamp_census"])
    ):
        raise IntegrityError("market-state restart output metadata is not exact")
    if kind == "statistics" and (
        output.get("ledger_output_path")
        != output_path.removesuffix(".jsonl") + ".ledger.jsonl"
        or type(output.get("ledger_output_sha256")) is not str
    ):
        raise IntegrityError("market-state restart ledger metadata is not exact")
    return dict(output)


def _write_market_state_progress(
    *,
    run_root: Path,
    resume_id: str,
    kind: str,
    item: SelectedFamilyFile,
    output: Mapping[str, object],
    stage: Path,
) -> dict[str, object]:
    source_key = _market_state_source_key(kind, item)
    normalized = _validate_output_metadata(output, kind=kind, item=item)
    output_path = str(normalized["output_path"])
    target = stage / output_path
    assert_plain_file(target)
    sizes = {output_path: target.stat().st_size}
    if sha256_file(target) != normalized["output_sha256"]:
        raise IntegrityError("market-state restart output hash changed before checkpoint")
    if kind == "statistics":
        ledger_path = str(normalized["ledger_output_path"])
        ledger_target = stage / ledger_path
        assert_plain_file(ledger_target)
        sizes[ledger_path] = ledger_target.stat().st_size
        if sha256_file(ledger_target) != normalized["ledger_output_sha256"]:
            raise IntegrityError(
                "market-state restart ledger hash changed before checkpoint"
            )
    core = {
        "kind": kind,
        "output": normalized,
        "output_sizes": dict(sorted(sizes.items())),
        "progress_version": MARKET_STATE_RESUME_VERSION,
        "resume_id": resume_id,
        "source_binding": item.as_coverage_binding(),
        "source_key": source_key,
    }
    payload = {**core, "progress_id": sha256_json(core)}
    _atomic_canonical_document(
        _progress_path(run_root, kind=kind, item=item), payload
    )
    return normalized


def _load_market_state_progress(
    *,
    run_root: Path,
    resume_id: str,
    kind: str,
    item: SelectedFamilyFile,
    stage: Path,
) -> dict[str, object] | None:
    path = _progress_path(run_root, kind=kind, item=item)
    if not path.exists():
        return None
    payload = _canonical_object(path, description="market-state source progress")
    core = {key: value for key, value in payload.items() if key != "progress_id"}
    source_key = _market_state_source_key(kind, item)
    if (
        set(payload)
        != {
            "kind",
            "output",
            "output_sizes",
            "progress_id",
            "progress_version",
            "resume_id",
            "source_binding",
            "source_key",
        }
        or payload.get("progress_id") != sha256_json(core)
        or payload.get("progress_version") != MARKET_STATE_RESUME_VERSION
        or payload.get("resume_id") != resume_id
        or payload.get("kind") != kind
        or payload.get("source_key") != source_key
        or payload.get("source_binding") != item.as_coverage_binding()
        or not isinstance(payload.get("output_sizes"), dict)
    ):
        raise IntegrityError("market-state source progress binding is invalid")
    output = _validate_output_metadata(payload.get("output"), kind=kind, item=item)
    expected_sizes = payload["output_sizes"]
    output_path = str(output["output_path"])
    target = stage / output_path
    assert_plain_file(target)
    if (
        set(expected_sizes)
        != (
            {output_path}
            if kind == "status"
            else {output_path, str(output["ledger_output_path"])}
        )
        or target.stat().st_size != expected_sizes.get(output_path)
    ):
        raise IntegrityError("market-state checkpointed output bytes changed")
    if kind == "statistics":
        ledger_path = str(output["ledger_output_path"])
        ledger_target = stage / ledger_path
        assert_plain_file(ledger_target)
        if ledger_target.stat().st_size != expected_sizes.get(ledger_path):
            raise IntegrityError("market-state checkpointed ledger bytes changed")
    return output


def _existing_status_output(
    *,
    item: SelectedFamilyFile,
    target: Path,
    batch_rows: int,
) -> dict[str, object]:
    count = 0
    timestamp_census = _ProviderTimestampCensus()
    try:
        with target.open("rb") as handle:
            for record, line in zip(
                iter_statuses(
                    item.binding,
                    market=item.market,
                    expected_query_contract=item.query_contract,
                    batch_rows=batch_rows,
                ),
                handle,
                strict=True,
            ):
                expected = canonical_bytes(record.as_dict()) + b"\n"
                if line != expected:
                    raise IntegrityError(
                        "existing status restart output differs from its DBN source"
                    )
                timestamp_census.observe(record)
                count += 1
    except (OSError, ValueError) as exc:
        raise IntegrityError("existing status restart output is incomplete") from exc
    if count == 0:
        raise IntegrityError("existing status restart output is empty")
    census = timestamp_census.as_dict()
    return {
        **item.as_coverage_binding(),
        "output_path": _output_path("status", item),
        "output_sha256": sha256_file(target),
        "row_count": count,
        "timestamp_census": census,
        "timestamp_census_sha256": sha256_json(census),
    }


def _existing_statistics_output(
    *,
    item: SelectedFamilyFile,
    target: Path,
    ledger_target: Path,
    statistics_roles: StatisticsRolePolicy,
    batch_rows: int,
) -> dict[str, object]:
    count = 0
    timestamp_census = _ProviderTimestampCensus()
    try:
        with target.open("rb") as handle, ledger_target.open("rb") as ledger_handle:
            for record, line, ledger_line in zip(
                iter_statistics(
                    item.binding,
                    market=item.market,
                    expected_query_contract=item.query_contract,
                    batch_rows=batch_rows,
                ),
                handle,
                ledger_handle,
                strict=True,
            ):
                if line != canonical_bytes(record.as_dict()) + b"\n":
                    raise IntegrityError(
                        "existing statistics restart output differs from its DBN source"
                    )
                expected_ledger = canonical_bytes(
                    _statistics_ledger_event(record, roles=statistics_roles)
                ) + b"\n"
                if ledger_line != expected_ledger:
                    raise IntegrityError(
                        "existing statistics restart ledger differs from its source"
                    )
                timestamp_census.observe(record)
                count += 1
    except ValueError as exc:
        if str(exc).startswith("zip() argument"):
            raise _InterruptedStatisticsPair(
                "existing statistics restart pair is an exact incomplete prefix"
            ) from exc
        raise IntegrityError("existing statistics restart output is invalid") from exc
    except OSError as exc:
        raise IntegrityError("existing statistics restart output is incomplete") from exc
    if count == 0:
        raise IntegrityError("existing statistics restart output is empty")
    census = timestamp_census.as_dict()
    output_path = _output_path("statistics", item)
    return {
        **item.as_coverage_binding(),
        "ledger_output_path": output_path.removesuffix(".jsonl") + ".ledger.jsonl",
        "ledger_output_sha256": sha256_file(ledger_target),
        "output_path": output_path,
        "output_sha256": sha256_file(target),
        "row_count": count,
        "timestamp_census": census,
        "timestamp_census_sha256": sha256_json(census),
    }


def _statistics_quarantine_path(
    run_root: Path, *, item: SelectedFamilyFile
) -> Path:
    source_key = _market_state_source_key("statistics", item)
    return run_root / "q" / source_key[:16] / "quarantine.json"


def _recover_statistics_quarantine(
    *,
    path: Path,
    run_root: Path,
    resume_id: str,
    item: SelectedFamilyFile,
    stage: Path,
) -> None:
    payload = _canonical_object(path, description="statistics restart quarantine")
    core = {key: value for key, value in payload.items() if key != "quarantine_id"}
    source_key = _market_state_source_key("statistics", item)
    output_path = _output_path("statistics", item)
    ledger_output_path = output_path.removesuffix(".jsonl") + ".ledger.jsonl"
    expected_paths = {output_path, ledger_output_path}
    files = payload.get("files")
    if (
        set(payload)
        != {
            "files",
            "kind",
            "quarantine_id",
            "quarantine_version",
            "reason",
            "resume_id",
            "source_binding",
            "source_key",
        }
        or payload.get("quarantine_id") != sha256_json(core)
        or payload.get("quarantine_version") != MARKET_STATE_RESUME_VERSION
        or payload.get("resume_id") != resume_id
        or payload.get("kind") != "statistics"
        or payload.get("reason") != "INTERRUPTED_EXACT_PREFIX"
        or payload.get("source_binding") != item.as_coverage_binding()
        or payload.get("source_key") != source_key
        or not isinstance(files, dict)
        or set(files) != expected_paths
    ):
        raise IntegrityError("statistics restart quarantine binding is invalid")
    quarantine_root = path.parent
    for staged_path in sorted(expected_paths):
        metadata = files.get(staged_path)
        if (
            not isinstance(metadata, dict)
            or set(metadata) != {"name", "sha256", "size"}
            or type(metadata.get("name")) is not str
            or Path(str(metadata["name"])).name != metadata["name"]
            or type(metadata.get("sha256")) is not str
            or type(metadata.get("size")) is not int
            or int(metadata["size"]) < 0
        ):
            raise IntegrityError("statistics restart quarantine file binding is invalid")
        target = stage / staged_path
        destination = quarantine_root / str(metadata["name"])
        if destination.exists():
            assert_plain_file(destination)
            if (
                target.exists()
                or destination.stat().st_size != metadata["size"]
                or sha256_file(destination) != metadata["sha256"]
            ):
                raise IntegrityError("statistics restart quarantine bytes changed")
            continue
        assert_plain_file(target)
        if (
            target.stat().st_size != metadata["size"]
            or sha256_file(target) != metadata["sha256"]
        ):
            raise IntegrityError("statistics interrupted bytes changed before quarantine")
        os.replace(target, destination)
        fsync_directory(target.parent)
        fsync_directory(quarantine_root)


def _quarantine_interrupted_statistics_pair(
    *,
    run_root: Path,
    resume_id: str,
    item: SelectedFamilyFile,
    stage: Path,
) -> None:
    path = _statistics_quarantine_path(run_root, item=item)
    if path.exists():
        _recover_statistics_quarantine(
            path=path,
            run_root=run_root,
            resume_id=resume_id,
            item=item,
            stage=stage,
        )
        return
    output_path = _output_path("statistics", item)
    ledger_output_path = output_path.removesuffix(".jsonl") + ".ledger.jsonl"
    target = stage / output_path
    ledger_target = stage / ledger_output_path
    assert_plain_file(target)
    assert_plain_file(ledger_target)
    files = {
        output_path: {
            "name": "raw.interrupted.jsonl",
            "sha256": sha256_file(target),
            "size": target.stat().st_size,
        },
        ledger_output_path: {
            "name": "ledger.interrupted.jsonl",
            "sha256": sha256_file(ledger_target),
            "size": ledger_target.stat().st_size,
        },
    }
    core = {
        "files": files,
        "kind": "statistics",
        "quarantine_version": MARKET_STATE_RESUME_VERSION,
        "reason": "INTERRUPTED_EXACT_PREFIX",
        "resume_id": resume_id,
        "source_binding": item.as_coverage_binding(),
        "source_key": _market_state_source_key("statistics", item),
    }
    _atomic_canonical_document(
        path,
        {**core, "quarantine_id": sha256_json(core)},
    )
    _recover_statistics_quarantine(
        path=path,
        run_root=run_root,
        resume_id=resume_id,
        item=item,
        stage=stage,
    )


def _new_attempt_directory(
    run_root: Path, *, kind: str, item: SelectedFamilyFile
) -> Path:
    source_key = _market_state_source_key(kind, item)
    attempts_root = (
        run_root / "attempts" / kind[0] / source_key[:16]
    )
    attempts_root.mkdir(parents=True, exist_ok=True)
    attempts = sorted(path for path in attempts_root.iterdir() if path.is_dir())
    if any(is_linklike(path) for path in attempts):
        raise IntegrityError("market-state source attempt contains a link-like path")
    if len(attempts) >= MARKET_STATE_ATTEMPT_CAP:
        raise IntegrityError(
            "market-state source attempt retention cap reached; review is required"
        )
    attempt = attempts_root / uuid.uuid4().hex[:16]
    attempt.mkdir()
    return attempt


def _prepared_attempts(
    run_root: Path, *, kind: str, item: SelectedFamilyFile
) -> list[Path]:
    source_key = _market_state_source_key(kind, item)
    attempts_root = (
        run_root / "attempts" / kind[0] / source_key[:16]
    )
    if not attempts_root.exists():
        return []
    return sorted(attempts_root.glob("*/prepared.json"))


def _load_prepared_attempt(
    path: Path,
    *,
    resume_id: str,
    kind: str,
    item: SelectedFamilyFile,
) -> tuple[dict[str, object], dict[str, str], Path]:
    payload = _canonical_object(path, description="market-state prepared attempt")
    core = {key: value for key, value in payload.items() if key != "prepared_id"}
    source_key = _market_state_source_key(kind, item)
    if (
        set(payload)
        != {
            "kind",
            "output",
            "prepared_id",
            "prepared_version",
            "resume_id",
            "source_binding",
            "source_key",
            "temporary_files",
        }
        or payload.get("prepared_id") != sha256_json(core)
        or payload.get("prepared_version") != MARKET_STATE_RESUME_VERSION
        or payload.get("resume_id") != resume_id
        or payload.get("kind") != kind
        or payload.get("source_binding") != item.as_coverage_binding()
        or payload.get("source_key") != source_key
        or not isinstance(payload.get("temporary_files"), dict)
        or any(
            type(key) is not str or type(value) is not str
            for key, value in payload["temporary_files"].items()
        )
    ):
        raise IntegrityError("market-state prepared attempt binding is invalid")
    output = _validate_output_metadata(payload.get("output"), kind=kind, item=item)
    expected_paths = {str(output["output_path"])}
    if kind == "statistics":
        expected_paths.add(str(output["ledger_output_path"]))
    temporary_files = dict(payload["temporary_files"])
    if set(temporary_files) != expected_paths:
        raise IntegrityError("market-state prepared attempt file set is invalid")
    attempt_root = path.parent
    for output_path, name in temporary_files.items():
        if Path(name).name != name or name in {"", ".", ".."}:
            raise IntegrityError("market-state prepared temporary filename is invalid")
        temporary = attempt_root / name
        if temporary.exists():
            assert_plain_file(temporary)
            expected_hash = (
                output["output_sha256"]
                if output_path == output["output_path"]
                else output["ledger_output_sha256"]
            )
            if sha256_file(temporary) != expected_hash:
                raise IntegrityError("market-state prepared temporary bytes changed")
    return output, temporary_files, attempt_root


def _promote_prepared_attempt(
    *,
    output: Mapping[str, object],
    temporary_files: Mapping[str, str],
    attempt_root: Path,
    stage: Path,
) -> None:
    expected_hashes = {str(output["output_path"]): str(output["output_sha256"])}
    if "ledger_output_path" in output:
        expected_hashes[str(output["ledger_output_path"])] = str(
            output["ledger_output_sha256"]
        )
    for output_path in sorted(expected_hashes):
        target = stage / output_path
        temporary = attempt_root / temporary_files[output_path]
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            assert_plain_file(target)
            if sha256_file(target) != expected_hashes[output_path]:
                raise IntegrityError(
                    "market-state prepared promotion conflicts with staged bytes"
                )
            continue
        assert_plain_file(temporary)
        if sha256_file(temporary) != expected_hashes[output_path]:
            raise IntegrityError("market-state prepared source changed before promotion")
        os.replace(temporary, target)
        fsync_directory(target.parent)


def _materialize_prepared_status_attempt(
    *,
    run_root: Path,
    resume_id: str,
    item: SelectedFamilyFile,
    batch_rows: int,
) -> tuple[dict[str, object], dict[str, str], Path]:
    attempt = _new_attempt_directory(run_root, kind="status", item=item)
    temporary = attempt / "output.jsonl.tmp"
    count = 0
    timestamp_census = _ProviderTimestampCensus()
    with temporary.open("xb") as handle:
        for record in iter_statuses(
            item.binding,
            market=item.market,
            expected_query_contract=item.query_contract,
            batch_rows=batch_rows,
        ):
            handle.write(canonical_bytes(record.as_dict()) + b"\n")
            timestamp_census.observe(record)
            count += 1
        handle.flush()
        os.fsync(handle.fileno())
    if count == 0:
        raise IntegrityError("selected status DBN decoded to zero rows")
    census = timestamp_census.as_dict()
    output_path = _output_path("status", item)
    output = {
        **item.as_coverage_binding(),
        "output_path": output_path,
        "output_sha256": sha256_file(temporary),
        "row_count": count,
        "timestamp_census": census,
        "timestamp_census_sha256": sha256_json(census),
    }
    temporary_files = {output_path: temporary.name}
    core = {
        "kind": "status",
        "output": output,
        "prepared_version": MARKET_STATE_RESUME_VERSION,
        "resume_id": resume_id,
        "source_binding": item.as_coverage_binding(),
        "source_key": _market_state_source_key("status", item),
        "temporary_files": temporary_files,
    }
    _atomic_canonical_document(
        attempt / "prepared.json",
        {**core, "prepared_id": sha256_json(core)},
    )
    return output, temporary_files, attempt


def _materialize_prepared_statistics_attempt(
    *,
    run_root: Path,
    resume_id: str,
    item: SelectedFamilyFile,
    statistics_roles: StatisticsRolePolicy,
    batch_rows: int,
) -> tuple[dict[str, object], dict[str, str], Path]:
    attempt = _new_attempt_directory(run_root, kind="statistics", item=item)
    temporary = attempt / "output.jsonl.tmp"
    ledger_temporary = attempt / "ledger.jsonl.tmp"
    count = 0
    timestamp_census = _ProviderTimestampCensus()
    with temporary.open("xb") as handle, ledger_temporary.open(
        "xb"
    ) as ledger_handle:
        for record in iter_statistics(
            item.binding,
            market=item.market,
            expected_query_contract=item.query_contract,
            batch_rows=batch_rows,
        ):
            handle.write(canonical_bytes(record.as_dict()) + b"\n")
            ledger_handle.write(
                canonical_bytes(
                    _statistics_ledger_event(record, roles=statistics_roles)
                )
                + b"\n"
            )
            timestamp_census.observe(record)
            count += 1
        handle.flush()
        os.fsync(handle.fileno())
        ledger_handle.flush()
        os.fsync(ledger_handle.fileno())
    if count == 0:
        raise IntegrityError("selected statistics DBN decoded to zero rows")
    census = timestamp_census.as_dict()
    output_path = _output_path("statistics", item)
    ledger_output_path = output_path.removesuffix(".jsonl") + ".ledger.jsonl"
    output = {
        **item.as_coverage_binding(),
        "ledger_output_path": ledger_output_path,
        "ledger_output_sha256": sha256_file(ledger_temporary),
        "output_path": output_path,
        "output_sha256": sha256_file(temporary),
        "row_count": count,
        "timestamp_census": census,
        "timestamp_census_sha256": sha256_json(census),
    }
    temporary_files = {
        output_path: temporary.name,
        ledger_output_path: ledger_temporary.name,
    }
    core = {
        "kind": "statistics",
        "output": output,
        "prepared_version": MARKET_STATE_RESUME_VERSION,
        "resume_id": resume_id,
        "source_binding": item.as_coverage_binding(),
        "source_key": _market_state_source_key("statistics", item),
        "temporary_files": temporary_files,
    }
    _atomic_canonical_document(
        attempt / "prepared.json",
        {**core, "prepared_id": sha256_json(core)},
    )
    return output, temporary_files, attempt


def _resume_status_source(
    *,
    item: SelectedFamilyFile,
    stage: Path,
    run_root: Path,
    resume_id: str,
    batch_rows: int,
) -> dict[str, object]:
    progress = _load_market_state_progress(
        run_root=run_root,
        resume_id=resume_id,
        kind="status",
        item=item,
        stage=stage,
    )
    if progress is not None:
        return progress
    target = stage / _output_path("status", item)
    if target.exists():
        output = _existing_status_output(
            item=item, target=target, batch_rows=batch_rows
        )
    else:
        prepared = _prepared_attempts(run_root, kind="status", item=item)
        if len(prepared) > 1:
            raise IntegrityError("multiple prepared status attempts are ambiguous")
        if prepared:
            output, temporary_files, attempt_root = _load_prepared_attempt(
                prepared[0],
                resume_id=resume_id,
                kind="status",
                item=item,
            )
        else:
            output, temporary_files, attempt_root = (
                _materialize_prepared_status_attempt(
                    run_root=run_root,
                    resume_id=resume_id,
                    item=item,
                    batch_rows=batch_rows,
                )
            )
        _promote_prepared_attempt(
            output=output,
            temporary_files=temporary_files,
            attempt_root=attempt_root,
            stage=stage,
        )
    return _write_market_state_progress(
        run_root=run_root,
        resume_id=resume_id,
        kind="status",
        item=item,
        output=output,
        stage=stage,
    )


def _resume_statistics_source(
    *,
    item: SelectedFamilyFile,
    stage: Path,
    run_root: Path,
    resume_id: str,
    statistics_roles: StatisticsRolePolicy,
    batch_rows: int,
) -> dict[str, object]:
    progress = _load_market_state_progress(
        run_root=run_root,
        resume_id=resume_id,
        kind="statistics",
        item=item,
        stage=stage,
    )
    if progress is not None:
        return progress
    output_path = _output_path("statistics", item)
    target = stage / output_path
    ledger_target = stage / (
        output_path.removesuffix(".jsonl") + ".ledger.jsonl"
    )
    quarantine_path = _statistics_quarantine_path(run_root, item=item)
    if quarantine_path.exists():
        _recover_statistics_quarantine(
            path=quarantine_path,
            run_root=run_root,
            resume_id=resume_id,
            item=item,
            stage=stage,
        )
    if target.exists() and ledger_target.exists():
        try:
            output = _existing_statistics_output(
                item=item,
                target=target,
                ledger_target=ledger_target,
                statistics_roles=statistics_roles,
                batch_rows=batch_rows,
            )
        except _InterruptedStatisticsPair:
            _quarantine_interrupted_statistics_pair(
                run_root=run_root,
                resume_id=resume_id,
                item=item,
                stage=stage,
            )
            output, temporary_files, attempt_root = (
                _materialize_prepared_statistics_attempt(
                    run_root=run_root,
                    resume_id=resume_id,
                    item=item,
                    statistics_roles=statistics_roles,
                    batch_rows=batch_rows,
                )
            )
            _promote_prepared_attempt(
                output=output,
                temporary_files=temporary_files,
                attempt_root=attempt_root,
                stage=stage,
            )
    else:
        prepared = _prepared_attempts(run_root, kind="statistics", item=item)
        if len(prepared) > 1:
            raise IntegrityError("multiple prepared statistics attempts are ambiguous")
        if target.exists() != ledger_target.exists() and not prepared:
            raise IntegrityError(
                "statistics restart output pair is incomplete without a prepared attempt"
            )
        if prepared:
            output, temporary_files, attempt_root = _load_prepared_attempt(
                prepared[0],
                resume_id=resume_id,
                kind="statistics",
                item=item,
            )
        else:
            output, temporary_files, attempt_root = (
                _materialize_prepared_statistics_attempt(
                    run_root=run_root,
                    resume_id=resume_id,
                    item=item,
                    statistics_roles=statistics_roles,
                    batch_rows=batch_rows,
                )
            )
        _promote_prepared_attempt(
            output=output,
            temporary_files=temporary_files,
            attempt_root=attempt_root,
            stage=stage,
        )
    return _write_market_state_progress(
        run_root=run_root,
        resume_id=resume_id,
        kind="statistics",
        item=item,
        output=output,
        stage=stage,
    )


def _source_coverage(
    selection: ResolvedFoundationSelection,
) -> tuple[int, int, int, Decimal, Decimal]:
    required = {
        (str(row["market"]), int(row["year"]))
        for row in selection.coverage_matrix
        if row["required_for_bar_foundation"] is True
    }
    status = {
        (item.market, item.year) for item in selection.status_files
    } & required
    statistics = {
        (item.market, item.year) for item in selection.statistics_files
    } & required
    denominator = len(required)
    if denominator == 0:
        raise IntegrityError("foundation selection has no required market/year coverage")
    return (
        denominator,
        len(status),
        len(statistics),
        Decimal(len(status)) / Decimal(denominator),
        Decimal(len(statistics)) / Decimal(denominator),
    )


def _assert_manifest_matches_market_state_outputs(
    manifest: ReleaseManifest,
    *,
    status_outputs: Sequence[Mapping[str, object]],
    statistics_outputs: Sequence[Mapping[str, object]],
) -> None:
    """Bind resumed checkpoint hashes to the fresh central manifest pre-publish."""

    entries = {entry.logical_path: entry.sha256 for entry in manifest.files}
    expected: dict[str, str] = {}
    for output in status_outputs:
        expected[str(output["output_path"])] = str(output["output_sha256"])
    for output in statistics_outputs:
        expected[str(output["output_path"])] = str(output["output_sha256"])
        expected[str(output["ledger_output_path"])] = str(
            output["ledger_output_sha256"]
        )
    if (
        len(expected)
        != len(status_outputs) + (2 * len(statistics_outputs))
        or set(entries) != set(expected)
        or any(entries[path] != digest for path, digest in expected.items())
    ):
        raise IntegrityError(
            "market-state checkpoint hashes differ from the fresh stage manifest"
        )


def _recover_published_market_state_receipt(
    *,
    selection: ResolvedFoundationSelection,
    source_selection_receipt: VerifiedReleaseReceipt,
    source_selection_manifest_id: str,
    policies: VerifiedFoundationPolicies,
    coverage_policy: FoundationCoveragePolicy,
    statistics_roles: StatisticsRolePolicy,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt | None:
    """Adopt an exact commit-last release after a kill before its checkpoint."""

    if not selection.status_files:
        raise IntegrityError("market-state selection lacks a status source")
    source_release_id = selection.status_files[0].binding.source_release_id
    expected_source_release_ids = tuple(
        sorted(
            (
                source_selection_receipt.release_id,
                source_release_id,
                policies.receipt.release_id,
            )
        )
    )
    manifest_root = publisher.boundary.active_root / MANIFEST_ROOT / "market_state"
    if not manifest_root.exists():
        return None
    candidates: list[VerifiedReleaseReceipt] = []
    for path in sorted(manifest_root.glob("*.json")):
        manifest = verify_data_release_manifest(
            path, publisher.boundary, verify_files=False
        )
        if (
            manifest.release_kind != MARKET_STATE_RELEASE_KIND
            or manifest.schema_version != MARKET_STATE_SCHEMA_VERSION
            or manifest.source_release_ids != expected_source_release_ids
            or manifest.metadata.get("coverage_matrix_id")
            != selection.coverage_matrix_id
            or manifest.metadata.get("foundation_policy_set_id")
            != policies.policy_set_id
            or manifest.metadata.get("query_manifest_id")
            != selection.query_manifest_id
            or manifest.metadata.get("source_selection_manifest_id")
            != source_selection_manifest_id
        ):
            continue
        contract = manifest.embedded_documents.get("market_state_contract.json")
        if not isinstance(contract, dict):
            raise IntegrityError("published market-state contract is absent")
        if (
            contract.get("coverage_policy_hash") != coverage_policy.policy_hash
            or contract.get("statistics_role_policy_hash")
            != statistics_roles.policy_hash
            or contract.get("source_selection_receipt_id")
            != source_selection_receipt.receipt_id
            or contract.get("foundation_policy_release_receipt_id")
            != policies.receipt.receipt_id
        ):
            continue
        candidates.append(
            VerifiedReleaseReceipt.from_manifest(
                path, publisher.boundary, verify_files=False
            )
        )
    if len(candidates) > 1:
        raise IntegrityError("multiple exact published market-state releases exist")
    return candidates[0] if candidates else None


def publish_market_state_foundation(
    *,
    selection: ResolvedFoundationSelection,
    source_selection_receipt: VerifiedReleaseReceipt,
    policies: VerifiedFoundationPolicies,
    coverage_policy: FoundationCoveragePolicy,
    statistics_roles: StatisticsRolePolicy,
    publisher: AtomicPublisher,
    batch_rows: int = 100_000,
) -> VerifiedReleaseReceipt:
    """Decode every selected status/statistics file into one immutable release."""

    if type(batch_rows) is not int or batch_rows <= 0:
        raise ContractError("market-state decode batch size must be positive")
    if policies.boundary.repository_id != publisher.boundary.repository_id:
        raise IntegrityError("market-state policies belong to another repository")
    policies.verify()
    source_selection_manifest = source_selection_receipt.verify(publisher.boundary)
    source_selection_manifest_id = source_selection_manifest.metadata.get(
        "selection_manifest_id"
    )
    if (
        type(source_selection_manifest_id) is not str
        or source_selection_manifest.metadata.get("query_manifest_id")
        != selection.query_manifest_id
    ):
        raise IntegrityError("market-state selection receipt is not the resolved selection")
    denominator, status_present, statistics_present, status_fraction, statistics_fraction = (
        _source_coverage(selection)
    )
    if (
        status_fraction < coverage_policy.minimum_status_source_market_year_fraction
        or statistics_fraction
        < coverage_policy.minimum_statistics_source_market_year_fraction
    ):
        raise IntegrityError("canonical market-state source coverage is below its pinned gate")
    published = _recover_published_market_state_receipt(
        selection=selection,
        source_selection_receipt=source_selection_receipt,
        source_selection_manifest_id=source_selection_manifest_id,
        policies=policies,
        coverage_policy=coverage_policy,
        statistics_roles=statistics_roles,
        publisher=publisher,
    )
    if published is not None:
        return published
    resume_contract = _market_state_resume_contract(
        selection=selection,
        source_selection_receipt=source_selection_receipt,
        source_selection_manifest_id=source_selection_manifest_id,
        policies=policies,
        coverage_policy=coverage_policy,
        statistics_roles=statistics_roles,
        batch_rows=batch_rows,
    )
    stage, run_root = _market_state_resume_stage(
        publisher=publisher,
        contract=resume_contract,
    )
    resume_id = str(resume_contract["resume_id"])
    status_outputs: list[dict[str, object]] = []
    statistics_outputs: list[dict[str, object]] = []
    total_status_rows = 0
    total_statistics_rows = 0
    for item in selection.status_files:
        output = _resume_status_source(
            item=item,
            stage=stage,
            run_root=run_root,
            resume_id=resume_id,
            batch_rows=batch_rows,
        )
        total_status_rows += int(output["row_count"])
        status_outputs.append(output)
    for item in selection.statistics_files:
        output = _resume_statistics_source(
            item=item,
            stage=stage,
            run_root=run_root,
            resume_id=resume_id,
            statistics_roles=statistics_roles,
            batch_rows=batch_rows,
        )
        total_statistics_rows += int(output["row_count"])
        statistics_outputs.append(output)
    if total_status_rows == 0 or total_statistics_rows == 0:
        raise IntegrityError("canonical status/statistics families must be nonempty")
    coverage = list(selection.coverage_matrix)
    status_timestamp_census = _aggregate_timestamp_censuses(
        [item["timestamp_census"] for item in status_outputs]  # type: ignore[list-item]
    )
    statistics_timestamp_census = _aggregate_timestamp_censuses(
        [item["timestamp_census"] for item in statistics_outputs]  # type: ignore[list-item]
    )
    if any(
        census["undefined_timestamp_rows"] != 0
        or census["receive_order_violation_rows"] != 0
        for census in (status_timestamp_census, statistics_timestamp_census)
    ):
        raise IntegrityError("market-state provider timestamp stream is invalid")
    contract_core = {
        "coverage_matrix_id": selection.coverage_matrix_id,
        "coverage_policy": coverage_policy.as_dict(),
        "coverage_policy_hash": coverage_policy.policy_hash,
        "feature_eligible_statistic_types": [],
        "foundation_policy_hash": policies.foundation.policy_hash,
        "foundation_policy_release_id": policies.receipt.release_id,
        "foundation_policy_release_receipt_id": policies.receipt.receipt_id,
        "foundation_policy_set_id": policies.policy_set_id,
        "provider_data_epochs_sha256": policies.foundation.provider_data_epochs_sha256,
        "required_market_year_count": denominator,
        "query_manifest_id": selection.query_manifest_id,
        "query_mode_census": list(selection.query_mode_census),
        "schema_version": MARKET_STATE_SCHEMA_VERSION,
        "selected_file_count": selection.selected_file_count,
        "source_selection_receipt_id": source_selection_receipt.receipt_id,
        "source_selection_manifest_id": source_selection_manifest_id,
        "statistics_outputs": statistics_outputs,
        "statistics_role_policy": statistics_roles.as_dict(),
        "statistics_role_policy_hash": statistics_roles.policy_hash,
        "statistics_rows": total_statistics_rows,
        "statistics_timestamp_census": statistics_timestamp_census,
        "statistics_timestamp_census_sha256": sha256_json(
            statistics_timestamp_census
        ),
        "statistics_ledger_rows": total_statistics_rows,
        "statistics_source_market_year_count": statistics_present,
        "statistics_source_market_year_fraction": str(statistics_fraction),
        "status_outputs": status_outputs,
        "status_rows": total_status_rows,
        "status_timestamp_census": status_timestamp_census,
        "status_timestamp_census_sha256": sha256_json(status_timestamp_census),
        "status_source_market_year_count": status_present,
        "status_source_market_year_fraction": str(status_fraction),
    }
    contract = {**contract_core, "market_state_foundation_id": sha256_json(contract_core)}
    manifest = ReleaseManifest.build(
        stage,
        phase="market_state",
        release_kind=MARKET_STATE_RELEASE_KIND,
        schema_version=MARKET_STATE_SCHEMA_VERSION,
        logical_paths={
            path.relative_to(stage).as_posix(): path.relative_to(stage).as_posix()
            for path in stage.rglob("*")
            if path.is_file()
        },
        source_release_ids=(
            source_selection_receipt.release_id,
            selection.status_files[0].binding.source_release_id,
            policies.receipt.release_id,
        ),
        embedded_documents={
            "coverage_matrix.json": coverage,
            "market_state_contract.json": contract,
        },
        metadata={
            "coverage_matrix_id": selection.coverage_matrix_id,
            "foundation_policy_set_id": policies.policy_set_id,
            "market_state_foundation_id": contract["market_state_foundation_id"],
            "query_manifest_id": selection.query_manifest_id,
            "source_selection_manifest_id": source_selection_manifest_id,
            "statistics_ledger_rows": total_statistics_rows,
            "statistics_rows": total_statistics_rows,
            "statistics_timestamp_census_sha256": sha256_json(
                statistics_timestamp_census
            ),
            "status_rows": total_status_rows,
            "status_timestamp_census_sha256": sha256_json(
                status_timestamp_census
            ),
        },
    )
    _assert_manifest_matches_market_state_outputs(
        manifest,
        status_outputs=status_outputs,
        statistics_outputs=statistics_outputs,
    )
    manifest_path = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, publisher.boundary)
    return receipt


def _validate_output_rows(
    release_paths: Mapping[str, Path],
    outputs: Sequence[Mapping[str, object]],
    *,
    kind: str,
    expected_source_release_id: str,
) -> tuple[int, dict[str, object]]:
    total = 0
    censuses: list[dict[str, object]] = []
    seen_paths: set[str] = set()
    for output in outputs:
        output_path = output.get("output_path")
        if type(output_path) is not str or output_path in seen_paths:
            raise IntegrityError(f"{kind} output path is invalid or duplicated")
        seen_paths.add(output_path)
        path = release_paths.get(output_path)
        if path is None:
            raise IntegrityError(f"{kind} output is absent from the verified manifest")
        if sha256_file(path) != output.get("output_sha256"):
            raise IntegrityError(f"{kind} output hash differs from its contract")
        count = 0
        timestamp_census = _ProviderTimestampCensus()
        try:
            with path.open("rb") as handle:
                for line in handle:
                    payload = json.loads(line.decode("utf-8"))
                    if line != canonical_bytes(payload) + b"\n":
                        raise IntegrityError(f"{kind} output is not canonical JSONL")
                    if kind == "status":
                        record = StatusRecordV1.from_dict(payload)
                    else:
                        record = StatisticsRecordV1.from_dict(payload)
                    if (
                        record.market != output.get("market")
                        or record.source_file_path != output.get("path")
                        or record.source_file_sha256 != output.get("sha256")
                        or record.source_release_id != expected_source_release_id
                    ):
                        raise IntegrityError(
                            f"{kind} output row differs from its exact source binding"
                        )
                    timestamp_census.observe(record)
                    count += 1
        except (OSError, UnicodeDecodeError, ValueError, ContractError) as exc:
            raise IntegrityError(f"{kind} output row is invalid") from exc
        observed_census = timestamp_census.as_dict()
        if (
            count <= 0
            or count != output.get("row_count")
            or observed_census != output.get("timestamp_census")
            or sha256_json(observed_census)
            != output.get("timestamp_census_sha256")
        ):
            raise IntegrityError(f"{kind} output row count differs from its contract")
        censuses.append(observed_census)
        total += count
    return total, _aggregate_timestamp_censuses(censuses)


def _validate_statistics_ledger_outputs(
    release_paths: Mapping[str, Path],
    outputs: Sequence[Mapping[str, object]],
    *,
    roles: StatisticsRolePolicy,
) -> int:
    total = 0
    for output in outputs:
        raw_path = release_paths.get(str(output["output_path"]))
        ledger_path = release_paths.get(str(output.get("ledger_output_path")))
        if raw_path is None or ledger_path is None:
            raise IntegrityError(
                "statistics raw or ledger output is absent from the verified manifest"
            )
        if sha256_file(ledger_path) != output.get("ledger_output_sha256"):
            raise IntegrityError("statistics ledger output hash differs from its contract")
        count = 0
        try:
            with raw_path.open("rb") as raw_handle, ledger_path.open("rb") as ledger_handle:
                for raw_line, ledger_line in zip(
                    raw_handle, ledger_handle, strict=True
                ):
                    record_payload = json.loads(raw_line.decode("utf-8"))
                    ledger_payload = json.loads(ledger_line.decode("utf-8"))
                    record = StatisticsRecordV1.from_dict(record_payload)
                    expected = _statistics_ledger_event(record, roles=roles)
                    if (
                        ledger_line != canonical_bytes(ledger_payload) + b"\n"
                        or ledger_payload != expected
                    ):
                        raise IntegrityError(
                            "statistics ledger differs from its exact NEW/DELETE stream"
                        )
                    count += 1
        except (OSError, UnicodeDecodeError, ValueError, ContractError) as exc:
            raise IntegrityError("statistics ledger output is invalid") from exc
        if count != output.get("row_count"):
            raise IntegrityError("statistics ledger row count differs from source records")
        total += count
    return total


def _checkpoint_verified_market_state_manifest(
    receipt: VerifiedReleaseReceipt,
    *,
    boundary: RepoBoundary,
    checkpoint_mtime_ns: int,
) -> ReleaseManifest:
    """Reuse one completed full verification while rejecting later file changes."""

    if type(checkpoint_mtime_ns) is not int or checkpoint_mtime_ns <= 0:
        raise ContractError("market-state checkpoint mtime must be a positive integer")
    VerifiedReleaseReceipt.from_dict(receipt.as_dict())
    if receipt.repository_id != boundary.repository_id:
        raise IntegrityError("market-state checkpoint belongs to another repository")
    manifest_path = boundary.assert_active_path(
        boundary.active_root / receipt.manifest_path,
        purpose="checkpointed market-state manifest",
        subtree="manifests/data_releases",
    )
    assert_plain_file(manifest_path)
    manifest = verify_data_release_manifest(
        manifest_path,
        boundary,
        verify_files=False,
    )
    if (
        manifest_path.stat().st_mtime_ns > checkpoint_mtime_ns
        or sha256_file(manifest_path) != receipt.manifest_sha256
        or manifest.release_id != receipt.release_id
        or manifest.phase != receipt.phase
        or manifest.release_kind != receipt.release_kind
        or manifest.schema_version != receipt.schema_version
    ):
        raise IntegrityError("market-state manifest changed after its durable checkpoint")
    for entry in manifest.files:
        physical = boundary.assert_active_path(
            boundary.active_root / manifest.physical_relative_path(entry),
            purpose="checkpointed market-state file",
            subtree="data/market_state",
        )
        assert_plain_file(physical)
        stat = physical.stat()
        if stat.st_size != entry.size or stat.st_mtime_ns > checkpoint_mtime_ns:
            raise IntegrityError(
                "market-state file changed after its durable checkpoint"
            )
    return manifest


def load_market_state_foundation(
    receipt: VerifiedReleaseReceipt,
    *,
    boundary: RepoBoundary,
    expected_selection: ResolvedFoundationSelection,
    expected_source_selection_receipt: VerifiedReleaseReceipt,
    expected_policies: VerifiedFoundationPolicies,
    expected_coverage_policy: FoundationCoveragePolicy,
    expected_statistics_roles: StatisticsRolePolicy,
    trusted_checkpoint_mtime_ns: int | None = None,
) -> LoadedMarketStateFoundation:
    manifest = (
        receipt.verify(boundary)
        if trusted_checkpoint_mtime_ns is None
        else _checkpoint_verified_market_state_manifest(
            receipt,
            boundary=boundary,
            checkpoint_mtime_ns=trusted_checkpoint_mtime_ns,
        )
    )
    release_paths: dict[str, Path] = {}
    for entry in manifest.files:
        if entry.logical_path in release_paths:
            raise IntegrityError("market-state manifest contains a duplicated logical path")
        release_paths[entry.logical_path] = (
            boundary.active_root / manifest.physical_relative_path(entry)
        )
    if expected_policies.boundary.repository_id != boundary.repository_id:
        raise IntegrityError("market-state policies belong to another repository")
    expected_policies.verify()
    source_selection_manifest = expected_source_selection_receipt.verify(boundary)
    source_selection_manifest_id = source_selection_manifest.metadata.get(
        "selection_manifest_id"
    )
    if (
        manifest.release_kind != MARKET_STATE_RELEASE_KIND
        or manifest.schema_version != MARKET_STATE_SCHEMA_VERSION
        or set(manifest.metadata)
        != {
            "coverage_matrix_id",
            "foundation_policy_set_id",
            "market_state_foundation_id",
            "query_manifest_id",
            "source_selection_manifest_id",
            "statistics_ledger_rows",
            "statistics_rows",
            "statistics_timestamp_census_sha256",
            "status_rows",
            "status_timestamp_census_sha256",
        }
    ):
        raise IntegrityError("market-state release kind/schema/metadata is invalid")
    try:
        contract = manifest.embedded_documents["market_state_contract.json"]
        coverage = manifest.embedded_documents["coverage_matrix.json"]
    except KeyError as exc:
        raise IntegrityError("market-state embedded contract is absent") from exc
    if (
        not isinstance(contract, dict)
        or not isinstance(coverage, list)
        or coverage != list(expected_selection.coverage_matrix)
        or sha256_json(coverage) != expected_selection.coverage_matrix_id
    ):
        raise IntegrityError("market-state coverage matrix differs from exact selection")
    status_outputs = contract.get("status_outputs")
    statistics_outputs = contract.get("statistics_outputs")
    if not isinstance(status_outputs, list) or not isinstance(statistics_outputs, list):
        raise IntegrityError("market-state output indexes are invalid")
    if not expected_selection.status_files or not expected_selection.statistics_files:
        raise IntegrityError("market-state expected selection lacks a canonical family")
    source_release_id = expected_selection.status_files[0].binding.source_release_id
    expected_status_bindings = [
        {**item.as_coverage_binding(), "output_path": _output_path("status", item)}
        for item in expected_selection.status_files
    ]
    expected_statistics_bindings = [
        {
            **item.as_coverage_binding(),
            "ledger_output_path": (
                _output_path("statistics", item).removesuffix(".jsonl")
                + ".ledger.jsonl"
            ),
            "output_path": _output_path("statistics", item),
        }
        for item in expected_selection.statistics_files
    ]
    if (
        len(status_outputs) != len(expected_status_bindings)
        or len(statistics_outputs) != len(expected_statistics_bindings)
    ):
        raise IntegrityError("market-state output index cardinality is invalid")
    for observed, expected in zip(
        status_outputs, expected_status_bindings, strict=True
    ):
        if (
            set(observed)
            != set(expected)
            | {
                "output_sha256",
                "row_count",
                "timestamp_census",
                "timestamp_census_sha256",
            }
            or any(observed.get(key) != value for key, value in expected.items())
        ):
            raise IntegrityError("status output index differs from exact selection")
    for observed, expected in zip(
        statistics_outputs, expected_statistics_bindings, strict=True
    ):
        if (
            set(observed)
            != set(expected)
            | {
                "ledger_output_sha256",
                "output_sha256",
                "row_count",
                "timestamp_census",
                "timestamp_census_sha256",
            }
            or any(observed.get(key) != value for key, value in expected.items())
        ):
            raise IntegrityError("statistics output index differs from exact selection")
    if trusted_checkpoint_mtime_ns is None:
        status_total, observed_status_timestamp_census = _validate_output_rows(
            release_paths,
            status_outputs,
            kind="status",
            expected_source_release_id=source_release_id,
        )
        statistics_total, observed_statistics_timestamp_census = _validate_output_rows(
            release_paths,
            statistics_outputs,
            kind="statistics",
            expected_source_release_id=source_release_id,
        )
        statistics_ledger_total = _validate_statistics_ledger_outputs(
            release_paths, statistics_outputs, roles=expected_statistics_roles
        )
    else:
        try:
            status_total = int(contract["status_rows"])
            statistics_total = int(contract["statistics_rows"])
            statistics_ledger_total = int(contract["statistics_ledger_rows"])
            observed_status_timestamp_census = dict(
                contract["status_timestamp_census"]
            )
            observed_statistics_timestamp_census = dict(
                contract["statistics_timestamp_census"]
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise IntegrityError(
                "checkpointed market-state census is invalid"
            ) from exc
    core = {key: value for key, value in contract.items() if key != "market_state_foundation_id"}
    expected_paths = {
        *(str(item["output_path"]) for item in status_outputs),
        *(str(item["output_path"]) for item in statistics_outputs),
        *(str(item["ledger_output_path"]) for item in statistics_outputs),
    }
    if (
        set(contract)
        != {
            "coverage_matrix_id",
            "coverage_policy",
            "coverage_policy_hash",
            "feature_eligible_statistic_types",
            "foundation_policy_hash",
            "foundation_policy_release_id",
            "foundation_policy_release_receipt_id",
            "foundation_policy_set_id",
            "market_state_foundation_id",
            "required_market_year_count",
            "query_manifest_id",
            "query_mode_census",
            "provider_data_epochs_sha256",
            "schema_version",
            "selected_file_count",
            "source_selection_receipt_id",
            "source_selection_manifest_id",
            "statistics_outputs",
            "statistics_ledger_rows",
            "statistics_role_policy",
            "statistics_role_policy_hash",
            "statistics_rows",
            "statistics_timestamp_census",
            "statistics_timestamp_census_sha256",
            "statistics_source_market_year_count",
            "statistics_source_market_year_fraction",
            "status_outputs",
            "status_rows",
            "status_timestamp_census",
            "status_timestamp_census_sha256",
            "status_source_market_year_count",
            "status_source_market_year_fraction",
        }
        or contract["market_state_foundation_id"] != sha256_json(core)
        or contract["market_state_foundation_id"]
        != manifest.metadata["market_state_foundation_id"]
        or contract["coverage_matrix_id"] != expected_selection.coverage_matrix_id
        or contract["query_manifest_id"] != expected_selection.query_manifest_id
        or source_selection_manifest_id != contract["source_selection_manifest_id"]
        or source_selection_manifest.metadata.get("query_manifest_id")
        != expected_selection.query_manifest_id
        or contract["query_mode_census"] != list(expected_selection.query_mode_census)
        or manifest.metadata["query_manifest_id"]
        != expected_selection.query_manifest_id
        or contract["coverage_policy"] != expected_coverage_policy.as_dict()
        or contract["coverage_policy_hash"] != expected_coverage_policy.policy_hash
        or contract["statistics_role_policy"] != expected_statistics_roles.as_dict()
        or contract["statistics_role_policy_hash"] != expected_statistics_roles.policy_hash
        or contract["feature_eligible_statistic_types"] != []
        or contract["source_selection_receipt_id"]
        != expected_source_selection_receipt.receipt_id
        or contract["foundation_policy_release_id"]
        != expected_policies.receipt.release_id
        or contract["foundation_policy_release_receipt_id"]
        != expected_policies.receipt.receipt_id
        or contract["foundation_policy_set_id"] != expected_policies.policy_set_id
        or contract["foundation_policy_hash"]
        != expected_policies.foundation.policy_hash
        or contract["provider_data_epochs_sha256"]
        != expected_policies.foundation.provider_data_epochs_sha256
        or observed_status_timestamp_census
        != contract["status_timestamp_census"]
        or sha256_json(observed_status_timestamp_census)
        != contract["status_timestamp_census_sha256"]
        or observed_statistics_timestamp_census
        != contract["statistics_timestamp_census"]
        or sha256_json(observed_statistics_timestamp_census)
        != contract["statistics_timestamp_census_sha256"]
        or manifest.metadata["foundation_policy_set_id"]
        != expected_policies.policy_set_id
        or manifest.metadata["source_selection_manifest_id"]
        != source_selection_manifest_id
        or manifest.metadata["status_timestamp_census_sha256"]
        != contract["status_timestamp_census_sha256"]
        or manifest.metadata["statistics_timestamp_census_sha256"]
        != contract["statistics_timestamp_census_sha256"]
        or status_total != contract["status_rows"]
        or statistics_total != contract["statistics_rows"]
        or statistics_ledger_total != contract["statistics_ledger_rows"]
        or status_total != manifest.metadata["status_rows"]
        or statistics_total != manifest.metadata["statistics_rows"]
        or statistics_ledger_total != manifest.metadata["statistics_ledger_rows"]
        or {entry.path for entry in manifest.files} != expected_paths
        or manifest.source_release_ids
        != tuple(
            sorted(
                (
                    expected_source_selection_receipt.release_id,
                    source_release_id,
                    expected_policies.receipt.release_id,
                )
            )
        )
    ):
        raise IntegrityError("market-state release contract or dependency closure is invalid")
    return LoadedMarketStateFoundation(
        receipt=receipt,
        contract=contract,
        coverage_matrix=tuple(coverage),
        boundary=boundary,
        release_paths=MappingProxyType(release_paths),
        status_outputs=tuple(status_outputs),
        statistics_outputs=tuple(statistics_outputs),
    )


def publish_status_eligibility(
    *,
    causal_receipt: VerifiedReleaseReceipt,
    market_state: LoadedMarketStateFoundation,
    market: str,
    year: int,
    publisher: AtomicPublisher,
    batch_rows: int = 100_000,
) -> VerifiedReleaseReceipt:
    causal_path, causal_report = load_causal_interval(
        causal_receipt, boundary=publisher.boundary
    )
    if type(batch_rows) is not int or batch_rows <= 0:
        raise ContractError("status eligibility batch size must be positive")
    if (
        market_state.receipt.repository_id != publisher.boundary.repository_id
        or causal_report.get("logical_root", "").split("/")[2:4]
        != [market, str(year)]
        or market_state.contract.get("foundation_policy_set_id")
        != causal_report.get("foundation_policy_set_id")
    ):
        raise IntegrityError("status eligibility inputs belong to another interval")
    ledger = AsOfStatusLedger(
        market_state.iter_status_records(market=market, year=year)
    )
    stage = publisher.create_stage("status_eligibility")
    keys_path = stage / "status_eligible_keys.parquet"
    total = resolved = eligible = unresolved = ineligible = 0
    status_dispositions: dict[str, int] = {}
    eligible_keys: set[tuple[str, int, int]] = set()
    key_buffer: list[dict[str, object]] = []
    parquet = pq.ParquetFile(causal_path)
    source_columns = (
        "actual_identity_hash",
        "dataset",
        "disposition",
        "event_at_ns",
        "instrument_id",
        "instrument_id_date_utc",
        "publisher_id",
        "resolution_as_of_ns",
    )
    writer = pq.ParquetWriter(
        keys_path,
        STATUS_ELIGIBLE_KEY_SCHEMA,
        compression="zstd",
        compression_level=9,
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
        data_page_version="2.0",
        use_deprecated_int96_timestamps=False,
    )
    try:
        for batch in parquet.iter_batches(
            batch_size=batch_rows,
            columns=list(source_columns),
        ):
            columns = {
                name: batch.column(index).to_pylist()
                for index, name in enumerate(source_columns)
            }
            for index in range(batch.num_rows):
                decision_at_ns = columns["resolution_as_of_ns"][index]
                event_at_ns = columns["event_at_ns"][index]
                if type(decision_at_ns) is not int or type(event_at_ns) is not int:
                    raise IntegrityError(
                        "status eligibility causal clocks are invalid"
                    )
                decision = ledger.as_of(
                    dataset=str(columns["dataset"][index]),
                    publisher_id=int(columns["publisher_id"][index]),
                    instrument_id=int(columns["instrument_id"][index]),
                    instrument_id_date_utc=str(
                        columns["instrument_id_date_utc"][index]
                    ),
                    decision_at_ns=decision_at_ns,
                )
                if decision.in_coverage_denominator is not True:
                    raise IntegrityError(
                        "status decision escaped the coverage denominator"
                    )
                causal_eligible = columns["disposition"][index] == "ELIGIBLE"
                foundation_eligible = (
                    causal_eligible and decision.foundation_eligible
                )
                status_disposition = (
                    decision.status_disposition
                    if causal_eligible
                    else "CAUSAL_BAR_INELIGIBLE"
                )
                status_dispositions[status_disposition] = (
                    status_dispositions.get(status_disposition, 0) + 1
                )
                total += 1
                resolved += int(decision.status_resolved)
                unresolved += int(not decision.status_resolved)
                eligible += int(foundation_eligible)
                ineligible += int(not foundation_eligible)
                if not foundation_eligible:
                    continue
                identity_hash = columns["actual_identity_hash"][index]
                if (
                    type(identity_hash) is not str
                    or re.fullmatch(r"[0-9a-f]{64}", identity_hash) is None
                ):
                    raise IntegrityError(
                        "status-eligible causal identity is invalid"
                    )
                key = (identity_hash, event_at_ns, decision_at_ns)
                if key in eligible_keys:
                    raise IntegrityError(
                        "status eligibility join key is duplicated"
                    )
                eligible_keys.add(key)
                key_buffer.append(
                    {
                        "actual_identity_hash": identity_hash,
                        "bar_event_at_ns": event_at_ns,
                        "decision_at_ns": decision_at_ns,
                    }
                )
                if len(key_buffer) >= batch_rows:
                    writer.write_table(
                        pa.Table.from_pylist(
                            key_buffer, schema=STATUS_ELIGIBLE_KEY_SCHEMA
                        ),
                        row_group_size=len(key_buffer),
                    )
                    key_buffer.clear()
        if key_buffer:
            writer.write_table(
                pa.Table.from_pylist(
                    key_buffer, schema=STATUS_ELIGIBLE_KEY_SCHEMA
                ),
                row_group_size=len(key_buffer),
            )
    finally:
        writer.close()
    descriptor = os.open(keys_path, os.O_RDWR | getattr(os, "O_BINARY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if total == 0 or total != resolved + unresolved or total != eligible + ineligible:
        raise IntegrityError("status eligibility denominator census is invalid")
    eligible_key_set_sha256 = sha256_json(sorted(eligible_keys))
    contract_core = {
        "eligible_rows": eligible,
        "eligible_join_keys_materialized": True,
        "eligible_key_set_sha256": eligible_key_set_sha256,
        "eligible_keys_schema": STATUS_ELIGIBLE_KEY_SCHEMA.metadata[
            b"schema_id"
        ].decode("ascii"),
        "ineligible_rows": ineligible,
        "market": market,
        "prediction_in_coverage_denominator_rows": total,
        "resolved_status_rows": resolved,
        "row_level_decision_records_materialized": False,
        "schema_version": STATUS_ELIGIBILITY_SCHEMA_VERSION,
        "source_causal_release_receipt_id": causal_receipt.receipt_id,
        "source_market_state_release_receipt_id": market_state.receipt.receipt_id,
        "status_disposition_counts": dict(sorted(status_dispositions.items())),
        "statistics_feature_use": False,
        "total_rows": total,
        "unresolved_status_rows": unresolved,
        "year": year,
    }
    contract = {**contract_core, "status_eligibility_id": sha256_json(contract_core)}
    (stage / "status_eligibility_contract.json").write_bytes(
        canonical_bytes(contract) + b"\n"
    )
    manifest = ReleaseManifest.build(
        stage,
        phase="status_eligibility",
        release_kind=STATUS_ELIGIBILITY_RELEASE_KIND,
        schema_version=STATUS_ELIGIBILITY_SCHEMA_VERSION,
        logical_paths={
            "status_eligibility_contract.json": (
                f"data/status_eligibility/{market}/{year}/"
                f"{str(causal_report['logical_root']).split('/')[-1]}/"
                "status_eligibility_contract.json"
            ),
            "status_eligible_keys.parquet": (
                f"data/status_eligibility/{market}/{year}/"
                f"{str(causal_report['logical_root']).split('/')[-1]}/"
                "status_eligible_keys.parquet"
            ),
        },
        source_release_ids=(causal_receipt.release_id, market_state.receipt.release_id),
        metadata={
            "eligible_rows": eligible,
            "eligible_key_set_sha256": eligible_key_set_sha256,
            "eligible_keys_schema": STATUS_ELIGIBLE_KEY_SCHEMA.metadata[
                b"schema_id"
            ].decode("ascii"),
            "market": market,
            "status_eligibility_id": contract["status_eligibility_id"],
            "total_rows": total,
            "year": year,
        },
    )
    manifest_path = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, publisher.boundary)
    load_status_eligibility(
        receipt,
        causal_receipt=causal_receipt,
        market_state_receipt=market_state.receipt,
        boundary=publisher.boundary,
    )
    return receipt


def load_status_eligibility(
    receipt: VerifiedReleaseReceipt,
    *,
    causal_receipt: VerifiedReleaseReceipt,
    market_state_receipt: VerifiedReleaseReceipt,
    boundary: RepoBoundary,
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "status_eligibility"
        or manifest.release_kind != STATUS_ELIGIBILITY_RELEASE_KIND
        or manifest.schema_version != STATUS_ELIGIBILITY_SCHEMA_VERSION
        or {Path(entry.path).name for entry in manifest.files}
        != {"status_eligibility_contract.json", "status_eligible_keys.parquet"}
        or manifest.source_release_ids
        != tuple(sorted((causal_receipt.release_id, market_state_receipt.release_id)))
        or set(manifest.metadata)
        != {
            "eligible_key_set_sha256",
            "eligible_keys_schema",
            "eligible_rows",
            "market",
            "status_eligibility_id",
            "total_rows",
            "year",
        }
    ):
        raise IntegrityError("status eligibility release dependencies are invalid")
    contract = _canonical_object(
        receipt.resolve_unique_filename("status_eligibility_contract.json", boundary),
        description="status eligibility contract",
    )
    core = {key: value for key, value in contract.items() if key != "status_eligibility_id"}
    try:
        keys = pq.ParquetFile(
            receipt.resolve_unique_filename(
                "status_eligible_keys.parquet", boundary
            )
        )
    except Exception as exc:
        raise IntegrityError("status eligibility key Parquet is invalid") from exc
    schema_id = STATUS_ELIGIBLE_KEY_SCHEMA.metadata[b"schema_id"].decode(
        "ascii"
    )
    dispositions = contract.get("status_disposition_counts")
    if (
        set(contract)
        != {
            "eligible_rows",
            "eligible_join_keys_materialized",
            "eligible_key_set_sha256",
            "eligible_keys_schema",
            "ineligible_rows",
            "market",
            "prediction_in_coverage_denominator_rows",
            "resolved_status_rows",
            "row_level_decision_records_materialized",
            "schema_version",
            "source_causal_release_receipt_id",
            "source_market_state_release_receipt_id",
            "statistics_feature_use",
            "status_disposition_counts",
            "status_eligibility_id",
            "total_rows",
            "unresolved_status_rows",
            "year",
        }
        or contract["status_eligibility_id"] != sha256_json(core)
        or contract["status_eligibility_id"]
        != manifest.metadata["status_eligibility_id"]
        or contract["source_causal_release_receipt_id"] != causal_receipt.receipt_id
        or contract["source_market_state_release_receipt_id"]
        != market_state_receipt.receipt_id
        or contract["statistics_feature_use"] is not False
        or contract["eligible_join_keys_materialized"] is not True
        or contract["row_level_decision_records_materialized"] is not False
        or contract["eligible_keys_schema"] != schema_id
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(contract.get("eligible_key_set_sha256")),
        )
        is None
        or not keys.schema_arrow.equals(
            STATUS_ELIGIBLE_KEY_SCHEMA, check_metadata=True
        )
        or keys.metadata.num_rows != contract["eligible_rows"]
        or type(contract["total_rows"]) is not int
        or contract["total_rows"] <= 0
        or type(contract["eligible_rows"]) is not int
        or type(contract["ineligible_rows"]) is not int
        or type(contract["resolved_status_rows"]) is not int
        or type(contract["unresolved_status_rows"]) is not int
        or contract["total_rows"]
        != contract["prediction_in_coverage_denominator_rows"]
        or contract["total_rows"]
        != contract["resolved_status_rows"]
        + contract["unresolved_status_rows"]
        or contract["total_rows"]
        != contract["eligible_rows"] + contract["ineligible_rows"]
        or not isinstance(dispositions, dict)
        or not dispositions
        or any(
            type(name) is not str
            or not name
            or type(count) is not int
            or count < 0
            for name, count in dispositions.items()
        )
        or sum(dispositions.values()) != contract["total_rows"]
        or manifest.metadata["eligible_rows"] != contract["eligible_rows"]
        or manifest.metadata["eligible_key_set_sha256"]
        != contract["eligible_key_set_sha256"]
        or manifest.metadata["eligible_keys_schema"] != schema_id
        or manifest.metadata["market"] != contract["market"]
        or manifest.metadata["total_rows"] != contract["total_rows"]
        or manifest.metadata["year"] != contract["year"]
    ):
        raise IntegrityError("status eligibility contract/counts are invalid")
    return contract


def status_eligible_decision_keys(
    receipt: VerifiedReleaseReceipt,
    *,
    causal_receipt: VerifiedReleaseReceipt,
    market_state_receipt: VerifiedReleaseReceipt,
    boundary: RepoBoundary,
) -> frozenset[tuple[str, int, int]]:
    """Return the exact status-eligible identity/event/decision join keys."""

    contract = load_status_eligibility(
        receipt,
        causal_receipt=causal_receipt,
        market_state_receipt=market_state_receipt,
        boundary=boundary,
    )
    keys: set[tuple[str, int, int]] = set()
    parquet = pq.ParquetFile(
        receipt.resolve_unique_filename(
            "status_eligible_keys.parquet", boundary
        )
    )
    names = tuple(STATUS_ELIGIBLE_KEY_SCHEMA.names)
    for batch in parquet.iter_batches(columns=list(names)):
        columns = {
            name: batch.column(index).to_pylist()
            for index, name in enumerate(names)
        }
        for index in range(batch.num_rows):
            identity_hash = columns["actual_identity_hash"][index]
            event_at_ns = columns["bar_event_at_ns"][index]
            decision_at_ns = columns["decision_at_ns"][index]
            if (
                type(identity_hash) is not str
                or re.fullmatch(r"[0-9a-f]{64}", identity_hash) is None
                or type(event_at_ns) is not int
                or type(decision_at_ns) is not int
            ):
                raise IntegrityError(
                    "status eligibility join key is invalid"
                )
            key = (identity_hash, event_at_ns, decision_at_ns)
            if key in keys:
                raise IntegrityError(
                    "status eligibility join key is duplicated"
                )
            keys.add(key)
    if (
        len(keys) != contract["eligible_rows"]
        or sha256_json(sorted(keys))
        != contract["eligible_key_set_sha256"]
    ):
        raise IntegrityError("status eligibility join-key census is invalid")
    return frozenset(keys)
