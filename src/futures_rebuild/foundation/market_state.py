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
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Iterator, Mapping, Sequence

import pyarrow.parquet as pq

from ..boundary import RepoBoundary
from ..canonical import canonical_bytes, sha256_file, sha256_json
from ..errors import ContractError, IntegrityError
from ..release import AtomicPublisher, ReleaseManifest, VerifiedReleaseReceipt
from .decoder import iter_statistics, iter_statuses
from .materialize import CAUSAL_RELEASE_KIND, load_causal_interval
from .records import INT64_NULL, StatisticsRecordV1, StatusRecordV1
from .selection import ResolvedFoundationSelection, SelectedFamilyFile


MARKET_STATE_RELEASE_KIND = "futures_status_statistics_foundation"
MARKET_STATE_SCHEMA_VERSION = "2.0.0"
STATUS_ELIGIBILITY_RELEASE_KIND = "futures_status_asof_eligibility"
STATUS_ELIGIBILITY_SCHEMA_VERSION = "1.0.0"

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


def _canonical_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} JSON is invalid") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


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
    """Deterministic bitemporal ledger; future records are never backfilled."""

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
        self._event_keys: dict[tuple[str, int, int, str], tuple[int, ...]] = {}
        for key, values in grouped.items():
            ordered = tuple(
                sorted(
                    values,
                    key=lambda item: (
                        item.ts_event_ns,
                        item.ts_recv_ns,
                        item.row_ordinal,
                        item.row_sha256,
                    ),
                )
            )
            self._records[key] = ordered
            self._event_keys[key] = tuple(item.ts_event_ns for item in ordered)

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
        event_keys = self._event_keys.get(key, ())
        index = bisect.bisect_right(event_keys, decision_at_ns) - 1
        matched: StatusRecordV1 | None = None
        while index >= 0:
            candidate = values[index]
            if candidate.ts_recv_ns <= decision_at_ns:
                matched = candidate
                break
            index -= 1
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
        self._records = {
            key: tuple(
                sorted(
                    values,
                    key=lambda item: (
                        item.ts_recv_ns,
                        item.ts_event_ns,
                        item.sequence,
                        item.row_ordinal,
                        item.row_sha256,
                    ),
                )
            )
            for key, values in grouped.items()
        }
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
        records = self._records.get(
            (
                dataset,
                publisher_id,
                instrument_id,
                instrument_id_date_utc,
                stat_type,
                ts_ref_ns,
            ),
            (),
        )
        candidates = [
            item
            for item in records
            if item.ts_event_ns <= decision_at_ns and item.ts_recv_ns <= decision_at_ns
        ]
        role = self.roles.role_for(stat_type)
        if not candidates:
            return StatisticsStateV1("STATISTICS_UNRESOLVED", role, False, None, None, None)
        latest = candidates[-1]
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
    root: Path
    status_outputs: tuple[dict[str, object], ...]
    statistics_outputs: tuple[dict[str, object], ...]

    def iter_status_records(self, *, market: str, year: int) -> Iterator[StatusRecordV1]:
        for output in self.status_outputs:
            if output["market"] != market or int(output["year"]) != year:
                continue
            path = self.root / str(output["output_path"])
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
    directory = {"status": "s", "statistics": "x"}.get(prefix)
    if directory is None:
        raise ContractError("market-state output family is invalid")
    return f"{directory}/{item.market}/{item.year}/{item.binding.sha256[:16]}.jsonl"


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


def publish_market_state_foundation(
    *,
    selection: ResolvedFoundationSelection,
    source_selection_receipt: VerifiedReleaseReceipt,
    coverage_policy: FoundationCoveragePolicy,
    statistics_roles: StatisticsRolePolicy,
    publisher: AtomicPublisher,
    batch_rows: int = 100_000,
) -> VerifiedReleaseReceipt:
    """Decode every selected status/statistics file into one immutable release."""

    if type(batch_rows) is not int or batch_rows <= 0:
        raise ContractError("market-state decode batch size must be positive")
    denominator, status_present, statistics_present, status_fraction, statistics_fraction = (
        _source_coverage(selection)
    )
    if (
        status_fraction < coverage_policy.minimum_status_source_market_year_fraction
        or statistics_fraction
        < coverage_policy.minimum_statistics_source_market_year_fraction
    ):
        raise IntegrityError("canonical market-state source coverage is below its pinned gate")
    stage = publisher.create_stage("market_state_foundation")
    status_outputs: list[dict[str, object]] = []
    statistics_outputs: list[dict[str, object]] = []
    total_status_rows = 0
    total_statistics_rows = 0
    for item in selection.status_files:
        output_path = _output_path("status", item)
        target = stage / output_path
        target.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with target.open("xb") as handle:
            for record in iter_statuses(
                item.binding,
                market=item.market,
                expected_query_contract=item.query_contract,
                batch_rows=batch_rows,
            ):
                handle.write(canonical_bytes(record.as_dict()) + b"\n")
                count += 1
            handle.flush()
            os.fsync(handle.fileno())
        if count == 0:
            raise IntegrityError("selected status DBN decoded to zero rows")
        total_status_rows += count
        status_outputs.append(
            {
                **item.as_coverage_binding(),
                "output_path": output_path,
                "output_sha256": sha256_file(target),
                "row_count": count,
            }
        )
    for item in selection.statistics_files:
        output_path = _output_path("statistics", item)
        target = stage / output_path
        target.parent.mkdir(parents=True, exist_ok=True)
        ledger_output_path = (
            f"l/{item.market}/{item.year}/{item.binding.sha256[:16]}.jsonl"
        )
        ledger_target = stage / ledger_output_path
        ledger_target.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with target.open("xb") as handle, ledger_target.open("xb") as ledger_handle:
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
                count += 1
            handle.flush()
            os.fsync(handle.fileno())
            ledger_handle.flush()
            os.fsync(ledger_handle.fileno())
        if count == 0:
            raise IntegrityError("selected statistics DBN decoded to zero rows")
        total_statistics_rows += count
        statistics_outputs.append(
            {
                **item.as_coverage_binding(),
                "ledger_output_path": ledger_output_path,
                "ledger_output_sha256": sha256_file(ledger_target),
                "output_path": output_path,
                "output_sha256": sha256_file(target),
                "row_count": count,
            }
        )
    if total_status_rows == 0 or total_statistics_rows == 0:
        raise IntegrityError("canonical status/statistics families must be nonempty")
    coverage = list(selection.coverage_matrix)
    (stage / "coverage_matrix.json").write_bytes(canonical_bytes(coverage) + b"\n")
    contract_core = {
        "coverage_matrix_id": selection.coverage_matrix_id,
        "coverage_policy": coverage_policy.as_dict(),
        "coverage_policy_hash": coverage_policy.policy_hash,
        "feature_eligible_statistic_types": [],
        "required_market_year_count": denominator,
        "query_manifest_id": selection.query_manifest_id,
        "query_mode_census": list(selection.query_mode_census),
        "schema_version": MARKET_STATE_SCHEMA_VERSION,
        "selected_file_count": selection.selected_file_count,
        "source_selection_receipt_id": source_selection_receipt.receipt_id,
        "statistics_outputs": statistics_outputs,
        "statistics_role_policy": statistics_roles.as_dict(),
        "statistics_role_policy_hash": statistics_roles.policy_hash,
        "statistics_rows": total_statistics_rows,
        "statistics_ledger_rows": total_statistics_rows,
        "statistics_source_market_year_count": statistics_present,
        "statistics_source_market_year_fraction": str(statistics_fraction),
        "status_outputs": status_outputs,
        "status_rows": total_status_rows,
        "status_source_market_year_count": status_present,
        "status_source_market_year_fraction": str(status_fraction),
    }
    contract = {**contract_core, "market_state_foundation_id": sha256_json(contract_core)}
    (stage / "market_state_contract.json").write_bytes(
        canonical_bytes(contract) + b"\n"
    )
    manifest = ReleaseManifest.build(
        stage,
        release_kind=MARKET_STATE_RELEASE_KIND,
        schema_version=MARKET_STATE_SCHEMA_VERSION,
        source_release_ids=(
            source_selection_receipt.release_id,
            selection.status_files[0].binding.source_snapshot_id,
        ),
        metadata={
            "coverage_matrix_id": selection.coverage_matrix_id,
            "market_state_foundation_id": contract["market_state_foundation_id"],
            "query_manifest_id": selection.query_manifest_id,
            "statistics_ledger_rows": total_statistics_rows,
            "statistics_rows": total_statistics_rows,
            "status_rows": total_status_rows,
        },
    )
    release = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_release(release, publisher.boundary)
    load_market_state_foundation(
        receipt,
        boundary=publisher.boundary,
        expected_selection=selection,
        expected_source_selection_receipt=source_selection_receipt,
        expected_coverage_policy=coverage_policy,
        expected_statistics_roles=statistics_roles,
    )
    return receipt


def _validate_output_rows(
    root: Path,
    outputs: Sequence[Mapping[str, object]],
    *,
    kind: str,
    expected_source_snapshot_id: str,
) -> int:
    total = 0
    seen_paths: set[str] = set()
    for output in outputs:
        output_path = output.get("output_path")
        if type(output_path) is not str or output_path in seen_paths:
            raise IntegrityError(f"{kind} output path is invalid or duplicated")
        seen_paths.add(output_path)
        path = root / output_path
        if sha256_file(path) != output.get("output_sha256"):
            raise IntegrityError(f"{kind} output hash differs from its contract")
        count = 0
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
                        or record.source_release_id != expected_source_snapshot_id
                    ):
                        raise IntegrityError(
                            f"{kind} output row differs from its exact source binding"
                        )
                    count += 1
        except (OSError, UnicodeDecodeError, ValueError, ContractError) as exc:
            raise IntegrityError(f"{kind} output row is invalid") from exc
        if count <= 0 or count != output.get("row_count"):
            raise IntegrityError(f"{kind} output row count differs from its contract")
        total += count
    return total


def _validate_statistics_ledger_outputs(
    root: Path,
    outputs: Sequence[Mapping[str, object]],
    *,
    roles: StatisticsRolePolicy,
) -> int:
    total = 0
    for output in outputs:
        raw_path = root / str(output["output_path"])
        ledger_path = root / str(output.get("ledger_output_path"))
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


def load_market_state_foundation(
    receipt: VerifiedReleaseReceipt,
    *,
    boundary: RepoBoundary,
    expected_selection: ResolvedFoundationSelection,
    expected_source_selection_receipt: VerifiedReleaseReceipt,
    expected_coverage_policy: FoundationCoveragePolicy,
    expected_statistics_roles: StatisticsRolePolicy,
) -> LoadedMarketStateFoundation:
    manifest = receipt.verify(boundary)
    root = boundary.active_root / receipt.relative_root
    if (
        manifest.release_kind != MARKET_STATE_RELEASE_KIND
        or manifest.schema_version != MARKET_STATE_SCHEMA_VERSION
        or set(manifest.metadata)
        != {
            "coverage_matrix_id",
            "market_state_foundation_id",
            "query_manifest_id",
            "statistics_ledger_rows",
            "statistics_rows",
            "status_rows",
        }
    ):
        raise IntegrityError("market-state release kind/schema/metadata is invalid")
    contract = _canonical_object(
        root / "market_state_contract.json", description="market-state contract"
    )
    try:
        raw_coverage = (root / "coverage_matrix.json").read_bytes()
        coverage = json.loads(raw_coverage.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("market-state coverage matrix is invalid") from exc
    if (
        not isinstance(coverage, list)
        or raw_coverage != canonical_bytes(coverage) + b"\n"
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
    source_snapshot_id = expected_selection.status_files[0].binding.source_snapshot_id
    expected_status_bindings = [
        {**item.as_coverage_binding(), "output_path": _output_path("status", item)}
        for item in expected_selection.status_files
    ]
    expected_statistics_bindings = [
        {
            **item.as_coverage_binding(),
            "ledger_output_path": (
                f"l/{item.market}/{item.year}/{item.binding.sha256[:16]}.jsonl"
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
        if any(observed.get(key) != value for key, value in expected.items()):
            raise IntegrityError("status output index differs from exact selection")
    for observed, expected in zip(
        statistics_outputs, expected_statistics_bindings, strict=True
    ):
        if any(observed.get(key) != value for key, value in expected.items()):
            raise IntegrityError("statistics output index differs from exact selection")
    status_total = _validate_output_rows(
        root,
        status_outputs,
        kind="status",
        expected_source_snapshot_id=source_snapshot_id,
    )
    statistics_total = _validate_output_rows(
        root,
        statistics_outputs,
        kind="statistics",
        expected_source_snapshot_id=source_snapshot_id,
    )
    statistics_ledger_total = _validate_statistics_ledger_outputs(
        root, statistics_outputs, roles=expected_statistics_roles
    )
    core = {key: value for key, value in contract.items() if key != "market_state_foundation_id"}
    expected_paths = {
        "coverage_matrix.json",
        "market_state_contract.json",
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
            "market_state_foundation_id",
            "required_market_year_count",
            "query_manifest_id",
            "query_mode_census",
            "schema_version",
            "selected_file_count",
            "source_selection_receipt_id",
            "statistics_outputs",
            "statistics_ledger_rows",
            "statistics_role_policy",
            "statistics_role_policy_hash",
            "statistics_rows",
            "statistics_source_market_year_count",
            "statistics_source_market_year_fraction",
            "status_outputs",
            "status_rows",
            "status_source_market_year_count",
            "status_source_market_year_fraction",
        }
        or contract["market_state_foundation_id"] != sha256_json(core)
        or contract["market_state_foundation_id"]
        != manifest.metadata["market_state_foundation_id"]
        or contract["coverage_matrix_id"] != expected_selection.coverage_matrix_id
        or contract["query_manifest_id"] != expected_selection.query_manifest_id
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
                    source_snapshot_id,
                )
            )
        )
    ):
        raise IntegrityError("market-state release contract or dependency closure is invalid")
    return LoadedMarketStateFoundation(
        receipt=receipt,
        contract=contract,
        coverage_matrix=tuple(coverage),
        root=root,
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
        or causal_report.get("logical_root", "").split("/")[1:3]
        != [market, str(year)]
    ):
        raise IntegrityError("status eligibility inputs belong to another interval")
    ledger = AsOfStatusLedger(
        market_state.iter_status_records(market=market, year=year)
    )
    stage = publisher.create_stage("status_eligibility")
    rows_path = stage / "status_eligibility_rows.jsonl"
    total = resolved = eligible = unresolved = ineligible = 0
    parquet = pq.ParquetFile(causal_path)
    with rows_path.open("xb") as handle:
        for batch in parquet.iter_batches(batch_size=batch_rows):
            for row in batch.to_pylist():
                decision = ledger.as_of(
                    dataset=str(row["dataset"]),
                    publisher_id=int(row["publisher_id"]),
                    instrument_id=int(row["instrument_id"]),
                    instrument_id_date_utc=str(row["instrument_id_date_utc"]),
                    decision_at_ns=int(row["resolution_as_of_ns"]),
                )
                decision_payload = decision.as_dict()
                if row["disposition"] != "ELIGIBLE":
                    decision_payload.update(
                        {
                            "foundation_eligible": False,
                            "long_eligible": False,
                            "short_eligible": False,
                            "status_disposition": "CAUSAL_BAR_INELIGIBLE",
                        }
                    )
                core = {
                    "actual_identity_hash": row["actual_identity_hash"],
                    "bar_event_at_ns": row["event_at_ns"],
                    "dataset": row["dataset"],
                    "decision_at_ns": row["resolution_as_of_ns"],
                    "instrument_id": row["instrument_id"],
                    "instrument_id_date_utc": row["instrument_id_date_utc"],
                    "market": row["market"],
                    "publisher_id": row["publisher_id"],
                    "source_causal_release_id": causal_receipt.release_id,
                    "source_causal_row_sha256": row["source_row_sha256"],
                    "source_market_state_release_id": market_state.receipt.release_id,
                    **decision_payload,
                }
                record = {**core, "status_eligibility_record_id": sha256_json(core)}
                handle.write(canonical_bytes(record) + b"\n")
                total += 1
                if decision.status_resolved:
                    resolved += 1
                else:
                    unresolved += 1
                if record["foundation_eligible"] is True:
                    eligible += 1
                else:
                    ineligible += 1
        handle.flush()
        os.fsync(handle.fileno())
    if total == 0 or total != resolved + unresolved or total != eligible + ineligible:
        raise IntegrityError("status eligibility denominator census is invalid")
    contract_core = {
        "eligible_rows": eligible,
        "ineligible_rows": ineligible,
        "market": market,
        "prediction_in_coverage_denominator_rows": total,
        "resolved_status_rows": resolved,
        "schema_version": STATUS_ELIGIBILITY_SCHEMA_VERSION,
        "source_causal_release_receipt_id": causal_receipt.receipt_id,
        "source_market_state_release_receipt_id": market_state.receipt.receipt_id,
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
        release_kind=STATUS_ELIGIBILITY_RELEASE_KIND,
        schema_version=STATUS_ELIGIBILITY_SCHEMA_VERSION,
        source_release_ids=(causal_receipt.release_id, market_state.receipt.release_id),
        metadata={
            "eligible_rows": eligible,
            "market": market,
            "status_eligibility_id": contract["status_eligibility_id"],
            "total_rows": total,
            "year": year,
        },
    )
    release = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_release(release, publisher.boundary)
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
    root = boundary.active_root / receipt.relative_root
    if (
        manifest.release_kind != STATUS_ELIGIBILITY_RELEASE_KIND
        or manifest.schema_version != STATUS_ELIGIBILITY_SCHEMA_VERSION
        or {entry.path for entry in manifest.files}
        != {"status_eligibility_contract.json", "status_eligibility_rows.jsonl"}
        or manifest.source_release_ids
        != tuple(sorted((causal_receipt.release_id, market_state_receipt.release_id)))
    ):
        raise IntegrityError("status eligibility release dependencies are invalid")
    contract = _canonical_object(
        root / "status_eligibility_contract.json",
        description="status eligibility contract",
    )
    core = {key: value for key, value in contract.items() if key != "status_eligibility_id"}
    total = resolved = eligible = unresolved = ineligible = 0
    required_row_fields = {
        "actual_identity_hash",
        "bar_event_at_ns",
        "dataset",
        "decision_at_ns",
        "foundation_eligible",
        "in_coverage_denominator",
        "instrument_id",
        "instrument_id_date_utc",
        "long_eligible",
        "market",
        "matched_status_record_id",
        "publisher_id",
        "short_eligible",
        "source_causal_release_id",
        "source_causal_row_sha256",
        "source_market_state_release_id",
        "status_disposition",
        "status_eligibility_record_id",
        "status_resolved",
        "status_ts_event_ns",
        "status_ts_recv_ns",
    }
    try:
        with (root / "status_eligibility_rows.jsonl").open("rb") as handle:
            for line in handle:
                row = json.loads(line.decode("utf-8"))
                if (
                    not isinstance(row, dict)
                    or set(row) != required_row_fields
                    or line != canonical_bytes(row) + b"\n"
                    or row["in_coverage_denominator"] is not True
                    or row["source_causal_release_id"] != causal_receipt.release_id
                    or row["source_market_state_release_id"]
                    != market_state_receipt.release_id
                    or row["status_eligibility_record_id"]
                    != sha256_json(
                        {
                            key: value
                            for key, value in row.items()
                            if key != "status_eligibility_record_id"
                        }
                    )
                ):
                    raise IntegrityError("status eligibility row is invalid")
                total += 1
                resolved += int(row["status_resolved"] is True)
                unresolved += int(row["status_resolved"] is False)
                eligible += int(row["foundation_eligible"] is True)
                ineligible += int(row["foundation_eligible"] is False)
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("status eligibility JSONL is invalid") from exc
    if (
        set(contract)
        != {
            "eligible_rows",
            "ineligible_rows",
            "market",
            "prediction_in_coverage_denominator_rows",
            "resolved_status_rows",
            "schema_version",
            "source_causal_release_receipt_id",
            "source_market_state_release_receipt_id",
            "statistics_feature_use",
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
        or total <= 0
        or total != contract["total_rows"]
        or total != contract["prediction_in_coverage_denominator_rows"]
        or resolved != contract["resolved_status_rows"]
        or unresolved != contract["unresolved_status_rows"]
        or eligible != contract["eligible_rows"]
        or ineligible != contract["ineligible_rows"]
        or total != resolved + unresolved
        or total != eligible + ineligible
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
    root = boundary.active_root / receipt.relative_root
    keys: set[tuple[str, int, int]] = set()
    with (root / "status_eligibility_rows.jsonl").open("rb") as handle:
        for line in handle:
            try:
                row = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                raise IntegrityError("status eligibility JSONL is invalid") from exc
            if row["foundation_eligible"] is True:
                key = (
                    str(row["actual_identity_hash"]),
                    int(row["bar_event_at_ns"]),
                    int(row["decision_at_ns"]),
                )
                if key in keys:
                    raise IntegrityError("status eligibility join key is duplicated")
                keys.add(key)
    if len(keys) != contract["eligible_rows"]:
        raise IntegrityError("status eligibility join-key census is invalid")
    return frozenset(keys)
