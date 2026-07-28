"""Immutable, approval-gated CME exchange-calendar contracts.

The existing session policy remains the authority for assigning a trade date.
This module supplies the separate point-in-time schedule authority for the
trading states within that date.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from .errors import ContractError, IntegrityError
from .time_contracts import require_utc


CAPTURE_RELEASE_KIND = "cme_trading_hours_capture"
CAPTURE_SCHEMA_VERSION = "1.0.0"
SCHEDULE_RECOVERY_CAPTURE_SCHEMA_VERSION = "1.1.0"
CALENDAR_RELEASE_KIND = "verified_exchange_calendar"
CALENDAR_SCHEMA_VERSION = "1.0.0"
INDEX_RELEASE_KIND = "exchange_calendar_index"
INDEX_SCHEMA_VERSION = "1.0.0"
COVERAGE_RELEASE_KIND = "foundation_calendar_coverage"
COVERAGE_SCHEMA_VERSION = "1.0.0"
ELIGIBILITY_RELEASE_KIND = "calendar_state_eligibility"
ELIGIBILITY_SCHEMA_VERSION = "1.0.0"
SERVICE_RESPONSE_SCHEMA = "cme_trading_hours_service_response/1.0.0"
CAPTURE_APPROVAL_SCHEMA = "cme_calendar_capture_approval/1.0.0"
CAPTURE_OPERATION = "CAPTURE_BOUNDED_PUBLIC_CME_TRADING_HOURS"
SCHEDULE_RECOVERY_APPROVAL_SCHEMA = (
    "cme_calendar_schedule_coverage_recovery_approval/1.0.0"
)
SCHEDULE_RECOVERY_OPERATION = (
    "CAPTURE_BOUNDED_PUBLIC_CME_SCHEDULE_COVERAGE_RECOVERY"
)
MAPPING_APPROVAL_SCHEMA = "cme_product_mapping_approval/1.0.0"
MAPPING_CANDIDATES_SCHEMA = "cme_product_mapping_candidates/1.0.0"
ACTIVATION_APPROVAL_SCHEMA = "cme_calendar_activation_approval/1.0.0"
ACTIVE_POINTER_SCHEMA = "active_exchange_calendar/1.0.0"
PARSER_VERSION = "cme_trading_hours_parser/1.0.0"

CME_TIMEZONE = "America/Chicago"
CME_VENUE = "CME Globex"
ALLOWED_STATES = frozenset({"CLOSED", "OPEN", "PAUSED", "PCP", "PREOPEN"})
NON_TRADING_STATES = ALLOWED_STATES - {"OPEN"}
_HASH = re.compile(r"^[0-9a-f]{64}$")
_UTC_SECOND = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_MARKET = re.compile(r"^[0-9A-Z]{1,16}$")
_EXPECTED_POLICY: dict[str, object] = {
    "activation": {
        "general_max_age_hours": 168,
        "holiday_finalization_window_days": 14,
        "holiday_max_age_hours": 72,
        "minimum_continuous_forward_days": 90,
    },
    "capture": {
        "bootstrap_max_requests": 96,
        "max_duration_seconds": 900,
        "max_total_bytes": 268_435_456,
        "retries": 0,
        "steady_state_max_requests": 40,
        "workers": 1,
    },
    "cme": {
        "allowed_content_types": ["application/json", "text/html"],
        "filters_url": (
            "https://www.cmegroup.com/services/trading-hours-filters?isProtected"
        ),
        "landing_page_url": "https://www.cmegroup.com/trading-hours.html",
        "schedule_url": (
            "https://www.cmegroup.com/services/trading-hours-by-product"
        ),
        "source_timezone": CME_TIMEZONE,
        "venue": CME_VENUE,
    },
    "contract_version": "2.0.0",
    "historical_backfill_policy": (
        "DBN_EMPIRICAL_OBSERVABILITY_NO_OFFICIAL_CALENDAR_CLAIM"
    ),
    "historical_observability_policy_path": (
        "configs/historical_observability_policy.json"
    ),
    "roles": {
        "current_forward": (
            "AUTHORITATIVE_CME_CALENDAR_FOR_COCKPIT_AND_FORWARD_SCHEDULING"
        ),
        "historical_research": (
            "IMMUTABLE_DATABENTO_DBN_EMPIRICAL_OBSERVABILITY"
        ),
    },
    "states": ["CLOSED", "OPEN", "PAUSED", "PCP", "PREOPEN"],
    "trade_date_window": {
        "end_local": "17:00:00",
        "start_day_offset": -1,
        "start_local": "17:00:00",
    },
}


def _read_canonical_object(path: Path, *, description: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is not readable JSON") from exc
    if (
        not isinstance(payload, dict)
        or raw != canonical_bytes(payload) + b"\n"
    ):
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def load_exchange_calendar_policy(path: Path) -> dict[str, object]:
    payload = _read_canonical_object(
        path, description="exchange-calendar policy"
    )
    if payload != _EXPECTED_POLICY:
        raise IntegrityError(
            "exchange-calendar policy differs from the implemented contract"
        )
    return payload


def _date(value: object, *, name: str) -> date:
    if type(value) is not str:
        raise IntegrityError(f"{name} must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise IntegrityError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != value:
        raise IntegrityError(f"{name} is not canonical")
    return parsed


def _utc(value: object, *, name: str) -> datetime:
    if type(value) is not str or _UTC_SECOND.fullmatch(value) is None:
        raise IntegrityError(f"{name} must be UTC to whole seconds")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _utc_text(value: datetime) -> str:
    instant = require_utc(value, "calendar instant").replace(microsecond=0)
    return instant.isoformat().replace("+00:00", "Z")


def _receipt(payload: object, *, name: str) -> VerifiedReleaseReceipt:
    if not isinstance(payload, Mapping):
        raise IntegrityError(f"{name} receipt is invalid")
    try:
        return VerifiedReleaseReceipt.from_dict(payload)
    except (ContractError, IntegrityError, TypeError) as exc:
        raise IntegrityError(f"{name} receipt is invalid") from exc


def approved_research_markets(universe_path: Path) -> tuple[str, ...]:
    payload = _read_canonical_object(
        universe_path, description="research universe contract"
    )
    tiers = payload.get("tiers")
    if not isinstance(tiers, list):
        raise IntegrityError("research universe tiers are invalid")
    markets: set[str] = set()
    for tier in tiers:
        if not isinstance(tier, dict) or not isinstance(tier.get("symbols"), list):
            raise IntegrityError("research universe tier schema is invalid")
        for market in tier["symbols"]:
            if type(market) is not str or _MARKET.fullmatch(market) is None:
                raise IntegrityError("research universe market is invalid")
            markets.add(market)
    result = tuple(sorted(markets))
    if len(result) != 41:
        raise IntegrityError("approved exchange-calendar universe is not exactly 41 markets")
    return result


@dataclass(frozen=True, order=True)
class TradingStateInterval:
    state: str
    starts_at_utc: datetime
    ends_at_utc: datetime

    def __post_init__(self) -> None:
        if self.state not in ALLOWED_STATES:
            raise ContractError("calendar interval state is unsupported")
        start = require_utc(self.starts_at_utc, "calendar interval start")
        end = require_utc(self.ends_at_utc, "calendar interval end")
        if start >= end or start.microsecond or end.microsecond:
            raise ContractError("calendar interval must be a positive whole-second range")

    def as_dict(self) -> dict[str, str]:
        return {
            "ends_at_utc": _utc_text(self.ends_at_utc),
            "starts_at_utc": _utc_text(self.starts_at_utc),
            "state": self.state,
        }


@dataclass(frozen=True)
class CalendarSession:
    market: str
    trade_date: date
    intervals: tuple[TradingStateInterval, ...]

    def __post_init__(self) -> None:
        if _MARKET.fullmatch(self.market) is None or not self.intervals:
            raise ContractError("calendar session identity or interval set is invalid")
        if self.intervals != tuple(
            sorted(self.intervals, key=lambda item: item.starts_at_utc)
        ):
            raise ContractError("calendar intervals are not canonically ordered")
        for left, right in zip(self.intervals, self.intervals[1:]):
            if left.ends_at_utc != right.starts_at_utc:
                raise ContractError("calendar intervals contain a gap or overlap")

    def state_at(self, instant: datetime) -> str:
        value = require_utc(instant, "calendar lookup instant")
        for interval in self.intervals:
            if interval.starts_at_utc <= value < interval.ends_at_utc:
                return interval.state
        raise ContractError("calendar instant is outside verified schedule coverage")


@dataclass(frozen=True)
class CalendarProduct:
    market: str
    cme_product_code: str
    cme_product_name: str
    product_group: str
    product_type: str
    venue: str

    def as_dict(self) -> dict[str, str]:
        return {
            "cme_product_code": self.cme_product_code,
            "cme_product_name": self.cme_product_name,
            "market": self.market,
            "product_group": self.product_group,
            "product_type": self.product_type,
            "venue": self.venue,
        }


@dataclass(frozen=True)
class VerifiedExchangeCalendar:
    receipt: VerifiedReleaseReceipt
    source_capture_receipt: VerifiedReleaseReceipt
    coverage_start: date
    coverage_end: date
    source_retrieved_at: datetime
    products: Mapping[str, CalendarProduct]
    sessions: Mapping[tuple[str, date], CalendarSession]
    holiday_notices: tuple[dict[str, str], ...]
    calendar_id: str
    predecessor_calendar_release_id: str | None
    boundary: RepoBoundary

    @classmethod
    def from_release(
        cls,
        receipt: VerifiedReleaseReceipt,
        *,
        boundary: RepoBoundary,
        expected_markets: Sequence[str] | None = None,
    ) -> "VerifiedExchangeCalendar":
        manifest = receipt.verify(boundary)
        if (
            receipt.phase != "reference"
            or manifest.release_kind != CALENDAR_RELEASE_KIND
            or manifest.schema_version != CALENDAR_SCHEMA_VERSION
            or manifest.files
            or set(manifest.embedded_documents) != {"exchange_calendar.json"}
            or set(manifest.metadata)
            != {
                "calendar_id",
                "coverage_end_trade_date",
                "coverage_start_trade_date",
                "mapping_approval_receipt_id",
                "parser_version",
                "source_capture_release_id",
            }
        ):
            raise IntegrityError("exchange-calendar release contract is invalid")
        raw_payload = receipt.embedded_document("exchange_calendar.json", boundary)
        if not isinstance(raw_payload, dict):
            raise IntegrityError("exchange-calendar document is invalid")
        payload = dict(raw_payload)
        expected_keys = {
            "calendar_id",
            "coverage_end_trade_date",
            "coverage_start_trade_date",
            "effective_from_trade_date",
            "effective_through_trade_date",
            "holiday_notices",
            "mapping_approval",
            "markets",
            "parser_version",
            "predecessor_calendar_release_id",
            "schema_version",
            "source_capture_receipt",
            "source_retrieved_at_utc",
            "source_timezone",
        }
        if set(payload) != expected_keys:
            raise IntegrityError("exchange-calendar schema is invalid")
        calendar_id = payload.pop("calendar_id", None)
        start = _date(payload.get("coverage_start_trade_date"), name="coverage start")
        end = _date(payload.get("coverage_end_trade_date"), name="coverage end")
        if (
            calendar_id != sha256_json(payload)
            or calendar_id != manifest.metadata["calendar_id"]
            or payload.get("schema_version") != CALENDAR_SCHEMA_VERSION
            or payload.get("parser_version") != PARSER_VERSION
            or payload.get("source_timezone") != CME_TIMEZONE
            or start > end
            or payload.get("effective_from_trade_date") != start.isoformat()
            or payload.get("effective_through_trade_date") != end.isoformat()
            or manifest.metadata["coverage_start_trade_date"] != start.isoformat()
            or manifest.metadata["coverage_end_trade_date"] != end.isoformat()
            or manifest.metadata["parser_version"] != PARSER_VERSION
        ):
            raise IntegrityError("exchange-calendar identity or effective range is invalid")
        predecessor = payload.get("predecessor_calendar_release_id")
        if predecessor is not None and (
            type(predecessor) is not str or _HASH.fullmatch(predecessor) is None
        ):
            raise IntegrityError("exchange-calendar predecessor is invalid")
        capture = _receipt(payload.get("source_capture_receipt"), name="calendar source")
        capture_payload = load_cme_capture(capture, boundary=boundary)
        if (
            manifest.metadata["source_capture_release_id"] != capture.release_id
            or capture.release_id not in manifest.source_release_ids
            or (
                predecessor is None
                and manifest.source_release_ids != (capture.release_id,)
            )
            or (
                predecessor is not None
                and manifest.source_release_ids
                != tuple(sorted((capture.release_id, predecessor)))
            )
        ):
            raise IntegrityError("exchange-calendar source closure is invalid")
        retrieved = _utc(
            payload.get("source_retrieved_at_utc"), name="source retrieval time"
        )
        if retrieved != _utc(
            capture_payload["retrieved_at_utc"],
            name="capture retrieval time",
        ):
            raise IntegrityError(
                "exchange-calendar source retrieval time is not exact"
            )
        raw_markets = payload.get("markets")
        if not isinstance(raw_markets, list) or not raw_markets:
            raise IntegrityError("exchange-calendar market set is invalid")
        products: dict[str, CalendarProduct] = {}
        sessions: dict[tuple[str, date], CalendarSession] = {}
        canonical_markets: list[dict[str, object]] = []
        for raw_market in raw_markets:
            if not isinstance(raw_market, dict) or set(raw_market) != {
                "cme_product_code",
                "cme_product_name",
                "market",
                "product_group",
                "product_type",
                "sessions",
                "venue",
            }:
                raise IntegrityError("exchange-calendar market schema is invalid")
            strings = {
                key: raw_market[key]
                for key in (
                    "cme_product_code",
                    "cme_product_name",
                    "market",
                    "product_group",
                    "product_type",
                    "venue",
                )
            }
            if any(type(value) is not str or not value for value in strings.values()):
                raise IntegrityError("exchange-calendar product identity is invalid")
            product = CalendarProduct(**strings)
            if (
                product.market in products
                or product.product_type != "FUTURES"
                or product.venue != CME_VENUE
                or _MARKET.fullmatch(product.market) is None
            ):
                raise IntegrityError("exchange-calendar product mapping is ambiguous")
            raw_sessions = raw_market["sessions"]
            if not isinstance(raw_sessions, list):
                raise IntegrityError("exchange-calendar sessions are invalid")
            canonical_sessions: list[dict[str, object]] = []
            observed_dates: list[date] = []
            for raw_session in raw_sessions:
                if not isinstance(raw_session, dict) or set(raw_session) != {
                    "intervals",
                    "trade_date",
                }:
                    raise IntegrityError("exchange-calendar session schema is invalid")
                trade_date = _date(raw_session["trade_date"], name="trade date")
                raw_intervals = raw_session["intervals"]
                if not isinstance(raw_intervals, list) or not raw_intervals:
                    raise IntegrityError("exchange-calendar interval set is invalid")
                intervals: list[TradingStateInterval] = []
                for raw_interval in raw_intervals:
                    if not isinstance(raw_interval, dict) or set(raw_interval) != {
                        "ends_at_utc",
                        "starts_at_utc",
                        "state",
                    }:
                        raise IntegrityError("exchange-calendar interval schema is invalid")
                    state = raw_interval["state"]
                    if type(state) is not str:
                        raise IntegrityError("exchange-calendar state is invalid")
                    intervals.append(
                        TradingStateInterval(
                            state,
                            _utc(raw_interval["starts_at_utc"], name="interval start"),
                            _utc(raw_interval["ends_at_utc"], name="interval end"),
                        )
                    )
                session = CalendarSession(
                    product.market, trade_date, tuple(intervals)
                )
                _verify_session_window(session)
                key = (product.market, trade_date)
                if key in sessions:
                    raise IntegrityError("exchange-calendar session is duplicated")
                sessions[key] = session
                observed_dates.append(trade_date)
                canonical_sessions.append(
                    {
                        "intervals": [item.as_dict() for item in session.intervals],
                        "trade_date": trade_date.isoformat(),
                    }
                )
            expected_dates = list(_dates_inclusive(start, end))
            if observed_dates != expected_dates:
                raise IntegrityError("exchange-calendar market coverage is incomplete")
            products[product.market] = product
            canonical_markets.append({**product.as_dict(), "sessions": canonical_sessions})
        if canonical_markets != raw_markets:
            raise IntegrityError("exchange-calendar market order is noncanonical")
        observed_markets = tuple(products)
        if observed_markets != tuple(sorted(observed_markets)):
            raise IntegrityError("exchange-calendar market order is invalid")
        if expected_markets is not None and observed_markets != tuple(
            sorted(expected_markets)
        ):
            raise IntegrityError("exchange-calendar does not cover the expected universe")
        mapping_payload = (
            payload.get("mapping_approval")
            if isinstance(payload.get("mapping_approval"), Mapping)
            else {}
        )
        mapping_capture_release_id = str(
            mapping_payload.get("capture_release_id", "")
        )
        capture_manifest = capture.verify(boundary)
        if (
            mapping_capture_release_id != capture.release_id
            and mapping_capture_release_id
            not in capture_manifest.source_release_ids
        ):
            raise IntegrityError(
                "exchange-calendar mapping is not in the direct capture lineage"
            )
        approved_mappings = validate_mapping_approval(
            mapping_payload,
            capture_release_id=mapping_capture_release_id,
            expected_markets=observed_markets,
        )
        expected_mappings = tuple(
            {
                "cme_product_code": products[market].cme_product_code,
                "market": market,
            }
            for market in observed_markets
        )
        mapping_approval = payload["mapping_approval"]
        assert isinstance(mapping_approval, Mapping)
        mapping_approved_at = _utc(
            mapping_approval["approved_at"],
            name="mapping approval time",
        )
        if (
            approved_mappings != expected_mappings
            or manifest.metadata["mapping_approval_receipt_id"]
            != mapping_approval["approval_receipt_id"]
            or (
                mapping_capture_release_id == capture.release_id
                and mapping_approved_at < retrieved
            )
        ):
            raise IntegrityError(
                "exchange-calendar product mapping approval is not exact"
            )
        notices = _validate_holiday_notices(payload.get("holiday_notices"))
        payload["calendar_id"] = calendar_id
        return cls(
            receipt=receipt,
            source_capture_receipt=capture,
            coverage_start=start,
            coverage_end=end,
            source_retrieved_at=retrieved,
            products=MappingProxyType(products),
            sessions=MappingProxyType(sessions),
            holiday_notices=notices,
            calendar_id=str(calendar_id),
            predecessor_calendar_release_id=predecessor,
            boundary=boundary,
        )

    def verify(self) -> None:
        rebuilt = type(self).from_release(
            self.receipt,
            boundary=self.boundary,
            expected_markets=tuple(self.products),
        )
        if rebuilt.calendar_id != self.calendar_id:
            raise IntegrityError("exchange calendar changed after verification")

    def state_at(
        self, market: str, instant: datetime, *, trade_date: date
    ) -> str:
        self.verify()
        try:
            session = self.sessions[(market, trade_date)]
        except KeyError as exc:
            raise ContractError("calendar lacks the requested market/trade date") from exc
        return session.state_at(instant)

    def trading_intervals(
        self, market: str, trade_date: date
    ) -> tuple[TradingStateInterval, ...]:
        self.verify()
        try:
            session = self.sessions[(market, trade_date)]
        except KeyError as exc:
            raise ContractError("calendar lacks the requested market/trade date") from exc
        return tuple(item for item in session.intervals if item.state == "OPEN")

    def require_coverage(
        self, markets: Sequence[str], start: date, end: date
    ) -> dict[str, object]:
        self.verify()
        normalized = tuple(sorted(set(markets)))
        if tuple(markets) != normalized or start > end:
            raise ContractError("calendar coverage request is noncanonical")
        missing = [
            f"{market}:{trade_date.isoformat()}"
            for market in normalized
            for trade_date in _dates_inclusive(start, end)
            if (market, trade_date) not in self.sessions
        ]
        if missing:
            raise ContractError("calendar coverage is incomplete")
        core: dict[str, object] = {
            "calendar_release_id": self.receipt.release_id,
            "coverage_end_trade_date": end.isoformat(),
            "coverage_start_trade_date": start.isoformat(),
            "markets": list(normalized),
        }
        return {**core, "coverage_proof_id": sha256_json(core)}


@dataclass(frozen=True)
class LoadedCalendarIndex:
    receipt: VerifiedReleaseReceipt
    segments: tuple[dict[str, object], ...]
    calendar_by_release_id: Mapping[str, VerifiedExchangeCalendar]
    index_id: str
    predecessor_index_release_id: str | None
    boundary: RepoBoundary

    def calendar_for(self, market: str, trade_date: date) -> VerifiedExchangeCalendar:
        for segment in self.segments:
            start = date.fromisoformat(str(segment["effective_from_trade_date"]))
            end = date.fromisoformat(str(segment["effective_through_trade_date"]))
            if start <= trade_date <= end:
                calendar = self.calendar_by_release_id[
                    str(segment["calendar_release_id"])
                ]
                if market not in calendar.products:
                    raise ContractError("calendar index segment lacks the market")
                return calendar
        raise ContractError("calendar index has no segment for the trade date")


@dataclass(frozen=True)
class LoadedFoundationCalendarCoverage:
    receipt: VerifiedReleaseReceipt
    index: LoadedCalendarIndex
    requirements: tuple[dict[str, object], ...]
    coverage_id: str
    boundary: RepoBoundary

    def calendar_for(self, market: str, trade_date: date) -> VerifiedExchangeCalendar:
        permitted = False
        for requirement in self.requirements:
            if (
                requirement["market"] == market
                and date.fromisoformat(str(requirement["start_trade_date"]))
                <= trade_date
                <= date.fromisoformat(str(requirement["end_trade_date"]))
            ):
                permitted = True
                break
        if not permitted:
            raise ContractError("foundation calendar coverage excludes the request")
        return self.index.calendar_for(market, trade_date)


def _dates_inclusive(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _verify_session_window(session: CalendarSession) -> None:
    zone = ZoneInfo(CME_TIMEZONE)
    expected_start = datetime.combine(
        session.trade_date - timedelta(days=1), time(17), zone
    ).astimezone(timezone.utc)
    expected_end = datetime.combine(
        session.trade_date, time(17), zone
    ).astimezone(timezone.utc)
    if (
        session.intervals[0].starts_at_utc != expected_start
        or session.intervals[-1].ends_at_utc != expected_end
    ):
        raise ContractError("calendar session does not cover the full trade-date window")


def _validate_holiday_notices(value: object) -> tuple[dict[str, str], ...]:
    if not isinstance(value, list):
        raise IntegrityError("calendar holiday notices are invalid")
    result: list[dict[str, str]] = []
    for raw in value:
        if not isinstance(raw, dict) or set(raw) != {"name", "trade_date"}:
            raise IntegrityError("calendar holiday notice schema is invalid")
        name = raw["name"]
        trade_date = _date(raw["trade_date"], name="holiday trade date")
        if type(name) is not str or not name:
            raise IntegrityError("calendar holiday notice name is invalid")
        result.append({"name": name, "trade_date": trade_date.isoformat()})
    if result != sorted(
        result, key=lambda item: (item["trade_date"], item["name"])
    ) or len({(item["trade_date"], item["name"]) for item in result}) != len(result):
        raise IntegrityError("calendar holiday notices are noncanonical")
    return tuple(result)


def _localize_unique(value: object) -> datetime:
    if type(value) is not str:
        raise IntegrityError("CME local timestamp is invalid")
    try:
        naive = datetime.fromisoformat(value)
        zone = ZoneInfo(CME_TIMEZONE)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise IntegrityError("CME local timestamp is invalid") from exc
    if naive.tzinfo is not None or naive.microsecond:
        raise IntegrityError("CME local timestamp must be a whole-second wall time")
    candidates: set[datetime] = set()
    for fold in (0, 1):
        aware = naive.replace(tzinfo=zone, fold=fold)
        round_trip = aware.astimezone(timezone.utc).astimezone(zone)
        if round_trip.replace(tzinfo=None) == naive:
            candidates.add(aware.astimezone(timezone.utc))
    if len(candidates) != 1:
        raise IntegrityError("CME local timestamp is ambiguous or nonexistent")
    return candidates.pop()


def validate_mapping_approval(
    payload: Mapping[str, object],
    *,
    capture_release_id: str,
    expected_markets: Sequence[str],
) -> tuple[dict[str, str], ...]:
    core_keys = {
        "approved_at",
        "capture_release_id",
        "mappings",
        "operation",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    if set(payload) != {*core_keys, "approval_receipt_id"}:
        raise IntegrityError("CME product-mapping approval schema is invalid")
    core = {key: payload[key] for key in core_keys}
    if (
        payload.get("schema_version") != MAPPING_APPROVAL_SCHEMA
        or payload.get("status") != "APPROVED"
        or payload.get("operation") != "APPROVE_CME_PRODUCT_MAPPING"
        or payload.get("capture_release_id") != capture_release_id
        or type(payload.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(payload["approved_at"])) is None
        or type(payload.get("user_authorization_id")) is not str
        or _HASH.fullmatch(str(payload["user_authorization_id"])) is None
        or payload.get("approval_receipt_id") != sha256_json(core)
    ):
        raise IntegrityError("CME product mapping lacks exact hash-bound approval")
    mappings = payload.get("mappings")
    if not isinstance(mappings, list):
        raise IntegrityError("CME product mappings are invalid")
    result: list[dict[str, str]] = []
    for raw in mappings:
        if not isinstance(raw, dict) or set(raw) != {
            "cme_product_code",
            "market",
        }:
            raise IntegrityError("CME product mapping schema is invalid")
        market = raw["market"]
        code = raw["cme_product_code"]
        if (
            type(market) is not str
            or type(code) is not str
            or _MARKET.fullmatch(market) is None
            or not code
        ):
            raise IntegrityError("CME product mapping value is invalid")
        result.append({"cme_product_code": code, "market": market})
    expected = tuple(sorted(expected_markets))
    if (
        tuple(item["market"] for item in result) != expected
        or len({item["cme_product_code"] for item in result}) != len(result)
    ):
        raise IntegrityError("CME product mapping is incomplete or ambiguous")
    return tuple(result)


def load_cme_capture(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    is_recovery = (
        manifest.schema_version
        == SCHEDULE_RECOVERY_CAPTURE_SCHEMA_VERSION
    )
    if (
        receipt.phase != "reference"
        or manifest.release_kind != CAPTURE_RELEASE_KIND
        or manifest.schema_version
        not in {
            CAPTURE_SCHEMA_VERSION,
            SCHEDULE_RECOVERY_CAPTURE_SCHEMA_VERSION,
        }
        or set(manifest.embedded_documents) != {"capture_receipt.json"}
        or set(manifest.metadata)
        != {
            "approval_receipt_id",
            "capture_id",
            "coverage_end_trade_date",
            "coverage_start_trade_date",
            "parser_version",
            "plan_id",
            "retrieved_at_utc",
        }
    ):
        raise IntegrityError("CME capture release contract is invalid")
    raw_payload = receipt.embedded_document("capture_receipt.json", boundary)
    if not isinstance(raw_payload, dict):
        raise IntegrityError("CME capture receipt is invalid")
    payload = dict(raw_payload)
    expected = {
        "approval_receipt_id",
        "bounds",
        "capture_approval",
        "capture_id",
        "coverage_end_trade_date",
        "coverage_start_trade_date",
        "mode",
        "elapsed_milliseconds",
        "parser_version",
        "plan_id",
        "predecessor_capture_release_id",
        "request_count",
        "responses",
        "retrieved_at_utc",
        "schema_version",
        "total_bytes",
    }
    if is_recovery:
        expected |= {
            "mapping_capture_release_id",
            "network_request_count",
            "reused_response_count",
        }
    if set(payload) != expected:
        raise IntegrityError("CME capture receipt schema is invalid")
    capture_id = payload.pop("capture_id", None)
    start = _date(payload.get("coverage_start_trade_date"), name="capture start")
    end = _date(payload.get("coverage_end_trade_date"), name="capture end")
    retrieved = _utc(payload.get("retrieved_at_utc"), name="capture retrieval time")
    predecessor = payload.get("predecessor_capture_release_id")
    bounds = payload.get("bounds")
    approval = payload.get("capture_approval")
    if is_recovery:
        maximum_requests = 56
        expected_bounds = {
            "allow_redirects": False,
            "max_duration_seconds": 900,
            "max_network_requests": 24,
            "max_output_responses": maximum_requests,
            "max_total_bytes": 268_435_456,
            "retries": 0,
            "workers": 1,
        }
        allowed_modes = {"SCHEDULE_COVERAGE_RECOVERY"}
        approval_schema = SCHEDULE_RECOVERY_APPROVAL_SCHEMA
        approval_operation = SCHEDULE_RECOVERY_OPERATION
    else:
        maximum_requests = {
            "BOOTSTRAP": 96,
            "PRODUCT_DISCOVERY": 3,
            "STEADY_STATE": 40,
        }.get(str(payload.get("mode")))
        expected_bounds = {
            "allow_redirects": False,
            "max_duration_seconds": 900,
            "max_requests": maximum_requests,
            "max_total_bytes": 268_435_456,
            "retries": 0,
            "workers": 1,
        }
        allowed_modes = {"BOOTSTRAP", "PRODUCT_DISCOVERY", "STEADY_STATE"}
        approval_schema = CAPTURE_APPROVAL_SCHEMA
        approval_operation = CAPTURE_OPERATION
    mapping_capture = payload.get("mapping_capture_release_id")
    recovery_source_ids = (
        tuple(sorted((str(predecessor), str(mapping_capture))))
        if is_recovery
        else ()
    )
    if (
        capture_id != sha256_json(payload)
        or capture_id != manifest.metadata["capture_id"]
        or payload.get("schema_version") != manifest.schema_version
        or payload.get("parser_version") != PARSER_VERSION
        or payload.get("mode") not in allowed_modes
        or maximum_requests is None
        or start > end
        or type(payload.get("plan_id")) is not str
        or _HASH.fullmatch(str(payload["plan_id"])) is None
        or type(payload.get("approval_receipt_id")) is not str
        or _HASH.fullmatch(str(payload["approval_receipt_id"])) is None
        or type(payload.get("request_count")) is not int
        or type(payload.get("total_bytes")) is not int
        or type(payload.get("elapsed_milliseconds")) is not int
        or payload["request_count"] <= 0
        or payload["total_bytes"] <= 0
        or payload["elapsed_milliseconds"] < 0
        or payload["elapsed_milliseconds"] > 900_000
        or bounds != expected_bounds
        or not isinstance(approval, dict)
        or payload["request_count"] > maximum_requests
        or payload["total_bytes"] > expected_bounds["max_total_bytes"]
        or predecessor is not None
        and (type(predecessor) is not str or _HASH.fullmatch(predecessor) is None)
        or is_recovery
        and (
            type(predecessor) is not str
            or _HASH.fullmatch(str(predecessor)) is None
            or
            type(mapping_capture) is not str
            or _HASH.fullmatch(str(mapping_capture)) is None
            or type(payload.get("network_request_count")) is not int
            or payload.get("network_request_count") != 24
            or type(payload.get("reused_response_count")) is not int
            or payload.get("reused_response_count") != 32
            or payload["request_count"]
            != payload["network_request_count"]
            + payload["reused_response_count"]
        )
        or manifest.metadata
        != {
            "approval_receipt_id": payload["approval_receipt_id"],
            "capture_id": capture_id,
            "coverage_end_trade_date": end.isoformat(),
            "coverage_start_trade_date": start.isoformat(),
            "parser_version": PARSER_VERSION,
            "plan_id": payload["plan_id"],
            "retrieved_at_utc": _utc_text(retrieved),
        }
        or manifest.source_release_ids
        != (
            recovery_source_ids
            if is_recovery
            else ((str(predecessor),) if predecessor is not None else ())
        )
    ):
        raise IntegrityError("CME capture identity or bounds are invalid")
    approval_keys = {
        "approval_receipt_id",
        "approved_at",
        "operation",
        "plan_id",
        "plan_sha256",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    approval_core = (
        {
            key: approval[key]
            for key in approval
            if key != "approval_receipt_id"
        }
        if isinstance(approval, dict)
        else {}
    )
    if (
        not isinstance(approval, dict)
        or set(approval) != approval_keys
        or approval.get("schema_version") != approval_schema
        or approval.get("operation") != approval_operation
        or approval.get("status") != "APPROVED"
        or approval.get("plan_id") != payload["plan_id"]
        or type(approval.get("plan_sha256")) is not str
        or _HASH.fullmatch(str(approval["plan_sha256"])) is None
        or type(approval.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(approval["approved_at"])) is None
        or type(approval.get("user_authorization_id")) is not str
        or _HASH.fullmatch(str(approval["user_authorization_id"])) is None
        or approval.get("approval_receipt_id") != sha256_json(approval_core)
        or approval.get("approval_receipt_id")
        != payload["approval_receipt_id"]
        or _utc(approval["approved_at"], name="capture approval time")
        > retrieved
    ):
        raise IntegrityError("CME capture approval is not durably hash-bound")
    responses = payload.get("responses")
    if not isinstance(responses, list) or len(responses) != payload["request_count"]:
        raise IntegrityError("CME capture response census is invalid")
    entries = {entry.logical_path: entry for entry in manifest.files}
    total = 0
    observed_paths: list[str] = []
    request_ids: list[str] = []
    request_kinds: list[str] = []
    reused_count = (
        int(payload["reused_response_count"]) if is_recovery else 0
    )
    for ordinal, raw in enumerate(responses):
        if not isinstance(raw, dict) or set(raw) != {
            "content_type",
            "logical_path",
            "received_at_utc",
            "request_id",
            "request_kind",
            "safe_headers",
            "sha256",
            "size",
            "status_code",
            "url",
        }:
            raise IntegrityError("CME capture response schema is invalid")
        logical = raw["logical_path"]
        if (
            type(logical) is not str
            or logical not in entries
            or raw["status_code"] != 200
            or type(raw["request_id"]) is not str
            or type(raw["request_kind"]) is not str
            or raw["request_kind"] not in {"FILTERS", "LANDING_PAGE", "SCHEDULE"}
            or type(raw["content_type"]) is not str
            or raw["content_type"]
            != (
                "text/html"
                if raw["request_kind"] == "LANDING_PAGE"
                else "application/json"
            )
            or type(raw["safe_headers"]) is not dict
            or any(
                key not in {"cache-control", "content-type", "date", "etag", "last-modified"}
                or type(value) is not str
                for key, value in raw["safe_headers"].items()
            )
            or type(raw["size"]) is not int
            or raw["size"] != entries[logical].size
            or raw["sha256"] != entries[logical].sha256
            or (
                ordinal >= reused_count
                and _utc(
                    raw["received_at_utc"],
                    name="response retrieval time",
                )
                < retrieved - timedelta(minutes=20)
            )
        ):
            raise IntegrityError("CME capture response identity is invalid")
        total += raw["size"]
        observed_paths.append(logical)
        request_ids.append(str(raw["request_id"]))
        request_kinds.append(str(raw["request_kind"]))
    if (
        total != payload["total_bytes"]
        or sorted(observed_paths) != sorted(entries)
        or len(set(observed_paths)) != len(observed_paths)
        or len(set(request_ids)) != len(request_ids)
        or request_kinds.count("LANDING_PAGE") != 1
        or request_kinds.count("FILTERS") != 1
        or request_kinds.count("SCHEDULE") < 1
    ):
        raise IntegrityError("CME capture file closure is invalid")
    if is_recovery:
        predecessor_path = (
            boundary.active_root
            / "manifests"
            / "data_releases"
            / "reference"
            / f"{predecessor}.json"
        )
        predecessor_receipt = VerifiedReleaseReceipt.from_manifest(
            predecessor_path, boundary
        )
        predecessor_manifest = predecessor_receipt.verify(boundary)
        predecessor_capture = load_cme_capture(
            predecessor_receipt, boundary=boundary
        )
        predecessor_responses = predecessor_capture.get("responses")
        if (
            predecessor_receipt.schema_version != CAPTURE_SCHEMA_VERSION
            or mapping_capture not in predecessor_manifest.source_release_ids
            or not isinstance(predecessor_responses, list)
            or reused_count != len(predecessor_responses)
            or responses[:reused_count] != predecessor_responses
            or start
            != _date(
                predecessor_capture["coverage_start_trade_date"],
                name="predecessor capture start",
            )
            or end
            <= _date(
                predecessor_capture["coverage_end_trade_date"],
                name="predecessor capture end",
            )
        ):
            raise IntegrityError(
                "CME schedule recovery source closure is invalid"
            )
    payload["capture_id"] = capture_id
    return payload


def _response_page_number(url: object) -> int:
    if type(url) is not str:
        raise IntegrityError("CME schedule response URL is invalid")
    raw_query = urllib.parse.urlparse(url).query
    normalized_fields: list[str] = []
    for field in raw_query.split("&"):
        if field == "isProtected":
            normalized_fields.append("isProtected=")
        elif "=" in field:
            normalized_fields.append(field)
        else:
            raise IntegrityError("CME schedule response query is malformed")
    try:
        query = urllib.parse.parse_qs(
            "&".join(normalized_fields),
            keep_blank_values=True,
            strict_parsing=True,
        )
    except ValueError as exc:
        raise IntegrityError("CME schedule response query is malformed") from exc
    if query.get("isProtected") != [""]:
        raise IntegrityError("CME schedule response protection flag is invalid")
    pages = query.get("pageNumber")
    if pages is None or len(pages) != 1:
        raise IntegrityError("CME schedule response pagination is absent")
    try:
        page = int(pages[0])
    except ValueError as exc:
        raise IntegrityError("CME schedule response page is invalid") from exc
    if page <= 0 or str(page) != pages[0]:
        raise IntegrityError("CME schedule response page is noncanonical")
    return page


def generate_mapping_candidates(
    capture_receipt: VerifiedReleaseReceipt,
    *,
    boundary: RepoBoundary,
    expected_markets: Sequence[str],
) -> dict[str, object]:
    capture = load_cme_capture(capture_receipt, boundary=boundary)
    products_by_code: dict[str, dict[str, object]] = {}
    notices: set[tuple[str, str]] = set()
    responses = capture["responses"]
    assert isinstance(responses, list)
    schedule_count = 0
    for response in responses:
        assert isinstance(response, dict)
        if response["request_kind"] == "FILTERS":
            path = capture_receipt.resolve_file(
                str(response["logical_path"]), boundary
            )
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, ValueError) as exc:
                raise IntegrityError("CME filters response is not valid JSON") from exc
            _merge_filters_response(raw, holiday_notices=notices)
            continue
        if response["request_kind"] != "SCHEDULE":
            continue
        schedule_count += 1
        path = capture_receipt.resolve_file(str(response["logical_path"]), boundary)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError("CME schedule response is not valid JSON") from exc
        _merge_service_response(
            raw,
            products_by_code=products_by_code,
            holiday_notices=notices,
            expected_page_number=_response_page_number(response["url"]),
            maximum_page_number=1,
        )
    if schedule_count == 0 or not products_by_code:
        raise IntegrityError("CME capture contains no mapping candidates")
    products = [
        {
            "cme_product_code": code,
            "cme_product_name": str(product["cme_product_name"]),
            "product_group": str(product["product_group"]),
            "product_type": "FUTURES",
            "venue": CME_VENUE,
        }
        for code, product in sorted(products_by_code.items())
    ]
    core: dict[str, object] = {
        "capture_release_id": capture_receipt.release_id,
        "expected_markets": list(tuple(sorted(expected_markets))),
        "products": products,
        "schema_version": MAPPING_CANDIDATES_SCHEMA,
        "status": "REVIEW_REQUIRED_NO_MAPPING_AUTHORITY",
    }
    return {**core, "mapping_candidates_id": sha256_json(core)}


def publish_verified_exchange_calendar(
    *,
    capture_receipt: VerifiedReleaseReceipt,
    mapping_approval: Mapping[str, object],
    expected_markets: Sequence[str],
    publisher: AtomicPublisher,
    predecessor_calendar_receipt: VerifiedReleaseReceipt | None = None,
) -> VerifiedReleaseReceipt:
    capture = load_cme_capture(capture_receipt, boundary=publisher.boundary)
    manifest = capture_receipt.verify(publisher.boundary)
    mapping_capture_release_id = str(
        mapping_approval.get("capture_release_id", "")
    )
    if (
        mapping_capture_release_id != capture_receipt.release_id
        and mapping_capture_release_id not in manifest.source_release_ids
    ):
        raise IntegrityError(
            "CME product mapping is not in the direct capture lineage"
        )
    mappings = validate_mapping_approval(
        mapping_approval,
        capture_release_id=mapping_capture_release_id,
        expected_markets=expected_markets,
    )
    mapped_by_code = {
        item["cme_product_code"]: item["market"] for item in mappings
    }
    products_by_code: dict[str, dict[str, object]] = {}
    holiday_notices: set[tuple[str, str]] = set()
    responses = capture["responses"]
    assert isinstance(responses, list)
    schedule_count = 0
    for response in responses:
        assert isinstance(response, dict)
        if response["request_kind"] == "FILTERS":
            path = capture_receipt.resolve_file(
                str(response["logical_path"]), publisher.boundary
            )
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, ValueError) as exc:
                raise IntegrityError("CME filters response is not valid JSON") from exc
            _merge_filters_response(raw, holiday_notices=holiday_notices)
            continue
        if response["request_kind"] != "SCHEDULE":
            continue
        schedule_count += 1
        path = capture_receipt.resolve_file(
            str(response["logical_path"]), publisher.boundary
        )
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise IntegrityError("CME schedule response is not valid JSON") from exc
        _merge_service_response(
            raw,
            products_by_code=products_by_code,
            holiday_notices=holiday_notices,
            expected_page_number=_response_page_number(response["url"]),
            maximum_page_number=1,
        )
    if schedule_count == 0:
        raise IntegrityError("CME capture contains no schedule response")
    missing_codes = sorted(set(mapped_by_code) - set(products_by_code))
    if missing_codes:
        raise IntegrityError("CME schedule response lacks an approved product mapping")
    start = _date(capture["coverage_start_trade_date"], name="capture start")
    end = _date(capture["coverage_end_trade_date"], name="capture end")
    markets: list[dict[str, object]] = []
    for code, market in sorted(mapped_by_code.items(), key=lambda item: item[1]):
        raw_product = products_by_code[code]
        sessions_by_date = raw_product["sessions"]
        assert isinstance(sessions_by_date, dict)
        sessions: list[dict[str, object]] = []
        for trade_date in _dates_inclusive(start, end):
            raw_intervals = _product_intervals_for_date(
                raw_product, trade_date=trade_date
            )
            if raw_intervals is None:
                raise IntegrityError(
                    "CME schedule response does not continuously cover every market/date"
                )
            intervals: list[TradingStateInterval] = []
            for raw_interval in raw_intervals:
                assert isinstance(raw_interval, dict)
                state = str(raw_interval["state"]).upper()
                if state not in ALLOWED_STATES:
                    raise IntegrityError("CME schedule response contains an unknown state")
                intervals.append(
                    TradingStateInterval(
                        state=state,
                        starts_at_utc=_localize_unique(raw_interval["starts_at_local"]),
                        ends_at_utc=_localize_unique(raw_interval["ends_at_local"]),
                    )
                )
            session = CalendarSession(market, trade_date, tuple(intervals))
            _verify_session_window(session)
            sessions.append(
                {
                    "intervals": [item.as_dict() for item in session.intervals],
                    "trade_date": trade_date.isoformat(),
                }
            )
        markets.append(
            {
                "cme_product_code": code,
                "cme_product_name": raw_product["cme_product_name"],
                "market": market,
                "product_group": raw_product["product_group"],
                "product_type": "FUTURES",
                "sessions": sessions,
                "venue": CME_VENUE,
            }
        )
    predecessor_id = (
        predecessor_calendar_receipt.release_id
        if predecessor_calendar_receipt is not None
        else None
    )
    if predecessor_calendar_receipt is not None:
        VerifiedExchangeCalendar.from_release(
            predecessor_calendar_receipt,
            boundary=publisher.boundary,
            expected_markets=expected_markets,
        )
    core: dict[str, object] = {
        "coverage_end_trade_date": end.isoformat(),
        "coverage_start_trade_date": start.isoformat(),
        "effective_from_trade_date": start.isoformat(),
        "effective_through_trade_date": end.isoformat(),
        "holiday_notices": [
            {"name": name, "trade_date": trade_date}
            for trade_date, name in sorted(holiday_notices)
        ],
        "mapping_approval": dict(mapping_approval),
        "markets": markets,
        "parser_version": PARSER_VERSION,
        "predecessor_calendar_release_id": predecessor_id,
        "schema_version": CALENDAR_SCHEMA_VERSION,
        "source_capture_receipt": capture_receipt.as_dict(),
        "source_retrieved_at_utc": capture["retrieved_at_utc"],
        "source_timezone": CME_TIMEZONE,
    }
    payload = {**core, "calendar_id": sha256_json(core)}
    stage = publisher.create_stage("exchange_calendar")
    source_release_ids = [capture_receipt.release_id]
    if predecessor_id is not None:
        source_release_ids.append(predecessor_id)
    release = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CALENDAR_RELEASE_KIND,
        schema_version=CALENDAR_SCHEMA_VERSION,
        logical_paths={},
        source_release_ids=tuple(sorted(source_release_ids)),
        embedded_documents={"exchange_calendar.json": payload},
        metadata={
            "calendar_id": payload["calendar_id"],
            "coverage_end_trade_date": end.isoformat(),
            "coverage_start_trade_date": start.isoformat(),
            "mapping_approval_receipt_id": mapping_approval[
                "approval_receipt_id"
            ],
            "parser_version": PARSER_VERSION,
            "source_capture_release_id": capture_receipt.release_id,
        },
    )
    path = publisher.publish(stage, release)
    receipt = VerifiedReleaseReceipt.from_manifest(path, publisher.boundary)
    VerifiedExchangeCalendar.from_release(
        receipt, boundary=publisher.boundary, expected_markets=expected_markets
    )
    return receipt


_NATIVE_EVENT_STATES = {
    "closed": "CLOSED",
    "open": "OPEN",
    "paused": "PAUSED",
    "pcp": "PCP",
    "preopen": "PREOPEN",
}
_NATIVE_EVENT_TIME = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")


def _merge_filters_response(
    payload: object, *, holiday_notices: set[tuple[str, str]]
) -> None:
    if (
        not isinstance(payload, dict)
        or set(payload) != {"error", "filters"}
        or payload["error"] is not None
        or not isinstance(payload["filters"], dict)
    ):
        raise IntegrityError("CME filters response schema drifted")
    filters = payload["filters"]
    if set(filters) != {
        "assetClasses",
        "checkboxOptions",
        "dateTime",
        "exchanges",
        "holidays",
    }:
        raise IntegrityError("CME filters response schema drifted")
    if (
        not isinstance(filters["assetClasses"], list)
        or not isinstance(filters["checkboxOptions"], list)
        or type(filters["dateTime"]) is not str
        or not isinstance(filters["exchanges"], list)
        or not isinstance(filters["holidays"], list)
    ):
        raise IntegrityError("CME filters response field type is invalid")
    try:
        captured_at = datetime.fromisoformat(
            filters["dateTime"].replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise IntegrityError("CME filters response timestamp is invalid") from exc
    if captured_at.tzinfo != timezone.utc:
        raise IntegrityError("CME filters response timestamp is not UTC")
    for raw in filters["holidays"]:
        if not isinstance(raw, dict) or set(raw) != {
            "date",
            "endPeriod",
            "holiday",
            "startPeriod",
        }:
            raise IntegrityError("CME filters holiday schema drifted")
        holiday = _date(raw["date"], name="CME holiday date")
        _date(raw["startPeriod"], name="CME holiday start")
        _date(raw["endPeriod"], name="CME holiday end")
        name = raw["holiday"]
        if type(name) is not str or not name.strip():
            raise IntegrityError("CME filters holiday is invalid")
        holiday_notices.add((holiday.isoformat(), name.strip()))


def _merge_native_service_response(
    payload: Mapping[str, object],
    *,
    products_by_code: dict[str, dict[str, object]],
    expected_page_number: int,
    maximum_page_number: int,
) -> None:
    if set(payload) != {"products", "props"}:
        raise IntegrityError("CME native schedule response schema drifted")
    props = payload["props"]
    products = payload["products"]
    if (
        not isinstance(props, dict)
        or set(props)
        != {
            "hasEvents",
            "pageNumber",
            "pageSize",
            "pageTotal",
            "sortAsc",
            "total",
        }
        or not isinstance(products, list)
    ):
        raise IntegrityError("CME native schedule response schema drifted")
    if (
        type(props["hasEvents"]) is not bool
        or type(props["pageNumber"]) is not int
        or type(props["pageSize"]) is not int
        or type(props["pageTotal"]) is not int
        or props["sortAsc"] != "true"
        or type(props["total"]) is not int
        or props["pageNumber"] != expected_page_number
        or props["pageNumber"] <= 0
        or props["pageSize"] <= 0
        or props["pageTotal"] < props["pageNumber"]
        or props["pageTotal"] > maximum_page_number
        or props["total"] < len(products)
        or len(products) > props["pageSize"]
        or (props["pageTotal"] == 1 and props["total"] != len(products))
        or props["hasEvents"] is not bool(products)
    ):
        raise IntegrityError("CME native schedule pagination overflowed")
    for raw_product in products:
        if not isinstance(raw_product, dict) or set(raw_product) != {
            "foi",
            "globex",
            "id",
            "name",
            "prodGroup",
            "tradingHours",
            "url",
        }:
            raise IntegrityError("CME native product schema drifted")
        if (
            raw_product["foi"] != "Futures"
            or type(raw_product["globex"]) is not str
            or not raw_product["globex"]
            or type(raw_product["id"]) is not int
            or raw_product["id"] <= 0
            or type(raw_product["name"]) is not str
            or not raw_product["name"]
            or type(raw_product["prodGroup"]) is not str
            or not raw_product["prodGroup"]
            or type(raw_product["url"]) is not str
            or not raw_product["url"].startswith("/markets/")
            or urllib.parse.urlparse(raw_product["url"]).path
            != raw_product["url"]
            or not isinstance(raw_product["tradingHours"], dict)
        ):
            raise IntegrityError("CME native product identity is invalid")
        trading_hours = raw_product["tradingHours"]
        if (
            set(trading_hours) != {"eventCount", "schedules"}
            or type(trading_hours["eventCount"]) is not int
            or trading_hours["eventCount"] < 0
            or not isinstance(trading_hours["schedules"], list)
        ):
            raise IntegrityError("CME native trading-hours schema drifted")
        code = str(raw_product["id"])
        existing = products_by_code.setdefault(
            code,
            {
                "cme_product_name": raw_product["name"],
                "native_events": {},
                "native_source_dates": set(),
                "product_group": raw_product["prodGroup"],
                "sessions": {},
            },
        )
        if (
            existing["cme_product_name"] != raw_product["name"]
            or existing["product_group"] != raw_product["prodGroup"]
            or not isinstance(existing.get("native_events"), dict)
            or not isinstance(existing.get("native_source_dates"), set)
        ):
            raise IntegrityError("CME native product identity changed within capture")
        native_events = existing["native_events"]
        source_dates = existing["native_source_dates"]
        assert isinstance(native_events, dict)
        assert isinstance(source_dates, set)
        schedules_with_events = 0
        for schedule in trading_hours["schedules"]:
            if not isinstance(schedule, dict) or set(schedule) != {
                "eventDate",
                "events",
                "groupCode",
            }:
                raise IntegrityError("CME native schedule schema drifted")
            event_date = _date(
                schedule["eventDate"], name="CME native event date"
            )
            if (
                schedule["groupCode"]
                not in {raw_product["globex"], raw_product["prodGroup"]}
                or not isinstance(schedule["events"], list)
            ):
                raise IntegrityError("CME native schedule identity is invalid")
            source_dates.add(event_date.isoformat())
            if schedule["events"]:
                schedules_with_events += 1
            for event in schedule["events"]:
                if not isinstance(event, dict) or set(event) != {
                    "eventTime",
                    "marketEventType",
                    "tradingDate",
                }:
                    raise IntegrityError("CME native event schema drifted")
                event_time = event["eventTime"]
                event_type = event["marketEventType"]
                trading_date = _date(
                    event["tradingDate"], name="CME native trading date"
                )
                if (
                    type(event_time) is not str
                    or _NATIVE_EVENT_TIME.fullmatch(event_time) is None
                    or type(event_type) is not str
                    or event_type not in _NATIVE_EVENT_STATES
                ):
                    raise IntegrityError("CME native event value is invalid")
                timestamp = f"{event_date.isoformat()}T{event_time}:00"
                value = {
                    "state": _NATIVE_EVENT_STATES[event_type],
                    "trading_date": trading_date.isoformat(),
                }
                observed = native_events.setdefault(timestamp, value)
                if observed != value:
                    raise IntegrityError(
                        "CME native event changed within one capture"
                    )
        if schedules_with_events != trading_hours["eventCount"]:
            raise IntegrityError("CME native event census is inconsistent")


def _product_intervals_for_date(
    raw_product: Mapping[str, object], *, trade_date: date
) -> list[dict[str, str]] | None:
    sessions = raw_product.get("sessions")
    if not isinstance(sessions, dict):
        raise IntegrityError("CME product sessions are invalid")
    raw_intervals = sessions.get(trade_date.isoformat())
    if isinstance(raw_intervals, list):
        return raw_intervals
    native_events = raw_product.get("native_events")
    source_dates = raw_product.get("native_source_dates")
    if not isinstance(native_events, dict) or not isinstance(source_dates, set):
        return None
    previous = trade_date - timedelta(days=1)
    if not {
        previous.isoformat(),
        trade_date.isoformat(),
    }.issubset(source_dates):
        return None
    start = datetime.fromisoformat(f"{previous.isoformat()}T17:00:00")
    end = datetime.fromisoformat(f"{trade_date.isoformat()}T17:00:00")
    observed: list[tuple[datetime, str]] = []
    for timestamp, event in native_events.items():
        if type(timestamp) is not str or not isinstance(event, dict):
            raise IntegrityError("CME native event store is invalid")
        instant = datetime.fromisoformat(timestamp)
        state = event.get("state")
        if type(state) is not str or state not in ALLOWED_STATES:
            raise IntegrityError("CME native event store is invalid")
        observed.append((instant, state))
    observed.sort()
    state = "CLOSED"
    for instant, candidate in observed:
        if instant <= start:
            state = candidate
        else:
            break
    cursor = start
    intervals: list[dict[str, str]] = []
    for instant, candidate in observed:
        if instant <= start or instant >= end:
            continue
        if candidate == state:
            continue
        intervals.append(
            {
                "ends_at_local": instant.isoformat(),
                "starts_at_local": cursor.isoformat(),
                "state": state,
            }
        )
        cursor = instant
        state = candidate
    intervals.append(
        {
            "ends_at_local": end.isoformat(),
            "starts_at_local": cursor.isoformat(),
            "state": state,
        }
    )
    return intervals


def _merge_service_response(
    payload: object,
    *,
    products_by_code: dict[str, dict[str, object]],
    holiday_notices: set[tuple[str, str]],
    expected_page_number: int,
    maximum_page_number: int,
) -> None:
    if isinstance(payload, dict) and set(payload) == {"products", "props"}:
        _merge_native_service_response(
            payload,
            products_by_code=products_by_code,
            expected_page_number=expected_page_number,
            maximum_page_number=maximum_page_number,
        )
        return
    if not isinstance(payload, dict) or set(payload) != {
        "holiday_notices",
        "pagination",
        "products",
        "schema_version",
        "timezone",
    }:
        raise IntegrityError("CME schedule response schema drifted")
    if (
        payload["schema_version"] != SERVICE_RESPONSE_SCHEMA
        or payload["timezone"] != CME_TIMEZONE
        or not isinstance(payload["pagination"], dict)
        or not isinstance(payload["products"], list)
        or not isinstance(payload["holiday_notices"], list)
    ):
        raise IntegrityError("CME schedule response version or timezone is unsupported")
    pagination = payload["pagination"]
    if (
        set(pagination) != {"page_number", "total_pages"}
        or type(pagination["page_number"]) is not int
        or type(pagination["total_pages"]) is not int
        or pagination["page_number"] != expected_page_number
        or pagination["total_pages"] < pagination["page_number"]
        or pagination["total_pages"] > maximum_page_number
    ):
        raise IntegrityError("CME schedule response pagination overflowed")
    for notice in payload["holiday_notices"]:
        if not isinstance(notice, dict) or set(notice) != {"name", "trade_date"}:
            raise IntegrityError("CME holiday notice schema drifted")
        trade_date = _date(notice["trade_date"], name="holiday trade date")
        name = notice["name"]
        if type(name) is not str or not name:
            raise IntegrityError("CME holiday notice is invalid")
        holiday_notices.add((trade_date.isoformat(), name))
    for raw_product in payload["products"]:
        if not isinstance(raw_product, dict) or set(raw_product) != {
            "cme_product_code",
            "cme_product_name",
            "product_group",
            "product_type",
            "sessions",
            "venue",
        }:
            raise IntegrityError("CME product schema drifted")
        strings = {
            key: raw_product[key]
            for key in (
                "cme_product_code",
                "cme_product_name",
                "product_group",
                "product_type",
                "venue",
            )
        }
        if any(type(value) is not str or not value for value in strings.values()):
            raise IntegrityError("CME product identity is invalid")
        if strings["product_type"].upper() != "FUTURES" or strings["venue"] != CME_VENUE:
            raise IntegrityError("CME response contains a non-futures or wrong-venue product")
        raw_sessions = raw_product["sessions"]
        if not isinstance(raw_sessions, list):
            raise IntegrityError("CME product sessions are invalid")
        code = strings["cme_product_code"]
        existing = products_by_code.setdefault(
            code,
            {
                "cme_product_name": strings["cme_product_name"],
                "product_group": strings["product_group"],
                "sessions": {},
            },
        )
        if (
            existing["cme_product_name"] != strings["cme_product_name"]
            or existing["product_group"] != strings["product_group"]
        ):
            raise IntegrityError("CME product identity changed within one capture")
        sessions = existing["sessions"]
        assert isinstance(sessions, dict)
        for raw_session in raw_sessions:
            if not isinstance(raw_session, dict) or set(raw_session) != {
                "intervals",
                "trade_date",
            }:
                raise IntegrityError("CME session schema drifted")
            trade_date = _date(raw_session["trade_date"], name="CME trade date")
            intervals = raw_session["intervals"]
            if not isinstance(intervals, list) or not intervals:
                raise IntegrityError("CME session intervals are invalid")
            canonical: list[dict[str, str]] = []
            for raw_interval in intervals:
                if not isinstance(raw_interval, dict) or set(raw_interval) != {
                    "ends_at_local",
                    "starts_at_local",
                    "state",
                }:
                    raise IntegrityError("CME interval schema drifted")
                if any(type(raw_interval[key]) is not str for key in raw_interval):
                    raise IntegrityError("CME interval field type is invalid")
                state = raw_interval["state"].upper()
                if state not in ALLOWED_STATES:
                    raise IntegrityError("CME interval state is unsupported")
                canonical.append(
                    {
                        "ends_at_local": raw_interval["ends_at_local"],
                        "starts_at_local": raw_interval["starts_at_local"],
                        "state": state,
                    }
                )
            key = trade_date.isoformat()
            prior = sessions.get(key)
            if prior is not None and prior != canonical:
                raise IntegrityError("overlapping CME responses conflict")
            sessions[key] = canonical


def diff_exchange_calendars(
    predecessor: VerifiedExchangeCalendar | None,
    successor: VerifiedExchangeCalendar,
) -> dict[str, object]:
    changed: list[dict[str, str]] = []
    if predecessor is None:
        for market in successor.products:
            for trade_date in _dates_inclusive(
                successor.coverage_start, successor.coverage_end
            ):
                changed.append(
                    {
                        "change": "ADDED",
                        "market": market,
                        "trade_date": trade_date.isoformat(),
                    }
                )
    else:
        markets = sorted(set(predecessor.products) | set(successor.products))
        start = min(predecessor.coverage_start, successor.coverage_start)
        end = max(predecessor.coverage_end, successor.coverage_end)
        for market in markets:
            for trade_date in _dates_inclusive(start, end):
                left = predecessor.sessions.get((market, trade_date))
                right = successor.sessions.get((market, trade_date))
                left_payload = (
                    [item.as_dict() for item in left.intervals]
                    if left is not None
                    else None
                )
                right_payload = (
                    [item.as_dict() for item in right.intervals]
                    if right is not None
                    else None
                )
                if left_payload == right_payload:
                    continue
                changed.append(
                    {
                        "change": (
                            "ADDED"
                            if left is None
                            else "REMOVED"
                            if right is None
                            else "CHANGED"
                        ),
                        "market": market,
                        "trade_date": trade_date.isoformat(),
                    }
                )
    core: dict[str, object] = {
        "changed_market_dates": changed,
        "predecessor_calendar_release_id": (
            predecessor.receipt.release_id if predecessor is not None else None
        ),
        "successor_calendar_release_id": successor.receipt.release_id,
    }
    return {**core, "diff_report_id": sha256_json(core)}


def validate_activation_approval(
    payload: Mapping[str, object],
    *,
    candidate_calendar: VerifiedExchangeCalendar,
    predecessor_index_release_id: str | None,
    diff_report_id: str,
) -> str:
    core_keys = {
        "approved_at",
        "candidate_calendar_release_id",
        "diff_report_id",
        "operation",
        "predecessor_index_release_id",
        "schema_version",
        "status",
        "user_authorization_id",
    }
    if set(payload) != {*core_keys, "approval_receipt_id"}:
        raise IntegrityError("calendar activation approval schema is invalid")
    core = {key: payload[key] for key in core_keys}
    if (
        payload.get("schema_version") != ACTIVATION_APPROVAL_SCHEMA
        or payload.get("status") != "APPROVED"
        or payload.get("operation") != "ACTIVATE_CME_EXCHANGE_CALENDAR"
        or payload.get("candidate_calendar_release_id")
        != candidate_calendar.receipt.release_id
        or payload.get("predecessor_index_release_id")
        != predecessor_index_release_id
        or payload.get("diff_report_id") != diff_report_id
        or type(payload.get("approved_at")) is not str
        or _UTC_SECOND.fullmatch(str(payload["approved_at"])) is None
        or type(payload.get("user_authorization_id")) is not str
        or _HASH.fullmatch(str(payload["user_authorization_id"])) is None
        or payload.get("approval_receipt_id") != sha256_json(core)
        or _utc(payload["approved_at"], name="activation approval time")
        < candidate_calendar.source_retrieved_at
    ):
        raise IntegrityError("calendar activation lacks exact hash-bound approval")
    return str(payload["approval_receipt_id"])


def load_calendar_index(
    receipt: VerifiedReleaseReceipt,
    *,
    boundary: RepoBoundary,
    expected_markets: Sequence[str] | None = None,
) -> LoadedCalendarIndex:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "controls"
        or manifest.release_kind != INDEX_RELEASE_KIND
        or manifest.schema_version != INDEX_SCHEMA_VERSION
        or manifest.files
        or set(manifest.embedded_documents) != {"exchange_calendar_index.json"}
        or set(manifest.metadata)
        != {"activated_at_utc", "activation_approval_receipt_id", "index_id"}
    ):
        raise IntegrityError("exchange-calendar index release contract is invalid")
    raw_payload = receipt.embedded_document("exchange_calendar_index.json", boundary)
    if not isinstance(raw_payload, dict):
        raise IntegrityError("exchange-calendar index document is invalid")
    payload = dict(raw_payload)
    expected = {
        "activation_approval",
        "activated_at_utc",
        "activation_approval_receipt_id",
        "diff_report",
        "index_id",
        "predecessor_index_release_id",
        "schema_version",
        "segments",
    }
    if set(payload) != expected:
        raise IntegrityError("exchange-calendar index schema is invalid")
    index_id = payload.pop("index_id", None)
    predecessor = payload.get("predecessor_index_release_id")
    if (
        index_id != sha256_json(payload)
        or index_id != manifest.metadata["index_id"]
        or payload.get("schema_version") != INDEX_SCHEMA_VERSION
        or _utc(payload.get("activated_at_utc"), name="calendar activation time")
        is None
        or type(payload.get("activation_approval_receipt_id")) is not str
        or _HASH.fullmatch(str(payload["activation_approval_receipt_id"])) is None
        or predecessor is not None
        and (type(predecessor) is not str or _HASH.fullmatch(predecessor) is None)
    ):
        raise IntegrityError("exchange-calendar index identity is invalid")
    raw_segments = payload.get("segments")
    if not isinstance(raw_segments, list) or not raw_segments:
        raise IntegrityError("exchange-calendar index segments are invalid")
    calendars: dict[str, VerifiedExchangeCalendar] = {}
    segments: list[dict[str, object]] = []
    prior_end: date | None = None
    for raw in raw_segments:
        if not isinstance(raw, dict) or set(raw) != {
            "calendar_receipt",
            "effective_from_trade_date",
            "effective_through_trade_date",
        }:
            raise IntegrityError("exchange-calendar index segment schema is invalid")
        start = _date(raw["effective_from_trade_date"], name="segment start")
        end = _date(raw["effective_through_trade_date"], name="segment end")
        calendar_receipt = _receipt(raw["calendar_receipt"], name="calendar segment")
        calendar = VerifiedExchangeCalendar.from_release(
            calendar_receipt,
            boundary=boundary,
            expected_markets=expected_markets,
        )
        if (
            start > end
            or start < calendar.coverage_start
            or end > calendar.coverage_end
            or prior_end is not None
            and start <= prior_end
        ):
            raise IntegrityError("exchange-calendar index segments overlap or escape coverage")
        prior_end = end
        calendars[calendar_receipt.release_id] = calendar
        segments.append(
            {
                "calendar_receipt": calendar_receipt.as_dict(),
                "calendar_release_id": calendar_receipt.release_id,
                "effective_from_trade_date": start.isoformat(),
                "effective_through_trade_date": end.isoformat(),
            }
        )
    expected_sources = set(calendars)
    if predecessor is not None:
        expected_sources.add(predecessor)
    if set(manifest.source_release_ids) != expected_sources:
        raise IntegrityError("exchange-calendar index dependency closure is invalid")
    activation_approval = payload.get("activation_approval")
    diff_report = payload.get("diff_report")
    if not isinstance(activation_approval, Mapping) or not isinstance(
        diff_report, dict
    ):
        raise IntegrityError("exchange-calendar activation evidence is invalid")
    diff_expected_keys = {
        "changed_market_dates",
        "diff_report_id",
        "predecessor_calendar_release_id",
        "successor_calendar_release_id",
    }
    diff_core = {
        key: diff_report[key]
        for key in diff_report
        if key != "diff_report_id"
    }
    changed = diff_report.get("changed_market_dates")
    if (
        set(diff_report) != diff_expected_keys
        or diff_report.get("diff_report_id") != sha256_json(diff_core)
        or not isinstance(changed, list)
        or any(
            not isinstance(item, dict)
            or set(item) != {"change", "market", "trade_date"}
            or item.get("change") not in {"ADDED", "CHANGED", "REMOVED"}
            or type(item.get("market")) is not str
            or _MARKET.fullmatch(str(item["market"])) is None
            or type(item.get("trade_date")) is not str
            or _date(item["trade_date"], name="calendar diff trade date") is None
            for item in changed
        )
        or changed
        != sorted(
            changed,
            key=lambda item: (str(item["market"]), str(item["trade_date"])),
        )
        or len(
            {
                (str(item["market"]), str(item["trade_date"]))
                for item in changed
            }
        )
        != len(changed)
    ):
        raise IntegrityError("exchange-calendar diff evidence is invalid")
    candidate_id = activation_approval.get("candidate_calendar_release_id")
    candidate = calendars.get(str(candidate_id))
    if (
        candidate is None
        or diff_report.get("successor_calendar_release_id")
        != candidate.receipt.release_id
        or diff_report.get("predecessor_calendar_release_id")
        != candidate.predecessor_calendar_release_id
        or payload["activation_approval_receipt_id"]
        != activation_approval.get("approval_receipt_id")
        or payload["activated_at_utc"] != activation_approval.get("approved_at")
        or validate_activation_approval(
            activation_approval,
            candidate_calendar=candidate,
            predecessor_index_release_id=predecessor,
            diff_report_id=str(diff_report["diff_report_id"]),
        )
        != payload["activation_approval_receipt_id"]
    ):
        raise IntegrityError(
            "exchange-calendar activation approval is not exact"
        )
    return LoadedCalendarIndex(
        receipt=receipt,
        segments=tuple(segments),
        calendar_by_release_id=MappingProxyType(calendars),
        index_id=str(index_id),
        predecessor_index_release_id=predecessor,
        boundary=boundary,
    )


def publish_calendar_index(
    *,
    candidate_calendar_receipt: VerifiedReleaseReceipt,
    activation_approval: Mapping[str, object],
    publisher: AtomicPublisher,
    expected_markets: Sequence[str],
    freshness_at: datetime,
    minimum_forward_days: int = 90,
    predecessor_index_receipt: VerifiedReleaseReceipt | None = None,
) -> VerifiedReleaseReceipt:
    candidate = VerifiedExchangeCalendar.from_release(
        candidate_calendar_receipt,
        boundary=publisher.boundary,
        expected_markets=expected_markets,
    )
    predecessor = (
        load_calendar_index(
            predecessor_index_receipt,
            boundary=publisher.boundary,
            expected_markets=expected_markets,
        )
        if predecessor_index_receipt is not None
        else None
    )
    predecessor_calendar: VerifiedExchangeCalendar | None = None
    if candidate.predecessor_calendar_release_id is not None:
        if predecessor is None:
            raise IntegrityError("calendar successor has no predecessor index")
        predecessor_calendar = predecessor.calendar_by_release_id.get(
            candidate.predecessor_calendar_release_id
        )
        if predecessor_calendar is None:
            raise IntegrityError("calendar successor predecessor is not active")
    diff = diff_exchange_calendars(predecessor_calendar, candidate)
    approval_id = validate_activation_approval(
        activation_approval,
        candidate_calendar=candidate,
        predecessor_index_release_id=(
            predecessor.receipt.release_id if predecessor is not None else None
        ),
        diff_report_id=str(diff["diff_report_id"]),
    )
    segments = _calendar_index_successor_segments(
        candidate_calendar_receipt=candidate_calendar_receipt,
        candidate=candidate,
        predecessor=predecessor,
    )
    preview_calendars = (
        dict(predecessor.calendar_by_release_id)
        if predecessor is not None
        else {}
    )
    preview_calendars[candidate.receipt.release_id] = candidate
    preview_segments = tuple(
        {
            **segment,
            "calendar_release_id": str(
                segment["calendar_receipt"]["release_id"]  # type: ignore[index]
            ),
        }
        for segment in segments
    )
    preview = LoadedCalendarIndex(
        receipt=candidate_calendar_receipt,
        segments=preview_segments,
        calendar_by_release_id=MappingProxyType(preview_calendars),
        index_id=sha256_json(
            {
                "candidate_calendar_release_id": candidate.receipt.release_id,
                "predecessor_index_release_id": (
                    predecessor.receipt.release_id
                    if predecessor is not None
                    else None
                ),
                "segments": segments,
            }
        ),
        predecessor_index_release_id=(
            predecessor.receipt.release_id if predecessor is not None else None
        ),
        boundary=publisher.boundary,
    )
    verify_calendar_freshness(
        preview,
        expected_markets=expected_markets,
        now=freshness_at,
        minimum_forward_days=minimum_forward_days,
    )
    activated_at = str(activation_approval["approved_at"])
    core: dict[str, object] = {
        "activation_approval": dict(activation_approval),
        "activated_at_utc": activated_at,
        "activation_approval_receipt_id": approval_id,
        "diff_report": diff,
        "predecessor_index_release_id": (
            predecessor.receipt.release_id if predecessor is not None else None
        ),
        "schema_version": INDEX_SCHEMA_VERSION,
        "segments": segments,
    }
    payload = {**core, "index_id": sha256_json(core)}
    source_ids = {
        str(segment["calendar_receipt"]["release_id"])  # type: ignore[index]
        for segment in segments
    }
    if predecessor is not None:
        source_ids.add(predecessor.receipt.release_id)
    stage = publisher.create_stage("calendar_index")
    release = ReleaseManifest.build(
        stage,
        phase="controls",
        release_kind=INDEX_RELEASE_KIND,
        schema_version=INDEX_SCHEMA_VERSION,
        logical_paths={},
        source_release_ids=tuple(sorted(source_ids)),
        embedded_documents={"exchange_calendar_index.json": payload},
        metadata={
            "activated_at_utc": activated_at,
            "activation_approval_receipt_id": approval_id,
            "index_id": payload["index_id"],
        },
    )
    path = publisher.publish(stage, release)
    receipt = VerifiedReleaseReceipt.from_manifest(path, publisher.boundary)
    load_calendar_index(
        receipt, boundary=publisher.boundary, expected_markets=expected_markets
    )
    return receipt


def _calendar_index_successor_segments(
    *,
    candidate_calendar_receipt: VerifiedReleaseReceipt,
    candidate: VerifiedExchangeCalendar,
    predecessor: LoadedCalendarIndex | None,
) -> list[dict[str, object]]:
    segments: list[dict[str, object]] = []
    if predecessor is not None:
        for segment in predecessor.segments:
            start = date.fromisoformat(str(segment["effective_from_trade_date"]))
            end = date.fromisoformat(str(segment["effective_through_trade_date"]))
            if end < candidate.coverage_start or start > candidate.coverage_end:
                segments.append(
                    {
                        "calendar_receipt": segment["calendar_receipt"],
                        "effective_from_trade_date": start.isoformat(),
                        "effective_through_trade_date": end.isoformat(),
                    }
                )
                continue
            if start < candidate.coverage_start:
                segments.append(
                    {
                        "calendar_receipt": segment["calendar_receipt"],
                        "effective_from_trade_date": start.isoformat(),
                        "effective_through_trade_date": (
                            candidate.coverage_start - timedelta(days=1)
                        ).isoformat(),
                    }
                )
            if end > candidate.coverage_end:
                segments.append(
                    {
                        "calendar_receipt": segment["calendar_receipt"],
                        "effective_from_trade_date": (
                            candidate.coverage_end + timedelta(days=1)
                        ).isoformat(),
                        "effective_through_trade_date": end.isoformat(),
                    }
                )
    segments.append(
        {
            "calendar_receipt": candidate_calendar_receipt.as_dict(),
            "effective_from_trade_date": candidate.coverage_start.isoformat(),
            "effective_through_trade_date": candidate.coverage_end.isoformat(),
        }
    )
    segments.sort(key=lambda item: str(item["effective_from_trade_date"]))
    return segments


def active_pointer_payload(
    index_receipt: VerifiedReleaseReceipt,
    *,
    activation_approval_receipt_id: str,
    activated_at_utc: str,
) -> dict[str, object]:
    core: dict[str, object] = {
        "activated_at_utc": activated_at_utc,
        "activation_approval_receipt_id": activation_approval_receipt_id,
        "calendar_index_receipt": index_receipt.as_dict(),
        "schema_version": ACTIVE_POINTER_SCHEMA,
    }
    return {**core, "pointer_id": sha256_json(core)}


def load_active_calendar_index(
    *,
    boundary: RepoBoundary,
    expected_markets: Sequence[str],
    path: Path | None = None,
) -> LoadedCalendarIndex:
    pointer_path = path or boundary.active_root / "configs" / "active_exchange_calendar.json"
    boundary.assert_active_path(
        pointer_path, purpose="active exchange calendar", subtree="configs"
    )
    if not pointer_path.exists():
        raise IntegrityError("HISTORICAL_CALENDAR_SOURCE_NOT_ESTABLISHED")
    payload = _read_canonical_object(
        pointer_path, description="active exchange-calendar pointer"
    )
    expected = {
        "activated_at_utc",
        "activation_approval_receipt_id",
        "calendar_index_receipt",
        "pointer_id",
        "schema_version",
    }
    core = {key: payload[key] for key in payload if key != "pointer_id"}
    if (
        set(payload) != expected
        or payload.get("schema_version") != ACTIVE_POINTER_SCHEMA
        or payload.get("pointer_id") != sha256_json(core)
        or type(payload.get("activation_approval_receipt_id")) is not str
        or _HASH.fullmatch(str(payload["activation_approval_receipt_id"])) is None
        or type(payload.get("activated_at_utc")) is not str
        or _UTC_SECOND.fullmatch(str(payload["activated_at_utc"])) is None
    ):
        raise IntegrityError("active exchange-calendar pointer is invalid")
    receipt = _receipt(payload["calendar_index_receipt"], name="active calendar index")
    index = load_calendar_index(
        receipt, boundary=boundary, expected_markets=expected_markets
    )
    manifest = receipt.verify(boundary)
    if (
        manifest.metadata.get("activation_approval_receipt_id")
        != payload["activation_approval_receipt_id"]
        or manifest.metadata.get("activated_at_utc") != payload["activated_at_utc"]
    ):
        raise IntegrityError("active calendar pointer differs from its index")
    return index


def verify_calendar_freshness(
    index: LoadedCalendarIndex,
    *,
    expected_markets: Sequence[str],
    now: datetime,
    minimum_forward_days: int = 90,
    general_max_age_hours: int = 168,
    holiday_window_days: int = 14,
    holiday_max_age_hours: int = 72,
) -> dict[str, object]:
    instant = require_utc(now, "calendar freshness time")
    cme_zone = ZoneInfo(CME_TIMEZONE)
    today = instant.astimezone(cme_zone).date()
    end = today + timedelta(days=minimum_forward_days - 1)
    calendars: dict[str, VerifiedExchangeCalendar] = {}
    farther_holidays = sorted(
        {
            date.fromisoformat(notice["trade_date"])
            for calendar in index.calendar_by_release_id.values()
            for notice in calendar.holiday_notices
            if date.fromisoformat(notice["trade_date"]) > end
        }
    )
    required_dates = [*_dates_inclusive(today, end), *farther_holidays]
    for trade_date in required_dates:
        for market in expected_markets:
            try:
                calendar = index.calendar_for(market, trade_date)
            except ContractError as exc:
                raise IntegrityError(
                    "active exchange calendar coverage is incomplete"
                ) from exc
            calendars[calendar.receipt.release_id] = calendar
    for calendar in calendars.values():
        age = instant - calendar.source_retrieved_at
        if age < timedelta(0) or age > timedelta(hours=general_max_age_hours):
            raise IntegrityError("active exchange calendar is stale")
        for notice in calendar.holiday_notices:
            holiday = date.fromisoformat(notice["trade_date"])
            days_until = (holiday - today).days
            if 0 <= days_until <= holiday_window_days and (
                age > timedelta(hours=holiday_max_age_hours)
                or calendar.source_retrieved_at.astimezone(cme_zone).date()
                < holiday - timedelta(days=holiday_window_days)
            ):
                raise IntegrityError("upcoming CME holiday has not been revalidated")
    core: dict[str, object] = {
        "calendar_index_release_id": index.receipt.release_id,
        "checked_at_utc": _utc_text(instant),
        "coverage_end_trade_date": (
            max([end, *farther_holidays]).isoformat()
        ),
        "coverage_start_trade_date": today.isoformat(),
        "markets": list(expected_markets),
        "status": "CURRENT",
    }
    return {**core, "freshness_check_id": sha256_json(core)}


def _normalize_requirements(
    intervals: Sequence[object],
) -> tuple[dict[str, object], ...]:
    requirements: list[dict[str, object]] = []
    for interval in intervals:
        if isinstance(interval, Mapping):
            market = interval.get("market")
            start_raw = interval.get("start")
            end_raw = interval.get("end")
            interval_key = interval.get("interval_key")
            year = interval.get("year")
        else:
            market = getattr(interval, "market", None)
            start_raw = getattr(interval, "start", None)
            end_raw = getattr(interval, "end", None)
            interval_key = None
            year = getattr(interval, "year", None)
        if type(market) is not str or _MARKET.fullmatch(market) is None:
            raise ContractError("foundation calendar requirement market is invalid")
        start = _date(start_raw, name="foundation interval start")
        end_exclusive = _date(end_raw, name="foundation interval end")
        if start >= end_exclusive:
            raise ContractError("foundation interval range is invalid")
        key = (
            str(interval_key)
            if type(interval_key) is str and interval_key
            else (
                f"{market}/{year}/{start.isoformat()}_{end_exclusive.isoformat()}"
                if type(year) is int
                else f"{market}:{start.isoformat()}:{end_exclusive.isoformat()}"
            )
        )
        requirements.append(
            {
                "end_trade_date": (end_exclusive - timedelta(days=1)).isoformat(),
                "interval_key": key,
                "market": market,
                "start_trade_date": start.isoformat(),
            }
        )
    result = tuple(sorted(requirements, key=lambda item: str(item["interval_key"])))
    if len({item["interval_key"] for item in result}) != len(result):
        raise ContractError("foundation calendar requirements are duplicated")
    return result


def publish_foundation_calendar_coverage(
    *,
    index_receipt: VerifiedReleaseReceipt,
    intervals: Sequence[object],
    publisher: AtomicPublisher,
    expected_markets: Sequence[str] | None = None,
) -> VerifiedReleaseReceipt:
    index = load_calendar_index(
        index_receipt,
        boundary=publisher.boundary,
        expected_markets=expected_markets,
    )
    requirements = _normalize_requirements(intervals)
    calendar_ids: set[str] = set()
    for requirement in requirements:
        start = date.fromisoformat(str(requirement["start_trade_date"]))
        end = date.fromisoformat(str(requirement["end_trade_date"]))
        market = str(requirement["market"])
        for trade_date in _dates_inclusive(start, end):
            calendar_ids.add(index.calendar_for(market, trade_date).receipt.release_id)
    calendar_receipts = [
        index.calendar_by_release_id[release_id].receipt.as_dict()
        for release_id in sorted(calendar_ids)
    ]
    core: dict[str, object] = {
        "calendar_index_receipt": index_receipt.as_dict(),
        "calendar_release_receipts": calendar_receipts,
        "requirements": list(requirements),
        "schema_version": COVERAGE_SCHEMA_VERSION,
    }
    payload = {**core, "coverage_id": sha256_json(core)}
    stage = publisher.create_stage("calendar_coverage")
    release = ReleaseManifest.build(
        stage,
        phase="controls",
        release_kind=COVERAGE_RELEASE_KIND,
        schema_version=COVERAGE_SCHEMA_VERSION,
        logical_paths={},
        source_release_ids=tuple(
            sorted({index_receipt.release_id, *calendar_ids})
        ),
        embedded_documents={"foundation_calendar_coverage.json": payload},
        metadata={
            "calendar_index_release_id": index_receipt.release_id,
            "coverage_id": payload["coverage_id"],
            "interval_count": len(requirements),
        },
    )
    path = publisher.publish(stage, release)
    receipt = VerifiedReleaseReceipt.from_manifest(path, publisher.boundary)
    load_foundation_calendar_coverage(
        receipt,
        boundary=publisher.boundary,
        expected_markets=expected_markets,
        expected_intervals=intervals,
    )
    return receipt


def load_foundation_calendar_coverage(
    receipt: VerifiedReleaseReceipt,
    *,
    boundary: RepoBoundary,
    expected_markets: Sequence[str] | None = None,
    expected_intervals: Sequence[object] | None = None,
) -> LoadedFoundationCalendarCoverage:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "controls"
        or manifest.release_kind != COVERAGE_RELEASE_KIND
        or manifest.schema_version != COVERAGE_SCHEMA_VERSION
        or manifest.files
        or set(manifest.embedded_documents)
        != {"foundation_calendar_coverage.json"}
        or set(manifest.metadata)
        != {"calendar_index_release_id", "coverage_id", "interval_count"}
    ):
        raise IntegrityError("foundation calendar-coverage release is invalid")
    raw_payload = receipt.embedded_document(
        "foundation_calendar_coverage.json", boundary
    )
    if not isinstance(raw_payload, dict):
        raise IntegrityError("foundation calendar-coverage document is invalid")
    payload = dict(raw_payload)
    expected = {
        "calendar_index_receipt",
        "calendar_release_receipts",
        "coverage_id",
        "requirements",
        "schema_version",
    }
    if set(payload) != expected:
        raise IntegrityError("foundation calendar-coverage schema is invalid")
    coverage_id = payload.pop("coverage_id", None)
    if (
        coverage_id != sha256_json(payload)
        or coverage_id != manifest.metadata["coverage_id"]
        or payload.get("schema_version") != COVERAGE_SCHEMA_VERSION
    ):
        raise IntegrityError("foundation calendar-coverage identity is invalid")
    index_receipt = _receipt(
        payload.get("calendar_index_receipt"), name="foundation calendar index"
    )
    index = load_calendar_index(
        index_receipt, boundary=boundary, expected_markets=expected_markets
    )
    raw_requirements = payload.get("requirements")
    if not isinstance(raw_requirements, list):
        raise IntegrityError("foundation calendar requirements are invalid")
    requirements: list[dict[str, object]] = []
    for raw in raw_requirements:
        if not isinstance(raw, dict) or set(raw) != {
            "end_trade_date",
            "interval_key",
            "market",
            "start_trade_date",
        }:
            raise IntegrityError("foundation calendar requirement schema is invalid")
        start = _date(raw["start_trade_date"], name="coverage requirement start")
        end = _date(raw["end_trade_date"], name="coverage requirement end")
        if (
            type(raw["market"]) is not str
            or _MARKET.fullmatch(str(raw["market"])) is None
            or type(raw["interval_key"]) is not str
            or not raw["interval_key"]
            or start > end
        ):
            raise IntegrityError("foundation calendar requirement is invalid")
        requirements.append(dict(raw))
        for trade_date in _dates_inclusive(start, end):
            index.calendar_for(str(raw["market"]), trade_date)
    if requirements != sorted(
        requirements, key=lambda item: str(item["interval_key"])
    ):
        raise IntegrityError("foundation calendar requirements are noncanonical")
    if expected_intervals is not None and tuple(requirements) != _normalize_requirements(
        expected_intervals
    ):
        raise IntegrityError("foundation calendar coverage differs from selected intervals")
    raw_calendars = payload.get("calendar_release_receipts")
    if not isinstance(raw_calendars, list):
        raise IntegrityError("foundation calendar receipt set is invalid")
    observed_ids: list[str] = []
    for raw in raw_calendars:
        calendar_receipt = _receipt(raw, name="foundation calendar")
        calendar = VerifiedExchangeCalendar.from_release(
            calendar_receipt,
            boundary=boundary,
            expected_markets=expected_markets,
        )
        if calendar.receipt.release_id not in index.calendar_by_release_id:
            raise IntegrityError("foundation calendar is not present in its index")
        observed_ids.append(calendar.receipt.release_id)
    if observed_ids != sorted(set(observed_ids)):
        raise IntegrityError("foundation calendar receipt set is noncanonical")
    expected_sources = {index_receipt.release_id, *observed_ids}
    if (
        set(manifest.source_release_ids) != expected_sources
        or manifest.metadata["calendar_index_release_id"] != index_receipt.release_id
        or manifest.metadata["interval_count"] != len(requirements)
    ):
        raise IntegrityError("foundation calendar-coverage closure is invalid")
    payload["coverage_id"] = coverage_id
    return LoadedFoundationCalendarCoverage(
        receipt=receipt,
        index=index,
        requirements=tuple(requirements),
        coverage_id=str(coverage_id),
        boundary=boundary,
    )


def coverage_matches_active_index(
    coverage: LoadedFoundationCalendarCoverage,
    active_index: LoadedCalendarIndex,
) -> bool:
    for requirement in coverage.requirements:
        start = date.fromisoformat(str(requirement["start_trade_date"]))
        end = date.fromisoformat(str(requirement["end_trade_date"]))
        market = str(requirement["market"])
        for trade_date in _dates_inclusive(start, end):
            bound = coverage.index.calendar_for(market, trade_date)
            active = active_index.calendar_for(market, trade_date)
            bound_product = bound.products[market].as_dict()
            active_product = active.products[market].as_dict()
            bound_session = bound.sessions[(market, trade_date)]
            active_session = active.sessions[(market, trade_date)]
            if (
                bound_product != active_product
                or tuple(item.as_dict() for item in bound_session.intervals)
                != tuple(item.as_dict() for item in active_session.intervals)
            ):
                return False
    return True


def publish_calendar_state_eligibility(
    *,
    causal_receipt: VerifiedReleaseReceipt,
    coverage_receipt: VerifiedReleaseReceipt,
    market: str,
    year: int,
    interval_key: str,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    from .foundation.materialize import load_causal_interval

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - pinned foundation dependency
        raise ContractError("calendar eligibility requires the pinned PyArrow runtime") from exc
    coverage = load_foundation_calendar_coverage(
        coverage_receipt, boundary=publisher.boundary
    )
    bars_path, report = load_causal_interval(
        causal_receipt, boundary=publisher.boundary
    )
    if report.get("market") != market or report.get("year") != year:
        raise IntegrityError("calendar eligibility interval identity is invalid")
    counts = {state: 0 for state in sorted(ALLOWED_STATES)}
    unresolved = 0
    mismatched_session_dates = 0
    rows = 0
    parquet = pq.ParquetFile(bars_path)
    for batch in parquet.iter_batches(
        batch_size=100_000,
        columns=["event_at_ns", "exchange_session_date"],
    ):
        data = batch.to_pydict()
        for event_at_ns, session_date_raw in zip(
            data["event_at_ns"], data["exchange_session_date"], strict=True
        ):
            rows += 1
            try:
                trade_date = date.fromisoformat(str(session_date_raw))
                calendar = coverage.calendar_for(market, trade_date)
                instant = datetime.fromtimestamp(
                    int(event_at_ns) / 1_000_000_000, tz=timezone.utc
                )
                state = calendar.state_at(
                    market, instant, trade_date=trade_date
                )
                counts[state] += 1
            except (ContractError, IntegrityError, TypeError, ValueError, OverflowError):
                unresolved += 1
                continue
            local_trade_date = (
                instant.astimezone(ZoneInfo(CME_TIMEZONE)).date()
                + (
                    timedelta(days=1)
                    if instant.astimezone(ZoneInfo(CME_TIMEZONE)).time() >= time(17)
                    else timedelta(0)
                )
            )
            if local_trade_date != trade_date:
                mismatched_session_dates += 1
    failed_rows = (
        unresolved
        + mismatched_session_dates
        + sum(counts[state] for state in NON_TRADING_STATES)
    )
    disposition = "ELIGIBLE" if failed_rows == 0 and rows > 0 else "FAIL_CALENDAR_STATE"
    core: dict[str, object] = {
        "calendar_coverage_receipt": coverage_receipt.as_dict(),
        "causal_release_receipt": causal_receipt.as_dict(),
        "disposition": disposition,
        "interval_key": interval_key,
        "market": market,
        "mismatched_session_date_rows": mismatched_session_dates,
        "row_count": rows,
        "schema_version": ELIGIBILITY_SCHEMA_VERSION,
        "state_counts": counts,
        "unresolved_rows": unresolved,
        "year": year,
    }
    payload = {**core, "calendar_eligibility_id": sha256_json(core)}
    stage = publisher.create_stage("calendar_eligibility")
    release = ReleaseManifest.build(
        stage,
        phase="calendar_eligibility",
        release_kind=ELIGIBILITY_RELEASE_KIND,
        schema_version=ELIGIBILITY_SCHEMA_VERSION,
        logical_paths={},
        source_release_ids=tuple(
            sorted((causal_receipt.release_id, coverage_receipt.release_id))
        ),
        embedded_documents={"calendar_state_eligibility.json": payload},
        metadata={
            "calendar_eligibility_id": payload["calendar_eligibility_id"],
            "disposition": disposition,
            "interval_key": interval_key,
            "market": market,
            "row_count": rows,
            "year": year,
        },
    )
    path = publisher.publish(stage, release)
    receipt = VerifiedReleaseReceipt.from_manifest(path, publisher.boundary)
    loaded = load_calendar_state_eligibility(
        receipt,
        boundary=publisher.boundary,
        expected_causal_receipt=causal_receipt,
        expected_coverage_receipt=coverage_receipt,
    )
    if loaded["disposition"] != "ELIGIBLE":
        raise IntegrityError("foundation bars violate the verified CME schedule")
    return receipt


def load_calendar_state_eligibility(
    receipt: VerifiedReleaseReceipt,
    *,
    boundary: RepoBoundary,
    expected_causal_receipt: VerifiedReleaseReceipt,
    expected_coverage_receipt: VerifiedReleaseReceipt,
) -> dict[str, object]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "calendar_eligibility"
        or manifest.release_kind != ELIGIBILITY_RELEASE_KIND
        or manifest.schema_version != ELIGIBILITY_SCHEMA_VERSION
        or manifest.files
        or set(manifest.embedded_documents) != {"calendar_state_eligibility.json"}
        or set(manifest.metadata)
        != {
            "calendar_eligibility_id",
            "disposition",
            "interval_key",
            "market",
            "row_count",
            "year",
        }
    ):
        raise IntegrityError("calendar-state eligibility release is invalid")
    raw_payload = receipt.embedded_document(
        "calendar_state_eligibility.json", boundary
    )
    if not isinstance(raw_payload, dict):
        raise IntegrityError("calendar-state eligibility document is invalid")
    payload = dict(raw_payload)
    expected = {
        "calendar_coverage_receipt",
        "calendar_eligibility_id",
        "causal_release_receipt",
        "disposition",
        "interval_key",
        "market",
        "mismatched_session_date_rows",
        "row_count",
        "schema_version",
        "state_counts",
        "unresolved_rows",
        "year",
    }
    if set(payload) != expected:
        raise IntegrityError("calendar-state eligibility schema is invalid")
    eligibility_id = payload.pop("calendar_eligibility_id", None)
    causal = _receipt(payload["causal_release_receipt"], name="calendar causal")
    coverage = _receipt(payload["calendar_coverage_receipt"], name="calendar coverage")
    counts = payload.get("state_counts")
    count_fields = (
        payload.get("row_count"),
        payload.get("unresolved_rows"),
        payload.get("mismatched_session_date_rows"),
    )
    if (
        eligibility_id != sha256_json(payload)
        or eligibility_id != manifest.metadata["calendar_eligibility_id"]
        or payload.get("schema_version") != ELIGIBILITY_SCHEMA_VERSION
        or causal != expected_causal_receipt
        or coverage != expected_coverage_receipt
        or manifest.source_release_ids
        != tuple(sorted((causal.release_id, coverage.release_id)))
        or payload.get("disposition") not in {"ELIGIBLE", "FAIL_CALENDAR_STATE"}
        or not isinstance(counts, dict)
        or set(counts) != ALLOWED_STATES
        or any(type(value) is not int or value < 0 for value in counts.values())
        or any(type(value) is not int or value < 0 for value in count_fields)
    ):
        raise IntegrityError("calendar-state eligibility identity or census is invalid")
    failed_rows = (
        payload["unresolved_rows"]
        + payload["mismatched_session_date_rows"]
        + sum(counts[state] for state in NON_TRADING_STATES)
    )
    expected_disposition = (
        "ELIGIBLE"
        if failed_rows == 0 and payload["row_count"] > 0
        else "FAIL_CALENDAR_STATE"
    )
    if (
        sum(counts.values()) + payload["unresolved_rows"] != payload["row_count"]
        or payload["mismatched_session_date_rows"] > payload["row_count"]
        or payload.get("disposition") != expected_disposition
        or manifest.metadata
        != {
            "calendar_eligibility_id": eligibility_id,
            "disposition": payload["disposition"],
            "interval_key": payload["interval_key"],
            "market": payload["market"],
            "row_count": payload["row_count"],
            "year": payload["year"],
        }
    ):
        raise IntegrityError("calendar-state eligibility identity or census is invalid")
    payload["calendar_eligibility_id"] = eligibility_id
    return payload
